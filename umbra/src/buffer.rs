//! The buffer manager (Umbra §2.1–2.3, LeanStore's swizzling and cooling).
//!
//! **Memory.** Each size class reserves an anonymous virtual memory region as large as the whole
//! budget, split into frames of its page size. Physical memory is attached only to frames that
//! hold a page, and handed back with `MADV_DONTNEED` when a page leaves, so every class can use
//! the whole budget and pages of mixed sizes never fragment it. Only bytes held are counted
//! against the budget.
//!
//! **Finding a page.** A parent refers to a child through a [`Swip`], a 64-bit word in the
//! parent's page. Swizzled, it is the address of the child's frame; unswizzled (low bit set), it
//! is the child's [`PageId`]. A swizzled access is one load and one branch, with no table. A page
//! has exactly one swip pointing at it, and its frame remembers where that swip is.
//!
//! **Eviction.** When the pool fills, random-order unpinned pages with no swizzled children are
//! *cooled*: their parent's swip is unswizzled, but the page stays in memory in a FIFO. An access
//! to a cooling page swizzles it again without IO. Pages that reach the end of the FIFO are
//! written back if dirty and their memory released. A parent is never cooled before its
//! children, so a page goes to disk only with every swip in it unswizzled: pointers never reach
//! the disk.
//!
//! The prototype does IO under the eviction lock and the parent's swizzle lock, and writes pages
//! back only at eviction; the write-ahead log decides when a dirty page may go to disk.

use crate::latch::VersionedLatch;
use crate::page::{PageId, SizeClass, CLASS_COUNT, HEADER_SIZE};
use crate::store::{PageStore, ReadError};
use parking_lot::{Condvar, Mutex};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};

/// A reference from a parent to a child page.
pub type Swip = AtomicU64;

const NO_CHILD: u64 = 0;

fn unswizzled(pid: PageId) -> u64 {
    (pid.raw() << 1) | 1
}

fn swizzled(frame: &Frame) -> u64 {
    frame as *const Frame as u64
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    /// A page read from disk was not whole.
    Invalid(PageId),
    /// The class's pages are larger than the whole budget.
    TooLarge(SizeClass),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ReadError> for Error {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::Io(e) => Self::Io(e),
            ReadError::Invalid(pid) => Self::Invalid(pid),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// What a page's swips need from the page holding them: its latch, a lock serializing changes to
/// its swips, and the count of its children in memory.
#[derive(Default)]
pub struct SwipOwner {
    latch: VersionedLatch,
    swizzle: Mutex<()>,
    children: AtomicU32,
}

/// The parent of a root page: one swip outside any page.
#[derive(Default)]
pub struct Anchor {
    swip: Swip,
    owner: SwipOwner,
}

impl Anchor {
    pub fn new() -> Box<Self> {
        Box::default()
    }

    /// An anchor for a root already on disk.
    pub fn on_disk(pid: PageId) -> Box<Self> {
        let anchor = Self::new();
        anchor.swip.store(unswizzled(pid), Ordering::Relaxed);
        anchor
    }

    pub fn owner(&self) -> &SwipOwner {
        &self.owner
    }

    pub fn swip(&self) -> &Swip {
        &self.swip
    }
}

const FREE: u8 = 0;
const HOT: u8 = 1;
const COOL: u8 = 2;

/// Allocations waiting for a latched page to be let go so it can be evicted.
#[derive(Default)]
struct Waiters {
    count: AtomicUsize,
    released: Mutex<u64>,
    wake: Condvar,
}

impl Waiters {
    fn released(&self) {
        if self.count.load(Ordering::SeqCst) > 0 {
            *self.released.lock() += 1;
            self.wake.notify_all();
        }
    }
}

pub struct Frame {
    owner: SwipOwner,
    waiters: *const Waiters,
    pid: AtomicU64,
    state: AtomicU8,
    dirty: AtomicBool,
    class: SizeClass,
    index: u32,
    data: *mut u8,
    parent_swip: AtomicPtr<Swip>,
    parent: AtomicPtr<SwipOwner>,
}

impl Frame {
    fn pid(&self) -> PageId {
        PageId::from_raw(self.pid.load(Ordering::Relaxed))
    }

    fn bytes(&self) -> &[u8] {
        // SAFETY: the frame's memory is mapped for the manager's lifetime.
        unsafe { std::slice::from_raw_parts(self.data, self.class.size()) }
    }

    #[allow(clippy::mut_from_ref)]
    fn bytes_mut(&self) -> &mut [u8] {
        // SAFETY: callers hold the frame exclusively.
        unsafe { std::slice::from_raw_parts_mut(self.data, self.class.size()) }
    }

    fn swip(&self, offset: usize) -> &Swip {
        assert!(
            offset >= HEADER_SIZE && offset % 8 == 0 && offset + 8 <= self.class.size(),
            "swip offset {offset} out of the page"
        );
        // SAFETY: in the page, 8-aligned (frames are page-aligned), and the page outlives the
        // borrow.
        unsafe { &*(self.data.add(offset) as *const Swip) }
    }
}

struct Region {
    base: *mut u8,
    len: usize,
    frames: Box<[Frame]>,
    free: Mutex<Vec<u32>>,
}

impl Drop for Region {
    fn drop(&mut self) {
        if self.len > 0 {
            // SAFETY: the region was mapped with this length and nothing refers to it any more.
            unsafe { libc::munmap(self.base.cast(), self.len) };
        }
    }
}

#[derive(Default)]
struct Cooling {
    fifo: VecDeque<*const Frame>,
    pages: HashMap<PageId, *const Frame>,
    bytes: usize,
    hand: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub reads: u64,
    pub writes: u64,
    pub cooled: u64,
    pub cool_hits: u64,
    pub evicted: u64,
}

pub struct BufferManager {
    store: PageStore,
    budget: usize,
    used: AtomicUsize,
    regions: Vec<Region>,
    cooling: Mutex<Cooling>,
    waiters: Box<Waiters>,
    cooled: AtomicU64,
    cool_hits: AtomicU64,
    evicted: AtomicU64,
}

// SAFETY: frames are reached only through latches and locks; the raw pointers are into regions
// the manager owns for its whole lifetime.
unsafe impl Send for BufferManager {}
unsafe impl Sync for BufferManager {}

impl BufferManager {
    /// A buffer manager holding at most `budget` bytes of pages in memory.
    pub fn new(store: PageStore, budget: usize) -> io::Result<Self> {
        let waiters = Box::<Waiters>::default();
        let mut regions = Vec::with_capacity(CLASS_COUNT);
        for class in SizeClass::all() {
            let count = budget / class.size();
            let len = count * class.size();
            let base = if len == 0 {
                ptr::null_mut()
            } else {
                // SAFETY: an anonymous private mapping; MAP_NORESERVE commits nothing up front.
                let base = unsafe {
                    libc::mmap(
                        ptr::null_mut(),
                        len,
                        libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                        -1,
                        0,
                    )
                };
                if base == libc::MAP_FAILED {
                    return Err(io::Error::last_os_error());
                }
                base.cast::<u8>()
            };
            let frames = (0..count)
                .map(|i| Frame {
                    owner: SwipOwner::default(),
                    waiters: &*waiters,
                    pid: AtomicU64::new(0),
                    state: AtomicU8::new(FREE),
                    dirty: AtomicBool::new(false),
                    class,
                    index: i as u32,
                    // SAFETY: inside the mapping.
                    data: unsafe { base.add(i * class.size()) },
                    parent_swip: AtomicPtr::new(ptr::null_mut()),
                    parent: AtomicPtr::new(ptr::null_mut()),
                })
                .collect::<Box<[_]>>();
            let free = (0..count as u32).rev().collect();
            regions.push(Region {
                base,
                len,
                frames,
                free: Mutex::new(free),
            });
        }
        Ok(Self {
            store,
            budget,
            used: AtomicUsize::new(0),
            regions,
            cooling: Mutex::new(Cooling::default()),
            waiters,
            cooled: AtomicU64::new(0),
            cool_hits: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
        })
    }

    pub fn stats(&self) -> Stats {
        Stats {
            reads: self.store.reads(),
            writes: self.store.writes(),
            cooled: self.cooled.load(Ordering::Relaxed),
            cool_hits: self.cool_hits.load(Ordering::Relaxed),
            evicted: self.evicted.load(Ordering::Relaxed),
        }
    }

    /// Bytes of pages held in memory, hot or cooling.
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Creates a page of `class` and points `swip`, which must be empty, at it.
    pub fn allocate<'a>(
        &'a self,
        parent: &SwipOwner,
        swip: &Swip,
        class: SizeClass,
    ) -> Result<ExclusiveGuard<'a>> {
        // Room first: making it may cool this parent's other children, which takes its lock.
        let frame = self.take_frame(class)?;
        let _swips = parent.swizzle.lock();
        assert_eq!(swip.load(Ordering::Relaxed), NO_CHILD, "allocating over a child");
        let pid = self.store.allocate(class);
        frame.bytes_mut().fill(0);
        frame.pid.store(pid.raw(), Ordering::Relaxed);
        frame.dirty.store(true, Ordering::Relaxed);
        frame.owner.latch.lock_exclusive();
        self.attach(frame, parent, swip);
        Ok(ExclusiveGuard { frame })
    }

    /// The child `swip` points at, latched shared. The caller holds `parent` at least shared.
    pub fn fix_shared<'a>(&'a self, parent: &SwipOwner, swip: &Swip) -> Result<SharedGuard<'a>> {
        loop {
            let frame = self.resolve(parent, swip)?;
            frame.owner.latch.lock_shared();
            if swip.load(Ordering::Acquire) == swizzled(frame) {
                return Ok(SharedGuard { frame });
            }
            frame.owner.latch.unlock_shared();
        }
    }

    /// The child `swip` points at, latched exclusively. The caller holds `parent` at least shared.
    pub fn fix_exclusive<'a>(
        &'a self,
        parent: &SwipOwner,
        swip: &Swip,
    ) -> Result<ExclusiveGuard<'a>> {
        loop {
            let frame = self.resolve(parent, swip)?;
            frame.owner.latch.lock_exclusive();
            if swip.load(Ordering::Acquire) == swizzled(frame) {
                return Ok(ExclusiveGuard { frame });
            }
            frame.owner.latch.unlock_exclusive();
        }
    }

    /// Runs `read` over the child without latching it, again until no writer intervened. `read`
    /// may see a page mid-write and must only copy out of it.
    pub fn read_optimistic<R>(
        &self,
        parent: &SwipOwner,
        swip: &Swip,
        read: impl Fn(&[u8]) -> R,
    ) -> Result<R> {
        loop {
            let frame = self.resolve(parent, swip)?;
            let Some(version) = frame.owner.latch.optimistic() else {
                std::thread::yield_now();
                continue;
            };
            if swip.load(Ordering::Acquire) != swizzled(frame) {
                continue;
            }
            let out = read(frame.bytes());
            if frame.owner.latch.validate(version) && swip.load(Ordering::Acquire) == swizzled(frame)
            {
                return Ok(out);
            }
        }
    }

    /// The frame `swip` points at, loading or re-swizzling it when it is not swizzled. The frame
    /// is not latched: the caller latches it and checks that `swip` still points at it.
    fn resolve(&self, parent: &SwipOwner, swip: &Swip) -> Result<&Frame> {
        loop {
            let word = swip.load(Ordering::Acquire);
            assert_ne!(word, NO_CHILD, "following an empty swip");
            if word & 1 == 0 {
                // SAFETY: a swizzled swip holds the address of one of our frames.
                return Ok(unsafe { &*(word as *const Frame) });
            }
            let pid = PageId::from_raw(word >> 1);
            {
                let _swips = parent.swizzle.lock();
                if swip.load(Ordering::Acquire) != word {
                    continue;
                }
                if let Some(frame) = self.take_cooling(pid) {
                    self.cool_hits.fetch_add(1, Ordering::Relaxed);
                    self.attach(frame, parent, swip);
                    return Ok(frame);
                }
            }
            // Room first, without the parent's lock: making it may cool the parent's other
            // children.
            let frame = self.take_frame(pid.class())?;
            let _swips = parent.swizzle.lock();
            if swip.load(Ordering::Acquire) != word {
                self.release(frame);
                continue;
            }
            if let Some(cooled) = self.take_cooling(pid) {
                self.release(frame);
                self.cool_hits.fetch_add(1, Ordering::Relaxed);
                self.attach(cooled, parent, swip);
                return Ok(cooled);
            }
            if let Err(e) = self.store.read(pid, frame.bytes_mut()) {
                self.release(frame);
                return Err(e.into());
            }
            frame.pid.store(pid.raw(), Ordering::Relaxed);
            frame.dirty.store(false, Ordering::Relaxed);
            self.attach(frame, parent, swip);
            return Ok(frame);
        }
    }

    /// The cooling frame holding `pid`, taken out of the cooling stage.
    fn take_cooling(&self, pid: PageId) -> Option<&Frame> {
        let mut cooling = self.cooling.lock();
        let frame = cooling.pages.remove(&pid)?;
        cooling.bytes -= pid.class().size();
        // SAFETY: cooling frames are ours.
        Some(unsafe { &*frame })
    }

    /// Makes `frame` the hot child behind `swip`. The caller holds `parent.swizzle`.
    fn attach(&self, frame: &Frame, parent: &SwipOwner, swip: &Swip) {
        frame.state.store(HOT, Ordering::Relaxed);
        frame.parent_swip.store(ptr::from_ref(swip).cast_mut(), Ordering::Relaxed);
        frame.parent.store(ptr::from_ref(parent).cast_mut(), Ordering::Relaxed);
        parent.children.fetch_add(1, Ordering::Relaxed);
        swip.store(swizzled(frame), Ordering::Release);
    }

    /// A free frame of `class`, with its page's bytes counted against the budget.
    fn take_frame(&self, class: SizeClass) -> Result<&Frame> {
        let region = &self.regions[class.index()];
        if region.frames.is_empty() {
            return Err(Error::TooLarge(class));
        }
        self.make_room(class.size())?;
        let index = region
            .free
            .lock()
            .pop()
            .expect("bytes under the budget always leave a frame of every class free");
        Ok(&region.frames[index as usize])
    }

    fn release(&self, frame: &Frame) {
        let len = frame.class.size();
        // SAFETY: the frame holds no page any more; its bytes go back to the kernel.
        unsafe { libc::madvise(frame.data.cast(), len, libc::MADV_DONTNEED) };
        frame.state.store(FREE, Ordering::Relaxed);
        frame.dirty.store(false, Ordering::Relaxed);
        frame.parent_swip.store(ptr::null_mut(), Ordering::Relaxed);
        frame.parent.store(ptr::null_mut(), Ordering::Relaxed);
        self.regions[frame.class.index()].free.lock().push(frame.index);
        self.used.fetch_sub(len, Ordering::Relaxed);
    }

    /// Counts `len` more bytes against the budget, evicting until they fit. While every page
    /// that could go is latched, waits for one to be let go: a caller that itself holds pages
    /// worth the whole budget waits for ever.
    fn make_room(&self, len: usize) -> Result<()> {
        loop {
            self.waiters.count.fetch_add(1, Ordering::SeqCst);
            let seen = *self.waiters.released.lock();
            let room = self.try_make_room(len);
            if !matches!(room, Ok(false)) {
                self.waiters.count.fetch_sub(1, Ordering::SeqCst);
                return room.map(|_| ());
            }
            let mut released = self.waiters.released.lock();
            while *released == seen {
                self.waiters.wake.wait(&mut released);
            }
            drop(released);
            self.waiters.count.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// False when nothing could be evicted: everything that could go is latched.
    fn try_make_room(&self, len: usize) -> Result<bool> {
        let mut cooling = self.cooling.lock();
        loop {
            let used = self.used.load(Ordering::Relaxed);
            if used + len <= self.budget {
                self.used.fetch_add(len, Ordering::Relaxed);
                // Keep about a tenth of the pool cooling, so evictions find candidates at once.
                if used + len > self.budget / 10 * 9 && cooling.bytes < self.budget / 10 {
                    self.cool(&mut cooling, self.budget / 10);
                }
                return Ok(true);
            }
            if cooling.fifo.is_empty() {
                self.cool(&mut cooling, self.budget / 10);
            }
            if !self.evict_one(&mut cooling)? {
                // Cool more in case what is cooling is all latched, and try once more.
                let target = cooling.bytes + len;
                self.cool(&mut cooling, target);
                if !self.evict_one(&mut cooling)? {
                    return Ok(false);
                }
            }
        }
    }

    /// Cools pages until `target` bytes are cooling or a sweep of every frame found no more.
    fn cool(&self, cooling: &mut Cooling, target: usize) {
        let total: usize = self.regions.iter().map(|r| r.frames.len()).sum();
        let mut scanned = 0;
        while cooling.bytes < target && scanned < total {
            scanned += 1;
            cooling.hand = (cooling.hand + 1) % total;
            let mut at = cooling.hand;
            let region = self
                .regions
                .iter()
                .find(|r| {
                    let found = at < r.frames.len();
                    if !found {
                        at -= r.frames.len();
                    }
                    found
                })
                .expect("the hand is inside the frames");
            let frame = &region.frames[at];
            if frame.state.load(Ordering::Relaxed) != HOT
                || frame.owner.children.load(Ordering::Relaxed) != 0
                || !frame.owner.latch.try_lock_exclusive()
            {
                continue;
            }
            if self.try_unswizzle(frame) {
                frame.state.store(COOL, Ordering::Relaxed);
                cooling.fifo.push_back(frame);
                cooling.pages.insert(frame.pid(), frame);
                cooling.bytes += frame.class.size();
                self.cooled.fetch_add(1, Ordering::Relaxed);
            }
            frame.owner.latch.unlock_exclusive();
        }
    }

    /// Points the frame's parent swip back at its page id. The caller holds the frame exclusively.
    fn try_unswizzle(&self, frame: &Frame) -> bool {
        let parent = frame.parent.load(Ordering::Relaxed);
        if parent.is_null() || frame.owner.children.load(Ordering::Relaxed) != 0 {
            return false;
        }
        // SAFETY: a parent stays in memory while it has a child in memory.
        let parent = unsafe { &*parent };
        let Some(_swips) = parent.swizzle.try_lock() else {
            return false;
        };
        // A writer holding the parent may be rewriting the page the swip lives in.
        if !parent.latch.try_lock_shared() {
            return false;
        }
        // SAFETY: as for the parent.
        let swip = unsafe { &*frame.parent_swip.load(Ordering::Relaxed) };
        let done = swip.load(Ordering::Acquire) == swizzled(frame);
        if done {
            swip.store(unswizzled(frame.pid()), Ordering::Release);
            parent.children.fetch_sub(1, Ordering::Relaxed);
        }
        parent.latch.unlock_shared();
        done
    }

    /// Writes back and frees the oldest cooling page. False when none could be.
    fn evict_one(&self, cooling: &mut Cooling) -> Result<bool> {
        for _ in 0..cooling.fifo.len() {
            let frame = cooling.fifo.pop_front().expect("counted");
            // SAFETY: cooling frames are ours.
            let frame = unsafe { &*frame };
            if frame.state.load(Ordering::Relaxed) != COOL
                || cooling.pages.get(&frame.pid()) != Some(&ptr::from_ref(frame))
            {
                // Swizzled again since it was queued.
                continue;
            }
            if !frame.owner.latch.try_lock_exclusive() {
                // A reader that found it just before it cooled is about to let go.
                cooling.fifo.push_back(frame);
                continue;
            }
            cooling.pages.remove(&frame.pid());
            cooling.bytes -= frame.class.size();
            if frame.dirty.load(Ordering::Relaxed) {
                debug_assert_eq!(frame.owner.children.load(Ordering::Relaxed), 0);
                if let Err(e) = self.store.write(frame.pid(), frame.bytes_mut()) {
                    cooling.pages.insert(frame.pid(), frame);
                    cooling.bytes += frame.class.size();
                    cooling.fifo.push_front(frame);
                    frame.owner.latch.unlock_exclusive();
                    return Err(e.into());
                }
            }
            self.release(frame);
            self.evicted.fetch_add(1, Ordering::Relaxed);
            frame.owner.latch.unlock_exclusive();
            return Ok(true);
        }
        Ok(false)
    }

    #[cfg(test)]
    fn cool_everything(&self) {
        let mut cooling = self.cooling.lock();
        self.cool(&mut cooling, usize::MAX);
    }
}

pub struct SharedGuard<'a> {
    frame: &'a Frame,
}

impl SharedGuard<'_> {
    pub fn pid(&self) -> PageId {
        self.frame.pid()
    }

    pub fn owner(&self) -> &SwipOwner {
        &self.frame.owner
    }

    /// The swip at `offset` in this page.
    pub fn swip(&self, offset: usize) -> &Swip {
        self.frame.swip(offset)
    }
}

impl Deref for SharedGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.frame.bytes()
    }
}

impl Drop for SharedGuard<'_> {
    fn drop(&mut self) {
        self.frame.owner.latch.unlock_shared();
        // SAFETY: the waiters live as long as the manager owning the frame.
        unsafe { &*self.frame.waiters }.released();
    }
}

pub struct ExclusiveGuard<'a> {
    frame: &'a Frame,
}

impl ExclusiveGuard<'_> {
    pub fn pid(&self) -> PageId {
        self.frame.pid()
    }

    pub fn owner(&self) -> &SwipOwner {
        &self.frame.owner
    }

    pub fn swip(&self, offset: usize) -> &Swip {
        self.frame.swip(offset)
    }
}

impl Deref for ExclusiveGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.frame.bytes()
    }
}

impl DerefMut for ExclusiveGuard<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.frame.dirty.store(true, Ordering::Relaxed);
        self.frame.bytes_mut()
    }
}

impl Drop for ExclusiveGuard<'_> {
    fn drop(&mut self) {
        self.frame.owner.latch.unlock_exclusive();
        // SAFETY: as for shared guards.
        unsafe { &*self.frame.waiters }.released();
    }
}

#[cfg(test)]
mod tests;
