//! The buffer manager (Umbra §2.1–2.3, with LeanStore's swizzling and cooling).
//!
//! **Memory.** Each size class reserves an anonymous virtual memory region as large as the whole
//! budget, split into frames of its page size. Physical memory backs only frames that hold a
//! page and is handed back with `MADV_DONTNEED` when the page leaves, so every class can use the
//! whole budget and pages of mixed sizes never fragment it. Only bytes held count against it.
//!
//! **Finding a page.** A parent refers to a child through a [`Swip`], a 64-bit word in the
//! parent's page. Swizzled, it is the address of the child's frame; unswizzled (low bit set), the
//! child's [`PageId`]. A swizzled access is one load and one branch. A page has exactly one swip
//! pointing at it, and its frame remembers where.
//!
//! **Nothing waits.** Every operation returns `IOResult`. A page that is not in memory is read
//! with the frame already swizzled in the `Loading` state, and the caller gets the load's
//! completion; anyone else reaching the frame before it lands waits on the same completion. A
//! latch held by someone else, or a pool with nothing to evict, is a yield: under Turso's
//! cooperative scheduling the holder may be another connection on this very thread.
//!
//! **Eviction.** When the pool fills, unlatched pages with no children in memory are *cooled*:
//! their parent's swip is unswizzled, but the page stays in a FIFO, and touching it swizzles it
//! back without IO. A dirty page has its write-back submitted as it cools, so by the time it
//! reaches the end of the FIFO it is usually clean and leaves without waiting. A parent never
//! cools before its children, so a page goes to disk with every swip in it unswizzled: frame
//! addresses never reach the disk.
//!
//! Write-back writes a copy taken under a shared latch, so the page may change while the write
//! is in flight; the frame is clean afterwards only if it did not. The copy goes to a fresh slot
//! (the store writes out of place), and once it lands its remap is appended to the log. It does
//! not wait for the log first: a copy whose remap a crash lost is never read, so a copy may hold
//! changes the log has not made durable yet.

use super::latch::VersionedLatch;
use super::page::{self, PageId, SizeClass, CLASS_COUNT, HEADER_SIZE};
use super::store::{PageRead, PageStore};
use super::wal::{Record, Wal};
use crate::io::{Buffer, Completion, IO};
use crate::sync::atomic::{
    AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::ptr;

/// A reference from a parent to a child page.
pub type Swip = AtomicU64;

const NO_CHILD: u64 = 0;

fn unswizzled(pid: PageId) -> u64 {
    (pid.raw() << 1) | 1
}

fn swizzled(frame: &Frame) -> u64 {
    frame as *const Frame as u64
}

fn yield_now<T>() -> Result<IOResult<T>> {
    Ok(IOResult::IO(IOCompletions(Completion::new_yield())))
}

fn wait_on<T>(c: Completion) -> Result<IOResult<T>> {
    Ok(IOResult::IO(IOCompletions(c)))
}

/// What a page's swips need from the page holding them: its latch, a lock serializing changes to
/// its swips, the count of its children in memory, and where its swizzled swips are.
#[derive(Default)]
pub struct SwipOwner {
    latch: VersionedLatch,
    swizzle: Mutex<()>,
    children: AtomicU32,
    swizzled: Mutex<Vec<usize>>,
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
const LOADING: u8 = 1;
const HOT: u8 = 2;
const COOL: u8 = 3;
const FAILED: u8 = 4;

/// Releases of a latch, for callers waiting until something can be evicted.
#[derive(Default)]
struct Releases {
    armed: AtomicBool,
    waiting: Mutex<Option<Completion>>,
}

impl Releases {
    /// A completion that finishes at the next release of any latch.
    fn next(&self) -> Completion {
        let mut waiting = self.waiting.lock();
        let c = waiting.get_or_insert_with(Completion::new_wait).clone();
        self.armed.store(true, Ordering::SeqCst);
        c
    }

    fn released(&self) {
        if self.armed.load(Ordering::SeqCst) {
            self.armed.store(false, Ordering::SeqCst);
            if let Some(c) = self.waiting.lock().take() {
                c.complete(0);
            }
        }
    }
}

pub struct Frame {
    owner: SwipOwner,
    releases: *const Releases,
    pid: AtomicU64,
    state: AtomicU8,
    dirty: AtomicBool,
    /// Bumped by every exclusive holder that changed the page.
    changes: AtomicU64,
    /// The load in flight, while `Loading`.
    loaded: Mutex<Option<Completion>>,
    failure: Mutex<Option<CompletionError>>,
    /// The write-back in flight, if any.
    writing: Mutex<Option<Completion>>,
    class: SizeClass,
    index: u32,
    data: *mut u8,
    parent_swip: AtomicPtr<Swip>,
    parent: AtomicPtr<SwipOwner>,
}

/// A frame pointer a completion callback may carry to another thread.
#[derive(Clone, Copy)]
struct FramePtr(*const Frame);
// SAFETY: frames live in the manager's regions, which outlive every IO the manager submits
// (dropping the manager waits for them), and their shared state is atomics and locks.
unsafe impl Send for FramePtr {}
unsafe impl Sync for FramePtr {}

impl FramePtr {
    fn get(&self) -> &Frame {
        // SAFETY: see above.
        unsafe { &*self.0 }
    }
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
        // SAFETY: callers hold the frame exclusively, or it is not reachable by anyone else.
        unsafe { std::slice::from_raw_parts_mut(self.data, self.class.size()) }
    }

    fn swip(&self, offset: usize) -> &Swip {
        assert!(
            offset >= HEADER_SIZE && offset % 8 == 0 && offset + 8 <= self.class.size(),
            "swip offset {offset} out of the page"
        );
        // SAFETY: in the page and 8-aligned (frames are page-aligned).
        unsafe { &*(self.data.add(offset) as *const Swip) }
    }

    /// The bytes in `range` as they must reach the log or the disk: every swizzled swip in it
    /// replaced by its child's page id.
    fn durable_bytes(&self, range: std::ops::Range<usize>) -> Vec<u8> {
        let mut out = self.bytes()[range.clone()].to_vec();
        for &addr in self.owner.swizzled.lock().iter() {
            let offset = addr - self.data as usize;
            if offset < range.start || offset + 8 > range.end {
                continue;
            }
            // SAFETY: the address is a swip in this page.
            let word = unsafe { &*(addr as *const Swip) }.load(Ordering::Acquire);
            let word = if word & 1 == 0 {
                // SAFETY: a swizzled swip points at one of our frames.
                unswizzled(unsafe { &*(word as *const Frame) }.pid())
            } else {
                word
            };
            let at = offset - range.start;
            out[at..at + 8].copy_from_slice(&word.to_le_bytes());
        }
        out
    }

    fn releases(&self) -> &Releases {
        // SAFETY: owned by the manager, which outlives its frames' guards.
        unsafe { &*self.releases }
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
            // SAFETY: mapped with this length; nothing refers to it any more.
            unsafe { libc::munmap(self.base.cast(), self.len) };
        }
    }
}

#[derive(Default)]
struct Cooling {
    fifo: VecDeque<FramePtr>,
    pages: HashMap<PageId, FramePtr>,
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
    io: Arc<dyn IO>,
    store: Arc<PageStore>,
    wal: Option<Arc<Wal>>,
    budget: usize,
    /// Bytes to keep cooling, so evictions find candidates at once.
    cooling_target: usize,
    used: AtomicUsize,
    regions: Vec<Region>,
    cooling: Mutex<Cooling>,
    releases: Box<Releases>,
    cooled: AtomicU64,
    cool_hits: AtomicU64,
    evicted: AtomicU64,
}

// SAFETY: frames are reached only through latches, locks and atomics; the raw pointers are into
// regions the manager owns for its whole lifetime.
unsafe impl Send for BufferManager {}
unsafe impl Sync for BufferManager {}

/// LeanStore keeps this share of the pool cooling (Leis et al., ICDE 2018, §IV-C).
pub const DEFAULT_COOLING_SHARE: f64 = 0.1;

impl BufferManager {
    /// A manager holding at most `budget` bytes of pages, keeping `cooling_share` of them cooling.
    /// With `wal`, every copy written back is remapped in the log.
    pub fn new(
        io: Arc<dyn IO>,
        store: Arc<PageStore>,
        wal: Option<Arc<Wal>>,
        budget: usize,
        cooling_share: f64,
    ) -> Result<Self> {
        assert!(
            (0.0..1.0).contains(&cooling_share),
            "cooling share {cooling_share}"
        );
        let releases = Box::<Releases>::default();
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
                    return Err(LimboError::InternalError(format!(
                        "reserving {len} bytes for {class:?}: {}",
                        std::io::Error::last_os_error()
                    )));
                }
                base.cast::<u8>()
            };
            let frames = (0..count)
                .map(|i| Frame {
                    owner: SwipOwner::default(),
                    releases: &*releases,
                    pid: AtomicU64::new(0),
                    state: AtomicU8::new(FREE),
                    dirty: AtomicBool::new(false),
                    changes: AtomicU64::new(0),
                    loaded: Mutex::new(None),
                    failure: Mutex::new(None),
                    writing: Mutex::new(None),
                    class,
                    index: i as u32,
                    // SAFETY: inside the mapping.
                    data: unsafe { base.add(i * class.size()) },
                    parent_swip: AtomicPtr::new(ptr::null_mut()),
                    parent: AtomicPtr::new(ptr::null_mut()),
                })
                .collect::<Box<[_]>>();
            regions.push(Region {
                base,
                len,
                frames,
                free: Mutex::new((0..count as u32).rev().collect()),
            });
        }
        Ok(Self {
            io,
            store,
            wal,
            budget,
            cooling_target: (budget as f64 * cooling_share) as usize,
            used: AtomicUsize::new(0),
            regions,
            cooling: Mutex::new(Cooling::default()),
            releases,
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

    pub fn store(&self) -> &Arc<PageStore> {
        &self.store
    }

    /// Creates a page of `class` and points `swip`, which must be empty, at it.
    pub fn allocate<'a>(
        &'a self,
        parent: &SwipOwner,
        swip: &Swip,
        class: SizeClass,
    ) -> Result<IOResult<ExclusiveGuard<'a>>> {
        let frame = match self.take_frame(class)? {
            IOResult::Done(frame) => frame,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let _swips = parent.swizzle.lock();
        assert_eq!(
            swip.load(Ordering::Relaxed),
            NO_CHILD,
            "allocating over a child"
        );
        frame.bytes_mut().fill(0);
        frame
            .pid
            .store(self.store.allocate(class).raw(), Ordering::Relaxed);
        frame.dirty.store(true, Ordering::Relaxed);
        assert!(
            frame.owner.latch.try_lock_exclusive(),
            "a free frame is unlatched"
        );
        self.attach(frame, HOT, parent, swip);
        Ok(IOResult::Done(ExclusiveGuard {
            frame,
            touched: true,
        }))
    }

    /// The child `swip` points at, latched shared. The caller holds `parent` at least shared.
    pub fn fix_shared<'a>(
        &'a self,
        parent: &SwipOwner,
        swip: &Swip,
    ) -> Result<IOResult<SharedGuard<'a>>> {
        let frame = match self.resolve(parent, swip)? {
            IOResult::Done(frame) => frame,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        if !frame.owner.latch.try_lock_shared() {
            return yield_now();
        }
        if swip.load(Ordering::Acquire) != swizzled(frame) {
            frame.owner.latch.unlock_shared();
            return yield_now();
        }
        Ok(IOResult::Done(SharedGuard { frame }))
    }

    /// The child `swip` points at, latched exclusively. The caller holds `parent` at least shared.
    pub fn fix_exclusive<'a>(
        &'a self,
        parent: &SwipOwner,
        swip: &Swip,
    ) -> Result<IOResult<ExclusiveGuard<'a>>> {
        let frame = match self.resolve(parent, swip)? {
            IOResult::Done(frame) => frame,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        if !frame.owner.latch.try_lock_exclusive() {
            return yield_now();
        }
        if swip.load(Ordering::Acquire) != swizzled(frame) {
            frame.owner.latch.unlock_exclusive();
            return yield_now();
        }
        Ok(IOResult::Done(ExclusiveGuard {
            frame,
            touched: false,
        }))
    }

    /// The frame `swip` points at, once its page is in memory. Not latched: the caller latches it
    /// and checks that `swip` still points at it.
    fn resolve(&self, parent: &SwipOwner, swip: &Swip) -> Result<IOResult<&Frame>> {
        let word = swip.load(Ordering::Acquire);
        assert_ne!(word, NO_CHILD, "following an empty swip");
        if word & 1 == 0 {
            // SAFETY: a swizzled swip holds the address of one of our frames.
            let frame = unsafe { &*(word as *const Frame) };
            return match frame.state.load(Ordering::Acquire) {
                HOT => Ok(IOResult::Done(frame)),
                LOADING => match frame.loaded.lock().clone() {
                    Some(c) => wait_on(c),
                    // Landed between the two reads.
                    None => yield_now(),
                },
                FAILED => Err(self.unload_failed(frame, parent, swip)),
                // Cooled, evicted or reused since `word` was read.
                _ => yield_now(),
            };
        }
        let pid = PageId::from_raw(word >> 1);
        {
            let _swips = parent.swizzle.lock();
            if swip.load(Ordering::Acquire) != word {
                return yield_now();
            }
            if let Some(frame) = self.take_cooling(pid) {
                self.cool_hits.fetch_add(1, Ordering::Relaxed);
                self.attach(frame, HOT, parent, swip);
                return Ok(IOResult::Done(frame));
            }
        }
        // Room first, without the parent's lock: making it may cool the parent's other children.
        let frame = match self.take_frame(pid.class())? {
            IOResult::Done(frame) => frame,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let loaded = Completion::new_wait();
        {
            let _swips = parent.swizzle.lock();
            if swip.load(Ordering::Acquire) != word {
                self.release(frame);
                return yield_now();
            }
            if let Some(cooled) = self.take_cooling(pid) {
                self.release(frame);
                self.cool_hits.fetch_add(1, Ordering::Relaxed);
                self.attach(cooled, HOT, parent, swip);
                return Ok(IOResult::Done(cooled));
            }
            frame.pid.store(pid.raw(), Ordering::Relaxed);
            frame.dirty.store(false, Ordering::Relaxed);
            *frame.loaded.lock() = Some(loaded.clone());
            self.attach(frame, LOADING, parent, swip);
        }
        let target = FramePtr(frame);
        let landed = loaded.clone();
        let read = self.store.read(pid, move |read| {
            let frame = target.get();
            match read {
                PageRead::Valid(buf) => {
                    frame.bytes_mut().copy_from_slice(buf.as_slice());
                    frame.state.store(HOT, Ordering::Release);
                    frame.loaded.lock().take();
                    landed.complete(0);
                }
                PageRead::Invalid => Self::fail_load(
                    frame,
                    CompletionError::PageCodecError {
                        page_idx: pid.raw() as usize,
                    },
                    &landed,
                ),
                PageRead::Failed(e) => Self::fail_load(frame, e, &landed),
            }
        });
        if let Err(e) = read {
            Self::fail_load(frame, CompletionError::Aborted, &loaded);
            drop(self.unload_failed(frame, parent, swip));
            return Err(e);
        }
        if loaded.finished() {
            return match frame.state.load(Ordering::Acquire) {
                HOT => Ok(IOResult::Done(frame)),
                _ => Err(self.unload_failed(frame, parent, swip)),
            };
        }
        wait_on(loaded)
    }

    fn fail_load(frame: &Frame, err: CompletionError, loaded: &Completion) {
        *frame.failure.lock() = Some(err);
        frame.state.store(FAILED, Ordering::Release);
        frame.loaded.lock().take();
        loaded.error(err);
    }

    /// Takes a page whose load failed back out of its parent; the error is what the caller gets.
    fn unload_failed(&self, frame: &Frame, parent: &SwipOwner, swip: &Swip) -> LimboError {
        let err = frame.failure.lock().unwrap_or(CompletionError::Aborted);
        let _swips = parent.swizzle.lock();
        if swip.load(Ordering::Acquire) == swizzled(frame)
            && frame.state.load(Ordering::Acquire) == FAILED
        {
            swip.store(unswizzled(frame.pid()), Ordering::Release);
            Self::detach(frame, parent, swip);
            self.release(frame);
        }
        LimboError::CompletionError(err)
    }

    /// The cooling frame holding `pid`, taken out of the cooling stage.
    fn take_cooling(&self, pid: PageId) -> Option<&Frame> {
        let mut cooling = self.cooling.lock();
        let frame = cooling.pages.remove(&pid)?;
        cooling.bytes -= pid.class().size();
        // SAFETY: cooling frames are ours.
        Some(unsafe { &*frame.0 })
    }

    /// Makes `frame` the child behind `swip`. The caller holds `parent.swizzle`.
    fn attach(&self, frame: &Frame, state: u8, parent: &SwipOwner, swip: &Swip) {
        frame.state.store(state, Ordering::Release);
        frame
            .parent_swip
            .store(ptr::from_ref(swip).cast_mut(), Ordering::Relaxed);
        frame
            .parent
            .store(ptr::from_ref(parent).cast_mut(), Ordering::Relaxed);
        parent.children.fetch_add(1, Ordering::Relaxed);
        parent.swizzled.lock().push(ptr::from_ref(swip) as usize);
        swip.store(swizzled(frame), Ordering::Release);
    }

    /// Undoes `attach`'s bookkeeping on the parent. The caller holds `parent.swizzle` and has
    /// unswizzled `swip`.
    fn detach(_frame: &Frame, parent: &SwipOwner, swip: &Swip) {
        parent.children.fetch_sub(1, Ordering::Relaxed);
        let mut swizzled = parent.swizzled.lock();
        let at = swizzled
            .iter()
            .position(|&a| a == ptr::from_ref(swip) as usize);
        swizzled.swap_remove(at.expect("a swizzled swip is listed"));
    }

    /// A free frame of `class`, with its page's bytes counted against the budget.
    fn take_frame(&self, class: SizeClass) -> Result<IOResult<&Frame>> {
        let region = &self.regions[class.index()];
        if region.frames.is_empty() {
            return Err(LimboError::InternalError(format!(
                "{} byte pages are larger than the {} byte buffer pool",
                class.size(),
                self.budget
            )));
        }
        match self.make_room(class.size())? {
            IOResult::Done(()) => {}
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
        let index = region
            .free
            .lock()
            .pop()
            .expect("bytes under the budget always leave a frame of every class free");
        Ok(IOResult::Done(&region.frames[index as usize]))
    }

    fn release(&self, frame: &Frame) {
        let len = frame.class.size();
        // SAFETY: the frame holds no page any more; its memory goes back to the kernel.
        unsafe { libc::madvise(frame.data.cast(), len, libc::MADV_DONTNEED) };
        frame.state.store(FREE, Ordering::Release);
        frame.dirty.store(false, Ordering::Relaxed);
        frame.failure.lock().take();
        frame.parent_swip.store(ptr::null_mut(), Ordering::Relaxed);
        frame.parent.store(ptr::null_mut(), Ordering::Relaxed);
        self.regions[frame.class.index()]
            .free
            .lock()
            .push(frame.index);
        self.used.fetch_sub(len, Ordering::Relaxed);
    }

    /// Counts `len` more bytes against the budget, evicting until they fit.
    fn make_room(&self, len: usize) -> Result<IOResult<()>> {
        let mut writebacks: Vec<Writeback> = Vec::new();
        let outcome = self.make_room_locked(len, &mut writebacks);
        // Write-backs are submitted only now that no lock is held: a backend may finish them
        // inside the submission.
        for w in writebacks {
            self.submit_writeback(w)?;
        }
        outcome
    }

    fn make_room_locked(
        &self,
        len: usize,
        writebacks: &mut Vec<Writeback>,
    ) -> Result<IOResult<()>> {
        let mut cooling = self.cooling.lock();
        loop {
            let used = self.used.load(Ordering::Relaxed);
            if used + len <= self.budget {
                self.used.fetch_add(len, Ordering::Relaxed);
                if used + len + self.cooling_target > self.budget
                    && cooling.bytes < self.cooling_target
                {
                    self.cool(&mut cooling, self.cooling_target, writebacks);
                }
                return Ok(IOResult::Done(()));
            }
            match self.evict_one(&mut cooling, writebacks) {
                Evict::Freed => continue,
                Evict::Wait(c) => return wait_on(c),
                Evict::None => {}
            }
            // Arm the wait before the last attempt, so a latch let go after it still wakes us.
            let released = self.releases.next();
            let target = cooling.bytes + len;
            self.cool(&mut cooling, target, writebacks);
            match self.evict_one(&mut cooling, writebacks) {
                Evict::Freed => continue,
                Evict::Wait(c) => return wait_on(c),
                Evict::None => return wait_on(released),
            }
        }
    }

    /// Cools pages until `target` bytes are cooling or a sweep of every frame found no more,
    /// queueing a write-back for each dirty page cooled whose log is durable.
    fn cool(&self, cooling: &mut Cooling, target: usize, writebacks: &mut Vec<Writeback>) {
        let total: usize = self.regions.iter().map(|r| r.frames.len()).sum();
        let mut scanned = 0;
        while cooling.bytes < target && scanned < total {
            scanned += 1;
            cooling.hand = (cooling.hand + 1) % total;
            let frame = self.frame_at(cooling.hand);
            if frame.state.load(Ordering::Acquire) != HOT
                || frame.owner.children.load(Ordering::Relaxed) != 0
                || !frame.owner.latch.try_lock_exclusive()
            {
                continue;
            }
            let cooled = self.try_unswizzle(frame);
            if cooled {
                frame.state.store(COOL, Ordering::Release);
                cooling.fifo.push_back(FramePtr(frame));
                cooling.pages.insert(frame.pid(), FramePtr(frame));
                cooling.bytes += frame.class.size();
                self.cooled.fetch_add(1, Ordering::Relaxed);
            }
            frame.owner.latch.unlock_exclusive();
            if cooled {
                writebacks.extend(self.writeback_copy(frame));
            }
        }
    }

    fn frame_at(&self, mut at: usize) -> &Frame {
        for region in &self.regions {
            if at < region.frames.len() {
                return &region.frames[at];
            }
            at -= region.frames.len();
        }
        unreachable!("the hand is inside the frames")
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
        // A writer holding the parent may be rewriting the bytes the swip lives in.
        if !parent.latch.try_lock_shared() {
            return false;
        }
        // SAFETY: as for the parent.
        let swip = unsafe { &*frame.parent_swip.load(Ordering::Relaxed) };
        let done = swip.load(Ordering::Acquire) == swizzled(frame);
        if done {
            swip.store(unswizzled(frame.pid()), Ordering::Release);
            Self::detach(frame, parent, swip);
        }
        parent.latch.unlock_shared();
        done
    }

    /// A copy of a dirty, unlatched page to write back, unless one is in flight.
    fn writeback_copy(&self, frame: &Frame) -> Option<Writeback> {
        if !frame.dirty.load(Ordering::Acquire) || frame.writing.lock().is_some() {
            return None;
        }
        if !frame.owner.latch.try_lock_shared() {
            return None;
        }
        let buf = Arc::new(Buffer::new_temporary(frame.class.size()));
        buf.as_mut_slice().copy_from_slice(frame.bytes());
        // Under the latch: the copy holds exactly the changes counted so far.
        frame.writing.lock().replace(Completion::new_wait());
        let copy = Writeback {
            frame: FramePtr(frame),
            lsn: page::lsn(&buf.as_slice()[..HEADER_SIZE]),
            buf,
            changes: frame.changes.load(Ordering::Acquire),
        };
        frame.owner.latch.unlock_shared();
        Some(copy)
    }

    fn submit_writeback(&self, w: Writeback) -> Result<()> {
        let Writeback {
            frame,
            buf,
            changes,
            lsn,
        } = w;
        let pid = frame.get().pid();
        let wal = self.wal.clone();
        let written = self.store.write(pid, buf, move |res| {
            let frame = frame.get();
            let res = res.and_then(|slot| match &wal {
                // After every record the copy holds, which were appended before its LSN.
                Some(wal) => wal
                    .append(&[Record::Remap { pid, slot, lsn }])
                    .map(|_| ())
                    .map_err(|_| {
                        CompletionError::IOError(std::io::ErrorKind::Other, "the log failed")
                    }),
                None => Ok(()),
            });
            if res.is_ok() && frame.changes.load(Ordering::Acquire) == changes {
                frame.dirty.store(false, Ordering::Release);
            }
            if let Some(c) = frame.writing.lock().take() {
                match res {
                    Ok(()) => c.complete(0),
                    Err(e) => c.error(e),
                }
            }
        });
        if let Err(e) = written {
            if let Some(c) = frame.get().writing.lock().take() {
                c.abort();
            }
            return Err(e);
        }
        Ok(())
    }

    /// Frees the oldest cooling page, or says what to wait for.
    fn evict_one(&self, cooling: &mut Cooling, writebacks: &mut Vec<Writeback>) -> Evict {
        let mut wait = None;
        for _ in 0..cooling.fifo.len() {
            let ptr = cooling.fifo.pop_front().expect("counted");
            let frame = ptr.get();
            if frame.state.load(Ordering::Acquire) != COOL
                || cooling.pages.get(&frame.pid()).map(|p| p.0) != Some(ptr.0)
            {
                // Swizzled again since it was queued.
                continue;
            }
            if let Some(c) = frame.writing.lock().clone() {
                cooling.fifo.push_back(ptr);
                wait.get_or_insert(c);
                continue;
            }
            if frame.dirty.load(Ordering::Acquire) {
                cooling.fifo.push_back(ptr);
                if let Some(w) = self.writeback_copy(frame) {
                    if let Some(c) = frame.writing.lock().clone() {
                        wait.get_or_insert(c);
                    }
                    writebacks.push(w);
                }
                continue;
            }
            if !frame.owner.latch.try_lock_exclusive() {
                // Someone that found it just before it cooled is letting go.
                cooling.fifo.push_back(ptr);
                continue;
            }
            cooling.pages.remove(&frame.pid());
            cooling.bytes -= frame.class.size();
            frame.owner.latch.unlock_exclusive();
            self.release(frame);
            self.evicted.fetch_add(1, Ordering::Relaxed);
            return Evict::Freed;
        }
        match wait {
            Some(c) => Evict::Wait(c),
            None => Evict::None,
        }
    }

    /// Cools and writes back everything that can go, as pressure eventually would.
    #[cfg(test)]
    pub(crate) fn evict_everything(&self) -> Result<IOResult<()>> {
        loop {
            let mut writebacks: Vec<Writeback> = Vec::new();
            let step = {
                let mut cooling = self.cooling.lock();
                self.cool(&mut cooling, usize::MAX, &mut writebacks);
                self.evict_one(&mut cooling, &mut writebacks)
            };
            for w in writebacks {
                self.submit_writeback(w)?;
            }
            match step {
                Evict::Freed => continue,
                Evict::Wait(c) => return wait_on(c),
                Evict::None => return Ok(IOResult::Done(())),
            }
        }
    }
}

/// A page's write-back, prepared under the cooling lock and submitted after it.
struct Writeback {
    frame: FramePtr,
    buf: Arc<Buffer>,
    /// The frame's change count the copy holds: the frame is clean afterwards only if it held.
    changes: u64,
    /// The LSN of the last change the copy holds.
    lsn: u64,
}

impl Drop for BufferManager {
    /// Waits for the IO in flight: its callbacks write into the frames.
    fn drop(&mut self) {
        let mut pending = Vec::new();
        for region in &self.regions {
            for frame in region.frames.iter() {
                pending.extend(frame.loaded.lock().clone());
                pending.extend(frame.writing.lock().clone());
            }
        }
        if let Err(e) = self.io.drain_completions(&pending) {
            tracing::error!("buffer manager dropped with IO that did not finish: {e}");
        }
    }
}

enum Evict {
    Freed,
    Wait(Completion),
    None,
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
        self.frame.releases().released();
    }
}

pub struct ExclusiveGuard<'a> {
    frame: &'a Frame,
    touched: bool,
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

    /// The bytes in `range` as they must be logged or written: page ids in every swip.
    pub fn durable_bytes(&self, range: std::ops::Range<usize>) -> Vec<u8> {
        self.frame.durable_bytes(range)
    }

    /// Marks the page changed without writing to it: one of its swips now names a new child.
    pub fn mark_dirty(&mut self) {
        self.touched = true;
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
        self.touched = true;
        self.frame.bytes_mut()
    }
}

impl Drop for ExclusiveGuard<'_> {
    fn drop(&mut self) {
        if self.touched {
            self.frame.changes.fetch_add(1, Ordering::AcqRel);
            self.frame.dirty.store(true, Ordering::Release);
        }
        self.frame.owner.latch.unlock_exclusive();
        self.frame.releases().released();
    }
}

#[cfg(test)]
mod tests;
