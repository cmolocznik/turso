//! The pool's memory: one region carved into 2 MiB batches, each lent to one size class at a time.
//!
//! A batch holds frames of its class: 512 frames of 4 KiB, 256 of 8 KiB, down to one of 2 MiB. A
//! 4 MiB or 8 MiB frame takes an aligned run of 2 or 4 batches, handed out by a buddy allocator. A
//! frame is named by the 4 KiB grain it starts at, so a frame index names the same memory whichever
//! class holds it, and one array of headers serves every class.
//!
//! The region is backed by 2 MiB huge pages where the OS allows. Its memory is never unmapped: a
//! freed frame is reused as it is, and memory moves between classes a whole batch at a time, so no
//! huge page is ever split. A batch moves once every frame in it is free: the pool marks it
//! draining, which keeps new frames out of it, and moves or evicts what it holds.
//!
//! Untouched memory costs nothing: the region, the frame headers and the hash table are mapped
//! without reserving physical memory, which the OS commits on first touch.

use super::super::page::{SizeClass, CLASS_COUNT, MIN_PAGE_SHIFT, MIN_PAGE_SIZE};
use crate::io::{Buffer, RegionBuffer};
use crate::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use crate::sync::{Arc, Mutex};
use crate::{LimboError, Result};
use std::collections::BTreeSet;
use std::ptr::NonNull;

/// log2 of the batch size.
pub const BATCH_SHIFT: u32 = 21;
/// A batch: 2 MiB, the huge page size of x86-64 and of aarch64 with 4 KiB base pages.
pub const BATCH: usize = 1 << BATCH_SHIFT;
/// Grains of 4 KiB in a batch, which is also the most frames a batch holds.
pub const GRAINS: u32 = (BATCH / MIN_PAGE_SIZE) as u32;
/// The class whose frames are exactly one batch.
const BATCH_CLASS: usize = (BATCH_SHIFT - MIN_PAGE_SHIFT) as usize;
/// Runs of 1, 2 and 4 batches: the largest class, 8 MiB, takes 4.
const ORDERS: usize = CLASS_COUNT - BATCH_CLASS;
/// Free-frame bitmap words per batch.
const WORDS: usize = GRAINS as usize / 64;
/// `owner` of a batch that continues a run begun by an earlier batch.
const CONTINUED: u8 = u8::MAX;

/// The smallest budget a pool takes: one frame of the largest class and one batch beside it, so a
/// large value and the tree pages leading to it fit together.
pub const MIN_BUDGET: usize = (1 << (ORDERS - 1)) * BATCH + BATCH;

/// The order of the runs a class's frames take: 0 for one batch, up to `ORDERS - 1`.
fn order(class: SizeClass) -> usize {
    class.index().saturating_sub(BATCH_CLASS)
}

/// Frames of `class` in one batch, or one per run for classes of a batch or more.
pub fn frames_per_batch(class: SizeClass) -> u32 {
    GRAINS >> class.index().min(BATCH_CLASS)
}

/// The frame at position `i` of the batch (or run) starting at `batch`.
pub fn frame_at(class: SizeClass, batch: u32, i: u32) -> u32 {
    batch * GRAINS + (i << class.index().min(BATCH_CLASS))
}

/// The batch holding `frame`'s first byte, and the frame's position in it.
fn position(class: SizeClass, frame: u32) -> (u32, u32) {
    (
        frame / GRAINS,
        (frame % GRAINS) >> class.index().min(BATCH_CLASS),
    )
}

/// Types whose all-zero bytes are a valid value, so arrays of them can sit in memory the OS hands
/// out zeroed and commits only when touched.
///
/// # Safety
///
/// Every field must be valid as zeros.
pub unsafe trait Zeroable: Default {}

unsafe impl Zeroable for AtomicU8 {}
unsafe impl Zeroable for AtomicU32 {}
unsafe impl Zeroable for AtomicU64 {}

/// An array of `T` that starts zeroed and takes physical memory only where it is touched.
pub struct Zeroed<T: Zeroable> {
    #[cfg(not(shuttle))]
    map: Mapping,
    #[cfg(not(shuttle))]
    len: usize,
    #[cfg(not(shuttle))]
    _marker: std::marker::PhantomData<T>,
    // Shuttle's atomics are not valid as zeros: build them instead.
    #[cfg(shuttle)]
    items: Box<[T]>,
}

// SAFETY: a `Zeroed<T>` hands out only `&T`, as a `Box<[T]>` would.
unsafe impl<T: Zeroable + Sync> Sync for Zeroed<T> {}
unsafe impl<T: Zeroable + Send> Send for Zeroed<T> {}

impl<T: Zeroable> Zeroed<T> {
    pub fn new(len: usize) -> Result<Self> {
        #[cfg(not(shuttle))]
        {
            let bytes = (len * std::mem::size_of::<T>()).max(1);
            let align = std::mem::align_of::<T>().max(MIN_PAGE_SIZE);
            Ok(Self {
                map: Mapping::new(bytes.next_multiple_of(MIN_PAGE_SIZE), align, false)?,
                len,
                _marker: std::marker::PhantomData,
            })
        }
        #[cfg(shuttle)]
        Ok(Self {
            items: (0..len).map(|_| T::default()).collect(),
        })
    }
}

impl<T: Zeroable> std::ops::Deref for Zeroed<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        #[cfg(not(shuttle))]
        // SAFETY: the mapping holds `len` zero-initialized `T`s, valid as zeros, and is aligned
        // for `T`; only shared references are handed out.
        unsafe {
            std::slice::from_raw_parts(self.map.start.as_ptr().cast::<T>(), self.len)
        }
        #[cfg(shuttle)]
        &self.items
    }
}

/// Anonymous memory, zeroed, mapped without reserving physical memory.
struct Mapping {
    start: NonNull<u8>,
    raw: *mut u8,
    raw_len: usize,
    #[cfg(not(all(unix, not(miri))))]
    layout: std::alloc::Layout,
}

impl Mapping {
    fn new(len: usize, align: usize, huge: bool) -> Result<Self> {
        #[cfg(all(unix, not(miri)))]
        {
            let raw_len = len + align;
            #[cfg(target_os = "linux")]
            let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;
            #[cfg(not(target_os = "linux"))]
            let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
            // SAFETY: a fresh anonymous mapping that nothing else refers to.
            let raw = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    raw_len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    flags,
                    -1,
                    0,
                )
            };
            if raw == libc::MAP_FAILED {
                return Err(LimboError::InternalError(format!(
                    "mapping {len} bytes for the buffer pool: {}",
                    std::io::Error::last_os_error()
                )));
            }
            let raw = raw.cast::<u8>();
            let start = (raw as usize).next_multiple_of(align) as *mut u8;
            #[cfg(target_os = "linux")]
            if huge {
                // Huge pages only make the pool faster, so a refusal leaves 4 KiB pages.
                // SAFETY: inside the mapping just made.
                unsafe { libc::madvise(start.cast(), len, libc::MADV_HUGEPAGE) };
            }
            #[cfg(not(target_os = "linux"))]
            let _ = huge;
            Ok(Self {
                start: NonNull::new(start).expect("a mapping is never at address zero"),
                raw,
                raw_len,
            })
        }
        #[cfg(not(all(unix, not(miri))))]
        {
            let _ = huge;
            let layout = std::alloc::Layout::from_size_align(len, align)
                .map_err(|e| LimboError::InternalError(format!("buffer pool layout: {e}")))?;
            // SAFETY: the layout has a non-zero size.
            let raw = unsafe { std::alloc::alloc_zeroed(layout) };
            let start = NonNull::new(raw).ok_or_else(|| {
                LimboError::InternalError(format!("allocating {len} bytes for the buffer pool"))
            })?;
            Ok(Self {
                start,
                raw,
                raw_len: len,
                layout,
            })
        }
    }
}

// SAFETY: the mapping is plain memory owned by this value.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Drop for Mapping {
    fn drop(&mut self) {
        #[cfg(all(unix, not(miri)))]
        // SAFETY: the mapping made in `new`.
        unsafe {
            libc::munmap(self.raw.cast(), self.raw_len);
        }
        #[cfg(not(all(unix, not(miri))))]
        // SAFETY: allocated in `new` with this layout.
        unsafe {
            std::alloc::dealloc(self.raw, self.layout);
        }
        let _ = self.raw_len;
    }
}

/// The frames' memory.
pub struct Region {
    map: Mapping,
    len: usize,
}

impl Region {
    fn new(len: usize) -> Result<Self> {
        Ok(Self {
            map: Mapping::new(len, BATCH, true)?,
            len,
        })
    }

    /// The first byte of `frame`.
    pub fn ptr(&self, frame: u32) -> *mut u8 {
        let at = frame as usize * MIN_PAGE_SIZE;
        debug_assert!(at < self.len);
        // SAFETY: inside the region.
        unsafe { self.map.start.as_ptr().add(at) }
    }

    /// `frame`'s `len` bytes as an IO buffer that keeps the region alive.
    pub fn buffer(self: &Arc<Self>, frame: u32, len: usize) -> Arc<Buffer> {
        assert!(frame as usize * MIN_PAGE_SIZE + len <= self.len);
        // SAFETY: inside the region, which the buffer keeps alive. The caller holds the frame
        // exclusively while an IO into it is in flight.
        let region = unsafe {
            RegionBuffer::new(self.clone(), NonNull::new_unchecked(self.ptr(frame)), len)
        };
        Arc::new(Buffer::new_region(region))
    }
}

/// A class's batches.
#[derive(Default)]
struct ClassBatches {
    /// Every batch (first batch of each run) the class holds.
    all: Vec<u32>,
    /// Those not draining: what eviction samples, and what a drain chooses from. A draining
    /// batch's freed frames are leaving, and would make the class look as if it had room.
    live: Vec<u32>,
    /// Those with a free frame and not draining, lowest first. Taking frames from the lowest
    /// leaves the highest batches emptier, and so cheaper to drain.
    partial: BTreeSet<u32>,
}

/// Free runs of batches by order, as a buddy allocator.
struct Runs {
    free: [BTreeSet<u32>; ORDERS],
    /// A run being emptied for a class of a larger order: batches freed inside it wait here
    /// until the whole run is free, and it then goes to that class, so no other class takes them
    /// meanwhile.
    reserved: Option<Reserved>,
}

struct Reserved {
    start: u32,
    order: usize,
    missing: u32,
    class: SizeClass,
}

impl Reserved {
    fn holds(&self, batch: u32) -> bool {
        (self.start..self.start + (1 << self.order)).contains(&batch)
    }
}

impl Runs {
    fn new(batches: u32) -> Self {
        let mut free: [BTreeSet<u32>; ORDERS] = Default::default();
        let mut start = 0;
        while start < batches {
            let order = (0..ORDERS)
                .rev()
                .find(|&o| start % (1 << o) == 0 && start + (1 << o) <= batches)
                .expect("order 0 always fits");
            free[order].insert(start);
            start += 1 << order;
        }
        Self {
            free,
            reserved: None,
        }
    }

    fn take(&mut self, order: usize) -> Option<u32> {
        let from = (order..ORDERS).find(|&o| !self.free[o].is_empty())?;
        let start = self.free[from].pop_first().expect("non-empty");
        for o in (order..from).rev() {
            self.free[o].insert(start + (1 << o));
        }
        Some(start)
    }

    /// Returns a run to the free lists, or to the reserved run if it lies in it or holds it.
    /// Returns the reserved run's class and first batch once it is whole: it goes to that class,
    /// not to the free lists.
    fn give(&mut self, start: u32, order: usize) -> Option<(SizeClass, u32)> {
        if let Some(r) = &mut self.reserved {
            // Aligned runs either nest or do not meet.
            if r.holds(start) && order <= r.order {
                r.missing -= 1 << order;
                if r.missing > 0 {
                    return None;
                }
                let r = self.reserved.take().expect("reserved");
                return Some((r.class, r.start));
            }
            if order > r.order && (start..start + (1 << order)).contains(&r.start) {
                let r = self.reserved.take().expect("reserved");
                self.give_around(start, order, r.start, r.order);
                return Some((r.class, r.start));
            }
        }
        self.coalesce(start, order);
        None
    }

    /// Frees the run at `start` of `order` but for the aligned run at `keep` of `keep_order`
    /// inside it: the halves not holding `keep`, down to its order.
    fn give_around(&mut self, mut start: u32, mut order: usize, keep: u32, keep_order: usize) {
        while order > keep_order {
            order -= 1;
            let upper = start + (1 << order);
            let (kept, other) = if keep >= upper {
                (upper, start)
            } else {
                (start, upper)
            };
            // Its buddy is kept, so it merges with nothing.
            self.free[order].insert(other);
            start = kept;
        }
    }

    fn coalesce(&mut self, mut start: u32, mut order: usize) {
        while order + 1 < ORDERS {
            let buddy = start ^ (1 << order);
            if !self.free[order].remove(&buddy) {
                break;
            }
            start = start.min(buddy);
            order += 1;
        }
        self.free[order].insert(start);
    }

    /// Holds the aligned run of `class`'s order around `batch` for `class`: its free batches leave
    /// the free lists, and the rest join it as they are freed. Returns its first batch, and
    /// whether it is whole already; `None` while another run is held.
    fn reserve(&mut self, batch: u32, class: SizeClass) -> Option<(u32, bool)> {
        if self.reserved.is_some() {
            return None;
        }
        let order = order(class);
        let start = batch & !((1u32 << order) - 1);
        let end = start + (1 << order);
        // A free run holding the whole of it.
        for o in order..ORDERS {
            let s = start & !((1u32 << o) - 1);
            if self.free[o].remove(&s) {
                self.give_around(s, o, start, order);
                return Some((start, true));
            }
        }
        let mut free = 0;
        for o in 0..order {
            let inside: Vec<u32> = self.free[o].range(start..end).copied().collect();
            for s in inside {
                self.free[o].remove(&s);
                free += 1 << o;
            }
        }
        // Free runs coalesce, so a run wholly free is one run, found above.
        debug_assert!(free < 1 << order);
        self.reserved = Some(Reserved {
            start,
            order,
            missing: (1 << order) - free,
            class,
        });
        Some((start, false))
    }
}

/// Which frames are free, and which class holds each batch.
pub struct Memory {
    pub region: Arc<Region>,
    pub batches: u32,
    /// Per batch: 0 if free, 1 + the class for a run's first batch, [`CONTINUED`] for the rest.
    owner: Zeroed<AtomicU8>,
    /// Per batch, [`WORDS`] words: a set bit is a free frame. Changed under the owner's lock.
    free: Zeroed<AtomicU64>,
    /// Per batch: frames taken. Changed under the owner's lock.
    taken: Zeroed<AtomicU32>,
    /// Per batch: 1 while draining. Changed under the owner's lock.
    draining: Zeroed<AtomicU8>,
    /// Per draining batch: 1 + the class it is drained for, which it goes to once empty; 0 for
    /// none. Without it, the class it is drained from could take it back first, and two classes
    /// short of memory could drain each other's batches indefinitely.
    beneficiary: Zeroed<AtomicU8>,
    classes: [Mutex<ClassBatches>; CLASS_COUNT],
    /// Locked before any class's lock.
    runs: Mutex<Runs>,
}

impl Memory {
    pub fn new(budget: usize) -> Result<Self> {
        if budget < MIN_BUDGET {
            return Err(LimboError::InternalError(format!(
                "a buffer pool of {budget} bytes is below the minimum of {MIN_BUDGET}"
            )));
        }
        let batches = u32::try_from(budget / BATCH)
            .ok()
            .filter(|&b| (b as u64) * (GRAINS as u64) < u32::MAX as u64)
            .ok_or_else(|| {
                LimboError::InternalError(format!("a buffer pool of {budget} bytes is too large"))
            })?;
        Ok(Self {
            region: Arc::new(Region::new(batches as usize * BATCH)?),
            batches,
            owner: Zeroed::new(batches as usize)?,
            free: Zeroed::new(batches as usize * WORDS)?,
            taken: Zeroed::new(batches as usize)?,
            draining: Zeroed::new(batches as usize)?,
            beneficiary: Zeroed::new(batches as usize)?,
            classes: Default::default(),
            runs: Mutex::new(Runs::new(batches)),
        })
    }

    /// Frames the region holds, at one per 4 KiB.
    pub fn grains(&self) -> u32 {
        self.batches * GRAINS
    }

    /// The class holding `frame`'s batch, while the frame is taken.
    pub fn class_of(&self, frame: u32) -> SizeClass {
        let owner = self.owner[(frame / GRAINS) as usize].load(Ordering::Acquire);
        debug_assert!(
            owner != 0 && owner != CONTINUED,
            "frame {frame} is in no class"
        );
        SizeClass::new(owner as usize - 1).expect("a class")
    }

    fn words(&self, batch: u32) -> &[AtomicU64] {
        &self.free[batch as usize * WORDS..(batch as usize + 1) * WORDS]
    }

    /// A free frame of `class` from a batch it holds, if any.
    pub fn take(&self, class: SizeClass) -> Option<u32> {
        let n = frames_per_batch(class);
        let mut c = self.classes[class.index()].lock();
        while let Some(&batch) = c.partial.first() {
            let words = self.words(batch);
            let found = words.iter().enumerate().find_map(|(w, word)| {
                let bits = word.load(Ordering::Relaxed);
                (bits != 0).then(|| (w, bits.trailing_zeros()))
            });
            let Some((w, bit)) = found else {
                c.partial.remove(&batch);
                continue;
            };
            let i = w as u32 * 64 + bit;
            debug_assert!(i < n);
            let left = words[w].fetch_and(!(1 << bit), Ordering::Relaxed) & !(1 << bit);
            self.taken[batch as usize].fetch_add(1, Ordering::Relaxed);
            if left == 0 && words.iter().all(|w| w.load(Ordering::Relaxed) == 0) {
                c.partial.remove(&batch);
            }
            return Some(frame_at(class, batch, i));
        }
        None
    }

    /// Lends a free run to `class`; false if there is none.
    pub fn grow(&self, class: SizeClass) -> bool {
        let mut runs = self.runs.lock();
        let Some(batch) = runs.take(order(class)) else {
            return false;
        };
        self.assign(class, batch);
        true
    }

    /// Gives the free run at `batch` to `class`, with every frame free. The runs' lock is held.
    fn assign(&self, class: SizeClass, batch: u32) {
        let mut c = self.classes[class.index()].lock();
        for b in batch + 1..batch + (1 << order(class)) {
            self.owner[b as usize].store(CONTINUED, Ordering::Release);
        }
        self.owner[batch as usize].store(class.index() as u8 + 1, Ordering::Release);
        let n = frames_per_batch(class);
        for (w, word) in self.words(batch).iter().enumerate() {
            let below = n.saturating_sub(w as u32 * 64).min(64);
            let bits = if below == 64 {
                u64::MAX
            } else {
                (1 << below) - 1
            };
            word.store(bits, Ordering::Relaxed);
        }
        self.taken[batch as usize].store(0, Ordering::Relaxed);
        self.draining[batch as usize].store(0, Ordering::Relaxed);
        c.all.push(batch);
        c.live.push(batch);
        c.partial.insert(batch);
    }

    /// Returns `frame` of `class`. True if that emptied a draining batch, which then left the
    /// class.
    pub fn put(&self, class: SizeClass, frame: u32) -> bool {
        let (batch, i) = position(class, frame);
        let emptied = {
            let mut c = self.classes[class.index()].lock();
            debug_assert!(self.owns(class, batch), "frame {frame} is not {class:?}'s");
            let word = &self.words(batch)[i as usize / 64];
            let before = word.fetch_or(1 << (i % 64), Ordering::Relaxed);
            debug_assert_eq!(before & (1 << (i % 64)), 0, "frame {frame} freed twice");
            let taken = self.taken[batch as usize].fetch_sub(1, Ordering::Relaxed) - 1;
            if self.draining[batch as usize].load(Ordering::Relaxed) == 0 {
                c.partial.insert(batch);
                false
            } else {
                taken == 0
            }
        };
        if emptied {
            self.release(class, batch);
        }
        emptied
    }

    /// Whether `class` holds `batch`. Stable while the class's lock is held: a batch changes hands
    /// only under the lock of the class giving it up, and goes to a class only when no class
    /// holds it.
    fn owns(&self, class: SizeClass, batch: u32) -> bool {
        self.owner[batch as usize].load(Ordering::Acquire) == class.index() as u8 + 1
    }

    /// Hands an emptied draining batch to the run reserved around it, whatever class it was
    /// drained for, or else to that class, or back to the free runs.
    fn release(&self, class: SizeClass, batch: u32) {
        let mut runs = self.runs.lock();
        let beneficiary = {
            let mut c = self.classes[class.index()].lock();
            if !self.owns(class, batch)
                || self.draining[batch as usize].load(Ordering::Relaxed) == 0
                || self.taken[batch as usize].load(Ordering::Relaxed) != 0
            {
                return;
            }
            self.draining[batch as usize].store(0, Ordering::Relaxed);
            for word in self.words(batch) {
                word.store(0, Ordering::Relaxed);
            }
            c.all.retain(|&b| b != batch);
            debug_assert!(!c.live.contains(&batch));
            for b in batch..batch + (1 << order(class)) {
                self.owner[b as usize].store(0, Ordering::Release);
            }
            let b = self.beneficiary[batch as usize].swap(0, Ordering::Relaxed);
            SizeClass::new((b as usize).wrapping_sub(1)).filter(|b| order(*b) == order(class))
        };
        let reserved = runs.reserved.as_ref().is_some_and(|r| r.holds(batch));
        match beneficiary.filter(|_| !reserved) {
            Some(b) => self.assign(b, batch),
            None => {
                if let Some((to, start)) = runs.give(batch, order(class)) {
                    self.assign(to, start);
                }
            }
        }
    }

    /// Drains `batch` of `class` (as [`drain`](Self::drain)) for `beneficiary`, a class whose
    /// batches are one batch long, which gets it once it is empty.
    pub fn drain_for(&self, class: SizeClass, batch: u32, beneficiary: SizeClass) -> bool {
        debug_assert_eq!(order(beneficiary), 0);
        self.start_drain(class, batch, Some(beneficiary))
    }

    /// Stops new frames being taken from `batch` of `class`; it leaves the class once every frame
    /// in it is free. True if it was not draining already. False, and nothing done, if `class`
    /// no longer holds the batch: it was chosen before it left.
    #[cfg(test)]
    pub fn drain(&self, class: SizeClass, batch: u32) -> bool {
        self.start_drain(class, batch, None)
    }

    fn start_drain(&self, class: SizeClass, batch: u32, beneficiary: Option<SizeClass>) -> bool {
        let empty = {
            let mut c = self.classes[class.index()].lock();
            if !self.owns(class, batch) {
                return false;
            }
            if let Some(b) = beneficiary {
                // The first class to ask keeps it.
                let _ = self.beneficiary[batch as usize].compare_exchange(
                    0,
                    b.index() as u8 + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            }
            if self.draining[batch as usize].swap(1, Ordering::Relaxed) == 1 {
                return false;
            }
            c.partial.remove(&batch);
            c.live.retain(|&b| b != batch);
            self.taken[batch as usize].load(Ordering::Relaxed) == 0
        };
        if empty {
            self.release(class, batch);
        }
        true
    }

    /// Holds the aligned run of `class`'s order around `batch` for `class`, and drains every
    /// batch in it that a class holds: the run goes to `class` once it is free. False while
    /// another run is held.
    pub fn drain_run(&self, class: SizeClass, batch: u32) -> bool {
        let run = 1u32 << order(class);
        assert!(
            (batch & !(run - 1)) + run <= self.batches,
            "the run of {class:?} around batch {batch} leaves the region"
        );
        let mut empty = Vec::new();
        {
            let mut runs = self.runs.lock();
            let Some((start, whole)) = runs.reserve(batch, class) else {
                return false;
            };
            if whole {
                self.assign(class, start);
                return true;
            }
            // Batches change hands only under the runs' lock, held here, so every run a class
            // holds in the reserved one is drained, and none slips out to another class.
            let mut heads = Vec::new();
            for b in start..start + (1 << order(class)) {
                let mut head = b;
                while self.owner[head as usize].load(Ordering::Acquire) == CONTINUED {
                    head -= 1;
                }
                if self.owner[head as usize].load(Ordering::Acquire) != 0 && !heads.contains(&head)
                {
                    heads.push(head);
                }
            }
            for head in heads {
                let owner = self.owner[head as usize].load(Ordering::Acquire);
                let holder = SizeClass::new(owner as usize - 1).expect("a class");
                let mut c = self.classes[holder.index()].lock();
                self.draining[head as usize].store(1, Ordering::Relaxed);
                c.partial.remove(&head);
                c.live.retain(|&b| b != head);
                if self.taken[head as usize].load(Ordering::Relaxed) == 0 {
                    empty.push((holder, head));
                }
            }
        }
        for (holder, head) in empty {
            self.release(holder, head);
        }
        true
    }

    /// Whether `class` may take frames from more batches than it holds now: a free run exists.
    #[cfg(test)]
    pub fn has_free_run(&self, class: SizeClass) -> bool {
        let runs = self.runs.lock();
        (order(class)..ORDERS).any(|o| !runs.free[o].is_empty())
    }

    /// Whether `class` holds any batch.
    pub fn holds(&self, class: SizeClass) -> bool {
        !self.classes[class.index()].lock().all.is_empty()
    }

    /// Whether `frame` of `class` is free.
    pub fn is_free(&self, class: SizeClass, frame: u32) -> bool {
        let (batch, i) = position(class, frame);
        self.words(batch)[i as usize / 64].load(Ordering::Relaxed) & (1 << (i % 64)) != 0
    }

    /// Up to `n` frames of `class` drawn at random from the batches it holds and is not draining
    /// (taken or not).
    pub fn sample(&self, class: SizeClass, n: usize, rng: &mut impl rand::Rng) -> Vec<u32> {
        let c = self.classes[class.index()].lock();
        if c.live.is_empty() {
            return Vec::new();
        }
        let per = frames_per_batch(class);
        (0..n)
            .map(|_| {
                let batch = c.live[rng.random_range(0..c.live.len())];
                frame_at(class, batch, rng.random_range(0..per))
            })
            .collect()
    }

    /// The batch of `class`, not draining, with the fewest frames taken, to drain for `to`: one
    /// whose aligned run of `to`'s order lies inside the region, which a short last run may not.
    pub fn emptiest(&self, class: SizeClass, to: SizeClass) -> Option<u32> {
        let run = 1u32 << order(to);
        let c = self.classes[class.index()].lock();
        c.live
            .iter()
            .copied()
            .filter(|&b| (b & !(run - 1)) + run <= self.batches)
            .min_by_key(|&b| self.taken[b as usize].load(Ordering::Relaxed))
    }

    /// Every draining batch, with its class.
    pub fn draining(&self) -> Vec<(SizeClass, u32)> {
        let mut out = Vec::new();
        for class in SizeClass::all() {
            let c = self.classes[class.index()].lock();
            out.extend(
                c.all
                    .iter()
                    .filter(|&&b| self.draining[b as usize].load(Ordering::Relaxed) == 1)
                    .map(|&b| (class, b)),
            );
        }
        out
    }

    /// Bytes of the region each class holds.
    pub fn held(&self) -> [usize; CLASS_COUNT] {
        let mut out = [0; CLASS_COUNT];
        for class in SizeClass::all() {
            out[class.index()] =
                self.classes[class.index()].lock().all.len() * (BATCH << order(class));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(i: usize) -> SizeClass {
        SizeClass::new(i).unwrap()
    }

    #[test]
    fn a_drained_batch_moves_to_another_class_and_a_large_class_gets_an_aligned_run() {
        // 8 batches: two free runs of 4.
        let m = Memory::new(8 * BATCH).unwrap();
        assert!(m.grow(class(0)));
        let first = m.take(class(0)).unwrap();
        assert!(
            m.grow(class(11)),
            "an 8 MiB frame needs a free aligned run of 4"
        );
        let big = m.take(class(11)).unwrap();
        assert_eq!(big % (4 * GRAINS), 0);
        assert_eq!(m.held()[11], 4 * BATCH);
        assert!(!m.grow(class(11)), "no second aligned run of 4 is free");
        assert!(m.grow(class(10)));
        assert!(!m.has_free_run(class(10)));
        // Drain the 8 MiB frame's run: two class-10 runs fit in it.
        assert!(m.drain(class(11), big / GRAINS));
        assert!(
            m.put(class(11), big),
            "freeing its last frame releases the run"
        );
        assert_eq!(m.held()[11], 0);
        assert!(m.grow(class(10)) && m.grow(class(10)));
        // Class 0's batch leaves only once its frame is back, and hands out none meanwhile.
        assert!(m.drain(class(0), first / GRAINS));
        assert_eq!(m.take(class(0)), None);
        assert_eq!(m.held()[0], BATCH);
        assert!(m.put(class(0), first));
        assert_eq!(m.held()[0], 0);
    }

    #[test]
    fn a_batch_drained_for_a_class_goes_to_that_class_and_not_back() {
        let m = Memory::new(8 * BATCH).unwrap();
        assert!(m.grow(class(1)));
        let frame = m.take(class(1)).unwrap();
        assert!(m.drain_for(class(1), frame / GRAINS, class(0)));
        assert!(m.put(class(1), frame), "freeing its last frame empties it");
        assert_eq!(m.held()[1], 0);
        assert_eq!(m.held()[0], BATCH, "the batch went back to the free runs");
        assert!(m.take(class(0)).is_some());
    }

    #[test]
    fn a_reserved_run_collects_its_batches_as_they_are_freed() {
        // 5 batches: a run of 4 at 0 and one batch at 4. Five class-9 frames fill them.
        let m = Memory::new(5 * BATCH).unwrap();
        let mut frames: Vec<u32> = (0..5)
            .map(|_| {
                assert!(m.grow(class(9)));
                m.take(class(9)).unwrap()
            })
            .collect();
        frames.sort_unstable();
        assert!(m.drain_run(class(11), frames[1] / GRAINS));
        assert!(
            !m.drain_run(class(11), frames[1] / GRAINS),
            "one run held at a time"
        );
        for &f in &frames[..3] {
            m.put(class(9), f);
            assert!(
                !m.has_free_run(class(0)),
                "a reserved batch went back to the free runs"
            );
        }
        m.put(class(9), frames[4]);
        assert_eq!(
            m.take(class(9)),
            Some(frames[4]),
            "a batch not draining keeps its class and its freed frame"
        );
        assert!(!m.grow(class(11)));
        m.put(class(9), frames[3]);
        assert_eq!(
            m.held()[11],
            4 * BATCH,
            "the run goes to the class that reserved it once its last batch is free"
        );
        assert!(m.take(class(11)).is_some());
    }

    #[test]
    fn a_drain_of_a_batch_that_left_the_class_does_nothing() {
        let m = Memory::new(8 * BATCH).unwrap();
        assert!(m.grow(class(1)));
        let frame = m.take(class(1)).unwrap();
        let batch = frame / GRAINS;
        // Chosen to be drained for class 2, the batch empties and goes to class 0 first.
        assert!(m.drain_for(class(1), batch, class(0)));
        assert!(m.put(class(1), frame));
        assert!(
            !m.drain_for(class(1), batch, class(2)),
            "class 1 no longer holds the batch"
        );
        assert!(!m.drain(class(1), batch));
        assert!(m.draining().is_empty());
        for _ in 0..frames_per_batch(class(0)) {
            assert_eq!(
                m.take(class(0)).map(|f| f / GRAINS),
                Some(batch),
                "class 0 keeps every frame of it"
            );
        }
        assert_eq!(m.held()[0], BATCH);
        assert_eq!(m.held()[2], 0);
    }

    #[test]
    fn a_reserved_run_takes_a_batch_drained_for_another_class() {
        let m = Memory::new(5 * BATCH).unwrap();
        let mut frames: Vec<u32> = (0..5)
            .map(|_| {
                assert!(m.grow(class(9)));
                m.take(class(9)).unwrap()
            })
            .collect();
        frames.sort_unstable();
        // One batch is drained for class 0 before the run is reserved, and one after, by a
        // drain chosen before.
        assert!(m.drain_for(class(9), frames[0] / GRAINS, class(0)));
        assert!(m.drain_run(class(11), frames[1] / GRAINS));
        assert!(!m.drain_for(class(9), frames[2] / GRAINS, class(0)));
        for &f in &frames[..4] {
            m.put(class(9), f);
        }
        assert_eq!(m.held()[11], 4 * BATCH, "the reserved run is whole");
        assert_eq!(m.held()[0], 0);
    }

    #[test]
    fn a_run_reserved_inside_a_larger_run_gets_its_part_and_the_rest_is_free() {
        // 5 batches: an 8 MiB frame takes the run of 4.
        let m = Memory::new(5 * BATCH).unwrap();
        assert!(m.grow(class(11)));
        let big = m.take(class(11)).unwrap();
        assert!(m.drain_run(class(10), big / GRAINS));
        assert!(m.put(class(11), big));
        assert_eq!(m.held()[11], 0);
        assert_eq!(
            m.held()[10],
            2 * BATCH,
            "the reserved half goes to class 10"
        );
        assert!(m.grow(class(10)), "the other half is free");
        assert_eq!(m.held()[10], 4 * BATCH);
    }

    #[test]
    fn a_batch_for_a_large_class_is_chosen_where_its_run_fits() {
        // 5 batches: a run of 4 at 0 and one batch at 4, each given a 2 MiB frame. Batch 4 is
        // the emptiest once its frame is back, but no aligned run of two fits around it.
        let m = Memory::new(5 * BATCH).unwrap();
        let frames: Vec<u32> = (0..5)
            .map(|_| {
                assert!(m.grow(class(9)));
                m.take(class(9)).unwrap()
            })
            .collect();
        let last = *frames.iter().find(|&&f| f / GRAINS == 4).unwrap();
        m.put(class(9), last);
        assert_eq!(m.emptiest(class(9), class(9)), Some(4));
        assert!(
            m.emptiest(class(9), class(10)).is_some_and(|b| b < 4),
            "a drain for 4 MiB frames chose a batch with no aligned run of two"
        );
    }
}
