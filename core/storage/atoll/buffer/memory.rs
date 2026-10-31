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
    /// Those with a free frame and not draining, lowest first. Taking frames from the lowest
    /// leaves the highest batches emptier, and so cheaper to drain.
    partial: BTreeSet<u32>,
}

/// Free runs of batches by order, as a buddy allocator.
struct Runs {
    free: [BTreeSet<u32>; ORDERS],
    /// A run being emptied for a class of a larger order: batches freed inside it wait here
    /// until the whole run is free, so no other class takes them meanwhile.
    reserved: Option<Reserved>,
}

struct Reserved {
    start: u32,
    order: usize,
    missing: u32,
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

    fn give(&mut self, mut start: u32, mut order: usize) {
        if let Some(r) = &mut self.reserved {
            let end = r.start + (1 << r.order);
            if start >= r.start && start < end {
                r.missing -= 1 << order;
                if r.missing > 0 {
                    return;
                }
                (start, order) = (r.start, r.order);
                self.reserved = None;
            }
        }
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

    /// Holds the aligned run of `order` around `batch` for whoever drains it: its free batches
    /// leave the free lists, and the rest join it as they are freed. False while another run is
    /// held.
    fn reserve(&mut self, batch: u32, order: usize) -> Option<u32> {
        if self.reserved.is_some() {
            return None;
        }
        let start = batch & !((1u32 << order) - 1);
        let end = start + (1 << order);
        let mut free = 0;
        for o in 0..ORDERS {
            let inside: Vec<u32> = self.free[o].range(start..end).copied().collect();
            for s in inside {
                self.free[o].remove(&s);
                free += 1 << o;
            }
        }
        let missing = (1 << order) - free;
        if missing == 0 {
            self.give(start, order);
        } else {
            self.reserved = Some(Reserved {
                start,
                order,
                missing,
            });
        }
        Some(start)
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
        c.partial.insert(batch);
        true
    }

    /// Returns `frame` of `class`. True if that emptied a draining batch, which then left the
    /// class.
    pub fn put(&self, class: SizeClass, frame: u32) -> bool {
        let (batch, i) = position(class, frame);
        let emptied = {
            let mut c = self.classes[class.index()].lock();
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

    /// Hands an emptied draining batch back to the free runs.
    fn release(&self, class: SizeClass, batch: u32) {
        let mut runs = self.runs.lock();
        {
            let mut c = self.classes[class.index()].lock();
            if self.draining[batch as usize].load(Ordering::Relaxed) == 0
                || self.taken[batch as usize].load(Ordering::Relaxed) != 0
            {
                return;
            }
            self.draining[batch as usize].store(0, Ordering::Relaxed);
            for word in self.words(batch) {
                word.store(0, Ordering::Relaxed);
            }
            c.all.retain(|&b| b != batch);
            for b in batch..batch + (1 << order(class)) {
                self.owner[b as usize].store(0, Ordering::Release);
            }
        }
        runs.give(batch, order(class));
    }

    /// Stops new frames being taken from `batch` of `class`; it leaves the class once every frame
    /// in it is free. True if it was not draining already.
    pub fn drain(&self, class: SizeClass, batch: u32) -> bool {
        let empty = {
            let mut c = self.classes[class.index()].lock();
            if self.draining[batch as usize].swap(1, Ordering::Relaxed) == 1 {
                return false;
            }
            c.partial.remove(&batch);
            self.taken[batch as usize].load(Ordering::Relaxed) == 0
        };
        if empty {
            self.release(class, batch);
        }
        true
    }

    /// Holds the aligned run of `order(class)` batches around `batch` for `class`, and drains
    /// every batch in it that a class holds. False while another run is held.
    pub fn drain_run(&self, class: SizeClass, batch: u32) -> bool {
        let order = order(class);
        let Some(start) = self.runs.lock().reserve(batch, order) else {
            return false;
        };
        for b in start..start + (1 << order) {
            let owner = self.owner[b as usize].load(Ordering::Acquire);
            if owner != 0 && owner != CONTINUED {
                let holder = SizeClass::new(owner as usize - 1).expect("a class");
                self.drain(holder, b);
            }
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

    /// Up to `n` frames of `class` drawn at random from the batches it holds (taken or not).
    pub fn sample(&self, class: SizeClass, n: usize, rng: &mut impl rand::Rng) -> Vec<u32> {
        let c = self.classes[class.index()].lock();
        if c.all.is_empty() {
            return Vec::new();
        }
        let per = frames_per_batch(class);
        (0..n)
            .map(|_| {
                let batch = c.all[rng.random_range(0..c.all.len())];
                frame_at(class, batch, rng.random_range(0..per))
            })
            .collect()
    }

    /// The batch of `class`, not draining, with the fewest frames taken.
    pub fn emptiest(&self, class: SizeClass) -> Option<u32> {
        let c = self.classes[class.index()].lock();
        c.all
            .iter()
            .copied()
            .filter(|&b| self.draining[b as usize].load(Ordering::Relaxed) == 0)
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
        assert!(
            m.grow(class(11)),
            "the whole run is free once its last batch is"
        );
    }
}
