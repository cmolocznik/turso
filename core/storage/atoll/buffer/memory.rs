//! The pool's memory: one region carved into 2 MiB batches, each lent to one size class at a time.
//!
//! A batch holds frames of its class: 512 frames of 4 KiB, 256 of 8 KiB, down to one of 2 MiB. A
//! 4 MiB or 8 MiB frame takes an aligned run of 2 or 4 batches, handed out by a buddy allocator. A
//! frame is named by the 4 KiB grain it starts at, so a frame index names the same memory whichever
//! class holds it, and one array of headers serves every class.
//!
//! The region is backed by huge pages where the OS allows, of the kernel's PMD size: 2 MiB with
//! 4 KiB base pages, 32 MiB with 16 KiB, 512 MiB with 64 KiB, so a huge page holds a whole number
//! of batches. Its start is aligned to a huge page, and only the whole huge pages inside it are
//! advised; the rest keeps base pages, so no huge page reaches past the region and none of its
//! memory is committed past its length. Its memory is never unmapped: a freed frame is reused as
//! it is, and memory moves between classes a whole batch at a time in the pool's own bookkeeping,
//! so no huge page is ever split. A batch moves once every frame in it is free: the pool marks it
//! draining, which keeps new frames out of it, and moves or evicts what it holds.
//!
//! Untouched memory costs nothing: the region, the frame headers and the hash table are mapped
//! without reserving physical memory, which the OS commits on first touch.

use self::sync::{fence, AtomicU32, AtomicU64, Ordering};
use super::super::page::{SizeClass, CLASS_COUNT, MIN_PAGE_SHIFT, MIN_PAGE_SIZE};
use crate::io::{Buffer, RegionBuffer};
use crate::sync::atomic::AtomicU8;
use crate::sync::Arc;
use crate::{LimboError, Result};
use std::ptr::NonNull;

/// The engine's atomics for the allocator, or loom's, which explore the reorderings shuttle
/// cannot.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicU32, AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicU32, AtomicU64, Ordering};
}

/// log2 of the batch size.
pub const BATCH_SHIFT: u32 = 21;
/// A batch: 2 MiB, the smallest huge page of the four architectures, the PMD size with 4 KiB base
/// pages; a larger huge page holds a whole number of batches.
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
pub(super) fn order(class: SizeClass) -> usize {
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
unsafe impl Zeroable for crate::sync::atomic::AtomicU32 {}
unsafe impl Zeroable for crate::sync::atomic::AtomicU64 {}
// Under loom a `Zeroed` is built from defaults, never from zeros.
#[cfg(atoll_loom)]
unsafe impl Zeroable for AtomicU32 {}
#[cfg(atoll_loom)]
unsafe impl Zeroable for AtomicU64 {}

/// An array of `T` that starts zeroed and takes physical memory only where it is touched.
pub struct Zeroed<T: Zeroable> {
    #[cfg(not(any(shuttle, atoll_loom)))]
    map: Mapping,
    #[cfg(not(any(shuttle, atoll_loom)))]
    len: usize,
    #[cfg(not(any(shuttle, atoll_loom)))]
    _marker: std::marker::PhantomData<T>,
    // Shuttle's and loom's atomics are not valid as zeros: build them instead.
    #[cfg(any(shuttle, atoll_loom))]
    items: Box<[T]>,
}

// SAFETY: a `Zeroed<T>` hands out only `&T`, as a `Box<[T]>` would.
unsafe impl<T: Zeroable + Sync> Sync for Zeroed<T> {}
unsafe impl<T: Zeroable + Send> Send for Zeroed<T> {}

impl<T: Zeroable> Zeroed<T> {
    pub fn new(len: usize) -> Result<Self> {
        #[cfg(not(any(shuttle, atoll_loom)))]
        {
            let bytes = (len * std::mem::size_of::<T>()).max(1);
            let align = std::mem::align_of::<T>().max(MIN_PAGE_SIZE);
            Ok(Self {
                map: Mapping::new(bytes.next_multiple_of(MIN_PAGE_SIZE), align)?,
                len,
                _marker: std::marker::PhantomData,
            })
        }
        #[cfg(any(shuttle, atoll_loom))]
        Ok(Self {
            items: (0..len).map(|_| T::default()).collect(),
        })
    }
}

impl<T: Zeroable> std::ops::Deref for Zeroed<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        #[cfg(not(any(shuttle, atoll_loom)))]
        // SAFETY: the mapping holds `len` zero-initialized `T`s, valid as zeros, and is aligned
        // for `T`; only shared references are handed out.
        unsafe {
            std::slice::from_raw_parts(self.map.start.as_ptr().cast::<T>(), self.len)
        }
        #[cfg(any(shuttle, atoll_loom))]
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
    fn new(len: usize, align: usize) -> Result<Self> {
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
            Ok(Self {
                start: NonNull::new(start).expect("a mapping is never at address zero"),
                raw,
                raw_len,
            })
        }
        #[cfg(not(all(unix, not(miri))))]
        {
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

/// The kernel's transparent huge page size, its PMD size, where it has them.
fn huge_page_size() -> Option<usize> {
    if !cfg!(all(target_os = "linux", not(miri))) {
        return None;
    }
    let text =
        std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size").ok()?;
    parse_huge_page_size(&text)
}

/// A huge page size as the kernel writes it, in bytes: a power of two.
fn parse_huge_page_size(text: &str) -> Option<usize> {
    text.trim()
        .parse::<usize>()
        .ok()
        .filter(|n| n.is_power_of_two())
}

impl Region {
    fn new(len: usize) -> Result<Self> {
        Self::on_huge_pages(len, huge_page_size())
    }

    /// `len` bytes on huge pages of `huge` bytes, the kernel's, where it has them: the start
    /// aligned to one, the whole huge pages inside advised, and the rest kept on base pages, so no
    /// huge page reaches past the region. Huge pages only make the pool faster, so a refusal leaves
    /// base pages.
    fn on_huge_pages(len: usize, huge: Option<usize>) -> Result<Self> {
        let map = Mapping::new(len, huge.map_or(BATCH, |h| h.max(BATCH)))?;
        #[cfg(all(target_os = "linux", not(miri)))]
        if let Some(huge) = huge {
            let whole = len / huge * huge;
            let start = map.start.as_ptr();
            // SAFETY: inside the mapping just made, of `len` bytes from `start`.
            unsafe {
                if whole > 0 {
                    libc::madvise(start.cast(), whole, libc::MADV_HUGEPAGE);
                }
                if whole < len {
                    libc::madvise(start.add(whole).cast(), len - whole, libc::MADV_NOHUGEPAGE);
                }
            }
        }
        Ok(Self { map, len })
    }

    /// The first byte of `frame`.
    pub fn ptr(&self, frame: u32) -> *mut u8 {
        let at = frame as usize * MIN_PAGE_SIZE;
        debug_assert!(at < self.len);
        // SAFETY: inside the region.
        unsafe { self.map.start.as_ptr().add(at) }
    }

    /// `frame`'s `len` bytes as an IO buffer that keeps the region alive: for a read into a quiet
    /// frame, or a write out of one, that the caller holds exclusively while the IO is in flight.
    pub fn buffer(self: &Arc<Self>, frame: u32, len: usize) -> Arc<Buffer> {
        assert!(frame as usize * MIN_PAGE_SIZE + len <= self.len);
        // SAFETY: inside the region, which the buffer keeps alive.
        let region = unsafe {
            RegionBuffer::new(self.clone(), NonNull::new_unchecked(self.ptr(frame)), len)
        };
        Arc::new(Buffer::new_region(region))
    }
}

/// A batch's state, in the one word every change to it goes through: which class holds it,
/// whether it is draining, whether a release owns it, the frames taken from it and the class it is
/// drained for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct State(u64);

const OWNER: u64 = 0xff;
/// No new frame is taken from the batch; it leaves its class once every frame in it is free.
const DRAINING: u64 = 1 << 8;
/// The batch's last frame is back while it drains: the release under way owns it.
const RELEASING: u64 = 1 << 9;
const TAKEN_SHIFT: u32 = 10;
const TAKEN_ONE: u64 = 1 << TAKEN_SHIFT;
/// Frames taken or promised: up to [`GRAINS`], 512, in 10 bits.
const TAKEN: u64 = 0x3ff << TAKEN_SHIFT;
const BENEFICIARY_SHIFT: u32 = 20;
const BENEFICIARY: u64 = 0xff << BENEFICIARY_SHIFT;

impl State {
    fn holding(class: SizeClass) -> Self {
        Self(class.index() as u64 + 1)
    }

    fn owner(self) -> u8 {
        (self.0 & OWNER) as u8
    }

    /// The class holding the batch, if it is the first of a run a class holds.
    fn class(self) -> Option<SizeClass> {
        let owner = self.owner();
        (owner != 0 && owner != CONTINUED)
            .then(|| SizeClass::new(owner as usize - 1))
            .flatten()
    }

    fn held_by(self, class: SizeClass) -> bool {
        self.owner() == class.index() as u8 + 1
    }

    fn draining(self) -> bool {
        self.0 & DRAINING != 0
    }

    fn releasing(self) -> bool {
        self.0 & RELEASING != 0
    }

    fn taken(self) -> u32 {
        ((self.0 & TAKEN) >> TAKEN_SHIFT) as u32
    }

    fn beneficiary(self) -> Option<SizeClass> {
        let b = (self.0 & BENEFICIARY) >> BENEFICIARY_SHIFT;
        (b != 0).then(|| SizeClass::new(b as usize - 1)).flatten()
    }

    fn with_beneficiary(self, class: SizeClass) -> Self {
        Self(self.0 & !BENEFICIARY | (class.index() as u64 + 1) << BENEFICIARY_SHIFT)
    }

    /// Frames may be taken from it for `class`.
    fn open_to(self, class: SizeClass) -> bool {
        self.held_by(class) && self.0 & (DRAINING | RELEASING) == 0
    }
}

/// A bit per batch.
struct Bits(Zeroed<AtomicU64>);

impl Bits {
    fn new(batches: u32) -> Result<Self> {
        Ok(Self(Zeroed::new((batches as usize).div_ceil(64))?))
    }

    /// Sets `batch`'s bit; true if it was clear.
    fn set(&self, batch: u32) -> bool {
        let bit = 1 << (batch % 64);
        self.0[batch as usize / 64].fetch_or(bit, Ordering::AcqRel) & bit == 0
    }

    /// Clears `batch`'s bit; true if it was set.
    fn clear(&self, batch: u32) -> bool {
        let bit = 1 << (batch % 64);
        self.0[batch as usize / 64].fetch_and(!bit, Ordering::AcqRel) & bit != 0
    }

    /// The batches whose bit is set, lowest first, each word read once.
    fn ones(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter().enumerate().flat_map(|(w, word)| {
            let mut bits = word.load(Ordering::Acquire);
            std::iter::from_fn(move || {
                (bits != 0).then(|| {
                    let b = bits.trailing_zeros();
                    bits &= bits - 1;
                    w as u32 * 64 + b
                })
            })
        })
    }
}

/// A class's batches (the first of each run): those it holds, those not draining, and those that
/// may have a free frame. Each is a hint the batch's [`State`] confirms.
struct ClassIndex {
    held: Bits,
    /// Held and not draining: what eviction samples, and what a drain chooses from. A draining
    /// batch's freed frames are leaving, and would make the class look as if it had room.
    live: Bits,
    /// May have a free frame: what a take looks through, lowest first, which leaves the highest
    /// batches emptier, and so cheaper to drain. Set by whoever frees a frame or adds a batch,
    /// cleared by a take that finds none.
    partial: Bits,
    /// How many batches the class holds, and how many of those are live. Relaxed: counts, for
    /// a sample's size and a hint whether the class holds any.
    held_count: AtomicU32,
    live_count: AtomicU32,
}

impl ClassIndex {
    fn new(batches: u32) -> Result<Self> {
        Ok(Self {
            held: Bits::new(batches)?,
            live: Bits::new(batches)?,
            partial: Bits::new(batches)?,
            held_count: AtomicU32::new(0),
            live_count: AtomicU32::new(0),
        })
    }

    fn leave_live(&self, batch: u32) {
        if self.live.clear(batch) {
            self.live_count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// The run held for a large class while its batches are emptied, in one word: its first batch,
/// its order, the class, how many of its batches are not free yet, and [`HELD`]. 0 for none.
/// Batches freed inside it count down to it instead of going back to the free runs, so no other
/// class takes them meanwhile; the batch that makes it whole hands it to the class.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Reservation(u64);

const HELD: u64 = 1 << 63;
const MISSING_SHIFT: u32 = 40;

impl Reservation {
    fn new(start: u32, order: usize, class: SizeClass) -> Self {
        Self(
            HELD | start as u64
                | (order as u64) << 32
                | (class.index() as u64) << 34
                | (1u64 << order) << MISSING_SHIFT,
        )
    }

    fn start(self) -> u32 {
        self.0 as u32
    }

    fn order(self) -> usize {
        (self.0 >> 32 & 3) as usize
    }

    fn class(self) -> SizeClass {
        SizeClass::new((self.0 >> 34 & 0x3f) as usize).expect("a class")
    }

    fn missing(self) -> u32 {
        (self.0 >> MISSING_SHIFT & 0xf) as u32
    }

    /// Whether a batch freed at `batch` counts to it: one of its batches, while it still misses
    /// some. A run made whole is its class's from then on, though its reservation is cleared only
    /// after the hand-over, and a drain of it meanwhile frees its batches as any other.
    fn covers(self, batch: u32) -> bool {
        self.0 & HELD != 0
            && self.missing() > 0
            && (self.start()..self.start() + (1 << self.order())).contains(&batch)
    }

    fn without_missing(self) -> u64 {
        self.0 & !(0xf << MISSING_SHIFT)
    }

    /// One more of its batches free.
    fn collected(self) -> Self {
        assert!(self.missing() > 0, "a batch counted to a run already whole");
        Self(self.0 - (1 << MISSING_SHIFT))
    }
}

/// What looking for a free frame found.
pub enum Take {
    Frame(u32),
    /// Free frames, none of which was usable.
    Unusable,
    /// No free frame.
    None,
}

/// Which frames are free, and which class holds each batch. Lock-free: every change to a batch is
/// a compare-and-swap of its [`State`], so a frame is taken only from a batch its class holds and
/// that is not draining, and exactly one thread releases a batch.
pub struct Memory {
    pub region: Arc<Region>,
    pub batches: u32,
    /// Per batch: its [`State`]; [`CONTINUED`] for a batch continuing a run begun by an earlier
    /// one, 0 for a free one.
    state: Zeroed<AtomicU64>,
    /// Per batch, [`WORDS`] words: a set bit is a free frame.
    free: Zeroed<AtomicU64>,
    classes: Box<[ClassIndex]>,
    /// The batches free and not counted to the reserved run, a bit each. Aligned runs of up to
    /// four lie in one word, so taking a run is one compare-and-swap, and runs coalesce as their
    /// bits are set.
    unowned: Bits,
    /// The [`Reservation`].
    reserved: AtomicU64,
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
        let unowned = Bits::new(batches)?;
        for b in 0..batches {
            unowned.set(b);
        }
        Ok(Self {
            region: Arc::new(Region::new(batches as usize * BATCH)?),
            batches,
            state: Zeroed::new(batches as usize)?,
            free: Zeroed::new(batches as usize * WORDS)?,
            classes: (0..CLASS_COUNT)
                .map(|_| ClassIndex::new(batches))
                .collect::<Result<_>>()?,
            unowned,
            reserved: AtomicU64::new(0),
        })
    }

    /// Frames the region holds, at one per 4 KiB.
    pub fn grains(&self) -> u32 {
        self.batches * GRAINS
    }

    fn state(&self, batch: u32) -> State {
        State(self.state[batch as usize].load(Ordering::Acquire))
    }

    fn index(&self, class: SizeClass) -> &ClassIndex {
        &self.classes[class.index()]
    }

    /// The class holding `frame`'s batch, while the frame is taken.
    pub fn class_of(&self, frame: u32) -> SizeClass {
        let class = self.state(frame / GRAINS).class();
        debug_assert!(class.is_some(), "frame {frame} is in no class");
        class.expect("a class")
    }

    fn words(&self, batch: u32) -> &[AtomicU64] {
        &self.free[batch as usize * WORDS..(batch as usize + 1) * WORDS]
    }

    /// A free frame of `class` from a batch it holds, if any.
    #[cfg(test)]
    pub fn take(&self, class: SizeClass) -> Option<u32> {
        match self.take_where(class, |_| true) {
            Take::Frame(f) => Some(f),
            Take::Unusable | Take::None => None,
        }
    }

    /// A free frame of `class` that `usable` accepts, from the lowest batch that has one.
    pub fn take_where(&self, class: SizeClass, usable: impl Fn(u32) -> bool) -> Take {
        let mut unusable = false;
        for batch in self.index(class).partial.ones() {
            if !self.promise_frame(class, batch) {
                continue;
            }
            match self.claim_frame(class, batch, &usable) {
                Some(frame) => return Take::Frame(frame),
                None => {
                    unusable = true;
                    self.unpromise_frame(class, batch);
                }
            }
        }
        if unusable {
            Take::Unusable
        } else {
            Take::None
        }
    }

    /// Counts a frame of `batch` taken for `class` before choosing it, if the class holds the
    /// batch, it is not draining and not every frame is taken. A free frame is then the caller's:
    /// the set bits never fall below the frames promised and not yet claimed.
    fn promise_frame(&self, class: SizeClass, batch: u32) -> bool {
        let word = &self.state[batch as usize];
        let mut s = State(word.load(Ordering::Acquire));
        loop {
            if !s.open_to(class) || s.taken() == frames_per_batch(class) {
                self.unmark_partial(class, batch);
                return false;
            }
            match word.compare_exchange_weak(
                s.0,
                s.0 + TAKEN_ONE,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(now) => s = State(now),
            }
        }
    }

    /// Clears `batch`'s partial bit, found with no frame to give. A frame freed meanwhile sets it
    /// again: setting and clearing are acquire-release exchanges of one word, so either the free's
    /// setting follows this clearing, or this clearing read it and so reads the free's count.
    fn unmark_partial(&self, class: SizeClass, batch: u32) {
        let index = self.index(class);
        if !index.partial.clear(batch) {
            return;
        }
        let s = self.state(batch);
        if s.open_to(class) && s.taken() < frames_per_batch(class) {
            index.partial.set(batch);
        }
    }

    /// Claims a free frame of `batch` that `usable` accepts; one is free for the promise taken.
    /// None if every free frame was unusable.
    fn claim_frame(
        &self,
        class: SizeClass,
        batch: u32,
        usable: &impl Fn(u32) -> bool,
    ) -> Option<u32> {
        let words = self.words(batch);
        loop {
            let mut unusable = false;
            for (w, word) in words.iter().enumerate() {
                let mut bits = word.load(Ordering::Acquire);
                while bits != 0 {
                    let b = bits.trailing_zeros();
                    bits &= bits - 1;
                    let frame = frame_at(class, batch, w as u32 * 64 + b);
                    if !usable(frame) {
                        unusable = true;
                        continue;
                    }
                    // Acquire: whatever the frame's last holder did before freeing it.
                    if word.fetch_and(!(1 << b), Ordering::Acquire) & 1 << b != 0 {
                        return Some(frame);
                    }
                }
            }
            if unusable {
                return None;
            }
            // Every free frame seen went to another promise first: one is left for this one.
        }
    }

    /// Gives back a promise no frame was claimed for.
    fn unpromise_frame(&self, class: SizeClass, batch: u32) {
        self.count_frame_back(class, batch);
    }

    /// Lends a free run to `class`; false if there is none.
    pub fn grow(&self, class: SizeClass) -> bool {
        let Some(start) = self.take_run(order(class)) else {
            return false;
        };
        self.assign(class, start, true);
        true
    }

    /// Takes a free run of `order`: an aligned one from the smallest free run that holds one, as a
    /// buddy allocator would, so larger runs stay whole. None of the reserved run.
    fn take_run(&self, order: usize) -> Option<u32> {
        for exact in order..ORDERS {
            for (w, word) in self.unowned.0.iter().enumerate() {
                let mut seen = word.load(Ordering::Acquire);
                loop {
                    let fits = runs_of(self.outside_reservation(w, seen), exact);
                    if fits == 0 {
                        break;
                    }
                    let at = fits.trailing_zeros();
                    let run = ((1u64 << (1 << order)) - 1) << at;
                    match word.compare_exchange_weak(
                        seen,
                        seen & !run,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return Some(w as u32 * 64 + at),
                        Err(now) => seen = now,
                    }
                }
            }
        }
        None
    }

    /// `bits`, the free batches of word `w`, less those of the reserved run.
    fn outside_reservation(&self, w: usize, bits: u64) -> u64 {
        let r = Reservation(self.reserved.load(Ordering::Acquire));
        if r.0 & HELD == 0 {
            return bits;
        }
        let (start, end) = (r.start() as usize, r.start() as usize + (1 << r.order()));
        let (lo, hi) = (start.max(w * 64), end.min(w * 64 + 64));
        if lo >= hi {
            return bits;
        }
        let run = ((1u128 << (hi - lo)) - 1) as u64;
        bits & !(run << (lo - w * 64))
    }

    /// Gives the run at `start` to `class`, with every frame free. Unless the run is the reserved
    /// one being handed over (`check` false), a run reserved around it meanwhile drains it: either
    /// the reserving drain sees this class hold it, or this sees the reservation.
    fn assign(&self, class: SizeClass, start: u32, check: bool) {
        let run = 1u32 << order(class);
        // The class's bits first: a drain that finds the batch the class's, once its state says
        // so, and releases it, clears them after they are set.
        let index = self.index(class);
        if index.held.set(start) {
            index.held_count.fetch_add(1, Ordering::Relaxed);
        }
        if index.live.set(start) {
            index.live_count.fetch_add(1, Ordering::Relaxed);
        }
        for b in start + 1..start + run {
            self.state[b as usize].store(CONTINUED as u64, Ordering::Release);
        }
        let n = frames_per_batch(class);
        for (w, word) in self.words(start).iter().enumerate() {
            let below = n.saturating_sub(w as u32 * 64).min(64);
            let bits = if below == 64 {
                u64::MAX
            } else {
                (1 << below) - 1
            };
            word.store(bits, Ordering::Relaxed);
        }
        // Release: a take that reads it sees the frames free.
        self.state[start as usize].store(State::holding(class).0, Ordering::Release);
        // Pairs with a reservation's drain: either it sees this class hold the run, or the check
        // below sees the reservation.
        fence(Ordering::SeqCst);
        index.partial.set(start);
        if check {
            let r = Reservation(self.reserved.load(Ordering::Acquire));
            if (start..start + run).any(|b| r.covers(b)) {
                self.start_drain(class, start, None);
            }
        }
    }

    /// Returns `frame` of `class`. True if that emptied a draining batch, which then left the
    /// class.
    pub fn put(&self, class: SizeClass, frame: u32) -> bool {
        let (batch, i) = position(class, frame);
        debug_assert!(self.owns(class, batch), "frame {frame} is not {class:?}'s");
        let bit = 1 << (i % 64);
        // Release: whatever this thread did with the frame, to whoever takes it next.
        let before = self.words(batch)[i as usize / 64].fetch_or(bit, Ordering::Release);
        debug_assert_eq!(before & bit, 0, "frame {frame} freed twice");
        self.count_frame_back(class, batch)
    }

    /// Counts a frame of `batch` back: freed, or promised and not claimed. True if that emptied
    /// the batch while it drained, and this released it.
    fn count_frame_back(&self, class: SizeClass, batch: u32) -> bool {
        let word = &self.state[batch as usize];
        let mut s = State(word.load(Ordering::Acquire));
        loop {
            debug_assert!(s.held_by(class) && s.taken() > 0, "{s:?}");
            let release = s.draining() && s.taken() == 1;
            let next = s.0 - TAKEN_ONE | if release { RELEASING } else { 0 };
            match word.compare_exchange_weak(s.0, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(now) => s = State(now),
            }
        }
        if s.draining() {
            if s.taken() == 1 {
                self.release(class, batch);
                return true;
            }
            return false;
        }
        // After the count: a take clearing the bit either follows this or sees the count.
        self.index(class).partial.set(batch);
        false
    }

    /// Whether `class` holds `batch`.
    fn owns(&self, class: SizeClass, batch: u32) -> bool {
        self.state(batch).held_by(class)
    }

    /// Hands an emptied draining batch, which this thread released, to the run reserved around
    /// it, or else to the class it was drained for, or back to the free runs.
    fn release(&self, class: SizeClass, batch: u32) {
        let run = 1u32 << order(class);
        let index = self.index(class);
        // Relaxed: the batch is draining, so no take promises a frame of it to read these, and its
        // next class sets them before the release that opens it.
        for word in self.words(batch) {
            word.store(0, Ordering::Relaxed);
        }
        index.partial.clear(batch);
        index.leave_live(batch);
        if index.held.clear(batch) {
            index.held_count.fetch_sub(1, Ordering::Relaxed);
        }
        let beneficiary = self
            .state(batch)
            .beneficiary()
            .filter(|b| order(*b) == order(class));
        let r = Reservation(self.reserved.load(Ordering::Acquire));
        match beneficiary.filter(|_| !(batch..batch + run).any(|b| r.covers(b))) {
            Some(b) => self.assign(b, batch, true),
            None => {
                for b in batch..batch + run {
                    self.state[b as usize].store(0, Ordering::Release);
                }
                for b in batch..batch + run {
                    self.give(b);
                }
            }
        }
    }

    /// Frees `batch`: counted to the reserved run if it lies in it, else back to the free runs.
    /// A run reserved meanwhile finds it either way: either its reserving sees the bit set here,
    /// or this sees it reserved.
    fn give(&self, batch: u32) {
        if Reservation(self.reserved.load(Ordering::Acquire)).covers(batch) {
            self.collect();
            return;
        }
        self.unowned.set(batch);
        fence(Ordering::SeqCst);
        if Reservation(self.reserved.load(Ordering::Acquire)).covers(batch)
            && self.unowned.clear(batch)
        {
            self.collect();
        }
    }

    /// Counts one more batch of the reserved run free; the one that makes it whole hands it to
    /// its class. The run stays reserved until then, so no batch of it counts twice.
    fn collect(&self) {
        let mut r = Reservation(self.reserved.load(Ordering::Acquire));
        loop {
            let next = r.collected();
            match self.reserved.compare_exchange_weak(
                r.0,
                next.0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if next.missing() == 0 {
                        self.assign(next.class(), next.start(), false);
                        self.reserved.store(0, Ordering::Release);
                    }
                    return;
                }
                Err(now) => r = Reservation(now),
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
        let word = &self.state[batch as usize];
        let mut s = State(word.load(Ordering::Acquire));
        let release = loop {
            if !s.held_by(class) || s.releasing() {
                return false;
            }
            // The first class to ask keeps it.
            let mut next = match beneficiary {
                Some(b) if s.beneficiary().is_none() => s.with_beneficiary(b),
                _ => s,
            };
            if s.draining() {
                if next == s {
                    return false;
                }
            } else {
                next.0 |= DRAINING;
                if s.taken() == 0 {
                    next.0 |= RELEASING;
                }
            }
            match word.compare_exchange_weak(s.0, next.0, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) if s.draining() => return false,
                Ok(_) => break next.releasing(),
                Err(now) => s = State(now),
            }
        };
        let index = self.index(class);
        index.leave_live(batch);
        index.partial.clear(batch);
        if release {
            self.release(class, batch);
        }
        true
    }

    /// Holds the aligned run of `class`'s order around `batch` for `class`, and drains every
    /// batch in it that a class holds: the run goes to `class` once it is free. False while
    /// another run is held.
    pub fn drain_run(&self, class: SizeClass, batch: u32) -> bool {
        let order = order(class);
        let run = 1u32 << order;
        let start = batch & !(run - 1);
        assert!(
            start + run <= self.batches,
            "the run of {class:?} around batch {batch} leaves the region"
        );
        let r = Reservation::new(start, order, class);
        if self
            .reserved
            .compare_exchange(0, r.0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        // Pairs with a batch freed or lent meanwhile (`give`, `assign`).
        fence(Ordering::SeqCst);
        for b in start..start + run {
            if self.unowned.clear(b) {
                self.collect();
            }
        }
        for b in start..start + run {
            // Whole, the run is the class's: nothing more to drain. A class already holding it
            // drains it too, and gets it back whole.
            if !self.still_missing(r) {
                break;
            }
            let mut head = b;
            while self.state(head).owner() == CONTINUED {
                head -= 1;
            }
            if let Some(holder) = self.state(head).class() {
                self.start_drain(holder, head, None);
            }
        }
        true
    }

    /// Whether the run `r` reserved still misses batches.
    fn still_missing(&self, r: Reservation) -> bool {
        let now = Reservation(self.reserved.load(Ordering::Acquire));
        now.without_missing() == r.without_missing() && now.missing() > 0
    }

    /// Whether `class` may take frames from more batches than it holds now: a free run exists.
    #[cfg(test)]
    pub fn has_free_run(&self, class: SizeClass) -> bool {
        (0..self.unowned.0.len()).any(|w| {
            let bits = self.outside_reservation(w, self.unowned.0[w].load(Ordering::Acquire));
            (order(class)..ORDERS).any(|exact| runs_of(bits, exact) != 0)
        })
    }

    /// Whether `class` holds any batch.
    pub fn holds(&self, class: SizeClass) -> bool {
        self.index(class).held_count.load(Ordering::Relaxed) > 0
    }

    /// Whether `frame` of `class` is free. Relaxed: a hint, so a drain looks again at a frame
    /// misread and its latch refuses one taken meanwhile.
    pub fn is_free(&self, class: SizeClass, frame: u32) -> bool {
        let (batch, i) = position(class, frame);
        self.words(batch)[i as usize / 64].load(Ordering::Relaxed) & (1 << (i % 64)) != 0
    }

    /// Up to `n` frames of `class` drawn at random from the batches it holds and is not draining
    /// (taken or not): each from a batch drawn uniformly, read from the live batches in one pass.
    pub fn sample(&self, class: SizeClass, n: usize, rng: &mut impl rand::Rng) -> Vec<u32> {
        let index = self.index(class);
        let live = index.live_count.load(Ordering::Relaxed) as usize;
        if live == 0 {
            return Vec::new();
        }
        let per = frames_per_batch(class);
        let mut ranks: Vec<usize> = (0..n).map(|_| rng.random_range(0..live)).collect();
        ranks.sort_unstable();
        let mut out = Vec::with_capacity(n);
        let (mut next, mut last) = (0, None);
        for (rank, batch) in index.live.ones().enumerate() {
            while next < ranks.len() && ranks[next] == rank {
                out.push(frame_at(class, batch, rng.random_range(0..per)));
                next += 1;
            }
            last = Some(batch);
            if next == ranks.len() {
                break;
            }
        }
        // Batches that left while the pass read: their draws go to the last batch it found.
        if let Some(batch) = last {
            for _ in next..ranks.len() {
                out.push(frame_at(class, batch, rng.random_range(0..per)));
            }
        }
        out
    }

    /// The frames taken in the `count` batches from `start`, whichever classes hold them; a frame
    /// begun before `start` that reaches into it counts too. It may be stale by the time it is
    /// used: it estimates what draining the batches would evict.
    pub fn taken_in(&self, start: u32, count: u32) -> Vec<u32> {
        let mut out = Vec::new();
        let mut b = start;
        while b > 0 && self.state(b).owner() == CONTINUED {
            b -= 1;
        }
        while b < start + count {
            let Some(class) = self.state(b).class() else {
                b += 1;
                continue;
            };
            for i in 0..frames_per_batch(class) {
                let frame = frame_at(class, b, i);
                if !self.is_free(class, frame) {
                    out.push(frame);
                }
            }
            b += 1 << order(class);
        }
        out
    }

    /// The batch of `class`, not draining, with the fewest frames taken, to drain for `to`: one
    /// whose aligned run of `to`'s order lies inside the region, which a short last run may not.
    pub fn emptiest(
        &self,
        class: SizeClass,
        to: SizeClass,
        drainable: impl Fn(u32) -> bool,
    ) -> Option<u32> {
        let run = 1u32 << order(to);
        self.index(class)
            .live
            .ones()
            .filter(|&b| (b & !(run - 1)) + run <= self.batches && drainable(b))
            .min_by_key(|&b| self.state(b).taken())
    }

    /// Every draining batch, with its class.
    pub fn draining(&self) -> Vec<(SizeClass, u32)> {
        let mut out = Vec::new();
        for class in SizeClass::all() {
            out.extend(
                self.index(class)
                    .held
                    .ones()
                    .filter(|&b| {
                        let s = self.state(b);
                        s.held_by(class) && s.draining()
                    })
                    .map(|b| (class, b)),
            );
        }
        out
    }

    /// Every free frame of `class`, for a test to show.
    #[cfg(test)]
    pub fn free_frames(&self, class: SizeClass) -> Vec<u32> {
        self.index(class)
            .held
            .ones()
            .flat_map(|b| (0..frames_per_batch(class)).map(move |i| frame_at(class, b, i)))
            .filter(|&f| self.is_free(class, f))
            .collect()
    }

    /// Each class's batches: (batch, frames taken, draining), for a test to show.
    #[cfg(test)]
    pub fn describe(&self) -> String {
        let mut out = String::new();
        for class in SizeClass::all() {
            let index = self.index(class);
            let batches: Vec<_> = index
                .held
                .ones()
                .map(|b| (b, self.state(b).taken(), self.state(b).draining()))
                .collect();
            if batches.is_empty() {
                continue;
            }
            let partial: Vec<u32> = index.partial.ones().collect();
            out.push_str(&format!("{class:?}: {batches:?} partial {partial:?}\n"));
        }
        let free: Vec<u32> = self.unowned.ones().collect();
        out.push_str(&format!(
            "free batches: {free:?} reserved {:?}\n",
            Reservation(self.reserved.load(Ordering::Acquire))
        ));
        out
    }

    pub fn held(&self) -> [usize; CLASS_COUNT] {
        let mut out = [0; CLASS_COUNT];
        for class in SizeClass::all() {
            out[class.index()] = self.index(class).held_count.load(Ordering::Relaxed) as usize
                * (BATCH << order(class));
        }
        out
    }
}

/// Of `bits`, a word of free batches, the first bit of each free run of exactly order `exact`:
/// aligned and wholly free, in an aligned run of the next order that is not. A run of a lower order
/// is taken from its start.
fn runs_of(bits: u64, exact: usize) -> u64 {
    let whole = |o: usize| -> u64 {
        // A bit at each aligned run of order `o` wholly free.
        let mut w = bits;
        for k in 0..o {
            w &= w >> (1 << k);
        }
        w & aligned(o)
    };
    let mut runs = whole(exact);
    if exact + 1 < ORDERS {
        let larger = whole(exact + 1);
        runs &= !(larger | larger << (1 << exact));
    }
    runs
}

/// A bit at the start of every aligned run of order `o` in a word.
fn aligned(o: usize) -> u64 {
    match o {
        0 => u64::MAX,
        1 => 0x5555_5555_5555_5555,
        2 => 0x1111_1111_1111_1111,
        _ => unreachable!("runs of at most four batches"),
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests;

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;

    /// Takers take and free frames of four classes while a drainer drains their batches, the
    /// reserving class's among them, and reserves runs: no frame is handed out twice, a frame
    /// taken is its class's, and once every frame is back every drain has ended, the last
    /// reservation is whole and every batch is held or free.
    fn churn(rounds: usize) {
        use crate::sync::atomic::AtomicBool;
        let m = Arc::new(Memory::new(8 * BATCH).unwrap());
        let out: Arc<Vec<AtomicBool>> =
            Arc::new((0..m.grains()).map(|_| AtomicBool::new(false)).collect());
        let classes = [
            class(0),
            class(1),
            class(BATCH_CLASS),
            class(BATCH_CLASS + 1),
        ];
        let takers: Vec<_> = (0..3)
            .map(|t| {
                let (m, out) = (m.clone(), out.clone());
                crate::thread::spawn(move || {
                    for round in 0..rounds {
                        let c = classes[(t + round) % classes.len()];
                        let Some(frame) = m.take(c).or_else(|| m.grow(c).then(|| m.take(c))?)
                        else {
                            continue;
                        };
                        assert!(
                            !out[frame as usize].swap(true, Ordering::AcqRel),
                            "frame {frame} handed out twice"
                        );
                        assert_eq!(m.class_of(frame), c);
                        crate::thread::yield_now();
                        out[frame as usize].store(false, Ordering::Release);
                        m.put(c, frame);
                    }
                })
            })
            .collect();
        let drainer = {
            let m = m.clone();
            crate::thread::spawn(move || {
                for round in 0..rounds {
                    // Every class, the one runs are reserved for too: a run made whole is drained
                    // again as its reservation clears.
                    let from = classes[round % classes.len()];
                    if let Some(batch) = m.emptiest(from, class(0), |_| true) {
                        m.drain_for(from, batch, class(0));
                    }
                    if round % 4 == 0 {
                        m.drain_run(class(BATCH_CLASS + 1), (round as u32 / 4) % 4 * 2);
                    }
                    crate::thread::yield_now();
                }
            })
        };
        for t in takers {
            t.join().unwrap();
        }
        drainer.join().unwrap();
        assert_eq!(m.reserved.load(Ordering::Acquire), 0, "{}", m.describe());
        assert!(m.draining().is_empty(), "{}", m.describe());
        let held: usize = m.held().iter().sum();
        let free = m.unowned.ones().count() * BATCH;
        assert_eq!(held + free, m.batches as usize * BATCH, "{}", m.describe());
    }

    #[cfg(not(shuttle))]
    #[test]
    fn frames_go_out_once_while_batches_drain_and_runs_are_reserved() {
        churn(20_000);
    }

    #[cfg(shuttle)]
    #[test]
    fn frames_go_out_once_while_batches_drain_and_runs_are_reserved_in_shuttle_schedules() {
        crate::storage::atoll::testing::schedules(|| churn(20), 2_000);
    }

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
        assert_eq!(m.emptiest(class(9), class(9), |_| true), Some(4));
        assert!(
            m.emptiest(class(9), class(10), |_| true)
                .is_some_and(|b| b < 4),
            "a drain for 4 MiB frames chose a batch with no aligned run of two"
        );
    }

    /// A region starts on a huge page and advises only the whole huge pages inside it, the rest
    /// kept on base pages, as the kernel's own flags for its mappings show: with this kernel's
    /// huge pages, as the kernel reports them, and with 512 MiB ones, a 64 KiB-page kernel's,
    /// under which a region smaller than one keeps base pages throughout.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_region_advises_only_the_whole_huge_pages_inside_it() {
        let kernel = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size")
            .expect("this kernel reports no huge page size")
            .trim()
            .parse::<usize>()
            .unwrap();
        let large = 256 * BATCH;
        for (huge, len) in [
            (None, 5 * BATCH),
            (Some(large), 5 * BATCH),
            (Some(large), 320 * BATCH),
        ] {
            let region = match huge {
                None => Region::new(len),
                Some(h) => Region::on_huge_pages(len, Some(h)),
            }
            .unwrap();
            let huge = huge.unwrap_or(kernel);
            let start = region.ptr(0) as usize;
            assert_eq!(
                start % huge,
                0,
                "{huge} {len}: the region starts off a huge page"
            );
            let whole = len / huge * huge;
            let has = |at: usize, flag: &str| {
                let flags = vm_flags(start + at);
                (flags.iter().any(|f| f == flag), flags)
            };
            if whole > 0 {
                for at in [0, whole - 1] {
                    let (hg, flags) = has(at, "hg");
                    assert!(hg, "{huge} {len}: byte {at} is not advised huge: {flags:?}");
                }
            }
            if whole < len {
                for at in [whole, len - 1] {
                    let (nh, flags) = has(at, "nh");
                    assert!(
                        nh && !flags.iter().any(|f| f == "hg"),
                        "{huge} {len}: byte {at} may take a huge page past the region: {flags:?}"
                    );
                }
            }
        }
    }

    /// The flags of the mapping holding `at`, as `/proc/self/smaps` gives them.
    #[cfg(target_os = "linux")]
    fn vm_flags(at: usize) -> Vec<String> {
        let smaps = std::fs::read_to_string("/proc/self/smaps").unwrap();
        let mut holds = false;
        for line in smaps.lines() {
            let range = line.split(' ').next().and_then(|r| r.split_once('-'));
            let range = range
                .map(|(lo, hi)| (usize::from_str_radix(lo, 16), usize::from_str_radix(hi, 16)));
            if let Some((Ok(lo), Ok(hi))) = range {
                holds = lo <= at && at < hi;
            } else if let Some(flags) = line.strip_prefix("VmFlags:").filter(|_| holds) {
                return flags.split_whitespace().map(String::from).collect();
            }
        }
        panic!("no mapping holds {at:#x}");
    }
}
