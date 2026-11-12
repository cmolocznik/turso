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
use crate::storage::atoll::epoch::Epochs;
use crate::storage::atoll::once::SetOnce;
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
pub(super) const BATCH_CLASS: usize = (BATCH_SHIFT - MIN_PAGE_SHIFT) as usize;
/// Runs of 1, 2 and 4 batches: the largest class, 8 MiB, takes 4.
const ORDERS: usize = CLASS_COUNT - BATCH_CLASS;
/// Free-frame bitmap words per batch.
pub(super) const WORDS: usize = GRAINS as usize / 64;
/// `owner` of a batch that continues a run begun by an earlier batch.
const CONTINUED: u8 = u8::MAX;
/// `owner` of a lend batch (DESIGN.md 9.1.1): lent for kept copies, the image bank or the log's
/// ring, so closed to every cache promise and grow for as long as it is one.
const LENT: u8 = u8::MAX - 1;
/// `owner` of a batch a provide's drain released for the lend road and the rare path has not yet
/// taken: no class holds it and it is not in the unowned set, so no cache claim or grow takes it.
const LENDING: u8 = u8::MAX - 2;
/// The beneficiary byte that names the lend road: a provide's drain.
const LEND_ROAD: u64 = 0xff;

/// The smallest budget a pool takes: one frame of the largest class and one batch beside it, so a
/// large value and the tree pages leading to it fit together.
pub const MIN_BUDGET: usize = (1 << (ORDERS - 1)) * BATCH + BATCH;

/// The batches at the region's start that are cache batches for good (DESIGN.md 9.1.1): the
/// aligned run the largest class takes and one batch beside it. No lend batch is taken from them,
/// so a lend never breaks the run the largest class needs.
pub const CORE: u32 = (MIN_BUDGET / BATCH) as u32;

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
/// whether it is draining, whether a release owns it, the class it is drained for, and the frames
/// taken from it (DESIGN.md 9.1.2). Whoever changes who holds the batch, or its flags, validates
/// the whole word; the count changes by an add that cannot fail, and a change of the low bits is
/// published by adding the difference, so it never stores over the count.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct State(u64);

const OWNER: u64 = 0xff;
/// No new frame is taken from the batch; it leaves its class once every frame in it is free.
const DRAINING: u64 = 1 << 8;
/// The batch's last frame is back while it drains: the release under way owns it.
const RELEASING: u64 = 1 << 9;
const BENEFICIARY_SHIFT: u32 = 10;
const BENEFICIARY: u64 = 0xff << BENEFICIARY_SHIFT;
/// The owner, the flags and the beneficiary: what a change of who holds the batch publishes.
const LOW: u64 = (1 << TAKEN_SHIFT) - 1;
const TAKEN_SHIFT: u32 = 24;
const TAKEN_ONE: u64 = 1 << TAKEN_SHIFT;

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

    /// Frames taken or promised: at most [`GRAINS`], in the word's top 40 bits.
    fn taken(self) -> u32 {
        let taken = self.0 >> TAKEN_SHIFT;
        debug_assert!(taken <= GRAINS as u64, "{self:?}");
        taken as u32
    }

    fn beneficiary(self) -> Option<SizeClass> {
        let b = (self.0 & BENEFICIARY) >> BENEFICIARY_SHIFT;
        (b != 0 && b != LEND_ROAD)
            .then(|| SizeClass::new(b as usize - 1))
            .flatten()
    }

    /// Drained for the lend road: released, it waits as [`LENDING`] for the rare path.
    fn lend_road(self) -> bool {
        (self.0 & BENEFICIARY) >> BENEFICIARY_SHIFT == LEND_ROAD
    }

    fn has_beneficiary(self) -> bool {
        self.0 & BENEFICIARY != 0
    }

    fn with_beneficiary(self, to: Beneficiary) -> Self {
        let b = match to {
            Beneficiary::Class(class) => class.index() as u64 + 1,
            Beneficiary::LendRoad => LEND_ROAD,
        };
        Self(self.0 & !BENEFICIARY | b << BENEFICIARY_SHIFT)
    }

    /// Frames may be taken from it for `class`.
    fn open_to(self, class: SizeClass) -> bool {
        self.held_by(class) && self.0 & (DRAINING | RELEASING) == 0
    }
}

/// Whom a drained batch goes to once it is empty.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Beneficiary {
    /// A class whose frames are one batch.
    Class(SizeClass),
    /// The lend road: a provide (DESIGN.md 9.1.1).
    LendRoad,
}

/// What a lend batch is lent for (DESIGN.md 9.1.1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum LendKind {
    /// Not a lend batch.
    None = 0,
    /// Released by a provide's drain, waiting for the rare path ([`LENDING`]).
    Lending = 1,
    /// Kept copies of one class.
    Copy = 2,
    /// The checkpoint's image bank.
    Bank = 3,
    /// The log's ring.
    Ring = 4,
    /// A copy batch whose retire's winner owns it, on its way to free or to another class.
    Retiring = 5,
}

const KIND: u64 = 0x7;
const LEND_CLASS_SHIFT: u32 = 3;
const LEND_CLASS: u64 = 0x1f << LEND_CLASS_SHIFT;
const LEND_LOW: u64 = (1 << LENT_SHIFT) - 1;
const LENT_SHIFT: u32 = 24;
const LENT_ONE: u64 = 1 << LENT_SHIFT;

/// A batch's lend word, beside its cache word (DESIGN.md 9.1.1): its kind, its class and the frames
/// promised or lent in it, in the top 40 bits. The count changes by an add, which a moment's
/// overshoot cannot carry into the kind: an overshoot adds at most three a thread, and 2^40 / 3
/// threads' stacks exceed every target's 48-bit user space. A change of kind or class is published
/// by adding the difference, so an overshoot arriving meanwhile stays counted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LendWord(u64);

impl LendWord {
    fn low(kind: LendKind, class: Option<SizeClass>) -> u64 {
        kind as u64 | class.map_or(0, |c| (c.index() as u64 + 1) << LEND_CLASS_SHIFT)
    }

    pub fn kind(self) -> LendKind {
        match self.0 & KIND {
            0 => LendKind::None,
            1 => LendKind::Lending,
            2 => LendKind::Copy,
            3 => LendKind::Bank,
            4 => LendKind::Ring,
            5 => LendKind::Retiring,
            k => unreachable!("lend kind {k}"),
        }
    }

    pub fn class(self) -> Option<SizeClass> {
        let c = (self.0 & LEND_CLASS) >> LEND_CLASS_SHIFT;
        (c != 0).then(|| SizeClass::new(c as usize - 1)).flatten()
    }

    /// Frames promised or lent.
    pub fn count(self) -> u64 {
        self.0 >> LENT_SHIFT
    }

    /// A copy batch of `class`.
    pub fn is_copy_of(self, class: SizeClass) -> bool {
        self.kind() == LendKind::Copy && self.class() == Some(class)
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
/// What a search of a batch, taking nothing, saw.
enum Search {
    /// A free frame the caller can use.
    Usable,
    /// Free frames, none the caller can use.
    Unusable,
    /// No free frame of the class, or the class does not hold the batch open.
    Nothing,
}

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
    /// Per batch: its [`LendWord`] (DESIGN.md 9.1.1).
    lend: Zeroed<AtomicU64>,
    /// Per class, its copy batches, a bit each: a hint the lend word confirms.
    copies: Box<[Bits]>,
    /// The checkpoint whose cut is in effect, or 0: read here by the release of a provided batch
    /// and by the retire of an empty copy batch (DESIGN.md 9.1.1).
    pub cutting: AtomicU64,
    /// Who holds the rare path's token, or [`FREE_TOKEN`]: the lend road's rare path or a
    /// `drain_run` (DESIGN.md 9.1.1). Taken by one strong compare-and-swap; nothing reads it to see
    /// whether it is free.
    rare: AtomicU64,
    /// The supply a batch's release is made in: the pool's
    /// ([`join_pool`](Self::join_pool)). None for a `Memory` of its own.
    supply: SetOnce<Arc<super::supply::Supply>>,
    /// The epochs a hand-over is stamped in: the pool's. None for a `Memory` of its own, whose
    /// hand-overs are stamped 0 and so never held.
    epochs: SetOnce<Arc<Epochs>>,
    /// Per batch: the stamp of the last hand-over of the run it begins (`handed_at`, DESIGN.md
    /// 9.1.3), written before the state word publishes the class.
    handed_at: Zeroed<AtomicU64>,
}

/// The rare path's token, free.
pub const FREE_TOKEN: u64 = 0;
/// The rare path's token, held by the lend road's owner.
pub const LEND_TOKEN: u64 = 1;
/// The rare path's token, held by a `drain_run`.
pub const RUN_TOKEN: u64 = 2;

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
            lend: Zeroed::new(batches as usize)?,
            copies: (0..CLASS_COUNT)
                .map(|_| Bits::new(batches))
                .collect::<Result<_>>()?,
            cutting: AtomicU64::new(0),
            rare: AtomicU64::new(FREE_TOKEN),
            supply: SetOnce::default(),
            epochs: SetOnce::default(),
            handed_at: Zeroed::new(batches as usize)?,
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

    /// The class holding `frame`'s batch, while the frame is taken: a lend batch's from its lend
    /// word (DESIGN.md 9.1.1).
    pub fn class_of(&self, frame: u32) -> SizeClass {
        let batch = frame / GRAINS;
        if self.state(batch).owner() == LENT {
            return self.lend_word(batch).class().expect("a lend batch's class");
        }
        let class = self.state(batch).class();
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

    /// A free frame of `class` that `usable` accepts, from the lowest batch that has one: one pass
    /// over the class's partial batches as read (DESIGN.md 9.1.2). A batch is promised only once
    /// a search of it, taking nothing, saw a free frame `usable` accepts (DESIGN.md 9.1.3, the
    /// Read search): a promise held on a batch keeps every other claim's look off its frames, so
    /// one made for frames this caller cannot use would hide them from a claim that can.
    pub fn take_where(&self, class: SizeClass, usable: impl Fn(u32) -> bool) -> Take {
        let mut unusable = false;
        for batch in self.index(class).partial.ones() {
            match self.search(class, batch, &usable) {
                Search::Usable => {}
                Search::Unusable => {
                    unusable = true;
                    continue;
                }
                Search::Nothing => continue,
            }
            if !self.promise_frame(class, batch) {
                continue;
            }
            // A promise that claims nothing is given back, a change of the supply (it held the
            // batch's count while claims looked), and the pass goes on; if the give-back is the
            // last count back of a batch that began draining after the promise, it releases the
            // batch too (`release`).
            match self.claim_frame(class, batch, &usable) {
                Take::Frame(frame) => return Take::Frame(frame),
                Take::Unusable => {
                    unusable = true;
                    self.unpromise_frame(class, batch);
                }
                Take::None => self.unpromise_frame(class, batch),
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
    ///
    /// One compare-and-swap, tried once: lost, the batch is left for this pass (the word moved by
    /// another claim's promise, a put or a drain), never tried again in it.
    fn promise_frame(&self, class: SizeClass, batch: u32) -> bool {
        let word = &self.state[batch as usize];
        let s = State(word.load(Ordering::Acquire));
        if !s.open_to(class) || s.taken() == frames_per_batch(class) {
            self.unmark_partial(class, batch);
            return false;
        }
        crate::race::point("memory.promise");
        // SeqCst: the change and the reservation's read in `owe_drain` pair with `drain_run`'s.
        if word
            .compare_exchange(s.0, s.0 + TAKEN_ONE, Ordering::SeqCst, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.owe_drain(class, batch);
        true
    }

    /// A reserved run's drain, carried by whoever changes a batch's word (DESIGN.md 9.1.2): after
    /// a promise, or a count back that leaves the batch not draining, a run reserved around the
    /// batch has its drain tried once from here. `drain_run` tries each batch once; one lost to a
    /// change of the word is tried by that change's maker, so the last change before the batch
    /// drains starts it, and no attempt repeats.
    fn owe_drain(&self, class: SizeClass, batch: u32) {
        // SeqCst: one of this read and `drain_run`'s read of the word sees the other's change.
        let r = Reservation(self.reserved.load(Ordering::SeqCst));
        if r.covers(batch) {
            self.start_drain(class, batch, None, None);
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

    /// Claims a free frame of `batch` that `usable` accepts, for the promise taken: one pass over
    /// the batch's bit words, each read once, one `fetch_and` for each usable free frame seen.
    /// With the promise held there are at least as many free frames as promises not yet claimed,
    /// and a word the pass leaves gains no frame but by a put, so a pass that met no put ends with
    /// a frame. If it did not, every usable frame it saw went to another promise and a put during
    /// the pass set a bit behind it, which signalled the released frames: [`Take::None`].
    fn claim_frame(&self, class: SizeClass, batch: u32, usable: &impl Fn(u32) -> bool) -> Take {
        let mut unusable = false;
        for (w, word) in self.words(batch).iter().enumerate() {
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
                    return Take::Frame(frame);
                }
            }
            crate::race::point("memory.claim_word");
        }
        if unusable {
            Take::Unusable
        } else {
            Take::None
        }
    }

    /// Searches `batch` for `class`, taking nothing, before a promise: whether the class holds it
    /// open, and then whether a free frame there is one `usable` accepts. A hint, made before the
    /// promise holds the batch to its class, so the batch may change hands under it: its owner is
    /// read first (the partial bit may be set late, after a retype), and each bit word is masked to
    /// the frames of `class` before a bit is read as one of its frames, so a word written for
    /// another class is never read as frames of this one (the auditor's stale-partial and retype
    /// cases, 2026-11-09). The promise then validates the owner, and the claim checks `usable`
    /// again for the frame it takes.
    fn search(&self, class: SizeClass, batch: u32, usable: &impl Fn(u32) -> bool) -> Search {
        if !self.state(batch).open_to(class) {
            return Search::Nothing;
        }
        let frames = frames_per_batch(class);
        let mut seen = Search::Nothing;
        for (w, word) in self.words(batch).iter().enumerate() {
            let first = w as u32 * 64;
            if first >= frames {
                break;
            }
            let valid = frames - first;
            let mask = if valid >= 64 {
                u64::MAX
            } else {
                (1u64 << valid) - 1
            };
            let mut bits = word.load(Ordering::Acquire) & mask;
            while bits != 0 {
                let b = bits.trailing_zeros();
                bits &= bits - 1;
                if usable(frame_at(class, batch, first + b)) {
                    return Search::Usable;
                }
                seen = Search::Unusable;
            }
        }
        seen
    }

    /// Gives back a promise of `batch` that claimed no frame: a change of the supply, made in its
    /// scope (DESIGN.md 9.1.3, rule 5), as the promise may have hidden the batch's frames from a
    /// claim that looked while it stood.
    fn unpromise_frame(&self, class: SizeClass, batch: u32) {
        match self.supply.get_ref() {
            Some(supply) => supply.change(|event| {
                self.count_frame_back(class, batch);
                event.arm();
            }),
            None => {
                self.count_frame_back(class, batch);
            }
        }
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
    ///
    /// One pass over the runs each word showed when read, a strong compare-and-swap for each, tried
    /// once: lost, the pass goes on to the next run it read, so nothing is taken in part and
    /// nothing given back (DESIGN.md 9.1.2).
    fn take_run(&self, order: usize) -> Option<u32> {
        for exact in order..ORDERS {
            for (w, word) in self.unowned.0.iter().enumerate() {
                let mut seen = word.load(Ordering::Acquire);
                let mut fits = runs_of(self.outside_reservation(w, seen), exact);
                while fits != 0 {
                    let at = fits.trailing_zeros();
                    fits &= fits - 1;
                    let run = ((1u64 << (1 << order)) - 1) << at;
                    if seen & run != run {
                        continue;
                    }
                    crate::race::point("memory.take_run");
                    match word.compare_exchange(
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
            self.publish_low(b, CONTINUED as u64);
        }
        // The shortage gate's holder waiting for this class gets the run's first frame, set aside
        // before the run is published, so no claim takes it first (DESIGN.md 9.1.3, phase A).
        let supply = self.supply.get_ref().filter(|s| s.wants(class));
        let n = frames_per_batch(class);
        for (w, word) in self.words(start).iter().enumerate() {
            let below = n.saturating_sub(w as u32 * 64).min(64);
            let mut bits = if below == 64 {
                u64::MAX
            } else {
                (1 << below) - 1
            };
            if w == 0 && supply.is_some() {
                bits &= !1;
            }
            word.store(bits, Ordering::Relaxed);
        }
        if supply.is_some() {
            // Counted taken before the state word publishes the class, as a claim's promise is.
            self.state[start as usize].fetch_add(TAKEN_ONE, Ordering::Relaxed);
        }
        // Before the state word: a load of the word that shows the class shows the stamp.
        self.handed_at[start as usize].store(self.hand_over_stamp(), Ordering::Relaxed);
        // Release: a take that reads it sees the frames free, and the stamp.
        self.publish_low(start, State::holding(class).0);
        // Pairs with a reservation's drain: either it sees this class hold the run, or the check
        // below sees the reservation.
        fence(Ordering::SeqCst);
        index.partial.set(start);
        if check {
            let r = Reservation(self.reserved.load(Ordering::Acquire));
            if (start..start + run).any(|b| r.covers(b)) {
                self.start_drain(class, start, None, None);
            }
        }
        if let Some(supply) = supply {
            let frame = start * GRAINS;
            if supply.offer(class, frame) {
                supply.change(|event| event.arm());
            } else {
                // The demand was withdrawn meanwhile: the frame is free as the rest.
                self.put(class, frame);
            }
        }
    }

    /// Whether the batch of `frame` of `class` drains: no new owner takes a frame of it.
    pub fn draining_at(&self, class: SizeClass, frame: u32) -> bool {
        self.state(position(class, frame).0).draining()
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
    ///
    /// One subtract, which cannot fail. The subtract that leaves a draining batch no frame taken
    /// is that zero's one owner, and claims the release by setting `RELEASING`; a second drain of
    /// a draining batch never claims it (`start_drain`), so the release happens once.
    fn count_frame_back(&self, class: SizeClass, batch: u32) -> bool {
        let word = &self.state[batch as usize];
        // SeqCst: as `promise_frame`'s, for `owe_drain`.
        let s = State(word.fetch_sub(TAKEN_ONE, Ordering::SeqCst));
        debug_assert!(s.held_by(class) && s.taken() > 0, "{s:?}");
        crate::race::point("memory.count_back");
        if s.draining() {
            if s.taken() == 1 && !State(word.fetch_or(RELEASING, Ordering::AcqRel)).releasing() {
                self.release(class, batch);
                return true;
            }
            return false;
        }
        // After the count: a take clearing the bit either follows this or sees the count.
        self.index(class).partial.set(batch);
        self.owe_drain(class, batch);
        false
    }

    /// Sets the low bits of `batch`'s word (owner, flags, beneficiary) to `low` by adding the
    /// difference, which leaves the count as it is.
    fn publish_low(&self, batch: u32, low: u64) {
        let word = &self.state[batch as usize];
        let old = word.load(Ordering::Acquire) & LOW;
        // Release: whatever was set up before the publication, to whoever reads the new owner.
        let before = word.fetch_add(low.wrapping_sub(old), Ordering::AcqRel);
        debug_assert_eq!(
            before & LOW,
            old,
            "the low bits of batch {batch} changed meanwhile"
        );
    }

    /// Whether `class` holds `batch`.
    fn owns(&self, class: SizeClass, batch: u32) -> bool {
        self.state(batch).held_by(class)
    }

    /// Hands an emptied draining batch, which this thread released, to the run reserved around
    /// it, or else to the class it was drained for, or back to the free runs.
    fn release(&self, class: SizeClass, batch: u32) {
        // A release is supply, whichever path made it (a put, a promise given back, a drain
        // started on an empty batch): made in a change scope of the pool's supply, joining one
        // open on this thread, and armed (DESIGN.md 9.1.3).
        match self.supply.get_ref() {
            Some(supply) => supply.change(|event| {
                self.release_batch(class, batch);
                event.arm();
            }),
            None => self.release_batch(class, batch),
        }
    }

    /// The pool this memory serves: the supply every batch released from now on is made in, and
    /// the epochs every hand-over is stamped in. Set once, as the pool opens.
    pub(super) fn join_pool(&self, supply: &Arc<super::supply::Supply>, epochs: &Arc<Epochs>) {
        self.supply.set(supply.clone());
        self.epochs.set(epochs.clone());
    }

    /// A fresh stamp for a hand-over: it advances the global epoch, so every step under way now
    /// announced below it, and one entered later announces it or above (DESIGN.md 9.1.3).
    fn hand_over_stamp(&self) -> u64 {
        self.epochs.get_ref().map_or(0, |e| e.stamp())
    }

    /// The stamp that holds the aligned run of `class`'s order around `batch` for the classes
    /// handed its batches, if one is still held at `horizon`: the highest of their stamps, once
    /// above `horizon` (a step under way at that hand-over has not ended). A claim of another
    /// class does not choose it to drain (DESIGN.md 9.1.3).
    pub fn held_until(&self, class: SizeClass, batch: u32, horizon: u64) -> Option<u64> {
        let run = 1u32 << order(class);
        let start = batch & !(run - 1);
        (start..start + run)
            .map(|b| self.handed_at[b as usize].load(Ordering::Relaxed))
            .max()
            .filter(|&stamp| stamp > horizon)
    }

    /// The release itself: the batch's bits cleared, then the batch given to the lend road, its
    /// beneficiary or the run reserved around it, or back free.
    fn release_batch(&self, class: SizeClass, batch: u32) {
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
        if self.state(batch).lend_road() {
            self.release_for_lend(batch, run);
            return;
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
                    self.publish_low(b, 0);
                }
                for b in batch..batch + run {
                    self.give(b);
                }
            }
        }
    }

    /// Publishes the batches of a run a provide drained, emptied by this release, as [`LENDING`]
    /// (DESIGN.md 9.1.1). Each batch's lend word first, then its cache word, the moment it becomes
    /// claimable; then, after a fence, the reservation and the cut: a batch a run covers goes to
    /// the run, and with no cut in effect the batch goes back free, each by the one
    /// compare-and-swap that claims it off `LENDING`, which the rare path's owner, `drain_run` and
    /// `end_cut`'s sweep claim it by too, so it leaves `LENDING` once.
    fn release_for_lend(&self, batch: u32, run: u32) {
        for b in batch..batch + run {
            self.publish_lend_low(b, LendWord::low(LendKind::Lending, None));
            crate::race::point("memory.lending_kind");
            self.publish_low(b, LENDING as u64);
        }
        // SeqCst: pairs with `drain_run`'s reserve and with `end_cut`'s store of the cut, so of
        // this read and theirs one sees the other's change.
        fence(Ordering::SeqCst);
        crate::race::point("memory.lending_read");
        let r = Reservation(self.reserved.load(Ordering::SeqCst));
        let cut = self.cutting.load(Ordering::SeqCst);
        for b in batch..batch + run {
            if (r.covers(b) || cut == 0) && self.claim_lending(b) {
                self.unlend(b);
            }
        }
    }

    /// Claims `batch` off [`LENDING`] by one compare-and-swap of its cache word to free: true for
    /// the one claimant that wins it, who then finishes it ([`unlend`](Self::unlend) or a
    /// publication of [`LENT`]).
    pub fn claim_lending(&self, batch: u32) -> bool {
        self.state[batch as usize]
            .compare_exchange(LENDING as u64, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Claims `batch` off [`LENDING`] straight into [`LENT`], for the rare path's owner, which then
    /// publishes its lend word.
    pub fn claim_lending_lent(&self, batch: u32) -> bool {
        self.state[batch as usize]
            .compare_exchange(
                LENDING as u64,
                LENT as u64,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Gives back free `batch`, claimed off [`LENDING`] or out of a copy batch, whose cache word
    /// reads free: its lend word first, then the unowned set or the reserved run.
    pub fn unlend(&self, batch: u32) {
        self.publish_lend_low(batch, LendWord::low(LendKind::None, None));
        self.give(batch);
    }

    /// Sets the low bits of `batch`'s lend word (kind and class) by adding the difference, which
    /// leaves the count, and any overshoot in it, as it is.
    fn publish_lend_low(&self, batch: u32, low: u64) {
        let word = &self.lend[batch as usize];
        let old = word.load(Ordering::Acquire) & LEND_LOW;
        // AcqRel: what was set up before, the frames' bits, to whoever reads the new kind.
        let before = word.fetch_add(low.wrapping_sub(old), Ordering::AcqRel);
        debug_assert_eq!(
            before & LEND_LOW,
            old,
            "the kind of batch {batch} changed meanwhile"
        );
    }

    /// `batch`'s lend word.
    pub fn lend_word(&self, batch: u32) -> LendWord {
        LendWord(self.lend[batch as usize].load(Ordering::SeqCst))
    }

    /// Whether `batch` is a lend batch or waits as one ([`LENT`] or [`LENDING`]).
    pub fn is_lend_batch(&self, batch: u32) -> bool {
        matches!(self.state(batch).owner(), LENT | LENDING)
    }

    /// Whether the reserved run covers `batch`.
    pub fn reserved_covers(&self, batch: u32) -> bool {
        Reservation(self.reserved.load(Ordering::SeqCst)).covers(batch)
    }

    /// Whether `batch` waits as [`LENDING`].
    pub fn is_lending(&self, batch: u32) -> bool {
        self.state(batch).owner() == LENDING
    }

    /// The batches outside the core that are free and outside the reserved run, highest first, as
    /// read, each word once: a hint [`take_free_batch`](Self::take_free_batch) confirms. Allocates
    /// nothing (DESIGN.md 9.1.1, fixed storage).
    pub fn free_batches_outside_core(&self) -> impl Iterator<Item = u32> + '_ {
        self.unowned
            .0
            .iter()
            .enumerate()
            .rev()
            .flat_map(move |(w, word)| {
                let mut bits = self.outside_reservation(w, word.load(Ordering::Acquire));
                std::iter::from_fn(move || {
                    (bits != 0).then(|| {
                        let b = 63 - bits.leading_zeros();
                        bits &= !(1 << b);
                        w as u32 * 64 + b
                    })
                })
                .filter(move |&batch| batch >= CORE && batch < self.batches)
            })
    }

    /// Takes free `batch` for the lend road by one `fetch_and` of its bit in the unowned set: true
    /// if it was free. Its cache word reads free; the taker publishes it [`LENT`].
    pub fn take_free_batch(&self, batch: u32) -> bool {
        debug_assert!(batch >= CORE, "batch {batch} is in the core");
        self.unowned.clear(batch)
    }

    /// Publishes `batch`, which this thread holds (a free batch it took, or one it claimed off
    /// `LENDING` into `LENT`), as a lend batch of `kind` and `class`, its frames free (DESIGN.md
    /// 9.1.1, into a copy batch): `LENT` in the cache word, closing it to the cache; the frames'
    /// bits; the lend word by adding the difference; and a copy batch into its class's set. The
    /// caller reads the cut after it.
    pub fn publish_lent(&self, batch: u32, kind: LendKind, class: SizeClass) {
        debug_assert!(batch >= CORE, "batch {batch} is in the core");
        let word = &self.state[batch as usize];
        let s = State(word.load(Ordering::Acquire));
        debug_assert!(
            s.0 == 0 || s.owner() == LENT,
            "batch {batch} is not ours: {s:?}"
        );
        if s.owner() != LENT {
            word.store(LENT as u64, Ordering::Release);
        }
        self.set_frames_free(batch, class);
        crate::race::point("memory.publish_lent");
        self.publish_lend_low(batch, LendWord::low(kind, Some(class)));
        if kind == LendKind::Copy {
            self.copies[class.index()].set(batch);
        }
    }

    /// Every frame of `class` in `batch` free, in its bit words.
    fn set_frames_free(&self, batch: u32, class: SizeClass) {
        let n = frames_per_batch(class);
        for (w, word) in self.words(batch).iter().enumerate() {
            let below = n.saturating_sub(w as u32 * 64).min(64);
            let bits = if below == 64 {
                u64::MAX
            } else {
                (1 << below) - 1
            };
            word.store(bits, Ordering::Release);
        }
    }

    /// `class`'s copy batches, as the hint reads.
    pub fn copy_batches(&self, class: SizeClass) -> impl Iterator<Item = u32> + '_ {
        self.copies[class.index()].ones()
    }

    /// Promises `n` frames of copy batch `batch` of `class` by one add (DESIGN.md 9.1.1). The word
    /// it returns decides: still a copy batch of the class, with `n` frames unpromised before the
    /// add, and they are promised; else the same `n` is subtracted again and the word after that
    /// is returned, for whoever takes the count to zero.
    pub fn promise_lent(
        &self,
        batch: u32,
        class: SizeClass,
        n: u32,
    ) -> std::result::Result<(), LendWord> {
        let word = &self.lend[batch as usize];
        let add = n as u64 * LENT_ONE;
        let before = LendWord(word.fetch_add(add, Ordering::SeqCst));
        crate::race::point("memory.promise_lent");
        if before.is_copy_of(class) && before.count() + n as u64 <= frames_per_batch(class) as u64 {
            return Ok(());
        }
        Err(LendWord(word.fetch_sub(add, Ordering::SeqCst) - add))
    }

    /// One frame of lend batch `batch`, of `kind` and `class` (the ring's or the image bank's):
    /// one add to its count, then one pass over its bit words, one `fetch_and` for each free frame
    /// seen (DESIGN.md 9.1.2's claim in a batch). None, and the add given back, if the word shows
    /// another kind or class or no frame unpromised, or the pass met none.
    pub fn lend_one(&self, batch: u32, kind: LendKind, class: SizeClass) -> Option<u32> {
        let word = &self.lend[batch as usize];
        let before = LendWord(word.fetch_add(LENT_ONE, Ordering::SeqCst));
        if before.kind() == kind
            && before.class() == Some(class)
            && before.count() < frames_per_batch(class) as u64
        {
            for (w, bits) in self.words(batch).iter().enumerate() {
                let mut seen = bits.load(Ordering::Acquire);
                while seen != 0 {
                    let b = seen.trailing_zeros();
                    seen &= seen - 1;
                    // Acquire: whatever the frame's last holder did before giving it back.
                    if bits.fetch_and(!(1 << b), Ordering::Acquire) & 1 << b != 0 {
                        return Some(frame_at(class, batch, w as u32 * 64 + b));
                    }
                }
            }
        }
        word.fetch_sub(LENT_ONE, Ordering::SeqCst);
        None
    }

    /// Gives back `n` frames promised of `batch` and not taken: the word after the subtract.
    pub fn unpromise_lent(&self, batch: u32, n: u32) -> LendWord {
        let add = n as u64 * LENT_ONE;
        LendWord(self.lend[batch as usize].fetch_sub(add, Ordering::SeqCst) - add)
    }

    /// Clears `mask` in bit word `w` of `batch` by one `fetch_and`: true if every bit of it was
    /// set; else the bits it did clear are set back, and false.
    pub fn take_lent_bits(&self, batch: u32, w: usize, mask: u64) -> bool {
        let word = &self.words(batch)[w];
        // Acquire: whatever the frames' last holders did before giving them back.
        let before = word.fetch_and(!mask, Ordering::Acquire);
        if before & mask == mask {
            return true;
        }
        crate::race::point("memory.take_lent_bits");
        word.fetch_or(before & mask, Ordering::Release);
        false
    }

    /// `batch`'s bit word `w`, as read.
    pub fn lent_bits(&self, batch: u32, w: usize) -> u64 {
        self.words(batch)[w].load(Ordering::Acquire)
    }

    /// Gives back lent `frame` of `class` in `batch`: its bit, then one subtract from the count.
    /// The word after it, so whoever takes the count to zero tries the retire.
    pub fn return_lent(&self, class: SizeClass, batch: u32, frame: u32) -> LendWord {
        let (b, i) = position(class, frame);
        debug_assert_eq!(b, batch);
        let bit = 1 << (i % 64);
        // Release: whatever this thread did with the frame, to whoever takes it next.
        let before = self.words(batch)[i as usize / 64].fetch_or(bit, Ordering::Release);
        debug_assert_eq!(before & bit, 0, "lent frame {frame} given back twice");
        LendWord(self.lend[batch as usize].fetch_sub(LENT_ONE, Ordering::SeqCst) - LENT_ONE)
    }

    /// Retires empty copy batch `batch` (DESIGN.md 9.1.1): one compare-and-swap of its lend word
    /// from a copy batch of `class` with nothing lent to retiring, tried once. The winner owns the
    /// batch, out of its class's set; it gives it back free ([`free_retired`](Self::free_retired))
    /// or retypes it ([`publish_lent`](Self::publish_lent)). A loser does not look again.
    pub fn retire(&self, batch: u32, class: SizeClass) -> bool {
        let empty = LendWord::low(LendKind::Copy, Some(class));
        let retiring = LendWord::low(LendKind::Retiring, Some(class));
        crate::race::point("memory.retire");
        if self.lend[batch as usize]
            .compare_exchange(empty, retiring, Ordering::SeqCst, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.copies[class.index()].clear(batch);
        true
    }

    /// Gives back free a batch this thread retired: its lend word none, its cache word free, then
    /// the unowned set or the reserved run.
    pub fn free_retired(&self, batch: u32) {
        self.publish_lend_low(batch, LendWord::low(LendKind::None, None));
        self.state[batch as usize].store(0, Ordering::Release);
        self.give(batch);
    }

    /// Takes the rare path's token for `holder` by one strong compare-and-swap from free: false if
    /// another holds it. Nothing reads the token to see whether it is free.
    pub fn take_token(&self, holder: u64) -> bool {
        self.rare
            .compare_exchange(FREE_TOKEN, holder, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Lets the rare path's token go: freed by one swap. Its losers are woken by the pool's gate
    /// cell after this (DESIGN.md 9.1.3, rule 4).
    pub fn let_token_go(&self, holder: u64) {
        let was = self.rare.swap(FREE_TOKEN, Ordering::SeqCst);
        debug_assert_eq!(was, holder, "the rare path's token was not {holder}'s");
    }

    /// Whether the aligned run of `class`'s order around `batch` holds a [`LENT`] batch, which no
    /// drain frees: for `drain_run`'s caller, holding the token, before it reserves.
    pub fn run_holds_lent(&self, class: SizeClass, batch: u32) -> bool {
        let run = 1u32 << order(class);
        let start = batch & !(run - 1);
        (start..start + run).any(|b| self.state(b).owner() == LENT)
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
    ///
    /// One subtract, which cannot fail: each batch of the run is counted once (`give` and
    /// `drain_run` take its bit in the unowned set, which only one of them gets).
    fn collect(&self) {
        let r = Reservation(
            self.reserved
                .fetch_sub(1 << MISSING_SHIFT, Ordering::AcqRel),
        );
        let next = r.collected();
        if next.missing() == 0 {
            self.assign(next.class(), next.start(), false);
            self.reserved.store(0, Ordering::Release);
        }
    }

    /// Drains `batch` of `class` (as [`drain`](Self::drain)) for `beneficiary`, a class whose
    /// batches are one batch long, which gets it once it is empty. Claims drain through
    /// [`drain_chosen_for`](Self::drain_chosen_for); this one is the tests'.
    #[cfg(test)]
    pub fn drain_for(&self, class: SizeClass, batch: u32, beneficiary: SizeClass) -> bool {
        debug_assert_eq!(order(beneficiary), 0);
        self.start_drain(class, batch, Some(Beneficiary::Class(beneficiary)), None)
    }

    /// [`drain_for`](Self::drain_for) as a claim's choice made at `horizon`: nothing is started if
    /// the batch is held for the class it was handed to (DESIGN.md 9.1.3), read after the word.
    pub fn drain_chosen_for(
        &self,
        class: SizeClass,
        batch: u32,
        beneficiary: SizeClass,
        horizon: u64,
    ) -> bool {
        debug_assert_eq!(order(beneficiary), 0);
        self.start_drain(
            class,
            batch,
            Some(Beneficiary::Class(beneficiary)),
            Some(horizon),
        )
    }

    /// Drains `batch` of `class` (as [`drain`](Self::drain)) for the lend road: a provide
    /// (DESIGN.md 9.1.1). Released, the batch waits as [`LENDING`] for the rare path.
    pub fn drain_for_lend(&self, class: SizeClass, batch: u32) -> bool {
        debug_assert!(batch >= CORE, "batch {batch} is in the core");
        self.start_drain(class, batch, Some(Beneficiary::LendRoad), None)
    }

    /// Stops new frames being taken from `batch` of `class`; it leaves the class once every frame
    /// in it is free. True if it was not draining already. False, and nothing done, if `class`
    /// no longer holds the batch: it was chosen before it left.
    #[cfg(test)]
    pub fn drain(&self, class: SizeClass, batch: u32) -> bool {
        self.start_drain(class, batch, None, None)
    }

    ///
    /// One strong compare-and-swap of the whole word from the value read, tried once (DESIGN.md
    /// 9.1.2): the owner and flags validated with the count, so a drain chosen for a batch that
    /// has since gone to another owner changes nothing. Lost, no drain is started: the word moved
    /// by a promise, a put (which signals the released frames), or another drain or release. A
    /// batch read already draining only gets a missing beneficiary, never the release, whatever
    /// count it shows: the count back that took it to zero owns that zero.
    ///
    /// A claim's choice (`chosen`, the horizon it chose at) starts nothing on a batch held for its
    /// class: the stamp is read after the word, so it is the stamp of the hand-over that word
    /// published or a later one (DESIGN.md 9.1.3). Every other drain is owed and takes no check.
    fn start_drain(
        &self,
        class: SizeClass,
        batch: u32,
        beneficiary: Option<Beneficiary>,
        chosen: Option<u64>,
    ) -> bool {
        let word = &self.state[batch as usize];
        let s = State(word.load(Ordering::Acquire));
        if !s.held_by(class) || s.releasing() {
            return false;
        }
        if chosen
            .is_some_and(|horizon| self.handed_at[batch as usize].load(Ordering::Relaxed) > horizon)
        {
            return false;
        }
        crate::race::point("memory.drain");
        // The first class to ask keeps it.
        let mut next = match beneficiary {
            Some(b) if !s.has_beneficiary() => s.with_beneficiary(b),
            _ => s,
        };
        if s.draining() {
            if next != s {
                let _ = word.compare_exchange(s.0, next.0, Ordering::AcqRel, Ordering::Acquire);
            }
            return false;
        }
        next.0 |= DRAINING;
        if s.taken() == 0 {
            next.0 |= RELEASING;
        }
        if word
            .compare_exchange(s.0, next.0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        let index = self.index(class);
        index.leave_live(batch);
        index.partial.clear(batch);
        if next.releasing() {
            self.release(class, batch);
        }
        true
    }

    /// Holds the aligned run of `class`'s order around `batch` for `class`, and drains every
    /// batch in it that a class holds: the run goes to `class` once it is free. False while
    /// another run is held. A batch released here is a change of the supply scope the caller
    /// holds the rare path's token in, so its wakers run once that scope closes, the token let
    /// go before (DESIGN.md 9.1.3).
    #[cfg(test)]
    pub fn drain_run(&self, class: SizeClass, batch: u32) -> bool {
        self.drain_run_chosen(class, batch, None)
    }

    /// [`drain_run`](Self::drain_run), and as a claim's choice made at `horizon` (`chosen`):
    /// nothing is reserved while a batch of the run is held for the class it was handed to
    /// (DESIGN.md 9.1.3). Once reserved, every drain of the run is owed and takes no check.
    pub fn drain_run_chosen(&self, class: SizeClass, batch: u32, chosen: Option<u64>) -> bool {
        if chosen.is_some_and(|horizon| self.held_until(class, batch, horizon).is_some()) {
            return false;
        }
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
            // A batch a provide's release left `LENDING` goes to the run by the one
            // compare-and-swap that claims it, as the release's own look would give it (DESIGN.md
            // 9.1.1). The caller holds the rare path's token, so no `LENT` is published meanwhile.
            if self.state(b).owner() == LENDING {
                if self.claim_lending(b) {
                    self.unlend(b);
                }
                continue;
            }
            let mut head = b;
            while self.state(head).owner() == CONTINUED {
                head -= 1;
            }
            // SeqCst: one of this read and a promise's or count back's read of the reservation
            // (`owe_drain`) sees the other's change, so a drain lost here is tried there.
            if let Some(holder) = State(self.state[head as usize].load(Ordering::SeqCst)).class() {
                self.start_drain(holder, head, None, None);
            }
        }
        true
    }

    /// Reserves the run of `class` around `batch` and nothing more: the state between
    /// `drain_run`'s reservation and its pass, which other threads see (DESIGN.md 9.1.1), for a
    /// test. False while another run is held.
    #[cfg(test)]
    pub fn reserve_only(&self, class: SizeClass, batch: u32) -> bool {
        let run = 1u32 << order(class);
        let r = Reservation::new(batch & !(run - 1), order(class), class);
        self.reserved
            .compare_exchange(0, r.0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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

    /// Every frame of the batches `class` holds and is not draining, in order of frame: what an
    /// eviction round walks when its sample found nothing to take (DESIGN.md 9.1.2).
    pub fn live_frames(&self, class: SizeClass) -> impl Iterator<Item = u32> + '_ {
        self.index(class)
            .live
            .ones()
            .flat_map(move |b| (0..frames_per_batch(class)).map(move |i| frame_at(class, b, i)))
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

    /// Whether `batch` is draining.
    pub fn is_draining(&self, batch: u32) -> bool {
        self.state(batch).draining()
    }

    /// A batch draining for the lend road, if one is: its class and the first batch of its run.
    pub fn draining_for_lend(&self) -> Option<(SizeClass, u32)> {
        (CORE..self.batches).rev().find_map(|b| {
            let s = self.state(b);
            (s.draining() && s.lend_road()).then(|| s.class().map(|c| (c, b)))?
        })
    }

    /// The cache batch of the highest number whose whole run lies outside the core and that is
    /// not draining: what a provide drains for the lend road (DESIGN.md 9.1.1), with its class.
    pub fn highest_cache_batch_outside_core(&self) -> Option<(SizeClass, u32)> {
        (CORE..self.batches).rev().find_map(|b| {
            let s = self.state(b);
            let class = s.class()?;
            (!s.draining() && !s.releasing()).then_some((class, b))
        })
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

    #[cfg(test)]
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
pub(super) mod tests {
    use super::*;

    /// A take whose search of a batch finds free frames, none of them usable (a Read whose frames
    /// are not quiet yet), promises nothing: a promise would hold the batch's count while another
    /// claim, which could use the frame, looked, and give it back with nothing changed (the
    /// auditor's `normal-refund` case, DESIGN.md 9.1.3, the Read search). A usable frame is taken.
    #[test]
    fn a_take_that_finds_no_usable_frame_promises_none() {
        use crate::race;
        let m = Memory::new(8 * BATCH).unwrap();
        assert!(m.grow(class(10)));
        let promised = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let promised = promised.clone();
            race::at("memory.promise", move || {
                promised.store(true, Ordering::SeqCst)
            });
        }
        assert!(matches!(m.take_where(class(10), |_| false), Take::Unusable));
        assert!(
            !promised.load(Ordering::SeqCst),
            "a batch promised with no usable frame in it"
        );
        assert_eq!(
            m.state(m.index(class(10)).held.ones().next().unwrap())
                .taken(),
            0
        );
        // A batch the class does not hold open is not read as its frames.
        assert!(matches!(m.take_where(class(9), |_| true), Take::None));
        assert!(matches!(m.take_where(class(10), |_| true), Take::Frame(_)));
    }

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

    /// The audit's `one-try-real-memory` run (2026-11-08): `drain_run` reserves a 4 MiB run over
    /// two 4 KiB batches, and between its read of batch 0 and its drain a claim promises and takes
    /// a frame there, so `drain_run`'s one try is lost. The obligation goes with the word: the
    /// claim, or the put that returns the frame, starts the drain, and the run is collected whole.
    #[test]
    fn a_drain_a_reserved_run_lost_to_a_claim_is_started_by_whoever_changes_the_batch_next() {
        use crate::race;
        let m = Arc::new(Memory::new(5 * BATCH).unwrap());
        for _ in 0..5 {
            assert!(m.grow(class(0)));
        }
        let taken = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
        {
            let (m, taken) = (m.clone(), taken.clone());
            race::at("memory.drain", move || {
                *taken.lock() = Some(m.take(class(0)).expect("a free 4 KiB frame"));
            });
        }
        assert!(m.drain_run(class(10), 0));
        assert!(
            !race::pending("memory.drain"),
            "the claim ran between the read and the drain"
        );
        let frame = taken.lock().take().unwrap();
        assert_eq!(
            frame / GRAINS,
            0,
            "the claim took a frame of the run's first batch"
        );
        assert!(
            m.put(class(0), frame),
            "the frame's return released the batch to the run"
        );
        assert_eq!(
            Reservation(m.reserved.load(Ordering::Acquire)),
            Reservation(0),
            "the reserved run was collected whole: {}",
            m.describe()
        );
        assert_eq!(m.held()[10], 2 * BATCH, "{}", m.describe());
        assert!(m.take(class(10)).is_some());
    }

    /// Eight batches of 4 KiB frames, all free, and a cut in effect: what the lend tests start from.
    fn eight_held(cut: u64) -> Arc<Memory> {
        let m = Arc::new(Memory::new(8 * BATCH).unwrap());
        for _ in 0..8 {
            assert!(m.grow(class(0)));
        }
        m.cutting.store(cut, Ordering::SeqCst);
        m
    }

    /// A batch a provide drains, released while a cut is in effect, waits as `LENDING`: its lend
    /// word says so, no cache take reaches a frame of it and no grow takes it; one claim takes it
    /// off `LENDING`, once (DESIGN.md 9.1.1). With no cut in effect, the release gives it back free.
    #[test]
    fn a_provided_batch_waits_lending_closed_to_the_cache_and_with_no_cut_goes_back_free() {
        let m = eight_held(1);
        assert!(m.drain_for_lend(class(0), 7));
        assert!(m.is_lending(7), "{}", m.describe());
        assert_eq!(m.lend_word(7).kind(), LendKind::Lending);
        let mut taken = Vec::new();
        while let Some(f) = m.take(class(0)) {
            assert_ne!(f / GRAINS, 7, "a cache take reached the provided batch");
            taken.push(f);
        }
        assert_eq!(taken.len(), 7 * GRAINS as usize);
        assert!(!m.grow(class(0)), "a grow took the provided batch");
        assert!(m.claim_lending(7));
        assert!(!m.claim_lending(7), "claimed off LENDING twice");
        m.unlend(7);
        assert_eq!(m.lend_word(7).kind(), LendKind::None);
        assert!(m.grow(class(0)), "the batch went back free");

        let m = eight_held(0);
        assert!(m.drain_for_lend(class(0), 7));
        assert!(!m.is_lending(7), "with no cut the release gives it back");
        assert_eq!(m.lend_word(7).kind(), LendKind::None);
        assert!(m.has_free_run(class(0)));
    }

    /// A provided batch's release racing a run reserved around it: paused between its publication
    /// of `LENDING` and its read of the reservation, a `drain_run` reserves the run and its pass
    /// claims the batch; the release's read then sees the run and its claim loses. The run gets
    /// the batch once and is whole (DESIGN.md 9.1.1, the audit's `lending-release-sc` order).
    #[test]
    fn a_provided_batch_goes_to_a_run_reserved_around_it_once() {
        use crate::race;
        let m = eight_held(1);
        {
            let m = m.clone();
            race::at("memory.lending_read", move || {
                assert!(m.is_lending(7));
                assert!(m.drain_run(class(BATCH_CLASS + 1), 6));
            });
        }
        assert!(m.drain_for_lend(class(0), 7));
        assert!(!race::pending("memory.lending_read"));
        assert_eq!(
            Reservation(m.reserved.load(Ordering::Acquire)),
            Reservation(0),
            "the run was collected whole: {}",
            m.describe()
        );
        assert_eq!(m.held()[BATCH_CLASS + 1], 2 * BATCH, "{}", m.describe());
        assert_eq!(m.lend_word(7).kind(), LendKind::None);
    }

    /// A cache claim of the same class that read a batch before a provide lent it: paused between
    /// its read of the batch's word and its promise, the batch is drained for the lend road,
    /// claimed off `LENDING` and published a copy batch with every frame free. The claim's
    /// compare-and-swap fails on `LENT`, so it takes nothing there, and every frame stays the lend
    /// road's (DESIGN.md 9.1.1: the cache word closes a lend batch to the cache).
    #[test]
    fn a_cache_claim_that_read_a_batch_before_a_provide_lent_it_takes_nothing_there() {
        use crate::race;
        let m = eight_held(1);
        let others = 7 * GRAINS as usize;
        for _ in 0..others {
            let f = m.take(class(0)).expect("a free frame outside batch 7");
            assert_ne!(f / GRAINS, 7);
        }
        {
            let m = m.clone();
            race::at("memory.promise", move || {
                assert!(m.drain_for_lend(class(0), 7), "the provide drained batch 7");
                assert!(m.is_lending(7), "{}", m.describe());
                assert!(m.claim_lending_lent(7));
                m.publish_lent(7, LendKind::Copy, class(0));
            });
        }
        let got = m.take(class(0));
        assert!(
            !race::pending("memory.promise"),
            "the claim reached its promise"
        );
        assert_eq!(got, None, "the claim took a frame of the lent batch");
        assert!(m.lend_word(7).is_copy_of(class(0)));
        assert_eq!(m.lend_word(7).count(), 0);
        let words = (GRAINS as usize).div_ceil(64);
        for w in 0..words {
            assert_eq!(
                m.lent_bits(7, w),
                u64::MAX,
                "frame word {w} of the lent batch lost a bit"
            );
        }
    }

    /// A pause between a provide's two writes into `LENDING` (DESIGN.md 9.1.1): its lend word says
    /// `Lending` and its cache word does not yet, so the rare path's owner, run there, cannot claim
    /// the batch, whose claim compares the cache word. The release then publishes the cache word
    /// and the batch waits `LENDING`, both words agreeing, and is claimed into a copy batch after.
    #[test]
    fn no_claim_takes_a_provided_batch_before_its_cache_word_says_lending() {
        use crate::race;
        let m = eight_held(1);
        {
            let m = m.clone();
            race::at("memory.lending_kind", move || {
                if m.claim_lending_lent(7) {
                    m.publish_lent(7, LendKind::Copy, class(1));
                }
            });
        }
        assert!(m.drain_for_lend(class(0), 7));
        assert!(!race::pending("memory.lending_kind"));
        assert!(m.is_lending(7), "the cache word: {}", m.describe());
        assert_eq!(m.lend_word(7).kind(), LendKind::Lending, "the lend word");
        assert!(m.claim_lending_lent(7));
        m.publish_lent(7, LendKind::Copy, class(1));
        assert!(m.lend_word(7).is_copy_of(class(1)));
    }

    /// What a take's search accepts and its claim then refuses: a frame usable when searched and
    /// not when claimed, as a Read's quiet frame taken by another claim between the two, the one
    /// way a promise is still given back with nothing claimed (DESIGN.md 9.1.3, the Read search).
    pub(in super::super) fn usable_once() -> impl Fn(u32) -> bool {
        let seen = std::cell::Cell::new(false);
        move |_| !seen.replace(true)
    }

    /// A promise given back with nothing claimed, which is the last count back of a batch that
    /// began draining after the promise: it releases the batch (the auditor's race trace of
    /// 2026-11-08; DESIGN.md 9.1.2).
    #[test]
    fn a_promise_given_back_that_empties_a_draining_batch_releases_it() {
        use crate::race;
        let m = eight_held(0);
        {
            let m = m.clone();
            race::at("memory.count_back", move || {
                assert!(m.drain(class(0), 0), "the drain started under the promise");
            });
        }
        let take = m.take_where(class(0), usable_once());
        assert!(!race::pending("memory.count_back"));
        assert!(matches!(take, Take::Unusable));
        assert!(
            !m.owns(class(0), 0),
            "the give-back released the batch it emptied"
        );
    }

    /// The auditor's `unpromise-supply` reproduction (2026-11-08), on a real pool and a real
    /// claim: every batch held by a class; a read promises a frame of a 4 MiB batch it cannot reuse
    /// yet; at that promise a drain of the batch for 2 MiB starts and a real 2 MiB claim parks;
    /// the read gives its promise back, the batch's last count, which releases it. The release
    /// wakes the parked claim, and a claim then takes the memory (DESIGN.md 9.1.2).
    #[cfg(not(shuttle))]
    #[test]
    fn a_claim_parked_while_a_promise_holds_a_draining_batch_wakes_when_the_promise_releases_it() {
        use super::super::{BufferConfig, BufferPool, LocalSource, Purpose};
        use crate::io::{Completion, MemoryIO, IO};
        use crate::race;
        use crate::storage::atoll::meta::TREES;
        use crate::storage::atoll::store::{PageStore, StoreConfig};
        use crate::types::{IOCompletions, IOResult};
        use std::{cell::RefCell, rc::Rc};

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let store = PageStore::open(
            crate::storage::atoll::requests::counted_for_test(io),
            "/db/unpromise-supply",
            StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            [[1; CLASS_COUNT]; TREES],
            0,
        );
        let pool = BufferPool::new(
            BufferConfig::with_budget(MIN_BUDGET),
            Arc::new(LocalSource::new(store.clone())),
            store.clone(),
        )
        .unwrap();
        let (donor, wanted) = (class(10), class(9));
        assert!(pool.memory.grow(donor));
        assert!(pool.memory.grow(donor));
        assert!(pool.memory.grow(class(0)));
        assert!(
            !pool.memory.grow(wanted),
            "all five batches belong to classes"
        );
        let batch = pool.memory.index(donor).held.ones().next().unwrap();
        // Every frame freed just now, under a step still open: none is quiet for a read.
        let old_reader = store.epochs().enter();
        let stamp = store.epochs().stamp();
        for grain in pool.frames.given_up.iter() {
            grain.store(stamp, Ordering::Release);
        }
        let waiting: Rc<RefCell<Option<Completion>>> = Rc::default();
        {
            let (pool, waiting) = (pool.clone(), waiting.clone());
            race::at("memory.claim_word", move || {
                // The read has promised this frame and finds it no longer usable. A drain starts
                // before the promise is given back, which then holds the batch's only count.
                assert_eq!(pool.memory.state(batch).taken(), 1);
                assert!(pool.memory.drain_for(donor, batch, wanted));
                let wait = match pool.claim(wanted, Purpose::New, &[]).unwrap() {
                    IOResult::IO(IOCompletions(c)) => c,
                    IOResult::Done(_) => panic!("the draining batch is not free yet"),
                };
                assert!(!wait.finished(), "the claim parks on the promise");
                *waiting.borrow_mut() = Some(wait);
            });
        }
        let came_to = pool.memory.take_where(donor, usable_once());
        assert!(!race::pending("memory.claim_word"));
        assert!(matches!(came_to, Take::Unusable));
        let wait = waiting.borrow_mut().take().unwrap();
        let woken = wait.finished();
        let free = pool.memory.unowned.ones().count();
        let obtainable = match pool.claim(wanted, Purpose::New, &[]).unwrap() {
            IOResult::Done(frame) => {
                pool.memory.put(wanted, frame);
                true
            }
            IOResult::IO(_) => false,
        };
        drop(old_reader);
        drop(wait);
        drop(pool);
        drop(store);
        assert!(obtainable && free > 0, "the release made memory available");
        assert!(
            woken,
            "the last promise released the drained batch without waking the claim waiting on it"
        );
    }

    /// A run's drain that releases empty batches at once, under the rare path's token, tells the
    /// released frames only once the token is let go: a waker the tell runs may enter the pool
    /// and must find the token free (the auditor's US2 of 2026-11-08: a waker driving the pool
    /// parked on the token its own thread held; DESIGN.md 9.1.2).
    #[cfg(not(shuttle))]
    #[test]
    fn a_release_under_the_run_token_wakes_its_waiters_once_the_token_is_let_go() {
        use super::super::{BufferConfig, BufferPool, LocalSource};
        use crate::io::{MemoryIO, IO};
        use crate::storage::atoll::meta::TREES;
        use crate::storage::atoll::store::{PageStore, StoreConfig};
        use std::task::{Wake, Waker};
        /// Wakes by reading who holds the token.
        struct Sees {
            pool: Arc<BufferPool>,
            woke: AtomicU64,
            token: AtomicU64,
        }
        impl Wake for Sees {
            fn wake(self: std::sync::Arc<Self>) {
                self.wake_by_ref();
            }

            fn wake_by_ref(self: &std::sync::Arc<Self>) {
                self.woke.fetch_add(1, Ordering::SeqCst);
                let holder = self.pool.memory.rare.load(Ordering::SeqCst);
                self.token.store(holder, Ordering::SeqCst);
            }
        }
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let store = PageStore::open(
            crate::storage::atoll::requests::counted_for_test(io),
            "/db/release-under-token",
            StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            [[1; CLASS_COUNT]; TREES],
            0,
        );
        let pool = BufferPool::new(
            BufferConfig::with_budget(MIN_BUDGET),
            Arc::new(LocalSource::new(store.clone())),
            store,
        )
        .unwrap();
        // Every batch the 4 KiB class's and empty: a run's drain releases its batches at once.
        for _ in 0..pool.memory.batches {
            assert!(pool.memory.grow(class(0)));
        }
        let waiting = pool.released_wait();
        let sees = std::sync::Arc::new(Sees {
            pool: pool.clone(),
            woke: AtomicU64::new(0),
            token: AtomicU64::new(u64::MAX),
        });
        waiting.set_waker(&Waker::from(sees.clone()));
        assert!(matches!(
            pool.drain_run(class(10), 0, None),
            super::super::Drained::Ran(true)
        ));
        assert!(waiting.finished(), "the release told the released frames");
        assert_eq!(sees.woke.load(Ordering::SeqCst), 1);
        assert_eq!(
            sees.token.load(Ordering::SeqCst),
            FREE_TOKEN,
            "the released frames were told with the run token held"
        );
    }

    /// A copy batch: promised frames keep it from retiring, a promise given back leaves it empty,
    /// one retire wins and a second loses without looking again; freed, a grow takes it
    /// (DESIGN.md 9.1.1). A promise past its frames, or of another class, is given back.
    #[test]
    fn a_copy_batch_retires_once_only_when_nothing_is_lent_in_it() {
        let m = Arc::new(Memory::new(8 * BATCH).unwrap());
        assert!(m.take_free_batch(7));
        m.publish_lent(7, LendKind::Copy, class(0));
        assert!(m.is_lend_batch(7));
        assert_eq!(m.copy_batches(class(0)).collect::<Vec<_>>(), vec![7]);
        assert!(m.promise_lent(7, class(0), 3).is_ok());
        assert!(
            m.promise_lent(7, class(1), 1).is_err(),
            "another class's promise"
        );
        assert!(
            m.promise_lent(7, class(0), GRAINS).is_err(),
            "a promise past its frames"
        );
        assert_eq!(m.lend_word(7).count(), 3);
        assert!(!m.retire(7, class(0)), "retired with frames promised");
        assert_eq!(m.unpromise_lent(7, 3).count(), 0);
        assert!(m.retire(7, class(0)));
        assert!(!m.retire(7, class(0)), "retired twice");
        assert_eq!(m.copy_batches(class(0)).count(), 0);
        assert!(
            m.promise_lent(7, class(0), 1).is_err(),
            "a promise on a retiring batch"
        );
        m.free_retired(7);
        assert!(!m.is_lend_batch(7));
        assert!(m.has_free_run(class(BATCH_CLASS)));
    }

    /// A second drain of a draining batch while the count back that left it empty has subtracted
    /// and not yet claimed the release: the drain only adds its beneficiary, and the count back
    /// releases the batch, once, to that beneficiary (DESIGN.md 9.1.2).
    #[test]
    fn a_second_drain_never_claims_the_release_a_last_count_back_owns() {
        use crate::race;
        let m = Arc::new(Memory::new(8 * BATCH).unwrap());
        assert!(m.grow(class(1)));
        let frame = m.take(class(1)).unwrap();
        let batch = frame / GRAINS;
        assert!(m.drain(class(1), batch));
        {
            let m = m.clone();
            race::at("memory.count_back", move || {
                assert!(
                    !m.drain_for(class(1), batch, class(0)),
                    "a drain under way starts none"
                );
            });
        }
        assert!(m.put(class(1), frame), "the count back released the batch");
        assert!(!race::pending("memory.count_back"));
        assert_eq!(m.held()[1], 0, "{}", m.describe());
        assert_eq!(
            m.held()[0],
            BATCH,
            "it went to the second drain's beneficiary: {}",
            m.describe()
        );
        let s = m.state(batch);
        assert!(
            s.held_by(class(0)) && !s.releasing() && !s.draining(),
            "released once, to class 0, and not released again: {s:?}"
        );
        let next = m.take(class(0)).expect("class 0 holds the batch");
        assert_eq!(next / GRAINS, batch, "{}", m.describe());
    }

    /// A drain chosen for a batch that, between the drain's read and its compare-and-swap, is
    /// drained by another, gets its last frame back and goes to another class: the stale drain
    /// changes nothing on the new owner's batch (DESIGN.md 9.1.2).
    #[test]
    fn a_drain_chosen_before_its_batch_changed_hands_changes_nothing() {
        use crate::race;
        let m = Arc::new(Memory::new(8 * BATCH).unwrap());
        assert!(m.grow(class(1)));
        let frame = m.take(class(1)).unwrap();
        let batch = frame / GRAINS;
        let after = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
        {
            let (m, after) = (m.clone(), after.clone());
            race::at("memory.drain", move || {
                assert!(m.drain_for(class(1), batch, class(0)));
                assert!(
                    m.put(class(1), frame),
                    "the last frame back releases it to class 0"
                );
                *after.lock() = Some(m.state(batch));
            });
        }
        assert!(
            !m.drain_for(class(1), batch, class(2)),
            "the stale drain started none"
        );
        assert!(!race::pending("memory.drain"));
        let after = after.lock().unwrap();
        assert!(after.held_by(class(0)) && !after.draining(), "{after:?}");
        assert_eq!(m.state(batch), after, "the new owner's word is untouched");
    }

    /// A grow whose compare-and-swap on a run it read is lost to another grow: it goes on to the
    /// next run it read and takes that one, never trying the lost one again (DESIGN.md 9.1.2).
    #[test]
    fn a_grow_lost_on_one_run_takes_the_next_it_read() {
        use crate::race;
        let m = Arc::new(Memory::new(8 * BATCH).unwrap());
        let other = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
        {
            let (m, other) = (m.clone(), other.clone());
            race::at("memory.take_run", move || {
                *other.lock() = Some(m.take_run(0).expect("a free batch"));
            });
        }
        let mine = m.take_run(0).expect("the next free batch it read");
        assert!(!race::pending("memory.take_run"));
        let other = other.lock().unwrap();
        assert_ne!(mine, other, "two grows, two batches");
    }

    /// A claim whose pass meets a put behind it and the one frame ahead taken by another claim:
    /// one pass, no frame, the promise given back (no supply event; the put signalled), and the
    /// next look takes the frame the put freed (DESIGN.md 9.1.2).
    #[test]
    fn a_claim_whose_pass_meets_a_put_behind_it_gives_its_promise_back() {
        use crate::race;
        let m = Arc::new(Memory::new(8 * BATCH).unwrap());
        assert!(m.grow(class(0)));
        let all: Vec<u32> = (0..GRAINS).map(|_| m.take(class(0)).unwrap()).collect();
        let batch = all[0] / GRAINS;
        // Frame 64, the first of the second bit word, is the one free frame.
        assert!(!m.put(class(0), all[64]));
        {
            let m = m.clone();
            let first = all[0];
            race::at("memory.claim_word", move || {
                // After the pass left word 0: a put frees frame 0 behind it, and another claim,
                // which cannot use frame 0, takes frame 64 ahead of it.
                assert!(!m.put(class(0), first));
                match m.take_where(class(0), |f| f != first) {
                    Take::Frame(f) => assert_eq!(f, first + 64),
                    _ => panic!("the other claim takes frame 64"),
                }
            });
        }
        assert!(matches!(m.take_where(class(0), |_| true), Take::None));
        assert!(!race::pending("memory.claim_word"));
        assert_eq!(
            m.state(batch).taken(),
            GRAINS - 1,
            "the promise was given back"
        );
        assert_eq!(
            m.take(class(0)),
            Some(all[0]),
            "the next look takes the frame put back"
        );
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
