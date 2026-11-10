//! Frame headers: one per 4 KiB of the pool, in an array that is never unmapped, so a frame
//! hint, however stale, always reads a header.

use super::super::latch::VersionedLatch;
use super::memory::Zeroable;
use super::watt::Tracker;
use crate::sync::atomic::{AtomicU64, AtomicU8, Ordering};

/// The frame holds no page.
pub const FREE: u8 = 0;
/// A page is being read into the frame, which the load holds exclusively.
pub const LOADING: u8 = 1;
/// The frame holds a page.
pub const RESIDENT: u8 = 2;
/// The page is being written out of the frame, which the write-out holds exclusively until the
/// write ends.
pub const OUTGOING: u8 = 3;
/// The frame is lent beside the page table ([`super::lent`]): it holds no page, and its holder
/// gives it back.
pub const LENT: u8 = 4;

/// A copy of the page is being written back.
pub const WRITING: u8 = 1;
/// Evict the page once its writeback lands, unless it changed meanwhile.
pub const EVICT_AFTER: u8 = 2;
/// The page was unlinked from what referred to it and is about to be freed: hints to it are not
/// followed.
pub const ABANDONED: u8 = 4;

/// `pid` of a frame holding no page; no page id has every bit set.
pub const NO_PAGE: u64 = u64::MAX;

#[derive(Debug, Default)]
pub struct FrameHeader {
    /// The page's latch. Its version also rises on eviction, so a reader holding a hint learns
    /// that the frame changed hands.
    pub latch: VersionedLatch,
    /// The page held; meaningful only with `status` RESIDENT, LOADING or OUTGOING.
    pub pid: AtomicU64,
    /// The page's change count, mirrored in its header: one more at each change.
    pub changes: AtomicU64,
    /// The count of the newest copy written, or loaded: the page is dirty while `changes` is past
    /// it, and may be dropped once it is not.
    pub written: AtomicU64,
    /// The count its checkpoint copy holds: the page is owed an image while `changes` differs
    /// from it (audit 18, review 1).
    pub imaged: AtomicU64,
    /// The checkpoint whose cut found the page owed and has not imaged it yet: its first change
    /// meanwhile keeps its bytes for that checkpoint.
    pub pending: AtomicU64,
    /// This frame's link in its hash chain: the next frame, a mark and a tag ([`super::table`]).
    pub next: super::table::Link,
    pub status: AtomicU8,
    pub flags: AtomicU8,
    pub tracker: Tracker,
    /// The global epoch when a step last fixed the page, the highest any did: a page marked at
    /// or above the epochs' horizon may be held by a step under way. Relaxed: it only prices a
    /// drain, and memory is read into again only once the epochs pass `given_up`'s stamps.
    pub held: AtomicU64,
    /// Where the IO of the hold that reads the frame in or writes it out stands at the backend:
    /// one past the hold's version, shifted, once its request is handed over, and that with its
    /// low bit once it landed. Only rises, as `fetch_max`: a hold's states rise, and every later
    /// hold's are above them, so a mark written late, after the frame changed hands, is lost. A
    /// fix that finds the frame loading or outgoing waits on that IO, stepping it, only while it
    /// is at the backend, and otherwise on the pool's supply, which the submission's and the
    /// landing's events finish (DESIGN.md 9.1.3, wait kinds).
    pub io: AtomicU64,
}

// SAFETY: every field is atomics, valid as zeros: an unlocked latch at version 0, a FREE frame.
unsafe impl Zeroable for FrameHeader {}

impl FrameHeader {
    /// The IO of the hold taken at `version` was handed to the backend.
    pub fn submitted_at(&self, version: u64) {
        self.io.fetch_max(io_mark(version), Ordering::AcqRel);
    }

    /// The IO of the hold taken at `version` landed: none of it is at the backend any more.
    pub fn landed_at(&self, version: u64) {
        self.io.fetch_max(io_mark(version) | 1, Ordering::AcqRel);
    }

    /// Whether the IO of the hold taken at `version` was handed to the backend, landed or not.
    pub fn handed_over(&self, version: u64) -> bool {
        self.io.load(Ordering::Acquire) >= io_mark(version)
    }

    /// Whether the IO of the hold taken at `version` is at the backend: handed over and not landed.
    pub fn at_backend(&self, version: u64) -> bool {
        self.io.load(Ordering::Acquire) == io_mark(version)
    }

    pub fn pid(&self) -> u64 {
        self.pid.load(Ordering::Acquire)
    }

    pub fn status(&self) -> u8 {
        self.status.load(Ordering::Acquire)
    }

    pub fn dirty(&self) -> bool {
        self.changes.load(Ordering::Acquire) > self.written.load(Ordering::Acquire)
    }

    /// Whether the page differs from its checkpoint copy.
    pub fn owed(&self) -> bool {
        self.changes.load(Ordering::Acquire) != self.imaged.load(Ordering::Acquire)
    }

    pub fn has(&self, flag: u8) -> bool {
        self.flags.load(Ordering::Acquire) & flag != 0
    }
}

/// The IO state of the hold taken at `version`, handed over: a latch's version has 62 bits.
fn io_mark(version: u64) -> u64 {
    (version + 1) << 1
}

// Shuttle's and loom's atomics are larger than the hardware's.
#[cfg(not(any(shuttle, atoll_loom)))]
const _: () = assert!(std::mem::size_of::<FrameHeader>() == 128);
