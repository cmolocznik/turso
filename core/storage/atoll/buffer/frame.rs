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
/// A LENT frame lent for as long as its holder lives, as the log's ring is: drained for another
/// class, its batch would wait for it for good.
pub const FOR_GOOD: u8 = 8;

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
}

// SAFETY: every field is atomics, valid as zeros: an unlocked latch at version 0, a FREE frame.
unsafe impl Zeroable for FrameHeader {}

impl FrameHeader {
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

// Shuttle's and loom's atomics are larger than the hardware's.
#[cfg(not(any(shuttle, atoll_loom)))]
const _: () = assert!(std::mem::size_of::<FrameHeader>() == 120);
