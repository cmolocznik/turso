//! Frame headers: one per 4 KiB of the pool, in an array that is never unmapped, so a frame
//! hint, however stale, always reads a header.

use super::super::latch::VersionedLatch;
use super::memory::Zeroable;
use super::watt::Tracker;
use crate::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

/// The frame holds no page.
pub const FREE: u8 = 0;
/// A page is being read into the frame, which the load holds exclusively.
pub const LOADING: u8 = 1;
/// The frame holds a page.
pub const RESIDENT: u8 = 2;

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
    /// The page held; meaningful only with `status` RESIDENT or LOADING.
    pub pid: AtomicU64,
    /// The LSN of the page's last change, mirrored from its header.
    pub page_lsn: AtomicU64,
    /// The LSN of the newest copy written back, or loaded: the page is dirty while `page_lsn` is
    /// past it.
    pub written_lsn: AtomicU64,
    /// The next frame, plus one, in this frame's hash chain.
    pub next: AtomicU32,
    pub status: AtomicU8,
    pub flags: AtomicU8,
    pub tracker: Tracker,
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
        self.page_lsn.load(Ordering::Acquire) > self.written_lsn.load(Ordering::Acquire)
    }

    pub fn has(&self, flag: u8) -> bool {
        self.flags.load(Ordering::Acquire) & flag != 0
    }
}

// Shuttle's atomics are larger than the hardware's.
#[cfg(not(shuttle))]
const _: () = assert!(std::mem::size_of::<FrameHeader>() == 96);
