//! Copies of pages written back while they stay in their frames, which only a sweep makes (a
//! test's): eviction writes a dirty page out of its frame, so the engine makes none (one memory,
//! Clay's ruling of 2026-11-05). A page written out of its frame needs none: its write-out holds
//! the frame, and it is written from there.
//!
//! Each copy is a block of its own, dropped once its write lands, and the copies in flight stay
//! within a number of bytes: a copy that does not fit waits for one to land.

use super::super::aligned::AlignedBlock;
use super::super::page::SizeClass;
use crate::io::Buffer;
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::Arc;

pub struct Staging {
    /// Bytes the copies in flight may take.
    cap: usize,
    /// Bytes they take now.
    in_flight: AtomicUsize,
    /// Copies made, for tests to tell a write made from one.
    #[cfg(test)]
    made: crate::sync::atomic::AtomicU64,
}

/// A copy and its share of the bytes in flight, given back when dropped: a write that never
/// starts, or a writeback dropped before it ends, keeps none.
pub struct Leased {
    staging: Arc<Staging>,
    block: Arc<AlignedBlock>,
}

impl Leased {
    /// The copy's bytes, to copy a page into before it is written.
    pub fn bytes(&mut self) -> &mut [u8] {
        Arc::get_mut(&mut self.block)
            .expect("copied into before it is written")
            .bytes_mut()
    }

    /// The copy as an IO buffer, to write from.
    pub fn buffer(&self) -> Arc<Buffer> {
        self.block.region(0, self.block.len())
    }
}

impl Drop for Leased {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.staging
            .in_flight
            .fetch_sub(self.block.len(), Ordering::AcqRel);
    }
}

impl Staging {
    /// Copies in flight within `cap` bytes.
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            in_flight: AtomicUsize::new(0),
            #[cfg(test)]
            made: crate::sync::atomic::AtomicU64::new(0),
        }
    }

    /// A block for a copy of a page of `class`, if the copies in flight leave room for it.
    pub fn lease(self: &Arc<Self>, class: SizeClass) -> Option<Leased> {
        let len = class.size();
        let mut now = self.in_flight.load(Ordering::Acquire);
        loop {
            if now + len > self.cap {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                now,
                now + len,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(seen) => now = seen,
            }
        }
        #[cfg(test)]
        self.made.fetch_add(1, Ordering::Relaxed);
        Some(Leased {
            staging: self.clone(),
            block: Arc::new(AlignedBlock::zeroed(len)),
        })
    }

    /// Copies made so far.
    #[cfg(test)]
    pub fn copies(&self) -> u64 {
        self.made.load(Ordering::Relaxed)
    }
}
