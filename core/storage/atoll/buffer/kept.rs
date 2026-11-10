//! The room for the copies a checkpoint's cut keeps (DESIGN.md §9.6; Clay's ruling of
//! 2026-11-05): a page owed an image is copied as it stood at the cut before its first change
//! since, and the copies are held until the checkpoint images them.
//!
//! The copies are charged when made and released only when their bytes are dropped, wherever
//! they then are ([`KeptCopy`]). A change asks for room for every page it will change before
//! its first edit ([`BufferPool::keep`](super::BufferPool::keep)); refused, it lets go of what
//! it holds and waits, driving the checkpoint itself. Only node pages are kept, at most 64 KiB
//! each ([`MAX_CLASS`](super::super::btree::node::MAX_CLASS)), and a change asks for three at
//! most, so every request fits the smallest limit, one page of the largest class.

use super::super::aligned::AlignedBlock;
use super::super::epoch::Epochs;
use super::cell::{CompletionCell, Retiring};
use crate::io::{Completion, IO};
use crate::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::sync::Arc;
use crate::MonotonicInstant;
use std::ops::Deref;

/// The upper bound of the default limit: past a 2 GiB frame budget, the limit stays here (the
/// ruling's initial choice, not a measured optimum).
pub const KEPT_CAP: usize = 256 << 20;

/// The most one change asks for: only node pages are kept, at most
/// [`MAX_CLASS`](super::super::btree::node::MAX_CLASS), and a merge asks for three.
pub const LARGEST_REQUEST: usize =
    3 * (super::super::page::MIN_PAGE_SIZE << super::super::btree::node::MAX_CLASS);

/// The default limit for a budget of `budget` bytes: an eighth of it, at most [`KEPT_CAP`], and
/// never under [`LARGEST_REQUEST`], so every change's request fits. The copies take frames of the
/// budget, so the floor is no larger than a request (the ruling's floor of one page of the
/// largest class predates the copies living in the frames, and was most of a small pool).
pub fn default_limit(budget: usize) -> usize {
    (budget / 8).min(KEPT_CAP).max(LARGEST_REQUEST)
}

/// What the room has held and how often it refused, since the pool opened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KeptStats {
    pub limit: usize,
    pub used: usize,
    pub peak: usize,
    pub refusals: u64,
    /// Nanoseconds from each refusal to the refused thread's next admitted request.
    pub waited_ns: u64,
    /// The longest of those waits since the peaks were last reset.
    pub longest_wait_ns: u64,
}

pub struct KeptRoom {
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    refusals: AtomicU64,
    waited_ns: AtomicU64,
    longest_wait_ns: AtomicU64,
    /// Bytes of a request refused since one was last admitted: the checkpoint images kept
    /// copies first while it is not 0.
    pressing: AtomicUsize,
    /// Finished whenever room is given back, if anyone waits for it.
    freed: CompletionCell,
    /// Whether another thread gives it back ([`freed_elsewhere`](Self::freed_elsewhere)).
    freed_elsewhere: crate::sync::atomic::AtomicBool,
    /// Waits handed out that block until another thread gives room back, for tests.
    #[cfg(test)]
    blocking_waits: AtomicU64,
    retiring: Retiring,
    epochs: Arc<Epochs>,
    io: Arc<dyn IO>,
}

crate::thread::thread_local! {
    /// When this thread's last request was refused, if none has been admitted since.
    static REFUSED_AT: std::cell::Cell<Option<MonotonicInstant>> = const { std::cell::Cell::new(None) };
}

impl KeptRoom {
    pub fn new(limit: usize, epochs: Arc<Epochs>, io: Arc<dyn IO>) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            refusals: AtomicU64::new(0),
            waited_ns: AtomicU64::new(0),
            longest_wait_ns: AtomicU64::new(0),
            pressing: AtomicUsize::new(0),
            freed: CompletionCell::default(),
            freed_elsewhere: crate::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            blocking_waits: AtomicU64::new(0),
            retiring: Retiring::new(epochs.clone()),
            epochs,
            io,
        })
    }

    /// Charges `bytes`, if they fit: whether they did.
    pub fn charge(&self, bytes: usize) -> bool {
        debug_assert!(
            bytes <= self.limit,
            "a request of {bytes} bytes no room holds"
        );
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            if used + bytes > self.limit {
                return false;
            }
            match self.used.compare_exchange_weak(
                used,
                used + bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.peak.fetch_max(used + bytes, Ordering::AcqRel);
                    return true;
                }
                Err(now) => used = now,
            }
        }
    }

    /// A request of `bytes` refused: the thread waits from now.
    pub fn refused(&self, bytes: usize) {
        self.refusals.fetch_add(1, Ordering::Relaxed);
        self.pressing.fetch_max(bytes, Ordering::AcqRel);
        let now = self.io.current_time_monotonic();
        REFUSED_AT.with(|at| {
            if at.get().is_none() {
                at.set(Some(now));
            }
        });
    }

    /// A request of this thread's admitted: its wait, if it waited, ends.
    pub fn admitted(&self) {
        let Some(at) = REFUSED_AT.with(|at| at.take()) else {
            return;
        };
        self.pressing.store(0, Ordering::Release);
        let waited = self.io.current_time_monotonic().duration_since(at);
        let waited = u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX);
        self.waited_ns.fetch_add(waited, Ordering::Relaxed);
        self.longest_wait_ns.fetch_max(waited, Ordering::Relaxed);
    }

    /// Whether the largest request refused since one was last admitted fits now.
    pub fn takes_refused(&self) -> bool {
        self.used.load(Ordering::Acquire) + self.pressing.load(Ordering::Acquire) <= self.limit
    }

    /// Whether a request was refused since one was last admitted.
    pub fn pressed(&self) -> bool {
        self.pressing.load(Ordering::Acquire) != 0
    }

    /// The cut ended: no copy is kept, and none is wanted.
    pub fn cut_ended(&self) {
        self.pressing.store(0, Ordering::Release);
        self.wake();
    }

    /// Gives back `bytes` charged for copies that were not made after all.
    pub fn uncharge(&self, bytes: usize) {
        self.release(bytes);
    }

    fn release(&self, bytes: usize) {
        let before = self.used.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(before >= bytes, "a kept copy released twice");
        self.wake();
    }

    /// Done when room is next given back or, while writers step the checkpoint, its stepper
    /// leaves: what a refused writer that could not step the checkpoint waits on.
    pub fn wait(&self) -> Completion {
        let fresh = if self.freed_elsewhere.load(Ordering::Acquire) {
            #[cfg(test)]
            self.blocking_waits.fetch_add(1, Ordering::Relaxed);
            Completion::new_remote_wait
        } else {
            Completion::new_wait
        };
        self.freed
            .current_with(&self.epochs.enter(), &self.retiring, fresh)
    }

    /// Whether room is given back by another thread, the checkpointing thread's images: a writer
    /// waiting for it then blocks rather than stepping its IO.
    /// Waits handed out so far that block until another thread gives room back.
    #[cfg(test)]
    pub fn blocking_waits(&self) -> u64 {
        self.blocking_waits.load(Ordering::Relaxed)
    }

    pub fn freed_elsewhere(&self, elsewhere: bool) {
        self.freed_elsewhere.store(elsewhere, Ordering::Release);
    }

    /// Wakes whoever waits for room.
    pub fn wake(&self) {
        if let Some(c) = self.freed.take(&self.retiring) {
            c.complete(0);
        }
    }

    #[cfg(test)]
    pub fn stats(&self) -> KeptStats {
        KeptStats {
            limit: self.limit,
            used: self.used.load(Ordering::Acquire),
            peak: self.peak.load(Ordering::Acquire),
            refusals: self.refusals.load(Ordering::Relaxed),
            waited_ns: self.waited_ns.load(Ordering::Relaxed),
            longest_wait_ns: self.longest_wait_ns.load(Ordering::Relaxed),
        }
    }

    /// Starts the peak again from what is held now, and the longest wait from none, for a
    /// measurement.
    #[cfg(test)]
    pub fn reset_peaks(&self) {
        self.peak
            .store(self.used.load(Ordering::Acquire), Ordering::Release);
        self.longest_wait_ns.store(0, Ordering::Relaxed);
    }
}

/// A page as a cut found it, in a frame lent for it, charged to the room until the copy drops
/// and gives the frame back.
pub struct KeptCopy {
    block: AlignedBlock,
    room: Arc<KeptRoom>,
}

impl KeptCopy {
    /// `block`'s copy, already charged to `room`.
    pub fn charged(block: AlignedBlock, room: &Arc<KeptRoom>) -> Self {
        Self {
            block,
            room: room.clone(),
        }
    }
}

impl Deref for KeptCopy {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.block.bytes()
    }
}

impl Drop for KeptCopy {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.room.release(self.block.len());
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
