//! A position that rises past runs completed in any order, without a lock: the tail of the
//! writeback ring, past the leases given back, and the log's durable position, past the writes
//! landed.
//!
//! Runs start at whole units, and lie within one span of the position until it passes them. A run
//! completed records its end at the unit it starts in, and whoever completes one then moves the
//! position past every run recorded at it. The record and the position are each written and then
//! the other read across a fence, so either the completer sees the position reach its run, or
//! the thread that moved the position there sees the run: no run is left behind the position.

use self::sync::{fence, AtomicU64, Ordering};

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicU64, Ordering};
}

pub struct Prefix {
    unit: u64,
    span: u64,
    at: AtomicU64,
    /// Per unit of the span: the end of the run completed that starts there, or an older one's.
    ends: Box<[AtomicU64]>,
}

impl Prefix {
    /// A position at `start`, past runs of whole `unit`s that lie within `span` of it, a multiple
    /// of `unit`.
    pub fn new(start: u64, span: u64, unit: u64) -> Self {
        assert!(
            unit > 0 && span > 0 && span.is_multiple_of(unit),
            "a span of {span} in units of {unit}"
        );
        Self {
            unit,
            span,
            at: AtomicU64::new(start),
            ends: (0..span / unit).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Everything before it is completed.
    pub fn at(&self) -> u64 {
        self.at.load(Ordering::Acquire)
    }

    fn end_at(&self, at: u64) -> &AtomicU64 {
        &self.ends[((at % self.span) / self.unit) as usize]
    }

    /// Completes the run from `start` to `end`, and moves the position past every run completed
    /// at it.
    pub fn complete(&self, start: u64, end: u64) {
        debug_assert!(start.is_multiple_of(self.unit) && end > start);
        // Release: whoever moves the position past the run sees what was done before it.
        self.end_at(start).store(end, Ordering::Release);
        loop {
            // Pairs with the fence of whoever moves the position: one sees the other.
            fence(Ordering::SeqCst);
            let at = self.at.load(Ordering::Acquire);
            // An end recorded a span before is at or behind the position.
            let end = self.end_at(at).load(Ordering::Acquire);
            if end <= at {
                return;
            }
            let _ = self
                .at
                .compare_exchange(at, end, Ordering::AcqRel, Ordering::Acquire);
        }
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;
    use crate::sync::Arc;

    /// Threads complete runs of one to three units in an order of their own, each a run handed
    /// out in turn: the position passes each run only once it is complete, and ends past all.
    fn the_position_passes_runs_only_once_complete(threads: u64, runs: u64) {
        let prefix = Arc::new(Prefix::new(0, 64, 1));
        let next = Arc::new(AtomicU64::new(0));
        let done = Arc::new(
            (0..threads * runs * 3)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>(),
        );
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let (prefix, next, done) = (prefix.clone(), next.clone(), done.clone());
                crate::thread::spawn(move || {
                    for r in 0..runs {
                        let len = 1 + (t + r) % 3;
                        // Within a span of the position: wait for it to come close enough.
                        let start = next.fetch_add(len, Ordering::AcqRel);
                        while start + len > prefix.at() + 64 {
                            crate::thread::yield_now();
                        }
                        for u in start..start + len {
                            done[u as usize].store(1, Ordering::Release);
                        }
                        prefix.complete(start, start + len);
                        let at = prefix.at();
                        for u in at.saturating_sub(64)..at {
                            assert_eq!(
                                done[u as usize].load(Ordering::Acquire),
                                1,
                                "the position passed unit {u}, not complete"
                            );
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            prefix.at(),
            next.load(Ordering::Acquire),
            "a run was left behind"
        );
    }

    #[cfg(not(shuttle))]
    #[test]
    fn the_position_passes_runs_only_once_complete_on_threads() {
        the_position_passes_runs_only_once_complete(3, if cfg!(miri) { 200 } else { 20_000 });
    }

    #[cfg(shuttle)]
    #[test]
    fn the_position_passes_runs_only_once_complete_in_shuttle_schedules() {
        crate::storage::atoll::testing::schedules(
            || the_position_passes_runs_only_once_complete(2, 4),
            2_000,
        );
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Two runs completed at once, the later first: the position passes both.
    #[test]
    fn runs_completed_in_any_order_all_are_passed() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let prefix = Arc::new(Prefix::new(0, 4, 1));
            let other = {
                let prefix = prefix.clone();
                thread::spawn(move || prefix.complete(2, 4))
            };
            prefix.complete(0, 2);
            other.join().unwrap();
            assert_eq!(prefix.at(), 4, "a run completed was left behind");
        });
    }
}
