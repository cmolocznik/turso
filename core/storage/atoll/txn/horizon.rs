//! The published horizon: every commit timestamped at or below it is in its slot's commit log.
//!
//! A commit under way marks its slot with the clock as it began, then draws its timestamp, which
//! is above that, then marks the slot with it, and clears the mark once its pair is published.
//! The horizon rises to the clock read before the marks, and below every mark: a commit at or
//! below that clock marked its slot before it drew, so the scan finds it drawing, drawn, or
//! published. A commit is acknowledged once the horizon covers it; one that finds it does not
//! waits for the commits below it, woken whenever one is published to raise the horizon and look
//! again.

use self::sync::{fence, AtomicU64, Ordering};
use super::super::stack::Stack;
use super::{Clock, Ts};
use crate::io::Completion;
use crossbeam_utils::CachePadded;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicU64, Ordering};
}

/// In a slot's mark: its commit's timestamp is not drawn yet, and will be above the clock the
/// mark holds.
const DRAWING: u64 = 1 << 63;

pub(super) struct Horizon {
    /// Raised by every commit and read by every snapshot, so on a line of its own.
    at: CachePadded<AtomicU64>,
    /// Per slot, its commit under way: 0 for none; the clock as it began, with [`DRAWING`], until
    /// its timestamp is drawn; then its timestamp, until its pair is published. Each written by its
    /// slot's commits alone, so on a line of its own.
    marks: Box<[CachePadded<AtomicU64>]>,
    /// Commits waiting for the horizon to reach them.
    waits: Stack<Completion>,
}

impl Horizon {
    /// A horizon for `slots` slots, at `base`, every commit at or below it published.
    pub(super) fn new(slots: usize, base: Ts) -> Self {
        Self {
            at: CachePadded::new(AtomicU64::new(base)),
            marks: (0..slots)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            waits: Stack::default(),
        }
    }

    pub(super) fn at(&self) -> Ts {
        self.at.load(Ordering::SeqCst)
    }

    /// Draws `slot`'s commit timestamp from `clock`, marking the slot first.
    pub(super) fn draw(&self, slot: u16, clock: &Clock) -> Ts {
        let mark = &self.marks[slot as usize];
        mark.store(clock.now() | DRAWING, Ordering::SeqCst);
        let ts = clock.next();
        mark.store(ts, Ordering::SeqCst);
        ts
    }

    /// `slot`'s commit is published: the horizon may rise past it, and the commits waiting may be
    /// covered.
    pub(super) fn published(&self, slot: u16, clock: &Clock) {
        self.marks[slot as usize].store(0, Ordering::SeqCst);
        self.raise(clock);
        // Pairs with the fence of a commit waiting: either this finds it, or it sees the mark gone.
        fence(Ordering::SeqCst);
        for c in self.waits.take_all() {
            c.complete(0);
        }
    }

    /// Raises the horizon as far as every commit at or below it is published; returns it.
    fn raise(&self, clock: &Clock) -> Ts {
        let mut to = clock.now();
        for mark in self.marks.iter() {
            let m = mark.load(Ordering::SeqCst);
            if m & DRAWING != 0 {
                to = to.min(m & !DRAWING);
            } else if m != 0 {
                to = to.min(m - 1);
            }
        }
        self.at.fetch_max(to, Ordering::SeqCst).max(to)
    }

    /// `None` once the horizon covers `ts`; until then, the completion to wait on, to look again
    /// once it is done. A completion left behind by one that saw the horizon is done later, for
    /// no one.
    pub(super) fn wait(&self, ts: Ts, clock: &Clock) -> Option<Completion> {
        if self.raise(clock) >= ts {
            return None;
        }
        let c = crate::storage::atoll::notify::wait();
        self.waits.push(c.clone());
        fence(Ordering::SeqCst);
        (self.raise(clock) < ts).then_some(c)
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Two slots commit at once while a reader takes the horizon: every commit at or below it was
    /// published before, and each commit is covered once both are done, or its wait is.
    #[test]
    fn the_horizon_covers_only_commits_published() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let clock = Arc::new(Clock::after(0));
            let horizon = Arc::new(Horizon::new(2, 0));
            let published: Arc<[AtomicU64; 2]> = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
            let committers: Vec<_> = (0..2u16)
                .map(|slot| {
                    let (clock, horizon, published) =
                        (clock.clone(), horizon.clone(), published.clone());
                    thread::spawn(move || {
                        let ts = horizon.draw(slot, &clock);
                        published[slot as usize].store(ts, Ordering::Relaxed);
                        horizon.published(slot, &clock);
                        (ts, horizon.wait(ts, &clock))
                    })
                })
                .collect();
            let at = horizon.at();
            let seen = [
                published[0].load(Ordering::Relaxed),
                published[1].load(Ordering::Relaxed),
            ];
            let done: Vec<_> = committers.into_iter().map(|c| c.join().unwrap()).collect();
            for (slot, (ts, wait)) in done.into_iter().enumerate() {
                if ts <= at {
                    assert_eq!(seen[slot], ts, "the horizon passed a commit not published");
                }
                if let Some(c) = wait {
                    assert!(
                        c.finished(),
                        "a commit's wait for the horizon was never done"
                    );
                }
                assert!(horizon.raise(&clock) >= ts);
            }
        });
    }
}
