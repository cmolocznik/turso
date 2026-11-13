//! The log's writes in flight, each in a record its parts are written from one at a time (ASYNC1
//! D2d v1.4): a write its backend writes short is continued from where it stopped, its place among
//! the writes in flight held from its first part to its last part's retirement.
//!
//! A record's state is two bitmaps, a bit each: `free`, set while it is at rest, and `owed`, set
//! while a part retired short and waits for the IO thread's unit. With neither set, the record has
//! one owner, and only that owner touches its fields: the planner that took it, under the flush's
//! turn; a part in flight, its callback and then its retirement observer, ordered by the request;
//! the unit that claimed it; or a submitter whose submission was refused. Only a turn's holder
//! clears a `free` bit, and the turns are one at a time, so a take needs no retry. A rest publishes
//! its `free` bit with `Release`, which the next take acquires; an owed record's fields are
//! published by its `owed` bit, `Release`, which the unit's swap of the word acquires, the swap
//! being its claim.

use self::sync::{AtomicU64, Ordering, Plain};

/// The engine's primitives, or loom's.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU64, Ordering};

    /// A record's fields, which its owner alone reads and writes; under loom, loom's checked cell.
    pub(super) struct Plain<T>(std::cell::UnsafeCell<T>);

    impl<T> Plain<T> {
        pub(super) fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
        }

        pub(super) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::cell::UnsafeCell as Plain;
    pub(super) use loom::sync::atomic::{AtomicU64, Ordering};
}

/// A write's range in the log, where its bytes are, and how many of them are written.
pub(super) struct Fields<S> {
    pub(super) start: u64,
    pub(super) end: u64,
    /// The block it is written from, and where in it `start` lies; `None` for the ring's.
    pub(super) source: Option<S>,
    /// The bytes its parts have written so far: at most `end - start`, which the take bounds by
    /// `u32::MAX`.
    pub(super) done: u32,
    /// Whether the part in flight was told its outcome: set by its callback, read by its observer.
    pub(super) told: bool,
}

/// The records and their two bitmaps.
pub(super) struct Writes<S> {
    records: Box<[Plain<Fields<S>>]>,
    free: Box<[AtomicU64]>,
    owed: Box<[AtomicU64]>,
    /// The times each record rested.
    #[cfg(test)]
    rests: Box<[crate::sync::atomic::AtomicUsize]>,
}

// SAFETY: a record's fields are reached only by its one owner, handed between threads by the
// orders above.
unsafe impl<S: Send> Sync for Writes<S> {}
unsafe impl<S: Send> Send for Writes<S> {}

impl<S> Writes<S> {
    /// `n` records, all at rest: a bit each in words of 64, the last word's bits past `n` never
    /// set.
    pub(super) fn new(n: usize) -> Self {
        assert!(n > 0, "a log with no writes in flight");
        let words = n.div_ceil(64);
        let free = (0..words)
            .map(|w| {
                let bits = (n - w * 64).min(64);
                AtomicU64::new(if bits == 64 {
                    u64::MAX
                } else {
                    (1 << bits) - 1
                })
            })
            .collect();
        Self {
            records: (0..n)
                .map(|_| {
                    Plain::new(Fields {
                        start: 0,
                        end: 0,
                        source: None,
                        done: 0,
                        told: false,
                    })
                })
                .collect(),
            free,
            owed: (0..words).map(|_| AtomicU64::new(0)).collect(),
            #[cfg(test)]
            rests: (0..n)
                .map(|_| crate::sync::atomic::AtomicUsize::new(0))
                .collect(),
        }
    }

    /// The lowest record at rest, taken: `None` if none is. Only a holder of the flush's turn
    /// takes, so the bit it finds is still set when it clears it.
    pub(super) fn take(&self) -> Option<usize> {
        for (w, word) in self.free.iter().enumerate() {
            let bits = word.load(Ordering::Relaxed);
            if bits == 0 {
                continue;
            }
            let bit = bits.trailing_zeros() as usize;
            let had = word.fetch_and(!(1 << bit), Ordering::Acquire);
            return (had & (1 << bit) != 0).then_some(w * 64 + bit);
        }
        None
    }

    /// Runs `f` on record `j`'s fields.
    ///
    /// # Safety
    ///
    /// The caller is record `j`'s owner: it took it, claimed it, or is the request it was handed
    /// to, and no other thread touches it until the caller hands it on.
    pub(super) unsafe fn with<R>(&self, j: usize, f: impl FnOnce(&mut Fields<S>) -> R) -> R {
        // SAFETY: the caller owns the record, so this is the only reference to its fields.
        self.records[j].with_mut(|p| f(unsafe { &mut *p }))
    }

    /// Hands record `j`, its fields written, to the unit: its `owed` bit set, `Release`.
    pub(super) fn owe(&self, j: usize) {
        self.owed[j / 64].fetch_or(1 << (j % 64), Ordering::Release);
    }

    /// Claims every record owed now, each word swapped once, `Acquire`, and runs `f` on each:
    /// a record owed again while this runs is the next claim's.
    pub(super) fn claim_owed(&self, mut f: impl FnMut(usize)) {
        for (w, word) in self.owed.iter().enumerate() {
            let mut bits = word.swap(0, Ordering::Acquire);
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                f(w * 64 + bit);
            }
        }
    }

    /// Rests record `j`: its source dropped, then its `free` bit set, `Release`. The caller owned
    /// it, and touches it no more.
    pub(super) fn rest(&self, j: usize) {
        // SAFETY: the caller owns the record until the bit below is set.
        drop(unsafe { self.with(j, |f| f.source.take()) });
        #[cfg(test)]
        self.rests[j].fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
        self.free[j / 64].fetch_or(1 << (j % 64), Ordering::Release);
    }

    /// The records at rest and the records owed, for a test reading them once nothing is in
    /// flight.
    #[cfg(test)]
    pub(super) fn counts(&self) -> (u32, u32) {
        let ones = |words: &[AtomicU64]| {
            words
                .iter()
                .map(|w| w.load(Ordering::SeqCst).count_ones())
                .sum()
        };
        (ones(&self.free), ones(&self.owed))
    }

    /// The times each record rested, for a test.
    #[cfg(test)]
    pub(super) fn rests(&self) -> Vec<usize> {
        self.rests
            .iter()
            .map(|r| r.load(crate::sync::atomic::Ordering::SeqCst))
            .collect()
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;

    /// Records past one word: each taken once, lowest first, none past the last; the last word's
    /// bits past the records never set; rested and owed across the word's edge, each claimed once
    /// by one pass and a record owed during the pass left for the next (mutant: every bit of the
    /// last word set, taking a record that does not exist).
    #[test]
    fn seventy_records_are_taken_owed_and_claimed_across_their_words() {
        let writes = Writes::<()>::new(70);
        let taken: Vec<usize> = std::iter::from_fn(|| writes.take()).collect();
        assert_eq!(taken, (0..70).collect::<Vec<_>>(), "the records taken");
        assert_eq!(
            writes.counts(),
            (0, 0),
            "(at rest, owed) with every record taken"
        );
        for j in [3, 63, 64, 69] {
            writes.owe(j);
        }
        let mut claimed = Vec::new();
        writes.claim_owed(|j| {
            claimed.push(j);
            // Owed again during the pass: the next pass's, not this one's.
            if j == 63 {
                writes.owe(63);
            }
        });
        let mut again = Vec::new();
        writes.claim_owed(|j| again.push(j));
        for j in 0..70 {
            writes.rest(j);
        }
        assert_eq!(claimed, vec![3, 63, 64, 69], "the first pass's claims");
        assert_eq!(again, vec![63], "the second pass's claims");
        assert_eq!(
            writes.counts(),
            (70, 0),
            "(at rest, owed) once every record rested"
        );
        assert_eq!(writes.rests(), vec![1; 70], "the rests of each record");
        assert_eq!(writes.take(), Some(0), "the first record taken again");
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::atomic::AtomicBool;
    use loom::sync::Arc;

    /// D2d v1.4 T6, over these records and bitmaps: a part's observer writes its progress and owes
    /// the record while another thread fails the log, and the unit, already under way, claims what
    /// is owed with one pass, with no other order to the observer than the bit's; after both
    /// threads end, the next unit's pass claims what is left. The progress the unit reads is the
    /// one written, each record is claimed once, and every record ends at rest, nothing owed
    /// (mutant `log-bit-before-progress`: the bit set before the progress is written, which loom
    /// reports as two accesses of the record's cell unordered).
    #[test]
    fn an_owed_records_progress_reaches_the_unit_that_claims_it() {
        loom::model(|| {
            let writes = Arc::new(Writes::<u8>::new(2));
            let failed = Arc::new(AtomicBool::new(false));
            let (a, b) = (writes.take().unwrap(), writes.take().unwrap());
            // SAFETY: taken by this thread, the planner, and handed on by the spawn below.
            unsafe {
                writes.with(a, |f| {
                    f.end = 8;
                    f.source = Some(1);
                });
            }
            let observer = {
                let writes = writes.clone();
                loom::thread::spawn(move || {
                    // SAFETY: the part's request owns the record until it owes it.
                    unsafe { writes.with(a, |f| f.done = 4) };
                    writes.owe(a);
                })
            };
            let failer = {
                let failed = failed.clone();
                loom::thread::spawn(move || failed.store(true, Ordering::SeqCst))
            };
            let mut seen = Vec::new();
            let pass = |seen: &mut Vec<u32>| {
                writes.claim_owed(|j| {
                    // SAFETY: claimed by the swap.
                    seen.push(unsafe { writes.with(j, |f| f.done) });
                    // Served or, the log failed, let go: either way it rests here in the model.
                    let _ = failed.load(Ordering::SeqCst);
                    writes.rest(j);
                });
            };
            pass(&mut seen);
            observer.join().unwrap();
            failer.join().unwrap();
            pass(&mut seen);
            writes.rest(b);
            assert_eq!(seen, vec![4], "the progress the unit read, once");
            assert_eq!(writes.counts(), (2, 0), "(at rest, owed) at the end");
            assert_eq!(writes.rests(), vec![1, 1], "the rests of each record");
        });
    }
}
