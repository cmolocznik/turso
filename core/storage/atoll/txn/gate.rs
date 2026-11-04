//! The write gate (DESIGN.md §17, R5, R12, R15): shared for a transaction that writes, exclusive
//! for one that writes the schema or began IMMEDIATE, with SQLite's PENDING, so an exclusive taker
//! blocks new shared takers while it waits for the holders to leave.
//!
//! - **The word** is 0, or an exclusive take: its owner's slot plus one in the low 17 bits, then
//!   whether it is held or still pending, then the take's number, drawn from a counter, so a wait
//!   names one take and a give-up clears only its own.
//! - **Shared holders are named per slot**, by the start of the transaction holding shared there,
//!   which that transaction alone writes, so a drain waits on each by name. A shared taker stores
//!   its mark first and then reads the word; a taker that reads a take clears its mark, wakes the
//!   drain, and waits on the take. An exclusive taker puts its take in the word first and then
//!   reads the marks. Each passes a SeqCst fence between its store and its load (CONCURRENCY.md
//!   §3), so of a taker and a writer meeting, at least one sees the other.
//! - **Wakes.** Every release of shared wakes the drain, so an upgrader, whose own mark never
//!   leaves, is woken by the last other holder. The release of an exclusive take and every give-up
//!   wake the shared takers and later exclusive takers waiting on it.
//! - **The schema timestamp** is the commit timestamp of the last exclusive holder that wrote the
//!   schema, stored before its take leaves the word and read after a read of the word that saw it
//!   gone.

use self::sync::{fence, AtomicU64, Ordering};
use super::Ts;
use crate::io::Completion;
use crate::storage::atoll::stack::Stack;
use crossbeam_utils::CachePadded;

/// The engine's atomics, or loom's.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicU64, Ordering};
}

/// Bits of the word naming the owner's slot plus one: 65 536 slots need 17.
const OWNER_BITS: u32 = 17;
const OWNER: u64 = (1 << OWNER_BITS) - 1;
const HELD: u64 = 1 << OWNER_BITS;
const TAKE_SHIFT: u32 = OWNER_BITS + 1;

fn owner_of(word: u64) -> Option<u16> {
    match word & OWNER {
        0 => None,
        o => Some((o - 1) as u16),
    }
}

/// What a take of the gate came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Taken {
    /// Taken.
    Done,
    /// Another transaction's exclusive take, `take`, owned by slot `owner`, stands in the way: the
    /// completion registered is completed once it leaves the word.
    Behind { take: u64, owner: u16 },
    /// These slots' transactions hold shared, by their starts: the completion registered is
    /// completed at each release.
    Holders(Vec<(u16, Ts)>),
    /// The take was cleared from the word by another, for a victim: it is no longer pending.
    Lost,
}

pub(super) struct Gate {
    word: CachePadded<AtomicU64>,
    shared: Box<[CachePadded<AtomicU64>]>,
    takes: AtomicU64,
    schema: CachePadded<AtomicU64>,
    /// Waiting on a take: shared takers and later exclusive takers.
    turns: Stack<Completion>,
    /// The exclusive taker waiting for the holders to leave.
    drain: Stack<Completion>,
}

impl Gate {
    pub(super) fn new(slots: usize) -> Self {
        Self {
            word: CachePadded::new(AtomicU64::new(0)),
            shared: (0..slots)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            takes: AtomicU64::new(0),
            schema: CachePadded::new(AtomicU64::new(0)),
            turns: Stack::default(),
            drain: Stack::default(),
        }
    }

    /// Takes shared for the transaction that started at `start` in `slot`. Meeting another's
    /// exclusive take, it registers `c` on the take's end and reports it; the caller takes again
    /// once `c` is completed.
    pub(super) fn take_shared(&self, slot: u16, start: Ts, c: &Completion) -> Taken {
        loop {
            self.shared[slot as usize].store(start, Ordering::SeqCst);
            // Paired with the fence in `drain`, between a take's store and its read of the marks.
            fence(Ordering::SeqCst);
            let w = self.word.load(Ordering::SeqCst);
            let Some(owner) = owner_of(w).filter(|&o| o != slot) else {
                return Taken::Done;
            };
            // Out of the way of the take, which may have counted this mark.
            self.unmark(slot, start);
            self.turns.push(c.clone());
            fence(Ordering::SeqCst);
            if self.word.load(Ordering::SeqCst) == w {
                return Taken::Behind {
                    take: w >> TAKE_SHIFT,
                    owner,
                };
            }
        }
    }

    fn unmark(&self, slot: u16, start: Ts) {
        let _ = self.shared[slot as usize].compare_exchange(
            start,
            0,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.wake(&self.drain);
    }

    fn wake(&self, waits: &Stack<Completion>) {
        fence(Ordering::SeqCst);
        if waits.is_empty() {
            return;
        }
        for c in waits.take_all() {
            c.complete(0);
        }
    }

    /// Releases shared, which the transaction that started at `start` in `slot` holds.
    pub(super) fn release_shared(&self, slot: u16, start: Ts) {
        self.unmark(slot, start);
    }

    /// Puts an exclusive take of `slot`'s in the word, pending: the word, or the take in the way,
    /// with `c` registered on its end.
    pub(super) fn begin_exclusive(&self, slot: u16, c: &Completion) -> Result<u64, Taken> {
        loop {
            let w = self.word.load(Ordering::SeqCst);
            if w == 0 {
                let take = self.takes.fetch_add(1, Ordering::Relaxed) + 1;
                let mine = (slot as u64 + 1) | take << TAKE_SHIFT;
                if self
                    .word
                    .compare_exchange(0, mine, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Ok(mine);
                }
                continue;
            }
            let owner = owner_of(w).expect("a take names its owner");
            self.turns.push(c.clone());
            fence(Ordering::SeqCst);
            if self.word.load(Ordering::SeqCst) == w {
                return Err(Taken::Behind {
                    take: w >> TAKE_SHIFT,
                    owner,
                });
            }
        }
    }

    /// Drains the shared holders other than `slot` for its pending take `mine`, then holds it. With
    /// holders left it registers `c` on their releases and names them.
    pub(super) fn drain(&self, slot: u16, mine: u64, c: &Completion) -> Taken {
        loop {
            // Paired with the fence in `take_shared`, between a mark's store and its read of the
            // word: the take is in the word since `begin_exclusive`.
            fence(Ordering::SeqCst);
            let holders = self.holders(slot);
            if holders.is_empty() {
                return match self.word.compare_exchange(
                    mine,
                    mine | HELD,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => Taken::Done,
                    Err(_) => Taken::Lost,
                };
            }
            self.drain.push(c.clone());
            fence(Ordering::SeqCst);
            let holders = self.holders(slot);
            if !holders.is_empty() {
                return Taken::Holders(holders);
            }
        }
    }

    fn holders(&self, slot: u16) -> Vec<(u16, Ts)> {
        self.shared
            .iter()
            .enumerate()
            .filter(|&(s, _)| s != slot as usize)
            .filter_map(|(s, m)| match m.load(Ordering::SeqCst) {
                0 => None,
                start => Some((s as u16, start)),
            })
            .collect()
    }

    /// Clears the take `take` from the word, pending or held, if it is still there, and wakes
    /// whoever waits on it: a give-up, a release, or a flagger acting for a victim.
    pub(super) fn clear(&self, take: u64) -> bool {
        let cleared = self
            .word
            .compare_exchange(take, 0, Ordering::SeqCst, Ordering::SeqCst)
            .or_else(|_| {
                self.word
                    .compare_exchange(take | HELD, 0, Ordering::SeqCst, Ordering::SeqCst)
            })
            .is_ok();
        self.wake(&self.turns);
        cleared
    }

    /// The word, as an exclusive taker's take reads in it: held or not.
    pub(super) fn word(&self) -> u64 {
        self.word.load(Ordering::SeqCst)
    }

    /// Whether take number `take` is the one in the word, and its owner.
    pub(super) fn take_owner(&self, take: u64) -> Option<u16> {
        let w = self.word.load(Ordering::SeqCst);
        (w != 0 && w >> TAKE_SHIFT == take)
            .then(|| owner_of(w))
            .flatten()
    }

    /// Whether the transaction that started at `start` in `slot` holds shared.
    pub(super) fn holds_shared(&self, slot: u16, start: Ts) -> bool {
        self.shared[slot as usize].load(Ordering::SeqCst) == start
    }

    pub(super) fn schema(&self) -> Ts {
        self.schema.load(Ordering::SeqCst)
    }

    /// Releases the held exclusive take `take`, noting first the timestamp of its commit if it
    /// wrote the schema, so a writer past the gate reads it.
    pub(super) fn release_exclusive(&self, take: u64, schema: Option<Ts>) {
        if let Some(ts) = schema {
            self.schema.fetch_max(ts, Ordering::SeqCst);
        }
        self.clear(take);
    }
}

/// Whether a word's take is held, not only pending.
pub(super) fn held(word: u64) -> bool {
    word & HELD != 0
}

/// The number of a word's take.
pub(super) fn take_of(word: u64) -> u64 {
    word >> TAKE_SHIFT
}

#[cfg(all(test, not(atoll_loom)))]
mod tests;

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::atomic::AtomicUsize;
    use loom::sync::Arc;
    use loom::thread;

    fn model(f: impl Fn() + Sync + Send + 'static) {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(f);
    }

    /// A writer takes shared as an exclusive taker drains and holds: never both inside at once.
    #[test]
    fn shared_and_exclusive_never_overlap() {
        model(|| {
            let gate = Arc::new(Gate::new(2));
            let inside = Arc::new(AtomicUsize::new(0));
            let writer = {
                let (gate, inside) = (gate.clone(), inside.clone());
                thread::spawn(move || {
                    let c = Completion::new_wait();
                    if gate.take_shared(0, 10, &c) == Taken::Done {
                        inside.fetch_add(1, Ordering::SeqCst);
                        inside.fetch_sub(1, Ordering::SeqCst);
                        gate.release_shared(0, 10);
                    }
                })
            };
            let c = Completion::new_wait();
            let mine = gate.begin_exclusive(1, &c).unwrap();
            if gate.drain(1, mine, &c) == Taken::Done {
                assert_eq!(
                    inside.load(Ordering::SeqCst),
                    0,
                    "a writer inside the held gate"
                );
                gate.clear(mine);
            } else {
                gate.clear(mine);
            }
            writer.join().unwrap();
        });
    }

    /// A drain meets a holder that then releases: a drain left waiting on it is woken.
    #[test]
    fn a_drain_wake_is_never_lost() {
        model(|| {
            let gate = Arc::new(Gate::new(2));
            let c0 = Completion::new_wait();
            assert_eq!(gate.take_shared(0, 10, &c0), Taken::Done);
            let holder = {
                let gate = gate.clone();
                thread::spawn(move || gate.release_shared(0, 10))
            };
            let c = Completion::new_wait();
            let mine = gate.begin_exclusive(1, &c).unwrap();
            let drained = gate.drain(1, mine, &c);
            holder.join().unwrap();
            if let Taken::Holders(_) = drained {
                assert!(
                    c.finished(),
                    "a drain waiting on a holder gone was never woken"
                );
            }
        });
    }

    /// An upgrader holds shared itself and drains the one other holder: that holder's release
    /// wakes it.
    #[test]
    fn an_upgrader_is_woken_by_the_last_other_holder() {
        model(|| {
            let gate = Arc::new(Gate::new(2));
            let c0 = Completion::new_wait();
            assert_eq!(gate.take_shared(0, 10, &c0), Taken::Done);
            assert_eq!(gate.take_shared(1, 11, &c0), Taken::Done);
            let other = {
                let gate = gate.clone();
                thread::spawn(move || gate.release_shared(1, 11))
            };
            let c = Completion::new_wait();
            let mine = gate.begin_exclusive(0, &c).unwrap();
            let drained = gate.drain(0, mine, &c);
            other.join().unwrap();
            match drained {
                Taken::Done => {}
                Taken::Holders(h) => {
                    assert_eq!(h, vec![(1, 11)]);
                    assert!(c.finished(), "an upgrader never woken");
                }
                other => panic!("{other:?}"),
            }
        });
    }

    /// A shared taker waits on a pending take whose taker gives up: the give-up wakes it.
    #[test]
    fn a_take_waiter_is_woken_by_a_give_up() {
        model(|| {
            let gate = Arc::new(Gate::new(2));
            let c = Completion::new_wait();
            let mine = gate.begin_exclusive(1, &c).unwrap();
            let taker = {
                let gate = gate.clone();
                thread::spawn(move || {
                    gate.clear(mine);
                })
            };
            let cw = Completion::new_wait();
            let taken = gate.take_shared(0, 10, &cw);
            taker.join().unwrap();
            if let Taken::Behind { .. } = taken {
                assert!(cw.finished(), "a wait on a take given up never woken");
            }
        });
    }

    /// A stale clear of an old take never clears the next take in the word.
    #[test]
    fn a_clear_clears_only_its_own_take() {
        model(|| {
            let gate = Arc::new(Gate::new(2));
            let c = Completion::new_wait();
            let first = gate.begin_exclusive(0, &c).unwrap();
            assert!(gate.clear(first));
            let stale = {
                let gate = gate.clone();
                thread::spawn(move || gate.clear(first))
            };
            let second = gate.begin_exclusive(1, &c).unwrap();
            assert!(!stale.join().unwrap(), "an old take cleared again");
            assert_eq!(
                gate.word(),
                second,
                "the next take cleared by an old one's clear"
            );
        });
    }

    /// A writer raises a tree's stamp and then releases shared while an exclusive taker drains: a
    /// taker that holds reads the raised stamp.
    #[test]
    fn a_tree_stamp_raised_before_release_is_seen_after_the_drain() {
        model(|| {
            let gate = Arc::new(Gate::new(2));
            let stamp = Arc::new(AtomicU64::new(0));
            let c0 = Completion::new_wait();
            assert_eq!(gate.take_shared(0, 10, &c0), Taken::Done);
            let writer = {
                let (gate, stamp) = (gate.clone(), stamp.clone());
                thread::spawn(move || {
                    stamp.fetch_max(5, Ordering::Release);
                    gate.release_shared(0, 10);
                })
            };
            let c = Completion::new_wait();
            let mine = gate.begin_exclusive(1, &c).unwrap();
            if gate.drain(1, mine, &c) == Taken::Done {
                assert_eq!(
                    stamp.load(Ordering::Acquire),
                    5,
                    "a drained commit's stamp missed"
                );
            }
            gate.clear(mine);
            writer.join().unwrap();
        });
    }

    /// An exclusive holder notes its schema write and releases while a writer takes shared: a
    /// writer past the gate reads the schema timestamp it stored.
    #[test]
    fn a_writer_past_the_gate_reads_the_schema_timestamp() {
        model(|| {
            let gate = Arc::new(Gate::new(2));
            let c = Completion::new_wait();
            let mine = gate.begin_exclusive(1, &c).unwrap();
            assert_eq!(gate.drain(1, mine, &c), Taken::Done);
            let holder = {
                let gate = gate.clone();
                thread::spawn(move || gate.release_exclusive(mine, Some(5)))
            };
            let cw = Completion::new_wait();
            if gate.take_shared(0, 10, &cw) == Taken::Done {
                assert_eq!(
                    gate.schema(),
                    5,
                    "a writer past the gate missed the schema write"
                );
            }
            holder.join().unwrap();
        });
    }
}
