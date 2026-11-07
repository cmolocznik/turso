//! The preparation's word (DESIGN.md 22.5): where the segment being prepared stands, and who owns
//! it at each point.
//!
//! - *done*: no preparation under way;
//! - *needed*: a segment must be prepared and nobody has started it;
//! - *in flight*: its zero-fill writes or its data sync are submitted, owned by those requests;
//! - *owed*: its data sync landed, and the directory sync, the prepared end's publication and the
//!   look for the next segment are owed.
//!
//! Starting a preparation (a spare's rename, a file's create and allocation) and finishing one (the
//! directory sync) are synchronous file calls, so only a caller runs them, never a callback. A
//! caller takes the word by compare-and-swap into a taken state, *starting* from done or needed,
//! *finishing* from owed, so one caller owns each step and any other goes on. Taking it is no lock:
//! no step waits for it.
//!
//! A move to needed or owed makes a step runnable, so its mover wakes every waiter: it stores the
//! word and then, across a sequentially consistent fence, takes the waits; a waiter pushes its
//! wait and then, across one, reads the word. So either the mover takes the wait, or the waiter
//! reads the word and runs the step.

use self::sync::{AtomicU8, Ordering};

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU8, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicU8, Ordering};
}

const DONE: u8 = 0;
const NEEDED: u8 = 1;
const IN_FLIGHT: u8 = 2;
const OWED: u8 = 3;
const STARTING: u8 = 4;
const FINISHING: u8 = 5;

/// The step a caller took, which it owns until it moves the word on or lets it go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Step {
    /// Make the segment: a spare's rename, or a file's create, allocation and zero fill.
    Start,
    /// Sync the directory, publish the prepared end.
    Finish,
}

/// The word.
pub(super) struct Word(AtomicU8);

impl Default for Word {
    fn default() -> Self {
        Self(AtomicU8::new(DONE))
    }
}

impl Word {
    /// Moves done to needed, where a flush meets padding waiting for a segment nobody prepares:
    /// whether it moved, which makes its mover wake every waiter.
    pub(super) fn need(&self) -> bool {
        self.0
            .compare_exchange(DONE, NEEDED, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Moves in flight to owed, as the data sync of the segment lands: its mover then wakes every
    /// waiter.
    pub(super) fn owe(&self) {
        let was = self.0.swap(OWED, Ordering::SeqCst);
        assert_eq!(
            was, IN_FLIGHT,
            "a data sync landed for no preparation in flight"
        );
    }

    /// Whether a step is runnable for a caller: the word needed or owed. A word done with less than
    /// a segment prepared ahead is runnable too, which the caller knows ([`take`](Self::take)).
    pub(super) fn wants_a_caller(&self) -> bool {
        matches!(self.0.load(Ordering::SeqCst), NEEDED | OWED)
    }

    /// Takes the step a caller may run: finishing from owed, starting from needed, or from done
    /// when `short` says less than a segment is prepared ahead of what is admitted.
    pub(super) fn take(&self, short: impl Fn() -> bool) -> Option<Step> {
        loop {
            let (from, to, step) = match self.0.load(Ordering::SeqCst) {
                OWED => (OWED, FINISHING, Step::Finish),
                NEEDED => (NEEDED, STARTING, Step::Start),
                DONE if short() => (DONE, STARTING, Step::Start),
                _ => return None,
            };
            if self
                .0
                .compare_exchange(from, to, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Some(step);
            }
        }
    }

    /// Starting to in flight, before the first zero-fill write is submitted: the preparation is
    /// then its requests'.
    pub(super) fn hand_to_requests(&self) {
        self.moved(STARTING, IN_FLIGHT);
    }

    /// Starting to finishing: a spare, renamed, needs only its directory synced, which its starter
    /// runs too.
    pub(super) fn start_to_finish(&self) {
        self.moved(STARTING, FINISHING);
    }

    /// Finishing to done, once the prepared end is published: the finisher's ownership ends.
    pub(super) fn done(&self) {
        self.moved(FINISHING, DONE);
    }

    /// Lets a taken step go where it stands, with nothing more done: starting back to needed,
    /// finishing back to owed. What a step does on finding the log failed.
    pub(super) fn let_go(&self, step: Step) {
        match step {
            Step::Start => self.moved(STARTING, NEEDED),
            Step::Finish => self.moved(FINISHING, OWED),
        }
    }

    fn moved(&self, from: u8, to: u8) {
        let was = self.0.swap(to, Ordering::SeqCst);
        assert_eq!(
            was, from,
            "the preparation's word moved from {was}, not {from}"
        );
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::atomic::AtomicUsize;
    use loom::sync::Arc;
    use loom::thread;

    fn check(f: impl Fn() + Sync + Send + 'static) {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(f);
    }

    /// Two callers race for a needed word: exactly one takes the step, and the other goes on.
    #[test]
    fn one_caller_takes_each_step() {
        check(|| {
            let word = Arc::new(Word::default());
            assert!(word.need());
            let taken = Arc::new(AtomicUsize::new(0));
            let callers: Vec<_> = (0..2)
                .map(|_| {
                    let (word, taken) = (word.clone(), taken.clone());
                    thread::spawn(move || {
                        if let Some(step) = word.take(|| false) {
                            assert_eq!(step, Step::Start);
                            taken.fetch_add(1, Ordering::SeqCst);
                        }
                    })
                })
                .collect();
            for c in callers {
                c.join().unwrap();
            }
            assert_eq!(
                taken.load(Ordering::SeqCst),
                1,
                "a step taken twice or never"
            );
        });
    }

    /// A waiter registers and then reads the word, as `admit` and `wait_durable` do; a mover makes
    /// the word owed and then takes the waits, as the data sync's callback does: either the mover
    /// takes the wait, or the waiter reads the word and takes the step. Never neither.
    #[test]
    fn a_move_to_owed_wakes_or_is_seen_by_a_waiter() {
        use crate::storage::atoll::stack::Stack;
        use loom::sync::atomic::{fence, AtomicBool};
        check(|| {
            let word = Arc::new(Word::default());
            assert_eq!(word.take(|| true), Some(Step::Start));
            word.hand_to_requests();
            let waiting = Arc::new(Stack::<Arc<AtomicBool>>::default());
            let mover = {
                let (word, waiting) = (word.clone(), waiting.clone());
                thread::spawn(move || {
                    word.owe();
                    fence(Ordering::SeqCst);
                    for w in waiting.take_all() {
                        w.store(true, Ordering::Release);
                    }
                })
            };
            let woken = Arc::new(AtomicBool::new(false));
            waiting.push(woken.clone());
            fence(Ordering::SeqCst);
            let took = word.take(|| false);
            mover.join().unwrap();
            assert!(
                took == Some(Step::Finish) || woken.load(Ordering::Acquire),
                "a waiter neither woken nor running the owed step"
            );
        });
    }
}
