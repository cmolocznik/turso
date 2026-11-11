//! Waits for a position to rise to a mark, without a lock: completions on a take-all stack.
//!
//! A waiter pushes its completion and then looks at the position again, across a fence; whoever
//! raises the position raises it and then takes the stack, across a fence, completing every wait
//! it covers and putting back the rest. So either the raiser finds the wait, or the waiter sees
//! the position raised. A raiser that puts waits back looks at the position again, for one raised
//! while it held them.
//!
//! Putting a wait back is a registration, and it ends as one does: across a fence, the raiser then
//! looks at what else finishes every wait (the log's failure), since a failure whose take ran
//! while it held the waits found none of them, and at the position again. It puts them back in
//! the nodes they came in, allocating nothing; only if a raise while it held them reached one does
//! it take the stack a second time, finishing what that reaches and, looking once more, finishing
//! in place each wait a raise during that hold reached, as that wait's waiter would. So a wake
//! takes the stack twice at most to finish waits, however the position moves meanwhile, never
//! chasing it, and once more, after either look, if the log has failed, to fail every wait (the
//! auditor's #4360, #4366, #4373 and #4394).
//!
//! A registration is its waiter's ([`Registration`]): given up (its wait ended or dropped) while
//! still on the stack, it is counted, and a take drops it rather than putting it back, finishing
//! its wait then, for no one or for a waiter that still looks at it: the waiter giving it up runs
//! no wait's callbacks, and a waiter that drops its registration and waits on its wait all the
//! same is woken as one standing is, never left with a wait finished at once to call again on.
//! The entries, on the stack or held by a take, and those of them given up are counted in
//! one word, so a registration reads both at one moment: finding those given up outnumbering the
//! rest, it first takes the stack as a raise does. So once no registration, give-up or wake is
//! under way, the entries number at most one more than twice the waits that stood at the newest
//! registration, however many waits are made and given up while the position stands still;
//! those given up after it stay until the next take, by a raise or a registration, so the
//! entries never number more than one more than twice the most waits that stood at once (the
//! auditor's #4356 and #4360).

use self::sync::{fence, AtomicU64, AtomicU8, Ordering};
use super::super::stack::{Gathered, Stack};
use crate::io::Completion;
use crate::sync::Arc;
use crate::CompletionError;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicU64, AtomicU8, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicU64, AtomicU8, Ordering};
}

/// An entry: its mark, its wait, and its end ([`STANDING`], [`GIVEN_UP`] or [`FINISHED`]).
type Entry = (u64, Completion, Arc<AtomicU8>);

/// An entry's end: its waiter waits.
const STANDING: u8 = 0;
/// Its waiter gave it up: the take that drops it finishes its wait.
const GIVEN_UP: u8 = 1;
/// Its wait is finished: by a take, or in place by a raiser that put it back.
const FINISHED: u8 = 2;

/// One entry in [`Waits`]' count word; one given up or finished in place, still an entry, is
/// counted in its high half too.
const ONE_ENTRY: u64 = 1;
const ONE_GIVEN_UP: u64 = 1 << 32;

#[derive(Default)]
pub(super) struct Waits {
    stack: Stack<Entry>,
    /// The entries, on the stack or held by a take, in the low half; those of them given up or
    /// finished in place, in the high half.
    counts: Arc<AtomicU64>,
}

/// A waiter's registration among the waits ([`Waits::wait`]): its wait, which the waiter gives up
/// once it no longer waits.
pub(super) struct Registration {
    pub(super) wait: Completion,
    end: Arc<AtomicU8>,
    counts: Arc<AtomicU64>,
}

impl Registration {
    /// The waiter no longer waits: unless its wait is finished already, its entry is given up,
    /// counted while it stays on the stack, for the take that drops it to finish its wait.
    pub(super) fn give_up(self) {
        end_standing(&self.counts, &self.end, GIVEN_UP);
    }
}

/// Moves a standing entry's end to `to`, counted given up before the move, so a take that sees
/// the move never counts below zero; a move lost to another takes the count back. Whether it
/// moved.
fn end_standing(counts: &AtomicU64, end: &AtomicU8, to: u8) -> bool {
    counts.fetch_add(ONE_GIVEN_UP, Ordering::SeqCst);
    let moved = end
        .compare_exchange(STANDING, to, Ordering::AcqRel, Ordering::Acquire)
        .is_ok();
    if !moved {
        counts.fetch_sub(ONE_GIVEN_UP, Ordering::SeqCst);
    }
    moved
}

#[cfg(test)]
crate::thread::thread_local! {
    /// The passes of `wake_again` on this thread that put waits back.
    static WAKES_AGAIN: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// What a raiser that put waits back finds when it looks again.
pub(super) enum Look {
    /// Nothing that finishes every wait.
    Stay,
    /// The log failed: every wait fails.
    Fail(CompletionError),
}

impl Waits {
    /// A registration whose wait finishes once `position()` reaches `mark`, or `None` if it has. A
    /// registration left behind by a waiter that saw the position is given up by it. Those given
    /// up outnumbering the rest, the stack is taken first as a raise takes it (`look` as the
    /// raise's), which drops them.
    pub(super) fn wait(
        &self,
        mark: u64,
        position: impl Fn() -> u64,
        look: impl Fn() -> Look,
    ) -> Option<Registration> {
        if position() >= mark {
            return None;
        }
        let counts = self.counts.load(Ordering::SeqCst);
        if 2 * (counts / ONE_GIVEN_UP) > counts % ONE_GIVEN_UP {
            self.wake(&position, &look);
        }
        let c = crate::storage::atoll::notify::wait();
        let end = Arc::new(AtomicU8::new(STANDING));
        self.counts.fetch_add(ONE_ENTRY, Ordering::SeqCst);
        self.stack.push((mark, c.clone(), end.clone()));
        fence(Ordering::SeqCst);
        let registration = Registration {
            wait: c,
            end,
            counts: self.counts.clone(),
        };
        if position() >= mark {
            registration.give_up();
            return None;
        }
        Some(registration)
    }

    /// Drops an entry taken, counted gone, finishing its wait with `outcome` unless it was
    /// finished in place: a wait reached or failed, or one given up, for no one or for a waiter
    /// that still looks at it.
    fn drop_taken(&self, c: &Completion, end: &AtomicU8, outcome: impl FnOnce(&Completion)) {
        let was = end.swap(FINISHED, Ordering::AcqRel);
        self.counts.fetch_sub(
            match was {
                STANDING => ONE_ENTRY,
                _ => ONE_ENTRY + ONE_GIVEN_UP,
            },
            Ordering::SeqCst,
        );
        if was != FINISHED {
            outcome(c);
        }
    }

    /// Finishes every wait `position()` reaches, once it is raised, allocating nothing as it does:
    /// takes the stack, finishing the waits reached and dropping those given up, and pushes the
    /// rest back in the nodes they came in. Across a fence, those put back are looked at again:
    /// with `look`, which a failure stores before its take, and at the position. Unless a raise
    /// while they were held reached one of them, which that raise's take may have missed, nothing
    /// more is owed: every raise past the least of their marks comes after the look, and its take
    /// finds them. Otherwise one more pass ([`wake_again`](Self::wake_again)) finishes them, so a
    /// wake takes the stack twice at most to finish waits, however the position moves meanwhile,
    /// and once more, from either look, to fail them all if the log has failed (the auditor's
    /// #4360, #4373 and #4394).
    pub(super) fn wake(&self, position: impl Fn() -> u64, look: impl Fn() -> Look) {
        fence(Ordering::SeqCst);
        let reached = position();
        let mut kept = Gathered::default();
        let mut least = u64::MAX;
        let mut taken = self.stack.take_chain();
        while let Some(held) = taken.next_held() {
            let (mark, _, end) = held.item();
            if *mark > reached && end.load(Ordering::Acquire) == STANDING {
                // Its waiter may give it up from here on: counted, and a later take drops it.
                least = least.min(*mark);
                kept.add(held);
            } else {
                let (_, c, end) = held.into_item();
                self.drop_taken(&c, &end, |c| c.complete(0));
            }
        }
        if kept.is_empty() {
            return;
        }
        self.stack.push_gathered(kept);
        // Those kept were on no stack for a moment: a failure's take then missed them, and a
        // position raised then woke none of them.
        fence(Ordering::SeqCst);
        if let Look::Fail(e) = look() {
            return self.fail(e);
        }
        if position() >= least {
            self.wake_again(position, look);
        }
    }

    /// A wake's pass after a raise during its hold reached a wait it put back: takes the stack
    /// once, finishing the waits reached and dropping those given up, puts the rest back, and
    /// across a fence looks at the failure and at the position again, finishing in place each one
    /// a raise during this hold reached. It keeps what that look needs of each, which allocates,
    /// in a pass made only when a wait is to be finished.
    fn wake_again(&self, position: impl Fn() -> u64, look: impl Fn() -> Look) {
        fence(Ordering::SeqCst);
        let reached = position();
        let mut kept = Gathered::default();
        let mut looked = Vec::new();
        let mut taken = self.stack.take_chain();
        while let Some(held) = taken.next_held() {
            let (mark, c, end) = held.item();
            if *mark > reached && end.load(Ordering::Acquire) == STANDING {
                looked.push((*mark, c.clone(), end.clone()));
                kept.add(held);
            } else {
                let (_, c, end) = held.into_item();
                self.drop_taken(&c, &end, |c| c.complete(0));
            }
        }
        if kept.is_empty() {
            return;
        }
        #[cfg(test)]
        WAKES_AGAIN.with(|n| n.set(n.get() + 1));
        self.stack.push_gathered(kept);
        fence(Ordering::SeqCst);
        if let Look::Fail(e) = look() {
            return self.fail(e);
        }
        let now = position();
        if now == reached {
            return;
        }
        // Finished in place, as their waiters would see the position, for a later take to drop: a
        // standing one counted with those given up; one given up while held, counted already, for
        // a waiter that still looks at it, the take that would have finished it having found the
        // stack without it (the auditor's #4366).
        for (mark, c, end) in looked {
            if mark <= now
                && (end_standing(&self.counts, &end, FINISHED)
                    || end
                        .compare_exchange(GIVEN_UP, FINISHED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok())
            {
                c.complete(0);
            }
        }
    }

    /// Fails every wait with `e`, once the failure is visible to waiters looking again.
    pub(super) fn fail(&self, e: CompletionError) {
        fence(Ordering::SeqCst);
        for (_, c, end) in self.stack.take_all() {
            self.drop_taken(&c, &end, |c| c.error(e));
        }
    }

    /// Whether anyone waits; one a waker holds does not count.
    pub(super) fn is_empty(&self) -> bool {
        self.stack.is_empty()
    }

    /// The entries, on the stack or held by a take, and those of them given up, for a test.
    #[cfg(test)]
    pub(super) fn counts(&self) -> (u64, u64) {
        let counts = self.counts.load(Ordering::SeqCst);
        (counts % ONE_GIVEN_UP, counts / ONE_GIVEN_UP)
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// A wake on a log that has not failed takes the stack twice at most however the position
    /// moves meanwhile (the auditor's #4360): a raise during its first hold that reaches a wait put back costs one more pass,
    /// whose take finishes it, and a wait that pass put back that a raise during its own hold
    /// reached is finished in place, counted given up for the next take to drop (mutant: the
    /// first pass taking the stack again while the position moves, which chases a position
    /// raised at every look).
    #[test]
    fn a_wake_takes_the_stack_twice_at_most_however_the_position_moves_meanwhile() {
        let waits = Waits::default();
        let position = Cell::new(5u64);
        let registrations: Vec<_> = [10, 100, 1000]
            .into_iter()
            .map(|m| waits.wait(m, || position.get(), || Look::Stay).unwrap())
            .collect();
        WAKES_AGAIN.with(|n| n.set(0));
        // Each look raises the position past the next mark, as raises landing during each hold do.
        let looks = Cell::new(0u32);
        waits.wake(
            || position.get(),
            || {
                looks.set(looks.get() + 1);
                match looks.get() {
                    1 => position.set(15),
                    2 => position.set(150),
                    3 => position.set(1500),
                    // A wake that looks again and again is cut off here.
                    _ => return Look::Fail(CompletionError::Aborted),
                }
                Look::Stay
            },
        );
        let looked = looks.get();
        let again = WAKES_AGAIN.with(|n| n.get());
        let finished: Vec<bool> = registrations.iter().map(|r| r.wait.finished()).collect();
        let failed = registrations.iter().any(|r| r.wait.failed());
        let after = waits.counts();
        drop(registrations);
        assert_eq!(looked, 2, "looks the wake took");
        assert_eq!(again, 1, "passes after the first");
        assert_eq!(finished, [true, true, false], "waits finished, by mark");
        assert!(!failed, "a wait failed");
        assert_eq!(after, (2, 1), "entries and those given up after the wake");
    }

    /// A wait given up that a raise during a pass's hold reached is finished by that pass's look
    /// at the position, counted given up already, for a waiter that dropped its registration and
    /// still waits on its wait: the raise's take found the stack without it (the auditor's #4366;
    /// mutant: the look finishing only waits still standing).
    #[test]
    fn a_wait_given_up_and_reached_during_a_hold_is_finished_by_the_look() {
        let waits = Waits::default();
        let position = Cell::new(5u64);
        let near = waits.wait(10, || position.get(), || Look::Stay).unwrap();
        let far = waits.wait(20, || position.get(), || Look::Stay).unwrap();
        let far_wait = far.wait.clone();
        let far = std::cell::RefCell::new(Some(far));
        let looks = Cell::new(0u32);
        waits.wake(
            || position.get(),
            || {
                looks.set(looks.get() + 1);
                match looks.get() {
                    // The first pass's hold: a raise reaches the near wait, so a second pass runs.
                    1 => position.set(10),
                    // The second pass's hold: the far wait's waiter gives it up and a raise
                    // reaches it, the raise's take having found the stack without it.
                    2 => {
                        far.borrow_mut().take().unwrap().give_up();
                        position.set(20);
                    }
                    _ => {}
                }
                Look::Stay
            },
        );
        let finished = (near.wait.finished(), far_wait.finished());
        let after = waits.counts();
        drop(near);
        assert_eq!(finished, (true, true), "(near, far given up) finished");
        assert_eq!(after, (1, 1), "entries and those given up after the wake");
    }

    /// A wake whose waits all stand puts them back in the nodes they came in and makes no second
    /// pass, so it allocates nothing; one that finishes some drops their nodes and makes none
    /// (the auditor's #4373; mutant: the waits put back as items pushed anew, each in a node made
    /// for it).
    #[test]
    fn a_wake_puts_waits_back_in_their_own_nodes() {
        use super::super::super::stack::NODES_MADE;
        let waits = Waits::default();
        let position = Cell::new(0u64);
        let registrations: Vec<_> = (1..=64)
            .map(|m| {
                waits
                    .wait(m * 10, || position.get(), || Look::Stay)
                    .unwrap()
            })
            .collect();
        WAKES_AGAIN.with(|n| n.set(0));
        NODES_MADE.with(|n| n.set(0));
        position.set(5);
        waits.wake(|| position.get(), || Look::Stay);
        let standing = (NODES_MADE.with(|n| n.get()), waits.counts());
        position.set(320);
        waits.wake(|| position.get(), || Look::Stay);
        let half = (NODES_MADE.with(|n| n.get()), waits.counts());
        let again = WAKES_AGAIN.with(|n| n.get());
        let finished = registrations.iter().filter(|r| r.wait.finished()).count();
        drop(registrations);
        assert_eq!(
            standing,
            (0, (64, 0)),
            "(nodes made, counts) after a wake finishing none"
        );
        assert_eq!(
            half,
            (0, (32, 0)),
            "(nodes made, counts) once half were reached"
        );
        assert_eq!(finished, 32, "waits finished");
        assert_eq!(again, 0, "second passes");
    }

    /// A raise's take drops every wait given up and counts them gone, and the count of those given
    /// up never passes the entries: the word read whole after waits given up and finished in
    /// every order. A wait given up is finished by the take that drops it, never by its waiter:
    /// one that drops its registration and waits on its wait all the same is woken by the raise,
    /// not left calling again on a wait finished at once (mutant: the give-up finishing the wait,
    /// which spins a caller whose wait is made anew each call and never pumps its backend).
    #[test]
    fn a_take_counts_every_wait_it_drops_or_finishes_gone() {
        let waits = Waits::default();
        let position = Cell::new(0u64);
        let mut registrations = (1..=6)
            .map(|m| {
                waits
                    .wait(m * 10, || position.get(), || Look::Stay)
                    .unwrap()
            })
            .collect::<Vec<_>>()
            .into_iter();
        // Two given up, one reached by the raise, one given up after the raise reached it.
        let given_up: Vec<Completion> = (0..2)
            .map(|_| {
                let r = registrations.next().unwrap();
                let wait = r.wait.clone();
                r.give_up();
                wait
            })
            .collect();
        let reached = registrations.next().unwrap();
        let late = registrations.next().unwrap();
        let rest: Vec<_> = registrations.collect();
        let unfinished_before = given_up.iter().all(|w| !w.finished());
        let before = waits.counts();
        position.set(40);
        waits.wake(|| position.get(), || Look::Stay);
        late.give_up();
        let after = waits.counts();
        let finished =
            given_up.iter().all(|w| w.finished() && !w.failed()) && reached.wait.finished();
        drop((reached, rest));
        assert!(unfinished_before, "a give-up finished its wait");
        assert_eq!(
            before,
            (6, 2),
            "entries and those given up before the raise"
        );
        assert!(finished, "a wait reached or given up was left by the take");
        assert_eq!(after, (2, 0), "entries and those given up after the raise");
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::atomic::AtomicU64;
    use loom::sync::Arc;
    use loom::thread;

    /// One thread waits for a mark while two raise the position to it in two steps, each waking
    /// after: the wait sees the mark reached or is finished.
    #[test]
    fn a_wait_for_a_mark_reached_is_never_left_waiting() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let waits = Arc::new(Waits::default());
            let position = Arc::new(AtomicU64::new(0));
            let raisers: Vec<_> = [5, 10]
                .into_iter()
                .map(|to| {
                    let (waits, position) = (waits.clone(), position.clone());
                    thread::spawn(move || {
                        position.fetch_max(to, Ordering::Relaxed);
                        waits.wake(|| position.load(Ordering::Relaxed), || Look::Stay);
                    })
                })
                .collect();
            let waited = waits
                .wait(10, || position.load(Ordering::Relaxed), || Look::Stay)
                .map(|r| r.wait.clone());
            for r in raisers {
                r.join().unwrap();
            }
            if let Some(c) = waited {
                assert!(c.finished(), "a wait for a mark reached was left waiting");
            }
        });
    }

    /// A raiser takes a wait its raise does not reach and puts it back while the log fails, as a
    /// write's landing does beside a failed one: either the failure's take finds the wait, or the
    /// raiser's look sees the failure. Never neither.
    #[test]
    fn a_wait_put_back_is_failed_by_a_failure_its_raiser_held_it_through() {
        use loom::sync::atomic::AtomicBool;
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let waits = Arc::new(Waits::default());
            let position = Arc::new(AtomicU64::new(0));
            let failed = Arc::new(AtomicBool::new(false));
            let waited = waits
                .wait(20, || position.load(Ordering::Relaxed), || Look::Stay)
                .expect("a wait for a mark not reached")
                .wait
                .clone();
            let raiser = {
                let (waits, position, failed) = (waits.clone(), position.clone(), failed.clone());
                thread::spawn(move || {
                    position.fetch_max(10, Ordering::Relaxed);
                    waits.wake(
                        || position.load(Ordering::Relaxed),
                        || match failed.load(Ordering::SeqCst) {
                            true => Look::Fail(CompletionError::Aborted),
                            false => Look::Stay,
                        },
                    );
                })
            };
            failed.store(true, Ordering::SeqCst);
            waits.fail(CompletionError::Aborted);
            raiser.join().unwrap();
            assert!(waited.finished(), "a wait put back missed the failure");
        });
    }

    /// A wait given up while a raiser holds it taken, a second raise reaching its mark meanwhile
    /// and finding the stack without it: the raiser that puts it back finishes it as it looks at
    /// the position again, so a waiter that gave up its registration and waits on its wait all the
    /// same is woken with no further take (the auditor's #4366; mutant: the look finishing only
    /// waits still standing).
    #[test]
    fn a_wait_given_up_while_held_is_finished_by_its_raisers_look() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let waits = Arc::new(Waits::default());
            let position = Arc::new(AtomicU64::new(5));
            let registration = waits
                .wait(10, || position.load(Ordering::SeqCst), || Look::Stay)
                .expect("a wait for a mark not reached");
            let wait = registration.wait.clone();
            let raisers: Vec<_> = [5, 10]
                .into_iter()
                .map(|to| {
                    let (waits, position) = (waits.clone(), position.clone());
                    thread::spawn(move || {
                        position.fetch_max(to, Ordering::SeqCst);
                        waits.wake(|| position.load(Ordering::SeqCst), || Look::Stay);
                    })
                })
                .collect();
            registration.give_up();
            for r in raisers {
                r.join().unwrap();
            }
            assert!(
                wait.finished(),
                "a wait given up while held was left at its mark"
            );
        });
    }

    /// A wait given up while a raiser holds it taken, beside a registration and the raise (the
    /// auditor's #4360): the wait the raise reached is finished, and once the position reaches
    /// every mark and a last wake takes the stack, the count word is back to nothing, so no
    /// entry or give-up is counted twice or lost however the three interleave.
    #[test]
    fn a_give_up_a_registration_and_a_raise_at_once_leave_the_count_whole() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let waits = Arc::new(Waits::default());
            let position = Arc::new(AtomicU64::new(0));
            let near = waits
                .wait(10, || position.load(Ordering::SeqCst), || Look::Stay)
                .expect("a wait for a mark not reached");
            let far = waits
                .wait(30, || position.load(Ordering::SeqCst), || Look::Stay)
                .expect("a wait for a mark not reached");
            let near_wait = near.wait.clone();
            let raiser = {
                let (waits, position) = (waits.clone(), position.clone());
                thread::spawn(move || {
                    position.fetch_max(20, Ordering::SeqCst);
                    waits.wake(|| position.load(Ordering::SeqCst), || Look::Stay);
                })
            };
            let canceller = thread::spawn(move || far.give_up());
            let late = waits.wait(40, || position.load(Ordering::SeqCst), || Look::Stay);
            raiser.join().unwrap();
            canceller.join().unwrap();
            assert!(near_wait.finished(), "a wait the raise reached was left");
            position.store(100, Ordering::SeqCst);
            waits.wake(|| position.load(Ordering::SeqCst), || Look::Stay);
            if let Some(late) = late {
                assert!(
                    late.wait.finished(),
                    "a wait the last raise reached was left"
                );
                late.give_up();
            }
            near.give_up();
            assert_eq!(waits.counts(), (0, 0), "entries and give-ups left counted");
        });
    }
}
