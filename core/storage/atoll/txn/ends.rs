//! Waiting for a transaction's end (DESIGN.md §17, §11.3), and naming the group that runs one.
//!
//! - **Ends.** Each slot keeps one list of end waiters, shared by every transaction it runs: starts
//!   are unique clock values, so a waiter that registers late is completed at the next end, for no
//!   one. A waiter registers, fences, and only then judges its key again, stepping the same write;
//!   a waker stores first (the slot's running start, or the leaf an undo restored), fences, and
//!   only then takes the list. By the two SeqCst fences, either the take finds the registration or
//!   the judgment reads the store. A waiter registers one completion per wait, which it reuses
//!   while that completion is still unfinished, so a list grows with the waiters, never with their
//!   retries.
//! - **Owners.** Each slot names the group its transaction belongs to, stored before its running
//!   start and cleared after it. A holder read as running, owner, running again, the start both
//!   times, is the group of that transaction: a later transaction's owner is stored after the slot's
//!   running start was cleared, so reading it makes the second read miss the start.

use super::sync::{fence, AtomicU64, Ordering};
use super::Ts;
use crate::io::Completion;
use crate::storage::atoll::stack::Stack;

/// A slot's end waiters.
#[derive(Default)]
pub(super) struct Ends {
    waits: Stack<Completion>,
}

impl Ends {
    /// Registers `c` to be completed at the slot's next end or undo. The caller then judges its
    /// key again before it parks on `c`.
    pub(super) fn register(&self, c: &Completion) {
        self.waits.push(c.clone());
        fence(Ordering::SeqCst);
    }

    /// Completes every waiter registered, after a store that ends or undoes what they wait on.
    pub(super) fn wake(&self) {
        fence(Ordering::SeqCst);
        if self.waits.is_empty() {
            return;
        }
        for c in self.waits.take_all() {
            c.complete(0);
        }
    }
}

/// Seats the transaction that started at `start`, of group `group`, in a slot: the owner first.
pub(super) fn seat(running: &AtomicU64, owner: &AtomicU64, start: Ts, group: u64) {
    owner.store(group, Ordering::Release);
    running.store(start, Ordering::Release);
}

/// Clears a slot's running transaction and its owner.
pub(super) fn vacate(running: &AtomicU64, owner: &AtomicU64) {
    running.store(0, Ordering::Release);
    owner.store(0, Ordering::Release);
}

/// The group of the transaction that started at `start`, if it still runs in the slot.
pub(super) fn holder(running: &AtomicU64, owner: &AtomicU64, start: Ts) -> Option<u64> {
    if running.load(Ordering::Acquire) != start {
        return None;
    }
    let group = owner.load(Ordering::Acquire);
    (running.load(Ordering::Acquire) == start).then_some(group)
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// A waiter registers and then reads the slot, as a writer ends it: if the waiter reads its
    /// writer still running and parks, the end completes its wait.
    #[test]
    fn an_end_wait_is_found_or_finds_the_end() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let ends = Arc::new(Ends::default());
            let running = Arc::new(AtomicU64::new(5));
            let writer = {
                let (ends, running) = (ends.clone(), running.clone());
                thread::spawn(move || {
                    running.store(0, Ordering::Release);
                    ends.wake();
                })
            };
            let c = crate::storage::atoll::notify::wait();
            ends.register(&c);
            let parks = running.load(Ordering::Acquire) == 5;
            writer.join().unwrap();
            assert!(!parks || c.finished(), "a parked wait never woken");
        });
    }

    /// The same for an undo, which restores the key a waiter judges again while its writer runs
    /// on.
    #[test]
    fn an_undo_wakes_a_waiter_or_is_seen_by_its_judgment() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let ends = Arc::new(Ends::default());
            let key = Arc::new(AtomicU64::new(1));
            let undo = {
                let (ends, key) = (ends.clone(), key.clone());
                thread::spawn(move || {
                    key.store(0, Ordering::Release);
                    ends.wake();
                })
            };
            let c = crate::storage::atoll::notify::wait();
            ends.register(&c);
            let parks = key.load(Ordering::Acquire) == 1;
            undo.join().unwrap();
            assert!(!parks || c.finished(), "a parked wait never woken");
        });
    }

    /// A slot runs one transaction, then another of another group, while a reader asks for the
    /// first's group: it gets the first's group or none, never the second's.
    #[test]
    fn a_slot_holder_is_never_misattributed() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let running = Arc::new(AtomicU64::new(0));
            let owner = Arc::new(AtomicU64::new(0));
            seat(&running, &owner, 10, 1);
            let slot = {
                let (running, owner) = (running.clone(), owner.clone());
                thread::spawn(move || {
                    vacate(&running, &owner);
                    seat(&running, &owner, 11, 2);
                })
            };
            let group = holder(&running, &owner, 10);
            slot.join().unwrap();
            assert!(
                group.is_none() || group == Some(1),
                "the first's start named {group:?}"
            );
        });
    }
}
