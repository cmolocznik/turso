//! Work done by one thread at a time, without a lock: whoever finds it under way leaves it to the
//! thread doing it, which looks again before it stops, so no request goes unanswered.
//!
//! [`Solo`] is for work done within one call: a request swaps a flag in, which the thread taking a
//! turn swaps back before it works, so it sees what the request was made after, and after its turn
//! it looks at the flag again. A request is a swap, not a store: a store could fall before an
//! earlier request in the flag's order, and a turn swapping back that one would take both without
//! seeing what the later was made after. [`Claim`] is for work held
//! across IO, let go by whoever finishes it: the holder lets go and then looks again at what is
//! needed. In both the request and the letting go are each written and then the other read across
//! a fence, so either the requester finds the work let go and does it, or the holder sees the
//! request.

use self::sync::{fence, AtomicBool, Ordering};

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicBool, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicBool, Ordering};
}

/// Work done within one call, one turn at a time.
#[derive(Default)]
pub struct Solo {
    busy: AtomicBool,
    asked: AtomicBool,
}

/// A turn at the work: the next is taken once it is dropped.
pub struct Turn<'a>(&'a Solo);

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        self.0.busy.store(false, Ordering::Release);
        // Pairs with the fence of a request: either the next turn is taken for it, or it takes
        // the turn itself.
        fence(Ordering::SeqCst);
    }
}

impl Solo {
    /// Asks for a turn's work: done by a turn taken after this, which sees what this thread did
    /// before asking.
    pub fn ask(&self) {
        self.asked.swap(true, Ordering::Release);
        fence(Ordering::SeqCst);
    }

    /// A turn, if one was asked for and none is under way.
    pub fn turn(&self) -> Option<Turn<'_>> {
        // Relaxed: read after the fence of a request or of the last turn's end, which orders it.
        if !self.asked.load(Ordering::Relaxed) {
            return None;
        }
        if self
            .busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        // Taken back before the work, acquiring the request it reads: one made during the work
        // gets another turn.
        self.asked.swap(false, Ordering::Acquire);
        Some(Turn(self))
    }
}

/// Work held across IO by one thread at a time, and only ever tried.
#[derive(Default)]
pub struct Claim(AtomicBool);

impl Claim {
    /// Takes the claim, unless it is held: its holder looks again when it lets go.
    pub fn take(&self) -> bool {
        // Pairs with the fence of a holder letting go: one sees the other.
        fence(Ordering::SeqCst);
        self.0
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Lets the claim go: its holder then looks again at what is needed, and takes it again if
    /// anything is.
    pub fn release(&self) {
        self.0.store(false, Ordering::Release);
        fence(Ordering::SeqCst);
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

    /// Two threads each make a request and ask for a turn: whatever the order, a turn taken after
    /// the second request sees both.
    #[test]
    fn every_request_is_seen_by_a_turn_taken_after_it() {
        check(|| {
            let solo = Arc::new(Solo::default());
            let made = Arc::new(AtomicUsize::new(0));
            let seen = Arc::new(AtomicUsize::new(0));
            let request = |solo: &Solo, made: &AtomicUsize, seen: &AtomicUsize| {
                made.fetch_add(1, Ordering::Relaxed);
                solo.ask();
                while let Some(_turn) = solo.turn() {
                    seen.fetch_max(made.load(Ordering::Relaxed), Ordering::Relaxed);
                }
            };
            let other = {
                let (solo, made, seen) = (solo.clone(), made.clone(), seen.clone());
                thread::spawn(move || request(&solo, &made, &seen))
            };
            request(&solo, &made, &seen);
            other.join().unwrap();
            assert_eq!(seen.load(Ordering::Relaxed), 2, "a request no turn saw");
        });
    }

    /// A need arises while the claim is held, its holder letting go: either the one who needs it
    /// takes the claim, or the holder sees the need.
    #[test]
    fn a_need_arising_as_the_claim_is_let_go_is_met() {
        check(|| {
            let claim = Arc::new(Claim::default());
            let needed = Arc::new(AtomicBool::new(false));
            assert!(claim.take());
            let meet = |claim: &Claim, needed: &AtomicBool| {
                while needed.load(Ordering::Relaxed) {
                    if !claim.take() {
                        return;
                    }
                    needed.store(false, Ordering::Relaxed);
                    claim.release();
                }
            };
            let other = {
                let (claim, needed) = (claim.clone(), needed.clone());
                thread::spawn(move || {
                    needed.store(true, Ordering::Relaxed);
                    meet(&claim, &needed);
                })
            };
            claim.release();
            meet(&claim, &needed);
            other.join().unwrap();
            assert!(!needed.load(Ordering::Relaxed), "a need was left unmet");
        });
    }
}
