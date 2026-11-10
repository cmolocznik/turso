//! Waits for a position to rise to a mark, without a lock: completions on a take-all stack.
//!
//! A waiter pushes its completion and then looks at the position again, across a fence; whoever
//! raises the position raises it and then takes the stack, across a fence, completing every wait
//! it covers and putting back the rest. So either the raiser finds the wait, or the waiter sees
//! the position raised. A raiser that puts waits back looks at the position again, for one raised
//! while it held them.
//!
//! Putting a wait back is a registration, and it ends as one does: across a fence, the raiser then
//! looks at what else finishes every wait (the log's failure, a preparation step made runnable),
//! since a failure or a move whose take ran while it held the waits found none of them.

use self::sync::{fence, Ordering};
use super::super::stack::Stack;
use crate::io::Completion;
use crate::CompletionError;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, Ordering};
}

#[derive(Default)]
pub(super) struct Waits(Stack<(u64, Completion)>);

/// What a raiser that put waits back finds when it looks again.
pub(super) enum Look {
    /// Nothing that finishes every wait.
    Stay,
    /// The log failed: every wait fails.
    Fail(CompletionError),
    /// A step is runnable: every wait is finished, for its caller to call again.
    Retry,
}

impl Waits {
    /// A completion that finishes once `position()` reaches `mark`, or `None` if it has. A
    /// completion left behind by a waiter that saw the position is finished later, for no one.
    pub(super) fn wait(&self, mark: u64, position: impl Fn() -> u64) -> Option<Completion> {
        if position() >= mark {
            return None;
        }
        let c = crate::storage::atoll::notify::wait();
        self.0.push((mark, c.clone()));
        fence(Ordering::SeqCst);
        (position() < mark).then_some(c)
    }

    /// Finishes every wait `position()` reaches, once it is raised. Waits put back are looked at
    /// again with `look`, which a failure or a move stores before its take.
    pub(super) fn wake(&self, position: impl Fn() -> u64, look: impl Fn() -> Look) {
        loop {
            fence(Ordering::SeqCst);
            let reached = position();
            let mut kept = false;
            for (mark, c) in self.0.take_all() {
                if mark <= reached {
                    c.complete(0);
                } else {
                    self.0.push((mark, c));
                    kept = true;
                }
            }
            if !kept {
                return;
            }
            // Those kept were on no stack for a moment: a failure's or a move's take then missed
            // them, and a position raised then woke none of them.
            fence(Ordering::SeqCst);
            match look() {
                Look::Fail(e) => return self.fail(e),
                Look::Retry => return self.wake_all(),
                Look::Stay => {}
            }
            if position() == reached {
                return;
            }
        }
    }

    /// Finishes every wait, whatever its mark, for its caller to call again: a preparation step
    /// has become runnable. Across a fence from the move the caller stored.
    pub(super) fn wake_all(&self) {
        fence(Ordering::SeqCst);
        for (_, c) in self.0.take_all() {
            c.complete(0);
        }
    }

    /// Fails every wait with `e`, once the failure is visible to waiters looking again.
    pub(super) fn fail(&self, e: CompletionError) {
        fence(Ordering::SeqCst);
        for (_, c) in self.0.take_all() {
            c.error(e);
        }
    }

    /// Whether anyone waits; one a waker holds does not count.
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
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
            let waited = waits.wait(10, || position.load(Ordering::Relaxed));
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
                .wait(20, || position.load(Ordering::Relaxed))
                .expect("a wait for a mark not reached");
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
}
