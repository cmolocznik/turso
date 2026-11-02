//! Waits for a position to rise to a mark, without a lock: completions on a take-all stack.
//!
//! A waiter pushes its completion and then looks at the position again, across a fence; whoever
//! raises the position raises it and then takes the stack, across a fence, completing every wait
//! it covers and putting back the rest. So either the raiser finds the wait, or the waiter sees
//! the position raised. A raiser that puts waits back looks at the position again, for one raised
//! while it held them.

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

impl Waits {
    /// A completion that finishes once `position()` reaches `mark`, or `None` if it has. A
    /// completion left behind by a waiter that saw the position is finished later, for no one.
    pub(super) fn wait(&self, mark: u64, position: impl Fn() -> u64) -> Option<Completion> {
        if position() >= mark {
            return None;
        }
        let c = Completion::new_wait();
        self.0.push((mark, c.clone()));
        fence(Ordering::SeqCst);
        (position() < mark).then_some(c)
    }

    /// Finishes every wait `position()` reaches, once it is raised.
    pub(super) fn wake(&self, position: impl Fn() -> u64) {
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
            // A position raised while those kept were taken woke none of them.
            fence(Ordering::SeqCst);
            if !kept || position() == reached {
                return;
            }
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
                        waits.wake(|| position.load(Ordering::Relaxed));
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
}
