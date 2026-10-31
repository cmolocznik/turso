//! A thread that fires each action handed to it once its instant passes. It wakes an asynchronous
//! step that must wait out a busy handler's delay, which the engine's own wake would have polled
//! again at once, and it ends the park of a checkpoint at its busy timeout.

use crate::Completion;
use parking_lot::{Condvar, Mutex};
use std::{cmp::Ordering, collections::BinaryHeap, sync::LazyLock, task::Waker, time::Instant};

/// What the timer does once an action's instant passes.
enum Fire {
    Wake(Waker),
    /// Completes the completion, unless something completed it first.
    Complete(Completion),
}

impl Fire {
    fn fire(self) {
        match self {
            Fire::Wake(waker) => waker.wake(),
            Fire::Complete(completion) => completion.complete(0),
        }
    }
}

/// An action and the instant it is due, ordered so that the earliest is the heap's top.
struct Due {
    at: Instant,
    fire: Fire,
}

impl PartialEq for Due {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
    }
}

impl Eq for Due {}

impl PartialOrd for Due {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Due {
    fn cmp(&self, other: &Self) -> Ordering {
        other.at.cmp(&self.at)
    }
}

#[derive(Default)]
struct Timer {
    due: Mutex<BinaryHeap<Due>>,
    changed: Condvar,
}

/// The timer, or `None` where its thread cannot run.
static TIMER: LazyLock<Option<&'static Timer>> = LazyLock::new(|| {
    if cfg!(target_family = "wasm") {
        return None;
    }
    let timer: &'static Timer = Box::leak(Box::default());
    std::thread::Builder::new()
        .name("turso-timer".to_string())
        .spawn(|| timer.run())
        .ok()
        .map(|_| timer)
});

/// Whether the timer's thread runs, so that an action handed to it fires.
pub fn runs() -> bool {
    TIMER.is_some()
}

/// Wakes `waker` once `at` has passed. False where the timer's thread cannot run.
pub fn wake_at(at: Instant, waker: Waker) -> bool {
    add(at, Fire::Wake(waker))
}

/// Completes `completion` once `at` has passed, unless something completed it first. False where
/// the timer's thread cannot run.
pub(crate) fn complete_at(at: Instant, completion: Completion) -> bool {
    add(at, Fire::Complete(completion))
}

fn add(at: Instant, fire: Fire) -> bool {
    let Some(timer) = *TIMER else {
        return false;
    };
    let mut due = timer.due.lock();
    let earliest = due.peek().is_none_or(|next| at < next.at);
    due.push(Due { at, fire });
    if earliest {
        timer.changed.notify_one();
    }
    true
}

impl Timer {
    fn run(&self) {
        let mut due = self.due.lock();
        loop {
            let now = Instant::now();
            let mut ready = Vec::new();
            while due.peek().is_some_and(|next| next.at <= now) {
                ready.push(due.pop().expect("a due action was peeked").fire);
            }
            if !ready.is_empty() {
                // Fired outside the lock: a waker may poll its task at once, and the task hand
                // this timer another action.
                drop(due);
                ready.into_iter().for_each(Fire::fire);
                due = self.due.lock();
                continue;
            }
            match due.peek().map(|next| next.at - now) {
                None => self.changed.wait(&mut due),
                Some(wait) => {
                    self.changed.wait_for(&mut due, wait);
                }
            }
        }
    }
}
