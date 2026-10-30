//! A thread that wakes each waker handed to it once its instant passes. An asynchronous step that
//! must wait out a busy handler's delay is polled again when the delay ends, where the engine's own
//! wake would have it polled again at once.

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    sync::{Condvar, LazyLock, Mutex},
    task::Waker,
    time::Instant,
};

/// A waker and the instant it is due, ordered so that the earliest is the heap's top.
struct Due {
    at: Instant,
    waker: Waker,
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

static TIMER: LazyLock<&'static Timer> = LazyLock::new(|| {
    let timer: &'static Timer = Box::leak(Box::default());
    std::thread::Builder::new()
        .name("turso-busy-timer".to_string())
        .spawn(|| timer.run())
        .expect("failed to start the busy-wait timer thread");
    timer
});

/// Wakes `waker` once `at` has passed.
pub(crate) fn wake_at(at: Instant, waker: Waker) {
    TIMER.add(at, waker);
}

impl Timer {
    fn add(&self, at: Instant, waker: Waker) {
        let mut due = self.due.lock().unwrap();
        let earliest = due.peek().is_none_or(|next| at < next.at);
        due.push(Due { at, waker });
        if earliest {
            self.changed.notify_one();
        }
    }

    fn run(&self) {
        let mut due = self.due.lock().unwrap();
        loop {
            let now = Instant::now();
            let mut ready = Vec::new();
            while due.peek().is_some_and(|next| next.at <= now) {
                ready.push(due.pop().expect("a due waker was peeked").waker);
            }
            if !ready.is_empty() {
                // Woken outside the lock: a waker may poll its task at once, and the task hand
                // this timer another waker.
                drop(due);
                ready.into_iter().for_each(Waker::wake);
                due = self.due.lock().unwrap();
                continue;
            }
            due = match due.peek().map(|next| next.at - now) {
                None => self.changed.wait(due).unwrap(),
                Some(wait) => self.changed.wait_timeout(due, wait).unwrap().0,
            };
        }
    }
}
