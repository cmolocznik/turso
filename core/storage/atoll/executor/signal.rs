//! Signals (DESIGN.md 22.2 item 2): "something a task watches changed". A signal is a counter and
//! the list of tasks registered on it, under the signal's own mutex.
//!
//! A notifier publishes its change first, by its own release store or under its own lock, then
//! moves the counter (release) and, under the mutex, wakes every listed task and empties the list.
//! A task reads each counter it watches (acquire) before it looks at its condition; finding it
//! unmet, its step says `Idle` with the counts it read, and the executor registers it on each
//! signal under the mutex, then reads the counter again: one moved means a notify came between the
//! look and the registration, and the task is queued at once. A notify the second read missed took
//! the mutex after the registration let it go, so it finds the task listed. "Moved" is inequality:
//! the counter would have to come all the way round between the two reads to hide a notify.

use self::sync::{lock, AtomicU64, Mutex, Ordering};
use super::{wake, Header};
use std::sync::Arc;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::storage::atoll::locks::{Mutex, MutexGuard};
    pub(super) use crate::sync::atomic::{AtomicU64, Ordering};
    pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock()
    }
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicU64, Ordering};
    pub(super) use loom::sync::{Mutex, MutexGuard};
    pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().unwrap()
    }
}

/// The most signals one step may name (22.2: the checkpoint's due, full requested and room
/// wanted).
pub(crate) const WATCHED: usize = 3;

pub(crate) struct Signal {
    count: AtomicU64,
    /// The tasks registered, each at most once, with room for every task declared to watch it,
    /// so registering and notifying allocate nothing.
    listed: Mutex<Vec<Arc<Header>>>,
}

impl Default for Signal {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            listed: Mutex::new(Vec::new()),
        }
    }
}

impl Signal {
    /// The count, read before a task looks at the condition the signal announces.
    pub(crate) fn read(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }

    /// Announces a change already published: the count moves, then every task listed is woken
    /// and the list emptied, under the mutex. Its work is at most the list, however often
    /// notifies come.
    pub(crate) fn notify(&self) {
        self.count.fetch_add(1, Ordering::Release);
        let mut listed = lock(&self.listed);
        for header in listed.drain(..) {
            wake(&header);
        }
    }

    /// Makes room for one more task that may watch the signal.
    pub(super) fn declare(&self) {
        lock(&self.listed).reserve(1);
    }

    /// Registers `header` unless it is listed already; whether the count still reads `read`.
    pub(super) fn register(&self, header: &Arc<Header>, read: u64) -> bool {
        crate::race::point("executor.registering");
        {
            let mut listed = lock(&self.listed);
            if !listed.iter().any(|h| Arc::ptr_eq(h, header)) {
                listed.push(header.clone());
            }
        }
        crate::race::point("executor.registered");
        self.read() == read
    }

    /// Takes `header` off the list: a task removed, or closed.
    pub(super) fn unlist(&self, header: &Arc<Header>) {
        lock(&self.listed).retain(|h| !Arc::ptr_eq(h, header));
    }

    #[cfg(test)]
    pub(super) fn listed(&self) -> usize {
        lock(&self.listed).len()
    }
}

/// The signals one step waits on, each with the count it read before looking: at most
/// [`WATCHED`], held inline.
pub(crate) struct Watched(pub(super) [Option<(Arc<Signal>, u64)>; WATCHED]);

impl Watched {
    /// `signals`, each with the count read before the look; more than [`WATCHED`] is a job
    /// naming signals it did not declare.
    pub(crate) fn of(signals: &[(&Arc<Signal>, u64)]) -> Self {
        assert!(signals.len() <= WATCHED, "a step watching too many signals");
        let mut watched = [const { None }; WATCHED];
        for (slot, (signal, read)) in watched.iter_mut().zip(signals) {
            *slot = Some(((*signal).clone(), *read));
        }
        Self(watched)
    }
}
