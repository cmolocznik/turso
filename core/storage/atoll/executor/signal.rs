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
use super::{wake_nested, Header, Shared};
use std::sync::{Arc, OnceLock, Weak};

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
    listed: Mutex<Listed>,
    /// The executor whose tasks register here, set by the first: a signal serves one, whose
    /// drain invokes the external runners a notify's wakes moved.
    owner: OnceLock<Weak<Shared>>,
}

/// The tasks registered, each at most once, and how many tasks are declared to watch the signal:
/// the list keeps room for every one of them, so registering and notifying allocate nothing.
struct Listed {
    tasks: Vec<Arc<Header>>,
    declared: usize,
}

impl Default for Signal {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            listed: Mutex::new(Listed {
                tasks: Vec::new(),
                declared: 0,
            }),
            owner: OnceLock::new(),
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
        self.bump();
        self.wake_listed();
    }

    /// The count moves: a change is published, and a look that read the count before sees it.
    pub(super) fn bump(&self) {
        self.count.fetch_add(1, Ordering::Release);
    }

    /// Wakes every task listed and empties the list, under the mutex; then, the mutex let go,
    /// invokes the external runners those wakes moved (22.3).
    pub(super) fn wake_listed(&self) {
        let moved: usize = {
            let mut listed = lock(&self.listed);
            listed
                .tasks
                .drain(..)
                .map(|header| wake_nested(&header))
                .sum()
        };
        if moved > 0 {
            if let Some(shared) = self.owner.get().and_then(Weak::upgrade) {
                shared.drain(moved);
            }
        }
    }

    /// One more task may watch the signal: the list makes room for every task declared.
    pub(super) fn declare(&self) {
        let mut listed = lock(&self.listed);
        listed.declared += 1;
        let room = listed.declared - listed.tasks.len();
        listed.tasks.reserve(room);
    }

    /// Registers `header`, a task of `shared` declared to watch the signal, unless it is listed
    /// already; whether the count still reads `read`.
    pub(super) fn register(&self, header: &Arc<Header>, read: u64, shared: &Arc<Shared>) -> bool {
        let owner = self.owner.get_or_init(|| Arc::downgrade(shared));
        assert!(
            std::ptr::eq(owner.as_ptr(), Arc::as_ptr(shared)),
            "a signal watched by the tasks of two executors"
        );
        crate::race::point("executor.registering");
        {
            let mut listed = lock(&self.listed);
            if !listed.tasks.iter().any(|h| Arc::ptr_eq(h, header)) {
                listed.tasks.push(header.clone());
            }
        }
        crate::race::point("executor.registered");
        self.read() == read
    }

    /// A task declared to watch the signal watches it no more (done, or closed): off the list,
    /// and no longer counted in the room the list keeps.
    pub(super) fn leave(&self, header: &Arc<Header>) {
        let mut listed = lock(&self.listed);
        let Some(declared) = listed.declared.checked_sub(1) else {
            drop(listed);
            unreachable!("a task left a signal it was not declared to watch");
        };
        listed.declared = declared;
        listed.tasks.retain(|h| !Arc::ptr_eq(h, header));
    }

    #[cfg(test)]
    pub(super) fn listed(&self) -> usize {
        lock(&self.listed).tasks.len()
    }

    /// How many tasks the list has room for.
    #[cfg(test)]
    pub(super) fn room(&self) -> usize {
        lock(&self.listed).tasks.capacity()
    }

    /// Whether the list's mutex is free to take now.
    #[cfg(all(test, not(atoll_loom)))]
    pub(super) fn listed_free(&self) -> bool {
        self.listed.try_lock().is_some()
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
