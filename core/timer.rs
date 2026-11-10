//! One thread fires deadlines. Each waiting caller owns its registration; ending the wait removes
//! its action and releases the completion or waker without waiting for the deadline.

use crate::Completion;
use parking_lot::{Condvar, Mutex};
use std::{fmt, sync::LazyLock, task::Waker, time::Instant};

/// An armed deadline, cancelled on drop. Keep it until the wait ends.
#[must_use = "dropping the registration cancels the deadline"]
pub struct Timed {
    timer: &'static Timer,
    key: Key,
    completion: Option<usize>,
    at: Instant,
}

impl Timed {
    #[cfg(test)]
    pub(crate) fn registration(&self) -> (usize, u64) {
        (self.key.slot, self.key.generation)
    }

    /// Whether this registration wakes the same completion. Checking it never takes the timer's
    /// lock. The caller holds `completion` and checks its deadline first: before that deadline,
    /// only dropping this unique registration can remove its action, so address reuse cannot
    /// make a different completion compare equal.
    pub(crate) fn is_for(&self, at: Instant, completion: &Completion) -> bool {
        self.at == at && self.completion == completion.addr()
    }
}

impl fmt::Debug for Timed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timed").field("key", &self.key).finish()
    }
}

impl Drop for Timed {
    fn drop(&mut self) {
        self.timer.cancel(self.key);
    }
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

/// Wakes `waker` once `at` has passed. `None` where the timer's thread cannot run.
#[must_use = "keep the registration until the wait ends"]
pub fn wake_at(at: Instant, waker: Waker) -> Option<Timed> {
    add(at, Fire::Wake(waker))
}

/// Completes `completion` once `at` has passed, unless something completed it first.
/// `None` where the timer's thread cannot run.
#[must_use = "keep the registration until the wait ends"]
pub(crate) fn complete_at(at: Instant, completion: Completion) -> Option<Timed> {
    debug_assert!(completion.addr().is_some(), "a yield has no deadline wait");
    add(at, Fire::Complete(completion))
}

fn add(at: Instant, fire: Fire) -> Option<Timed> {
    let timer = (*TIMER)?;
    let completion = match &fire {
        Fire::Complete(completion) => completion.addr(),
        Fire::Wake(_) => None,
    };
    let (key, earliest) = {
        let mut due = timer.due.lock();
        let earliest = due.first().is_none_or(|first| at < first);
        (due.insert(at, fire), earliest)
    };
    if earliest {
        timer.changed.notify_one();
    }
    Some(Timed {
        timer,
        key,
        completion,
        at,
    })
}

/// A caller may take the timer lock while holding a connection or wait-cell lock. Actions run
/// and are dropped after the timer lock is released; nothing takes an Atoll lock under it.
#[derive(Default)]
struct Timer {
    due: Mutex<Deadlines>,
    changed: Condvar,
}

impl Timer {
    fn run(&self) {
        let _worker = AbortOnPanic::new();
        let mut ready = Vec::new();
        let mut due = self.due.lock();
        loop {
            let now = Instant::now();
            due.take_ready(now, &mut ready);
            if !ready.is_empty() {
                drop(due);
                fire_ready(&mut ready);
                due = self.due.lock();
                continue;
            }
            match due.first().map(|at| at.saturating_duration_since(now)) {
                None => self.changed.wait(&mut due),
                Some(wait) => {
                    self.changed.wait_for(&mut due, wait);
                }
            }
        }
    }

    fn cancel(&self, key: Key) {
        let _cancel = AbortOnPanic::new();
        let (action, earliest_changed) = {
            let mut due = self.due.lock();
            let first = due.first();
            let action = due.remove(key);
            (action, first != due.first())
        };
        if earliest_changed {
            self.changed.notify_one();
        }
        // Completion and RawWaker drops can run code. No timer lock is held here.
        drop(action);
    }
}

/// A panic cannot leave the process accepting deadlines after its only timer thread died.
/// The same fail-stop rule as an Atoll delivery applies to every action in the batch.
fn fire_ready(ready: &mut Vec<Fire>) {
    let _firing = AbortOnPanic::new();
    for fire in ready.drain(..) {
        fire.fire();
    }
}

struct AbortOnPanic {
    was_unwinding: bool,
}

impl AbortOnPanic {
    fn new() -> Self {
        Self {
            was_unwinding: crate::thread::panicking(),
        }
    }
}

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if !self.was_unwinding && crate::thread::panicking() {
            crate::io::fatal("a timer action panicked");
        }
    }
}

/// What the timer does once an action's instant passes.
enum Fire {
    Wake(Waker),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Key {
    slot: usize,
    generation: u64,
}

/// A slot stores its heap position while armed, and its next free slot otherwise.
struct Entry {
    generation: u64,
    at: Instant,
    action: Option<Fire>,
    position: usize,
    next_free: Option<usize>,
}

/// An indexed min-heap. Cancellation removes the action itself, not a tombstone for a later scan.
#[derive(Default)]
struct Deadlines {
    entries: Vec<Entry>,
    heap: Vec<usize>,
    free: Option<usize>,
}

impl Deadlines {
    fn insert(&mut self, at: Instant, action: Fire) -> Key {
        let slot = match self.free {
            Some(slot) => {
                self.free = self.entries[slot].next_free;
                let entry = &mut self.entries[slot];
                debug_assert!(entry.action.is_none());
                entry.at = at;
                entry.action = Some(action);
                entry.position = self.heap.len();
                entry.next_free = None;
                slot
            }
            None => {
                let slot = self.entries.len();
                self.entries.push(Entry {
                    generation: 0,
                    at,
                    action: Some(action),
                    position: self.heap.len(),
                    next_free: None,
                });
                slot
            }
        };
        self.heap.push(slot);
        self.up(self.heap.len() - 1);
        Key {
            slot,
            generation: self.entries[slot].generation,
        }
    }

    fn remove(&mut self, key: Key) -> Option<Fire> {
        let entry = self.entries.get(key.slot)?;
        if entry.generation != key.generation || entry.action.is_none() {
            return None;
        }
        let position = entry.position;
        let generation = entry
            .generation
            .checked_add(1)
            .expect("timer slot generation exhausted");
        let removed = self.heap.swap_remove(position);
        debug_assert_eq!(removed, key.slot);
        if position < self.heap.len() {
            let moved = self.heap[position];
            self.entries[moved].position = position;
            if position != 0 && self.before(position, (position - 1) / 2) {
                self.up(position);
            } else {
                self.down(position);
            }
        }
        let entry = &mut self.entries[key.slot];
        entry.generation = generation;
        entry.next_free = self.free;
        self.free = Some(key.slot);
        entry.action.take()
    }

    fn take_ready(&mut self, now: Instant, ready: &mut Vec<Fire>) {
        while let Some(&slot) = self.heap.first() {
            let entry = &self.entries[slot];
            if entry.at > now {
                break;
            }
            let key = Key {
                slot,
                generation: entry.generation,
            };
            ready.push(self.remove(key).expect("the heap lists an armed slot"));
        }
    }

    fn first(&self) -> Option<Instant> {
        self.heap.first().map(|&slot| self.entries[slot].at)
    }

    fn up(&mut self, mut position: usize) {
        while position != 0 {
            let parent = (position - 1) / 2;
            if !self.before(position, parent) {
                break;
            }
            self.swap(position, parent);
            position = parent;
        }
    }

    fn down(&mut self, mut position: usize) {
        loop {
            let left = position * 2 + 1;
            if left >= self.heap.len() {
                break;
            }
            let right = left + 1;
            let next = if right < self.heap.len() && self.before(right, left) {
                right
            } else {
                left
            };
            if !self.before(next, position) {
                break;
            }
            self.swap(position, next);
            position = next;
        }
    }

    fn before(&self, a: usize, b: usize) -> bool {
        let a = self.heap[a];
        let b = self.heap[b];
        (self.entries[a].at, a) < (self.entries[b].at, b)
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.heap.swap(a, b);
        self.entries[self.heap[a]].position = a;
        self.entries[self.heap[b]].position = b;
    }
}

#[cfg(test)]
mod tests;
