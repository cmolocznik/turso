//! A completion posted where steps find it, without a lock.
//!
//! A cell holds a completion or none, as one pointer. Whoever owns the cell puts a completion in
//! and takes it out; anyone in a step reads it and takes a reference of their own. A completion
//! taken out is let go only once no step that could have read the pointer is left, through the
//! epochs ([`Retired`]), so a reader never takes a reference to one already freed.

use super::super::epoch::{Epochs, Retired, Step};
use super::memory::Zeroable;
use crate::io::Completion;
use crate::sync::atomic::{AtomicPtr, Ordering};
use crate::sync::Arc;
use std::ptr;

#[derive(Default)]
pub struct CompletionCell(AtomicPtr<()>);

// SAFETY: all zeros is a null pointer, an empty cell.
unsafe impl Zeroable for CompletionCell {}

impl Drop for CompletionCell {
    fn drop(&mut self) {
        let raw = self.0.load(Ordering::Acquire);
        if !raw.is_null() {
            // SAFETY: the cell's own reference, and nothing reads the cell any more.
            drop(unsafe { Completion::from_raw(raw) });
        }
    }
}

/// The completions taken out of cells and not yet let go, and the epochs that say when.
pub struct Retiring {
    epochs: Arc<Epochs>,
    retired: Retired<Completion>,
}

impl Retiring {
    pub fn new(epochs: Arc<Epochs>) -> Self {
        Self {
            epochs,
            retired: Retired::default(),
        }
    }

    fn go(&self, c: Completion) {
        self.retired.retire(&self.epochs, c);
        drop(self.retired.reclaim(&self.epochs));
    }
}

impl CompletionCell {
    /// The completion in the cell, if any, as a reference of the caller's own.
    pub fn get(&self, _step: &Step) -> Option<Completion> {
        let raw = self.0.load(Ordering::Acquire);
        // SAFETY: put with its reference, which the cell lets go only once every step that could
        // have read the pointer, this one among them, has ended.
        (!raw.is_null()).then(|| unsafe { Completion::clone_raw(raw) })
    }

    /// Puts `c` in the cell, which must be empty: for the cell's owner.
    pub fn put(&self, c: Completion) {
        let old = self.0.swap(c.into_raw().cast_mut(), Ordering::AcqRel);
        debug_assert!(old.is_null(), "a completion put in a cell that held one");
    }

    /// Takes the completion out of the cell, if any: for the cell's owner.
    pub fn take(&self, retiring: &Retiring) -> Option<Completion> {
        let raw = self.0.swap(ptr::null_mut(), Ordering::AcqRel);
        if raw.is_null() {
            return None;
        }
        // SAFETY: the cell's own reference, which the swap handed over.
        let held = unsafe { Completion::from_raw(raw) };
        let out = held.clone();
        retiring.go(held);
        Some(out)
    }

    /// The unfinished completion in the cell, or a new one put there if it holds none or a
    /// finished one: for a cell anyone may fill, whose completion anyone may finish.
    pub fn current(&self, _step: &Step, retiring: &Retiring) -> Completion {
        loop {
            let raw = self.0.load(Ordering::Acquire);
            if !raw.is_null() {
                // SAFETY: as in `get`.
                let c = unsafe { Completion::clone_raw(raw) };
                if !c.finished() {
                    return c;
                }
            }
            let fresh = Completion::new_wait();
            let fresh_raw = fresh.clone().into_raw().cast_mut();
            match self
                .0
                .compare_exchange(raw, fresh_raw, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    if !raw.is_null() {
                        // SAFETY: the reference the cell held, handed over by the exchange.
                        retiring.go(unsafe { Completion::from_raw(raw) });
                    }
                    return fresh;
                }
                // SAFETY: never published, so still this thread's own.
                Err(_) => drop(unsafe { Completion::from_raw(fresh_raw) }),
            }
        }
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;

    const ROUNDS: usize = if cfg!(miri) { 200 } else { 20_000 };

    /// The owner puts completions in a cell and takes them out, finishing them, while readers
    /// in steps take references to whatever it holds and others replace a finished one: every
    /// reference is taken to a completion still alive, which Miri checks, and none leaks.
    #[test]
    fn readers_never_reach_a_completion_let_go() {
        let epochs = Arc::new(Epochs::new());
        let retiring = Arc::new(Retiring::new(epochs.clone()));
        let cell = Arc::new(CompletionCell::default());
        let shared = Arc::new(CompletionCell::default());
        let owner = {
            let (cell, retiring) = (cell.clone(), retiring.clone());
            crate::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    cell.put(Completion::new_wait());
                    crate::thread::yield_now();
                    if let Some(c) = cell.take(&retiring) {
                        c.complete(0);
                    }
                }
            })
        };
        let readers: Vec<_> = (0..2)
            .map(|r| {
                let (epochs, retiring, cell, shared) = (
                    epochs.clone(),
                    retiring.clone(),
                    cell.clone(),
                    shared.clone(),
                );
                crate::thread::spawn(move || {
                    for _ in 0..ROUNDS {
                        let step = epochs.enter();
                        if let Some(c) = cell.get(&step) {
                            let _ = c.finished();
                        }
                        let c = shared.current(&step, &retiring);
                        if r == 0 {
                            c.complete(0);
                        }
                        drop(step);
                        crate::thread::yield_now();
                    }
                })
            })
            .collect();
        owner.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
        assert!(cell.take(&retiring).is_none());
    }
}
