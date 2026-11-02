//! An `Arc` shared through one pointer, without a lock: anyone in a step takes a reference of their
//! own, and one taken out or replaced is let go through the epochs ([`Retired`]), once no step
//! that could have read the pointer is left. A completion has a cell of its own
//! ([`CompletionCell`](super::buffer::cell::CompletionCell)): its handle is no `Arc` this holds.

use super::epoch::{Epochs, Retired, Step};
use crate::sync::atomic::{AtomicPtr, Ordering};
use crate::sync::Arc;
use std::ptr;

/// An `Arc<T>` or none.
pub struct ArcCell<T>(AtomicPtr<T>);

impl<T> Default for ArcCell<T> {
    fn default() -> Self {
        Self(AtomicPtr::new(ptr::null_mut()))
    }
}

// SAFETY: the cell holds an `Arc<T>` as its raw pointer.
unsafe impl<T: Send + Sync> Send for ArcCell<T> {}
unsafe impl<T: Send + Sync> Sync for ArcCell<T> {}

/// The values taken out of cells and not yet let go, and the epochs that say when.
pub struct Releasing<T> {
    epochs: Arc<Epochs>,
    retired: Retired<Arc<T>>,
}

impl<T> Releasing<T> {
    pub fn new(epochs: Arc<Epochs>) -> Self {
        Self {
            epochs,
            retired: Retired::default(),
        }
    }

    pub fn epochs(&self) -> &Arc<Epochs> {
        &self.epochs
    }

    fn release(&self, value: Arc<T>) {
        self.retired.retire(&self.epochs, value);
        drop(self.retired.reclaim(&self.epochs));
    }
}

impl<T: Send + Sync> ArcCell<T> {
    /// A cell holding `value`.
    pub fn new(value: Arc<T>) -> Self {
        Self(AtomicPtr::new(Arc::into_raw(value).cast_mut()))
    }

    /// The value in the cell, if any, as a reference of the caller's own.
    pub fn get(&self, _step: &Step) -> Option<Arc<T>> {
        let raw = self.0.load(Ordering::Acquire);
        // SAFETY: put with the cell's reference, which is let go only once every step that could
        // have read the pointer, this one among them, has ended.
        (!raw.is_null()).then(|| unsafe {
            Arc::increment_strong_count(raw);
            Arc::from_raw(raw)
        })
    }

    /// Puts `value` in the cell if it is empty; returns the value the cell then holds.
    pub fn put_if_empty(&self, value: Arc<T>, step: &Step) -> Arc<T> {
        let raw = Arc::into_raw(value.clone()).cast_mut();
        match self
            .0
            .compare_exchange(ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => value,
            Err(_) => {
                // SAFETY: never published, so still this thread's own.
                drop(unsafe { Arc::from_raw(raw) });
                // Taken out since by a remover: it holds what the cell holds now, or `value` is
                // put after it.
                match self.get(step) {
                    Some(kept) => kept,
                    None => self.put_if_empty(value, step),
                }
            }
        }
    }

    /// Replaces what the cell holds with `value`, letting the old one go.
    pub fn replace(&self, value: Option<Arc<T>>, releasing: &Releasing<T>) {
        let raw = value.map_or(ptr::null_mut(), |v| Arc::into_raw(v).cast_mut());
        let old = self.0.swap(raw, Ordering::AcqRel);
        if !old.is_null() {
            // SAFETY: the cell's reference, which the swap handed over.
            releasing.release(unsafe { Arc::from_raw(old) });
        }
    }
}

impl<T> Drop for ArcCell<T> {
    fn drop(&mut self) {
        let raw = self.0.load(Ordering::Acquire);
        if !raw.is_null() {
            // SAFETY: the cell's reference, and nothing reads the cell any more.
            drop(unsafe { Arc::from_raw(raw) });
        }
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;
    use crate::sync::atomic::AtomicUsize;

    struct Counted(Arc<AtomicUsize>, u64);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Readers in steps take the value while a writer replaces it, takes it out and puts one back
    /// if the cell is empty: every reference taken is to a value still alive, which Miri checks,
    /// and every value is let go once the cell is.
    fn readers_never_reach_a_value_let_go(readers: usize, rounds: u64) {
        let live = Arc::new(AtomicUsize::new(0));
        let make = |n: u64| {
            live.fetch_add(1, Ordering::AcqRel);
            Arc::new(Counted(live.clone(), n))
        };
        {
            let releasing = Arc::new(Releasing::new(Arc::new(Epochs::new())));
            let cell = Arc::new(ArcCell::new(make(0)));
            let handles: Vec<_> = (0..readers)
                .map(|_| {
                    let (releasing, cell) = (releasing.clone(), cell.clone());
                    crate::thread::spawn(move || {
                        for _ in 0..rounds {
                            let step = releasing.epochs().enter();
                            if let Some(v) = cell.get(&step) {
                                assert!(v.1 <= rounds * 2 + 1);
                            }
                            drop(step);
                            crate::thread::yield_now();
                        }
                    })
                })
                .collect();
            for n in 1..=rounds {
                cell.replace(Some(make(2 * n)), &releasing);
                cell.replace(None, &releasing);
                let step = releasing.epochs().enter();
                assert_eq!(cell.put_if_empty(make(2 * n + 1), &step).1, 2 * n + 1);
            }
            for h in handles {
                h.join().unwrap();
            }
        }
        assert_eq!(live.load(Ordering::Acquire), 0, "a value was never let go");
    }

    #[cfg(not(shuttle))]
    #[test]
    fn readers_never_reach_a_value_let_go_on_threads() {
        readers_never_reach_a_value_let_go(3, if cfg!(miri) { 300 } else { 50_000 });
    }

    #[cfg(shuttle)]
    #[test]
    fn readers_never_reach_a_value_let_go_in_shuttle_schedules() {
        crate::storage::atoll::testing::schedules(
            || readers_never_reach_a_value_let_go(2, 4),
            2_000,
        );
    }
}
