//! Values set or taken once, by compare-and-swap: a set-once cell read with one load, and a
//! one-shot value taken by whichever thread swaps it out first.

use crate::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;

/// A value taken once, by whichever thread swaps it out first: a one-shot callback a completion
/// that may run more than once calls at most once.
pub struct TakeOnce<T>(AtomicPtr<T>);

impl<T> TakeOnce<T> {
    pub fn new(value: T) -> Self {
        Self(AtomicPtr::new(Box::into_raw(Box::new(value))))
    }

    pub fn take(&self) -> Option<T> {
        let p = self.0.swap(ptr::null_mut(), Ordering::AcqRel);
        // SAFETY: from `new`'s box, and the swap made this thread its only holder.
        (!p.is_null()).then(|| *unsafe { Box::from_raw(p) })
    }
}

impl<T> Drop for TakeOnce<T> {
    fn drop(&mut self) {
        drop(self.take());
    }
}

/// A value set once and kept for good, the first set winning: one compare-and-swap to set, one load
/// to read. Freed only with the cell, so a reader never meets it freed.
pub struct SetOnce<T>(AtomicPtr<T>);

impl<T> Default for SetOnce<T> {
    fn default() -> Self {
        Self(AtomicPtr::new(ptr::null_mut()))
    }
}

impl<T: Clone> SetOnce<T> {
    pub fn get(&self) -> Option<T> {
        let p = self.0.load(Ordering::Acquire);
        // SAFETY: set once from a box, freed only with the cell.
        (!p.is_null()).then(|| unsafe { (*p).clone() })
    }

    /// Sets `value` unless one is set already; returns the one kept.
    pub fn set(&self, value: T) -> T {
        let boxed = Box::into_raw(Box::new(value));
        match self
            .0
            .compare_exchange(ptr::null_mut(), boxed, Ordering::AcqRel, Ordering::Acquire)
        {
            // SAFETY: published now, freed only with the cell.
            Ok(_) => unsafe { (*boxed).clone() },
            Err(kept) => {
                // SAFETY: never published.
                drop(unsafe { Box::from_raw(boxed) });
                // SAFETY: as in `get`.
                unsafe { (*kept).clone() }
            }
        }
    }
}

impl<T> Drop for SetOnce<T> {
    fn drop(&mut self) {
        let p = self.0.load(Ordering::Acquire);
        if !p.is_null() {
            // SAFETY: the box `set` published, which nothing else frees.
            drop(unsafe { Box::from_raw(p) });
        }
    }
}
