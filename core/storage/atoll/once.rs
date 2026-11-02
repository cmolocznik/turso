//! Values set or taken by compare-and-swap: a set-once cell read with one load, a value held by
//! one thread at a time, taken by swapping it out and put back by its taker, and a one-shot value
//! taken by whichever thread swaps it out first.

use crate::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;

/// A value held by one thread at a time: whoever swaps it out holds it, and puts it back, or on,
/// when done.
pub struct Baton<T>(AtomicPtr<T>);

// SAFETY: the value moves between threads only through the pointer's swaps.
unsafe impl<T: Send> Send for Baton<T> {}
unsafe impl<T: Send> Sync for Baton<T> {}

impl<T> Baton<T> {
    pub fn new(value: Option<T>) -> Self {
        Self(AtomicPtr::new(
            value.map_or(ptr::null_mut(), |v| Box::into_raw(Box::new(v))),
        ))
    }

    /// The value, if no one holds it.
    pub fn take(&self) -> Option<T> {
        let p = self.0.swap(ptr::null_mut(), Ordering::AcqRel);
        // SAFETY: from a box put here, and the swap made this thread its only holder.
        (!p.is_null()).then(|| *unsafe { Box::from_raw(p) })
    }

    /// Puts the value held back, for the next to take: by its holder, or whoever it hands it to.
    pub fn put(&self, value: T) {
        let old = self
            .0
            .swap(Box::into_raw(Box::new(value)), Ordering::AcqRel);
        assert!(old.is_null(), "a value put back while another was there");
    }
}

impl<T> Drop for Baton<T> {
    fn drop(&mut self) {
        drop(self.take());
    }
}

/// A value taken once, by whichever thread swaps it out first: a one-shot callback a completion
/// that may run more than once calls at most once.
pub struct TakeOnce<T>(Baton<T>);

impl<T> TakeOnce<T> {
    pub fn new(value: T) -> Self {
        Self(Baton::new(Some(value)))
    }

    pub fn take(&self) -> Option<T> {
        self.0.take()
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

impl<T> SetOnce<T> {
    /// The value set, if any, for as long as the cell lives.
    pub fn get_ref(&self) -> Option<&T> {
        let p = self.0.load(Ordering::Acquire);
        // SAFETY: set once from a box, freed only with the cell.
        (!p.is_null()).then(|| unsafe { &*p })
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
