//! Values cached in cells and held by pins, without a lock.
//!
//! A cell holds a value or none, as the cache's own reference to it. Whoever finds a value pins it
//! before using it, and a value no pin holds may be evicted. Pinning and evicting each change the
//! value's pin word by compare-and-swap, an eviction taking it from no pins to evicted, so of a pin
//! and an eviction racing on a value one wins: either the pin keeps it in its cell or the eviction
//! takes it out, and the pin fails and looks again.
//!
//! Whoever unlinks a value from its cell, evicted or forgotten, lets the cell's reference to it go
//! through the epochs, so a step that read the cell before the unlink still reaches the value.

use self::sync::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
// The engine's: what the cache hands in, under loom too.
use super::epoch::{Epochs, Retired, Step};
use crate::sync::Arc;
use std::ops::Deref;
use std::ptr;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
}

/// In a pin word: the value was evicted, and pins no longer take.
const EVICTED: u64 = 1 << 63;

/// How many hold a value, or that it was evicted.
#[derive(Default)]
pub struct Pins(AtomicU64);

impl Pins {
    /// Pins the value, unless it was evicted.
    fn pin(&self) -> bool {
        let mut w = self.0.load(Ordering::Relaxed);
        loop {
            if w & EVICTED != 0 {
                return false;
            }
            // Acquire: an evictor's unlink of a value whose word this read does not see evicted
            // comes after, so the cell still held it.
            match self
                .0
                .compare_exchange_weak(w, w + 1, Ordering::Acquire, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(now) => w = now,
            }
        }
    }

    /// Adds a pin to a value its caller already holds pinned.
    pub fn repin(&self) {
        let was = self.0.fetch_add(1, Ordering::Relaxed);
        debug_assert!(
            was != 0 && was & EVICTED == 0,
            "repinned a value not pinned"
        );
    }

    pub fn unpin(&self) {
        let was = self.0.fetch_sub(1, Ordering::Release);
        debug_assert!(was & !EVICTED != 0, "unpinned a value no pin holds");
    }

    /// Marks the value evicted, if no pin holds it.
    fn evict(&self) -> bool {
        self.0
            .compare_exchange(0, EVICTED, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    fn evicted(&self) -> bool {
        self.0.load(Ordering::Acquire) & EVICTED != 0
    }
}

/// A value cells hold by pins.
pub trait Pinnable: Send + Sync {
    fn pins(&self) -> &Pins;
}

/// A value pinned: kept in its cell while any pin holds it, unless it is forgotten.
pub struct Pinned<T: Pinnable>(Arc<T>);

impl<T: Pinnable> Pinned<T> {
    /// A value of the caller's own, pinned by the caller, to put in a cell.
    pub fn new(value: T) -> Self {
        let value = Arc::new(value);
        assert!(value.pins().pin(), "a new value evicted");
        Self(value)
    }
}

impl<T: Pinnable> Clone for Pinned<T> {
    fn clone(&self) -> Self {
        self.0.pins().repin();
        Self(self.0.clone())
    }
}

impl<T: Pinnable> Drop for Pinned<T> {
    fn drop(&mut self) {
        self.0.pins().unpin();
    }
}

impl<T: Pinnable> Deref for Pinned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// The values a set of cells hold: how many, and the references unlinked cells let go of.
pub struct Cells<T> {
    epochs: Arc<Epochs>,
    retired: Retired<Arc<T>>,
    count: AtomicUsize,
}

impl<T> Cells<T> {
    pub fn new(epochs: Arc<Epochs>) -> Self {
        Self {
            epochs,
            retired: Retired::default(),
            count: AtomicUsize::new(0),
        }
    }

    /// The values the cells hold.
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    pub fn epochs(&self) -> &Arc<Epochs> {
        &self.epochs
    }
}

/// A cell holding a value or none.
pub struct PinCell<T>(AtomicPtr<T>);

impl<T> Default for PinCell<T> {
    fn default() -> Self {
        Self(AtomicPtr::new(ptr::null_mut()))
    }
}

// SAFETY: the cell holds an `Arc<T>` as its raw pointer.
unsafe impl<T: Send + Sync> Send for PinCell<T> {}
unsafe impl<T: Send + Sync> Sync for PinCell<T> {}

impl<T: Pinnable> PinCell<T> {
    /// The value in the cell, pinned, if the cell holds one not evicted. An evicted one is
    /// unlinked on the way, as its evictor will.
    pub fn get(&self, _step: &Step, cells: &Cells<T>) -> Option<Pinned<T>> {
        loop {
            let p = self.0.load(Ordering::Acquire);
            if p.is_null() {
                return None;
            }
            // SAFETY: the cell's reference, let go only once every step that could have read the
            // pointer, this one among them, has ended.
            let value = unsafe { &*p };
            if value.pins().pin() {
                // SAFETY: as above; the pin is the new reference's.
                return Some(Pinned(unsafe {
                    Arc::increment_strong_count(p);
                    Arc::from_raw(p)
                }));
            }
            self.unlink(p, cells);
        }
    }

    /// Puts `value` in the cell if it is empty: whether it went in.
    pub fn put(&self, value: &Pinned<T>, cells: &Cells<T>) -> bool {
        let raw = Arc::into_raw(value.0.clone()).cast_mut();
        match self
            .0
            .compare_exchange(ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                cells.count.fetch_add(1, Ordering::AcqRel);
                true
            }
            Err(_) => {
                // SAFETY: never published, so still this thread's own.
                drop(unsafe { Arc::from_raw(raw) });
                false
            }
        }
    }

    /// Evicts the value in the cell, if no pin holds it: whether it went.
    pub fn evict(&self, _step: &Step, cells: &Cells<T>) -> bool {
        let p = self.0.load(Ordering::Acquire);
        if p.is_null() {
            return false;
        }
        // SAFETY: as in `get`.
        let value = unsafe { &*p };
        if !value.pins().evict() && !value.pins().evicted() {
            return false;
        }
        self.unlink(p, cells);
        true
    }

    /// Unlinks `value` if the cell still holds it: whether this call did.
    pub fn forget(&self, value: &Pinned<T>, cells: &Cells<T>) -> bool {
        self.unlink(Arc::as_ptr(&value.0).cast_mut(), cells)
    }

    /// Whether the cell holds a value, evicted or not.
    pub fn occupied(&self) -> bool {
        !self.0.load(Ordering::Acquire).is_null()
    }

    /// Unlinks `p` if the cell still holds it, and lets the cell's reference go once no step can
    /// still read it.
    fn unlink(&self, p: *mut T, cells: &Cells<T>) -> bool {
        if self
            .0
            .compare_exchange(p, ptr::null_mut(), Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        cells.count.fetch_sub(1, Ordering::AcqRel);
        // SAFETY: the cell's reference, which the exchange handed over.
        cells
            .retired
            .retire(&cells.epochs, unsafe { Arc::from_raw(p) });
        drop(cells.retired.reclaim(&cells.epochs));
        true
    }
}

impl<T> Drop for PinCell<T> {
    fn drop(&mut self) {
        if cfg!(atoll_loom) && std::thread::panicking() {
            return;
        }
        let p = self.0.load(Ordering::Acquire);
        if !p.is_null() {
            // SAFETY: the cell's reference, and nothing reads the cell any more.
            drop(unsafe { Arc::from_raw(p) });
        }
    }
}

#[cfg(test)]
struct Counted {
    pins: Pins,
    id: u64,
    live: Arc<sync::AtomicUsize>,
}

#[cfg(test)]
impl Counted {
    fn new(id: u64, live: &Arc<sync::AtomicUsize>) -> Self {
        live.fetch_add(1, Ordering::AcqRel);
        Self {
            pins: Pins::default(),
            id,
            live: live.clone(),
        }
    }
}

#[cfg(test)]
impl Drop for Counted {
    fn drop(&mut self) {
        if !(cfg!(atoll_loom) && std::thread::panicking()) {
            self.live.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
impl Pinnable for Counted {
    fn pins(&self) -> &Pins {
        &self.pins
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;

    /// Threads find values in a few cells, putting one where none is, and evict them, while the
    /// value each holds stays the one its cell holds: a value pinned is never evicted, and every
    /// value is let go once its cells are dropped.
    fn a_pinned_value_stays_in_its_cell(threads: u64, rounds: u64) {
        let live = Arc::new(sync::AtomicUsize::new(0));
        {
            let cells = Arc::new(Cells::new(Arc::new(Epochs::new())));
            let slots: Arc<Vec<PinCell<Counted>>> =
                Arc::new((0..3).map(|_| PinCell::default()).collect());
            let handles: Vec<_> = (0..threads)
                .map(|t| {
                    let (cells, slots, live) = (cells.clone(), slots.clone(), live.clone());
                    crate::thread::spawn(move || {
                        for round in 0..rounds {
                            let cell = &slots[((t + round) % 3) as usize];
                            let held = {
                                let step = cells.epochs().enter();
                                match cell.get(&step, &cells) {
                                    Some(v) => v,
                                    None => {
                                        let fresh =
                                            Pinned::new(Counted::new(t << 32 | round, &live));
                                        if !cell.put(&fresh, &cells) {
                                            continue;
                                        }
                                        fresh
                                    }
                                }
                            };
                            let other = &slots[((t + round + 1) % 3) as usize];
                            {
                                let step = cells.epochs().enter();
                                let _ = other.evict(&step, &cells);
                            }
                            crate::thread::yield_now();
                            let step = cells.epochs().enter();
                            let now = cell.get(&step, &cells).expect("a pinned value evicted");
                            assert_eq!(now.id, held.id, "a pinned value left its cell");
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        }
        assert_eq!(live.load(Ordering::Acquire), 0, "a value was never let go");
    }

    #[cfg(not(shuttle))]
    #[test]
    fn a_pinned_value_stays_in_its_cell_on_threads() {
        a_pinned_value_stays_in_its_cell(3, if cfg!(miri) { 300 } else { 50_000 });
    }

    #[cfg(shuttle)]
    #[test]
    fn a_pinned_value_stays_in_its_cell_in_shuttle_schedules() {
        crate::storage::atoll::testing::schedules(|| a_pinned_value_stays_in_its_cell(2, 4), 2_000);
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    fn check(f: impl Fn() + Sync + Send + 'static) {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(f);
    }

    /// A value is found and pinned as another thread evicts it: either the pin holds it in its
    /// cell, or it is gone and the finder puts a new one; it is never both pinned and gone.
    #[test]
    fn a_value_pinned_as_it_is_evicted_stays_or_goes_whole() {
        check(|| {
            let live = Arc::new(sync::AtomicUsize::new(0));
            {
                let cells = Arc::new(Cells::new(Arc::new(Epochs::new())));
                let cell = Arc::new(PinCell::default());
                assert!(cell.put(&Pinned::new(Counted::new(1, &live)), &cells));
                let evictor = {
                    let (cells, cell) = (cells.clone(), cell.clone());
                    thread::spawn(move || {
                        let step = cells.epochs().enter();
                        cell.evict(&step, &cells)
                    })
                };
                let held = {
                    let step = cells.epochs().enter();
                    cell.get(&step, &cells)
                };
                let evicted = evictor.join().unwrap();
                match held {
                    Some(v) => {
                        assert!(!evicted, "a pinned value was evicted");
                        assert_eq!(v.id, 1);
                        assert!(cell.occupied());
                    }
                    None => assert!(evicted, "a value went that no one evicted"),
                }
                assert_eq!(cells.count(), cell.occupied() as usize);
            }
            assert_eq!(live.load(Ordering::Acquire), 0, "a value was never let go");
        });
    }

    /// A value forgotten by its holder as another thread evicts it, once its holder lets it go:
    /// it leaves its cell once, and is let go once.
    #[test]
    fn a_value_forgotten_as_it_is_evicted_goes_once() {
        check(|| {
            let live = Arc::new(sync::AtomicUsize::new(0));
            {
                let cells = Arc::new(Cells::new(Arc::new(Epochs::new())));
                let cell = Arc::new(PinCell::default());
                let held = Pinned::new(Counted::new(1, &live));
                assert!(cell.put(&held, &cells));
                let evictor = {
                    let (cells, cell) = (cells.clone(), cell.clone());
                    thread::spawn(move || {
                        let step = cells.epochs().enter();
                        let _ = cell.evict(&step, &cells);
                    })
                };
                let _ = cell.forget(&held, &cells);
                drop(held);
                evictor.join().unwrap();
                assert_eq!(cells.count(), cell.occupied() as usize);
            }
            assert_eq!(
                live.load(Ordering::Acquire),
                0,
                "a value was let go twice or never"
            );
        });
    }
}
