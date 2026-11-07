//! A registry of floors: positions in the log that holders keep from being passed, a slot each,
//! and the least of them, read by a scan; a floor may carry a tag, and a scan finds the tags of
//! the floors below a bound. Lock-free: a slot is claimed from a free list, or past the last one
//! handed out, and its position set and cleared with stores.
//!
//! A scan reads every slot handed out once. It is not a snapshot, but it misses only a floor set
//! after it read the floor's slot, which a snapshot taken under a lock misses too; a floor held
//! throughout the scan is always read.

use self::sync::{AtomicU64, Ordering};
// The engine's: what the store hands in, under loom too.
use super::epoch::Epochs;
use super::segmented::Segmented;
use super::stack::FreeList;
use crate::sync::Arc;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicU64, Ordering};
}

#[derive(Default)]
struct Slot {
    /// The floor plus one; 0 for none.
    floor: AtomicU64,
    /// Written before the floor, so a scan that reads the floor reads its tag.
    tag: AtomicU64,
}

pub struct Floors {
    slots: Segmented<Slot>,
    /// Slots handed out so far: a scan reads no further.
    used: AtomicU64,
    free: FreeList<u64>,
    epochs: Arc<Epochs>,
}

/// A floor held: kept until it is given back with [`Floors::release`].
#[derive(Debug)]
#[must_use = "a floor is held until released"]
pub struct Floor {
    slot: u64,
}

impl Floors {
    pub fn new(epochs: Arc<Epochs>) -> Self {
        Self {
            slots: Segmented::default(),
            used: AtomicU64::new(0),
            free: FreeList::default(),
            epochs,
        }
    }

    /// Holds `at` until released.
    pub fn hold(&self, at: u64) -> Floor {
        self.hold_tagged(at, 0)
    }

    /// Holds `at`, tagged `tag`, until released.
    pub fn hold_tagged(&self, at: u64, tag: u64) -> Floor {
        let slot = {
            let step = self.epochs.enter();
            self.free.pop(&self.epochs, &step)
        }
        .unwrap_or_else(|| self.used.fetch_add(1, Ordering::AcqRel));
        let s = self.slots.get_or_make(slot);
        // Relaxed: published by the floor's release, read after its acquire.
        s.tag.store(tag, Ordering::Relaxed);
        s.floor.store(at + 1, Ordering::Release);
        Floor { slot }
    }

    pub fn release(&self, floor: Floor) {
        self.slots
            .get_or_make(floor.slot)
            .floor
            .store(0, Ordering::Release);
        self.free.push(floor.slot);
    }

    /// The least floor held, if any.
    pub fn least(&self) -> Option<u64> {
        (0..self.used.load(Ordering::Acquire))
            .filter_map(|i| self.slots.get(i))
            .map(|s| s.floor.load(Ordering::Acquire))
            .filter(|&v| v != 0)
            .min()
            .map(|v| v - 1)
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;

    /// Holders hold and release floors while a scan reads the least: a floor held throughout a
    /// scan is never missed, and none is read once all are released.
    fn a_floor_held_through_a_scan_is_read(threads: u64, rounds: u64) {
        let floors = Arc::new(Floors::new(Arc::new(Epochs::new())));
        // One floor held for good, below every other.
        let kept = floors.hold(5);
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let floors = floors.clone();
                crate::thread::spawn(move || {
                    for round in 0..rounds {
                        let f = floors.hold(10 + t * rounds + round);
                        crate::thread::yield_now();
                        floors.release(f);
                    }
                })
            })
            .collect();
        for _ in 0..rounds {
            assert_eq!(
                floors.least(),
                Some(5),
                "the floor held throughout was missed"
            );
            crate::thread::yield_now();
        }
        for h in handles {
            h.join().unwrap();
        }
        floors.release(kept);
        assert_eq!(floors.least(), None);
    }

    #[cfg(not(shuttle))]
    #[test]
    fn a_floor_held_through_a_scan_is_read_on_threads() {
        a_floor_held_through_a_scan_is_read(3, if cfg!(miri) { 200 } else { 50_000 });
    }

    #[cfg(shuttle)]
    #[test]
    fn a_floor_held_through_a_scan_is_read_in_shuttle_schedules() {
        crate::storage::atoll::testing::schedules(
            || a_floor_held_through_a_scan_is_read(2, 4),
            2_000,
        );
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

    /// A floor held throughout a scan is read while another is held and released beside it.
    #[test]
    fn a_floor_held_through_a_scan_is_read() {
        check(|| {
            let floors = Arc::new(Floors::new(Arc::new(Epochs::new())));
            let kept = floors.hold(5);
            let other = {
                let floors = floors.clone();
                thread::spawn(move || {
                    let f = floors.hold(9);
                    floors.release(f);
                })
            };
            assert_eq!(floors.least(), Some(5));
            other.join().unwrap();
            floors.release(kept);
            assert_eq!(floors.least(), None);
        });
    }

    /// A slot given back is taken again while its old holder lets it go: the new floor stays.
    #[test]
    fn a_slot_taken_again_keeps_its_new_floor() {
        check(|| {
            let floors = Arc::new(Floors::new(Arc::new(Epochs::new())));
            let other = {
                let floors = floors.clone();
                thread::spawn(move || {
                    let f = floors.hold(9);
                    floors.release(f);
                })
            };
            let kept = floors.hold(7);
            other.join().unwrap();
            assert_eq!(floors.least(), Some(7));
            floors.release(kept);
        });
    }
}
