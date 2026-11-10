//! The epochs under loom, which explores the reorderings the C11 memory model allows as well as
//! the interleavings: a missing fence or too weak an ordering fails here, where shuttle, which
//! treats every access as sequentially consistent, and x86 stress cannot fail.
//!
//! An object is plain memory, read by steps and written when it is destroyed: loom fails any read
//! and write of it not ordered by happens-before, which is the order reusing a frame's memory or
//! a page number relies on. Everything else the tests share is relaxed, so that order comes from
//! the epochs alone.
//!
//! Run with `RUSTFLAGS="--cfg atoll_loom" cargo test -p turso_core --lib --release epoch::loom`.

use super::*;
use loom::cell::UnsafeCell;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;

/// A notifier loom sees: a flag set with release, read with acquire.
#[derive(Clone)]
struct Flag(Arc<AtomicBool>);

impl Flag {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Spins until notified.
    fn wait(&self) {
        while !self.notified() {
            thread::yield_now();
        }
    }
}

impl Notify for Flag {
    fn notify(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn notified(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Every interleaving and reordering with up to three preemptions.
fn check(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

const ALIVE: u64 = 1;
const DESTROYED: u64 = 0;

/// An object's memory.
struct Object(UnsafeCell<u64>);

// SAFETY: shared to be raced on: loom checks every access.
unsafe impl Sync for Object {}

/// Two objects, the first current.
struct World {
    epochs: Arc<Epochs>,
    current: Arc<AtomicUsize>,
    objects: Arc<[Object; 2]>,
}

impl World {
    fn new() -> Self {
        Self {
            epochs: Arc::new(Epochs::new()),
            current: Arc::new(AtomicUsize::new(0)),
            objects: Arc::new([
                Object(UnsafeCell::new(ALIVE)),
                Object(UnsafeCell::new(ALIVE)),
            ]),
        }
    }

    /// A thread whose step reads the current object.
    fn reader(&self) -> thread::JoinHandle<()> {
        let (epochs, current, objects) = (
            self.epochs.clone(),
            self.current.clone(),
            self.objects.clone(),
        );
        thread::spawn(move || {
            let _step = epochs.enter();
            let held = current.load(Ordering::Relaxed);
            thread::yield_now();
            // SAFETY: loom fails this read if a destroy is not ordered with it.
            let value = objects[held].0.with(|v| unsafe { *v });
            assert_eq!(value, ALIVE, "object {held} destroyed while a step held it");
        })
    }

    /// Makes the second object current: the first is unlinked from here on.
    fn unlink(&self) {
        self.current.store(1, Ordering::Relaxed);
    }

    fn destroy_first(&self) {
        // SAFETY: loom fails this write if a read is not ordered with it.
        self.objects[0].0.with_mut(|v| unsafe { *v = DESTROYED });
    }
}

/// A retired object comes back from a reclaim only once no step that may have read it is left.
#[test]
fn a_reclaimed_object_outlives_every_step_that_read_it() {
    check(|| {
        let world = World::new();
        let reader = world.reader();
        let retired = Retired::default();
        world.unlink();
        retired.retire(&world.epochs, 0usize);
        for object in retired.reclaim(&world.epochs) {
            assert_eq!(object, 0);
            world.destroy_first();
        }
        reader.join().unwrap();
    });
}

/// A horizon one thread's scan found lets another reuse what is stamped at or below it without
/// a scan of its own.
#[test]
fn an_object_reused_by_a_known_horizon_outlives_every_step_that_read_it() {
    check(|| {
        let world = World::new();
        let reader = world.reader();
        let scanner = {
            let epochs = world.epochs.clone();
            thread::spawn(move || {
                epochs.horizon();
            })
        };
        world.unlink();
        let stamp = world.epochs.stamp();
        if stamp <= world.epochs.known_horizon() {
            world.destroy_first();
        }
        reader.join().unwrap();
        scanner.join().unwrap();
    });
}

/// A wait for an object's stamp is notified, and the object destroyed then, only once no step
/// that may have read it is left; and it is always notified.
#[test]
fn a_wait_ends_with_the_step_holding_it_back_and_never_sooner() {
    check(|| {
        let world = World::new();
        let reader = world.reader();
        world.unlink();
        let stamp = world.epochs.stamp();
        let flag = Flag::new();
        if world.epochs.wait_for(stamp, flag.clone()) {
            flag.wait();
        }
        world.destroy_first();
        reader.join().unwrap();
    });
}

/// Two steps held back by one wait end at once: one's wake may take the list while the other's
/// wake has it, and the wait must still be notified, after both steps' reads.
#[test]
fn a_wait_is_notified_when_two_steps_holding_it_end_at_once() {
    check(|| {
        let world = World::new();
        let readers = [world.reader(), world.reader()];
        world.unlink();
        let flag = Flag::new();
        if world.epochs.wait_for(world.epochs.stamp(), flag.clone()) {
            flag.wait();
        }
        world.destroy_first();
        for r in readers {
            r.join().unwrap();
        }
    });
}

/// Two waits pushed while one step holds them back: once the step has ended, both are notified,
/// what each waiter wrote of its wait having reached the waking thread through the list alone.
#[test]
fn two_waits_held_back_by_one_step_are_both_notified() {
    check(|| {
        let world = World::new();
        let reader = world.reader();
        world.unlink();
        let stamp = world.epochs.stamp();
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let epochs = world.epochs.clone();
                thread::spawn(move || {
                    let flag = Flag::new();
                    let waiting = epochs.wait_for(stamp, flag.clone());
                    (flag, waiting)
                })
            })
            .collect();
        let waits: Vec<_> = waiters.into_iter().map(|w| w.join().unwrap()).collect();
        reader.join().unwrap();
        for (flag, waiting) in waits {
            assert!(
                !waiting || flag.notified(),
                "a wait outlived the step holding it back"
            );
        }
    });
}

/// Three threads join at once, two slots to a chunk: each takes a slot of its own, the third in
/// a second chunk one of them links, and a scan reads all three announcements.
#[test]
fn threads_joining_at_once_take_slots_of_their_own_across_chunks() {
    check(|| {
        let epochs = Arc::new(Epochs::new());
        let threads: Vec<_> = (0..3)
            .map(|_| {
                let epochs = epochs.clone();
                thread::spawn(move || {
                    let slot = epochs.registry.join().slot;
                    slot.word.store(announcing(1), Ordering::Release);
                    slot as *const Slot as usize
                })
            })
            .collect();
        let mut slots: Vec<usize> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        slots.sort_unstable();
        slots.dedup();
        assert_eq!(slots.len(), 3, "threads share a slot");
        assert_eq!(epochs.participants(), 3);
        let announced = epochs
            .registry
            .slots()
            .filter(|s| announced(s.word.load(Ordering::Relaxed)).is_some())
            .count();
        assert_eq!(announced, 3, "a scan misses a slot");
    });
}

/// Objects retired and reclaimed from two threads at once all come back, each exactly once.
#[test]
fn retired_objects_come_back_exactly_once() {
    check(|| {
        let epochs = Arc::new(Epochs::new());
        let retired = Arc::new(Retired::default());
        let threads: Vec<_> = (0..2usize)
            .map(|t| {
                let (epochs, retired) = (epochs.clone(), retired.clone());
                thread::spawn(move || {
                    retired.retire(&epochs, t);
                    retired.reclaim(&epochs)
                })
            })
            .collect();
        let mut back: Vec<usize> = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        // No step is left: a scan's worth of reclaims gets the scan that finds them all.
        for _ in 0..epochs.participants().max(1) {
            back.extend(retired.reclaim(&epochs));
        }
        back.sort_unstable();
        assert_eq!(back, vec![0, 1], "objects lost or come back twice");
    });
}
