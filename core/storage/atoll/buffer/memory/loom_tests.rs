//! The allocator under loom: every interleaving and C11 reordering of takes, frees, drains and
//! reservations racing on a few batches.
//!
//! Run with `RUSTFLAGS="--cfg atoll_loom" cargo test -p turso_core --lib --release memory::loom`.

use super::*;
use loom::sync::Arc;
use loom::thread;

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

/// 2 MiB frames: one per batch.
fn big() -> SizeClass {
    class(BATCH_CLASS)
}

fn check(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

/// The smallest pool: a run of four batches and one more.
fn memory() -> Arc<Memory> {
    Arc::new(Memory::new(MIN_BUDGET).unwrap())
}

/// Two takes race for a batch's one free frame: one gets it, once.
#[test]
fn two_takes_of_one_free_frame_get_it_once() {
    check(|| {
        let m = memory();
        assert!(m.grow(big()));
        let takers: Vec<_> = (0..2)
            .map(|_| {
                let m = m.clone();
                thread::spawn(move || m.take(big()))
            })
            .collect();
        let got: Vec<_> = takers.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(got.iter().flatten().count(), 1, "{got:?}");
    });
}

/// A frame freed while a take finds its batch full: the take gets it, or the batch is marked as
/// having it, so the next take does.
#[test]
fn a_frame_freed_while_a_take_finds_its_batch_full_is_found() {
    check(|| {
        let m = memory();
        assert!(m.grow(big()));
        let frame = m.take(big()).unwrap();
        let freer = {
            let m = m.clone();
            thread::spawn(move || {
                m.put(big(), frame);
            })
        };
        let raced = m.take(big());
        freer.join().unwrap();
        if raced.is_none() {
            assert_eq!(
                m.take(big()),
                Some(frame),
                "the freed frame is lost to takes"
            );
        }
    });
}

/// A batch's last frames freed while it is drained for another class: it is released once, and
/// goes to that class once.
#[test]
fn a_batch_emptied_as_it_drains_is_released_once() {
    check(|| {
        let m = memory();
        assert!(m.grow(class(0)));
        let frames = [m.take(class(0)).unwrap(), m.take(class(0)).unwrap()];
        let batch = frames[0] / GRAINS;
        let freers: Vec<_> = frames
            .into_iter()
            .map(|f| {
                let m = m.clone();
                thread::spawn(move || m.put(class(0), f))
            })
            .collect();
        m.drain_for(class(0), batch, class(1));
        let released = freers
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(|&r| r)
            .count();
        assert!(released <= 1, "released by {released} frees");
        assert_eq!(m.held()[0], 0);
        assert_eq!(
            m.held()[1],
            BATCH,
            "the batch went to its beneficiary {} times",
            m.held()[1] / BATCH
        );
    });
}

/// The pool's five batches each hold a 2 MiB frame; batch 0's is freed as the run of two at 0 is
/// reserved for 4 MiB frames: counted to the run once, which goes whole to that class once batch
/// 1's frame is back too.
#[test]
fn a_batch_freed_as_its_run_is_reserved_counts_once() {
    check(|| {
        let m = memory();
        let mut frames = [0; 5];
        for f in &mut frames {
            assert!(m.grow(big()));
            *f = m.take(big()).unwrap();
        }
        frames.sort_unstable();
        assert!(m.drain(big(), 0));
        let freer = {
            let m = m.clone();
            thread::spawn(move || {
                m.put(big(), frames[0]);
            })
        };
        assert!(m.drain_run(class(BATCH_CLASS + 1), 0));
        freer.join().unwrap();
        m.put(big(), frames[1]);
        assert_eq!(m.held()[BATCH_CLASS + 1], 2 * BATCH, "{}", m.describe());
        assert_eq!(m.held()[BATCH_CLASS], 3 * BATCH);
    });
}

/// The one free batch lies in a run being reserved, and a class grows into it meanwhile: either
/// the reservation drains it or the growth sees the reservation, so the run still comes whole.
#[test]
fn a_batch_lent_as_its_run_is_reserved_is_drained_into_it() {
    check(|| {
        let m = memory();
        let mut frames = [0; 5];
        for f in &mut frames {
            assert!(m.grow(big()));
            *f = m.take(big()).unwrap();
        }
        frames.sort_unstable();
        // Batch 1 back to the free runs.
        assert!(m.drain(big(), 1));
        assert!(m.put(big(), frames[1]));
        let grower = {
            let m = m.clone();
            thread::spawn(move || m.grow(big()))
        };
        assert!(m.drain_run(class(BATCH_CLASS + 1), 0));
        grower.join().unwrap();
        m.put(big(), frames[0]);
        assert_eq!(m.held()[BATCH_CLASS + 1], 2 * BATCH, "{}", m.describe());
        assert_eq!(m.held()[BATCH_CLASS], 3 * BATCH);
    });
}

/// A run handed to its class is drained again before its reservation is cleared: its batches go
/// back to the free runs, not to the reservation they already completed.
#[test]
fn a_run_drained_as_its_reservation_completes_goes_back_free() {
    check(|| {
        let m = memory();
        let class = class(BATCH_CLASS + 1);
        let drainer = {
            let m = m.clone();
            thread::spawn(move || m.drain(class, 0))
        };
        assert!(m.drain_run(class, 0));
        drainer.join().unwrap();
        let held: usize = m.held().iter().sum();
        let free = m.unowned.ones().count() * BATCH;
        assert_eq!(held + free, m.batches as usize * BATCH, "{}", m.describe());
    });
}
