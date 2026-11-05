//! What every running snapshot sees (audit 18): a checkpoint's image of a versioned leaf stands for
//! the leaf only where the oldest snapshot running reads the two alike.
//!
//! Each slot publishes a timestamp at or below the snapshot its transaction reads: 0 while it takes
//! one, then the snapshot; a read-committed statement's later snapshot replaces it; [`NONE`] once
//! the slot is free. The 0 is stored, then a SeqCst fence, then the horizon read; a reader of the
//! slots reads the horizon, then a SeqCst fence, then the slots ([`oldest`]). If the reader's fence
//! comes first it finds the 0 or the snapshot; if the taker's does, the taker reads a horizon at or
//! above the one the reader read, its value coherence-ordered after.

use super::sync::{fence, AtomicU64, Ordering};
use super::Ts;

/// What a slot with no snapshot publishes.
pub(super) const NONE: Ts = Ts::MAX;

/// Takes a snapshot, the horizon `read`, published in `published` before and after.
pub(super) fn take(published: &AtomicU64, read: impl FnOnce() -> Ts) -> Ts {
    published.store(0, Ordering::Release);
    fence(Ordering::SeqCst);
    let snapshot = read();
    published.store(snapshot, Ordering::Release);
    snapshot
}

/// Publishes a later snapshot of the slot's transaction, at or above the one published.
pub(super) fn rise(published: &AtomicU64, snapshot: Ts) {
    published.store(snapshot, Ordering::Release);
}

/// The slot's transaction has ended.
pub(super) fn vacate(published: &AtomicU64) {
    published.store(NONE, Ordering::Release);
}

/// At or below every snapshot running now or taken from now on, the horizon read at or above `at`
/// before: the oldest published, `at` where none is older.
pub(super) fn oldest<'a>(published: impl Iterator<Item = &'a AtomicU64>, at: Ts) -> Ts {
    fence(Ordering::SeqCst);
    published
        .map(|p| p.load(Ordering::Acquire))
        .fold(at, Ts::min)
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::super::horizon::Horizon;
    use super::super::Clock;
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// A transaction takes its snapshot while another commits and a checkpoint reads the horizon,
    /// then the oldest snapshot: a snapshot below the horizon the checkpoint read is at or above the
    /// oldest. The checkpoint only reads, as a checkpoint's cut does.
    #[test]
    fn a_snapshot_below_the_horizon_read_first_is_never_missed() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let clock = Arc::new(Clock::after(0));
            let horizon = Arc::new(Horizon::new(1, 0));
            let published = Arc::new(AtomicU64::new(NONE));
            let taker = {
                let (horizon, published) = (horizon.clone(), published.clone());
                thread::spawn(move || take(&published, || horizon.at()))
            };
            let committer = {
                let (clock, horizon) = (clock.clone(), horizon.clone());
                thread::spawn(move || {
                    horizon.draw(0, &clock);
                    horizon.published(0, &clock);
                })
            };
            let at = horizon.at();
            let oldest = oldest(std::iter::once(&*published), at);
            let snapshot = taker.join().unwrap();
            committer.join().unwrap();
            assert!(
                snapshot >= at || oldest <= snapshot,
                "snapshot {snapshot} below the horizon {at}, the oldest read {oldest}"
            );
        });
    }
}
