//! The pace of a checkpoint the background pool runs, PostgreSQL's `checkpoint_completion_target`:
//! the checkpoint spreads its writes over its budget, the completion target's share of the time the
//! log is expected to take to grow by a threshold, so that the disk takes them beside the commits'
//! syncs rather than all at once. It stops pacing once the log has grown by a threshold since it
//! began, when the next checkpoint is due, or once something waits for it to end (`unpace`).

use crate::mvcc::persistent_storage::DurableStorage;
use crate::storage::pager::WritePace;
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::{Arc, Mutex};
use crate::Completion;
use std::time::{Duration, Instant};

/// PostgreSQL's `checkpoint_completion_target` default: a checkpoint's writes may take this share
/// of the time until the next checkpoint is due.
pub const DEFAULT_CHECKPOINT_COMPLETION_TARGET: f64 = 0.9;

pub(crate) struct CheckpointPace {
    began: Instant,
    /// The time the checkpoint's writes may take.
    budget: Duration,
    /// The log's appended bytes at which the next checkpoint is due.
    due_at: u64,
    storage: Arc<dyn DurableStorage>,
    unpaced: AtomicBool,
    /// The instant the last hold waits for, until the pool takes it.
    held_until: Mutex<Option<Instant>>,
}

impl CheckpointPace {
    /// The pace of a checkpoint beginning now, whose writes may take `budget`, until `storage`'s
    /// appended bytes reach `due_at`.
    pub(crate) fn new(storage: Arc<dyn DurableStorage>, budget: Duration, due_at: u64) -> Self {
        Self {
            began: Instant::now(),
            budget,
            due_at,
            storage,
            unpaced: AtomicBool::new(false),
            held_until: Mutex::new(None),
        }
    }

    /// The log's appended bytes at which the pace ends.
    pub(crate) fn due_at(&self) -> u64 {
        self.due_at
    }

    /// The instant the checkpoint's last hold waits for, taken once: the pool parks the
    /// checkpoint until then.
    pub(crate) fn take_hold(&self) -> Option<Instant> {
        self.held_until.lock().take()
    }

    /// Ends the pace: the checkpoint finishes without waiting.
    pub(crate) fn unpace(&self) {
        self.unpaced.store(true, Ordering::Release);
    }
}

impl WritePace for CheckpointPace {
    fn hold(&self, progress: f64) -> Option<Completion> {
        if self.unpaced.load(Ordering::Acquire) {
            return None;
        }
        if self.storage.appended_bytes() >= self.due_at {
            self.unpace();
            return None;
        }
        let due = self.began + self.budget.mul_f64(progress.clamp(0.0, 1.0));
        if due <= Instant::now() {
            return None;
        }
        *self.held_until.lock() = Some(due);
        let wait = Completion::new_wait();
        // Were a caller other than the pool to wait on it, the hold would still end on schedule.
        crate::timer::complete_at(due, wait.clone());
        Some(wait)
    }
}
