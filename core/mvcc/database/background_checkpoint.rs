//! The auto passive checkpoint on its own thread and connection, so no commit pays for one.
//!
//! A commit that leaves the logical log past the checkpoint threshold hands the checkpoint here
//! and returns. The thread holds the database only by a weak reference and opens a connection for
//! each checkpoint, so it neither keeps a closed database alive nor counts as an open connection
//! between checkpoints. It stops when the store that owns it is dropped.
//!
//! A passive checkpoint truncates the log only when no commit landed after its snapshot, which a
//! checkpoint running beside writers rarely sees. So once a checkpoint has had to keep the log,
//! the thread checkpoints again as soon as writes pause, and that one truncates it.
//!
//! An encrypted database, or one read through an external page codec, keeps its inline checkpoint:
//! the key material lives only on the connections given it, and the thread would have to hold it
//! for the database's lifetime.
use crate::storage::wal::CheckpointMode;
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync::{Arc, Weak};
use crate::{ConnectOpts, Connection, Database};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// How long the thread rests after a failed checkpoint before it takes the next request.
const RETRY_PAUSE: Duration = Duration::from_millis(10);

/// How long the log must stay the same size before a kept log is checkpointed to truncate it.
const QUIET: Duration = Duration::from_millis(100);

static CHECKPOINTS: AtomicU64 = AtomicU64::new(0);
static FAILURES: AtomicU64 = AtomicU64::new(0);
static MICROS: AtomicU64 = AtomicU64::new(0);
static MICROS_MAX: AtomicU64 = AtomicU64::new(0);

/// Process-wide background checkpoint counts: (checkpoints, failures, total µs, longest µs).
pub fn background_checkpoint_stats() -> (u64, u64, u64, u64) {
    (
        CHECKPOINTS.load(Ordering::Relaxed),
        FAILURES.load(Ordering::Relaxed),
        MICROS.load(Ordering::Relaxed),
        MICROS_MAX.load(Ordering::Relaxed),
    )
}

pub(crate) struct BackgroundCheckpoint {
    signal: Arc<Signal>,
}

struct Signal {
    enabled: AtomicBool,
    started: AtomicBool,
    /// Whether the thread is running; cleared as it exits.
    running: AtomicBool,
    /// The checkpoints this store's thread completed.
    checkpoints: AtomicU64,
    state: Mutex<State>,
    wake: Condvar,
}

/// A view of one store's checkpoint thread that outlives the store, for tests.
#[cfg(test)]
pub(crate) struct BackgroundCheckpointProbe {
    signal: Arc<Signal>,
}

#[cfg(test)]
impl BackgroundCheckpointProbe {
    pub(crate) fn checkpoints(&self) -> u64 {
        self.signal.checkpoints.load(Ordering::Acquire)
    }

    pub(crate) fn running(&self) -> bool {
        self.signal.running.load(Ordering::Acquire)
    }
}

#[derive(Default)]
struct State {
    wanted: bool,
    stopped: bool,
}

impl BackgroundCheckpoint {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            signal: Arc::new(Signal {
                enabled: AtomicBool::new(enabled),
                started: AtomicBool::new(false),
                running: AtomicBool::new(false),
                checkpoints: AtomicU64::new(0),
                state: Mutex::new(State::default()),
                wake: Condvar::new(),
            }),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.signal.enabled.load(Ordering::Acquire)
    }

    /// Off, commits past the threshold checkpoint inline again.
    pub(crate) fn set_enabled(&self, enabled: bool) {
        self.signal.enabled.store(enabled, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn probe(&self) -> BackgroundCheckpointProbe {
        BackgroundCheckpointProbe {
            signal: self.signal.clone(),
        }
    }

    /// Hands the checkpoint of `connection`'s database to the thread, starting it on first use.
    /// False when the thread is off or cannot start, or the connection holds key material, and
    /// the commit checkpoints inline.
    pub(crate) fn request(&self, connection: &Arc<Connection>) -> bool {
        if !self.enabled()
            || connection.encryption_key.read().is_some()
            || connection.pager.load().has_external_page_codec()
        {
            return false;
        }
        let db = &connection.db;
        if !self.signal.started.swap(true, Ordering::AcqRel) {
            self.signal.running.store(true, Ordering::Release);
            let (signal, db) = (self.signal.clone(), Arc::downgrade(db));
            let spawned = std::thread::Builder::new()
                .name("turso-mvcc-checkpoint".into())
                .spawn(move || run(db, signal));
            if let Err(err) = spawned {
                tracing::warn!("MVCC background checkpoint thread did not start: {err}");
                self.signal.running.store(false, Ordering::Release);
                self.signal.started.store(false, Ordering::Release);
                return false;
            }
        }
        let mut state = self.signal.state.lock().expect("checkpoint signal poisoned");
        state.wanted = true;
        self.signal.wake.notify_one();
        true
    }
}

impl std::fmt::Debug for BackgroundCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackgroundCheckpoint")
            .field("enabled", &self.enabled())
            .field("started", &self.signal.started.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for BackgroundCheckpoint {
    fn drop(&mut self) {
        let mut state = self.signal.state.lock().expect("checkpoint signal poisoned");
        state.stopped = true;
        self.signal.wake.notify_one();
    }
}

fn run(db: Weak<Database>, signal: Arc<Signal>) {
    serve(db, &signal);
    signal.running.store(false, Ordering::Release);
}

fn serve(db: Weak<Database>, signal: &Signal) {
    // The log's offset after a checkpoint that could not truncate it, or at the last look since.
    let mut kept: Option<u64> = None;
    loop {
        let requested = {
            let mut state = signal.state.lock().expect("checkpoint signal poisoned");
            loop {
                if state.stopped {
                    return;
                }
                if state.wanted {
                    state.wanted = false;
                    break true;
                }
                if kept.is_none() {
                    state = signal.wake.wait(state).expect("checkpoint signal poisoned");
                    continue;
                }
                let (next, waited) = signal
                    .wake
                    .wait_timeout(state, QUIET)
                    .expect("checkpoint signal poisoned");
                state = next;
                if waited.timed_out() && !state.wanted && !state.stopped {
                    break false;
                }
            }
        };
        let Some(db) = db.upgrade() else {
            return;
        };
        if !requested {
            let offset = log_offset(&db);
            if kept != Some(offset) {
                // Commits landed since the last look; wait for the next quiet stretch.
                kept = Some(offset);
                continue;
            }
        }
        let checkpointed = checkpoint(&db, signal);
        let offset = log_offset(&db);
        kept = (offset > 0).then_some(offset);
        if !checkpointed {
            std::thread::sleep(RETRY_PAUSE);
        }
    }
}

fn log_offset(db: &Arc<Database>) -> u64 {
    db.get_mv_store()
        .as_ref()
        .map_or(0, |store| store.storage.logical_log_offset())
}

/// Checkpoints until the log's uncheckpointed bytes are back under the threshold, since commits
/// that land during a checkpoint can leave them over again. False when a checkpoint failed.
fn checkpoint(db: &Arc<Database>, signal: &Signal) -> bool {
    let conn = match db.connect(ConnectOpts::new()) {
        Ok(conn) => conn,
        Err(err) => {
            tracing::info!("MVCC background checkpoint could not connect: {err}");
            return false;
        }
    };
    loop {
        let started = std::time::Instant::now();
        let result = conn.checkpoint(CheckpointMode::Passive {
            upper_bound_inclusive: None,
        });
        let micros = started.elapsed().as_micros() as u64;
        MICROS.fetch_add(micros, Ordering::Relaxed);
        MICROS_MAX.fetch_max(micros, Ordering::Relaxed);
        if let Err(err) = result {
            FAILURES.fetch_add(1, Ordering::Relaxed);
            tracing::info!("MVCC background checkpoint failed: {err}");
            return false;
        }
        CHECKPOINTS.fetch_add(1, Ordering::Relaxed);
        signal.checkpoints.fetch_add(1, Ordering::Release);
        let over = db
            .get_mv_store()
            .as_ref()
            .is_some_and(|store| store.storage.should_checkpoint());
        if !over {
            return true;
        }
    }
}
