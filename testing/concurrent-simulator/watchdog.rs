//! Atoll mode's watchdog (SHORT-HOLD1): a thread that, once one of the simulator's steps has not
//! returned in two minutes, prints the park report of every Atoll database the run opened to
//! stderr, and again every two minutes while the step still runs, with a line for each fiber: its
//! connection as the pool's registry of keepers names it, its statement's SQL and what its last
//! step returned (SHORT-HOLD1b); and the census of taken frames the run checks at its end.
//!
//! The report is read while the stepping thread waits. A wait on memory parks at the operation's
//! top holding no Step and no change gate (Atoll's `park.rs` asserts both at every park), and no
//! Atoll mutex is held across IO (Atoll's CONCURRENCY.md §8), so the report takes nothing the
//! parked thread holds: it reads the buffer pool's atomics and takes the shortage queue's mutex
//! alone, as any of its callers does, and is built whole before it is printed.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use turso_core::Database;

/// How long a step runs before its first report, and how long between reports while it runs on.
const STUCK: Duration = Duration::from_secs(120);

/// How often the watchdog looks at the step under way.
const LOOK: Duration = Duration::from_secs(1);

pub(crate) struct Watchdog {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

struct Shared {
    seed: u64,
    start: Instant,
    /// When the step under way began, in milliseconds since `start`, plus one: 0 between steps.
    began: AtomicU64,
    /// Steps begun, which tells one step from the next.
    steps: AtomicU64,
    stop: AtomicBool,
    /// The databases the run opened, held weakly and upgraded only under the lock, which the
    /// stepping thread takes to forget them before it drops them: the last reference to a
    /// database always goes on the stepping thread.
    databases: Mutex<Vec<Weak<Database>>>,
    /// The fiber whose step is under way, plus one: 0 for none, or a step of no fiber's.
    fiber: AtomicUsize,
    /// What each fiber's last step was, by fiber, written by the stepping thread after the step.
    fibers: Mutex<Vec<Option<FiberLine>>>,
}

/// A fiber's last step: its connection as the registry of keepers names it, its statement's SQL
/// and what the step returned.
struct FiberLine {
    connection: Option<usize>,
    sql: String,
    returned: String,
}

/// A step under way, for as long as it lives.
pub(crate) struct Watched(Arc<Shared>);

impl Drop for Watched {
    fn drop(&mut self) {
        self.0.began.store(0, Ordering::Release);
    }
}

impl Watchdog {
    pub(crate) fn start(seed: u64) -> Self {
        let shared = Arc::new(Shared {
            seed,
            start: Instant::now(),
            began: AtomicU64::new(0),
            steps: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            databases: Mutex::new(Vec::new()),
            fiber: AtomicUsize::new(0),
            fibers: Mutex::new(Vec::new()),
        });
        let watched = shared.clone();
        let thread = std::thread::Builder::new()
            .name("whopper-watchdog".to_string())
            .spawn(move || watch(&watched))
            .expect("the watchdog's thread starts");
        Self {
            shared,
            thread: Some(thread),
        }
    }

    /// A step begins, of `fiber`'s if it is one: it is watched until the returned value drops.
    pub(crate) fn watch(&self, fiber: Option<usize>) -> Watched {
        self.shared.steps.fetch_add(1, Ordering::AcqRel);
        self.shared
            .fiber
            .store(fiber.map_or(0, |f| f + 1), Ordering::Release);
        let at = self.shared.start.elapsed().as_millis() as u64;
        self.shared.began.store(at + 1, Ordering::Release);
        Watched(self.shared.clone())
    }

    /// Fiber `fiber` stepped its statement, of SQL `sql`, on the connection the registry of keepers
    /// names `connection`, and the step returned `returned`.
    pub(crate) fn stepped(
        &self,
        fiber: usize,
        connection: Option<usize>,
        sql: &str,
        returned: String,
    ) {
        let mut fibers = self.shared.fibers();
        if fibers.len() <= fiber {
            fibers.resize_with(fiber + 1, || None);
        }
        fibers[fiber] = Some(FiberLine {
            connection,
            sql: sql.to_string(),
            returned,
        });
    }

    /// The run opened `db`, or opened it again through the database registry.
    pub(crate) fn opened(&self, db: &Arc<Database>) {
        let mut databases = self.shared.databases();
        databases.retain(|d| d.strong_count() > 0);
        if !databases.iter().any(|d| d.as_ptr() == Arc::as_ptr(db)) {
            databases.push(Arc::downgrade(db));
        }
    }

    /// The run is about to drop its databases, to open them again.
    pub(crate) fn closing(&self) {
        self.shared.databases().clear();
    }

    /// The open databases the census read, and every taken frame of their buffer pools in no
    /// place or in more than one.
    pub(crate) fn taken_frames_census(&self) -> (usize, Vec<String>) {
        let databases = self.shared.databases();
        let (mut read, mut out) = (0, Vec::new());
        for (i, db) in databases.iter().enumerate() {
            if let Some(db) = db.upgrade() {
                read += 1;
                out.extend(
                    db.atoll_taken_frames_census()
                        .into_iter()
                        .map(|line| format!("database {i}: {line}")),
                );
            }
        }
        (read, out)
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

/// The watchdog's thread: looks once a second, and prints the report of a step running for
/// [`STUCK`] and every [`STUCK`] after while it runs on.
fn watch(shared: &Shared) {
    let mut reported: Option<(u64, Instant)> = None;
    while !shared.stop.load(Ordering::Acquire) {
        std::thread::park_timeout(LOOK);
        let began = shared.began.load(Ordering::Acquire);
        if began == 0 {
            reported = None;
            continue;
        }
        let step = shared.steps.load(Ordering::Acquire);
        let running = shared
            .start
            .elapsed()
            .saturating_sub(Duration::from_millis(began - 1));
        let due = match reported {
            Some((last, at)) if last == step => at.elapsed() >= STUCK,
            _ => running >= STUCK,
        };
        if due {
            let report = shared.report(step, running);
            eprintln!("{report}");
            reported = Some((step, Instant::now()));
        }
    }
}

impl Shared {
    fn databases(&self) -> std::sync::MutexGuard<'_, Vec<Weak<Database>>> {
        self.databases
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn fibers(&self) -> std::sync::MutexGuard<'_, Vec<Option<FiberLine>>> {
        self.fibers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The report of every open database, built before anything is printed.
    fn report(&self, step: u64, running: Duration) -> String {
        let mut out = format!(
            "whopper watchdog (seed {}): step {step} has not returned in {} s\n",
            self.seed,
            running.as_secs()
        );
        let databases = self.databases();
        for (i, db) in databases.iter().enumerate() {
            let report = match db.upgrade() {
                Some(db) => db
                    .atoll_park_report()
                    .unwrap_or_else(|| "not in Atoll mode\n".to_string()),
                None => "closed\n".to_string(),
            };
            out.push_str(&format!("database {i}:\n{report}"));
        }
        drop(databases);
        let under_way = self.fiber.load(Ordering::Acquire).checked_sub(1);
        out.push_str(&format!(
            "fibers, the step under way {}:\n",
            under_way.map_or("of none".to_string(), |f| format!("fiber {f}'s"))
        ));
        for (i, line) in self.fibers().iter().enumerate() {
            match line {
                Some(line) => out.push_str(&format!(
                    "  fiber {i}: connection {}, last step returned {}, sql {:?}\n",
                    line.connection
                        .map_or("not in Atoll mode".to_string(), |c| format!("{c:#x}")),
                    line.returned,
                    line.sql
                )),
                None => out.push_str(&format!("  fiber {i}: no step yet\n")),
            }
        }
        out
    }
}
