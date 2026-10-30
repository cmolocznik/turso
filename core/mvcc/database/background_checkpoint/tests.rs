use super::*;
use crate::mvcc::database::checkpoint_state_machine::CheckpointYieldPoint;
use crate::mvcc::database::tests::MvccTestDbNoConn;
use crate::mvcc::persistent_storage::logical_log::LOG_SEGMENT_FILES;
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{FailureInjector, YieldInjector, YieldPoint};
use crate::{Connection, DatabaseOpts, LimboError, MvStore, StepResult};
use parking_lot::{Condvar, Mutex};

/// Waits up to ten seconds for `done`.
fn wait_until(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

/// A pool of the test's own, so that tests running side by side share no thread, whose idle
/// threads stop soon.
fn pool(thread_limit: usize) -> CheckpointPool {
    CheckpointPool::with_timing(
        thread_limit,
        Duration::from_millis(50),
        Duration::from_millis(100),
    )
}

/// A database on a test's pool with the background checkpoint on, a 4 KB checkpoint threshold and
/// a table `t`.
struct TestDb {
    db: MvccTestDbNoConn,
    conn: Arc<Connection>,
    store: Arc<MvStore>,
    probe: BackgroundCheckpointProbe,
}

impl TestDb {
    fn open(pool: &CheckpointPool) -> Self {
        let db = MvccTestDbNoConn::new_with_random_db_passive();
        let conn = db.connect();
        let store = db.get_mvcc_store();
        store.set_checkpoint_pool(pool);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
            .unwrap();
        conn.execute("PRAGMA mvcc_background_checkpoint = 1")
            .unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = 4096")
            .unwrap();
        let probe = store.background_checkpoint_probe();
        Self {
            db,
            conn,
            store,
            probe,
        }
    }

    fn insert(&self, bytes: usize) {
        self.conn
            .execute(format!("INSERT INTO t(v) VALUES (randomblob({bytes}))"))
            .unwrap();
    }

    /// Inserts until the log is past its threshold, which hands a checkpoint to the pool, and
    /// returns how many rows it inserted.
    fn fill(&self) -> i64 {
        for inserted in 0..1000 {
            if self.store.storage.should_checkpoint() {
                return inserted;
            }
            self.insert(256);
        }
        panic!("1000 inserts left the log under its threshold");
    }

    fn log(&self) -> u64 {
        self.store.storage.logical_log_offset()
    }

    fn owes(&self) -> bool {
        owes_checkpoint(self.store.storage.as_ref())
    }

    fn rows(&self) -> i64 {
        let mut stmt = self.conn.prepare("SELECT count(*) FROM t").unwrap();
        let mut rows = None;
        stmt.run_with_row_callback(|row| {
            rows = row.get_values().next().and_then(|value| value.as_int());
            Ok(())
        })
        .unwrap();
        rows.unwrap()
    }

    fn assert_intact(&self) {
        let mut stmt = self.conn.prepare("PRAGMA integrity_check").unwrap();
        let mut result = Vec::new();
        stmt.run_with_row_callback(|row| {
            result.extend(row.get_values().map(|value| value.to_string()));
            Ok(())
        })
        .unwrap();
        assert_eq!(result, ["ok"]);
    }
}

/// Holds the first checkpoint that reaches its snapshot until the test lets it go.
#[derive(Debug, Default)]
struct HoldAfterSnapshot {
    /// Whether a checkpoint is held, and whether the test let it go.
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl HoldAfterSnapshot {
    fn wait_held(&self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self.state.lock();
        while !state.0 {
            if self.changed.wait_until(&mut state, deadline).timed_out() {
                break;
            }
        }
        state.0
    }

    fn release(&self) {
        self.state.lock().1 = true;
        self.changed.notify_all();
    }
}

impl YieldInjector for HoldAfterSnapshot {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CheckpointYieldPoint::AfterCollectTableRows.point() {
            let mut state = self.state.lock();
            if !state.1 {
                state.0 = true;
                self.changed.notify_all();
                while !state.1 {
                    self.changed.wait(&mut state);
                }
            }
        }
        false
    }
}

/// With the background checkpoint on, a commit that crosses the threshold hands the checkpoint to
/// the pool and returns. A commit that lands after that checkpoint's snapshot makes it keep the
/// log, and once writes pause the pool checkpoints again and truncates the log.
#[test]
fn test_background_checkpoint_runs_off_the_commit_and_truncates_when_writes_pause() {
    let pool = pool(1);
    let db = TestDb::open(&pool);
    let hold = Arc::new(HoldAfterSnapshot::default());
    db.probe.set_injector(Some(hold.clone()));
    let mut inserted = db.fill();
    assert!(
        hold.wait_held(),
        "the pool's checkpoint did not reach its snapshot"
    );
    // No commit asks for a checkpoint now, so the pool's next one is its own.
    db.conn
        .execute("PRAGMA mvcc_checkpoint_threshold = 1048576")
        .unwrap();
    db.insert(16);
    inserted += 1;
    let before = db.probe.checkpoints();
    hold.release();
    // The log empties inside the checkpoint, before the pool counts it.
    assert!(
        wait_until(|| db.log() == 0 && db.probe.checkpoints() >= before + 2),
        "{} checkpoints left {} log bytes",
        db.probe.checkpoints(),
        db.log()
    );
    assert!(
        db.probe.checkpoints() >= before + 2,
        "the held checkpoint kept the log and a later one truncated it: {} checkpoints after {before}",
        db.probe.checkpoints()
    );
    assert!(wait_until(|| !db.probe.scheduled()));
    assert_eq!(db.rows(), inserted);
    db.assert_intact();
}

/// A log a checkpoint had to keep is truncated only once its writes pause: while commits go on
/// landing in it, under the threshold, the pool leaves it alone.
#[test]
fn test_a_kept_log_waits_for_its_writes_to_pause() {
    let quiet_for = Duration::from_millis(500);
    let pool = CheckpointPool::with_timing(1, Duration::from_millis(50), quiet_for);
    let db = TestDb::open(&pool);
    let hold = Arc::new(HoldAfterSnapshot::default());
    db.probe.set_injector(Some(hold.clone()));
    db.fill();
    assert!(hold.wait_held());
    db.conn
        .execute("PRAGMA mvcc_checkpoint_threshold = 67108864")
        .unwrap();
    db.insert(16);
    hold.release();
    assert!(wait_until(|| db.probe.checkpoints() == 1));
    // Commits one after another for three quiet periods: the log never stays the same size long
    // enough.
    let writing = Instant::now();
    while writing.elapsed() < 3 * quiet_for {
        db.insert(16);
    }
    assert_eq!(
        db.probe.checkpoints(),
        1,
        "the pool checkpointed a log still being written"
    );
    // The log empties inside the checkpoint, before the pool counts it.
    assert!(wait_until(|| db.log() == 0 && db.probe.checkpoints() >= 2));
    assert_eq!(db.probe.checkpoints(), 2);
}

/// The background checkpoint is off unless turned on: a commit past the threshold checkpoints
/// inline, so the log never holds much more than the threshold, and the pool starts no thread.
#[test]
fn test_background_checkpoint_is_off_by_default() {
    let pool = pool(1);
    let db = MvccTestDbNoConn::new_with_random_db_passive();
    let conn = db.connect();
    let store = db.get_mvcc_store();
    store.set_checkpoint_pool(&pool);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = 4096")
        .unwrap();
    let mut stmt = conn.prepare("PRAGMA mvcc_background_checkpoint").unwrap();
    let mut enabled = None;
    stmt.run_with_row_callback(|row| {
        enabled = row.get_values().next().and_then(|value| value.as_int());
        Ok(())
    })
    .unwrap();
    assert_eq!(enabled, Some(0));
    let probe = store.background_checkpoint_probe();
    for i in 0..200 {
        conn.execute("INSERT INTO t(v) VALUES (randomblob(256))")
            .unwrap();
        assert!(
            store.storage.logical_log_offset() < 8192,
            "insert {i} left {} log bytes",
            store.storage.logical_log_offset()
        );
    }
    assert_eq!(probe.checkpoints(), 0);
    assert!(!probe.scheduled());
    assert_eq!(pool.stats().threads_peak, 0);
}

/// A dropped database leaves its pool: the pool does not keep it alive, and the pool's threads
/// stop once idle.
#[test]
fn test_a_dropped_database_leaves_its_pool_and_the_idle_threads_stop() {
    let pool = pool(1);
    let (probe, database) = {
        let db = TestDb::open(&pool);
        db.fill();
        assert!(wait_until(|| db.probe.checkpoints() > 0));
        (db.probe, Arc::downgrade(&db.db.get_db()))
    };
    assert!(
        wait_until(|| database.upgrade().is_none()),
        "the pool keeps a dropped database alive"
    );
    assert!(wait_until(|| !probe.scheduled()));
    assert!(
        wait_until(|| pool.stats().threads == 0),
        "the pool's threads did not stop: {:?}",
        pool.stats()
    );
}

/// One pool checkpoints many databases on no more threads than its limit, and truncates every
/// log once the writes pause.
#[test]
fn test_one_pool_checkpoints_many_databases_on_its_threads() {
    let pool = pool(2);
    let dbs: Vec<_> = (0..24).map(|_| TestDb::open(&pool)).collect();
    for db in &dbs {
        db.fill();
    }
    assert!(
        wait_until(|| dbs
            .iter()
            .all(|db| db.probe.checkpoints() > 0 && db.log() == 0)),
        "checkpoints {:?}, logs {:?}, {:?}",
        dbs.iter()
            .map(|db| db.probe.checkpoints())
            .collect::<Vec<_>>(),
        dbs.iter().map(TestDb::log).collect::<Vec<_>>(),
        pool.stats()
    );
    assert!(wait_until(|| dbs.iter().all(|db| !db.probe.scheduled())));
    let stats = pool.stats();
    assert!(
        (1..=2).contains(&stats.threads_peak),
        "two threads at most: {stats:?}"
    );
    assert_eq!(stats.failures, 0, "{stats:?}");
    assert!(stats.checkpoints >= 24, "{stats:?}");
}

/// While every thread of a pool is in a turn, the databases that ask for a checkpoint wait in its
/// queue: no thread starts past the limit, a database dropped while it waits leaves the queue at
/// once, and the thread serves the others once its turn ends.
#[test]
fn test_databases_wait_in_the_queue_while_every_thread_is_busy() {
    let pool = pool(1);
    let busy = TestDb::open(&pool);
    let hold = Arc::new(HoldAfterSnapshot::default());
    busy.probe.set_injector(Some(hold.clone()));
    busy.fill();
    assert!(
        hold.wait_held(),
        "the pool's checkpoint did not reach its snapshot"
    );
    let mut waiting: Vec<_> = (0..3).map(|_| TestDb::open(&pool)).collect();
    for db in &waiting {
        db.fill();
    }
    let stats = pool.stats();
    assert_eq!(
        (stats.threads_peak, stats.queued),
        (1, 3),
        "three databases wait for the one thread: {stats:?}"
    );
    let dropped = waiting.remove(0);
    let database = Arc::downgrade(&dropped.db.get_db());
    drop(dropped);
    assert!(database.upgrade().is_none());
    assert_eq!(
        pool.stats().queued,
        2,
        "a dropped database stayed in the queue: {:?}",
        pool.stats()
    );
    hold.release();
    assert!(
        wait_until(|| waiting
            .iter()
            .all(|db| db.probe.checkpoints() > 0 && db.log() == 0)),
        "{:?}",
        pool.stats()
    );
    assert!(wait_until(|| !busy.probe.scheduled() && busy.log() == 0));
    assert_eq!(pool.stats().threads_peak, 1, "{:?}", pool.stats());
}

/// A database dropped while the pool waits to look at a log its checkpoint had to keep takes its
/// timer out of the pool at once.
#[test]
fn test_a_database_dropped_with_a_timer_armed_leaves_the_timers() {
    let pool = CheckpointPool::with_timing(1, Duration::from_millis(50), Duration::from_secs(60));
    let db = TestDb::open(&pool);
    let hold = Arc::new(HoldAfterSnapshot::default());
    db.probe.set_injector(Some(hold.clone()));
    db.fill();
    assert!(hold.wait_held());
    db.conn
        .execute("PRAGMA mvcc_checkpoint_threshold = 67108864")
        .unwrap();
    db.insert(16);
    hold.release();
    assert!(
        wait_until(|| db.probe.checkpoints() == 1 && pool.stats().timed == 1),
        "the kept log armed no timer: {:?}",
        pool.stats()
    );
    drop(db);
    assert_eq!(
        pool.stats().timed,
        0,
        "a dropped database kept its timer: {:?}",
        pool.stats()
    );
}

/// Commits into its database at every checkpoint the pool runs on it, so that each leaves the log
/// past its threshold.
struct WriteDuringCheckpoint {
    conn: Arc<Connection>,
    on: AtomicBool,
}

impl std::fmt::Debug for WriteDuringCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WriteDuringCheckpoint")
    }
}

impl YieldInjector for WriteDuringCheckpoint {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CheckpointYieldPoint::AfterCollectTableRows.point()
            && self.on.load(Ordering::Acquire)
        {
            self.conn
                .execute("INSERT INTO t(v) VALUES (randomblob(8192))")
                .unwrap();
        }
        false
    }
}

/// A database still past its threshold after a slice of checkpoints gives the pool's thread to
/// another database waiting for it.
#[test]
fn test_a_database_past_its_threshold_after_a_slice_gives_way_to_another() {
    let pool = pool(1);
    let hot = TestDb::open(&pool);
    let writer = Arc::new(WriteDuringCheckpoint {
        conn: hot.db.connect(),
        on: AtomicBool::new(true),
    });
    hot.probe.set_injector(Some(writer.clone()));
    hot.fill();
    assert!(wait_until(|| hot.probe.checkpoints() >= 2));
    let other = TestDb::open(&pool);
    other.fill();
    let served = wait_until(|| other.probe.checkpoints() > 0);
    writer.on.store(false, Ordering::Release);
    assert!(
        served,
        "the hot database kept the thread through {} checkpoints: {:?}",
        hot.probe.checkpoints(),
        pool.stats()
    );
    assert!(wait_until(|| !hot.owes() && !other.owes()));
    assert!(wait_until(
        || !hot.probe.scheduled() && !other.probe.scheduled()
    ));
    hot.assert_intact();
}

/// Under writes that never pause, every checkpoint the pool runs sees a commit land after its
/// snapshot and cannot truncate the log. The log rolls instead: however many checkpoints run, it
/// keeps to a few segments, the pool empties it once the writes stop, and a restart finds every
/// row.
#[test]
fn test_the_pool_keeps_the_log_bounded_under_writes_that_never_pause() {
    let pool = pool(1);
    let db = TestDb::open(&pool);
    let writer = Arc::new(WriteDuringCheckpoint {
        conn: db.db.connect(),
        on: AtomicBool::new(true),
    });
    db.probe.set_injector(Some(writer.clone()));
    db.fill();
    let mut most = 0;
    let mut segments = 0;
    let ran = wait_until(|| {
        most = most.max(db.store.storage.logical_log_size().unwrap());
        segments = segments.max(db.store.storage.logical_log_segments());
        db.probe.checkpoints() >= 50
    });
    writer.on.store(false, Ordering::Release);
    assert!(ran, "{:?}", pool.stats());
    assert!(
        (2..=LOG_SEGMENT_FILES).contains(&segments),
        "the log was spread over {segments} segments"
    );
    // Unrolled, the log would hold every commit, 50 of them 8 KB each.
    assert!(
        most < 64 * 1024,
        "the log grew to {most} bytes over 50 checkpoints"
    );
    assert!(
        wait_until(|| !db.probe.scheduled() && db.log() == 0),
        "the log kept {} bytes once the writes stopped",
        db.log()
    );
    let rows = db.rows();
    let TestDb {
        db: mut restarted,
        conn,
        store,
        probe,
    } = db;
    store.set_background_checkpoint(false);
    assert!(!probe.scheduled());
    drop((conn, store, writer));
    restarted.restart();
    let conn = restarted.connect();
    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
    let mut recovered = None;
    stmt.run_with_row_callback(|row| {
        recovered = row.get_values().next().and_then(|value| value.as_int());
        Ok(())
    })
    .unwrap();
    assert_eq!(recovered, Some(rows));
}

/// A store moved to another pool leaves the first: the first drops its retries of the store's
/// turns, and the second serves its checkpoints.
#[test]
fn test_a_store_moved_to_another_pool_leaves_the_first() {
    let first = pool(1);
    let db = TestDb::open(&first);
    let fail = Arc::new(FailCheckpoints::default());
    fail.on.store(true, Ordering::Release);
    db.probe.set_failure_injector(Some(fail.clone()));
    db.fill();
    assert!(wait_until(|| fail.attempts.load(Ordering::Acquire) >= 2));
    let second = pool(1);
    db.store.set_checkpoint_pool(&second);
    // A turn under way when the store moved ends; no other follows in the first pool.
    let attempts = fail.attempts.load(Ordering::Acquire);
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        fail.attempts.load(Ordering::Acquire) <= attempts + 1,
        "the first pool kept retrying the store that left it"
    );
    assert!(wait_until(|| {
        let stats = first.stats();
        stats.queued == 0 && stats.timed == 0
    }));
    db.insert(256);
    assert!(
        wait_until(|| second.stats().checkpoints > 0 && !db.owes()),
        "{:?}",
        second.stats()
    );
}

/// Panics in the first checkpoint it is given to.
#[derive(Debug, Default)]
struct PanicInCheckpoint {
    fired: AtomicBool,
}

impl YieldInjector for PanicInCheckpoint {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CheckpointYieldPoint::AfterCollectTableRows.point()
            && !self.fired.swap(true, Ordering::AcqRel)
        {
            panic!("a checkpoint panicked");
        }
        false
    }
}

/// A turn that panics is contained: the pool counts it, turns the database's background checkpoint
/// off, and goes on serving the others.
#[test]
fn test_a_turn_that_panics_is_contained() {
    let pool = pool(1);
    let broken = TestDb::open(&pool);
    broken
        .probe
        .set_injector(Some(Arc::new(PanicInCheckpoint::default())));
    broken.fill();
    assert!(
        wait_until(|| pool.stats().panics == 1),
        "{:?}",
        pool.stats()
    );
    assert!(!broken.store.background_checkpoint_enabled());
    let other = TestDb::open(&pool);
    other.fill();
    assert!(
        wait_until(|| other.probe.checkpoints() > 0 && !other.owes()),
        "the pool stopped serving after a panic: {:?}",
        pool.stats()
    );
}

/// Fails every checkpoint as it begins collecting rows while on, counting the attempts.
#[derive(Debug, Default)]
struct FailCheckpoints {
    on: AtomicBool,
    attempts: AtomicU64,
}

impl FailureInjector for FailCheckpoints {
    fn should_fail(
        &self,
        _instance_id: u64,
        _selection_key: u64,
        point: YieldPoint,
    ) -> Option<LimboError> {
        (point == CheckpointYieldPoint::BeforeCollectTableRows.point()
            && self.on.load(Ordering::Acquire))
        .then(|| {
            self.attempts.fetch_add(1, Ordering::AcqRel);
            LimboError::InternalError("injected checkpoint failure".to_string())
        })
    }
}

/// A turn that fails is retried after a delay that doubles each time, and holds no thread
/// meanwhile, so the pool's thread serves another database between the retries.
#[test]
fn test_a_failing_checkpoint_is_retried_later_and_holds_no_thread() {
    let pool = pool(1);
    let failing = TestDb::open(&pool);
    let fail = Arc::new(FailCheckpoints::default());
    fail.on.store(true, Ordering::Release);
    failing.probe.set_failure_injector(Some(fail.clone()));
    failing.fill();
    assert!(
        wait_until(|| fail.attempts.load(Ordering::Acquire) >= 2),
        "{} attempts, {} checkpoints, {} failures, {:?}",
        fail.attempts.load(Ordering::Acquire),
        failing.probe.checkpoints(),
        failing.probe.failures(),
        pool.stats()
    );
    let other = TestDb::open(&pool);
    other.fill();
    assert!(
        wait_until(|| other.probe.checkpoints() > 0),
        "the failing database held the thread: {:?}",
        pool.stats()
    );
    // After the second attempt the delays are 20, 40, 80 and 160 ms and on: at most four attempts
    // fit in 300 ms.
    let attempts = fail.attempts.load(Ordering::Acquire);
    std::thread::sleep(Duration::from_millis(300));
    let retried = fail.attempts.load(Ordering::Acquire) - attempts;
    // While the pool's turns fail, commits that leave the log four thresholds behind checkpoint
    // inline, so the log stays bounded.
    let mut most = 0;
    for _ in 0..80 {
        failing.insert(256);
        most = most.max(failing.store.storage.uncheckpointed_bytes());
    }
    fail.on.store(false, Ordering::Release);
    assert!(
        retried <= 6,
        "{retried} attempts in 300 ms: the retries do not back off"
    );
    assert!(failing.probe.failures() >= 2);
    assert!(failing.probe.assists() > 0);
    assert!(
        most < 5 * 4096,
        "{most} uncheckpointed bytes while the pool's turns failed"
    );
    assert!(
        wait_until(|| failing.probe.checkpoints() > 0 && !failing.owes()),
        "{:?}",
        pool.stats()
    );
    assert!(wait_until(|| !failing.probe.scheduled()));
    failing.assert_intact();
}

/// Steps `stmt` until it is done, false, or waits for a checkpoint running on its database, true.
fn step_until_parked_or_done(db: &TestDb, stmt: &mut crate::Statement) -> bool {
    loop {
        match stmt.step().unwrap() {
            StepResult::IO if db.store.checkpoint_waiters() > 0 => return true,
            StepResult::IO => stmt.get_pager().io.step().unwrap(),
            StepResult::Done => return false,
            other => panic!("expected the insert to commit, got {other:?}"),
        }
    }
}

/// While a checkpoint runs on a database, a commit that leaves the log `BEHIND` thresholds past
/// waits for that checkpoint to end before it decides on its own, so writers cannot outrun a
/// checkpoint that takes long: here the pool's own checkpoint of the database, held after its
/// snapshot.
#[test]
fn test_a_commit_far_behind_waits_for_the_checkpoint_running_on_its_database() {
    let pool = pool(1);
    let db = TestDb::open(&pool);
    let hold = Arc::new(HoldAfterSnapshot::default());
    db.probe.set_injector(Some(hold.clone()));
    db.fill();
    assert!(
        hold.wait_held(),
        "the pool's checkpoint did not reach its snapshot"
    );
    // Commits leave the checkpoint to the pool until one takes the log four thresholds behind.
    let mut parked = None;
    for _ in 0..1_000 {
        let mut stmt = db
            .conn
            .prepare("INSERT INTO t(v) VALUES (randomblob(256))")
            .unwrap();
        if step_until_parked_or_done(&db, &mut stmt) {
            parked = Some(stmt);
            break;
        }
    }
    let behind = db.store.storage.uncheckpointed_bytes();
    let mut stmt = parked.unwrap_or_else(|| {
        panic!("1000 commits returned while the pool's checkpoint ran, {behind} bytes behind")
    });
    let threshold = db.store.storage.checkpoint_threshold() as u64;
    assert!(
        far_behind(db.store.storage.as_ref()) && behind < (BEHIND + 1) * threshold,
        "the commit waited {behind} bytes behind, at a {threshold}-byte threshold"
    );
    hold.release();
    while step_until_parked_or_done(&db, &mut stmt) {
        std::thread::yield_now();
    }
    assert_eq!(db.store.checkpoint_waiters(), 0);
    assert!(
        wait_until(|| !far_behind(db.store.storage.as_ref())),
        "{} uncheckpointed bytes after the commit went on, {:?}",
        db.store.storage.uncheckpointed_bytes(),
        pool.stats()
    );
    db.assert_intact();
}

/// When the pool has fallen so far behind on a database that its uncheckpointed log reaches four
/// thresholds, the database's commits checkpoint inline, so the log stays bounded.
#[test]
fn test_commits_checkpoint_inline_while_the_pool_is_far_behind_on_their_database() {
    let pool = pool(1);
    let busy = TestDb::open(&pool);
    let hold = Arc::new(HoldAfterSnapshot::default());
    busy.probe.set_injector(Some(hold.clone()));
    busy.fill();
    assert!(hold.wait_held(), "the pool's one thread was not held");
    let behind = TestDb::open(&pool);
    let mut most = 0;
    for _ in 0..200 {
        behind.insert(256);
        most = most.max(behind.store.storage.uncheckpointed_bytes());
    }
    let assists = behind.probe.assists();
    hold.release();
    assert!(assists > 0, "no commit checkpointed inline");
    assert!(
        most < 5 * 4096,
        "{most} uncheckpointed bytes with a 4096-byte threshold"
    );
    assert!(wait_until(
        || !behind.probe.scheduled() && !busy.probe.scheduled()
    ));
    assert_eq!(pool.stats().assists, assists);
    behind.assert_intact();
}

/// A turn that finds another checkpoint running on its database gives way, and is retried once
/// that one is done, without checkpointing beside it meanwhile.
#[test]
fn test_a_turn_beside_another_checkpoint_waits_for_it() {
    let pool = pool(1);
    let db = TestDb::open(&pool);
    let hold = Arc::new(HoldAfterSnapshot::default());
    let explicit = db.db.connect();
    explicit.set_yield_injector(Some(hold.clone()));
    let checkpointing = std::thread::spawn(move || {
        explicit
            .checkpoint(CheckpointMode::Passive {
                upper_bound_inclusive: None,
            })
            .map(|_| ())
    });
    assert!(hold.wait_held(), "the explicit checkpoint was not held");
    db.fill();
    assert!(
        wait_until(|| db.probe.deferrals() >= 2),
        "{:?}",
        pool.stats()
    );
    std::thread::sleep(Duration::from_millis(100));
    let checkpoints = db.probe.checkpoints();
    hold.release();
    checkpointing.join().unwrap().unwrap();
    assert_eq!(
        checkpoints, 0,
        "the pool checkpointed beside a running checkpoint"
    );
    assert!(
        wait_until(|| db.probe.checkpoints() > 0 && !db.owes()),
        "{:?}",
        pool.stats()
    );
    assert!(wait_until(|| !db.probe.scheduled()));
    db.assert_intact();
}

/// At a threshold of zero every commit asks for a checkpoint, and the pool stops once the log is
/// covered rather than checkpointing an empty log forever.
#[test]
fn test_at_a_threshold_of_zero_the_pool_stops_once_the_log_is_covered() {
    let pool = pool(1);
    let db = TestDb::open(&pool);
    db.conn
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    for _ in 0..20 {
        db.insert(64);
    }
    assert!(
        wait_until(|| !db.probe.scheduled()),
        "the pool keeps checkpointing: {:?}",
        pool.stats()
    );
    let checkpoints = db.probe.checkpoints();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(db.probe.checkpoints(), checkpoints);
    assert_eq!(db.log(), 0);
}

/// A pool whose thread limit drops to zero lets go of its databases, and their commits checkpoint
/// inline, even while its thread is still busy.
#[test]
fn test_a_pool_without_threads_leaves_checkpoints_to_the_commits() {
    let pool = pool(1);
    let busy = TestDb::open(&pool);
    let hold = Arc::new(HoldAfterSnapshot::default());
    busy.probe.set_injector(Some(hold.clone()));
    busy.fill();
    assert!(hold.wait_held(), "the pool's one thread was not held");
    pool.set_thread_limit(0);
    let db = TestDb::open(&pool);
    for i in 0..200 {
        db.insert(256);
        assert!(db.log() < 8192, "insert {i} left {} log bytes", db.log());
    }
    hold.release();
    assert_eq!(db.probe.checkpoints(), 0);
    assert!(
        wait_until(|| pool.stats().threads == 0),
        "{:?}",
        pool.stats()
    );
    assert!(!busy.probe.scheduled());
}

/// An attached database whose store has the background checkpoint on hands its checkpoints to the
/// store's pool as well: here a database another handle has open, which the attach shares.
#[test]
fn test_an_attached_database_checkpoints_on_its_pool() {
    let pool = pool(1);
    let aux = TestDb::open(&pool);
    let main = MvccTestDbNoConn::new_with_random_db_with_opts(
        DatabaseOpts::new()
            .with_attach(true)
            .with_experimental_mvcc_passive_checkpoint(true),
    );
    let conn = main.connect();
    conn.execute(format!("ATTACH '{}' AS aux", aux.db.get_db().path))
        .unwrap();
    let aux_id = conn.get_database_id_by_name("aux").unwrap();
    let attached = conn
        .mv_store_for_db(aux_id)
        .expect("aux is an MVCC database");
    assert!(
        Arc::ptr_eq(&attached, &aux.store),
        "the attach opened the database again instead of sharing it"
    );
    for _ in 0..100 {
        conn.execute("INSERT INTO aux.t(v) VALUES (randomblob(256))")
            .unwrap();
    }
    assert!(
        wait_until(|| aux.probe.checkpoints() > 0 && !aux.owes()),
        "{:?}",
        pool.stats()
    );
    assert_eq!(pool.stats().assists, 0);
}
