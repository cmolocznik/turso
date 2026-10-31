//! The logical log rolls under writes that never pause: a passive checkpoint that has to keep
//! frames, as every one does while commits land after its snapshot, seals the segment being
//! written and the log goes on in another file, which a later checkpoint frees. Recovery reads the
//! segments as one log.

use super::TransactionState;
use crate::mvcc::database::checkpoint_state_machine::CheckpointYieldPoint;
use crate::mvcc::database::tests::MvccTestDbNoConn;
use crate::mvcc::persistent_storage::logical_log::{segment_path, LOG_SEGMENT_FILES};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::storage::wal::CheckpointMode;
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::sync::Arc;
use crate::{Connection, StepResult};
use std::time::{Duration, Instant};

/// A busy timeout no test here waits out: each park in them ends by the release it waits for.
const PARKS_UNTIL_RELEASED: Duration = Duration::from_secs(60);

/// Commits a row on its connection once the checkpoint it is given to has taken its snapshot, so
/// the checkpoint cannot truncate the log.
struct CommitAfterSnapshot {
    conn: Arc<Connection>,
    fired: AtomicBool,
}

impl std::fmt::Debug for CommitAfterSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CommitAfterSnapshot")
    }
}

impl YieldInjector for CommitAfterSnapshot {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CheckpointYieldPoint::AfterCollectTableRows.point()
            && !self.fired.swap(true, Ordering::AcqRel)
        {
            self.conn
                .execute("INSERT INTO t(v) VALUES (randomblob(512))")
                .unwrap();
        }
        false
    }
}

/// A passive MVCC database with a table `t` and automatic checkpoints off, and the connections
/// the tests use: one that checkpoints and one that writes.
struct TestDb {
    db: MvccTestDbNoConn,
    checkpointer: Arc<Connection>,
    writer: Arc<Connection>,
}

impl TestDb {
    fn open() -> Self {
        let db = MvccTestDbNoConn::new_with_random_db_passive();
        let writer = db.connect();
        writer
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
            .unwrap();
        writer
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        let checkpointer = db.connect();
        Self {
            db,
            checkpointer,
            writer,
        }
    }

    fn insert(&self) {
        self.writer
            .execute("INSERT INTO t(v) VALUES (randomblob(512))")
            .unwrap();
    }

    /// A passive checkpoint beside a commit that lands after its snapshot.
    fn checkpoint_beside_a_commit(&self) {
        let injector = Arc::new(CommitAfterSnapshot {
            conn: self.writer.clone(),
            fired: AtomicBool::new(false),
        });
        self.checkpointer.set_yield_injector(Some(injector.clone()));
        self.checkpointer
            .checkpoint(CheckpointMode::Passive {
                upper_bound_inclusive: None,
            })
            .unwrap();
        self.checkpointer.set_yield_injector(None);
        assert!(injector.fired.load(Ordering::Acquire));
    }

    fn log_size(&self) -> u64 {
        self.db.get_mvcc_store().storage.logical_log_size().unwrap()
    }

    fn segments(&self) -> usize {
        self.db.get_mvcc_store().storage.logical_log_segments()
    }

    /// The path of the file of slot `slot` of the log.
    fn slot_path(&self, slot: usize) -> String {
        let base = std::path::Path::new(&self.db.get_db().path)
            .with_extension("db-log")
            .to_string_lossy()
            .into_owned();
        segment_path(&base, slot)
    }

    /// The size of the file of slot `slot` of the log.
    fn slot_size(&self, slot: usize) -> u64 {
        std::fs::metadata(self.slot_path(slot)).map_or(0, |metadata| metadata.len())
    }

    /// Reopens the database from its files, as after a crash, and returns its rows and whether its
    /// integrity check passes.
    fn restart(self) -> (MvccTestDbNoConn, i64) {
        let Self {
            mut db,
            checkpointer,
            writer,
        } = self;
        drop(checkpointer);
        drop(writer);
        db.restart();
        let conn = db.connect();
        let rows = count(&conn);
        assert_integrity_ok(&conn);
        (db, rows)
    }
}

/// A database is refused when log files left by another sit in the slots its log rolls through,
/// as it is beside a leftover `.db-log`: it would otherwise read them as its own log.
#[test]
fn test_a_database_beside_log_files_left_by_another_refuses_to_open() {
    let donor = TestDb::open();
    donor.insert();
    donor.checkpoint_beside_a_commit();
    donor.checkpoint_beside_a_commit();
    assert_eq!(donor.segments(), 2);
    // Covers everything: the log resets into a numbered segment, at offset zero, in the second
    // file, where a later commit lands.
    donor
        .checkpointer
        .checkpoint(CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();
    donor.insert();
    let leftover = std::fs::read(donor.slot_path(1)).unwrap();
    assert!(leftover.len() > crate::mvcc::persistent_storage::logical_log::LOG_HDR_SIZE);

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("fresh.db");
    let path = path.to_str().unwrap();
    let log = std::path::Path::new(path).with_extension("db-log");
    std::fs::write(segment_path(&log.to_string_lossy(), 1), &leftover).unwrap();
    let io: Arc<dyn crate::IO> = Arc::new(crate::PlatformIO::new().unwrap());
    let opened = crate::Database::open_file_with_flags(
        io,
        path,
        crate::OpenFlags::default(),
        crate::DatabaseOpts::new()
            .with_mvcc(true)
            .with_experimental_mvcc_passive_checkpoint(true),
        None,
        Arc::new(crate::SqliteDialect),
    );
    assert!(
        matches!(opened, Err(crate::LimboError::Corrupt(_))),
        "a database opened beside another log's files"
    );
}

fn count(conn: &Arc<Connection>) -> i64 {
    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
    let mut rows = None;
    stmt.run_with_row_callback(|row| {
        rows = row.get_values().next().and_then(|value| value.as_int());
        Ok(())
    })
    .unwrap();
    rows.unwrap()
}

fn assert_integrity_ok(conn: &Arc<Connection>) {
    let mut stmt = conn.prepare("PRAGMA integrity_check").unwrap();
    let mut result = Vec::new();
    stmt.run_with_row_callback(|row| {
        result.extend(row.get_values().map(|value| value.to_string()));
        Ok(())
    })
    .unwrap();
    assert_eq!(result, ["ok"]);
}

/// Every checkpoint sees a commit land after its snapshot, as under writes that never pause, and
/// cannot truncate the log. It rolls instead: however many checkpoints run, the log keeps to a few
/// segments of a few commits each, and a restart replays every commit.
#[test]
fn test_a_log_every_checkpoint_has_to_keep_rolls_and_stays_bounded() {
    let db = TestDb::open();
    let mut rows = 0;
    let mut most = 0;
    for _ in 0..60 {
        db.insert();
        db.checkpoint_beside_a_commit();
        rows += 2;
        assert!(db.segments() <= LOG_SEGMENT_FILES);
        most = most.max(db.log_size());
    }
    // Unrolled, the log would hold every one of the 120 commits, over 60 KB.
    assert!(
        most < 8 * 1024,
        "the log grew to {most} bytes over 60 checkpoints"
    );
    assert!(db.segments() >= 2, "the log never rolled");
    let (_db, recovered) = db.restart();
    assert_eq!(recovered, rows);
}

/// A restart after the log rolled replays the segments that hold commits no checkpoint covered: a
/// sealed one and the one being written.
#[test]
fn test_recovery_replays_a_log_rolled_over_segments() {
    let db = TestDb::open();
    db.insert();
    // The first checkpoint that keeps frames readies a file; the second rolls into it.
    db.checkpoint_beside_a_commit();
    db.checkpoint_beside_a_commit();
    assert_eq!(db.segments(), 2);
    for _ in 0..3 {
        db.insert();
    }
    let (_db, recovered) = db.restart();
    assert_eq!(recovered, 6);
}

/// A commit still being made when a TRUNCATE runs keeps every checkpoint from covering what came
/// after it, so the TRUNCATE cannot free the segment it began beside. Once its passes can free no
/// more, it reports Busy and the log keeps its frames: never a success that leaves them in the
/// files. Once the commit is made, the next TRUNCATE empties the log.
#[test]
fn test_a_truncate_that_cannot_free_what_came_before_it_reports_busy() {
    let db = TestDb::open();
    db.insert();
    let store = db.db.get_mvcc_store();
    let making = store
        .begin_tx(db.checkpointer.pager.load().clone())
        .unwrap();
    let set_state = |state: TransactionState| {
        store.txs.get(&making).unwrap().value().state.store(state);
    };
    // Below every commit made so far, so no snapshot covers any of them.
    set_state(TransactionState::Preparing(1));
    let truncate = || {
        db.checkpointer.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
    };
    let res = truncate();
    assert!(
        matches!(res, Err(crate::LimboError::Busy)),
        "a TRUNCATE that cannot free what came before it must report Busy, got {res:?}"
    );
    assert_eq!(
        db.segments(),
        2,
        "its passes rolled the log, then could free no more"
    );
    assert!(db.log_size() > 0);
    set_state(TransactionState::Committed(1));
    truncate().unwrap();
    assert_eq!((db.segments(), db.log_size()), (1, 0));
}

/// Commits a row once the first pass of the checkpoint it is given to has taken its snapshot, so
/// that pass cannot empty the log. Once a later pass has written the WAL, it begins a read on
/// another connection and leaves it open: a reader of the WAL beside the rest of that pass.
struct ReadBesideALaterPass {
    writer: Arc<Connection>,
    reader: Arc<Connection>,
    passes: AtomicUsize,
    reading: AtomicBool,
}

impl std::fmt::Debug for ReadBesideALaterPass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadBesideALaterPass")
    }
}

impl YieldInjector for ReadBesideALaterPass {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CheckpointYieldPoint::AfterCollectTableRows.point()
            && self.passes.fetch_add(1, Ordering::AcqRel) == 0
        {
            self.writer
                .execute("INSERT INTO t(v) VALUES (randomblob(512))")
                .unwrap();
        }
        if point == CheckpointYieldPoint::BeforePublishWindow.point()
            && self.passes.load(Ordering::Acquire) >= 2
            && !self.reading.swap(true, Ordering::AcqRel)
        {
            self.reader.execute("BEGIN").unwrap();
            count(&self.reader);
        }
        false
    }
}

/// A TRUNCATE's first pass empties the WAL, and the passes after it write only what was committed
/// after that pass's snapshot, so they run as passive passes: a reader of the WAL beside one of
/// them does not hold the TRUNCATE back.
#[test]
fn test_a_truncates_later_passes_run_beside_a_reader_of_the_wal() {
    let db = TestDb::open();
    db.insert();
    let injector = Arc::new(ReadBesideALaterPass {
        writer: db.writer.clone(),
        reader: db.db.connect(),
        passes: AtomicUsize::new(0),
        reading: AtomicBool::new(false),
    });
    db.checkpointer.set_yield_injector(Some(injector.clone()));
    let res = db.checkpointer.checkpoint(CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    });
    db.checkpointer.set_yield_injector(None);
    assert!(
        injector.reading.load(Ordering::Acquire),
        "no later pass wrote the WAL"
    );
    assert!(
        res.is_ok(),
        "a reader of the WAL held back a later pass of the TRUNCATE: {res:?}"
    );
    injector.reader.execute("COMMIT").unwrap();
    assert_eq!(count(&db.checkpointer), 2);
}

/// Counts the passes of the checkpoint it is given to, committing a row once each has taken its
/// snapshot.
struct CountPasses {
    writer: Arc<Connection>,
    passes: AtomicUsize,
}

impl std::fmt::Debug for CountPasses {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CountPasses")
    }
}

impl YieldInjector for CountPasses {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CheckpointYieldPoint::AfterCollectTableRows.point() {
            self.passes.fetch_add(1, Ordering::AcqRel);
            self.writer
                .execute("INSERT INTO t(v) VALUES (randomblob(512))")
                .unwrap();
        }
        false
    }
}

/// A TRUNCATE statement that reported Busy, stepped again, goes on toward the goal of its first
/// attempt: the commits made since it began do not cost it more passes.
#[test]
fn test_a_busy_truncate_stepped_again_goes_on_toward_its_first_goal() {
    let db = TestDb::open();
    db.insert();
    let store = db.db.get_mvcc_store();
    let io = db.checkpointer.pager.load().io.clone();
    let making = store
        .begin_tx(db.checkpointer.pager.load().clone())
        .unwrap();
    let set_state = |state: TransactionState| {
        store.txs.get(&making).unwrap().value().state.store(state);
    };
    // Below every commit made so far, so no snapshot covers any of them.
    set_state(TransactionState::Preparing(1));
    let mut truncate = db
        .checkpointer
        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    loop {
        match truncate.step().unwrap() {
            StepResult::Busy => break,
            StepResult::IO | StepResult::Yield => io.step().unwrap(),
            other => panic!("the first attempt ended in {other:?}"),
        }
    }
    assert_eq!(db.segments(), 2, "the first attempt rolled the log");
    set_state(TransactionState::Committed(1));
    db.insert();
    db.insert();
    let passes = Arc::new(CountPasses {
        writer: db.writer.clone(),
        passes: AtomicUsize::new(0),
    });
    db.checkpointer.set_yield_injector(Some(passes.clone()));
    loop {
        match truncate.step().unwrap() {
            StepResult::Done => break,
            StepResult::Row => {}
            StepResult::IO | StepResult::Yield => io.step().unwrap(),
            other => panic!("the retry ended in {other:?}"),
        }
    }
    db.checkpointer.set_yield_injector(None);
    assert_eq!(
        passes.passes.load(Ordering::Acquire),
        1,
        "the retry freed the segment its first attempt began beside, in one pass"
    );
    assert_eq!(count(&db.checkpointer), 4);
}

/// Once a checkpoint has freed the log's first file and the segments after it are elsewhere, a
/// restart, which first completes the checkpoint the WAL shows was interrupted, goes on in the
/// newest segment and replays every commit.
#[test]
fn test_a_restart_after_the_first_file_was_freed_keeps_the_newer_segments() {
    let db = TestDb::open();
    let mut rows = 0;
    let mut cycles = 0;
    while !(db.slot_size(0) == 0 && db.log_size() > 0) {
        assert!(cycles < 20, "the log's first file was never freed");
        db.insert();
        db.checkpoint_beside_a_commit();
        rows += 2;
        cycles += 1;
    }
    let (db, recovered) = db.restart();
    assert_eq!(recovered, rows);
    // Writing goes on, and survives another restart.
    let conn = db.connect();
    conn.execute("INSERT INTO t(v) VALUES (randomblob(512))")
        .unwrap();
    drop(conn);
    let mut db = db;
    db.restart();
    assert_eq!(count(&db.connect()), rows + 1);
}

/// A log rolled under passive checkpoints, its process gone, is reopened without them: recovery
/// replays every segment, and a blocking checkpoint, which clears the whole log, frees every
/// segment and empties every file the log rolled through, and the rows stay.
#[test]
fn test_a_rolled_log_reopened_without_passive_checkpoints_is_cleared_by_a_blocking_one() {
    let rolled = TestDb::open();
    rolled.insert();
    rolled.checkpoint_beside_a_commit();
    rolled.checkpoint_beside_a_commit();
    assert_eq!(rolled.segments(), 2);
    rolled.insert();
    let slots: Vec<String> = (0..LOG_SEGMENT_FILES)
        .map(|slot| rolled.slot_path(slot))
        .collect();
    let TestDb {
        mut db,
        checkpointer,
        writer,
    } = rolled;
    drop((checkpointer, writer));
    let path = db.get_db().path.clone();
    drop(db.db.take());
    let reopen = || {
        crate::database::remove_file_from_database_registry(&path);
        crate::Database::open_file_with_flags(
            Arc::new(crate::PlatformIO::new().unwrap()),
            &path,
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
            None,
            Arc::new(crate::SqliteDialect),
        )
        .unwrap()
    };
    let blocking = reopen();
    let conn = blocking.connect(crate::ConnectOpts::new()).unwrap();
    assert_eq!(count(&conn), 4);
    conn.checkpoint(CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })
    .unwrap();
    let store = blocking.get_mv_store().as_ref().cloned().unwrap();
    assert_eq!(store.storage.logical_log_segments(), 1);
    assert_eq!(store.storage.logical_log_size().unwrap(), 0);
    for slot in &slots {
        let len = std::fs::metadata(slot).map_or(0, |metadata| metadata.len());
        assert_eq!(len, 0, "{slot} kept {len} bytes");
    }
    drop((conn, store, blocking));
    let reopened = reopen();
    let conn = reopened.connect(crate::ConnectOpts::new()).unwrap();
    assert_eq!(count(&conn), 4);
    assert_integrity_ok(&conn);
}

/// Counts the wakes of a step that parked.
struct Wakes(AtomicUsize);

impl std::task::Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Begins a read on another connection once the first pass of the checkpoint it is given to has
/// written the WAL, and leaves it open: a reader of the frames that checkpoint copies.
struct ReadOnceTheWalIsWritten {
    reader: Arc<Connection>,
    reading: AtomicBool,
}

impl std::fmt::Debug for ReadOnceTheWalIsWritten {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadOnceTheWalIsWritten")
    }
}

impl YieldInjector for ReadOnceTheWalIsWritten {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CheckpointYieldPoint::BeforePublishWindow.point()
            && !self.reading.swap(true, Ordering::AcqRel)
        {
            self.reader.execute("BEGIN").unwrap();
            count(&self.reader);
        }
        false
    }
}

/// Steps `stmt` with `waker`, driving its IO, until it parks in a checkpoint (`None`) or ends.
fn step_until_parked_or_done(
    stmt: &mut crate::Statement,
    io: &Arc<dyn crate::IO>,
    waker: &std::task::Waker,
) -> Option<StepResult> {
    for _ in 0..100_000 {
        match stmt.step_with_waker(waker).unwrap() {
            StepResult::IO | StepResult::Yield => {
                if stmt.parked_checkpoint() {
                    return None;
                }
                io.step().unwrap();
            }
            StepResult::Row => {}
            ended => return Some(ended),
        }
    }
    panic!("the statement neither parked nor ended");
}

impl TestDb {
    /// The size of the database's WAL file.
    fn wal_size(&self) -> u64 {
        let path = format!("{}-wal", self.db.get_db().path);
        std::fs::metadata(path).map_or(0, |metadata| metadata.len())
    }

    /// A TRUNCATE statement to step with a waker, on a connection whose busy timeout is
    /// `busy_timeout`, as long as the checkpoint may park.
    fn truncate_parking_for(&self, busy_timeout: Duration) -> crate::Statement {
        self.checkpointer.set_busy_timeout(busy_timeout);
        self.checkpointer
            .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap()
    }

    /// A TRUNCATE statement stepped with a waker that has parked on a reader of the frames it
    /// copied, for at most `busy_timeout`, with that reader and the waker's count.
    fn park_a_truncate_on_a_reader(
        &self,
        busy_timeout: Duration,
    ) -> (
        crate::Statement,
        Arc<ReadOnceTheWalIsWritten>,
        Arc<Wakes>,
        std::task::Waker,
    ) {
        let io = self.checkpointer.pager.load().io.clone();
        let injector = Arc::new(ReadOnceTheWalIsWritten {
            reader: self.db.connect(),
            reading: AtomicBool::new(false),
        });
        self.checkpointer.set_yield_injector(Some(injector.clone()));
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(wakes.clone());
        let mut truncate = self.truncate_parking_for(busy_timeout);
        let ended = step_until_parked_or_done(&mut truncate, &io, &waker);
        self.checkpointer.set_yield_injector(None);
        assert!(injector.reading.load(Ordering::Acquire), "no reader began");
        assert!(
            ended.is_none(),
            "the TRUNCATE ended beside a reader of its frames: {ended:?}"
        );
        (truncate, injector, wakes, waker)
    }
}

/// A TRUNCATE stepped with a waker that meets a reader of frames it copied parks instead of
/// reporting Busy, and that reader's end wakes it. Nothing wakes it before, and a reader that
/// began after its backfill reads the database file, so it does not hold the TRUNCATE.
#[test]
fn test_a_truncate_parks_on_a_reader_of_the_wal_until_that_reader_ends() {
    let db = TestDb::open();
    db.insert();
    let io = db.checkpointer.pager.load().io.clone();
    let (mut truncate, injector, wakes, waker) =
        db.park_a_truncate_on_a_reader(PARKS_UNTIL_RELEASED);
    let parked_at = wakes.0.load(Ordering::SeqCst);
    assert!(matches!(
        truncate.step_with_waker(&waker).unwrap(),
        StepResult::IO
    ));
    assert!(
        truncate.parked_checkpoint(),
        "stepped again, it did not stay parked"
    );
    let later = db.db.connect();
    later.execute("BEGIN").unwrap();
    count(&later);
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        parked_at,
        "woken while the reader of its frames read"
    );
    injector.reader.execute("COMMIT").unwrap();
    assert!(
        wakes.0.load(Ordering::SeqCst) > parked_at,
        "the reader's end did not wake the TRUNCATE"
    );
    let ended = step_until_parked_or_done(&mut truncate, &io, &waker);
    assert!(
        matches!(ended, Some(StepResult::Done)),
        "the TRUNCATE did not end beside a reader of the database file: {ended:?}"
    );
    assert_eq!(db.wal_size(), 0, "the TRUNCATE left the WAL its bytes");
    later.execute("COMMIT").unwrap();
    assert_eq!(count(&db.checkpointer), 1);
}

/// A parked TRUNCATE whose statement is reset lets every lock go, the database's checkpoint and
/// the WAL's, so the next checkpoint runs.
#[test]
fn test_a_parked_truncate_reset_lets_its_locks_go() {
    let db = TestDb::open();
    db.insert();
    let (mut truncate, injector, _wakes, _waker) =
        db.park_a_truncate_on_a_reader(PARKS_UNTIL_RELEASED);
    truncate.reset().unwrap();
    assert!(!db.db.get_mvcc_store().checkpoint_in_progress());
    injector.reader.execute("COMMIT").unwrap();
    db.checkpointer
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    assert_eq!((db.log_size(), db.wal_size()), (0, 0));
}

/// A parked TRUNCATE whose reader outlasts its busy timeout is woken at that timeout, reports Busy
/// with no busy-handler retry, and lets every lock go, so run again it ends.
#[test]
fn test_a_parked_truncate_reports_busy_at_its_busy_timeout_and_lets_its_locks_go() {
    let db = TestDb::open();
    db.insert();
    let io = db.checkpointer.pager.load().io.clone();
    let busy_timeout = Duration::from_millis(200);
    let (mut truncate, injector, wakes, waker) = db.park_a_truncate_on_a_reader(busy_timeout);
    let parked_at = wakes.0.load(Ordering::SeqCst);
    let waiting = Instant::now();
    while wakes.0.load(Ordering::SeqCst) == parked_at {
        assert!(
            waiting.elapsed() < PARKS_UNTIL_RELEASED,
            "nothing woke the TRUNCATE at its busy timeout"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let ended = step_until_parked_or_done(&mut truncate, &io, &waker);
    assert!(
        matches!(ended, Some(StepResult::Busy)),
        "past its busy timeout the TRUNCATE must report Busy, got {ended:?}"
    );
    assert!(!truncate.parked_checkpoint());
    assert!(!db.db.get_mvcc_store().checkpoint_in_progress());
    injector.reader.execute("COMMIT").unwrap();
    truncate.reset().unwrap();
    assert!(matches!(
        step_until_parked_or_done(&mut truncate, &io, &waker),
        Some(StepResult::Done)
    ));
    assert_eq!((db.log_size(), db.wal_size()), (0, 0));
}

/// Without a busy timeout a TRUNCATE does not wait at all, as SQLite does not: stepped with a
/// waker beside a reader of its frames, it reports Busy at once.
#[test]
fn test_a_truncate_without_a_busy_timeout_reports_busy_beside_a_reader() {
    let db = TestDb::open();
    db.insert();
    let io = db.checkpointer.pager.load().io.clone();
    let injector = Arc::new(ReadOnceTheWalIsWritten {
        reader: db.db.connect(),
        reading: AtomicBool::new(false),
    });
    db.checkpointer.set_yield_injector(Some(injector.clone()));
    let waker = std::task::Waker::from(Arc::new(Wakes(AtomicUsize::new(0))));
    let mut truncate = db
        .checkpointer
        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let ended = step_until_parked_or_done(&mut truncate, &io, &waker);
    db.checkpointer.set_yield_injector(None);
    assert!(injector.reading.load(Ordering::Acquire));
    assert!(
        matches!(ended, Some(StepResult::Busy)),
        "without a busy timeout the TRUNCATE must report Busy beside a reader, got {ended:?}"
    );
    injector.reader.execute("COMMIT").unwrap();
    truncate.reset().unwrap();
    assert!(matches!(
        step_until_parked_or_done(&mut truncate, &io, &waker),
        Some(StepResult::Done)
    ));
    assert_eq!((db.log_size(), db.wal_size()), (0, 0));
}

/// A TRUNCATE whose connection has a busy handler of its own does not park: stepped with a waker
/// beside a reader of its frames, it reports Busy to that handler, which decides.
#[test]
fn test_a_truncate_with_a_busy_handler_of_its_own_asks_it_rather_than_park() {
    let db = TestDb::open();
    db.insert();
    let io = db.checkpointer.pager.load().io.clone();
    let asked = Arc::new(AtomicUsize::new(0));
    let counted = asked.clone();
    db.checkpointer.set_busy_handler(Some(Box::new(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        0
    })));
    let injector = Arc::new(ReadOnceTheWalIsWritten {
        reader: db.db.connect(),
        reading: AtomicBool::new(false),
    });
    db.checkpointer.set_yield_injector(Some(injector.clone()));
    let waker = std::task::Waker::from(Arc::new(Wakes(AtomicUsize::new(0))));
    let mut truncate = db
        .checkpointer
        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let ended = step_until_parked_or_done(&mut truncate, &io, &waker);
    db.checkpointer.set_yield_injector(None);
    assert!(injector.reading.load(Ordering::Acquire));
    assert!(
        matches!(ended, Some(StepResult::Busy)),
        "a TRUNCATE whose busy handler refuses must report Busy, got {ended:?}"
    );
    assert_eq!(
        asked.load(Ordering::SeqCst),
        1,
        "its busy handler was not asked"
    );
    injector.reader.execute("COMMIT").unwrap();
}

/// A TRUNCATE whose pass can free nothing, as a commit its snapshot cannot cover is still in
/// flight, parks until that commit ends, which wakes it, and then frees the log.
#[test]
fn test_a_truncate_parks_on_a_commit_its_pass_cannot_cover_until_that_commit_ends() {
    let db = TestDb::open();
    db.insert();
    let io = db.checkpointer.pager.load().io.clone();
    let store = db.db.get_mvcc_store();
    let making = store
        .begin_tx(db.checkpointer.pager.load().clone())
        .unwrap();
    let set_state = |state: TransactionState| {
        store.txs.get(&making).unwrap().value().state.store(state);
    };
    // Below every commit made so far, so no snapshot covers any of them.
    set_state(TransactionState::Preparing(1));
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(wakes.clone());
    let mut truncate = db.truncate_parking_for(PARKS_UNTIL_RELEASED);
    assert!(
        step_until_parked_or_done(&mut truncate, &io, &waker).is_none(),
        "the TRUNCATE did not park on the commit its pass cannot cover"
    );
    assert_eq!(
        db.segments(),
        2,
        "its passes rolled the log, then could free no more"
    );
    let parked_at = wakes.0.load(Ordering::SeqCst);
    set_state(TransactionState::Committed(1));
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        parked_at,
        "woken before the commit ended"
    );
    store.remove_tx(making);
    assert!(
        wakes.0.load(Ordering::SeqCst) > parked_at,
        "the commit's end did not wake the TRUNCATE"
    );
    assert!(matches!(
        step_until_parked_or_done(&mut truncate, &io, &waker),
        Some(StepResult::Done)
    ));
    assert_eq!((db.segments(), db.log_size()), (1, 0));
}

/// Stepped without a waker, a TRUNCATE beside a reader of its frames reports Busy at once, as
/// before: a caller that cannot suspend keeps its busy handler.
#[test]
fn test_a_truncate_stepped_without_a_waker_reports_busy_beside_a_reader() {
    let db = TestDb::open();
    db.insert();
    let injector = Arc::new(ReadOnceTheWalIsWritten {
        reader: db.db.connect(),
        reading: AtomicBool::new(false),
    });
    db.checkpointer.set_yield_injector(Some(injector.clone()));
    let res = db.checkpointer.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    db.checkpointer.set_yield_injector(None);
    assert!(injector.reading.load(Ordering::Acquire));
    assert!(
        matches!(res, Err(crate::LimboError::Busy)),
        "without a waker the TRUNCATE must report Busy beside a reader, got {res:?}"
    );
    injector.reader.execute("COMMIT").unwrap();
    db.checkpointer
        .execute("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    assert_eq!((db.log_size(), db.wal_size()), (0, 0));
}

/// A TRUNCATE stepped with a waker beside a checkpoint running on another thread parks until that
/// one ends, which wakes it, and then runs.
#[test]
fn test_a_truncate_parks_on_a_checkpoint_running_on_another_thread() {
    use crate::mvcc::database::checkpoint_state_machine::CheckpointStateMachine;
    use crate::state_machine::{StateTransition, TransitionResult};

    let db = TestDb::open();
    db.insert();
    let io = db.checkpointer.pager.load().io.clone();
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let other = db.db.connect();
    let store = db.db.get_mvcc_store();
    let running = std::thread::spawn(move || {
        let pager = other.pager.load().clone();
        let mut running = CheckpointStateMachine::new(
            pager.clone(),
            store,
            other.clone(),
            true,
            other.get_sync_mode(),
            crate::MAIN_DB_ID,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        assert!(matches!(
            running.step(&()).unwrap(),
            TransitionResult::Continue
        ));
        held_tx.send(()).unwrap();
        go_rx.recv().unwrap();
        loop {
            match running.step(&()).unwrap() {
                TransitionResult::Io(io) => io.wait(pager.io.as_ref()).unwrap(),
                TransitionResult::Continue => {}
                TransitionResult::Done(_) => break,
            }
        }
    });
    held_rx.recv().unwrap();
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(wakes.clone());
    let mut truncate = db.truncate_parking_for(PARKS_UNTIL_RELEASED);
    assert!(
        step_until_parked_or_done(&mut truncate, &io, &waker).is_none(),
        "the TRUNCATE did not park beside the running checkpoint"
    );
    let parked_at = wakes.0.load(Ordering::SeqCst);
    go_tx.send(()).unwrap();
    running.join().unwrap();
    assert!(
        wakes.0.load(Ordering::SeqCst) > parked_at,
        "the running checkpoint's end did not wake the TRUNCATE"
    );
    assert!(matches!(
        step_until_parked_or_done(&mut truncate, &io, &waker),
        Some(StepResult::Done)
    ));
    assert_eq!((db.log_size(), db.wal_size()), (0, 0));
}

/// A reader of the database file that began before a TRUNCATE holds read mark 0, which the
/// TRUNCATE's backfill needs to itself: stepped with a waker, the TRUNCATE parks until that reader
/// ends, which wakes it, and then runs.
#[test]
fn test_a_truncate_parks_on_a_reader_of_the_database_file_until_it_ends() {
    let db = TestDb::open();
    db.insert();
    db.checkpointer
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    db.insert();
    let io = db.checkpointer.pager.load().io.clone();
    // Begun on an empty WAL, it reads the database file through read mark 0.
    let reader = db.db.connect();
    reader.execute("BEGIN").unwrap();
    count(&reader);
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(wakes.clone());
    let mut truncate = db.truncate_parking_for(PARKS_UNTIL_RELEASED);
    assert!(
        step_until_parked_or_done(&mut truncate, &io, &waker).is_none(),
        "the TRUNCATE did not park beside a reader of the database file"
    );
    let parked_at = wakes.0.load(Ordering::SeqCst);
    reader.execute("COMMIT").unwrap();
    assert!(
        wakes.0.load(Ordering::SeqCst) > parked_at,
        "the reader's end did not wake the TRUNCATE"
    );
    assert!(matches!(
        step_until_parked_or_done(&mut truncate, &io, &waker),
        Some(StepResult::Done)
    ));
    assert_eq!((db.log_size(), db.wal_size()), (0, 0));
    assert_eq!(count(&db.checkpointer), 2);
}

/// A reader pinned at an older frame of the WAL keeps a TRUNCATE from copying the frames after it:
/// stepped with a waker, the TRUNCATE parks until that reader ends, which wakes it, and then
/// copies every frame and runs.
#[test]
fn test_a_truncate_parks_on_a_reader_pinned_below_its_frames_until_it_ends() {
    let db = TestDb::open();
    db.insert();
    let io = db.checkpointer.pager.load().io.clone();
    // A reader of the database file keeps a passive checkpoint from backfilling what it writes.
    let file_reader = db.db.connect();
    file_reader.execute("BEGIN").unwrap();
    count(&file_reader);
    db.checkpointer
        .checkpoint(CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();
    // Pinned at the frames that checkpoint wrote, below any the TRUNCATE writes next.
    let pinned = db.db.connect();
    pinned.execute("BEGIN").unwrap();
    count(&pinned);
    file_reader.execute("COMMIT").unwrap();
    db.insert();
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(wakes.clone());
    let mut truncate = db.truncate_parking_for(PARKS_UNTIL_RELEASED);
    assert!(
        step_until_parked_or_done(&mut truncate, &io, &waker).is_none(),
        "the TRUNCATE did not park beside a reader pinned below its frames"
    );
    let parked_at = wakes.0.load(Ordering::SeqCst);
    pinned.execute("COMMIT").unwrap();
    assert!(
        wakes.0.load(Ordering::SeqCst) > parked_at,
        "the reader's end did not wake the TRUNCATE"
    );
    assert!(matches!(
        step_until_parked_or_done(&mut truncate, &io, &waker),
        Some(StepResult::Done)
    ));
    assert_eq!((db.log_size(), db.wal_size()), (0, 0));
    assert_eq!(count(&db.checkpointer), 2);
}
