//! A database whose logical log grows in steps (`PRAGMA mvcc_log_growth`): a checkpoint leaves the
//! log's file its size and restarts the log in place, recovery reads what was committed since, a
//! TRUNCATE empties the file, and the log syncs only its data.

use crate::io::{File, FileId, FileSyncType};
use crate::mvcc::database::checkpoint_state_machine::CheckpointYieldPoint;
use crate::mvcc::database::tests::MvccTestDbNoConn;
use crate::mvcc::persistent_storage::logical_log::{segment_path, LOG_SEGMENT_FILES};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::storage::wal::CheckpointMode;
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::{Arc, Mutex};
use crate::{
    Buffer, Clock, Completion, ConnectOpts, Connection, Database, DatabaseOpts, MemoryIO,
    MonotonicInstant, OpenFlags, Result, SqliteDialect, WallClockInstant, IO,
};
use std::collections::HashMap;

/// The step the tests' logs grow by.
const STEP: u64 = 65536;

/// A passive MVCC database on files, with a table `t`, automatic checkpoints off and its log
/// growing in steps.
fn open() -> (MvccTestDbNoConn, Arc<Connection>) {
    open_with(DatabaseOpts::new())
}

fn open_with(opts: DatabaseOpts) -> (MvccTestDbNoConn, Arc<Connection>) {
    let db = MvccTestDbNoConn::new_with_random_db_with_opts(
        opts.with_experimental_mvcc_passive_checkpoint(true),
    );
    let conn = connect(&db);
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    (db, conn)
}

/// A connection to `db`, which sets what the tests' logs run with: a setting lasts as long as the
/// database stays open.
fn connect(db: &MvccTestDbNoConn) -> Arc<Connection> {
    let conn = db.connect();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute(format!("PRAGMA mvcc_log_growth = {STEP}"))
        .unwrap();
    conn
}

fn insert(conn: &Arc<Connection>, ids: impl IntoIterator<Item = i64>) {
    for id in ids {
        conn.execute(format!("INSERT INTO t VALUES ({id}, 'v{id}')"))
            .unwrap();
    }
}

fn ids(conn: &Arc<Connection>) -> Vec<i64> {
    let mut stmt = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
    let mut ids = Vec::new();
    stmt.run_with_row_callback(|row| {
        ids.push(row.get::<i64>(0).unwrap());
        Ok(())
    })
    .unwrap();
    ids
}

fn passive(conn: &Arc<Connection>) {
    conn.checkpoint(CheckpointMode::Passive {
        upper_bound_inclusive: None,
    })
    .unwrap();
}

/// The bytes the log's files hold, and the log's offset.
fn log(db: &MvccTestDbNoConn) -> (u64, u64) {
    let store = db.get_mvcc_store();
    (
        store.storage.logical_log_size().unwrap(),
        store.storage.logical_log_offset(),
    )
}

/// Reopens the database from its files, as after a crash.
fn crash(db: &mut MvccTestDbNoConn, conn: Arc<Connection>) -> Arc<Connection> {
    drop(conn);
    db.restart();
    connect(db)
}

/// A checkpoint that covers every frame leaves the log's file its size and restarts the log at its
/// start. Recovery right after reads the frames from before, which it does not replay, and once
/// commits follow, the frames written since; neither loses or brings back a row.
#[test]
fn test_a_checkpoint_restarts_a_log_that_grows_in_steps_in_place() {
    let (mut db, conn) = open();
    insert(&conn, 1..=20);
    conn.execute("DELETE FROM t WHERE id = 5").unwrap();
    let (size, offset) = log(&db);
    assert_eq!(size, STEP, "the log's file is not one step");
    assert!(offset > 0);

    passive(&conn);
    assert_eq!(log(&db), (STEP, 0), "the checkpoint did not restart the log in place");
    let expected: Vec<i64> = (1..=20).filter(|&id| id != 5).collect();
    let conn = crash(&mut db, conn);
    assert_eq!(ids(&conn), expected);

    passive(&conn);
    insert(&conn, 21..=23);
    conn.execute("DELETE FROM t WHERE id = 7").unwrap();
    assert_eq!(log(&db).0, STEP);
    let conn = crash(&mut db, conn);
    let expected: Vec<i64> = (1..=23).filter(|&id| id != 5 && id != 7).collect();
    assert_eq!(ids(&conn), expected);
}

/// The control: without steps, the checkpoint truncates the log's file.
#[test]
fn test_a_checkpoint_truncates_a_log_that_does_not_grow_in_steps() {
    let (db, conn) = open();
    conn.execute("PRAGMA mvcc_log_growth = 0").unwrap();
    insert(&conn, 1..=20);
    passive(&conn);
    assert_eq!(log(&db), (0, 0));
}

/// Commits row `id` on its connection once the checkpoint it is given to has taken its snapshot,
/// so that checkpoint cannot cover it.
struct CommitAfterSnapshot {
    conn: Arc<Connection>,
    id: i64,
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
            insert(&self.conn, [self.id]);
        }
        false
    }
}

/// A TRUNCATE empties the file of a log that grows in steps, its passes after the first, which
/// run as passive ones, included: here a commit after its first pass's snapshot leaves it a
/// second pass to run.
#[test]
fn test_a_truncate_empties_the_file_of_a_log_that_grows_in_steps() {
    let (db, conn) = open();
    insert(&conn, 1..=20);
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(log(&db), (0, 0));

    insert(&conn, 21..=40);
    let checkpointer = connect(&db);
    let injector = Arc::new(CommitAfterSnapshot {
        conn: conn.clone(),
        id: 1000,
        fired: AtomicBool::new(false),
    });
    checkpointer.set_yield_injector(Some(injector.clone()));
    checkpointer
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    checkpointer.set_yield_injector(None);
    assert!(injector.fired.load(Ordering::Acquire));
    assert_eq!(log(&db), (0, 0), "the TRUNCATE left the log's file its size");
    assert_eq!(ids(&conn).len(), 41);
}

/// A passive checkpoint beside the commit of row `id` after its snapshot, which it cannot cover.
fn checkpoint_beside_a_commit(checkpointer: &Arc<Connection>, writer: &Arc<Connection>, id: i64) {
    let injector = Arc::new(CommitAfterSnapshot {
        conn: writer.clone(),
        id,
        fired: AtomicBool::new(false),
    });
    checkpointer.set_yield_injector(Some(injector.clone()));
    passive(checkpointer);
    checkpointer.set_yield_injector(None);
    assert!(injector.fired.load(Ordering::Acquire));
}

/// The sizes of the files of the log's slots, by slot.
fn slot_sizes(db: &MvccTestDbNoConn) -> Vec<u64> {
    let base = std::path::Path::new(&db.get_db().path)
        .with_extension("db-log")
        .to_string_lossy()
        .into_owned();
    (0..LOG_SEGMENT_FILES)
        .map(|slot| std::fs::metadata(segment_path(&base, slot)).map_or(0, |m| m.len()))
        .collect()
}

/// A log that rolls, as passive checkpoints beside commits make it, grows the file it rolls into in
/// steps too, and recovery reads the segments as one log.
#[test]
fn test_the_file_a_log_that_grows_in_steps_rolls_into_grows_in_steps() {
    let (mut db, conn) = open();
    insert(&conn, 1..=20);
    let checkpointer = connect(&db);
    checkpoint_beside_a_commit(&checkpointer, &conn, 1000);
    insert(&conn, 21..=40);
    checkpoint_beside_a_commit(&checkpointer, &conn, 1001);
    insert(&conn, 41..=60);
    assert_eq!(db.get_mvcc_store().storage.logical_log_segments(), 2);
    let sizes = slot_sizes(&db);
    assert_eq!(
        sizes.iter().filter(|&&size| size == STEP).count(),
        2,
        "the two segments' files are not a step each: {sizes:?}"
    );
    drop(checkpointer);
    let conn = crash(&mut db, conn);
    let mut expected: Vec<i64> = (1..=60).collect();
    expected.extend([1000, 1001]);
    assert_eq!(ids(&conn), expected);
}

/// VACUUM asks whether the log holds a frame no checkpoint covered, not whether its file is empty:
/// a log restarted in place holds none in a file that keeps its size.
#[test]
fn test_vacuum_runs_beside_a_log_restarted_in_place() {
    let (db, conn) = open_with(DatabaseOpts::new().with_vacuum(true));
    insert(&conn, 1..=20);
    assert!(db.get_mvcc_store().has_uncheckpointed_log().unwrap());
    passive(&conn);
    assert_eq!(log(&db), (STEP, 0));
    assert!(!db.get_mvcc_store().has_uncheckpointed_log().unwrap());
    conn.execute("VACUUM").unwrap();
    assert_eq!(ids(&conn), (1..=20).collect::<Vec<_>>());
}

/// Memory files that record the syncs asked of each, and tell a file's size at any moment.
#[derive(Default)]
struct CountingIo {
    inner: MemoryIO,
    files: Mutex<HashMap<String, Arc<CountingFile>>>,
}

impl CountingIo {
    fn file(&self, path: &str) -> Arc<CountingFile> {
        self.files.lock().get(path).expect("no such file").clone()
    }

    /// The kinds of the syncs asked of `path`'s file, in order.
    fn syncs(&self, path: &str) -> Vec<FileSyncType> {
        self.file(path).syncs.lock().clone()
    }

    fn size(&self, path: &str) -> u64 {
        self.file(path).size().unwrap()
    }
}

impl Clock for CountingIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for CountingIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let mut files = self.files.lock();
        if let Some(file) = files.get(path) {
            return Ok(file.clone());
        }
        let file = Arc::new(CountingFile {
            inner: self.inner.open_file(path, flags, direct)?,
            syncs: Mutex::new(Vec::new()),
        });
        files.insert(path.to_string(), file.clone());
        Ok(file)
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.files.lock().remove(path);
        self.inner.remove_file(path)
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
}

struct CountingFile {
    inner: Arc<dyn File>,
    syncs: Mutex<Vec<FileSyncType>>,
}

impl File for CountingFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        self.inner.pwritev(pos, buffers, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.syncs.lock().push(sync_type);
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }
}

/// A passive MVCC database on counting memory files, with a table `t`.
fn open_counting(path: &str) -> (Arc<Database>, Arc<CountingIo>, Arc<Connection>) {
    let io = Arc::new(CountingIo::default());
    let db = Database::open_file_with_flags(
        io.clone(),
        path,
        OpenFlags::default(),
        DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("PRAGMA synchronous = FULL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    (db, io, conn)
}

/// A log that grows in steps syncs only its data, which commits no journal while its frames fit,
/// where the platform has such a sync; without steps it syncs as the database does.
#[test]
fn test_a_log_that_grows_in_steps_syncs_only_its_data() {
    let data_sync = if cfg!(any(target_os = "linux", target_os = "android")) {
        FileSyncType::DataSync
    } else {
        FileSyncType::Fsync
    };
    for (growth, expected) in [(STEP, data_sync), (0, FileSyncType::Fsync)] {
        let path = format!("sync-{growth}.db");
        let (_db, io, conn) = open_counting(&path);
        conn.execute(format!("PRAGMA mvcc_log_growth = {growth}"))
            .unwrap();
        let log = format!("{path}-log");
        let before = io.syncs(&log).len();
        insert(&conn, 1..=3);
        let syncs = io.syncs(&log)[before..].to_vec();
        assert!(!syncs.is_empty(), "the commits synced no log");
        assert!(
            syncs.iter().all(|sync| *sync == expected),
            "growth {growth}: {syncs:?}"
        );
    }
}

/// The step reads back what was set, starts at 0, and refuses what it cannot be.
#[test]
fn test_the_log_growth_step_reads_back_and_refuses_what_it_cannot_be() {
    let (_db, io, conn) = open_counting("settings.db");
    let read = || -> i64 {
        let mut stmt = conn.prepare("PRAGMA mvcc_log_growth").unwrap();
        let mut value = None;
        stmt.run_with_row_callback(|row| {
            value = Some(row.get::<i64>(0).unwrap());
            Ok(())
        })
        .unwrap();
        value.unwrap()
    };
    assert_eq!(read(), 0);
    conn.execute("PRAGMA mvcc_log_growth = 16777216").unwrap();
    assert_eq!(read(), 16777216);
    for refused in ["-1", "3", "4097", "2147483648"] {
        assert!(
            conn.execute(format!("PRAGMA mvcc_log_growth = {refused}"))
                .is_err(),
            "{refused} was taken"
        );
    }
    assert_eq!(read(), 16777216);
    insert(&conn, 1..=2);
    assert_eq!(io.size("settings.db-log"), 16777216);
}
