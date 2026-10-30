//! The logical log rolls under writes that never pause: a passive checkpoint that has to keep
//! frames, as every one does while commits land after its snapshot, seals the segment being
//! written and the log goes on in another file, which a later checkpoint frees. Recovery reads the
//! segments as one log.

use crate::mvcc::database::checkpoint_state_machine::CheckpointYieldPoint;
use crate::mvcc::database::tests::MvccTestDbNoConn;
use crate::mvcc::persistent_storage::logical_log::{segment_path, LOG_SEGMENT_FILES};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::storage::wal::CheckpointMode;
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::Arc;
use crate::Connection;

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
