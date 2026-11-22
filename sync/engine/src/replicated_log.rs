//! Applying a database's MVCC logical log to another database, frame by
//! frame, as the first database's [`turso_core::LogReplicator`] receives it.
//!
//! Each frame names the tables its transaction wrote, in its portable
//! changes, so the database that applies it finds each table by its name, as
//! this engine replays the log a sync client pulls: row by row, through SQL.
//! The applying database's own writes keep its indexes, so a frame's index
//! operations are left out, and so are the rows of internal tables
//! (`sqlite_*`, `__turso_internal_*`).
//!
//! The frames of one call are applied as one transaction of the applying
//! database, on a connection that applies replicated commits: its own log
//! replicator neither refuses them nor receives their frames.

use std::collections::HashMap;
use std::sync::Arc;

use turso_core::{ConnectOpts, Connection, Database, ReplicatedFrame};

use crate::database_replay_generator::DatabaseReplayGenerator;
use crate::database_sync_operations::{
    decode_mvcc_frame_txns, logical_txn_to_tape_operations, parse_mvcc_frame,
};
use crate::database_tape::{DatabaseReplaySession, DatabaseReplaySessionOpts};
use crate::errors::Error;
use crate::types::Coro;
use crate::Result;

/// Applies another database's frames to one database.
pub struct ReplicatedLogApplier {
    replay: DatabaseReplaySession,
}

impl ReplicatedLogApplier {
    /// An applier to `db`, on a connection of its own.
    pub fn new(db: &Arc<Database>) -> Result<Self> {
        let conn = db.connect(ConnectOpts::new())?;
        conn.set_applies_replicated_commits(true);
        Ok(Self {
            replay: DatabaseReplaySession {
                conn: conn.clone(),
                cached_delete_stmt: HashMap::new(),
                cached_insert_stmt: HashMap::new(),
                cached_update_stmt: HashMap::new(),
                in_txn: false,
                generator: DatabaseReplayGenerator::new(
                    conn,
                    // Each row keeps the rowid it has in the database it came
                    // from, so that a later frame's rowid finds it.
                    DatabaseReplaySessionOpts {
                        use_implicit_rowid: true,
                    },
                ),
            },
        })
    }

    /// The connection the frames are applied on.
    pub fn conn(&self) -> &Arc<Connection> {
        &self.replay.conn
    }

    /// Applies the transactions of `frames`, in order, as one transaction of
    /// this database, and returns its commit timestamp here: `None` if the
    /// frames changed nothing this database keeps, so that it committed
    /// nothing. Each frame's CRC must continue from the CRC it says comes
    /// before it. On an error, nothing of the frames is applied.
    pub fn apply(&mut self, frames: &[ReplicatedFrame]) -> Result<Option<u64>> {
        let before = self.replay.conn.last_commit_ts();
        self.replay.conn.execute("BEGIN IMMEDIATE")?;
        self.replay.in_txn = true;
        let applied = drive(|coro| {
            let replay = &mut self.replay;
            async move {
                for frame in frames {
                    apply_frame(&coro, replay, frame).await?;
                }
                Ok(())
            }
        });
        self.replay.in_txn = false;
        if let Err(err) = applied {
            self.replay.conn.execute("ROLLBACK")?;
            return Err(err);
        }
        self.replay.conn.execute("COMMIT")?;
        let after = self.replay.conn.last_commit_ts();
        Ok(after.filter(|_| after != before))
    }
}

async fn apply_frame(
    coro: &Coro<()>,
    replay: &mut DatabaseReplaySession,
    frame: &ReplicatedFrame,
) -> Result<()> {
    let parsed = parse_mvcc_frame(&frame.bytes, 0, Some(frame.crc_before))?;
    if parsed.end != frame.bytes.len() {
        return Err(Error::DatabaseSyncEngineError(format!(
            "a replicated frame of {} bytes ends at {}",
            frame.bytes.len(),
            parsed.end
        )));
    }
    let Some(changes) = parsed.changes else {
        return Err(Error::DatabaseSyncEngineError(
            "a replicated frame has no portable changes, so its tables cannot be named".to_string(),
        ));
    };
    for txn in decode_mvcc_frame_txns(
        &changes.portable_payload,
        changes.recovery_payload,
        changes.op_count,
    )? {
        for operation in logical_txn_to_tape_operations(&txn)? {
            replay.replay(coro, operation).await?;
        }
    }
    Ok(())
}

/// Runs `task` to its end on a blocking coroutine, whose statements wait for their own IO in
/// place (`Coro::wait_for`), so nothing here steps the IO in a loop of its own, which spins on a
/// backend whose step does nothing (DESIGN.md 22.5).
fn drive<'a, F>(task: impl FnOnce(Coro<()>) -> F) -> Result<()>
where
    F: std::future::Future<Output = Result<()>> + 'a,
{
    let mut gen = genawaiter::sync::Gen::new(|co| task(Coro::blocking(co)));
    match gen.resume_with(Ok(())) {
        genawaiter::GeneratorState::Complete(result) => result,
        genawaiter::GeneratorState::Yielded(..) => Err(Error::DatabaseSyncEngineError(
            "a replay yielded for IO it waits for in place".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::TempDir;
    use turso_core::{
        Completion, Database, DatabaseOpts, LogReplicator, OpenFlags, ReplicatedFrame,
        SqliteDialect, Value,
    };

    use super::ReplicatedLogApplier;

    /// Keeps each frame of a leader's commits; a follower's takes none.
    struct Recorder {
        frames: Mutex<Vec<ReplicatedFrame>>,
        takes_commits: bool,
    }

    impl LogReplicator for Recorder {
        fn takes_commits(&self) -> bool {
            self.takes_commits
        }

        fn replicate(&self, frame: ReplicatedFrame) -> Completion {
            self.frames.lock().unwrap().push(frame);
            Completion::new_yield()
        }
    }

    fn open(dir: &TempDir, name: &str, takes_commits: bool) -> (Arc<Database>, Arc<Recorder>) {
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            dir.path().join(name).to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new()
                .with_mvcc(true)
                .with_experimental_mvcc_passive_checkpoint(true)
                .with_mvcc_log_dropped_on_open(true)
                .with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let recorder = Arc::new(Recorder {
            frames: Mutex::new(Vec::new()),
            takes_commits,
        });
        db.set_log_replicator(recorder.clone()).unwrap();
        (db, recorder)
    }

    fn rows(db: &Arc<Database>, sql: &str) -> Vec<Vec<Value>> {
        let conn = db.connect(turso_core::ConnectOpts::new()).unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push(row.get_values().cloned().collect());
            Ok(())
        })
        .unwrap();
        rows
    }

    fn frames(recorder: &Recorder) -> Vec<ReplicatedFrame> {
        std::mem::take(&mut *recorder.frames.lock().unwrap())
    }

    const CHECKS: &[&str] = &[
        "SELECT rowid, * FROM things ORDER BY rowid",
        "SELECT name, id FROM things INDEXED BY things_by_name ORDER BY name",
        "SELECT path FROM files WHERE path_sync(path, seq, '[\"a/**\"]', 0) ORDER BY path",
        "SELECT rowid, * FROM files ORDER BY rowid",
        "PRAGMA user_version",
    ];

    #[test]
    fn a_follower_that_applies_a_leader_s_frames_holds_its_rows_and_its_indexes_find_them() {
        let dir = tempfile::tempdir().unwrap();
        let (leader, recorder) = open(&dir, "leader.db", true);
        let (follower, _) = open(&dir, "follower.db", false);
        let conn = leader.connect(turso_core::ConnectOpts::new()).unwrap();
        for sql in [
            "CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT UNIQUE, n INTEGER)",
            "CREATE INDEX things_by_name ON things (name)",
            "CREATE TABLE files (path TEXT, kind INTEGER, seq INTEGER)",
            "CREATE INDEX files_by_path ON files USING path_trie (path, seq)",
            "INSERT INTO things VALUES (1, 'one', 1), (2, 'two', 2), (3, 'three', 3)",
            "INSERT INTO files VALUES ('a/x.rs', 1, 1), ('a/b/y.rs', 1, 2), ('b/z.rs', 1, 3)",
        ] {
            conn.execute(sql).unwrap();
        }
        let mut applier = ReplicatedLogApplier::new(&follower).unwrap();
        let first = applier.apply(&frames(&recorder)).unwrap();
        assert!(first.is_some(), "the first frames committed nothing");
        assert_eq!(applier.conn().last_commit_ts(), first);
        for sql in [
            "UPDATE things SET name = 'uno' WHERE id = 1",
            "DELETE FROM things WHERE id = 2",
            "UPDATE things SET n = n + 10",
            "DELETE FROM files WHERE path = 'a/x.rs'",
            "INSERT INTO files VALUES ('a/c/w.rs', 1, 4)",
            "PRAGMA user_version = 7",
        ] {
            conn.execute(sql).unwrap();
        }
        let second = applier.apply(&frames(&recorder)).unwrap();
        assert!(second > first, "{second:?} after {first:?}");
        for sql in CHECKS {
            assert_eq!(rows(&follower, sql), rows(&leader, sql), "{sql}");
        }
        assert_eq!(
            rows(&follower, CHECKS[2]),
            vec![
                vec![Value::build_text("a/b/y.rs")],
                vec![Value::build_text("a/c/w.rs")]
            ]
        );
        assert_eq!(applier.apply(&[]).unwrap(), None);
    }

    /// A table keyed by other than the rowid: the follower's rowids need not
    /// be the leader's, so each delete and update reaches it by the row's
    /// key, whether the row was still in the log or already checkpointed.
    #[test]
    fn a_follower_finds_rows_keyed_by_other_than_the_rowid_by_their_keys() {
        let dir = tempfile::tempdir().unwrap();
        let (leader, recorder) = open(&dir, "leader.db", true);
        let (follower, _) = open(&dir, "follower.db", false);
        let conn = leader.connect(turso_core::ConnectOpts::new()).unwrap();
        for sql in [
            "CREATE TABLE keyed (id TEXT PRIMARY KEY, v TEXT)",
            "INSERT INTO keyed VALUES ('a', 'one'), ('b', 'two'), ('c', 'three')",
        ] {
            conn.execute(sql).unwrap();
        }
        let mut applier = ReplicatedLogApplier::new(&follower).unwrap();
        applier.apply(&frames(&recorder)).unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        for sql in [
            "INSERT INTO keyed VALUES ('d', 'four'), ('e', 'five')",
            "UPDATE keyed SET v = 'TWO' WHERE id = 'b'",
            "DELETE FROM keyed WHERE id = 'c'",
            "DELETE FROM keyed WHERE id = 'e'",
        ] {
            conn.execute(sql).unwrap();
        }
        applier.apply(&frames(&recorder)).unwrap();
        let all = "SELECT id, v FROM keyed ORDER BY id";
        assert_eq!(rows(&follower, all), rows(&leader, all));
        assert_eq!(
            rows(&follower, all),
            vec![
                vec![Value::build_text("a"), Value::build_text("one")],
                vec![Value::build_text("b"), Value::build_text("TWO")],
                vec![Value::build_text("d"), Value::build_text("four")],
            ]
        );
    }

    /// Over an IO that finishes nothing until it is stepped, the frames apply: the replay's
    /// statements wait for their own IO in place, which steps such a backend, and nothing steps
    /// it in a loop of its own. The follower's rows are in its file, its pages read cold, so the
    /// replay's updates and deletes wait for their reads.
    #[test]
    fn a_follower_on_an_io_that_finishes_nothing_until_stepped_applies_the_frames() {
        let dir = tempfile::tempdir().unwrap();
        let (leader, recorder) = open(&dir, "leader.db", true);
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryYieldIO::new());
        let follower = || {
            Database::open_file_with_flags(
                io.clone(),
                "follower.db",
                OpenFlags::default(),
                DatabaseOpts::new()
                    .with_mvcc(true)
                    .with_experimental_mvcc_passive_checkpoint(true)
                    .with_mvcc_log_dropped_on_open(true)
                    .with_index_method(true),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap()
        };
        let conn = leader.connect(turso_core::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE things (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        conn.execute(
            "INSERT INTO things SELECT value, printf('%0200d', value) \
             FROM generate_series(1, 3000)",
        )
        .unwrap();
        // Applied on a thread, so that a replay that never waits fails the test, not hangs it.
        let apply = |db: Arc<Database>, frames: Vec<ReplicatedFrame>| {
            let (done, applied) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let mut applier = ReplicatedLogApplier::new(&db).unwrap();
                let _ = done.send(applier.apply(&frames).map(|ts| ts.is_some()));
            });
            applied
                .recv_timeout(std::time::Duration::from_secs(60))
                .expect("the frames never applied")
                .unwrap()
        };
        let db = follower();
        assert!(apply(db.clone(), frames(&recorder)));
        let checkpoint = db.connect(turso_core::ConnectOpts::new()).unwrap();
        checkpoint
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(checkpoint);
        drop(db);

        conn.execute("UPDATE things SET body = 'changed' WHERE id % 97 = 0")
            .unwrap();
        conn.execute("DELETE FROM things WHERE id % 89 = 0")
            .unwrap();
        let db = follower();
        assert!(apply(db.clone(), frames(&recorder)));
        let all = "SELECT id, body FROM things ORDER BY id";
        assert_eq!(rows(&db, all), rows(&leader, all));
        assert_eq!(rows(&db, all).len(), 3000 - 3000 / 89);
    }

    #[test]
    fn frames_with_one_whose_crc_does_not_continue_apply_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (leader, recorder) = open(&dir, "leader.db", true);
        let (follower, _) = open(&dir, "follower.db", false);
        let conn = leader.connect(turso_core::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO things VALUES (1, 'one')")
            .unwrap();
        let mut applier = ReplicatedLogApplier::new(&follower).unwrap();
        let mut sent = frames(&recorder);
        sent[1].crc_before ^= 1;
        assert!(applier.apply(&sent).is_err(), "a broken frame was applied");
        let tables = "SELECT name FROM sqlite_schema WHERE name = 'things'";
        assert_eq!(rows(&follower, tables), Vec::<Vec<Value>>::new());
        sent[1].crc_before ^= 1;
        applier.apply(&sent).unwrap();
        assert_eq!(
            rows(&follower, "SELECT * FROM things"),
            vec![vec![Value::from_i64(1), Value::build_text("one")]]
        );
    }

    #[test]
    fn a_frame_that_is_not_one_whole_frame_naming_its_tables_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (leader, recorder) = open(&dir, "leader.db", true);
        let (follower, _) = open(&dir, "follower.db", false);
        let conn = leader.connect(turso_core::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO things VALUES (1, 'one')")
            .unwrap();
        let sent = frames(&recorder);
        let mut applier = ReplicatedLogApplier::new(&follower).unwrap();

        // Two frames sent as one: the second would be lost.
        let joined = ReplicatedFrame {
            commit_ts: sent[1].commit_ts,
            bytes: [&sent[0].bytes[..], &sent[1].bytes[..]].concat().into(),
            crc_before: sent[0].crc_before,
        };
        assert!(
            applier.apply(&[joined]).is_err(),
            "two frames as one applied"
        );

        // A frame of a database that keeps no portable changes.
        let plain = Database::open_file_with_flags(
            Arc::new(turso_core::PlatformIO::new().unwrap()),
            dir.path().join("plain.db").to_str().unwrap(),
            OpenFlags::default(),
            DatabaseOpts::new().with_mvcc(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        plain
            .connect(turso_core::ConnectOpts::new())
            .unwrap()
            .execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        let log = std::fs::read(dir.path().join("plain.db-log")).unwrap();
        let unnamed = ReplicatedFrame {
            commit_ts: 0,
            bytes: log[56..].into(),
            // A log's CRC chain starts from its salt's.
            crc_before: crc32c::crc32c(&log[8..16]),
        };
        assert!(
            applier.apply(&[unnamed]).is_err(),
            "an unnamed frame applied"
        );
        applier.apply(&sent).unwrap();
    }
}
