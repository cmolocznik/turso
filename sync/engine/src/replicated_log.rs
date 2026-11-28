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
//! replicator neither refuses them nor receives their frames. The transaction
//! is concurrent, so other writers of the database, such as a follower's own
//! index merge, go on beside it; frames that change the schema or the header
//! are applied alone, under the write lock, as every schema change is made.

use std::collections::HashMap;
use std::sync::Arc;

use turso_core::{ConnectOpts, Connection, Database, ReplicatedFrame};

use crate::client_proto::{LogicalOpType, LogicalTxnData};
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
    /// A test's, run while a call's transaction is open, before it commits.
    #[cfg(test)]
    before_commit: Option<Box<dyn FnMut() + Send>>,
}

impl ReplicatedLogApplier {
    /// An applier to `db`, on a connection of its own.
    pub fn new(db: &Arc<Database>) -> Result<Self> {
        let conn = db.connect(ConnectOpts::new())?;
        conn.set_applies_replicated_commits(true);
        Ok(Self {
            #[cfg(test)]
            before_commit: None,
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
    /// before it. On an error, nothing of the frames is applied, and the
    /// connection is in no transaction. Another transaction may refuse it,
    /// nothing committed: one that holds the database's write lock, with
    /// `Busy`, and one that wrote a row the frames write, with
    /// `WriteWriteConflict`, `BusySnapshot` or `Conflict`. The caller tries
    /// the same frames again once it is free.
    pub fn apply(&mut self, frames: &[ReplicatedFrame]) -> Result<Option<u64>> {
        self.apply_decoded(&DecodedFrames::decode(frames)?)
    }

    /// [`Self::apply`], of frames decoded already.
    pub fn apply_decoded(&mut self, frames: &DecodedFrames) -> Result<Option<u64>> {
        let before = self.replay.conn.last_commit_ts();
        let begin = if frames.changes_schema() {
            "BEGIN IMMEDIATE"
        } else {
            "BEGIN CONCURRENT"
        };
        self.replay.conn.execute(begin)?;
        self.replay.in_txn = true;
        let applied = drive(|coro| {
            let replay = &mut self.replay;
            async move {
                for txn in &frames.txns {
                    // A run of a transaction's inserts into one table as one statement, as the
                    // leader wrote them: an index that writes a statement's additions as one
                    // segment keeps as few here.
                    replay
                        .replay_all(&coro, logical_txn_to_tape_operations(txn)?)
                        .await?;
                }
                Ok(())
            }
        });
        self.replay.in_txn = false;
        #[cfg(test)]
        if let (Ok(()), Some(before_commit)) = (&applied, self.before_commit.as_mut()) {
            before_commit();
        }
        if let Err(err) = applied {
            self.end_refused()?;
            return Err(err);
        }
        if let Err(err) = self.replay.conn.execute("COMMIT") {
            self.end_refused()?;
            return Err(err.into());
        }
        let after = self.replay.conn.last_commit_ts();
        Ok(after.filter(|_| after != before))
    }

    /// Ends the transaction a refusal left open, so the next frames begin one
    /// of their own; some refusals end it themselves.
    fn end_refused(&self) -> Result<()> {
        if !self.replay.conn.get_auto_commit() {
            self.replay.conn.execute("ROLLBACK")?;
        }
        Ok(())
    }
}

/// Frames decoded to apply: each transaction they hold, in order.
pub struct DecodedFrames {
    txns: Vec<LogicalTxnData>,
}

impl DecodedFrames {
    /// Decodes `frames`, checking that each frame's CRC continues from the CRC
    /// it says comes before it.
    pub fn decode(frames: &[ReplicatedFrame]) -> Result<Self> {
        let mut txns = Vec::new();
        for frame in frames {
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
                    "a replicated frame has no portable changes, so its tables cannot be named"
                        .to_string(),
                ));
            };
            txns.extend(decode_mvcc_frame_txns(
                &changes.portable_payload,
                changes.recovery_payload,
                changes.op_count,
            )?);
        }
        Ok(Self { txns })
    }

    /// Whether applying them changes the schema or the database header, which
    /// only a transaction holding the write lock makes.
    pub fn changes_schema(&self) -> bool {
        self.txns.iter().flat_map(|txn| &txn.ops).any(|op| {
            op.op_type == LogicalOpType::Schema as i32
                || op.op_type == LogicalOpType::UpdateHeader as i32
        })
    }
}

/// Runs `task` to its end on a blocking coroutine, whose statements wait for their own IO in
/// place (`Coro::wait_for`), so nothing here steps the IO in a loop of its own, which spins on a
/// backend whose step does nothing.
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

    use super::{DecodedFrames, ReplicatedLogApplier};

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

    /// Another connection's transaction that holds the follower's write
    /// lock refuses the applier's writes, as a conflict, and nothing is
    /// committed: the connection is in no transaction, and the same frames
    /// apply once when tried again after the lock is free.
    #[test]
    fn frames_refused_by_another_s_write_lock_apply_once_when_tried_again() {
        let dir = tempfile::tempdir().unwrap();
        let (leader, recorder) = open(&dir, "leader.db", true);
        let (follower, _) = open(&dir, "follower.db", false);
        let conn = leader.connect(turso_core::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE log (v TEXT)").unwrap();
        let mut applier = ReplicatedLogApplier::new(&follower).unwrap();
        applier.apply(&frames(&recorder)).unwrap();
        conn.execute("INSERT INTO log VALUES ('once')").unwrap();
        let pending = frames(&recorder);
        let holder = follower.connect(turso_core::ConnectOpts::new()).unwrap();
        holder.execute("BEGIN IMMEDIATE").unwrap();
        let before = applier.conn().last_commit_ts();
        let refused = applier.apply(&pending);
        assert!(
            matches!(
                refused,
                Err(crate::errors::Error::TursoError(
                    turso_core::LimboError::Busy | turso_core::LimboError::WriteWriteConflict
                ))
            ),
            "{refused:?}"
        );
        assert!(applier.conn().get_auto_commit());
        assert_eq!(applier.conn().last_commit_ts(), before);
        holder.execute("ROLLBACK").unwrap();
        let applied = applier.apply(&pending).unwrap();
        assert!(applied > before, "{applied:?} after {before:?}");
        assert_eq!(
            rows(&follower, "SELECT v FROM log"),
            vec![vec![Value::build_text("once")]]
        );
    }

    /// A writer of the follower's own, as its index merge is, beside the
    /// applier: one that writes other rows goes on beside it, and one that
    /// writes a row the frames write refuses them whole, nothing committed,
    /// until it ends.
    #[test]
    fn frames_apply_beside_another_writer_and_wait_for_one_of_their_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (leader, recorder) = open(&dir, "leader.db", true);
        let (follower, _) = open(&dir, "follower.db", false);
        let conn = leader.connect(turso_core::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE own (v TEXT)").unwrap();
        conn.execute("INSERT INTO log VALUES (1, 'first')").unwrap();
        let mut applier = ReplicatedLogApplier::new(&follower).unwrap();
        let schema = frames(&recorder);
        assert!(DecodedFrames::decode(&schema).unwrap().changes_schema());
        // A writer that commits while a call's transaction is open: beside rows, never beside a
        // schema change.
        let during = Arc::new(Mutex::new(Vec::new()));
        let other = follower.connect(turso_core::ConnectOpts::new()).unwrap();
        other.set_applies_replicated_commits(true);
        other.execute("CREATE TABLE noted (v TEXT)").unwrap();
        let commits = |applier: &mut ReplicatedLogApplier| {
            let (other, during) = (other.clone(), during.clone());
            applier.before_commit = Some(Box::new(move || {
                let made = other
                    .execute("BEGIN CONCURRENT")
                    .and_then(|()| other.execute("INSERT INTO noted VALUES ('during')"))
                    .and_then(|()| other.execute("COMMIT"));
                if !other.get_auto_commit() {
                    other.execute("ROLLBACK").unwrap();
                }
                during.lock().unwrap().push(made.is_ok());
            }));
        };
        commits(&mut applier);
        applier.apply(&schema).unwrap();
        conn.execute("INSERT INTO log VALUES (4, 'fourth')")
            .unwrap();
        commits(&mut applier);
        applier.apply(&frames(&recorder)).unwrap();
        applier.before_commit = None;
        assert_eq!(*during.lock().unwrap(), vec![false, true]);

        // Frames that fail to replay leave nothing and no transaction.
        let (bare, _) = open(&dir, "bare.db", false);
        let mut elsewhere = ReplicatedLogApplier::new(&bare).unwrap();
        conn.execute("INSERT INTO log VALUES (5, 'fifth')").unwrap();
        let fifth = frames(&recorder);
        assert!(elsewhere.apply(&fifth).is_err());
        assert!(elsewhere.conn().get_auto_commit());
        applier.apply(&fifth).unwrap();

        let writer = follower.connect(turso_core::ConnectOpts::new()).unwrap();
        writer.set_applies_replicated_commits(true);
        writer.execute("BEGIN CONCURRENT").unwrap();
        writer.execute("INSERT INTO own VALUES ('mine')").unwrap();
        conn.execute("INSERT INTO log VALUES (2, 'second')")
            .unwrap();
        let pending = frames(&recorder);
        assert!(!DecodedFrames::decode(&pending).unwrap().changes_schema());
        applier.apply(&pending).unwrap();
        writer.execute("COMMIT").unwrap();

        writer.execute("BEGIN CONCURRENT").unwrap();
        writer
            .execute("UPDATE log SET v = 'held' WHERE id = 2")
            .unwrap();
        conn.execute("UPDATE log SET v = 'leader' WHERE id = 2")
            .unwrap();
        conn.execute("INSERT INTO log VALUES (3, 'third')").unwrap();
        let pending = frames(&recorder);
        let before = applier.conn().last_commit_ts();
        let refused = applier.apply(&pending);
        assert!(
            matches!(
                refused,
                Err(crate::errors::Error::TursoError(
                    turso_core::LimboError::WriteWriteConflict
                        | turso_core::LimboError::BusySnapshot
                        | turso_core::LimboError::Conflict(_)
                        | turso_core::LimboError::Busy
                ))
            ),
            "{refused:?}"
        );
        assert!(applier.conn().get_auto_commit());
        assert_eq!(applier.conn().last_commit_ts(), before);
        assert_eq!(
            rows(&follower, "SELECT count(*) FROM log"),
            vec![vec![Value::from_i64(4)]]
        );
        writer.execute("ROLLBACK").unwrap();
        applier.apply(&pending).unwrap();
        for sql in ["SELECT * FROM log ORDER BY id", "SELECT * FROM own"] {
            let expected = if sql.contains("own") {
                vec![vec![Value::build_text("mine")]]
            } else {
                rows(&leader, sql)
            };
            assert_eq!(rows(&follower, sql), expected, "{sql}");
        }
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

    /// A statement that adds texts to a trigram index writes one segment of them, so a
    /// follower that applied each row as a statement of its own would hold a segment per row,
    /// which its queries read one by one. Applied, the leader's statement is one segment on the
    /// follower too: a merge there, as on the leader, finds no level of eight to merge.
    #[test]
    fn a_follower_s_index_holds_as_few_segments_as_its_leader_s() {
        let dir = tempfile::tempdir().unwrap();
        let (leader, recorder) = open(&dir, "leader.db", true);
        let (follower, _) = open(&dir, "follower.db", false);
        let conn = leader.connect(turso_core::ConnectOpts::new()).unwrap();
        for sql in [
            "CREATE TABLE docs (path TEXT PRIMARY KEY, text TEXT)",
            "CREATE INDEX docs_by_trigram ON docs USING trigram (text)",
            "CREATE TABLE plain (id INTEGER PRIMARY KEY, text TEXT)",
            "CREATE INDEX plain_by_trigram ON plain USING trigram (text)",
            "INSERT INTO docs SELECT 'p' || value, 'fn f' || value || '() {}' \
             FROM generate_series(1, 20)",
            "INSERT INTO plain SELECT value, 'struct S' || value FROM generate_series(1, 20)",
            // More rows than one statement binds values for: a run in two statements.
            "CREATE TABLE many (id INTEGER PRIMARY KEY, n INTEGER)",
            "INSERT INTO many SELECT value, value * 7 FROM generate_series(1, 20000)",
        ] {
            conn.execute(sql).unwrap();
        }
        let mut applier = ReplicatedLogApplier::new(&follower).unwrap();
        applier.apply(&frames(&recorder)).unwrap();
        let merged = |conn: &Arc<turso_core::Connection>, index: &str| {
            let before = conn.last_commit_ts();
            conn.execute(format!("OPTIMIZE INDEX {index}")).unwrap();
            conn.last_commit_ts() != before
        };
        for (index, check) in [
            (
                "docs_by_trigram",
                "SELECT path FROM docs WHERE code_match(text, 'f1[0-9]\\(') ORDER BY path",
            ),
            (
                "plain_by_trigram",
                "SELECT id FROM plain WHERE code_match(text, 'S1[0-9]') ORDER BY id",
            ),
        ] {
            assert!(!merged(&conn, index), "the leader merged {index}");
            assert!(
                !merged(applier.conn(), index),
                "the follower merged {index}"
            );
            assert_eq!(rows(&follower, check), rows(&leader, check), "{check}");
            assert_eq!(rows(&follower, check).len(), 10, "{check}");
        }
        let many = "SELECT id, n FROM many ORDER BY id";
        assert_eq!(rows(&follower, many), rows(&leader, many));
        assert_eq!(rows(&follower, many).len(), 20000);
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
