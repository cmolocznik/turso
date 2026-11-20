//! A log replicator receives each committed transaction's MVCC logical-log
//! frame once it is written, in log order, naming its tables; a commit it
//! refuses fails with nothing written; a connection that applies replicated
//! commits is neither asked nor replicated; a replicator that is full makes
//! the writer wait; and a checkpoint writes the database file only through the
//! replicator's cut, which is where the database reopens, its log dropped.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use turso_core::{Completion, DatabaseOpts, LimboError, LogReplicator, ReplicatedFrame};

use crate::common::{limbo_exec_rows, TempDatabase};

/// Keeps every frame; refuses commits while `refuse` is set, holds each
/// frame's completion while `hold` is, and gives `cut` to checkpoints when set.
#[derive(Default)]
struct Recorder {
    frames: Mutex<Vec<ReplicatedFrame>>,
    refuse: AtomicBool,
    hold: AtomicBool,
    held: Mutex<Vec<Completion>>,
    cut: Mutex<Option<u64>>,
}

impl Recorder {
    /// The commit timestamp of the last frame received.
    fn last_commit_ts(&self) -> u64 {
        self.frames.lock().unwrap().last().unwrap().commit_ts
    }

    fn cut_at(&self, cut: Option<u64>) {
        *self.cut.lock().unwrap() = cut;
    }
}

impl LogReplicator for Recorder {
    fn takes_commits(&self) -> bool {
        !self.refuse.load(Ordering::SeqCst)
    }

    fn replicate(&self, frame: ReplicatedFrame) -> Completion {
        self.frames.lock().unwrap().push(frame);
        if self.hold.load(Ordering::SeqCst) {
            let waiting = Completion::new_remote_wait();
            self.held.lock().unwrap().push(waiting.clone());
            return waiting;
        }
        Completion::new_yield()
    }

    fn checkpoint_cut(&self, newest: u64) -> u64 {
        self.cut.lock().unwrap().unwrap_or(newest)
    }
}

/// What a database a log replicator replicates is opened with.
fn replicated() -> DatabaseOpts {
    DatabaseOpts::new()
        .with_mvcc(true)
        .with_experimental_mvcc_passive_checkpoint(true)
        .with_mvcc_log_dropped_on_open(true)
        .with_index_method(true)
}

/// The database at `path`, new or opened again, replicated by `recorder`.
fn open_replicated(path: &Path, recorder: &Arc<Recorder>) -> TempDatabase {
    let db = TempDatabase::builder()
        .with_db_path(path)
        .with_opts(replicated())
        .build();
    db.db.set_log_replicator(recorder.clone()).unwrap();
    db
}

/// A new database named `name`, replicated by a new recorder.
fn leader(name: &str) -> (TempDatabase, Arc<Recorder>) {
    let db = TempDatabase::builder()
        .with_db_name(name)
        .with_opts(replicated())
        .build();
    let recorder = Arc::new(Recorder::default());
    db.db.set_log_replicator(recorder.clone()).unwrap();
    (db, recorder)
}

fn logical_log(db: &TempDatabase) -> Vec<u8> {
    std::fs::read(db.path.with_extension("db-log")).unwrap()
}

/// The frames are the log's last bytes, in order; each names its own commit
/// timestamp, which no other has; and each frame's CRC chain starts where the
/// one before ended. Group commit writes concurrent transactions' frames in
/// the order they reached its queue, which need not be their timestamps'.
fn assert_frames_are_the_log(db: &TempDatabase, frames: &[ReplicatedFrame]) {
    let written: Vec<u8> = frames
        .iter()
        .flat_map(|f| f.bytes.iter().copied())
        .collect();
    let log = logical_log(db);
    assert!(
        log.len() >= written.len() + 56,
        "the frames are longer than the log"
    );
    assert!(
        log.ends_with(&written),
        "the frames are not the log's bytes as written"
    );
    let mut timestamps = std::collections::HashSet::new();
    for frame in frames {
        // A frame's header: magic (4), payload size (8), op count (4), commit timestamp (8).
        let in_header = u64::from_le_bytes(frame.bytes[16..24].try_into().unwrap());
        assert_eq!(
            frame.commit_ts, in_header,
            "a frame's commit timestamp is not its own"
        );
        assert!(
            timestamps.insert(frame.commit_ts),
            "two frames of one commit timestamp"
        );
    }
    for pair in frames.windows(2) {
        let trailer = &pair[0].bytes[pair[0].bytes.len() - 8..];
        let crc_after = u32::from_le_bytes(trailer[..4].try_into().unwrap());
        assert_eq!(
            pair[1].crc_before, crc_after,
            "a frame's CRC chain does not continue"
        );
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn each_commit_s_frame_reaches_the_replicator_as_written_and_names_its_table() {
    let (db, recorder) = leader("log_replicator_frames.db");
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    for i in 0..3 {
        conn.execute(format!("INSERT INTO things VALUES ({i}, 'thing {i}')"))
            .unwrap();
    }
    let frames = recorder.frames.lock().unwrap().clone();
    assert_eq!(frames.len(), 4, "the table and three rows");
    assert_frames_are_the_log(&db, &frames);
    for frame in &frames[1..] {
        assert!(
            contains(&frame.bytes, b"things"),
            "a row's frame does not name its table"
        );
    }

    // A checkpoint empties the log; the next write is its first, header and
    // frame together, and the replicator still gets the frame alone.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute("INSERT INTO things VALUES (3, 'after')")
        .unwrap();
    let after = recorder.frames.lock().unwrap().last().unwrap().clone();
    let log = logical_log(&db);
    assert_eq!(
        log.len(),
        56 + after.bytes.len(),
        "the log is its header and one frame"
    );
    assert_eq!(&log[56..], &after.bytes[..]);
}

#[test]
fn concurrent_commits_reach_the_replicator_in_log_order() {
    let (db, recorder) = leader("log_replicator_concurrent.db");
    db.connect_limbo()
        .execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let db = Arc::new(db);
    let (writers, each) = (8, 25);
    let threads: Vec<_> = (0..writers)
        .map(|w| {
            let db = db.clone();
            std::thread::spawn(move || {
                let conn = db.connect_limbo();
                for i in 0..each {
                    conn.execute("BEGIN CONCURRENT").unwrap();
                    conn.execute(format!(
                        "INSERT INTO things VALUES ({}, 'w{w}')",
                        w * each + i
                    ))
                    .unwrap();
                    conn.execute("COMMIT").unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let frames = recorder.frames.lock().unwrap().clone();
    assert_eq!(frames.len(), 1 + writers * each);
    assert_frames_are_the_log(&db, &frames);
}

#[test]
fn a_commit_the_replicator_refuses_fails_and_leaves_nothing() {
    let (db, recorder) = leader("log_replicator_refused.db");
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    let log_before = logical_log(&db);
    recorder.refuse.store(true, Ordering::SeqCst);
    let refused = conn.execute("INSERT INTO things VALUES (1, 'refused')");
    assert!(matches!(refused, Err(LimboError::ReadOnly)), "{refused:?}");
    assert_eq!(
        logical_log(&db),
        log_before,
        "a refused commit wrote to the log"
    );
    assert_eq!(recorder.frames.lock().unwrap().len(), 1);
    // A read still runs, and sees nothing of it.
    let rows = limbo_exec_rows(&conn, "SELECT count(*) FROM things");
    assert_eq!(rows, vec![vec![rusqlite::types::Value::Integer(0)]]);

    recorder.refuse.store(false, Ordering::SeqCst);
    conn.execute("INSERT INTO things VALUES (2, 'taken')")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT id FROM things");
    assert_eq!(rows, vec![vec![rusqlite::types::Value::Integer(2)]]);
    assert_eq!(recorder.frames.lock().unwrap().len(), 2);
}

#[test]
fn a_connection_that_applies_replicated_commits_is_neither_asked_nor_replicated() {
    let (db, recorder) = leader("log_replicator_applier.db");
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    recorder.refuse.store(true, Ordering::SeqCst);
    let applier = db.connect_limbo();
    applier.set_applies_replicated_commits(true);
    applier
        .execute("INSERT INTO things VALUES (1, 'from the leader')")
        .unwrap();
    assert_eq!(
        recorder.frames.lock().unwrap().len(),
        1,
        "the applier's frame was replicated"
    );
    // Its row is in the log, and in the table.
    assert!(contains(&logical_log(&db), b"from the leader"));
    let rows = limbo_exec_rows(&conn, "SELECT id FROM things");
    assert_eq!(rows, vec![vec![rusqlite::types::Value::Integer(1)]]);
    // Every other connection is still refused.
    let refused = conn.execute("INSERT INTO things VALUES (2, 'refused')");
    assert!(matches!(refused, Err(LimboError::ReadOnly)), "{refused:?}");
}

#[test]
fn a_full_replicator_makes_the_writer_wait() {
    let (db, recorder) = leader("log_replicator_full.db");
    db.connect_limbo()
        .execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    recorder.hold.store(true, Ordering::SeqCst);
    let db = Arc::new(db);
    let (done, finished) = std::sync::mpsc::channel();
    let writer = {
        let db = db.clone();
        std::thread::spawn(move || {
            let result = db
                .connect_limbo()
                .execute("INSERT INTO things VALUES (1, 'waits')");
            done.send(result.is_ok()).unwrap();
        })
    };
    // Its frame is with the replicator, which holds it: the commit waits.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while recorder.held.lock().unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the frame never reached the replicator"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        finished.recv_timeout(Duration::from_millis(300)).is_err(),
        "the commit did not wait"
    );

    for held in recorder.held.lock().unwrap().drain(..) {
        held.complete(0);
    }
    assert_eq!(finished.recv_timeout(Duration::from_secs(10)), Ok(true));
    writer.join().unwrap();
}

/// A table with a `path_trie` index, whose method writes B-trees of its own:
/// every frame still names a table the object map knows, so its commits go
/// through with portable metadata on.
#[test]
fn a_path_trie_index_commits_under_a_log_replicator() {
    let (db, recorder) = leader("log_replicator_path_trie.db");
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE files (path TEXT, seq INTEGER, size INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX files_by_path ON files USING path_trie (path, seq)")
        .unwrap();
    for (i, path) in ["a/x.rs", "a/y.rs", "b/z.rs"].iter().enumerate() {
        conn.execute(format!("INSERT INTO files VALUES ('{path}', {i}, 1)"))
            .unwrap();
    }
    conn.execute("DELETE FROM files WHERE path = 'a/x.rs'")
        .unwrap();
    let frames = recorder.frames.lock().unwrap().clone();
    assert_frames_are_the_log(&db, &frames);
    let rows = frames
        .iter()
        .filter(|f| contains(&f.bytes, b"files"))
        .count();
    assert!(rows >= 4, "the rows' frames do not name their table");
}

fn rows(conn: &Arc<turso_core::Connection>) -> Vec<(i64, String)> {
    limbo_exec_rows(conn, "SELECT id, name FROM things ORDER BY id")
        .into_iter()
        .map(|row| match &row[..] {
            [rusqlite::types::Value::Integer(id), rusqlite::types::Value::Text(name)] => {
                (*id, name.clone())
            }
            other => panic!("{other:?}"),
        })
        .collect()
}

fn named(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
    rows.iter()
        .map(|(id, name)| (*id, name.to_string()))
        .collect()
}

/// A checkpoint writes the commits at or below the replicator's cut and none
/// after it, while the running database still sees them all. The database
/// reopens from its file: at the cut, its log dropped, and its clock going on
/// from there.
#[test]
fn a_checkpoint_writes_through_the_replicator_s_cut_where_the_database_reopens() {
    let (db, recorder) = leader("log_replicator_cut.db");
    let path = db.path.clone();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO things VALUES (1, 'at the cut')")
        .unwrap();
    let cut = recorder.last_commit_ts();
    conn.execute("INSERT INTO things VALUES (2, 'after the cut')")
        .unwrap();
    recorder.cut_at(Some(cut));
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    assert_eq!(db.db.mvcc_checkpointed_through(), Some(cut));
    assert_eq!(
        rows(&conn),
        named(&[(1, "at the cut"), (2, "after the cut")]),
        "the running database lost a commit above the cut"
    );
    drop(conn);
    drop(db);
    // The log is not read: one whose header is torn opens as well.
    let log = path.with_extension("db-log");
    let mut bytes = std::fs::read(&log).unwrap();
    bytes[..8].copy_from_slice(b"torn hdr");
    std::fs::write(&log, bytes).unwrap();

    let recorder = Arc::new(Recorder::default());
    let db = open_replicated(&path, &recorder);
    let conn = db.connect_limbo();
    assert_eq!(rows(&conn), named(&[(1, "at the cut")]));
    assert_eq!(db.db.mvcc_checkpointed_through(), Some(cut));
    assert!(
        logical_log(&db).len() <= 56,
        "the reopened database kept its log"
    );
    conn.execute("INSERT INTO things VALUES (3, 'replayed')")
        .unwrap();
    assert!(recorder.last_commit_ts() > cut);
    assert_eq!(rows(&conn), named(&[(1, "at the cut"), (3, "replayed")]));
}

/// A database no checkpoint has written reopens as its file is: empty, even
/// of the tables its log made, and whatever its log holds, a torn header too.
#[test]
fn a_database_never_checkpointed_reopens_empty_whatever_its_log_holds() {
    let (db, recorder) = leader("log_replicator_never_checkpointed.db");
    let path = db.path.clone();
    recorder.cut_at(Some(0));
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO things VALUES (1, 'in the log alone')")
        .unwrap();
    drop(conn);
    drop(db);
    let log = path.with_extension("db-log");
    let mut bytes = std::fs::read(&log).unwrap();
    assert!(bytes.len() > 56, "the log holds no frame");
    bytes[..8].copy_from_slice(b"torn hdr");
    std::fs::write(&log, bytes).unwrap();

    let db = open_replicated(&path, &Arc::new(Recorder::default()));
    let conn = db.connect_limbo();
    let tables = limbo_exec_rows(
        &conn,
        "SELECT name FROM sqlite_schema WHERE name = 'things'",
    );
    assert!(tables.is_empty(), "{tables:?}");
    assert_eq!(db.db.mvcc_checkpointed_through(), Some(0));
}

/// A row overwritten above the cut keeps, through garbage collection and
/// checkpoints at the cut, the version a checkpoint there writes; once the cut
/// passes the overwrite, the next checkpoint writes that.
fn an_overwritten_row_reopens_as(cut_passes_the_overwrite: bool) -> Vec<(i64, String)> {
    let name = format!("log_replicator_overwrite_{cut_passes_the_overwrite}.db");
    let (db, recorder) = leader(&name);
    let path = db.path.clone();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO things VALUES (1, 'at the cut')")
        .unwrap();
    recorder.cut_at(Some(recorder.last_commit_ts()));
    conn.execute("UPDATE things SET name = 'above the cut' WHERE id = 1")
        .unwrap();
    let store = db.db.get_mv_store().clone().unwrap();
    for _ in 0..2 {
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
        store.drop_unused_row_versions();
    }
    if cut_passes_the_overwrite {
        recorder.cut_at(None);
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    }
    drop(store);
    drop(conn);
    drop(db);
    let db = open_replicated(&path, &Arc::new(Recorder::default()));
    rows(&db.connect_limbo())
}

#[test]
fn a_row_overwritten_above_the_cut_reopens_with_its_value_at_the_cut() {
    assert_eq!(
        an_overwritten_row_reopens_as(false),
        named(&[(1, "at the cut")])
    );
}

#[test]
fn a_row_overwritten_above_the_cut_reopens_overwritten_once_the_cut_passes() {
    assert_eq!(
        an_overwritten_row_reopens_as(true),
        named(&[(1, "above the cut")])
    );
}

/// A log replicator is installed only on a database whose checkpoints run
/// beside writers and whose log is dropped on open.
#[test]
fn a_log_replicator_needs_passive_checkpoints_and_its_log_dropped_on_open() {
    let mvcc = DatabaseOpts::new().with_mvcc(true);
    for (name, opts) in [
        ("neither", mvcc),
        (
            "passive",
            mvcc.with_experimental_mvcc_passive_checkpoint(true),
        ),
        ("dropped", mvcc.with_mvcc_log_dropped_on_open(true)),
    ] {
        let db = TempDatabase::builder()
            .with_db_name(format!("log_replicator_needs_{name}.db"))
            .with_opts(opts)
            .build();
        let refused = db.db.set_log_replicator(Arc::new(Recorder::default()));
        assert!(
            matches!(refused, Err(LimboError::InvalidArgument(_))),
            "{name}: {refused:?}"
        );
    }
}
