//! A log replicator through the Rust binding: a database opened in MVCC mode
//! gives it each commit's frame, and a replicator that is full makes the
//! writer wait without blocking the runtime, until a task of the
//! replicator's own makes room, as one waiting on a Raft quorum does. A commit
//! it refuses fails, and the connection that applies replicated commits is
//! not asked. A checkpoint writes through the replicator's cut, where the
//! database reopens.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use turso::core::{Completion, LogReplicator, ReplicatedFrame};
use turso::{Builder, Value};

const DELAY: Duration = Duration::from_millis(100);

/// Keeps each frame and makes room for the next after `DELAY`, from a task
/// of its own; refuses commits while `refuse` is set.
#[derive(Default)]
struct Later {
    frames: Mutex<Vec<ReplicatedFrame>>,
    refuse: AtomicBool,
}

impl LogReplicator for Later {
    fn takes_commits(&self) -> bool {
        !self.refuse.load(Ordering::SeqCst)
    }

    fn replicate(&self, frame: ReplicatedFrame) -> Completion {
        self.frames.lock().unwrap().push(frame);
        let room = Completion::new_remote_wait();
        let made = room.clone();
        tokio::spawn(async move {
            tokio::time::sleep(DELAY).await;
            made.complete(0);
        });
        room
    }
}

async fn ids(conn: &turso::Connection) -> Vec<i64> {
    let mut rows = conn
        .query("SELECT id FROM things ORDER BY id", ())
        .await
        .unwrap();
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        match row.get_value(0).unwrap() {
            Value::Integer(n) => ids.push(n),
            other => panic!("{other:?}"),
        }
    }
    ids
}

/// One thread: the replicator's task runs only while the commit waits
/// without blocking it.
#[tokio::test(flavor = "current_thread")]
async fn a_full_log_replicator_paces_the_writer_without_blocking_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let replicator = Arc::new(Later::default());
    let db = Builder::new_local(dir.path().join("t.db").to_str().unwrap())
        .with_mvcc(true)
        .with_log_replicator(replicator.clone())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    conn.execute(
        "CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)",
        (),
    )
    .await
    .unwrap();
    let started = Instant::now();
    conn.execute("INSERT INTO things VALUES (1, 'one')", ())
        .await
        .unwrap();
    assert!(
        started.elapsed() >= DELAY,
        "the commit did not wait for room"
    );
    assert_eq!(ids(&conn).await, [1]);
    let frames = replicator.frames.lock().unwrap().clone();
    assert_eq!(frames.len(), 2, "the table and the row");
    assert!(
        frames[1].bytes.windows(6).any(|w| w == b"things"),
        "the row's frame does not name its table"
    );

    replicator.refuse.store(true, Ordering::SeqCst);
    let refused = conn
        .execute("INSERT INTO things VALUES (2, 'refused')", ())
        .await;
    assert!(refused.is_err(), "a refused commit succeeded");
    let applier = db.connect().unwrap();
    applier.set_applies_replicated_commits(true);
    applier
        .execute("INSERT INTO things VALUES (3, 'from the leader')", ())
        .await
        .unwrap();
    assert_eq!(ids(&conn).await, [1, 3]);
    assert_eq!(replicator.frames.lock().unwrap().len(), 2);
    let applied = applier
        .last_commit_ts()
        .expect("the applier's commit has a timestamp");
    assert!(applied > replicator.frames.lock().unwrap()[1].commit_ts);
}

#[tokio::test]
async fn a_log_replicator_needs_mvcc() {
    let dir = tempfile::tempdir().unwrap();
    let built = Builder::new_local(dir.path().join("t.db").to_str().unwrap())
        .with_log_replicator(Arc::new(Later::default()))
        .build()
        .await;
    assert!(
        built.is_err(),
        "a log replicator was installed on a WAL database"
    );
}

/// Keeps each frame of at most `LIMIT` bytes.
#[derive(Default)]
struct Limited {
    frames: Mutex<Vec<ReplicatedFrame>>,
}

const LIMIT: usize = 4096;

impl LogReplicator for Limited {
    fn takes_commits(&self) -> bool {
        true
    }

    fn replicate(&self, frame: ReplicatedFrame) -> Completion {
        self.frames.lock().unwrap().push(frame);
        Completion::new_yield()
    }

    fn frame_limit(&self) -> usize {
        LIMIT
    }
}

/// A commit whose frame is larger than the replicator takes fails as too
/// big, and the next one goes through.
#[tokio::test]
async fn a_commit_over_the_replicator_s_limit_fails_as_too_big() {
    let dir = tempfile::tempdir().unwrap();
    let replicator = Arc::new(Limited::default());
    let db = Builder::new_local(dir.path().join("t.db").to_str().unwrap())
        .with_mvcc(true)
        .with_log_replicator(replicator.clone())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    conn.execute(
        "CREATE TABLE things (id INTEGER PRIMARY KEY, body BLOB)",
        (),
    )
    .await
    .unwrap();
    let refused = conn
        .execute("INSERT INTO things VALUES (1, zeroblob(8192))", ())
        .await;
    assert!(
        matches!(refused, Err(turso::Error::TooBig(_))),
        "{refused:?}"
    );
    conn.execute("INSERT INTO things VALUES (2, zeroblob(100))", ())
        .await
        .unwrap();
    assert_eq!(ids(&conn).await, [2]);
    let frames = replicator.frames.lock().unwrap();
    assert_eq!(frames.len(), 2, "the table and the row taken");
    assert!(frames.iter().all(|frame| frame.bytes.len() <= LIMIT));
}

/// Keeps each frame and gives checkpoints `cut`, when set.
#[derive(Default)]
struct Cutting {
    frames: Mutex<Vec<ReplicatedFrame>>,
    cut: Mutex<Option<u64>>,
}

impl LogReplicator for Cutting {
    fn takes_commits(&self) -> bool {
        true
    }

    fn replicate(&self, frame: ReplicatedFrame) -> Completion {
        self.frames.lock().unwrap().push(frame);
        Completion::new_yield()
    }

    fn checkpoint_cut(&self, newest: u64) -> u64 {
        self.cut.lock().unwrap().unwrap_or(newest)
    }
}

async fn replicated(path: &std::path::Path, replicator: Arc<Cutting>) -> turso::Database {
    Builder::new_local(path.to_str().unwrap())
        .with_mvcc(true)
        .with_log_replicator(replicator)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_database_with_a_log_replicator_reopens_at_its_cut() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db");
    let replicator = Arc::new(Cutting::default());
    let db = replicated(&path, replicator.clone()).await;
    let conn = db.connect().unwrap();
    conn.execute(
        "CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT)",
        (),
    )
    .await
    .unwrap();
    conn.execute("INSERT INTO things VALUES (1, 'at the cut')", ())
        .await
        .unwrap();
    let cut = replicator.frames.lock().unwrap().last().unwrap().commit_ts;
    conn.execute("INSERT INTO things VALUES (2, 'after the cut')", ())
        .await
        .unwrap();
    *replicator.cut.lock().unwrap() = Some(cut);
    let mut checkpoint = conn
        .query("PRAGMA wal_checkpoint(PASSIVE)", ())
        .await
        .unwrap();
    while checkpoint.next().await.unwrap().is_some() {}
    drop(checkpoint);
    assert_eq!(db.mvcc_checkpointed_through().unwrap(), Some(cut));
    assert_eq!(ids(&conn).await, [1, 2]);
    drop(conn);
    drop(db);

    let db = replicated(&path, Arc::new(Cutting::default())).await;
    let conn = db.connect().unwrap();
    assert_eq!(ids(&conn).await, [1]);
    assert_eq!(db.mvcc_checkpointed_through().unwrap(), Some(cut));
}
