//! A commit replicator through the Rust binding: a commit waits for a
//! replicator that finishes it later from a task of its own, as one that
//! waits for a Raft quorum does, and a commit it fails is rolled back.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso::core::{CommitReplicator, Completion, CompletionError, ReplicatedCommit};
use turso::{Builder, Value};

const DELAY: Duration = Duration::from_millis(100);

/// Finishes each commit after `DELAY`, from a task of its own: with an error
/// while `fail` is set.
#[derive(Default)]
struct Later {
    fail: AtomicBool,
    pages: AtomicUsize,
}

impl CommitReplicator for Later {
    fn replicate(&self, commit: ReplicatedCommit) -> Completion {
        self.pages.fetch_add(commit.pages.len(), Ordering::SeqCst);
        let fail = self.fail.load(Ordering::SeqCst);
        let waiting = Completion::new_remote_wait();
        let done = waiting.clone();
        tokio::spawn(async move {
            tokio::time::sleep(DELAY).await;
            if fail {
                done.error(CompletionError::Aborted);
            } else {
                done.complete(0);
            }
        });
        waiting
    }
}

async fn count(conn: &turso::Connection) -> i64 {
    let mut rows = conn.query("SELECT count(*) FROM t", ()).await.unwrap();
    match rows.next().await.unwrap().unwrap().get_value(0).unwrap() {
        Value::Integer(n) => n,
        other => panic!("{other:?}"),
    }
}

/// One thread: the replicator's task runs only while the statement waits
/// without blocking it.
#[tokio::test(flavor = "current_thread")]
async fn a_commit_waits_for_its_replicator_without_blocking_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let replicator = Arc::new(Later::default());
    let db = Builder::new_local(dir.path().join("t.db").to_str().unwrap())
        .with_commit_replicator(replicator.clone())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x)", ()).await.unwrap();
    let started = Instant::now();
    conn.execute("INSERT INTO t VALUES (1)", ()).await.unwrap();
    assert!(
        started.elapsed() >= DELAY,
        "the commit waited for its replicator"
    );
    assert_eq!(count(&conn).await, 1);
    assert!(replicator.pages.load(Ordering::SeqCst) > 0);

    replicator.fail.store(true, Ordering::SeqCst);
    assert!(conn.execute("INSERT INTO t VALUES (2)", ()).await.is_err());
    assert_eq!(count(&conn).await, 1, "the failed commit was rolled back");
    replicator.fail.store(false, Ordering::SeqCst);
    conn.execute("INSERT INTO t VALUES (3)", ()).await.unwrap();
    assert_eq!(count(&conn).await, 2);
}

#[tokio::test]
async fn the_engine_s_database_is_the_binding_s() {
    let dir = tempfile::tempdir().unwrap();
    let db = Builder::new_local(dir.path().join("t.db").to_str().unwrap())
        .with_commit_replicator(Arc::new(Later::default()))
        .build()
        .await
        .unwrap();
    // It took the builder's replicator, and takes no second one.
    assert!(db
        .core()
        .unwrap()
        .set_commit_replicator(Arc::new(Later::default()))
        .is_err());
}
