//! A commit replicator receives each WAL commit's pages before the commit is
//! visible. A follower given those pages, applied through the WAL insert API,
//! holds the leader's database; a reader never sees a commit the replicator
//! has not let through; and a commit it fails is rolled back.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use turso_core::{CommitReplicator, Completion, CompletionError, Connection, ReplicatedCommit};

use crate::common::{compute_dbhash, limbo_exec_rows, TempDatabase};

const PAGE: usize = 4096;

/// Keeps every commit, and lets each through at once unless told to fail
/// commit number `fail` (counted from 0), or to hold them.
#[derive(Default)]
struct Recorder {
    commits: Mutex<Vec<ReplicatedCommit>>,
    fail: Mutex<Option<usize>>,
    seen: AtomicUsize,
    hold: Mutex<bool>,
    held: Mutex<Vec<Completion>>,
}

impl CommitReplicator for Recorder {
    fn replicate(&self, commit: ReplicatedCommit) -> Completion {
        let n = self.seen.fetch_add(1, Ordering::SeqCst);
        if *self.fail.lock().unwrap() == Some(n) {
            let failed = Completion::new_remote_wait();
            failed.error(CompletionError::Aborted);
            return failed;
        }
        self.commits.lock().unwrap().push(commit);
        if *self.hold.lock().unwrap() {
            let waiting = Completion::new_remote_wait();
            self.held.lock().unwrap().push(waiting.clone());
            return waiting;
        }
        Completion::new_yield()
    }
}

fn leader() -> (TempDatabase, Arc<Recorder>) {
    let db = TempDatabase::new_empty();
    let recorder = Arc::new(Recorder::default());
    db.db.set_commit_replicator(recorder.clone()).unwrap();
    (db, recorder)
}

/// The follower's image of a page: zeros past its end, as the leader's image
/// of a page its transaction adds.
fn follower_page(conn: &Arc<Connection>, number: u32, db_size: u32) -> Vec<u8> {
    let mut page = vec![0u8; PAGE];
    if number <= db_size
        && !conn
            .try_wal_watermark_read_page(number, &mut page, None)
            .unwrap()
    {
        page.fill(0);
    }
    page
}

fn follower_db_size(conn: &Arc<Connection>) -> u32 {
    let mut first = vec![0u8; PAGE];
    if !conn
        .try_wal_watermark_read_page(1, &mut first, None)
        .unwrap()
    {
        return 0;
    }
    u32::from_be_bytes(first[28..32].try_into().unwrap())
}

/// Applies a commit's pages to a follower as one transaction, after checking
/// that each page's image before is the follower's. The session's start
/// initializes a new follower's page 1, as the leader's first write did
/// before its first commit.
fn apply(conn: &Arc<Connection>, commit: &ReplicatedCommit) {
    conn.wal_insert_begin().unwrap();
    let db_size = follower_db_size(conn);
    for page in &commit.pages {
        assert_eq!(
            follower_page(conn, page.number, db_size),
            &page.before[..],
            "page {}'s image before the commit is the follower's",
            page.number
        );
    }
    let mut next = conn.wal_state().unwrap().max_frame + 1;
    for (i, page) in commit.pages.iter().enumerate() {
        let mut frame = vec![0u8; 24 + PAGE];
        frame[0..4].copy_from_slice(&page.number.to_be_bytes());
        if i + 1 == commit.pages.len() {
            frame[4..8].copy_from_slice(&commit.db_size.to_be_bytes());
        }
        frame[24..].copy_from_slice(&page.after);
        conn.wal_insert_frame(next, &frame).unwrap();
        next += 1;
    }
    conn.wal_insert_end(false).unwrap();
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<rusqlite::types::Value>> {
    limbo_exec_rows(conn, sql)
}

#[test]
fn a_follower_given_each_commit_s_pages_holds_the_leader_s_database() {
    let (leader, recorder) = leader();
    let conn = leader.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, body BLOB)")
        .unwrap();
    conn.execute("CREATE INDEX t_name ON t(name)").unwrap();
    for i in 0..200 {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'row {i}', randomblob({}))",
            10 + i * 7
        ))
        .unwrap();
    }
    // Overflow pages, and a transaction too big for the page cache, so some
    // of its pages are spilled before it commits.
    conn.execute("INSERT INTO t VALUES (1000, 'big', randomblob(4096 * 20))")
        .unwrap();
    conn.execute("PRAGMA cache_size = 16").unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 2000..3000 {
        conn.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'spill {i}', randomblob(300))"
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    // A savepoint rolled back inside a transaction that commits.
    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET name = 'kept' WHERE id = 1")
        .unwrap();
    conn.execute("SAVEPOINT s").unwrap();
    conn.execute("DELETE FROM t WHERE id < 100").unwrap();
    conn.execute("ROLLBACK TO s").unwrap();
    conn.execute("COMMIT").unwrap();
    // A rolled-back transaction is never replicated.
    let before = recorder.commits.lock().unwrap().len();
    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t").unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(recorder.commits.lock().unwrap().len(), before);
    conn.execute("DELETE FROM t WHERE id % 3 = 0").unwrap();
    conn.execute("UPDATE t SET body = NULL WHERE id = 1000")
        .unwrap();
    conn.execute("DROP INDEX t_name").unwrap();

    let follower = TempDatabase::new_empty();
    let fconn = follower.connect_limbo();
    for commit in recorder.commits.lock().unwrap().iter() {
        apply(&fconn, commit);
    }
    let query = "SELECT id, name, length(body), hex(body) FROM t ORDER BY id";
    assert_eq!(rows(&fconn, query), rows(&conn, query));
    assert_eq!(
        rows(&fconn, "PRAGMA integrity_check"),
        rows(&conn, "PRAGMA integrity_check")
    );
    drop((conn, fconn));
    assert_eq!(compute_dbhash(&follower).hash, compute_dbhash(&leader).hash);
}

#[test]
fn a_commit_is_seen_only_once_its_replicator_lets_it_through() {
    let (leader, recorder) = leader();
    let conn = leader.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    *recorder.hold.lock().unwrap() = true;
    let writer = {
        let conn = leader.connect_limbo();
        std::thread::spawn(move || conn.execute("INSERT INTO t VALUES (1)"))
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while recorder.held.lock().unwrap().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the replicator never received the commit"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    // The commit's frames are written and synced, and still no reader sees
    // them.
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM t"),
        [[rusqlite::types::Value::Integer(0)]]
    );
    assert!(!writer.is_finished(), "the writer waits for its replicator");
    recorder.held.lock().unwrap()[0].complete(0);
    writer.join().unwrap().unwrap();
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM t"),
        [[rusqlite::types::Value::Integer(1)]]
    );
}

#[test]
fn a_commit_its_replicator_fails_is_rolled_back() {
    let (leader, recorder) = leader();
    let conn = leader.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    *recorder.fail.lock().unwrap() = Some(recorder.seen.load(Ordering::SeqCst));
    assert!(conn.execute("INSERT INTO t VALUES (2)").is_err());
    assert_eq!(
        rows(&conn, "SELECT x FROM t"),
        [[rusqlite::types::Value::Integer(1)]]
    );
    let other = leader.connect_limbo();
    assert_eq!(
        rows(&other, "SELECT x FROM t"),
        [[rusqlite::types::Value::Integer(1)]]
    );
    // The next commit goes through, and a follower of the commits the
    // replicator took holds the same database.
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    let follower = TempDatabase::new_empty();
    let fconn = follower.connect_limbo();
    for commit in recorder.commits.lock().unwrap().iter() {
        apply(&fconn, commit);
    }
    assert_eq!(
        rows(&fconn, "SELECT x FROM t ORDER BY x"),
        rows(&conn, "SELECT x FROM t ORDER BY x")
    );
    assert_eq!(
        rows(&fconn, "PRAGMA integrity_check"),
        rows(&conn, "PRAGMA integrity_check")
    );
}

/// A crash right after a commit its replicator failed leaves no trace of it:
/// the commit's frames reach the WAL only once the replicator lets it through.
#[test]
fn a_commit_its_replicator_fails_is_not_in_what_a_crash_leaves() {
    let (leader, recorder) = leader();
    let conn = leader.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    *recorder.fail.lock().unwrap() = Some(recorder.seen.load(Ordering::SeqCst));
    assert!(conn.execute("INSERT INTO t VALUES (2)").is_err());
    // The files as they are now, with the database still open: what a crash
    // would leave.
    let dir = tempfile::tempdir().unwrap();
    let copy = dir.path().join("crashed.db");
    std::fs::copy(&leader.path, &copy).unwrap();
    let wal = leader.path.with_extension("db-wal");
    std::fs::copy(&wal, copy.with_extension("db-wal")).unwrap();
    let crashed = TempDatabase::new_with_existent(&copy);
    let reopened = crashed.connect_limbo();
    assert_eq!(
        rows(&reopened, "SELECT x FROM t"),
        [[rusqlite::types::Value::Integer(1)]]
    );
}

#[test]
fn a_database_takes_one_replicator() {
    let (leader, _) = leader();
    assert!(leader
        .db
        .set_commit_replicator(Arc::new(Recorder::default()))
        .is_err());
}
