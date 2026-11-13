//! First light (DESIGN.md §17, R33): Turso's SQL on an Atoll database through `Database::open`,
//! end to end.

use crate::io::{MemoryIO, OpenFlags, IO};
use crate::storage::atoll::checkpoint::{Checkpointing, WRITER_STEPS};
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::txn::arena::PAGES_TAKEN;
use crate::sync::Arc;
use crate::{ConnectOpts, Connection, Database, DatabaseOpts, OpenOptions, SqliteDialect, Value};

/// Each test opens its own path: the registry keys databases by their file across IOs.
fn open(io: &Arc<dyn IO>, path: &str) -> Arc<Database> {
    open_with(io, path, config())
}

fn open_with(
    io: &Arc<dyn IO>,
    path: &str,
    config: crate::storage::atoll::db::AtollConfig,
) -> Arc<Database> {
    Database::open(
        io.clone(),
        path,
        OpenOptions::new(Arc::new(SqliteDialect)).atoll(config),
    )
    .unwrap()
}

/// [`config`] with a checkpointing thread.
fn threaded() -> crate::storage::atoll::db::AtollConfig {
    crate::storage::atoll::db::AtollConfig {
        checkpointing: Checkpointing::Thread,
        ..config()
    }
}

/// Runs `test` on a thread of its own, failing if it is not done within a minute: what it waits
/// on is the checkpointing thread's work on a database in memory, far shorter, and a wait nothing
/// ends would otherwise hang the run.
fn within_a_minute(test: impl FnOnce() + Send + 'static) {
    within_a_minute_or("a wait on the checkpointing thread never ended", test)
}

/// [`within_a_minute`], failing with `never` if `test` is not done within it.
fn within_a_minute_or(never: &'static str, test: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    let runner = std::thread::spawn(move || {
        test();
        let _ = done.send(());
    });
    match finished.recv_timeout(std::time::Duration::from_secs(60)) {
        Ok(()) => runner.join().unwrap(),
        // The test panicked: its own message is the failure.
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => runner.join().unwrap(),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!("{never}"),
    }
}

/// Runs `test` on a thread of its own, which reports each step of its work that moved
/// (`progress`): `Err` once `quiet` passes with none, so a step that never ends, or one retried
/// without end, fails the test rather than hanging the run, however long its whole work takes on a
/// loaded host. Progress is one counter, so reports never queue.
fn watch_progress(
    quiet: std::time::Duration,
    test: impl FnOnce(&dyn Fn()) + Send + 'static,
) -> std::result::Result<(), String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    let progress = std::sync::Arc::new(AtomicU64::new(0));
    let (done, finished) = std::sync::mpsc::channel::<()>();
    let runner = {
        let progress = progress.clone();
        std::thread::spawn(move || {
            test(&|| {
                progress.fetch_add(1, Ordering::Relaxed);
            });
            let _ = done.send(());
        })
    };
    let mut seen = progress.load(Ordering::Relaxed);
    loop {
        match finished.recv_timeout(quiet) {
            // Done, or the test panicked: its own message is the failure.
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                runner.join().unwrap();
                return Ok(());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                let now = progress.load(Ordering::Relaxed);
                if now == seen {
                    return Err(format!("{quiet:?} passed with no step of the test moving"));
                }
                seen = now;
            }
        }
    }
}

/// [`watch_progress`] with a minute of quiet allowed, failing the test past it.
fn beating_within_a_minute(test: impl FnOnce(&dyn Fn()) + Send + 'static) {
    if let Err(e) = watch_progress(std::time::Duration::from_secs(60), test) {
        panic!("{e}");
    }
}

/// The watchdog fails a test whose steps never move however often they are retried, and keeps
/// alive one that moves, past its quiet bound, for as long as it does (audit chain-fixes-v1,
/// P2-CF2).
#[test]
fn the_progress_watchdog_fails_retries_that_never_move_and_keeps_a_moving_test() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let quiet = std::time::Duration::from_millis(100);
    // Retried, never moving, until refused: the watchdog answers at its first quiet bound. The
    // retries end by themselves only far past that, so a watchdog that never refuses lets them
    // finish and is caught as a failure, not a hang.
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let retries = {
        let stop = stop.clone();
        let unrefused = std::time::Instant::now() + quiet * 300;
        watch_progress(quiet, move |_progress| {
            while !stop.load(Ordering::Acquire) && std::time::Instant::now() < unrefused {
                std::thread::yield_now();
            }
        })
    };
    stop.store(true, Ordering::Release);
    assert!(retries.is_err(), "retries that never moved were let run");
    // Moving every 10 ms for five quiet bounds: kept alive to its end.
    let moving = watch_progress(quiet, |progress| {
        for _ in 0..50 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            progress();
        }
    });
    assert_eq!(moving, Ok(()), "a test that kept moving was refused");
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn names(conn: &Arc<Connection>) -> Vec<Value> {
    rows(conn, "SELECT b FROM t ORDER BY a")
        .into_iter()
        .map(|mut r| r.remove(0))
        .collect()
}

/// Whether tree `id` has a catalog row, as a transaction begun now reads it.
fn catalogued(db: &Database, id: u64) -> bool {
    use crate::storage::atoll::drive;
    use crate::storage::atoll::sql::catalog::key;
    use crate::storage::atoll::txn::versioned::Get;
    use crate::storage::atoll::txn::Isolation;
    let adb = db.atoll.as_ref().expect("Atoll mode");
    let txns = adb.atoll().transactions();
    let mut txn = drive(adb.io(), || txns.begin(Isolation::Snapshot)).unwrap();
    let mut get = Get::new(&key(id));
    let row = drive(adb.io(), || {
        adb.catalog().get(txns, adb.versions(), &mut txn, &mut get)
    })
    .unwrap();
    txns.abort(txn);
    row.is_some()
}

fn text(s: &str) -> Value {
    Value::build_text(s.to_string())
}

/// A table created, written, read, written in a transaction rolled back and in one committed,
/// then read again after the database is closed and opened again: every row Atoll's, the pager
/// holding none.
#[test]
fn first_light_creates_writes_reads_rolls_back_and_reopens() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    {
        let db = open(&io, "/db/first-light");
        assert!(db.atoll.is_some(), "the database opened in Atoll mode");
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t (b) VALUES ('x'), ('y')")
            .unwrap();
        assert_eq!(names(&conn), vec![text("x"), text("y")]);
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t (b) VALUES ('rolled back')")
            .unwrap();
        assert_eq!(names(&conn).len(), 3, "its own write");
        conn.execute("ROLLBACK").unwrap();
        assert_eq!(names(&conn), vec![text("x"), text("y")]);
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t (b) VALUES ('z')").unwrap();
        conn.execute("COMMIT").unwrap();
        assert_eq!(
            rows(&conn, "SELECT a, b FROM t ORDER BY a"),
            vec![
                vec![Value::from_i64(1), text("x")],
                vec![Value::from_i64(2), text("y")],
                vec![Value::from_i64(4), text("z")],
            ],
            "a rowid the rolled-back insert drew is not drawn again (§8)"
        );
        conn.close().unwrap();
    }
    let db = open(&io, "/db/first-light");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(names(&conn), vec![text("x"), text("y"), text("z")]);
    conn.execute("INSERT INTO t (b) VALUES ('after')").unwrap();
    assert_eq!(
        rows(&conn, "SELECT a FROM t WHERE b = 'after'"),
        vec![vec![Value::from_i64(5)]],
        "the counter seeded again from the tree"
    );
}

/// A database opened without the option, over a file Atoll's control file holds, opens in Atoll
/// mode; one with the option over a SQLite file is refused, and the file is left as it is.
#[test]
fn an_atoll_database_is_detected_and_a_sqlite_one_refused() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    {
        let db = open(&io, "/db/detected");
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
            .unwrap();
        conn.close().unwrap();
    }
    let db = Database::open(
        io.clone(),
        "/db/detected",
        OpenOptions::new(Arc::new(SqliteDialect)),
    )
    .unwrap();
    assert!(db.atoll.is_some(), "detected without the option");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    assert!(names(&conn).is_empty());
    drop(conn);
    drop(db);

    let sqlite = "/db/sqlite";
    {
        let db = Database::open(
            io.clone(),
            sqlite,
            OpenOptions::new(Arc::new(SqliteDialect)),
        )
        .unwrap();
        assert!(db.atoll.is_none());
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE s (a)").unwrap();
        conn.close().unwrap();
    }
    let err = Database::open(
        io.clone(),
        sqlite,
        OpenOptions::new(Arc::new(SqliteDialect)).atoll(config()),
    )
    .expect_err("a SQLite file opened in Atoll mode");
    assert!(err.to_string().contains("not an Atoll database"), "{err}");
    let db = Database::open(io, sqlite, OpenOptions::new(Arc::new(SqliteDialect))).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    assert!(
        rows(&conn, "SELECT a FROM s").is_empty(),
        "the SQLite file still opens"
    );
}

/// An empty main file beside a WAL that holds frames is a SQLite database whose rows are all in
/// its WAL: opened with the option, it is refused and neither file is written.
#[test]
fn an_empty_file_beside_a_wal_that_holds_anything_is_refused() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let path = "/db/wal-only";
    drop(io.open_file(path, OpenFlags::Create, false).unwrap());
    let wal = io
        .open_file(&format!("{path}-wal"), OpenFlags::Create, false)
        .unwrap();
    let frames = Arc::new(crate::io::Buffer::new(vec![7u8; 4096]));
    let c = crate::io::Completion::new_write(|_| {});
    io.wait_for_completion(wal.pwrite(0, frames, c).unwrap())
        .unwrap();
    let err = Database::open(
        io.clone(),
        path,
        OpenOptions::new(Arc::new(SqliteDialect)).atoll(config()),
    )
    .expect_err("a WAL's rows opened in Atoll mode");
    assert!(err.to_string().contains("not an Atoll database"), "{err}");
    let main = io.open_file(path, OpenFlags::ReadOnly, false).unwrap();
    assert_eq!(main.size().unwrap(), 0, "the main file was written");
    assert_eq!(wal.size().unwrap(), 4096, "the WAL was written");
}

/// A statement prepared before another connection's DDL is prepared again when it runs: the
/// reprepare reads the committed cookie and the schema through Atoll, never the pager.
#[test]
fn a_statement_prepared_before_another_connections_ddl_is_prepared_again() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/reprepare");
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    a.execute("INSERT INTO t (b) VALUES ('x')").unwrap();
    let mut stale = b.prepare("SELECT b FROM t").unwrap();
    a.execute("CREATE TABLE u (c)").unwrap();
    assert_eq!(stale.run_collect_rows().unwrap(), vec![vec![text("x")]]);
    assert!(rows(&b, "SELECT c FROM u").is_empty(), "the new table seen");
}

/// The main pager of a database in Atoll mode refuses its transactions by name: a site that
/// missed its Atoll arm fails, where with no WAL it would commit nothing and report success.
#[test]
fn the_main_pager_refuses_its_transactions() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/refusing");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let pager = conn.pager.load().clone();
    let err = pager.begin_read_tx().unwrap_err();
    assert!(err.to_string().contains("Atoll mode"), "{err}");
    let err = pager
        .begin_write_tx(crate::storage::wal::WalAutoActions::empty())
        .unwrap_err();
    assert!(err.to_string().contains("Atoll mode"), "{err}");
    let err = pager
        .commit_tx(&conn, conn.get_sync_mode(), false)
        .unwrap_err();
    assert!(err.to_string().contains("Atoll mode"), "{err}");
}

/// DDL and DML past first light: an index built over rows and read through, a UNIQUE index
/// refusing a second key, an UPDATE and a DELETE through the index, a whole-table DELETE that
/// clears the tree and counts its rows, and a DROP TABLE, each surviving a reopen.
#[test]
fn indexes_updates_deletes_clears_and_drops_reach_atoll() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    {
        let db = open(&io, "/db/ddl");
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT, c INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t (b, c) VALUES ('x', 3), ('y', 1), ('z', 2)")
            .unwrap();
        conn.execute("CREATE INDEX t_c ON t (c)").unwrap();
        conn.execute("CREATE UNIQUE INDEX t_b ON t (b)").unwrap();
        assert_eq!(
            rows(&conn, "SELECT b FROM t WHERE c >= 2 ORDER BY c"),
            vec![vec![text("z")], vec![text("x")]],
            "through the index on c"
        );
        let err = conn
            .execute("INSERT INTO t (b, c) VALUES ('x', 9)")
            .unwrap_err();
        assert!(err.to_string().contains("UNIQUE"), "{err}");
        conn.execute("UPDATE t SET c = 10 WHERE b = 'y'").unwrap();
        conn.execute("DELETE FROM t WHERE c = 2").unwrap();
        assert_eq!(
            rows(&conn, "SELECT b, c FROM t ORDER BY c"),
            vec![
                vec![text("x"), Value::from_i64(3)],
                vec![text("y"), Value::from_i64(10)]
            ]
        );
        assert_eq!(
            rows(&conn, "SELECT b FROM t WHERE c = 10"),
            vec![vec![text("y")]]
        );
        conn.execute("CREATE TABLE gone (a)").unwrap();
        conn.execute("INSERT INTO gone VALUES (1)").unwrap();
        let gone = rows(
            &conn,
            "SELECT rootpage FROM sqlite_schema WHERE name = 'gone'",
        );
        let Value::Numeric(crate::numeric::Numeric::Integer(gone)) = gone[0][0] else {
            panic!("a root page of {gone:?}");
        };
        assert!(catalogued(&db, gone as u64));
        conn.execute("DROP TABLE gone").unwrap();
        assert!(
            !catalogued(&db, gone as u64),
            "the dropped tree's catalog row"
        );
        conn.execute("CREATE TABLE cleared (a)").unwrap();
        conn.execute("INSERT INTO cleared VALUES (1), (2), (3)")
            .unwrap();
        conn.execute("DELETE FROM cleared").unwrap();
        assert_eq!(conn.changes(), 3, "the rows a clear removed");
        assert!(rows(&conn, "SELECT a FROM cleared").is_empty());
        conn.execute("INSERT INTO cleared VALUES (4)").unwrap();
        conn.close().unwrap();
    }
    let db = open(&io, "/db/ddl");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(
        rows(&conn, "SELECT b FROM t WHERE c = 3"),
        vec![vec![text("x")]]
    );
    assert_eq!(
        rows(&conn, "SELECT a FROM cleared"),
        vec![vec![Value::from_i64(4)]]
    );
    let err = conn
        .prepare("SELECT a FROM gone")
        .expect_err("a dropped table");
    assert!(err.to_string().contains("no such table"), "{err}");
}

/// Twelve connections on twelve threads, four transaction slots: each transaction adds one to a
/// shared row and logs itself, the second writer of the row retrying in place behind the first and
/// failing with a conflict once it commits, then retrying the transaction. No update is lost, every transaction's log row
/// is there once, and conflicts were met: the waits and slots were reached.
#[test]
fn twelve_writers_through_sql_lose_no_update() {
    const THREADS: i64 = 12;
    const ROUNDS: i64 = 25;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/twelve");
    let setup = db.connect(ConnectOpts::new()).unwrap();
    setup
        .execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    setup.execute("CREATE TABLE log (who, round)").unwrap();
    setup.execute("INSERT INTO counter VALUES (1, 0)").unwrap();
    let start = Arc::new(std::sync::Barrier::new(THREADS as usize));
    let threads: Vec<_> = (0..THREADS)
        .map(|who| {
            let (db, start) = (db.clone(), start.clone());
            std::thread::spawn(move || {
                let conn = db.connect(ConnectOpts::new()).unwrap();
                // A write behind an open writer parks until that writer ends, for as long as the
                // busy timeout (R34).
                conn.set_busy_timeout(std::time::Duration::from_secs(30));
                start.wait();
                let mut conflicts = 0;
                for round in 0..ROUNDS {
                    loop {
                        conn.execute("BEGIN").unwrap();
                        let done = conn
                            .execute("UPDATE counter SET n = n + 1 WHERE id = 1")
                            .and_then(|_| {
                                conn.execute(format!("INSERT INTO log VALUES ({who}, {round})"))
                            })
                            .and_then(|_| conn.execute("COMMIT"));
                        match done {
                            Ok(()) => break,
                            Err(crate::LimboError::WriteWriteConflict) => {
                                conflicts += 1;
                                // The conflict rolled the transaction back.
                                assert!(conn.get_auto_commit(), "a conflict left it open");
                            }
                            Err(e) => panic!("thread {who}, round {round}: {e}"),
                        }
                    }
                }
                conflicts
            })
        })
        .collect();
    let conflicts: i64 = threads.into_iter().map(|t| t.join().unwrap()).sum();
    assert_eq!(
        rows(&setup, "SELECT n FROM counter"),
        vec![vec![Value::from_i64(THREADS * ROUNDS)]],
        "an update lost"
    );
    assert_eq!(
        rows(
            &setup,
            "SELECT count(*), count(DISTINCT who * 1000 + round) FROM log"
        ),
        vec![vec![
            Value::from_i64(THREADS * ROUNDS),
            Value::from_i64(THREADS * ROUNDS)
        ]]
    );
    assert!(conflicts > 0, "no transaction met another's write");
    eprintln!("twelve writers: {conflicts} conflicts retried");
}

/// An autocommit UPDATE behind an open writer of its row retries in place, then meets the
/// writer's commit as a conflict and restarts with a new snapshot, as if it had run later: both
/// increments land (§3.2).
#[test]
fn an_autocommit_update_behind_a_writer_restarts_after_its_commit() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/restart");
    let a = db.connect(ConnectOpts::new()).unwrap();
    a.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    a.execute("INSERT INTO counter VALUES (1, 0)").unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("UPDATE counter SET n = n + 1 WHERE id = 1")
        .unwrap();
    let behind = {
        let db = db.clone();
        std::thread::spawn(move || {
            let b = db.connect(ConnectOpts::new()).unwrap();
            b.set_busy_timeout(std::time::Duration::from_secs(30));
            b.execute("UPDATE counter SET n = n + 1 WHERE id = 1")
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(200));
    a.execute("COMMIT").unwrap();
    behind.join().unwrap().unwrap();
    assert_eq!(
        rows(&a, "SELECT n FROM counter"),
        vec![vec![Value::from_i64(2)]]
    );
}

/// Twelve connections each running autocommit increments of one row: every one lands, behind
/// the others' writes and their commits, with no error reaching a caller (§3.2, R34).
#[test]
fn twelve_autocommit_incrementers_all_land() {
    const THREADS: i64 = 12;
    const ROUNDS: i64 = 25;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/incrementers");
    let setup = db.connect(ConnectOpts::new()).unwrap();
    setup
        .execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    setup.execute("INSERT INTO counter VALUES (1, 0)").unwrap();
    let start = Arc::new(std::sync::Barrier::new(THREADS as usize));
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let (db, start) = (db.clone(), start.clone());
            std::thread::spawn(move || {
                let conn = db.connect(ConnectOpts::new()).unwrap();
                conn.set_busy_timeout(std::time::Duration::from_secs(30));
                start.wait();
                for _ in 0..ROUNDS {
                    conn.execute("UPDATE counter SET n = n + 1 WHERE id = 1")
                        .unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(
        rows(&setup, "SELECT n FROM counter"),
        vec![vec![Value::from_i64(THREADS * ROUNDS)]]
    );
}

/// With no busy timeout, a write behind an open writer of its row is Busy at once, as SQLite's
/// lock is: the SQLite frontend never parks, so no wait outlasts the busy timeout (R34).
#[test]
fn a_write_behind_a_writer_with_no_busy_timeout_is_busy_at_once() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/busy");
    let a = db.connect(ConnectOpts::new()).unwrap();
    a.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    a.execute("INSERT INTO counter VALUES (1, 0)").unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("UPDATE counter SET n = n + 1 WHERE id = 1")
        .unwrap();
    let (sent, got) = std::sync::mpsc::channel();
    {
        let db = db.clone();
        std::thread::spawn(move || {
            let b = db.connect(ConnectOpts::new()).unwrap();
            let _ = sent.send(b.execute("UPDATE counter SET n = n + 1 WHERE id = 1"));
        });
    }
    let behind = got
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the write behind the writer parked");
    assert!(matches!(behind, Err(crate::LimboError::Busy)), "{behind:?}");
    a.execute("COMMIT").unwrap();
    assert_eq!(
        rows(&a, "SELECT n FROM counter"),
        vec![vec![Value::from_i64(1)]]
    );
}

/// Commits and writing statements step the checkpointer: after twenty times the log budget of
/// rows, the redo point trails the log's end by about the budget, as DESIGN §10 has it, plus what
/// was appended while a pass ran, so the log a recovery replays and keeps stays bounded (§11).
#[test]
fn writes_step_the_checkpointer_and_the_redo_point_follows_the_log() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/checkpointed");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    let budget = config().log_budget.expect("a log budget");
    let row = 8 << 10;
    for _ in 0..20 * budget / row {
        conn.execute(format!("INSERT INTO t (b) VALUES (zeroblob({row}))"))
            .unwrap();
    }
    let atoll = db.atoll.as_ref().unwrap().atoll();
    let (end, redo) = (atoll.log().end(), atoll.checkpointer().superblock().redo);
    eprintln!("log end {end}, redo point {redo}, lag {}", end - redo);
    assert!(
        end - redo <= 2 * budget,
        "the redo point trails the log's end by {} bytes, past twice the {budget} byte budget",
        end - redo
    );
}

/// One bulk statement appending twenty times the log budget: its commit waits for a full
/// checkpoint, so the redo point trails the log's end by no more after it than after many small
/// commits (§11).
#[test]
fn a_bulk_statements_commit_brings_the_redo_point_within_twice_the_budget() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/bulk");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    let budget = config().log_budget.expect("a log budget");
    let row = 8 << 10;
    conn.execute(format!(
        "INSERT INTO t (b) WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
         WHERE i < {}) SELECT zeroblob({row}) FROM n",
        20 * budget / row
    ))
    .unwrap();
    let atoll = db.atoll.as_ref().unwrap().atoll();
    let lag = atoll.checkpointer().lag();
    eprintln!("after the bulk commit, lag {lag}");
    assert!(lag <= 2 * budget, "the redo point trails by {lag} bytes");
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64((20 * budget / row) as i64)]]
    );
}

/// With a checkpointing thread, statements and commits do no checkpoint work, and a statement
/// that finds one due wakes the thread to do it: small commits appending one and a half times the
/// log budget, short of the twice past which a transaction's end waits for a full checkpoint, see
/// a checkpoint done all the same, the thread's.
#[test]
fn with_a_checkpointing_thread_a_due_checkpoint_is_the_threads_woken_by_a_statement() {
    within_a_minute(|| {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_with(&io, "/db/thread-commits", threaded());
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")
            .unwrap();
        let budget = config().log_budget.expect("a log budget");
        let row = 8 << 10;
        let atoll = db.atoll.as_ref().unwrap().atoll();
        let (steps, first, fulls) = (
            WRITER_STEPS.with(|n| n.get()),
            atoll.checkpointer().superblock().checkpoint,
            atoll.checkpointer().fulls_requested(),
        );
        for _ in 0..3 * budget / (2 * row) {
            conn.execute(format!("INSERT INTO t (b) VALUES (zeroblob({row}))"))
                .unwrap();
        }
        assert_eq!(
            WRITER_STEPS.with(|n| n.get()),
            steps,
            "a writer stepped the checkpoint"
        );
        assert_eq!(
            atoll.checkpointer().fulls_requested(),
            fulls,
            "a transaction's end asked for a full checkpoint"
        );
        while atoll.checkpointer().superblock().checkpoint == first {
            std::thread::yield_now();
        }
    });
}

/// With a checkpointing thread, one bulk statement appending twenty times the log budget: its
/// commit waits for the thread's full checkpoint, doing none of it, and the redo point trails by
/// no more than twice the budget after it.
#[test]
fn with_a_checkpointing_thread_a_bulk_commit_waits_for_its_full_checkpoint() {
    within_a_minute(|| {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_with(&io, "/db/thread-bulk", threaded());
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")
            .unwrap();
        let budget = config().log_budget.expect("a log budget");
        let row = 8 << 10;
        let steps = WRITER_STEPS.with(|n| n.get());
        conn.execute(format!(
            "INSERT INTO t (b) WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
             WHERE i < {}) SELECT zeroblob({row}) FROM n",
            20 * budget / row
        ))
        .unwrap();
        assert_eq!(
            WRITER_STEPS.with(|n| n.get()),
            steps,
            "a writer stepped the checkpoint"
        );
        let lag = db.atoll.as_ref().unwrap().atoll().checkpointer().lag();
        assert!(lag <= 2 * budget, "the redo point trails by {lag} bytes");
        assert_eq!(
            rows(&conn, "SELECT count(*) FROM t"),
            vec![vec![Value::from_i64((20 * budget / row) as i64)]]
        );
    });
}

/// Each begin collects the version arenas (DESIGN.md §11.5): a writer updating its rows again and
/// again, no older snapshot running, leaves its slot no page but the one it appends to once the
/// next begin has collected, where without collection every update's replaced value would stay.
#[test]
fn begins_collect_the_arenas_so_a_writers_replaced_values_do_not_pile_up() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/collected");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    conn.execute(
        "INSERT INTO t (b) WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
         WHERE i < 64) SELECT zeroblob(200) FROM n",
    )
    .unwrap();
    let page = crate::storage::atoll::txn::arena::CLASS.size() as u64;
    let pool = db.atoll.as_ref().unwrap().atoll().pool().clone();
    let taken = PAGES_TAKEN.with(|n| n.get());
    for _ in 0..200 {
        conn.execute("UPDATE t SET b = randomblob(200)").unwrap();
    }
    let taken = PAGES_TAKEN.with(|n| n.get()) - taken;
    assert!(taken > 4, "the updates took {taken} arena pages");
    // A begin of its own, which collects what the last update's transaction left.
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(64)]]
    );
    let left = pool.temporary_bytes();
    assert!(left <= page, "{left} bytes of arena pages left");
}

/// A long explicit transaction appending twenty times the log budget before it commits: each of
/// its writing statements steps the checkpointer, so the redo point follows the log while it runs,
/// not only at its commit (§11), as far as the transaction's first part spilled, which holds it
/// until the commit (§11.4.1); after the commit it follows the log's end again.
#[test]
fn a_long_transactions_statements_step_the_checkpointer_before_its_commit() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/long");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    let budget = config().log_budget.expect("a log budget");
    let row = 8 << 10;
    conn.execute("BEGIN").unwrap();
    for _ in 0..20 * budget / row {
        conn.execute(format!("INSERT INTO t (b) VALUES (zeroblob({row}))"))
            .unwrap();
    }
    let atoll = db.atoll.as_ref().unwrap().atoll();
    let held = atoll.transactions().held_from();
    assert!(
        held < atoll.log().end(),
        "the long transaction spilled nothing"
    );
    let lag = atoll.checkpointer().lag_to(held);
    eprintln!("before the long transaction's commit, lag {lag} to its first part");
    assert!(
        lag <= 2 * budget,
        "the redo point trails the transaction's first part by {lag} bytes"
    );
    conn.execute("COMMIT").unwrap();
    let lag = atoll.checkpointer().lag();
    assert!(
        lag <= 2 * budget,
        "after the commit, the redo point trails by {lag} bytes"
    );
}

/// Named savepoints: ROLLBACK TO puts back what was written since and keeps the savepoint,
/// RELEASE of an inner one keeps the transaction open, and a SAVEPOINT that began the transaction
/// commits it at its RELEASE.
#[test]
fn savepoints_roll_back_to_where_they_stood_and_commit_at_the_outermost_release() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/savepoints");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t (b) VALUES ('a')").unwrap();
    conn.execute("SAVEPOINT s1").unwrap();
    conn.execute("INSERT INTO t (b) VALUES ('b')").unwrap();
    conn.execute("SAVEPOINT s2").unwrap();
    conn.execute("INSERT INTO t (b) VALUES ('c')").unwrap();
    conn.execute("ROLLBACK TO s1").unwrap();
    assert_eq!(names(&conn), vec![text("a")]);
    conn.execute("INSERT INTO t (b) VALUES ('d')").unwrap();
    conn.execute("RELEASE s1").unwrap();
    assert!(
        !conn.get_auto_commit(),
        "an inner release keeps the transaction"
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(names(&conn), vec![text("a"), text("d")]);

    conn.execute("SAVEPOINT outer").unwrap();
    conn.execute("INSERT INTO t (b) VALUES ('e')").unwrap();
    conn.execute("ROLLBACK TO outer").unwrap();
    conn.execute("INSERT INTO t (b) VALUES ('f')").unwrap();
    conn.execute("RELEASE outer").unwrap();
    assert!(conn.get_auto_commit(), "the outermost release committed");
    let other = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(names(&other), vec![text("a"), text("d"), text("f")]);
}

/// An UPDATE whose row a subquery chose deletes and reinserts it through the table's cursor, then
/// reads the row back through it for RETURNING: an insert leaves the cursor at the entry it wrote.
#[test]
fn an_update_returns_the_row_it_wrote_through_its_cursor() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/returning");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE q (id INTEGER PRIMARY KEY, attempts INTEGER, locked INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO q VALUES (1, 0, NULL), (2, 1, NULL)")
        .unwrap();
    assert_eq!(
        rows(
            &conn,
            "UPDATE q SET attempts = attempts + 1, locked = 100 WHERE id = \
             (SELECT id FROM q ORDER BY attempts, id LIMIT 1) RETURNING id, attempts"
        ),
        vec![vec![Value::from_i64(1), Value::from_i64(1)]]
    );
    assert_eq!(
        rows(&conn, "SELECT id, attempts, locked FROM q ORDER BY id"),
        vec![
            vec![Value::from_i64(1), Value::from_i64(1), Value::from_i64(100)],
            vec![Value::from_i64(2), Value::from_i64(1), Value::Null],
        ]
    );
}

/// Steps `stmt` to its next row, through its IO; `None` at its end.
fn next_row(stmt: &mut crate::Statement) -> Option<Vec<Value>> {
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Row => {
                return Some(stmt.row().unwrap().get_values().cloned().collect())
            }
            crate::StepResult::IO | crate::StepResult::Yield => stmt._io().step().unwrap(),
            crate::StepResult::Done => return None,
            other => panic!("{other:?}"),
        }
    }
}

/// A temporary table lives in temp's pager, not Atoll: its rowids come from its own cursor, and
/// its writes commit with the statement, as the WAL arm ends temp's transaction.
#[test]
fn a_temporary_tables_rows_take_their_rowids_and_commit_in_temps_pager() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/temp");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TEMP TABLE tt (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("INSERT INTO tt (b) VALUES ('x'), ('y')")
        .unwrap();
    conn.execute("INSERT INTO tt (b) VALUES ('z')").unwrap();
    assert_eq!(
        rows(&conn, "SELECT a, b FROM tt ORDER BY a"),
        vec![
            vec![Value::from_i64(1), text("x")],
            vec![Value::from_i64(2), text("y")],
            vec![Value::from_i64(3), text("z")],
        ]
    );
}

/// A writer's halt commits the connection's shared autocommit transaction while a sibling
/// statement still reads: the sibling goes on in a read transaction begun at that commit, to the
/// end of its rows (DESIGN.md §17, §3).
#[test]
fn a_statement_reading_while_a_sibling_writes_reads_on_after_the_writers_commit() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/siblings");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (b) VALUES ('x'), ('y'), ('z')")
        .unwrap();
    let mut reading = conn.prepare("SELECT b FROM t ORDER BY a").unwrap();
    let mut seen = vec![next_row(&mut reading).expect("a first row")];
    conn.execute("INSERT INTO t (b) VALUES ('w')").unwrap();
    while let Some(row) = next_row(&mut reading) {
        seen.push(row);
    }
    // As in SQLite, where a scan reads on in the tree its connection's write changed, the row
    // written past where the reader stood is read.
    assert_eq!(
        seen,
        [
            vec![text("x")],
            vec![text("y")],
            vec![text("z")],
            vec![text("w")]
        ]
    );
    drop(reading);
    assert_eq!(names(&conn).len(), 4, "the writer's row committed");
}

/// A schema reparse reads the cookie, `sqlite_schema` and every sequence's descriptor in one
/// Atoll view it holds, its nested reads among them: another connection's CREATE SEQUENCE is seen,
/// and so is the sequence by the reparse of a database opened again (DESIGN.md §17, §6).
#[test]
fn a_reparse_reads_the_schema_and_its_sequences_in_one_view() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    {
        let db = open(&io, "/db/reparse");
        let (a, b) = (
            db.connect(ConnectOpts::new()).unwrap(),
            db.connect(ConnectOpts::new()).unwrap(),
        );
        a.execute("CREATE TABLE t (a)").unwrap();
        assert!(rows(&b, "SELECT a FROM t").is_empty());
        a.execute("CREATE SEQUENCE s START 10 INCREMENT 1").unwrap();
        assert_eq!(
            rows(&b, "SELECT nextval('s')"),
            vec![vec![Value::from_i64(10)]]
        );
        assert_eq!(
            rows(&a, "SELECT nextval('s')"),
            vec![vec![Value::from_i64(11)]]
        );
        a.close().unwrap();
        b.close().unwrap();
    }
    let db = open(&io, "/db/reparse");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(
        rows(&conn, "SELECT nextval('s')"),
        vec![vec![Value::from_i64(12)]],
        "a drawn value stays drawn across the reopen"
    );
}

/// Atoll's log is the database's journal: reading the journal mode or setting the one it has
/// works, a mode name Turso does not know or does not run reads the mode, as SQLite's does, and
/// changing it to one Turso runs is refused by name.
#[test]
fn the_journal_mode_cannot_change() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/journal");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let mode = rows(&conn, "PRAGMA journal_mode");
    eprintln!("journal mode {mode:?}");
    let [row] = &mode[..] else { panic!("{mode:?}") };
    let Value::Text(current) = &row[0] else {
        panic!("{mode:?}")
    };
    conn.execute(format!("PRAGMA journal_mode = {}", current.as_str()))
        .unwrap();
    for name in ["nonexistent", "delete"] {
        assert_eq!(
            rows(&conn, &format!("PRAGMA journal_mode = {name}")),
            mode,
            "{name}"
        );
    }
    let other = if current.as_str().eq_ignore_ascii_case("mvcc") {
        "wal"
    } else {
        "mvcc"
    };
    let err = conn
        .execute(format!("PRAGMA journal_mode = {other}"))
        .unwrap_err();
    assert!(
        err.to_string().contains("journal mode cannot change"),
        "{err}"
    );
}

/// `PRAGMA wal_checkpoint` runs a full checkpoint of Atoll's log, the redo point moved past every
/// record written before it, and reports what SQLite reports for a database not in WAL mode.
#[test]
fn a_wal_checkpoint_pragma_checkpoints_atolls_log_whole() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/wal-checkpoint");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    conn.execute("INSERT INTO t (b) VALUES (zeroblob(4096))")
        .unwrap();
    let atoll = db.atoll.as_ref().unwrap().atoll();
    let end = atoll.log().end();
    assert!(
        atoll.checkpointer().superblock().redo < end,
        "the insert's records follow the redo point"
    );
    let reported = rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    eprintln!("wal_checkpoint reported {reported:?}");
    let redo = atoll.checkpointer().superblock().redo;
    assert!(
        redo >= end,
        "the redo point at {redo}, before the log's end {end}"
    );
}

/// VACUUM rebuilds every tree under the write gate, with garbage collection's runtime drain (R9),
/// and is refused by name in Atoll mode until then.
#[test]
fn vacuum_is_refused_in_atoll_mode() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open(
        io.clone(),
        "/db/vacuum",
        OpenOptions::new(Arc::new(SqliteDialect))
            .atoll(config())
            .db_opts(crate::DatabaseOpts::default().with_vacuum(true)),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a)").unwrap();
    let err = conn.execute("VACUUM").unwrap_err();
    assert!(
        err.to_string().contains("VACUUM is refused in Atoll mode"),
        "{err}"
    );
    assert!(
        rows(&conn, "SELECT a FROM t").is_empty(),
        "the database goes on"
    );
}

/// A cursor resolves its tree as it opens and stays bound to it, as a b-tree cursor is to its
/// root (R22): a Destroy of the tree later in its statement, as a DROP's, leaves it reading the
/// rows of the tree it opened.
#[test]
fn a_cursor_stays_on_the_tree_it_opened_through_a_destroy_in_its_statement() {
    use crate::storage::atoll::drive;
    use crate::storage::atoll::sql::mode::{Begin, SqlCursor};
    use crate::storage::btree::CursorTrait;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/opened");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t (b) VALUES ('x'), ('y'), ('z')")
        .unwrap();
    let id = match &rows(&conn, "SELECT rootpage FROM sqlite_schema WHERE name = 't'")[..] {
        [row] => match row[0] {
            Value::Numeric(crate::Numeric::Integer(id)) => id as u64,
            ref other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    };
    let ct = conn.atoll().expect("Atoll mode");
    drive(&io, || ct.begin(Begin::Write { schema: true })).unwrap();
    ct.start_statement().unwrap();
    let mut cursor = SqlCursor::new(ct, conn.pager.load().clone(), id, None)
        .opened()
        .unwrap();
    let mut op = None;
    drive(&io, || ct.retire_tree(&mut op, id, false)).unwrap();
    assert!(!catalogued_in(ct, id), "the Destroy took the catalog row");
    assert_eq!(drive(&io, || cursor.count().map_err(|e| *e)).unwrap(), 3);
    drop(cursor);
    ct.end(false)
        .expect("a transaction to end")
        .finish()
        .unwrap();
    assert_eq!(names(&conn), vec![text("x"), text("y"), text("z")]);
}

/// Whether tree `id` has a catalog row as `ct`'s transaction reads it now.
fn catalogued_in(ct: &crate::storage::atoll::sql::mode::ConnTxn, id: u64) -> bool {
    use crate::storage::atoll::drive;
    use crate::storage::atoll::sql::catalog::key;
    use crate::storage::atoll::txn::versioned::Get;
    let db = ct.db();
    let mut guard = ct.cell.lock();
    let txn = guard.as_mut().expect("an open transaction");
    let mut get = Get::new(&key(id));
    drive(db.io(), || {
        db.catalog()
            .get(db.atoll().transactions(), db.versions(), txn, &mut get)
    })
    .unwrap()
    .is_some()
}

/// An ALTER COLUMN that the table's rows refuse fails whole, in autocommit: the schema row it
/// rewrote before its check ran is put back, as the statement's every write is.
/// A transaction reads a tree's catalog row once for all its statements, and each cursor starts
/// at the leaf the one before it stood at; the row is read again after a statement of its own
/// changes the catalog and after a rollback of statements, which may undo one. Rows a Clear took
/// read back after a rollback to before it, and a row written after the Clear goes into the tree
/// the Clear made.
#[test]
fn a_transaction_reads_a_trees_catalog_row_once_and_again_after_its_own_change_or_a_rollback() {
    use crate::storage::atoll::btree::NEAR_TAKEN;
    use crate::storage::atoll::sql::mode::CATALOG_READS;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/known-rows");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    let reads = || CATALOG_READS.with(|r| std::mem::take(&mut *r.borrow_mut()));
    let taken = || NEAR_TAKEN.with(|n| n.get());
    let reaches = || crate::storage::atoll::btree::LEAF_REACHES.with(|n| n.get());
    conn.execute("BEGIN").unwrap();
    reads();
    let statements = 50;
    conn.execute("INSERT INTO t VALUES (0, 'r0')").unwrap();
    let (before, reached) = (taken(), reaches());
    for i in 1..statements {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'r{i}')"))
            .unwrap();
    }
    let read = reads();
    let once: std::collections::BTreeSet<u64> = read.iter().copied().collect();
    assert!(
        !read.is_empty() && once.len() == read.len(),
        "each tree's row read once over {statements} statements: {read:?}"
    );
    // Every statement after the first reaches one leaf, the one the last left, by its probe: its
    // rowid check and its write answer from the probe's look.
    assert_eq!(
        (reaches() - reached, taken() - before),
        ((statements - 1) as u64, (statements - 1) as u64),
        "leaves reached, and reached from the last, over {statements} statements"
    );
    conn.execute("SAVEPOINT s").unwrap();
    conn.execute("DELETE FROM t").unwrap();
    conn.execute("INSERT INTO t VALUES (100, 'after')").unwrap();
    assert_eq!(
        rows(&conn, "SELECT a FROM t"),
        vec![vec![Value::from_i64(100)]]
    );
    conn.execute("ROLLBACK TO s").unwrap();
    assert_eq!(
        rows(&conn, "SELECT count(*), min(a), max(a) FROM t"),
        vec![vec![
            Value::from_i64(statements as i64),
            Value::from_i64(0),
            Value::from_i64(statements as i64 - 1)
        ]]
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(statements as i64)]]
    );
}

/// A catalog row a transaction read is its own: one read at a snapshot before another
/// connection's Clear committed is not the next transaction's, which reads the tree the Clear made.
#[test]
fn a_catalog_row_read_at_an_older_snapshot_is_not_the_next_transactions() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/known-snapshot");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE u (a INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    conn.execute("INSERT INTO u VALUES (1)").unwrap();
    conn.execute("BEGIN").unwrap();
    // The snapshot, taken before the Clear commits.
    assert_eq!(
        rows(&conn, "SELECT a FROM u"),
        vec![vec![Value::from_i64(1)]]
    );
    other.execute("DELETE FROM t").unwrap();
    // Read at the snapshot: the tree before the Clear, with both rows.
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(2)]]
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM t"),
        vec![vec![Value::from_i64(0)]]
    );
    conn.execute("INSERT INTO t VALUES (3, 'z')").unwrap();
    assert_eq!(
        rows(&other, "SELECT a FROM t"),
        vec![vec![Value::from_i64(3)]]
    );
}

/// A transaction reads a header field once for all its statements, and again after a statement of
/// its own writes the header and after a rollback of statements: the schema version reads as each
/// CREATE left it, and as it stood before the CREATE a rollback to a savepoint undid.
#[test]
fn a_transaction_reads_a_header_field_once_and_again_after_its_own_write_or_a_rollback() {
    use crate::storage::atoll::sql::mode::HEADER_READS;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/known-fields");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY)")
        .unwrap();
    let version =
        |conn: &Arc<Connection>| match rows(conn, "PRAGMA schema_version").remove(0).remove(0) {
            Value::Numeric(crate::numeric::Numeric::Integer(v)) => v,
            other => panic!("a schema version of {other:?}"),
        };
    let reads = || HEADER_READS.with(|r| r.get());
    conn.execute("BEGIN").unwrap();
    let v0 = version(&conn);
    let before = reads();
    let statements = 50;
    for i in 0..statements {
        conn.execute(format!("INSERT INTO t VALUES ({i})")).unwrap();
    }
    // At most each of the header's fields read once.
    let (read, fields) = (
        reads() - before,
        crate::storage::atoll::sql::header::Field::ALL.len() as u64,
    );
    assert!(
        read <= fields,
        "{read} header reads over {statements} statements"
    );
    conn.execute("CREATE TABLE a (x)").unwrap();
    assert_eq!(version(&conn), v0 + 1);
    conn.execute("SAVEPOINT s").unwrap();
    conn.execute("CREATE TABLE b (x)").unwrap();
    assert_eq!(version(&conn), v0 + 2);
    conn.execute("ROLLBACK TO s").unwrap();
    assert_eq!(version(&conn), v0 + 1);
    conn.execute("COMMIT").unwrap();
    assert_eq!(version(&conn), v0 + 1);
    assert!(reads() > before, "the header was read at all");
}

/// A header field a transaction read is its own: the schema version read at a snapshot before
/// another connection's CREATE committed is not the next transaction's, which reads the CREATE's.
#[test]
fn a_header_field_read_at_an_older_snapshot_is_not_the_next_transactions() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/known-field-snapshot");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE u (a INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO u VALUES (1)").unwrap();
    let version = |conn: &Arc<Connection>| rows(conn, "PRAGMA schema_version");
    conn.execute("BEGIN").unwrap();
    // The snapshot, taken before the CREATE commits.
    assert_eq!(
        rows(&conn, "SELECT a FROM u"),
        vec![vec![Value::from_i64(1)]]
    );
    let before = version(&conn);
    other.execute("CREATE TABLE z (x)").unwrap();
    // Read at the snapshot: the version before the CREATE.
    assert_eq!(version(&conn), before);
    conn.execute("COMMIT").unwrap();
    assert_eq!(version(&conn), version(&other));
    assert_ne!(version(&conn), before);
}

#[test]
fn a_rejected_alter_column_leaves_the_schema_as_it_was() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/alter");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, -1), (2, NULL)")
        .unwrap();
    let schema = "SELECT sql FROM sqlite_schema WHERE name = 't'";
    let before = rows(&conn, schema);
    let err = conn
        .execute("ALTER TABLE t ALTER COLUMN v TO v2 INTEGER CHECK(v2 > 0)")
        .unwrap_err();
    assert!(err.to_string().contains("CHECK constraint failed"), "{err}");
    assert_eq!(rows(&conn, schema), before);
    let other = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(rows(&other, schema), before, "nothing of it committed");
}

/// Steps `scan`, running `between` after each row it returns with the row's number, and returns
/// the rows.
fn scan_running(
    scan: &mut crate::Statement,
    mut between: impl FnMut(usize, &[Value]),
) -> Vec<Vec<Value>> {
    let mut seen = Vec::new();
    while let Some(row) = next_row(scan) {
        between(seen.len(), &row);
        seen.push(row);
    }
    seen
}

/// A whole-table DELETE of another statement of the connection, while a scan stands at one of
/// the table's rows, replaces the tree under it: the scan reads no row there and goes on to the
/// end of the new tree, as SQLite's cursor reads NULL once its table is cleared (DESIGN.md §17,
/// §3, the writer's view).
#[test]
fn a_scan_reads_no_row_where_its_connection_cleared_the_table() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/cleared-under");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t1(a, b)").unwrap();
    conn.execute("CREATE TABLE t2(c, d)").unwrap();
    conn.execute("INSERT INTO t1 VALUES(1, 2)").unwrap();
    conn.execute("INSERT INTO t2 VALUES(3, 4), (5, 6)").unwrap();
    let mut scan = conn
        .prepare("SELECT CASE WHEN c = 5 THEN b ELSE NULL END AS b, c, d FROM t1, t2")
        .unwrap();
    let seen = scan_running(&mut scan, |_, _| {
        conn.execute("DELETE FROM t1").unwrap();
    });
    let int = Value::from_i64;
    assert_eq!(
        seen,
        vec![
            vec![Value::Null, int(3), int(4)],
            vec![Value::Null, int(5), int(6)],
        ]
    );
}

/// In an explicit transaction, a row another statement of the connection deletes under a scan
/// reads as no row, and the scan goes on from its key; a row another statement updates reads as
/// it now is (DESIGN.md §17, §3, its own writes).
#[test]
fn a_scan_reads_the_row_under_it_as_its_connection_left_it() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/changed-under");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, v)")
        .unwrap();
    conn.execute("CREATE TABLE t2(x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t2 VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO t1 VALUES (10, 'a'), (20, 'b'), (30, 'c')")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let mut scan = conn
        .prepare("SELECT t1.id, t1.v, t2.x FROM t1, t2")
        .unwrap();
    let seen = scan_running(&mut scan, |_, row| {
        if row[0] == Value::from_i64(10) && row[2] == Value::from_i64(1) {
            conn.execute("UPDATE t1 SET v = 'a2' WHERE id = 10")
                .unwrap();
        }
        if row[0] == Value::from_i64(20) && row[2] == Value::from_i64(1) {
            conn.execute("DELETE FROM t1 WHERE id = 20").unwrap();
        }
    });
    drop(scan);
    conn.execute("COMMIT").unwrap();
    let (int, null) = (Value::from_i64, Value::Null);
    assert_eq!(
        seen,
        vec![
            vec![int(10), text("a"), int(1)],
            vec![int(10), text("a2"), int(2)],
            vec![int(20), text("b"), int(1)],
            vec![null.clone(), null, int(2)],
            vec![int(30), text("c"), int(1)],
            vec![int(30), text("c"), int(2)],
        ]
    );
}

/// A ROLLBACK TO of the connection under a scan puts back rows the scan read changed: the scan
/// reads the row it stands at as the rollback left it.
#[test]
fn a_scan_reads_the_row_under_it_as_a_rollback_to_left_it() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/rolled-under");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t1(id INTEGER PRIMARY KEY, v)")
        .unwrap();
    conn.execute("CREATE TABLE t2(x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO t2 VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO t1 VALUES (10, 'a')").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT s").unwrap();
    conn.execute("UPDATE t1 SET v = 'changed' WHERE id = 10")
        .unwrap();
    let mut scan = conn.prepare("SELECT t1.v, t2.x FROM t1, t2").unwrap();
    let seen = scan_running(&mut scan, |i, _| {
        if i == 0 {
            conn.execute("ROLLBACK TO s").unwrap();
        }
    });
    drop(scan);
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        seen,
        vec![
            vec![text("changed"), Value::from_i64(1)],
            vec![text("a"), Value::from_i64(2)],
        ]
    );
}

/// A cursor an outer statement opened before its trigger cleared the cursor's table, a DELETE of
/// every row, reaches the tree the Clear made at its next seek, rowid or write, as SQLite's cursor
/// does the emptied b-tree (DESIGN.md §17, §3): its seek reads the trigger's row, its NewRowid
/// draws from the new tree's counter and its write lands in the new tree, where the old tree's
/// gave a row the trigger had deleted and lost the write.
#[test]
fn a_cursor_opened_before_a_trigger_cleared_its_table_seeks_draws_and_writes_in_the_new_tree() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/cleared-by-trigger");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let int = Value::from_i64;
    // A seek: the UPDATE's second row is the one its trigger put back.
    conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("INSERT INTO u VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    conn.execute("CREATE TABLE log(o, n)").unwrap();
    conn.execute(
        "CREATE TRIGGER au AFTER UPDATE ON u BEGIN \
         INSERT INTO log VALUES (old.x, new.x); DELETE FROM u; INSERT INTO u VALUES (2, 'q'); END",
    )
    .unwrap();
    conn.execute("UPDATE u SET x = x || '!' WHERE id IN (1, 2)")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT o, n FROM log ORDER BY rowid"),
        vec![vec![text("a"), text("a!")], vec![text("q"), text("q!")]],
        "the seek after the Clear"
    );
    assert_eq!(
        rows(&conn, "SELECT id, x FROM u"),
        vec![vec![int(2), text("q")]]
    );
    // A NewRowid and its insert: each row drawn from the tree its trigger just emptied.
    conn.execute("CREATE TABLE v(x)").unwrap();
    conn.execute("INSERT INTO v VALUES ('a'), ('b'), ('c')")
        .unwrap();
    conn.execute("CREATE TRIGGER bi BEFORE INSERT ON v BEGIN DELETE FROM v; END")
        .unwrap();
    conn.execute("INSERT INTO v(x) VALUES ('p'), ('q')")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT rowid, x FROM v"),
        vec![vec![int(1), text("q")]],
        "the NewRowid and insert after the Clear"
    );
    // A write at the rowid the statement found: sqlite_sequence's row for t, put back where it
    // stood though the trigger emptied the table and wrote another row there.
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v)")
        .unwrap();
    conn.execute("INSERT INTO t(v) VALUES ('seed')").unwrap();
    conn.execute(
        "CREATE TRIGGER ti BEFORE INSERT ON t BEGIN \
         DELETE FROM sqlite_sequence; INSERT INTO sqlite_sequence VALUES ('zz', 1); END",
    )
    .unwrap();
    conn.execute("INSERT INTO t(v) VALUES ('a')").unwrap();
    conn.execute("INSERT INTO t(v) VALUES ('b'), ('c')")
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![int(1), text("seed")],
            vec![int(2), text("a")],
            vec![int(3), text("b")],
            vec![int(4), text("c")],
        ]
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT rowid, name, seq FROM sqlite_sequence ORDER BY rowid"
        ),
        vec![vec![int(1), text("t"), int(4)]],
        "the write at a key after the Clear"
    );
}

/// A WITHOUT ROWID table is keyed on its primary key, as an index is (R29), with the row as the
/// value: its rows read in key order, a descending key column's included, whatever order they went
/// in; a lookup by the key finds its row; a second row with a key already there is a constraint
/// error; and the rows read the same after a reopen. Its key columns come first, as Turso's
/// WITHOUT ROWID rows read right only then, in WAL mode too: another order reads them with the
/// other columns' affinities.
#[test]
fn a_without_rowid_table_is_keyed_on_its_primary_key() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let open = || {
        Database::open(
            io.clone(),
            "/db/without-rowid",
            OpenOptions::new(Arc::new(SqliteDialect))
                .atoll(config())
                .db_opts(DatabaseOpts::default().with_without_rowid(true)),
        )
        .unwrap()
    };
    let in_key_order = |conn: &Arc<Connection>| {
        assert_eq!(
            rows(conn, "SELECT a, b, c FROM w"),
            vec![
                vec![text("a"), Value::from_i64(2), text("r")],
                vec![text("y"), Value::from_i64(2), text("q")],
                vec![text("x"), Value::from_i64(1), text("p")],
                vec![text("z"), Value::from_i64(0), text("s")],
            ]
        );
    };
    {
        let db = open();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE w(b INTEGER, a TEXT, c, PRIMARY KEY (b DESC, a)) WITHOUT ROWID")
            .unwrap();
        conn.execute(
            "INSERT INTO w VALUES (1, 'x', 'p'), (2, 'y', 'q'), (2, 'a', 'r'), (0, 'z', 's')",
        )
        .unwrap();
        in_key_order(&conn);
        assert_eq!(
            rows(&conn, "SELECT c FROM w WHERE b = 2 AND a = 'y'"),
            vec![vec![text("q")]]
        );
        let err = conn
            .execute("INSERT INTO w VALUES (1, 'x', 'again')")
            .unwrap_err();
        assert!(
            err.to_string().contains("UNIQUE constraint failed"),
            "{err}"
        );
        in_key_order(&conn);
        conn.close().unwrap();
    }
    let db = open();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    in_key_order(&conn);
}

/// A materialized view over an Atoll table keeps its rows in an Atoll tree, read through the view's
/// cursor: it follows the table's inserts, updates and deletes, a transaction reads its own changes
/// to it, a trigger reading it inside the statement that changes the table reads that statement's
/// earlier rows, a rollback leaves it as it stood, and it reads the same after a reopen and goes on
/// following the table.
#[test]
fn a_materialized_view_follows_its_table_through_a_rollback_and_a_reopen() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let open = || {
        Database::open(
            io.clone(),
            "/db/matview",
            OpenOptions::new(Arc::new(SqliteDialect))
                .atoll(config())
                .db_opts(DatabaseOpts::default().with_views(true)),
        )
        .unwrap()
    };
    let counts = |conn: &Arc<Connection>| rows(conn, "SELECT g, c FROM v ORDER BY g");
    let row = |g: &str, c: i64| vec![text(g), Value::from_i64(c)];
    {
        let db = open();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t(g, n)").unwrap();
        conn.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 5)")
            .unwrap();
        conn.execute("CREATE MATERIALIZED VIEW v AS SELECT g, count(*) AS c FROM t GROUP BY g")
            .unwrap();
        assert_eq!(counts(&conn), vec![row("a", 2), row("b", 1)]);
        conn.execute("UPDATE t SET g = 'b' WHERE n = 2").unwrap();
        conn.execute("DELETE FROM t WHERE n = 1").unwrap();
        assert_eq!(counts(&conn), vec![row("b", 2)]);
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES ('c', 7)").unwrap();
        assert_eq!(
            counts(&conn),
            vec![row("b", 2), row("c", 1)],
            "its own change"
        );
        conn.execute("ROLLBACK").unwrap();
        assert_eq!(counts(&conn), vec![row("b", 2)]);
        conn.execute("CREATE TABLE seen(g, c)").unwrap();
        conn.execute(
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
             INSERT INTO seen SELECT g, c FROM v WHERE g = new.g; END",
        )
        .unwrap();
        conn.execute("INSERT INTO t VALUES ('d', 1), ('d', 2)")
            .unwrap();
        assert_eq!(
            rows(&conn, "SELECT g, c FROM seen ORDER BY rowid"),
            vec![row("d", 1), row("d", 2)],
            "the view read inside the statement"
        );
        assert_eq!(counts(&conn), vec![row("b", 2), row("d", 2)]);
        conn.close().unwrap();
    }
    let db = open();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(counts(&conn), vec![row("b", 2), row("d", 2)]);
    conn.execute("INSERT INTO t VALUES ('a', 4)").unwrap();
    assert_eq!(counts(&conn), vec![row("a", 1), row("b", 2), row("d", 2)]);
}

/// A statement changing an attached database's table, with change capture on, writes the main
/// database's CDC table too, so it holds main's write gate for it: its transaction there is a
/// write, where Turso had it a read and the write ran without the gate (Atoll's R24).
#[test]
fn a_captured_change_to_an_attached_table_writes_the_main_cdc_table_under_its_gate() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open(
        io.clone(),
        "/db/attached-cdc",
        OpenOptions::new(Arc::new(SqliteDialect))
            .atoll(config())
            .db_opts(DatabaseOpts::default().with_attach(true)),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute(
        "CREATE TABLE turso_cdc(change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, \
         change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)",
    )
    .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("ATTACH ':memory:' AS aux").unwrap();
    conn.execute("CREATE TABLE aux.t1(x INTEGER)").unwrap();
    conn.execute("INSERT INTO aux.t1 VALUES (1)").unwrap();
    conn.execute("DELETE FROM aux.t1 WHERE x = 1").unwrap();
    assert_eq!(
        rows(
            &conn,
            "SELECT table_name, change_type FROM turso_cdc WHERE table_name != 'sqlite_schema' \
             ORDER BY change_id"
        ),
        vec![
            vec![text("t1"), Value::from_i64(1)],
            vec![text("t1"), Value::from_i64(-1)],
        ]
    );
}

/// An index method whose rows are a backing store, the toy sparse IVF, keeps them in Atoll trees
/// (DESIGN.md §17): its query goes through the index and finds the nearest row, an update in a
/// transaction is read through it, a rollback leaves it as it stood, and it answers the same after
/// a reopen.
#[test]
fn a_backing_store_index_method_answers_through_a_rollback_and_a_reopen() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let open = || {
        Database::open(
            io.clone(),
            "/db/toy-ivf",
            OpenOptions::new(Arc::new(SqliteDialect))
                .atoll(config())
                .db_opts(DatabaseOpts::default().with_index_method(true)),
        )
        .unwrap()
    };
    let nearest = |conn: &Arc<Connection>, vector: &str| {
        let sql = format!(
            "SELECT id FROM vectors ORDER BY \
             vector_distance_jaccard(embedding, vector32_sparse('{vector}')) LIMIT 1"
        );
        let plan = rows(conn, &format!("EXPLAIN {sql}"));
        assert!(
            plan.iter()
                .any(|row| row[1].to_string() == "IndexMethodQuery"),
            "the query does not go through the index"
        );
        rows(conn, &sql)
    };
    let one = |id: i64| vec![vec![Value::from_i64(id)]];
    {
        let db = open();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE vectors(id INTEGER PRIMARY KEY, embedding)")
            .unwrap();
        conn.execute("CREATE INDEX vectors_idx ON vectors USING toy_vector_sparse_ivf (embedding)")
            .unwrap();
        conn.execute(
            "INSERT INTO vectors VALUES (1, vector32_sparse('[1, 0, 0]')), \
             (2, vector32_sparse('[0, 1, 0]'))",
        )
        .unwrap();
        assert_eq!(nearest(&conn, "[1, 0, 0]"), one(1));
        conn.execute("BEGIN").unwrap();
        conn.execute("UPDATE vectors SET embedding = vector32_sparse('[0, 0, 1]') WHERE id = 2")
            .unwrap();
        assert_eq!(nearest(&conn, "[0, 0, 1]"), one(2), "its own update");
        conn.execute("ROLLBACK").unwrap();
        assert_eq!(nearest(&conn, "[0, 1, 0]"), one(2));
        conn.close().unwrap();
    }
    let db = open();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(nearest(&conn, "[0, 1, 0]"), one(2));
    assert_eq!(nearest(&conn, "[1, 0, 0]"), one(1));
}

/// Steps `stmt` until it is done, busy or fails, doing its IO meanwhile.
fn step_until_busy(stmt: &mut crate::Statement) -> crate::Result<crate::StepResult> {
    loop {
        match stmt.step() {
            Ok(crate::StepResult::IO | crate::StepResult::Yield) => stmt._io().step()?,
            Ok(crate::StepResult::Row) => {}
            Err(crate::LimboError::Busy) => return Ok(crate::StepResult::Busy),
            other => return other,
        }
    }
}

/// Two transactions waiting on each other's rows close a cycle in the wait graph, and the younger
/// fails with `Deadlock` at once and is rolled back, though neither has a busy timeout and each
/// `Busy` is retried in place: the older's write, retried, then goes on (DESIGN.md §17, §3.1; R34
/// as re-ruled 2026-11-04). Without the graph both come back `Busy` for ever. The younger begins
/// with BEGIN CONCURRENT, which is BEGIN in Atoll mode.
#[test]
fn a_cycle_of_writers_retrying_busy_in_place_fails_the_younger() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/deadlock-busy");
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("UPDATE t SET v = 'a1' WHERE id = 1").unwrap();
    b.execute("BEGIN CONCURRENT").unwrap();
    b.execute("UPDATE t SET v = 'b2' WHERE id = 2").unwrap();
    let mut a2 = a.prepare("UPDATE t SET v = 'a2' WHERE id = 2").unwrap();
    assert!(matches!(
        step_until_busy(&mut a2),
        Ok(crate::StepResult::Busy)
    ));
    let err = b.execute("UPDATE t SET v = 'b1' WHERE id = 1").unwrap_err();
    assert!(matches!(err, crate::LimboError::Deadlock), "{err}");
    assert!(
        b.get_auto_commit(),
        "the victim's transaction was rolled back"
    );
    assert!(matches!(
        step_until_busy(&mut a2),
        Ok(crate::StepResult::Done)
    ));
    drop(a2);
    a.execute("COMMIT").unwrap();
    assert_eq!(
        rows(&b, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::from_i64(1), text("a1")],
            vec![Value::from_i64(2), text("a2")],
        ]
    );
}

/// A writer parked behind another, with a busy timeout, and the other then waiting on it: the
/// younger fails with `Deadlock` at once, not at its timeout, and the parked one wakes at its end
/// and commits.
#[test]
fn a_cycle_through_a_parked_writer_fails_the_younger_at_once() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/deadlock-park");
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    let timeout = std::time::Duration::from_secs(10);
    a.set_busy_timeout(timeout);
    b.set_busy_timeout(timeout);
    a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("UPDATE t SET v = 'a1' WHERE id = 1").unwrap();
    b.execute("BEGIN").unwrap();
    b.execute("UPDATE t SET v = 'b2' WHERE id = 2").unwrap();
    let parked = std::thread::spawn(move || {
        let done = a.execute("UPDATE t SET v = 'a2' WHERE id = 2");
        (a, done)
    });
    // Until a's wait is published, b's write only waits too: it closes the cycle once it is.
    std::thread::sleep(std::time::Duration::from_millis(200));
    let started = std::time::Instant::now();
    let err = b.execute("UPDATE t SET v = 'b1' WHERE id = 1").unwrap_err();
    let took = started.elapsed();
    assert!(matches!(err, crate::LimboError::Deadlock), "{err}");
    assert!(took < timeout / 4, "the cycle was broken after {took:?}");
    let (a, done) = parked.join().unwrap();
    done.unwrap();
    a.execute("COMMIT").unwrap();
    assert_eq!(
        rows(&b, "SELECT v FROM t ORDER BY id"),
        vec![vec![text("a1")], vec![text("a2")]]
    );
}

/// A write behind an open writer that never ends parks until its busy timeout and then comes back
/// `Busy` once: the busy handler does not wait the timeout again.
#[test]
fn a_wait_past_its_busy_timeout_comes_back_busy_once() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/wait-timeout");
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    let timeout = std::time::Duration::from_millis(500);
    b.set_busy_timeout(timeout);
    a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("UPDATE t SET v = 'a1' WHERE id = 1").unwrap();
    let started = std::time::Instant::now();
    let err = b.execute("UPDATE t SET v = 'b1' WHERE id = 1").unwrap_err();
    let took = started.elapsed();
    assert!(matches!(err, crate::LimboError::Busy), "{err}");
    assert!(
        took >= timeout && took < timeout * 19 / 10,
        "Busy after {took:?}, for a {timeout:?} timeout"
    );
    a.execute("ROLLBACK").unwrap();
}

/// Twelve threads move money between accounts in transactions that take two rows in a random
/// order, so their waits close cycles: each cycle fails one of them with `Deadlock`, which retries
/// its transfer, and in the end no money is lost or made and some cycle was met.
#[test]
fn twelve_transferring_writers_break_their_deadlocks_and_lose_nothing() {
    const THREADS: usize = 12;
    const ROUNDS: usize = 20;
    const ACCOUNTS: i64 = 6;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/transfers");
    let setup = db.connect(ConnectOpts::new()).unwrap();
    setup
        .execute("CREATE TABLE acct(id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    for id in 1..=ACCOUNTS {
        setup
            .execute(format!("INSERT INTO acct VALUES ({id}, 100)"))
            .unwrap();
    }
    let start = Arc::new(std::sync::Barrier::new(THREADS));
    let threads: Vec<_> = (0..THREADS)
        .map(|who| {
            let (db, start) = (db.clone(), start.clone());
            std::thread::spawn(move || {
                let conn = db.connect(ConnectOpts::new()).unwrap();
                conn.set_busy_timeout(std::time::Duration::from_secs(30));
                let mut state = who as u64 * 2654435761 + 1;
                let mut next = |n: i64| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state % n as u64) as i64 + 1
                };
                start.wait();
                let (mut deadlocks, mut conflicts) = (0, 0);
                for _ in 0..ROUNDS {
                    let from = next(ACCOUNTS);
                    let to = (from + next(ACCOUNTS - 1) - 1) % ACCOUNTS + 1;
                    loop {
                        conn.execute("BEGIN").unwrap();
                        let done = conn
                            .execute(format!("UPDATE acct SET n = n - 1 WHERE id = {from}"))
                            .and_then(|_| {
                                conn.execute(format!("UPDATE acct SET n = n + 1 WHERE id = {to}"))
                            })
                            .and_then(|_| conn.execute("COMMIT"));
                        match done {
                            Ok(()) => break,
                            Err(crate::LimboError::Deadlock) => deadlocks += 1,
                            Err(crate::LimboError::WriteWriteConflict) => conflicts += 1,
                            Err(e) => panic!("thread {who}: {e}"),
                        }
                        assert!(conn.get_auto_commit(), "a failed transfer left it open");
                    }
                }
                (deadlocks, conflicts)
            })
        })
        .collect();
    let (mut deadlocks, mut conflicts) = (0, 0);
    for t in threads {
        let (d, c) = t.join().unwrap();
        deadlocks += d;
        conflicts += c;
    }
    assert_eq!(
        rows(&setup, "SELECT sum(n), count(*) FROM acct"),
        vec![vec![
            Value::from_i64(100 * ACCOUNTS),
            Value::from_i64(ACCOUNTS)
        ]]
    );
    eprintln!("transfers: {deadlocks} deadlocks and {conflicts} conflicts retried");
    assert!(deadlocks > 0, "no cycle of waits was met");
}

/// A write parked behind another transaction wakes at that transaction's end, not at the next poll
/// of a busy handler (R34): over a hundred hand-offs, a waiter behind a writer that commits after
/// 5 ms finishes a median of well under the 3 ms a poll at SQLite's backoff (1, 3, 8 ms) would
/// leave it waiting past the commit. The lateness is counted past the waiter's own run of the same
/// prepared statement with nothing in its way, so a slow machine slows both sides alike.
#[test]
fn a_parked_write_wakes_at_its_blockers_commit_not_at_a_poll() {
    const HANDOFFS: usize = 100;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/wake");
    let holder = db.connect(ConnectOpts::new()).unwrap();
    holder
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    holder.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    let (go, gone) = std::sync::mpsc::channel::<()>();
    let (done, finished) = std::sync::mpsc::channel::<(std::time::Instant, std::time::Duration)>();
    let waiter = {
        let db = db.clone();
        std::thread::spawn(move || {
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.set_busy_timeout(std::time::Duration::from_secs(10));
            let mut update = conn.prepare("UPDATE t SET n = n + 1 WHERE id = 1").unwrap();
            while gone.recv().is_ok() {
                update.run_ignore_rows().unwrap();
                update.reset().unwrap();
                let woke = std::time::Instant::now();
                update.run_ignore_rows().unwrap();
                update.reset().unwrap();
                done.send((woke, woke.elapsed())).unwrap();
            }
        })
    };
    let (mut late, mut own) = (Vec::with_capacity(HANDOFFS), Vec::with_capacity(HANDOFFS));
    for _ in 0..HANDOFFS {
        holder.execute("BEGIN").unwrap();
        holder
            .execute("UPDATE t SET n = n + 1 WHERE id = 1")
            .unwrap();
        go.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        holder.execute("COMMIT").unwrap();
        let committed = std::time::Instant::now();
        let (woke, alone) = finished.recv().unwrap();
        late.push(woke.saturating_duration_since(committed));
        own.push(alone);
    }
    drop(go);
    waiter.join().unwrap();
    late.sort();
    own.sort();
    let (median, p99, alone) = (
        late[HANDOFFS / 2],
        late[HANDOFFS * 99 / 100],
        own[HANDOFFS / 2],
    );
    eprintln!("wake after commit: median {median:?}, p99 {p99:?}; the statement alone: {alone:?}");
    assert_eq!(
        rows(&holder, "SELECT n FROM t"),
        vec![vec![Value::from_i64(3 * HANDOFFS as i64)]]
    );
    assert!(
        median.saturating_sub(alone) < std::time::Duration::from_micros(1500),
        "a waiter finished a median {median:?} after its blocker's commit, past its own {alone:?}"
    );
}

/// A statement that destroys a tree, then opens a cursor on it, reads the tree as empty, as
/// SQLite's cursor reads a freed root: a DROP of CDC's version table, which deletes that table's
/// rows after its Destroy, succeeds.
#[test]
fn a_cursor_its_statement_opens_on_a_tree_it_destroyed_reads_as_empty() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/destroyed-open");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('full')")
        .unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    let named = "SELECT count(*) FROM sqlite_schema WHERE name = 'turso_cdc_version'";
    assert_eq!(rows(&conn, named), vec![vec![Value::from_i64(1)]]);
    conn.execute("DROP TABLE turso_cdc_version").unwrap();
    assert_eq!(rows(&conn, named), vec![vec![Value::from_i64(0)]]);
}

/// `PRAGMA page_count` counts the page store's pages in use (DESIGN.md §17, §12), and grows by at
/// least the bytes written; `max_page_count` reads as SQLite's no-cap default and cannot be set,
/// Atoll having no page cap (R35).
#[test]
fn page_count_comes_from_the_page_store_and_there_is_no_page_cap() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/page-count");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")
        .unwrap();
    let count = |conn: &Arc<Connection>| match &rows(conn, "PRAGMA page_count")[..] {
        [row] => match row[0] {
            Value::Numeric(crate::Numeric::Integer(n)) => n,
            ref other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    };
    let before = count(&conn);
    assert!(before > 0, "the catalog's and the schema's pages");
    let (n, bytes) = (100, 4000);
    for _ in 0..n {
        conn.execute(format!("INSERT INTO t (b) VALUES (zeroblob({bytes}))"))
            .unwrap();
    }
    let after = count(&conn);
    assert!(
        after - before >= n * bytes / super::PAGE as i64,
        "{before} pages before {n} rows of {bytes} bytes, {after} after"
    );
    assert_eq!(
        rows(&conn, "PRAGMA max_page_count"),
        vec![vec![Value::from_i64(super::NO_PAGE_CAP as i64)]]
    );
    let err = conn.execute("PRAGMA max_page_count = 50").unwrap_err();
    assert!(err.to_string().contains("no page cap"), "{err}");
}

/// Two opens of one path while the first is alive get one instance, under an IO whose files are
/// not on disk, as under one whose files are: the registry keys the path by the IO's identity.
#[test]
fn two_opens_of_a_path_share_one_instance() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let first = open(&io, "/db/shared-instance");
    let second = open(&io, "/db/shared-instance");
    assert!(Arc::ptr_eq(&first, &second));
}

/// A statement that stops waiting for its exclusive take of the gate, as one dropped after Busy
/// does, gives the take up (R15): its connection's next write takes the gate shared and writes,
/// never behind its own abandoned take.
#[test]
fn a_ddl_dropped_after_busy_gives_up_its_take_and_its_connection_writes_through_the_gate() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/given-up");
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("CREATE TABLE t (x)").unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("INSERT INTO t VALUES (1)").unwrap();
    b.execute("PRAGMA busy_timeout = 0").unwrap();
    let err = b.execute("CREATE TABLE u (y)").unwrap_err();
    assert!(matches!(err, crate::LimboError::Busy), "{err:?}");
    b.execute("INSERT INTO t VALUES (2)").unwrap();
    a.execute("COMMIT").unwrap();
    assert_eq!(
        rows(&a, "SELECT x FROM t ORDER BY x"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
    );
    b.execute("CREATE TABLE u (y)").unwrap();
}

/// A value `nextval` drew stays drawn whatever follows: a checkpoint, a transaction that writes
/// and rolls back, another connection's draws and a schema reparse after its DDL (DESIGN.md §17,
/// R8).
#[test]
fn a_drawn_sequence_value_stays_drawn_whatever_follows() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/seq-drawn");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (x)").unwrap();
    conn.execute("CREATE SEQUENCE s START 1 INCREMENT 5 MINVALUE 1 MAXVALUE 41 CYCLE")
        .unwrap();
    let draw = |conn: &Arc<Connection>| rows(conn, "SELECT nextval('s')");
    assert_eq!(draw(&conn), vec![vec![Value::from_i64(1)]]);
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(draw(&conn), vec![vec![Value::from_i64(6)]]);
    // Another connection's DDL: this one reads the schema again, its sequences among it.
    let other = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(draw(&other), vec![vec![Value::from_i64(11)]]);
    other.execute("CREATE TABLE u (y)").unwrap();
    assert_eq!(draw(&conn), vec![vec![Value::from_i64(16)]]);
    assert_eq!(draw(&other), vec![vec![Value::from_i64(21)]]);
}

/// A DROP SEQUENCE that meets Busy, behind another connection's open write, drops nothing: the
/// sequence draws on from where it stood (DESIGN.md §17, R8).
#[test]
fn a_drop_sequence_that_met_busy_leaves_the_sequence_drawing_on() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/seq-busy-drop");
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("CREATE TABLE t (x)").unwrap();
    a.execute("CREATE SEQUENCE s START 1 INCREMENT 5 MINVALUE 1 MAXVALUE 41 CYCLE")
        .unwrap();
    assert_eq!(
        rows(&a, "SELECT nextval('s')"),
        vec![vec![Value::from_i64(1)]]
    );
    a.execute("BEGIN").unwrap();
    a.execute("INSERT INTO t VALUES (1)").unwrap();
    b.execute("PRAGMA busy_timeout = 0").unwrap();
    let err = b.execute("DROP SEQUENCE s").unwrap_err();
    assert!(matches!(err, crate::LimboError::Busy), "{err:?}");
    a.execute("ROLLBACK").unwrap();
    assert_eq!(
        rows(&a, "SELECT nextval('s')"),
        vec![vec![Value::from_i64(6)]]
    );
}

/// A writing statement that waited behind another connection's DDL finds the schema changed when
/// it takes the gate, and is prepared again, its schema read again in a view the reparse pins; the
/// transaction it then begins in autocommit commits at its halt (DESIGN.md §17, §6): a value it
/// drew is drawn by nobody again.
#[test]
fn a_write_reprepared_after_waiting_behind_ddl_commits_at_its_halt() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/reprepared-write");
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("CREATE SEQUENCE s START 1 INCREMENT 5").unwrap();
    a.execute("PRAGMA busy_timeout = 0").unwrap();
    let mut draw = a.prepare("SELECT nextval('s')").unwrap();
    b.execute("BEGIN").unwrap();
    b.execute("CREATE TABLE z (x)").unwrap();
    assert!(matches!(
        draw.step(),
        Ok(crate::StepResult::Busy) | Err(crate::LimboError::Busy)
    ));
    b.execute("COMMIT").unwrap();
    let mut drawn = Vec::new();
    while let Some(row) = next_row(&mut draw) {
        drawn.push(row);
    }
    assert_eq!(drawn, vec![vec![Value::from_i64(1)]]);
    drop(draw);
    assert!(
        !a.atoll().unwrap().is_open(),
        "the reprepared statement's transaction ended at its halt"
    );
    assert_eq!(
        rows(&b, "SELECT nextval('s')"),
        vec![vec![Value::from_i64(6)]]
    );
}

/// A writing statement in autocommit that begins the Atoll transaction ends it at its halt, even
/// where the connection's state still read a transaction open, as a schema reparse once left it.
#[test]
fn an_autocommit_write_that_begins_the_transaction_commits_it_at_its_halt() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/stale-state");
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("CREATE TABLE t (x)").unwrap();
    assert!(!a.atoll().unwrap().is_open());
    a.set_tx_state(crate::connection::TransactionState::Read);
    a.execute("INSERT INTO t VALUES (1)").unwrap();
    assert!(
        !a.atoll().unwrap().is_open(),
        "the insert's transaction ended at its halt"
    );
    assert_eq!(rows(&b, "SELECT x FROM t"), vec![vec![Value::from_i64(1)]]);
}

/// A schema writer that reads rows of a tree, in a transaction whose snapshot a commit to that
/// tree has passed, fails with BusySnapshot before it takes the gate (DESIGN.md §17, §3, defect
/// 4): its index build would read the table at that snapshot and miss the rows committed since.
/// It does not wait behind a writer to find out. Begun again, it builds them all.
#[test]
fn a_schema_write_from_a_stale_snapshot_is_busy_snapshot() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/stale-ddl");
    let (a, b, c) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("CREATE TABLE t (x)").unwrap();
    a.execute("CREATE TABLE u (y)").unwrap();
    a.execute("INSERT INTO t VALUES (1)").unwrap();
    a.execute("PRAGMA busy_timeout = 0").unwrap();
    a.execute("BEGIN").unwrap();
    assert_eq!(rows(&a, "SELECT x FROM t"), vec![vec![Value::from_i64(1)]]);
    b.execute("INSERT INTO t VALUES (2)").unwrap();
    c.execute("BEGIN").unwrap();
    c.execute("INSERT INTO u VALUES (1)").unwrap();
    let err = a.execute("CREATE INDEX i ON t (x)").unwrap_err();
    assert!(matches!(err, crate::LimboError::BusySnapshot), "{err:?}");
    c.execute("COMMIT").unwrap();
    a.execute("ROLLBACK").unwrap();
    a.execute("CREATE INDEX i ON t (x)").unwrap();
    assert_eq!(rows(&a, "PRAGMA integrity_check"), vec![vec![text("ok")]]);
    assert_eq!(
        rows(&b, "SELECT x FROM t INDEXED BY i WHERE x > 0 ORDER BY x"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
    );
}

/// A commit that lands while a schema writer drains the gate, to a tree the writer reads, fails it
/// with BusySnapshot once it holds the gate (DESIGN.md §17, §3, defect 4): checked before it was
/// pending, the stamp had not moved yet.
#[test]
fn a_commit_during_a_schema_writers_drain_fails_it_once_held() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/stale-during-drain");
    let (a, c) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("CREATE TABLE t (x)").unwrap();
    a.execute("INSERT INTO t VALUES (1)").unwrap();
    a.execute("PRAGMA busy_timeout = 0").unwrap();
    a.execute("BEGIN").unwrap();
    assert_eq!(rows(&a, "SELECT x FROM t"), vec![vec![Value::from_i64(1)]]);
    c.execute("BEGIN").unwrap();
    c.execute("INSERT INTO t VALUES (2)").unwrap();
    let mut index = a.prepare("CREATE INDEX i ON t (x)").unwrap();
    assert!(matches!(
        index.step(),
        Ok(crate::StepResult::Busy) | Err(crate::LimboError::Busy)
    ));
    c.execute("COMMIT").unwrap();
    let err = loop {
        match index.step() {
            Ok(crate::StepResult::IO) => index._io().step().unwrap(),
            Ok(other) => panic!("{other:?}"),
            Err(e) => break e,
        }
    };
    assert!(matches!(err, crate::LimboError::BusySnapshot), "{err:?}");
    drop(index);
    a.execute("ROLLBACK").unwrap();
    a.execute("CREATE INDEX i ON t (x)").unwrap();
    assert_eq!(rows(&a, "PRAGMA integrity_check"), vec![vec![text("ok")]]);
}

/// The rows of `t` as `conn` reads them.
fn table_state(conn: &Arc<Connection>) -> std::collections::BTreeMap<i64, String> {
    rows(conn, "SELECT k, v FROM t ORDER BY k")
        .into_iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Numeric(crate::Numeric::Integer(k)), Value::Text(v)) => {
                (*k, v.as_str().to_string())
            }
            other => panic!("{other:?}"),
        })
        .collect()
}

/// A crash anywhere in a SQL workload leaves a database that opens and holds exactly the
/// transactions committed before it (DESIGN.md §17, §12): an image taken while transactions write
/// and commit recovers to the state after some prefix of them, never part of one; one taken once a
/// COMMIT has returned holds every transaction committed by then, and nothing after; and each
/// passes the integrity check. Images fall at every second operation, so they land inside
/// transactions, commits and the checkpoints that follow them.
#[test]
fn a_crash_anywhere_in_sql_recovers_a_prefix_of_its_commits() {
    use crate::io::crash::CrashIO;
    use std::collections::{BTreeMap, BTreeSet};
    const TRANSACTIONS: i64 = 30;
    for seed in 0..2u64 {
        let path = format!("/db/sql-crash-{seed}");
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let db = open(&io, &path);
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        let mut model: BTreeMap<i64, String> = BTreeMap::new();
        let mut states = vec![model.clone()];
        let mut acked = Vec::new();
        crash.capture(|_| false, 2, seed);
        for j in 1..=TRANSACTIONS {
            let filler = "x".repeat(300);
            conn.execute("BEGIN").unwrap();
            conn.execute(format!("INSERT INTO t VALUES ({j}, 'v{j}-{filler}')"))
                .unwrap();
            model.insert(j, format!("v{j}-{filler}"));
            if j > 2 {
                conn.execute(format!("UPDATE t SET v = 'u{j}' WHERE k = {}", j / 2))
                    .unwrap();
                if let Some(v) = model.get_mut(&(j / 2)) {
                    *v = format!("u{j}");
                }
            }
            if j > 4 && j % 3 == 0 {
                conn.execute(format!("DELETE FROM t WHERE k = {}", j - 4))
                    .unwrap();
                model.remove(&(j - 4));
            }
            conn.execute("COMMIT").unwrap();
            states.push(model.clone());
            acked.push((crash.crash(seed * 1000 + j as u64), j as usize));
        }
        let images = crash.captured();
        drop(conn);
        drop(db);
        let recover = |image: CrashIO, label: &str| {
            let io: Arc<dyn IO> = Arc::new(image);
            let db = open(&io, &path);
            let conn = db.connect(ConnectOpts::new()).unwrap();
            let state = table_state(&conn);
            assert_eq!(
                rows(&conn, "PRAGMA integrity_check"),
                vec![vec![text("ok")]],
                "{label}"
            );
            state
        };
        for (image, j) in acked {
            let label = format!("seed {seed}, after commit {j}");
            assert_eq!(recover(image, &label), states[j], "{label}");
        }
        // Every commit acknowledged completed at least one write of the log, and an image falls
        // at every second operation completed.
        assert!(
            images.len() >= TRANSACTIONS as usize / 2,
            "seed {seed}: {} images",
            images.len()
        );
        let mut reached = BTreeSet::new();
        for (i, image) in images.into_iter().enumerate() {
            let label = format!("seed {seed}, image {i}");
            let state = recover(image, &label);
            let prefix = states.iter().position(|s| *s == state);
            assert!(
                prefix.is_some(),
                "{label}: no prefix of the commits: {state:?}"
            );
            reached.extend(prefix);
        }
        assert!(
            reached.len() > TRANSACTIONS as usize / 2,
            "seed {seed}: images fell after only {} distinct prefixes",
            reached.len()
        );
    }
}

/// Two connections with foreign keys on, over a parent `p` and a child `c` made by `schema`, with
/// parent 1 in place.
fn fk_pair(path: &str, schema: &[&str]) -> (Arc<Connection>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, path);
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("PRAGMA foreign_keys = ON").unwrap();
    b.execute("PRAGMA foreign_keys = ON").unwrap();
    for sql in schema {
        a.execute(sql).unwrap();
    }
    (a, b)
}

/// Every child row whose parent is missing, over the tables the foreign-key tests make: `c` and
/// `d` naming `p` by `pid` (its rowid) or `pk` (its UNIQUE `k`). Turso has no PRAGMA
/// foreign_key_check: an unknown pragma reads as no rows, whatever is there.
fn orphans(conn: &Arc<Connection>) -> Vec<Vec<Value>> {
    let mut found = Vec::new();
    for (child, column, parent) in [("c", "pid", "id"), ("c", "pk", "k"), ("d", "pid", "id")] {
        let has = rows(conn, &format!("PRAGMA table_info({child})"))
            .iter()
            .any(|r| r.get(1) == Some(&text(column)));
        if has {
            found.extend(rows(
                conn,
                &format!(
                    "SELECT '{child}', id FROM {child} WHERE {column} IS NOT NULL AND NOT EXISTS \
                     (SELECT 1 FROM p WHERE p.{parent} = {child}.{column})"
                ),
            ));
        }
    }
    found
}

/// A parent deleted while another transaction holds a child of it open waits for that child's
/// writer, and conflicts once it commits, so the child never outlives its parent (R10): the delete
/// writes first and then probes the children every version the snapshot cannot see, through a
/// scan of the child table, which has no index on its key. Without the probe both commit and the
/// child is an orphan.
#[test]
fn a_parent_delete_waits_for_a_child_held_open_and_then_conflicts() {
    for (path, index) in [
        ("/db/fk-scan", None),
        ("/db/fk-index", Some("CREATE INDEX c_pid ON c(pid)")),
    ] {
        let mut schema = vec![
            "CREATE TABLE p(id INTEGER PRIMARY KEY)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
            "INSERT INTO p VALUES (1), (2)",
        ];
        schema.extend(index);
        let (a, b) = fk_pair(path, &schema);
        a.execute("BEGIN").unwrap();
        a.execute("INSERT INTO c VALUES (1, 1)").unwrap();
        b.execute("BEGIN").unwrap();
        b.execute("DELETE FROM p WHERE id = 2").unwrap();
        let mut delete = b.prepare("DELETE FROM p WHERE id = 1").unwrap();
        assert!(
            matches!(step_until_busy(&mut delete), Ok(crate::StepResult::Busy)),
            "{path}: the delete went on past a child held open"
        );
        a.execute("COMMIT").unwrap();
        let err = step_until_busy(&mut delete).unwrap_err();
        assert!(
            matches!(err, crate::LimboError::WriteWriteConflict),
            "{path}: {err}"
        );
        drop(delete);
        assert!(
            b.get_auto_commit(),
            "{path}: the conflict rolled the transaction back"
        );
        assert_eq!(orphans(&b), Vec::<Vec<Value>>::new(), "{path}");
        assert_eq!(
            rows(&b, "SELECT id FROM p ORDER BY id"),
            vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]],
            "{path}: the rolled-back transaction's other delete went with it"
        );
    }
}

/// A child inserted while another transaction holds its parent's delete open waits for that
/// writer, and conflicts once it commits (R10): the insert writes first and then probes the
/// parent key's versions the snapshot cannot see, here by rowid and through a UNIQUE index.
#[test]
fn a_child_insert_waits_for_its_parents_delete_held_open_and_then_conflicts() {
    for (path, schema, insert) in [
        (
            "/db/fk-child-rowid",
            [
                "CREATE TABLE p(id INTEGER PRIMARY KEY)",
                "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
                "INSERT INTO p VALUES (1)",
            ],
            "INSERT INTO c VALUES (1, 1)",
        ),
        (
            "/db/fk-child-unique",
            [
                "CREATE TABLE p(id INTEGER PRIMARY KEY, k TEXT UNIQUE)",
                "CREATE TABLE c(id INTEGER PRIMARY KEY, pk TEXT REFERENCES p(k))",
                "INSERT INTO p VALUES (1, 'one')",
            ],
            "INSERT INTO c VALUES (1, 'one')",
        ),
    ] {
        let (a, b) = fk_pair(path, &schema);
        a.execute("BEGIN").unwrap();
        a.execute("DELETE FROM p WHERE id = 1").unwrap();
        b.execute("BEGIN").unwrap();
        let mut child = b.prepare(insert).unwrap();
        assert!(
            matches!(step_until_busy(&mut child), Ok(crate::StepResult::Busy)),
            "{path}: the child went in past its parent's delete held open"
        );
        a.execute("COMMIT").unwrap();
        let err = step_until_busy(&mut child).unwrap_err();
        assert!(
            matches!(err, crate::LimboError::WriteWriteConflict),
            "{path}: {err}"
        );
        drop(child);
        assert!(b.get_auto_commit(), "{path}");
        assert_eq!(orphans(&b), Vec::<Vec<Value>>::new(), "{path}");
        assert_eq!(
            rows(&b, "SELECT count(*) FROM c"),
            vec![vec![Value::from_i64(0)]],
            "{path}"
        );
    }
}

/// `held` runs in `a`'s open transaction and `blocked` then in `b`'s: `blocked` comes back `Busy`
/// on `a`'s open version, and `WriteWriteConflict` once `a` commits, which rolls `b` back; no
/// child is left without its parent.
fn fk_race(path: &str, schema: &[&str], held: &[&str], blocked: &str) {
    let (a, b) = fk_pair(path, schema);
    a.execute("BEGIN").unwrap();
    for sql in held {
        a.execute(sql).unwrap();
    }
    b.execute("BEGIN").unwrap();
    let mut stmt = b.prepare(blocked).unwrap();
    assert!(
        matches!(step_until_busy(&mut stmt), Ok(crate::StepResult::Busy)),
        "{path}: {blocked} went on past {held:?} held open"
    );
    a.execute("COMMIT").unwrap();
    let err = step_until_busy(&mut stmt).unwrap_err();
    assert!(
        matches!(err, crate::LimboError::WriteWriteConflict),
        "{path}: {err}"
    );
    drop(stmt);
    assert!(
        b.get_auto_commit(),
        "{path}: the conflict rolled the transaction back"
    );
    assert_eq!(orphans(&b), Vec::<Vec<Value>>::new(), "{path}");
}

/// `held` runs in `a`'s open transaction and `other` then in `b`'s, at once: nothing of `a`'s is
/// one it guards against.
fn fk_no_wait(path: &str, schema: &[&str], held: &[&str], other: &str) {
    let (a, b) = fk_pair(path, schema);
    a.execute("BEGIN").unwrap();
    for sql in held {
        a.execute(sql).unwrap();
    }
    b.execute("BEGIN").unwrap();
    let mut stmt = b.prepare(other).unwrap();
    assert!(
        matches!(step_until_busy(&mut stmt), Ok(crate::StepResult::Done)),
        "{path}: {other} waited on {held:?}"
    );
    drop(stmt);
    b.execute("COMMIT").unwrap();
    a.execute("COMMIT").unwrap();
    assert_eq!(orphans(&b), Vec::<Vec<Value>>::new(), "{path}");
}

const FK_ROWID_PARENT: [&str; 3] = [
    "CREATE TABLE p(id INTEGER PRIMARY KEY, note TEXT)",
    "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
    "INSERT INTO p VALUES (1, 'x'), (2, 'y')",
];

const FK_UNIQUE_PARENT: [&str; 3] = [
    "CREATE TABLE p(id INTEGER PRIMARY KEY, k TEXT UNIQUE, note TEXT)",
    "CREATE TABLE c(id INTEGER PRIMARY KEY, pk TEXT REFERENCES p(k))",
    "INSERT INTO p VALUES (1, 'one', 'x'), (2, 'two', 'y')",
];

/// A parent key removed every way a statement removes one owes its guard (R10): ON DELETE
/// CASCADE and SET NULL, whose actions read the children the snapshot sees, an UPDATE or an
/// UPSERT of the key, and a REPLACE that puts back a row with another key, each with or without
/// an index on the child's key.
#[test]
fn every_way_of_removing_a_parent_key_waits_for_a_child_held_open() {
    let cascade = [
        "CREATE TABLE p(id INTEGER PRIMARY KEY, note TEXT)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE)",
        "INSERT INTO p VALUES (1, 'x')",
    ];
    let set_null = [
        "CREATE TABLE p(id INTEGER PRIMARY KEY, note TEXT)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE SET NULL)",
        "INSERT INTO p VALUES (1, 'x')",
    ];
    let index = "CREATE INDEX c_pid ON c(pid)";
    let held = ["INSERT INTO c VALUES (1, 1)"];
    for (path, schema, blocked) in [
        ("/db/fk-cascade", &cascade, "DELETE FROM p WHERE id = 1"),
        ("/db/fk-set-null", &set_null, "DELETE FROM p WHERE id = 1"),
        (
            "/db/fk-update-rowid",
            &FK_ROWID_PARENT,
            "UPDATE p SET id = 10 WHERE id = 1",
        ),
    ] {
        fk_race(path, schema, &held, blocked);
        let mut indexed = schema.to_vec();
        indexed.push(index);
        fk_race(&format!("{path}-index"), &indexed, &held, blocked);
    }
    let held = ["INSERT INTO c VALUES (1, 'one')"];
    for (path, blocked) in [
        ("/db/fk-update-key", "UPDATE p SET k = 'uno' WHERE id = 1"),
        (
            "/db/fk-upsert-key",
            "INSERT INTO p VALUES (1, 'other', 'z') ON CONFLICT(id) DO UPDATE SET k = 'uno'",
        ),
        (
            "/db/fk-replace-key",
            "INSERT OR REPLACE INTO p VALUES (1, 'uno', 'z')",
        ),
    ] {
        fk_race(path, &FK_UNIQUE_PARENT, &held, blocked);
        let mut indexed = FK_UNIQUE_PARENT.to_vec();
        indexed.push("CREATE INDEX c_pk ON c(pk)");
        fk_race(&format!("{path}-index"), &indexed, &held, blocked);
    }
}

/// What leaves a parent's key in place owes nothing, and neither does a key no child names (R10,
/// key-share): a child goes in beside a non-key update of its parent held open, and a non-key
/// update, a REPLACE that puts the same key back, an UPDATE that sets the key to itself, or a
/// delete of another parent goes on beside a child held open, with or without an index on the
/// child's key.
#[test]
fn what_leaves_a_parent_key_in_place_does_not_wait() {
    fk_no_wait(
        "/db/fk-share-child",
        &FK_ROWID_PARENT,
        &["UPDATE p SET note = 'z' WHERE id = 1"],
        "INSERT INTO c VALUES (1, 1)",
    );
    fk_no_wait(
        "/db/fk-share-child-unique",
        &FK_UNIQUE_PARENT,
        &["UPDATE p SET note = 'z' WHERE id = 1"],
        "INSERT INTO c VALUES (1, 'one')",
    );
    for indexed in [false, true] {
        let mut rowid = FK_ROWID_PARENT.to_vec();
        let mut unique = FK_UNIQUE_PARENT.to_vec();
        if indexed {
            rowid.push("CREATE INDEX c_pid ON c(pid)");
            unique.push("CREATE INDEX c_pk ON c(pk)");
        }
        let held = ["INSERT INTO c VALUES (1, 1)"];
        for (n, other) in [
            "UPDATE p SET note = 'z' WHERE id = 1",
            "INSERT OR REPLACE INTO p VALUES (1, 'z')",
            "UPDATE p SET id = 1 WHERE id = 1",
            "DELETE FROM p WHERE id = 2",
        ]
        .into_iter()
        .enumerate()
        {
            fk_no_wait(
                &format!("/db/fk-share-rowid-{indexed}-{n}"),
                &rowid,
                &held,
                other,
            );
        }
        let held = ["INSERT INTO c VALUES (1, 'one')"];
        for (n, other) in [
            "UPDATE p SET note = 'z' WHERE id = 1",
            "INSERT OR REPLACE INTO p VALUES (1, 'one', 'z')",
            "UPDATE p SET k = 'one' WHERE id = 1",
            "DELETE FROM p WHERE id = 2",
        ]
        .into_iter()
        .enumerate()
        {
            fk_no_wait(
                &format!("/db/fk-share-unique-{indexed}-{n}"),
                &unique,
                &held,
                other,
            );
        }
    }
}

/// A key held only by a version between the newest and the one the snapshot sees is judged too
/// (R10, audit 4): a rollback of the newest would bring it back. On the parent side, a child
/// committed after the snapshot whose delete is held open makes the parent's delete conflict;
/// on the child side, a parent delete committed after the snapshot under a re-insert held open
/// makes the child's insert conflict. Neither waits for the open writer: the version below it
/// already decides.
#[test]
fn a_key_held_only_by_a_version_below_the_newest_is_judged() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/fk-between");
    let conns: Vec<_> = (0..3)
        .map(|_| {
            let c = db.connect(ConnectOpts::new()).unwrap();
            c.execute("PRAGMA foreign_keys = ON").unwrap();
            c
        })
        .collect();
    let (t1, t2, t3) = (&conns[0], &conns[1], &conns[2]);
    for sql in FK_ROWID_PARENT {
        t1.execute(sql).unwrap();
    }
    // The parent side.
    t1.execute("BEGIN").unwrap();
    rows(t1, "SELECT count(*) FROM p");
    t2.execute("INSERT INTO c VALUES (5, 1)").unwrap();
    t3.execute("BEGIN").unwrap();
    t3.execute("DELETE FROM c WHERE id = 5").unwrap();
    let err = t1.execute("DELETE FROM p WHERE id = 1").unwrap_err();
    assert!(
        matches!(err, crate::LimboError::WriteWriteConflict),
        "{err}"
    );
    assert!(t1.get_auto_commit());
    t3.execute("ROLLBACK").unwrap();
    assert_eq!(orphans(t1), Vec::<Vec<Value>>::new());
    t1.execute("DELETE FROM c").unwrap();
    // The child side.
    t1.execute("BEGIN").unwrap();
    rows(t1, "SELECT count(*) FROM p");
    t2.execute("DELETE FROM p WHERE id = 1").unwrap();
    t3.execute("BEGIN").unwrap();
    t3.execute("INSERT INTO p VALUES (1, 'back')").unwrap();
    let err = t1.execute("INSERT INTO c VALUES (6, 1)").unwrap_err();
    assert!(
        matches!(err, crate::LimboError::WriteWriteConflict),
        "{err}"
    );
    assert!(t1.get_auto_commit());
    t3.execute("ROLLBACK").unwrap();
    assert_eq!(orphans(t1), Vec::<Vec<Value>>::new());
    assert_eq!(
        rows(t1, "SELECT count(*) FROM c"),
        vec![vec![Value::from_i64(0)]]
    );
}

/// A deferred foreign key is guarded at statement time like an immediate one (R10): its count is
/// checked at commit, but the write and its guard happen now.
#[test]
fn a_deferred_foreign_key_is_guarded_at_its_statement() {
    let schema = [
        "CREATE TABLE p(id INTEGER PRIMARY KEY)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, \
         pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
        "INSERT INTO p VALUES (1)",
    ];
    fk_race(
        "/db/fk-deferred-child",
        &schema,
        &["DELETE FROM p WHERE id = 1"],
        "INSERT INTO c VALUES (1, 1)",
    );
    fk_race(
        "/db/fk-deferred-parent",
        &schema,
        &["INSERT INTO c VALUES (1, 1)"],
        "DELETE FROM p WHERE id = 1",
    );
}

/// A child key written every way a statement writes one owes its guard (R10): an UPDATE of the
/// key, an UPSERT that sets it, a multi-row insert, a trigger's insert, and ON DELETE SET
/// DEFAULT's action, which points the children of a deleted parent at another one. The trigger and
/// the action run as subprograms, which carry the guards with them.
#[test]
fn every_way_of_writing_a_child_key_waits_for_its_parents_delete_held_open() {
    let schema = [
        "CREATE TABLE p(id INTEGER PRIMARY KEY)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 2 \
         REFERENCES p(id) ON DELETE SET DEFAULT)",
        "CREATE TABLE log(x INTEGER)",
        "CREATE TRIGGER log_child AFTER INSERT ON log BEGIN INSERT INTO c VALUES (NEW.x, 2); END",
        "INSERT INTO p VALUES (1), (2), (3)",
        "INSERT INTO c VALUES (9, 1)",
    ];
    let held = ["DELETE FROM p WHERE id = 2"];
    for (path, blocked) in [
        ("/db/fk-child-update", "UPDATE c SET pid = 2 WHERE id = 9"),
        (
            "/db/fk-child-upsert",
            "INSERT INTO c VALUES (9, 3) ON CONFLICT(id) DO UPDATE SET pid = 2",
        ),
        (
            "/db/fk-child-multirow",
            "INSERT INTO c VALUES (10, 3), (11, 2)",
        ),
        ("/db/fk-child-trigger", "INSERT INTO log VALUES (12)"),
        ("/db/fk-child-set-default", "DELETE FROM p WHERE id = 1"),
    ] {
        fk_race(path, &schema, &held, blocked);
    }
}

/// An explicit transaction capturing its changes commits: its COMMIT writes the capture's commit
/// record without becoming a write statement itself, which COMMIT refuses to run beside.
#[test]
fn a_captured_explicit_transaction_commits() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/cdc-commit");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y UNIQUE)")
        .unwrap();
    conn.execute("PRAGMA capture_data_changes_conn('id,custom_cdc')")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    conn.execute("DELETE FROM t WHERE x = 1").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        rows(&conn, "SELECT count(*) FROM custom_cdc"),
        vec![vec![Value::from_i64(3)]]
    );
}

/// Twelve connections race parent deletes, inserts and key changes against child inserts,
/// re-points and deletes, two statements a transaction, over a child with no index on its key and
/// one with (R10): a conflict, a deadlock or a failed constraint rolls its transaction back, and no
/// snapshot a checker takes all through the run, nor the end, shows a child without its parent.
/// Without the guards the two sides of a race both commit; a parent put back later can heal the
/// orphan, which is why the checker reads while they run.
#[test]
fn twelve_writers_racing_parents_and_children_leave_no_orphan() {
    const THREADS: usize = 12;
    const ROUNDS: usize = 40;
    const PARENTS: i64 = 4;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/fk-stress");
    let setup = db.connect(ConnectOpts::new()).unwrap();
    setup.execute("PRAGMA foreign_keys = ON").unwrap();
    for sql in [
        "CREATE TABLE p(id INTEGER PRIMARY KEY, note INTEGER)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
        "CREATE TABLE d(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
        "CREATE INDEX d_pid ON d(pid)",
    ] {
        setup.execute(sql).unwrap();
    }
    for id in 1..=PARENTS {
        setup
            .execute(format!("INSERT INTO p VALUES ({id}, 0)"))
            .unwrap();
    }
    let start = Arc::new(std::sync::Barrier::new(THREADS + 1));
    // Halfway, every writer pauses between transactions and waits on `release`; the checker reads
    // one snapshot after the last has paused and before it releases them, so a read provably falls
    // inside the run whatever the scheduler does (no count of reads is timed against the writers).
    let paused = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let release = Arc::new(std::sync::Barrier::new(THREADS + 1));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let checker = {
        let (db, start, paused, release, running) = (
            db.clone(),
            start.clone(),
            paused.clone(),
            release.clone(),
            running.clone(),
        );
        std::thread::spawn(move || {
            let conn = db.connect(ConnectOpts::new()).unwrap();
            // Its begins wait for a slot behind the writers' (R13).
            conn.set_busy_timeout(std::time::Duration::from_secs(30));
            start.wait();
            let (mut checks, mut seen) = (0usize, Vec::new());
            let mut check = |checks: &mut usize| {
                let found = orphans(&conn);
                *checks += 1;
                if !found.is_empty() && seen.is_empty() {
                    seen = found;
                }
            };
            // Snapshots while the first half runs, each a real read.
            while paused.load(std::sync::atomic::Ordering::Acquire) < THREADS {
                check(&mut checks);
            }
            // Every writer paused: the read inside the run, then the rest of the run.
            check(&mut checks);
            let at_pause = checks;
            release.wait();
            while running.load(std::sync::atomic::Ordering::Acquire) {
                check(&mut checks);
            }
            (checks, at_pause, seen)
        })
    };
    let threads: Vec<_> = (0..THREADS)
        .map(|who| {
            let (db, start, paused, release) =
                (db.clone(), start.clone(), paused.clone(), release.clone());
            std::thread::spawn(move || {
                let conn = db.connect(ConnectOpts::new()).unwrap();
                conn.execute("PRAGMA foreign_keys = ON").unwrap();
                conn.set_busy_timeout(std::time::Duration::from_secs(30));
                let mut state = who as u64 * 2654435761 + 7;
                let mut next = |n: i64| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state % n as u64) as i64 + 1
                };
                start.wait();
                let mut ended = [0usize; 4];
                for round in 0..ROUNDS as i64 {
                    let mut op = |n: i64| -> Vec<String> {
                        let t = if next(2) == 1 { "c" } else { "d" };
                        let (k, k2) = (next(PARENTS), next(PARENTS));
                        let uid = (who as i64 * ROUNDS as i64 + round) * 2 + n;
                        match next(6) {
                            1 | 2 => vec![format!("INSERT INTO {t} VALUES ({uid}, {k})")],
                            // A parent purged: its children the snapshot sees, then itself.
                            3 => vec![
                                format!("DELETE FROM c WHERE pid = {k}"),
                                format!("DELETE FROM d WHERE pid = {k}"),
                                format!("DELETE FROM p WHERE id = {k}"),
                            ],
                            4 => vec![format!("INSERT OR IGNORE INTO p VALUES ({k}, 0)")],
                            5 => vec![format!("UPDATE {t} SET pid = {k} WHERE pid = {k2}")],
                            _ => vec![format!("UPDATE OR IGNORE p SET id = {k2} WHERE id = {k}")],
                        }
                    };
                    let statements: Vec<String> = op(0).into_iter().chain(op(1)).collect();
                    conn.execute("BEGIN").unwrap();
                    let done = statements
                        .iter()
                        .try_for_each(|sql| conn.execute(sql))
                        .and_then(|_| conn.execute("COMMIT"));
                    let slot = match done {
                        Ok(()) => 0,
                        Err(crate::LimboError::WriteWriteConflict) => 1,
                        Err(crate::LimboError::Deadlock) => 2,
                        Err(
                            crate::LimboError::Constraint(_)
                            | crate::LimboError::ConstraintViolation(_)
                            | crate::LimboError::ForeignKeyConstraint(_),
                        ) => 3,
                        Err(e) => panic!("thread {who}: {statements:?}: {e}"),
                    };
                    ended[slot] += 1;
                    if !conn.get_auto_commit() {
                        conn.execute("ROLLBACK").unwrap();
                    }
                    if round == ROUNDS as i64 / 2 {
                        paused.fetch_add(1, std::sync::atomic::Ordering::Release);
                        release.wait();
                    }
                }
                ended
            })
        })
        .collect();
    let mut ended = [0usize; 4];
    for t in threads {
        for (sum, n) in ended.iter_mut().zip(t.join().unwrap()) {
            *sum += n;
        }
    }
    running.store(false, std::sync::atomic::Ordering::Release);
    let (checks, at_pause, seen) = checker.join().unwrap();
    // The races met depend on the schedule, so they are reported, not asserted; the tests of
    // conflicts and deadlocks force their own.
    eprintln!(
        "fk stress: {} committed, {} conflicts, {} deadlocks, {} constraint failures; {checks} \
         checks, {at_pause} by the writers' pause",
        ended[0], ended[1], ended[2], ended[3]
    );
    assert_eq!(
        seen,
        Vec::<Vec<Value>>::new(),
        "a snapshot showed an orphan"
    );
    assert_eq!(orphans(&setup), Vec::<Vec<Value>>::new());
    assert!(ended[0] > 0, "nothing committed");
}

/// The orphan check the foreign-key tests read sees an orphan: one written with foreign keys off,
/// on a child with no index on its key and on one with.
#[test]
fn the_orphan_check_reports_an_orphan() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/fk-orphan-control");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    for sql in [
        "CREATE TABLE p(id INTEGER PRIMARY KEY)",
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
        "CREATE TABLE d(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
        "CREATE INDEX d_pid ON d(pid)",
        "INSERT INTO p VALUES (1)",
        "INSERT INTO c VALUES (1, 1), (2, 7)",
        "INSERT INTO d VALUES (1, 8)",
    ] {
        conn.execute(sql).unwrap();
    }
    assert_eq!(orphans(&conn).len(), 2, "{:?}", orphans(&conn));
}

/// An UPDATE that assigns no child key of a foreign key whose parent has no UNIQUE key goes on, as
/// in SQLite: the mismatch is an error only where the key is used, and the guards resolve no
/// foreign key the counters do not, a self-reference's included.
#[test]
fn an_update_of_another_column_ignores_a_mismatched_foreign_key() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/fk-mismatch-other-column");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE c(id INTEGER PRIMARY KEY, x, y REFERENCES c(x))")
        .unwrap();
    conn.execute("INSERT INTO c VALUES (1, 1, NULL)").unwrap();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    conn.execute("UPDATE c SET id = 7").unwrap();
    assert_eq!(
        rows(&conn, "SELECT id, x, y FROM c"),
        vec![vec![Value::from_i64(7), Value::from_i64(1), Value::Null]]
    );
}

/// NoConflict on a UNIQUE index judges a key by its newest version (A3.12): an autocommit insert
/// of a value another open transaction is deleting waits for that delete, then restarts on its
/// commit and goes in, where judged by the snapshot alone it would fail its constraint. A visible
/// present version is a UNIQUE violation as ever.
#[test]
fn a_unique_insert_waits_for_a_delete_of_its_value_held_open() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/no-conflict-waits");
    let a = db.connect(ConnectOpts::new()).unwrap();
    a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT UNIQUE)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("DELETE FROM t WHERE k = 'a'").unwrap();
    let behind = {
        let db = db.clone();
        std::thread::spawn(move || {
            let b = db.connect(ConnectOpts::new()).unwrap();
            b.set_busy_timeout(std::time::Duration::from_secs(30));
            b.execute("INSERT INTO t VALUES (3, 'a')")
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert!(
        !behind.is_finished(),
        "the insert did not wait for the delete held open"
    );
    a.execute("COMMIT").unwrap();
    behind.join().unwrap().unwrap();
    assert_eq!(
        rows(&a, "SELECT id, k FROM t ORDER BY id"),
        vec![
            vec![Value::from_i64(2), text("b")],
            vec![Value::from_i64(3), text("a")]
        ]
    );
    let err = a.execute("INSERT INTO t VALUES (4, 'b')").unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
}

/// An explicit transaction whose insert meets a UNIQUE value deleted by a commit after its
/// snapshot conflicts, as under PostgreSQL's REPEATABLE READ (A3.12).
#[test]
fn a_unique_insert_in_a_transaction_conflicts_with_a_later_delete_of_its_value() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/no-conflict-conflicts");
    let (a, b) = (
        db.connect(ConnectOpts::new()).unwrap(),
        db.connect(ConnectOpts::new()).unwrap(),
    );
    a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT UNIQUE)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    b.execute("BEGIN").unwrap();
    rows(&b, "SELECT count(*) FROM t");
    a.execute("DELETE FROM t WHERE k = 'a'").unwrap();
    let err = b.execute("INSERT INTO t VALUES (3, 'a')").unwrap_err();
    assert!(
        matches!(err, crate::LimboError::WriteWriteConflict),
        "{err}"
    );
    assert!(b.get_auto_commit());
}

/// An insert's rowid check judges the rowid by its newest version (A4.9): an autocommit insert of
/// a rowid another open transaction is deleting waits for that delete, then restarts on its
/// commit and goes in, where judged by the snapshot alone it would fail its primary key.
#[test]
fn an_insert_of_a_rowid_waits_for_its_delete_held_open() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/key-probe-waits");
    let a = db.connect(ConnectOpts::new()).unwrap();
    a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("DELETE FROM t WHERE id = 1").unwrap();
    let behind = {
        let db = db.clone();
        std::thread::spawn(move || {
            let b = db.connect(ConnectOpts::new()).unwrap();
            b.set_busy_timeout(std::time::Duration::from_secs(30));
            b.execute("INSERT INTO t VALUES (1, 'new')")
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert!(
        !behind.is_finished(),
        "the insert did not wait for the delete held open"
    );
    a.execute("COMMIT").unwrap();
    behind.join().unwrap().unwrap();
    assert_eq!(
        rows(&a, "SELECT id, v FROM t"),
        vec![vec![Value::from_i64(1), text("new")]]
    );
}

/// Runs `stmt` with `values` bound, to its end, and resets it.
fn run_with(stmt: &mut crate::Statement, values: &[Value]) {
    for (i, v) in values.iter().enumerate() {
        stmt.bind_at(std::num::NonZero::new(i + 1).unwrap(), v.clone())
            .unwrap();
    }
    stmt.run_ignore_rows().unwrap();
    stmt.reset().unwrap();
}

fn cursors_built() -> u64 {
    crate::storage::atoll::sql::cursor::CURSORS_BUILT.with(|n| n.get())
}

/// A prepared statement run again takes up the cursors its last run parked, within a transaction
/// and across transactions, and a read through one reads what the writes put there.
#[test]
fn a_statement_run_again_takes_up_the_cursors_it_parked() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/parked");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    let mut insert = conn.prepare("INSERT INTO t VALUES (?, ?)").unwrap();
    let row = |i: i64| [Value::from_i64(i), text(&format!("r{i}"))];
    conn.execute("BEGIN").unwrap();
    run_with(&mut insert, &row(0));
    let mut built = 0;
    for i in 1..20 {
        let before = cursors_built();
        run_with(&mut insert, &row(i));
        built += cursors_built() - before;
    }
    conn.execute("COMMIT").unwrap();
    for i in 20..25 {
        let before = cursors_built();
        run_with(&mut insert, &row(i));
        built += cursors_built() - before;
    }
    assert_eq!(built, 0, "cursors built for the inserts after the first");
    let mut select = conn.prepare("SELECT b FROM t WHERE a = ?").unwrap();
    for i in [3, 17, 24, 3] {
        select
            .bind_at(std::num::NonZero::<usize>::MIN, Value::from_i64(i))
            .unwrap();
        assert_eq!(next_row(&mut select), Some(vec![text(&format!("r{i}"))]));
        assert_eq!(next_row(&mut select), None);
        select.reset().unwrap();
    }
}

/// A parked cursor over a tree a Clear replaced since is not taken up: the statement's next run
/// reads the tree the Clear made.
#[test]
fn a_parked_cursor_over_a_tree_a_clear_replaced_reads_the_new_tree() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/parked-clear");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    for i in 0..30 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'r{i}')"))
            .unwrap();
    }
    let mut count = conn.prepare("SELECT count(*), max(b) FROM t").unwrap();
    assert_eq!(
        next_row(&mut count),
        Some(vec![Value::from_i64(30), text("r9")])
    );
    assert_eq!(next_row(&mut count), None);
    count.reset().unwrap();
    conn.execute("DELETE FROM t").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'new')").unwrap();
    assert_eq!(
        next_row(&mut count),
        Some(vec![Value::from_i64(1), text("new")])
    );
}

/// A statement prepared again after another connection's DDL writes through what its new program
/// opens: a cursor its old program parked is checked again against the tree and catalog row the
/// new one opens, as any parked cursor is.
#[test]
fn a_statement_prepared_again_writes_through_its_new_program() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/parked-reprepare");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    let mut insert = conn.prepare("INSERT INTO t VALUES (?, ?)").unwrap();
    run_with(&mut insert, &[Value::from_i64(1), text("one")]);
    run_with(&mut insert, &[Value::from_i64(2), text("two")]);
    other.execute("CREATE TABLE u (x)").unwrap();
    run_with(&mut insert, &[Value::from_i64(3), text("three")]);
    assert_eq!(
        rows(&conn, "SELECT a, b FROM t ORDER BY a"),
        vec![
            vec![Value::from_i64(1), text("one")],
            vec![Value::from_i64(2), text("two")],
            vec![Value::from_i64(3), text("three")],
        ]
    );
}

/// A statement whose write came back busy inside a transaction, reset, leaves no wait in the wait
/// graph: a later wait on that transaction closes no cycle through it, so the transaction is no
/// deadlock's victim, and the statement run again writes.
#[test]
fn a_busy_statement_reset_leaves_no_wait_behind() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/parked-wait");
    let a = db.connect(ConnectOpts::new()).unwrap();
    let b = db.connect(ConnectOpts::new()).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 0), (2, 0)").unwrap();
    let mut update = b.prepare("UPDATE t SET n = n + 1 WHERE id = ?").unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("UPDATE t SET n = 10 WHERE id = 1").unwrap();
    b.execute("BEGIN").unwrap();
    update
        .bind_at(std::num::NonZero::<usize>::MIN, Value::from_i64(1))
        .unwrap();
    let busy = update.run_ignore_rows();
    assert!(matches!(busy, Err(crate::LimboError::Busy)), "{busy:?}");
    update.reset().unwrap();
    b.execute("UPDATE t SET n = 20 WHERE id = 2").unwrap();
    let behind = a.execute("UPDATE t SET n = 11 WHERE id = 2");
    assert!(matches!(behind, Err(crate::LimboError::Busy)), "{behind:?}");
    update
        .bind_at(std::num::NonZero::<usize>::MIN, Value::from_i64(2))
        .unwrap();
    let again = update.run_ignore_rows();
    assert!(again.is_ok(), "{again:?}");
    update.reset().unwrap();
    assert_eq!(
        rows(&b, "SELECT n FROM t WHERE id = 2"),
        vec![vec![Value::from_i64(21)]]
    );
    b.execute("ROLLBACK").unwrap();
    a.execute("ROLLBACK").unwrap();
}

/// A statement whose write came back busy, reset and run again, writes its next run's row: the
/// write the busy run left is not taken up with its cursor.
#[test]
fn a_statement_run_again_after_a_busy_write_writes_its_own_row() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/parked-busy");
    let a = db.connect(ConnectOpts::new()).unwrap();
    let b = db.connect(ConnectOpts::new()).unwrap();
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
        .unwrap();
    a.execute("INSERT INTO t VALUES (1, 0), (2, 0)").unwrap();
    let mut update = b.prepare("UPDATE t SET n = n + 1 WHERE id = ?").unwrap();
    a.execute("BEGIN").unwrap();
    a.execute("UPDATE t SET n = 10 WHERE id = 1").unwrap();
    update
        .bind_at(std::num::NonZero::<usize>::MIN, Value::from_i64(1))
        .unwrap();
    let busy = update.run_ignore_rows();
    assert!(matches!(busy, Err(crate::LimboError::Busy)), "{busy:?}");
    update.reset().unwrap();
    a.execute("ROLLBACK").unwrap();
    run_with(&mut update, &[Value::from_i64(2)]);
    assert_eq!(
        rows(&a, "SELECT id, n FROM t ORDER BY id"),
        vec![
            vec![Value::from_i64(1), Value::from_i64(0)],
            vec![Value::from_i64(2), Value::from_i64(1)]
        ]
    );
}

/// A writing statement steps the checkpointer only while a pass is due or under way: fifty small
/// inserts in one transaction, far short of a pass's share of the log, each pass over it, and so
/// does the transaction's end.
#[test]
fn a_writing_statement_passes_over_a_checkpointer_with_nothing_due() {
    use crate::storage::atoll::checkpoint::STEPS_PASSED_OVER;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/nothing-due");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    let before = STEPS_PASSED_OVER.with(|n| n.get());
    for i in 0..50 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'r{i}')"))
            .unwrap();
    }
    let inserted = STEPS_PASSED_OVER.with(|n| n.get()) - before;
    conn.execute("COMMIT").unwrap();
    let ended = STEPS_PASSED_OVER.with(|n| n.get()) - before - inserted;
    drop((conn, db));
    assert_eq!(inserted, 50);
    // The transaction's end passes over it too (DESIGN.md 22.9, Due; mutant: an end that wakes
    // the checkpoint's task whatever is due).
    assert_eq!(ended, 1, "the transaction's end");
}

/// A prepared insert looks its table's rowid counter up once: the cursor it parks keeps it, and
/// the rowids it draws go on from the rows written.
#[test]
fn a_prepared_insert_looks_its_rowid_counter_up_once() {
    use crate::storage::atoll::sql::rowid::LOOKUPS;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/counter-kept");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    let mut insert = conn.prepare("INSERT INTO t (b) VALUES (?)").unwrap();
    run_with(&mut insert, &[text("first")]);
    let before = LOOKUPS.with(|n| n.get());
    for i in 0..20 {
        run_with(&mut insert, &[text(&format!("r{i}"))]);
    }
    assert_eq!(LOOKUPS.with(|n| n.get()) - before, 0);
    assert_eq!(
        rows(&conn, "SELECT count(*), max(a) FROM t"),
        vec![vec![Value::from_i64(21), Value::from_i64(21)]]
    );
}

#[test]
fn a_repolled_wait_owns_one_timer_and_ending_it_releases_the_completion() {
    use super::{Waiting, Waits};
    use crate::sync::atomic::Ordering;
    use crate::IOResult;
    let waits = Waits::new();
    waits.bound.store(60_000_000_000, Ordering::Release);
    let c = crate::storage::atoll::notify::remote_wait();
    let mut waiting = Waiting::default();
    let mut keys = Vec::new();
    let mut refs = Vec::new();
    let mut all_parked = true;
    for _ in 0..32 {
        all_parked &= matches!(
            waiting.wait::<()>(&waits, c.clone()).unwrap(),
            IOResult::IO(_)
        );
        keys.push(waiting.timed.as_ref().unwrap().registration());
        refs.push(c.handles());
    }
    waiting.over().unwrap();
    let after_end = c.handles();
    drop(waiting);
    drop(waits);
    assert!(all_parked);
    assert!(
        keys.iter().all(|key| key == &keys[0]),
        "a re-poll re-armed its timer: {keys:?}"
    );
    assert!(
        refs.iter().all(|&refs| refs == 2),
        "each poll must retain only the timer's one handle: {refs:?}"
    );
    assert_eq!(
        after_end, 1,
        "ending the wait releases its distant timer immediately"
    );
}

#[test]
fn a_changed_completion_replaces_its_timer_at_the_first_waits_deadline() {
    use super::{Waiting, Waits, UNBOUNDED};
    use crate::sync::atomic::Ordering;
    use std::time::Duration;
    let waits = Waits::new();
    waits.bound.store(60_000_000_000, Ordering::Release);
    let old = crate::storage::atoll::notify::remote_wait();
    let new = crate::storage::atoll::notify::remote_wait();
    let mut waiting = Waiting::default();
    drop(waiting.wait::<()>(&waits, old.clone()).unwrap());
    let start = waiting.since.unwrap();
    drop(waiting.wait::<()>(&waits, new.clone()).unwrap());
    let same_start = waiting.since == Some(start);
    let new_target = waiting
        .timed
        .as_ref()
        .unwrap()
        .is_for(start + Duration::from_secs(60), &new);
    let refs = (old.handles(), new.handles());
    waits.bound.store(UNBOUNDED, Ordering::Release);
    drop(waiting.wait::<()>(&waits, new.clone()).unwrap());
    let unbounded_cancelled = waiting.timed.is_none() && new.handles() == 1;
    let first_park_kept = waiting.since == Some(start);
    drop(waiting);
    drop(waits);
    assert!(
        same_start && new_target,
        "a replacement completion changed its original deadline"
    );
    assert_eq!(
        refs,
        (1, 2),
        "only the current completion is held by the timer"
    );
    assert!(
        unbounded_cancelled,
        "an unbounded wait must discard an old timeout"
    );
    assert!(
        first_park_kept,
        "changing the bound must preserve the first park, as before"
    );
}

#[test]
fn dropping_a_wait_cancels_its_timer_without_waiting_for_the_deadline() {
    use super::{Waiting, Waits};
    use crate::sync::atomic::Ordering;
    let waits = Waits::new();
    waits.bound.store(60_000_000_000, Ordering::Release);
    let c = crate::storage::atoll::notify::remote_wait();
    let mut waiting = Waiting::default();
    drop(waiting.wait::<()>(&waits, c.clone()).unwrap());
    drop(waiting);
    drop(waits);
    assert_eq!(
        c.handles(),
        1,
        "an abandoned wait left its timer registered"
    );
    assert!(!c.finished());
}

/// A transaction's ending is its own top (DESIGN.md 9.1.3, *The top and the owner*): a rollback
/// whose undo comes short of memory reserves its frame at the ending, holding nothing, and ends
/// its tenure once done.
#[test]
fn an_endings_rollback_that_comes_short_reserves_at_the_ending_and_ends_there() {
    // Checked with every Atoll value let go: a failed check unwinding through one ends the
    // process rather than failing the test.
    if let Err(e) = ending_comes_short() {
        panic!("{e}");
    }
}

fn ending_comes_short() -> std::result::Result<(), String> {
    use crate::storage::atoll::btree::Mode;
    use crate::storage::atoll::buffer::scarce;
    use crate::storage::atoll::page::SizeClass;
    use crate::storage::atoll::sql::AtollDb;
    use crate::storage::atoll::testing::drive;
    use crate::storage::atoll::txn::versioned::{VersionedTree, Write, WriteOp};
    use crate::storage::atoll::txn::{Commit, Isolation};
    use crate::types::{IOCompletions, IOResult, ImmutableRecord};
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Arc::new(AtollDb::open(io.clone(), "/db/ending-short", config()).unwrap());
    let txns = db.atoll().transactions();
    let begin = || {
        let mut t = drive(&io, || txns.begin(Isolation::Snapshot)).unwrap();
        crate::storage::atoll::testing::share(txns, &mut t);
        txns.statement(&mut t).unwrap();
        t
    };
    let a = db.atoll();
    let record = ImmutableRecord::from_values(&[Value::build_text("v".repeat(100))], 1).unwrap();
    let write = |tree: &VersionedTree, txn: &mut crate::storage::atoll::txn::Txn, rowid: i64| {
        let key = crate::storage::atoll::sql::key::rowid(rowid);
        let mut w = WriteOp::put(&key, record.get_payload().to_vec(), Mode::Upsert);
        match drive(&io, || tree.write(txns, db.versions(), txn, &mut w)) {
            Ok(Write::Done(_)) => Ok(()),
            out => Err(format!("{out:?}")),
        }
    };
    // A tree of more than one level, committed: its root a node no open transaction wrote.
    let mut txn = begin();
    let root = drive(&io, || db.create_tree(&mut txn, SizeClass::new(0).unwrap())).unwrap();
    let tree = VersionedTree::open(io.clone(), a.pool().clone(), a.store().clone(), root);
    for rowid in 0..300 {
        write(&tree, &mut txn, rowid)?;
    }
    let mut op = Commit::new(txn);
    drive(&io, || txns.commit(&mut op)).unwrap();
    // A row written, then the tree written back and its root evicted: the undo reaches the
    // row's leaf through the root, read in again.
    let mut txn = begin();
    write(&tree, &mut txn, 150)?;
    super::full_checkpoint(&db).unwrap();
    let pool = a.pool();
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(pool)).unwrap();
    if !pool.evict_clean(root) {
        return Err("the tree's root was not evicted".into());
    }
    let mut ending =
        super::Ending::rollback(db.clone(), crate::storage::atoll::buffer::Conn::new(), txn);
    let mut short = 0;
    loop {
        match scarce(|| ending.step()).map_err(|e| e.to_string())? {
            IOResult::Done(()) => break,
            IOResult::IO(IOCompletions(w)) => {
                if ending.shortage_queued() {
                    short += 1;
                }
                if !w.finished() {
                    crate::storage::atoll::park::wait(io.as_ref(), &w);
                }
            }
        }
    }
    if short == 0 {
        return Err("the undo never came short: the test reached no shortage".into());
    }
    if ending.shortage_queued() || pool.shortage_len() != 0 {
        return Err("the ending done kept its tenure".into());
    }
    Ok(())
}

/// A statement's reset ends the memory-shortage tenure of the cursors it keeps for its next
/// execution (DESIGN.md 9.1.3, *The top and the owner*): a write that came short and was reset
/// leaves no place in the queue, nor the gate, behind it.
#[test]
fn a_statements_reset_ends_its_cursors_shortage_tenure() {
    // Checked with every Atoll value let go: a failed check unwinding through one ends the
    // process rather than failing the test.
    if let Err(e) = reset_ends_tenure() {
        panic!("{e}");
    }
}

fn reset_ends_tenure() -> std::result::Result<(), String> {
    use crate::storage::atoll::buffer::scarce;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/reset-short");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    let pool = db
        .atoll
        .as_ref()
        .expect("Atoll mode")
        .atoll()
        .pool()
        .clone();
    let mut insert = conn.prepare("INSERT INTO t (b) VALUES (?1)").unwrap();
    for i in 0..1024 {
        insert
            .bind_at(
                std::num::NonZero::<usize>::MIN,
                Value::build_text(format!("{i:0600}")),
            )
            .unwrap();
        let mut came_short = false;
        loop {
            match scarce(|| insert.step()).map_err(|e| e.to_string())? {
                crate::StepResult::Done => break,
                crate::StepResult::IO | crate::StepResult::Yield => {
                    if pool.shortage_len() > 0 {
                        came_short = true;
                        break;
                    }
                    insert._io().step().unwrap();
                }
                other => return Err(format!("{other:?}")),
            }
        }
        insert.reset().unwrap();
        if came_short {
            return match pool.shortage_len() {
                0 => Ok(()),
                n => Err(format!("the reset left {n} tenures behind")),
            };
        }
    }
    Err("no insert came short: the test reached no shortage".into())
}

/// Steps `stmt` under scarce memory until it is done, or until it comes short with its place in
/// the queue (`stop_short`): whether it stopped short. A wait no event of the test finishes is a
/// statement waiting behind its own connection: refused.
fn step_scarce(
    stmt: &mut crate::Statement,
    pool: &Arc<crate::storage::atoll::buffer::BufferPool>,
    stop_short: bool,
) -> std::result::Result<bool, String> {
    use crate::storage::atoll::buffer::scarce;
    loop {
        match scarce(|| stmt.step()).map_err(|e| e.to_string())? {
            crate::StepResult::Done => return Ok(false),
            crate::StepResult::Row => {}
            crate::StepResult::IO | crate::StepResult::Yield => {
                if stop_short && pool.shortage_len() > 0 {
                    return Ok(true);
                }
                stmt._io().step().map_err(|e| e.to_string())?;
            }
            other => return Err(format!("{other:?}")),
        }
    }
}

/// A connection's statement that comes short while another of its statements is suspended in the
/// shortage queue takes that one's place rather than waiting behind it, and completes; the
/// suspended one, stepped again, finds its place gone and completes afresh (DESIGN.md 9.1.3,
/// *Revoking an inactive tenure*; Clay's ruling 2026-11-09: no new error). Two scans of a table
/// larger than the pool, whose early pages each reads in again.
#[test]
fn a_connections_statement_takes_the_place_of_its_suspended_sibling_and_both_complete() {
    // No checkpointing thread: a change the gate a cut closed refuses is the one to step the cut
    // (DESIGN.md 9.1.3), or the bulk write spins; each row written or read and each stage reports
    // progress, a retry none, so a regression fails, not hangs.
    beating_within_a_minute(|beat| {
        if let Err(e) = sibling_place_taken(beat) {
            panic!("{e}");
        }
    });
}

fn sibling_place_taken(beat: &dyn Fn()) -> std::result::Result<(), String> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/sibling-revoked");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    let pool = db
        .atoll
        .as_ref()
        .expect("Atoll mode")
        .atoll()
        .pool()
        .clone();
    // Rows twice the pool's budget: the table's first pages are written back and evicted.
    const ROW: usize = 3000;
    let rows = 2 * crate::storage::atoll::buffer::MIN_DB_BUDGET / ROW;
    let began = std::time::Instant::now();
    conn.execute("BEGIN").unwrap();
    let mut insert = conn.prepare("INSERT INTO t (b) VALUES (?1)").unwrap();
    for i in 0..rows {
        insert
            .bind_at(
                std::num::NonZero::<usize>::MIN,
                Value::build_text(format!("{i:0ROW$}")),
            )
            .unwrap();
        insert.run_ignore_rows().map_err(|e| e.to_string())?;
        insert.reset().unwrap();
        beat();
    }
    drop(insert);
    conn.execute("COMMIT").unwrap();
    beat();
    eprintln!("SIBLING {rows} rows written in {:?}", began.elapsed());
    // The first scan, suspended with its place in the queue.
    let mut first = conn.prepare("SELECT b FROM t").unwrap();
    if !step_scarce(&mut first, &pool, true)? {
        return Err("the first scan never came short".into());
    }
    beat();
    eprintln!("SIBLING first suspended at {:?}", began.elapsed());
    if pool.shortage_len() != 1 {
        return Err(format!("{} places queued", pool.shortage_len()));
    }
    // A second scan of the connection comes short in turn: it takes the first's place.
    let mut second = conn.prepare("SELECT count(*) FROM t").unwrap();
    let mut came_short = false;
    let mut counted = None;
    loop {
        match crate::storage::atoll::buffer::scarce(|| second.step()).map_err(|e| e.to_string())? {
            crate::StepResult::Done => break,
            crate::StepResult::Row => {
                counted = second.row().and_then(|r| r.get_values().next().cloned());
                beat();
            }
            crate::StepResult::IO | crate::StepResult::Yield => {
                came_short |= pool.shortage_len() > 0;
                // Taken to read: the statement steps on without it, a wait being only a retry's
                // cue.
                let pending = second
                    .take_io_completions()
                    .is_some_and(|crate::types::IOCompletions(c)| !c.finished());
                if pending {
                    return Err("the second scan waits behind its own first".into());
                }
                second._io().step().map_err(|e| e.to_string())?;
            }
            other => return Err(format!("{other:?}")),
        }
    }
    drop(second);
    beat();
    eprintln!("SIBLING second done at {:?}", began.elapsed());
    if !came_short {
        return Err("the second scan never came short".into());
    }
    // The first, stepped again, completes afresh.
    if step_scarce(&mut first, &pool, false)? {
        return Err("the first stopped short".into());
    }
    drop(first);
    if pool.shortage_len() != 0 {
        return Err(format!("{} places left queued", pool.shortage_len()));
    }
    match counted {
        Some(Value::Numeric(crate::numeric::Numeric::Integer(k))) if k as usize == rows => Ok(()),
        other => Err(format!("{other:?} rows counted, {rows} written")),
    }
}

/// A transaction's end cancels its connection's suspended tenures (DESIGN.md 9.1.3): no sibling
/// statement keeps the gate past the transaction it served.
#[test]
fn a_transactions_end_cancels_its_connections_suspended_tenures() {
    if let Err(e) = end_cancels() {
        panic!("{e}");
    }
}

fn end_cancels() -> std::result::Result<(), String> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, "/db/end-cancels");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    let pool = db
        .atoll
        .as_ref()
        .expect("Atoll mode")
        .atoll()
        .pool()
        .clone();
    let mut insert = conn.prepare("INSERT INTO t (b) VALUES (?1)").unwrap();
    let mut n = 0;
    loop {
        if n == 1024 {
            return Err("the statement never came short".into());
        }
        insert
            .bind_at(
                std::num::NonZero::<usize>::MIN,
                Value::build_text(format!("{n:0600}")),
            )
            .unwrap();
        n += 1;
        if step_scarce(&mut insert, &pool, true)? {
            break;
        }
        insert.reset().unwrap();
    }
    if pool.shortage_len() != 1 {
        return Err(format!("{} places queued", pool.shortage_len()));
    }
    // The transaction ends under the suspended statement.
    let ct = conn.atoll().expect("Atoll mode");
    let ending = ct.end(false);
    let queued = pool.shortage_len();
    if let Some(mut ending) = ending {
        ending.finish().map_err(|e| e.to_string())?;
    }
    drop(insert);
    match queued {
        0 => Ok(()),
        n => Err(format!("the end left {n} places of its connection queued")),
    }
}

/// Every `drive` a connection's facade method runs carries the connection's identity
/// (`drive_for`), so a shortage it meets takes the place of the connection's own suspended
/// operation rather than waiting behind it (DESIGN.md 9.1.3, *Revoking an inactive tenure*). The
/// census reads `sql/mode.rs` and lists the bare `drive`s left, each with why it has no
/// connection's statement to be suspended behind.
#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn every_drive_of_the_connections_facade_carries_its_identity() {
    use crate::storage::atoll::testing::{atoll_dir, code_of};
    const BARE: &[(&str, &str)] = &[
        (
            "full_checkpoint",
            "a full checkpoint, maintenance: no tenure, no connection",
        ),
        (
            "drive_to_end",
            "an ending's own owner carries the connection; the outer drive only parks",
        ),
    ];
    let code = code_of(&std::fs::read_to_string(atoll_dir().join("sql/mode.rs")).unwrap());
    let code = code.split("mod tests {").next().unwrap();
    let mut found = Vec::new();
    for (at, _) in code.match_indices("drive(") {
        let before = &code[..at];
        // `drive_for(` and `drive_as(` are other names; `drive(` is the bare call alone.
        if before.ends_with('_') || before.ends_with("fn ") || before.ends_with("::") {
            continue;
        }
        let caller: String = before
            .rfind("fn ")
            .map(|i| {
                before[i + 3..]
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect()
            })
            .unwrap_or_default();
        found.push(caller);
    }
    found.sort();
    let mut listed: Vec<String> = BARE.iter().map(|(f, _)| f.to_string()).collect();
    listed.sort();
    assert!(!found.is_empty(), "the census found no drive at all");
    assert_eq!(
        found, listed,
        "a bare drive in the connection's facade not listed with why it carries no connection, \
         or one listed that is gone"
    );
}

#[cfg(not(any(shuttle, atoll_loom, miri)))]
#[test]
fn a_statement_with_one_pending_wait_keeps_only_its_latest_waker() {
    use std::sync::mpsc;
    use std::task::{Wake, Waker};
    struct Calls(std::sync::atomic::AtomicUsize);
    impl Wake for Calls {
        fn wake(self: std::sync::Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_with(
        &io,
        "/db/raw-waker-bounds",
        crate::storage::atoll::db::AtollConfig {
            log_budget: Some(1),
            ..config()
        },
    );
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY)")
        .unwrap();
    let mut statement = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
    let tasks: Vec<_> = (0..32)
        .map(|_| std::sync::Arc::new(Calls(std::sync::atomic::AtomicUsize::new(0))))
        .collect();
    let wakers: Vec<_> = tasks.iter().map(|t| Waker::from(t.clone())).collect();
    let atoll = db.atoll.as_ref().unwrap().atoll();
    let (
        pending,
        registered,
        shares_the_tasks_wait,
        idle_wakes,
        steady_allocations,
        steady_pending,
        finished_wakes,
        cut_wait_pending,
        checkpoint_done,
        statement_done,
    ) = std::thread::scope(|scope| {
        let (held_tx, held_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let store = atoll.store().clone();
        let holder = scope.spawn(move || {
            let change = store.change().expect("gate initially open");
            held_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            drop(change);
        });
        held_rx.recv().unwrap();
        let cp = atoll.checkpointer();
        let mut ticket = cp.request_full();
        // The writers claim the checkpoint's task: the step this claim runs waits for the held
        // change to leave the closing gate, its own waker on that wait.
        let cut_wait = match cp.step_full(&mut ticket, atoll.transactions()).unwrap() {
            crate::types::IOResult::IO(crate::types::IOCompletions(c)) if !c.finished() => Some(c),
            _ => None,
        };
        let cut_wait_pending = cut_wait.is_some();
        let mut pending = 0;
        let mut wait = None;
        for waker in &wakers {
            let out = statement.step_with_waker(waker).unwrap();
            if pending == 0 {
                eprintln!("STATEMENT_FIRST_STEP {out:?}");
            }
            if matches!(out, crate::StepResult::IO) {
                wait = statement.pending_io_for_test();
                pending += usize::from(wait.as_ref().is_some_and(|c| !c.finished()));
            } else {
                break;
            }
        }
        let registered = wait.as_ref().map_or(0, crate::Completion::wakers);
        // The statement's claims step the same task, which waits on the same cut wait.
        let shares_the_tasks_wait = matches!((&wait, &cut_wait), (Some(w), Some(c)) if w.ptr_eq(c));
        let idle_wakes = tasks
            .iter()
            .map(|t| t.0.load(std::sync::atomic::Ordering::Relaxed))
            .sum::<usize>();
        let before = crate::storage::atoll::testing::allocations();
        let mut steady_pending = 0;
        for _ in 0..32 {
            steady_pending += usize::from(matches!(
                statement.step_with_waker(&wakers[31]).unwrap(),
                crate::StepResult::IO
            ));
        }
        let steady_allocations = crate::storage::atoll::testing::allocations() - before;
        resume_tx.send(()).unwrap();
        holder.join().unwrap();
        let checkpoint_done = crate::storage::atoll::testing::drive(&io, || {
            cp.step_full(&mut ticket, atoll.transactions())
        })
        .is_ok();
        let statement_done = statement.run_ignore_rows().is_ok();
        let finished_wakes = tasks
            .iter()
            .map(|t| t.0.load(std::sync::atomic::Ordering::Relaxed))
            .collect::<Vec<_>>();
        (
            pending,
            registered,
            shares_the_tasks_wait,
            idle_wakes,
            steady_allocations,
            steady_pending,
            finished_wakes,
            cut_wait_pending,
            checkpoint_done,
            statement_done,
        )
    });
    drop(statement);
    drop(conn);
    drop(db);
    drop(io);
    drop(wakers);
    drop(tasks);
    eprintln!("STATEMENT_WAKERS pending={pending} registered={registered} shares_the_tasks_wait={shares_the_tasks_wait} idle_wakes={idle_wakes} steady_allocations={steady_allocations} steady_pending={steady_pending} finish_wakes={finished_wakes:?} cut_wait_pending={cut_wait_pending} checkpoint_done={checkpoint_done} statement_done={statement_done}");
    assert!(cut_wait_pending && pending == 32 && checkpoint_done && statement_done);
    assert!(
        shares_the_tasks_wait,
        "the statement does not wait on the checkpoint task's own wait"
    );
    // The statement's latest waker, and the checkpoint's task.
    assert_eq!(
        registered, 2,
        "one live SQL statement retained obsolete Context wakers"
    );
    assert_eq!(idle_wakes, 0);
    assert_eq!(
        (steady_pending, steady_allocations),
        (32, 0),
        "stable pending statement polls must not allocate"
    );
    assert!(
        finished_wakes[..31].iter().all(|n| *n == 0) && finished_wakes[31] == 1,
        "only the latest statement waker must be notified"
    );
}

/// Proof 3 (DESIGN.md 22.15): a step of the checkpoint's task is one unit, and a unit visits at
/// most one page, over a checkpoint that reads values out of line through their directories,
/// follows a running transaction's versions into the arenas, writes the metadata it edits and
/// does the frees the last checkpoint deferred. Run in a child: a unit visiting two pages ends the
/// process in a debug build (mutant: an image unit going on to the next page).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_checkpoint_unit_visits_at_most_one_page() {
    use crate::storage::atoll::testing::{drive, in_child};
    use crate::storage::atoll::unit;
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_checkpoint_unit_visits_at_most_one_page"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = open_with(
                &io,
                "/db/proof-3",
                crate::storage::atoll::db::AtollConfig {
                    log_budget: Some(64 << 20),
                    ..config()
                },
            );
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b BLOB)")
                .unwrap();
            // Many leaves, a value out of line through its directory on every tenth row.
            for i in 0..2_000 {
                let len = if i % 10 == 0 { 40_000 } else { 100 };
                conn.execute(format!("INSERT INTO t VALUES ({i}, zeroblob({len}))"))
                    .unwrap();
            }
            let atoll = db.atoll.as_ref().unwrap().atoll();
            let cp = atoll.checkpointer();
            let mut ticket = cp.request_full();
            drive(&io, || cp.step_full(&mut ticket, atoll.transactions())).unwrap();
            // Frees the next checkpoint does, and a running transaction's versions, which the cut
            // reads below the leaves' in the arenas.
            conn.execute("DELETE FROM t WHERE a % 2 = 1 AND a % 7 = 0")
                .unwrap();
            let writer = db.connect(ConnectOpts::new()).unwrap();
            writer.execute("BEGIN").unwrap();
            writer
                .execute("UPDATE t SET b = zeroblob(10) WHERE a % 2 = 0")
                .unwrap();
            let in_charge = cp.superblock().checkpoint;
            cp.request_full();
            unit::most_and_units();
            let imaged = cp.stats().pages;
            let mut steps = 0u64;
            while cp.superblock().checkpoint == in_charge && steps < 1 << 20 {
                if !cp.claim_one() {
                    break;
                }
                steps += 1;
            }
            let (most, units) = unit::most_and_units();
            let pages = cp.stats().pages - imaged;
            let ended = cp.superblock().checkpoint != in_charge;
            writer.execute("ROLLBACK").unwrap();
            drop(writer);
            drop(conn);
            drop(db);
            eprintln!("PROOF3 most={most} units={units} steps={steps} pages={pages}");
            assert!(ended, "the checkpoint did not end in {steps} steps");
            assert_eq!(most, 1, "a unit visited {most} pages");
            assert_eq!(units, steps, "{units} units in {steps} steps");
            assert!(pages >= 20, "{pages} pages imaged");
            assert!(units > 2 * pages, "{units} units imaged {pages} pages");
        },
    );
    let Some(ended) = ended else { return };
    assert!(
        !ended.aborted && !ended.hung && ended.code == Some(0),
        "{:?}\n{}",
        ended.code,
        ended.stderr
    );
    assert!(ended.stderr.contains("PROOF3 most=1 "), "{}", ended.stderr);
}

/// How a test drops a database while it reopens it, and where the close pauses.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum Closing {
    /// The test drops its last handle, and the drop pauses as it begins: nothing of the database
    /// dropped yet, its handle no longer upgrading.
    DropBegins,
    /// The test drops its last handle, and the drop pauses as the Atoll database begins to close.
    AtollCloses,
    /// The test drops its handle while a connection holds the database, then the connection, and
    /// the drop pauses as the Atoll database begins to close: the connection's transaction holds it
    /// past the `Database`, whose fields have all dropped by then.
    LastConnection,
}

/// How a test reopens the file.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum Reopen {
    /// [`Database::open`], which resolves Atoll mode first.
    Open,
    /// [`Database::open_async`] with Atoll's options set, its first phase reading the registry.
    OpenAsync,
}

#[cfg(target_os = "linux")]
impl Reopen {
    fn open(self, io: &Arc<dyn IO>, path: &str) -> Arc<Database> {
        match self {
            Reopen::Open => open(io, path),
            Reopen::OpenAsync => {
                let options = OpenOptions::new(Arc::new(SqliteDialect))
                    .atoll(config())
                    .storage(Arc::new(crate::storage::atoll::sql::mode::RefusingStorage));
                let mut state = crate::database::OpenDbAsyncState::new();
                loop {
                    match Database::open_async(&mut state, io.clone(), path, &options).unwrap() {
                        crate::IOResult::Done(db) => break db,
                        crate::IOResult::IO(c) => c.wait(&**io).unwrap(),
                    }
                }
            }
        }
    }
}

/// Whether another holder could not take a write lock on the whole of `file`: an open file
/// description's lock query, which meets a traditional lock even this process holds.
#[cfg(target_os = "linux")]
fn locked(file: &std::fs::File) -> bool {
    use std::os::fd::AsRawFd;
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as libc::c_short;
    lock.l_whence = libc::SEEK_SET as libc::c_short;
    // SAFETY: a query on a descriptor the caller holds open, into a struct this call owns.
    let r = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &mut lock) };
    assert_eq!(r, 0, "the lock query failed");
    lock.l_type != libc::F_UNLCK as libc::c_short
}

/// A database closing keeps its files, and the locks they hold, until they close, and an open of
/// the file meanwhile waits for them: no probe and no second open closes a descriptor of the file
/// while they are open, which would drop every lock the process holds on it (R17), the reopened
/// database's own once the old files close (the auditor's #4385, #4389). The lock is looked at
/// through one descriptor kept open to the end, at the open, while the reopen waits and after both
/// have ended: an open file description's lock query meets a traditional lock even this process
/// holds, and it is asserted to meet the first database's.
#[cfg(target_os = "linux")]
fn reopen_while_closing(closing: Closing, reopen: Reopen) {
    use crate::database::{DROP_PAUSE, REGISTRY_WAITS};
    use crate::storage::atoll::testing::CLOSE_PAUSE;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db").to_str().unwrap().to_string();
    let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let first = open(&io, &path);
    let look = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let held_at_open = locked(&look);
    // What the closer drops: the database, or a connection holding it once the test's handle went.
    let last: Box<dyn std::any::Any + Send> = match closing {
        Closing::DropBegins | Closing::AtollCloses => Box::new(first),
        Closing::LastConnection => {
            let conn = first.connect(ConnectOpts::new()).unwrap();
            drop(first);
            Box::new(conn)
        }
    };
    let (told, heard) = std::sync::mpsc::channel::<&'static str>();
    let (go, wait_go) = std::sync::mpsc::channel::<()>();
    let closer = {
        let told = told.clone();
        std::thread::spawn(move || {
            let pause: Box<dyn FnOnce()> = Box::new(move || {
                told.send("paused").unwrap();
                wait_go.recv().unwrap();
            });
            match closing {
                Closing::DropBegins => DROP_PAUSE.with(|p| *p.borrow_mut() = Some(pause)),
                Closing::AtollCloses | Closing::LastConnection => {
                    CLOSE_PAUSE.with(|p| *p.borrow_mut() = Some(pause))
                }
            }
            drop(last);
        })
    };
    let paused = heard.recv().unwrap();
    let opener = {
        let (io, path) = (io.clone(), path.clone());
        std::thread::spawn(move || {
            REGISTRY_WAITS.with(|w| *w.borrow_mut() = Some(told.clone()));
            let db = reopen.open(&io, &path);
            told.send("opened").unwrap();
            db
        })
    };
    let met = heard.recv().unwrap();
    // The close still paused: an open that probed the file first has closed a descriptor of it.
    let held_while_closing = locked(&look);
    go.send(()).unwrap();
    closer.join().unwrap();
    let second = opener.join().unwrap();
    let held_after = locked(&look);
    drop(second);
    drop(look);
    assert!(held_at_open, "an open database holds no lock on its file");
    assert_eq!(paused, "paused", "the close did not pause");
    assert_eq!(
        met, "waiting",
        "an open of the file went ahead while its database was closing"
    );
    assert!(
        held_while_closing,
        "the closing database's lock on its file was dropped while it closed"
    );
    assert!(
        held_after,
        "the reopened database's lock on its file was dropped as the old one closed"
    );
}

/// What a reopen test that does not end within a minute says: its close never reached its pause,
/// or the close or the reopen never ended.
#[cfg(target_os = "linux")]
const NEVER_REOPENED: &str = "a close never paused, or a close or a reopen never ended";

/// Mutant: an open's resolution taking a database whose handle no longer upgrades for none, and
/// probing the file (its first phase then waits, but the probe has closed a descriptor).
#[cfg(target_os = "linux")]
#[test]
fn a_reopen_as_the_databases_drop_begins_waits_and_keeps_the_files_lock() {
    within_a_minute_or(NEVER_REOPENED, || {
        reopen_while_closing(Closing::DropBegins, Reopen::Open)
    });
}

/// Mutant: the same, or the registry forgetting the database before its files close.
#[cfg(target_os = "linux")]
#[test]
fn a_reopen_while_the_database_closes_waits_and_keeps_the_files_lock() {
    within_a_minute_or(NEVER_REOPENED, || {
        reopen_while_closing(Closing::AtollCloses, Reopen::Open)
    });
}

/// Mutant: the registry forgetting the database once the `Database` has dropped, its Atoll files
/// still open in the connection's transaction (#4389).
#[cfg(target_os = "linux")]
#[test]
fn a_reopen_while_the_last_connection_closes_the_database_waits_and_keeps_the_files_lock() {
    within_a_minute_or(NEVER_REOPENED, || {
        reopen_while_closing(Closing::LastConnection, Reopen::Open)
    });
}

/// Mutant: the open's first phase taking a database whose handle no longer upgrades for none.
#[cfg(target_os = "linux")]
#[test]
fn an_async_reopen_while_the_database_closes_waits_and_keeps_the_files_lock() {
    within_a_minute_or(NEVER_REOPENED, || {
        reopen_while_closing(Closing::AtollCloses, Reopen::OpenAsync)
    });
}

/// How the open a test pauses ends while another open of the file waits for it.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum FirstOpen {
    /// It registers its database.
    Registers,
    /// It fails once its Atoll database is open, which then closes, and the close pauses.
    Fails,
}

/// An open of a file another open of it has under way waits for that open's end, once, and looks
/// again: it takes the database the first registered, or, the first having failed, opens the file
/// once everything the first opened has closed, the first's lock held until then and its own after
/// (the auditor's #4392: a yield and a retry until the first ended, before).
#[cfg(target_os = "linux")]
fn open_while_another_opens(first: FirstOpen, reopen: Reopen) {
    use crate::database::{OPEN_PAUSE, REGISTRY_WAITS};
    use crate::storage::atoll::testing::CLOSE_PAUSE;
    use crate::{DatabaseKey, RegistryEntry, DATABASE_MANAGER};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db").to_str().unwrap().to_string();
    let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    drop(open(&io, &path));
    let key = DatabaseKey::File(io.file_id(&path).unwrap());
    let look = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let (told, heard) = std::sync::mpsc::channel::<&'static str>();
    let (go, wait_go) = std::sync::mpsc::channel::<()>();
    let (go_close, wait_go_close) = std::sync::mpsc::channel::<()>();
    let opener = {
        let (io, path) = (io.clone(), path.clone());
        std::thread::spawn(move || {
            let paused = told.clone();
            OPEN_PAUSE.with(|p| {
                *p.borrow_mut() = Some(Box::new(move || {
                    paused.send("paused").unwrap();
                    wait_go.recv().unwrap();
                    match first {
                        FirstOpen::Registers => Ok(()),
                        FirstOpen::Fails => Err(crate::LimboError::InternalError(
                            "the test's open fails".to_string(),
                        )),
                    }
                }))
            });
            if let FirstOpen::Fails = first {
                CLOSE_PAUSE.with(|p| {
                    *p.borrow_mut() = Some(Box::new(move || {
                        told.send("closing").unwrap();
                        wait_go_close.recv().unwrap();
                    }))
                });
            }
            Database::open(
                io.clone(),
                &path,
                OpenOptions::new(Arc::new(SqliteDialect)).atoll(config()),
            )
        })
    };
    assert_eq!(
        heard.recv().unwrap(),
        "paused",
        "the first open did not pause"
    );
    let mark = match DATABASE_MANAGER.lock().get(&key) {
        Some(RegistryEntry::Opening(c)) => c.clone(),
        _ => panic!("the paused open has no mark on its file's entry"),
    };
    let (seen, waits) = std::sync::mpsc::channel::<&'static str>();
    let second = {
        let (io, path) = (io.clone(), path.clone());
        std::thread::spawn(move || {
            REGISTRY_WAITS.with(|w| *w.borrow_mut() = Some(seen.clone()));
            let db = reopen.open(&io, &path);
            seen.send("opened").unwrap();
            db
        })
    };
    assert_eq!(
        waits.recv().unwrap(),
        "opening",
        "the second open did not wait"
    );
    go.send(()).unwrap();
    // The first failed: its database closes, paused, its mark still standing and its lock held.
    let closing = match first {
        FirstOpen::Registers => None,
        FirstOpen::Fails => {
            let closed = heard.recv().unwrap();
            let state = (closed, !mark.finished(), locked(&look));
            go_close.send(()).unwrap();
            Some(state)
        }
    };
    let first_opened = opener.join().unwrap();
    let second = second.join().unwrap();
    let held_after = locked(&look);
    let after: Vec<&'static str> = waits.try_iter().collect();
    drop(look);
    // Every database dropped before any assertion, so none drops while a failed one unwinds.
    let (first_opened, same) = match first_opened {
        Ok(first) => (Ok(()), Some(Arc::ptr_eq(&first, &second))),
        Err(e) => (Err(e), None),
    };
    drop(second);
    assert_eq!(
        after,
        ["opened"],
        "the second open looked at the registry again before the first ended"
    );
    match (first, first_opened, same, closing) {
        (FirstOpen::Registers, Ok(()), Some(same), None) => assert!(
            same,
            "the second open did not take the database the first registered"
        ),
        (FirstOpen::Fails, Err(_), None, Some((closed, marked, held))) => {
            assert_eq!(
                closed, "closing",
                "the failed open's database did not close"
            );
            assert!(
                marked,
                "the failed open's mark went before its database closed"
            );
            assert!(
                held,
                "the failed open's lock went before its database closed"
            );
            assert!(
                held_after,
                "the second open's lock went as the failed one closed"
            );
        }
        (_, opened, _, _) => panic!("the first open ended unlike the test made it: {opened:?}"),
    }
}

/// Mutant: an open's resolution yielding and looking again while another open is under way.
#[cfg(target_os = "linux")]
#[test]
fn an_open_while_another_opens_the_file_waits_once_and_takes_its_database() {
    within_a_minute_or(NEVER_REOPENED, || {
        open_while_another_opens(FirstOpen::Registers, Reopen::Open)
    });
}

/// Mutant: the async open's first phase handing back a yield, not the first open's end.
#[cfg(target_os = "linux")]
#[test]
fn an_async_open_while_another_opens_the_file_waits_once_and_takes_its_database() {
    within_a_minute_or(NEVER_REOPENED, || {
        open_while_another_opens(FirstOpen::Registers, Reopen::OpenAsync)
    });
}

/// Mutants: a failed open taking its mark out as it fails, before what it opened closes; and a
/// mark whose drop wakes no one (the test's bound ends it).
#[cfg(target_os = "linux")]
#[test]
fn an_open_while_another_fails_waits_for_what_the_failed_one_opened_to_close() {
    within_a_minute_or(NEVER_REOPENED, || {
        open_while_another_opens(FirstOpen::Fails, Reopen::Open)
    });
}

/// Steps `sql` on `conn` until its transaction's ending stands at `phase` (`"ending"`: its storage
/// ending under way; `"attached"`: ended, temp's and the attached databases' transactions still to
/// end) and waits on IO there, then drops the statement; whether it got there, and what the steps
/// met otherwise.
fn drop_at_its_ending(
    conn: &Arc<Connection>,
    crash: &crate::io::crash::CrashIO,
    sql: &str,
    phase: &str,
) -> (bool, Option<String>) {
    drop_at_its_ending_closing(conn, crash, sql, phase, false)
}

/// [`drop_at_its_ending`], if `close` with a close of the connection tried first, the statement
/// still holding its ending, which must be refused, and tried again once the statement has dropped,
/// which must close it.
fn drop_at_its_ending_closing(
    conn: &Arc<Connection>,
    crash: &crate::io::crash::CrashIO,
    sql: &str,
    phase: &str,
    close: bool,
) -> (bool, Option<String>) {
    let mut statement = conn.prepare(sql).unwrap();
    let met = loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => {
                if statement.atoll_ending_phase() == Some(phase) {
                    break (true, None);
                }
                crash.step().unwrap();
            }
            Ok(crate::StepResult::Done) => break (false, None),
            Ok(other) => break (false, Some(format!("{other:?}"))),
            Err(e) => break (false, Some(e.to_string())),
        }
    };
    let refused = match close && met.0 {
        true => match conn.close() {
            Err(crate::LimboError::StatementsInProgress(_)) => None,
            other => Some(format!("a close at the ending: {other:?}")),
        },
        false => None,
    };
    drop(statement);
    let closed = match close && met.0 {
        true => conn
            .close()
            .err()
            .map(|e| format!("the close after the drop: {e}")),
        false => None,
    };
    match (refused, closed) {
        (None, None) => met,
        (Some(e), _) | (None, Some(e)) => (false, Some(e)),
    }
}

/// Whether `conn` can read the table `name`, as a prepared statement sees it.
fn sees(conn: &Arc<Connection>, name: &str) -> std::result::Result<(), String> {
    conn.prepare(format!("SELECT * FROM {name}"))
        .map(drop)
        .map_err(|e| e.to_string())
}

/// A statement dropped once its commit's Atoll ending has begun finishes the commit whole, as its
/// next step would have: the table it created is in the shared schema, every connection sees it
/// with no reparse, its connection is back in autocommit, and a reopen finds it (the auditor's
/// P1-DDL-END1: before, the ending's drop committed the table and nothing published the schema).
#[test]
fn a_create_table_dropped_once_its_commit_began_publishes_the_table() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(651));
    let io: Arc<dyn IO> = crash.clone();
    let path = "/db/ddl-ending";
    let db = open(&io, path);
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let met = drop_at_its_ending(&writer, &crash, "CREATE TABLE made(a INTEGER)", "ending");
    let seen = (sees(&reader, "made"), sees(&writer, "made"));
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, reader, db));
    let reopened = open(&io, path);
    let fresh = reopened.connect(ConnectOpts::new()).unwrap();
    let after_reopen = sees(&fresh, "made");
    drop((fresh, reopened));
    assert_eq!(
        met,
        (true, None),
        "the statement never reached its commit's ending"
    );
    assert_eq!(
        seen,
        (Ok(()), Ok(())),
        "a commit finished by a drop left its table unpublished"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
    assert_eq!(after_reopen, Ok(()));
}

/// Runs `sql` on `conn` to its end on `crash`, stepping the IO it waits on.
fn run_on(conn: &Arc<Connection>, crash: &crate::io::crash::CrashIO, sql: &str) {
    let mut statement = conn.prepare(sql).unwrap();
    loop {
        match statement.step().unwrap() {
            crate::StepResult::IO => crash.step().unwrap(),
            crate::StepResult::Done => break,
            other => panic!("{sql} stepped to {other:?}"),
        }
    }
}

/// The same for an explicit transaction's `COMMIT`, a table made in the main database and one in
/// `beside` (`TEMP`, or a database file attached as `aux`), dropped at `phase`: both are in their
/// schemas once the dropped `COMMIT` has finished, the row written beside is there, and the
/// connection is back in autocommit.
fn commit_dropped_at(phase: &str, seed: u64, beside: &str) {
    commit_dropped_at_closing(phase, seed, beside, false)
}

/// [`commit_dropped_at`], if `close` with a close of the connection tried while the `COMMIT` holds
/// its ending, refused with the connection left as it was, the statement then finishing the commit
/// whole as it drops, and the connection closed after (the auditor's #4414, #4417). What it kept is
/// read through another connection.
fn commit_dropped_at_closing(phase: &str, seed: u64, beside: &str, close: bool) {
    let name = format!(
        "commit-ending-{phase}{}",
        if close { "-closed" } else { "" }
    );
    let crash = Arc::new(crate::io::crash::CrashIO::new(seed));
    let io: Arc<dyn IO> = crash.clone();
    let db = Database::open(
        io.clone(),
        &format!("/db/{name}"),
        OpenOptions::new(Arc::new(SqliteDialect))
            .atoll(config())
            .db_opts(DatabaseOpts::default().with_attach(true)),
    )
    .unwrap();
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let side = if beside == "TEMP" {
        "temp.side".to_string()
    } else {
        run_on(&writer, &crash, &format!("ATTACH '/db/{name}-aux' AS aux"));
        "aux.side".to_string()
    };
    for sql in [
        "BEGIN".to_string(),
        "CREATE TABLE kept(a INTEGER)".to_string(),
        format!("CREATE TABLE {side}(b INTEGER)"),
        format!("INSERT INTO {side} VALUES (1)"),
    ] {
        run_on(&writer, &crash, &sql);
    }
    let met = drop_at_its_ending_closing(&writer, &crash, "COMMIT", phase, close);
    let looker = if close {
        let looker = db.connect(ConnectOpts::new()).unwrap();
        if beside != "TEMP" {
            run_on(&looker, &crash, &format!("ATTACH '/db/{name}-aux' AS aux"));
        }
        looker
    } else {
        writer.clone()
    };
    let seen = (sees(&reader, "kept"), sees(&looker, "kept"));
    let autocommit = close || writer.get_auto_commit();
    let kept_beside = looker
        .prepare(format!("SELECT b FROM {side}"))
        .and_then(|mut statement| statement.run_collect_rows())
        .map_err(|e| e.to_string());
    drop(looker);
    drop((writer, reader, db));
    assert_eq!(met, (true, None), "the COMMIT never reached {phase}");
    assert_eq!(
        seen,
        (Ok(()), Ok(())),
        "a COMMIT finished by a drop left its main table unpublished"
    );
    assert!(
        autocommit,
        "a COMMIT finished by a drop left its connection in a transaction"
    );
    assert_eq!(kept_beside, Ok(vec![vec![Value::from_i64(1)]]));
}

#[test]
fn a_commit_dropped_once_its_ending_began_finishes_the_main_and_temporary_schemas() {
    commit_dropped_at("ending", 652, "TEMP");
}

#[test]
fn a_commit_dropped_once_its_storage_ended_finishes_the_attached_databases_commit() {
    commit_dropped_at("attached", 654, "aux");
}

#[test]
fn a_close_once_a_commits_storage_ended_is_refused_and_the_attached_commit_finishes() {
    commit_dropped_at_closing("attached", 658, "aux", true);
}

/// A writer's `CREATE TABLE` dropped once its commit's ending began, while a sibling statement of
/// its connection is part way through a scan: the commit is finished whole with the sibling still
/// counted, so the scan goes on in the read transaction the halt begins for it and reads every
/// row, and the table is in the shared schema (the auditor's #4413).
#[test]
fn a_create_table_dropped_at_its_ending_beside_a_scan_finishes_and_the_scan_reads_on() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(655));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/ddl-ending-beside-a-scan");
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    run_on(&writer, &crash, "CREATE TABLE t(a INTEGER)");
    for a in 1..=3 {
        run_on(&writer, &crash, &format!("INSERT INTO t VALUES ({a})"));
    }
    let mut scan = writer.prepare("SELECT a FROM t").unwrap();
    let mut scanned = Vec::new();
    // The scan's first row, then the writer's statement beside it.
    loop {
        match scan.step().unwrap() {
            crate::StepResult::IO => crash.step().unwrap(),
            crate::StepResult::Row => {
                scanned.push(
                    scan.row()
                        .unwrap()
                        .get_values()
                        .cloned()
                        .collect::<Vec<_>>(),
                );
                break;
            }
            other => panic!("the scan stepped to {other:?}"),
        }
    }
    let met = drop_at_its_ending(&writer, &crash, "CREATE TABLE made(a INTEGER)", "ending");
    let rest = loop {
        match scan.step() {
            Ok(crate::StepResult::IO) => crash.step().unwrap(),
            Ok(crate::StepResult::Row) => scanned.push(
                scan.row()
                    .unwrap()
                    .get_values()
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            Ok(crate::StepResult::Done) => break Ok(()),
            Ok(other) => break Err(format!("{other:?}")),
            Err(e) => break Err(e.to_string()),
        }
    };
    drop(scan);
    let seen = (sees(&reader, "made"), sees(&writer, "made"));
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, reader, db));
    assert_eq!(
        met,
        (true, None),
        "the statement never reached its commit's ending"
    );
    assert_eq!(
        rest,
        Ok(()),
        "the scan beside the dropped commit did not go on"
    );
    assert_eq!(
        scanned,
        (1..=3)
            .map(|a| vec![Value::from_i64(a)])
            .collect::<Vec<_>>(),
        "the scan beside the dropped commit lost rows"
    );
    assert_eq!(
        seen,
        (Ok(()), Ok(())),
        "a commit finished by a drop left its table unpublished"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
}

/// A close of a connection whose `CREATE TABLE` is at its commit's ending is refused, the connection
/// left as it was; the statement then finishes the commit whole as it drops, the table is in the
/// shared schema for the other connection and after a reopen, and the connection closes (the
/// auditor's #4414, #4417: before, the close rolled the connection's schema back under the ending
/// and returned, and nothing published the table).
#[test]
fn a_close_at_a_create_tables_ending_is_refused_and_the_commit_finishes_as_it_drops() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(656));
    let io: Arc<dyn IO> = crash.clone();
    let path = "/db/ddl-ending-close";
    let db = open(&io, path);
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let mut statement = writer.prepare("CREATE TABLE made(a INTEGER)").unwrap();
    let met = loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => {
                if statement.atoll_ending_phase() == Some("ending") {
                    break Ok(());
                }
                crash.step().unwrap();
            }
            Ok(other) => break Err(format!("{other:?}")),
            Err(e) => break Err(e.to_string()),
        }
    };
    let refused = match writer.close() {
        Err(crate::LimboError::StatementsInProgress(_)) => Ok(()),
        other => Err(format!("{other:?}")),
    };
    let left = (writer.is_closed(), writer.get_tx_state());
    drop(statement);
    let closed = writer.close().map_err(|e| e.to_string());
    let seen = sees(&reader, "made");
    drop((writer, reader, db));
    let reopened = open(&io, path);
    let fresh = reopened.connect(ConnectOpts::new()).unwrap();
    let after_reopen = sees(&fresh, "made");
    drop((fresh, reopened));
    assert_eq!(
        met,
        Ok(()),
        "the statement never reached its commit's ending"
    );
    assert_eq!(
        refused,
        Ok(()),
        "a close at a commit's ending was not refused"
    );
    assert_eq!(
        left,
        (
            false,
            crate::connection::TransactionState::Write {
                schema_did_change: true
            }
        ),
        "a refused close changed its connection"
    );
    assert_eq!(
        closed,
        Ok(()),
        "the connection did not close once its commit had finished"
    );
    assert_eq!(
        seen,
        Ok(()),
        "the commit a refused close left to its statement was not published"
    );
    assert_eq!(after_reopen, Ok(()));
}

/// A `CREATE TABLE` whose commit has been published, whose wait for durability then fails: the
/// commit stays, so its table is published to the shared schema all the same as the statement
/// fails (the auditor's #4412: the schema published only once the whole ending had succeeded).
#[test]
fn a_create_table_whose_published_commit_then_fails_publishes_its_table() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(657));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/ddl-published-then-failed");
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let mut statement = writer.prepare("CREATE TABLE made(a INTEGER)").unwrap();
    let published = loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => {
                if statement.atoll_ending_published() {
                    break Ok(());
                }
                crash.step().unwrap();
            }
            Ok(other) => break Err(format!("{other:?}")),
            Err(e) => break Err(e.to_string()),
        }
    };
    crash.set_faults(crate::io::crash::Faults {
        read: 0.0,
        write: 1.0,
        sync: 1.0,
        short_write: 0.0,
    });
    let ended = loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => {
                if let Err(e) = crash.step() {
                    break Err(e.to_string());
                }
            }
            Ok(crate::StepResult::Done) => break Ok(()),
            Ok(other) => break Err(format!("{other:?}")),
            Err(e) => break Err(e.to_string()),
        }
    };
    drop(statement);
    let seen = sees(&reader, "made");
    drop((writer, reader, db));
    assert_eq!(
        published,
        Ok(()),
        "the commit was never published with its ending waiting"
    );
    assert!(
        ended.is_err(),
        "the commit's wait for durability did not fail: {ended:?}"
    );
    assert_eq!(
        seen,
        Ok(()),
        "a published commit that then failed left its table unpublished: {ended:?}"
    );
}

/// Runs `sql` on `conn` to its end on `crash`, stepping the IO it waits on, and says how it ended.
fn ran_on(
    conn: &Arc<Connection>,
    crash: &crate::io::crash::CrashIO,
    sql: &str,
) -> std::result::Result<(), String> {
    let mut statement = conn.prepare(sql).map_err(|e| e.to_string())?;
    loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => crash.step().map_err(|e| e.to_string())?,
            Ok(crate::StepResult::Done) => return Ok(()),
            Ok(crate::StepResult::Row) => {}
            Ok(other) => return Err(format!("{other:?}")),
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// An Atoll database at `/db/{name}` whose connections may attach database files.
fn open_attaching(io: &Arc<dyn IO>, name: &str) -> Arc<Database> {
    Database::open(
        io.clone(),
        &format!("/db/{name}"),
        OpenOptions::new(Arc::new(SqliteDialect))
            .atoll(config())
            .db_opts(DatabaseOpts::default().with_attach(true)),
    )
    .unwrap()
}

/// The rows of `side` in the database file `/db/{name}-aux`, read through a database of its own.
fn side_rows(io: &Arc<dyn IO>, name: &str) -> std::result::Result<Vec<Vec<Value>>, String> {
    let aux = Database::open(
        io.clone(),
        &format!("/db/{name}-aux"),
        OpenOptions::new(Arc::new(SqliteDialect)),
    )
    .map_err(|e| e.to_string())?;
    let conn = aux.connect(ConnectOpts::new()).map_err(|e| e.to_string())?;
    conn.prepare("SELECT b FROM side")
        .and_then(|mut statement| statement.run_collect_rows())
        .map_err(|e| e.to_string())
}

/// A `COMMIT` of a table made in the main database and a row written to a table of an attached
/// database file, its commit published, then its log failing (faults on the log's file alone): the
/// commit stands, so the statement finishes it whole, the attached database's part committed, and
/// returns the failure once done. Stepped to its end, or, if `dropped`, dropped with its wait on
/// the failing log in flight, the reset finishing it (before, the failure aborted the statement,
/// rolling the attached database's part back under the main database's published commit).
fn a_published_commit_whose_log_fails(seed: u64, dropped: bool) {
    let name = format!(
        "published-then-failed-attached{}",
        if dropped { "-dropped" } else { "" }
    );
    let crash = Arc::new(crate::io::crash::CrashIO::new(seed));
    let io: Arc<dyn IO> = crash.clone();
    let db = open_attaching(&io, &name);
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        format!("ATTACH '/db/{name}-aux' AS aux"),
        "BEGIN".to_string(),
        "CREATE TABLE kept(a INTEGER)".to_string(),
        "CREATE TABLE aux.side(b INTEGER)".to_string(),
        "INSERT INTO aux.side VALUES (1)".to_string(),
    ]
    .iter()
    .map(|sql| ran_on(&writer, &crash, sql))
    .collect::<std::result::Result<Vec<_>, _>>();
    let mut statement = writer.prepare("COMMIT").unwrap();
    let published = loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => {
                if statement.atoll_ending_published() {
                    break Ok(());
                }
                crash.step().unwrap();
            }
            Ok(other) => break Err(format!("{other:?}")),
            Err(e) => break Err(e.to_string()),
        }
    };
    crash.set_faults_on(Some(&format!("/db/{name}.log.{:016x}", 0)));
    crash.set_faults(crate::io::crash::Faults {
        read: 0.0,
        write: 1.0,
        sync: 1.0,
        short_write: 0.0,
    });
    let ended = match dropped {
        true => None,
        false => Some(loop {
            match statement.step() {
                Ok(crate::StepResult::IO) => crash.step().unwrap(),
                Ok(crate::StepResult::Done) => break Ok(()),
                Ok(other) => break Err(format!("{other:?}")),
                Err(e) => break Err(e.to_string()),
            }
        }),
    };
    drop(statement);
    crash.set_faults(crate::io::crash::Faults::default());
    crash.set_faults_on(None);
    // The log failed for good: a later commit to the main database fails.
    let later = ran_on(&reader, &crash, "CREATE TABLE later(a INTEGER)");
    let seen = sees(&reader, "kept");
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, reader, db));
    let beside = side_rows(&io, &name);
    assert_eq!(setup.map(drop), Ok(()));
    assert_eq!(
        published,
        Ok(()),
        "the COMMIT was never published with its ending waiting"
    );
    if let Some(ended) = ended {
        assert!(
            ended.is_err(),
            "the COMMIT did not return its log's failure: {ended:?}"
        );
    }
    assert!(
        later.is_err(),
        "the log did not fail: a later commit went through"
    );
    assert_eq!(
        seen,
        Ok(()),
        "a published commit whose log failed left its main table unpublished"
    );
    assert_eq!(
        beside,
        Ok(vec![vec![Value::from_i64(1)]]),
        "a published commit whose log failed rolled back its attached database's part"
    );
    assert_eq!(
        state,
        (true, crate::connection::TransactionState::None),
        "a published commit whose log failed left its connection in a transaction"
    );
}

#[test]
fn a_published_commit_whose_log_fails_commits_its_attached_part_and_returns_the_failure() {
    a_published_commit_whose_log_fails(659, false);
}

#[test]
fn a_published_commit_dropped_as_its_log_fails_commits_its_attached_part() {
    a_published_commit_whose_log_fails(660, true);
}

/// An Atoll ending told that an IO it waited on failed, its commit published, returns that failure
/// at its next step and is over: stepped again, it is done, nothing of it run again.
#[test]
fn a_published_endings_failure_is_what_its_next_step_returns() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(661));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/ending-told-it-failed");
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let setup = ["BEGIN", "CREATE TABLE t(a INTEGER)"]
        .iter()
        .map(|sql| ran_on(&conn, &crash, sql))
        .collect::<std::result::Result<Vec<_>, _>>();
    let mut ending = conn.atoll().unwrap().end(true).unwrap();
    let published = loop {
        match ending.step() {
            Ok(crate::IOResult::IO(_)) if ending.published() => break Ok(()),
            Ok(crate::IOResult::IO(_)) => crash.step().unwrap(),
            Ok(crate::IOResult::Done(())) => break Err("done before it waited".to_string()),
            Err(e) => break Err(e.to_string()),
        }
    };
    ending.failed(crate::LimboError::InternalError(
        "the test's failure".to_string(),
    ));
    let first = ending.step().map(|_| ()).map_err(|e| e.to_string());
    let second = ending
        .step()
        .map(|stepped| !stepped.is_io())
        .map_err(|e| e.to_string());
    drop(ending);
    drop((conn, db));
    assert_eq!(setup.map(drop), Ok(()));
    assert_eq!(published, Ok(()), "the commit never waited once published");
    assert_eq!(first, Err("Internal error: the test's failure".to_string()));
    assert_eq!(
        second,
        Ok(true),
        "an ending over after its failure stepped on"
    );
}

/// A progress handler asking to stop once a `COMMIT`'s ending has begun does not stop it: the
/// outcome is decided, so the commit finishes whole, the attached database's part committed, and
/// the statement is done (before, its interrupt aborted the `COMMIT` mid-ending, rolling the
/// attached database's part back under the main database's commit).
#[test]
fn a_progress_handler_at_a_commits_ending_does_not_stop_it() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let name = "progress-at-ending";
    let crash = Arc::new(crate::io::crash::CrashIO::new(662));
    let io: Arc<dyn IO> = crash.clone();
    let db = open_attaching(&io, name);
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        format!("ATTACH '/db/{name}-aux' AS aux"),
        "BEGIN".to_string(),
        "CREATE TABLE kept(a INTEGER)".to_string(),
        "CREATE TABLE aux.side(b INTEGER)".to_string(),
        "INSERT INTO aux.side VALUES (1)".to_string(),
    ]
    .iter()
    .map(|sql| ran_on(&writer, &crash, sql))
    .collect::<std::result::Result<Vec<_>, _>>();
    let stop = Arc::new(AtomicBool::new(false));
    writer.set_progress_handler(
        1,
        Some(Box::new({
            let stop = stop.clone();
            move || stop.load(Ordering::SeqCst)
        })),
    );
    let mut statement = writer.prepare("COMMIT").unwrap();
    let outcome = loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => {
                if statement.atoll_ending_phase() == Some("ending") {
                    stop.store(true, Ordering::SeqCst);
                }
                crash.step().unwrap();
            }
            Ok(crate::StepResult::Done) => break Ok(()),
            Ok(other) => break Err(format!("{other:?}")),
            Err(e) => break Err(e.to_string()),
        }
    };
    drop(statement);
    writer.set_progress_handler(0, None);
    let seen = sees(&reader, "kept");
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, reader, db));
    let beside = side_rows(&io, name);
    assert_eq!(setup.map(drop), Ok(()));
    assert!(
        stop.load(Ordering::SeqCst),
        "the COMMIT never waited in its ending"
    );
    assert_eq!(
        outcome,
        Ok(()),
        "a progress handler stopped a COMMIT whose ending had begun"
    );
    assert_eq!(seen, Ok(()));
    assert_eq!(
        beside,
        Ok(vec![vec![Value::from_i64(1)]]),
        "a COMMIT stopped mid-ending rolled back its attached database's part"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
}

/// A `ROLLBACK` ends its transaction within its first step, its ending driven to its end in place
/// (`Connection::rollback_atoll`, a synchronous site, R19), so no statement is ever left holding a
/// rollback's ending to be reset or dropped: the transaction made a table in the main database and
/// one in an attached database file, both gone from every schema once that step returns, the
/// connection back in autocommit, and the main table's name free for its next statement.
#[test]
fn a_rollback_ends_its_transaction_within_its_first_step() {
    let name = "rollback-in-one-step";
    let crash = Arc::new(crate::io::crash::CrashIO::new(663));
    let io: Arc<dyn IO> = crash.clone();
    let db = open_attaching(&io, name);
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        format!("ATTACH '/db/{name}-aux' AS aux"),
        "BEGIN".to_string(),
        "CREATE TABLE gone(a INTEGER)".to_string(),
        "INSERT INTO gone VALUES (1)".to_string(),
        "CREATE TABLE aux.side(b INTEGER)".to_string(),
        "INSERT INTO aux.side VALUES (1)".to_string(),
    ]
    .iter()
    .map(|sql| ran_on(&writer, &crash, sql))
    .collect::<std::result::Result<Vec<_>, _>>();
    let first = writer
        .prepare("ROLLBACK")
        .and_then(|mut statement| statement.step())
        .map(|stepped| format!("{stepped:?}"))
        .map_err(|e| e.to_string());
    let gone = (
        sees(&reader, "gone").is_err(),
        sees(&writer, "gone").is_err(),
        sees(&writer, "aux.side").is_err(),
    );
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    let remade = ran_on(&writer, &crash, "CREATE TABLE gone(a INTEGER)");
    let seen_remade = sees(&reader, "gone");
    drop((writer, reader, db));
    assert_eq!(setup.map(drop), Ok(()));
    assert_eq!(
        first,
        Ok("Done".to_string()),
        "a ROLLBACK left its first step with its ending unfinished"
    );
    assert_eq!(
        gone,
        (true, true, true),
        "a ROLLBACK left a table it rolled back"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
    assert_eq!(remade, Ok(()));
    assert_eq!(seen_remade, Ok(()));
}

/// Steps `statement` until it returns `IO` with its Atoll commit published, stepping the IO it
/// waits on before that, and says how far it got.
fn step_until_published(
    statement: &mut crate::Statement,
    crash: &crate::io::crash::CrashIO,
) -> std::result::Result<(), String> {
    loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => {
                if statement.atoll_ending_published() {
                    return Ok(());
                }
                crash.step().map_err(|e| e.to_string())?;
            }
            Ok(other) => return Err(format!("{other:?}")),
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// Steps `statement` to its end, stepping the IO it waits on, and says how it ended.
fn step_to_end(
    statement: &mut crate::Statement,
    crash: &crate::io::crash::CrashIO,
) -> std::result::Result<(), String> {
    loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => crash.step().map_err(|e| e.to_string())?,
            Ok(crate::StepResult::Done) => return Ok(()),
            Ok(crate::StepResult::Row) => {}
            Ok(other) => return Err(format!("{other:?}")),
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// A statement of a connection started while another of its statements holds the connection's
/// end, a `CREATE TABLE` waiting in its commit's ending, is refused and changes nothing, and a close
/// then is too; once the first has finished, as it drops, the refused statement runs and the
/// connection closes (the auditor's #4423: before, the second ran its own ending to its end, letting
/// the first's protection go, and a close then went ahead under the first).
#[test]
fn a_statement_started_while_another_ends_the_transaction_is_refused() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(665));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/ending-overlap");
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let setup = ran_on(&writer, &crash, "CREATE TABLE t(a INTEGER)");
    let mut first = writer.prepare("CREATE TABLE made(a INTEGER)").unwrap();
    let met = loop {
        match first.step() {
            Ok(crate::StepResult::IO) => {
                if first.atoll_ending_phase() == Some("ending") {
                    break Ok(());
                }
                crash.step().unwrap();
            }
            Ok(other) => break Err(format!("{other:?}")),
            Err(e) => break Err(e.to_string()),
        }
    };
    let mut second = writer.prepare("INSERT INTO t VALUES (2)").unwrap();
    let refused = match second.step() {
        Err(crate::LimboError::StatementsInProgress(_)) => Ok(()),
        other => Err(format!("{other:?}")),
    };
    let close_refused = match writer.close() {
        Err(crate::LimboError::StatementsInProgress(_)) => Ok(()),
        other => Err(format!("{other:?}")),
    };
    drop(first);
    let ran = second
        .reset()
        .map_err(|e| e.to_string())
        .and_then(|()| step_to_end(&mut second, &crash));
    drop(second);
    let rows = reader
        .prepare("SELECT a FROM t")
        .and_then(|mut statement| statement.run_collect_rows())
        .map_err(|e| e.to_string());
    let seen = sees(&reader, "made");
    let closed = writer.close().map_err(|e| e.to_string());
    drop((writer, reader, db));
    assert_eq!(setup, Ok(()));
    assert_eq!(met, Ok(()), "the CREATE TABLE never waited in its ending");
    assert_eq!(
        refused,
        Ok(()),
        "a statement started while another ended the transaction"
    );
    assert_eq!(
        close_refused,
        Ok(()),
        "a close went ahead while a statement ended the transaction"
    );
    assert_eq!(
        ran,
        Ok(()),
        "the refused statement did not run once the ending had finished"
    );
    assert_eq!(rows, Ok(vec![vec![Value::from_i64(2)]]));
    assert_eq!(seen, Ok(()));
    assert_eq!(closed, Ok(()));
}

/// A `CREATE TABLE` whose commit is published and still waits for its durability: its table is in
/// the shared schema already, so another connection prepares a statement on it and commits a
/// `CREATE TABLE` of its own meanwhile, and both tables are there once both have finished and after a
/// reopen (before, the schema was published only once the whole ending had ended, so for as long as
/// it waited, on durability, its drain or a full checkpoint, the committed table was missing from the
/// schema every other connection prepared against).
#[test]
fn a_commits_schema_is_published_with_its_commit() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(666));
    let io: Arc<dyn IO> = crash.clone();
    let path = "/db/ddl-published-with-its-commit";
    let db = open(&io, path);
    let first_conn = db.connect(ConnectOpts::new()).unwrap();
    let second_conn = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let mut first = first_conn.prepare("CREATE TABLE first(a INTEGER)").unwrap();
    let published = step_until_published(&mut first, &crash);
    let while_waiting = sees(&reader, "first");
    let second = ran_on(&second_conn, &crash, "CREATE TABLE second(a INTEGER)");
    let first_ended = step_to_end(&mut first, &crash);
    drop(first);
    let seen = (sees(&reader, "first"), sees(&reader, "second"));
    drop((first_conn, second_conn, reader, db));
    let reopened = open(&io, path);
    let fresh = reopened.connect(ConnectOpts::new()).unwrap();
    let after_reopen = (sees(&fresh, "first"), sees(&fresh, "second"));
    drop((fresh, reopened));
    assert_eq!(
        published,
        Ok(()),
        "the CREATE TABLE's commit was never published with its ending waiting"
    );
    assert_eq!(
        while_waiting,
        Ok(()),
        "a published commit's table was missing from the shared schema while its ending waited"
    );
    assert_eq!(second, Ok(()));
    assert_eq!(first_ended, Ok(()));
    assert_eq!(seen, (Ok(()), Ok(())));
    assert_eq!(after_reopen, (Ok(()), Ok(())));
}

/// Steps `scan` to its end, stepping the IO it waits on, and says what it read and how it ended.
fn scan_rest(
    scan: &mut crate::Statement,
    crash: &crate::io::crash::CrashIO,
    read: &mut Vec<Vec<Value>>,
) -> std::result::Result<(), String> {
    loop {
        match scan.step() {
            Ok(crate::StepResult::IO) => crash.step().map_err(|e| e.to_string())?,
            Ok(crate::StepResult::Row) => {
                read.push(scan.row().unwrap().get_values().cloned().collect());
            }
            Ok(crate::StepResult::Done) => return Ok(()),
            Ok(other) => return Err(format!("{other:?}")),
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// Steps `scan` to its first row, stepping the IO it waits on.
fn scan_first(
    scan: &mut crate::Statement,
    crash: &crate::io::crash::CrashIO,
) -> std::result::Result<Vec<Value>, String> {
    loop {
        match scan.step() {
            Ok(crate::StepResult::IO) => crash.step().map_err(|e| e.to_string())?,
            Ok(crate::StepResult::Row) => {
                return Ok(scan.row().unwrap().get_values().cloned().collect());
            }
            Ok(other) => return Err(format!("{other:?}")),
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// Steps `statement` until it returns `IO` in its Atoll ending with its commit published or not, as
/// `published` asks, and says so; the first `IO` of the other kind is an error naming it.
fn step_to_ending(
    statement: &mut crate::Statement,
    crash: &crate::io::crash::CrashIO,
    published: bool,
) -> std::result::Result<(), String> {
    loop {
        match statement.step() {
            Ok(crate::StepResult::IO) => {
                if statement.atoll_ending_phase() == Some("ending") {
                    return match statement.atoll_ending_published() == published {
                        true => Ok(()),
                        false => Err(format!(
                            "waited in its ending with its commit published: {}",
                            !published
                        )),
                    };
                }
                crash.step().map_err(|e| e.to_string())?;
            }
            Ok(other) => return Err(format!("{other:?}")),
            Err(e) => return Err(e.to_string()),
        }
    }
}

/// A scan of a connection, part way through, while another statement of the connection ends the
/// transaction it reads, its ending waiting with its commit published: the scan, stepped on,
/// reads on in a read transaction begun for it, which sees what the commit left, and reads every
/// row; the ending then finishes, its table published (the auditor's #4435: before, the scan
/// failed, an Atoll cursor used outside a transaction, the transaction gone with the ending and
/// the siblings' read begun only at its end).
#[test]
fn a_scan_running_while_its_connection_publishes_a_commit_reads_on() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(667));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/scan-beside-a-published-ending");
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        "CREATE TABLE t(a INTEGER)",
        "INSERT INTO t VALUES (1), (2), (3)",
    ]
    .iter()
    .map(|sql| ran_on(&writer, &crash, sql))
    .collect::<std::result::Result<Vec<_>, _>>();
    let mut scan = writer.prepare("SELECT a FROM t").unwrap();
    let mut read = Vec::new();
    let first = scan_first(&mut scan, &crash).map(|row| read.push(row));
    let mut ender = writer.prepare("CREATE TABLE made(a INTEGER)").unwrap();
    let waited = step_to_ending(&mut ender, &crash, true);
    let rest = scan_rest(&mut scan, &crash, &mut read);
    drop(scan);
    let ended = step_to_end(&mut ender, &crash);
    drop(ender);
    let seen = (sees(&reader, "made"), sees(&writer, "made"));
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, reader, db));
    assert_eq!(setup.map(drop), Ok(()));
    assert_eq!(first, Ok(()));
    assert_eq!(waited, Ok(()));
    assert_eq!(
        rest,
        Ok(()),
        "a scan stepped while its connection's commit ended did not read on"
    );
    assert_eq!(
        read,
        (1..=3)
            .map(|a| vec![Value::from_i64(a)])
            .collect::<Vec<_>>(),
        "a scan stepped while its connection's commit ended lost rows"
    );
    assert_eq!(ended, Ok(()));
    assert_eq!(seen, (Ok(()), Ok(())));
    assert_eq!(state, (true, crate::connection::TransactionState::None));
}

/// The same while the ending waits before its commit is published (a test's pause before the
/// publication, waited on as the ending's own IO), the scan over a table the transaction wrote: the
/// scan, stepped on, steps the ending in the connection's place itself and waits on what the ending
/// waits on, and once that is done and the commit published it reads on in a read that sees what
/// the transaction wrote, every row; the `COMMIT` then finishes.
#[test]
fn a_scan_running_while_its_connection_commit_waits_to_publish_steps_it_and_reads_on() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(668));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/scan-beside-an-unpublished-ending");
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        "CREATE TABLE t(a INTEGER)",
        "INSERT INTO t VALUES (1), (2), (3)",
        "BEGIN",
        "INSERT INTO t VALUES (4), (5)",
    ]
    .iter()
    .map(|sql| ran_on(&writer, &crash, sql))
    .collect::<std::result::Result<Vec<_>, _>>();
    let mut scan = writer.prepare("SELECT a FROM t").unwrap();
    let mut read = Vec::new();
    let first = scan_first(&mut scan, &crash).map(|row| read.push(row));
    let pause = crate::io::Completion::new_wait();
    crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = Some(pause.clone()));
    let mut ender = writer.prepare("COMMIT").unwrap();
    let waited = step_to_ending(&mut ender, &crash, false);
    let scan_waited = match scan.step() {
        Ok(crate::StepResult::IO) => Ok(()),
        other => Err(format!("{other:?}")),
    };
    pause.complete(0);
    crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = None);
    let rest = scan_rest(&mut scan, &crash, &mut read);
    drop(scan);
    let ended = step_to_end(&mut ender, &crash);
    drop(ender);
    let committed = reader
        .prepare("SELECT count(*) FROM t")
        .and_then(|mut statement| statement.run_collect_rows())
        .map_err(|e| e.to_string());
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, reader, db));
    assert_eq!(setup.map(drop), Ok(()));
    assert_eq!(first, Ok(()));
    assert_eq!(waited, Ok(()), "the COMMIT never waited before publishing");
    assert_eq!(
        scan_waited,
        Ok(()),
        "the scan did not wait on the unpublished ending it stepped"
    );
    assert_eq!(
        rest,
        Ok(()),
        "a scan stepped while its connection's commit waited to publish did not read on"
    );
    assert_eq!(
        read,
        (1..=5)
            .map(|a| vec![Value::from_i64(a)])
            .collect::<Vec<_>>(),
        "a scan stepped while its connection's commit waited to publish read another view"
    );
    assert_eq!(ended, Ok(()));
    assert_eq!(committed, Ok(vec![vec![Value::from_i64(5)]]));
    assert_eq!(state, (true, crate::connection::TransactionState::None));
}

/// A scan of a connection read on while the connection's `COMMIT` ends its attached database's
/// part, then dropped before that part has ended: the attached part commits all the same, the
/// read begun for the scan does not outlive it (the connection's next statement sees a row another
/// connection commits after), and the connection is back in autocommit.
#[test]
fn a_scan_dropped_while_its_connection_commits_its_attached_part_leaves_it_whole() {
    let name = "scan-dropped-beside-an-attached-ending";
    let crash = Arc::new(crate::io::crash::CrashIO::new(669));
    let io: Arc<dyn IO> = crash.clone();
    let db = open_attaching(&io, name);
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        format!("ATTACH '/db/{name}-aux' AS aux"),
        "CREATE TABLE t(a INTEGER)".to_string(),
        "INSERT INTO t VALUES (1), (2), (3)".to_string(),
        "BEGIN".to_string(),
        "INSERT INTO t VALUES (4)".to_string(),
        "CREATE TABLE aux.side(b INTEGER)".to_string(),
        "INSERT INTO aux.side VALUES (1)".to_string(),
    ]
    .iter()
    .map(|sql| ran_on(&writer, &crash, sql))
    .collect::<std::result::Result<Vec<_>, _>>();
    let mut scan = writer.prepare("SELECT a FROM t").unwrap();
    let first = scan_first(&mut scan, &crash);
    let mut commit = writer.prepare("COMMIT").unwrap();
    let attached = loop {
        match commit.step() {
            Ok(crate::StepResult::IO) => {
                if commit.atoll_ending_phase() == Some("attached") {
                    break Ok(());
                }
                crash.step().unwrap();
            }
            Ok(other) => break Err(format!("{other:?}")),
            Err(e) => break Err(e.to_string()),
        }
    };
    let second = scan_first(&mut scan, &crash);
    drop(scan);
    let ended = step_to_end(&mut commit, &crash);
    drop(commit);
    let later = ran_on(&other, &crash, "INSERT INTO t VALUES (5)");
    let seen = writer
        .prepare("SELECT count(*) FROM t")
        .and_then(|mut statement| statement.run_collect_rows())
        .map_err(|e| e.to_string());
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, other, db));
    let beside = side_rows(&io, name);
    assert_eq!(setup.map(drop), Ok(()));
    assert_eq!(first, Ok(vec![Value::from_i64(1)]));
    assert_eq!(
        attached,
        Ok(()),
        "the COMMIT never waited on its attached part"
    );
    assert_eq!(
        second,
        Ok(vec![Value::from_i64(2)]),
        "the scan did not read on"
    );
    assert_eq!(ended, Ok(()));
    assert_eq!(later, Ok(()));
    assert_eq!(
        seen,
        Ok(vec![vec![Value::from_i64(5)]]),
        "the read begun for a dropped scan outlived it"
    );
    assert_eq!(
        beside,
        Ok(vec![vec![Value::from_i64(1)]]),
        "a scan dropped beside a COMMIT's attached part rolled it back"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
}

/// A scan begun inside an explicit transaction, part way through when its `COMMIT` ends, reads on
/// in the read the `COMMIT`'s end begins for it, and once the scan has finished and dropped that
/// read is gone: the connection's next statement sees a row another connection committed after (the
/// auditor's #4446: the read stayed open with no statement left, and the next statement read its
/// old snapshot, missing the row).
#[test]
fn a_scan_begun_in_a_transaction_and_finished_after_its_commit_leaves_no_read_behind() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(670));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/scan-spanning-a-commit");
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        "CREATE TABLE scanned(a INTEGER)",
        "INSERT INTO scanned VALUES (10), (20), (30)",
        "CREATE TABLE kept(a INTEGER)",
        "BEGIN",
        "INSERT INTO kept VALUES (1)",
    ]
    .iter()
    .map(|sql| ran_on(&writer, &crash, sql))
    .collect::<std::result::Result<Vec<_>, _>>();
    let mut scan = writer.prepare("SELECT a FROM scanned").unwrap();
    let mut read = Vec::new();
    let first = scan_first(&mut scan, &crash).map(|row| read.push(row));
    let committed = ran_on(&writer, &crash, "COMMIT");
    let rest = scan_rest(&mut scan, &crash, &mut read);
    drop(scan);
    let left_open = writer.atoll().unwrap().is_open();
    let later = ran_on(&other, &crash, "INSERT INTO kept VALUES (2)");
    let seen = writer
        .prepare("SELECT a FROM kept")
        .and_then(|mut statement| statement.run_collect_rows())
        .map_err(|e| e.to_string());
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, other, db));
    assert_eq!(setup.map(drop), Ok(()));
    assert_eq!(first, Ok(()));
    assert_eq!(committed, Ok(()));
    assert_eq!(rest, Ok(()));
    assert_eq!(
        read,
        [10, 20, 30].map(|a| vec![Value::from_i64(a)]).to_vec()
    );
    assert!(
        !left_open,
        "a read begun for a scan outlived it with no statement left"
    );
    assert_eq!(later, Ok(()));
    assert_eq!(
        seen,
        Ok(vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]),
        "the connection's next statement read an old snapshot"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
}

/// A scan that read on beside a `CREATE TABLE` whose commit, published, still waits, then dropped
/// before that ending is done: the read begun for it is ended as it goes, while the ending still
/// waits, since only the statement ending the transaction is left and it reads in no read of its
/// own (the auditor's #4443 item 4), and with no ending of its own made beside the one waiting (the
/// auditor's #4459); the ending then finishes, its table published.
#[test]
fn a_sibling_gone_while_its_connections_commit_waits_releases_its_read_at_once() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(671));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/sibling-gone-beside-an-ending");
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        "CREATE TABLE t(a INTEGER)",
        "INSERT INTO t VALUES (1), (2), (3)",
    ]
    .iter()
    .map(|sql| ran_on(&writer, &crash, sql))
    .collect::<std::result::Result<Vec<_>, _>>();
    let mut scan = writer.prepare("SELECT a FROM t").unwrap();
    let first = scan_first(&mut scan, &crash);
    let mut ender = writer.prepare("CREATE TABLE made(a INTEGER)").unwrap();
    let waited = step_to_ending(&mut ender, &crash, true);
    let second = scan_first(&mut scan, &crash);
    let served = writer.atoll().unwrap().is_open();
    let made = crate::storage::atoll::testing::endings_made();
    drop(scan);
    let after_the_sibling = (
        writer.atoll().unwrap().is_open(),
        ender.atoll_ending_phase(),
        crate::storage::atoll::testing::endings_made() - made,
    );
    let ended = step_to_end(&mut ender, &crash);
    drop(ender);
    let seen = sees(&reader, "made");
    let state = (
        writer.get_auto_commit(),
        writer.get_tx_state(),
        writer.atoll().unwrap().is_open(),
    );
    drop((writer, reader, db));
    assert_eq!(setup.map(drop), Ok(()));
    assert_eq!(first, Ok(vec![Value::from_i64(1)]));
    assert_eq!(waited, Ok(()));
    assert_eq!(second, Ok(vec![Value::from_i64(2)]));
    assert!(served, "no read was begun for the scan");
    assert_eq!(
        after_the_sibling,
        (false, Some("ending"), 0),
        "the read begun for a scan outlived it while the ending waited, or its end made a second \
         ending of the connection"
    );
    assert_eq!(ended, Ok(()));
    assert_eq!(seen, Ok(()));
    assert_eq!(
        state,
        (true, crate::connection::TransactionState::None, false)
    );
}

/// The setup of a `COMMIT` of a table made in the main database and a row written to a table of
/// an attached database file, with a scan of the connection at its first row beside it.
fn commit_beside_a_scan(
    name: &str,
    crash: &Arc<crate::io::crash::CrashIO>,
) -> (
    Arc<Database>,
    Arc<Connection>,
    Arc<Connection>,
    std::result::Result<(), String>,
) {
    let io: Arc<dyn IO> = crash.clone();
    let db = open_attaching(&io, name);
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        format!("ATTACH '/db/{name}-aux' AS aux"),
        "CREATE TABLE scanned(a INTEGER)".to_string(),
        "INSERT INTO scanned VALUES (10), (20), (30)".to_string(),
        "CREATE TABLE aux.side(b INTEGER)".to_string(),
        "BEGIN".to_string(),
        "CREATE TABLE kept(a INTEGER)".to_string(),
        "INSERT INTO aux.side VALUES (1)".to_string(),
    ]
    .iter()
    .try_for_each(|sql| ran_on(&writer, crash, sql));
    (db, writer, reader, setup)
}

/// A scan reading on beside its connection's `COMMIT`, paused before it publishes, is the one to
/// step it to its publication: the scan reads on at once, never handed the commit's wait for
/// durability, and when the main log then fails, the failure is the `COMMIT`'s, which returns it
/// and finishes the commit whole, the attached row committed and the table published (the
/// auditor's #4454: the scan was handed that wait, met its failure, and its abort rolled the
/// attached part back under the published main commit).
#[test]
fn a_sibling_that_publishes_its_connections_commit_reads_on_and_leaves_the_failure_to_it() {
    let name = "sibling-publishes-then-the-log-fails";
    let crash = Arc::new(crate::io::crash::CrashIO::new(672));
    let (db, writer, reader, setup) = commit_beside_a_scan(name, &crash);
    let mut scan = writer.prepare("SELECT a FROM scanned").unwrap();
    let mut read = Vec::new();
    let first = scan_first(&mut scan, &crash).map(|row| read.push(row));
    let pause = crate::io::Completion::new_wait();
    crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = Some(pause.clone()));
    let mut ender = writer.prepare("COMMIT").unwrap();
    let waited = step_to_ending(&mut ender, &crash, false);
    pause.complete(0);
    crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = None);
    let helped = match scan.step() {
        Ok(crate::StepResult::Row) => {
            read.push(scan.row().unwrap().get_values().cloned().collect());
            Ok(())
        }
        other => Err(format!("{other:?}")),
    };
    let published = ender.atoll_ending_published();
    crash.set_faults_on(Some(&format!("/db/{name}.log.{:016x}", 0)));
    crash.set_faults(crate::io::crash::Faults {
        read: 0.0,
        write: 1.0,
        sync: 1.0,
        short_write: 0.0,
    });
    let rest = scan_rest(&mut scan, &crash, &mut read);
    drop(scan);
    let ended = step_to_end(&mut ender, &crash);
    drop(ender);
    crash.set_faults(crate::io::crash::Faults::default());
    crash.set_faults_on(None);
    let table = sees(&reader, "kept");
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, reader, db));
    let io: Arc<dyn IO> = crash.clone();
    let side = side_rows(&io, name);
    drop(io);
    let in_flight = crash.in_flight();
    assert_eq!(setup, Ok(()));
    assert_eq!(first, Ok(()));
    assert_eq!(waited, Ok(()), "the COMMIT never waited before publishing");
    assert_eq!(
        helped,
        Ok(()),
        "the scan that published the commit did not read on at once"
    );
    assert!(published, "the scan's step did not publish the commit");
    assert_eq!(rest, Ok(()), "the scan met the commit's failure");
    assert_eq!(
        read,
        [10, 20, 30].map(|a| vec![Value::from_i64(a)]).to_vec()
    );
    assert!(ended.is_err(), "the COMMIT lost the log's failure");
    assert_eq!(table, Ok(()));
    assert_eq!(
        side,
        Ok(vec![vec![Value::from_i64(1)]]),
        "the attached part of the published commit was rolled back"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
    assert_eq!(in_flight, 0);
}

/// A scan that fails while another statement of its connection ends the transaction rolls
/// nothing back: here its step of the `COMMIT`'s ending, paused before publishing, fails (the
/// test's pause failed, standing for any failure a statement reading on meets), the scan returns
/// the failure, and the `COMMIT` goes on to commit whole, the attached row and the table committed
/// (the auditor's #4454: the failing statement's abort rolled the connection's transaction back,
/// the attached part with it, under the ending).
#[test]
fn a_sibling_that_fails_beside_its_connections_ending_rolls_nothing_back() {
    let name = "sibling-fails-beside-an-ending";
    let crash = Arc::new(crate::io::crash::CrashIO::new(673));
    let (db, writer, reader, setup) = commit_beside_a_scan(name, &crash);
    let mut scan = writer.prepare("SELECT a FROM scanned").unwrap();
    let first = scan_first(&mut scan, &crash);
    let held = crate::io::Completion::new_wait();
    crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = Some(held.clone()));
    let mut ender = writer.prepare("COMMIT").unwrap();
    let waited = step_to_ending(&mut ender, &crash, false);
    // The ending's next stepper, the scan, waits on a pause of its own, which fails.
    let failing = crate::io::Completion::new_wait();
    crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = Some(failing.clone()));
    held.complete(0);
    let scan_waited = match scan.step() {
        Ok(crate::StepResult::IO) => Ok(()),
        other => Err(format!("{other:?}")),
    };
    failing.error(crate::error::CompletionError::IOError(
        std::io::ErrorKind::Other,
        "a sibling's pause",
    ));
    crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = None);
    let failed = match scan.step() {
        Err(e) => Ok(e.to_string()),
        other => Err(format!("{other:?}")),
    };
    drop(scan);
    let ended = step_to_end(&mut ender, &crash);
    drop(ender);
    let table = sees(&reader, "kept");
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, reader, db));
    let io: Arc<dyn IO> = crash.clone();
    let side = side_rows(&io, name);
    drop(io);
    let in_flight = crash.in_flight();
    assert_eq!(setup, Ok(()));
    assert_eq!(first, Ok(vec![Value::from_i64(10)]));
    assert_eq!(waited, Ok(()), "the COMMIT never waited before publishing");
    assert_eq!(scan_waited, Ok(()), "the scan did not step the ending");
    assert!(
        failed.is_ok(),
        "the scan's failure was not returned: {failed:?}"
    );
    assert_eq!(ended, Ok(()), "the COMMIT did not finish");
    assert_eq!(table, Ok(()));
    assert_eq!(
        side,
        Ok(vec![vec![Value::from_i64(1)]]),
        "a failing scan rolled the attached part back under its connection's ending"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
    assert_eq!(in_flight, 0);
}

/// A scan reading on beside its connection's `COMMIT` begins its read only once the horizon covers
/// the commit, so that the read sees it: while the horizon lags the published commit (held here by
/// the test, as a commit below it still drawing on another slot holds it: row writers hold the
/// write gate shared, so several connections' commits draw at once), the scan waits, and once it
/// rises the scan reads every row its transaction wrote (begun at the publication, the read would
/// be below the commit and miss them).
#[test]
fn a_sibling_reads_on_only_once_the_horizon_covers_its_connections_commit() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(674));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/sibling-under-a-held-horizon");
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        "CREATE TABLE t(a INTEGER)",
        "INSERT INTO t VALUES (1), (2), (3)",
        "BEGIN",
        "INSERT INTO t VALUES (4), (5)",
    ]
    .iter()
    .try_for_each(|sql| ran_on(&writer, &crash, sql));
    let mut scan = writer.prepare("SELECT a FROM t").unwrap();
    let mut read = Vec::new();
    let first = scan_first(&mut scan, &crash).map(|row| read.push(row));
    writer.atoll().unwrap().hold_horizon(true);
    let mut ender = writer.prepare("COMMIT").unwrap();
    let published = step_until_published(&mut ender, &crash);
    let scan_waited = match scan.step() {
        Ok(crate::StepResult::IO) => Ok(()),
        other => Err(format!("{other:?}")),
    };
    writer.atoll().unwrap().hold_horizon(false);
    let rest = scan_rest(&mut scan, &crash, &mut read);
    drop(scan);
    let ended = step_to_end(&mut ender, &crash);
    drop(ender);
    let committed = reader
        .prepare("SELECT count(*) FROM t")
        .and_then(|mut statement| statement.run_collect_rows())
        .map_err(|e| e.to_string());
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((writer, reader, db));
    assert_eq!(setup, Ok(()));
    assert_eq!(first, Ok(()));
    assert_eq!(published, Ok(()));
    assert_eq!(
        scan_waited,
        Ok(()),
        "the scan began its read while the horizon lagged the commit"
    );
    assert_eq!(rest, Ok(()));
    assert_eq!(
        read,
        (1..=5)
            .map(|a| vec![Value::from_i64(a)])
            .collect::<Vec<_>>(),
        "the scan read below its connection's commit"
    );
    assert_eq!(ended, Ok(()));
    assert_eq!(committed, Ok(vec![vec![Value::from_i64(5)]]));
    assert_eq!(state, (true, crate::connection::TransactionState::None));
}

/// Steps `statement` to its end, stepping the IO it waits on, and says how it ended and whether its
/// commit was ever published.
fn step_to_end_published(
    statement: &mut crate::Statement,
    crash: &crate::io::crash::CrashIO,
) -> (std::result::Result<(), String>, bool) {
    let mut published = false;
    loop {
        let step = statement.step();
        published |= statement.atoll_ending_published();
        match step {
            Ok(crate::StepResult::IO) => {
                if let Err(e) = crash.step() {
                    return (Err(e.to_string()), published);
                }
            }
            Ok(crate::StepResult::Done) => return (Ok(()), published),
            Ok(crate::StepResult::Row) => {}
            Ok(other) => return (Err(format!("{other:?}")), published),
            Err(e) => return (Err(e.to_string()), published),
        }
    }
}

/// Faults every write and sync of the database at `name`'s first log file.
fn fail_the_log(crash: &crate::io::crash::CrashIO, name: &str) {
    crash.set_faults_on(Some(&format!("{name}.log.{:016x}", 0)));
    crash.set_faults(crate::io::crash::Faults {
        read: 0.0,
        write: 1.0,
        sync: 1.0,
        short_write: 0.0,
    });
}

fn clear_faults(crash: &crate::io::crash::CrashIO) {
    crash.set_faults(crate::io::crash::Faults::default());
    crash.set_faults_on(None);
}

/// A `COMMIT` whose log fails before it publishes, its transaction small enough that every change
/// is still in memory: the commit returns the failure, its transaction is rolled back whole, and
/// its slot and its hold of the write gate go, the database itself not failed (the auditor's
/// #4468: the transaction stayed seated with the gate held, and nothing owned it).
#[test]
fn a_commit_failing_before_it_publishes_rolls_back_and_lets_its_slot_and_gate_go() {
    use crate::storage::atoll::txn::Isolation;
    use crate::types::IOResult;
    let name = "/db/commit-fails-before-publishing";
    let crash = Arc::new(crate::io::crash::CrashIO::new(675));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, name);
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    let setup = [
        "CREATE TABLE t(a INTEGER)",
        "CREATE TABLE u(b INTEGER)",
        "BEGIN",
        "INSERT INTO t VALUES (1)",
    ]
    .iter()
    .try_for_each(|sql| ran_on(&writer, &crash, sql));
    // The log fails under another connection's commit.
    fail_the_log(&crash, name);
    let other_failed = ran_on(&other, &crash, "INSERT INTO u VALUES (1)").is_err();
    let mut commit = writer.prepare("COMMIT").unwrap();
    let (ended, published) = step_to_end_published(&mut commit, &crash);
    drop(commit);
    clear_faults(&crash);
    let inner = writer.atoll().unwrap().db.clone();
    let txns = inner.atoll().transactions();
    let failure = txns.failure().map(|e| e.to_string());
    let mut begun = Vec::new();
    let mut begins = Vec::new();
    for _ in 0..txns.slot_count() {
        match txns.begin(Isolation::Snapshot) {
            Ok(IOResult::Done(txn)) => {
                begins.push("Done");
                begun.push(txn);
            }
            Ok(IOResult::IO(_)) => begins.push("IO"),
            Err(_) => begins.push("Err"),
        }
    }
    let gate = match begun.first_mut() {
        Some(txn) => format!(
            "{:?}",
            txns.take_exclusive(txn, &crate::io::Completion::new_wait())
        ),
        None => "no transaction".to_string(),
    };
    for txn in begun {
        txns.abort(txn);
    }
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((inner, writer, other, db));
    drop(io);
    let in_flight = crash.in_flight();
    assert_eq!(setup, Ok(()));
    assert!(
        other_failed,
        "the log did not fail under the other connection's commit"
    );
    assert!(
        ended.is_err() && !published,
        "the COMMIT did not fail before publishing: {ended:?}, published {published}"
    );
    assert_eq!(
        failure, None,
        "a commit rolled back whole failed the database"
    );
    assert_eq!(
        begins, ["Done"; 4],
        "the failed commit's transaction kept its slot"
    );
    assert_eq!(
        gate, "Ok(Done)",
        "the failed commit's transaction kept the gate"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
    assert_eq!(in_flight, 0);
}

/// A read-only transaction's `COMMIT` meeting a failed log: the commit ends its transaction at
/// once, its slot freed, and only then waits for the log, so the failure it returns leaves nothing
/// to roll back: every slot is free and the database has not failed (the auditor's #4479: a commit
/// not published has not always kept its transaction).
#[test]
fn a_read_only_commit_meeting_a_failed_log_has_nothing_to_roll_back() {
    use crate::storage::atoll::txn::Isolation;
    use crate::types::IOResult;
    let name = "/db/read-only-commit-failed-log";
    let crash = Arc::new(crate::io::crash::CrashIO::new(678));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, name);
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    let setup = ["CREATE TABLE t(a INTEGER)", "INSERT INTO t VALUES (1)"]
        .iter()
        .try_for_each(|sql| ran_on(&reader, &crash, sql));
    // The log fails under another connection's commit, past what is durable.
    fail_the_log(&crash, name);
    let other_failed = ran_on(&other, &crash, "INSERT INTO t VALUES (2)").is_err();
    // A read transaction whose snapshot stands past what the log made durable.
    let read = ["BEGIN", "SELECT a FROM t"]
        .iter()
        .try_for_each(|sql| ran_on(&reader, &crash, sql));
    let mut commit = reader.prepare("COMMIT").unwrap();
    let (ended, published) = step_to_end_published(&mut commit, &crash);
    drop(commit);
    clear_faults(&crash);
    let inner = reader.atoll().unwrap().db.clone();
    let txns = inner.atoll().transactions();
    let failure = txns.failure().map(|e| e.to_string());
    let mut begun = Vec::new();
    for _ in 0..txns.slot_count() {
        if let Ok(IOResult::Done(txn)) = txns.begin(Isolation::Snapshot) {
            begun.push(txn);
        }
    }
    let free = begun.len();
    for txn in begun {
        txns.abort(txn);
    }
    let state = (reader.get_auto_commit(), reader.get_tx_state());
    drop((inner, reader, other, db));
    drop(io);
    let in_flight = crash.in_flight();
    assert_eq!(setup, Ok(()));
    assert!(
        other_failed,
        "the log did not fail under the other connection's commit"
    );
    assert_eq!(read, Ok(()));
    assert!(
        ended.is_err() && !published,
        "the read-only COMMIT did not meet the failed log: {ended:?}"
    );
    assert_eq!(failure, None, "a read-only commit failed the database");
    assert_eq!(free, 4, "the read-only commit's transaction kept its slot");
    assert_eq!(state, (true, crate::connection::TransactionState::None));
    assert_eq!(in_flight, 0);
}

/// A `COMMIT` whose log fails before it publishes, its transaction having spilled parts the log
/// never made durable: its rollback cannot read them back, so it leaves the transaction where it
/// stands, slot, gate and versions, and the database fails until it is reopened; whoever waited on
/// it is woken to the failure, a writer on one of its keys, a begin on the slots and an exclusive
/// take behind its hold, and a begin finds it even with a slot free. The reopened database has
/// none of the transaction's rows (the auditor's #4468, #4466: no slot freed under its versions).
/// Ended by the statement's drop, or by its reset if `reset`.
fn a_commit_whose_rollback_cannot_read_its_parts(seed: u64, reset: bool) {
    use crate::storage::atoll::txn::Isolation;
    use crate::types::IOResult;
    let name = format!(
        "/db/commit-unreadable-rollback{}",
        if reset { "-reset" } else { "" }
    );
    let crash = Arc::new(crate::io::crash::CrashIO::new(seed));
    let io: Arc<dyn IO> = crash.clone();
    let mut cfg = config();
    cfg.log.buffer_bytes = 32 << 10;
    cfg.log.chunk_bytes = 8 << 10;
    cfg.log_budget = Some(16 << 20);
    let db = open_with(&io, &name, cfg.clone());
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    let values = (0..100)
        .map(|n| format!("({n}, zeroblob(2048))"))
        .collect::<Vec<_>>()
        .join(", ");
    let setup = [
        "CREATE TABLE data(id INTEGER PRIMARY KEY, payload BLOB)".to_string(),
        "BEGIN".to_string(),
        format!("INSERT INTO data VALUES {values}"),
    ]
    .iter()
    .try_for_each(|sql| ran_on(&writer, &crash, sql));
    // Another connection's write waits on one of the transaction's keys, its waits unbounded.
    other.atoll().unwrap().park_waits();
    let began = ran_on(&other, &crash, "BEGIN");
    let mut blocked = other.prepare("INSERT INTO data VALUES (5, x'01')").unwrap();
    let blocked_step = format!("{:?}", blocked.step());
    let inner = writer.atoll().unwrap().db.clone();
    let txns = inner.atoll().transactions();
    // The other slots taken, a begin waits for one, and an exclusive take waits behind the holds.
    let mut held: Vec<_> = (0..2)
        .filter_map(|_| match txns.begin(Isolation::Snapshot) {
            Ok(IOResult::Done(txn)) => Some(txn),
            _ => None,
        })
        .collect();
    let slot_wait = match txns.begin(Isolation::Snapshot) {
        Ok(IOResult::IO(crate::types::IOCompletions(c))) => Some(c),
        _ => None,
    };
    let exclusive_wait = crate::io::Completion::new_wait();
    let behind = held
        .first_mut()
        .map(|txn| format!("{:?}", txns.take_exclusive(txn, &exclusive_wait)));
    // The spilled parts' writes fail: the log has failed with the transaction still open.
    fail_the_log(&crash, &name);
    let mut wait = crate::storage::atoll::log::LogWait::default();
    drop(
        inner
            .atoll()
            .log()
            .wait_durable(&mut wait, inner.atoll().log().end()),
    );
    drop(wait);
    while crash.in_flight() > 0 {
        if crash.step().is_err() {
            break;
        }
    }
    let log_failed = inner.atoll().log().failure().is_some();
    let mut commit = writer.prepare("COMMIT").unwrap();
    let (ended, published) = step_to_end_published(&mut commit, &crash);
    if reset {
        let _ = commit.reset();
    }
    drop(commit);
    clear_faults(&crash);
    let failure = txns.failure().map(|e| e.to_string());
    let slot_woken = slot_wait.as_ref().map(|c| c.finished());
    let exclusive_woken = exclusive_wait.finished();
    let retake = held.first_mut().map(|txn| {
        txns.take_exclusive(txn, &crate::io::Completion::new_wait())
            .is_err()
    });
    let blocked_after = match blocked.step() {
        Err(e) => Ok(e.to_string()),
        other => Err(format!("{other:?}")),
    };
    drop(blocked);
    // A slot freed: a begin still finds the failure.
    if let Some(txn) = held.pop() {
        txns.abort(txn);
    }
    let begin_after = match txns.begin(Isolation::Snapshot) {
        Err(e) => Ok(e.to_string()),
        Ok(IOResult::Done(txn)) => {
            txns.abort(txn);
            Err("Done".to_string())
        }
        Ok(IOResult::IO(_)) => Err("IO".to_string()),
    };
    for txn in held {
        txns.abort(txn);
    }
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((inner, writer, other, db));
    let in_flight = crash.in_flight();
    let reopened = open_with(&io, &name, cfg);
    let rows = reopened
        .connect(ConnectOpts::new())
        .and_then(|conn| {
            conn.prepare("SELECT count(*) FROM data")
                .and_then(|mut statement| statement.run_collect_rows())
        })
        .map_err(|e| e.to_string());
    drop(reopened);
    drop(io);
    assert_eq!(setup, Ok(()));
    assert_eq!(began, Ok(()));
    assert_eq!(
        blocked_step, "Ok(IO)",
        "the other connection's write did not wait on the key"
    );
    assert!(slot_wait.is_some(), "no begin waited for a slot");
    assert!(
        behind
            .as_deref()
            .is_some_and(|b| b.starts_with("Ok(Holders(")),
        "the exclusive take did not wait behind the holds: {behind:?}"
    );
    assert!(log_failed, "the log did not fail before the COMMIT");
    assert!(
        ended.is_err() && !published,
        "the COMMIT did not fail before publishing: {ended:?}, published {published}"
    );
    assert!(
        failure.is_some(),
        "an unfinished rollback did not fail the database"
    );
    assert_eq!(
        slot_woken,
        Some(true),
        "a begin waiting for a slot was not woken"
    );
    assert!(
        exclusive_woken,
        "an exclusive take waiting behind the holds was not woken"
    );
    assert_eq!(
        retake,
        Some(true),
        "an exclusive take on a failed database did not fail"
    );
    assert!(
        blocked_after.is_ok(),
        "a write waiting on the left transaction's key did not fail: {blocked_after:?}"
    );
    assert!(
        begin_after.is_ok(),
        "a begin on a failed database with a slot free did not fail: {begin_after:?}"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
    assert_eq!(in_flight, 0);
    assert_eq!(
        rows,
        Ok(vec![vec![Value::from_i64(0)]]),
        "the reopened database kept rows of the left transaction"
    );
}

#[test]
fn a_commit_whose_rollback_cannot_read_its_parts_fails_the_database_and_wakes_its_waiters() {
    a_commit_whose_rollback_cannot_read_its_parts(676, false);
}

#[test]
fn a_commit_whose_rollback_cannot_read_its_parts_reset_fails_the_database_the_same() {
    a_commit_whose_rollback_cannot_read_its_parts(677, true);
}

/// The end of a connection's transaction is one word that a close and an ending's start take by
/// compare-and-swap, so they exclude each other on any threads: an ending holding it refuses a
/// close, which goes ahead once the ending lets go, and a second ending; a close holding it keeps an
/// ending from beginning.
#[test]
fn a_close_and_an_endings_start_exclude_each_other() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(664));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, "/db/end-word");
    let ending_first = db.connect(ConnectOpts::new()).unwrap();
    let closed_first = db.connect(ConnectOpts::new()).unwrap();
    let ending = crate::vdbe::EndingBegun::new(&ending_first).map_err(|e| e.to_string());
    let second_refused = matches!(
        crate::vdbe::EndingBegun::new(&ending_first),
        Err(crate::LimboError::StatementsInProgress(_))
    );
    let refused = matches!(
        ending_first.close(),
        Err(crate::LimboError::StatementsInProgress(_))
    );
    let open_after_refusal = !ending_first.is_closed();
    drop(ending);
    let closed = ending_first.close().map_err(|e| e.to_string());
    let closing = closed_first.close().map_err(|e| e.to_string());
    let begun = crate::vdbe::EndingBegun::new(&closed_first)
        .map(drop)
        .map_err(|e| e.to_string());
    drop((ending_first, closed_first, db));
    assert!(
        second_refused,
        "a second ending began while another held the end"
    );
    assert!(refused, "a close went ahead while an ending held the end");
    assert!(open_after_refusal, "a refused close closed its connection");
    assert_eq!(closed, Ok(()));
    assert_eq!(closing, Ok(()));
    assert_eq!(
        begun,
        Err("Internal error: Connection closed".to_string()),
        "an ending began on a connection a close held"
    );
}

/// A scan stepping its connection's paused `COMMIT` after the log failed under another
/// connection's commit: the commit fails before it publishes, and the failure is the `COMMIT`'s,
/// never the scan's (DESIGN.md §17). Small, the commit's rollback undoes it whole in the scan's step
/// and the scan reads on, every slot free and the database not failed; with parts the log never
/// made durable (`spilled`), the rollback cannot read them back and the database fails, so the
/// scan's read on finds the database's failure. Either way the `COMMIT` returns the failure and none
/// of its rows is there (with the commit's failure only turned to a rollback, the scan's step
/// returned it and the `COMMIT` then found the rollback done and returned success).
fn a_sibling_meeting_its_connections_commit_failure(seed: u64, spilled: bool) {
    use crate::storage::atoll::txn::Isolation;
    use crate::types::IOResult;
    let name = format!(
        "/db/sibling-meets-commit-failure{}",
        if spilled { "-spilled" } else { "" }
    );
    let crash = Arc::new(crate::io::crash::CrashIO::new(seed));
    let io: Arc<dyn IO> = crash.clone();
    let mut cfg = config();
    if spilled {
        cfg.log.buffer_bytes = 32 << 10;
        cfg.log.chunk_bytes = 8 << 10;
        cfg.log_budget = Some(16 << 20);
    }
    let db = open_with(&io, &name, cfg.clone());
    let writer = db.connect(ConnectOpts::new()).unwrap();
    let other = db.connect(ConnectOpts::new()).unwrap();
    let values = (0..if spilled { 100 } else { 1 })
        .map(|n| format!("({n}, zeroblob(2048))"))
        .collect::<Vec<_>>()
        .join(", ");
    let setup = [
        "CREATE TABLE scanned(a INTEGER)".to_string(),
        "INSERT INTO scanned VALUES (10), (20), (30)".to_string(),
        "CREATE TABLE data(id INTEGER PRIMARY KEY, payload BLOB)".to_string(),
        "CREATE TABLE side(b INTEGER)".to_string(),
        "BEGIN".to_string(),
        format!("INSERT INTO data VALUES {values}"),
    ]
    .iter()
    .try_for_each(|sql| ran_on(&writer, &crash, sql));
    let mut scan = writer.prepare("SELECT a FROM scanned").unwrap();
    let first = scan_first(&mut scan, &crash);
    // The log fails under another connection's commit, with the transaction's last parts.
    fail_the_log(&crash, &name);
    let other_failed = ran_on(&other, &crash, "INSERT INTO side VALUES (1)").is_err();
    // The `COMMIT` pauses in its ending before it publishes, and the scan steps it on.
    let pause = crate::io::Completion::new_wait();
    crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = Some(pause.clone()));
    let mut commit = writer.prepare("COMMIT").unwrap();
    let waited = step_to_ending(&mut commit, &crash, false);
    pause.complete(0);
    crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = None);
    let mut read = Vec::new();
    let scanned = scan_rest(&mut scan, &crash, &mut read);
    drop(scan);
    let ended = step_to_end(&mut commit, &crash);
    drop(commit);
    clear_faults(&crash);
    let inner = writer.atoll().unwrap().db.clone();
    let txns = inner.atoll().transactions();
    let failure = txns.failure().map(|e| e.to_string());
    let mut begun = Vec::new();
    for _ in 0..txns.slot_count() {
        if let Ok(IOResult::Done(txn)) = txns.begin(Isolation::Snapshot) {
            begun.push(txn);
        }
    }
    let free = begun.len();
    for txn in begun {
        txns.abort(txn);
    }
    let state = (writer.get_auto_commit(), writer.get_tx_state());
    drop((inner, writer, other, db));
    let in_flight = crash.in_flight();
    let reopened = open_with(&io, &name, cfg);
    let rows = reopened
        .connect(ConnectOpts::new())
        .and_then(|conn| {
            conn.prepare("SELECT count(*) FROM data")
                .and_then(|mut statement| statement.run_collect_rows())
        })
        .map_err(|e| e.to_string());
    drop(reopened);
    drop(io);
    assert_eq!(setup, Ok(()));
    assert_eq!(first, Ok(vec![Value::from_i64(10)]));
    assert!(
        other_failed,
        "the log did not fail under the other connection's commit"
    );
    assert_eq!(waited, Ok(()));
    if spilled {
        assert!(
            scanned.is_err(),
            "the scan read on in a failed database: {read:?}"
        );
        assert!(failure.is_some(), "the commit's rollback finished");
        assert_eq!(free, 0, "a begin went ahead in a failed database");
    } else {
        assert_eq!(scanned, Ok(()), "the scan met its connection's failure");
        assert_eq!(
            read,
            vec![vec![Value::from_i64(20)], vec![Value::from_i64(30)]]
        );
        assert_eq!(failure, None, "an undone commit failed the database");
        assert_eq!(free, 4, "the failed commit's transaction kept its slot");
    }
    assert!(
        ended.is_err(),
        "the COMMIT did not return its commit's failure: {ended:?}"
    );
    assert_eq!(state, (true, crate::connection::TransactionState::None));
    assert_eq!(in_flight, 0);
    assert_eq!(rows, Ok(vec![vec![Value::from_i64(0)]]));
}

#[test]
fn a_sibling_meeting_its_connections_commit_failure_leaves_it_to_the_commit() {
    a_sibling_meeting_its_connections_commit_failure(682, false);
}

#[test]
fn a_sibling_meeting_its_connections_commit_failure_with_unreadable_parts_leaves_it_to_it() {
    a_sibling_meeting_its_connections_commit_failure(683, true);
}

/// The log's IO failing in the call that issues it, as a file reports a failure: refused at
/// submission, or delivered failed on the completion handed back.
#[cfg(not(any(shuttle, atoll_loom, miri)))]
mod log_failing_in_the_call {
    use super::*;
    use crate::io::{Clock, Completion, File, FileSyncType, ReadyKey, Stepped};
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    #[repr(u8)]
    enum Fault {
        None = 0,
        Refused = 1,
        Delivered = 2,
    }

    /// What the log files' writes and syncs meet, and how many met a fault.
    #[derive(Default)]
    struct Faulting {
        fault: AtomicU8,
        hits: AtomicUsize,
        db: std::sync::OnceLock<std::sync::Weak<Database>>,
    }

    impl Faulting {
        /// The fault for a log file's write or sync, once one is set and the database's schema
        /// holds `kept`, which the commit's publication puts there.
        fn meet(&self, c: &Completion) -> Option<crate::Result<Completion>> {
            let fault = self.fault.load(Ordering::Acquire);
            if fault == Fault::None as u8 {
                return None;
            }
            let published = self
                .db
                .get()
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|db| {
                    db.clone_schema()
                        .get_table(crate::namespace::NamespaceId::ROOT, "kept")
                        .is_some()
                });
            if !published {
                return None;
            }
            self.hits.fetch_add(1, Ordering::AcqRel);
            let e = crate::error::CompletionError::IOError(
                std::io::ErrorKind::Other,
                "a log IO failing in its call",
            );
            Some(if fault == Fault::Refused as u8 {
                Err(crate::LimboError::CompletionError(e))
            } else {
                c.deliver(Err(e));
                Ok(c.clone())
            })
        }
    }

    struct FaultingIO {
        inner: Arc<crate::io::crash::CrashIO>,
        faulting: Arc<Faulting>,
    }

    impl Clock for FaultingIO {
        fn current_time_monotonic(&self) -> crate::MonotonicInstant {
            self.inner.current_time_monotonic()
        }
        fn current_time_wall_clock(&self) -> crate::WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }

    impl IO for FaultingIO {
        fn open_file(
            &self,
            path: &str,
            flags: OpenFlags,
            direct: bool,
        ) -> crate::Result<Arc<dyn File>> {
            Ok(Arc::new(FaultingFile {
                inner: self.inner.open_file(path, flags, direct)?,
                faulting: path.contains(".log.").then(|| self.faulting.clone()),
            }))
        }
        fn remove_file(&self, path: &str) -> crate::Result<()> {
            self.inner.remove_file(path)
        }
        fn sync_directory(&self, path: &str) -> crate::Result<()> {
            self.inner.sync_directory(path)
        }
        fn rename_file(&self, from: &str, to: &str) -> crate::Result<bool> {
            self.inner.rename_file(from, to)
        }
        fn file_id(&self, path: &str) -> crate::Result<crate::io::FileId> {
            self.inner.file_id(path)
        }
        fn step(&self) -> crate::Result<()> {
            self.inner.step()
        }
        fn try_step(&self, key: ReadyKey, ready: &std::task::Waker) -> crate::Result<Stepped> {
            self.inner.try_step(key, ready)
        }
        fn says_what_a_step_found(&self) -> bool {
            self.inner.says_what_a_step_found()
        }
        fn withdraw(&self, key: ReadyKey) {
            self.inner.withdraw(key)
        }
    }

    struct FaultingFile {
        inner: Arc<dyn File>,
        /// For a log file.
        faulting: Option<Arc<Faulting>>,
    }

    impl FaultingFile {
        fn meet(&self, c: &Completion) -> Option<crate::Result<Completion>> {
            self.faulting.as_ref().and_then(|f| f.meet(c))
        }
    }

    impl File for FaultingFile {
        fn lock_file(&self, exclusive: bool) -> crate::Result<()> {
            self.inner.lock_file(exclusive)
        }
        fn unlock_file(&self) -> crate::Result<()> {
            self.inner.unlock_file()
        }
        fn pread(&self, pos: u64, c: Completion) -> crate::Result<Completion> {
            self.inner.pread(pos, c)
        }
        fn pwrite(
            &self,
            pos: u64,
            b: Arc<crate::Buffer>,
            c: Completion,
        ) -> crate::Result<Completion> {
            match self.meet(&c) {
                Some(met) => met,
                None => self.inner.pwrite(pos, b, c),
            }
        }
        fn sync(&self, c: Completion, kind: FileSyncType) -> crate::Result<Completion> {
            match self.meet(&c) {
                Some(met) => met,
                None => self.inner.sync(c, kind),
            }
        }
        fn allocate(&self, pos: u64, len: u64) -> crate::Result<()> {
            self.inner.allocate(pos, len)
        }
        fn size(&self) -> crate::Result<u64> {
            self.inner.size()
        }
        fn truncate(&self, len: u64, c: Completion) -> crate::Result<Completion> {
            self.inner.truncate(len, c)
        }
    }

    /// A scan stepping its connection's paused `COMMIT` on to its publication, the log's next
    /// write or sync then failing in the call that issues it (`fault`): the scan steps the commit
    /// to its transaction's end and no further, never into its wait for durability, so it reads
    /// on having issued no IO of the log; the `COMMIT` meets the failure in its own wait and
    /// returns it, its table published and the attached database's row committed (the auditor's
    /// #4481: the scan's step went on into the commit's wait for durability, met the failure there
    /// and returned it).
    fn a_sibling_publishing_its_connections_commit(seed: u64, fault: Fault) {
        let name = format!("sibling-publishing-commit-{fault:?}");
        let crash = Arc::new(crate::io::crash::CrashIO::new(seed));
        let faulting = Arc::new(Faulting::default());
        let io: Arc<dyn IO> = Arc::new(FaultingIO {
            inner: crash.clone(),
            faulting: faulting.clone(),
        });
        let db = open_attaching(&io, &name);
        let _ = faulting.db.set(Arc::downgrade(&db));
        let writer = db.connect(ConnectOpts::new()).unwrap();
        let reader = db.connect(ConnectOpts::new()).unwrap();
        let setup = [
            "CREATE TABLE scanned(a INTEGER)".to_string(),
            "INSERT INTO scanned VALUES (10), (20), (30)".to_string(),
            format!("ATTACH '/db/{name}-aux' AS aux"),
            "CREATE TABLE aux.side(b INTEGER)".to_string(),
            "BEGIN".to_string(),
            "CREATE TABLE kept(a INTEGER)".to_string(),
            "INSERT INTO aux.side VALUES (1)".to_string(),
        ]
        .iter()
        .try_for_each(|sql| ran_on(&writer, &crash, sql));
        let mut scan = writer.prepare("SELECT a FROM scanned").unwrap();
        let first = scan_first(&mut scan, &crash);
        let pause = Completion::new_wait();
        crate::storage::atoll::testing::ENDING_PAUSE
            .with(|p| *p.borrow_mut() = Some(pause.clone()));
        let mut commit = writer.prepare("COMMIT").unwrap();
        let waited = step_to_ending(&mut commit, &crash, false);
        let failed_before = writer.atoll().unwrap().db.atoll().log().failure().is_some();
        pause.complete(0);
        crate::storage::atoll::testing::ENDING_PAUSE.with(|p| *p.borrow_mut() = None);
        faulting.fault.store(fault as u8, Ordering::Release);
        let stepped = format!("{:?}", scan.step());
        let published = commit.atoll_ending_published();
        let hits_in_scan = faulting.hits.load(Ordering::Acquire);
        drop(scan);
        let ended = step_to_end(&mut commit, &crash);
        drop(commit);
        let hits = faulting.hits.load(Ordering::Acquire);
        faulting.fault.store(Fault::None as u8, Ordering::Release);
        let failed_after = writer.atoll().unwrap().db.atoll().log().failure().is_some();
        let table = sees(&reader, "kept");
        let state = (writer.get_auto_commit(), writer.get_tx_state());
        drop((writer, reader, db));
        let side = side_rows(&io, &name);
        drop(io);
        let in_flight = crash.in_flight();
        let faulted = fault != Fault::None;
        assert_eq!(setup, Ok(()));
        assert_eq!(first, Ok(vec![Value::from_i64(10)]));
        assert_eq!(waited, Ok(()));
        assert!(!failed_before, "the log failed before the commit published");
        assert!(published, "the scan's step did not publish the commit");
        assert_eq!(
            (stepped.as_str(), hits_in_scan),
            ("Ok(Row)", 0),
            "the scan stepped the commit into its wait for durability"
        );
        assert_eq!(hits > 0, faulted, "the COMMIT's wait met no fault");
        assert_eq!(failed_after, faulted);
        assert_eq!(
            ended.is_err(),
            faulted,
            "the COMMIT's outcome is not its wait's: {ended:?}"
        );
        assert_eq!(table, Ok(()));
        assert_eq!(side, Ok(vec![vec![Value::from_i64(1)]]));
        assert_eq!(state, (true, crate::connection::TransactionState::None));
        assert_eq!(in_flight, 0);
    }

    #[test]
    fn a_sibling_publishing_its_connections_commit_issues_none_of_its_log_io() {
        a_sibling_publishing_its_connections_commit(679, Fault::None);
    }

    #[test]
    fn a_sibling_publishing_its_connections_commit_issues_none_of_its_log_io_refused() {
        a_sibling_publishing_its_connections_commit(680, Fault::Refused);
    }

    #[test]
    fn a_sibling_publishing_its_connections_commit_issues_none_of_its_log_io_failed_inline() {
        a_sibling_publishing_its_connections_commit(681, Fault::Delivered);
    }
}

/// A rollback's ending dropped before anything stepped it, its destructor the first and only
/// driver, the tree's root evicted so its undo yields a read, with an exclusive take queued behind
/// its hold of the gate (the auditor's #4485/#4486 probe). If `fail`, the reads fail, the log
/// staying healthy: the driver sees the read's completion fail rather than the rollback's step, and
/// the ending takes the failure as that step would have, leaving the transaction to
/// `Transactions::abandon`: the database failed, the queued take woken to the failure and every
/// begin refused (before, the transaction was dropped with its slot and gate held, the take asleep
/// and nothing failed). Otherwise the rollback lets its slot and gate go. With `other`, a second
/// transaction writing a key of its own is open beside it: on a failed database its commit is
/// refused and it rolls back (the auditor's #4484), and otherwise it commits.
fn a_rollback_ending_dropped_unstepped(fail: bool, other: bool) {
    use crate::storage::atoll::btree::Mode;
    use crate::storage::atoll::page::SizeClass;
    use crate::storage::atoll::sql::AtollDb;
    use crate::storage::atoll::testing::drive;
    use crate::storage::atoll::txn::versioned::{VersionedTree, Write, WriteOp};
    use crate::storage::atoll::txn::{Abort, Blocker, Commit, Isolation, Txn};
    use crate::types::{IOResult, ImmutableRecord};
    let observed = (|| -> std::result::Result<_, String> {
        let crash = Arc::new(crate::io::crash::CrashIO::new(684));
        let io: Arc<dyn IO> = crash.clone();
        let path = format!("/db/rollback-dropped-unstepped-{fail}-{other}");
        let db = Arc::new(AtollDb::open(io.clone(), &path, config()).map_err(|e| e.to_string())?);
        let txns = db.atoll().transactions();
        let begin = || -> std::result::Result<Txn, String> {
            let mut txn =
                drive(&io, || txns.begin(Isolation::Snapshot)).map_err(|e| e.to_string())?;
            crate::storage::atoll::testing::share(txns, &mut txn);
            txns.statement(&mut txn).map_err(|e| e.to_string())?;
            Ok(txn)
        };
        let record = ImmutableRecord::from_values(&[Value::build_text("v".repeat(100))], 1)
            .map_err(|e| e.to_string())?;
        let write =
            |tree: &VersionedTree, txn: &mut Txn, rowid: i64| -> std::result::Result<(), String> {
                let key = crate::storage::atoll::sql::key::rowid(rowid);
                let mut op = WriteOp::put(&key, record.get_payload().to_vec(), Mode::Upsert);
                match drive(&io, || tree.write(txns, db.versions(), txn, &mut op)) {
                    Ok(Write::Done(_)) => Ok(()),
                    other => Err(format!("write: {other:?}")),
                }
            };
        let mut txn = begin()?;
        let root = drive(&io, || db.create_tree(&mut txn, SizeClass::new(0).unwrap()))
            .map_err(|e| e.to_string())?;
        let tree = VersionedTree::open(
            io.clone(),
            db.atoll().pool().clone(),
            db.atoll().store().clone(),
            root,
        );
        for rowid in 0..300 {
            write(&tree, &mut txn, rowid)?;
        }
        let mut commit = Commit::new(txn);
        drive(&io, || txns.commit(&mut commit)).map_err(|e| e.to_string())?;
        drop(commit);
        let mut txn = begin()?;
        write(&tree, &mut txn, 150)?;
        let blocker = Blocker {
            slot: txn.slot(),
            start: txn.start(),
        };
        let mut second = match other {
            true => {
                let mut second = begin()?;
                write(&tree, &mut second, 400)?;
                Some(second)
            }
            false => None,
        };
        super::full_checkpoint(&db).map_err(|e| e.to_string())?;
        let pool = db.atoll().pool().clone();
        let mut sweep = pool.sweep();
        drive(&io, || sweep.step(&pool)).map_err(|e| e.to_string())?;
        drop(sweep);
        let evicted = pool.evict_clean(root);
        let mut waiter =
            drive(&io, || txns.begin(Isolation::Snapshot)).map_err(|e| e.to_string())?;
        let waiting = crate::io::Completion::new_wait();
        let queued = format!("{:?}", txns.take_exclusive(&mut waiter, &waiting));
        let before = crash.completed();
        if fail {
            crash.set_faults(crate::io::crash::Faults {
                read: 1.0,
                write: 0.0,
                sync: 0.0,
                short_write: 0.0,
            });
        }
        // Its first and only driver is its destructor.
        drop(super::Ending::rollback(
            db.clone(),
            crate::storage::atoll::buffer::Conn::new(),
            txn,
        ));
        let in_drop = crash.completed() - before;
        crash.set_faults(crate::io::crash::Faults::default());
        let log_failed = db.atoll().log().failure().is_some();
        let failure = txns.failure().map(|e| e.to_string());
        // The second transaction, open when the database failed or not, ends.
        let second_ended = second.take().map(|txn| {
            let mut commit = Commit::new(txn);
            let committed = drive(&io, || txns.commit(&mut commit)).map(|ts| ts.is_some());
            let published = commit.published();
            let rolled_back = commit.take_txn().map(|txn| {
                let mut abort = Abort::new(txn);
                drive(&io, || txns.rollback(db.versions(), &mut abort)).is_ok()
            });
            (committed.map_err(|e| e.to_string()), published, rolled_back)
        });
        let held_after = txns.holds_shared(blocker);
        let woken = waiting.finished();
        let gate_after = format!(
            "{:?}",
            txns.take_exclusive(&mut waiter, &crate::io::Completion::new_wait())
        );
        txns.give_up(&mut waiter);
        txns.abort(waiter);
        let mut begun = Vec::new();
        let mut begins = Vec::new();
        for _ in 0..txns.slot_count() {
            match txns.begin(Isolation::Snapshot) {
                Ok(IOResult::Done(txn)) => {
                    begins.push("Done".to_string());
                    begun.push(txn);
                }
                Ok(IOResult::IO(_)) => begins.push("IO".to_string()),
                Err(e) => begins.push(format!("Err({e})")),
            }
        }
        for txn in begun {
            txns.abort(txn);
        }
        drop((waiting, pool, tree, db, io));
        let in_flight = crash.in_flight();
        Ok((
            evicted,
            queued,
            in_drop,
            log_failed,
            failure,
            second_ended,
            held_after,
            woken,
            gate_after,
            begins,
            in_flight,
        ))
    })();
    let (
        evicted,
        queued,
        in_drop,
        log_failed,
        failure,
        second_ended,
        held_after,
        woken,
        gate_after,
        begins,
        in_flight,
    ) = observed.expect("the fixture failed, every database dropped");
    assert!(evicted, "the tree's root was not evicted");
    assert!(
        queued.starts_with("Ok(Holders("),
        "the exclusive take did not queue behind the hold: {queued}"
    );
    assert!(in_drop > 0, "the destructor's rollback issued no IO");
    assert!(!log_failed, "the log failed: the reads alone were to fail");
    assert_eq!(in_flight, 0);
    if fail {
        assert!(
            failure.is_some(),
            "the rollback whose read failed left the database not failed"
        );
        assert!(woken, "the queued exclusive take was not woken");
        assert!(
            gate_after.starts_with("Err("),
            "an exclusive take went ahead on a failed database: {gate_after}"
        );
        assert!(
            begins.iter().all(|b| b.starts_with("Err(")),
            "a begin went ahead on a failed database: {begins:?}"
        );
        if other {
            assert_eq!(
                second_ended,
                Some((Err(failure.clone().unwrap()), false, Some(true))),
                "a transaction open when the database failed committed"
            );
        }
    } else {
        assert_eq!(failure, None);
        assert!(!held_after && woken, "the rollback kept its hold");
        assert_eq!(gate_after, "Ok(Done)");
        assert_eq!(begins, ["Done"; 4]);
        if other {
            assert_eq!(second_ended, Some((Ok(true), true, None)));
        }
    }
}

#[test]
fn a_rollback_ending_dropped_unstepped_lets_its_slot_and_gate_go() {
    a_rollback_ending_dropped_unstepped(false, false);
}

#[test]
fn a_rollback_ending_dropped_unstepped_whose_read_fails_fails_the_database() {
    a_rollback_ending_dropped_unstepped(true, false);
}

#[test]
fn a_rollback_ending_dropped_unstepped_beside_a_writer_lets_the_writer_commit() {
    a_rollback_ending_dropped_unstepped(false, true);
}

#[test]
fn a_writer_open_when_the_database_fails_cannot_commit() {
    a_rollback_ending_dropped_unstepped(true, true);
}

/// A commit told of a failure its driver saw before its transaction ended (`Ending::met`), as
/// `Log::fail` tells a commit waiting for the log's room by failing its admission: the ending rolls
/// the transaction back, its slot and its hold of the gate let go and the database not failed, and
/// its outcome is the failure.
#[test]
fn a_commit_told_of_its_drivers_failure_before_it_ended_rolls_back() {
    use crate::storage::atoll::btree::Mode;
    use crate::storage::atoll::page::SizeClass;
    use crate::storage::atoll::sql::AtollDb;
    use crate::storage::atoll::testing::drive;
    use crate::storage::atoll::txn::versioned::{VersionedTree, Write, WriteOp};
    use crate::storage::atoll::txn::{Blocker, Commit, Isolation, Txn};
    use crate::types::{IOResult, ImmutableRecord};
    let observed = (|| -> std::result::Result<_, String> {
        let crash = Arc::new(crate::io::crash::CrashIO::new(686));
        let io: Arc<dyn IO> = crash.clone();
        let db = Arc::new(
            AtollDb::open(io.clone(), "/db/commit-told-of-a-failure", config())
                .map_err(|e| e.to_string())?,
        );
        let txns = db.atoll().transactions();
        let begin = || -> std::result::Result<Txn, String> {
            let mut txn =
                drive(&io, || txns.begin(Isolation::Snapshot)).map_err(|e| e.to_string())?;
            crate::storage::atoll::testing::share(txns, &mut txn);
            txns.statement(&mut txn).map_err(|e| e.to_string())?;
            Ok(txn)
        };
        let record = ImmutableRecord::from_values(&[Value::build_text("v".repeat(100))], 1)
            .map_err(|e| e.to_string())?;
        let mut txn = begin()?;
        let root = drive(&io, || db.create_tree(&mut txn, SizeClass::new(0).unwrap()))
            .map_err(|e| e.to_string())?;
        let mut commit = Commit::new(txn);
        drive(&io, || txns.commit(&mut commit)).map_err(|e| e.to_string())?;
        drop(commit);
        let tree = VersionedTree::open(
            io.clone(),
            db.atoll().pool().clone(),
            db.atoll().store().clone(),
            root,
        );
        let mut txn = begin()?;
        let key = crate::storage::atoll::sql::key::rowid(1);
        let mut op = WriteOp::put(&key, record.get_payload().to_vec(), Mode::Upsert);
        match drive(&io, || tree.write(txns, db.versions(), &mut txn, &mut op)) {
            Ok(Write::Done(_)) => {}
            other => return Err(format!("write: {other:?}")),
        }
        let blocker = Blocker {
            slot: txn.slot(),
            start: txn.start(),
        };
        let mut ending = super::Ending::commit(
            db.clone(),
            crate::storage::atoll::buffer::Conn::new(),
            txn,
            Vec::new(),
            Vec::new(),
            None,
        );
        ending.met(crate::LimboError::InternalError(
            "the log failed under an admission".to_string(),
        ));
        let ended = ending.finish().map_err(|e| e.to_string());
        drop(ending);
        let held = txns.holds_shared(blocker);
        let failure = txns.failure().map(|e| e.to_string());
        let mut begun = Vec::new();
        for _ in 0..txns.slot_count() {
            if let Ok(IOResult::Done(txn)) = txns.begin(Isolation::Snapshot) {
                begun.push(txn);
            }
        }
        let free = begun.len();
        for txn in begun {
            txns.abort(txn);
        }
        drop((tree, db, io));
        let in_flight = crash.in_flight();
        Ok((ended, held, failure, free, in_flight))
    })();
    let (ended, held, failure, free, in_flight) =
        observed.expect("the fixture failed, every database dropped");
    assert!(
        ended
            .as_ref()
            .is_err_and(|e| e.contains("the log failed under an admission")),
        "the commit's outcome is not the failure it was told: {ended:?}"
    );
    assert!(!held, "the told commit kept its hold of the gate");
    assert_eq!(free, 4, "the told commit kept its transaction's slot");
    assert_eq!(
        failure, None,
        "a commit undone in memory failed the database"
    );
    assert_eq!(in_flight, 0);
}

/// A commit waiting for the log's room, driven on by its ending's own driver (`Ending::finish`, or
/// its drop) as the first to wait on the admission: the admission registered and flushed what the
/// log held unflushed (a change published alone), and that write failing fails the log, which fails
/// the admitters (`Log::fail`). The failure reaches the driver as its wait's completion, which
/// tells the commit (`Ending::met`), and the commit, its transaction not ended, rolls it back: its
/// slot and its hold of the gate let go, the database not failed, the failure the outcome (the
/// auditor's #4499: the admission's own schedule, where the last slice's test told the ending
/// directly). A statement's drop is no such driver: its reset waits on the statement's IO first and
/// meets the failure there, the ending then stepping into the failed log. Without the fault, the
/// held room let go after the first step, the same commit commits.
fn a_commit_whose_admission_its_driver_sees_fail(seed: u64, fail: bool, by_drop: bool) {
    use super::COMMITS_MET;
    use crate::storage::atoll::btree::Mode;
    use crate::storage::atoll::page::SizeClass;
    use crate::storage::atoll::sql::AtollDb;
    use crate::storage::atoll::testing::drive;
    use crate::storage::atoll::txn::versioned::{VersionedTree, Write, WriteOp};
    use crate::storage::atoll::txn::{Blocker, Commit, Isolation, Txn};
    use crate::types::{IOResult, ImmutableRecord};
    let name = format!(
        "/db/commit-admission-{}{}",
        if fail { "fails" } else { "control" },
        if by_drop { "-drop" } else { "" }
    );
    let observed = (|| -> std::result::Result<_, String> {
        let crash = Arc::new(crate::io::crash::CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let mut cfg = config();
        cfg.log.buffer_bytes = 32 << 10;
        cfg.log.chunk_bytes = 8 << 10;
        let db = Arc::new(AtollDb::open(io.clone(), &name, cfg).map_err(|e| e.to_string())?);
        let txns = db.atoll().transactions();
        let log = db.atoll().log().clone();
        let begin = || -> std::result::Result<Txn, String> {
            let mut txn =
                drive(&io, || txns.begin(Isolation::Snapshot)).map_err(|e| e.to_string())?;
            crate::storage::atoll::testing::share(txns, &mut txn);
            txns.statement(&mut txn).map_err(|e| e.to_string())?;
            Ok(txn)
        };
        let record = ImmutableRecord::from_values(&[Value::build_text("v".repeat(100))], 1)
            .map_err(|e| e.to_string())?;
        let mut txn = begin()?;
        let root = drive(&io, || db.create_tree(&mut txn, SizeClass::new(0).unwrap()))
            .map_err(|e| e.to_string())?;
        let mut commit = Commit::new(txn);
        drive(&io, || txns.commit(&mut commit)).map_err(|e| e.to_string())?;
        drop(commit);
        let tree = VersionedTree::open(
            io.clone(),
            db.atoll().pool().clone(),
            db.atoll().store().clone(),
            root,
        );
        let mut txn = begin()?;
        let key = crate::storage::atoll::sql::key::rowid(1);
        let mut op = WriteOp::put(&key, record.get_payload().to_vec(), Mode::Upsert);
        match drive(&io, || tree.write(txns, db.versions(), &mut txn, &mut op)) {
            Ok(Write::Done(_)) => {}
            other => return Err(format!("write: {other:?}")),
        }
        let blocker = Blocker {
            slot: txn.slot(),
            start: txn.start(),
        };
        // A transaction's records stay its own until its commit: a change published alone (an
        // empty `RUN`) is appended and not flushed, nobody waiting on the log, and the rest of the
        // buffer is held, so the commit's admission registers and flushes that record.
        drive(&io, || {
            txns.publish_alone(
                &mut crate::storage::atoll::log::LogWait::default(),
                &crate::storage::atoll::txn::changes::Changes::new(),
            )
        })
        .map_err(|e| e.to_string())?;
        let unflushed = log.end() > log.durable();
        let mut held = Some(log.hold_buffer_room());
        if fail {
            fail_the_log(&crash, &name);
        }
        let mut ending = super::Ending::commit(
            db.clone(),
            crate::storage::atoll::buffer::Conn::new(),
            txn,
            Vec::new(),
            Vec::new(),
            None,
        );
        let first = format!("{:?}", ending.step().map(|r| r.is_io()));
        let admitting = log.admitting();
        let flushed = crash.in_flight();
        // A fixture that did not arm the wait lets the room go before the drive, so that it ends
        // and the assertions below report it.
        if !fail || first != "Ok(true)" || !admitting || flushed == 0 {
            drop(held.take());
        }
        let met_before = COMMITS_MET.with(|n| n.get());
        let ended = if by_drop {
            drop(ending);
            None
        } else {
            let ended = ending.finish().map_err(|e| e.to_string());
            drop(ending);
            Some(ended)
        };
        let met = COMMITS_MET.with(|n| n.get()) - met_before;
        drop(held);
        clear_faults(&crash);
        let log_failed = log.failure().is_some();
        let gate_held = txns.holds_shared(blocker);
        let failure = txns.failure().map(|e| e.to_string());
        let mut begun = Vec::new();
        for _ in 0..txns.slot_count() {
            if let Ok(IOResult::Done(txn)) = txns.begin(Isolation::Snapshot) {
                begun.push(txn);
            }
        }
        let free = begun.len();
        for txn in begun {
            txns.abort(txn);
        }
        drop((tree, log, db, io));
        let in_flight = crash.in_flight();
        Ok((
            unflushed, first, admitting, flushed, met, ended, log_failed, gate_held, failure, free,
            in_flight,
        ))
    })();
    let (
        unflushed,
        first,
        admitting,
        flushed,
        met,
        ended,
        log_failed,
        gate_held,
        failure,
        free,
        in_flight,
    ) = observed.expect("the fixture failed, every database dropped");
    assert!(
        unflushed,
        "the record published alone was flushed before the commit"
    );
    assert_eq!(first, "Ok(true)", "the commit's first step did not wait");
    assert!(admitting, "the commit did not wait for the log's room");
    assert!(flushed > 0, "the commit's admission flushed nothing");
    assert_eq!(
        met,
        u64::from(fail),
        "the ending's driver told the commit of {met} failures"
    );
    assert_eq!(log_failed, fail);
    match &ended {
        Some(Err(e)) => assert!(fail, "the commit failed without a fault: {e}"),
        Some(Ok(())) => assert!(!fail, "the commit whose log failed returned success"),
        None => {}
    }
    assert!(!gate_held, "the ended commit kept its hold of the gate");
    assert_eq!(free, 4, "the ended commit kept its transaction's slot");
    assert_eq!(
        failure, None,
        "a commit undone in memory failed the database"
    );
    assert_eq!(in_flight, 0);
}

#[test]
fn a_commit_whose_admission_its_driver_sees_fail_rolls_back() {
    a_commit_whose_admission_its_driver_sees_fail(687, true, false);
}

#[test]
fn a_commit_whose_admission_its_destructor_sees_fail_rolls_back() {
    a_commit_whose_admission_its_driver_sees_fail(689, true, true);
}

#[test]
fn a_commit_whose_admission_waited_commits_once_its_room_is_let_go() {
    a_commit_whose_admission_its_driver_sees_fail(688, false, false);
}

/// A `ROLLBACK` steps its transaction's ending as `COMMIT` does, never driving it in place
/// (DESIGN.md 22.18): its undo, meeting a page written back and evicted, returns `IO` from the
/// statement's step, and once done the transaction's write is undone, the tables it created gone
/// from the schema and from temp's, the connection out of its transaction, every slot free and the gate free. The
/// same `ROLLBACK` dropped after its first `IO`: its ending, in the connection's place, is driven to
/// its end by the drop, with the same outcome (mutant: the rollback driven in place,
/// `Connection::rollback_atoll`, its step never returning `IO`). With the root resident (`evict`
/// false) the ending is done in the instruction's first call, which returns no `IO`, and the
/// rollback's own work runs there, the transaction's read-only mark let go among it (mutants: that
/// work skipped on the first call, or on the re-entry). Ended on the first call or on the re-entry,
/// the transaction is let go whole: its deferred foreign-key violation and its CDC transaction id
/// are gone, so the next transaction commits (the auditor's #4518; mutant: the re-entry arm as it
/// was, which kept a rollback's violations and every end's CDC id).
fn a_rollback_statement_steps_its_ending(seed: u64, dropped: bool, evict: bool) {
    use crate::storage::atoll::sql::catalog::key;
    use crate::storage::atoll::txn::versioned::Get;
    use crate::storage::atoll::txn::Isolation;
    use crate::types::IOResult;
    let name = format!("/db/rollback-steps-{dropped}-{evict}");
    let crash = Arc::new(crate::io::crash::CrashIO::new(seed));
    let io: Arc<dyn IO> = crash.clone();
    let db = open(&io, &name);
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let values = (0..300)
        .map(|n| format!("({n}, zeroblob(100))"))
        .collect::<Vec<_>>()
        .join(", ");
    let setup = [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB)".to_string(),
        format!("INSERT INTO t VALUES {values}"),
        "CREATE TABLE p(id INTEGER PRIMARY KEY)".to_string(),
        "CREATE TABLE c(id INTEGER PRIMARY KEY, \
         pid INTEGER REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)"
            .to_string(),
        "PRAGMA foreign_keys = ON".to_string(),
        "PRAGMA capture_data_changes_conn('id')".to_string(),
    ]
    .iter()
    .try_for_each(|sql| ran_on(&conn, &crash, sql));
    let id = match rows(&conn, "SELECT rootpage FROM sqlite_schema WHERE name = 't'")
        .first()
        .and_then(|r| r.first())
    {
        Some(Value::Numeric(crate::numeric::Numeric::Integer(id))) => *id as u64,
        other => panic!("a root page of {other:?}"),
    };
    let inner = conn.atoll().unwrap().db.clone();
    let txns = inner.atoll().transactions();
    let root = {
        let mut txn =
            crate::storage::atoll::drive(inner.io(), || txns.begin(Isolation::Snapshot)).unwrap();
        let mut get = Get::new(&key(id));
        let entry = crate::storage::atoll::drive(inner.io(), || {
            inner
                .catalog()
                .get(txns, inner.versions(), &mut txn, &mut get)
        });
        txns.abort(txn);
        entry.unwrap().expect("t's catalog row").root
    };
    // A row rewritten, a child without its parent and a table created in the transaction, then the
    // tree written back and its root evicted: the undo reaches the row's leaf through the root,
    // read in again.
    let began = [
        "BEGIN",
        "UPDATE t SET b = zeroblob(50) WHERE a = 150",
        "INSERT INTO c VALUES (1, 99)",
        "CREATE TABLE u(x)",
        "CREATE TEMP TABLE v(x)",
    ]
    .iter()
    .try_for_each(|sql| ran_on(&conn, &crash, sql));
    super::full_checkpoint(&inner).unwrap();
    let pool = inner.atoll().pool().clone();
    let mut sweep = pool.sweep();
    crate::storage::atoll::drive(inner.io(), || sweep.step(&pool)).unwrap();
    drop(sweep);
    let evicted = !evict || pool.evict_clean(root);
    // As `BEGIN READ ONLY` marks its transaction: the rollback's own work lets the mark go.
    conn.set_tx_read_only(true);
    let held = (
        conn.get_deferred_foreign_key_violations(),
        conn.get_cdc_transaction_id(),
    );
    let mut rollback = conn.prepare("ROLLBACK").unwrap();
    let mut ios = 0;
    let ended = loop {
        match rollback.step() {
            Ok(crate::StepResult::IO) => {
                ios += 1;
                if dropped {
                    break Ok(());
                }
                if let Err(e) = crash.step() {
                    break Err(e.to_string());
                }
            }
            Ok(crate::StepResult::Done) => break Ok(()),
            other => break Err(format!("{other:?}")),
        }
    };
    drop(rollback);
    let read_only = conn.is_tx_read_only();
    let let_go = (
        conn.get_deferred_foreign_key_violations(),
        conn.get_cdc_transaction_id(),
    );
    let state = (conn.get_auto_commit(), conn.get_tx_state());
    let next = ["BEGIN", "INSERT INTO p VALUES (7)", "COMMIT"]
        .iter()
        .try_for_each(|sql| ran_on(&conn, &crash, sql));
    let kept = rows(&conn, "SELECT count(*), length(b) FROM t WHERE a = 150");
    let count = rows(&conn, "SELECT count(*) FROM t");
    let u_gone = conn.prepare("SELECT x FROM u").is_err();
    let v_gone = conn.prepare("SELECT x FROM v").is_err();
    let u_rows = rows(&conn, "SELECT count(*) FROM sqlite_schema WHERE name = 'u'");
    let mut begun = Vec::new();
    for _ in 0..txns.slot_count() {
        if let Ok(IOResult::Done(txn)) = txns.begin(Isolation::Snapshot) {
            begun.push(txn);
        }
    }
    let free = begun.len();
    let gate = match begun.first_mut() {
        Some(txn) => format!(
            "{:?}",
            txns.take_exclusive(txn, &crate::io::Completion::new_wait())
        ),
        None => "no transaction".to_string(),
    };
    for txn in begun {
        txns.abort(txn);
    }
    let failure = txns.failure().map(|e| e.to_string());
    drop((pool, inner, conn, db));
    drop(io);
    let in_flight = crash.in_flight();
    assert_eq!(setup, Ok(()));
    assert_eq!(began, Ok(()));
    assert!(evicted, "t's root was not evicted");
    assert_eq!(ended, Ok(()));
    if evict {
        assert!(
            ios > 0,
            "the ROLLBACK's step never returned IO: its ending was driven in place"
        );
    } else {
        assert_eq!(ios, 0, "a ROLLBACK with every page resident returned IO");
    }
    assert_eq!(state, (true, crate::connection::TransactionState::None));
    assert!(
        !read_only,
        "the rolled-back transaction's read-only mark stayed"
    );
    assert!(
        held.0 > 0 && held.1 != -1,
        "the transaction held no deferred violation or no CDC id: {held:?}"
    );
    assert_eq!(
        let_go,
        (0, -1),
        "the rolled-back transaction's deferred violations or CDC id stayed"
    );
    assert_eq!(next, Ok(()), "the next transaction did not commit");
    assert_eq!(
        kept,
        vec![vec![Value::from_i64(1), Value::from_i64(100)]],
        "the rolled-back update is still there"
    );
    assert_eq!(count, vec![vec![Value::from_i64(300)]]);
    assert!(
        u_gone,
        "the rolled-back table is still in the connection's schema"
    );
    assert_eq!(u_rows, vec![vec![Value::from_i64(0)]]);
    assert!(
        v_gone,
        "the rolled-back temporary table is still in temp's schema"
    );
    assert_eq!(free, 4, "the rolled-back transaction kept its slot");
    assert_eq!(
        gate, "Ok(Done)",
        "the rolled-back transaction kept the gate"
    );
    assert_eq!(failure, None);
    assert_eq!(in_flight, 0);
}

#[test]
fn a_rollback_statement_steps_its_ending_and_returns_its_io() {
    a_rollback_statement_steps_its_ending(690, false, true);
}

#[test]
fn a_rollback_statement_dropped_mid_ending_finishes_it() {
    a_rollback_statement_steps_its_ending(691, true, true);
}

#[test]
fn a_rollback_statement_whose_ending_needs_no_io_is_done_in_its_first_call() {
    a_rollback_statement_steps_its_ending(692, false, false);
}

/// Facade reads in a frame of a database opened async (ASYNC1 D2e §2.2): each test's body in a
/// child of its own.
#[cfg(all(unix, not(miri)))]
mod in_frames {
    use super::*;

    // ---------------------------------------------- facade reads in a frame (ASYNC1 D2e §2.2)

    /// A database opened async (ASYNC1 D2a): a cold read inside a cooperative frame is queued for
    /// its IO thread and returns IO, and the test waits for it outside the frame.
    fn deferred(path: &str) -> (Arc<dyn IO>, Arc<Database>, Arc<Connection>) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = crate::storage::atoll::requests::opened_async(|| open(&io, path));
        let conn = db.connect(ConnectOpts::new()).unwrap();
        (io, db, conn)
    }

    /// Checkpoints everything written so far, so the pages written are clean and can be evicted:
    /// twice, the second writing the page-table pages the first changed as it placed the pages.
    fn checkpointed(conn: &Arc<Connection>) {
        rows(conn, "PRAGMA wal_checkpoint(TRUNCATE)");
        rows(conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    }

    /// The database's IO thread held before its next idle, until the sender returned drops.
    fn held_io(db: &Database) -> std::sync::mpsc::Sender<()> {
        let atoll = db.atoll.as_ref().unwrap().atoll();
        crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(
            atoll.requests_for_test(),
        )
        .expect("the IO thread held before its idle")
    }

    /// Waits for the IO thread's idle note `idle` armed before the work: whether it came.
    fn idled(idle: &std::sync::mpsc::Receiver<()>) -> bool {
        idle.recv_timeout(std::time::Duration::from_secs(20))
            .is_ok()
    }

    /// Waits until every request on the database's files has retired: whether they did.
    fn retired(db: &Database) -> bool {
        let atoll = db.atoll.as_ref().unwrap().atoll();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while atoll.outstanding_requests() != 0 {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        true
    }

    /// Waits outside any frame for `c`, which the IO thread finishes: whether it finished.
    fn waited(io: &Arc<dyn IO>, c: &crate::Completion) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !c.finished() {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            let _ = io.step();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        true
    }

    /// Evicts every page nothing holds or keeps, the pool's and the metadata cache's. The last
    /// metadata read's request a test build keeps on this thread (`meta::LAST_READ`), whose callback
    /// holds its page, is dropped first, so only what the read itself keeps stays (#5231).
    fn chill(db: &Database) {
        drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
        let atoll = db.atoll.as_ref().unwrap().atoll();
        atoll.pool().evict_all_clean();
        atoll.store().meta().evict_all();
    }

    /// What a facade read kept across its calls did.
    struct Framed<T> {
        value: T,
        calls: usize,
        /// The pool's pages and the metadata pages loaded meanwhile, in order.
        pool: Vec<u64>,
        meta: Vec<crate::storage::atoll::meta::MetaId>,
    }

    /// Calls `read` in a test frame until it is done. Each return for IO is checked to hold no guard
    /// of the connection's transaction and to keep the read's state; it is delivered outside the
    /// frame, its load landed and its request retired, and every page nothing keeps is evicted
    /// before the next call.
    fn in_frames_chilled<T>(
        io: &Arc<dyn IO>,
        db: &Database,
        ct: &crate::storage::atoll::sql::mode::ConnTxn,
        mut read: impl FnMut(
            &mut Option<Box<crate::storage::atoll::sql::mode::FacadeRead>>,
        ) -> crate::Result<crate::types::IOResult<T>>,
    ) -> Framed<T> {
        use crate::types::{IOCompletions, IOResult};
        let atoll = db.atoll.as_ref().unwrap().atoll();
        let (pool_before, meta_before) = (
            atoll.pool().loaded_pages().len(),
            atoll.store().meta().loaded_pages().len(),
        );
        let mut kept = None;
        let mut calls = 0;
        let value = loop {
            calls += 1;
            // The IO thread held before its idle while the call runs, so whatever the call
            // queues is still waiting when it returns (#5249); let go once it has, the idle
            // note armed first so it is of the idle after that work.
            let held = held_io(db);
            let out = {
                let _frame = crate::storage::atoll::cooperative::Frame::enter();
                read(&mut kept).unwrap()
            };
            let idle = atoll.io_idle_for_test();
            drop(held);
            assert!(
                ct.cell.try_lock().is_some(),
                "call {calls}: a guard of the transaction held across the return"
            );
            match out {
                IOResult::Done(value) => {
                    assert!(kept.is_none(), "a finished read's state kept");
                    break value;
                }
                IOResult::IO(IOCompletions(c)) => {
                    assert!(
                        kept.is_some(),
                        "call {calls}: the read's state not kept across its return"
                    );
                    assert!(waited(io, &c), "call {calls}: its wait never finished");
                    // Every request retired, so no record of one holds a frame the eviction below
                    // would otherwise take, and the IO thread idle again, so no capture of its own
                    // holds a page either (#5248).
                    assert!(
                        retired(db),
                        "call {calls}: a request still outstanding before the eviction"
                    );
                    assert!(
                        idled(&idle),
                        "call {calls}: the IO thread never went idle after its work"
                    );
                    chill(db);
                }
            }
            assert!(calls < 64, "the read did not finish within {calls} calls");
        };
        Framed {
            value,
            calls,
            pool: atoll.pool().loaded_pages()[pool_before..].to_vec(),
            meta: atoll.store().meta().loaded_pages()[meta_before..].to_vec(),
        }
    }

    /// Asserts no page was loaded twice, the pool's and the metadata cache's counted apart, and that
    /// the read took at most a call for each page it loaded and one more.
    fn loaded_once<T>(what: &str, framed: &Framed<T>) {
        loaded_at_most(what, framed, 1);
    }

    /// Asserts no page was loaded more than `times` times, the pool's and the metadata cache's
    /// counted apart, and that the read took at most a call for each page it loaded and one more. A
    /// read of `times` keys whose descents share pages above their leaves loads a shared page again
    /// only for a later key's descent, once that page was let go for the page below it: at most once
    /// a key (ASYNC1 D2e §2.2b, #5216).
    fn loaded_at_most<T>(what: &str, framed: &Framed<T>, times: usize) {
        fn most<P: std::hash::Hash + Eq>(pages: &[P]) -> usize {
            let mut counts = std::collections::HashMap::new();
            for p in pages {
                *counts.entry(p).or_insert(0) += 1;
            }
            counts.into_values().max().unwrap_or(0)
        }
        assert!(
            most(&framed.pool) <= times && most(&framed.meta) <= times,
            "{what}: the page loaded again on re-entry, past {times} a page: pool {:?}, metadata {:?}",
            framed.pool,
            framed.meta
        );
        let loads = framed.pool.len() + framed.meta.len();
        assert!(
            framed.calls <= loads + 1,
            "{what}: {} calls for {loads} loads",
            framed.calls
        );
        assert!(loads > 0, "{what}: nothing was cold");
    }

    /// A transaction begun on `conn` for a read, and its connection's facade.
    fn reading<'c>(
        io: &Arc<dyn IO>,
        conn: &'c Arc<Connection>,
    ) -> &'c crate::storage::atoll::sql::mode::ConnTxn {
        let ct = conn.atoll().expect("Atoll mode");
        crate::storage::atoll::drive(io, || {
            ct.begin(crate::storage::atoll::sql::mode::Begin::Read)
        })
        .unwrap();
        ct
    }

    /// The schema cookie and the header, read in a frame of a transaction whose header pages and
    /// the metadata pages that locate them are cold: each returns IO holding no guard of the
    /// transaction, goes on where it stopped when called again, every page not kept evicted between
    /// calls, and finishes with the synchronous value, no page loaded twice (mutants `field-parks`,
    /// `no-keep`, the metadata keep removed).
    #[test]
    fn a_cold_header_read_in_a_frame_returns_io_and_finishes_with_its_synchronous_value() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_cold_header_read_in_a_frame_returns_io_and_finishes_with_its_synchronous_value"
            ),
            || {
                let (io, db, conn) = deferred("/db/frame-header");
                conn.execute("CREATE TABLE t (a)").unwrap();
                checkpointed(&conn);
                let other = db.connect(ConnectOpts::new()).unwrap();
                let (cookie, header) = {
                    // Read once synchronously, which opens every file the reads reach.
                    let ct = reading(&io, &other);
                    (ct.schema_cookie().unwrap(), ct.header().unwrap())
                };
                let ct = reading(&io, &conn);
                // The pages the begins changed, written, so the eviction below takes them too.
                let ck = db.connect(ConnectOpts::new()).unwrap();
                checkpointed(&ck);
                chill(&db);
                let framed = in_frames_chilled(&io, &db, ct, |read| ct.schema_cookie_in(read));
                assert_eq!(framed.value, cookie, "the schema cookie read in frames");
                loaded_once("the schema cookie", &framed);
                assert!(
                    framed.calls > 1,
                    "the schema cookie's read never returned IO"
                );
                assert!(
                    !framed.pool.is_empty(),
                    "the header's tree pages were never cold: metadata {:?}",
                    framed.meta
                );
                let third = db.connect(ConnectOpts::new()).unwrap();
                let ct = reading(&io, &third);
                checkpointed(&ck);
                chill(&db);
                let framed = in_frames_chilled(&io, &db, ct, |read| ct.header_in(read));
                assert_eq!(
                    (
                        framed.value.schema_cookie.get(),
                        framed.value.user_version.get()
                    ),
                    (header.schema_cookie.get(), header.user_version.get()),
                    "the header read in frames: (schema cookie, user version)"
                );
                loaded_once("the header", &framed);
                assert!(
                    !framed.pool.is_empty(),
                    "the header's tree pages were never cold for the header: metadata {:?}",
                    framed.meta
                );
            },
        );
    }

    /// The page store's pages in use, read in a frame with every class's metadata root cold: one
    /// class's root a call, each kept until the next call, the classes counted so far kept with the
    /// read; it finishes within a call for each root and one more, with the synchronous count
    /// (mutants `pages-restarts`, the class loop starting again at the first class each call, and
    /// `pages-parks`).
    #[test]
    fn the_pages_in_use_read_in_a_frame_goes_on_from_the_class_it_stopped_at() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::the_pages_in_use_read_in_a_frame_goes_on_from_the_class_it_stopped_at"
            ),
            || {
                let (io, db, conn) = deferred("/db/frame-pages");
                conn.execute("CREATE TABLE t (a)").unwrap();
                conn.execute("INSERT INTO t VALUES (randomblob(100000))")
                    .unwrap();
                checkpointed(&conn);
                let ct = reading(&io, &conn);
                let pages = ct.pages().unwrap();
                chill(&db);
                let framed = in_frames_chilled(&io, &db, ct, |read| ct.pages_in(read));
                assert_eq!(framed.value, pages, "the pages in use read in frames");
                loaded_once("the pages in use", &framed);
                assert!(
                    framed.meta.len() > 1,
                    "only {} metadata roots were cold: {:?}",
                    framed.meta.len(),
                    framed.meta
                );
            },
        );
    }

    /// A schema writer's begin whose read set names two trees whose catalog rows are in different
    /// leaves, both cold, in a frame: the check returns IO at each leaf, goes on from the row it
    /// stopped at, and finishes with the synchronous answer, each leaf loaded once (mutants
    /// `since-restarts`, the read set's loop starting again each call, and `since-parks`).
    #[test]
    fn a_begins_read_set_check_in_a_frame_goes_on_from_the_row_it_stopped_at() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_begins_read_set_check_in_a_frame_goes_on_from_the_row_it_stopped_at"
            ),
            || {
                use crate::storage::atoll::sql::mode::Begin;
                let (io, db, conn) = deferred("/db/frame-since");
                // Enough tables that the catalog's rows span leaves, the last past the first page-table
                // page's pages.
                for i in 0..700 {
                    conn.execute(format!("CREATE TABLE t{i} (a)")).unwrap();
                }
                let id = |name: &str| match &rows(
                    &conn,
                    &format!("SELECT rootpage FROM sqlite_schema WHERE name = '{name}'"),
                )[..]
                {
                    [row] => match row[0] {
                        Value::Numeric(crate::Numeric::Integer(id)) => id as u64,
                        ref other => panic!("{other:?}"),
                    },
                    other => panic!("{other:?}"),
                };
                checkpointed(&conn);
                let reads = vec![id("t0"), id("t699")];
                // A transaction with a snapshot, holding the gate exclusive already, so the begin's check
                // after its take is its one read of the set.
                let ct = reading(&io, &conn);
                crate::storage::atoll::drive(&io, || {
                    ct.begin_reading(Begin::Write { schema: true }, &[], &mut None)
                })
                .unwrap();
                chill(&db);
                let framed = in_frames_chilled(&io, &db, ct, |read| {
                    ct.begin_reading(Begin::Write { schema: true }, &reads, read)
                });
                assert!(
                    !framed.value,
                    "the begin found itself beginning a transaction"
                );
                // Two keys: a page above the leaves loaded again for the second key's descent, at most.
                loaded_at_most("the read set's check", &framed, reads.len());
                // The page-table pages that locate the catalog's pages were cold too, and the read crossed
                // two of them, so a descent waited on a page-table page of its own between a kept node and
                // its child (#5220, #5226): a page-table page maps `MAP_ENTRIES` pages.
                let mapped: std::collections::HashSet<_> = framed.meta.iter().collect();
                assert!(
                    mapped.len() >= 2,
                    "the read crossed {} page-table pages, past {} pages a page: pool {:?}, metadata {:?}",
                    mapped.len(),
                    crate::storage::atoll::radix::MAP_ENTRIES,
                    framed.pool,
                    framed.meta
                );
                assert!(
                    framed.pool.len() > 1,
                    "the read set's rows were in {} cold leaves",
                    framed.pool.len()
                );
            },
        );
    }

    /// A statement reset while its header read waits in a frame (#5219, #5225): `PRAGMA
    /// schema_version` stepped in a frame of a transaction whose header pages are cold returns IO,
    /// its read kept in the statement's program state with the exact page it waits for, a pool
    /// frame in a load's group or a metadata page. `Statement::reset` drops that state, its owner
    /// and so that keep: the frame no group keeps any more, or the metadata page evictable, once
    /// the request retired; and the statement stepped again outside any frame finishes with the
    /// synchronous value (mutant `reset-keeps-read`, the reset leaving the read in place).
    #[test]
    fn a_statement_reset_while_its_header_read_waits_lets_the_reads_page_go() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_statement_reset_while_its_header_read_waits_lets_the_reads_page_go"
            ),
            || {
                let (io, db, conn) = deferred("/db/frame-reset");
                conn.execute("CREATE TABLE t (a)").unwrap();
                checkpointed(&conn);
                let expected = rows(&conn, "PRAGMA schema_version");
                let atoll = db.atoll.as_ref().unwrap().atoll();
                conn.execute("BEGIN").unwrap();
                // The transaction begun outside any frame, so the statement's own begin finds it.
                assert_eq!(
                    rows(&conn, "SELECT count(*) FROM t"),
                    vec![vec![Value::from_i64(0)]]
                );
                let mut stmt = conn.prepare("PRAGMA schema_version").unwrap();
                chill(&db);
                let held = held_io(&db);
                let stepped = {
                    let _frame = crate::storage::atoll::cooperative::Frame::enter();
                    stmt.step().map(|r| matches!(r, crate::StepResult::IO))
                };
                let idle = atoll.io_idle_for_test();
                drop(held);
                // The load it waits for landed and retired, and the test build's handle on the last
                // metadata request dropped, so only the read's keep is left of it (#5231).
                let landed = stmt.pending_io_for_test().is_some_and(|c| waited(&io, &c))
                    && retired(&db)
                    && idled(&idle);
                drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
                let before = stmt.atoll_read_for_test().map(|r| r.keeps_for_test());
                let (frame, meta) = before.unwrap_or_default();
                let kept_before = (
                    frame.map(|f| atoll.pool().frame_kept_for_test(f)),
                    meta.and_then(|id| atoll.store().meta().pins_of(id)),
                );
                stmt.reset().unwrap();
                let after = stmt.atoll_read_for_test().map(|r| r.keeps_for_test());
                let all_retired = retired(&db);
                let kept_after = (
                    frame.map(|f| atoll.pool().frame_kept_for_test(f)),
                    meta.and_then(|id| atoll.store().meta().pins_of(id)),
                );
                let mut value = None;
                for _ in 0..1000 {
                    match stmt.step().unwrap() {
                        crate::StepResult::Row => {
                            value = Some(
                                stmt.row()
                                    .unwrap()
                                    .get_values()
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            )
                        }
                        crate::StepResult::Done => break,
                        crate::StepResult::IO => stmt._io().step().unwrap(),
                        other => panic!("{other:?}"),
                    }
                }
                drop(stmt);
                conn.execute("COMMIT").unwrap();
                drop((conn, db));
                assert_eq!(
                    stepped.ok(),
                    Some(true),
                    "the statement's step in a frame returned IO"
                );
                assert!(landed, "the statement's wait never landed and retired");
                assert!(
                    before.is_some_and(|(f, m)| f.is_some() || m.is_some()),
                    "the statement's waiting read kept nothing: {before:?}"
                );
                assert_eq!(
                    kept_before.0,
                    frame.map(|_| true),
                    "before the reset, the frame kept for {before:?}"
                );
                assert!(
                    meta.is_none() || kept_before.1.is_some_and(|pins| pins >= 1),
                    "before the reset, the metadata page's pins {:?} for {before:?}",
                    kept_before.1
                );
                assert_eq!(after, None, "the statement's kept read survived its reset");
                assert!(all_retired, "a request outstanding after the reset");
                assert_eq!(
                    kept_after.0,
                    frame.map(|_| false),
                    "after the reset, the frame kept for {before:?}"
                );
                // The read's own pin let go: one fewer, whatever else holds the page.
                assert_eq!(
                    meta.map(|_| kept_after.1.unwrap_or(0)),
                    meta.map(|_| kept_before.1.unwrap_or(0) - 1),
                    "after the reset, the metadata page's pins, one fewer than before, for {before:?}"
                );
                assert_eq!(
                    value.map(|row| vec![row]),
                    Some(expected),
                    "the schema version read after the reset"
                );
            },
        );
    }

    /// A read in a frame whose owner comes short of memory keeps its place in the shortage queue
    /// across its return (#5180 E1). Memory scarce for every call, the read's calls go on until one
    /// comes short: after that return the owner it keeps is still queued, holding the frame reserved
    /// for it, and its next call, still scarce, goes on at that place, its load started in the frame
    /// reserved; the read then finishes with the synchronous value. A read dropped unfinished gives
    /// its place back (mutant `read-owner-fresh`, a fresh owner each call, which loses the place and
    /// its frame at its return and comes short again).
    #[test]
    fn a_read_short_of_memory_in_a_frame_keeps_its_place_across_its_return() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_read_short_of_memory_in_a_frame_keeps_its_place_across_its_return"
            ),
            || {
                use crate::types::{IOCompletions, IOResult};
                let (io, db, conn) = deferred("/db/frame-short");
                conn.execute("CREATE TABLE t (a)").unwrap();
                checkpointed(&conn);
                let other = db.connect(ConnectOpts::new()).unwrap();
                let cookie = reading(&io, &other).schema_cookie().unwrap();
                let ct = reading(&io, &conn);
                let pool = db.atoll.as_ref().unwrap().atoll().pool().clone();
                chill(&db);
                let deliver =
                    |c: crate::Completion| assert!(waited(&io, &c), "a wait never finished");
                type Kept = Option<Box<crate::storage::atoll::sql::mode::FacadeRead>>;
                let scarce_call = |ct: &crate::storage::atoll::sql::mode::ConnTxn,
                                   kept: &mut Kept| {
                    crate::storage::atoll::buffer::scarce(|| {
                        let _frame = crate::storage::atoll::cooperative::Frame::enter();
                        ct.schema_cookie_in(kept).unwrap()
                    })
                };
                let mut kept: Kept = None;
                // Until a call comes short: the metadata pages it reads first take no frame.
                let mut short = false;
                for _ in 0..8 {
                    match scarce_call(ct, &mut kept) {
                        IOResult::IO(IOCompletions(c)) => {
                            if kept.as_ref().is_some_and(|r| r.queued_for_test()) {
                                short = true;
                                break;
                            }
                            deliver(c);
                        }
                        IOResult::Done(_) => break,
                    }
                }
                let place_after_return = pool.shortage_len_for_test();
                let loads = pool.loaded_pages().len();
                let mut out = scarce_call(ct, &mut kept);
                let loaded_at_its_place = pool.loaded_pages().len() > loads;
                let value = loop {
                    match out {
                        IOResult::Done(v) => break v,
                        IOResult::IO(IOCompletions(c)) => deliver(c),
                    }
                    out = {
                        let _frame = crate::storage::atoll::cooperative::Frame::enter();
                        ct.schema_cookie_in(&mut kept).unwrap()
                    };
                };
                // A second read short of memory, dropped unfinished: its place goes back.
                let third = db.connect(ConnectOpts::new()).unwrap();
                let ct3 = reading(&io, &third);
                chill(&db);
                let mut dropped: Kept = None;
                let mut queued_before_drop = 0;
                for _ in 0..8 {
                    match scarce_call(ct3, &mut dropped) {
                        IOResult::IO(IOCompletions(c)) => {
                            if dropped.as_ref().is_some_and(|r| r.queued_for_test()) {
                                queued_before_drop = pool.shortage_len_for_test();
                                break;
                            }
                            deliver(c);
                        }
                        IOResult::Done(_) => break,
                    }
                }
                drop(dropped);
                let queued_after_drop = pool.shortage_len_for_test();
                drop(pool);
                drop((third, other, conn, db));
                assert!(short, "no call of the read came short of memory");
                assert_eq!(place_after_return, 1, "the read's place after its return");
                assert!(
                    loaded_at_its_place,
                    "the call after the shortage did not go on at its place: no load started"
                );
                assert_eq!(value, cookie, "the schema cookie read across the shortage");
                assert_eq!(
                    (queued_before_drop, queued_after_drop),
                    (1, 0),
                    "the places of a read dropped unfinished, (before, after) the drop"
                );
            },
        );
    }
}

/// Every site in the SQL facade that drives an operation to its end, by the function it is in,
/// and why that function may block its thread (ASYNC1 D2e §1 (i)): each later slice, or D3, that
/// converts it, or the one condition under which it drives.
const DRIVE_SITES: &[(&str, &str, &str)] = &[
    (
        "integrity.rs",
        "roots",
        "slice (iii): the integrity check's walk",
    ),
    (
        "integrity.rs",
        "reached",
        "slice (iii): the integrity check's walk",
    ),
    (
        "integrity.rs",
        "account",
        "slice (iii): the integrity check's walk",
    ),
    ("lifecycle.rs", "drain_lists", "D3: the open phase"),
    ("mod.rs", "drop", "D3: the database's drop"),
    ("mod.rs", "open_holding", "D3: the open phase"),
    ("mod.rs", "ensure_schema_row", "D3: the open phase"),
    ("mod.rs", "load_committed_header", "D3: the open phase"),
    (
        "sequence.rs",
        "drop_unnamed_sequences",
        "D3: the open phase",
    ),
    ("mode.rs", "row_in", "slice (ii): the cursor's resolution"),
    (
        "mode.rs",
        "pin",
        "slice (iii): the reparse's pin, with its cookie read",
    ),
    (
        "mode.rs",
        "read_since_snapshot",
        "outside a frame only, its own condition: in one, the read is kept",
    ),
    (
        "mode.rs",
        "rollback_statement",
        "slice (iii): a statement's rollback",
    ),
    (
        "mode.rs",
        "field_in",
        "blocking hosts only: the header's synchronous forms",
    ),
    (
        "mode.rs",
        "pages",
        "blocking hosts only: the pages' synchronous form",
    ),
    ("mode.rs", "count_rows", "slice (iii): a Clear's count"),
    (
        "mode.rs",
        "full_checkpoint",
        "slice (iii): the checkpoint task's work",
    ),
    ("mode.rs", "drive_to_end", "D3: an ending's finish and drop"),
];

/// The facade's reads converted in slice (i): each returns what it waits for, and drives nothing.
const CONVERTED: &[&str] = &[
    "schema_cookie_in",
    "header_in",
    "pages_in",
    "read_in",
    "field_step",
    "header_step",
    "field_at",
    "pages_step",
    "since_step",
    "begin_reading",
];

/// A file's production code: up to its inline test module, which by the house layout comes last.
fn production_of(code: &str) -> &str {
    let mut at = 0;
    while let Some(i) = code[at..].find("#[cfg(") {
        let start = at + i;
        let line_end = code[start..].find('\n').map_or(code.len(), |e| start + e);
        if code[start..line_end].contains("test")
            && code[line_end..].trim_start().starts_with("mod tests {")
        {
            return &code[..start];
        }
        at = line_end;
    }
    code
}

/// The functions of `code` that drive an operation to its end, by name, with each site's line.
fn drive_sites(code: &str) -> Vec<(String, usize)> {
    let mut sites = Vec::new();
    let mut within = String::new();
    for (n, line) in production_of(code).lines().enumerate() {
        let words: Vec<&str> = line
            .split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .filter(|w| !w.is_empty())
            .collect();
        if let Some(i) = words.iter().position(|w| *w == "fn") {
            if let Some(name) = words.get(i + 1) {
                if line.trim_start().starts_with("fn ")
                    || line.trim_start().starts_with("pub")
                    || line.trim_start().starts_with("unsafe fn")
                {
                    within = name.to_string();
                }
            }
        }
        let calls = |name: &str| {
            line.match_indices(&format!("{name}(")).any(|(i, _)| {
                let before = line[..i].chars().last();
                !before.is_some_and(|c| c.is_alphanumeric() || c == '_')
                    && !line[..i].trim_end().ends_with("fn")
            })
        };
        if calls("drive") || calls("drive_for") {
            sites.push((within.clone(), n + 1));
        }
    }
    sites
}

/// The census of the facade's drive sites (ASYNC1 D2e §1 (i)): every function of `sql` that
/// drives an operation to its end is ruled, every rule names a function that does, and no read
/// slice (i) converted drives anything (mutant `census-converted-drives`, a drive in a converted
/// read).
#[test]
fn every_drive_site_of_the_facade_is_ruled_and_no_converted_read_drives() {
    use crate::storage::atoll::testing::{atoll_dir, code_of};
    let dir = atoll_dir().join("sql");
    let mut found = std::collections::BTreeSet::new();
    let mut wrong = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        let file = path.file_name().unwrap().to_string_lossy().to_string();
        if !file.ends_with(".rs") {
            continue;
        }
        let code = code_of(&std::fs::read_to_string(&path).unwrap());
        for (within, line) in drive_sites(&code) {
            if CONVERTED.contains(&within.as_str()) {
                wrong.push(format!(
                    "sql/{file}:{line}: {within} drives, a converted read"
                ));
            } else if !DRIVE_SITES
                .iter()
                .any(|(f, name, _)| *f == file && *name == within)
            {
                wrong.push(format!("sql/{file}:{line}: {within} drives, ruled nowhere"));
            }
            found.insert((file.clone(), within));
        }
    }
    for (file, name, _) in DRIVE_SITES {
        if !found.contains(&(file.to_string(), name.to_string())) {
            wrong.push(format!("sql/{file}: {name} is ruled but drives nothing"));
        }
    }
    assert!(
        found.len() >= DRIVE_SITES.len(),
        "the census found {found:?}"
    );
    assert!(
        wrong.is_empty(),
        "the facade's drive sites:\n{}",
        wrong.join("\n")
    );
}

/// The census's control: a planted drive in a converted read is found, by its function; one in a
/// test module, or a function named for one, is not.
#[test]
fn the_drive_census_sees_a_planted_drive() {
    let planted = "fn field_step(&self) {\n    let w = drive_for(conn, io, || op())?;\n}\n\
                   fn drive_for() {}\n\
                   #[cfg(test)]\nmod tests {\n    fn t() { drive(io, || op()); }\n}\n";
    assert_eq!(
        drive_sites(planted),
        vec![("field_step".to_string(), 2)],
        "the census's sites in the planted code"
    );
}
