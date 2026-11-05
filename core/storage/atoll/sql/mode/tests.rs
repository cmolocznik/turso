//! First light (DESIGN.md §17, R33): Turso's SQL on an Atoll database through `Database::open`,
//! end to end.

use crate::io::{MemoryIO, OpenFlags, IO};
use crate::storage::atoll::sql::catalog::tests::config;
use crate::sync::Arc;
use crate::{ConnectOpts, Connection, Database, DatabaseOpts, OpenOptions, SqliteDialect, Value};

/// Each test opens its own path: the registry keys databases by their file across IOs.
fn open(io: &Arc<dyn IO>, path: &str) -> Arc<Database> {
    Database::open(
        io.clone(),
        path,
        OpenOptions::new(Arc::new(SqliteDialect)).atoll(config()),
    )
    .unwrap()
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
    .err()
    .expect("a SQLite file opened in Atoll mode");
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
    .err()
    .expect("a WAL's rows opened in Atoll mode");
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
        .err()
        .expect("a dropped table");
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
                // A write behind an open writer comes back Busy and is retried in place, as SQLite
                // waits on a lock, for as long as the busy timeout (R34).
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

/// A long explicit transaction appending twenty times the log budget before it commits: each of
/// its writing statements steps the checkpointer, so the redo point follows the log while it runs,
/// not only at its commit (§11).
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
    let lag = db.atoll.as_ref().unwrap().atoll().checkpointer().lag();
    eprintln!("before the long transaction's commit, lag {lag}");
    assert!(lag <= 2 * budget, "the redo point trails by {lag} bytes");
    conn.execute("COMMIT").unwrap();
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
        assert!(images.len() > 20, "seed {seed}: {} images", images.len());
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
