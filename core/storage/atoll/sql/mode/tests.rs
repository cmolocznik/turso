//! First light (DESIGN.md §17, R33): Turso's SQL on an Atoll database through `Database::open`,
//! end to end.

use crate::io::{MemoryIO, OpenFlags, IO};
use crate::storage::atoll::sql::catalog::tests::config;
use crate::sync::Arc;
use crate::{ConnectOpts, Connection, Database, OpenOptions, SqliteDialect, Value};

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
/// works, and changing it is refused by name.
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
    let other = if current.as_str().eq_ignore_ascii_case("delete") {
        "wal"
    } else {
        "delete"
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
