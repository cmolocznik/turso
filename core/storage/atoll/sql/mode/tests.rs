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
