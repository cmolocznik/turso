//! Under MVCC, does a connection that restarts the WAL in a checkpoint of its own, after another
//! connection's checkpoint wrote and backfilled frames it never read, go on reading its old pages?
//!
//!     restart_stale
//!
//! A reads every row; B updates every row and checkpoints (TRUNCATE), writing the rows' pages
//! into the WAL, backfilling and restarting it; A checkpoints (TRUNCATE) with nothing of its own to
//! write, which restarts the WAL again; A reads every row. Prints what A's last read saw.
use std::sync::Arc;

use turso_core::{ConnectOpts, Connection, Database, MemoryIO, SqliteDialect, Value};

fn rows(conn: &Arc<Connection>, sql: &str) -> turso_core::Result<Vec<Vec<Value>>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect());
        Ok(())
    })?;
    Ok(rows)
}

/// The versions the rows read at, and how many bodies disagree with their version.
fn versions(conn: &Arc<Connection>) -> String {
    match rows(conn, "SELECT v, substr(body, 1, 1), substr(body, -1, 1) FROM t") {
        Ok(found) => {
            let mut by_version = std::collections::BTreeMap::<i64, usize>::new();
            let mut torn = 0;
            for row in &found {
                let v = row[0].as_int().unwrap();
                *by_version.entry(v).or_default() += 1;
                let letter = char::from(b'a' + (v % 26) as u8).to_string();
                if row[1].to_string() != letter || row[2].to_string() != letter {
                    torn += 1;
                }
            }
            format!("{} rows by version {by_version:?}, {torn} with another version's body", found.len())
        }
        Err(error) => format!("error: {error}"),
    }
}

fn main() {
    // LENGTH sets the bodies' length (default 1000; past a page they overflow). Rows are written
    // and checkpointed, then the database is opened afresh so that A reads them from the B-tree.
    let length: usize = std::env::var("LENGTH").ok().and_then(|s| s.parse().ok()).unwrap_or(1000);
    let io = Arc::new(MemoryIO::new());
    {
        let db = Database::open_file(io.clone(), "restart-stale.db", Arc::new(SqliteDialect)).unwrap();
        let setup = db.connect(ConnectOpts::new()).unwrap();
        setup.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        setup.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL, body TEXT NOT NULL)").unwrap();
        for id in 1..=300 {
            setup.execute(format!("INSERT INTO t VALUES ({id}, 0, '{}')", "a".repeat(length))).unwrap();
        }
        setup.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        setup.close().unwrap();
    }
    let db = Database::open_file(io, "restart-stale.db", Arc::new(SqliteDialect)).unwrap();
    let a = db.connect(ConnectOpts::new()).unwrap();
    let b = db.connect(ConnectOpts::new()).unwrap();
    println!("A before: {}", versions(&a));
    b.execute(format!("UPDATE t SET v = 1, body = '{}'", "b".repeat(length))).unwrap();
    b.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    println!("B after its checkpoint: {}", versions(&b));
    a.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let seen = versions(&a);
    println!("A after its own checkpoint: {seen}");
    let fresh = db.connect(ConnectOpts::new()).unwrap();
    println!("a new connection: {}", versions(&fresh));
    std::process::exit(if seen.starts_with("300 rows by version {1: 300}, 0") { 0 } else { 1 });
}
