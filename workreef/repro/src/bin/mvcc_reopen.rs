//! Does an MVCC database still open after the process writing it is killed mid-checkpoint?
//!
//!     mvcc_reopen DIR ROUNDS [SEED]
//!
//! Each round starts a child that opens DIR/reopen.db under MVCC (synchronous NORMAL) and, until
//! killed, inserts and updates rows of a plain table and runs `PRAGMA wal_checkpoint(TRUNCATE)` after
//! every few commits. After a random 0.1 to 1.5 s the round kills it with SIGKILL and opens the
//! database: it must open, pass PRAGMA integrity_check and hold every row the child reported
//! committed. Prints a line a round and stops at the first failure, keeping DIR.
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Duration;

use turso_core::{ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value};

fn open(path: &str) -> turso_core::Result<(Arc<Database>, Arc<Connection>)> {
    let io = Arc::new(PlatformIO::new()?);
    let db = Database::open_file_with_flags(io, path, OpenFlags::default(), DatabaseOpts::new(), None, Arc::new(SqliteDialect))?;
    let conn = db.connect(ConnectOpts::new())?;
    Ok((db, conn))
}

fn rows(conn: &Arc<Connection>, sql: &str) -> turso_core::Result<Vec<Vec<Value>>> {
    let mut stmt = conn.prepare(sql)?;
    let mut out = Vec::new();
    stmt.run_with_row_callback(|row| {
        out.push(row.get_values().cloned().collect());
        Ok(())
    })?;
    Ok(out)
}

/// Write until killed, printing the count of rows committed after each commit.
fn child(dir: &str, seed: u64) -> turso_core::Result<()> {
    let (_db, conn) = open(&format!("{dir}/reopen.db"))?;
    conn.execute("PRAGMA synchronous = NORMAL")?;
    if rows(&conn, "SELECT count(*) FROM sqlite_schema WHERE name = 't'")?[0][0].as_int() == Some(0) {
        conn.execute("PRAGMA journal_mode = 'mvcc'")?;
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL, body BLOB NOT NULL)")?;
    }
    let mut state = seed | 1;
    let mut next = rows(&conn, "SELECT coalesce(max(id), 0) FROM t")?[0][0].as_int().unwrap_or(0) + 1;
    loop {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        conn.execute("BEGIN CONCURRENT")?;
        for _ in 0..1 + state % 8 {
            conn.execute(format!("INSERT INTO t VALUES ({next}, {next}, randomblob(300))"))?;
            next += 1;
        }
        conn.execute(format!("UPDATE t SET n = n + 1 WHERE id = {}", 1 + state % next as u64))?;
        conn.execute("COMMIT")?;
        println!("committed {}", next - 1);
        if state % 5 == 0 {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("child") {
        let result = child(&args[2], args[3].parse().unwrap());
        eprintln!("child stopped: {result:?}");
        std::process::exit(2);
    }
    let (dir, rounds) = (&args[1], args[2].parse::<usize>().unwrap());
    let mut state: u64 = args.get(3).and_then(|s| s.parse::<u64>().ok()).unwrap_or(1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    for round in 1..=rounds {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["child", dir, &state.to_string()])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let after = Duration::from_millis(100 + state % 1400);
        std::thread::sleep(after);
        child.kill().unwrap();
        child.wait().unwrap();
        let committed = BufReader::new(child.stdout.take().unwrap())
            .lines()
            .map_while(|l| l.ok())
            .filter_map(|l| l.strip_prefix("committed ").and_then(|n| n.parse::<i64>().ok()))
            .last()
            .unwrap_or(0);
        let verdict = (|| -> Result<String, String> {
            let (_db, conn) = open(&format!("{dir}/reopen.db")).map_err(|e| format!("open: {e}"))?;
            let integrity = rows(&conn, "PRAGMA integrity_check").map_err(|e| format!("integrity_check: {e}"))?;
            if integrity.len() != 1 || integrity[0][0].to_string() != "ok" {
                return Err(format!("integrity_check: {:?}", &integrity[..integrity.len().min(4)]));
            }
            let held = rows(&conn, "SELECT count(*) FROM t").map_err(|e| e.to_string())?.first().and_then(|r| r[0].as_int()).unwrap_or(0);
            if held < committed {
                return Err(format!("{held} rows after the child reported {committed} committed"));
            }
            Ok(format!("{held} rows, {committed} reported committed"))
        })();
        match verdict {
            Ok(summary) => println!("round {round}: killed after {} ms; {summary}", after.as_millis()),
            Err(failure) => {
                println!("round {round}: killed after {} ms; FAILED: {failure}", after.as_millis());
                std::process::exit(1);
            }
        }
    }
    println!("{rounds} rounds passed");
}
