//! Connections writing rows of up to two pages, reading in and out of transactions and
//! checkpointing in every mode, interleaved at random on one thread over in-memory files, each read
//! checked against a model of what its snapshot should see. The same workload as turso_core's
//! `read_changes_tests::reads_beside_writes_and_checkpoints_see_every_commit`, through the public
//! API, so it runs on any revision.
//!
//!     read_changes MODE FIRST_SEED SEEDS [ACTIONS]
//!
//! MODE is wal or mvcc. Prints the first failing read with the 40 actions before it and exits 1.
use std::collections::BTreeMap;
use std::sync::Arc;

use turso_core::{ConnectOpts, Connection, Database, MemoryIO, SqliteDialect, Value};

type Rows = BTreeMap<i64, (i64, usize)>;

fn rows(conn: &Arc<Connection>, sql: &str) -> turso_core::Result<Vec<Vec<Value>>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect());
        Ok(())
    })?;
    Ok(rows)
}

fn body(v: i64, length: usize) -> String {
    let letter = char::from(b'a' + (v % 26) as u8);
    std::iter::repeat_n(letter, length).collect()
}

fn write(conn: &Arc<Connection>, id: i64, v: i64, length: usize) {
    conn.execute(format!(
        "INSERT INTO t VALUES ({id}, {v}, '{}') ON CONFLICT(id) DO UPDATE SET v = excluded.v, body = excluded.body",
        body(v, length)
    ))
    .unwrap();
}

fn read(conn: &Arc<Connection>) -> Result<Rows, String> {
    let mut seen = Rows::new();
    let found = rows(conn, "SELECT id, v, length(body), substr(body, 1, 1), substr(body, -1, 1) FROM t ORDER BY id")
        .map_err(|error| error.to_string())?;
    for row in found {
        let id = row[0].as_int().unwrap();
        let v = row[1].as_int().unwrap();
        let length = row[2].as_int().unwrap() as usize;
        let letter = body(v, 1);
        for end in [&row[3], &row[4]] {
            if end.to_string() != letter {
                return Err(format!("row {id} at version {v} has a body of another version"));
            }
        }
        seen.insert(id, (v, length));
    }
    Ok(seen)
}

fn workload(mvcc: bool, seed: u64, actions: usize) -> Result<usize, String> {
    let db = Database::open_file(Arc::new(MemoryIO::new()), &format!("read-changes-{mvcc}-{seed}.db"), Arc::new(SqliteDialect)).unwrap();
    let setup = db.connect(ConnectOpts::new()).unwrap();
    if mvcc {
        setup.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    setup.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL, body TEXT NOT NULL)").unwrap();
    let conns: Vec<_> = (0..4).map(|_| db.connect(ConnectOpts::new()).unwrap()).collect();
    for conn in &conns {
        conn.execute("PRAGMA cache_size = 64").unwrap();
    }
    let begin = if mvcc { "BEGIN CONCURRENT" } else { "BEGIN" };
    let mut state = 0x9e37_79b9_7f4a_7c15 ^ seed.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1;
    let mut next = move |below: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % below
    };
    let mut model = Rows::new();
    let mut snapshots: Vec<Option<Rows>> = vec![None; conns.len()];
    let mut reads = 0;
    let mut log: Vec<String> = Vec::new();
    let fail = |log: &[String], what: String| -> String { format!("{what}\n{}", log[log.len().saturating_sub(40)..].join("\n")) };
    for action in 0..actions {
        let who = next(conns.len() as u64) as usize;
        let conn = &conns[who];
        let context = format!("mvcc {mvcc}, seed {seed}, action {action}, connection {who}");
        match next(10) {
            0..=3 if snapshots[who].is_none() => {
                let mut changed = model.clone();
                conn.execute(begin).unwrap();
                let changes = if next(10) == 0 { 150 } else { 1 + next(4) };
                for _ in 0..changes {
                    let id = 1 + next(400) as i64;
                    if next(5) == 0 {
                        conn.execute(format!("DELETE FROM t WHERE id = {id}")).unwrap();
                        changed.remove(&id);
                    } else {
                        let v = changed.get(&id).map_or(0, |(v, _)| v + 1);
                        let length = 1 + next(8000) as usize;
                        write(conn, id, v, length);
                        changed.insert(id, (v, length));
                    }
                }
                if next(8) == 0 {
                    conn.execute("ROLLBACK").unwrap();
                    log.push(format!("{action}: {who} rolled back {changes} changes"));
                } else {
                    conn.execute("COMMIT").unwrap();
                    model = changed;
                    log.push(format!("{action}: {who} committed {changes} changes"));
                }
            }
            4..=6 => {
                let expected = snapshots[who].as_ref().unwrap_or(&model);
                let seen = read(conn).map_err(|error| fail(&log, format!("{context}: {error}")))?;
                if &seen != expected {
                    return Err(fail(&log, format!("{context}: read another snapshot")));
                }
                log.push(format!("{action}: {who} read"));
                reads += 1;
            }
            7 => match snapshots[who].take() {
                Some(_) => {
                    conn.execute("COMMIT").unwrap();
                    log.push(format!("{action}: {who} ended its read"));
                }
                None => {
                    conn.execute(begin).unwrap();
                    let seen = read(conn).map_err(|error| fail(&log, format!("{context}: {error}")))?;
                    if seen != model {
                        return Err(fail(&log, format!("{context}: began on another snapshot")));
                    }
                    snapshots[who] = Some(model.clone());
                    log.push(format!("{action}: {who} began a read"));
                }
            },
            8 if snapshots[who].is_none() => {
                let mode = ["PASSIVE", "FULL", "RESTART", "TRUNCATE"][next(4) as usize];
                let done = conn.execute(format!("PRAGMA wal_checkpoint({mode})"));
                log.push(format!("{action}: {who} checkpoint {mode} {done:?}"));
            }
            _ => {}
        }
    }
    Ok(reads)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mvcc = args.get(1).map(String::as_str) == Some("mvcc");
    let first: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    let seeds: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
    let actions: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(400);
    let mut failed = 0;
    for seed in first..first + seeds {
        match workload(mvcc, seed, actions) {
            Ok(reads) => println!("seed {seed}: passed, {reads} reads checked"),
            Err(report) => {
                println!("seed {seed}: FAILED {report}");
                failed += 1;
            }
        }
    }
    println!("{} of {seeds} seeds failed", failed);
    std::process::exit(if failed > 0 { 1 } else { 0 });
}
