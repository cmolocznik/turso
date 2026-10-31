//! Does an MVCC database whose logical log rolls under writes that never pause still open, whole,
//! after the process writing it is killed?
//!
//!     [THRESHOLD=bytes] mvcc_rolling DIR ROUNDS [SEED] [LONGEST_MS]
//!
//! Each round starts a child that opens DIR/roll.db under MVCC with passive checkpoints and the
//! background checkpoint on, at a 64 KB threshold unless THRESHOLD says another, and runs four
//! writers that commit without pausing: inserts of 300-byte rows and an update, each writer in its
//! own range of ids. Every checkpoint sees commits land after its snapshot, so the log rolls.
//! After a random 0.2 s to LONGEST_MS (3,000 unless given) the round kills the child with SIGKILL
//! and opens the database: it must open, pass PRAGMA integrity_check and hold every row of each
//! writer's range up to the last one the writer reported committed. Each round prints the most the
//! log's files held while the child ran. Stops at the first failure, keeping DIR.
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::{ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value};

const WRITERS: usize = 4;
/// Each writer's ids follow its number times this.
const RANGE: i64 = 1 << 40;

fn open(path: &str) -> turso_core::Result<(Arc<Database>, Arc<Connection>)> {
    let io = Arc::new(PlatformIO::new()?);
    let opts = DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true);
    let db = Database::open_file_with_flags(io, path, OpenFlags::default(), opts, None, Arc::new(SqliteDialect))?;
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

fn int(conn: &Arc<Connection>, sql: &str) -> turso_core::Result<i64> {
    Ok(rows(conn, sql)?.first().and_then(|row| row[0].as_int()).unwrap_or(0))
}

/// The bytes the log's files hold.
fn log_bytes(dir: &str) -> u64 {
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().contains(".db-log"))
            .filter_map(|entry| entry.metadata().ok())
            .map(|metadata| metadata.len())
            .sum()
    })
}

/// Commits until killed, printing `committed WRITER ID` after each commit, ID its highest row.
fn writer(db: Arc<Database>, number: usize, seed: u64) -> turso_core::Result<()> {
    let conn = db.connect(ConnectOpts::new())?;
    conn.execute("PRAGMA synchronous = NORMAL")?;
    let base = number as i64 * RANGE;
    let mut next = int(&conn, &format!("SELECT coalesce(max(id), {base}) FROM t WHERE id > {base} AND id <= {}", base + RANGE))? + 1;
    let mut state = seed | 1;
    loop {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let first = next;
        conn.execute("BEGIN CONCURRENT")?;
        for _ in 0..1 + state % 4 {
            conn.execute(format!("INSERT INTO t VALUES ({next}, 0, randomblob(300))"))?;
            next += 1;
        }
        conn.execute(format!("UPDATE t SET n = n + 1 WHERE id = {}", first + (state % (next - first) as u64) as i64))?;
        if let Err(err) = conn.execute("COMMIT") {
            // The rows are not committed: the next transaction takes their ids.
            eprintln!("writer {number}: COMMIT failed: {err}");
            let _ = conn.execute("ROLLBACK");
            next = first;
            continue;
        }
        println!("committed {number} {}", next - 1);
    }
}

fn child(dir: &str, seed: u64) -> turso_core::Result<()> {
    let (db, conn) = open(&format!("{dir}/roll.db"))?;
    if int(&conn, "SELECT count(*) FROM sqlite_schema WHERE name = 't'")? == 0 {
        conn.execute("PRAGMA journal_mode = 'mvcc'")?;
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL, body BLOB NOT NULL)")?;
        if std::env::var("INDEX").is_ok_and(|v| v == "1") {
            conn.execute("CREATE INDEX t_n ON t(n)")?;
        }
    }
    if std::env::var("BACKGROUND_CHECKPOINT").map_or(true, |v| v != "0") {
        conn.execute("PRAGMA mvcc_background_checkpoint = 1")?;
    }
    let threshold = std::env::var("THRESHOLD").map_or(65536, |value| value.parse::<i64>().expect("THRESHOLD is a byte count"));
    conn.execute(format!("PRAGMA mvcc_checkpoint_threshold = {threshold}"))?;
    let writers: Vec<_> = (0..WRITERS)
        .map(|number| {
            let db = db.clone();
            std::thread::spawn(move || writer(db, number, seed.wrapping_add(number as u64)))
        })
        .collect();
    // A writer that stops is a failure: the round finds the child gone before it is killed.
    for writer in writers {
        let stopped = writer.join();
        eprintln!("writer stopped: {stopped:?}");
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("child") {
        let stopped = child(&args[2], args[3].parse().unwrap());
        eprintln!("child stopped: {stopped:?}");
        std::process::exit(2);
    }
    let (dir, rounds) = (&args[1], args[2].parse::<usize>().unwrap());
    let mut state: u64 = args.get(3).and_then(|s| s.parse::<u64>().ok()).unwrap_or(1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let longest = args.get(4).and_then(|s| s.parse::<u64>().ok()).unwrap_or(3000).max(201);
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let stderr = std::fs::File::create(format!("{dir}/child.stderr")).unwrap();
    let mut most_of_all = 0;
    for round in 1..=rounds {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["child", dir, &state.to_string()])
            .stdout(std::process::Stdio::piped())
            .stderr(stderr.try_clone().unwrap())
            .spawn()
            .unwrap();
        // Read as the child writes, or a full pipe stops its writers.
        let stdout = child.stdout.take().unwrap();
        let reports = std::thread::spawn(move || {
            let mut committed = [0i64; WRITERS];
            for line in BufReader::new(stdout).lines().map_while(|line| line.ok()) {
                let mut fields = line.split(' ');
                if fields.next() != Some("committed") {
                    continue;
                }
                let (Some(Ok(number)), Some(Ok(id))) = (fields.next().map(str::parse::<usize>), fields.next().map(str::parse::<i64>)) else {
                    continue;
                };
                committed[number] = committed[number].max(id);
            }
            committed
        });
        let after = Duration::from_millis(200 + state % (longest - 200));
        let started = Instant::now();
        let mut most = 0;
        let mut exited = None;
        while started.elapsed() < after {
            most = most.max(log_bytes(dir));
            if let Some(status) = child.try_wait().unwrap() {
                exited = Some(status);
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        if exited.is_none() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
        most_of_all = most_of_all.max(most);
        let committed = reports.join().unwrap();
        let verdict = (|| -> Result<String, String> {
            if let Some(status) = exited {
                return Err(format!("the child stopped before it was killed: {status}; see {dir}/child.stderr"));
            }
            let (_db, conn) = open(&format!("{dir}/roll.db")).map_err(|e| format!("open: {e}"))?;
            let integrity = rows(&conn, "PRAGMA integrity_check").map_err(|e| format!("integrity_check: {e}"))?;
            if integrity.len() != 1 || integrity[0][0].to_string() != "ok" {
                return Err(format!("integrity_check: {:?}", &integrity[..integrity.len().min(4)]));
            }
            let mut reported = 0;
            for (number, &last) in committed.iter().enumerate() {
                let base = number as i64 * RANGE;
                if last == 0 {
                    continue;
                }
                // A writer's ids run without a gap from its base, across rounds.
                let held = int(&conn, &format!("SELECT count(*) FROM t WHERE id > {base} AND id <= {last}")).map_err(|e| e.to_string())?;
                if held != last - base {
                    return Err(format!("writer {number} reported rows to {} committed and {held} of them are held", last - base));
                }
                reported += last - base;
            }
            let total = int(&conn, "SELECT count(*) FROM t").map_err(|e| e.to_string())?;
            Ok(format!("{total} rows, {reported} reported committed; the log's files held at most {most} bytes"))
        })();
        match verdict {
            Ok(summary) => println!("round {round}: killed after {} ms; {summary}", after.as_millis()),
            Err(failure) => {
                println!("round {round}: killed after {} ms; FAILED: {failure}", after.as_millis());
                std::process::exit(1);
            }
        }
    }
    println!("{rounds} rounds passed; the log's files held at most {most_of_all} bytes");
}
