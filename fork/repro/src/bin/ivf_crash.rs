//! Does an ivf_rq index still agree with its table after the process writing them is killed?
//!
//!     ivf_crash run DIR MODE ROUNDS [SEED]
//!
//! Each round starts a child that opens DIR/crash.db (MODE wal or mvcc, synchronous NORMAL) and
//! writes to it until killed: inserts, updates and deletes, alone and in transactions it commits or
//! rolls back, OPTIMIZE INDEX in steps, and checkpoints. After a random 0.2 to 2.5 s the round
//! kills it with SIGKILL, opens the database, recovers it, and checks it: PRAGMA integrity_check,
//! `ivf_rq::check` (every row's code where the index says, coded from its vector), and the top ten
//! of three queries, every candidate reranked, against a brute-force scan of the table. Then it
//! checkpoints the database with TRUNCATE, and the next round's child writes on from there. Prints
//! a line a round and stops at the first failure, keeping DIR.
//!
//! THREADS=n runs n writers on their own connections and threads, BEGIN CONCURRENT under MVCC (with
//! CONCURRENT=1, each lone statement in a transaction of its own too), beside a thread running
//! OPTIMIZE INDEX over and over; a statement or transaction that fails
//! (busy, a conflict) is rolled back and counted. The child reports its commits and failures, the
//! OPTIMIZE INDEX statements among them that failed, and the rebuilds the index made; when
//! it has run 2 s without a commit the round saves its threads' stacks (eu-stack) to
//! DIR/stall-ROUND.txt before the kill, and the run reports how many rounds stalled.
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::index_method::ivf_rq;
use turso_core::{ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value};

const DIMS: usize = 16;

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn unit(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 24) as f32 - 0.5
    }
}

fn vector(random: &mut Random) -> Vec<f32> {
    // Near one of eight centres, so the index trains and splits partitions.
    let centre = random.below(8) as f32;
    (0..DIMS).map(|i| ((centre + i as f32) * 0.7).sin() * 3.0 + random.unit()).collect()
}

fn literal(v: &[f32]) -> String {
    format!("vector32('[{}]')", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","))
}

fn open(path: &str) -> turso_core::Result<(Arc<Database>, Arc<Connection>)> {
    let io = Arc::new(PlatformIO::new()?);
    let db = Database::open_file_with_flags(io, path, OpenFlags::default(), DatabaseOpts::new().with_index_method(true), None, Arc::new(SqliteDialect))?;
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

/// Write until killed, on `THREADS` threads.
fn threaded(dir: &str, mvcc: bool, seed: u64, threads: usize) -> turso_core::Result<()> {
    let (db, conn) = open(&format!("{dir}/crash.db"))?;
    conn.execute("PRAGMA synchronous = NORMAL")?;
    if rows(&conn, "SELECT count(*) FROM sqlite_schema WHERE name = 'v'")?[0][0].as_int() == Some(0) {
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'")?;
        }
        conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)")?;
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 100000, refine = 4)")?;
    }
    let next = Arc::new(AtomicI64::new(rows(&conn, "SELECT coalesce(max(id), 0) FROM v")?[0][0].as_int().unwrap_or(0) + 1));
    let (commits, failures, optimizes) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let conflicts = Arc::new(AtomicU64::new(0));
    let optimize_failures = Arc::new(AtomicU64::new(0));
    // The conflicts by what met them: an insert, update or delete statement, or a COMMIT.
    let met: Arc<[AtomicU64; 4]> = Arc::new(Default::default());
    let begin = if mvcc { "BEGIN CONCURRENT" } else { "BEGIN" };
    // CONCURRENT=1 runs each lone statement in a `BEGIN CONCURRENT` transaction of its own under
    // MVCC, as no autocommit write takes the store's exclusive lock.
    let concurrent = mvcc && std::env::var_os("CONCURRENT").is_some();
    for t in 0..threads {
        let (db, next, commits, failures, conflicts, met) = (db.clone(), next.clone(), commits.clone(), failures.clone(), conflicts.clone(), met.clone());
        std::thread::spawn(move || {
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.set_busy_timeout(Duration::from_secs(10));
            conn.execute("PRAGMA synchronous = NORMAL").unwrap();
            let mut random = Random((seed ^ (t as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)) | 1);
            let tally = |kind: usize, done: turso_core::Result<()>| {
                if matches!(done, Err(turso_core::LimboError::WriteWriteConflict)) {
                    met[kind].fetch_add(1, Ordering::Relaxed);
                }
                done
            };
            let one = |random: &mut Random| -> turso_core::Result<()> {
                let top = next.load(Ordering::Relaxed) as u64;
                match random.below(10) {
                    0..=4 => {
                        let n = 1 + random.below(20) as i64;
                        let first = next.fetch_add(n, Ordering::Relaxed);
                        let values: Vec<String> = (0..n).map(|i| format!("({}, {})", first + i, literal(&vector(random)))).collect();
                        tally(0, conn.execute(format!("INSERT INTO v VALUES {}", values.join(","))))
                    }
                    5..=7 => tally(1, conn.execute(format!("UPDATE v SET e = {} WHERE id = {}", literal(&vector(random)), 1 + random.below(top)))),
                    _ => tally(2, conn.execute(format!("DELETE FROM v WHERE id = {}", 1 + random.below(top)))),
                }
            };
            loop {
                let done = match random.below(10) {
                    0..=7 if concurrent => (|| {
                        conn.execute(begin)?;
                        one(&mut random)?;
                        tally(3, conn.execute("COMMIT"))
                    })(),
                    0..=7 => one(&mut random),
                    _ => (|| {
                        conn.execute(begin)?;
                        for _ in 0..2 + random.below(5) {
                            one(&mut random)?;
                        }
                        tally(3, conn.execute(if random.below(6) == 0 { "ROLLBACK" } else { "COMMIT" }))
                    })(),
                };
                match done {
                    Ok(()) => commits.fetch_add(1, Ordering::Relaxed),
                    Err(e) => {
                        if !conn.get_auto_commit() {
                            let _ = conn.execute("ROLLBACK");
                        }
                        if matches!(e, turso_core::LimboError::WriteWriteConflict) {
                            conflicts.fetch_add(1, Ordering::Relaxed);
                        }
                        failures.fetch_add(1, Ordering::Relaxed)
                    }
                };
            }
        });
    }
    {
        let (db, optimizes, failures, optimize_failures) = (db.clone(), optimizes.clone(), failures.clone(), optimize_failures.clone());
        std::thread::spawn(move || {
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.set_busy_timeout(Duration::from_secs(10));
            loop {
                match conn.execute("OPTIMIZE INDEX v_ivf") {
                    Ok(()) => optimizes.fetch_add(1, Ordering::Relaxed),
                    Err(_) => {
                        optimize_failures.fetch_add(1, Ordering::Relaxed);
                        failures.fetch_add(1, Ordering::Relaxed)
                    }
                };
                std::thread::sleep(Duration::from_millis(20));
            }
        });
    }
    loop {
        std::thread::sleep(Duration::from_millis(200));
        eprintln!(
            "progress commits={} failures={} conflicts={} (insert {}, update {}, delete {}, commit {}) optimizes={} redone={} optimize_failures={} rebuilds={}",
            commits.load(Ordering::Relaxed),
            failures.load(Ordering::Relaxed),
            conflicts.load(Ordering::Relaxed),
            met[0].load(Ordering::Relaxed),
            met[1].load(Ordering::Relaxed),
            met[2].load(Ordering::Relaxed),
            met[3].load(Ordering::Relaxed),
            optimizes.load(Ordering::Relaxed),
            turso_core::index_method::ivf_rq::REDONE.load(Ordering::Relaxed),
            optimize_failures.load(Ordering::Relaxed),
            turso_core::index_method::ivf_rq::REBUILDS.load(Ordering::Relaxed)
        );
    }
}

/// Write until killed.
fn child(dir: &str, mvcc: bool, seed: u64) -> turso_core::Result<()> {
    let threads: usize = std::env::var("THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    if threads > 1 {
        return threaded(dir, mvcc, seed, threads);
    }
    let (_db, conn) = open(&format!("{dir}/crash.db"))?;
    conn.execute("PRAGMA synchronous = NORMAL")?;
    let fresh = rows(&conn, "SELECT count(*) FROM sqlite_schema WHERE name = 'v'")?[0][0].as_int() == Some(0);
    if fresh {
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'")?;
        }
        conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)")?;
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 100000, refine = 4)")?;
    }
    let begin = if mvcc { "BEGIN CONCURRENT" } else { "BEGIN" };
    // For narrowing a failure down: leave out transactions, their rollbacks, OPTIMIZE INDEX or checkpoints.
    let off = |name: &str| std::env::var_os(name).is_some();
    let (no_transactions, no_rollbacks, no_optimize, no_checkpoints) = (off("NO_TRANSACTIONS"), off("NO_ROLLBACKS"), off("NO_OPTIMIZE"), off("NO_CHECKPOINTS"));
    let mut random = Random(seed | 1);
    let mut next = rows(&conn, "SELECT coalesce(max(id), 0) FROM v")?[0][0].as_int().unwrap_or(0) + 1;
    loop {
        let roll = random.below(100);
        let mut one = |random: &mut Random, next: &mut i64| -> turso_core::Result<()> {
            match random.below(10) {
                0..=4 => {
                    let n = 1 + random.below(20) as i64;
                    let values: Vec<String> = (0..n).map(|i| format!("({}, {})", *next + i, literal(&vector(random)))).collect();
                    *next += n;
                    conn.execute(format!("INSERT INTO v VALUES {}", values.join(",")))
                }
                5..=7 => conn.execute(format!("UPDATE v SET e = {} WHERE id = {}", literal(&vector(random)), 1 + random.below(*next as u64))),
                _ => conn.execute(format!("DELETE FROM v WHERE id = {}", 1 + random.below(*next as u64))),
            }
        };
        match roll {
            0..=79 => one(&mut random, &mut next)?,
            80..=91 if !no_transactions => {
                conn.execute(begin)?;
                for _ in 0..2 + random.below(5) {
                    one(&mut random, &mut next)?;
                }
                conn.execute(if random.below(6) == 0 && !no_rollbacks { "ROLLBACK" } else { "COMMIT" })?;
            }
            92..=96 if !no_optimize => conn.execute("OPTIMIZE INDEX v_ivf")?,
            80..=96 => one(&mut random, &mut next)?,
            _ if no_checkpoints => one(&mut random, &mut next)?,
            _ => {
                let mode = ["PASSIVE", "TRUNCATE"][random.below(2) as usize];
                let _ = conn.execute(format!("PRAGMA wal_checkpoint({mode})"));
            }
        }
    }
}

/// Recover the database and check it; a description of the first thing wrong, if any.
fn verify(dir: &str, random: &mut Random) -> Result<String, String> {
    let (db, conn) = open(&format!("{dir}/crash.db")).map_err(|e| format!("open: {e}"))?;
    let integrity = rows(&conn, "PRAGMA integrity_check").map_err(|e| format!("integrity_check: {e}"))?;
    if integrity.len() != 1 || integrity[0][0].to_string() != "ok" {
        return Err(format!("integrity_check: {integrity:?}"));
    }
    let tables = rows(&conn, "SELECT count(*) FROM sqlite_schema WHERE name = 'v_ivf'").map_err(|e| e.to_string())?;
    if tables[0][0].as_int() == Some(0) {
        return Ok("killed before the index was created".to_string());
    }
    let found = ivf_rq::check(&conn, "v_ivf").map_err(|e| format!("check: {e}"))?;
    if !found.consistent() {
        return Err(format!("check: {} findings: {:#?}", found.findings_total, found.findings));
    }
    let table: Vec<(i64, Vec<f32>)> = rows(&conn, "SELECT id, e FROM v")
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|row| {
            let blob = row[1].to_blob().unwrap();
            (row[0].as_int().unwrap(), blob.chunks_exact(4).map(|b| f32::from_le_bytes(b.try_into().unwrap())).collect())
        })
        .collect();
    let by_id: std::collections::HashMap<i64, &Vec<f32>> = table.iter().map(|(id, v)| (*id, v)).collect();
    for _ in 0..3 {
        let q = vector(random);
        let distance = |v: &Vec<f32>| -v.iter().zip(&q).map(|(a, b)| (*a as f64) * (*b as f64)).sum::<f64>();
        let mut exact: Vec<(f64, i64)> = table.iter().map(|(id, v)| (distance(v), *id)).collect();
        exact.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let want: Vec<i64> = exact.iter().take(10).map(|(_, id)| *id).collect();
        let got: Vec<i64> = rows(&conn, &format!("SELECT id, vector_distance_dot(e, {}) AS d FROM v ORDER BY d LIMIT 10", literal(&q)))
            .map_err(|e| format!("query: {e}"))?
            .iter()
            .map(|row| row[0].as_int().unwrap())
            .collect();
        // Every candidate is reranked by its f32 distance, so the answer is the exact ten up to rows
        // whose distances tie with the tenth within f32 rounding.
        let tenth = exact.get(want.len().saturating_sub(1)).map_or(0.0, |e| e.0);
        let tied = |id: &i64| by_id.get(id).is_some_and(|v| distance(v) <= tenth + 1e-5 * tenth.abs().max(1.0));
        let distinct: std::collections::HashSet<&i64> = got.iter().collect();
        if got != want && !(got.len() == want.len() && distinct.len() == got.len() && got.iter().all(tied)) {
            return Err(format!("top ten {got:?}, exact {want:?}"));
        }
    }
    let summary = format!("{} rows, {} partitions, {} blocks, {} tail entries, {} stale copies, {} codes checked", found.rows, found.partitions, found.blocks, found.tail_entries, found.stale_copies, found.codes);
    let truncated = rows(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").map_err(|e| format!("checkpoint: {e}"))?;
    if truncated.first().and_then(|row| row[0].as_int()) != Some(0) {
        return Err(format!("checkpoint: TRUNCATE with no other connection answered {truncated:?}"));
    }
    drop(conn);
    drop(db);
    Ok(summary)
}

/// Top-ten recall of `queries` random queries over DIR's database, as the index is created and with
/// more candidates reranked: a miss that more candidates mend is the estimates' cut, not a lost row.
/// OPTIMIZE INDEX on a round's database alone, after the check: its shape before and after, and the
/// time it took.
fn optimize(dir: &str) -> turso_core::Result<()> {
    let (_db, conn) = open(&format!("{dir}/crash.db"))?;
    let shape = |conn: &Arc<Connection>| -> turso_core::Result<String> {
        let found = ivf_rq::check(conn, "v_ivf")?;
        Ok(format!("{} rows, {} partitions, {} blocks, {} tail entries, {} stale copies, {} pending, consistent {}", found.rows, found.partitions, found.blocks, found.tail_entries, found.stale_copies, found.pending, found.consistent()))
    };
    println!("before: {}", shape(&conn)?);
    let started = Instant::now();
    conn.execute("OPTIMIZE INDEX v_ivf")?;
    println!("OPTIMIZE INDEX took {:.1} s", started.elapsed().as_secs_f64());
    println!("after: {}", shape(&conn)?);
    Ok(())
}

fn diagnose(dir: &str, queries: usize) {
    let (_db, conn) = open(&format!("{dir}/crash.db")).unwrap();
    let table: Vec<(i64, Vec<f32>)> = rows(&conn, "SELECT id, e FROM v")
        .unwrap()
        .into_iter()
        .map(|row| (row[0].as_int().unwrap(), row[1].to_blob().unwrap().chunks_exact(4).map(|b| f32::from_le_bytes(b.try_into().unwrap())).collect()))
        .collect();
    let mut random = Random(7);
    for refine in [0usize, 16, 64] {
        ivf_rq::REFINE_OVERRIDE.store(refine, std::sync::atomic::Ordering::Relaxed);
        let (mut hits, mut misses) = (0, 0);
        let mut state = Random(7);
        for _ in 0..queries {
            let q = vector(&mut state);
            let mut exact: Vec<(f64, i64)> = table.iter().map(|(id, v)| (-v.iter().zip(&q).map(|(a, b)| (*a as f64) * (*b as f64)).sum::<f64>(), *id)).collect();
            exact.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let want: std::collections::HashSet<i64> = exact.iter().take(10).map(|(_, id)| *id).collect();
            let got: Vec<i64> = rows(&conn, &format!("SELECT id, vector_distance_dot(e, {}) AS d FROM v ORDER BY d LIMIT 10", literal(&q))).unwrap().iter().map(|r| r[0].as_int().unwrap()).collect();
            let found = got.iter().filter(|id| want.contains(id)).count();
            hits += found;
            misses += 10 - found;
        }
        let _ = &mut random;
        println!("{} rows, refine {}: {hits} of {} found, {misses} missed", table.len(), if refine == 0 { "as created (4)".to_string() } else { refine.to_string() }, hits + misses);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("child") => {
            let result = child(&args[2], args[3] == "mvcc", args[4].parse().unwrap());
            eprintln!("child stopped: {result:?}");
            std::process::exit(2);
        }
        Some("diagnose") => {
            diagnose(&args[2], args.get(3).and_then(|s| s.parse().ok()).unwrap_or(50));
            return;
        }
        Some("optimize") => {
            optimize(&args[2]).unwrap();
            return;
        }
        Some("run") => {}
        _ => {
            eprintln!("usage: ivf_crash run DIR MODE ROUNDS [SEED]");
            std::process::exit(1);
        }
    }
    let (dir, mode, rounds) = (&args[2], &args[3], args[4].parse::<usize>().unwrap());
    let seed: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(1);
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    // Rerank every candidate: the check is that every row is reachable, not the estimates' cut.
    ivf_rq::REFINE_OVERRIDE.store(1_000_000, Ordering::Relaxed);
    let started = Instant::now();
    let mut stalled = Vec::new();
    for round in 1..=rounds {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["child", dir, mode, &random.next().to_string()])
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        // The child's stderr, read as it runs, so a stall is seen before the kill.
        let err = Arc::new(std::sync::Mutex::new(String::new()));
        let reader = {
            let (err, stderr) = (err.clone(), child.stderr.take().unwrap());
            std::thread::spawn(move || {
                use std::io::BufRead;
                for line in std::io::BufReader::new(stderr).lines().map_while(|l| l.ok()) {
                    let mut err = err.lock().unwrap();
                    err.push_str(&line);
                    err.push('\n');
                }
            })
        };
        let after = Duration::from_millis(200 + random.below(2300));
        std::thread::sleep(after);
        if let Ok(Some(status)) = child.try_wait() {
            reader.join().unwrap();
            println!("round {round}: FAILED: the child ended by itself ({status}): {}", err.lock().unwrap());
            std::process::exit(1);
        }
        let quiet = {
            let err = err.lock().unwrap();
            let lines: Vec<&str> = err.lines().filter(|l| l.starts_with("progress")).collect();
            lines.len() >= 10 && lines.iter().all(|l| l.contains("commits=0 "))
        };
        if quiet {
            let stacks = std::process::Command::new("eu-stack").args(["-p", &child.id().to_string()]).output();
            let path = format!("{dir}/stall-{round}.txt");
            let _ = std::fs::write(&path, stacks.map(|o| o.stdout).unwrap_or_default());
            stalled.push(round);
        }
        child.kill().unwrap();
        child.wait().unwrap();
        reader.join().unwrap();
        let err = err.lock().unwrap().clone();
        let mut progress = err.lines().rev().find(|l| l.starts_with("progress")).unwrap_or("").to_string();
        if quiet {
            progress.push_str(&format!(" (no commit in 2 s: stacks in {dir}/stall-{round}.txt)"));
        }
        match verify(dir, &mut random) {
            Ok(summary) => println!("round {round}: killed after {} ms; {progress}; {summary}", after.as_millis()),
            Err(failure) => {
                println!("round {round}: killed after {} ms; FAILED: {failure}", after.as_millis());
                std::process::exit(1);
            }
        }
    }
    println!("{rounds} rounds passed in {:.0} s; {} stalled without a commit for 2 s: {stalled:?}", started.elapsed().as_secs_f64(), stalled.len());
}
