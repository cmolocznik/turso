//! The ivf_mvcc workload's shape with no index method, against the fork with or without its
//! fixes: writers insert, revise and delete rows
//! through BEGIN CONCURRENT, a maintainer rewrites batches of 64 existing rows the same way, and a
//! reader scans throughout. If this hangs, the hang is Turso's MVCC, not ivf_rq.
//!
//!     mvcc_control DIR WRITERS ROWS_PER_WRITER SECONDS_BEFORE_DECLARING_A_HANG
//!
//! BACKGROUND_CHECKPOINT=0 makes the commit that crosses the checkpoint threshold checkpoint
//! inline, as stock Turso does.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Each COMMIT's start, in microseconds since the run began, and how long it took.
type Commits = Mutex<Vec<(u64, u64)>>;

static RUN_START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize] as f64 / 1000.0
}

/// Errors after which the whole transaction is retried: write conflicts, a busy or locked
/// database, a stale snapshot, and a cascading abort of a commit dependency.
fn retryable(e: &turso::Error) -> bool {
    let text = e.to_string().to_lowercase();
    text.contains("conflict") || text.contains("busy") || text.contains("locked") || text.contains("snapshot") || text.contains("dependency aborted")
}

/// Waits before a conflicted transaction is retried, a growing jittered interval up to `ceiling_ms`.
/// A conflict lasts until the transaction holding the row ends, and a retry that never suspends
/// keeps its worker thread, starving the tasks that thread woke: the holder's own commit among them.
async fn back_off(attempt: u32, ceiling_ms: u64) {
    if attempt <= 2 {
        tokio::task::yield_now().await;
        return;
    }
    let ceiling = (1u64 << attempt.min(7)).min(ceiling_ms).max(1);
    let jitter = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.subsec_nanos() as u64) % ceiling;
    tokio::time::sleep(std::time::Duration::from_millis(1 + jitter)).await;
}

async fn concurrent(db: &turso::Connection, statements: &[String], retries: &AtomicUsize, ceiling_ms: u64, commits: &Commits) -> anyhow::Result<()> {
    let mut attempt = 0u32;
    loop {
        db.execute("BEGIN CONCURRENT", ()).await?;
        let mut failed = None;
        for sql in statements {
            if let Err(e) = db.execute(sql, ()).await {
                failed = Some(e);
                break;
            }
        }
        let outcome = match failed {
            None => {
                let started = std::time::Instant::now();
                let committed = db.execute("COMMIT", ()).await.map(|_| ());
                if committed.is_ok() {
                    let at = started.duration_since(*RUN_START.get_or_init(std::time::Instant::now)).as_micros() as u64;
                    commits.lock().unwrap().push((at, started.elapsed().as_micros() as u64));
                }
                committed
            }
            Some(e) => Err(e),
        };
        match outcome {
            Ok(()) => {
                // A Turso call completes inside one poll when it has no IO to wait for, so a task
                // that never yields keeps its worker and starves the tasks that worker woke.
                tokio::task::yield_now().await;
                return Ok(());
            }
            Err(e) if retryable(&e) => {
                let _ = db.execute("ROLLBACK", ()).await;
                retries.fetch_add(1, Ordering::Relaxed);
                attempt += 1;
                back_off(attempt, ceiling_ms).await;
            }
            Err(e) => {
                let _ = db.execute("ROLLBACK", ()).await;
                return Err(e.into());
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let (dir, writers, rows, patience): (&str, usize, usize, u64) = (&args[1], args[2].parse()?, args[3].parse()?, args[4].parse()?);
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir)?;
    let database = turso::Builder::new_local(&format!("{dir}/control.db"))
        .experimental_mvcc_passive_checkpoint(std::env::var_os("BLOCKING_CHECKPOINT").is_none())
        .build()
        .await?;
    RUN_START.get_or_init(std::time::Instant::now);
    let setup = database.connect()?;
    {
        let mut r = setup.query("PRAGMA journal_mode = 'mvcc'", ()).await?;
        while r.next().await?.is_some() {}
    }
    if std::env::var("BACKGROUND_CHECKPOINT").is_ok_and(|v| v == "0") {
        setup.execute("PRAGMA mvcc_background_checkpoint = 0", ()).await?;
    }
    if let Ok(bytes) = std::env::var("CHECKPOINT_BYTES") {
        let mut r = setup.query(&format!("PRAGMA mvcc_checkpoint_threshold = {bytes}"), ()).await?;
        while r.next().await?.is_some() {}
    }
    setup.execute_batch("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL); CREATE TABLE side(id INTEGER PRIMARY KEY, x BLOB NOT NULL);").await?;
    let (stop, progress) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)));
    let (writer_retries, upkeep_retries) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (writer_commits, upkeep_commits) = (Arc::new(Commits::default()), Arc::new(Commits::default()));
    let total = writers * rows;
    let maintainer = {
        let (db, stop, retries, progress, commits) = (database.connect()?, stop.clone(), upkeep_retries.clone(), progress.clone(), upkeep_commits.clone());
        tokio::spawn(async move {
            let mut n = 0usize;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                // Rewrite 64 existing rows and move a side row, as a pack or a move does.
                let ids: Vec<String> = (0..64).map(|k| ((n * 64 + k) * 7919 % total + 1).to_string()).collect();
                let statements = vec![
                    format!("UPDATE v SET e = e WHERE id IN ({})", ids.join(",")),
                    format!("DELETE FROM side WHERE id = {}", n % 97),
                    format!("INSERT INTO side VALUES ({}, randomblob(600)) ON CONFLICT(id) DO UPDATE SET x = excluded.x", n % 97),
                ];
                concurrent(&db, &statements, &retries, 100, &commits).await?;
                progress.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            anyhow::Ok(())
        })
    };
    let reader = {
        let (db, stop) = (database.connect()?, stop.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                // A read that saw a commit still in flight aborts with it, so it retries as a
                // conflicted write does.
                let mut attempt = 0u32;
                loop {
                    let read = async {
                        let mut r = db.query("SELECT count(*), sum(length(e)) FROM v", ()).await?;
                        while r.next().await?.is_some() {}
                        turso::Result::Ok(())
                    };
                    match read.await {
                        Ok(()) => break,
                        Err(e) if retryable(&e) => {
                            attempt += 1;
                            back_off(attempt, 8).await;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                tokio::task::yield_now().await;
            }
            anyhow::Ok(())
        })
    };
    let mut tasks = Vec::new();
    for w in 0..writers {
        let (db, retries, progress, commits) = (database.connect()?, writer_retries.clone(), progress.clone(), writer_commits.clone());
        tasks.push(tokio::spawn(async move {
            let id = |n: usize| w * rows + n + 1;
            for start in (0..rows).step_by(16) {
                let end = (start + 16).min(rows);
                let batch: Vec<String> = (start..end).map(|n| format!("INSERT INTO v VALUES ({}, randomblob(512))", id(n))).collect();
                concurrent(&db, &batch, &retries, 8, &commits).await?;
                let mut changes = Vec::new();
                for n in start..end {
                    if n % 5 == 0 {
                        changes.push(format!("UPDATE v SET e = randomblob(512) WHERE id = {}", id(n)));
                    }
                    if n % 7 == 0 {
                        changes.push(format!("DELETE FROM v WHERE id = {}", id(n)));
                    }
                }
                if !changes.is_empty() {
                    concurrent(&db, &changes, &retries, 8, &commits).await?;
                }
                progress.fetch_add(1, Ordering::Relaxed);
            }
            anyhow::Ok(())
        }));
    }
    // A hang is a stretch of `patience` seconds in which no write transaction finishes; reads do
    // not count, so a reader that keeps going cannot hide a starved writer.
    let watch = {
        let progress = progress.clone();
        tokio::spawn(async move {
            let mut last = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(patience)).await;
                let now = progress.load(Ordering::Relaxed);
                if now == last {
                    return true;
                }
                last = now;
            }
        })
    };
    let writes = async {
        for t in tasks {
            t.await??;
        }
        anyhow::Ok(())
    };
    tokio::select! {
        r = writes => r?,
        _ = watch => {
            if let Some(path) = std::env::var_os("STACK_ON_HANG") {
                let out = std::process::Command::new("eu-stack").arg("-p").arg(std::process::id().to_string()).output();
                if let Ok(out) = out {
                    let _ = std::fs::write(&path, [out.stdout, out.stderr].concat());
                }
            }
            println!("{}", serde_json::json!({"hang": true, "writer_retries": writer_retries.load(Ordering::Relaxed), "upkeep_retries": upkeep_retries.load(Ordering::Relaxed)}));
            std::process::exit(3);
        }
    }
    stop.store(true, Ordering::Relaxed);
    maintainer.await??;
    reader.await??;
    // The checkpoint that truncates a log kept under load runs once writes pause.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let log_bytes = std::fs::metadata(format!("{dir}/control.db-log")).map_or(0, |m| m.len());
    let mut slowest = writer_commits.lock().unwrap().clone();
    slowest.sort_unstable_by_key(|&(_, took)| std::cmp::Reverse(took));
    slowest.truncate(5);
    let slowest: Vec<(f64, f64)> = slowest.iter().map(|&(at, took)| (at as f64 / 1000.0, took as f64 / 1000.0)).collect();
    let mut writes: Vec<u64> = writer_commits.lock().unwrap().iter().map(|&(_, took)| took).collect();
    writes.sort_unstable();
    let mut upkeep: Vec<u64> = upkeep_commits.lock().unwrap().iter().map(|&(_, took)| took).collect();
    upkeep.sort_unstable();
    println!(
        "{}",
        serde_json::json!({
            "hang": false,
            "writer_retries": writer_retries.load(Ordering::Relaxed),
            "upkeep_retries": upkeep_retries.load(Ordering::Relaxed),
            "writer_commits": writes.len(),
            "writer_commit_ms_p50": percentile(&writes, 0.5),
            "writer_commit_ms_p99": percentile(&writes, 0.99),
            "writer_commit_ms_max": percentile(&writes, 1.0),
            "writer_commit_slowest_at_ms": slowest,
            "upkeep_commit_ms_p99": percentile(&upkeep, 0.99),
            "upkeep_commit_ms_max": percentile(&upkeep, 1.0),
            "log_bytes_after_pause": log_bytes,
        })
    );
    Ok(())
}
