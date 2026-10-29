//! Does one slow sync stall more than the commit that issued it? The mvcc_inline workload (writers
//! inserting, revising and deleting through BEGIN CONCURRENT, a maintainer rewriting batches of 64
//! rows, a reader scanning) over in-memory files, where the first sync of the file named by
//! SLOW_FILE after ARM_MS blocks its thread for SLOW_MS, as an fsync on a stalled disk does. Prints
//! the stall and, for each task, the commits it finished while the stall held and its longest
//! commit.
//!
//!     slow_sync WRITERS ROWS_PER_WRITER
//!
//! SLOW_FILE is a file-name suffix (default "-wal": an inline checkpoint's WAL sync; "-log": a
//! commit's logical-log sync). CHECKPOINT_BYTES sets the auto-checkpoint threshold (default 262144).
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use turso_core::io::FileSyncType;
use turso_core::{Buffer, Clock, Completion, File, MemoryIO, MonotonicInstant, OpenFlags, WallClockInstant, IO};

static RUN_START: OnceLock<Instant> = OnceLock::new();

fn now_us() -> u64 {
    RUN_START.get_or_init(Instant::now).elapsed().as_micros() as u64
}

/// The one stall: armed by the workload, taken by the first matching sync, and its span.
struct Stall {
    suffix: String,
    delay: Duration,
    armed: AtomicUsize,
    started_us: AtomicU64,
    ended_us: AtomicU64,
}

struct SlowSyncIo {
    inner: Arc<MemoryIO>,
    stall: Arc<Stall>,
}

struct SlowFile {
    inner: Arc<dyn File>,
    stall: Option<Arc<Stall>>,
}

impl Clock for SlowSyncIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for SlowSyncIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> turso_core::Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        let stall = path.ends_with(&self.stall.suffix).then(|| self.stall.clone());
        Ok(Arc::new(SlowFile { inner, stall }))
    }
    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }
    fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
        self.inner.file_id(path)
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }
    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }
}

impl File for SlowFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pwritev(pos, buffers, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        if let Some(stall) = &self.stall {
            if stall.armed.compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                stall.started_us.store(now_us(), Ordering::Release);
                // Blocks the calling thread, as a synchronous fsync on a stalled device does.
                std::thread::sleep(stall.delay);
                stall.ended_us.store(now_us(), Ordering::Release);
            }
        }
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

/// Each finished COMMIT's start and end, in microseconds since the run began.
type Commits = Mutex<Vec<(u64, u64)>>;

fn retryable(e: &turso::Error) -> bool {
    let text = e.to_string().to_lowercase();
    text.contains("conflict") || text.contains("busy") || text.contains("locked") || text.contains("snapshot") || text.contains("dependency aborted")
}

async fn back_off(attempt: u32, ceiling_ms: u64) {
    if attempt <= 2 {
        tokio::task::yield_now().await;
        return;
    }
    let ceiling = (1u64 << attempt.min(7)).min(ceiling_ms).max(1);
    let jitter = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.subsec_nanos() as u64) % ceiling;
    tokio::time::sleep(Duration::from_millis(1 + jitter)).await;
}

async fn concurrent(db: &turso::Connection, statements: &[String], ceiling_ms: u64, commits: &Commits) -> anyhow::Result<()> {
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
                let started = now_us();
                let committed = db.execute("COMMIT", ()).await.map(|_| ());
                if committed.is_ok() {
                    commits.lock().unwrap().push((started, now_us()));
                }
                committed
            }
            Some(e) => Err(e),
        };
        match outcome {
            Ok(()) => {
                tokio::task::yield_now().await;
                return Ok(());
            }
            Err(e) if retryable(&e) => {
                let _ = db.execute("ROLLBACK", ()).await;
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
    let (writers, rows): (usize, usize) = (args[1].parse()?, args[2].parse()?);
    let env = |name: &str, default: u64| std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default);
    let stall = Arc::new(Stall {
        suffix: std::env::var("SLOW_FILE").unwrap_or_else(|_| "-wal".into()),
        delay: Duration::from_millis(env("SLOW_MS", 3000)),
        armed: AtomicUsize::new(0),
        started_us: AtomicU64::new(0),
        ended_us: AtomicU64::new(0),
    });
    let io = Arc::new(SlowSyncIo { inner: Arc::new(MemoryIO::new()), stall: stall.clone() });
    let database = turso::Builder::new_local("slow_sync.db")
        .with_io_impl(io)
        .experimental_mvcc_passive_checkpoint(std::env::var_os("BLOCKING_CHECKPOINT").is_none())
        .build()
        .await?;
    RUN_START.get_or_init(Instant::now);
    let setup = database.connect()?;
    {
        let mut r = setup.query("PRAGMA journal_mode = 'mvcc'", ()).await?;
        while r.next().await?.is_some() {}
    }
    {
        let mut r = setup.query(&format!("PRAGMA mvcc_checkpoint_threshold = {}", env("CHECKPOINT_BYTES", 262144)), ()).await?;
        while r.next().await?.is_some() {}
    }
    setup.execute_batch("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL); CREATE TABLE side(id INTEGER PRIMARY KEY, x BLOB NOT NULL);").await?;
    let stop = Arc::new(AtomicBool::new(false));
    let total = writers * rows;
    // One commit log per task: the writers first, then the maintainer.
    let logs: Vec<Arc<Commits>> = (0..=writers).map(|_| Arc::new(Commits::default())).collect();
    let maintainer = {
        let (db, stop, commits) = (database.connect()?, stop.clone(), logs[writers].clone());
        tokio::spawn(async move {
            let mut n = 0usize;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let ids: Vec<String> = (0..64).map(|k| ((n * 64 + k) * 7919 % total + 1).to_string()).collect();
                let statements = vec![
                    format!("UPDATE v SET e = e WHERE id IN ({})", ids.join(",")),
                    format!("DELETE FROM side WHERE id = {}", n % 97),
                    format!("INSERT INTO side VALUES ({}, randomblob(600)) ON CONFLICT(id) DO UPDATE SET x = excluded.x", n % 97),
                ];
                concurrent(&db, &statements, 100, &commits).await?;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            anyhow::Ok(())
        })
    };
    let reader = {
        let (db, stop) = (database.connect()?, stop.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
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
    let arm = {
        let stall = stall.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(env("ARM_MS", 500))).await;
            stall.armed.store(1, Ordering::Release);
        })
    };
    let mut tasks = Vec::new();
    for w in 0..writers {
        let (db, commits) = (database.connect()?, logs[w].clone());
        tasks.push(tokio::spawn(async move {
            let id = |n: usize| w * rows + n + 1;
            for start in (0..rows).step_by(16) {
                let end = (start + 16).min(rows);
                let batch: Vec<String> = (start..end).map(|n| format!("INSERT INTO v VALUES ({}, randomblob(512))", id(n))).collect();
                concurrent(&db, &batch, 8, &commits).await?;
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
                    concurrent(&db, &changes, 8, &commits).await?;
                }
            }
            anyhow::Ok(())
        }));
    }
    for t in tasks {
        t.await??;
    }
    stop.store(true, Ordering::Relaxed);
    maintainer.await??;
    reader.await??;
    arm.await?;
    let (from, to) = (stall.started_us.load(Ordering::Acquire), stall.ended_us.load(Ordering::Acquire));
    let mut per_task = Vec::new();
    for (task, log) in logs.iter().enumerate() {
        let log = log.lock().unwrap();
        // A commit finished while the stall held, with a 50 ms margin each side.
        let during = log.iter().filter(|&&(_, end)| from > 0 && end > from + 50_000 && end + 50_000 < to).count();
        let spanned = log.iter().any(|&(start, end)| from > 0 && start <= from + 50_000 && end + 50_000 >= to);
        let longest_ms = log.iter().map(|&(s, e)| e - s).max().unwrap_or(0) as f64 / 1e3;
        let name = if task == writers { "maintainer".to_string() } else { format!("writer{task}") };
        per_task.push(serde_json::json!({"task": name, "commits": log.len(), "finished_during_stall": during, "a_commit_spanned_the_stall": spanned, "longest_commit_ms": longest_ms}));
    }
    println!(
        "{}",
        serde_json::json!({"slow_file": stall.suffix, "stall_ms": if from > 0 { (to - from) as f64 / 1e3 } else { 0.0 }, "stall_at_ms": from as f64 / 1e3, "tasks": per_task})
    );
    Ok(())
}
