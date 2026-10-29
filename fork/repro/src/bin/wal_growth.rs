//! Does the WAL restart under concurrent writers, or grow without bound?
//!
//!     wal_growth DIR THREADS SECONDS [READERS]
//!
//! THREADS connections on their own threads insert 200-byte rows into a plain table, one row a
//! transaction in autocommit, for SECONDS; READERS more connections count the table over and over.
//! CHECKPOINT=mode (PASSIVE, FULL, RESTART or TRUNCATE) adds a connection running that checkpoint
//! every CHECKPOINT_MS (default 200), and AUTOCHECKPOINT=n sets the writers' wal_autocheckpoint.
//! Every half second it prints the WAL file's size and its
//! header's checkpoint sequence, which a restart advances, and at the end the commits, the
//! restarts, the frames the WAL held at most and the commits' latency.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::{ConnectOpts, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect};

fn wal_header(path: &str) -> Option<(u64, u32)> {
    let bytes = std::fs::read(path).ok()?;
    let seq = u32::from_be_bytes(bytes.get(12..16)?.try_into().ok()?);
    Some((bytes.len() as u64, seq))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (dir, threads, seconds) = (&args[1], args[2].parse::<usize>().unwrap(), args[3].parse::<u64>().unwrap());
    let readers = args.get(4).and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let path = format!("{dir}/growth.db");
    let wal = format!("{path}-wal");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(io, &path, OpenFlags::default(), DatabaseOpts::new(), None, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA synchronous = NORMAL").unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB NOT NULL)").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let (commits, failures, reads) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let (db, stop, commits, failures, latencies) = (db.clone(), stop.clone(), commits.clone(), failures.clone(), latencies.clone());
        handles.push(std::thread::spawn(move || {
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.set_busy_timeout(Duration::from_secs(10));
            conn.execute("PRAGMA synchronous = NORMAL").unwrap();
            if let Ok(pages) = std::env::var("AUTOCHECKPOINT") {
                conn.execute(format!("PRAGMA wal_autocheckpoint = {pages}")).unwrap();
            }
            let mut mine = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let began = Instant::now();
                match conn.execute("INSERT INTO t(body) VALUES (randomblob(200))") {
                    Ok(()) => commits.fetch_add(1, Ordering::Relaxed),
                    Err(_) => failures.fetch_add(1, Ordering::Relaxed),
                };
                mine.push(began.elapsed().as_micros() as u64);
            }
            latencies.lock().unwrap().extend(mine);
        }));
    }
    for _ in 0..readers {
        let (db, stop, reads) = (db.clone(), stop.clone(), reads.clone());
        handles.push(std::thread::spawn(move || {
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.set_busy_timeout(Duration::from_secs(10));
            while !stop.load(Ordering::Relaxed) {
                let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
                let _ = stmt.run_with_row_callback(|_| Ok(()));
                reads.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    if let Ok(mode) = std::env::var("CHECKPOINT") {
        let every = Duration::from_millis(std::env::var("CHECKPOINT_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(200));
        let (db, stop) = (db.clone(), stop.clone());
        handles.push(std::thread::spawn(move || {
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.set_busy_timeout(Duration::from_secs(10));
            let (mut runs, mut busy, mut errors, mut longest) = (0u64, 0u64, 0u64, Duration::ZERO);
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(every);
                let began = Instant::now();
                let mut answered = None;
                let done = conn.prepare(format!("PRAGMA wal_checkpoint({mode})")).and_then(|mut stmt| {
                    stmt.run_with_row_callback(|row| {
                        answered = row.get_values().next().and_then(|v| v.as_int());
                        Ok(())
                    })
                });
                match (done, answered) {
                    (Ok(()), Some(0)) => runs += 1,
                    (Ok(()), _) => busy += 1,
                    (Err(_), _) => errors += 1,
                }
                longest = longest.max(began.elapsed());
            }
            println!("checkpoint {mode} every {} ms: {runs} completed, {busy} busy, {errors} failed, longest {} ms", every.as_millis(), longest.as_millis());
        }));
    }
    let start = Instant::now();
    let (_, first_seq) = wal_header(&wal).unwrap_or((0, 0));
    let mut largest = 0;
    let mut last_seq = first_seq;
    while start.elapsed() < Duration::from_secs(seconds) {
        std::thread::sleep(Duration::from_millis(500));
        if let Some((size, seq)) = wal_header(&wal) {
            largest = largest.max(size);
            last_seq = seq;
            println!("{:5.1} s: wal {:>11} bytes, checkpoint seq {seq}, commits {}", start.elapsed().as_secs_f64(), size, commits.load(Ordering::Relaxed));
        }
    }
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().unwrap();
    }
    let mut latencies = latencies.lock().unwrap().clone();
    latencies.sort_unstable();
    let at = |q: f64| latencies.get(((latencies.len() as f64 * q) as usize).min(latencies.len().saturating_sub(1))).copied().unwrap_or(0);
    println!("commit latency p50 {} us, p99 {} us, p99.9 {} us, max {} us", at(0.5), at(0.99), at(0.999), latencies.last().copied().unwrap_or(0));
    println!(
        "threads {threads} readers {readers}: commits {} failures {} reads {}, restarts {}, largest wal {} bytes ({} frames)",
        commits.load(Ordering::Relaxed),
        failures.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
        last_seq.wrapping_sub(first_seq),
        largest,
        largest.saturating_sub(32) / (24 + 4096)
    );
}
