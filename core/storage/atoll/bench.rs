//! Throughput of Atoll's shared hot paths from many threads at once, the measure a layout change
//! such as cache padding is judged by: epoch steps, retiring and reclaiming through them, a free
//! list's pops and pushes, and transaction commits through the log. Each runs on every thread
//! count from one, doubling, up to the machine's parallelism, and prints operations per second:
//!
//! ```text
//! cargo test --release -p turso_core --lib storage::atoll::bench -- --ignored --nocapture --test-threads 1
//! ```

use super::buffer::{min_budget_beside, BufferConfig, WRITEBACK_BYTES};
use super::db::{Atoll, AtollConfig};
use super::drive;
use super::epoch::{Epochs, Retired};
use super::log::{ring_bytes, LogConfig};
use super::stack::FreeList;
use super::store::StoreConfig;
use super::txn::{Commit, Isolation, TxnConfig};
use crate::io::{MemoryIO, IO};
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::Arc;
use std::time::{Duration, Instant};

/// How long each run measures: starting and joining its threads takes well under a millisecond,
/// a tenth of a percent of it.
const RUN: Duration = Duration::from_secs(1);

/// Runs per thread count, of which the median is reported: one run disturbed by the rest of the
/// machine cannot move it.
const REPEATS: usize = 5;

type Op = Arc<dyn Fn() + Send + Sync>;

/// One, doubling, up to the machine's parallelism, and the parallelism itself.
fn thread_counts() -> Vec<usize> {
    let most = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut counts: Vec<usize> = std::iter::successors(Some(1), |n| Some(n * 2))
        .take_while(|&n| n < most)
        .collect();
    counts.push(most);
    counts
}

/// Operations per second of `op` run on `threads` threads at once for [`RUN`].
fn rate(threads: usize, op: Op) -> f64 {
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(std::sync::Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let (op, stop, start) = (op.clone(), stop.clone(), start.clone());
            std::thread::spawn(move || {
                start.wait();
                let mut done = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    op();
                    done += 1;
                }
                done
            })
        })
        .collect();
    start.wait();
    let began = Instant::now();
    std::thread::sleep(RUN);
    stop.store(true, Ordering::Relaxed);
    let done: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    done as f64 / began.elapsed().as_secs_f64()
}

/// Prints the median rate of `name` at every thread count, each run on fresh state from `make`.
fn report(name: &str, make: impl Fn(usize) -> Op) {
    for threads in thread_counts() {
        let mut runs: Vec<f64> = (0..REPEATS).map(|_| rate(threads, make(threads))).collect();
        runs.sort_by(f64::total_cmp);
        println!(
            "bench {name} threads {threads} ops/s {:.0} range {:.0}..{:.0}",
            runs[REPEATS / 2],
            runs[0],
            runs[REPEATS - 1]
        );
    }
}

/// Entering and leaving a step.
#[test]
#[ignore]
fn epoch_steps() {
    report("epoch-steps", |_| {
        let epochs = Arc::new(Epochs::new());
        Arc::new(move || drop(epochs.enter()))
    });
}

/// A step that retires an object and reclaims what no step can hold any more, as freeing a page
/// id does.
#[test]
#[ignore]
fn epoch_retire_and_reclaim() {
    report("epoch-retire", |_| {
        let epochs = Arc::new(Epochs::new());
        let retired: Arc<Retired<u64>> = Arc::new(Retired::default());
        Arc::new(move || {
            let _step = epochs.enter();
            retired.retire(&epochs, 0);
            drop(retired.reclaim(&epochs));
        })
    });
}

/// A pop from a free list and a push of what it took, as a floor's slot is taken and given back.
#[test]
#[ignore]
fn free_list_pop_and_push() {
    report("free-list", |threads| {
        let epochs = Arc::new(Epochs::new());
        let list: Arc<FreeList<u64>> = Arc::new(FreeList::default());
        for n in 0..threads as u64 {
            list.push(n);
        }
        Arc::new(move || {
            let step = epochs.enter();
            if let Some(n) = list.pop(&epochs, &step) {
                list.push(n);
            }
        })
    });
}

/// A transaction that commits one small change, durable on memory: the Commit record's admission,
/// reservation, copy and flush, the clock, the horizon and the commit logs. The change is noted
/// and not made: the commit pays for its body alone.
#[test]
#[ignore]
fn commits() {
    report("commit", |threads| {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let log = LogConfig {
            segment_bytes: 64 << 20,
            buffer_bytes: 4 << 20,
            chunk_bytes: 64 << 10,
            writes_in_flight: 4,
            direct: true,
        };
        let config = AtollConfig {
            store: StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            buffer: BufferConfig {
                budget: min_budget_beside(
                    ring_bytes(&log),
                    crate::storage::atoll::buffer::bank_frames(
                        crate::storage::atoll::checkpoint::WRITE_BYTES,
                    ),
                ),
                writeback_bytes: WRITEBACK_BYTES,
                kept_bytes: None,
                lent_at_open: Default::default(),
            },
            log,
            log_budget: None,
            checkpoint_write_bytes: None,
            checkpointing: crate::storage::atoll::checkpoint::Checkpointing::Writers,
            txn: TxnConfig {
                slots: threads.max(TxnConfig::default().slots),
            },
        };
        let db = Arc::new(Atoll::open(io.clone(), "/db/bench", config).unwrap());
        let class = super::page::SizeClass::new(0).unwrap();
        let root = drive(&io, || super::btree::Tree::create(db.pool(), class)).unwrap();
        Arc::new(move || {
            let mut txn = drive(&io, || db.transactions().begin(Isolation::ReadCommitted)).unwrap();
            let start = txn.start().to_be_bytes();
            txn.changed(|s, c| c.plain_put(s, root, &start, &[]));
            let mut op = Commit::new(txn);
            drive(&io, || db.transactions().commit(&mut op)).unwrap();
        })
    });
}

/// The engine's own cost per operation, on one thread with every page in memory and the log on
/// memory: puts and lookups in a plain tree, and versioned inserts and reads in transactions of
/// [`TXN_ROWS`] rows. Prints nanoseconds per operation of the timed phase alone: the log's
/// segments are prepared and, for reads, the tree is built before it. With `ATOLL_BENCH=<name>`
/// only that case runs; `ATOLL_BENCH_ROWS` sets the rows (200,000); `ATOLL_PERF_CTL` and
/// `ATOLL_PERF_ACK` name a `perf record --control` fifo pair, enabled for the timed phase only.
///
/// ```text
/// cargo test --release -p turso_core --lib storage::atoll::bench::paths -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn paths() {
    use super::btree::{Cursor, Lookup, Mode, Near, Outcome, Put, Seek, Tree};
    use super::page::SizeClass;
    use super::txn::versioned::{Get, VersionedCursor, VersionedTree, Versions, Write, WriteOp};

    const VALUE: usize = 100;
    // Reads repeat their passes for at least this long, so the phase outweighs its own start.
    const READ_FOR: Duration = Duration::from_secs(2);
    let rows: u64 = std::env::var("ATOLL_BENCH_ROWS").map_or(200_000, |r| r.parse().unwrap());
    let only = std::env::var("ATOLL_BENCH").ok();
    let runs = |name: &str| only.as_deref().is_none_or(|o| o == name);
    let perf = perf_control;
    let key = |n: u64| (n ^ (1 << 63)).to_be_bytes();
    let value = vec![7u8; VALUE];
    let open = || {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let config = bench_config();
        let db = Atoll::open(io.clone(), "/db/bench", config).unwrap();
        // The log's segments are prepared by its first appends: here, not in a timed phase.
        let root = drive(&io, || Tree::create(db.pool(), SizeClass::new(0).unwrap())).unwrap();
        let scratch = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root);
        for n in 0..1_000 {
            let mut op = Put::new(&key(n), value.clone(), Mode::Insert);
            drive(&io, || scratch.put(&mut op)).unwrap();
        }
        (io, db)
    };
    let shuffled = permutation(rows);
    let timed = |name: &str, log: &super::log::Log, phase: &mut dyn FnMut() -> u64| {
        let logged = log.end();
        perf("enable");
        let began = Instant::now();
        let ops = phase();
        let ns = began.elapsed().as_nanos() as f64 / ops as f64;
        perf("disable");
        let bytes = (log.end() - logged) as f64 / ops as f64;
        println!("bench {name} ops {ops} ns/op {ns:.0} log-bytes/op {bytes:.0}");
    };
    for (name, order) in [("tree-put-seq", false), ("tree-put-rand", true)] {
        let get = order && runs("tree-get");
        let scan = order && runs("tree-scan");
        if !runs(name) && !get && !scan {
            continue;
        }
        let (io, db) = open();
        let root = drive(&io, || Tree::create(db.pool(), SizeClass::new(0).unwrap())).unwrap();
        let tree = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root);
        // Each put tries the leaf the one before reached, as a cursor's writes do.
        let mut put = || {
            let mut near = Near::NONE;
            for i in 0..rows {
                let n = if order { shuffled[i as usize] } else { i };
                let mut op = Put::new(&key(n), value.clone(), Mode::Insert).near(near);
                drive(&io, || tree.put(&mut op)).unwrap();
                near = op.reached();
            }
            rows
        };
        if runs(name) {
            timed(name, db.log(), &mut put);
        } else {
            put();
        }
        if scan {
            timed("tree-scan", db.log(), &mut || {
                let (began, mut ops) = (Instant::now(), 0);
                while began.elapsed() < READ_FOR {
                    let mut cursor = Cursor::new();
                    let mut more = drive(&io, || tree.seek(&mut cursor, &[], Seek::Ge)).unwrap();
                    while more {
                        ops += 1;
                        more = drive(&io, || tree.next(&mut cursor)).unwrap();
                    }
                }
                ops
            });
        }
        if get {
            timed("tree-get", db.log(), &mut || {
                let (began, mut ops) = (Instant::now(), 0);
                while began.elapsed() < READ_FOR {
                    for &n in &shuffled {
                        let mut op = Lookup::new(&key(n));
                        assert!(drive(&io, || tree.lookup(&mut op)).unwrap().is_some());
                    }
                    ops += rows;
                }
                ops
            });
        }
    }
    if runs("tree-update") {
        let (io, db) = open();
        let root = drive(&io, || Tree::create(db.pool(), SizeClass::new(0).unwrap())).unwrap();
        let tree = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root);
        for &n in &shuffled {
            let mut op = Put::new(&key(n), value.clone(), Mode::Insert);
            drive(&io, || tree.put(&mut op)).unwrap();
        }
        // The same length, four bytes changed: a column's new value.
        let mut changed = value.clone();
        changed[40..44].copy_from_slice(&[1, 2, 3, 4]);
        timed("tree-update", db.log(), &mut || {
            let mut near = Near::NONE;
            for &n in &shuffled {
                let mut op = Put::new(&key(n), changed.clone(), Mode::Update).near(near);
                drive(&io, || tree.put(&mut op)).unwrap();
                near = op.reached();
            }
            rows
        });
    }
    let versioned = [
        "versioned-insert",
        "versioned-get",
        "versioned-update",
        "versioned-delete",
        "versioned-scan",
        "versioned-insert-mt",
        "versioned-update-mt",
    ];
    if versioned.iter().any(|v| runs(v)) {
        let (io, db) = open();
        let root = drive(&io, || {
            Tree::create_versioned(db.pool(), SizeClass::new(0).unwrap())
        })
        .unwrap();
        let slots = TxnConfig::default().slots;
        let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), slots);
        let tree = VersionedTree::open(io.clone(), db.pool().clone(), db.store().clone(), root);
        let txns = db.transactions();
        let mut insert = || {
            let mut near = Near::NONE;
            for chunk in shuffled.chunks(TXN_ROWS) {
                let mut txn = drive(&io, || txns.begin(Isolation::Snapshot)).unwrap();
                for &n in chunk {
                    txns.statement(&mut txn).unwrap();
                    let mut op = WriteOp::put(&key(n), value.clone(), Mode::Insert).near(near);
                    match drive(&io, || tree.write(txns, &versions, &mut txn, &mut op)).unwrap() {
                        Write::Done(_) => {}
                        other => panic!("{other:?}"),
                    }
                    near = op.reached();
                }
                let mut op = Commit::new(txn);
                drive(&io, || txns.commit(&mut op)).unwrap();
            }
            rows
        };
        // On `ATOLL_BENCH_THREADS` threads at once, each its own share of the keys, in
        // transactions as above: inserts, then every row changed.
        let threads: usize = std::env::var("ATOLL_BENCH_THREADS").map_or(1, |t| t.parse().unwrap());
        let shares: Vec<&[u64]> = shuffled.chunks(shuffled.len().div_ceil(threads)).collect();
        let on_threads = |update: bool| {
            let mut changed = value.clone();
            changed[40..44].copy_from_slice(&[1, 2, 3, 4]);
            std::thread::scope(|s| {
                for share in &shares {
                    let (io, tree, versions, value, changed) =
                        (&io, &tree, &versions, &value, &changed);
                    s.spawn(move || {
                        let mut near = Near::NONE;
                        for chunk in share.chunks(TXN_ROWS) {
                            let mut txn = drive(io, || txns.begin(Isolation::Snapshot)).unwrap();
                            for &n in chunk {
                                txns.statement(&mut txn).unwrap();
                                let mut op = match update {
                                    true => WriteOp::put(&key(n), changed.clone(), Mode::Update),
                                    false => WriteOp::put(&key(n), value.clone(), Mode::Insert),
                                }
                                .near(near);
                                let wrote =
                                    drive(io, || tree.write(txns, versions, &mut txn, &mut op));
                                assert!(matches!(wrote.unwrap(), Write::Done(_)));
                                near = op.reached();
                            }
                            let mut op = Commit::new(txn);
                            drive(io, || txns.commit(&mut op)).unwrap();
                        }
                    });
                }
            });
            rows
        };
        if runs("versioned-insert-mt") || runs("versioned-update-mt") {
            if runs("versioned-insert-mt") {
                timed("versioned-insert-mt", db.log(), &mut || on_threads(false));
            } else {
                on_threads(false);
                timed("versioned-update-mt", db.log(), &mut || on_threads(true));
            }
        } else if runs("versioned-insert") {
            timed("versioned-insert", db.log(), &mut insert);
        } else {
            insert();
        }
        // Each row changed, or deleted, in transactions as the inserts were.
        let rewrite = |delete: bool| {
            let mut changed = value.clone();
            changed[40..44].copy_from_slice(&[1, 2, 3, 4]);
            let mut near = Near::NONE;
            for chunk in shuffled.chunks(TXN_ROWS) {
                let mut txn = drive(&io, || txns.begin(Isolation::Snapshot)).unwrap();
                for &n in chunk {
                    txns.statement(&mut txn).unwrap();
                    let mut op = match delete {
                        true => WriteOp::delete(&key(n)),
                        false => WriteOp::put(&key(n), changed.clone(), Mode::Update),
                    }
                    .near(near);
                    match drive(&io, || tree.write(txns, &versions, &mut txn, &mut op)).unwrap() {
                        Write::Done(Outcome::Updated) => {}
                        other => panic!("{other:?}"),
                    }
                    near = op.reached();
                }
                let mut op = Commit::new(txn);
                drive(&io, || txns.commit(&mut op)).unwrap();
            }
            rows
        };
        if runs("versioned-update") {
            timed("versioned-update", db.log(), &mut || rewrite(false));
        }
        if runs("versioned-delete") {
            timed("versioned-delete", db.log(), &mut || rewrite(true));
        }
        if runs("versioned-scan") {
            let mut txn = drive(&io, || txns.begin(Isolation::Snapshot)).unwrap();
            txns.statement(&mut txn).unwrap();
            timed("versioned-scan", db.log(), &mut || {
                let (began, mut ops) = (Instant::now(), 0);
                while began.elapsed() < READ_FOR {
                    let mut cursor = VersionedCursor::new();
                    let mut more = drive(&io, || {
                        tree.seek(txns, &versions, &mut txn, &mut cursor, &[], Seek::Ge)
                    })
                    .unwrap();
                    while more {
                        ops += 1;
                        more = drive(&io, || tree.next(txns, &versions, &mut txn, &mut cursor))
                            .unwrap();
                    }
                }
                ops
            });
        }
        if runs("versioned-get") {
            let mut txn = drive(&io, || txns.begin(Isolation::Snapshot)).unwrap();
            txns.statement(&mut txn).unwrap();
            timed("versioned-get", db.log(), &mut || {
                let (began, mut ops) = (Instant::now(), 0);
                while began.elapsed() < READ_FOR {
                    for &n in &shuffled {
                        let mut op = Get::new(&key(n));
                        let found = drive(&io, || tree.get(txns, &versions, &mut txn, &mut op));
                        assert!(found.unwrap().is_some());
                    }
                    ops += rows;
                }
                ops
            });
        }
    }
}

/// SQL's cost per row through Turso, one connection, every page and the log in memory: rows
/// inserted in transactions of [`TXN_ROWS`], in no order; read, by a scan summing rowids, a scan
/// summing values' lengths, and a lookup of each value by its rowid, each in one transaction;
/// then each updated, its value changed at the same length, then each deleted, in transactions of
/// [`TXN_ROWS`]. Atoll's, or with `ATOLL_BENCH_WAL` set SQLite's WAL journal's, on the same
/// statements. Each phase runs, since the next reads its rows; `ATOLL_BENCH=sql-<phase>` times one
/// alone, as [`paths`] times its cases, under the same `ATOLL_BENCH_ROWS` and perf fifo pair. With
/// `ATOLL_BENCH_SEQ` set, every phase goes in key order, as rowids a table assigns are inserted.
/// `ATOLL_BENCH_VALUE` sets the values' length (100 bytes; at least 44), `ATOLL_BENCH_TXN` the rows
/// a writing transaction holds. Beside its time, a phase prints per row the allocations made, the
/// bytes copied into the log's ring and the bytes of changes encoded.
///
/// ```text
/// cargo test --release -p turso_core --lib storage::atoll::bench::sql -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn sql() {
    use crate::{ConnectOpts, Database, OpenOptions, SqliteDialect, Value};
    use std::num::NonZero;

    let rows: u64 = std::env::var("ATOLL_BENCH_ROWS").map_or(200_000, |r| r.parse().unwrap());
    let only = std::env::var("ATOLL_BENCH").ok();
    let wal = std::env::var_os("ATOLL_BENCH_WAL").is_some();
    let journal = if wal { "wal" } else { "atoll" };
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let options = if wal {
        options
    } else {
        options.atoll(bench_config())
    };
    let db = Database::open(io, "/db/sql-bench", options).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
        .unwrap();
    let shuffled = match std::env::var_os("ATOLL_BENCH_SEQ") {
        Some(_) => (0..rows).collect(),
        None => permutation(rows),
    };
    let length: usize = std::env::var("ATOLL_BENCH_VALUE").map_or(100, |v| v.parse().unwrap());
    assert!(length >= 44, "values of {length} bytes");
    let txn_rows: usize = std::env::var("ATOLL_BENCH_TXN").map_or(TXN_ROWS, |t| t.parse().unwrap());
    let value = Value::build_text("v".repeat(length));
    let changed = Value::build_text(format!("{}abcd{}", "v".repeat(40), "v".repeat(length - 44)));
    let (mut begin, mut commit) = (
        conn.prepare("BEGIN").unwrap(),
        conn.prepare("COMMIT").unwrap(),
    );
    let mut phase = |name: &str, sql: &str, data: Option<&Value>| {
        let timed = only.as_deref().is_none_or(|o| o == format!("sql-{name}"));
        let mut stmt = conn.prepare(sql).unwrap();
        let reads = sql.starts_with("SELECT");
        if reads {
            begin.run_ignore_rows().unwrap();
            begin.reset().unwrap();
        }
        if timed {
            perf_control("enable");
        }
        let counted = counts();
        let began = Instant::now();
        if reads && sql.contains("?1") {
            // A lookup per row, in the order rows went in.
            for &n in &shuffled {
                stmt.bind_at(NonZero::<usize>::MIN, Value::from_i64(n as i64))
                    .unwrap();
                stmt.run_ignore_rows().unwrap();
                stmt.reset().unwrap();
            }
        } else if reads {
            // A scan, once over every row.
            stmt.run_ignore_rows().unwrap();
            stmt.reset().unwrap();
        }
        for chunk in shuffled.chunks(txn_rows).filter(|_| !reads) {
            begin.run_ignore_rows().unwrap();
            begin.reset().unwrap();
            for &n in chunk {
                stmt.bind_at(NonZero::<usize>::MIN, Value::from_i64(n as i64))
                    .unwrap();
                if let Some(data) = data {
                    stmt.bind_at(NonZero::new(2).unwrap(), data.clone())
                        .unwrap();
                }
                stmt.run_ignore_rows().unwrap();
                stmt.reset().unwrap();
            }
            commit.run_ignore_rows().unwrap();
            commit.reset().unwrap();
        }
        let ns = began.elapsed().as_nanos() as f64 / rows as f64;
        if timed {
            perf_control("disable");
            let per = |n: u64| n as f64 / rows as f64;
            let held = super::txn::changes::HELD.with(|n| n.get());
            let (allocs, ring, redo) = counts();
            println!(
                "bench sql-{name} {journal} ops {rows} ns/op {ns:.0} allocs/op {:.2} ring/op {:.1} \
                 redo/op {:.1} held_kb {}",
                per(allocs - counted.0),
                per(ring - counted.1),
                per(redo - counted.2),
                held >> 10
            );
        }
        if reads {
            commit.run_ignore_rows().unwrap();
            commit.reset().unwrap();
        }
    };
    phase(
        "insert",
        "INSERT INTO t (id, data) VALUES (?1, ?2)",
        Some(&value),
    );
    phase("scan-rowid", "SELECT sum(id) FROM t", None);
    phase("scan-data", "SELECT sum(length(data)) FROM t", None);
    phase("lookup", "SELECT data FROM t WHERE id = ?1", None);
    phase(
        "update",
        "UPDATE t SET data = ?2 WHERE id = ?1",
        Some(&changed),
    );
    phase("delete", "DELETE FROM t WHERE id = ?1", None);
    let left = conn
        .prepare("SELECT count(*) FROM t")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(left, vec![vec![Value::from_i64(0)]], "every row deleted");
}

/// Sustained random updates on real files, the pool a quarter of the data or less, so pages are
/// evicted and checkpoints run all along (the audit's benchmark, with the stepau review's three
/// phases): first with no reader; then with one reader holding an older snapshot, whose values are
/// read again at the phase's end and must not have moved; then with that reader gone, while the
/// versions and copies it held are given back. One writer, in transactions of [`TXN_ROWS`] rows,
/// the next begun as the last commits. Each second prints the rows committed, the commit
/// latencies, the bytes the process wrote, and for Atoll the checkpoint's pages and bytes, the
/// swap copies written, the kept copies' room (peak, refusals, the writers' wait), the temporary
/// pages the versions hold and the log appended. The end prints the work left: for Atoll the pages
/// owed an image and the log a recovery would replay, for WAL the WAL's size. WAL runs with
/// `ATOLL_BENCH_WAL`, its page cache the pool's size:
///
/// ```text
/// ATOLL_BENCH_DIR=/data/bench cargo test --release -p turso_core --lib storage::atoll::bench::sustained -- --ignored --nocapture
/// ```
///
/// `ATOLL_BENCH_POOL` (bytes, 256 MiB), `ATOLL_BENCH_ROWS` (4,000,000), `ATOLL_BENCH_VALUE` (bytes,
/// 200), `ATOLL_BENCH_PHASE` (seconds, 30) and `ATOLL_BENCH_TXN` (rows a transaction of the timed
/// phases, [`TXN_ROWS`]) change the sizes.
#[test]
#[ignore]
fn sustained() {
    use crate::{ConnectOpts, Database, OpenOptions, SqliteDialect, Value};
    use std::num::NonZero;

    let env =
        |name: &str, default: u64| std::env::var(name).map_or(default, |v| v.parse().expect(name));
    let dir =
        std::env::var("ATOLL_BENCH_DIR").expect("ATOLL_BENCH_DIR: a directory on a real disk");
    let pool = env("ATOLL_BENCH_POOL", 256 << 20) as usize;
    let rows = env("ATOLL_BENCH_ROWS", 4_000_000);
    let length = env("ATOLL_BENCH_VALUE", 200) as usize;
    let phase_secs = env("ATOLL_BENCH_PHASE", 30);
    let txn_rows = env("ATOLL_BENCH_TXN", TXN_ROWS as u64);
    let wal = std::env::var_os("ATOLL_BENCH_WAL").is_some();
    let journal = if wal { "wal" } else { "atoll" };
    let path = format!("{dir}/sustained-{journal}-{}.db", std::process::id());
    let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let options = if wal {
        options
    } else {
        options.atoll(AtollConfig::with_budget(pool))
    };
    let db = Database::open(io, &path, options).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    if wal {
        conn.execute(format!("PRAGMA cache_size = -{}", pool >> 10))
            .unwrap();
    }
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT NOT NULL)")
        .unwrap();
    let value = |id: u64, round: u64| {
        let head = format!("{id}:{round}:");
        Value::build_text(format!(
            "{head}{}",
            "v".repeat(length.saturating_sub(head.len()))
        ))
    };
    let (mut begin, mut commit) = (
        conn.prepare("BEGIN").unwrap(),
        conn.prepare("COMMIT").unwrap(),
    );
    let mut insert = conn
        .prepare("INSERT INTO t (id, data) VALUES (?1, ?2)")
        .unwrap();
    let loading = Instant::now();
    // In ranges, not a list of the ids: a list of 4 million is 32 MiB of the capped memory.
    for start in (0..rows).step_by(1000) {
        begin.run_ignore_rows().unwrap();
        begin.reset().unwrap();
        for id in start..(start + 1000).min(rows) {
            insert
                .bind_at(NonZero::<usize>::MIN, Value::from_i64(id as i64))
                .unwrap();
            insert
                .bind_at(NonZero::new(2).unwrap(), value(id, 0))
                .unwrap();
            insert.run_ignore_rows().unwrap();
            insert.reset().unwrap();
        }
        commit.run_ignore_rows().unwrap();
        commit.reset().unwrap();
    }
    let synchronous = conn
        .prepare("PRAGMA synchronous")
        .and_then(|mut s| s.run_collect_rows())
        .map_or("unknown".to_string(), |r| format!("{r:?}"));
    println!(
        "sustained {journal} pool {} MiB rows {rows} value {length} loaded in {:.1} s, \
         synchronous {synchronous}",
        pool >> 20,
        loading.elapsed().as_secs_f64()
    );
    let atoll = db.atoll.clone();
    // The scope the run is capped in: its memory and the time its tasks stalled on memory, as
    // the kernel counts them for either engine, page cache included.
    let cgroup = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("0::").map(|p| format!("/sys/fs/cgroup{p}")))
        })
        .filter(|d| std::path::Path::new(&format!("{d}/memory.current")).exists());
    let cg_read = |file: &str| {
        cgroup
            .as_ref()
            .and_then(|d| std::fs::read_to_string(format!("{d}/{file}")).ok())
    };
    let cg_value = |file: &str| cg_read(file).and_then(|s| s.trim().parse::<u64>().ok());
    let cg_stat = |key: &str| {
        cg_read("memory.stat").and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix(key)?.strip_prefix(' ')?.parse::<u64>().ok())
        })
    };
    // Microseconds some or all of the scope's tasks stalled on memory, since it began.
    let cg_stall = |kind: &str| {
        cg_read("memory.pressure").and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix(kind)?
                    .split_whitespace()
                    .find_map(|f| f.strip_prefix("total=")?.parse::<u64>().ok())
            })
        })
    };
    println!(
        "sustained {journal} binary {} cgroup {} memory.max {}",
        std::env::current_exe().map_or("unread".to_string(), |p| p.display().to_string()),
        cgroup.as_deref().unwrap_or("none"),
        cg_read("memory.max").map_or("unread".to_string(), |s| s.trim().to_string()),
    );
    let written = || {
        std::fs::read_to_string("/proc/self/io")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find_map(|l| {
                        l.strip_prefix("write_bytes: ")
                            .map(|n| n.parse::<u64>().ok())
                    })
                    .flatten()
            })
            .unwrap_or(0)
    };
    /// What a second of the run counts, read before and after it.
    #[derive(Clone, Copy, Default)]
    struct Counts {
        written: u64,
        checkpoint_pages: u64,
        checkpoint_bytes: u64,
        swap: u64,
        log: u64,
        refusals: u64,
        waited_ns: u64,
        stall_some_us: u64,
        stall_full_us: u64,
    }
    let counts = || {
        let mut c = Counts {
            written: written(),
            stall_some_us: cg_stall("some").unwrap_or(0),
            stall_full_us: cg_stall("full").unwrap_or(0),
            ..Counts::default()
        };
        if let Some(a) = &atoll {
            let (cp, pool) = (a.atoll().checkpointer().stats(), a.atoll().pool());
            let kept = pool.kept_stats();
            c.checkpoint_pages = cp.pages;
            c.checkpoint_bytes = cp.bytes;
            c.swap = pool.swap_written();
            c.log = a.atoll().log().end();
            c.refusals = kept.refusals;
            c.waited_ns = kept.waited_ns;
        }
        c
    };
    let mut update = conn
        .prepare("UPDATE t SET data = ?2 WHERE id = ?1")
        .unwrap();
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next_id = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % rows
    };
    let mut round = 0u64;
    let mut run_phase = |name: &str| {
        let mb = |b: u64| b as f64 / (1 << 20) as f64;
        let began = Instant::now();
        // The second's start, its counts then, its rows, and each transaction's time from
        // BEGIN through COMMIT and its COMMIT's alone.
        let mut window = (
            Instant::now(),
            counts(),
            0u64,
            Vec::<u64>::new(),
            Vec::<u64>::new(),
        );
        while began.elapsed() < Duration::from_secs(phase_secs) {
            round += 1;
            let txn = Instant::now();
            begin.run_ignore_rows().unwrap();
            begin.reset().unwrap();
            for _ in 0..txn_rows {
                let id = next_id();
                update
                    .bind_at(NonZero::<usize>::MIN, Value::from_i64(id as i64))
                    .unwrap();
                update
                    .bind_at(NonZero::new(2).unwrap(), value(id, round))
                    .unwrap();
                update.run_ignore_rows().unwrap();
                update.reset().unwrap();
            }
            let committing = Instant::now();
            commit.run_ignore_rows().unwrap();
            commit.reset().unwrap();
            window.2 += txn_rows;
            window.3.push(txn.elapsed().as_micros() as u64);
            window.4.push(committing.elapsed().as_micros() as u64);
            let secs = window.0.elapsed().as_secs_f64();
            if secs >= 1.0 {
                let now = counts();
                let was = window.1;
                let (mut txns, mut commits) =
                    (std::mem::take(&mut window.3), std::mem::take(&mut window.4));
                txns.sort_unstable();
                commits.sort_unstable();
                let pcts = |lat: &[u64]| {
                    let pct = |p: usize| lat[(lat.len() * p / 100).min(lat.len() - 1)];
                    format!("p50 {} p99 {} max {}", pct(50), pct(99), lat[lat.len() - 1])
                };
                let mut line = format!(
                    "sustained {journal} {name} t {:.0} rows/s {:.0} txn_us {} commit_us {} \
                     written_MB/s {:.1}",
                    began.elapsed().as_secs_f64(),
                    window.2 as f64 / secs,
                    pcts(&txns),
                    pcts(&commits),
                    mb(now.written - was.written) / secs,
                );
                match &cgroup {
                    Some(_) => {
                        let unread = |v: Option<u64>| {
                            v.map_or("unread".to_string(), |b| format!("{:.1}", mb(b)))
                        };
                        line += &format!(
                            " mem_MB {} anon_MB {} file_MB {} stall_some_ms {:.1} stall_full_ms {:.1}",
                            unread(cg_value("memory.current")),
                            unread(cg_stat("anon")),
                            unread(cg_stat("file")),
                            (now.stall_some_us - was.stall_some_us) as f64 / 1e3,
                            (now.stall_full_us - was.stall_full_us) as f64 / 1e3,
                        );
                    }
                    None => line += " cgroup none",
                }
                if let Some(a) = &atoll {
                    let kept = a.atoll().pool().kept_stats();
                    let lent = a.atoll().pool().lent_stats();
                    line += &format!(
                        " ckpt_pages {} ckpt_MB {:.1} swap_MB {:.1} log_MB {:.1} kept_peak_MB {:.1} \
                         refusals {} waited_ms {:.1} longest_wait_ms {:.1} temp_MB {:.1} \
                         meta_MB {:.1} meta_cut_MB {:.1} lent_peak_MB kept {:.1} image {:.1} log \
                         {:.1}",
                        now.checkpoint_pages - was.checkpoint_pages,
                        mb(now.checkpoint_bytes - was.checkpoint_bytes),
                        mb(now.swap - was.swap),
                        mb(now.log - was.log),
                        mb(kept.peak as u64),
                        now.refusals - was.refusals,
                        (now.waited_ns - was.waited_ns) as f64 / 1e6,
                        kept.longest_wait_ns as f64 / 1e6,
                        mb(a.atoll().pool().temporary_bytes()),
                        mb((a.atoll().store().meta().resident() * super::meta::META_PAGE) as u64),
                        mb(a.atoll().store().meta().cut_stats().0 as u64),
                        mb(lent.kept.1 as u64),
                        mb(lent.image.1 as u64),
                        mb(lent.log.1 as u64),
                    );
                    a.atoll().pool().reset_lent_peaks();
                    a.atoll().pool().reset_kept_peaks();
                }
                println!("{line}");
                txns.clear();
                commits.clear();
                window = (Instant::now(), now, 0, txns, commits);
            }
        }
    };
    run_phase("no-reader");
    // A reader holding the snapshot of now, its values of a sample read and kept.
    let reader = db.connect(ConnectOpts::new()).unwrap();
    let mut lookup = reader.prepare("SELECT data FROM t WHERE id = ?1").unwrap();
    reader.execute("BEGIN").unwrap();
    let sample: Vec<u64> = (0..1000).map(|i| i * (rows / 1000)).collect();
    let read = |lookup: &mut crate::Statement| {
        sample
            .iter()
            .map(|&id| {
                lookup
                    .bind_at(NonZero::<usize>::MIN, Value::from_i64(id as i64))
                    .unwrap();
                let got = lookup.run_collect_rows().unwrap();
                lookup.reset().unwrap();
                got
            })
            .collect::<Vec<_>>()
    };
    let seen = read(&mut lookup);
    run_phase("old-reader");
    assert!(
        read(&mut lookup) == seen,
        "the older snapshot read values committed after it"
    );
    reader.execute("COMMIT").unwrap();
    println!(
        "sustained {journal} old-reader: its {} values unmoved",
        sample.len()
    );
    run_phase("reader-gone");
    match &atoll {
        Some(a) => {
            let cp = a.atoll().checkpointer().stats();
            println!(
                "sustained atoll end: owed_pages {} replay_MB {:.1} temp_MB {:.1} \
                 checkpoint working_peak_KiB {} flying_peak_KiB {} stepping_s {:.1} \
                 kept {:?}",
                a.atoll().pool().owed_pages(),
                a.atoll().checkpointer().lag() as f64 / (1 << 20) as f64,
                a.atoll().pool().temporary_bytes() as f64 / (1 << 20) as f64,
                cp.working_peak >> 10,
                cp.flying_peak >> 10,
                cp.stepping_ns as f64 / 1e9,
                a.atoll().pool().kept_stats(),
            );
        }
        None => println!(
            "sustained wal end: wal_MB {:.1}",
            std::fs::metadata(format!("{path}-wal")).map_or(0, |m| m.len()) as f64
                / (1 << 20) as f64
        ),
    }
    if cgroup.is_some() {
        println!(
            "sustained {journal} cgroup end: memory.peak {} events {}",
            cg_read("memory.peak").map_or("unread".to_string(), |s| s.trim().to_string()),
            cg_read("memory.events").map_or("unread".to_string(), |s| s
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")),
        );
    }
}

/// Rows per transaction in [`paths`]'s versioned inserts, as the SQL throughput benchmark writes.
const TXN_ROWS: usize = 100;

/// A stream partition's ingest and read (DESIGN.md §21), its log in memory: `ATOLL_BENCH_ROWS`
/// messages (200,000) of `ATOLL_BENCH_VALUE` bytes (100), appended in batches of
/// `ATOLL_BENCH_TXN` (100) each waited on, then read back whole in leases of 1 MiB.
/// `ATOLL_BENCH=stream-append` or `stream-read` times one alone, under the perf fifo pair as
/// [`sql`]'s phases are. Prints per message the time, allocations and bytes copied into the ring.
///
/// ```text
/// cargo test --release -p turso_core --lib storage::atoll::bench::stream -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn stream() {
    use super::stream::{Append, Partition, Read};
    let rows: u64 = std::env::var("ATOLL_BENCH_ROWS").map_or(200_000, |r| r.parse().unwrap());
    let length: usize = std::env::var("ATOLL_BENCH_VALUE").map_or(100, |v| v.parse().unwrap());
    let per: u64 = std::env::var("ATOLL_BENCH_TXN").map_or(TXN_ROWS as u64, |t| t.parse().unwrap());
    let only = std::env::var("ATOLL_BENCH").ok();
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let p = Partition::open(io.clone(), "/db/stream-bench", bench_config().log).unwrap();
    let message = vec![b'v'; length];
    let timed = |name: &str| {
        only.as_deref()
            .is_none_or(|o| o == format!("stream-{name}"))
    };
    let report = |name: &str, began: Instant, counted: (u64, u64, u64)| {
        let ns = began.elapsed().as_nanos() as f64 / rows as f64;
        let (allocs, ring, _) = counts();
        let per = |n: u64| n as f64 / rows as f64;
        println!(
            "bench stream-{name} ops {rows} ns/op {ns:.0} allocs/op {:.2} ring/op {:.1}",
            per(allocs - counted.0),
            per(ring - counted.1)
        );
    };
    let (on, counted, began) = (timed("append"), counts(), Instant::now());
    if on {
        perf_control("enable");
    }
    let mut i = 0;
    while i < rows {
        let mut batch = p.batch();
        let mut n = 0;
        while i < rows && n < per && batch.push(&message, i) {
            i += 1;
            n += 1;
        }
        let mut op = Append::new(batch);
        drive(&io, || p.append(&mut op)).unwrap();
    }
    if on {
        perf_control("disable");
        report("append", began, counted);
    }
    let (on, counted, began) = (timed("read"), counts(), Instant::now());
    if on {
        perf_control("enable");
    }
    let (mut at, mut read) = (0, 0u64);
    while at < rows {
        let Read::Batches(batches) = p.read(at, 1 << 20).unwrap() else {
            panic!("expired")
        };
        for b in &batches {
            for (offset, m) in b.messages() {
                if offset >= at {
                    read += m.len() as u64;
                    at = offset + 1;
                }
            }
        }
    }
    if on {
        perf_control("disable");
        report("read", began, counted);
    }
    assert_eq!(read, rows * length as u64);
}

/// On this thread so far: allocations, bytes copied into the log's ring, and bytes of changes
/// encoded into transactions' buffers. Starts the most memory a transaction's buffer held again.
/// An exclusive fix of a page already resident and its release, the pair a latch's release is
/// costed on (DESIGN.md 9.1.3, the latch's release): the same page every time, its first release
/// (marked, so the slow one) and a warm-up before the timed phase, no checkpoint and nothing else
/// modifying the pool meanwhile, so every timed release is unmarked. `ATOLL_BENCH_PAIRS` sets the
/// pairs (10,000,000); `ATOLL_PERF_CTL` and `ATOLL_PERF_ACK` as for [`paths`].
///
/// ```text
/// cargo test --release -p turso_core --lib storage::atoll::bench::fix_pairs -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn fix_pairs() {
    use super::page::SizeClass;
    use crate::types::IOResult;
    let pairs: u64 = std::env::var("ATOLL_BENCH_PAIRS").map_or(10_000_000, |r| r.parse().unwrap());
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Atoll::open(io.clone(), "/db/bench-fix", bench_config()).unwrap();
    let pool = db.pool().clone();
    let pid = {
        let mut page = drive(&io, || pool.allocate(SizeClass::new(0).unwrap())).unwrap();
        page.changed();
        page.pid()
    };
    let pair = || match pool.fix_exclusive(pid, None).unwrap() {
        IOResult::Done(page) => drop(page),
        IOResult::IO(_) => panic!("a resident page's fix waited"),
    };
    for _ in 0..pairs / 10 {
        pair();
    }
    perf_control("enable");
    let began = Instant::now();
    for _ in 0..pairs {
        pair();
    }
    let ns = began.elapsed().as_nanos() as f64 / pairs as f64;
    perf_control("disable");
    println!("bench fix-pair ops {pairs} ns/op {ns:.2}");
}

fn counts() -> (u64, u64, u64) {
    super::txn::changes::HELD.with(|n| n.set(0));
    (
        super::testing::allocations(),
        super::log::COPIED_IN.with(|n| n.get()),
        super::txn::changes::ENCODED.with(|n| n.get()),
    )
}

/// Tells the profiler named by `ATOLL_PERF_CTL` and `ATOLL_PERF_ACK` (a `perf --control` fifo pair)
/// to `enable` or `disable`, and waits for its answer: it records the timed phase alone.
fn perf_control(command: &str) {
    use std::io::{Read, Write as _};
    let (Ok(ctl), Ok(ack)) = (
        std::env::var("ATOLL_PERF_CTL"),
        std::env::var("ATOLL_PERF_ACK"),
    ) else {
        return;
    };
    std::fs::OpenOptions::new()
        .write(true)
        .open(ctl)
        .unwrap()
        .write_all(format!("{command}\n").as_bytes())
        .unwrap();
    let mut answer = [0u8; 5];
    std::fs::File::open(ack)
        .unwrap()
        .read_exact(&mut answer)
        .unwrap();
}

/// The database the cost benchmarks run on: every page in memory, the log on memory.
fn bench_config() -> AtollConfig {
    AtollConfig {
        log: LogConfig {
            segment_bytes: 256 << 20,
            buffer_bytes: 16 << 20,
            chunk_bytes: 64 << 10,
            writes_in_flight: 4,
            direct: true,
        },
        store: StoreConfig {
            file_bytes: 1 << 34,
            meta_pages: 1 << 12,
        },
        buffer: BufferConfig {
            budget: 2 << 30,
            writeback_bytes: WRITEBACK_BYTES,
            kept_bytes: None,
            lent_at_open: Default::default(),
        },
        log_budget: None,
        checkpoint_write_bytes: None,
        checkpointing: crate::storage::atoll::checkpoint::Checkpointing::Writers,
        txn: TxnConfig::default(),
    }
}

/// A fixed permutation of `0..rows`, for keys in no order.
fn permutation(rows: u64) -> Vec<u64> {
    let mut keys: Vec<u64> = (0..rows).collect();
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for i in (1..keys.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        keys.swap(i, (state % (i as u64 + 1)) as usize);
    }
    keys
}
