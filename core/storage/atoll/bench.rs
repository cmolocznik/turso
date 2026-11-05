//! Throughput of Atoll's shared hot paths from many threads at once, the measure a layout change
//! such as cache padding is judged by: epoch steps, retiring and reclaiming through them, a free
//! list's pops and pushes, and transaction commits through the log. Each runs on every thread
//! count from one, doubling, up to the machine's parallelism, and prints operations per second:
//!
//! ```text
//! cargo test --release -p turso_core --lib storage::atoll::bench -- --ignored --nocapture --test-threads 1
//! ```

use super::buffer::{BufferConfig, MIN_BUDGET, WRITEBACK_BYTES};
use super::db::{Atoll, AtollConfig};
use super::drive;
use super::epoch::{Epochs, Retired};
use super::log::LogConfig;
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
        let config = AtollConfig {
            log: LogConfig {
                segment_bytes: 64 << 20,
                buffer_bytes: 4 << 20,
                chunk_bytes: 64 << 10,
                writes_in_flight: 4,
                direct: true,
            },
            store: StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            buffer: BufferConfig {
                budget: MIN_BUDGET,
                writeback_bytes: WRITEBACK_BYTES,
            },
            log_budget: None,
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
/// `ATOLL_BENCH_VALUE` sets the values' length (100 bytes; at least 44).
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
        for chunk in shuffled.chunks(TXN_ROWS).filter(|_| !reads) {
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
            println!("bench sql-{name} {journal} ops {rows} ns/op {ns:.0}");
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

/// Rows per transaction in [`paths`]'s versioned inserts, as the SQL throughput benchmark writes.
const TXN_ROWS: usize = 100;

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
        },
        log_budget: None,
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
