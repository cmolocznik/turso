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
use super::log::{framed_len, Kind, LogConfig};
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

/// A record a benchmark transaction writes, standing for a change.
const WRITE: Kind = Kind::new(100).unwrap();

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

/// A transaction that logs one record and commits, durable on memory: the log's admission,
/// reservation, copy and flush, the clock, the horizon and the commit logs.
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
                checkpoint_shards: 8,
                writeback_bytes: WRITEBACK_BYTES,
            },
            log_budget: None,
            txn: TxnConfig {
                slots: threads.max(TxnConfig::default().slots),
            },
        };
        let db = Arc::new(Atoll::open(io.clone(), "/db/bench", config).unwrap());
        Arc::new(move || {
            let mut txn = drive(&io, || db.transactions().begin(Isolation::ReadCommitted)).unwrap();
            let log = db.log();
            let mut admitted = drive(&io, || log.admit(framed_len(8))).unwrap();
            let start = txn.start().to_le_bytes();
            db.transactions()
                .append(&mut txn, &mut admitted, WRITE, &[&start]);
            let mut op = Commit::new(txn);
            drive(&io, || db.transactions().commit(&mut op)).unwrap();
        })
    });
}
