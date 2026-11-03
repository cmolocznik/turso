use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
use crate::sync::Mutex;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;

const BASE: &str = "/db/txn";

/// A record a test transaction writes, standing for a change.
const WRITE: Kind = Kind::new(100).unwrap();

fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 1 << 20,
        buffer_bytes: 256 << 10,
        chunk_bytes: 16 << 10,
        writes_in_flight: 3,
        direct: true,
    }
}

fn open(io: &Arc<dyn IO>, slots: usize) -> Atoll {
    let config = AtollConfig {
        log: log_config(),
        store: StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        buffer: BufferConfig {
            budget: MIN_BUDGET,
            checkpoint_shards: 8,
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
        },
        log_budget: Some(512 << 10),
        txn: TxnConfig { slots },
    };
    Atoll::open(io.clone(), BASE, config).unwrap()
}

fn begin(io: &Arc<dyn IO>, db: &Atoll, isolation: Isolation) -> Txn {
    let mut out = None;
    drive(io, || {
        out = Some(match db.transactions().begin(isolation)? {
            IOResult::Done(t) => t,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        });
        Ok(IOResult::Done(()))
    })
    .unwrap();
    out.unwrap()
}

/// Logs a record for `txn`, as a change would; returns its LSN.
fn write(io: &Arc<dyn IO>, db: &Atoll, txn: &mut Txn) -> Lsn {
    let log = db.log();
    let mut admitted = drive(io, || log.admit(framed_len(8))).unwrap();
    let start = txn.start().to_le_bytes();
    db.transactions()
        .append(txn, &mut admitted, WRITE, &[&start])
}

fn commit(io: &Arc<dyn IO>, db: &Atoll, txn: Txn) -> Option<Ts> {
    let mut op = Commit::new(txn);
    let mut out = None;
    drive(io, || {
        out = Some(match db.transactions().commit(&mut op)? {
            IOResult::Done(ts) => ts,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        });
        Ok(IOResult::Done(()))
    })
    .unwrap();
    out.unwrap()
}

/// A reader's snapshot, a transaction it looked at, and whether that was visible.
type Seen = (Ts, (u16, Ts), bool);

/// Writers commit transactions, and abort some that wrote nothing, while readers look at whether
/// the transactions begun so far are visible to their snapshots, under both isolations: each is
/// visible exactly when it committed at or below the snapshot, and a commit once acknowledged is
/// visible to the next snapshot taken.
fn a_snapshot_sees_exactly_the_transactions_committed_at_or_before_it(
    writers: u64,
    readers: u64,
    rounds: usize,
) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Arc::new(open(&io, (writers + readers) as usize));
    // Every writing transaction begun, and the commit timestamp of each that committed.
    let begun: Arc<Mutex<Vec<(u16, Ts)>>> = Arc::new(Mutex::new(Vec::new()));
    let committed: Arc<Mutex<HashMap<(u16, Ts), Ts>>> = Arc::new(Mutex::new(HashMap::new()));
    // What readers saw: their snapshot, the transaction, and whether it was visible.
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
    let mut threads = Vec::new();
    for w in 0..writers {
        let (io, db, begun, committed) = (io.clone(), db.clone(), begun.clone(), committed.clone());
        threads.push(crate::thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(w);
            for _ in 0..rounds {
                let mut txn = begin(&io, &db, Isolation::Snapshot);
                if rng.random_range(0..4) == 0 {
                    // Wrote nothing, and ends without committing.
                    db.transactions().abort(txn);
                    continue;
                }
                begun.lock().push((txn.slot(), txn.start()));
                write(&io, &db, &mut txn);
                let key = (txn.slot(), txn.start());
                let ts = commit(&io, &db, txn).expect("a commit timestamp");
                committed.lock().insert(key, ts);
                let mut after = begin(&io, &db, Isolation::Snapshot);
                db.transactions().statement(&mut after).unwrap();
                assert!(
                    db.transactions().visible(&mut after, key.0, key.1, 1),
                    "{key:?}, committed at {ts}, unseen by a snapshot taken after"
                );
                db.transactions().abort(after);
            }
        }));
    }
    for r in 0..readers {
        let (io, db, begun, seen) = (io.clone(), db.clone(), begun.clone(), seen.clone());
        threads.push(crate::thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(100 + r);
            let isolation = [Isolation::Snapshot, Isolation::ReadCommitted][r as usize % 2];
            for _ in 0..rounds / 2 {
                let mut txn = begin(&io, &db, isolation);
                for _ in 0..3 {
                    db.transactions().statement(&mut txn).unwrap();
                    let known = begun.lock().clone();
                    for _ in 0..20 {
                        let Some(&(slot, start)) =
                            known.get(rng.random_range(0..known.len().max(1)))
                        else {
                            continue;
                        };
                        let v = db.transactions().visible(&mut txn, slot, start, 1);
                        // Asked again in the same statement, the answer holds.
                        assert_eq!(v, db.transactions().visible(&mut txn, slot, start, 1));
                        seen.lock().push((txn.snapshot, (slot, start), v));
                    }
                }
                db.transactions().abort(txn);
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
    let committed = std::mem::take(&mut *committed.lock());
    let seen = std::mem::take(&mut *seen.lock());
    if rounds >= 100 {
        // Both answers come up: visible, and committed only after the snapshot.
        let after = seen
            .iter()
            .filter(|&&(at, key, v)| !v && committed.get(&key).is_some_and(|&ts| ts > at))
            .count();
        assert!(
            after > 0 && seen.iter().any(|s| s.2),
            "{after} of {}",
            seen.len()
        );
    }
    for (snapshot, key, visible) in seen {
        let before = committed.get(&key).is_some_and(|&ts| ts <= snapshot);
        assert_eq!(
            visible,
            before,
            "{key:?} at snapshot {snapshot}: committed at {:?}",
            committed.get(&key)
        );
    }
}

#[cfg(not(shuttle))]
#[test]
fn a_snapshot_sees_exactly_the_transactions_committed_at_or_before_it_on_threads() {
    a_snapshot_sees_exactly_the_transactions_committed_at_or_before_it(6, 6, 400);
}

#[cfg(shuttle)]
#[test]
fn a_snapshot_sees_exactly_the_transactions_committed_at_or_before_it_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || a_snapshot_sees_exactly_the_transactions_committed_at_or_before_it(2, 1, 4),
        300,
    );
}

#[test]
fn a_transaction_sees_its_own_writes_from_its_next_statement_and_others_once_committed() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 4);
    let txns = db.transactions();
    let mut a = begin(&io, &db, Isolation::Snapshot);
    txns.statement(&mut a).unwrap();
    write(&io, &db, &mut a);
    let (slot, start) = (a.slot(), a.start());
    assert!(
        !txns.visible(&mut a, slot, start, 1),
        "its own statement's write"
    );
    txns.statement(&mut a).unwrap();
    assert!(
        txns.visible(&mut a, slot, start, 1),
        "an earlier statement's write"
    );
    let mut snapshot = begin(&io, &db, Isolation::Snapshot);
    let mut committed = begin(&io, &db, Isolation::ReadCommitted);
    for t in [&mut snapshot, &mut committed] {
        txns.statement(t).unwrap();
        assert!(!txns.visible(t, slot, start, 1), "a write not committed");
    }
    commit(&io, &db, a);
    for t in [&mut snapshot, &mut committed] {
        assert!(
            !txns.visible(t, slot, start, 1),
            "within a statement, a commit after its snapshot"
        );
        txns.statement(t).unwrap();
    }
    assert!(
        !txns.visible(&mut snapshot, slot, start, 1),
        "snapshot isolation"
    );
    assert!(
        txns.visible(&mut committed, slot, start, 1),
        "read committed"
    );
    let mut later = begin(&io, &db, Isolation::Snapshot);
    txns.statement(&mut later).unwrap();
    assert!(txns.visible(&mut later, slot, start, 1));
}

#[test]
fn a_commit_logs_its_timestamp_and_a_read_only_one_logs_nothing() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 4);
    let end = db.log().end();
    let reader = begin(&io, &db, Isolation::Snapshot);
    assert_eq!(commit(&io, &db, reader), None);
    assert_eq!(db.log().end(), end, "a read-only commit logged");
    let mut txn = begin(&io, &db, Isolation::Snapshot);
    let start = txn.start();
    let written = write(&io, &db, &mut txn);
    let group = txn.tail().expect("a group written");
    let ts = commit(&io, &db, txn).unwrap();
    assert!(ts > start);
    assert!(db.log().durable() >= db.log().end(), "done before durable");
    let mut reader = Log::reader(io.clone(), BASE, &log_config(), written);
    let r = reader.next_record().unwrap().expect("the Commit record");
    assert_eq!(r.header.kind, COMMIT);
    assert_eq!((r.header.txn, r.header.prev), (start, Some(group)));
    assert_eq!(commit_ts(&r.payload), Some(ts));
}

#[test]
fn begin_waits_for_a_free_slot() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 2);
    let txns = db.transactions();
    let a = begin(&io, &db, Isolation::Snapshot);
    let _b = begin(&io, &db, Isolation::Snapshot);
    let IOResult::IO(IOCompletions(wait)) = txns.begin(Isolation::Snapshot).unwrap() else {
        panic!("a third transaction began with two slots");
    };
    assert!(!wait.finished());
    commit(&io, &db, a);
    assert!(wait.finished(), "freeing a slot wakes a waiter");
    assert!(matches!(
        txns.begin(Isolation::Snapshot).unwrap(),
        IOResult::Done(_)
    ));
}

#[test]
fn the_clock_restarts_above_every_timestamp_and_what_was_written_reads_committed() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let mut versions = Vec::new();
    // First the newest timestamps are in the log past the redo point; then a checkpoint after
    // the last commit leaves them only in the superblock.
    for checkpoint_last in [false, true] {
        let db = open(&io, 4);
        let mut last = 0;
        for i in 0..20 {
            let mut txn = begin(&io, &db, Isolation::Snapshot);
            write(&io, &db, &mut txn);
            versions.push((txn.slot(), txn.start()));
            last = commit(&io, &db, txn).unwrap();
            if i == 10 || (checkpoint_last && i == 19) {
                let ticket = db.checkpointer().request_full();
                drive(&io, || db.checkpointer().step_full(ticket)).unwrap();
            }
        }
        // A transaction that wrote and never ended: its id is logged, and no new one may reuse
        // it.
        let mut open_one = begin(&io, &db, Isolation::Snapshot);
        write(&io, &db, &mut open_one);
        drive(&io, || db.log().wait_durable(db.log().end())).unwrap();
        let last = last.max(open_one.start());
        std::mem::forget(open_one);
        drop(db);
        let db = open(&io, 4);
        assert!(db.recovered_end().is_some());
        let mut txn = begin(&io, &db, Isolation::Snapshot);
        assert!(
            txn.start() > last,
            "checkpoint last {checkpoint_last}: {} after {last}",
            txn.start()
        );
        db.transactions().statement(&mut txn).unwrap();
        for &(slot, start) in &versions {
            assert!(db.transactions().visible(&mut txn, slot, start, 1));
        }
    }
}

/// A full checkpoint, done; returns the superblock it leaves in charge.
fn checkpoint(io: &Arc<dyn IO>, db: &Atoll) -> crate::storage::atoll::control::Superblock {
    let ticket = db.checkpointer().request_full();
    drive(io, || db.checkpointer().step_full(ticket)).unwrap();
    db.checkpointer().superblock()
}

/// Commits transactions of 32 KiB records until `bytes` of log are appended.
fn fill(io: &Arc<dyn IO>, db: &Atoll, bytes: u64) {
    let filler = vec![7u8; 32 << 10];
    let end = db.log().end() + bytes;
    while db.log().end() < end {
        let mut txn = begin(io, db, Isolation::Snapshot);
        let mut admitted = drive(io, || db.log().admit(framed_len(filler.len()))).unwrap();
        db.transactions()
            .append(&mut txn, &mut admitted, WRITE, &[&filler]);
        commit(io, db, txn);
    }
}

#[test]
fn a_writer_keeps_its_log_across_checkpoints_until_it_commits() {
    use crate::storage::atoll::log::{decode_table, Writer};
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 4);
    let mut long = begin(&io, &db, Isolation::Snapshot);
    write(&io, &db, &mut long);
    let first = long.tail().unwrap();
    let segment = log_config().segment_bytes;
    // Three segments' worth committed by others, checkpointed after each.
    for _ in 0..3 {
        fill(&io, &db, segment);
        checkpoint(&io, &db);
    }
    let sb = checkpoint(&io, &db);
    assert!(
        sb.redo >= first.get() + 2 * segment,
        "the redo point {} never passed the writer's segment",
        sb.redo
    );
    let table = Log::read_group(io.clone(), BASE, &log_config(), sb.writers.expect("a table"))
        .unwrap();
    assert_eq!(
        decode_table(&table[0].payload).unwrap(),
        vec![Writer {
            txn: long.start(),
            first,
            last: first
        }]
    );
    let group = Log::read_group(io.clone(), BASE, &log_config(), first)
        .expect("the writer's first group deleted while it may need undo");
    assert_eq!(group[0].header.txn, long.start());
    let held = db.checkpointer().retention().expect("retention reported");
    assert_eq!((held.writer.txn, held.bytes), (long.start(), sb.redo - first.get()));
    // Once it commits, the next checkpoint that moves the redo point lets its log go.
    commit(&io, &db, long);
    fill(&io, &db, segment);
    let sb = checkpoint(&io, &db);
    assert_eq!(sb.writers, None, "a table listing no one");
    assert!(
        Log::read_group(io.clone(), BASE, &log_config(), first).is_err(),
        "the committed writer's segment kept"
    );
    assert_eq!(db.checkpointer().retention(), None);
}
