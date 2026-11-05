use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::{Mode, Put, Tree};
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
use crate::sync::Mutex;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;

const BASE: &str = "/db/txn";

/// A record a test transaction writes, standing for a change.
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
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
            kept_bytes: None,
        },
        log_budget: Some(512 << 10),
        checkpoint_write_bytes: None,
        txn: TxnConfig { slots },
    };
    let db = Atoll::open(io.clone(), BASE, config).unwrap();
    if drive(io, || db.store().entry(root())).unwrap() == FREE {
        let made = drive(io, || Tree::create(db.pool(), root().class())).unwrap();
        assert_eq!(made, root(), "the first page a new database allocates");
        // In a checkpoint before any write names it, as a database's system trees are.
        checkpoint(io, &db);
    }
    db
}

/// The plain tree the tests' transactions write to.
fn root() -> PageId {
    PageId::new(SizeClass::new(0).unwrap(), 0)
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

/// A change of `txn`'s own: its start, put into the tests' plain tree, as a SQL write to a plain
/// tree is made and noted.
fn write(io: &Arc<dyn IO>, db: &Atoll, txn: &mut Txn) {
    let key = txn.start().to_be_bytes();
    let tree = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root());
    let mut put = Put::new(&key, vec![1], Mode::Upsert);
    drive(io, || tree.put(&mut put)).unwrap();
    txn.changed(|s, c| c.plain_put(s, root(), &key, &[1]));
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
                    db.transactions().visible(&mut after, key.0, key.1),
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
                        let v = db.transactions().visible(&mut txn, slot, start);
                        // Asked again in the same statement, the answer holds.
                        assert_eq!(v, db.transactions().visible(&mut txn, slot, start));
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
fn a_transaction_sees_its_own_writes_at_once_and_others_once_committed() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 4);
    let txns = db.transactions();
    let mut a = begin(&io, &db, Isolation::Snapshot);
    txns.statement(&mut a).unwrap();
    write(&io, &db, &mut a);
    let (slot, start) = (a.slot(), a.start());
    assert!(
        txns.visible(&mut a, slot, start),
        "its own statement's write"
    );
    txns.statement(&mut a).unwrap();
    assert!(
        txns.visible(&mut a, slot, start),
        "an earlier statement's write"
    );
    let mut snapshot = begin(&io, &db, Isolation::Snapshot);
    let mut committed = begin(&io, &db, Isolation::ReadCommitted);
    for t in [&mut snapshot, &mut committed] {
        txns.statement(t).unwrap();
        assert!(!txns.visible(t, slot, start), "a write not committed");
    }
    commit(&io, &db, a);
    for t in [&mut snapshot, &mut committed] {
        assert!(
            !txns.visible(t, slot, start),
            "within a statement, a commit after its snapshot"
        );
        txns.statement(t).unwrap();
    }
    assert!(
        !txns.visible(&mut snapshot, slot, start),
        "snapshot isolation"
    );
    assert!(txns.visible(&mut committed, slot, start), "read committed");
    let mut later = begin(&io, &db, Isolation::Snapshot);
    txns.statement(&mut later).unwrap();
    assert!(txns.visible(&mut later, slot, start));
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
    write(&io, &db, &mut txn);
    assert_eq!(db.log().end(), end, "a change logged before its commit");
    let ts = commit(&io, &db, txn).unwrap();
    assert!(ts > start);
    assert!(db.log().durable() >= db.log().end(), "done before durable");
    let mut reader = Log::reader(io.clone(), BASE, &log_config(), end);
    let r = reader.next_record().unwrap().expect("the Commit record");
    assert_eq!(r.header.kind, COMMIT);
    assert_eq!(r.header.txn, start);
    assert_eq!(commit_ts(&r.payload), Some(ts));
    assert!(
        reader.next_record().unwrap().is_none(),
        "a record past the Commit"
    );
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
                checkpoint(&io, &db);
            }
        }
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
            assert!(db.transactions().visible(&mut txn, slot, start));
        }
    }
}

/// A full checkpoint, done.
fn checkpoint(io: &Arc<dyn IO>, db: &Atoll) {
    let ticket = db.checkpointer().request_full();
    drive(io, || {
        db.checkpointer().step_full(ticket, db.transactions())
    })
    .unwrap();
}

/// A cut's snapshot is the clock as the cut read it. A transaction that commits after, which may
/// change pages the cut's walk has passed, lies above it, even where the horizon covers that commit
/// before the cut returns: a commit drawn earlier and published late held the cut waiting.
#[test]
fn a_cuts_snapshot_is_the_clock_it_read_never_a_commit_drawn_after() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 4);
    let txns = db.transactions();
    let held = begin(&io, &db, Isolation::Snapshot);
    let drawn = txns.horizon.draw(held.slot(), &txns.clock);
    let mut cut = Cut::default();
    assert!(
        matches!(txns.cut(&mut cut).unwrap(), IOResult::IO(_)),
        "the cut waits for the commit drawn before it"
    );
    let mut t = begin(&io, &db, Isolation::Snapshot);
    txns.statement(&mut t).unwrap();
    write(&io, &db, &mut t);
    let mut commit = Commit::new(t);
    assert!(
        matches!(txns.commit(&mut commit).unwrap(), IOResult::IO(_)),
        "a commit above one unpublished waits for it"
    );
    txns.horizon.published(held.slot(), &txns.clock);
    let ts = drive(&io, || txns.commit(&mut commit))
        .unwrap()
        .expect("a commit timestamp");
    let (_, c) = drive(&io, || txns.cut(&mut cut)).unwrap();
    assert!(
        txns.horizon.at() >= ts,
        "the horizon covers the later commit"
    );
    assert!(
        drawn <= c && c < ts,
        "the cut's snapshot {c}, drawn {drawn} before it and {ts} after"
    );
    txns.abort(held);
}

/// Commits, some long enough to spill parts ahead of their Commit, run beside checkpoints' cuts
/// (audit 18): every part and Commit of a commit above a cut's C lies at or after its L, and every
/// Commit before L is at or below C.
fn a_cut_divides_the_log_by_commit_timestamp(writers: usize, commits: usize, cuts: usize) {
    use crate::storage::atoll::log::Log;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Arc::new(open(&io, writers + 1));
    let spill = log_config().chunk_bytes as usize + 1;
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let (io, db) = (io.clone(), db.clone());
            crate::thread::spawn(move || {
                for i in 0..commits {
                    let mut t = begin(&io, &db, Isolation::Snapshot);
                    db.transactions().statement(&mut t).unwrap();
                    write(&io, &db, &mut t);
                    let len = if (w + i) % 2 == 0 { spill } else { 8 };
                    let key = (i as u64).to_be_bytes();
                    t.changed(|s, c| c.plain_put(s, root(), &key, &vec![w as u8; len]));
                    commit(&io, &db, t);
                }
            })
        })
        .collect();
    let mut taken = Vec::new();
    for _ in 0..cuts {
        let mut op = Cut::default();
        let mut out = None;
        drive(&io, || {
            out = Some(match db.transactions().cut(&mut op)? {
                IOResult::Done(at) => at,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            });
            Ok(IOResult::Done(()))
        })
        .unwrap();
        taken.push(out.unwrap());
        crate::thread::yield_now();
    }
    for h in handles {
        h.join().unwrap();
    }
    // Per transaction, where its first part or its Commit starts, where its Commit starts, and
    // its timestamp.
    let mut commits_seen: HashMap<Ts, (Lsn, Option<(Lsn, Ts)>)> = HashMap::new();
    let mut reader = Log::reader(io.clone(), BASE, &log_config(), 0);
    while let Some(r) = reader.next_record().unwrap() {
        if r.header.kind == SPILL {
            let start = u64::from_le_bytes(r.payload[..8].try_into().unwrap());
            commits_seen.entry(start).or_insert((r.start, None));
        } else if r.header.kind == COMMIT {
            let ts = commit_ts(&r.payload).unwrap();
            commits_seen
                .entry(r.header.txn)
                .or_insert((r.start, None))
                .1 = Some((r.start, ts));
        }
    }
    assert_eq!(commits_seen.len(), writers * commits);
    let spilled = commits_seen
        .values()
        .filter(|(first, c)| Some(*first) != c.map(|c| c.0));
    assert!(spilled.count() > 0, "no commit spilled a part");
    for &(l, c) in &taken {
        for (start, &(first, commit)) in &commits_seen {
            let (at, ts) = commit.expect("a Commit");
            if ts > c {
                assert!(
                    first >= l,
                    "commit {start} at {ts}, above {c}, has a part at {first}, before {l}"
                );
            }
            if at < l {
                assert!(ts <= c, "commit {start} before {l} at {ts}, above {c}");
            }
        }
    }
}

#[test]
fn a_cut_divides_the_log_by_commit_timestamp_on_threads() {
    a_cut_divides_the_log_by_commit_timestamp(4, 40, 200);
}

#[cfg(shuttle)]
#[test]
fn a_cut_divides_the_log_by_commit_timestamp_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || a_cut_divides_the_log_by_commit_timestamp(2, 2, 2),
        300,
    );
}
