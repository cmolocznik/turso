use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::{Mode, Put, Tree};
use crate::storage::atoll::buffer::{BufferConfig, MIN_DB_BUDGET};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::executor::Class;
use crate::storage::atoll::locks::Mutex;
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
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
    open_with_budget(io, slots, 512 << 10)
}

/// [`open`], recovery kept to replaying `log_budget` bytes of log.
fn open_with_budget(io: &Arc<dyn IO>, slots: usize, log_budget: u64) -> Atoll {
    open_in(
        io,
        slots,
        log_budget,
        crate::storage::atoll::checkpoint::Checkpointing::Writers,
    )
}

/// [`open`], its checkpoints stepped as `checkpointing` says.
fn open_in(
    io: &Arc<dyn IO>,
    slots: usize,
    log_budget: u64,
    checkpointing: crate::storage::atoll::checkpoint::Checkpointing,
) -> Atoll {
    let config = AtollConfig {
        log: log_config(),
        store: StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        buffer: BufferConfig {
            budget: MIN_DB_BUDGET,
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
            kept_bytes: None,
            lent_at_open: Default::default(),
        },
        log_budget: Some(log_budget),
        checkpoint_write_bytes: None,
        checkpointing,
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
/// visible to the next snapshot taken. Both answers come up on every schedule: a transaction begun
/// before the threads and committed only after them is invisible to every reader's statement, and
/// a snapshot taken last sees every commit. Nothing is asserted while the database is open: a
/// thread's failure is raised once it is closed (DESIGN.md 22.11).
fn a_snapshot_sees_exactly_the_transactions_committed_at_or_before_it(
    writers: u64,
    readers: u64,
    rounds: usize,
) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Arc::new(open(&io, (writers + readers + 1) as usize));
    // Every writing transaction begun, and the commit timestamp of each that committed.
    let begun: Arc<Mutex<Vec<(u16, Ts)>>> = Arc::new(Mutex::new(Vec::new()));
    let committed: Arc<Mutex<HashMap<(u16, Ts), Ts>>> = Arc::new(Mutex::new(HashMap::new()));
    // What readers saw: their snapshot, the transaction, and whether it was visible.
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
    // Committed once every thread is done: after every reader's snapshot.
    let mut held = begin(&io, &db, Isolation::Snapshot);
    write(&io, &db, &mut held);
    let held_key = (held.slot(), held.start());
    begun.lock().push(held_key);
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
                let visible = db.transactions().visible(&mut after, key.0, key.1);
                db.transactions().abort(after);
                assert!(
                    visible,
                    "{key:?}, committed at {ts}, unseen by a snapshot taken after"
                );
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
                    let sampled = (0..20).map(|_| known[rng.random_range(0..known.len())]);
                    for (slot, start) in std::iter::once(held_key).chain(sampled) {
                        let v = db.transactions().visible(&mut txn, slot, start);
                        // Asked again in the same statement, the answer holds.
                        let again = db.transactions().visible(&mut txn, slot, start);
                        seen.lock().push((txn.snapshot, (slot, start), v));
                        if v != again {
                            db.transactions().abort(txn);
                            panic!("{slot}/{start}: {v}, then {again} in one statement");
                        }
                    }
                }
                db.transactions().abort(txn);
            }
        }));
    }
    let joined: Vec<_> = threads.into_iter().map(|t| t.join()).collect();
    let ts = commit(&io, &db, held).expect("a commit timestamp");
    committed.lock().insert(held_key, ts);
    let committed = std::mem::take(&mut *committed.lock());
    let mut last = begin(&io, &db, Isolation::Snapshot);
    db.transactions().statement(&mut last).unwrap();
    let mut seen = std::mem::take(&mut *seen.lock());
    for &key in committed.keys() {
        let v = db.transactions().visible(&mut last, key.0, key.1);
        seen.push((last.snapshot, key, v));
    }
    db.transactions().abort(last);
    drop(db);
    for result in joined {
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    }
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
        assert!(db.recovered());
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
    let mut ticket = db.checkpointer().request_full();
    drive(io, || {
        db.checkpointer().step_full(&mut ticket, db.transactions())
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

// A real spill pauses after taking log space and before publishing its first position.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn audit_a_cut_waits_for_a_reserving_spill_without_ready_retries() {
    use crate::storage::atoll::log::Log;
    use std::sync::{
        atomic::{AtomicUsize as Count, Ordering as Order},
        mpsc,
    };
    use std::task::{Wake, Waker};
    struct Seen(Count);
    impl Wake for Seen {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Order::Relaxed);
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, Order::Relaxed);
        }
    }
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Arc::new(open(&io, 4));
    let mut txn = begin(&io, &db, Isolation::Snapshot);
    db.transactions().statement(&mut txn).unwrap();
    for _ in 0..2000 {
        write(&io, &db, &mut txn);
    }
    let start = txn.start();
    let sealed = txn.changes.sealed_len().is_some();
    let (paused_tx, paused_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let (spilled_tx, spilled_rx) = mpsc::sync_channel(1);
    let (commit_tx, commit_rx) = mpsc::sync_channel(1);
    let worker = {
        let io = io.clone();
        let db = db.clone();
        std::thread::spawn(move || {
            crate::race::at("txn.before_reserved_at", move || {
                let _ = paused_tx.send(());
                let _ = resume_rx.recv_timeout(std::time::Duration::from_secs(15));
            });
            if drive(&io, || db.transactions().spill(&mut txn)).is_err() {
                db.transactions().abort(txn);
                let _ = spilled_tx.send(false);
                return None;
            }
            let _ = spilled_tx.send(true);
            let _ = commit_rx.recv_timeout(std::time::Duration::from_secs(15));
            commit(&io, &db, txn)
        })
    };
    let paused = paused_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .is_ok();
    let captured_log_end = db.log().end();
    let seen = std::sync::Arc::new(Seen(Count::new(0)));
    let waker = Waker::from(seen.clone());
    let mut cut = Cut::default();
    let mut other_cut = Cut::default();
    let mut ready = 0;
    let mut proper_kind = true;
    let mut pending = 0;
    let mut early_done = false;
    let mut error = None;
    let mut last = None;
    if paused {
        for _ in 0..32 {
            match db.transactions().cut(&mut cut) {
                Ok(IOResult::IO(IOCompletions(c))) => {
                    c.set_waker(&waker);
                    if !c.finished() {
                        proper_kind &= c.is_engine_wait() && c.is_remote_wait();
                    }
                    let finished = c.finished();
                    last = Some(c);
                    if finished {
                        ready += 1;
                    } else {
                        pending += 1;
                        break;
                    }
                }
                Ok(IOResult::Done(_)) => {
                    early_done = true;
                    break;
                }
                Err(e) => {
                    error = Some(e.to_string());
                    break;
                }
            }
        }
    }
    // A second cut must also decode the WAITED mark as reserving, not as an LSN.
    let other_wait = if paused {
        match db.transactions().cut(&mut other_cut) {
            Ok(IOResult::IO(IOCompletions(c))) => Some(c),
            _ => None,
        }
    } else {
        None
    };
    let other_pending = other_wait.as_ref().is_some_and(|c| !c.finished());
    let wakes_before = seen.0.load(Order::Relaxed);
    let _ = resume_tx.send(());
    let spilled = spilled_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .ok();
    let finished_after = last.as_ref().is_some_and(Completion::finished);
    let other_finished = other_wait.as_ref().is_some_and(Completion::finished);
    let wakes_after = seen.0.load(Order::Relaxed);
    let cut_result = if spilled == Some(true) {
        drive(&io, || db.transactions().cut(&mut cut)).ok()
    } else {
        None
    };
    let other_result = if spilled == Some(true) {
        drive(&io, || db.transactions().cut(&mut other_cut)).ok()
    } else {
        None
    };
    let _ = commit_tx.send(());
    let committed = worker.join().ok().flatten();
    let mut reader = Log::reader(io.clone(), BASE, &log_config(), 0);
    let mut first_part = None;
    while let Some(r) = reader.next_record().unwrap() {
        if r.header.kind == SPILL && u64::from_le_bytes(r.payload[..8].try_into().unwrap()) == start
        {
            first_part = Some(first_part.map_or(r.start, |p: Lsn| p.min(r.start)));
        }
    }
    drop(reader);
    drop(last);
    drop(other_wait);
    drop(waker);
    drop(seen);
    drop(db);
    drop(io);
    println!("RESERVATION_NATIVE sealed={sealed} paused={paused} ready={ready} pending={pending} early_done={early_done} error={error:?} other_pending={other_pending} wakes_before={wakes_before} wakes_after={wakes_after} finished_after={finished_after} other_finished={other_finished} spilled={spilled:?} captured_log_end={captured_log_end} first_part={first_part:?} cut={cut_result:?} other_cut={other_result:?} committed={committed:?}");
    assert!(
        sealed && paused && spilled == Some(true) && error.is_none() && !early_done,
        "real spill/cut setup and cleanup must complete"
    );
    assert!(
        finished_after && other_finished,
        "publishing a marked reservation must finish both cut waits"
    );
    let p = first_part.expect("real SPILL record read back from the log");
    let (l, c) = cut_result.expect("cut done after spill publication");
    let (other_l, other_c) = other_result.expect("second cut done after publication");
    assert!(
        p < captured_log_end
            && committed.is_some_and(|t| t > c && t > other_c)
            && l == p
            && other_l == p,
        "cut must retain the first part of a commit above its snapshot"
    );
    assert_eq!(
        (ready, pending, wakes_before),
        (0, 1, 0),
        "a reserving spill must park the cut rather than hand back ready retries"
    );
    assert!(proper_kind, "reservation waits must be remote engine waits");
    assert!(
        other_pending,
        "a second cut must wait on an already marked reservation"
    );
    assert!(
        wakes_after > wakes_before,
        "publishing the first position wakes the registered cut"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_reservation_published_before_the_cut_marks_it_does_not_strand_the_cut() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Arc::new(open(&io, 2));
    let txns = db.transactions();
    let txn = begin(&io, &db, Isolation::Snapshot);
    let slot = txn.slot();
    txns.reserving(slot);
    // A unit interleaving of the mark helpers: the real SPILL test separately checks the log.
    let publisher = db.clone();
    crate::race::at("txn.reservation_wait", move || {
        publisher
            .transactions()
            .reserved_at(slot, GroupStart::at(0));
    });
    let mut cut = Cut::default();
    let result = txns.cut(&mut cut).unwrap();
    let stranded = match &result {
        IOResult::IO(IOCompletions(wait)) => !wait.finished(),
        IOResult::Done(_) => false,
    };
    let point_taken = !crate::race::pending("txn.reservation_wait");
    let final_cut = drive(&io, || txns.cut(&mut cut)).unwrap();
    txns.abort(txn);
    drop(result);
    drop(db);
    drop(io);
    assert!(point_taken, "publication must run between capture and mark");
    assert!(
        !stranded,
        "a cut that lost its mark returned an unfinished wait with no producer"
    );
    assert_eq!(
        final_cut.0, 0,
        "the cut reads the published position after the lost mark"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_later_log_part_does_not_replace_a_reservations_first_position() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 2);
    let txns = db.transactions();
    let txn = begin(&io, &db, Isolation::Snapshot);
    let slot = txn.slot();
    txns.reserving(slot);
    txns.reserved_at(slot, GroupStart::at(0));
    let first = txns.reserved[slot as usize].load(Ordering::SeqCst);
    txns.reserving(slot);
    txns.reserved_at(slot, GroupStart::at(BLOCK));
    let later = txns.reserved[slot as usize].load(Ordering::SeqCst);
    let domain_unused = txns.reservation_completions.get().is_none();
    txns.abort(txn);
    drop(db);
    drop(io);
    assert_eq!(decode_reservation(first), Reservation::Position(0));
    assert_eq!(
        later, first,
        "a later part overwrote the first reservation position"
    );
    assert!(
        domain_unused,
        "an unmarked publication initializes no completion domain"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn uncontended_commits_never_initialize_reservation_completions() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 2);
    let unused_before = db.transactions().reservation_completions.get().is_none();
    for _ in 0..8 {
        let mut txn = begin(&io, &db, Isolation::Snapshot);
        db.transactions().statement(&mut txn).unwrap();
        write(&io, &db, &mut txn);
        commit(&io, &db, txn);
    }
    let unused_after = db.transactions().reservation_completions.get().is_none();
    drop(db);
    drop(io);
    assert!(
        unused_before && unused_after,
        "uncontended commits initialized reservation completions"
    );
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn reservation_words_leave_the_flag_bits_separate_from_positions() {
    assert_eq!(decode_reservation(0), Reservation::None);
    assert_eq!(decode_reservation(RESERVING), Reservation::Reserving);
    assert_eq!(decode_reservation(RESERVING_WAITED), Reservation::Reserving);
    for at in [0, 1, BLOCK, RESERVATION_WAITED - 2] {
        assert_eq!(
            decode_reservation(encode_reservation(at)),
            Reservation::Position(at)
        );
    }
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
#[should_panic(expected = "reservation position exhausted")]
fn a_reservation_position_cannot_use_a_flag_bit() {
    encode_reservation(RESERVATION_WAITED - 1);
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
#[should_panic(expected = "reservation position exhausted")]
fn an_overflowing_reservation_position_is_refused_before_addition() {
    encode_reservation(u64::MAX);
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
#[should_panic(expected = "invalid reservation word")]
fn a_waited_bit_without_a_reservation_is_refused() {
    decode_reservation(RESERVATION_WAITED);
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
#[should_panic(expected = "invalid reservation word")]
fn a_reservation_cannot_also_have_position_bits() {
    decode_reservation(RESERVING | 1);
}

/// Commits transactions of `bytes`-byte changes into the tests' plain tree until a checkpoint is
/// due by the log appended since the last began: what a writing statement then finds.
fn append_until_due(io: &Arc<dyn IO>, db: &Atoll, bytes: usize) {
    let tree = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root());
    let value = vec![7u8; bytes];
    while !db.checkpointer().is_due() {
        let mut txn = begin(io, db, Isolation::Snapshot);
        let key = txn.start().to_be_bytes();
        let mut put = Put::new(&key, value.clone(), Mode::Upsert);
        drive(io, || tree.put(&mut put)).unwrap();
        txn.changed(|s, c| c.plain_put(s, root(), &key, &value));
        commit(io, db, txn);
    }
}

/// A writing statement that finds a checkpoint due raises the checkpoint's task to the due class
/// as it wakes it; the task's own step that finds nothing more due drops it to maintenance again
/// (DESIGN.md 22.9, Due; 22.4; mutants: a trigger that only wakes, a step that never clears).
#[test]
fn a_checkpoint_due_raises_its_tasks_level_until_its_step_finds_none_due() {
    // IO that completes only as it is stepped: a unit's writes stay in flight, so a call
    // returns with its checkpoint under way.
    let io: Arc<dyn IO> = Arc::new(crate::io::crash::CrashIO::new(0));
    let db = open(&io, 4);
    let cp = db.checkpointer();
    let rested = cp.task_class();
    append_until_due(&io, &db, 32 << 10);
    let before = cp.task_class();
    let mut next = cp.step_due(db.transactions()).unwrap();
    let under_way = (cp.is_under_way(), cp.task_class());
    // The statements after it step the checkpoint under way to its end: done once it ended.
    while let IOResult::IO(IOCompletions(c)) = next {
        io.wait(&c);
        next = cp.step_due(db.transactions()).unwrap();
    }
    let ended = (cp.is_under_way(), cp.is_due(), cp.task_class());
    drop(db);
    assert_eq!(rested, Some(Class::Maintenance), "nothing due at open");
    assert_eq!(
        before,
        Some(Class::Maintenance),
        "due, but no statement looked"
    );
    assert_eq!(
        under_way,
        (true, Some(Class::Due)),
        "the statement's trigger"
    );
    assert_eq!(
        ended,
        (false, false, Some(Class::Maintenance)),
        "nothing due once it ended"
    );
}

/// A checkpoint ending with another share of the log appended since it began is due again: its
/// task goes on rather than rest, keeping its level, with no statement to trigger it (DESIGN.md
/// 22.9, Due: the task compares the two itself before it returns Idle; mutant: no compare).
#[test]
fn a_checkpoint_ending_due_again_keeps_its_tasks_level() {
    // IO that completes only as it is stepped: a unit's writes stay in flight, so a call
    // returns with its checkpoint under way.
    let io: Arc<dyn IO> = Arc::new(crate::io::crash::CrashIO::new(0));
    let db = open(&io, 4);
    let cp = db.checkpointer();
    append_until_due(&io, &db, 32 << 10);
    if let IOResult::IO(IOCompletions(c)) = cp.step_due(db.transactions()).unwrap() {
        io.wait(&c);
    }
    let began = cp.is_under_way();
    // Another share while it runs: no statement steps the checkpoint, the commits only append.
    append_until_due(&io, &db, 32 << 10);
    let still = cp.is_under_way();
    while let IOResult::IO(IOCompletions(c)) = cp.step(db.transactions()).unwrap() {
        io.wait(&c);
    }
    let ended = (cp.is_under_way(), cp.is_due(), cp.task_class());
    drop(db);
    assert!(
        began && still,
        "the first checkpoint began, and ran on past the appends"
    );
    assert_eq!(
        ended,
        (false, true, Some(Class::Due)),
        "the task, due again as its checkpoint ended"
    );
}

/// Slots a checkpoint freed that its own erasure could not take yet, still waiting for a step
/// that may have looked them up, are erased by the writers' steps that follow that step's end
/// while nothing is due, and passed over before it: the erasure held back lowered its level and
/// waits for the step's end, which raises it (DESIGN.md 22.9, the auditor's P2-CP-E1; mutants:
/// owed erasure passed over, the level not kept, the wait's notify leaving the level down).
#[test]
fn slots_owed_erasure_are_erased_by_the_steps_that_follow_with_nothing_due() {
    use crate::storage::atoll::checkpoint::STEPS_PASSED_OVER;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 4);
    let tree = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root());
    // The same keys rewritten between full checkpoints: each frees the slots of the images the
    // one before wrote. Another thread's step stands through the last, as a reader's would, so
    // the slots that checkpoint frees wait for it, and its own erasure leaves them owed.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (leave_tx, leave_rx) = std::sync::mpsc::channel::<()>();
    let reader = {
        let epochs = db.store().epochs().clone();
        std::thread::spawn(move || {
            let step = epochs.enter();
            entered_tx.send(()).unwrap();
            let _ = leave_rx.recv();
            drop(step);
        })
    };
    for round in 0..3u8 {
        for k in 0..64u64 {
            let mut put = Put::new(&k.to_be_bytes(), vec![round; 512], Mode::Upsert);
            drive(&io, || tree.put(&mut put)).unwrap();
        }
        if round == 2 {
            entered_rx.recv().unwrap();
        }
        checkpoint(&io, &db);
    }
    // While the reader's step stands, statements pass over the erasure it holds back.
    let held = db.checkpointer().erasure_held();
    let (passed_held, stepped_held) = {
        use crate::storage::atoll::checkpoint::WRITER_STEPS;
        let (passed, stepped) = (
            STEPS_PASSED_OVER.with(|n| n.get()),
            WRITER_STEPS.with(|n| n.get()),
        );
        for _ in 0..16 {
            drive(&io, || db.checkpointer().step_due(db.transactions())).unwrap();
        }
        (
            STEPS_PASSED_OVER.with(|n| n.get()) - passed,
            WRITER_STEPS.with(|n| n.get()) - stepped,
        )
    };
    // The reader's step ends: the slots may be erased now.
    let _ = leave_tx.send(());
    let joined = reader.join().is_ok();
    let cp = db.checkpointer();
    let owed = db.store().erase_owed();
    let due = cp.is_due() || cp.is_under_way();
    let passed = STEPS_PASSED_OVER.with(|n| n.get());
    let bound = owed + db.store().epochs().participants() + 1;
    let mut steps = 0;
    while db.store().erasure_owed() && steps < bound {
        drive(&io, || cp.step_due(db.transactions())).unwrap();
        steps += 1;
    }
    let passed = STEPS_PASSED_OVER.with(|n| n.get()) - passed;
    let left = db.store().erase_owed();
    drop((tree, db));
    assert!(joined, "the reader panicked");
    assert!(
        owed > 0 && !due,
        "slots owed erasure, nothing due: {owed} owed"
    );
    assert!(
        held,
        "the erasure the reader held back did not lower its level"
    );
    assert_eq!(
        (passed_held, stepped_held),
        (16, 0),
        "statements while the reader stood: passed over, and checkpoint steps taken"
    );
    assert_eq!(
        (left, passed),
        (0, 0),
        "{owed} slots owed, after {steps} statements' steps"
    );
}

/// On a checkpoint's own thread, slots a checkpoint freed while a step held them back are erased
/// once that step ends, with no statement after it, and statements meanwhile wake no one for them:
/// the erasure, held, lowered its level and waits for the step's end, whose notify wakes the task
/// (the auditor's P2-CP-E1; mutants: a held erasure registering no wait, a statement waking the
/// task for owed erasure).
#[test]
fn erasure_a_step_holds_back_runs_at_its_end_and_statements_meanwhile_wake_no_one() {
    use crate::storage::atoll::checkpoint::{Checkpointing, DRIVER_WAKES};
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_in(&io, 4, 512 << 10, Checkpointing::Thread);
    let tree = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root());
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (leave_tx, leave_rx) = std::sync::mpsc::channel::<()>();
    let reader = {
        let epochs = db.store().epochs().clone();
        std::thread::spawn(move || {
            let step = epochs.enter();
            entered_tx.send(()).unwrap();
            let _ = leave_rx.recv();
            drop(step);
        })
    };
    for round in 0..3u8 {
        for k in 0..64u64 {
            let mut put = Put::new(&k.to_be_bytes(), vec![round; 512], Mode::Upsert);
            drive(&io, || tree.put(&mut put)).unwrap();
        }
        if round == 2 {
            entered_rx.recv().unwrap();
        }
        checkpoint(&io, &db);
    }
    let cp = db.checkpointer();
    // The task's erasure finds the slots held back and lowers the level.
    let mut held = false;
    for _ in 0..10_000 {
        held = cp.erasure_held() && !cp.is_under_way();
        if held {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let owed = db.store().erase_owed();
    let wakes = DRIVER_WAKES.with(|n| n.get());
    for _ in 0..16 {
        drive(&io, || cp.step_due(db.transactions())).unwrap();
    }
    let woken = DRIVER_WAKES.with(|n| n.get()) - wakes;
    let still_owed = db.store().erase_owed();
    // The reader's step ends, and no statement follows.
    let _ = leave_tx.send(());
    let joined = reader.join().is_ok();
    let mut left = db.store().erase_owed();
    for _ in 0..10_000 {
        if left == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        left = db.store().erase_owed();
    }
    drop((tree, db));
    assert!(joined, "the reader panicked");
    assert!(held, "the task's erasure held back did not lower its level");
    assert!(owed > 0, "no slot owed erasure while the reader stood");
    assert_eq!(woken, 0, "wakes of the task from statements meanwhile");
    assert_eq!(still_owed, owed, "slots erased while the reader stood");
    assert_eq!(left, 0, "slots left unerased once the reader's step ended");
}

/// A checkpoint releases the log's segments before its redo point one a unit, then syncs their
/// directory in a unit of its own (the auditor's P2-CP-U1; mutant: the whole prefix released in
/// one unit).
#[test]
fn a_checkpoint_releases_the_logs_segments_one_a_unit() {
    use crate::storage::atoll::log::SEGMENTS_RELEASED;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 4);
    let tree = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root());
    let value = vec![9u8; 32 << 10];
    // Four segments of log and more, none of it checkpointed: the commits only append.
    while db.log().end() < 4 * log_config().segment_bytes {
        let mut txn = begin(&io, &db, Isolation::Snapshot);
        let key = txn.start().to_be_bytes();
        let mut put = Put::new(&key, value.clone(), Mode::Upsert);
        drive(&io, || tree.put(&mut put)).unwrap();
        txn.changed(|s, c| c.plain_put(s, root(), &key, &value));
        commit(&io, &db, txn);
    }
    let cp = db.checkpointer();
    let ticket = cp.request_full();
    let (mut most, mut total) = (0, 0);
    while cp.fulls_done() < ticket.ticket() {
        let before = SEGMENTS_RELEASED.with(|r| r.get());
        if let Some(c) = cp.one_unit(db.transactions()).unwrap() {
            io.wait(&c);
        }
        let released = SEGMENTS_RELEASED.with(|r| r.get()) - before;
        most = most.max(released);
        total += released;
    }
    drop((tree, db));
    assert!(total >= 2, "{total} segments released");
    assert_eq!(most, 1, "the most segments one unit released, of {total}");
}

/// A quiet database is not due again because of its checkpoints' own records: with a log budget
/// of a byte or of 64, its checkpoint's intent record past the threshold, a checkpoint ends and the
/// task rests, and the statements after it pass over the checkpointer; new input makes it due
/// (the auditor's P1-CP-D1; mutant: the checkpoint's own record counted as input).
#[test]
fn a_quiet_database_is_not_due_for_its_checkpoints_own_records() {
    use crate::storage::atoll::checkpoint::STEPS_PASSED_OVER;
    for budget in [1u64, 64] {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        // Its first full checkpoint is made at open.
        let db = open_with_budget(&io, 4, budget);
        let cp = db.checkpointer();
        let rested = (cp.is_due(), cp.task_class());
        let checkpoint = cp.superblock().checkpoint;
        let passed = STEPS_PASSED_OVER.with(|n| n.get());
        for _ in 0..3 {
            drive(&io, || cp.step_due(db.transactions())).unwrap();
        }
        let quiet = (
            STEPS_PASSED_OVER.with(|n| n.get()) - passed,
            cp.superblock().checkpoint - checkpoint,
        );
        let mut txn = begin(&io, &db, Isolation::Snapshot);
        write(&io, &db, &mut txn);
        commit(&io, &db, txn);
        let written = cp.is_due();
        drop(db);
        assert_eq!(
            rested,
            (false, Some(Class::Maintenance)),
            "budget {budget}: due after the open's checkpoint"
        );
        assert_eq!(
            quiet,
            (3, 0),
            "budget {budget}: passes over, checkpoints made"
        );
        assert!(written, "budget {budget}: a commit's record is input");
    }
}

/// Statements that find a checkpoint due trigger its task once for the crossing, the rest reading
/// the word that says so (the auditor's P2-EX-H1; mutant: a trigger, its four locks, a statement).
#[test]
fn statements_finding_a_checkpoint_due_trigger_its_task_once() {
    use crate::storage::atoll::checkpoint::TRIGGERS;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 4);
    let cp = db.checkpointer();
    append_until_due(&io, &db, 32 << 10);
    let triggers = TRIGGERS.with(|n| n.get());
    // The state held, as an integrity check holds it: no checkpoint begins, so the crossing stands.
    let held = cp.hold().expect("the state free");
    let stepped = (0..5).all(|_| cp.step_due(db.transactions()).is_ok());
    let triggered = TRIGGERS.with(|n| n.get()) - triggers;
    drop(held);
    drop(db);
    assert!(stepped, "a statement's step failed");
    assert_eq!(triggered, 1, "triggers over five statements");
}

/// A checkpoint once due stays due until the next begins: a checkpoint's own record appended
/// meanwhile is no input and takes nothing away, and the next checkpoint's begin, with nothing
/// appended since but its own record, leaves none due (the auditor's P1-CP-D1 follow-up: a due
/// that fell back within one start lost its one trigger).
#[test]
fn a_checkpoint_once_due_stays_due_until_the_next_begins() {
    use crate::storage::atoll::log::{Header, Kind};
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_with_budget(&io, 4, 64);
    let cp = db.checkpointer();
    let rested = cp.is_due();
    let mut txn = begin(&io, &db, Isolation::Snapshot);
    write(&io, &db, &mut txn);
    commit(&io, &db, txn);
    let written = cp.is_due();
    // A record of the checkpoint's own kind, appended as its intent is.
    let header = Header {
        kind: Kind::new(31).unwrap(),
        flags: 0,
        txn: 0,
        prev: None,
    };
    let mut admitted = drive(&io, || {
        db.log()
            .admit(&mut crate::storage::atoll::log::LogWait::default(), 256)
    })
    .unwrap();
    db.log().append_own(&mut admitted, &header, &[&[0u8; 64]]);
    drop(admitted);
    let own_appended = cp.is_due();
    checkpoint(&io, &db);
    let after = cp.is_due();
    drop(db);
    assert_eq!(
        (rested, written, own_appended, after),
        (false, true, true, false),
        "at rest, after a commit, after an own record, after the next checkpoint"
    );
}
