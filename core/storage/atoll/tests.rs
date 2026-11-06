//! A whole database under crashes (audit 18): transactions committing through the buffer pool while
//! it evicts pages to swap and checkpoints run, recovered on several threads; the gap a recovery
//! records past a torn log block; and a database dropped with its IO in flight.

use super::aligned::AlignedBlock;
use super::btree::{Mode, Tree};
use super::buffer::{BufferConfig, MIN_DB_BUDGET};
use super::control::{self, Control, MAX_CONTINUATIONS};
use super::db::{Atoll, AtollConfig, Opening};
use super::log::{Log, LogConfig, Lsn, BLOCK};
use super::page::{PageId, SizeClass, HEADER_SIZE};
use super::shared::BytesMut;
use super::store::StoreConfig;
use super::testing::drive;
use super::txn::versioned::{Get, VersionedTree, Versions, Write, WriteOp};
use super::txn::{Commit, Isolation, Txn};
use crate::io::crash::{CrashIO, Faults};
use crate::io::{Completion, OpenFlags, IO};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::Result;
use std::cell::Cell;
use std::collections::BTreeMap;

const BASE: &str = "/db/atoll";

fn config() -> AtollConfig {
    AtollConfig {
        log: LogConfig {
            segment_bytes: 512 << 10,
            buffer_bytes: 128 << 10,
            chunk_bytes: 16 << 10,
            writes_in_flight: 3,
            direct: true,
        },
        store: StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        buffer: BufferConfig {
            budget: MIN_DB_BUDGET,
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
            kept_bytes: None,
        },
        log_budget: Some(96 << 10),
        checkpoint_write_bytes: None,
        checkpointing: crate::storage::atoll::checkpoint::Checkpointing::Writers,
        txn: Default::default(),
    }
}

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

/// Opens the database on `threads` threads, the way an embedder lends them to recovery.
fn try_open(io: Arc<dyn IO>, threads: usize) -> Result<Atoll> {
    let opening = Opening::new(io, BASE, config())?;
    let worked: Vec<Result<()>> = std::thread::scope(|s| {
        let others: Vec<_> = (1..threads).map(|_| s.spawn(|| opening.work())).collect();
        let mut worked = vec![opening.work()];
        worked.extend(others.into_iter().map(|h| h.join().unwrap()));
        worked
    });
    worked.into_iter().collect::<Result<Vec<()>>>()?;
    opening.finish()
}

/// The gaps the superblock in charge records.
fn gaps(io: &Arc<dyn IO>) -> Vec<(Lsn, Lsn)> {
    let (_, sb) = Control::open(io.clone(), BASE).unwrap();
    sb.expect("a superblock in charge").continuations
}

/// A database and one versioned tree in it.
struct Db {
    io: Arc<dyn IO>,
    db: Atoll,
    versions: Versions,
    tree: VersionedTree,
}

impl Db {
    /// A new database with its tree, in a checkpoint before any write, as a database's creation
    /// has its system trees.
    fn create(io: Arc<dyn IO>) -> Self {
        let db = try_open(io.clone(), 1).unwrap();
        let root = drive(&io, || Tree::create_versioned(db.pool(), class(0))).unwrap();
        let made = Self::with(io, db, root);
        made.checkpoint();
        made
    }

    /// What a crash left, recovered on `threads` threads.
    fn recover(io: Arc<dyn IO>, root: PageId, threads: usize) -> Result<Self> {
        let db = try_open(io.clone(), threads)?;
        Ok(Self::with(io, db, root))
    }

    fn with(io: Arc<dyn IO>, db: Atoll, root: PageId) -> Self {
        let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), 4);
        let tree = VersionedTree::open(io.clone(), db.pool().clone(), db.store().clone(), root);
        Self {
            io,
            db,
            versions,
            tree,
        }
    }

    fn begin(&self) -> Txn {
        let txns = self.db.transactions();
        let mut txn = drive(&self.io, || txns.begin(Isolation::Snapshot)).unwrap();
        txns.statement(&mut txn).unwrap();
        txn
    }

    /// Commits `value` under `key` in a transaction of its own; durable once it returns.
    fn put(&self, key: &str, value: &[u8]) {
        let txns = self.db.transactions();
        let mut txn = self.begin();
        let mut op = WriteOp::put(key.as_bytes(), value.to_vec(), Mode::Upsert);
        let write = drive(&self.io, || {
            self.tree.write(txns, &self.versions, &mut txn, &mut op)
        })
        .unwrap();
        assert!(matches!(write, Write::Done(_)), "{key}: {write:?}");
        let mut commit = Commit::new(txn);
        drive(&self.io, || txns.commit(&mut commit)).unwrap();
    }

    /// What a transaction begun now reads of `keys`.
    fn state(&self, keys: &[String]) -> BTreeMap<String, Option<Vec<u8>>> {
        let txns = self.db.transactions();
        let mut txn = self.begin();
        let out = keys
            .iter()
            .map(|key| {
                let mut op = Get::new(key.as_bytes());
                let v = drive(&self.io, || {
                    self.tree.get(txns, &self.versions, &mut txn, &mut op)
                })
                .unwrap();
                (key.clone(), v)
            })
            .collect();
        let mut commit = Commit::new(txn);
        drive(&self.io, || txns.commit(&mut commit)).unwrap();
        out
    }

    fn checkpoint(&self) {
        let checkpointer = self.db.checkpointer();
        let ticket = checkpointer.request_full();
        drive(&self.io, || {
            checkpointer.step_full(ticket, self.db.transactions())
        })
        .unwrap();
    }

    /// Steps the checkpointer as a writing statement does.
    fn step_checkpoints(&self) {
        drive(&self.io, || {
            self.db.checkpointer().step_due(self.db.transactions())
        })
        .unwrap();
    }
}

/// The value of commit `i` to its key: long enough, every few, to go out of line.
fn value(i: usize) -> Vec<u8> {
    let len = if i % 5 == 0 { 6000 } else { 120 };
    let mut v = format!("v{i}-").into_bytes();
    v.resize(len, b'.');
    v
}

/// Commits keys in turn, many times each, through a pool small enough to evict to swap and a log
/// budget small enough to checkpoint again and again; crash images are taken every few IO
/// operations and after each round of commits. An image taken after a round recovers exactly what
/// was committed by then; an image taken mid-round recovers, for every key, its value as of some
/// commit at or after the last before the round, every acknowledged commit before the round
/// included. Recovery runs on two threads.
fn crashes_recover_every_commit_acknowledged(durable_writes: bool) {
    let crash_io = Arc::new(CrashIO::new(1));
    crash_io.set_durable_writes(durable_writes);
    let io: Arc<dyn IO> = crash_io.clone();
    let db = Db::create(io);
    let root = db.tree.root();
    let keys: Vec<String> = (0..24).map(|k| format!("k{k:02}")).collect();
    // Per key, every value committed to it, in order.
    let mut written: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
    let mut rounds = Vec::new();
    crash_io.capture(|_| true, 37, 1);
    let mut i = 0;
    for round in 0..8 {
        for key in &keys {
            let v = value(i);
            db.put(key, &v);
            written.entry(key.clone()).or_default().push(v);
            db.step_checkpoints();
            i += 1;
        }
        rounds.push(crash_io.crash(round));
    }
    let after = db.state(&keys);
    drop(db);
    let mid = crash_io.captured();
    assert!(!mid.is_empty(), "no image mid-round");
    for (n, image) in rounds.into_iter().enumerate() {
        let db = Db::recover(Arc::new(image), root, 2).unwrap();
        let state = db.state(&keys);
        // Round n's image holds every commit of rounds 0 to n: each key's (n + 1)th value.
        for (key, vs) in &written {
            assert_eq!(state[key], Some(vs[n].clone()), "round {n}: {key}");
        }
    }
    let mut checked = 0;
    for (j, image) in mid.into_iter().enumerate() {
        let db = Db::recover(Arc::new(image), root, 2).unwrap();
        let state = db.state(&keys);
        for (key, vs) in &written {
            // Some commit's value, or none before any.
            assert!(
                state[key].as_ref().is_none_or(|v| vs.contains(v)),
                "image {j}: {key} reads a value never committed"
            );
        }
        checked += 1;
    }
    assert!(checked > 0);
    assert_eq!(after.len(), keys.len());
}

#[test]
fn crashes_recover_every_commit_acknowledged_with_durable_writes() {
    crashes_recover_every_commit_acknowledged(true);
}

#[test]
fn crashes_recover_every_commit_acknowledged_with_writes_then_syncs() {
    crashes_recover_every_commit_acknowledged(false);
}

#[test]
fn a_crash_once_recovery_records_its_gap_never_reads_what_the_first_crash_left_past_it() {
    // Commits each flushed on their own, so each starts a block, nothing else appended between;
    // every other one follows an empty run published with no wait, so its Commit starts inside a
    // block and ends inside it.
    let crash_io = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash_io.clone();
    let db = Db::create(io);
    let root = db.tree.root();
    let written = |i: usize| value(i * 5 + 1);
    let mut ends = Vec::new();
    for i in 0..32 {
        if i % 2 == 1 {
            let run = super::txn::changes::Changes::new();
            drive(&db.io, || db.db.transactions().publish_alone(&run)).unwrap();
        }
        db.put(&format!("k{i:02}"), &written(i));
        ends.push((format!("k{i:02}"), written(i), db.db.log().end()));
    }
    let redo = db.db.checkpointer().superblock().redo;
    let whole = crash_io.crash(0);
    drop(db);
    let log = config().log;
    let read_to = |io: Arc<dyn IO>, from: Lsn| {
        let mut reader = Log::reader(io, BASE, &log, from);
        let mut starts = Vec::new();
        while let Some(r) = reader.next_record().unwrap() {
            starts.push(r.start);
        }
        (starts, reader.position())
    };
    // A crash that tore a block and kept the next: the bytes from a record inside the block to
    // its end are lost, and the log ends inside the block, with whole records from before the
    // crash at the next block boundary, where recovery reopens it and records a gap.
    let (starts, _) = read_to(Arc::new(whole.crash(0)), redo);
    let (torn, end, restart) = starts
        .iter()
        .filter(|&&b| b % BLOCK == 0 && b > redo + BLOCK)
        .find_map(|&restart| {
            let from = *starts.iter().rfind(|&&s| s < restart)?;
            if from % BLOCK == 0 {
                return None;
            }
            let torn = whole.crash(0);
            let file = torn
                .open_file(
                    &format!("{BASE}.log.{:016x}", from / log.segment_bytes),
                    OpenFlags::None,
                    false,
                )
                .unwrap();
            let len = (restart - from) as usize;
            let zeros = Arc::new(AlignedBlock::zeroed(len.next_multiple_of(BLOCK as usize)));
            let at = from % log.segment_bytes;
            let c = file
                .pwrite(at, zeros.region(0, len), Completion::new_write(|_| {}))
                .unwrap();
            torn.wait_for_completion(c).unwrap();
            let c = file.sync_data(Completion::new_sync(|_| {})).unwrap();
            torn.wait_for_completion(c).unwrap();
            let torn = Arc::new(torn);
            let (_, end) = read_to(torn.clone(), redo);
            (end % BLOCK != 0 && Log::resumes_at(end) == restart).then_some((torn, end, restart))
        })
        .expect("a block a crash can tear with whole records after it");
    let (past, _) = read_to(torn.clone(), restart);
    assert!(
        !past.is_empty(),
        "no record from before the crash past {restart}"
    );
    let keys: Vec<String> = ends.iter().map(|(k, _, _)| k.clone()).collect();
    // A commit is recovered exactly when its record lies wholly before the log's end.
    let expected: BTreeMap<String, Option<Vec<u8>>> = ends
        .iter()
        .map(|(k, v, e)| (k.clone(), (*e <= end).then(|| v.clone())))
        .collect();
    // A superblock that holds every gap it has room for, behind the redo point where no reader
    // goes past them, leaves no room for this one: recovery refuses before it applies, reopens or
    // writes anything, and recovers once there is room.
    let full: Arc<dyn IO> = Arc::new(torn.crash(0));
    let held = control::rewrite_in_charge(full.clone(), BASE, |sb| {
        sb.continuations = vec![(0, 0); MAX_CONTINUATIONS];
    });
    let refused = try_open(full.clone(), 1)
        .err()
        .expect("a recovery with no room for its gap");
    assert!(refused.to_string().contains("gaps it holds"), "{refused}");
    let (_, now) = Control::open(full.clone(), BASE).unwrap();
    assert_eq!(now, Some(held), "a refused recovery wrote a superblock");
    control::rewrite_in_charge(full.clone(), BASE, |sb| sb.continuations.clear());
    let db = Db::recover(full, root, 1).unwrap();
    assert_eq!(db.state(&keys), expected, "recovered once there was room");
    drop(db);
    // Crashed after each operation of the recovery that records the gap, each image recovers the
    // log up to its end and nothing the crash left past the gap.
    torn.capture(|_| false, 1, 0);
    let db = Db::recover(torn.clone(), root, 1).unwrap();
    let images = torn.captured();
    assert_eq!(db.state(&keys), expected, "recovered");
    drop(db);
    let count = images.len();
    let mut gapped = 0;
    for (i, image) in images.into_iter().enumerate() {
        let image: Arc<dyn IO> = Arc::new(image);
        gapped += usize::from(!gaps(&image).is_empty());
        let label = format!(
            "image {i} of {count}, the gap from {end} to {restart} before {} records from \
             before the crash",
            past.len()
        );
        let db =
            Db::recover(image, root, 1).unwrap_or_else(|e| panic!("{label}: recovery failed: {e}"));
        assert_eq!(db.state(&keys), expected, "{label}");
    }
    assert!(gapped > 0, "none of {count} images holds the gap");
}

/// A checkpoint whose image write fails leaves the checkpoint in charge in charge, and none runs
/// again: what it marked imaged and named as it stands is not undone, so only a recovery from the
/// one in charge is sure to hold nothing of it. Commits go on, durable in the log, and the database
/// opened again holds every one and checkpoints once more.
#[test]
fn a_checkpoint_that_fails_leaves_the_one_in_charge_and_none_runs_until_the_database_is_opened_again(
) {
    let crash_io = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash_io.clone();
    let db = Db::create(io.clone());
    let root = db.tree.root();
    let keys: Vec<String> = (0..40).map(|i| format!("k{i:02}")).collect();
    for (i, key) in keys.iter().enumerate().take(20) {
        db.put(key, &value(i));
    }
    db.checkpoint();
    let in_charge = db.db.checkpointer().superblock();
    for (i, key) in keys.iter().enumerate().skip(20) {
        db.put(key, &value(i));
    }
    // The writes of the tree's pages fail; the log's never do.
    crash_io.set_faults_on(Some(&format!("{BASE}.c0.0")));
    crash_io.set_faults(Faults {
        write: 1.0,
        ..Faults::default()
    });
    let checkpointer = db.db.checkpointer();
    // Stepped here, not through `drive`, which returns a failed write's error from its completion
    // before the checkpointer has seen it: the checkpointer meets the failure itself.
    let step = |ticket: u64| loop {
        match checkpointer.step_full(ticket, db.db.transactions()) {
            Err(e) => return Err(e),
            Ok(IOResult::Done(())) => return Ok(()),
            Ok(IOResult::IO(IOCompletions(c))) => {
                while !c.finished() {
                    io.step().unwrap();
                }
            }
        }
    };
    let ticket = checkpointer.request_full();
    let failed = step(ticket)
        .err()
        .expect("a checkpoint went on past a failed write");
    crash_io.set_faults(Faults::default());
    crash_io.set_faults_on(None);
    assert!(
        matches!(failed, crate::LimboError::CompletionError(_)),
        "{failed}"
    );
    assert_eq!(
        checkpointer.superblock(),
        in_charge,
        "a failed checkpoint took charge"
    );
    // What it held is let go: no cut is in effect, and the change gate is open.
    assert_eq!(db.db.pool().cutting(), 0, "the pool's cut");
    assert_eq!(db.db.store().meta().cutting(), 0, "the metadata cut");
    assert!(db.db.store().change().is_some(), "the change gate");
    let refused = step(checkpointer.request_full())
        .err()
        .expect("a checkpoint after one failed");
    assert!(refused.to_string().contains("opened again"), "{refused}");
    db.put("after", b"the failure");
    let expected = db.state(&keys);
    drop(db);
    let reopened = Db::recover(io, root, 1).unwrap();
    assert_eq!(reopened.state(&keys), expected, "reopened");
    assert_eq!(
        reopened.state(&["after".to_string()])["after"].as_deref(),
        Some(&b"the failure"[..])
    );
    reopened.checkpoint();
}

/// [`drive`] while `left` holds IO steps, each step spending one, shared across calls: `None` once
/// they run out, with what the operation submitted still in flight. A yield spends one too, so
/// they always run out.
fn drive_for<T>(
    io: &Arc<dyn IO>,
    left: &Cell<u64>,
    mut op: impl FnMut() -> Result<IOResult<T>>,
) -> Option<T> {
    loop {
        match op().unwrap() {
            IOResult::Done(v) => return Some(v),
            IOResult::IO(IOCompletions(c)) => loop {
                if left.get() == 0 {
                    return None;
                }
                left.set(left.get() - 1);
                io.step().unwrap();
                if c.finished() {
                    break;
                }
            },
        }
    }
}

/// Steps the IO while `left` holds IO steps until nothing is in flight: `None` if they ran out.
fn settle(crash: &CrashIO, io: &Arc<dyn IO>, left: &Cell<u64>) -> Option<()> {
    while crash.in_flight() > 0 {
        if left.get() == 0 {
            return None;
        }
        left.set(left.get() - 1);
        io.step().unwrap();
    }
    Some(())
}

/// Every kind of IO a database submits, for as long as `left` holds IO steps: commits, a full
/// checkpoint (its images, metadata and superblock), the next log segment zeroed and synced,
/// temporary pages enough to write some out, the tree read again, then the database dropped,
/// reopened, recovered and read again from disk. `None` where the steps ran out.
fn every_kind_of_io(crash: &CrashIO, io: &Arc<dyn IO>, left: &Cell<u64>) -> Option<()> {
    let root = {
        let db = try_open(io.clone(), 1).unwrap();
        let root = drive_for(io, left, || Tree::create_versioned(db.pool(), class(0)))?;
        let db = Db::with(io.clone(), db, root);
        let txns = db.db.transactions();
        for v in 1..=8 {
            let mut txn = drive_for(io, left, || txns.begin(Isolation::Snapshot))?;
            txns.statement(&mut txn).unwrap();
            let mut op = WriteOp::put(&[v as u8], vec![v as u8; 3000], Mode::Upsert);
            drive_for(io, left, || {
                db.tree.write(txns, &db.versions, &mut txn, &mut op)
            })?;
            let mut commit = Commit::new(txn);
            drive_for(io, left, || txns.commit(&mut commit))?;
        }
        let ticket = db.db.checkpointer().request_full();
        drive_for(io, left, || db.db.checkpointer().step_full(ticket, txns))?;
        // The next segment's zeros go one write at a time, then its sync: all of it lands.
        settle(crash, io, left)?;
        // Twice the pool of them: a pool's worth at least is written out, and the tree's pages
        // leave memory.
        let temp = class(5);
        for v in 0..2 * MIN_DB_BUDGET / temp.size() {
            let mut page = drive_for(io, left, || db.db.pool().allocate_temporary(temp))?;
            page.page_mut().put_u64(HEADER_SIZE, v as u64);
            page.changed();
        }
        let mut txn = drive_for(io, left, || txns.begin(Isolation::Snapshot))?;
        txns.statement(&mut txn).unwrap();
        for v in 1..=8u8 {
            let mut op = Get::new(&[v]);
            drive_for(io, left, || {
                db.tree.get(txns, &db.versions, &mut txn, &mut op)
            })?;
        }
        txns.abort(txn);
        root
    };
    // What the first one left in flight lands before the reopen, as it would after a crash.
    settle(crash, io, left)?;
    let db = Db::recover(io.clone(), root, 1).unwrap();
    let txns = db.db.transactions();
    let mut txn = drive_for(io, left, || txns.begin(Isolation::Snapshot))?;
    txns.statement(&mut txn).unwrap();
    for v in 1..=8u8 {
        let mut op = Get::new(&[v]);
        drive_for(io, left, || {
            db.tree.get(txns, &db.versions, &mut txn, &mut op)
        })?;
    }
    txns.abort(txn);
    Some(())
}

/// A database dropped with its IO in flight leaves nothing holding the IO: every completion it
/// hands the IO waits in it, so one that held its owner, or a file it syncs, strongly would keep
/// the IO and all it holds alive for good once nothing steps it. Dropped after every number of IO
/// steps through every kind of IO it submits, with and without durable writes.
#[test]
fn a_database_dropped_with_io_in_flight_leaves_nothing_holding_its_io() {
    for durable_writes in [true, false] {
        let mut dropped_in_flight = 0;
        for steps in 0.. {
            let crash = Arc::new(CrashIO::new(0));
            crash.set_durable_writes(durable_writes);
            let io: Arc<dyn IO> = crash.clone();
            let finished = every_kind_of_io(&crash, &io, &Cell::new(steps)).is_some();
            dropped_in_flight += usize::from(crash.in_flight() > 0);
            drop(io);
            assert_eq!(
                (Arc::strong_count(&crash), crash.other_handles()),
                (1, 0),
                "dropped after {steps} IO steps with {} in flight (durable writes: \
                 {durable_writes}): something it submitted holds the IO",
                crash.in_flight()
            );
            if finished {
                break;
            }
        }
        assert!(
            dropped_in_flight > 0,
            "no drop with IO in flight (durable writes: {durable_writes})"
        );
    }
}
