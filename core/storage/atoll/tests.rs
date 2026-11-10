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

crate::thread::thread_local! {
    /// Whether the steps [`drive_for`] or [`settle`] were given ran out with a request still owned
    /// by the backend: what dropping the database then leaves for its close to retire.
    static OWNED_AT_RUN_OUT: Cell<bool> = const { Cell::new(false) };
}

/// [`drive`] while `left` holds IO steps, each step spending one, shared across calls: `None` once
/// they run out, with what the operation submitted still in flight. A yield spends one too, so
/// they always run out.
fn drive_for<T>(
    crash: &CrashIO,
    io: &Arc<dyn IO>,
    left: &Cell<u64>,
    mut op: impl FnMut() -> Result<IOResult<T>>,
) -> Option<T> {
    loop {
        match op().unwrap() {
            IOResult::Done(v) => return Some(v),
            IOResult::IO(IOCompletions(c)) => loop {
                if left.get() == 0 {
                    // What the operation waits on may be no request itself (a checkpoint waits
                    // for its writes' retirement): the backend's own count says what is in
                    // flight.
                    OWNED_AT_RUN_OUT.with(|o| o.set(crash.in_flight() > 0));
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
            OWNED_AT_RUN_OUT.with(|o| o.set(true));
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
        let root = drive_for(crash, io, left, || {
            Tree::create_versioned(db.pool(), class(0))
        })?;
        let db = Db::with(io.clone(), db, root);
        let txns = db.db.transactions();
        for v in 1..=8 {
            let mut txn = drive_for(crash, io, left, || txns.begin(Isolation::Snapshot))?;
            txns.statement(&mut txn).unwrap();
            let mut op = WriteOp::put(&[v as u8], vec![v as u8; 3000], Mode::Upsert);
            drive_for(crash, io, left, || {
                db.tree.write(txns, &db.versions, &mut txn, &mut op)
            })?;
            let mut commit = Commit::new(txn);
            drive_for(crash, io, left, || txns.commit(&mut commit))?;
        }
        let ticket = db.db.checkpointer().request_full();
        drive_for(crash, io, left, || {
            db.db.checkpointer().step_full(ticket, txns)
        })?;
        // The next segment's zeros go one write at a time, then its sync: all of it lands.
        settle(crash, io, left)?;
        // Twice the pool of them: a pool's worth at least is written out, and the tree's pages
        // leave memory.
        let temp = class(5);
        for v in 0..2 * MIN_DB_BUDGET / temp.size() {
            let mut page = drive_for(crash, io, left, || db.db.pool().allocate_temporary(temp))?;
            page.page_mut().put_u64(HEADER_SIZE, v as u64);
            page.changed();
        }
        let mut txn = drive_for(crash, io, left, || txns.begin(Isolation::Snapshot))?;
        txns.statement(&mut txn).unwrap();
        for v in 1..=8u8 {
            let mut op = Get::new(&[v]);
            drive_for(crash, io, left, || {
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
    let mut txn = drive_for(crash, io, left, || txns.begin(Isolation::Snapshot))?;
    txns.statement(&mut txn).unwrap();
    for v in 1..=8u8 {
        let mut op = Get::new(&[v]);
        drive_for(crash, io, left, || {
            db.tree.get(txns, &db.versions, &mut txn, &mut op)
        })?;
    }
    txns.abort(txn);
    Some(())
}

/// A database dropped with its IO in flight retires every request on its files before the drop
/// returns (DESIGN.md 22.12, step 4), and leaves nothing holding the IO: a completion that held
/// its owner, or a file it syncs, strongly would keep the IO and all it holds alive for good.
/// Dropped after every number of IO steps through every kind of IO it submits, with and without
/// durable writes.
#[test]
fn a_database_dropped_with_io_in_flight_leaves_nothing_holding_its_io() {
    for durable_writes in [true, false] {
        let mut dropped_in_flight = 0;
        for steps in 0.. {
            let crash = Arc::new(CrashIO::new(0));
            crash.set_durable_writes(durable_writes);
            let io: Arc<dyn IO> = crash.clone();
            let finished = every_kind_of_io(&crash, &io, &Cell::new(steps)).is_some();
            dropped_in_flight += usize::from(OWNED_AT_RUN_OUT.with(|o| o.replace(false)));
            assert_eq!(
                crash.in_flight(),
                0,
                "dropped after {steps} IO steps (durable writes: {durable_writes}): a request \
                 outlived the database's close"
            );
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

/// A full checkpoint of the database, its units counted from here: what each visited at most, and
/// how many ran, with the pages it imaged.
fn checkpoint_counted(db: &Db) -> (u32, u64, u64) {
    let pages = db.db.checkpointer().stats().pages;
    let _ = super::visits::most_and_units();
    db.checkpoint();
    let (most, units) = super::visits::most_and_units();
    (most, units, db.db.checkpointer().stats().pages - pages)
}

/// Proof 3 (DESIGN.md 22.15): a checkpoint's units each visit at most one page, however much it
/// images: 4,000 rows, values with a directory page each, and every row rewritten by a
/// transaction still open at the cut, so each leaf's image as of the cut follows its versions
/// into the arenas. Debug builds assert the bound in each unit; the units' count is weighed
/// against the pages imaged (mutants: the phase loop that images every page in one call; an
/// image built whole, every arena page in one call).
#[test]
fn a_checkpoints_units_each_visit_at_most_one_page() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let db = Db::create(io.clone());
    for i in 0..4_000 {
        db.put(&format!("k{i:05}"), &value(i));
    }
    // Larger than a page of the largest class: each needs a directory page.
    for i in 0..4 {
        db.put(
            &format!("big{i}"),
            &vec![i as u8; 2 * crate::storage::atoll::page::MAX_PAGE_SIZE],
        );
    }
    let txns = db.db.transactions();
    let mut open = db.begin();
    for i in 0..4_000 {
        let mut op = WriteOp::put(format!("k{i:05}").as_bytes(), value(i + 1), Mode::Upsert);
        let write = drive(&io, || {
            db.tree.write(txns, &db.versions, &mut open, &mut op)
        })
        .unwrap();
        assert!(matches!(write, Write::Done(_)), "{write:?}");
    }
    let (most, units, pages) = checkpoint_counted(&db);
    assert_eq!(most, 1, "the units visited {most} pages at most");
    // More pages than a flight set holds, so the phase's units interleave writes and landings.
    assert!(
        pages > super::checkpoint::WRITES as u64 && units >= 2 * pages,
        "{units} units imaged {pages} pages: a page's source and its write are a unit each"
    );
    let mut commit = Commit::new(open);
    drive(&io, || txns.commit(&mut commit)).unwrap();
}

/// The checkpoint keeps its flight set full: each unit starts one write, and it waits only once
/// [`super::checkpoint::WRITES`] are in flight (DESIGN.md 22.1; mutant: a wait after every
/// submission, one write in flight). The writes stay in flight until the IO is stepped.
#[test]
fn a_checkpoint_keeps_its_flight_set_full() {
    let crash = Arc::new(CrashIO::new(3));
    let io: Arc<dyn IO> = crash.clone();
    let db = Db::create(io.clone());
    for i in 0..2_000 {
        db.put(&format!("k{i:05}"), &value(i * 5 + 1));
    }
    db.checkpoint();
    let peak = db.db.checkpointer().stats().flying_writes_peak;
    assert_eq!(peak, flight_set(), "image writes in flight at most");
}

/// The image writes of the tree's pages a checkpoint keeps in flight: [`super::checkpoint::WRITES`]
/// of them, within its write bytes.
fn flight_set() -> usize {
    super::checkpoint::WRITES.min(super::checkpoint::WRITE_BYTES / class(0).size())
}

/// A cancelled image write keeps its place in the flight set until its request retires, and the
/// unit waiting for a place waits on that retirement (DESIGN.md 22.13; mutants: the place freed
/// when the write finishes, letting twice the limit be owned; the wait on the write's own
/// completion, finished already, which a waiter would spin on).
#[test]
fn a_cancelled_image_write_holds_its_place_until_it_retires() {
    let crash = Arc::new(CrashIO::new(5));
    let io: Arc<dyn IO> = crash.clone();
    let db = Db::create(io.clone());
    for i in 0..2_000 {
        db.put(&format!("k{i:05}"), &value(i * 5 + 1));
    }
    let cp = db.db.checkpointer();
    let txns = db.db.transactions();
    cp.request_full();
    let full = flight_set();
    // Units until the flight set is full, the IO stepped only for what is no image write.
    loop {
        match cp.one_unit(txns).unwrap() {
            Some(_) if cp.image_writes_in_flight() == full => break,
            Some(c) => io.wait(&c),
            None => {}
        }
    }
    assert_eq!(cp.cancel_image_writes(), full);
    let waited = cp.one_unit(txns).unwrap();
    assert_eq!(
        cp.image_writes_in_flight(),
        full,
        "a cancelled write let its place go before it retired"
    );
    let waited = waited.expect("the unit waits for a place");
    assert!(
        !waited.finished(),
        "the unit waits on a completion finished already"
    );
    while crash.in_flight() > 0 {
        io.step().unwrap();
    }
    assert!(waited.finished(), "the retirement did not end the wait");
}

/// A unit of the checkpoint never waits: one that would ends the process, its wait refused
/// inside the unit (DESIGN.md 22.1, 22.15 proof 3; mutant: no unit frame, so the wait goes on and
/// the child ends normally).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_checkpoint_unit_that_waits_ends_the_process() {
    let Some(ended) = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_checkpoint_unit_that_waits_ends_the_process"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
            let db = Db::create(io.clone());
            db.put("k", b"v");
            let finished = Completion::new_yield();
            let waiter = io.clone();
            super::checkpoint::IN_UNIT.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || waiter.wait(&finished)));
            });
            db.checkpoint();
            eprintln!("the unit's wait went on");
        },
    ) else {
        return;
    };
    assert!(
        ended.aborted,
        "the unit's wait did not end the process: {:?} {}",
        ended.code, ended.stderr
    );
    assert!(
        ended
            .stderr
            .contains("a wait for a completion inside a unit of a job's work"),
        "{}",
        ended.stderr
    );
}

/// A flush asked for during the checkpoint's own turn of it, as another thread's commit would, is
/// answered by the checkpoint's next unit: no request made during a turn is left for nobody
/// (DESIGN.md 22.1, `Solo`). Asked during each turn toward the intent record's durability, so
/// during the last, after which the checkpoint takes no turn of its own: the log's end is durable
/// by then (mutant: the owed turn dropped, the request still asked for once the checkpoint is done).
#[test]
fn a_flush_asked_during_the_checkpoints_turn_is_answered() {
    use super::log::{ASK_DURING_TURN, TURNS};
    let crash = Arc::new(CrashIO::new(7));
    let io: Arc<dyn IO> = crash.clone();
    let db = Db::create(io.clone());
    for i in 0..50 {
        db.put(&format!("k{i:05}"), &value(i * 5 + 1));
    }
    let cp = db.db.checkpointer();
    let txns = db.db.transactions();
    let ticket = cp.request_full();
    let (mut asked, mut turns_after) = (0, 0);
    while cp.fulls_done() < ticket {
        // The next turn the checkpoint takes toward its intent's durability is asked for again
        // while it runs.
        let armed = cp.next_turn_for_intent();
        if armed {
            ASK_DURING_TURN.with(|a| a.set(true));
        }
        let turns = TURNS.with(|n| n.get());
        match cp.one_unit(txns).unwrap() {
            Some(c) => io.wait(&c),
            // The commit that asked waits for its own record, stepping this backend as it does.
            None if armed => io.step().unwrap(),
            None => {}
        }
        if armed && !ASK_DURING_TURN.with(|a| a.replace(false)) {
            asked += 1;
            turns_after = 0;
        } else {
            turns_after += TURNS.with(|n| n.get()) - turns;
        }
    }
    assert!(
        asked > 0,
        "no turn toward the intent's durability was asked for again"
    );
    assert!(
        turns_after > 0,
        "the checkpoint took no turn after the request made during its last"
    );
    assert!(
        !db.db.log().flush_asked(),
        "a flush asked for during the checkpoint's turn was left unanswered"
    );
}

/// A checkpoint unit whose intent record's admission must start the log's next segment runs that
/// preparation inside the unit: the interim adapter's one step any caller runs, the stated
/// exception to a unit's one page until the segment preparer is a task (DESIGN.md 22.16 steps 4
/// and 8), its budget one segment, on this inline backend its whole zero-fill. Debug builds assert
/// the budget in each unit (mutant: preparation that goes on past one segment in one call).
#[test]
fn a_checkpoint_unit_whose_admission_needs_a_segment_prepares_one() {
    use super::log::{framed_len, Header, Kind, ALIGN};
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let db = Db::create(io.clone());
    for i in 0..200 {
        db.put(&format!("k{i:05}"), &value(i * 5 + 1));
    }
    let cp = db.db.checkpointer();
    let txns = db.db.transactions();
    let log = db.db.log();
    // Admitted to a record's alignment short of being within a segment of the prepared end, in
    // records no larger than half the log's buffer: the checkpoint's one admission, its intent
    // record's, larger than that alignment, comes within a segment, and prepares.
    let header = Header {
        kind: Kind::new(1).unwrap(),
        flags: 0,
        txn: 0,
        prev: None,
    };
    let largest = framed_len(config().log.buffer_bytes as usize / 2);
    let overhead = framed_len(0) as usize;
    loop {
        let (ahead, segment) = log.prepared_ahead();
        let room = ahead.saturating_sub(segment);
        if room < framed_len(0) + ALIGN {
            break;
        }
        // A record of exactly this framed length: room aligned, the payload filling the frame.
        let framed = (room - ALIGN).min(largest);
        let filler = vec![0u8; framed as usize - overhead];
        assert_eq!(framed_len(filler.len()), framed);
        let mut admitted = drive(&io, || log.admit(framed)).unwrap();
        log.append(&mut admitted, &header, &[&filler]);
        assert!(
            log.prepared_ahead().0 < ahead,
            "the filler's admission started preparing a segment"
        );
    }
    let (ahead, segment) = log.prepared_ahead();
    assert!(ahead > segment, "the filler itself came within a segment");
    let _ = super::visits::most_segments();
    let ticket = cp.request_full();
    while cp.fulls_done() < ticket {
        if let Some(c) = cp.one_unit(txns).unwrap() {
            io.wait(&c);
        }
    }
    assert_eq!(
        super::visits::most_segments(),
        1,
        "the segment the intent record's admission needed, started inside its unit"
    );
}

/// A checkpoint whose metadata writes fail ends failed at the first failed one it finds, and never
/// waits on a write that has ended: with more dirty metadata pages than its flight set holds, the
/// write it waits on for a place is one that failed (DESIGN.md 22.9; mutant: a failed write kept in
/// the flight set, its unit waiting on it, ended already, for ever). Bounded by the units the same
/// checkpoint took when its writes landed: a failure only ends it sooner.
#[test]
fn a_checkpoint_whose_metadata_writes_fail_ends_failed() {
    use super::checkpoint::WRITES;
    use super::meta::{MetaId, Tree as MetaTree};
    let crash = Arc::new(CrashIO::new(11));
    let io: Arc<dyn IO> = crash.clone();
    let db = Db::create(io.clone());
    for i in 0..50 {
        db.put(&format!("k{i:05}"), &value(i * 5 + 1));
    }
    db.checkpoint();
    // Twice the pages the flight set holds, in the map tree's first file, each made dirty by a
    // change that leaves its bytes as they are: the next checkpoint writes every one.
    let meta = db.db.store().meta().clone();
    let dirty_pages = || {
        for index in 0..2 * WRITES as u64 {
            let id = MetaId {
                tree: MetaTree::Map,
                class: SizeClass::new(0).unwrap(),
                level: 0,
                index,
            };
            let frame = drive(&io, || meta.get(id)).unwrap();
            frame.try_lock().expect("no change under way").commit(&meta);
        }
    };
    let cp = db.db.checkpointer();
    let txns = db.db.transactions();
    let run = |most: Option<u64>| {
        dirty_pages();
        let ticket = cp.request_full();
        let mut units = 0u64;
        let ended = loop {
            if cp.fulls_done() >= ticket {
                break Some(Ok(()));
            }
            if most.is_some_and(|most| units >= most) {
                break None;
            }
            units += 1;
            match cp.one_unit(txns) {
                Err(e) => break Some(Err(e)),
                Ok(Some(c)) => io.wait(&c),
                Ok(None) => {}
            }
        };
        (units, ended)
    };
    let (landed, ended) = run(None);
    assert!(
        matches!(ended, Some(Ok(()))),
        "the checkpoint failed with its writes landing"
    );
    crash.set_faults_on(Some(&format!("{BASE}.map0.0")));
    crash.set_faults(Faults {
        write: 1.0,
        ..Faults::default()
    });
    let (units, ended) = run(Some(landed));
    crash.set_faults(Faults::default());
    crash.set_faults_on(None);
    match ended {
        Some(Err(crate::LimboError::CompletionError(_))) => {}
        Some(other) => panic!("the checkpoint ended {other:?}"),
        None => panic!(
            "the checkpoint still ran after {units} units, {landed} for one whose writes landed"
        ),
    }
}
