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
            lent_at_open: Default::default(),
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
        let mut ticket = checkpointer.request_full();
        drive(&self.io, || {
            checkpointer.step_full(&mut ticket, self.db.transactions())
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
    let step = |mut ticket: crate::storage::atoll::checkpoint::FullWait| loop {
        match checkpointer.step_full(&mut ticket, db.db.transactions()) {
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
    let failed = step(ticket).expect_err("a checkpoint went on past a failed write");
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
    let refused = step(checkpointer.request_full()).expect_err("a checkpoint after one failed");
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
/// they always run out. Whether a request is in flight at the run-out is the backend's own count:
/// the operation may be waiting for a place in a flight set, which a request's retirement gives
/// back, and no request lies beneath that wait.
fn drive_for<T>(
    crash: &CrashIO,
    left: &Cell<u64>,
    mut op: impl FnMut() -> Result<IOResult<T>>,
) -> Option<T> {
    loop {
        match op().unwrap() {
            IOResult::Done(v) => return Some(v),
            IOResult::IO(IOCompletions(c)) => loop {
                if left.get() == 0 {
                    OWNED_AT_RUN_OUT.with(|o| o.set(crash.in_flight() > 0));
                    return None;
                }
                left.set(left.get() - 1);
                crash.step().unwrap();
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
        let root = drive_for(crash, left, || Tree::create_versioned(db.pool(), class(0)))?;
        let db = Db::with(io.clone(), db, root);
        let txns = db.db.transactions();
        for v in 1..=8 {
            let mut txn = drive_for(crash, left, || txns.begin(Isolation::Snapshot))?;
            txns.statement(&mut txn).unwrap();
            let mut op = WriteOp::put(&[v as u8], vec![v as u8; 3000], Mode::Upsert);
            drive_for(crash, left, || {
                db.tree.write(txns, &db.versions, &mut txn, &mut op)
            })?;
            let mut commit = Commit::new(txn);
            drive_for(crash, left, || txns.commit(&mut commit))?;
        }
        let mut ticket = db.db.checkpointer().request_full();
        drive_for(crash, left, || {
            db.db.checkpointer().step_full(&mut ticket, txns)
        })?;
        // The next segment's zeros go one write at a time, then its sync: all of it lands.
        settle(crash, io, left)?;
        // Twice the pool of them: a pool's worth at least is written out, and the tree's pages
        // leave memory.
        let temp = class(5);
        for v in 0..2 * MIN_DB_BUDGET / temp.size() {
            let mut page = drive_for(crash, left, || db.db.pool().allocate_temporary(temp))?;
            page.page_mut().put_u64(HEADER_SIZE, v as u64);
            page.changed();
        }
        let mut txn = drive_for(crash, left, || txns.begin(Isolation::Snapshot))?;
        txns.statement(&mut txn).unwrap();
        for v in 1..=8u8 {
            let mut op = Get::new(&[v]);
            drive_for(crash, left, || {
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
    let mut txn = drive_for(crash, left, || txns.begin(Isolation::Snapshot))?;
    txns.statement(&mut txn).unwrap();
    for v in 1..=8u8 {
        let mut op = Get::new(&[v]);
        drive_for(crash, left, || {
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

/// With no checkpointing thread, a change the gate refused steps the cut that closed it (DESIGN.md
/// 9.1.3, the change gate closed by a cut): the cut's own stepper left it waiting on a change since
/// gone, and no one else would step it. Each try the gate refuses returns a wait that the IO it
/// stepped finishes, until the change goes in; a wait on the reopening alone would never end, and
/// a retry at once with the gate still closed would go round for good.
#[test]
fn a_change_the_gate_refused_steps_the_cut_left_waiting() {
    use super::buffer::{gate_closed, Owner};
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let db = try_open(io.clone(), 1).unwrap();
    let store = db.store().clone();
    let change = store.change().expect("the gate open");
    let mut ticket = db.checkpointer().request_full();
    let left_waiting = matches!(
        db.checkpointer().step_full(&mut ticket, db.transactions()),
        Ok(IOResult::IO(_))
    ) && store.gate_closed();
    drop(change);
    let mut owner = Owner::new();
    let mut refusals = 0;
    let outcome = loop {
        let tried = owner.call(|| {
            Ok::<_, crate::LimboError>(match store.change() {
                Some(_) => IOResult::Done(()),
                None => gate_closed::<()>(db.pool()),
            })
        });
        match tried {
            Ok(IOResult::Done(())) => break Ok(()),
            Ok(IOResult::IO(IOCompletions(wait))) => {
                refusals += 1;
                // Called again at once, with the gate still closed: refused again, a retry that
                // moved nothing.
                if wait.finished() && store.gate_closed() {
                    break Err(format!(
                        "refusal {refusals} came back at once with the gate still closed"
                    ));
                }
                if let Err(e) = io.step() {
                    break Err(format!("stepping IO failed: {e}"));
                }
                if !wait.finished() {
                    break Err(format!(
                        "refusal {refusals} waits on what nothing steps: the gate stays closed"
                    ));
                }
            }
            Err(e) => break Err(format!("the change failed: {e}")),
        }
    };
    drop(owner);
    let finished = drive(&io, || {
        db.checkpointer().step_full(&mut ticket, db.transactions())
    })
    .is_ok();
    drop(store);
    // Checked once the database is gone: a panic unwinding through it ends the process.
    drop(db);
    assert!(left_waiting, "the cut did not wait on the change under way");
    assert!(
        refusals > 0,
        "the gate refused nothing: the test reached no refusal"
    );
    if let Err(e) = outcome {
        panic!("{e}");
    }
    assert!(finished, "the full checkpoint did not finish");
}

/// A cut closing the gate while changes are inside waits for the last to leave, not a retry at
/// once: the same pending wait at every look while a change stays in (its holder paused), finished
/// by the last change out; a change leaving an open gate, or not the last out, takes no slow path
/// (DESIGN.md 9.1.3, the gate's close waiting for the changes under way).
#[test]
fn a_cut_closing_the_gate_waits_for_the_last_change_out() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let db = try_open(io.clone(), 1).unwrap();
    let store = db.store().clone();
    let quiet = store.change().expect("the gate open");
    drop(quiet);
    let uncontended = store.drain_wakes();
    let (first, second) = (
        store.change().expect("the gate open"),
        store.change().expect("the gate open"),
    );
    let mut waits = Vec::new();
    for _ in 0..32 {
        match store.close_gate() {
            IOResult::IO(IOCompletions(c)) => waits.push(c),
            IOResult::Done(()) => break,
        }
    }
    let pending = waits.len() == 32 && waits.iter().all(|c| !c.finished());
    drop(first);
    let after_first = waits.iter().any(|c| c.finished());
    drop(second);
    let after_last = waits.iter().all(|c| c.finished());
    let closed = matches!(store.close_gate(), IOResult::Done(()));
    let slow = store.drain_wakes();
    store.open_gate();
    drop(waits);
    drop(store);
    // Checked once the database is gone: a panic unwinding through it ends the process.
    drop(db);
    assert_eq!(
        uncontended, 0,
        "a change leaving an open gate took the slow path"
    );
    assert!(
        pending,
        "a look with changes inside came back ready, not on one pending wait"
    );
    assert!(
        !after_first,
        "a change out before the last finished the cut's wait"
    );
    assert!(
        after_last,
        "the last change out left the cut's wait unfinished"
    );
    assert!(closed, "the gate did not close once the changes were out");
    assert_eq!(slow, 1, "only the last change out takes the slow path");
}

/// A full checkpoint's step that finds the checkpointer's state held waits for its let-go, not a
/// retry at once: the same pending wait at every look while the holder is paused, finished by the
/// let-go; a let-go no caller waits on takes no slow path (DESIGN.md 9.1.3, the checkpointer's
/// state held by another stepper).
#[test]
fn a_full_checkpoint_step_finding_the_state_held_waits_for_its_let_go() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let db = try_open(io.clone(), 1).unwrap();
    let cp = db.checkpointer();
    drop(cp.hold().expect("the state free"));
    let uncontended = cp.released_wakes();
    let mut ticket = cp.request_full();
    let held = cp.hold().expect("the state free");
    let mut waits = Vec::new();
    for _ in 0..32 {
        match cp.step_full(&mut ticket, db.transactions()) {
            Ok(IOResult::IO(IOCompletions(c))) => waits.push(c),
            _ => break,
        }
    }
    let pending = waits.len() == 32 && waits.iter().all(|c| !c.finished());
    drop(held);
    let woken = waits.iter().all(|c| c.finished());
    let slow = cp.released_wakes();
    let finished = drive(&io, || cp.step_full(&mut ticket, db.transactions())).is_ok();
    drop(waits);
    // Checked once the database is gone: a panic unwinding through it ends the process.
    drop(db);
    assert_eq!(
        uncontended, 0,
        "a let-go with no one waiting took the slow path"
    );
    assert!(
        pending,
        "a look with the state held came back ready, not on one pending wait"
    );
    assert!(woken, "the let-go left the waiting step's wait unfinished");
    assert_eq!(slow, 1, "one let-go found the waiter");
    assert!(finished, "the full checkpoint did not finish");
}

/// How a checkpoint's look at a held metadata page meets the hold's end.
#[derive(Clone, Copy, Debug)]
enum HoldEnds {
    /// After the look marked the hold: the let-go finishes the look's wait.
    AfterTheMark,
    /// Between the look and its mark: the mark finds the hold changed, and the look yields.
    BeforeTheMark,
}

/// A checkpoint whose metadata phase finds a page dirty at its cut held by a change that has taken
/// the page's latch and not yet kept the page as the cut found it waits for the hold, or yields,
/// and asks the page again: the page is written for that checkpoint. Left unwritten, it would stay
/// dirty, and only a landed write puts a dirty page back on the stack a cut takes, so no later
/// checkpoint would name it (mutant: the phase asks each page once).
fn a_checkpoint_writes_a_metadata_page_it_found_held(ends: HoldEnds) {
    use super::meta::{MetaId, Tree as MetaTree};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::mpsc;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let db = Db::create(io.clone());
    for i in 0..20 {
        db.put(&format!("k{i:02}"), &value(i));
    }
    let meta = db.db.store().meta().clone();
    // A page of the size class no page of the database is in: the checkpoint's own edits, which
    // would keep it as the cut found it, never touch it.
    let id = MetaId {
        tree: MetaTree::Map,
        class: class(11),
        level: 0,
        index: 0,
    };
    let frame = drive(&io, || meta.get(id)).unwrap();
    frame.try_lock().expect("the page free").commit(&meta);
    let n = meta.in_charge() + 1;
    let (ran, met, written) = std::thread::scope(|s| {
        let (held_tx, held_rx) = mpsc::channel::<()>();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let (released_tx, released_rx) = mpsc::channel::<()>();
        let holder = &frame;
        // A change paused between taking the latch and keeping the page as the cut found it.
        s.spawn(move || {
            crate::race::at("meta.lock_before_keep", move || {
                held_tx.send(()).unwrap();
                let _ = go_rx.recv();
            });
            let hold = holder.try_lock().expect("the page free");
            hold.release_unchanged();
            released_tx.send(()).unwrap();
        });
        held_rx.recv().unwrap();
        // Ends the hold, once, and returns after it ended.
        let release = Rc::new(RefCell::new(Some((go_tx, released_rx))));
        let end_hold = {
            let release = release.clone();
            move || {
                if let Some((go, released)) = release.borrow_mut().take() {
                    go.send(()).unwrap();
                    released.recv().unwrap();
                }
            }
        };
        let met = Rc::new(Cell::new(false));
        {
            let (met, end_hold) = (met.clone(), end_hold.clone());
            crate::race::at("meta.write_found_held", move || {
                met.set(true);
                if matches!(ends, HoldEnds::BeforeTheMark) {
                    crate::race::at("meta.wait_before_mark", end_hold);
                }
            });
        }
        let cp = db.db.checkpointer();
        let mut ticket = cp.request_full();
        // What went wrong comes back as a value, so the hold ends before anything is checked and
        // the holder's join returns.
        let ran = (|| loop {
            match cp.step_full(&mut ticket, db.db.transactions()) {
                Err(e) => return Err(format!("the checkpoint failed: {e}")),
                Ok(IOResult::Done(())) => return Ok(()),
                Ok(IOResult::IO(IOCompletions(c))) => {
                    if met.get() {
                        end_hold();
                    }
                    let mut steps = 0;
                    while !c.finished() {
                        io.step().map_err(|e| format!("a step failed: {e}"))?;
                        steps += 1;
                        if steps > 64 {
                            return Err(
                                "the checkpoint waits on something no IO and no let-go ends".into(),
                            );
                        }
                    }
                }
            }
        })();
        end_hold();
        (ran, met.get(), frame.last_written_for())
    });
    // Checked once the database is gone: a panic unwinding through it ends the process.
    drop(frame);
    drop(meta);
    drop(db);
    ran.unwrap_or_else(|e| panic!("{ends:?}: {e}"));
    assert!(
        met,
        "{ends:?}: the checkpoint's metadata phase never found the page held"
    );
    assert_eq!(
        written, n,
        "{ends:?}: a metadata page found held was left unwritten by checkpoint {n}"
    );
}

#[test]
fn a_checkpoint_writes_a_metadata_page_found_held_once_its_marked_hold_ends() {
    a_checkpoint_writes_a_metadata_page_it_found_held(HoldEnds::AfterTheMark);
}

#[test]
fn a_checkpoint_writes_a_metadata_page_found_held_whose_hold_ended_before_the_mark() {
    a_checkpoint_writes_a_metadata_page_it_found_held(HoldEnds::BeforeTheMark);
}

/// Every yield in Atoll's code, by file, enclosing function and kind, with each one's class: the
/// change it follows, or the signal a held state's wait finishes, or the slice that still owes it
/// one (DESIGN.md 9.1.3, *No retry without a change*). Test modules and test files are left out:
/// a test's own retries are its own. A new site fails until classified; a site gone fails until
/// its row goes.
#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn every_yield_names_the_change_it_follows_or_the_wait_it_owes() {
    use super::testing::{atoll_dir, atoll_sources, code_of};
    use std::collections::BTreeMap;
    let mut found: BTreeMap<(String, String, &str), usize> = BTreeMap::new();
    for path in atoll_sources() {
        let rel = path
            .strip_prefix(atoll_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let name = path.file_name().unwrap().to_string_lossy();
        if name == "tests.rs"
            || name.ends_with("_tests.rs")
            || rel.split('/').any(|part| part == "tests")
            || name == "testing.rs"
            || name == "bench.rs"
        {
            continue;
        }
        let code = without_test_modules(&code_of(&std::fs::read_to_string(&path).unwrap()));
        for kind in ["Completion::new_yield", "busy", "look_again", "yield_now"] {
            let mut at = 0;
            while let Some(i) = code[at..].find(kind) {
                let start = at + i;
                at = start + kind.len();
                let before = code[..start].chars().next_back().unwrap_or(' ');
                let after = code[at..].trim_start();
                let bare = kind.contains("::")
                    || code[..start].ends_with("Self::")
                    || !(before.is_alphanumeric()
                        || before == '_'
                        || before == ':'
                        || before == '.');
                let word_end = !code[at..].starts_with(|c: char| c.is_alphanumeric() || c == '_');
                if !bare || !word_end || !after.starts_with('(') {
                    continue;
                }
                let line_start = code[..start].rfind('\n').map_or(0, |n| n + 1);
                if code[line_start..start].contains("fn ") {
                    continue;
                }
                let enclosing = code[..start]
                    .rmatch_indices("fn ")
                    .find_map(|(i, _)| {
                        let prev = code[..i].chars().next_back().unwrap_or(' ');
                        (!(prev.is_alphanumeric() || prev == '_')).then(|| {
                            code[i + 3..]
                                .chars()
                                .take_while(|c| c.is_alphanumeric() || *c == '_')
                                .collect::<String>()
                        })
                    })
                    .unwrap_or_default();
                *found.entry((rel.clone(), enclosing, kind)).or_default() += 1;
            }
        }
    }
    let listed: BTreeMap<(String, String, &str), usize> = YIELDS
        .iter()
        .map(|(file, f, kind, n, _)| ((file.to_string(), f.to_string(), *kind), *n))
        .collect();
    for (key, n) in &found {
        if listed.get(key) != Some(n) {
            eprintln!("unlisted or miscounted: {key:?} x{n}");
        }
    }
    assert_eq!(
        found, listed,
        "a yield site unclassified, or a row whose site is gone"
    );
}

/// Blanks every `#[cfg(...test...)]` module block, keeping line breaks.
#[cfg(not(any(shuttle, atoll_loom)))]
fn without_test_modules(code: &str) -> String {
    let mut out = code.to_string();
    let mut from = 0;
    while let Some(i) = out[from..].find("#[cfg(") {
        let attr = from + i;
        let close = attr + out[attr..].find(")]").unwrap() + 2;
        from = close;
        if !out[attr..close].contains("test") {
            continue;
        }
        let rest = out[close..].trim_start();
        let rest = rest.strip_prefix("pub(crate) ").unwrap_or(rest);
        let rest = rest.strip_prefix("pub(super) ").unwrap_or(rest);
        let rest = rest.strip_prefix("pub ").unwrap_or(rest);
        let Some(after_mod) = rest.strip_prefix("mod ") else {
            continue;
        };
        let header = after_mod.find(['{', ';']).unwrap();
        if after_mod.as_bytes()[header] == b';' {
            continue;
        }
        let open = out.len() - after_mod.len() + header;
        let mut depth = 0;
        let mut end = open;
        for (j, c) in out[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = open + j + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        let blank: String = out[attr..end]
            .chars()
            .map(|c| if c == '\n' { '\n' } else { ' ' })
            .collect();
        out.replace_range(attr..end, &blank);
        from = attr + blank.len();
    }
    out
}

/// The collector's step after a begin runs as no owner's operation (the census's K1, DESIGN.md
/// 9.1.3, *Maintenance keeps what it owns*): the begin discards what the collection comes to, so a
/// claim of it that comes short must never be the begin's demand, and must never take the begin's
/// reserved frame.
#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn the_collector_after_a_begin_runs_as_no_owners_operation() {
    use super::testing::{atoll_dir, code_of};
    let code = code_of(&std::fs::read_to_string(atoll_dir().join("sql/mod.rs")).unwrap());
    let start = code
        .find("fn collect_after_begin")
        .expect("the begin's collection");
    let body = &code[start..];
    let body = &body[..body.find("\n    }\n").expect("its body's end")];
    assert_eq!(
        body.matches("versions.collect(").count(),
        1,
        "the begin's collection is one call"
    );
    let collect = body.find("versions.collect(").unwrap();
    let scope = body[..collect]
        .rfind("unowned(")
        .expect("the begin's collection runs outside a scope that masks its owner");
    assert!(
        !body[scope..collect].contains(';'),
        "the scope does not enclose the collection"
    );
}

/// The checkpoint's step runs in its demand's scope (DESIGN.md 9.1.3, *Maintenance keeps what it
/// owns*): its claims name the checkpoint's demand, whoever steps it, never an operation's.
#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn the_checkpoint_steps_in_its_demands_scope() {
    use super::testing::{atoll_dir, code_of};
    let code = code_of(&std::fs::read_to_string(atoll_dir().join("checkpoint.rs")).unwrap());
    let state = code.find("impl State {").expect("the checkpointer's state");
    let body = &code[state..];
    let body = &body[..body.find("\n}\n").expect("its impl's end")];
    assert_eq!(
        body.matches("run.step(").count(),
        1,
        "the state steps its run once"
    );
    let step = body.find("run.step(").unwrap();
    let scope = body[..step]
        .rfind("upkeep_scope(")
        .expect("the run steps outside its demand's scope");
    assert!(
        !body[scope..step].contains(';'),
        "the scope does not enclose the run's step"
    );
}

/// The yield sites ([`every_yield_names_the_change_it_follows_or_the_wait_it_owes`]): file,
/// enclosing function, kind, count, class.
#[cfg(not(any(shuttle, atoll_loom)))]
const YIELDS: &[(&str, &str, &str, usize, &str)] = &[
    ("btree/mod.rs", "merge", "yield_now", 3, "change: a descent's Restart; the pairs read failed validation; the test's shrink hold"),
    ("btree/mod.rs", "merge_pair", "yield_now", 2, "change: a corrupt read under an invalid parent, a pair no longer related"),
    ("btree/mod.rs", "run", "yield_now", 1, "change: a descent's Restart or Shorter"),
    ("btree/mod.rs", "shrink", "yield_now", 1, "change: the root no longer over the child alone"),
    ("btree/mod.rs", "split", "yield_now", 1, "change: a descent's Restart"),
    ("btree/mod.rs", "yield_now", "Completion::new_yield", 1, "the helper itself"),
    ("btree/mod.rs", "yield_now", "yield_now", 1, "change: the found! macro's Restart or Shorter"),
    ("btree/value.rs", "copy", "Completion::new_yield", 1, "change: a value page moved or evicted while copied"),
    ("btree/value.rs", "directory", "Completion::new_yield", 1, "change: a chunk page moved or evicted while read"),
    ("buffer/cell.rs", "current_with", "Completion::new_yield", 1, "change: the cell changed during its read"),
    ("buffer/lend.rs", "provide", "Completion::new_yield", 1, "change: a batch changed hands, or drains for another"),
    ("buffer/mod.rs", "cut_run", "Completion::new_yield", 1, "change: an outgoing frame freed or reused while read"),
    ("buffer/mod.rs", "claim", "look_again", 3, "change: work of the claim's own started, a latch it found changed, a horizon reached"),
    ("buffer/mod.rs", "free_page", "Completion::new_yield", 1, "change: its own write landed (settle_writes)"),
    ("buffer/mod.rs", "look_again", "Completion::new_yield", 1, "the helper itself"),
    ("buffer/mod.rs", "wait_for_resident_latch", "Completion::new_yield", 3, "change: the latch free at the look, the frame no longer resident, the hold changed before the mark"),
    ("buffer/shortage.rs", "gate_closed", "Completion::new_yield", 1, "held: the gate's reopening, waited at the operation's top (wait_for_gate)"),
    ("buffer/shortage.rs", "run", "Completion::new_yield", 1, "change: the gate found open after the wait or the cut stepped"),
    ("buffer/shortage.rs", "short", "Completion::new_yield", 1, "held: memory, reserved at the operation's top (the shortage queue)"),
    ("checkpoint.rs", "step_full", "Completion::new_yield", 1, "change: a step of its own done and more of the full checkpoint to do"),
    ("checkpoint.rs", "claim_full", "Completion::new_yield", 2, "change: a claimed step done and more of the full checkpoint to do, or the task over since the writer looked"),
    ("meta.rs", "get", "Completion::new_yield", 1, "change: a load's phase moved before the wait was registered"),
    ("meta.rs", "wait_while_held", "Completion::new_yield", 2, "change: the hold let go or changed before the mark"),
    ("meta.rs", "write_for", "Completion::new_yield", 3, "change: the writing cell changed, a validation failed, a write put first by another"),
    ("store.rs", "busy", "Completion::new_yield", 1, "the helper itself"),
    ("store.rs", "edit_cut", "busy", 1, "change: a tree's height changed after the latches"),
    ("store.rs", "free_slot", "busy", 1, "change: a tree's height changed after the latches"),
    ("txn/mod.rs", "wait_for_reservation", "Completion::new_yield", 1, "change: the reservation changed after the look"),
];

/// The checkpoint keeps its flight set full: each unit starts one write, and it waits only once
/// [`super::checkpoint::IMAGE_WRITES`] are in flight (DESIGN.md 22.1; mutant: a wait after every
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
    // Asserted once the database is closed: a panic through it ends the process.
    drop(db);
    assert_eq!(peak, flight_set(), "image writes in flight at most");
}

/// The image writes of the tree's pages a checkpoint keeps in flight: [`super::checkpoint::IMAGE_WRITES`]
/// of them, within its write bytes.
fn flight_set() -> usize {
    super::checkpoint::IMAGE_WRITES.min(super::checkpoint::WRITE_BYTES / class(0).size())
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
    let ticket = cp.request_full();
    let full = flight_set();
    // Units until the flight set is full, the IO stepped only for what is no image write; a
    // checkpoint that ends first never filled it, which the asserts below find.
    while cp.fulls_done() < ticket.ticket() {
        match cp.one_unit(txns).unwrap() {
            Some(_) if cp.image_writes_in_flight() == full => break,
            Some(c) => io.wait(&c),
            None => {}
        }
    }
    let cancelled = cp.cancel_image_writes();
    // A failure taken for a landing is no wait: read as none.
    let waited = cp.one_unit(txns).ok().flatten();
    let held = cp.image_writes_in_flight();
    let pending = waited.as_ref().map(|c| !c.finished());
    while crash.in_flight() > 0 {
        io.step().unwrap();
    }
    let ended = waited.as_ref().map(Completion::finished);
    // Asserted once the database is closed: a panic through it ends the process.
    drop(db);
    assert_eq!(cancelled, full);
    assert_eq!(
        held, full,
        "a cancelled write let its place go before it retired"
    );
    assert_eq!(
        pending,
        Some(true),
        "the unit waits for a place, on a completion not finished yet"
    );
    assert_eq!(ended, Some(true), "the retirement did not end the wait");
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
    while cp.fulls_done() < ticket.ticket() {
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
    let left = db.db.log().flush_asked();
    // Asserted once the database is closed: a panic through it ends the process.
    drop(db);
    assert!(
        asked > 0,
        "no turn toward the intent's durability was asked for again"
    );
    assert!(
        turns_after > 0,
        "the checkpoint took no turn after the request made during its last"
    );
    assert!(
        !left,
        "a flush asked for during the checkpoint's turn was left unanswered"
    );
}
