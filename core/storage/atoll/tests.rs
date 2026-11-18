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

pub(super) const BASE: &str = "/db/atoll";

pub(super) fn config() -> AtollConfig {
    AtollConfig {
        log: LogConfig {
            segment_bytes: 512 << 10,
            // Segments of 16, 32, 64, 128 and 256 KiB before those of 512 (DISK-MIN1): every crash
            // test here recovers across segments of several sizes.
            first_segment_bytes: 16 << 10,
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
    try_open_with(io, threads, config())
}

fn try_open_with(io: Arc<dyn IO>, threads: usize, config: AtollConfig) -> Result<Atoll> {
    let opening = Opening::new(io, BASE, config, None)?;
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
    let (_, sb) = Control::open(
        crate::storage::atoll::requests::counted_for_test(io.clone()),
        BASE,
    )
    .unwrap();
    sb.expect("a superblock in charge").continuations
}

/// A database and one versioned tree in it.
pub(super) struct Db {
    io: Arc<dyn IO>,
    db: Atoll,
    versions: Versions,
    tree: VersionedTree,
}

impl Db {
    /// A new database with its tree, in a checkpoint before any write, as a database's creation
    /// has its system trees.
    pub(super) fn create(io: Arc<dyn IO>) -> Self {
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
    pub(super) fn put(&self, key: &str, value: &[u8]) {
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

    /// What a transaction begun now reads of `key`, or the error its read ended with.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let txns = self.db.transactions();
        let mut txn = self.begin();
        let mut op = Get::new(key.as_bytes());
        let read = drive(&self.io, || {
            self.tree.get(txns, &self.versions, &mut txn, &mut op)
        })
        .map_err(|e| e.to_string());
        let mut commit = Commit::new(txn);
        drive(&self.io, || txns.commit(&mut commit)).unwrap();
        read
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
/// A database whose log ends inside a block, as a crash that tore its last block leaves it: created,
/// an empty run published with no wait so the next commit starts inside a block, one commit, a
/// close, then that commit's bytes zeroed to its block's end. Its reopening records the gap to the
/// next block with a superblock (D1a §6.11 a).
pub(super) fn ending_inside_a_block(io: Arc<dyn IO>) {
    let db = Db::create(io.clone());
    let run = super::txn::changes::Changes::new();
    drive(&db.io, || {
        db.db
            .transactions()
            .publish_alone(&mut crate::storage::atoll::log::LogWait::default(), &run)
    })
    .unwrap();
    let from = db.db.log().end();
    db.put("k", &value(1));
    let to = db.db.log().end();
    let segments = segments_to(db.db.log(), to);
    drop(db);
    let block_end = from.next_multiple_of(BLOCK);
    assert!(
        from % BLOCK != 0 && to <= block_end,
        "the commit [{from}, {to}) is not inside one block"
    );
    let log = config().log;
    let first_record = |io: Arc<dyn IO>| {
        Log::reader(io, BASE, &log, from)
            .next_record()
            .unwrap()
            .map(|r| r.start)
    };
    assert_eq!(first_record(io.clone()), Some(from), "no record at {from}");
    let (number, at) = place(&segments, from);
    let file = io
        .open_file(&format!("{BASE}.log.{number:016x}"), OpenFlags::None, false)
        .unwrap();
    let len = (block_end - from) as usize;
    let zeros = Arc::new(AlignedBlock::zeroed(len.next_multiple_of(BLOCK as usize)));
    let c = file
        .pwrite(at, zeros.region(0, len), Completion::new_write(|_| {}))
        .unwrap();
    io.wait_for_completion(c).unwrap();
    let c = file.sync_data(Completion::new_sync(|_| {})).unwrap();
    io.wait_for_completion(c).unwrap();
    assert_eq!(
        first_record(io.clone()),
        None,
        "the zeros at {from} were not its record's"
    );
}

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
    let (last, size) = db.db.log().segment_at(db.db.log().end());
    assert!(
        last >= 3 && size >= 128 << 10,
        "the log reached segment {last} of {size} bytes: the crashes met too few sizes"
    );
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

/// VALUE-LOST1: a value's page imaged by a checkpoint from its swap copy, its frame evicted before the
/// checkpoint settles it, is settled all the same: its swap copy given up, so no later cut owes it
/// and leaves it out of its map. After a crash, a recovery from a later checkpoint replays values
/// committed since, which take pages free in the map in charge, and the first still reads whole.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_value_whose_frame_is_evicted_before_its_settle_keeps_its_page_through_a_crash() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_value_whose_frame_is_evicted_before_its_settle_keeps_its_page_through_a_crash"
        ),
        || evicted_before_its_settle(Ending::Crash),
    );
}

/// The same with a clean close and an open: the open replays what the last checkpoint does not
/// hold, as a recovery does.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_value_whose_frame_is_evicted_before_its_settle_keeps_its_page_through_a_close() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_value_whose_frame_is_evicted_before_its_settle_keeps_its_page_through_a_close"
        ),
        || evicted_before_its_settle(Ending::Close),
    );
}

/// How a database is left before it is opened again.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[derive(Clone, Copy)]
enum Ending {
    Crash,
    Close,
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn evicted_before_its_settle(ending: Ending) {
    let crash_io = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash_io.clone();
    let db = Db::create(io.clone());
    let root = db.tree.root();
    let pool = db.db.pool().clone();
    let store = db.db.store().clone();
    let first = vec![b'a'; 60_000];
    db.put("a", &first);
    // Written back and evicted: the value's page lives in its swap copy alone.
    let mut sweep = pool.sweep();
    drive(&db.io, || sweep.step(&pool)).unwrap();
    pool.evict_all_clean();
    let values: Vec<PageId> = store
        .swap_pages()
        .into_iter()
        .filter(|p| p.class().size() >= first.len())
        .collect();
    let value = values[0];
    // The checkpoint reads it in to image it; its frame goes once its image landed, before its
    // settle.
    let evicted = std::rc::Rc::new(Cell::new(false));
    evict_at_its_settle(&pool, &store, value, evicted.clone());
    db.checkpoint();
    let evicted_at_its_checkpoint = evicted.get();
    let swap_after_settle = store.swap_slot(value).is_some();
    let mut entries = Vec::new();
    for _ in 0..3 {
        db.checkpoint();
        entries.push(drive(&db.io, || store.entry(value)).unwrap());
    }
    // Values committed past the last checkpoint: the next open replays them, their values taking
    // pages free in the map in charge.
    for i in 0..8u8 {
        db.put(&format!("c{i}"), &vec![b'c' + i; 60_000]);
    }
    let read_live = db.read("a");
    drop(pool);
    drop(store);
    let reopened = match ending {
        Ending::Crash => {
            let image = crash_io.crash(0);
            drop(db);
            Db::recover(Arc::new(image), root, 1).unwrap()
        }
        Ending::Close => {
            drop(db);
            Db::recover(io, root, 1).unwrap()
        }
    };
    reopened.put("d", &vec![b'd'; 60_000]);
    let read_reopened = reopened.read("a");
    drop(reopened);
    assert_eq!(
        values.len(),
        1,
        "the value's pages in swap alone: {values:?}"
    );
    assert!(
        evicted_at_its_checkpoint,
        "the value's frame evicted before its settle"
    );
    assert!(
        !swap_after_settle,
        "the settle gave up the value's swap copy"
    );
    assert!(
        entries
            .iter()
            .all(|&e| e != crate::storage::atoll::radix::FREE),
        "the value's page free in the map after its settle: {entries:?}"
    );
    assert_eq!(read_live, Ok(Some(first.clone())), "the first value, live");
    assert_eq!(
        read_reopened,
        Ok(Some(first)),
        "the first value, opened again"
    );
}

/// A load of a value's page that claims its frame after its settle looked at the table under the
/// swap table's lock, and gave the swap copy up, finds no swap copy: it reads the checkpoint's
/// copy, which the image named before the settle, and comes in owed nothing.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_load_after_the_settle_gave_the_swap_copy_up_reads_the_checkpoint_copy_and_is_owed_nothing() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_load_after_the_settle_gave_the_swap_copy_up_reads_the_checkpoint_copy_and_is_owed_nothing"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(CrashIO::new(0));
            let db = Db::create(io);
            let pool = db.db.pool().clone();
            let store = db.db.store().clone();
            let first = vec![b'a'; 60_000];
            db.put("a", &first);
            let value = value_in_swap_alone(&db, first.len());
            let evicted = std::rc::Rc::new(Cell::new(false));
            evict_at_its_settle(&pool, &store, value, evicted.clone());
            let loading = std::rc::Rc::new(Cell::new(false));
            load_once_its_swap_copy_is_given_up(&pool, &store, value, loading.clone());
            db.checkpoint();
            let swap_after = store.swap_slot(value);
            let landed = drive(&db.io, || pool.fix_exclusive(value, None))
                .map(|g| drop(g.release_unchanged()))
                .map_err(|e| e.to_string());
            let owed = pool.frame_owed(value);
            let read = db.read("a");
            drop(pool);
            drop(store);
            drop(db);
            assert!(evicted.get(), "the value's frame evicted before its settle");
            assert!(loading.get(), "the load began after the swap copy was given up");
            assert_eq!(swap_after, None, "the swap copy outlived the settle");
            assert_eq!(landed, Ok(()), "the load");
            assert_eq!(owed, Some(false), "the page the load brought in is owed");
            assert_eq!(read, Ok(Some(first)), "the value");
        },
    );
}

/// A value's frame another thread holds at its settle, as an evictor's try or a fix may: the
/// settle waits for the latch and settles the page once it is let go, so the page is owed nothing
/// and its swap copy goes. Before, the settle returned at once and left the page owed, and the
/// next cut left it out (VALUE-LOST1's second way, with no eviction).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_value_whose_frame_is_held_at_its_settle_is_settled_once_let_go() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_value_whose_frame_is_held_at_its_settle_is_settled_once_let_go"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(CrashIO::new(0));
            let db = Db::create(io.clone());
            let pool = db.db.pool().clone();
            let store = db.db.store().clone();
            let first = vec![b'a'; 60_000];
            db.put("a", &first);
            let value = value_in_swap_alone(&db, first.len());
            let (release, released) = std::sync::mpsc::channel::<()>();
            let holding = Holding {
                held: std::rc::Rc::new(Cell::new(false)),
                waited: std::rc::Rc::new(Cell::new(false)),
                holder: std::rc::Rc::new(std::cell::RefCell::new(None)),
            };
            hold_at_its_settle(
                &pool,
                &store,
                &io,
                value,
                released,
                release.clone(),
                holding.clone(),
            );
            db.checkpoint();
            // The settle never waited: the holder still waits to let go.
            let _ = release.send(());
            if let Some(holder) = holding.holder.borrow_mut().take() {
                holder.join().unwrap();
            }
            let swap_after = store.swap_slot(value);
            let owed = pool.frame_owed(value);
            let read = db.read("a");
            drop(pool);
            drop(store);
            drop(db);
            assert!(
                holding.held.get(),
                "another thread held the frame at its settle"
            );
            assert!(
                holding.waited.get(),
                "the settle went on past a frame another held"
            );
            assert_eq!(swap_after, None, "the swap copy outlived the settle");
            assert_eq!(owed, Some(false), "the page is owed after its settle");
            assert_eq!(read, Ok(Some(first)), "the value");
        },
    );
}

/// VALUE-LEAK1: a page number taken before a cut whose page is not yet changed is in no image of
/// that checkpoint and is not left out by it, so its entry in the map the checkpoint writes reads
/// taken with no copy anywhere. After a crash, a recovery from that checkpoint must find the number
/// free: nothing names the page and nothing would ever free it.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_number_taken_before_a_cut_and_never_changed_is_free_after_a_recovery() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_number_taken_before_a_cut_and_never_changed_is_free_after_a_recovery"
        ),
        || {
            let crash_io = Arc::new(CrashIO::new(0));
            let io: Arc<dyn IO> = crash_io.clone();
            let db = Db::create(io);
            let root = db.tree.root();
            db.put("a", &vec![b'a'; 60_000]);
            let pool = db.db.pool().clone();
            let store = db.db.store().clone();
            let class = SizeClass::fitting(60_000).unwrap();
            let taken = drive(&db.io, || pool.allocate(class)).unwrap();
            let pid = taken.pid();
            drop(taken);
            db.checkpoint();
            let before = drive(&db.io, || store.entry(pid)).unwrap();
            drop(pool);
            drop(store);
            let image = crash_io.crash(0);
            drop(db);
            let recovered = Db::recover(Arc::new(image), root, 1).unwrap();
            let after = drive(&recovered.io, || recovered.db.store().entry(pid)).unwrap();
            drop(recovered);
            assert_eq!(
                before,
                crate::storage::atoll::radix::ALLOCATED,
                "the number as it stands after the checkpoint that left it out"
            );
            assert_eq!(
                after,
                crate::storage::atoll::radix::FREE,
                "the number taken before the cut, never changed, after a recovery"
            );
        },
    );
}

/// A put made while a checkpoint's cut walks the frames, by a writer that commits after the cut, is
/// in no image of that checkpoint, whose snapshot does not see it, and comes back by replay after a
/// crash (OPEN-CORRUPT1, point 4: a leaf put does not enter the change gate).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_put_during_the_cuts_walk_committed_after_it_comes_back_by_replay() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_put_during_the_cuts_walk_committed_after_it_comes_back_by_replay"
        ),
        || {
            let after = put_during_the_walk(true);
            assert_eq!(
                after,
                Ok(Some(b"two".to_vec())),
                "a put during the walk, committed after the cut, after a crash"
            );
        },
    );
}

/// A put made while a checkpoint's cut walks the frames, by a writer that never commits, is in no
/// image of that checkpoint and does not come back after a crash.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_put_during_the_cuts_walk_never_committed_does_not_come_back() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_put_during_the_cuts_walk_never_committed_does_not_come_back"
        ),
        || {
            let after = put_during_the_walk(false);
            assert_eq!(
                after,
                Ok(Some(b"one".to_vec())),
                "a put during the walk, never committed, after a crash"
            );
        },
    );
}

/// A database with `a` committed as `one`; a checkpoint whose cut's first run of the frame walk
/// is where a writer puts `a` as `two`; that writer committed after the checkpoint if `commits`,
/// else left open; then a crash and a recovery: what a read of `a` after the recovery gives.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn put_during_the_walk(commits: bool) -> Result<Option<Vec<u8>>, String> {
    use std::cell::RefCell;
    use std::rc::Rc;
    let crash_io = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash_io.clone();
    let db = Rc::new(Db::create(io));
    let root = db.tree.root();
    db.put("a", b"one");
    let writer = Rc::new(RefCell::new(Some(db.begin())));
    {
        let (db, writer) = (db.clone(), writer.clone());
        crate::race::at("buffer.cut_run_listing", move || {
            let txns = db.db.transactions();
            let mut op = WriteOp::put(b"a", b"two".to_vec(), Mode::Upsert);
            let mut guard = writer.borrow_mut();
            let txn = guard.as_mut().expect("the writer");
            let write = drive(&db.io, || db.tree.write(txns, &db.versions, txn, &mut op));
            assert!(matches!(write, Ok(Write::Done(_))), "the put: {write:?}");
        });
    }
    db.checkpoint();
    assert!(
        !crate::race::pending("buffer.cut_run_listing"),
        "the cut's walk never reached its listing"
    );
    let txn = writer.borrow_mut().take().expect("the writer");
    let txns = db.db.transactions();
    let image = if commits {
        let mut commit = Commit::new(txn);
        drive(&db.io, || txns.commit(&mut commit)).unwrap();
        crash_io.crash(0)
    } else {
        let image = crash_io.crash(0);
        let mut abort = super::txn::Abort::new(txn);
        drive(&db.io, || txns.rollback(&db.versions, &mut abort)).unwrap();
        image
    };
    drop(writer);
    drop(Rc::try_unwrap(db).ok().expect("the database held once"));
    let recovered = Db::recover(Arc::new(image), root, 1).unwrap();
    let after = recovered.read("a");
    drop(recovered);
    after
}

/// VALUE-LEAK1: a number taken at the page store with no frame yet when a checkpoint cuts, as
/// between a take and the frame's install, is in no image and left out by none. After a crash, a
/// recovery from that checkpoint must find it free.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_number_taken_with_no_frame_at_a_cut_is_free_after_a_recovery() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_number_taken_with_no_frame_at_a_cut_is_free_after_a_recovery"
        ),
        || {
            let crash_io = Arc::new(CrashIO::new(0));
            let io: Arc<dyn IO> = crash_io.clone();
            let db = Db::create(io);
            let root = db.tree.root();
            let store = db.db.store().clone();
            let class = SizeClass::new(0).unwrap();
            let pid = drive(&db.io, || store.allocate(class)).unwrap();
            db.checkpoint();
            drop(store);
            let image = crash_io.crash(0);
            drop(db);
            let recovered = Db::recover(Arc::new(image), root, 1).unwrap();
            let after = drive(&recovered.io, || recovered.db.store().entry(pid)).unwrap();
            drop(recovered);
            assert_eq!(
                after,
                crate::storage::atoll::radix::FREE,
                "the number taken with no frame at the cut, after a recovery"
            );
        },
    );
}

/// VALUE-LEAK1: a number taken before a cut and freed after it, before the checkpoint looks for
/// numbers to leave out. The cut's page table still holds it taken, the live one free; the
/// checkpoint reads the cut's, so it leaves the number out, and a recovery from it finds the
/// number free.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_number_taken_before_a_cut_and_freed_during_its_checkpoint_is_free_after_a_recovery() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_number_taken_before_a_cut_and_freed_during_its_checkpoint_is_free_after_a_recovery"
        ),
        || {
            let crash_io = Arc::new(CrashIO::new(0));
            let io: Arc<dyn IO> = crash_io.clone();
            let db = Db::create(io);
            let root = db.tree.root();
            let store = db.db.store().clone();
            let class = SizeClass::new(0).unwrap();
            let pid = drive(&db.io, || store.allocate(class)).unwrap();
            let freed = std::rc::Rc::new(Cell::new(None));
            {
                let (io, store, freed) = (db.io.clone(), store.clone(), freed.clone());
                crate::race::at("checkpoint.untaken_begins", move || {
                    drive(&io, || store.free(pid)).unwrap();
                    freed.set(Some(drive(&io, || store.entry(pid)).unwrap()));
                });
            }
            db.checkpoint();
            drop(store);
            let image = crash_io.crash(0);
            drop(db);
            let recovered = Db::recover(Arc::new(image), root, 1).unwrap();
            let after = drive(&recovered.io, || recovered.db.store().entry(pid)).unwrap();
            drop(recovered);
            assert_eq!(
                freed.get(),
                Some(crate::storage::atoll::radix::FREE),
                "the number as it stands once freed during the checkpoint"
            );
            assert_eq!(
                after,
                crate::storage::atoll::radix::FREE,
                "the number freed during the checkpoint, after a recovery"
            );
        },
    );
}

/// A number left out because it was taken and unchanged at one cut, then written as a node: the
/// next checkpoint copies it, and after a crash a recovery from that checkpoint reads it as written.
/// The leave-out edits the cut's map alone; the map as it stands keeps the number taken.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_number_left_out_at_one_cut_and_written_after_is_copied_by_the_next_checkpoint() {
    crate::storage::atoll::testing::cleanly(
        concat!(
            module_path!(),
            "::a_number_left_out_at_one_cut_and_written_after_is_copied_by_the_next_checkpoint"
        ),
        || {
            use super::btree::node;
            let crash_io = Arc::new(CrashIO::new(0));
            let io: Arc<dyn IO> = crash_io.clone();
            let db = Db::create(io);
            let root = db.tree.root();
            let pool = db.db.pool().clone();
            let store = db.db.store().clone();
            let class = SizeClass::new(0).unwrap();
            let taken = drive(&db.io, || pool.allocate(class)).unwrap();
            let pid = taken.pid();
            drop(taken);
            db.checkpoint();
            let live_after_first = drive(&db.io, || store.entry(pid)).unwrap();
            let mut page = drive(&db.io, || pool.fix_exclusive(pid, None)).unwrap();
            let layout = node::Layout {
                root: pid,
                level: 0,
                lower: Some(b"left out, then written"),
                upper: None,
                next: (PageId::from_raw(0), 0),
                reserving: false,
            };
            node::build(&mut page.page_mut(), &layout, []);
            let written = node::image(page.page());
            page.changed();
            drop(page);
            db.checkpoint();
            let live_after_second = drive(&db.io, || store.entry(pid)).unwrap();
            drop(pool);
            drop(store);
            let image = crash_io.crash(0);
            drop(db);
            let recovered = Db::recover(Arc::new(image), root, 1).unwrap();
            let after = drive(&recovered.io, || recovered.db.store().entry(pid)).unwrap();
            let read = drive(&recovered.io, || {
                recovered.db.pool().fix_exclusive(pid, None)
            })
            .map(|page| {
                let read = node::image(page.page());
                drop(page.release_unchanged());
                read
            })
            .map_err(|e| e.to_string());
            drop(recovered);
            assert_eq!(
                live_after_first,
                crate::storage::atoll::radix::ALLOCATED,
                "the number as it stands after the cut that left it out"
            );
            assert!(
                live_after_second >= crate::storage::atoll::radix::SLOT_BASE,
                "the next checkpoint did not copy it: {live_after_second}"
            );
            assert_eq!(after, live_after_second, "its entry after a recovery");
            assert_eq!(read, Ok(written), "the page after a recovery");
        },
    );
}

/// The check behind OPEN-CORRUPT1, in every build: a checkpoint that would image a node page past
/// the change count its cut found it at, a change since the cut having kept no copy, ends the
/// process with its words instead of writing a map no open can use. The mark is taken off the
/// tree's leaf once the cut has ended, as the missed mark left it, and the leaf changed.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_checkpoint_that_would_image_a_page_past_its_cut_ends_the_process() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::a_checkpoint_that_would_image_a_page_past_its_cut_ends_the_process"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(CrashIO::new(0));
            let db = Db::create(io);
            db.put("a", b"one");
            let pool = db.db.pool().clone();
            let leaf = db.tree.root();
            {
                let (pool, io) = (pool.clone(), db.io.clone());
                crate::race::at("checkpoint.cut_ended", move || {
                    assert!(pool.forget_cut_mark(leaf), "the leaf has no frame");
                    let mut fixed = drive(&io, || pool.fix_exclusive(leaf, None)).unwrap();
                    fixed.changed();
                    drop(fixed);
                });
            }
            db.checkpoint();
        },
    );
    let Some(ended) = ended else { return };
    assert_ne!(
        ended.code,
        Some(0),
        "the checkpoint went on:\n{}",
        ended.stderr
    );
    assert!(
        ended.stderr.contains("a change since the cut kept no copy"),
        "{}",
        ended.stderr
    );
}

/// The check behind VALUE-LOST1, in every build: a checkpoint that would leave a value's page out
/// of its map as unnamed while the page has a checkpoint copy, so freeing a live value, ends the
/// process with its words instead. A value's page marked changed after its checkpoint, its bytes as
/// they were, is owed at the next cut with no image naming it, as a settle that kept its swap copy
/// left one.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_checkpoint_that_would_free_a_live_values_page_ends_the_process() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::a_checkpoint_that_would_free_a_live_values_page_ends_the_process"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(CrashIO::new(0));
            let db = Db::create(io);
            let pool = db.db.pool().clone();
            let store = db.db.store().clone();
            db.put("a", &vec![b'a'; 60_000]);
            let mut sweep = pool.sweep();
            drive(&db.io, || sweep.step(&pool)).unwrap();
            pool.evict_all_clean();
            let value = store
                .swap_pages()
                .into_iter()
                .find(|p| p.class().size() >= 60_000)
                .expect("the value's page in swap alone");
            db.checkpoint();
            // Owed again with its bytes unchanged: written to a new swap copy, its frame gone.
            let mut fixed = drive(&db.io, || pool.fix_exclusive(value, None)).unwrap();
            fixed.changed();
            drop(fixed);
            let mut sweep = pool.sweep();
            drive(&db.io, || sweep.step(&pool)).unwrap();
            pool.evict_all_clean();
            db.checkpoint();
        },
    );
    let Some(ended) = ended else { return };
    assert_ne!(
        ended.code,
        Some(0),
        "the checkpoint went on:\n{}",
        ended.stderr
    );
    assert!(
        ended.stderr.contains(
            "out of its map as a value no image names, but the page has a checkpoint copy in slot"
        ),
        "{}",
        ended.stderr
    );
}

/// Evicts `pid`'s frame at its own settle: the first settle after a checkpoint copy of it landed
/// (its entry set to the copy just before), `evicted` then set. Its frame not evictable there,
/// `evicted` stays unset.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn evict_at_its_settle(
    pool: &Arc<super::buffer::BufferPool>,
    store: &Arc<super::store::PageStore>,
    pid: PageId,
    evicted: std::rc::Rc<Cell<bool>>,
) {
    let (pool_at, store_at) = (pool.clone(), store.clone());
    crate::race::at("buffer.settle_begins", move || {
        let copied = matches!(
            store_at.entry(pid),
            Ok(IOResult::Done(e)) if e >= crate::storage::atoll::radix::SLOT_BASE
        );
        if copied {
            evicted.set(pool_at.evict_clean(pid));
        } else {
            evict_at_its_settle(&pool_at, &store_at, pid, evicted);
        }
    });
}

/// Starts a load of `pid` once a settle has given its swap copy up and let go of the swap table's
/// lock, its read left in flight; `began` set then.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn load_once_its_swap_copy_is_given_up(
    pool: &Arc<super::buffer::BufferPool>,
    store: &Arc<super::store::PageStore>,
    pid: PageId,
    began: std::rc::Rc<Cell<bool>>,
) {
    let (pool_at, store_at) = (pool.clone(), store.clone());
    crate::race::at("store.swap_given_up", move || {
        if store_at.swap_slot(pid).is_none() {
            began.set(matches!(
                pool_at.fix_exclusive(pid, None),
                Ok(IOResult::IO(_))
            ));
        } else {
            load_once_its_swap_copy_is_given_up(&pool_at, &store_at, pid, began);
        }
    });
}

/// What [`hold_at_its_settle`] saw: the holder's thread, whether it held the frame, and whether
/// the settle waited for it.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[derive(Clone)]
struct Holding {
    held: std::rc::Rc<Cell<bool>>,
    waited: std::rc::Rc<Cell<bool>>,
    holder: std::rc::Rc<std::cell::RefCell<Option<std::thread::JoinHandle<()>>>>,
}

/// At `pid`'s own settle (the first after its checkpoint copy's entry is set), another thread
/// fixes its frame exclusively and holds it until `released` gets a word: `release` sends it once
/// that settle has waited.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn hold_at_its_settle(
    pool: &Arc<super::buffer::BufferPool>,
    store: &Arc<super::store::PageStore>,
    io: &Arc<dyn IO>,
    pid: PageId,
    released: std::sync::mpsc::Receiver<()>,
    release: std::sync::mpsc::Sender<()>,
    holding: Holding,
) {
    let (pool_at, store_at, io_at) = (pool.clone(), store.clone(), io.clone());
    crate::race::at("buffer.settle_begins", move || {
        let copied = matches!(
            store_at.entry(pid),
            Ok(IOResult::Done(e)) if e >= crate::storage::atoll::radix::SLOT_BASE
        );
        if !copied {
            return hold_at_its_settle(
                &pool_at, &store_at, &io_at, pid, released, release, holding,
            );
        }
        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let (pool_thread, io_thread) = (pool_at.clone(), io_at.clone());
        let thread = std::thread::spawn(move || {
            let fixed = drive(&io_thread, || pool_thread.fix_exclusive(pid, None)).unwrap();
            held_tx.send(()).unwrap();
            let _ = released.recv();
            drop(fixed.release_unchanged());
        });
        holding.held.set(
            held_rx
                .recv_timeout(std::time::Duration::from_secs(60))
                .is_ok(),
        );
        *holding.holder.borrow_mut() = Some(thread);
        let waited = holding.waited.clone();
        crate::race::at("checkpoint.settle_waits", move || {
            waited.set(true);
            let _ = release.send(());
        });
    });
}

/// Writes back the value of `len` bytes just put and evicts every clean page: the value's page
/// then lives in its swap copy alone. That page.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn value_in_swap_alone(db: &Db, len: usize) -> PageId {
    let pool = db.db.pool().clone();
    let mut sweep = pool.sweep();
    drive(&db.io, || sweep.step(&pool)).unwrap();
    pool.evict_all_clean();
    let values: Vec<PageId> = db
        .db
        .store()
        .swap_pages()
        .into_iter()
        .filter(|p| p.class().size() >= len)
        .collect();
    assert_eq!(
        values.len(),
        1,
        "the value's pages in swap alone: {values:?}"
    );
    values[0]
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
            drive(&db.io, || {
                db.db
                    .transactions()
                    .publish_alone(&mut crate::storage::atoll::log::LogWait::default(), &run)
            })
            .unwrap();
        }
        db.put(&format!("k{i:02}"), &written(i));
        ends.push((format!("k{i:02}"), written(i), db.db.log().end()));
    }
    let redo = db.db.checkpointer().superblock().redo;
    let whole = crash_io.crash(0);
    let segments = segments_to(db.db.log(), db.db.log().end());
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
            let (number, at) = place(&segments, from);
            let file = torn
                .open_file(&format!("{BASE}.log.{number:016x}"), OpenFlags::None, false)
                .unwrap();
            let len = (restart - from) as usize;
            let zeros = Arc::new(AlignedBlock::zeroed(len.next_multiple_of(BLOCK as usize)));
            let c = file
                .pwrite(at, zeros.region(0, len), Completion::new_write(|_| {}))
                .unwrap();
            torn.wait_for_completion(c).unwrap();
            let c = file.sync_data(Completion::new_sync(|_| {})).unwrap();
            torn.wait_for_completion(c).unwrap();
            let torn = Arc::new(torn);
            let (kept, end) = read_to(torn.clone(), redo);
            let before: Vec<Lsn> = starts.iter().copied().take_while(|&s| s < from).collect();
            assert_eq!(kept, before, "the zeros at {from} were not its record's");
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
    let (_, now) = Control::open(
        crate::storage::atoll::requests::counted_for_test(full.clone()),
        BASE,
    )
    .unwrap();
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

/// The log's segments from the first to the one `end` lies in: each one's number, start and
/// size, as the log's own arithmetic names them.
fn segments_to(log: &Log, end: Lsn) -> Vec<(u64, Lsn, u64)> {
    let mut segments = Vec::new();
    let mut start = 0;
    loop {
        let (number, size) = log.segment_at(start);
        segments.push((number, start, size));
        if end < start + size {
            return segments;
        }
        start += size;
    }
}

/// The segment `lsn` lies in and its offset there.
fn place(segments: &[(u64, Lsn, u64)], lsn: Lsn) -> (u64, u64) {
    assert!(
        segments
            .iter()
            .zip(0..)
            .all(|(&(number, ..), n)| number == n),
        "segments numbered out of turn: {segments:?}"
    );
    let &(number, start, _) = segments
        .iter()
        .find(|&&(_, start, size)| (start..start + size).contains(&lsn))
        .unwrap_or_else(|| panic!("{lsn} lies past the segments {segments:?}"));
    (number, lsn - start)
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

/// A record appended and not waited for, the log's end after it.
fn appended_undurable(io: &Arc<dyn IO>, log: &Arc<Log>) -> Lsn {
    use super::log::{framed_len, Header, Kind};
    let payload = [7u8; 300];
    let mut admitted = drive(io, || {
        log.admit(
            &mut crate::storage::atoll::log::LogWait::default(),
            framed_len(payload.len()),
        )
    })
    .unwrap();
    let header = Header {
        kind: Kind::new(1).unwrap(),
        flags: 0,
        txn: 0,
        prev: None,
    };
    log.append(&mut admitted, &header, &[&payload]);
    log.end()
}

/// A database closed makes its log durable to the end it had when close began, then closes its
/// jobs (DESIGN.md 22.12 step 1; mutant: no durable end waited for).
#[test]
fn a_database_closed_makes_its_log_durable_to_the_end_it_had() {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let db = try_open(io.clone(), 1).unwrap();
    let log = db.log().clone();
    let end = appended_undurable(&io, &log);
    let before = log.durable();
    let closed = db.close();
    let after = log.durable();
    drop(log);
    assert!(closed.is_ok(), "{closed:?}");
    assert!(
        before < end,
        "the record was durable before close: {before} of {end}"
    );
    assert!(
        after >= end,
        "close left the log durable to {after} of {end}"
    );
    assert_eq!(crash.in_flight(), 0, "a request outlived the close");
}

/// A log that cannot be made durable at close is returned by an explicit close, after every
/// later step: the checkpoint's task closed and every request retired (DESIGN.md 22.12; mutant:
/// the failure dropped).
#[test]
fn a_close_whose_log_fails_returns_the_failure_after_closing() {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let db = try_open(io.clone(), 1).unwrap();
    let log = db.log().clone();
    appended_undurable(&io, &log);
    crash.set_faults(Faults {
        write: 1.0,
        ..Faults::default()
    });
    let closed = db.close();
    crash.set_faults(Faults::default());
    let in_flight = crash.in_flight();
    drop(log);
    assert!(closed.is_err(), "a close whose log failed reported success");
    assert_eq!(in_flight, 0, "a request outlived the close");
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
    // A free-space page of the size class no page of the database is in: the checkpoint's own
    // edits, which would keep it as the cut found it, never touch it, and no phase before the
    // metadata phase reads it (the leave-out of untaken numbers reads every page-table leaf the
    // checkpoint writes, and waits for one held).
    let id = MetaId {
        tree: MetaTree::Space,
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
    ("buffer/mod.rs", "cut_run", "Completion::new_yield", 2, "change: an outgoing frame freed or reused while read, at the walk and at the late look"),
    ("buffer/mod.rs", "claim_noting", "look_again", 3, "change: work of the claim's own started, a latch it found changed, a horizon reached"),
    ("buffer/mod.rs", "free_page", "Completion::new_yield", 1, "change: its own write landed (settle_writes)"),
    ("buffer/mod.rs", "look_again", "Completion::new_yield", 1, "the helper itself"),
    ("buffer/mod.rs", "wait_for_resident_latch", "Completion::new_yield", 3, "change: the latch free at the look, the frame no longer resident, the hold changed before the mark"),
    ("buffer/shortage.rs", "gate_closed", "Completion::new_yield", 1, "held: the gate's reopening, waited at the operation's top (wait_for_gate)"),
    ("buffer/shortage.rs", "run", "Completion::new_yield", 1, "change: the gate found open after the wait or the cut stepped"),
    ("buffer/shortage.rs", "short", "Completion::new_yield", 1, "held: memory, reserved at the operation's top (the shortage queue)"),
    ("checkpoint.rs", "step_full", "Completion::new_yield", 1, "change: a step of its own done and more of the full checkpoint to do"),
    ("checkpoint.rs", "claim_full", "Completion::new_yield", 2, "change: a claimed step done and more of the full checkpoint to do, or the task over since the writer looked"),
    ("gc/list.rs", "cut_back", "Completion::new_yield", 1, "change: the list's last page written, moved or evicted while read"),
    ("gc/list.rs", "give_back", "Completion::new_yield", 1, "change: the list's first page moved or evicted while read"),
    ("gc/list.rs", "look", "Completion::new_yield", 1, "change: a list page written by an append past the end, moved or evicted while read"),
    ("log/mod.rs", "run_preparer", "Completion::new_yield", 1, "change: a step of the preparer done and more of it to do now"),
    ("meta.rs", "join", "Completion::new_yield", 1, "change: a load's phase moved before the wait was registered"),
    ("meta.rs", "wait_while_held", "Completion::new_yield", 2, "change: the hold let go or changed before the mark"),
    ("meta.rs", "write_for", "Completion::new_yield", 3, "change: the writing cell changed, a validation failed, a write put first by another"),
    ("store.rs", "claim_slot", "Completion::new_yield", 2, "change: a kept claim's poll lost its slot to another claim or grew the free space, its walk's place kept, and the search to go on"),
    ("store.rs", "walk", "Completion::new_yield", 2, "change: a node or leaf of a kept walk scanned with nothing takeable past its place, the place moved past it, and more of the walk to do"),
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
/// answered by a turn after it: no request made during a turn is left for nobody (DESIGN.md 22.1,
/// `Solo`). Asked during each turn toward the intent record's durability, so during the last,
/// after which the checkpoint takes no turn of its own: the log's end is durable by then. Each of
/// those units runs with the database's IO thread held before its idle, so no turn but the
/// checkpoint's is taken in it, and the checkpoint, the turn's holder, owes a turn when the unit
/// ends (mutant: the checkpoint's owed turn dropped, which the IO thread's later turns would
/// otherwise hide, its unit rung by the intent write's retirement). The turns after the last ask
/// are the log's own count of them, whatever thread took them: once let go, the IO thread takes
/// the owed turn as often as the checkpoint's next unit does. Real threads only: it holds the IO
/// thread at a pause no model build makes.
#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_flush_asked_during_the_checkpoints_turn_is_answered() {
    use super::log::{ASKED_AT_TURN, ASK_DURING_TURN};
    let crash = Arc::new(CrashIO::new(7));
    let io: Arc<dyn IO> = crash.clone();
    let db = Db::create(io.clone());
    for i in 0..50 {
        db.put(&format!("k{i:05}"), &value(i * 5 + 1));
    }
    let cp = db.db.checkpointer();
    let txns = db.db.transactions();
    let log = db.db.log();
    let ticket = cp.request_full();
    let mut asked = 0;
    let mut unowed = 0;
    let mut reached = true;
    while cp.fulls_done() < ticket.ticket() {
        // The next turn the checkpoint takes toward its intent's durability is asked for again
        // while it runs, the IO thread held until the unit has ended.
        let armed = cp.next_turn_for_intent();
        let held = if armed {
            let held =
                crate::storage::atoll::testing::io_thread_held_before_idle(log.requests_for_test());
            reached &= held.is_some();
            ASK_DURING_TURN.with(|a| a.set(true));
            held
        } else {
            None
        };
        let unit = cp.one_unit(txns).unwrap();
        if armed && !ASK_DURING_TURN.with(|a| a.replace(false)) {
            asked += 1;
            if !cp.owes_a_turn() {
                unowed += 1;
            }
        }
        drop(held);
        match unit {
            Some(c) => io.wait(&c),
            // The commit that asked waits for its own record, stepping this backend as it does.
            None if armed => io.step().unwrap(),
            None => {}
        }
    }
    // Read with the database's IO thread held before its idle, owing nothing and finding no bit:
    // a turn its unit took has ended, and been counted, by then.
    let held = crate::storage::atoll::testing::io_thread_held_before_idle(log.requests_for_test());
    reached &= held.is_some();
    let left = log.flush_asked();
    let turns_after = log.turns_taken() - ASKED_AT_TURN.with(|a| a.get());
    drop(held);
    // Asserted once the database is closed: a panic through it ends the process.
    drop(db);
    assert!(reached, "the IO thread never reached its pause before idle");
    assert!(
        asked > 0,
        "no turn toward the intent's durability was asked for again"
    );
    assert_eq!(
        unowed, 0,
        "the checkpoint did not owe a turn for the flush asked for during its own"
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

/// A recovery whose replay window names one metadata page in many intent records, as checkpoints
/// cut short one after another leave it, pins that page once: one frame a distinct page, at most
/// the database's metadata pages (DESIGN.md 22.18, *What bounds the queue*; mutant: every record's
/// ids kept, duplicates included). The window holds the last checkpoint's own intent record too,
/// so the pages pinned are those it names and the two these records name, each once.
#[test]
fn a_recovery_pins_a_page_many_intent_records_name_once() {
    use super::checkpoint::{intent_payload, META_INTENT};
    use super::log::{framed_len, Header, LogWait};
    use super::meta::{MetaId, Tree as MetaTree};
    use super::recovery::INTENTS_PINNED;
    use std::collections::BTreeSet;
    let crash_io = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash_io.clone();
    let db = Db::create(io.clone());
    let root = db.tree.root();
    for i in 0..20 {
        db.put(&format!("k{i:02}"), &value(i));
    }
    // The records follow the last checkpoint, in the replay window; nothing makes another due.
    db.checkpoint();
    // A page the database's class uses, named by every record, and one of a class no page of the
    // database is in, named by every other record.
    let used = MetaId {
        tree: MetaTree::Map,
        class: class(0),
        level: 0,
        index: 0,
    };
    let unused = MetaId {
        class: class(11),
        ..used
    };
    let log = db.db.log().clone();
    let records = 8u64;
    for n in 0..records {
        let ids: BTreeSet<MetaId> = if n % 2 == 0 {
            [used].into()
        } else {
            [used, unused].into()
        };
        let payload = intent_payload(1_000 + n, &ids);
        let mut admitted = drive(&io, || {
            log.admit(&mut LogWait::default(), framed_len(payload.len()))
        })
        .unwrap();
        let header = Header {
            kind: META_INTENT,
            flags: 0,
            txn: 0,
            prev: None,
        };
        log.append_own(&mut admitted, &header, &[&payload]);
    }
    let mut wait = LogWait::default();
    let durable = drive(&io, || log.wait_durable(&mut wait, log.end())).map_err(|e| e.to_string());
    drop(wait);
    let image = crash_io.crash(0);
    drop(log);
    drop(db);
    INTENTS_PINNED.with(|p| p.borrow_mut().clear());
    let recovered = Db::recover(Arc::new(image), root, 1);
    let pinned = INTENTS_PINNED.with(|p| std::mem::take(&mut *p.borrow_mut()));
    let distinct: BTreeSet<MetaId> = pinned.iter().copied().collect();
    let named = |id| pinned.iter().filter(|&&p| p == id).count();
    let state = recovered
        .as_ref()
        .map(|db| db.state(&["k00".to_string(), "k19".to_string()]))
        .map_err(|e| e.to_string());
    drop(recovered);
    assert_eq!(durable, Ok(()));
    assert_eq!(
        state,
        Ok(BTreeMap::from([
            ("k00".to_string(), Some(value(0))),
            ("k19".to_string(), Some(value(19))),
        ]))
    );
    assert_eq!(
        (named(used), named(unused)),
        (1, 1),
        "{records} intent records naming 2 pages: {} and {} frames pinned for them",
        named(used),
        named(unused)
    );
    assert_eq!(
        pinned.len(),
        distinct.len(),
        "{} frames pinned for {} distinct pages: {pinned:?}",
        pinned.len(),
        distinct.len()
    );
}

/// A new database in the default configuration writes and allocates its first log segment, 1 MiB,
/// and the one prepared ahead, 2 MiB, not two of the largest, 256 MiB each (DISK-MIN1); and
/// opened again it keeps the sizes its superblock records, whatever the configuration given
/// says. On files of its own, so the lengths read are what the file system allocated.
#[test]
fn a_new_database_writes_small_first_log_segments() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("db").to_str().unwrap().to_string();
    let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let open = |config: AtollConfig| {
        let opening = Opening::new(io.clone(), &base, config, None)?;
        opening.work()?;
        opening.finish()
    };
    let default = super::sql::mode::config_for(None);
    assert_eq!(
        (default.log.first_segment_bytes, default.log.segment_bytes),
        (1 << 20, 256 << 20),
        "the default configuration's first and largest log segment sizes"
    );
    let db = open(default.clone()).unwrap();
    for _ in 0..10_000 {
        if db.log().run_preparer().is_none() {
            break;
        }
    }
    drop(db);
    let sizes: Vec<(u64, u64)> = (0..4)
        .filter_map(|n| {
            std::fs::metadata(format!("{base}.log.{n:016x}"))
                .ok()
                .map(|m| (n, m.len()))
        })
        .collect();
    assert_eq!(
        sizes,
        vec![(0, 1 << 20), (1, 2 << 20)],
        "the log segments a new database made"
    );
    let mut other = default;
    other.log.first_segment_bytes = 256 << 20;
    let reopened = open(other).unwrap();
    let first = reopened.log().segment_at(0);
    drop(reopened);
    assert_eq!(
        first,
        (0, 1 << 20),
        "the reopened database took the configuration's sizes over its superblock's"
    );
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
mod retired;
