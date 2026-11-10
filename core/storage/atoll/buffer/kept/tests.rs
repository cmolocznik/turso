//! The room for kept copies (Clay's ruling of 2026-11-05), on one connection with the checkpoint
//! held after its cut: a change past the room lets go of what it holds and steps the checkpoint
//! itself, whose images give the room back; the copies never pass the limit, a change that
//! asked for none fails the invariant check, and what the checkpoint writes is the tree as its
//! cut found it.

use crate::io::crash::{CrashIO, Faults};
use crate::io::Completion;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::node::MAX_CLASS;
use crate::storage::atoll::btree::{
    Check, Drain, Drained, Lookup, Mode, Put, Remove, Tree, HOLD_SHRINK, MERGES_REFUSED,
    SHRINKS_HELD, SHRINKS_REFUSED, SPLITS_REFUSED,
};
use crate::storage::atoll::buffer::{
    BufferConfig, BufferPool, LentUse, FRAME_SHORTS, MIN_BUDGET, MIN_DB_BUDGET, WRITEBACK_BYTES,
};
use crate::storage::atoll::checkpoint::{Checkpointing, WRITER_STEPS, WRITE_BYTES};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::{ring_bytes, LogConfig};
use crate::storage::atoll::meta::MetaRef;
use crate::storage::atoll::page::{PageId, SizeClass, MAX_PAGE_SIZE};
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::{PageStore, StoreConfig};
use crate::storage::atoll::testing::drive;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::BTreeMap;

const BASE: &str = "/db/kept";

/// Frames for every tree below with their copies, so no page leaves its frame unless a test
/// sends it: its eighth is under one page of the largest class, so the room is that page, 8 MiB.
const BUDGET: usize = 64 << 20;

/// Entries of the trees: 8-byte keys, values of 1000 bytes, about 60 to a 64 KiB node.
const VALUE: usize = 1000;
/// Keys put first, every other number: 200 full leaves, more than the room holds copies of.
const KEYS: u64 = 12_000;
/// A change every this many keys leaves every leaf owed an image again: two to a leaf.
const STRIDE: usize = 30;

fn config() -> AtollConfig {
    AtollConfig {
        log: LogConfig {
            segment_bytes: 1 << 20,
            buffer_bytes: 256 << 10,
            chunk_bytes: 16 << 10,
            writes_in_flight: 3,
            direct: true,
        },
        store: StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        buffer: BufferConfig {
            budget: BUDGET,
            writeback_bytes: WRITEBACK_BYTES,
            kept_bytes: None,
        },
        log_budget: Some(512 << 10),
        checkpoint_write_bytes: None,
        checkpointing: crate::storage::atoll::checkpoint::Checkpointing::Writers,
        txn: Default::default(),
    }
}

/// The largest node class, 64 KiB: the room holds 128 of its copies.
fn node_class() -> SizeClass {
    SizeClass::new(MAX_CLASS).unwrap()
}

fn key(k: u64) -> [u8; 8] {
    k.to_be_bytes()
}

fn value(k: u64, round: u8) -> Vec<u8> {
    let mut v = vec![round; VALUE];
    v[..8].copy_from_slice(&k.to_le_bytes());
    v
}

/// Steps `op` to its end as `drive` does, failing rather than hanging: a change waiting on
/// what nothing advances would wait forever. Each return images a page, lands a write, ends the
/// cut or yields to the latch a test lets go at the first refusal, so a million is past any of
/// these tests' work. A failed IO the change waited on is its own failure, as the VDBE takes it.
fn within<T>(io: &Arc<dyn IO>, op: impl FnMut() -> Result<IOResult<T>>) -> T {
    within_returns(io, 1 << 20, op)
}

/// [`within`], failing after `returns` returns.
fn within_returns<T>(
    io: &Arc<dyn IO>,
    returns: usize,
    op: impl FnMut() -> Result<IOResult<T>>,
) -> T {
    within_waiting(io, returns, || {}, op)
}

/// [`within_returns`], calling `before_wait` at each return before it waits: where a test lets go
/// of what a wait on another thread needs.
fn within_waiting<T>(
    io: &Arc<dyn IO>,
    returns: usize,
    mut before_wait: impl FnMut(),
    mut op: impl FnMut() -> Result<IOResult<T>>,
) -> T {
    for _ in 0..returns {
        let out = op().unwrap();
        if let IOResult::IO(_) = &out {
            before_wait();
        }
        match out {
            IOResult::Done(v) => return v,
            IOResult::IO(IOCompletions(c)) if c.is_remote_wait() => remote(&c),
            IOResult::IO(IOCompletions(c)) => {
                // Every IO these tests start ends within that many steps of the IO: one each for
                // the checkpoint's 32 writes and the log's 3. A wait still unfinished past it
                // waits on nothing in flight.
                let mut steps = 0;
                while !c.finished() {
                    io.step().unwrap();
                    steps += 1;
                    assert!(steps <= 64, "a wait no IO in flight ends");
                }
                if let Some(e) = c.get_error() {
                    panic!("the change met a failed IO: {e}");
                }
            }
        }
    }
    panic!("no progress in {returns} returns");
}

/// Waits for `c`, which another thread finishes (the checkpointing thread), a minute at most:
/// what it waits for is the thread's images of a few pages in memory, far shorter.
fn remote(c: &Completion) {
    let began = std::time::Instant::now();
    while !c.finished() {
        assert!(
            began.elapsed() < std::time::Duration::from_secs(60),
            "a wait the checkpointing thread never ended"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    if let Some(e) = c.get_error() {
        panic!("the change met a failed IO: {e}");
    }
}

/// Runs `test` on a thread of its own, failing if it is not done within a minute: a change that
/// retries within one call, never returning, is caught only so.
fn within_a_minute(test: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    let runner = std::thread::spawn(move || {
        test();
        let _ = done.send(());
    });
    match finished.recv_timeout(std::time::Duration::from_secs(60)) {
        Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => runner.join().unwrap(),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!("not done within a minute"),
    }
}

struct Db {
    io: Arc<dyn IO>,
    crash: Option<Arc<CrashIO>>,
    db: Atoll,
}

impl Db {
    fn memory() -> Self {
        Self::on(Arc::new(MemoryIO::new()), None)
    }

    /// On IO that completes only as it is stepped: writes stay in flight.
    fn crash() -> Self {
        let crash = Arc::new(CrashIO::new(0));
        Self::on(crash.clone(), Some(crash))
    }

    fn on(io: Arc<dyn IO>, crash: Option<Arc<CrashIO>>) -> Self {
        Self::with(io, crash, config())
    }

    /// With a checkpointing thread stepping the checkpoints, on IO that completes only as it is
    /// stepped.
    fn threaded() -> Self {
        let crash = Arc::new(CrashIO::new(0));
        let config = AtollConfig {
            checkpointing: Checkpointing::Thread,
            ..config()
        };
        Self::with(crash.clone(), Some(crash), config)
    }

    fn with(io: Arc<dyn IO>, crash: Option<Arc<CrashIO>>, config: AtollConfig) -> Self {
        let db = Atoll::open(io.clone(), BASE, config).unwrap();
        Self { io, crash, db }
    }

    fn reopen(self) -> Self {
        let Self { io, crash, db } = self;
        drop(db);
        Self::on(io, crash)
    }

    /// Waits, a minute at most, for the checkpointing thread to reach a cut.
    fn cut_by_the_thread(&self) {
        let began = std::time::Instant::now();
        while self.db.checkpointer().cut_taken().is_none() {
            assert!(
                began.elapsed() < std::time::Duration::from_secs(60),
                "the checkpointing thread took no cut"
            );
            std::thread::yield_now();
        }
    }

    fn pool(&self) -> &Arc<BufferPool> {
        self.db.pool()
    }

    fn store(&self) -> &Arc<PageStore> {
        self.db.store()
    }

    fn create(&self, class: SizeClass) -> Tree {
        let root = drive(&self.io, || Tree::create(self.pool(), class)).unwrap();
        self.tree(root)
    }

    fn tree(&self, root: PageId) -> Tree {
        Tree::open(
            self.io.clone(),
            self.pool().clone(),
            self.store().clone(),
            root,
        )
    }

    fn put(&self, tree: &Tree, k: u64, v: Vec<u8>) {
        let mut put = Put::new(&key(k), v, Mode::Upsert);
        within(&self.io, || tree.put(&mut put));
    }

    fn get(&self, tree: &Tree, k: u64) -> Option<Vec<u8>> {
        let mut lookup = Lookup::new(&key(k));
        drive(&self.io, || tree.lookup(&mut lookup)).unwrap()
    }

    fn pages(&self, tree: &Tree) -> Vec<PageId> {
        let mut check = Check::default();
        drive(&self.io, || tree.check(&mut check)).unwrap().pages
    }

    /// A full checkpoint, done.
    fn checkpoint(&self) {
        let cp = self.db.checkpointer();
        let ticket = cp.request_full();
        self.finish(ticket);
    }

    fn finish(&self, ticket: u64) {
        let cp = self.db.checkpointer();
        within(&self.io, || cp.step_full(ticket, self.db.transactions()));
    }

    /// The page-table leaf of `orphan`, a page handed to the checkpointer: latched, it holds the
    /// next checkpoint after its cut, in its first edit of the orphan.
    fn orphan_leaf(&self, orphan: PageId) -> MetaRef {
        self.store().orphaned(orphan);
        drive(&self.io, || self.store().leaf(orphan))
            .unwrap()
            .expect("the orphan's page-table leaf")
    }

    /// Begins a full checkpoint and steps it to its cut, which the latch held keeps it at.
    fn take_cut(&self) -> u64 {
        let cp = self.db.checkpointer();
        let ticket = cp.request_full();
        assert!(
            matches!(
                cp.step_full(ticket, self.db.transactions()).unwrap(),
                IOResult::IO(_)
            ),
            "the checkpoint went on under a held latch"
        );
        assert!(cp.cut_taken().is_some(), "the hold came before the cut");
        ticket
    }

    /// Changes leaves of `tree` from its lowest key up, every `STRIDE`th key, until the room
    /// holds no other copy: the leaves past those changed have none.
    fn fill_room(&self, tree: &Tree) {
        let page = node_class().size();
        for i in (0..KEYS).step_by(STRIDE) {
            let stats = self.pool().kept_stats();
            if stats.used + page > stats.limit {
                return;
            }
            self.put(tree, 2 * i, value(2 * i, 3));
        }
        panic!("every leaf changed, and room left");
    }

    /// A tree of 64 KiB nodes holding the even keys up to `2 * KEYS` in full leaves, in a
    /// checkpoint, then every leaf changed in place: owed an image again, as a cut finds it. The
    /// keys' values, as the cut is to find them.
    fn owed_tree(&self) -> (Tree, BTreeMap<u64, Vec<u8>>) {
        let tree = self.create(node_class());
        let mut model = BTreeMap::new();
        for i in 0..KEYS {
            model.insert(2 * i, value(2 * i, 0));
            self.put(&tree, 2 * i, value(2 * i, 0));
        }
        self.checkpoint();
        for i in (0..KEYS).step_by(STRIDE) {
            model.insert(2 * i, value(2 * i, 1));
            self.put(&tree, 2 * i, value(2 * i, 1));
        }
        (tree, model)
    }

    fn assert_holds(&self, tree: &Tree, model: &BTreeMap<u64, Vec<u8>>, what: &str) {
        for (&k, v) in model {
            assert_eq!(self.get(tree, k).as_ref(), Some(v), "{what}: key {k}");
        }
        let entries = {
            let mut check = Check::default();
            drive(&self.io, || tree.check(&mut check)).unwrap().entries
        };
        assert_eq!(entries, model.len(), "{what}: the tree's entries");
    }
}

/// The room's peak within its limit and filled to it, so the room was what refused, and the
/// writers' waits measured.
fn assert_within_room(pool: &BufferPool, page: usize) {
    let stats = pool.kept_stats();
    assert!(stats.refusals > 0, "no change was refused room: {stats:?}");
    assert!(
        0 < stats.longest_wait_ns && stats.longest_wait_ns <= stats.waited_ns,
        "the writers' waits: {stats:?}"
    );
    assert!(
        stats.peak <= stats.limit && stats.peak + page > stats.limit,
        "the copies' peak: {stats:?}"
    );
    assert_eq!(
        stats.limit, MAX_PAGE_SIZE,
        "the room of a {BUDGET} byte pool"
    );
}

/// Inserts into full leaves split them, a split's first change of a leaf the cut owes keeping
/// its copy. Past the room a split is refused, steps the checkpoint itself and finishes; the
/// checkpoint writes the tree as its cut found it, which the database reopened holds.
#[test]
fn splits_past_the_room_step_the_checkpoint_themselves_and_finish_within_it() {
    let db = Db::memory();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let (tree, model) = db.owed_tree();
    let frame = db.orphan_leaf(orphan);
    let mut hold = Some(frame.try_lock().expect("the leaf is free"));
    let ticket = db.take_cut();
    let refused = SPLITS_REFUSED.with(|n| n.get());
    let imaged = db.db.checkpointer().stats().pages;
    let mut first = true;
    for i in 0..KEYS {
        let k = 2 * i + 1;
        let mut put = Put::new(&key(k), value(k, 2), Mode::Insert);
        within(&db.io, || {
            if db.pool().kept_stats().refusals > 0 {
                if let Some(h) = hold.take() {
                    h.release_unchanged();
                }
            }
            tree.put(&mut put)
        });
        // With every kept page of one size and no other writer, the first writer refused did
        // only the work that let it in: the images of as many kept copies as its request, three
        // pages at most.
        if first && db.pool().kept_stats().refusals > 0 {
            first = false;
            let done = db.db.checkpointer().stats().pages - imaged;
            assert!(
                (1..=3).contains(&done),
                "the refused writer stepped {done} images"
            );
            assert_ne!(db.pool().cutting(), 0, "the refused writer ended the cut");
        }
    }
    assert!(hold.is_none(), "no insert was refused room");
    assert!(
        SPLITS_REFUSED.with(|n| n.get()) > refused,
        "no split was refused room"
    );
    assert_within_room(db.pool(), node_class().size());
    db.finish(ticket);
    assert_eq!(
        db.pool().kept_stats().used,
        0,
        "copies left once the cut ended"
    );
    drop(hold);
    drop(frame);
    let root = tree.root();
    drop(tree);
    let db = db.reopen();
    db.assert_holds(&db.tree(root), &model, "the tree as the cut found it");
}

/// Leaves a third full, then emptied from the highest key down: each leaf the removes reach
/// after the first is reached by a merge, its right neighbour, already kept and left underfull,
/// taking it in and keeping its copy first. Past the room a merge is refused, steps the checkpoint itself and
/// finishes; the checkpoint writes the tree as its cut found it.
#[test]
fn merges_past_the_room_step_the_checkpoint_themselves_and_finish_within_it() {
    let db = Db::memory();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let (tree, mut model) = db.owed_tree();
    // Two of every three keys removed: each leaf a third full, above underfull.
    for i in (0..KEYS).filter(|i| i % 3 != 0) {
        let mut remove = Remove::new(&key(2 * i));
        assert!(within(&db.io, || tree.remove(&mut remove)));
        model.remove(&(2 * i));
    }
    let frame = db.orphan_leaf(orphan);
    let mut hold = Some(frame.try_lock().expect("the leaf is free"));
    let ticket = db.take_cut();
    let refused = MERGES_REFUSED.with(|n| n.get());
    for i in (0..KEYS).rev().filter(|i| i % 3 == 0) {
        let mut remove = Remove::new(&key(2 * i));
        let removed = within(&db.io, || {
            if db.pool().kept_stats().refusals > 0 {
                if let Some(h) = hold.take() {
                    h.release_unchanged();
                }
            }
            tree.remove(&mut remove)
        });
        assert!(removed, "key {} was there", 2 * i);
    }
    assert!(hold.is_none(), "no remove was refused room");
    assert!(
        MERGES_REFUSED.with(|n| n.get()) > refused,
        "no merge was refused room"
    );
    assert_within_room(db.pool(), node_class().size());
    db.finish(ticket);
    drop(hold);
    drop(frame);
    let root = tree.root();
    drop(tree);
    let db = db.reopen();
    db.assert_holds(&db.tree(root), &model, "the tree as the cut found it");
}

/// A root its last merge left with one child, a checkpoint's cut taken before the shrink the
/// merge goes on to: the shrink changes two pages the cut owes, the root and its child, and asks
/// for both copies first. Past the room it is refused, steps the checkpoint itself and shrinks;
/// the checkpoint writes the tree as its cut found it, the root over its one child, which the
/// database reopened holds.
#[test]
fn a_roots_shrink_past_the_room_steps_the_checkpoint_itself_and_finishes_within_it() {
    let db = Db::memory();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let (filler, _) = db.owed_tree();
    // A root over two leaves, made after the filler's checkpoint: owed at the cut.
    let tree = db.create(node_class());
    let mut model = BTreeMap::new();
    let mut n = 0;
    while db.pages(&tree).len() < 3 {
        model.insert(n, value(n, 0));
        db.put(&tree, n, value(n, 0));
        n += 1;
    }
    assert_eq!(db.pages(&tree).len(), 3, "a root over two leaves");
    // Emptied from the highest key down until its two leaves merge: the merge leaves the root
    // with one child, and the remove yields where it would shrink it.
    HOLD_SHRINK.with(|h| h.set(true));
    let held = SHRINKS_HELD.with(|c| c.get());
    let at_shrink = || SHRINKS_HELD.with(|c| c.get()) > held;
    let mut pending = None;
    for k in (0..n).rev() {
        let mut remove = Remove::new(&key(k));
        model.remove(&k);
        loop {
            match tree.remove(&mut remove).unwrap() {
                IOResult::Done(removed) => {
                    assert!(removed, "key {k} was there");
                    break;
                }
                IOResult::IO(IOCompletions(c)) => {
                    if at_shrink() {
                        break;
                    }
                    while !c.finished() {
                        db.io.step().unwrap();
                    }
                }
            }
        }
        if at_shrink() {
            pending = Some(remove);
            break;
        }
    }
    let mut remove = pending.expect("the two leaves never merged");
    assert_eq!(db.pages(&tree).len(), 2, "the root over its one child");
    let frame = db.orphan_leaf(orphan);
    let mut hold = Some(frame.try_lock().expect("the leaf is free"));
    let ticket = db.take_cut();
    db.fill_room(&filler);
    HOLD_SHRINK.with(|h| h.set(false));
    let refused = SHRINKS_REFUSED.with(|c| c.get());
    let removed = within(&db.io, || {
        if db.pool().kept_stats().refusals > 0 {
            if let Some(h) = hold.take() {
                h.release_unchanged();
            }
        }
        tree.remove(&mut remove)
    });
    assert!(removed, "the held remove's key was there");
    assert!(hold.is_none(), "no change was refused room");
    assert!(
        SHRINKS_REFUSED.with(|c| c.get()) > refused,
        "the shrink was not refused room"
    );
    assert_eq!(db.pages(&tree).len(), 1, "the root did not shrink");
    assert_within_room(db.pool(), node_class().size());
    db.finish(ticket);
    drop(hold);
    drop(frame);
    let root = tree.root();
    drop((tree, filler));
    let db = db.reopen();
    let tree = db.tree(root);
    db.assert_holds(&tree, &model, "the tree as the cut found it");
    assert_eq!(
        db.pages(&tree).len(),
        2,
        "the cut's root over its one child"
    );
}

/// A change of a page the cut owes that asked for no room first fails the invariant check: a
/// copy made then would pass the room's limit. The check fires with the page held exclusive and a
/// metadata hold alive, so the panic passes their guards and ends the process (DESIGN.md 22.11):
/// it runs in a child process.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_change_that_asked_for_no_room_fails_the_invariant_check() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::a_change_that_asked_for_no_room_fails_the_invariant_check"
        ),
        || {
            let db = Db::memory();
            let orphan = drive(&db.io, || {
                Tree::create(db.pool(), SizeClass::new(0).unwrap())
            })
            .unwrap();
            let (tree, _) = db.owed_tree();
            let frame = db.orphan_leaf(orphan);
            let _hold = frame.try_lock().expect("the leaf is free");
            db.take_cut();
            // Every leaf is owed: the first one changed fails.
            for pid in db.pages(&tree) {
                let mut page = within(&db.io, || db.pool().fix_exclusive(pid, None));
                page.page_mut();
            }
        },
    );
    let Some(ended) = ended else { return };
    assert!(
        ended.aborted,
        "the child did not abort: {:?}\n{}",
        ended.code, ended.stderr
    );
    assert!(
        ended.stderr.contains("with no room asked for its copy"),
        "{}",
        ended.stderr
    );
}

/// The copies a cut keeps are in frames the pool lends (one memory), and the room's bytes are
/// theirs; the checkpoint writes its images of node pages from lent frames too. Once the cut ends,
/// the pool lends nothing but the log's ring, which it lends for as long as the log is open.
#[test]
fn kept_copies_and_images_live_in_frames_the_pool_lends_and_go_back_once_written() {
    let db = Db::memory();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let ring = ring_bytes(&config().log);
    assert_eq!(db.pool().lent_stats().log.0, ring, "the log's ring");
    let (tree, _) = db.owed_tree();
    let frame = db.orphan_leaf(orphan);
    let hold = frame.try_lock().expect("the leaf is free");
    let ticket = db.take_cut();
    for i in (0..KEYS).step_by(STRIDE).take(20) {
        db.put(&tree, 2 * i, value(2 * i, 3));
    }
    let (kept, lent) = (db.pool().kept_stats(), db.pool().lent_stats());
    assert!(kept.used > 0, "no copy kept: {kept:?}");
    assert_eq!(lent.kept.0, kept.used, "the copies' frames: {lent:?}");
    hold.release_unchanged();
    db.finish(ticket);
    drop(frame);
    let lent = db.pool().lent_stats();
    assert_eq!(
        (lent.kept.0, lent.image.0, lent.log.0),
        (0, 0, ring),
        "lent once the cut ended: {lent:?}"
    );
    assert_eq!(
        db.pool().lent_frame_bytes(),
        ring,
        "frames still lent once the cut ended"
    );
    assert!(
        lent.image.1 > 0,
        "no image written from a lent frame: {lent:?}"
    );
}

/// A change whose copy finds no frame free without an eviction is turned down as one past the
/// room is: it lets go of its latches, takes a frame holding nothing, which may evict a page, and
/// goes on with it.
#[test]
fn a_copy_that_finds_no_free_frame_takes_one_holding_nothing_and_goes_on() {
    within_a_minute(|| {
        let db = Db::memory();
        let orphan = drive(&db.io, || {
            Tree::create(db.pool(), SizeClass::new(0).unwrap())
        })
        .unwrap();
        let (tree, _) = db.owed_tree();
        let frame = db.orphan_leaf(orphan);
        let hold = frame.try_lock().expect("the leaf is free");
        let ticket = db.take_cut();
        // Every free frame of the class lent, and the rest of the region with them.
        let mut taken = Vec::new();
        while let Some(block) = db.pool().try_lend(node_class(), LentUse::Image) {
            taken.push(block);
        }
        assert!(!taken.is_empty(), "no frame free to take");
        let shorts = FRAME_SHORTS.with(|n| n.get());
        // Done within a few returns: the change turned down, the frame it takes (an eviction's write
        // at most), and the change again. A thousand is far past that.
        let mut put = Put::new(&key(0), value(0, 3), Mode::Upsert);
        within_returns(&db.io, 1 << 10, || tree.put(&mut put));
        assert!(
            FRAME_SHORTS.with(|n| n.get()) > shorts,
            "the copy found a frame free"
        );
        assert_eq!(db.get(&tree, 0), Some(value(0, 3)), "the change went in");
        let kept = db.pool().kept_stats();
        assert!(kept.used > 0 && kept.peak <= kept.limit, "{kept:?}");
        drop(taken);
        hold.release_unchanged();
        db.finish(ticket);
        drop(frame);
    });
}

/// The room of the smallest database is an eighth of its budget: the copies take its frames, so
/// no floor of a page of the largest class takes most of a small pool.
#[test]
fn the_room_of_the_smallest_database_is_an_eighth_of_its_budget() {
    assert_eq!(super::default_limit(MIN_DB_BUDGET), MIN_DB_BUDGET / 8);
}

/// A budget whose smallest pool does not fit beside the log's ring, which the pool lends for as
/// long as the log is open, is refused when the database opens.
#[test]
fn a_budget_too_small_for_the_smallest_pool_beside_the_logs_ring_is_refused_at_open() {
    let mut small = config();
    small.buffer.budget = MIN_BUDGET;
    let err = Atoll::open(Arc::new(MemoryIO::new()), BASE, small)
        .err()
        .expect("opened");
    assert!(err.to_string().contains("log's ring"), "{err}");
}

/// With a checkpointing thread, a writer refused room does none of the checkpoint's work: it
/// lets go of what it holds and waits for the room the thread frees, its kept copies imaged
/// first; and the thread writes the tree as its cut found it, which the database reopened holds.
#[test]
fn with_a_checkpointing_thread_a_writer_refused_room_waits_for_the_room_it_frees() {
    let db = Db::threaded();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let (tree, model) = db.owed_tree();
    let frame = db.orphan_leaf(orphan);
    let mut hold = Some(frame.try_lock().expect("the leaf is free"));
    let ticket = db.db.checkpointer().request_full();
    db.cut_by_the_thread();
    let steps = WRITER_STEPS.with(|n| n.get());
    for i in 0..KEYS {
        let k = 2 * i + 1;
        let mut put = Put::new(&key(k), value(k, 2), Mode::Insert);
        // The leaf the thread waits on let go once a writer waits for room the thread frees.
        let let_go = || {
            if db.pool().kept_stats().refusals > 0 {
                if let Some(h) = hold.take() {
                    h.release_unchanged();
                }
            }
        };
        within_waiting(&db.io, 1 << 20, let_go, || tree.put(&mut put));
    }
    assert!(hold.is_none(), "no insert was refused room");
    assert_eq!(
        WRITER_STEPS.with(|n| n.get()),
        steps,
        "a writer stepped the checkpoint"
    );
    assert!(
        db.pool().blocking_room_waits() > 0,
        "a refused writer stepped its IO waiting for room"
    );
    assert_within_room(db.pool(), node_class().size());
    db.finish(ticket);
    drop(hold);
    drop(frame);
    let root = tree.root();
    drop(tree);
    let db = db.reopen();
    db.assert_holds(&db.tree(root), &model, "the tree as the cut found it");
}

/// The checkpointing thread, finding the state held, waits as its contender, and the hold let go
/// wakes it: a full checkpoint asked for while the state was held is done once it is let go, with
/// nothing else waking the thread meanwhile (a transaction's end waiting for it would). A thread
/// that parks without registering, or a hold let go with no wake, leaves it parked with the
/// checkpoint due, as the threaded test above met when its probe for the cut took the state ("the
/// checkpointing thread took no cut").
#[test]
fn a_hold_let_go_wakes_the_checkpointing_thread_that_found_the_state_held() {
    let db = Db::threaded();
    let cp = db.db.checkpointer();
    let held = cp.hold().expect("the state free");
    let ticket = cp.request_full();
    let began = std::time::Instant::now();
    while !cp.driver_contends() {
        assert!(
            began.elapsed() < std::time::Duration::from_secs(60),
            "the checkpointing thread never waited for the state"
        );
        std::thread::yield_now();
    }
    drop(held);
    let began = std::time::Instant::now();
    while cp.fulls_done() < ticket {
        assert!(
            began.elapsed() < std::time::Duration::from_secs(60),
            "the checkpointing thread was left parked after the state was let go"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// The ends of transactions waiting for a full checkpoint are woken with no lock held: a waiter
/// whose waker waits for the next full checkpoint again, from inside its wake on the
/// checkpointing thread, is not blocked by the lock the wake would otherwise still hold.
#[test]
fn a_waiter_woken_for_a_full_checkpoint_may_wait_again_from_its_wake() {
    use std::sync::mpsc;
    use std::task::{Wake, Waker};
    struct Again {
        cp: Arc<crate::storage::atoll::checkpoint::Checkpointer>,
        ticket: u64,
        told: crate::storage::atoll::locks::Mutex<mpsc::Sender<bool>>,
    }
    impl Wake for Again {
        fn wake(self: std::sync::Arc<Self>) {
            let waited = self.cp.wait_full_on_the_thread(self.ticket).is_ok();
            let _ = self.told.lock().send(waited);
        }
    }
    let db = Db::threaded();
    let cp = db.db.checkpointer_arc().clone();
    let held = cp.hold().expect("the state free");
    let ticket = cp.request_full();
    let IOResult::IO(IOCompletions(c)) = cp.step_full(ticket, db.db.transactions()).unwrap() else {
        panic!("a full checkpoint done while its state was held");
    };
    let (told, woke) = mpsc::channel();
    c.set_waker(&Waker::from(std::sync::Arc::new(Again {
        cp: cp.clone(),
        ticket: ticket + 1,
        told: crate::storage::atoll::locks::Mutex::new(told),
    })));
    drop(held);
    match woke.recv_timeout(std::time::Duration::from_secs(60)) {
        Ok(waited) => assert!(waited, "the wait again failed"),
        Err(_) => {
            // The checkpointing thread is stuck in the wake: closing would wait for it forever.
            std::mem::forget(db);
            panic!("a waker waiting again from its wake was blocked");
        }
    }
    drop(cp);
}

/// The ends of transactions waiting for a full checkpoint are woken once the checkpoint's state
/// is let go: a waker that takes the state, from inside its wake on the checkpointing thread,
/// finds it free.
#[test]
fn a_waiter_woken_for_a_full_checkpoint_may_take_the_checkpoint_state_from_its_wake() {
    use std::sync::mpsc;
    use std::task::{Wake, Waker};
    struct Takes {
        cp: Arc<crate::storage::atoll::checkpoint::Checkpointer>,
        told: crate::storage::atoll::locks::Mutex<mpsc::Sender<bool>>,
    }
    impl Wake for Takes {
        fn wake(self: std::sync::Arc<Self>) {
            let took = self.cp.hold().is_some();
            let _ = self.told.lock().send(took);
        }
    }
    let db = Db::threaded();
    let cp = db.db.checkpointer_arc().clone();
    let held = cp.hold().expect("the state free");
    let ticket = cp.request_full();
    let IOResult::IO(IOCompletions(c)) = cp.step_full(ticket, db.db.transactions()).unwrap() else {
        panic!("a full checkpoint done while its state was held");
    };
    let (told, woke) = mpsc::channel();
    c.set_waker(&Waker::from(std::sync::Arc::new(Takes {
        cp: cp.clone(),
        told: crate::storage::atoll::locks::Mutex::new(told),
    })));
    drop(held);
    // The database closes before any assertion: a panic while it is open would end the process at
    // its close, an entry made while unwinding, rather than fail this test.
    let Ok(took) = woke.recv_timeout(std::time::Duration::from_secs(60)) else {
        // The checkpointing thread is stuck in the wake: closing would wait for it forever.
        std::mem::forget((cp, db));
        panic!("the waiter was never woken");
    };
    drop((cp, db));
    assert!(
        took,
        "the waker found the checkpoint's state held: it was woken under the hold"
    );
}

/// A checkpoint failing on the checkpointing thread is told to a transaction's end waiting on it
/// for a full one, which then fails rather than waits for good.
#[test]
fn a_checkpoint_failing_on_the_thread_is_told_to_whoever_waits_for_it() {
    let db = Db::threaded();
    let crash = db.crash.clone().unwrap();
    let (_tree, _) = db.owed_tree();
    // The writes of the tree's pages fail; the log's never do.
    crash.set_faults_on(Some(&format!("{BASE}.c{}.0", node_class().index())));
    crash.set_faults(Faults {
        write: 1.0,
        ..Faults::default()
    });
    let cp = db.db.checkpointer();
    let ticket = cp.request_full();
    let began = std::time::Instant::now();
    let failed = loop {
        match cp.step_full(ticket, db.db.transactions()) {
            Err(e) => break e,
            Ok(IOResult::Done(())) => {
                panic!("a full checkpoint done with every image write failing")
            }
            Ok(IOResult::IO(IOCompletions(c))) => {
                assert!(
                    c.is_remote_wait(),
                    "a writer given IO of the checkpoint's to step"
                );
                remote(&c);
            }
        }
        assert!(
            began.elapsed() < std::time::Duration::from_secs(60),
            "no failure told"
        );
    };
    assert!(failed.to_string().contains("failed"), "{failed}");
}

/// A closed database ends and joins its checkpointing thread: the thread holds nothing of it.
#[test]
fn a_closed_database_ends_and_joins_its_checkpointing_thread() {
    let db = Db::threaded();
    let held = Arc::downgrade(db.db.checkpointer_arc());
    drop(db);
    assert!(
        held.upgrade().is_none(),
        "the checkpointing thread holds the checkpointer"
    );
}

/// Every page of the tree written to its swap copy and evicted after the cut, still owed: read
/// back, each is pending again, and a change keeps it within the room as it would have in its
/// frame. A descent through an inner node it reads back mends the node's stale hints, which no
/// copy is made for.
#[test]
fn pages_evicted_after_the_cut_are_kept_within_the_room_when_read_back_and_changed() {
    let db = Db::memory();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let (tree, mut model) = db.owed_tree();
    // Inner nodes owed too: inserts split some leaves.
    for i in 0..KEYS / 4 {
        let k = 2 * i + 1;
        model.insert(k, value(k, 1));
        db.put(&tree, k, value(k, 1));
    }
    let frame = db.orphan_leaf(orphan);
    let mut hold = Some(frame.try_lock().expect("the leaf is free"));
    let ticket = db.take_cut();
    let pool = db.pool();
    let mut sweep = pool.sweep();
    drive(&db.io, || sweep.step(pool)).unwrap();
    let pages = db.pages(&tree);
    let evicted = pages.iter().filter(|&&p| pool.evict_clean(p)).count();
    assert!(
        evicted * node_class().size() > pool.kept_stats().limit,
        "{evicted} of {} pages evicted, fewer than the room holds",
        pages.len()
    );
    for i in 0..KEYS {
        let mut put = Put::new(&key(2 * i), value(2 * i, 2), Mode::Upsert);
        within(&db.io, || {
            if db.pool().kept_stats().refusals > 0 {
                if let Some(h) = hold.take() {
                    h.release_unchanged();
                }
            }
            tree.put(&mut put)
        });
    }
    assert!(hold.is_none(), "no change was refused room");
    assert_within_room(db.pool(), node_class().size());
    db.finish(ticket);
    drop(hold);
    drop(frame);
    let root = tree.root();
    drop(tree);
    let db = db.reopen();
    db.assert_holds(&db.tree(root), &model, "the tree as the cut found it");
}

/// A root growing, a full leaf with no parent, and a drain unlinking leaves from their parent ask
/// for room for the pages they change before their first edit, as every change does: during a cut
/// with room to spare, their copies are asked for, or the invariant check fails them.
#[test]
fn a_roots_growth_and_a_drains_steps_ask_for_room_before_their_first_edit() {
    let db = Db::memory();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    // How many entries a lone root takes before it grows.
    let probe = db.create(node_class());
    let mut fits = 0;
    while db.pages(&probe).len() == 1 {
        fits += 1;
        db.put(&probe, fits, value(fits, 0));
    }
    let root = db.create(node_class());
    for k in 1..fits {
        db.put(&root, k, value(k, 0));
    }
    // A tree of several leaves, on a list to drain it from.
    let list = db.create(SizeClass::new(0).unwrap());
    let drained = db.create(node_class());
    for k in 0..4 * fits {
        db.put(&drained, 2 * k, value(2 * k, 0));
    }
    let mut put = Put::new(
        &drained.root().raw().to_be_bytes(),
        vec![7; 8],
        Mode::Insert,
    );
    within(&db.io, || list.put(&mut put));
    db.checkpoint();
    // Owed at the cut: the root in place, the drained tree's root by a split under it.
    db.put(&root, 1, value(1, 1));
    for k in 0..fits {
        db.put(&drained, 2 * k + 1, value(2 * k + 1, 1));
    }
    let frame = db.orphan_leaf(orphan);
    let hold = frame.try_lock().expect("the leaf is free");
    let ticket = db.take_cut();
    db.put(&root, fits, value(fits, 2));
    assert!(db.pages(&root).len() > 1, "the root did not grow");
    let mut op = Drain::new(&drained.root().raw().to_be_bytes());
    let mut steps = 0;
    while within(&db.io, || drained.drain(&list, &mut op)) != Drained::Gone {
        steps += 1;
    }
    assert!(steps > 1, "the drain unlinked no leaf");
    let stats = db.pool().kept_stats();
    assert!(stats.peak > 0 && stats.refusals == 0, "{stats:?}");
    hold.release_unchanged();
    db.finish(ticket);
    drop(frame);
}

/// The checkpoint a refused writer steps fails, its image writes failing: it is abandoned, its
/// cut ended and its copies given back, and the writer finishes, as commits go on after a failed
/// checkpoint. The failure is told to whoever steps checkpoints next.
#[test]
fn a_checkpoint_failing_under_a_refused_writer_gives_the_room_back_and_the_writer_finishes() {
    let db = Db::crash();
    let crash = db.crash.clone().unwrap();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let (tree, mut model) = db.owed_tree();
    let frame = db.orphan_leaf(orphan);
    let mut hold = Some(frame.try_lock().expect("the leaf is free"));
    let ticket = db.take_cut();
    // The writes of the tree's pages fail; the log's never do.
    crash.set_faults_on(Some(&format!("{BASE}.c{}.0", node_class().index())));
    crash.set_faults(Faults {
        write: 1.0,
        ..Faults::default()
    });
    for i in 0..KEYS {
        let k = 2 * i + 1;
        model.insert(k, value(k, 2));
        let mut put = Put::new(&key(k), value(k, 2), Mode::Insert);
        within(&db.io, || {
            if db.pool().kept_stats().refusals > 0 {
                if let Some(h) = hold.take() {
                    h.release_unchanged();
                }
            }
            tree.put(&mut put)
        });
    }
    assert!(hold.is_none(), "no insert was refused room");
    let cp = db.db.checkpointer();
    let failed = loop {
        match cp.step_full(ticket, db.db.transactions()) {
            Err(e) => break e,
            Ok(IOResult::Done(())) => panic!("a checkpoint went on past failed writes"),
            Ok(IOResult::IO(IOCompletions(c))) => {
                while !c.finished() {
                    db.io.step().unwrap();
                }
            }
        }
    };
    crash.set_faults(Faults::default());
    crash.set_faults_on(None);
    assert!(
        failed.to_string().contains("opened again")
            || matches!(failed, LimboError::CompletionError(_)),
        "{failed}"
    );
    let stats = db.pool().kept_stats();
    assert_eq!(stats.used, 0, "copies held past the failure: {stats:?}");
    assert!(stats.peak <= stats.limit, "{stats:?}");
    assert_eq!(db.pool().cutting(), 0, "the pool's cut");
    assert!(db.store().change().is_some(), "the change gate");
    db.assert_holds(&tree, &model, "the tree as it stands");
}

/// The checkpointer's own free of an orphan its cut found owed makes no copy: the cut left the
/// page out, and no image names it.
#[test]
fn an_orphan_the_checkpoint_frees_is_kept_by_no_copy() {
    let db = Db::memory();
    let tree = db.create(SizeClass::new(0).unwrap());
    db.checkpoint();
    db.put(&tree, 1, value(1, 1));
    let orphan = tree.root();
    db.store().orphaned(orphan);
    db.checkpoint();
    assert_eq!(
        drive(&db.io, || db.store().entry(orphan)).unwrap(),
        FREE,
        "the orphan freed"
    );
    let stats = db.pool().kept_stats();
    assert_eq!(stats.peak, 0, "{stats:?}");
}

/// A page the cut owes, freed when the room holds no copy of it: handed to the checkpointer as
/// it stands, unchanged since the cut, rather than waiting; the checkpoint writes it, and the next
/// frees it.
#[test]
fn a_free_with_no_room_for_its_copy_hands_the_page_over_instead_of_waiting() {
    let db = Db::memory();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let (tree, _) = db.owed_tree();
    let freed = db.create(SizeClass::new(0).unwrap());
    db.checkpoint();
    db.put(&freed, 1, value(1, 1));
    for i in (0..KEYS).step_by(STRIDE) {
        db.put(&tree, 2 * i, value(2 * i, 2));
    }
    let frame = db.orphan_leaf(orphan);
    let hold = frame.try_lock().expect("the leaf is free");
    let ticket = db.take_cut();
    db.fill_room(&tree);
    let before = db.pool().kept_stats();
    assert!(
        before.used + SizeClass::new(0).unwrap().size() > before.limit,
        "the room has room: {before:?}"
    );
    // Bounded: a free that waited on the page-table leaf held would never end.
    within(&db.io, || db.pool().free_unreferenced(freed.root()));
    assert!(
        db.store().orphans().contains(&freed.root()),
        "the page was not handed over"
    );
    assert_eq!(db.pool().kept_stats(), before, "a copy was made");
    hold.release_unchanged();
    db.finish(ticket);
    drop(frame);
    db.checkpoint();
    assert_eq!(
        drive(&db.io, || db.store().entry(freed.root())).unwrap(),
        FREE,
        "the next checkpoint freed it"
    );
}

/// A writer refused room while someone holds the checkpointer idle waits on the room, not
/// spinning, and letting the checkpointer go wakes it to step the checkpoint itself.
#[test]
fn a_writer_waiting_for_room_behind_a_held_checkpointer_is_woken_when_it_is_let_go() {
    let db = Db::memory();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let (tree, _) = db.owed_tree();
    let frame = db.orphan_leaf(orphan);
    let hold = frame.try_lock().expect("the leaf is free");
    let ticket = db.take_cut();
    db.fill_room(&tree);
    hold.release_unchanged();
    let idle = db
        .db
        .checkpointer()
        .hold()
        .expect("no one steps the checkpointer");
    // The highest key's leaf, past every leaf the room filled with.
    let last = 2 * (KEYS - 1);
    let mut put = Put::new(&key(last), value(last, 3), Mode::Upsert);
    let wait = match tree.put(&mut put).unwrap() {
        IOResult::IO(IOCompletions(c)) => c,
        IOResult::Done(_) => panic!("a change went in past a full room"),
    };
    assert!(
        !wait.finished(),
        "the wait ended with the checkpointer held"
    );
    drop(idle);
    assert!(
        wait.finished(),
        "letting the checkpointer go woke no waiter"
    );
    within(&db.io, || tree.put(&mut put));
    assert_within_room(db.pool(), node_class().size());
    db.finish(ticket);
    drop(frame);
}

/// With a thread driving the checkpoints, a writer refused room waits for room given back and for
/// nothing else: the thread lets the state go after every unit, and a wake at each let-go would
/// wake the writer only to be refused again (mutant: every let-go wakes the room's waiters). A
/// writer coming to wait once room has been given back for what was refused starts again at once,
/// never waiting for a release that has happened (mutant: the room not looked at again once the
/// wait is taken). A thread attached that never steps stands for the driver, and the test steps
/// the checkpoint as it would, so nothing steps it behind the test's back.
#[test]
fn with_a_thread_driving_a_writer_waiting_for_room_is_woken_by_room_given_back_alone() {
    let db = Db::memory();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let (tree, _) = db.owed_tree();
    let frame = db.orphan_leaf(orphan);
    let hold = frame.try_lock().expect("the leaf is free");
    let ticket = db.take_cut();
    db.fill_room(&tree);
    hold.release_unchanged();
    let cp = db.db.checkpointer();
    let parked = std::thread::spawn(std::thread::park);
    cp.attach_driver(parked.thread().clone());
    // The highest key's leaf, past every leaf the room filled with.
    let last = 2 * (KEYS - 1);
    let mut put = Put::new(&key(last), value(last, 3), Mode::Upsert);
    let wait = match tree.put(&mut put).unwrap() {
        IOResult::IO(IOCompletions(c)) => c,
        IOResult::Done(_) => panic!("a change went in past a full room"),
    };
    drop(cp.hold().expect("no one steps the checkpointer"));
    assert!(
        !wait.finished(),
        "the state let go, no room given back, woke the writer"
    );
    let txns = db.db.transactions();
    while !db.pool().room_takes_refused() {
        if let Some(c) = cp.one_unit(txns).unwrap() {
            db.io.wait(&c);
        }
    }
    assert_ne!(
        db.pool().cutting(),
        0,
        "the cut ended before the room took the request"
    );
    assert!(wait.finished(), "room given back woke no waiter");
    assert!(
        matches!(db.pool().wait_for_room().unwrap(), IOResult::Done(())),
        "a writer came to wait for room given back already"
    );
    cp.detach_driver();
    parked.thread().unpark();
    parked.join().unwrap();
    within(&db.io, || tree.put(&mut put));
    assert_within_room(db.pool(), node_class().size());
    db.finish(ticket);
    drop(frame);
}

/// While a writer waits for room, the checkpoint images the kept copies first: with another
/// tree's owed pages ahead of them in its queue, the refused writer is let in after no more images
/// than one round of writes in flight, all of them copies.
#[test]
fn a_waiting_writer_is_let_in_once_the_checkpoint_images_kept_copies_first() {
    let db = Db::crash();
    let orphan = drive(&db.io, || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    // Made first, its pages lie ahead of the other's in the frames the cut walks.
    let (ahead, _) = db.owed_tree();
    let (tree, _) = {
        let tree = db.create(node_class());
        for i in 0..KEYS {
            db.put(&tree, 2 * i, value(2 * i, 0));
        }
        db.checkpoint();
        for i in (0..KEYS).step_by(STRIDE) {
            db.put(&ahead, 2 * i, value(2 * i, 1));
            db.put(&tree, 2 * i, value(2 * i, 1));
        }
        (tree, ())
    };
    let frame = db.orphan_leaf(orphan);
    let mut hold = Some(frame.try_lock().expect("the leaf is free"));
    let ticket = db.take_cut();
    let cp = db.db.checkpointer();
    let imaged = cp.stats().pages;
    let mut admitted = None;
    for i in 0..KEYS {
        let k = 2 * i + 1;
        let first = db.pool().kept_stats().refusals == 0;
        let mut put = Put::new(&key(k), value(k, 2), Mode::Insert);
        within(&db.io, || {
            if db.pool().kept_stats().refusals > 0 {
                if let Some(h) = hold.take() {
                    h.release_unchanged();
                }
            }
            tree.put(&mut put)
        });
        if first && db.pool().kept_stats().refusals > 0 {
            admitted = Some(cp.stats().pages - imaged);
            break;
        }
    }
    let admitted = admitted.expect("no insert was refused room");
    assert!(
        (1..=32).contains(&admitted),
        "the refused writer was let in after {admitted} images"
    );
    drop(hold);
    db.finish(ticket);
    drop(frame);
}

/// Value pages of 4 MiB owed at a cut: their image writes in flight stay within the write
/// bytes, one page of the largest class, so two go at once where 32 small ones would; and the
/// checkpoint's working memory within two such pages and those bytes.
#[test]
fn image_writes_in_flight_stay_within_their_bytes_for_large_pages() {
    let db = Db::crash();
    let tree = db.create(SizeClass::new(0).unwrap());
    // Out of line, each in a page of its own of 4 MiB.
    let large = 3 << 20;
    for k in 0..6 {
        db.put(&tree, k, vec![k as u8; large]);
    }
    db.checkpoint();
    let stats = db.db.checkpointer().stats();
    assert!(
        stats.flying_peak <= WRITE_BYTES && stats.flying_peak >= 2 * (4 << 20),
        "{stats:?}"
    );
    assert!(
        stats.working_peak <= 2 * MAX_PAGE_SIZE + WRITE_BYTES,
        "{stats:?}"
    );
    for k in 0..6 {
        assert_eq!(db.get(&tree, k), Some(vec![k as u8; large]));
    }
}

/// An image write that lands short fails the checkpoint: the copy it names would be torn.
#[test]
fn a_short_image_write_fails_the_checkpoint() {
    let db = Db::crash();
    let crash = db.crash.clone().unwrap();
    let tree = db.create(node_class());
    for k in 0..100 {
        db.put(&tree, k, value(k, 0));
    }
    crash.set_faults_on(Some(&format!("{BASE}.c{}.0", node_class().index())));
    crash.set_faults(Faults {
        short_write: 1.0,
        ..Faults::default()
    });
    let cp = db.db.checkpointer();
    let ticket = cp.request_full();
    let failed = loop {
        match cp.step_full(ticket, db.db.transactions()) {
            Err(e) => break e,
            Ok(IOResult::Done(())) => panic!("a checkpoint went on past a short write"),
            Ok(IOResult::IO(IOCompletions(c))) => {
                while !c.finished() {
                    db.io.step().unwrap();
                }
            }
        }
    };
    crash.set_faults(Faults::default());
    crash.set_faults_on(None);
    assert!(
        matches!(
            failed,
            LimboError::CompletionError(CompletionError::ShortWrite)
        ),
        "{failed}"
    );
}
