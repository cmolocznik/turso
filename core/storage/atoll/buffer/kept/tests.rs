//! The room for kept copies (Clay's ruling of 2026-11-05), on one connection with the checkpoint
//! held after its cut: a change past the room lets go of what it holds and steps the checkpoint
//! itself, whose images give the room back; the copies never pass the limit, a change that
//! asked for none fails the invariant check, and what the checkpoint writes is the tree as its
//! cut found it.

use crate::io::crash::{CrashIO, Faults};
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::node::MAX_CLASS;
use crate::storage::atoll::btree::{
    Check, Drain, Drained, Lookup, Mode, Put, Remove, Tree, HOLD_SHRINK, MERGES_REFUSED,
    SHRINKS_HELD, SHRINKS_REFUSED, SPLITS_REFUSED,
};
use crate::storage::atoll::buffer::{BufferConfig, BufferPool, WRITEBACK_BYTES};
use crate::storage::atoll::checkpoint::WRITE_BYTES;
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::LogConfig;
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
fn within<T>(io: &Arc<dyn IO>, mut op: impl FnMut() -> Result<IOResult<T>>) -> T {
    for _ in 0..1 << 20 {
        match op().unwrap() {
            IOResult::Done(v) => return v,
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
    panic!("no progress in a million steps");
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
        let db = Atoll::open(io.clone(), BASE, config()).unwrap();
        Self { io, crash, db }
    }

    fn reopen(self) -> Self {
        let Self { io, crash, db } = self;
        drop(db);
        Self::on(io, crash)
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
        drive(&self.io, || cp.step_full(ticket, self.db.transactions())).unwrap();
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
/// copy made then would pass the room's limit.
#[test]
#[should_panic(expected = "with no room asked for its copy")]
fn a_change_that_asked_for_no_room_fails_the_invariant_check() {
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
