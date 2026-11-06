//! The page store (audit 18): numbers and slots freed only once a checkpoint lets them go, slots
//! erased before they are taken again, and a latched metadata page making its users yield.

use super::*;
use crate::io::MemoryIO;
use crate::storage::atoll::buffer::{min_budget_beside, BufferConfig};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::{ring_bytes, LogConfig};
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::versioned::Versions;

const BASE: &str = "/db/store";

fn config() -> AtollConfig {
    let log = LogConfig {
        segment_bytes: 256 << 10,
        buffer_bytes: 64 << 10,
        chunk_bytes: 16 << 10,
        writes_in_flight: 3,
        direct: true,
    };
    AtollConfig {
        buffer: BufferConfig {
            budget: min_budget_beside(ring_bytes(&log)),
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
            kept_bytes: None,
        },
        log,
        store: StoreConfig {
            file_bytes: 64 << 10,
            meta_pages: 64,
        },
        log_budget: Some(512 << 10),
        checkpoint_write_bytes: None,
        txn: Default::default(),
    }
}

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

struct Db {
    io: Arc<dyn IO>,
    db: Atoll,
    versions: Versions,
}

impl Db {
    fn open() -> Self {
        Self::open_on(Arc::new(MemoryIO::new()))
    }

    fn open_on(io: Arc<dyn IO>) -> Self {
        let db = Atoll::open(io.clone(), BASE, config()).unwrap();
        let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), 1);
        Self { io, db, versions }
    }

    fn store(&self) -> &Arc<PageStore> {
        self.db.store()
    }

    fn checkpoint(&self) {
        let cp = self.db.checkpointer();
        let ticket = cp.request_full();
        drive(&self.io, || cp.step_full(ticket, self.db.transactions())).unwrap();
    }

    /// A new plain tree's root, a node a checkpoint images.
    fn node(&self) -> PageId {
        drive(&self.io, || {
            crate::storage::atoll::btree::Tree::create(self.db.pool(), class(0))
        })
        .unwrap()
    }

    fn tree(&self, root: PageId) -> crate::storage::atoll::btree::Tree {
        crate::storage::atoll::btree::Tree::open(
            self.io.clone(),
            self.db.pool().clone(),
            self.store().clone(),
            root,
        )
    }

    /// A change to the node `root`, a key put in its tree.
    fn change(&self, root: PageId, key: &[u8]) {
        self.put(root, key, b"v".to_vec());
    }

    fn put(&self, root: PageId, key: &[u8], value: Vec<u8>) {
        let mut put = crate::storage::atoll::btree::Put::new(
            key,
            value,
            crate::storage::atoll::btree::Mode::Upsert,
        );
        drive(&self.io, || self.tree(root).put(&mut put)).unwrap();
    }

    /// The change count a whole copy of `pid` in `slot` holds, if the slot holds one.
    fn copy_in(&self, pid: PageId, slot: u64) -> Option<u64> {
        let block = Arc::new(crate::storage::atoll::aligned::AlignedBlock::zeroed(
            pid.class().size(),
        ));
        let into = block.region(0, pid.class().size());
        let read = Arc::new(crate::sync::Mutex::new(None));
        let landed = read.clone();
        let c = self
            .store()
            .read_slot_into(pid, slot, into, move |r| {
                *landed.lock() = Some(match r {
                    PageRead::Valid(b) => Some(page::seq(b.as_slice())),
                    _ => None,
                });
            })
            .unwrap();
        self.io.wait_for_completion(c).unwrap();
        let out = read.lock().take().expect("the read ended");
        out
    }

    fn slot_of(&self, pid: PageId) -> u64 {
        let entry = drive(&self.io, || self.store().entry(pid)).unwrap();
        entry.checked_sub(SLOT_BASE).expect("a page with a copy")
    }
}

/// A number freed is not taken again until a checkpoint begun after its free is in charge: the
/// checkpoint in charge may still name the page, and recovery from it would find two pages under
/// one number.
#[test]
fn a_freed_number_is_taken_again_only_once_a_checkpoint_begun_after_its_free_is_in_charge() {
    let db = Db::open();
    let class = class(0);
    let first = drive(&db.io, || db.store().allocate(class)).unwrap();
    let second = drive(&db.io, || db.store().allocate(class)).unwrap();
    assert!(first < second);
    drive(&db.io, || db.store().free(first)).unwrap();
    let third = drive(&db.io, || db.store().allocate(class)).unwrap();
    assert_ne!(third, first, "taken again before a checkpoint");
    db.checkpoint();
    let fourth = drive(&db.io, || db.store().allocate(class)).unwrap();
    assert_eq!(
        fourth, first,
        "the lowest free number, its checkpoint in charge"
    );
}

/// A checkpoint writes a changed page's copy to a new slot; the slot of the copy before stays in
/// use, holding that copy, until the new checkpoint is in charge, and is then freed and erased.
#[test]
fn a_slot_a_checkpoint_stops_naming_is_kept_until_it_is_in_charge_then_freed_and_erased() {
    // Writes land only as the IO steps, so the checkpoint is looked at between its steps.
    let db = Db::open_on(Arc::new(crate::io::crash::CrashIO::new(0)));
    let class = class(0);
    let pid = db.node();
    db.checkpoint();
    let old = db.slot_of(pid);
    let first = db.copy_in(pid, old).expect("the copy in charge");
    db.change(pid, b"k");
    let cp = db.db.checkpointer();
    let in_charge = cp.superblock().checkpoint;
    let ticket = cp.request_full();
    let mut steps = 0;
    loop {
        let out = cp.step_full(ticket, db.db.transactions()).unwrap();
        if cp.superblock().checkpoint == in_charge {
            // An eraser may run at any time: none erases a slot the checkpoint in charge names.
            drive(&db.io, || db.store().erase_freed()).unwrap();
            assert_eq!(
                db.copy_in(pid, old),
                Some(first),
                "step {steps}: the copy in charge"
            );
        }
        match out {
            IOResult::Done(()) => break,
            IOResult::IO(IOCompletions(c)) => {
                while !c.finished() {
                    db.io.step().unwrap();
                }
            }
        }
        steps += 1;
    }
    assert!(
        steps > 0,
        "the checkpoint ran in one step: nothing was looked at between"
    );
    let new = db.slot_of(pid);
    assert_ne!(new, old);
    assert!(
        db.copy_in(pid, new) > Some(first),
        "the new copy holds the change"
    );
    assert!(
        !db.store().deferred_slots().contains(&(class, old)),
        "still waiting"
    );
    drive(&db.io, || db.store().erase_freed()).unwrap();
    assert_eq!(
        db.copy_in(pid, old),
        None,
        "the slot let go still holds the copy"
    );
}

/// A page evicted while it differs from its checkpoint copy is in its swap copy alone, which
/// memory alone names and a crash loses: the next checkpoint images it from there, as its cut found
/// it, though the page is read back and changed before the image is taken.
#[test]
fn a_page_in_its_swap_copy_alone_at_a_cut_is_imaged_as_the_cut_found_it() {
    // Reads land only as the IO steps: the checkpoint yields before it has the page read in.
    let db = Db::open_on(Arc::new(crate::io::crash::CrashIO::new(0)));
    let pid = db.node();
    db.checkpoint();
    db.change(pid, b"a");
    let pool = db.db.pool();
    let mut sweep = pool.sweep();
    drive(&db.io, || sweep.step(pool)).unwrap();
    assert!(pool.evict_clean(pid), "the page left its frame");
    let swap = db.store().swap_slot(pid).expect("in its swap copy alone");
    let at_cut = db.copy_in(pid, swap).expect("the swap copy");
    let cp = db.db.checkpointer();
    let ticket = cp.request_full();
    loop {
        let out = cp.step_full(ticket, db.db.transactions()).unwrap();
        if cp.cut_taken().is_some() {
            break;
        }
        match out {
            IOResult::Done(()) => panic!("the checkpoint ended with no step after its cut"),
            IOResult::IO(IOCompletions(c)) => {
                while !c.finished() {
                    db.io.step().unwrap();
                }
            }
        }
    }
    db.change(pid, b"b");
    drive(&db.io, || cp.step_full(ticket, db.db.transactions())).unwrap();
    assert_eq!(
        db.copy_in(pid, db.slot_of(pid)),
        Some(at_cut),
        "the copy in charge is the page as the cut found it"
    );
}

/// A checkpoint writes what its cut found, whatever changes while it yields after the cut. A value
/// written since the checkpoint before, and removed meanwhile, is kept out of its leaf's image,
/// which is the leaf as the cut found it; its pages, which no copy holds yet, stay for the image to
/// name, and only the next checkpoint frees them, though this one yields freeing the pages handed
/// over before its cut. The reopened tree reads the value whole.
#[test]
fn a_value_removed_while_a_checkpoint_yields_after_its_cut_is_written_as_the_cut_found_it() {
    let db = Db::open();
    let root = db.node();
    let orphan = db.node();
    db.checkpoint();
    // Out of line in the tree's 4 KiB nodes.
    let value = vec![7u8; 12 << 10];
    db.put(root, b"k", value.clone());
    db.store().orphaned(orphan);
    let frame = drive(&db.io, || db.store().leaf(orphan))
        .unwrap()
        .expect("the orphan's page-table leaf");
    let cp = db.db.checkpointer();
    let ticket = cp.request_full();
    let hold = frame.try_lock().expect("the leaf is free");
    assert!(
        matches!(
            cp.step_full(ticket, db.db.transactions()).unwrap(),
            IOResult::IO(_)
        ),
        "the checkpoint went on under a held latch"
    );
    assert!(cp.cut_taken().is_some(), "the yield came before the cut");
    let mut remove = crate::storage::atoll::btree::Remove::new(b"k");
    drive(&db.io, || db.tree(root).remove(&mut remove)).unwrap();
    hold.release_unchanged();
    drive(&db.io, || cp.step_full(ticket, db.db.transactions())).unwrap();
    drop(frame);
    let Db {
        io,
        db: atoll,
        versions,
    } = db;
    drop(versions);
    drop(atoll);
    let reopened = Atoll::open(io.clone(), BASE, config()).unwrap();
    let tree = crate::storage::atoll::btree::Tree::open(
        io.clone(),
        reopened.pool().clone(),
        reopened.store().clone(),
        root,
    );
    let mut lookup = crate::storage::atoll::btree::Lookup::new(b"k");
    assert_eq!(
        drive(&io, || tree.lookup(&mut lookup)).unwrap(),
        Some(value),
        "the value as the checkpoint in charge cut it"
    );
}

/// What every user of a metadata page does while a change holds its latch: yields, and finishes
/// once the latch is released. None waits for it, so a holder on the same thread, under Turso's
/// cooperative scheduling, never leaves another connection stuck.
#[test]
fn a_latched_metadata_page_makes_its_users_yield_and_none_wait() {
    let db = Db::open();
    let class = class(0);
    let pid = drive(&db.io, || db.store().allocate(class)).unwrap();
    let leaf = radix::path(
        Tree::Map,
        class,
        db.store().height(Tree::Map, class),
        pid.number(),
    )[0];
    let frame = drive(&db.io, || db.store().meta().get(leaf)).unwrap();
    let yields = |res: Result<IOResult<()>>, what: &str| match res.unwrap() {
        IOResult::IO(IOCompletions(c)) => assert!(c.is_explicit_yield(), "{what} waited"),
        IOResult::Done(()) => panic!("{what} went on under a held latch"),
    };
    let hold = frame.try_lock().expect("the leaf is free");
    yields(db.store().entry(pid).map(|r| r.map(drop)), "a reader");
    yields(
        db.store().allocate(class).map(|r| r.map(drop)),
        "an allocation",
    );
    yields(db.store().free(pid), "a free");
    drop(hold);
    assert_ne!(drive(&db.io, || db.store().entry(pid)).unwrap(), FREE);
    drive(&db.io, || db.store().allocate(class)).unwrap();
    drive(&db.io, || db.store().free(pid)).unwrap();
    db.checkpoint();
}

/// Frees waiting for a checkpoint that a yield interrupts are all still waiting: freeing the first
/// slot yields on its free-space leaf, and the second slot's free is kept, then done.
#[test]
fn a_yield_while_doing_the_frees_a_checkpoint_let_go_keeps_every_one_still_owed() {
    let db = Db::open();
    let store = db.store();
    let class = class(0);
    let pages: Vec<PageId> = (0..2)
        .map(|_| drive(&db.io, || store.allocate(class)).unwrap())
        .collect();
    let n = db.db.checkpointer().superblock().checkpoint + 1;
    let (_, heights) = store.begin_cut(n);
    for &pid in &pages {
        let slot = drive(&db.io, || store.take_copy_slot(class)).unwrap();
        drive(&db.io, || store.copied(n, &heights, pid, slot)).unwrap();
    }
    store.meta().end_cut(n, &[]);
    for &pid in &pages {
        drive(&db.io, || store.free(pid)).unwrap();
    }
    let owed = store.deferred_slots();
    assert_eq!(owed.len(), 2, "{owed:?}");
    let leaf = MetaId {
        tree: Tree::Space,
        class,
        level: 0,
        index: 0,
    };
    let frame = drive(&db.io, || store.meta().get(leaf)).unwrap();
    let hold = frame.try_lock().expect("the free-space leaf is free");
    assert!(
        matches!(store.after(n + 1).unwrap(), IOResult::IO(_)),
        "a free went on under a held latch"
    );
    assert_eq!(store.deferred_slots(), owed, "a free dropped by the yield");
    hold.release_unchanged();
    drive(&db.io, || store.after(n + 1)).unwrap();
    assert!(
        store.deferred_slots().is_empty(),
        "{:?}",
        store.deferred_slots()
    );
}

/// A page handed to the checkpointer is free in the checkpoint that frees it, even when its
/// metadata edit yields first: the reopened database has its number free.
#[test]
fn an_orphan_whose_free_yielded_is_free_in_the_checkpoint_and_after_a_reopen() {
    let db = Db::open();
    let root = drive(&db.io, || {
        crate::storage::atoll::btree::Tree::create(db.db.pool(), class(0))
    })
    .unwrap();
    db.checkpoint();
    db.store().orphaned(root);
    let frame = drive(&db.io, || db.store().leaf(root))
        .unwrap()
        .expect("the root's page-table leaf");
    let cp = db.db.checkpointer();
    let ticket = cp.request_full();
    let hold = frame.try_lock().expect("the leaf is free");
    assert!(
        matches!(
            cp.step_full(ticket, db.db.transactions()).unwrap(),
            IOResult::IO(_)
        ),
        "the checkpoint went on under a held latch"
    );
    hold.release_unchanged();
    drive(&db.io, || cp.step_full(ticket, db.db.transactions())).unwrap();
    assert_eq!(drive(&db.io, || db.store().entry(root)).unwrap(), FREE);
    drop(frame);
    let Db {
        io,
        db: atoll,
        versions,
    } = db;
    drop(versions);
    drop(atoll);
    let reopened = Atoll::open(io.clone(), BASE, config()).unwrap();
    assert_eq!(
        drive(&io, || reopened.store().entry(root)).unwrap(),
        FREE,
        "freed by the checkpoint in charge"
    );
}
