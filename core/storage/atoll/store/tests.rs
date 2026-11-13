//! The page store (audit 18): numbers and slots freed only once a checkpoint lets them go, slots
//! erased before they are taken again, and a latched metadata page making its users yield.

use super::*;
use crate::io::MemoryIO;
use crate::storage::atoll::buffer::{min_budget_beside, BufferConfig};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::{ring_bytes, LogConfig};
use crate::storage::atoll::requests::counted_for_test;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::versioned::Versions;
use std::collections::BTreeSet;

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
            budget: min_budget_beside(
                ring_bytes(&log),
                crate::storage::atoll::buffer::bank_frames(
                    crate::storage::atoll::checkpoint::WRITE_BYTES,
                ),
            ),
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
            kept_bytes: None,
            lent_at_open: Default::default(),
        },
        log,
        store: StoreConfig {
            file_bytes: 64 << 10,
            meta_pages: 64,
        },
        log_budget: Some(512 << 10),
        checkpoint_write_bytes: None,
        checkpointing: crate::storage::atoll::checkpoint::Checkpointing::Writers,
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
        Self::open_with(io, config())
    }

    fn open_with(io: Arc<dyn IO>, config: AtollConfig) -> Self {
        let db = Atoll::open(io.clone(), BASE, config).unwrap();
        let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), 1);
        Self { io, db, versions }
    }

    fn store(&self) -> &Arc<PageStore> {
        self.db.store()
    }

    fn checkpoint(&self) {
        let cp = self.db.checkpointer();
        let mut ticket = cp.request_full();
        drive(&self.io, || {
            cp.step_full(&mut ticket, self.db.transactions())
        })
        .unwrap();
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
        let copy = into.clone();
        let read = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
        let landed = read.clone();
        let c = self
            .store()
            .read_slot_into(pid, slot, into, move |r| {
                *landed.lock() = Some(match r {
                    PageRead::Valid => Some(page::seq(copy.as_slice())),
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
    let mut ticket = cp.request_full();
    let mut steps = 0;
    loop {
        let out = cp.step_full(&mut ticket, db.db.transactions()).unwrap();
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
    let mut ticket = cp.request_full();
    loop {
        let out = cp.step_full(&mut ticket, db.db.transactions()).unwrap();
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
    drive(&db.io, || cp.step_full(&mut ticket, db.db.transactions())).unwrap();
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
    let mut ticket = cp.request_full();
    let hold = frame.try_lock().expect("the leaf is free");
    // Read with the latch held, asserted once the database is closed: a panic through it ends
    // the process.
    let yielded = matches!(
        cp.step_full(&mut ticket, db.db.transactions()).unwrap(),
        IOResult::IO(_)
    );
    let cut_first = cp.cut_taken().is_some();
    let mut remove = crate::storage::atoll::btree::Remove::new(b"k");
    drive(&db.io, || db.tree(root).remove(&mut remove)).unwrap();
    hold.release_unchanged();
    drive(&db.io, || cp.step_full(&mut ticket, db.db.transactions())).unwrap();
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
    let found = drive(&io, || tree.lookup(&mut lookup)).unwrap();
    drop(tree);
    drop(reopened);
    assert!(yielded, "the checkpoint went on under a held latch");
    assert!(cut_first, "the yield came before the cut");
    assert_eq!(
        found,
        Some(value),
        "the value as the checkpoint in charge cut it"
    );
}

/// Metadata contention is cooperative: return one pending release signal and let the holder
/// continue. Repeating a caller while the same hold remains does no work and supplies no wake.
#[test]
fn a_latched_metadata_page_parks_its_users_until_release() {
    let db = Db::open();
    let class = class(0);
    let pid = drive(&db.io, || db.store().allocate(class)).unwrap();
    let frame = drive(&db.io, || db.store().leaf(pid))
        .unwrap()
        .expect("allocated leaf");
    let hold = frame.try_lock().expect("the leaf is free");
    let mut waits = Vec::with_capacity(3);
    let mut proper = true;
    for result in [
        db.store().entry(pid).map(|r| r.map(drop)),
        db.store().allocate(class).map(|r| r.map(drop)),
        db.store().free(pid),
    ] {
        match result {
            Ok(IOResult::IO(IOCompletions(c))) => {
                proper &= !c.finished() && c.is_remote_wait() && c.is_engine_wait();
                waits.push(c);
            }
            _ => proper = false,
        }
    }
    let shared = waits.len() == 3 && waits.iter().all(|c| c.ptr_eq(&waits[0]));
    hold.release_unchanged();
    let finished = waits.iter().all(Completion::finished);
    for c in &waits {
        if !c.finished() {
            c.complete(0);
        }
    }
    let resumed = drive(&db.io, || db.store().entry(pid)).unwrap() != FREE;
    drive(&db.io, || db.store().allocate(class)).unwrap();
    drive(&db.io, || db.store().free(pid)).unwrap();
    db.checkpoint();
    drop(waits);
    drop(frame);
    drop(db);
    assert!(
        proper && shared && finished && resumed,
        "metadata readers, allocation and free share a pending release, then resume"
    );
}

/// A copy slot claimed by a step that then waits, the free-space leaf it marks held by another
/// change, is given back as the step returns: no claim is left behind for the next call to pass
/// over, and the next call takes a slot once the leaf is let go (the auditor's #4573; mutant: the
/// claim kept on the wait's return, as before).
#[test]
fn a_copy_slot_claimed_by_a_step_that_waits_is_given_back() {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::mpsc;
    const BOUND: std::time::Duration = std::time::Duration::from_secs(60);
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let claimed_then = Rc::new(Cell::new(None));
    let holder = Rc::new(RefCell::new(None));
    {
        let (store, io, claimed_then, holder) = (
            store.clone(),
            db.io.clone(),
            claimed_then.clone(),
            holder.clone(),
        );
        crate::race::at("store.copy_slot_claimed", move || {
            let claimed = store.claimed[class.index()].ones();
            claimed_then.set(claimed.first().copied());
            let Some(&slot) = claimed.first() else { return };
            let leaf = radix::path(Tree::Space, class, store.height(Tree::Space, class), slot)[0];
            let (held_tx, held_rx) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                let frame = drive(&io, || store.meta().get(leaf)).unwrap();
                let hold = frame.try_lock().expect("the free-space leaf is free");
                let _ = held_tx.send(());
                let _ = go_rx.recv_timeout(BOUND);
                hold.release_unchanged();
            });
            let held = held_rx.recv_timeout(BOUND).is_ok();
            *holder.borrow_mut() = Some((thread, held));
        });
    }
    let mut mark = MarkEdit::default();
    let waited = matches!(store.take_copy_slot(class, &mut mark), Ok(IOResult::IO(_)));
    let left = store.claimed[class.index()].ones();
    let _ = go_tx.send(());
    let (joined, held) = holder
        .borrow_mut()
        .take()
        .map_or((false, false), |(t, held)| (t.join().is_ok(), held));
    let next = drive(&db.io, || store.take_copy_slot(class, &mut mark)).is_ok();
    let left_after = store.claimed[class.index()].ones();
    // Every assertion with the database closed.
    drop((store, db));
    assert!(
        claimed_then.get().is_some() && held && joined,
        "the step's claim was not reached, or the leaf not held"
    );
    assert!(waited, "the step did not wait on the held leaf");
    assert!(
        left.is_empty(),
        "the waiting step left its claim behind: {left:?}"
    );
    assert!(next, "the next call took no slot");
    assert!(
        left_after.is_empty(),
        "a claim left after the next call: {left_after:?}"
    );
}

/// A copy slot's take whose walk waits on a read of the free-space leaf it searches, the leaf out of
/// memory and its read kept by the backend, holds no claim while it waits: it claims only once its
/// walk finds the slot, and its mark then needs no read, its pages being the walk's (M2b 1c). The
/// read in, the next call takes the slot and leaves no claim (the auditor's #4578, restated now
/// that a mark cannot wait on a read after its claim; mutant: the claim kept past its call).
#[test]
fn a_copy_slot_take_whose_walk_waits_on_a_metadata_read_holds_no_claim() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(0));
    let db = Db::open_on(crash.clone());
    let store = db.store().clone();
    // A class nothing has used: its free-space leaf reads in clean, held by no one, so it may be
    // evicted before the take.
    let class = class(1);
    let path = radix::path(Tree::Space, class, store.height(Tree::Space, class), 0);
    drop(drive(&db.io, || store.meta().get_all(&path)).unwrap());
    // The last read's request, kept for a test to cancel, holds its page through its callback.
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    store.meta().evict(path[0]);
    let out = !store.meta().in_memory(path[0]);
    let before = crash.in_flight();
    let mut mark = MarkEdit::default();
    let waited = matches!(store.take_copy_slot(class, &mut mark), Ok(IOResult::IO(_)));
    let reading = crash.in_flight() > before;
    let left = store.claimed[class.index()].ones();
    let next = drive(&db.io, || store.take_copy_slot(class, &mut mark)).is_ok();
    let left_after = store.claimed[class.index()].ones();
    // Every assertion with the database closed.
    drop(mark);
    drop((store, db));
    assert!(out, "the free-space leaf in memory before the take");
    assert!(
        waited && reading,
        "the take did not wait on the leaf's read with the backend"
    );
    assert!(
        left.is_empty(),
        "the take waiting on its walk's read holds a claim: {left:?}"
    );
    assert!(next, "the next call took no slot");
    assert!(
        left_after.is_empty(),
        "a claim left after the next call: {left_after:?}"
    );
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
    let mut mark = MarkEdit::default();
    for &pid in &pages {
        let slot = drive(&db.io, || store.take_copy_slot(class, &mut mark)).unwrap();
        let mut edit = CutEdit::default();
        drive(&db.io, || store.copied(n, &heights, pid, slot, &mut edit)).unwrap();
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
    let mut freeing = FreeEdit::default();
    assert!(
        matches!(store.after(n + 1, &mut freeing).unwrap(), IOResult::IO(_)),
        "a free went on under a held latch"
    );
    assert_eq!(store.deferred_slots(), owed, "a free dropped by the yield");
    hold.release_unchanged();
    drive(&db.io, || store.after(n + 1, &mut freeing)).unwrap();
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
    let mut ticket = cp.request_full();
    let hold = frame.try_lock().expect("the leaf is free");
    assert!(
        matches!(
            cp.step_full(&mut ticket, db.db.transactions()).unwrap(),
            IOResult::IO(_)
        ),
        "the checkpoint went on under a held latch"
    );
    hold.release_unchanged();
    drive(&db.io, || cp.step_full(&mut ticket, db.db.transactions())).unwrap();
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

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn local_source_waits_before_claim_and_rechecks_before_read() {
    use crate::storage::atoll::buffer::{LocalSource, PageSource};
    use crate::sync::atomic::AtomicUsize;
    let db = Db::open();
    let class = class(0);
    let pid = drive(&db.io, || db.store().allocate(class)).unwrap();
    let frame = drive(&db.io, || db.store().leaf(pid))
        .unwrap()
        .expect("allocated leaf");
    let source = LocalSource::new(db.store().clone());
    let before_hold = match source.locate(pid).unwrap() {
        IOResult::Done(located) => Some(located),
        _ => None,
    };
    let hold = frame.try_lock().expect("free");
    let first = match source.locate(pid).unwrap() {
        IOResult::IO(c) => Some(c.0),
        IOResult::Done(x) => {
            drop(x);
            None
        }
    };
    let mut ready = 0;
    let mut pending = 0;
    let mut same = true;
    let start = crate::storage::atoll::testing::allocations();
    for _ in 0..32 {
        match source.locate(pid).unwrap() {
            IOResult::IO(c) => {
                same &= first.as_ref().is_some_and(|f| f.ptr_eq(&c.0));
                if c.finished() {
                    ready += 1
                } else {
                    pending += 1
                }
            }
            IOResult::Done(x) => {
                drop(x);
                ready += 1;
            }
        }
    }
    let allocations = crate::storage::atoll::testing::allocations() - start;
    let submits = crate::storage::atoll::buffer::Counting::new(None);
    let callbacks = Arc::new(AtomicUsize::new(0));
    let buffer = Arc::new(crate::io::Buffer::new_temporary(class.size()));
    let raced = before_hold.map(|located| {
        let callbacks = callbacks.clone();
        located.read(
            buffer.clone(),
            Box::new(move |_| {
                callbacks.fetch_add(1, Ordering::Relaxed);
            }),
            &submits,
        )
    });
    let raced_wait = match raced {
        Some(Ok(IOResult::IO(c))) => Some(c.0),
        _ => None,
    };
    let raced_pending = raced_wait.as_ref().is_some_and(|c| !c.finished());
    let raced_same = first
        .as_ref()
        .zip(raced_wait.as_ref())
        .is_some_and(|(a, b)| a.ptr_eq(b));
    let no_submission = submits.handed.get() == 0 && callbacks.load(Ordering::Relaxed) == 0;
    hold.release_unchanged();
    let finished = first.as_ref().is_some_and(Completion::finished)
        && raced_wait.as_ref().is_some_and(Completion::finished);
    for c in [&first, &raced_wait].into_iter().flatten() {
        if !c.finished() {
            c.complete(0);
        }
    }
    let after = match source.locate(pid).unwrap() {
        IOResult::Done(located) => {
            let callbacks = callbacks.clone();
            matches!(
                located.read(
                    buffer.clone(),
                    Box::new(move |_| {
                        callbacks.fetch_add(1, Ordering::Relaxed);
                    }),
                    &submits
                ),
                Ok(IOResult::Done(None))
            )
        }
        _ => false,
    };
    let done_once = callbacks.load(Ordering::Relaxed) == 1 && submits.handed.get() == 0;
    drop(buffer);
    drop(first);
    drop(raced_wait);
    drop(source);
    drop(frame);
    drop(submits);
    drop(callbacks);
    drop(db);
    assert_eq!(
        (ready, pending),
        (0, 32),
        "locate must park before claiming a data frame"
    );
    assert!(
        same && raced_same && raced_pending && no_submission && finished && after && done_once,
        "read must recheck a hold acquired after locate, submit no IO, and resume after release"
    );
    assert_eq!(
        allocations, 0,
        "unchanged held locate polls allocate neither a located read nor an image"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_swap_copy_does_not_wait_for_the_metadata_entry_it_bypasses() {
    use crate::storage::atoll::buffer::{LocalSource, PageSource};
    let db = Db::open();
    let class = class(0);
    let pid = drive(&db.io, || db.store().allocate(class)).unwrap();
    let slot = drive(&db.io, || db.store().take_swap_slot(class)).unwrap();
    db.store().set_swap(pid, slot);
    let frame = drive(&db.io, || db.store().leaf(pid))
        .unwrap()
        .expect("allocated leaf");
    let source = LocalSource::new(db.store().clone());
    let hold = frame.try_lock().expect("free");
    // Locate alone needs no entry or bytes from the metadata page when a swap slot exists.
    let located = match source.locate(pid).unwrap() {
        IOResult::Done(x) => {
            drop(x);
            true
        }
        IOResult::IO(c) => {
            if !c.finished() {
                c.0.complete(0);
            }
            false
        }
    };
    hold.release_unchanged();
    db.store().drop_swap(pid);
    drop(source);
    drop(frame);
    drop(db);
    assert!(
        located,
        "the new held check must preserve the existing swap-copy bypass"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_lock_group_waits_on_the_blocked_page_and_releases_earlier_pages() {
    let db = Db::open();
    let class = class(0);
    let ids = [0, 1].map(|index| MetaId {
        tree: Tree::Map,
        class,
        level: 0,
        index,
    });
    let frames = ids.map(|id| drive(&db.io, || db.store().meta().get(id)).unwrap());
    let holder = frames[1].try_lock().expect("second free");
    let expected = frames[1].wait_while_held();
    let wait = match Locked::new(&frames) {
        Err(wait) => Some(wait),
        Ok(group) => {
            group.release_unchanged();
            None
        }
    };
    let released = frames[0]
        .try_lock()
        .map(|h| h.release_unchanged())
        .is_some();
    let pending = wait.as_ref().is_some_and(|c| !c.finished());
    let same = wait.as_ref().is_some_and(|c| c.ptr_eq(&expected));
    holder.release_unchanged();
    let finished = wait.as_ref().is_some_and(Completion::finished);
    if !expected.finished() {
        expected.complete(0);
    }
    if let Some(c) = &wait {
        if !c.finished() {
            c.complete(0);
        }
    }
    drop(wait);
    drop(expected);
    drop(frames);
    drop(db);
    assert!(
        released && pending && same && finished,
        "a group releases its prefix and returns exactly its failed page's pending release"
    );
}

/// A growth or an erasure that finds another under way waits for its let-go, not a retry at once:
/// the same pending wait at every look while the holder is paused, finished by its let-go, and a
/// let-go no one waits for takes no slow path (DESIGN.md 9.1.3, *No retry without a change*).
#[test]
fn a_growth_or_erasure_finding_another_under_way_waits_for_its_let_go() {
    let db = Db::open();
    let store = db.store().clone();
    drop(own(&store.growing, &store.grown).unwrap_or_else(|_| panic!("free")));
    drop(own(&store.erasure, &store.erased).unwrap_or_else(|_| panic!("free")));
    let uncontended = (store.grown.woke(), store.erased.woke());
    let mut outcomes = Vec::new();
    for (name, held) in [("growth", true), ("erasure", false)] {
        let holder =
            held.then(|| own(&store.growing, &store.grown).unwrap_or_else(|_| panic!("free")));
        let eraser =
            (!held).then(|| own(&store.erasure, &store.erased).unwrap_or_else(|_| panic!("free")));
        let mut waits = Vec::new();
        for _ in 0..32 {
            let out = if held {
                store.grow(Tree::Map, class(0))
            } else {
                store.erase_freed()
            };
            match out {
                Ok(IOResult::IO(IOCompletions(c))) => waits.push(c),
                _ => break,
            }
        }
        let pending = waits.len() == 32 && waits.iter().all(|c| !c.finished());
        // The holder's let-go: its guard's drop.
        drop(holder);
        drop(eraser);
        let woken = waits.iter().all(|c| c.finished());
        outcomes.push((name, pending, woken));
    }
    drop(store);
    // Checked once the database is gone: a panic unwinding through it ends the process.
    drop(db);
    assert_eq!(
        uncontended,
        (0, 0),
        "a let-go no one waited for took the slow path"
    );
    for (name, pending, woken) in outcomes {
        assert!(
            pending,
            "{name}: a look with it held came back ready, not on one pending wait"
        );
        assert!(
            woken,
            "{name}: the let-go left the waiter's wait unfinished"
        );
    }
}

/// A step of the checkpoint's task is one unit of its work (DESIGN.md 22.1, 22.9 item 1): on a
/// backend whose writes land inline, a step images at most one page, where one step once imaged
/// every page owed, and the checkpoint still ends with every changed page imaged.
#[test]
fn a_step_of_the_checkpoints_task_images_at_most_one_page() {
    const PAGES: u64 = 64;
    let db = Db::open();
    let roots: Vec<PageId> = (0..PAGES).map(|_| db.node()).collect();
    db.checkpoint();
    for (i, &root) in roots.iter().enumerate() {
        db.change(root, &(i as u64).to_le_bytes());
    }
    let cp = db.db.checkpointer();
    let in_charge = cp.superblock().checkpoint;
    cp.request_full();
    let imaged = cp.stats().pages;
    let (mut steps, mut most, mut refused) = (0u64, 0u64, false);
    while cp.superblock().checkpoint == in_charge && steps < 1 << 20 {
        let before = cp.stats().pages;
        if !cp.claim_one() {
            refused = true;
            break;
        }
        most = most.max(cp.stats().pages - before);
        steps += 1;
    }
    let ended = cp.superblock().checkpoint != in_charge;
    let pages = cp.stats().pages - imaged;
    // Asserted once the database is closed: a panic through it ends the process.
    drop(db);
    assert!(!refused, "a claim refused with no other stepper");
    assert!(ended, "the checkpoint did not end in {steps} steps");
    assert!(
        pages >= PAGES,
        "{pages} pages imaged of the {PAGES} changed"
    );
    assert_eq!(most, 1, "a step imaged {most} pages");
    assert!(steps > pages, "{steps} steps imaged {pages} pages");
}

// ---------------------------------------------------------------- held requests

/// The data files from this one on are held: files no database here opens of itself.
const HELD_FROM: u64 = 1000;

/// What [`HeldIO`] holds: once armed, every sync to a held data file (and every write too, with
/// `writes`) is accepted and kept, its callback not run, until the test answers it and then lets
/// it go, so a place in flight is seen held past its answer, until its request retires.
#[derive(Default)]
struct Held {
    armed: crate::sync::atomic::AtomicBool,
    writes: crate::sync::atomic::AtomicBool,
    queue: Mutex<VecDeque<(Completion, i32)>>,
    most: crate::sync::atomic::AtomicUsize,
    /// Where each held write's bytes start: the blocks the writes read from.
    sources: Mutex<BTreeSet<usize>>,
}

impl Held {
    fn hold(&self, c: Completion, result: i32) -> Result<Completion> {
        c.accept();
        let mut queue = self.queue.lock();
        queue.push_back((c.clone(), result));
        self.most.fetch_max(queue.len(), Ordering::Relaxed);
        Ok(c)
    }

    fn most(&self) -> usize {
        self.most.load(Ordering::Relaxed)
    }

    /// The oldest request held, taken out: the test answers it, then lets it go.
    fn oldest(&self) -> Option<(Completion, i32)> {
        self.queue.lock().pop_front()
    }
}

/// A [`MemoryIO`] whose held data files ([`HELD_FROM`] on, class 0) hold their requests while
/// [`Held`] is armed. No file here punches holes, so an erasure writes zeros.
struct HeldIO {
    inner: Arc<MemoryIO>,
    held: Arc<Held>,
}

impl crate::io::Clock for HeldIO {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for HeldIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let held = path
            .strip_prefix(&format!("{BASE}.c0."))
            .and_then(|n| n.parse::<u64>().ok())
            .is_some_and(|n| n >= HELD_FROM);
        Ok(Arc::new(HeldFile {
            file: self.inner.open_file(path, flags, direct)?,
            held: held.then(|| self.held.clone()),
        }))
    }
    fn sync_directory(&self, path: &str) -> Result<()> {
        self.inner.sync_directory(path)
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn delivery(&self) -> crate::io::Delivery {
        self.inner.delivery()
    }
    fn step(&self) -> Result<()> {
        self.inner.step()
    }
    fn try_step(
        &self,
        key: crate::io::ReadyKey,
        ready: &std::task::Waker,
    ) -> Result<crate::io::Stepped> {
        self.inner.try_step(key, ready)
    }
    fn says_what_a_step_found(&self) -> bool {
        self.inner.says_what_a_step_found()
    }
    fn withdraw(&self, key: crate::io::ReadyKey) {
        self.inner.withdraw(key)
    }
}

struct HeldFile {
    file: Arc<dyn File>,
    held: Option<Arc<Held>>,
}

impl HeldFile {
    fn holding(&self, write: bool) -> Option<&Held> {
        self.held.as_deref().filter(|h| {
            h.armed.load(Ordering::Relaxed) && (!write || h.writes.load(Ordering::Relaxed))
        })
    }
}

impl File for HeldFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }
    fn pread(&self, at: u64, c: Completion) -> Result<Completion> {
        self.file.pread(at, c)
    }
    fn pwrite(&self, at: u64, bytes: Arc<crate::io::Buffer>, c: Completion) -> Result<Completion> {
        match self.holding(true) {
            Some(held) => {
                held.sources.lock().insert(bytes.as_ptr() as usize);
                held.hold(c, bytes.len() as i32)
            }
            None => self.file.pwrite(at, bytes, c),
        }
    }
    fn sync(&self, c: Completion, how: crate::io::FileSyncType) -> Result<Completion> {
        match self.holding(false) {
            Some(held) => held.hold(c, 0),
            None => self.file.sync(c, how),
        }
    }
    fn size(&self) -> Result<u64> {
        self.file.size()
    }
    fn truncate(&self, n: u64, c: Completion) -> Result<Completion> {
        self.file.truncate(n, c)
    }
}

/// What answering and letting go of held requests one at a time showed, for the wait each answer
/// was for.
#[derive(Default)]
struct Answers {
    /// Waits still pending once the oldest request was answered, finished once it was let go: a
    /// place held past the answer, until the retirement.
    held_to_retirement: usize,
    /// Waits still pending once the oldest request was let go too.
    left_pending: usize,
}

impl Answers {
    /// Answers the oldest request `held` holds and lets it go, reading `wait` between.
    fn answer_oldest(&mut self, held: &Held, wait: &Completion) -> bool {
        let Some((request, result)) = held.oldest() else {
            return false;
        };
        request.complete(result);
        let pending = !wait.finished();
        request.retire();
        match wait.finished() {
            true if pending => self.held_to_retirement += 1,
            true => {}
            false => self.left_pending += 1,
        }
        true
    }
}

/// An erasure batch takes a run of slots at most, and its zero writes and syncs hold their places
/// in flight to `WRITES`, each until its request retires, not until it is answered; the zero
/// writes all read from the class's one block (the auditor's P2-CP-U1, P2-CP-F1 and #4324;
/// mutants: a batch of every slot ready, the flight unbounded, a place let go at the answer, a
/// block of zeros made for each write).
#[test]
fn an_erasure_batch_is_a_run_of_slots_its_requests_held_to_the_flight_limit_until_retired() {
    let held = Arc::new(Held::default());
    let db = Db::open_on(Arc::new(HeldIO {
        inner: Arc::new(MemoryIO::new()),
        held: held.clone(),
    }));
    let store = db.store().clone();
    let class = class(0);
    // Slots in the held files, freed as a checkpoint's frees free them: more than two batches'.
    let slots = 2 * ERASE_RUN as u64 + 40;
    let first = HELD_FROM * store.slots_per_file(class);
    for slot in first..first + slots {
        store.unerased[class.index()].set(slot);
        store.erase_owed.fetch_add(1, Ordering::AcqRel);
        store.to_erase.retire(&store.epochs, (class, slot));
    }
    held.writes.store(true, Ordering::Relaxed);
    held.armed.store(true, Ordering::Relaxed);
    let mut answers = Answers::default();
    let (mut owed, mut batches, mut largest) = (store.erase_owed(), Vec::new(), 0);
    let (mut finished, mut stuck, mut error) = (false, false, None);
    for _ in 0..1 << 20 {
        match store.erase_one() {
            Ok(IOResult::Done(Erasing::Done | Erasing::HeldBack(_))) => {
                finished = true;
                break;
            }
            Ok(IOResult::Done(Erasing::More)) => {}
            Ok(IOResult::IO(IOCompletions(wait))) => {
                if !wait.finished() && !answers.answer_oldest(&held, &wait) {
                    stuck = true;
                    break;
                }
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
        let now = store.erase_owed();
        if now < owed {
            batches.push(owed - now);
            largest = largest.max(owed - now);
            owed = now;
        }
    }
    held.armed.store(false, Ordering::Relaxed);
    while let Some((request, result)) = held.oldest() {
        request.complete(result);
        request.retire();
    }
    let most = held.most();
    let sources = held.sources.lock().len();
    drop(store);
    // Checked once the database is gone: a panic unwinding through it ends the process.
    drop(db);
    assert_eq!(error, None);
    assert_eq!(sources, 1, "the blocks the zero writes read from");
    assert!(!stuck, "an erasure waited with nothing held");
    assert!(finished, "the erasure did not end");
    assert_eq!(owed, 0, "slots left unerased");
    assert!(
        largest <= ERASE_RUN,
        "a batch of {largest} slots: {batches:?}"
    );
    assert!(batches.len() >= 3, "batches {batches:?}");
    assert_eq!(most, WRITES, "requests in flight at most");
    assert!(
        answers.held_to_retirement > 0,
        "no place was held past its answer"
    );
    assert_eq!(
        answers.left_pending, 0,
        "a wait outlived the oldest request"
    );
}

/// A checkpoint syncs the data files a file a unit, walking them by a cursor, and their syncs hold
/// their places in flight to `WRITES`, each until its request retires (the auditor's P2-CP-F1 and
/// P2-CP-U1; mutants: the flight unbounded, a place let go at the answer).
#[test]
fn a_checkpoints_file_syncs_hold_their_places_to_the_flight_limit_until_retired() {
    let held = Arc::new(Held::default());
    let db = Db::open_on(Arc::new(HeldIO {
        inner: Arc::new(MemoryIO::new()),
        held: held.clone(),
    }));
    let store = db.store().clone();
    let files = WRITES as u64 + 9;
    let per_file = store.slots_per_file(class(0));
    for n in HELD_FROM..HELD_FROM + files {
        store.data_file(class(0), n * per_file).unwrap();
    }
    let root = db.node();
    db.checkpoint();
    db.change(root, b"k");
    held.armed.store(true, Ordering::Relaxed);
    let cp = db.db.checkpointer();
    let in_charge = cp.superblock().checkpoint;
    let mut ticket = cp.request_full();
    let mut answers = Answers::default();
    let (mut finished, mut stuck, mut error) = (false, false, None);
    for _ in 0..1 << 20 {
        match cp.step_full(&mut ticket, db.db.transactions()) {
            Ok(IOResult::Done(())) => {
                finished = true;
                break;
            }
            Ok(IOResult::IO(IOCompletions(wait))) => {
                if wait.finished() || answers.answer_oldest(&held, &wait) {
                    continue;
                }
                for _ in 0..1 << 16 {
                    if wait.finished() {
                        break;
                    }
                    db.io.step().unwrap();
                }
                if !wait.finished() {
                    stuck = true;
                    break;
                }
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    held.armed.store(false, Ordering::Relaxed);
    while let Some((request, result)) = held.oldest() {
        request.complete(result);
        request.retire();
    }
    let ended = cp.superblock().checkpoint != in_charge;
    let most = held.most();
    drop(store);
    drop(db);
    assert_eq!(error, None);
    assert!(!stuck, "the checkpoint waited on what nothing advances");
    assert!(finished && ended, "the checkpoint did not end");
    assert_eq!(
        most, WRITES,
        "syncs in flight at most, of {files} held files"
    );
    assert!(
        answers.held_to_retirement > 0,
        "no place was held past its answer"
    );
}

/// The data files are walked by a cursor: each file opened before the walk once, in order, a run
/// of their places looked at a call, a segment of places never made passed whole, and the walk
/// ends at the ends it took although a file is opened past them after every call (the auditor's
/// P2-CP-U1 and #4327; mutants: a run unbounded, a run's end not kept, the ends not kept).
#[test]
fn the_data_files_are_walked_a_run_of_places_a_call() {
    let db = Db::open();
    let store = db.store().clone();
    // Files far apart in one segment of places, and one in a later segment.
    let per_file = store.slots_per_file(class(0));
    let wanted: Vec<u64> = vec![2000, 2001, 2000 + 3 * FILE_RUN + 5, 9000];
    for &n in &wanted {
        store.data_file(class(0), n * per_file).unwrap();
    }
    let mut opened = BTreeSet::new();
    for class in 0..CLASS_COUNT {
        store.files[class].each(|i, cell| {
            if cell.get().is_some() {
                opened.insert((class, i));
            }
        });
    }
    let ends = store.data_files_end();
    let mut at = (0, 0);
    let (mut found, mut runs, mut calls) = (Vec::new(), 0, 0);
    // A file opened past class 0's end after every call: a later writer's, which this walk passes.
    let mut later = Vec::new();
    let past = ends[0].next_multiple_of(1 << 20);
    while calls < 1 << 16 {
        calls += 1;
        let step = store.data_file_from(&mut at, &ends);
        let n = past + later.len() as u64;
        store.data_file(class(0), n * per_file).unwrap();
        later.push((0, n));
        match step {
            // The cursor stands just past the file found.
            Some(Some(_)) => found.push((at.0, at.1 - 1)),
            Some(None) => runs += 1,
            None => break,
        }
    }
    let last = store.data_file_from(&mut at, &ends).is_none();
    drop(store);
    drop(db);
    assert!(
        later.iter().all(|f| !found.contains(f)),
        "a file opened past the end the walk took was walked"
    );
    let found_set: BTreeSet<_> = found.iter().copied().collect();
    assert_eq!(
        found.len(),
        found_set.len(),
        "a file found twice: {found:?}"
    );
    assert_eq!(found_set, opened, "the files found against those opened");
    assert!(
        found.windows(2).all(|w| w[0] < w[1]),
        "out of order: {found:?}"
    );
    assert!(
        runs >= 3,
        "{runs} runs found no file: a gap of {} places",
        3 * FILE_RUN + 3
    );
    assert!(last, "a walk passed its end began again");
}

/// A shard of the swap table is listed a run at a time, in page order, from where the last run
/// ended: every page once and none past the run (the auditor's P2-CP-U1; mutants: a run
/// unbounded, a run listed from the shard's start again).
#[test]
fn a_swap_shard_is_listed_a_run_at_a_time_from_where_the_last_ended() {
    const RUN: usize = 7;
    let db = Db::open();
    let store = db.store().clone();
    // Pages of one shard, chosen by the shard the table puts them in.
    let target: *const _ = store.swap_shard(PageId::new(class(0), 1 << 20));
    let pages: Vec<PageId> = ((1 << 20)..)
        .map(|n| PageId::new(class(0), n))
        .filter(|&pid| std::ptr::eq(store.swap_shard(pid), target))
        .take(3 * RUN + 2)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    for (i, &pid) in pages.iter().enumerate() {
        store.set_swap(pid, i as u64 + 1);
    }
    let shard = (0..store.swap_shards())
        .find(|&i| std::ptr::eq(&store.swap[i], target))
        .expect("the shard");
    let (mut listed, mut runs, mut after) = (Vec::new(), Vec::new(), None);
    for _ in 0..pages.len() + 2 {
        let run = store.swap_shard_pages(shard, after, RUN);
        runs.push(run.len());
        listed.extend(run.iter().copied());
        if run.len() < RUN {
            break;
        }
        after = run.last().copied();
    }
    // Taken out of the table alone: the slots named were never taken, so none is given up.
    for &pid in &pages {
        store.swap[shard].lock().remove(&pid);
    }
    drop(store);
    drop(db);
    assert!(runs.iter().all(|&n| n <= RUN), "runs {runs:?}");
    assert_eq!(listed, pages, "the pages listed against those swapped");
    assert_eq!(runs, [RUN, RUN, RUN, 2]);
}

/// A database whose metadata cache keeps one page, and so one read with its backend at a time
/// (DESIGN.md 22.18, a metadata read credit), over `CrashIO`, whose reads wait until it is stepped.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
fn one_read() -> (Arc<crate::io::crash::CrashIO>, Db) {
    reads_at_once(1)
}

/// A database whose metadata cache keeps `pages` pages, and so as many reads with its backend at
/// a time, over `CrashIO`.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
fn reads_at_once(pages: usize) -> (Arc<crate::io::crash::CrashIO>, Db) {
    let crash = Arc::new(crate::io::crash::CrashIO::new(0));
    let mut config = config();
    config.store.meta_pages = pages;
    let db = Db::open_with(crash.clone(), config);
    (crash, db)
}

/// A checkpoint's edit of its cut over six metadata pages, none in memory: a tree's root page,
/// its page table three levels high, its copy's slot in the free space's first leaf and the new
/// one in its second, the free space two levels high.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
struct CutCase {
    crash: Arc<crate::io::crash::CrashIO>,
    db: Db,
    store: Arc<PageStore>,
    n: u64,
    heights: [[u8; CLASS_COUNT]; TREES],
    /// The root page, a node a checkpoint imaged, and a second.
    node: PageId,
    second: PageId,
    /// A number the page table reaches only at three levels, in a leaf of its own.
    far: PageId,
    new: u64,
    /// The edit's pages, in id order.
    pages: Vec<MetaId>,
    /// How many of them are out of memory before the edit, evicted clean or never read: every
    /// one, so none is kept in memory but by the edit itself.
    evicted: usize,
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
fn cut_case() -> CutCase {
    let (crash, db) = one_read();
    let store = db.store().clone();
    let class = class(0);
    let far = PageId::new(class, radix::MAP_ENTRIES * radix::FANOUT + 7);
    drive(&db.io, || store.allocate_at(far)).unwrap();
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    let (node, second) = (db.node(), db.node());
    // The nodes' copies named; then every metadata page written, so clean and evictable: the first
    // checkpoint's edits of the page table as it stands are the second's to write.
    db.checkpoint();
    db.checkpoint();
    let old = db.slot_of(node);
    let new = radix::SPACE_BITS + 5;
    let n = db.db.checkpointer().superblock().checkpoint + 1;
    let (_, heights) = store.begin_cut(n);
    let mut pages: Vec<MetaId> = radix::path(Tree::Map, class, 3, node.number())
        .iter()
        .chain(radix::path(Tree::Space, class, 2, old).iter())
        .chain(radix::path(Tree::Space, class, 2, new).iter())
        .copied()
        .collect();
    pages.sort();
    pages.dedup();
    // The last read's request, kept for a test to cancel, holds its page through its callback.
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    for &id in &pages {
        store.meta().evict(id);
    }
    let evicted = pages
        .iter()
        .filter(|&&id| !store.meta().in_memory(id))
        .count();
    CutCase {
        crash,
        db,
        store,
        n,
        heights,
        node,
        second,
        far,
        new,
        pages,
        evicted,
    }
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
impl CutCase {
    fn copied(&self, edit: &mut CutEdit) -> Result<IOResult<Edited>> {
        self.store
            .copied(self.n, &self.heights, self.node, self.new, edit)
    }

    /// Whether no one holds `id`'s page: evicted now, or out of memory already, as the cache's
    /// budget evicts a page no one holds.
    fn let_go(&self, id: MetaId) -> bool {
        self.store.meta().evict(id) || !self.store.meta().in_memory(id)
    }

    /// Every read with the backend retired, and the last one's request, which a test hook keeps
    /// and which holds its page through its callback, let go: only the edit holds its pages then.
    fn settle(&self) {
        while self.crash.in_flight() > 0 {
            self.crash.step().unwrap();
        }
        drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    }
}

/// A checkpoint's edit of its cut over six metadata pages, under one read credit, keeps each page
/// it found from its look on, loading or in memory, across every poll, so none is evicted between
/// them and each is read once: six reads, one with the backend at a time, seven polls, and the
/// page table names the new copy. Every one of its pages is tried for eviction after each poll and
/// again once its reads retire, so a page the edit let go is caught whether or not it is held at
/// that moment (DESIGN.md 22.18; the auditor's #4597; mutant: the edit's pages let go at every
/// wait).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_cut_edit_wider_than_its_read_credits_keeps_its_pages_and_reads_each_once() {
    let case = cut_case();
    let clean = (case.evicted, case.pages.len());
    let mut edit = CutEdit::default();
    let before = case.crash.completed();
    let (mut polls, mut most) = (0, 0);
    let mut evicted_held = Vec::new();
    let edited = loop {
        polls += 1;
        match case.copied(&mut edit) {
            Ok(IOResult::Done(edited)) => break Some(edited.to_vec()),
            Ok(IOResult::IO(_)) => {}
            Err(_) => break None,
        }
        most = most.max(case.crash.in_flight());
        // Each page the edit found stays, loading and once read, before the next poll: none of its
        // pages is in memory but by the edit, so any page evicted is one it let go.
        for &id in &case.pages {
            if case.store.meta().evict(id) {
                evicted_held.push(id);
            }
        }
        case.settle();
        for &id in &case.pages {
            if case.store.meta().evict(id) {
                evicted_held.push(id);
            }
        }
        if polls > case.pages.len() + 2 {
            break None;
        }
    };
    let reads = case.crash.completed() - before;
    let entry = drive(&case.db.io, || case.store.entry(case.node)).ok();
    let (pages, new) = (case.pages.clone(), case.new);
    drop(edit);
    drop(case);
    assert_eq!(
        clean.0, clean.1,
        "pages out of memory before the edit, of its pages"
    );
    assert_eq!(pages.len(), 6, "the fixture's pages: {pages:?}");
    assert_eq!(edited, Some(pages), "the pages the edit latched and named");
    assert!(
        evicted_held.is_empty(),
        "pages the edit held evicted: {evicted_held:?}"
    );
    assert_eq!((reads, polls), (6, 7), "(metadata reads, polls)");
    assert!(most <= 1, "{most} reads with the backend at once");
    assert_eq!(entry, Some(new + SLOT_BASE), "the page table's entry");
}

/// A cut edit whose every page is in memory and held, refused a latch another change holds,
/// waits holding no latch and every frame: none is evicted while it waits, and once the latch is
/// let go it is made with no read (the auditor's #4597; mutant: the pages let go at a latch's
/// wait).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_cut_edit_waiting_on_a_latch_keeps_every_page_and_reads_none_again() {
    let case = cut_case();
    let clean = (case.evicted, case.pages.len());
    let mut edit = CutEdit::default();
    let (mut polls, mut early) = (0, false);
    // Every page held, the last read retired and not yet seen. An edit made is not polled again:
    // a second mark of its slot is refused under its latches, which ends the process.
    while !early && edit.state.pages.held() < case.pages.len() && polls <= case.pages.len() + 2 {
        polls += 1;
        early = matches!(case.copied(&mut edit), Ok(IOResult::Done(_)));
        case.settle();
    }
    let held = edit.state.pages.held();
    let reads = case.crash.completed();
    let (waited, kept) = match early {
        true => (false, 0),
        false => case.store.with_map_latched(case.node, || {
            let waited = matches!(case.copied(&mut edit), Ok(IOResult::IO(_)));
            let kept = case
                .pages
                .iter()
                .filter(|&&id| !case.store.meta().evict(id))
                .count();
            (waited, kept)
        }),
    };
    let held_waiting = edit.state.pages.held();
    let done = !early && matches!(case.copied(&mut edit), Ok(IOResult::Done(_)));
    let read_after = case.crash.completed() - reads;
    let pages = case.pages.len();
    drop(edit);
    drop(case);
    assert_eq!(
        clean.0, clean.1,
        "pages out of memory before the edit, of its pages"
    );
    assert!(!early, "the edit was made before every page was held");
    assert_eq!(held, pages, "pages held before the latch was taken");
    assert!(waited, "the edit did not wait on the latch held");
    assert_eq!(
        (kept, held_waiting),
        (pages, pages),
        "(pages not evicted, pages held) while it waited"
    );
    assert!(done, "the edit was not made once the latch was let go");
    assert_eq!(read_after, 0, "reads after its pages were all in memory");
}

/// The page table growing a level between two polls of a cut edit changes nothing it needs: the
/// cut's heights are its own, and the leaf it edits as the page stands is the cut's, at every
/// height. It reads no page of the new root and names none (the auditor's #4617).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_cut_edit_whose_page_table_grows_between_its_polls_needs_no_page_more() {
    let case = cut_case();
    let mut edit = CutEdit::default();
    let first = matches!(case.copied(&mut edit), Ok(IOResult::IO(_)));
    case.settle();
    let grown = drive(&case.db.io, || case.store.grow(Tree::Map, class(0))).is_ok();
    let mut edited = None;
    for _ in 0..=case.pages.len() + 2 {
        match case.copied(&mut edit) {
            Ok(IOResult::Done(e)) => {
                edited = Some(e.to_vec());
                break;
            }
            Ok(IOResult::IO(_)) => case.settle(),
            Err(_) => break,
        }
    }
    let entry = drive(&case.db.io, || case.store.entry(case.node)).ok();
    let (pages, new) = (case.pages.clone(), case.new);
    drop(edit);
    drop(case);
    assert!(first && grown, "(the edit waited, the page table grew)");
    assert_eq!(
        edited,
        Some(pages),
        "the pages edited, the new root not among them"
    );
    assert_eq!(entry, Some(new + SLOT_BASE), "the page table's entry");
}

/// One edit's state given another edit's key lets go of every page of the first before the second
/// looks at any: here the first page's leaf, of no use to the second, is evictable at once (the
/// auditor's #4597; mutant: the state kept for another key).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_cut_edit_given_another_page_lets_go_of_the_first_ones_pages() {
    let case = cut_case();
    let clean = (case.evicted, case.pages.len());
    let mut edit = CutEdit::default();
    let first = matches!(case.copied(&mut edit), Ok(IOResult::IO(_)));
    case.settle();
    let first_held = edit.state.pages.held_ids();
    let other = case
        .store
        .left_out(case.n, &case.heights, case.far, &mut edit)
        .is_ok();
    let other_held = edit.state.pages.held_ids();
    let pending = edit.pending();
    case.settle();
    let leaf = radix::path(Tree::Map, class(0), 3, case.node.number())[0];
    let evicted = case.let_go(leaf);
    let far_key = CutKey::of(case.n, case.far, None);
    drop(edit);
    drop(case);
    assert_eq!(
        clean.0, clean.1,
        "pages out of memory before the edit, of its pages"
    );
    assert!(first && other, "(the first edit waited, the second polled)");
    assert_eq!(first_held, vec![leaf], "the first edit's page held");
    assert!(
        !other_held.contains(&leaf),
        "the second edit holds the first's leaf: {other_held:?}"
    );
    assert_eq!(pending, Some(far_key), "the edit under way");
    assert!(evicted, "the first edit's leaf was not let go");
}

/// A cut edit whose read fails ends with the failure and lets go of every page it held: the page
/// read before it is evictable at once (the auditor's #4597).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_cut_edit_whose_read_fails_lets_go_of_every_page() {
    let case = cut_case();
    let clean = (case.evicted, case.pages.len());
    let mut edit = CutEdit::default();
    let (mut polls, mut early) = (0, false);
    // An edit made is not polled again: a second mark of its slot ends the process.
    while !early && edit.state.pages.held() < 2 && polls <= case.pages.len() {
        polls += 1;
        early = matches!(case.copied(&mut edit), Ok(IOResult::Done(_)));
        if edit.state.pages.held() < 2 {
            case.settle();
        }
    }
    let read_first = edit.state.pages.held_ids();
    case.crash.set_faults(crate::io::crash::Faults {
        read: 1.0,
        ..Default::default()
    });
    case.settle();
    case.crash.set_faults(Default::default());
    let failed = !early && case.copied(&mut edit).is_err();
    let (pending, held) = (edit.pending(), edit.state.pages.held());
    let evicted = read_first.first().is_some_and(|&id| case.let_go(id));
    drop(edit);
    drop(case);
    assert_eq!(
        clean.0, clean.1,
        "pages out of memory before the edit, of its pages"
    );
    assert!(!early, "the edit was made before its second read");
    assert_eq!(read_first.len(), 2, "pages held as the second read failed");
    assert!(failed, "the edit did not end with the read's failure");
    assert_eq!(
        (pending, held),
        (None, 0),
        "(the edit under way, pages held)"
    );
    assert!(evicted, "the page read before the failure was not let go");
}

/// A cut edit abandoned while a read of its is with the backend lets go of every page: the one
/// read and the one loading are evictable once the read retires (the auditor's #4597).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn an_abandoned_cut_edit_lets_go_of_every_page() {
    let case = cut_case();
    let clean = (case.evicted, case.pages.len());
    let mut edit = CutEdit::default();
    let (mut polls, mut early) = (0, false);
    // An edit made is not polled again: a second mark of its slot ends the process.
    while !early && edit.state.pages.held() < 2 && polls <= case.pages.len() {
        polls += 1;
        early = matches!(case.copied(&mut edit), Ok(IOResult::Done(_)));
        if edit.state.pages.held() < 2 {
            case.settle();
        }
    }
    let held = edit.state.pages.held_ids();
    let loading = case.crash.in_flight();
    drop(edit);
    case.settle();
    let evicted = held.iter().filter(|&&id| case.let_go(id)).count();
    drop(case);
    assert_eq!(
        clean.0, clean.1,
        "pages out of memory before the edit, of its pages"
    );
    assert_eq!(
        (held.len(), loading),
        (2, 1),
        "(pages held, reads with the backend)"
    );
    assert_eq!(evicted, 2, "pages let go by the edit abandoned");
}

/// A page left out over the metadata pages a first edit of the cut already latched, their cut
/// copies kept and in memory, its latches free and no free deferred, allocates nothing: the
/// edit's own set, latch group, paths and pages named are inline, the count made printed (the
/// auditor's #4606 and #4646: a copy's edit may still allocate a deferred free's entry, and a
/// page's first change in a cut its cut copy; mutant: the latch group collected into a vector).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_page_left_out_over_pages_a_cut_edit_kept_allocates_nothing() {
    let case = cut_case();
    let mut edit = CutEdit::default();
    let first = drive(&case.db.io, || case.copied(&mut edit)).is_ok();
    let before = crate::storage::atoll::testing::allocations();
    let second = case
        .store
        .left_out(case.n, &case.heights, case.second, &mut edit);
    let made = crate::storage::atoll::testing::allocations() - before;
    let done = matches!(second, Ok(IOResult::Done(_)));
    drop(second);
    drop(edit);
    drop(case);
    eprintln!("a page left out over pages a cut edit kept: {made} allocations");
    assert!(
        first && done,
        "(the first edit made, the second made at once)"
    );
    assert!(
        made == 0,
        "{made} allocations by a page left out over pages a cut edit kept"
    );
}

/// A tree is 1 to `MAX_HEIGHT` levels high: the fewest whose root reaches every leaf a metadata
/// id names; a store's tree stored as 0 opens 1 high; a growth past `MAX_HEIGHT` is refused before
/// it reads or changes anything (the auditor's #4606 and #4613; mutant: a growth past it).
#[test]
fn a_tree_is_one_to_max_height_levels_high_and_grows_no_further() {
    let leaves = |height: u8| (radix::FANOUT as u128).pow(u32::from(height) - 1);
    let reaches = 1u128 << crate::storage::atoll::meta::INDEX_BITS;
    assert_eq!(MAX_HEIGHT, 7);
    assert!(leaves(MAX_HEIGHT) >= reaches && leaves(MAX_HEIGHT - 1) < reaches);
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let store = PageStore::open(
        counted_for_test(io),
        BASE,
        config().store,
        [[0; CLASS_COUNT]; TREES],
        0,
    );
    let opened = store.height(Tree::Map, class(0));
    store.heights[Tree::Map as usize][1].store(MAX_HEIGHT, Ordering::Release);
    let refused = matches!(
        store.grow(Tree::Map, class(1)),
        Err(LimboError::InternalError(_))
    );
    let after = store.height(Tree::Map, class(1));
    let resident = store.meta().resident();
    drop(store);
    assert_eq!(opened, 1, "a tree stored as 0 levels high opened");
    assert!(refused, "a growth past MAX_HEIGHT was not refused");
    assert_eq!(
        (after, resident),
        (MAX_HEIGHT, 0),
        "(height, pages read) after the refusal"
    );
}

/// The size of each owner of a store edit's state, printed for the record (the auditor's #4606).
#[test]
fn the_sizes_of_a_store_edits_state() {
    eprintln!(
        "PageSet<CUT_PAGES>={} CutEdit={} Locked={} Edited={}",
        std::mem::size_of::<PageSet<CUT_PAGES>>(),
        std::mem::size_of::<CutEdit>(),
        std::mem::size_of::<Locked<'static>>(),
        std::mem::size_of::<Edited>(),
    );
}

/// A growth publishes its height before it lets go of the two roots: a page number taken in the
/// gap a release before the publication would leave, under the old root's latch with the old
/// height read, would be counted by the old root alone and missed by the new one's count, copied
/// from it before. Here one is taken just after the roots are let go and is counted in the new
/// root (the auditor's #4617.2 and #4619, whose reproduction this is; mutant: the height published
/// after the roots are let go).
#[test]
fn a_growth_publishes_its_height_before_letting_go_of_its_roots() {
    use std::cell::Cell;
    use std::rc::Rc;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let store = PageStore::open(
        counted_for_test(io.clone()),
        "/db/growth",
        StoreConfig::default(),
        [[1; CLASS_COUNT]; TREES],
        0,
    );
    let class = class(0);
    let leaf = radix::root(Tree::Map, class, 1);
    let new_root = radix::root(Tree::Map, class, 2);
    // Both roots in memory, so the take in the gap never waits on a read.
    let warmed = drive(&io, || store.meta().get_all(&[leaf, new_root])).unwrap();
    let (ran, taken, seen) = (
        Rc::new(Cell::new(false)),
        Rc::new(Cell::new(false)),
        Rc::new(Cell::new(0)),
    );
    let pid = PageId::new(class, 1);
    {
        let (ran, taken, seen, store) = (ran.clone(), taken.clone(), seen.clone(), store.clone());
        crate::race::at("store.grown", move || {
            ran.set(true);
            seen.set(store.height(Tree::Map, class));
            taken.set(matches!(
                store.take_number_at(pid, true),
                Ok(IOResult::Done(true))
            ));
        });
    }
    let grown = drive(&io, || store.grow(Tree::Map, class)).is_ok();
    let entry = drive(&io, || store.entry(pid)).ok();
    let used = drive(&io, || store.pages_in_use(class)).ok();
    let height = store.height(Tree::Map, class);
    drop((warmed, store, io));
    assert!(
        ran.get() && taken.get(),
        "the take just after the roots were let go was not made"
    );
    assert_eq!(seen.get(), 2, "the height the take read");
    assert!(
        grown && height == 2 && entry == Some(ALLOCATED),
        "(grown, height, entry): ({grown}, {height}, {entry:?})"
    );
    assert_eq!(used, Some(1), "pages in use as the new root counts them");
}

/// The count `id` keeps of the units in use below its `child`, as checkpoint `n`'s cut found it.
fn cut_count(db: &Db, n: u64, id: MetaId, child: u64) -> Option<u64> {
    let frame = drive(&db.io, || db.store().meta().get(id)).ok()?;
    frame.read_cut(
        n,
        |live| radix::used_below(live, child),
        |cut| radix::used_below(cut, child),
    )
}

/// Whether `slot` is in use in its free-space leaf `id`, as checkpoint `n`'s cut found it.
fn cut_bit(db: &Db, n: u64, id: MetaId, slot: u64) -> Option<bool> {
    let frame = drive(&db.io, || db.store().meta().get(id)).ok()?;
    frame.read_cut(
        n,
        |live| radix::space_bit(live, slot),
        |cut| radix::space_bit(cut, slot),
    )
}

/// A page taken in `class`, with `slots` in use in its free space, and checkpoint `n`'s cut
/// begun: the cut's heights.
fn cut_with_slots(
    db: &Db,
    class: SizeClass,
    slots: &[u64],
) -> (PageId, u64, [[u8; CLASS_COUNT]; TREES]) {
    let store = db.store();
    for &s in slots {
        drive(&db.io, || store.plant_slot(class, s, true)).unwrap();
    }
    let pid = drive(&db.io, || store.allocate(class)).unwrap();
    let n = db.db.checkpointer().superblock().checkpoint + 1;
    let (_, heights) = store.begin_cut(n);
    (pid, n, heights)
}

/// A copy's slot taken past the cut's free space, the free space as it stands grown since the cut,
/// grows the cut's own tree a level to reach it: its new root counts below its first child the two
/// units in use under the cut's root, the one leaf, and below its second the slot (the auditor's
/// #4620 and #4625; mutants: no cut root grown, and the root's count not carried).
#[test]
fn a_copy_slot_past_the_cuts_free_space_grows_the_cuts_tree_a_level() {
    let db = Db::open();
    let class = class(1);
    let (pid, n, heights) = cut_with_slots(&db, class, &[3, 4]);
    drive(&db.io, || db.store().grow(Tree::Space, class)).unwrap();
    let slot = radix::SPACE_BITS + 5;
    let mut edit = CutEdit::default();
    let done = drive(&db.io, || {
        db.store().copied(n, &heights, pid, slot, &mut edit)
    })
    .ok();
    let root = radix::root(Tree::Space, class, 2);
    let leaf = radix::path(Tree::Space, class, 2, slot)[0];
    let counts = (
        cut_count(&db, n, root, 0),
        cut_count(&db, n, root, 1),
        cut_bit(&db, n, leaf, slot),
    );
    drop(db);
    let edited = done.expect("the edit failed");
    assert_eq!(
        edited.space, 2,
        "the cut's free-space height the edit ended at"
    );
    assert!(
        edited.contains(&root) && edited.contains(&radix::root(Tree::Space, class, 1)),
        "the cut's old and new roots among the pages edited: {edited:?}"
    );
    assert_eq!(
        counts,
        (Some(2), Some(1), Some(true)),
        "(the new root's counts below its first and second children, the slot's bit) at the cut"
    );
}

/// A copy's slot past the cut's free space by two levels grows the cut's tree two levels, each new
/// root counting below its first child what the root under it counts (the auditor's #4625, its
/// multi-level case).
#[test]
fn a_copy_slot_two_levels_past_the_cuts_free_space_grows_the_cuts_tree_two_levels() {
    let db = Db::open();
    let class = class(2);
    let (pid, n, heights) = cut_with_slots(&db, class, &[3, 4]);
    drive(&db.io, || db.store().grow(Tree::Space, class)).unwrap();
    drive(&db.io, || db.store().grow(Tree::Space, class)).unwrap();
    let slot = radix::capacity(Tree::Space, 2) + 7;
    let mut edit = CutEdit::default();
    let done = drive(&db.io, || {
        db.store().copied(n, &heights, pid, slot, &mut edit)
    })
    .ok();
    let (middle, top) = (
        radix::root(Tree::Space, class, 2),
        radix::root(Tree::Space, class, 3),
    );
    let path = radix::path(Tree::Space, class, 3, slot);
    let counts = (
        cut_count(&db, n, middle, 0),
        cut_count(&db, n, top, 0),
        cut_count(&db, n, top, 1),
        cut_count(&db, n, path[1], radix::child(Tree::Space, 1, slot)),
        cut_bit(&db, n, path[0], slot),
    );
    drop(db);
    let space = done.expect("the edit failed").space;
    assert_eq!(path[1].index, 1, "the slot's node of level 1");
    assert_eq!(space, 3, "the cut's free-space height the edit ended at");
    assert_eq!(
        counts,
        (Some(2), Some(2), Some(1), Some(1), Some(true)),
        "(level-1 root's first count, the top root's first and second, the slot's node's count, \
         the slot's bit) at the cut"
    );
}

/// A page with a copy in the cut's free-space leaf, copied again to a slot past the cut's free
/// space, frees its old slot in the grown tree: the new root counts the leaf's units less the old
/// slot, the old slot's bit is clear and its free waits for the checkpoint (the auditor's #4625).
#[test]
fn a_copy_past_the_cuts_free_space_frees_its_old_slot_in_the_grown_tree() {
    let db = Db::open();
    let store = db.store();
    let class = class(3);
    let old = 10;
    let (pid, first, heights) = cut_with_slots(&db, class, &[3, 4]);
    // Its first copy, in the leaf: in use as it stands too, as its take marks it.
    drive(&db.io, || store.plant_slot(class, old, true)).unwrap();
    let mut edit = CutEdit::default();
    drive(&db.io, || {
        store.copied(first, &heights, pid, old, &mut edit)
    })
    .unwrap();
    store.meta().end_cut(first, &[]);
    let n = first + 1;
    let (_, heights) = store.begin_cut(n);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    let slot = radix::SPACE_BITS + 9;
    let done = drive(&db.io, || store.copied(n, &heights, pid, slot, &mut edit)).ok();
    let root = radix::root(Tree::Space, class, 2);
    let leaf = radix::root(Tree::Space, class, 1);
    let counts = (
        cut_count(&db, n, root, 0),
        cut_count(&db, n, root, 1),
        cut_bit(&db, n, leaf, old),
    );
    let owed = store.deferred_slots().contains(&(class, old));
    drop(db);
    let space = done.expect("the edit failed").space;
    assert_eq!(space, 2, "the cut's free-space height the edit ended at");
    assert_eq!(
        counts,
        (Some(2), Some(1), Some(false)),
        "(the new root's counts below its first and second children, the old slot's bit) at the cut"
    );
    assert!(owed, "the old slot's free does not wait for the checkpoint");
}

/// A checkpoint whose copy's slot lies past the free space its cut found writes a free space that
/// reaches it: here every slot of the one leaf is in use, so the checkpoint's copies go to the
/// second, and once reopened the free space is two levels high and names every unit it named
/// before the close, the copy's slot among them (the auditor's #4620; mutant: the checkpoint's
/// height not advanced past the edit).
#[test]
fn a_checkpoint_whose_copy_slot_is_past_its_cut_writes_a_free_space_that_reaches_it() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let class = class(0);
    let (slot, before) = {
        let db = Db::open_on(io.clone());
        let store = db.store();
        let node = db.node();
        let leaf = radix::root(Tree::Space, class, 1);
        let frame = drive(&db.io, || store.meta().get(leaf)).unwrap();
        let free: Vec<u64> = frame
            .read(|b| {
                (0..radix::SPACE_BITS)
                    .filter(|&s| !radix::space_bit(b, s))
                    .collect()
            })
            .expect("the leaf read");
        drop(frame);
        for s in free {
            drive(&db.io, || store.plant_slot(class, s, true)).unwrap();
        }
        db.checkpoint();
        let slot = db.slot_of(node);
        let mut census = Census::new(Tree::Space, class);
        drive(&db.io, || store.census(&mut census)).unwrap();
        (slot, census.units)
    };
    let db = Db::open_on(io.clone());
    let height = db.store().height(Tree::Space, class);
    let mut census = Census::new(Tree::Space, class);
    let read = drive(&db.io, || db.store().census(&mut census)).is_ok();
    drop(db);
    assert!(
        slot >= radix::SPACE_BITS,
        "the copy went to slot {slot}, in the first leaf"
    );
    assert!(read, "the reopened free space's census failed");
    assert_eq!(height, 2, "the reopened free space's height");
    assert!(census.miscounts().is_empty(), "{:?}", census.miscounts());
    assert!(
        census.units.iter().any(|&(u, _)| u == slot),
        "the copy's slot {slot} is not in the reopened free space"
    );
    assert_eq!(
        census.units.len(),
        before.len(),
        "units in use reopened against before the close"
    );
}

/// The auditor's reproduction of #4620, as a test: a copy slot taken after the cut past the cut's
/// free space, with the free space's one leaf full, its live tree grown by the take. The edit's
/// metadata pages written as a checkpoint writes them, and the store reopened at the heights the
/// checkpoint's superblock records (the one the edit returned for the free space): the page table
/// names the slot, and the reopened free space lists it with every slot of the full leaf, the
/// leaf's units and the one past it in use. The same with the growth before the cut, the control
/// (mutants: no cut root grown, the root's count not carried).
#[test]
fn a_copy_slot_past_the_cuts_free_space_is_in_the_free_space_reopened() {
    for grow_before_cut in [true, false] {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let base = "/db/cut-space";
        let store = PageStore::open(
            counted_for_test(io.clone()),
            base,
            StoreConfig::default(),
            [[1; CLASS_COUNT]; TREES],
            0,
        );
        let class = class(0);
        let pid = drive(&io, || store.allocate(class)).unwrap();
        let mut last = None;
        let mut mark = MarkEdit::default();
        for _ in 0..radix::SPACE_BITS {
            last = Some(drive(&io, || store.take_copy_slot(class, &mut mark)).unwrap());
        }
        if grow_before_cut {
            drive(&io, || store.grow(Tree::Space, class)).unwrap();
        }
        let (dirty, mut heights) = store.begin_cut(1);
        let mut ids: BTreeSet<_> = dirty.collect();
        let slot = drive(&io, || store.take_copy_slot(class, &mut mark)).unwrap();
        let mut edit = CutEdit::default();
        let edited = drive(&io, || store.copied(1, &heights, pid, slot, &mut edit)).unwrap();
        ids.extend(edited.iter().copied());
        let space = edited.space;
        // The height the checkpoint takes from the edit, for its superblock.
        heights[Tree::Space as usize][class.index()] = space;
        let ids: Vec<_> = ids.into_iter().collect();
        let frames = drive(&io, || store.meta().get_all(&ids)).unwrap();
        for frame in &frames {
            while let Some(wait) = store.meta().write_for(frame, 1).unwrap() {
                io.wait_for_completion(wait).unwrap();
            }
        }
        store.meta().end_cut(1, &frames);
        store.meta().durable_sides(1, &frames);
        store.meta().put_in_charge(1);
        drop((frames, store));
        let reopened = PageStore::open(
            counted_for_test(io.clone()),
            base,
            StoreConfig::default(),
            heights,
            1,
        );
        let entry = drive(&io, || reopened.entry(pid)).ok();
        let mut census = Census::new(Tree::Space, class);
        let read = drive(&io, || reopened.census(&mut census)).is_ok();
        let listed = census.units.iter().any(|&(unit, _)| unit == slot);
        let (used, miscounts) = (census.units.len(), census.miscounts());
        drop((census, reopened, io));
        let case = if grow_before_cut {
            "grown before the cut"
        } else {
            "grown after the cut"
        };
        assert_eq!(
            last,
            Some(radix::SPACE_BITS - 1),
            "{case}: the full leaf's setup"
        );
        assert_eq!(slot, radix::SPACE_BITS, "{case}: the slot past the leaf");
        assert_eq!(space, 2, "{case}: the cut's free-space height");
        assert_eq!(
            entry,
            Some(slot + SLOT_BASE),
            "{case}: the page table's entry"
        );
        assert!(read && miscounts.is_empty(), "{case}: {miscounts:?}");
        assert!(listed, "{case}: the slot is not in the free space reopened");
        assert_eq!(
            used as u64,
            radix::SPACE_BITS + 1,
            "{case}: slots in use reopened"
        );
    }
}

/// A page left out whose edit waited on a latch, its pages all held, keeps them when another page's
/// copy grows the cut's free space meanwhile: polled again at the new height, it looks only at the
/// new root, and frees its old slot in the grown tree, the new root counting the leaf's units less
/// it (the auditor's #4627; mutant: the frames let go at an aim).
#[test]
fn a_waiting_left_out_keeps_its_pages_across_a_growth_of_the_cuts_free_space() {
    let db = Db::open();
    let store = db.store();
    let class = class(4);
    let old = 10;
    let (pid, first, heights) = cut_with_slots(&db, class, &[3, 4]);
    drive(&db.io, || store.plant_slot(class, old, true)).unwrap();
    let mut edit = CutEdit::default();
    drive(&db.io, || {
        store.copied(first, &heights, pid, old, &mut edit)
    })
    .unwrap();
    store.meta().end_cut(first, &[]);
    // Another page, in a page-table leaf of its own.
    let other = PageId::new(class, radix::MAP_ENTRIES + 3);
    drive(&db.io, || store.allocate_at(other)).unwrap();
    let n = first + 1;
    let (_, mut heights) = store.begin_cut(n);
    let mut leaving = CutEdit::default();
    // The free space's leaf latched, as another change holds it: the edit reads its entry as of
    // the cut, then waits on that latch with every page held.
    let space_leaf = radix::root(Tree::Space, class, 1);
    let frame = drive(&db.io, || store.meta().get(space_leaf)).unwrap();
    let hold = frame.try_lock().expect("the leaf's latch free");
    let waited = matches!(
        store.left_out(n, &heights, pid, &mut leaving),
        Ok(IOResult::IO(_))
    );
    // The page table's pages come before the leaf in id order: latched first, and let go when the
    // leaf was refused, so none is held as the edit returns.
    let map = radix::path(
        Tree::Map,
        class,
        heights[Tree::Map as usize][class.index()],
        pid.number(),
    );
    let free_latches = |map: &[MetaId]| {
        map.iter()
            .filter(|&&id| {
                let page = drive(&db.io, || store.meta().get(id)).unwrap();
                let hold = page.try_lock();
                let free = hold.is_some();
                if let Some(hold) = hold {
                    hold.release_unchanged();
                }
                free
            })
            .count()
    };
    let free_first = free_latches(&map);
    hold.release_unchanged();
    let held = leaving.state.pages.held();
    // The other page's copy, past the cut's free space: the cut's tree grows.
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    let mut landing = CutEdit::default();
    let slot = radix::SPACE_BITS + 5;
    let landed = drive(&db.io, || {
        store.copied(n, &heights, other, slot, &mut landing)
    })
    .ok();
    if let Some(landed) = &landed {
        heights[Tree::Space as usize][class.index()] = landed.space;
    }
    let looks = leaving.state.pages.looks();
    let hold = frame.try_lock().expect("the leaf's latch free");
    let rewaited = matches!(
        store.left_out(n, &heights, pid, &mut leaving),
        Ok(IOResult::IO(_))
    );
    let free_second = free_latches(&map);
    hold.release_unchanged();
    let looked = leaving.state.pages.looks() - looks;
    let held_grown = leaving.state.pages.held();
    let done = drive(&db.io, || store.left_out(n, &heights, pid, &mut leaving)).is_ok();
    let root = radix::root(Tree::Space, class, 2);
    let leaf = radix::root(Tree::Space, class, 1);
    let counts = (
        cut_count(&db, n, root, 0),
        cut_count(&db, n, root, 1),
        cut_bit(&db, n, leaf, old),
    );
    drop(frame);
    drop(db);
    assert!(
        waited && rewaited,
        "(the first poll waited, the second waited)"
    );
    assert_eq!(
        (free_first, free_second),
        (map.len(), map.len()),
        "the page table's latches free as each poll returned waiting on the leaf, of {}",
        map.len()
    );
    assert_eq!(
        landed.map(|l| l.space),
        Some(2),
        "the cut's free-space height"
    );
    assert_eq!(
        (looked, held_grown),
        (1, held + 1),
        "(looks at the new height, pages held) by the edit polled again"
    );
    assert!(done, "the edit was not made");
    assert_eq!(
        counts,
        (Some(2), Some(1), Some(false)),
        "(the new root's counts below its first and second children, the old slot's bit) at the cut"
    );
}

/// The units of `class`'s free space in use, as a census walks it from its root as it stands.
fn space_units(db: &Db, class: SizeClass) -> Vec<u64> {
    let mut census = Census::new(Tree::Space, class);
    drive(&db.io, || db.store().census(&mut census)).unwrap();
    census.units.iter().map(|&(unit, _)| unit).collect()
}

/// The counts below each child of `class`'s first free-space node at `level`, as it stands.
fn node_counts(db: &Db, class: SizeClass, level: u8) -> Vec<u64> {
    let id = MetaId {
        tree: Tree::Space,
        class,
        level,
        index: 0,
    };
    let frame = drive(&db.io, || db.store().meta().get(id)).unwrap();
    let counts = frame.read(|b| {
        (0..radix::FANOUT)
            .map(|child| radix::used_below(b, child))
            .collect()
    });
    counts.unwrap_or_default()
}

/// The counts below each child of the first free-space node at `level` that `units` in use make.
fn counts_of(units: &[u64], level: u8) -> Vec<u64> {
    let mut counts = vec![0; radix::FANOUT as usize];
    for &unit in units {
        counts[radix::child(Tree::Space, level, unit) as usize] += 1;
    }
    counts
}

/// A slot in use in a free space two levels high, its free deferred for a checkpoint made
/// durable, its leaf and root out of memory, under one read credit.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
struct SlotCase {
    crash: Arc<crate::io::crash::CrashIO>,
    db: Db,
    store: Arc<PageStore>,
    class: SizeClass,
    /// The durable checkpoint the free waited for.
    n: u64,
    slot: u64,
    /// The units in use before the free, the slot among them.
    units: Vec<u64>,
    /// The slot's path, its leaf first, as ids order them.
    pages: Vec<MetaId>,
    /// How many of them are out of memory before the free.
    evicted: usize,
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
fn slot_case() -> SlotCase {
    let (crash, db) = one_read();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    let mut mark = MarkEdit::default();
    let slot = drive(&db.io, || store.take_copy_slot(class, &mut mark)).unwrap();
    // Every metadata page written, so clean and evictable.
    db.checkpoint();
    db.checkpoint();
    let n = db.db.checkpointer().superblock().checkpoint;
    let units = space_units(&db, class);
    store
        .deferred
        .lock()
        .entry(n)
        .or_default()
        .push_back(Deferred::Slot(class, slot));
    let pages = radix::path(Tree::Space, class, 2, slot).to_vec();
    // The last read's request, kept for a test to cancel, holds its page through its callback.
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    for &id in &pages {
        store.meta().evict(id);
    }
    let evicted = pages
        .iter()
        .filter(|&&id| !store.meta().in_memory(id))
        .count();
    SlotCase {
        crash,
        db,
        store,
        class,
        n,
        slot,
        units,
        pages,
        evicted,
    }
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
impl SlotCase {
    fn after_one(&self, edit: &mut FreeEdit) -> Result<IOResult<bool>> {
        self.store.after_one(self.n, edit)
    }

    /// Every read with the backend retired, and the last one's request let go.
    fn settle(&self) {
        while self.crash.in_flight() > 0 {
            self.crash.step().unwrap();
        }
        drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    }

    /// The units in use the free should leave: those before it but its slot.
    fn freed(&self) -> Vec<u64> {
        self.units
            .iter()
            .copied()
            .filter(|&u| u != self.slot)
            .collect()
    }
}

/// A deferred free of a slot two levels deep, its leaf and root out of memory, under one read
/// credit, keeps each page it found from its look on across every poll: each read once, one read
/// with the backend at a time, three polls, no page of its path evictable after any poll or once
/// its reads retire. The free space then lists exactly the slots in use before it but the slot,
/// and its root counts exactly those (DESIGN.md 22.18; the auditor's #4651 and #4653; mutant: the
/// pages let go at every wait).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_deferred_free_wider_than_its_read_credits_keeps_its_pages_and_reads_each_once() {
    let case = slot_case();
    let clean = (case.evicted, case.pages.len());
    let mut edit = FreeEdit::default();
    let before = case.crash.completed();
    let (mut polls, mut most) = (0, 0);
    let mut evicted = Vec::new();
    let freed = loop {
        polls += 1;
        match case.after_one(&mut edit) {
            Ok(IOResult::Done(none_left)) => break Some(none_left),
            Ok(IOResult::IO(_)) => {}
            Err(_) => break None,
        }
        most = most.max(case.crash.in_flight());
        // No page of its path is in memory but by the edit: any evicted is one it let go.
        for &id in &case.pages {
            if case.store.meta().evict(id) {
                evicted.push(id);
            }
        }
        case.settle();
        for &id in &case.pages {
            if case.store.meta().evict(id) {
                evicted.push(id);
            }
        }
        if polls > case.pages.len() + 2 {
            break None;
        }
    };
    let reads = case.crash.completed() - before;
    let units = space_units(&case.db, case.class);
    let root = node_counts(&case.db, case.class, 1);
    let expected = case.freed();
    let (was_used, pages, owed) = (
        case.units.contains(&case.slot),
        case.pages.len(),
        case.store
            .deferred_slots()
            .contains(&(case.class, case.slot)),
    );
    drop(edit);
    drop(case);
    assert_eq!(
        clean.0, clean.1,
        "pages out of memory before the free, of its pages"
    );
    assert_eq!(pages, 2, "the slot's path: its leaf and its root");
    assert!(was_used, "the slot was not in use before its free");
    assert_eq!(freed, Some(false), "the free not made");
    assert!(
        evicted.is_empty(),
        "pages the free held evicted: {evicted:?}"
    );
    assert_eq!((reads, polls), (2, 3), "(metadata reads, polls)");
    assert!(most <= 1, "{most} reads with the backend at once");
    assert_eq!(units, expected, "the slots in use after the free");
    assert_eq!(
        root,
        counts_of(&expected, 1),
        "the root's counts after the free"
    );
    assert!(!owed, "the free still owed");
}

/// A deferred free whose root another change holds latches its leaf first, as ids order a leaf
/// before its root, then meets the root: it lets the leaf's latch go, so another change takes the
/// leaf while it waits, and keeps both pages, so the leaf cannot be evicted. Once the root is let
/// go the free is made with no read (the auditor's #4651; mutant: the pages let go at a latch's
/// wait).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_deferred_free_meeting_its_root_held_lets_its_leaf_go_and_keeps_its_pages() {
    let case = slot_case();
    let (leaf, root) = (case.pages[0], case.pages[1]);
    let mut edit = FreeEdit::default();
    let (mut polls, mut early) = (0, false);
    // Both pages held, the last read retired. A free made is not polled again.
    while !early && edit.state.pages.held() < case.pages.len() && polls <= case.pages.len() + 2 {
        polls += 1;
        early = matches!(case.after_one(&mut edit), Ok(IOResult::Done(_)));
        case.settle();
    }
    let held = edit.state.pages.held();
    let reads = case.crash.completed();
    // The root's frame and latch held in a scope of their own, let go before anything more.
    let (latched, waited, leaf_free, leaf_kept, held_waiting) = {
        let root_frame = drive(&case.db.io, || case.store.meta().get(root)).unwrap();
        let hold = root_frame.try_lock();
        let latched = hold.is_some();
        let waited = !early && latched && matches!(case.after_one(&mut edit), Ok(IOResult::IO(_)));
        let leaf_free = {
            let frame = drive(&case.db.io, || case.store.meta().get(leaf)).unwrap();
            let taken = frame.try_lock();
            let free = taken.is_some();
            if let Some(taken) = taken {
                taken.release_unchanged();
            }
            free
        };
        let leaf_kept = !case.store.meta().evict(leaf);
        let held_waiting = edit.state.pages.held();
        if let Some(hold) = hold {
            hold.release_unchanged();
        }
        (latched, waited, leaf_free, leaf_kept, held_waiting)
    };
    let done = !early && matches!(case.after_one(&mut edit), Ok(IOResult::Done(false)));
    let read_after = case.crash.completed() - reads;
    let units = space_units(&case.db, case.class);
    let expected = case.freed();
    let pages = case.pages.len();
    drop(edit);
    drop(case);
    assert!(!early, "the free was made before both pages were held");
    assert_eq!(held, pages, "pages held before the root was taken");
    assert!(latched, "the root's latch not free for the test");
    assert!(waited, "the free did not wait on the root held");
    assert!(leaf_free, "the leaf's latch kept while the free waited");
    assert!(leaf_kept, "the leaf evicted while the free waited");
    assert_eq!(held_waiting, pages, "pages held while the free waited");
    assert!(done, "the free not made once the root was let go");
    assert_eq!(read_after, 0, "reads after both pages were in memory");
    assert_eq!(units, expected, "the slots in use after the free");
}

/// A deferred free whose free space grows a level after its pages are ready and before it latches
/// them reads the height again under its latches, lets them go and goes round: it latches the new
/// root too and counts the free in it. Anchored on the occupancy before it, N, the free space then
/// lists exactly N - 1 slots, and the old and new roots count exactly those (the auditor's #4651
/// and #4653; mutant: no height read under the latches, so the new root still counts the slot).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_deferred_free_whose_free_space_grows_under_it_counts_the_free_in_the_new_root() {
    use std::cell::Cell;
    use std::rc::Rc;
    let case = slot_case();
    let grown = Rc::new(Cell::new(None));
    {
        let (store, io, grown, class) = (
            case.store.clone(),
            case.db.io.clone(),
            grown.clone(),
            case.class,
        );
        crate::race::at("store.slot_ready", move || {
            grown.set(Some(drive(&io, || store.grow(Tree::Space, class)).is_ok()));
        });
    }
    let mut edit = FreeEdit::default();
    let mut freed = None;
    for _ in 0..=case.pages.len() + 3 {
        match case.after_one(&mut edit) {
            Ok(IOResult::Done(none_left)) => {
                freed = Some(none_left);
                break;
            }
            Ok(IOResult::IO(_)) => case.settle(),
            Err(_) => break,
        }
    }
    let unreached = crate::race::pending("store.slot_ready");
    let height = case.store.height(Tree::Space, case.class);
    let units = space_units(&case.db, case.class);
    let (low, high) = (
        node_counts(&case.db, case.class, 1),
        node_counts(&case.db, case.class, 2),
    );
    let (before, expected) = (case.units.len(), case.freed());
    drop(edit);
    drop(case);
    assert!(!unreached, "the free never reached its pages ready");
    assert_eq!(
        grown.get(),
        Some(true),
        "the free space did not grow at the hook"
    );
    assert_eq!(height, 3, "the free space's height after the growth");
    assert_eq!(freed, Some(false), "the free not made");
    assert_eq!(units.len() + 1, before, "slots in use: N - 1 of N");
    assert_eq!(units, expected, "the slots in use after the free");
    assert_eq!(low, counts_of(&expected, 1), "the old root's counts");
    assert_eq!(high, counts_of(&expected, 2), "the new root's counts");
}

/// A copy slot's take whose free space grows a level after its mark's pages are ready and before
/// it latches them goes round the same way: the new root counts the slot. Anchored on the
/// occupancy before it, N, the free space then lists exactly N + 1 slots, and both roots count
/// exactly those. Its claim walk finds its pages in memory: a cold claim walk is slice 1c's (the
/// auditor's #4651 and #4653; mutant: no height read under the latches).
#[test]
fn a_copy_slot_whose_free_space_grows_under_its_mark_counts_it_in_the_new_root() {
    use std::cell::Cell;
    use std::rc::Rc;
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    let before = space_units(&db, class);
    let grown = Rc::new(Cell::new(None));
    {
        let (store, io, grown) = (store.clone(), db.io.clone(), grown.clone());
        crate::race::at("store.slot_ready", move || {
            grown.set(Some(drive(&io, || store.grow(Tree::Space, class)).is_ok()));
        });
    }
    // Called directly, so the hook's growth is an operation of its own rather than one inside it.
    let mut mark = MarkEdit::default();
    let slot = match store.take_copy_slot(class, &mut mark) {
        Ok(IOResult::Done(slot)) => Some(slot),
        _ => None,
    };
    let unreached = crate::race::pending("store.slot_ready");
    let height = store.height(Tree::Space, class);
    let units = space_units(&db, class);
    let (low, high) = (node_counts(&db, class, 1), node_counts(&db, class, 2));
    let claimed = store.claimed[class.index()].ones();
    let mut expected = before.clone();
    expected.extend(slot);
    expected.sort_unstable();
    drop(mark);
    drop(store);
    drop(db);
    assert!(!unreached, "the mark never reached its pages ready");
    assert_eq!(
        grown.get(),
        Some(true),
        "the free space did not grow at the hook"
    );
    assert_eq!(height, 3, "the free space's height after the growth");
    assert!(
        slot.is_some_and(|s| !before.contains(&s)),
        "the slot taken: {slot:?}, of {before:?} in use before"
    );
    assert_eq!(units.len(), before.len() + 1, "slots in use: N + 1 of N");
    assert_eq!(units, expected, "the slots in use after the take");
    assert_eq!(low, counts_of(&expected, 1), "the old root's counts");
    assert_eq!(high, counts_of(&expected, 2), "the new root's counts");
    assert!(claimed.is_empty(), "a claim left: {claimed:?}");
}

/// A copy slot's take whose mark waited on a latch another change held, every page of its path
/// held: its claim given back as it returned, its next call's walk goes on where it was and takes
/// the same slot again, over the same pages, reading none. The pages are still the take's as that
/// next claim is made: none can be evicted at that point. No claim is left (the auditor's #4651,
/// #4653 and #4666; M2b 1c: the walk keeps its place, so the slot is its own again; mutant: the
/// pages let go as the next claim is made).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_copy_slot_take_claiming_again_after_a_latch_keeps_its_pages_and_reads_none_again() {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::mpsc;
    const BOUND: std::time::Duration = std::time::Duration::from_secs(60);
    let (crash, db) = reads_at_once(2);
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    // Every metadata page written, so clean: a page let go could be evicted.
    db.checkpoint();
    db.checkpoint();
    let before = space_units(&db, class);
    let first = Rc::new(Cell::new(None));
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let holder = Rc::new(RefCell::new(None));
    {
        let (store, io, first, holder) =
            (store.clone(), db.io.clone(), first.clone(), holder.clone());
        crate::race::at("store.copy_slot_claimed", move || {
            let Some(&slot) = store.claimed[class.index()].ones().first() else {
                return;
            };
            first.set(Some(slot));
            let leaf = radix::path(Tree::Space, class, store.height(Tree::Space, class), slot)[0];
            let (held_tx, held_rx) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                let frame = drive(&io, || store.meta().get(leaf)).unwrap();
                let hold = frame.try_lock();
                let _ = held_tx.send(hold.is_some());
                let _ = go_rx.recv_timeout(BOUND);
                if let Some(hold) = hold {
                    hold.release_unchanged();
                }
            });
            let held = held_rx.recv_timeout(BOUND).unwrap_or(false);
            *holder.borrow_mut() = Some((thread, held));
        });
    }
    let mut mark = MarkEdit::default();
    let waited = matches!(store.take_copy_slot(class, &mut mark), Ok(IOResult::IO(_)));
    let held = mark.state.pages.held_ids();
    let left = store.claimed[class.index()].ones();
    let _ = go_tx.send(());
    let (joined, latched) = holder
        .borrow_mut()
        .take()
        .map_or((false, false), |(t, held)| (t.join().is_ok(), held));
    let evicted_at_claim = Rc::new(Cell::new(None));
    {
        let (store, evicted_at_claim) = (store.clone(), evicted_at_claim.clone());
        crate::race::at("store.copy_slot_claimed", move || {
            let Some(&slot) = store.claimed[class.index()].ones().first() else {
                return;
            };
            let path = radix::path(Tree::Space, class, store.height(Tree::Space, class), slot);
            let evicted = path.iter().filter(|&&id| store.meta().evict(id)).count();
            evicted_at_claim.set(Some(evicted));
        });
    }
    let reads = crash.completed();
    let next = match store.take_copy_slot(class, &mut mark) {
        Ok(IOResult::Done(slot)) => Some(slot),
        _ => None,
    };
    let read_after = crash.completed() - reads;
    let units = space_units(&db, class);
    let claimed = store.claimed[class.index()].ones();
    let first = first.get();
    let mut expected = before.clone();
    expected.extend(next);
    expected.sort_unstable();
    drop(mark);
    drop(store);
    drop(db);
    assert!(
        joined && latched,
        "(the latch thread joined, the leaf latched)"
    );
    assert!(waited, "the first call did not wait on the latch");
    assert_eq!(held.len(), 2, "pages held as the first call returned");
    assert!(
        left.is_empty(),
        "a claim left by the waiting call: {left:?}"
    );
    assert_eq!(next, first, "the next call's slot, of the first claim");
    assert_eq!(
        evicted_at_claim.get(),
        Some(0),
        "pages evicted as the next claim was made"
    );
    assert_eq!(read_after, 0, "reads by the next call");
    assert_eq!(units, expected, "the slots in use after the take");
    assert!(claimed.is_empty(), "a claim left: {claimed:?}");
}

/// A copy slot's take whose mark waited on a latch, under one read credit, keeps its pages across
/// its next calls' claims, whatever they return: when the next claim's walk moves past its leaf,
/// every free slot there now held for erasure, and waits on the next leaf's read, the root stays
/// held; when that read fails, the take ends with the error and lets every page go (the auditor's
/// #4651: the whole call is the edit's; M2b 1c; mutant: the claim outside the edit, so a claim's
/// error returns with the old mark's pages still held).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_copy_slot_take_whose_claim_fails_after_its_mark_waited_lets_its_pages_go() {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::mpsc;
    const BOUND: std::time::Duration = std::time::Duration::from_secs(60);
    let (crash, db) = one_read();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    db.checkpoint();
    db.checkpoint();
    let root = radix::root(Tree::Space, class, 2);
    let leaf = |index| MetaId {
        tree: Tree::Space,
        class,
        level: 0,
        index,
    };
    let (first, second) = (leaf(0), leaf(1));
    // The root and the first leaf held in memory by the test for the first call; the second leaf
    // out of memory.
    let warm: Vec<_> = [root, first]
        .iter()
        .map(|&id| drive(&db.io, || store.meta().get(id)).unwrap())
        .collect();
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    store.meta().evict(second);
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let holder = Rc::new(RefCell::new(None));
    {
        let (store, io, holder) = (store.clone(), db.io.clone(), holder.clone());
        crate::race::at("store.copy_slot_claimed", move || {
            let (held_tx, held_rx) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                let frame = drive(&io, || store.meta().get(first)).unwrap();
                let hold = frame.try_lock();
                let _ = held_tx.send(hold.is_some());
                let _ = go_rx.recv_timeout(BOUND);
                if let Some(hold) = hold {
                    hold.release_unchanged();
                }
            });
            let held = held_rx.recv_timeout(BOUND).unwrap_or(false);
            *holder.borrow_mut() = Some((thread, held));
        });
    }
    let mut mark = MarkEdit::default();
    let latch_wait = matches!(store.take_copy_slot(class, &mut mark), Ok(IOResult::IO(_)));
    let held_first = mark.state.pages.held_ids();
    let _ = go_tx.send(());
    let (joined, latched) = holder
        .borrow_mut()
        .take()
        .map_or((false, false), |(t, held)| (t.join().is_ok(), held));
    drop(warm);
    for u in 0..radix::SPACE_BITS {
        store.unerased[class.index()].set(u);
    }
    crash.set_faults(crate::io::crash::Faults {
        read: 1.0,
        ..Default::default()
    });
    // A leaf a poll: the first leaf scanned past, then the second's read.
    let scanned = match store.take_copy_slot(class, &mut mark) {
        Ok(IOResult::IO(IOCompletions(c))) => c.finished(),
        _ => false,
    };
    let second_wait = matches!(store.take_copy_slot(class, &mut mark), Ok(IOResult::IO(_)));
    let reading = crash.in_flight();
    let root_kept = !store.meta().evict(root);
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    crash.set_faults(Default::default());
    let failed = store.take_copy_slot(class, &mut mark).is_err();
    let (pending, held) = (mark.pending(), mark.state.pages.held());
    let root_let_go = store.meta().evict(root) || !store.meta().in_memory(root);
    drop(mark);
    drop(store);
    drop(db);
    assert!(
        joined && latched,
        "(the latch thread joined, the leaf latched)"
    );
    assert!(latch_wait, "the first call did not wait on the latch");
    assert_eq!(held_first.len(), 2, "pages held after the first call");
    assert!(
        scanned,
        "the next poll did not end its work with the first leaf"
    );
    assert!(
        second_wait,
        "the next claim did not wait on the second leaf's read"
    );
    assert_eq!(reading, 1, "reads with the backend as the claim waits");
    assert!(root_kept, "the root let go while the next claim waited");
    assert!(failed, "the take did not end with the claim's failed read");
    assert_eq!(
        (pending, held),
        (None, 0),
        "(the take under way, pages held)"
    );
    assert!(root_let_go, "the root still held after the take failed");
}

/// A copy slot's take of one class whose mark waited holding its page, followed by a take of
/// another class whose claim waits on a read, has let the first take's page go before that claim
/// runs: the first class's leaf is evictable while the second waits (the auditor's #4651; mutant:
/// only the mark under the edit, so a claim that waits returns before the edit sees the class).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_copy_slot_take_of_another_class_lets_the_first_classs_pages_go_before_its_claim() {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::mpsc;
    const BOUND: std::time::Duration = std::time::Duration::from_secs(60);
    let (crash, db) = reads_at_once(2);
    let store = db.store().clone();
    let (a, b) = (class(0), class(1));
    // A slot of class B taken first, so its free space's leaf is a page the checkpoints write.
    let mut setup = MarkEdit::default();
    drive(&db.io, || store.take_copy_slot(b, &mut setup)).unwrap();
    db.checkpoint();
    db.checkpoint();
    let leaf_of = |class: SizeClass| radix::path(Tree::Space, class, 1, 0)[0];
    let (leaf_a, leaf_b) = (leaf_of(a), leaf_of(b));
    // Class A's leaf held in memory by the test for its claim walk until class B's take; class
    // B's out, so its claim waits on a read.
    let warm_a = drive(&db.io, || store.meta().get(leaf_a)).unwrap();
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    store.meta().evict(leaf_b);
    let b_out = !store.meta().in_memory(leaf_b);
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let holder = Rc::new(RefCell::new(None));
    {
        let (store, io, holder) = (store.clone(), db.io.clone(), holder.clone());
        crate::race::at("store.slot_ready", move || {
            let (held_tx, held_rx) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                let frame = drive(&io, || store.meta().get(leaf_a)).unwrap();
                let hold = frame.try_lock();
                let _ = held_tx.send(hold.is_some());
                let _ = go_rx.recv_timeout(BOUND);
                if let Some(hold) = hold {
                    hold.release_unchanged();
                }
            });
            let held = held_rx.recv_timeout(BOUND).unwrap_or(false);
            *holder.borrow_mut() = Some((thread, held));
        });
    }
    let mut mark = MarkEdit::default();
    let waited_a = matches!(store.take_copy_slot(a, &mut mark), Ok(IOResult::IO(_)));
    let held_a = mark.state.pages.held_ids();
    let _ = go_tx.send(());
    let (joined, latched) = holder
        .borrow_mut()
        .take()
        .map_or((false, false), |(t, held)| (t.join().is_ok(), held));
    drop(warm_a);
    let waited_b = matches!(store.take_copy_slot(b, &mut mark), Ok(IOResult::IO(_)));
    let b_reading = crash.in_flight();
    let a_let_go = store.meta().evict(leaf_a) || !store.meta().in_memory(leaf_a);
    let pending = mark.pending();
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    let done_b = drive(&db.io, || store.take_copy_slot(b, &mut mark)).is_ok();
    let claimed = (
        store.claimed[a.index()].ones(),
        store.claimed[b.index()].ones(),
    );
    drop(mark);
    drop(store);
    drop(db);
    assert!(b_out, "class B's leaf in memory before its take");
    assert!(
        joined && latched,
        "(the latch thread joined, class A's leaf latched)"
    );
    assert!(waited_a, "class A's take did not wait on its leaf's latch");
    assert_eq!(held_a, vec![leaf_a], "pages held by class A's take");
    assert!(waited_b, "class B's claim did not wait on its leaf's read");
    assert_eq!(
        b_reading, 1,
        "reads with the backend as class B's claim waits"
    );
    assert!(a_let_go, "class A's leaf held while class B's claim waited");
    assert_eq!(
        pending,
        Some(b),
        "the take under way as class B's claim waits"
    );
    assert!(done_b, "class B's take not made");
    assert!(
        claimed.0.is_empty() && claimed.1.is_empty(),
        "claims left: {claimed:?}"
    );
}

/// A checkpoint's After phase over a deferred free whose leaf and root it finds out of memory,
/// under one read credit, waits on their reads across its units, the free kept at the front of
/// its list meanwhile, and then makes it: each page read once, the free space then lists exactly
/// the slots in use before it but the slot, and no free is owed (the auditor's #4650: the run
/// owns the free's edit).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_checkpoints_deferred_free_that_waits_on_its_reads_stays_owed_until_it_is_made() {
    use std::cell::Cell;
    use std::rc::Rc;
    let (crash, db) = one_read();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    let mut mark = MarkEdit::default();
    let slot = drive(&db.io, || store.take_copy_slot(class, &mut mark)).unwrap();
    db.checkpoint();
    db.checkpoint();
    let before = space_units(&db, class);
    let n = db.db.checkpointer().superblock().checkpoint + 1;
    store
        .deferred
        .lock()
        .entry(n)
        .or_default()
        .push_back(Deferred::Slot(class, slot));
    let path = radix::path(Tree::Space, class, 2, slot).to_vec();
    let out = Rc::new(Cell::new(None));
    let at_ready = Rc::new(Cell::new(None));
    {
        let (store, crash, out, at_ready, path) = (
            store.clone(),
            crash.clone(),
            out.clone(),
            at_ready.clone(),
            path.clone(),
        );
        crate::race::at("checkpoint.after_free", move || {
            drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
            for &id in &path {
                store.meta().evict(id);
            }
            out.set(Some(
                path.iter()
                    .filter(|&&id| !store.meta().in_memory(id))
                    .count(),
            ));
            let reads = crash.completed();
            crate::race::at("store.slot_ready", move || {
                let owed = store.deferred_slots().contains(&(class, slot));
                at_ready.set(Some((crash.completed() - reads, owed)));
            });
        });
    }
    db.checkpoint();
    let unreached = (
        crate::race::pending("checkpoint.after_free"),
        crate::race::pending("store.slot_ready"),
    );
    let owed = store.deferred_slots().contains(&(class, slot));
    let units = space_units(&db, class);
    let expected: Vec<u64> = before.iter().copied().filter(|&u| u != slot).collect();
    drop(mark);
    drop(store);
    drop(db);
    assert_eq!(
        unreached,
        (false, false),
        "(the After phase, the free's pages ready) unreached"
    );
    assert_eq!(
        out.get(),
        Some(2),
        "the free's pages out of memory as the After phase began"
    );
    assert_eq!(
        at_ready.get(),
        Some((2, true)),
        "(reads, the free owed) as its pages were ready"
    );
    assert!(!owed, "the free still owed after the checkpoint");
    assert!(
        before.contains(&slot),
        "the slot not in use before its free"
    );
    assert_eq!(units, expected, "the slots in use after the checkpoint");
}

/// Sets the counts below `children` of the metadata page `id` of a store, under the page's latch,
/// as a test's forging of a free space's occupancy.
fn forge_counts(db: &Db, id: MetaId, children: impl IntoIterator<Item = (u64, u64)>) {
    let store = db.store();
    let frame = drive(&db.io, || store.meta().get(id)).unwrap();
    let mut hold = frame.try_lock().expect("a forged page's latch free");
    for (child, used) in children {
        radix::set_used_below(hold.page(), child, used);
    }
    hold.commit(store.meta());
}

/// A copy slot's take over a free space two levels high whose root and leaf are both out of
/// memory, under one read credit, keeps its walk's place and pages across its waits: three polls,
/// two reads, no page of the path evictable after any poll or once its reads retire, and none let
/// go as the walk goes on where it waited. The cold control: both pages out of memory first (the
/// auditor's #4664 probe, whose walk without them alternates root and leaf for twelve polls; M2b
/// 1c; mutants: the walk's pages let go at a wait, the walk going down again from the root).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_copy_slot_take_over_a_cold_free_space_keeps_its_walk_and_reads_each_page_once() {
    use std::cell::Cell;
    use std::rc::Rc;
    let (crash, db) = one_read();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    db.checkpoint();
    db.checkpoint();
    let cursor = store.cursors[class.index()].load(crate::sync::atomic::Ordering::Relaxed);
    let pages = radix::path(Tree::Space, class, 2, cursor).to_vec();
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    for &id in &pages {
        store.meta().evict(id);
    }
    let out = pages
        .iter()
        .filter(|&&id| !store.meta().in_memory(id))
        .count();
    let settle = || {
        while crash.in_flight() > 0 {
            crash.step().unwrap();
        }
        drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    };
    let before = crash.completed();
    let mut mark = MarkEdit::default();
    let (mut polls, mut evicted) = (0, Vec::new());
    let resumed_evicted = Rc::new(Cell::new(None));
    let slot = loop {
        polls += 1;
        match store.take_copy_slot(class, &mut mark) {
            Ok(IOResult::Done(slot)) => break Some(slot),
            Ok(IOResult::IO(_)) => {}
            Err(_) => break None,
        }
        // No page of the path is in memory but by the take: any evicted is one it let go.
        for &id in &pages {
            if store.meta().evict(id) {
                evicted.push(id);
            }
        }
        settle();
        for &id in &pages {
            if store.meta().evict(id) {
                evicted.push(id);
            }
        }
        // As the next poll aims again, before it looks: its pages are still the take's.
        if polls == 2 {
            let (store, pages, resumed_evicted) =
                (store.clone(), pages.clone(), resumed_evicted.clone());
            crate::race::at("store.walk_aimed", move || {
                let n = pages.iter().filter(|&&id| store.meta().evict(id)).count();
                resumed_evicted.set(Some(n));
            });
        }
        if polls >= 12 {
            break None;
        }
    };
    let reads = crash.completed() - before;
    drop(mark);
    drop(store);
    drop(db);
    assert_eq!(out, 2, "the path's pages out of memory before the take");
    assert!(
        evicted.is_empty(),
        "pages the take held evicted: {evicted:?}"
    );
    assert_eq!(
        resumed_evicted.get(),
        Some(0),
        "pages evicted as the walk went on where it waited"
    );
    assert_eq!(
        (slot.is_some(), polls, reads),
        (true, 3, 2),
        "(a slot taken, polls, metadata reads)"
    );
}

/// A copy slot's take whose walk moves past a leaf with nothing takeable, every free slot in it
/// still to be erased, and waits on the next leaf's read, goes on from that leaf: the leaf it
/// moved past, let go and evicted meanwhile, is not read again, and the take claims the next
/// leaf's first slot (M2b 1c, its first finding; mutant: the walk's place not kept, so the next
/// poll starts at the cursor again).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_copy_slot_take_whose_walk_waits_past_a_leaf_goes_on_from_there() {
    let (crash, db) = one_read();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    db.checkpoint();
    db.checkpoint();
    let cursor = store.cursors[class.index()].load(crate::sync::atomic::Ordering::Relaxed);
    let root = radix::root(Tree::Space, class, 2);
    let leaf = |index| MetaId {
        tree: Tree::Space,
        class,
        level: 0,
        index,
    };
    let (first, second) = (leaf(0), leaf(1));
    for u in cursor..radix::SPACE_BITS {
        store.unerased[class.index()].set(u);
    }
    // The root and the first leaf held in memory by the test; the second out of it.
    let warm: Vec<_> = [root, first]
        .iter()
        .map(|&id| drive(&db.io, || store.meta().get(id)).unwrap())
        .collect();
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    store.meta().evict(second);
    let second_out = !store.meta().in_memory(second);
    let mut mark = MarkEdit::default();
    // A leaf a poll: the first leaf scanned, the poll's work done; then the second leaf's read.
    let scanned = match store.take_copy_slot(class, &mut mark) {
        Ok(IOResult::IO(IOCompletions(c))) => c.finished(),
        _ => false,
    };
    let waited = matches!(store.take_copy_slot(class, &mut mark), Ok(IOResult::IO(_)))
        && crash.in_flight() == 1;
    drop(warm);
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    let first_let_go = store.meta().evict(first) || !store.meta().in_memory(first);
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    let reads = crash.completed();
    let taken = match store.take_copy_slot(class, &mut mark) {
        Ok(IOResult::Done(slot)) => Some(slot),
        _ => None,
    };
    let read_after = crash.completed() - reads;
    let first_out = !store.meta().in_memory(first);
    drop(mark);
    drop(store);
    drop(db);
    assert!(second_out, "the second leaf in memory before the take");
    assert!(
        scanned,
        "the first poll did not end its work with the first leaf"
    );
    assert!(waited, "the take did not wait on the second leaf's read");
    assert!(
        first_let_go,
        "the first leaf held after the walk moved past it"
    );
    assert_eq!(taken, Some(radix::SPACE_BITS), "the slot taken");
    assert_eq!(read_after, 0, "reads by the take's next poll");
    assert!(first_out, "the first leaf read again");
}

/// A free space three levels high, its cursor in the root's first subtree, that subtree's room
/// all before the cursor and none of it takeable (every free slot of its first leaf still to be
/// erased, its other leaves counted full): a copy slot's take goes on past the subtree to the next
/// one, takes its first slot, and the free space does not grow. Wrapping round from the
/// subtree's node instead spends the walk's one wrap there, finds nothing, and grows the free
/// space a level each time until it grows no further (M2b 1c, its second finding; mutant: the
/// wrap from an internal node).
#[test]
fn a_copy_slot_take_goes_on_past_a_subtree_with_no_room_after_its_cursor() {
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    store.cursors[class.index()].store(radix::SPACE_BITS, crate::sync::atomic::Ordering::Relaxed);
    for u in 0..radix::SPACE_BITS {
        store.unerased[class.index()].set(u);
    }
    let first_subtree = MetaId {
        tree: Tree::Space,
        class,
        level: 1,
        index: 0,
    };
    forge_counts(
        &db,
        first_subtree,
        (1..radix::FANOUT).map(|c| (c, radix::SPACE_BITS)),
    );
    let mut mark = MarkEdit::default();
    let taken = drive(&db.io, || store.take_copy_slot(class, &mut mark)).ok();
    let height = store.height(Tree::Space, class);
    drop(mark);
    drop(store);
    drop(db);
    assert_eq!(
        (taken, height),
        (Some(radix::span(Tree::Space, 1)), 3),
        "(the slot taken, the free space's height)"
    );
}

/// The same layout for a swap slot's take and for a page number's allocation, whose walks are
/// their call's alone: each goes on past the subtree with no room after its cursor, and neither
/// tree grows (M2b 1c; mutant: the wrap from an internal node).
#[test]
fn a_swap_slot_and_a_page_number_go_on_past_a_subtree_with_no_room_after_their_cursor() {
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    for tree in [Tree::Space, Tree::Map] {
        drive(&db.io, || store.grow(tree, class)).unwrap();
        drive(&db.io, || store.grow(tree, class)).unwrap();
    }
    store.cursors[class.index()].store(radix::SPACE_BITS, crate::sync::atomic::Ordering::Relaxed);
    store.numbers[class.index()].store(radix::MAP_ENTRIES, crate::sync::atomic::Ordering::Relaxed);
    for u in 0..radix::SPACE_BITS {
        store.unerased[class.index()].set(u);
    }
    for n in 0..radix::MAP_ENTRIES {
        store.unreusable[class.index()].set(n);
    }
    for tree in [Tree::Space, Tree::Map] {
        let subtree = MetaId {
            tree,
            class,
            level: 1,
            index: 0,
        };
        let units = radix::leaf_units(tree);
        forge_counts(&db, subtree, (1..radix::FANOUT).map(|c| (c, units)));
    }
    let swap = drive(&db.io, || store.take_swap_slot(class)).ok();
    let number = drive(&db.io, || store.allocate(class))
        .ok()
        .map(|p| p.number());
    let heights = (
        store.height(Tree::Space, class),
        store.height(Tree::Map, class),
    );
    drop(store);
    drop(db);
    assert_eq!(
        (swap, number, heights),
        (
            Some(radix::span(Tree::Space, 1)),
            Some(radix::span(Tree::Map, 1)),
            (3, 3)
        ),
        "(the swap slot, the page number, the trees' heights)"
    );
}

/// A free space `MAX_HEIGHT` levels high whose units a metadata id names are all counted in use:
/// a copy slot's take answers the full free space's error, and no walk looks for a page past the
/// ones a metadata id names, which the cache would refuse (M2b 1c, its third finding; mutant: no
/// bound on the children a walk may go down to).
#[test]
fn a_free_space_whose_nameable_units_are_all_in_use_is_full() {
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    for _ in 1..MAX_HEIGHT {
        drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    }
    let height = store.height(Tree::Space, class);
    let root = radix::root(Tree::Space, class, height);
    let span = radix::span(Tree::Space, height - 2);
    let nameable_children = radix::nameable(Tree::Space).div_ceil(span);
    forge_counts(&db, root, (0..nameable_children).map(|c| (c, span)));
    let mut mark = MarkEdit::default();
    let refused = match drive(&db.io, || store.take_copy_slot(class, &mut mark)) {
        Err(e) => Some(e.to_string()),
        Ok(_) => None,
    };
    drop(mark);
    // The partly nameable child counted with room, its own node's nameable children all in use:
    // the bound holds below the root too, relative to the node.
    forge_counts(&db, root, [(nameable_children - 1, 0)]);
    let partial = MetaId {
        tree: Tree::Space,
        class,
        level: height - 2,
        index: nameable_children - 1,
    };
    let below = radix::span(Tree::Space, height - 3);
    let partial_first = (nameable_children - 1) * span;
    let partial_children = (radix::nameable(Tree::Space) - partial_first).div_ceil(below);
    forge_counts(&db, partial, (0..partial_children).map(|c| (c, below)));
    let mut mark = MarkEdit::default();
    let refused_below = match drive(&db.io, || store.take_copy_slot(class, &mut mark)) {
        Err(e) => Some(e.to_string()),
        Ok(_) => None,
    };
    drop(mark);
    drop(store);
    drop(db);
    assert!(
        partial_children > 0 && partial_children < radix::FANOUT,
        "the partly nameable node's nameable children: {partial_children}"
    );
    let refused_below = refused_below.expect("a take from a full free space, below the root");
    assert!(
        refused_below.contains("grows no further"),
        "the take's error below the root: {refused_below}"
    );
    assert_eq!(height, MAX_HEIGHT, "the free space's height");
    assert_eq!(
        nameable_children, 3,
        "the root's children a metadata id reaches"
    );
    let refused = refused.expect("a take from a full free space");
    assert!(
        refused.contains("grows no further"),
        "the take's error: {refused}"
    );
}

/// A copy slot's take over a free space whose free slots are all held for erasure, every page in
/// memory, scans one node or leaf a poll, its place and its one wrap kept across the polls: two
/// leaves past the cursor, the root with no room past them, the wrap, the two leaves and the root
/// again, then the free space full at its height, so it grows a level, and the take claims the
/// first slot of the new subtree. Seven yields, eight polls (the auditor's #4681 W2 and #4685: a
/// stated quantum, one descent a poll; mutants: no quantum for a leaf, so one poll walks every
/// leaf; a growth not charged; the wrap forgotten between polls, so the walk wraps again and
/// never ends).
#[test]
fn a_copy_slot_take_scans_a_leaf_a_poll_and_wraps_once() {
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    store.cursors[class.index()].store(0, crate::sync::atomic::Ordering::Relaxed);
    for u in 0..2 * radix::SPACE_BITS {
        store.unerased[class.index()].set(u);
    }
    let root = radix::root(Tree::Space, class, 2);
    forge_counts(
        &db,
        root,
        (2..radix::FANOUT).map(|c| (c, radix::SPACE_BITS)),
    );
    let mut mark = MarkEdit::default();
    let (mut polls, mut yields) = (0, 0);
    let taken = loop {
        polls += 1;
        match store.take_copy_slot(class, &mut mark) {
            Ok(IOResult::Done(slot)) => break Some(slot),
            Ok(IOResult::IO(IOCompletions(c))) if c.finished() => yields += 1,
            _ => break None,
        }
        if polls >= 12 {
            break None;
        }
    };
    let height = store.height(Tree::Space, class);
    drop(mark);
    drop(store);
    drop(db);
    assert_eq!(
        (taken, height, polls, yields),
        (Some(radix::capacity(Tree::Space, 2)), 3, 8, 7),
        "(the slot taken, the free space's height, polls, yields)"
    );
}

/// A copy slot's take over a full free space one level high, every page out of memory, under one
/// read credit: its walk finds none, and its growth's new root is read while the take holds the old
/// one. Once that read retires, neither root can be evicted; the take's next poll grows the free
/// space once, its walk going on at the new height, reads the new leaf, and takes its first slot:
/// three reads, one growth (the auditor's #4681 W3; mutant: the growth's pages let go at its wait,
/// as `get_all` lets them go).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_copy_slot_take_whose_free_space_grows_keeps_both_roots_across_the_growths_read() {
    let (crash, db) = one_read();
    let store = db.store().clone();
    let class = class(0);
    db.checkpoint();
    db.checkpoint();
    for u in 0..radix::SPACE_BITS {
        store.unerased[class.index()].set(u);
    }
    let (old_root, new_root) = (
        radix::root(Tree::Space, class, 1),
        radix::root(Tree::Space, class, 2),
    );
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    store.meta().evict(old_root);
    let out = !store.meta().in_memory(old_root) && !store.meta().in_memory(new_root);
    let settle = || {
        while crash.in_flight() > 0 {
            crash.step().unwrap();
        }
        drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    };
    let before = crash.completed();
    let mut mark = MarkEdit::default();
    let (mut polls, mut evicted_at_growth) = (0, None);
    let taken = loop {
        polls += 1;
        match store.take_copy_slot(class, &mut mark) {
            Ok(IOResult::Done(slot)) => break Some(slot),
            Ok(IOResult::IO(_)) => {}
            Err(_) => break None,
        }
        // The growth's read made: the new root's frame in its cell, its read with the backend.
        let growing = store.meta().in_memory(new_root);
        settle();
        if growing && evicted_at_growth.is_none() {
            let n = [old_root, new_root]
                .iter()
                .filter(|&&id| store.meta().evict(id))
                .count();
            evicted_at_growth = Some(n);
        }
        if polls >= 12 {
            break None;
        }
    };
    let reads = crash.completed() - before;
    let height = store.height(Tree::Space, class);
    drop(mark);
    drop(store);
    drop(db);
    assert!(out, "the roots in memory before the take");
    assert_eq!(
        evicted_at_growth,
        Some(0),
        "roots evicted as the growth's read retired"
    );
    assert_eq!(
        (taken, height, reads),
        (Some(radix::SPACE_BITS), 2, 3),
        "(the slot taken, the free space's height, metadata reads)"
    );
}

/// A walk that found the free space's last searchable unit, whose claim another then took, wraps
/// to the start and goes down again from the root: the leaf it found the unit in no longer holds
/// its place, so the first leaf's free units are found (the auditor's #4684; mutant: the node the
/// walk stopped at kept across the wrap, so it scans that leaf alone again and finds the free
/// space full).
#[test]
fn a_walk_whose_last_unit_is_claimed_by_another_wraps_from_the_root() {
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    let end = radix::searchable(Tree::Space, 2);
    let last_leaf = radix::FANOUT - 1;
    // In the last leaf only its last unit is takeable; the first leaf is free.
    for u in last_leaf * radix::SPACE_BITS..end - 1 {
        store.unerased[class.index()].set(u);
    }
    let root = radix::root(Tree::Space, class, 2);
    forge_counts(&db, root, (1..last_leaf).map(|c| (c, radix::SPACE_BITS)));
    let (mut walk, mut pages) = (Walk::default(), PageSet::default());
    walk.from = Some(end - 1);
    let found = match store.walk(Tree::Space, class, true, &mut walk, &mut pages, true) {
        Ok(IOResult::Done(found)) => found,
        _ => None,
    };
    // Another claims it first, and the claim goes on past it as `claim_slot` does.
    store.claimed[class.index()].set(end - 1);
    walk.from = Some(end);
    let (mut polls, mut again) = (0, None);
    while polls < 4 {
        polls += 1;
        match store.walk(Tree::Space, class, true, &mut walk, &mut pages, true) {
            Ok(IOResult::Done(found)) => {
                again = Some(found);
                break;
            }
            Ok(IOResult::IO(_)) => {}
            Err(_) => break,
        }
    }
    drop(pages);
    drop(store);
    drop(db);
    assert_eq!(found, Some(end - 1), "the unit the first walk found");
    assert_eq!(
        (again, polls),
        (Some(Some(0)), 1),
        "(what the walk found once the claim was lost, polls)"
    );
}

/// A copy slot's take whose free space's root counts room in children whose own counts show none,
/// as a walk reads them when other changes fill those children between its reads, scans one such
/// node a poll: three nodes passed, a yield each, and the fourth's first slot taken in the fourth
/// poll (the auditor's #4685; mutant: a node with nothing past the place goes down again from the
/// root in the same poll, so one poll passes every node).
#[test]
fn a_copy_slot_take_scans_a_node_a_poll_when_counts_above_it_promised_room() {
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    store.cursors[class.index()].store(0, crate::sync::atomic::Ordering::Relaxed);
    for index in 0..3 {
        let node = MetaId {
            tree: Tree::Space,
            class,
            level: 1,
            index,
        };
        forge_counts(
            &db,
            node,
            (0..radix::FANOUT).map(|c| (c, radix::SPACE_BITS)),
        );
    }
    let mut mark = MarkEdit::default();
    let (mut polls, mut yields) = (0, 0);
    let taken = loop {
        polls += 1;
        match store.take_copy_slot(class, &mut mark) {
            Ok(IOResult::Done(slot)) => break Some(slot),
            Ok(IOResult::IO(IOCompletions(c))) if c.finished() => yields += 1,
            _ => break None,
        }
        if polls >= 12 {
            break None;
        }
    };
    let height = store.height(Tree::Space, class);
    drop(mark);
    drop(store);
    drop(db);
    assert_eq!(
        (taken, height, polls, yields),
        (Some(3 * radix::span(Tree::Space, 1)), 3, 4, 3),
        "(the slot taken, the free space's height, polls, yields)"
    );
}

/// A copy slot's take whose claim another takes first, between its walk finding the slot and its
/// claim, ends the poll there with a yield, and the next poll goes on in the same leaf and claims
/// the next slot (the auditor's #4685: a lost claim charged as a restart; mutant: the claim goes
/// on to search again in the same poll).
#[test]
fn a_copy_slot_take_whose_claim_is_lost_yields_and_goes_on_in_the_same_leaf() {
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    store.cursors[class.index()].store(0, crate::sync::atomic::Ordering::Relaxed);
    {
        let store = store.clone();
        crate::race::at("store.slot_found", move || {
            store.claimed[class.index()].set(0);
        });
    }
    let mut mark = MarkEdit::default();
    let (mut polls, mut yields) = (0, 0);
    let taken = loop {
        polls += 1;
        match store.take_copy_slot(class, &mut mark) {
            Ok(IOResult::Done(slot)) => break Some(slot),
            Ok(IOResult::IO(IOCompletions(c))) if c.finished() => yields += 1,
            _ => break None,
        }
        if polls >= 12 {
            break None;
        }
    };
    drop(mark);
    drop(store);
    drop(db);
    assert_eq!(
        (taken, polls, yields),
        (Some(1), 2, 1),
        "(the slot taken, polls, yields)"
    );
}

/// A copy slot's take whose walk passes children the root counts full and waits on the read of the
/// next, under one read credit, goes on at that child: its place moved to the child's start as
/// the walk went down, the child holds it, so the next poll aims the child and the root and
/// neither can be evicted as it does, and nothing is read again (M2b 1c, with #4684's rule that a
/// node holds the place it saves; mutant: the place left behind as the walk goes down, so the
/// next poll goes down again from the root, the child let go and read again).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_copy_slot_take_waiting_at_a_child_past_full_siblings_goes_on_there() {
    use std::cell::Cell;
    use std::rc::Rc;
    let (crash, db) = one_read();
    let store = db.store().clone();
    let class = class(0);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    store.cursors[class.index()].store(0, crate::sync::atomic::Ordering::Relaxed);
    let root = radix::root(Tree::Space, class, 2);
    forge_counts(&db, root, (0..2).map(|c| (c, radix::SPACE_BITS)));
    db.checkpoint();
    db.checkpoint();
    let child = MetaId {
        tree: Tree::Space,
        class,
        level: 0,
        index: 2,
    };
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    store.meta().evict(child);
    let out = !store.meta().in_memory(child);
    let settle = || {
        while crash.in_flight() > 0 {
            crash.step().unwrap();
        }
        drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    };
    let before = crash.completed();
    let mut mark = MarkEdit::default();
    let resumed_evicted = Rc::new(Cell::new(None));
    let mut polls = 0;
    let taken = loop {
        polls += 1;
        match store.take_copy_slot(class, &mut mark) {
            Ok(IOResult::Done(slot)) => break Some(slot),
            Ok(IOResult::IO(_)) => {}
            Err(_) => break None,
        }
        settle();
        // As the next poll aims, before it looks: the child is still the take's.
        if polls == 1 {
            let (store, resumed_evicted) = (store.clone(), resumed_evicted.clone());
            crate::race::at("store.walk_aimed", move || {
                let n = [child, root]
                    .iter()
                    .filter(|&&id| store.meta().evict(id))
                    .count();
                resumed_evicted.set(Some(n));
            });
        }
        if polls >= 12 {
            break None;
        }
    };
    let reads = crash.completed() - before;
    drop(mark);
    drop(store);
    drop(db);
    assert!(out, "the child out of memory before the take");
    assert_eq!(
        resumed_evicted.get(),
        Some(0),
        "pages evicted as the walk went on at the child"
    );
    assert_eq!(
        (taken, polls, reads),
        (Some(2 * radix::SPACE_BITS), 2, 1),
        "(the slot taken, polls, metadata reads)"
    );
}

/// A metadata page whose first unit is past the last a metadata id names is refused, as an
/// error, before its cell is made: a free-space leaf past `INDEX_BITS`, and a level-5 node with no
/// nameable unit below it, whose index, 3, is far below that, beside the last level-5 node that
/// has one (the third finding's guard, made exact when a walk without its bound reached a level-1
/// node near `1 << 46` and its cell was asked for; the table it reads is checked for every tree
/// and level in `radix`; mutant: the guard by index alone, so the level-5 node is made and read).
#[test]
fn a_metadata_page_with_no_nameable_unit_is_refused_as_a_leaf_and_as_a_node() {
    let db = Db::open();
    let store = db.store().clone();
    let class = class(0);
    let at = |level: u8, index: u64| MetaId {
        tree: Tree::Space,
        class,
        level,
        index,
    };
    let first_past =
        |level: u8| radix::nameable(Tree::Space).div_ceil(radix::span(Tree::Space, level));
    let refused = |id: MetaId| match drive(&db.io, || store.meta().get(id)) {
        Err(e) => e.to_string().contains("past the pages a metadata id names"),
        Ok(_) => false,
    };
    let results = (
        refused(at(5, first_past(5) - 1)),
        refused(at(5, first_past(5))),
        refused(at(0, first_past(0))),
        first_past(5),
    );
    drop(store);
    drop(db);
    assert_eq!(
        results,
        (false, true, true, 3),
        "(the last level-5 node with a nameable unit refused, the first without, the first leaf \
         past, the first level-5 index past)"
    );
}
