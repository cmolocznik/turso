//! The page store (audit 18): numbers and slots freed only once a checkpoint lets them go, slots
//! erased before they are taken again, and a latched metadata page making its users yield.

use super::*;
use crate::io::MemoryIO;
use crate::storage::atoll::buffer::{min_budget_beside, BufferConfig};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::{ring_bytes, LogConfig};
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
        let db = Atoll::open(io.clone(), BASE, config()).unwrap();
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
    let waited = matches!(store.take_copy_slot(class), Ok(IOResult::IO(_)));
    let left = store.claimed[class.index()].ones();
    let _ = go_tx.send(());
    let (joined, held) = holder
        .borrow_mut()
        .take()
        .map_or((false, false), |(t, held)| (t.join().is_ok(), held));
    let next = drive(&db.io, || store.take_copy_slot(class)).is_ok();
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

/// A copy slot claimed by a step that then waits on a read of the free-space leaf it marks, the
/// leaf evicted and its read kept by the backend, is given back as the step returns; the read in,
/// the next call takes a slot and leaves no claim (the auditor's #4578; mutant: the claim kept on
/// the wait's return, as before).
#[test]
fn a_copy_slot_claimed_by_a_step_waiting_on_a_metadata_read_is_given_back() {
    use std::cell::Cell;
    use std::rc::Rc;
    let crash = Arc::new(crate::io::crash::CrashIO::new(0));
    let db = Db::open_on(crash.clone());
    let store = db.store().clone();
    // A class nothing has used: its free-space pages read in clean, held by no one, so the
    // step's search finds them in memory and its leaf may then be evicted.
    let class = class(1);
    let path = radix::path(Tree::Space, class, store.height(Tree::Space, class), 0);
    drop(drive(&db.io, || store.meta().get_all(&path)).unwrap());
    // The last read's request, kept for a test to cancel, holds its page through its callback.
    drop(crate::storage::atoll::meta::LAST_READ.with(|r| r.borrow_mut().take()));
    let evicted = Rc::new(Cell::new(None));
    {
        let (store, evicted) = (store.clone(), evicted.clone());
        crate::race::at("store.copy_slot_claimed", move || {
            let claimed = store.claimed[class.index()].ones();
            let Some(&slot) = claimed.first() else { return };
            let leaf = radix::path(Tree::Space, class, store.height(Tree::Space, class), slot)[0];
            evicted.set(Some(store.meta().evict(leaf)));
        });
    }
    let before = crash.in_flight();
    let waited = matches!(store.take_copy_slot(class), Ok(IOResult::IO(_)));
    let reading = crash.in_flight() > before;
    let left = store.claimed[class.index()].ones();
    let next = drive(&db.io, || store.take_copy_slot(class)).is_ok();
    let left_after = store.claimed[class.index()].ones();
    // Every assertion with the database closed.
    drop((store, db));
    assert_eq!(
        evicted.get(),
        Some(true),
        "the step's claim was not reached, or the free-space leaf not evicted"
    );
    assert!(
        waited && reading,
        "the step did not wait on the leaf's read with the backend"
    );
    assert!(
        left.is_empty(),
        "the step waiting on the read left its claim behind: {left:?}"
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
    let submits = Arc::new(AtomicUsize::new(0));
    let callbacks = Arc::new(AtomicUsize::new(0));
    let buffer = Arc::new(crate::io::Buffer::new_temporary(class.size()));
    let raced = before_hold.map(|located| {
        let callbacks = callbacks.clone();
        let submit = || {
            submits.fetch_add(1, Ordering::Relaxed);
        };
        located.read(
            buffer.clone(),
            Box::new(move |_| {
                callbacks.fetch_add(1, Ordering::Relaxed);
            }),
            &submit,
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
    let no_submission =
        submits.load(Ordering::Relaxed) == 0 && callbacks.load(Ordering::Relaxed) == 0;
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
            let submit = || {
                submits.fetch_add(1, Ordering::Relaxed);
            };
            matches!(
                located.read(
                    buffer.clone(),
                    Box::new(move |_| {
                        callbacks.fetch_add(1, Ordering::Relaxed);
                    }),
                    &submit
                ),
                Ok(IOResult::Done(None))
            )
        }
        _ => false,
    };
    let done_once = callbacks.load(Ordering::Relaxed) == 1 && submits.load(Ordering::Relaxed) == 0;
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
        io.clone(),
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
    let done = drive(&db.io, || db.store().copied(n, &heights, pid, slot)).ok();
    let root = radix::root(Tree::Space, class, 2);
    let leaf = radix::path(Tree::Space, class, 2, slot)[0];
    let counts = (
        cut_count(&db, n, root, 0),
        cut_count(&db, n, root, 1),
        cut_bit(&db, n, leaf, slot),
    );
    drop(db);
    let (edited, space) = done.expect("the edit failed");
    assert_eq!(space, 2, "the cut's free-space height the edit ended at");
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
    let done = drive(&db.io, || db.store().copied(n, &heights, pid, slot)).ok();
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
    let (_, space) = done.expect("the edit failed");
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
    drive(&db.io, || store.copied(first, &heights, pid, old)).unwrap();
    store.meta().end_cut(first, &[]);
    let n = first + 1;
    let (_, heights) = store.begin_cut(n);
    drive(&db.io, || store.grow(Tree::Space, class)).unwrap();
    let slot = radix::SPACE_BITS + 9;
    let done = drive(&db.io, || store.copied(n, &heights, pid, slot)).ok();
    let root = radix::root(Tree::Space, class, 2);
    let leaf = radix::root(Tree::Space, class, 1);
    let counts = (
        cut_count(&db, n, root, 0),
        cut_count(&db, n, root, 1),
        cut_bit(&db, n, leaf, old),
    );
    let owed = store.deferred_slots().contains(&(class, old));
    drop(db);
    let (_, space) = done.expect("the edit failed");
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
            io.clone(),
            base,
            StoreConfig::default(),
            [[1; CLASS_COUNT]; TREES],
            0,
        );
        let class = class(0);
        let pid = drive(&io, || store.allocate(class)).unwrap();
        let mut last = None;
        for _ in 0..radix::SPACE_BITS {
            last = Some(drive(&io, || store.take_copy_slot(class)).unwrap());
        }
        if grow_before_cut {
            drive(&io, || store.grow(Tree::Space, class)).unwrap();
        }
        let (dirty, mut heights) = store.begin_cut(1);
        let mut ids: BTreeSet<_> = dirty.collect();
        let slot = drive(&io, || store.take_copy_slot(class)).unwrap();
        let (edited, space) = drive(&io, || store.copied(1, &heights, pid, slot)).unwrap();
        ids.extend(edited);
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
        let reopened = PageStore::open(io.clone(), base, StoreConfig::default(), heights, 1);
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
