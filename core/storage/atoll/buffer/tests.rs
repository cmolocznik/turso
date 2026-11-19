use super::*;
use crate::io::crash::CrashIO;
use crate::io::{OpenFlags, IO};
use crate::storage::atoll::page::HEADER_SIZE;
use crate::storage::atoll::shared::{Bytes, BytesMut};
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::{backends, drive, Backend};
#[cfg(not(shuttle))]
use crate::storage::atoll::testing::{step_past, HoldAt};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

fn open(b: &Backend, budget: usize) -> Arc<BufferPool> {
    open_on(&b.io, &b.base, budget)
}

fn open_on(io: &Arc<dyn IO>, base: &str, budget: usize) -> Arc<BufferPool> {
    open_with(io, base, BufferConfig::with_budget(budget))
}

fn open_with(io: &Arc<dyn IO>, base: &str, config: BufferConfig) -> Arc<BufferPool> {
    let store = PageStore::open(
        crate::storage::atoll::requests::counted_for_test(io.clone()),
        base,
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        0,
    );
    BufferPool::new(config, Arc::new(LocalSource::new(store.clone())), store).unwrap()
}

/// Writes `version` into `page` beside its id: after the header, and at its end, so a copy torn
/// or mixed up with another page's reads wrong.
fn stamp<P: BytesMut + ?Sized>(page: &mut P, pid: PageId, version: u64) {
    page.put_u64(HEADER_SIZE, pid.raw());
    page.put_u64(HEADER_SIZE + 8, version);
    let end = page.len();
    page.put_u64(end - 8, version);
}

/// The version stamped in `page`, if it is stamped as `pid` and whole.
fn stamped<P: Bytes + ?Sized>(page: &P, pid: PageId) -> Option<u64> {
    let version = page.u64_at(HEADER_SIZE + 8);
    (page.u64_at(HEADER_SIZE) == pid.raw() && page.u64_at(page.len() - 8) == version)
        .then_some(version)
}

/// One page of the model: its id, the version it should hold and the hint kept beside it.
struct Page {
    pid: PageId,
    version: u64,
    hint: AtomicU32,
}

struct Model<'a> {
    io: &'a Arc<dyn IO>,
    pool: &'a Arc<BufferPool>,
    pages: Vec<Page>,
}

impl Model<'_> {
    fn allocate(&mut self, class: SizeClass) {
        let mut g = drive(self.io, || self.pool.allocate(class)).unwrap();
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, 1);
        g.changed();
        let hint = AtomicU32::new(g.hint());
        drop(g);
        self.pages.push(Page {
            pid,
            version: 1,
            hint,
        });
    }

    fn update(&mut self, i: usize) {
        let (pid, version) = (self.pages[i].pid, self.pages[i].version + 1);
        let mut g = drive(self.io, || {
            self.pool.fix_exclusive(pid, Some(&self.pages[i].hint))
        })
        .unwrap();
        assert_eq!(
            stamped(g.page(), pid),
            Some(version - 1),
            "{pid:?} before an update"
        );
        stamp(&mut g.page_mut(), pid, version);
        g.changed();
        drop(g);
        self.pages[i].version = version;
    }

    fn check(&self, i: usize, optimistic: bool) {
        let p = &self.pages[i];
        let seen = if optimistic {
            loop {
                let g = drive(self.io, || self.pool.fix_optimistic(p.pid, Some(&p.hint))).unwrap();
                if let Some(seen) = g.read(|b| stamped(b, p.pid)) {
                    break seen;
                }
            }
        } else {
            let g = drive(self.io, || self.pool.fix_exclusive(p.pid, Some(&p.hint))).unwrap();
            let seen = stamped(g.page(), p.pid);
            drop(g.release_unchanged());
            seen
        };
        assert_eq!(seen, Some(p.version), "{:?} read back wrong", p.pid);
    }

    fn churn(&mut self, rng: &mut ChaCha8Rng, ops: usize) {
        for _ in 0..ops {
            let i = rng.random_range(0..self.pages.len());
            match rng.random_range(0..4) {
                0 | 1 => self.update(i),
                2 => self.check(i, false),
                _ => self.check(i, true),
            }
        }
    }

    fn check_all(&self) {
        for i in 0..self.pages.len() {
            self.check(i, i % 2 == 0);
        }
    }
}

#[test]
fn pages_keep_their_contents_through_eviction_writeback_and_moves_between_classes() {
    for b in backends() {
        let pool = open(&b, MIN_BUDGET);
        let mut model = Model {
            io: &b.io,
            pool: &pool,
            pages: Vec::new(),
        };
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        // 600 pages of 32 KiB: nearly twice the pool, all of it taken by class 3.
        for _ in 0..600 {
            model.allocate(class(3));
        }
        model.churn(&mut rng, 2000);
        assert_eq!(
            pool.held()[3],
            MIN_BUDGET,
            "{}: class 3 holds the pool",
            b.name
        );
        let overflow = 600 * class(3).size() as u64 - MIN_BUDGET as u64;
        assert!(
            pool.evicted.load(Ordering::Relaxed) >= overflow,
            "{}: evicted {} bytes, less than the {overflow} that cannot fit",
            b.name,
            pool.evicted.load(Ordering::Relaxed)
        );
        // 3000 pages of 4 KiB, used as often each as the large ones: per byte they are worth
        // more, so class 0 takes memory from class 3.
        for _ in 0..3000 {
            model.allocate(class(0));
        }
        model.churn(&mut rng, 6000);
        let held = pool.held();
        assert!(
            held[0] > held[3],
            "{}: class 0 holds {} bytes, class 3 {}",
            b.name,
            held[0],
            held[3]
        );
        model.check_all();
        // A sweep leaves no page dirty.
        let mut sweep = pool.sweep();
        drive(&b.io, || sweep.step(&pool)).unwrap();
        let dirty = (0..pool.memory.grains())
            .filter(|&f| pool.header(f).status() == RESIDENT && pool.header(f).dirty())
            .count();
        assert_eq!(dirty, 0, "{}: pages left dirty by the sweep", b.name);
        // Dropped pages free their frames; the rest still read back.
        let kept = model.pages.split_off(model.pages.len() / 2);
        for p in std::mem::replace(&mut model.pages, kept) {
            drive(&b.io, || {
                match pool.fix_exclusive(p.pid, Some(&p.hint))? {
                    IOResult::Done(page) => pool.free(page),
                    IOResult::IO(io) => Ok(IOResult::IO(io)),
                }
            })
            .unwrap();
        }
        model.check_all();
        let census = pool.taken_frames_census();
        drop(model);
        drop(pool);
        assert!(
            census.is_empty(),
            "{}: the census of taken frames at the end: {census:#?}",
            b.name
        );
    }
}

/// Eviction writes a dirty page out of its frame (one memory): pages changed at random, twice
/// what the pool holds, are written to their swap copies and evicted with no copy made of any to
/// write from, and read back as changed. A writeback's record for tests goes once it ends.
#[test]
fn eviction_writes_dirty_pages_out_of_their_frames_with_no_copy() {
    for b in backends() {
        let pool = open(&b, MIN_BUDGET);
        let mut model = Model {
            io: &b.io,
            pool: &pool,
            pages: Vec::new(),
        };
        let mut rng = ChaCha8Rng::seed_from_u64(11);
        for _ in 0..600 {
            model.allocate(class(3));
        }
        model.churn(&mut rng, 2000);
        assert!(
            pool.swap_written() > 0,
            "{}: no page written to a swap copy",
            b.name
        );
        assert_eq!(
            pool.staging.copies(),
            0,
            "{}: a copy made to write a page from",
            b.name
        );
        model.check_all();
        let mut sweep = pool.sweep();
        drive(&b.io, || sweep.step(&pool)).unwrap();
        assert!(
            pool.stepped.lock().is_empty(),
            "{}: records of writebacks that ended",
            b.name
        );
    }
}

#[test]
fn concurrent_fixes_lose_no_change() {
    const THREADS: usize = 4;
    const OPS: usize = 3000;
    for b in backends() {
        let pool = open(&b, MIN_BUDGET);
        let mut model = Model {
            io: &b.io,
            pool: &pool,
            pages: Vec::new(),
        };
        // More 4 KiB pages than the pool's 2,560 frames.
        for _ in 0..4000 {
            model.allocate(class(0));
        }
        let pages = &model.pages;
        let counts: Vec<Vec<u64>> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..THREADS)
                .map(|t| {
                    let model = &model;
                    s.spawn(move || {
                        let mut rng = ChaCha8Rng::seed_from_u64(t as u64);
                        let mut counts = vec![0u64; pages.len()];
                        for _ in 0..OPS {
                            let i = rng.random_range(0..pages.len());
                            let p = &pages[i];
                            if rng.random_bool(0.5) {
                                let mut g = drive(model.io, || {
                                    model.pool.fix_exclusive(p.pid, Some(&p.hint))
                                })
                                .unwrap();
                                let n = stamped(g.page(), p.pid).expect("a whole page") + 1;
                                stamp(&mut g.page_mut(), p.pid, n);
                                g.changed();
                                counts[i] += 1;
                            } else {
                                loop {
                                    let g = drive(model.io, || {
                                        model.pool.fix_optimistic(p.pid, Some(&p.hint))
                                    })
                                    .unwrap();
                                    if let Some(seen) = g.read(|b| stamped(b, p.pid)) {
                                        assert!(seen.is_some(), "{:?} read torn", p.pid);
                                        break;
                                    }
                                }
                            }
                        }
                        counts
                    })
                })
                .collect();
            workers.into_iter().map(|w| w.join().unwrap()).collect()
        });
        for (i, p) in model.pages.iter_mut().enumerate() {
            p.version = 1 + counts.iter().map(|c| c[i]).sum::<u64>();
        }
        model.check_all();
    }
}

/// A page whose write was in flight when a sweep began may have changed since its copy was taken,
/// by a change made before the sweep: the sweep waits for the write, and writes the page again.
#[test]
fn a_sweep_writes_again_a_page_changed_while_its_earlier_write_was_in_flight() {
    // Writes complete only at a step, so one stays in flight while the page changes.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(3));
    let pool = open_on(&io, "/db/sweep", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        pool: &pool,
        pages: Vec::new(),
    };
    model.allocate(class(0));
    let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
    // Written back the way eviction does: marked, then stepped until the write is submitted.
    pool.header(frame).flags.fetch_or(WRITING, Ordering::AcqRel);
    let written = pool.write_back(class(0), vec![frame], false).unwrap();
    while let Some(c) = pool.drive_writebacks().unwrap() {
        io.wait_for_completion(c).unwrap();
    }
    assert!(!written.finished(), "the write is in flight");
    model.update(0);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    assert!(
        !pool.header(frame).dirty(),
        "a page changed while its write was in flight was not written again"
    );
    model.check_all();
}

/// A checkpoint settles a page at the change count it imaged. A page changed past that count, and
/// written back since, has its newest copy in its swap slot: it keeps that copy, and evicted, reads
/// back as it stands.
#[test]
fn a_page_settled_at_a_count_it_has_moved_past_keeps_its_newer_swap_copy() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/settled", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        pool: &pool,
        pages: Vec::new(),
    };
    model.allocate(class(0));
    let pid = model.pages[0].pid;
    let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
    let imaged = pool.header(frame).changes.load(Ordering::Acquire);
    model.update(0);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    assert!(
        pool.store.swap_slot(pid).is_some(),
        "written to a swap copy"
    );
    assert!(
        matches!(pool.settled(pid, imaged).unwrap(), IOResult::Done(())),
        "a resident frame settles at once"
    );
    assert!(
        pool.store.swap_slot(pid).is_some(),
        "the newer swap copy was given up"
    );
    assert!(pool.evict_clean(pid), "the page left its frame");
    model.check(0, false);
}

/// VALUE-LOST1: a page settled at the count of its swap copy while no frame holds it gives the
/// copy up, as it would with a frame: its checkpoint copy stands for it, and no later cut finds it
/// owed.
#[test]
fn a_page_settled_with_no_frame_at_its_swap_copys_count_gives_the_copy_up() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/settled-unloaded", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        pool: &pool,
        pages: Vec::new(),
    };
    model.allocate(class(0));
    let pid = model.pages[0].pid;
    let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let written = pool.header(frame).written.load(Ordering::Acquire);
    let evicted = pool.evict_clean(pid);
    let swapped = pool.store.swap_slot(pid).is_some();
    let settled = matches!(pool.settled(pid, written).unwrap(), IOResult::Done(()));
    let kept = pool.store.swap_slot(pid).is_some();
    drop(model);
    drop(pool);
    assert!(evicted, "the page left its frame");
    assert!(swapped, "written to a swap copy");
    assert!(settled, "a page with no frame settles at once");
    assert!(!kept, "the swap copy outlived the settle");
}

/// A page settled with no frame at a count it has moved past keeps its newer swap copy, written by
/// the eviction that took its frame, and reads back as it stands.
#[test]
fn a_page_settled_with_no_frame_at_a_count_it_has_moved_past_keeps_its_newer_swap_copy() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/settled-moved", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        pool: &pool,
        pages: Vec::new(),
    };
    model.allocate(class(0));
    let pid = model.pages[0].pid;
    let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
    let imaged = pool.header(frame).changes.load(Ordering::Acquire);
    model.update(0);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let evicted = pool.evict_clean(pid);
    let settled = matches!(pool.settled(pid, imaged).unwrap(), IOResult::Done(()));
    let kept = pool.store.swap_slot(pid).is_some();
    let read = stamp_read(&io, &pool, pid);
    let version = model.pages[0].version;
    drop(model);
    drop(pool);
    assert!(evicted, "the page left its frame");
    assert!(settled, "a page with no frame settles at once");
    assert!(kept, "the newer swap copy was given up");
    assert_eq!(read, Ok(Some(version)), "the page read back");
}

/// A settle that finds no frame, then a load of the page beginning before the settle gives the swap
/// copy up: the load claims its frame before it asks the swap table, so the settle, looking again
/// under the swap table's lock, sees it and keeps the copy the load reads. It waits on the load's
/// read, as a fix of a loading page does, then settles the page the load brought in.
#[test]
fn a_settle_gives_up_no_swap_copy_a_load_begun_after_its_look_reads() {
    // Reads complete only at a step, so the load's stays in flight across the settle.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(3));
    let pool = open_on(&io, "/db/settle-load", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        pool: &pool,
        pages: Vec::new(),
    };
    model.allocate(class(0));
    let pid = model.pages[0].pid;
    let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let written = pool.header(frame).written.load(Ordering::Acquire);
    let evicted = pool.evict_clean(pid);
    let slot = pool.store.swap_slot(pid);
    let loading = std::rc::Rc::new(std::cell::Cell::new(false));
    let (began, loader) = (loading.clone(), pool.clone());
    crate::race::at("buffer.settle_found_no_frame", move || {
        let fix = loader.fix_exclusive(pid, None).unwrap();
        began.set(matches!(fix, IOResult::IO(_)));
    });
    let wait = match pool.settled(pid, written).unwrap() {
        IOResult::IO(IOCompletions(c)) => Some(c),
        IOResult::Done(()) => None,
    };
    let waited_unfinished = wait.as_ref().map(|c| !c.finished());
    let kept_while_loading = pool.store.swap_slot(pid);
    let read = stamp_read(&io, &pool, pid);
    let wait_ended = wait.as_ref().map(|c| c.finished());
    drive(&io, || pool.settled(pid, written)).unwrap();
    let after = pool.store.swap_slot(pid);
    let header = pool
        .table
        .find(&pool.frames.headers, pid.raw())
        .map(|f| pool.header(f));
    let owed =
        header.map(|h| h.imaged.load(Ordering::Acquire) != h.changes.load(Ordering::Acquire));
    let version = model.pages[0].version;
    drop(model);
    drop(pool);
    assert!(evicted, "the page left its frame");
    assert!(slot.is_some(), "written to a swap copy");
    assert!(loading.get(), "the load began, its read in flight");
    assert!(
        wait.is_some(),
        "the settle went on while the load was in flight"
    );
    assert_eq!(
        waited_unfinished,
        Some(true),
        "the settle's wait was a yield, not the load's read"
    );
    assert_eq!(
        wait_ended,
        Some(true),
        "the load landed and its read's wait did not end"
    );
    assert_eq!(
        kept_while_loading, slot,
        "the swap copy the load reads was given up"
    );
    assert_eq!(read, Ok(Some(version)), "the page the load read");
    assert_eq!(after, None, "the swap copy outlived the second settle");
    assert_eq!(
        owed,
        Some(false),
        "the page loaded is owed after its settle"
    );
}

/// The stamp `pid` reads back with, fixed exclusively, or the fix's error.
fn stamp_read(
    io: &Arc<dyn IO>,
    pool: &Arc<BufferPool>,
    pid: PageId,
) -> Result<Option<u64>, String> {
    drive(io, || pool.fix_exclusive(pid, None))
        .map(|g| {
            let seen = stamped(g.page(), pid);
            drop(g.release_unchanged());
            seen
        })
        .map_err(|e| e.to_string())
}

/// Marks `pid` retired, as a versioned write that replaces its value does.
fn retire(store: &Arc<PageStore>, pid: PageId) {
    store.retire(pid);
}

/// A retired page freed while its writeback is in flight (DESIGN.md §6.5): the free waits for the
/// write to land, whose remap would otherwise bring the page back, and the page ends free,
/// unmarked and in no frame.
#[test]
fn a_retired_page_freed_with_its_write_in_flight_stays_free_after_the_write_lands() {
    // Writes complete only at a step, so the page's write stays in flight across the first try.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(3));
    let pool = open_on(&io, "/db/retired", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        pool: &pool,
        pages: Vec::new(),
    };
    model.allocate(class(0));
    let pid = model.pages[0].pid;
    let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
    retire(&pool.store, pid);
    // Written back the way eviction does: marked, then stepped until the write is submitted.
    pool.header(frame).flags.fetch_or(WRITING, Ordering::AcqRel);
    let written = pool.write_back(class(0), vec![frame], false).unwrap();
    while let Some(c) = pool.drive_writebacks().unwrap() {
        io.wait_for_completion(c).unwrap();
    }
    assert!(!written.finished(), "the write is in flight");
    assert!(
        matches!(pool.free_unreferenced(pid).unwrap(), IOResult::IO(_)),
        "freed while its write was in flight"
    );
    drive(&io, || pool.free_unreferenced(pid)).unwrap();
    drive(&io, || pool.settle_writes()).unwrap();
    assert!(written.finished(), "the write never landed");
    assert_eq!(
        drive(&io, || pool.store.entry(pid)).unwrap(),
        crate::storage::atoll::radix::FREE,
        "the write brought the page back"
    );
    assert!(
        pool.store.swap_slot(pid).is_none(),
        "the write named a swap copy of a page freed"
    );
    assert!(!pool.store.retired(pid), "still marked retired");
    assert!(
        pool.table.find(&pool.frames.headers, pid.raw()).is_none(),
        "a frame still holds it"
    );
}

/// A batch drained for another class empties even while a page in it changes faster than it can
/// be written: the page is written out, unavailable until its write lands, rather than written
/// back and kept because it changed meanwhile.
#[test]
fn a_drain_empties_its_batch_while_a_page_in_it_keeps_changing() {
    // Writes complete only at a step, so a page can change while its write is in flight.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(5));
    let pool = open_on(&io, "/db/drain", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        pool: &pool,
        pages: Vec::new(),
    };
    // Five 2 MiB pages fill the pool: a 4 KiB page needs one of their batches drained.
    for _ in 0..5 {
        model.allocate(class(9));
    }
    // A write lands within a few steps (the intent's flush, then the page); a drain that keeps
    // its page because it changed never ends at all.
    let mut allocated = None;
    for _ in 0..1000 {
        if let IOResult::Done(page) = pool.allocate(class(0)).unwrap() {
            allocated = Some(page.pid());
            break;
        }
        // Every 2 MiB page still resident changes, if it can at once, before the step that
        // lands writes: the one in the batch being drained is a page that keeps changing.
        for i in 0..model.pages.len() {
            let p = &model.pages[i];
            if pool.table.find(&pool.frames.headers, p.pid.raw()).is_none() {
                continue;
            }
            if let IOResult::Done(mut g) = pool.fix_exclusive(p.pid, Some(&p.hint)).unwrap() {
                let version = p.version + 1;
                stamp(&mut g.page_mut(), p.pid, version);
                g.changed();
                drop(g);
                model.pages[i].version = version;
            }
        }
        io.step().unwrap();
    }
    assert!(
        allocated.is_some(),
        "the drained batch never emptied while one of its pages kept changing"
    );
    model.check_all();
}

/// A write-out holds its frame through the frames, never the pool, so a pool dropped with one
/// still queued is freed rather than kept alive by its own queue.
#[test]
fn a_write_out_queued_when_the_pool_is_dropped_does_not_keep_it() {
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(5));
    let pool = open_on(&io, "/db/dropped", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        pool: &pool,
        pages: Vec::new(),
    };
    model.allocate(class(9));
    let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
    drop(model);
    pool.drain(class(9), frame / memory::GRAINS).unwrap();
    assert_eq!(pool.header(frame).status(), OUTGOING, "not written out");
    let weak = Arc::downgrade(&pool);
    drop(pool);
    assert!(
        weak.upgrade().is_none(),
        "a queued write-out kept the pool alive"
    );
}

#[test]
fn a_failed_upgrade_leaves_the_holders_latch_alone() {
    let b = &backends()[0];
    let pool = open(b, MIN_BUDGET);
    let page = drive(&b.io, || pool.allocate(class(0))).unwrap();
    let pid = page.pid();
    drop(page);
    let first = drive(&b.io, || pool.fix_optimistic(pid, None)).unwrap();
    let second = drive(&b.io, || pool.fix_optimistic(pid, None)).unwrap();
    let held = first.upgrade().expect("the first upgrade");
    assert!(second.upgrade().is_none(), "two upgrades from one version");
    assert!(
        matches!(pool.fix_exclusive(pid, None).unwrap(), IOResult::IO(_)),
        "a second hold got in while the page was held"
    );
    drop(held);
    assert!(matches!(
        pool.fix_exclusive(pid, None).unwrap(),
        IOResult::Done(_)
    ));
}

#[test]
fn a_drain_chosen_before_its_batch_changed_class_leaves_the_new_class_alone() {
    for b in backends() {
        let pool = open(&b, MIN_BUDGET);
        let mut model = Model {
            io: &b.io,
            pool: &pool,
            pages: Vec::new(),
        };
        for _ in 0..8 {
            model.allocate(class(2));
        }
        // Written back, so a wrong eviction drops them at once rather than writing them first.
        let mut sweep = pool.sweep();
        drive(&b.io, || sweep.step(&pool)).unwrap();
        let frame = |p: &Page| p.hint.load(Ordering::Relaxed) - 1;
        let batch = frame(&model.pages[0]) / memory::GRAINS;
        // A drain of the batch as class 0's, chosen before it went to class 2.
        pool.drain(class(0), batch).unwrap();
        for p in &model.pages {
            assert_eq!(
                pool.table.find(&pool.frames.headers, p.pid.raw()),
                Some(frame(p)),
                "{}: {:?} left its frame",
                b.name,
                p.pid
            );
        }
        assert_eq!(pool.memory.take(class(0)), None, "{}", b.name);
        model.check_all();
    }
}

/// The log's ring is a lend batch taken as the pool opens, from the highest free batch outside the
/// core (DESIGN.md 9.1.1): no cache claim takes a frame of it however full the cache is, it is in
/// no class's index, so no drain chooses it, and a run around it is not drainable. Measured before
/// lend batches: a long transaction's commit, needing frames of another class for its checkpoint's
/// images, drained the ring's batch, the emptiest of its class, and stalled.
#[test]
fn a_lend_batch_taken_at_open_is_never_drained_and_no_cache_claim_takes_it() {
    for b in backends() {
        let pool = open_with(
            &b.io,
            &b.base,
            BufferConfig {
                lent_at_open: LentAtOpen {
                    ring: Some((class(2), 4)),
                    bank: 0,
                },
                ..BufferConfig::with_budget(MIN_KEEPING_BUDGET)
            },
        );
        let top = pool.memory.batches - 1;
        assert!(
            pool.memory.is_lend_batch(top),
            "{}: the ring's batch",
            b.name
        );
        let ring: Vec<_> = (0..4).map(|_| pool.lend_ring(class(2)).unwrap()).collect();
        // Cache claims until one must wait for room: claims alone, as no page's metadata, whose
        // load a backend may defer, has any part in which frame a claim takes.
        let mut frames = Vec::new();
        while let IOResult::Done(frame) = pool.claim(class(0), Purpose::New, &[]).unwrap() {
            assert_ne!(
                frame / memory::GRAINS,
                top,
                "{}: a cache claim took a frame of the ring's batch",
                b.name
            );
            frames.push(frame);
        }
        assert!(!frames.is_empty(), "{}: no claim was served", b.name);
        assert!(
            !pool.drainable(class(CLASS_COUNT - 1), top),
            "{}: a run around the ring's batch drained",
            b.name
        );
        assert_eq!(
            pool.memory.emptiest(class(2), class(0), |_| true),
            None,
            "{}: the ring's batch is in its class's index",
            b.name
        );
        for f in frames {
            pool.free_frame(class(0), f, pool.take_free_latch(f));
        }
        let census = pool.taken_frames_census();
        drop(ring);
        drop(pool);
        assert!(
            census.is_empty(),
            "{}: the census with the ring's frames lent: {census:#?}",
            b.name
        );
    }
}

#[test]
fn temporary_pages_go_to_their_files_and_never_to_a_checkpoint() {
    for b in backends() {
        let pool = open(&b, MIN_BUDGET);
        let read = |pid: PageId| loop {
            let g = drive(&b.io, || pool.fix_optimistic(pid, None)).unwrap();
            if let Some(seen) = g.read(|p| {
                (
                    stamped(p, pid),
                    p.copied(0..p.len()).iter().all(|&x| x == 0),
                )
            }) {
                break seen;
            }
        };
        // 600 pages of 32 KiB, nearly twice the pool: most are written to the temporary file.
        let mut pages = Vec::new();
        for v in 1..=600u64 {
            let mut g = drive(&b.io, || pool.allocate_temporary(class(3))).unwrap();
            let pid = g.pid();
            stamp(&mut g.page_mut(), pid, v);
            g.changed();
            pages.push((pid, v));
        }
        for &(pid, v) in &pages {
            assert_eq!(read(pid).0, Some(v), "{}: {pid:?} read back wrong", b.name);
        }
        let overflow = 600 * class(3).size() as u64 - MIN_BUDGET as u64;
        assert!(
            pool.evicted.load(Ordering::Relaxed) >= overflow,
            "{}",
            b.name
        );
        // A sweep leaves them dirty, and a checkpoint's cut finds none of them owed an image: a
        // page of the store changed beside them is the one it finds.
        let mut g = drive(&b.io, || pool.allocate(class(0))).unwrap();
        let stored = g.pid();
        stamp(&mut g.page_mut(), stored, 1);
        g.changed();
        drop(g);
        let mut scan = CutScan::default();
        assert!(matches!(pool.begin_cut(1, &mut scan), IOResult::Done(())));
        pool.end_cut(1);
        assert_eq!(
            scan.owed,
            vec![stored],
            "{}: what the cut found owed",
            b.name
        );
        let mut sweep = pool.sweep();
        drive(&b.io, || sweep.step(&pool)).unwrap();
        let dirty = (0..pool.memory.grains())
            .filter(|&f| {
                let h = pool.header(f);
                h.status() == RESIDENT && h.dirty() && PageId::from_raw(h.pid()).is_temporary()
            })
            .count();
        assert!(dirty > 0, "{}: a sweep wrote temporary pages", b.name);
        // A freed number comes back as a new page, zeros, even once evicted unchanged.
        let freed: Vec<PageId> = pages.drain(..100).map(|(pid, _)| pid).collect();
        for &pid in &freed {
            drive(&b.io, || match pool.fix_exclusive(pid, None)? {
                IOResult::Done(page) => pool.free(page),
                IOResult::IO(io) => Ok(IOResult::IO(io)),
            })
            .unwrap();
        }
        let mut again: Vec<PageId> = (0..100)
            .map(|_| {
                drive(&b.io, || pool.allocate_temporary(class(3)))
                    .unwrap()
                    .pid()
            })
            .collect();
        again.sort_unstable();
        let mut sorted = freed.clone();
        sorted.sort_unstable();
        assert_eq!(
            again, sorted,
            "{}: freed numbers were not handed out again",
            b.name
        );
        for &(pid, v) in &pages {
            assert_eq!(read(pid).0, Some(v), "{}: {pid:?}", b.name);
        }
        for &pid in &again {
            assert!(read(pid).1, "{}: {pid:?} reads its earlier life", b.name);
        }
        // Opening again deletes the files.
        let path = format!("{}.t3", b.base);
        let file = b.io.open_file(&path, OpenFlags::Create, false).unwrap();
        assert!(file.size().unwrap() > 0, "{}: nothing was written", b.name);
        drop((file, pool));
        let _pool = open(&b, MIN_BUDGET);
        let file = b.io.open_file(&path, OpenFlags::Create, false).unwrap();
        assert_eq!(
            file.size().unwrap(),
            0,
            "{}: the temporary file survived",
            b.name
        );
    }
}

/// A page and the version last written to it, if one is allocated.
type Slot = Mutex<Option<(PageId, u64)>>;

/// Three threads allocate, change, read and free pages of 1, 2 and 4 MiB, two of each, in a pool
/// of five 2 MiB batches: they need 14 MiB, so the pool evicts and writes back, and memory moves
/// between the classes, a 4 MiB frame taking an aligned run of two batches. Each page reads back
/// the version last written to it.
fn concurrent_pages(ops: usize, seed: u64) {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/schedules", MIN_BUDGET);
    // Pages and the versions written to them, behind one lock per page: a thread changing a page
    // holds its entry, so the version it writes is the one it records.
    let pages: Arc<Vec<Slot>> = Arc::new((0..6).map(|_| Mutex::new(None)).collect());
    let mut handles = Vec::new();
    for t in 0..3u64 {
        let (io, pool, pages) = (io.clone(), pool.clone(), pages.clone());
        handles.push(crate::thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(seed * 10 + t);
            for _ in 0..ops {
                let i = rng.random_range(0..pages.len());
                let Some(mut slot) = pages[i].try_lock() else {
                    continue;
                };
                let class = class(8 + i % 3);
                match (*slot, rng.random_range(0..4)) {
                    (None, _) => {
                        let mut g = named_in(&io, "allocate", Some(&pool), || pool.allocate(class));
                        let pid = g.pid();
                        stamp(&mut g.page_mut(), pid, 1);
                        g.changed();
                        *slot = Some((pid, 1));
                    }
                    (Some((pid, version)), 0) => {
                        let mut g = named_in(&io, "fix_exclusive", Some(&pool), || {
                            pool.fix_exclusive(pid, None)
                        });
                        assert_eq!(stamped(g.page(), pid), Some(version));
                        stamp(&mut g.page_mut(), pid, version + 1);
                        g.changed();
                        *slot = Some((pid, version + 1));
                    }
                    (Some((pid, _)), 1) => {
                        named_in(&io, "free", Some(&pool), || {
                            match pool.fix_exclusive(pid, None)? {
                                IOResult::Done(page) => pool.free(page),
                                IOResult::IO(io) => Ok(IOResult::IO(io)),
                            }
                        });
                        *slot = None;
                    }
                    (Some((pid, version)), _) => loop {
                        let g = named_in(&io, "fix_optimistic", Some(&pool), || {
                            pool.fix_optimistic(pid, None)
                        });
                        if let Some(seen) = g.read(|p| stamped(p, pid)) {
                            assert_eq!(seen, Some(version), "{pid:?} read back wrong");
                            break;
                        }
                    },
                }
            }
        }));
    }
    // The checkpointer's part: checkpoints of the metadata, each cut while allocations and
    // frees change it.
    {
        let io = io.clone();
        handles.push(crate::thread::spawn(move || {
            for _ in 0..ops {
                checkpoint_meta(&io, &pool.store);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

/// Drives `op` like [`drive`], and names it in a panic once it has been tried 100,000 times: a
/// schedule that livelocks says where.
fn named<T>(io: &Arc<dyn IO>, name: &str, op: impl FnMut() -> Result<IOResult<T>>) -> T {
    named_in(io, name, None, op)
}

/// Tries after which an operation that has not finished is reported as livelocked.
const TRIES: u64 = 100_000;

/// Reports an operation that has tried [`TRIES`] times: by default it fails the test. With
/// ATOLL_PARK_ON_LIVELOCK set, the thread stops here instead, so a stall catcher can take every
/// thread's stack while the livelock stands. With ATOLL_SLOW_GOES_ON, it says so and goes on: a
/// run that then ends was slow, not stuck.
fn too_many_tries(state: String) {
    if std::env::var_os("ATOLL_PARK_ON_LIVELOCK").is_some() {
        eprintln!("{state}\nparked");
        loop {
            std::thread::park();
        }
    }
    if std::env::var_os("ATOLL_SLOW_GOES_ON").is_some() {
        eprintln!("{state}\ngoing on");
    } else {
        panic!("{state}");
    }
}

/// [`named`], showing `pool`'s state in the panic too.
fn named_in<T>(
    io: &Arc<dyn IO>,
    name: &str,
    pool: Option<&BufferPool>,
    mut op: impl FnMut() -> Result<IOResult<T>>,
) -> T {
    // Tries handed back something already done (a yield), and something to wait on.
    let (mut tries, mut ready) = (0u64, 0u64);
    drive(io, || {
        tries += 1;
        if tries == TRIES {
            too_many_tries(format!(
                "{name} tried {tries} times, {ready} handed back a yield\n{}",
                pool.map_or(String::new(), BufferPool::describe)
            ));
        }
        let r = op();
        // A slow operation that ends says so, with the steps under way then: the one it waited
        // on has left.
        if tries >= TRIES && matches!(r, Ok(IOResult::Done(_))) {
            eprintln!(
                "{name} done after {tries} tries; {}",
                pool.map_or(String::new(), |p| p.store.epochs().describe())
            );
        }
        if let Ok(IOResult::IO(IOCompletions(c))) = &r {
            ready += c.finished() as u64;
        }
        r
    })
    .unwrap()
}

#[cfg(not(shuttle))]
#[test]
fn pages_survive_concurrent_fixes_evictions_and_frees() {
    for seed in 0..20 {
        concurrent_pages(200, seed);
    }
}

#[cfg(shuttle)]
#[test]
fn pages_survive_concurrent_fixes_evictions_and_frees_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || concurrent_pages(12, crate::storage::atoll::testing::workload_seed()),
        300,
    );
}

/// A page written back to be evicted afterwards is evicted only if it is still the page in the
/// frame: once its write ends, another thread may evict it and put a new page in the frame,
/// which the writer must not evict in its stead.
fn written_page_eviction() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/written", MIN_BUDGET);
    let class = class(9);
    // Five 2 MiB frames fill the pool; four stay latched, so the page written back is the only
    // one another thread can evict.
    let mut held = Vec::new();
    for v in 0..5u64 {
        let mut g = named(&io, "allocate", || pool.allocate(class));
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, v);
        g.changed();
        held.push(g);
    }
    let written = held.remove(0);
    let frame = written.hint() - 1;
    drop(written);
    pool.header(frame)
        .flags
        .fetch_or(WRITING | EVICT_AFTER, Ordering::AcqRel);
    drop(pool.write_back(class, vec![frame], true));
    let writer = {
        let (io, pool) = (io.clone(), pool.clone());
        crate::thread::spawn(move || {
            // As `drive` does: a yield gives the processor up, or a thread spinning here can keep
            // one holding what it waits on from ever running, as a priority scheduler did.
            while let Some(c) = pool.drive_writebacks().unwrap() {
                crate::thread::yield_now();
                io.wait_for_completion(c).unwrap();
            }
        })
    };
    let allocator = {
        let (io, pool) = (io.clone(), pool.clone());
        crate::thread::spawn(move || drop(named(&io, "allocate", || pool.allocate(class))))
    };
    writer.join().unwrap();
    allocator.join().unwrap();
    drop(held);
}

#[cfg(not(shuttle))]
#[test]
fn a_written_page_is_evicted_only_while_its_write_pins_it() {
    for _ in 0..200 {
        written_page_eviction();
    }
}

#[cfg(shuttle)]
#[test]
fn a_written_page_is_evicted_only_while_its_write_pins_it_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(written_page_eviction, 300);
}

/// A sweep waiting for its own writeback wakes when another thread finishes it. A writeback
/// finishes after its last write settles, so a sweep that looks in between, while the other thread
/// holds the writeback, waits on the pool: only the finish can wake it.
#[test]
fn a_sweep_waiting_on_its_writeback_wakes_when_another_thread_finishes_it() {
    // Writes stay in flight until the IO steps, so the sweep cannot settle its own on submitting.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(1));
    let pool = open_on(&io, "/db/woken", MIN_BUDGET);
    let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
    let pid = g.pid();
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    drop(g);
    // The sweep claims the page and submits its write, which stays in flight until the IO steps.
    let mut sweep = pool.sweep();
    while pool.writes_in_flight.load(Ordering::Acquire) == 0 {
        match sweep.step(&pool).unwrap() {
            IOResult::IO(IOCompletions(c)) => io.wait_for_completion(c).unwrap(),
            IOResult::Done(()) => panic!("the sweep wrote nothing"),
        }
    }
    // Another thread settles the write; the sweep steps once its last write has settled and before
    // the writeback finishes, and finds the writeback held.
    let waited: Arc<Mutex<Option<Completion>>> = Arc::new(Mutex::new(None));
    pool.with_queued(|wb| {
        wb.before_finish = Some({
            let (pool, waited) = (pool.clone(), waited.clone());
            Box::new(move || match sweep.step(&pool).unwrap() {
                IOResult::IO(IOCompletions(c)) => *waited.lock() = Some(c),
                IOResult::Done(()) => panic!("the sweep finished before its writeback"),
            })
        })
    });
    drive(&io, || pool.settle_writes()).unwrap();
    let waited = waited.lock().take().expect("the sweep stepped");
    assert!(
        waited.finished(),
        "the sweep waits on a wakeup no one sends once its writeback has finished"
    );
}

/// A sweep writing back more pages than the writeback buffer holds copies of writes what fits,
/// takes room again as writes land, and writes every page: none is left dirty, and each reads
/// back as written. The writes stay in flight until the IO steps, so the buffer fills.
#[test]
fn a_sweep_writes_back_more_pages_than_the_writeback_buffer_holds() {
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(3));
    let pool = open_with(
        &io,
        "/db/room",
        BufferConfig {
            budget: MIN_BUDGET,
            writeback_bytes: page::MAX_PAGE_SIZE,
            kept_bytes: None,
            lent_at_open: Default::default(),
        },
    );
    let room = page::MAX_PAGE_SIZE / class(0).size();
    let mut pids = Vec::new();
    for v in 0..(room + room / 8) as u64 {
        let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, v);
        g.changed();
        pids.push((pid, v));
    }
    assert!(pids.len() > room, "the pages fit the buffer");
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let dirty = (0..pool.memory.grains())
        .filter(|&f| pool.header(f).status() == RESIDENT && pool.header(f).dirty())
        .count();
    assert_eq!(dirty, 0, "pages left dirty");
    for (pid, v) in pids {
        let g = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
        assert_eq!(g.read(|p| stamped(p, pid)), Some(Some(v)));
    }
}

/// Two 1 MiB pages share a 2 MiB batch; the first is freed before a reader's step begins, the
/// second while the reader is in its step and has the page fixed. The batch then goes to the 2 MiB
/// class, so the only frame for a 2 MiB page spans memory the reader may still read. `during` runs
/// while the reader is in its step, with the pool, the 2 MiB class, that frame, and a call that
/// lets the reader leave and waits until it has.
#[cfg(not(shuttle))]
fn with_a_reader_in_freed_memory(
    base: &str,
    during: impl FnOnce(&Arc<BufferPool>, SizeClass, u32, &dyn Fn()),
) {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, base, MIN_BUDGET);
    let (small, large) = (class(8), class(9));
    let first = drive(&io, || pool.allocate(small)).unwrap();
    let mut second = drive(&io, || pool.allocate(small)).unwrap();
    let batch = first.frame / memory::GRAINS;
    assert_eq!(
        second.frame / memory::GRAINS,
        batch,
        "the pages share a batch"
    );
    assert!(first.frame < second.frame);
    let (pid, held_frame) = (second.pid(), second.frame);
    stamp(&mut second.page_mut(), pid, 1);
    let first_pid = first.pid();
    drop((first, second));
    let free = |pid| {
        drive(&io, || match pool.fix_exclusive(pid, None)? {
            IOResult::Done(page) => pool.free(page),
            IOResult::IO(io) => Ok(IOResult::IO(io)),
        })
        .unwrap()
    };
    // Every other batch to the large class, so the small pages' batch is the only memory left
    // for a large page.
    let mut others = 0;
    while let Got::Frame(_) = pool.take_or_grow(large, Purpose::New) {
        others += 1;
    }
    assert!(others > 0);
    free(first_pid);
    let frame = memory::frame_at(large, batch, 0);
    assert!(grains(frame, large).contains(&(held_frame as usize)));
    std::thread::scope(|s| {
        // Dropped if this thread panics, which lets the reader go.
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (leave, leave_rx) = std::sync::mpsc::channel::<()>();
        let (pool, io) = (&pool, &io);
        let reader = s.spawn(move || {
            let _step = pool.store.epochs().enter();
            let g = drive(io, || pool.fix_optimistic(pid, None)).unwrap();
            assert_eq!(g.read(|p| stamped(p, pid)), Some(Some(1)));
            entered.send(()).unwrap();
            let _ = leave_rx.recv();
        });
        entered_rx.recv().unwrap();
        free(pid);
        assert!(pool.memory.drain_for(small, batch, large));
        let reader = std::cell::RefCell::new(Some((reader, leave)));
        let let_go = || {
            if let Some((reader, leave)) = reader.borrow_mut().take() {
                leave.send(()).unwrap();
                reader.join().unwrap();
            }
        };
        during(pool, large, frame, &let_go);
        let_go();
    });
}

/// A frame read into from disk is written plainly by the IO, so it is taken only once no step
/// that could still read its memory's last pages is left: every 4 KiB grain it spans counts, not
/// just the first. A new page may take the frame at once, but a read waits for the reader.
#[cfg(not(shuttle))]
#[test]
fn a_frame_is_read_into_only_once_every_grain_it_spans_is_quiet() {
    with_a_reader_in_freed_memory("/db/quiet", |pool, large, frame, leave| {
        // A new page is written through the frame's writer, whose stores race with nothing.
        match pool.take_or_grow(large, Purpose::New) {
            Got::Frame(f) => assert_eq!(f, frame),
            _ => panic!("no frame for a new page in the drained batch"),
        }
        pool.memory.put(large, frame);
        assert!(
            matches!(pool.take_or_grow(large, Purpose::Read), Got::Quieting(_)),
            "a read took memory a reader in its step may still be reading"
        );
        leave();
        match pool.take_or_grow(large, Purpose::Read) {
            Got::Frame(f) => assert_eq!(f, frame),
            _ => panic!("the frame stayed unquiet after the reader left"),
        }
    });
}

/// A read that finds the only free frame's memory still readable by a step under way waits on a
/// completion, and the step's end finishes it: it neither yields to be retried at once, over and
/// over while the step lasts, nor waits past it.
#[cfg(not(shuttle))]
#[test]
fn a_read_into_unquiet_memory_waits_for_the_step_that_may_read_it() {
    with_a_reader_in_freed_memory("/db/quiet-wait", |pool, large, frame, leave| {
        let waited = match pool.claim(large, Purpose::Read, &[]).unwrap() {
            IOResult::IO(IOCompletions(c)) => c,
            IOResult::Done(f) => panic!("a read took frame {f} while a reader may read it"),
        };
        assert!(
            !waited.finished(),
            "the read handed back a yield while the reader's step lasts"
        );
        leave();
        assert!(
            waited.finished(),
            "the reader left and the read still waits"
        );
        assert!(
            !pool.released_wait().finished(),
            "the pool hands out the wait the reader's end finished, so claims go round at once"
        );
        match pool.claim(large, Purpose::Read, &[]).unwrap() {
            IOResult::Done(f) => assert_eq!(f, frame),
            IOResult::IO(_) => panic!("the frame stayed unquiet after the reader left"),
        }
    });
}

/// A step holding a page of one class, as an operation holds the parent it reads a child through,
/// needs memory for a class that holds none, while every batch holds pages read this epoch.
/// Draining the held page's batch would only send that page's next fix to drain the memory back,
/// round after round: the class takes a batch no step under way holds, and the held page stays.
#[cfg(not(shuttle))]
#[test]
fn a_class_holding_no_memory_drains_none_its_step_holds() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/held", MIN_BUDGET);
    let parent = drive(&io, || pool.allocate(class(0))).unwrap().pid();
    // Of the other four 2 MiB batches, two to a 4 MiB page and two to four 1 MiB pages, each
    // read this epoch.
    drop(drive(&io, || pool.allocate(class(10))).unwrap());
    let mut small: Vec<PageId> = (0..4)
        .map(|_| drive(&io, || pool.allocate(class(8))).unwrap().pid())
        .collect();
    assert!(!pool.memory.has_free_run(class(8)), "the pool is full");
    // A free moves the epoch past the steps that read them, as frees and evictions do in a pool
    // at work.
    let freed = small.pop().unwrap();
    drive(&io, || match pool.fix_exclusive(freed, None)? {
        IOResult::Done(page) => pool.free(page),
        IOResult::IO(io) => Ok(IOResult::IO(io)),
    })
    .unwrap();
    let held = drive(&io, || pool.fix_optimistic(parent, None)).unwrap();
    let child = drive(&io, || pool.allocate(class(9))).unwrap();
    assert!(
        held.validate(),
        "the memory for {:?} came from the page its step holds",
        child.pid()
    );
}

/// A fix that finds its page leaving its frame waits on the pool: only stepping the write-out's
/// writeback submits the write. A drain marks its pages leaving before it queues their writeback,
/// so a fix looking in between has nothing to step, and queueing the writeback wakes it.
#[cfg(not(shuttle))]
#[test]
fn a_fix_that_finds_its_page_leaving_wakes_when_the_write_out_is_queued() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/leaving", MIN_BUDGET);
    let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (g.pid(), g.frame);
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    drop(g);
    // As a drain does: the page marked leaving, then its writeback queued.
    let latch = || FrameLatch {
        frames: pool.frames.clone(),
        frame,
    };
    let outgoing = pool.start_write_out(pool.latch_to_evict(class(0), frame, latch).unwrap());
    let woken = match pool.fix_optimistic(pid, None).unwrap() {
        IOResult::IO(IOCompletions(c)) => c,
        IOResult::Done(_) => panic!("fixed a page leaving its frame"),
    };
    assert!(!woken.finished());
    pool.write_out(class(0), vec![outgoing]);
    assert!(
        woken.finished(),
        "the fix waits on a write-out queued after it looked, and nothing wakes it"
    );
    let g = named(&io, "fix once written out", || {
        pool.fix_optimistic(pid, None)
    });
    assert_eq!(g.read(|p| stamped(p, pid)), Some(Some(1)));
}

/// A cut finding a page with a swap copy leaving its frame, its write-out marked and not yet
/// stepped, does not wait for the write: it holds no page another caller's change needs, and a
/// writer refused by the closed gate, stepping the cut for it, is the only caller that would have
/// stepped the writeback. Once the write lands the page reads back in pending, still owed the
/// image as the cut found it.
#[test]
fn a_cut_does_not_wait_on_a_write_out_it_does_not_step() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-leaving", MIN_BUDGET);
    let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (g.pid(), g.frame);
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    drop(g);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    assert!(pool.store.swap_slot(pid).is_some(), "a swap copy written");
    // As a drain does: the page marked leaving; its writeback is queued only later.
    let latch = || FrameLatch {
        frames: pool.frames.clone(),
        frame,
    };
    let outgoing = pool.start_write_out(pool.latch_to_evict(class(0), frame, latch).unwrap());
    let mut scan = CutScan::default();
    let mut looks = 0;
    let cut = loop {
        match pool.begin_cut(1, &mut scan) {
            IOResult::Done(()) => break true,
            IOResult::IO(_) if looks < 3 => looks += 1,
            IOResult::IO(_) => break false,
        }
    };
    pool.write_out(class(0), vec![outgoing]);
    let g = named(&io, "fix once written out", || {
        pool.fix_optimistic(pid, None)
    });
    let back = g.frame;
    drop(g);
    let pending = pool.header(back).pending.load(Ordering::Acquire);
    pool.end_cut(1);
    // Checked once the pool is gone: a panic unwinding through it ends the process.
    drop(pool);
    assert!(cut, "the cut waited on a write-out no one steps");
    assert!(
        scan.owed.contains(&pid),
        "the leaving page is owed its image"
    );
    assert_eq!(pending, 1, "read back in, the page is pending for the cut");
}

/// A claim takes its wait on the pool before it makes room, so a drain finding every page of its
/// batch already leaving, and queueing nothing, must wake no one: the wake would reach the claim's
/// own wait, handing it back finished, and a caller that steps IO only while it waits would retry
/// forever, the write it waits on never landing.
#[cfg(not(shuttle))]
#[test]
fn a_drain_that_queues_no_write_out_wakes_no_one() {
    // Writes complete only at a step.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(5));
    let pool = open_on(&io, "/db/nothing-queued", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        pool: &pool,
        pages: Vec::new(),
    };
    // Five dirty 2 MiB pages fill the pool: a 4 KiB page waits for one of them to be written out.
    for _ in 0..5 {
        model.allocate(class(9));
    }
    drop(named_in(&io, "a 4 KiB page", Some(&pool), || {
        pool.allocate(class(0))
    }));
    model.check_all();
}

/// A write that lands while another thread steps its writeback, after that step took the writes
/// landed so far, is left for the next step. A waiter that found the writeback held when the
/// write's own wakeup had already passed waits for the stepper: the stepper wakes it.
#[test]
fn a_write_landing_during_another_threads_step_wakes_whoever_waits_on_the_writeback() {
    // Writes stay in flight until the IO steps, which completes them in an order its seed picks:
    // a seed whose write is still in flight once the sweep submitted it builds the case.
    let mut built = 0;
    for seed in 0..32 {
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let pool = open_on(&io, "/db/landed", MIN_BUDGET);
        let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, 1);
        g.changed();
        drop(g);
        // Stepped until it submits the write, and no further: its wait ends only once it lands.
        let mut sweep = pool.sweep();
        loop {
            match sweep.step(&pool).unwrap() {
                IOResult::IO(IOCompletions(_))
                    if pool.writes_in_flight.load(Ordering::Acquire) > 0 =>
                {
                    break
                }
                IOResult::IO(IOCompletions(c)) => io.wait_for_completion(c).unwrap(),
                IOResult::Done(()) => panic!("the sweep wrote nothing"),
            }
        }
        let mut landings = None;
        pool.with_queued(|wb| landings = Some(wb.landings()));
        let landings = landings.expect("a writeback queued");
        if !landings.is_empty() || crash.in_flight() == 0 {
            continue;
        }
        built += 1;
        // Inside the step, once it took the landed writes (none): the write lands, its wakeup
        // sweeps, and a waiter finds the writeback held.
        let waited: Arc<Mutex<Option<Completion>>> = Arc::new(Mutex::new(None));
        pool.with_queued(|wb| {
            wb.after_taking_landed = Some({
                let (pool, waited, crash, io) =
                    (pool.clone(), waited.clone(), crash.clone(), io.clone());
                Box::new(move || {
                    while crash.in_flight() > 0 {
                        io.step().unwrap();
                    }
                    assert!(!landings.is_empty(), "the write did not land");
                    match pool.settle_writes().unwrap() {
                        IOResult::IO(IOCompletions(c)) => *waited.lock() = Some(c),
                        IOResult::Done(()) => panic!("settled with the write unsettled"),
                    }
                })
            })
        });
        assert!(pool.drive_writebacks().unwrap().is_none());
        let waited = waited.lock().take().expect("the waiter looked");
        assert!(
            waited.finished(),
            "seed {seed}: the waiter waits on a wakeup no one sends, with its write landed and \
             unsettled"
        );
    }
    assert!(built > 0, "no seed left the write in flight");
}

/// A page leaving its frame, wanted while another thread steps its write-out, which waits on
/// its write plan before it submits the write: the reader waits for that thread, and is woken by
/// it, not by the write, which nothing submits until someone steps the write-out again.
#[test]
fn a_reader_of_a_page_written_out_wakes_when_its_write_out_waits_before_submitting() {
    // Reads stay in flight until the IO steps, so the write plan's metadata page waits.
    let crash = Arc::new(CrashIO::new(3));
    let io: Arc<dyn IO> = crash.clone();
    let pool = open_on(&io, "/db/outgoing", MIN_BUDGET);
    let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
    let pid = g.pid();
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    let frame = g.frame;
    drop(g);
    // The page leaves its frame, as a drain makes it.
    let hold = pool
        .latch_to_evict(class(0), frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .expect("the page is held by no one");
    let out = pool.start_write_out(hold);
    pool.write_out(class(0), vec![out]);
    let waited: Arc<Mutex<Option<Completion>>> = Arc::new(Mutex::new(None));
    pool.with_queued(|wb| {
        wb.before_step = Some({
            let (pool, waited) = (pool.clone(), waited.clone());
            Box::new(move || match pool.fix_optimistic(pid, None).unwrap() {
                IOResult::IO(IOCompletions(c)) => *waited.lock() = Some(c),
                IOResult::Done(_) => panic!("the page read as resident while it is written out"),
            })
        })
    });
    match pool.drive_writebacks().unwrap() {
        Some(c) if !c.finished() => {}
        other => panic!(
            "the write-out did not wait before submitting: {:?}",
            other.map(|c| c.finished())
        ),
    }
    assert_eq!(pool.writes_in_flight.load(Ordering::Acquire), 0);
    let waited = waited.lock().take().expect("the reader looked");
    assert!(
        waited.finished(),
        "the reader waits for a write no one submits while the write-out waits on its plan"
    );
}

/// Slots of the directory page in [`linked_pages`]: each names a child and the life it is in.
const SLOTS: usize = 6;

fn slot_at(i: usize) -> usize {
    HEADER_SIZE + 16 * i
}

/// Waits for `c` as [`drive`] does between tries.
fn wait_for(io: &Arc<dyn IO>, c: Completion) {
    crate::thread::yield_now();
    while !c.finished() {
        io.step().unwrap();
        if !c.finished() {
            crate::thread::yield_now();
        }
    }
    assert!(c.get_error().is_none(), "{:?}", c.get_error());
}

/// What one try of an operation through the directory came to; a retry says where it came from.
enum Try<T> {
    Done(T),
    /// Wait for this, holding nothing, then try again.
    Wait(Completion, &'static str),
    Again(&'static str),
}

/// Runs `op` until it is done, waiting between tries with nothing held, as an operation that
/// returns IO drops its guards.
fn through<T>(
    io: &Arc<dyn IO>,
    pool: &BufferPool,
    name: &str,
    mut op: impl FnMut() -> Try<T>,
) -> T {
    let mut retries: BTreeMap<&'static str, u64> = BTreeMap::new();
    for tries in 1u64.. {
        if tries == TRIES {
            too_many_tries(format!(
                "{name} tried {tries} times, retried {retries:?}\n{}",
                pool.describe()
            ));
        }
        match op() {
            Try::Done(v) => {
                if tries >= TRIES {
                    eprintln!(
                        "{name} done after {tries} tries; {}",
                        pool.store.epochs().describe()
                    );
                }
                return v;
            }
            Try::Wait(c, why) => {
                *retries.entry(why).or_default() += 1;
                wait_for(io, c)
            }
            Try::Again(why) => {
                *retries.entry(why).or_default() += 1;
                crate::thread::yield_now()
            }
        }
    }
    unreachable!("tries run out")
}

/// One try of a fix, for [`through`]: a fix of a number the directory no longer names may find
/// it freed, which is only an error while the directory still names it.
fn fixed<T>(r: Result<IOResult<T>>, directory: &Optimistic<'_>, class: SizeClass) -> Try<T> {
    match r {
        Ok(IOResult::Done(v)) => Try::Done(v),
        Ok(IOResult::IO(IOCompletions(c))) => {
            let why = match (c.finished(), class.index()) {
                (true, 8) => "child fix yielded, 1 MiB",
                (true, 9) => "child fix yielded, 2 MiB",
                (true, _) => "child fix yielded, 4 MiB",
                (false, _) => "child fix waited",
            };
            Try::Wait(c, why)
        }
        Err(LimboError::Corrupt(_)) if !directory.validate() => Try::Again("child freed"),
        Err(e) => panic!("a fix through a valid directory failed: {e}"),
    }
}

/// Children linked from one directory page, as tree nodes are from their parent. Three threads
/// read children through the directory, change them, unlink and free them, and allocate and
/// link new ones, with no lock of the test's own: whatever excludes them is the pool's latches
/// and the directory's. The children are 1, 2 and 4 MiB in a pool of 10 MiB, so they are evicted,
/// written back and moved between classes meanwhile, and their numbers freed and reused. A
/// thread that finds the directory unchanged after it read a child read that child whole, as the
/// page and in the life the directory names: never a freed number's next page.
fn linked_pages(ops: usize, seed: u64) {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/linked", MIN_BUDGET);
    let dir = {
        let mut g = named_in(&io, "allocate", Some(&pool), || pool.allocate(class(0)));
        g.changed();
        g.pid()
    };
    let lives = Arc::new(AtomicU64::new(1));
    let mut handles = Vec::new();
    for t in 0..3u64 {
        let (io, pool, lives) = (io.clone(), pool.clone(), lives.clone());
        handles.push(crate::thread::spawn(move || {
            let free = |pid: PageId| {
                named_in(&io, "free", Some(&pool), || {
                    match pool.fix_exclusive(pid, None)? {
                        IOResult::Done(page) => pool.free(page),
                        IOResult::IO(io) => Ok(IOResult::IO(io)),
                    }
                })
            };
            let mut rng = ChaCha8Rng::seed_from_u64(seed * 10 + t);
            for _ in 0..ops {
                let i = rng.random_range(0..SLOTS);
                match rng.random_range(0..8) {
                    // Allocate a child and link it, if the slot is still empty then.
                    0 => {
                        let life = lives.fetch_add(1, Ordering::Relaxed);
                        let mut g = named_in(&io, "allocate", Some(&pool), || {
                            pool.allocate(class(8 + i % 3))
                        });
                        let pid = g.pid();
                        stamp(&mut g.page_mut(), pid, life << 32);
                        g.changed();
                        drop(g);
                        let linked = through(&io, &pool, "link", || {
                            let mut d = match pool.fix_exclusive(dir, None).unwrap() {
                                IOResult::Done(d) => d,
                                IOResult::IO(IOCompletions(c)) => {
                                    return Try::Wait(c, "directory fix for a link")
                                }
                            };
                            if d.page().u64_at(slot_at(i)) != 0 {
                                d.release_unchanged();
                                return Try::Done(false);
                            }
                            d.page_mut().put_u64(slot_at(i), pid.raw());
                            d.page_mut().put_u64(slot_at(i) + 8, life);
                            d.changed();
                            Try::Done(true)
                        });
                        if !linked {
                            free(pid);
                        }
                    }
                    // Unlink a child, then free it.
                    1 => {
                        let unlinked = through(&io, &pool, "unlink", || {
                            let mut d = match pool.fix_exclusive(dir, None).unwrap() {
                                IOResult::Done(d) => d,
                                IOResult::IO(IOCompletions(c)) => {
                                    return Try::Wait(c, "directory fix for an unlink")
                                }
                            };
                            let raw = d.page().u64_at(slot_at(i));
                            if raw == 0 {
                                d.release_unchanged();
                                return Try::Done(None);
                            }
                            d.page_mut().put_u64(slot_at(i), 0);
                            d.page_mut().put_u64(slot_at(i) + 8, 0);
                            d.changed();
                            Try::Done(Some(PageId::from_raw(raw)))
                        });
                        if let Some(pid) = unlinked {
                            free(pid);
                        }
                    }
                    // Change a child, reached through the directory.
                    2..4 => {
                        through(&io, &pool, "change", || {
                            let d = match pool.fix_optimistic(dir, None).unwrap() {
                                IOResult::Done(d) => d,
                                IOResult::IO(IOCompletions(c)) => {
                                    return Try::Wait(c, "directory fix for a change")
                                }
                            };
                            let Some((raw, life)) =
                                d.read(|p| (p.u64_at(slot_at(i)), p.u64_at(slot_at(i) + 8)))
                            else {
                                return Try::Again("directory changed while read");
                            };
                            if raw == 0 {
                                return Try::Done(());
                            }
                            let pid = PageId::from_raw(raw);
                            let mut g = match fixed(pool.fix_exclusive(pid, None), &d, pid.class())
                            {
                                Try::Done(g) => g,
                                Try::Wait(c, why) => return Try::Wait(c, why),
                                Try::Again(why) => return Try::Again(why),
                            };
                            if !d.validate() {
                                g.release_unchanged();
                                return Try::Again("directory changed under the child");
                            }
                            let v = stamped(g.page(), pid).unwrap_or_else(|| {
                                panic!("{pid:?} changed torn or as another page")
                            });
                            assert_eq!(v >> 32, life, "{pid:?} changed in another life");
                            stamp(&mut g.page_mut(), pid, v + 1);
                            g.changed();
                            Try::Done(())
                        });
                    }
                    // Read a child through the directory.
                    _ => through(&io, &pool, "read", || {
                        let d = match pool.fix_optimistic(dir, None).unwrap() {
                            IOResult::Done(d) => d,
                            IOResult::IO(IOCompletions(c)) => {
                                return Try::Wait(c, "directory fix for a read")
                            }
                        };
                        let Some((raw, life)) =
                            d.read(|p| (p.u64_at(slot_at(i)), p.u64_at(slot_at(i) + 8)))
                        else {
                            return Try::Again("directory changed while read");
                        };
                        if raw == 0 {
                            return Try::Done(());
                        }
                        let pid = PageId::from_raw(raw);
                        let holder = || d.validate();
                        let child = match fixed(
                            pool.fix_optimistic_held(pid, None, &holder),
                            &d,
                            pid.class(),
                        ) {
                            Try::Done(Some(c)) => c,
                            Try::Done(None) => {
                                return Try::Again("directory changed before the load")
                            }
                            Try::Again(why) => return Try::Again(why),
                            Try::Wait(c, why) => return Try::Wait(c, why),
                        };
                        let Some(seen) = child.read(|p| stamped(p, pid)) else {
                            return Try::Again("child changed while read");
                        };
                        if !d.validate() {
                            return Try::Again("directory changed under the child");
                        }
                        let v =
                            seen.unwrap_or_else(|| panic!("{pid:?} read torn or as another page"));
                        assert_eq!(v >> 32, life, "{pid:?} read in another life");
                        Try::Done(())
                    }),
                }
            }
        }));
    }
    // The checkpointer's part: checkpoints of the metadata, each cut while allocations and
    // frees change it.
    {
        let (io, pool) = (io.clone(), pool.clone());
        handles.push(crate::thread::spawn(move || {
            for _ in 0..ops {
                checkpoint_meta(&io, &pool.store);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[cfg(not(shuttle))]
#[test]
fn children_read_through_their_directory_are_whole_and_in_the_life_it_names() {
    for seed in 0..20 {
        linked_pages(200, seed);
    }
}

#[cfg(shuttle)]
#[test]
fn children_read_through_their_directory_are_whole_and_in_the_life_it_names_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || linked_pages(12, crate::storage::atoll::testing::workload_seed()),
        300,
    );
}

/// One checkpoint of the store's metadata alone: cut, every page dirty at the cut written as it
/// stood then, the cut ended and the checkpoint put in charge, its waiting frees done.
fn checkpoint_meta(io: &Arc<dyn IO>, store: &Arc<PageStore>) {
    let n = store.meta().in_charge() + 1;
    let dirty: Vec<_> = store
        .begin_cut(n)
        .0
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let frames = drive(io, || store.meta().get_all(&dirty)).unwrap();
    for frame in &frames {
        // Asked until written: a wait for a hold or a settling change writes nothing.
        while let Some(c) = store.meta().write_for(frame, n).unwrap() {
            wait_for(io, c);
        }
    }
    store.meta().end_cut(n, &frames);
    store.meta().checkpointed(n, &frames);
    let mut freeing = crate::storage::atoll::store::FreeEdit::default();
    drive(io, || store.after(n, &mut freeing)).unwrap();
}

/// An eviction round whose threshold every candidate is over, as a cold value in the threshold's
/// sample can leave it: the round evicts the least valued candidate it can latch rather than
/// nothing (DESIGN.md 9.1.2).
#[test]
fn a_round_whose_threshold_every_candidate_is_over_still_evicts_one() {
    for b in backends() {
        let pool = open(&b, MIN_BUDGET);
        let mut model = Model {
            io: &b.io,
            pool: &pool,
            pages: Vec::new(),
        };
        for _ in 0..(MIN_BUDGET / class(3).size()) {
            model.allocate(class(3));
        }
        let mut sweep = pool.sweep();
        drive(&b.io, || sweep.step(&pool)).unwrap();
        let before = pool.evicted.load(Ordering::Relaxed);
        THRESHOLD.with(|t| t.set(Some(-1.0)));
        pool.evict_round(class(3)).unwrap();
        THRESHOLD.with(|t| t.set(None));
        assert!(
            pool.evicted.load(Ordering::Relaxed) > before,
            "{}: the round evicted a page though every candidate was over its threshold",
            b.name
        );
        model.check_all();
    }
}

/// The audit's `late-cut-copy-review` (2026-11-08): a failed checkpoint's `end_cut` between a
/// change's reading of the cut, with its frame and room taken, and its insertion of the copy. The
/// copy is not kept: its frame and its room's bytes go back, and nothing is left for a checkpoint
/// that will never image it (DESIGN.md 9.1.1).
#[test]
fn a_copy_made_after_its_cut_ended_is_not_kept() {
    use crate::race;
    for b in backends() {
        let pool = open(&b, MIN_KEEPING_BUDGET);
        let mut g = drive(&b.io, || pool.allocate(class(0))).unwrap();
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, 1);
        g.changed();
        drop(g);
        let mut scan = CutScan::default();
        assert!(matches!(pool.begin_cut(1, &mut scan), IOResult::Done(())));
        assert_eq!(
            scan.owed,
            vec![pid],
            "{}: the page is owed an image",
            b.name
        );
        let g = drive(&b.io, || pool.fix_exclusive(pid, None)).unwrap();
        {
            let pool = pool.clone();
            race::at("buffer.copy_for_cut", move || pool.end_cut(1));
        }
        assert!(
            pool.keep(&[&g], &mut RoomWait::default()).is_ok(),
            "{}: the change went on",
            b.name
        );
        assert!(!race::pending("buffer.copy_for_cut"));
        drop(g);
        assert!(
            pool.kept.lock().is_empty(),
            "{}: no copy kept for an ended cut",
            b.name
        );
        assert_eq!(
            pool.kept_stats().used,
            0,
            "{}: the room's bytes are back",
            b.name
        );
        assert_eq!(
            pool.lent_stats().kept.0,
            0,
            "{}: the copy's frame is back",
            b.name
        );
    }
}

type PendingReads =
    Arc<crate::storage::atoll::locks::Mutex<Vec<(Arc<crate::io::Buffer>, source::ReadDone)>>>;

/// A temporary page with no copy reads as zeros inside the call and is never counted at the
/// backend; one with a copy in the file calls `Submitting` once, right before the file's read, and
/// lands as the IO steps (the auditor's #3876: the count holds requests at the backend only).
#[test]
fn a_temporary_read_counts_a_request_only_for_its_files_copy() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/temp-submit", MIN_BUDGET);
    let c = class(0);
    let pid = pool.temp.allocate(c);
    let read = |pool: &Arc<BufferPool>| {
        let files = |place| pool.file_for(place);
        let submitted = source::Counting::new(Some(&files));
        let result = Arc::new(Mutex::new(None));
        let into = Arc::new(crate::io::Buffer::new_temporary(c.size()));
        let r = result.clone();
        let done: source::ReadDone = Box::new(move |res| *r.lock() = Some(res.is_ok()));
        pool.temp.read(pid, into.clone(), done, &submitted).unwrap();
        let inline = *result.lock();
        let mut steps = 0;
        while result.lock().is_none() && steps < 64 {
            io.step().unwrap();
            steps += 1;
        }
        let first = into.as_slice()[0];
        let landed = *result.lock();
        (submitted.handed.get(), inline, landed, first)
    };
    let unwritten = read(&pool);
    let written = Arc::new(Mutex::new(None));
    {
        let w = written.clone();
        // The write's request, stepped below until it lands.
        let (place, request) = pool
            .temp
            .write_request(pid, c.size(), move |res| *w.lock() = Some(res.is_ok()));
        let (file, at) = pool.file_for(place).unwrap();
        drop(
            file.pwrite(
                at,
                Arc::new(crate::io::Buffer::new(vec![7; c.size()])),
                request,
            )
            .unwrap(),
        );
    }
    let mut steps = 0;
    while written.lock().is_none() && steps < 64 {
        io.step().unwrap();
        steps += 1;
    }
    let copied = read(&pool);
    let written = *written.lock();
    drop(pool);
    assert_eq!(
        unwritten,
        (0, Some(true), Some(true), 0),
        "no copy: (submitted, done inside the call, done, first byte)"
    );
    assert_eq!(written, Some(true), "the copy was written");
    // Whether the backend lands it inside the call is the backend's; counted once either way.
    assert_eq!(
        (copied.0, copied.2, copied.3),
        (1, Some(true), 7),
        "a copy: (submitted, done, first byte)"
    );
}

/// A page source that breaks the read contract: it returns done with its read neither handed to
/// the pool's IO nor landed, and the test lands it later. The pool refuses it (source.rs).
struct DeferredSource(PendingReads);
struct DeferredLocated(PendingReads);

impl source::PageSource for DeferredSource {
    fn locate(&self, _pid: PageId) -> Result<IOResult<Box<dyn source::Located>>> {
        Ok(IOResult::Done(Box::new(DeferredLocated(self.0.clone()))))
    }
}

impl source::Located for DeferredLocated {
    /// Never submitting, never done: the contract's breach.
    fn read(
        self: Box<Self>,
        into: Arc<crate::io::Buffer>,
        done: source::ReadDone,
        _submitting: source::Submitting<'_>,
    ) -> Result<IOResult<Option<Completion>>> {
        self.0.lock().push((into, done));
        Ok(IOResult::Done(None))
    }
}

/// The audit's `all-loading` run (2026-11-08): every frame of the pool is loading, reads at the
/// backend that land only when the test lands them. `allocate` parks, its wait not finished, rather than handing back
/// a wait its own step finishes as it returns; and the reads landing wake it (DESIGN.md 9.1.2).
#[test]
fn a_claim_while_every_frame_loads_parks_and_wakes_when_the_reads_land() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let store = PageStore::open(
        crate::storage::atoll::requests::counted_for_test(io),
        "/db/all-loading",
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        0,
    );
    let reads: PendingReads = Arc::new(crate::storage::atoll::locks::Mutex::new(Vec::new()));
    let pool = BufferPool::new(
        BufferConfig::with_budget(MIN_BUDGET),
        Arc::new(SubmittingSource(reads.clone())),
        store,
    )
    .unwrap();
    let c = class(9);
    assert_eq!(c.size(), memory::BATCH);
    let mut loading = Vec::new();
    for i in 0..pool.memory.batches {
        match pool
            .fix_optimistic(PageId::new(c, i as u64 + 1), None)
            .unwrap()
        {
            IOResult::IO(IOCompletions(wait)) => loading.push(wait),
            IOResult::Done(_) => panic!("a read at the backend came back resident"),
        }
    }
    let parked = match pool.allocate(c).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(_) => panic!("allocated while every frame loads"),
    };
    assert!(
        !parked.finished(),
        "the claim parks: its wait is not one its own step finished"
    );
    for (buffer, done) in std::mem::take(&mut *reads.lock()) {
        buffer.as_mut_slice().fill(0);
        done(Ok(()));
    }
    assert!(loading.iter().all(Completion::finished));
    assert!(parked.finished(), "the reads landing woke the parked claim");
}

/// Item 2 of 9.1.2's claim proofs: a page published resident by its read landing, or left resident
/// by a write-out that did not happen, tells a parked claim only after its latch is let go. Paused
/// between the two, the claim is still parked and the page still latched; once the latch is let go,
/// the claim's wait is finished (DESIGN.md 9.1.2).
#[test]
fn a_page_made_resident_tells_a_parked_claim_after_its_latch_is_let_go() {
    use crate::race;
    fn pause_at(name: &'static str, pool: &Arc<BufferPool>, frame: u32, parked: &Completion) {
        let (pool, parked) = (pool.clone(), parked.clone());
        race::at(name, move || {
            assert_eq!(pool.header(frame).status.load(Ordering::Acquire), RESIDENT);
            assert!(
                Held::try_take(FrameLatch {
                    frames: pool.frames.clone(),
                    frame,
                })
                .is_none(),
                "{name}: the latch is still held"
            );
            assert!(
                !parked.finished(),
                "{name}: told before the latch was let go"
            );
        });
    }

    // A read landing.
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let store = PageStore::open(
        crate::storage::atoll::requests::counted_for_test(io),
        "/db/resident-loaded",
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        0,
    );
    let reads: PendingReads = Arc::new(crate::storage::atoll::locks::Mutex::new(Vec::new()));
    let pool = BufferPool::new(
        BufferConfig::with_budget(MIN_BUDGET),
        Arc::new(SubmittingSource(reads.clone())),
        store,
    )
    .unwrap();
    let pid = PageId::new(class(9), 1);
    assert!(matches!(
        pool.fix_optimistic(pid, None).unwrap(),
        IOResult::IO(_)
    ));
    let frame = pool
        .table
        .find(&pool.frames.headers, pid.raw())
        .expect("in the table");
    let parked = pool.released_wait();
    pause_at("buffer.loaded_published", &pool, frame, &parked);
    for (buffer, done) in std::mem::take(&mut *reads.lock()) {
        buffer.as_mut_slice().fill(0);
        done(Ok(()));
    }
    assert!(
        !race::pending("buffer.loaded_published"),
        "the read paused where it publishes"
    );
    assert!(parked.finished(), "the read landing told the parked claim");

    // A write-out that did not happen.
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/resident-kept", MIN_BUDGET);
    let page = drive(&io, || pool.allocate(class(9))).unwrap();
    let frame = page.frame;
    drop(page);
    let hold = Held::try_take(FrameLatch {
        frames: pool.frames.clone(),
        frame,
    })
    .expect("the page's latch is free");
    pool.header(frame).status.store(OUTGOING, Ordering::Release);
    let parked = pool.released_wait();
    pause_at("buffer.kept_published", &pool, frame, &parked);
    pool.frames.kept(hold);
    assert!(
        !race::pending("buffer.kept_published"),
        "the write-out paused where it publishes"
    );
    assert!(
        parked.finished(),
        "the page left resident told the parked claim"
    );
}

/// Item 4 of 9.1.2's claim proofs: the audit's `two-holder-sc` (2026-11-08). Every page a claim
/// could evict is latched by H1 when it first looks; before its fresh registration H1 lets go and
/// H2 takes them all. The claim parks, its wait finished by H2's release and not by H1's. And a
/// caller holding every page it could evict parks on its own release, each look alike, and does
/// not go round (DESIGN.md 9.1.2).
#[test]
fn a_claim_waits_on_whoever_holds_its_pages_last_and_on_a_caller_holding_them_all() {
    use crate::race;
    use std::{cell::RefCell, rc::Rc};
    let c = class(9);
    let parked_on = |pool: &Arc<BufferPool>| match pool.allocate(c).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(_) => panic!("allocated with every page latched"),
    };
    let hold_all = |pool: &Arc<BufferPool>, frames: &[u32]| -> Vec<Held<FrameLatch>> {
        frames
            .iter()
            .map(|&frame| {
                Held::try_take(FrameLatch {
                    frames: pool.frames.clone(),
                    frame,
                })
                .expect("the page's latch is free")
            })
            .collect()
    };

    // H1, then H2.
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/two-holders", MIN_BUDGET);
    let frames: Vec<u32> = (0..pool.memory.batches)
        .map(|_| drive(&io, || pool.allocate(c)).unwrap().frame)
        .collect();
    let h1 = Rc::new(RefCell::new(hold_all(&pool, &frames)));
    let h2 = Rc::new(RefCell::new(Vec::new()));
    {
        let (pool, frames, h1, h2) = (pool.clone(), frames.clone(), h1.clone(), h2.clone());
        race::at("buffer.claim_fresh", move || {
            h1.borrow_mut().clear();
            *h2.borrow_mut() = hold_all(&pool, &frames);
        });
    }
    let parked = parked_on(&pool);
    assert!(
        !race::pending("buffer.claim_fresh"),
        "the claim reached its fresh registration"
    );
    assert!(h1.borrow().is_empty() && h2.borrow().len() == frames.len());
    assert!(
        !parked.finished(),
        "H1's release does not finish the wait H2 holds up"
    );
    h2.borrow_mut().clear();
    assert!(parked.finished(), "H2's release told the parked claim");

    // The caller holds them all: each page's birth guard dropped (its first release spent), then
    // fixed again and held, so a release can tell the claim only through the claim's own mark.
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/caller-holds", MIN_BUDGET);
    let pids: Vec<PageId> = (0..pool.memory.batches)
        .map(|_| drive(&io, || pool.allocate(c)).unwrap().pid)
        .collect();
    let mut pages: Vec<_> = pids
        .iter()
        .map(|&pid| drive(&io, || pool.fix_exclusive(pid, None)).unwrap())
        .collect();
    let parked = parked_on(&pool);
    let again = parked_on(&pool);
    assert!(
        !parked.finished() && !again.finished(),
        "a caller holding every page waits on its own release and does not go round"
    );
    drop(pages.pop());
    assert!(
        parked.finished() && again.finished(),
        "the caller's release told both looks"
    );
}

/// Item 5 of 9.1.2's claim proofs: a round finds a page's latch held both ways, by an optimistic
/// read that finds it held and by a hold at the version it read lost to a holder who took it in
/// between. Either way the round marks it after the registration, and the holder's release
/// finishes the registration (DESIGN.md 9.1.2).
#[test]
fn a_round_marks_a_latch_held_when_it_reads_it_and_one_taken_before_its_hold() {
    use crate::race;
    use std::{cell::RefCell, rc::Rc};
    for taken_between in [false, true] {
        let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
        let pool = open_on(&io, "/db/round-marks", MIN_BUDGET);
        let c = class(9);
        let frame = drive(&io, || pool.allocate(c)).unwrap().frame;
        let take = {
            let pool = pool.clone();
            move || {
                Held::try_take(FrameLatch {
                    frames: pool.frames.clone(),
                    frame,
                })
                .expect("the page's latch is free")
            }
        };
        let hold = Rc::new(RefCell::new(None));
        if taken_between {
            let hold = hold.clone();
            race::at("buffer.evict_take", move || {
                *hold.borrow_mut() = Some(take())
            });
        } else {
            *hold.borrow_mut() = Some(take());
        }
        let waiting = pool.released_wait();
        let (mut outgoing, mut room) = (Vec::new(), Room::default());
        assert!(!pool.evict_or_write(c, frame, &mut outgoing, &mut room));
        assert!(
            !race::pending("buffer.evict_take"),
            "taken between {taken_between}"
        );
        assert!(
            room.marked && !room.changed,
            "taken between {taken_between}: the round marked the latch it found held: {room:?}"
        );
        assert!(!waiting.finished());
        drop(hold.borrow_mut().take());
        assert!(
            waiting.finished(),
            "taken between {taken_between}: the holder's release finished the registration"
        );
    }
}

/// Item 7 of 9.1.2's claim proofs, the audit's late-registration kill on the real claim: every
/// page the claim could evict is latched; the last round marks them, and the holder lets them go
/// before the claim returns. Its registration came before the marks, so the releases finished the
/// wait it returns, and its caller looks again at once (DESIGN.md 9.1.2).
#[test]
fn a_release_after_the_last_rounds_marks_finishes_the_wait_the_claim_returns() {
    use crate::race;
    use std::{cell::RefCell, rc::Rc};
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/last-round", MIN_BUDGET);
    let c = class(9);
    let holds: Vec<_> = (0..pool.memory.batches)
        .map(|_| {
            let frame = drive(&io, || pool.allocate(c)).unwrap().frame;
            Held::try_take(FrameLatch {
                frames: pool.frames.clone(),
                frame,
            })
            .expect("the page's latch is free")
        })
        .collect();
    let holds = Rc::new(RefCell::new(holds));
    {
        let holds = holds.clone();
        race::at("buffer.claim_last_round", move || {
            holds.borrow_mut().clear()
        });
    }
    let wait = match pool.allocate(c).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(_) => panic!("allocated with every page latched"),
    };
    assert!(
        !race::pending("buffer.claim_last_round"),
        "the claim made its last round"
    );
    assert!(holds.borrow().is_empty());
    assert!(
        wait.finished(),
        "the releases after the last round's marks finished the wait the claim returned"
    );
}

/// Item 9 of 9.1.2's claim proofs, the audit's P1-C schedule (2026-11-08): every other frame of
/// the class holds a page another caller keeps latched throughout (a read in flight would make the
/// claim's wait stepped, which only a landing finishes), and five new pages are installed in the five frames left, each
/// install paused before it publishes its page while the next starts; inside the fifth, another
/// claim looks, finds nothing to take and parks. The new pages' first releases, changed or
/// unchanged, of cache pages and of temporary pages alike, wake it, and so does that of a page
/// installed to be freed (`free_unreferenced`) whose free waits (DESIGN.md 9.1.2).
#[test]
fn new_pages_published_after_a_claim_looked_tell_it_with_their_first_release() {
    use crate::race;
    use std::{cell::RefCell, rc::Rc};
    #[derive(Clone, Copy, Debug)]
    enum New {
        Changed,
        Unchanged,
        Temporary,
        Unreferenced,
    }
    const INSTALLS: u32 = 5;
    fn install(pool: &Arc<BufferPool>, c: SizeClass, temporary: bool) -> Exclusive<'_> {
        let page = if temporary {
            pool.allocate_temporary(c)
        } else {
            pool.allocate(c)
        };
        match page.unwrap() {
            IOResult::Done(page) => page,
            IOResult::IO(_) => panic!("a frame was free for the install"),
        }
    }
    let c = class(9);
    for new in [
        New::Changed,
        New::Unchanged,
        New::Temporary,
        New::Unreferenced,
    ] {
        let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
        let pool = open_on(&io, &format!("/db/first-release-{new:?}"), MIN_BUDGET);
        let installs: u32 = match new {
            New::Unreferenced => 1,
            _ => INSTALLS,
        };
        let held: Vec<Exclusive<'_>> = (installs..pool.memory.batches)
            .map(|_| install(&pool, c, false))
            .collect();
        let point = match new {
            New::Unreferenced => "buffer.unreferenced_publish",
            _ => "buffer.install_publish",
        };
        let parked: Rc<RefCell<Option<Completion>>> = Rc::new(RefCell::new(None));
        let temporary = matches!(new, New::Temporary);
        // Install k pauses at its publication and starts install k + 1 there; the last pause runs
        // the claim. Each install's page is released once every later one has published.
        for k in 1..=installs {
            let (pool, parked) = (pool.clone(), parked.clone());
            race::at(point, move || {
                if k < installs {
                    let page = install(&pool, c, temporary);
                    match new {
                        New::Unchanged => drop(page.release_unchanged()),
                        _ => drop(page),
                    }
                    return;
                }
                match pool.allocate(c).unwrap() {
                    IOResult::IO(IOCompletions(wait)) => {
                        assert!(!wait.finished(), "{new:?}: parked with every frame claimed");
                        *parked.borrow_mut() = Some(wait);
                    }
                    IOResult::Done(_) => panic!("{new:?}: a frame for a claim with none to take"),
                }
            });
        }
        match new {
            New::Unreferenced => {
                let pid = match pool.store.allocate(c).unwrap() {
                    IOResult::Done(pid) => pid,
                    IOResult::IO(_) => panic!("a page number at once"),
                };
                // The free waits on the map's latches, leaving the page it installed resident: its
                // first release is what tells the claim.
                let freed = pool
                    .store
                    .with_map_latched(pid, || pool.free_unreferenced(pid).unwrap());
                assert!(
                    matches!(freed, IOResult::IO(_)),
                    "the free waits on the map"
                );
                let looked = parked.borrow().clone().expect("the claim looked");
                assert!(
                    looked.finished(),
                    "{new:?}: the first release told the claim"
                );
                assert!(matches!(
                    pool.free_unreferenced(pid).unwrap(),
                    IOResult::Done(())
                ));
            }
            _ => {
                let page = install(&pool, c, temporary);
                let looked = parked.borrow().clone().expect("the claim looked");
                assert!(
                    looked.finished(),
                    "{new:?}: the first releases told the claim"
                );
                match new {
                    New::Unchanged => drop(page.release_unchanged()),
                    _ => drop(page),
                }
            }
        }
        assert!(
            !race::pending(point),
            "{new:?}: every install paused before publishing"
        );
        let looked = parked.borrow().clone().expect("the claim looked");
        assert!(looked.finished(), "{new:?}: the claim was told");
        drop(held);
    }
}

/// The rare path's token as a claim's run drain meets it (DESIGN.md 9.1.1, 9.1.2; the audit's
/// #3616): five batches each hold a page of 2 MiB, so a 4 MiB claim drains a run.
fn token_pool(io: &Arc<dyn IO>, base: &str) -> Arc<BufferPool> {
    let pool = open_on(io, base, MIN_BUDGET);
    for _ in 0..pool.memory.batches {
        drive(io, || pool.allocate(class(9))).unwrap();
    }
    pool
}

fn claim_parked(pool: &Arc<BufferPool>) -> Completion {
    match pool.allocate(class(10)).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(_) => panic!("a 4 MiB frame with every run held"),
    }
}

/// A claim whose run drain is refused (a run reserved already, its pass not yet made) takes the
/// token and hands it back unused: no supply, so it finishes nothing, and the claim parks on its
/// registration rather than going round with nothing changed (the audit's 16 finished waits).
#[test]
fn a_token_handed_back_unused_wakes_no_one_and_the_claim_parks() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = token_pool(&io, "/db/token-unused");
    assert!(pool.memory.reserve_only(class(10), 2));
    let parked = claim_parked(&pool);
    assert!(
        !parked.finished(),
        "the claim's own token, handed back, finished its registration"
    );
}

/// A claim whose run drain finds the gate held waits on the gate's wait alone, taken before its
/// try, which the holder letting go finishes; a holder letting go between the claim's failed try
/// and its return has finished it already, so neither order strands it (DESIGN.md 9.1.3, rule 4;
/// the audit's #3616/#3619 orders, now with no mark to race).
#[test]
fn a_claim_that_loses_the_token_is_woken_by_its_holders_release_however_late() {
    use crate::race;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = token_pool(&io, "/db/token-held");
    assert!(pool.memory.take_token(memory::LEND_TOKEN));
    let parked = claim_parked(&pool);
    assert!(!parked.finished(), "parked while the gate is held");
    pool.let_token_go(memory::LEND_TOKEN);
    assert!(parked.finished(), "the holder's release woke the claim");

    // The holder lets go between the claim's failed try and its return.
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = token_pool(&io, "/db/token-late");
    assert!(pool.memory.take_token(memory::LEND_TOKEN));
    {
        let pool = pool.clone();
        race::at("buffer.run_token_lost", move || {
            pool.let_token_go(memory::LEND_TOKEN)
        });
    }
    let parked = claim_parked(&pool);
    assert!(
        !race::pending("buffer.run_token_lost"),
        "the gate was not lost"
    );
    assert!(
        parked.finished(),
        "a release between the failed try and the return left the claim parked"
    );
}

/// To-prove 1 of 9.1.1, the step 4 stall on the real pool (the audit's drain-evidence reproducer,
/// ported): a copy kept under a cut, every other frame holding a page of 4 KiB, and the copy's
/// batch, were it a cache batch, left with the copy alone, so the emptiest. A claim of 64 KiB, an
/// arena page's class, gets its frame while the copy is kept: the copy lies in a lend batch, which
/// no drain chooses, and the claim drains a cache batch whose pages it can evict. Before lend
/// batches the claim drained the copy's batch and waited on the copy, which only the checkpoint
/// stopped at that claim would give back (DESIGN.md 9.1.1).
#[test]
fn a_kept_copy_never_holds_up_a_claim_of_another_class() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/kept-stall", MIN_KEEPING_BUDGET);
    let (small, arena) = (class(0), class(4));
    let mut g = drive(&io, || pool.allocate(small)).unwrap();
    let pid = g.pid();
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    drop(g);
    let mut scan = CutScan::default();
    assert!(matches!(pool.begin_cut(1, &mut scan), IOResult::Done(())));
    assert_eq!(scan.owed, vec![pid]);
    let g = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
    assert!(
        pool.keep(&[&g], &mut RoomWait::default()).is_ok(),
        "the copy kept"
    );
    drop(g);
    let copy_frame = (0..pool.memory.grains())
        .find(|&f| pool.header(f).status() == LENT)
        .expect("the copy's frame");
    let copy_batch = copy_frame / memory::GRAINS;
    // Every frame the cache can take, at most one pass of the region: none left free.
    let mut pages = Vec::new();
    for _ in 0..pool.memory.grains() {
        match pool.allocate_temporary(small).unwrap() {
            IOResult::Done(page) => pages.push((page.pid(), page.frame)),
            IOResult::IO(_) => break,
        }
    }
    // Were the copy in a cache batch, it would now be that batch's only frame taken.
    for &(pid, frame) in &pages {
        if frame / memory::GRAINS == copy_batch {
            drive(&io, || match pool.fix_exclusive(pid, None)? {
                IOResult::Done(page) => pool.free(page),
                IOResult::IO(io) => Ok(IOResult::IO(io)),
            })
            .unwrap();
        }
    }
    let mut obtained = false;
    for _ in 0..32 {
        match pool.allocate(arena).unwrap() {
            IOResult::Done(page) => {
                drop(page);
                obtained = true;
                break;
            }
            IOResult::IO(IOCompletions(c)) => {
                assert!(
                    c.finished(),
                    "the claim waits on another party while the copy is kept: {}",
                    pool.describe()
                );
                io.wait_for_completion(c).unwrap();
            }
        }
    }
    assert!(
        obtained,
        "no 64 KiB frame while the copy is kept: {}",
        pool.describe()
    );
    pool.end_cut(1);
}

/// To-prove of 9.1.1, the fast path: two writers want three frames of one class from a copy batch
/// with four free. Both read the batch with four free; the first adds and clears its three; the
/// second's add, made after, overshoots and is given back, so it goes to the rare path having taken
/// nothing. Neither holds part of a group.
#[test]
fn of_two_fast_requests_for_three_with_four_free_one_has_three_and_the_other_goes_rare() {
    use crate::race;
    use std::{cell::RefCell, rc::Rc};
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/fast-pair", MIN_KEEPING_BUDGET);
    let c = class(4);
    let fits = memory::frames_per_batch(c);
    let batch = pool.memory.batches - 1;
    assert!(pool.memory.take_free_batch(batch));
    pool.memory.publish_lent(batch, memory::LendKind::Copy, c);
    // All but four lent already.
    let lent = fits - 4;
    assert!(pool.memory.promise_lent(batch, c, lent).is_ok());
    assert!(pool.memory.take_lent_bits(batch, 0, (1u64 << lent) - 1));
    // The second has read the batch and chosen its frames; the first runs whole before its add.
    let first = Rc::new(RefCell::new(None));
    {
        let (pool, first) = (pool.clone(), first.clone());
        race::at("lend.fast_chosen", move || {
            *first.borrow_mut() = Some(pool.lend_fast(c, 3).map(|_| ()));
        });
    }
    let second = pool.lend_fast(c, 3).map(|_| ());
    assert!(!race::pending("lend.fast_chosen"));
    assert_eq!(*first.borrow(), Some(Ok(())), "the first has its three");
    assert_eq!(
        second,
        Err(lend::Rare::Overshoot),
        "the second, its add past the frames, goes rare with nothing"
    );
    let word = pool.memory.lend_word(batch);
    assert_eq!(
        word.count(),
        (lent + 3) as u64,
        "the second's add given back"
    );
    assert_eq!(
        pool.memory.lent_bits(batch, 0).count_ones(),
        1,
        "one frame left free"
    );
}

/// A pool for the lend road's proofs: every batch outside the core held by the cache's 4 KiB
/// class but the highest, a copy batch of `class` with `lent` of its frames lent; nothing else is
/// lend supply. Returns the copy batch.
fn lend_pool(io: &Arc<dyn IO>, base: &str, copies: SizeClass, lent: u32) -> (Arc<BufferPool>, u32) {
    let pool = open_on(io, base, MIN_KEEPING_BUDGET);
    let batch = pool.memory.batches - 1;
    assert!(pool.memory.take_free_batch(batch));
    pool.memory
        .publish_lent(batch, memory::LendKind::Copy, copies);
    if lent > 0 {
        assert!(pool.memory.promise_lent(batch, copies, lent).is_ok());
        let mut left = lent;
        for w in 0..memory::WORDS {
            let k = left.min(64);
            if k == 0 {
                break;
            }
            let mask = if k == 64 { u64::MAX } else { (1u64 << k) - 1 };
            assert!(pool.memory.take_lent_bits(batch, w, mask));
            left -= k;
        }
    }
    assert!(pool.hold_free_batches(class(0)) > 0);
    pool.memory.cutting.store(1, Ordering::SeqCst);
    (pool, batch)
}

fn want_of(classes: &[SizeClass]) -> lend::Want {
    let mut want = lend::Want::default();
    for &c in classes {
        want.add(c);
    }
    want
}

/// A refusal for frames: the gate's wait if it lost the gate, and the epoch it was short of.
fn frames_refusal(refused: Refused) -> (Option<Completion>, Option<u64>) {
    match refused.why {
        lend::Why::Frames { gate, quiet_at, .. } => (gate, quiet_at),
        lend::Why::Room { .. } => panic!("refused for room, not frames"),
    }
}

/// Two rare requests: the one that finds the gate held is turned down on the gate's wait, taken
/// before its try, and the holder letting go finishes it; it never blocks (DESIGN.md 9.1.3,
/// rule 4).
#[test]
fn a_rare_request_that_loses_the_token_is_started_by_its_holders_release() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let (pool, _) = lend_pool(&io, "/db/rare-pair", class(4), 0);
    assert!(pool.memory.take_token(memory::LEND_TOKEN));
    let refused = pool
        .lend_copies(&want_of(&[class(4), class(1)]))
        .expect_err("the token is held");
    let (gate, _) = frames_refusal(refused);
    let gate = gate.expect("turned down on the gate's wait");
    assert!(!gate.finished());
    pool.let_token_go(memory::LEND_TOKEN);
    assert!(gate.finished(), "the holder's release started it again");
}

/// The last return to a copy batch during a cut, emptying it, is supply for every class (an empty
/// copy batch is any class's by its retype): a request of another class short only of a batch
/// is woken by it, and its read then finds the batch it can retype (DESIGN.md 9.1.1; 9.1.3).
#[test]
fn a_last_return_emptying_a_copy_batch_wakes_a_request_of_another_class() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let (pool, batch) = lend_pool(&io, "/db/last-return", class(4), 0);
    let lent = pool
        .lend_copies(&want_of(&[class(4)]))
        .expect("a frame free");
    let want = want_of(&[class(1)]);
    pool.lend_copies(&want).expect_err("no batch for 8 KiB");
    // As `wait_for_room` does: the ticket, then the read.
    let ticket = pool.frames.supply.ticket();
    assert!(!pool.lend_hint(&want), "nothing moved yet");
    let mut lent = lent;
    let frame = lent.take(class(4)).expect("the frame");
    pool.give_back_lent(class(4), frame);
    assert_eq!(pool.memory.lend_word(batch).count(), 0);
    assert!(
        ticket.moved(),
        "the emptied copy batch woke the 8 KiB request"
    );
    assert!(
        pool.lend_hint(&want),
        "the 8 KiB request's read finds the batch to retype"
    );
}

/// The last loan of a copy batch given back after the cut ended (a failed checkpoint's abandon, or
/// a copy dropped late): no empty copy batch stays a lend batch; it goes back free (DESIGN.md
/// 9.1.1, Returns).
#[test]
fn a_copy_batch_emptied_after_its_cut_ended_goes_back_free() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let (pool, batch) = lend_pool(&io, "/db/late-return", class(4), 0);
    let mut lent = pool
        .lend_copies(&want_of(&[class(4)]))
        .expect("a frame free");
    pool.end_cut(1);
    assert!(pool.memory.is_lend_batch(batch), "a frame still lent in it");
    pool.give_back_lent(class(4), lent.take(class(4)).unwrap());
    assert!(
        !pool.memory.is_lend_batch(batch),
        "the empty copy batch stayed lent with no cut"
    );
    assert_eq!(pool.memory.lend_word(batch).kind(), memory::LendKind::None);
}

/// A batch a provide left lending, waiting for the rare path, when the cut ends: `end_cut`'s
/// sweep gives it back free, once (DESIGN.md 9.1.1, LENDING ends like an empty copy batch).
#[test]
fn a_provided_batch_still_lending_when_its_cut_ends_goes_back_free() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/lending-sweep", MIN_KEEPING_BUDGET);
    assert!(pool.hold_free_batches(class(0)) > 0);
    pool.memory.cutting.store(1, Ordering::SeqCst);
    let (holder, batch) = pool
        .memory
        .highest_cache_batch_outside_core()
        .expect("a cache batch outside the core");
    assert!(pool.memory.drain_for_lend(holder, batch));
    assert!(pool.memory.is_lending(batch), "provided and lending");
    pool.end_cut(1);
    assert!(!pool.memory.is_lending(batch), "the sweep left it lending");
    assert!(!pool.memory.is_lend_batch(batch));
    assert_eq!(pool.memory.lend_word(batch).kind(), memory::LendKind::None);
}

/// The order of supply (DESIGN.md 9.1.1): a copy batch of the class partly lent is taken before an
/// empty one of the class, so lent frames pack; and with none of the class, an empty copy batch of
/// another class is retyped before a free batch is taken, so the copy batches a long cut keeps
/// stay those its copies need, not every class's peak.
#[test]
fn the_lend_road_takes_the_fullest_copy_batch_then_retypes_an_empty_one_before_a_free_one() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/supply-order", MIN_KEEPING_BUDGET);
    let c = class(4);
    let top = pool.memory.batches - 1;
    let (partly, empty) = (top, top - 1);
    for b in [partly, empty] {
        assert!(pool.memory.take_free_batch(b));
        pool.memory.publish_lent(b, memory::LendKind::Copy, c);
    }
    assert!(pool.memory.promise_lent(partly, c, 5).is_ok());
    assert!(pool.memory.take_lent_bits(partly, 0, 0b11111));
    pool.memory.cutting.store(1, Ordering::SeqCst);
    let mut lent = pool.lend_rare(&want_of(&[c])).expect("frames free");
    let frame = lent.take(c).unwrap();
    assert_eq!(
        frame / memory::GRAINS,
        partly,
        "the partly lent batch first"
    );
    assert_eq!(
        pool.memory.lend_word(empty).count(),
        0,
        "the empty one untouched"
    );
    pool.give_back_lent(c, frame);

    // None of 8 KiB: the empty copy batch of 64 KiB is retyped, the free batch left.
    let free_before: Vec<u32> = pool.memory.free_batches_outside_core().collect();
    assert!(!free_before.is_empty(), "a free batch is there to take");
    let mut lent = pool
        .lend_rare(&want_of(&[class(1)]))
        .expect("an empty batch retyped");
    let frame = lent.take(class(1)).unwrap();
    assert_eq!(
        frame / memory::GRAINS,
        empty,
        "the empty copy batch, retyped"
    );
    assert!(pool.memory.lend_word(empty).is_copy_of(class(1)));
    let free_after: Vec<u32> = pool.memory.free_batches_outside_core().collect();
    assert_eq!(
        free_after, free_before,
        "no free batch taken while one could be retyped"
    );
    pool.give_back_lent(class(1), frame);
}

/// A rare plan that loses a piece after taking another keeps what it took, never plans the batch
/// it lost again in that ownership, and plans the rest from what it reads: the fullest copy batch
/// gave one frame, the next lost its planned frame to another taker, and the rest comes from an
/// empty copy batch of another class, retyped (DESIGN.md 9.1.1, Take).
#[test]
fn a_rare_plan_keeps_what_it_took_and_never_plans_a_lost_batch_again() {
    use crate::race;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/rare-lost", MIN_KEEPING_BUDGET);
    let c = class(4);
    let fits = memory::frames_per_batch(c);
    let top = pool.memory.batches - 1;
    let (a, b, spare) = (top, top - 1, top - 2);
    for (batch, lent, of) in [(a, fits - 1, c), (b, fits - 2, c), (spare, 0, class(1))] {
        assert!(pool.memory.take_free_batch(batch));
        pool.memory.publish_lent(batch, memory::LendKind::Copy, of);
        if lent > 0 {
            assert!(pool.memory.promise_lent(batch, of, lent).is_ok());
            assert!(pool.memory.take_lent_bits(batch, 0, (1u64 << lent) - 1));
        }
    }
    pool.memory.cutting.store(1, Ordering::SeqCst);
    // The plan's second piece is b's frame: taken by another just before it.
    race::at("lend.take_frames", || {});
    {
        let pool = pool.clone();
        race::at("lend.take_frames", move || {
            let bit = 1u64 << (fits - 2);
            assert!(pool.memory.promise_lent(b, c, 1).is_ok());
            assert!(
                pool.memory.take_lent_bits(b, 0, bit),
                "b's planned frame taken"
            );
        });
    }
    let again = pool.lend_counts().planned_again;
    let mut lent = pool.lend_rare(&want_of(&[c, c])).expect("two frames");
    assert!(!race::pending("lend.take_frames"));
    assert_eq!(
        pool.lend_counts().planned_again,
        again + 1,
        "planned the rest once"
    );
    let mut batches: Vec<u32> = (0..2)
        .map(|_| lent.take(c).expect("a frame") / memory::GRAINS)
        .collect();
    batches.sort_unstable();
    assert_eq!(
        batches,
        vec![spare, a],
        "the frame from the fullest kept, the rest from the retyped batch, b not planned again"
    );
}

/// A fast request reserving the last frame of the class while a rare owner plans: the owner is
/// short and turned down, takes its ticket at `wait_for_room` and parks; the fast request's frame
/// given back later is a change of the supply that wakes it, and its read then finds the frame
/// (DESIGN.md 9.1.3, rule 5: what the marks and `quiet` did, one rule does).
#[test]
fn a_rare_owner_short_while_a_fast_request_reserves_is_woken_by_its_return() {
    use crate::race;
    use std::{cell::RefCell, rc::Rc};
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let c = class(4);
    // Every batch outside the core a copy batch of the class, full but for the last frame of the
    // last one, the core the cache's: nothing to provide.
    let fits = memory::frames_per_batch(c);
    let pool = open_on(&io, "/db/reserving", MIN_KEEPING_BUDGET);
    let last = pool.memory.batches - 1;
    for b in memory::CORE..pool.memory.batches {
        let lent = if b == last { fits - 1 } else { fits };
        assert!(pool.memory.take_free_batch(b));
        pool.memory.publish_lent(b, memory::LendKind::Copy, c);
        assert!(pool.memory.promise_lent(b, c, lent).is_ok());
        assert!(pool.memory.take_lent_bits(b, 0, u64::MAX >> (64 - lent)));
    }
    pool.hold_free_batches(class(0));
    pool.memory.cutting.store(1, Ordering::SeqCst);
    assert!(pool.memory.highest_cache_batch_outside_core().is_none());
    let refused = Rc::new(RefCell::new(None));
    {
        let (pool, refused) = (pool.clone(), refused.clone());
        race::at("lend.fast_taken", move || {
            *refused.borrow_mut() = Some(
                pool.lend_rare(&want_of(&[c]))
                    .expect_err("the last frame reserved"),
            );
        });
    }
    let mut lent = pool
        .lend_fast(c, 1)
        .expect("the fast request has the last frame");
    assert!(!race::pending("lend.fast_taken"));
    let refused = refused
        .borrow_mut()
        .take()
        .expect("the owner was turned down");
    let wait = match pool.wait_for_room(refused).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(()) => panic!("started again with the frame still lent"),
    };
    assert!(!wait.finished(), "parked: the frame is lent");
    pool.give_back_lent(c, lent.take(c).unwrap());
    assert!(wait.finished(), "the fast request's return woke the owner");
    assert!(
        pool.lend_hint(&want_of(&[c])),
        "and its read finds the frame"
    );
}

/// The image bank exhausted at `S` leases: the next lease waits, on the completion a lease's drop
/// finishes, taken before its count; the drop of a block (its last owner letting go, as a write's
/// request does once retired) wakes it and the frame goes back to the bank (DESIGN.md 9.1.1,
/// Images do not wait on copies either).
#[test]
fn a_lease_past_the_bank_waits_for_a_leased_block_to_drop() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_with(
        &io,
        "/db/bank",
        BufferConfig {
            lent_at_open: LentAtOpen {
                ring: None,
                bank: 2,
            },
            ..BufferConfig::with_budget(MIN_KEEPING_BUDGET + memory::BATCH)
        },
    );
    let leased: Vec<_> = (0..2)
        .map(|_| match pool.lease_image(class(2)).unwrap() {
            IOResult::Done(block) => block,
            IOResult::IO(_) => panic!("a frame of the bank free"),
        })
        .collect();
    assert_eq!(
        leased[0].len(),
        class(2).size(),
        "an image writes its own length"
    );
    let wait = match pool.lease_image(class(4)).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(_) => panic!("a third lease past a bank of two"),
    };
    assert!(!wait.finished(), "waits for a lease's drop");
    let mut leased = leased;
    drop(leased.pop());
    assert!(wait.finished(), "the block's drop woke the waiting lease");
    match pool.lease_image(class(4)).unwrap() {
        IOResult::Done(block) => drop(block),
        IOResult::IO(_) => panic!("the frame went back to the bank"),
    }
}

/// A writer turned down for a frame, with no generation moved since its plan, no cache batch
/// outside the core to provide and no relief: `wait_for_room` parks it on what it registered on,
/// never starting it again on supply that merely appears to exist, and the next return of its
/// class wakes it (DESIGN.md 9.1.1, Turned down).
#[test]
fn a_turned_down_writer_with_nothing_new_parks_until_the_next_return() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let c = class(4);
    let fits = memory::frames_per_batch(c);
    // Every batch outside the core a full copy batch, the core the cache's: nothing to provide.
    let pool = open_on(&io, "/db/parked-writer", MIN_KEEPING_BUDGET);
    for b in memory::CORE..pool.memory.batches {
        assert!(pool.memory.take_free_batch(b));
        pool.memory.publish_lent(b, memory::LendKind::Copy, c);
        assert!(pool.memory.promise_lent(b, c, fits).is_ok());
        assert!(pool.memory.take_lent_bits(b, 0, u64::MAX >> (64 - fits)));
    }
    let full = pool.memory.batches - 1;
    pool.hold_free_batches(class(0));
    pool.memory.cutting.store(1, Ordering::SeqCst);
    assert!(pool.memory.highest_cache_batch_outside_core().is_none());
    let refused = pool.lend_copies(&want_of(&[c])).expect_err("nothing free");
    let wait = match pool.wait_for_room(refused).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(()) => panic!("started again with nothing changed"),
    };
    assert!(!wait.finished(), "parked on its registration");
    pool.give_back_lent(c, memory::frame_at(c, full, 0));
    assert!(wait.finished(), "the return of its class woke it");
}

/// The auditor's `refund-pingpong` trace (2026-11-08): two changes each wanting a 4 KiB and a
/// 64 KiB frame, every batch outside the core a full 64 KiB copy batch, the core the cache's and
/// nothing to provide. Neither takes anything and both are turned down; neither turn-down is a
/// supply event, so neither starts the other again: each parks on what it registered on, and the
/// generations its plan read stand (DESIGN.md 9.1.1, Turned down).
#[test]
fn of_two_changes_turned_down_for_lack_of_frames_neither_starts_the_other() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let c = class(4);
    let fits = memory::frames_per_batch(c);
    let pool = open_on(&io, "/db/refund-pingpong", MIN_KEEPING_BUDGET);
    for b in memory::CORE..pool.memory.batches {
        assert!(pool.memory.take_free_batch(b));
        pool.memory.publish_lent(b, memory::LendKind::Copy, c);
        assert!(pool.memory.promise_lent(b, c, fits).is_ok());
        assert!(pool.memory.take_lent_bits(b, 0, u64::MAX >> (64 - fits)));
    }
    pool.hold_free_batches(class(0));
    pool.memory.cutting.store(1, Ordering::SeqCst);
    assert!(pool.memory.highest_cache_batch_outside_core().is_none());
    let want = want_of(&[class(0), c]);
    let first = pool.lend_copies(&want).expect_err("no frame to lend");
    let second = pool.lend_copies(&want).expect_err("no frame to lend");
    assert_eq!(pool.lend_counts().turned_down, 2);
    let parked = |refused| match pool.wait_for_room(refused).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(()) => panic!("started again by the other's turn-down"),
    };
    let (first, second) = (parked(first), parked(second));
    assert!(
        !first.finished() && !second.finished(),
        "a turn-down woke a parked change"
    );
    for b in memory::CORE..pool.memory.batches {
        assert_eq!(
            pool.memory.lend_word(b).count(),
            fits as u64,
            "batch {b} gave a frame"
        );
    }
    // The next real supply, a return of 64 KiB, starts both.
    pool.give_back_lent(c, memory::frame_at(c, pool.memory.batches - 1, 0));
    assert!(
        first.finished() && second.finished(),
        "the return started both"
    );
}

/// Three copy batches of 64 KiB frames with one frame free each, every other batch outside the
/// core full and nothing to provide: a change wanting three frames finds no batch with three free,
/// and the rare path's plan splits it, one frame from each (DESIGN.md 9.1.1, the order of supply).
#[test]
fn a_change_of_three_split_across_three_batches_with_one_free_each_is_served() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let c = class(4);
    let fits = memory::frames_per_batch(c);
    let pool = open_on(&io, "/db/split-three", MIN_KEEPING_BUDGET);
    let top = pool.memory.batches - 1;
    let open: Vec<u32> = (top - 2..=top).collect();
    for b in memory::CORE..pool.memory.batches {
        let lent = if open.contains(&b) { fits - 1 } else { fits };
        assert!(pool.memory.take_free_batch(b));
        pool.memory.publish_lent(b, memory::LendKind::Copy, c);
        assert!(pool.memory.promise_lent(b, c, lent).is_ok());
        assert!(pool.memory.take_lent_bits(b, 0, u64::MAX >> (64 - lent)));
    }
    pool.hold_free_batches(class(0));
    pool.memory.cutting.store(1, Ordering::SeqCst);
    let mut lent = pool
        .lend_copies(&want_of(&[c, c, c]))
        .expect("the plan split the three across the batches");
    let mut from = Vec::new();
    for _ in 0..3 {
        from.push(lent.take(c).expect("three frames lent") / memory::GRAINS);
    }
    from.sort_unstable();
    assert_eq!(from, open, "one frame from each batch with one free");
    for &b in &open {
        assert_eq!(pool.memory.lend_word(b).count(), fits as u64);
        pool.give_back_lent(c, memory::frame_at(c, b, fits - 1));
    }
}

/// A return publishes its frame, its bit and its count, before its supply event: a request woken
/// by the event finds the frame free as it wakes, never the event ahead of the frame (DESIGN.md
/// 9.1.1, Returns).
#[test]
fn a_return_publishes_its_frame_before_it_wakes_a_request() {
    use crate::sync::atomic::AtomicUsize;
    /// Wakes by reading the batch the return gives a frame back to.
    struct SeesFrame {
        pool: Arc<BufferPool>,
        batch: u32,
        woke: AtomicUsize,
        free: AtomicUsize,
    }
    impl std::task::Wake for SeesFrame {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.woke.fetch_add(1, Ordering::SeqCst);
            let fits = memory::frames_per_batch(class(4)) as u64;
            if self.pool.memory.lend_word(self.batch).count() < fits
                && self.pool.memory.lent_bits(self.batch, 0) & 1 != 0
            {
                self.free.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let c = class(4);
    let fits = memory::frames_per_batch(c);
    let pool = open_on(&io, "/db/return-order", MIN_KEEPING_BUDGET);
    for b in memory::CORE..pool.memory.batches {
        assert!(pool.memory.take_free_batch(b));
        pool.memory.publish_lent(b, memory::LendKind::Copy, c);
        assert!(pool.memory.promise_lent(b, c, fits).is_ok());
        assert!(pool.memory.take_lent_bits(b, 0, u64::MAX >> (64 - fits)));
    }
    pool.hold_free_batches(class(0));
    pool.memory.cutting.store(1, Ordering::SeqCst);
    let want = want_of(&[c]);
    pool.lend_copies(&want).expect_err("nothing free");
    let wait = pool.frames.supply.ticket().remote();
    assert!(!pool.lend_hint(&want), "nothing moved yet");
    let full = pool.memory.batches - 1;
    let sees = Arc::new(SeesFrame {
        pool: pool.clone(),
        batch: full,
        woke: AtomicUsize::new(0),
        free: AtomicUsize::new(0),
    });
    wait.set_waker(&std::task::Waker::from(sees.clone()));
    pool.give_back_lent(c, memory::frame_at(c, full, 0));
    let woke = sees.woke.load(Ordering::SeqCst);
    assert!(woke > 0, "the return woke the request");
    assert_eq!(
        sees.free.load(Ordering::SeqCst),
        woke,
        "woken before its frame was published"
    );
}

/// A writer turned down for frames whose relief finds the checkpoint's state held by another
/// caller waits for the state to be let go: the holder may stop before a frame of its classes
/// comes back, and the writer steps the checkpoint next (the auditor's lead of 2026-11-08). One
/// whose checkpoint the checkpointing thread steps is woken by it too: there is one supply, and a
/// state let go is a change of it (DESIGN.md 9.1.3), a wake whose look costs its read.
#[test]
fn a_writer_turned_down_for_frames_wakes_when_a_held_checkpoint_is_let_go() {
    struct Says(fn() -> Relieved);
    impl Relief for Says {
        fn relieve(&self, _: Retry<'_>) -> Relieved {
            (self.0)()
        }

        fn demand(&self) -> Option<crate::storage::atoll::executor::Ticket> {
            None
        }
    }
    for (says, base, woken) in [
        (Says(|| Relieved::Held), "/db/relief-held", true),
        (Says(|| Relieved::Elsewhere), "/db/relief-thread", true),
    ] {
        let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
        let c = class(4);
        let fits = memory::frames_per_batch(c);
        let pool = open_on(&io, base, MIN_KEEPING_BUDGET);
        for b in memory::CORE..pool.memory.batches {
            assert!(pool.memory.take_free_batch(b));
            pool.memory.publish_lent(b, memory::LendKind::Copy, c);
            assert!(pool.memory.promise_lent(b, c, fits).is_ok());
            assert!(pool.memory.take_lent_bits(b, 0, u64::MAX >> (64 - fits)));
        }
        pool.hold_free_batches(class(0));
        pool.memory.cutting.store(1, Ordering::SeqCst);
        pool.set_relief(Arc::new(says));
        let refused = pool.lend_copies(&want_of(&[c])).expect_err("nothing free");
        let wait = match pool.wait_for_room(refused).unwrap() {
            IOResult::IO(IOCompletions(wait)) => wait,
            IOResult::Done(()) => panic!("{base}: started again with nothing changed"),
        };
        assert!(!wait.finished(), "{base}: parked on its registration");
        // What every `StateHold` let go does.
        pool.wake_room();
        assert_eq!(wait.finished(), woken, "{base}: the state let go");
    }
}

/// A writer refused frames, parked, when the cut ends with nothing returned to its classes (the
/// shortage is every batch outside the core lent full): the cut's end is its event, for it needs
/// no copy any more, and it starts again on it (the auditor's lead of 2026-11-08; DESIGN.md 9.1.1,
/// Turned down).
#[test]
fn a_writer_refused_frames_starts_again_when_the_cut_ends_with_nothing_returned() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let c = class(4);
    let fits = memory::frames_per_batch(c);
    let pool = open_on(&io, "/db/cut-ends-parked", MIN_KEEPING_BUDGET);
    for b in memory::CORE..pool.memory.batches {
        assert!(pool.memory.take_free_batch(b));
        pool.memory.publish_lent(b, memory::LendKind::Copy, c);
        assert!(pool.memory.promise_lent(b, c, fits).is_ok());
        assert!(pool.memory.take_lent_bits(b, 0, u64::MAX >> (64 - fits)));
    }
    pool.hold_free_batches(class(0));
    pool.memory.cutting.store(1, Ordering::SeqCst);
    let refused = pool
        .lend_copies(&want_of(&[class(0), c]))
        .expect_err("nothing free");
    let wait = match pool.wait_for_room(refused).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(()) => panic!("started again with nothing changed"),
    };
    assert!(!wait.finished(), "parked on its registration");
    pool.end_cut(1);
    assert!(wait.finished(), "the cut's end started the writer again");
    for b in memory::CORE..pool.memory.batches {
        assert_eq!(
            pool.memory.lend_word(b).count(),
            fits as u64,
            "batch {b} gave a frame"
        );
    }
}

/// A claim's registration on the released frames is finished when a promise given back by a later
/// look releases a batch that began draining after the promise: that release made memory
/// available, and it is signalled as a put's release is (the auditor's race trace of 2026-11-08,
/// the deadlock and the spin of the directory race variant; DESIGN.md 9.1.2).
#[cfg(not(atoll_loom))]
#[test]
fn a_given_back_promise_that_releases_a_draining_batch_tells_the_released_frames() {
    use crate::race;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/promise-release", MIN_BUDGET);
    let c = class(9);
    assert_eq!(memory::frames_per_batch(c), 1, "one frame a batch");
    let page = drive(&io, || pool.allocate(c)).unwrap();
    let (pid, frame) = (page.pid(), page.frame);
    drop(page);
    drive(&io, || match pool.fix_exclusive(pid, None)? {
        IOResult::Done(page) => pool.free(page),
        IOResult::IO(io) => Ok(IOResult::IO(io)),
    })
    .unwrap();
    let batch = frame / memory::GRAINS;
    assert!(pool.quiet_at(frame, c) > 0, "the frame's free is stamped");
    let waiting = pool.released_wait();
    assert!(!waiting.finished());
    {
        let pool = pool.clone();
        race::at("memory.count_back", move || {
            assert!(
                pool.memory.drain(c, batch),
                "the drain started under the promise"
            );
        });
    }
    // A look whose frame is usable when searched and not when claimed: the promise is given back,
    // the batch's last count.
    assert!(matches!(
        pool.memory.take_where(c, memory::tests::usable_once()),
        memory::Take::Unusable
    ));
    assert!(!race::pending("memory.count_back"));
    assert!(waiting.finished(), "the release told the released frames");
}

/// The audit's P1-A (2026-11-08): a round's take of a page is lost to its holder, who lets it go
/// before the round marks it. The mark finds the latch free and the round says the latch changed,
/// so the claim looks again rather than counting the page as neither taken nor waited on
/// (DESIGN.md 9.1.2).
#[test]
fn a_latch_let_go_between_a_lost_take_and_the_mark_is_a_change() {
    use crate::race;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/lost-take", MIN_BUDGET);
    let c = class(9);
    let page = drive(&io, || pool.allocate(c)).unwrap();
    let frame = page.frame;
    drop(page);
    let hold = Held::try_take(FrameLatch {
        frames: pool.frames.clone(),
        frame,
    })
    .expect("the page's latch is free");
    let hold = std::rc::Rc::new(std::cell::RefCell::new(Some(hold)));
    {
        let hold = hold.clone();
        race::at("buffer.evict_lost", move || {
            hold.borrow_mut().take();
        });
    }
    let (mut outgoing, mut room) = (Vec::new(), Room::default());
    assert!(
        !pool.evict_or_write(c, frame, &mut outgoing, &mut room),
        "the take was lost"
    );
    assert!(
        hold.borrow().is_none(),
        "the holder let go between the lost take and the mark"
    );
    assert!(
        room.changed && !room.marked,
        "a latch let go before the mark is a change, not nothing: {room:?}"
    );
}

/// The audit's P1-C (2026-11-08): a claim that looked while a new page was being installed found
/// its frame free and nothing to take; the page then becomes evictable with its first release, and
/// that release tells the released frames, its installer having marked its latch (DESIGN.md 9.1.2).
#[test]
fn a_new_pages_first_release_tells_a_waiting_claim() {
    for b in backends() {
        let pool = open(&b, MIN_BUDGET);
        let page = drive(&b.io, || pool.allocate(class(0))).unwrap();
        // Nothing left in flight on any backend that could change the supply before the release
        // does: waited for, as the ring's delivery thread can retire a request of the allocation
        // after its completion is finished.
        let requests = pool.store.counted().requests();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while requests.outstanding() != 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(
            requests.outstanding(),
            0,
            "{}: the allocation left a request outstanding",
            b.name
        );
        let waiting = pool.released_wait();
        assert!(!waiting.finished());
        drop(page);
        assert!(
            waiting.finished(),
            "{}: the new page's first release told the claim",
            b.name
        );
    }
}

/// The audit's P1-B (2026-11-08): a drain a claim started for another class meets a page latched by
/// its holder. It marks the latch, so the holder's release wakes the claim parked on the drain,
/// as eviction's rounds do (DESIGN.md 9.1.2).
#[test]
fn a_drain_marks_the_latched_pages_it_meets() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/drain-marks", MIN_BUDGET);
    let page = drive(&io, || pool.allocate(class(0))).unwrap();
    let frame = page.frame;
    drop(page);
    let batch = frame / memory::GRAINS;
    let hold = Held::try_take(FrameLatch {
        frames: pool.frames.clone(),
        frame,
    })
    .expect("the page's latch is free");
    let room = pool.drain(class(0), batch).unwrap();
    assert!(
        room.marked,
        "the drain marked the latch it found held: {room:?}"
    );
    let waiting = pool.released_wait();
    drop(hold);
    assert!(
        waiting.finished(),
        "the holder's release told the released frames"
    );
}

/// Every hold on a frame's latch in the pool is taken through a reference that names the frames
/// (`FrameRef`, `FrameLatch`), so a release a claim marked tells the released frames; none is taken
/// on a bare `&VersionedLatch` (DESIGN.md 9.1.2). The census reads the pool's sources, tests left out,
/// and its control finds the bare form in a line written for it.
#[test]
fn no_frame_latch_is_held_through_a_bare_reference() {
    let bare = |line: &str| {
        (line.contains("Held::try_take(&") || line.contains("Held::try_take_at(&"))
            && line.contains("latch")
    };
    assert!(
        bare("let hold = Held::try_take_at(&h.latch, version);"),
        "the control"
    );
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("storage/atoll/buffer");
    let mut read = 0;
    let mut found = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|e| e != "rs") || path.file_name().unwrap() == "tests.rs" {
            continue;
        }
        read += 1;
        let text = std::fs::read_to_string(&path).unwrap();
        // The sources' own tests are not holds of the pool's frames.
        let text = text.split("#[cfg(test)]\nmod tests").next().unwrap();
        for (i, line) in text.lines().enumerate() {
            if bare(line) {
                found.push(format!("{}:{}: {}", path.display(), i + 1, line.trim()));
            }
        }
    }
    assert!(read >= 5, "the census read {read} files");
    assert!(
        found.is_empty(),
        "frame latches held through a bare reference: {found:#?}"
    );
}

/// A promise given back and a mark do not tell the released frames; the marked latch let go does
/// (DESIGN.md 9.1.2).
#[test]
fn no_promise_given_back_and_no_mark_tells_the_released_frames() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/no-signal", MIN_BUDGET);
    let page = drive(&io, || pool.allocate(class(0))).unwrap();
    let frame = page.frame;
    drop(page);
    let waiting = pool.released_wait();
    assert!(!waiting.finished());
    // A promise given back: a take whose only frame is unusable.
    let other = pool.memory.take(class(0)).unwrap();
    assert!(matches!(
        pool.memory.take_where(class(0), |_| false),
        memory::Take::Unusable
    ));
    // A mark, and a mark lost to a release.
    let hold = Held::try_take(FrameLatch {
        frames: pool.frames.clone(),
        frame,
    })
    .unwrap();
    let (mut outgoing, mut room) = (Vec::new(), Room::default());
    assert!(!pool.evict_or_write(class(0), frame, &mut outgoing, &mut room));
    assert!(room.marked);
    assert!(
        !waiting.finished(),
        "a promise given back and a mark told no one"
    );
    drop(hold);
    assert!(
        waiting.finished(),
        "the marked latch let go told them: the control"
    );
    assert!(!pool.memory.put(class(0), other));
}

/// The audit's `final-quieting` schedule (2026-11-08) on the claim itself: every frame is given
/// back just before the claim's fresh registration, stamped past the step the caller is in, so the
/// last look finds free frames not yet quiet. The registration is put on their epoch, and the
/// caller's step ending finishes it (DESIGN.md 9.1.2).
#[test]
fn a_last_look_that_finds_frames_not_yet_quiet_waits_on_their_epoch() {
    use crate::race;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/final-quieting", MIN_BUDGET);
    let c = class(9);
    let mut taken = Vec::new();
    for _ in 0..pool.memory.batches {
        match pool.claim(c, Purpose::New, &[]).unwrap() {
            IOResult::Done(f) => taken.push(f),
            IOResult::IO(_) => panic!("an empty pool's claim waited"),
        }
    }
    let step = pool.store.epochs().enter();
    {
        let pool = pool.clone();
        race::at("buffer.claim_fresh", move || {
            for f in taken {
                pool.free_frame(c, f, pool.take_free_latch(f));
            }
        });
    }
    let wait = match pool.claim(c, Purpose::Read, &[]).unwrap() {
        IOResult::IO(IOCompletions(w)) => w,
        IOResult::Done(_) => panic!("a read took a frame given back after its step began"),
    };
    assert!(!race::pending("buffer.claim_fresh"));
    assert!(
        !wait.finished(),
        "the frames are not quiet while the step lasts"
    );
    drop(step);
    assert!(
        wait.finished(),
        "the step's end, their epoch, finished the registration"
    );
}

/// A fast request past its read of `slow` before the rare owner set it, paused at its chosen
/// batch, takes the frame the owner's plan read free: the owner's next read of those bits finds
/// none. It loses that batch for its ownership and plans on, and either grants the whole request
/// or refuses it holding nothing, never a partial placement as a grant (the auditor's
/// owner-overlap test of 2026-11-09, ported; DESIGN.md 9.1.3, the hint).
#[cfg(not(shuttle))]
#[test]
fn a_fast_request_already_past_slow_never_leaves_the_owner_a_partial_grant() {
    use crate::race;
    use std::{cell::RefCell, rc::Rc, sync::mpsc, time::Duration};
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_with(
        &io,
        "/db/owner-fast-overlap",
        BufferConfig::with_budget(7 * memory::BATCH),
    );
    let c = class(4);
    let fits = memory::frames_per_batch(c);
    assert!(fits > 1 && fits <= 64);
    pool.memory.cutting.store(1, Ordering::SeqCst);
    // Copy batch 5 with one frame free; batch 6 the ring's: nothing else to plan.
    assert!(pool.memory.take_free_batch(5));
    pool.memory.publish_lent(5, memory::LendKind::Copy, c);
    assert!(pool.memory.take_free_batch(6));
    pool.memory.publish_lent(6, memory::LendKind::Ring, c);
    let seeded = fits - 1;
    assert!(pool.memory.promise_lent(5, c, seeded).is_ok());
    assert!(pool.memory.take_lent_bits(5, 0, (1u64 << seeded) - 1));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let fast = {
        let pool = pool.clone();
        std::thread::spawn(move || {
            race::at("lend.fast_chosen", move || {
                let _ = ready_tx.send(());
                let _ = resume_rx.recv_timeout(Duration::from_secs(10));
            });
            let _ = done_tx.send(pool.lend_fast(c, 1).ok());
        })
    };
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the fast request chose its batch before `slow` was set");
    // Whether the plan took the batch, whether it was the owner's, and what it lent.
    type Seen = Option<(bool, bool, Option<lend::Lent>)>;
    let seen: Rc<RefCell<Seen>> = Rc::default();
    {
        let (pool, seen) = (pool.clone(), seen.clone());
        race::at("lend.owner_plans_frames", move || {
            let slow = pool.lend.slow.load(Ordering::SeqCst);
            let one_free = pool.memory.lent_bits(5, 0).count_ones() == 1;
            let _ = resume_tx.send(());
            let fast = done_rx.recv_timeout(Duration::from_secs(10)).ok().flatten();
            *seen.borrow_mut() = Some((slow, one_free, fast));
        });
    }
    let own = pool.lend_rare(&want_of(&[c]));
    assert!(
        !race::pending("lend.owner_plans_frames"),
        "the owner never planned frames"
    );
    fast.join().unwrap();
    let (slow, one_free, fast_lent) = seen.borrow_mut().take().expect("the overlap");
    let fast_frames = fast_lent.as_ref().map_or(0, |l| l.count_of(c));
    let granted = own.as_ref().ok().map(|l| l.count_of(c));
    for mut lent in fast_lent.into_iter().chain(own.ok()) {
        while let Some(frame) = lent.take(c) {
            pool.give_back_lent(c, frame);
        }
    }
    assert!(
        slow && one_free && fast_frames == 1,
        "the overlap was not reached: slow {slow}, one free {one_free}, fast {fast_frames}"
    );
    assert!(
        granted.is_none_or(|n| n == 1),
        "the owner granted {granted:?} frames of one asked for"
    );
}

/// Memory handed to a class while a step is under way stays with it until that step ends: a claim
/// of another class, short and holding nothing, does not drain it, and waits on the hand-over's
/// epoch, which the step's end reaches (DESIGN.md 9.1.3; the directory race variant's livelock,
/// where two classes drained one empty batch away from each other).
#[test]
fn memory_handed_to_a_class_is_not_drained_by_another_until_the_steps_under_way_end() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/handed", MIN_BUDGET);
    let (held, short) = (class(1), class(0));
    for c in SizeClass::all() {
        assert_eq!(
            pool.memory.emptiest(c, short, |b| pool.drainable(short, b)),
            None,
            "a batch {c:?} held at open"
        );
    }
    // Outcomes are recorded and asserted once every step and the pool are let go, so a failed
    // assertion unwinds through nothing of Atoll's.
    let step = pool.store.epochs().enter();
    let mut handed = 0;
    while pool.memory.grow(held) {
        handed += 1;
    }
    let wait = match pool.claim(short, Purpose::New, &[]).unwrap() {
        IOResult::IO(IOCompletions(w)) => Some(w),
        IOResult::Done(_) => None,
    };
    let drained = !pool.memory.draining().is_empty();
    let early = wait.as_ref().is_some_and(|w| w.finished());
    drop(step);
    let reached = wait.as_ref().is_some_and(|w| w.finished());
    let after = wait.is_some().then(|| claim_without_waiting(&pool, short));
    let after_class = after.map(|f| f.map(|f| pool.memory.class_of(f)));
    drop(wait);
    drop(pool);
    assert!(handed > 0);
    assert!(
        after_class.is_some(),
        "a claim drained memory handed to another class mid-step"
    );
    assert!(!drained, "a drain started");
    assert!(
        !early,
        "the wait finished while the hand-over's step was under way"
    );
    assert!(reached, "the step's end reached the hand-over's epoch");
    let after_class = after_class
        .unwrap()
        .unwrap_or_else(|e| panic!("after the step: {e}"));
    assert_eq!(after_class, short);
}

/// Claims a frame of `class` where memory is there to drain, never waiting: each look again
/// follows a drain or an eviction it started, at most one per batch, so a claim that waits on
/// anything, or looks again more often than that, failed to take memory it could have. Returns
/// what went wrong rather than panicking, so the caller lets Atoll's state go before it asserts.
fn claim_without_waiting(pool: &Arc<BufferPool>, class: SizeClass) -> Result<u32, &'static str> {
    for _ in 0..=pool.memory.batches {
        match pool.claim(class, Purpose::New, &[]).unwrap() {
            IOResult::Done(frame) => return Ok(frame),
            IOResult::IO(IOCompletions(wait)) if !wait.finished() => {
                return Err("the claim waited with memory there to drain")
            }
            IOResult::IO(_) => {}
        }
    }
    Err("the claim looked again more often than there are batches")
}

/// A batch handed over again between a claim's choice and its drain is not drained by that
/// choice: the drain reads the stamp after the word, so it sees the new hand-over, held for the
/// step under way (DESIGN.md 9.1.3).
#[test]
fn a_hand_over_between_a_claims_choice_and_its_drain_is_not_drained_by_it() {
    use crate::race;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/handed-again", MIN_BUDGET);
    let (held, short) = (class(1), class(0));
    while pool.memory.grow(held) {}
    let step = pool.store.epochs().enter();
    // The batch the claim chooses: the first of the emptiest, all of them free.
    let chosen = pool
        .memory
        .emptiest(held, short, |b| pool.drainable(short, b))
        .unwrap();
    // What the action found: the batch drained, handed again, and held.
    let setup = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
    {
        let (pool, setup) = (pool.clone(), setup.clone());
        race::at("buffer.chosen", move || {
            let drained = pool.memory.drain(held, chosen);
            let handed = pool.memory.grow(held);
            let horizon = pool.store.epochs().horizon();
            let is_held = pool.memory.held_until(short, chosen, horizon).is_some();
            *setup.lock() = Some((drained, handed, is_held));
        });
    }
    let frame = claim_without_waiting(&pool, short);
    let chose = !race::pending("buffer.chosen");
    drop(step);
    drop(pool);
    let setup = setup.lock().take();
    assert!(chose, "the claim chose a batch");
    assert_eq!(
        setup,
        Some((true, true, true)),
        "the chosen batch drained, handed again and held"
    );
    let frame = frame.unwrap_or_else(|e| panic!("{e}"));
    assert_ne!(
        frame / memory::GRAINS,
        chosen,
        "the claim drained the batch handed over after its choice"
    );
}

/// A run for a class of more than one batch is not chosen while any batch of it is held, though
/// the batch the choice starts from is not (DESIGN.md 9.1.3, every batch of the run).
#[test]
fn a_run_is_not_chosen_while_a_batch_of_it_is_held() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/handed-run", MIN_BUDGET);
    let (held, large) = (class(1), class(memory::BATCH_CLASS + 1));
    while pool.memory.grow(held) {}
    let first = pool
        .memory
        .emptiest(held, large, |b| pool.drainable(large, b))
        .unwrap();
    let step = pool.store.epochs().enter();
    // The other batch of its run handed over again in the step.
    let drained = pool.memory.drain(held, first ^ 1);
    let handed = pool.memory.grow(held);
    let horizon = pool.store.epochs().horizon();
    let other_held = pool.memory.held_until(held, first ^ 1, horizon).is_some();
    let first_held = pool.memory.held_until(held, first, horizon).is_some();
    let frame = claim_without_waiting(&pool, large);
    drop(step);
    drop(pool);
    assert!(
        drained && handed,
        "the other batch of the run handed over again"
    );
    assert!(other_held, "the batch handed over in the step is held");
    assert!(!first_held, "the batch handed over before the step is not");
    let frame = frame.unwrap_or_else(|e| panic!("{e}"));
    assert_ne!(
        frame / memory::GRAINS / 2,
        first / 2,
        "the claim drained a run holding a batch handed over in the step"
    );
}

/// A claim that evicts from its own class rather than drain an unheld but costlier batch still
/// waits on the epoch of the held empty batches it passed over: once the step under way at their
/// hand-over ends they can be drained, and that must finish its wait, though its own class's latch
/// stays held (the auditor's mixed-victim case of 2026-11-09, ported; DESIGN.md 9.1.3).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_claim_that_evicts_its_own_still_waits_on_the_held_batches_it_passed_over() {
    use std::sync::mpsc;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/handed-mixed-victims", MIN_BUDGET);
    let (wanted, expensive, empty) = (class(9), class(10), class(1));
    let own = drive(&io, || pool.allocate(wanted)).unwrap();
    let own_frame = own.frame;
    drop(own);
    // A live page used this epoch: with a finite threshold, the owner does not drain it.
    let hot = drive(&io, || pool.allocate(expensive)).unwrap();
    drop(hot);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (end_old_tx, end_old_rx) = mpsc::channel::<()>();
    let old = {
        let pool = pool.clone();
        std::thread::spawn(move || {
            let step = pool.store.epochs().enter();
            entered_tx.send(()).unwrap();
            let _ = end_old_rx.recv();
            drop(step);
        })
    };
    entered_rx.recv().unwrap();
    let mut empty_batches = 0;
    while pool.memory.grow(empty) {
        empty_batches += 1;
    }
    assert_eq!(
        empty_batches, 2,
        "one page of each class, two empty batches"
    );
    let (latched_tx, latched_rx) = mpsc::channel();
    let (let_go_tx, let_go_rx) = mpsc::channel::<()>();
    let holder = {
        let pool = pool.clone();
        std::thread::spawn(move || {
            // Entered after the hand-overs, so its latch outlasts the step they wait on.
            let step = pool.store.epochs().enter();
            let hold = Held::try_take(FrameLatch {
                frames: pool.frames.clone(),
                frame: own_frame,
            })
            .unwrap();
            latched_tx.send(()).unwrap();
            let _ = let_go_rx.recv();
            drop(hold);
            drop(step);
        })
    };
    latched_rx.recv().unwrap();
    THRESHOLD.with(|t| t.set(Some(1.0)));
    let wait = match pool.allocate(wanted).unwrap() {
        IOResult::IO(IOCompletions(wait)) => wait,
        IOResult::Done(_) => panic!("the empty memory was taken while the old step held it"),
    };
    let pending_before = !wait.finished();
    let writes = pool.writes_in_flight.load(Ordering::Acquire);
    end_old_tx.send(()).unwrap();
    old.join().unwrap();
    let woke_on_epoch = wait.finished();
    // The memory is there to take while the own class's latch is still held.
    let retry_succeeded = match pool.allocate(wanted).unwrap() {
        IOResult::Done(page) => {
            drop(page);
            true
        }
        IOResult::IO(_) => false,
    };
    THRESHOLD.with(|t| t.set(None));
    let_go_tx.send(()).unwrap();
    holder.join().unwrap();
    let finished_after_cleanup = wait.finished();
    drop(wait);
    drop(pool);
    assert!(
        pending_before && writes == 0 && retry_succeeded && finished_after_cleanup,
        "the fixture isolates the passed epoch: pending {pending_before}, writes {writes}, \
         retry {retry_succeeded}, finished after {finished_after_cleanup}"
    );
    assert!(
        woke_on_epoch,
        "the claim evicting its own dropped the epoch of the held batches it passed over"
    );
}

/// A claim whose last drain loses its compare-and-swap to a newer request, with no supply event,
/// still waits on the epoch of the held batches it passed over: the chosen batch holds it no more
/// than the drain did, so that epoch is the one producer left (DESIGN.md 9.1.3; the auditor's
/// chosen-drain case of 2026-11-09).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_claim_whose_chosen_drain_is_lost_still_waits_on_the_held_batches_it_passed_over() {
    use crate::race;
    use std::sync::mpsc;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/handed-chosen-lost", MIN_BUDGET);
    let (wanted, other, empty) = (class(9), class(10), class(1));
    let own = drive(&io, || pool.allocate(wanted)).unwrap();
    let own_frame = own.frame;
    drop(own);
    // A frame of another class claimed and not yet a page: its batch is in use to a drain.
    let first = match pool.claim(other, Purpose::New, &[]).unwrap() {
        IOResult::Done(f) => f,
        IOResult::IO(_) => panic!("an empty pool's claim waited"),
    };
    let (entered_tx, entered_rx) = mpsc::channel();
    let (end_old_tx, end_old_rx) = mpsc::channel::<()>();
    let old = {
        let pool = pool.clone();
        std::thread::spawn(move || {
            let step = pool.store.epochs().enter();
            entered_tx.send(()).unwrap();
            let _ = end_old_rx.recv();
            drop(step);
        })
    };
    entered_rx.recv().unwrap();
    let mut empty_batches = 0;
    while pool.memory.grow(empty) {
        empty_batches += 1;
    }
    assert_eq!(
        empty_batches, 2,
        "one page of each class, two empty batches"
    );
    let (latched_tx, latched_rx) = mpsc::channel();
    let (let_go_tx, let_go_rx) = mpsc::channel::<()>();
    let holder = {
        let pool = pool.clone();
        std::thread::spawn(move || {
            // Entered after the hand-overs, so its latch outlasts the step they wait on.
            let step = pool.store.epochs().enter();
            let hold = Held::try_take(FrameLatch {
                frames: pool.frames.clone(),
                frame: own_frame,
            })
            .unwrap();
            latched_tx.send(()).unwrap();
            let _ = let_go_rx.recv();
            drop(hold);
            drop(step);
        })
    };
    latched_rx.recv().unwrap();
    // Before the fresh registration, the other class's frame comes back, so the last round
    // chooses that batch; at its drain, after the word is read, a newer request takes from it.
    let taken = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
    {
        let (pool, taken) = (pool.clone(), taken.clone());
        race::at("buffer.claim_fresh", move || {
            pool.free_frame(other, first, pool.take_free_latch(first));
            let (pool, taken) = (pool.clone(), taken.clone());
            race::at("memory.drain", move || {
                match pool.claim(other, Purpose::New, &[]).unwrap() {
                    IOResult::Done(f) => *taken.lock() = Some(f),
                    IOResult::IO(_) => panic!("the newer request found no frame in the batch"),
                }
            });
        });
    }
    THRESHOLD.with(|t| t.set(Some(1.0)));
    let wait = match pool.claim(wanted, Purpose::New, &[]).unwrap() {
        IOResult::IO(IOCompletions(wait)) => Some(wait),
        IOResult::Done(_) => None,
    };
    THRESHOLD.with(|t| t.set(None));
    // Outcomes are recorded and asserted once the threads, the frames and the pool are let go.
    let reached = !race::pending("buffer.claim_fresh") && !race::pending("memory.drain");
    let again = taken.lock().take();
    let drain_started = !pool.memory.draining().is_empty();
    let pending_before = wait.as_ref().is_some_and(|w| !w.finished());
    end_old_tx.send(()).unwrap();
    old.join().unwrap();
    let woke_on_epoch = wait.as_ref().is_some_and(|w| w.finished());
    if let Some(again) = again {
        pool.free_frame(other, again, pool.take_free_latch(again));
    }
    let_go_tx.send(()).unwrap();
    holder.join().unwrap();
    let waited = wait.is_some();
    drop(wait);
    drop(pool);
    assert!(
        waited,
        "a frame was taken while the old step held the empty memory"
    );
    assert!(
        reached,
        "the claim reached the fresh registration and the chosen drain"
    );
    assert!(
        again.is_some(),
        "the newer request took a frame of the chosen batch"
    );
    assert!(!drain_started, "the chosen drain did not start");
    assert!(
        pending_before,
        "the claim's wait finished before the old step ended"
    );
    assert!(
        woke_on_epoch,
        "the claim whose chosen drain was lost dropped the epoch of the held batches it passed over"
    );
}

/// The pool `pool` with a page of class 0 written once, resident and dirty, and its id.
fn dirty_page(io: &Arc<dyn IO>, pool: &Arc<BufferPool>) -> PageId {
    let mut page = drive(io, || pool.allocate(class(0))).unwrap();
    page.changed();
    page.pid()
}

/// A fix that finds its page latched by another holder waits on that holder's release, a wait of
/// the remote kind: never a yield that sends it back to look while the holder is paused. The
/// release, marked, is the event that finishes it (DESIGN.md 9.1.3, a fix that finds its page
/// latched; the auditor's foreign-latch fixture).
#[test]
fn a_fix_of_a_page_another_holds_waits_remote_on_its_release_and_never_yields() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/latched", MIN_BUDGET);
    let pid = dirty_page(&io, &pool);
    let held = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
    let wait = match pool.fix_exclusive(pid, None).unwrap() {
        IOResult::IO(IOCompletions(wait)) => Some(wait),
        IOResult::Done(_) => None,
    };
    let parked = wait.as_ref().map(|w| (w.finished(), w.is_remote_wait()));
    drop(held);
    let woken = wait.as_ref().is_some_and(Completion::finished);
    let fixed = matches!(pool.fix_exclusive(pid, None).unwrap(), IOResult::Done(_));
    drop(wait);
    drop(pool);
    assert_eq!(
        parked,
        Some((false, true)),
        "the fix of a latched page came back as (finished, remote)"
    );
    assert!(woken, "the holder's release did not finish the wait");
    assert!(fixed, "the page was not fixed once let go");
}

/// A claim whose only producer is the end of another caller's step, with none of the pool's IO in
/// flight, waits on it remote: a pumped backend with nothing of its own in flight is not stepped
/// in a loop for it (DESIGN.md 9.1.3, wait kinds; the auditor's foreign-epoch fixture).
#[test]
fn a_claim_waiting_only_on_another_steps_epoch_waits_remote() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/foreign-epoch", MIN_BUDGET);
    let (held, short) = (class(1), class(0));
    let step = pool.store.epochs().enter();
    while pool.memory.grow(held) {}
    let io_before = pool.io_in_flight();
    let wait = match pool.claim(short, Purpose::New, &[]).unwrap() {
        IOResult::IO(IOCompletions(w)) => Some(w),
        IOResult::Done(_) => None,
    };
    let parked = wait.as_ref().map(|w| (w.finished(), w.is_remote_wait()));
    drop(step);
    let woken = wait.as_ref().is_some_and(Completion::finished);
    drop(wait);
    drop(pool);
    assert!(!io_before, "IO of the pool was in flight");
    assert_eq!(
        parked,
        Some((false, true)),
        "the claim's wait on another step came back as (finished, remote)"
    );
    assert!(woken, "the step's end did not finish the wait");
}

/// A source whose reads the test lands, having reported each request as submitted: what a read
/// of the pool's IO looks like to the pool, its landing at the test's hand.
struct SubmittingSource(PendingReads);
struct SubmittingLocated(PendingReads);

impl source::PageSource for SubmittingSource {
    fn locate(&self, _pid: PageId) -> Result<IOResult<Box<dyn source::Located>>> {
        Ok(IOResult::Done(Box::new(SubmittingLocated(self.0.clone()))))
    }
}

impl source::Located for SubmittingLocated {
    fn read(
        self: Box<Self>,
        into: Arc<crate::io::Buffer>,
        done: source::ReadDone,
        submitting: source::Submitting<'_>,
    ) -> Result<IOResult<Option<Completion>>> {
        submitting.announce();
        self.0.lock().push((into.clone(), done));
        // The request as an inline backend leaves it once its submission returns, owned by none:
        // the frame's record is let go at once, the landing still the test's.
        Ok(IOResult::Done(Some(Completion::new_read(into, |_| None))))
    }
}

/// A fix that finds its page loading before the read is submitted waits on the pool, remote, and
/// the submission's event finishes it; once submitted, a fix waits on the read itself, stepped.
/// Neither is a yield, and the waiter before the submission does not step a backend with nothing
/// of its in flight (DESIGN.md 9.1.3, wait kinds; the auditor's pre-submit load, #3823).
#[test]
fn a_fix_of_a_page_whose_read_is_not_yet_submitted_waits_remote_until_the_submission() {
    use crate::race;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let store = PageStore::open(
        crate::storage::atoll::requests::counted_for_test(io),
        "/db/pre-submit",
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        0,
    );
    let reads: PendingReads = Arc::new(crate::storage::atoll::locks::Mutex::new(Vec::new()));
    let pool = BufferPool::new(
        BufferConfig::with_budget(MIN_BUDGET),
        Arc::new(SubmittingSource(reads.clone())),
        store,
    )
    .unwrap();
    let pid = PageId::new(class(0), 1);
    // What a second fix found with the page loading and its read not yet submitted.
    let before = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
    {
        let (pool, before) = (pool.clone(), before.clone());
        race::at("buffer.read_submit", move || {
            let wait = match pool.fix_optimistic(pid, None).unwrap() {
                IOResult::IO(IOCompletions(wait)) => Some(wait),
                IOResult::Done(_) => None,
            };
            *before.lock() = Some(wait);
        });
    }
    let loading = match pool.fix_optimistic(pid, None).unwrap() {
        IOResult::IO(IOCompletions(wait)) => Some(wait),
        IOResult::Done(_) => None,
    };
    let reached = !race::pending("buffer.read_submit");
    let before = before.lock().take().flatten();
    let seen_before = before.as_ref().map(|w| (w.is_remote_wait(), w.finished()));
    let after = match pool.fix_optimistic(pid, None).unwrap() {
        IOResult::IO(IOCompletions(wait)) => Some(wait),
        IOResult::Done(_) => None,
    };
    let seen_after = after.as_ref().map(|w| (w.is_remote_wait(), w.finished()));
    for (buffer, done) in std::mem::take(&mut *reads.lock()) {
        buffer.as_mut_slice().fill(0);
        done(Ok(()));
    }
    let landed = [&loading, &after]
        .iter()
        .all(|w| w.as_ref().is_some_and(Completion::finished));
    drop((loading, before, after));
    drop(pool);
    assert!(reached, "the load never came to its submission");
    assert_eq!(
        seen_before,
        Some((true, true)),
        "before the submission, the fix's wait came back as (remote, finished by the submission)"
    );
    assert_eq!(
        seen_after,
        Some((false, false)),
        "after the submission, the fix's wait came back as (remote, finished)"
    );
    assert!(landed, "the read landing did not finish the waits on it");
}

/// The audit's same-hold promotions (#3819): a resident page turns outgoing and its write is
/// queued under the same hold, before the fix's frame look, after its release wait registers,
/// or before its held-word mark. The phase recheck or queue notification returns a ready wait,
/// so the next fix can advance the queued write instead of parking on its held latch.
#[test]
fn a_page_turned_outgoing_under_the_same_hold_around_a_fixs_ticket_strands_no_fix() {
    use crate::race;
    for at in [
        "buffer.frame_before_look",
        "buffer.frame_wait_registered",
        "latch.mark_held",
    ] {
        let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
        let pool = open_on(&io, "/db/same-hold", MIN_BUDGET);
        let pid = dirty_page(&io, &pool);
        let frame = pool.table.find(&pool.frames.headers, pid.raw()).unwrap();
        let hold = Arc::new(crate::storage::atoll::locks::Mutex::new(Held::try_take(
            FrameLatch {
                frames: pool.frames.clone(),
                frame,
            },
        )));
        let held = hold.lock().is_some();
        {
            let (pool, hold) = (pool.clone(), hold.clone());
            race::at(at, move || {
                let taken = hold.lock().take();
                if let Some(hold) = taken {
                    let out = pool.start_write_out(hold);
                    pool.write_out(class(0), vec![out]);
                }
            });
        }
        let wait = match pool.fix_exclusive(pid, None).unwrap() {
            IOResult::IO(IOCompletions(wait)) => Some(wait),
            IOResult::Done(_) => None,
        };
        let promoted = !race::pending(at);
        let seen = wait.as_ref().map(Completion::finished);
        drop(wait);
        // Cleanup must not wait for a hook whose reachability the test is checking.
        if race::pending(at) {
            race::point(at);
        }
        let fixed = drive(&io, || pool.fix_exclusive(pid, None)).is_ok();
        drop(hold);
        drop(pool);
        assert!(held, "{at}: the test could not hold the page");
        assert!(promoted, "{at}: the page was never turned outgoing");
        assert_eq!(
            seen,
            Some(true),
            "{at}: the fix parked with the write queued and nothing to finish its wait"
        );
        assert!(fixed, "{at}: the page was not fixed once written out");
    }
}

/// A page of class 0, written once and dirty, latched for its write-out and its write queued, as
/// a drain makes it: the page's id.
fn queued_write_out(io: &Arc<dyn IO>, pool: &Arc<BufferPool>) -> PageId {
    let mut g = drive(io, || pool.allocate(class(0))).unwrap();
    let pid = g.pid();
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    let frame = g.frame;
    drop(g);
    let hold = pool
        .latch_to_evict(class(0), frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .expect("the page is held by no one");
    let out = pool.start_write_out(hold);
    pool.write_out(class(0), vec![out]);
    pid
}

/// A wait that `fix` came back with, as (remote, finished), or `None` if it fixed the page.
fn fix_wait(pool: &Arc<BufferPool>, pid: PageId) -> (Option<(bool, bool)>, Option<Completion>) {
    match pool.fix_optimistic(pid, None).unwrap() {
        IOResult::IO(IOCompletions(w)) => (Some((w.is_remote_wait(), w.finished())), Some(w)),
        IOResult::Done(_) => (None, None),
    }
}

/// Only the pool's requests at its backend make a wait stepped (#3844, #3846): a writeback taken
/// by a thread paused before it submits, and a write that landed and is not yet settled, are CPU
/// work another thread does, whose end is an event; with nothing of the pool's at its backend a
/// waiter is not handed a wait that steps a backend with nothing to land, and the submission's
/// event, nothing else, finishes its wait. While the write is at the backend, a fix waits on it,
/// stepped (DESIGN.md 9.1.3, wait kinds).
#[test]
fn only_requests_at_the_backend_make_a_wait_stepped() {
    use crate::race;
    // Writes stay in flight until the IO steps.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(1));
    let pool = open_on(&io, "/db/kinds", MIN_BUDGET);
    let pid = queued_write_out(&io, &pool);
    type Paused = (usize, usize, bool, Option<(bool, bool)>, Option<Completion>);
    let paused: Arc<Mutex<Option<Paused>>> = Arc::new(Mutex::new(None));
    {
        // The writeback taken by this thread's step, its swap slot in hand, about to submit.
        let (pool, paused) = (pool.clone(), paused.clone());
        race::at("buffer.write_submit", move || {
            let (queued, requests) = (
                pool.queued.load(Ordering::Acquire),
                pool.requests.load(Ordering::Acquire),
            );
            let kind_remote = pool.supply_wait(pool.ticket()).is_remote_wait();
            let (fix, wait) = fix_wait(&pool, pid);
            *paused.lock() = Some((queued, requests, kind_remote, fix, wait));
        });
    }
    let mut steps = 0;
    while race::pending("buffer.write_submit") && steps < 64 {
        if let Some(c) = pool.drive_writebacks().unwrap() {
            io.wait_for_completion(c).unwrap();
        }
        steps += 1;
    }
    let paused = paused.lock().take();
    // Read before the write lands: its landing is an event too, and would finish it.
    let paused_woken = paused
        .as_ref()
        .and_then(|p| p.4.as_ref())
        .is_some_and(Completion::finished);
    let at_backend = (
        pool.requests.load(Ordering::Acquire),
        pool.supply_wait(pool.ticket()).is_remote_wait(),
    );
    let (fix_at_backend, write) = fix_wait(&pool, pid);
    let mut landing = 0;
    while pool.requests.load(Ordering::Acquire) > 0 && landing < 64 {
        io.step().unwrap();
        landing += 1;
    }
    let landed = (
        pool.writes_in_flight.load(Ordering::Acquire),
        pool.requests.load(Ordering::Acquire),
        pool.supply_wait(pool.ticket()).is_remote_wait(),
    );
    let write_finished = write.as_ref().is_some_and(Completion::finished);
    let settled = drive(&io, || pool.settle_writes()).is_ok();
    drop(write);
    let (queued, requests, kind_remote, fix, wait) = paused.expect("the writeback was stepped");
    drop(wait);
    drop(pool);
    assert_eq!(
        (queued, requests),
        (1, 0),
        "taken before submitting: queued, at the backend"
    );
    assert!(
        kind_remote,
        "a writeback held before its submission made the wait stepped"
    );
    assert_eq!(
        fix,
        Some((true, false)),
        "the fix's wait before the submission"
    );
    assert!(paused_woken, "the submission did not finish the fix's wait");
    assert_eq!(
        at_backend,
        (1, false),
        "submitted: at the backend, and the wait stepped"
    );
    assert_eq!(
        fix_at_backend,
        Some((false, false)),
        "the fix's wait on the write at the backend"
    );
    assert_eq!(
        landed,
        (1, 0, true),
        "landed, not settled: a write not at the backend"
    );
    assert!(
        write_finished,
        "the write landing did not finish the fix's wait on it"
    );
    assert!(settled);
}

/// A read whose landing has begun is not waited on by stepping (#3846): the landing marks it off
/// the backend inside its event before anything else, so a fix that looks while the landing is
/// paused before the page is published waits on the pool, and the landing's event finishes it.
#[test]
fn a_fix_while_a_reads_landing_is_paused_waits_on_the_pool_not_the_read() {
    use crate::race;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let store = PageStore::open(
        crate::storage::atoll::requests::counted_for_test(io),
        "/db/landing",
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        0,
    );
    let reads: PendingReads = Arc::new(Mutex::new(Vec::new()));
    let pool = BufferPool::new(
        BufferConfig::with_budget(MIN_BUDGET),
        Arc::new(SubmittingSource(reads.clone())),
        store,
    )
    .unwrap();
    let pid = PageId::new(class(0), 1);
    let (_, loading) = fix_wait(&pool, pid);
    let at_backend = pool.requests.load(Ordering::Acquire);
    let (paused_tx, paused_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let landing = {
        let read = reads.lock().pop();
        std::thread::spawn(move || {
            race::at("buffer.read_landed", move || {
                paused_tx.send(()).unwrap();
                go_rx.recv().unwrap();
            });
            if let Some((buffer, done)) = read {
                buffer.as_mut_slice().fill(0);
                done(Ok(()));
            }
            !race::pending("buffer.read_landed")
        })
    };
    let paused = paused_rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .is_ok();
    let (during, wait) = if paused {
        fix_wait(&pool, pid)
    } else {
        (None, None)
    };
    go_tx.send(()).ok();
    let reached = landing.join().unwrap();
    let woken = wait.as_ref().is_some_and(Completion::finished);
    let landed = loading.as_ref().is_some_and(Completion::finished);
    drop((wait, loading));
    drop(pool);
    assert_eq!(at_backend, 1, "the read was not counted at the backend");
    assert!(paused && reached, "the landing never paused");
    assert_eq!(
        during,
        Some((true, false)),
        "a fix during a paused landing came back as (remote, finished)"
    );
    assert!(woken, "the landing's event did not finish the fix's wait");
    assert!(landed, "the read's own wait was not finished");
}

/// How a fix came back: fixed, a wait as (remote, finished), or an error.
#[derive(Debug, PartialEq)]
enum FixOutcome {
    Fixed,
    Wait(bool, bool),
    Failed(CompletionError),
}

/// What [`initiating_fix_during_a_landing`] saw.
struct Landing {
    /// The fix that started the read, and a second fix while the landing stayed paused.
    starting: FixOutcome,
    during: Option<FixOutcome>,
    /// The landing paused and went on.
    paused: bool,
    /// The starting fix's wait and the second fix's, finished once the landing went on.
    starting_woken: bool,
    woken: bool,
    /// A fix after the landing of a read that succeeded, which left the page resident.
    after: Option<FixOutcome>,
}

/// The fix that starts a read whose landing it sees begun, the read failing or not, paused at
/// `pause` on the landing's thread, and a second fix while it stays paused.
fn initiating_fix_during_a_landing(pause: &'static str, fails: bool) -> Landing {
    use crate::race;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let store = PageStore::open(
        crate::storage::atoll::requests::counted_for_test(io),
        "/db/initiating-error",
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        0,
    );
    let reads: PendingReads = Arc::new(Mutex::new(Vec::new()));
    let pool = BufferPool::new(
        BufferConfig::with_budget(MIN_BUDGET),
        Arc::new(SubmittingSource(reads.clone())),
        store,
    )
    .unwrap();
    let pid = PageId::new(class(0), 1);
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let landing: Arc<Mutex<Option<std::thread::JoinHandle<bool>>>> = Arc::new(Mutex::new(None));
    let paused = Arc::new(crate::sync::atomic::AtomicBool::new(false));
    {
        // The read submitted and the load returned: its landing starts on another thread, which
        // fails it and pauses at `pause`, before the starting fix looks again.
        let (landing, paused) = (landing.clone(), paused.clone());
        race::at("buffer.loader_follows", move || {
            let read = reads.lock().pop();
            let (paused_tx, paused_rx) = std::sync::mpsc::channel();
            *landing.lock() = Some(std::thread::spawn(move || {
                race::at(pause, move || {
                    paused_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                });
                if let Some((buffer, done)) = read {
                    if fails {
                        done(Err(CompletionError::IOError(
                            std::io::ErrorKind::Other,
                            "the read failed",
                        )));
                    } else {
                        buffer.as_mut_slice().fill(0);
                        done(Ok(()));
                    }
                }
                !race::pending(pause)
            }));
            paused.store(
                paused_rx
                    .recv_timeout(std::time::Duration::from_secs(60))
                    .is_ok(),
                Ordering::Release,
            );
        });
    }
    let outcome = |pool: &Arc<BufferPool>| match pool.fix_optimistic(pid, None) {
        Ok(IOResult::Done(_)) => (FixOutcome::Fixed, None),
        Ok(IOResult::IO(IOCompletions(w))) => {
            (FixOutcome::Wait(w.is_remote_wait(), w.finished()), Some(w))
        }
        Err(LimboError::CompletionError(e)) => (FixOutcome::Failed(e), None),
        Err(e) => panic!("{e}"),
    };
    let (starting, starting_wait) = outcome(&pool);
    let paused = paused.load(Ordering::Acquire);
    let (during, wait) = if paused {
        let (o, w) = outcome(&pool);
        (Some(o), w)
    } else {
        (None, None)
    };
    go_tx.send(()).ok();
    let reached = landing.lock().take().is_some_and(|l| l.join().unwrap());
    let woken = wait.as_ref().is_some_and(Completion::finished);
    let starting_woken = starting_wait.as_ref().is_some_and(Completion::finished);
    drop((wait, starting_wait));
    let after = (!fails).then(|| outcome(&pool).0);
    drop(pool);
    Landing {
        starting,
        during,
        paused: paused && reached,
        starting_woken,
        woken,
        after,
    }
}

fn the_read_failed() -> FixOutcome {
    FixOutcome::Failed(CompletionError::IOError(
        std::io::ErrorKind::Other,
        "the read failed",
    ))
}

/// The fix that starts a read keeps its error whenever it sees the landing begun (the auditor's
/// #3858, P1-1 on v26): the landing publishes the read's result on its own wait before it marks
/// the read off the backend, so a starting fix that sees it landed, and takes the pool's wait
/// instead of the read's, reads that error and returns it; a second fix waits on the pool, remote,
/// which the landing's event finishes (DESIGN.md 9.1.3, wait kinds).
#[test]
fn the_fix_that_starts_a_read_keeps_its_error_once_the_landing_marked_it_landed() {
    let seen = initiating_fix_during_a_landing("buffer.read_landed", true);
    assert!(seen.paused, "the landing never paused");
    assert_eq!(
        seen.starting,
        the_read_failed(),
        "the starting fix lost its read's error"
    );
    assert_eq!(
        seen.during,
        Some(FixOutcome::Wait(true, false)),
        "a fix during the paused landing"
    );
    assert!(
        seen.woken,
        "the landing's event did not finish the second fix's wait"
    );
}

/// Between the read's result and its landing mark, a read still counted at the backend that holds
/// its result is not waited on: its own wait is finished, so a fix that took it would look again
/// at once, and on until the landing went on. The fix that started it has its error; a second fix
/// waits on the pool, remote, which the landing's event finishes (DESIGN.md 9.1.3, wait kinds).
#[test]
fn a_read_holding_its_result_before_its_landing_mark_is_waited_on_remote() {
    let seen = initiating_fix_during_a_landing("buffer.read_result", true);
    assert!(seen.paused, "the landing never paused");
    assert_eq!(
        seen.starting,
        the_read_failed(),
        "the starting fix lost its read's error"
    );
    assert_eq!(
        seen.during,
        Some(FixOutcome::Wait(true, false)),
        "a fix while the read holds its result before its landing mark"
    );
    assert!(
        seen.woken,
        "the landing's event did not finish the second fix's wait"
    );
}

/// A read that succeeded and holds its result, its page not yet published, is not handed to any
/// fix, the one that started it included: each waits on the pool, remote, which the landing's
/// event finishes, and the page is fixed after it, whether the landing paused before its mark or
/// after it (the auditor's v26 report: success never exposes bytes while the frame is loading).
#[test]
fn a_read_that_succeeded_is_not_handed_out_before_its_page_is_published() {
    for pause in ["buffer.read_result", "buffer.read_landed"] {
        let seen = initiating_fix_during_a_landing(pause, false);
        assert!(seen.paused, "{pause}: the landing never paused");
        assert_eq!(
            (seen.starting, seen.during),
            (
                FixOutcome::Wait(true, false),
                Some(FixOutcome::Wait(true, false))
            ),
            "{pause}: the starting fix and a second one before the page is published"
        );
        assert!(
            seen.starting_woken && seen.woken,
            "{pause}: the landing's event did not finish both waits"
        );
        assert_eq!(
            seen.after,
            Some(FixOutcome::Fixed),
            "{pause}: the page after the landing"
        );
    }
}

/// A write-out whose write landing has begun is not waited on by stepping (#3846): the landing
/// marks it off the backend inside its event first, so a fix that looks while the landing is
/// paused before it finishes the write-out's wait steps the writeback that settles it, and waits
/// on the pool, never on a write no request is left of.
#[test]
fn a_fix_while_a_write_outs_landing_is_paused_does_not_wait_on_the_write() {
    use crate::race;
    let crash = Arc::new(CrashIO::new(1));
    let io: Arc<dyn IO> = crash.clone();
    let pool = open_on(&io, "/db/write-landing", MIN_BUDGET);
    let pid = queued_write_out(&io, &pool);
    let mut steps = 0;
    while pool.requests.load(Ordering::Acquire) == 0 && steps < 64 {
        if let Some(c) = pool.drive_writebacks().unwrap() {
            io.wait_for_completion(c).unwrap();
        }
        steps += 1;
    }
    let at_backend = pool.requests.load(Ordering::Acquire);
    let (paused_tx, paused_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let landing = {
        let (io, pool) = (io.clone(), pool.clone());
        std::thread::spawn(move || {
            race::at("buffer.write_landed", move || {
                paused_tx.send(()).unwrap();
                go_rx.recv().unwrap();
            });
            let mut n = 0;
            while race::pending("buffer.write_landed") && n < 64 {
                io.step().unwrap();
                n += 1;
            }
            drop(pool);
            !race::pending("buffer.write_landed")
        })
    };
    let paused = paused_rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .is_ok();
    let (during, wait) = if paused {
        fix_wait(&pool, pid)
    } else {
        (None, None)
    };
    go_tx.send(()).ok();
    let reached = landing.join().unwrap();
    let settled = drive(&io, || pool.settle_writes()).is_ok();
    drop(wait);
    drop(pool);
    assert_eq!(at_backend, 1, "the write was not counted at the backend");
    assert!(paused && reached, "the landing never paused");
    assert!(
        during.is_some_and(|(remote, _)| remote),
        "a fix during a paused write landing came back as {during:?}, not the pool's wait"
    );
    assert!(settled);
}

/// The auditor's root-growth pressure (#3850), on a real tree: a root of four values needs a new
/// page for a fifth, and every other frame is a dirty page written out, its write at the backend.
/// The put's claim waits on those writes, stepped; the put lets its root go on the way out, and
/// that release, a change but no landing, must not finish the wait, or the caller looks again
/// with nothing stepped and the writes never land. Each attempt comes back with a wait its steps
/// finish; stepping it lands writes, and the put completes with every value read back.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_root_split_with_every_other_frame_written_out_waits_on_a_landing_not_its_own_release() {
    use crate::storage::atoll::btree::{Lookup, Mode as PutMode, Put, Tree};
    let crash = Arc::new(CrashIO::new(3));
    let io: Arc<dyn IO> = crash.clone();
    let pool = open_on(&io, "/db/root-pressure", MIN_BUDGET);
    let c = class(0);
    let root = drive(&io, || Tree::create(&pool, c)).unwrap();
    let tree = Tree::open(io.clone(), pool.clone(), pool.store.clone(), root);
    let value = vec![37u8; 950];
    for i in 0u64..4 {
        let mut op = Put::new(&i.to_be_bytes(), value.clone(), PutMode::Upsert);
        drive(&io, || tree.put(&mut op)).unwrap();
    }
    // Every other frame a dirty page held for its write-out.
    let count = MIN_BUDGET / c.size() - 1;
    let mut outgoing = Vec::with_capacity(count);
    for i in 0..count {
        let mut page = drive(&io, || pool.allocate(c)).unwrap();
        let pid = page.pid();
        stamp(&mut page.page_mut(), pid, i as u64);
        page.changed();
        let frame = page.frame;
        drop(page);
        let hold = pool
            .latch_to_evict(c, frame, || FrameLatch {
                frames: pool.frames.clone(),
                frame,
            })
            .unwrap();
        outgoing.push(pool.start_write_out(hold));
    }
    pool.write_out(c, outgoing);
    // The writes' preparation finished, stopped as soon as they are at the backend.
    let mut prepared = false;
    for _ in 0..10_000 {
        let wait = pool.drive_writebacks().unwrap();
        if pool.requests.load(Ordering::Acquire) > 0 && crash.in_flight() > 0 {
            prepared = true;
            break;
        }
        match wait {
            Some(c) => io.wait_for_completion(c).unwrap(),
            None if crash.in_flight() > 0 => io.step().unwrap(),
            None => {}
        }
    }
    let at_backend = crash.in_flight();
    let mut op = Put::new(&4u64.to_be_bytes(), value.clone(), PutMode::Upsert);
    // Each attempt: (finished when handed back, stepped kind, landed by stepping it).
    let mut attempts = Vec::new();
    let mut done = false;
    let mut error = None;
    if prepared {
        for _ in 0..32 {
            match tree.put(&mut op) {
                Ok(IOResult::Done(_)) => {
                    done = true;
                    break;
                }
                Ok(IOResult::IO(IOCompletions(w))) => {
                    let handed = (w.finished(), !w.is_remote_wait());
                    let before = crash.in_flight();
                    let mut steps = 0;
                    while !w.finished() && steps < 100_000 {
                        io.step().unwrap();
                        steps += 1;
                    }
                    attempts.push((handed, w.finished() && crash.in_flight() < before));
                }
                Err(e) => {
                    error = Some(e.to_string());
                    break;
                }
            }
        }
    }
    for _ in 0..100_000 {
        if crash.in_flight() == 0 {
            break;
        }
        io.step().unwrap();
    }
    let drained = crash.in_flight() == 0;
    let settled = drive(&io, || pool.settle_writes()).is_ok();
    let finished = done || drive(&io, || tree.put(&mut op)).is_ok();
    let mut values = true;
    for i in 0u64..5 {
        let mut lookup = Lookup::new(&i.to_be_bytes());
        values &= drive(&io, || tree.lookup(&mut lookup)).is_ok_and(|v| v == Some(value.clone()));
    }
    drop(op);
    drop(tree);
    drop(pool);
    assert!(
        prepared && at_backend > 0,
        "the writes never reached the backend"
    );
    assert!(error.is_none(), "{error:?}");
    let self_finished = attempts
        .iter()
        .filter(|((finished, _), _)| *finished)
        .count();
    assert_eq!(
        self_finished, 0,
        "attempts handed back a wait finished already, its own release finishing it: {attempts:?}"
    );
    assert!(
        attempts
            .iter()
            .all(|((_, stepped), landed)| *stepped && *landed),
        "an attempt's wait was not stepped, or stepping it landed nothing: {attempts:?}"
    );
    assert!(
        done || !attempts.is_empty(),
        "the put neither waited nor completed"
    );
    assert!(drained && settled && finished && values);
}

/// The fix that starts a read waits as any fix of the page does (the auditor's P2-1 on v24): on
/// the read itself, stepped, while its request is at the backend, as a follower does; both are
/// finished by the landing, and the page is fixed after it. A read whose source handed nothing over
/// is refused (`a_read_returned_neither_handed_over_nor_done_ends_the_process`), so the starting
/// fix never meets one; its error, had the read failed, is its own
/// (`the_fix_that_starts_a_read_keeps_its_error_once_the_landing_marked_it_landed`).
#[test]
fn the_fix_that_starts_a_read_waits_on_it_only_while_its_request_is_at_the_backend() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let store = PageStore::open(
        crate::storage::atoll::requests::counted_for_test(io),
        "/db/initial-read",
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        0,
    );
    let reads: PendingReads = Arc::new(Mutex::new(Vec::new()));
    let pool = BufferPool::new(
        BufferConfig::with_budget(MIN_BUDGET),
        Arc::new(SubmittingSource(reads.clone())),
        store,
    )
    .unwrap();
    let pid = PageId::new(class(0), 1);
    let (initial, first) = fix_wait(&pool, pid);
    let (follower, second) = fix_wait(&pool, pid);
    for (buffer, done) in std::mem::take(&mut *reads.lock()) {
        buffer.as_mut_slice().fill(0);
        done(Ok(()));
    }
    let landed = [&first, &second]
        .iter()
        .all(|w| w.as_ref().is_some_and(Completion::finished));
    let fixed = matches!(pool.fix_optimistic(pid, None).unwrap(), IOResult::Done(_));
    drop((first, second));
    drop(pool);
    assert_eq!(
        (initial, follower, landed, fixed),
        (Some((false, false)), Some((false, false)), true, true),
        "(the starting fix's wait, a follower's, landed, fixed after) as (remote, finished)"
    );
}

/// A page source whose read returns done having neither handed its request to the pool's IO nor
/// run `done` breaks the read contract (source.rs, `Located::read`): the starting fix could take
/// the pool's wait and lose the read's result. The pool ends the process there rather than run on.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_read_returned_neither_handed_over_nor_done_ends_the_process() {
    use crate::storage::atoll::testing::in_child;
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_read_returned_neither_handed_over_nor_done_ends_the_process"
        ),
        || {
            let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
            let store = PageStore::open(
                crate::storage::atoll::requests::counted_for_test(io),
                "/db/breach",
                StoreConfig {
                    file_bytes: 1 << 30,
                    meta_pages: 1 << 10,
                },
                [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
                0,
            );
            let reads: PendingReads = Arc::new(Mutex::new(Vec::new()));
            let pool = BufferPool::new(
                BufferConfig::with_budget(MIN_BUDGET),
                Arc::new(DeferredSource(reads)),
                store,
            )
            .unwrap();
            let _ = pool.fix_optimistic(PageId::new(class(0), 1), None);
        },
    );
    let Some(ended) = ended else { return };
    assert!(
        ended.aborted,
        "the child did not abort: {:?}\n{}",
        ended.code, ended.stderr
    );
    assert!(
        ended
            .stderr
            .contains("a page source's read returned neither handing its request over nor done"),
        "{}",
        ended.stderr
    );
}

/// The auditor's FOREIGN_ROOT probe (impl-913-v27 review), run under the operation's owner: a
/// root whose only free frames another caller holds parks on one pending wait rather than waking
/// itself on its own latches' release (DESIGN.md 9.1.3, *A shortage never waits under a hold*).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_root_waiting_for_foreign_holders_does_not_wake_itself_and_retry() {
    use crate::storage::atoll::btree::{Lookup, Mode as PutMode, Put, Tree};
    use std::sync::{
        atomic::{AtomicUsize as Counter, Ordering as NativeOrder},
        mpsc,
    };
    use std::task::{Wake, Waker};
    struct Wakes(Counter);
    impl Wake for Wakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, NativeOrder::Relaxed);
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, NativeOrder::Relaxed);
        }
    }
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/audit/foreign-root-pressure", MIN_BUDGET);
    let c = class(4);
    let root = drive(&io, || Tree::create(&pool, c)).unwrap();
    let tree = Tree::open(io.clone(), pool.clone(), pool.store.clone(), root);
    let len = tree.limits().max_entry - 18;
    let value = vec![73; len];
    for i in 0u64..4 {
        let mut op = Put::new(&i.to_be_bytes(), value.clone(), PutMode::Upsert);
        drive(&io, || tree.put(&mut op)).unwrap();
    }
    let calls = std::sync::Arc::new(Wakes(Counter::new(0)));
    let waker = Waker::from(calls.clone());
    let count = MIN_BUDGET / c.size() - 1;
    let (
        queued,
        parked,
        holders,
        ready,
        pending,
        before_wakes,
        after_wakes,
        no_io,
        done_while_held,
        error,
        finished_after_release,
    ) = std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel();
        let pool_ref = &pool;
        let io_ref = &io;
        let holder = scope.spawn(move || {
            let held: Vec<_> = (0..count)
                .map(|_| drive(io_ref, || pool_ref.allocate(c)).unwrap())
                .collect();
            ready_tx.send(held.len()).unwrap();
            let _ = release_rx.recv();
            drop(held);
        });
        let holders = ready_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .ok();
        let no_io = pool.requests.load(Ordering::Acquire) == 0;
        let mut op = Put::new(&4u64.to_be_bytes(), value.clone(), PutMode::Upsert);
        let mut ready = 0;
        let mut pending = 0;
        let mut done_while_held = false;
        let mut error = None;
        let mut last = None;
        // The operation's top, as `drive` is: a shortage is reserved there, holding nothing.
        let mut owner = super::Owner::new();
        if holders == Some(count) {
            for _ in 0..32 {
                match owner.call(|| tree.put(&mut op)) {
                    Ok(IOResult::Done(_)) => {
                        done_while_held = true;
                        break;
                    }
                    Ok(IOResult::IO(IOCompletions(wait))) => {
                        wait.set_waker(&waker);
                        let finished = wait.finished();
                        last = Some(wait);
                        if finished {
                            ready += 1;
                        } else {
                            pending += 1;
                            break;
                        }
                    }
                    Err(e) => {
                        error = Some(e.to_string());
                        break;
                    }
                }
            }
        }
        let queued = owner.queued();
        let parked = last.as_ref().is_some_and(|w| !w.finished());
        let before = calls.0.load(NativeOrder::Relaxed);
        let _ = release_tx.send(());
        let _ = holder.join();
        let after = calls.0.load(NativeOrder::Relaxed);
        let finished = last.as_ref().is_some_and(Completion::finished);
        drop(last);
        drop(op);
        drop(owner);
        (
            queued,
            parked,
            holders,
            ready,
            pending,
            before,
            after,
            no_io,
            done_while_held,
            error,
            finished,
        )
    });
    let mut op = Put::new(&4u64.to_be_bytes(), value.clone(), PutMode::Upsert);
    let completed_after_release = drive(&io, || tree.put(&mut op)).is_ok();
    let mut values_ok = true;
    for i in 0u64..5 {
        let mut lookup = Lookup::new(&i.to_be_bytes());
        values_ok &=
            drive(&io, || tree.lookup(&mut lookup)).is_ok_and(|v| v == Some(value.clone()));
    }
    drop(op);
    drop(waker);
    drop(calls);
    drop(tree);
    drop(pool);
    drop(io);
    println!("FOREIGN_ROOT holders={holders:?} expected={count} ready={ready} pending={pending} before_wakes={before_wakes} after_wakes={after_wakes} no_io={no_io} done_while_held={done_while_held} error={error:?} finished_after_release={finished_after_release} completed_after_release={completed_after_release} values_ok={values_ok}");
    assert!(
        holders == Some(count)
            && no_io
            && !done_while_held
            && error.is_none()
            && finished_after_release
            && completed_after_release
            && values_ok,
        "the foreign-holder setup must prevent growth and recover cleanly once those holders leave"
    );
    assert!(
        queued,
        "the root came short and took a place in the shortage queue"
    );
    // Phase A protects nothing (DESIGN.md 9.1.3): its first look makes room among the tree's own
    // pages and looks once more, a look following something new; the next finds nothing and the
    // root parks until the holders let go. A yield's waker runs at once, so wakes before the
    // release are the yields' alone.
    assert!(
        ready <= 1 && pending == 1 && parked && before_wakes == ready,
        "a real root needing a page held by OTHER callers must park rather than wake itself and \
         retry: ready={ready} pending={pending} parked={parked} before_wakes={before_wakes}"
    );
    assert!(
        after_wakes > before_wakes,
        "the foreign holders must wake the pending root"
    );
}

/// A frame a shortage reserved goes to the claim of exactly its demand and to no other, and is
/// given back when its operation ends without taking it (DESIGN.md 9.1.3).
#[test]
fn a_frame_reserved_for_a_shortage_goes_to_its_own_claim_alone_and_back_when_unused() {
    // Checked with every owner let go: a failed check unwinding through one ends the process
    // rather than failing the test.
    if let Err(e) = reserved_frame_goes_to_its_demand() {
        panic!("{e}");
    }
}

/// Fails a check with its message, as `assert!` would, returning instead of panicking.
macro_rules! check {
    ($cond:expr, $($msg:tt)+) => {
        if !$cond {
            return Err(format!($($msg)+));
        }
    };
}

fn reserved_frame_goes_to_its_demand() -> std::result::Result<(), String> {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/reserved", MIN_BUDGET);
    let c = class(4);
    let free = |p: &Arc<BufferPool>| p.memory.free_frames(c).len();
    // A page of the class, let go, so the class holds a batch whose free frames the test counts;
    // the test holds nothing of the pool at the top, as an owner's caller must not (W1).
    drop(drive(&io, || pool.allocate(c)).unwrap());
    let before = free(&pool);
    check!(before > 1, "the class's batch has free frames");
    let came_short =
        || Ok::<_, crate::LimboError>(shortage::short::<()>(&pool, c, Purpose::New, &[]));

    // A call that came short: its top reserves the frame, holding nothing, and yields.
    let mut owner = Owner::new();
    let reserved = owner.call(came_short).unwrap();
    check!(
        matches!(&reserved, IOResult::IO(w) if w.0.finished()),
        "a reservation yields"
    );
    check!(owner.queued(), "the owner holds a tenure");
    check!(free(&pool) == before - 1, "one frame reserved");
    let census = pool.taken_frames_census();
    check!(
        census.is_empty(),
        "the census with a frame reserved for the shortage: {census:#?}"
    );
    // Inside the operation: a claim of another purpose does not take it; the claim of its demand
    // does, and nothing more leaves the free frames for it.
    let (other, own, free_then, free_after) = match owner
        .call(|| {
            let other = match pool.claim(c, Purpose::Read, &[])? {
                IOResult::Done(f) => f,
                IOResult::IO(_) => panic!("a free frame was there"),
            };
            let free_then = free(&pool);
            let own = match pool.claim(c, Purpose::New, &[])? {
                IOResult::Done(f) => f,
                IOResult::IO(_) => panic!("the reserved frame was there"),
            };
            Ok(IOResult::Done((other, own, free_then, free(&pool))))
        })
        .unwrap()
    {
        IOResult::Done(frames) => frames,
        IOResult::IO(_) => return Err("the operation was not done".into()),
    };
    pool.give_back_reserved(c, other);
    pool.give_back_reserved(c, own);
    check!(
        free_after == free_then,
        "the demand's claim took a free frame, not the reserved one"
    );
    check!(other != own, "two claims took one frame");
    check!(!owner.queued(), "done: the tenure ended");
    check!(free(&pool) == before, "every frame back");

    // Reserved and never taken: given back when the operation ends.
    let mut owner = Owner::new();
    check!(
        matches!(owner.call(came_short).unwrap(), IOResult::IO(_)),
        "a reservation yields"
    );
    check!(free(&pool) == before - 1, "one frame reserved");
    check!(
        matches!(
            owner.call(|| Ok::<_, crate::LimboError>(IOResult::Done(()))),
            Ok(IOResult::Done(()))
        ),
        "the operation done"
    );
    check!(free(&pool) == before, "the unused frame given back");
    check!(pool.shortage.len() == 0, "the queue empty");
    Ok(())
}

/// The pages the gate's holder keeps resident are not evicted while it holds the gate, and are
/// again once its tenure ends (DESIGN.md 9.1.3, phase B).
#[test]
fn the_shortage_holders_pages_stay_resident_until_its_tenure_ends() {
    // Checked with the pool let go: a failed check unwinding through it ends the process rather
    // than failing the test.
    if let Err(e) = protected_pages_stay() {
        panic!("{e}");
    }
}

fn protected_pages_stay() -> std::result::Result<(), String> {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/protected", MIN_BUDGET);
    let c = class(4);
    let page = drive(&io, || pool.allocate(c)).unwrap();
    let (pid, frame) = (page.pid(), page.hint() - 1);
    drop(page);
    pool.protect(&[pid.raw()]);
    check!(pool.protected(frame), "the published page reads protected");
    let mut outgoing = Vec::new();
    let mut room = Room::default();
    check!(
        !pool.evict_or_write(c, frame, &mut outgoing, &mut room),
        "a protected page was evicted"
    );
    pool.protect(&[]);
    check!(!pool.protected(frame), "the retired set protects nothing");
    check!(
        pool.evict_or_write(c, frame, &mut outgoing, &mut room) || !outgoing.is_empty(),
        "the page, no longer protected, was neither evicted nor written"
    );
    Ok(())
}

/// One holder at a time: a second operation that comes short waits for its turn, reserving
/// nothing, until the first operation ends; then its turn is finished and it reserves its frame
/// (DESIGN.md 9.1.3, *Tenure*).
#[test]
fn a_second_shortage_waits_its_turn_behind_the_gates_holder() {
    // Checked with every owner let go: a failed check unwinding through one ends the process
    // rather than failing the test.
    if let Err(e) = second_shortage_waits() {
        panic!("{e}");
    }
}

fn second_shortage_waits() -> std::result::Result<(), String> {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/turns", MIN_BUDGET);
    let c = class(4);
    drop(drive(&io, || pool.allocate(c)).unwrap());
    let free = |p: &Arc<BufferPool>| p.memory.free_frames(c).len();
    let before = free(&pool);
    let came_short =
        || Ok::<_, crate::LimboError>(shortage::short::<()>(&pool, c, Purpose::New, &[]));
    let (mut first, mut second) = (Owner::new(), Owner::new());
    check!(
        matches!(first.call(came_short).unwrap(), IOResult::IO(w) if w.0.finished()),
        "the first reserves at once"
    );
    check!(free(&pool) == before - 1, "one frame reserved");
    let turn = match second.call(came_short).unwrap() {
        IOResult::IO(IOCompletions(w)) => w,
        IOResult::Done(()) => return Err("came short".into()),
    };
    check!(!turn.finished(), "the second waits for its turn");
    check!(free(&pool) == before - 1, "and reserves nothing meanwhile");
    check!(pool.shortage.len() == 2, "both queued");
    check!(
        matches!(
            first.call(|| Ok::<_, crate::LimboError>(IOResult::Done(()))),
            Ok(IOResult::Done(()))
        ),
        "the first done"
    );
    check!(turn.finished(), "the first's end hands the gate on");
    check!(free(&pool) == before, "and gives its unused frame back");
    // Called at its turn, the second reserves its demand's frame before its operation runs
    // again.
    let mut seen = None;
    check!(
        matches!(
            second.call(|| {
                seen = Some(free(&pool));
                Ok::<_, crate::LimboError>(IOResult::<()>::IO(IOCompletions(
                    Completion::new_yield(),
                )))
            }),
            Ok(IOResult::IO(_))
        ),
        "the second's operation ran on"
    );
    check!(
        seen == Some(before - 1),
        "the second reserves once it holds the gate: {seen:?} free"
    );
    drop(second);
    check!(
        free(&pool) == before,
        "the second's end gives its frame back"
    );
    check!(pool.shortage.len() == 0, "the queue empty");
    Ok(())
}

/// The auditor's two probes on impl-s1s2-v1 (bus #3994), kept as tests: the demand's run is
/// reserved before the step's pages are protected, so a run needing the frame of the step's own
/// page is obtained and the page read in again (P1); and a publication barrier still pending
/// holds the operation back however often it is called (P2).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn audit_a_small_core_page_does_not_prevent_reserving_the_large_run() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/audit/s1/large-run", MIN_BUDGET);
    let small = class(0);
    let big = class(CLASS_COUNT - 1);
    let core_grains = big.size() / small.size();
    let mut allocated = Vec::new();
    let mut chosen = None;
    // Ordinary allocations fill the small tail before the only large aligned run.
    for _ in 0..=memory::GRAINS {
        let mut page = drive(&io, || pool.allocate(small)).unwrap();
        let pid = page.pid();
        stamp(&mut page.page_mut(), pid, 1);
        page.changed();
        let frame = page.hint() - 1;
        allocated.push(pid);
        drop(page);
        if (frame as usize) < core_grains {
            chosen = Some((pid, frame));
            break;
        }
    }
    let (root, frame) = chosen.expect("ordinary allocation reached the core");
    for pid in allocated {
        if pid != root {
            drive(&io, || pool.free_unreferenced(pid)).unwrap();
        }
    }
    let mut owner = Owner::new();
    let mut ready = 0;
    let mut pending = 0;
    let mut done = false;
    let mut error = None;
    let mut last = None;
    let mut original_frame = None;
    for _ in 0..128 {
        let out = owner.call(|| {
            let page = match pool.fix_exclusive(root, None)? {
                IOResult::Done(p) => p,
                IOResult::IO(w) => return Ok(IOResult::IO(w)),
            };
            let answer = pool.claim(big, Purpose::New, &[root]);
            drop(page);
            answer
        });
        match out {
            Ok(IOResult::Done(f)) => {
                original_frame = Some(f);
                done = true;
                break;
            }
            Ok(IOResult::IO(IOCompletions(wait))) => {
                if wait.finished() {
                    ready += 1;
                } else {
                    pending += 1;
                    last = Some(wait);
                    break;
                }
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    let protected = pool.protected(frame);
    let no_io = pool.requests.load(Ordering::Acquire) == 0;
    let queued = pool.shortage.len();
    let stalled_wait = last.as_ref().is_some_and(|c| !c.finished());
    drop(owner);
    if let Some(f) = original_frame {
        pool.give_back_reserved(big, f);
    }
    drop(last);
    // Control: the same pool can reserve the large frame with no protection, then refetch the
    // exact working page into the tail while that raw frame is still reserved.
    let mut control = Owner::new();
    let mut control_frame = None;
    let mut control_pending = false;
    for _ in 0..128 {
        match control.call(|| pool.claim(big, Purpose::New, &[])).unwrap() {
            IOResult::Done(f) => {
                control_frame = Some(f);
                break;
            }
            IOResult::IO(IOCompletions(c)) => {
                if !c.finished() {
                    control_pending = true;
                    break;
                }
            }
        }
    }
    let refetched = if control_frame.is_some() {
        let page = drive(&io, || pool.fix_exclusive(root, None)).unwrap();
        let correct = stamped(page.page(), root) == Some(1);
        drop(page);
        correct
    } else {
        false
    };
    drop(control);
    if let Some(f) = control_frame {
        pool.give_back_reserved(big, f);
    }
    let control_obtained = control_frame.is_some();
    drop(pool);
    drop(io);
    println!("S1_LARGE_RUN root={root:?} frame={frame} core_grains={core_grains} ready={ready} pending={pending} done={done} error={error:?} protected={protected} no_io={no_io} queued={queued} stalled_wait={stalled_wait} control_obtained={control_obtained} control_pending={control_pending} refetched={refetched}");
    assert!(
        no_io && error.is_none() && control_obtained && refetched,
        "same-pool capacity/refetch control must pass"
    );
    assert!(done, "protecting the core working page before reserving the large run blocks its own reservation");
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn audit_a_publication_barrier_stays_pending_across_an_early_poll() {
    use std::sync::mpsc;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/audit/s1/barrier", MIN_BUDGET);
    let c = class(4);
    let page = drive(&io, || pool.allocate(c)).unwrap();
    let pid = page.pid();
    drop(page);
    let epochs = pool.store.epochs().clone();
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let scanner = std::thread::spawn(move || {
        let step = epochs.enter();
        let _ = ready_tx.send(());
        let _ = release_rx.recv();
        drop(step);
    });
    let scanner_started = ready_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .is_ok();
    let mut owner = Owner::new();
    // Protocol unit entry, as in the author's reservation test: an earlier miss may be followed
    // by freed capacity before its top executes. The old scan still has to finish first.
    let first = owner
        .call(|| Ok::<_, crate::LimboError>(shortage::short::<()>(&pool, c, Purpose::New, &[pid])))
        .unwrap();
    let first_wait = match first {
        IOResult::IO(IOCompletions(w)) => Some(w),
        _ => None,
    };
    let first_pending = first_wait.as_ref().is_some_and(|c| !c.finished());
    let second = owner
        .call(|| Ok::<_, crate::LimboError>(shortage::short::<()>(&pool, c, Purpose::New, &[pid])))
        .unwrap();
    let second_ready = matches!(&second, IOResult::Done(_))
        || matches!(&second, IOResult::IO(w) if w.0.finished());
    let original_still_pending = first_wait.as_ref().is_some_and(|c| !c.finished());
    drop(second);
    drop(owner);
    let _ = release_tx.send(());
    let joined = scanner.join().is_ok();
    let finished_after = first_wait.as_ref().is_some_and(Completion::finished);
    drop(first_wait);
    drop(pool);
    drop(io);
    println!("S1_BARRIER started={scanner_started} first_pending={first_pending} second_ready={second_ready} original_still_pending={original_still_pending} joined={joined} finished_after={finished_after}");
    assert!(
        scanner_started && first_pending && original_still_pending && joined && finished_after,
        "real epoch barrier setup and release control must pass"
    );
    assert!(
        !second_ready,
        "a pending publication barrier cannot be bypassed by another call"
    );
}

/// The auditor's probe on impl-s1s2s3-v1 (bus #4003), kept as a test: a later demand's phase A
/// holds and protects nothing, its earlier publication retired, so the step's own page never
/// blocks the run it needs.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn audit_a_later_demand_reserves_without_its_earlier_protection() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/audit/s3/later-demand", MIN_BUDGET);
    let small = class(0);
    let big = class(CLASS_COUNT - 1);
    let core_grains = big.size() / small.size();
    let mut allocated = Vec::new();
    let mut chosen = None;
    for _ in 0..=memory::GRAINS {
        let mut page = drive(&io, || pool.allocate(small)).unwrap();
        let pid = page.pid();
        stamp(&mut page.page_mut(), pid, 1);
        page.changed();
        let frame = page.hint() - 1;
        allocated.push(pid);
        drop(page);
        if (frame as usize) < core_grains {
            chosen = Some((pid, frame));
            break;
        }
    }
    let (root, frame) = chosen.expect("ordinary allocation reached the core");
    for pid in allocated {
        if pid != root {
            drive(&io, || pool.free_unreferenced(pid)).unwrap();
        }
    }
    let mut owner = Owner::new();
    // Protocol-unit entry: an earlier miss may find capacity freed when its top reserves.
    // Only this first notification is forced; the later 8 MiB claim is a real pressure claim.
    let first = owner
        .call(|| {
            Ok::<_, crate::LimboError>(shortage::short::<()>(&pool, small, Purpose::New, &[root]))
        })
        .unwrap();
    let first_ready = matches!(&first, IOResult::IO(c) if c.0.finished());
    drop(first);
    let mut ready = 0;
    let mut pending = 0;
    let mut obtained = None;
    let mut error = None;
    for _ in 0..128 {
        match owner.call(|| {
            let page = match pool.fix_exclusive(root, None)? {
                IOResult::Done(p) => p,
                IOResult::IO(w) => return Ok(IOResult::IO(w)),
            };
            // A changed path now needs the larger run, before it uses the old small demand.
            let result = pool.claim(big, Purpose::New, &[root]);
            drop(page);
            result
        }) {
            Ok(IOResult::Done(f)) => {
                obtained = Some(f);
                break;
            }
            Ok(IOResult::IO(c)) => {
                if c.0.finished() {
                    ready += 1;
                } else {
                    pending += 1;
                    break;
                }
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    let protected = pool.protected(frame);
    let no_io = pool.requests.load(Ordering::Acquire) == 0;
    let queued = pool.shortage.len();
    drop(owner);
    if let Some(f) = obtained {
        pool.give_back_reserved(big, f);
    }
    // Positive control reserves BOTH runs first, retains them, then refetches the real page.
    let small_control = drive(&io, || pool.claim(small, Purpose::New, &[])).unwrap();
    let big_control = drive(&io, || pool.claim(big, Purpose::New, &[])).unwrap();
    let page = drive(&io, || pool.fix_exclusive(root, None)).unwrap();
    let refetched = stamped(page.page(), root) == Some(1);
    drop(page);
    pool.give_back_reserved(big, big_control);
    pool.give_back_reserved(small, small_control);
    drop(pool);
    drop(io);
    println!("S3_LATER_DEMAND root={root:?} frame={frame} first_ready={first_ready} ready={ready} pending={pending} obtained={} protected={protected} no_io={no_io} queued={queued} error={error:?} refetched={refetched}", obtained.is_some());
    assert!(
        first_ready && no_io && error.is_none() && refetched,
        "initial reservation and same-pool two-run/refetch capacity control must pass"
    );
    assert!(
        obtained.is_some(),
        "a later phase A must not retain protection that blocks its own demand"
    );
}

/// A step that comes short again in phase B ends its tenure and queues anew (DESIGN.md 9.1.3,
/// *Ending*): the frame reserved for its earlier demand is given back, and its new demand is
/// reserved in a fresh phase A.
#[test]
fn a_later_demand_ends_the_tenure_and_queues_again() {
    if let Err(e) = later_demand_queues_again() {
        panic!("{e}");
    }
}

fn later_demand_queues_again() -> std::result::Result<(), String> {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/queues-again", MIN_BUDGET);
    let (small, big) = (class(0), class(4));
    drop(drive(&io, || pool.allocate(small)).unwrap());
    let free = |p: &Arc<BufferPool>| p.memory.free_frames(small).len();
    let before = free(&pool);
    let pool_ref = &pool;
    let short = |c: SizeClass| {
        move || Ok::<_, crate::LimboError>(shortage::short::<()>(pool_ref, c, Purpose::New, &[]))
    };
    let mut owner = Owner::new();
    check!(
        matches!(owner.call(short(small)).unwrap(), IOResult::IO(w) if w.0.finished()),
        "the small demand reserved"
    );
    check!(
        owner.reserved_class() == Some(small.size()) && free(&pool) == before - 1,
        "{:?} reserved, {} free",
        owner.reserved_class(),
        free(&pool)
    );
    check!(
        matches!(owner.call(short(big)).unwrap(), IOResult::IO(w) if w.0.finished()),
        "the big demand reserved"
    );
    check!(
        owner.reserved_class() == Some(big.size()),
        "the tenure anew holds the big demand's frame alone: {:?}",
        owner.reserved_class()
    );
    check!(
        free(&pool) == before,
        "the earlier demand's frame given back: {} free",
        free(&pool)
    );
    check!(pool.shortage.len() == 1, "one place queued");
    drop(owner);
    check!(pool.shortage.len() == 0, "the queue empty");
    Ok(())
}

/// A change the change gate refused, its operation unwound to its top, waits for the gate's
/// reopening rather than yielding round and round, and the reopening finishes the wait
/// (DESIGN.md 9.1.3, the change gate closed by a cut). No relief is set here, as when another
/// caller or the checkpointing thread steps the cut.
#[test]
fn a_change_the_gate_refused_waits_for_its_reopening() {
    if let Err(e) = gate_refusal_waits() {
        panic!("{e}");
    }
}

fn gate_refusal_waits() -> std::result::Result<(), String> {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/gate-refused", MIN_BUDGET);
    check!(
        matches!(pool.store.close_gate(), IOResult::Done(())),
        "the gate closed with no change inside"
    );
    let mut owner = Owner::new();
    let refused = owner
        .call(|| Ok::<_, crate::LimboError>(shortage::gate_closed::<()>(&pool)))
        .unwrap();
    let wait = match refused {
        IOResult::IO(IOCompletions(w)) => w,
        IOResult::Done(()) => return Err("the refused change was done".into()),
    };
    check!(
        !wait.finished(),
        "the refused change yields instead of waiting"
    );
    pool.store.open_gate();
    check!(wait.finished(), "the reopening leaves the wait unfinished");
    check!(pool.store.gate_wait().is_none(), "the gate reads closed");
    Ok(())
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_hold_taken_after_locate_returns_the_claimed_frame_before_waiting() {
    use crate::race;
    use std::sync::mpsc;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/meta-read-race", MIN_BUDGET);
    let pid = drive(&io, || pool.store.allocate(class(0))).unwrap();
    let leaf = drive(&io, || pool.store.leaf(pid))
        .unwrap()
        .expect("allocated leaf");
    let (start_tx, start_rx) = mpsc::sync_channel(1);
    let (held_tx, held_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let owner = std::thread::spawn(move || {
        let _ = start_rx.recv();
        let hold = leaf.try_lock();
        let _ = held_tx.send(hold.is_some());
        let _ = release_rx.recv();
        if let Some(hold) = hold {
            hold.release_unchanged();
        }
    });
    let cancel_start = start_tx.clone();
    let reached = Arc::new(AtomicU32::new(0));
    {
        let reached = reached.clone();
        race::at("buffer.read_submit", move || {
            let _ = start_tx.send(());
            if held_rx.recv_timeout(std::time::Duration::from_secs(10)) == Ok(true) {
                reached.store(1, Ordering::Relaxed);
            }
        });
    }
    let wait = match pool.fix_exclusive(pid, None).unwrap() {
        IOResult::IO(c) => Some(c.0),
        IOResult::Done(page) => {
            drop(page);
            None
        }
    };
    let pending = wait
        .as_ref()
        .is_some_and(|c| !c.finished() && c.is_remote_wait());
    let returned = pool.table.find(&pool.frames.headers, pid.raw()).is_none();
    let no_request = pool.requests.load(Ordering::Acquire) == 0;
    // Let the holder exit even when a defective build never reaches the injection.
    let _ = cancel_start.try_send(());
    let _ = release_tx.send(());
    let joined = owner.join().is_ok();
    let finished = wait.as_ref().is_some_and(Completion::finished);
    if let Some(c) = &wait {
        if !c.finished() {
            c.complete(0);
        }
    }
    let mut resumed = false;
    for _ in 0..8 {
        match pool.fix_exclusive(pid, None).unwrap() {
            IOResult::Done(page) => {
                resumed = page.page().u64_at(HEADER_SIZE) == 0;
                drop(page);
                break;
            }
            IOResult::IO(c) => {
                if !c.finished() {
                    c.0.complete(0);
                }
            }
        }
    }
    let hook = reached.load(Ordering::Relaxed) == 1;
    drop(wait);
    drop(reached);
    drop(pool);
    drop(io);
    assert!(hook&&joined&&pending&&returned&&no_request&&finished&&resumed,"a read that finds metadata held submits no request, removes its loading entry, returns the claimed frame and resumes after release");
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn audit_a_cut_keeps_a_page_leaving_before_its_first_swap_copy() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/audit-cut-first-swap", MIN_BUDGET);
    let mut page = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (page.pid(), page.frame);
    stamp(&mut page.page_mut(), pid, 1);
    page.changed();
    drop(page);
    let initially_unmapped = pool.store.swap_slot(pid).is_none();
    let latch = || FrameLatch {
        frames: pool.frames.clone(),
        frame,
    };
    let outgoing = pool.start_write_out(pool.latch_to_evict(class(0), frame, latch).unwrap());
    // A real writeback queued for another call to submit, not a test-only unqueued hold.
    pool.write_out(class(0), vec![outgoing]);
    let mut scan = CutScan::default();
    let mut cut_done = false;
    for _ in 0..4 {
        if matches!(pool.begin_cut(1, &mut scan), IOResult::Done(())) {
            cut_done = true;
            break;
        }
    }
    let included = scan.owed.contains(&pid);
    let back = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
    let bytes_ok = back.read(|p| stamped(p, pid)) == Some(Some(1));
    let pending = pool.header(back.frame).pending.load(Ordering::Acquire);
    drop(back);
    pool.end_cut(1);
    drop(pool);
    drop(io);
    println!("FIRST_SWAP initially_unmapped={initially_unmapped} cut_done={cut_done} included={included} back_pending={pending} bytes_ok={bytes_ok}");
    assert!(
        initially_unmapped && cut_done && bytes_ok,
        "the fixture must have a first queued writeout and finish its cleanup"
    );
    assert!(
        included,
        "the cut omitted an owed page before its first swap copy was available"
    );
    assert_eq!(
        pending, 1,
        "the page read back must still owe the cut its image"
    );
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_cut_rejects_an_outgoing_header_if_the_frame_was_reused() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-outgoing-reused", MIN_BUDGET);
    let mut page = drive(&io, || pool.allocate(class(0))).unwrap();
    let (old_pid, frame) = (page.pid(), page.frame);
    stamp(&mut page.page_mut(), old_pid, 1);
    page.changed();
    drop(page);
    let new_pid = drive(&io, || pool.store.allocate(class(0))).unwrap();
    let latch = || FrameLatch {
        frames: pool.frames.clone(),
        frame,
    };
    let out = pool.start_write_out(pool.latch_to_evict(class(0), frame, latch).unwrap());
    pool.write_out(class(0), vec![out]);
    {
        let pool = pool.clone();
        crate::race::at("buffer.cut_outgoing_seen", move || {
            // Complete the old real write and reuse that exact physical frame for another page.
            let wait = pool.drive_writebacks().unwrap();
            assert!(
                wait.is_none(),
                "MemoryIO must finish the old write in one step"
            );
            let taken = pool
                .memory
                .take_where(class(0), |candidate| candidate == frame);
            assert!(matches!(taken, memory::Take::Frame(got) if got == frame));
            let step = pool.store.epochs().enter();
            let mut page = pool.install(frame, new_pid, step);
            stamp(&mut page.page_mut(), new_pid, 2);
            page.changed();
            drop(page);
            let latch = || FrameLatch {
                frames: pool.frames.clone(),
                frame,
            };
            let out = pool.start_write_out(pool.latch_to_evict(class(0), frame, latch).unwrap());
            pool.write_out(class(0), vec![out]);
        });
    }
    let mut owed = Vec::new();
    let accepted = pool.record_outgoing(frame, &mut owed, &mut HashMap::new());
    let hooked = !crate::race::pending("buffer.cut_outgoing_seen");
    if !hooked {
        crate::race::point("buffer.cut_outgoing_seen");
    }
    let stranger_added = owed.contains(&new_pid);
    let page = drive(&io, || pool.fix_optimistic(new_pid, None)).unwrap();
    let bytes_ok = page.read(|p| stamped(p, new_pid)) == Some(Some(2));
    drop(page);
    drop(pool);
    drop(io);
    println!("OUTGOING_REUSE hooked={hooked} accepted={accepted} stranger_added={stranger_added} bytes_ok={bytes_ok}");
    assert!(
        hooked && bytes_ok,
        "the old write must finish and its frame must be reused"
    );
    assert!(
        !accepted && !stranger_added,
        "the old holds version admitted a different pages header"
    );
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_cut_leaves_out_temporary_pages_being_written_out() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-outgoing-temporary", MIN_BUDGET);
    let mut page = drive(&io, || pool.allocate_temporary(class(0))).unwrap();
    let (pid, frame) = (page.pid(), page.frame);
    stamp(&mut page.page_mut(), pid, 1);
    page.changed();
    drop(page);
    let latch = || FrameLatch {
        frames: pool.frames.clone(),
        frame,
    };
    let out = pool.start_write_out(pool.latch_to_evict(class(0), frame, latch).unwrap());
    pool.write_out(class(0), vec![out]);
    let mut scan = CutScan::default();
    let cut_done = matches!(pool.begin_cut(1, &mut scan), IOResult::Done(()));
    let omitted = !scan.owed.contains(&pid);
    let page = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
    let bytes_ok = page.read(|p| stamped(p, pid)) == Some(Some(1));
    drop(page);
    pool.end_cut(1);
    drop(pool);
    drop(io);
    println!("OUTGOING_TEMP cut_done={cut_done} omitted={omitted} bytes_ok={bytes_ok}");
    assert!(
        cut_done && bytes_ok,
        "the temporary writeout must finish normally"
    );
    assert!(omitted, "temporary pages cannot enter the durable cut");
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn audit_a_cut_waits_for_a_resident_frame_in_its_main_walk() {
    let trace = audit_cut_latch_trace(false);
    println!("CUT_LATCH_MAIN {trace:?}");
    assert_eq!(
        (trace.pending, trace.ready),
        (32, 0),
        "a held resident frame must return a pending producer wait"
    );
    assert_eq!(trace.wakes_before, 0, "a paused holder produces no wake");
    assert_eq!(
        trace.wakes_after, 1,
        "releasing the marked frame must wake its cut waiter once"
    );
    assert!(trace.finished && trace.cut_done && trace.page_pending == 1);
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn audit_a_cut_waits_for_a_resident_frame_in_its_swap_copy_walk() {
    let trace = audit_cut_latch_trace(true);
    println!("CUT_LATCH_SWAPPED {trace:?}");
    assert_eq!(
        (trace.pending, trace.ready),
        (32, 0),
        "the swap-copy walk must wait for its resident frame"
    );
    assert_eq!(trace.wakes_before, 0);
    assert_eq!(
        trace.wakes_after, 1,
        "the resident frames release is the cuts producer"
    );
    assert!(trace.finished && trace.cut_done && trace.page_pending == 1);
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[derive(Debug)]
struct AuditCutLatchTrace {
    pending: usize,
    ready: usize,
    wakes_before: usize,
    wakes_after: usize,
    finished: bool,
    cut_done: bool,
    page_pending: u64,
}

#[cfg(not(any(shuttle, atoll_loom)))]
fn audit_cut_latch_trace(swapped: bool) -> AuditCutLatchTrace {
    use std::sync::mpsc;
    use std::task::{Wake, Waker};
    struct Wakes(std::sync::atomic::AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/audit-cut-latch", MIN_BUDGET);
    let mut page = drive(&io, || pool.allocate(class(0))).unwrap();
    let pid = page.pid();
    stamp(&mut page.page_mut(), pid, 1);
    page.changed();
    drop(page);
    if swapped {
        let mut sweep = pool.sweep();
        drive(&io, || sweep.step(&pool)).unwrap();
        assert!(
            pool.store.swap_slot(pid).is_some(),
            "a real swap copy must exist"
        );
    }
    let wake = std::sync::Arc::new(Wakes(std::sync::atomic::AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut trace = AuditCutLatchTrace {
        pending: 0,
        ready: 0,
        wakes_before: 0,
        wakes_after: 0,
        finished: false,
        cut_done: false,
        page_pending: 0,
    };
    std::thread::scope(|scope| {
        let (held_tx, held_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let holder_pool = pool.clone();
        let holder_io = io.clone();
        let holder = scope.spawn(move || {
            let page = drive(&holder_io, || holder_pool.fix_exclusive(pid, None)).unwrap();
            held_tx.send(page.frame).unwrap();
            resume_rx.recv().unwrap();
            page.release_unchanged();
        });
        let frame = held_rx.recv().unwrap();
        let mut scan = CutScan::default();
        if swapped {
            // The initial walk has passed before a page with a swap copy is found resident.
            scan.next = pool.memory.grains();
        }
        let mut waited = None;
        for _ in 0..32 {
            match pool.begin_cut(1, &mut scan) {
                IOResult::Done(()) => {
                    trace.cut_done = true;
                    break;
                }
                IOResult::IO(IOCompletions(c)) => {
                    if c.finished() {
                        trace.ready += 1;
                    } else {
                        trace.pending += 1;
                    }
                    c.set_waker(&waker);
                    waited = Some(c);
                }
            }
        }
        trace.wakes_before = wake.0.load(std::sync::atomic::Ordering::Relaxed);
        resume_tx.send(()).unwrap();
        holder.join().unwrap();
        trace.wakes_after = wake.0.load(std::sync::atomic::Ordering::Relaxed);
        trace.finished = waited.as_ref().is_some_and(|c| c.finished());
        for _ in 0..4 {
            if matches!(pool.begin_cut(1, &mut scan), IOResult::Done(())) {
                trace.cut_done = true;
                break;
            }
        }
        trace.page_pending = pool.header(frame).pending.load(Ordering::Acquire);
        pool.end_cut(1);
    });
    drop(pool);
    drop(io);
    drop(waker);
    drop(wake);
    trace
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_frame_latch_release_finishes_its_wait_after_unlock_and_scope_close() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as StdOrdering};
    use std::task::{Wake, Waker};
    struct Observe {
        pool: crate::sync::Weak<BufferPool>,
        frame: u32,
        calls: AtomicUsize,
        unlocked: AtomicBool,
        scope_closed: AtomicBool,
    }
    impl Wake for Observe {
        fn wake(self: std::sync::Arc<Self>) {
            self.calls.fetch_add(1, StdOrdering::Relaxed);
            let unlocked = self
                .pool
                .upgrade()
                .is_some_and(|pool| pool.header(self.frame).latch.optimistic().is_some());
            self.unlocked.store(unlocked, StdOrdering::Relaxed);
            self.scope_closed.store(
                !crate::storage::atoll::notify::scope_open(),
                StdOrdering::Relaxed,
            );
        }
    }
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/frame-release-observed", MIN_BUDGET);
    let page = drive(&io, || pool.allocate(class(0))).unwrap();
    let frame = page.frame;
    drop(page);
    let hold = Held::try_take(FrameLatch {
        frames: pool.frames.clone(),
        frame,
    })
    .unwrap();
    let wait = pool.wait_for_resident_latch(frame);
    let observer = std::sync::Arc::new(Observe {
        pool: Arc::downgrade(&pool),
        frame,
        calls: AtomicUsize::new(0),
        unlocked: AtomicBool::new(false),
        scope_closed: AtomicBool::new(false),
    });
    wait.set_waker(&Waker::from(observer.clone()));
    let was_pending = !wait.finished();
    hold.release_unchanged();
    let finished = wait.finished();
    let calls = observer.calls.load(StdOrdering::Relaxed);
    let unlocked = observer.unlocked.load(StdOrdering::Relaxed);
    let scope_closed = observer.scope_closed.load(StdOrdering::Relaxed);
    // Cleanup must not depend on the production release notification being correct.
    if !wait.finished() {
        crate::storage::atoll::notify::finish(&wait, Ok(0));
    }
    drop(wait);
    drop(observer);
    drop(pool);
    drop(io);
    println!("FRAME_LATCH_RELEASE pending={was_pending} finished={finished} calls={calls} unlocked={unlocked} scope_closed={scope_closed}");
    assert!(was_pending && finished && calls == 1 && unlocked && scope_closed);
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn releasing_a_marked_prefix_frame_does_not_finish_another_frames_wait() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/frame-specific-prefix", MIN_BUDGET);
    let target = drive(&io, || pool.allocate(class(0))).unwrap();
    let prefix = drive(&io, || pool.allocate(class(0))).unwrap();
    let distinct = target.frame != prefix.frame;
    let target_wait = pool.wait_for_resident_latch(target.frame);
    let prefix_wait = pool.wait_for_resident_latch(prefix.frame);
    let both_pending = !target_wait.finished() && !prefix_wait.finished();
    prefix.release_unchanged();
    let prefix_finished = prefix_wait.finished();
    let target_still_pending = !target_wait.finished();
    target.release_unchanged();
    let target_finished = target_wait.finished();
    if !target_wait.finished() {
        crate::storage::atoll::notify::finish(&target_wait, Ok(0));
    }
    if !prefix_wait.finished() {
        crate::storage::atoll::notify::finish(&prefix_wait, Ok(0));
    }
    drop(prefix_wait);
    drop(target_wait);
    drop(pool);
    drop(io);
    println!("FRAME_PREFIX distinct={distinct} both_pending={both_pending} prefix_finished={prefix_finished} target_still_pending={target_still_pending} target_finished={target_finished}");
    assert!(
        distinct && both_pending && prefix_finished && target_still_pending && target_finished,
        "a different frames release finished the target wait"
    );
}

#[cfg(not(any(shuttle, atoll_loom, miri)))]
#[test]
fn unmarked_frame_holds_and_repeated_wait_lookups_add_no_allocations() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/frame-release-allocation", MIN_BUDGET);
    let page = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (page.pid(), page.frame);
    page.release_unchanged();
    for _ in 0..2 {
        let page = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
        page.release_unchanged();
    }
    let before = crate::storage::atoll::testing::allocations();
    for _ in 0..32 {
        let page = match pool.fix_exclusive(pid, None).unwrap() {
            IOResult::Done(page) => page,
            IOResult::IO(_) => panic!("the unmodified resident page must be available"),
        };
        page.release_unchanged();
    }
    let hold_allocations = crate::storage::atoll::testing::allocations() - before;
    let no_signal = pool.frames.released[frame as usize]
        .get(&pool.frames.completions)
        .unwrap()
        .is_none();
    let page = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
    let first = pool.wait_for_resident_latch(frame);
    drop(pool.wait_for_resident_latch(frame)); // Warm the protected read slot too.
    let before = crate::storage::atoll::testing::allocations();
    let mut all_pending = true;
    for _ in 0..32 {
        all_pending &= !pool.wait_for_resident_latch(frame).finished();
    }
    let wait_allocations = crate::storage::atoll::testing::allocations() - before;
    let metadata_bytes = pool.frames.released.len() * std::mem::size_of::<CompletionCell>();
    let expected_bytes = pool.memory.grains() as usize * 8;
    page.release_unchanged();
    let finished = first.finished();
    drop(first);
    drop(pool);
    drop(io);
    println!("FRAME_ALLOC hold_allocations={hold_allocations} no_signal={no_signal} wait_allocations={wait_allocations} all_pending={all_pending} finished={finished} metadata_bytes={metadata_bytes} expected_bytes={expected_bytes}");
    assert!(hold_allocations == 0 && no_signal && wait_allocations == 0 && all_pending && finished);
    assert_eq!(metadata_bytes, expected_bytes);
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_resident_wait_rechecks_the_status_of_the_hold_it_observed() {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let pool = open_on(&io, "/db/frame-wait-new-loading", MIN_BUDGET);
    let mut page = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (page.pid(), page.frame);
    stamp(&mut page.page_mut(), pid, 1);
    page.changed();
    drop(page);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let held = pool
        .latch_to_evict(class(0), frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .unwrap();
    {
        let pool = pool.clone();
        crate::race::at("buffer.frame_before_look", move || {
            // A clean resident is evicted and its frame starts a real pumped read of that page.
            pool.evict(class(0), frame, held);
            assert!(
                matches!(pool.memory.take_where(class(0), |f| f == frame), memory::Take::Frame(f) if f == frame)
            );
            let located = match pool.locate(pid).unwrap() {
                IOResult::Done(located) => located,
                IOResult::IO(_) => panic!("the real swap copy is already known"),
            };
            let loaded = pool.load(pid, frame, located).unwrap();
            assert!(matches!(loaded, IOResult::Done(Some(_))));
        });
    }
    let wait = pool.wait_for_resident_latch(frame);
    let hooked = !crate::race::pending("buffer.frame_before_look");
    if !hooked {
        crate::race::point("buffer.frame_before_look");
    }
    let loading = pool.header(frame).status() == LOADING;
    let changed = wait.finished();
    let remote = wait.is_remote_wait();
    // Cleanup pumps the actual read; it is deliberately after measuring the returned wait.
    let page = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
    let bytes_ok = page.read(|p| stamped(p, pid)) == Some(Some(1));
    drop(page);
    if !wait.finished() {
        crate::storage::atoll::notify::finish(&wait, Ok(0));
    }
    drop(wait);
    drop(pool);
    drop(io);
    drop(crash);
    println!("FRAME_NEW_LOADING hooked={hooked} loading={loading} changed={changed} remote={remote} bytes_ok={bytes_ok}");
    assert!(
        hooked && loading && bytes_ok,
        "the frame must move from resident to a real pending pumped read"
    );
    assert!(
        changed,
        "a stale resident observation returned a remote wait on a newly loading IO hold"
    );
}

#[cfg(not(any(shuttle, atoll_loom, miri)))]
#[test]
fn optimistic_guard_waits_follow_the_page_latch_and_free_changes_allocate_nothing() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/optimistic-wait-adapters", MIN_BUDGET);
    let page = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (page.pid(), page.frame);
    page.release_unchanged();
    let guard = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
    let before = crate::storage::atoll::testing::allocations();
    let free_ready = guard.wait_if_held().finished();
    let free_allocations = crate::storage::atoll::testing::allocations() - before;
    let no_free_signal = pool.frames.released[frame as usize]
        .get(&pool.frames.completions)
        .unwrap()
        .is_none();
    let immediate = match guard.upgrade_or_wait() {
        Ok(page) => {
            page.release_unchanged();
            true
        }
        Err(_) => false,
    };
    let stale = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
    let mut writer = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
    writer.changed();
    drop(writer);
    let before = crate::storage::atoll::testing::allocations();
    let changed_ready = match stale.upgrade_or_wait() {
        Err(c) => c.finished(),
        Ok(page) => {
            page.release_unchanged();
            false
        }
    };
    let changed_allocations = crate::storage::atoll::testing::allocations() - before;
    let guard = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
    let writer = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
    let read_wait = guard.wait_if_held();
    let upgrade_wait = guard.upgrade_or_wait().err();
    let pending = !read_wait.finished() && upgrade_wait.as_ref().is_some_and(|c| !c.finished());
    writer.release_unchanged();
    let finished = read_wait.finished() && upgrade_wait.as_ref().is_some_and(|c| c.finished());
    if !read_wait.finished() {
        crate::storage::atoll::notify::finish(&read_wait, Ok(0));
    }
    if let Some(c) = &upgrade_wait {
        if !c.finished() {
            crate::storage::atoll::notify::finish(c, Ok(0));
        }
    }
    drop(read_wait);
    drop(upgrade_wait);
    drop(pool);
    drop(io);
    println!("OPTIMISTIC_WAIT free_ready={free_ready} free_allocations={free_allocations} no_free_signal={no_free_signal} immediate={immediate} changed_ready={changed_ready} changed_allocations={changed_allocations} pending={pending} finished={finished}");
    assert!(free_ready && no_free_signal && immediate && changed_ready && pending && finished);
    assert!(
        free_allocations == 0 && changed_allocations == 0,
        "a free latch or changed free version must create no wait allocation"
    );
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_frame_wait_is_not_published_before_the_latch_unlocks() {
    use std::cell::Cell;
    use std::rc::Rc;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/frame-unlock-before-ready", MIN_BUDGET);
    let page = drive(&io, || pool.allocate(class(0))).unwrap();
    let frame = page.frame;
    page.release_unchanged();
    let page = drive(&io, || {
        pool.fix_exclusive(PageId::from_raw(pool.header(frame).pid()), None)
    })
    .unwrap();
    let wait = pool.wait_for_resident_latch(frame);
    let observed = Rc::new(Cell::new(None));
    {
        let observed = observed.clone();
        let wait = wait.clone();
        let pool = pool.clone();
        crate::race::at("buffer.frame_will_unlock", move || {
            let held = matches!(pool.header(frame).latch.look(), Look::Held(_));
            observed.set(Some((held, wait.finished())));
        });
    }
    page.release_unchanged();
    let hooked = !crate::race::pending("buffer.frame_will_unlock");
    if !hooked {
        crate::race::point("buffer.frame_will_unlock");
    }
    let observation = observed.get();
    let finished = wait.finished();
    if !finished {
        crate::storage::atoll::notify::finish(&wait, Ok(0));
    }
    drop(wait);
    drop(pool);
    drop(io);
    println!("FRAME_RESULT_ORDER hooked={hooked} observation={observation:?} finished={finished}");
    assert!(
        hooked && observation == Some((true, false)) && finished,
        "a frame wait was published while the latch was still held"
    );
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_frame_release_before_the_mark_returns_a_changed_wait() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/frame-release-before-mark", MIN_BUDGET);
    let page = drive(&io, || pool.allocate(class(0))).unwrap();
    let frame = page.frame;
    page.release_unchanged();
    let hold = Held::try_take(FrameLatch {
        frames: pool.frames.clone(),
        frame,
    })
    .unwrap();
    crate::race::at("buffer.frame_before_mark", move || hold.release_unchanged());
    let wait = pool.wait_for_resident_latch(frame);
    let hooked = !crate::race::pending("buffer.frame_before_mark");
    if !hooked {
        crate::race::point("buffer.frame_before_mark");
    }
    let changed = wait.finished();
    // A late unused cell is bounded by its frame; no caller was given that unfinished cell.
    pool.frames.finish_frame_wait(frame);
    if !wait.finished() {
        crate::storage::atoll::notify::finish(&wait, Ok(0));
    }
    drop(wait);
    drop(pool);
    drop(io);
    println!("FRAME_RELEASE_BEFORE_MARK hooked={hooked} changed={changed}");
    assert!(
        hooked && changed,
        "the lost mark returned a wait with no remaining holder"
    );
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_frame_wait_finishes_when_its_hold_becomes_a_queued_writeout() {
    frame_to_outgoing("buffer.frame_wait_registered");
    frame_to_outgoing("buffer.frame_before_mark");
}

#[cfg(not(any(shuttle, atoll_loom)))]
fn frame_to_outgoing(at: &'static str) {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/frame-transition-outgoing", MIN_BUDGET);
    let mut page = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (page.pid(), page.frame);
    stamp(&mut page.page_mut(), pid, 1);
    page.changed();
    drop(page);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let held = pool
        .latch_to_evict(class(0), frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .unwrap();
    {
        let pool = pool.clone();
        crate::race::at(at, move || {
            let out = pool.start_write_out(held);
            pool.write_out(class(0), vec![out]);
        });
    }
    let wait = pool.wait_for_resident_latch(frame);
    let hooked = !crate::race::pending(at);
    if !hooked {
        crate::race::point(at);
    }
    let changed = wait.finished();
    let outgoing = pool.header(frame).status() == OUTGOING;
    let mut scan = CutScan::default();
    let mut cut_done = false;
    for _ in 0..4 {
        if matches!(pool.begin_cut(1, &mut scan), IOResult::Done(())) {
            cut_done = true;
            break;
        }
    }
    let page = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
    let bytes_ok = page.read(|p| stamped(p, pid)) == Some(Some(1));
    drop(page);
    pool.end_cut(1);
    if !wait.finished() {
        crate::storage::atoll::notify::finish(&wait, Ok(0));
    }
    drop(wait);
    drop(pool);
    drop(io);
    println!("FRAME_OUTGOING at={at} hooked={hooked} changed={changed} outgoing={outgoing} cut_done={cut_done} bytes_ok={bytes_ok}");
    assert!(hooked && outgoing && cut_done && bytes_ok);
    assert!(
        changed,
        "queueing the writeout left its old resident wait unfinished"
    );
}

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_queued_frame_wakes_after_publication_and_rechecks_a_late_registration() {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as StdOrdering};
    use std::task::{Wake, Waker};
    struct Queued {
        pool: crate::sync::Weak<BufferPool>,
        calls: AtomicUsize,
        queued: AtomicBool,
        scope_closed: AtomicBool,
    }
    impl Wake for Queued {
        fn wake(self: std::sync::Arc<Self>) {
            self.calls.fetch_add(1, StdOrdering::Relaxed);
            self.queued.store(
                self.pool
                    .upgrade()
                    .is_some_and(|p| p.queued.load(Ordering::Acquire) > 0),
                StdOrdering::Relaxed,
            );
            self.scope_closed.store(
                !crate::storage::atoll::notify::scope_open(),
                StdOrdering::Relaxed,
            );
        }
    }
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/frame-queued-before-wake", MIN_BUDGET);
    let mut page = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (page.pid(), page.frame);
    stamp(&mut page.page_mut(), pid, 1);
    page.changed();
    drop(page);
    let held = pool
        .latch_to_evict(class(0), frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .unwrap();
    let wait = pool.wait_for_resident_latch(frame);
    let pending = !wait.finished();
    let observed = std::sync::Arc::new(Queued {
        pool: Arc::downgrade(&pool),
        calls: AtomicUsize::new(0),
        queued: AtomicBool::new(false),
        scope_closed: AtomicBool::new(false),
    });
    wait.set_waker(&Waker::from(observed.clone()));
    let late = Rc::new(Cell::new(None));
    {
        let pool = pool.clone();
        let late = late.clone();
        crate::race::at("buffer.outgoing_waits_finished", move || {
            late.set(Some(pool.wait_for_resident_latch(frame).finished()));
        });
    }
    let out = pool.start_write_out(held);
    pool.write_out(class(0), vec![out]);
    let hooked = !crate::race::pending("buffer.outgoing_waits_finished");
    if !hooked {
        crate::race::point("buffer.outgoing_waits_finished");
    }
    let finished = wait.finished();
    let calls = observed.calls.load(StdOrdering::Relaxed);
    let queued = observed.queued.load(StdOrdering::Relaxed);
    let scope_closed = observed.scope_closed.load(StdOrdering::Relaxed);
    let late_changed = late.get() == Some(true);
    let page = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
    let bytes_ok = page.read(|p| stamped(p, pid)) == Some(Some(1));
    drop(page);
    if !wait.finished() {
        crate::storage::atoll::notify::finish(&wait, Ok(0));
    }
    drop(wait);
    drop(observed);
    drop(pool);
    drop(io);
    println!("FRAME_QUEUE pending={pending} finished={finished} calls={calls} queued={queued} scope_closed={scope_closed} late_changed={late_changed} bytes_ok={bytes_ok}");
    assert!(pending && finished && calls == 1 && queued && scope_closed && late_changed && bytes_ok, "frame notification ran before queue publication or a late registration missed the new status");
}

/// A sweep that finds a dirty page held for a change, and a write-back whose copy finds its page
/// held, each come back on the wait the holder's release finishes, never on a yield it would come
/// back from to find the page still held; each then writes the page.
#[cfg(not(shuttle))]
#[test]
fn a_sweep_or_a_copy_finding_its_page_held_waits_for_its_release() {
    let mut outcomes = Vec::new();
    for case in ["sweep", "copy"] {
        let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
        let pool = open_on(&io, "/db/held-at-write", MIN_BUDGET);
        let mut model = Model {
            io: &io,
            pool: &pool,
            pages: Vec::new(),
        };
        model.allocate(class(0));
        let pid = model.pages[0].pid;
        let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
        let mut sweep = pool.sweep();
        let (passed, let_go): (_, Box<dyn FnOnce()>) = if case == "sweep" {
            // Held here before the sweep looks: a change under way.
            let page = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
            let mut first = None;
            drive(&io, || {
                first = Some(sweep.step(&pool)?);
                Ok(IOResult::Done(()))
            })
            .unwrap();
            (first, Box::new(move || drop(page.release_unchanged())))
        } else {
            // Taken by another thread once the sweep claimed the page, as its copy is taken.
            let hold = HoldAt::set("buffer.copy_for_write", &io, &pool, move |_, _| {
                (vec![], vec![pid])
            });
            let passed = step_past(&io, "buffer.copy_for_write", || sweep.step(&pool));
            (passed, Box::new(move || hold.let_go()))
        };
        let wait = match passed {
            Some(IOResult::IO(IOCompletions(c))) => Some(c),
            _ => None,
        };
        // An unrelated wake: room given back finishes the global supply, never this wait.
        pool.wake_room();
        let pending = wait.as_ref().is_some_and(|c| !c.finished());
        let_go();
        let woken = wait.as_ref().is_some_and(|c| c.finished());
        drive(&io, || sweep.step(&pool)).unwrap();
        let written = !pool.header(frame).dirty();
        model.check_all();
        outcomes.push((case, pending, woken, written));
    }
    for (case, pending, woken, written) in outcomes {
        assert!(
            pending,
            "{case}: the step came back ready, not on one pending wait"
        );
        assert!(
            woken,
            "{case}: the holder's release left the wait unfinished"
        );
        assert!(written, "{case}: the page was not written once let go");
    }
}

/// A frame freed while the shortage gate's holder waits in phase A for its class goes to that
/// holder, not to the free lists: a fast claim racing the free finds none, and the holder's next
/// call allocates in exactly that frame. A holder whose tenure ends with a handed frame unused
/// gives it back, to the next claim (DESIGN.md 9.1.3, *Tenure*, phase A).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_frame_freed_while_the_gate_holder_waits_goes_to_it_not_to_a_fast_claim() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/phase-a-handoff", MIN_BUDGET);
    // One frame per batch: every batch taken, nothing of the class free and nothing to evict.
    let c = class(9);
    let mut taken = Vec::new();
    for _ in 0..pool.memory.batches {
        match pool.claim(c, Purpose::New, &[]).unwrap() {
            IOResult::Done(f) => taken.push(f),
            IOResult::IO(_) => panic!("an empty pool's claim waited"),
        }
    }
    let fast_claim = |pool: &Arc<BufferPool>| match pool.claim(c, Purpose::New, &[]).unwrap() {
        IOResult::Done(f) => Some(f),
        IOResult::IO(_) => None,
    };
    let mut owner = super::Owner::new();
    let first = owner.call(|| pool.claim(c, Purpose::New, &[]));
    let waited = matches!(&first, Ok(IOResult::IO(IOCompletions(w))) if !w.finished());
    let queued = owner.queued();
    // Freed while the holder waits, and a fast claim at once after.
    let freed = taken.pop().unwrap();
    pool.free_frame(c, freed, pool.take_free_latch(freed));
    let raced = fast_claim(&pool);
    let woken = matches!(&first, Ok(IOResult::IO(IOCompletions(w))) if w.finished());
    let second = owner.call(|| pool.claim(c, Purpose::New, &[]));
    let allocated = match second {
        Ok(IOResult::Done(f)) => Some(f),
        _ => None,
    };
    if let Some(f) = allocated {
        pool.free_frame(c, f, pool.take_free_latch(f));
    }
    owner.end();
    if let Some(f) = raced {
        taken.push(f);
    }

    // A holder handed a frame whose tenure ends before it runs gives the frame back.
    let back = fast_claim(&pool);
    if let Some(f) = back {
        taken.push(f);
    }
    let mut ended = super::Owner::new();
    let waiting = matches!(
        ended.call(|| pool.claim(c, Purpose::New, &[])),
        Ok(IOResult::IO(IOCompletions(w))) if !w.finished()
    );
    let handed = taken.pop().unwrap();
    pool.free_frame(c, handed, pool.take_free_latch(handed));
    let kept_from_fast_claim = fast_claim(&pool).is_none();
    ended.end();
    let given_back = fast_claim(&pool);
    for f in taken.into_iter().chain(given_back) {
        pool.free_frame(c, f, pool.take_free_latch(f));
    }
    // Checked once every owner and the pool are gone: a panic unwinding through them ends the
    // process.
    drop((owner, ended, pool));
    assert!(waited && queued, "the holder did not wait in phase A");
    assert_eq!(
        raced, None,
        "a fast claim took the frame freed while the holder waited"
    );
    assert!(woken, "the free left the holder's wait unfinished");
    assert_eq!(
        allocated,
        Some(freed),
        "the holder's next call did not allocate in the freed frame"
    );
    assert!(
        back.is_some(),
        "the frame freed after the allocation was not free again"
    );
    assert!(waiting, "the second holder did not wait in phase A");
    assert!(
        kept_from_fast_claim,
        "a fast claim took the frame handed to the second holder"
    );
    assert_eq!(
        given_back,
        Some(handed),
        "the frame handed to an ended tenure was not given back"
    );
}

/// The auditor's P2-S6-2 (impl-s6a-v1 review): a frame handed for a read that is not quiet yet,
/// a step older than its free still under way on another thread, gives its holder one wait,
/// however often it is called early, and the place's end finishes that wait (DESIGN.md 9.1.3,
/// phase A).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_handed_frame_not_quiet_gives_its_holder_one_wait_its_end_finishes() {
    use std::sync::mpsc;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/phase-a-quiet", MIN_BUDGET);
    let c = class(9);
    let mut taken = Vec::new();
    for _ in 0..pool.memory.batches {
        match pool.claim(c, Purpose::New, &[]).unwrap() {
            IOResult::Done(f) => taken.push(f),
            IOResult::IO(_) => panic!("an empty pool's claim waited"),
        }
    }
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (leave_tx, leave_rx) = mpsc::channel::<()>();
    let waits = std::thread::scope(|scope| {
        let pool_ref = &pool;
        let reader = scope.spawn(move || {
            // A step under way before the free: the freed frame is not quiet until it ends.
            let step = pool_ref.store.epochs().enter();
            entered_tx.send(()).unwrap();
            let _ = leave_rx.recv();
            drop(step);
        });
        entered_rx.recv().unwrap();
        let mut owner = super::Owner::new();
        let first = owner.call(|| pool.claim(c, Purpose::Read, &[]));
        let waited = matches!(&first, Ok(IOResult::IO(IOCompletions(w))) if !w.finished());
        let freed = taken.pop().unwrap();
        pool.free_frame(c, freed, pool.take_free_latch(freed));
        let mut early = Vec::new();
        for _ in 0..32 {
            match owner.call(|| pool.claim(c, Purpose::Read, &[])) {
                Ok(IOResult::IO(IOCompletions(w))) => early.push(w),
                _ => break,
            }
        }
        let pending = early.len() == 32 && early.iter().all(|w| !w.finished());
        owner.end();
        let finished_at_end = early.iter().filter(|w| w.finished()).count();
        drop(owner);
        let _ = leave_tx.send(());
        let _ = reader.join();
        (waited, pending, finished_at_end, early.len(), freed)
    });
    let (waited, pending, finished_at_end, calls, freed) = waits;
    // The frame went back with the place: free again.
    let back = match pool.claim(c, Purpose::New, &[]).unwrap() {
        IOResult::Done(f) => Some(f),
        IOResult::IO(_) => None,
    };
    for f in taken.into_iter().chain(back) {
        pool.free_frame(c, f, pool.take_free_latch(f));
    }
    drop(pool);
    assert!(waited, "the holder did not wait in phase A");
    assert!(pending, "{calls} early calls, not all on a pending wait");
    assert_eq!(
        finished_at_end, 32,
        "the place's end left a quiet wait unfinished"
    );
    assert_eq!(
        back,
        Some(freed),
        "the handed frame did not go back with the place"
    );
}

/// A run formed for the class the gate's holder waits for, here a batch freed whole and taken by a
/// fast claim's growth, sets the holder's frame aside before the run is published: the fast claim
/// that formed it finds nothing, and the holder's next call has the frame (DESIGN.md 9.1.3, phase
/// A).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_run_formed_for_the_waited_class_goes_to_the_gate_holder_before_any_claim() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/phase-a-run", MIN_BUDGET);
    let c = class(9);
    let mut taken = Vec::new();
    for _ in 0..pool.memory.batches {
        match pool.claim(c, Purpose::New, &[]).unwrap() {
            IOResult::Done(f) => taken.push(f),
            IOResult::IO(_) => panic!("an empty pool's claim waited"),
        }
    }
    let mut owner = super::Owner::new();
    let first = owner.call(|| pool.claim(c, Purpose::New, &[]));
    let waited = matches!(&first, Ok(IOResult::IO(IOCompletions(w))) if !w.finished());
    // One batch drained and its frame freed: the batch leaves the class whole, free, so nothing
    // goes to the holder until a run forms of it.
    let freed = taken.pop().unwrap();
    let batch = freed / crate::storage::atoll::buffer::memory::GRAINS;
    assert!(pool.memory.drain(c, batch), "the drain did not start");
    pool.free_frame(c, freed, pool.take_free_latch(freed));
    // The free finished the holder's first wait. A wait taken now is finished only by a change
    // after it: here, the run's hand-over.
    let after_free = pool.frames.supply.ticket().remote();
    // A fast claim grows the class by that batch.
    let raced = match pool.claim(c, Purpose::New, &[]).unwrap() {
        IOResult::Done(f) => Some(f),
        IOResult::IO(_) => None,
    };
    let woken = after_free.finished();
    let got = match owner.call(|| pool.claim(c, Purpose::New, &[])) {
        Ok(IOResult::Done(f)) => Some(f),
        _ => None,
    };
    owner.end();
    for f in taken.into_iter().chain(raced).chain(got) {
        pool.free_frame(c, f, pool.take_free_latch(f));
    }
    drop((owner, pool));
    assert!(waited, "the holder did not wait in phase A");
    assert_eq!(
        raced, None,
        "the fast claim took the frame of the run it formed"
    );
    assert!(
        woken,
        "the run's hand-over left the holder's wait unfinished"
    );
    assert_eq!(
        got,
        Some(freed),
        "the holder's next call did not have the run's frame"
    );
}

/// A phase-A claim can wait on the rare path's gate. A frame handed meanwhile satisfies the
/// demand independently: wake the holder's own admission, without completing the foreign gate.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn audit_a_handed_frame_wakes_phase_a_while_its_old_foreign_gate_stays_held() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::task::{Wake, Waker};
    struct Calls(AtomicUsize);
    impl Wake for Calls {
        fn wake(self: std::sync::Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/audit/s6ab/grant-while-gate-held", MIN_BUDGET);
    let large = class(10);
    let small = class(9);
    let big = match pool.take_or_grow(large, Purpose::New) {
        Got::Frame(f) => f,
        _ => panic!("initial large frame"),
    };
    let mut taken = Vec::new();
    while let Got::Frame(f) = pool.take_or_grow(small, Purpose::New) {
        taken.push(f);
    }
    let counts = std::sync::Arc::new(Calls(AtomicUsize::new(0)));
    let waker = Waker::from(counts.clone());
    let (
        held,
        queued,
        pending,
        old_was_gate,
        finished_on_grant,
        foreign_on_grant,
        wakes_on_grant,
        got,
        foreign_after_release,
        wakes_after_release,
    ) = std::thread::scope(|scope| {
        let (held_tx, held_rx) = mpsc::channel();
        let (leave_tx, leave_rx) = mpsc::channel();
        let pool_ref = &pool;
        let holder = scope.spawn(move || {
            let held = pool_ref.memory.take_token(memory::RUN_TOKEN);
            let _ = held_tx.send(held);
            let _ = leave_rx.recv();
            if held {
                pool_ref.let_token_go(memory::RUN_TOKEN);
            }
        });
        let held = held_rx.recv().unwrap();
        let foreign_gate = pool.frames.supply.gate_wait();
        let mut owner = super::Owner::new();
        // The existing test-only scarcity injection makes this operation enter phase A;
        // advance then runs the actual unowned pool claim, with the real rare gate held.
        let first = super::shortage::scarce(|| owner.call(|| pool.claim(large, Purpose::New, &[])));
        let queued = owner.queued();
        let wait = match first {
            Ok(IOResult::IO(IOCompletions(w))) => Some(w),
            _ => None,
        };
        if let Some(w) = &wait {
            w.set_waker(&waker);
        }
        let pending = wait.as_ref().is_some_and(|w| !w.finished());
        let old_was_gate = wait
            .as_ref()
            .is_some_and(|w| w.addr() == foreign_gate.addr());
        pool.free_frame(large, big, pool.take_free_latch(big));
        let finished_on_grant = wait.as_ref().is_some_and(Completion::finished);
        let foreign_on_grant = foreign_gate.finished();
        let wakes_on_grant = counts.0.load(Ordering::Relaxed);
        // An early call proves the granted frame suffices even while that old gate stays held.
        let got = match owner.call(|| pool.claim(large, Purpose::New, &[])) {
            Ok(IOResult::Done(f)) => Some(f),
            _ => None,
        };
        owner.end();
        if let Some(f) = got {
            pool.free_frame(large, f, pool.take_free_latch(f));
        }
        let _ = leave_tx.send(());
        let _ = holder.join();
        let foreign_after_release = foreign_gate.finished();
        let wakes_after_release = counts.0.load(Ordering::Relaxed);
        drop(owner);
        (
            held,
            queued,
            pending,
            old_was_gate,
            finished_on_grant,
            foreign_on_grant,
            wakes_on_grant,
            got,
            foreign_after_release,
            wakes_after_release,
        )
    });
    for f in taken {
        pool.free_frame(small, f, pool.take_free_latch(f));
    }
    drop((pool, io, counts, waker));
    eprintln!("PHASE_A_GRANT_GATE held={held} queued={queued} pending={pending} old_wait_is_foreign_gate={old_was_gate} finished_on_grant={finished_on_grant} foreign_on_grant={foreign_on_grant} wakes_on_grant={wakes_on_grant} got={got:?} expected={big} foreign_after_release={foreign_after_release} wakes_after_release={wakes_after_release}");
    assert!(
        held && queued && pending && got == Some(big) && foreign_after_release,
        "the phase-A/rare-gate/grant preconditions were not reached"
    );
    assert!(
        !foreign_on_grant,
        "granting memory finished another producer's gate"
    );
    assert!(
        finished_on_grant && wakes_on_grant == 1,
        "the handed frame did not wake its phase-A holder, whose old gate was no longer needed"
    );
}

/// The census's K1: maintenance a foreground operation helps along (the collector's step after a
/// begin) runs as no owner's operation. A claim of it that finds no frame leaves the operation's
/// owner untouched when the operation goes on to a wait of its own, and it never takes the frame
/// reserved for the operation, which the operation's own claim then gets (DESIGN.md 9.1.3,
/// *Maintenance keeps what it owns*).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn maintenance_a_foreground_operation_helps_along_leaves_its_owner_and_frame_alone() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/maintenance-unowned", MIN_BUDGET);
    let c = class(9);
    let mut taken = Vec::new();
    for _ in 0..pool.memory.batches {
        match pool.claim(c, Purpose::New, &[]).unwrap() {
            IOResult::Done(f) => taken.push(f),
            IOResult::IO(_) => panic!("an empty pool's claim waited"),
        }
    }
    // An operation that helps maintenance along, whose claim finds no frame, then goes on to a
    // wait of its own: its owner takes no place in the queue.
    let mut owner = super::Owner::new();
    let helped = super::scarce(|| {
        owner.call(|| {
            let _ = super::unowned(|| pool.claim(c, Purpose::New, &[]));
            Ok(IOResult::<()>::IO(IOCompletions(notify::remote_wait())))
        })
    });
    let untouched = matches!(helped, Ok(IOResult::IO(_))) && !owner.queued();
    owner.end();
    drop(owner);

    // An operation with a frame reserved for it: the maintenance it helps along does not take it.
    let mut owner = super::Owner::new();
    let first = owner.call(|| pool.claim(c, Purpose::New, &[]));
    let waited = matches!(&first, Ok(IOResult::IO(_))) && owner.queued();
    let freed = taken.pop().unwrap();
    pool.free_frame(c, freed, pool.take_free_latch(freed));
    let mut maintenance = None;
    let own = owner.call(|| {
        maintenance = Some(super::unowned(|| pool.claim(c, Purpose::New, &[])));
        pool.claim(c, Purpose::New, &[])
    });
    let maintenance_took = match maintenance {
        Some(Ok(IOResult::Done(f))) => Some(f),
        _ => None,
    };
    let own_got = match own {
        Ok(IOResult::Done(f)) => Some(f),
        _ => None,
    };
    owner.end();
    for f in taken.into_iter().chain(maintenance_took).chain(own_got) {
        pool.free_frame(c, f, pool.take_free_latch(f));
    }
    drop((owner, pool));
    assert!(
        untouched,
        "the operation's owner took the maintenance's demand"
    );
    assert!(waited, "the owner did not wait in phase A");
    assert_eq!(
        maintenance_took, None,
        "the maintenance took the operation's reserved frame"
    );
    assert_eq!(
        own_got,
        Some(freed),
        "the operation's own claim did not get its reserved frame"
    );
}

/// Takes every one-frame batch of `c` in `pool`, as raw claims.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
fn take_every_frame(pool: &Arc<BufferPool>, c: SizeClass) -> Vec<u32> {
    (0..pool.memory.batches)
        .map(|_| match pool.claim(c, Purpose::New, &[]).unwrap() {
            IOResult::Done(f) => f,
            IOResult::IO(_) => panic!("an empty pool's claim waited"),
        })
        .collect()
}

/// The checkpoint's demand comes first (DESIGN.md 9.1.3, *Maintenance keeps what it owns*): a frame
/// freed while it and the shortage gate's holder both wait for the class goes to the checkpoint,
/// not to the holder nor to a fast claim, and the checkpoint's next step, on another thread, has it.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_freed_frame_goes_to_the_checkpoints_demand_before_the_gate_holder_and_any_claim() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/upkeep-first", MIN_BUDGET);
    let c = class(9);
    let mut taken = take_every_frame(&pool, c);
    let mut upkeep = super::Upkeep::default();
    let first = super::upkeep_scope(&mut upkeep, || pool.claim(c, Purpose::New, &[]));
    let checkpoint_waits = matches!(&first, Ok(IOResult::IO(IOCompletions(w))) if !w.finished());
    let mut owner = super::Owner::new();
    let holder = owner.call(|| pool.claim(c, Purpose::New, &[]));
    let holder_waits =
        matches!(&holder, Ok(IOResult::IO(IOCompletions(w))) if !w.finished()) && owner.queued();
    let freed = taken.pop().unwrap();
    pool.free_frame(c, freed, pool.take_free_latch(freed));
    let fast = match pool.claim(c, Purpose::New, &[]).unwrap() {
        IOResult::Done(f) => Some(f),
        IOResult::IO(_) => None,
    };
    let checkpoint_woken = matches!(&first, Ok(IOResult::IO(IOCompletions(w))) if w.finished());
    // The holder may wake on the free's supply event; it does not have the frame.
    let holder_left = !matches!(
        owner.call(|| pool.claim(c, Purpose::New, &[])),
        Ok(IOResult::Done(_))
    );
    // The checkpoint's next step, taken by another thread.
    let (got, upkeep) = std::thread::scope(|s| {
        s.spawn(|| {
            let got = match super::upkeep_scope(&mut upkeep, || pool.claim(c, Purpose::New, &[])) {
                Ok(IOResult::Done(f)) => Some(f),
                _ => None,
            };
            (got, upkeep)
        })
        .join()
        .unwrap()
    });
    owner.end();
    for f in taken.into_iter().chain(fast).chain(got) {
        pool.free_frame(c, f, pool.take_free_latch(f));
    }
    drop((upkeep, owner, pool));
    assert!(
        checkpoint_waits && holder_waits,
        "the checkpoint and the holder did not both wait"
    );
    assert_eq!(
        fast, None,
        "a fast claim took the frame the checkpoint waited for"
    );
    assert!(
        checkpoint_woken,
        "the free left the checkpoint's wait unfinished"
    );
    assert!(
        holder_left,
        "the gate holder was given the checkpoint's frame"
    );
    assert_eq!(
        got,
        Some(freed),
        "the checkpoint's next step did not have the freed frame"
    );
}

/// A checkpoint step that no longer claims, its page found resident or its step moved on, lets its
/// demand go: a frame handed to it meanwhile goes back, to the next claim.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_checkpoint_step_that_no_longer_claims_gives_back_what_was_handed_to_it() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/upkeep-abandon", MIN_BUDGET);
    let c = class(9);
    let mut taken = take_every_frame(&pool, c);
    let mut upkeep = super::Upkeep::default();
    let first = super::upkeep_scope(&mut upkeep, || pool.claim(c, Purpose::New, &[]));
    let waited = matches!(&first, Ok(IOResult::IO(IOCompletions(w))) if !w.finished());
    let freed = taken.pop().unwrap();
    pool.free_frame(c, freed, pool.take_free_latch(freed));
    let kept = match pool.claim(c, Purpose::New, &[]).unwrap() {
        IOResult::Done(f) => Some(f),
        IOResult::IO(_) => None,
    };
    // The next step finds what it needed without claiming.
    super::upkeep_scope(&mut upkeep, || ());
    let back = match pool.claim(c, Purpose::New, &[]).unwrap() {
        IOResult::Done(f) => Some(f),
        IOResult::IO(_) => None,
    };
    for f in taken.into_iter().chain(kept).chain(back) {
        pool.free_frame(c, f, pool.take_free_latch(f));
    }
    drop((upkeep, pool));
    assert!(waited, "the checkpoint did not wait");
    assert_eq!(
        kept, None,
        "a claim took the frame handed to the checkpoint"
    );
    assert_eq!(
        back,
        Some(freed),
        "the frame handed to an abandoned demand did not go back"
    );
}

/// A frame handed to the checkpoint for a read that is not quiet yet, a step older than its free
/// still under way on another thread, gives the checkpoint one wait however often it steps early,
/// and the demand's end finishes that wait and gives the frame back.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_frame_handed_to_the_checkpoint_not_quiet_gives_it_one_wait() {
    use std::sync::mpsc;
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/upkeep-quiet", MIN_BUDGET);
    let c = class(9);
    let mut taken = take_every_frame(&pool, c);
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (leave_tx, leave_rx) = mpsc::channel::<()>();
    let (waited, one, early, finished_at_end, freed, census) = std::thread::scope(|s| {
        let pool_ref = &pool;
        let reader = s.spawn(move || {
            let step = pool_ref.store.epochs().enter();
            entered_tx.send(()).unwrap();
            let _ = leave_rx.recv();
            drop(step);
        });
        entered_rx.recv().unwrap();
        let mut upkeep = super::Upkeep::default();
        let first = super::upkeep_scope(&mut upkeep, || pool.claim(c, Purpose::Read, &[]));
        let waited = matches!(&first, Ok(IOResult::IO(IOCompletions(w))) if !w.finished());
        let freed = taken.pop().unwrap();
        pool.free_frame(c, freed, pool.take_free_latch(freed));
        let mut early = Vec::new();
        for _ in 0..32 {
            match super::upkeep_scope(&mut upkeep, || pool.claim(c, Purpose::Read, &[])) {
                Ok(IOResult::IO(IOCompletions(w))) => early.push(w),
                _ => break,
            }
        }
        let one = early.len() == 32
            && early
                .iter()
                .all(|w| !w.finished() && w.addr() == early[0].addr());
        let census = pool.taken_frames_census();
        drop(upkeep);
        let finished_at_end = early.iter().filter(|w| w.finished()).count();
        let _ = leave_tx.send(());
        let _ = reader.join();
        (waited, one, early.len(), finished_at_end, freed, census)
    });
    let mut unplaced = taken.clone();
    unplaced.sort_unstable();
    let back = match pool.claim(c, Purpose::New, &[]).unwrap() {
        IOResult::Done(f) => Some(f),
        IOResult::IO(_) => None,
    };
    for f in taken.into_iter().chain(back) {
        pool.free_frame(c, f, pool.take_free_latch(f));
    }
    drop(pool);
    assert!(waited, "the checkpoint did not wait");
    assert!(one, "{early} early steps, not all on one pending wait");
    assert_eq!(
        finished_at_end, 32,
        "the demand's end left its quiet wait unfinished"
    );
    assert_eq!(
        back,
        Some(freed),
        "the handed frame did not go back with the demand"
    );
    // The frames the test took for itself are in no place; the one kept for the checkpoint's
    // demand is in its place.
    let named: Vec<u32> = census
        .iter()
        .filter(|line| line.contains(": taken and in no place [];"))
        .filter_map(|line| line.strip_prefix("frame ")?.split(' ').next()?.parse().ok())
        .collect();
    assert!(
        named == unplaced && census.len() == unplaced.len(),
        "the census with frame {freed} kept for the checkpoint's demand names other than the \
         frames the test took, {unplaced:?}: {census:#?}"
    );
}

/// A writer that helps the checkpoint along (its wait for room relieving the checkpoint, within the
/// writer's own call) keeps its owner untouched: the checkpoint's claim that finds no frame is the
/// checkpoint's demand, not the writer's shortage.
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_checkpoint_step_inside_an_operations_call_leaves_the_operations_owner_alone() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/upkeep-masks-owner", MIN_BUDGET);
    let c = class(9);
    let taken = take_every_frame(&pool, c);
    let mut upkeep = super::Upkeep::default();
    let mut owner = super::Owner::new();
    let helped = super::scarce(|| {
        owner.call(|| {
            let step = super::upkeep_scope(&mut upkeep, || pool.claim(c, Purpose::New, &[]));
            let waited = matches!(step, Ok(IOResult::IO(_)));
            Ok(IOResult::<bool>::IO(IOCompletions(if waited {
                notify::remote_wait()
            } else {
                Completion::new_yield()
            })))
        })
    });
    let untouched = matches!(helped, Ok(IOResult::IO(_))) && !owner.queued();
    let named = pool.frames.supply.upkeep.wants(c);
    owner.end();
    drop((owner, upkeep));
    for f in taken {
        pool.free_frame(c, f, pool.take_free_latch(f));
    }
    drop(pool);
    assert!(
        untouched,
        "the operation's owner took the checkpoint's demand"
    );
    assert!(named, "the checkpoint's claim named no demand of its own");
}

/// A page the cut's walk marks owed, written back and evicted once the cut has listed its swap
/// shard, and read back in before the cut has listed the rest, still owes the cut its copy: its
/// first change after the cut keeps it as the cut found it, and the checkpoint images that copy
/// (OPEN-CORRUPT1). Before, it was read back unmarked, the list of pages owed not yet set, so its
/// change kept nothing and the checkpoint imaged the page as it stood after the change: a leaf
/// split after the cut, beside its parent as the cut found it.
#[test]
fn a_page_read_back_in_during_the_cut_still_owes_the_cut_its_copy() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-read-back", MIN_KEEPING_BUDGET);
    const N: u64 = 1;
    let shards = pool.store.swap_shards();
    let pid = owed_page_before_the_last_shard(&io, &pool);
    // The cut, run by run, until every shard but the last is listed.
    let mut scan = CutScan::default();
    let mut ran = true;
    while ran && scan.shard + 1 < shards {
        ran = matches!(pool.cut_run(N, &mut scan), IOResult::Done(false));
    }
    let marked = marked_for(&pool, pid, N);
    // Written back, evicted and read in again, its swap copy landing after its shard was listed.
    let evicted = read_back_in(&io, &pool, pid);
    let ended = finish_cut(&pool, N, &mut scan);
    let (kept, imaged) = first_change_and_image(&io, &pool, pid, N);
    pool.end_cut(N);
    drop(pool);
    assert!(
        ran && ended,
        "the cut waited or ended before its last shard"
    );
    assert!(marked, "the walk did not mark the page owed");
    assert!(evicted, "the page was not evicted");
    assert!(kept, "the change found no room for its copy");
    assert_eq!(
        imaged,
        ("kept", Some(1)),
        "the checkpoint's image of a page read back in during the cut"
    );
}

/// A run of the cut's walk that stops at a frame another holds lists the pages it marked before
/// it stopped: one of them read back in after the cut still owes the cut its copy (mutant: the
/// pages listed only when a run ends at its last frame).
#[test]
fn a_page_marked_by_a_run_that_stops_at_a_held_frame_still_owes_the_cut_its_copy() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-run-stopped", MIN_KEEPING_BUDGET);
    const N: u64 = 1;
    let pid = owed_page(&io, &pool, 1);
    // A second page, later in the same run of frames, held as the run reaches it.
    let frame_of = |pid: PageId| pool.table.find(&pool.frames.headers, pid.raw()).unwrap();
    let held = loop {
        let other = owed_page(&io, &pool, 1);
        let (a, b) = (frame_of(pid), frame_of(other));
        if b > a && a as usize / CUT_RUN == b as usize / CUT_RUN {
            break other;
        }
    };
    let holder = drive(&io, || pool.fix_exclusive(held, None)).unwrap();
    let mut scan = CutScan::default();
    let mut stopped = false;
    while !stopped && scan.next < pool.memory.grains() {
        stopped = matches!(pool.cut_run(N, &mut scan), IOResult::IO(_));
    }
    let marked = marked_for(&pool, pid, N);
    holder.release_unchanged();
    let ended = finish_cut(&pool, N, &mut scan);
    let evicted = read_back_in(&io, &pool, pid);
    let (kept, imaged) = first_change_and_image(&io, &pool, pid, N);
    pool.end_cut(N);
    drop(pool);
    assert!(stopped, "the run did not stop at the held frame");
    assert!(ended, "the cut waited with no holder");
    assert!(marked, "the run did not mark the page before it stopped");
    assert!(evicted, "the page was not evicted");
    assert!(kept, "the change found no room for its copy");
    assert_eq!(
        imaged,
        ("kept", Some(1)),
        "the checkpoint's image of a page a stopped run marked"
    );
}

/// A page owed with no frame at the cut, its swap copy listed by a shard's run, read in after the
/// cut still owes the cut its copy (mutant: the shards' pages not listed).
#[test]
fn a_page_owed_in_its_swap_copy_alone_read_in_after_the_cut_still_owes_the_cut_its_copy() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-swapped", MIN_KEEPING_BUDGET);
    const N: u64 = 1;
    let pid = owed_page(&io, &pool, 1);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let evicted = pool.evict_clean(pid);
    let mut scan = CutScan::default();
    let ended = finish_cut(&pool, N, &mut scan);
    let listed = scan.owed.contains(&pid);
    let g = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
    g.release_unchanged();
    let (kept, imaged) = first_change_and_image(&io, &pool, pid, N);
    pool.end_cut(N);
    drop(pool);
    assert!(evicted, "the page was not evicted");
    assert!(ended, "the cut waited with no holder");
    assert!(listed, "the cut did not find the page owed");
    assert!(kept, "the change found no room for its copy");
    assert_eq!(
        imaged,
        ("kept", Some(1)),
        "the checkpoint's image of a page owed in its swap copy alone"
    );
}

/// A page owed to the cut, evicted to its swap copy so its batch holds no frame, and read back in
/// while the walk passes an empty batch, still owes the cut its copy (DEFAULT-CACHE1): the walk
/// passes a batch with no frame taken over whole, and the page's swap copy is listed by the shards'
/// runs, as before the walk could pass a batch (mutant: a passed batch's grains never walked again
/// by anything, the shards' pages not listed).
#[test]
fn a_page_read_back_in_while_the_walk_passes_an_empty_batch_still_owes_the_cut_its_copy() {
    use crate::race;
    use std::{cell::Cell, rc::Rc};
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-passed-batch", MIN_KEEPING_BUDGET);
    const N: u64 = 1;
    let pid = owed_page(&io, &pool, 1);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let evicted = pool.evict_clean(pid);
    let read_in = Rc::new(Cell::new(false));
    {
        // The pool held weakly: an action the walk never reached is dropped at the thread's end,
        // where the pool must not be.
        let (io, pool, read_in) = (io.clone(), Arc::downgrade(&pool), read_in.clone());
        race::at("buffer.cut_run_passed_a_batch", move || {
            let pool = pool.upgrade().expect("the pool is open during its cut");
            // Evicted before the cut already: read in again here, from its swap copy.
            read_back_in(&io, &pool, pid);
            read_in.set(true);
        });
    }
    let mut scan = CutScan::default();
    let ended = finish_cut(&pool, N, &mut scan);
    let passed = !race::pending("buffer.cut_run_passed_a_batch");
    let listed = scan.owed.contains(&pid);
    let (kept, imaged) = first_change_and_image(&io, &pool, pid, N);
    pool.end_cut(N);
    drop(pool);
    assert!(evicted, "the page was not evicted");
    assert!(passed, "the walk passed no empty batch");
    assert!(
        read_in.get(),
        "the page was not read in while the walk passed"
    );
    assert!(ended, "the cut waited with no holder");
    assert!(listed, "the cut did not find the page owed");
    assert!(kept, "the change found no room for its copy");
    assert_eq!(
        imaged,
        ("kept", Some(1)),
        "the checkpoint's image of a page read back in while the walk passed an empty batch"
    );
}

/// A page the walk marks and that is evicted and read in again before its run lists it, the run
/// not yet returned, still owes the cut its copy: its swap copy landed before the shards are
/// listed, so the cut marks it again once they are (mutant: the swapped pages not marked again).
#[test]
fn a_page_read_back_in_before_its_run_lists_it_still_owes_the_cut_its_copy() {
    use crate::race;
    use std::{cell::Cell, rc::Rc};
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-before-listing", MIN_KEEPING_BUDGET);
    const N: u64 = 1;
    let pid = owed_page(&io, &pool, 1);
    let frame = pool.table.find(&pool.frames.headers, pid.raw()).unwrap();
    let mut scan = CutScan::default();
    // Runs until the one that walks the page's frame.
    let mut walked = true;
    while walked && (scan.next as usize) / CUT_RUN < frame as usize / CUT_RUN {
        walked = matches!(pool.cut_run(N, &mut scan), IOResult::Done(false));
    }
    let evicted = Rc::new(Cell::new(false));
    {
        let (io, pool, evicted) = (io.clone(), pool.clone(), evicted.clone());
        race::at("buffer.cut_run_listing", move || {
            evicted.set(read_back_in(&io, &pool, pid));
        });
    }
    let ended = finish_cut(&pool, N, &mut scan);
    let ran = !race::pending("buffer.cut_run_listing");
    let (kept, imaged) = first_change_and_image(&io, &pool, pid, N);
    pool.end_cut(N);
    drop(pool);
    assert!(walked && ended, "the cut waited with no holder");
    assert!(ran, "the run never reached its listing");
    assert!(evicted.get(), "the page was not evicted");
    assert!(kept, "the change found no room for its copy");
    assert_eq!(
        imaged,
        ("kept", Some(1)),
        "the checkpoint's image of a page read back in before its run listed it"
    );
}

/// A page read in from its swap copy during the cut, its read landed before the cut lists it and
/// the page made resident only after the cut's last look at its frame, still owes the cut its
/// copy: the read makes the page resident before it reads the list, so either it finds the page
/// listed or the cut finds the frame resident and held and marks it once the read lets go (mutant:
/// the list read before the page is resident).
#[test]
fn a_page_made_resident_after_the_cuts_last_look_at_its_frame_still_owes_the_cut_its_copy() {
    use crate::race;
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-last-look", MIN_KEEPING_BUDGET);
    const N: u64 = 1;
    // Owed, in its swap copy alone.
    let pid = owed_page(&io, &pool, 1);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let evicted = pool.evict_clean(pid);
    // The walk over the frames, then the read: the rest of the cut runs once the read has landed,
    // its page not yet resident.
    let scan = Rc::new(RefCell::new(CutScan::default()));
    let mut walked = true;
    while walked && scan.borrow().next < pool.memory.grains() {
        walked = matches!(
            pool.cut_run(N, &mut scan.borrow_mut()),
            IOResult::Done(false)
        );
    }
    let ended = Rc::new(Cell::new(false));
    {
        let (pool, scan, ended) = (pool.clone(), scan.clone(), ended.clone());
        race::at("buffer.loaded_resident", move || {
            ended.set(finish_cut(&pool, N, &mut scan.borrow_mut()));
        });
    }
    let g = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
    g.release_unchanged();
    let ran = !race::pending("buffer.loaded_resident");
    let listed = scan.borrow().owed.contains(&pid);
    let (kept, imaged) = first_change_and_image(&io, &pool, pid, N);
    pool.end_cut(N);
    drop(pool);
    assert!(evicted, "the page was not evicted");
    assert!(walked, "the walk over the frames waited");
    assert!(
        ran && ended.get(),
        "the cut did not end where the read landed"
    );
    assert!(listed, "the cut did not find the page owed");
    assert!(kept, "the change found no room for its copy");
    assert_eq!(
        imaged,
        ("kept", Some(1)),
        "the checkpoint's image of a page made resident after the cut's last look"
    );
}

/// A page owed in its swap copy alone, read in and changed while the cut walks the frames, before
/// a shard's run lists it, as a leaf put may (it does not enter the change gate), is listed at the
/// count its swap copy holds and marked by the late look at the count it stands at then: the count
/// its image must stand at (mutant: the late look leaves the count the shard's run listed).
#[test]
fn a_page_changed_during_the_walk_before_its_shard_lists_it_is_owed_at_its_count_then() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-changed-in-walk", MIN_KEEPING_BUDGET);
    const N: u64 = 1;
    let pid = owed_page(&io, &pool, 1);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let evicted = pool.evict_clean(pid);
    let swapped_at = pool.store.swap_count(pid);
    let mut scan = CutScan::default();
    let mut walked = true;
    while walked && scan.next < pool.memory.grains() {
        walked = matches!(pool.cut_run(N, &mut scan), IOResult::Done(false));
    }
    // Read in and changed with no copy: not yet listed, so not pending.
    let mut g = drive(&io, || pool.fix_exclusive(pid, None)).unwrap();
    let kept = pool.keep(&[&g], &mut RoomWait::default()).is_ok();
    stamp(&mut g.page_mut(), pid, 2);
    g.changed();
    let now = pool.header(g.frame).changes.load(Ordering::Acquire);
    drop(g);
    let ended = finish_cut(&pool, N, &mut scan);
    let counted = scan.counts.get(&pid).copied();
    let marked = marked_for(&pool, pid, N);
    pool.end_cut(N);
    drop(pool);
    assert!(evicted, "the page was not evicted");
    assert!(walked && ended, "the cut waited with no holder");
    assert!(kept, "the change found no room");
    assert!(marked, "the late look did not mark the page");
    assert_ne!(
        swapped_at,
        Some(now),
        "the change moved the page past its swap copy"
    );
    assert_eq!(
        counted,
        Some(now),
        "the count a page changed during the walk is to be imaged at"
    );
}

/// A page owed in its swap copy alone, freed as unreferenced with no cut in effect, is read in
/// before its free: a cut that began after the free looked would owe it, and an unread free keeps
/// no copy (OPEN-CORRUPT1; mutant: the swap copy not asked about).
#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn a_page_in_its_swap_copy_alone_is_read_in_before_an_unreferenced_free() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/unreferenced-swapped", MIN_KEEPING_BUDGET);
    let pid = owed_page(&io, &pool, 1);
    let mut sweep = pool.sweep();
    drive(&io, || sweep.step(&pool)).unwrap();
    let evicted = pool.evict_clean(pid);
    let swapped = pool.store.swap_slot(pid).is_some();
    let before = pool.loaded_pages().len();
    let freed = drive(&io, || pool.free_unreferenced(pid)).is_ok();
    let read_in = pool.loaded_pages()[before..].contains(&pid.raw());
    drop(pool);
    assert!(evicted && swapped, "the page is not in its swap copy alone");
    assert!(freed, "the free failed");
    assert!(read_in, "the page was freed unread");
}

/// A new page stamped `version` and changed, so owed an image: resident, no swap copy.
fn owed_page(io: &Arc<dyn IO>, pool: &Arc<BufferPool>, version: u64) -> PageId {
    let mut g = drive(io, || pool.allocate(class(0))).unwrap();
    let pid = g.pid();
    stamp(&mut g.page_mut(), pid, version);
    g.changed();
    drop(g);
    pid
}

/// An owed page, resident with no swap copy, whose swap copy would go in a shard the cut lists
/// before its last: written back once to learn its shard, that copy then given up.
fn owed_page_before_the_last_shard(io: &Arc<dyn IO>, pool: &Arc<BufferPool>) -> PageId {
    let shards = pool.store.swap_shards();
    loop {
        let pid = owed_page(io, pool, 1);
        let mut sweep = pool.sweep();
        drive(io, || sweep.step(pool)).unwrap();
        let shard = (0..shards)
            .find(|&i| {
                pool.store
                    .swap_shard_pages(i, None, usize::MAX)
                    .contains(&pid)
            })
            .expect("written back");
        pool.store.drop_swap(pid);
        let mut g = drive(io, || pool.fix_exclusive(pid, None)).unwrap();
        stamp(&mut g.page_mut(), pid, 1);
        g.changed();
        drop(g);
        if shard + 1 < shards {
            return pid;
        }
    }
}

/// Whether `pid`'s frame is marked owed a copy for cut `n`.
fn marked_for(pool: &Arc<BufferPool>, pid: PageId, n: u64) -> bool {
    let frame = pool.table.find(&pool.frames.headers, pid.raw()).unwrap();
    pool.owes_copy(frame, pid, n)
}

/// Writes `pid` back, evicts it and reads it in again: whether it was evicted.
fn read_back_in(io: &Arc<dyn IO>, pool: &Arc<BufferPool>, pid: PageId) -> bool {
    let mut sweep = pool.sweep();
    drive(io, || sweep.step(pool)).unwrap();
    let evicted = pool.evict_clean(pid);
    let g = drive(io, || pool.fix_exclusive(pid, None)).unwrap();
    g.release_unchanged();
    evicted
}

/// Runs cut `n` to its end: whether it got there with no wait.
fn finish_cut(pool: &Arc<BufferPool>, n: u64, scan: &mut CutScan) -> bool {
    loop {
        match pool.cut_run(n, scan) {
            IOResult::Done(true) => return true,
            IOResult::Done(false) => {}
            IOResult::IO(_) => return false,
        }
    }
}

/// `pid`'s first change after cut `n`, as every change makes it (its copies first, then version
/// 2), then what checkpoint `n` images of it: whether the change had room for its copies, and
/// whether the image is the kept copy or the page as it stands, with its version.
fn first_change_and_image(
    io: &Arc<dyn IO>,
    pool: &Arc<BufferPool>,
    pid: PageId,
    n: u64,
) -> (bool, (&'static str, Option<u64>)) {
    let mut g = drive(io, || pool.fix_exclusive(pid, None)).unwrap();
    let kept = pool.keep(&[&g], &mut RoomWait::default()).is_ok();
    stamp(&mut g.page_mut(), pid, 2);
    g.changed();
    drop(g);
    let imaged = match drive(io, || pool.image_source(n, pid)).unwrap() {
        Source::Kept(copy) => ("kept", stamped(&copy[..], pid)),
        Source::Live { bytes, .. } => ("live", stamped(&bytes[..], pid)),
    };
    (kept, imaged)
}

/// A cut walks the frames a run at a time (DESIGN.md 22.1, 22.9 item 1): the frame headers one
/// page of the header table holds, then a shard of the swap table, then a run of the swapped
/// pages, and finds owed what one walk finds (mutant: the frames walked whole in one run).
#[test]
fn a_cut_walks_the_frames_a_run_at_a_time() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-runs", MIN_BUDGET);
    let grains = pool.memory.grains();
    let mut stored = Vec::new();
    for v in 0..8 {
        let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, v);
        g.changed();
        drop(g);
        stored.push(pid);
    }
    let mut scan = CutScan::default();
    let (mut runs, mut most) = (0usize, 0u32);
    // The frames a run looks at: those of batches with a frame taken, a batch with none being
    // passed over whole.
    let walked = |from: u32, to: u32| {
        (from..to.min(grains))
            .filter(|&g| !pool.memory.holds_no_frame(g / memory::GRAINS))
            .count() as u32
    };
    let in_use = walked(0, grains);
    loop {
        let before = scan.next;
        let done = match pool.cut_run(1, &mut scan) {
            IOResult::Done(done) => done,
            IOResult::IO(_) => panic!("a cut with no holder waited"),
        };
        most = most.max(walked(before, scan.next));
        runs += 1;
        if done {
            break;
        }
    }
    pool.end_cut(1);
    let mut owed = std::mem::take(&mut scan.owed);
    owed.sort();
    stored.sort();
    drop(pool);
    assert!(
        in_use as usize > 2 * CUT_RUN,
        "{in_use} frames in batches in use: a walk of one run"
    );
    assert_eq!(most as usize, CUT_RUN, "a run looked at {most} frames");
    assert!(
        runs > (in_use as usize).div_ceil(CUT_RUN),
        "{runs} runs over {in_use} frames in batches in use"
    );
    assert_eq!(owed, stored, "what the cut found owed");
}

/// A cut lists the pages it finds owed in its set of pages seen as it walks the frames, a run at a
/// time, so no call copies every page owed into the set at once (the auditor's P2-CP-U1; mutant:
/// the owed list copied whole into the set at the swap table's first shard).
#[test]
fn a_cut_lists_the_pages_owed_a_run_at_a_time() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-seen", 8 * MIN_BUDGET);
    let mut stored = Vec::new();
    for v in 0..CUT_RUN as u64 + 2 {
        let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, v);
        g.changed();
        drop(g);
        stored.push(pid);
    }
    let mut scan = CutScan::default();
    let mut most = 0;
    loop {
        let before = scan.seen.len();
        let done = match pool.cut_run(1, &mut scan) {
            IOResult::Done(done) => done,
            IOResult::IO(_) => panic!("a cut with no holder waited"),
        };
        // The set is handed to the pool once the swap table is listed, and reads empty then.
        most = most.max(scan.seen.len().saturating_sub(before));
        if done {
            break;
        }
    }
    pool.end_cut(1);
    let owed = scan.owed.len();
    drop(pool);
    assert!(owed > CUT_RUN, "{owed} pages owed: a cut of one run");
    assert!(
        most <= CUT_RUN,
        "a call put {most} pages in the set, of {owed} owed"
    );
}

/// A cut lists the swap table a run of pages a call, a shard's run from where its last ended:
/// every swapped page owed once and no call listing more than a run (the auditor's P2-CP-U1;
/// mutants: a shard listed whole, a shard left after its first run).
#[cfg(not(shuttle))]
#[test]
fn a_cut_lists_the_swap_table_a_run_a_call_and_every_swapped_page_once() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/cut-swap", MIN_BUDGET);
    // Enough swapped pages that shards hold more than a run: none of them resident, the slots
    // named never taken.
    let count = (pool.store.swap_shards() * (2 * CUT_RUN + 8)) as u64;
    let pages: Vec<PageId> = (0..count)
        .map(|i| PageId::new(class(0), (1 << 20) + i))
        .collect();
    for (i, &pid) in pages.iter().enumerate() {
        pool.store.set_swap(pid, (1 << 30) + i as u64, u64::MAX);
    }
    let fullest = (0..pool.store.swap_shards())
        .map(|i| pool.store.swap_shard_pages(i, None, usize::MAX).len())
        .max()
        .unwrap_or(0);
    const N: u64 = 3;
    let mut scan = CutScan::default();
    let (mut most, mut calls, mut done) = (0, 0, false);
    while calls < 1 << 20 {
        calls += 1;
        let listed = scan.swapped.len();
        match pool.cut_run(N, &mut scan) {
            IOResult::Done(true) => {
                done = true;
                break;
            }
            IOResult::Done(false) => {}
            IOResult::IO(IOCompletions(c)) => {
                while !c.finished() {
                    io.step().unwrap();
                }
            }
        }
        most = most.max(scan.swapped.len().saturating_sub(listed));
    }
    let owed = pool.cut_owed.lock().clone();
    let listed_twice = scan.owed.len() - scan.owed.iter().collect::<HashSet<_>>().len();
    pool.end_cut(N);
    for &pid in &pages {
        pool.store.drop_swap(pid);
    }
    drop(pool);
    assert!(
        fullest > CUT_RUN,
        "no shard holds more than a run: {fullest}"
    );
    assert!(done, "the cut did not end in {calls} calls");
    assert!(most <= CUT_RUN, "a call listed {most} swapped pages");
    assert_eq!(listed_twice, 0, "pages owed twice");
    let missed: Vec<_> = pages.iter().filter(|pid| !owed.contains(pid)).collect();
    assert!(missed.is_empty(), "{} swapped pages not owed", missed.len());
}

/// The end of a cut runs in steps: the cut stopped first, then the copies it kept dropped a run a
/// call, each giving its room back, then the lend batches swept a run a call, the room told on the
/// last (the auditor's P2-CP-U1; mutants: every copy dropped in one call, every batch swept in one
/// call).
#[cfg(not(shuttle))]
#[test]
fn a_cuts_end_drops_its_copies_and_sweeps_its_batches_a_run_a_call() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    // More lend batches past the core than a run sweeps.
    let budget = MIN_BUDGET + (CUT_RUN + 2) * memory::BATCH;
    let pool = open_on(&io, "/db/cut-end", budget);
    const N: u64 = 7;
    let copies = 2 * CUT_RUN + 3;
    let size = class(0).size();
    pool.memory.cutting.store(N, Ordering::SeqCst);
    let mut charged = 0;
    for i in 0..copies {
        if !pool.room.charge(size) {
            break;
        }
        charged += 1;
        let block = crate::storage::atoll::aligned::AlignedBlock::zeroed(size);
        let pid = PageId::new(class(0), (1 << 20) + i as u64);
        pool.kept
            .lock()
            .insert(pid, KeptCopy::charged(block, &pool.room));
    }
    let used = pool.room.stats().used;
    let swept = pool.memory.batches - memory::CORE;
    let mut at = EndCut::default();
    let (mut calls, mut ended, mut stopped_first) = (Vec::new(), false, None);
    while calls.len() < 1 << 16 {
        ended = pool.end_cut_run(N, &mut at);
        stopped_first.get_or_insert(pool.memory.cutting.load(Ordering::SeqCst) == 0);
        calls.push((pool.kept.lock().len(), at.swept));
        if ended {
            break;
        }
    }
    let used_after = pool.room.stats().used;
    drop(pool);
    assert_eq!(charged, copies, "the room refused a copy");
    assert!(swept as usize > CUT_RUN, "{swept} batches swept");
    assert!(ended, "the cut did not end in {} calls", calls.len());
    assert_eq!(
        stopped_first,
        Some(true),
        "the first call left the cut running"
    );
    let mut left = copies;
    let mut from = memory::CORE;
    for &(kept, at) in &calls {
        assert!(
            left - kept <= CUT_RUN,
            "a call dropped {} copies",
            left - kept
        );
        left = kept;
        if let Some(to) = at {
            assert!(
                to - from <= CUT_RUN as u32,
                "a call swept {} batches",
                to - from
            );
            from = to;
        }
    }
    assert_eq!((left, from), (0, memory::CORE + swept));
    assert_eq!(
        used - used_after,
        copies * size,
        "the room the copies gave back"
    );
}

/// What [`HeldDataIO`] holds: once armed with a path prefix, every read and write made to a file
/// under it is carried out on the memory it wraps at once, then accepted and kept, its callback not
/// run, until the test delivers it and then lets it go: a backend that delivers a request before it
/// retires it.
#[derive(Default)]
struct HeldRequests {
    prefix: crate::storage::atoll::locks::Mutex<Option<String>>,
    queue: crate::storage::atoll::locks::Mutex<Vec<HeldRequest>>,
}

#[derive(Clone)]
struct HeldRequest {
    c: Completion,
    /// What its delivery reports: the bytes read or written.
    result: i32,
    /// Where the memory it reads into or writes from begins.
    memory: usize,
}

impl HeldRequests {
    fn hold(&self, c: Completion, result: i32, memory: usize) -> Result<Completion> {
        c.accept();
        self.queue.lock().push(HeldRequest {
            c: c.clone(),
            result,
            memory,
        });
        Ok(c)
    }

    /// The requests held so far, oldest first.
    fn all(&self) -> Vec<HeldRequest> {
        self.queue.lock().clone()
    }

    /// The requests a backend still owns over the memory beginning at `memory`.
    fn owned_over(&self, memory: usize) -> usize {
        self.all()
            .iter()
            .filter(|r| r.memory == memory && r.c.is_owned())
            .count()
    }

    /// Delivers every request held not yet delivered, and lets every one go.
    fn release_all(&self, delivered: &mut std::collections::HashSet<usize>) {
        for (i, r) in self.all().into_iter().enumerate() {
            if delivered.insert(i) {
                r.c.complete(r.result);
            }
            if r.c.is_owned() {
                r.c.retire();
            }
        }
    }
}

/// A [`MemoryIO`] whose files under a prefix, once named, hold their reads and writes
/// ([`HeldRequests`]).
struct HeldDataIO {
    inner: Arc<crate::io::MemoryIO>,
    held: Arc<HeldRequests>,
}

impl crate::io::Clock for HeldDataIO {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for HeldDataIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn crate::io::File>> {
        Ok(Arc::new(HeldDataFile {
            file: self.inner.open_file(path, flags, direct)?,
            path: path.to_string(),
            held: self.held.clone(),
        }))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn sync_directory(&self, path: &str) -> Result<()> {
        self.inner.sync_directory(path)
    }
    fn rename_file(&self, from: &str, to: &str) -> Result<bool> {
        self.inner.rename_file(from, to)
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

struct HeldDataFile {
    file: Arc<dyn crate::io::File>,
    path: String,
    held: Arc<HeldRequests>,
}

impl HeldDataFile {
    fn holding(&self) -> bool {
        self.held
            .prefix
            .lock()
            .as_deref()
            .is_some_and(|prefix| self.path.starts_with(prefix))
    }
}

impl crate::io::File for HeldDataFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }
    fn pread(&self, at: u64, c: Completion) -> Result<Completion> {
        if !self.holding() {
            return self.file.pread(at, c);
        }
        let into = c.as_read().buf_arc();
        let memory = into.as_ptr() as usize;
        let read = Arc::new(std::sync::atomic::AtomicI32::new(-1));
        let n = read.clone();
        // The memory it wraps reads at once, into the request's own memory.
        drop(self.file.pread(
            at,
            Completion::new_read(into, move |res| {
                n.store(
                    res.map_or(-1, |(_, n)| n),
                    std::sync::atomic::Ordering::Relaxed,
                );
                None
            }),
        )?);
        self.held
            .hold(c, read.load(std::sync::atomic::Ordering::Relaxed), memory)
    }
    fn pwrite(&self, at: u64, bytes: Arc<crate::io::Buffer>, c: Completion) -> Result<Completion> {
        if !self.holding() {
            return self.file.pwrite(at, bytes, c);
        }
        let (memory, len) = (bytes.as_ptr() as usize, bytes.len() as i32);
        c.keep_write_buffer_alive(bytes.clone());
        drop(self.file.pwrite(at, bytes, Completion::new_write(|_| {}))?);
        self.held.hold(c, len, memory)
    }
    fn sync(&self, c: Completion, how: crate::io::FileSyncType) -> Result<Completion> {
        self.file.sync(c, how)
    }
    fn size(&self) -> Result<u64> {
        self.file.size()
    }
    fn truncate(&self, n: u64, c: Completion) -> Result<Completion> {
        self.file.truncate(n, c)
    }
}

/// A frame's record holds its load's and its write-out's request until the backend retires it,
/// not until its callback runs (DESIGN.md 22.18, a frame; the auditor's frame-record schedule):
/// a page loaded and delivered, its read still owned, is not evicted; a frame its write-out's
/// landing freed, the write still owned, is given to no claim, a load of another page taking
/// another frame; a cancelled read's frame the same; each given back once its request retires,
/// and never two requests outstanding over one frame's memory (mutant: the record let go at
/// delivery).
#[test]
fn a_frames_record_holds_its_load_and_its_write_out_until_each_retires() {
    let held = Arc::new(HeldRequests::default());
    let io: Arc<dyn IO> = Arc::new(HeldDataIO {
        inner: Arc::new(crate::io::MemoryIO::new()),
        held: held.clone(),
    });
    let base = "/db/frame-record";
    let pool = open_on(&io, base, MIN_BUDGET);
    let latch = |frame: u32| FrameLatch {
        frames: pool.frames.clone(),
        frame,
    };
    let memory_of =
        |frame: u32| pool.memory.region.buffer(frame, class(0).size()).as_ptr() as usize;
    // Two pages written out of their frames, nothing held: each in its copy alone.
    let mut pages = Vec::new();
    for version in 1..=2 {
        let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
        let (pid, frame) = (g.pid(), g.frame);
        stamp(&mut g.page_mut(), pid, version);
        g.changed();
        drop(g);
        let out = pool.start_write_out(
            pool.latch_to_evict(class(0), frame, || latch(frame))
                .unwrap(),
        );
        pool.write_out(class(0), vec![out]);
        drive(&io, || pool.settle_writes()).unwrap();
        pages.push(pid);
    }
    let (p, q) = (pages[0], pages[1]);
    *held.prefix.lock() = Some(format!("{base}.c0."));
    let mut delivered = std::collections::HashSet::new();
    let mut most_over_one_frame = 0;
    let watch = |frames: &[u32], most: &mut usize| {
        for &f in frames {
            *most = (*most).max(held.owned_over(memory_of(f)));
        }
    };

    // A load whose read is delivered and still owned: resident, and not evicted until it retires.
    let first = matches!(pool.fix_optimistic(p, None).unwrap(), IOResult::IO(_));
    let first_requests = held.all().len();
    let f = pool.table.find(&pool.frames.headers, p.raw()).unwrap();
    let read = held.all()[0].clone();
    read.c.complete(read.result);
    delivered.insert(0);
    watch(&[f], &mut most_over_one_frame);
    let evicted_while_owned = pool.evict_clean(p);
    // Fixed again: resident, or, evicted, loaded again into whichever frame a claim takes.
    let again = pool.fix_optimistic(p, None).unwrap();
    let resident_while_owned = matches!(again, IOResult::Done(_));
    drop(again);
    watch(&[f], &mut most_over_one_frame);
    held.release_all(&mut delivered);
    let read_back = match pool.fix_optimistic(p, None).unwrap() {
        IOResult::Done(g) => g.read(|page| stamped(page, p)),
        IOResult::IO(_) => None,
    };
    let f = pool.table.find(&pool.frames.headers, p.raw()).unwrap();

    // The page written out of its frame, the write delivered and still owned: the frame, freed by
    // the landing, is given to no claim, and a load of another page takes another frame.
    let mut g = drive(&io, || pool.fix_exclusive(p, None)).unwrap();
    stamp(&mut g.page_mut(), p, 3);
    g.changed();
    drop(g);
    let writes_before = held.all().len();
    let out = pool.start_write_out(pool.latch_to_evict(class(0), f, || latch(f)).unwrap());
    pool.write_out(class(0), vec![out]);
    let _ = pool.settle_writes();
    let write = held.all().get(writes_before).cloned();
    let write_held = write.is_some();
    if let Some(w) = &write {
        w.c.complete(w.result);
        delivered.insert(writes_before);
    }
    let settled = matches!(pool.settle_writes(), Ok(IOResult::Done(())));
    let freed_while_written = pool.header(f).status() == FREE;
    let free_while_written = pool.memory.free_frames(class(0)).contains(&f);
    let census_while_written = pool.taken_frames_census();
    let _ = pool.fix_optimistic(q, None).unwrap();
    let q_frame = pool.table.find(&pool.frames.headers, q.raw());
    watch(&[f], &mut most_over_one_frame);
    if let Some(w) = &write {
        w.c.retire();
    }
    let free_once_written = pool.memory.free_frames(class(0)).contains(&f);
    held.release_all(&mut delivered);

    // A read cancelled while its request is still owned: its frame is freed and given to no claim
    // until the request retires.
    let evicted_q = pool.evict_clean(q);
    let reads_before = held.all().len();
    let _ = pool.fix_optimistic(q, None).unwrap();
    let g_frame = pool.table.find(&pool.frames.headers, q.raw());
    let cancelled = held.all().get(reads_before).cloned();
    if let Some(r) = &cancelled {
        r.c.abort();
        delivered.insert(reads_before);
    }
    let free_while_cancelled =
        g_frame.is_some_and(|g| pool.memory.free_frames(class(0)).contains(&g));
    let census_while_cancelled = pool.taken_frames_census();
    if let Some(g) = g_frame {
        watch(&[g], &mut most_over_one_frame);
    }
    if let Some(r) = &cancelled {
        r.c.retire();
    }
    let free_once_cancelled =
        g_frame.is_some_and(|g| pool.memory.free_frames(class(0)).contains(&g));

    // Every request delivered and let go before the assertions, and nothing held from here.
    held.release_all(&mut delivered);
    *held.prefix.lock() = None;
    let q_back = drive(&io, || pool.fix_optimistic(q, None))
        .unwrap()
        .read(|page| stamped(page, q));
    let failure = pool.failed().err();
    drop(pool);
    assert!(first, "the first fix found the page resident");
    assert_eq!(first_requests, 1, "the first fix held other than its read");
    assert!(
        !evicted_while_owned,
        "a page evicted while its read was owned"
    );
    assert!(
        resident_while_owned,
        "the page not resident while its read was owned"
    );
    assert_eq!(read_back, Some(Some(1)));
    assert!(
        write_held && settled,
        "the write-out not held, or not settled once delivered"
    );
    assert!(
        freed_while_written,
        "the write-out's landing did not free its frame"
    );
    assert!(
        !free_while_written,
        "a frame given to the free frames while its write-out was owned"
    );
    assert_ne!(
        q_frame,
        Some(f),
        "a load took a frame whose write-out was owned"
    );
    assert!(
        free_once_written,
        "the frame not given back once its write-out retired"
    );
    assert!(evicted_q && g_frame.is_some() && cancelled.is_some());
    assert!(
        !free_while_cancelled,
        "a frame given to the free frames while its cancelled read was owned"
    );
    assert!(
        free_once_cancelled,
        "the frame not given back once its cancelled read retired"
    );
    assert_eq!(
        most_over_one_frame, 1,
        "requests outstanding at once over one frame's memory"
    );
    assert_eq!(q_back, Some(Some(2)));
    assert!(failure.is_none(), "the pool failed: {failure:?}");
    assert!(
        census_while_written.is_empty(),
        "the census while the write-out's request was owned: {census_while_written:#?}"
    );
    assert!(
        census_while_cancelled.is_empty(),
        "the census while the cancelled read's request was owned: {census_while_cancelled:#?}"
    );
}

/// Counts the wakes a claim's wait gets.
#[derive(Default)]
struct WakeCount(std::sync::atomic::AtomicUsize);

impl std::task::Wake for WakeCount {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Two claims that pass over a frame for its record, its load delivered and its request still
/// owned, each mark the record (the second over the first's mark) and wait; the request's
/// retirement wakes both, once each (the auditor's #4556; mutant: the retirement telling no one).
#[test]
fn claims_passing_over_a_frame_for_its_record_are_woken_by_its_retirement() {
    let held = Arc::new(HeldRequests::default());
    let io: Arc<dyn IO> = Arc::new(HeldDataIO {
        inner: Arc::new(crate::io::MemoryIO::new()),
        held: held.clone(),
    });
    let base = "/db/frame-record-wake";
    let pool = open_on(&io, base, MIN_BUDGET);
    let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (g.pid(), g.frame);
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    drop(g);
    let out = pool.start_write_out(
        pool.latch_to_evict(class(0), frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .unwrap(),
    );
    pool.write_out(class(0), vec![out]);
    drive(&io, || pool.settle_writes()).unwrap();
    *held.prefix.lock() = Some(format!("{base}.c0."));
    let loading = matches!(pool.fix_optimistic(pid, None).unwrap(), IOResult::IO(_));
    let frame = pool.table.find(&pool.frames.headers, pid.raw()).unwrap();
    let read = held.all().first().cloned();
    let mut delivered = std::collections::HashSet::new();
    if let Some(r) = &read {
        r.c.complete(r.result);
        delivered.insert(0);
    }
    let counts = [
        std::sync::Arc::new(WakeCount::default()),
        std::sync::Arc::new(WakeCount::default()),
    ];
    let mut waits = Vec::new();
    let mut looks = Vec::new();
    for count in &counts {
        let wait = pool.released_wait();
        wait.set_waker(&std::task::Waker::from(count.clone()));
        let mut outgoing = Vec::new();
        let mut room = Room::default();
        let took = pool.evict_or_write(class(0), frame, &mut outgoing, &mut room);
        looks.push((took, room.marked, room.changed, outgoing.len()));
        waits.push(wait);
    }
    let before: Vec<_> = waits.iter().map(Completion::finished).collect();
    if let Some(r) = &read {
        r.c.retire();
    }
    let after: Vec<_> = waits.iter().map(Completion::finished).collect();
    let wakes: Vec<_> = counts
        .iter()
        .map(|c| c.0.load(std::sync::atomic::Ordering::Relaxed))
        .collect();
    held.release_all(&mut delivered);
    *held.prefix.lock() = None;
    let failure = pool.failed().err();
    drop((waits, read, pool));
    assert!(loading, "the page was resident before its load");
    assert_eq!(
        looks,
        vec![(false, true, false, 0); 2],
        "each claim passed over the frame and marked its record"
    );
    assert_eq!(
        before,
        vec![false, false],
        "a wait finished before the retirement"
    );
    assert_eq!(
        after,
        vec![true, true],
        "the retirement left a claim waiting"
    );
    assert_eq!(wakes, vec![1, 1], "the claims' wakes");
    assert!(failure.is_none(), "the pool failed: {failure:?}");
}

/// A frame its write-out's landing freed, the write still owned, goes to no other size class: a
/// 2 MiB claim that only the frame's batch can serve waits, and once the write retires it takes
/// that batch, the frame's memory among its own (the auditor's #4556; mutant: the frame given to
/// the free frames at once).
#[test]
fn a_frame_freed_under_its_write_out_goes_to_another_class_only_once_the_write_retires() {
    let held = Arc::new(HeldRequests::default());
    let io: Arc<dyn IO> = Arc::new(HeldDataIO {
        inner: Arc::new(crate::io::MemoryIO::new()),
        held: held.clone(),
    });
    let base = "/db/frame-record-class";
    let pool = open_on(&io, base, MIN_BUDGET);
    let big = class(memory::BATCH_CLASS);
    let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, frame) = (g.pid(), g.frame);
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    drop(g);
    // 2 MiB pages kept resident over every other batch, and the class-0 page with them, so that
    // filling the pool writes nothing out: a further one needs the class-0 batch.
    let mut bigs = vec![pid.raw()];
    pool.protect(&bigs);
    loop {
        match pool.allocate(big).unwrap() {
            IOResult::Done(mut b) => {
                let p = b.pid();
                stamp(&mut b.page_mut(), p, 1);
                b.changed();
                bigs.push(p.raw());
                drop(b);
                pool.protect(&bigs);
            }
            IOResult::IO(_) => break,
        }
    }
    // Still waiting: the class-0 batch holds a resident page.
    *held.prefix.lock() = Some(format!("{base}.c0."));
    let out = pool.start_write_out(
        pool.latch_to_evict(class(0), frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .unwrap(),
    );
    pool.write_out(class(0), vec![out]);
    let _ = pool.settle_writes().unwrap();
    let write = held.all().first().cloned();
    let mut delivered = std::collections::HashSet::new();
    if let Some(w) = &write {
        w.c.complete(w.result);
        delivered.insert(0);
    }
    let settled = matches!(pool.settle_writes(), Ok(IOResult::Done(())));
    let freed =
        pool.header(frame).status() == FREE && !pool.memory.free_frames(class(0)).contains(&frame);
    let waiting = match pool.allocate(big).unwrap() {
        IOResult::IO(IOCompletions(c)) => Some(c),
        IOResult::Done(_) => None,
    };
    let finished_before = waiting.as_ref().map(Completion::finished);
    if let Some(w) = &write {
        w.c.retire();
    }
    let finished_after = waiting.as_ref().map(Completion::finished);
    let memory_of = |f: u32, size: usize| {
        let at = pool.memory.region.buffer(f, size).as_ptr() as usize;
        at..at + size
    };
    let freed_at = memory_of(frame, class(0).size()).start;
    let got = drive(&io, || pool.allocate(big)).unwrap();
    let reused = memory_of(got.frame, big.size()).contains(&freed_at);
    drop(got);
    held.release_all(&mut delivered);
    *held.prefix.lock() = None;
    pool.protect(&[]);
    let failure = pool.failed().err();
    drop((waiting, write, pool));
    assert!(bigs.len() > 1, "no 2 MiB page fit beside the class-0 batch");
    assert!(
        settled && freed,
        "the write-out's landing did not free its frame, deferred"
    );
    assert_eq!(
        finished_before,
        Some(false),
        "a 2 MiB claim was served while the frame's write-out was owned"
    );
    assert_eq!(
        finished_after,
        Some(true),
        "the write's retirement left the 2 MiB claim waiting"
    );
    assert!(
        reused,
        "the 2 MiB page took memory other than the freed frame's batch"
    );
    assert!(failure.is_none(), "the pool failed: {failure:?}");
}

/// A source whose read hands no request over: it fills the page itself, or gives up for a moment
/// (an IO), the frame going back free as its read is dropped.
struct AnnouncingNothing(bool);
struct AnnouncingNothingLocated(bool);

impl source::PageSource for AnnouncingNothing {
    fn locate(&self, _pid: PageId) -> Result<IOResult<Box<dyn source::Located>>> {
        Ok(IOResult::Done(Box::new(AnnouncingNothingLocated(self.0))))
    }
}

impl source::Located for AnnouncingNothingLocated {
    fn read(
        self: Box<Self>,
        into: Arc<crate::io::Buffer>,
        done: source::ReadDone,
        _submitting: source::Submitting<'_>,
    ) -> Result<IOResult<Option<Completion>>> {
        if self.0 {
            into.as_mut_slice().fill(0);
            done(Ok(()));
            Ok(IOResult::Done(None))
        } else {
            Ok(IOResult::IO(IOCompletions(Completion::new_yield())))
        }
    }
}

/// A load whose read announced no request, the read returned and its frame let go, meeting the
/// frame given to another page whose write-out's request is held, before the load judges its
/// read: whether the setup reached it, whether that other request's record was still held once
/// the load returned, and whether the load's judgement panicked, caught so that every request is
/// let go and the pool dropped before a test asserts (the auditor's #4562 and #4568).
fn a_load_judging_its_read_after_its_frame_changed_hands(fills: bool) -> (bool, bool, bool) {
    let held = Arc::new(HeldRequests::default());
    let io: Arc<dyn IO> = Arc::new(HeldDataIO {
        inner: Arc::new(crate::io::MemoryIO::new()),
        held: held.clone(),
    });
    let base = if fills {
        "/db/frame-record-fill"
    } else {
        "/db/frame-record-gave-up"
    };
    let store = PageStore::open(
        crate::storage::atoll::requests::counted_for_test(io.clone()),
        base,
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        0,
    );
    let pool = BufferPool::new(
        BufferConfig::with_budget(MIN_BUDGET),
        Arc::new(AnnouncingNothing(fills)),
        store,
    )
    .unwrap();
    let big = class(memory::BATCH_CLASS);
    // 2 MiB pages held resident over every batch but one: the load and the page after it take
    // that one's frame.
    let mut kept = Vec::new();
    loop {
        match pool.allocate(big).unwrap() {
            IOResult::Done(mut b) => {
                let p = b.pid();
                stamp(&mut b.page_mut(), p, 1);
                b.changed();
                kept.push(p.raw());
                drop(b);
                pool.protect(&kept);
            }
            IOResult::IO(_) => break,
        }
    }
    let last = kept.pop();
    if let Some(p) = last {
        // The last 2 MiB page given back, so its batch is the one left.
        let f = pool.table.find(&pool.frames.headers, p).unwrap();
        pool.protect(&kept);
        let hold = pool.latch_to_evict(big, f, || pool.frame_ref(f)).unwrap();
        pool.table.remove(&pool.frames.headers, p, f);
        pool.free_frame(big, f, hold);
    }
    let other = std::rc::Rc::new(std::cell::Cell::new(None));
    {
        let (pool, io, held, other) = (pool.clone(), io.clone(), held.clone(), other.clone());
        crate::race::at("buffer.read_returned", move || {
            let z = PageId::new(big, 1 << 20);
            if fills {
                pool.evict_clean(z);
            }
            let mut y = drive(&io, || pool.allocate(big)).unwrap();
            let (ypid, yframe) = (y.pid(), y.frame);
            stamp(&mut y.page_mut(), ypid, 1);
            y.changed();
            drop(y);
            *held.prefix.lock() = Some(format!("{base}.c{}.", big.index()));
            let out = pool.start_write_out(
                pool.latch_to_evict(big, yframe, || FrameLatch {
                    frames: pool.frames.clone(),
                    frame: yframe,
                })
                .unwrap(),
            );
            pool.write_out(big, vec![out]);
            let _ = pool.settle_writes();
            other.set(Some((yframe, pool.header(yframe).record_held())));
        });
    }
    let panicked = crate::storage::atoll::testing::catch_test_panic(|| {
        let _ = pool
            .fix_optimistic(PageId::new(big, 1 << 20), None)
            .unwrap();
    })
    .is_err();
    let reached = !crate::race::pending("buffer.read_returned")
        && other.get().is_some_and(|(_, held_then)| held_then);
    let still_held = other
        .get()
        .is_some_and(|(f, _)| pool.header(f).record_held());
    // A record let go before its request retired is held again for that retirement, which the
    // cleanup still owes it: the cleanup then finds the record it lets go, and the assertion
    // reports rather than the retirement ending the process.
    if let Some((f, _)) = other.get().filter(|_| !still_held) {
        // The cleanup's own hold: no request of the pool's is handed this record.
        let _ = pool.header(f).hold_record(f, pool.memory.class_of(f));
    }
    // Every request delivered and let go before the assertions, and nothing held from here.
    let mut delivered = std::collections::HashSet::new();
    held.release_all(&mut delivered);
    *held.prefix.lock() = None;
    let _ = pool.settle_writes();
    pool.protect(&[]);
    drop(pool);
    (reached, still_held, panicked)
}

/// A load whose read gave up with no request announced, its frame freed and given to another
/// page whose write-out is held before the load judges its read: the load lets no record go,
/// the other request's held as long as it is owned (the auditor's #4562; mutant: the record let
/// go on the frame's header, as 8a3787a72 judged it).
#[test]
fn a_load_whose_read_gave_up_lets_go_no_record_of_its_frames_next_page() {
    let (reached, still_held, panicked) =
        a_load_judging_its_read_after_its_frame_changed_hands(false);
    assert!(
        reached,
        "the frame was not given to another page's held write-out"
    );
    assert!(!panicked, "the load's judgement panicked");
    assert!(
        still_held,
        "the load let go the record of a request on its frame's next page"
    );
}

/// A load whose read filled its page itself, the page evicted and its frame given to another page
/// whose write-out is held before the load judges its read: the load judges its own read, none
/// announced, and the other request's record stays held (the auditor's #4562).
#[test]
fn a_load_whose_read_filled_its_page_judges_its_own_read_not_its_frames_next_page() {
    let (reached, still_held, panicked) =
        a_load_judging_its_read_after_its_frame_changed_hands(true);
    assert!(
        reached,
        "the frame was not given to another page's held write-out"
    );
    assert!(
        !panicked,
        "the load judged the next page's record as its own read's: its judgement panicked"
    );
    assert!(
        still_held,
        "the record of a request on the frame's next page was let go"
    );
}

// ------------------------------------------------- a load's group (ASYNC1 D2e §2.2b)

/// A load's group (ASYNC1 D2e §2.2b), natively: each test's body in a child of its own.
/// The census names a frame taken from its batch and put in no place, and a resident page's
/// frame also named kept for the checkpoint's demand, in two; with the frame given back and the
/// entry cleared it names nothing, and the report of a pool with no batch leaving says so
/// (SHORT-HOLD1).
#[test]
fn the_census_names_a_frame_taken_and_put_in_no_place_and_one_put_in_two() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let pool = open_on(&io, "/db/census-places", MIN_BUDGET);
    let page = drive(&io, || pool.allocate(class(0))).unwrap();
    let (pid, resident) = (page.pid(), page.frame);
    drop(page);
    let before = pool.taken_frames_census();
    let report = pool.park_report();
    let lost = pool.memory.take(class(0)).expect("a free 4 KiB frame");
    let in_no_place = pool.taken_frames_census();
    pool.memory.put(class(0), lost);
    let entry = pool
        .keepers
        .add(resident, pid.raw(), super::keepers::Keeper::Checkpoint);
    let in_two = pool.taken_frames_census();
    if let Some(slot) = entry {
        pool.keepers.remove(slot);
    }
    let after = pool.taken_frames_census();
    drop(pool);
    assert!(
        before.is_empty(),
        "the census of a pool holding one page: {before:#?}"
    );
    assert!(
        report.contains("leaving batches: none\n"),
        "the report of a pool with no batch leaving: {report}"
    );
    assert!(
        in_no_place.len() == 1
            && in_no_place[0].starts_with(&format!(
                "frame {lost} of class 0: taken and in no place [];"
            )),
        "the census did not name frame {lost}, taken and put in no place, alone: {in_no_place:#?}"
    );
    assert!(entry.is_some(), "the registry of keepers had no free slot");
    assert!(
        in_two.len() == 1
            && in_two[0].starts_with(&format!(
                "frame {resident} of class 0: taken and in more than one place [kept for the \
                 checkpoint's demand, the page table];"
            )),
        "the census did not name frame {resident}, in the page table and kept for the \
         checkpoint's demand, alone: {in_two:#?}"
    );
    assert!(
        after.is_empty(),
        "the census once the frame is back and the entry cleared: {after:#?}"
    );
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
mod group {
    use super::super::group as group_word;
    use super::*;

    /// A pool over a source whose reads land only when the test lands them, each request leaving the
    /// backend as soon as it is submitted.
    fn held_reads_pool(base: &str) -> (Arc<BufferPool>, PendingReads) {
        let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
        let store = PageStore::open(
            crate::storage::atoll::requests::counted_for_test(io),
            base,
            StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
            0,
        );
        let reads: PendingReads = Arc::new(crate::storage::atoll::locks::Mutex::new(Vec::new()));
        let pool = BufferPool::new(
            BufferConfig::with_budget(MIN_BUDGET),
            Arc::new(SubmittingSource(reads.clone())),
            store,
        )
        .unwrap();
        (pool, reads)
    }

    /// Lands every read the source holds with `result`: how many.
    fn land(reads: &PendingReads, result: std::result::Result<(), CompletionError>) -> usize {
        let landed = std::mem::take(&mut *reads.lock());
        let n = landed.len();
        for (buffer, done) in landed {
            buffer.as_mut_slice().fill(0);
            done(result.clone());
        }
        n
    }

    /// One call of a fix of `pid` as `owner`'s, the page let go at once: whether it was done.
    fn fixed_as(owner: &mut Owner, pool: &Arc<BufferPool>, pid: PageId) -> bool {
        let out = owner.call(|| {
            Ok(match pool.fix_optimistic(pid, None)? {
                IOResult::Done(_) => IOResult::Done(()),
                IOResult::IO(io) => IOResult::IO(io),
            })
        });
        matches!(out, Ok(IOResult::Done(())))
    }

    /// A fix of `pid` outside any owner, the page let go at once: whether it was done.
    fn fixed_unowned(pool: &Arc<BufferPool>, pid: PageId) -> bool {
        matches!(pool.fix_optimistic(pid, None), Ok(IOResult::Done(_)))
    }

    fn frame_of(pool: &BufferPool, pid: PageId) -> u32 {
        pool.table
            .find(&pool.frames.headers, pid.raw())
            .expect("the page's frame")
    }

    /// How many times `frame` is among `class`'s free frames.
    fn free_times(pool: &BufferPool, class: SizeClass, frame: u32) -> usize {
        pool.memory
            .free_frames(class)
            .into_iter()
            .filter(|&f| f == frame)
            .count()
    }

    /// Two owners on one load (#5188): A's miss loads the page, and B's fix joins the load while its
    /// frame is loading and latched. Once the read lands and its request retires, A's next call fixes
    /// the page and lets its reference go; an eviction forced then passes the frame over for B's
    /// reference, and B's next call fixes the page without reading it again. Both let go, the page
    /// is evictable (mutant `joiner-no-keep`, a joiner taking no reference).
    #[test]
    fn a_joiner_keeps_the_page_it_waited_for_until_its_next_call() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_joiner_keeps_the_page_it_waited_for_until_its_next_call"
            ),
            || {
                let (pool, reads) = held_reads_pool("/db/group-two");
                let pid = PageId::new(class(0), 1);
                let (mut a, mut b) = (Owner::new(), Owner::new());
                assert!(!fixed_as(&mut a, &pool, pid), "A's miss waits for its load");
                assert!(!fixed_as(&mut b, &pool, pid), "B waits for A's load");
                let frame = frame_of(&pool, pid);
                let kept = (a.kept_frame(), b.kept_frame());
                assert_eq!(land(&reads, Ok(())), 1, "the reads of the page");
                assert!(fixed_as(&mut a, &pool, pid), "A fixes the page");
                assert_eq!(a.kept_frame(), None, "A's reference after its call");
                assert!(
                    !pool.evict_clean(pid),
                    "B's page was evicted while B waited for it"
                );
                assert!(fixed_as(&mut b, &pool, pid), "B fixes the page");
                assert_eq!(reads.lock().len(), 0, "B's call read the page again");
                assert_eq!(b.kept_frame(), None, "B's reference after its call");
                assert!(
                    pool.evict_clean(pid),
                    "the page stayed kept after both let go"
                );
                assert!(pool.group(frame).released(), "the group after both let go");
                assert_eq!(
                    kept,
                    (Some(frame), Some(frame)),
                    "(A, B) keep the loading frame"
                );
            },
        );
    }

    /// A joiner whose owner is dropped while the load is held lets its reference go then: the
    /// loader's guard and its own reference keep the frame, and once the loader lets go after its
    /// next call the frame is evictable, one read made.
    #[test]
    fn a_joiner_dropped_while_the_load_is_held_lets_its_reference_go() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_joiner_dropped_while_the_load_is_held_lets_its_reference_go"
            ),
            || {
                let (pool, reads) = held_reads_pool("/db/group-dropped");
                let pid = PageId::new(class(0), 1);
                let (mut a, mut b) = (Owner::new(), Owner::new());
                assert!(!fixed_as(&mut a, &pool, pid));
                assert!(!fixed_as(&mut b, &pool, pid));
                let frame = frame_of(&pool, pid);
                drop(b);
                assert!(
                    !pool.group(frame).released() && pool.header(frame).record_keeps(),
                    "the guard and the loader's reference keep the frame"
                );
                assert_eq!(land(&reads, Ok(())), 1);
                assert!(fixed_as(&mut a, &pool, pid), "A fixes the page");
                assert!(
                    pool.evict_clean(pid),
                    "the page stayed kept once both let go"
                );
                assert!(pool.group(frame).released());
            },
        );
    }

    /// A joiner's release on either side of the loader's close, which a loader with no owner makes
    /// with the joiner's the only other reference: before it, the close takes the word to zero and
    /// lets the group's keep go; just after its subtraction, under the loader's latch, the joiner's
    /// release does. Either way the keep goes once (a second would fail `ungroup`'s assert), never
    /// while the joiner holds its reference, and the page is evictable after.
    #[test]
    fn a_joiners_release_on_either_side_of_the_close_lets_the_group_go_once() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_joiners_release_on_either_side_of_the_close_lets_the_group_go_once"
            ),
            || {
                for after_the_close in [false, true] {
                    let (pool, reads) = held_reads_pool("/db/group-close");
                    let pid = PageId::new(class(0), 1);
                    assert!(!fixed_unowned(&pool, pid), "the load waits");
                    let mut b = Owner::new();
                    assert!(!fixed_as(&mut b, &pool, pid), "B joins the load");
                    let frame = frame_of(&pool, pid);
                    assert_eq!(b.kept_frame(), Some(frame));
                    let kept_at_release = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
                    if after_the_close {
                        let (pool, kept_at_release) = (pool.clone(), kept_at_release.clone());
                        crate::race::at("buffer.group_closed", move || {
                            *kept_at_release.lock() = Some(pool.header(frame).record_keeps());
                            drop(b);
                        });
                    } else {
                        *kept_at_release.lock() = Some(pool.header(frame).record_keeps());
                        drop(b);
                    }
                    assert_eq!(land(&reads, Ok(())), 1);
                    assert!(
                        !crate::race::pending("buffer.group_closed"),
                        "the landing never reached the close"
                    );
                    assert_eq!(
                        *kept_at_release.lock(),
                        Some(true),
                        "after the close {after_the_close}: the frame was not kept when B let go"
                    );
                    assert!(
                        !pool.header(frame).record_keeps(),
                        "after the close {after_the_close}: the group's keep stayed"
                    );
                    assert!(pool.group(frame).released());
                    assert!(pool.evict_clean(pid), "after the close {after_the_close}");
                }
            },
        );
    }

    /// A join refused: a loader with no owner, and B's fix paused just after its probe found the page
    /// loading. The load lands meanwhile, its close the group's last reference, so B's join finds the
    /// word at zero: it holds nothing, finishes with the page, and the page is evictable (mutant
    /// `join-at-zero`, an admission at zero: B's finished call lets that reference go as the group's
    /// last, and its release fails in the child at "a frame let go by a load's group that did not keep
    /// it", the keep long gone).
    #[test]
    fn a_join_refused_once_its_group_is_released_keeps_nothing() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_join_refused_once_its_group_is_released_keeps_nothing"
            ),
            || {
                let (pool, reads) = held_reads_pool("/db/group-refused");
                let pid = PageId::new(class(0), 1);
                assert!(!fixed_unowned(&pool, pid), "the load waits");
                let frame = frame_of(&pool, pid);
                {
                    let reads = reads.clone();
                    crate::race::at("buffer.join_probed", move || {
                        assert_eq!(land(&reads, Ok(())), 1);
                    });
                }
                let mut b = Owner::new();
                let done = fixed_as(&mut b, &pool, pid) || fixed_as(&mut b, &pool, pid);
                assert!(pool.group(frame).released(), "the group after its close");
                assert!(
                    !crate::race::pending("buffer.join_probed"),
                    "B's fix never found the load"
                );
                assert!(done, "B's fix of the landed page");
                assert_eq!(b.kept_frame(), None, "B kept a page its group had let go");
                assert!(
                    pool.evict_clean(pid),
                    "the page was kept after its group let go"
                );
            },
        );
    }

    /// A late admission: B's fix paused just after its probe found the page loading, and the load
    /// lands meanwhile with the loader A's reference still held. B's join finds the closed group with
    /// a reference left and is admitted, two references then, A's and B's, seen right after the join;
    /// B's fix then finds the page resident and finishes in that call, which lets its reference go.
    /// The page is read once, and kept until A lets go.
    #[test]
    fn a_late_join_is_admitted_to_a_closed_group_with_a_reference_left() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_late_join_is_admitted_to_a_closed_group_with_a_reference_left"
            ),
            || {
                let (pool, reads) = held_reads_pool("/db/group-late");
                let pid = PageId::new(class(0), 1);
                let mut a = Owner::new();
                assert!(!fixed_as(&mut a, &pool, pid), "A's load waits");
                let frame = frame_of(&pool, pid);
                {
                    let reads = reads.clone();
                    crate::race::at("buffer.join_probed", move || {
                        assert_eq!(land(&reads, Ok(())), 1);
                    });
                }
                let at_join = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
                {
                    let (pool, at_join) = (pool.clone(), at_join.clone());
                    crate::race::at("buffer.joined", move || {
                        *at_join.lock() = Some(pool.group(frame).references());
                    });
                }
                let mut b = Owner::new();
                let finished = fixed_as(&mut b, &pool, pid);
                assert!(
                    !crate::race::pending("buffer.joined"),
                    "B's fix never joined"
                );
                assert_eq!(
                    *at_join.lock(),
                    Some(2),
                    "the closed group's references right after B's join: A's and B's"
                );
                assert!(finished, "B's fix of the resident page");
                assert_eq!(
                    b.kept_frame(),
                    None,
                    "B's reference after its finished call"
                );
                assert_eq!(pool.group(frame).references(), 1, "A's reference left");
                assert!(!pool.evict_clean(pid), "the page evicted while A kept it");
                assert!(fixed_as(&mut a, &pool, pid), "A fixes the page");
                assert_eq!(reads.lock().len(), 0, "the page read again");
                assert!(pool.evict_clean(pid));
            },
        );
    }

    /// A failed load with a joiner: B's owner dropped before the read fails, and after. The unload
    /// closes the group and frees the frame, deferred while a reference is left; the last reference's
    /// release hands the frame to the free frames, exactly once, with nothing of its record or group
    /// left.
    #[test]
    fn a_failed_load_with_a_joiner_frees_its_frame_once_after_its_last_reference() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_failed_load_with_a_joiner_frees_its_frame_once_after_its_last_reference"
            ),
            || {
                for joiner_first in [true, false] {
                    let (pool, reads) = held_reads_pool("/db/group-failed");
                    let pid = PageId::new(class(0), 1);
                    let (mut a, mut b) = (Owner::new(), Owner::new());
                    assert!(!fixed_as(&mut a, &pool, pid));
                    assert!(!fixed_as(&mut b, &pool, pid));
                    let frame = frame_of(&pool, pid);
                    if joiner_first {
                        drop(b);
                        assert_eq!(land(&reads, Err(CompletionError::Aborted)), 1);
                    } else {
                        assert_eq!(land(&reads, Err(CompletionError::Aborted)), 1);
                        assert_eq!(
                            free_times(&pool, class(0), frame),
                            0,
                            "the failed load's frame was freed while B kept it"
                        );
                        drop(b);
                    }
                    assert_eq!(
                        free_times(&pool, class(0), frame),
                        0,
                        "joiner first {joiner_first}: the frame was freed while the loader kept it"
                    );
                    let census = pool.taken_frames_census();
                    assert!(
                        census.is_empty(),
                        "joiner first {joiner_first}: the census with the frame deferred for the \
                         loader's reference: {census:#?}"
                    );
                    drop(a);
                    assert_eq!(
                        free_times(&pool, class(0), frame),
                        1,
                        "joiner first {joiner_first}: the frame's frees once its references went"
                    );
                    assert!(!pool.header(frame).record_keeps());
                    assert!(pool.group(frame).released());
                }
            },
        );
    }

    /// Every frame of a class kept by an owner's reference after its load: a claim of the class
    /// passes each over and parks, the frames marked waited; one owner dropped tells the claim
    /// (mutant `keep-forever`, references never released: the claim is never told).
    #[test]
    fn an_owner_dropped_while_keeping_tells_a_claim_that_passed_its_frame_over() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::an_owner_dropped_while_keeping_tells_a_claim_that_passed_its_frame_over"
            ),
            || {
                let (pool, reads) = held_reads_pool("/db/group-claim");
                let c = class(9);
                let mut owners = Vec::new();
                for i in 0..pool.memory.batches {
                    let mut owner = Owner::new();
                    assert!(!fixed_as(&mut owner, &pool, PageId::new(c, i as u64 + 1)));
                    owners.push(owner);
                }
                assert_eq!(land(&reads, Ok(())), pool.memory.batches as usize);
                let parked = match pool.allocate(c).unwrap() {
                    IOResult::IO(IOCompletions(wait)) => wait,
                    IOResult::Done(_) => panic!("a frame its group keeps was evicted"),
                };
                assert!(!parked.finished(), "the claim parks behind the kept frames");
                let census = pool.taken_frames_census();
                assert!(
                    census.is_empty(),
                    "the census with every frame kept by its loader: {census:#?}"
                );
                drop(owners.pop());
                assert!(
                    parked.finished(),
                    "the claim waiting on a kept frame was not told of its release"
                );
                drop(parked);
                drop(owners);
            },
        );
    }

    /// A call that keeps a page it loaded and then comes short of memory in the same call (#5196): the
    /// read lands while its loader follows it, so the fix is done in the call and its owner keeps the
    /// page; the call's allocation then comes short. The owner lets its keeps go before it reserves,
    /// the current call's included, so the shortage waits holding nothing: the page's frame is
    /// evictable, and the operation's next calls, each handed a wait already finished, finish it
    /// (mutant `keep-current-on-shortage`).
    #[test]
    fn a_shortage_after_a_load_in_the_same_call_keeps_nothing() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_shortage_after_a_load_in_the_same_call_keeps_nothing"
            ),
            || {
                let (pool, reads) = held_reads_pool("/db/group-short");
                let cold = PageId::new(class(0), 1);
                {
                    let reads = reads.clone();
                    crate::race::at("buffer.loader_follows", move || {
                        assert_eq!(land(&reads, Ok(())), 1);
                    });
                }
                let allocate = || {
                    Ok(match pool.allocate(class(0))? {
                        IOResult::Done(_) => IOResult::Done(()),
                        IOResult::IO(io) => IOResult::IO(io),
                    })
                };
                let mut owner = Owner::new();
                let out = owner.call(|| {
                    match pool.fix_optimistic(cold, None)? {
                        IOResult::Done(_) => {}
                        IOResult::IO(_) => {
                            panic!("the read landed while its loader followed it, and it waited")
                        }
                    }
                    crate::storage::atoll::buffer::scarce(allocate)
                });
                assert!(!crate::race::pending("buffer.loader_follows"));
                assert!(matches!(out, Ok(IOResult::IO(_))), "the call came short");
                assert!(owner.queued(), "the shortage's place after the return");
                assert_eq!(
                    (owner.kept_frame(), owner.keeps_meta()),
                    (None, false),
                    "the owner kept a page across its shortage: (pool frame, metadata page)"
                );
                let frame = frame_of(&pool, cold);
                assert!(!pool.header(frame).record_keeps(), "the page's frame kept");
                let mut calls = 0;
                loop {
                    calls += 1;
                    match owner.call(allocate).unwrap() {
                        IOResult::Done(()) => break,
                        IOResult::IO(IOCompletions(wait)) => {
                            assert!(
                                wait.finished(),
                                "call {calls} handed back a wait not yet finished"
                            );
                            assert!(
                                calls < 3,
                                "the reserved operation did not finish in {calls} calls"
                            );
                        }
                    }
                }
                assert!(!owner.queued(), "the place after the operation ended");
            },
        );
    }

    /// Generation (#5219): a joiner B paused in its step right after its probe found load L under
    /// way. Meanwhile L lands with no reference left, its page is evicted and its frame freed:
    /// while B's step lasts, neither the frame in its class nor the frame of a larger class over
    /// the same memory is quiet for a read, so no later load can open a group there, and every
    /// read claim route waits for that (the quiet-memory tests). B's join then finds L's word at
    /// zero and is refused, touching nothing, and its fix goes on to a load of its own elsewhere.
    /// Once B's step has ended, the frame is quiet again.
    #[test]
    fn a_joiners_step_keeps_every_later_load_off_the_frame_it_probed() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_joiners_step_keeps_every_later_load_off_the_frame_it_probed"
            ),
            || {
                let (pool, reads) = held_reads_pool("/db/group-generation");
                let pid = PageId::new(class(0), 1);
                assert!(!fixed_unowned(&pool, pid), "the load waits");
                let frame = frame_of(&pool, pid);
                let large = class(9);
                let over = memory::frame_at(large, frame / memory::GRAINS, 0);
                assert!(grains(over, large).contains(&(frame as usize)));
                let seen = Arc::new(crate::storage::atoll::locks::Mutex::new(None));
                {
                    let (pool, reads, seen) = (pool.clone(), reads.clone(), seen.clone());
                    crate::race::at("buffer.join_probed", move || {
                        assert_eq!(land(&reads, Ok(())), 1);
                        let evicted = pool.evict_clean(pid);
                        let horizon = pool.store.epochs().horizon();
                        *seen.lock() = Some((
                            evicted,
                            pool.quiet_at(frame, class(0)) > horizon,
                            pool.quiet_at(over, large) > horizon,
                            pool.group(frame).released(),
                        ));
                    });
                }
                let mut b = Owner::new();
                let _ = fixed_as(&mut b, &pool, pid);
                let probed = !crate::race::pending("buffer.join_probed");
                let after_join = (pool.group(frame).released(), b.kept_frame());
                let quiet_after = pool.quiet_at(frame, class(0)) <= pool.store.epochs().horizon();
                land(&reads, Ok(()));
                drop(b);
                drop((pool, reads));
                assert!(probed, "B's fix never found the load");
                assert_eq!(
                    *seen.lock(),
                    Some((true, true, true, true)),
                    "in B's step: (L's page evicted, its frame unquiet, the larger frame over it \
                     unquiet, L's group released)"
                );
                assert!(after_join.0, "L's word after B's refused join");
                assert_ne!(
                    after_join.1,
                    Some(frame),
                    "B kept the frame L's group had let go"
                );
                assert!(quiet_after, "the frame stayed unquiet after B's step ended");
            },
        );
    }

    /// A source whose every read is a request of the store's first slot of the page's class, submitted
    /// through the pool's hook to the IO under the pool: on a pumped backend it stays in flight until a
    /// step delivers it, and it lands whole whatever the file held.
    struct PumpedSource;
    struct PumpedLocated(PageId);

    impl source::PageSource for PumpedSource {
        fn locate(&self, pid: PageId) -> Result<IOResult<Box<dyn source::Located>>> {
            Ok(IOResult::Done(Box::new(PumpedLocated(pid))))
        }
    }

    impl source::Located for PumpedLocated {
        fn read(
            self: Box<Self>,
            into: Arc<crate::io::Buffer>,
            done: source::ReadDone,
            submitting: source::Submitting<'_>,
        ) -> Result<IOResult<Option<Completion>>> {
            let (fill, done) = (
                into.clone(),
                crate::storage::atoll::once::TakeOnce::new(done),
            );
            let c = Completion::new_read(into, move |_| {
                let done = done.take()?;
                fill.as_mut_slice().fill(0);
                done(Ok(()));
                None
            });
            let place = Place::new(false, false, self.0.class(), 0);
            Ok(IOResult::Done(Some(submitting.read(place, c)?)))
        }
    }

    /// A pool of the smallest budget over `io`, its pages read through [`PumpedSource`].
    fn pumped_reads_pool(io: &Arc<dyn IO>, base: &str) -> Arc<BufferPool> {
        let store = PageStore::open(
            crate::storage::atoll::requests::counted_for_test(io.clone()),
            base,
            StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
            0,
        );
        BufferPool::new(
            BufferConfig::with_budget(MIN_BUDGET),
            Arc::new(PumpedSource),
            store,
        )
        .unwrap()
    }

    /// SHORT-HOLD1's F3 at the pool: A's load of a class 3 page is submitted to a pumped backend
    /// and not delivered, A outside any owner, so no one keeps the page once it lands. Class 3
    /// holds one batch and class 4 none, and every other batch went to class 0 while a step that
    /// began before is under way. B's read of a class 4 page drains class 3's only batch, whose one
    /// taken frame is A's, loading, and B then drives its read to its end on this one thread,
    /// blocking on each wait as `drive` does, a watchdog beside it. F3 read the drain's wait there
    /// as remote; it is of the stepped kind while the pool's own read is at the backend
    /// (`supply_wait`), so the wait steps the backend before packet 2's change as after it: B's
    /// wait delivers A's load, the drain takes the frame, and B's read lands. The kinds of B's
    /// waits are asserted, since a blocking wait now steps a pumped backend for both kinds and no
    /// hang would show a drain's wait made remote (mutant `supply-wait-remote`: `supply_wait`
    /// always remote).
    #[test]
    fn a_drain_waiting_on_a_load_in_flight_delivers_it_and_goes_on() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_drain_waiting_on_a_load_in_flight_delivers_it_and_goes_on"
            ),
            || {
                let crash = Arc::new(CrashIO::new(1));
                let io: Arc<dyn IO> = crash.clone();
                let pool = pumped_reads_pool(&io, "/db/f3-drain");
                let loading = PageId::new(class(3), 1);
                let wanted = PageId::new(class(4), 1);
                let a_waited = !fixed_unowned(&pool, loading);
                let frame = frame_of(&pool, loading);
                let a_in_flight = (crash.in_flight(), pool.header(frame).status() == LOADING);
                let step = pool.store.epochs().enter();
                let to_class_0 = pool.hold_free_batches(class(0));
                let mut b = Owner::new();
                let mut read = || {
                    b.call(|| {
                        Ok(match pool.fix_optimistic(wanted, None)? {
                            IOResult::Done(_) => IOResult::Done(()),
                            IOResult::IO(io) => IOResult::IO(io),
                        })
                    })
                };
                // The call that chooses the drain, with the step under way.
                let first = read();
                drop(step);
                let leaving = pool.memory.leaving();
                // Each wait B blocked on: whether remote, and the requests in flight then.
                let mut waits = Vec::new();
                let done = crate::child_test::watched(
                    "B's blocking read behind a drain of a frame loading",
                    std::time::Duration::from_secs(10),
                    || loop {
                        match read() {
                            Ok(IOResult::Done(())) => return Ok(()),
                            Ok(IOResult::IO(IOCompletions(c))) if c.finished() => {
                                crate::storage::atoll::park::yield_now()
                            }
                            Ok(IOResult::IO(IOCompletions(c))) => {
                                waits.push((c.is_remote_wait(), crash.in_flight()));
                                crate::storage::atoll::park::wait(io.as_ref(), &c);
                            }
                            Err(e) => return Err(e),
                        }
                    },
                );
                let after = (crash.in_flight(), pool.header(frame).status());
                drop(b);
                drop(pool);
                assert!(
                    a_waited && a_in_flight == (1, true),
                    "A's load was not left in flight: waited {a_waited}, in flight and loading \
                     {a_in_flight:?}"
                );
                assert_eq!(to_class_0, 4, "the batches handed to class 0");
                assert!(
                    matches!(first, Ok(IOResult::IO(_))),
                    "B's first call was done: {first:?}"
                );
                assert!(
                    leaving.len() == 1
                        && leaving[0].holder == class(3)
                        && leaving[0].draining
                        && leaving[0].beneficiary == Some(memory::Beneficiary::Class(class(4))),
                    "the drain chose class 3's only batch for class 4: {leaving:?}"
                );
                assert!(done.is_ok(), "B's read failed: {done:?}");
                assert_eq!(
                    waits,
                    [(false, 1), (false, 1)],
                    "B's waits as (remote, requests in flight): the drain's wait on A's load and \
                     then B's own read, each of the stepped kind with one request in flight"
                );
                assert_eq!(after.0, 0, "a request left in flight after B's read");
            },
        );
    }

    /// SHORT-HOLD1's park, built in a small pool, and its end (SHORT-HOLD1b). Owner A's load of a
    /// 32 KiB page (class 3) lands after A's call returned, so A keeps its reference in the load's
    /// group, the page it last loaded, idle: a statement whose call returned in a cooperative frame
    /// while its only thread steps another. Class 3 holds one batch and class 4 none, and every
    /// other batch went to class 0 while a step that began before is under way, so a drain passes
    /// them over. Owner B's read of a 64 KiB page (class 4, a version arena's) then drains class
    /// 3's only batch for class 4. On SHORT-HOLD1a's base B parked there: the batch's one taken
    /// frame stayed, kept by A's reference alone. Now the drain revokes A's idle keep, the
    /// group's generation rising, and takes the frame: B's read gets its frame in that batch, and
    /// A's next call finds its keep revoked and reads its page in again, keeping it sturdy.
    #[test]
    fn a_read_drains_the_only_batch_of_a_class_whose_one_frame_a_suspended_owner_keeps() {
        use std::{cell::RefCell, rc::Rc};
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_read_drains_the_only_batch_of_a_class_whose_one_frame_a_suspended_owner_keeps"
            ),
            || {
                let (pool, reads) = held_reads_pool("/db/short-hold");
                let kept = PageId::new(class(3), 1);
                let wanted = PageId::new(class(4), 1);
                let mut a = Owner::new();
                let a_waited = !fixed_as(&mut a, &pool, kept);
                let landed = land(&reads, Ok(()));
                let frame = frame_of(&pool, kept);
                // The park report as the drain is about to revoke A's keep, and the batches leaving
                // and the frame's group word once it did.
                let (report, at_revocation) =
                    (Rc::new(RefCell::new(None)), Rc::new(RefCell::new(None)));
                {
                    let (seen, weak) = (report.clone(), Arc::downgrade(&pool));
                    crate::race::at("group.revoke_seen", move || {
                        if let Some(pool) = weak.upgrade() {
                            *seen.borrow_mut() = Some(pool.park_report());
                        }
                    });
                    let (seen, weak) = (at_revocation.clone(), Arc::downgrade(&pool));
                    crate::race::at("buffer.revoked", move || {
                        if let Some(pool) = weak.upgrade() {
                            *seen.borrow_mut() =
                                Some((pool.memory.leaving(), pool.group(frame).word()));
                        }
                    });
                }
                // Every free batch to class 0, handed over while a step that began before is under
                // way: held for class 0 until the step ends, so B's claim drains none of them.
                let step = pool.store.epochs().enter();
                let to_class_0 = pool.hold_free_batches(class(0));
                let mut b = Owner::new();
                let mut read = || {
                    b.call(|| {
                        Ok(match pool.fix_optimistic(wanted, None)? {
                            IOResult::Done(_) => IOResult::Done(()),
                            IOResult::IO(io) => IOResult::IO(io),
                        })
                    })
                };
                // The call that chooses the drain; the next ones, with the step ended, reserve at
                // the top and go on to the frame, each read landed as it is made.
                let first = read();
                drop(step);
                let mut b_done = false;
                for _ in 0..16 {
                    match read() {
                        Ok(IOResult::Done(())) => {
                            b_done = true;
                            break;
                        }
                        Ok(IOResult::IO(_)) => {
                            land(&reads, Ok(()));
                        }
                        Err(_) => break,
                    }
                }
                let b_frame = pool.table.find(&pool.frames.headers, wanted.raw());
                let a_revoked_seen = a.keep_seen();
                let a_reloads = !fixed_as(&mut a, &pool, kept);
                let a_sturdy = a.keep_seen().0.map(|(_, _, _, sturdy)| sturdy);
                let relanded = land(&reads, Ok(()));
                let a_done = fixed_as(&mut a, &pool, kept);
                let loads_of_kept = pool
                    .loaded_pages()
                    .iter()
                    .filter(|&&p| p == kept.raw())
                    .count();
                let census = pool.taken_frames_census();
                let a_serial = a.serial();
                let a_revocations = a.revocations();
                let seen = at_revocation.borrow_mut().take();
                let report = report.borrow_mut().take().unwrap_or_default();
                drop(b);
                drop(a);
                drop((pool, reads));
                assert!(b_done, "B's read never got its frame behind A's idle keep");
                let (leaving, word) = seen.expect("the drain revoked A's keep");
                assert!(
                    leaving.len() == 1
                        && leaving[0].holder == class(3)
                        && leaving[0].draining
                        && leaving[0].beneficiary == Some(memory::Beneficiary::Class(class(4))),
                    "the drain chose class 3's only batch for class 4: {leaving:?}\n{report}"
                );
                assert!(
                    a_waited && landed == 1,
                    "A's load waited and its read landed"
                );
                assert_eq!(to_class_0, 4, "the batches handed to class 0\n{report}");
                assert!(
                    matches!(first, Ok(IOResult::IO(_))),
                    "B's first call was done"
                );
                assert_eq!(
                    (
                        group_word::references(word),
                        group_word::idle(word),
                        group_word::generation(word)
                    ),
                    (0, 0, 1),
                    "the group word as A's keep was revoked: (references, idle, generation)"
                );
                assert_eq!(
                    b_frame.map(|f| f / memory::GRAINS),
                    Some(frame / memory::GRAINS),
                    "B's frame is in class 3's old batch"
                );
                assert!(
                    matches!(a_revoked_seen, (Some((f, 0, true, false)), None) if f == frame),
                    "A still kept its frame, idle and ordinary, before its next call: {a_revoked_seen:?}"
                );
                assert!(
                    a_reloads && a_sturdy == Some(true) && relanded == 1 && a_done,
                    "A's next call reloaded its page into a sturdy keep ({a_reloads}, {a_sturdy:?}, \
                     {relanded}), and the one after was done ({a_done})"
                );
                assert_eq!(
                    (a_revocations, loads_of_kept),
                    (1, 2),
                    "(A's calls that found its keep revoked, the loads of its page)"
                );
                // The report as the drain was about to revoke: A's frame alone taken in the batch,
                // kept by A's idle ordinary keep, which the registry names idle.
                let frames: Vec<&str> = report
                    .lines()
                    .filter(|l| l.starts_with("  frame "))
                    .collect();
                assert!(
                    frames.len() == 1
                        && frames[0]
                            .starts_with(&format!("  frame {frame}: page {:#x} ", kept.raw()))
                        && frames[0].contains("resident, clean"),
                    "the taken frame is A's page, resident and clean\n{report}"
                );
                assert!(
                    frames[0].contains("record 0xc (waited|group)")
                        || frames[0].contains("record 0x8 (group)"),
                    "the frame's record is grouped\n{report}"
                );
                assert!(
                    frames[0].contains(
                        "group 0x2002 (closed, references 1, idle 1, sturdy 0, generation 0)"
                    ),
                    "the frame's group holds one idle ordinary keep\n{report}"
                );
                assert!(
                    frames[0].contains(&format!("keepers [owner #{a_serial} (connection 0x0, "))
                        && frames[0].contains(", idle)]"),
                    "the report names A as the frame's keeper, idle\n{report}"
                );
                assert!(
                    census.is_empty(),
                    "the census of taken frames: {census:#?}\n{report}"
                );
            },
        );
    }

    // ---------------------------------------------------------------- the revocable keep

    /// Every read the source holds landed: how many.
    fn land_small(reads: &PendingReads) -> usize {
        land(reads, Ok(()))
    }

    /// A pool of five batches whose claims of classes 3 and 4 can drain one batch alone, the one
    /// the first page read into a free batch takes, and with `spare` one more, of class 0, holding
    /// two pages read this epoch: dearer than one keep's page, cheaper than a frame held. Every
    /// other batch holds a 2 MiB page whose read never lands while the test runs, as held as a step
    /// under way: a drain chooses one only where nothing else can be. Those reads are kept apart,
    /// the third value, which the test lands at its end.
    fn shaped(base: &str, spare: bool) -> (Arc<BufferPool>, PendingReads, PendingReads) {
        let (pool, reads) = held_reads_pool(base);
        let fillers = if spare { 3 } else { 4 };
        for n in 1..=fillers {
            assert!(
                !fixed_unowned(&pool, PageId::new(class(9), n)),
                "a filler's read waits"
            );
        }
        let held: PendingReads = Arc::new(crate::storage::atoll::locks::Mutex::new(
            std::mem::take(&mut *reads.lock()),
        ));
        if spare {
            for n in 1..=2 {
                assert!(!fixed_unowned(&pool, PageId::new(class(0), n)));
            }
            assert_eq!(land_small(&reads), 2, "the spare batch's pages");
        }
        (pool, reads, held)
    }

    /// Moves the epoch past the steps that fixed pages so far, as frees and evictions do in a pool
    /// at work: a page a turn fixed reads as held by no step under way in the next turn's.
    fn later(pool: &BufferPool) {
        pool.store.epochs().stamp();
    }

    /// The loads begun of `pid`.
    fn loads_of(pool: &BufferPool, pid: PageId) -> usize {
        pool.loaded_pages()
            .iter()
            .filter(|&&p| p == pid.raw())
            .count()
    }

    /// One call of an operation of `owner`'s that fixes `pid` and, once it has, goes on later: it
    /// returns a yield, keeping the page. Whether it ran with its page.
    fn ran_as(owner: &mut Owner, pool: &Arc<BufferPool>, pid: PageId) -> bool {
        let mut ran = false;
        let out = owner.call(|| {
            Ok(match pool.fix_optimistic(pid, None)? {
                IOResult::Done(_) => {
                    ran = true;
                    IOResult::<()>::IO(IOCompletions(Completion::new_yield()))
                }
                IOResult::IO(io) => IOResult::IO(io),
            })
        });
        assert!(matches!(out, Ok(IOResult::IO(_))), "the operation's call");
        ran
    }

    /// Calls a statement of `owner`'s reading `pid` until it comes back with a wait nothing has
    /// finished yet, the fillers' reads landed after its first call, once its drain is chosen: no
    /// request of the pool is left at the backend then, so the wait is one a let-go finishes, not
    /// a landing alone (DESIGN.md 9.1.3, wait kinds).
    fn waiting_after_choice(
        owner: &mut Owner,
        pool: &Arc<BufferPool>,
        pid: PageId,
        fillers: &PendingReads,
    ) -> Option<Completion> {
        for call in 0..8 {
            let out = owner.call(|| {
                Ok(match pool.fix_optimistic(pid, None)? {
                    IOResult::Done(_) => IOResult::Done(()),
                    IOResult::IO(io) => IOResult::IO(io),
                })
            });
            if call == 0 {
                land(fillers, Ok(()));
            }
            match out {
                Ok(IOResult::IO(IOCompletions(wait))) if !wait.finished() => return Some(wait),
                Ok(IOResult::IO(_)) => {}
                _ => return None,
            }
        }
        None
    }

    /// How a statement driven to its end came out.
    #[derive(Debug)]
    enum Driven {
        Done,
        Failed(LimboError),
        /// A wait that neither a landing nor five seconds finished: a park.
        Parked,
    }

    /// Drives a statement of `owner`'s that fixes `pids` in turn to its end, as a host's blocking
    /// step does: each wait it comes back with is landed or, if still unfinished, waited for, up to
    /// five seconds, the watchdog's verdict past that.
    fn drive_statement(
        owner: &mut Owner,
        pool: &Arc<BufferPool>,
        reads: &PendingReads,
        pids: &[PageId],
    ) -> Driven {
        let mut next = 0;
        for _ in 0..64 {
            let out = owner.call(|| {
                while next < pids.len() {
                    match pool.fix_optimistic(pids[next], None)? {
                        IOResult::Done(_) => next += 1,
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    }
                }
                Ok(IOResult::Done(()))
            });
            match out {
                Ok(IOResult::Done(())) => return Driven::Done,
                Err(e) => return Driven::Failed(e),
                Ok(IOResult::IO(IOCompletions(wait))) => {
                    land_small(reads);
                    let since = std::time::Instant::now();
                    while !wait.finished() {
                        if since.elapsed() > std::time::Duration::from_secs(5) {
                            return Driven::Parked;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
            }
        }
        Driven::Parked
    }

    /// Draft 3 section 7: two connections, one thread, the blocking API. Connection A's operation
    /// loads its 32 KiB page and returns IO to its host, keeping the page, idle. The host then
    /// runs connection B's operation on the same thread through the blocking drive
    /// (`drive_for`, which a statement's header read takes): a new 64 KiB page, in a pool where
    /// only A's batch can be drained for it. On SHORT-HOLD1a's base B parks there for good, its
    /// wait finished by nothing (the child's watchdog says so); now the drain revokes A's idle
    /// keep, B's drive returns its page, and A's next calls read A's page in again and run with it.
    #[test]
    fn on_one_thread_a_blocking_drive_behind_another_connections_idle_keep_returns() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::on_one_thread_a_blocking_drive_behind_another_connections_idle_keep_returns"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/blocking", false);
                let p = PageId::new(class(3), 1);
                let mut a = Owner::of(Conn::new());
                let a_first = ran_as(&mut a, &pool, p);
                land(&reads, Ok(()));
                later(&pool);
                let a_frame = frame_of(&pool, p);
                // The watchdog: a park of B's drive is reported, and ends the child.
                let returned = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let watchdog = {
                    let (returned, pool) = (returned.clone(), Arc::downgrade(&pool));
                    std::thread::spawn(move || {
                        let since = std::time::Instant::now();
                        while since.elapsed() < std::time::Duration::from_secs(20) {
                            if returned.load(std::sync::atomic::Ordering::Acquire) {
                                return;
                            }
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                        let report = pool.upgrade().map(|p| p.park_report()).unwrap_or_default();
                        eprintln!(
                            "the watchdog: B's blocking drive parked behind A's idle keep\n{report}"
                        );
                        std::process::abort();
                    })
                };
                let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
                let b_conn = Conn::new();
                let b_page = crate::storage::atoll::drive_for(&b_conn, &io, || {
                    Ok(match pool.allocate(class(4))? {
                        IOResult::Done(page) => IOResult::Done(page.pid()),
                        IOResult::IO(io) => IOResult::IO(io),
                    })
                });
                returned.store(true, std::sync::atomic::Ordering::Release);
                watchdog.join().unwrap();
                let b_frame = b_page
                    .as_ref()
                    .ok()
                    .and_then(|pid| pool.table.find(&pool.frames.headers, pid.raw()));
                let mut a_ran = false;
                for _ in 0..16 {
                    a_ran = ran_as(&mut a, &pool, p);
                    land(&reads, Ok(()));
                    if a_ran {
                        break;
                    }
                }
                let (a_revoked, a_loads) = (a.revocations(), loads_of(&pool, p));
                drop(a);
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(!a_first, "A's first call loads its page and returns IO");
                assert!(
                    b_frame.is_some_and(|f| f / memory::GRAINS == a_frame / memory::GRAINS),
                    "B's drive returned no page in A's old batch: {b_page:?}, {b_frame:?}"
                );
                assert!(
                    a_ran && a_revoked == 1 && a_loads == 2,
                    "A's next calls read its page in again and ran with it: ran {a_ran}, \
                     revocations {a_revoked}, loads {a_loads}"
                );
            },
        );
    }

    /// F1's alternation (SHORT-HOLD1b): two statements on one thread take turns over the one batch
    /// classes 3 and 4 can share, with a spare batch beside it. A's operation loads its 32 KiB page
    /// and returns IO; B's statement wants a 64 KiB page, and its drain takes the batch, revoking
    /// A's idle keep; A's next call finds the keep revoked and reads its page in again, its drain
    /// taking the batch back from B's page, unkept now; B's next statement wants its page again.
    /// A's new keep is sturdy, so that drain takes the spare batch, at its real cost, and A runs
    /// with its page within two loads of it, B within no revocation. Once A has run with its page
    /// its keep is ordinary again: a third statement's drain then takes A's batch, holding one
    /// page, rather than B's, holding two (mutants `sturdy-revocable`, `sturdy-never-set`,
    /// `sturdy-left-set`).
    #[test]
    fn two_statements_taking_turns_over_one_batch_each_run_with_their_page_within_two_loads() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::two_statements_taking_turns_over_one_batch_each_run_with_their_page_within_two_loads"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/turns", true);
                let p = PageId::new(class(3), 1);
                let (q, q2, r) = (
                    PageId::new(class(4), 1),
                    PageId::new(class(4), 2),
                    PageId::new(class(5), 1),
                );
                let (mut a, mut b, mut c) = (Owner::new(), Owner::new(), Owner::new());
                let mut runs = Vec::new();
                let first = ran_as(&mut a, &pool, p);
                land_small(&reads);
                later(&pool);
                let b_first = drive_statement(&mut b, &pool, &reads, &[q]);
                later(&pool);
                // A's next calls, until it runs with its page, each read landed as the host steps.
                let mut a_ran = false;
                for _ in 0..8 {
                    a_ran = ran_as(&mut a, &pool, p);
                    runs.push((a_ran, a.keep_seen(), loads_of(&pool, p)));
                    land_small(&reads);
                    later(&pool);
                    if a_ran {
                        break;
                    }
                    let b_next = drive_statement(&mut b, &pool, &reads, &[q, q2]);
                    later(&pool);
                    if !matches!(b_next, Driven::Done) {
                        runs.push((false, (None, None), usize::MAX));
                        break;
                    }
                }
                let a_loads = loads_of(&pool, p);
                let a_revoked = a.revocations();
                let a_after_run = a.keep_seen();
                // A's operation goes on to another page of its class: revoked once, it keeps that
                // one sturdy too (the mark carried on, the auditor's #6341).
                let p2 = PageId::new(class(3), 2);
                let a_moved_ran = ran_as(&mut a, &pool, p2);
                land_small(&reads);
                later(&pool);
                let a_moved = a.keep_seen();
                // A third statement, of a class with no batch: A's batch holds A's sturdy keep, B's
                // two pages no keep, so it drains B's.
                let c_done = drive_statement(&mut c, &pool, &reads, &[r]);
                let a_again = ran_as(&mut a, &pool, p2);
                let a_revoked_by_c = a.revocations() - a_revoked;
                land(&reads, Ok(()));
                let census = pool.taken_frames_census();
                let b_revoked = b.revocations();
                drop((a, b, c));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(!first, "A's first call loads its page");
                assert!(
                    matches!(b_first, Driven::Done),
                    "B's first statement: {b_first:?}"
                );
                assert!(
                    a_ran && a_loads <= 2,
                    "A never ran with its page within two loads of it: ran {a_ran}, loads \
                     {a_loads}, calls (ran, keep, loads) {runs:?}"
                );
                assert_eq!(
                    (a_revoked, b_revoked),
                    (1, 0),
                    "(A's revocations before it ran, B's): {runs:?}"
                );
                assert!(
                    matches!(a_after_run, (Some((_, _, true, true)), None)),
                    "A's keep once A ran with its page, idle and still sturdy: {a_after_run:?}"
                );
                assert!(
                    !a_moved_ran && matches!(a_moved, (Some((_, _, true, true)), None)),
                    "A's next keep, of another page, sturdy too: loaded {}, {a_moved:?}",
                    !a_moved_ran
                );
                assert!(
                    matches!(c_done, Driven::Done) && a_again && a_revoked_by_c == 0,
                    "the third statement drained B's batch and left A's sturdy keep: {c_done:?}, A \
                     ran again {a_again}, revocations {a_revoked_by_c}"
                );
                assert!(census.is_empty(), "the census: {census:#?}");
            },
        );
    }

    /// F1's alternation with no spare batch (SHORT-HOLD1b, A3): A's reloaded page keeps the one
    /// batch sturdy, so B's next statement has nothing it can drain, every frame it waits behind
    /// kept by a sturdy idle keep alone. With no busy timeout B's claim ends at once in the
    /// shortage error, naming the class it wants, the batch and the sturdy keep (mutant
    /// `error-never-raised`: the claim parks, and the watchdog's verdict says so).
    #[test]
    fn taking_turns_with_no_spare_batch_ends_at_once_in_the_shortage_error_with_no_busy_timeout() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::taking_turns_with_no_spare_batch_ends_at_once_in_the_shortage_error_with_no_busy_timeout"
            ),
            || {
                let (outcome, elapsed) =
                    taking_turns_with_no_spare("/db/turns-0", BOwner::Connection(0));
                assert!(
                    matches!(&outcome, Driven::Failed(LimboError::CacheError(
                        crate::storage::page_cache::CacheError::TooSmall(e)
                    )) if e.starts_with(
                        "the page cache is too small for the statements open: a frame of class 4 \
                         (64 KiB) for a read waits on batch "
                    ) && e.contains(" of class 3, held by statements between their steps; sturdy \
                                     keeps: 1")),
                    "B's second statement: {outcome:?}"
                );
                assert!(
                    elapsed < std::time::Duration::from_secs(1),
                    "the error came after {elapsed:?}"
                );
            },
        );
    }

    /// [`taking_turns_with_no_spare_batch_ends_at_once_in_the_shortage_error_with_no_busy_timeout`]
    /// with a busy timeout of 200 ms: the claim waits that long behind the sturdy keep, the timer
    /// waking it, and then ends in the shortage error.
    #[test]
    fn taking_turns_with_no_spare_batch_ends_in_the_shortage_error_once_the_busy_timeout_passed() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::taking_turns_with_no_spare_batch_ends_in_the_shortage_error_once_the_busy_timeout_passed"
            ),
            || {
                let bound = std::time::Duration::from_millis(200);
                let (outcome, elapsed) =
                    taking_turns_with_no_spare("/db/turns-200", BOwner::Connection(bound.as_nanos() as u64));
                assert!(
                    matches!(&outcome, Driven::Failed(LimboError::CacheError(
                        crate::storage::page_cache::CacheError::TooSmall(e)
                    )) if e.starts_with(
                        "the page cache is too small for the statements open"
                    )),
                    "B's second statement: {outcome:?}"
                );
                assert!(
                    elapsed >= bound && elapsed < bound * 10,
                    "the error came after {elapsed:?}, against a busy timeout of {bound:?}"
                );
            },
        );
    }

    /// A statement of a connection whose busy timeout is set on the connection's own transaction
    /// (`ConnTxn::shortage_bound`) and whose claim is of a transaction the connection lent it
    /// (ASYNC1 E1b, `Conn::beside`): the claim waits its connection's busy timeout, 200 ms, then
    /// ends in the shortage error (the timeout on `ConnOf`, which every transaction of the
    /// connection shares; on the transaction's own identity it would read 0 and end at once).
    #[test]
    fn a_lent_statements_claim_waits_its_connections_busy_timeout_then_ends_in_too_small() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_lent_statements_claim_waits_its_connections_busy_timeout_then_ends_in_too_small"
            ),
            || {
                let bound = std::time::Duration::from_millis(200);
                let (outcome, elapsed) = taking_turns_with_no_spare(
                    "/db/turns-lent",
                    BOwner::Lent(bound.as_nanos() as u64),
                );
                assert!(
                    matches!(&outcome, Driven::Failed(LimboError::CacheError(
                        crate::storage::page_cache::CacheError::TooSmall(_)
                    ))),
                    "the lent statement's second statement: {outcome:?}"
                );
                assert!(
                    elapsed >= bound && elapsed < bound * 10,
                    "the lent statement's claim waited {elapsed:?}, against its connection's busy \
                     timeout of {bound:?}"
                );
            },
        );
    }

    /// An owner of no connection (`Owner::new`, every `drive` site outside the tests, the
    /// synchronous integrity check among them): its busy timeout is 0 (`Owner::busy`), so at the
    /// end of the road it gets the shortage error at once, whatever any connection's busy timeout
    /// is (the auditor's #6341: until connection operations run as their connection's).
    #[test]
    fn an_owner_of_no_connection_ends_at_once_in_too_small_at_the_end_of_the_road() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::an_owner_of_no_connection_ends_at_once_in_too_small_at_the_end_of_the_road"
            ),
            || {
                let (outcome, elapsed) =
                    taking_turns_with_no_spare("/db/turns-unowned", BOwner::NoConnection);
                assert!(
                    matches!(
                        &outcome,
                        Driven::Failed(LimboError::CacheError(
                            crate::storage::page_cache::CacheError::TooSmall(_)
                        ))
                    ),
                    "the owner of no connection's statement: {outcome:?}"
                );
                assert!(
                    elapsed < std::time::Duration::from_secs(1),
                    "the error came after {elapsed:?}"
                );
            },
        );
    }

    /// B's owner in the turns with no spare batch.
    enum BOwner {
        /// Of its connection's own transaction, the connection's busy timeout this many
        /// nanoseconds.
        Connection(u64),
        /// Of a transaction its connection lent it, the connection's busy timeout set on the
        /// connection's own transaction.
        Lent(u64),
        /// Of no connection.
        NoConnection,
    }

    /// The turns with no spare batch, B's owner `b_owner`: how B's second statement came out,
    /// and how long it took.
    fn taking_turns_with_no_spare(base: &str, b_owner: BOwner) -> (Driven, std::time::Duration) {
        let (pool, reads, fillers) = shaped(base, false);
        let p = PageId::new(class(3), 1);
        let q = PageId::new(class(4), 1);
        let b = match b_owner {
            BOwner::Connection(busy) => {
                let conn = Conn::new();
                conn.set_busy(busy);
                Owner::of(conn)
            }
            BOwner::Lent(busy) => {
                let own = Conn::new();
                own.set_busy(busy);
                Owner::of(Conn::beside(&own))
            }
            BOwner::NoConnection => Owner::new(),
        };
        let (mut a, mut b) = (Owner::new(), b);
        let mut steps = vec![ran_as(&mut a, &pool, p)];
        land_small(&reads);
        later(&pool);
        let b_first = drive_statement(&mut b, &pool, &reads, &[q]);
        later(&pool);
        steps.push(ran_as(&mut a, &pool, p));
        land_small(&reads);
        later(&pool);
        let a_sturdy = a.keep_seen();
        let since = std::time::Instant::now();
        let outcome = drive_statement(&mut b, &pool, &reads, &[q]);
        let elapsed = since.elapsed();
        let report = pool.park_report();
        drop(b);
        let a_ran = ran_as(&mut a, &pool, p);
        land(&reads, Ok(()));
        drop(a);
        land(&fillers, Ok(()));
        drop((pool, reads, fillers));
        assert!(
            steps == [false, false] && matches!(b_first, Driven::Done),
            "A's two loads and B's first statement: {steps:?}, {b_first:?}"
        );
        assert!(
            matches!(a_sturdy, (Some((_, _, true, true)), None)),
            "A's reloaded page, kept idle and sturdy: {a_sturdy:?}"
        );
        assert!(
            !matches!(outcome, Driven::Parked),
            "B's claim parked behind A's sturdy keep, its wait never finished\n{report}"
        );
        assert!(a_ran, "A ran with its page after B's statement ended");
        (outcome, elapsed)
    }

    /// F4 (SHORT-HOLD1b): a kept page that another statement changed while its keep was idle. B's
    /// drain writes the page out, holding its frame until the write lands, and only then revokes
    /// A's keep and frees the frame; A's next call reads its page in again and reads the change.
    #[test]
    fn a_dirty_kept_page_is_written_out_then_its_keep_revoked_and_the_change_read_after_the_reload()
    {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_dirty_kept_page_is_written_out_then_its_keep_revoked_and_the_change_read_after_the_reload"
            ),
            || {
                let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
                let pool = open_on(&io, "/db/dirty-kept", MIN_BUDGET);
                // A page of class 3 written to its copy and out of the pool, so a fix reads it in.
                let p = {
                    let mut g = drive(&io, || pool.allocate(class(3))).unwrap();
                    let p = g.pid();
                    stamp(&mut g.page_mut(), p, 1);
                    g.changed();
                    let frame = g.frame;
                    drop(g);
                    let hold = pool
                        .latch_to_evict(class(3), frame, || FrameLatch {
                            frames: pool.frames.clone(),
                            frame,
                        })
                        .expect("the page is held by no one");
                    let out = pool.start_write_out(hold);
                    pool.write_out(class(3), vec![out]);
                    drive(&io, || pool.settle_writes()).unwrap();
                    p
                };
                let mut a = Owner::new();
                let a_first = ran_as(&mut a, &pool, p);
                let frame = frame_of(&pool, p);
                // Another statement changes the page while A's keep is idle.
                {
                    let mut g = drive(&io, || pool.fix_exclusive(p, None)).unwrap();
                    stamp(&mut g.page_mut(), p, 2);
                    g.changed();
                }
                let dirty = pool.header(frame).dirty();
                later(&pool);
                let step = pool.store.epochs().enter();
                let to_class_0 = pool.hold_free_batches(class(0));
                let mut b = Owner::new();
                let mut allocate = || {
                    b.call(|| {
                        Ok(match pool.allocate(class(4))? {
                            IOResult::Done(page) => IOResult::Done(page.pid()),
                            IOResult::IO(io) => IOResult::IO(io),
                        })
                    })
                };
                // The frame as the drain revokes A's keep: written out, clean, still held for the
                // write-out and kept.
                let at_revocation = std::rc::Rc::new(std::cell::RefCell::new(None));
                {
                    let (seen, weak) = (at_revocation.clone(), Arc::downgrade(&pool));
                    crate::race::at("buffer.revoked", move || {
                        if let Some(pool) = weak.upgrade() {
                            let h = pool.header(frame);
                            *seen.borrow_mut() = Some((h.status(), h.dirty(), h.grouped()));
                        }
                    });
                }
                let first = allocate();
                drop(step);
                let mut b_page = None;
                for _ in 0..64 {
                    match allocate() {
                        Ok(IOResult::Done(pid)) => {
                            b_page = Some(pid);
                            break;
                        }
                        Ok(IOResult::IO(IOCompletions(wait))) => {
                            if !wait.finished() {
                                io.step().unwrap();
                            }
                        }
                        Err(_) => break,
                    }
                }
                let written_while_kept = at_revocation.borrow_mut().take();
                let b_frame = b_page.and_then(|pid| pool.table.find(&pool.frames.headers, pid.raw()));
                let word = pool.group(frame).word();
                let a_reloaded = ran_as(&mut a, &pool, p);
                let read = drive(&io, || pool.fix_optimistic(p, None))
                    .unwrap()
                    .read(|page| stamped(page, p))
                    .flatten();
                let loads = loads_of(&pool, p);
                let a_revoked = a.revocations();
                drop((a, b));
                drop(pool);
                assert!(!a_first || dirty, "A's first call read its page in: {a_first}");
                assert!(dirty, "the page was dirty while A's keep was idle");
                assert_eq!(to_class_0, 4, "the batches handed to class 0");
                assert!(matches!(first, Ok(IOResult::IO(_))), "B's first call was done");
                assert!(
                    b_frame.is_some_and(|f| f / memory::GRAINS == frame / memory::GRAINS),
                    "B's new page got no frame in class 3's old batch: {b_page:?}, {b_frame:?}"
                );
                assert_eq!(
                    written_while_kept,
                    Some((OUTGOING, false, true)),
                    "as A's keep was revoked, the page was written, clean, held for its write-out and \
                     kept: (status, dirty, grouped)"
                );
                assert_eq!(
                    (group_word::references(word), group_word::generation(word)),
                    (0, 1),
                    "A's keep revoked once the write landed: (references, generation)"
                );
                assert!(
                    a_reloaded && a_revoked == 1 && loads == 2,
                    "A's next call reloaded its page: ran {a_reloaded}, revocations {a_revoked}, \
                     loads {loads}"
                );
                assert_eq!(read, Some(2), "the page read after the reload");
            },
        );
    }

    /// A1 (SHORT-HOLD1b): a frame whose generation is one below its last value still takes a keep,
    /// and its revocation takes it to the last; the revoked keep's next call finds another
    /// generation and its release changes nothing. A load into a frame at the last generation
    /// keeps nothing, a fix that finds that load is refused its join, and both complete unkept.
    #[test]
    fn a_frame_at_its_last_generation_takes_no_keep_and_a_stale_release_changes_nothing() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_frame_at_its_last_generation_takes_no_keep_and_a_stale_release_changes_nothing"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/last-generation", false);
                // Every grain no group holds, at `generation`.
                let preset = |generation: u32| {
                    let word = group_word::word_of(false, 0, 0, 0, generation);
                    for grain in 0..pool.memory.grains() {
                        if group_word::released(pool.group(grain).word()) {
                            pool.group(grain).preset(word);
                        }
                    }
                };
                let last = group_word::LAST_GENERATION;
                preset(last - 1);
                let p = PageId::new(class(3), 1);
                let (q, q2) = (PageId::new(class(4), 1), PageId::new(class(4), 2));
                let mut a = Owner::new();
                let a_first = ran_as(&mut a, &pool, p);
                let a_kept = a.keep_seen();
                land_small(&reads);
                later(&pool);
                let frame = frame_of(&pool, p);
                // E's statement drains the batch, revoking A's keep.
                let mut e = Owner::new();
                let e_done = drive_statement(&mut e, &pool, &reads, &[q]);
                let revoked = pool.group(frame).word();
                // A's next call's entry: its keep revoked, its release refused, the word left as it
                // was; then the call, which reads A's page in again.
                a.enter_for_test();
                let after = pool.group(frame).word();
                let a_revoked = a.revocations();
                drop(a);
                land_small(&reads);
                later(&pool);
                // Every frame at the last generation: B's load keeps nothing, and C's join of it is
                // refused.
                preset(last);
                let (mut b, mut c) = (Owner::new(), Owner::new());
                let b_first = fixed_as(&mut b, &pool, q2);
                let mut b_calls = 1;
                while pool.table.find(&pool.frames.headers, q2.raw()).is_none() && b_calls < 16 {
                    assert!(!fixed_as(&mut b, &pool, q2), "B's read before its load");
                    b_calls += 1;
                }
                let b_frame = frame_of(&pool, q2);
                let at_load = pool.group(b_frame).word();
                let b_kept = b.keep_seen();
                let c_first = fixed_as(&mut c, &pool, q2);
                let c_kept = c.keep_seen();
                let landed = land_small(&reads);
                let (b_done, c_done) = (fixed_as(&mut b, &pool, q2), fixed_as(&mut c, &pool, q2));
                land(&reads, Ok(()));
                drop((b, c, e));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(
                    !a_first
                        && matches!(a_kept, (Some((_, at, true, false)), None) if at == last - 1),
                    "A's load keeps its page one generation below the last: {a_kept:?}"
                );
                assert!(
                    matches!(e_done, Driven::Done)
                        && group_word::generation(revoked) == last
                        && group_word::released(revoked),
                    "E's statement revoked A's keep, its frame at the last generation: {e_done:?}, \
                     {revoked:#x}"
                );
                assert!(
                    a_revoked == 1 && after == revoked,
                    "A's stale release changed the word: revocations {a_revoked}, {revoked:#x} to \
                     {after:#x}"
                );
                assert!(!b_first, "B's first call reads its page in");
                assert!(
                    group_word::generation(at_load) == last
                        && group_word::references(at_load) == 1
                        && b_kept.0.is_none(),
                    "B's load at the last generation, its guard alone, B keeping nothing: \
                     {at_load:#x}, {b_kept:?}"
                );
                assert!(
                    !c_first && c_kept.0.is_none(),
                    "C's join refused at the last generation: {c_first}, {c_kept:?}"
                );
                assert!(
                    landed == 1 && b_done && c_done,
                    "B's and C's fixes completed unkept: landed {landed}, {b_done}, {c_done}"
                );
            },
        );
    }

    /// A2 (SHORT-HOLD1b): the references of a load's group at their bound, 4,095. A fix that finds
    /// the load under way is refused its join, keeps nothing, and waits for the load unkept; once
    /// the read lands it fixes the page without reading it again.
    #[test]
    fn a_join_at_the_references_limit_is_refused_and_its_fixer_waits_for_the_load_unkept() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_join_at_the_references_limit_is_refused_and_its_fixer_waits_for_the_load_unkept"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/references-limit", false);
                let p = PageId::new(class(3), 1);
                let (mut a, mut c) = (Owner::new(), Owner::new());
                let a_first = ran_as(&mut a, &pool, p);
                let frame = frame_of(&pool, p);
                let real = pool.group(frame).word();
                pool.group(frame)
                    .preset(group_word::word_of(true, group_word::MOST, 1, 0, 0));
                let c_first = fixed_as(&mut c, &pool, p);
                let c_kept = c.keep_seen();
                let at_bound = pool.group(frame).word();
                pool.group(frame).preset(real);
                let landed = land_small(&reads);
                let c_done = fixed_as(&mut c, &pool, p);
                let a_ran = ran_as(&mut a, &pool, p);
                let loads = loads_of(&pool, p);
                land(&reads, Ok(()));
                drop((a, c));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(!a_first, "A's call reads its page in");
                assert_eq!(
                    real,
                    group_word::word_of(true, 2, 1, 0, 0),
                    "the load's group: open, its guard and A's idle keep"
                );
                assert!(
                    !c_first
                        && c_kept.0.is_none()
                        && group_word::references(at_bound) == group_word::MOST,
                    "C's join at the bound was admitted or kept: {c_first}, {c_kept:?}, \
                     {at_bound:#x}"
                );
                assert!(
                    landed == 1 && c_done && a_ran && loads == 1,
                    "C completed unkept on the one load: landed {landed}, {c_done}, A {a_ran}, \
                     loads {loads}"
                );
            },
        );
    }

    /// A revoked keep dropped after its page came back to its very frame (draft 3 section 7): the
    /// frame's group is at a later generation, so the stale keep's release changes nothing, and
    /// the new keeper's reference stands (mutant `revoke-keeps-generation`).
    #[test]
    fn a_revoked_keep_dropped_after_its_page_came_back_to_its_frame_releases_nothing() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_revoked_keep_dropped_after_its_page_came_back_to_its_frame_releases_nothing"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/stale-drop", false);
                let p = PageId::new(class(3), 1);
                let q = PageId::new(class(4), 1);
                let (mut a, mut b, mut c) = (Owner::new(), Owner::new(), Owner::new());
                let a_first = ran_as(&mut a, &pool, p);
                land_small(&reads);
                later(&pool);
                let frame = frame_of(&pool, p);
                let b_done = drive_statement(&mut b, &pool, &reads, &[q]);
                let q_evicted = pool.evict_clean(q);
                later(&pool);
                // C reads A's page in again: the batch drained back, the page in its old frame.
                let c_first = fixed_as(&mut c, &pool, p);
                land_small(&reads);
                let c_frame = pool.table.find(&pool.frames.headers, p.raw());
                let c_kept = c.keep_seen();
                let before = pool.group(frame).word();
                drop(a);
                let after = pool.group(frame).word();
                let intact = after == before;
                let c_done = intact && fixed_as(&mut c, &pool, p);
                land(&reads, Ok(()));
                if intact {
                    drop(c);
                } else {
                    // Its reference is gone: releasing it would end the process first.
                    std::mem::forget(c);
                }
                drop(b);
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(
                    intact,
                    "A's stale keep released C's reference: {before:#x} to {after:#x}"
                );
                assert!(
                    !a_first && matches!(b_done, Driven::Done) && q_evicted,
                    "A's load, B's statement and its page's eviction: {b_done:?}"
                );
                assert!(
                    !c_first && c_frame == Some(frame),
                    "C's load of A's page into A's frame: {c_frame:?}, not {frame}"
                );
                assert!(
                    matches!(c_kept, (Some((f, 1, true, false)), None) if f == frame),
                    "C keeps the frame, idle, at the next generation: {c_kept:?}"
                );
                assert_eq!(
                    before,
                    group_word::word_of(false, 1, 1, 0, 1),
                    "C's keep, closed and idle, at the next generation"
                );
                assert!(c_done, "C's next call fixed its page");
            },
        );
    }

    /// Wake against revoke (draft 3.1, packet 1): the first swap wins. With the owner's entry
    /// first, the drain passes the frame over, its keep active; the owner's IO return then leaves
    /// the frame revocable and tells the claim, which goes on and revokes. With the drain first,
    /// the owner's entry finds another generation and its next fix reads the page in again (mutant
    /// `revoke-active`).
    #[test]
    fn a_wake_racing_a_revocation_keeps_the_page_if_it_is_first_and_reloads_it_if_not() {
        use std::{cell::RefCell, rc::Rc};
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_wake_racing_a_revocation_keeps_the_page_if_it_is_first_and_reloads_it_if_not"
            ),
            || {
                let p = PageId::new(class(3), 1);
                let q = PageId::new(class(4), 1);
                // The owner first: its entry runs where the drain is about to swap.
                let (pool, reads, fillers) = shaped("/db/wake-first", false);
                let a = Rc::new(RefCell::new(Owner::new()));
                let a_first = ran_as(&mut a.borrow_mut(), &pool, p);
                land_small(&reads);
                later(&pool);
                let frame = frame_of(&pool, p);
                {
                    let a = a.clone();
                    crate::race::at("group.revoke_seen", move || a.borrow_mut().enter_for_test());
                }
                let mut b = Owner::new();
                let b_wait = waiting_after_choice(&mut b, &pool, q, &fillers);
                let passed_over = pool.group(frame).word();
                let a_active = a.borrow().keep_seen();
                let waited_before = b_wait.as_ref().map(Completion::finished);
                a.borrow_mut().return_io_for_test();
                let told = b_wait.as_ref().map(Completion::finished);
                let b_done = drive_statement(&mut b, &pool, &reads, &[q]);
                let a_reloads = !ran_as(&mut a.borrow_mut(), &pool, p);
                for _ in 0..16 {
                    if loads_of(&pool, p) == 2 || ran_as(&mut a.borrow_mut(), &pool, p) {
                        break;
                    }
                }
                let first_case = (a.borrow().revocations(), loads_of(&pool, p));
                land(&reads, Ok(()));
                drop(b_wait);
                drop(b);
                drop(a);
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                // The drain first: it runs where the owner's entry is about to swap.
                let (pool, reads, fillers) = shaped("/db/revoke-first", false);
                let mut a = Owner::new();
                let a_first_again = ran_as(&mut a, &pool, p);
                land_small(&reads);
                later(&pool);
                let frame_again = frame_of(&pool, p);
                let b_outcome = Rc::new(RefCell::new(None));
                {
                    let (weak, reads, outcome) =
                        (Arc::downgrade(&pool), reads.clone(), b_outcome.clone());
                    crate::race::at("group.wake_seen", move || {
                        if let Some(pool) = weak.upgrade() {
                            let mut b = Owner::new();
                            *outcome.borrow_mut() =
                                Some(drive_statement(&mut b, &pool, &reads, &[q]));
                        }
                    });
                }
                let a_ran = ran_as(&mut a, &pool, p);
                let revoked = pool.group(frame_again).word();
                let second_case = (a.revocations(), a.keep_seen(), loads_of(&pool, p));
                land(&reads, Ok(()));
                let b_outcome = b_outcome.borrow_mut().take();
                drop(a);
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(!a_first && !a_first_again, "A's first calls load its page");
                assert_eq!(
                    passed_over,
                    group_word::word_of(false, 1, 0, 0, 0),
                    "with A's entry first, the drain passed the frame over, A's keep active"
                );
                assert!(
                    matches!(a_active, (Some((f, 0, false, false)), None) if f == frame),
                    "A's keep, active and held: {a_active:?}"
                );
                assert_eq!(
                    (waited_before, told),
                    (Some(false), Some(true)),
                    "B's claim waited behind A's active keep until A's IO return told it"
                );
                assert!(
                    matches!(b_done, Driven::Done) && a_reloads && first_case == (1, 2),
                    "then B's drain revoked A's idle keep and A reloaded: {b_done:?}, \
                     (revocations, loads) {first_case:?}"
                );
                assert!(
                    matches!(b_outcome, Some(Driven::Done)),
                    "with the drain first, B's statement: {b_outcome:?}"
                );
                assert!(
                    !a_ran
                        && group_word::generation(revoked) >= 1
                        && matches!(second_case, (1, (Some((_, _, true, true)), None), 2)),
                    "A's entry found its keep revoked and reloaded the page, sturdy: ran {a_ran}, \
                     {revoked:#x}, {second_case:?}"
                );
            },
        );
    }

    /// Join against revoke (draft 3.1, packet 1): the join's swap holds the generation. A join
    /// first leaves a reference that is not idle, so the drain passes the frame over until it is
    /// let go; a revocation first leaves no reference, so the join is refused and its fixer keeps
    /// nothing, reading the page in again itself.
    #[test]
    fn a_join_racing_a_revocation_holds_the_frame_if_it_is_first_and_keeps_nothing_if_not() {
        use std::{cell::RefCell, rc::Rc};
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_join_racing_a_revocation_holds_the_frame_if_it_is_first_and_keeps_nothing_if_not"
            ),
            || {
                let p = PageId::new(class(3), 1);
                let q = PageId::new(class(4), 1);
                // The join first, as a fix that probed the load before it landed joins after.
                let (pool, reads, fillers) = shaped("/db/join-first", false);
                let mut a = Owner::new();
                let a_first = ran_as(&mut a, &pool, p);
                land_small(&reads);
                later(&pool);
                let frame = frame_of(&pool, p);
                let joined = Rc::new(RefCell::new(None));
                {
                    let (weak, joined) = (Arc::downgrade(&pool), joined.clone());
                    crate::race::at("group.revoke_seen", move || {
                        if let Some(pool) = weak.upgrade() {
                            let step = pool.store.epochs().enter();
                            *joined.borrow_mut() =
                                pool.group(frame).join(&step, group_word::Keep::Ordinary);
                        }
                    });
                }
                let mut b = Owner::new();
                let b_wait = waiting_after_choice(&mut b, &pool, q, &fillers);
                let passed_over = pool.group(frame).word();
                let joined = joined.borrow_mut().take();
                if let Some(at) = joined {
                    pool.release_kept(frame, p, at, false, group_word::Keep::Ordinary);
                }
                let told = b_wait.as_ref().map(Completion::finished);
                let b_done = drive_statement(&mut b, &pool, &reads, &[q]);
                land(&reads, Ok(()));
                drop(b_wait);
                drop((a, b));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                // The revocation first: the fix probed the load under way, the read landed, and the
                // drain revokes A's keep before the join's swap.
                let (pool, reads, fillers) = shaped("/db/revoke-before-join", false);
                let mut a = Owner::new();
                let a_first_again = ran_as(&mut a, &pool, p);
                let frame_again = frame_of(&pool, p);
                {
                    let reads = reads.clone();
                    crate::race::at("buffer.join_probed", move || {
                        land_small(&reads);
                    });
                }
                let b_outcome = Rc::new(RefCell::new(None));
                {
                    let (weak, reads, outcome) =
                        (Arc::downgrade(&pool), reads.clone(), b_outcome.clone());
                    crate::race::at("group.join_seen", move || {
                        if let Some(pool) = weak.upgrade() {
                            pool.store.epochs().stamp();
                            let mut b = Owner::new();
                            *outcome.borrow_mut() =
                                Some(drive_statement(&mut b, &pool, &reads, &[q]));
                        }
                    });
                }
                later(&pool);
                let mut c = Owner::new();
                let c_first = fixed_as(&mut c, &pool, p);
                let revoked = pool.group(frame_again).word();
                let c_refused = c.keep_seen();
                // B's drain handed the batch over during C's step, so C's own claim may have
                // chosen a filler's: its read lands, as reads do.
                land(&fillers, Ok(()));
                // C's fix goes on to read the page in itself.
                let mut c_calls = Vec::new();
                for _ in 0..16 {
                    if loads_of(&pool, p) == 2 {
                        break;
                    }
                    let fixed = fixed_as(&mut c, &pool, p);
                    c_calls.push((fixed, c.keep_seen(), loads_of(&pool, p)));
                    if fixed {
                        break;
                    }
                }

                let c_kept = c.keep_seen();
                let loads = loads_of(&pool, p);
                land(&reads, Ok(()));
                let b_outcome = b_outcome.borrow_mut().take();
                drop((a, c));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(!a_first && !a_first_again, "A's first calls load its page");
                assert!(
                    joined == Some(0) && passed_over == group_word::word_of(false, 2, 1, 0, 0),
                    "with the join first, it was admitted and the drain passed the frame over: \
                     {joined:?}, {passed_over:#x}"
                );
                assert!(
                    told == Some(true) && matches!(b_done, Driven::Done),
                    "the joined reference's release told B's claim, which went on: {told:?}, \
                     {b_done:?}"
                );
                assert!(
                    matches!(b_outcome, Some(Driven::Done)),
                    "with the revocation first, B's statement: {b_outcome:?}"
                );
                assert!(
                    !c_first && group_word::generation(revoked) >= 1 && c_refused.0.is_none(),
                    "C's join was refused after the revocation: {c_first}, {revoked:#x}, \
                     {c_refused:?}"
                );
                assert!(
                    matches!(c_kept, (Some((_, _, true, false)), None)) && loads == 2,
                    "C read the page in itself, keeping it: {c_kept:?}, loads {loads}, calls \
                     {c_calls:?}"
                );
            },
        );
    }

    /// Release against revoke (draft 3.1, packet 1): the generation decides. A release first takes
    /// the group to no reference, its releaser letting the group's keep go, and the drain frees
    /// the frame once, no revocation made; a revocation first leaves the release another
    /// generation, and it releases nothing.
    #[test]
    fn a_release_racing_a_revocation_lets_the_frame_go_once_whichever_is_first() {
        use std::{cell::RefCell, rc::Rc};
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_release_racing_a_revocation_lets_the_frame_go_once_whichever_is_first"
            ),
            || {
                let p = PageId::new(class(3), 1);
                let q = PageId::new(class(4), 1);
                // The release first: A's owner dropped where the drain is about to swap.
                let (pool, reads, fillers) = shaped("/db/release-first", false);
                let a = Rc::new(RefCell::new(Some(Owner::new())));
                let a_first = ran_as(a.borrow_mut().as_mut().unwrap(), &pool, p);
                land_small(&reads);
                later(&pool);
                let frame = frame_of(&pool, p);
                {
                    let a = a.clone();
                    crate::race::at("group.revoke_seen", move || drop(a.borrow_mut().take()));
                }
                let mut b = Owner::new();
                let b_done = drive_statement(&mut b, &pool, &reads, &[q]);
                let released = pool.group(frame).word();
                let dropped = a.borrow().is_none();
                land(&reads, Ok(()));
                let census = pool.taken_frames_census();
                drop(b);
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                // The revocation first: B's statement drains where A's release is about to swap.
                let (pool, reads, fillers) = shaped("/db/revoke-before-release", false);
                let mut a = Owner::new();
                let a_first_again = ran_as(&mut a, &pool, p);
                land_small(&reads);
                later(&pool);
                let frame_again = frame_of(&pool, p);
                let b_outcome = Rc::new(RefCell::new(None));
                {
                    let (weak, reads, outcome) =
                        (Arc::downgrade(&pool), reads.clone(), b_outcome.clone());
                    crate::race::at("group.release_seen", move || {
                        if let Some(pool) = weak.upgrade() {
                            let mut b = Owner::new();
                            *outcome.borrow_mut() =
                                Some(drive_statement(&mut b, &pool, &reads, &[q]));
                        }
                    });
                }
                drop(a);
                let after = pool.group(frame_again).word();
                let q_frame = pool.table.find(&pool.frames.headers, q.raw());
                land(&reads, Ok(()));
                let census_again = pool.taken_frames_census();
                let b_outcome = b_outcome.borrow_mut().take();
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(!a_first && !a_first_again, "A's first calls load its page");
                assert!(
                    dropped && matches!(b_done, Driven::Done) && released == 0,
                    "with the release first, the frame let go and drained with no revocation: \
                     {b_done:?}, {released:#x}"
                );
                assert!(census.is_empty(), "the census: {census:#?}");
                assert!(
                    matches!(b_outcome, Some(Driven::Done))
                        && q_frame.is_some()
                        && after == group_word::word_of(false, 0, 0, 0, 1),
                    "with the revocation first, A's release released nothing: {b_outcome:?}, \
                     {q_frame:?}, {after:#x}"
                );
                assert!(census_again.is_empty(), "the census: {census_again:#?}");
            },
        );
    }

    /// Ordinary eviction honours an idle keep (draft 3 section 5): a class evicting its own pages
    /// never revokes, so an operation under pressure runs with its page at its first load. Class 9
    /// holds every batch, one frame each, four of them being read in: B's claim finds A's page, kept
    /// idle, the class's one page to evict, passes it over and waits; A's next call runs with its
    /// page and lets it go, which tells B (mutant `evict-revokes`).
    #[test]
    fn an_operation_whose_class_evicts_its_own_runs_with_its_page_at_its_first_load() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::an_operation_whose_class_evicts_its_own_runs_with_its_page_at_its_first_load"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/evict-own", false);
                let p = PageId::new(class(9), 10);
                let q = PageId::new(class(9), 11);
                let (mut a, mut b) = (Owner::new(), Owner::new());
                let a_first = fixed_as(&mut a, &pool, p);
                land_small(&reads);
                later(&pool);
                let mut b_wait = None;
                for _ in 0..8 {
                    match b.call(|| {
                        Ok(match pool.fix_optimistic(q, None)? {
                            IOResult::Done(_) => IOResult::Done(()),
                            IOResult::IO(io) => IOResult::IO(io),
                        })
                    }) {
                        Ok(IOResult::IO(IOCompletions(wait))) if !wait.finished() => {
                            b_wait = Some(wait);
                            break;
                        }
                        _ => {}
                    }
                }
                let b_loads_before = loads_of(&pool, q);
                let b_waited = b_wait.as_ref().map(Completion::finished);
                let a_done = fixed_as(&mut a, &pool, p);
                let b_done = drive_statement(&mut b, &pool, &reads, &[q]);
                let (a_loads, a_revoked) = (loads_of(&pool, p), a.revocations());
                land(&reads, Ok(()));
                drop(b_wait);
                drop((a, b));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(!a_first, "A's first call loads its page");
                assert!(
                    a_done && a_loads == 1 && a_revoked == 0,
                    "A ran with its page at its first load: done {a_done}, loads {a_loads}, \
                     revocations {a_revoked}"
                );
                assert!(
                    b_loads_before == 0
                        && b_waited == Some(false)
                        && matches!(b_done, Driven::Done),
                    "B waited behind A's keep until A let it go: loads before {b_loads_before}, \
                     waited {b_waited:?}, {b_done:?}"
                );
            },
        );
    }

    /// F2 (SHORT-HOLD1b): a swap that would leave more idle keeps than references ends the process.
    #[test]
    fn a_keep_counted_idle_beyond_its_references_ends_the_process() {
        let test = concat!(
            module_path!(),
            "::a_keep_counted_idle_beyond_its_references_ends_the_process"
        );
        if let Some(ended) = crate::storage::atoll::testing::in_child(test, || {
            let g = group_word::GroupWord::default();
            let at = g.open(Some(group_word::Keep::Ordinary)).expect("a keep");
            assert!(!g.close(), "the loader's keep is left");
            g.idle(at);
            g.idle(at);
        }) {
            assert!(
                ended.code != Some(0)
                    && ended.stderr.contains(
                        "a keep going idle left more idle or sturdy keeps than references"
                    ),
                "the child ended {:?}: {}",
                ended.code,
                ended.stderr
            );
        }
    }

    /// A claim that passed a frame over for a step's mark (SHORT-HOLD1b): the page's keeps are
    /// revocable, but a step under way may hold the page, so the drain leaves it and the claim
    /// waits on the epoch one past the mark. The step's end, on another thread, is what wakes it:
    /// no other let-go comes. Woken, its drain revokes the keep (mutant `held-mark-no-wake`: the
    /// claim waits on nothing the step's end finishes, and parks).
    #[test]
    fn a_claim_passed_over_for_a_steps_mark_is_woken_when_the_step_ends() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_claim_passed_over_for_a_steps_mark_is_woken_when_the_step_ends"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/step-mark", false);
                let p = PageId::new(class(3), 1);
                let q = PageId::new(class(4), 1);
                let mut a = Owner::new();
                let a_first = ran_as(&mut a, &pool, p);
                land(&reads, Ok(()));
                later(&pool);
                let frame = frame_of(&pool, p);
                // A step under way on another thread, entered before A's next fix marks the page.
                let (entered, entered_seen) = std::sync::mpsc::channel();
                let (end, end_seen) = std::sync::mpsc::channel::<()>();
                let stepper = {
                    let pool = pool.clone();
                    std::thread::spawn(move || {
                        let step = pool.store.epochs().enter();
                        entered.send(()).unwrap();
                        let _ = end_seen.recv();
                        drop(step);
                    })
                };
                entered_seen.recv().unwrap();
                let a_ran = ran_as(&mut a, &pool, p);
                let mut b = Owner::new();
                let b_wait = waiting_after_choice(&mut b, &pool, q, &fillers);
                let word_while_held = pool.group(frame).word();
                let waited = b_wait.as_ref().map(Completion::finished);
                end.send(()).unwrap();
                stepper.join().unwrap();
                let since = std::time::Instant::now();
                let told = b_wait.as_ref().is_some_and(|w| {
                    while !w.finished() && since.elapsed() < std::time::Duration::from_secs(5) {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    w.finished()
                });
                let b_done = drive_statement(&mut b, &pool, &reads, &[q]);
                let a_next = ran_as(&mut a, &pool, p);
                let a_revoked = a.revocations();
                land(&reads, Ok(()));
                drop((a, b));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(!a_first && a_ran, "A's load, then its run with its page");
                assert!(
                    waited == Some(false)
                        && word_while_held == group_word::word_of(false, 1, 1, 0, 0),
                    "B's claim waited, A's idle keep left while the step was under way: {waited:?}, \
                     {word_while_held:#x}"
                );
                assert!(
                    told,
                    "the step's end never woke the claim it held back from A's frame"
                );
                assert!(
                    matches!(b_done, Driven::Done) && !a_next && a_revoked == 1,
                    "then B's drain revoked A's keep: {b_done:?}, A ran {a_next}, revocations \
                     {a_revoked}"
                );
            },
        );
    }

    /// A request's retirement from a frame its load's group still keeps (SHORT-HOLD1b, the
    /// auditor's #6317): a claim passes the frame over for its record, which holds the load's
    /// request still; once the request retires, the frame's keeps are idle ordinary ones a drain
    /// revokes, and the retirement tells the claim, which goes on (mutant `retirement-silent`).
    #[test]
    fn a_requests_retirement_tells_a_claim_that_passed_a_kept_frame_over_for_its_record() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_requests_retirement_tells_a_claim_that_passed_a_kept_frame_over_for_its_record"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/retirement", false);
                let p = PageId::new(class(3), 1);
                let q = PageId::new(class(4), 1);
                let mut a = Owner::new();
                let a_first = ran_as(&mut a, &pool, p);
                land(&reads, Ok(()));
                later(&pool);
                let frame = frame_of(&pool, p);
                // The load's request still held by the frame's record, the backend yet to retire it.
                let held = pool.header(frame).hold_record(frame, class(3));
                let mut b = Owner::new();
                let b_wait = waiting_after_choice(&mut b, &pool, q, &fillers);
                let waited = b_wait.as_ref().map(Completion::finished);
                pool.give_back(held);
                let told = b_wait.as_ref().map(Completion::finished);
                let b_done = drive_statement(&mut b, &pool, &reads, &[q]);
                let a_next = ran_as(&mut a, &pool, p);
                let a_revoked = a.revocations();
                land(&reads, Ok(()));
                drop((a, b));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(!a_first, "A's call loads its page");
                assert_eq!(
                    (waited, told),
                    (Some(false), Some(true)),
                    "the retirement told the claim that passed the frame over for its record"
                );
                assert!(
                    matches!(b_done, Driven::Done) && !a_next && a_revoked == 1,
                    "then B's drain revoked A's keep: {b_done:?}, A ran {a_next}, revocations \
                     {a_revoked}"
                );
            },
        );
    }

    /// A load that lost the page table's insert to another frame (SHORT-HOLD1b, the auditor's
    /// #6325): the loser fixes for no owner, so keeps nothing; between its load's publication and
    /// its insert, another fix of the page loads it into a frame of its own and inserts it, and
    /// an owner's fix follows a hint to the loser's frame, joins its group and returns IO, idle;
    /// a claim passes the frame over for that keep and waits. The loser's close leaves the group
    /// revocable, the joiner's keep alone in it, and its free defers the frame to the group; no
    /// landing comes, and the joiner never acts again. The signal after the loser's free tells
    /// the claim (mutant `lost-insert-silent`).
    #[test]
    fn a_load_that_lost_its_insert_tells_a_claim_that_passed_its_frame_over_for_a_joiners_keep() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_load_that_lost_its_insert_tells_a_claim_that_passed_its_frame_over_for_a_joiners_keep"
            ),
            || {
                let (pool, reads) = held_reads_pool("/db/lost-insert");
                let p = PageId::new(class(3), 1);
                // What the race point's action saw: the loser's frame, the joiner's call, whether
                // the claim's pass marked the frame, and the claim's wait.
                type Seen = (u32, bool, bool, Completion);
                let seen: std::rc::Rc<std::cell::RefCell<Option<Seen>>> = std::rc::Rc::new(std::cell::RefCell::new(None));
                let joiner: std::rc::Rc<std::cell::RefCell<Owner>> = std::rc::Rc::new(std::cell::RefCell::new(Owner::new()));
                let (weak, reads_in, seen_in, joiner_in) = (
                    Arc::downgrade(&pool),
                    reads.clone(),
                    seen.clone(),
                    joiner.clone(),
                );
                crate::race::at("buffer.load_published", move || {
                    let pool = weak.upgrade().expect("the pool outlives the loser's load");
                    // The winner: no frame holds the page yet, so it loads it into its own and
                    // inserts it; its read lands.
                    assert!(!fixed_unowned(&pool, p), "the winner's load");
                    land(&reads_in, Ok(()));
                    let loser = (0..pool.frames.headers.len() as u32)
                        .find(|&f| {
                            let h = pool.header(f);
                            h.pid() == p.raw() && h.status() == LOADING
                        })
                        .expect("the loser's frame, published and loading");
                    // The joiner: a hint to the loser's frame, whose load it waits for, keeping it.
                    let hint = std::sync::atomic::AtomicU32::new(loser + 1);
                    let out = joiner_in.borrow_mut().call(|| {
                        Ok(match pool.fix_optimistic(p, Some(&hint))? {
                            IOResult::Done(_) => IOResult::Done(()),
                            IOResult::IO(io) => IOResult::IO(io),
                        })
                    });
                    // The claim: its ticket before its look, its pass over the frame, its wait.
                    let ticket = pool.ticket();
                    let mut room = Room::default();
                    pool.pass_over_kept(loser, &mut None, &mut room);
                    let wait = pool.supply_wait(ticket);
                    *seen_in.borrow_mut() =
                        Some((loser, matches!(out, Ok(IOResult::IO(_))), room.marked, wait));
                });
                let loser_fixed = fixed_unowned(&pool, p);
                let (loser, joiner_waited, marked, wait) =
                    seen.borrow_mut().take().expect("the race point's action ran");
                let told = wait.finished();
                let word = pool.group(loser).word();
                let freed = pool.header(loser).status() == FREE;
                drop(wait);
                drop(std::rc::Rc::try_unwrap(joiner).ok().expect("the joiner").into_inner());
                drop((pool, reads));
                assert!(
                    loser_fixed && joiner_waited && marked,
                    "the loser found the winner's page, the joiner waited keeping the loser's \
                     frame, the claim marked it: {loser_fixed}, {joiner_waited}, {marked}"
                );
                assert!(
                    freed && group_word::revocable(word),
                    "the loser's close left the frame freed and its group revocable: {}",
                    group_word::describe(word)
                );
                assert!(
                    told,
                    "the lost insert's signal told the claim that passed the loser's frame over"
                );
            },
        );
    }

    /// A page source that refuses every read at its submission, for the refused read's test.
    struct RefusingSource;

    /// The location [`RefusingSource`] gives every page: its read refused.
    struct RefusingLocated;

    impl source::PageSource for RefusingSource {
        fn locate(&self, _pid: PageId) -> Result<IOResult<Box<dyn source::Located>>> {
            Ok(IOResult::Done(Box::new(RefusingLocated)))
        }
    }
    impl source::Located for RefusingLocated {
        fn read(
            self: Box<Self>,
            _into: Arc<crate::io::Buffer>,
            _done: source::ReadDone,
            _submitting: source::Submitting<'_>,
        ) -> Result<IOResult<Option<Completion>>> {
            Err(LimboError::InternalError("the read refused".to_string()))
        }
    }

    /// A read refused at its submission (SHORT-HOLD1b, the auditor's #6325, the close of a read
    /// given up before it started): between the load's publication and the submission, an
    /// owner's fix of the page joins the load's group and returns IO, idle, and a claim passes
    /// the frame over for that keep and waits; then the source refuses the read. The read,
    /// dropped inside the submission's scope, unloads the frame: its close leaves the group
    /// revocable, the joiner's keep alone in it, and its free defers the frame to the group. The
    /// scope's arm tells the claim (mutant `refused-read-unarmed`, which takes out the scope's
    /// arm and the dropped read's own).
    #[test]
    fn a_read_refused_at_submission_tells_a_claim_that_passed_its_frame_over_for_a_joiners_keep() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_read_refused_at_submission_tells_a_claim_that_passed_its_frame_over_for_a_joiners_keep"
            ),
            || {
                let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
                let store = PageStore::open(
                    crate::storage::atoll::requests::counted_for_test(io),
                    "/db/refused-read",
                    StoreConfig {
                        file_bytes: 1 << 30,
                        meta_pages: 1 << 10,
                    },
                    [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
                    0,
                );
                let pool = BufferPool::new(
                    BufferConfig::with_budget(MIN_BUDGET),
                    Arc::new(RefusingSource),
                    store,
                )
                .unwrap();
                let p = PageId::new(class(3), 1);
                // What the race point's action saw: the loading frame, the joiner's call, whether
                // the claim's pass marked the frame, and the claim's wait.
                type Seen = (u32, bool, bool, Completion);
                let seen: std::rc::Rc<std::cell::RefCell<Option<Seen>>> =
                    std::rc::Rc::new(std::cell::RefCell::new(None));
                let joiner: std::rc::Rc<std::cell::RefCell<Owner>> =
                    std::rc::Rc::new(std::cell::RefCell::new(Owner::new()));
                let (weak, seen_in, joiner_in) =
                    (Arc::downgrade(&pool), seen.clone(), joiner.clone());
                crate::race::at("buffer.read_submit", move || {
                    let pool = weak.upgrade().expect("the pool outlives the load");
                    let frame = frame_of(&pool, p);
                    // The joiner: the page found loading, its load waited for, keeping it.
                    let out = joiner_in.borrow_mut().call(|| {
                        Ok(match pool.fix_optimistic(p, None)? {
                            IOResult::Done(_) => IOResult::Done(()),
                            IOResult::IO(io) => IOResult::IO(io),
                        })
                    });
                    // The claim: its ticket before its look, its pass over the frame, its wait.
                    let ticket = pool.ticket();
                    let mut room = Room::default();
                    pool.pass_over_kept(frame, &mut None, &mut room);
                    let wait = pool.supply_wait(ticket);
                    *seen_in.borrow_mut() =
                        Some((frame, matches!(out, Ok(IOResult::IO(_))), room.marked, wait));
                });
                let refused = matches!(
                    pool.fix_optimistic(p, None),
                    Err(LimboError::InternalError(ref words)) if words == "the read refused"
                );
                let (frame, joiner_waited, marked, wait) =
                    seen.borrow_mut().take().expect("the race point's action ran");
                let told = wait.finished();
                let word = pool.group(frame).word();
                let freed = pool.header(frame).status() == FREE;
                drop(wait);
                drop(
                    std::rc::Rc::try_unwrap(joiner)
                        .ok()
                        .expect("the joiner")
                        .into_inner(),
                );
                drop(pool);
                assert!(
                    refused && joiner_waited && marked,
                    "the loader's read was refused, the joiner waited keeping the frame, the claim \
                     marked it: {refused}, {joiner_waited}, {marked}"
                );
                assert!(
                    freed && group_word::revocable(word),
                    "the refused read's close left the frame freed and its group revocable: {}",
                    group_word::describe(word)
                );
                assert!(
                    told,
                    "the refused read's scope told the claim that passed its frame over"
                );
            },
        );
    }

    /// The checkpoint's demand behind sturdy idle keeps alone (SHORT-HOLD1b, the designer's #6344),
    /// at the smallest cache, eight owners each revoked once and suspended at their reloaded pages
    /// ([`eight_sturdy`]), no read at the backend: the demand's claims are the demand's
    /// (`upkeep::claim`), which run as no owner's and note no end of the road, so it waits, not
    /// ending in the shortage error; the owner in its drain's way ends its operation, letting its
    /// keep go, which tells the demand, and the demand's next claims get its frame.
    #[test]
    fn a_checkpoints_demand_behind_a_sturdy_idle_keep_waits_and_gets_its_frame_once_it_is_let_go() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_checkpoints_demand_behind_a_sturdy_idle_keep_waits_and_gets_its_frame_once_it_is_let_go"
            ),
            || {
                let (pool, reads, mut owners) = eight_sturdy("/db/upkeep-eight");
                let mut upkeep = super::Upkeep::default();
                let first = checkpoint_claims(&pool, &mut upkeep, class(4), None);
                let waits = match &first {
                    Ok(IOResult::IO(IOCompletions(w))) => Some(w.finished()),
                    _ => None,
                };
                let first_seen = format!("{first:?}");
                let leaving: Vec<u32> = pool.memory.leaving().iter().map(|l| l.batch).collect();
                let in_the_way = owners.iter().position(|(o, _)| {
                    o.keep_seen()
                        .0
                        .is_some_and(|(f, ..)| leaving.contains(&(f / memory::GRAINS)))
                });
                // The owner in the demand's way ends its operation with its page.
                let ended = in_the_way.map(|i| {
                    let (owner, page) = &mut owners[i];
                    fixed_as(owner, &pool, *page)
                });
                let told = match &first {
                    Ok(IOResult::IO(IOCompletions(w))) => w.finished(),
                    _ => false,
                };
                let got = checkpoint_claims(&pool, &mut upkeep, class(4), None);
                let got_frame = match &got {
                    Ok(IOResult::Done(f)) => Some(*f),
                    _ => None,
                };
                drop(first);
                if let Some(f) = got_frame {
                    pool.free_frame(class(4), f, pool.take_free_latch(f));
                }
                drop((upkeep, owners));
                land(&reads, Ok(()));
                drop((pool, reads));
                assert_eq!(
                    waits,
                    Some(false),
                    "the checkpoint's demand behind the sturdy keeps waited, not failing: {first_seen}"
                );
                assert!(
                    in_the_way.is_some() && ended == Some(true),
                    "the owner in the demand's way ended its operation with its page: \
                     {in_the_way:?}, {ended:?}"
                );
                assert!(told, "its operation's end told the checkpoint's demand");
                assert!(
                    got_frame.is_some(),
                    "the checkpoint's demand got its frame once the keep was let go: {got:?}"
                );
            },
        );
    }

    /// The checkpoint's demand behind an ordinary idle keep (SHORT-HOLD1b, the designer's #6346,
    /// the auditor's chaos 127 park): the demand's drain revokes the suspended statement's keep
    /// and the checkpoint gets its frame; the statement's next call finds its keep revoked, reads
    /// its page in again, sturdy, and its call after runs with its page.
    #[test]
    fn a_checkpoints_drain_revokes_an_ordinary_idle_keep_and_the_statement_reloads_and_runs() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::a_checkpoints_drain_revokes_an_ordinary_idle_keep_and_the_statement_reloads_and_runs"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/upkeep-ordinary", false);
                let p = PageId::new(class(3), 1);
                let mut a = Owner::new();
                let a_loaded = ran_as(&mut a, &pool, p);
                land_small(&reads);
                later(&pool);
                let a_kept = a.keep_seen();
                let mut upkeep = super::Upkeep::default();
                let got = checkpoint_claims(&pool, &mut upkeep, class(4), Some(&fillers));
                let got_frame = match &got {
                    Ok(IOResult::Done(f)) => Some(*f),
                    _ => None,
                };
                // A's next calls, each read landed as the host steps, until it runs with its page.
                let a_reload = ran_as(&mut a, &pool, p);
                let a_revoked = a.revocations();
                let mut a_ran = false;
                for _ in 0..8 {
                    land_small(&reads);
                    later(&pool);
                    a_ran = ran_as(&mut a, &pool, p);
                    if a_ran {
                        break;
                    }
                }
                let a_loads = loads_of(&pool, p);
                let a_after = a.keep_seen();
                if let Some(f) = got_frame {
                    pool.free_frame(class(4), f, pool.take_free_latch(f));
                }
                drop((upkeep, a));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(
                    !a_loaded && matches!(a_kept, (Some((_, _, true, false)), None)),
                    "A's load, its keep idle and ordinary: {a_kept:?}"
                );
                assert!(
                    got_frame.is_some(),
                    "the checkpoint's demand got its frame, its drain revoking A's keep: {got:?}"
                );
                assert!(
                    !a_reload && a_revoked == 1 && a_ran && a_loads == 2,
                    "A's next call found its keep revoked and read its page in again, and a call \
                     after ran with it: reload {}, revocations {a_revoked}, ran {a_ran}, loads \
                     {a_loads}",
                    !a_reload
                );
                assert!(
                    matches!(a_after, (Some((_, _, true, true)), None)),
                    "A's page kept sturdy after its reload: {a_after:?}"
                );
            },
        );
    }

    /// Calls the checkpoint's demand's claim of a frame of `class`, as its stepper would, until
    /// it is done or comes back with a wait nothing has finished, `fillers`' reads landed after
    /// its first call if given: what the last call came to.
    fn checkpoint_claims(
        pool: &Arc<BufferPool>,
        upkeep: &mut super::Upkeep,
        class: SizeClass,
        fillers: Option<&PendingReads>,
    ) -> Result<IOResult<u32>> {
        let mut out = super::upkeep_scope(upkeep, || pool.claim(class, Purpose::New, &[]));
        if let Some(fillers) = fillers {
            land(fillers, Ok(()));
        }
        for _ in 0..8 {
            match &out {
                Ok(IOResult::IO(IOCompletions(w))) if w.finished() => {}
                _ => return out,
            }
            out = super::upkeep_scope(upkeep, || pool.claim(class, Purpose::New, &[]));
        }
        out
    }

    /// A tenure's advance that fails ends the operation (SHORT-HOLD1b): B's claim waits behind A's
    /// sturdy keep alone, C queued behind B, and once B's busy timeout passed its advance fails with
    /// the shortage error. B's tenure is ended with it, its place freed, and C, next in the queue,
    /// holds the gate (mutant `advance-error-keeps-tenure`).
    #[test]
    fn an_advance_that_fails_ends_its_operation_and_the_next_place_holds_the_gate() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::an_advance_that_fails_ends_its_operation_and_the_next_place_holds_the_gate"
            ),
            || {
                let (pool, reads, fillers) = shaped("/db/advance-fails", false);
                let p = PageId::new(class(3), 1);
                let (q, q2) = (PageId::new(class(4), 1), PageId::new(class(4), 2));
                let (b_conn, c_conn) = (Conn::new(), Conn::new());
                b_conn.set_busy(std::time::Duration::from_millis(200).as_nanos() as u64);
                c_conn.set_busy(std::time::Duration::from_secs(60).as_nanos() as u64);
                let (mut a, mut b, mut c) = (Owner::new(), Owner::of(b_conn), Owner::of(c_conn));
                let mut e = Owner::new();
                // A's page kept sturdy in the one batch, as in the turns with no spare batch.
                let a_first = ran_as(&mut a, &pool, p);
                land(&reads, Ok(()));
                later(&pool);
                let e_done = drive_statement(&mut e, &pool, &reads, &[q]);
                later(&pool);
                let a_reloads = ran_as(&mut a, &pool, p);
                land(&reads, Ok(()));
                later(&pool);
                let read = |o: &mut Owner, pid: PageId| {
                    o.call(|| {
                        Ok(match pool.fix_optimistic(pid, None)? {
                            IOResult::Done(_) => IOResult::Done(()),
                            IOResult::IO(io) => IOResult::IO(io),
                        })
                    })
                };
                // B at the head, waiting out its busy timeout behind A's sturdy keep.
                let mut b_wait = None;
                for _ in 0..8 {
                    if let Ok(IOResult::IO(IOCompletions(w))) = read(&mut b, q) {
                        if !w.finished() {
                            b_wait = Some(w);
                            break;
                        }
                    }
                }
                // C behind it, waiting for its turn.
                let c_turn = match read(&mut c, q2) {
                    Ok(IOResult::IO(IOCompletions(w))) => Some(w),
                    _ => None,
                };
                let before = (pool.shortage_len(), b.holds_gate(), c.holds_gate());
                let since = std::time::Instant::now();
                while b_wait.as_ref().is_some_and(|w| !w.finished())
                    && since.elapsed() < std::time::Duration::from_secs(5)
                {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                let b_failed = read(&mut b, q);
                let after = (
                    pool.shortage_len(),
                    b.queued(),
                    c_turn.as_ref().map(Completion::finished),
                    c.holds_gate(),
                );
                drop(b_wait);
                drop(c_turn);
                drop((b, c));
                let a_ran = ran_as(&mut a, &pool, p);
                land(&reads, Ok(()));
                drop((a, e));
                land(&fillers, Ok(()));
                drop((pool, reads, fillers));
                assert!(
                    !a_first && matches!(e_done, Driven::Done) && !a_reloads,
                    "A's load, E's statement, A's sturdy reload: {e_done:?}"
                );
                assert_eq!(
                    before,
                    (2, true, false),
                    "(places, B holds the gate, C holds the gate) as B waits"
                );
                assert!(
                    matches!(
                        b_failed,
                        Err(LimboError::CacheError(
                            crate::storage::page_cache::CacheError::TooSmall(_)
                        ))
                    ),
                    "B's advance after its busy timeout: {b_failed:?}"
                );
                assert_eq!(
                    after,
                    (1, false, Some(true), true),
                    "B's failed advance ended its tenure and handed the gate on: (places, B queued, \
                     C's turn finished, C holds the gate)"
                );
                assert!(a_ran, "A ran with its page");
            },
        );
    }

    /// A pool of the smallest configuration (`smallest_config()`: MIN_DB_BUDGET, 11 batches of 2 MiB,
    /// the log's ring and the image bank lent at open, 8 a cache claim can drain), over a source
    /// whose reads the test lands. Eight owners, each reading a page of its own class into a batch
    /// of its own, are each revoked once, by a statement of class 4 draining that batch, and
    /// suspended at their reload: eight sturdy idle keeps, one in each batch. Returns the pool, its
    /// reads and the eight owners with their pages.
    fn eight_sturdy(base: &str) -> (Arc<BufferPool>, PendingReads, Vec<(Owner, PageId)>) {
        let config = crate::storage::atoll::sql::mode::smallest_config();
        let mut buffer = config.buffer.clone();
        buffer.lent_at_open = LentAtOpen {
            ring: crate::storage::atoll::log::ring_chunks(&config.log),
            bank: bank_frames(config.checkpoint_write_bytes()),
        };
        let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
        let store = PageStore::open(
            crate::storage::atoll::requests::counted_for_test(io),
            base,
            StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
            0,
        );
        let reads: PendingReads = Arc::new(crate::storage::atoll::locks::Mutex::new(Vec::new()));
        let pool =
            BufferPool::new(buffer, Arc::new(SubmittingSource(reads.clone())), store).unwrap();
        let mut owners: Vec<(Owner, PageId)> = [0, 1, 2, 3, 5, 6, 7, 8]
            .into_iter()
            .map(|c| (Owner::new(), PageId::new(class(c), 1)))
            .collect();
        for (owner, page) in owners.iter_mut() {
            assert!(!ran_as(owner, &pool, *page), "an owner's load");
            land(&reads, Ok(()));
        }
        later(&pool);
        let mut d = Owner::new();
        for round in 0..owners.len() as u64 {
            let q = PageId::new(class(4), 100 + round);
            assert!(
                matches!(drive_statement(&mut d, &pool, &reads, &[q]), Driven::Done),
                "the drain's statement of round {round}"
            );
            assert!(pool.evict_clean(q), "its page evicted");
            later(&pool);
            // The owner it revoked, its keep's generation passed.
            let revoked = owners.iter().position(|(o, _)| {
                o.keep_seen()
                    .0
                    .is_some_and(|(f, at, _, _)| group_word::generation(pool.group(f).word()) != at)
            });
            let i = revoked.expect("an owner revoked this round");
            let (owner, page) = &mut owners[i];
            assert!(!ran_as(owner, &pool, *page), "the revoked owner's reload");
            land(&reads, Ok(()));
            later(&pool);
        }
        for (owner, _) in &owners {
            assert!(
                matches!(owner.keep_seen(), (Some((_, 1, true, true)), None)),
                "an owner's keep, revoked once, reloaded sturdy and idle: {:?}",
                owner.keep_seen()
            );
        }
        let batches: std::collections::BTreeSet<u32> = owners
            .iter()
            .filter_map(|(o, _)| o.keep_seen().0.map(|(f, ..)| f / memory::GRAINS))
            .collect();
        assert_eq!(batches.len(), 8, "a sturdy keep in each of eight batches");
        (pool, reads, owners)
    }

    /// The smallest configuration's end of the road (SHORT-HOLD1b, A3, the auditor's #6311): eight
    /// sturdy idle keeps, one in each batch a cache claim can drain, and a ninth claim that needs a
    /// batch: it waits its busy timeout, 200 ms, and ends in `TooSmall`, its words naming the
    /// class wanted, the batch and the sturdy keep.
    #[test]
    fn at_the_smallest_cache_eight_sturdy_keeps_end_a_ninth_claim_in_too_small() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::at_the_smallest_cache_eight_sturdy_keeps_end_a_ninth_claim_in_too_small"
            ),
            || {
                let (pool, reads, owners) = eight_sturdy("/db/default-eight");
                let conn = Conn::new();
                let bound = std::time::Duration::from_millis(200);
                conn.set_busy(bound.as_nanos() as u64);
                let mut ninth = Owner::of(conn);
                let since = std::time::Instant::now();
                let outcome =
                    drive_statement(&mut ninth, &pool, &reads, &[PageId::new(class(4), 1)]);
                let elapsed = since.elapsed();
                let lent = (0..pool.memory.batches)
                    .filter(|&b| pool.memory.is_lend_batch(b))
                    .count();
                let batches = pool.memory.batches;
                drop(ninth);
                drop(owners);
                land(&reads, Ok(()));
                drop((pool, reads));
                assert_eq!(
                    (batches, lent),
                    (11, 3),
                    "the smallest pool: (batches, lent at open)"
                );
                assert!(
                    matches!(&outcome, Driven::Failed(LimboError::CacheError(
                        crate::storage::page_cache::CacheError::TooSmall(e)
                    )) if e.starts_with(
                        "the page cache is too small for the statements open: a frame of class 4 \
                         (64 KiB) for a read waits on batch "
                    ) && e.ends_with(", held by statements between their steps; sturdy keeps: 1")),
                    "the ninth claim: {outcome:?}"
                );
                assert!(
                    elapsed >= bound && elapsed < bound * 10,
                    "the error came after {elapsed:?}, against a busy timeout of {bound:?}"
                );
            },
        );
    }

    /// [`at_the_smallest_cache_eight_sturdy_keeps_end_a_ninth_claim_in_too_small`] with the owner in
    /// the ninth claim's way run to its operation's end while the claim waits: its keep, sturdy to
    /// that end (the mark carried on, the auditor's #6341), is released there, which frees the
    /// frame and tells the claim; the claim gets its frame.
    #[test]
    fn at_the_smallest_cache_one_of_eight_sturdy_keepers_ended_gives_the_ninth_claim_its_frame() {
        crate::storage::atoll::testing::cleanly(
            concat!(
                module_path!(),
                "::at_the_smallest_cache_one_of_eight_sturdy_keepers_ended_gives_the_ninth_claim_its_frame"
            ),
            || {
                let (pool, reads, mut owners) = eight_sturdy("/db/default-eight-ended");
                let conn = Conn::new();
                conn.set_busy(std::time::Duration::from_secs(60).as_nanos() as u64);
                let mut ninth = Owner::of(conn);
                let wanted = PageId::new(class(4), 1);
                let none: PendingReads =
                    Arc::new(crate::storage::atoll::locks::Mutex::new(Vec::new()));
                let wait = waiting_after_choice(&mut ninth, &pool, wanted, &none);
                let leaving: Vec<u32> = pool.memory.leaving().iter().map(|l| l.batch).collect();
                let in_the_way = owners.iter().position(|(o, _)| {
                    o.keep_seen()
                        .0
                        .is_some_and(|(f, ..)| leaving.contains(&(f / memory::GRAINS)))
                });
                let ended = in_the_way.map(|i| {
                    let (owner, page) = &mut owners[i];
                    fixed_as(owner, &pool, *page)
                });
                let told = wait.as_ref().map(Completion::finished);
                let outcome = drive_statement(&mut ninth, &pool, &reads, &[wanted]);
                let revoked: Vec<u64> = owners.iter().map(|(o, _)| o.revocations()).collect();
                drop(wait);
                drop(ninth);
                drop(owners);
                land(&reads, Ok(()));
                drop((pool, reads));
                assert!(
                    in_the_way.is_some() && ended == Some(true),
                    "the owner in the ninth claim's way ended its operation with its page: \
                     {in_the_way:?}, {ended:?}"
                );
                assert_eq!(told, Some(true), "its operation's end told the waiting claim");
                assert!(
                    matches!(outcome, Driven::Done),
                    "the ninth claim: {outcome:?}"
                );
                assert_eq!(
                    revoked.iter().filter(|&&n| n == 1).count(),
                    8,
                    "each owner revoked once before the ninth: {revoked:?}"
                );
            },
        );
    }

    /// SHORT-HOLD1b's cost probes (COST-PROBES.md), measurement only, each `#[ignore]`d and run
    /// alone: instructions in user space over this thread, through the harness's counter and
    /// `measure` (cost_probe.rs), with this thread's allocations (`testing::allocations`, which the
    /// base has too). The same text builds on c0fe4b832, but for the one probe marked packet only.
    #[cfg(target_os = "linux")]
    mod cost {
        use super::*;

        /// A user-space instruction counter over this thread and the threads it starts after it
        /// opens.
        struct Instructions(i32);

        impl Instructions {
            fn start() -> Self {
                let mut attr = [0u64; 8];
                // type PERF_TYPE_HARDWARE (0), size 64 (VER0); config PERF_COUNT_HW_INSTRUCTIONS.
                attr[0] = 64 << 32;
                attr[1] = 1;
                // read_format: TOTAL_TIME_ENABLED | TOTAL_TIME_RUNNING.
                attr[4] = 1 | 2;
                // disabled, inherit, exclude_kernel, exclude_hv.
                attr[5] = 1 | 2 | (1 << 5) | (1 << 6);
                // SAFETY: a perf_event_attr of VER0's 64 bytes, for this thread, any CPU.
                let fd = unsafe {
                    libc::syscall(libc::SYS_perf_event_open, attr.as_ptr(), 0, -1, -1, 0)
                } as i32;
                assert!(
                    fd >= 0,
                    "perf_event_open: {}",
                    std::io::Error::last_os_error()
                );
                for (op, what) in [(0x2403, "RESET"), (0x2400, "ENABLE")] {
                    // SAFETY: an ioctl on the counter's own descriptor.
                    let r = unsafe { libc::ioctl(fd, op, 0) };
                    assert_eq!(r, 0, "{what}: {}", std::io::Error::last_os_error());
                }
                Self(fd)
            }

            /// (instructions, time enabled, time running).
            fn stop(self) -> (u64, u64, u64) {
                // SAFETY: an ioctl on the counter's own descriptor.
                let r = unsafe { libc::ioctl(self.0, 0x2401, 0) };
                assert_eq!(r, 0, "DISABLE: {}", std::io::Error::last_os_error());
                let mut v = [0u64; 3];
                // SAFETY: reads 24 bytes into `v`, which holds 24.
                let n = unsafe { libc::read(self.0, v.as_mut_ptr() as *mut libc::c_void, 24) };
                // SAFETY: the counter's own descriptor, closed once.
                unsafe { libc::close(self.0) };
                assert_eq!(n, 24, "read: {n}, {}", std::io::Error::last_os_error());
                assert!(v[0] > 0, "a zero instruction count");
                assert!(
                    v[1] > 0 && v[1] == v[2],
                    "the counter multiplexed: enabled {} running {}",
                    v[1],
                    v[2]
                );
                (v[0], v[1], v[2])
            }
        }

        /// Counts `f`, `ops` operations, and prints the exact totals.
        fn measure(label: &str, ops: u64, f: impl FnOnce()) {
            let a = crate::storage::atoll::testing::allocations();
            let t = std::time::Instant::now();
            let i = Instructions::start();
            f();
            let (n, enabled, running) = i.stop();
            let ns = t.elapsed().as_nanos();
            let a = crate::storage::atoll::testing::allocations() - a;
            println!(
                "COST {label} ops={ops} instructions={n} enabled={enabled} running={running} \
                 allocs={a} ns={ns}"
            );
        }

        /// One call of an operation of `owner`'s that fixes `pid`, a hit, and returns a yield,
        /// keeping its page: what an operation that runs with its page and goes on later does.
        fn hit_and_yield(owner: &mut Owner, pool: &Arc<BufferPool>, pid: PageId) {
            let out = owner.call(|| {
                Ok(match pool.fix_optimistic(pid, None)? {
                    IOResult::Done(_) => IOResult::<()>::IO(IOCompletions(Completion::new_yield())),
                    IOResult::IO(io) => IOResult::IO(io),
                })
            });
            assert!(matches!(out, Ok(IOResult::IO(_))), "the probe's call");
        }

        /// keep-io-return: one owner's calls, each a miss that loads a page of its own and
        /// returns IO with its read pending, the page kept: N of them. The reads are landed after
        /// the count.
        #[test]
        #[ignore]
        fn sh1b_cost_keep_io_return() {
            const N: u64 = 2_000;
            let (pool, reads) = held_reads_pool("/db/cost-keep-io-return");
            let mut owner = Owner::new();
            measure("keep-io-return", N, || {
                for n in 1..=N {
                    assert!(!fixed_as(&mut owner, &pool, PageId::new(class(0), n)));
                }
            });
            drop(owner);
            land(&reads, Ok(()));
        }

        /// keep-next-entry: one owner whose keep is valid and idle, called N times with an
        /// operation that fixes its page, a hit, and returns a yield: each call enters with the
        /// keep idle and leaves it idle again.
        #[test]
        #[ignore]
        fn sh1b_cost_keep_next_entry() {
            const N: u64 = 10_000;
            let (pool, reads) = held_reads_pool("/db/cost-keep-next-entry");
            let pid = PageId::new(class(0), 1);
            let mut owner = Owner::new();
            assert!(!fixed_as(&mut owner, &pool, pid), "the page's load");
            land(&reads, Ok(()));
            hit_and_yield(&mut owner, &pool, pid);
            measure("keep-next-entry", N, || {
                for _ in 0..N {
                    hit_and_yield(&mut owner, &pool, pid);
                }
            });
            drop(owner);
            land(&reads, Ok(()));
        }

        /// keep-next-entry's split form (packet only: the base has no keep states): a call's entry
        /// and its IO return alone, N times, the keep idle at each entry.
        #[test]
        #[ignore]
        fn sh1b_cost_keep_entry_and_return() {
            const N: u64 = 10_000;
            let (pool, reads) = held_reads_pool("/db/cost-keep-entry-return");
            let pid = PageId::new(class(0), 1);
            let mut owner = Owner::new();
            assert!(!fixed_as(&mut owner, &pool, pid), "the page's load");
            land(&reads, Ok(()));
            hit_and_yield(&mut owner, &pool, pid);
            measure("keep-entry-and-return", N, || {
                for _ in 0..N {
                    owner.enter_for_test();
                    owner.return_io_for_test();
                }
            });
            drop(owner);
            land(&reads, Ok(()));
        }

        /// drain-weighs: a drain's weighing of one class-0 batch whose 512 frames hold resident
        /// pages no one keeps, N times.
        #[test]
        #[ignore]
        fn sh1b_cost_drain_weighs() {
            const N: u64 = 1_000;
            let (pool, reads) = held_reads_pool("/db/cost-drain-weighs");
            let per = memory::frames_per_batch(class(0)) as u64;
            for n in 1..=per {
                assert!(!fixed_unowned(&pool, PageId::new(class(0), n)));
            }
            land(&reads, Ok(()));
            let batch = frame_of(&pool, PageId::new(class(0), 1)) / memory::GRAINS;
            for n in 1..=per {
                assert_eq!(
                    frame_of(&pool, PageId::new(class(0), n)) / memory::GRAINS,
                    batch,
                    "every page in the one batch"
                );
            }
            let horizon = pool.store.epochs().horizon();
            let mut held = 0;
            measure("drain-weighs", N, || {
                for _ in 0..N {
                    held += pool.drain_cost(class(1), batch, horizon).0;
                }
            });
            assert_eq!(held, 0, "a page held");
            drop((pool, reads));
        }

        /// lookup-no-io: a fix of a resident page no one keeps, N times outside any owner, and N
        /// times as an owner's call that is done.
        #[test]
        #[ignore]
        fn sh1b_cost_lookup_no_io() {
            const N: u64 = 100_000;
            let (pool, reads) = held_reads_pool("/db/cost-lookup-no-io");
            let pid = PageId::new(class(0), 1);
            assert!(!fixed_unowned(&pool, pid), "the page's load");
            land(&reads, Ok(()));
            measure("lookup-no-io", N, || {
                for _ in 0..N {
                    assert!(fixed_unowned(&pool, pid));
                }
            });
            let mut owner = Owner::new();
            measure("lookup-no-io-owned", N, || {
                for _ in 0..N {
                    assert!(fixed_as(&mut owner, &pool, pid));
                }
            });
            drop(owner);
            drop((pool, reads));
        }
    }
}
