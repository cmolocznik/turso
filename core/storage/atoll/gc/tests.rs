//! The lists of retired values and their collector, on a database of their own: each test's body
//! returns the words of its first failed check, and the test fails only once the database is
//! dropped.

use super::collect::{Collector, Due};
use super::list::{Lists, Reader, PER_PAGE};
use super::Writers;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::value::ValueRef;
use crate::storage::atoll::btree::{Mode, Put, StoredValue, Tree};
use crate::storage::atoll::buffer::{BufferConfig, MIN_DB_BUDGET};
use crate::storage::atoll::db::{Atoll, AtollConfig, Opening};
use crate::storage::atoll::drive;
use crate::storage::atoll::locks::Mutex;
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::page::{PageId, SizeClass};
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::txn::Ts;
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};
use std::collections::{BTreeSet, HashMap};

type Words<T> = std::result::Result<T, String>;

macro_rules! ensure {
    ($cond:expr, $($words:tt)+) => {
        if !$cond {
            return Err(format!($($words)+));
        }
    };
}

const SLOTS: usize = 4;

/// The lists' items, written one by one by their slot's transaction and read back in order,
/// across pages; a give-back frees the pages wholly behind a position, never the last, and a read
/// behind it is refused (S2).
#[test]
fn a_slots_items_read_back_in_order_across_its_pages_and_a_give_back_frees_whole_pages() {
    with_db(MIN_DB_BUDGET * 4, |io, db| {
        let (lists, writers) = (Lists::new(db.pool().clone(), SLOTS), Fake::default());
        writers.begin(0, 10);
        writers.begin(1, 20);
        let n = 2 * PER_PAGE + 48;
        for i in 0..n {
            append(io, &lists, &writers, 0, 10, reference(i))?;
            if i % 50 == 0 {
                append(io, &lists, &writers, 1, 20, reference(1_000 + i))?;
            }
        }
        ensure!(lists.end(0) == n && lists.end(1) == 5, "the ends");
        let read = read_all(io, &lists, 0, 0)?;
        ensure!(read.len() as u64 == n, "{} items read of {n}", read.len());
        for (i, item) in read.iter().enumerate() {
            let i = i as u64;
            ensure!(
                item.start == 10 && item.position == i && item.reference == reference(i),
                "item {i}: {item:?}"
            );
        }
        let other = read_all(io, &lists, 1, 0)?;
        ensure!(
            other.iter().map(|i| i.reference).collect::<Vec<_>>()
                == (0..5)
                    .map(|k| reference(1_000 + 50 * k))
                    .collect::<Vec<_>>(),
            "the other slot's items: {other:?}"
        );
        let pages = lists.pages(io, 0).map_err(|e| e.to_string())?;
        ensure!(pages.len() == 3, "{} pages for {n} items", pages.len());
        let given = go(io, "give back", || lists.give_back(0, PER_PAGE + 10))?;
        ensure!(
            given == 1,
            "{given} pages given back behind {}",
            PER_PAGE + 10
        );
        let given = go(io, "give back", || lists.give_back(0, n))?;
        let left = lists.pages(io, 0).map_err(|e| e.to_string())?;
        ensure!(
            given == 1 && left == pages[2..],
            "behind the end: {given} given, {left:?} left of {pages:?}"
        );
        let none = go(io, "give back", || lists.give_back(0, u64::MAX))?;
        let left = lists.pages(io, 0).map_err(|e| e.to_string())?;
        ensure!(
            none == 0 && left == pages[2..],
            "past every item: {none} given, the last page kept: {left:?}"
        );
        let behind = drive(io, || lists.read(0, &mut Reader::from(0)));
        ensure!(
            matches!(behind, Err(LimboError::Corrupt(_))),
            "a read behind the give-back: {behind:?}"
        );
        let kept = read_all(io, &lists, 0, 2 * PER_PAGE)?;
        ensure!(kept.len() == 48, "{} items on the last page", kept.len());
        Ok(())
    });
}

/// An item written again before it is published lands on its own place; a statement rolled back
/// cuts the list back, its pages past the cut given back, and the next item goes where the cut
/// left the end (S2).
#[test]
fn an_item_written_again_lands_on_its_place_and_a_cut_back_drops_the_tail() {
    with_db(MIN_DB_BUDGET * 4, |io, db| {
        let (lists, writers) = (Lists::new(db.pool().clone(), SLOTS), Fake::default());
        writers.begin(2, 7);
        go(io, "append", || lists.append(2, 7, reference(1), &writers))?;
        go(io, "append", || lists.append(2, 7, reference(2), &writers))?;
        lists.publish(2, 7);
        let read = read_all(io, &lists, 2, 0)?;
        ensure!(
            read.len() == 1 && read[0].reference == reference(2),
            "written twice, published once: {read:?}"
        );
        for i in 3..3 + 2 * PER_PAGE {
            append(io, &lists, &writers, 2, 7, reference(i))?;
        }
        ensure!(
            lists.pages(io, 2).map_err(|e| e.to_string())?.len() == 3,
            "three pages"
        );
        go(io, "cut back", || lists.cut_back(2, 50))?;
        let pages = lists.pages(io, 2).map_err(|e| e.to_string())?;
        ensure!(
            lists.end(2) == 50 && pages.len() == 1,
            "cut back to 50: end {}, {} pages",
            lists.end(2),
            pages.len()
        );
        append(io, &lists, &writers, 2, 7, reference(99))?;
        let read = read_all(io, &lists, 2, 49)?;
        ensure!(
            read.iter()
                .map(|i| (i.position, i.reference))
                .collect::<Vec<_>>()
                == [(49, reference(51)), (50, reference(99))],
            "after the cut: {read:?}"
        );
        Ok(())
    });
}

/// The order the checkpoint's stop rule rests on, held at the append: an item's writer is its
/// slot's running transaction, and the item before it is that writer's or an earlier one's
/// (the auditor's #6186, item 2).
#[test]
fn an_append_out_of_its_slots_order_is_refused() {
    with_db(MIN_DB_BUDGET * 4, |io, db| {
        let (lists, writers) = (Lists::new(db.pool().clone(), SLOTS), Fake::default());
        let idle = drive(io, || lists.append(0, 5, reference(1), &writers));
        ensure!(
            matches!(idle, Err(LimboError::InternalError(_))),
            "an item of a transaction not running: {idle:?}"
        );
        writers.begin(0, 20);
        append(io, &lists, &writers, 0, 20, reference(2))?;
        writers.begin(0, 15);
        let earlier = drive(io, || lists.append(0, 15, reference(3), &writers));
        ensure!(
            matches!(earlier, Err(LimboError::InternalError(_))),
            "an item of a transaction that began before the last item's: {earlier:?}"
        );
        writers.begin(0, 30);
        append(io, &lists, &writers, 0, 30, reference(4))?;
        ensure!(lists.end(0) == 2, "two items kept");
        Ok(())
    });
}

/// A step frees the values of the items whose writers committed at or below its cutoff, from the
/// head, and stops at the first it does not cover; the due word tells a begin's look whether
/// there is work: a commit below the cutoff, a list grown past where it was freed, or a writer no
/// longer running (S3).
#[test]
fn a_step_frees_the_items_its_cutoff_covers_and_stops_at_the_first_it_does_not() {
    with_db(MIN_DB_BUDGET * 4, |io, db| {
        let (lists, collector, writers) = parts(db);
        let values = retired(io, db, 4)?;
        writers.begin(0, 10);
        append(io, &lists, &writers, 0, 10, values[0].0)?;
        append(io, &lists, &writers, 0, 10, values[1].0)?;
        writers.commit(0, 10, 11);
        writers.begin(0, 12);
        append(io, &lists, &writers, 0, 12, values[2].0)?;
        writers.commit(0, 12, 20);
        writers.begin(0, 21);
        append(io, &lists, &writers, 0, 21, values[3].0)?;
        let freed = |io: &Arc<dyn IO>| -> Words<Vec<bool>> {
            let mut out = Vec::new();
            for (_, pages) in &values {
                out.push(pages_free(io, db, pages)?);
            }
            Ok(out)
        };
        let has = |cutoff| collector.has_work(0, cutoff, &lists, &writers);
        ensure!(has(15), "a list grown past where it was freed");
        let n = step(io, db, &collector, &lists, &writers, 15)?;
        let after = freed(io)?;
        ensure!(
            n == 2 && after == [true, true, false, false],
            "under 15: {n} freed, {after:?}"
        );
        ensure!(
            collector.due(0) == Due::Commit(20),
            "{:?}",
            collector.due(0)
        );
        ensure!(!has(15) && has(20), "the due word against 15 and 20");
        let n = step(io, db, &collector, &lists, &writers, 20)?;
        let after = freed(io)?;
        ensure!(
            n == 1 && after == [true, true, true, false] && collector.due(0) == Due::Pending(21),
            "under 20: {n} freed, {after:?}, {:?}",
            collector.due(0)
        );
        ensure!(!has(100), "a running writer's item has no work");
        writers.commit(0, 21, 30);
        ensure!(has(25), "a writer no longer running is looked at");
        let n = step(io, db, &collector, &lists, &writers, 25)?;
        ensure!(
            n == 0 && collector.due(0) == Due::Commit(30) && !has(25),
            "under 25: {n} freed, {:?}",
            collector.due(0)
        );
        let n = step(io, db, &collector, &lists, &writers, 30)?;
        let after = freed(io)?;
        ensure!(
            n == 1 && after == [true; 4] && collector.due(0) == Due::Empty(4) && !has(30),
            "under 30: {n} freed, {after:?}, {:?}",
            collector.due(0)
        );
        ensure!(
            collector.head(0) == (4, 0),
            "the head {:?}",
            collector.head(0)
        );
        Ok(())
    });
}

/// A page's free and the head's progress are one change inside the store's change gate: a cut
/// trying to close the gate between the two finds a change inside, so it finds both or neither.
#[test]
fn a_pages_free_and_the_heads_progress_are_one_change_inside_the_gate() {
    with_db(MIN_DB_BUDGET * 4, |io, db| {
        let (lists, collector, writers) = parts(db);
        let values = retired(io, db, 1)?;
        writers.begin(1, 10);
        append(io, &lists, &writers, 1, 10, values[0].0)?;
        writers.commit(1, 10, 11);
        let inside = Arc::new(Mutex::new(None));
        let (seen, store) = (inside.clone(), db.store().clone());
        crate::race::at("gc.page_freed", move || {
            *seen.lock() = Some(matches!(store.close_gate(), IOResult::IO(_)));
            store.open_gate();
        });
        let n = step(io, db, &collector, &lists, &writers, 11)?;
        let found = *inside.lock();
        ensure!(
            found == Some(true),
            "a cut between the free and its progress found a change inside: {found:?}"
        );
        ensure!(
            n == 1 && collector.head(1) == (1, 0) && pages_free(io, db, &values[0].1)?,
            "after the step: {n} freed, head {:?}",
            collector.head(1)
        );
        Ok(())
    });
}

/// A value of several pages is freed a page a step, the head's progress recorded at each, and a
/// step that begins with no free under way goes on from the recorded progress (S3).
#[test]
fn a_chunked_values_pages_are_freed_a_step_at_a_time_from_the_heads_progress() {
    with_db(64 << 20, |io, db| {
        let (lists, collector, writers) = parts(db);
        let (reference, pages) = retired_chunked(io, db)?;
        writers.begin(0, 10);
        append(io, &lists, &writers, 0, 10, reference)?;
        writers.commit(0, 10, 11);
        let one = |collector: &Collector| {
            go(io, "step", || {
                collector.step(0, 11, 1, &lists, db.pool(), db.store(), &writers)
            })
        };
        ensure!(one(&collector)? == 1, "one page a step");
        ensure!(
            collector.head(0) == (0, 1),
            "after one: {:?}",
            collector.head(0)
        );
        let freed: Vec<bool> = pages
            .iter()
            .map(|&p| pages_free(io, db, &[p]))
            .collect::<Words<_>>()?;
        ensure!(freed == [false, true, false], "the first chunk: {freed:?}");
        ensure!(one(&collector)? == 1, "one page a step");
        ensure!(
            collector.head(0) == (0, 2),
            "after two: {:?}",
            collector.head(0)
        );
        ensure!(one(&collector)? == 1, "one page a step");
        ensure!(
            collector.head(0) == (1, 0) && pages_free(io, db, &pages)?,
            "after three: {:?}",
            collector.head(0)
        );
        Ok(())
    });
}

/// A step whose list's first page went to the temporary file reads it back in as any page is,
/// holding no page guard of any tree and not inside the change gate as it reads, and frees what
/// the page names (the auditor's #6186, item 4).
// Its pool helpers are built outside shuttle and loom alone.
#[cfg(not(any(shuttle, atoll_loom, miri)))]
#[test]
fn a_step_reads_its_lists_first_page_back_from_the_temporary_file_holding_nothing() {
    with_db(MIN_DB_BUDGET, |io, db| {
        let (lists, collector, writers) = parts(db);
        let values = retired(io, db, 2)?;
        writers.begin(3, 10);
        append(io, &lists, &writers, 3, 10, values[0].0)?;
        append(io, &lists, &writers, 3, 10, values[1].0)?;
        writers.commit(3, 10, 11);
        let first = lists.pages(io, 3).map_err(|e| e.to_string())?[0];
        // Three pools' worth of temporary pages written after it push it out.
        for _ in 0..3 * MIN_DB_BUDGET / SizeClass::new(0).unwrap().size() {
            let mut page = go(io, "a temporary page", || {
                db.pool().allocate_temporary(SizeClass::new(0).unwrap())
            })?;
            page.changed();
        }
        let loaded = |pid: PageId| db.pool().loaded_pages().contains(&pid.raw());
        ensure!(
            !loaded(first),
            "the list's first page read in before the step"
        );
        let held = Arc::new(Mutex::new(None));
        let seen = held.clone();
        crate::race::at("gc.list_read", move || {
            *seen.lock() = Some(crate::storage::atoll::park::holds::count());
        });
        let n = step(io, db, &collector, &lists, &writers, 11)?;
        let holds = *held.lock();
        ensure!(loaded(first), "the list's first page was not read back in");
        ensure!(holds == Some(0), "holds as the page was read: {holds:?}");
        ensure!(
            n == 2 && pages_free(io, db, &values[0].1)? && pages_free(io, db, &values[1].1)?,
            "{n} freed"
        );
        Ok(())
    });
}

/// A writer no longer running whose items are still in the list, with no commit published, is
/// an error, not a slot that never frees (the designer's note on #6184).
#[test]
fn a_writer_gone_with_no_commit_published_fails_the_step() {
    with_db(MIN_DB_BUDGET * 4, |io, db| {
        let (lists, collector, writers) = parts(db);
        let values = retired(io, db, 1)?;
        writers.begin(0, 10);
        append(io, &lists, &writers, 0, 10, values[0].0)?;
        let n = step(io, db, &collector, &lists, &writers, 100)?;
        ensure!(n == 0, "a running writer's item freed");
        writers.vanish(0);
        ensure!(
            collector.has_work(0, 100, &lists, &writers),
            "a writer gone is looked at"
        );
        let out = drive(io, || {
            collector.step(0, 100, 8, &lists, db.pool(), db.store(), &writers)
        });
        ensure!(
            matches!(out, Err(LimboError::InternalError(_))),
            "the step: {out:?}"
        );
        Ok(())
    });
}

/// A due word naming a writer that rolled back, its place in the list taken by the slot's next
/// transaction's item, is stored anew: the head item, read again after the running word's load, is
/// another writer's, which is no fault (the designer's #6192, gap 1).
#[test]
fn a_due_word_naming_a_writer_gone_by_rollback_is_stored_anew() {
    with_db(MIN_DB_BUDGET * 4, |io, db| {
        let parts = Arc::new(parts(db));
        let (lists, collector, writers) = &*parts;
        let values = retired(io, db, 2)?;
        writers.begin(0, 10);
        append(io, lists, writers, 0, 10, values[0].0)?;
        let n = step(io, db, collector, lists, writers, 100)?;
        ensure!(
            n == 0 && collector.due(0) == Due::Pending(10),
            "a running writer: {n} freed, {:?}",
            collector.due(0)
        );
        // Between the step's read of the head and its load of the running word: the writer rolls
        // back, and the slot's next transaction appends at the same place.
        let failed = Arc::new(Mutex::new(None));
        let (io_, parts_, failed_, next) = (io.clone(), parts.clone(), failed.clone(), values[1].0);
        crate::race::at("gc.head_read", move || {
            let (lists, _, writers) = &*parts_;
            let out = drive(&io_, || lists.cut_back(0, 0)).and_then(|()| {
                writers.vanish(0);
                writers.begin(0, 20);
                drive(&io_, || lists.append(0, 20, next, writers))
            });
            match out {
                Ok(()) => lists.publish(0, 20),
                Err(e) => *failed_.lock() = Some(e.to_string()),
            }
        });
        let n = step(io, db, collector, lists, writers, 100)?;
        let failed = failed.lock().clone();
        ensure!(
            failed.is_none(),
            "the rollback and the next append: {failed:?}"
        );
        ensure!(
            n == 0 && collector.due(0) == Due::Pending(20),
            "after the rollback: {n} freed, {:?}",
            collector.due(0)
        );
        writers.commit(0, 20, 21);
        let n = step(io, db, collector, lists, writers, 100)?;
        ensure!(
            n == 1 && pages_free(io, db, &values[1].1)? && !pages_free(io, db, &values[0].1)?,
            "the next writer's value freed, the rolled-back one's kept: {n} freed"
        );
        Ok(())
    });
}

/// Two collectors step one slot at once while its writer appends and commits, on real threads:
/// the claim lets one step at a time, each page is freed once (a second free of one is refused as
/// corrupt), and every value is freed in the end.
#[cfg(not(shuttle))]
#[test]
fn two_collectors_and_a_writer_on_one_slot_free_each_value_once() {
    for seed in 0..20 {
        collectors_and_a_writer(seed, 24);
    }
}

#[cfg(shuttle)]
#[test]
fn two_collectors_and_a_writer_on_one_slot_free_each_value_once_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || collectors_and_a_writer(crate::storage::atoll::testing::workload_seed(), 6),
        300,
    );
}

/// Steps a collector takes, at most, once the writer is done.
const STEPS_AFTER_THE_WRITER: u32 = 20_000;

fn collectors_and_a_writer(seed: u64, values: usize) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Arc::new(open(&io, MIN_DB_BUDGET * 4));
    let retired = retired(&io, &db, values).expect("the values retired");
    let parts = Arc::new(parts(&db));
    let failed = Arc::new(Mutex::new(Vec::<String>::new()));
    let done = Arc::new(crate::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (io, parts, failed, done) = (io.clone(), parts.clone(), failed.clone(), done.clone());
        let references: Vec<_> = retired.iter().map(|(r, _)| *r).collect();
        crate::thread::spawn(move || {
            let (lists, _, writers) = &*parts;
            for (t, chunk) in references.chunks(1 + (seed as usize % 3)).enumerate() {
                let start = 10 * (t as Ts + 1);
                writers.begin(0, start);
                for &r in chunk {
                    if let Err(e) = append(&io, lists, writers, 0, start, r) {
                        failed.lock().push(e);
                    }
                }
                writers.commit(0, start, start + 5);
            }
            done.store(true, crate::sync::atomic::Ordering::Release);
        })
    };
    let collectors: Vec<_> = (0..2)
        .map(|_| {
            let (io, db, parts, failed, done) = (
                io.clone(),
                db.clone(),
                parts.clone(),
                failed.clone(),
                done.clone(),
            );
            crate::thread::spawn(move || {
                let (lists, collector, writers) = &*parts;
                // Steps while the writer runs, and a bounded number after: what is left unfreed
                // then is reported below.
                let mut after = 0;
                while after < STEPS_AFTER_THE_WRITER {
                    let finished = done.load(crate::sync::atomic::Ordering::Acquire);
                    if let Err(e) = drive(&io, || {
                        collector.step(0, Ts::MAX >> 2, 2, lists, db.pool(), db.store(), writers)
                    }) {
                        failed.lock().push(e.to_string());
                        return;
                    }
                    if finished && collector.head(0).0 == lists.end(0) {
                        return;
                    }
                    after += finished as u32;
                    crate::thread::yield_now();
                }
            })
        })
        .collect();
    writer.join().unwrap();
    for c in collectors {
        c.join().unwrap();
    }
    let mut words = failed.lock().clone();
    for (r, pages) in &retired {
        match pages_free(&io, &db, pages) {
            Ok(true) => {}
            Ok(false) => words.push(format!("{r:?} not freed")),
            Err(e) => words.push(e),
        }
    }
    drop(parts);
    drop(db);
    assert!(words.is_empty(), "seed {seed}: {}", words.join("; "));
}

/// What the lists and the collector read of the transactions, set by the test: the starts running
/// in each slot and the commits published.
#[derive(Default)]
struct Fake {
    running: Mutex<HashMap<u16, Ts>>,
    commits: Mutex<HashMap<(u16, Ts), Ts>>,
}

impl Writers for Fake {
    fn running(&self, slot: u16, start: Ts) -> bool {
        self.running.lock().get(&slot) == Some(&start)
    }

    fn commit_of(&self, slot: u16, start: Ts) -> Option<Ts> {
        self.commits.lock().get(&(slot, start)).copied()
    }
}

impl Fake {
    fn begin(&self, slot: u16, start: Ts) {
        self.running.lock().insert(slot, start);
    }

    /// The pair published, then the slot let go, as a commit does.
    fn commit(&self, slot: u16, start: Ts, ts: Ts) {
        self.commits.lock().insert((slot, start), ts);
        self.running.lock().remove(&slot);
    }

    /// The slot let go with no pair published: what no transaction does.
    fn vanish(&self, slot: u16) {
        self.running.lock().remove(&slot);
    }
}

/// Opens a database, runs `body` on it, drops it, then fails with the body's words, if any.
fn with_db(budget: usize, body: impl FnOnce(&Arc<dyn IO>, &Atoll) -> Words<()>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, budget);
    let outcome = body(&io, &db);
    drop(db);
    if let Err(words) = outcome {
        panic!("{words}");
    }
}

fn open(io: &Arc<dyn IO>, budget: usize) -> Atoll {
    let config = AtollConfig {
        log: LogConfig {
            segment_bytes: 1 << 20,
            first_segment_bytes: 1 << 20,
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
            budget,
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
            kept_bytes: None,
            lent_at_open: Default::default(),
        },
        log_budget: Some(512 << 10),
        checkpoint_write_bytes: None,
        checkpointing: crate::storage::atoll::checkpoint::Checkpointing::Writers,
        txn: Default::default(),
    };
    let opening = Opening::new(io.clone(), "/db/gc", config, None).unwrap();
    opening.work().unwrap();
    opening.finish().unwrap()
}

/// The lists, the collector and the transactions' stand-in, for every slot.
fn parts(db: &Atoll) -> (Lists, Collector, Fake) {
    (
        Lists::new(db.pool().clone(), SLOTS),
        Collector::new(SLOTS),
        Fake::default(),
    )
}

/// `n` values of one page each, written into a tree and retired by a second put each: each
/// value's reference and pages.
fn retired(io: &Arc<dyn IO>, db: &Atoll, n: usize) -> Words<Vec<(ValueRef, Vec<PageId>)>> {
    let root = go(io, "create", || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })?;
    let tree = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root);
    let mut out = Vec::new();
    for i in 0..n {
        let key = (i as u32).to_be_bytes();
        let mut op = Put::new(&key, vec![i as u8 + 1; 5_000], Mode::Insert);
        go(io, "insert", || tree.put(&mut op))?;
        let reference = match go(io, "stored", || tree.stored(&key))?.map(|s| s.value) {
            Some(StoredValue::OutOfLine(r)) => r,
            other => return Err(format!("{i}: not out of line: {other:?}")),
        };
        let mut op = Put::new(&key, b"small".to_vec(), Mode::Update).retiring();
        go(io, "retire", || tree.put(&mut op))?;
        ensure!(
            db.store().retired(reference.pid),
            "{i}: its page not marked retired"
        );
        out.push((reference, vec![reference.pid]));
    }
    Ok(out)
}

/// A value of two chunks and a directory, written into a tree and retired by a second put: its
/// reference and pages, the directory first.
fn retired_chunked(io: &Arc<dyn IO>, db: &Atoll) -> Words<(ValueRef, Vec<PageId>)> {
    let root = go(io, "create", || {
        Tree::create(db.pool(), SizeClass::new(0).unwrap())
    })?;
    let tree = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root);
    let len = crate::storage::atoll::btree::value::chunk() + 12_345;
    let mut op = Put::new(b"big", vec![9; len], Mode::Insert);
    go(io, "insert", || tree.put(&mut op))?;
    let reference = match go(io, "stored", || tree.stored(b"big"))?.map(|s| s.value) {
        Some(StoredValue::OutOfLine(r)) => r,
        other => return Err(format!("not out of line: {other:?}")),
    };
    let pages = match go(io, "pages", || {
        crate::storage::atoll::btree::value::pages(db.pool(), reference, &|| true)
    })? {
        Ok(pages) => pages,
        Err(_) => return Err("its pages not read".into()),
    };
    let mut op = Put::new(b"big", b"small".to_vec(), Mode::Update).retiring();
    go(io, "retire", || tree.put(&mut op))?;
    ensure!(pages.len() == 3, "{} pages", pages.len());
    Ok((reference, pages))
}

/// Appends an item and publishes it, as a write retiring `reference` does once its put is done.
fn append(
    io: &Arc<dyn IO>,
    lists: &Lists,
    writers: &Fake,
    slot: u16,
    start: Ts,
    reference: ValueRef,
) -> Words<()> {
    go(io, "append", || {
        lists.append(slot, start, reference, writers)
    })?;
    lists.publish(slot, start);
    Ok(())
}

/// Every item of `slot`'s list from position `from` to its end.
fn read_all(
    io: &Arc<dyn IO>,
    lists: &Lists,
    slot: u16,
    from: u64,
) -> Words<Vec<super::list::Item>> {
    let mut reader = Reader::from(from);
    let mut out = Vec::new();
    while let Some(item) = go(io, "read", || lists.read(slot, &mut reader))? {
        out.push(item);
        reader.advance();
    }
    Ok(out)
}

/// A step of every slot's collection under `cutoff`: the pages it freed.
fn step(
    io: &Arc<dyn IO>,
    db: &Atoll,
    collector: &Collector,
    lists: &Lists,
    writers: &Fake,
    cutoff: Ts,
) -> Words<u64> {
    let mut freed = 0;
    for slot in 0..SLOTS as u16 {
        freed += go(io, "step", || {
            collector.step(slot, cutoff, 64, lists, db.pool(), db.store(), writers)
        })?;
    }
    Ok(freed)
}

/// Whether every page in `pages` is free in the map.
fn pages_free(io: &Arc<dyn IO>, db: &Atoll, pages: &[PageId]) -> Words<bool> {
    for &p in pages {
        if go(io, "entry", || db.store().entry(p))? != FREE {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A reference no page backs, for the lists' own tests.
fn reference(i: u64) -> ValueRef {
    ValueRef {
        len: 5_000,
        pid: PageId::new(SizeClass::new(1).unwrap(), i),
        id: i,
    }
}

/// `op` stepped to its end, its failure the words of a failed check.
fn go<T>(io: &Arc<dyn IO>, what: &str, op: impl FnMut() -> Result<IOResult<T>>) -> Words<T> {
    drive(io, op).map_err(|e| format!("{what}: {e}"))
}

/// A retired file's block and header read back as written, and fail as corrupt once any byte
/// flips, once read at another place, or once forged with a valid checksum but not a block or a
/// header (S6).
#[test]
fn a_retired_files_block_and_header_read_back_and_fail_once_changed_or_read_elsewhere() {
    use super::file::*;
    let block = Block {
        list: 7,
        count: 3,
        before: Some(12),
        items: (0..PER_BLOCK as u64)
            .map(|i| file_item(i, reference(i)))
            .collect(),
    };
    let bytes = encode_block(5, &block);
    assert_eq!(decode_block(5, &bytes).unwrap(), block, "a block read back");
    assert!(
        matches!(decode_block(6, &bytes), Err(LimboError::Corrupt(_))),
        "a block read at another place"
    );
    for at in 0..BLOCK {
        let mut flipped = bytes.clone();
        flipped[at] ^= 1;
        assert!(
            matches!(decode_block(5, &flipped), Err(LimboError::Corrupt(_))),
            "a block with byte {at} flipped"
        );
    }
    let mut forged = bytes;
    forged[4] ^= 1;
    reseal(&mut forged, 5);
    assert!(
        matches!(decode_block(5, &forged), Err(LimboError::Corrupt(_))),
        "a block of a valid checksum and no magic"
    );
    let header = Header {
        checkpoint: 9,
        next_list: 44,
        first: Some(3),
        lists: 250,
        sum: 0xdead_beef,
    };
    for header in [
        header,
        Header {
            first: None,
            lists: 0,
            sum: 0,
            ..header
        },
    ] {
        let bytes = encode_header(1, &header);
        assert_eq!(
            decode_header(1, 9, &bytes).unwrap(),
            header,
            "a header read back"
        );
    }
    let bytes = encode_header(1, &header);
    assert!(
        matches!(decode_header(0, 9, &bytes), Err(LimboError::Corrupt(_))),
        "a header read in the other place"
    );
    assert!(
        matches!(decode_header(1, 8, &bytes), Err(LimboError::Corrupt(_))),
        "a header of another checkpoint"
    );
    for at in 0..BLOCK {
        let mut flipped = bytes.clone();
        flipped[at] ^= 1;
        assert!(
            matches!(decode_header(1, 9, &flipped), Err(LimboError::Corrupt(_))),
            "a header with byte {at} flipped"
        );
    }
    let mut forged = bytes;
    forged[4] ^= 1;
    reseal(&mut forged, 1);
    assert!(
        matches!(decode_header(1, 9, &forged), Err(LimboError::Corrupt(_))),
        "a header of a valid checksum and no magic"
    );
}

/// A block sealed whole that names more items than a block holds, a writer's fault its checksum
/// covers, fails as corrupt: the check message 6 found unreached, reached by a forged block
/// (mutant: the check gone).
#[test]
fn a_sealed_block_naming_more_items_than_a_block_holds_fails_as_corrupt() {
    use super::file::*;
    let block = Block {
        list: 7,
        count: 3,
        before: None,
        items: (0..PER_BLOCK as u64)
            .map(|i| file_item(i, reference(i)))
            .collect(),
    };
    let mut forged = encode_block(5, &block);
    forged[32..36].copy_from_slice(&(PER_BLOCK as u32 + 1).to_le_bytes());
    reseal(&mut forged, 5);
    match decode_block(5, &forged) {
        Err(LimboError::Corrupt(words)) => {
            assert!(words.contains("more items than a block holds"), "{words}")
        }
        other => panic!("a block naming {} items read as {other:?}", PER_BLOCK + 1),
    }
}

/// Every read of the retired file hands its decoder a whole block, so a decoder handed another
/// length ends the process with its words: the caller's fault, not the file's. The length check
/// message 6 found unreached, an assertion now, for a block, a header place and a record's block
/// (mutant: the assertion gone).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_block_handed_short_to_its_decoder_ends_the_process() {
    let ended = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_block_handed_short_to_its_decoder_ends_the_process"
        ),
        || {
            let block = super::file::Block {
                list: 7,
                count: 0,
                before: None,
                items: Vec::new(),
            };
            let bytes = super::file::encode_block(5, &block);
            let _ = super::file::decode_block(5, &bytes[..super::file::BLOCK - 1]);
        },
    );
    short_ended(ended, "a block read as");
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_header_place_handed_short_to_its_decoder_ends_the_process() {
    let ended = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_header_place_handed_short_to_its_decoder_ends_the_process"
        ),
        || {
            let header = super::file::Header {
                checkpoint: 9,
                next_list: 0,
                first: None,
                lists: 0,
                sum: 0,
            };
            let bytes = super::file::encode_header(1, &header);
            let _ = super::file::decode_header(1, 9, &bytes[..super::file::BLOCK - 1]);
        },
    );
    short_ended(ended, "a header place read as");
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_record_block_handed_short_to_its_decoder_ends_the_process() {
    let ended = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_record_block_handed_short_to_its_decoder_ends_the_process"
        ),
        || {
            let header = super::file::Header {
                checkpoint: 9,
                next_list: 0,
                first: Some(3),
                lists: 1,
                sum: 0,
            };
            let source = Memory(HashMap::from([(3, vec![0; super::file::BLOCK - 1])]));
            let _ = super::file::RecordRead::new(header).step(&source);
        },
    );
    short_ended(ended, "a record block read as");
}

/// How a child handed a decoder a short block ended: with a failure naming `words` and the length
/// handed. `None` in the child itself.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn short_ended(ended: Option<crate::child_test::Ended>, words: &str) {
    let Some(ended) = ended else {
        return;
    };
    let words = format!(
        "{words} {} bytes, not a block of {}",
        super::file::BLOCK - 1,
        super::file::BLOCK
    );
    assert_ne!(
        ended.code,
        Some(0),
        "the decoder took a short block:\n{}",
        ended.stderr
    );
    assert!(ended.stderr.contains(&words), "{}", ended.stderr);
}

/// A record lies in as many blocks as its lists need, chained forward from the block its header
/// names, whatever their numbers, and reads back whole: none, one, and more lists than one block
/// holds (the auditor's #6229).
#[test]
fn a_record_of_more_lists_than_one_block_holds_reads_back_from_its_chain() {
    use super::file::*;
    for lists in [0, 1, ENTRIES, ENTRIES + 1, 2 * ENTRIES + 48] {
        // Block 1 held, so the chain skips it.
        let mut blocks = Blocks::new(3, &BTreeSet::from([1]));
        let record = record_of(9, lists);
        let (header, written) = write_record(&record, &mut blocks);
        let numbers: Vec<u64> = written.iter().map(|(n, _)| *n).collect();
        assert_eq!(
            numbers,
            [0, 2, 3][..lists.div_ceil(ENTRIES)],
            "the blocks of a record of {lists} lists"
        );
        let mut file = Memory(written.into_iter().collect());
        file.put(1, 9, 0, None, &[1]);
        let header = decode_header(0, 9, &encode_header(0, &header)).unwrap();
        assert!(
            read_record(&file, header).unwrap() == (record, numbers),
            "a record of {lists} lists read back"
        );
    }
}

/// A record's chain meeting a block missing, a block of another record, or its blocks in another
/// order, fails the open as corrupt; so does a byte flipped, a list's block in its chain, or a
/// chain ending short of the lists its header names (the auditor's #6229).
#[test]
fn a_record_block_missing_of_another_record_or_out_of_its_order_fails_the_open_as_corrupt() {
    use super::file::*;
    let lists = 2 * ENTRIES + 48;
    let lay = |record: &Record| write_record(record, &mut Blocks::default());
    let record = record_of(9, lists);
    let (header, written) = lay(&record);
    let whole = Memory(written.iter().cloned().collect());
    assert!(
        read_record(&whole, header).unwrap() == (record.clone(), vec![0, 1, 2]),
        "the record whole"
    );
    // An earlier record whose first list alone differs: its later blocks hold the same entries as
    // this one's at the same numbers, so only the check of each block's record fails them.
    let mut earlier = record_of(7, lists);
    earlier.lists[0].head.at = match earlier.lists[0].head.at {
        At::Index(index) => At::Index(index + 1),
        at => at,
    };
    let (_, earlier_written) = lay(&earlier);
    let mut another = whole.clone();
    another.0.insert(1, earlier_written[1].1.clone());
    // The record's first two blocks' entries swapped, each block of its checkpoint.
    let mut swapped = record.clone();
    swapped.lists[..2 * ENTRIES].rotate_left(ENTRIES);
    let out_of_order = Memory(lay(&swapped).1.into_iter().collect());
    let mut missing = whole.clone();
    missing.0.remove(&1);
    let mut list_block = whole.clone();
    list_block.put(2, 100, 0, None, &[1]);
    let short = Memory(
        lay(&Record {
            lists: record.lists[..2 * ENTRIES].to_vec(),
            ..record.clone()
        })
        .1
        .into_iter()
        .collect(),
    );
    for (what, file) in [
        ("a block missing", &missing),
        ("a block of another record", &another),
        ("its blocks in another order", &out_of_order),
        ("a list's block in its chain", &list_block),
        ("a chain ending short", &short),
    ] {
        let out = read_record(file, header);
        assert!(
            matches!(out, Err(LimboError::Corrupt(_))),
            "{what}: {:?}",
            out.map(|(record, numbers)| (record.lists.len(), numbers))
        );
    }
    let mut flipped = whole;
    for number in 0..3 {
        for at in 0..BLOCK {
            flipped.0.get_mut(&number).unwrap()[at] ^= 1;
            let out = read_record(&flipped, header);
            assert!(
                matches!(out, Err(LimboError::Corrupt(_))),
                "block {number} with byte {at} flipped: {:?}",
                out.map(|(record, numbers)| (record.lists.len(), numbers))
            );
            flipped.0.get_mut(&number).unwrap()[at] ^= 1;
        }
    }
}

/// The walk back from a list's last block stops at its head's block by its number, so a block
/// before the head that was given back and reused for another list is never read (DESIGN.md
/// §6.5; the auditor's #6195).
#[test]
fn a_lists_walk_back_stops_at_its_heads_block_and_reads_no_block_reused_for_another_list() {
    use super::file::*;
    let mut file = Memory::default();
    file.put(10, 99, 0, None, &[50]);
    file.put(11, 1, 1, Some(10), &[1, 2, 3, 4, 5]);
    file.put(12, 1, 2, Some(11), &[6, 7]);
    let entry = Entry {
        list: 1,
        head: Head {
            block: 11,
            at: At::Index(3),
            freed: 0,
        },
        last: 12,
        last_count: 2,
    };
    let blocks = walk(&file, entry).unwrap();
    let numbers: Vec<u64> = blocks.iter().map(|(n, _)| *n).collect();
    assert_eq!(numbers, [11, 12], "the blocks the walk read");
}

/// A walk back that meets a block of another list, a block of its list at another count, a block
/// missing, or a chain that ends short of the head's block, fails as corrupt.
#[test]
fn a_walk_back_meeting_another_lists_block_or_a_wrong_count_fails_as_corrupt() {
    use super::file::*;
    let entry = Entry {
        list: 1,
        head: Head {
            block: 11,
            at: At::Index(0),
            freed: 0,
        },
        last: 12,
        last_count: 2,
    };
    let base = || {
        let mut file = Memory::default();
        file.put(11, 1, 0, None, &[1]);
        file
    };
    let mut other_list = base();
    other_list.put(12, 1, 2, Some(13), &[2]);
    other_list.put(13, 2, 1, Some(11), &[3]);
    let mut other_count = base();
    other_count.put(12, 1, 2, Some(13), &[2]);
    other_count.put(13, 1, 5, Some(11), &[3]);
    let mut missing = base();
    missing.put(12, 1, 2, Some(13), &[2]);
    let mut short = Memory::default();
    short.put(12, 1, 1, None, &[2]);
    let short_entry = Entry {
        last_count: 1,
        ..entry
    };
    for (what, file, entry) in [
        ("another list's block", &other_list, entry),
        ("a block at another count", &other_count, entry),
        ("a block missing", &missing, entry),
        ("a chain short of the head", &short, short_entry),
    ] {
        let out = walk(file, entry);
        assert!(
            matches!(out, Err(LimboError::Corrupt(_))),
            "{what}: {:?}",
            out.map(|b| b.into_iter().map(|(n, _)| n).collect::<Vec<_>>())
        );
    }
}

/// The free blocks are those no list of the newest durable record holds; a take gives the lowest
/// free one, the file grows at its end when none is free, and a block freed joins the runs beside
/// it (S6; the auditor's #6308, finding 3).
#[test]
fn a_block_is_taken_only_once_no_list_of_the_durable_record_holds_it() {
    use super::file::Blocks;
    let held = |b: &[u64]| b.iter().copied().collect::<BTreeSet<_>>();
    let mut blocks = Blocks::new(5, &held(&[1, 3]));
    let taken: Vec<u64> = (0..4).map(|_| blocks.take()).collect();
    assert_eq!(taken, [0, 2, 4, 5], "the blocks taken");
    // A record holding 0 and 5 durable: the record before's 1 and 3 free, and 2 and 4, taken by a
    // checkpoint that failed.
    for block in [3, 1, 4, 2] {
        blocks.free(block);
    }
    let taken: Vec<u64> = (0..5).map(|_| blocks.take()).collect();
    assert_eq!(
        taken,
        [1, 2, 3, 4, 6],
        "after a record holding 0 and 5 is durable"
    );
    assert_eq!(blocks.len(), 7);
    let blocks = Blocks::new(2, &held(&[4]));
    assert_eq!(blocks.len(), 5, "a file as long as the last block held");
}

/// A block freed while it is free, or past the file's end, ends the process: either would hand a
/// block out twice (the auditor's #6308, finding 3).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_block_freed_while_free_ends_the_process() {
    let ended = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_block_freed_while_free_ends_the_process"
        ),
        || {
            let mut blocks = super::file::Blocks::new(4, &BTreeSet::from([0]));
            blocks.free(2);
        },
    );
    freed_ended(ended, "block 2 freed while free, in the run 1..4");
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_block_freed_past_the_files_end_ends_the_process() {
    let ended = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_block_freed_past_the_files_end_ends_the_process"
        ),
        || {
            let mut blocks = super::file::Blocks::new(4, &BTreeSet::from([0]));
            blocks.free(4);
        },
    );
    freed_ended(ended, "block 4 freed, past the file's 4 blocks");
}

/// How a child that freed a block wrongly ended: with a failure naming `words`. `None` in the
/// child itself.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn freed_ended(ended: Option<crate::child_test::Ended>, words: &str) {
    let Some(ended) = ended else {
        return;
    };
    assert_ne!(ended.code, Some(0), "the free went on:\n{}", ended.stderr);
    assert!(ended.stderr.contains(words), "{}", ended.stderr);
}

/// A kill before a checkpoint's superblock leaves the record before in charge and whole, whichever
/// of the checkpoint's writes reached the disk: its lists' new blocks, its record's blocks and its
/// header place. Each goes to a block the record before does not hold, its own blocks among them,
/// or to the header place the superblock does not name (the auditor's #6229).
#[test]
fn a_kill_between_a_records_blocks_and_the_header_place_leaves_the_record_before_in_charge_and_whole(
) {
    use super::file::*;
    const LISTS: u64 = ENTRIES as u64 + 20;
    // Checkpoint 7, in header place 0: list 0's head in its second block, so its first is free;
    // every other list of one block.
    let mut file = Memory::default();
    let mut blocks = Blocks::default();
    let mut lists = Vec::new();
    for k in 0..LISTS {
        let first = blocks.take();
        file.put(first, 100 + k, 0, None, &[k]);
        let mut last = (first, 0);
        if k == 0 {
            last = (blocks.take(), 1);
            file.put(last.0, 100, 1, Some(first), &[1000]);
        }
        lists.push(Entry {
            list: 100 + k,
            head: Head {
                block: last.0,
                at: At::Index(0),
                freed: 0,
            },
            last: last.0,
            last_count: last.1,
        });
    }
    let before = Record {
        checkpoint: 7,
        next_list: 100 + LISTS,
        lists,
    };
    let (header, written) = write_record(&before, &mut blocks);
    file.0.extend(written);
    let named = 0;
    let places = [encode_header(named, &header), vec![0u8; BLOCK]];
    let opened = open_file(&file, &places, named, 7).unwrap();
    assert!(opened.0 == before, "the record before read back");
    // The open, then checkpoint 9: two lists get a block each.
    let (_, numbers, walks) = &opened;
    let held: BTreeSet<u64> = numbers
        .iter()
        .copied()
        .chain(walks.iter().flatten().map(|(n, _)| *n))
        .collect();
    let mut blocks = Blocks::new(file.len(), &held);
    let mut after = Record {
        checkpoint: 9,
        ..before.clone()
    };
    let mut writes = Vec::new();
    for k in [1, 2] {
        let entry = &mut after.lists[k];
        let number = blocks.take();
        let block = Block {
            list: entry.list,
            count: 1,
            before: Some(entry.last),
            items: vec![file_item(0, reference(2000 + k as u64))],
        };
        writes.push((Some(number), encode_block(number, &block)));
        (entry.last, entry.last_count) = (number, 1);
    }
    assert_eq!(writes[0].0, Some(0), "list 0's first block taken again");
    let (header, written) = write_record(&after, &mut blocks);
    writes.extend(written.into_iter().map(|(n, bytes)| (Some(n), bytes)));
    let place = header_place(named);
    writes.push((None, encode_header(place, &header)));
    let reached = |subset: u32| {
        let (mut file, mut places) = (file.clone(), places.clone());
        for (i, (number, bytes)) in writes.iter().enumerate() {
            if subset >> i & 1 == 1 {
                match number {
                    Some(number) => _ = file.0.insert(*number, bytes.clone()),
                    None => places[place as usize] = bytes.clone(),
                }
            }
        }
        (file, places)
    };
    for subset in 0..1u32 << writes.len() {
        let (file, places) = reached(subset);
        let out = open_file(&file, &places, named, 7);
        assert!(
            matches!(&out, Ok(o) if *o == opened),
            "writes {subset:05b} of {} reached the disk: {:?}",
            writes.len(),
            out.err()
        );
    }
    let (file, places) = reached(u32::MAX);
    let out = open_file(&file, &places, place, 9);
    assert!(
        matches!(&out, Ok((record, ..)) if *record == after),
        "checkpoint 9's record once its superblock is written: {:?}",
        out.err()
    );
}

/// The open reads a record's lists into the owed list in the record's order, each from its head,
/// and marks retired every page of each item except those a list's head item had freed: a
/// chunked value's directory is read to mark its chunks (DESIGN.md §6.5; S6).
#[test]
fn the_open_reads_a_records_lists_in_order_into_the_owed_list_and_marks_their_unfreed_pages() {
    use super::file::*;
    with_db(64 << 20, |io, db| {
        let lists = Lists::new(db.pool().clone(), SLOTS);
        let mut values = retired(io, db, 4)?;
        let (chunked, chunked_pages) = retired_chunked(io, db)?;
        // Its first chunk freed before the crash, as the head's progress says.
        let mut free = crate::storage::atoll::btree::value::RetiredFree::new(chunked, 0);
        go(io, "a chunk freed", || free.step(db.pool(), db.store()))?;
        values.push((chunked, chunked_pages.clone()));
        // A new process: no mark survives.
        for (_, pages) in &values {
            for &p in pages {
                db.store().reinstate(p);
            }
        }
        let refs: Vec<ValueRef> = values.iter().map(|(r, _)| *r).collect();
        let mut file = Memory::default();
        // An owed list's list from an earlier open, then two slots' lists.
        file.put_items(3, 70, 0, None, &[refs[0], refs[1]]);
        file.put_items(4, 80, 0, None, &[refs[4], refs[2]]);
        file.put_items(6, 90, 0, None, &[refs[0], refs[3]]);
        let entry = |list, block, index, freed| Entry {
            list,
            head: Head {
                block,
                at: At::Index(index),
                freed,
            },
            last: block,
            last_count: 0,
        };
        let record = Record {
            checkpoint: 3,
            next_list: 91,
            lists: vec![entry(70, 3, 0, 0), entry(80, 4, 0, 1), entry(90, 6, 1, 0)],
        };
        let mut read = OwedRead::new(record.clone());
        let parts = go(io, "the open's read", || {
            read.step(&file, &lists, db.pool(), db.store())
        })?;
        let owed = read_all(io, &lists, lists.owed(), 0)?;
        let order: Vec<ValueRef> = owed.iter().map(|i| i.reference).collect();
        ensure!(
            order == [refs[0], refs[1], refs[4], refs[2], refs[3]],
            "the owed list's order: {order:?}"
        );
        ensure!(
            parts.iter().map(|p| (p.entry, p.from)).collect::<Vec<_>>()
                == [
                    (record.lists[0], 0),
                    (record.lists[1], 2),
                    (record.lists[2], 4)
                ],
            "the parts: {parts:?}"
        );
        let marked = |pages: &[PageId]| {
            pages
                .iter()
                .map(|&p| db.store().retired(p))
                .collect::<Vec<_>>()
        };
        for (k, (_, pages)) in values.iter().enumerate().take(4) {
            ensure!(
                marked(pages) == [true],
                "value {k}'s page marked: {:?}",
                marked(pages)
            );
        }
        ensure!(
            marked(&chunked_pages) == [true, false, true],
            "the chunked value's directory, freed chunk and other chunk: {:?}",
            marked(&chunked_pages)
        );
        Ok(())
    });
}

/// A slot list's head named by its position (form 1): the open takes its block's items at or past
/// it, gives the head's freed pages to the first only if its position is the head's, and holds the
/// part by its index from then on (the auditor's #6336 and #6343). List 70's block holds positions
/// 0, 2, 3 and 4, its head at 3 with one page freed: the open owes positions 3 and 4, the chunked
/// value at 3 marked but for its freed chunk. List 80's block holds 0, 2 and 3, its head at 1, an
/// item the image left out, with one page freed: the open owes positions 2 and 3, every page
/// marked. Both parts are held by index, 2 and 1, the second with no page freed.
#[test]
fn the_open_finds_a_head_by_its_position_in_a_block_with_gaps_and_holds_its_part_by_index() {
    use super::file::*;
    with_db(64 << 20, |io, db| {
        let lists = Lists::new(db.pool().clone(), SLOTS);
        let values = retired(io, db, 6)?;
        let (chunked, chunked_pages) = retired_chunked(io, db)?;
        for (_, pages) in values.iter().chain([&(chunked, chunked_pages.clone())]) {
            for &p in pages {
                db.store().reinstate(p);
            }
        }
        let r: Vec<ValueRef> = values.iter().map(|(r, _)| *r).collect();
        let mut file = Memory::default();
        file.put_at(
            3,
            70,
            0,
            None,
            &[(0, r[0]), (2, r[1]), (3, chunked), (4, r[2])],
        );
        file.put_at(4, 80, 0, None, &[(0, r[3]), (2, r[4]), (3, r[5])]);
        let entry = |list, block, position| Entry {
            list,
            head: Head {
                block,
                at: At::Position(position),
                freed: 1,
            },
            last: block,
            last_count: 0,
        };
        let record = Record {
            checkpoint: 3,
            next_list: 91,
            lists: vec![entry(70, 3, 3), entry(80, 4, 1)],
        };
        let mut read = OwedRead::new(record);
        let parts = go(io, "the open's read", || {
            read.step(&file, &lists, db.pool(), db.store())
        })?;
        let owed = read_all(io, &lists, lists.owed(), 0)?;
        let order: Vec<ValueRef> = owed.iter().map(|i| i.reference).collect();
        ensure!(
            order == [chunked, r[2], r[4], r[5]],
            "the owed list's order: {order:?}"
        );
        let heads: Vec<Head> = parts.iter().map(|p| p.entry.head).collect();
        ensure!(
            heads
                == [
                    Head {
                        block: 3,
                        at: At::Index(2),
                        freed: 1
                    },
                    Head {
                        block: 4,
                        at: At::Index(1),
                        freed: 0
                    }
                ],
            "the parts' heads as the open holds them: {heads:?}"
        );
        let marked = |pages: &[PageId]| {
            pages
                .iter()
                .map(|&p| db.store().retired(p))
                .collect::<Vec<_>>()
        };
        ensure!(
            marked(&chunked_pages) == [true, false, true],
            "the chunked value's directory, freed chunk and other chunk: {:?}",
            marked(&chunked_pages)
        );
        for k in [2, 4, 5] {
            ensure!(
                marked(&values[k].1) == [true],
                "value {k}'s page marked: {:?}",
                marked(&values[k].1)
            );
        }
        Ok(())
    });
}

/// A head named by a position past every item of its block, or a head block whose positions do
/// not ascend, fails the open as corrupt (the auditor's #6343).
#[test]
fn a_head_position_past_its_blocks_items_or_a_block_out_of_order_fails_the_open_as_corrupt() {
    use super::file::*;
    with_db(64 << 20, |io, db| {
        let lists = Lists::new(db.pool().clone(), SLOTS);
        let values = retired(io, db, 3)?;
        let r: Vec<ValueRef> = values.iter().map(|(r, _)| *r).collect();
        let open = |items: &[(u64, ValueRef)], position| {
            let mut file = Memory::default();
            file.put_at(3, 70, 0, None, items);
            let record = Record {
                checkpoint: 3,
                next_list: 71,
                lists: vec![Entry {
                    list: 70,
                    head: Head {
                        block: 3,
                        at: At::Position(position),
                        freed: 0,
                    },
                    last: 3,
                    last_count: 0,
                }],
            };
            let mut read = OwedRead::new(record);
            drive(io, || read.step(&file, &lists, db.pool(), db.store()))
        };
        let past = open(&[(0, r[0]), (2, r[1])], 5);
        let unordered = open(&[(0, r[0]), (3, r[1]), (2, r[2])], 1);
        ensure!(
            matches!(&past, Err(LimboError::Corrupt(w)) if w.contains("is past every item")),
            "a head's position past every item of its block: {past:?}"
        );
        ensure!(
            matches!(&unordered, Err(LimboError::Corrupt(w)) if w.contains("do not ascend")),
            "a head block whose positions do not ascend: {unordered:?}"
        );
        Ok(())
    });
}

/// A record entry is 40 bytes, 101 to a block: its head's form in bit 63 of its place, its block
/// numbers in 48 bits each. Entries at every field's bound, a small position and an index with
/// bit 62 set read back as written, through a record of two blocks (the auditor's #6343; mutants: the form read from another bit, a block number
/// written or read in 32 bits).
#[test]
fn a_record_entrys_form_and_48_bit_block_numbers_read_back_at_their_bounds() {
    use super::file::*;
    assert_eq!(ENTRIES, 101, "entries a record block holds");
    let bound = |k: u64| Entry {
        list: u64::MAX - k,
        head: Head {
            block: (1 << 48) - 1 - k,
            at: match k % 2 {
                0 => At::Position((1 << 63) - 1 - k),
                _ => At::Index((1 << 32) + k),
            },
            freed: u32::MAX - k as u32,
        },
        last: (1 << 48) - 2 - k,
        last_count: u64::MAX - 3 - k,
    };
    // And a small position and an index with bit 62 set: a form read from any bit but 63 tells
    // one of them wrong.
    let small = |k: u64, at| Entry {
        list: k,
        head: Head {
            block: 1 << 40,
            at,
            freed: 1,
        },
        last: (1 << 40) + 1,
        last_count: 1,
    };
    let record = Record {
        checkpoint: 9,
        next_list: u64::MAX,
        lists: (0..ENTRIES as u64)
            .map(bound)
            .chain([
                small(7, At::Position(5)),
                small(8, At::Index((1 << 62) + 1)),
            ])
            .collect(),
    };
    let mut blocks = Blocks::new(0, &BTreeSet::new());
    let (header, written) = write_record(&record, &mut blocks);
    let file = Memory(written.into_iter().collect());
    let header = decode_header(0, 9, &encode_header(0, &header)).unwrap();
    let (read, _) = read_record(&file, header).unwrap();
    assert!(
        read == record,
        "the entries read back: {:?}",
        &read.lists[..2]
    );
}

/// Block bytes held in memory, a block a number.
#[derive(Default, Clone)]
struct Memory(HashMap<u64, Vec<u8>>);

impl super::file::Source for Memory {
    fn block(&self, number: u64) -> Result<IOResult<Option<Vec<u8>>>> {
        Ok(IOResult::Done(self.0.get(&number).cloned()))
    }
}

impl Memory {
    /// The file's length in blocks.
    fn len(&self) -> u64 {
        self.0.keys().max().map_or(0, |n| n + 1)
    }

    /// Block `number` of list `list` at `count` after `before`, its items naming the references
    /// `ids` give.
    fn put(&mut self, number: u64, list: u64, count: u64, before: Option<u64>, ids: &[u64]) {
        let refs: Vec<ValueRef> = ids.iter().map(|&i| reference(i)).collect();
        self.put_items(number, list, count, before, &refs);
    }

    /// Block `number` of list `list` at `count` after `before`, its items at the positions given.
    fn put_at(
        &mut self,
        number: u64,
        list: u64,
        count: u64,
        before: Option<u64>,
        items: &[(u64, ValueRef)],
    ) {
        let block = super::file::Block {
            list,
            count,
            before,
            items: items.iter().map(|&(i, r)| file_item(i, r)).collect(),
        };
        self.0
            .insert(number, super::file::encode_block(number, &block));
    }

    fn put_items(
        &mut self,
        number: u64,
        list: u64,
        count: u64,
        before: Option<u64>,
        refs: &[ValueRef],
    ) {
        let block = super::file::Block {
            list,
            count,
            before,
            items: refs
                .iter()
                .enumerate()
                .map(|(i, &r)| file_item(i as u64, r))
                .collect(),
        };
        self.0
            .insert(number, super::file::encode_block(number, &block));
    }
}

/// An item of the retired file, its writer's start and position `i`.
fn file_item(i: u64, reference: ValueRef) -> super::file::Item {
    super::file::Item {
        start: 10 + i,
        position: i,
        reference,
    }
}

/// The walk back of `entry` over `file`, stepped to its end.
fn walk(file: &Memory, entry: super::file::Entry) -> Result<Vec<(u64, super::file::Block)>> {
    let mut walk = super::file::Walk::new(entry);
    match walk.step(file)? {
        IOResult::Done(blocks) => Ok(blocks),
        IOResult::IO(_) => unreachable!("blocks in memory"),
    }
}

/// The record `header` names, read from `file`, and the blocks it lies in.
fn read_record(
    file: &Memory,
    header: super::file::Header,
) -> Result<(super::file::Record, Vec<u64>)> {
    let mut read = super::file::RecordRead::new(header);
    match read.step(file)? {
        IOResult::Done(read) => Ok(read),
        IOResult::IO(_) => unreachable!("blocks in memory"),
    }
}

/// What an open reads of `file` when the superblock names header place `place` and checkpoint
/// `checkpoint`: the record, its blocks, and each list's blocks from its head's to its last.
#[allow(clippy::type_complexity)]
fn open_file(
    file: &Memory,
    places: &[Vec<u8>; 2],
    place: u8,
    checkpoint: u64,
) -> Result<(
    super::file::Record,
    Vec<u64>,
    Vec<Vec<(u64, super::file::Block)>>,
)> {
    let header = super::file::decode_header(place, checkpoint, &places[place as usize])?;
    let (record, numbers) = read_record(file, header)?;
    let walks = record
        .lists
        .iter()
        .map(|entry| walk(file, *entry))
        .collect::<Result<_>>()?;
    Ok((record, numbers, walks))
}

/// A record of checkpoint `checkpoint` and `lists` lists, every field of each entry its own.
fn record_of(checkpoint: u64, lists: usize) -> super::file::Record {
    use super::file::*;
    Record {
        checkpoint,
        next_list: 100 + lists as u64,
        lists: (0..lists as u64)
            .map(|k| Entry {
                list: 100 + k,
                head: Head {
                    block: 1000 + k,
                    at: At::Index(k % 7),
                    freed: (k % 3) as u32,
                },
                last: 2000 + k,
                last_count: k,
            })
            .collect(),
    }
}

/// Seals `bytes` again, as block `number`, after a forgery.
fn reseal(bytes: &mut [u8], number: u64) {
    let sum = crate::storage::atoll::crc::append(
        crate::storage::atoll::crc::of(&number.to_le_bytes()),
        &bytes[4..],
    );
    bytes[..4].copy_from_slice(&sum.to_le_bytes());
}
