use super::*;
use crate::io::crash::CrashIO;
use crate::storage::atoll::control::{Control, Superblock};
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::page::HEADER_SIZE;
use crate::storage::atoll::testing::{backends, drive};
use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;

const BASE: &str = "/db/x";

fn log_config() -> LogConfig {
    LogConfig {
        segment_bytes: 256 << 10,
        buffer_bytes: 64 << 10,
        chunk_bytes: 16 << 10,
        writes_in_flight: 3,
        direct: true,
    }
}

/// Small data files, so slots spread over several.
fn store_config() -> StoreConfig {
    StoreConfig {
        file_bytes: 64 << 10,
        meta_pages: 64,
    }
}

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

/// A copy of a page: every byte after the header is `version`.
fn copy(class: SizeClass, version: u8) -> Arc<Buffer> {
    let block = Arc::new(AlignedBlock::zeroed(class.size()));
    // SAFETY: a fresh block.
    unsafe { std::ptr::write_bytes(block.as_ptr(), version, class.size()) };
    block.region(0, class.size())
}

/// A database's log, page store and control file, and the superblock last written.
struct Db {
    io: Arc<dyn IO>,
    log: Arc<Log>,
    store: Arc<PageStore>,
    control: Control,
    seq: u64,
}

fn create(io: Arc<dyn IO>, base: &str) -> Db {
    create_with(io, base, store_config())
}

fn create_with(io: Arc<dyn IO>, base: &str, config: StoreConfig) -> Db {
    let (control, sb) = Control::open(io.clone(), base).unwrap();
    assert!(sb.is_none());
    let log = Log::open(io.clone(), base, log_config(), 0).unwrap();
    let store = PageStore::new(
        io.clone(),
        base,
        config,
        log.clone(),
        [[1; CLASS_COUNT]; TREES],
    );
    let mut db = Db {
        io,
        log,
        store,
        control,
        seq: 0,
    };
    write_superblock(&mut db, 0);
    db
}

fn write_superblock(db: &mut Db, redo: Lsn) {
    db.seq += 1;
    let sb = Superblock {
        seq: db.seq,
        redo,
        segment_bytes: log_config().segment_bytes,
        file_bytes: store_config().file_bytes,
        database_id: [7; 16],
        heights: db.store.heights(),
        clock: 0,
        writers: None,
        continuations: Vec::new(),
    };
    db.control.write_and_wait(&sb).unwrap();
}

/// Recovers the database at `base`: replays the page store's records after the superblock's
/// redo point, gives back the slots of unmatched intents, frees the pages still retired as
/// recovery does once its undo is done, checkpoints and erases.
fn recover(io: Arc<dyn IO>, base: &str) -> Db {
    let (control, sb) = Control::open(io.clone(), base).unwrap();
    let sb = sb.expect("a superblock");
    let store = PageStore::recovering(io.clone(), base, store_config(), sb.heights);
    let mut reader = Log::reader(io.clone(), base, &log_config(), sb.redo);
    while let Some(r) = reader.next_record().unwrap() {
        if kinds::owns(r.header.kind) {
            drive(&io, || store.redo(&r)).unwrap();
        }
    }
    let log = Log::open(io.clone(), base, log_config(), reader.position()).unwrap();
    store.resume(log.clone());
    drive(&io, || store.release_orphans()).unwrap();
    store.end_recovery();
    // No buffer pool here, so no frame can hold one: the store frees them itself.
    drive(&io, || {
        store.free_retired_pages(|pid| store.free_retired(pid))
    })
    .unwrap();
    let mut db = Db {
        io,
        log,
        store,
        control,
        seq: sb.seq,
    };
    store_erase(&db);
    checkpoint(&mut db);
    db.store.allow_erasing();
    store_erase(&db);
    db
}

fn store_erase(db: &Db) {
    drive(&db.io, || db.log.wait_durable(db.log.end())).unwrap();
    drive(&db.io, || db.store.erase_freed()).unwrap();
}

/// Writes every changed metadata page, then a superblock whose redo point is as far on as the
/// page store allows.
fn checkpoint(db: &mut Db) {
    let end = db.log.end();
    drive(&db.io, || db.store.write_metadata(end)).unwrap();
    let redo = db.store.redo_bound().map_or(end, |b| b.min(end));
    write_superblock(db, redo);
}

/// Steps an operation to the end, the way the test running it chooses.
type Runner<'a> = dyn Fn(&mut dyn FnMut() -> Result<IOResult<()>>) + 'a;

/// Writes a copy of each of `pids` (all of one class) holding `version`.
fn write_copies(db: &Db, pids: &[PageId], version: u8, run: &Runner<'_>) {
    let mut plan = db.store.plan_writes(pids.to_vec());
    run(&mut || plan.step());
    let writes: Vec<Completion> = pids
        .iter()
        .enumerate()
        .map(|(i, pid)| {
            let copy = copy(pid.class(), version);
            page::seal(copy.as_mut_slice(), *pid);
            plan.write(i, copy, |res| {
                res.expect("a write failed");
            })
            .unwrap()
        })
        .collect();
    run(&mut || match writes.iter().find(|c| !c.finished()) {
        Some(c) => Ok(IOResult::IO(IOCompletions(c.clone()))),
        None => Ok(IOResult::Done(())),
    });
    // Every write landed: settling remaps each page.
    for i in 0..pids.len() {
        run(&mut || Ok(plan.settle(i, &Ok(()))));
    }
}

/// What every page reads as now: its entry and, if it has a copy, the version the copy holds.
fn read_back(db: &Db, pid: PageId) -> (u64, Option<u8>) {
    let entry = drive(&db.io, || db.store.entry(pid)).unwrap();
    let Some(slot) = entry.checked_sub(SLOT_BASE) else {
        return (entry, None);
    };
    let got = Arc::new(Mutex::new(None));
    let out = got.clone();
    let c = db
        .store
        .read_slot(pid, slot, move |read| {
            *out.lock() = Some(match read {
                PageRead::Valid(buf) => {
                    let body = &buf.as_slice()[HEADER_SIZE..];
                    assert!(body.iter().all(|&b| b == body[0]), "{pid:?}: a mixed copy");
                    Some(body[0])
                }
                _ => None,
            })
        })
        .unwrap();
    db.io.wait_for_completion(c).unwrap();
    let version = got.lock().take().expect("the read completed");
    assert!(
        version.is_some(),
        "{pid:?}: slot {slot} holds no whole copy"
    );
    (entry, version)
}

/// Sets and clears retired marks in a group of their own, as an owner's group carries them.
fn change_marks(
    store: &Arc<PageStore>,
    log: &Arc<Log>,
    retire: &[PageId],
    reinstate: &[PageId],
    run: &Runner<'_>,
) {
    run(&mut || {
        let marks = match store.marks(retire, reinstate)? {
            IOResult::Done(m) => m,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let records = marks.records();
        let lens: Vec<u64> = records.iter().map(|(_, p)| framed_len(p.len())).collect();
        let mut admitted = match log.admit(lens.iter().sum())? {
            IOResult::Done(a) => a,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let Some(locked) = marks.lock(store) else {
            return Ok(busy());
        };
        let pieces: Vec<[&[u8]; 1]> = records.iter().map(|(_, p)| [p.as_slice()]).collect();
        let group: Vec<(Header, &[&[u8]])> = records
            .iter()
            .zip(&pieces)
            .map(|((h, _), p)| (*h, &p[..]))
            .collect();
        let lsn = if group.len() == 1 {
            log.append(&mut admitted, &group[0].0, group[0].1)
        } else {
            *log.append_group(&mut admitted, &group).1.last().unwrap()
        };
        locked.commit(store, lsn - lens.iter().sum::<u64>(), lsn);
        Ok(IOResult::Done(()))
    });
}

/// The page numbers of `class` below `numbers` marked retired, read from the marks' leaves.
fn retired_numbers(db: &Db, class: SizeClass, numbers: u64) -> BTreeSet<u64> {
    let height = db.store.heights()[Tree::Retired as usize][class.index()];
    let numbers = numbers.min(radix::capacity(Tree::Retired, height));
    let mut retired = BTreeSet::new();
    for leaf in 0..numbers.div_ceil(radix::SPACE_BITS) {
        let id = MetaId {
            tree: Tree::Retired,
            class,
            level: 0,
            index: leaf,
        };
        let frame = drive(&db.io, || db.store.meta.get(id)).unwrap();
        let bits = frame
            .read(|b| {
                (leaf * radix::SPACE_BITS..((leaf + 1) * radix::SPACE_BITS).min(numbers))
                    .filter(|&n| radix::space_bit(b, n))
                    .collect::<Vec<u64>>()
            })
            .expect("a still store's page is latched by no one");
        retired.extend(bits);
    }
    retired
}

/// Checks the page table against free space for `class`: the slots the entries name are exactly
/// the slots marked in use, each named once, every page marked retired is allocated, and every
/// count above the leaves adds up.
fn check_consistent(db: &Db, class: SizeClass, numbers: u64) {
    let mut referenced = BTreeSet::new();
    for n in 0..numbers {
        let pid = PageId::new(class, n);
        if let Some(slot) = drive(&db.io, || db.store.entry(pid))
            .unwrap()
            .checked_sub(SLOT_BASE)
        {
            assert!(
                referenced.insert(slot),
                "slot {slot} of {class:?} is named twice"
            );
        }
    }
    let heights = db.store.heights();
    let space_height = heights[Tree::Space as usize][class.index()];
    let slots = radix::capacity(Tree::Space, space_height).min(1 << 16);
    let mut used = BTreeSet::new();
    for leaf in 0..slots.div_ceil(radix::SPACE_BITS) {
        let id = MetaId {
            tree: Tree::Space,
            class,
            level: 0,
            index: leaf,
        };
        let frame = drive(&db.io, || db.store.meta.get(id)).unwrap();
        let bits = frame
            .read(|b| {
                (leaf * radix::SPACE_BITS..(leaf + 1) * radix::SPACE_BITS)
                    .filter(|&s| radix::space_bit(b, s))
                    .collect::<Vec<u64>>()
            })
            .expect("a still store's page is latched by no one");
        used.extend(bits);
    }
    assert_eq!(
        referenced, used,
        "{class:?}: slots named by the page table and slots in use differ"
    );
    for n in retired_numbers(db, class, numbers) {
        let entry = drive(&db.io, || db.store.entry(PageId::new(class, n))).unwrap();
        assert_ne!(entry, FREE, "{class:?}: page {n} is retired and free");
    }
    for tree in [Tree::Map, Tree::Space, Tree::Retired] {
        let height = heights[tree as usize][class.index()];
        check_counts(db, tree, class, height - 1, 0);
    }
}

/// The units in use below the node at `level`, `index`, checking each node's counts on the way.
fn check_counts(db: &Db, tree: Tree, class: SizeClass, level: u8, index: u64) -> u64 {
    let id = MetaId {
        tree,
        class,
        level,
        index,
    };
    let frame = drive(&db.io, || db.store.meta.get(id)).unwrap();
    if level == 0 {
        let first = index * radix::leaf_units(tree);
        return frame
            .read(|b| {
                (first..first + radix::leaf_units(tree))
                    .filter(|&u| radix::used(tree, b, u))
                    .count() as u64
            })
            .expect("a still store's page is latched by no one");
    }
    let counts: Vec<u64> = frame
        .read(|b| {
            (0..radix::FANOUT)
                .map(|c| radix::used_below(b, c))
                .collect()
        })
        .expect("a still store's page is latched by no one");
    let mut total = 0;
    for (c, &count) in counts.iter().enumerate() {
        if count == 0 {
            continue;
        }
        let below = check_counts(db, tree, class, level - 1, index * radix::FANOUT + c as u64);
        assert_eq!(
            below, count,
            "{tree:?} {class:?} level {level} node {index} child {c}"
        );
        total += count;
    }
    total
}

/// Checks that no slot of `class` outside those the page table names holds a whole page copy.
fn check_erased(db: &Db, class: SizeClass, numbers: u64) {
    let named: BTreeSet<u64> = (0..numbers)
        .filter_map(|n| {
            drive(&db.io, || db.store.entry(PageId::new(class, n)))
                .unwrap()
                .checked_sub(SLOT_BASE)
        })
        .collect();
    let per_file = store_config().file_bytes / class.size() as u64;
    for file_index in 0..8u64 {
        let path = format!("{BASE}.c{}.{file_index}", class.index());
        let Ok(file) = db.io.open_file(&path, OpenFlags::Direct, false) else {
            continue;
        };
        let size = file.size().unwrap();
        for at in (0..size).step_by(class.size()) {
            let slot = file_index * per_file + at / class.size() as u64;
            if named.contains(&slot) {
                continue;
            }
            let block = Arc::new(AlignedBlock::zeroed(class.size()));
            let c = file
                .pread(
                    at,
                    Completion::new_read(block.region(0, class.size()), |_| None),
                )
                .unwrap();
            db.io.wait_for_completion(c).unwrap();
            // SAFETY: the read filled the block, which nothing else uses.
            let bytes = unsafe { std::slice::from_raw_parts(block.as_ptr(), class.size()) };
            assert!(
                !page::verify(bytes, page::pid(bytes)),
                "{class:?} slot {slot}, named by nothing, still holds a copy of {:?}",
                page::pid(bytes)
            );
        }
    }
}

#[test]
fn copies_are_read_back_from_their_newest_slot_and_superseded_slots_are_erased_on_every_backend() {
    for b in backends() {
        let mut db = create(b.io.clone(), &b.base);
        let pids: Vec<PageId> = (0..5)
            .map(|_| drive(&db.io, || db.store.allocate(class(1))).unwrap())
            .collect();
        drive(&db.io, || db.store.adopt(&pids)).unwrap();
        let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| drive(&db.io, op).unwrap();
        write_copies(&db, &pids, 1, &run);
        write_copies(&db, &pids, 2, &run);
        store_erase(&db);
        for &pid in &pids {
            assert_eq!(read_back(&db, pid).1, Some(2), "{}: {pid:?}", b.name);
        }
        drive(&db.io, || db.store.free(pids[0])).unwrap();
        store_erase(&db);
        checkpoint(&mut db);
        let (entry, _) = read_back(&db, pids[0]);
        assert_eq!(entry, FREE, "{}", b.name);
        let numbers = pids.iter().map(|p| p.number() + 1).max().unwrap();
        check_consistent(&db, class(1), numbers);
        drop(db);
        let db = recover(b.io.clone(), &b.base);
        for &pid in &pids[1..] {
            assert_eq!(
                read_back(&db, pid).1,
                Some(2),
                "{}: {pid:?} after reopening",
                b.name
            );
        }
        check_consistent(&db, class(1), numbers);
    }
}

/// What the workload has had acknowledged about a page: allocated, freed, the version of its
/// last acknowledged copy, and versions written since that may or may not have stuck.
#[derive(Clone, Default)]
struct Expected {
    freed: bool,
    version: Option<u8>,
    pending: Vec<u8>,
    free_pending: bool,
    /// Its retire is durable, and no reinstate since: recovery frees it.
    retired: bool,
    /// A retire or a reinstate of it may or may not have reached the log.
    mark_pending: bool,
}

/// Runs a workload of allocations, copies, frees, retires, reinstates, erasures and checkpoints,
/// and after every few IO steps recovers what a crash then would leave and checks it: a page
/// retired is freed by recovery, whether its mark was still in the log or only in a metadata page
/// a checkpoint wrote, and one reinstated is kept.
fn a_crash_at_any_moment_recovers_a_consistent_page_store(durable_writes: bool) {
    for seed in 0..4u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        crash_io.set_durable_writes(durable_writes);
        let io: Arc<dyn IO> = crash_io.clone();
        let mut db = create(io.clone(), BASE);
        let classes = [class(0), class(2)];
        let expected: RefCell<BTreeMap<PageId, Expected>> = RefCell::new(BTreeMap::new());
        let steps = Cell::new(0u64);
        // While an allocation is in flight, a freed number of its class may already be taken again.
        let allocating: Cell<Option<SizeClass>> = Cell::new(None);
        let check = |crash_seed: u64| {
            let crashed: Arc<dyn IO> = Arc::new(crash_io.crash(crash_seed));
            let db = recover(crashed, BASE);
            let expected = expected.borrow();
            for (&pid, e) in expected.iter() {
                let (entry, version) = read_back(&db, pid);
                if e.retired && !e.mark_pending {
                    assert_eq!(
                        entry, FREE,
                        "seed {seed}/{crash_seed}: {pid:?} was retired, recovery left it"
                    );
                    continue;
                }
                if e.mark_pending && entry == FREE {
                    continue;
                }
                if e.freed {
                    let reused = allocating.get() == Some(pid.class()) && entry == ALLOCATED;
                    assert!(
                        entry == FREE || reused,
                        "seed {seed}/{crash_seed}: {pid:?} was freed, reads {entry}"
                    );
                    continue;
                }
                if e.free_pending && entry == FREE {
                    continue;
                }
                assert_ne!(entry, FREE, "seed {seed}/{crash_seed}: {pid:?} lost");
                if let Some(v) = e.version {
                    let version = version.unwrap_or_else(|| {
                        panic!("seed {seed}/{crash_seed}: {pid:?} has no copy, acknowledged {v}, entry {entry}")
                    });
                    assert!(
                        version == v || e.pending.contains(&version),
                        "seed {seed}/{crash_seed}: {pid:?} reads version {version}, acknowledged {v}"
                    );
                }
            }
            for &c in &classes {
                let numbers = expected
                    .keys()
                    .filter(|p| p.class() == c)
                    .map(|p| p.number() + 1)
                    .max()
                    .unwrap_or(0);
                check_consistent(&db, c, numbers);
                check_erased(&db, c, numbers);
                assert_eq!(
                    drive(&db.io, || db.store.find_used(Tree::Retired, c, 0)).unwrap(),
                    None,
                    "seed {seed}/{crash_seed}: {c:?} has a page still retired after recovery"
                );
            }
        };
        let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| loop {
            match op().unwrap() {
                IOResult::Done(()) => return,
                IOResult::IO(IOCompletions(c)) => {
                    while !c.finished() {
                        io.step().unwrap();
                        steps.set(steps.get() + 1);
                        if steps.get() % 3 == 0 {
                            check(seed * 1_000_000 + steps.get());
                        }
                    }
                }
            }
        };
        let durable = |db: &Db| run(&mut || db.log.wait_durable(db.log.end()));
        for round in 0..12u8 {
            for &c in &classes {
                let mut pid = None;
                allocating.set(Some(c));
                run(&mut || {
                    pid = Some(match db.store.allocate(c)? {
                        IOResult::Done(p) => p,
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    });
                    Ok(IOResult::Done(()))
                });
                run(&mut || db.store.adopt(&[pid.unwrap()]));
                durable(&db);
                expected
                    .borrow_mut()
                    .insert(pid.unwrap(), Expected::default());
                allocating.set(None);
            }
            for &c in &classes {
                let live: Vec<PageId> = expected
                    .borrow()
                    .iter()
                    .filter(|(p, e)| p.class() == c && !e.freed && !e.free_pending && !e.retired)
                    .map(|(&p, _)| p)
                    .take(3 + (round as usize % 3))
                    .collect();
                let version = round + 1;
                for p in &live {
                    expected
                        .borrow_mut()
                        .get_mut(p)
                        .unwrap()
                        .pending
                        .push(version);
                }
                write_copies(&db, &live, version, &run);
                durable(&db);
                for p in &live {
                    let mut ex = expected.borrow_mut();
                    let e = ex.get_mut(p).unwrap();
                    e.version = Some(version);
                    e.pending.clear();
                }
            }
            // A page of each class retired, and from the third round on one retired before
            // put back, so marks are set and cleared across checkpoints.
            for &c in &classes {
                let pick = |retired: bool| {
                    expected
                        .borrow()
                        .iter()
                        .filter(|(p, e)| {
                            p.class() == c && !e.freed && !e.free_pending && e.retired == retired
                        })
                        .map(|(&p, _)| p)
                        .last()
                };
                let retire: Vec<PageId> = pick(false).into_iter().collect();
                let reinstate: Vec<PageId> =
                    pick(true).filter(|_| round >= 2).into_iter().collect();
                if retire.is_empty() && reinstate.is_empty() {
                    continue;
                }
                for p in retire.iter().chain(&reinstate) {
                    expected.borrow_mut().get_mut(p).unwrap().mark_pending = true;
                }
                change_marks(&db.store, &db.log, &retire, &reinstate, &run);
                durable(&db);
                for p in &retire {
                    let mut ex = expected.borrow_mut();
                    let e = ex.get_mut(p).unwrap();
                    (e.retired, e.mark_pending) = (true, false);
                }
                for p in &reinstate {
                    let mut ex = expected.borrow_mut();
                    let e = ex.get_mut(p).unwrap();
                    (e.retired, e.mark_pending) = (false, false);
                }
            }
            if round % 3 == 2 {
                let victim = expected
                    .borrow()
                    .iter()
                    .find(|(_, e)| !e.freed && !e.free_pending && !e.retired)
                    .map(|(&p, _)| p);
                if let Some(victim) = victim {
                    expected.borrow_mut().get_mut(&victim).unwrap().free_pending = true;
                    run(&mut || db.store.free(victim));
                    durable(&db);
                    expected.borrow_mut().get_mut(&victim).unwrap().freed = true;
                }
            }
            run(&mut || db.store.erase_freed());
            if round % 4 == 3 {
                let end = db.log.end();
                run(&mut || db.store.write_metadata(end));
                let redo = db.store.redo_bound().map_or(end, |b| b.min(end));
                write_superblock(&mut db, redo);
            }
        }
        assert!(
            steps.get() > 50,
            "seed {seed}: only {} IO steps",
            steps.get()
        );
        // Running on, every page retired is still allocated and marked.
        let retired: Vec<PageId> = expected
            .borrow()
            .iter()
            .filter(|(_, e)| e.retired)
            .map(|(&p, _)| p)
            .collect();
        assert!(
            retired.len() >= 2,
            "seed {seed}: {} pages retired",
            retired.len()
        );
        for &p in &retired {
            assert!(
                drive(&db.io, || db.store.retired(p)).unwrap(),
                "{p:?} lost its mark"
            );
            assert_ne!(drive(&db.io, || db.store.entry(p)).unwrap(), FREE);
        }
        check(seed);
    }
}

#[test]
fn a_crash_at_any_moment_recovers_a_consistent_page_store_with_durable_writes() {
    a_crash_at_any_moment_recovers_a_consistent_page_store(true);
}

#[test]
fn a_crash_at_any_moment_recovers_a_consistent_page_store_with_writes_then_syncs() {
    a_crash_at_any_moment_recovers_a_consistent_page_store(false);
}

/// Pages retired past the marks' first leaf, which grows them a level, then a checkpoint whose
/// redo point passes every mark record: recovery finds the marks in the metadata pages alone,
/// through the levels the superblock records, and frees the pages still retired, while the one
/// put back keeps its copy.
#[test]
fn marks_behind_the_redo_point_and_past_their_first_leaf_still_free_their_pages_in_recovery() {
    let crash_io = Arc::new(CrashIO::new(3));
    let io: Arc<dyn IO> = crash_io.clone();
    let mut db = create(io.clone(), BASE);
    let c = class(0);
    let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| drive(&io, op).unwrap();
    // One leaf of marks holds SPACE_BITS page numbers: the last pages here are past it.
    let numbers = radix::SPACE_BITS + 8;
    let mut pids = Vec::new();
    for _ in 0..numbers {
        pids.push(drive(&io, || db.store.allocate(c)).unwrap());
    }
    for batch in pids.chunks(512) {
        run(&mut || db.store.adopt(batch));
    }
    let (near, kept, far) = (pids[5], pids[7], pids[numbers as usize - 3]);
    assert!(far.number() >= radix::SPACE_BITS);
    write_copies(&db, &[near, kept, far], 1, &run);
    change_marks(&db.store, &db.log, &[near, kept, far], &[], &run);
    change_marks(&db.store, &db.log, &[], &[kept], &run);
    assert_eq!(db.store.heights()[Tree::Retired as usize][c.index()], 2);
    for (p, retired) in [(near, true), (kept, false), (far, true)] {
        assert_eq!(
            drive(&io, || db.store.retired(p)).unwrap(),
            retired,
            "{p:?}"
        );
    }
    let marked = db.log.end();
    run(&mut || db.log.wait_durable(marked));
    let end = db.log.end();
    run(&mut || db.store.write_metadata(end));
    let redo = db.store.redo_bound().map_or(end, |b| b.min(end));
    assert!(
        redo >= marked,
        "the redo point {redo} stays before the marks, which end at {marked}"
    );
    write_superblock(&mut db, redo);
    let recovered = recover(Arc::new(crash_io.crash(0)), BASE);
    assert_eq!(
        read_back(&recovered, near).0,
        FREE,
        "a page still retired was kept"
    );
    assert_eq!(
        read_back(&recovered, far).0,
        FREE,
        "a page retired past the first leaf was kept"
    );
    assert_eq!(
        read_back(&recovered, kept).1,
        Some(1),
        "the page put back lost its copy"
    );
    check_consistent(&recovered, c, numbers);
    assert_eq!(
        drive(&recovered.io, || recovered.store.find_used(
            Tree::Retired,
            c,
            0
        ))
        .unwrap(),
        None
    );
}

#[test]
fn records_decode_as_encoded() {
    let pid = PageId::new(class(3), 77);
    for r in [
        StoreRecord::New { pid },
        StoreRecord::Free {
            pid,
            old: Some(5),
            retired: false,
        },
        StoreRecord::Free {
            pid,
            old: None,
            retired: true,
        },
        StoreRecord::Retire {
            pids: vec![pid, PageId::new(class(3), 9)],
        },
        StoreRecord::Reinstate { pids: vec![pid] },
        StoreRecord::Intent {
            class: class(3),
            slots: vec![1, 2, 9],
        },
        StoreRecord::Remap {
            pid,
            new: 4,
            old: None,
        },
        StoreRecord::Release {
            class: class(3),
            slots: vec![8],
        },
        StoreRecord::Grow {
            tree: Tree::Space,
            class: class(3),
            height: 3,
            used: 12345,
        },
        StoreRecord::Grow {
            tree: Tree::Retired,
            class: class(3),
            height: 2,
            used: 7,
        },
    ] {
        assert_eq!(StoreRecord::decode(r.kind(), &r.encode()), Some(r.clone()));
        assert_eq!(
            StoreRecord::decode_logged(r.kind(), &r.logged([2, 3, 4])),
            Some((r.clone(), [2, 3, 4]))
        );
    }
    // A retire's levels are one class's.
    let mixed = StoreRecord::Retire {
        pids: vec![pid, PageId::new(class(4), 9)],
    };
    assert_eq!(StoreRecord::decode(mixed.kind(), &mixed.encode()), None);
}

/// Writers on `threads` threads each retire their own pages, all under one leaf of marks, and put
/// back every other one, while the checkpointer writes metadata back: every mark lands once, none
/// is lost to another's change of the same page, and the counts above the leaf add up.
fn marks_from_many_writers_each_land_once(threads: usize, pages: usize) {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let db = create(io.clone(), BASE);
    let c = class(1);
    let pids: Vec<PageId> = (0..threads * pages)
        .map(|_| drive(&db.io, || db.store.allocate(c)).unwrap())
        .collect();
    drive(&db.io, || db.store.adopt(&pids)).unwrap();
    let mut workers = Vec::new();
    for own in pids.chunks(pages) {
        let (io, store, log, own) = (io.clone(), db.store.clone(), db.log.clone(), own.to_vec());
        workers.push(crate::thread::spawn(move || {
            let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| drive(&io, op).unwrap();
            for (i, &pid) in own.iter().enumerate() {
                change_marks(&store, &log, &[pid], &[], &run);
                if i % 2 == 1 {
                    change_marks(&store, &log, &[], &[pid], &run);
                }
            }
        }));
    }
    let (io, store, log) = (io.clone(), db.store.clone(), db.log.clone());
    workers.push(crate::thread::spawn(move || {
        for _ in 0..3 {
            drive(&io, || store.write_metadata(log.end())).unwrap();
        }
    }));
    for w in workers {
        w.join().unwrap();
    }
    for own in pids.chunks(pages) {
        for (i, &pid) in own.iter().enumerate() {
            assert_eq!(
                drive(&db.io, || db.store.retired(pid)).unwrap(),
                i % 2 == 0,
                "{pid:?}"
            );
        }
    }
    let retired = retired_numbers(&db, c, pids.len() as u64);
    assert_eq!(retired.len(), threads * pages.div_ceil(2));
    check_consistent(&db, c, pids.len() as u64);
}

#[test]
fn marks_from_many_writers_each_land_once_on_threads() {
    marks_from_many_writers_each_land_once(12, 40);
}

#[cfg(shuttle)]
#[test]
fn marks_from_many_writers_each_land_once_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(|| marks_from_many_writers_each_land_once(3, 2), 300);
}

/// Frees change the page table's leaves while the checkpointer writes them back: in no schedule
/// do they wait on each other for good. Each free changes two leaves, the number's entry and its
/// copy's slot, so two frees and a write can each hold what another waits for unless every path
/// takes the locks in one order (CONCURRENCY.md §8).
#[cfg(shuttle)]
#[test]
fn frees_and_metadata_writes_never_wait_on_each_other_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || {
            let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
            let db = create(io.clone(), BASE);
            let pids: Vec<PageId> = (0..4)
                .map(|_| drive(&db.io, || db.store.allocate(class(1))).unwrap())
                .collect();
            drive(&db.io, || db.store.adopt(&pids)).unwrap();
            let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| drive(&db.io, op).unwrap();
            write_copies(&db, &pids, 1, &run);
            let mut threads = Vec::new();
            for pair in pids.chunks(2) {
                let (io, store, pair) = (io.clone(), db.store.clone(), pair.to_vec());
                threads.push(crate::thread::spawn(move || {
                    for pid in pair {
                        drive(&io, || store.free(pid)).unwrap();
                    }
                }));
            }
            let (io, store, log) = (io.clone(), db.store.clone(), db.log.clone());
            threads.push(crate::thread::spawn(move || {
                for _ in 0..2 {
                    drive(&io, || store.write_metadata(log.end())).unwrap();
                }
            }));
            for t in threads {
                t.join().unwrap();
            }
        },
        300,
    );
}

/// A number is not handed out again while the step that freed it runs: that step may still hold
/// the page in memory under it (CONCURRENCY.md §4, §6).
#[cfg(shuttle)]
#[test]
fn a_freed_number_waits_out_its_freers_step_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || {
            let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
            let db = create(io.clone(), BASE);
            let freed = drive(&db.io, || db.store.allocate(class(1))).unwrap();
            drive(&db.io, || db.store.adopt(&[freed])).unwrap();
            // Set only while the freer's step runs.
            let freeing = Arc::new(crate::sync::atomic::AtomicBool::new(false));
            let (fio, fstore, flag) = (io.clone(), db.store.clone(), freeing.clone());
            let freer = crate::thread::spawn(move || {
                let step = fstore.epochs().enter();
                flag.store(true, crate::sync::atomic::Ordering::SeqCst);
                drive(&fio, || fstore.free(freed)).unwrap();
                crate::thread::yield_now();
                flag.store(false, crate::sync::atomic::Ordering::SeqCst);
                drop(step);
            });
            let (aio, astore) = (io.clone(), db.store.clone());
            let allocator = crate::thread::spawn(move || {
                for _ in 0..3 {
                    let pid = drive(&aio, || astore.allocate(class(1))).unwrap();
                    assert!(
                        pid != freed || !freeing.load(crate::sync::atomic::Ordering::SeqCst),
                        "{pid:?} was handed out while its freer's step ran"
                    );
                }
            });
            freer.join().unwrap();
            allocator.join().unwrap();
        },
        300,
    );
}

/// What every user of a metadata page does while a change holds its latch: yields, and finishes
/// once the latch is released. None waits for it, so a holder on the same thread, under Turso's
/// cooperative scheduling, never leaves another connection stuck.
#[test]
fn a_latched_metadata_page_makes_its_users_yield_and_none_wait() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let mut db = create(io.clone(), "/db/latched");
    let class = class(0);
    let pid = drive(&io, || db.store.allocate(class)).unwrap();
    drive(&io, || db.store.adopt(&[pid])).unwrap();
    let first = drive(&io, || db.store.allocate(class)).unwrap();
    drive(&io, || db.store.adopt(&[first])).unwrap();
    write_copies(&db, &[first], 1, &|op| drive(&io, op).unwrap());
    let leaf = radix::path(Tree::Map, class, 1, pid.number())[0];
    let frame = drive(&io, || db.store.meta.get(leaf)).unwrap();
    let yields = |res: Result<IOResult<()>>, what: &str| match res.unwrap() {
        IOResult::IO(IOCompletions(c)) => assert!(c.is_explicit_yield(), "{what} waited"),
        IOResult::Done(()) => panic!("{what} went on under a held latch"),
    };
    // A write of `first`, landed and not yet settled.
    let mut plan = db.store.plan_writes(vec![first]);
    drive(&io, || plan.step()).unwrap();
    let copy = copy(class, 2);
    page::seal(copy.as_mut_slice(), first);
    let written = plan.write(0, copy, |res| res.unwrap()).unwrap();
    drive(&io, || {
        Ok(match written.finished() {
            true => IOResult::Done(()),
            false => IOResult::IO(IOCompletions(written.clone())),
        })
    })
    .unwrap();
    let hold = frame.try_lock().expect("the leaf is free");
    yields(db.store.entry(pid).map(|r| r.map(drop)), "a reader");
    yields(
        db.store.allocate(class).map(|r| r.map(drop)),
        "an allocation",
    );
    yields(db.store.free(pid), "a free");
    yields(db.store.write_metadata(db.log.end()), "a metadata write");
    yields(Ok(plan.settle(0, &Ok(()))), "a remap");
    drop(hold);
    drive(&io, || Ok(plan.settle(0, &Ok(())))).unwrap();
    assert_ne!(drive(&io, || db.store.entry(pid)).unwrap(), FREE);
    drive(&io, || db.store.allocate(class)).unwrap();
    drive(&io, || db.store.free(pid)).unwrap();
    checkpoint(&mut db);
}

/// Two changes to one metadata page, each committed by its own thread: the page's oldest unwritten
/// change is the first one's, whichever thread notes its change first. A page noted at the later
/// change's record would let a checkpoint's redo point pass the earlier one, still unwritten.
#[cfg(shuttle)]
#[test]
fn a_metadata_page_keeps_its_oldest_change_whatever_order_commits_finish_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || {
            let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
            let mut db = create(io.clone(), BASE);
            drive(&db.io, || db.store.allocate(class(1))).unwrap();
            checkpoint(&mut db);
            assert_eq!(db.store.meta.oldest_unwritten(), None);
            // Where the first change's record starts: both allocations change the same leaf.
            let first = db.log.end();
            let threads: Vec<_> = (0..2)
                .map(|_| {
                    let (io, store) = (io.clone(), db.store.clone());
                    crate::thread::spawn(move || {
                        drive(&io, || store.allocate(class(1))).unwrap();
                    })
                })
                .collect();
            for t in threads {
                t.join().unwrap();
            }
            assert_eq!(db.store.meta.oldest_unwritten(), Some(first));
        },
        300,
    );
}

/// A metadata page stays in the cache while anyone holds it, however small the cache: here
/// allocations in `classes` classes, `each` per class, and a checkpointer writing the metadata
/// `writes` times share a cache of one page. A page dropped while an allocation held it would
/// lose the allocation's change, and its number would be handed out again; a page dropped dirty,
/// or written and still counted dirty, would leave a change unwritten once all are written.
fn a_metadata_page_in_use_stays_in_a_cache_too_small_for_it(
    classes: usize,
    each: usize,
    writes: usize,
) {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let config = StoreConfig {
        meta_pages: 1,
        ..store_config()
    };
    let db = create_with(io.clone(), BASE, config);
    let allocators: Vec<_> = (0..classes)
        .map(|c| {
            let (io, store) = (io.clone(), db.store.clone());
            crate::thread::spawn(move || {
                (0..each)
                    .map(|_| drive(&io, || store.allocate(class(c))).unwrap())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let (cio, cstore, clog) = (io.clone(), db.store.clone(), db.log.clone());
    let checkpointer = crate::thread::spawn(move || {
        for _ in 0..writes {
            drive(&cio, || cstore.write_metadata(clog.end())).unwrap();
        }
    });
    let pids: Vec<PageId> = allocators
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    checkpointer.join().unwrap();
    let distinct: BTreeSet<PageId> = pids.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        pids.len(),
        "a number was handed out twice: {pids:?}"
    );
    for &pid in &pids {
        assert_ne!(
            drive(&io, || db.store.entry(pid)).unwrap(),
            FREE,
            "{pid:?} lost"
        );
    }
    drive(&io, || db.store.write_metadata(db.log.end())).unwrap();
    assert_eq!(
        db.store.meta.oldest_unwritten(),
        None,
        "a change was left unwritten"
    );
}

#[cfg(not(shuttle))]
#[test]
fn a_metadata_page_in_use_stays_in_a_cache_too_small_for_it_on_threads() {
    for _ in 0..if cfg!(miri) { 1 } else { 20 } {
        a_metadata_page_in_use_stays_in_a_cache_too_small_for_it(11, 40, 40);
    }
}

#[cfg(shuttle)]
#[test]
fn a_metadata_page_in_use_stays_in_a_cache_too_small_for_it_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || a_metadata_page_in_use_stays_in_a_cache_too_small_for_it(2, 3, 3),
        300,
    );
}
