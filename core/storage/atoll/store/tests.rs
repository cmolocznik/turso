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
    let (control, sb) = Control::open(io.clone(), base).unwrap();
    assert!(sb.is_none());
    let log = Log::open(io.clone(), base, log_config(), 0).unwrap();
    let store = PageStore::new(
        io.clone(),
        base,
        store_config(),
        log.clone(),
        [[1; CLASS_COUNT]; 2],
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
    };
    db.control.write_and_wait(&sb).unwrap();
}

/// Recovers the database at `base`: replays the page store's records after the superblock's
/// redo point, gives back the slots of unmatched intents, checkpoints and erases.
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
    let plan = Arc::new(Mutex::new(db.store.plan_writes(pids.to_vec())));
    run(&mut || plan.lock().step());
    let writes: Vec<Completion> = pids
        .iter()
        .enumerate()
        .map(|(i, pid)| {
            WritePlan::write(&plan, i, copy(pid.class(), version), |res| {
                res.expect("a write failed");
            })
            .unwrap()
        })
        .collect();
    // A write's completion runs its remap, so once every one is finished every page is remapped.
    run(&mut || match writes.iter().find(|c| !c.finished()) {
        Some(c) => Ok(IOResult::IO(IOCompletions(c.clone()))),
        None => Ok(IOResult::Done(())),
    });
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

/// Checks the page table against free space for `class`: the slots the entries name are exactly
/// the slots marked in use, each named once, and every count above the leaves adds up.
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
        frame.read(|b| {
            for s in leaf * radix::SPACE_BITS..(leaf + 1) * radix::SPACE_BITS {
                if radix::space_bit(b, s) {
                    used.insert(s);
                }
            }
        });
    }
    assert_eq!(
        referenced, used,
        "{class:?}: slots named by the page table and slots in use differ"
    );
    for tree in [Tree::Map, Tree::Space] {
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
        return frame.read(|b| {
            (first..first + radix::leaf_units(tree))
                .filter(|&u| radix::used(tree, b, u))
                .count() as u64
        });
    }
    let counts: Vec<u64> = frame.read(|b| {
        (0..radix::FANOUT)
            .map(|c| radix::used_below(b, c))
            .collect()
    });
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
}

/// Runs a workload of allocations, copies, frees, erasures and checkpoints, and after every IO
/// step recovers what a crash then would leave and checks it.
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
                    .filter(|(p, e)| p.class() == c && !e.freed && !e.free_pending)
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
            if round % 3 == 2 {
                let victim = expected
                    .borrow()
                    .iter()
                    .find(|(_, e)| !e.freed && !e.free_pending)
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

#[test]
fn records_decode_as_encoded() {
    let pid = PageId::new(class(3), 77);
    for r in [
        StoreRecord::New { pid },
        StoreRecord::Free { pid, old: Some(5) },
        StoreRecord::Free { pid, old: None },
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
    ] {
        assert_eq!(StoreRecord::decode(r.kind(), &r.encode()), Some(r.clone()));
        assert_eq!(
            StoreRecord::decode_logged(r.kind(), &r.logged([2, 3])),
            Some((r.clone(), [2, 3]))
        );
    }
}
