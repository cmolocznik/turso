use super::*;
use crate::io::crash::CrashIO;
use crate::io::MemoryIO;
use crate::storage::atoll::buffer::{BufferConfig, MIN_DB_BUDGET};
use crate::storage::atoll::db::{Atoll, AtollConfig, Opening};
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::page::CLASS_COUNT;
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
#[cfg(not(shuttle))]
use crate::storage::atoll::testing::{step_past, HoldAt};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};

const BASE: &str = "/db/tree";

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

fn config(budget: usize) -> AtollConfig {
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
            budget,
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
            kept_bytes: None,
            lent_at_open: Default::default(),
        },
        log_budget: Some(512 << 10),
        checkpoint_write_bytes: None,
        checkpointing: crate::storage::atoll::checkpoint::Checkpointing::Writers,
        txn: Default::default(),
    }
}

/// Opens the database on `threads` threads, the way an embedder lends them to recovery.
fn open(io: &Arc<dyn IO>, budget: usize, threads: usize) -> Atoll {
    let opening = Opening::new(io.clone(), BASE, config(budget)).unwrap();
    std::thread::scope(|s| {
        for _ in 1..threads {
            s.spawn(|| opening.work().unwrap());
        }
        opening.work().unwrap();
    });
    opening.finish().unwrap()
}

fn tree_of(io: &Arc<dyn IO>, db: &Atoll, root: PageId) -> Tree {
    Tree::open(io.clone(), db.pool().clone(), db.store().clone(), root)
}

type Runner<'a> = dyn Fn(&mut dyn FnMut() -> Result<IOResult<()>>) + 'a;

fn run_to<T>(run: &Runner<'_>, mut op: impl FnMut() -> Result<IOResult<T>>) -> T {
    let mut out = None;
    run(&mut || {
        out = Some(match op()? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        });
        Ok(IOResult::Done(()))
    });
    out.unwrap()
}

fn get(tree: &Tree, run: &Runner<'_>, key: &[u8]) -> Option<Vec<u8>> {
    let mut op = Lookup::new(key);
    run_to(run, || tree.lookup(&mut op))
}

/// Every entry, walked forward or backward with a cursor.
fn scan(tree: &Tree, run: &Runner<'_>, forward: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut cursor = Cursor::new();
    let mut out = Vec::new();
    let (start, seek) = if forward {
        (Vec::new(), Seek::Ge)
    } else {
        (vec![0xff; 4096], Seek::Le)
    };
    let mut found = run_to(run, || tree.seek(&mut cursor, &start, seek));
    while found {
        out.push((
            cursor.key().unwrap().to_vec(),
            cursor.value().unwrap().to_vec(),
        ));
        found = if forward {
            run_to(run, || tree.next(&mut cursor))
        } else {
            run_to(run, || tree.prev(&mut cursor))
        };
    }
    if !forward {
        out.reverse();
    }
    out
}

/// Checks the tree's structure, and that the pages allocated in the store are exactly those of
/// its nodes and values: nothing leaked, nothing freed that is in use.
fn check_whole(tree: &Tree, db: &Atoll, run: &Runner<'_>, label: &str) -> Stats {
    check_reachable(tree, db, run, label, false)
}

/// [`check_whole`], but for pages handed over to be freed and not freed yet: with `handed_over`,
/// those are allocated and unreachable, and every page allocated and unreachable is one of them.
fn check_reachable(
    tree: &Tree,
    db: &Atoll,
    run: &Runner<'_>,
    label: &str,
    handed_over: bool,
) -> Stats {
    let mut check = Check::default();
    let stats = run_to(run, || tree.check(&mut check));
    let mut expected: BTreeSet<PageId> = stats.pages.iter().copied().collect();
    for &v in &stats.values {
        // The tree is still: its leaves keep their references.
        let pages = run_to(run, || value::pages(db.pool(), v, &|| true));
        expected.extend(pages.unwrap_or_else(|_| panic!("{label}: value {v:?} lost its pages")));
    }
    let mut allocated = BTreeSet::new();
    if tree.temporary() {
        allocated.extend(db.pool().temporary_pages());
    }
    for c in 0..CLASS_COUNT {
        let class = class(c);
        for number in 0..crate::storage::atoll::testing::page_numbers(db.store(), class) {
            let pid = PageId::new(class, number);
            if run_to(run, || db.store().entry(pid)) != FREE {
                allocated.insert(pid);
            }
        }
    }
    let orphans: BTreeSet<PageId> = db.store().orphans().into_iter().collect();
    if handed_over {
        expected.extend(&orphans);
    }
    let leaked: Vec<_> = allocated.difference(&expected).collect();
    let lost: Vec<_> = expected.difference(&allocated).collect();
    assert!(
        leaked.is_empty() && lost.is_empty(),
        "{label}: pages allocated and unreachable {leaked:?}; reachable and free {lost:?}; \
         handed over to be freed {:?}",
        db.store().orphans()
    );
    stats
}

fn drive_runner(io: &Arc<dyn IO>) -> impl Fn(&mut dyn FnMut() -> Result<IOResult<()>>) + '_ {
    move |op| drive(io, op).unwrap()
}

/// Keys from a small alphabet, so prefixes and equal heads are common, with some near the
/// longest a node takes.
fn random_key(rng: &mut ChaCha8Rng, max: usize) -> Vec<u8> {
    let len = if rng.random_range(0..100) == 0 {
        rng.random_range(max - 20..=max)
    } else {
        rng.random_range(1..24)
    };
    (0..len)
        .map(|_| b"abcd\0\xff"[rng.random_range(0..6)])
        .collect()
}

/// Values mostly inline, some out of line in one blob page.
fn random_value(rng: &mut ChaCha8Rng, key: &[u8], version: u64) -> Vec<u8> {
    let len = match rng.random_range(0..100) {
        0..70 => rng.random_range(0..100),
        70..95 => rng.random_range(100..1500),
        _ => rng.random_range(2_000..40_000),
    };
    stamp(key, version, len)
}

/// A value that names its key and version, so a reader can tell it is whole and whose it is.
fn stamp(key: &[u8], version: u64, len: usize) -> Vec<u8> {
    let mut v = version.to_le_bytes().to_vec();
    v.extend((key.len() as u32).to_le_bytes());
    v.extend_from_slice(key);
    let mut x = version.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    while v.len() < len.max(12 + key.len()) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.push(x as u8);
    }
    v
}

fn stamped_key(value: &[u8]) -> Option<&[u8]> {
    let len = u32::from_le_bytes(value.get(8..12)?.try_into().ok()?) as usize;
    value.get(12..12 + len)
}

/// A new tree, logged or temporary.
fn create(io: &Arc<dyn IO>, db: &Atoll, run: &Runner<'_>, temporary: bool) -> Tree {
    let root = run_to(run, || {
        if temporary {
            Tree::create_temporary(db.pool(), class(0))
        } else {
            Tree::create(db.pool(), class(0))
        }
    });
    tree_of(io, db, root)
}

/// A full checkpoint, done: every page owed an image written, and its superblock in charge.
fn checkpoint(db: &Atoll, run: &Runner<'_>) {
    let ticket = db.checkpointer().request_full();
    run(&mut || db.checkpointer().step_full(ticket, db.transactions()));
}

/// One step of the full checkpoint `ticket`, the IO it started waited out: true once it is done.
fn step_checkpoint(io: &Arc<dyn IO>, db: &Atoll, ticket: u64) -> bool {
    match db
        .checkpointer()
        .step_full(ticket, db.transactions())
        .unwrap()
    {
        IOResult::Done(()) => true,
        IOResult::IO(IOCompletions(c)) => {
            while !c.finished() {
                io.step().unwrap();
            }
            false
        }
    }
}

#[test]
fn a_tree_holds_what_a_sorted_map_holds_through_puts_removes_merges_and_eviction() {
    holds_what_a_sorted_map_holds(false);
}

#[test]
fn a_temporary_tree_holds_what_a_sorted_map_holds() {
    holds_what_a_sorted_map_holds(true);
}

fn holds_what_a_sorted_map_holds(temporary: bool) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, MIN_DB_BUDGET, 1);
    let run = drive_runner(&io);
    let tree = create(&io, &db, &run, temporary);
    let max = tree.limits().max_key;
    let mut ticket = None;
    let mut rng = ChaCha8Rng::seed_from_u64(1);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut version = 0u64;
    let put = |key: &[u8], value: Vec<u8>, mode: Mode| {
        let mut op = Put::new(key, value, mode);
        run_to(&run, || tree.put(&mut op))
    };
    let remove = |key: &[u8]| {
        let mut op = Remove::new(key);
        run_to(&run, || tree.remove(&mut op))
    };
    let compare = |model: &BTreeMap<Vec<u8>, Vec<u8>>, label: &str| {
        let expected: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert!(
            scan(&tree, &run, true) == expected,
            "{label}: a forward scan differs"
        );
        assert!(
            scan(&tree, &run, false) == expected,
            "{label}: a backward scan differs"
        );
        let stats = check_whole(&tree, &db, &run, label);
        assert_eq!(stats.entries, model.len(), "{label}");
    };
    // In order first: the right edge splits so that nodes fill.
    for i in 0..3000u32 {
        let key = format!("seq{i:08}").into_bytes();
        version += 1;
        let value = stamp(&key, version, 40);
        assert_eq!(put(&key, value.clone(), Mode::Insert), Outcome::Inserted);
        model.insert(key, value);
    }
    let stats = check_whole(&tree, &db, &run, "in order");
    assert!(
        stats.nodes < 3000 * 60 / 3964 + 40,
        "{} nodes for 3000 entries inserted in order",
        stats.nodes
    );
    for op in 0..12_000u32 {
        let key = if rng.random_range(0..3) == 0 && !model.is_empty() {
            let skip = rng.random_range(0..model.len());
            model.keys().nth(skip).unwrap().clone()
        } else {
            random_key(&mut rng, max)
        };
        version += 1;
        let present = model.contains_key(&key);
        match rng.random_range(0..10) {
            0..4 => {
                let value = random_value(&mut rng, &key, version);
                let mode = [Mode::Insert, Mode::Update, Mode::Upsert][rng.random_range(0..3)];
                let expected = match (mode, present) {
                    (Mode::Insert, true) | (Mode::Update, false) => Outcome::Unchanged,
                    (_, true) => Outcome::Updated,
                    (_, false) => Outcome::Inserted,
                };
                assert_eq!(put(&key, value.clone(), mode), expected, "op {op}");
                if expected != Outcome::Unchanged {
                    model.insert(key, value);
                }
            }
            4..7 => {
                assert_eq!(remove(&key), present, "op {op}");
                model.remove(&key);
            }
            _ => assert_eq!(get(&tree, &run, &key), model.get(&key).cloned(), "op {op}"),
        }
        // A checkpoint steps between the changes: a page changed after its cut keeps what it
        // writes.
        if op % 1000 == 0 {
            ticket.get_or_insert_with(|| db.checkpointer().request_full());
        }
        if let Some(t) = ticket {
            if step_checkpoint(&io, &db, t) {
                ticket = None;
            }
        }
        if op % 3000 == 2999 {
            compare(&model, &format!("op {op}"));
        }
    }
    // Remove nearly everything: nodes merge and the root comes down.
    let before = check_whole(&tree, &db, &run, "before removing").nodes;
    let keys: Vec<_> = model.keys().cloned().collect();
    for (i, key) in keys.iter().enumerate() {
        if i % 50 != 0 {
            assert!(remove(key));
            model.remove(key);
        }
    }
    checkpoint(&db, &run);
    compare(&model, "after removing");
    let after = check_whole(&tree, &db, &run, "after removing").nodes;
    assert!(
        after * 10 <= before,
        "{after} nodes left of {before} for {} entries",
        model.len()
    );
    for key in model.keys().cloned().collect::<Vec<_>>() {
        assert!(remove(&key));
    }
    checkpoint(&db, &run);
    model.clear();
    let stats = check_whole(&tree, &db, &run, "empty");
    assert_eq!(stats.nodes, 1, "an empty tree is its root");
    if !temporary {
        // What the checkpoint wrote is the tree, whole, with nothing else in use.
        let root = tree.root();
        drop((tree, db));
        let db = open(&io, MIN_DB_BUDGET, 1);
        let tree = tree_of(&io, &db, root);
        assert!(scan(&tree, &run, true).is_empty(), "reopened");
        let stats = check_whole(&tree, &db, &run, "reopened");
        assert_eq!(stats.nodes, 1, "reopened");
    }
}

/// Keys appended in order past the rightmost leaf split it keeping every entry, under a new upper
/// fence: the separator, which keys sharing a long start make long. A leaf too full to take that
/// fence splits in half instead.
#[test]
fn keys_appended_with_long_shared_starts_split_leaves_that_fit() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 64 << 20, 1);
    let run = drive_runner(&io);
    let root = run_to(&run, || Tree::create(db.pool(), class(0)));
    let tree = tree_of(&io, &db, root);
    let start = "x".repeat(300);
    let keys: Vec<Vec<u8>> = (0..400)
        .map(|i| format!("{start}{i:05}").into_bytes())
        .collect();
    for key in &keys {
        let mut put = Put::new(key, key.clone(), Mode::Upsert);
        run_to(&run, || tree.put(&mut put));
    }
    let entries = scan(&tree, &run, true);
    assert_eq!(entries.len(), keys.len());
    assert!(entries
        .iter()
        .zip(&keys)
        .all(|((k, v), key)| k == key && v == key));
    check_whole(&tree, &db, &run, "appended");
}

#[test]
fn values_longer_than_a_chunk_are_written_read_and_freed_whole() {
    long_values(false);
}

#[test]
fn a_head_stays_in_the_leaf_when_the_rest_of_its_value_goes_out_of_line() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 64 << 20, 1);
    let run = drive_runner(&io);
    let plain = create(&io, &db, &run, false);
    let tree = tree_of(&io, &db, plain.root()).with_head(16);
    assert_eq!(tree.limits().max_key, plain.limits().max_key - 16);
    for (i, len) in [10usize, 900, 5000, value::chunk() + 7]
        .into_iter()
        .enumerate()
    {
        let key = format!("k{i}").into_bytes();
        let head = vec![i as u8 + 1; 16];
        let value = stamp(&key, i as u64, len);
        let mut op = Put::with_head(&key, head.clone(), value.clone(), Mode::Insert);
        assert_eq!(run_to(&run, || tree.put(&mut op)), Outcome::Inserted);
        let whole: Vec<u8> = head.iter().chain(&value).copied().collect();
        assert!(
            get(&tree, &run, &key) == Some(whole.clone()),
            "{len}: read back"
        );
        let stored = run_to(&run, || tree.stored(&key)).expect("stored");
        assert_eq!(stored.head, head, "{len}: the head");
        match stored.value {
            StoredValue::Inline(v) => assert!(v == value && len < 1000, "{len}: inline"),
            StoredValue::OutOfLine(r) => {
                assert!(len >= 1000, "{len}: out of line");
                assert_eq!(r.len as usize, len);
            }
        }
        let scanned = scan(&tree, &run, true);
        assert!(scanned.iter().any(|(k, v)| *k == key && *v == whole));
    }
    check_whole(&tree, &db, &run, "with heads");
}

#[test]
fn a_temporary_trees_long_values_are_temporary_pages() {
    long_values(true);
}

fn long_values(temporary: bool) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 64 << 20, 1);
    let run = drive_runner(&io);
    let tree = create(&io, &db, &run, temporary);
    let chunk = value::chunk();
    let sizes = [chunk, chunk + 1, 2 * chunk + 12_345];
    for (i, &len) in sizes.iter().enumerate() {
        let key = format!("big{i}").into_bytes();
        let value = stamp(&key, i as u64, len);
        let mut op = Put::new(&key, value.clone(), Mode::Insert);
        assert_eq!(run_to(&run, || tree.put(&mut op)), Outcome::Inserted);
        assert!(
            get(&tree, &run, &key) == Some(value),
            "a {len} byte value read back"
        );
    }
    let entries = scan(&tree, &run, true);
    assert_eq!(entries.len(), 3);
    for (i, (key, value)) in entries.iter().enumerate() {
        assert_eq!(value.len(), sizes[i]);
        assert_eq!(stamped_key(value), Some(key.as_slice()));
    }
    check_whole(&tree, &db, &run, "written");
    // Replaced by a small value, then removed: every page of the long one is freed.
    let mut op = Put::new(b"big2", b"small".to_vec(), Mode::Update);
    assert_eq!(run_to(&run, || tree.put(&mut op)), Outcome::Updated);
    checkpoint(&db, &run);
    check_whole(&tree, &db, &run, "replaced");
    for i in 0..3 {
        let mut op = Remove::new(format!("big{i}").as_bytes());
        assert!(run_to(&run, || tree.remove(&mut op)));
    }
    checkpoint(&db, &run);
    let stats = check_whole(&tree, &db, &run, "removed");
    assert_eq!((stats.nodes, stats.entries), (1, 0));
}

#[test]
fn concurrent_writers_and_scans_keep_every_key_whole_and_in_order() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 64 << 20, 1);
    let run = drive_runner(&io);
    let root = run_to(&run, || Tree::create(db.pool(), class(0)));
    let tree = tree_of(&io, &db, root);
    let writers = 4u64;
    let shared: Vec<Vec<u8>> = (0..40).map(|i| format!("hot{i:03}").into_bytes()).collect();
    let stop = crate::sync::atomic::AtomicBool::new(false);
    let models: Vec<BTreeMap<Vec<u8>, Vec<u8>>> = std::thread::scope(|s| {
        let scanners: Vec<_> = (0..2)
            .map(|_| {
                s.spawn(|| {
                    let run = drive_runner(&io);
                    let mut scans = 0;
                    while !stop.load(Ordering::Acquire) {
                        let entries = scan(&tree, &run, scans % 2 == 0);
                        for w in entries.windows(2) {
                            assert!(w[0].0 < w[1].0, "a scan out of order");
                        }
                        for (k, v) in &entries {
                            assert_eq!(
                                stamped_key(v),
                                Some(k.as_slice()),
                                "a scan read a torn value"
                            );
                        }
                        scans += 1;
                    }
                    assert!(scans > 0);
                })
            })
            .collect();
        let handles: Vec<_> = (0..writers)
            .map(|t| {
                let (io, tree, db, shared) = (&io, &tree, &db, &shared);
                s.spawn(move || {
                    let run = drive_runner(io);
                    let mut rng = ChaCha8Rng::seed_from_u64(100 + t);
                    let mut model = BTreeMap::new();
                    for op in 0..2500u64 {
                        let version = t << 32 | op;
                        if rng.random_range(0..5) == 0 {
                            let key = &shared[rng.random_range(0..shared.len())];
                            let mut put =
                                Put::new(key, random_value(&mut rng, key, version), Mode::Upsert);
                            run_to(&run, || tree.put(&mut put));
                            continue;
                        }
                        let key = format!("t{t}-{:05}", rng.random_range(0..800)).into_bytes();
                        if rng.random_range(0..3) == 0 {
                            let mut remove = Remove::new(&key);
                            let removed = run_to(&run, || tree.remove(&mut remove));
                            assert_eq!(removed, model.remove(&key).is_some());
                        } else {
                            let value = random_value(&mut rng, &key, version);
                            let mut put = Put::new(&key, value.clone(), Mode::Upsert);
                            run_to(&run, || tree.put(&mut put));
                            model.insert(key.clone(), value);
                        }
                        assert_eq!(get(tree, &run, &key), model.get(&key).cloned());
                        if op % 64 == 0 {
                            checkpoint(db, &run);
                        }
                    }
                    model
                })
            })
            .collect();
        let models: Vec<_> = handles.into_iter().map(|h| h.join()).collect();
        stop.store(true, Ordering::Release);
        let models = models.into_iter().map(|m| m.unwrap()).collect();
        for h in scanners {
            h.join().unwrap();
        }
        models
    });
    let entries = scan(&tree, &run, true);
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = models.into_iter().flatten().collect();
    for (k, v) in &entries {
        if k.starts_with(b"hot") {
            assert_eq!(stamped_key(v), Some(k.as_slice()));
            expected.insert(k.clone(), v.clone());
        }
    }
    let expected: Vec<_> = expected.into_iter().collect();
    assert!(
        entries == expected,
        "the tree differs from what the writers wrote"
    );
    checkpoint(&db, &run);
    check_whole(&tree, &db, &run, "after the writers");
}

/// The tree each checkpoint wrote, by its number: as the tree stood at that checkpoint's cut.
type AtCut = BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>;

/// A plain tree is the checkpoint in charge's (audit 18): a crash anywhere recovers it exactly as
/// the checkpoint whose superblock landed last cut it, whole, that checkpoint never older than the
/// last one acknowledged. Checkpoints step between the changes, so the pages a change after a cut
/// touches are written as their first change since kept them, and a crash lands inside every
/// phase of one.
fn crashes_recover_the_tree_the_checkpoint_in_charge_cut(durable_writes: bool) {
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        crash_io.set_durable_writes(durable_writes);
        let io: Arc<dyn IO> = crash_io.clone();
        let db = open(&io, MIN_DB_BUDGET, 1);
        let at_cut: RefCell<AtCut> = RefCell::new(BTreeMap::new());
        let model: RefCell<BTreeMap<Vec<u8>, Vec<u8>>> = RefCell::new(BTreeMap::new());
        let acknowledged = Cell::new(0u64);
        let steps = Cell::new(0u64);
        let checks = Cell::new(0u64);
        let in_charge_seen = RefCell::new(BTreeSet::new());
        let root = Cell::new(None);
        let crash_and_check = |crash_seed: u64| {
            let Some(root) = root.get() else {
                return;
            };
            let threads = 1 + (crash_seed % 3) as usize;
            let crashed: Arc<dyn IO> = Arc::new(crash_io.crash(crash_seed));
            let label = format!("seed {seed}/{crash_seed}, {threads} threads");
            let recovered = open(&crashed, MIN_DB_BUDGET, threads);
            // Recovery ends with a full checkpoint of its own, one past the one in charge.
            let in_charge = recovered.checkpointer().superblock().checkpoint - 1;
            in_charge_seen.borrow_mut().insert(in_charge);
            assert!(
                in_charge >= acknowledged.get(),
                "{label}: checkpoint {in_charge} in charge, before {}, which was acknowledged",
                acknowledged.get()
            );
            let expected = at_cut
                .borrow()
                .get(&in_charge)
                .cloned()
                .unwrap_or_else(|| panic!("{label}: checkpoint {in_charge} in charge, uncut"));
            let verify = |db: &Atoll, label: &str| {
                let run = drive_runner(&crashed);
                let tree = tree_of(&crashed, db, root);
                let entries: BTreeMap<_, _> = scan(&tree, &run, true).into_iter().collect();
                assert!(
                    entries == expected,
                    "{label}: the tree differs from checkpoint {in_charge}'s cut"
                );
                for (k, v) in &expected {
                    assert!(get(&tree, &run, k).as_ref() == Some(v), "{label}: {k:?}");
                }
                check_whole(&tree, db, &run, label);
            };
            verify(&recovered, &label);
            drop(recovered);
            // What recovery left recovers again to the same state.
            verify(
                &open(&crashed, MIN_DB_BUDGET, 1),
                &format!("{label}, reopened"),
            );
            checks.set(checks.get() + 1);
        };
        let wait = |c: &crate::io::Completion| {
            while !c.finished() {
                io.step().unwrap();
                steps.set(steps.get() + 1);
                if steps.get() % 5 == 0 {
                    crash_and_check(seed * 1_000_000 + steps.get());
                }
            }
        };
        let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| loop {
            match op().unwrap() {
                IOResult::Done(()) => return,
                IOResult::IO(IOCompletions(c)) => wait(&c),
            }
        };
        // One step of the full checkpoint `ticket`: true once it is done. The tree as the step
        // found it is the tree at any cut it took.
        let step = |ticket: u64| {
            let cp = db.checkpointer();
            let out = cp.step_full(ticket, db.transactions()).unwrap();
            let n = cp.cut_taken().unwrap_or_else(|| cp.superblock().checkpoint);
            at_cut
                .borrow_mut()
                .entry(n)
                .or_insert_with(|| model.borrow().clone());
            match out {
                IOResult::Done(()) => {
                    acknowledged.set(cp.superblock().checkpoint);
                    true
                }
                IOResult::IO(IOCompletions(c)) => {
                    wait(&c);
                    false
                }
            }
        };
        let created = run_to(&run, || Tree::create(db.pool(), class(0)));
        let first = db.checkpointer().request_full();
        while !step(first) {}
        root.set(Some(created));
        let tree = tree_of(&io, &db, created);
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut version = 0u64;
        let mut ticket = None;
        for op in 0..1400u32 {
            let key = format!("k{:04}", rng.random_range(0..600)).into_bytes();
            version += 1;
            if rng.random_range(0..3) == 0 {
                let mut remove = Remove::new(&key);
                run_to(&run, || tree.remove(&mut remove));
                model.borrow_mut().remove(&key);
            } else {
                let value = random_value(&mut rng, &key, version);
                let mut put = Put::new(&key, value.clone(), Mode::Upsert);
                run_to(&run, || tree.put(&mut put));
                model.borrow_mut().insert(key, value);
            }
            if op % 50 == 0 {
                ticket.get_or_insert_with(|| db.checkpointer().request_full());
            }
            if let Some(t) = ticket {
                if step(t) {
                    ticket = None;
                }
            }
        }
        assert!(
            checks.get() >= 100,
            "seed {seed}: only {} crashes",
            checks.get()
        );
        assert!(
            in_charge_seen.borrow().len() >= 2,
            "seed {seed}: every crash found checkpoint {:?} in charge",
            in_charge_seen.borrow()
        );
        crash_and_check(seed);
    }
}

#[test]
fn crashes_recover_the_tree_the_checkpoint_in_charge_cut_with_durable_writes() {
    crashes_recover_the_tree_the_checkpoint_in_charge_cut(true);
}

#[test]
fn crashes_recover_the_tree_the_checkpoint_in_charge_cut_with_writes_then_syncs() {
    crashes_recover_the_tree_the_checkpoint_in_charge_cut(false);
}

/// A leaf a caller reached last ([`Near`]) is used only for the keys it answers: a key between
/// its fences, the upper included, and the keys just above one from its lower fence to below its
/// upper. From every leaf of a tree, every seek from each fence and from a key inside lands where
/// a sorted map says, a seek above an upper fence going on to the next leaf, and a put of a key
/// another leaf holds lands there.
#[test]
fn a_leaf_a_caller_reached_answers_only_the_keys_between_its_fences() {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, MIN_DB_BUDGET, 1);
    let run = drive_runner(&io);
    let tree = create(&io, &db, &run, false);
    let key = |n: u64| format!("k{n:05}").into_bytes();
    let mut map = BTreeMap::new();
    for n in 0..1_000 {
        let value = vec![n as u8; 100];
        let mut op = Put::new(&key(n), value.clone(), Mode::Insert);
        run_to(&run, || tree.put(&mut op));
        map.insert(key(n), value);
    }
    // Every leaf, in order, by the leaf a scan read each entry in.
    let mut leaves: Vec<Near> = Vec::new();
    let mut cursor = Cursor::new();
    let mut more = run_to(&run, || tree.seek(&mut cursor, &[], Seek::Ge));
    while more {
        if leaves.last() != Some(&cursor.near()) {
            leaves.push(cursor.near());
        }
        more = run_to(&run, || tree.next(&mut cursor));
    }
    assert!(leaves.len() >= 10, "only {} leaves", leaves.len());
    let fences = |near: Near| {
        let (pid, hint) = near.0.expect("a leaf");
        let leaf = db
            .pool()
            .fix_optimistic_at(pid, hint)
            .expect("in its frame");
        leaf.read(|p| {
            let v = View::new(p);
            Some((v.lower()?, v.upper()?))
        })
        .flatten()
        .expect("fences")
    };
    let expected = |map: &BTreeMap<Vec<u8>, Vec<u8>>, target: &[u8], seek: Seek| {
        let t = target.to_vec();
        match seek {
            Seek::Ge => map.range((Included(t), Unbounded)).next(),
            Seek::Gt => map.range((Excluded(t), Unbounded)).next(),
            Seek::Le => map.range((Unbounded, Included(t))).next_back(),
            Seek::Lt => map.range((Unbounded, Excluded(t))).next_back(),
        }
        .map(|(k, _)| k.clone())
    };
    let (mut seeks, mut at_answered, mut above_answered) = (0, 0, 0);
    for (i, &near) in leaves.iter().enumerate() {
        let (lower, upper) = fences(near);
        // Each fence, the leaf's first key, and a key just after it that the tree lacks.
        let first = match &lower {
            Some(l) => map.range::<Vec<u8>, _>((Excluded(l), Unbounded)).next(),
            None => map.iter().next(),
        }
        .map(|(k, _)| k.clone())
        .unwrap();
        let after_first = [first.as_slice(), &[0]].concat();
        let mut targets: Vec<Vec<u8>> = [lower.clone(), upper.clone()]
            .into_iter()
            .flatten()
            .collect();
        targets.extend([first, after_first]);
        for target in &targets {
            let (pid, hint) = near.0.unwrap();
            let leaf = db.pool().fix_optimistic_at(pid, hint).unwrap();
            let (at, above) = leaf
                .read(|p| {
                    let v = View::new(p);
                    Some((v.covers(target)?, v.covers_above(target)?))
                })
                .flatten()
                .unwrap();
            let after_lower = lower
                .as_ref()
                .is_none_or(|l| l.as_slice() < target.as_slice());
            let from_lower = lower
                .as_ref()
                .is_none_or(|l| l.as_slice() <= target.as_slice());
            let to_upper = upper
                .as_ref()
                .is_none_or(|u| target.as_slice() <= u.as_slice());
            let below_upper = upper
                .as_ref()
                .is_none_or(|u| target.as_slice() < u.as_slice());
            assert_eq!(at, after_lower && to_upper, "leaf {i} at {target:?}");
            assert_eq!(
                above,
                from_lower && below_upper,
                "leaf {i} above {target:?}"
            );
            at_answered += at as usize;
            above_answered += above as usize;
            for seek in [Seek::Ge, Seek::Gt, Seek::Le, Seek::Lt] {
                let mut cursor = Cursor::new();
                cursor.place(b"", near);
                let taken = NEAR_TAKEN.with(|n| n.get());
                let found = run_to(&run, || tree.seek(&mut cursor, target, seek));
                let answers = if seek == Seek::Gt { above } else { at };
                assert_eq!(
                    NEAR_TAKEN.with(|n| n.get()) - taken,
                    answers as u64,
                    "leaf {i}, {seek:?} from {target:?}: the leaf near taken"
                );
                let got = found.then(|| cursor.key().unwrap().to_vec());
                assert_eq!(
                    got,
                    expected(&map, target, seek),
                    "leaf {i}, {seek:?} from {target:?}"
                );
                seeks += 1;
            }
        }
        // A key the next leaf holds, put from this one: it lands in the next.
        if let Some(&next) = leaves.get(i + 1) {
            let (_, upper) = fences(near);
            let (k, _) = map
                .range((Excluded(upper.unwrap()), Unbounded))
                .next()
                .unwrap();
            let k = k.clone();
            let mut op = Put::new(&k, vec![0xaa; 100], Mode::Update).near(near);
            assert_eq!(run_to(&run, || tree.put(&mut op)), Outcome::Updated);
            assert_eq!(
                op.reached().page(),
                next.page(),
                "leaf {i}'s neighbour's key"
            );
            map.insert(k.clone(), vec![0xaa; 100]);
            assert_eq!(get(&tree, &run, &k), Some(vec![0xaa; 100]));
        }
    }
    // Every leaf answered its first key, at it and above it: the leaf near was taken that often.
    assert!(
        seeks >= 4 * 4 * 10 && at_answered >= 2 * leaves.len() && above_answered >= leaves.len(),
        "{seeks} seeks, {at_answered} at and {above_answered} above answered"
    );
    assert_eq!(scan(&tree, &run, true), map.into_iter().collect::<Vec<_>>());
}

/// A value out of line whose page no longer carries the value's id, as corruption leaves it, is
/// read again by its key once, then refused, though its leaf is unchanged: a scan that steps onto
/// it reports it and never steps past it to the next entry.
#[test]
fn a_scan_refuses_a_value_out_of_line_its_page_no_longer_names_rather_than_step_past_it() {
    use crate::storage::atoll::page::HEADER_SIZE;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, MIN_DB_BUDGET, 1);
    let run = drive_runner(&io);
    let tree = create(&io, &db, &run, false);
    for (key, len) in [(&b"k0"[..], 10), (b"k1", 2_000), (b"k2", 10)] {
        let mut op = Put::new(key, vec![1u8; len], Mode::Insert);
        run_to(&run, || tree.put(&mut op));
    }
    let stored = run_to(&run, || tree.stored(b"k1")).expect("k1");
    let StoredValue::OutOfLine(reference) = stored.value else {
        panic!("k1's value is inline");
    };
    let pages = run_to(&run, || value::pages(db.pool(), reference, &|| true))
        .unwrap_or_else(|_| panic!("k1's pages unread"));
    {
        let mut page = run_to(&run, || db.pool().fix_exclusive(pages[0], None));
        let id = page.page().u64_at(HEADER_SIZE);
        page.page_mut().put_u64(HEADER_SIZE, !id);
    }
    let mut cursor = Cursor::new();
    assert!(run_to(&run, || tree.seek(&mut cursor, b"", Seek::Ge)));
    assert_eq!(cursor.key(), Some(&b"k0"[..]));
    match drive(&io, || tree.next(&mut cursor)) {
        Err(LimboError::Corrupt(_)) => {}
        Err(e) => panic!("the step onto k1 failed otherwise: {e}"),
        Ok(found) => panic!("the step onto k1 came to {found}, at {:?}", cursor.key()),
    }
}

/// A look kept past its step is given back while its leaf is unchanged, with the place and entry
/// it found, and a put goes in there; once the leaf changed it is not, and nothing answers from it.
#[test]
fn a_kept_look_is_given_back_only_while_its_leaf_is_unchanged() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, MIN_DB_BUDGET, 1);
    let run = drive_runner(&io);
    let tree = create(&io, &db, &run, false);
    let put = |key: &[u8], value: &[u8]| {
        let mut op = Put::new(key, value.to_vec(), Mode::Upsert);
        run_to(&run, || tree.put(&mut op))
    };
    put(b"a", b"1");
    put(b"c", b"3");
    let kept = run_to(&run, || tree.look(b"b", Near::NONE)).keep();
    assert!(tree.holds_kept(&kept));
    let looked = tree.redeem(kept).expect("the leaf is unchanged");
    assert!(looked.present().is_none());
    let mut op = Put::new(b"b", b"2".to_vec(), Mode::Upsert);
    let mut looked = Some(looked);
    let put_there = run_to(&run, || match looked.take() {
        Some(l) => tree.put_looked(&mut op, l),
        None => tree.put(&mut op),
    });
    assert_eq!(put_there, Outcome::Inserted);
    let kept = run_to(&run, || tree.look(b"b", Near::NONE)).keep();
    assert_eq!(kept.present(), Some((&b"2"[..], false)));
    put(b"d", b"4");
    assert!(!tree.holds_kept(&kept), "the leaf changed");
    assert!(tree.redeem(kept).is_none(), "the leaf changed");
    assert_eq!(
        scan(&tree, &run, true),
        [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")]
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .to_vec()
    );
}

#[test]
fn a_put_expecting_an_entry_changes_nothing_when_the_entry_differs() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, MIN_DB_BUDGET, 1);
    let run = drive_runner(&io);
    let tree = create(&io, &db, &run, false).with_head(4);
    let put = |value: &[u8], expected: Option<&Stored>| {
        let mut op =
            Put::with_head(b"k", vec![1; 4], value.to_vec(), Mode::Upsert).expecting(expected);
        run_to(&run, || tree.put(&mut op))
    };
    let stored = |run: &Runner<'_>| run_to(run, || tree.stored(b"k"));
    assert_eq!(put(b"one", None), Outcome::Inserted);
    let first = stored(&run).unwrap();
    assert_eq!(put(b"two", None), Outcome::Changed, "expected absent");
    assert_eq!(put(b"two", Some(&first)), Outcome::Updated);
    assert_eq!(
        put(b"three", Some(&first)),
        Outcome::Changed,
        "expected stale"
    );
    assert_eq!(
        get(&tree, &run, b"k"),
        Some([&[1u8; 4][..], b"two"].concat())
    );
}

/// Two writers put and remove keys, each its own and both two shared ones, while a reader scans
/// both ways, four times. Values take most of an inline entry's room, so a node holds a few entries and
/// splits and merges come within a few operations. Scans see keys in order, each value whole and
/// its key's, and the tree ends holding what the writers left and checks whole.
fn concurrent_tree(seed: u64, ops: u64) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Arc::new(open(&io, MIN_DB_BUDGET, 1));
    let run = drive_runner(&io);
    let root = run_to(&run, || Tree::create(db.pool(), class(0)));
    let tree = Arc::new(tree_of(&io, &db, root));
    let shared: Vec<Vec<u8>> = (0..2).map(|i| format!("hot{i}").into_bytes()).collect();
    let max_entry = Limits::of(class(0)).max_entry;
    // A fixed number of scans, so a schedule's length does not depend on how the scanner's
    // turns fall against the writers'.
    let scanner = {
        let (io, tree) = (io.clone(), tree.clone());
        crate::thread::spawn(move || {
            let run = drive_runner(&io);
            for scans in 0..4 {
                let entries = scan(&tree, &run, scans % 2 == 0);
                for w in entries.windows(2) {
                    assert!(w[0].0 < w[1].0, "a scan out of order");
                }
                for (k, v) in &entries {
                    assert_eq!(
                        stamped_key(v),
                        Some(k.as_slice()),
                        "a scan read a torn value"
                    );
                }
            }
        })
    };
    let writers: Vec<_> = (0..2u64)
        .map(|t| {
            let (io, db, tree, shared) = (io.clone(), db.clone(), tree.clone(), shared.clone());
            crate::thread::spawn(move || {
                let run = drive_runner(&io);
                let mut rng = ChaCha8Rng::seed_from_u64(seed * 10 + t);
                let mut model = BTreeMap::new();
                for op in 0..ops {
                    let version = t << 32 | op;
                    let len = rng.random_range(max_entry / 2..max_entry - 40);
                    if rng.random_range(0..4) == 0 {
                        let key = &shared[rng.random_range(0..shared.len())];
                        let mut put = Put::new(key, stamp(key, version, len), Mode::Upsert);
                        run_to(&run, || tree.put(&mut put));
                        continue;
                    }
                    let key = format!("t{t}-{}", rng.random_range(0..8)).into_bytes();
                    if rng.random_range(0..3) == 0 {
                        let mut remove = Remove::new(&key);
                        let removed = run_to(&run, || tree.remove(&mut remove));
                        assert_eq!(removed, model.remove(&key).is_some());
                    } else {
                        let value = stamp(&key, version, len);
                        let mut put = Put::new(&key, value.clone(), Mode::Upsert);
                        run_to(&run, || tree.put(&mut put));
                        model.insert(key, value);
                    }
                    if op == ops / 2 {
                        checkpoint(&db, &run);
                    }
                }
                model
            })
        })
        .collect();
    let models: Vec<_> = writers.into_iter().map(|w| w.join().unwrap()).collect();
    scanner.join().unwrap();
    let entries = scan(&tree, &run, true);
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = models.into_iter().flatten().collect();
    for (k, v) in &entries {
        if k.starts_with(b"hot") {
            assert_eq!(stamped_key(v), Some(k.as_slice()));
            expected.insert(k.clone(), v.clone());
        }
    }
    assert!(
        entries == expected.into_iter().collect::<Vec<_>>(),
        "the tree differs from what the writers wrote"
    );
    // A merge whose free of the node it emptied had to wait hands the node to the checkpointer,
    // which frees it in its next checkpoint: until then those are the only pages unreachable.
    check_reachable(&tree, &db, &run, "schedules, before the checkpointer", true);
    checkpoint(&db, &run);
    check_whole(&tree, &db, &run, "schedules");
}

/// A tree's puts, checkpointed, its database then dropped and recovered on `threads` threads at
/// once: one recovers while the rest wait for it, every key reads back, and the tree is whole.
fn a_tree_recovered_on_threads_at_once_reads_back_whole(threads: usize, keys: u64) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let value = |k: u64| stamp(&k.to_be_bytes(), k, 40 + (k % 7) as usize * 30);
    let root = {
        let db = open(&io, MIN_DB_BUDGET, 1);
        let run = drive_runner(&io);
        let tree = create(&io, &db, &run, false);
        for k in 0..keys {
            let mut op = Put::new(&k.to_be_bytes(), value(k), Mode::Upsert);
            run_to(&run, || tree.put(&mut op));
        }
        checkpoint(&db, &run);
        tree.root()
    };
    let opening = Arc::new(Opening::new(io.clone(), BASE, config(MIN_DB_BUDGET)).unwrap());
    let others: Vec<_> = (1..threads)
        .map(|_| {
            let opening = opening.clone();
            crate::thread::spawn(move || opening.work().unwrap())
        })
        .collect();
    opening.work().unwrap();
    for t in others {
        t.join().unwrap();
    }
    let db = Arc::into_inner(opening)
        .expect("the opening, every thread done")
        .finish()
        .unwrap();
    assert!(db.recovered(), "the open did not recover");
    let run = drive_runner(&io);
    let tree = tree_of(&io, &db, root);
    for k in 0..keys {
        assert_eq!(
            get(&tree, &run, &k.to_be_bytes()),
            Some(value(k)),
            "key {k}"
        );
    }
    let stats = check_whole(&tree, &db, &run, "recovered");
    assert_eq!(stats.entries, keys as usize);
}

#[cfg(not(shuttle))]
#[test]
fn a_tree_recovered_on_threads_at_once_reads_back_whole_on_threads() {
    for threads in [1, 3, 12] {
        a_tree_recovered_on_threads_at_once_reads_back_whole(threads, 3_000);
    }
}

#[cfg(shuttle)]
#[test]
fn a_tree_recovered_on_threads_at_once_reads_back_whole_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || a_tree_recovered_on_threads_at_once_reads_back_whole(2, 300),
        100,
    );
}

#[cfg(shuttle)]
#[test]
fn concurrent_writers_and_scans_keep_every_key_whole_and_in_order_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || concurrent_tree(crate::storage::atoll::testing::workload_seed(), 12),
        300,
    );
}

/// The schedule test's workload on real threads, so it runs, and is stressed, without shuttle.
#[cfg(not(shuttle))]
#[test]
fn concurrent_writers_and_scans_through_splits_and_merges() {
    for seed in 0..20 {
        concurrent_tree(seed, 40);
    }
}

/// A node's children, in order.
#[cfg(not(shuttle))]
fn children(io: &Arc<dyn IO>, pool: &Arc<BufferPool>, pid: PageId) -> Vec<PageId> {
    let node = drive(io, || pool.fix_optimistic(pid, None)).unwrap();
    node.read(|p| {
        let v = View::new(p);
        let mut out = (0..v.count()?)
            .map(|i| v.child(i).map(|c| c.0))
            .collect::<Option<Vec<_>>>()?;
        out.push(v.next().0);
        Some(out)
    })
    .flatten()
    .expect("the node read whole")
}

/// The level of `pid`'s node, 0 for a leaf.
#[cfg(not(shuttle))]
fn level_of(io: &Arc<dyn IO>, pool: &Arc<BufferPool>, pid: PageId) -> u8 {
    let node = drive(io, || pool.fix_optimistic(pid, None)).unwrap();
    node.read(|p| View::new(p).level())
        .expect("the node read whole")
}

/// Each step that finds a page held as it upgrades it, or as it reads a leaf, comes back on the
/// wait the holder's release finishes, never on a yield it would come back from to find the page
/// still held; a page it upgraded before the held one goes back unchanged. Each operation then
/// ends as it would have, and the tree checks whole.
#[cfg(not(shuttle))]
#[test]
fn a_page_found_held_at_its_upgrade_or_read_waits_for_its_release() {
    const VALUE: usize = 400;
    let key = |i: usize| format!("k{i:05}").into_bytes();
    let mut outcomes = Vec::new();
    for case in [
        "put's leaf",
        "remove's leaf",
        "scan's leaf",
        "grow's root",
        "split's node",
        "merge's left",
        "merge's right",
        "shrink's child",
    ] {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open(&io, MIN_DB_BUDGET, 1);
        let run = drive_runner(&io);
        let tree = create(&io, &db, &run, false);
        let pool = db.pool().clone();
        let root = tree.root;
        let put = |i: usize| {
            let (tree, mut op) = (&tree, Put::new(&key(i), vec![i as u8; VALUE], Mode::Upsert));
            move || tree.put(&mut op).map(|r| r.map(drop))
        };
        // Two keys in one leaf, or keys until the root first has two children.
        let two_levels = !matches!(
            case,
            "put's leaf" | "remove's leaf" | "scan's leaf" | "grow's root"
        );
        let mut n = 0;
        if two_levels {
            while level_of(&io, &pool, root) == 0 {
                run(&mut put(n));
                n += 1;
            }
        } else {
            for i in [0, 2] {
                run(&mut put(i));
            }
        }
        let point = match case {
            "put's leaf" | "remove's leaf" => "btree.leaf_upgrade",
            "scan's leaf" => "btree.leaf_read",
            "grow's root" => "btree.grow_upgrade",
            "split's node" => "btree.split_upgrade",
            "merge's left" | "merge's right" => "btree.merge_upgrade",
            _ => "btree.shrink_upgrade",
        };
        let pick = move |io: &Arc<dyn IO>, pool: &Arc<BufferPool>| match case {
            "put's leaf" | "remove's leaf" | "scan's leaf" | "grow's root" => (vec![], vec![root]),
            // The parent upgraded first; every leaf held, so the one split is.
            "split's node" => (vec![root], children(io, pool, root)),
            "merge's left" => (vec![root], vec![children(io, pool, root)[0]]),
            "merge's right" => {
                let c = children(io, pool, root);
                (vec![root, c[0]], vec![c[1]])
            }
            _ => (vec![root], children(io, pool, root)),
        };
        let hold = HoldAt::set(point, &io, &pool, pick);
        let mut cursor = Cursor::new();
        let mut removed = n;
        let mut passed = None;
        // Each case's operations until one passes the point.
        while passed.is_none() {
            passed = match case {
                "put's leaf" => step_past(&io, point, put(1)),
                "remove's leaf" => {
                    let mut op = Remove::new(&key(0));
                    step_past(&io, point, || tree.remove(&mut op).map(|r| r.map(drop)))
                }
                "scan's leaf" => step_past(&io, point, || {
                    tree.seek(&mut cursor, b"", Seek::Ge).map(|r| r.map(drop))
                }),
                "grow's root" | "split's node" => {
                    n += 1;
                    step_past(&io, point, put(n))
                }
                _ => {
                    assert!(
                        removed > 0,
                        "{case}: every key removed, the point never passed"
                    );
                    removed -= 1;
                    let mut op = Remove::new(&key(removed));
                    step_past(&io, point, || tree.remove(&mut op).map(|r| r.map(drop)))
                }
            };
        }
        let wait = match passed.unwrap() {
            IOResult::IO(IOCompletions(c)) => Some(c),
            IOResult::Done(()) => None,
        };
        // An unrelated wake: room given back finishes the global supply, never this wait.
        pool.wake_room();
        let pending = wait.as_ref().is_some_and(|c| !c.finished());
        let before = hold.versions().expect("the point passed");
        let watched = match case {
            "split's node" | "merge's left" | "shrink's child" => vec![root],
            "merge's right" => vec![root, children(&io, &pool, root)[0]],
            _ => vec![],
        };
        let after: Vec<u64> = watched
            .iter()
            .map(|&pid| {
                drive(&io, || pool.fix_optimistic(pid, None))
                    .unwrap()
                    .version()
            })
            .collect();
        hold.let_go();
        let woken = wait.as_ref().is_some_and(|c| c.finished());
        // The operation that passed the point, again, to its end; then the tree whole.
        match case {
            "put's leaf" => run(&mut put(1)),
            "remove's leaf" => {
                let mut op = Remove::new(&key(0));
                run(&mut || tree.remove(&mut op).map(|r| r.map(drop)));
            }
            "scan's leaf" => {
                run(&mut || tree.seek(&mut cursor, b"", Seek::Ge).map(|r| r.map(drop)));
                assert_eq!(
                    cursor.key(),
                    Some(&key(0)[..]),
                    "{case}: the scan's first key"
                );
            }
            "grow's root" | "split's node" => run(&mut put(n)),
            _ => {
                let mut op = Remove::new(&key(removed));
                run(&mut || tree.remove(&mut op).map(|r| r.map(drop)));
            }
        }
        check_whole(&tree, &db, &run, case);
        outcomes.push((case, pending, before == after, woken));
    }
    for (case, pending, unchanged, woken) in outcomes {
        assert!(
            pending,
            "{case}: the step came back ready, not on one pending wait"
        );
        assert!(
            unchanged,
            "{case}: a page upgraded before the held one went back changed"
        );
        assert!(
            woken,
            "{case}: the holder's release left the wait unfinished"
        );
    }
}
