use super::*;
use crate::io::crash::CrashIO;
use crate::io::MemoryIO;
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET};
use crate::storage::atoll::db::{Atoll, AtollConfig, Opening};
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::page::CLASS_COUNT;
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
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
            checkpoint_shards: 8,
        },
        log_budget: Some(512 << 10),
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
    let mut check = Check::default();
    let stats = run_to(run, || tree.check(&mut check));
    let mut expected: BTreeSet<PageId> = stats.pages.iter().copied().collect();
    for &v in &stats.values {
        let pages = run_to(run, || value::pages(db.pool(), v));
        expected.extend(pages.unwrap_or_else(|| panic!("{label}: value {v:?} lost its pages")));
    }
    let mut allocated = BTreeSet::new();
    for c in 0..CLASS_COUNT {
        let (mut number, mut free_run) = (0, 0);
        while free_run < 512 {
            let pid = PageId::new(class(c), number);
            if run_to(run, || db.store().entry(pid)) != FREE {
                allocated.insert(pid);
                free_run = 0;
            } else {
                free_run += 1;
            }
            number += 1;
        }
    }
    let leaked: Vec<_> = allocated.difference(&expected).collect();
    let lost: Vec<_> = expected.difference(&allocated).collect();
    assert!(
        leaked.is_empty() && lost.is_empty(),
        "{label}: pages allocated and unreachable {leaked:?}; reachable and free {lost:?}"
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

#[test]
fn a_tree_holds_what_a_sorted_map_holds_through_puts_removes_merges_and_eviction() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, MIN_BUDGET, 1);
    let run = drive_runner(&io);
    let root = run_to(&run, || Tree::create(&io, db.pool(), db.store(), class(0)));
    let tree = tree_of(&io, &db, root);
    let max = tree.limits().max_key;
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
        run(&mut || db.checkpointer().step());
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
    run(&mut || db.checkpointer().step());
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
    run(&mut || db.checkpointer().step());
    model.clear();
    let stats = check_whole(&tree, &db, &run, "empty");
    assert_eq!(stats.nodes, 1, "an empty tree is its root");
}

#[test]
fn values_longer_than_a_chunk_are_written_read_and_freed_whole() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 64 << 20, 1);
    let run = drive_runner(&io);
    let root = run_to(&run, || Tree::create(&io, db.pool(), db.store(), class(0)));
    let tree = tree_of(&io, &db, root);
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
    run(&mut || db.checkpointer().step());
    check_whole(&tree, &db, &run, "replaced");
    for i in 0..3 {
        let mut op = Remove::new(format!("big{i}").as_bytes());
        assert!(run_to(&run, || tree.remove(&mut op)));
    }
    run(&mut || db.checkpointer().step());
    let stats = check_whole(&tree, &db, &run, "removed");
    assert_eq!((stats.nodes, stats.entries), (1, 0));
}

#[test]
fn concurrent_writers_and_scans_keep_every_key_whole_and_in_order() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open(&io, 64 << 20, 1);
    let run = drive_runner(&io);
    let root = run_to(&run, || Tree::create(&io, db.pool(), db.store(), class(0)));
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
                            run(&mut || db.checkpointer().step());
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
    run(&mut || db.checkpointer().step());
    check_whole(&tree, &db, &run, "after the writers");
}

/// Every key's history: the LSN of each change and what it left.
type History = BTreeMap<Vec<u8>, Vec<(u64, Option<Vec<u8>>)>>;

fn state_at(history: &History, end: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    history
        .iter()
        .filter_map(|(k, changes)| {
            let last = changes.iter().rev().find(|(lsn, _)| *lsn <= end)?;
            Some((k.clone(), last.1.clone()?))
        })
        .collect()
}

fn crashes_recover_every_durable_change_and_a_whole_tree(durable_writes: bool) {
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        crash_io.set_durable_writes(durable_writes);
        let io: Arc<dyn IO> = crash_io.clone();
        let db = open(&io, MIN_BUDGET, 1);
        let history: RefCell<History> = RefCell::new(BTreeMap::new());
        let acknowledged = Cell::new(0u64);
        let steps = Cell::new(0u64);
        let checks = Cell::new(0u64);
        let root = Cell::new(None);
        let crash_and_check = |crash_seed: u64| {
            let Some(root) = root.get() else {
                return;
            };
            let threads = 1 + (crash_seed % 3) as usize;
            let crashed: Arc<dyn IO> = Arc::new(crash_io.crash(crash_seed));
            let label = format!("seed {seed}/{crash_seed}, {threads} threads");
            let recovered = open(&crashed, MIN_BUDGET, threads);
            let end = recovered.recovered_end().expect("a recovery");
            assert!(
                end >= acknowledged.get(),
                "{label}: recovery ended at {end}, before {}, which was durable",
                acknowledged.get()
            );
            let expected = state_at(&history.borrow(), end);
            let verify = |db: &Atoll, label: &str| {
                let run = drive_runner(&crashed);
                let tree = tree_of(&crashed, db, root);
                let entries: BTreeMap<_, _> = scan(&tree, &run, true).into_iter().collect();
                assert!(
                    entries == expected,
                    "{label}: the tree differs from the log up to {end}"
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
                &open(&crashed, MIN_BUDGET, 1),
                &format!("{label}, reopened"),
            );
            checks.set(checks.get() + 1);
        };
        let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| loop {
            match op().unwrap() {
                IOResult::Done(()) => return,
                IOResult::IO(IOCompletions(c)) => {
                    while !c.finished() {
                        io.step().unwrap();
                        steps.set(steps.get() + 1);
                        if steps.get() % 5 == 0 {
                            crash_and_check(seed * 1_000_000 + steps.get());
                        }
                    }
                }
            }
        };
        let created = run_to(&run, || Tree::create(&io, db.pool(), db.store(), class(0)));
        run(&mut || db.log().wait_durable(db.log().end()));
        acknowledged.set(db.log().end());
        root.set(Some(created));
        let tree = tree_of(&io, &db, created);
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut version = 0u64;
        for op in 0..1400u32 {
            let key = format!("k{:04}", rng.random_range(0..600)).into_bytes();
            version += 1;
            let change = if rng.random_range(0..3) == 0 {
                let mut remove = Remove::new(&key);
                run_to(&run, || tree.remove(&mut remove));
                (remove.lsn(), None)
            } else {
                let value = random_value(&mut rng, &key, version);
                let mut put = Put::new(&key, value.clone(), Mode::Upsert);
                run_to(&run, || tree.put(&mut put));
                (put.lsn(), Some(value))
            };
            if change.0 > 0 {
                history.borrow_mut().entry(key).or_default().push(change);
            }
            run(&mut || db.checkpointer().step());
            if op % 16 == 15 {
                let end = db.log().end();
                run(&mut || db.log().wait_durable(end));
                acknowledged.set(end);
            }
        }
        assert!(
            checks.get() >= 100,
            "seed {seed}: only {} crashes",
            checks.get()
        );
        crash_and_check(seed);
    }
}

#[test]
fn crashes_recover_every_durable_change_and_a_whole_tree_with_durable_writes() {
    crashes_recover_every_durable_change_and_a_whole_tree(true);
}

#[test]
fn crashes_recover_every_durable_change_and_a_whole_tree_with_writes_then_syncs() {
    crashes_recover_every_durable_change_and_a_whole_tree(false);
}

/// Ends the log at `at`, as a crash that kept everything before it and nothing after would.
fn cut_log(io: &Arc<dyn IO>, segment_bytes: u64, at: u64) {
    use crate::io::{Buffer, Completion, OpenFlags};
    let mut n = at / segment_bytes;
    loop {
        let path = format!("{BASE}.log.{n:016x}");
        let Ok(file) = io.open_file(&path, OpenFlags::None, false) else {
            break;
        };
        let from = if n == at / segment_bytes {
            at % segment_bytes
        } else {
            0
        };
        let zeros = Arc::new(Buffer::new(vec![0u8; (segment_bytes - from) as usize]));
        let c = file
            .pwrite(from, zeros, Completion::new_write(|_| {}))
            .unwrap();
        io.wait_for_completion(c).unwrap();
        n += 1;
    }
}

#[test]
fn a_log_ending_at_any_record_of_a_structure_change_recovers_a_whole_tree() {
    let crash_io = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash_io.clone();
    // A pool that holds everything: no page is written back, so recovery rebuilds every node
    // from the log alone, up to wherever it ends.
    let db = open(&io, 64 << 20, 1);
    let run = drive_runner(&io);
    let root = run_to(&run, || Tree::create(&io, db.pool(), db.store(), class(0)));
    let tree = tree_of(&io, &db, root);
    let ticket = db.checkpointer().request_full();
    run(&mut || db.checkpointer().step_full(ticket));
    let start = db.log().end();
    let mut history: History = BTreeMap::new();
    // Splits, the root growing twice, then merges and the root shrinking.
    let keys: Vec<Vec<u8>> = (0..240u32)
        .map(|i| format!("{:06}", i * 7919 % 1000).into_bytes())
        .collect();
    for (i, key) in keys.iter().enumerate() {
        let mut op = Put::new(key, stamp(key, i as u64, 400), Mode::Upsert);
        run_to(&run, || tree.put(&mut op));
        history
            .entry(key.clone())
            .or_default()
            .push((op.lsn(), Some(stamp(key, i as u64, 400))));
    }
    let grown = check_whole(&tree, &db, &run, "grown");
    for key in keys.iter().take(230) {
        let mut op = Remove::new(key);
        if run_to(&run, || tree.remove(&mut op)) {
            history.get_mut(key).unwrap().push((op.lsn(), None));
        }
    }
    let shrunk = check_whole(&tree, &db, &run, "shrunk");
    assert!(
        grown.nodes > 20 && shrunk.nodes < 4,
        "{} nodes, then {}",
        grown.nodes,
        shrunk.nodes
    );
    run(&mut || db.log().wait_durable(db.log().end()));
    while crash_io.in_flight() > 0 {
        io.step().unwrap();
    }
    let mut reader = Log::reader(io.clone(), BASE, &config(0).log, start);
    let mut boundaries = vec![start];
    let mut grouped = 0;
    while let Some(r) = reader.next_record().unwrap() {
        boundaries.push(r.lsn);
        grouped += r.place.grouped() as usize;
    }
    assert!(grouped > 100, "only {grouped} records in groups");
    for (i, &at) in boundaries.iter().enumerate() {
        let cut: Arc<dyn IO> = Arc::new(crash_io.crash(i as u64));
        cut_log(&cut, config(0).log.segment_bytes, at);
        let db = open(&cut, 64 << 20, 1);
        let end = db.recovered_end().unwrap();
        assert!(end <= at, "recovery read past the cut at {at}, to {end}");
        let run = drive_runner(&cut);
        let tree = tree_of(&cut, &db, root);
        let expected = state_at(&history, end);
        let entries: BTreeMap<_, _> = scan(&tree, &run, true).into_iter().collect();
        assert!(
            entries == expected,
            "the log cut at {at} (recovered to {end})"
        );
        check_whole(&tree, &db, &run, &format!("the log cut at {at}"));
    }
}
