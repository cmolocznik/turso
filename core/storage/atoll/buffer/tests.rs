use super::*;
use crate::storage::atoll::testing::{backends, corrupt, drive, Backend};
use crate::types::IOResult;

const PAGE: usize = page::MIN_PAGE_SIZE;

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

fn manager(b: &Backend, budget: usize) -> BufferManager {
    let store = PageStore::open(&b.io, &b.path("pages"), false).unwrap();
    BufferManager::new(
        b.io.clone(),
        Arc::new(store),
        None,
        budget,
        DEFAULT_COOLING_SHARE,
    )
    .unwrap()
}

/// Offset of the `i`th swip in a page used as an inner node.
fn slot(i: usize) -> usize {
    HEADER_SIZE + 8 * i
}

/// Every byte of a leaf's payload carries the leaf's number.
fn fill(page: &mut [u8], n: u32) {
    page[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&n.to_le_bytes());
    page[HEADER_SIZE + 4..].fill(n as u8);
}

fn check(page: &[u8], n: u32, backend: &str) {
    assert_eq!(
        u32::from_le_bytes(page[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap()),
        n,
        "{backend}"
    );
    assert!(
        page[HEADER_SIZE + 4..].iter().all(|&b| b == n as u8),
        "{backend}: leaf {n} came back wrong"
    );
}

/// Anchor → root → `fanout` inner pages → `fanout` leaves each, leaf classes from `leaf_class`.
fn build(
    b: &Backend,
    bm: &BufferManager,
    fanout: usize,
    leaf_class: impl Fn(u32) -> SizeClass,
) -> Box<Anchor> {
    let io = &b.io;
    let anchor = Anchor::new();
    drop(drive(io, || bm.allocate(anchor.owner(), anchor.swip(), class(0))).unwrap());
    let root = drive(io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
    for i in 0..fanout {
        drop(
            drive(io, || {
                bm.allocate(root.owner(), root.swip(slot(i)), class(0))
            })
            .unwrap(),
        );
        let inner = drive(io, || bm.fix_shared(root.owner(), root.swip(slot(i)))).unwrap();
        for j in 0..fanout {
            let n = (i * fanout + j) as u32;
            let mut leaf = drive(io, || {
                bm.allocate(inner.owner(), inner.swip(slot(j)), leaf_class(n))
            })
            .unwrap();
            fill(&mut leaf, n);
        }
    }
    drop(root);
    anchor
}

fn check_all(b: &Backend, bm: &BufferManager, anchor: &Anchor, fanout: usize) {
    let io = &b.io;
    let root = drive(io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
    for i in 0..fanout {
        let inner = drive(io, || bm.fix_shared(root.owner(), root.swip(slot(i)))).unwrap();
        for j in 0..fanout {
            let leaf = drive(io, || bm.fix_shared(inner.owner(), inner.swip(slot(j)))).unwrap();
            check(&leaf, (i * fanout + j) as u32, b.name);
        }
    }
}

#[test]
fn a_tree_four_times_the_budget_reads_back_whole() {
    for b in backends() {
        let bm = manager(&b, 20 * PAGE);
        let anchor = build(&b, &bm, 8, |_| class(0));
        assert!(
            bm.used() <= 20 * PAGE,
            "{}: {} bytes over a budget of {}",
            b.name,
            bm.used(),
            20 * PAGE
        );
        let built = bm.stats();
        assert!(built.evicted >= 73 - 20, "{}: {built:?}", b.name);
        check_all(&b, &bm, &anchor, 8);
        assert!(bm.stats().reads > 0, "{}: {:?}", b.name, bm.stats());
        // Twice over, the second pass from a pool the first one churned.
        check_all(&b, &bm, &anchor, 8);
        assert!(bm.used() <= 20 * PAGE, "{}", b.name);
    }
}

#[test]
fn pages_of_mixed_sizes_share_one_budget() {
    for b in backends() {
        let budget = 40 * PAGE;
        let bm = manager(&b, budget);
        // Leaves alternate 64 KiB and 256 KiB: 32 × 64 + 32 × 256 KiB = 10 MiB, four times the pool.
        let anchor = build(&b, &bm, 8, |n| class(if n % 2 == 0 { 0 } else { 2 }));
        assert!(bm.used() <= budget, "{}", b.name);
        check_all(&b, &bm, &anchor, 8);
        assert!(bm.used() <= budget, "{}", b.name);
        assert!(bm.stats().evicted > 0, "{}", b.name);
    }
}

fn cool_everything(bm: &BufferManager) -> Vec<Writeback> {
    let mut writebacks = Vec::new();
    let mut cooling = bm.cooling.lock();
    bm.cool(&mut cooling, usize::MAX, &mut writebacks);
    writebacks
}

#[test]
fn a_cooling_page_comes_back_without_io() {
    for b in backends() {
        let bm = manager(&b, 64 * PAGE);
        let anchor = build(&b, &bm, 2, |_| class(0));
        let before = bm.stats();
        // Not submitted: the pages stay dirty and in memory.
        let writebacks = cool_everything(&bm);
        // The four leaves have no children; the inner pages and the root keep theirs.
        assert_eq!(bm.stats().cooled, 4, "{}: {:?}", b.name, bm.stats());
        for w in &writebacks {
            w.frame.get().writing.lock().take();
        }
        check_all(&b, &bm, &anchor, 2);
        let after = bm.stats();
        assert_eq!(after.cool_hits, 4, "{}", b.name);
        assert_eq!(
            after.reads, before.reads,
            "{}: a cooling page was read from disk",
            b.name
        );
    }
}

#[test]
fn a_parent_is_cooled_only_after_its_children() {
    for b in backends() {
        let bm = manager(&b, 64 * PAGE);
        let anchor = build(&b, &bm, 2, |_| class(0));
        drop(cool_everything(&bm));
        {
            let root = drive(&b.io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
            for i in 0..2 {
                let word = root.swip(slot(i)).load(Ordering::Relaxed);
                assert_eq!(
                    word & 1,
                    0,
                    "{}: inner page {i} cooled while its leaves were in memory",
                    b.name
                );
            }
        }
        // With the leaves cooled, a second sweep cools the inner pages.
        drop(cool_everything(&bm));
        let root = drive(&b.io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
        for i in 0..2 {
            assert_eq!(
                root.swip(slot(i)).load(Ordering::Relaxed) & 1,
                1,
                "{}",
                b.name
            );
        }
    }
}

fn read_from_disk(b: &Backend, bm: &BufferManager, pid: PageId) -> Option<Vec<u8>> {
    let got = Arc::new(Mutex::new(None));
    let out = got.clone();
    let c = bm
        .store
        .read(pid, move |read| {
            *out.lock() = Some(match read {
                PageRead::Valid(buf) => Some(buf.as_slice().to_vec()),
                _ => None,
            })
        })
        .unwrap();
    b.io.wait_for_completion(c).unwrap();
    let page = got.lock().take().expect("the read finished");
    page
}

#[test]
fn a_page_reaches_disk_with_its_swips_unswizzled() {
    for b in backends() {
        let bm = manager(&b, 4 * PAGE);
        let anchor = Anchor::new();
        drop(
            drive(&b.io, || {
                bm.allocate(anchor.owner(), anchor.swip(), class(0))
            })
            .unwrap(),
        );
        {
            let root = drive(&b.io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
            let mut leaf = drive(&b.io, || {
                bm.allocate(root.owner(), root.swip(slot(0)), class(0))
            })
            .unwrap();
            fill(&mut leaf, 9);
        }
        // The leaf goes first, then the root, with a page id in its swip.
        drive(&b.io, || bm.evict_everything()).unwrap();
        let word = anchor.swip().load(Ordering::Relaxed);
        assert_eq!(word & 1, 1, "{}: the root stayed in memory", b.name);
        let page = read_from_disk(&b, &bm, PageId::from_raw(word >> 1)).expect("the root on disk");
        let on_disk = u64::from_le_bytes(page[slot(0)..slot(0) + 8].try_into().unwrap());
        assert_eq!(
            on_disk & 1,
            1,
            "{}: a frame address reached the disk",
            b.name
        );
        let root = drive(&b.io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
        check(
            &drive(&b.io, || bm.fix_shared(root.owner(), root.swip(slot(0)))).unwrap(),
            9,
            b.name,
        );
    }
}

#[test]
fn an_allocation_with_nothing_to_evict_waits_for_a_latch_to_be_let_go() {
    for b in backends() {
        let bm = manager(&b, 2 * PAGE);
        let anchor = Anchor::new();
        drop(
            drive(&b.io, || {
                bm.allocate(anchor.owner(), anchor.swip(), class(0))
            })
            .unwrap(),
        );
        let root = drive(&b.io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
        drop(
            drive(&b.io, || {
                bm.allocate(root.owner(), root.swip(slot(0)), class(0))
            })
            .unwrap(),
        );
        // The root has a child in memory and the leaf is held: nothing can be evicted.
        let held = drive(&b.io, || bm.fix_shared(root.owner(), root.swip(slot(0)))).unwrap();
        let wait = match bm
            .allocate(root.owner(), root.swip(slot(1)), class(0))
            .unwrap()
        {
            IOResult::IO(io) => io.0,
            IOResult::Done(_) => panic!("{}: a page was allocated past a full budget", b.name),
        };
        for _ in 0..3 {
            b.io.step().unwrap();
        }
        assert!(
            !wait.finished(),
            "{}: the wait ended with the leaf still held",
            b.name
        );
        drop(held);
        assert!(
            wait.finished(),
            "{}: letting the leaf go did not end the wait",
            b.name
        );
        drop(
            drive(&b.io, || {
                bm.allocate(root.owner(), root.swip(slot(1)), class(0))
            })
            .unwrap(),
        );
        assert!(bm.used() <= 2 * PAGE);
        assert_eq!(bm.stats().evicted, 1, "{}", b.name);
    }
}

#[test]
fn a_page_being_loaded_is_read_once_for_everyone_waiting() {
    for b in backends()
        .into_iter()
        .filter(|b| b.name != "memory" && b.name != "syscall")
    {
        let bm = manager(&b, 8 * PAGE);
        let anchor = build(&b, &bm, 2, |_| class(0));
        drive(&b.io, || bm.evict_everything()).unwrap();
        let reads = bm.stats().reads;
        let first = match bm.fix_shared(anchor.owner(), anchor.swip()).unwrap() {
            IOResult::IO(io) => io.0,
            IOResult::Done(_) => panic!("{}: the evicted root was in memory", b.name),
        };
        let second = match bm.fix_shared(anchor.owner(), anchor.swip()).unwrap() {
            IOResult::IO(io) => io.0,
            IOResult::Done(_) => panic!("{}: the root landed before its read completed", b.name),
        };
        assert!(
            first.ptr_eq(&second),
            "{}: the second caller did not wait on the first load",
            b.name
        );
        assert_eq!(bm.stats().reads, reads + 1, "{}", b.name);
        b.io.wait_for_completion(first).unwrap();
        drop(drive(&b.io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap());
        assert_eq!(
            bm.stats().reads,
            reads + 1,
            "{}: the root was read twice",
            b.name
        );
    }
}

#[test]
fn a_torn_page_fails_to_load_and_leaves_nothing_behind() {
    for b in backends() {
        let bm = manager(&b, 8 * PAGE);
        let anchor = Anchor::new();
        {
            let mut root = drive(&b.io, || {
                bm.allocate(anchor.owner(), anchor.swip(), class(0))
            })
            .unwrap();
            fill(&mut root, 3);
        }
        drive(&b.io, || bm.evict_everything()).unwrap();
        assert_eq!(bm.used(), 0, "{}", b.name);
        let pid = PageId::from_raw(anchor.swip().load(Ordering::Relaxed) >> 1);
        corrupt(&b, &b.path("pages"), &bm.store, pid);
        for attempt in 0..2 {
            let err = drive(&b.io, || bm.fix_shared(anchor.owner(), anchor.swip())).err();
            assert!(
                err.is_some(),
                "{}: a torn page loaded on attempt {attempt}",
                b.name
            );
            assert_eq!(bm.used(), 0, "{}: the failed load kept its frame", b.name);
            assert_eq!(
                anchor.swip().load(Ordering::Relaxed) & 1,
                1,
                "{}: the swip still points at a frame",
                b.name
            );
        }
    }
}

#[test]
fn a_page_changed_while_its_write_back_is_in_flight_stays_dirty() {
    for b in backends()
        .into_iter()
        .filter(|b| b.name == "memory-deferred")
    {
        let bm = manager(&b, 8 * PAGE);
        let anchor = Anchor::new();
        {
            let mut root = drive(&b.io, || {
                bm.allocate(anchor.owner(), anchor.swip(), class(0))
            })
            .unwrap();
            fill(&mut root, 1);
        }
        // Cooling submits the write-back; the deferred backend holds its completion.
        let in_flight = match bm.evict_everything().unwrap() {
            IOResult::IO(io) => io.0,
            IOResult::Done(()) => panic!("the write-back completed inside its submission"),
        };
        {
            let mut root =
                drive(&b.io, || bm.fix_exclusive(anchor.owner(), anchor.swip())).unwrap();
            fill(&mut root, 2);
        }
        b.io.wait_for_completion(in_flight).unwrap();
        drive(&b.io, || bm.evict_everything()).unwrap();
        let pid = PageId::from_raw(anchor.swip().load(Ordering::Relaxed) >> 1);
        check(&read_from_disk(&b, &bm, pid).expect("written"), 2, b.name);
    }
}

#[test]
fn every_write_back_goes_to_a_fresh_slot_and_the_old_copy_is_kept() {
    for b in backends() {
        let bm = manager(&b, 8 * PAGE);
        let anchor = Anchor::new();
        {
            let mut root = drive(&b.io, || {
                bm.allocate(anchor.owner(), anchor.swip(), class(0))
            })
            .unwrap();
            fill(&mut root, 1);
        }
        drive(&b.io, || bm.evict_everything()).unwrap();
        let pid = PageId::from_raw(anchor.swip().load(Ordering::Relaxed) >> 1);
        let first = bm.store.location(pid).expect("written");
        {
            let mut root =
                drive(&b.io, || bm.fix_exclusive(anchor.owner(), anchor.swip())).unwrap();
            fill(&mut root, 2);
        }
        drive(&b.io, || bm.evict_everything()).unwrap();
        let second = bm.store.location(pid).expect("written");
        assert_ne!(
            first, second,
            "{}: a write-back overwrote the copy before it",
            b.name
        );
        assert_eq!(
            bm.store.retired(),
            1,
            "{}: the superseded copy's slot was not kept",
            b.name
        );
        check(
            &read_from_disk(&b, &bm, pid).expect("current copy"),
            2,
            b.name,
        );
        let old = Arc::new(Mutex::new(None));
        let out = old.clone();
        let c = bm
            .store
            .read_slot(pid, first, move |r| {
                if let PageRead::Valid(buf) = r {
                    *out.lock() = Some(buf.as_slice().to_vec());
                }
            })
            .unwrap();
        b.io.wait_for_completion(c).unwrap();
        check(
            &old.lock().take().expect("the old copy is whole"),
            1,
            b.name,
        );
    }
}

#[test]
fn readers_and_writers_on_threads_under_eviction_lose_nothing() {
    for b in backends() {
        let fanout = 8;
        let bm = Arc::new(manager(&b, 24 * PAGE));
        let anchor: Arc<Anchor> = Arc::from(build(&b, &bm, fanout, |_| class(0)));
        let io = b.io.clone();
        // Writers bump a counter in their own leaves; readers check every leaf they pass.
        let counter = HEADER_SIZE + 8;
        let threads: Vec<_> = (0..6u64)
            .map(|t| {
                let (bm, anchor, io) = (bm.clone(), anchor.clone(), io.clone());
                std::thread::spawn(move || {
                    let mut seed = 0x9e37_79b9_7f4a_7c15u64 ^ t;
                    let mut bumps = 0u64;
                    for _ in 0..2000 {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        let i = (seed % fanout as u64) as usize;
                        let j = ((seed >> 8) % fanout as u64) as usize;
                        let root =
                            drive(&io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
                        let inner =
                            drive(&io, || bm.fix_shared(root.owner(), root.swip(slot(i)))).unwrap();
                        if t < 2 && j % 2 == t as usize {
                            let mut leaf =
                                drive(&io, || bm.fix_exclusive(inner.owner(), inner.swip(slot(j))))
                                    .unwrap();
                            let c =
                                u32::from_le_bytes(leaf[counter..counter + 4].try_into().unwrap());
                            leaf[counter..counter + 4].copy_from_slice(&(c + 1).to_le_bytes());
                            bumps += 1;
                        } else {
                            let leaf =
                                drive(&io, || bm.fix_shared(inner.owner(), inner.swip(slot(j))))
                                    .unwrap();
                            let n = u32::from_le_bytes(
                                leaf[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap(),
                            );
                            assert_eq!(n, (i * fanout + j) as u32);
                        }
                    }
                    bumps
                })
            })
            .collect();
        let bumps: u64 = threads.into_iter().map(|t| t.join().unwrap()).sum();
        assert!(
            bm.stats().evicted > 0,
            "{}: nothing was evicted, so no IO was exercised",
            b.name
        );
        assert!(bm.used() <= 24 * PAGE, "{}", b.name);
        // Every increment survived the trips to disk.
        let root = drive(&b.io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
        let mut total = 0u64;
        for i in 0..fanout {
            let inner = drive(&b.io, || bm.fix_shared(root.owner(), root.swip(slot(i)))).unwrap();
            for j in 0..fanout {
                let leaf =
                    drive(&b.io, || bm.fix_shared(inner.owner(), inner.swip(slot(j)))).unwrap();
                let n = (i * fanout + j) as u8;
                let v = u32::from_le_bytes(leaf[counter..counter + 4].try_into().unwrap());
                total += (v - u32::from_le_bytes([n; 4])) as u64;
            }
        }
        assert_eq!(total, bumps, "{}", b.name);
    }
}
