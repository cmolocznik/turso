use super::*;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

const PAGE: usize = crate::page::MIN_PAGE_SIZE;

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

fn manager(budget: usize) -> (tempfile::TempDir, BufferManager) {
    let dir = tempfile::tempdir().unwrap();
    let store = PageStore::open(dir.path(), false).unwrap();
    (dir, BufferManager::new(store, budget).unwrap())
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

fn check(page: &[u8], n: u32) {
    assert_eq!(u32::from_le_bytes(page[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap()), n);
    assert!(page[HEADER_SIZE + 4..].iter().all(|&b| b == n as u8), "leaf {n} came back wrong");
}

/// Anchor → root → `fanout` inner pages → `fanout` leaves each, leaf classes from `leaf_class`.
fn build(bm: &BufferManager, fanout: usize, leaf_class: impl Fn(u32) -> SizeClass) -> Box<Anchor> {
    let anchor = Anchor::new();
    drop(bm.allocate(anchor.owner(), anchor.swip(), class(0)).unwrap());
    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
    for i in 0..fanout {
        drop(bm.allocate(root.owner(), root.swip(slot(i)), class(0)).unwrap());
        let inner = bm.fix_shared(root.owner(), root.swip(slot(i))).unwrap();
        for j in 0..fanout {
            let n = (i * fanout + j) as u32;
            let mut leaf = bm.allocate(inner.owner(), inner.swip(slot(j)), leaf_class(n)).unwrap();
            fill(&mut leaf, n);
        }
    }
    drop(root);
    anchor
}

fn check_all(bm: &BufferManager, anchor: &Anchor, fanout: usize) {
    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
    for i in 0..fanout {
        let inner = bm.fix_shared(root.owner(), root.swip(slot(i))).unwrap();
        for j in 0..fanout {
            let leaf = bm.fix_shared(inner.owner(), inner.swip(slot(j))).unwrap();
            check(&leaf, (i * fanout + j) as u32);
        }
    }
}

#[test]
fn a_tree_four_times_the_budget_reads_back_whole() {
    let (_dir, bm) = manager(20 * PAGE);
    let anchor = build(&bm, 8, |_| class(0));
    assert!(bm.used() <= 20 * PAGE, "{} bytes held over a budget of {}", bm.used(), 20 * PAGE);
    let after_build = bm.stats();
    assert!(after_build.evicted >= 73 - 20, "{after_build:?}");
    assert!(after_build.writes >= after_build.evicted - 1, "dirty pages evicted unwritten");
    check_all(&bm, &anchor, 8);
    let after_read = bm.stats();
    assert!(after_read.reads > 0, "{after_read:?}");
    assert!(bm.used() <= 20 * PAGE);
    // Twice over, the second pass from a pool the first one churned.
    check_all(&bm, &anchor, 8);
}

#[test]
fn pages_of_mixed_sizes_share_one_budget() {
    let budget = 40 * PAGE;
    let (_dir, bm) = manager(budget);
    // Leaves alternate 64 KiB and 256 KiB: 32 × 64 + 32 × 256 KiB = 10 MiB, four times the pool.
    let anchor = build(&bm, 8, |n| class(if n % 2 == 0 { 0 } else { 2 }));
    assert!(bm.used() <= budget);
    check_all(&bm, &anchor, 8);
    assert!(bm.used() <= budget);
    assert!(bm.stats().evicted > 0);
}

#[test]
fn a_cooling_page_comes_back_without_io() {
    let (_dir, bm) = manager(64 * PAGE);
    let anchor = build(&bm, 2, |_| class(0));
    let before = bm.stats();
    assert_eq!(before.evicted, 0);
    bm.cool_everything();
    let cooled = bm.stats().cooled;
    // The four leaves have no children; the inner pages and the root keep theirs swizzled.
    assert_eq!(cooled, 4, "{:?}", bm.stats());
    check_all(&bm, &anchor, 2);
    let after = bm.stats();
    assert_eq!(after.cool_hits, 4);
    assert_eq!(after.reads, before.reads, "a cooling page was read from disk");
}

#[test]
fn a_parent_is_cooled_only_after_its_children() {
    let (_dir, bm) = manager(64 * PAGE);
    let anchor = build(&bm, 2, |_| class(0));
    bm.cool_everything();
    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
    for i in 0..2 {
        let word = root.swip(slot(i)).load(Ordering::Relaxed);
        assert_eq!(word & 1, 0, "inner page {i} cooled while its leaves were in memory");
    }
    drop(root);
    // With the leaves cooled, a second sweep cools the inner pages.
    bm.cool_everything();
    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
    for i in 0..2 {
        assert_eq!(root.swip(slot(i)).load(Ordering::Relaxed) & 1, 1);
    }
}

#[test]
fn a_page_reaches_disk_with_its_swips_unswizzled() {
    let (_dir, bm) = manager(4 * PAGE);
    let anchor = Anchor::new();
    drop(bm.allocate(anchor.owner(), anchor.swip(), class(0)).unwrap());
    {
        let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
        let mut leaf = bm.allocate(root.owner(), root.swip(slot(0)), class(0)).unwrap();
        fill(&mut leaf, 9);
    }
    // Push the root out: the leaf goes first, then the root, with a page id in its swip.
    for _ in 0..3 {
        bm.cool_everything();
        let mut cooling = bm.cooling.lock();
        while bm.evict_one(&mut cooling).unwrap() {}
    }
    assert_eq!(anchor.swip().load(Ordering::Relaxed) & 1, 1, "the root stayed in memory");
    let root_pid = PageId::from_raw(anchor.swip().load(Ordering::Relaxed) >> 1);
    let mut page = vec![0u8; PAGE];
    bm.store.read(root_pid, &mut page).unwrap();
    let on_disk = u64::from_le_bytes(page[slot(0)..slot(0) + 8].try_into().unwrap());
    assert_eq!(on_disk & 1, 1, "a frame address reached the disk");
    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
    check(&bm.fix_shared(root.owner(), root.swip(slot(0))).unwrap(), 9);
}

#[test]
fn an_allocation_waits_for_a_latched_page_to_be_let_go() {
    let (_dir, bm) = manager(2 * PAGE);
    let bm = Arc::new(bm);
    let anchor: Arc<Anchor> = Arc::from(Anchor::new());
    drop(bm.allocate(anchor.owner(), anchor.swip(), class(0)).unwrap());
    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
    drop(bm.allocate(root.owner(), root.swip(slot(0)), class(0)).unwrap());
    // The root has a child in memory and the leaf is held: nothing can be evicted.
    let held = bm.fix_shared(root.owner(), root.swip(slot(0))).unwrap();
    let (done, finished) = std::sync::mpsc::channel();
    let third = {
        let (bm, anchor) = (bm.clone(), anchor.clone());
        std::thread::spawn(move || {
            let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
            drop(bm.allocate(root.owner(), root.swip(slot(1)), class(0)).unwrap());
            done.send(()).unwrap();
        })
    };
    for _ in 0..1000 {
        std::thread::yield_now();
    }
    assert!(finished.try_recv().is_err(), "a page was allocated past a full budget");
    drop(held);
    third.join().unwrap();
    finished.recv().unwrap();
    assert!(bm.used() <= 2 * PAGE);
    assert_eq!(bm.stats().evicted, 1);
}

#[test]
fn a_page_larger_than_the_budget_is_refused() {
    let (_dir, bm) = manager(2 * PAGE);
    let anchor = Anchor::new();
    assert!(matches!(
        bm.allocate(anchor.owner(), anchor.swip(), class(2)),
        Err(Error::TooLarge(c)) if c == class(2)
    ));
}

#[test]
fn an_optimistic_read_never_returns_a_page_mid_write() {
    let (_dir, bm) = manager(64 * PAGE);
    let anchor = Anchor::new();
    {
        let mut root = bm.allocate(anchor.owner(), anchor.swip(), class(0)).unwrap();
        fill(&mut root, 0);
    }
    let bm = Arc::new(bm);
    let anchor: Arc<Anchor> = Arc::from(anchor);
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (bm, anchor, stop) = (bm.clone(), anchor.clone(), stop.clone());
        std::thread::spawn(move || {
            for n in 1..2000u32 {
                let mut page = bm.fix_exclusive(anchor.owner(), anchor.swip()).unwrap();
                fill(&mut page, n);
            }
            stop.store(true, Ordering::Relaxed);
        })
    };
    let mut reads = 0;
    while !stop.load(Ordering::Relaxed) {
        let (first, last) = bm
            .read_optimistic(anchor.owner(), anchor.swip(), |p| (p[HEADER_SIZE + 4], p[PAGE - 1]))
            .unwrap();
        assert_eq!(first, last, "an optimistic read validated over a half-written page");
        reads += 1;
    }
    writer.join().unwrap();
    assert!(reads > 0);
}

#[test]
fn readers_and_writers_under_eviction_lose_nothing() {
    let fanout = 8;
    let (_dir, bm) = manager(24 * PAGE);
    let anchor: Arc<Anchor> = Arc::from(build(&bm, fanout, |_| class(0)));
    let bm = Arc::new(bm);
    // Writers bump a counter in their own leaves; readers check every leaf they pass is whole.
    let counter = HEADER_SIZE + 8;
    let threads: Vec<_> = (0..6)
        .map(|t| {
            let (bm, anchor) = (bm.clone(), anchor.clone());
            std::thread::spawn(move || {
                let mut seed = 0x9e37_79b9_7f4a_7c15u64 ^ t;
                for _ in 0..3000 {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let (i, j) = ((seed % fanout as u64) as usize, ((seed >> 8) % fanout as u64) as usize);
                    let n = (i * fanout + j) as u32;
                    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
                    let inner = bm.fix_shared(root.owner(), root.swip(slot(i))).unwrap();
                    if t < 2 && j % 2 == t as usize {
                        let mut leaf = bm.fix_exclusive(inner.owner(), inner.swip(slot(j))).unwrap();
                        let c = u32::from_le_bytes(leaf[counter..counter + 4].try_into().unwrap());
                        leaf[counter..counter + 4].copy_from_slice(&(c + 1).to_le_bytes());
                    } else {
                        let leaf = bm.fix_shared(inner.owner(), inner.swip(slot(j))).unwrap();
                        assert_eq!(
                            u32::from_le_bytes(leaf[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap()),
                            n
                        );
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert!(bm.stats().evicted > 0, "nothing was evicted: the test exercised no IO");
    assert!(bm.used() <= 24 * PAGE);
    // Every increment survived the trips to disk: the counters' total is what the writers did.
    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
    let mut total = 0u64;
    for i in 0..fanout {
        let inner = bm.fix_shared(root.owner(), root.swip(slot(i))).unwrap();
        for j in 0..fanout {
            let leaf = bm.fix_shared(inner.owner(), inner.swip(slot(j))).unwrap();
            let v = u32::from_le_bytes(leaf[counter..counter + 4].try_into().unwrap());
            // The counter overwrote the fill pattern's first bytes only.
            assert!(leaf[counter + 8..].iter().all(|&b| b == (i * fanout + j) as u8));
            total += v as u64 - u32::from_le_bytes([(i * fanout + j) as u8; 4]) as u64;
        }
    }
    let writes_expected = recount_writes(fanout);
    assert_eq!(total, writes_expected);
}

/// The number of writer iterations the threads above made, replaying their generators.
fn recount_writes(fanout: usize) -> u64 {
    let mut count = 0;
    for t in 0..2u64 {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64 ^ t;
        for _ in 0..3000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let j = ((seed >> 8) % fanout as u64) as usize;
            if j % 2 == t as usize {
                count += 1;
            }
        }
    }
    count
}
