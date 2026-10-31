use super::*;
use crate::buffer::{Anchor, BufferManager};
use crate::page::{SizeClass, MIN_PAGE_SIZE};
use crate::txn::Txn;
use std::collections::HashMap;
use std::sync::Arc;

const PAGE: usize = MIN_PAGE_SIZE;

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

fn pid(slot: u64) -> PageId {
    PageId::new(class(0), slot)
}

fn range(slot: u64, offset: u32, bytes: &[u8]) -> Record {
    Record::Range { pid: pid(slot), offset, bytes: bytes.to_vec() }
}

#[test]
fn frames_read_back_and_a_torn_tail_ends_the_log() {
    let a = vec![range(1, 16, b"alpha"), Record::Image { pid: pid(2), bytes: vec![3; PAGE] }];
    let b = vec![range(1, 100, b"beta")];
    let mut log = encode_frame(0, &a);
    let second_at = log.len() as u64;
    log.extend(encode_frame(second_at, &b));
    let (frames, end) = scan(&log);
    assert_eq!(frames, vec![(second_at, a.clone()), (log.len() as u64, b)]);
    assert_eq!(end, log.len() as u64);
    for cut in [1, 4, 8, log.len() - second_at as usize - 1] {
        let (frames, end) = scan(&log[..log.len() - cut]);
        assert_eq!(frames.len(), 1, "a frame missing {cut} bytes was read");
        assert_eq!(end, second_at);
    }
    let mut flipped = log.clone();
    *flipped.last_mut().unwrap() ^= 1;
    assert_eq!(scan(&flipped).0.len(), 1);
}

#[test]
fn a_frame_copied_to_another_offset_does_not_read() {
    let frame = encode_frame(0, &[range(1, 16, b"x")]);
    let mut log = encode_frame(0, &[range(1, 16, b"y")]);
    log.extend(&frame);
    assert_eq!(scan(&log).0.len(), 1, "a stale frame read as the log's next one");
}

#[test]
fn concurrent_commits_share_fsyncs_and_all_land() {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(Wal::open(&dir.path().join("wal")).unwrap());
    let threads: Vec<_> = (0..8u64)
        .map(|t| {
            let wal = wal.clone();
            std::thread::spawn(move || {
                for i in 0..200u64 {
                    let lsn = wal.append(&[range(t, 16, &i.to_le_bytes())]).unwrap();
                    wal.wait_durable(lsn).unwrap();
                    assert!(wal.durable() >= lsn);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let log = std::fs::read(dir.path().join("wal")).unwrap();
    let (frames, end) = scan(&log);
    assert_eq!(frames.len(), 1600);
    assert_eq!(end, wal.durable());
    assert!(wal.syncs() < 1600, "every commit paid its own fsync: {}", wal.syncs());
    // Each thread's values landed in order.
    let mut last = HashMap::new();
    for (_, records) in &frames {
        let Record::Range { pid, bytes, .. } = &records[0] else { unreachable!() };
        let v = u64::from_le_bytes(bytes[..].try_into().unwrap());
        let prev = last.insert(pid.slot(), v);
        assert_eq!(prev.map_or(0, |p| p + 1), v);
    }
}

/// A tree of leaves under one root, written through transactions, with the root's page id.
struct World {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
}

impl World {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        Self { _dir: dir, path }
    }

    fn open(&self, budget: usize) -> (Arc<Wal>, BufferManager, Recovered) {
        let store = PageStore::open(&self.path, false).unwrap();
        let recovered = recover(&self.path.join("wal"), &store).unwrap();
        let wal = Arc::new(Wal::open(&self.path.join("wal")).unwrap());
        let bm = BufferManager::with_wal(store, budget, wal.clone()).unwrap();
        (wal, bm, recovered)
    }
}

fn slot(i: usize) -> usize {
    HEADER_SIZE + 8 * i
}

const LEAVES: usize = 24;
/// Offset of a leaf's counter.
const VALUE: usize = HEADER_SIZE + 64;

/// Creates the root and its leaves in one transaction; returns the root's page id.
fn create(wal: &Wal, bm: &BufferManager, anchor: &Anchor) -> PageId {
    let mut txn = Txn::new(wal);
    let root = bm.allocate(anchor.owner(), anchor.swip(), class(0)).unwrap();
    let root_pid = root.pid();
    let root = txn.adopt(root, true);
    for i in 0..LEAVES {
        let leaf = {
            let r = txn.guard(root);
            bm.allocate(r.owner(), r.swip(slot(i)), class(0)).unwrap()
        };
        txn.link(root, slot(i));
        let leaf = txn.adopt(leaf, true);
        txn.write(leaf, VALUE, &0u64.to_le_bytes());
        // A fill that makes a torn or stale page visible.
        txn.write(leaf, VALUE + 8, &vec![i as u8; 1000]);
    }
    txn.commit().unwrap();
    root_pid
}

/// Adds `by` to leaf `i`'s counter in one transaction, or rolls it back.
fn bump(wal: &Wal, bm: &BufferManager, anchor: &Anchor, i: usize, by: u64, commit: bool) {
    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
    let leaf = bm.fix_exclusive(root.owner(), root.swip(slot(i))).unwrap();
    let mut txn = Txn::new(wal);
    let leaf = txn.adopt(leaf, false);
    let v = u64::from_le_bytes(txn.page(leaf)[VALUE..VALUE + 8].try_into().unwrap());
    txn.write(leaf, VALUE, &(v + by).to_le_bytes());
    if commit {
        txn.commit().unwrap();
    } else {
        txn.abort();
    }
}

fn values(bm: &BufferManager, anchor: &Anchor) -> Vec<u64> {
    let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
    (0..LEAVES)
        .map(|i| {
            let leaf = bm.fix_shared(root.owner(), root.swip(slot(i))).unwrap();
            assert!(leaf[VALUE + 8..VALUE + 1008].iter().all(|&b| b == i as u8), "leaf {i} torn");
            u64::from_le_bytes(leaf[VALUE..VALUE + 8].try_into().unwrap())
        })
        .collect()
}

#[test]
fn committed_work_survives_a_crash_and_aborted_work_does_not() {
    let world = World::new();
    let mut expected = vec![0u64; LEAVES];
    // The tree is created by one transaction, which holds every page until it commits: it needs
    // a pool the tree fits in. A crash then leaves it in the log alone.
    let root_pid = {
        let (wal, bm, _) = world.open(64 * PAGE);
        create(&wal, &bm, &Anchor::new())
    };
    {
        // A pool of a third of the tree: pages go to disk and come back throughout.
        let (wal, bm, recovered) = world.open(8 * PAGE);
        assert_eq!(recovered.pages, LEAVES as u64 + 1);
        let anchor = Anchor::on_disk(root_pid);
        for round in 0..20u64 {
            for (i, value) in expected.iter_mut().enumerate() {
                let by = round * 100 + i as u64 + 1;
                let commit = (i + round as usize) % 5 != 0;
                bump(&wal, &bm, &anchor, i, by, commit);
                if commit {
                    *value += by;
                }
            }
        }
        assert_eq!(values(&bm, &anchor), expected);
        assert!(bm.stats().evicted > 0, "nothing reached disk before the crash");
        // Dropped here without writing back what is still in memory: the crash.
    }
    let (wal, bm, recovered) = world.open(8 * PAGE);
    assert_eq!(recovered.pages, LEAVES as u64 + 1);
    let anchor = Anchor::on_disk(root_pid);
    assert_eq!(values(&bm, &anchor), expected);
    // And it carries on from there.
    bump(&wal, &bm, &anchor, 3, 7, true);
    expected[3] += 7;
    assert_eq!(values(&bm, &anchor), expected);
}

#[test]
fn a_page_torn_on_disk_is_rebuilt_from_its_image() {
    let world = World::new();
    let root_pid = {
        let (wal, bm, _) = world.open(64 * PAGE);
        let anchor = Anchor::new();
        let root_pid = create(&wal, &bm, &anchor);
        bump(&wal, &bm, &anchor, 5, 11, true);
        bm.evict_everything().unwrap();
        root_pid
    };
    // Leaf 5's write-back was torn: its second half never reached disk.
    let leaf5 = {
        let store = PageStore::open(&world.path, false).unwrap();
        let mut root = vec![0u8; PAGE];
        store.read(root_pid, &mut root).unwrap();
        let word = u64::from_le_bytes(root[slot(5)..slot(5) + 8].try_into().unwrap());
        assert_eq!(word & 1, 1, "the root on disk holds a pointer");
        PageId::from_raw(word >> 1)
    };
    let file = OpenOptions::new().write(true).open(world.path.join("class-0.pages")).unwrap();
    file.write_all_at(&vec![0xa5u8; PAGE / 2], leaf5.offset() + (PAGE / 2) as u64).unwrap();
    let mut probe = vec![0u8; PAGE];
    assert!(PageStore::open(&world.path, false).unwrap().read(leaf5, &mut probe).is_err());
    let (_wal, bm, _) = world.open(64 * PAGE);
    let anchor = Anchor::on_disk(root_pid);
    let mut expected = vec![0u64; LEAVES];
    expected[5] = 11;
    assert_eq!(values(&bm, &anchor), expected);
}

#[test]
fn a_commit_torn_in_the_log_is_not_recovered() {
    let world = World::new();
    let root_pid = {
        let (wal, bm, _) = world.open(64 * PAGE);
        let anchor = Anchor::new();
        let root_pid = create(&wal, &bm, &anchor);
        bump(&wal, &bm, &anchor, 0, 1, true);
        bump(&wal, &bm, &anchor, 1, 2, true);
        root_pid
    };
    let log = world.path.join("wal");
    let len = std::fs::metadata(&log).unwrap().len();
    OpenOptions::new().write(true).open(&log).unwrap().set_len(len - 3).unwrap();
    let (wal, bm, _) = world.open(64 * PAGE);
    let anchor = Anchor::on_disk(root_pid);
    let mut expected = vec![0u64; LEAVES];
    expected[0] = 1;
    assert_eq!(values(&bm, &anchor), expected);
    // The torn bytes were cut: the next commit is read after a restart.
    bump(&wal, &bm, &anchor, 2, 3, true);
    drop((bm, wal));
    let (_wal, bm, _) = world.open(64 * PAGE);
    expected[2] = 3;
    assert_eq!(values(&bm, &Anchor::on_disk(root_pid)), expected);
}

#[test]
fn recovery_run_twice_applies_nothing_the_second_time() {
    let world = World::new();
    {
        let (wal, bm, _) = world.open(64 * PAGE);
        let anchor = Anchor::new();
        create(&wal, &bm, &anchor);
        for i in 0..LEAVES {
            bump(&wal, &bm, &anchor, i, 1, true);
        }
    }
    let first = world.open(64 * PAGE).2;
    assert_eq!(first.skipped, 0);
    let second = world.open(64 * PAGE).2;
    assert_eq!(second.frames, first.frames);
    assert_eq!(second.skipped, first.frames + LEAVES as u64, "{second:?}");
}

#[test]
fn a_page_changed_after_its_image_is_logged_as_ranges_merged() {
    let world = World::new();
    let (wal, bm, _) = world.open(64 * PAGE);
    let anchor = Anchor::new();
    create(&wal, &bm, &anchor);
    {
        let root = bm.fix_shared(anchor.owner(), anchor.swip()).unwrap();
        let leaf = bm.fix_exclusive(root.owner(), root.swip(slot(0))).unwrap();
        let mut txn = Txn::new(&wal);
        let leaf = txn.adopt(leaf, false);
        txn.write(leaf, 200, b"aaaa");
        txn.write(leaf, 202, b"bbbb");
        txn.write(leaf, 300, b"c");
        txn.write(leaf, 196, b"dd");
        txn.commit().unwrap();
    }
    let log = std::fs::read(world.path.join("wal")).unwrap();
    let (frames, _) = scan(&log);
    let (_, last) = frames.last().unwrap();
    let ranges: Vec<(u32, &[u8])> = last
        .iter()
        .map(|r| match r {
            Record::Range { offset, bytes, .. } => (*offset, &bytes[..]),
            Record::Image { .. } => panic!("a page logged whole after its image"),
        })
        .collect();
    // 200..204 and 202..206 overlap and merge; 196..198 stops short of 200 and stays apart.
    assert_eq!(ranges, vec![(196, &b"dd"[..]), (200, &b"aabbbb"[..]), (300, &b"c"[..])]);
}

#[test]
fn a_page_goes_to_disk_only_after_the_log_holds_its_changes() {
    let world = World::new();
    let (wal, bm, _) = world.open(64 * PAGE);
    let anchor = Anchor::new();
    let mut page = bm.allocate(anchor.owner(), anchor.swip(), class(0)).unwrap();
    // A change appended to the log but not yet synced, as a commit in flight leaves it.
    let lsn = wal.append(&[Record::Image { pid: page.pid(), bytes: vec![0; PAGE] }]).unwrap();
    crate::page::set_lsn(&mut page, lsn);
    drop(page);
    assert!(wal.durable() < lsn);
    bm.evict_everything().unwrap();
    assert!(bm.stats().evicted > 0);
    assert!(wal.durable() >= lsn, "a page reached disk ahead of its log record");
}
