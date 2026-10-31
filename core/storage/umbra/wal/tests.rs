use super::*;
use crate::io::OpenFlags;
use crate::storage::umbra::buffer::{Anchor, BufferManager, ExclusiveGuard, DEFAULT_COOLING_SHARE};
use crate::storage::umbra::page::{SizeClass, MIN_PAGE_SIZE};
use crate::storage::umbra::testing::{backends, corrupt, drive, Backend};

const PAGE: usize = MIN_PAGE_SIZE;

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

fn range(slot: u64, offset: u32, bytes: &[u8]) -> Record {
    Record::Range {
        pid: PageId::new(class(0), slot),
        offset,
        bytes: bytes.to_vec(),
    }
}

fn scan(log: &[u8]) -> (Vec<(Lsn, Vec<Record>)>, u64) {
    let mut frames = Vec::new();
    let mut at = 0u64;
    while let Ok(Some((records, len))) = parse_frame(at, &log[at as usize..]) {
        at += len as u64;
        frames.push((at, records));
    }
    (frames, at)
}

#[test]
fn frames_read_back_and_a_torn_tail_ends_the_log() {
    let pid = PageId::new(class(0), 2);
    let a = vec![
        Record::New { pid },
        range(2, 24, b"alpha"),
        Record::Remap {
            pid,
            slot: 9,
            lsn: 77,
        },
    ];
    let b = vec![range(1, 100, b"beta")];
    let mut log = encode_frame(0, &a);
    let second_at = log.len() as u64;
    log.extend(encode_frame(second_at, &b));
    let (frames, end) = scan(&log);
    assert_eq!(frames, vec![(second_at, a), (log.len() as u64, b)]);
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
    let frame = encode_frame(0, &[range(1, 24, b"x")]);
    let mut log = encode_frame(0, &[range(1, 24, b"y")]);
    log.extend(&frame);
    assert_eq!(
        scan(&log).0.len(),
        1,
        "a stale frame read as the log's next one"
    );
}

fn open_log(b: &Backend) -> Arc<dyn File> {
    b.io.open_file(&b.path("log"), OpenFlags::Create, false)
        .unwrap()
}

fn log_bytes(b: &Backend) -> Vec<u8> {
    let file = open_log(b);
    read_at(&b.io, &file, 0, file.size().unwrap() as usize).unwrap()
}

#[test]
fn concurrent_commits_share_fsyncs_and_all_land_in_order() {
    for b in backends() {
        let wal = Wal::open(&b.io, open_log(&b), 0).unwrap();
        let threads: Vec<_> = (0..8u64)
            .map(|t| {
                let (wal, io) = (wal.clone(), b.io.clone());
                std::thread::spawn(move || {
                    for i in 0..200u64 {
                        let lsn = wal.append(&[range(t, 24, &i.to_le_bytes())]).unwrap();
                        drive(&io, || wal.wait_durable(lsn)).unwrap();
                        assert!(wal.durable() >= lsn);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let (frames, end) = scan(&log_bytes(&b));
        assert_eq!(frames.len(), 1600, "{}", b.name);
        assert_eq!(end, wal.durable(), "{}", b.name);
        // The memory backend's fsync is free and completes inside the call, so nothing waits long
        // enough to share one; the backends with real fsyncs must share them.
        if b.name == "syscall" || b.name == "io_uring" {
            assert!(
                wal.syncs() < 1600,
                "{}: every commit paid its own fsync",
                b.name
            );
        }
        let mut last = HashMap::new();
        for (_, records) in &frames {
            let Record::Range { pid, bytes, .. } = &records[0] else {
                unreachable!()
            };
            let v = u64::from_le_bytes(bytes[..].try_into().unwrap());
            let prev = last.insert(pid.number(), v);
            assert_eq!(prev.map_or(0, |p| p + 1), v, "{}", b.name);
        }
    }
}

#[test]
fn the_next_write_goes_out_while_the_fsync_before_it_runs() {
    for b in backends()
        .into_iter()
        .filter(|b| b.name == "memory-deferred")
    {
        let wal = Wal::open(&b.io, open_log(&b), 0).unwrap();
        let first = wal.append(&[range(0, 24, b"a")]).unwrap();
        let c1 = wal.durable_completion(first).expect("not durable yet");
        assert!(wal.tail.lock().writing);
        // Appended while the first write is in flight: it waits for that write, not its fsync.
        let second = wal.append(&[range(0, 24, b"b")]).unwrap();
        let c2 = wal.durable_completion(second).expect("not durable yet");
        assert_eq!(wal.tail.lock().pending.len() as u64, second - first);
        // The first write completes: its fsync starts, and so does the second write beside it.
        while wal.tail.lock().written < first {
            b.io.step().unwrap();
        }
        {
            let tail = wal.tail.lock();
            assert_eq!(tail.syncing, Some(first), "the first fsync did not start");
            assert!(
                tail.writing || tail.written == second,
                "the second write waited for the fsync"
            );
        }
        assert_eq!(wal.overlapped(), 1);
        b.io.wait_for_completion(c1).unwrap();
        assert!(wal.durable() >= first);
        b.io.wait_for_completion(c2).unwrap();
        // The first fsync could not cover the second write, which completed after it began.
        assert_eq!(wal.syncs(), 2);
        assert_eq!(wal.durable(), second);
        let tail = wal.tail.lock();
        assert!(!tail.writing && tail.syncing.is_none());
    }
}

#[test]
fn threads_committing_end_durable_to_every_frame_with_writes_beside_fsyncs() {
    // Threads committing on a real fsync: the log ends durable to every frame appended, and on
    // io_uring, where completions arrive while others are submitted, writes went out beside an
    // fsync in flight.
    for b in backends()
        .into_iter()
        .filter(|b| b.name == "syscall" || b.name == "io_uring")
    {
        let wal = Wal::open(&b.io, open_log(&b), 0).unwrap();
        let threads: Vec<_> = (0..8u64)
            .map(|t| {
                let (wal, io) = (wal.clone(), b.io.clone());
                std::thread::spawn(move || {
                    for i in 0..300u64 {
                        let lsn = wal.append(&[range(t, 24, &i.to_le_bytes())]).unwrap();
                        drive(&io, || wal.wait_durable(lsn)).unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let end = wal.tail.lock().end;
        assert_eq!(wal.durable(), end, "{}", b.name);
        assert_eq!(scan(&log_bytes(&b)).0.len(), 2400, "{}", b.name);
        if b.name == "io_uring" {
            assert!(
                wal.overlapped() > 0,
                "{}: no write ever went out beside an fsync",
                b.name
            );
        }
    }
}

/// A tree of leaves under one root, changed through the log the way a transaction layer will.
struct World<'a> {
    b: &'a Backend,
}

const LEAVES: usize = 24;
/// Offset of a leaf's counter.
const VALUE: usize = HEADER_SIZE + 64;

fn slot(i: usize) -> usize {
    HEADER_SIZE + 8 * i
}

impl World<'_> {
    fn open(&self, budget: usize) -> (Arc<Wal>, BufferManager, Recovered) {
        let io = &self.b.io;
        let store = Arc::new(PageStore::open(io, &self.b.path("pages"), false).unwrap());
        let (wal, recovered) = Wal::open_recovered(io, open_log(self.b), &store, budget).unwrap();
        let bm = BufferManager::new(
            io.clone(),
            store,
            Some(wal.clone()),
            budget,
            DEFAULT_COOLING_SHARE,
        )
        .unwrap();
        (wal, bm, recovered)
    }
}

/// Logs each page's change (a new page's creation, then the ranges written), stamps the pages
/// with the frame's LSN, lets them go and waits for durability.
/// A page a commit changed: held exclusively, whether it is new, and the byte ranges written.
type Changed<'a> = (ExclusiveGuard<'a>, bool, Vec<(usize, usize)>);

fn commit(io: &Arc<dyn IO>, wal: &Arc<Wal>, pages: Vec<Changed<'_>>) {
    let mut records = Vec::new();
    for (page, created, ranges) in &pages {
        if *created {
            records.push(Record::New { pid: page.pid() });
        }
        for &(start, end) in ranges {
            records.push(Record::Range {
                pid: page.pid(),
                offset: start as u32,
                bytes: page.durable_bytes(start..end),
            });
        }
    }
    let lsn = wal.append(&records).unwrap();
    for (mut page, _, _) in pages {
        page::set_lsn(&mut page, lsn);
    }
    drive(io, || wal.wait_durable(lsn)).unwrap();
}

/// Makes everything appended durable, as the next commit would.
fn settle(io: &Arc<dyn IO>, wal: &Arc<Wal>) {
    let end = wal.tail.lock().end;
    drive(io, || wal.wait_durable(end)).unwrap();
}

fn create(io: &Arc<dyn IO>, wal: &Arc<Wal>, bm: &BufferManager, anchor: &Anchor) -> PageId {
    let root = drive(io, || bm.allocate(anchor.owner(), anchor.swip(), class(0))).unwrap();
    let root_pid = root.pid();
    let mut pages = Vec::new();
    for i in 0..LEAVES {
        let mut leaf = drive(io, || {
            bm.allocate(root.owner(), root.swip(slot(i)), class(0))
        })
        .unwrap();
        leaf[VALUE..VALUE + 8].copy_from_slice(&0u64.to_le_bytes());
        leaf[VALUE + 8..VALUE + 1008].fill(i as u8);
        pages.push((leaf, true, vec![(VALUE, VALUE + 1008)]));
    }
    // The root's swips, which the log carries as page ids.
    pages.insert(0, (root, true, vec![(slot(0), slot(LEAVES))]));
    commit(io, wal, pages);
    root_pid
}

fn bump(io: &Arc<dyn IO>, wal: &Arc<Wal>, bm: &BufferManager, anchor: &Anchor, i: usize, by: u64) {
    let root = drive(io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
    let mut leaf = drive(io, || bm.fix_exclusive(root.owner(), root.swip(slot(i)))).unwrap();
    let v = u64::from_le_bytes(leaf[VALUE..VALUE + 8].try_into().unwrap());
    leaf[VALUE..VALUE + 8].copy_from_slice(&(v + by).to_le_bytes());
    commit(io, wal, vec![(leaf, false, vec![(VALUE, VALUE + 8)])]);
}

fn values(io: &Arc<dyn IO>, bm: &BufferManager, anchor: &Anchor, name: &str) -> Vec<u64> {
    let root = drive(io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
    (0..LEAVES)
        .map(|i| {
            let leaf = drive(io, || bm.fix_shared(root.owner(), root.swip(slot(i)))).unwrap();
            assert!(
                leaf[VALUE + 8..VALUE + 1008].iter().all(|&b| b == i as u8),
                "{name}: leaf {i} torn"
            );
            u64::from_le_bytes(leaf[VALUE..VALUE + 8].try_into().unwrap())
        })
        .collect()
}

#[test]
fn committed_work_survives_a_crash_with_pages_going_to_disk_throughout() {
    for b in backends() {
        let io = &b.io;
        let world = World { b: &b };
        let root_pid = {
            let (wal, bm, _) = world.open(64 * PAGE);
            create(io, &wal, &bm, &Anchor::new())
        };
        let mut expected = vec![0u64; LEAVES];
        {
            // A pool of a third of the tree: pages go to disk and come back throughout.
            let (wal, bm, recovered) = world.open(8 * PAGE);
            assert_eq!(recovered.pages, LEAVES as u64 + 1, "{}", b.name);
            let anchor = Anchor::on_disk(root_pid);
            for round in 0..10u64 {
                for (i, value) in expected.iter_mut().enumerate() {
                    let by = round * 100 + i as u64 + 1;
                    bump(io, &wal, &bm, &anchor, i, by);
                    *value += by;
                }
            }
            assert_eq!(values(io, &bm, &anchor, b.name), expected, "{}", b.name);
            assert!(
                bm.stats().evicted > 0,
                "{}: nothing reached disk before the crash",
                b.name
            );
            // Dropped without writing back what is still in memory: the crash.
        }
        let (wal, bm, _) = world.open(8 * PAGE);
        let anchor = Anchor::on_disk(root_pid);
        assert_eq!(values(io, &bm, &anchor, b.name), expected, "{}", b.name);
        bump(io, &wal, &bm, &anchor, 3, 7);
        expected[3] += 7;
        assert_eq!(values(io, &bm, &anchor, b.name), expected, "{}", b.name);
    }
}

#[test]
fn recovery_holding_fewer_pages_than_the_log_names_still_recovers_every_one() {
    for b in backends() {
        let io = &b.io;
        let world = World { b: &b };
        let root_pid = {
            let (wal, bm, _) = world.open(64 * PAGE);
            let anchor = Anchor::new();
            let root_pid = create(io, &wal, &bm, &anchor);
            for i in 0..LEAVES {
                bump(io, &wal, &bm, &anchor, i, i as u64 + 1);
            }
            root_pid
        };
        // Recovery may hold four pages at a time; the log names twenty-five.
        let (_, bm, recovered) = world.open(4 * PAGE);
        assert_eq!(recovered.pages, LEAVES as u64 + 1, "{}", b.name);
        let expected: Vec<u64> = (1..=LEAVES as u64).collect();
        assert_eq!(
            values(io, &bm, &Anchor::on_disk(root_pid), b.name),
            expected,
            "{}",
            b.name
        );
    }
}

#[test]
fn a_torn_newest_copy_falls_back_to_the_copy_before_it() {
    for b in backends() {
        let io = &b.io;
        let world = World { b: &b };
        let (root_pid, leaf5) = {
            let (wal, bm, _) = world.open(64 * PAGE);
            let anchor = Anchor::new();
            let root_pid = create(io, &wal, &bm, &anchor);
            bump(io, &wal, &bm, &anchor, 5, 11);
            drive(io, || bm.evict_everything()).unwrap();
            settle(io, &wal);
            // Leaf 5 changes again and goes out again, to a fresh slot.
            bump(io, &wal, &bm, &anchor, 5, 100);
            drive(io, || bm.evict_everything()).unwrap();
            settle(io, &wal);
            let root = drive(io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
            let word = root.swip(slot(5)).load(Ordering::Relaxed);
            assert_eq!(word & 1, 1, "{}", b.name);
            let leaf5 = PageId::from_raw(word >> 1);
            // Its newest copy was torn: the second half never reached disk.
            corrupt(&b, bm.store(), leaf5);
            (root_pid, leaf5)
        };
        let (_, bm, recovered) = world.open(64 * PAGE);
        assert!(
            recovered.stale_copies >= 1,
            "{}: the torn copy was not passed over: {recovered:?}",
            b.name
        );
        let mut expected = vec![0u64; LEAVES];
        expected[5] = 111;
        assert_eq!(
            values(io, &bm, &Anchor::on_disk(root_pid), b.name),
            expected,
            "{}",
            b.name
        );
        let _ = leaf5;
    }
}

#[test]
fn a_commit_torn_in_the_log_is_not_recovered_and_is_cut() {
    for b in backends() {
        let io = &b.io;
        let world = World { b: &b };
        let root_pid = {
            let (wal, bm, _) = world.open(64 * PAGE);
            let anchor = Anchor::new();
            let root_pid = create(io, &wal, &bm, &anchor);
            bump(io, &wal, &bm, &anchor, 0, 1);
            bump(io, &wal, &bm, &anchor, 1, 2);
            root_pid
        };
        let log = open_log(&b);
        let len = log.size().unwrap();
        b.io.wait_for_completion(
            log.truncate(len - 3, Completion::new_trunc(|_| {}))
                .unwrap(),
        )
        .unwrap();
        let mut expected = vec![0u64; LEAVES];
        expected[0] = 1;
        {
            let (wal, bm, _) = world.open(64 * PAGE);
            let anchor = Anchor::on_disk(root_pid);
            assert_eq!(values(io, &bm, &anchor, b.name), expected, "{}", b.name);
            bump(io, &wal, &bm, &anchor, 2, 3);
        }
        // The torn bytes were cut, so the commit after them reads after a restart.
        let (_, bm, _) = world.open(64 * PAGE);
        expected[2] = 3;
        assert_eq!(
            values(io, &bm, &Anchor::on_disk(root_pid), b.name),
            expected,
            "{}",
            b.name
        );
    }
}

#[test]
fn recovery_run_twice_rewrites_nothing_the_second_time() {
    for b in backends() {
        let io = &b.io;
        let world = World { b: &b };
        {
            let (wal, bm, _) = world.open(64 * PAGE);
            let anchor = Anchor::new();
            create(io, &wal, &bm, &anchor);
            for i in 0..LEAVES {
                bump(io, &wal, &bm, &anchor, i, 1);
            }
        }
        let first = world.open(64 * PAGE).2;
        assert_eq!(first.rewritten, LEAVES as u64 + 1, "{}: {first:?}", b.name);
        // The first recovery logged where it wrote every page: the second starts each from that
        // copy, which already holds every change.
        let second = world.open(64 * PAGE).2;
        assert_eq!(second.rewritten, 0, "{}: {second:?}", b.name);
        assert_eq!(second.stale_copies, 0, "{}: {second:?}", b.name);
        assert!(second.skipped > 0, "{}", b.name);
    }
}

#[test]
fn a_copy_whose_remap_a_crash_lost_is_never_read() {
    for b in backends() {
        let io = &b.io;
        let world = World { b: &b };
        let root_pid = {
            let (wal, bm, _) = world.open(64 * PAGE);
            let anchor = Anchor::new();
            let root_pid = create(io, &wal, &bm, &anchor);
            // A change appended but never made durable, written back out of place; its remap is
            // appended behind it and lost with it.
            let root = drive(io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
            let mut leaf =
                drive(io, || bm.fix_exclusive(root.owner(), root.swip(slot(0)))).unwrap();
            leaf[VALUE..VALUE + 8].copy_from_slice(&999u64.to_le_bytes());
            let lsn = wal
                .append(&[Record::Range {
                    pid: leaf.pid(),
                    offset: VALUE as u32,
                    bytes: 999u64.to_le_bytes().to_vec(),
                }])
                .unwrap();
            page::set_lsn(&mut leaf, lsn);
            drop(leaf);
            drop(root);
            drive(io, || bm.evict_everything()).unwrap();
            assert!(wal.durable() < lsn, "{}: the change became durable", b.name);
            assert!(
                bm.stats().writes > 0,
                "{}: nothing was written back",
                b.name
            );
            root_pid
        };
        let (_, bm, _) = world.open(64 * PAGE);
        assert_eq!(
            values(io, &bm, &Anchor::on_disk(root_pid), b.name),
            vec![0u64; LEAVES],
            "{}",
            b.name
        );
    }
}

#[test]
fn a_log_longer_than_one_read_with_frames_across_reads_recovers_whole() {
    for b in backends() {
        let io = &b.io;
        let world = World { b: &b };
        let pids: Vec<PageId> = {
            let (wal, bm, _) = world.open(256 * PAGE);
            let anchor = Anchor::new();
            let root = drive(io, || bm.allocate(anchor.owner(), anchor.swip(), class(0))).unwrap();
            let mut pids = vec![root.pid()];
            commit(io, &wal, vec![(root, true, vec![(slot(0), slot(160))])]);
            let root = drive(io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
            // 160 frames of nearly a page each: 10 MiB of log, more than one read of 8 MiB.
            for i in 0..160 {
                let mut leaf = drive(io, || {
                    bm.allocate(root.owner(), root.swip(slot(i)), class(0))
                })
                .unwrap();
                leaf[VALUE..VALUE + 8].copy_from_slice(&(i as u64).to_le_bytes());
                leaf[VALUE + 8..].fill(i as u8);
                pids.push(leaf.pid());
                commit(io, &wal, vec![(leaf, true, vec![(VALUE, PAGE)])]);
            }
            pids
        };
        let (_, bm, recovered) = world.open(256 * PAGE);
        assert_eq!(recovered.frames, 161, "{}", b.name);
        assert_eq!(recovered.pages, 161, "{}", b.name);
        let store = bm.store();
        for (i, pid) in pids.iter().enumerate().skip(1) {
            let got = Arc::new(Mutex::new(None));
            let out = got.clone();
            let c = store
                .read(*pid, move |r| {
                    if let PageRead::Valid(buf) = r {
                        *out.lock() = Some(u64::from_le_bytes(
                            buf.as_slice()[VALUE..VALUE + 8].try_into().unwrap(),
                        ));
                    }
                })
                .unwrap();
            b.io.wait_for_completion(c).unwrap();
            assert_eq!(*got.lock(), Some(i as u64 - 1), "{}: page {i}", b.name);
        }
    }
}
