use super::*;
use crate::storage::atoll::buffer::{Anchor, DEFAULT_COOLING_SHARE};
use crate::storage::atoll::testing::{
    backends, bump, create, drive, settle, slot, values, World, LEAVES,
};

fn open_at(world: &World<'_>, budget: usize) -> Opened {
    open(&world.b.io, &world.base(), budget, DEFAULT_COOLING_SHARE).unwrap()
}

#[test]
fn a_superblock_reads_back_as_written() {
    let sb = Superblock {
        seq: 7,
        redo: 12345,
        map_next_slot: 9,
        classes: (0..CLASS_COUNT as u64)
            .map(|c| (c * 10, c * 20, (0..c).map(|d| 100 + d).collect()))
            .collect(),
    };
    let buf = sb.encode();
    assert_eq!(Superblock::decode(buf.as_slice()), Some(sb));
    assert_eq!(Superblock::decode(&vec![0u8; PAGE]), None);
}

#[test]
fn a_checkpoint_cuts_the_log_and_recovery_starts_at_its_redo_point() {
    for b in backends() {
        let io = &b.io;
        let world = World { b: &b };
        let mut expected = vec![0u64; LEAVES];
        let (root_pid, redo) = {
            let db = open_at(&world, 64 * PAGE);
            let anchor = Anchor::new();
            let root_pid = create(io, &db.wal, &db.buffer, &anchor);
            for (i, v) in expected.iter_mut().enumerate() {
                bump(io, &db.wal, &db.buffer, &anchor, i, 1);
                *v += 1;
            }
            drive(io, || db.buffer.evict_everything()).unwrap();
            settle(io, &db.wal);
            let done = drive(io, || db.checkpointer.checkpoint(&db.buffer, &db.wal)).unwrap();
            assert!(done.redo > 0, "{}: {done:?}", b.name);
            // Every page was clean, so nothing before the log's end is needed.
            assert_eq!(done.redo, db.wal.end(), "{}", b.name);
            for (i, v) in expected.iter_mut().enumerate() {
                bump(io, &db.wal, &db.buffer, &anchor, i, 10);
                *v += 10;
            }
            (root_pid, done.redo)
            // A crash: the bumps after the checkpoint are only in the log.
        };
        let db = open_at(&world, 64 * PAGE);
        assert_eq!(
            db.recovered.frames, LEAVES as u64,
            "{}: recovery read before the redo point: {:?}",
            b.name, db.recovered
        );
        assert_eq!(
            values(io, &db.buffer, &Anchor::on_disk(root_pid), b.name),
            expected,
            "{}",
            b.name
        );
        if b.name.starts_with("memory") {
            let cut = redo - redo % 4096;
            assert!(
                world.log().has_hole(0, cut as usize).unwrap(),
                "{}: the log before the redo point was kept",
                b.name
            );
        }
    }
}

#[test]
fn a_checkpoint_erases_and_reuses_the_slots_it_superseded() {
    for b in backends() {
        let io = &b.io;
        let world = World { b: &b };
        let db = open_at(&world, 64 * PAGE);
        let anchor = Anchor::new();
        create(io, &db.wal, &db.buffer, &anchor);
        drive(io, || db.buffer.evict_everything()).unwrap();
        settle(io, &db.wal);
        let root = drive(io, || db.buffer.fix_shared(anchor.owner(), anchor.swip())).unwrap();
        let leaf0 = PageId::from_raw(root.swip(slot(0)).load(Ordering::Relaxed) >> 1);
        drop(root);
        let first = db.store.location(leaf0).expect("written");
        for i in 0..LEAVES {
            bump(io, &db.wal, &db.buffer, &anchor, i, 1);
        }
        drive(io, || db.buffer.evict_everything()).unwrap();
        settle(io, &db.wal);
        assert_ne!(db.store.location(leaf0), Some(first), "{}", b.name);
        let retired = db.store.retired();
        assert!(retired >= LEAVES, "{}: {retired} retired", b.name);
        let free_before = db.store.free_slots();
        drive(io, || db.checkpointer.checkpoint(&db.buffer, &db.wal)).unwrap();
        assert_eq!(db.store.retired(), 0, "{}", b.name);
        assert!(db.store.free_slots() >= free_before + retired, "{}", b.name);
        // The superseded copy's bytes are gone from the file.
        let file = io
            .open_file(
                &format!("{}.pages.c0", world.base()),
                OpenFlags::Create,
                false,
            )
            .unwrap();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let out = bytes.clone();
        let c = file
            .pread(
                first * PAGE as u64,
                Completion::new_read(Arc::new(Buffer::new_temporary(PAGE)), move |r| {
                    if let Ok((buf, n)) = r {
                        *out.lock() = buf.as_slice()[..n.max(0) as usize].to_vec();
                    }
                    None
                }),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
        assert!(
            bytes.lock().iter().all(|&b| b == 0),
            "{}: a recycled slot kept its old bytes",
            b.name
        );
    }
}

#[test]
fn changes_made_while_a_checkpoint_runs_survive_a_crash() {
    for b in backends()
        .into_iter()
        .filter(|b| b.name == "memory-deferred" || b.name == "io_uring")
    {
        let io = &b.io;
        let world = World { b: &b };
        let mut expected = vec![0u64; LEAVES];
        let root_pid = {
            let db = open_at(&world, 16 * PAGE);
            let anchor = Anchor::new();
            let root_pid = create(io, &db.wal, &db.buffer, &anchor);
            settle(io, &db.wal);
            // Each time the checkpoint waits on its IO, a writer commits a change.
            let mut i = 0;
            let mut interleaved = 0;
            loop {
                match db.checkpointer.checkpoint(&db.buffer, &db.wal).unwrap() {
                    IOResult::Done(_) => break,
                    IOResult::IO(_) => {
                        bump(io, &db.wal, &db.buffer, &anchor, i % LEAVES, 1);
                        expected[i % LEAVES] += 1;
                        i += 1;
                        interleaved += 1;
                    }
                }
            }
            assert!(interleaved > 0, "{}: the checkpoint never waited", b.name);
            for (i, v) in expected.iter_mut().enumerate() {
                bump(io, &db.wal, &db.buffer, &anchor, i, 100);
                *v += 100;
            }
            root_pid
        };
        let db = open_at(&world, 16 * PAGE);
        assert_eq!(
            values(io, &db.buffer, &Anchor::on_disk(root_pid), b.name),
            expected,
            "{}",
            b.name
        );
    }
}

#[test]
fn a_superblock_torn_mid_write_leaves_the_checkpoint_before_it_in_charge() {
    for b in backends()
        .into_iter()
        .filter(|b| b.name == "memory-deferred")
    {
        let io = &b.io;
        let world = World { b: &b };
        let mut expected = vec![0u64; LEAVES];
        let (root_pid, torn_slot) = {
            let db = open_at(&world, 64 * PAGE);
            let anchor = Anchor::new();
            let root_pid = create(io, &db.wal, &db.buffer, &anchor);
            drive(io, || db.checkpointer.checkpoint(&db.buffer, &db.wal)).unwrap();
            for (i, v) in expected.iter_mut().enumerate() {
                bump(io, &db.wal, &db.buffer, &anchor, i, 5);
                *v += 5;
            }
            drive(io, || db.buffer.evict_everything()).unwrap();
            settle(io, &db.wal);
            // The next checkpoint stops once its superblock is written and before it is synced:
            // the crash comes there, and tears it.
            loop {
                match db.checkpointer.checkpoint(&db.buffer, &db.wal).unwrap() {
                    IOResult::Done(_) => panic!("the checkpoint finished before the crash"),
                    IOResult::IO(_) => {
                        let at_sync = matches!(
                            db.checkpointer.run.lock().as_ref().map(|r| &r.phase),
                            Some(Phase::SyncSuperblock(_))
                        );
                        if at_sync {
                            break;
                        }
                        io.step().unwrap();
                    }
                }
            }
            let seq = db.checkpointer.run.lock().as_ref().expect("running").seq;
            (root_pid, seq % SUPERBLOCKS)
        };
        let map = io
            .open_file(&format!("{}.map", world.base()), OpenFlags::Create, false)
            .unwrap();
        let junk = Arc::new(Buffer::new(vec![0xa5; PAGE / 2]));
        let c = map
            .pwrite(
                torn_slot * PAGE as u64 + (PAGE / 2) as u64,
                junk,
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
        let db = open_at(&world, 64 * PAGE);
        assert_eq!(
            values(io, &db.buffer, &Anchor::on_disk(root_pid), b.name),
            expected,
            "{}",
            b.name
        );
    }
}
