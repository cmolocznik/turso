use super::*;

const PATH: &str = "/db/file";

fn open(io: &CrashIO, flags: OpenFlags) -> Arc<dyn File> {
    io.open_file(PATH, flags, false).unwrap()
}

fn write(io: &CrashIO, file: &Arc<dyn File>, pos: u64, bytes: &[u8]) {
    let c = file
        .pwrite(
            pos,
            Arc::new(Buffer::new(bytes.to_vec())),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
}

fn sync(io: &CrashIO, file: &Arc<dyn File>) -> Result<()> {
    let c = file
        .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
        .unwrap();
    io.wait_for_completion(c)
}

fn read(io: &CrashIO, file: &Arc<dyn File>, len: usize) -> Vec<u8> {
    let got = Arc::new(Mutex::new(Vec::new()));
    let out = got.clone();
    let c = file
        .pread(
            0,
            Completion::new_read(Arc::new(Buffer::new_temporary(len)), move |r| {
                let (buf, n) = r.unwrap();
                *out.lock() = buf.as_slice()[..n as usize].to_vec();
                None
            }),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
    let bytes = got.lock().clone();
    bytes
}

/// The file's contents after a crash chosen by `seed`, or `None` if it is gone.
fn after_crash(io: &CrashIO, seed: u64) -> Option<Vec<u8>> {
    let crashed = io.crash(seed);
    let file = crashed.open_file(PATH, OpenFlags::None, false).ok()?;
    let len = file.size().unwrap() as usize;
    Some(read(&crashed, &file, len))
}

/// A new file in a directory already synced, so its existence is durable.
fn durable_file(io: &CrashIO) -> Arc<dyn File> {
    let file = open(io, OpenFlags::Create);
    io.sync_directory("/db").unwrap();
    file
}

#[test]
fn a_synced_write_survives_every_crash() {
    let io = CrashIO::new(1);
    let file = durable_file(&io);
    write(&io, &file, 0, &[7; 2048]);
    sync(&io, &file).unwrap();
    for seed in 0..200 {
        assert_eq!(after_crash(&io, seed), Some(vec![7; 2048]), "seed {seed}");
    }
}

#[test]
fn an_unsynced_write_survives_some_crashes_whole_torn_or_not_at_all() {
    let io = CrashIO::new(2);
    let file = durable_file(&io);
    write(&io, &file, 0, &[1; 4 * SECTOR]);
    sync(&io, &file).unwrap();
    write(&io, &file, 0, &[2; 4 * SECTOR]);
    let (mut old, mut new, mut torn) = (0, 0, 0);
    for seed in 0..400 {
        let image = after_crash(&io, seed).unwrap();
        assert_eq!(image.len(), 4 * SECTOR, "seed {seed}");
        for sector in image.chunks(SECTOR) {
            assert!(
                sector.iter().all(|&b| b == sector[0]),
                "seed {seed}: a sector was torn inside"
            );
        }
        match (image.iter().all(|&b| b == 1), image.iter().all(|&b| b == 2)) {
            (true, _) => old += 1,
            (_, true) => new += 1,
            _ => torn += 1,
        }
    }
    assert!(
        old > 0 && new > 0 && torn > 0,
        "old {old}, new {new}, torn {torn}"
    );
}

#[test]
fn a_durable_write_survives_every_crash_and_supersedes_unsynced_writes_beneath_it() {
    let io = CrashIO::new(3);
    let file = durable_file(&io);
    write(&io, &file, 0, &[1; SECTOR]);
    let c = file
        .pwrite_durable(
            0,
            Arc::new(Buffer::new(vec![9; SECTOR])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
    for seed in 0..200 {
        assert_eq!(after_crash(&io, seed), Some(vec![9; SECTOR]), "seed {seed}");
    }
}

#[test]
fn a_failed_sync_loses_the_writes_it_covered() {
    let io = CrashIO::new(4);
    let file = durable_file(&io);
    write(&io, &file, 0, &[5; SECTOR]);
    io.set_faults(Faults {
        sync: 1.0,
        ..Faults::default()
    });
    assert!(sync(&io, &file).is_err());
    io.set_faults(Faults::default());
    assert_eq!(read(&io, &file, SECTOR), vec![5; SECTOR], "still visible");
    sync(&io, &file).unwrap();
    for seed in 0..200 {
        assert_eq!(after_crash(&io, seed), Some(Vec::new()), "seed {seed}");
    }
}

#[test]
fn a_sync_does_not_cover_a_write_that_completes_after_it_starts() {
    let mut write_first = 0;
    for seed in 0..64 {
        let io = CrashIO::new(seed);
        let file = durable_file(&io);
        let order = Arc::new(Mutex::new(Vec::new()));
        let wrote = order.clone();
        let w = file
            .pwrite(
                0,
                Arc::new(Buffer::new(vec![3; SECTOR])),
                Completion::new_write(move |_| wrote.lock().push("write")),
            )
            .unwrap();
        let synced = order.clone();
        let s = file
            .sync(
                Completion::new_sync(move |_| synced.lock().push("sync")),
                FileSyncType::Fsync,
            )
            .unwrap();
        io.drain_completions(&[w, s]).unwrap();
        if order.lock()[0] != "write" {
            continue;
        }
        // The write completed first, but after the sync started: the sync does not cover it.
        write_first += 1;
        assert!(
            (0..32).any(|c| after_crash(&io, c) != Some(vec![3; SECTOR])),
            "seed {seed}: every crash kept a write no sync covered"
        );
    }
    assert!(write_first > 0, "the write never completed first");
}

#[test]
fn completions_arrive_in_varying_order() {
    let mut orders = std::collections::BTreeSet::new();
    for seed in 0..32 {
        let io = CrashIO::new(seed);
        let file = durable_file(&io);
        let order = Arc::new(Mutex::new(Vec::new()));
        let cs: Vec<_> = (0..3u8)
            .map(|i| {
                let order = order.clone();
                file.pwrite(
                    u64::from(i) * SECTOR as u64,
                    Arc::new(Buffer::new(vec![i; SECTOR])),
                    Completion::new_write(move |_| order.lock().push(i)),
                )
                .unwrap()
            })
            .collect();
        io.drain_completions(&cs).unwrap();
        orders.insert(order.lock().clone());
    }
    assert!(
        orders.len() > 1,
        "always completed in one order: {orders:?}"
    );
}

#[test]
fn a_new_file_needs_its_directory_synced_to_survive() {
    let io = CrashIO::new(5);
    let file = open(&io, OpenFlags::Create);
    write(&io, &file, 0, &[1; SECTOR]);
    sync(&io, &file).unwrap();
    let kept = (0..200).filter(|&s| after_crash(&io, s).is_some()).count();
    assert!(kept > 0 && kept < 200, "kept in {kept} of 200 crashes");
    io.sync_directory("/db").unwrap();
    assert!((0..200).all(|s| after_crash(&io, s) == Some(vec![1; SECTOR])));
}

#[test]
fn direct_io_refuses_what_is_not_aligned() {
    let io = CrashIO::new(6);
    let file = open(&io, OpenFlags::Create | OpenFlags::Direct);
    let unaligned = file.pwrite(
        0,
        Arc::new(Buffer::new(vec![0; 100])),
        Completion::new_write(|_| {}),
    );
    assert!(unaligned.is_err());
    let misplaced = file.pwrite(
        512,
        Arc::new(Buffer::new_temporary(DIRECT_IO_ALIGNMENT)),
        Completion::new_write(|_| {}),
    );
    assert!(misplaced.is_err());
}

#[test]
fn writes_past_the_capacity_fail_with_no_space() {
    let io = CrashIO::new(7);
    let file = durable_file(&io);
    io.set_capacity(Some(SECTOR as u64));
    write(&io, &file, 0, &[1; SECTOR]);
    let c = file
        .pwrite(
            SECTOR as u64,
            Arc::new(Buffer::new(vec![1; SECTOR])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    let err = io.wait_for_completion(c).unwrap_err();
    assert!(
        matches!(
            err,
            LimboError::CompletionError(CompletionError::IOError(ErrorKind::StorageFull, _))
        ),
        "{err:?}"
    );
}
