//! Streams' first part (DESIGN.md §21): batches appended to a partition read back by offset, from
//! several producers at once in offset order, through a crash, and past whole segments let go.

use super::*;
use crate::io::crash::CrashIO;
use crate::io::MemoryIO;
use crate::storage::atoll::drive;

const BASE: &str = "/db/stream-p0";

fn config() -> LogConfig {
    LogConfig {
        segment_bytes: 256 << 10,
        buffer_bytes: 64 << 10,
        chunk_bytes: 16 << 10,
        writes_in_flight: 3,
        direct: true,
    }
}

/// Message `i` of a test: its number, then filler of a length that varies with it, small enough
/// that batches of a few go through the ring and large ones are published in place.
fn message(i: u64) -> Vec<u8> {
    let mut m = i.to_le_bytes().to_vec();
    m.resize(8 + (i * 7919 % 900) as usize, (i % 251) as u8);
    m
}

/// Appends messages `from..to` in batches of up to `per`, each with time `i`; returns each batch's
/// first offset and count.
fn append(io: &Arc<dyn IO>, p: &Partition, from: u64, to: u64, per: u64) -> Vec<(u64, u32)> {
    let mut out = Vec::new();
    let mut i = from;
    while i < to {
        let mut batch = p.batch();
        let mut n = 0;
        while i < to && n < per && batch.push(&message(i), i) {
            i += 1;
            n += 1;
        }
        assert!(n > 0, "a message fits no batch");
        let mut op = Append::new(batch);
        out.push(drive(io, || p.append(&mut op)).unwrap());
    }
    out
}

/// Every message from `from` on, as reads of up to `bytes` return them.
fn read_all(p: &Partition, from: u64, bytes: usize) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::new();
    let mut at = from;
    loop {
        let Read::Batches(batches) = p.read(at, bytes).unwrap() else {
            panic!("{at} expired");
        };
        if batches.is_empty() {
            return out;
        }
        for b in &batches {
            for (offset, m) in b.messages() {
                if offset >= at {
                    out.push((offset, m.to_vec()));
                }
            }
        }
        at = out.last().map_or(at, |&(o, _)| o + 1);
    }
}

/// Batches large and small read back by offset, message by message, from any offset, the first
/// read answering the whole batch holding it; offsets run on with no gap from 0.
#[test]
fn batches_read_back_by_offset() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let p = Partition::open(io.clone(), BASE, config()).unwrap();
    let mut published = append(&io, &p, 0, 300, 1);
    published.extend(append(&io, &p, 300, 3000, 40));
    let mut next = 0;
    for &(base, count) in &published {
        assert_eq!(base, next, "offsets run on");
        next += count as u64;
    }
    assert_eq!(p.next_offset(), 3000);
    let all = read_all(&p, 0, 1 << 20);
    assert_eq!(all.len(), 3000);
    for (i, (offset, m)) in all.iter().enumerate() {
        assert_eq!(*offset, i as u64);
        assert_eq!(m, &message(i as u64), "message {i}");
    }
    for from in [0, 1, 299, 300, 1234, 2999] {
        let Read::Batches(b) = p.read(from, 1).unwrap() else {
            panic!("expired")
        };
        assert_eq!(b.len(), 1, "one batch, past the bytes asked for");
        let first = b[0].base();
        assert!(first <= from && from < first + b[0].count() as u64);
    }
    assert_eq!(p.read(3000, 1 << 20).unwrap(), Read::Batches(Vec::new()));
}

/// Producers on threads append to one partition at once: every message reads back once, offsets
/// rise with the log, and each producer's batches lie in the order it appended them.
#[test]
fn producers_at_once_take_offsets_in_log_order() {
    for io in [
        Arc::new(MemoryIO::new()) as Arc<dyn IO>,
        Arc::new(CrashIO::new(3)) as Arc<dyn IO>,
    ] {
        let p = Partition::open(io.clone(), BASE, config()).unwrap();
        let (producers, each) = (6u64, 400u64);
        let handles: Vec<_> = (0..producers)
            .map(|w| {
                let (io, p) = (io.clone(), p.clone());
                crate::thread::spawn(move || {
                    let mut seq = 0;
                    let mut i = 0;
                    while i < each {
                        let mut batch = p.batch();
                        batch.from_producer(w, seq);
                        let mut n = 0;
                        while i < each && n < 1 + (i % 9) && batch.push(&message(w << 32 | i), i) {
                            i += 1;
                            n += 1;
                        }
                        let mut op = Append::new(batch);
                        drive(&io, || p.append(&mut op)).unwrap();
                        seq += 1;
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let Read::Batches(batches) = p.read(0, usize::MAX).unwrap() else {
            panic!("expired")
        };
        let (mut next, mut seqs) = (0, vec![0u64; producers as usize]);
        let mut messages = 0;
        for b in &batches {
            assert_eq!(b.base(), next, "offsets rise with the log");
            next += b.count() as u64;
            let (w, seq) = b.producer();
            assert_eq!(seq, seqs[w as usize], "producer {w}'s batches out of order");
            seqs[w as usize] += 1;
            for (_, m) in b.messages() {
                let n = u64::from_le_bytes(m[..8].try_into().unwrap());
                assert_eq!(m, &message(n)[..]);
                messages += 1;
            }
        }
        assert_eq!(messages, producers * each);
    }
}

/// A crash mid-append recovers every batch whose append was done and nothing torn; appending
/// goes on with the next offset past the gap the crash left, through a second crash, and reads
/// pass the gaps.
#[test]
fn a_crash_recovers_every_appended_batch_and_appends_resume_past_its_gap() {
    let crash_io = Arc::new(CrashIO::new(9));
    let io: Arc<dyn IO> = crash_io.clone();
    let p = Partition::open(io.clone(), BASE, config()).unwrap();
    append(&io, &p, 0, 500, 7);
    let done = p.next_offset();
    crash_io.capture(|_| true, 5, 1);
    append(&io, &p, done, done + 200, 3);
    drop(p);
    let images = crash_io.captured();
    assert!(!images.is_empty());
    let mut checked = 0;
    for (n, image) in images.into_iter().enumerate() {
        let io: Arc<dyn IO> = Arc::new(image);
        let p = Partition::open(io.clone(), BASE, config()).unwrap();
        let next = p.next_offset();
        assert!(
            next >= done,
            "image {n}: {next} offsets, fewer than the {done} appended"
        );
        let all = read_all(&p, 0, 1 << 16);
        assert_eq!(all.len() as u64, next, "image {n}");
        assert!(all.iter().all(|(o, m)| m == &message(*o)), "image {n}");
        // Appends go on past the gap, and survive a second open.
        append(&io, &p, next, next + 50, 4);
        drop(p);
        let p = Partition::open(io.clone(), BASE, config()).unwrap();
        assert_eq!(p.next_offset(), next + 50, "image {n}, reopened");
        let all = read_all(&p, 0, 1 << 16);
        assert_eq!(all.len() as u64, next + 50, "image {n}, reopened");
        assert!(
            all.iter().all(|(o, m)| m == &message(*o)),
            "image {n}, reopened"
        );
        checked += 1;
    }
    assert!(checked > 0);
}

/// Letting go of history before an offset deletes whole segments before its batch, keeps every
/// message from that batch on, answers a read before it as expired, and holds across an open.
#[test]
fn whole_segments_before_the_first_retained_batch_are_let_go() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let p = Partition::open(io.clone(), BASE, config()).unwrap();
    append(&io, &p, 0, 6000, 30);
    let segment = |n: u64| format!("{BASE}.log.{n:016x}");
    assert!(io.open_file(&segment(0), OpenFlags::None, false).is_ok());
    let keep = 4321;
    p.retain_from(keep).unwrap();
    let first = p.first_offset();
    assert!(
        first <= keep && keep - first < 30,
        "the first retained, {first}"
    );
    assert!(
        io.open_file(&segment(0), OpenFlags::None, false).is_err(),
        "the first segment kept"
    );
    assert_eq!(p.read(first - 1, 1).unwrap(), Read::Expired { first });
    let check = |p: &Partition| {
        let all = read_all(p, first, 1 << 16);
        assert_eq!(all.first().map(|m| m.0), Some(first));
        assert_eq!(all.len() as u64, 6000 - first);
        assert!(all.iter().all(|(o, m)| m == &message(*o)));
    };
    check(&p);
    drop(p);
    let p = Partition::open(io.clone(), BASE, config()).unwrap();
    assert_eq!(p.first_offset(), first);
    check(&p);
    append(&io, &p, 6000, 6100, 10);
    assert_eq!(p.next_offset(), 6100);
}

/// A crash at any point of a partition's own file's writes, from its creation to the first
/// letting go of history, leaves a partition that opens with every batch appended before the crash
/// (audit 2026-11-06): its first copy is written whole before any batch, so a torn update of one
/// copy leaves the other, and a creation the crash cut short opens as new.
#[test]
fn a_crash_during_the_first_metadata_writes_leaves_a_partition_that_opens() {
    let own = |path: &str| path.ends_with(".stream");
    let mut images_checked = 0;
    for seed in 0..20 {
        // Creation cut short: nothing appended yet, and the partition opens empty.
        let crash_io = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash_io.clone();
        crash_io.capture(own, 1, seed);
        let p = Partition::open(io.clone(), BASE, config()).unwrap();
        for image in crash_io.captured() {
            let io: Arc<dyn IO> = Arc::new(image);
            let p = Partition::open(io.clone(), BASE, config()).unwrap();
            assert_eq!(p.next_offset(), 0, "seed {seed}");
            append(&io, &p, 0, 5, 2);
            assert_eq!(read_all(&p, 0, 1 << 16).len(), 5, "seed {seed}");
            images_checked += 1;
        }
        // The first letting go cut short: every message from 40 on, each durable once appended,
        // reads back.
        append(&io, &p, 0, 60, 1);
        crash_io.capture(own, 1, seed);
        p.retain_from(40).unwrap();
        for image in crash_io.captured() {
            let io: Arc<dyn IO> = Arc::new(image);
            let p = Partition::open(io.clone(), BASE, config()).unwrap();
            let read = read_all(&p, 40, 1 << 16);
            assert_eq!(
                read.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
                (40..60).collect::<Vec<_>>(),
                "seed {seed}"
            );
            images_checked += 1;
        }
    }
    assert!(images_checked >= 40, "{images_checked} images");
}

/// An IO that holds the first open of the log's first segment until let go: a read paused after it
/// read the partition's state and before it reached the segment.
struct PausingOpen {
    inner: MemoryIO,
    pause: crate::storage::atoll::locks::Mutex<
        Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    >,
}

impl crate::io::Clock for PausingOpen {
    fn current_time_monotonic(&self) -> crate::io::clock::MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> crate::io::clock::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for PausingOpen {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        if path.ends_with(".log.0000000000000000") {
            if let Some((entered, resume)) = self.pause.lock().take() {
                entered.send(()).unwrap();
                resume.recv().unwrap();
            }
        }
        self.inner.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }

    fn rename_file(&self, from: &str, to: &str) -> Result<bool> {
        self.inner.rename_file(from, to)
    }

    fn sync_directory(&self, path: &str) -> Result<()> {
        self.inner.sync_directory(path)
    }

    fn step(&self) -> Result<()> {
        self.inner.step()
    }
}

/// A read that read the partition's state before history was let go reads every batch it found
/// there (audit 2026-11-06): the segments it reads are deleted only once it is done, never under
/// it as if the stream ended; a read after it is answered as expired.
#[test]
fn a_read_under_way_keeps_the_segments_retention_lets_go_until_it_is_done() {
    use std::sync::mpsc::{channel, RecvTimeoutError};
    use std::time::Duration;
    let backing = Arc::new(PausingOpen {
        inner: MemoryIO::new(),
        pause: crate::storage::atoll::locks::Mutex::new(None),
    });
    let io: Arc<dyn IO> = backing.clone();
    let config = LogConfig {
        segment_bytes: 64 << 10,
        ..config()
    };
    let p = Partition::open(io.clone(), BASE, config).unwrap();
    append(&io, &p, 0, 60, 1);
    assert!(
        p.log().end() > 2 * (64 << 10),
        "the batches span segments past the first"
    );
    let (entered, entered_rx) = channel();
    let (resume_tx, resume) = channel();
    *backing.pause.lock() = Some((entered, resume));
    let reader = std::thread::spawn({
        let p = p.clone();
        move || p.read(0, usize::MAX).unwrap()
    });
    entered_rx.recv().unwrap();
    let (retained_tx, retained) = channel();
    let retention = std::thread::spawn({
        let p = p.clone();
        move || {
            p.retain_from(40).unwrap();
            retained_tx.send(()).unwrap();
        }
    });
    // Retention lets go of nothing the paused read holds: it waits for the read.
    assert_eq!(
        retained.recv_timeout(Duration::from_millis(500)),
        Err(RecvTimeoutError::Timeout),
        "retention deleted segments under a read"
    );
    resume_tx.send(()).unwrap();
    let Read::Batches(batches) = reader.join().unwrap() else {
        panic!("the read expired");
    };
    let offsets: Vec<u64> = batches
        .iter()
        .flat_map(|b| b.messages().map(|(o, _)| o).collect::<Vec<_>>())
        .collect();
    assert_eq!(offsets, (0..60).collect::<Vec<_>>());
    retention.join().unwrap();
    assert_eq!(p.first_offset(), 40);
    assert!(matches!(
        p.read(0, usize::MAX).unwrap(),
        Read::Expired { first: 40 }
    ));
    assert_eq!(read_all(&p, 40, 1 << 16).len(), 20);
}
