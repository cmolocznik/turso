use super::record::ALIGN;
use super::*;
use crate::io::crash::{CrashIO, Faults};
use crate::io::{Completion, File, MemoryIO, OpenFlags, IO};
use crate::storage::atoll::aligned::AlignedBlock;
use crate::storage::atoll::testing::drive;
use std::cell::Cell;

const BASE: &str = "/db/x";

/// Small enough that a test crosses chunks, segments and a full buffer many times.
fn small() -> LogConfig {
    LogConfig {
        segment_bytes: 64 << 10,
        buffer_bytes: 32 << 10,
        chunk_bytes: 8 << 10,
        writes_in_flight: 3,
        direct: true,
    }
}

fn header(txn: u64) -> Header {
    Header {
        kind: Kind::new(1).unwrap(),
        flags: 0,
        txn,
        prev: None,
    }
}

/// Who appended a record, its number, and filler of a length that varies with both.
fn payload(writer: u64, seq: u64) -> Vec<u8> {
    let filler = 1 + ((writer * 7_919 + seq * 104_729) % 600) as usize;
    let mut p = Vec::with_capacity(16 + filler);
    p.extend_from_slice(&writer.to_le_bytes());
    p.extend_from_slice(&seq.to_le_bytes());
    p.resize(16 + filler, (seq % 251) as u8);
    p
}

fn read_all(io: Arc<dyn IO>, start: Lsn) -> (Vec<OwnedRecord>, Lsn) {
    let mut reader = Log::reader(io, BASE, &small(), start);
    let mut records = Vec::new();
    while let Some(r) = reader.next_record().unwrap() {
        records.push(r);
    }
    (records, reader.position())
}

/// Writers append records at once, waiting on every seventeenth as a commit does, through a log
/// small enough that they wait to be admitted, pad blocks and cross chunks and segments: every
/// record reads back once, each writer's in its order. Miri sees a copy into the ring racing the
/// write out of it, the two ordered only by the log's own publication. On memory, writes land
/// within their submission; on the crash backend, later and in any order, durable as they land
/// or once synced.
fn records_appended_from_many_threads_read_back_once_each_in_each_threads_order(
    io: Arc<dyn IO>,
    writers: u64,
    each: u64,
) {
    let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
    let handles: Vec<_> = (0..writers)
        .map(|writer| {
            let (io, log) = (io.clone(), log.clone());
            crate::thread::spawn(move || {
                for seq in 0..each {
                    let p = payload(writer, seq);
                    let mut admitted = drive(&io, || {
                        log.admit(&mut LogWait::default(), framed_len(p.len()))
                    })
                    .unwrap();
                    let lsn = log.append(&mut admitted, &header(writer), &[&p]);
                    if seq % 17 == 0 {
                        drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    drive(&io, || log.wait_durable(&mut LogWait::default(), log.end())).unwrap();
    let (records, _) = read_all(io, 0);
    assert_eq!(records.len() as u64, writers * each);
    let mut next = vec![0u64; writers as usize];
    let mut last = 0;
    for r in &records {
        assert!(r.lsn > last, "out of order at {}", r.start);
        last = r.lsn;
        let writer = r.header.txn;
        let seq = u64::from_le_bytes(r.payload[8..16].try_into().unwrap());
        assert_eq!(seq, next[writer as usize], "writer {writer} out of order");
        assert_eq!(r.payload, payload(writer, seq));
        next[writer as usize] += 1;
    }
}

/// The backends the threaded test runs on: memory, and the crash backend with and without
/// durable writes, its writes landing in an order its seed picks.
fn threaded_backends(seed: u64) -> Vec<Arc<dyn IO>> {
    let durable = Arc::new(CrashIO::new(seed));
    let synced = Arc::new(CrashIO::new(seed));
    synced.set_durable_writes(false);
    vec![Arc::new(MemoryIO::new()), durable, synced]
}

#[cfg(not(shuttle))]
#[test]
fn records_appended_from_many_threads_read_back_once_each_in_each_threads_order_on_threads() {
    let (writers, each) = if cfg!(miri) { (3, 40) } else { (8, 300) };
    for io in threaded_backends(7) {
        records_appended_from_many_threads_read_back_once_each_in_each_threads_order(
            io, writers, each,
        );
    }
}

#[cfg(shuttle)]
#[test]
fn records_appended_from_many_threads_read_back_once_each_in_each_threads_order_in_shuttle_schedules(
) {
    crate::storage::atoll::testing::schedules(
        || {
            let seed = crate::storage::atoll::testing::workload_seed();
            for io in threaded_backends(seed) {
                records_appended_from_many_threads_read_back_once_each_in_each_threads_order(
                    io, 2, 40,
                );
            }
        },
        300,
    );
}

/// Writers append records into the ring and publish others in place from blocks of their own
/// (DESIGN.md §11.4.1), some waited on, through a log small enough to wait for room and cross
/// chunks and segments: every record reads back once, each writer's in its order, an in-place
/// record's payload whole, and every in-place record starts on a block boundary.
fn records_published_in_place_read_back_among_the_rings(io: Arc<dyn IO>, writers: u64, each: u64) {
    let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
    let handles: Vec<_> = (0..writers)
        .map(|writer| {
            let (io, log) = (io.clone(), log.clone());
            crate::thread::spawn(move || {
                for seq in 0..each {
                    let mut p = payload(writer, seq);
                    let lsn = if seq % 3 == 1 {
                        // Its payload past a block, up to a chunk less the framing.
                        p.resize(4096 + ((seq * 977) % 3000) as usize, (seq % 251) as u8);
                        let mut block = AlignedBlock::zeroed(small().chunk_bytes as usize);
                        block.bytes_mut()[record::HEADER..record::HEADER + p.len()]
                            .copy_from_slice(&p);
                        let room = framed_len(p.len()).next_multiple_of(BLOCK) + BLOCK;
                        let mut admitted =
                            drive(&io, || log.admit(&mut LogWait::default(), room)).unwrap();
                        let (at, lsn) =
                            log.publish(&mut admitted, &header(writer), block, p.len(), |_| {});
                        assert!(at.get().is_multiple_of(BLOCK), "in place at {}", at.get());
                        lsn
                    } else {
                        let mut admitted = drive(&io, || {
                            log.admit(&mut LogWait::default(), framed_len(p.len()))
                        })
                        .unwrap();
                        log.append(&mut admitted, &header(writer), &[&p])
                    };
                    if seq % 7 == 0 {
                        drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    drive(&io, || log.wait_durable(&mut LogWait::default(), log.end())).unwrap();
    let (records, _) = read_all(io, 0);
    assert_eq!(records.len() as u64, writers * each);
    let mut next = vec![0u64; writers as usize];
    let mut in_place = 0;
    for r in &records {
        let writer = r.header.txn;
        let seq = u64::from_le_bytes(r.payload[8..16].try_into().unwrap());
        assert_eq!(seq, next[writer as usize], "writer {writer} out of order");
        let mut want = payload(writer, seq);
        if seq % 3 == 1 {
            want.resize(4096 + ((seq * 977) % 3000) as usize, (seq % 251) as u8);
            assert!(r.start.is_multiple_of(BLOCK), "in place at {}", r.start);
            in_place += 1;
        }
        assert_eq!(r.payload, want, "writer {writer} record {seq}");
        next[writer as usize] += 1;
    }
    assert!(in_place > 0);
}

#[cfg(not(shuttle))]
#[test]
fn records_published_in_place_read_back_among_the_rings_on_threads() {
    let (writers, each) = if cfg!(miri) { (2, 20) } else { (6, 120) };
    for io in threaded_backends(11) {
        records_published_in_place_read_back_among_the_rings(io, writers, each);
    }
}

/// A log dropped while its writes are in flight is freed at once: they wait in the log's own IO,
/// so a completion that held the log would keep it, its ring and its files alive until the IO
/// next steps, and for good once nothing does.
#[cfg(not(shuttle))]
#[test]
fn a_log_dropped_with_writes_in_flight_is_freed() {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
    let p = payload(0, 0);
    let mut admitted = drive(&io, || {
        log.admit(&mut LogWait::default(), framed_len(p.len()))
    })
    .unwrap();
    let lsn = log.append(&mut admitted, &header(0), &[&p]);
    drop(admitted);
    // Asks for the flush, and nothing steps the IO from here: its write stays in flight.
    let waiting = matches!(
        log.wait_durable(&mut LogWait::default(), lsn).unwrap(),
        IOResult::IO(_)
    );
    assert!(waiting && crash.in_flight() > 0, "no write left in flight");
    let held = Arc::downgrade(&log);
    drop(log);
    assert!(
        held.upgrade().is_none(),
        "a write in flight kept the dropped log alive"
    );
}

/// Appends records one after another, some waited on as commits are, and after every few IO
/// steps checks what a crash then would leave: an unbroken prefix of what was appended that
/// holds every record waited on. Then reopens one such crash and appends past it.
fn a_crash_leaves_an_unbroken_prefix_holding_every_acknowledged_record(durable_writes: bool) {
    // One block per chunk, so a flush of several blocks is several writes in flight, completing
    // in any order.
    let config = LogConfig {
        chunk_bytes: BLOCK,
        ..small()
    };
    for seed in 0..12u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        crash_io.set_durable_writes(durable_writes);
        let io: Arc<dyn IO> = crash_io.clone();
        let log = Log::open(io.clone(), BASE, config.clone(), 0).unwrap();
        assert_eq!(log.durable_writes(), durable_writes);
        let appended = Cell::new(0u64);
        let acknowledged = Cell::new(0u64);
        let steps = Cell::new(0u64);
        let check = |crash_seed: u64| {
            let crashed: Arc<dyn IO> = Arc::new(crash_io.crash(crash_seed));
            let (records, _) = read_all(crashed, 0);
            let kept = records.len() as u64;
            assert!(
                kept >= acknowledged.get() && kept <= appended.get(),
                "seed {seed}: {kept} records survived; {} acknowledged, {} appended",
                acknowledged.get(),
                appended.get()
            );
            for (seq, r) in records.iter().enumerate() {
                assert_eq!(
                    r.payload,
                    payload(0, seq as u64),
                    "seed {seed}: record {seq}"
                );
            }
        };
        let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| loop {
            match op().unwrap() {
                IOResult::Done(()) => return,
                IOResult::IO(IOCompletions(c)) => {
                    while !c.finished() {
                        io.step().unwrap();
                        steps.set(steps.get() + 1);
                        check(seed * 100_000 + steps.get());
                    }
                    if let Some(e) = c.get_error() {
                        panic!("seed {seed}: {e}");
                    }
                }
            }
        };
        for seq in 0..400u64 {
            let p = payload(0, seq);
            let mut admitted = None;
            run(&mut || {
                admitted = Some(
                    match log.admit(&mut LogWait::default(), framed_len(p.len()))? {
                        IOResult::Done(a) => a,
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    },
                );
                Ok(IOResult::Done(()))
            });
            let lsn = log.append(admitted.as_mut().unwrap(), &header(0), &[&p]);
            appended.set(seq + 1);
            // Waits at varying distances, so flushes run from part of a block to several.
            if (seq * 7 + seed) % 23 == 0 {
                run(&mut || log.wait_durable(&mut LogWait::default(), lsn));
                acknowledged.set(seq + 1);
                // Right after the acknowledgement, with whatever writes are still in flight.
                for c in 0..4 {
                    check(seed * 100_000 + steps.get() * 10 + c);
                }
            }
        }
        // Reopen a crash, append past it, and read both runs of records back.
        let crashed = Arc::new(crash_io.crash(seed));
        let crashed_io: Arc<dyn IO> = crashed.clone();
        let (before, end) = read_all(crashed_io.clone(), 0);
        assert!(before.len() as u64 >= acknowledged.get());
        let reopened = Log::open(crashed_io.clone(), BASE, small(), end).unwrap();
        let resumed = reopened.end();
        for seq in 0..50u64 {
            let p = payload(1, seq);
            let mut admitted = drive(&crashed_io, || {
                reopened.admit(&mut LogWait::default(), framed_len(p.len()))
            })
            .unwrap();
            let lsn = reopened.append(&mut admitted, &header(1), &[&p]);
            drive(&crashed_io, || {
                reopened.wait_durable(&mut LogWait::default(), lsn)
            })
            .unwrap();
        }
        // Where the crash left a partial block, a reader from the start stops there; where it
        // ended on a block boundary, the new records continue the log.
        let (again, _) = read_all(crashed_io.clone(), 0);
        assert_eq!(
            again[..before.len()],
            before[..],
            "seed {seed}: reopening changed the records before it"
        );
        let continues = end == resumed;
        assert_eq!(
            again.len(),
            before.len() + if continues { 50 } else { 0 },
            "seed {seed}"
        );
        let (after, _) = read_all(crashed_io, resumed);
        let seqs: Vec<u64> = after
            .iter()
            .map(|r| u64::from_le_bytes(r.payload[8..16].try_into().unwrap()))
            .collect();
        assert_eq!(seqs, (0..50).collect::<Vec<_>>(), "seed {seed}");
    }
}

#[test]
fn a_crash_leaves_an_unbroken_prefix_with_durable_writes() {
    a_crash_leaves_an_unbroken_prefix_holding_every_acknowledged_record(true);
}

#[test]
fn a_crash_leaves_an_unbroken_prefix_with_writes_then_syncs() {
    a_crash_leaves_an_unbroken_prefix_holding_every_acknowledged_record(false);
}

#[test]
fn no_byte_of_the_log_is_written_twice() {
    let crash_io = Arc::new(CrashIO::new(9));
    let io: Arc<dyn IO> = crash_io.clone();
    let config = small();
    let log = Log::open(io.clone(), BASE, config.clone(), 0).unwrap();
    for seq in 0..400u64 {
        let p = payload(0, seq);
        let mut admitted = drive(&io, || {
            log.admit(&mut LogWait::default(), framed_len(p.len()))
        })
        .unwrap();
        let lsn = log.append(&mut admitted, &header(0), &[&p]);
        // Waiting on every record pads a partial block each time.
        drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
    }
    let segments = log.end() / config.segment_bytes + 1;
    let mut log_writes = 0;
    for number in 0..segments {
        let writes = crash_io.writes_to(&format!("{BASE}.log.{number:016x}"));
        // Preparing a segment fills it with zeros before any record goes in.
        let mut zeroed = 0u64;
        let mut written = writes.into_iter().skip_while(|&(_, len)| {
            let filling = zeroed < config.segment_bytes;
            if filling {
                zeroed += len as u64;
            }
            filling
        });
        let mut covered: Vec<(u64, u64)> = Vec::new();
        for (at, len) in written.by_ref() {
            let (start, end) = (at, at + len as u64);
            assert!(
                start % BLOCK == 0 && end % BLOCK == 0,
                "segment {number}: {start}..{end}"
            );
            for &(s, e) in &covered {
                assert!(
                    end <= s || start >= e,
                    "segment {number}: {start}..{end} rewrote {s}..{e}"
                );
            }
            covered.push((start, end));
            log_writes += 1;
        }
    }
    assert!(
        log_writes > 400 / 2,
        "only {log_writes} log writes were seen"
    );
}

/// Segments released below where the log is kept become spares, and later segments are those
/// spares renamed, prepared with no zeros: every byte written to the log is a zero of a segment
/// prepared fresh or a block of records, each once. A reader from where the log is kept reads
/// exactly the records appended since, never one a spare held in its earlier life, and so does
/// every crash, which keeps an unbroken prefix of them; a crash reopened takes its spares back.
#[test]
fn released_segments_come_back_as_spares_whose_old_records_never_read() {
    let crash_io = Arc::new(CrashIO::new(3));
    let io: Arc<dyn IO> = crash_io.clone();
    let config = small();
    let size = config.segment_bytes;
    let log = Log::open(io.clone(), BASE, config.clone(), 0).unwrap();
    let mut lsns = Vec::new();
    // Where the log is kept from, and the first record a reader from there reads.
    let (mut from, mut first) = (0, 0);
    let mut acknowledged = 0;
    for seq in 0..3_000u64 {
        let p = payload(0, seq);
        let mut admitted = drive(&io, || {
            log.admit(&mut LogWait::default(), framed_len(p.len()))
        })
        .unwrap();
        lsns.push(log.append(&mut admitted, &header(0), &[&p]));
        if seq % 50 == 49 {
            let lsn = *lsns.last().unwrap();
            drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
            acknowledged = lsns.len();
            // As a checkpoint does: the log before a record two segments back is let go.
            if let Some(i) = lsns.iter().rposition(|&l| l + 2 * size <= lsn) {
                (from, first) = (lsns[i], i + 1);
                log.release_before(from).unwrap();
            }
        }
    }
    let used = log.end().div_ceil(size);
    assert!(used >= 8, "only {used} segments used");
    let segment = |n: u64| format!("{BASE}.log.{n:016x}");
    let written: u64 = (0..used)
        .flat_map(|n| crash_io.writes_to(&segment(n)))
        .map(|(_, len)| len as u64)
        .sum();
    let flushed = log.durable().next_multiple_of(BLOCK);
    let zeroed = written - flushed;
    assert_eq!(
        zeroed % size,
        0,
        "{written} bytes written, {flushed} of records"
    );
    let fresh = zeroed / size;
    assert!(
        fresh + 2 < used,
        "{fresh} of {used} segments were prepared with zeros"
    );
    let expect = |records: &[OwnedRecord], label: &str| {
        for (k, r) in records.iter().enumerate() {
            assert_eq!(
                r.payload,
                payload(0, (first + k) as u64),
                "{label}: record {k}"
            );
        }
    };
    let (records, _) = read_all(io.clone(), from);
    assert_eq!(records.len(), lsns.len() - first);
    expect(&records, "the log");
    for c in 0..16 {
        let crashed: Arc<dyn IO> = Arc::new(crash_io.crash(c));
        let (records, end) = read_all(crashed.clone(), from);
        assert!(
            records.len() >= acknowledged - first,
            "crash {c}: {} records from {from}",
            records.len()
        );
        expect(&records, &format!("crash {c}"));
        let reopened = Log::open(crashed.clone(), BASE, config.clone(), end).unwrap();
        let p = payload(1, 0);
        let mut admitted = drive(&crashed, || {
            reopened.admit(&mut LogWait::default(), framed_len(p.len()))
        })
        .unwrap();
        let lsn = reopened.append(&mut admitted, &header(1), &[&p]);
        drive(&crashed, || {
            reopened.wait_durable(&mut LogWait::default(), lsn)
        })
        .unwrap();
        let (again, _) = read_all(crashed, from);
        assert_eq!(again[..records.len()], records[..], "crash {c}: reopened");
    }
}

#[test]
fn a_failed_write_fails_its_waiters_and_every_later_operation() {
    let crash_io = Arc::new(CrashIO::new(3));
    let io: Arc<dyn IO> = crash_io.clone();
    let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
    let p = payload(0, 0);
    let mut admitted = drive(&io, || {
        log.admit(&mut LogWait::default(), framed_len(p.len()))
    })
    .unwrap();
    let lsn = log.append(&mut admitted, &header(0), &[&p]);
    drop(admitted);
    crash_io.set_faults(Faults {
        write: 1.0,
        ..Faults::default()
    });
    assert!(drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).is_err());
    assert!(log.failure().is_some());
    assert!(log.admit(&mut LogWait::default(), 64).is_err());
}

/// A segment is zeroed whole before the log uses it: a zero write that lands short fails the open,
/// for the segment's end is not yet in the file.
#[test]
fn a_short_write_while_opening_a_log_fails_the_open() {
    let crash_io = Arc::new(CrashIO::new(3));
    crash_io.set_faults(Faults {
        short_write: 1.0,
        ..Faults::default()
    });
    let io: Arc<dyn IO> = crash_io;
    assert!(Log::open(io, BASE, small(), 0).is_err());
}

/// Padding a partial block moves every record admitted and not yet appended along with it. Here
/// it would push one past the end of the last prepared segment: the record must wait for the next
/// segment's zeros to be written, or they land on it.
#[test]
fn padding_never_pushes_an_admitted_record_into_a_segment_still_being_prepared() {
    let config = small();
    let segment = config.segment_bytes;
    // Records of 1 KiB, so the log fills to a block boundary exactly.
    let filler = vec![7u8; 996];
    assert_eq!(framed_len(filler.len()), 1024);
    let resume = segment - 3 * BLOCK;
    for seed in 0..16 {
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let log = Log::open(io.clone(), BASE, config.clone(), 0).unwrap();
        while log.end() < resume {
            let mut admitted = drive(&io, || log.admit(&mut LogWait::default(), 1024)).unwrap();
            log.append(&mut admitted, &header(0), &[&filler]);
        }
        drive(&io, || log.wait_durable(&mut LogWait::default(), resume)).unwrap();
        drop(log);
        // Reopened there, as after a crash: the next segment is prepared only once asked for.
        let log = Log::open(io.clone(), BASE, config.clone(), resume).unwrap();
        let first = vec![1u8; 36];
        let mut admitted_first = drive(&io, || {
            log.admit(&mut LogWait::default(), framed_len(first.len()))
        })
        .unwrap();
        // The second fits before the segment's end from where the log stands now.
        let second = vec![2u8; (segment - resume - 64 - 64) as usize - 28];
        let second_len = framed_len(second.len());
        assert!(resume + framed_len(first.len()) + second_len <= segment);
        let mut admitted_second =
            drive(&io, || log.admit(&mut LogWait::default(), second_len)).unwrap();
        let lsn = log.append(&mut admitted_first, &header(1), &[&first]);
        // Waiting on the first pads its block, moving the second past the segment's end.
        drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
        let lsn = log.append(&mut admitted_second, &header(2), &[&second]);
        drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
        while crash.in_flight() > 0 {
            io.step().unwrap();
        }
        let (records, _) = read_all(io, resume);
        assert_eq!(
            records.len(),
            2,
            "seed {seed}: the log holds {} records",
            records.len()
        );
        assert_eq!(records[1].payload, second, "seed {seed}");
    }
}

/// The records of unit `seq` of a log of singles and groups: one record, or a group of two to
/// four, with lengths that vary so groups cross blocks, chunks and segments.
fn unit(seq: u64) -> Vec<Vec<u8>> {
    let records = [1, 2, 3, 4][(seq % 4) as usize];
    (0..records)
        .map(|i| {
            let mut p = payload(i, seq);
            p.resize(p.len() + (seq as usize * 131 + i as usize * 977) % 1800, 9);
            p
        })
        .collect()
}

fn append_unit(log: &Arc<Log>, admitted: &mut Admitted, records: &[Vec<u8>]) -> Lsn {
    if records.len() == 1 {
        return log.append(admitted, &header(0), &[&records[0]]);
    }
    let pieces: Vec<[&[u8]; 1]> = records.iter().map(|r| [r.as_slice()]).collect();
    let group: Vec<(Header, &[&[u8]])> = pieces.iter().map(|p| (header(0), &p[..])).collect();
    let (start, lsns) = log.append_group(admitted, &group);
    assert_eq!(start + framed_len(records[0].len()), lsns[0]);
    *lsns.last().unwrap()
}

/// Checks that `records` are units 0, 1, ... in order, each whole; returns how many.
fn whole_units(records: &[OwnedRecord], label: &str) -> u64 {
    let mut at = 0;
    let mut seq = 0;
    while at < records.len() {
        let expected = unit(seq);
        let got = &records[at..(at + expected.len()).min(records.len())];
        assert_eq!(
            got.len(),
            expected.len(),
            "{label}: unit {seq} read in part"
        );
        for (i, (r, p)) in got.iter().zip(&expected).enumerate() {
            assert_eq!(&r.payload, p, "{label}: unit {seq} record {i}");
            assert_eq!(r.group, got[0].start, "{label}: unit {seq} record {i}");
        }
        at += expected.len();
        seq += 1;
    }
    seq
}

fn crashes_leave_whole_groups_or_none_of_them(durable_writes: bool) {
    let config = LogConfig {
        chunk_bytes: BLOCK,
        ..small()
    };
    for seed in 0..8u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        crash_io.set_durable_writes(durable_writes);
        let io: Arc<dyn IO> = crash_io.clone();
        let log = Log::open(io.clone(), BASE, config.clone(), 0).unwrap();
        let appended = Cell::new(0u64);
        let acknowledged = Cell::new(0u64);
        let steps = Cell::new(0u64);
        let check = |crash_seed: u64| {
            let crashed: Arc<dyn IO> = Arc::new(crash_io.crash(crash_seed));
            let (records, _) = read_all(crashed, 0);
            let kept = whole_units(&records, &format!("seed {seed}/{crash_seed}"));
            assert!(
                kept >= acknowledged.get() && kept <= appended.get(),
                "seed {seed}: {kept} units survived; {} acknowledged, {} appended",
                acknowledged.get(),
                appended.get()
            );
        };
        let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| loop {
            match op().unwrap() {
                IOResult::Done(()) => return,
                IOResult::IO(IOCompletions(c)) => {
                    while !c.finished() {
                        io.step().unwrap();
                        steps.set(steps.get() + 1);
                        check(seed * 100_000 + steps.get());
                    }
                }
            }
        };
        for seq in 0..160u64 {
            let records = unit(seq);
            let len = records.iter().map(|r| framed_len(r.len())).sum();
            let mut admitted = None;
            run(&mut || {
                admitted = Some(match log.admit(&mut LogWait::default(), len)? {
                    IOResult::Done(a) => a,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                });
                Ok(IOResult::Done(()))
            });
            let lsn = append_unit(&log, admitted.as_mut().unwrap(), &records);
            appended.set(seq + 1);
            if (seq * 5 + seed) % 11 == 0 {
                run(&mut || log.wait_durable(&mut LogWait::default(), lsn));
                acknowledged.set(seq + 1);
            }
        }
        assert!(steps.get() > 100, "seed {seed}: {} IO steps", steps.get());
    }
}

#[test]
fn crashes_leave_whole_groups_or_none_of_them_with_durable_writes() {
    crashes_leave_whole_groups_or_none_of_them(true);
}

#[test]
fn crashes_leave_whole_groups_or_none_of_them_with_writes_then_syncs() {
    crashes_leave_whole_groups_or_none_of_them(false);
}

#[test]
fn records_appended_after_a_partial_group_never_complete_it() {
    // A record and a group's first record fill the first block exactly, so after the rest of the
    // group is lost the log restarts at the very position the group's second record had.
    let first = vec![1u8; 996];
    let g0 = vec![2u8; 3044];
    assert_eq!(framed_len(first.len()) + framed_len(g0.len()), BLOCK);
    let g1 = vec![3u8; 100];
    for later_group in [false, true] {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
        let mut admitted = drive(&io, || log.admit(&mut LogWait::default(), 2 * BLOCK)).unwrap();
        log.append(&mut admitted, &header(0), &[&first]);
        let (start, lsns) = log.append_group(
            &mut admitted,
            &[(header(0), &[g0.as_slice()]), (header(0), &[g1.as_slice()])],
        );
        assert_eq!((start, lsns[0]), (framed_len(first.len()), BLOCK));
        drive(&io, || log.wait_durable(&mut LogWait::default(), lsns[1])).unwrap();
        drop(log);
        // The crash loses the block holding the group's second record.
        let file = io
            .open_file(&format!("{BASE}.log.{:016x}", 0), OpenFlags::None, false)
            .unwrap();
        let zeros = Arc::new(AlignedBlock::zeroed(BLOCK as usize));
        let c = file
            .pwrite(
                BLOCK,
                zeros.region(0, BLOCK as usize),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        io.wait_for_completion(c).unwrap();
        let (records, end) = read_all(io.clone(), 0);
        assert_eq!(
            (records.len(), end),
            (1, start),
            "the partial group read back"
        );
        let log = Log::open(io.clone(), BASE, small(), end).unwrap();
        assert_eq!(
            log.end(),
            BLOCK,
            "the log restarts where the group's second record was"
        );
        let mut admitted = drive(&io, || log.admit(&mut LogWait::default(), BLOCK)).unwrap();
        let later = vec![4u8; 50];
        let lsn = if later_group {
            log.append_group(
                &mut admitted,
                &[
                    (header(0), &[later.as_slice()]),
                    (header(0), &[later.as_slice()]),
                ],
            )
            .1[1]
        } else {
            log.append(&mut admitted, &header(0), &[&later])
        };
        drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
        let (records, end) = read_all(io.clone(), 0);
        assert_eq!(
            (records.len(), end),
            (1, start),
            "a later {} completed the partial group",
            if later_group { "group" } else { "record" }
        );
        let (records, _) = read_all(io, BLOCK);
        assert_eq!(records.len(), 1 + later_group as usize);
    }
}

/// A walk from group to group reads each whole from where it starts, across blocks, chunks and
/// segments, and refuses a position no group starts at.
#[test]
fn a_group_reads_whole_from_where_it_starts_and_nowhere_else() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
    let mut starts = Vec::new();
    for seq in 0..200 {
        let records = unit(seq);
        let len = records.iter().map(|r| framed_len(r.len())).sum();
        let mut admitted = drive(&io, || log.admit(&mut LogWait::default(), len)).unwrap();
        let mut reserved = None;
        let start = if records.len() == 1 {
            log.append_with(&mut admitted, &header(seq), &[&records[0]], |s| {
                reserved = Some(s)
            })
            .0
        } else {
            let pieces: Vec<[&[u8]; 1]> = records.iter().map(|r| [r.as_slice()]).collect();
            let group: Vec<(Header, &[&[u8]])> =
                pieces.iter().map(|p| (header(seq), &p[..])).collect();
            log.append_group_with(&mut admitted, &group, |s| reserved = Some(s))
                .0
        };
        assert_eq!(reserved, Some(start), "the hook saw another start");
        starts.push((start, records));
    }
    drive(&io, || log.wait_durable(&mut LogWait::default(), log.end())).unwrap();
    for (start, records) in &starts {
        let read = Log::read_group(io.clone(), BASE, &small(), *start).unwrap();
        let payloads: Vec<&[u8]> = read.iter().map(|r| r.payload.as_slice()).collect();
        let expected: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
        assert_eq!(payloads, expected, "the group at {}", start.get());
    }
    // A group's second record starts no group.
    let (start, records) = starts.iter().find(|(_, r)| r.len() > 1).unwrap();
    let inside = GroupStart::at(start.get() + framed_len(records[0].len()));
    assert!(Log::read_group(io.clone(), BASE, &small(), inside).is_err());
    // Past the end, nothing does.
    let past = GroupStart::at(log.end().next_multiple_of(ALIGN));
    assert!(Log::read_group(io.clone(), BASE, &small(), past).is_err());
}

/// A segment of many chunks is zero-filled one write after another on an IO whose writes complete
/// inside their submission, as a memory IO's do, within a small stack: no write's completion runs
/// inside another's, so the fill costs no frame per chunk. 1,024 chunks nested a frame chain each
/// would need well past the 256 KiB stack, the fill's chain taking more than 256 bytes a chunk.
#[test]
fn a_segment_of_many_chunks_fills_on_a_small_stack_when_writes_complete_inline() {
    std::thread::Builder::new()
        .stack_size(256 << 10)
        .spawn(|| {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let config = LogConfig {
                segment_bytes: 4 << 20,
                buffer_bytes: 64 << 10,
                chunk_bytes: 4 << 10,
                writes_in_flight: 3,
                direct: false,
            };
            let log = Log::open(io.clone(), BASE, config, 0).unwrap();
            let p = payload(0, 0);
            let mut admitted = drive(&io, || {
                log.admit(&mut LogWait::default(), framed_len(p.len()))
            })
            .unwrap();
            let lsn = log.append(&mut admitted, &header(0), &[&p]);
            drop(admitted);
            drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
        })
        .unwrap()
        .join()
        .unwrap();
}

/// The log's callbacks only submit and record (DESIGN.md 22.5): every synchronous file call its
/// preparation makes (a create, an allocation, a spare's rename, a directory sync) runs in a
/// caller, never in a callback a step delivers. Over many segments, fresh and spares alike.
#[test]
fn no_callback_of_the_log_does_file_work() {
    let config = small();
    let segment = config.segment_bytes;
    let filler = vec![7u8; 996];
    assert_eq!(framed_len(filler.len()), 1024);
    for seed in 0..4 {
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let log = Log::open(io.clone(), BASE, config.clone(), 0).unwrap();
        let mut released = 0;
        while log.end() < 8 * segment {
            let mut admitted = drive(&io, || log.admit(&mut LogWait::default(), 1024)).unwrap();
            let lsn = log.append(&mut admitted, &header(0), &[&filler]);
            drop(admitted);
            if lsn % (segment / 4) < 1024 {
                drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
                // Two segments behind: those before come back as spares for the next ones.
                if lsn >= released + 3 * segment {
                    released = lsn - 2 * segment;
                    log.release_before(released).unwrap();
                }
            }
        }
        drive(&io, || log.wait_durable(&mut LogWait::default(), log.end())).unwrap();
        assert!(
            log.end() >= 8 * segment,
            "seed {seed}: the log prepared too little"
        );
        let calls = crash.file_work_in_callbacks();
        assert!(calls.is_empty(), "seed {seed}: callbacks made {calls:?}");
    }
}

/// Where no worker steps the preparer, a caller waiting on what only it brings runs it (DESIGN.md
/// 22.7, 22.15 proof 12: a fresh segment needed at an admission with no worker and no later
/// caller): the admission that comes within a segment of the prepared end starts the next one,
/// and a wait for durability whose padding waits for that segment runs the preparer and waits on
/// the preparer's own request, never on the log alone, so each landing brings it back to run the
/// next step, until the segment is published and the record is durable. The IO is stepped one
/// request at a time and no other caller comes (mutant: the wait returning its registration
/// without running the preparer, which nothing then finishes).
#[test]
fn a_waiter_runs_the_preparer_where_no_worker_steps_it_and_waits_on_its_requests() {
    let config = small();
    let segment = config.segment_bytes;
    let filler = vec![7u8; 996];
    let resume = segment - 3 * BLOCK;
    for seed in 0..8 {
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let log = Log::open(io.clone(), BASE, config.clone(), 0).unwrap();
        while log.end() < resume {
            let mut admitted = drive(&io, || log.admit(&mut LogWait::default(), 1024)).unwrap();
            log.append(&mut admitted, &header(0), &[&filler]);
        }
        drive(&io, || log.wait_durable(&mut LogWait::default(), resume)).unwrap();
        // The next segment this log prepared goes, so that the log opened next prepares a fresh
        // one rather than making a spare of it.
        let next = log.segments.path(1);
        drop(log);
        io.remove_file(&next).unwrap();
        // Reopened there: only its first segment is prepared.
        let log = Log::open(io.clone(), BASE, config.clone(), resume).unwrap();
        let record = vec![1u8; 36];
        // Less than a segment is prepared ahead, so this admission starts preparing the next:
        // its zero fill is in flight until the IO is stepped.
        let mut admitted = drive(&io, || {
            log.admit(&mut LogWait::default(), framed_len(record.len()))
        })
        .unwrap();
        let lsn = log.append(&mut admitted, &header(1), &[&record]);
        drop(admitted);
        let started = log.preparation.preparing();
        let prepared = log.room.prepared_end();
        // Room granted to the prepared end, as an admission may be, leaves none to pad the
        // record's block with: the padding waits for the segment in flight.
        let rest = segment - 8 - log.room.reach();
        assert!(log.room.grant(rest, true));
        let held = Admitted {
            log: log.clone(),
            left: rest,
        };
        let mut wait = LogWait::default();
        let mut calls = 0;
        let mut on_requests = true;
        loop {
            calls += 1;
            assert!(calls < 10_000, "seed {seed}: never durable");
            let IOResult::IO(IOCompletions(c)) = log.wait_durable(&mut wait, lsn).unwrap() else {
                break;
            };
            // Only the IO's steps finish what the caller waits on: no other caller comes.
            while !c.finished() {
                if crash.in_flight() == 0 {
                    on_requests = false;
                    break;
                }
                io.step().unwrap();
            }
            if !on_requests {
                break;
            }
        }
        let durable = log.durable();
        drop(wait);
        drop(held);
        drop(log);
        assert!(started, "seed {seed}: the admission started no segment");
        assert_eq!(
            prepared, segment,
            "seed {seed}: the next segment prepared at once"
        );
        assert!(
            on_requests,
            "seed {seed}: a wait left with nothing in flight to finish it"
        );
        assert!(durable >= lsn, "seed {seed}");
        let calls = crash.file_work_in_callbacks();
        assert!(calls.is_empty(), "seed {seed}: callbacks made {calls:?}");
    }
}

/// A log opened three blocks before its first segment's end, nothing granted, less than a segment
/// prepared ahead: the preparer's next step prepares the next segment. With `fresh_next`, the
/// next segment the log before it prepared is gone, so it is created fresh, not made of a spare.
fn reopened_short(io: &Arc<dyn IO>, fresh_next: bool) -> Arc<Log> {
    let config = small();
    let segment = config.segment_bytes;
    let filler = vec![7u8; 996];
    let resume = segment - 3 * BLOCK;
    let log = Log::open(io.clone(), BASE, config.clone(), 0).unwrap();
    while log.end() < resume {
        let mut admitted = drive(io, || log.admit(&mut LogWait::default(), 1024)).unwrap();
        log.append(&mut admitted, &header(0), &[&filler]);
    }
    drive(io, || log.wait_durable(&mut LogWait::default(), resume)).unwrap();
    let next = log.segments.path(1);
    drop(log);
    if fresh_next {
        let _ = io.remove_file(&next);
    }
    Log::open(io.clone(), BASE, config, resume).unwrap()
}

/// A log opened three blocks before its first segment's end, with a record appended and room
/// granted past it straight from the room, as no admission does, so that padding the record's
/// block waits for the next segment while the preparer has not begun it: the state a flush meets
/// only if the preparer has not run since less than a segment was prepared ahead. Returns the
/// log, the record's LSN and the room still held.
fn padding_with_no_preparation(io: &Arc<dyn IO>, fresh_next: bool) -> (Arc<Log>, Lsn, Admitted) {
    let segment = small().segment_bytes;
    let log = reopened_short(io, fresh_next);
    let record = vec![1u8; 36];
    let len = framed_len(record.len());
    assert!(log.room.grant(len, true));
    let mut admitted = Admitted {
        log: log.clone(),
        left: len,
    };
    let lsn = log.append(&mut admitted, &header(1), &[&record]);
    let rest = segment - 8 - log.room.reach();
    assert!(log.room.grant(rest, true));
    assert!(
        !log.preparation.needed() && !log.preparation.preparing(),
        "a preparation already under way"
    );
    let held = Admitted {
        log: log.clone(),
        left: rest,
    };
    (log, lsn, held)
}

/// A hook that fails `log` as the synchronous file call `at` begins: another request failing
/// while that call runs.
fn failing_at(log: &Arc<Log>, at: &'static str) -> Option<Arc<dyn Fn(&'static str) + Send + Sync>> {
    let log = Arc::downgrade(log);
    Some(Arc::new(move |what| {
        if what == at {
            if let Some(log) = log.upgrade() {
                log.fail(CompletionError::Aborted);
            }
        }
    }))
}

/// Another request's failure published while the preparer's create runs: it reads the failure as
/// the create returns, allocating nothing and submitting nothing, and its task ends; every waiter
/// gets the error.
#[test]
fn a_failure_during_a_starters_create_stops_the_preparation() {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let (log, lsn, held) = padding_with_no_preparation(&io, true);
    log.wanted.fetch_max(lsn, Ordering::SeqCst);
    let durable = log
        .durable_waits
        .wait(lsn, || log.durable(), || super::waits::Look::Stay)
        .expect("not durable yet")
        .wait
        .clone();
    let in_flight = crash.in_flight();
    crash.on_file_work(failing_at(&log, "open"));
    let _ = log.run_preparer();
    crash.on_file_work(None);
    let created = io
        .open_file(&log.segments.path(1), OpenFlags::None, false)
        .expect("the starter's create ran");
    assert_eq!(created.size().unwrap(), 0, "allocated after the failure");
    assert_eq!(crash.in_flight(), in_flight, "submitted after the failure");
    assert!(
        log.preparer().is_over(),
        "the preparer went on after the failure"
    );
    assert_eq!(durable.get_error(), Some(CompletionError::Aborted));
    drop(held);
}

/// Another request's failure published while the preparer's directory sync runs: it reads the
/// failure as the sync returns, publishing no prepared end, and its task ends; every waiter gets
/// the error.
#[test]
fn a_failure_during_a_finishers_directory_sync_publishes_nothing() {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    // The next segment from a spare: the preparer's start goes straight on to finish it.
    let (log, lsn, held) = padding_with_no_preparation(&io, false);
    log.wanted.fetch_max(lsn, Ordering::SeqCst);
    let durable = log
        .durable_waits
        .wait(lsn, || log.durable(), || super::waits::Look::Stay)
        .expect("not durable yet")
        .wait
        .clone();
    let prepared = log.room.prepared_end();
    crash.on_file_work(failing_at(&log, "directory sync"));
    let _ = log.run_preparer();
    crash.on_file_work(None);
    assert_eq!(
        log.room.prepared_end(),
        prepared,
        "published after the failure"
    );
    assert!(
        log.preparer().is_over(),
        "the preparer went on after the failure"
    );
    assert_eq!(durable.get_error(), Some(CompletionError::Aborted));
    drop(held);
}

/// An admission whose room is granted at once, and whose run of the preparer, no worker stepping
/// it, meets a failure, returns the failure, never the room, and gives the room back.
#[test]
fn an_admission_whose_step_fails_the_log_returns_the_failure_and_gives_its_room_back() {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let log = reopened_short(&io, true);
    assert!(log.short_of_segments(), "no preparation would start");
    let reach = log.room.reach();
    crash.on_file_work(failing_at(&log, "open"));
    let admitted = log.admit(&mut LogWait::default(), 64);
    crash.on_file_work(None);
    match admitted {
        Err(LimboError::CompletionError(CompletionError::Aborted)) => {}
        Ok(IOResult::Done(_)) => panic!("room handed over from a failed log"),
        other => panic!("not the failure: {:?}", other.map(|_| ())),
    }
    assert_eq!(log.room.reach(), reach, "the room granted not given back");
}

/// A flush that meets padding waiting for a segment nobody prepares sets the word needed and makes
/// the preparer due (DESIGN.md 22.9 item 2), finishing no waiter: waiters are woken by their marks
/// and by room. A caller's wait then runs the preparer, and the record becomes durable (mutant:
/// the word set and the preparer left at its own level).
#[test]
fn a_flush_meeting_padding_nobody_prepares_makes_the_preparer_due_and_wakes_no_waiter() {
    use crate::storage::atoll::executor::Class;
    for seed in 0..4 {
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let (log, lsn, held) = padding_with_no_preparation(&io, false);
        log.wanted.fetch_max(lsn, Ordering::SeqCst);
        let durable = log
            .durable_waits
            .wait(lsn, || log.durable(), || super::waits::Look::Stay)
            .expect("not durable yet")
            .wait
            .clone();
        let admission = log.admitters.wait();
        let before = log.preparer().class();
        // What a write's callback runs once it lands.
        log.flush();
        let needed = log.preparation.needed();
        let after = log.preparer().class();
        let woken = (durable.finished(), admission.finished());
        drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
        let reached = log.durable() >= lsn;
        drop(held);
        drop(log);
        assert_eq!(
            before,
            Class::Maintenance,
            "seed {seed}: due before any flush"
        );
        assert!(needed, "seed {seed}: the padding needed no segment");
        assert_eq!(after, Class::Due, "seed {seed}: the preparer not made due");
        assert_eq!(
            woken,
            (false, false),
            "seed {seed}: a waiter woken by the flush"
        );
        assert!(reached, "seed {seed}");
        let calls = crash.file_work_in_callbacks();
        assert!(calls.is_empty(), "seed {seed}: callbacks made {calls:?}");
    }
}

/// A completion from `wait_durable` means only call again: a wait whose padding waits for a
/// segment, the caller running the preparer, is told to call again on the preparer's request,
/// never that the log is durable, while that request is still in flight.
#[test]
fn a_finished_completion_is_no_report_of_durability() {
    for seed in 0..4 {
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let (log, lsn, held) = padding_with_no_preparation(&io, false);
        let first = log.wait_durable(&mut LogWait::default(), lsn).unwrap();
        let durable_then = log.durable();
        drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
        let reached = log.durable() >= lsn;
        drop(held);
        drop(log);
        assert!(
            matches!(first, IOResult::IO(_)),
            "seed {seed}: reported durable to {lsn} with the log durable to {durable_then}"
        );
        assert!(reached, "seed {seed}");
    }
}

/// Every call of [`Log::admit`] and [`Log::wait_durable`] in Atoll outside its tests, the
/// facade's included: its file, the function it is in, the call, how it calls again once the
/// completion it got finishes without an error, and what it does with an error (DESIGN.md 22.5).
/// A completion from either means only: call again.
#[cfg(not(any(shuttle, atoll_loom)))]
const CALLERS: &[(&str, &str, &str, &str, &str)] = &[
    (
        "stream.rs",
        "append",
        "admit",
        "returns the IO; its caller steps the append again",
        "returns it",
    ),
    (
        "stream.rs",
        "append",
        "wait_durable",
        "returns the IO; its caller steps the append again",
        "returns it",
    ),
    (
        "checkpoint.rs",
        "step",
        "admit",
        "unit!: the unit waits on it; whoever steps the checkpoint runs the unit again",
        "returns it: the checkpoint fails",
    ),
    (
        "checkpoint.rs",
        "durable_turn",
        "wait_durable_turn",
        "the unit waits on it, or, a turn owed, takes that turn first; whoever steps the \
         checkpoint runs the unit again (the intent record, the log's end)",
        "returns it: the checkpoint fails",
    ),
    (
        "txn/rollback.rs",
        "walk_on",
        "wait_durable",
        "done!: returns the IO; the rollback's stepper steps it again",
        "returns it: the rollback fails",
    ),
    (
        "txn/mod.rs",
        "spill",
        "admit",
        "returns the IO; the statement steps the spill again",
        "returns it",
    ),
    (
        "txn/mod.rs",
        "publish_alone",
        "admit",
        "returns the IO; its caller steps it again until done",
        "returns it",
    ),
    (
        "txn/mod.rs",
        "end_commit",
        "admit",
        "returns the IO; the commit is stepped again",
        "returns it: the commit fails before its transaction ended, and rolls back",
    ),
    (
        "txn/mod.rs",
        "commit",
        "wait_durable",
        "returns the IO; the commit is stepped again",
        "returns it: the commit fails",
    ),
    (
        "sql/mod.rs",
        "drop",
        "wait_durable",
        "drive: called again until done",
        "logged: the database is closing",
    ),
    (
        "db.rs",
        "close_in_order",
        "wait_durable",
        "drive: called again until done",
        "close goes on through its later steps and returns it after the last",
    ),
];

#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn every_caller_of_admit_and_wait_durable_calls_again() {
    use crate::storage::atoll::testing::{atoll_dir, atoll_sources, code_of};
    let dir = atoll_dir();
    let mut found = Vec::new();
    for path in atoll_sources() {
        let file = path.strip_prefix(&dir).unwrap().display().to_string();
        let test_only = file.ends_with("tests.rs")
            || file.contains("/tests/")
            || ["bench.rs", "testing.rs"].contains(&file.as_str());
        if test_only {
            continue;
        }
        let code = code_of(&std::fs::read_to_string(&path).unwrap());
        let code = code.split("mod tests {").next().unwrap();
        for call in ["admit", "wait_durable", "wait_durable_turn"] {
            for (at, _) in code.match_indices(&format!(".{call}(")) {
                let caller: String = code[..at]
                    .rfind("fn ")
                    .map(|i| {
                        code[i + 3..]
                            .chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect()
                    })
                    .unwrap_or_default();
                found.push((file.clone(), caller, call.to_string()));
            }
        }
    }
    let mut listed: Vec<(String, String, String)> = CALLERS
        .iter()
        .map(|(f, c, call, _, _)| (f.to_string(), c.to_string(), call.to_string()))
        .collect();
    found.sort();
    listed.sort();
    // The scan reads what it must: a commit's admission is one call it finds.
    assert!(
        found
            .iter()
            .any(|(f, c, call)| f == "txn/mod.rs" && c == "end_commit" && call == "admit"),
        "the census found {found:?}"
    );
    assert_eq!(
        found, listed,
        "a call of admit or wait_durable not listed with how it calls again, or one listed that \
         is gone"
    );
}

/// Once the log has failed no preparation step begins work: the preparer reads the failure before
/// it takes the word, creates no segment, and its task ends.
#[test]
fn a_failed_log_begins_no_preparation_step() {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let (log, _, held) = padding_with_no_preparation(&io, true);
    assert!(log.short_of_segments(), "no preparation would start");
    log.fail(CompletionError::Aborted);
    let ran = log.run_preparer();
    let next = log.segments.path(1);
    let created = io.open_file(&next, OpenFlags::None, false).is_ok();
    let over = log.preparer().is_over();
    drop(held);
    drop(log);
    assert!(ran.is_none(), "a failed log's preparer waits on a request");
    assert!(!created, "a failed log created its next segment");
    assert!(over, "a failed log's preparer goes on");
}

/// A unit's wait for durability takes one turn of the flush, and a request made during that turn
/// is owed to the unit's next, whose turn answers it (DESIGN.md 22.1; mutant: the flush's loop
/// over turns run inside the unit).
#[test]
fn a_units_durable_wait_takes_one_flush_turn_and_owes_one_asked_during_it() {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
    let p = payload(0, 0);
    let mut admitted = drive(&io, || {
        log.admit(&mut LogWait::default(), framed_len(p.len()))
    })
    .unwrap();
    let lsn = log.append(&mut admitted, &header(0), &[&p]);
    drop(admitted);
    TURNS.with(|n| n.set(0));
    ASK_DURING_TURN.with(|a| a.set(true));
    let (waited, owed) = log.wait_durable_turn(&mut LogWait::default(), lsn).unwrap();
    let turns = TURNS.with(|n| n.get());
    let waits = matches!(waited, IOResult::IO(_));
    // The next unit answers it, with one turn of its own, and owes nothing more.
    let owed_again = log.flush_one_turn();
    let turns_after = TURNS.with(|n| n.get());
    drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
    // Asserted once the log is closed: a panic through it ends the process.
    drop(log);
    assert_eq!(turns, 1, "turns the unit took");
    assert!(owed, "the request made during the turn was not owed");
    assert!(waits, "durable with its write unstepped");
    assert!(!owed_again, "a turn owed with no request during it");
    assert_eq!(turns_after, 2);
}

/// What [`HoldingIO`] holds: once armed, every write and sync to the one segment it holds is
/// accepted and kept, its callback not run, until the test answers it and then lets it go, so the
/// preparer's place is seen held past the answer, until the request retires.
#[derive(Default)]
struct Holding {
    path: crate::storage::atoll::locks::Mutex<Option<String>>,
    queue: crate::storage::atoll::locks::Mutex<std::collections::VecDeque<(Completion, i32)>>,
    submitted: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
}

impl Holding {
    fn hold(&self, c: Completion, result: i32) -> Result<Completion> {
        c.accept();
        let mut queue = self.queue.lock();
        queue.push_back((c.clone(), result));
        self.submitted.fetch_add(1, Ordering::Relaxed);
        self.most.fetch_max(queue.len(), Ordering::Relaxed);
        Ok(c)
    }

    /// The oldest request held, taken out: the test answers it, then lets it go.
    fn oldest(&self) -> Option<(Completion, i32)> {
        self.queue.lock().pop_front()
    }
}

/// A [`MemoryIO`] whose one held segment, once named, holds its writes and syncs ([`Holding`]).
struct HoldingIO {
    inner: Arc<MemoryIO>,
    holding: Arc<Holding>,
}

impl crate::io::Clock for HoldingIO {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for HoldingIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let held = self.holding.path.lock().as_deref() == Some(path);
        Ok(Arc::new(HoldingFile {
            file: self.inner.open_file(path, flags, direct)?,
            holding: held.then(|| self.holding.clone()),
        }))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn sync_directory(&self, path: &str) -> Result<()> {
        self.inner.sync_directory(path)
    }
    fn rename_file(&self, from: &str, to: &str) -> Result<bool> {
        self.inner.rename_file(from, to)
    }
    fn delivery(&self) -> crate::io::Delivery {
        self.inner.delivery()
    }
    fn step(&self) -> Result<()> {
        self.inner.step()
    }
    fn try_step(
        &self,
        key: crate::io::ReadyKey,
        ready: &std::task::Waker,
    ) -> Result<crate::io::Stepped> {
        self.inner.try_step(key, ready)
    }
    fn says_what_a_step_found(&self) -> bool {
        self.inner.says_what_a_step_found()
    }
    fn withdraw(&self, key: crate::io::ReadyKey) {
        self.inner.withdraw(key)
    }
}

struct HoldingFile {
    file: Arc<dyn File>,
    holding: Option<Arc<Holding>>,
}

impl File for HoldingFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }
    fn pread(&self, at: u64, c: Completion) -> Result<Completion> {
        self.file.pread(at, c)
    }
    fn pwrite(&self, at: u64, bytes: Arc<crate::io::Buffer>, c: Completion) -> Result<Completion> {
        match &self.holding {
            Some(holding) => {
                c.keep_write_buffer_alive(bytes.clone());
                holding.hold(c, bytes.len() as i32)
            }
            None => self.file.pwrite(at, bytes, c),
        }
    }
    fn sync(&self, c: Completion, how: crate::io::FileSyncType) -> Result<Completion> {
        match &self.holding {
            Some(holding) => holding.hold(c, 0),
            None => self.file.sync(c, how),
        }
    }
    fn size(&self) -> Result<u64> {
        self.file.size()
    }
    fn truncate(&self, n: u64, c: Completion) -> Result<Completion> {
        self.file.truncate(n, c)
    }
}

/// The preparer's one request in flight holds its place until the request retires, not until its
/// result is published (the auditor's #4345, DESIGN.md 22.5): a zero-fill write answered and
/// still owned leaves the preparer, stepped again, waiting with nothing more submitted; let go,
/// the next write goes, one at a time to the segment's data sync; and a sync answered with no
/// success, then let go, fails the log and ends the preparer (mutant: the place let go at the
/// answer, a second write submitted while the first is owned).
#[test]
fn the_preparers_request_holds_its_place_until_it_retires() {
    let holding = Arc::new(Holding::default());
    let io: Arc<dyn IO> = Arc::new(HoldingIO {
        inner: Arc::new(MemoryIO::new()),
        holding: holding.clone(),
    });
    let log = reopened_short(&io, true);
    *holding.path.lock() = Some(log.segments.path(1));
    let mut held_past_answer = 0;
    let mut stepped_while_owned = 0;
    let mut failed = None;
    for _ in 0..10_000 {
        if log.preparer().is_over() {
            break;
        }
        let Some(wait) = log.run_preparer() else {
            break;
        };
        let Some((request, result)) = holding.oldest() else {
            continue;
        };
        let is_sync = result == 0;
        if is_sync {
            request.error(CompletionError::Aborted);
        } else {
            request.complete(result);
        }
        // Answered and still owned: the place is held, and a step meanwhile submits nothing.
        if !wait.finished() {
            held_past_answer += 1;
        }
        let before = holding.submitted.load(Ordering::Relaxed);
        let _ = log.run_preparer();
        if holding.submitted.load(Ordering::Relaxed) == before {
            stepped_while_owned += 1;
        }
        request.retire();
        if is_sync {
            failed = log.failure();
        }
    }
    let over = log.preparer().is_over();
    let submitted = holding.submitted.load(Ordering::Relaxed);
    let most = holding.most();
    drop(log);
    let writes = (small().segment_bytes / small().chunk_bytes) as usize;
    assert_eq!(
        most, 1,
        "more than one of the preparer's requests owned at once"
    );
    assert_eq!(submitted, writes + 1, "the zero fill's writes and its sync");
    assert_eq!(held_past_answer, submitted, "a place let go at the answer");
    assert_eq!(
        stepped_while_owned, submitted,
        "a request submitted while one was owned"
    );
    assert_eq!(
        failed,
        Some(CompletionError::Aborted),
        "the failed sync told no one"
    );
    assert!(over, "the preparer went on after the log failed");
}

impl Holding {
    fn most(&self) -> usize {
        self.most.load(Ordering::Relaxed)
    }
}

/// A log opened alone owns an executor with no worker, whose one task is its preparer, stepped
/// only by callers of the log's own calls, each holding the log: so the log is never dropped
/// inside a step of its preparer, and dropped, it closes an executor with no step running
/// (DESIGN.md 22.12, the auditor's #4345). Here the log is dropped while its preparer waits on a
/// zero-fill write in flight; the write then lands for a log gone, and nothing is left holding
/// the IO's requests.
#[test]
fn a_log_dropped_while_its_preparer_waits_on_a_request_leaves_nothing_behind() {
    for seed in 0..4 {
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let log = reopened_short(&io, true);
        // A step a call: the fresh segment's create and allocation, then its first write.
        let mut waiting = log.run_preparer();
        for _ in 0..2 {
            if crash.in_flight() > 0 {
                break;
            }
            waiting = log.run_preparer();
        }
        let in_flight = crash.in_flight();
        let on_request = waiting.is_some_and(|c| !c.finished());
        let weak = Arc::downgrade(&log);
        drop(log);
        let gone = weak.upgrade().is_none();
        while crash.in_flight() > 0 {
            io.step().unwrap();
        }
        let calls = crash.file_work_in_callbacks();
        assert!(on_request, "seed {seed}: the preparer waits on no request");
        assert!(in_flight > 0, "seed {seed}: nothing in flight");
        assert!(gone, "seed {seed}: the log outlived its last handle");
        assert!(calls.is_empty(), "seed {seed}: callbacks made {calls:?}");
    }
}

/// An admission whose room is granted makes no claim of the preparer while it waits on its
/// request or rests, so its fast path takes no executor mutex (the auditor's #4345): only one
/// that finds the preparer queued runs it, where no worker steps it. Here the first admissions
/// within a segment of the prepared end make the preparer due and run it, a step each, to its
/// first write; while that write is in flight a hundred more admissions are granted with no claim;
/// once it lands, the next admission claims it again (mutant: a claim at every admission within a
/// segment of the prepared end).
#[test]
fn an_admission_granted_claims_the_preparer_only_when_it_is_queued() {
    use super::preparer::CLAIMS;
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let log = reopened_short(&io, true);
    CLAIMS.with(|n| n.set(0));
    // A step an admission: the fresh segment's create and allocation, then its first write.
    for _ in 0..2 {
        let mut admitted = drive(&io, || log.admit(&mut LogWait::default(), 64)).unwrap();
        log.append(&mut admitted, &header(0), &[&[0u8; 36][..]]);
        drop(admitted);
        if crash.in_flight() > 0 {
            break;
        }
    }
    let first = CLAIMS.with(|n| n.replace(0));
    let in_flight = crash.in_flight();
    for _ in 0..100 {
        let IOResult::Done(admitted) = log.admit(&mut LogWait::default(), 64).unwrap() else {
            break;
        };
        drop(admitted);
    }
    let meanwhile = CLAIMS.with(|n| n.replace(0));
    while crash.in_flight() > 0 {
        io.step().unwrap();
    }
    let IOResult::Done(admitted) = log.admit(&mut LogWait::default(), 64).unwrap() else {
        panic!("an admission with room waited");
    };
    drop(admitted);
    let after = CLAIMS.with(|n| n.replace(0));
    drop(log);
    assert!(first > 0, "the first admission ran no preparer");
    assert!(in_flight > 0, "no request of the preparer in flight");
    assert_eq!(
        meanwhile, 0,
        "admissions claimed the preparer waiting on its request"
    );
    assert!(after > 0, "the preparer queued again was not claimed");
}

/// Every step of the preparer is a unit of a job's work (DESIGN.md 22.1, the auditor's #4356):
/// what runs inside it runs inside a unit, where every park is refused (mutant: the step outside
/// a unit).
#[test]
fn the_preparers_step_is_a_unit_of_a_jobs_work() {
    use super::preparer::DURING_UNIT;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let log = reopened_short(&io, true);
    let inside = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let inside = inside.clone();
        DURING_UNIT.with(|d| {
            *d.borrow_mut() = Some(Box::new(move || {
                inside.store(
                    crate::storage::atoll::unit::inside(),
                    std::sync::atomic::Ordering::SeqCst,
                )
            }))
        });
    }
    let _ = log.run_preparer();
    let ran = DURING_UNIT.with(|d| d.borrow().is_none());
    drop(log);
    assert!(ran, "no step of the preparer ran");
    assert!(
        inside.load(std::sync::atomic::Ordering::SeqCst),
        "the preparer's step ran outside a unit"
    );
}

/// The preparer's finish takes one turn of the log's flush, and a turn asked for during it is
/// owed to the preparer's next unit, which takes it first (the auditor's #4356; mutant: the
/// finish flushing turns until none is asked for).
#[test]
fn the_preparers_finish_takes_one_flush_turn_and_owes_one_asked_during_it() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    // The next segment from a spare: the preparer's first step renames it and finishes it.
    let log = reopened_short(&io, false);
    let before = log.prepared_ahead().0;
    TURNS.with(|n| n.set(0));
    ASK_DURING_TURN.with(|a| a.set(true));
    let _ = log.run_preparer();
    let finishing = TURNS.with(|n| n.replace(0));
    let published = log.prepared_ahead().0 > before;
    let _ = log.run_preparer();
    let owed = TURNS.with(|n| n.replace(0));
    drop(log);
    assert!(published, "the first step published no segment");
    assert_eq!(finishing, 1, "turns the finish took");
    assert_eq!(owed, 1, "turns the next unit took for the one owed");
}

/// Durability waits made and given up while the log's durable position stands still never leave
/// the waits' count of entries above one more than twice the most waits that stood (the
/// auditor's #4356 and #4360): a wait given up is counted, and a registration made while those
/// outnumber the rest takes the stack first. Once the last waiter is gone, the entries left are
/// all given up, still within that bound, and the raise once the IO paused resumes takes every
/// one (mutant: a wait given up left uncounted, every one kept until the position moves).
#[test]
fn waits_given_up_while_the_log_stands_still_never_outnumber_twice_the_most_that_stood() {
    const STANDING: u64 = 4;
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
    let p = payload(0, 0);
    let mut admitted = drive(&io, || {
        log.admit(&mut LogWait::default(), framed_len(p.len()))
    })
    .unwrap();
    let lsn = log.append(&mut admitted, &header(0), &[&p]);
    drop(admitted);
    // Nothing stepped from here on: the IO paused, the log stays short of `lsn`.
    let mut standing = Vec::new();
    for _ in 0..STANDING {
        let mut wait = LogWait::default();
        let waits = matches!(log.wait_durable(&mut wait, lsn), Ok(IOResult::IO(_)));
        standing.push((wait, waits));
    }
    let mut most = 0;
    for _ in 0..1000 {
        let mut wait = LogWait::default();
        let _ = log.wait_durable(&mut wait, lsn);
        drop(wait);
        most = most.max(log.durable_waits.counts().0);
    }
    let waited = standing.iter().all(|(_, waits)| *waits);
    drop(standing);
    let left = log.durable_waits.counts();
    while crash.in_flight() > 0 {
        io.step().unwrap();
    }
    drive(&io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
    let after = log.durable_waits.counts();
    drop(log);
    assert!(waited, "a standing wait found the log durable");
    assert!(
        most <= 2 * STANDING + 1,
        "{most} entries held for {STANDING} waits that stand"
    );
    assert!(
        left.0 <= 2 * STANDING + 1 && left.1 == left.0,
        "(entries, given up) {left:?} once every waiter was gone"
    );
    assert_eq!(
        after,
        (0, 0),
        "(entries, given up) once the log was durable"
    );
}

/// The auditor's configuration for P1-LOG-ADMIT1: a buffer four segments long.
fn wide() -> LogConfig {
    LogConfig {
        segment_bytes: 64 << 10,
        buffer_bytes: 256 << 10,
        chunk_bytes: 8 << 10,
        writes_in_flight: 3,
        direct: true,
    }
}

/// How an admission's calls are delivered: the backend's requests landing within their
/// submission, or held until stepped; the preparer stepped by its callers, or by a worker.
#[derive(Clone, Copy, Debug)]
struct Delivery {
    pumped: bool,
    worker: bool,
}

/// One admission of `bytes` on a wide log, empty or, `near`, filled to three blocks short of its
/// first segment's end and durable there: whether it was admitted, or stranded on a wait nothing
/// can finish (in a caller's run, a wait left unfinished with no request in flight), or failed.
fn admit_wide(delivery: Delivery, near: bool, bytes: u64) -> std::result::Result<bool, String> {
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = if delivery.pumped {
        crash.clone()
    } else {
        Arc::new(MemoryIO::new())
    };
    let executor = delivery
        .worker
        .then(|| Executor::new(io.clone(), 1).unwrap());
    let log = match &executor {
        Some(ex) => Log::open_on(io.clone(), BASE, wide(), 0, ex).unwrap(),
        None => Log::open(io.clone(), BASE, wide(), 0).unwrap(),
    };
    if near {
        let filler = vec![7u8; 996];
        let to = wide().segment_bytes - 3 * BLOCK;
        while log.end() < to {
            let mut admitted = drive(&io, || log.admit(&mut LogWait::default(), 1024)).unwrap();
            log.append(&mut admitted, &header(0), &[&filler]);
        }
        let end = log.end();
        drive(&io, || log.wait_durable(&mut LogWait::default(), end)).unwrap();
    }
    let mut wait = LogWait::default();
    let got = if delivery.worker {
        // A worker prepares: the caller waits, a wait nothing finishes hanging the test.
        drive(&io, || log.admit(&mut wait, bytes))
            .map(|a| a.left == bytes)
            .map_err(|e| e.to_string())
    } else {
        let mut got = Err("never admitted".to_string());
        for _ in 0..10_000 {
            match log.admit(&mut wait, bytes) {
                Ok(IOResult::Done(a)) => {
                    got = Ok(a.left == bytes);
                    break;
                }
                Ok(IOResult::IO(IOCompletions(c))) => {
                    while !c.finished() && crash.in_flight() > 0 {
                        io.step().unwrap();
                    }
                    if !c.finished() {
                        got = Err("stranded on a wait nothing can finish".to_string());
                        break;
                    }
                }
                Err(e) => {
                    got = Err(e.to_string());
                    break;
                }
            }
        }
        got
    };
    drop(wait);
    drop(log);
    drop(executor);
    got
}

/// An admission larger than what is prepared ahead, and within the log's buffer, is admitted: it
/// has the preparer prepare to the end it needs, however many segments past the prepared end
/// that is (the auditor's P1-LOG-ADMIT1: 192 KiB on an empty log of 64 KiB segments). Sizes past
/// one segment, three, and the buffer whole; from an empty log and from three blocks short of a
/// segment's end; requests landing inline and held until stepped; the preparer stepped by the
/// caller and by a worker (mutant: the preparer's want read only as less than a segment prepared
/// ahead, which strands the caller's). The caller's cases run first: a caller's strand is
/// reported, where a worker's would leave the test waiting.
#[test]
fn an_admission_past_what_is_prepared_ahead_is_prepared_for() {
    let segment = wide().segment_bytes;
    let mut failures = Vec::new();
    let mut cases = 0;
    for worker in [false, true] {
        for pumped in [false, true] {
            let delivery = Delivery { pumped, worker };
            for near in [false, true] {
                for bytes in [segment + BLOCK, 3 * segment, wide().buffer_bytes] {
                    cases += 1;
                    match admit_wide(delivery, near, bytes) {
                        Ok(true) => {}
                        got => failures.push(format!("{delivery:?} near {near} {bytes}: {got:?}")),
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    }
    assert_eq!(cases, 24, "cases run");
}

/// An admission granted runs a step of the preparer it finds queued and returns, what the step
/// left to do not waited on: the next admission runs it on, so admissions granted carry a fresh
/// segment's preparation through to its publication with no admission refused and no caller
/// waiting, nothing pumping the backend between them where its requests land inline, only its
/// delivery where they land later (the auditor's #4360; mutant: a granted admission running no
/// step, which leaves the preparation to the first admission refused).
#[test]
fn admissions_granted_carry_a_preparation_through_to_its_publication() {
    for pumped in [false, true] {
        let crash = Arc::new(CrashIO::new(0));
        let io: Arc<dyn IO> = if pumped {
            crash.clone()
        } else {
            Arc::new(MemoryIO::new())
        };
        let log = reopened_short(&io, true);
        let before = log.room.prepared_end();
        let filler = [5u8; 36];
        super::preparer::GRANTED_MORE.with(|n| n.set(0));
        let (mut granted, mut refused, mut published) = (0u64, 0u64, false);
        for _ in 0..1000 {
            match log
                .admit(&mut LogWait::default(), framed_len(filler.len()))
                .unwrap()
            {
                IOResult::Done(mut admitted) => {
                    granted += 1;
                    log.append(&mut admitted, &header(0), &[&filler]);
                }
                IOResult::IO(_) => refused += 1,
            }
            if log.room.prepared_end() > before {
                published = true;
                break;
            }
            while crash.in_flight() > 0 {
                io.step().unwrap();
            }
        }
        let more = super::preparer::GRANTED_MORE.with(|n| n.replace(0));
        drop(log);
        assert_eq!(refused, 0, "pumped {pumped}: admissions refused");
        assert!(
            published,
            "pumped {pumped}: {granted} granted, no segment published"
        );
        assert!(
            more > 0,
            "pumped {pumped}: no granted admission's step left more to do"
        );
    }
}
