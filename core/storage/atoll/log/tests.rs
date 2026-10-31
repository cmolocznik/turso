use super::*;
use crate::io::crash::{CrashIO, Faults};
use crate::io::{Completion, MemoryIO, OpenFlags, IO};
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
        prev: 0,
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

#[test]
fn records_appended_from_many_threads_read_back_once_each_in_each_threads_order() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
    let (writers, each) = (8u64, 300u64);
    std::thread::scope(|s| {
        for writer in 0..writers {
            let (io, log) = (io.clone(), log.clone());
            s.spawn(move || {
                for seq in 0..each {
                    let p = payload(writer, seq);
                    let mut admitted = drive(&io, || log.admit(framed_len(p.len()))).unwrap();
                    let lsn = log.append(&mut admitted, &header(writer), &[&p]);
                    if seq % 17 == 0 {
                        drive(&io, || log.wait_durable(lsn)).unwrap();
                    }
                }
            });
        }
    });
    drive(&io, || log.wait_durable(log.end())).unwrap();
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
                admitted = Some(match log.admit(framed_len(p.len()))? {
                    IOResult::Done(a) => a,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                });
                Ok(IOResult::Done(()))
            });
            let lsn = log.append(admitted.as_mut().unwrap(), &header(0), &[&p]);
            appended.set(seq + 1);
            // Waits at varying distances, so flushes run from part of a block to several.
            if (seq * 7 + seed) % 23 == 0 {
                run(&mut || log.wait_durable(lsn));
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
            let mut admitted = drive(&crashed_io, || reopened.admit(framed_len(p.len()))).unwrap();
            let lsn = reopened.append(&mut admitted, &header(1), &[&p]);
            drive(&crashed_io, || reopened.wait_durable(lsn)).unwrap();
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
        let mut admitted = drive(&io, || log.admit(framed_len(p.len()))).unwrap();
        let lsn = log.append(&mut admitted, &header(0), &[&p]);
        // Waiting on every record pads a partial block each time.
        drive(&io, || log.wait_durable(lsn)).unwrap();
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

#[test]
fn a_failed_write_fails_its_waiters_and_every_later_operation() {
    let crash_io = Arc::new(CrashIO::new(3));
    let io: Arc<dyn IO> = crash_io.clone();
    let log = Log::open(io.clone(), BASE, small(), 0).unwrap();
    let p = payload(0, 0);
    let mut admitted = drive(&io, || log.admit(framed_len(p.len()))).unwrap();
    let lsn = log.append(&mut admitted, &header(0), &[&p]);
    drop(admitted);
    crash_io.set_faults(Faults {
        write: 1.0,
        ..Faults::default()
    });
    assert!(drive(&io, || log.wait_durable(lsn)).is_err());
    assert!(log.failure().is_some());
    assert!(log.admit(64).is_err());
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
            let mut admitted = drive(&io, || log.admit(1024)).unwrap();
            log.append(&mut admitted, &header(0), &[&filler]);
        }
        drive(&io, || log.wait_durable(resume)).unwrap();
        drop(log);
        // Reopened there, as after a crash: the next segment is prepared only once asked for.
        let log = Log::open(io.clone(), BASE, config.clone(), resume).unwrap();
        let first = vec![1u8; 36];
        let mut admitted_first = drive(&io, || log.admit(framed_len(first.len()))).unwrap();
        // The second fits before the segment's end from where the log stands now.
        let second = vec![2u8; (segment - resume - 64 - 64) as usize - 28];
        let second_len = framed_len(second.len());
        assert!(resume + framed_len(first.len()) + second_len <= segment);
        let mut admitted_second = drive(&io, || log.admit(second_len)).unwrap();
        let lsn = log.append(&mut admitted_first, &header(1), &[&first]);
        // Waiting on the first pads its block, moving the second past the segment's end.
        drive(&io, || log.wait_durable(lsn)).unwrap();
        let lsn = log.append(&mut admitted_second, &header(2), &[&second]);
        drive(&io, || log.wait_durable(lsn)).unwrap();
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
                admitted = Some(match log.admit(len)? {
                    IOResult::Done(a) => a,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                });
                Ok(IOResult::Done(()))
            });
            let lsn = append_unit(&log, admitted.as_mut().unwrap(), &records);
            appended.set(seq + 1);
            if (seq * 5 + seed) % 11 == 0 {
                run(&mut || log.wait_durable(lsn));
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
        let mut admitted = drive(&io, || log.admit(2 * BLOCK)).unwrap();
        log.append(&mut admitted, &header(0), &[&first]);
        let (start, lsns) = log.append_group(
            &mut admitted,
            &[(header(0), &[g0.as_slice()]), (header(0), &[g1.as_slice()])],
        );
        assert_eq!((start, lsns[0]), (framed_len(first.len()), BLOCK));
        drive(&io, || log.wait_durable(lsns[1])).unwrap();
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
        let mut admitted = drive(&io, || log.admit(BLOCK)).unwrap();
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
        drive(&io, || log.wait_durable(lsn)).unwrap();
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
