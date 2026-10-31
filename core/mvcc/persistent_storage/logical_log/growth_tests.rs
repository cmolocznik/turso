//! A log that grows in steps (`PRAGMA mvcc_log_growth`) writes its frames within its file's size
//! and restarts in place, and every frame written before the end of its file is followed by an
//! `END_GUARD`, so the bytes after it are never read as frames.

use super::{
    LogicalLog, RecoveredLog, RecoveredSegment, StreamingLogicalLogReader, END_GUARD,
    MAX_LOG_GROWTH, TX_HEADER_SIZE,
};
use crate::io::{MemoryIO, IO};
use crate::mvcc::database::tests::generate_simple_string_row;
use crate::mvcc::database::{LogRecord, PackedTs, RowVersion, TxTimestampOrID, WalPos};
use crate::mvcc::persistent_storage::LogicalLogTruncateOutcome;
use crate::sync::Arc;
use crate::util::IOExt as _;
use crate::{Buffer, Completion, File, OpenFlags};

fn open() -> (Arc<dyn IO>, Arc<dyn File>, LogicalLog) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let file = io
        .open_file("growth.db-log", OpenFlags::Create, false)
        .unwrap();
    let log = LogicalLog::new(file.clone(), io.clone(), None);
    (io, file, log)
}

/// A commit at `ts` of row `id` holding `len` bytes.
fn record(ts: u64, id: i64, len: usize) -> LogRecord {
    let row = generate_simple_string_row((-2).into(), id, &"x".repeat(len));
    let mut tx = LogRecord::new(ts, crate::alloc::DynAllocator::default()).unwrap();
    tx.push_row_version_for_test(
        &row.id,
        &RowVersion {
            id: id as u64,
            begin: PackedTs::pack(Some(TxTimestampOrID::Timestamp(ts))),
            end: PackedTs::pack(None),
            data: row.data,
            btree_resident: false,
            materialized_at: WalPos::ORIGIN,
        },
    );
    tx
}

/// Appends `tx` as a commit does: written, then the offset advanced once the write is done.
fn append(io: &Arc<dyn IO>, log: &mut LogicalLog, tx: LogRecord) {
    let (c, bytes) = log.log_tx_deferred_offset(tx, None).unwrap();
    io.wait_for_completion(c).unwrap();
    log.advance_offset_after_success(bytes);
}

/// The commit timestamps of the frames recovery reads from `file`.
fn commits(io: &Arc<dyn IO>, file: &Arc<dyn File>) -> Vec<u64> {
    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    reader.read_header(io).unwrap();
    let mut commits = Vec::new();
    while let Some(frame) = reader.next_frame_blocking(io).unwrap() {
        commits.push(frame[0].commit_ts());
    }
    commits
}

/// Flips the byte at `pos` of `file`, as a torn write leaves a frame.
fn flip(io: &Arc<dyn IO>, file: &Arc<dyn File>, pos: u64) {
    let mut reader = StreamingLogicalLogReader::new(file.clone(), None);
    let byte = io.block(|| reader.read_exact_at(pos, 1)).unwrap()[0];
    let c = file
        .pwrite(
            pos,
            Arc::new(Buffer::new(vec![!byte])),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
}

/// A torn frame, its commit never acknowledged, can sit before an intact one, since the disk takes
/// a sync's pages in any order. Recovery ends before the torn frame, and the commit written next
/// can be it again, byte for byte: then the intact frame after it would chain on and replay a
/// commit no one was told of. The zeros after the frame written again end the log there.
#[test]
fn test_a_frame_written_again_over_a_torn_one_ends_the_log() {
    let (io, file, mut log) = open();
    append(&io, &mut log, record(10, 1, 100));
    let (end, end_crc) = (log.offset, log.running_crc);
    append(&io, &mut log, record(20, 2, 100));
    append(&io, &mut log, record(30, 3, 100));
    flip(&io, &file, end + TX_HEADER_SIZE as u64 + 1);
    assert_eq!(commits(&io, &file), [10], "the torn frame did not end the log");

    log.restore(RecoveredLog {
        segments: vec![RecoveredSegment {
            slot: 0,
            header: log.header().unwrap().clone(),
            end,
            end_crc,
            max_commit_ts: 10,
        }],
        max_seq: 0,
    })
    .unwrap();
    append(&io, &mut log, record(20, 2, 100));
    assert_eq!(
        commits(&io, &file),
        [10, 20],
        "the frame after the one written again was read"
    );
}

/// A log that grows in steps grows its file a whole step at a time, when a frame and its guard run
/// past the file's end, and reads back every frame.
#[test]
fn test_a_log_that_grows_in_steps_grows_its_file_a_step_at_a_time() {
    let (io, file, mut log) = open();
    log.set_growth(4096).unwrap();
    let mut sizes = Vec::new();
    for ts in 1..=12 {
        append(&io, &mut log, record(ts * 10, ts as i64, 1000));
        let size = file.size().unwrap();
        assert_eq!(
            size,
            (log.offset + END_GUARD as u64).next_multiple_of(4096),
            "the file is not the fewest steps that hold the frames and the guard"
        );
        sizes.push(size);
    }
    sizes.dedup();
    assert!(sizes.len() >= 3, "the file grew fewer than twice: {sizes:?}");
    assert_eq!(
        commits(&io, &file),
        (1..=12).map(|ts| ts * 10).collect::<Vec<_>>()
    );
}

/// The control: a log that does not grow in steps grows its file by each frame, and writes no
/// zeros past a frame at the file's end.
#[test]
fn test_a_log_that_does_not_grow_in_steps_ends_its_file_at_its_last_frame() {
    let (io, file, mut log) = open();
    for ts in 1..=4 {
        append(&io, &mut log, record(ts * 10, ts as i64, 1000));
        assert_eq!(file.size().unwrap(), log.offset);
    }
}

/// A checkpoint that covers every frame restarts a log that grows in steps in place: its file
/// keeps its size, and the frames written after are the log's only ones. Until the first of them
/// reaches the file, it holds the frames from before, which recovery reads.
#[test]
fn test_a_log_that_grows_in_steps_restarts_in_place() {
    let (io, file, mut log) = open();
    log.set_growth(4096).unwrap();
    append(&io, &mut log, record(10, 1, 100));
    append(&io, &mut log, record(20, 2, 100));
    let (c, outcome) = log.truncate_in_place(u64::MAX).unwrap();
    io.wait_for_completion(c).unwrap();
    assert!(matches!(outcome, LogicalLogTruncateOutcome::Truncated));
    assert_eq!(log.offset, 0);
    assert_eq!(file.size().unwrap(), 4096);
    assert_eq!(commits(&io, &file), [10, 20]);

    append(&io, &mut log, record(30, 3, 100));
    assert_eq!(commits(&io, &file), [30]);
    assert_eq!(file.size().unwrap(), 4096);
}

/// The control: a log that does not grow in steps truncates its file, in place or not, and one that
/// does truncates it when not asked to restart in place.
#[test]
fn test_a_log_truncates_its_file_unless_it_grows_in_steps_and_restarts_in_place() {
    for (growth, in_place) in [(0, true), (0, false), (4096, false)] {
        let (io, file, mut log) = open();
        log.set_growth(growth).unwrap();
        append(&io, &mut log, record(10, 1, 100));
        let (c, outcome) = if in_place {
            log.truncate_in_place(u64::MAX).unwrap()
        } else {
            log.truncate(u64::MAX).unwrap()
        };
        io.wait_for_completion(c).unwrap();
        assert!(matches!(outcome, LogicalLogTruncateOutcome::Truncated));
        assert_eq!(file.size().unwrap(), 0, "growth {growth}, in place {in_place}");
    }
}

/// The step is 0 or a power of two up to the largest one write can report.
#[test]
fn test_the_growth_step_is_zero_or_a_power_of_two_one_write_can_report() {
    let (_io, _file, mut log) = open();
    for bytes in [0, 1, 4096, MAX_LOG_GROWTH] {
        log.set_growth(bytes).unwrap();
        assert_eq!(log.growth(), bytes);
    }
    for bytes in [3, 4097, MAX_LOG_GROWTH * 2, u64::MAX] {
        assert!(log.set_growth(bytes).is_err(), "{bytes} was taken");
        assert_eq!(log.growth(), MAX_LOG_GROWTH);
    }
}
