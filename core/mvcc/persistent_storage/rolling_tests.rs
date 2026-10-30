//! A logical log that rolls: a checkpoint that has to keep frames seals the segment being written
//! and the log goes on in another file, which a later checkpoint frees; recovery reads the
//! segments as one log.

use crate::io::FileSyncType;
use crate::io::{MemoryIO, IO};
use crate::mvcc::database::tests::generate_simple_string_row;
use crate::mvcc::database::{LogRecord, PackedTs, RowVersion, TxTimestampOrID, WalPos};
use crate::mvcc::persistent_storage::logical_log::{
    segment_path, HeaderReadResult, LogChainReader, LogicalLog, RecoveredLog, LOG_SEGMENT_FILES,
};
use crate::mvcc::persistent_storage::LogicalLogTruncateOutcome;
use crate::storage::encryption::{CipherMode, EncryptionContext, EncryptionKey};
use crate::sync::{Arc, Mutex};
use crate::util::IOExt as _;
use crate::{Buffer, Clock, Completion, File, MonotonicInstant, OpenFlags, WallClockInstant};

const BASE: &str = "rolling.db-log";

fn open(io: &Arc<dyn IO>) -> LogicalLog {
    let file = io.open_file(BASE, OpenFlags::Create, false).unwrap();
    LogicalLog::new_rolling(file, io.clone(), None, BASE.to_string(), OpenFlags::Create).unwrap()
}

fn record(commit_ts: u64) -> LogRecord {
    record_of(commit_ts, "value")
}

fn record_of(commit_ts: u64, value: &str) -> LogRecord {
    let mut tx = LogRecord::new(commit_ts, crate::alloc::DynAllocator::default()).unwrap();
    let row = generate_simple_string_row((-2).into(), commit_ts as i64, value);
    let version = RowVersion {
        id: commit_ts,
        begin: PackedTs::pack(Some(TxTimestampOrID::Timestamp(commit_ts))),
        end: PackedTs::pack(None),
        data: row.data.clone(),
        btree_resident: false,
        materialized_at: WalPos::ORIGIN,
    };
    tx.push_row_version_for_test(&row.id, &version);
    tx
}

/// Appends a commit at `commit_ts` as a commit does: the frame is written, then its offset advanced.
fn append(io: &Arc<dyn IO>, log: &mut LogicalLog, commit_ts: u64) {
    append_value(io, log, commit_ts, "value");
}

fn append_value(io: &Arc<dyn IO>, log: &mut LogicalLog, commit_ts: u64, value: &str) {
    let (c, bytes) = log
        .log_tx_deferred_offset(record_of(commit_ts, value), None)
        .unwrap();
    io.wait_for_completion(c).unwrap();
    log.advance_offset_after_success(bytes);
}

/// What a checkpoint that covered every commit through `boundary` does to the log, its IO done.
fn checkpoint(io: &Arc<dyn IO>, log: &mut LogicalLog, boundary: u64) -> LogicalLogTruncateOutcome {
    let (c, outcome) = log.truncate(boundary).unwrap();
    io.wait_for_completion(c).unwrap();
    outcome
}

/// Opens the log's files again and reads them as recovery does: the commits replayed, in order,
/// what recovery hands the writer, and the reopened log.
fn recover(io: &Arc<dyn IO>) -> (Vec<u64>, RecoveredLog, LogicalLog) {
    recover_after(io, 0)
}

/// As [`recover`], with a checkpoint having covered every commit through `boundary`.
fn recover_after(io: &Arc<dyn IO>, boundary: u64) -> (Vec<u64>, RecoveredLog, LogicalLog) {
    let log = open(io);
    let mut reader = LogChainReader::new(log.files(), None);
    let header = io.block(|| reader.try_read_header_nonblock()).unwrap();
    assert!(!matches!(header, HeaderReadResult::Invalid));
    reader.plan(boundary).unwrap();
    let mut commits = Vec::new();
    while let Some(frame) = io.block(|| reader.next_frame()).unwrap() {
        commits.push(frame[0].commit_ts());
    }
    (commits, reader.recovered(), log)
}

fn slot_file(io: &Arc<dyn IO>, slot: usize) -> Arc<dyn File> {
    io.open_file(&segment_path(BASE, slot), OpenFlags::None, false)
        .unwrap()
}

fn read_all(io: &Arc<dyn IO>, file: &Arc<dyn File>) -> Vec<u8> {
    let len = file.size().unwrap() as usize;
    let buf = Arc::new(Buffer::new_temporary(len));
    let c = file
        .pread(0, Completion::new_read(buf.clone(), |_| None))
        .unwrap();
    io.wait_for_completion(c).unwrap();
    buf.as_slice()[..len].to_vec()
}

fn write_all(io: &Arc<dyn IO>, file: &Arc<dyn File>, bytes: &[u8]) {
    let c = file
        .pwrite(
            0,
            Arc::new(Buffer::new(bytes.to_vec())),
            Completion::new_write(|_| {}),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
}

fn truncate_file(io: &Arc<dyn IO>, file: &Arc<dyn File>, len: u64) {
    let c = file.truncate(len, Completion::new_trunc(|_| {})).unwrap();
    io.wait_for_completion(c).unwrap();
}

/// A log rolled once: commits 1 to 4 sealed in the first file, 5 and 6 in the second.
fn rolled_log(io: &Arc<dyn IO>) -> LogicalLog {
    let mut log = open(io);
    for ts in 1..=2 {
        append(io, &mut log, ts);
    }
    // A checkpoint that covers nothing, with no free file yet, readies one.
    assert_eq!(
        checkpoint(io, &mut log, 0),
        LogicalLogTruncateOutcome::Retained
    );
    assert_eq!(log.segments(), 1);
    for ts in 3..=4 {
        append(io, &mut log, ts);
    }
    // The next one rolls into it.
    assert_eq!(
        checkpoint(io, &mut log, 0),
        LogicalLogTruncateOutcome::Retained
    );
    assert_eq!(log.segments(), 2);
    for ts in 5..=6 {
        append(io, &mut log, ts);
    }
    log
}

/// Under writes that never pause every checkpoint has to keep frames. The log rolls instead, and
/// each segment it seals is freed by the next checkpoint, so however many checkpoints run, the log
/// keeps to a few files of a few frames each.
#[test]
fn a_log_that_has_to_keep_frames_rolls_and_the_next_checkpoint_frees_it() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let mut log = open(&io);
    let mut ts = 0;
    for round in 0..200 {
        // Of each round's two commits, the checkpoint covers only the first.
        ts += 1;
        append(&io, &mut log, ts);
        ts += 1;
        append(&io, &mut log, ts);
        assert_eq!(
            checkpoint(&io, &mut log, ts - 1),
            LogicalLogTruncateOutcome::Retained
        );
        assert!(
            log.segments() <= LOG_SEGMENT_FILES,
            "round {round}: {} segments",
            log.segments()
        );
        assert!(
            log.size().unwrap() < 2048,
            "round {round}: the log holds {} bytes",
            log.size().unwrap()
        );
    }
    assert_eq!(log.segments(), 2);
    drop(log);
    // The segment the last roll began holds no frame, so has no header on disk: recovery goes
    // on writing in the one sealed before it.
    let (commits, recovered, _) = recover_after(&io, ts - 1);
    assert_eq!(commits, [ts - 1, ts]);
    assert_eq!(recovered.segments.len(), 1);
}

/// Recovery reads a rolled log as one log, and writing goes on after its last frame.
#[test]
fn recovery_replays_every_segment_and_writing_goes_on_in_the_newest() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    drop(rolled_log(&io));
    let (commits, recovered, mut log) = recover(&io);
    assert_eq!(commits, [1, 2, 3, 4, 5, 6]);
    assert_eq!(recovered.segments.len(), 2);
    log.restore(recovered).unwrap();
    assert_eq!(log.segments(), 2);
    append(&io, &mut log, 7);
    drop(log);
    let (commits, _, _) = recover(&io);
    assert_eq!(commits, [1, 2, 3, 4, 5, 6, 7]);
}

/// A sealed segment torn before its successor was written ends the log there, as frames past a
/// torn one in a single file are not replayed, and writing goes on after its last valid frame.
#[test]
fn a_sealed_segment_torn_before_its_successor_ends_the_log() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    drop(rolled_log(&io));
    let sealed = slot_file(&io, 0);
    let size = sealed.size().unwrap();
    truncate_file(&io, &sealed, size - 3);
    let (commits, recovered, mut log) = recover(&io);
    assert_eq!(commits, [1, 2, 3]);
    assert_eq!(recovered.segments.len(), 1);
    log.restore(recovered).unwrap();
    assert_eq!(log.segments(), 1);
    // The segment past the torn one is freed: nothing will read it.
    assert_eq!(slot_file(&io, 1).size().unwrap(), 0);
    append(&io, &mut log, 10);
    drop(log);
    let (commits, _, _) = recover(&io);
    assert_eq!(commits, [1, 2, 3, 10]);
}

/// Recovery refuses to begin at a segment past commits no checkpoint has covered: the segment
/// holding them is missing. Once they are covered it begins there.
#[test]
fn recovery_refuses_a_log_whose_needed_older_segment_is_missing() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    drop(rolled_log(&io));
    truncate_file(&io, &slot_file(&io, 0), 0);
    let log = open(&io);
    let mut reader = LogChainReader::new(log.files(), None);
    io.block(|| reader.try_read_header_nonblock()).unwrap();
    assert!(
        reader.plan(3).is_err(),
        "recovery began past commit 4, which no checkpoint covered"
    );
    drop(log);
    let (commits, recovered, _) = recover_after(&io, 4);
    assert_eq!(commits, [5, 6]);
    assert_eq!(recovered.segments.len(), 1);
}

/// A checkpoint covered the sealed segment, which is still there: recovery begins after it.
#[test]
fn recovery_begins_after_the_segments_a_checkpoint_covered() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    drop(rolled_log(&io));
    let (commits, recovered, _) = recover_after(&io, 4);
    assert_eq!(commits, [5, 6]);
    assert_eq!(recovered.segments.len(), 1);
    assert_eq!(recovered.segments[0].slot, 1);
}

/// A recovery that ended at a torn segment frees the segment that followed it, which no recovery
/// may replay. Should that file survive in a slot the next roll does not reuse, after the torn
/// segment's writer has gone on past where it began, a recovery still follows the chain the
/// writer wrote: each segment names its predecessor, and the next segment takes a number of its
/// own.
#[test]
fn a_recovery_follows_the_chain_past_a_successor_left_by_a_torn_segment() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    drop(rolled_log(&io));
    let unreachable = read_all(&io, &slot_file(&io, 1));
    let sealed = slot_file(&io, 0);
    let size = sealed.size().unwrap();
    truncate_file(&io, &sealed, size - 3);
    let (_, recovered, mut log) = recover(&io);
    log.restore(recovered).unwrap();
    // Longer than the torn commit 4, so the torn segment now ends past where its old successor
    // began.
    append_value(&io, &mut log, 10, &"v".repeat(64));
    // Covers nothing: rolls into the freed file.
    assert_eq!(
        checkpoint(&io, &mut log, 0),
        LogicalLogTruncateOutcome::Retained
    );
    assert_eq!(log.segments(), 2);
    append(&io, &mut log, 11);
    drop(log);
    let stale = io
        .open_file(&segment_path(BASE, 2), OpenFlags::Create, false)
        .unwrap();
    write_all(&io, &stale, &unreachable);
    let (commits, recovered, _) = recover(&io);
    assert_eq!(commits, [1, 2, 3, 10, 11]);
    assert_eq!(recovered.max_seq, 2);
}

/// A roll a checkpoint asks for while a frame is being written happens once the frame lands, and
/// the frame stays in the segment it sealed.
#[test]
fn a_roll_asked_while_a_frame_is_written_happens_once_it_lands() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let mut log = open(&io);
    for ts in 1..=2 {
        append(&io, &mut log, ts);
    }
    checkpoint(&io, &mut log, 0);
    let (c, bytes) = log.log_tx_deferred_offset(record(3), None).unwrap();
    io.wait_for_completion(c).unwrap();
    assert_eq!(
        checkpoint(&io, &mut log, 0),
        LogicalLogTruncateOutcome::Retained
    );
    assert_eq!(
        log.segments(),
        1,
        "the log rolled under a frame being written"
    );
    log.advance_offset_after_success(bytes);
    assert_eq!(log.segments(), 2);
    append(&io, &mut log, 4);
    drop(log);
    let (commits, recovered, _) = recover(&io);
    assert_eq!(commits, [1, 2, 3, 4]);
    assert_eq!(recovered.segments.len(), 2);
    assert_eq!(recovered.segments[0].max_commit_ts, 3);
}

/// Once a checkpoint covers every commit, a log that has rolled resets into a numbered segment of
/// its own, so a sealed segment whose free never reached the disk cannot outrank it.
#[test]
fn a_reset_after_rolling_outranks_a_segment_freed_before_it() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let mut log = rolled_log(&io);
    let stale = read_all(&io, &slot_file(&io, 0));
    assert_eq!(
        checkpoint(&io, &mut log, 6),
        LogicalLogTruncateOutcome::Truncated
    );
    assert_eq!(log.segments(), 1);
    append(&io, &mut log, 7);
    drop(log);
    // The first file's truncate is lost: it holds commits 1 to 4 again.
    write_all(&io, &slot_file(&io, 0), &stale);
    let (commits, recovered, _) = recover_after(&io, 6);
    assert_eq!(commits, [7]);
    assert_eq!(recovered.segments.len(), 1);
    assert_eq!(recovered.segments[0].slot, 1);
    // A reset follows nothing, whatever a checkpoint is known to have covered.
    let (commits, _, _) = recover_after(&io, 0);
    assert_eq!(commits, [7]);
}

/// Memory files that lose, at a power loss, every write no sync of their own file covered.
struct PowerLossIO {
    memory: Arc<MemoryIO>,
    files: Mutex<Vec<Arc<PowerLossFile>>>,
}

struct PowerLossFile {
    path: String,
    file: Arc<dyn File>,
    io: Arc<MemoryIO>,
    /// The file's bytes as of its last sync.
    durable: Mutex<Vec<u8>>,
}

impl PowerLossIO {
    fn new() -> Self {
        Self {
            memory: Arc::new(MemoryIO::new()),
            files: Mutex::new(Vec::new()),
        }
    }

    /// Every file goes back to what its last sync made durable.
    fn power_loss(&self) {
        let io: Arc<dyn IO> = self.memory.clone();
        for file in self.files.lock().iter() {
            let durable = file.durable.lock().clone();
            truncate_file(&io, &file.file, 0);
            if !durable.is_empty() {
                write_all(&io, &file.file, &durable);
            }
        }
    }
}

impl Clock for PowerLossIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.memory.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.memory.current_time_wall_clock()
    }
}

impl IO for PowerLossIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> crate::Result<Arc<dyn File>> {
        let mut files = self.files.lock();
        if let Some(file) = files.iter().find(|file| file.path == path) {
            return Ok(file.clone());
        }
        let file = Arc::new(PowerLossFile {
            path: path.to_string(),
            file: self.memory.open_file(path, flags, direct)?,
            io: self.memory.clone(),
            durable: Mutex::new(Vec::new()),
        });
        files.push(file.clone());
        Ok(file)
    }

    fn remove_file(&self, path: &str) -> crate::Result<()> {
        self.memory.remove_file(path)
    }
}

impl File for PowerLossFile {
    fn lock_file(&self, exclusive: bool) -> crate::Result<()> {
        self.file.lock_file(exclusive)
    }

    fn unlock_file(&self) -> crate::Result<()> {
        self.file.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> crate::Result<Completion> {
        self.file.pread(pos, c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> crate::Result<Completion> {
        self.file.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> crate::Result<Completion> {
        let io: Arc<dyn IO> = self.io.clone();
        *self.durable.lock() = read_all(&io, &self.file);
        self.file.sync(c, sync_type)
    }

    fn size(&self) -> crate::Result<u64> {
        self.file.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> crate::Result<Completion> {
        self.file.truncate(len, c)
    }
}

/// A frame written before a roll is in the sealed segment's file. The sync of the first commit
/// after the roll covers that file as well as the new segment's, so a power loss after it loses
/// neither frame.
#[test]
fn a_sync_after_a_roll_makes_the_sealed_segment_durable() {
    let power = Arc::new(PowerLossIO::new());
    let io: Arc<dyn IO> = power.clone();
    let mut log = open(&io);
    for ts in 1..=2 {
        append(&io, &mut log, ts);
    }
    io.wait_for_completion(log.sync(FileSyncType::Fsync).unwrap())
        .unwrap();
    checkpoint(&io, &mut log, 0);
    // Written, not yet synced, and sealed by the roll.
    append(&io, &mut log, 3);
    checkpoint(&io, &mut log, 0);
    assert_eq!(log.segments(), 2);
    append(&io, &mut log, 4);
    io.wait_for_completion(log.sync(FileSyncType::Fsync).unwrap())
        .unwrap();
    drop(log);
    power.power_loss();
    let (commits, _, _) = recover(&io);
    assert_eq!(commits, [1, 2, 3, 4]);
}

fn encryption(key: u8) -> EncryptionContext {
    EncryptionContext::new(
        CipherMode::Aes128Gcm,
        &EncryptionKey::Key128([key; 16]),
        4096,
    )
    .unwrap()
}

/// An encrypted log rolls as a plain one does. Each segment's header has a salt of its own, which
/// the segment's frames are sealed under, and recovery decrypts each segment under its own; under
/// another key it replays nothing.
#[test]
fn an_encrypted_log_rolls_and_recovery_decrypts_every_segment() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let open_with = |key: u8| {
        let file = io.open_file(BASE, OpenFlags::Create, false).unwrap();
        LogicalLog::new_rolling(
            file,
            io.clone(),
            Some(encryption(key)),
            BASE.to_string(),
            OpenFlags::Create,
        )
        .unwrap()
    };
    let mut log = open_with(0x42);
    for ts in 1..=2 {
        append(&io, &mut log, ts);
    }
    assert_eq!(
        checkpoint(&io, &mut log, 0),
        LogicalLogTruncateOutcome::Retained
    );
    for ts in 3..=4 {
        append(&io, &mut log, ts);
    }
    assert_eq!(
        checkpoint(&io, &mut log, 0),
        LogicalLogTruncateOutcome::Retained
    );
    assert_eq!(log.segments(), 2);
    for ts in 5..=6 {
        append(&io, &mut log, ts);
    }
    drop(log);
    let salt = |slot| {
        let header = read_all(&io, &slot_file(&io, slot));
        u64::from_le_bytes(header[8..16].try_into().unwrap())
    };
    assert_ne!(salt(0), salt(1), "the two segments share a salt");
    let replay = |key: u8| {
        let log = open_with(key);
        let mut reader = LogChainReader::new(log.files(), Some(encryption(key)));
        let header = io.block(|| reader.try_read_header_nonblock()).unwrap();
        assert!(matches!(header, HeaderReadResult::Valid(_)));
        reader.plan(0).unwrap();
        let mut commits = Vec::new();
        while let Some(frame) = io.block(|| reader.next_frame()).unwrap() {
            commits.push(frame[0].commit_ts());
        }
        commits
    };
    assert_eq!(replay(0x42), [1, 2, 3, 4, 5, 6]);
    assert_eq!(replay(0x24), Vec::<u64>::new());
}
