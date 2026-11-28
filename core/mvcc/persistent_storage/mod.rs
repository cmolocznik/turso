use crate::io::FileSyncType;
use crate::storage::encryption::EncryptionContext;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::turso_assert;
use std::fmt::Debug;

#[cfg(test)]
mod discard_pending_tests;
pub mod logical_log;
#[cfg(test)]
mod rolling_tests;
use crate::mvcc::database::{LogRecord, RowID, RowVersion};
use crate::mvcc::persistent_storage::logical_log::{
    LogSerializer, LogicalLog, OnSerializationComplete, RecoveredLog,
    DEFAULT_LOG_CHECKPOINT_THRESHOLD,
};
use crate::{CheckpointResult, Completion, File, LimboError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalLogTruncateOutcome {
    Truncated,
    Retained,
}

/// Which frames a logical log holds, by the generation of their segment, as far as a checkpoint
/// can change it: a checkpoint that leaves it as it found it can free no more of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalLogHold {
    /// The generation of the oldest segment holding a frame, `None` when none does.
    pub oldest: Option<u64>,
    /// The generation of the segment being written.
    pub writing: u64,
    /// Whether a file is ready for the next roll.
    pub file_ready: bool,
    /// Whether a roll waits on the frame being written.
    pub roll_pending: bool,
}

pub trait DurableStorage: Send + Sync + Debug {
    /// Append one row-version op to `log_record`'s payload buffer, in the
    /// on-disk wire format used by the logical log. Updates `op_count`.
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_id: &RowID,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()>;

    /// Append a `DatabaseHeader` op to `log_record`'s payload buffer.
    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> Result<()>;

    /// Write a transaction to the logical log without advancing the writer offset.
    ///
    /// If `on_serialization_complete` is provided, it is called with shared
    /// ownership of the framed bytes and the frame's
    /// [`logical_log::LogTxFrameInfo`] chain state (start offset, pre-frame
    /// committed CRC, post-frame CRC) after framing but before the disk write.
    /// The callback runs while the internal write lock is held, so it should
    /// be fast.
    fn log_tx(
        &self,
        m: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)>;

    /// If `m` needs a logical-log header upgrade before it can be appended,
    /// start that write and return its completion. While a header write or a
    /// truncate of the log is in flight, return its completion instead.
    /// Callers must wait for this completion and call this again; once it
    /// returns `None`, they call `log_tx`.
    fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> Result<Option<Completion>>;

    /// The most bytes `m`'s frame takes once [`Self::log_tx`] writes it, not
    /// counting the log's header.
    fn frame_len_bound(&self, m: &LogRecord) -> Result<usize>;

    fn sync(&self, sync_type: FileSyncType) -> Result<Completion>;

    /// Called after a logical-log write completed successfully, before the
    /// transaction is made visible by advancing the logical-log offset.
    ///
    /// Implementations may return a completion for any additional durability
    /// work that must finish before commit publication.
    fn on_log_write_complete(&self) -> Result<Completion> {
        Ok(Completion::new_yield())
    }

    /// Persist the current logical-log header to durable storage.
    ///
    /// This is used by MVCC recovery/checkpoint flows. Keeping this in the trait avoids
    /// reaching into concrete storage internals.
    fn update_header(&self) -> Result<Completion>;

    /// Truncate the logical log, discarding frames at or below
    /// `checkpointed_through_ts` (the checkpoint's published boundary). Frames
    /// above the boundary (uncheckpointed concurrent commits) are preserved.
    ///
    /// Returns whether the log was actually truncated ([`LogicalLogTruncateOutcome::Truncated`])
    /// or left intact ([`LogicalLogTruncateOutcome::Retained`]). Callers wait for
    /// the completion and then call `finish_truncate`.
    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> Result<(Completion, LogicalLogTruncateOutcome)>;

    /// Applies a finished truncate started by `truncate`: if it succeeded, the
    /// log is empty and `logical_log_offset` returns 0; if it failed, the log
    /// keeps its frames and offset.
    fn finish_truncate(&self);

    /// Reset the logical log to a fresh header-only file.
    ///
    /// Used after an external database restore so future MVCC recovery starts
    /// from the restored image instead of replaying stale local log frames.
    fn reset_to_fresh_header(&self) -> Result<Completion>;
    fn get_logical_log_file(&self) -> Arc<dyn File>;
    fn logical_log_offset(&self) -> u64;
    /// Whether the log holds at least the threshold of bytes no checkpoint has covered yet.
    fn should_checkpoint(&self) -> bool;
    /// The log's bytes no checkpoint has covered yet.
    fn uncheckpointed_bytes(&self) -> u64 {
        self.logical_log_offset()
    }
    /// Records that a checkpoint covered the log up to `offset`, so the threshold counts only the
    /// bytes after it. A log a passive checkpoint cannot truncate keeps its size, and a trigger on
    /// size alone would ask for a checkpoint at every commit.
    fn set_checkpoint_floor(&self, _offset: u64) {}
    /// Set the checkpoint threshold in bytes of logical-log data written.
    /// A negative value disables automatic checkpointing.
    fn set_checkpoint_threshold(&self, threshold: i64);
    fn checkpoint_threshold(&self) -> i64;
    fn advance_logical_log_offset_after_success(&self, bytes: u64) -> Result<()>;
    #[aristo::intent(
        "the pending running-CRC slot is cleared by the storage abort path after an abandoned deferred-offset write",
        id = "logical_log_pending_crc_cleared_on_abort",
        verify = "full"
    )]
    fn discard_pending_log_write(&self) -> Result<()>;
    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32);

    /// Set the in-memory log header from a previously-read on-disk header.
    ///
    /// Called during recovery to seed the CRC state from the header's salt.
    fn set_header(&self, header: logical_log::LogHeader);

    /// The log's files for recovery to read, by slot: a log that rolls has one per segment file.
    fn log_files(&self) -> Vec<(usize, Arc<dyn File>)> {
        vec![(0, self.get_logical_log_file())]
    }

    /// Takes up writing where recovery left the log: after the last valid frame of the last
    /// segment it replayed.
    fn restore_after_recovery(&self, recovered: RecoveredLog) -> Result<()> {
        if let Some(active) = recovered.segments.last() {
            self.set_header(active.header.clone());
            self.restore_logical_log_state_after_recovery(active.end, active.end_crc);
        }
        Ok(())
    }

    /// The bytes the log's files hold, every segment still needed counted.
    fn logical_log_size(&self) -> Result<u64> {
        self.get_logical_log_file().size()
    }

    /// How many segments the log is spread over: the one being written and those sealed behind it.
    fn logical_log_segments(&self) -> usize {
        1
    }

    /// Which frames the log holds, by generation. A log that does not count its segments reads as
    /// one segment, which holds frames or none.
    fn logical_log_hold(&self) -> LogicalLogHold {
        LogicalLogHold {
            oldest: (self.logical_log_offset() > 0).then_some(0),
            writing: 0,
            file_ready: false,
            roll_pending: false,
        }
    }

    /// Goes on writing in the segment of `slot`, the log's newest, whose header is `header`.
    fn set_active_segment(&self, _slot: usize, header: logical_log::LogHeader) {
        self.set_header(header);
    }

    /// Lets the log roll with portable changes in its frames, as a log replicator takes them.
    fn roll_with_portable_changes(&self) {}

    /// Called when a checkpoint begins, before any rows are written to the B-tree.
    fn on_checkpoint_start(&self) -> Result<()> {
        Ok(())
    }

    /// Called after the checkpoint has fully completed: rows are flushed, WAL is
    /// truncated, and the logical log is reset.
    ///
    /// Runs while checkpoint locks are still held.
    fn on_checkpoint_end(&self, _result: Result<&CheckpointResult>) -> Result<()> {
        Ok(())
    }

    fn encryption_ctx(&self) -> Option<EncryptionContext> {
        None
    }
}

pub struct Storage {
    pub logical_log: RwLock<LogicalLog>,
    /// Shadowed from LogicalLog::offset for lock-free should_checkpoint() reads.
    log_offset: AtomicU64,
    /// The offset the last checkpoint covered; zero once the log is truncated.
    checkpoint_floor: AtomicU64,
    checkpoint_threshold: AtomicI64,
}

static LOG_SYNCS: AtomicU64 = AtomicU64::new(0);
static LOG_SYNC_MICROS: AtomicU64 = AtomicU64::new(0);
static LOG_SYNC_MICROS_MAX: AtomicU64 = AtomicU64::new(0);

/// Process-wide logical-log fsyncs: (count, total µs, longest µs). The fsync is the one wait a
/// durable commit cannot avoid, so a commit no slower than the longest fsync waited on the disk.
pub fn logical_log_sync_stats() -> (u64, u64, u64) {
    (
        LOG_SYNCS.load(Ordering::Relaxed),
        LOG_SYNC_MICROS.load(Ordering::Relaxed),
        LOG_SYNC_MICROS_MAX.load(Ordering::Relaxed),
    )
}

impl Storage {
    /// A log of one file, `file`.
    pub fn new(
        file: Arc<dyn File>,
        io: Arc<dyn crate::IO>,
        encryption_ctx: Option<EncryptionContext>,
    ) -> Self {
        Self::from_log(LogicalLog::new(file, io, encryption_ctx))
    }

    /// A log that rolls through the files `base`, `base.1` and on, `file` being `base`'s, opened
    /// with `flags`; see [`LogicalLog::new_rolling`].
    pub fn new_rolling(
        file: Arc<dyn File>,
        io: Arc<dyn crate::IO>,
        encryption_ctx: Option<EncryptionContext>,
        base: String,
        flags: crate::OpenFlags,
    ) -> Result<Self> {
        Ok(Self::from_log(LogicalLog::new_rolling(
            file,
            io,
            encryption_ctx,
            base,
            flags,
        )?))
    }

    fn from_log(log: LogicalLog) -> Self {
        Self {
            logical_log: RwLock::new(log),
            log_offset: AtomicU64::new(0),
            checkpoint_floor: AtomicU64::new(0),
            checkpoint_threshold: AtomicI64::new(DEFAULT_LOG_CHECKPOINT_THRESHOLD),
        }
    }

    /// Update the shadow offset to stay in sync with LogicalLog::offset.
    /// Called after any operation that mutates the canonical offset under the write lock.
    #[inline(always)]
    fn shadow_offset_store(&self, value: u64) {
        self.log_offset.store(value, Ordering::Relaxed);
        // A floor past the end means the log restarted below it.
        if self.checkpoint_floor.load(Ordering::Relaxed) > value {
            self.checkpoint_floor.store(value, Ordering::Relaxed);
        }
    }

    /// Runs `f` with the log locked for writing, then copies the log's offset
    /// to the shadow offset. Every write access goes through here because the
    /// offset can change in any of them: a truncate that finished after its
    /// call returned resets the offset to 0 the next time the log is used.
    fn with_log<R>(&self, f: impl FnOnce(&mut LogicalLog) -> R) -> R {
        let mut log = self.logical_log.write();
        let result = f(&mut log);
        // The log's absolute end, past the files it rolled out of.
        self.shadow_offset_store(log.segment_start() + log.offset);
        result
    }
}

impl DurableStorage for Storage {
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_id: &RowID,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()> {
        LogSerializer::new(&mut log_record.buf).serialize_op_entry(
            row_id,
            row_version,
            portable_extension,
        )?;
        log_record.op_count = log_record.op_count.checked_add(1).ok_or_else(|| {
            LimboError::InternalError("logical log op_count exceeds u32".to_string())
        })?;
        Ok(())
    }

    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> Result<()> {
        turso_assert!(
            !log_record.has_header,
            "DatabaseHeader op appended more than once to a single LogRecord"
        );
        LogSerializer::new(&mut log_record.buf).serialize_header_entry(header)?;
        log_record.has_header = true;
        log_record.op_count = log_record.op_count.checked_add(1).ok_or_else(|| {
            LimboError::InternalError("logical log op_count exceeds u32".to_string())
        })?;
        Ok(())
    }

    fn log_tx(
        &self,
        m: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)> {
        self.with_log(|log| log.log_tx_deferred_offset(m, on_serialization_complete))
    }

    fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> Result<Option<Completion>> {
        self.with_log(|log| log.upgrade_header_for_log_tx(m))
    }

    fn frame_len_bound(&self, m: &LogRecord) -> Result<usize> {
        self.logical_log.read().frame_len_bound(m)
    }

    fn sync(&self, sync_type: FileSyncType) -> Result<Completion> {
        let started = std::time::Instant::now();
        let completion = self.with_log(|log| log.sync(sync_type));
        let micros = started.elapsed().as_micros() as u64;
        LOG_SYNCS.fetch_add(1, Ordering::Relaxed);
        LOG_SYNC_MICROS.fetch_add(micros, Ordering::Relaxed);
        LOG_SYNC_MICROS_MAX.fetch_max(micros, Ordering::Relaxed);
        completion
    }

    fn update_header(&self) -> Result<Completion> {
        self.with_log(|log| log.update_header())
    }

    #[aristo::intent("after a truncate, once no write is in flight, the in-memory shadow_offset equals the on-disk durable_offset (the tracked end-of-log matches what's been fsync'd)", id = "aristos:logical_log_shadow_offset_matches_durable", verify = "full")]
    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> Result<(Completion, LogicalLogTruncateOutcome)> {
        self.with_log(|log| log.truncate(checkpointed_through_ts))
    }

    fn finish_truncate(&self) {
        self.with_log(|log| log.publish_finished_header_change());
    }

    fn reset_to_fresh_header(&self) -> Result<Completion> {
        self.with_log(|log| log.reset_to_fresh_header())
    }

    fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.logical_log.read().file.clone()
    }

    fn logical_log_offset(&self) -> u64 {
        self.log_offset.load(Ordering::Relaxed)
    }

    fn encryption_ctx(&self) -> Option<EncryptionContext> {
        self.logical_log.read().encryption_ctx().cloned()
    }

    /// Lock-free: reads shadowed atomics only.
    fn should_checkpoint(&self) -> bool {
        let threshold = self.checkpoint_threshold.load(Ordering::Relaxed);
        threshold >= 0 && self.uncheckpointed_bytes() >= threshold as u64
    }

    fn uncheckpointed_bytes(&self) -> u64 {
        self.log_offset
            .load(Ordering::Relaxed)
            .saturating_sub(self.checkpoint_floor.load(Ordering::Relaxed))
    }

    fn set_checkpoint_floor(&self, offset: u64) {
        let end = self.log_offset.load(Ordering::Relaxed);
        self.checkpoint_floor
            .store(offset.min(end), Ordering::Relaxed);
    }

    fn set_checkpoint_threshold(&self, threshold: i64) {
        self.checkpoint_threshold
            .store(threshold, Ordering::Relaxed);
    }

    fn checkpoint_threshold(&self) -> i64 {
        self.checkpoint_threshold.load(Ordering::Relaxed)
    }

    fn advance_logical_log_offset_after_success(&self, bytes: u64) -> Result<()> {
        self.with_log(|log| log.advance_offset_after_success(bytes));
        Ok(())
    }

    fn discard_pending_log_write(&self) -> Result<()> {
        self.with_log(|log| log.discard_pending_write());
        Ok(())
    }

    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32) {
        self.with_log(|log| {
            log.offset = offset;
            log.running_crc = running_crc;
        });
    }

    fn set_header(&self, header: logical_log::LogHeader) {
        self.with_log(|log| log.set_header(header));
    }

    fn log_files(&self) -> Vec<(usize, Arc<dyn File>)> {
        self.logical_log.read().files()
    }

    fn restore_after_recovery(&self, recovered: RecoveredLog) -> Result<()> {
        let mut log = self.logical_log.write();
        log.restore(recovered)?;
        let end = log.segment_start() + log.offset;
        drop(log);
        self.shadow_offset_store(end);
        Ok(())
    }

    fn logical_log_size(&self) -> Result<u64> {
        self.logical_log.read().size()
    }

    fn logical_log_segments(&self) -> usize {
        self.logical_log.read().segments()
    }

    fn logical_log_hold(&self) -> LogicalLogHold {
        self.logical_log.read().hold()
    }

    fn set_active_segment(&self, slot: usize, header: logical_log::LogHeader) {
        self.logical_log.write().set_active_segment(slot, header);
    }

    fn roll_with_portable_changes(&self) {
        self.logical_log.write().roll_with_portable_changes();
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalLog {{ logical_log }}")
    }
}
