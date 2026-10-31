//! The log: one ordered stream of records for the whole database.
//!
//! **Appending.** Records are copied into a ring of aligned chunks in memory. An appender reserves
//! its bytes with one atomic add on the insert position and copies them in, in parallel with
//! every other appender. Each chunk counts the bytes copied into it over its whole life, so a
//! flusher knows a stretch of the log is complete when the count reaches what was reserved there.
//! Space is never waited for in the middle of a change: an operation is admitted first
//! ([`Log::admit`]), for at most the bytes it will append, waiting there if the unflushed log is
//! full or the next segment is not ready.
//!
//! **Groups.** A change spanning several pages appends its records as one group
//! ([`Log::append_group`]): one reservation, so the group is contiguous, and each record carries
//! its place in it ([`Place`]). Recovery replays a group only once it has read all of it; a crash
//! that leaves part of one ends the log at the group's start.
//!
//! **Flushing.** The flusher writes the complete prefix of the log in whole [`BLOCK`]s, straight
//! from the chunks. When someone waits on a record in a block that is not full and nothing more is
//! coming, the block is padded to its end; the next flush starts in a fresh block, so bytes
//! already durable are never written again. Where the file supports it, writes are durable when
//! they complete (`RWF_DSYNC`) and several are in flight, the durable position being the end of
//! the contiguous prefix of completed writes. Elsewhere a write is followed by `fdatasync`, the
//! next write overlapping the sync in flight. A flush starts when someone waits, or when a chunk's
//! worth is complete; there are no timers. Everything is a chain of completions, so it runs the
//! same whether the backend completes inside a submission or later from `IO::step`, and no lock is
//! held across a submission.
//!
//! **Segments.** The log is cut into segment files ([`segment`]). A segment is written full of
//! zeros and synced, with its directory, before any record goes into it, so a later write changes
//! no file metadata and a commit costs one data write. Segments behind what is still needed are
//! deleted.
//!
//! **Failure.** A failed write or sync means nothing about the bytes in flight can be trusted: the
//! log refuses every append after it and fails everyone waiting.

mod reader;
mod record;
mod segment;

pub use reader::{LogReader, OwnedRecord};
pub use record::{framed_len, Header, Kind, Lsn, Place, ALIGN, BLOCK, MAX_PAYLOAD};

use super::aligned::AlignedBlock;
use crate::io::{Completion, CompletionGroup, IO};
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use segment::Segments;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Bytes per segment file, a multiple of [`BLOCK`]. At the 1 GB/s log rate Umbra measured
    /// (Freitag et al., VLDB 2022 §4), the default of 256 MiB lasts a quarter of a second.
    pub segment_bytes: u64,
    /// Bytes appended and not yet durable, past which new operations wait to be admitted. The
    /// default of 32 MiB is 32 ms of log at 1 GB/s, far longer than a group commit takes, so
    /// operations wait here only while the device stalls.
    pub buffer_bytes: u64,
    /// The ring's chunk size, a multiple of [`BLOCK`]: the largest single write, and the most a
    /// slow copier holds back a flush by.
    pub chunk_bytes: u64,
    /// Writes in flight at once when writes are durable on completion: enough to keep the
    /// device's write latency overlapped.
    pub writes_in_flight: usize,
    /// Open segments with direct IO.
    pub direct: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 256 << 20,
            buffer_bytes: 32 << 20,
            chunk_bytes: 256 << 10,
            writes_in_flight: 4,
            direct: cfg!(target_os = "linux"),
        }
    }
}

struct Slot {
    block: Arc<AlignedBlock>,
    /// Bytes copied into this slot over all the chunks it has held.
    copied: AtomicU64,
}

#[derive(Default)]
struct Flush {
    /// Where the next write starts: the end of everything handed to writes.
    submitted: Lsn,
    /// Writes in flight or completed out of order, by start: their end and whether they are done.
    writes: BTreeMap<Lsn, (Lsn, bool)>,
    /// Without durable writes: where the completed writes end, the sync in flight and the
    /// segments written since their last sync.
    written: Lsn,
    syncing: bool,
    unsynced: BTreeSet<u64>,
    waiters: Vec<(Lsn, Completion)>,
    failed: Option<CompletionError>,
}

#[derive(Default)]
struct Admission {
    waiting: Vec<Completion>,
    /// The segment being prepared.
    preparing: Option<u64>,
}

pub struct Log {
    io: Arc<dyn IO>,
    segments: Segments,
    config: LogConfig,
    durable_writes: bool,
    ring: Box<[Slot]>,
    insert: AtomicU64,
    durable: AtomicU64,
    /// Records may be written below this: every segment before it is prepared.
    prepared_end: AtomicU64,
    /// Someone waits on a position no flush covers yet, or to be admitted.
    flush_wanted: AtomicBool,
    /// Bytes admitted and not yet appended.
    granted: AtomicU64,
    admitters_waiting: AtomicBool,
    failed: AtomicBool,
    flush: Mutex<Flush>,
    admission: Mutex<Admission>,
    zeros: Arc<AlignedBlock>,
}

/// What padding a partial block came to.
enum Pad {
    /// Padded to here.
    Padded(Lsn),
    /// Someone reserved bytes past the block's start meanwhile.
    Raced,
    /// Records already admitted would be pushed past the prepared segments.
    Unprepared,
}

/// Space an operation may append into, taken from [`Log::admit`]; what it does not use goes back
/// when it is dropped.
pub struct Admitted {
    log: Arc<Log>,
    left: u64,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        if self.left > 0 {
            self.log.granted.fetch_sub(self.left, Ordering::SeqCst);
            self.log.wake_admitters();
        }
    }
}

impl Log {
    /// Opens the log of the database at `base` to append from `end`, where recovery found its
    /// valid records end ([`LogReader`]). A new log has `end` 0.
    ///
    /// Appending resumes at the next block boundary, and everything from there to the end of its
    /// segment is zeroed first, so no record a crash left beyond `end` can ever be read as part of
    /// the log again. The bytes between `end` and that boundary stay unreadable, so a reader
    /// starting before `end` stops there: the caller must not acknowledge anything appended until
    /// a checkpoint whose redo point is at or past [`Log::end`] is durable.
    pub fn open(io: Arc<dyn IO>, base: &str, config: LogConfig, end: Lsn) -> Result<Arc<Self>> {
        assert!(
            config.segment_bytes.is_multiple_of(BLOCK)
                && config.chunk_bytes.is_multiple_of(BLOCK)
                && config.segment_bytes > 0
                && config.chunk_bytes > 0
                && config.writes_in_flight > 0,
            "log config {config:?}"
        );
        assert!(
            config.buffer_bytes >= config.chunk_bytes,
            "a log buffer smaller than one chunk"
        );
        let segments = Segments::new(io.clone(), base, config.segment_bytes, config.direct);
        let start = end.next_multiple_of(BLOCK);
        let current = segments.number(start);
        let durable_writes = Self::open_segments(&io, &segments, end, start)?;
        // Room for the admitted bytes, a padded block per write in flight, and the chunk being
        // flushed while the next fills.
        let slack = config.writes_in_flight as u64 * BLOCK + 2 * config.chunk_bytes;
        let slots = (config.buffer_bytes + slack).div_ceil(config.chunk_bytes) as usize;
        let ring = (0..slots)
            .map(|_| Slot {
                block: Arc::new(AlignedBlock::zeroed(config.chunk_bytes as usize)),
                copied: AtomicU64::new(0),
            })
            .collect();
        let log = Arc::new(Self {
            io,
            durable_writes,
            ring,
            insert: AtomicU64::new(start),
            durable: AtomicU64::new(start),
            prepared_end: AtomicU64::new(segments.start(current + 1)),
            flush_wanted: AtomicBool::new(false),
            granted: AtomicU64::new(0),
            admitters_waiting: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            flush: Mutex::new(Flush {
                submitted: start,
                written: start,
                ..Flush::default()
            }),
            admission: Mutex::new(Admission::default()),
            zeros: Arc::new(AlignedBlock::zeroed(config.chunk_bytes as usize)),
            segments,
            config,
        });
        log.account_resumed(start);
        Ok(log)
    }

    /// Readies the segment holding `start` to append into and removes every segment after it:
    /// they hold nothing of the log. Returns whether its writes are durable on completion.
    fn open_segments(io: &Arc<dyn IO>, segments: &Segments, end: Lsn, start: Lsn) -> Result<bool> {
        let current = segments.number(start);
        let mut later = current + 1;
        while segments.open(later)?.is_some() {
            segments.remove(later)?;
            later += 1;
        }
        let keeps_records = end > segments.start(current);
        let file = match segments.open(current)? {
            Some(file) if keeps_records => file,
            Some(_) => {
                segments.remove(current)?;
                segments.create(current)?
            }
            None if keeps_records => {
                return Err(LimboError::Corrupt(format!(
                    "log segment {current} holds records up to {end} but does not exist"
                )))
            }
            None => segments.create(current)?,
        };
        let from = start - segments.start(current);
        let zeros = Arc::new(AlignedBlock::zeroed((1 << 20).min(segments.size as usize)));
        let mut at = from;
        while at < segments.size {
            let len = (segments.size - at).min(zeros.len() as u64);
            let c = file.pwrite(
                at,
                zeros.region(0, len as usize),
                Completion::new_write(|_| {}),
            )?;
            io.wait_for_completion(c)?;
            at += len;
        }
        io.wait_for_completion(file.sync_data(Completion::new_sync(|_| {}))?)?;
        io.sync_directory(&segments.directory())?;
        Ok(file.supports_durable_writes())
    }

    /// Every chunk before `start`'s, and the bytes before `start` in its own, count as copied:
    /// they belong to the log before this process, and the first flush starts at `start`.
    fn account_resumed(&self, start: Lsn) {
        let chunk_bytes = self.config.chunk_bytes;
        let ring = self.ring.len() as u64;
        let chunk = start / chunk_bytes;
        for (s, slot) in self.ring.iter().enumerate() {
            let s = s as u64;
            let before = chunk / ring + u64::from(s < chunk % ring);
            let mut copied = before * chunk_bytes;
            if s == chunk % ring {
                copied += start - chunk * chunk_bytes;
            }
            slot.copied.store(copied, Ordering::Release);
        }
    }

    /// Where the next record will start.
    pub fn end(&self) -> Lsn {
        self.insert.load(Ordering::Acquire)
    }

    /// Everything before this is durable.
    /// The most an operation may be admitted for at once ([`LogConfig::buffer_bytes`]).
    pub fn buffer_bytes(&self) -> u64 {
        self.config.buffer_bytes
    }

    /// The ring's chunk size: a record longer than it holds a flush back by more than a chunk.
    pub fn chunk_bytes(&self) -> u64 {
        self.config.chunk_bytes
    }

    pub fn durable(&self) -> Lsn {
        self.durable.load(Ordering::Acquire)
    }

    pub fn durable_writes(&self) -> bool {
        self.durable_writes
    }

    /// Room to append at most `bytes` (the sum of [`framed_len`] of the records), taken before an
    /// operation's first latch. Waits while the unflushed log or the prepared segments are too
    /// short for it.
    pub fn admit(self: &Arc<Self>, bytes: u64) -> Result<IOResult<Admitted>> {
        if bytes > self.config.buffer_bytes {
            return Err(LimboError::InternalError(format!(
                "an operation needs {bytes} bytes of log, more than the {} byte log buffer",
                self.config.buffer_bytes
            )));
        }
        let wait = {
            let mut a = self.admission.lock();
            if self.failed.load(Ordering::Acquire) {
                None
            } else {
                // `granted` before `insert`: an append between the two reads is then counted
                // twice rather than not at all.
                let granted = self.granted.load(Ordering::SeqCst);
                let insert = self.insert.load(Ordering::SeqCst);
                let unflushed = insert - self.durable.load(Ordering::SeqCst) + granted;
                let reach = insert + granted + bytes;
                if unflushed + bytes <= self.config.buffer_bytes
                    && reach <= self.prepared_end.load(Ordering::Acquire)
                {
                    self.granted.fetch_add(bytes, Ordering::SeqCst);
                    None
                } else {
                    let c = Completion::new_wait();
                    a.waiting.push(c.clone());
                    self.admitters_waiting.store(true, Ordering::Release);
                    Some(c)
                }
            }
        };
        if let Some(e) = self.failure() {
            return Err(LimboError::CompletionError(e));
        }
        self.prepare_ahead();
        match wait {
            None => Ok(IOResult::Done(Admitted {
                log: self.clone(),
                left: bytes,
            })),
            Some(c) => {
                self.flush_wanted.store(true, Ordering::Release);
                self.flush();
                Ok(IOResult::IO(IOCompletions(c)))
            }
        }
    }

    /// Appends one record within `admitted`'s room and returns its LSN. The caller holds the
    /// latch of every page the record changes, so the log orders changes to a page as they were
    /// made.
    pub fn append(
        self: &Arc<Self>,
        admitted: &mut Admitted,
        header: &Header,
        payload: &[&[u8]],
    ) -> Lsn {
        let len = framed_len(payload_len(payload));
        let start = self.reserve(admitted, len);
        let lsn = self.write_record(start, header, Place::ALONE, payload);
        self.count_copied(start, lsn);
        if self.flush_wanted.load(Ordering::Acquire) {
            self.flush();
        }
        lsn
    }

    /// Appends `records` as one group: contiguous in the log, and replayed by recovery all
    /// together or not at all, so a change spanning several pages survives a crash whole or not
    /// at all. Returns where the group starts and each record's LSN. As with
    /// [`append`](Self::append), the caller holds the latch of every page the records change.
    pub fn append_group(
        self: &Arc<Self>,
        admitted: &mut Admitted,
        records: &[(Header, &[&[u8]])],
    ) -> (Lsn, Vec<Lsn>) {
        assert!(
            (2..=u8::MAX as usize).contains(&records.len()),
            "a group of {} records",
            records.len()
        );
        let total = records
            .iter()
            .map(|(_, payload)| framed_len(payload_len(payload)))
            .sum();
        // One reservation, so no other record lands inside the group.
        let start = self.reserve(admitted, total);
        let size = records.len() as u8;
        let mut at = start;
        let lsns = records
            .iter()
            .enumerate()
            .map(|(i, (header, payload))| {
                let place = Place {
                    index: i as u8,
                    size,
                };
                at = self.write_record(at, header, place, payload);
                at
            })
            .collect();
        self.count_copied(start, at);
        if self.flush_wanted.load(Ordering::Acquire) {
            self.flush();
        }
        (start, lsns)
    }

    /// Takes `len` bytes of `admitted`'s room at the log's end; returns where they start.
    fn reserve(&self, admitted: &mut Admitted, len: u64) -> Lsn {
        assert!(
            len <= admitted.left,
            "{len} bytes of records appended with {} bytes admitted",
            admitted.left
        );
        let start = self.insert.fetch_add(len, Ordering::SeqCst);
        self.granted.fetch_sub(len, Ordering::SeqCst);
        admitted.left -= len;
        start
    }

    /// Copies a record into the ring at `start`; returns its LSN.
    fn write_record(&self, start: Lsn, header: &Header, place: Place, payload: &[&[u8]]) -> Lsn {
        let payload_len = payload_len(payload);
        let head = record::header_bytes(header, place, payload_len);
        let crc = record::crc(start, &head, payload);
        let mut at = self.copy_in(start, &head);
        for piece in payload {
            at = self.copy_in(at, piece);
        }
        at = self.copy_in(at, &crc.to_le_bytes());
        let end = start + framed_len(payload_len);
        self.zero_to(at, end);
        end
    }

    /// Done once everything before `lsn` is durable; until then, the completion to wait on.
    pub fn wait_durable(self: &Arc<Self>, lsn: Lsn) -> Result<IOResult<()>> {
        if self.durable() >= lsn {
            return Ok(IOResult::Done(()));
        }
        let c = Completion::new_wait();
        {
            let mut f = self.flush.lock();
            if let Some(e) = f.failed {
                return Err(LimboError::CompletionError(e));
            }
            if self.durable() >= lsn {
                return Ok(IOResult::Done(()));
            }
            f.waiters.push((lsn, c.clone()));
            self.flush_wanted.store(true, Ordering::Release);
        }
        self.flush();
        if c.finished() {
            return match c.get_error() {
                Some(e) => Err(LimboError::CompletionError(e)),
                None => Ok(IOResult::Done(())),
            };
        }
        Ok(IOResult::IO(IOCompletions(c)))
    }

    /// Deletes the segments wholly before `lsn`, which nothing needs any more.
    pub fn release_before(&self, lsn: Lsn) -> Result<()> {
        let keep = self.segments.number(lsn.min(self.durable()));
        let mut removed = false;
        let mut n = keep;
        while n > 0 {
            n -= 1;
            if self.segments.open(n)?.is_none() {
                break;
            }
            self.segments.remove(n)?;
            removed = true;
        }
        if removed {
            self.io.sync_directory(&self.segments.directory())?;
        }
        Ok(())
    }

    fn slot(&self, lsn: Lsn) -> (&Slot, usize) {
        let chunk = lsn / self.config.chunk_bytes;
        let slot = &self.ring[(chunk % self.ring.len() as u64) as usize];
        (slot, (lsn - chunk * self.config.chunk_bytes) as usize)
    }

    /// Copies `bytes` into the ring from `at`; returns where they end.
    fn copy_in(&self, mut at: Lsn, mut bytes: &[u8]) -> Lsn {
        while !bytes.is_empty() {
            let (slot, offset) = self.slot(at);
            let n = bytes.len().min(self.config.chunk_bytes as usize - offset);
            // SAFETY: inside the chunk; the bytes were reserved by this appender alone, and the
            // chunk's previous use is durable, so no write reads them.
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), slot.block.as_ptr().add(offset), n)
            };
            bytes = &bytes[n..];
            at += n as u64;
        }
        at
    }

    fn zero_to(&self, mut at: Lsn, end: Lsn) {
        while at < end {
            let (slot, offset) = self.slot(at);
            let n = ((end - at) as usize).min(self.config.chunk_bytes as usize - offset);
            // SAFETY: as in `copy_in`.
            unsafe { std::ptr::write_bytes(slot.block.as_ptr().add(offset), 0, n) };
            at += n as u64;
        }
    }

    fn count_copied(&self, mut at: Lsn, end: Lsn) {
        while at < end {
            let (slot, offset) = self.slot(at);
            let n = ((end - at) as usize).min(self.config.chunk_bytes as usize - offset);
            slot.copied.fetch_add(n as u64, Ordering::Release);
            at += n as u64;
        }
    }

    /// The position up to which every byte reserved since `from` has been copied in.
    fn complete_from(&self, from: Lsn, insert: Lsn) -> Lsn {
        let chunk_bytes = self.config.chunk_bytes;
        let ring = self.ring.len() as u64;
        let mut chunk = from / chunk_bytes;
        loop {
            let start = chunk * chunk_bytes;
            if start >= insert {
                return insert;
            }
            let end = (start + chunk_bytes).min(insert);
            let slot = &self.ring[(chunk % ring) as usize];
            let expected = (chunk / ring) * chunk_bytes + (end - start);
            if slot.copied.load(Ordering::Acquire) != expected {
                return start.max(from);
            }
            if end == insert {
                return insert;
            }
            chunk += 1;
        }
    }

    /// Writes padding from `at` to the end of its block, if nothing has been reserved past `at`.
    ///
    /// Padding moves every record admitted and not yet appended along with it. Admission checked
    /// that they fit before the end of the prepared segments from where the log stood then, so the
    /// padding is admitted like a record: if it would push them past that end, it waits for the
    /// next segment, whose preparation flushes again once done.
    fn pad(&self, at: Lsn) -> Pad {
        let to = at.next_multiple_of(BLOCK);
        {
            // No one is admitted while this holds.
            let _admission = self.admission.lock();
            if to + self.granted.load(Ordering::SeqCst) > self.prepared_end.load(Ordering::Acquire)
            {
                return Pad::Unprepared;
            }
            if self
                .insert
                .compare_exchange(at, to, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Pad::Raced;
            }
        }
        self.copy_in(at, &record::padding(at));
        self.count_copied(at, to);
        Pad::Padded(to)
    }

    /// Starts writing what is complete, as far as the writes allowed in flight go.
    fn flush(self: &Arc<Self>) {
        let mut writes = Vec::new();
        let mut unprepared = false;
        {
            let mut f = self.flush.lock();
            if f.failed.is_some() {
                return;
            }
            let wanted = f.waiters.iter().map(|w| w.0).max().unwrap_or(0);
            let admitting = self.admitters_waiting.load(Ordering::Acquire);
            let limit = if self.durable_writes {
                self.config.writes_in_flight
            } else {
                1
            };
            while f.writes.len() < limit {
                let start = f.submitted;
                let insert = self.insert.load(Ordering::Acquire);
                let complete = self.complete_from(start, insert);
                let mut end = complete - complete % BLOCK;
                let someone_waits = wanted > start || admitting;
                if end <= start {
                    // No whole block is ready. Pad the partial one if someone waits on it and
                    // every byte reserved so far is in.
                    if !(someone_waits && complete == insert && insert > start) {
                        break;
                    }
                    match self.pad(insert) {
                        Pad::Padded(padded) => end = padded,
                        // Someone reserved more meanwhile: look again.
                        Pad::Raced => continue,
                        Pad::Unprepared => {
                            unprepared = true;
                            break;
                        }
                    }
                } else if !someone_waits && end - start < self.config.chunk_bytes {
                    // Nobody waits: let a chunk's worth gather.
                    break;
                }
                let chunk_end = (start / self.config.chunk_bytes + 1) * self.config.chunk_bytes;
                let segment_end = self.segments.start(self.segments.number(start) + 1);
                let write_end = end.min(chunk_end).min(segment_end);
                f.writes.insert(start, (write_end, false));
                f.submitted = write_end;
                writes.push((start, write_end));
            }
        }
        for (start, end) in writes {
            self.submit_write(start, end);
        }
        if unprepared {
            self.prepare_ahead();
        }
    }

    fn submit_write(self: &Arc<Self>, start: Lsn, end: Lsn) {
        let segment = self.segments.number(start);
        let file = self.segments.file(segment);
        let (slot, offset) = self.slot(start);
        let buffer = slot.block.region(offset, (end - start) as usize);
        let me = self.clone();
        let len = end - start;
        let c = Completion::new_write(move |res| {
            let res = match res {
                Ok(n) if n as u64 == len => Ok(()),
                Ok(_) => Err(CompletionError::ShortWrite),
                Err(e) => Err(e),
            };
            me.written(start, end, segment, res);
        });
        let at = start - self.segments.start(segment);
        let submitted = if self.durable_writes {
            file.pwrite_durable(at, buffer, c)
        } else {
            file.pwrite(at, buffer, c)
        };
        if let Err(e) = submitted {
            self.fail(io_failure(&e));
        }
    }

    fn written(
        self: &Arc<Self>,
        start: Lsn,
        end: Lsn,
        segment: u64,
        res: std::result::Result<(), CompletionError>,
    ) {
        if let Err(e) = res {
            self.fail(e);
            return;
        }
        let (released, sync) = {
            let mut f = self.flush.lock();
            if f.failed.is_some() {
                return;
            }
            f.writes.get_mut(&start).expect("a write in flight").1 = true;
            debug_assert_eq!(f.writes[&start].0, end);
            let mut reached = None;
            while let Some((&s, &(e, done))) = f.writes.iter().next() {
                if !done {
                    break;
                }
                f.writes.remove(&s);
                reached = Some(e);
            }
            if self.durable_writes {
                match reached {
                    Some(e) => (self.durable_to(&mut f, e), None),
                    None => (Vec::new(), None),
                }
            } else {
                f.unsynced.insert(segment);
                if let Some(e) = reached {
                    f.written = e;
                }
                (Vec::new(), self.start_sync(&mut f))
            }
        };
        self.after_progress(released);
        if let Some((covers, files)) = sync {
            self.submit_sync(covers, files);
        }
        self.flush();
    }

    /// Moves the durable position to `end` and takes the waiters it releases.
    fn durable_to(&self, f: &mut Flush, end: Lsn) -> Vec<Completion> {
        self.durable.store(end, Ordering::Release);
        let (done, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut f.waiters)
            .into_iter()
            .partition(|(lsn, _)| *lsn <= end);
        f.waiters = waiting;
        if f.waiters.is_empty() && !self.admitters_waiting.load(Ordering::Acquire) {
            self.flush_wanted.store(false, Ordering::Release);
        }
        done.into_iter().map(|(_, c)| c).collect()
    }

    fn after_progress(&self, released: Vec<Completion>) {
        for c in released {
            c.complete(0);
        }
        self.wake_admitters();
    }

    /// The sync to start, if the completed writes are not all durable and none is in flight.
    fn start_sync(&self, f: &mut Flush) -> Option<(Lsn, Vec<u64>)> {
        if f.syncing || f.written <= self.durable() || f.unsynced.is_empty() {
            return None;
        }
        f.syncing = true;
        Some((
            f.written,
            std::mem::take(&mut f.unsynced).into_iter().collect(),
        ))
    }

    fn submit_sync(self: &Arc<Self>, covers: Lsn, segments: Vec<u64>) {
        let me = self.clone();
        let mut group = CompletionGroup::new(move |res| {
            me.synced(covers, res);
        });
        let syncs: Vec<_> = segments
            .iter()
            .map(|_| {
                let c = Completion::new_sync(|_| {});
                group.add(&c);
                c
            })
            .collect();
        let all = group.build();
        for (segment, c) in segments.iter().zip(syncs) {
            if let Err(e) = self.segments.file(*segment).sync_data(c) {
                all.abort();
                self.fail(io_failure(&e));
                return;
            }
        }
    }

    fn synced(self: &Arc<Self>, covers: Lsn, res: std::result::Result<i32, CompletionError>) {
        if let Err(e) = res {
            self.fail(e);
            return;
        }
        let (released, next) = {
            let mut f = self.flush.lock();
            f.syncing = false;
            let released = self.durable_to(&mut f, covers);
            (released, self.start_sync(&mut f))
        };
        self.after_progress(released);
        if let Some((covers, files)) = next {
            self.submit_sync(covers, files);
        }
        self.flush();
    }

    /// Wakes everyone waiting to be admitted, to try again.
    fn wake_admitters(&self) {
        let waiting = {
            let mut a = self.admission.lock();
            self.admitters_waiting.store(false, Ordering::Release);
            std::mem::take(&mut a.waiting)
        };
        for c in waiting {
            c.complete(0);
        }
    }

    /// Starts preparing the next segment when less than one is left ahead of what is admitted.
    fn prepare_ahead(self: &Arc<Self>) {
        let number = {
            let mut a = self.admission.lock();
            let prepared = self.prepared_end.load(Ordering::Acquire);
            let reach = self.insert.load(Ordering::SeqCst) + self.granted.load(Ordering::SeqCst);
            if a.preparing.is_some() || prepared.saturating_sub(reach) > self.segments.size {
                return;
            }
            let number = self.segments.number(prepared);
            a.preparing = Some(number);
            number
        };
        let file = match self.segments.create(number) {
            Ok(file) => file,
            Err(e) => return self.fail(io_failure(&e)),
        };
        let _ = file.allocate(0, self.segments.size);
        self.zero_fill(number, 0);
    }

    /// Writes zeros into segment `number` from `at`, one write after another, then syncs it and
    /// its directory.
    fn zero_fill(self: &Arc<Self>, number: u64, at: u64) {
        let file = self.segments.file(number);
        if at == self.segments.size {
            let me = self.clone();
            let c = Completion::new_sync(move |res| match res {
                Ok(_) => me.prepared(number),
                Err(e) => me.fail(e),
            });
            if let Err(e) = file.sync_data(c) {
                self.fail(io_failure(&e));
            }
            return;
        }
        let len = (self.segments.size - at).min(self.zeros.len() as u64);
        let me = self.clone();
        let c = Completion::new_write(move |res| match res {
            Ok(n) if n as u64 == len => me.zero_fill(number, at + len),
            Ok(_) => me.fail(CompletionError::ShortWrite),
            Err(e) => me.fail(e),
        });
        if let Err(e) = file.pwrite(at, self.zeros.region(0, len as usize), c) {
            self.fail(io_failure(&e));
        }
    }

    fn prepared(self: &Arc<Self>, number: u64) {
        if let Err(e) = self.io.sync_directory(&self.segments.directory()) {
            return self.fail(io_failure(&e));
        }
        self.prepared_end
            .store(self.segments.start(number + 1), Ordering::Release);
        self.admission.lock().preparing = None;
        self.wake_admitters();
        self.prepare_ahead();
        // A flush may be waiting to pad until this segment was ready.
        self.flush();
    }

    /// Whether the bytes in flight reached the disk is unknown: nothing may follow them, and
    /// everyone waiting is told.
    fn fail(&self, e: CompletionError) {
        let waiters = {
            let mut f = self.flush.lock();
            if f.failed.is_none() {
                tracing::error!("the log failed: {e}");
                f.failed = Some(e);
            }
            self.failed.store(true, Ordering::Release);
            std::mem::take(&mut f.waiters)
        };
        for (_, c) in waiters {
            c.error(e);
        }
        let admitting = {
            let mut a = self.admission.lock();
            self.admitters_waiting.store(false, Ordering::Release);
            std::mem::take(&mut a.waiting)
        };
        for c in admitting {
            c.error(e);
        }
    }

    /// The error the log failed with, if it did.
    pub fn failure(&self) -> Option<CompletionError> {
        if !self.failed.load(Ordering::Acquire) {
            return None;
        }
        self.flush.lock().failed
    }

    /// A reader of this database's log from `start`, for recovery and followers.
    pub fn reader(io: Arc<dyn IO>, base: &str, config: &LogConfig, start: Lsn) -> LogReader {
        LogReader::new(
            io.clone(),
            Arc::new(Segments::new(io, base, config.segment_bytes, config.direct)),
            start,
        )
    }
}

fn payload_len(payload: &[&[u8]]) -> usize {
    let len = payload.iter().map(|p| p.len()).sum();
    assert!(len <= MAX_PAYLOAD, "a {len} byte log record");
    len
}

fn io_failure(e: &LimboError) -> CompletionError {
    match e {
        LimboError::CompletionError(e) => *e,
        _ => CompletionError::IOError(std::io::ErrorKind::Other, "log IO submission"),
    }
}

#[cfg(test)]
mod tests;
