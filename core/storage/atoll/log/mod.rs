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
//! same whether the backend completes inside a submission or later from `IO::step`.
//!
//! **No lock.** Admission and padding take room by compare-and-swap of one word ([`room`]). The
//! writes landed raise a position past them in any order ([`Prefix`]). A flush, a sync and a
//! segment's preparation each run one at a time ([`solo`](super::solo)): whoever finds one under
//! way leaves it the work, and it looks again before it stops, so no request goes unanswered.
//! Those waiting to be admitted or for the log to be durable wait on take-all stacks ([`waits`]),
//! looking again once they are on them, so a waker either finds them or they see what it did.
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
mod room;
mod segment;
mod waits;

pub use reader::{LogReader, OwnedRecord};
pub use record::{
    framed_len, GroupStart, Header, Kind, Lsn, Place, ALIGN, BLOCK, MAX_PAYLOAD, NO_GROUP,
};

use super::aligned::AlignedBlock;
use super::buffer::{BufferPool, LentUse};
use super::drive;
use super::once::SetOnce;
use super::page::SizeClass;
use super::prefix::Prefix;
use super::solo::{Claim, Solo};
use super::stack::Stack;
use super::{whole, whole_write};
use crate::io::{Completion, CompletionGroup, IO};
use crate::sync::atomic::{fence, AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use crate::sync::{Arc, Weak};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use crossbeam_utils::CachePadded;
use room::{Pad, Room};
use segment::Segments;
use waits::Waits;

/// The chunks of the ring of `config`: room for the admitted bytes, a padded block per write in
/// flight, and the chunk being flushed while the next fills.
fn ring_slots(config: &LogConfig) -> usize {
    let slack = config.writes_in_flight as u64 * BLOCK + 2 * config.chunk_bytes;
    (config.buffer_bytes + slack).div_ceil(config.chunk_bytes) as usize
}

/// Bytes of the pool's frames the ring of `config` takes, each chunk in a frame of the class
/// that fits it.
pub fn ring_bytes(config: &LogConfig) -> usize {
    let frame = SizeClass::fitting(config.chunk_bytes as usize).map_or(0, SizeClass::size);
    ring_slots(config) * frame
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Bytes per segment file, a multiple of [`BLOCK`]. At the 1 GB/s log rate Umbra measured
    /// (Freitag et al., VLDB 2022 §4), the default of 256 MiB lasts a quarter of a second.
    pub segment_bytes: u64,
    /// Bytes appended and not yet durable, past which new operations wait to be admitted. The
    /// default is what the device may be writing at once, `writes_in_flight` chunks: the ring
    /// holds them and the chunk filling while they land, in frames of the engine's one memory,
    /// so every byte more is a byte less of cache. A stall longer than those writes take makes
    /// appenders wait for the device, as a full page cache makes WAL's writers wait.
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
            buffer_bytes: 4 * (256 << 10),
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

pub struct Log {
    io: Arc<dyn IO>,
    segments: Segments,
    config: LogConfig,
    durable_writes: bool,
    ring: Box<[Slot]>,
    room: Room,
    /// Past every write landed, in whole blocks: durable, with durable writes. Raised by every
    /// write landing, so on a line of its own, as is every field below written often: an append
    /// reads the fields around them.
    landed: CachePadded<Prefix>,
    /// The furthest anyone has waited for the log to be durable to. Raised by every commit, so on
    /// a line of its own.
    wanted: CachePadded<AtomicU64>,
    /// Where the flush taking its turn writes from next: the turn's own.
    submitted: CachePadded<AtomicU64>,
    /// Writes submitted and not landed. Moved by every write and its landing, so on a line of its
    /// own.
    in_flight: CachePadded<AtomicUsize>,
    /// Asked for by every commit, so on a line of its own.
    flushes: CachePadded<Solo>,
    /// The sync in flight, without durable writes.
    sync: CachePadded<Claim>,
    /// The preparation of a segment under way.
    preparation: CachePadded<Claim>,
    /// Waiting for the log to be durable to a mark. Pushed onto by every commit that waits, so on a
    /// line of its own.
    durable_waits: CachePadded<Waits>,
    /// Waiting to be admitted, woken to try again whenever room may have been made.
    admitters: CachePadded<Stack<Completion>>,
    failed: AtomicBool,
    failure: SetOnce<CompletionError>,
    zeros: Arc<AlignedBlock>,
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
            self.log.room.give_back(self.left);
            self.log.wake_admitters();
        }
    }
}

impl Log {
    /// Where [`open`](Self::open) resumes appending to a log that ends at `end`: the next block
    /// boundary.
    pub fn resumes_at(end: Lsn) -> Lsn {
        end.next_multiple_of(BLOCK)
    }

    /// Opens the log of the database at `base` to append from `end`, where recovery found its
    /// valid records end ([`LogReader`]). A new log has `end` 0.
    ///
    /// Appending resumes at the next block boundary, and everything from there to the end of its
    /// segment is zeroed first, so no record a crash left beyond `end` can ever be read as part of
    /// the log again. The bytes between `end` and that boundary stay unreadable, so a reader
    /// starting before `end` stops there: the caller must not acknowledge anything appended until
    /// a checkpoint whose redo point is at or past [`Log::end`] is durable.
    pub fn open(io: Arc<dyn IO>, base: &str, config: LogConfig, end: Lsn) -> Result<Arc<Self>> {
        Self::open_with(io, base, config, end, |len| Ok(AlignedBlock::zeroed(len)))
    }

    /// [`open`](Self::open), its ring's chunks in frames `pool` lends for as long as the log is
    /// open, so the log's memory is the pool's (one memory, Clay's ruling of 2026-11-05).
    pub fn open_in(
        io: Arc<dyn IO>,
        base: &str,
        config: LogConfig,
        end: Lsn,
        pool: &Arc<BufferPool>,
    ) -> Result<Arc<Self>> {
        let lend_io = io.clone();
        Self::open_with(io, base, config, end, |len| {
            let class = SizeClass::fitting(len).ok_or_else(|| {
                LimboError::InternalError(format!("a log chunk of {len} bytes fits no page class"))
            })?;
            let mut block = drive(&lend_io, || pool.lend(class, LentUse::Log))?;
            // Zeroed as the heap's are: the log pads with what a chunk holds.
            block.bytes_mut().fill(0);
            Ok(block)
        })
    }

    fn open_with(
        io: Arc<dyn IO>,
        base: &str,
        config: LogConfig,
        end: Lsn,
        mut chunk: impl FnMut(usize) -> Result<AlignedBlock>,
    ) -> Result<Arc<Self>> {
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
        segments.adopt_spares()?;
        let start = Self::resumes_at(end);
        let current = segments.number(start);
        let durable_writes = Self::open_segments(&io, &segments, end, start)?;
        let slots = ring_slots(&config);
        let span = slots as u64 * config.chunk_bytes;
        let ring = (0..slots)
            .map(|_| {
                Ok(Slot {
                    block: Arc::new(chunk(config.chunk_bytes as usize)?),
                    copied: AtomicU64::new(0),
                })
            })
            .collect::<Result<_>>()?;
        let log = Arc::new(Self {
            io,
            durable_writes,
            ring,
            room: Room::new(start, segments.start(current + 1), config.buffer_bytes),
            // The writes in flight lie within the ring of the position, in whole blocks.
            landed: CachePadded::new(Prefix::new(start, span, BLOCK)),
            wanted: CachePadded::new(AtomicU64::new(0)),
            submitted: CachePadded::new(AtomicU64::new(start)),
            in_flight: CachePadded::new(AtomicUsize::new(0)),
            flushes: CachePadded::default(),
            sync: CachePadded::default(),
            preparation: CachePadded::default(),
            durable_waits: CachePadded::default(),
            admitters: CachePadded::default(),
            failed: AtomicBool::new(false),
            failure: SetOnce::default(),
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
            segments.release(later)?;
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
            let (write, zeroed) = whole_write(len as usize);
            drop(file.pwrite(at, zeros.region(0, len as usize), write)?);
            io.wait_for_completion(zeroed)?;
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
        self.room.end()
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
        self.room.durable()
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
        if let Some(e) = self.failure() {
            return Err(LimboError::CompletionError(e));
        }
        let admitted = || Admitted {
            log: self.clone(),
            left: bytes,
        };
        if self.room.grant(bytes, true) {
            self.prepare_ahead();
            return Ok(IOResult::Done(admitted()));
        }
        let c = Completion::new_wait();
        // A completion left behind is completed by the next wake, for no one.
        room::wait_on(&self.admitters, c.clone());
        if let Some(e) = self.failure() {
            return Err(LimboError::CompletionError(e));
        }
        if self.room.grant(bytes, true) {
            self.prepare_ahead();
            return Ok(IOResult::Done(admitted()));
        }
        self.prepare_ahead();
        self.flush();
        Ok(IOResult::IO(IOCompletions(c)))
    }

    /// Whether anyone waits to be admitted or for the log to be durable: an append then flushes.
    /// Across a fence from the copy the append just counted, pairing with the one a waiter passes
    /// between pushing its wait and flushing: either this finds the wait, or the waiter's flush
    /// finds the copy. Without it each can miss the other, and nobody flushes.
    fn flush_wanted(&self) -> bool {
        fence(Ordering::SeqCst);
        !self.durable_waits.is_empty() || !self.admitters.is_empty()
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
        self.append_with(admitted, header, payload, |_| {}).1
    }

    /// [`append`](Self::append), handing `reserved` where the record starts once its room is
    /// reserved and before its copy completes: the log is never durable past the record before
    /// `reserved` returns. Returns where the record starts and its LSN.
    pub fn append_with(
        self: &Arc<Self>,
        admitted: &mut Admitted,
        header: &Header,
        payload: &[&[u8]],
        reserved: impl FnOnce(GroupStart),
    ) -> (GroupStart, Lsn) {
        let len = framed_len(payload_len(payload));
        let start = self.reserve(admitted, len);
        reserved(GroupStart::at(start));
        let lsn = self.write_record(start, header, Place::ALONE, payload);
        self.count_copied(start, lsn);
        if self.flush_wanted() {
            self.flush();
        }
        (GroupStart::at(start), lsn)
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
        let (start, lsns) = self.append_group_with(admitted, records, |_| {});
        (start.get(), lsns)
    }

    /// [`append_group`](Self::append_group), handing `reserved` where the group starts as
    /// [`append_with`](Self::append_with) does.
    pub fn append_group_with(
        self: &Arc<Self>,
        admitted: &mut Admitted,
        records: &[(Header, &[&[u8]])],
        reserved: impl FnOnce(GroupStart),
    ) -> (GroupStart, Vec<Lsn>) {
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
        reserved(GroupStart::at(start));
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
        if self.flush_wanted() {
            self.flush();
        }
        (GroupStart::at(start), lsns)
    }

    /// Takes `len` bytes of `admitted`'s room at the log's end; returns where they start.
    fn reserve(&self, admitted: &mut Admitted, len: u64) -> Lsn {
        assert!(
            len <= admitted.left,
            "{len} bytes of records appended with {} bytes admitted",
            admitted.left
        );
        let start = self.room.reserve(len);
        admitted.left -= len;
        start
    }

    /// Copies a record into the ring at `start`; returns its LSN. Its CRC is taken over the bytes
    /// once they are in the ring, in one run (two where the record crosses a chunk), the value
    /// [`record::crc`] gives from the pieces.
    fn write_record(&self, start: Lsn, header: &Header, place: Place, payload: &[&[u8]]) -> Lsn {
        let payload_len = payload_len(payload);
        let head = record::header_bytes(header, place, payload_len);
        let mut at = self.copy_in(start, &head);
        for piece in payload {
            at = self.copy_in(at, piece);
        }
        let crc = self.crc_in(start, at);
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
        if let Some(e) = self.failure() {
            return Err(LimboError::CompletionError(e));
        }
        self.wanted.fetch_max(lsn, Ordering::SeqCst);
        let Some(c) = self.durable_waits.wait(lsn, || self.durable()) else {
            return Ok(IOResult::Done(()));
        };
        if let Some(e) = self.failure() {
            return Err(LimboError::CompletionError(e));
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
            self.segments.release(n)?;
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

    /// The CRC of the record copied into the ring from `start` to `end`, as [`record::crc`] takes
    /// it: its position, then its bytes.
    fn crc_in(&self, start: Lsn, end: Lsn) -> u32 {
        let mut crc = super::crc::of(&start.to_le_bytes());
        let mut at = start;
        while at < end {
            let (slot, offset) = self.slot(at);
            let n = ((end - at) as usize).min(self.config.chunk_bytes as usize - offset);
            // SAFETY: inside the chunk, in bytes this appender reserved alone and has copied in;
            // no write reads them before the copy is counted.
            let bytes = unsafe { std::slice::from_raw_parts(slot.block.as_ptr().add(offset), n) };
            crc = super::crc::append(crc, bytes);
            at += n as u64;
        }
        crc
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

    /// Pads from `at` to the end of its block, if nothing has been reserved past `at` and it
    /// pushes no record granted past the prepared segments ([`Room::pad`]); padding waiting for a
    /// segment flushes again once it is prepared.
    fn pad(&self, at: Lsn) -> Pad {
        let padded = self.room.pad(at);
        match padded {
            Pad::To(to) => {
                self.copy_in(at, &record::padding(at));
                self.count_copied(at, to);
            }
            // The room it held may have turned an admission away.
            Pad::Raced => self.wake_admitters(),
            Pad::Unprepared => {}
        }
        padded
    }

    /// Starts writing what is complete, as far as the writes allowed in flight go: a turn at a
    /// time ([`Solo`]).
    fn flush(self: &Arc<Self>) {
        self.flushes.ask();
        while let Some(turn) = self.flushes.turn() {
            let (writes, unprepared) = self.plan_writes();
            drop(turn);
            for (start, end) in writes {
                self.submit_write(start, end);
            }
            if unprepared {
                self.prepare_ahead();
            }
        }
    }

    /// The writes to start, from where the last ended, and whether padding waits for a segment:
    /// for the flush under way.
    fn plan_writes(&self) -> (Vec<(Lsn, Lsn)>, bool) {
        let mut writes = Vec::new();
        if self.failed.load(Ordering::Acquire) {
            return (writes, false);
        }
        let wanted = self.wanted.load(Ordering::SeqCst);
        let admitting = !self.admitters.is_empty();
        let limit = if self.durable_writes {
            self.config.writes_in_flight
        } else {
            1
        };
        // The turn's own, handed from turn to turn by the turns themselves.
        let mut start = self.submitted.load(Ordering::Relaxed);
        let mut unprepared = false;
        while self.in_flight.load(Ordering::Acquire) < limit {
            let insert = self.room.end();
            let complete = self.complete_from(start, insert);
            let mut end = complete - complete % BLOCK;
            let someone_waits = wanted > start || admitting;
            if end <= start {
                // No whole block is ready. Pad the partial one if someone waits on it and every
                // byte reserved so far is in.
                if !(someone_waits && complete == insert && insert > start) {
                    break;
                }
                match self.pad(insert) {
                    Pad::To(padded) => end = padded,
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
            self.in_flight.fetch_add(1, Ordering::AcqRel);
            writes.push((start, write_end));
            start = write_end;
        }
        self.submitted.store(start, Ordering::Relaxed);
        (writes, unprepared)
    }

    /// The hold a completion keeps on the log, weak: the completion waits in the log's own IO, so
    /// a strong hold would keep a dropped log, its ring and its files alive while it is in flight,
    /// and for good once nothing steps that IO. A write that lands after the log is gone leaves
    /// what a crash then would.
    fn held(self: &Arc<Self>) -> Weak<Self> {
        Arc::downgrade(self)
    }

    fn submit_write(self: &Arc<Self>, start: Lsn, end: Lsn) {
        let segment = self.segments.number(start);
        let file = self.segments.file(segment);
        let (slot, offset) = self.slot(start);
        let buffer = slot.block.region(offset, (end - start) as usize);
        let me = self.held();
        let len = end - start;
        let c = Completion::new_write(move |res| {
            if let Some(me) = me.upgrade() {
                me.written(start, end, whole(len as usize, res));
            }
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
        res: std::result::Result<(), CompletionError>,
    ) {
        if let Err(e) = res {
            self.fail(e);
            return;
        }
        self.landed.complete(start, end);
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        if self.durable_writes {
            self.room.raise_durable(self.landed.at());
            self.wake();
        } else {
            self.start_sync();
        }
        self.flush();
    }

    /// Starts syncing the writes landed and not yet durable, unless a sync is in flight, which
    /// starts the next when it ends.
    fn start_sync(self: &Arc<Self>) {
        while self.sync.take() {
            let durable = self.durable();
            let landed = self.landed.at();
            if landed > durable {
                return self.submit_sync(durable, landed);
            }
            self.sync.release();
            if self.landed.at() <= self.durable() {
                return;
            }
        }
    }

    /// Syncs the segments the writes from `from` to `covers` went to.
    fn submit_sync(self: &Arc<Self>, from: Lsn, covers: Lsn) {
        let me = self.held();
        let mut group = CompletionGroup::new(move |res| {
            if let Some(me) = me.upgrade() {
                me.synced(covers, res);
            }
        });
        let segments: Vec<u64> =
            (self.segments.number(from)..=self.segments.number(covers - 1)).collect();
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
        self.room.raise_durable(covers);
        self.sync.release();
        self.wake();
        self.start_sync();
        self.flush();
    }

    /// Wakes those waiting for the durable position raised, and those waiting to be admitted.
    fn wake(&self) {
        self.durable_waits.wake(|| self.durable());
        self.wake_admitters();
    }

    /// Wakes everyone waiting to be admitted, to try again.
    fn wake_admitters(&self) {
        room::wake(&self.admitters, |c| c.complete(0));
    }

    /// Starts preparing the next segment when less than one is left ahead of what is admitted.
    /// One preparation runs at a time ([`Claim`]): whoever finds one under way leaves it the work,
    /// and it looks again when it ends.
    fn prepare_ahead(self: &Arc<Self>) {
        let needed = |log: &Self| {
            log.room.prepared_end().saturating_sub(log.room.reach()) <= log.segments.size
        };
        // Read alone first, so an admission that needs no segment writes nothing shared: one
        // whose grant makes the need finds it here, and a holder letting go looks again.
        if !needed(self) {
            return;
        }
        while self.preparation.take() {
            if needed(self) {
                let number = self.segments.number(self.room.prepared_end());
                // A spare is whole already: renamed, it is ready once its directory is synced.
                match self.segments.reuse(number) {
                    Ok(Some(_)) => return self.prepared(number),
                    Ok(None) => {}
                    Err(e) => return self.fail(io_failure(&e)),
                }
                let file = match self.segments.create(number) {
                    Ok(file) => file,
                    Err(e) => return self.fail(io_failure(&e)),
                };
                let _ = file.allocate(0, self.segments.size);
                return self.zero_fill(number, 0);
            }
            self.preparation.release();
            if !needed(self) {
                return;
            }
        }
    }
    /// Writes zeros into segment `number` from `at`, one write after another, then syncs it and
    /// its directory. A write whose completion runs inside its own submission, as a memory IO's
    /// does, lets this loop go on to the next; one completed later resumes the fill from its
    /// completion. Either way no write's completion runs inside another's, so a segment of many
    /// chunks costs no frame per chunk.
    fn zero_fill(self: &Arc<Self>, number: u64, mut at: u64) {
        /// The submission has not returned, nor the completion run.
        const SUBMITTING: u8 = 0;
        /// The completion ran inside the submission: the loop goes on.
        const INLINE: u8 = 1;
        /// The submission returned with the write in flight: the completion goes on.
        const IN_FLIGHT: u8 = 2;
        /// The write failed: nobody goes on.
        const FAILED: u8 = 3;
        let file = self.segments.file(number);
        loop {
            if at == self.segments.size {
                let me = self.held();
                let c = Completion::new_sync(move |res| {
                    let Some(me) = me.upgrade() else { return };
                    match res {
                        Ok(_) => me.prepared(number),
                        Err(e) => me.fail(e),
                    }
                });
                if let Err(e) = file.sync_data(c) {
                    self.fail(io_failure(&e));
                }
                return;
            }
            let len = (self.segments.size - at).min(self.zeros.len() as u64);
            let state = Arc::new(AtomicU8::new(SUBMITTING));
            let seen = state.clone();
            let me = self.held();
            let c = Completion::new_write(move |res| {
                let Some(me) = me.upgrade() else { return };
                match whole(len as usize, res) {
                    Ok(()) => {
                        if seen
                            .compare_exchange(
                                SUBMITTING,
                                INLINE,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_err()
                        {
                            me.zero_fill(number, at + len);
                        }
                    }
                    Err(e) => {
                        seen.store(FAILED, Ordering::Release);
                        me.fail(e);
                    }
                }
            });
            if let Err(e) = file.pwrite(at, self.zeros.region(0, len as usize), c) {
                self.fail(io_failure(&e));
                return;
            }
            match state.compare_exchange(SUBMITTING, IN_FLIGHT, Ordering::AcqRel, Ordering::Acquire)
            {
                Err(INLINE) => at += len,
                _ => return,
            }
        }
    }

    fn prepared(self: &Arc<Self>, number: u64) {
        if let Err(e) = self.io.sync_directory(&self.segments.directory()) {
            return self.fail(io_failure(&e));
        }
        self.room.set_prepared_end(self.segments.start(number + 1));
        self.preparation.release();
        self.wake_admitters();
        self.prepare_ahead();
        // A flush may be waiting to pad until this segment was ready.
        self.flush();
    }

    /// Whether the bytes in flight reached the disk is unknown: nothing may follow them, and
    /// everyone waiting is told.
    fn fail(&self, e: CompletionError) {
        // The first failure is the one kept and told.
        let e = self.failure.set(e);
        if !self.failed.swap(true, Ordering::SeqCst) {
            tracing::error!("the log failed: {e}");
        }
        self.durable_waits.fail(e);
        // Pairs with the fence of an admitter: either this finds it, or it sees the failure.
        fence(Ordering::SeqCst);
        for c in self.admitters.take_all() {
            c.error(e);
        }
    }

    /// The error the log failed with, if it did.
    pub fn failure(&self) -> Option<CompletionError> {
        if !self.failed.load(Ordering::SeqCst) {
            return None;
        }
        self.failure.get()
    }

    /// A reader of this database's log from `start`, for recovery and followers.
    pub fn reader(io: Arc<dyn IO>, base: &str, config: &LogConfig, start: Lsn) -> LogReader {
        LogReader::new(
            io.clone(),
            Arc::new(Segments::new(io, base, config.segment_bytes, config.direct)),
            start,
        )
    }

    /// The group of this database's log that starts at `at`, read whole and checked: what a
    /// transaction's chain, or a superblock, names.
    pub fn read_group(
        io: Arc<dyn IO>,
        base: &str,
        config: &LogConfig,
        at: GroupStart,
    ) -> Result<Vec<OwnedRecord>> {
        LogReader::group_at(
            io.clone(),
            Arc::new(Segments::new(io, base, config.segment_bytes, config.direct)),
            at,
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
