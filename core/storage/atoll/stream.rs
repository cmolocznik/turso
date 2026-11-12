//! Streams (DESIGN.md §21): immutable batches of messages appended to partitions, the bytes as
//! appended their permanent copy.
//!
//! A partition is a log of its own (section 7) under its own base, whose records are batches
//! ([`BATCH`]). A producer builds a batch in a chunk the partition lends ([`Partition::batch`]);
//! publishing it ([`Partition::append`]) assigns its offsets under the partition's lock and takes
//! its room in the log in the same order, so offsets rise with log position. A batch of at least a
//! block is published in place, written from its chunk; a smaller one is copied into the ring.
//!
//! A batch's payload, as it lies on disk and as a reader hands it on:
//!
//! ```text
//! base offset: u64 | count: u32 | flags: u32 | first time: u64 | last time: u64 |
//! producer: u64 | sequence: u64 | messages | ends: count × u32
//! ```
//!
//! `ends` gives each message's end within the messages. Beside its segments a partition keeps a
//! small file of its own, two copies written in turn as the control file's superblocks are: the
//! first offset retained and where its batch starts, and the gaps a recovery left, which a reader
//! passes.

use super::aligned::AlignedBlock;
use super::log::{framed_len, Admitted, Header, Kind, Log, LogConfig, LogWait, Lsn, BLOCK};
use super::txn::changes::Chunks;
use super::whole_write;
use crate::io::{Completion, File, OpenFlags, IO};
use crate::storage::atoll::locks::{Mutex, RwLock};
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};

/// A batch's record.
pub const BATCH: Kind = Kind::new(1).unwrap();
/// Bytes of a batch's head.
pub const HEAD: usize = 48;
/// Where a batch's payload starts in its chunk: past its record's header.
const PAYLOAD: usize = 24;
/// Bytes a record takes past its payload: its CRC.
const CRC: usize = 4;

/// A batch being built, in a chunk its partition lent.
pub struct Batch {
    block: AlignedBlock,
    /// Bytes of messages in it.
    len: usize,
    /// Each message's end within the messages.
    ends: Vec<u32>,
    first: u64,
    last: u64,
    producer: u64,
    sequence: u64,
}

impl Batch {
    /// The most bytes of messages and ends it holds.
    fn room(&self) -> usize {
        self.block.len() - PAYLOAD - HEAD - CRC
    }

    /// Adds `message`, made at `time` (microseconds): `false`, adding nothing, if the batch has no
    /// room for it.
    pub fn push(&mut self, message: &[u8], time: u64) -> bool {
        let after = self.len + message.len() + 4 * (self.ends.len() + 1);
        if after > self.room() {
            return false;
        }
        let at = PAYLOAD + HEAD + self.len;
        self.block.bytes_mut()[at..at + message.len()].copy_from_slice(message);
        self.len += message.len();
        self.ends.push(self.len as u32);
        if self.ends.len() == 1 {
            self.first = time;
        }
        self.last = time;
        true
    }

    /// Names the batch's producer and its sequence there, by which a retry is told from a new
    /// batch.
    pub fn set_producer(&mut self, producer: u64, sequence: u64) {
        self.producer = producer;
        self.sequence = sequence;
    }

    /// Messages in it.
    pub fn count(&self) -> u32 {
        self.ends.len() as u32
    }

    fn payload_len(&self) -> usize {
        HEAD + self.len + 4 * self.ends.len()
    }

    /// Writes its head, as the batch at `base`, and its ends after its messages.
    fn seal(&mut self, base: u64) {
        let (count, first, last, producer, sequence) = (
            self.count(),
            self.first,
            self.last,
            self.producer,
            self.sequence,
        );
        let len = self.len;
        let bytes = self.block.bytes_mut();
        let head = &mut bytes[PAYLOAD..PAYLOAD + HEAD];
        head[0..8].copy_from_slice(&base.to_le_bytes());
        head[8..12].copy_from_slice(&count.to_le_bytes());
        head[12..16].copy_from_slice(&0u32.to_le_bytes());
        head[16..24].copy_from_slice(&first.to_le_bytes());
        head[24..32].copy_from_slice(&last.to_le_bytes());
        head[32..40].copy_from_slice(&producer.to_le_bytes());
        head[40..48].copy_from_slice(&sequence.to_le_bytes());
        let mut at = PAYLOAD + HEAD + len;
        for end in &self.ends {
            bytes[at..at + 4].copy_from_slice(&end.to_le_bytes());
            at += 4;
        }
    }

    fn payload(&self) -> &[u8] {
        &self.block.bytes()[PAYLOAD..PAYLOAD + self.payload_len()]
    }
}

/// A batch read back: its head and its messages, in the bytes as they lie in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBatch {
    payload: Vec<u8>,
}

impl ReadBatch {
    fn word(&self, at: usize) -> u64 {
        u64::from_le_bytes(self.payload[at..at + 8].try_into().unwrap())
    }

    /// Its first message's offset.
    pub fn base(&self) -> u64 {
        self.word(0)
    }

    pub fn count(&self) -> u32 {
        u32::from_le_bytes(self.payload[8..12].try_into().unwrap())
    }

    /// Its producer and the producer's sequence.
    pub fn producer(&self) -> (u64, u64) {
        (self.word(32), self.word(40))
    }

    /// Its messages, each with its offset, oldest first.
    pub fn messages(&self) -> impl Iterator<Item = (u64, &[u8])> {
        let count = self.count() as usize;
        let ends_at = self.payload.len() - 4 * count;
        let messages = &self.payload[HEAD..ends_at];
        let ends = &self.payload[ends_at..];
        let base = self.base();
        (0..count).map(move |i| {
            let end = u32::from_le_bytes(ends[4 * i..4 * i + 4].try_into().unwrap()) as usize;
            let start = match i {
                0 => 0,
                _ => u32::from_le_bytes(ends[4 * i - 4..4 * i].try_into().unwrap()) as usize,
            };
            (base + i as u64, &messages[start..end])
        })
    }

    /// The batch whose payload is `payload`, checked whole: `None` if its counts do not hold
    /// together.
    fn decode(payload: Vec<u8>) -> Option<Self> {
        let count = u32::from_le_bytes(payload.get(8..12)?.try_into().unwrap()) as usize;
        let ends_at = payload.len().checked_sub(4 * count)?;
        if ends_at < HEAD {
            return None;
        }
        let ends = &payload[ends_at..];
        let mut last = 0;
        for i in 0..count {
            let end = u32::from_le_bytes(ends[4 * i..4 * i + 4].try_into().unwrap()) as usize;
            if end < last {
                return None;
            }
            last = end;
        }
        (last == ends_at - HEAD).then_some(Self { payload })
    }
}

/// What a read found.
#[derive(Debug, PartialEq, Eq)]
pub enum Read {
    /// Whole batches from the one holding the offset asked for, oldest first.
    Batches(Vec<ReadBatch>),
    /// The offset is before the first retained: its batch is gone.
    Expired { first: u64 },
}

/// A publication in progress: step it with [`Partition::append`].
pub struct Append {
    batch: Option<Batch>,
    admitted: Option<Admitted>,
    /// Once published: its base offset, its count, and its record's LSN.
    published: Option<(u64, u32, Lsn)>,
    /// Its waits on the log: the batch's admission, then its durability.
    log_wait: LogWait,
}

impl Append {
    pub fn new(batch: Batch) -> Self {
        Self {
            batch: Some(batch),
            admitted: None,
            published: None,
            log_wait: LogWait::default(),
        }
    }
}

struct State {
    /// The offset the next batch takes.
    next: u64,
    /// A batch about every chunk of log, by base offset and where its record starts, oldest
    /// first: where a read begins.
    index: Vec<(u64, Lsn)>,
    /// Past the last batch indexed by a chunk of log: the next batch at or past it is indexed.
    index_from: Lsn,
    kept: Kept,
}

/// What a partition's own file holds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Kept {
    seq: u64,
    /// The first offset retained, and where its batch's record starts.
    first: u64,
    first_at: Lsn,
    /// Where the log ended at a recovery, and where it went on, in order.
    gaps: Vec<(Lsn, Lsn)>,
}

/// Bytes of one copy of a partition's own file.
const KEPT_SIZE: usize = 4096;
/// Gaps one copy holds: all that fit between its count and its checksum.
const MAX_GAPS: usize = (KEPT_SIZE - 40) / 16;
const KEPT_MAGIC: u64 = u64::from_le_bytes(*b"ATOLLSP1");

impl Kept {
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![0u8; KEPT_SIZE];
        out[0..8].copy_from_slice(&KEPT_MAGIC.to_le_bytes());
        out[8..16].copy_from_slice(&self.seq.to_le_bytes());
        out[16..24].copy_from_slice(&self.first.to_le_bytes());
        out[24..32].copy_from_slice(&self.first_at.to_le_bytes());
        out[32..36].copy_from_slice(&(self.gaps.len() as u32).to_le_bytes());
        for (i, &(end, on)) in self.gaps.iter().enumerate() {
            let at = 36 + 16 * i;
            out[at..at + 8].copy_from_slice(&end.to_le_bytes());
            out[at + 8..at + 16].copy_from_slice(&on.to_le_bytes());
        }
        let crc = super::crc::of(&out[..KEPT_SIZE - 4]);
        out[KEPT_SIZE - 4..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let word = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let crc = u32::from_le_bytes(bytes[KEPT_SIZE - 4..].try_into().unwrap());
        if word(0) != KEPT_MAGIC || crc != super::crc::of(&bytes[..KEPT_SIZE - 4]) {
            return None;
        }
        let n = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;
        if n > MAX_GAPS {
            return None;
        }
        let gaps = (0..n)
            .map(|i| (word(36 + 16 * i), word(44 + 16 * i)))
            .collect();
        Some(Self {
            seq: word(8),
            first: word(16),
            first_at: word(24),
            gaps,
        })
    }
}

/// A partition of a stream.
pub struct Partition {
    io: Arc<dyn IO>,
    base: String,
    config: LogConfig,
    log: super::log::AloneLog,
    chunks: Arc<Chunks>,
    file: Arc<dyn File>,
    state: Mutex<State>,
    /// Held shared by each read for as long as it reads, and exclusively while segments let go
    /// are deleted or kept as spares: a read never meets a segment gone or reused under it.
    segments: RwLock<()>,
}

impl Partition {
    /// Opens the partition whose files are named from `base`, creating it if it has none, and
    /// recovers what they hold: every whole batch from the first retained, its offsets following
    /// the last (DESIGN.md §21.5). A batch torn by a crash ends it; appending resumes at the next
    /// block, the gap recorded durably first.
    pub fn open(io: Arc<dyn IO>, base: &str, config: LogConfig) -> Result<Arc<Self>> {
        let file = io.open_file(
            &format!("{base}.stream"),
            OpenFlags::Create | OpenFlags::Direct,
            false,
        )?;
        let mut kept = match read_kept(&io, &file)? {
            Some(kept) => kept,
            // A new partition: its first copy is in charge, durable with the file's name, before
            // a batch can be appended, so a torn update of either copy leaves the other.
            None => {
                let kept = Kept::default();
                write_kept(&io, &file, &kept)?;
                io.sync_directory(&super::store::directory(base))?;
                kept
            }
        };
        let mut reader =
            Log::reader(io.clone(), base, &config, kept.first_at).following(&kept.gaps);
        let (mut next, mut index, mut index_from) = (kept.first, Vec::new(), 0);
        while let Some(r) = reader.next_record()? {
            if r.header.kind != BATCH {
                return Err(corrupt(
                    base,
                    &format!("a record of kind {:?}", r.header.kind),
                ));
            }
            let batch = ReadBatch::decode(r.payload)
                .ok_or_else(|| corrupt(base, &format!("a batch at {} does not decode", r.start)))?;
            if batch.base() != next {
                return Err(corrupt(
                    base,
                    &format!(
                        "the batch at {} has offset {}, not {next}",
                        r.start,
                        batch.base()
                    ),
                ));
            }
            if r.start >= index_from {
                index.push((batch.base(), r.start));
                index_from = r.start + config.chunk_bytes;
            }
            next += batch.count() as u64;
        }
        let end = reader.position();
        let log = Log::open(io.clone(), base, config.clone(), end)?;
        let restart = Log::resumes_at(end);
        if restart > end {
            if kept.gaps.len() == MAX_GAPS {
                return Err(LimboError::InternalError(format!(
                    "stream partition {base} holds {MAX_GAPS} gaps, all it has room for"
                )));
            }
            kept.gaps.push((end, restart));
            kept.seq += 1;
            write_kept(&io, &file, &kept)?;
        }
        // Free chunks kept: as many as the log's buffer holds unflushed, and one more.
        let chunks = Chunks::new(
            config.chunk_bytes as usize,
            config.buffer_bytes.div_ceil(config.chunk_bytes) as usize + 1,
        );
        Ok(Arc::new(Self {
            io,
            base: base.to_string(),
            config,
            log,
            chunks,
            file,
            segments: RwLock::new(()),
            state: Mutex::new(State {
                next,
                index,
                index_from,
                kept,
            }),
        }))
    }

    /// A batch to build, in a chunk the partition lends.
    pub fn batch(&self) -> Batch {
        Batch {
            block: self.chunks.take(),
            len: 0,
            ends: Vec::new(),
            first: 0,
            last: 0,
            producer: 0,
            sequence: 0,
        }
    }

    /// The offset the next batch takes.
    pub fn next_offset(&self) -> u64 {
        self.state.lock().next
    }

    /// Publishes `op`'s batch (DESIGN.md §21.1): its offsets assigned and its room taken in the
    /// log in one order, under the partition's lock, written from its chunk when it fills a block
    /// or more, else copied into the ring. Done, with its first offset and count, once the log is
    /// durable past it. Step it until done.
    pub fn append(&self, op: &mut Append) -> Result<IOResult<(u64, u32)>> {
        if op.published.is_none() {
            let batch = op.batch.as_ref().expect("a batch to publish");
            assert!(batch.count() > 0, "an empty batch");
            let payload = batch.payload_len();
            let in_place = framed_len(payload) >= BLOCK;
            if op.admitted.is_none() {
                // In place: its record in whole blocks, and the rest of the block before it.
                let room = match in_place {
                    true => framed_len(payload).next_multiple_of(BLOCK) + BLOCK,
                    false => framed_len(payload),
                };
                op.admitted = Some(match self.log.admit(&mut op.log_wait, room)? {
                    IOResult::Done(a) => a,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                });
            }
            let mut batch = op.batch.take().expect("a batch");
            let mut admitted = op.admitted.take().expect("room admitted");
            let header = Header {
                kind: BATCH,
                flags: 0,
                txn: 0,
                prev: None,
            };
            let mut state = self.state.lock();
            let base = state.next;
            let count = batch.count();
            batch.seal(base);
            let (at, lsn) = match in_place {
                true => self
                    .log
                    .publish(&mut admitted, &header, batch.block, payload, |_| {}),
                false => self
                    .log
                    .append_with(&mut admitted, &header, &[batch.payload()], |_| {}),
            };
            state.next += count as u64;
            if at.get() >= state.index_from {
                state.index.push((base, at.get()));
                state.index_from = at.get() + self.config.chunk_bytes;
            }
            drop(state);
            op.published = Some((base, count, lsn));
        }
        let (base, count, lsn) = op.published.expect("published");
        Ok(match self.log.wait_durable(&mut op.log_wait, lsn)? {
            IOResult::Done(()) => IOResult::Done((base, count)),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// Whole batches from the one holding `offset`, up to `bytes` of payload but at least that
    /// one, of what is durable (DESIGN.md §21.2): none if `offset` is past them.
    pub fn read(&self, offset: u64, bytes: usize) -> Result<Read> {
        // Before the state is read: retention past it waits to delete what this read reads.
        let _segments = self.segments.read();
        let (start, gaps, next) = {
            let state = self.state.lock();
            if offset < state.kept.first {
                return Ok(Read::Expired {
                    first: state.kept.first,
                });
            }
            let i = state.index.partition_point(|&(base, _)| base <= offset);
            let start = match i {
                0 => state.kept.first_at,
                _ => state.index[i - 1].1,
            };
            (start, state.kept.gaps.clone(), state.next)
        };
        let mut out = Vec::new();
        if offset >= next {
            return Ok(Read::Batches(out));
        }
        let durable = self.log.durable();
        let mut reader =
            Log::reader(self.io.clone(), &self.base, &self.config, start).following(&gaps);
        let mut read = 0;
        while let Some(r) = reader.next_record()? {
            if r.lsn > durable {
                break;
            }
            let batch = ReadBatch::decode(r.payload).ok_or_else(|| {
                corrupt(
                    &self.base,
                    &format!("a batch at {} does not decode", r.start),
                )
            })?;
            if batch.base() + batch.count() as u64 <= offset {
                continue;
            }
            read += batch.payload.len();
            out.push(batch);
            if read >= bytes {
                break;
            }
        }
        Ok(Read::Batches(out))
    }

    /// Lets go of every whole segment before the batch holding `offset` (DESIGN.md §21.4): the
    /// first offset retained, that batch's, recorded durably first, and the segments then
    /// deleted, once every read that read the state before is done. A read hands back copies, so
    /// none holds a segment past its return.
    pub fn retain_from(&self, offset: u64) -> Result<()> {
        let mut state = self.state.lock();
        if offset <= state.kept.first {
            return Ok(());
        }
        let offset = offset.min(state.next);
        let i = state.index.partition_point(|&(base, _)| base <= offset);
        let from = match i {
            0 => state.kept.first_at,
            _ => state.index[i - 1].1,
        };
        // The batch holding `offset`, or the next batch past the last: where reading starts now.
        let mut reader = Log::reader(self.io.clone(), &self.base, &self.config, from)
            .following(&state.kept.gaps);
        let (mut first, mut first_at) = (state.next, None);
        while let Some(r) = reader.next_record()? {
            let batch = ReadBatch::decode(r.payload).ok_or_else(|| {
                corrupt(
                    &self.base,
                    &format!("a batch at {} does not decode", r.start),
                )
            })?;
            if batch.base() + batch.count() as u64 > offset {
                first = batch.base();
                first_at = Some(r.start);
                break;
            }
        }
        let Some(first_at) = first_at else {
            // Nothing durable holds `offset` yet: nothing whole lies before it to let go.
            return Ok(());
        };
        let mut kept = state.kept.clone();
        kept.seq += 1;
        kept.first = first;
        kept.first_at = first_at;
        kept.gaps.retain(|&(_, on)| on > first_at);
        write_kept(&self.io, &self.file, &kept)?;
        state.kept = kept;
        state.index.retain(|&(_, at)| at >= first_at);
        drop(state);
        // Reads that read the state before this one hold what it lets go until they are done.
        let _segments = self.segments.write();
        self.log.release_before(first_at)
    }

    /// The first offset retained.
    pub fn first_offset(&self) -> u64 {
        self.state.lock().kept.first
    }

    /// The partition's log.
    pub fn log(&self) -> &Arc<Log> {
        &self.log
    }
}

fn corrupt(base: &str, what: &str) -> LimboError {
    LimboError::Corrupt(format!("stream partition {base}: {what}"))
}

/// The newest whole copy of a partition's own file; `None` for a new one.
fn read_kept(io: &Arc<dyn IO>, file: &Arc<dyn File>) -> Result<Option<Kept>> {
    let len = file.size()? as usize;
    if len == 0 {
        return Ok(None);
    }
    let block = Arc::new(AlignedBlock::zeroed(2 * KEPT_SIZE));
    let c = Completion::new_read(block.region(0, 2 * KEPT_SIZE), |res| res.err());
    crate::storage::atoll::park::wait_for_completion(io.as_ref(), file.pread(0, c)?)?;
    let bytes = block.bytes();
    let newest = (0..2)
        .filter(|i| len >= (i + 1) * KEPT_SIZE)
        .filter_map(|i| Kept::decode(&bytes[i * KEPT_SIZE..(i + 1) * KEPT_SIZE]))
        .max_by_key(|k| k.seq);
    match newest {
        Some(kept) => Ok(Some(kept)),
        // The first copy torn: a creation a crash cut short, before any batch could be appended.
        // Every later write goes to the other copy first, the file then two copies long.
        None if len <= KEPT_SIZE => Ok(None),
        None => Err(LimboError::Corrupt(
            "a stream partition's own file holds no whole copy".into(),
        )),
    }
}

/// Writes `kept` over the copy not in charge, durably.
fn write_kept(io: &Arc<dyn IO>, file: &Arc<dyn File>, kept: &Kept) -> Result<()> {
    let block = Arc::new(AlignedBlock::zeroed(KEPT_SIZE));
    // SAFETY: a fresh block of the copy's size, held alone.
    unsafe { std::ptr::copy_nonoverlapping(kept.encode().as_ptr(), block.as_ptr(), KEPT_SIZE) };
    let at = (kept.seq % 2) * KEPT_SIZE as u64;
    let (write, written) = whole_write(KEPT_SIZE);
    drop(file.pwrite(at, block.region(0, KEPT_SIZE), write)?);
    crate::storage::atoll::park::wait_for_completion(io.as_ref(), written)?;
    crate::storage::atoll::park::wait_for_completion(
        io.as_ref(),
        file.sync_data(Completion::new_sync(|_| {}))?,
    )?;
    Ok(())
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
