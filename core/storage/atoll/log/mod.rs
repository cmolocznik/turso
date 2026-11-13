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
//! **In place.** A record built in a block of its own, a transaction's chunk of changes (DESIGN.md
//! §11.4.1), is published where it lies ([`Log::publish`]): room is taken at the next block
//! boundary, the rest of the block before it padded in the ring, and the block handed to the log.
//! The flusher writes it from the block, never through the ring, and the write holds the block
//! until it lands; the ring's chunks over those positions are counted as copied and left alone.
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
//! writes landed raise a position past them in any order ([`Prefix`]). A flush and a sync each run
//! one at a time ([`solo`](super::solo)): whoever finds one under way leaves it the work, and it
//! looks again before it stops, so no request goes unanswered. Those waiting to be admitted or for
//! the log to be durable wait on take-all stacks ([`waits`]), looking again once they are on them,
//! so a waker either finds them or they see what it did.
//!
//! **Preparing a segment** (DESIGN.md 22.9 item 2) is the work of the log's preparer, one executor
//! task per open log ([`preparer`]), the only producer of every step of it; a word of its own says
//! whether a flush's padding waits for a segment ([`preparation`]). Callbacks only record: a
//! flush meeting such padding sets the word and triggers the preparer, which is due too once
//! admission comes within a segment of the prepared end. A wait on the preparer is the waiter's
//! value ([`LogWait`]): a demand of the relief class on it while it stands, and, where no worker
//! steps it, the waiter's own run of it (DESIGN.md 22.7). So a completion either call returns
//! means only: call again. Room is reported only by `admit` returning it, durability only by
//! `wait_durable` returning `Done`, and the log's failure only by either returning it.
//!
//! **Segments.** The log is cut into segment files ([`segment`]). A segment is written full of
//! zeros and synced, with its directory, before any record goes into it, so a later write changes
//! no file metadata and a commit costs one data write. Segments behind what is still needed are
//! deleted.
//!
//! **Failure.** A failed write or sync means nothing about the bytes in flight can be trusted: the
//! log refuses every append after it and fails everyone waiting.

mod preparation;
mod preparer;
mod reader;
mod record;
mod room;
mod segment;
mod waits;

pub use preparer::LogWait;
#[cfg(test)]
pub use reader::OwnedRecord;
pub use reader::{LogReader, ReadBack, Rereader};
pub use record::{framed_len, GroupStart, Header, Kind, Lsn, Place, BLOCK, MAX_PAYLOAD};

use super::aligned::AlignedBlock;
use super::buffer::BufferPool;
use super::once::SetOnce;
use super::page::SizeClass;
use super::prefix::Prefix;
use super::requests::{Counted, LogWork};
use super::solo::{Claim, Solo};
use super::{whole, whole_write};
use crate::io::{Completion, CompletionGroup, File, IO};
use crate::storage::atoll::executor::{self, rank, Class, Executor, Signal, TaskRef};
use crate::sync::atomic::{fence, AtomicBool, AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Weak};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use crossbeam_utils::CachePadded;
use preparation::Word;
use preparer::{PreparerTask, Told};
use room::{Pad, Room};
use segment::{Segments, Typed};
use waits::{Look, Waits};

#[cfg(test)]
crate::thread::thread_local! {
    /// Bytes copied into the ring on this thread: what a measurement counts per operation.
    pub(crate) static COPIED_IN: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Turns of the flush taken on this thread.
    pub(crate) static TURNS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Segments released on this thread.
    pub(crate) static SEGMENTS_RELEASED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Segment files opened by name on this thread ([`segment::Segments::open`]).
    pub(crate) static SEGMENTS_OPENED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Set by a test: the next turn taken on this thread asks for another while it runs, as
    /// another thread's flush would.
    pub(crate) static ASK_DURING_TURN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// The log's count of turns, on whatever thread, at the last turn on this thread that asked
    /// for another while it ran ([`ASK_DURING_TURN`]): the turns past it are the ones taken since.
    pub(crate) static ASKED_AT_TURN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The chunks of the ring of `config`: room for the admitted bytes, a padded block per write in
/// flight, and the chunk being flushed while the next fills.
fn ring_slots(config: &LogConfig) -> usize {
    let slack = config.writes_in_flight as u64 * BLOCK + 2 * config.chunk_bytes;
    (config.buffer_bytes + slack).div_ceil(config.chunk_bytes) as usize
}

/// Bytes of the pool's frames the ring of `config` takes, each chunk in a frame of the class
/// that fits it.
/// The class of the ring's chunk frames and how many: what the pool lends the ring as it opens.
pub fn ring_chunks(config: &LogConfig) -> Option<(SizeClass, u32)> {
    let class = SizeClass::fitting(config.chunk_bytes as usize)?;
    Some((class, ring_slots(config) as u32))
}

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

/// A record published in place: where it lies, in whole blocks, and the block it is written from.
struct Extent {
    start: Lsn,
    end: Lsn,
    block: Arc<AlignedBlock>,
}

struct Slot {
    block: Arc<AlignedBlock>,
    /// Bytes copied into this slot over all the chunks it has held.
    copied: AtomicU64,
}

pub struct Log {
    io: Arc<dyn IO>,
    segments: Segments<Typed>,
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
    /// Whether a record other than a checkpoint's own was reserved since the last checkpoint began
    /// ([`begin_input`](Self::begin_input)), recovery's tail counting as one: set by every such
    /// append after its reservation, read first and written only while clear, so the appends
    /// after the first only read it; on a line of its own (DESIGN.md 22.9, Due).
    input: CachePadded<AtomicBool>,
    /// Where the flush taking its turn writes from next: the turn's own.
    submitted: CachePadded<AtomicU64>,
    /// Writes submitted and not yet retired: a place is taken at submission and given back at the
    /// write's retirement ([`write_retired`](Self::write_retired)), never at its result's
    /// publication. Moved by every write twice, so on a line of its own.
    in_flight: CachePadded<AtomicUsize>,
    /// Asked for by every commit, so on a line of its own.
    flushes: CachePadded<Solo>,
    /// The sync in flight, without durable writes.
    sync: CachePadded<Claim>,
    /// The members of the sync in flight still to retire, and its submitter's hold: set by the
    /// holder of the sync's token as it submits a group, the last decrement letting the token go,
    /// so the one group at a time has the count to itself (ASYNC1 v3.8, a log sync record).
    sync_members: AtomicUsize,
    /// Whether a flush's padding waits for a segment, and whether the preparer prepares one.
    preparation: CachePadded<Word>,
    /// The log's preparer (DESIGN.md 22.9 item 2), named once the log is made, on the executor it
    /// was opened on.
    preparer: std::sync::OnceLock<TaskRef>,
    /// The preparer's trigger.
    due: Arc<Signal>,
    /// The prepared end the preparer was last made due for by an admission coming within a
    /// segment of it: each prepared end is promoted once, the admissions after the first reading
    /// this word alone (DESIGN.md 22.9 item 2).
    triggered_for: AtomicU64,
    /// The prepared end an admission waiting on the preparer needs, raised to the furthest: the
    /// preparer prepares until the prepared end reaches it, however far ahead of a segment that is
    /// (the auditor's P1-LOG-ADMIT1: an admission up to the buffer's size).
    wanted_end: AtomicU64,
    /// Let go by a caller once its run of the preparer has stepped it, where no worker steps it: a
    /// caller that found it stepped by another waits on this (DESIGN.md 22.7).
    stepped: crate::storage::atoll::notify::LetGo,
    /// Waiting for the log to be durable to a mark. Pushed onto by every commit that waits, so on a
    /// line of its own.
    durable_waits: CachePadded<Waits>,
    /// Waiting to be admitted, woken to try again whenever room may have been made: one
    /// registration they share.
    admitters: CachePadded<room::Admitters>,
    failed: AtomicBool,
    failure: SetOnce<CompletionError>,
    zeros: Arc<AlignedBlock>,
    /// Records published in place and not yet all submitted to be written, by where they lie.
    extents: crate::storage::atoll::locks::Mutex<std::collections::VecDeque<Extent>>,
    /// How many: a flush with none looks at no lock.
    extent_count: AtomicUsize,
    /// What its plans and units did, for the tests, on whichever thread ran them.
    #[cfg(test)]
    pub(crate) counts: Counts,
    /// The holds a test sets at named points of its plans and units, on whichever thread passes.
    #[cfg(test)]
    pub(crate) holds: Holds,
    /// What a test keeps with the log, dropped with it on whichever thread drops the log last.
    #[cfg(test)]
    pub(crate) keepsake: crate::storage::atoll::locks::Mutex<Option<Box<dyn std::any::Any + Send>>>,
}

/// The counts a test reads of a log's plans and units (D1b §4), atomics so that work on the IO
/// thread is seen, and the outcomes a test forces.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Counts {
    /// Each `copied` load in `complete_from`.
    pub(crate) chunk_loads: AtomicUsize,
    pub(crate) pads_to: AtomicUsize,
    pub(crate) pads_raced: AtomicUsize,
    /// The extents lock taken by a plan, and the extents a plan's cursor passed.
    pub(crate) extent_locks: AtomicUsize,
    pub(crate) extent_visits: AtomicUsize,
    pub(crate) writes: AtomicUsize,
    pub(crate) turns: AtomicUsize,
    /// The most members a sync group had.
    pub(crate) sync_members_max: AtomicUsize,
    /// `pad` returns `Raced` this many times first.
    pub(crate) force_raced: AtomicUsize,
    /// This many turns, the next ones taken, each ask for another inside themselves.
    pub(crate) ask_in_turns: AtomicUsize,
}

/// A test's holds at named points of the log's plans and units: the thread that passes a point
/// first, the IO thread's unit or a caller's flush, tells the test it arrived and waits there until
/// let go. Unset, a point is one load.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Holds {
    armed: AtomicUsize,
    set: crate::storage::atoll::locks::Mutex<Vec<Hold>>,
}

/// A hold's point, the sender that tells the test it arrived, and the receiver it waits on.
#[cfg(test)]
type Hold = (
    &'static str,
    std::sync::mpsc::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

#[cfg(test)]
impl Holds {
    /// Holds the next thread to pass `point`: the receiver hears it arrive, and the sender lets it
    /// go on when it sends or drops.
    pub(crate) fn set(
        &self,
        point: &'static str,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (arrived_tx, arrived) = std::sync::mpsc::channel();
        let (go, go_rx) = std::sync::mpsc::channel();
        self.set.lock().push((point, arrived_tx, go_rx));
        self.armed.fetch_add(1, Ordering::Release);
        (arrived, go)
    }

    fn at(&self, point: &'static str) {
        if self.armed.load(Ordering::Acquire) == 0 {
            return;
        }
        let held = {
            let mut set = self.set.lock();
            set.iter()
                .position(|(p, ..)| *p == point)
                .map(|i| set.remove(i))
        };
        if let Some((_, arrived, go)) = held {
            self.armed.fetch_sub(1, Ordering::Release);
            let _ = arrived.send(());
            let _ = go.recv();
        }
    }
}

#[cfg(test)]
fn take_one(n: &AtomicUsize) -> bool {
    n.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_sub(1))
        .is_ok()
}

/// Space an operation may append into, taken from [`Log::admit`]; what it does not use goes back
/// when it is dropped.
pub struct Admitted {
    log: Arc<Log>,
    left: u64,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        if self.left > 0 {
            self.log.room.give_back(self.left);
            self.log.wake_admitters();
        }
    }
}

/// Where a write's bytes come from: the block of a record published in place and where in it the
/// write starts, or, for `None`, the ring.
type WriteSource = Option<(Arc<AlignedBlock>, usize)>;

/// A log opened alone, and the executor of its own that steps its preparer, with no worker, its
/// callers running it (DESIGN.md 22.7). The executor is the opener's, never the log's: a log owns
/// no executor and no thread, so whichever holder drops it last drops it lawfully, the IO thread's
/// unit included (D1b §3.2), and an executor's close is never made on that thread. The log goes
/// first, then the executor, as in close (22.12): a unit still holding the log meets the
/// executor's tasks closed, as it does between close's steps 2 and 4.
#[cfg(test)]
pub(crate) struct AloneLog {
    log: Arc<Log>,
    /// Held for its drop alone, after the log's.
    _executor: Executor,
}

#[cfg(test)]
impl std::ops::Deref for AloneLog {
    type Target = Arc<Log>;
    fn deref(&self) -> &Arc<Log> {
        &self.log
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
    ///
    /// Opened alone, the log has an executor of its own with no worker, which steps its preparer,
    /// its callers running it (DESIGN.md 22.7).
    #[cfg(test)]
    pub fn open(io: Arc<dyn IO>, base: &str, config: LogConfig, end: Lsn) -> Result<AloneLog> {
        // Its own counted IO, whose IO thread serves its follow-on work (D1b).
        let counted = super::requests::counted_for_test(io);
        let executor = Executor::new(counted.clone(), 0)?;
        let log = Self::open_with(counted, base, config, end, &executor, |len| {
            Ok(AlignedBlock::zeroed(len))
        })?;
        Ok(AloneLog {
            log,
            _executor: executor,
        })
    }

    /// [`open`](Self::open), its preparer a task of `executor`, whose workers then step it.
    #[cfg(test)]
    pub fn open_on(
        io: Arc<dyn IO>,
        base: &str,
        config: LogConfig,
        end: Lsn,
        executor: &Executor,
    ) -> Result<Arc<Self>> {
        let counted = super::requests::counted_for_test(io);
        Self::open_with(counted, base, config, end, executor, |len| {
            Ok(AlignedBlock::zeroed(len))
        })
    }

    /// The transaction slots, each with its read-back record, of a log [`open_async`] opens.
    ///
    /// [`open_async`]: Self::open_async
    #[cfg(test)]
    pub(crate) const ASYNC_SLOTS: usize = 2;

    /// [`open`](Self::open) in a database opened async (D2c): its counted IO with one IO thread,
    /// which queues what a cooperative frame asks, and [`ASYNC_SLOTS`](Self::ASYNC_SLOTS)
    /// transaction slots' read-back records; and an executor of its own with `workers` workers,
    /// which step its preparer when there are any.
    #[cfg(test)]
    pub(crate) fn open_async(
        io: Arc<dyn IO>,
        base: &str,
        config: LogConfig,
        workers: usize,
    ) -> Result<AloneLog> {
        let counted = Arc::new(super::requests::Counted::with_shape(
            io,
            Arc::new(super::requests::Requests::default()),
            super::requests::Shape {
                opened_async: true,
                io_threads: 1,
                meta_reads: 1,
                frames: 0,
                slots: Self::ASYNC_SLOTS,
            },
        )?);
        let executor = Executor::new(counted.clone(), workers)?;
        let log = Self::open_with(counted, base, config, 0, &executor, |len| {
            Ok(AlignedBlock::zeroed(len))
        })?;
        Ok(AloneLog {
            log,
            _executor: executor,
        })
    }

    /// [`open`](Self::open), its ring's chunks in frames `pool` lends for as long as the log is
    /// open, so the log's memory is the pool's (one memory, Clay's ruling of 2026-11-05), and its
    /// preparer a task of the database's `executor`.
    pub fn open_in(
        counted: Arc<Counted>,
        base: &str,
        config: LogConfig,
        end: Lsn,
        pool: &Arc<BufferPool>,
        executor: &Executor,
    ) -> Result<Arc<Self>> {
        Self::open_with(counted, base, config, end, executor, |len| {
            let class = SizeClass::fitting(len).ok_or_else(|| {
                LimboError::InternalError(format!("a log chunk of {len} bytes fits no page class"))
            })?;
            let mut block = pool.lend_ring(class)?;
            // Zeroed as the heap's are: the log pads with what a chunk holds.
            block.bytes_mut().fill(0);
            Ok(block)
        })
    }

    fn open_with(
        counted: Arc<Counted>,
        base: &str,
        config: LogConfig,
        end: Lsn,
        executor: &Executor,
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
        let io: Arc<dyn IO> = counted.clone();
        let segments = Segments::counted(counted, base, config.segment_bytes, config.direct);
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
            input: CachePadded::new(AtomicBool::new(true)),
            submitted: CachePadded::new(AtomicU64::new(start)),
            in_flight: CachePadded::new(AtomicUsize::new(0)),
            flushes: CachePadded::default(),
            sync: CachePadded::default(),
            sync_members: AtomicUsize::new(0),
            preparation: CachePadded::default(),
            preparer: std::sync::OnceLock::new(),
            due: Arc::new(Signal::default()),
            triggered_for: AtomicU64::new(0),
            wanted_end: AtomicU64::new(0),
            stepped: Default::default(),
            durable_waits: CachePadded::default(),
            admitters: CachePadded::default(),
            failed: AtomicBool::new(false),
            failure: SetOnce::default(),
            zeros: Arc::new(AlignedBlock::zeroed(config.chunk_bytes as usize)),
            extents: crate::storage::atoll::locks::Mutex::new(std::collections::VecDeque::new()),
            extent_count: AtomicUsize::new(0),
            #[cfg(test)]
            counts: Counts::default(),
            #[cfg(test)]
            holds: Holds::default(),
            #[cfg(test)]
            keepsake: crate::storage::atoll::locks::Mutex::new(None),
            segments,
            config,
        });
        log.account_resumed(start);
        // Its follow-on work is the dispatch's to serve, the log held weakly (D1b §5).
        let work: Weak<dyn LogWork> = Arc::downgrade(&log) as Weak<dyn LogWork>;
        log.segments.counted_io().attach_log(work);
        // The preparer, after the checkpoint in the order jobs wait on each other (DESIGN.md 22.4),
        // holding the log weakly: nothing the executor holds keeps the log alive.
        let task = executor.spawn_ranked(
            Class::Maintenance,
            rank::PREPARER,
            Box::new(PreparerTask::new(Arc::downgrade(&log), log.due.clone())),
            &[&log.due],
        );
        assert!(
            log.preparer.set(task).is_ok(),
            "a log given a second preparer"
        );
        Ok(log)
    }

    /// Readies the segment holding `start` to append into and removes every segment after it:
    /// they hold nothing of the log. Returns whether its writes are durable on completion.
    fn open_segments(
        io: &Arc<dyn IO>,
        segments: &Segments<Typed>,
        end: Lsn,
        start: Lsn,
    ) -> Result<bool> {
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
            crate::storage::atoll::park::wait_for_completion(io.as_ref(), zeroed)?;
            at += len;
        }
        crate::storage::atoll::park::wait_for_completion(
            io.as_ref(),
            file.sync_data(Completion::new_sync(|_| {}))?,
        )?;
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

    /// A record other than a checkpoint's own was reserved: the input mark set, if clear. After the
    /// reservation, both in the total order of the reservations and the mark: a checkpoint's begin
    /// that reads the end before this reservation clears the mark before this reads it.
    fn mark_input(&self) {
        if !self.input.load(Ordering::SeqCst) {
            self.input.store(true, Ordering::SeqCst);
        }
    }

    /// A checkpoint begins: the input mark cleared, then the end read, where the log ended as it
    /// began. A record whose reservation the read missed sets the mark again.
    pub fn begin_input(&self) -> Lsn {
        self.input.store(false, Ordering::SeqCst);
        self.room.end_ordered()
    }

    /// Whether a record other than a checkpoint's own was reserved since the last checkpoint
    /// began, or, before any, since the log was opened.
    pub fn input_since(&self) -> bool {
        self.input.load(Ordering::SeqCst)
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

    #[cfg(test)]
    pub fn durable_writes(&self) -> bool {
        self.durable_writes
    }

    /// Room to append at most `bytes` (the sum of [`framed_len`] of the records), taken before an
    /// operation's first latch. Waits while the unflushed log or the prepared segments are too
    /// short for it; `wait`, the caller's across its calls, stands on the preparer while the
    /// prepared segments are.
    pub fn admit(self: &Arc<Self>, wait: &mut LogWait, bytes: u64) -> Result<IOResult<Admitted>> {
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
            wait.end();
            return self.hand_over(admitted());
        }
        // A call: it registers, flushes, waits on the preparer if the prepared segments refuse
        // it, then reads again. The completion it returns means only that it should be called
        // again. One left behind is finished by the next wake, for no one.
        let c = self.admitters.wait();
        self.flush_or_ring()?;
        let on = match self.room.unprepared_for(bytes) {
            Some(wanted) => {
                // The preparer prepares to where this admission fits, raised before its trigger.
                self.wanted_end.fetch_max(wanted, Ordering::SeqCst);
                self.preparer().trigger(&self.due);
                self.wait_on_preparer(wait)?
            }
            None => None,
        };
        if let Some(e) = self.failure() {
            return Err(LimboError::CompletionError(e));
        }
        if self.room.grant(bytes, true) {
            wait.end();
            return self.hand_over(admitted());
        }
        Ok(IOResult::IO(IOCompletions(on.unwrap_or(c))))
    }

    /// Hands granted room over, the preparer made due first if the room reaches within a segment
    /// of the prepared end, and run by this admission while it has more to do now where no worker
    /// steps it, as admissions ran the preparation before it was a task (DESIGN.md 22.7); then
    /// reads the log's failure again: a failed log hands over no room, and what was granted goes
    /// back.
    fn hand_over(self: &Arc<Self>, admitted: Admitted) -> Result<IOResult<Admitted>> {
        // One read of the prepared end for the test and the trigger: what the trigger promotes is
        // the end the admission found itself within a segment of.
        let end = self.room.prepared_end();
        if end.saturating_sub(self.room.reach()) <= self.segments.size {
            self.trigger_short(end);
            let preparer = self.preparer();
            // Only with work queued: an admission's room handed over takes no executor mutex
            // while the preparer waits on its request or rests. Never inside a step or a unit of
            // a job's work, which runs no other's step: there the next caller outside one runs it.
            if preparer.queued()
                && !executor::inside()
                && !crate::storage::atoll::unit::inside()
                && !preparer.driven()
            {
                if self.segments.counted_io().queues() {
                    drop(admitted);
                    return Err(Self::undriven_in_a_frame());
                }
                // What the run would have the caller wait on is no concern of an admission
                // granted: the next caller runs the preparer on.
                let _more = self.run_preparer();
                #[cfg(test)]
                if _more.is_some() {
                    preparer::GRANTED_MORE.with(|n| n.set(n.get() + 1));
                }
            }
        }
        if let Some(e) = self.failure() {
            drop(admitted);
            return Err(LimboError::CompletionError(e));
        }
        Ok(IOResult::Done(admitted))
    }

    /// Whether anyone waits to be admitted or for the log to be durable: an append then flushes.
    /// Across a fence from the copy the append just counted, pairing with the one a waiter passes
    /// between pushing its wait and flushing: either this finds the wait, or the waiter's flush
    /// finds the copy. Without it each can miss the other, and nobody flushes.
    fn flush_wanted(&self) -> bool {
        fence(Ordering::SeqCst);
        !self.durable_waits.is_empty() || self.admitters.waiting()
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
        self.append_marked(admitted, header, payload, reserved, true)
    }

    /// [`append_with`](Self::append_with) for a checkpoint's own record, which is no input to the
    /// log: it leaves the input mark as it stands.
    pub fn append_own(
        self: &Arc<Self>,
        admitted: &mut Admitted,
        header: &Header,
        payload: &[&[u8]],
    ) -> (GroupStart, Lsn) {
        self.append_marked(admitted, header, payload, |_| {}, false)
    }

    fn append_marked(
        self: &Arc<Self>,
        admitted: &mut Admitted,
        header: &Header,
        payload: &[&[u8]],
        reserved: impl FnOnce(GroupStart),
        input: bool,
    ) -> (GroupStart, Lsn) {
        let len = framed_len(payload_len(payload));
        let start = self.reserve(admitted, len);
        if input {
            self.mark_input();
        }
        reserved(GroupStart::at(start));
        let lsn = self.write_record(start, header, Place::ALONE, payload);
        self.count_copied(start, lsn);
        if self.flush_wanted() {
            self.flush_or_fail();
        }
        (GroupStart::at(start), lsn)
    }

    /// Publishes in place the record whose payload, `payload_len` bytes, lies in `block` past
    /// [`record::HEADER`]'s room (DESIGN.md §11.4.1), within `admitted`'s room, which must hold its framed
    /// length in whole blocks and a block more. It takes room at the next block boundary, pads the
    /// rest of the block before it in the ring, writes the record's header, CRC and padding to
    /// its last block's end into `block`, and hands `block` to the log, which writes it from there
    /// and lets it go once its write lands. `reserved` is handed where the record starts as
    /// [`append_with`](Self::append_with) hands it. Returns where it starts and its LSN.
    pub fn publish(
        self: &Arc<Self>,
        admitted: &mut Admitted,
        header: &Header,
        mut block: AlignedBlock,
        payload_len: usize,
        reserved: impl FnOnce(GroupStart),
    ) -> (GroupStart, Lsn) {
        assert!(
            payload_len <= MAX_PAYLOAD,
            "a {payload_len} byte log record"
        );
        let len = framed_len(payload_len);
        let whole = len.next_multiple_of(BLOCK);
        assert!(
            whole as usize <= block.len(),
            "a record of {len} bytes in a block of {}",
            block.len()
        );
        let (gap, start) = self.room.reserve_aligned(whole);
        self.mark_input();
        assert!(
            start - gap + whole <= admitted.left,
            "{} bytes published with {} bytes admitted",
            start - gap + whole,
            admitted.left
        );
        admitted.left -= start - gap + whole;
        reserved(GroupStart::at(start));
        if gap < start {
            self.copy_in(gap, &record::padding(gap));
            self.count_copied(gap, start);
        }
        let bytes = block.bytes_mut();
        let head = record::header_bytes(header, Place::ALONE, payload_len);
        bytes[..record::HEADER].copy_from_slice(&head);
        let body = record::HEADER + payload_len;
        let crc = super::crc::append(super::crc::of(&start.to_le_bytes()), &bytes[..body]);
        bytes[body..body + 4].copy_from_slice(&crc.to_le_bytes());
        bytes[body + 4..len as usize].fill(0);
        if len < whole {
            let padding = record::padding(start + len);
            bytes[len as usize..whole as usize].copy_from_slice(&padding);
        }
        // In the queue before its bytes count as copied: a flusher that finds them copied finds
        // it there.
        {
            let mut extents = self.extents.lock();
            let at = extents.partition_point(|e| e.start < start);
            extents.insert(
                at,
                Extent {
                    start,
                    end: start + whole,
                    block: Arc::new(block),
                },
            );
            self.extent_count.fetch_add(1, Ordering::Release);
        }
        self.count_copied(start, start + whole);
        // Written at once, so its block goes back as soon as it can.
        self.flush_or_fail();
        (GroupStart::at(start), start + len)
    }

    /// Appends `records` as one group: contiguous in the log, and replayed by recovery all
    /// together or not at all, so a change spanning several pages survives a crash whole or not
    /// at all. Returns where the group starts and each record's LSN. As with
    /// [`append`](Self::append), the caller holds the latch of every page the records change.
    #[cfg(test)]
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
    #[cfg(test)]
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
        self.mark_input();
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
            self.flush_or_fail();
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

    /// Done once everything before `lsn` is durable, read from the durable position; until then,
    /// a completion that means only: call again. A call registers its wait, flushes, runs a step
    /// it finds runnable, then reads the position again.
    pub fn wait_durable(self: &Arc<Self>, wait: &mut LogWait, lsn: Lsn) -> Result<IOResult<()>> {
        Ok(self.wait_durable_turns(wait, lsn, false)?.0)
    }

    /// [`wait_durable`](Self::wait_durable) for a unit of a job's work (DESIGN.md 22.1), which
    /// takes at most one turn of the flush where `wait_durable` takes turns until none is asked
    /// for. Also says whether a turn is owed: one was asked for during this call's turn, which the
    /// turn's holder answers ([`Solo`]); the caller answers it with
    /// [`flush_one_turn`](Self::flush_one_turn) in its next unit.
    pub fn wait_durable_turn(
        self: &Arc<Self>,
        wait: &mut LogWait,
        lsn: Lsn,
    ) -> Result<(IOResult<()>, bool)> {
        self.wait_durable_turns(wait, lsn, true)
    }

    fn wait_durable_turns(
        self: &Arc<Self>,
        wait: &mut LogWait,
        lsn: Lsn,
        one: bool,
    ) -> Result<(IOResult<()>, bool)> {
        if self.durable() >= lsn {
            wait.end();
            return Ok((IOResult::Done(()), false));
        }
        if let Some(e) = self.failure() {
            return Err(LimboError::CompletionError(e));
        }
        if one && self.segments.counted_io().queues() {
            return Err(LimboError::InternalError(
                "a job's turn of the log's flush asked in a cooperative frame".to_string(),
            ));
        }
        self.wanted.fetch_max(lsn, Ordering::SeqCst);
        let c = match &wait.durability {
            // Still standing for the same mark: registered by an earlier call of this wait.
            Some((mark, registration)) if *mark == lsn && !registration.wait.finished() => {
                registration.wait.clone()
            }
            _ => {
                if let Some((_, registration)) = wait.durability.take() {
                    registration.give_up();
                }
                let Some(registration) =
                    self.durable_waits
                        .wait(lsn, || self.durable(), || self.look())
                else {
                    wait.end();
                    return Ok((IOResult::Done(()), false));
                };
                let c = registration.wait.clone();
                wait.durability = Some((lsn, registration));
                c
            }
        };
        let owed = if one {
            self.flush_one_turn()
        } else {
            self.flush_or_ring()?;
            false
        };
        // Padding waiting for a segment: the wait stands on the preparer.
        let on = if self.preparation.needed() {
            self.wait_on_preparer(wait)?
        } else {
            None
        };
        if let Some(e) = self.failure() {
            return Err(LimboError::CompletionError(e));
        }
        if self.durable() >= lsn {
            wait.end();
            return Ok((IOResult::Done(()), owed));
        }
        Ok((IOResult::IO(IOCompletions(on.unwrap_or(c))), owed))
    }

    /// Deletes the segments wholly before `lsn`, which nothing needs any more.
    #[cfg(test)]
    pub fn release_before(&self, lsn: Lsn) -> Result<()> {
        let mut n = self.kept_from(lsn);
        let mut removed = false;
        while n > 0 && self.release_segment(n - 1)? {
            n -= 1;
            removed = true;
        }
        if removed {
            self.sync_released()?;
        }
        Ok(())
    }

    /// The first segment kept for everything from `lsn` on, as far as the log is durable: those
    /// before it nothing needs any more, the newest of them first to go.
    pub fn kept_from(&self, lsn: Lsn) -> u64 {
        self.segments.number(lsn.min(self.durable()))
    }

    /// Releases segment `n`, made a spare or removed, if it is still there: whether it was. The
    /// segments before a released one were released before it.
    pub fn release_segment(&self, n: u64) -> Result<bool> {
        if self.segments.open(n)?.is_none() {
            return Ok(false);
        }
        self.segments.release(n)?;
        #[cfg(test)]
        SEGMENTS_RELEASED.with(|r| r.set(r.get() + 1));
        Ok(true)
    }

    /// Makes the segments released so far gone for good: the directory synced.
    pub fn sync_released(&self) -> Result<()> {
        self.io.sync_directory(&self.segments.directory())
    }

    fn slot(&self, lsn: Lsn) -> (&Slot, usize) {
        let chunk = lsn / self.config.chunk_bytes;
        let slot = &self.ring[(chunk % self.ring.len() as u64) as usize];
        (slot, (lsn - chunk * self.config.chunk_bytes) as usize)
    }

    /// Copies `bytes` into the ring from `at`; returns where they end.
    fn copy_in(&self, mut at: Lsn, mut bytes: &[u8]) -> Lsn {
        #[cfg(test)]
        COPIED_IN.with(|n| n.set(n.get() + bytes.len() as u64));
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
            #[cfg(test)]
            self.counts.chunk_loads.fetch_add(1, Ordering::Relaxed);
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
        #[cfg(test)]
        if take_one(&self.counts.force_raced) {
            self.counts.pads_raced.fetch_add(1, Ordering::Relaxed);
            return Pad::Raced;
        }
        let padded = self.room.pad(at);
        #[cfg(test)]
        match padded {
            Pad::To(_) => self.counts.pads_to.fetch_add(1, Ordering::Relaxed),
            Pad::Raced => self.counts.pads_raced.fetch_add(1, Ordering::Relaxed),
            Pad::Unprepared => 0,
        };
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

    /// A caller's flush (D2c §1): in place outside a cooperative frame, as ever; inside one, in a
    /// database opened async, the log's door rung, and its IO thread's unit takes the turns, so
    /// no caller in a frame submits a write. A ring refused, the database closing, is that error.
    fn flush_or_ring(self: &Arc<Self>) -> Result<()> {
        let counted = self.segments.counted_io();
        if counted.queues() {
            return counted.ring_log_from_frame();
        }
        self.flush();
        Ok(())
    }

    /// [`flush_or_ring`](Self::flush_or_ring) for an append that returns its position, not a
    /// `Result`: a refused ring fails the log, so every waiter on its durability and every
    /// admitter is told, and the next admission refuses (D2c §1).
    fn flush_or_fail(self: &Arc<Self>) {
        if self.flush_or_ring().is_err() {
            self.fail(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "the log's ring refused at a closed database",
            ));
        }
    }

    /// Starts writing what is complete, as far as the writes allowed in flight go: a turn at a
    /// time ([`Solo`]).
    fn flush(self: &Arc<Self>) {
        self.flushes.ask();
        while self.flush_turn() {}
    }

    /// Asks for a flush and takes at most one turn of it, for a unit of a job's work (DESIGN.md
    /// 22.1). Whether a turn is owed: this call took a turn and a request came during it. The
    /// holder of a turn answers the requests made during it ([`Solo`]), so a caller that stops
    /// after one turn takes the owed one in its next unit, never leaving it unanswered.
    pub fn flush_one_turn(self: &Arc<Self>) -> bool {
        self.flushes.ask();
        self.flush_turn() && self.flushes.asked()
    }

    /// How far the prepared segments reach past what is admitted, and a segment's size, for a test
    /// to bring an admission within a segment of the prepared end.
    #[cfg(test)]
    pub(crate) fn prepared_ahead(&self) -> (u64, u64) {
        (
            self.room.prepared_end().saturating_sub(self.room.reach()),
            self.segments.size,
        )
    }

    /// All the room left before the prepared end, granted straight from the room as no admission
    /// is, so that the next admission waits on the preparer, for a test.
    #[cfg(test)]
    pub(crate) fn hold_prepared_room(self: &Arc<Self>) -> Admitted {
        let left = self.room.prepared_end() - self.room.reach();
        assert!(self.room.grant(left, false), "the room left not granted");
        Admitted {
            log: self.clone(),
            left,
        }
    }

    /// All the room left within the buffer, granted straight from the room as no admission is, so
    /// that the next admission waits on a flush of what is appended, for a test.
    #[cfg(test)]
    pub(crate) fn hold_buffer_room(self: &Arc<Self>) -> Admitted {
        let left = self.config.buffer_bytes - (self.room.reach() - self.room.durable());
        assert!(
            self.room.grant(left, true),
            "the buffer's room left not granted"
        );
        Admitted {
            log: self.clone(),
            left,
        }
    }

    /// Whether anyone waits to be admitted, for a test.
    #[cfg(test)]
    pub(crate) fn admitting(&self) -> bool {
        self.admitters.waiting()
    }

    /// The requests of the database the log is in, for a test's look at its IO thread.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(crate) fn requests_for_test(&self) -> &Arc<super::requests::Requests> {
        self.segments.counted_io().requests()
    }

    /// The turns of the flush taken so far, on whatever thread, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(crate) fn turns_taken(&self) -> usize {
        self.counts.turns.load(Ordering::SeqCst)
    }

    /// Whether a turn of the flush is asked for and not taken, for a test.
    #[cfg(test)]
    pub(crate) fn flush_asked(&self) -> bool {
        self.flushes.asked()
    }

    /// One turn of the flush, if one is asked for and none is under way: whether it took one.
    fn flush_turn(self: &Arc<Self>) -> bool {
        let Some(turn) = self.flushes.turn() else {
            return false;
        };
        #[cfg(test)]
        {
            TURNS.with(|n| n.set(n.get() + 1));
            let turns = self.counts.turns.fetch_add(1, Ordering::Relaxed) + 1;
            if ASK_DURING_TURN.with(|a| a.replace(false)) {
                self.flushes.ask();
                ASKED_AT_TURN.with(|a| a.set(turns));
            }
            if take_one(&self.counts.ask_in_turns) {
                self.flushes.ask();
            }
        }
        let (writes, unprepared) = self.plan_writes();
        drop(turn);
        for (start, end, block) in writes {
            self.submit_write(start, end, block);
        }
        // Padding waits for a segment: the preparer's, made due by whoever sets the word first.
        if unprepared && self.preparation.need() {
            self.preparer().trigger(&self.due);
        }
        true
    }

    /// The writes to start, from where the last ended, and whether padding waits for a segment:
    /// for the flush under way. Bounded by the configuration alone (D1b §4): the horizon is
    /// captured once, at most `ring_slots` chunk loads, and never refreshed within the plan; at
    /// most one padding attempt, its padded block the plan's last; the in-place extents read only
    /// after the capture, locked once when there are any and passed once by a cursor; at most
    /// `writes_in_flight` writes.
    fn plan_writes(&self) -> (Vec<(Lsn, Lsn, WriteSource)>, bool) {
        let mut writes = Vec::new();
        if self.failed.load(Ordering::Acquire) {
            return (writes, false);
        }
        let wanted = self.wanted.load(Ordering::SeqCst);
        let admitting = self.admitters.waiting();
        let limit = if self.durable_writes {
            self.config.writes_in_flight
        } else {
            1
        };
        // No write can start: nothing is scanned or locked.
        if self.in_flight.load(Ordering::Acquire) >= limit {
            return (writes, false);
        }
        // The turn's own, handed from turn to turn by the turns themselves.
        let mut start = self.submitted.load(Ordering::Relaxed);
        let mut unprepared = false;
        #[cfg(test)]
        self.holds.at("plan.capture");
        let insert = self.room.end();
        let mut complete = self.complete_from(start, insert);
        // Only after the capture: an extent whose bytes the capture acquired was inserted and
        // counted before them, so a zero here means none lies inside it. The publisher takes the
        // lock only to insert, and nothing here waits while it is held.
        let mut extents = (self.extent_count.load(Ordering::Acquire) > 0).then(|| {
            #[cfg(test)]
            self.counts.extent_locks.fetch_add(1, Ordering::Relaxed);
            self.extents.lock()
        });
        let mut cursor = 0;
        while self.in_flight.load(Ordering::Acquire) < limit {
            let mut end = complete - complete % BLOCK;
            let someone_waits = wanted > start || admitting;
            if end <= start {
                // No whole block is ready. Pad the partial one if someone waits on it and every
                // byte reserved so far is in. A padded horizon is no longer the insert position,
                // so this is reached at most once.
                if !(someone_waits && complete == insert && insert > start) {
                    break;
                }
                // No extent lies in the partial block: one would be whole and complete.
                drop(extents.take());
                match self.pad(insert) {
                    Pad::To(padded) => {
                        complete = padded;
                        end = padded;
                        #[cfg(test)]
                        self.holds.at("plan.padded");
                    }
                    // Someone reserved more meanwhile: the next turn, asked for here, looks again.
                    Pad::Raced => {
                        self.flushes.ask();
                        break;
                    }
                    Pad::Unprepared => {
                        unprepared = true;
                        break;
                    }
                }
            }
            // A record published in place is written from its block, at once: it is one write
            // already, and its block goes back once it lands. The ring's writes stop at its start.
            // Each extent the cursor examines is one visit, the one it stops at included.
            let extent = extents.as_deref().and_then(|queue| loop {
                let e = queue.get(cursor)?;
                #[cfg(test)]
                self.counts.extent_visits.fetch_add(1, Ordering::Relaxed);
                if e.end > start {
                    break Some((e.start, e.end, e.block.clone()));
                }
                cursor += 1;
            });
            let in_place = extent.as_ref().is_some_and(|&(s, ..)| s <= start);
            if !someone_waits && !in_place && end - start < self.config.chunk_bytes {
                // Nobody waits: let a chunk's worth gather.
                break;
            }
            let chunk_end = (start / self.config.chunk_bytes + 1) * self.config.chunk_bytes;
            let segment_end = self.segments.start(self.segments.number(start) + 1);
            let (write_end, from) = match extent {
                Some((s, e, block)) if s <= start => {
                    let write_end = end.min(e).min(segment_end);
                    if write_end == e {
                        // Every extent up to this one is wholly submitted: let them go.
                        let queue = extents.as_mut().expect("an extent read under the lock");
                        while queue.front().is_some_and(|x| x.end <= e) {
                            queue.pop_front();
                            self.extent_count.fetch_sub(1, Ordering::Release);
                            cursor = cursor.saturating_sub(1);
                        }
                    }
                    (write_end, Some((block, s)))
                }
                Some((s, ..)) => (end.min(chunk_end).min(segment_end).min(s), None),
                None => (end.min(chunk_end).min(segment_end), None),
            };
            self.in_flight.fetch_add(1, Ordering::AcqRel);
            #[cfg(test)]
            self.counts.writes.fetch_add(1, Ordering::Relaxed);
            writes.push((
                start,
                write_end,
                from.map(|(block, s)| (block, (start - s) as usize)),
            ));
            start = write_end;
        }
        self.submitted.store(start, Ordering::Relaxed);
        (writes, unprepared)
    }

    /// The hold a completion keeps on the log, weak: the completion waits in the log's own IO, so
    /// a strong hold would keep a dropped log, its ring and its files alive while it is in flight,
    /// and for good once nothing steps that IO. A write that lands after the log is gone leaves
    /// what a crash then would, and its retirement rings nothing (D1b §6).
    fn held(self: &Arc<Self>) -> Weak<Self> {
        Arc::downgrade(self)
    }

    /// Rings the log's doorbell (D1b §3.1), from a retirement observer before its request is
    /// uncounted, or from the unit itself (the sync submitter's hold, D1b §2).
    fn ring(&self) {
        self.segments.counted_io().ring_log();
    }

    /// Writes `[start, end)`: from a record published in place, its block and where in it
    /// `start` lies, or from the ring. Its retirement, before it is uncounted, lets its place
    /// among the writes in flight go and rings the log's doorbell (D1b §2).
    fn submit_write(self: &Arc<Self>, start: Lsn, end: Lsn, from: WriteSource) {
        let segment = self.segments.number(start);
        let file = self.segments.file(segment);
        let buffer = match from {
            Some((block, offset)) => block.region(offset, (end - start) as usize),
            None => {
                let (slot, offset) = self.slot(start);
                slot.block.region(offset, (end - start) as usize)
            }
        };
        let me = self.held();
        let len = end - start;
        let c = Completion::new_write(move |res| {
            if let Some(me) = me.upgrade() {
                me.written(start, end, whole(len as usize, res));
            }
        });
        let at = start - self.segments.start(segment);
        let durable = self.durable_writes;
        let retired = self.held();
        let submitted = file.submit_then(
            c.clone(),
            |f| {
                if durable {
                    f.pwrite_durable(at, buffer, c)
                } else {
                    f.pwrite(at, buffer, c)
                }
            },
            move || {
                if let Some(me) = retired.upgrade() {
                    me.write_retired();
                }
            },
        );
        if let Err(e) = submitted {
            self.fail(io_failure(&e));
        }
    }

    /// A write's callback: its landing recorded, and with durable writes the durable position
    /// raised and its waiters woken. It submits and rings nothing (D1b §2).
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
        crate::race::point("log.written.landed");
        if self.durable_writes {
            self.room.raise_durable(self.landed.at());
            self.wake();
        }
    }

    /// A write retired, before it is uncounted: its place among those in flight goes, and the
    /// work that may now be owed rings the doorbell.
    fn write_retired(self: &Arc<Self>) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.ring();
        crate::race::point("log.write_retired.rang");
    }

    /// The unit's sync start (D1b §4 step 1), for files that do not write durably: one take of the
    /// sync claim; a group covering the landed bytes past the durable position, capped at `M`
    /// segments; or, nothing to sync, the claim released and owed reported if a write landed
    /// meanwhile. A claim held by a group is that group's: its last member rings at its release.
    fn sync_start(self: &Arc<Self>) -> bool {
        if !self.sync.take() {
            return false;
        }
        let durable = self.durable();
        let landed = self.landed.at();
        if landed > durable {
            let cap = self
                .segments
                .start(self.segments.number(durable) + self.sync_members_bound());
            self.submit_sync(durable, landed.min(cap));
            return false;
        }
        self.sync.release();
        self.landed.at() > self.durable()
    }

    /// The members a sync group has at most, `M = ceil(buffer_bytes / segment_bytes) + 1`: a
    /// bound by construction, since its covers is capped at `M` segments from the durable
    /// position's (D1b §4). Padding is granted outside the buffer (`Room::pad`), so the bytes
    /// landed and not durable can exceed the buffer, and no bound may rest on that.
    fn sync_members_bound(&self) -> u64 {
        self.config.buffer_bytes.div_ceil(self.segments.size) + 1
    }

    /// Syncs the segments the writes from `from` to `covers` went to, `covers` capped by
    /// [`sync_start`](Self::sync_start).
    fn submit_sync(self: &Arc<Self>, from: Lsn, covers: Lsn) {
        let me = self.held();
        let mut group = CompletionGroup::new(move |res| {
            if let Some(me) = me.upgrade() {
                me.synced(covers, res);
            }
        });
        let segments: Vec<u64> =
            (self.segments.number(from)..=self.segments.number(covers - 1)).collect();
        #[cfg(test)]
        self.counts
            .sync_members_max
            .fetch_max(segments.len(), Ordering::Relaxed);
        let syncs: Vec<_> = segments
            .iter()
            .map(|_| {
                let c = Completion::new_sync(|_| {});
                group.add(&c);
                c
            })
            .collect();
        drop(group.build());
        // The sync's token is let go when its last member retires, not when the group's callback
        // runs (ASYNC1 v3.8, a log sync record): the next sync then finds every record free. The
        // count holds one more, the submitter's, let go once every member is submitted, so no
        // member retiring at once, inside its submission, lets the token go with another still to
        // submit; a failed submission keeps it, the log failed. Set under the token, which the
        // last group's last decrement let go after it: no member of that group is left to count.
        self.sync_members.store(syncs.len() + 1, Ordering::Release);
        let mut members = segments.iter().zip(syncs);
        for (segment, c) in members.by_ref() {
            let retired = self.held();
            let member = c.clone();
            let submitted = self.segments.file(*segment).submit_then(
                c.clone(),
                |f| f.sync_data(c),
                move || {
                    if let Some(me) = retired.upgrade() {
                        me.sync_member_retired();
                    }
                },
            );
            if let Err(e) = submitted {
                // The log fails; the refused member and those never submitted finish here with
                // the refusal, and the group ends once the members a backend holds finish too:
                // its callback runs only with every member finished. The token stays held, as a
                // failed log syncs nothing more.
                self.fail(io_failure(&e));
                member.abort();
                for (_, unsent) in members {
                    unsent.abort();
                }
                return;
            }
        }
        self.sync_member_retired();
    }

    /// One of the sync's members retired, before it is uncounted, or its submitter let its hold
    /// go inside the unit: the last of them lets the sync's token go and, the log not failed,
    /// rings the doorbell, whose unit starts the next group (D1b §2).
    fn sync_member_retired(self: &Arc<Self>) {
        if self.sync_members.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        self.sync.release();
        if !self.failed.load(Ordering::Acquire) {
            self.ring();
        }
    }

    /// The group's callback: the durable position raised to `covers` and its waiters woken. It
    /// submits and rings nothing, and releases nothing: the claim is its members' (D1b §2).
    fn synced(self: &Arc<Self>, covers: Lsn, res: std::result::Result<i32, CompletionError>) {
        if let Err(e) = res {
            self.fail(e);
            return;
        }
        self.room.raise_durable(covers);
        self.wake();
    }

    /// Wakes those waiting for the durable position raised, and those waiting to be admitted.
    fn wake(&self) {
        self.durable_waits.wake(|| self.durable(), || self.look());
        self.wake_admitters();
    }

    /// What finishes every durable wait, read by a wake that put waits back: the log's failure.
    fn look(&self) -> Look {
        match self.failure() {
            Some(e) => Look::Fail(e),
            None => Look::Stay,
        }
    }

    /// Wakes everyone waiting to be admitted, to try again.
    fn wake_admitters(&self) {
        self.admitters.wake();
    }

    /// Whether a segment is to be prepared: less than a segment prepared ahead of what is
    /// admitted, or an admission waiting for a prepared end past the one published.
    fn short_of_segments(&self) -> bool {
        let end = self.room.prepared_end();
        end.saturating_sub(self.room.reach()) <= self.segments.size
            || self.wanted_end.load(Ordering::SeqCst) > end
    }

    /// Whether workers step the log's preparer: with none, whoever waits on it runs it.
    pub(crate) fn preparer_driven(&self) -> bool {
        self.preparer().driven()
    }

    /// The log's preparer.
    fn preparer(&self) -> &TaskRef {
        self.preparer
            .get()
            .expect("a log made without its preparer")
    }

    /// Makes the preparer due for `end`, the prepared end an admission came within a segment of,
    /// once per prepared end (DESIGN.md 22.9 item 2): the first admission past the line raises the
    /// word and notifies; those after it read the word alone. Keyed on the prepared end, which
    /// only rises, never on the reach, which falls as unused room is given back. The preparer,
    /// resting, looks itself before it rests, so an end it was not made due for is still
    /// prepared.
    fn trigger_short(&self, end: Lsn) {
        if self.triggered_for.load(Ordering::Acquire) < end
            && self.triggered_for.fetch_max(end, Ordering::AcqRel) < end
        {
            self.preparer().trigger(&self.due);
        }
    }

    /// A wait that stands on the preparer (DESIGN.md 22.4, 22.7). Made inside a step or a unit of
    /// a job's work, it names the preparer for whoever runs that step or unit to wait on
    /// (`Step::WaitOn`), the step's own class riding on it, since a step runs no other job's step
    /// and a unit holds no other. Otherwise `wait` holds a demand of the relief class on the
    /// preparer, and where no worker steps it, the caller runs it, a step a call
    /// ([`run_preparer`](Self::run_preparer)): what the caller then waits on, if not its own
    /// registration.
    fn wait_on_preparer(&self, wait: &mut LogWait) -> Result<Option<Completion>> {
        let preparer = self.preparer();
        if executor::inside() || crate::storage::atoll::unit::inside() {
            wait.on = Some(preparer.clone());
            return Ok(None);
        }
        if wait.demand.is_none() {
            wait.demand = Some(preparer.ticket(Class::Relief));
        }
        if preparer.driven() {
            return Ok(None);
        }
        // A frame never steps the preparer in place (D2c §0): an async database always has a
        // worker to drive it, so one undriven there is a broken open.
        if self.segments.counted_io().queues() {
            return Err(Self::undriven_in_a_frame());
        }
        Ok(self.run_preparer())
    }

    /// The error of a preparer found undriven by a caller in a cooperative frame, which never
    /// steps it in place (D2c §0).
    fn undriven_in_a_frame() -> LimboError {
        LimboError::InternalError(
            "the log's preparer has no worker driving it, found in a cooperative frame".to_string(),
        )
    }

    /// A caller's run of the preparer where no worker steps it (DESIGN.md 22.7), one step a call:
    /// claims it by name and steps it once on this thread, then says what the caller waits on: a
    /// yield, finished, where the preparer has more to do now, so the caller calls again (a
    /// blocking caller's loop, a cooperative poll's next poll) and steps it once more; the request
    /// the step waits on; or, another caller stepping it, that caller's let-go of it, registered
    /// first and then claimed once more, so a let-go between either finds the registration or is
    /// seen as a claim that steps. None: the preparer rests, or its task is over (the log failed,
    /// or its executor closed), and the caller waits on its own registration, finished by the
    /// failure or the publication it waits for. Whoever steps the preparer this way lets go after
    /// the step's end is published, so a caller that found it stepped finds it free when woken.
    pub(crate) fn run_preparer(&self) -> Option<Completion> {
        let preparer = self.preparer();
        let mut ran = preparer::claim(preparer);
        if ran.is_none() && !preparer.is_over() {
            let released = self.stepped.wait();
            ran = preparer::claim(preparer);
            if ran.is_none() {
                return (!preparer.is_over()).then_some(released);
            }
        }
        if ran.is_some() {
            self.stepped.let_go();
        }
        match ran? {
            Told::Again => Some(Completion::new_yield()),
            Told::Wait(c) => Some(c),
            Told::Rest => None,
        }
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
        // Its fence pairs with an admitter's: either this finds it, or it sees the failure.
        self.admitters.fail(e);
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

    /// A reader of this database's log's records back by where they start, for recovery.
    pub fn rereader(io: Arc<dyn IO>, base: &str, config: &LogConfig) -> Rereader {
        Rereader::new(
            io.clone(),
            Segments::new(io, base, config.segment_bytes, config.direct),
        )
    }

    /// Steps the read-back of the record outside any group that starts at `at` and ends at `end`, a
    /// part of a running transaction's changes, for the rollback of the transaction in `slot`: read
    /// through the log's own open segment files into `back`, its read returned as `IO` while it is
    /// in flight, never waited for in place (DESIGN.md 22.18). Inside a cooperative frame each read
    /// is queued in the slot's record for an IO thread (D2c §2), and a record its last read has not
    /// yet let go returns its rest wait instead; outside one, it is made in place. Its header and
    /// where its payload lies in `back` once read and checked. The log must be durable past it; the
    /// segments it lies in stay open while the transaction runs, since the log is held back for its
    /// parts ([`Transactions::held_from`]).
    ///
    /// [`Transactions::held_from`]: super::txn::Transactions::held_from
    pub fn read_record(
        &self,
        back: &mut ReadBack,
        slot: u16,
        at: GroupStart,
        end: Lsn,
    ) -> Result<IOResult<(Header, std::ops::Range<usize>)>> {
        let segments = &self.segments;
        let counted = segments.counted_io();
        let queued = counted.queues();
        back.step(segments, at.get(), end, |number, pos, c| {
            let file = segments.get(number).ok_or_else(|| {
                LimboError::InternalError(format!(
                    "log segment {number}, which a running transaction's part lies in, is not open"
                ))
            })?;
            if !queued {
                let file: Arc<dyn crate::io::File> = file;
                return Ok(IOResult::Done(file.pread(pos, c)?));
            }
            Ok(
                match counted.queue_slot_read(slot as usize, file, pos, c.clone())? {
                    IOResult::Done(()) => IOResult::Done(c),
                    IOResult::IO(wait) => IOResult::IO(wait),
                },
            )
        })
    }

    /// The group of this database's log that starts at `at`, read whole and checked: what a
    /// transaction's chain, or a superblock, names.
    #[cfg(test)]
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

impl LogWork for Log {
    /// One unit of the log's follow-on work (D1b §4), on the IO thread: the sync start, for files
    /// that do not write durably, then one flush turn. Whether more is owed.
    fn unit(self: Arc<Self>) -> bool {
        #[cfg(test)]
        self.holds.at("unit.started");
        if self.failed.load(Ordering::Acquire) {
            return false;
        }
        let synced = !self.durable_writes && self.sync_start();
        let flushed = self.flush_one_turn();
        #[cfg(test)]
        self.holds.at("unit.ended");
        synced || flushed
    }
}

/// A wait finished once no backend owns `request` (DESIGN.md 22.5, *Retirement is not
/// finishing*): the place its owner holds for it, the preparer's or a read-back's, is held until
/// then.
fn retirement(request: &Completion) -> Completion {
    let retired = crate::storage::atoll::notify::wait();
    let place = retired.clone();
    request.on_retired(move || place.complete(0));
    retired
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
#[cfg(all(test, not(any(shuttle, atoll_loom, miri))))]
mod unit_tests;
