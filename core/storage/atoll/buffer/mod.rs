//! The buffer manager (DESIGN.md §9).
//!
//! - [`memory`]: one region of frames on huge pages, lent to size classes 2 MiB at a time.
//! - [`table`]: the hash table from page id to frame, the only translation.
//! - [`frame`]: frame headers: the latch, the page id, dirtiness and the replacement tracker.
//! - [`watt`]: write-aware timestamp tracking, which chooses what to evict.
//! - [`clean`]: writing dirty pages back through the page store.
//! - [`source`]: where a missing page comes from.
//!
//! **Finding a page.** A reference to a page may carry a frame hint beside the page id: the frame
//! it was last found in, plus one, with 0 for none. A hint is never trusted. Following it latches
//! the frame and checks that it holds the page; if it does, the access is a hit with no hash
//! lookup. If not, the hash table answers, and the hint is refreshed. Eviction knows nothing of
//! hints: it bumps the frame's version, and every hint to the frame goes stale.
//!
//! **Fixing a page** takes it optimistically (no lock, validated afterwards), shared, or
//! exclusively. A busy latch is a cooperative yield and a page being loaded is a wait on its read,
//! returned as `IOResult::IO`; the caller steps again, as everything in Turso does. No guard may be
//! held across such a return.
//!
//! **A full pool** makes room on the miss that finds it full: it evicts clean pages chosen by
//! WATT, starts writing back dirty ones, or takes a batch from a size class whose pages are worth
//! less per byte. If nothing can be freed at once, the miss waits on the next frame freed.
//!
//! **Draining a batch** evicts every page in it. A clean page goes at once; a dirty one is
//! written out: its frame is held until the write lands, then freed, and whoever wants the page
//! meanwhile waits on the write as on a read. A page that keeps changing therefore still leaves,
//! and no page ever moves to another frame (CONCURRENCY.md §6).
//!
//! **Failure.** A failed page write, like a failed log write (§7.5), stops the pool: every later
//! miss fails until the database is reopened and recovered.

pub(super) mod cell;
mod clean;
mod frame;
mod kept;
mod lent;
mod memory;
mod source;
mod staging;
#[cfg(test)]
mod sweep;
mod table;
mod temp;
mod watt;

#[cfg(test)]
mod tests;

pub use kept::KeptCopy;
#[cfg(test)]
pub use kept::KeptStats;
#[cfg(test)]
pub use lent::LentStats;
pub use lent::Use as LentUse;
pub use memory::MIN_BUDGET;

/// The smallest budget a database takes whose log's ring fits one batch, as every ring of
/// chunks up to 256 KiB with the default writes in flight does: [`MIN_BUDGET`] and that batch.
pub const MIN_DB_BUDGET: usize = MIN_BUDGET + memory::BATCH;

/// The smallest budget a pool takes when `lent` bytes of it are lent for as long as it is open
/// (the log's ring): [`MIN_BUDGET`] beside them, in whole batches, since a batch lent to a class
/// for them holds no other class's pages.
pub fn min_budget_beside(lent: usize) -> usize {
    MIN_BUDGET + lent.next_multiple_of(memory::BATCH)
}
pub use source::{LocalSource, Located, PageSource, ReadDone};

use super::aligned::AlignedBlock;
use super::blob;
use super::epoch::Step;
use super::latch::{Held, VersionedLatch};
use super::once::SetOnce;
#[cfg(test)]
use super::page::CLASS_COUNT;
use super::page::{self, PageId, SizeClass};
use super::shared::{Bytes, BytesMut, Shared, Writer};
use super::stack::Stack;
use super::store::PageStore;
use cell::{CompletionCell, Retiring};
use clean::{Progress, Writeback};
use frame::{
    FrameHeader, ABANDONED, EVICT_AFTER, FOR_GOOD, FREE, LENT, LOADING, NO_PAGE, OUTGOING,
    RESIDENT, WRITING,
};
use kept::KeptRoom;
use lent::Lends;
use memory::{Memory, Take, Zeroed};
use table::Table;
use temp::TempPages;

use crate::io::{Buffer, Completion};
use crate::storage::atoll::locks::Mutex;
use crate::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Weak};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::ops::Deref;

/// An exclusive hold on a frame's latch, borrowed from the pool.
type Hold<'a> = Held<&'a VersionedLatch>;

/// The frame headers and the IO in flight for frames: what a hold on a frame reaches. Apart from
/// the pool, so a hold the pool itself keeps (a write-out queued) does not keep the pool alive.
struct Frames {
    headers: Zeroed<FrameHeader>,
    /// Per 4 KiB grain: one past the epoch stamp at which a frame over it was last given up, or 0
    /// if none ever was. A page is read into a frame only once every step that could still read
    /// what its memory held has ended ([`BufferPool::quiet_at`]).
    given_up: Zeroed<AtomicU64>,
    /// Per 4 KiB grain: the IO in flight for a loading or outgoing frame that begins there. Put
    /// and taken by whoever holds the frame, read by anyone in a step.
    transits: Zeroed<CompletionCell>,
    /// Completions taken out of the transits, and out of the pool's released cell.
    retiring: Retiring,
}

impl Frames {
    /// Takes the IO in flight for `frame` out of its cell: for the frame's holder.
    fn end_transit(&self, frame: u32) -> Option<Completion> {
        self.transits[frame as usize].take(&self.retiring)
    }

    /// Leaves the page whose write-out did not happen in the frame `hold` holds, and sends
    /// whoever waits on the write to look again.
    fn kept(&self, hold: Held<FrameLatch>) {
        let frame = hold.latch().frame;
        let waiting = self.end_transit(frame);
        self.headers[frame as usize]
            .status
            .store(RESIDENT, Ordering::Release);
        drop(hold);
        if let Some(c) = waiting {
            c.complete(0);
        }
    }
}

/// A frame's latch, reached through the frames that keep it: for a hold that outlives a borrow of
/// the pool, a read or a write-out in flight.
struct FrameLatch {
    frames: Arc<Frames>,
    frame: u32,
}

impl Deref for FrameLatch {
    type Target = VersionedLatch;

    fn deref(&self) -> &VersionedLatch {
        &self.frames.headers[self.frame as usize].latch
    }
}

/// A page being read into a frame, which it holds exclusively until the read ends. Dropped
/// before then, when the read was never started or a panic unwinds, it takes the frame out of
/// the table and frees it, and whoever waits on the read looks again. The read's completion keeps
/// it, and it keeps the pool weakly: the completion waits in the pool's own IO, so a strong hold
/// would keep a pool dropped with the read in flight alive, and for good once nothing steps that
/// IO. The frame's memory is the read buffer's to keep.
struct Loading {
    pool: Weak<BufferPool>,
    pid: PageId,
    hold: Option<Held<FrameLatch>>,
}

impl Loading {
    fn end(mut self, res: std::result::Result<(), CompletionError>) {
        let hold = self.hold.take().expect("a read ends once");
        if let Some(pool) = self.pool.upgrade() {
            pool.loaded(self.pid, hold, res);
        }
    }
}

impl Drop for Loading {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        if let (Some(hold), Some(pool)) = (self.hold.take(), self.pool.upgrade()) {
            pool.unload(self.pid, hold, None);
        }
    }
}

/// A dirty page being written out of its frame, which it holds exclusively until the write ends.
/// Dropped before then, when the write was never submitted, the pool dropped it unwritten, or a
/// panic unwinds, it leaves the page in the frame, still dirty, and whoever waits on the write
/// looks again; whoever dropped it wakes the pool's waiters.
pub(super) struct Outgoing {
    pid: PageId,
    hold: Option<Held<FrameLatch>>,
}

impl Outgoing {
    pub(super) fn pid(&self) -> PageId {
        self.pid
    }

    pub(super) fn frame(&self) -> u32 {
        self.hold
            .as_ref()
            .expect("held until it ends")
            .latch()
            .frame
    }

    /// Ends the write-out with the write of a copy holding change count `count`.
    pub(super) fn end(
        mut self,
        pool: &BufferPool,
        count: u64,
        res: std::result::Result<(), CompletionError>,
    ) {
        let hold = self.hold.take().expect("a write-out ends once");
        pool.written_out(self.pid, hold, count, res);
    }
}

impl Drop for Outgoing {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        if let Some(hold) = self.hold.take() {
            let frames = hold.latch().frames.clone();
            frames.kept(hold);
        }
    }
}
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Bytes of the engine's memory, at least [`MIN_BUDGET`]: the frames that cache pages and
    /// every frame lent beside them ([`lent`]) for kept copies, checkpoint images of node pages
    /// and the log's ring. Frame headers take 2% more and the hash table 0.2%, and none of
    /// it takes physical memory until used.
    pub budget: usize,
    /// Bytes the copies of pages written back while they stay in their frames ([`staging`]) may
    /// take in flight: only a sweep makes them, as eviction writes a page out of its frame. At
    /// least the largest page.
    pub writeback_bytes: usize,
    /// Bytes of the copies a checkpoint's cut keeps of pages changed before it images them
    /// ([`kept`]), each in a lent frame: `None` for [`kept::default_limit`] of the budget. At least
    /// the largest page.
    pub kept_bytes: Option<usize>,
}

/// The default bytes of a sweep's copies in flight: two of the largest page, so a write of one
/// never waits behind another, which is far more than the 32 to 128 small writes in flight that
/// keep a device busy.
pub const WRITEBACK_BYTES: usize = 2 * page::MAX_PAGE_SIZE;

impl BufferConfig {
    /// A pool of `budget` bytes with the default shards and writeback buffer.
    pub fn with_budget(budget: usize) -> Self {
        Self {
            budget,
            writeback_bytes: WRITEBACK_BYTES,
            kept_bytes: None,
        }
    }
}

/// How a page is fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Optimistic,
    Exclusive,
}

/// What following a frame found.
/// A frame found holding a page: the version an optimistic fix saw, and an exclusive fix's hold.
struct Fixed<'a> {
    frame: u32,
    version: u64,
    hold: Option<Hold<'a>>,
}

enum Probe<'a> {
    /// The frame holds the page; the version an optimistic fix saw.
    Hit(u64),
    /// The frame holds the page, latched exclusively.
    Held(Hold<'a>),
    /// The frame holds the page, but its latch is held.
    Busy,
    /// The page is being read into the frame.
    Loading(Completion),
    /// The page is being written out of the frame.
    Outgoing(Completion),
    /// The frame does not hold the page.
    Miss,
}

pub struct BufferPool {
    memory: Memory,
    frames: Arc<Frames>,
    table: Table,
    source: Arc<dyn PageSource>,
    store: Arc<PageStore>,
    /// The current WATT epoch. Relaxed: it only dates accesses for eviction's estimates.
    epoch: AtomicU32,
    /// Bytes evicted since the pool opened; each quarter of the pool is an epoch. Relaxed: a count.
    evicted: AtomicU64,
    epoch_bytes: u64,
    /// Done when a frame is next freed, if anyone waits for that.
    released: CompletionCell,
    /// Writebacks queued and not stepped to their end: a stepping thread takes them all and puts
    /// back each one not done.
    writebacks: Stack<Writeback>,
    /// Writebacks queued and not done, those a stepping thread holds included.
    queued: AtomicUsize,
    /// Where copies of pages written back while they stay in their frames are made.
    staging: Arc<staging::Staging>,
    writes_in_flight: AtomicUsize,
    /// Bytes of swap copies written since the pool opened. Relaxed: a count.
    swap_written: AtomicU64,
    failure: SetOnce<CompletionError>,
    /// The checkpoint whose cut is in effect, 0 for none.
    cutting: AtomicU64,
    /// Pages that checkpoint found owed, kept as they stood by their first change or free since
    /// (the copy hook, audit 18).
    kept: Mutex<HashMap<PageId, KeptCopy>>,
    /// The frames lent beside the page table, and what for.
    lends: Arc<Lends>,
    /// The room the copies are charged to ([`kept`]).
    room: Arc<KeptRoom>,
    /// What a writer refused room drives: the checkpoint, once the database is open.
    relief: SetOnce<Arc<dyn Relief>>,
    /// The pages that checkpoint found owed and has neither imaged nor kept: one read in again is
    /// pending.
    cut_owed: Mutex<HashSet<PageId>>,
    temp: Arc<TempPages>,
    /// Each queued writeback's state after its last step, by its number, for a test that stalled
    /// to show while another thread steps it.
    #[cfg(test)]
    stepped: Mutex<HashMap<u64, String>>,
}

/// Why a request for kept copies was turned down.
enum Turned {
    /// The room is full.
    Room,
    /// No frame of the class is free without an eviction.
    Frame(SizeClass),
}

crate::thread::thread_local! {
    /// Frames lent to this thread for kept copies while it held no latch, after a request found
    /// none free: its next request takes them first.
    static SPARES: std::cell::RefCell<Vec<AlignedBlock>> = const { std::cell::RefCell::new(Vec::new()) };
    /// The class this thread's last request found no free frame of, if that turned it down.
    static SHORT: std::cell::Cell<Option<SizeClass>> = const { std::cell::Cell::new(None) };
    /// Requests turned down for a frame on this thread, for tests to see one reach a wait.
    #[cfg(test)]
    pub(crate) static FRAME_SHORTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// What a claimed frame is for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Purpose {
    /// A page read in by an IO, which writes the frame's memory plainly: only a quiet frame.
    Read,
    /// A new page, written through the frame's writer: any free frame.
    New,
}

/// What looking for a free frame found.
enum Got {
    Frame(u32),
    /// Free frames, none quiet yet; the earliest is quiet once the horizon reaches this epoch.
    Quieting(u64),
    /// No free frame.
    None,
}

/// The grains `frame` of `class` spans.
fn grains(frame: u32, class: SizeClass) -> std::ops::Range<usize> {
    let start = frame as usize;
    start..start + class.size() / page::MIN_PAGE_SIZE
}

fn yield_now<T>() -> Result<IOResult<T>> {
    Ok(IOResult::IO(IOCompletions(Completion::new_yield())))
}

fn io_failure(e: &LimboError) -> CompletionError {
    match e {
        LimboError::CompletionError(e) => *e,
        _ => CompletionError::IOError(std::io::ErrorKind::Other, "page write submission"),
    }
}

impl BufferPool {
    /// A pool reading missing pages from `source` and writing dirty ones to `store`.
    pub fn new(
        config: BufferConfig,
        source: Arc<dyn PageSource>,
        store: Arc<PageStore>,
    ) -> Result<Arc<Self>> {
        let kept_bytes = config
            .kept_bytes
            .unwrap_or_else(|| kept::default_limit(config.budget));
        if kept_bytes < kept::LARGEST_REQUEST {
            return Err(LimboError::InternalError(format!(
                "a room of {kept_bytes} bytes for kept copies holds no change's request, {} bytes",
                kept::LARGEST_REQUEST
            )));
        }
        let memory = Memory::new(config.budget)?;
        if config.writeback_bytes < page::MAX_PAGE_SIZE
            || !config.writeback_bytes.is_multiple_of(page::MIN_PAGE_SIZE)
        {
            return Err(LimboError::InternalError(format!(
                "a writeback buffer of {} bytes holds no copy of the largest page, {} bytes, or is \
                 not in whole 4 KiB",
                config.writeback_bytes,
                page::MAX_PAGE_SIZE
            )));
        }
        let grains = memory.grains();
        let lends = Arc::new(Lends::new(memory.region.clone()));
        let epoch_bytes =
            (memory.batches as u64 * memory::BATCH as u64) / watt::EPOCHS_PER_TURNOVER as u64;
        let pool = Arc::new(Self {
            frames: Arc::new(Frames {
                headers: Zeroed::new(grains as usize)?,
                given_up: Zeroed::new(grains as usize)?,
                transits: Zeroed::new(grains as usize)?,
                retiring: Retiring::new(store.epochs().clone()),
            }),
            table: Table::new(grains)?,
            memory,
            source,
            epoch: AtomicU32::new(watt::FIRST_EPOCH),
            evicted: AtomicU64::new(0),
            epoch_bytes,
            released: CompletionCell::default(),
            writebacks: Stack::default(),
            queued: AtomicUsize::new(0),
            staging: Arc::new(staging::Staging::new(config.writeback_bytes)),
            writes_in_flight: AtomicUsize::new(0),
            swap_written: AtomicU64::new(0),
            failure: SetOnce::default(),
            cutting: AtomicU64::new(0),
            kept: Mutex::new(HashMap::new()),
            lends,
            room: KeptRoom::new(kept_bytes, store.epochs().clone(), store.io().clone()),
            relief: SetOnce::default(),
            cut_owed: Mutex::new(HashSet::new()),
            temp: Arc::new(TempPages::new(
                store.io().clone(),
                store.base(),
                store.epochs().clone(),
            )?),
            store,
            #[cfg(test)]
            stepped: Mutex::new(HashMap::new()),
        });
        pool.lends.set_pool(&pool);
        Ok(pool)
    }

    fn header(&self, frame: u32) -> &FrameHeader {
        &self.frames.headers[frame as usize]
    }

    fn transit(&self, frame: u32) -> &CompletionCell {
        &self.frames.transits[frame as usize]
    }

    fn now(&self) -> u32 {
        self.epoch.load(Ordering::Relaxed)
    }

    fn failed(&self) -> Result<()> {
        match self.failure.get() {
            Some(e) => Err(LimboError::CompletionError(e)),
            None => Ok(()),
        }
    }

    /// Bytes each size class holds.
    #[cfg(test)]
    pub fn held(&self) -> [usize; CLASS_COUNT] {
        self.memory.held()
    }

    // ---------------------------------------------------------------- fixing

    /// Fixes `pid` without a lock: read it with [`Optimistic::read`], which says afterwards
    /// whether what was read is valid.
    pub fn fix_optimistic(
        self: &Arc<Self>,
        pid: PageId,
        hint: Option<&AtomicU32>,
    ) -> Result<IOResult<Optimistic<'_>>> {
        Ok(match self.fix_optimistic_held(pid, hint, &|| true)? {
            IOResult::Done(page) => IOResult::Done(page.expect("held without a holder")),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// Fixes `pid`, which a page `holder` names, without a lock: `None` once `holder` no longer
    /// names it. A page number is reused only after an epoch, but a reader holds `pid` across
    /// steps; so before reading it in, the fix checks in its own step that `holder` still names
    /// it. A number freed after the check is not reused before that step ends, and the read
    /// finds it free; one freed before fails the check. Either way no copy of a freed page stays
    /// in memory, where a new page under its number would meet it (CONCURRENCY.md §6).
    pub fn fix_optimistic_held(
        self: &Arc<Self>,
        pid: PageId,
        hint: Option<&AtomicU32>,
        holder: &dyn Fn() -> bool,
    ) -> Result<IOResult<Option<Optimistic<'_>>>> {
        let step = self.store.epochs().enter();
        Ok(match self.fix(pid, hint, Mode::Optimistic, holder)? {
            IOResult::Done(Some(Fixed { frame, version, .. })) => {
                IOResult::Done(Some(Optimistic {
                    pool: self,
                    frame,
                    pid,
                    version,
                    _step: step,
                }))
            }
            IOResult::Done(None) => IOResult::Done(None),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    pub fn fix_exclusive(
        self: &Arc<Self>,
        pid: PageId,
        hint: Option<&AtomicU32>,
    ) -> Result<IOResult<Exclusive<'_>>> {
        let step = self.store.epochs().enter();
        Ok(match self.fix(pid, hint, Mode::Exclusive, &|| true)? {
            IOResult::Done(Some(Fixed { frame, hold, .. })) => IOResult::Done(Exclusive {
                pool: self,
                frame,
                pid,
                hold: hold.expect("an exclusive fix holds the latch"),
                step,
            }),
            IOResult::Done(None) => unreachable!("held without a holder"),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// The frame holding `pid`, latched in `mode`, and the version an optimistic fix saw; `None`
    /// if `pid` is not in memory and `holder` no longer names it.
    fn fix(
        self: &Arc<Self>,
        pid: PageId,
        hint: Option<&AtomicU32>,
        mode: Mode,
        holder: &dyn Fn() -> bool,
    ) -> Result<IOResult<Option<Fixed<'_>>>> {
        loop {
            // Relaxed, read and written: a hint, the probe checks the frame it names.
            let hinted = hint
                .map(|h| h.load(Ordering::Relaxed))
                .filter(|&h| h != 0 && h <= self.memory.grains());
            let found = hinted.map(|h| (h - 1, true)).into_iter().chain(
                // The table, asked only if the hint missed.
                std::iter::from_fn(|| {
                    self.table
                        .find(&self.frames.headers, pid.raw())
                        .map(|f| (f, false))
                })
                .take(1),
            );
            for (frame, from_hint) in found {
                let (version, hold) = match self.probe(frame, pid, mode) {
                    Probe::Hit(version) => (version, None),
                    Probe::Held(hold) => (hold.version(), Some(hold)),
                    Probe::Busy => return yield_now(),
                    Probe::Loading(c) => return Ok(IOResult::IO(IOCompletions(c))),
                    Probe::Outgoing(c) => {
                        // Only stepping its writeback submits its write, and settles it once it
                        // landed. The write landing completes `c` and wakes the pool's waiters;
                        // a thread holding the writeback wakes them too if it leaves the write
                        // unsubmitted, waiting on its plan, and `c` then waits on nothing anyone
                        // does; and a write-out whose writeback is not queued yet has none to
                        // step, its queueing waking them. So the wait is the pool's, started
                        // before the look.
                        let woken = self.released_wait();
                        if let Some(w) = self.drive_writebacks()? {
                            return Ok(IOResult::IO(IOCompletions(w)));
                        }
                        if !c.finished() {
                            return Ok(IOResult::IO(IOCompletions(woken)));
                        }
                        let h = self.header(frame);
                        if h.status() == OUTGOING && h.pid() == pid.raw() {
                            // Landed, and its settle met a latched page or is someone else's.
                            return yield_now();
                        }
                        // Settled: the page left, so look for it anew.
                        continue;
                    }
                    Probe::Miss => continue,
                };
                if !from_hint {
                    if let Some(h) = hint {
                        h.store(frame + 1, Ordering::Relaxed);
                    }
                }
                self.header(frame).tracker.read(self.now());
                self.mark_held(frame);
                return Ok(IOResult::Done(Some(Fixed {
                    frame,
                    version,
                    hold,
                })));
            }
            // In this step, before anything is read.
            if !holder() {
                return Ok(IOResult::Done(None));
            }
            let located = match self.locate(pid)? {
                IOResult::Done(l) => l,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let frame = match self.claim(pid.class(), Purpose::Read)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let loaded = match self.load(pid, frame, located)? {
                IOResult::Done(loaded) => loaded,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            if let Some(loaded) = loaded {
                if let Some(h) = hint {
                    h.store(frame + 1, Ordering::Relaxed);
                }
                if !loaded.finished() {
                    return Ok(IOResult::IO(IOCompletions(loaded)));
                }
                if let Some(e) = loaded.get_error() {
                    return Err(LimboError::CompletionError(e));
                }
            }
            // Loaded already, or loaded by someone else first: find it again.
        }
    }

    /// Latches `frame` in `mode` if it holds `pid`.
    fn probe(&self, frame: u32, pid: PageId, mode: Mode) -> Probe<'_> {
        let h = self.header(frame);
        let Some(version) = h.latch.optimistic() else {
            // Held: being read in, written out, changed, evicted or freed.
            if h.pid() != pid.raw() {
                return Probe::Miss;
            }
            let status = h.status();
            if status == LOADING || status == OUTGOING {
                if let Some(c) = self.transit(frame).get(&self.store.epochs().enter()) {
                    return match status {
                        LOADING => Probe::Loading(c),
                        _ => Probe::Outgoing(c),
                    };
                }
            }
            return Probe::Busy;
        };
        let holds = h.status() == RESIDENT && h.pid() == pid.raw();
        if !h.latch.validate(version) {
            return Probe::Busy;
        }
        if !holds {
            return Probe::Miss;
        }
        match mode {
            Mode::Optimistic => Probe::Hit(version),
            // Only at the version it was seen to hold the page at: a frame that changed hands
            // meanwhile is never held, so no one ever holds a free frame.
            Mode::Exclusive => match Held::try_take_at(&h.latch, version) {
                Some(hold) => Probe::Held(hold),
                None => Probe::Busy,
            },
        }
    }

    /// Gets ready to read `pid`: from the temporary files, or from the page source.
    fn locate(&self, pid: PageId) -> Result<IOResult<Box<dyn Located>>> {
        if pid.is_temporary() {
            return Ok(IOResult::Done(Box::new(TempLocated {
                temp: self.temp.clone(),
                pid,
            })));
        }
        self.source.locate(pid)
    }

    /// Claims `frame` for `pid` and starts reading the page into it. `None` if another frame
    /// holds `pid` already. Otherwise the read's completion, done when the page is resident. A
    /// yield, with the frame given back, if the read could not start for a moment.
    fn load(
        self: &Arc<Self>,
        pid: PageId,
        frame: u32,
        located: Box<dyn Located>,
    ) -> Result<IOResult<Option<Completion>>> {
        let h = self.header(frame);
        let hold = Held::try_take(FrameLatch {
            frames: self.frames.clone(),
            frame,
        })
        .unwrap_or_else(|| panic!("free frame {frame} is held"));
        h.pid.store(pid.raw(), Ordering::Release);
        h.flags.store(0, Ordering::Release);
        h.tracker.clear();
        h.tracker.read(self.now());
        self.mark_held(frame);
        h.status.store(LOADING, Ordering::Release);
        let loaded = Completion::new_wait();
        self.transit(frame).put(loaded.clone());
        if self
            .table
            .insert(&self.frames.headers, pid.raw(), frame)
            .is_err()
        {
            self.frames.end_transit(frame);
            self.free_frame(pid.class(), frame, hold);
            // A reader may have followed a hint here meanwhile: send it to the other frame.
            loaded.complete(0);
            return Ok(IOResult::Done(None));
        }
        // The frame is quiet (claimed for a read) and held: nothing else reaches its memory while
        // the IO writes it.
        let into = self.memory.region.buffer(frame, pid.class().size());
        let reading = Loading {
            pool: Arc::downgrade(self),
            pid,
            hold: Some(hold),
        };
        // On an error or a yield the read never starts, and dropping it frees the frame.
        Ok(
            match located.read(into, Box::new(move |res| reading.end(res)))? {
                IOResult::Done(()) => IOResult::Done(Some(loaded)),
                IOResult::IO(io) => IOResult::IO(io),
            },
        )
    }

    /// Ends the read of `pid` into the frame `hold` holds.
    fn loaded(
        &self,
        pid: PageId,
        hold: Held<FrameLatch>,
        res: std::result::Result<(), CompletionError>,
    ) {
        let frame = hold.latch().frame;
        let h = self.header(frame);
        match res {
            Ok(()) => {
                let waiting = self.frames.end_transit(frame);
                let count = page::seq(self.bytes(frame, pid.class()));
                h.changes.store(count, Ordering::Release);
                h.written.store(count, Ordering::Release);
                // A page read from its swap copy differs from its checkpoint copy.
                let owed = !pid.is_temporary() && self.store.swap_slot(pid).is_some();
                h.imaged
                    .store(if owed { 0 } else { count }, Ordering::Release);
                // Evicted owed at a cut and not yet imaged: still the page as the cut found it.
                let n = self.cutting.load(Ordering::Acquire);
                let pending = n != 0 && owed && self.cut_owed.lock().contains(&pid);
                h.pending
                    .store(if pending { n } else { 0 }, Ordering::Release);
                h.status.store(RESIDENT, Ordering::Release);
                drop(hold);
                if let Some(c) = waiting {
                    c.complete(0);
                }
            }
            Err(e) => self.unload(pid, hold, Some(e)),
        }
    }

    /// Gives up the read of `pid` into the frame `hold` holds: the frame is unlinked and freed.
    /// Whoever waits gets `error`, or looks again without one.
    fn unload(&self, pid: PageId, hold: Held<FrameLatch>, error: Option<CompletionError>) {
        let frame = hold.latch().frame;
        let waiting = self.frames.end_transit(frame);
        self.table.remove(&self.frames.headers, pid.raw(), frame);
        self.free_frame(pid.class(), frame, hold);
        match (waiting, error) {
            (Some(c), Some(e)) => c.error(e),
            (Some(c), None) => c.complete(0),
            (None, _) => {}
        }
    }

    /// Latches a free frame exclusively. No one else ever holds a free frame: every other hold
    /// is taken at a version seen with the frame holding a page.
    fn take_free_latch(&self, frame: u32) -> Hold<'_> {
        Held::try_take(&self.header(frame).latch)
            .unwrap_or_else(|| panic!("free frame {frame} is held"))
    }

    /// Returns `frame`, unlinked, to the free frames, releasing `hold` on it.
    fn free_frame<L: Deref<Target = VersionedLatch>>(
        &self,
        class: SizeClass,
        frame: u32,
        hold: Held<L>,
    ) {
        let h = self.header(frame);
        h.status.store(FREE, Ordering::Release);
        h.pid.store(NO_PAGE, Ordering::Release);
        h.flags.store(0, Ordering::Release);
        drop(hold);
        // Stamped after the release: a reader that fixed the page before it entered its step
        // before the stamp, and one that fixes the frame after finds it free.
        let stamp = self.store.epochs().stamp();
        for grain in &self.frames.given_up[grains(frame, class)] {
            grain.store(stamp, Ordering::Release);
        }
        self.memory.put(class, frame);
        self.signal_released();
    }

    /// Marks the page in `frame` held by the step under way, with the global epoch now: at or above
    /// the step's announcement, so at or above the horizon while the step lasts. The mark only
    /// rises, so a step that entered earlier does not lower a later one's.
    fn mark_held(&self, frame: u32) {
        let now = self.store.epochs().current();
        let held = &self.header(frame).held;
        if held.load(Ordering::Relaxed) < now {
            held.fetch_max(now, Ordering::Relaxed);
        }
    }

    /// Whether no step that could still read what `frame`'s memory held before is left, given
    /// the epochs' `horizon`: then an IO may read a page into it. A page written through the
    /// frame's writer needs no such wait, its stores being atomic.
    #[cfg(test)]
    fn quiet(&self, frame: u32, class: SizeClass, horizon: u64) -> bool {
        self.quiet_at(frame, class) <= horizon
    }

    /// The epoch the horizon must reach for `frame`'s memory to be quiet: the latest stamp of the
    /// grains it spans.
    fn quiet_at(&self, frame: u32, class: SizeClass) -> u64 {
        self.frames.given_up[grains(frame, class)]
            .iter()
            .map(|g| g.load(Ordering::Acquire))
            .max()
            .unwrap_or(0)
    }

    /// The bytes of `frame`, of `class`'s size: read under its latch, or optimistically and
    /// validated afterwards.
    fn bytes(&self, frame: u32, class: SizeClass) -> &Shared {
        // SAFETY: frames are aligned to 4 KiB and sized in whole pages, and the region outlives
        // the pool. Its bytes are reached only as `Shared`, but by an IO: a read into a frame,
        // quiet and held while it runs (`Purpose::Read`), and a write out of a frame its write-out
        // holds, which only reads it, as readers do.
        unsafe { Shared::from_raw(self.memory.region.ptr(frame), class.size()) }
    }

    /// The writer of `frame`'s bytes.
    ///
    /// # Safety
    ///
    /// The caller holds the frame's latch exclusively while the writer lives.
    unsafe fn writer(&self, frame: u32, class: SizeClass) -> Writer<'_> {
        self.bytes(frame, class).writer()
    }

    // ---------------------------------------------------------------- new pages

    /// A new page of `class`, all zeros, fixed exclusively.
    pub fn allocate(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<Exclusive<'_>>> {
        let step = self.store.epochs().enter();
        let frame = match self.claim(class, Purpose::New)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let pid = match self.store.allocate(class) {
            Ok(IOResult::Done(pid)) => pid,
            Ok(IOResult::IO(io)) => {
                self.free_frame(class, frame, self.take_free_latch(frame));
                return Ok(IOResult::IO(io));
            }
            Err(e) => {
                self.free_frame(class, frame, self.take_free_latch(frame));
                return Err(e);
            }
        };
        Ok(IOResult::Done(self.install(frame, pid, step)))
    }

    /// Page `pid`, which must be free, taken and fixed exclusively as a new page
    /// ([`PageStore::allocate_at`]).
    pub fn allocate_at(self: &Arc<Self>, pid: PageId) -> Result<IOResult<Exclusive<'_>>> {
        let step = self.store.epochs().enter();
        let frame = match self.claim(pid.class(), Purpose::New)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        match self.store.allocate_at(pid) {
            Ok(IOResult::Done(())) => {}
            Ok(IOResult::IO(io)) => {
                self.free_frame(pid.class(), frame, self.take_free_latch(frame));
                return Ok(IOResult::IO(io));
            }
            Err(e) => {
                self.free_frame(pid.class(), frame, self.take_free_latch(frame));
                return Err(e);
            }
        }
        Ok(IOResult::Done(self.install(frame, pid, step)))
    }

    /// A new temporary page of `class` (DESIGN.md §6.4), all zeros, fixed exclusively. No
    /// checkpoint writes it, and recovery never reads it.
    pub fn allocate_temporary(
        self: &Arc<Self>,
        class: SizeClass,
    ) -> Result<IOResult<Exclusive<'_>>> {
        let step = self.store.epochs().enter();
        let frame = match self.claim(class, Purpose::New)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        Ok(IOResult::Done(self.install(
            frame,
            self.temp.allocate(class),
            step,
        )))
    }

    /// Every temporary page allocated and not freed.
    #[cfg(test)]
    pub fn temporary_pages(&self) -> Vec<PageId> {
        self.temp.allocated()
    }

    /// Puts the new page `pid`, all zeros, in the claimed `frame`, fixed exclusively.
    fn install<'a>(self: &'a Arc<Self>, frame: u32, pid: PageId, step: Step<'a>) -> Exclusive<'a> {
        let class = pid.class();
        let h = self.header(frame);
        let hold = self.take_free_latch(frame);
        // SAFETY: latched exclusively.
        unsafe { self.writer(frame, class) }.fill(0..class.size(), 0);
        h.pid.store(pid.raw(), Ordering::Release);
        h.flags.store(0, Ordering::Release);
        h.changes.store(0, Ordering::Release);
        h.written.store(0, Ordering::Release);
        h.imaged.store(0, Ordering::Release);
        h.pending.store(0, Ordering::Release);
        h.tracker.clear();
        h.tracker.read(self.now());
        self.mark_held(frame);
        h.status.store(RESIDENT, Ordering::Release);
        // A number is handed out again only once every step that could hold its last page is
        // gone (`epoch`), and the page left the table within its freer's step.
        if let Err(other) = self.table.insert(&self.frames.headers, pid.raw(), frame) {
            panic!("new page {pid:?} is already in frame {other}");
        }
        Exclusive {
            pool: self,
            frame,
            pid,
            hold,
            step,
        }
    }

    /// Drops the page `page` holds: its number becomes free, its copy's slot is freed, and its
    /// frame is free. On `IOResult::IO` the page is unlatched; fix it again and retry.
    ///
    /// A value's page freed while a cut is in effect is not freed yet: an image the cut owes may
    /// name the value as the cut found it, and reads its pages. It is handed to the checkpointer,
    /// named by nothing, and the next checkpoint frees it ([`PageStore::orphaned`]).
    pub fn free(self: &Arc<Self>, page: Exclusive<'_>) -> Result<IOResult<()>> {
        self.free_page(page, true)
    }

    fn free_page(
        self: &Arc<Self>,
        page: Exclusive<'_>,
        wait_out_a_cut: bool,
    ) -> Result<IOResult<()>> {
        let (frame, pid) = (page.frame, page.pid);
        if wait_out_a_cut
            && !pid.is_temporary()
            && self.cutting.load(Ordering::Acquire) != 0
            && page::kind(page.page()) == blob::KIND
        {
            page.release_unchanged();
            self.store.orphaned(pid);
            return Ok(IOResult::Done(()));
        }
        if self.header(frame).has(WRITING) {
            // Its writeback would name a swap copy of it: let the write land first.
            drop(page);
            return self.settle_writes().map(|r| match r {
                IOResult::Done(()) => IOResult::IO(IOCompletions(Completion::new_yield())),
                io => io,
            });
        }
        if !pid.is_temporary() {
            if !wait_out_a_cut {
                // An orphan the checkpoint left out: no image of its cut names it.
                self.left_out_of_cut(frame, pid);
            } else if self.try_keep(&[&page]).is_err() {
                // A checkpoint cut before the free still writes the page as it stood: with no
                // room for a copy, it is handed to the checkpointer as it stands, unchanged since
                // the cut, and the next checkpoint frees it. A free never waits.
                page.release_unchanged();
                self.store.orphaned(pid);
                return Ok(IOResult::Done(()));
            }
            if let IOResult::IO(io) = self.store.free(pid)? {
                return Ok(IOResult::IO(io));
            }
        }
        // Its latch is released by freeing the frame; its step ends last.
        let Exclusive { hold, step, .. } = page;
        self.table.remove(&self.frames.headers, pid.raw(), frame);
        self.free_frame(pid.class(), frame, hold);
        if pid.is_temporary() {
            // Once no frame holds it, so a new page under its number finds none.
            self.temp.free(pid);
        }
        drop(step);
        Ok(IOResult::Done(()))
    }

    /// Frees `pid`, which nothing refers to any more: a page an operation gave up on, or one a
    /// retired version held that no snapshot reads any more. A page not in memory is not read in:
    /// a frame is taken for it unread and held exclusively, so no fix can read it in while its
    /// number is freed.
    pub fn free_unreferenced(self: &Arc<Self>, pid: PageId) -> Result<IOResult<()>> {
        self.free_unreferenced_page(pid, true)
    }

    /// [`free_unreferenced`](Self::free_unreferenced), for the checkpointer's orphans, freed at
    /// once: named by nothing as of the cut under way, no image it owes names them.
    pub fn free_orphan(self: &Arc<Self>, pid: PageId) -> Result<IOResult<()>> {
        self.free_unreferenced_page(pid, false)
    }

    fn free_unreferenced_page(
        self: &Arc<Self>,
        pid: PageId,
        wait_out_a_cut: bool,
    ) -> Result<IOResult<()>> {
        let _step = self.store.epochs().enter();
        loop {
            // During a cut the page is read in: a checkpoint may owe its image, which its free
            // keeps.
            let cut = self.cutting.load(Ordering::Acquire) != 0 && !pid.is_temporary();
            if cut || self.table.find(&self.frames.headers, pid.raw()).is_some() {
                return match self.fix_exclusive(pid, None)? {
                    IOResult::Done(page) => self.free_page(page, wait_out_a_cut),
                    IOResult::IO(io) => Ok(IOResult::IO(io)),
                };
            }
            if pid.is_temporary() {
                // Nothing reads it in: nothing refers to it.
                self.temp.free(pid);
                return Ok(IOResult::Done(()));
            }
            let class = pid.class();
            let frame = match self.claim(class, Purpose::New)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let h = self.header(frame);
            let hold = self.take_free_latch(frame);
            // Whatever the frame held before must not read as this page if the free waits.
            // SAFETY: latched exclusively.
            unsafe { self.writer(frame, class) }.fill(0..page::HEADER_SIZE, 0);
            h.pid.store(pid.raw(), Ordering::Release);
            h.flags.store(0, Ordering::Release);
            h.changes.store(0, Ordering::Release);
            h.written.store(0, Ordering::Release);
            h.imaged.store(0, Ordering::Release);
            h.pending.store(0, Ordering::Release);
            h.tracker.clear();
            h.status.store(RESIDENT, Ordering::Release);
            if self
                .table
                .insert(&self.frames.headers, pid.raw(), frame)
                .is_err()
            {
                // Read in meanwhile: free it where it is.
                self.free_frame(class, frame, hold);
                continue;
            }
            return self.free_page(
                Exclusive {
                    pool: self,
                    frame,
                    pid,
                    hold,
                    step: self.store.epochs().enter(),
                },
                wait_out_a_cut,
            );
        }
    }

    /// Fixes `pid` without a lock if the frame `hint` names holds it, looking nowhere else: a
    /// hint that may be stale is followed at no cost when it is.
    pub fn fix_optimistic_at(self: &Arc<Self>, pid: PageId, hint: u32) -> Option<Optimistic<'_>> {
        if hint == 0 || hint > self.memory.grains() || self.header(hint - 1).has(ABANDONED) {
            return None;
        }
        match self.probe(hint - 1, pid, Mode::Optimistic) {
            Probe::Hit(version) => Some(Optimistic {
                pool: self,
                frame: hint - 1,
                pid,
                version,
                _step: self.store.epochs().enter(),
            }),
            _ => None,
        }
    }

    // ---------------------------------------------------------------- prefetching

    // ---------------------------------------------------------------- frames

    fn take_or_grow(&self, class: SizeClass, purpose: Purpose) -> Got {
        if purpose == Purpose::New {
            return self.take_or_grow_by(class, None);
        }
        // Most free frames are quiet by the horizon some scan found already: no scan.
        match self.take_or_grow_by(class, Some(self.store.epochs().known_horizon())) {
            Got::Quieting(_) => self.take_or_grow_by(class, Some(self.store.epochs().horizon())),
            got => got,
        }
    }

    /// A free frame of `class` whose memory is quiet by `horizon`, any free frame with none,
    /// growing the class if it has no free frame.
    fn take_or_grow_by(&self, class: SizeClass, horizon: Option<u64>) -> Got {
        // Of the frames not quiet, the epoch the earliest is quiet at.
        let soonest = std::cell::Cell::new(u64::MAX);
        loop {
            let usable = |f| match horizon {
                None => true,
                Some(h) => {
                    let at = self.quiet_at(f, class);
                    soonest.set(soonest.get().min(at));
                    at <= h
                }
            };
            match self.memory.take_where(class, usable) {
                Take::Frame(f) => return Got::Frame(f),
                // Growing would take memory a later step does not need: once the steps under
                // way end, these frames are quiet.
                Take::Unusable => return Got::Quieting(soonest.get()),
                Take::None => {
                    if !self.memory.grow(class) {
                        return Got::None;
                    }
                }
            }
        }
    }

    /// A free frame of `class` for `purpose`, making room if there is none.
    fn claim(self: &Arc<Self>, class: SizeClass, purpose: Purpose) -> Result<IOResult<u32>> {
        self.failed()?;
        let first = self.take_or_grow(class, purpose);
        if let Got::Frame(f) = first {
            return Ok(IOResult::Done(f));
        }
        // Waiting starts before the next look, so a frame freed in between is not missed. Every
        // wait below is this one: a frame freed finishes it, whatever else it waits for too.
        let wait = self.released_wait();
        let next = match first {
            Got::Quieting(_) => self.take_or_grow(class, purpose),
            _ => {
                self.make_room(class)?;
                self.take_or_grow(class, purpose)
            }
        };
        match next {
            Got::Frame(f) => return Ok(IOResult::Done(f)),
            // Until the steps that may still read the earliest free frame's memory have ended.
            Got::Quieting(at) if self.store.epochs().wait_for(at, wait.clone()) => {
                return Ok(IOResult::IO(IOCompletions(wait)));
            }
            // Quiet since the look: look again.
            Got::Quieting(_) => return yield_now(),
            Got::None => {}
        }
        if let Some(c) = self.drive_writebacks()? {
            return Ok(IOResult::IO(IOCompletions(c)));
        }
        if self.writes_in_flight.load(Ordering::Acquire) > 0 {
            return Ok(IOResult::IO(IOCompletions(wait)));
        }
        // Every candidate is latched, or held by a writeback being stepped, by a step under way,
        // and nothing in flight will free a frame: until those steps have ended, or a frame is
        // freed first. A step held open while its thread waits for this claim, as a test holding
        // pages does, ends only after it: the freed frame is what ends the wait then.
        if self.store.epochs().grace_period(wait.clone()) {
            return Ok(IOResult::IO(IOCompletions(wait)));
        }
        yield_now()
    }

    /// The pool's memory and writes, for a test that stalled to show.
    #[cfg(test)]
    pub fn describe(&self) -> String {
        // Taken and put back: a stepping thread may hold some meanwhile, and shows in `stepped`.
        let taken = self.writebacks.take_all();
        let mut queued: Vec<String> = taken.iter().map(Writeback::describe).collect();
        for wb in taken {
            self.writebacks.push(wb);
        }
        let stepped = self.stepped.lock();
        queued.extend(
            stepped
                .iter()
                .map(|(id, state)| format!("{id} last stepped: {state}")),
        );
        drop(stepped);
        // What is left in each draining batch.
        let left: Vec<_> = self
            .memory
            .draining()
            .into_iter()
            .flat_map(|(class, batch)| {
                (0..memory::frames_per_batch(class))
                    .map(move |i| memory::frame_at(class, batch, i))
                    .filter(move |&f| !self.memory.is_free(class, f))
                    .map(|f| {
                        let h = self.header(f);
                        (
                            f,
                            h.status(),
                            h.flags.load(Ordering::Acquire),
                            h.latch.optimistic().is_none(),
                            h.dirty(),
                        )
                    })
            })
            .collect();
        // Every frame marked as being written, whichever writeback holds it, or none.
        let writing: Vec<_> = (0..self.frames.headers.len() as u32)
            .filter(|&f| self.header(f).has(WRITING))
            .map(|f| {
                let h = self.header(f);
                (
                    f,
                    PageId::from_raw(h.pid()),
                    h.status(),
                    h.flags.load(Ordering::Acquire),
                    h.latch.optimistic().is_none(),
                    h.dirty(),
                )
            })
            .collect();
        // Per class, its free frames and how many of them a read may take now.
        let horizon = self.store.epochs().horizon();
        let free: Vec<_> = SizeClass::all()
            .filter_map(|c| {
                let frames = self.memory.free_frames(c);
                let quiet = frames
                    .iter()
                    .filter(|&&f| self.quiet(f, c, horizon))
                    .count();
                (!frames.is_empty()).then_some((c.index(), frames.len(), quiet))
            })
            .collect();
        format!(
            "{}{}, horizon {horizon}; free frames (class, free, quiet) {free:?}\nwritebacks \
             {queued:?}, writes in flight {}, draining {:?}, left in them (frame, status, flags, \
             latched, dirty) {left:?}, marked writing (frame, page, status, flags, latched, \
             dirty) {writing:?}, failure {:?}",
            self.memory.describe(),
            self.store.epochs().describe(),
            self.writes_in_flight.load(Ordering::Acquire),
            self.memory.draining(),
            self.failure.get(),
        )
    }

    /// The completion a frame freed finishes. One finished otherwise, by an epoch's wait it was
    /// also registered for, is replaced: handed out finished, it would send every claim round
    /// again at once.
    fn released_wait(&self) -> Completion {
        self.released
            .current(&self.store.epochs().enter(), &self.frames.retiring)
    }

    fn signal_released(&self) {
        if let Some(c) = self.released.take(&self.frames.retiring) {
            c.complete(0);
        }
    }

    /// Frees frames for `class`: by moving memory from a class whose pages are worth less per
    /// byte, or else by evicting within the class.
    fn make_room(self: &Arc<Self>, class: SizeClass) -> Result<()> {
        let draining = self.memory.draining();
        for &(holder, batch) in &draining {
            self.drain(holder, batch)?;
        }
        if !draining.is_empty() {
            // Memory is on its way; meanwhile the class keeps evicting its own if it can.
            if self.memory.holds(class) {
                self.evict_round(class)?;
            }
            return Ok(());
        }
        // What evicting the class's own least valuable page gives up per byte, against what
        // draining memory from another class gives up: the value of the pages that drain evicts,
        // per byte it frees. A batch's free frames are worth nothing, but its pages are worth
        // what they are: drained for their free frames, a batch holding one hot page would lose
        // it, and the page's next reader would drain memory back, round after round. And a page
        // read this epoch is in use, whatever its value says: a page read in again starts with
        // no history, so it looks the coldest, and one operation needing it and a page of another
        // class would evict each for the other forever. A class with memory of its own drains
        // none in use. One with none must drain some batch, all of them perhaps in use: it takes
        // the batch with the fewest pages a step under way may hold, then the fewest in use, then
        // the cheapest. Draining a page the requesting step itself holds, as its operation's
        // other page, would only send that page's next fix to drain the memory back.
        let horizon = self.store.epochs().horizon();
        let own = self
            .threshold(class)
            .map_or(f32::INFINITY, |t| t / class.size() as f32);
        // Among the classes with a batch that can go to `class`: one whose only batches lie where
        // no run of `class`'s order fits has nothing to give it, and choosing it would leave
        // `class` waiting on a drain that never starts.
        let colder = SizeClass::all()
            .filter(|&c| c != class)
            .filter_map(|c| {
                let batch = self
                    .memory
                    .emptiest(c, class, |b| self.drainable(class, b))?;
                let (held, in_use, per_byte) = self.drain_cost(class, batch, horizon);
                Some((c, batch, held, in_use, per_byte))
            })
            .min_by(|a, b| a.2.cmp(&b.2).then(a.3.cmp(&b.3)).then(a.4.total_cmp(&b.4)));
        match colder {
            Some((holder, batch, _, in_use, per_byte))
                if own == f32::INFINITY || (in_use == 0 && per_byte < own) =>
            {
                let started = if class.size() <= memory::BATCH {
                    self.memory.drain_for(holder, batch, class)
                } else {
                    self.memory.drain_run(class, batch)
                };
                if started {
                    for (holder, batch) in self.memory.draining() {
                        self.drain(holder, batch)?;
                    }
                }
                Ok(())
            }
            _ => self.evict_round(class),
        }
    }

    /// Whether the memory around `batch` for `class` can be drained: no frame of it is lent for
    /// good ([`FOR_GOOD`]), which would never come back to free it. Measured: the log's ring, 19
    /// frames lent at open, made the batch holding it the emptiest of its class, and a claim of
    /// another class drained it and waited for good.
    fn drainable(&self, class: SizeClass, batch: u32) -> bool {
        let count = 1u32 << memory::order(class);
        let start = batch & !(count - 1);
        self.memory.taken_in(start, count).into_iter().all(|frame| {
            let h = self.header(frame);
            !(h.status() == LENT && h.flags.load(Ordering::Acquire) & FOR_GOOD != 0)
        })
    }

    /// What draining the memory around `batch` for `class` gives up: how many of its pages a step
    /// under way may hold, marked at or above the epochs' `horizon` or being read in or claimed
    /// for a new page; how many are in use, those and the pages read this epoch; and per byte it
    /// frees, the value of every page in it. A page being written out is leaving anyway.
    fn drain_cost(&self, class: SizeClass, batch: u32, horizon: u64) -> (usize, usize, f32) {
        let count = 1u32 << memory::order(class);
        let start = batch & !(count - 1);
        let now = self.now();
        let (mut held, mut in_use, mut value) = (0, 0, 0.0);
        for frame in self.memory.taken_in(start, count) {
            let h = self.header(frame);
            match h.status() {
                RESIDENT => {
                    let marked = h.held.load(Ordering::Relaxed) >= horizon;
                    held += marked as usize;
                    in_use += (marked || h.tracker.last() >= now) as usize;
                    value += h.tracker.value(now);
                }
                OUTGOING => {}
                _ => {
                    held += 1;
                    in_use += 1;
                    value = f32::INFINITY;
                }
            }
        }
        (
            held,
            in_use,
            value / (count as usize * memory::BATCH) as f32,
        )
    }

    /// The least value among a sample of `class`'s frames: free frames are worth nothing.
    /// `None` if the class holds no memory.
    fn threshold(&self, class: SizeClass) -> Option<f32> {
        let now = self.now();
        let sample = self
            .memory
            .sample(class, watt::THRESHOLD_SAMPLE, &mut rand::rng());
        sample
            .into_iter()
            .map(|f| {
                let h = self.header(f);
                if self.memory.is_free(class, f) {
                    0.0
                } else if h.status() == RESIDENT {
                    h.tracker.value(now)
                } else {
                    f32::INFINITY
                }
            })
            .min_by(f32::total_cmp)
    }

    /// One WATT round: evicts the clean pages among a sample whose value is at most the
    /// threshold, and starts writing the dirty ones out of their frames.
    fn evict_round(self: &Arc<Self>, class: SizeClass) -> Result<()> {
        let Some(threshold) = self.threshold(class) else {
            return Ok(());
        };
        let now = self.now();
        let mut candidates = self
            .memory
            .sample(class, watt::CANDIDATES, &mut rand::rng());
        candidates.sort_unstable();
        candidates.dedup();
        let mut outgoing = Vec::new();
        for frame in candidates {
            let h = self.header(frame);
            if h.tracker.value(now) > threshold {
                continue;
            }
            let latch = || FrameLatch {
                frames: self.frames.clone(),
                frame,
            };
            let Some(hold) = self.latch_to_evict(class, frame, latch) else {
                continue;
            };
            if h.dirty() {
                // Written out of its frame, then gone: a copy to write from would take a frame of
                // its own, which a full pool has none of to spare (one memory).
                outgoing.push(self.start_write_out(hold));
            } else {
                self.evict(class, frame, hold);
            }
        }
        self.write_out(class, outgoing);
        Ok(())
    }

    /// Evicts the clean page in `frame`, which `hold` latches.
    fn evict<L: Deref<Target = VersionedLatch>>(
        &self,
        class: SizeClass,
        frame: u32,
        hold: Held<L>,
    ) {
        let h = self.header(frame);
        debug_assert!(!h.dirty() && !h.has(WRITING));
        self.table.remove(&self.frames.headers, h.pid(), frame);
        self.free_frame(class, frame, hold);
        self.evicted(class);
    }

    /// Counts an eviction of a page of `class` toward the WATT epoch.
    fn evicted(&self, class: SizeClass) {
        let size = class.size() as u64;
        let before = self.evicted.fetch_add(size, Ordering::Relaxed);
        if (before + size) / self.epoch_bytes != before / self.epoch_bytes {
            self.epoch.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Empties what it can of `batch`, draining out of `class`: clean pages are evicted, dirty
    /// ones written out. A page latched or being written back is looked at again on the next
    /// drain.
    fn drain(self: &Arc<Self>, class: SizeClass, batch: u32) -> Result<()> {
        let mut outgoing = Vec::new();
        for i in 0..memory::frames_per_batch(class) {
            let frame = memory::frame_at(class, batch, i);
            if self.memory.is_free(class, frame) {
                continue;
            }
            let latch = || FrameLatch {
                frames: self.frames.clone(),
                frame,
            };
            let Some(hold) = self.latch_to_evict(class, frame, latch) else {
                continue;
            };
            if self.header(frame).dirty() {
                outgoing.push(self.start_write_out(hold));
            } else {
                self.evict(class, frame, hold);
            }
        }
        self.write_out(class, outgoing);
        Ok(())
    }

    /// Latches `frame` exclusively, through `latch`, to evict its page, if it holds a page of
    /// `class` not being written. A frame chosen while its batch was `class`'s may by now be part
    /// of another class's frame, holding a page of that class or none: evicting that page as
    /// `class`'s would give its memory to two pages.
    fn latch_to_evict<L: Deref<Target = VersionedLatch>>(
        &self,
        class: SizeClass,
        frame: u32,
        latch: impl FnOnce() -> L,
    ) -> Option<Held<L>> {
        let h = self.header(frame);
        let version = h.latch.optimistic()?;
        let holds = h.status() == RESIDENT && PageId::from_raw(h.pid()).class() == class;
        if !holds || !h.latch.validate(version) {
            return None;
        }
        let hold = Held::try_take_at(latch(), version)?;
        // WRITING is set under the latch by a pass claiming a dirty page, and cleared without it
        // when a write ends: look again later.
        if h.has(WRITING) {
            hold.release_unchanged();
            return None;
        }
        Some(hold)
    }

    /// Marks the dirty page in the frame `hold` holds as leaving it, and returns its write-out,
    /// for [`write_out`](Self::write_out). Whoever wants the page waits on the write from now on.
    fn start_write_out(&self, hold: Held<FrameLatch>) -> Outgoing {
        let frame = hold.latch().frame;
        let h = self.header(frame);
        let pid = PageId::from_raw(h.pid());
        self.transit(frame).put(Completion::new_wait());
        h.status.store(OUTGOING, Ordering::Release);
        Outgoing {
            pid,
            hold: Some(hold),
        }
    }

    /// Queues the writes of `outgoing`, pages of `class` leaving their frames, and wakes the
    /// pool's waiters: a fix that found one of the pages leaving before this had no writeback to
    /// step, and waits on the pool. Queueing none wakes no one: no page was marked leaving, and a
    /// claim draining a batch whose pages are all leaving already took its wait before the drain,
    /// so a wake would hand it back its own wait finished, its writes never stepped.
    fn write_out(&self, class: SizeClass, outgoing: Vec<Outgoing>) {
        if outgoing.is_empty() {
            return;
        }
        let (temporary, logged): (Vec<Outgoing>, Vec<Outgoing>) =
            outgoing.into_iter().partition(|o| o.pid.is_temporary());
        for (outgoing, temporary) in [(logged, false), (temporary, true)] {
            if !outgoing.is_empty() {
                self.queue(Writeback::out(class, outgoing, temporary));
            }
        }
        self.signal_released();
    }

    /// Ends the write-out of `pid` from the frame `hold` holds, with the write of a copy holding
    /// changes up to `count`. Written, the page leaves the frame, and whoever waits reads it again
    /// from that copy, already in the page table; not written, the page stays, dirty.
    fn written_out(
        &self,
        pid: PageId,
        hold: Held<FrameLatch>,
        count: u64,
        res: std::result::Result<(), CompletionError>,
    ) {
        let frame = hold.latch().frame;
        match res {
            Ok(()) => {
                let h = self.header(frame);
                h.written.fetch_max(count, Ordering::AcqRel);
                debug_assert!(!h.dirty(), "{pid:?} changed while held for its write-out");
                let waiting = self.frames.end_transit(frame);
                self.table.remove(&self.frames.headers, pid.raw(), frame);
                self.free_frame(pid.class(), frame, hold);
                self.evicted(pid.class());
                if let Some(c) = waiting {
                    c.complete(0);
                }
            }
            Err(e) => {
                self.fail(e);
                self.kept(hold);
            }
        }
        self.writes_in_flight.fetch_sub(1, Ordering::AcqRel);
        self.signal_released();
    }

    /// Leaves the page whose write-out failed in the frame `hold` holds, and wakes whoever waits.
    fn kept(&self, hold: Held<FrameLatch>) {
        self.frames.kept(hold);
        self.signal_released();
    }

    // ---------------------------------------------------------------- writing back

    /// Queues the writeback of `frames`, already marked WRITING.
    #[cfg(test)]
    fn write_back(
        &self,
        class: SizeClass,
        frames: Vec<u32>,
        evict_after: bool,
    ) -> Option<Completion> {
        let (temporary, logged): (Vec<u32>, Vec<u32>) = frames
            .into_iter()
            .partition(|&f| PageId::from_raw(self.header(f).pid()).is_temporary());
        let mut finished = None;
        for (frames, temporary) in [(logged, false), (temporary, true)] {
            if frames.is_empty() {
                continue;
            }
            let pids = frames
                .iter()
                .map(|&f| PageId::from_raw(self.header(f).pid()))
                .collect();
            let wb = Writeback::new(class, frames, pids, evict_after, temporary);
            // Passes, which wait on this, hand over no temporary pages.
            finished.get_or_insert(wb.finished.clone());
            self.queue(wb);
        }
        finished
    }

    /// Queues `wb` for stepping.
    fn queue(&self, wb: Writeback) {
        self.queued.fetch_add(1, Ordering::AcqRel);
        self.writebacks.push(wb);
    }

    /// Takes every queued writeback and steps each, putting back those not done: a thread that
    /// finds them taken has none to step, and the thread that took them steps them all. Returns
    /// something to wait on, if one is waiting on IO before its writes are submitted.
    fn drive_writebacks(self: &Arc<Self>) -> Result<Option<Completion>> {
        let mut wait = None;
        for wb in self.writebacks.take_all() {
            if let Progress::Waiting(c) = self.step_writeback(wb) {
                wait.get_or_insert(c);
            }
        }
        Ok(wait)
    }

    /// Steps `wb`, which this thread took from the queue: the only way a writeback is stepped. A
    /// waiter that found it taken saw it neither finished nor put back for another to step, so
    /// once it is put back or done, the pool's waiters are woken: nothing else would wake that
    /// one. Nor would anything wake it for a write that landed during the step, after the step
    /// took the writes landed so far: that write's wakeup came while the writeback was taken, and
    /// only a later step settles it.
    fn step_writeback(self: &Arc<Self>, mut wb: Writeback) -> Progress {
        let landings = wb.landings();
        let progress = wb.step(self);
        #[cfg(test)]
        {
            let came_to = match &progress {
                Progress::Waiting(c) if c.finished() => "yielded",
                Progress::Waiting(_) => "waiting",
                Progress::InFlight => "in flight",
                Progress::Done => "done",
            };
            let state = format!("{} -> {came_to}", wb.describe());
            self.stepped.lock().insert(wb.id, state);
        }
        match &progress {
            // Checked once put back: a write landing after this wakes the waiters itself, with
            // the writeback there to step.
            Progress::InFlight => {
                self.writebacks.push(wb);
                if !landings.is_empty() {
                    self.signal_released();
                }
            }
            Progress::Done => {
                // Kept only while queued: a record per writeback ever stepped would grow without
                // bound in a long test run.
                #[cfg(test)]
                self.stepped.lock().remove(&wb.id);
                self.queued.fetch_sub(1, Ordering::AcqRel);
                self.signal_released();
            }
            Progress::Waiting(_) => {
                self.writebacks.push(wb);
                self.signal_released();
            }
        }
        progress
    }

    /// Runs `f` on the oldest queued writeback, for a test to set its hooks.
    #[cfg(test)]
    fn with_queued(&self, f: impl FnOnce(&mut Writeback)) {
        let mut queued = self.writebacks.take_all();
        f(queued.first_mut().expect("a writeback queued"));
        for wb in queued {
            self.writebacks.push(wb);
        }
    }

    /// Notes that a write of `frame` landed, from its completion: whoever waits on the pool, or on
    /// the frame's write-out, wakes and steps the writeback that settles it.
    fn write_landed(&self, frame: u32, outgoing: bool) {
        if outgoing {
            let c = self.transit(frame).get(&self.store.epochs().enter());
            if let Some(c) = c {
                c.complete(0);
            }
        }
        self.signal_released();
    }

    /// Done once no writeback is queued or in flight; meanwhile, drives the queued ones.
    pub fn settle_writes(self: &Arc<Self>) -> Result<IOResult<()>> {
        let _step = self.store.epochs().enter();
        // Waiting starts before the look, so a write ending in between is not missed.
        let wait = self.released_wait();
        if let Some(c) = self.drive_writebacks()? {
            return Ok(IOResult::IO(IOCompletions(c)));
        }
        // One queued and held by another thread may not have submitted its writes yet.
        if self.writes_in_flight.load(Ordering::Acquire) > 0
            || self.queued.load(Ordering::Acquire) > 0
        {
            return Ok(IOResult::IO(IOCompletions(wait)));
        }
        Ok(IOResult::Done(()))
    }

    /// Ends the write of a copy of `pid`, in `frame`, holding change count `count`.
    fn write_ended(
        &self,
        frame: u32,
        pid: PageId,
        count: u64,
        evict_after: bool,
        res: std::result::Result<(), CompletionError>,
    ) {
        let h = self.header(frame);
        // Taken while WRITING keeps the page in the frame (nothing evicts or frees a page being
        // written), so the hold is on this page; once the flag is clear another may be in.
        let hold = (res.is_ok() && evict_after)
            .then(|| Held::try_take(&h.latch))
            .flatten();
        match res {
            Ok(()) => {
                h.written.fetch_max(count, Ordering::AcqRel);
            }
            Err(e) => self.fail(e),
        }
        h.flags
            .fetch_and(!(WRITING | EVICT_AFTER), Ordering::AcqRel);
        if let Some(hold) = hold {
            assert!(
                h.status() == RESIDENT && h.pid() == pid.raw(),
                "frame {frame} gave up {pid:?} while it was being written"
            );
            if h.dirty() {
                // Changed while it was written: it stays.
                hold.release_unchanged();
            } else {
                self.evict(self.memory.class_of(frame), frame, hold);
            }
        }
        self.writes_in_flight.fetch_sub(1, Ordering::AcqRel);
        self.signal_released();
    }

    fn fail(&self, e: CompletionError) {
        self.failure.set(e);
        self.signal_released();
    }

    // ---------------------------------------------------------------- lending

    /// A quiet free frame of `class`, lent for `what`, if one is free or the region has memory
    /// for the class: never an eviction or a wait, so it may be taken under latches.
    pub(crate) fn try_lend(
        self: &Arc<Self>,
        class: SizeClass,
        what: LentUse,
    ) -> Option<AlignedBlock> {
        match self.take_or_grow(class, Purpose::Read) {
            Got::Frame(frame) => Some(self.lend_frame(frame, class, what)),
            Got::Quieting(_) | Got::None => None,
        }
    }

    /// A quiet frame of `class`, lent for `what`, making room for it if need be. Step it until
    /// done, holding no latch.
    pub(crate) fn lend(
        self: &Arc<Self>,
        class: SizeClass,
        what: LentUse,
    ) -> Result<IOResult<AlignedBlock>> {
        let frame = match self.claim(class, Purpose::Read)? {
            IOResult::Done(frame) => frame,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        Ok(IOResult::Done(self.lend_frame(frame, class, what)))
    }

    /// `frame`, free and quiet, lent for `what`: a hint to the page it last held fails, as its
    /// latch's version rises.
    fn lend_frame(self: &Arc<Self>, frame: u32, class: SizeClass, what: LentUse) -> AlignedBlock {
        let h = self.header(frame);
        let hold = self.take_free_latch(frame);
        h.pid.store(NO_PAGE, Ordering::Release);
        let flags = if what == LentUse::Log { FOR_GOOD } else { 0 };
        h.flags.store(flags, Ordering::Release);
        h.status.store(LENT, Ordering::Release);
        drop(hold);
        let token = self.lends.lent(frame, class, what);
        let ptr =
            std::ptr::NonNull::new(self.memory.region.ptr(frame)).expect("the region's memory");
        // SAFETY: a frame of the region, which the lender keeps mapped; frames are aligned to
        // 4 KiB; and a LENT frame is no one else's until its block gives it back.
        unsafe { AlignedBlock::lent(ptr, class.size(), self.lends.clone(), token) }
    }

    /// A lent frame given back: free again.
    fn take_back(&self, class: SizeClass, frame: u32) {
        debug_assert_eq!(
            self.header(frame).status(),
            LENT,
            "frame {frame} given back twice"
        );
        // A lent frame's latch is no one's.
        let hold = self.take_free_latch(frame);
        self.free_frame(class, frame, hold);
    }

    /// The bytes lent beside the page table, by use, now and at most.
    #[cfg(test)]
    pub fn lent_stats(&self) -> LentStats {
        self.lends.stats()
    }

    /// Starts the peaks of the bytes lent again from what is lent now, for a measurement.
    #[cfg(test)]
    pub fn reset_lent_peaks(&self) {
        self.lends.reset_peaks();
    }

    /// Bytes of the frames whose header reads LENT, for a test to tell that a frame lent and given
    /// back is free again, as the counts alone do not.
    #[cfg(test)]
    pub fn lent_frame_bytes(&self) -> usize {
        (0..self.memory.grains())
            .filter(|&f| self.header(f).status() == LENT)
            .map(|f| self.memory.class_of(f).size())
            .sum()
    }

    // ---------------------------------------------------------------- checkpoints

    /// Checks that the page in `frame`, `pid`, latched exclusively, owes the cut in effect no
    /// copy before it changes. Every change asks for its copies first ([`keep`](Self::keep));
    /// one that reaches here owing a copy asked for none, and a copy made now would pass the
    /// room's limit, so it is a fault.
    fn assert_kept(&self, frame: u32, pid: PageId) {
        let n = self.cutting.load(Ordering::Acquire);
        assert!(
            n == 0 || !self.owes_copy(frame, pid, n),
            "{pid:?} changed past checkpoint {n}'s cut with no room asked for its copy"
        );
    }

    /// `pid`, in `frame` latched exclusively, is a page the cut in effect left out: owed no
    /// image, it needs no copy.
    fn left_out_of_cut(&self, frame: u32, pid: PageId) {
        let n = self.cutting.load(Ordering::Acquire);
        let h = self.header(frame);
        // Relaxed: under the frame's exclusive latch.
        if n != 0 && h.pending.load(Ordering::Relaxed) == n {
            h.pending.store(0, Ordering::Release);
            self.cut_owed.lock().remove(&pid);
        }
    }

    /// Whether `pid`, in `frame` latched exclusively, needs a copy before it changes for
    /// checkpoint `n`: its cut marked it, and nothing kept it since. A value's page needs none:
    /// it is written only before a leaf names its value, so no image of a cut that found it
    /// unwritten names it.
    fn owes_copy(&self, frame: u32, pid: PageId, n: u64) -> bool {
        // Relaxed: under the frame's exclusive latch, which the cut's marking took too.
        self.header(frame).pending.load(Ordering::Relaxed) == n
            && page::kind(self.bytes(frame, pid.class())) != blob::KIND
    }

    /// Copies `pid`, in `frame` latched exclusively, into `block` for the cut in effect, its room
    /// charged.
    fn copy_for_cut(&self, frame: u32, pid: PageId, mut block: AlignedBlock) {
        self.bytes(frame, pid.class()).copy_to(0, block.bytes_mut());
        self.kept
            .lock()
            .insert(pid, KeptCopy::charged(block, &self.room));
        self.cut_owed.lock().remove(&pid);
        self.header(frame).pending.store(0, Ordering::Release);
    }

    /// Room for the copies the cut in effect needs of `pages`, each latched exclusively, before
    /// any of them changes: every one still owed a copy is copied as it stands, all within the
    /// room or none. Refused, the change lets go of every latch it holds and of the change gate,
    /// then waits ([`wait_for_room`](Self::wait_for_room)) and starts again: nothing yields once
    /// a change has begun.
    pub fn keep(self: &Arc<Self>, pages: &[&Exclusive<'_>]) -> bool {
        match self.try_keep(pages) {
            Ok(()) => {
                self.room.admitted();
                true
            }
            Err(Turned::Room) => {
                let n = self.cutting.load(Ordering::Acquire);
                self.room.refused(self.owed_bytes(pages, n));
                false
            }
            Err(Turned::Frame(class)) => {
                #[cfg(test)]
                FRAME_SHORTS.with(|n| n.set(n.get() + 1));
                SHORT.with(|short| short.set(Some(class)));
                false
            }
        }
    }

    fn try_keep(self: &Arc<Self>, pages: &[&Exclusive<'_>]) -> std::result::Result<(), Turned> {
        let n = self.cutting.load(Ordering::Acquire);
        if n == 0 {
            return Ok(());
        }
        let bytes = self.owed_bytes(pages, n);
        if bytes == 0 {
            return Ok(());
        }
        if !self.room.charge(bytes) {
            return Err(Turned::Room);
        }
        // A frame for each copy, every one or none, before any is made: a frame that would take
        // an eviction is not waited for under the latches.
        let mut owed = Vec::with_capacity(pages.len());
        for (i, p) in pages.iter().enumerate() {
            if pages[..i].iter().any(|q| q.frame == p.frame) || !self.owes_copy(p.frame, p.pid, n) {
                continue;
            }
            match self.spare_or_lend(p.pid.class()) {
                Some(block) => owed.push((p.frame, p.pid, block)),
                None => {
                    drop(owed);
                    self.room.uncharge(bytes);
                    return Err(Turned::Frame(p.pid.class()));
                }
            }
        }
        for (frame, pid, block) in owed {
            self.copy_for_cut(frame, pid, block);
        }
        // A frame lent ahead and not taken goes back.
        SPARES.with(|spares| spares.borrow_mut().clear());
        Ok(())
    }

    /// A frame of `class` for a kept copy: one this thread was lent while it waited, or a free one.
    fn spare_or_lend(self: &Arc<Self>, class: SizeClass) -> Option<AlignedBlock> {
        let spare = SPARES.with(|spares| {
            let mut spares = spares.borrow_mut();
            let at = spares.iter().position(|b| b.len() == class.size())?;
            Some(spares.swap_remove(at))
        });
        spare.or_else(|| self.try_lend(class, LentUse::Kept))
    }

    /// Bytes the copies of `pages` for checkpoint `n` take, each page counted once.
    fn owed_bytes(&self, pages: &[&Exclusive<'_>], n: u64) -> usize {
        if n == 0 {
            return 0;
        }
        pages
            .iter()
            .enumerate()
            .filter(|&(i, p)| {
                pages[..i].iter().all(|q| q.frame != p.frame) && self.owes_copy(p.frame, p.pid, n)
            })
            .map(|(_, p)| p.pid.class().size())
            .sum()
    }

    /// For a change refused room, which has let go of every latch and the change gate: drives
    /// the checkpoint whose cut is in effect until its images give room back, or waits for
    /// whoever is stepping it. An embedded database may have one connection and no other thread,
    /// so the writer waiting is the one to step it. Done to start the change again at once.
    pub fn wait_for_room(self: &Arc<Self>) -> Result<IOResult<()>> {
        if let Some(class) = SHORT.with(|short| short.take()) {
            // Turned down for a free frame, not for room: one lent now, holding nothing, which the
            // change takes when it starts again. While it is being made, the change starting
            // again finds the room as it did.
            return Ok(match self.lend(class, LentUse::Kept)? {
                IOResult::Done(block) => {
                    SPARES.with(|spares| spares.borrow_mut().push(block));
                    IOResult::Done(())
                }
                IOResult::IO(io) => IOResult::IO(io),
            });
        }
        // Taken first, so a release or a stepper leaving after the look below finishes it.
        let wait = self.room.wait();
        if self.cutting.load(Ordering::Acquire) == 0 {
            return Ok(IOResult::Done(()));
        }
        match self.relief.get_ref().and_then(|relief| relief.relieve()) {
            Some(stepped) => stepped,
            None => Ok(IOResult::IO(IOCompletions(wait))),
        }
    }

    /// Sets what a writer refused room drives: once, when the database opens.
    pub fn set_relief(&self, relief: Arc<dyn Relief>) {
        self.relief.set(relief);
    }

    /// Whoever waits for room tries again: a stepper of the checkpoint leaving.
    pub fn wake_room(&self) {
        self.room.wake();
    }

    /// Whether room for kept copies is given back by another thread: the database's
    /// checkpointing thread, while it runs.
    pub fn room_freed_elsewhere(&self, elsewhere: bool) {
        self.room.freed_elsewhere(elsewhere);
    }

    /// Waits for room handed out that block until another thread gives it back.
    #[cfg(test)]
    pub fn blocking_room_waits(&self) -> u64 {
        self.room.blocking_waits()
    }

    /// Whether the room takes the largest request it refused since it last admitted one: a
    /// refused writer stepping the checkpoint stops then.
    pub fn room_takes_refused(&self) -> bool {
        self.cutting.load(Ordering::Acquire) == 0 || self.room.takes_refused()
    }

    /// Whether a change was refused room since one was last admitted: the checkpoint then images
    /// kept copies first.
    pub fn room_pressed(&self) -> bool {
        self.room.pressed()
    }

    /// A page the cut in effect kept, its copy taken out, if any.
    pub fn take_kept(&self) -> Option<(PageId, KeptCopy)> {
        let mut kept = self.kept.lock();
        let pid = *kept.keys().next()?;
        kept.remove_entry(&pid)
    }

    /// What the room for kept copies has held, and refused.
    #[cfg(test)]
    pub fn kept_stats(&self) -> KeptStats {
        self.room.stats()
    }

    /// Bytes of swap copies written since the pool opened.
    #[cfg(test)]
    pub fn swap_written(&self) -> u64 {
        self.swap_written.load(Ordering::Relaxed)
    }

    /// Bytes of the temporary pages held: the versions the arenas keep, beside any temporary
    /// tree's.
    #[cfg(test)]
    pub fn temporary_bytes(&self) -> u64 {
        self.temp.live_bytes()
    }

    /// The pages owed an image now, resident or in their swap copies alone: the work the next
    /// checkpoint has. A walk over every frame, for a report.
    #[cfg(test)]
    pub fn owed_pages(&self) -> usize {
        let mut owed: HashSet<PageId> = self.store.swap_pages().into_iter().collect();
        for frame in 0..self.memory.grains() {
            let h = self.header(frame);
            let pid = PageId::from_raw(h.pid());
            if h.status() == RESIDENT && !pid.is_temporary() && h.owed() {
                owed.insert(pid);
            }
        }
        owed.len()
    }

    /// Starts the kept copies' peak again from what is held now, and their longest wait from
    /// none, for a measurement.
    #[cfg(test)]
    pub fn reset_kept_peaks(&self) {
        self.room.reset_peaks();
    }

    /// The cut of checkpoint `n`, under the store's closed change gate: every page owed an image
    /// is marked pending, under its latch, so its first change from then on keeps it as it
    /// stands. `scan` collects them, those with a swap copy and no frame included. Eviction goes
    /// on meanwhile, so the swap copies are listed after the frames, and every page owed stays
    /// listed until it is imaged or kept: one read in again comes in pending. Step it until done.
    pub fn begin_cut(&self, n: u64, scan: &mut CutScan) -> IOResult<()> {
        self.cutting.store(n, Ordering::SeqCst);
        while scan.next < self.memory.grains() {
            let h = self.header(scan.next);
            if h.status() == RESIDENT {
                // A change holds it for a moment: come back.
                let Some(hold) = Held::try_take(&h.latch) else {
                    return IOResult::IO(IOCompletions(Completion::new_yield()));
                };
                self.mark_owed(scan.next, n, &mut scan.owed);
                hold.release_unchanged();
            }
            scan.next += 1;
        }
        if !scan.listed {
            let mut seen: HashSet<PageId> = scan.owed.iter().copied().collect();
            scan.swapped = self.store.swap_pages();
            scan.owed
                .extend(scan.swapped.iter().copied().filter(|pid| seen.insert(*pid)));
            *self.cut_owed.lock() = seen;
            scan.listed = true;
        }
        // A page with a swap copy read in after the walk passed its frame, before the list was
        // set: unchanged since, the gate being closed.
        while let Some(&pid) = scan.swapped.last() {
            if let Some(frame) = self.table.find(&self.frames.headers, pid.raw()) {
                let h = self.header(frame);
                let Some(hold) = Held::try_take(&h.latch) else {
                    return IOResult::IO(IOCompletions(Completion::new_yield()));
                };
                if h.status() == RESIDENT && h.pid() == pid.raw() {
                    h.pending.store(n, Ordering::Release);
                }
                hold.release_unchanged();
            }
            scan.swapped.pop();
        }
        IOResult::Done(())
    }

    fn mark_owed(&self, frame: u32, n: u64, owed: &mut Vec<PageId>) {
        let h = self.header(frame);
        let pid = PageId::from_raw(h.pid());
        if h.status() == RESIDENT && !pid.is_temporary() && h.owed() {
            h.pending.store(n, Ordering::Release);
            owed.push(pid);
        }
    }

    /// What checkpoint `n` images of `pid`, which its cut found owed: the page as its first change
    /// since kept it, or the page as it stands, with its change count, which no change can be
    /// making meanwhile. Step it until done.
    pub fn image_source(self: &Arc<Self>, n: u64, pid: PageId) -> Result<IOResult<Source>> {
        if let Some(copy) = self.kept.lock().remove(&pid) {
            return Ok(IOResult::Done(Source::Kept(copy)));
        }
        let page = match self.fix_exclusive(pid, None)? {
            IOResult::Done(p) => p,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        // Kept between the look and the latch.
        if let Some(copy) = self.kept.lock().remove(&pid) {
            page.release_unchanged();
            return Ok(IOResult::Done(Source::Kept(copy)));
        }
        let h = self.header(page.frame);
        let mut bytes = vec![0u8; pid.class().size()];
        page.page().copy_to(0, &mut bytes);
        let count = h.changes.load(Ordering::Acquire);
        if h.pending.load(Ordering::Acquire) == n {
            h.pending.store(0, Ordering::Release);
        }
        self.cut_owed.lock().remove(&pid);
        page.release_unchanged();
        Ok(IOResult::Done(Source::Live { bytes, count }))
    }

    /// A checkpoint wrote, as `pid`'s checkpoint copy, the page as it stood at change count
    /// `count`, or an image that reads to every reader as that page does: if the page is still at
    /// that count, it is owed nothing and is clean, the copy standing for it once it is evicted,
    /// and its swap copy, which is no newer, is given up. A page changed since keeps its swap copy,
    /// which a later eviction may have made the newest.
    pub fn settled(&self, pid: PageId, count: u64) {
        let Some(frame) = self.table.find(&self.frames.headers, pid.raw()) else {
            return;
        };
        let h = self.header(frame);
        let Some(hold) = Held::try_take(&h.latch) else {
            // Changing: it differs from the copy, and stays owed.
            return;
        };
        if h.status() != RESIDENT
            || h.pid() != pid.raw()
            || h.changes.load(Ordering::Acquire) != count
        {
            hold.release_unchanged();
            return;
        }
        h.imaged.store(count, Ordering::Release);
        h.written.fetch_max(count, Ordering::AcqRel);
        self.store.drop_swap(pid);
        hold.release_unchanged();
    }

    /// A sweep writing back every dirty page, for a test ([`sweep`]).
    #[cfg(test)]
    pub fn sweep(&self) -> sweep::Sweep {
        sweep::Sweep::new()
    }

    /// Evicts `pid`, resident, clean and unheld, for a test: the page is then in its copies alone.
    /// Whether it was.
    #[cfg(test)]
    pub fn evict_clean(&self, pid: PageId) -> bool {
        let Some(frame) = self.table.find(&self.frames.headers, pid.raw()) else {
            return false;
        };
        let h = self.header(frame);
        let Some(hold) = self.latch_to_evict(pid.class(), frame, || &h.latch) else {
            return false;
        };
        if h.dirty() {
            hold.release_unchanged();
            return false;
        }
        self.evict(pid.class(), frame, hold);
        true
    }

    /// The checkpoint whose cut is in effect, 0 for none.
    pub fn cutting(&self) -> u64 {
        self.cutting.load(Ordering::Acquire)
    }

    /// Checkpoint `n` has imaged every page its cut found owed: changes no longer keep pages.
    pub fn end_cut(&self, n: u64) {
        debug_assert_eq!(self.cutting.load(Ordering::Acquire), n);
        self.cutting.store(0, Ordering::SeqCst);
        // Each copy's room is given back as it drops.
        let kept = std::mem::take(&mut *self.kept.lock());
        drop(kept);
        self.cut_owed.lock().clear();
        self.room.cut_ended();
    }
}

/// A cut's walk over the frames for the pages owed an image ([`BufferPool::begin_cut`]).
#[derive(Default)]
pub struct CutScan {
    next: u32,
    pub owed: Vec<PageId>,
    /// Whether the swap copies are listed, and those still to look at.
    listed: bool,
    swapped: Vec<PageId>,
}

/// What a writer refused room for kept copies drives ([`BufferPool::wait_for_room`]).
pub trait Relief: Send + Sync {
    /// Steps the checkpoint whose cut is in effect while it is: `None` while someone else steps
    /// it, who wakes the waiters as it leaves.
    fn relieve(&self) -> Option<Result<IOResult<()>>>;
}

/// What a checkpoint images of a page.
pub enum Source {
    /// The page as its first change or free after the cut kept it, charged to the room until
    /// dropped.
    Kept(KeptCopy),
    /// The page as it stands, at its change count.
    Live { bytes: Vec<u8>, count: u64 },
}

/// A temporary page about to be read in.
struct TempLocated {
    temp: Arc<TempPages>,
    pid: PageId,
}

impl Located for TempLocated {
    fn read(self: Box<Self>, into: Arc<Buffer>, done: ReadDone) -> Result<IOResult<()>> {
        self.temp.read(self.pid, into, done)?;
        Ok(IOResult::Done(()))
    }
}

// -------------------------------------------------------------------- guards

/// A page fixed without a lock. Nothing read is valid until [`validate`](Self::validate) says so.
pub struct Optimistic<'a> {
    pool: &'a BufferPool,
    frame: u32,
    pid: PageId,
    version: u64,
    /// Nothing the guard names is reused while it lives.
    _step: Step<'a>,
}

impl Clone for Optimistic<'_> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool,
            frame: self.frame,
            pid: self.pid,
            version: self.version,
            _step: self.pool.store.epochs().enter(),
        }
    }
}

impl<'a> Optimistic<'a> {
    /// Runs `read` over the page, and returns what it returned if the page did not change
    /// meanwhile, `None` if it did. `read` may see the page mid-change, so it must not trust
    /// what it reads to be consistent: offsets and lengths are checked before use, and nothing
    /// it does may panic on garbage.
    #[inline]
    pub fn read<R>(&self, read: impl FnOnce(&Shared) -> R) -> Option<R> {
        let out = read(self.pool.bytes(self.frame, self.pid.class()));
        self.validate().then_some(out)
    }

    /// The latch version the fix saw: equal versions of one frame and page mean no change between.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Whether the page is unchanged since it was fixed.
    pub fn validate(&self) -> bool {
        self.pool.header(self.frame).latch.validate(self.version)
    }

    /// Latches the page exclusively if it is unchanged since it was fixed.
    pub fn upgrade(self) -> Option<Exclusive<'a>> {
        let (pool, frame, pid) = (self.pool, self.frame, self.pid);
        Held::try_take_at(&pool.header(frame).latch, self.version).map(|hold| Exclusive {
            pool,
            frame,
            pid,
            hold,
            step: self._step,
        })
    }

    pub fn pid(&self) -> PageId {
        self.pid
    }

    /// The hint to store beside a reference to this page.
    pub fn hint(&self) -> u32 {
        self.frame + 1
    }
}

/// A page latched exclusively.
pub struct Exclusive<'a> {
    pool: &'a BufferPool,
    frame: u32,
    pid: PageId,
    /// Released when the guard drops, before its step ends.
    hold: Hold<'a>,
    /// Nothing the guard names is reused while it lives.
    step: Step<'a>,
}

impl<'a> Exclusive<'a> {
    pub fn page(&self) -> &Shared {
        self.pool.bytes(self.frame, self.pid.class())
    }

    /// The page, to change. Every change is reported with [`changed`](Self::changed) before the
    /// latch is released; a change not reported is lost when the page is evicted.
    pub fn page_mut(&mut self) -> Writer<'_> {
        self.pool.assert_kept(self.frame, self.pid);
        // SAFETY: latched exclusively, and the writer borrows the guard.
        unsafe { self.pool.writer(self.frame, self.pid.class()) }
    }

    /// The page, for a write nothing depends on: a child hint, which every reader checks before
    /// use. No copy is kept for a cut: an image with the hint reads as one without.
    pub fn page_mut_unkept(&mut self) -> Writer<'_> {
        // SAFETY: latched exclusively, and the writer borrows the guard.
        unsafe { self.pool.writer(self.frame, self.pid.class()) }
    }

    /// Notes a change, made under the exclusive latch: the page's change count rises, and it
    /// differs from every copy on disk until one holding the change is written.
    pub fn changed(&mut self) {
        let h = self.pool.header(self.frame);
        // Relaxed: under the frame's exclusive latch.
        let count = h.changes.load(Ordering::Relaxed) + 1;
        page::set_seq(&mut self.page_mut(), count);
        h.changes.store(count, Ordering::Release);
        h.tracker.write(self.pool.now());
    }

    pub fn pid(&self) -> PageId {
        self.pid
    }

    pub fn hint(&self) -> u32 {
        self.frame + 1
    }

    /// Marks the page as unlinked from everything that refers to it, until it is freed: a hint
    /// to it is not followed ([`BufferPool::fix_optimistic_at`]), since what it holds no longer
    /// counts.
    pub fn abandon(&self) {
        self.pool
            .header(self.frame)
            .flags
            .fetch_or(ABANDONED, Ordering::AcqRel);
    }

    /// Releases the latch keeping the version, for a write nothing depends on: a frame hint,
    /// which every reader checks before use. Optimistic readers are not sent back for it, and
    /// the page stays held optimistically at that version.
    pub fn release_unchanged(self) -> Optimistic<'a> {
        let Exclusive {
            pool,
            frame,
            pid,
            hold,
            step,
        } = self;
        let version = hold.version();
        hold.release_unchanged();
        // Its step goes on in the reader's.
        Optimistic {
            pool,
            frame,
            pid,
            version,
            _step: step,
        }
    }
}
