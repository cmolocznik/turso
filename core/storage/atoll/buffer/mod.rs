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
mod lend;
mod lent;
mod memory;
mod shortage;
mod source;
mod staging;
mod supply;
use supply::Ticket;
#[cfg(test)]
mod sweep;
mod table;
mod temp;
mod upkeep;
mod watt;

#[cfg(test)]
mod tests;

pub use kept::KeptCopy;
#[cfg(test)]
pub use kept::KeptStats;
pub use kept::RoomWait;
pub use lend::{bank_frames, Refused, Retry};
#[cfg(test)]
pub use lent::LentStats;
pub use lent::Use as LentUse;
pub use memory::MIN_BUDGET;
#[cfg(test)]
pub(crate) use shortage::scarce;
pub(crate) use shortage::unowned;
pub(crate) use shortage::{cancel as cancel_shortage, gate_closed, Conn, Owner};
pub(crate) use upkeep::{scope as upkeep_scope, Upkeep};

/// Batches the copies' worst layout takes (DESIGN.md 9.1.1, Minimum budget): one for each of the
/// three classes a change can need copies of.
pub const COPY_BATCHES: usize = lend::MOST_PAGES;

/// The smallest budget a pool that keeps copies takes, lending nothing else: the core and the
/// copies' worst layout.
pub const MIN_KEEPING_BUDGET: usize = MIN_BUDGET + COPY_BATCHES * memory::BATCH;

/// The smallest budget a database takes whose log's ring fits one batch, as every ring of
/// chunks up to 256 KiB with the default writes in flight does, with the default image bank:
/// [`min_budget_beside`] of that ring and bank, 22 MiB.
pub const MIN_DB_BUDGET: usize = MIN_KEEPING_BUDGET + memory::BATCH + 2 * memory::BATCH;

/// The smallest budget a database takes (DESIGN.md 9.1.1, Minimum budget): the core, the log's
/// ring of `ring` bytes and the image bank of `bank` frames, each in whole batches, since a lend
/// batch holds no other class, and one batch for each of the three classes a change can need
/// copies of.
pub fn min_budget_beside(ring: usize, bank: u32) -> usize {
    MIN_KEEPING_BUDGET
        + ring.next_multiple_of(memory::BATCH)
        + (bank as usize).div_ceil(memory::frames_per_batch(lend::bank_class()) as usize)
            * memory::BATCH
}
pub use source::{LocalSource, Located, PageSource, ReadDone};

use super::aligned::AlignedBlock;
use super::blob;
use super::epoch::Step;
use super::latch::{Held, HeldWord, LatchRef, Look, Marked, VersionedLatch};
use super::once::SetOnce;
use super::page::{self, PageId, SizeClass, CLASS_COUNT};
use super::shared::{Bytes, BytesMut, Shared, Writer};
use super::stack::Stack;
use super::store::PageStore;
use crate::storage::atoll::notify;
use cell::{CompletionCell, CompletionDomain};
use clean::{Progress, Writeback};
use frame::{
    FrameHeader, ABANDONED, EVICT_AFTER, FREE, LENT, LOADING, NO_PAGE, OUTGOING, RESIDENT, WRITING,
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
type Hold<'a> = Held<FrameRef<'a>>;

/// A frame's latch reached by a borrow of the frames. Every hold on a frame's latch is taken
/// through this or [`FrameLatch`], so a release a waiter marked is a change of the pool's supply,
/// its event opened before the unlock (DESIGN.md 9.1.3).
#[derive(Clone, Copy)]
struct FrameRef<'a> {
    frames: &'a Frames,
    frame: u32,
}

impl Deref for FrameRef<'_> {
    type Target = VersionedLatch;

    fn deref(&self) -> &VersionedLatch {
        &self.frames.headers[self.frame as usize].latch
    }
}

impl LatchRef for FrameRef<'_> {
    fn release_marked<F: FnOnce()>(&self, unlock: F) {
        self.frames.release_marked(self.frame, unlock);
    }
}

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
    /// Waiters for a resident frame's held latch. Only that frame's release or a queued write-out
    /// finishes its cell, so another frame or the checkpoint's state cannot finish the wait.
    released: Zeroed<CompletionCell>,
    /// Completions taken out of the transits.
    completions: CompletionDomain,
    /// The pool's one supply (DESIGN.md 9.1.3): every change that can end a wait for frames, room
    /// or a lend is made in its scope, and every such wait is a ticket on it.
    supply: Arc<supply::Supply>,
}

impl Frames {
    /// Takes the IO in flight for `frame` out of its cell: for the frame's holder.
    fn end_transit(&self, frame: u32) -> Option<Completion> {
        self.transits[frame as usize].take(&self.completions)
    }

    /// Leaves the page whose write-out did not happen in the frame `hold` holds, and sends
    /// whoever waits on the write to look again.
    fn kept(&self, hold: Held<FrameLatch>) {
        let frame = hold.latch().frame;
        let waiting = self.end_transit(frame);
        self.headers[frame as usize]
            .status
            .store(RESIDENT, Ordering::Release);
        crate::race::point("buffer.kept_published");
        drop(hold);
        if let Some(c) = waiting {
            notify::finish(&c, Ok(0));
        }
        // Resident again with its latch let go and no step held: evictable, which a claim waiting
        // for room is told (DESIGN.md 9.1.2).
        self.signal_released();
    }

    /// A change made: every waiter on the supply looks again (DESIGN.md 9.1.3).
    fn signal_released(&self) {
        self.supply.change(|event| event.arm());
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

impl LatchRef for FrameLatch {
    fn release_marked<F: FnOnce()>(&self, unlock: F) {
        self.frames.release_marked(self.frame, unlock);
    }
}

impl Frames {
    fn release_marked<F: FnOnce()>(&self, frame: u32, unlock: F) {
        self.supply.change(|event| {
            crate::race::point("buffer.frame_will_unlock");
            unlock();
            self.finish_frame_wait(frame);
            event.arm();
        });
    }

    fn finish_frame_wait(&self, frame: u32) {
        if let Some(c) = self.released[frame as usize].take(&self.completions) {
            notify::finish(&c, Ok(0));
        }
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
    /// The read's own wait, which its landing finishes with its result before it marks the read
    /// landed, so whoever sees the read off the backend reads that result here.
    loaded: Completion,
}

impl Loading {
    fn end(mut self, res: std::result::Result<(), CompletionError>) {
        let hold = self.hold.take().expect("a read ends once");
        if let Some(pool) = self.pool.upgrade() {
            // Its event opened before the request leaves the count (DESIGN.md 9.1.3).
            pool.frames.supply.change(|event| {
                // Published first and woken at the scope's close: the fix that started the read
                // and sees it landed, so takes a wait other than this one, has its result
                // (DESIGN.md 9.1.3, wait kinds; the auditor's #3858).
                notify::finish(&self.loaded, res.map(|()| 0));
                crate::race::point("buffer.read_result");
                pool.out_of_flight(&hold, event);
                pool.loaded(self.pid, hold, res);
                event.arm();
            });
        }
    }
}

impl Drop for Loading {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        if let (Some(hold), Some(pool)) = (self.hold.take(), self.pool.upgrade()) {
            pool.frames.supply.change(|event| {
                pool.out_of_flight(&hold, event);
                pool.unload(self.pid, hold, None);
                event.arm();
            });
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

    /// The version of the write-out's hold: what its frame's submission is marked at.
    pub(super) fn version(&self) -> u64 {
        self.hold.as_ref().expect("held until it ends").version()
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

/// What a round of making room did: started work that makes room (a drain of a batch no drain had,
/// an eviction, a write-out), and what it did about the resident pages it found latched (DESIGN.md
/// 9.1.2).
#[derive(Default, Debug)]
struct Room {
    started: bool,
    /// A latch it found held marked waited: its release finishes the claim's registration.
    marked: bool,
    /// A latch it found held changed before it could mark it: another party's act.
    changed: bool,
    /// The rare path's gate, lost by a run's drain: its wait, all the claim then waits on
    /// (DESIGN.md 9.1.3, rule 4).
    gate: Option<Completion>,
    /// The lowest stamp holding a batch it passed over, being held for the class it was handed to,
    /// with nothing else to drain: the epoch the claim waits on (DESIGN.md 9.1.3).
    held: Option<u64>,
}

impl std::ops::BitOrAssign for Room {
    fn bitor_assign(&mut self, other: Self) {
        self.started |= other.started;
        self.marked |= other.marked;
        self.changed |= other.changed;
        if self.gate.is_none() {
            self.gate = other.gate;
        }
        self.held = match (self.held, other.held) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
}

/// What a run's drain came to: run (and whether a drain started), or the gate lost, with the
/// gate's wait.
enum Drained {
    Ran(bool),
    GateLost(Completion),
}

#[cfg(test)]
crate::thread::thread_local! {
    /// A threshold a test sets for this thread's eviction rounds, in place of the sampled one.
    pub(super) static THRESHOLD: std::cell::Cell<Option<f32>> = const { std::cell::Cell::new(None) };
}

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
    /// What the pool lends for as long as it is open, taken as it opens from the highest free
    /// batches, before anything is cached (DESIGN.md 9.1.1): the log's ring and the image bank.
    pub lent_at_open: LentAtOpen,
}

/// The lend batches a pool takes as it opens (DESIGN.md 9.1.1).
#[derive(Clone, Copy, Debug, Default)]
pub struct LentAtOpen {
    /// The class of the log's ring chunks, and how many.
    pub ring: Option<(SizeClass, u32)>,
    /// Frames of the image bank: `S`, of the largest node class.
    pub bank: u32,
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
            lent_at_open: LentAtOpen::default(),
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

/// What following a frame came to: the frame fixed, or not holding the page, or a wait.
enum Followed<'a> {
    Fixed(Option<(u64, Option<Hold<'a>>)>),
    Wait(Completion),
}

enum Probe<'a> {
    /// The frame holds the page; the version an optimistic fix saw.
    Hit(u64),
    /// The frame holds the page, latched exclusively.
    Held(Hold<'a>),
    /// A resident page another caller holds: its frame's release finishes the wait.
    ResidentBusy,
    /// A loading or outgoing frame between its phases, its transit not yet put or already
    /// taken. Waited on once the word the look read is marked (DESIGN.md 9.1.3).
    Busy(HeldWord),
    /// The page is being read into the frame: the read's completion, and whether its IO was
    /// submitted.
    Loading(Completion, bool),
    /// The page is being written out of the frame: the write's completion, and whether its IO
    /// was submitted.
    Outgoing(Completion, bool),
    /// The frame changed while it was looked at, by another party's act: a validation failed, a
    /// take lost, a transit's cell moved. Looked at again.
    Changed,
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
    /// Per class, the frame an eviction round's walk starts from next: where the last walk took a
    /// page. Relaxed: a place to start, any value of which is correct.
    walk: [AtomicU32; CLASS_COUNT],
    epoch_bytes: u64,
    /// Writebacks queued and not stepped to their end: a stepping thread takes them all and puts
    /// back each one not done.
    writebacks: Stack<Writeback>,
    /// Writebacks queued and not done, those a stepping thread holds included.
    queued: AtomicUsize,
    /// Where copies of pages written back while they stay in their frames are made.
    staging: Arc<staging::Staging>,
    writes_in_flight: AtomicUsize,
    /// The pool's own IO requests, reads and writes, handed to its backend and not yet landed:
    /// what decides a wait's kind (DESIGN.md 9.1.3, wait kinds). Never a writeback queued or held
    /// before its submission, nor a write landed and not yet settled: those are CPU work a thread
    /// does, whose end is an event. Up inside the submission's event, before the request, so
    /// before it can land; down inside the landing's event.
    requests: AtomicUsize,
    /// Bytes of swap copies written since the pool opened. Relaxed: a count.
    swap_written: AtomicU64,
    failure: SetOnce<CompletionError>,
    /// Pages that checkpoint found owed, kept as they stood by their first change or free since
    /// (the copy hook, audit 18).
    kept: Mutex<HashMap<PageId, KeptCopy>>,
    /// The frames lent beside the page table, and what for.
    lends: Arc<Lends>,
    /// The lend road's state for kept copies (DESIGN.md 9.1.1).
    lend: lend::LendRoad,
    /// The room the copies are charged to ([`kept`]).
    room: Arc<KeptRoom>,
    /// What a writer refused room drives: the checkpoint, once the database is open.
    relief: SetOnce<Arc<dyn Relief>>,
    /// The pages that checkpoint found owed and has neither imaged nor kept: one read in again is
    /// pending.
    cut_owed: Mutex<HashSet<PageId>>,
    temp: Arc<TempPages>,
    /// The operations waiting for memory (DESIGN.md 9.1.3, *A shortage never waits under a
    /// hold*).
    shortage: shortage::Queue,
    /// The pages the shortage gate's holder keeps resident, published while it holds the gate;
    /// `protecting` says whether any are, one load on eviction's path otherwise.
    protected: Mutex<Vec<u64>>,
    protecting: std::sync::atomic::AtomicBool,
    /// Each queued writeback's state after its last step, by its number, for a test that stalled
    /// to show while another thread steps it.
    #[cfg(test)]
    stepped: Mutex<HashMap<u64, String>>,
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
        let supply = Arc::new(supply::Supply::new(store.epochs().clone()));
        memory.join_pool(&supply, store.epochs());
        let batches = memory.batches;
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
                released: Zeroed::new(grains as usize)?,
                completions: CompletionDomain::new(store.epochs().clone()),
                supply: supply.clone(),
            }),
            table: Table::new(grains)?,
            memory,
            source,
            epoch: AtomicU32::new(watt::FIRST_EPOCH),
            evicted: AtomicU64::new(0),
            walk: std::array::from_fn(|_| AtomicU32::new(0)),
            epoch_bytes,
            writebacks: Stack::default(),
            queued: AtomicUsize::new(0),
            staging: Arc::new(staging::Staging::new(config.writeback_bytes)),
            writes_in_flight: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            swap_written: AtomicU64::new(0),
            failure: SetOnce::default(),
            kept: Mutex::new(HashMap::new()),
            lends,
            lend: lend::LendRoad::new(batches),
            room: KeptRoom::new(kept_bytes, supply.clone(), store.io().clone()),
            relief: SetOnce::default(),
            cut_owed: Mutex::new(HashSet::new()),
            temp: Arc::new(TempPages::new(
                store.io().clone(),
                store.base(),
                store.epochs().clone(),
            )?),
            store,
            shortage: shortage::Queue::new(supply.clone()),
            protected: Mutex::new(Vec::new()),
            protecting: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            stepped: Mutex::new(HashMap::new()),
        });
        pool.lends.set_pool(&pool);
        pool.lend_at_open(config.lent_at_open)?;
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
                let Some((version, hold)) = (match self.follow(frame, pid, mode)? {
                    Followed::Fixed(fixed) => fixed,
                    Followed::Wait(wait) => return Ok(IOResult::IO(IOCompletions(wait))),
                }) else {
                    continue;
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
            let frame = match self.claim(pid.class(), Purpose::Read, &[])? {
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
                    // The read's own wait only while its request is at the backend: the loader
                    // decides as any fix of the page does, by looking again (DESIGN.md 9.1.3, wait
                    // kinds; the auditor's P2-1 on v24). A read the source fills elsewhere is
                    // waited on remote, which its landing finishes. A read whose landing has begun
                    // has published its result before it was seen landed, so the loader keeps it.
                    crate::race::point("buffer.loader_follows");
                    if let Followed::Wait(wait) = self.wait_or_fix(frame, pid, mode)? {
                        if !loaded.finished() {
                            return Ok(IOResult::IO(IOCompletions(wait)));
                        }
                    }
                }
                // Landed meanwhile: its error is the loader's, kept on its own wait.
                if let Some(e) = loaded.get_error() {
                    return Err(LimboError::CompletionError(e));
                }
            }
            // Loaded already, or loaded by someone else first: find it again.
        }
    }

    /// [`wait_or_fix`](Self::wait_or_fix) with its first look inline: a frame free or latched as
    /// asked, or not holding `pid`, decided without the call.
    #[inline(always)]
    fn follow(self: &Arc<Self>, frame: u32, pid: PageId, mode: Mode) -> Result<Followed<'_>> {
        match self.probe(frame, pid, mode) {
            Probe::Hit(version) => Ok(Followed::Fixed(Some((version, None)))),
            Probe::Held(hold) => Ok(Followed::Fixed(Some((hold.version(), Some(hold))))),
            Probe::Miss => Ok(Followed::Fixed(None)),
            _ => self.wait_or_fix(frame, pid, mode),
        }
    }

    /// Follows `frame` for `pid`: the version an optimistic fix saw and an exclusive fix's hold,
    /// `None` if the frame does not hold the page, or what to wait on while another holds it
    /// (DESIGN.md 9.1.3). A resident frame waits on its own latch release. A loading or outgoing
    /// frame is looked at again after a ticket on the pool's supply is taken, so the second look
    /// decides the wait's kind and the ticket covers every change after it: a latch let go after
    /// it was marked, a read or a write submitted. A frame
    /// that changed while it was looked at is looked at again at once: that was another party's
    /// act, so the loop is lock-free, never a wait on anyone paused.
    #[cold]
    #[inline(never)]
    fn wait_or_fix(self: &Arc<Self>, frame: u32, pid: PageId, mode: Mode) -> Result<Followed<'_>> {
        let mut ticket = None;
        loop {
            match self.probe(frame, pid, mode) {
                Probe::Hit(version) => return Ok(Followed::Fixed(Some((version, None)))),
                Probe::Held(hold) => {
                    return Ok(Followed::Fixed(Some((hold.version(), Some(hold)))))
                }
                Probe::Miss => return Ok(Followed::Fixed(None)),
                Probe::Changed => ticket = None,
                Probe::ResidentBusy => {
                    return Ok(Followed::Wait(self.wait_for_resident_latch(frame)))
                }
                // Its IO submitted, the read lands and finishes it: stepped, its own producer. Once
                // it holds its result its landing is under way, and its event finishes a ticket.
                Probe::Loading(read, true) if !read.finished() => return Ok(Followed::Wait(read)),
                Probe::Outgoing(written, true) if !written.finished() => {
                    return Ok(Followed::Wait(written))
                }
                probe => {
                    let Some(taken) = ticket.take() else {
                        crate::race::point("buffer.fix_ticket");
                        ticket = Some(self.frames.supply.ticket());
                        continue;
                    };
                    match probe {
                        // Only the holder's release ends this hold: its marked release is an
                        // event, which finishes the ticket taken before the mark.
                        Probe::Busy(word) => match self.header(frame).latch.mark_held(word) {
                            Marked::Waits => return Ok(Followed::Wait(taken.remote())),
                            Marked::Changed => {}
                        },
                        // Only stepping its writeback submits its write, and settles it once it
                        // landed: stepped here, after the ticket, so a step of this call that
                        // submits or settles finishes it and the caller looks again; a writeback
                        // waiting on IO before its writes are submitted is waited on, stepped.
                        // What is left is another thread's: a writeback it holds, whose putting
                        // back or end is an event.
                        Probe::Outgoing(..) => {
                            if let Some(wait) = self.drive_writebacks()? {
                                return Ok(Followed::Wait(wait));
                            }
                            return Ok(Followed::Wait(taken.remote()));
                        }
                        // A read not yet submitted, whose submission is an event.
                        _ => return Ok(Followed::Wait(taken.remote())),
                    }
                }
            }
        }
    }

    /// Latches `frame` in `mode` if it holds `pid`.
    fn probe(&self, frame: u32, pid: PageId, mode: Mode) -> Probe<'_> {
        let h = self.header(frame);
        let version = match h.latch.look() {
            Look::Free(version) => version,
            // Held: being read in, written out, changed, evicted or freed.
            Look::Held(word) => {
                if h.pid() != pid.raw() {
                    return Probe::Miss;
                }
                let status = h.status();
                if status == LOADING || status == OUTGOING {
                    let c = match self.transit(frame).get(&self.frames.completions) {
                        Ok(c) => c,
                        Err(cell::Changed) => return Probe::Changed,
                    };
                    if let Some(c) = c {
                        // Read after the transit: a submission marked after its transit was put.
                        let submitted = h.at_backend(word.version());
                        return match status {
                            LOADING => Probe::Loading(c, submitted),
                            _ => Probe::Outgoing(c, submitted),
                        };
                    }
                }
                return if status == RESIDENT {
                    Probe::ResidentBusy
                } else {
                    Probe::Busy(word)
                };
            }
        };
        let holds = h.status() == RESIDENT && h.pid() == pid.raw();
        if !h.latch.validate(version) {
            return Probe::Changed;
        }
        if !holds {
            return Probe::Miss;
        }
        match mode {
            Mode::Optimistic => Probe::Hit(version),
            // Only at the version it was seen to hold the page at: a frame that changed hands
            // meanwhile is never held, so no one ever holds a free frame.
            Mode::Exclusive => match Held::try_take_at(self.frame_ref(frame), version) {
                Some(hold) => Probe::Held(hold),
                None => Probe::Changed,
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
        let loaded = crate::storage::atoll::notify::wait();
        self.transit(frame).put(loaded.clone());
        if self
            .table
            .insert(&self.frames.headers, pid.raw(), frame)
            .is_err()
        {
            self.frames.end_transit(frame);
            self.free_frame(pid.class(), frame, hold);
            // A reader may have followed a hint here meanwhile: send it to the other frame.
            notify::finish(&loaded, Ok(0));
            return Ok(IOResult::Done(None));
        }
        // The frame is quiet (claimed for a read) and held: nothing else reaches its memory while
        // the IO writes it.
        let into = self.memory.region.buffer(frame, pid.class().size());
        let version = hold.version();
        let reading = Loading {
            pool: Arc::downgrade(self),
            pid,
            hold: Some(hold),
            loaded: loaded.clone(),
        };
        // The read entering flight changes the wait a fix of the page needs, from the pool's to
        // the read's own, stepped (DESIGN.md 9.1.3, wait kinds): an event opened before the
        // submission and armed in this call, after the submission is marked, so every ticket
        // taken before the mark is finished. On an error or a yield the read never starts, and
        // dropping it frees the frame, which is a change too.
        crate::race::point("buffer.read_submit");
        let read = self.frames.supply.change(|event| {
            // Counted in flight and marked submitted by the source right before its request, so
            // before the read can land; the read ends, landed or given up, exactly once, through
            // `reading`, which counts it out if it was counted in, under the same hold.
            let submitting = || {
                self.requests.fetch_add(1, Ordering::AcqRel);
                h.submitted_at(version);
            };
            let read = located.read(into, Box::new(move |res| reading.end(res)), &submitting);
            event.arm();
            read
        });
        Ok(match read? {
            IOResult::Done(()) => {
                // A read returned done handed its request over or filled the page, so the fix that
                // started it waits on its request or has its result (source.rs, `Located::read`).
                crate::turso_assert!(
                    h.handed_over(version) || loaded.finished(),
                    "a page source's read returned neither handing its request over nor done"
                );
                IOResult::Done(Some(loaded))
            }
            IOResult::IO(io) => IOResult::IO(io),
        })
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
                let n = self.memory.cutting.load(Ordering::Acquire);
                let pending = n != 0 && owed && self.cut_owed.lock().contains(&pid);
                h.pending
                    .store(if pending { n } else { 0 }, Ordering::Release);
                h.status.store(RESIDENT, Ordering::Release);
                crate::race::point("buffer.loaded_published");
                drop(hold);
                if let Some(c) = waiting {
                    notify::finish(&c, Ok(0));
                }
                // The page is resident and its latch let go: evictable, so a claim waiting for
                // room has something to take now (DESIGN.md 9.1.2).
                self.signal_released();
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
            (Some(c), Some(e)) => notify::finish(&c, Err(e)),
            (Some(c), None) => notify::finish(&c, Ok(0)),
            (None, _) => {}
        }
    }

    /// Latches a free frame exclusively. No one else ever holds a free frame: every other hold
    /// is taken at a version seen with the frame holding a page.
    /// Marks the latch of `frame`, held by the caller, which is about to publish a new page in it,
    /// so the hold's first release tells the released frames (DESIGN.md 9.1.2): a claim that
    /// looked at the frame while it was free and found nothing to take is woken by the page
    /// becoming evictable, which no other event announces. Only a new page pays it.
    fn arm_first_release(&self, frame: u32) {
        let marked = self.header(frame).latch.mark_waited();
        debug_assert_eq!(
            marked,
            Marked::Waits,
            "frame {frame} is not held by its installer"
        );
    }

    /// `frame`'s latch, through a borrow of the frames.
    fn frame_ref(&self, frame: u32) -> FrameRef<'_> {
        FrameRef {
            frames: &self.frames,
            frame,
        }
    }

    fn take_free_latch(&self, frame: u32) -> Hold<'_> {
        Held::try_take(self.frame_ref(frame))
            .unwrap_or_else(|| panic!("free frame {frame} is held"))
    }

    /// Returns `frame`, unlinked, to the free frames, releasing `hold` on it.
    /// Publishes `pids` as the pages the gate's holder keeps resident (DESIGN.md 9.1.3, phase B).
    fn protect(&self, pids: &[u64]) {
        let mut set = self.protected.lock();
        set.clear();
        set.extend_from_slice(pids);
        self.protecting.store(!set.is_empty(), Ordering::Release);
    }

    /// Whether `frame` holds a page the gate's holder keeps resident.
    fn protected(&self, frame: u32) -> bool {
        if !self.protecting.load(Ordering::Acquire) {
            return false;
        }
        let pid = self.header(frame).pid();
        self.protected.lock().contains(&pid)
    }

    /// Gives back a frame a shortage reserved and its claim never took (DESIGN.md 9.1.3).
    fn give_back_reserved(&self, class: SizeClass, frame: u32) {
        self.free_frame(class, frame, self.take_free_latch(frame));
    }

    fn free_frame<L: LatchRef>(&self, class: SizeClass, frame: u32, hold: Held<L>) {
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
        // The shortage gate's holder waiting for a frame of this class takes it, its batch not
        // draining, rather than the free lists, where a fast claim would take it first
        // (DESIGN.md 9.1.3, phase A).
        let handed =
            !self.memory.draining_at(class, frame) && self.frames.supply.offer(class, frame);
        if !handed {
            // A batch this releases tells the pool itself (`batch_released`).
            self.memory.put(class, frame);
        }
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
        self.allocate_holding(class, &[])
    }

    /// [`allocate`](Self::allocate) for a step that holds the pages `held` meanwhile, which a
    /// shortage keeps resident while it waits for the frame (DESIGN.md 9.1.3, phase B).
    pub fn allocate_holding(
        self: &Arc<Self>,
        class: SizeClass,
        held: &[PageId],
    ) -> Result<IOResult<Exclusive<'_>>> {
        let step = self.store.epochs().enter();
        let frame = match self.claim(class, Purpose::New, held)? {
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
        let frame = match self.claim(pid.class(), Purpose::New, &[])? {
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
        self.allocate_temporary_holding(class, &[])
    }

    /// [`allocate_temporary`](Self::allocate_temporary) for a step that holds `held` meanwhile.
    pub fn allocate_temporary_holding(
        self: &Arc<Self>,
        class: SizeClass,
        held: &[PageId],
    ) -> Result<IOResult<Exclusive<'_>>> {
        let step = self.store.epochs().enter();
        let frame = match self.claim(class, Purpose::New, held)? {
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
        self.arm_first_release(frame);
        crate::race::point("buffer.install_publish");
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
            && self.memory.cutting.load(Ordering::Acquire) != 0
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
            let cut = self.memory.cutting.load(Ordering::Acquire) != 0 && !pid.is_temporary();
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
            let frame = match self.claim(class, Purpose::New, &[])? {
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
            self.arm_first_release(frame);
            crate::race::point("buffer.unreferenced_publish");
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
        // One take; on none, one grow and one take more (DESIGN.md 9.1.2): every look past those
        // is the caller's.
        for grown in [false, true] {
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
                Take::None if grown || !self.memory.grow(class) => return Got::None,
                Take::None => {}
            }
        }
        Got::None
    }

    /// A free frame of `class` for `purpose`, making room if there is none.
    fn claim(
        self: &Arc<Self>,
        class: SizeClass,
        purpose: Purpose,
        held: &[PageId],
    ) -> Result<IOResult<u32>> {
        self.failed()?;
        // The checkpoint's claims are its demand's, never an operation's (DESIGN.md 9.1.3,
        // *Maintenance keeps what it owns*).
        if let Some(out) = upkeep::claim(self, class, purpose) {
            return out;
        }
        if let Some(f) = shortage::take_reserved(class, purpose) {
            return Ok(IOResult::Done(f));
        }
        #[cfg(test)]
        if shortage::scarce_now() {
            return Ok(shortage::short(self, class, purpose, held));
        }
        let first = self.take_or_grow(class, purpose);
        if let Got::Frame(f) = first {
            return Ok(IOResult::Done(f));
        }
        // Inside an owner's operation, under its latches and Steps: no wait on memory here (W1,
        // DESIGN.md 9.1.3). Work it can start, a write in flight and the rare path's gate stay
        // as they are; what would be a wait on memory comes short, and the owner reserves the
        // frame holding nothing.
        let owned = shortage::owned();
        if owned && matches!(first, Got::Quieting(_)) {
            return Ok(shortage::short(self, class, purpose, held));
        }
        // Waiting starts before the next look, so a frame freed in between is not missed; and
        // before making room, whose marks on latches it finds held come after it (DESIGN.md 9.1.2).
        let mut wait = Some(self.frames.supply.ticket());
        let (room, next) = match first {
            Got::Quieting(_) => (Room::default(), self.take_or_grow(class, purpose)),
            _ => {
                let room = self.make_room(class)?;
                (room, self.take_or_grow(class, purpose))
            }
        };
        if owned {
            match next {
                Got::Frame(f) => return Ok(IOResult::Done(f)),
                Got::Quieting(_) => return Ok(shortage::short(self, class, purpose, held)),
                Got::None => {}
            }
        } else if let Some(done) = self.settle(next, &mut wait) {
            return done;
        }
        if let Some(c) = self.drive_writebacks()? {
            return Ok(IOResult::IO(IOCompletions(c)));
        }
        if self.writes_in_flight.load(Ordering::Acquire) > 0 {
            let wait = self.supply_wait(wait.expect("settle took no ticket"));
            return Ok(IOResult::IO(IOCompletions(wait)));
        }
        // Work of its own started, or a latch it found held changed under it: the caller looks
        // again on that, each look following something new.
        if room.started || room.changed {
            return Ok(Self::look_again());
        }
        // A run's drain lost the rare path's gate: its holder lets go within its own call, and that
        // let-go is all this waits on (DESIGN.md 9.1.3, rule 4).
        if let Some(gate) = room.gate {
            return Ok(IOResult::IO(IOCompletions(gate)));
        }
        if owned {
            return Ok(shortage::short(self, class, purpose, held));
        }
        // A fresh registration (the one before may be finished), one more look and one more round
        // of making room, so a frame freed or a page made evictable before it is not missed; every
        // outcome of that look handled as the second look's. Then the registration is returned: a
        // frame given back, a load landing, a write-out that did not happen and the release of
        // every latch the rounds marked waited after it finish it, whoever holds the latch, the
        // caller included, which then waits on itself and does not go round.
        crate::race::point("buffer.claim_fresh");
        let mut fresh = Some(self.frames.supply.ticket());
        if let Some(done) = self.settle(self.take_or_grow(class, purpose), &mut fresh) {
            return done;
        }
        let room = self.make_room(class)?;
        crate::race::point("buffer.claim_last_round");
        if room.started || room.changed {
            return Ok(Self::look_again());
        }
        if let Some(gate) = room.gate {
            return Ok(IOResult::IO(IOCompletions(gate)));
        }
        // A batch held for the class it was handed to, and nothing else to drain: the registration
        // goes on the stamp's epoch too, so the end of the last step under way at that hand-over
        // finishes it. Reached since the scan, the horizon moved: the caller looks again on that
        // rather than deciding again on the horizon it read (DESIGN.md 9.1.3).
        let fresh = self.supply_wait(fresh.expect("settle took no ticket"));
        if let Some(at) = room.held {
            if !self.store.epochs().wait_for(at, fresh.clone()) {
                return Ok(Self::look_again());
            }
        }
        Ok(IOResult::IO(IOCompletions(fresh)))
    }

    /// What a look that found a frame, or free frames not yet quiet, comes to: the frame, or `wait`
    /// put on the epoch the earliest is quiet at too (counting the caller's own step, as a frame's
    /// reuse must; its next step announces at or above the frame's stamp). None for no frame.
    fn settle(&self, got: Got, ticket: &mut Option<Ticket>) -> Option<Result<IOResult<u32>>> {
        match got {
            Got::Frame(f) => Some(Ok(IOResult::Done(f))),
            Got::Quieting(at) => {
                let wait = self.supply_wait(ticket.take().expect("a ticket taken before the look"));
                // Finished already if the epoch was reached: the caller looks again on that.
                self.store.epochs().wait_for(at, wait.clone());
                Some(Ok(IOResult::IO(IOCompletions(wait))))
            }
            Got::None => None,
        }
    }

    /// The wait a ticket gives a caller whose producers are other callers' acts and, maybe, the
    /// pool's own IO (DESIGN.md 9.1.3, wait kinds): stepped while any of its requests is at the
    /// backend, since stepping may be what lands it; remote otherwise, so a pumped backend with
    /// nothing of the pool's in flight is not stepped in a loop for it, whatever writeback another
    /// thread holds or write it has yet to settle. Decided after the look the ticket was taken
    /// before: a request submitted afterwards is an event, which finishes the remote kind, and the
    /// caller looks again and decides anew. The stepped kind is finished by the next landing
    /// alone: a request at the backend lands with steps, so its waiter steps until one does, and
    /// no other change, its own caller letting a page go on the way out included, sends it back
    /// to look with nothing stepped (the auditor's root-growth pressure, #3850).
    fn supply_wait(&self, ticket: Ticket) -> Completion {
        if self.io_in_flight() {
            ticket.stepped()
        } else {
            ticket.remote()
        }
    }

    /// Counts the read of the frame `hold` holds out of the requests and marks it landed, if its
    /// source handed it over, inside the landing's event and before anything else of it: a fix
    /// that looks from here waits on the pool, not on a read no request is left of; and the event
    /// is a landing, which finishes the stepped waits. The hold is the read's until here, so the
    /// frame's mark is this read's.
    fn out_of_flight(&self, hold: &Held<FrameLatch>, event: &supply::Event<'_>) {
        let (h, version) = (self.header(hold.latch().frame), hold.version());
        if h.at_backend(version) {
            h.landed_at(version);
            self.requests.fetch_sub(1, Ordering::AcqRel);
            event.land();
        }
        crate::race::point("buffer.read_landed");
    }

    /// Whether any of the pool's own IO requests is at its backend.
    fn io_in_flight(&self) -> bool {
        self.requests.load(Ordering::Acquire) > 0
    }

    /// A yield: its caller looks again at once, on the event the claim names. Allocates nothing.
    fn look_again<T>() -> IOResult<T> {
        IOResult::IO(IOCompletions(Completion::new_yield()))
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

    /// A ticket on the pool's supply, for a test that waits as `wait_for_room` does.
    #[cfg(test)]
    pub fn supply_ticket(&self) -> supply::Ticket {
        self.frames.supply.ticket()
    }

    /// A remote wait on the supply, finished by the next change, for a test.
    #[cfg(test)]
    fn released_wait(&self) -> Completion {
        self.frames.supply.ticket().remote()
    }

    /// A ticket on the pool's supply, the wait's kind decided after the look by
    /// [`supply_wait`](Self::supply_wait).
    fn ticket(&self) -> Ticket {
        self.frames.supply.ticket()
    }

    fn signal_released(&self) {
        self.frames.signal_released();
    }

    /// Frees frames for `class`: by moving memory from a class whose pages are worth less per
    /// byte, or else by evicting within the class.
    ///
    /// Whether it started something new that makes room: a drain of a batch no drain had, or an
    /// eviction or a write-out of a page of `class`.
    fn make_room(self: &Arc<Self>, class: SizeClass) -> Result<Room> {
        let draining = self.memory.draining();
        let mut room = Room::default();
        for &(holder, batch) in &draining {
            room |= self.drain(holder, batch)?;
        }
        if !draining.is_empty() {
            // Memory is on its way; meanwhile the class keeps evicting its own if it can.
            if self.memory.holds(class) {
                room |= self.evict_round(class)?;
            }
            return Ok(room);
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
        // `class` waiting on a drain that never starts. A batch held for the class it was handed
        // to is passed over, its stamp kept: with nothing else to drain, the claim waits on the
        // lowest (DESIGN.md 9.1.3).
        let passed = std::cell::Cell::new(None::<u64>);
        let colder = SizeClass::all()
            .filter(|&c| c != class)
            .filter_map(|c| {
                let batch = self.memory.emptiest(c, class, |b| {
                    if !self.drainable(class, b) {
                        return false;
                    }
                    match self.memory.held_until(class, b, horizon) {
                        Some(at) => {
                            passed.set(Some(passed.get().map_or(at, |p| p.min(at))));
                            false
                        }
                        None => true,
                    }
                })?;
                let (held, in_use, per_byte) = self.drain_cost(class, batch, horizon);
                Some((c, batch, held, in_use, per_byte))
            })
            .min_by(|a, b| a.2.cmp(&b.2).then(a.3.cmp(&b.3)).then(a.4.total_cmp(&b.4)));
        match colder {
            Some((holder, batch, _, in_use, per_byte))
                if own == f32::INFINITY || (in_use == 0 && per_byte < own) =>
            {
                crate::race::point("buffer.chosen");
                let (started, gate) = if class.size() <= memory::BATCH {
                    let started = self.memory.drain_chosen_for(holder, batch, class, horizon);
                    (started, None)
                } else {
                    match self.drain_run(class, batch, Some(horizon)) {
                        Drained::Ran(started) => (started, None),
                        Drained::GateLost(gate) => (false, Some(gate)),
                    }
                };
                // Not started, the batch may have been handed over since it was chosen: held,
                // its stamp is what the claim waits on.
                let held = (!started && gate.is_none())
                    .then(|| self.memory.held_until(class, batch, horizon))
                    .flatten();
                let mut room = Room {
                    started,
                    gate,
                    held,
                    ..Room::default()
                };
                room |= Room {
                    held: passed.get(),
                    ..Room::default()
                };
                if started {
                    for (holder, batch) in self.memory.draining() {
                        room |= self.drain(holder, batch)?;
                    }
                }
                Ok(room)
            }
            // Evicting its own: a batch passed over as held may, once let go, cost less than the
            // eviction did, so its stamp goes with the room whichever way the choice went.
            _ => {
                let mut room = self.evict_round(class)?;
                room |= Room {
                    held: passed.get(),
                    ..Room::default()
                };
                Ok(room)
            }
        }
    }

    /// Reserves and drains the run of `class` around `batch` holding the rare path's gate
    /// (DESIGN.md 9.1.1): no lend batch is published meanwhile, so a run holding a `LENT` batch is
    /// refused before it is reserved, and the pass gives a `LENDING` batch to the run by the one
    /// compare-and-swap that claims it. The gate is taken and let go inside one supply scope, so a
    /// batch the drain releases wakes its waiters once the scope closes, the gate already free
    /// (DESIGN.md 9.1.3). The gate lost, the gate's wait, taken before the try, is all the claim
    /// waits on: its holder lets go within its own call (rule 4).
    ///
    /// As a claim's choice made at `horizon` (`chosen`), nothing is reserved while a batch of the
    /// run is held for the class it was handed to (DESIGN.md 9.1.3).
    fn drain_run(&self, class: SizeClass, batch: u32, chosen: Option<u64>) -> Drained {
        let gate = self.frames.supply.gate_wait();
        self.frames.supply.change(|_| {
            if !self.memory.take_token(memory::RUN_TOKEN) {
                crate::race::point("buffer.run_token_lost");
                return Drained::GateLost(gate);
            }
            let started = !self.memory.run_holds_lent(class, batch)
                && self.memory.drain_run_chosen(class, batch, chosen);
            self.let_token_go(memory::RUN_TOKEN);
            Drained::Ran(started)
        })
    }

    /// Lets the rare path's gate go: freed, then every caller that lost it woken (DESIGN.md 9.1.3,
    /// rule 4). Nobody else: a gate let go is no supply.
    pub(super) fn let_token_go(&self, holder: u64) {
        self.memory.let_token_go(holder);
        self.frames.supply.gate_let_go();
    }

    /// Whether the memory around `batch` for `class` can be drained: no batch of its run is a
    /// lend batch, which no drain frees (DESIGN.md 9.1.1). A hint: `drain_run` checks it again
    /// holding the rare path's token.
    fn drainable(&self, class: SizeClass, batch: u32) -> bool {
        !self.memory.run_holds_lent(class, batch)
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
        #[cfg(test)]
        if let Some(t) = THRESHOLD.with(|t| t.get()) {
            return Some(t);
        }
        let now = self.now();
        let sample = self
            .memory
            .sample(class, watt::THRESHOLD_SAMPLE, &mut sample_rng());
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
    /// Whether it evicted a page or started a write-out.
    fn evict_round(self: &Arc<Self>, class: SizeClass) -> Result<Room> {
        let Some(threshold) = self.threshold(class) else {
            return Ok(Room::default());
        };
        let now = self.now();
        let mut candidates = self
            .memory
            .sample(class, watt::CANDIDATES, &mut sample_rng());
        candidates.sort_unstable();
        candidates.dedup();
        let mut outgoing = Vec::new();
        let mut evicted = false;
        let mut room = Room::default();
        // Over the threshold: the candidates a cold value in the threshold's sample left.
        let mut over = Vec::new();
        for frame in candidates {
            let h = self.header(frame);
            let value = h.tracker.value(now);
            if value > threshold {
                over.push((value, frame));
                continue;
            }
            evicted |= self.evict_or_write(class, frame, &mut outgoing, &mut room);
        }
        // A round that evicted nothing and started no write-out takes, from the candidates it
        // read, the one of least value it can latch; and if none, walks the class's frames from
        // where its last walk stopped, at most once round, for the first page it can latch
        // (DESIGN.md 9.1.2). A sample can hold no page at all while pages are there to take, as
        // when nearly every frame is lent. So a round frees a frame, starts a write, or found every
        // page of the class latched, held or outgoing.
        if !evicted && outgoing.is_empty() {
            over.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
            let taken = over
                .into_iter()
                .any(|(_, frame)| self.evict_or_write(class, frame, &mut outgoing, &mut room));
            evicted = taken || self.walk_to_evict(class, &mut outgoing, &mut room);
        }
        let wrote = !outgoing.is_empty();
        self.write_out(class, outgoing);
        room.started = evicted || wrote;
        Ok(room)
    }

    /// Walks `class`'s frames from where the last walk stopped, at most once round, and evicts or
    /// starts writing out the first resident page it can latch: whether it did.
    fn walk_to_evict(
        self: &Arc<Self>,
        class: SizeClass,
        outgoing: &mut Vec<Outgoing>,
        room: &mut Room,
    ) -> bool {
        let start = self.walk[class.index()].load(Ordering::Relaxed);
        let after = self.memory.live_frames(class).filter(|&f| f >= start);
        let before = self.memory.live_frames(class).take_while(|&f| f < start);
        for frame in after.chain(before) {
            if self.header(frame).status() == RESIDENT
                && self.evict_or_write(class, frame, outgoing, room)
            {
                self.walk[class.index()].store(frame + 1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    /// Evicts the page in `frame` if it is clean, or starts writing it out, if its latch can be
    /// taken: whether it did either. A resident page of the class whose latch is held, not for a
    /// write, is marked waited, after the claim's registration (DESIGN.md 9.1.2).
    fn evict_or_write(
        self: &Arc<Self>,
        class: SizeClass,
        frame: u32,
        outgoing: &mut Vec<Outgoing>,
        room: &mut Room,
    ) -> bool {
        let latch = || FrameLatch {
            frames: self.frames.clone(),
            frame,
        };
        if self.protected(frame) {
            return false;
        }
        let Some(hold) = self.latch_to_evict(class, frame, latch) else {
            crate::race::point("buffer.evict_lost");
            // The take was lost: the latch was held, or let go or taken again since it was read.
            // Marked whatever it reads now (a free latch's mark reports it changed), so a release
            // between the lost take and the mark is the change it is, never nothing (DESIGN.md
            // 9.1.2).
            let h = self.header(frame);
            let theirs = h.status() == RESIDENT
                && PageId::from_raw(h.pid()).class() == class
                && !h.has(WRITING);
            if theirs {
                match h.latch.mark_waited() {
                    Marked::Waits => room.marked = true,
                    Marked::Changed => room.changed = true,
                }
            }
            return false;
        };
        if self.header(frame).dirty() {
            // Written out of its frame, then gone: a copy to write from would take a frame of its
            // own, which a full pool has none of to spare (one memory).
            outgoing.push(self.start_write_out(hold));
        } else {
            self.evict(class, frame, hold);
        }
        true
    }

    /// Evicts the clean page in `frame`, which `hold` latches.
    fn evict<L: LatchRef>(&self, class: SizeClass, frame: u32, hold: Held<L>) {
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
    ///
    /// What it did, as a round of making room says it: a page evicted or a write-out started, and
    /// every latch it found held marked waited or found changed, as eviction marks them (DESIGN.md
    /// 9.1.2): a claim parked on the drain is woken by those releases too.
    fn drain(self: &Arc<Self>, class: SizeClass, batch: u32) -> Result<Room> {
        let mut outgoing = Vec::new();
        let mut room = Room::default();
        for i in 0..memory::frames_per_batch(class) {
            let frame = memory::frame_at(class, batch, i);
            if self.memory.is_free(class, frame) {
                continue;
            }
            let took = self.evict_or_write(class, frame, &mut outgoing, &mut room);
            room.started |= took;
        }
        room.started |= !outgoing.is_empty();
        self.write_out(class, outgoing);
        Ok(room)
    }

    /// Latches `frame` exclusively, through `latch`, to evict its page, if it holds a page of
    /// `class` not being written. A frame chosen while its batch was `class`'s may by now be part
    /// of another class's frame, holding a page of that class or none: evicting that page as
    /// `class`'s would give its memory to two pages.
    fn latch_to_evict<L: LatchRef>(
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
        crate::race::point("buffer.evict_take");
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
        self.transit(frame)
            .put(crate::storage::atoll::notify::wait());
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
        self.frames.supply.change(|event| {
            // The frames are OUTGOING before this take. A late registration acquires its empty
            // swap and sees that status; earlier waiters look again once the writes are queued.
            // Wakers stay deferred until every writeback below has been published.
            for out in &outgoing {
                self.frames.finish_frame_wait(out.frame());
            }
            crate::race::point("buffer.outgoing_waits_finished");
            let (temporary, logged): (Vec<Outgoing>, Vec<Outgoing>) =
                outgoing.into_iter().partition(|o| o.pid.is_temporary());
            for (outgoing, temporary) in [(logged, false), (temporary, true)] {
                if !outgoing.is_empty() {
                    self.queue(Writeback::out(class, outgoing, temporary));
                }
            }
            event.arm();
        });
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
                    notify::finish(&c, Ok(0));
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
    fn write_landed(&self, frame: u32, write_out: Option<u64>) {
        // Its event opened before the request leaves the count, and a write-out's mark says it
        // landed before anything else of it (DESIGN.md 9.1.3).
        self.frames.supply.change(|event| {
            if let Some(version) = write_out {
                self.header(frame).landed_at(version);
            }
            self.requests.fetch_sub(1, Ordering::AcqRel);
            crate::race::point("buffer.write_landed");
            if write_out.is_some() {
                if let Ok(Some(c)) = self.transit(frame).get(&self.frames.completions) {
                    notify::finish(&c, Ok(0));
                }
                // A changed cell belongs to a concurrent take or another transit. Its owner
                // finishes the removed wait; the event still wakes the writeback drivers.
            }
            event.land();
        });
    }

    /// Done once no writeback is queued or in flight; meanwhile, drives the queued ones.
    pub fn settle_writes(self: &Arc<Self>) -> Result<IOResult<()>> {
        let _step = self.store.epochs().enter();
        // Waiting starts before the look, so a write ending in between is not missed.
        let wait = self.ticket();
        if let Some(c) = self.drive_writebacks()? {
            return Ok(IOResult::IO(IOCompletions(c)));
        }
        // One queued and held by another thread may not have submitted its writes yet.
        if self.writes_in_flight.load(Ordering::Acquire) > 0
            || self.queued.load(Ordering::Acquire) > 0
        {
            return Ok(IOResult::IO(IOCompletions(self.supply_wait(wait))));
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
            .then(|| Held::try_take(self.frame_ref(frame)))
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

    /// A lent frame given back to its lend batch (DESIGN.md 9.1.1).
    fn take_back(&self, class: SizeClass, frame: u32) {
        debug_assert_eq!(
            self.header(frame).status(),
            LENT,
            "frame {frame} given back twice"
        );
        debug_assert!(self.memory.is_lend_batch(frame / memory::GRAINS));
        self.give_back_lent(class, frame);
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
        let n = self.memory.cutting.load(Ordering::Acquire);
        assert!(
            n == 0 || !self.owes_copy(frame, pid, n),
            "{pid:?} changed past checkpoint {n}'s cut with no room asked for its copy"
        );
    }

    /// `pid`, in `frame` latched exclusively, is a page the cut in effect left out: owed no
    /// image, it needs no copy.
    fn left_out_of_cut(&self, frame: u32, pid: PageId) {
        let n = self.memory.cutting.load(Ordering::Acquire);
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

    /// Copies `pid`, in `frame` latched exclusively, into `block` for cut `n`, its room charged.
    ///
    /// Kept only under its own cut (DESIGN.md 9.1.1): a failed checkpoint's abandon may end cut `n`
    /// between the change's reading of it and this insertion. `end_cut` stores `cutting` before it
    /// takes the map's mutex and empties the map, so under that mutex either the map is emptied
    /// after this insertion or `cutting` no longer reads `n`; then the copy is dropped, after the
    /// guard is let go, since its block gives its frame and the room's bytes back and wakes their
    /// waiters, and no wake runs under the map's mutex.
    fn copy_for_cut(&self, frame: u32, pid: PageId, mut block: AlignedBlock, n: u64) {
        self.bytes(frame, pid.class()).copy_to(0, block.bytes_mut());
        let copy = KeptCopy::charged(block, &self.room);
        crate::race::point("buffer.copy_for_cut");
        let mut kept = self.kept.lock();
        let late = if self.memory.cutting.load(Ordering::SeqCst) == n {
            kept.insert(pid, copy);
            None
        } else {
            Some(copy)
        };
        drop(kept);
        drop(late);
        self.cut_owed.lock().remove(&pid);
        self.header(frame).pending.store(0, Ordering::Release);
    }

    /// Room for the copies the cut in effect needs of `pages`, each latched exclusively, before
    /// any of them changes: every one still owed a copy is copied as it stands, all within the
    /// room and the lend road's frames or none (DESIGN.md 9.1.1). Refused, the change lets go of
    /// every latch it holds and of the change gate, then hands the refusal to
    /// [`wait_for_room`](Self::wait_for_room) and starts again: nothing yields once a change has
    /// begun. `wait` is the operation's wait for room, held across its retries (DESIGN.md 22.4):
    /// refused room, it holds a ticket for the bytes asked for, and the change admitted lets it go.
    /// A change refused frames keeps it: it is not admitted yet.
    pub fn keep(
        self: &Arc<Self>,
        pages: &[&Exclusive<'_>],
        wait: &mut RoomWait,
    ) -> std::result::Result<(), Refused> {
        let kept = self.try_keep(pages);
        match &kept {
            Ok(()) => self.room.admitted(wait),
            Err(Refused {
                why: lend::Why::Room { bytes },
            }) => self.room.refused(*bytes, wait, || {
                self.relief.get_ref().and_then(|relief| relief.demand())
            }),
            Err(_) => {}
        }
        kept
    }

    fn try_keep(self: &Arc<Self>, pages: &[&Exclusive<'_>]) -> std::result::Result<(), Refused> {
        let n = self.memory.cutting.load(Ordering::Acquire);
        if n == 0 {
            return Ok(());
        }
        let bytes = self.owed_bytes(pages, n);
        if bytes == 0 {
            return Ok(());
        }
        if !self.room.charge(bytes) {
            return Err(Refused {
                why: lend::Why::Room { bytes },
            });
        }
        // A frame for each copy, every one or none, before any is made, from the lend road, which
        // never waits under the latches.
        let mut want = lend::Want::default();
        for (i, p) in pages.iter().enumerate() {
            if pages[..i].iter().any(|q| q.frame == p.frame) || !self.owes_copy(p.frame, p.pid, n) {
                continue;
            }
            want.add(p.pid.class());
        }
        let mut lent = match self.lend_copies(&want) {
            Ok(lent) => lent,
            Err(refused) => {
                self.room.uncharge(bytes);
                return Err(refused);
            }
        };
        for (i, p) in pages.iter().enumerate() {
            if pages[..i].iter().any(|q| q.frame == p.frame) || !self.owes_copy(p.frame, p.pid, n) {
                continue;
            }
            let class = p.pid.class();
            let frame = lent.take(class).expect("a frame lent for each copy");
            let block = self.lent_block(class, frame);
            self.copy_for_cut(p.frame, p.pid, block, n);
        }
        Ok(())
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

    /// For a change refused (`refused`), which has let go of every latch and the change gate and
    /// made every undo of its attempt (DESIGN.md 9.1.1, Turned down; 9.1.3, rule 5): it takes its
    /// ticket on the supply here, then reads, taking nothing, whether what it lacks is there, and
    /// starts again at once on a yes. Else, refused frames, it provides; else relief runs units of
    /// the checkpoint, the only producer of returned frames and room, or the writer waits on its
    /// ticket, of the remote kind. An embedded database may have one connection and no other
    /// thread, so the writer waiting is the one to step it. Done to start the change again at
    /// once.
    pub fn wait_for_room(self: &Arc<Self>, refused: Refused) -> Result<IOResult<()>> {
        let lend::Why::Frames {
            want,
            gate,
            quiet_at,
        } = refused.why
        else {
            // The room's bytes. The ticket first, here, after every undo the refusal made and with
            // the writer's latches and change gate let go (DESIGN.md 9.1.3, rule 5); then the read:
            // no cut, or the bytes the change was refused now there.
            let ticket = self.frames.supply.ticket();
            if self.memory.cutting.load(Ordering::Acquire) == 0 || self.room.takes_refused() {
                return Ok(IOResult::Done(()));
            }
            return match self.relieve(Retry::Room(&ticket)) {
                Relieved::Stepped(stepped) => stepped,
                Relieved::Held | Relieved::Elsewhere => {
                    Ok(IOResult::IO(IOCompletions(self.room.waiting(ticket))))
                }
            };
        };
        // The rare path's gate lost: its holder lets go within its own call, and that is all the
        // request waits on (rule 4).
        if let Some(gate) = gate {
            return Ok(IOResult::IO(IOCompletions(gate)));
        }
        // The ticket after every undo, then the read without taking anything: no cut, or what
        // the change lacks reads as there now (rule 5).
        let ticket = self.frames.supply.ticket();
        if self.memory.cutting.load(Ordering::Acquire) == 0 || self.lend_hint(&want) {
            return Ok(IOResult::Done(()));
        }
        // A cache batch outside the core: drained for the lend road, a real wait on its writes
        // and loads, then the change again.
        if let Some(provided) = self.provide()? {
            return Ok(provided);
        }
        let retry = Retry::Frames {
            want: &want,
            ticket: &ticket,
        };
        match self.relieve(retry) {
            Relieved::Stepped(stepped) => return stepped,
            // Another caller steps the checkpoint, or the checkpointing thread does: a state let
            // go and every frame the checkpoint returns are changes of the supply the ticket is on.
            Relieved::Held | Relieved::Elsewhere => {}
        }
        let wait = self.room.waiting(ticket);
        if let Some(at) = quiet_at {
            // Short only of a free batch not yet quiet: that epoch is a producer too.
            self.store.epochs().wait_for(at, wait.clone());
        }
        Ok(IOResult::IO(IOCompletions(wait)))
    }

    /// For a change the change gate refused, its operation unwound to its top holding nothing
    /// (DESIGN.md 9.1.3, the change gate closed by a cut): its wait on the gate's reopening,
    /// registered first; then relief steps the cut, as a writer refused room does, since an
    /// embedded database may have one connection and no other thread; else, another caller or the
    /// checkpointing thread stepping it, the wait. Done to start the change again at once.
    pub fn wait_for_gate(self: &Arc<Self>) -> Result<IOResult<()>> {
        let Some(reopened) = self.store.gate_wait() else {
            return Ok(IOResult::Done(()));
        };
        match self.relieve(Retry::Gate) {
            Relieved::Stepped(stepped) => stepped,
            Relieved::Held | Relieved::Elsewhere => Ok(IOResult::IO(IOCompletions(reopened))),
        }
    }

    /// The relief a refused writer runs: [`Relieved::Elsewhere`] with none set.
    fn relieve(&self, retry: Retry<'_>) -> Relieved {
        self.relief
            .get_ref()
            .map_or(Relieved::Elsewhere, |relief| relief.relieve(retry))
    }

    /// Sets what a writer refused room drives: once, when the database opens.
    pub fn set_relief(&self, relief: Arc<dyn Relief>) {
        self.relief.set(relief);
    }

    /// Whoever waits for room tries again: a stepper of the checkpoint leaving.
    pub fn wake_room(&self) {
        self.room.wake();
    }

    /// Waits for room handed out that block until another thread gives it back.
    #[cfg(test)]
    pub fn blocking_room_waits(&self) -> u64 {
        self.room.blocking_waits()
    }

    /// Whether a refused writer may start its change again (DESIGN.md 9.1.1, Turned down): no cut
    /// is in effect, or what it waits for came, the room taking the largest request it refused
    /// since it last admitted one, or a frame of a class it wants. A refused writer stepping the
    /// checkpoint stops then, at either exit of a unit.
    pub fn may_retry(&self, retry: Retry<'_>) -> bool {
        let uncut = self.memory.cutting.load(Ordering::Acquire) == 0;
        match retry {
            // The gate closes before the cut takes effect and opens after: no cut in effect says
            // nothing of it.
            Retry::Gate => !self.store.gate_closed(),
            Retry::Room(_) => uncut || retry.moved() && self.room.takes_refused(),
            Retry::Frames { want, .. } => uncut || retry.moved() && self.lend_hint(want),
        }
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
    /// listed until it is imaged or kept: one read in again comes in pending. Each call does one
    /// run of the walk (DESIGN.md 22.1, 22.9 item 1): the frame headers one page of the header
    /// table holds ([`CUT_RUN`]), or one shard of the swap table, or [`CUT_RUN`] of the swapped
    /// pages looked at again. Done(true) once the cut has found every page owed.
    pub fn cut_run(&self, n: u64, scan: &mut CutScan) -> IOResult<bool> {
        self.memory.cutting.store(n, Ordering::SeqCst);
        let grains = self.memory.grains();
        if scan.next < grains {
            let end = grains.min(scan.next.saturating_add(CUT_RUN as u32));
            while scan.next < end {
                let h = self.header(scan.next);
                if h.status() == RESIDENT {
                    // A change holds it for a moment: come back.
                    let Some(hold) = Held::try_take(self.frame_ref(scan.next)) else {
                        return IOResult::IO(IOCompletions(
                            self.wait_for_resident_latch(scan.next),
                        ));
                    };
                    let listed = scan.owed.len();
                    self.mark_owed(scan.next, n, &mut scan.owed);
                    hold.release_unchanged();
                    scan.seen.extend(scan.owed[listed..].iter().copied());
                } else if h.status() == OUTGOING {
                    let listed = scan.owed.len();
                    if !self.record_outgoing(scan.next, &mut scan.owed) {
                        // The write-out finished or the frame changed hands while its header was
                        // read.
                        return IOResult::IO(IOCompletions(Completion::new_yield()));
                    }
                    scan.seen.extend(scan.owed[listed..].iter().copied());
                }
                scan.next += 1;
            }
            return IOResult::Done(false);
        }
        let shards = self.store.swap_shards();
        if scan.shard < shards {
            // The pages owed so far are in `seen` already, each run of the frames having put its
            // own there, so a swapped page among them is listed once.
            for pid in self.store.swap_shard_pages(scan.shard) {
                if scan.seen.insert(pid) {
                    scan.owed.push(pid);
                }
                scan.swapped.push(pid);
            }
            scan.shard += 1;
            if scan.shard == shards {
                *self.cut_owed.lock() = std::mem::take(&mut scan.seen);
            }
            return IOResult::Done(false);
        }
        // A page with a swap copy read in after the walk passed its frame, before the list was
        // set: unchanged since, the gate being closed.
        // Only a resident page is marked here: one loading marks itself pending as it lands, the
        // list being set, and one leaving leaves its frame, read back in pending. Their latches
        // are held until IO no one here steps ends.
        for _ in 0..CUT_RUN {
            let Some(&pid) = scan.swapped.last() else {
                return IOResult::Done(true);
            };
            if let Some(frame) = self.table.find(&self.frames.headers, pid.raw()) {
                let h = self.header(frame);
                if h.status() == RESIDENT {
                    let Some(hold) = Held::try_take(self.frame_ref(frame)) else {
                        return IOResult::IO(IOCompletions(self.wait_for_resident_latch(frame)));
                    };
                    if h.status() == RESIDENT && h.pid() == pid.raw() {
                        h.pending.store(n, Ordering::Release);
                    }
                    hold.release_unchanged();
                }
            }
            scan.swapped.pop();
        }
        IOResult::Done(scan.swapped.is_empty())
    }

    /// [`cut_run`](Self::cut_run) until the cut has found every page owed, for a test. Step it
    /// until done.
    #[cfg(test)]
    pub fn begin_cut(&self, n: u64, scan: &mut CutScan) -> IOResult<()> {
        loop {
            match self.cut_run(n, scan) {
                IOResult::Done(true) => return IOResult::Done(()),
                IOResult::Done(false) => {}
                IOResult::IO(io) => return IOResult::IO(io),
            }
        }
    }

    /// Waits for a resident frame's held latch. A frame no longer resident, a free latch, or a
    /// changed hold returns a yield so the caller fixes the page again under its current status.
    /// A free latch constructs no signal. A held word's signal is registered before its status
    /// is rechecked and that word is marked; its release finishes it after unlocking. A queued
    /// OUTGOING transition finishes it before its deferred wake.
    pub(super) fn wait_for_resident_latch(&self, frame: u32) -> Completion {
        let h = self.header(frame);
        crate::race::point("buffer.frame_before_look");
        let seen = match h.latch.look() {
            Look::Free(_) => return Completion::new_yield(),
            Look::Held(seen) => seen,
        };
        let wait = self.frames.released[frame as usize].current_with(
            &self.frames.completions,
            crate::storage::atoll::notify::remote_wait,
        );
        crate::race::point("buffer.frame_wait_registered");
        // The status belongs to the hold just observed. A clean eviction and a new load can
        // change both since the caller first found a resident frame.
        if h.status() != RESIDENT {
            return Completion::new_yield();
        }
        crate::race::point("buffer.frame_before_mark");
        match h.latch.mark_held(seen) {
            Marked::Waits => wait,
            Marked::Changed => Completion::new_yield(),
        }
    }

    /// Records an owed page frozen by its write-out, even before its first swap copy exists.
    /// The frame's hold stays at one version until the write frees it. The version check rejects
    /// a header read across that free and a reuse of the frame.
    fn record_outgoing(&self, frame: u32, owed: &mut Vec<PageId>) -> bool {
        let h = self.header(frame);
        let seen = match h.latch.look() {
            Look::Held(seen) => seen,
            Look::Free(_) => return false,
        };
        crate::race::point("buffer.cut_outgoing_seen");
        let pid = PageId::from_raw(h.pid());
        let needed = !pid.is_temporary() && h.owed();
        let outgoing = h.status() == OUTGOING;
        crate::sync::atomic::fence(Ordering::Acquire);
        let same_hold =
            matches!(h.latch.look(), Look::Held(now) if now.version() == seen.version());
        if !outgoing || !same_hold {
            return false;
        }
        if needed {
            owed.push(pid);
        }
        true
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
        let Some(hold) = Held::try_take(self.frame_ref(frame)) else {
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

    /// The operations waiting in the shortage queue or holding its gate, for a test.
    #[cfg(test)]
    pub(crate) fn shortage_len(&self) -> usize {
        self.shortage.len()
    }

    /// Evicts `pid`, resident, clean and unheld, for a test: the page is then in its copies alone.
    /// Whether it was.
    #[cfg(test)]
    pub fn evict_clean(&self, pid: PageId) -> bool {
        let Some(frame) = self.table.find(&self.frames.headers, pid.raw()) else {
            return false;
        };
        let h = self.header(frame);
        let Some(hold) = self.latch_to_evict(pid.class(), frame, || self.frame_ref(frame)) else {
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
        self.memory.cutting.load(Ordering::Acquire)
    }

    /// Checkpoint `n` has imaged every page its cut found owed: changes no longer keep pages.
    pub fn end_cut(&self, n: u64) {
        debug_assert_eq!(self.memory.cutting.load(Ordering::Acquire), n);
        self.memory.cutting.store(0, Ordering::SeqCst);
        // Each copy's room is given back as it drops.
        let kept = std::mem::take(&mut *self.kept.lock());
        drop(kept);
        self.cut_owed.lock().clear();
        // Every empty copy batch and every batch a provide left lending goes back free: with the
        // cut cleared first, a last return, a provide's release and this sweep cannot all miss
        // one another (DESIGN.md 9.1.1). One change of the supply, whatever the sweep gave back: a
        // writer refused frames or room needs no copy once the cut has ended, and starts again.
        self.frames.supply.change(|event| {
            self.sweep_lend_batches();
            self.room.cut_ended();
            event.arm();
        });
    }
}

/// What eviction samples frames with: the thread's generator, and under shuttle one seeded from
/// shuttle's own, so a failing schedule replays the same samples and so the same latches and frames.
fn sample_rng() -> impl rand::Rng {
    #[cfg(shuttle)]
    {
        use rand::SeedableRng;
        use shuttle::rand::RngCore;
        rand::rngs::StdRng::seed_from_u64(shuttle::rand::thread_rng().next_u64())
    }
    #[cfg(not(shuttle))]
    {
        rand::rng()
    }
}

/// The frames a cut's walk looks at in one run: the frame headers one page of the header table
/// holds (DESIGN.md 22.1), and as many of the swapped pages looked at again.
pub const CUT_RUN: usize = page::MIN_PAGE_SIZE / std::mem::size_of::<FrameHeader>();

/// A cut's walk over the frames for the pages owed an image ([`BufferPool::cut_run`]).
#[derive(Default)]
pub struct CutScan {
    next: u32,
    pub owed: Vec<PageId>,
    /// The next shard of the swap table to list, the pages owed listed so far, and the swapped
    /// pages still to look at again.
    shard: usize,
    seen: HashSet<PageId>,
    swapped: Vec<PageId>,
}

/// What a writer refused room for kept copies drives ([`BufferPool::wait_for_room`]).
pub trait Relief: Send + Sync {
    /// Steps the checkpoint whose cut is in effect until the writer may start again (`retry`,
    /// [`BufferPool::may_retry`]), or says who steps it instead.
    fn relieve(&self, retry: Retry<'_>) -> Relieved;

    /// A refused writer's demand on the checkpoint, of the relief class (DESIGN.md 22.4), held by
    /// its room ticket: none while the checkpoint is no task, or for the task's own step.
    fn demand(&self) -> Option<crate::storage::atoll::executor::Ticket>;
}

/// What a refused writer's relief did.
pub enum Relieved {
    /// Stepped the checkpoint: done, or waiting on its IO.
    Stepped(Result<IOResult<()>>),
    /// Another caller holds the checkpoint's state; letting it go wakes the room's waiters, and the
    /// writer steps it then.
    Held,
    /// The checkpointing thread steps it, or there is no checkpoint to step: nothing for the
    /// writer to do but wait for what it registered on.
    Elsewhere,
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
    fn read(
        self: Box<Self>,
        into: Arc<Buffer>,
        done: ReadDone,
        submitting: source::Submitting<'_>,
    ) -> Result<IOResult<()>> {
        self.temp.read(self.pid, into, done, submitting)?;
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

    /// Upgrades an unchanged page, or returns the wait for the hold that prevented the upgrade.
    /// A moved version yields so the caller fixes the page again.
    pub fn upgrade_or_wait(self) -> std::result::Result<Exclusive<'a>, Completion> {
        let (pool, frame) = (self.pool, self.frame);
        self.upgrade()
            .ok_or_else(|| pool.wait_for_resident_latch(frame))
    }

    /// The hold preventing an optimistic read from validating, or a yield after a change.
    pub fn wait_if_held(&self) -> Completion {
        self.pool.wait_for_resident_latch(self.frame)
    }

    /// Latches the page exclusively if it is unchanged since it was fixed.
    pub fn upgrade(self) -> Option<Exclusive<'a>> {
        let (pool, frame, pid) = (self.pool, self.frame, self.pid);
        Held::try_take_at(pool.frame_ref(frame), self.version).map(|hold| Exclusive {
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
