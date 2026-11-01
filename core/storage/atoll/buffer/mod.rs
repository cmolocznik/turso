//! The buffer manager (DESIGN.md §9).
//!
//! - [`memory`]: one region of frames on huge pages, lent to size classes 2 MiB at a time.
//! - [`table`]: the hash table from page id to frame, the only translation.
//! - [`frame`]: frame headers: the latch, the page id, dirtiness and the replacement tracker.
//! - [`watt`]: write-aware timestamp tracking, which chooses what to evict.
//! - [`clean`]: writing dirty pages back through the page store.
//! - [`pass`]: checkpoint passes over shards of frames, which bound the log recovery replays.
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

mod clean;
mod frame;
mod memory;
mod pass;
mod source;
mod staging;
mod table;
mod temp;
mod watt;

#[cfg(test)]
mod tests;

pub use memory::MIN_BUDGET;
pub use pass::Pass;
pub use source::{LocalSource, Located, PageSource, ReadDone};

use super::epoch::Step;
use super::latch::{Held, VersionedLatch};
use super::log::Lsn;
use super::page::{self, PageId, SizeClass, CLASS_COUNT};
use super::shared::{BytesMut, Shared, Writer};
use super::store::PageStore;
use clean::{Progress, Writeback};
use frame::{
    FrameHeader, ABANDONED, EVICT_AFTER, FREE, LOADING, NO_PAGE, OUTGOING, RESIDENT, WRITING,
};
use memory::{Memory, Take, Zeroed};
use pass::Shard;
use table::Table;
use temp::TempPages;

use crate::io::{Buffer, Completion};
use crate::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};
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
    /// what its memory held has ended ([`BufferPool::quiet`]).
    given_up: Zeroed<AtomicU64>,
    /// Per shard of frames: the IO in flight for each loading or outgoing frame.
    transits: Box<[Mutex<HashMap<u32, Completion>>]>,
}

impl Frames {
    fn transits_of(&self, frame: u32) -> &Mutex<HashMap<u32, Completion>> {
        &self.transits[frame as usize & (self.transits.len() - 1)]
    }

    /// Leaves the page whose write-out did not happen in the frame `hold` holds, and sends
    /// whoever waits on the write to look again.
    fn kept(&self, hold: Held<FrameLatch>) {
        let frame = hold.latch().frame;
        let waiting = self.transits_of(frame).lock().remove(&frame);
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
/// it, never the pool.
struct Loading {
    pool: Arc<BufferPool>,
    pid: PageId,
    hold: Option<Held<FrameLatch>>,
}

impl Loading {
    fn end(mut self, res: std::result::Result<(), CompletionError>) {
        let hold = self.hold.take().expect("a read ends once");
        self.pool.loaded(self.pid, hold, res);
    }
}

impl Drop for Loading {
    fn drop(&mut self) {
        if let Some(hold) = self.hold.take() {
            self.pool.unload(self.pid, hold, None);
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

    /// Ends the write-out with the write of a copy holding changes up to `lsn`.
    pub(super) fn end(
        mut self,
        pool: &BufferPool,
        lsn: Lsn,
        res: std::result::Result<(), CompletionError>,
    ) {
        let hold = self.hold.take().expect("a write-out ends once");
        pool.written_out(self.pid, hold, lsn, res);
    }
}

impl Drop for Outgoing {
    fn drop(&mut self) {
        if let Some(hold) = self.hold.take() {
            let frames = hold.latch().frames.clone();
            frames.kept(hold);
        }
    }
}
use std::collections::HashMap;
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Bytes of memory for frames, at least [`MIN_BUDGET`]. Frame headers take 2% more and the
    /// hash table 0.2%, and none of it takes physical memory until used.
    pub budget: usize,
    /// Shards of frames that checkpoint passes take in turn ([`Pass`]). Haubenschild et al.
    /// (SIGMOD 2020, §3.4) found 128 smoother than 10: each pass writes less at once, and the
    /// log kept for recovery strays less from its budget.
    pub checkpoint_shards: usize,
    /// Bytes of the writeback buffer ([`staging`]): what copies of pages written back while they
    /// stay in their frames are made in, and so a bound on such writes in flight. At least the
    /// largest page.
    pub writeback_bytes: usize,
}

/// The default writeback buffer: two of the largest page, so a write of one never waits behind
/// another, which is far more than the 32 to 128 small writes in flight that keep a device busy.
pub const WRITEBACK_BYTES: usize = 2 * page::MAX_PAGE_SIZE;

impl BufferConfig {
    /// A pool of `budget` bytes with the default shards and writeback buffer.
    pub fn with_budget(budget: usize) -> Self {
        Self {
            budget,
            checkpoint_shards: 128,
            writeback_bytes: WRITEBACK_BYTES,
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
    /// The current WATT epoch.
    epoch: AtomicU32,
    /// Bytes evicted since the pool opened; each quarter of the pool is an epoch.
    evicted: AtomicU64,
    epoch_bytes: u64,
    /// Done when a frame is next freed, if anyone waits for that.
    released: Mutex<Option<Completion>>,
    writebacks: Mutex<Vec<Arc<Mutex<Writeback>>>>,
    /// Where copies of pages written back while they stay in their frames are made.
    staging: Arc<staging::Staging>,
    writes_in_flight: AtomicUsize,
    failure: Mutex<Option<CompletionError>>,
    /// Checkpoint shards, each a run of frames by position.
    shards: Box<[Shard]>,
    temp: Arc<TempPages>,
    /// Each queued writeback's state after its last step, by address, for a test that stalled to
    /// show while another thread steps it.
    #[cfg(test)]
    stepped: Mutex<HashMap<usize, String>>,
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
    /// Free frames, none quiet yet.
    Quieting,
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
    /// A pool reading missing pages from `source` and writing dirty ones to `store`. `redo` is
    /// where a recovery would start replaying the log now: every change the pool's pages will
    /// receive is logged at or after it.
    pub fn new(
        config: BufferConfig,
        source: Arc<dyn PageSource>,
        store: Arc<PageStore>,
        redo: Lsn,
    ) -> Result<Arc<Self>> {
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
        assert!(
            config.checkpoint_shards > 0,
            "a pool with no checkpoint shards"
        );
        let shards = config.checkpoint_shards.min(grains as usize);
        // One shard of transits per hardware thread, so concurrent misses rarely share a lock.
        let load_shards = std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .next_power_of_two();
        let epoch_bytes =
            (memory.batches as u64 * memory::BATCH as u64) / watt::EPOCHS_PER_TURNOVER as u64;
        Ok(Arc::new(Self {
            frames: Arc::new(Frames {
                headers: Zeroed::new(grains as usize)?,
                given_up: Zeroed::new(grains as usize)?,
                transits: (0..load_shards)
                    .map(|_| Mutex::new(HashMap::new()))
                    .collect(),
            }),
            table: Table::new(grains)?,
            memory,
            source,
            epoch: AtomicU32::new(watt::FIRST_EPOCH),
            evicted: AtomicU64::new(0),
            epoch_bytes,
            released: Mutex::new(None),
            writebacks: Mutex::new(Vec::new()),
            staging: Arc::new(staging::Staging::new(config.writeback_bytes)),
            writes_in_flight: AtomicUsize::new(0),
            failure: Mutex::new(None),
            shards: (0..shards).map(|_| Shard::new(redo)).collect(),
            temp: Arc::new(TempPages::new(
                store.io().clone(),
                store.base(),
                store.epochs().clone(),
            )?),
            store,
            #[cfg(test)]
            stepped: Mutex::new(HashMap::new()),
        }))
    }

    fn header(&self, frame: u32) -> &FrameHeader {
        &self.frames.headers[frame as usize]
    }

    fn transits_of(&self, frame: u32) -> &Mutex<HashMap<u32, Completion>> {
        self.frames.transits_of(frame)
    }

    fn now(&self) -> u32 {
        self.epoch.load(Ordering::Relaxed)
    }

    fn failed(&self) -> Result<()> {
        match *self.failure.lock() {
            Some(e) => Err(LimboError::CompletionError(e)),
            None => Ok(()),
        }
    }

    /// Bytes each size class holds.
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
                        // does. So the wait is the pool's, started before the look.
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
                if let Some(c) = self.transits_of(frame).lock().get(&frame) {
                    return match status {
                        LOADING => Probe::Loading(c.clone()),
                        _ => Probe::Outgoing(c.clone()),
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
        h.status.store(LOADING, Ordering::Release);
        let loaded = Completion::new_wait();
        self.transits_of(frame).lock().insert(frame, loaded.clone());
        if self
            .table
            .insert(&self.frames.headers, pid.raw(), frame)
            .is_err()
        {
            self.transits_of(frame).lock().remove(&frame);
            self.free_frame(pid.class(), frame, hold);
            // A reader may have followed a hint here meanwhile: send it to the other frame.
            loaded.complete(0);
            return Ok(IOResult::Done(None));
        }
        // The frame is quiet (claimed for a read) and held: nothing else reaches its memory while
        // the IO writes it.
        let into = self.memory.region.buffer(frame, pid.class().size());
        let reading = Loading {
            pool: self.clone(),
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
                let waiting = self.transits_of(frame).lock().remove(&frame);
                let lsn = page::lsn(self.bytes(frame, pid.class()));
                h.page_lsn.store(lsn, Ordering::Release);
                h.written_lsn.store(lsn, Ordering::Release);
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
        let waiting = self.transits_of(frame).lock().remove(&frame);
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
        let stamp = self.store.epochs().stamp() + 1;
        for grain in &self.frames.given_up[grains(frame, class)] {
            grain.store(stamp, Ordering::Release);
        }
        self.memory.put(class, frame);
        self.signal_released();
    }

    /// Whether no step that could still read what `frame`'s memory held before is left, given
    /// the epochs' `horizon`: then an IO may read a page into it. A page written through the
    /// frame's writer needs no such wait, its stores being atomic.
    fn quiet(&self, frame: u32, class: SizeClass, horizon: u64) -> bool {
        self.frames.given_up[grains(frame, class)]
            .iter()
            .all(|g| g.load(Ordering::Acquire) <= horizon)
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

    /// A new temporary page of `class` (DESIGN.md §6.4), all zeros, fixed exclusively. Its
    /// changes are never logged: stamp them with [`temporary_lsn`](Self::temporary_lsn).
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

    /// The LSN to stamp the next change to a temporary page with.
    pub fn temporary_lsn(&self) -> Lsn {
        self.temp.next_lsn()
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
        h.page_lsn.store(0, Ordering::Release);
        h.written_lsn.store(0, Ordering::Release);
        h.tracker.clear();
        h.tracker.read(self.now());
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
    pub fn free(self: &Arc<Self>, page: Exclusive<'_>) -> Result<IOResult<()>> {
        let (frame, pid) = (page.frame, page.pid);
        if self.header(frame).has(WRITING) {
            // Its writeback's remap would bring the page back: let the write land first.
            drop(page);
            return self.settle_writes().map(|r| match r {
                IOResult::Done(()) => IOResult::IO(IOCompletions(Completion::new_yield())),
                io => io,
            });
        }
        if !pid.is_temporary() {
            match self.store.free(pid)? {
                IOResult::Done(()) => {}
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
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

    /// Frees `pid`, which nothing refers to any more: a provisional page left over by an
    /// operation that gave up on it, or by a crash. A page not in memory is not read in: a frame
    /// is taken for it unread and held exclusively, so no fix can read it in while its number is
    /// freed.
    pub fn free_unreferenced(self: &Arc<Self>, pid: PageId) -> Result<IOResult<()>> {
        let _step = self.store.epochs().enter();
        loop {
            if self.table.find(&self.frames.headers, pid.raw()).is_some() {
                return match self.fix_exclusive(pid, None)? {
                    IOResult::Done(page) => self.free(page),
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
            h.page_lsn.store(0, Ordering::Release);
            h.written_lsn.store(0, Ordering::Release);
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
            return self.free(Exclusive {
                pool: self,
                frame,
                pid,
                hold,
                step: self.store.epochs().enter(),
            });
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

    /// Starts reading those of `pids` that are not in memory, without waiting for anything:
    /// pages whose location needs a read first, and pages for which no frame is free at once,
    /// are skipped. A later fix finds each page resident or being read.
    pub fn prefetch(self: &Arc<Self>, pids: &[PageId]) -> Result<()> {
        let _step = self.store.epochs().enter();
        self.failed()?;
        for &pid in pids {
            if self.table.find(&self.frames.headers, pid.raw()).is_some() {
                continue;
            }
            let located = match self.locate(pid)? {
                IOResult::Done(l) => l,
                IOResult::IO(_) => continue,
            };
            let class = pid.class();
            let frame = match self.take_or_grow(class, Purpose::Read) {
                Got::Frame(f) => f,
                Got::Quieting => continue,
                Got::None => {
                    self.evict_round(class)?;
                    match self.take_or_grow(class, Purpose::Read) {
                        Got::Frame(f) => f,
                        Got::Quieting | Got::None => return Ok(()),
                    }
                }
            };
            drop(self.load(pid, frame, located)?);
        }
        Ok(())
    }

    // ---------------------------------------------------------------- frames

    fn take_or_grow(&self, class: SizeClass, purpose: Purpose) -> Got {
        let horizon = (purpose == Purpose::Read).then(|| self.store.epochs().horizon());
        loop {
            let usable = |f| horizon.is_none_or(|h| self.quiet(f, class, h));
            match self.memory.take_where(class, usable) {
                Take::Frame(f) => return Got::Frame(f),
                // Growing would take memory a later step does not need: once the steps under
                // way end, these frames are quiet.
                Take::Unusable => return Got::Quieting,
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
        match self.take_or_grow(class, purpose) {
            Got::Frame(f) => return Ok(IOResult::Done(f)),
            Got::Quieting => return yield_now(),
            Got::None => {}
        }
        // Waiting starts before the last look, so a frame freed in between is not missed.
        let wait = self.released_wait();
        self.make_room(class)?;
        match self.take_or_grow(class, purpose) {
            Got::Frame(f) => return Ok(IOResult::Done(f)),
            Got::Quieting => return yield_now(),
            Got::None => {}
        }
        if let Some(c) = self.drive_writebacks()? {
            return Ok(IOResult::IO(IOCompletions(c)));
        }
        if self.writes_in_flight.load(Ordering::Acquire) > 0 {
            return Ok(IOResult::IO(IOCompletions(wait)));
        }
        // Every candidate is latched for the moment, and nothing in flight will free a frame.
        yield_now()
    }

    /// The pool's memory and writes, for a test that stalled to show.
    #[cfg(test)]
    pub fn describe(&self) -> String {
        let queued: Vec<String> = self
            .writebacks
            .lock()
            .iter()
            .map(|wb| match wb.try_lock() {
                Some(w) => w.describe(),
                None => format!(
                    "held by its stepper, last stepped: {}",
                    self.stepped
                        .lock()
                        .get(&(Arc::as_ptr(wb) as usize))
                        .map_or("never", String::as_str)
                ),
            })
            .collect();
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
            self.failure.lock().as_ref(),
        )
    }

    fn released_wait(&self) -> Completion {
        self.released
            .lock()
            .get_or_insert_with(Completion::new_wait)
            .clone()
    }

    fn signal_released(&self) {
        let waiting = self.released.lock().take();
        if let Some(c) = waiting {
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
        // none in use; one with none takes the batch with the fewest in use, then the cheapest.
        let own = self
            .threshold(class)
            .map_or(f32::INFINITY, |t| t / class.size() as f32);
        // Among the classes with a batch that can go to `class`: one whose only batches lie where
        // no run of `class`'s order fits has nothing to give it, and choosing it would leave
        // `class` waiting on a drain that never starts.
        let colder = SizeClass::all()
            .filter(|&c| c != class)
            .filter_map(|c| {
                let batch = self.memory.emptiest(c, class)?;
                let (in_use, per_byte) = self.drain_cost(class, batch);
                Some((c, batch, in_use, per_byte))
            })
            .min_by(|a, b| a.2.cmp(&b.2).then(a.3.total_cmp(&b.3)));
        match colder {
            Some((holder, batch, in_use, per_byte))
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

    /// What draining the memory around `batch` for `class` gives up: how many of its pages are
    /// in use, read this epoch or being read in or claimed for a new page; and per byte it frees,
    /// the value of every page in it. A page being written out is leaving anyway.
    fn drain_cost(&self, class: SizeClass, batch: u32) -> (usize, f32) {
        let count = 1u32 << memory::order(class);
        let start = batch & !(count - 1);
        let now = self.now();
        let (mut in_use, mut value) = (0, 0.0);
        for frame in self.memory.taken_in(start, count) {
            let h = self.header(frame);
            match h.status() {
                RESIDENT => {
                    in_use += (h.tracker.last() >= now) as usize;
                    value += h.tracker.value(now);
                }
                OUTGOING => {}
                _ => {
                    in_use += 1;
                    value = f32::INFINITY;
                }
            }
        }
        (in_use, value / (count as usize * memory::BATCH) as f32)
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
    /// threshold, and starts writing back the dirty ones.
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
        let mut dirty = Vec::new();
        for frame in candidates {
            let h = self.header(frame);
            if h.tracker.value(now) > threshold {
                continue;
            }
            let Some(hold) = self.latch_to_evict(class, frame, || &h.latch) else {
                continue;
            };
            if h.dirty() {
                h.flags.fetch_or(WRITING | EVICT_AFTER, Ordering::AcqRel);
                drop(hold);
                dirty.push(frame);
            } else {
                self.evict(class, frame, hold);
            }
        }
        self.write_back(class, dirty, true);
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
        self.transits_of(frame)
            .lock()
            .insert(frame, Completion::new_wait());
        h.status.store(OUTGOING, Ordering::Release);
        Outgoing {
            pid,
            hold: Some(hold),
        }
    }

    /// Queues the writes of `outgoing`, pages of `class` leaving their frames.
    fn write_out(&self, class: SizeClass, outgoing: Vec<Outgoing>) {
        let (temporary, logged): (Vec<Outgoing>, Vec<Outgoing>) =
            outgoing.into_iter().partition(|o| o.pid.is_temporary());
        for (outgoing, temporary) in [(logged, false), (temporary, true)] {
            if !outgoing.is_empty() {
                let wb = Writeback::out(class, outgoing, temporary);
                self.writebacks.lock().push(Arc::new(Mutex::new(wb)));
            }
        }
    }

    /// Ends the write-out of `pid` from the frame `hold` holds, with the write of a copy holding
    /// changes up to `lsn`. Written, the page leaves the frame, and whoever waits reads it again
    /// from that copy, already in the page table; not written, the page stays, dirty.
    fn written_out(
        &self,
        pid: PageId,
        hold: Held<FrameLatch>,
        lsn: Lsn,
        res: std::result::Result<(), CompletionError>,
    ) {
        let frame = hold.latch().frame;
        match res {
            Ok(()) => {
                let h = self.header(frame);
                h.written_lsn.fetch_max(lsn, Ordering::AcqRel);
                debug_assert!(!h.dirty(), "{pid:?} changed while held for its write-out");
                let waiting = self.transits_of(frame).lock().remove(&frame);
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
            self.writebacks.lock().push(Arc::new(Mutex::new(wb)));
        }
        finished
    }

    /// Steps every queued writeback no one else is stepping. Returns something to wait on, if
    /// one is waiting on IO before its writes are submitted.
    fn drive_writebacks(self: &Arc<Self>) -> Result<Option<Completion>> {
        let queued: Vec<_> = self.writebacks.lock().clone();
        let mut wait = None;
        for wb in queued {
            if let Some(Progress::Waiting(c)) = self.step_writeback(&wb) {
                wait.get_or_insert(c);
            }
        }
        Ok(wait)
    }

    /// Steps `wb`, unless another thread is stepping it; the only way a writeback is stepped. A
    /// waiter that found it held saw it neither finished nor left for another to step, so once
    /// released after either, the pool's waiters are woken: nothing else would wake that one.
    /// Nor would anything wake it for a write that landed during the step, after the step took
    /// the writes landed so far: that write's wakeup came while the writeback was held, and only
    /// a later step settles it.
    fn step_writeback(self: &Arc<Self>, wb: &Arc<Mutex<Writeback>>) -> Option<Progress> {
        let mut w = wb.try_lock()?;
        let landings = w.landings();
        let progress = w.step(self);
        #[cfg(test)]
        {
            let came_to = match &progress {
                Progress::Waiting(c) if c.finished() => "yielded",
                Progress::Waiting(_) => "waiting",
                Progress::InFlight => "in flight",
                Progress::Done => "done",
            };
            let state = format!("{} -> {came_to}", w.describe());
            self.stepped.lock().insert(Arc::as_ptr(wb) as usize, state);
        }
        drop(w);
        match progress {
            // Checked once released: a write landing after this wakes the waiters itself, with
            // the writeback free to step.
            Progress::InFlight => {
                if !landings.lock().is_empty() {
                    self.signal_released();
                }
            }
            Progress::Done => {
                self.writebacks.lock().retain(|q| !Arc::ptr_eq(q, wb));
                self.signal_released();
            }
            Progress::Waiting(_) => self.signal_released(),
        }
        Some(progress)
    }

    /// Notes that a write of `frame` landed, from its completion: whoever waits on the pool, or on
    /// the frame's write-out, wakes and steps the writeback that settles it.
    fn write_landed(&self, frame: u32, outgoing: bool) {
        if outgoing {
            // Completed outside the lock: completing may run a waiter's callback.
            let c = self.transits_of(frame).lock().get(&frame).cloned();
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
        if self.writes_in_flight.load(Ordering::Acquire) > 0 || !self.writebacks.lock().is_empty() {
            return Ok(IOResult::IO(IOCompletions(wait)));
        }
        Ok(IOResult::Done(()))
    }

    /// Ends the write of a copy of `pid`, in `frame`, holding changes up to `lsn`.
    fn write_ended(
        &self,
        frame: u32,
        pid: PageId,
        lsn: Lsn,
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
                h.written_lsn.fetch_max(lsn, Ordering::AcqRel);
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
        self.failure.lock().get_or_insert(e);
        self.signal_released();
    }

    // ---------------------------------------------------------------- checkpoints

    /// How many checkpoint shards the frames are split into.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// The frames of `shard`, as indexes of 4 KiB grains.
    fn shard_frames(&self, shard: usize) -> Range<u32> {
        let (grains, n) = (self.memory.grains() as u64, self.shards.len() as u64);
        let at = |s: u64| (s * grains).div_ceil(n) as u32;
        at(shard as u64)..at(shard as u64 + 1)
    }

    #[cfg(test)]
    fn shard_of(&self, frame: u32) -> usize {
        let (grains, n) = (self.memory.grains() as u64, self.shards.len() as u64);
        (frame as u64 * n / grains) as usize
    }

    /// Starts a checkpoint pass over `shard`. Passes leave temporary pages alone.
    pub fn pass(&self, shard: usize) -> Pass {
        Pass::new(self, shard)
    }

    /// Where a recovery could start replaying the log now, as far as the pool is concerned: the
    /// lowest position of any shard.
    pub fn redo_floor(&self) -> Lsn {
        self.shards
            .iter()
            .map(Shard::position)
            .min()
            .expect("a shard")
    }
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

    /// The page, to change. Every change is logged, and reported with [`changed`](Self::changed)
    /// before the latch is released; a change not reported is lost when the page is evicted.
    pub fn page_mut(&mut self) -> Writer<'_> {
        // SAFETY: latched exclusively, and the writer borrows the guard.
        unsafe { self.pool.writer(self.frame, self.pid.class()) }
    }

    /// Notes a change whose log record ends at `lsn`: the page's LSN becomes `lsn`, and the page
    /// is dirty until a copy holding it is written back.
    pub fn changed(&mut self, lsn: Lsn) {
        let h = self.pool.header(self.frame);
        assert!(
            lsn > h.page_lsn.load(Ordering::Relaxed),
            "a change to {:?} at {lsn} is not after its last",
            self.pid
        );
        page::set_lsn(&mut self.page_mut(), lsn);
        h.page_lsn.store(lsn, Ordering::Release);
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
