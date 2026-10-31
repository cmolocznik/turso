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
//! **Failure.** A failed page write, like a failed log write (§7.5), stops the pool: every later
//! miss fails until the database is reopened and recovered.

mod clean;
mod frame;
mod memory;
mod pass;
mod source;
mod table;
mod watt;

#[cfg(test)]
mod tests;

pub use memory::MIN_BUDGET;
pub use pass::Pass;
pub use source::{LocalSource, Located, PageSource, ReadDone};

use super::log::Lsn;
use super::page::{self, PageId, SizeClass, CLASS_COUNT};
use super::store::PageStore;
use clean::Writeback;
use frame::{FrameHeader, ABANDONED, EVICT_AFTER, FREE, LOADING, NO_PAGE, RESIDENT, WRITING};
use memory::{Memory, Zeroed};
use pass::Shard;
use table::Table;

use crate::io::Completion;
use crate::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
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
}

impl BufferConfig {
    /// A pool of `budget` bytes with the default shards.
    pub fn with_budget(budget: usize) -> Self {
        Self {
            budget,
            checkpoint_shards: 128,
        }
    }
}

/// How a page is fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Optimistic,
    Shared,
    Exclusive,
}

/// What following a frame found.
enum Probe {
    /// The frame holds the page, latched in the mode asked for; for an optimistic fix, the
    /// version seen.
    Hit(u64),
    /// The frame holds the page, but its latch is held.
    Busy,
    /// The page is being read into the frame.
    Loading(Completion),
    /// The frame does not hold the page.
    Miss,
}

pub struct BufferPool {
    memory: Memory,
    headers: Zeroed<FrameHeader>,
    table: Table,
    source: Arc<dyn PageSource>,
    store: Arc<PageStore>,
    /// Per shard of frames: the read in flight into each loading frame.
    loads: Box<[Mutex<HashMap<u32, Completion>>]>,
    /// The current WATT epoch.
    epoch: AtomicU32,
    /// Bytes evicted since the pool opened; each quarter of the pool is an epoch.
    evicted: AtomicU64,
    epoch_bytes: u64,
    /// Done when a frame is next freed, if anyone waits for that.
    released: Mutex<Option<Completion>>,
    writebacks: Mutex<Vec<Arc<Mutex<Writeback>>>>,
    writes_in_flight: AtomicUsize,
    failure: Mutex<Option<CompletionError>>,
    /// Checkpoint shards, each a run of frames by position.
    shards: Box<[Shard]>,
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
        let grains = memory.grains();
        assert!(
            config.checkpoint_shards > 0,
            "a pool with no checkpoint shards"
        );
        let shards = config.checkpoint_shards.min(grains as usize);
        // One shard of loads per hardware thread, so concurrent misses rarely share a lock.
        let load_shards = std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .next_power_of_two();
        let epoch_bytes =
            (memory.batches as u64 * memory::BATCH as u64) / watt::EPOCHS_PER_TURNOVER as u64;
        Ok(Arc::new(Self {
            headers: Zeroed::new(grains as usize)?,
            table: Table::new(grains)?,
            memory,
            source,
            store,
            loads: (0..load_shards)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            epoch: AtomicU32::new(watt::FIRST_EPOCH),
            evicted: AtomicU64::new(0),
            epoch_bytes,
            released: Mutex::new(None),
            writebacks: Mutex::new(Vec::new()),
            writes_in_flight: AtomicUsize::new(0),
            failure: Mutex::new(None),
            shards: (0..shards).map(|_| Shard::new(redo)).collect(),
        }))
    }

    fn header(&self, frame: u32) -> &FrameHeader {
        &self.headers[frame as usize]
    }

    fn now(&self) -> u32 {
        self.epoch.load(Ordering::Relaxed)
    }

    fn loads_of(&self, frame: u32) -> &Mutex<HashMap<u32, Completion>> {
        &self.loads[frame as usize & (self.loads.len() - 1)]
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
        Ok(match self.fix(pid, hint, Mode::Optimistic)? {
            IOResult::Done((frame, version)) => IOResult::Done(Optimistic {
                pool: self,
                frame,
                pid,
                version,
            }),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    pub fn fix_shared(
        self: &Arc<Self>,
        pid: PageId,
        hint: Option<&AtomicU32>,
    ) -> Result<IOResult<Shared<'_>>> {
        Ok(match self.fix(pid, hint, Mode::Shared)? {
            IOResult::Done((frame, _)) => IOResult::Done(Shared {
                pool: self,
                frame,
                pid,
            }),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    pub fn fix_exclusive(
        self: &Arc<Self>,
        pid: PageId,
        hint: Option<&AtomicU32>,
    ) -> Result<IOResult<Exclusive<'_>>> {
        Ok(match self.fix(pid, hint, Mode::Exclusive)? {
            IOResult::Done((frame, _)) => IOResult::Done(Exclusive {
                pool: self,
                frame,
                pid,
            }),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// The frame holding `pid`, latched in `mode`, and the version an optimistic fix saw.
    fn fix(
        self: &Arc<Self>,
        pid: PageId,
        hint: Option<&AtomicU32>,
        mode: Mode,
    ) -> Result<IOResult<(u32, u64)>> {
        loop {
            let hinted = hint
                .map(|h| h.load(Ordering::Relaxed))
                .filter(|&h| h != 0 && h <= self.memory.grains());
            let found = hinted.map(|h| (h - 1, true)).into_iter().chain(
                // The table, asked only if the hint missed.
                std::iter::from_fn(|| {
                    self.table
                        .find(&self.headers, pid.raw())
                        .map(|f| (f, false))
                })
                .take(1),
            );
            for (frame, from_hint) in found {
                match self.probe(frame, pid, mode) {
                    Probe::Hit(version) => {
                        if !from_hint {
                            if let Some(h) = hint {
                                h.store(frame + 1, Ordering::Relaxed);
                            }
                        }
                        self.header(frame).tracker.read(self.now());
                        return Ok(IOResult::Done((frame, version)));
                    }
                    Probe::Busy => return yield_now(),
                    Probe::Loading(c) => return Ok(IOResult::IO(IOCompletions(c))),
                    Probe::Miss => {}
                }
            }
            let located = match self.source.locate(pid)? {
                IOResult::Done(l) => l,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let frame = match self.claim(pid.class())? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            if let Some(loaded) = self.load(pid, frame, located)? {
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
    fn probe(&self, frame: u32, pid: PageId, mode: Mode) -> Probe {
        let h = self.header(frame);
        let holds = |h: &FrameHeader| h.status() == RESIDENT && h.pid() == pid.raw();
        let latched = match mode {
            Mode::Optimistic => match h.latch.optimistic() {
                // The version read first: an eviction after it fails validation later.
                Some(v) => return if holds(h) { Probe::Hit(v) } else { Probe::Miss },
                None => false,
            },
            Mode::Shared => h.latch.try_lock_shared(),
            Mode::Exclusive => h.latch.try_lock_exclusive(),
        };
        if latched {
            if holds(h) {
                return Probe::Hit(0);
            }
            match mode {
                Mode::Shared => h.latch.unlock_shared(),
                _ => h.latch.unlock_exclusive_unchanged(),
            }
            return Probe::Miss;
        }
        if h.pid() != pid.raw() {
            return Probe::Miss;
        }
        if h.status() == LOADING {
            if let Some(c) = self.loads_of(frame).lock().get(&frame) {
                return Probe::Loading(c.clone());
            }
        }
        Probe::Busy
    }

    /// Claims `frame` for `pid` and starts reading the page into it. `None` if another frame
    /// holds `pid` already. Otherwise the read's completion, done when the page is resident.
    fn load(
        self: &Arc<Self>,
        pid: PageId,
        frame: u32,
        located: Box<dyn Located>,
    ) -> Result<Option<Completion>> {
        let h = self.header(frame);
        self.take_free_latch(frame);
        h.pid.store(pid.raw(), Ordering::Release);
        h.flags.store(0, Ordering::Release);
        h.tracker.clear();
        h.tracker.read(self.now());
        h.status.store(LOADING, Ordering::Release);
        let loaded = Completion::new_wait();
        self.loads_of(frame).lock().insert(frame, loaded.clone());
        if self.table.insert(&self.headers, pid.raw(), frame).is_err() {
            self.loads_of(frame).lock().remove(&frame);
            self.free_frame(pid.class(), frame);
            // A reader may have followed a hint here meanwhile: send it to the other frame.
            loaded.complete(0);
            return Ok(None);
        }
        let size = pid.class().size();
        let pool = self.clone();
        let read = located.read(
            self.memory.region.buffer(frame, size),
            Box::new(move |res| pool.loaded(pid, frame, res)),
        );
        if let Err(e) = read {
            self.loaded(pid, frame, Err(io_failure(&e)));
            return Err(e);
        }
        Ok(Some(loaded))
    }

    /// Ends the read of `pid` into `frame`.
    fn loaded(&self, pid: PageId, frame: u32, res: std::result::Result<(), CompletionError>) {
        let h = self.header(frame);
        let waiting = self.loads_of(frame).lock().remove(&frame);
        match res {
            Ok(()) => {
                // SAFETY: the frame is the load's alone until its latch is released below.
                let lsn = page::lsn(unsafe { self.bytes(frame, pid.class()) });
                h.page_lsn.store(lsn, Ordering::Release);
                h.written_lsn.store(lsn, Ordering::Release);
                h.status.store(RESIDENT, Ordering::Release);
                h.latch.unlock_exclusive();
                if let Some(c) = waiting {
                    c.complete(0);
                }
            }
            Err(e) => {
                self.table.remove(&self.headers, pid.raw(), frame);
                self.free_frame(pid.class(), frame);
                if let Some(c) = waiting {
                    c.error(e);
                }
            }
        }
    }

    /// Latches a free frame exclusively. Only a reader following a stale hint can hold it, for
    /// the instant it takes to see the frame holds nothing.
    fn take_free_latch(&self, frame: u32) {
        let latch = &self.header(frame).latch;
        while !latch.try_lock_exclusive() {
            std::hint::spin_loop();
        }
    }

    /// Returns `frame`, unlinked and latched exclusively, to the free frames.
    fn free_frame(&self, class: SizeClass, frame: u32) {
        let h = self.header(frame);
        h.status.store(FREE, Ordering::Release);
        h.pid.store(NO_PAGE, Ordering::Release);
        h.flags.store(0, Ordering::Release);
        h.latch.unlock_exclusive();
        self.memory.put(class, frame);
        self.signal_released();
    }

    /// The bytes of `frame`, of `class`'s size.
    ///
    /// # Safety
    ///
    /// The caller holds the frame's latch, or reads optimistically and validates afterwards.
    unsafe fn bytes(&self, frame: u32, class: SizeClass) -> &[u8] {
        std::slice::from_raw_parts(self.memory.region.ptr(frame), class.size())
    }

    /// # Safety
    ///
    /// The caller holds the frame's latch exclusively.
    #[allow(clippy::mut_from_ref)]
    unsafe fn bytes_mut(&self, frame: u32, class: SizeClass) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.memory.region.ptr(frame), class.size())
    }

    // ---------------------------------------------------------------- new pages

    /// A new page of `class`, all zeros, fixed exclusively.
    pub fn allocate(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<Exclusive<'_>>> {
        let frame = match self.claim(class)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let pid = match self.store.allocate(class) {
            Ok(IOResult::Done(pid)) => pid,
            Ok(IOResult::IO(io)) => {
                self.take_free_latch(frame);
                self.free_frame(class, frame);
                return Ok(IOResult::IO(io));
            }
            Err(e) => {
                self.take_free_latch(frame);
                self.free_frame(class, frame);
                return Err(e);
            }
        };
        let h = self.header(frame);
        self.take_free_latch(frame);
        // SAFETY: latched exclusively.
        unsafe { self.bytes_mut(frame, class) }.fill(0);
        h.pid.store(pid.raw(), Ordering::Release);
        h.flags.store(0, Ordering::Release);
        h.page_lsn.store(0, Ordering::Release);
        h.written_lsn.store(0, Ordering::Release);
        h.tracker.clear();
        h.tracker.read(self.now());
        h.status.store(RESIDENT, Ordering::Release);
        // The number may have been freed while its last page was still in memory: whoever freed
        // it holds that frame exclusively and drops it next, waiting on nothing (`free`).
        while let Err(other) = self.table.insert(&self.headers, pid.raw(), frame) {
            assert!(
                self.header(other).latch.optimistic().is_none(),
                "new page {pid:?} is already in frame {other}, which no one is freeing"
            );
            std::hint::spin_loop();
        }
        Ok(IOResult::Done(Exclusive {
            pool: self,
            frame,
            pid,
        }))
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
        match self.store.free(pid)? {
            IOResult::Done(()) => {}
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
        std::mem::forget(page);
        self.table.remove(&self.headers, pid.raw(), frame);
        self.free_frame(pid.class(), frame);
        Ok(IOResult::Done(()))
    }

    /// Frees `pid`, which nothing refers to any more: a provisional page left over by an
    /// operation that gave up on it, or by a crash. A page not in memory is not read in: a frame
    /// is taken for it unread and held exclusively, so no fix can read it in while its number is
    /// freed.
    pub fn free_unreferenced(self: &Arc<Self>, pid: PageId) -> Result<IOResult<()>> {
        loop {
            if self.table.find(&self.headers, pid.raw()).is_some() {
                return match self.fix_exclusive(pid, None)? {
                    IOResult::Done(page) => self.free(page),
                    IOResult::IO(io) => Ok(IOResult::IO(io)),
                };
            }
            let class = pid.class();
            let frame = match self.claim(class)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let h = self.header(frame);
            self.take_free_latch(frame);
            // Whatever the frame held before must not read as this page if the free waits.
            // SAFETY: latched exclusively.
            let bytes = unsafe { self.bytes_mut(frame, class) };
            bytes[..page::HEADER_SIZE].fill(0);
            h.pid.store(pid.raw(), Ordering::Release);
            h.flags.store(0, Ordering::Release);
            h.page_lsn.store(0, Ordering::Release);
            h.written_lsn.store(0, Ordering::Release);
            h.tracker.clear();
            h.status.store(RESIDENT, Ordering::Release);
            if self.table.insert(&self.headers, pid.raw(), frame).is_err() {
                // Read in meanwhile: free it where it is.
                self.free_frame(class, frame);
                continue;
            }
            return self.free(Exclusive {
                pool: self,
                frame,
                pid,
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
            }),
            _ => None,
        }
    }

    // ---------------------------------------------------------------- prefetching

    /// Starts reading those of `pids` that are not in memory, without waiting for anything:
    /// pages whose location needs a read first, and pages for which no frame is free at once,
    /// are skipped. A later fix finds each page resident or being read.
    pub fn prefetch(self: &Arc<Self>, pids: &[PageId]) -> Result<()> {
        self.failed()?;
        for &pid in pids {
            if self.table.find(&self.headers, pid.raw()).is_some() {
                continue;
            }
            let located = match self.source.locate(pid)? {
                IOResult::Done(l) => l,
                IOResult::IO(_) => continue,
            };
            let class = pid.class();
            let frame = match self.take_or_grow(class) {
                Some(f) => f,
                None => {
                    self.evict_round(class)?;
                    match self.take_or_grow(class) {
                        Some(f) => f,
                        None => return Ok(()),
                    }
                }
            };
            drop(self.load(pid, frame, located)?);
        }
        Ok(())
    }

    // ---------------------------------------------------------------- frames

    fn take_or_grow(&self, class: SizeClass) -> Option<u32> {
        loop {
            if let Some(f) = self.memory.take(class) {
                return Some(f);
            }
            if !self.memory.grow(class) {
                return None;
            }
        }
    }

    /// A free frame of `class`, making room if there is none.
    fn claim(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<u32>> {
        self.failed()?;
        if let Some(f) = self.take_or_grow(class) {
            return Ok(IOResult::Done(f));
        }
        // Waiting starts before the last look, so a frame freed in between is not missed.
        let wait = self.released_wait();
        self.make_room(class)?;
        if let Some(f) = self.take_or_grow(class) {
            return Ok(IOResult::Done(f));
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
        let own = self
            .threshold(class)
            .map_or(f32::INFINITY, |t| t / class.size() as f32);
        let colder = SizeClass::all()
            .filter(|&c| c != class)
            .filter_map(|c| self.threshold(c).map(|t| (c, t / c.size() as f32)))
            .min_by(|a, b| a.1.total_cmp(&b.1));
        match colder {
            Some((holder, per_byte)) if per_byte < own => {
                let Some(batch) = self.memory.emptiest(holder) else {
                    return Ok(());
                };
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
            if h.status() != RESIDENT || h.has(WRITING) || h.tracker.value(now) > threshold {
                continue;
            }
            if !h.latch.try_lock_exclusive() {
                continue;
            }
            if h.status() != RESIDENT || h.has(WRITING) {
                h.latch.unlock_exclusive();
                continue;
            }
            if h.dirty() {
                h.flags.fetch_or(WRITING | EVICT_AFTER, Ordering::AcqRel);
                h.latch.unlock_exclusive();
                dirty.push(frame);
            } else {
                self.evict(class, frame);
            }
        }
        self.write_back(class, dirty, true);
        Ok(())
    }

    /// Evicts the clean page in `frame`, latched exclusively.
    fn evict(&self, class: SizeClass, frame: u32) {
        let h = self.header(frame);
        debug_assert!(!h.dirty() && !h.has(WRITING));
        self.table.remove(&self.headers, h.pid(), frame);
        self.free_frame(class, frame);
        let size = class.size() as u64;
        let before = self.evicted.fetch_add(size, Ordering::Relaxed);
        if (before + size) / self.epoch_bytes != before / self.epoch_bytes {
            self.epoch.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Empties what it can of `batch`, draining out of `class`: each page moves to a free frame
    /// of the class outside the batch, or is evicted if clean, or starts being written back.
    fn drain(self: &Arc<Self>, class: SizeClass, batch: u32) -> Result<()> {
        let mut dirty = Vec::new();
        for i in 0..memory::frames_per_batch(class) {
            let frame = memory::frame_at(class, batch, i);
            if self.memory.is_free(class, frame) {
                continue;
            }
            let h = self.header(frame);
            if h.status() != RESIDENT || h.has(WRITING) || !h.latch.try_lock_exclusive() {
                continue;
            }
            if h.status() != RESIDENT || h.has(WRITING) {
                h.latch.unlock_exclusive();
                continue;
            }
            if let Some(to) = self.memory.take(class) {
                self.relocate(class, frame, to);
            } else if !h.dirty() {
                self.evict(class, frame);
            } else {
                h.flags.fetch_or(WRITING | EVICT_AFTER, Ordering::AcqRel);
                h.latch.unlock_exclusive();
                dirty.push(frame);
            }
        }
        self.write_back(class, dirty, true);
        Ok(())
    }

    /// Moves the page in `from`, latched exclusively, to the free frame `to`. Hints to `from` go
    /// stale like after an eviction.
    fn relocate(&self, class: SizeClass, from: u32, to: u32) {
        let (src, dst) = (self.header(from), self.header(to));
        self.take_free_latch(to);
        // SAFETY: both frames are latched exclusively, and distinct.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.memory.region.ptr(from),
                self.memory.region.ptr(to),
                class.size(),
            )
        };
        let pid = src.pid();
        if src.dirty() {
            let (a, b) = (self.shard_of(from), self.shard_of(to));
            if a != b {
                self.shards[b].arrived(self.shards[a].position());
            }
        }
        dst.pid.store(pid, Ordering::Release);
        dst.page_lsn
            .store(src.page_lsn.load(Ordering::Acquire), Ordering::Release);
        dst.written_lsn
            .store(src.written_lsn.load(Ordering::Acquire), Ordering::Release);
        dst.tracker.copy_from(&src.tracker);
        dst.flags.store(0, Ordering::Release);
        dst.status.store(RESIDENT, Ordering::Release);
        self.table.replace(&self.headers, pid, from, to);
        dst.latch.unlock_exclusive();
        self.free_frame(class, from);
    }

    // ---------------------------------------------------------------- writing back

    /// Queues the writeback of `frames`, already marked WRITING.
    fn write_back(
        &self,
        class: SizeClass,
        frames: Vec<u32>,
        evict_after: bool,
    ) -> Option<Completion> {
        if frames.is_empty() {
            return None;
        }
        let pids = frames
            .iter()
            .map(|&f| PageId::from_raw(self.header(f).pid()))
            .collect();
        let wb = Writeback::new(class, frames, pids, evict_after);
        let finished = wb.finished.clone();
        self.writebacks.lock().push(Arc::new(Mutex::new(wb)));
        Some(finished)
    }

    /// Steps every queued writeback no one else is stepping. Returns something to wait on, if
    /// one is waiting on IO before its writes are submitted.
    fn drive_writebacks(self: &Arc<Self>) -> Result<Option<Completion>> {
        let queued: Vec<_> = self.writebacks.lock().clone();
        let mut wait = None;
        for wb in queued {
            let Some(mut w) = wb.try_lock() else {
                continue;
            };
            let stepped = w.step(self);
            match stepped {
                Ok(IOResult::IO(IOCompletions(c))) => {
                    wait.get_or_insert(c);
                    continue;
                }
                Ok(IOResult::Done(())) => {}
                Err(ref e) => {
                    self.fail(io_failure(e));
                    w.abandon(self);
                }
            }
            drop(w);
            self.writebacks.lock().retain(|q| !Arc::ptr_eq(q, &wb));
            stepped.map(drop)?;
        }
        Ok(wait)
    }

    /// Done once no writeback is queued or in flight; meanwhile, drives the queued ones.
    pub fn settle_writes(self: &Arc<Self>) -> Result<IOResult<()>> {
        // Waiting starts before the look, so a write ending in between is not missed.
        let wait = self.released_wait();
        if let Some(c) = self.drive_writebacks()? {
            return Ok(IOResult::IO(IOCompletions(c)));
        }
        if self.writes_in_flight.load(Ordering::Acquire) > 0 {
            return Ok(IOResult::IO(IOCompletions(wait)));
        }
        Ok(IOResult::Done(()))
    }

    /// Ends the write of a copy of `frame`'s page holding changes up to `lsn`.
    fn write_ended(
        &self,
        frame: u32,
        lsn: Lsn,
        evict_after: bool,
        res: std::result::Result<(), CompletionError>,
    ) {
        let h = self.header(frame);
        let ok = res.is_ok();
        match res {
            Ok(()) => {
                h.written_lsn.fetch_max(lsn, Ordering::AcqRel);
            }
            Err(e) => self.fail(e),
        }
        h.flags
            .fetch_and(!(WRITING | EVICT_AFTER), Ordering::AcqRel);
        if ok && evict_after && h.latch.try_lock_exclusive() {
            if h.status() == RESIDENT && !h.dirty() && !h.has(WRITING) {
                self.evict(self.memory.class_of(frame), frame);
            } else {
                h.latch.unlock_exclusive();
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

    fn shard_of(&self, frame: u32) -> usize {
        let (grains, n) = (self.memory.grains() as u64, self.shards.len() as u64);
        (frame as u64 * n / grains) as usize
    }

    /// Starts a checkpoint pass over `shard`.
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

// -------------------------------------------------------------------- guards

/// A page fixed without a lock. Nothing read is valid until [`validate`](Self::validate) says so.
#[derive(Clone)]
pub struct Optimistic<'a> {
    pool: &'a BufferPool,
    frame: u32,
    pid: PageId,
    version: u64,
}

impl<'a> Optimistic<'a> {
    /// Runs `read` over the page, and returns what it returned if the page did not change
    /// meanwhile, `None` if it did. `read` may see the page mid-change, so it must not trust
    /// what it reads to be consistent: offsets and lengths are checked before use, and nothing
    /// it does may panic on garbage.
    pub fn read<R>(&self, read: impl FnOnce(&[u8]) -> R) -> Option<R> {
        // SAFETY: validated below; see the contract above.
        let out = read(unsafe { self.pool.bytes(self.frame, self.pid.class()) });
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
        let latch = &self.pool.header(self.frame).latch;
        // Built only once latched: a guard's drop releases the latch.
        latch
            .try_lock_exclusive_at(self.version)
            .then(|| Exclusive {
                pool: self.pool,
                frame: self.frame,
                pid: self.pid,
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

/// A page latched shared.
pub struct Shared<'a> {
    pool: &'a BufferPool,
    frame: u32,
    pid: PageId,
}

impl<'a> Shared<'a> {
    pub fn page(&self) -> &[u8] {
        // SAFETY: latched shared.
        unsafe { self.pool.bytes(self.frame, self.pid.class()) }
    }

    pub fn pid(&self) -> PageId {
        self.pid
    }

    pub fn hint(&self) -> u32 {
        self.frame + 1
    }

    /// Latches the page exclusively if no one else holds it shared.
    pub fn upgrade(self) -> std::result::Result<Exclusive<'a>, Self> {
        if self.pool.header(self.frame).latch.try_upgrade() {
            let (pool, frame, pid) = (self.pool, self.frame, self.pid);
            std::mem::forget(self);
            Ok(Exclusive { pool, frame, pid })
        } else {
            Err(self)
        }
    }
}

impl Drop for Shared<'_> {
    fn drop(&mut self) {
        self.pool.header(self.frame).latch.unlock_shared();
    }
}

/// A page latched exclusively.
pub struct Exclusive<'a> {
    pool: &'a BufferPool,
    frame: u32,
    pid: PageId,
}

impl<'a> Exclusive<'a> {
    pub fn page(&self) -> &[u8] {
        // SAFETY: latched exclusively.
        unsafe { self.pool.bytes(self.frame, self.pid.class()) }
    }

    /// The page, to change. Every change is logged, and reported with [`changed`](Self::changed)
    /// before the latch is released; a change not reported is lost when the page is evicted.
    pub fn page_mut(&mut self) -> &mut [u8] {
        // SAFETY: latched exclusively.
        unsafe { self.pool.bytes_mut(self.frame, self.pid.class()) }
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
        page::set_lsn(self.page_mut(), lsn);
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
        let (pool, frame, pid) = (self.pool, self.frame, self.pid);
        std::mem::forget(self);
        let latch = &pool.header(frame).latch;
        let version = latch.version_held();
        latch.unlock_exclusive_unchanged();
        Optimistic {
            pool,
            frame,
            pid,
            version,
        }
    }
}

impl Drop for Exclusive<'_> {
    fn drop(&mut self) {
        self.pool.header(self.frame).latch.unlock_exclusive();
    }
}
