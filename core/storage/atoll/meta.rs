//! Metadata pages: the page table and the free-space maps.
//!
//! They are 4 KiB pages in files of their own, one per tree, size class and level, and live in a
//! cache of their own rather than the buffer pool, because a buffer miss needs the page table.
//!
//! **Two places per page.** A metadata page has two places side by side at a position its index
//! fixes. It is always written to the place that does not hold its newest durable copy, so a torn
//! write leaves that copy whole, and a read takes both places and keeps the newest whole copy.
//! Metadata therefore needs no free space of its own and no parent pointing at it.
//!
//! **Logged first.** Every change is logged before it is made, and a page is written only once the
//! log is durable up to its LSN. A page with no whole copy has never had a change before the last
//! checkpoint's redo point, so recovery starts it from zeros and replays its changes.
//!
//! **Latching** is the buffer pool's (CONCURRENCY.md §5): a change holds the page's versioned
//! latch, taken without waiting, and a reader takes nothing and validates the version after it
//! read. A taker that finds the latch held yields.
//!
//! **No lock.** The cache keeps a cell per page, in an array per file that grows to the highest
//! page reached ([`Segmented`]). A page found is pinned, and only a page no pin holds is evicted
//! ([`pins`](super::pins)). A dirty page holds a pin of its own, and a floor among the dirty pages
//! tagged with its id, until it is written; a load or a write in flight holds one too: so a page
//! evicted is clean and idle. Whether a page is dirty is a bit of its count of changes, which a
//! write that lands clears by compare-and-swap, so it clears it only if no change came since its
//! copy.

use self::sync::AtomicU64;
use super::aligned::AlignedBlock;
use super::buffer::cell::{CompletionCell, Retiring};
use super::epoch::Epochs;
use super::floors::{Floor, Floors};
use super::latch::{Held, VersionedLatch};
use super::log::{Log, Lsn};
use super::once::{SetOnce, TakeOnce};
use super::page::{self, PageId, SizeClass, CLASS_COUNT, MIN_PAGE_SIZE};
use super::pins::{Cells, PinCell, Pinnable, Pinned, Pins};
use super::segmented::Segmented;
use super::shared::{Bytes, BytesMut, Shared, Writer};
use super::stack::Stack;
use super::whole;
use crate::io::{Completion, File, OpenFlags, IO};
use crate::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use crate::sync::{Arc, Mutex, Weak};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::VecDeque;

/// The engine's atomics, or loom's, for a page's dirtiness.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::AtomicU64;
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::AtomicU64;
}

pub const META_PAGE: usize = MIN_PAGE_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Tree {
    /// The page table: each page number's state and slot.
    Map,
    /// Free space: which slots are in use.
    Space,
}

/// A metadata page: its tree, the size class it describes, its level (0 for leaves) and its
/// index within the level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MetaId {
    pub tree: Tree,
    pub class: SizeClass,
    pub level: u8,
    pub index: u64,
}

const META_BIT: u64 = 1 << 55;
const TREE_SHIFT: u32 = 54;
const CLASS_SHIFT: u32 = 50;
const INDEX_BITS: u32 = 47;
/// Levels an id has room for, in the bits between its index and its class.
const LEVELS: usize = 1 << (CLASS_SHIFT - INDEX_BITS);

impl MetaId {
    /// The id sealed into the page: a class-0 page id in a number range data pages never use.
    pub fn pid(self) -> PageId {
        assert!(
            self.index < 1 << INDEX_BITS && (self.level as usize) < LEVELS,
            "metadata id {self:?} out of range"
        );
        let number = META_BIT
            | ((self.tree as u64) << TREE_SHIFT)
            | ((self.class.index() as u64) << CLASS_SHIFT)
            | ((self.level as u64) << INDEX_BITS)
            | self.index;
        PageId::new(SizeClass::new(0).expect("class 0"), number)
    }

    /// The id whose [`pid`](Self::pid) has `number`.
    fn from_number(number: u64) -> Self {
        Self {
            tree: if number >> TREE_SHIFT & 1 == 0 {
                Tree::Map
            } else {
                Tree::Space
            },
            class: SizeClass::new((number >> CLASS_SHIFT & 0xf) as usize).expect("a size class"),
            level: (number >> INDEX_BITS & (LEVELS as u64 - 1)) as u8,
            index: number & ((1 << INDEX_BITS) - 1),
        }
    }

    fn file_key(self) -> FileKey {
        (self.tree, self.class, self.level)
    }

    /// Its file's place among the cache's.
    fn file_index(self) -> usize {
        (self.tree as usize * CLASS_COUNT + self.class.index()) * LEVELS + self.level as usize
    }

    fn offset(self) -> u64 {
        self.index * 2 * META_PAGE as u64
    }
}

fn path(base: &str, (tree, class, level): FileKey) -> String {
    let tree = match tree {
        Tree::Map => "map",
        Tree::Space => "free",
    };
    format!("{base}.{tree}{}.{level}", class.index())
}

/// The file of one tree, size class and level.
type FileKey = (Tree, SizeClass, u8);

const LOADING: u8 = 0;
const READY: u8 = 1;
const FAILED: u8 = 2;

/// In a page's count of changes: a change not yet on disk.
const DIRTY: u64 = 1;

/// Whether a page has a change not yet on disk, and its floor among the dirty pages while it has.
#[derive(Default)]
struct Dirt {
    /// Twice the changes made, plus [`DIRTY`] while one is not on disk.
    changes: AtomicU64,
    /// The page's floor plus one: set as it turns dirty.
    floor: AtomicU64,
}

impl Dirt {
    /// Counts a change whose record starts at `start`, made by the only change under way:
    /// whether the page turned dirty, its floor then held among `dirty`, tagged `tag`.
    fn changed(&self, dirty: &Floors, start: Lsn, tag: u64) -> bool {
        // A write landing meanwhile may clear the dirty bit: this change then sets it again.
        let mut was = self.changes.load(Ordering::Relaxed);
        while let Err(now) = self.changes.compare_exchange_weak(
            was,
            (was + 2) | DIRTY,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            was = now;
        }
        if was & DIRTY != 0 {
            return false;
        }
        let floor = dirty.hold_tagged(start, tag);
        self.floor.store(floor.into_slot() + 1, Ordering::Release);
        true
    }

    /// The count a copy of the page taken now, while no change is under way, carries.
    fn count(&self) -> u64 {
        self.changes.load(Ordering::Acquire)
    }

    /// A write of the copy taken at `count` landed: whether the page is clean now, unless a
    /// change came since the copy, its floor then released. Writes of a page land one at a time.
    fn written(&self, dirty: &Floors, count: u64) -> bool {
        // Read before the exchange: only a page turning dirty sets it, and none turns dirty
        // before the exchange below succeeds.
        let floor = self.floor.load(Ordering::Acquire);
        if self
            .changes
            .compare_exchange(count, count & !DIRTY, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        dirty.release(Floor::from_slot(floor - 1));
        true
    }
}

pub struct MetaFrame {
    pub id: MetaId,
    latch: VersionedLatch,
    /// Written only under the latch, held exclusively; read under it, or optimistically.
    bytes: Box<Shared>,
    pins: Pins,
    /// [`LOADING`], [`READY`] or [`FAILED`], its error then in `failure`.
    status: AtomicU8,
    failure: SetOnce<CompletionError>,
    dirt: Dirt,
    /// The place holding the newest durable copy, plus one; 0 if the page has none.
    durable_side: AtomicU8,
    /// The write in flight, if any.
    writing: CompletionCell,
    /// Completed once a page loading is loaded.
    loaded: CompletionCell,
}

/// A metadata page held in the cache.
pub type MetaRef = Pinned<MetaFrame>;

impl Pinnable for MetaFrame {
    fn pins(&self) -> &Pins {
        &self.pins
    }
}

impl MetaFrame {
    fn new(id: MetaId) -> Self {
        Self {
            id,
            latch: VersionedLatch::default(),
            bytes: Shared::zeroed(META_PAGE),
            pins: Pins::default(),
            status: AtomicU8::new(LOADING),
            failure: SetOnce::default(),
            dirt: Dirt::default(),
            durable_side: AtomicU8::new(0),
            writing: CompletionCell::default(),
            loaded: CompletionCell::default(),
        }
    }

    fn fail(&self, e: CompletionError) {
        self.failure.set(e);
        self.status.store(FAILED, Ordering::Release);
    }

    /// Reads the page without a lock: `None` while a change holds the latch, else what `f`
    /// returned over a version no change overlapped. `f` may see a change half made, and then runs
    /// again, so it must not trust what it reads to be consistent, nor panic on it.
    pub fn read<R>(&self, f: impl Fn(&Shared) -> R) -> Option<R> {
        loop {
            let version = self.latch.optimistic()?;
            let out = f(&self.bytes);
            if self.latch.validate(version) {
                return Some(out);
            }
        }
    }

    /// Latches the page for a change, or `None` while a change holds it. The change's record is
    /// appended while the latch is held, so changes reach a page in the order of their records.
    pub fn try_lock(&self) -> Option<MetaHold<'_>> {
        Held::try_take(&self.latch).map(|hold| MetaHold {
            frame: self,
            // SAFETY: the hold has the latch exclusively, and keeps it while the writer lives.
            page: unsafe { self.bytes.writer() },
            hold,
        })
    }

    /// Notes a change, whose record starts at `start`, made under the latch, which is still held
    /// and so the only change under way.
    fn changed(&self, cache: &MetaCache, start: Lsn) {
        if self
            .dirt
            .changed(&cache.dirty, start, self.id.pid().number())
        {
            // Kept in the cache until it is written. The change's own holder has it pinned, so it
            // is not evicted meanwhile.
            self.pins.repin();
        }
    }

    /// A write of the copy taken at `count`, stamped `lsn`, landed.
    fn written(&self, cache: &MetaCache, count: u64, lsn: Lsn) {
        if self.dirt.written(&cache.dirty, count) {
            // Left clean, the page is what was written, unless a change has come since: one under
            // way holds the latch, and one done counted itself.
            debug_assert!(
                self.read(|b| page::lsn(b)).is_none_or(|now| now == lsn)
                    || self.dirt.count() != count & !DIRTY,
                "{:?} left clean behind a change not on disk",
                self.id
            );
            self.pins.unpin();
        }
    }

    /// Applies a logged change during recovery unless the page already holds it: whether it was
    /// applied, or `None` while a change holds the latch.
    pub fn redo(
        &self,
        cache: &MetaCache,
        start: Lsn,
        lsn: Lsn,
        f: impl FnOnce(&mut Writer<'_>),
    ) -> Option<bool> {
        let mut hold = self.try_lock()?;
        let page = hold.page();
        if page::lsn(page) >= lsn {
            hold.release_unchanged();
            return Some(false);
        }
        f(page);
        page::set_lsn(page, lsn);
        hold.commit(cache, start);
        Some(true)
    }
}

/// A metadata page latched for a change; released, with its version bumped, when dropped.
pub struct MetaHold<'a> {
    frame: &'a MetaFrame,
    page: Writer<'a>,
    hold: Held<&'a VersionedLatch>,
}

impl<'a> MetaHold<'a> {
    pub fn page(&mut self) -> &mut Writer<'a> {
        &mut self.page
    }

    /// Releases the latch after a change whose record starts at `start`, noting the page dirty
    /// first. Noted after the release, a later change could be noted before it and stand as the
    /// page's oldest, and a checkpoint's redo point would pass this one unwritten.
    pub fn commit(self, cache: &MetaCache, start: Lsn) {
        self.frame.changed(cache, start);
    }

    /// Releases the latch keeping the version, for a hold under which nothing was written.
    pub fn release_unchanged(self) {
        self.hold.release_unchanged();
    }
}

/// One tree's file for a size class and level, and the cells of its pages.
#[derive(Default)]
struct FileSlot {
    file: SetOnce<Arc<dyn File>>,
    frames: Segmented<PinCell<MetaFrame>>,
}

pub struct MetaCache {
    io: Arc<dyn IO>,
    base: String,
    /// Pages kept before clean ones no one holds are dropped.
    budget: usize,
    epochs: Arc<Epochs>,
    /// By [`MetaId::file_index`].
    files: Box<[FileSlot]>,
    cells: Cells<MetaFrame>,
    /// Pages put in the cache since an eviction last took them, for its order.
    arrivals: Stack<MetaId>,
    /// Pages in the order they came in, for eviction: owned by the eviction under way, and only
    /// ever tried, so an eviction that finds another under way leaves the work to it.
    order: Mutex<VecDeque<MetaId>>,
    /// Dirty pages, tagged with their ids, at where their oldest unwritten change's record
    /// starts.
    dirty: Floors,
    retiring: Retiring,
    /// While recovering, a page with no whole copy reads as zeros; afterwards it is corruption,
    /// unless it was never written at all.
    recovering: AtomicBool,
}

impl MetaCache {
    pub fn new(io: Arc<dyn IO>, base: &str, budget: usize, epochs: Arc<Epochs>) -> Self {
        Self {
            io,
            base: base.to_string(),
            budget,
            files: (0..2 * CLASS_COUNT * LEVELS)
                .map(|_| FileSlot::default())
                .collect(),
            cells: Cells::new(epochs.clone()),
            arrivals: Stack::default(),
            order: Mutex::new(VecDeque::new()),
            dirty: Floors::new(epochs.clone()),
            retiring: Retiring::new(epochs.clone()),
            epochs,
            recovering: AtomicBool::new(false),
        }
    }

    pub fn set_recovering(&self, recovering: bool) {
        self.recovering.store(recovering, Ordering::Release);
    }

    /// The hold a completion keeps on the cache, weak: the completion waits in the cache's own
    /// IO, so a strong hold would keep a cache dropped with it in flight alive, and for good once
    /// nothing steps that IO.
    fn held(self: &Arc<Self>) -> Weak<Self> {
        Arc::downgrade(self)
    }

    fn file(&self, id: MetaId) -> Result<Arc<dyn File>> {
        let slot = &self.files[id.file_index()];
        if let Some(f) = slot.file.get() {
            return Ok(f);
        }
        // Two first users may both open it, and the first to set it is kept.
        let f = self.io.open_file(
            &path(&self.base, id.file_key()),
            OpenFlags::Create | OpenFlags::Direct,
            false,
        )?;
        // A page written here must survive a crash, so the file's creation must too.
        self.io.sync_directory(&directory(&self.base))?;
        Ok(slot.file.set(f))
    }

    /// The page `id`, once it is in memory. It stays in the cache while the frame returned, or a
    /// clone of it, is held, so every change to the page reaches that frame.
    pub fn get(self: &Arc<Self>, id: MetaId) -> Result<IOResult<MetaRef>> {
        let cell = self.files[id.file_index()].frames.get_or_make(id.index);
        let frame = loop {
            let step = self.epochs.enter();
            if let Some(frame) = cell.get(&step, &self.cells) {
                break frame;
            }
            drop(step);
            let fresh = Pinned::new(MetaFrame::new(id));
            if cell.put(&fresh, &self.cells) {
                self.arrivals.push(id);
                self.load(&fresh)?;
                self.evict_over_budget();
                break fresh;
            }
        };
        match frame.status.load(Ordering::Acquire) {
            READY => Ok(IOResult::Done(frame)),
            FAILED => {
                // Forgotten, so the next to look loads it again.
                let _ = cell.forget(&frame, &self.cells);
                let e = frame.failure.get().expect("a failed page's error");
                Err(LimboError::CompletionError(e))
            }
            _ => {
                let step = self.epochs.enter();
                let loaded = frame.loaded.get(&step);
                Ok(IOResult::IO(IOCompletions(
                    loaded.unwrap_or_else(Completion::new_yield),
                )))
            }
        }
    }

    /// The page `id`, if it is in memory, held.
    fn cached(&self, id: MetaId) -> Option<MetaRef> {
        let cell = self.files[id.file_index()].frames.get(id.index)?;
        let step = self.epochs.enter();
        cell.get(&step, &self.cells)
    }

    /// Every page in `ids`, once all are in memory.
    pub fn get_all(self: &Arc<Self>, ids: &[MetaId]) -> Result<IOResult<Vec<MetaRef>>> {
        let mut out = Vec::with_capacity(ids.len());
        let mut wait = None;
        for &id in ids {
            match self.get(id)? {
                IOResult::Done(frame) => out.push(frame),
                IOResult::IO(io) => {
                    wait.get_or_insert(io);
                }
            }
        }
        match wait {
            Some(io) => Ok(IOResult::IO(io)),
            None => Ok(IOResult::Done(out)),
        }
    }

    /// Reads both places of `frame`'s page and keeps the newest whole copy. The read holds the
    /// page, which no one else is handed while it loads.
    fn load(self: &Arc<Self>, frame: &MetaRef) -> Result<()> {
        let id = frame.id;
        let file = match self.file(id) {
            Ok(file) => file,
            Err(e) => {
                frame.fail(io_failure(&e));
                return Err(e);
            }
        };
        let block = Arc::new(AlignedBlock::zeroed(2 * META_PAGE));
        let loaded = Completion::new_wait();
        frame.loaded.put(loaded.clone());
        let target = frame.clone();
        let me = self.held();
        let recovering = self.recovering.load(Ordering::Acquire);
        let held = block.clone();
        let c = Completion::new_read(block.region(0, 2 * META_PAGE), move |res| {
            // Gone, the cache has no one left to wait on the page.
            let me = me.upgrade()?;
            let outcome = match res {
                Ok((_, n)) => {
                    // SAFETY: the read filled the block, which nothing else uses.
                    let both = unsafe { std::slice::from_raw_parts(held.as_ptr(), 2 * META_PAGE) };
                    let n = n.max(0) as usize;
                    choose_copy(id, both, n, recovering)
                }
                Err(e) => Err(e),
            };
            // The page before its status: a page found ready reads whole.
            if let Ok((Some(bytes), _)) = &outcome {
                let mut hold = target
                    .try_lock()
                    .expect("a page loading is latched by no one");
                hold.page().write(0, bytes);
            }
            match outcome {
                Ok((_, side)) => {
                    target
                        .durable_side
                        .store(side.map_or(0, |s| s + 1), Ordering::Release);
                    target.status.store(READY, Ordering::Release);
                }
                Err(e) => target.fail(e),
            }
            if let Some(c) = target.loaded.take(&me.retiring) {
                c.complete(0);
            }
            None
        });
        if let Err(e) = file.pread(id.offset(), c) {
            frame.fail(io_failure(&e));
            if let Some(c) = frame.loaded.take(&self.retiring) {
                c.complete(0);
            }
            return Err(e);
        }
        Ok(())
    }

    /// Drops clean pages no one holds while over the budget, oldest first.
    fn evict_over_budget(&self) {
        if self.cells.count() <= self.budget {
            return;
        }
        let Some(mut order) = self.order.try_lock() else {
            return;
        };
        order.extend(self.arrivals.take_all());
        let step = self.epochs.enter();
        for _ in 0..order.len() {
            if self.cells.count() <= self.budget {
                break;
            }
            let Some(id) = order.pop_front() else { break };
            let Some(cell) = self.files[id.file_index()].frames.get(id.index) else {
                continue;
            };
            if !cell.evict(&step, &self.cells) && cell.occupied() {
                order.push_back(id);
            }
        }
    }

    /// Where the log record of the oldest change on a page not yet on disk starts.
    pub fn oldest_unwritten(&self) -> Option<Lsn> {
        self.dirty.least()
    }

    /// Writes every page dirty before `before` and waits for it, waiting first for the log to be
    /// durable up to each page's LSN.
    pub fn write_dirty(self: &Arc<Self>, log: &Arc<Log>, before: Lsn) -> Result<IOResult<()>> {
        let mut wait = None;
        for number in self.dirty.tagged_below(before) {
            // A dirty page is held in the cache until written; one gone was written meanwhile.
            let Some(frame) = self.cached(MetaId::from_number(number)) else {
                continue;
            };
            if let Some(c) = self.write(&frame, log)? {
                wait.get_or_insert(c);
            }
        }
        match wait {
            Some(c) => Ok(IOResult::IO(IOCompletions(c))),
            None => Ok(IOResult::Done(())),
        }
    }

    /// Starts writing `frame` if it is dirty; returns what to wait for, or `None` once it is
    /// clean.
    fn write(self: &Arc<Self>, frame: &MetaRef, log: &Arc<Log>) -> Result<Option<Completion>> {
        // Copied without the latch. A change is counted before its latch is released, so the
        // count read between the version and its check is the copy's: a change the copy misses
        // is counted after it, and the page stays dirty.
        let Some(version) = frame.latch.optimistic() else {
            return Ok(Some(Completion::new_yield()));
        };
        let changes = frame.dirt.count();
        let copy = Arc::new(AlignedBlock::zeroed(META_PAGE));
        // SAFETY: a fresh block of the page's size, which nothing else reaches yet.
        frame.bytes.copy_to(0, unsafe {
            std::slice::from_raw_parts_mut(copy.as_ptr(), META_PAGE)
        });
        if !frame.latch.validate(version) {
            return Ok(Some(Completion::new_yield()));
        }
        {
            let step = self.epochs.enter();
            if let Some(c) = frame.writing.get(&step) {
                return Ok(Some(c));
            }
        }
        if changes & DIRTY == 0 {
            return Ok(None);
        }
        // SAFETY: the copy is ours alone.
        let lsn = page::lsn(unsafe { std::slice::from_raw_parts(copy.as_ptr(), META_PAGE) });
        if log.durable() < lsn {
            return match log.wait_durable(lsn)? {
                IOResult::Done(()) => self.write(frame, log),
                IOResult::IO(IOCompletions(c)) => Ok(Some(c)),
            };
        }
        let writing = Completion::new_wait();
        if !frame.writing.try_put(writing.clone()) {
            // Another write of the page started meanwhile.
            let step = self.epochs.enter();
            return Ok(Some(
                frame
                    .writing
                    .get(&step)
                    .unwrap_or_else(Completion::new_yield),
            ));
        }
        // Read once the write before it ended, so it is the newest durable copy's place, which
        // this write leaves alone.
        let side: u8 = match frame.durable_side.load(Ordering::Acquire) {
            0 => 0,
            d => 2 - d,
        };
        // SAFETY: the copy is ours alone.
        let sealed = unsafe { std::slice::from_raw_parts_mut(copy.as_ptr(), META_PAGE) };
        page::seal(sealed, frame.id.pid());
        let file = match self.file(frame.id) {
            Ok(file) => file,
            Err(e) => {
                if let Some(c) = frame.writing.take(&self.retiring) {
                    c.error(io_failure(&e));
                }
                return Err(e);
            }
        };
        let at = frame.id.offset() + side as u64 * META_PAGE as u64;
        let target = frame.clone();
        let me = self.held();
        let durable = file.supports_durable_writes();
        // The file weakly too: it holds the IO the write waits in. A file closed before its sync
        // leaves the copy not durable.
        let sync_file = Arc::downgrade(&file);
        let done = move |res: std::result::Result<(), CompletionError>| {
            // Gone, the cache has no one left to wait on the write.
            let Some(me) = me.upgrade() else { return };
            if res.is_ok() {
                target.durable_side.store(side + 1, Ordering::Release);
                target.written(&me, changes, lsn);
            }
            if let Some(c) = target.writing.take(&me.retiring) {
                match res {
                    Ok(()) => c.complete(0),
                    Err(e) => c.error(e),
                }
            }
        };
        let done = Arc::new(TakeOnce::new(done));
        let finish = move |res| {
            if let Some(done) = done.take() {
                done(res);
            }
        };
        let c = Completion::new_write(move |res| {
            let res = whole(META_PAGE, res);
            match res {
                Ok(()) if !durable => {
                    // Durable only once synced: until then the other place keeps the newest
                    // durable copy, and the next write still goes here.
                    let Some(sync_file) = sync_file.upgrade() else {
                        return finish(Err(CompletionError::Aborted));
                    };
                    let after = finish.clone();
                    let synced = Completion::new_sync(move |r| after(r.map(|_| ())));
                    if let Err(e) = sync_file.sync_data(synced) {
                        finish(Err(io_failure(&e)));
                    }
                }
                res => finish(res),
            }
        });
        let buffer = copy.region(0, META_PAGE);
        let submitted = if durable {
            file.pwrite_durable(at, buffer, c)
        } else {
            file.pwrite(at, buffer, c)
        };
        if let Err(e) = submitted {
            if let Some(c) = frame.writing.take(&self.retiring) {
                c.error(io_failure(&e));
            }
            return Err(e);
        }
        Ok(Some(writing))
    }
}

/// The newest whole copy among a page's two places, and which place it is in: `(None, None)` for
/// a page never written.
#[allow(clippy::type_complexity)]
fn choose_copy(
    id: MetaId,
    both: &[u8],
    read: usize,
    recovering: bool,
) -> std::result::Result<(Option<Vec<u8>>, Option<u8>), CompletionError> {
    let pid = id.pid();
    let mut best: Option<(u64, u8)> = None;
    for side in 0..2u8 {
        let at = side as usize * META_PAGE;
        if read < at + META_PAGE {
            continue;
        }
        let copy = &both[at..at + META_PAGE];
        if page::verify(copy, pid) {
            let lsn = page::lsn(copy);
            if best.is_none_or(|(l, _)| lsn > l) {
                best = Some((lsn, side));
            }
        }
    }
    if let Some((_, side)) = best {
        let at = side as usize * META_PAGE;
        return Ok((Some(both[at..at + META_PAGE].to_vec()), Some(side)));
    }
    let written = both[..read.min(both.len())].iter().any(|&b| b != 0);
    if written && !recovering {
        return Err(CompletionError::PageCodecError {
            page_idx: id.pid().raw() as usize,
        });
    }
    Ok((None, None))
}

fn io_failure(e: &LimboError) -> CompletionError {
    match e {
        LimboError::CompletionError(e) => *e,
        _ => CompletionError::IOError(std::io::ErrorKind::Other, "metadata IO submission"),
    }
}

fn directory(base: &str) -> String {
    std::path::Path::new(base)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".".to_string())
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    /// A write of a dirty page lands as a change comes: the page ends dirty, with one floor
    /// held, at the oldest change not on disk; and once written again, clean, with none.
    #[test]
    fn a_change_as_a_write_lands_leaves_the_page_dirty_once() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let dirty = Arc::new(Floors::new(Arc::new(Epochs::new())));
            let dirt = Arc::new(Dirt::default());
            assert!(dirt.changed(&dirty, 10, 1));
            let copied = dirt.count();
            let write = {
                let (dirty, dirt) = (dirty.clone(), dirt.clone());
                thread::spawn(move || dirt.written(&dirty, copied))
            };
            let turned = dirt.changed(&dirty, 20, 1);
            let cleaned = write.join().unwrap();
            // Cleaned first, the change turned it dirty again; changed first, the write kept it.
            assert_eq!(turned, cleaned);
            assert_eq!(dirty.least(), Some(if cleaned { 20 } else { 10 }));
            assert_eq!(dirty.tagged_below(u64::MAX), vec![1]);
            assert!(dirt.written(&dirty, dirt.count()));
            assert_eq!(dirty.least(), None);
        });
    }
}
