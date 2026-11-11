//! Metadata pages: the page table, the free-space maps and the retired marks.
//!
//! They are 4 KiB pages in files of their own, one per tree, size class and level, and live in a
//! cache of their own rather than the buffer pool, because a buffer miss needs the page table.
//!
//! **Two places per page.** A metadata page has two places side by side at a position its index
//! fixes. Each copy carries the number of the checkpoint that wrote it in its header's `seq`, where
//! a data page carries its change count. A checkpoint writes to the place that does not hold the copy of the checkpoint in
//! charge, so a torn write leaves that copy whole, and a read takes the newest whole copy not past
//! the checkpoint in charge, so a copy a checkpoint wrote before a crash cut it short is passed
//! over. Metadata therefore needs no free space of its own and no parent pointing at it.
//!
//! **Written by checkpoints, as of their cut** (audit 18). Changes are made in memory, unlogged,
//! and only a checkpoint writes a page. Between a checkpoint's cut and the end of its writes, a
//! page's first change copies the page first, so the checkpoint writes it as it stood at the cut,
//! with the checkpoint's own edits ([`MetaCache::edit`]). A page a checkpoint cut short may have
//! written is passed over on reading and rewritten by the next ([`MetaCache::owe`]).
//!
//! **Latching** is the buffer pool's (CONCURRENCY.md §5): a change holds the page's versioned
//! latch, taken without waiting, and a reader takes nothing and validates the version after it
//! read. A taker that finds the latch held yields.
//!
//! **No lock.** The cache keeps a cell per page, in an array per file that grows to the highest
//! page reached ([`Segmented`]). A page found is pinned, and only a page no pin holds is evicted
//! ([`pins`](super::pins)). A dirty page holds a pin of its own, and is listed among the dirty
//! pages, until it is written; a load or a write in flight holds one too, and a checkpoint holds
//! the pages it writes until it is in charge: so a page evicted is clean and idle. Whether a page
//! is dirty is a bit of its count of changes, which a write that lands clears by compare-and-swap,
//! so it clears it only if no change came since its copy.

use self::sync::AtomicU64;
use super::aligned::AlignedBlock;
use super::buffer::cell::{Changed, CompletionCell, CompletionDomain};
use super::epoch::Epochs;
use super::latch::{Held, LatchRef, Look, Marked, VersionedLatch};
use super::once::{SetOnce, TakeOnce};
use super::page::{self, PageId, SizeClass, CLASS_COUNT, MIN_PAGE_SIZE};
use super::pins::{Cells, PinCell, Pinnable, Pinned, Pins};
use super::segmented::Segmented;
use super::shared::{Bytes, BytesMut, Shared, Writer};
use super::stack::{Chain, Stack};
use super::whole;
use crate::io::{Completion, File, OpenFlags, IO};
use crate::storage::atoll::locks::Mutex;
use crate::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use crate::sync::{Arc, Weak};
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

/// Metadata trees per size class.
pub const TREES: usize = 2;

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
const TREE_SHIFT: u32 = 53;
const CLASS_SHIFT: u32 = 49;
const INDEX_BITS: u32 = 46;
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

    /// The id whose [`pid`](Self::pid) is `pid`, if it is one.
    pub fn of_pid(pid: PageId) -> Option<Self> {
        let number = pid.number();
        (pid.class().index() == 0
            && number & META_BIT != 0
            && (number >> TREE_SHIFT & 3) < TREES as u64)
            .then(|| Self::from_number(number))
    }

    /// The id whose [`pid`](Self::pid) has `number`.
    fn from_number(number: u64) -> Self {
        Self {
            tree: match number >> TREE_SHIFT & 3 {
                0 => Tree::Map,
                1 => Tree::Space,
                _ => unreachable!("the cache tags only ids it made"),
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
const READING: u8 = 1;
const PUBLISHING: u8 = 2;
const READY: u8 = 3;
const FAILED: u8 = 4;

/// In a page's count of changes: a change not yet on disk.
const DIRTY: u64 = 1;

/// Whether a page has a change not yet on disk.
#[derive(Default)]
struct Dirt {
    /// Twice the changes made, plus [`DIRTY`] while one is not on disk.
    changes: AtomicU64,
}

impl Dirt {
    /// Counts a change made by the only change under way: whether the page turned dirty.
    fn changed(&self) -> bool {
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
        was & DIRTY == 0
    }

    /// The count a copy of the page taken now, while no change is under way, carries.
    fn count(&self) -> u64 {
        self.changes.load(Ordering::Acquire)
    }

    /// A write of the copy taken at `count` landed: whether the page is clean now, which it is
    /// unless a change came since the copy. Writes of a page land one at a time.
    fn written(&self, count: u64) -> bool {
        self.changes
            .compare_exchange(count, count & !DIRTY, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

/// Bytes of the metadata pages kept as a cut found them, now and at most. No one waits for
/// them, so a checkpoint always reaches the writes that drop them; their bound is a page each,
/// kept once a cut, beside the pages the checkpoint writes anyway (DESIGN.md §9.6).
#[derive(Default)]
pub struct CutBytes {
    used: AtomicUsize,
    peak: AtomicUsize,
}

impl CutBytes {
    /// Bytes held now, and the most held.
    #[cfg(test)]
    pub fn stats(&self) -> (usize, usize) {
        (
            self.used.load(Ordering::Acquire),
            self.peak.load(Ordering::Acquire),
        )
    }
}

/// A page as a cut found it, counted until its bytes are dropped.
struct CutCopy {
    bytes: Box<[u8]>,
    of: Arc<CutBytes>,
}

impl CutCopy {
    fn new(bytes: Box<[u8]>, of: &Arc<CutBytes>) -> Self {
        let used = of.used.fetch_add(bytes.len(), Ordering::AcqRel) + bytes.len();
        of.peak.fetch_max(used, Ordering::AcqRel);
        Self {
            bytes,
            of: of.clone(),
        }
    }
}

impl std::ops::Deref for CutCopy {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::ops::DerefMut for CutCopy {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl Drop for CutCopy {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.of.used.fetch_sub(self.bytes.len(), Ordering::AcqRel);
    }
}

pub struct MetaFrame {
    pub id: MetaId,
    latch: VersionedLatch,
    /// Written only under the latch, held exclusively; read under it, or optimistically.
    bytes: Box<Shared>,
    pins: Pins,
    /// Preparing, reading or publishing while below [`READY`]; [`FAILED`] keeps its error in
    /// `failure`.
    status: AtomicU8,
    failure: SetOnce<CompletionError>,
    dirt: Dirt,
    /// The place holding the copy of the checkpoint in charge, plus one; 0 if the page has none.
    durable_side: AtomicU8,
    /// The checkpoint that last wrote the page, and the place it wrote plus one: the page's
    /// durable place once that checkpoint is in charge ([`MetaCache::checkpointed`]).
    written_for: AtomicU64,
    written_side: AtomicU8,
    /// The checkpoint whose cut is in effect, 0 for none: shared with the cache.
    cutting: Arc<crate::sync::atomic::AtomicU64>,
    /// The page as it stood at the cut of checkpoint `cut_for`, kept by its first change since,
    /// or by that checkpoint's own edit of the cut alone.
    cut: Mutex<Option<CutCopy>>,
    cut_bytes: Arc<CutBytes>,
    cut_for: crate::sync::atomic::AtomicU64,
    /// The write in flight, if any.
    writing: CompletionCell,
    /// An accepted read, completed once its callback begins publishing the page.
    loaded: CompletionCell,
    /// The actor preparing a read or publishing its result, completed when that phase ends.
    load_work: CompletionCell,
    /// The hold's release, captured before marking its latch waited.
    released: CompletionCell,
    /// Announcements for the release signal, valid while this frame lives.
    release_completions: CompletionDomain,
}

/// A metadata page held in the cache.
pub type MetaRef = Pinned<MetaFrame>;

impl Pinnable for MetaFrame {
    fn pins(&self) -> &Pins {
        &self.pins
    }
}

impl MetaFrame {
    fn new(
        id: MetaId,
        cutting: Arc<crate::sync::atomic::AtomicU64>,
        cut_bytes: Arc<CutBytes>,
        epochs: Arc<Epochs>,
    ) -> Self {
        Self {
            id,
            latch: VersionedLatch::default(),
            bytes: Shared::zeroed(META_PAGE),
            pins: Pins::default(),
            status: AtomicU8::new(LOADING),
            failure: SetOnce::default(),
            dirt: Dirt::default(),
            durable_side: AtomicU8::new(0),
            written_for: AtomicU64::new(0),
            written_side: AtomicU8::new(0),
            cutting,
            cut: Mutex::new(None),
            cut_bytes,
            cut_for: crate::sync::atomic::AtomicU64::new(0),
            writing: CompletionCell::default(),
            loaded: CompletionCell::default(),
            load_work: CompletionCell::default(),
            released: CompletionCell::default(),
            release_completions: CompletionDomain::new(epochs),
        }
    }

    /// The page's bytes, copied, under a latch held or with no change under way.
    fn copy_bytes(&self) -> Box<[u8]> {
        let mut out = vec![0u8; META_PAGE].into_boxed_slice();
        self.bytes.copy_to(0, &mut out);
        out
    }

    /// Keeps the page as it stands, under its latch held exclusively, as it stood at the cut in
    /// effect, unless it is kept already: what that checkpoint writes of it.
    fn keep_cut(&self) {
        let n = self.cutting.load(Ordering::Acquire);
        if n != 0 && self.cut_for.load(Ordering::Acquire) != n {
            *self.cut.lock() = Some(CutCopy::new(self.copy_bytes(), &self.cut_bytes));
            self.cut_for.store(n, Ordering::Release);
        }
    }

    fn fail(&self, e: CompletionError) {
        self.failure.set(e);
        self.status.store(FAILED, Ordering::Release);
    }

    /// Reads the page as it stood at checkpoint `n`'s cut: `cut` over the copy kept by its first
    /// change since, or `live` over the page as it stands where none was; `None` while a change
    /// holds the latch.
    pub fn read_cut<R>(
        &self,
        n: u64,
        live: impl Fn(&Shared) -> R,
        cut: impl Fn(&[u8]) -> R,
    ) -> Option<R> {
        loop {
            let version = self.latch.optimistic()?;
            let out = match self.cut.lock().as_deref() {
                Some(bytes) if self.cut_for.load(Ordering::Acquire) == n => cut(bytes),
                _ => live(&self.bytes),
            };
            if self.latch.validate(version) {
                return Some(out);
            }
        }
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

    /// Latches the page for a change, or `None` while a change holds it. Between a checkpoint's
    /// cut and the end of its writes, the page is first kept as it stood at the cut.
    pub fn try_lock(&self) -> Option<MetaHold<'_>> {
        Held::try_take(MetaLatch(self)).map(|hold| {
            crate::race::point("meta.lock_before_keep");
            self.keep_cut();
            MetaHold {
                frame: self,
                // SAFETY: the hold has the latch exclusively, and keeps it while the writer lives.
                page: unsafe { self.bytes.writer() },
                hold,
            }
        })
    }

    /// The checkpoint the page's last landed write was for.
    #[cfg(test)]
    pub(crate) fn last_written_for(&self) -> u64 {
        self.written_for.load(Ordering::Acquire)
    }

    /// Whether a writer holds this page now. A read that follows still validates its own view.
    pub(crate) fn is_held(&self) -> bool {
        matches!(self.latch.look(), Look::Held(_))
    }

    /// Waits for a hold found by a read or edit to end. A hold that changed before the mark
    /// returns a yield on that change, with no wait on a signal whose producer is already gone.
    pub(crate) fn wait_while_held(&self) -> Completion {
        let seen = match self.latch.look() {
            Look::Free(_) => return Completion::new_yield(),
            Look::Held(seen) => seen,
        };
        let wait = self.released.current_with(
            &self.release_completions,
            crate::storage::atoll::notify::remote_wait,
        );
        crate::race::point("meta.wait_before_mark");
        match self.latch.mark_held(seen) {
            Marked::Waits => wait,
            Marked::Changed => Completion::new_yield(),
        }
    }

    /// Notes a change made under the latch, which is still held and so the only change under way.
    fn changed(&self, cache: &MetaCache) {
        if self.dirt.changed() {
            // Kept in the cache until it is written. The change's own holder has it pinned, so it
            // is not evicted meanwhile.
            self.pins.repin();
            cache.dirty.push(self.id);
        }
    }

    /// A write of the copy taken at `count` landed, or of the page as it stood at the cut for
    /// `None`: a change came since then.
    fn written(&self, cache: &MetaCache, count: Option<u64>) {
        match count {
            Some(count) if self.dirt.written(count) => self.pins.unpin(),
            // A change since the copy: the next checkpoint writes the page again.
            _ => cache.dirty.push(self.id),
        }
    }
}

/// The metadata frame whose latch is taken, so every marked release finishes its signal.
struct MetaLatch<'a>(&'a MetaFrame);

impl std::ops::Deref for MetaLatch<'_> {
    type Target = VersionedLatch;

    fn deref(&self) -> &Self::Target {
        &self.0.latch
    }
}

impl LatchRef for MetaLatch<'_> {
    fn release_marked<F: FnOnce()>(&self, unlock: F) {
        // The acquired mark publishes its registration. Free the latch before taking the cell:
        // a waiter that installs after the take must find this hold gone when it marks it.
        unlock();
        if let Some(wait) = self.0.released.take(&self.0.release_completions) {
            crate::storage::atoll::notify::finish(&wait, Ok(0));
        }
    }
}

/// A metadata page latched for a change; released, with its version bumped, when dropped.
pub struct MetaHold<'a> {
    frame: &'a MetaFrame,
    page: Writer<'a>,
    hold: Held<MetaLatch<'a>>,
}

impl<'a> MetaHold<'a> {
    pub fn page(&mut self) -> &mut Writer<'a> {
        &mut self.page
    }

    /// Releases the latch after a change, noting the page dirty first.
    pub fn commit(self, cache: &MetaCache) {
        self.frame.changed(cache);
    }

    /// Runs `f` on the page as it stood at the cut in effect, kept when the hold was taken: what
    /// that checkpoint writes of it. `None` with no cut in effect.
    pub fn with_cut<R>(&mut self, f: impl FnOnce(&mut [u8]) -> R) -> Option<R> {
        let n = self.frame.cutting.load(Ordering::Acquire);
        if n == 0 || self.frame.cut_for.load(Ordering::Acquire) != n {
            return None;
        }
        self.frame.cut.lock().as_mut().map(|bytes| f(bytes))
    }

    /// Releases the latch keeping the version, for a hold under which nothing was written.
    pub fn release_unchanged(self) {
        self.hold.release_unchanged();
    }
}

#[cfg(test)]
crate::thread::thread_local! {
    /// The request of the last metadata write submitted on this thread, for a test to cancel as a
    /// backend's cancel would.
    pub(crate) static LAST_WRITE: std::cell::RefCell<Option<Completion>> =
        const { std::cell::RefCell::new(None) };
}

/// Tells a metadata write's waiter how it ended, once ([`MetaCache::write_for`]).
type Tell = TakeOnce<Box<dyn FnOnce() + Send>>;

/// Runs a [`Tell`] if no one has yet.
fn tell_once(tell: &Tell) {
    if let Some(tell) = tell.take() {
        tell();
    }
}

/// One request of a metadata write retired (or a sync refused): the last of `unretired` tells.
fn retired(unretired: &AtomicUsize, tell: &Tell) {
    let before = unretired.fetch_sub(1, Ordering::AcqRel);
    debug_assert!(before > 0, "a metadata write's request retired twice");
    if before == 1 {
        tell_once(tell);
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
    /// Pages turned dirty, each once until a checkpoint takes it.
    dirty: Stack<MetaId>,
    completions: CompletionDomain,
    /// The checkpoint in charge: a copy written by a later one is passed over.
    in_charge: crate::sync::atomic::AtomicU64,
    /// The checkpoint whose cut is in effect, 0 for none, shared with every frame.
    cutting: Arc<crate::sync::atomic::AtomicU64>,
    /// The bytes of the pages kept as a cut found them, shared with every frame.
    cut_bytes: Arc<CutBytes>,
    /// Pages a checkpoint cut short by a crash may have written, as recovery found them in the
    /// log: their bytes past a copy in charge may be torn, which is no corruption.
    torn_ok: AtomicBool,
}

impl MetaCache {
    /// The cache of the metadata as checkpoint `in_charge` left it.
    pub fn new(
        io: Arc<dyn IO>,
        base: &str,
        budget: usize,
        epochs: Arc<Epochs>,
        in_charge: u64,
    ) -> Self {
        Self {
            io,
            base: base.to_string(),
            budget,
            files: (0..TREES * CLASS_COUNT * LEVELS)
                .map(|_| FileSlot::default())
                .collect(),
            cells: Cells::new(epochs.clone()),
            arrivals: Stack::default(),
            order: Mutex::new(VecDeque::new()),
            dirty: Stack::default(),
            completions: CompletionDomain::new(epochs.clone()),
            epochs,
            in_charge: crate::sync::atomic::AtomicU64::new(in_charge),
            cutting: Arc::new(crate::sync::atomic::AtomicU64::new(0)),
            cut_bytes: Arc::default(),
            torn_ok: AtomicBool::new(false),
        }
    }

    /// The checkpoint in charge.
    pub fn in_charge(&self) -> u64 {
        self.in_charge.load(Ordering::Acquire)
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
            let fresh = Pinned::new(MetaFrame::new(
                id,
                self.cutting.clone(),
                self.cut_bytes.clone(),
                self.epochs.clone(),
            ));
            if cell.put(&fresh, &self.cells) {
                self.arrivals.push(id);
                crate::race::point("meta.before_load");
                self.load(&fresh)?;
                self.evict_over_budget();
                break fresh;
            }
        };
        let mut status = frame.status.load(Ordering::Acquire);
        let loaded = if status < READY {
            let seen = status;
            crate::race::point("meta.loading_seen");
            let wait = if seen == READING {
                frame.loaded.current(&self.completions)
            } else {
                frame.load_work.current_with(
                    &self.completions,
                    crate::storage::atoll::notify::remote_wait,
                )
            };
            crate::race::point("meta.wait_registered");
            // Each phase is published before its signal is taken. An install after that take
            // sees the new phase here and never waits on the signal whose producer has left.
            status = frame.status.load(Ordering::Acquire);
            if status < READY && status != seen {
                return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
            }
            Some(wait)
        } else {
            None
        };
        match status {
            READY => Ok(IOResult::Done(frame)),
            FAILED => {
                // Forgotten, so the next to look loads it again.
                let _ = cell.forget(&frame, &self.cells);
                let e = frame.failure.get().expect("a failed page's error");
                Err(LimboError::CompletionError(e))
            }
            LOADING | READING | PUBLISHING => Ok(IOResult::IO(IOCompletions(
                loaded.expect("a loading page registered before its status was checked again"),
            ))),
            _ => unreachable!("invalid metadata frame status {status}"),
        }
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
                self.finish_load(frame);
                return Err(e);
            }
        };
        let block = Arc::new(AlignedBlock::zeroed(2 * META_PAGE));
        let target = frame.clone();
        let me = self.held();
        let (ceiling, torn_ok) = (self.in_charge(), self.torn_ok.load(Ordering::Acquire));
        let into = block.region(0, 2 * META_PAGE);
        let held = block;
        let c = Completion::new_read(into, move |res| {
            // Gone, the cache has no one left to wait on the page.
            let me = me.upgrade()?;
            me.read_arrived(&target);
            crate::race::point("meta.load_publishing");
            let outcome = match res {
                Ok((_, n)) => {
                    // SAFETY: the read filled the block, which nothing else uses.
                    let both = unsafe { std::slice::from_raw_parts(held.as_ptr(), 2 * META_PAGE) };
                    let n = n.max(0) as usize;
                    choose_copy(id, both, n, ceiling, torn_ok)
                }
                Err(e) => Err(e),
            };
            // The page before its status: a page found ready reads whole. Loaded, not changed:
            // no cut is kept of it.
            if let Ok((Some(bytes), _)) = &outcome {
                let hold = Held::try_take(MetaLatch(&target))
                    .expect("a page loading is latched by no one");
                crate::race::point("meta.load_held");
                // SAFETY: the hold has the latch exclusively while the writer lives.
                unsafe { target.bytes.writer() }.write(0, bytes);
                drop(hold);
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
            me.finish_load(&target);
            None
        });
        crate::race::point("meta.before_read_submit");
        if let Err(e) = file.pread(id.offset(), c) {
            frame.fail(io_failure(&e));
            self.finish_load(frame);
            return Err(e);
        }
        // The backend accepted the read. Its callback may already have published the page,
        // so only the preparing phase can become a queued read.
        if frame
            .status
            .compare_exchange(LOADING, READING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.finish_load_work(frame);
        }
        Ok(())
    }

    /// The read has reached its callback: further progress is this actor's publication work.
    fn read_arrived(&self, frame: &MetaRef) {
        frame.status.store(PUBLISHING, Ordering::Release);
        self.finish_read(frame);
    }

    /// A load's final status is published: both kinds of waiters look at it again.
    fn finish_load(&self, frame: &MetaRef) {
        self.finish_read(frame);
        self.finish_load_work(frame);
    }

    fn finish_read(&self, frame: &MetaRef) {
        if let Some(c) = frame.loaded.take(&self.completions) {
            c.complete(0);
        }
    }

    fn finish_load_work(&self, frame: &MetaRef) {
        if let Some(c) = frame.load_work.take(&self.completions) {
            c.complete(0);
        }
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

    // ---------------------------------------------------------------- checkpoints

    /// The cut of checkpoint `n`: from now until [`end_cut`](Self::end_cut), a page's first
    /// change keeps the page as it stood, for `n` to write. Returns the pages dirty at the cut,
    /// handed over unread for the checkpoint to read a run at a time (DESIGN.md 22.9 item 1), a
    /// page listed once each time it turned dirty.
    pub fn begin_cut(&self, n: u64) -> Chain<MetaId> {
        assert!(
            n > self.in_charge(),
            "checkpoint {n} cut after {}",
            self.in_charge()
        );
        self.cutting.store(n, Ordering::SeqCst);
        self.dirty.take_chain()
    }

    /// Marks `frame` owed a write: a checkpoint cut short may have written its other place, which
    /// the next checkpoint, taking the same number, must write over (audit 18, review 2).
    pub fn owe(&self, frame: &MetaFrame) -> Option<()> {
        let hold = Held::try_take(MetaLatch(frame))?;
        crate::race::point("meta.owe_held");
        frame.changed(self);
        drop(hold);
        Some(())
    }

    /// Starts writing `frame`, for checkpoint `n`, as it stood at the cut, to the place not
    /// holding the copy in charge; returns what to wait for, or `None` once written or clean.
    pub fn write_for(self: &Arc<Self>, frame: &MetaRef, n: u64) -> Result<Option<Completion>> {
        if frame.written_for.load(Ordering::Acquire) == n {
            return Ok(None);
        }
        match frame.writing.get(&self.completions) {
            Ok(Some(c)) => return Ok(Some(c)),
            Err(Changed) => return Ok(Some(Completion::new_yield())),
            Ok(None) => {}
        }
        // The page as it stood at the cut, or as it stands: no change came since the cut, and a
        // change under way would keep it first. A change after the copy counts itself.
        // A cut copy is immutable even under a live hold. Without one, wait before allocating
        // the image: repeated polls of the same paused hold reuse only its logical wait.
        let version = if frame.cut_for.load(Ordering::Acquire) == n {
            None
        } else {
            match frame.latch.optimistic() {
                Some(version) => Some(version),
                None => {
                    crate::race::point("meta.write_found_held");
                    return Ok(Some(frame.wait_while_held()));
                }
            }
        };
        let copy = Arc::new(AlignedBlock::zeroed(META_PAGE));
        // SAFETY: a fresh block of the page's size, which nothing else reaches yet.
        let out = unsafe { std::slice::from_raw_parts_mut(copy.as_ptr(), META_PAGE) };
        let count = if let Some(version) = version {
            let count = frame.dirt.count();
            frame.bytes.copy_to(0, out);
            if !frame.latch.validate(version) || frame.cut_for.load(Ordering::Acquire) == n {
                return Ok(Some(Completion::new_yield()));
            }
            Some(count)
        } else {
            out.copy_from_slice(frame.cut.lock().as_ref().expect("a page kept at the cut"));
            None
        };
        let writing = crate::storage::atoll::notify::wait();
        if !frame.writing.try_put(writing.clone()) {
            let wait = match frame.writing.get(&self.completions) {
                Ok(Some(c)) => c,
                Ok(None) | Err(Changed) => Completion::new_yield(),
            };
            return Ok(Some(wait));
        }
        let side: u8 = match frame.durable_side.load(Ordering::Acquire) {
            0 => 0,
            d => 2 - d,
        };
        page::set_seq(out, n);
        page::seal(out, frame.id.pid());
        let file = match self.file(frame.id) {
            Ok(file) => file,
            Err(e) => {
                if let Some(c) = frame.writing.take(&self.completions) {
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
        // How the write ended, as its callbacks found it. `writing` is told once every request of
        // the write has retired, no backend owning its buffer any more: the write's own and, where
        // the file is not written durably, its sync's, either of which may retire first on a
        // backend stepped by several threads. A checkpoint counts the write in its flight set
        // until then (DESIGN.md 22.1, 22.13).
        let outcome = Arc::new(Mutex::new(None));
        let tell: Arc<Tell> = {
            let (target, me, outcome) = (target.clone(), me.clone(), outcome.clone());
            Arc::new(TakeOnce::new(Box::new(move || {
                // Gone, the cache has no one left to wait on the write.
                let Some(me) = me.upgrade() else { return };
                if let Some(c) = target.writing.take(&me.completions) {
                    // A request let go with no outcome told never ran. Taken out of its lock
                    // before the wait is finished, whose waker may run code.
                    let told = outcome.lock().take();
                    match told.unwrap_or(Err(CompletionError::Aborted)) {
                        Ok(()) => c.complete(0),
                        Err(e) => c.error(e),
                    }
                }
            })))
        };
        let done = {
            let outcome = outcome.clone();
            move |res: std::result::Result<(), CompletionError>| {
                if let Some(me) = me.upgrade() {
                    if res.is_ok() {
                        target.written_side.store(side + 1, Ordering::Release);
                        target.written_for.store(n, Ordering::Release);
                        target.written(&me, count);
                    }
                }
                *outcome.lock() = Some(res);
            }
        };
        let done = Arc::new(TakeOnce::new(done));
        let finish = move |res| {
            if let Some(done) = done.take() {
                done(res);
            }
        };
        // The write's requests not yet retired: its own, and its sync's, counted before the sync is
        // submitted, which the write's own retirement, after its callback, cannot precede. The
        // last to retire tells.
        let unretired = Arc::new(AtomicUsize::new(1));
        let c = {
            let (tell, unretired) = (tell.clone(), unretired.clone());
            Completion::new_write(move |res| {
                let res = whole(META_PAGE, res);
                match res {
                    Ok(()) if !durable => {
                        // Durable only once synced: until then the copy in charge is the one
                        // that counts.
                        let Some(sync_file) = sync_file.upgrade() else {
                            return finish(Err(CompletionError::Aborted));
                        };
                        let after = finish.clone();
                        let synced = Completion::new_sync(move |r| after(r.map(|_| ())));
                        unretired.fetch_add(1, Ordering::AcqRel);
                        match sync_file.sync_data(synced) {
                            Ok(synced) => {
                                let (tell, unretired) = (tell.clone(), unretired.clone());
                                synced.on_retired(move || retired(&unretired, &tell));
                            }
                            // Refused, the sync never was a request.
                            Err(e) => {
                                finish(Err(io_failure(&e)));
                                retired(&unretired, &tell);
                            }
                        }
                    }
                    res => finish(res),
                }
            })
        };
        let buffer = copy.region(0, META_PAGE);
        let submitted = if durable {
            file.pwrite_durable(at, buffer, c)
        } else {
            file.pwrite(at, buffer, c)
        };
        match submitted {
            Ok(c) => {
                #[cfg(test)]
                LAST_WRITE.with(|w| *w.borrow_mut() = Some(c.clone()));
                c.on_retired(move || retired(&unretired, &tell))
            }
            Err(e) => {
                if let Some(c) = frame.writing.take(&self.completions) {
                    c.error(io_failure(&e));
                }
                return Err(e);
            }
        }
        Ok(Some(writing))
    }

    /// The checkpoint whose cut is in effect, 0 for none.
    pub fn cutting(&self) -> u64 {
        self.cutting.load(Ordering::Acquire)
    }

    /// Pages in the cache, held or not: their bytes are beyond the frames.
    #[cfg(test)]
    pub fn resident(&self) -> usize {
        self.cells.count()
    }

    /// Bytes of the pages kept as a cut found them, now and at most.
    #[cfg(test)]
    pub fn cut_stats(&self) -> (usize, usize) {
        self.cut_bytes.stats()
    }

    /// Checkpoint `n` has written every page it writes: changes no longer keep a page first, and
    /// what was kept goes, for the frames it wrote, `frames`.
    pub fn end_cut(&self, n: u64, frames: &[MetaRef]) {
        self.stop_keeping(n);
        self.uncut(n, frames);
    }

    /// Checkpoint `n` has written every page it writes: changes no longer keep a page first. What
    /// was kept goes with [`uncut`](Self::uncut), a run of the frames at a time.
    pub fn stop_keeping(&self, n: u64) {
        debug_assert_eq!(self.cutting.load(Ordering::Acquire), n);
        self.cutting.store(0, Ordering::SeqCst);
    }

    /// What checkpoint `n`'s cut kept of `frames` goes, changes no longer keeping any.
    pub fn uncut(&self, n: u64, frames: &[MetaRef]) {
        for f in frames {
            if f.cut_for.load(Ordering::Acquire) == n {
                *f.cut.lock() = None;
            }
        }
    }

    /// Checkpoint `n` is in charge, its superblock durable: each page it wrote, of `frames`, has
    /// its durable copy where it wrote it.
    #[cfg(test)]
    pub fn checkpointed(&self, n: u64, frames: &[MetaRef]) {
        self.durable_sides(n, frames);
        self.put_in_charge(n);
    }

    /// Checkpoint `n`'s superblock is durable: each page it wrote, of `frames`, has its durable
    /// copy where it wrote it. A run of the frames at a time, then
    /// [`put_in_charge`](Self::put_in_charge).
    pub fn durable_sides(&self, n: u64, frames: &[MetaRef]) {
        for f in frames {
            if f.written_for.load(Ordering::Acquire) == n {
                f.durable_side
                    .store(f.written_side.load(Ordering::Acquire), Ordering::Release);
            }
        }
    }

    /// Checkpoint `n` is in charge, every page it wrote given its durable place.
    pub fn put_in_charge(&self, n: u64) {
        self.in_charge.store(n, Ordering::Release);
    }

    /// While a recovery reads the pages a checkpoint cut short may have written: their other place
    /// may hold a torn copy, which is no corruption.
    pub fn set_torn_ok(&self, ok: bool) {
        self.torn_ok.store(ok, Ordering::Release);
    }
}

/// The newest whole copy among a page's two places not past checkpoint `ceiling`, and which place
/// it is in: `(None, None)` for a page no checkpoint up to it wrote. A whole copy past the ceiling
/// is a checkpoint's that a crash cut short, and counts as none. Bytes that hold no whole copy,
/// beside no copy that counts, are corruption, unless `torn_ok`: such a checkpoint may have torn
/// them.
#[allow(clippy::type_complexity)]
fn choose_copy(
    id: MetaId,
    both: &[u8],
    read: usize,
    ceiling: u64,
    torn_ok: bool,
) -> std::result::Result<(Option<Vec<u8>>, Option<u8>), CompletionError> {
    let pid = id.pid();
    let mut best: Option<(u64, u8)> = None;
    let mut torn = false;
    for side in 0..2u8 {
        let at = side as usize * META_PAGE;
        if read < at + META_PAGE {
            continue;
        }
        let copy = &both[at..at + META_PAGE];
        if !page::verify(copy, pid) {
            torn |= copy.iter().any(|&b| b != 0);
            continue;
        }
        let n = page::seq(copy);
        if n <= ceiling && best.is_none_or(|(b, _)| n > b) {
            best = Some((n, side));
        }
    }
    if let Some((_, side)) = best {
        let at = side as usize * META_PAGE;
        return Ok((Some(both[at..at + META_PAGE].to_vec()), Some(side)));
    }
    if torn && !torn_ok {
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

    /// A write of a dirty page lands as a change comes: the page ends dirty exactly when the change
    /// came after the copy, and once written again, clean.
    #[test]
    fn a_change_as_a_write_lands_leaves_the_page_dirty_once() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let dirt = Arc::new(Dirt::default());
            assert!(dirt.changed());
            let copied = dirt.count();
            let write = {
                let dirt = dirt.clone();
                thread::spawn(move || dirt.written(copied))
            };
            let turned = dirt.changed();
            let cleaned = write.join().unwrap();
            // Cleaned first, the change turned it dirty again; changed first, the write kept it.
            assert_eq!(turned, cleaned);
            assert!(dirt.count() & DIRTY != 0);
            assert!(dirt.written(dirt.count()));
            assert!(dirt.count() & DIRTY == 0);
        });
    }
}

#[cfg(all(test, not(any(atoll_loom, shuttle))))]
mod tests;
