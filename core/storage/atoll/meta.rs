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
//! read. A taker that finds the latch held yields. A page's state (dirtiness, its write in flight)
//! sits behind a short mutex, taken after the latch.

use super::aligned::AlignedBlock;
use super::latch::{Held, VersionedLatch};
use super::log::{Log, Lsn};
use super::page::{self, PageId, SizeClass, MIN_PAGE_SIZE};
use crate::io::{Completion, File, OpenFlags, IO};
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::cell::UnsafeCell;
use std::collections::{BTreeSet, HashMap, VecDeque};

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
const INDEX_BITS: u32 = 47;

impl MetaId {
    /// The id sealed into the page: a class-0 page id in a number range data pages never use.
    pub fn pid(self) -> PageId {
        assert!(
            self.index < 1 << INDEX_BITS,
            "metadata index {} out of range",
            self.index
        );
        let tree = match self.tree {
            Tree::Map => 0,
            Tree::Space => 1,
        };
        let number = META_BIT
            | (tree << 54)
            | ((self.class.index() as u64) << 50)
            | ((self.level as u64) << INDEX_BITS)
            | self.index;
        PageId::new(SizeClass::new(0).expect("class 0"), number)
    }

    fn file_key(self) -> FileKey {
        (self.tree, self.class, self.level)
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum Status {
    Loading,
    Ready,
    Failed(CompletionError),
}

struct FrameState {
    status: Status,
    /// Where the log record of the oldest change not yet on disk starts; `None` while clean.
    rec: Option<Lsn>,
    /// Bumped by every change.
    changes: u64,
    /// The write in flight, if any.
    writing: Option<Completion>,
    /// The place holding the newest durable copy; `None` if the page has none.
    durable_side: Option<u8>,
    pins: u32,
}

pub struct MetaFrame {
    pub id: MetaId,
    latch: VersionedLatch,
    /// Written only under the latch, held exclusively; read under it, or optimistically.
    bytes: UnsafeCell<Box<[u8]>>,
    state: Mutex<FrameState>,
    loaded: Mutex<Option<Completion>>,
}

// SAFETY: the bytes are written only under the latch, held exclusively, and read either under it
// or without it and validated against its version afterwards, as frames of the buffer pool are.
unsafe impl Sync for MetaFrame {}

impl MetaFrame {
    /// Reads the page without a lock: `None` while a change holds the latch, else what `f`
    /// returned over a version no change overlapped. `f` may see a change half made, and then runs
    /// again, so it must not trust what it reads to be consistent, nor panic on it.
    pub fn read<R>(&self, f: impl Fn(&[u8]) -> R) -> Option<R> {
        loop {
            let version = self.latch.optimistic()?;
            // SAFETY: what is read counts only once validated; see above.
            let out = f(unsafe { &*self.bytes.get() });
            if self.latch.validate(version) {
                return Some(out);
            }
        }
    }

    /// Latches the page for a change, or `None` while a change holds it. The change's record is
    /// appended while the latch is held, so changes reach a page in the order of their records.
    pub fn try_lock(&self) -> Option<MetaHold<'_>> {
        Held::try_take(&self.latch).map(|hold| MetaHold { frame: self, hold })
    }

    /// Notes a change, whose record starts at `start`, made under [`lock`](Self::lock).
    pub fn changed(&self, cache: &MetaCache, start: Lsn) {
        let mut state = self.state.lock();
        state.changes += 1;
        if state.rec.is_none() {
            state.rec = Some(start);
            cache.dirty.lock().insert((start, self.id));
        }
    }

    /// Applies a logged change during recovery unless the page already holds it: whether it was
    /// applied, or `None` while a change holds the latch.
    pub fn redo(
        &self,
        cache: &MetaCache,
        start: Lsn,
        lsn: Lsn,
        f: impl FnOnce(&mut [u8]),
    ) -> Option<bool> {
        let mut hold = self.try_lock()?;
        let page = hold.page();
        if page::lsn(page) >= lsn {
            hold.release_unchanged();
            return Some(false);
        }
        f(page);
        page::set_lsn(page, lsn);
        drop(hold);
        self.changed(cache, start);
        Some(true)
    }

    /// Keeps the page in the cache until [`unpin`](Self::unpin).
    pub fn pin(&self) {
        self.state.lock().pins += 1;
    }

    pub fn unpin(&self) {
        let mut state = self.state.lock();
        debug_assert!(state.pins > 0);
        state.pins -= 1;
    }
}

/// A metadata page latched for a change; released, with its version bumped, when dropped.
pub struct MetaHold<'a> {
    frame: &'a MetaFrame,
    hold: Held<&'a VersionedLatch>,
}

impl MetaHold<'_> {
    pub fn page(&mut self) -> &mut [u8] {
        // SAFETY: the latch is held exclusively, by this hold alone.
        unsafe { &mut *self.frame.bytes.get() }
    }

    /// Releases the latch keeping the version, for a hold under which nothing was written.
    pub fn release_unchanged(self) {
        self.hold.release_unchanged();
    }
}

pub struct MetaCache {
    io: Arc<dyn IO>,
    base: String,
    /// Pages held before clean, unpinned ones are dropped.
    budget: usize,
    frames: Mutex<HashMap<MetaId, Arc<MetaFrame>>>,
    /// Pages in the order they came in, for eviction.
    order: Mutex<VecDeque<MetaId>>,
    files: Mutex<HashMap<FileKey, Arc<dyn File>>>,
    /// Dirty pages by where their oldest unwritten change's record starts.
    dirty: Mutex<BTreeSet<(Lsn, MetaId)>>,
    /// While recovering, a page with no whole copy reads as zeros; afterwards it is corruption,
    /// unless it was never written at all.
    recovering: AtomicBool,
}

impl MetaCache {
    pub fn new(io: Arc<dyn IO>, base: &str, budget: usize) -> Self {
        Self {
            io,
            base: base.to_string(),
            budget,
            frames: Mutex::new(HashMap::new()),
            order: Mutex::new(VecDeque::new()),
            files: Mutex::new(HashMap::new()),
            dirty: Mutex::new(BTreeSet::new()),
            recovering: AtomicBool::new(false),
        }
    }

    pub fn set_recovering(&self, recovering: bool) {
        self.recovering.store(recovering, Ordering::Release);
    }

    fn file(&self, key: FileKey) -> Result<Arc<dyn File>> {
        let mut files = self.files.lock();
        if let Some(f) = files.get(&key) {
            return Ok(f.clone());
        }
        let f = self.io.open_file(
            &path(&self.base, key),
            OpenFlags::Create | OpenFlags::Direct,
            false,
        )?;
        // A page written here must survive a crash, so the file's creation must too.
        self.io.sync_directory(&directory(&self.base))?;
        files.insert(key, f.clone());
        Ok(f)
    }

    /// The page `id`, once it is in memory.
    pub fn get(self: &Arc<Self>, id: MetaId) -> Result<IOResult<Arc<MetaFrame>>> {
        let frame = {
            let mut frames = self.frames.lock();
            match frames.get(&id) {
                Some(frame) => frame.clone(),
                None => {
                    let frame = Arc::new(MetaFrame {
                        id,
                        latch: VersionedLatch::default(),
                        bytes: UnsafeCell::new(vec![0u8; META_PAGE].into_boxed_slice()),
                        state: Mutex::new(FrameState {
                            status: Status::Loading,
                            rec: None,
                            changes: 0,
                            writing: None,
                            durable_side: None,
                            pins: 0,
                        }),
                        loaded: Mutex::new(None),
                    });
                    frames.insert(id, frame.clone());
                    self.order.lock().push_back(id);
                    drop(frames);
                    self.load(&frame)?;
                    self.evict_over_budget();
                    frame
                }
            }
        };
        let status = frame.state.lock().status;
        match status {
            Status::Ready => Ok(IOResult::Done(frame)),
            Status::Failed(e) => {
                self.frames.lock().remove(&id);
                Err(LimboError::CompletionError(e))
            }
            Status::Loading => match frame.loaded.lock().clone() {
                Some(c) => Ok(IOResult::IO(IOCompletions(c))),
                None => Ok(IOResult::IO(IOCompletions(Completion::new_yield()))),
            },
        }
    }

    /// The page `id`, once it is in memory, pinned: it stays in the cache, and so receives every
    /// change made to the page, until [`MetaFrame::unpin`].
    pub fn get_pinned(self: &Arc<Self>, id: MetaId) -> Result<IOResult<Arc<MetaFrame>>> {
        loop {
            let frame = match self.get(id)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            // Eviction takes the map's lock, so a frame still in the map while it is held is the
            // one every change goes to.
            let frames = self.frames.lock();
            if frames.get(&id).is_some_and(|f| Arc::ptr_eq(f, &frame)) {
                frame.pin();
                return Ok(IOResult::Done(frame));
            }
        }
    }

    /// Every page in `ids`, once all are in memory.
    pub fn get_all(self: &Arc<Self>, ids: &[MetaId]) -> Result<IOResult<Vec<Arc<MetaFrame>>>> {
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

    /// Reads both places of `frame`'s page and keeps the newest whole copy.
    fn load(self: &Arc<Self>, frame: &Arc<MetaFrame>) -> Result<()> {
        let id = frame.id;
        let file = self.file(id.file_key())?;
        let block = Arc::new(AlignedBlock::zeroed(2 * META_PAGE));
        let loaded = Completion::new_wait();
        *frame.loaded.lock() = Some(loaded.clone());
        let target = frame.clone();
        let recovering = self.recovering.load(Ordering::Acquire);
        let held = block.clone();
        let c = Completion::new_read(block.region(0, 2 * META_PAGE), move |res| {
            let outcome = match res {
                Ok((_, n)) => {
                    // SAFETY: the read filled the block, which nothing else uses.
                    let both = unsafe { std::slice::from_raw_parts(held.as_ptr(), 2 * META_PAGE) };
                    let n = n.max(0) as usize;
                    choose_copy(id, both, n, recovering)
                }
                Err(e) => Err(e),
            };
            // The page before its state, the order a change takes them in. Nothing else holds a
            // page still loading: `get` hands out none.
            if let Ok((Some(bytes), _)) = &outcome {
                let mut hold = target.try_lock().expect("a page loading is latched");
                hold.page().copy_from_slice(bytes);
            }
            let mut state = target.state.lock();
            match outcome {
                Ok((_, side)) => {
                    state.durable_side = side;
                    state.status = Status::Ready;
                }
                Err(e) => state.status = Status::Failed(e),
            }
            drop(state);
            if let Some(c) = target.loaded.lock().take() {
                c.complete(0);
            }
            None
        });
        if let Err(e) = file.pread(id.offset(), c) {
            frame.state.lock().status = Status::Failed(io_failure(&e));
            loaded.complete(0);
            return Err(e);
        }
        Ok(())
    }

    /// Drops clean, unpinned pages while over the budget.
    fn evict_over_budget(&self) {
        // `frames` before `order`, as `get` takes them.
        let mut frames = self.frames.lock();
        let mut order = self.order.lock();
        let mut looked = 0;
        while frames.len() > self.budget && looked < order.len() {
            let Some(id) = order.pop_front() else { break };
            looked += 1;
            let evictable = frames.get(&id).is_some_and(|f| {
                let s = f.state.lock();
                s.status == Status::Ready && s.rec.is_none() && s.writing.is_none() && s.pins == 0
            });
            if evictable {
                frames.remove(&id);
            } else if frames.contains_key(&id) {
                order.push_back(id);
            }
        }
    }

    /// Where the log record of the oldest change on a page not yet on disk starts.
    pub fn oldest_unwritten(&self) -> Option<Lsn> {
        self.dirty.lock().first().map(|&(lsn, _)| lsn)
    }

    /// Writes every page dirty before `before` and waits for it, waiting first for the log to be
    /// durable up to each page's LSN.
    pub fn write_dirty(self: &Arc<Self>, log: &Arc<Log>, before: Lsn) -> Result<IOResult<()>> {
        let ids: Vec<MetaId> = self
            .dirty
            .lock()
            .iter()
            .take_while(|(lsn, _)| *lsn < before)
            .map(|&(_, id)| id)
            .collect();
        let mut wait = None;
        for id in ids {
            let Some(frame) = self.frames.lock().get(&id).cloned() else {
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
    fn write(
        self: &Arc<Self>,
        frame: &Arc<MetaFrame>,
        log: &Arc<Log>,
    ) -> Result<Option<Completion>> {
        let (copy, changes, side) = {
            // Copied without the latch, then checked under the state's mutex: a change is made
            // under the latch and counted in the state after, so one this copy misses either
            // fails the check or is counted after `changes` is read, and the page stays dirty.
            let Some(version) = frame.latch.optimistic() else {
                return Ok(Some(Completion::new_yield()));
            };
            let copy = Arc::new(AlignedBlock::zeroed(META_PAGE));
            // SAFETY: a fresh block of the page's size; what is copied counts only once
            // validated.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    (*frame.bytes.get()).as_ptr(),
                    copy.as_ptr(),
                    META_PAGE,
                )
            };
            let mut state = frame.state.lock();
            if !frame.latch.validate(version) {
                return Ok(Some(Completion::new_yield()));
            }
            if let Some(c) = &state.writing {
                return Ok(Some(c.clone()));
            }
            if state.rec.is_none() {
                return Ok(None);
            }
            // SAFETY: the copy is ours alone.
            let lsn = page::lsn(unsafe { std::slice::from_raw_parts(copy.as_ptr(), META_PAGE) });
            if log.durable() < lsn {
                drop(state);
                return match log.wait_durable(lsn)? {
                    IOResult::Done(()) => self.write(frame, log),
                    IOResult::IO(IOCompletions(c)) => Ok(Some(c)),
                };
            }
            let side = match state.durable_side {
                Some(s) => 1 - s,
                None => 0,
            };
            state.writing = Some(Completion::new_wait());
            (copy, state.changes, side)
        };
        // SAFETY: the copy is ours alone.
        let sealed = unsafe { std::slice::from_raw_parts_mut(copy.as_ptr(), META_PAGE) };
        page::seal(sealed, frame.id.pid());
        let file = self.file(frame.id.file_key())?;
        let at = frame.id.offset() + side as u64 * META_PAGE as u64;
        let target = frame.clone();
        let me = self.clone();
        let durable = file.supports_durable_writes();
        let sync_file = file.clone();
        let done = move |res: std::result::Result<(), CompletionError>| {
            let mut state = target.state.lock();
            let writing = state.writing.take();
            if res.is_ok() {
                state.durable_side = Some(side);
                if state.changes == changes {
                    if let Some(rec) = state.rec.take() {
                        me.dirty.lock().remove(&(rec, target.id));
                    }
                }
            }
            drop(state);
            if let Some(c) = writing {
                match res {
                    Ok(()) => c.complete(0),
                    Err(e) => c.error(e),
                }
            }
        };
        let done = Arc::new(Mutex::new(Some(done)));
        let finish = move |res| {
            if let Some(done) = done.lock().take() {
                done(res);
            }
        };
        let c = Completion::new_write(move |res| {
            let res = match res {
                Ok(n) if n as usize == META_PAGE => Ok(()),
                Ok(_) => Err(CompletionError::ShortWrite),
                Err(e) => Err(e),
            };
            match res {
                Ok(()) if !durable => {
                    // Durable only once synced: until then the other place keeps the newest
                    // durable copy, and the next write still goes here.
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
        let writing = frame.state.lock().writing.clone();
        let submitted = if durable {
            file.pwrite_durable(at, buffer, c)
        } else {
            file.pwrite(at, buffer, c)
        };
        if let Err(e) = submitted {
            let mut state = frame.state.lock();
            if let Some(c) = state.writing.take() {
                c.error(io_failure(&e));
            }
            return Err(e);
        }
        Ok(writing)
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
