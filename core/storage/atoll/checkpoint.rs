//! Checkpoints (audit 18): each writes every page owed an image as of one snapshot, the metadata
//! as of the same cut, and a superblock naming them, so recovery replays the log from the cut on,
//! applying only the commits past the snapshot.
//!
//! Checkpoint `n`:
//!
//! 1. **The cut**, with the store's change gate closed, so every change of several pages lies
//!    wholly before it or wholly after: the log position `L` and snapshot `C`
//!    ([`Transactions::cut`]), the pages owed an image marked pending
//!    ([`BufferPool::begin_cut`]), the metadata cut ([`PageStore::begin_cut`]), `C` held so no
//!    reserve a version it reads needs is given back, and the trees whose creation `C` does not
//!    see set aside.
//! 2. **The images.** Each pending node page is written to a fresh slot as of `C`: a versioned
//!    leaf as `C` reads it ([`AsOf`]), any other as it stood at the cut, its first change since
//!    having kept it. A node of a tree set aside is left out. Then each pending value page is
//!    written if an image written names its value, and left out if none does. The page table
//!    names each copy as of the cut, and as it stands once the copy has landed
//!    ([`PageStore::copied`]). An image equal to the page as it stands settles it: the page is
//!    owed nothing more, and is clean.
//! 3. **The metadata**: a record naming the metadata pages it writes is made durable first
//!    (audit 18, review 2), then each is written as of the cut to the place not holding the copy
//!    in charge.
//! 4. **Durable**: the data files synced and the log made durable to its end, so every commit at
//!    or below `C` is.
//! 5. **The superblock** `(n, C, L, clock, heights, continuations)`, written durably: checkpoint
//!    `n` is in charge.
//! 6. **After**: the frees that waited for `n` are done and the slots they free erased, the trees
//!    it mapped no longer set aside, the log before `L` released, and `C` let go.
//!
//! A checkpoint begins once a share of the log budget has been appended since the last began, or
//! when a full one is asked for.

use super::aligned::AlignedBlock;
use super::arc_cell::{ArcCell, Releasing};
use super::blob;
use super::btree::node::View;
use super::btree::value::{self, ValueRef};
use super::buffer::{BufferPool, CutScan, Relief, Source};
use super::control::{Control, Superblock, MAX_CONTINUATIONS};
use super::log::{framed_len, Admitted, Header, Kind, Log, Lsn};
use super::meta::{MetaId, MetaRef, TREES};
use super::page::{self, PageId, CLASS_COUNT};
use super::store::PageStore;
use super::txn::versioned::AsOf;
use super::txn::{Cut, Transactions, Ts};
use crate::io::Completion;
use crate::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex, Weak};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::{BTreeSet, HashSet, VecDeque};

#[cfg(test)]
crate::thread::thread_local! {
    /// Writing statements' steps that found nothing due, on this thread.
    pub(crate) static STEPS_PASSED_OVER: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The record naming the metadata pages a checkpoint is about to write: its number, then each
/// page's id (audit 18, review 2).
pub const META_INTENT: Kind = Kind::new(31).unwrap();

/// Image writes a checkpoint keeps in flight: the 32 to 128 small writes that keep a device busy
/// (the buffer pool's writeback sizing), at the low end, so a checkpoint never crowds the pool's
/// own writes out.
const WRITES: usize = 32;

/// The default bytes of image writes in flight: one page of the largest class, the least that
/// lets every page be written. [`WRITES`] small writes keep a device busy up to pages of 256 KiB,
/// which fill it; a larger write is itself cut into several of the device's commands, so fewer of
/// them keep it as busy, and the checkpoint's write buffers stay within one such page.
pub const WRITE_BYTES: usize = page::MAX_PAGE_SIZE;

/// What a checkpointer's working memory has held, and its work, since it was made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckpointStats {
    /// Bytes of the page being imaged (its source when read live, and its image) and of the image
    /// writes in flight, at most: within two pages of the largest class and the write bytes.
    pub working_peak: usize,
    /// Bytes of image writes in flight, at most.
    pub flying_peak: usize,
    /// Pages imaged and their bytes.
    pub pages: u64,
    pub bytes: u64,
    /// Nanoseconds spent stepping checkpoints, by whoever stepped them.
    pub stepping_ns: u64,
}

#[derive(Default)]
struct Counters {
    working_peak: AtomicUsize,
    flying_peak: AtomicUsize,
    pages: AtomicU64,
    bytes: AtomicU64,
    stepping_ns: AtomicU64,
}

/// Checkpoints between: the share of the log budget appended since the last began that starts the
/// next, so the log a recovery replays stays within the budget, the cut's own lag included.
const SHARES: u64 = 2;

pub struct Checkpointer {
    pool: Arc<BufferPool>,
    store: Arc<PageStore>,
    log: Arc<Log>,
    control: Control,
    /// How much log recovery should replay at most.
    budget: u64,
    /// Full checkpoints asked for so far.
    full_wanted: AtomicU64,
    /// Where the log ended when the last checkpoint began: what a writing statement reads before
    /// stepping ([`step_due`](Self::step_due)).
    began_at: AtomicU64,
    /// The superblock in charge, as the checkpoint stepping it last wrote it, for anyone to read.
    published: ArcCell<Superblock>,
    releasing: Releasing<Superblock>,
    /// Owned by whoever steps the checkpoints, and only ever tried.
    state: Mutex<State>,
    /// Bytes of image writes kept in flight, at most, beside [`WRITES`] of them.
    write_bytes: usize,
    counters: Counters,
}

/// How far a step goes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Until {
    /// Whatever is due, to the end.
    Done,
    /// Until the room for kept copies takes the request it refused: for the writer refused, who
    /// does only the work that lets it in, its stall that work's and no more.
    Room,
}

/// The pool's relief for a writer refused room: steps the checkpoint until the room takes it.
pub struct CheckpointRelief {
    checkpointer: Weak<Checkpointer>,
    txns: Weak<Transactions>,
}

impl CheckpointRelief {
    pub fn new(checkpointer: &Arc<Checkpointer>, txns: &Arc<Transactions>) -> Self {
        Self {
            checkpointer: Arc::downgrade(checkpointer),
            txns: Arc::downgrade(txns),
        }
    }
}

impl Relief for CheckpointRelief {
    fn relieve(&self) -> Option<Result<IOResult<()>>> {
        let (cp, txns) = (self.checkpointer.upgrade()?, self.txns.upgrade()?);
        cp.relieve(&txns)
    }
}

/// The checkpointer held idle ([`Checkpointer::hold`]): writers waiting for room try again when
/// it is let go.
struct HeldIdle<'a, G> {
    _state: G,
    pool: &'a BufferPool,
}

impl<G> Drop for HeldIdle<'_, G> {
    fn drop(&mut self) {
        self.pool.wake_room();
    }
}

struct State {
    /// The superblock in charge.
    superblock: Superblock,
    run: Option<Box<Run>>,
    /// Full checkpoints done.
    full_done: u64,
    /// Why a checkpoint failed, if one did.
    failed: Option<String>,
}

/// A checkpoint under way.
struct Run {
    n: u64,
    /// The full checkpoint it answers, if one was asked for when it began.
    full: Option<u64>,
    phase: Phase,
    cut: Cut,
    /// The log position replay starts from, the snapshot, and the clock: once cut.
    l: Lsn,
    c: Ts,
    clock: u64,
    heights: [[u8; CLASS_COUNT]; TREES],
    scan: CutScan,
    /// The metadata pages it writes: those dirty at the cut, then every one it edits.
    meta: BTreeSet<MetaId>,
    /// Roots of the trees whose creation the snapshot does not see: left out whole.
    set_aside: HashSet<PageId>,
    /// The pending pages still to image, and the value pages, judged once every node page is.
    queue: VecDeque<PageId>,
    values: Vec<PageId>,
    /// The value pages an image written names.
    named: HashSet<PageId>,
    imaging: Option<Imaging>,
    /// Pages a writer's wait for room had imaged before the queue reached them: passed over.
    early: HashSet<PageId>,
    flying: Vec<Flying>,
    /// Bytes of the image writes in flight.
    flying_bytes: usize,
    /// Pages named by nothing whose free waited for the checkpointer, taken before the cut, and
    /// those freed: left out, never imaged.
    orphans: Vec<PageId>,
    freed: HashSet<PageId>,
    /// The trees it maps: no longer set aside once it is in charge.
    mapped_roots: Vec<PageId>,
    intent: Option<Admitted>,
    intent_end: Option<Lsn>,
    frames: Vec<MetaRef>,
    waits: Vec<Completion>,
    written: Option<Completion>,
    superblock: Option<Superblock>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Gate,
    Cut,
    Orphans,
    Images,
    Values,
    Intent,
    Meta,
    Durable,
    Superblock,
    After,
}

/// The page being imaged.
struct Imaging {
    pid: PageId,
    source: Source,
    as_of: Option<AsOf>,
    /// The image, once built, and the change count of the page it settles, if it does.
    image: Option<(Vec<u8>, Option<u64>)>,
    /// The values its image names, and how many of them have had their pages read.
    refs: Option<(Vec<ValueRef>, usize)>,
}

/// An image write in flight.
struct Flying {
    pid: PageId,
    slot: u64,
    /// Done once the write has ended, either way: what a step waiting on it returns, so whoever
    /// steps the checkpoint waits for the write without taking its failure for their own.
    landed: Completion,
    /// How it ended, a short write a failure too: the checkpoint meets a failure as it lands
    /// its writes.
    failed: Arc<Mutex<Option<CompletionError>>>,
    bytes: usize,
    /// The change count of the page the image settles, if it does: the page as it stood then,
    /// or what reads to every reader as it does.
    settles: Option<u64>,
    /// Its page table edit made once it landed.
    edited: bool,
}

impl Checkpointer {
    /// A checkpointer for a database whose superblock in charge is `superblock`, keeping the log
    /// recovery replays to about `budget` bytes.
    pub fn new(
        pool: Arc<BufferPool>,
        store: Arc<PageStore>,
        log: Arc<Log>,
        control: Control,
        superblock: Superblock,
        budget: u64,
        write_bytes: usize,
    ) -> Self {
        let releasing = Releasing::new(store.epochs().clone());
        let began_at = AtomicU64::new(superblock.redo);
        Self {
            pool,
            store,
            log,
            control,
            budget,
            full_wanted: AtomicU64::new(0),
            began_at,
            published: ArcCell::new(Arc::new(superblock.clone())),
            releasing,
            state: Mutex::new(State {
                superblock,
                run: None,
                full_done: 0,
                failed: None,
            }),
            write_bytes: write_bytes.max(page::MAX_PAGE_SIZE),
            counters: Counters::default(),
        }
    }

    /// What its working memory has held, and its work.
    pub fn stats(&self) -> CheckpointStats {
        let c = &self.counters;
        CheckpointStats {
            working_peak: c.working_peak.load(Ordering::Acquire),
            flying_peak: c.flying_peak.load(Ordering::Acquire),
            pages: c.pages.load(Ordering::Relaxed),
            bytes: c.bytes.load(Ordering::Relaxed),
            stepping_ns: c.stepping_ns.load(Ordering::Relaxed),
        }
    }

    /// Steps the state, timed. Whoever steps it wakes any writer waiting for room once it has
    /// let the state go ([`left`](Self::left)).
    fn stepping(
        &self,
        state: &mut State,
        txns: &Transactions,
        until: Until,
    ) -> Result<IOResult<()>> {
        let clock = self.store.io();
        let began = clock.current_time_monotonic();
        let out = state.step(self, txns, until);
        let took = clock.current_time_monotonic().duration_since(began);
        self.counters.stepping_ns.fetch_add(
            u64::try_from(took.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        out
    }

    /// The state let go: a writer waiting for room tries again, finding the room given back, or
    /// steps the checkpoint on itself.
    fn left(&self) {
        self.pool.wake_room();
    }

    /// For a writer refused room for kept copies, holding nothing: steps the checkpoint under way
    /// until the room takes the request it refused, its images giving copies back, kept copies
    /// first. `None` while someone else steps it. A
    /// checkpoint that fails meanwhile is abandoned, its cut ended and its copies dropped, and
    /// tells its failure to whoever steps checkpoints next: the writer goes on, as commits do.
    pub fn relieve(&self, txns: &Transactions) -> Option<Result<IOResult<()>>> {
        let mut state = self.state.try_lock()?;
        let out = self.stepping(&mut state, txns, Until::Room);
        drop(state);
        self.left();
        Some(match out {
            Err(e) => {
                tracing::warn!("a checkpoint a writer stepped for room failed: {e}");
                Ok(IOResult::Done(()))
            }
            stepped => stepped,
        })
    }

    /// The log, in bytes, the checkpointer keeps a recovery to replaying.
    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// How far the redo point in charge trails the log's end, in bytes.
    pub fn lag(&self) -> u64 {
        self.log.end().saturating_sub(self.superblock().redo)
    }

    /// The superblock in charge.
    pub fn superblock(&self) -> Superblock {
        let step = self.releasing.epochs().enter();
        (*self.published.get(&step).expect("a superblock in charge")).clone()
    }

    /// Does whatever is due: a checkpoint once its share of the log has been appended, going on
    /// with one under way. Done once nothing more is due now, or while someone else steps it.
    pub fn step(&self, txns: &Transactions) -> Result<IOResult<()>> {
        let Some(mut state) = self.state.try_lock() else {
            return Ok(IOResult::Done(()));
        };
        let out = self.stepping(&mut state, txns, Until::Done);
        drop(state);
        self.left();
        out
    }

    /// A writing statement's step (DESIGN.md §17): only once a checkpoint is due by the log
    /// appended since the last began, or one is under way.
    pub fn step_due(&self, txns: &Transactions) -> Result<IOResult<()>> {
        let due = self
            .log
            .end()
            .saturating_sub(self.began_at.load(Ordering::Acquire))
            >= self.budget / SHARES;
        if !due {
            #[cfg(test)]
            STEPS_PASSED_OVER.with(|n| n.set(n.get() + 1));
            return Ok(IOResult::Done(()));
        }
        self.step(txns)
    }

    /// Holds the checkpointer idle, if no one is stepping it: no checkpoint runs until the hold is
    /// dropped, so the page store holds still for an integrity check (DESIGN.md §17, §12; R36).
    pub fn hold(&self) -> Option<impl Sized + '_> {
        self.state.try_lock().map(|state| HeldIdle {
            _state: state,
            pool: &self.pool,
        })
    }

    /// The checkpoint under way whose cut is taken, if any: what it writes is as of that cut.
    #[cfg(test)]
    pub(crate) fn cut_taken(&self) -> Option<u64> {
        let state = self.state.try_lock()?;
        let run = state.run.as_ref()?;
        (!matches!(run.phase, Phase::Gate | Phase::Cut)).then_some(run.n)
    }

    /// Asks for a full checkpoint: one begun after the ask, done. Step it with
    /// [`step_full`](Self::step_full).
    pub fn request_full(&self) -> u64 {
        self.full_wanted.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Done once the full checkpoint `ticket` names is done.
    pub fn step_full(&self, ticket: u64, txns: &Transactions) -> Result<IOResult<()>> {
        let Some(mut state) = self.state.try_lock() else {
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        };
        let stepped =
            (state.full_done < ticket).then(|| self.stepping(&mut state, txns, Until::Done));
        let done = state.full_done >= ticket;
        drop(state);
        self.left();
        if let Some(IOResult::IO(io)) = stepped.transpose()? {
            return Ok(IOResult::IO(io));
        }
        Ok(if done {
            IOResult::Done(())
        } else {
            IOResult::IO(IOCompletions(Completion::new_yield()))
        })
    }
}

impl State {
    fn step(
        &mut self,
        cp: &Checkpointer,
        txns: &Transactions,
        until: Until,
    ) -> Result<IOResult<()>> {
        if let Some(failed) = &self.failed {
            return Err(LimboError::InternalError(format!(
                "a checkpoint failed ({failed}): none runs until the database is opened again"
            )));
        }
        if until == Until::Room && self.run.is_none() {
            return Ok(IOResult::Done(()));
        }
        if self.run.is_none() {
            let wanted = cp.full_wanted.load(Ordering::Acquire);
            let due = cp
                .log
                .end()
                .saturating_sub(cp.began_at.load(Ordering::Acquire))
                >= cp.budget / SHARES;
            if wanted <= self.full_done && !due {
                return cp.store.erase_freed();
            }
            cp.began_at.store(cp.log.end(), Ordering::Release);
            self.run = Some(Box::new(Run::new(
                self.superblock.checkpoint + 1,
                (wanted > self.full_done).then_some(wanted),
            )));
        }
        let run = self.run.as_mut().expect("a checkpoint under way");
        let out = run.step(cp, txns, &self.superblock, until);
        match out {
            Ok(IOResult::Done(())) if run.superblock.is_none() => Ok(IOResult::Done(())),
            Ok(IOResult::Done(())) => {
                let run = self.run.take().expect("a checkpoint under way");
                self.superblock = run.superblock.expect("the checkpoint's superblock");
                if let Some(full) = run.full {
                    self.full_done = self.full_done.max(full);
                }
                Ok(IOResult::Done(()))
            }
            Ok(IOResult::IO(io)) => Ok(IOResult::IO(io)),
            Err(e) => {
                // A checkpoint that cannot finish leaves the one in charge in charge: the pages
                // it wrote are named by no superblock. What it marked imaged and edited as it
                // stands is not undone, so no checkpoint runs again until the database is opened
                // again and recovers from the one in charge.
                if let Some(run) = self.run.take() {
                    run.abandon(cp, txns);
                }
                self.failed = Some(e.to_string());
                Err(e)
            }
        }
    }
}

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

impl Run {
    fn new(n: u64, full: Option<u64>) -> Self {
        Self {
            n,
            full,
            phase: Phase::Gate,
            cut: Cut::default(),
            l: 0,
            c: 0,
            clock: 0,
            heights: [[0; CLASS_COUNT]; TREES],
            scan: CutScan::default(),
            meta: BTreeSet::new(),
            set_aside: HashSet::new(),
            queue: VecDeque::new(),
            values: Vec::new(),
            named: HashSet::new(),
            imaging: None,
            early: HashSet::new(),
            flying: Vec::new(),
            flying_bytes: 0,
            orphans: Vec::new(),
            freed: HashSet::new(),
            mapped_roots: Vec::new(),
            intent: None,
            intent_end: None,
            frames: Vec::new(),
            waits: Vec::new(),
            written: None,
            superblock: None,
        }
    }

    /// Lets go of what a checkpoint that cannot finish holds: the gate, the cuts, the snapshot,
    /// and the pages handed over to it.
    fn abandon(self, cp: &Checkpointer, txns: &Transactions) {
        if self.phase == Phase::Gate || self.phase == Phase::Cut {
            cp.store.open_gate();
        }
        if cp.pool.cutting() == self.n {
            cp.pool.end_cut(self.n);
        }
        if cp.store.meta().cutting() == self.n {
            cp.store.meta().end_cut(self.n, &self.frames);
        }
        for pid in self.orphans {
            cp.store.orphaned(pid);
        }
        txns.release_checkpoint();
    }

    fn step(
        &mut self,
        cp: &Checkpointer,
        txns: &Transactions,
        in_charge: &Superblock,
        until: Until,
    ) -> Result<IOResult<()>> {
        loop {
            if until == Until::Room && cp.pool.room_takes_refused() {
                return Ok(IOResult::Done(()));
            }
            match self.phase {
                Phase::Gate => {
                    if let IOResult::IO(io) = cp.store.close_gate() {
                        return Ok(IOResult::IO(io));
                    }
                    // Before the cut: a page handed over once it is in effect, a value's page an
                    // image may name, waits for the next checkpoint.
                    self.orphans = cp.store.take_orphans();
                    self.phase = Phase::Cut;
                }
                Phase::Cut => {
                    let (l, c) = done!(txns.cut(&mut self.cut));
                    self.l = l;
                    self.c = c;
                    self.clock = txns.clock_now();
                    txns.hold_checkpoint(c);
                    if let IOResult::IO(io) = cp.pool.begin_cut(self.n, &mut self.scan) {
                        return Ok(IOResult::IO(io));
                    }
                    let (dirty, heights) = cp.store.begin_cut(self.n);
                    cp.store.open_gate();
                    self.heights = heights;
                    self.meta.extend(dirty);
                    for (root, slot, start) in cp.store.creations() {
                        if txns.committed_by(slot, start, c) {
                            self.mapped_roots.push(root);
                        } else {
                            self.set_aside.insert(root);
                        }
                    }
                    self.queue = std::mem::take(&mut self.scan.owed).into_iter().collect();
                    self.phase = Phase::Orphans;
                }
                Phase::Orphans => {
                    while let Some(&pid) = self.orphans.last() {
                        // Named by nothing: free as of the cut, and freed as it stands. Marked
                        // left out only once its edit is made, so a yield in the edit makes it
                        // again.
                        if !self.freed.contains(&pid) {
                            let edited = done!(cp.store.left_out(self.n, &self.heights, pid));
                            self.meta.extend(edited);
                            self.freed.insert(pid);
                        }
                        done!(cp.pool.free_orphan(pid));
                        self.orphans.pop();
                    }
                    self.phase = Phase::Images;
                }
                Phase::Images => {
                    if done!(self.image_pages(cp, txns, false, until)) {
                        debug_assert!(self.early.is_empty(), "a page imaged early still queued");
                        self.queue = std::mem::take(&mut self.values).into_iter().collect();
                        self.phase = Phase::Values;
                    }
                }
                Phase::Values => {
                    if done!(self.image_pages(cp, txns, true, until)) {
                        cp.pool.end_cut(self.n);
                        self.phase = Phase::Intent;
                    }
                }
                Phase::Intent => {
                    if self.intent_end.is_none() {
                        let payload = self.intent_payload();
                        let admitted = match &mut self.intent {
                            Some(a) => a,
                            None => self
                                .intent
                                .insert(done!(cp.log.admit(framed_len(payload.len())))),
                        };
                        let header = Header {
                            kind: META_INTENT,
                            flags: 0,
                            txn: 0,
                            prev: None,
                        };
                        let end = cp.log.append(admitted, &header, &[&payload]);
                        self.intent = None;
                        self.intent_end = Some(end);
                    }
                    done!(cp.log.wait_durable(self.intent_end.expect("appended")));
                    self.phase = Phase::Meta;
                }
                Phase::Meta => {
                    if self.frames.is_empty() && !self.meta.is_empty() {
                        let ids: Vec<MetaId> = self.meta.iter().copied().collect();
                        self.frames = done!(cp.store.meta().get_all(&ids));
                        for frame in &self.frames {
                            if let Some(c) = cp.store.meta().write_for(frame, self.n)? {
                                self.waits.push(c);
                            }
                        }
                    }
                    done!(wait_all(&mut self.waits));
                    cp.store.meta().end_cut(self.n, &self.frames);
                    self.waits = cp.store.sync_data_files()?;
                    self.phase = Phase::Durable;
                }
                Phase::Durable => {
                    done!(wait_all(&mut self.waits));
                    done!(cp.log.wait_durable(cp.log.end()));
                    self.phase = Phase::Superblock;
                }
                Phase::Superblock => {
                    let sb = self.superblock.get_or_insert_with(|| {
                        let mut continuations: Vec<(Lsn, Lsn)> = in_charge
                            .continuations
                            .iter()
                            .copied()
                            .filter(|&(ended, _)| ended >= self.l)
                            .collect();
                        continuations.truncate(MAX_CONTINUATIONS);
                        Superblock {
                            seq: in_charge.seq + 1,
                            checkpoint: self.n,
                            redo: self.l,
                            snapshot: self.c,
                            clock: self.clock,
                            heights: self.heights,
                            continuations,
                            ..in_charge.clone()
                        }
                    });
                    done!(cp.control.step_write(sb, &mut self.written));
                    cp.store.meta().checkpointed(self.n, &self.frames);
                    cp.published
                        .replace(Some(Arc::new(sb.clone())), &cp.releasing);
                    self.phase = Phase::After;
                }
                Phase::After => {
                    done!(cp.store.after(self.n));
                    for root in self.mapped_roots.drain(..) {
                        cp.store.forget_creation(root);
                    }
                    cp.log.release_before(self.l)?;
                    txns.release_checkpoint();
                    return cp.store.erase_freed();
                }
            }
        }
    }

    /// The intent record's payload: the checkpoint's number, then each metadata page's id.
    fn intent_payload(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 8 * self.meta.len());
        out.extend_from_slice(&self.n.to_le_bytes());
        for id in &self.meta {
            out.extend_from_slice(&id.pid().raw().to_le_bytes());
        }
        out
    }

    /// Images the queue's pages: node pages, the value pages put aside for later, or, with
    /// `values`, value pages, written only where an image names them. Done once every write has
    /// landed and been named in the page table.
    fn image_pages(
        &mut self,
        cp: &Checkpointer,
        txns: &Transactions,
        values: bool,
        until: Until,
    ) -> Result<IOResult<bool>> {
        loop {
            done!(self.land(cp));
            if until == Until::Room && self.imaging.is_none() && cp.pool.room_takes_refused() {
                return Ok(IOResult::Done(false));
            }
            if self.imaging.is_none() {
                if self.flying.len() >= WRITES {
                    let c = self.flying[0].landed.clone();
                    return Ok(IOResult::IO(IOCompletions(c)));
                }
                // While a writer waits for room, a kept copy first: imaging it gives its room
                // back. Only node pages are kept, so only among the node pages.
                let early = match !values && cp.pool.room_pressed() {
                    true => cp.pool.take_kept(),
                    false => None,
                };
                let (pid, kept) = match early {
                    Some((pid, copy)) => {
                        self.early.insert(pid);
                        (pid, Some(copy))
                    }
                    None => {
                        let Some(pid) = self.queue.pop_front() else {
                            if self.flying.is_empty() {
                                return Ok(IOResult::Done(true));
                            }
                            let c = self.flying[0].landed.clone();
                            return Ok(IOResult::IO(IOCompletions(c)));
                        };
                        if self.early.remove(&pid) {
                            continue;
                        }
                        (pid, None)
                    }
                };
                if self.freed.contains(&pid) {
                    // An orphan, left out already.
                    continue;
                }
                if values && !self.named.contains(&pid) {
                    // No image written names it: a value no committed version at or below the
                    // snapshot reaches.
                    match cp.store.left_out(self.n, &self.heights, pid)? {
                        IOResult::Done(edited) => self.meta.extend(edited),
                        IOResult::IO(io) => {
                            self.queue.push_front(pid);
                            return Ok(IOResult::IO(io));
                        }
                    }
                    continue;
                }
                let source = match kept {
                    Some(copy) => Source::Kept(copy),
                    None => match cp.pool.image_source(self.n, pid)? {
                        IOResult::Done(s) => s,
                        IOResult::IO(io) => {
                            self.queue.push_front(pid);
                            return Ok(IOResult::IO(io));
                        }
                    },
                };
                self.imaging = Some(Imaging {
                    pid,
                    source,
                    as_of: None,
                    image: None,
                    refs: None,
                });
            }
            let imaging = self.imaging.as_mut().expect("a page being imaged");
            let pid = imaging.pid;
            let bytes: &[u8] = match &imaging.source {
                Source::Kept(b) => b,
                Source::Live { bytes, .. } => bytes,
            };
            let live = match &imaging.source {
                Source::Live { count, .. } => Some(*count),
                Source::Kept(_) => None,
            };
            if imaging.image.is_none() {
                // Every page owed an image is a node or a value's: its writer sets its kind
                // before the change that makes it owed.
                if page::kind(bytes) == blob::KIND {
                    if values {
                        imaging.image = Some((bytes.to_vec(), live));
                    } else {
                        // Judged once every node page has been.
                        self.values.push(pid);
                        self.imaging = None;
                        continue;
                    }
                } else {
                    let v = View::new(bytes);
                    if !v.is_node() {
                        return Err(LimboError::Corrupt(format!(
                            "page {pid:?}, owed an image, is neither a node nor a value's"
                        )));
                    }
                    if self.set_aside.contains(&v.root()) {
                        // A tree whose creation the snapshot does not see: left out whole.
                        let edited = done!(cp.store.left_out(self.n, &self.heights, pid));
                        self.meta.extend(edited);
                        self.imaging = None;
                        continue;
                    }
                    if v.is_leaf() && v.reserving() {
                        let as_of = match &mut imaging.as_of {
                            Some(a) => a,
                            None => imaging.as_of.insert(AsOf::new(bytes, self.c)?),
                        };
                        done!(as_of.step(txns, &cp.pool));
                        let as_of = imaging.as_of.take().expect("judged");
                        let adoptable = as_of.adoptable();
                        let image = as_of.image()?;
                        let settles = live.filter(|_| adoptable || image.as_slice() == bytes);
                        imaging.image = Some((image, settles));
                    } else {
                        imaging.image = Some((bytes.to_vec(), live));
                    }
                }
            }
            // The values the image names: their pages are mapped with it.
            let (image, _) = imaging.image.as_ref().expect("built");
            let v = View::new(image.as_slice());
            if v.is_node() && v.is_leaf() {
                let refs = match &mut imaging.refs {
                    Some(r) => r,
                    None => {
                        let entries = v.entries().ok_or_else(|| corrupt_image(pid))?;
                        let head = image_head(&v);
                        let refs = entries
                            .iter()
                            .filter(|e| e.out_of_line)
                            .map(|e| {
                                e.value
                                    .get(head..)
                                    .and_then(ValueRef::decode)
                                    .ok_or_else(|| corrupt_image(pid))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        imaging.refs.insert((refs, 0))
                    }
                };
                while refs.1 < refs.0.len() {
                    let pages = match done!(value::pages(&cp.pool, refs.0[refs.1], &|| true)) {
                        Ok(pages) => pages,
                        Err(_) => {
                            return Err(LimboError::Corrupt(format!(
                                "a value an image of {pid:?} names lost its pages"
                            )))
                        }
                    };
                    self.named.extend(pages);
                    refs.1 += 1;
                }
            }
            // Its write buffer joins the writes in flight only within their bytes, one always
            // going: a large page's writes go fewer at once.
            let len = self
                .imaging
                .as_ref()
                .and_then(|i| i.image.as_ref())
                .map_or(0, |i| i.0.len());
            if !self.flying.is_empty() && self.flying_bytes + len > cp.write_bytes {
                let c = self.flying[0].landed.clone();
                return Ok(IOResult::IO(IOCompletions(c)));
            }
            let imaging = self.imaging.take().expect("a page being imaged");
            let source_bytes = match &imaging.source {
                Source::Live { bytes, .. } => bytes.len(),
                // Charged to the room for kept copies, not to the checkpoint.
                Source::Kept(_) => 0,
            };
            let (mut image, settles) = imaging.image.expect("built");
            let slot = match cp.store.take_copy_slot(pid.class())? {
                IOResult::Done(s) => s,
                IOResult::IO(io) => {
                    // The image stays built: the page is imaged again from its source.
                    self.imaging = Some(Imaging {
                        image: Some((image, settles)),
                        ..imaging
                    });
                    return Ok(IOResult::IO(io));
                }
            };
            page::seal(&mut image[..], pid);
            let block = Arc::new(AlignedBlock::zeroed(image.len()));
            let buffer = block.region(0, image.len());
            buffer.as_mut_slice().copy_from_slice(&image);
            // The most it holds: the source, the image and every write buffer, this one's too.
            self.flying_bytes += image.len();
            let counters = &cp.counters;
            counters.working_peak.fetch_max(
                source_bytes + image.len() + self.flying_bytes,
                Ordering::AcqRel,
            );
            counters
                .flying_peak
                .fetch_max(self.flying_bytes, Ordering::AcqRel);
            counters.pages.fetch_add(1, Ordering::Relaxed);
            counters
                .bytes
                .fetch_add(image.len() as u64, Ordering::Relaxed);
            let landed = Completion::new_wait();
            let failed = Arc::new(Mutex::new(None));
            let (ends, fails) = (landed.clone(), failed.clone());
            // Its end is told through `landed`: the write's own completion stays the IO's.
            drop(cp.store.write_slot(pid.class(), slot, buffer, move |res| {
                *fails.lock() = res.err();
                ends.complete(0);
            })?);
            self.flying.push(Flying {
                pid,
                slot,
                landed,
                failed,
                bytes: image.len(),
                settles,
                edited: false,
            });
        }
    }

    /// Names in the page table every image write that has landed; fails on one that failed.
    fn land(&mut self, cp: &Checkpointer) -> Result<IOResult<()>> {
        let mut i = 0;
        while i < self.flying.len() {
            let f = &mut self.flying[i];
            if !f.landed.finished() {
                i += 1;
                continue;
            }
            if let Some(e) = *f.failed.lock() {
                return Err(LimboError::CompletionError(e));
            }
            if !f.edited {
                let edited = done!(cp.store.copied(self.n, &self.heights, f.pid, f.slot));
                self.meta.extend(edited);
                f.edited = true;
                if let Some(count) = f.settles {
                    cp.pool.settled(f.pid, count);
                }
            }
            self.flying_bytes -= f.bytes;
            self.flying.swap_remove(i);
        }
        Ok(IOResult::Done(()))
    }
}

/// The bytes before a value reference in an entry out of line: a versioned tree's head.
fn image_head<P: super::shared::Bytes + ?Sized>(v: &View<'_, P>) -> usize {
    if v.reserving() {
        super::txn::versioned::HEADER
    } else {
        0
    }
}

fn corrupt_image(pid: PageId) -> LimboError {
    LimboError::Corrupt(format!("the image of {pid:?} does not decode"))
}

/// Done once every one of `waits` has finished; the first error any ended with.
fn wait_all(waits: &mut Vec<Completion>) -> Result<IOResult<()>> {
    if let Some(c) = waits.iter().find(|c| !c.finished()) {
        return Ok(IOResult::IO(IOCompletions(c.clone())));
    }
    if let Some(e) = waits.iter().find_map(|c| c.get_error()) {
        waits.clear();
        return Err(LimboError::CompletionError(e));
    }
    waits.clear();
    Ok(IOResult::Done(()))
}
