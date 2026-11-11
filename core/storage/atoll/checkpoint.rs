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
use super::btree::node::{self, View};
use super::btree::value::{self, ValueRef};
use super::buffer::{BufferPool, CutScan, EndCut, Relief, Relieved, Retry, Source};
use super::control::{Control, Superblock, MAX_CONTINUATIONS};
use super::executor::{self, Class, Cx, Signal, Step, Task, TaskRef, Watched};
use super::guarded::{Contender, Guarded, Hold};
use super::log::{framed_len, Admitted, Header, Kind, Log, Lsn};
use super::meta::{MetaId, MetaRef, TREES};
use super::page::{self, PageId, CLASS_COUNT};
use super::stack::Chain;
use super::store::PageStore;
use super::txn::versioned::AsOf;
use super::txn::{Cut, Transactions, Ts};
use super::unit;
use crate::io::Completion;
use crate::storage::atoll::locks::Mutex;
use crate::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use crate::sync::{Arc, Weak};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::ops::Bound::{Excluded, Unbounded};
use std::ops::{Deref, DerefMut};

#[cfg(test)]
crate::thread::thread_local! {
    /// Writing statements' steps that found nothing due, on this thread.
    pub(crate) static STEPS_PASSED_OVER: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Steps of the checkpoint state taken on this thread, for tests to tell a thread that did
    /// checkpoint work: a writer's, or the worker stepping the checkpoint's task.
    pub(crate) static WRITER_STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Claims of a checkpoint's task that stepped it, made on this thread.
    #[cfg(test)]
    pub(crate) static CLAIMS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Triggers of a checkpoint's task notified on this thread.
    #[cfg(test)]
    pub(crate) static TRIGGERS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The record naming the metadata pages a checkpoint is about to write: its number, then each
/// page's id (audit 18, review 2).
pub const META_INTENT: Kind = Kind::new(31).unwrap();

/// One past the start of the latest checkpoint a trigger was notified for (`began_at` as the
/// notifying statement read it, plus one), raised and never lowered: one trigger a crossing of the
/// threshold, the statements after it reading this word alone.
#[derive(Default)]
struct TriggerWord(AtomicU64);

impl TriggerWord {
    /// Whether this claim, for the crossing since `began`, is the first to raise the word past
    /// `began`. A claim after it reads the word alone, and one that read an earlier start never
    /// lowers it, whatever claims run beside it: the raise is a maximum, never a store.
    fn claim(&self, began: Lsn) -> bool {
        self.0.load(Ordering::Acquire) <= began
            && self.0.fetch_max(began + 1, Ordering::AcqRel) <= began
    }
}

/// Image writes a checkpoint keeps in flight: the 32 to 128 small writes that keep a device busy
/// (the buffer pool's writeback sizing), at the low end, so a checkpoint never crowds the pool's
/// own writes out.
pub(crate) const WRITES: usize = 32;

/// [`WRITES`], for the image bank's size (DESIGN.md 9.1.1).
pub(crate) const IMAGE_WRITES: usize = WRITES;

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
    /// Image writes holding a place in the flight set, at most.
    pub flying_writes_peak: usize,
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
    flying_writes_peak: AtomicUsize,
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
    /// Notified by a writing statement that finds a checkpoint due (DESIGN.md 22.9, Due): it
    /// sets the task's own level to due as it wakes it, and the task reads it before it looks.
    due: Arc<Signal>,
    /// Whether a checkpoint is under way, set and cleared under the state: the writers stepping
    /// checkpoints step one under way whether or not another is due.
    under_way: AtomicBool,
    /// Which crossing of the threshold a trigger was last notified for (the auditor's P2-EX-H1).
    triggered_for: TriggerWord,
    /// The superblock in charge, as the checkpoint stepping it last wrote it, for anyone to read.
    published: ArcCell<Superblock>,
    releasing: Releasing<Superblock>,
    /// Owned by whoever steps the checkpoints, and only ever tried: the checkpointing thread and an
    /// integrity check wait for it as contenders, woken once it is let go.
    state: Guarded<State>,
    /// Bytes of image writes kept in flight, at most, beside [`WRITES`] of them.
    write_bytes: usize,
    counters: Counters,
    /// The checkpoint's task, while a worker of the database's executor steps it
    /// ([`Checkpointing::Thread`]): writers then only wake it. Unset, or once the task has ended,
    /// writers step checkpoints.
    task: std::sync::OnceLock<TaskRef>,
    /// Whether a worker steps the checkpoint's task, read by every writer's step without a lock;
    /// with a task and no worker, writers claim it and step it themselves.
    driven: AtomicBool,
    /// Full checkpoints done, and whether one failed, as the state last recorded them, for a
    /// transaction's end waiting on the thread without taking the state.
    full_done: AtomicU64,
    failure: Mutex<Option<String>>,
    /// The ends of transactions waiting on the thread for a full checkpoint, woken as each
    /// checkpoint ends, fails, or the thread does.
    waiters: Mutex<Vec<Completion>>,
    /// What a caller that found the state held waits on (`step_full`): finished as the state is
    /// let go (DESIGN.md 9.1.3, the checkpointer's state held by another stepper). Any number
    /// share it, which the state's two parked contenders (`Guarded`) cannot.
    released: crate::storage::atoll::notify::LetGo,
}

crate::thread::thread_local! {
    /// The checkpointer whose task this thread is stepping, by address, or 0.
    static STEPPING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// The claim of a checkpoint's task this thread's writer is making, if any.
    static CLAIMING: std::cell::RefCell<Claiming> = const { std::cell::RefCell::new(Claiming::No) };
}

/// A writer's claim of the checkpoint's task on this thread: made, then told by the step it runs.
enum Claiming {
    No,
    Made,
    Told(Claimed),
}

/// What a writer's claim of the checkpoint's task came to (DESIGN.md 22.7): the step it ran on its
/// own thread tells it, so the writer meets what that step met, as a writer stepping the state
/// does, and no other claim's step can stand in for it.
enum Claimed {
    /// No step: another writer's step holds the task, or the task is over.
    Refused,
    /// The step found the state held by another (an integrity check) and contends for it.
    Held,
    /// What the step's unit came to: more ready, its wait, nothing more due, or the failure it
    /// met.
    Stepped(Result<Unit>),
}

/// Tells a writer whose claim runs the step on this thread what the step came to; a step on a
/// worker has no claimant to tell.
fn tell_claimant(claimed: Claimed) {
    CLAIMING.with(|c| {
        let mut c = c.borrow_mut();
        if matches!(*c, Claiming::Made) {
            *c = Claiming::Told(claimed);
        }
    });
}

/// This thread steps `checkpointer`'s task until dropped.
struct Stepping(usize);

impl Stepping {
    fn enter(checkpointer: &Checkpointer) -> Self {
        Self(STEPPING.with(|s| s.replace(std::ptr::from_ref(checkpointer) as usize)))
    }
}

impl Drop for Stepping {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        STEPPING.with(|s| s.set(self.0));
    }
}

/// Who steps the checkpoint for a writer: the worker, which the writer only wakes; the writer
/// itself, claiming the checkpoint's task; or, with no task (recovery, before the database is
/// handed out) or a task that failed, the writer stepping the state.
enum Stepper<'a> {
    Worker,
    Caller(&'a TaskRef),
    Writer,
}

/// The checkpoint as its database's executor task (DESIGN.md 22.9 item 1, 22.16 step 7): a step
/// steps the state as far as it goes before it waits on writes or finds nothing more due, holding
/// it, or contends for it while an integrity check holds it. With nothing due it rests until a
/// writer finding a checkpoint due or a transaction ending, a full checkpoint asked for, a writer
/// wanting room or the state let go wakes it, as each woke the thread it replaces; with no worker,
/// the writers claim its steps themselves. A failure, told to every waiter by the state and to the
/// writer whose claim met it, ends it: no checkpoint runs until the database is opened again, and
/// writers go on, as they did with a thread of the database's own.
pub(crate) struct CheckpointTask {
    cp: Arc<Checkpointer>,
    txns: Arc<Transactions>,
}

impl CheckpointTask {
    pub(crate) fn new(cp: Arc<Checkpointer>, txns: Arc<Transactions>) -> Self {
        Self { cp, txns }
    }
}

impl Task for CheckpointTask {
    fn step(&mut self, cx: &Cx) -> Result<Step> {
        let cp = &*self.cp;
        // Read before the look: a trigger after it moves the count, keeping the task's level.
        let read = cp.due.read();
        let held = cp
            .state
            .hold_or_contend(&Contender::Task(cx.task()))
            .map(|hold| StateHold {
                hold: Some(hold),
                checkpointer: cp,
                waiters_owed: false,
                task: true,
            });
        let Some(mut state) = held else {
            // Its contender: the state let go wakes it.
            tell_claimant(Claimed::Held);
            return Ok(Step::Idle(Watched::of(&[])));
        };
        let out = {
            let _stepping = Stepping::enter(cp);
            cp.stepping(&mut state, &self.txns, Until::Done)
        };
        drop(state);
        crate::race::point("checkpoint.let_go");
        let step = match &out {
            Ok(Unit::Again) => Step::Again,
            Ok(Unit::Wait(c)) if c.finished() => Step::Again,
            Ok(Unit::Wait(c)) => Step::Wait(c.clone()),
            // A checkpoint ended, or none was due: the task looks itself before it rests, so a
            // database due when the task was made, or due again as a checkpoint ends, or a full
            // checkpoint asked for meanwhile, needs no further wake (DESIGN.md 22.9, Due).
            Ok(Unit::Done) if cp.due_now() || cp.full_pending() => Step::Again,
            // Nothing due: its own level drops, unless a trigger came since the read, which
            // keeps it and, the count moved, wakes it again.
            Ok(Unit::Done) => {
                cx.clear_due(&cp.due, read);
                Step::Idle(Watched::of(&[(&cp.due, read)]))
            }
            Err(_) => Step::Done,
        };
        tell_claimant(Claimed::Stepped(out));
        Ok(step)
    }

    /// The database closing (22.12 step 3): a checkpoint under way is abandoned as a failed one
    /// is, its gate, cut and protection let go, and writers would step checkpoints again.
    fn abandon(&mut self) {
        let cp = &*self.cp;
        if let Some(mut state) = cp.try_hold() {
            if let Some(run) = state.run.take() {
                run.abandon(cp, &self.txns);
                state.upkeep.abandon();
            }
            cp.under_way.store(false, Ordering::Release);
        }
        cp.ended();
    }
}

/// Who steps the checkpoints (Clay's ruling of 2026-11-05: off the commit path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checkpointing {
    /// A worker of the database's executor stepping the checkpoint's task, which keeps the
    /// checkpoint's writes going as fast as the device takes them. A writing statement and a
    /// transaction's end only wake it; a writer refused room for kept copies waits for the room it
    /// frees; and past twice the log budget a transaction's end waits for its full checkpoint,
    /// doing none of it.
    Thread,
    /// The writers, each statement and transaction's end, a writer refused room and a wait for a
    /// full checkpoint claiming the checkpoint's task and stepping it, one step a call (DESIGN.md
    /// 22.7): for a platform without threads, or a test that steps checkpoints itself.
    Writers,
}

impl Checkpointing {
    /// A thread where the platform has threads.
    pub const fn default_for_platform() -> Self {
        if cfg!(any(target_family = "wasm", shuttle, atoll_loom)) {
            Self::Writers
        } else {
            Self::Thread
        }
    }
}

/// How far a step goes.
#[derive(Clone, Copy)]
enum Until<'a> {
    /// Whatever is due, to the end.
    Done,
    /// Until the writer refused may start again ([`BufferPool::may_retry`]): the room takes the
    /// request it refused, or a frame of a class it wants came. It does only the work that lets
    /// it in, its stall that work's and no more.
    Room(Retry<'a>),
}

impl Until<'_> {
    /// Whether a refused writer's relief stops here.
    fn met(self, pool: &BufferPool) -> bool {
        matches!(self, Until::Room(retry) if pool.may_retry(retry))
    }
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
    fn relieve(&self, retry: Retry<'_>) -> Relieved {
        match (self.checkpointer.upgrade(), self.txns.upgrade()) {
            (Some(cp), Some(txns)) => cp.relieve(&txns, retry),
            // The database is closing: no checkpoint to step.
            _ => Relieved::Elsewhere,
        }
    }

    fn demand(&self) -> Option<executor::Ticket> {
        self.checkpointer.upgrade()?.demand()
    }
}

/// The checkpoint's state, held ([`Checkpointer::try_hold`]): every holder takes it so, and steps
/// it only so ([`Checkpointer::stepping`]). Letting it go wakes its contenders ([`Hold`]), then the
/// transactions' ends waiting for a full checkpoint, if a step under it moved what they wait for,
/// then, where the writers step checkpoints themselves, the writers waiting on it: a writer refused
/// room or the gate whose relief found the state held steps the checkpoint itself once it is let
/// go. Each is woken with the state let go, so a waker that takes it finds it free. A worker
/// stepping the task leaves its writers waiting on the room it frees and the gate it opens, and a
/// writer's claim of the task wakes them itself once the step it claimed has ended.
struct StateHold<'a> {
    hold: Option<Hold<'a, State>>,
    checkpointer: &'a Checkpointer,
    /// A step under this hold moved a full checkpoint's count or failed: its waiters are owed a
    /// wake.
    waiters_owed: bool,
    /// Held by the checkpoint's own task, whose writers this let-go does not wake.
    task: bool,
}

impl Deref for StateHold<'_> {
    type Target = State;

    fn deref(&self) -> &State {
        self.hold.as_deref().expect("the state held")
    }
}

impl DerefMut for StateHold<'_> {
    fn deref_mut(&mut self) -> &mut State {
        self.hold.as_deref_mut().expect("the state held")
    }
}

impl Drop for StateHold<'_> {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        drop(self.hold.take());
        if self.waiters_owed {
            self.checkpointer.wake_waiters();
        }
        if !self.task && !self.checkpointer.driven() {
            self.checkpointer.wake_writers();
        }
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
    /// The checkpoint's memory demand, kept across its steps whichever thread takes them
    /// (DESIGN.md 9.1.3, *Maintenance keeps what it owns*).
    upkeep: crate::storage::atoll::buffer::Upkeep,
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
    /// The metadata pages dirty at the cut, as the cut handed them over, still to move into `meta`.
    dirty: Chain<MetaId>,
    /// The metadata pages it writes: those dirty at the cut, then every one it edits.
    meta: BTreeSet<MetaId>,
    /// The last metadata page whose write it started: the next is the one after it.
    meta_at: Option<MetaId>,
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
    /// A turn of the log's flush asked for during this checkpoint's last one, which it answers
    /// first in its next unit.
    turn_owed: bool,
    /// The metadata pages fetched to write, in their order.
    frames: Vec<MetaRef>,
    /// What each metadata page asked to be written waits on, by its place in `frames`: its write,
    /// or a hold or a change to end before its write starts.
    writing: Vec<(usize, Completion)>,
    /// Where the walk of the data files for their syncs stands, and where each class's files ended
    /// as the walk began.
    files_at: (usize, u64),
    files_end: Option<[u64; page::CLASS_COUNT]>,
    /// The data files' syncs in flight, each with what its retirement completes: held to
    /// [`WRITES`] until they retire (the auditor's P2-CP-F1).
    syncing: Vec<(Completion, Completion)>,
    written: Option<Completion>,
    superblock: Option<Superblock>,
    /// Where the walk of `frames` a run at a time is, in the phases that take one.
    frames_at: usize,
    /// The last root of the registry of trees created read, once a run of it has been, and the
    /// registry's last root as the cut left it: the walk's end.
    creations_at: Option<PageId>,
    creations_end: Option<PageId>,
    /// Where the end of the pool's cut stands.
    end_cut: EndCut,
    /// The next segment to release, counting down from the first kept, once the release has
    /// begun; and whether one was released, which owes the directory its sync.
    release: Option<u64>,
    released: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Gate,
    Cut,
    Scan,
    /// The registry of trees created read, a run of roots a unit.
    Creations,
    Dirty,
    Orphans,
    Images,
    Values,
    /// The pool's cut ended, a run of its copies or lend batches a unit.
    EndCut,
    Intent,
    IntentDurable,
    Meta,
    /// The metadata's cut copies let go, a run of its frames a unit.
    Uncut,
    Files,
    Durable,
    Superblock,
    /// Each page the checkpoint wrote given its durable place, a run of its frames a unit, then
    /// the checkpoint put in charge.
    InCharge,
    After,
    /// The trees created before the cut forgotten as creations, one a unit.
    Forget,
    /// The log's segments before the redo point released, one a unit.
    Release,
    /// The directory synced, if a segment was released.
    Released,
    Erase,
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
    /// The write's own request, kept for a test to cancel as a backend's cancel would.
    #[cfg(test)]
    write: Completion,
    /// Done once the write's request has retired, no backend owning its buffer any more: what a
    /// step waiting for a place returns, so whoever steps the checkpoint waits for it without
    /// taking its failure for their own. The place is held until then (DESIGN.md 22.1, 22.13).
    landed: Completion,
    /// How it ended, as its callback was told, a short write a failure too; none, a request let
    /// go unrun. The checkpoint meets a failure as it lands its writes.
    outcome: Arc<Mutex<Option<std::result::Result<(), CompletionError>>>>,
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
            due: Arc::default(),
            under_way: AtomicBool::new(false),
            triggered_for: TriggerWord::default(),
            published: ArcCell::new(Arc::new(superblock.clone())),
            releasing,
            state: Guarded::new(State {
                superblock,
                run: None,
                full_done: 0,
                failed: None,
                upkeep: Default::default(),
            }),
            write_bytes: write_bytes.max(page::MAX_PAGE_SIZE),
            counters: Counters::default(),
            task: std::sync::OnceLock::new(),
            driven: AtomicBool::new(false),
            full_done: AtomicU64::new(0),
            failure: Mutex::new(None),
            waiters: Mutex::new(Vec::new()),
            released: Default::default(),
        }
    }

    /// What its working memory has held, and its work.
    #[cfg(test)]
    pub fn stats(&self) -> CheckpointStats {
        let c = &self.counters;
        CheckpointStats {
            working_peak: c.working_peak.load(Ordering::Acquire),
            flying_peak: c.flying_peak.load(Ordering::Acquire),
            flying_writes_peak: c.flying_writes_peak.load(Ordering::Acquire),
            pages: c.pages.load(Ordering::Relaxed),
            bytes: c.bytes.load(Ordering::Relaxed),
            stepping_ns: c.stepping_ns.load(Ordering::Relaxed),
        }
    }

    /// One unit of the state, timed. Whoever steps it wakes any waiter for a full checkpoint, and
    /// any writer waiting for room, once it has let the state go ([`StateHold`]).
    fn stepping(
        &self,
        state: &mut StateHold<'_>,
        txns: &Transactions,
        until: Until<'_>,
    ) -> Result<Unit> {
        let clock = self.store.io();
        let began = clock.current_time_monotonic();
        #[cfg(test)]
        WRITER_STEPS.with(|n| n.set(n.get() + 1));
        let out = {
            let _unit = unit::Unit::enter();
            state.step(self, txns, until)
        };
        let took = clock.current_time_monotonic().duration_since(began);
        self.counters.stepping_ns.fetch_add(
            u64::try_from(took.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let moved = self.recorded(state);
        state.waiters_owed |= moved;
        out
    }

    /// Units of the state, held, until one waits or nothing more is due, or, for a writer refused
    /// room, until the room takes it: a writer steps the checkpoint as far as it always has, a
    /// unit at a time.
    fn run_units(
        &self,
        state: &mut StateHold<'_>,
        txns: &Transactions,
        until: Until<'_>,
    ) -> Result<IOResult<()>> {
        loop {
            if until.met(&self.pool) {
                return Ok(IOResult::Done(()));
            }
            match self.stepping(state, txns, until)? {
                Unit::Again => {}
                Unit::Wait(c) => return Ok(IOResult::IO(IOCompletions(c))),
                Unit::Done => return Ok(IOResult::Done(())),
            }
        }
    }

    /// What the state records of full checkpoints and failure, published for a transaction's end
    /// waiting on the thread: whether either moved, which owes the waiters a wake.
    fn recorded(&self, state: &State) -> bool {
        let failed = state.failed.is_some();
        let before = self.full_done.swap(state.full_done, Ordering::AcqRel);
        if failed {
            let mut failure = self.failure.lock();
            if failure.is_none() {
                *failure = state.failed.clone();
            }
        }
        before != state.full_done || failed
    }

    /// Completes the waiters taken off the list, with no lock held: a waiter's waker may run code,
    /// and one that waits for a full checkpoint again takes the list's lock.
    fn wake_waiters(&self) {
        let waiters = std::mem::take(&mut *self.waiters.lock());
        for c in waiters {
            c.complete(0);
        }
    }

    /// The state, if no one holds it: let go, it wakes whoever waits on it.
    fn try_hold(&self) -> Option<StateHold<'_>> {
        self.state.try_hold().map(|hold| StateHold {
            hold: Some(hold),
            checkpointer: self,
            waiters_owed: false,
            task: false,
        })
    }

    /// The state, if no one holds it; else the wait its let-go finishes. Registered first and then
    /// tried again, so a let-go in between either finds the registration or is seen let go.
    fn hold_or_wait(&self) -> std::result::Result<StateHold<'_>, Completion> {
        if let Some(held) = self.try_hold() {
            return Ok(held);
        }
        let released = self.released.wait();
        self.try_hold().ok_or(released)
    }

    /// Let-goes of the state that found a caller waiting, for a test.
    #[cfg(test)]
    pub(crate) fn released_wakes(&self) -> u64 {
        self.released.woke()
    }

    /// The state, if no one holds it; else this thread is its contender, unparked once it is let
    /// go, and parks.
    fn hold_or_contend(&self) -> Option<StateHold<'_>> {
        self.state
            .hold_or_contend(&Contender::Thread(crate::thread::current()))
            .map(|hold| StateHold {
                hold: Some(hold),
                checkpointer: self,
                waiters_owed: false,
                task: false,
            })
    }

    /// Whether the checkpoint's task steps checkpoints.
    fn driven(&self) -> bool {
        self.driven.load(Ordering::Acquire)
    }

    /// Whether this thread is stepping the checkpoint's task.
    fn on_driver(&self) -> bool {
        STEPPING.with(|s| s.get() == std::ptr::from_ref(self) as usize)
    }

    /// Full checkpoints done, as the state last recorded them: read without waking anyone.
    #[cfg(test)]
    pub(crate) fn fulls_done(&self) -> u64 {
        self.full_done.load(Ordering::Acquire)
    }

    /// Whether the checkpoint's task is over: failed, or closed.
    #[cfg(test)]
    pub(crate) fn task_done(&self) -> bool {
        self.task.get().is_some_and(TaskRef::is_over)
    }

    /// Whether the checkpoint's task waits for the state as its contender.
    #[cfg(test)]
    pub(crate) fn driver_contends(&self) -> bool {
        self.task
            .get()
            .is_some_and(|t| self.state.contends(&Contender::Task(t.clone())))
    }

    /// A transaction's end's wait for full checkpoint `ticket`, as the thread does it: for a
    /// test's waker to wait again from inside its wake.
    #[cfg(test)]
    pub(crate) fn wait_full_on_the_thread(&self, ticket: u64) -> Result<IOResult<()>> {
        self.wait_full(ticket)
    }

    /// Wakes the checkpoint's task, from anywhere but its own step: a checkpoint may be due.
    fn wake_driver(&self) {
        if !self.driven() || self.on_driver() {
            return;
        }
        if let Some(task) = self.task.get() {
            task.wake();
        }
    }

    /// The checkpoint's task, named before the database is handed out: from then on only the
    /// task steps checkpoints, on a worker (`worker`, [`Checkpointing::Thread`]), which writers
    /// only wake, or claimed by the writers themselves ([`Checkpointing::Writers`]).
    pub(crate) fn attach(&self, task: TaskRef, worker: bool) {
        assert!(
            self.task.set(task).is_ok(),
            "a checkpointer given a second task"
        );
        self.driven.store(worker, Ordering::Release);
    }

    /// Who steps the checkpoint for a writer now.
    fn stepper(&self) -> Stepper<'_> {
        match self.task.get() {
            Some(_) if self.driven() => Stepper::Worker,
            // A task that failed or closed is over: the writer steps the state and meets the
            // failure, as writers stepping checkpoints always have.
            Some(task) if !task.is_over() => Stepper::Caller(task),
            _ => Stepper::Writer,
        }
    }

    /// A writer's named claim of the checkpoint's task (DESIGN.md 22.7): one step, on this
    /// thread, unless another writer is stepping it or the task is over; what it came to. A step
    /// that held the state has ended here, so whoever waits on the room, the gate or the state's
    /// let-go looks again: a claim that found the task running registered its wait first, and
    /// that step's own let-go may have come before the claim looked. A claim that stepped nothing,
    /// or whose step only contended for the state another holds, wakes no one, itself included:
    /// that holder's let-go does.
    fn claim(&self, task: &TaskRef) -> Claimed {
        CLAIMING.with(|c| *c.borrow_mut() = Claiming::Made);
        let ran = task.run_once();
        let claiming = CLAIMING.with(|c| std::mem::replace(&mut *c.borrow_mut(), Claiming::No));
        if !ran {
            return Claimed::Refused;
        }
        #[cfg(test)]
        CLAIMS.with(|n| n.set(n.get() + 1));
        let Claiming::Told(claimed) = claiming else {
            unreachable!("a claimed step did not tell its claimant what it came to")
        };
        if matches!(claimed, Claimed::Stepped(_)) {
            self.wake_writers();
        }
        claimed
    }

    /// Wakes the writers waiting on the state's let-go to step the checkpoint themselves: those
    /// refused room, those the gate refused while a cut is under way, and those waiting for a
    /// full checkpoint.
    fn wake_writers(&self) {
        self.pool.wake_room();
        if self.store.gate_closed() {
            self.store.wake_gate_waiters();
        }
        self.released.let_go();
    }

    /// The checkpoint's task has closed: writers would step checkpoints again, and no waiter is
    /// left waiting on the task.
    fn ended(&self) {
        self.driven.store(false, Ordering::Release);
        self.wake_waiters();
        self.pool.wake_room();
    }

    /// A transaction's end waiting for full checkpoint `ticket`, which the thread does: done once
    /// it is, a failure once one has failed, else a wait the thread finishes.
    fn wait_full(&self, ticket: u64) -> Result<IOResult<()>> {
        let mut waiters = self.waiters.lock();
        if let Some(failed) = self.failure.lock().as_ref() {
            return Err(LimboError::InternalError(format!(
                "a checkpoint failed ({failed}): none runs until the database is opened again"
            )));
        }
        if self.full_done.load(Ordering::Acquire) >= ticket {
            return Ok(IOResult::Done(()));
        }
        let c = crate::storage::atoll::notify::remote_wait();
        waiters.push(c.clone());
        drop(waiters);
        self.wake_driver();
        Ok(IOResult::IO(IOCompletions(c)))
    }

    /// A refused writer's demand on the checkpoint's task, of the relief class (DESIGN.md 22.4),
    /// which its room ticket holds: taken, it raises the task's class and wakes it. None with no
    /// task, a task over, or for the task's own step, whose writer is the task.
    fn demand(&self) -> Option<executor::Ticket> {
        if self.on_driver() {
            return None;
        }
        let task = self.task.get()?;
        (!task.is_over()).then(|| task.ticket(Class::Relief))
    }

    /// For a writer refused room for kept copies, holding nothing: steps the checkpoint under way
    /// until the room takes the request it refused, its images giving copies back, kept copies
    /// first. [`Relieved::Elsewhere`] while the checkpointing thread steps it, and
    /// [`Relieved::Held`] while another caller holds its state. A checkpoint that fails meanwhile
    /// is abandoned, its cut ended and its copies dropped, and tells its failure to whoever steps
    /// checkpoints next: the writer goes on, as commits do.
    pub fn relieve(&self, txns: &Transactions, retry: Retry<'_>) -> Relieved {
        // The task's own step, out of room: the state is its own to let go.
        if self.on_driver() {
            return Relieved::Held;
        }
        match self.stepper() {
            // The worker's to step: the writer waits for the room it frees, its ticket's demand
            // having woken it.
            Stepper::Worker => {
                self.wake_driver();
                return Relieved::Elsewhere;
            }
            // The task's units claimed one at a time, only as far as the writer's room needs.
            Stepper::Caller(task) => {
                if let Some(relieved) = self.claim_relief(task, retry) {
                    return relieved;
                }
                // The task over since the writer looked: it steps the state itself.
            }
            Stepper::Writer => {}
        }
        let Some(mut state) = self.try_hold() else {
            return Relieved::Held;
        };
        let out = self.run_units(&mut state, txns, Until::Room(retry));
        drop(state);
        Relieved::Stepped(match out {
            Err(e) => {
                tracing::warn!("a checkpoint a writer stepped for room failed: {e}");
                Ok(IOResult::Done(()))
            }
            stepped => stepped,
        })
    }

    /// [`relieve`](Self::relieve) for a writer that claims the checkpoint's task (DESIGN.md 22.4,
    /// 22.7): a unit a claim, until the writer may start again or a unit waits. [`Relieved::Held`]
    /// while another writer's claim or a holder of the state steps it, whose end wakes the room's
    /// waiters; `None` once the task is over, the writer then stepping the state.
    fn claim_relief(&self, task: &TaskRef, retry: Retry<'_>) -> Option<Relieved> {
        loop {
            if self.pool.may_retry(retry) {
                return Some(Relieved::Stepped(Ok(IOResult::Done(()))));
            }
            return Some(match self.claim(task) {
                Claimed::Stepped(Ok(Unit::Again)) => continue,
                Claimed::Stepped(Ok(Unit::Wait(c))) => {
                    Relieved::Stepped(Ok(IOResult::IO(IOCompletions(c))))
                }
                Claimed::Stepped(Ok(Unit::Done)) => Relieved::Stepped(Ok(IOResult::Done(()))),
                Claimed::Stepped(Err(e)) => {
                    tracing::warn!("a checkpoint a writer stepped for room failed: {e}");
                    Relieved::Stepped(Ok(IOResult::Done(())))
                }
                Claimed::Refused if task.is_over() => return None,
                Claimed::Refused | Claimed::Held => Relieved::Held,
            });
        }
    }

    /// The log, in bytes, the checkpointer keeps a recovery to replaying.
    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// How far the redo point in charge trails the log's end, in bytes.
    pub fn lag(&self) -> u64 {
        self.log.end().saturating_sub(self.superblock().redo)
    }

    /// How far the redo point in charge trails `at`, in bytes: the most a checkpoint could take
    /// off it while the log is held back there.
    pub fn lag_to(&self, at: Lsn) -> u64 {
        at.saturating_sub(self.superblock().redo)
    }

    /// The superblock in charge.
    pub fn superblock(&self) -> Superblock {
        let step = self.releasing.epochs().enter();
        (*self.published.get(&step).expect("a superblock in charge")).clone()
    }

    /// Does whatever is due: a checkpoint once its share of the log has been appended, going on
    /// with one under way. Done once nothing more is due now, or while someone else steps it.
    pub fn step(&self, txns: &Transactions) -> Result<IOResult<()>> {
        match self.stepper() {
            Stepper::Worker => {
                self.wake_driver();
                return Ok(IOResult::Done(()));
            }
            // Its units claimed one at a time, as far as they go now.
            Stepper::Caller(task) => loop {
                match self.claim(task) {
                    Claimed::Stepped(Ok(Unit::Again)) => {}
                    Claimed::Stepped(Ok(Unit::Wait(c))) => {
                        return Ok(IOResult::IO(IOCompletions(c)))
                    }
                    Claimed::Stepped(Ok(Unit::Done)) => return Ok(IOResult::Done(())),
                    Claimed::Stepped(Err(e)) => return Err(e),
                    // Another writer or holder steps it now: nothing is owed here.
                    Claimed::Refused | Claimed::Held => return Ok(IOResult::Done(())),
                }
            },
            Stepper::Writer => {}
        }
        let Some(mut state) = self.try_hold() else {
            return Ok(IOResult::Done(()));
        };
        let out = self.run_units(&mut state, txns, Until::Done);
        drop(state);
        out
    }

    /// A writing statement's step and a transaction's end (DESIGN.md §17, 22.9): only once a
    /// checkpoint is due by the log appended since the last began, one is under way, or slots
    /// freed are owed their erasure. Due, it notifies *due*, which sets the task's own level as it
    /// wakes it; a worker steps the task, and the writers stepping checkpoints claim it, as they
    /// do while one is under way or erasure is owed.
    pub fn step_due(&self, txns: &Transactions) -> Result<IOResult<()>> {
        // One read of where the last checkpoint began, for the threshold and the trigger alike.
        let began = self.began_at.load(Ordering::SeqCst);
        let due = self.due_since(began);
        let under_way = self.under_way.load(Ordering::Acquire);
        // Slots freed and not yet erased: the end of the step that may have held one, a
        // statement's or a transaction's, may let them be erased now.
        let erasing = !due && !under_way && self.store.erasure_owed();
        if !due && !under_way && !erasing {
            #[cfg(test)]
            STEPS_PASSED_OVER.with(|n| n.set(n.get() + 1));
            return Ok(IOResult::Done(()));
        }
        if due && self.claim_trigger(began) {
            if let Some(task) = self.task.get().filter(|task| !task.is_over()) {
                #[cfg(test)]
                TRIGGERS.with(|n| n.set(n.get() + 1));
                task.trigger(&self.due);
            }
        }
        match self.stepper() {
            // Its own IO steps a checkpoint under way, and the trigger woke it for one due; the
            // erasure owed is maintenance, woken at the task's own level.
            Stepper::Worker => {
                if erasing {
                    self.wake_driver();
                }
                Ok(IOResult::Done(()))
            }
            Stepper::Caller(_) | Stepper::Writer => self.step(txns),
        }
    }

    /// Whether this statement, which found a checkpoint due since `began`, is the one to notify the
    /// trigger for that crossing: the first to raise the word past `began`. A statement after it
    /// reads the word alone, and one that read an earlier start never lowers it.
    fn claim_trigger(&self, began: Lsn) -> bool {
        self.triggered_for.claim(began)
    }

    /// Whether a checkpoint is due by the log appended since the last began (DESIGN.md §17).
    fn due_now(&self) -> bool {
        self.due_since(self.began_at.load(Ordering::SeqCst))
    }

    /// Whether a checkpoint is due by the log appended since `began`: a share of the budget, at
    /// least a byte, with a record other than a checkpoint's own among it. A checkpoint's own
    /// intent record and the padding around it are no input (the auditor's P1-CP-D1: a quiet
    /// database whose checkpoints' own records made the next due checkpointed forever). Both rise
    /// and neither falls until a checkpoint begins, so once due, due until then.
    fn due_since(&self, began: Lsn) -> bool {
        self.log.end().saturating_sub(began) >= (self.budget / SHARES).max(1)
            && self.log.input_since()
    }

    /// Whether a full checkpoint asked for is not done yet, as the state last recorded them.
    fn full_pending(&self) -> bool {
        self.full_wanted.load(Ordering::Acquire) > self.full_done.load(Ordering::Acquire)
    }

    /// The signal a checkpoint due is notified on, which its task watches (DESIGN.md 22.9).
    pub(crate) fn due_signal(&self) -> &Arc<Signal> {
        &self.due
    }

    /// Holds the checkpointer idle, if no one is stepping it: no checkpoint runs until the hold is
    /// dropped, so the page store holds still for an integrity check (DESIGN.md §17, §12; R36).
    #[cfg(test)]
    pub fn hold(&self) -> Option<impl Sized + '_> {
        self.try_hold()
    }

    /// [`hold`](Self::hold), waiting while someone steps the checkpointer: parked as the state's
    /// contender, woken once it is let go.
    pub fn hold_waiting(&self) -> impl Sized + '_ {
        loop {
            if let Some(held) = self.hold_or_contend() {
                return held;
            }
            crate::storage::atoll::park::thread();
        }
    }

    /// The checkpoint under way whose cut is taken, if any: what it writes is as of that cut.
    #[cfg(test)]
    pub(crate) fn cut_taken(&self) -> Option<u64> {
        let state = self.try_hold()?;
        let run = state.run.as_ref()?;
        (!matches!(run.phase, Phase::Gate | Phase::Cut | Phase::Scan)).then_some(run.n)
    }

    /// Asks for a full checkpoint: one begun after the ask, done. Step it with
    /// [`step_full`](Self::step_full).
    /// Full checkpoints asked for so far, for a test to tell one was.
    #[cfg(test)]
    pub(crate) fn fulls_requested(&self) -> u64 {
        self.full_wanted.load(Ordering::Acquire)
    }

    pub fn request_full(&self) -> u64 {
        let ticket = self.full_wanted.fetch_add(1, Ordering::AcqRel) + 1;
        self.wake_driver();
        ticket
    }

    /// Done once the full checkpoint `ticket` names is done.
    pub fn step_full(&self, ticket: u64, txns: &Transactions) -> Result<IOResult<()>> {
        match self.stepper() {
            Stepper::Worker => return self.wait_full(ticket),
            Stepper::Caller(task) => return self.claim_full(task, ticket),
            Stepper::Writer => {}
        }
        let mut state = match self.hold_or_wait() {
            Ok(state) => state,
            Err(released) => return Ok(IOResult::IO(IOCompletions(released))),
        };
        let stepped =
            (state.full_done < ticket).then(|| self.run_units(&mut state, txns, Until::Done));
        let done = state.full_done >= ticket;
        drop(state);
        if let Some(IOResult::IO(io)) = stepped.transpose()? {
            return Ok(IOResult::IO(io));
        }
        Ok(if done {
            IOResult::Done(())
        } else {
            IOResult::IO(IOCompletions(Completion::new_yield()))
        })
    }

    /// [`step_full`](Self::step_full) for a writer that claims the checkpoint's task: its units
    /// claimed one at a time, as far as they go now, done once the full checkpoint `ticket` names
    /// is. A claim another writer's step holds, or whose step found the state held by another,
    /// registers on the state's let-go and claims once more, waiting only if that claim is refused
    /// too: a let-go before the registration is seen by the claim after it, and one after it
    /// finishes the wait. A claim that steps registers nothing, so a unit costs no wait (the
    /// auditor's P2-CP-W1).
    fn claim_full(&self, task: &TaskRef, ticket: u64) -> Result<IOResult<()>> {
        let done = |cp: &Self| cp.full_done.load(Ordering::Acquire) >= ticket;
        if done(self) {
            return Ok(IOResult::Done(()));
        }
        let mut released: Option<Completion> = None;
        loop {
            crate::race::point("checkpoint.claiming");
            return match self.claim(task) {
                Claimed::Stepped(Err(e)) => Err(e),
                Claimed::Stepped(Ok(Unit::Wait(c))) => Ok(IOResult::IO(IOCompletions(c))),
                Claimed::Stepped(Ok(Unit::Again | Unit::Done)) if done(self) => {
                    Ok(IOResult::Done(()))
                }
                Claimed::Stepped(Ok(Unit::Again)) => {
                    // The claim's own let-go finished any wait registered before it.
                    released = None;
                    continue;
                }
                // Nothing more due short of the full checkpoint, whose next call claims the next
                // unit; or the task over (failed or closed) since the writer looked, its last
                // claim's wakes perhaps before any wait was registered, whose next call steps
                // the state.
                Claimed::Stepped(Ok(Unit::Done)) => {
                    Ok(IOResult::IO(IOCompletions(Completion::new_yield())))
                }
                Claimed::Refused if task.is_over() => {
                    Ok(IOResult::IO(IOCompletions(Completion::new_yield())))
                }
                Claimed::Refused | Claimed::Held => match released.take() {
                    Some(released) => {
                        crate::race::point("checkpoint.claim_refused");
                        Ok(IOResult::IO(IOCompletions(released)))
                    }
                    None => {
                        released = Some(self.released.wait());
                        continue;
                    }
                },
            };
        }
    }

    /// Cancels every image write of the checkpoint under way, as a backend's cancel does: each
    /// finishes aborted and stays its backend's until delivered. How many there were.
    #[cfg(test)]
    pub(crate) fn cancel_image_writes(&self) -> usize {
        let state = self.try_hold().expect("the state free");
        let Some(run) = state.run.as_ref() else {
            return 0;
        };
        for f in &run.flying {
            f.write.abort();
        }
        run.flying.len()
    }

    /// Whether the checkpoint's next unit takes a turn toward its intent record's durability, not
    /// one owed: its turns of the log's flush, the last before its superblock among them.
    #[cfg(test)]
    pub(crate) fn next_turn_for_intent(&self) -> bool {
        let state = self.try_hold().expect("the state free");
        state
            .run
            .as_ref()
            .is_some_and(|run| run.phase == Phase::IntentDurable && !run.turn_owed)
    }

    /// The image writes of the checkpoint under way that hold a place in its flight set.
    #[cfg(test)]
    pub(crate) fn image_writes_in_flight(&self) -> usize {
        let state = self.try_hold().expect("the state free");
        state.run.as_ref().map_or(0, |run| run.flying.len())
    }

    /// One unit of the checkpoint, as a writer steps it, for a test: what it waits on, if it
    /// waits.
    #[cfg(test)]
    pub(crate) fn one_unit(&self, txns: &Transactions) -> Result<Option<Completion>> {
        let mut state = self.try_hold().expect("the state free");
        Ok(match self.stepping(&mut state, txns, Until::Done)? {
            Unit::Wait(c) => Some(c),
            Unit::Again | Unit::Done => None,
        })
    }

    /// Whether a checkpoint is due by the log appended since the last began.
    #[cfg(test)]
    pub(crate) fn is_due(&self) -> bool {
        self.due_now()
    }

    /// Whether a checkpoint is under way.
    #[cfg(test)]
    pub(crate) fn is_under_way(&self) -> bool {
        self.under_way.load(Ordering::Acquire)
    }

    /// The class of the checkpoint's task now, if it has one (DESIGN.md 22.4).
    #[cfg(test)]
    pub(crate) fn task_class(&self) -> Option<Class> {
        self.task.get().map(TaskRef::class)
    }

    /// One claim of the checkpoint's task, for a test: whether it stepped.
    #[cfg(test)]
    pub(crate) fn claim_one(&self) -> bool {
        let task = self.task.get().expect("the checkpoint's task");
        !matches!(self.claim(task), Claimed::Refused)
    }
}

/// The metadata ids a run of the dirty set moves: as many as one page of ids holds (DESIGN.md
/// 22.1).
const DIRTY_RUN: usize = page::MIN_PAGE_SIZE / std::mem::size_of::<MetaId>();

/// The metadata frames a run of the checkpoint's bookkeeping over them walks: as many as one page
/// of their references holds (DESIGN.md 22.1).
const FRAME_RUN: usize = page::MIN_PAGE_SIZE / std::mem::size_of::<MetaRef>();

/// The trees created a run of the registry's walk reads: as many as one page of its entries holds
/// (DESIGN.md 22.1).
const CREATION_RUN: usize = page::MIN_PAGE_SIZE / std::mem::size_of::<(PageId, u16, u64)>();

/// What a unit of a checkpoint's work leaves (DESIGN.md 22.1).
enum Unit {
    /// Nothing more to do now: no checkpoint is due and nothing is left to erase.
    Done,
    /// A unit done, and the next ready now.
    Again,
    /// The next unit waits on this: a request in flight, a place in the flight set, or what a
    /// step it depends on waits for.
    Wait(Completion),
}

/// The value of a step that is done, or the unit's wait on what it handed back.
macro_rules! unit {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(IOCompletions(c)) => return Ok(Unit::Wait(c)),
        }
    };
}

impl State {
    /// One unit of whatever is due: a checkpoint, begun once its share of the log is appended or
    /// a full one is asked for, or, with none due, erasure. A checkpoint's last unit erases what
    /// its frees let go, and says Done: one due again at once is begun by the next unit. A writer
    /// refused room has nothing to do here with none under way.
    fn step(&mut self, cp: &Checkpointer, txns: &Transactions, until: Until<'_>) -> Result<Unit> {
        if let Some(failed) = &self.failed {
            return Err(LimboError::InternalError(format!(
                "a checkpoint failed ({failed}): none runs until the database is opened again"
            )));
        }
        if matches!(until, Until::Room(_)) && self.run.is_none() {
            return Ok(Unit::Done);
        }
        if self.run.is_none() {
            let wanted = cp.full_wanted.load(Ordering::Acquire);
            if wanted <= self.full_done && !cp.due_now() {
                unit::visit();
                return Ok(match unit!(cp.store.erase_one()) {
                    true => Unit::Done,
                    false => Unit::Again,
                });
            }
            cp.began_at.store(cp.log.begin_input(), Ordering::SeqCst);
            cp.under_way.store(true, Ordering::Release);
            self.run = Some(Box::new(Run::new(
                self.superblock.checkpoint + 1,
                (wanted > self.full_done).then_some(wanted),
            )));
        }
        let run = self.run.as_mut().expect("a checkpoint under way");
        let superblock = &self.superblock;
        let out = crate::storage::atoll::buffer::upkeep_scope(&mut self.upkeep, || {
            run.step(cp, txns, superblock)
        });
        match out {
            // One checkpoint a step's run of units, as before them: one due again at once is the
            // next step's.
            Ok(Unit::Done) => {
                let run = self.run.take().expect("a checkpoint under way");
                cp.under_way.store(false, Ordering::Release);
                self.superblock = run.superblock.expect("the checkpoint's superblock");
                if let Some(full) = run.full {
                    self.full_done = self.full_done.max(full);
                }
                Ok(Unit::Done)
            }
            Ok(unit) => Ok(unit),
            Err(e) => {
                // A checkpoint that cannot finish leaves the one in charge in charge: the pages
                // it wrote are named by no superblock. What it marked imaged and edited as it
                // stands is not undone, so no checkpoint runs again until the database is opened
                // again and recovers from the one in charge.
                if let Some(run) = self.run.take() {
                    run.abandon(cp, txns);
                }
                cp.under_way.store(false, Ordering::Release);
                self.upkeep.abandon();
                self.failed = Some(e.to_string());
                Err(e)
            }
        }
    }
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
            dirty: Chain::default(),
            meta: BTreeSet::new(),
            meta_at: None,
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
            turn_owed: false,
            frames: Vec::new(),
            writing: Vec::new(),
            files_at: (0, 0),
            files_end: None,
            syncing: Vec::new(),
            written: None,
            superblock: None,
            frames_at: 0,
            creations_at: None,
            creations_end: None,
            end_cut: EndCut::default(),
            release: None,
            released: false,
        }
    }

    /// Lets go of what a checkpoint that cannot finish holds: the gate, the cuts, the snapshot,
    /// and the pages handed over to it. A turn of the log's flush it owes is taken, so no request
    /// made during its last turn goes unanswered.
    fn abandon(mut self, cp: &Checkpointer, txns: &Transactions) {
        let mut owed = self.turn_owed;
        while owed {
            owed = cp.log.flush_one_turn();
        }
        if matches!(self.phase, Phase::Gate | Phase::Cut | Phase::Scan) {
            cp.store.open_gate();
        }
        // A cut stopped, its end under way, is ended too.
        if cp.pool.cutting() == self.n || self.end_cut.begun() {
            while !cp.pool.end_cut_run(self.n, &mut self.end_cut) {}
        }
        // The cut copies its frames kept go even where the cut ended before (`Uncut`).
        if cp.store.meta().cutting() == self.n {
            cp.store.meta().stop_keeping(self.n);
        }
        cp.store.meta().uncut(self.n, &self.frames);
        for pid in self.orphans {
            cp.store.orphaned(pid);
        }
        txns.release_checkpoint();
    }

    /// One unit of the checkpoint (DESIGN.md 22.9 item 1): each phase is a cursor the next unit
    /// goes on from. Done once its superblock is in charge, every free that waited for it is done
    /// and the slots freed are erased.
    fn step(
        &mut self,
        cp: &Checkpointer,
        txns: &Transactions,
        in_charge: &Superblock,
    ) -> Result<Unit> {
        if self.turn_owed {
            // A turn of the log's flush asked for during this checkpoint's last one: answered
            // before anything else, as the turn's holder (`Solo`).
            self.turn_owed = cp.log.flush_one_turn();
            return Ok(Unit::Again);
        }
        loop {
            match self.phase {
                Phase::Gate => {
                    if let IOResult::IO(IOCompletions(c)) = cp.store.close_gate() {
                        return Ok(Unit::Wait(c));
                    }
                    // Before the cut: a page handed over once it is in effect, a value's page an
                    // image may name, waits for the next checkpoint.
                    self.orphans = cp.store.take_orphans();
                    self.phase = Phase::Cut;
                    return Ok(Unit::Again);
                }
                Phase::Cut => {
                    let (l, c) = unit!(txns.cut(&mut self.cut));
                    self.l = l;
                    self.c = c;
                    self.clock = txns.clock_now();
                    txns.hold_checkpoint(c);
                    self.phase = Phase::Scan;
                    return Ok(Unit::Again);
                }
                Phase::Scan => {
                    // The pool's walk a run at a time, then the metadata cut.
                    unit::visit();
                    match cp.pool.cut_run(self.n, &mut self.scan) {
                        IOResult::IO(IOCompletions(c)) => return Ok(Unit::Wait(c)),
                        IOResult::Done(false) => return Ok(Unit::Again),
                        IOResult::Done(true) => {}
                    }
                    let (dirty, heights) = cp.store.begin_cut(self.n);
                    // The registry's end, taken once the walk has found every page owed: each of
                    // those a tree's root was registered before the walk took its latch.
                    self.creations_end = cp.store.last_creation();
                    cp.store.open_gate();
                    self.heights = heights;
                    self.dirty = dirty;
                    // Handed over whole, no copy made: a vector becomes a queue in place.
                    self.queue = VecDeque::from(std::mem::take(&mut self.scan.owed));
                    self.phase = Phase::Creations;
                    return Ok(Unit::Again);
                }
                Phase::Creations => {
                    // The registry of trees created, a run of roots a unit from where the last
                    // ended, to the end taken before the gate reopened: a finite inventory, however
                    // many trees are created meanwhile. Every creation registered by then is read,
                    // only this checkpoint's `Forget` taking one out; one registered since is a
                    // tree created after the pool's walk, no page owed an image one of its pages,
                    // and those a run reads below the end are set aside as the snapshot does not
                    // see them.
                    let Some(end) = self.creations_end else {
                        self.phase = Phase::Dirty;
                        continue;
                    };
                    unit::visit();
                    let run = cp
                        .store
                        .creations_after(self.creations_at, end, CREATION_RUN);
                    for &(root, slot, start) in &run {
                        if txns.committed_by(slot, start, self.c) {
                            self.mapped_roots.push(root);
                        } else {
                            self.set_aside.insert(root);
                        }
                    }
                    match run.last() {
                        Some(&(root, ..)) if run.len() == CREATION_RUN && root != end => {
                            self.creations_at = Some(root)
                        }
                        _ => self.phase = Phase::Dirty,
                    }
                    return Ok(Unit::Again);
                }
                Phase::Dirty => {
                    if self.dirty_run() {
                        self.phase = Phase::Orphans;
                    }
                    return Ok(Unit::Again);
                }
                Phase::Orphans => {
                    let Some(&pid) = self.orphans.last() else {
                        self.phase = Phase::Images;
                        continue;
                    };
                    // Named by nothing: free as of the cut, and freed as it stands. Marked left
                    // out only once its edit is made, so a wait in the edit makes it again.
                    unit::visit();
                    if !self.freed.contains(&pid) {
                        let edited = unit!(cp.store.left_out(self.n, &self.heights, pid));
                        self.meta.extend(edited);
                        self.freed.insert(pid);
                        return Ok(Unit::Again);
                    }
                    unit!(cp.pool.free_orphan(pid));
                    self.orphans.pop();
                    return Ok(Unit::Again);
                }
                Phase::Images => match self.image_unit(cp, txns, false)? {
                    Unit::Done => {
                        debug_assert!(self.early.is_empty(), "a page imaged early still queued");
                        self.queue = VecDeque::from(std::mem::take(&mut self.values));
                        self.phase = Phase::Values;
                    }
                    unit => return Ok(unit),
                },
                Phase::Values => match self.image_unit(cp, txns, true)? {
                    Unit::Done => {
                        self.phase = Phase::EndCut;
                        return Ok(Unit::Again);
                    }
                    unit => return Ok(unit),
                },
                Phase::EndCut => {
                    unit::visit();
                    if cp.pool.end_cut_run(self.n, &mut self.end_cut) {
                        self.phase = Phase::Intent;
                    }
                    return Ok(Unit::Again);
                }
                Phase::Intent => {
                    // Its size checked before anything is built: a record past the log's buffer
                    // is never admitted.
                    let len = framed_len(8 + 8 * self.meta.len());
                    if len > cp.log.buffer_bytes() {
                        return Err(LimboError::InternalError(format!(
                            "checkpoint {} writes {} metadata pages: its intent record, {len} \
                             bytes, is past the log's buffer of {} bytes",
                            self.n,
                            self.meta.len(),
                            cp.log.buffer_bytes()
                        )));
                    }
                    let admitted = match &mut self.intent {
                        Some(a) => a,
                        None => self.intent.insert(unit!(cp.log.admit(len))),
                    };
                    let header = Header {
                        kind: META_INTENT,
                        flags: 0,
                        txn: 0,
                        prev: None,
                    };
                    let payload = intent_payload(self.n, &self.meta);
                    // The checkpoint's own record: no input that makes another due.
                    let (_, end) = cp.log.append_own(admitted, &header, &[&payload]);
                    self.intent = None;
                    self.intent_end = Some(end);
                    self.phase = Phase::IntentDurable;
                    return Ok(Unit::Again);
                }
                Phase::IntentDurable => {
                    let end = self.intent_end.expect("appended");
                    if let Some(unit) = self.durable_turn(cp, end)? {
                        return Ok(unit);
                    }
                    self.phase = Phase::Meta;
                    return Ok(Unit::Again);
                }
                Phase::Meta => return self.meta_unit(cp),
                Phase::Uncut => {
                    unit::visit();
                    if let Some(run) = self.frame_run() {
                        cp.store.meta().uncut(self.n, &self.frames[run]);
                        return Ok(Unit::Again);
                    }
                    self.phase = Phase::Files;
                    return Ok(Unit::Again);
                }
                Phase::Files => {
                    // A data file's sync started a unit, the files walked by a cursor; each holds
                    // its place in flight until its request retires, `WRITES` of them at most, a
                    // failure failing the checkpoint; then every one waited for.
                    if let Some(e) = self
                        .syncing
                        .iter()
                        .filter(|(_, retired)| retired.finished())
                        .find_map(|(sync, _)| sync.get_error())
                    {
                        return Err(LimboError::CompletionError(e));
                    }
                    self.syncing.retain(|(_, retired)| !retired.finished());
                    if self.syncing.len() >= WRITES {
                        return Ok(Unit::Wait(self.syncing[0].1.clone()));
                    }
                    // The files as they stand once every image is written: those opened later need
                    // no sync of this checkpoint's.
                    let ends = *self
                        .files_end
                        .get_or_insert_with(|| cp.store.data_files_end());
                    match cp.store.data_file_from(&mut self.files_at, &ends) {
                        Some(Some(file)) => {
                            unit::visit();
                            let sync = file.sync_data(Completion::new_sync(|_| {}))?;
                            let retired = crate::storage::atoll::notify::wait();
                            let place = retired.clone();
                            sync.on_retired(move || place.complete(0));
                            self.syncing.push((sync, retired));
                        }
                        // A run of places with no file in it.
                        Some(None) => unit::visit(),
                        None => {
                            if let Some((_, retired)) = self.syncing.first() {
                                return Ok(Unit::Wait(retired.clone()));
                            }
                            self.phase = Phase::Durable;
                        }
                    }
                    return Ok(Unit::Again);
                }
                Phase::Durable => {
                    if let Some(unit) = self.durable_turn(cp, cp.log.end())? {
                        return Ok(unit);
                    }
                    self.phase = Phase::Superblock;
                    return Ok(Unit::Again);
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
                    unit::visit();
                    unit!(cp.control.step_write(sb, &mut self.written));
                    self.frames_at = 0;
                    self.phase = Phase::InCharge;
                    return Ok(Unit::Again);
                }
                Phase::InCharge => {
                    unit::visit();
                    if let Some(run) = self.frame_run() {
                        cp.store.meta().durable_sides(self.n, &self.frames[run]);
                        return Ok(Unit::Again);
                    }
                    cp.store.meta().put_in_charge(self.n);
                    let sb = self.superblock.as_ref().expect("the superblock written");
                    cp.published
                        .replace(Some(Arc::new(sb.clone())), &cp.releasing);
                    self.phase = Phase::After;
                    return Ok(Unit::Again);
                }
                Phase::After => {
                    // A deferred free a unit.
                    unit::visit();
                    if !unit!(cp.store.after_one(self.n)) {
                        return Ok(Unit::Again);
                    }
                    self.phase = Phase::Forget;
                    return Ok(Unit::Again);
                }
                Phase::Forget => {
                    let Some(root) = self.mapped_roots.pop() else {
                        self.phase = Phase::Release;
                        continue;
                    };
                    unit::visit();
                    cp.store.forget_creation(root);
                    return Ok(Unit::Again);
                }
                Phase::Release => {
                    // The newest segment before the redo point first: the ones before a segment
                    // gone went before it.
                    let next = *self.release.get_or_insert_with(|| cp.log.kept_from(self.l));
                    unit::visit();
                    if next > 0 && cp.log.release_segment(next - 1)? {
                        self.release = Some(next - 1);
                        self.released = true;
                        return Ok(Unit::Again);
                    }
                    self.phase = Phase::Released;
                    return Ok(Unit::Again);
                }
                Phase::Released => {
                    if std::mem::take(&mut self.released) {
                        unit::visit();
                        cp.log.sync_released()?;
                    }
                    txns.release_checkpoint();
                    self.phase = Phase::Erase;
                    return Ok(Unit::Again);
                }
                Phase::Erase => {
                    unit::visit();
                    return Ok(match unit!(cp.store.erase_one()) {
                        true => Unit::Done,
                        false => Unit::Again,
                    });
                }
            }
        }
    }

    /// One turn of the log's flush toward everything before `lsn` being durable: `None` once it
    /// is; else the unit's outcome, a turn owed answered first ([`Log::wait_durable_turn`]).
    fn durable_turn(&mut self, cp: &Checkpointer, lsn: Lsn) -> Result<Option<Unit>> {
        let (r, owed) = cp.log.wait_durable_turn(lsn)?;
        self.turn_owed = owed;
        Ok(match r {
            IOResult::Done(()) => None,
            // The owed turn first: waiting with it unanswered would leave its request waiting on
            // this checkpoint's own write.
            IOResult::IO(_) if owed => Some(Unit::Again),
            IOResult::IO(IOCompletions(c)) => Some(Unit::Wait(c)),
        })
    }

    /// The next run of the frames the checkpoint wrote, [`FRAME_RUN`] of them at most, its cursor
    /// moved past it: `None` once the walk has passed them all.
    fn frame_run(&mut self) -> Option<std::ops::Range<usize>> {
        let at = self.frames_at;
        if at >= self.frames.len() {
            return None;
        }
        let end = self.frames.len().min(at + FRAME_RUN);
        self.frames_at = end;
        Some(at..end)
    }

    /// One run of the metadata pages dirty at the cut, [`DIRTY_RUN`] of them at most, into the
    /// ordered set the run walks, a page listed once for each time it turned dirty: whether none
    /// is left.
    fn dirty_run(&mut self) -> bool {
        unit::visit();
        for _ in 0..DIRTY_RUN {
            match self.dirty.next() {
                Some(id) => {
                    self.meta.insert(id);
                }
                None => return true,
            }
        }
        false
    }

    /// One unit of the metadata's writes, the pages taken in their order: a write that ended
    /// asked again, or the next page fetched and its write started, each held to [`WRITES`] in
    /// flight. Every page is asked until it is written for this checkpoint: a wait other than its
    /// write's (a hold to end, a change settling under the look) writes nothing, and a page left
    /// unwritten would stay dirty with no later cut naming it. Once every page is, the data files'
    /// syncs start.
    fn meta_unit(&mut self, cp: &Checkpointer) -> Result<Unit> {
        if let Some(i) = self.writing.iter().position(|(_, c)| c.finished()) {
            let (at, c) = self.writing.swap_remove(i);
            if let Some(e) = c.get_error() {
                return Err(LimboError::CompletionError(e));
            }
            unit::visit();
            if let Some(c) = cp.store.meta().write_for(&self.frames[at], self.n)? {
                self.writing.push((at, c));
            }
            return Ok(Unit::Again);
        }
        if self.writing.len() >= WRITES {
            return Ok(Unit::Wait(self.writing[0].1.clone()));
        }
        let next = match self.meta_at {
            None => self.meta.iter().next(),
            Some(at) => self.meta.range((Excluded(at), Unbounded)).next(),
        }
        .copied();
        let Some(id) = next else {
            if let Some((_, c)) = self.writing.first() {
                return Ok(Unit::Wait(c.clone()));
            }
            cp.store.meta().stop_keeping(self.n);
            self.frames_at = 0;
            self.phase = Phase::Uncut;
            return Ok(Unit::Again);
        };
        unit::visit();
        let frame = unit!(cp.store.meta().get(id));
        self.meta_at = Some(id);
        let at = self.frames.len();
        self.frames.push(frame);
        if let Some(c) = cp.store.meta().write_for(&self.frames[at], self.n)? {
            self.writing.push((at, c));
        }
        Ok(Unit::Again)
    }

    /// One unit of imaging the queue's pages (DESIGN.md 22.9 item 1): node pages, the value pages
    /// put aside for later, or, with `values`, value pages, written only where an image names
    /// them. A unit lands one write that ended, or takes one page's source, or builds one image,
    /// or reads one directory page of a value an image names, or starts one image's write: the
    /// flight set stays as full as its limits allow, and the unit waits only for a place in it, or
    /// for its last writes. Done once every page is imaged and every write has landed.
    fn image_unit(&mut self, cp: &Checkpointer, txns: &Transactions, values: bool) -> Result<Unit> {
        if unit!(self.land_one(cp)) {
            return Ok(Unit::Again);
        }
        loop {
            let Some(imaging) = self.imaging.as_mut() else {
                if self.flying.len() >= WRITES {
                    return Ok(Unit::Wait(self.flying[0].landed.clone()));
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
                            return Ok(match self.flying.first() {
                                None => Unit::Done,
                                Some(f) => Unit::Wait(f.landed.clone()),
                            });
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
                    unit::visit();
                    match cp.store.left_out(self.n, &self.heights, pid)? {
                        IOResult::Done(edited) => self.meta.extend(edited),
                        IOResult::IO(IOCompletions(c)) => {
                            self.queue.push_front(pid);
                            return Ok(Unit::Wait(c));
                        }
                    }
                    return Ok(Unit::Again);
                }
                unit::visit();
                let source = match kept {
                    Some(copy) => Source::Kept(copy),
                    None => match cp.pool.image_source(self.n, pid)? {
                        IOResult::Done(s) => s,
                        IOResult::IO(IOCompletions(c)) => {
                            self.queue.push_front(pid);
                            return Ok(Unit::Wait(c));
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
                return Ok(Unit::Again);
            };
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
                        unit::visit();
                        let edited = unit!(cp.store.left_out(self.n, &self.heights, pid));
                        self.meta.extend(edited);
                        self.imaging = None;
                        return Ok(Unit::Again);
                    }
                    if v.is_leaf() && v.reserving() {
                        let as_of = match &mut imaging.as_of {
                            Some(a) => a,
                            None => imaging.as_of.insert(AsOf::new(bytes, self.c)?),
                        };
                        // An arena page a unit, until every entry is judged.
                        if !unit!(as_of.step(txns, &cp.pool)) {
                            return Ok(Unit::Again);
                        }
                        let as_of = imaging.as_of.take().expect("judged");
                        let adoptable = as_of.adoptable();
                        let image = as_of.image()?;
                        let settles = live.filter(|_| adoptable || image.as_slice() == bytes);
                        imaging.image = Some((image, settles));
                        // The last arena page its versions reach was this unit's.
                        return Ok(Unit::Again);
                    }
                    imaging.image = Some((bytes.to_vec(), live));
                }
            }
            // The values the image names: their pages are mapped with it, a directory page a
            // unit.
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
                if let Some(&reference) = refs.0.get(refs.1) {
                    let pages = match unit!(value::pages(&cp.pool, reference, &|| true)) {
                        Ok(pages) => pages,
                        Err(_) => {
                            return Err(LimboError::Corrupt(format!(
                                "a value an image of {pid:?} names lost its pages"
                            )))
                        }
                    };
                    self.named.extend(pages);
                    refs.1 += 1;
                    if reference.directory() {
                        // Its directory page was read: this unit's.
                        return Ok(Unit::Again);
                    }
                    continue;
                }
            }
            return self.start_write(cp);
        }
    }

    /// Starts the write of the image built, within the flight set's bytes, one always going: a
    /// large page's writes go fewer at once.
    fn start_write(&mut self, cp: &Checkpointer) -> Result<Unit> {
        let len = self
            .imaging
            .as_ref()
            .and_then(|i| i.image.as_ref())
            .map_or(0, |i| i.0.len());
        if !self.flying.is_empty() && self.flying_bytes + len > cp.write_bytes {
            return Ok(Unit::Wait(self.flying[0].landed.clone()));
        }
        let imaging = self.imaging.take().expect("a page being imaged");
        let pid = imaging.pid;
        let source_bytes = match &imaging.source {
            Source::Live { bytes, .. } => bytes.len(),
            // Charged to the room for kept copies, not to the checkpoint.
            Source::Kept(_) => 0,
        };
        let (mut image, settles) = imaging.image.expect("built");
        // The write buffer of a node page is a frame lent from the pool, given back once the
        // write lands. A value page's, of a class a small pool holds one frame of, is not.
        let lent = pid.class().index() <= node::MAX_CLASS;
        let block = match lent {
            false => IOResult::Done(AlignedBlock::zeroed(pid.class().size())),
            true => cp.pool.lease_image(pid.class())?,
        };
        let block = match block {
            IOResult::Done(b) => b,
            IOResult::IO(IOCompletions(c)) => {
                // The image stays built.
                self.imaging = Some(Imaging {
                    image: Some((image, settles)),
                    ..imaging
                });
                return Ok(Unit::Wait(c));
            }
        };
        let slot = match cp.store.take_copy_slot(pid.class())? {
            IOResult::Done(s) => s,
            IOResult::IO(IOCompletions(c)) => {
                // The image stays built; its frame goes back meanwhile.
                drop(block);
                self.imaging = Some(Imaging {
                    image: Some((image, settles)),
                    ..imaging
                });
                return Ok(Unit::Wait(c));
            }
        };
        unit::visit();
        page::seal(&mut image[..], pid);
        let block = Arc::new(block);
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
        let landed = crate::storage::atoll::notify::wait();
        let outcome = Arc::new(Mutex::new(None));
        let told = outcome.clone();
        // Its end is told through `landed` once its request retires: the write's own completion
        // stays the IO's.
        let write = cp.store.write_slot(pid.class(), slot, buffer, move |res| {
            *told.lock() = Some(res.map(|_| ()));
        })?;
        let retired = landed.clone();
        write.on_retired(move || retired.complete(0));
        self.flying.push(Flying {
            pid,
            slot,
            #[cfg(test)]
            write,
            landed,
            outcome,
            bytes: image.len(),
            settles,
            edited: false,
        });
        counters
            .flying_writes_peak
            .fetch_max(self.flying.len(), Ordering::AcqRel);
        Ok(Unit::Again)
    }

    /// Names in the page table one image write that has landed, its place in the flight set
    /// freed: whether there was one. Fails on one that failed.
    fn land_one(&mut self, cp: &Checkpointer) -> Result<IOResult<bool>> {
        let Some(i) = self.flying.iter().position(|f| f.landed.finished()) else {
            return Ok(IOResult::Done(false));
        };
        let f = &mut self.flying[i];
        match *f.outcome.lock() {
            Some(Ok(())) => {}
            Some(Err(e)) => return Err(LimboError::CompletionError(e)),
            None => {
                return Err(LimboError::InternalError(format!(
                    "the image write of {:?} retired with no outcome",
                    f.pid
                )))
            }
        }
        if !f.edited {
            unit::visit();
            let edited = match cp.store.copied(self.n, &self.heights, f.pid, f.slot)? {
                IOResult::Done(edited) => edited,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            self.meta.extend(edited);
            f.edited = true;
            if let Some(count) = f.settles {
                cp.pool.settled(f.pid, count);
            }
        }
        self.flying_bytes -= f.bytes;
        self.flying.swap_remove(i);
        Ok(IOResult::Done(true))
    }
}

/// The intent record's payload: the checkpoint's number, then each metadata page's id.
fn intent_payload(n: u64, meta: &BTreeSet<MetaId>) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 8 * meta.len());
    out.extend_from_slice(&n.to_le_bytes());
    for id in meta {
        out.extend_from_slice(&id.pid().raw().to_le_bytes());
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The metadata pages dirty at a cut move into the ordered set a run of [`DIRTY_RUN`] at a
    /// time (DESIGN.md 22.9 item 1; mutant: the chain read whole in one unit).
    #[test]
    fn the_dirty_set_is_walked_a_run_at_a_time_into_order() {
        let stack = super::super::stack::Stack::default();
        let count = 2 * DIRTY_RUN + 1;
        // Pushed out of order, three of them twice.
        for i in (0..count as u64).rev().chain(0..3) {
            stack.push(MetaId {
                tree: super::super::meta::Tree::Map,
                class: page::SizeClass::new(0).unwrap(),
                level: 0,
                index: (i * 7919) % count as u64,
            });
        }
        let mut run = Run::new(1, None);
        run.dirty = stack.take_chain();
        let mut runs = 0;
        loop {
            let before = run.meta.len();
            let done = run.dirty_run();
            runs += 1;
            let moved = run.meta.len() - before;
            assert!(moved <= DIRTY_RUN, "a run moved {moved}");
            if done {
                break;
            }
        }
        assert_eq!(run.meta.len(), count, "every page once");
        // The chain holds three pages twice; the run that finds it empty says so.
        assert_eq!(
            runs,
            (count + 3) / DIRTY_RUN + 1,
            "runs for {} entries",
            count + 3
        );
        let ids: Vec<_> = run.meta.iter().collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "in order");
    }

    /// A database whose writers step its checkpoints, on `io`.
    fn open(io: &Arc<dyn crate::io::IO>) -> crate::storage::atoll::db::Atoll {
        use crate::storage::atoll::buffer::{BufferConfig, MIN_DB_BUDGET, WRITEBACK_BYTES};
        use crate::storage::atoll::db::{Atoll, AtollConfig};
        use crate::storage::atoll::log::LogConfig;
        use crate::storage::atoll::store::StoreConfig;
        let config = AtollConfig {
            log: LogConfig {
                segment_bytes: 1 << 20,
                buffer_bytes: 256 << 10,
                chunk_bytes: 16 << 10,
                writes_in_flight: 3,
                direct: true,
            },
            store: StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            buffer: BufferConfig {
                budget: MIN_DB_BUDGET,
                writeback_bytes: WRITEBACK_BYTES,
                kept_bytes: None,
                lent_at_open: Default::default(),
            },
            log_budget: Some(512 << 10),
            checkpoint_write_bytes: None,
            checkpointing: Checkpointing::Writers,
            txn: Default::default(),
        };
        Atoll::open(io.clone(), "/db/checkpoint-units", config).unwrap()
    }

    /// The units `run` takes in its phase from where it stands, stepped until it leaves it: a unit
    /// that goes on into the next phase counts.
    fn units_in(
        run: &mut Run,
        db: &crate::storage::atoll::db::Atoll,
        in_charge: &Superblock,
    ) -> usize {
        let phase = run.phase;
        let mut units = 0;
        while run.phase == phase {
            run.step(db.checkpointer(), db.transactions(), in_charge)
                .unwrap();
            units += 1;
        }
        units
    }

    /// The units `run` takes in its phase from where it stands, stepped until it leaves it or has
    /// taken `1 << 16`, and the most entries of bookkeeping any of them walked where the walk is
    /// done (`unit::walked`), whatever run its call site handed the walk.
    fn units_and_walk(
        run: &mut Run,
        db: &crate::storage::atoll::db::Atoll,
        in_charge: &Superblock,
    ) -> (usize, usize) {
        let phase = run.phase;
        let (mut units, mut most) = (0, 0);
        unit::take_walked();
        while run.phase == phase && units < 1 << 16 {
            run.step(db.checkpointer(), db.transactions(), in_charge)
                .unwrap();
            units += 1;
            most = most.max(unit::take_walked());
        }
        (units, most)
    }

    /// The trigger is claimed once a crossing: the first statement past a start claims it, the next
    /// for that start does not, one that read an earlier start neither claims it nor lowers it, and
    /// the next start's first does (the auditor's P2-EX-H1; mutant: the word swapped in, a stale
    /// start lowering it and the newer start claimed twice).
    #[test]
    fn the_trigger_is_claimed_once_a_crossing_and_never_for_an_earlier_start() {
        let io: Arc<dyn crate::io::IO> = Arc::new(crate::io::MemoryIO::new());
        let db = open(&io);
        let cp = db.checkpointer();
        let claims = [
            cp.claim_trigger(4096),
            cp.claim_trigger(4096),
            cp.claim_trigger(0),
            cp.claim_trigger(4096),
            cp.claim_trigger(8192),
            cp.claim_trigger(8192),
        ];
        drop(db);
        assert_eq!(claims, [true, false, false, false, true, false]);
    }

    /// Claims running beside one another: one for an earlier start, read stale, and two for the
    /// newer. One of the newer claims, and the word ends one past the newer start, whatever the
    /// interleaving: the stale claim never lowers it (the auditor's model of #4315; mutant: the
    /// word swapped in, which the stale claim lowers between the newer two, so both claim).
    fn a_stale_claim_never_lowers_the_trigger_word(rounds: usize) {
        for _ in 0..rounds {
            let word = Arc::new(TriggerWord::default());
            let claim = |began: Lsn| {
                let word = word.clone();
                crate::thread::spawn(move || word.claim(began))
            };
            let stale = claim(4096);
            let newer = [claim(8192), claim(8192)];
            let newer = newer
                .into_iter()
                .map(|h| h.join().unwrap())
                .filter(|&claimed| claimed)
                .count();
            stale.join().unwrap();
            assert_eq!(newer, 1, "claims for the newer start");
            assert_eq!(word.0.load(Ordering::SeqCst), 8193, "the word at the end");
        }
    }

    #[cfg(not(shuttle))]
    #[test]
    fn a_stale_claim_never_lowers_the_trigger_word_on_threads() {
        a_stale_claim_never_lowers_the_trigger_word(if cfg!(miri) { 20 } else { 5_000 });
    }

    #[cfg(shuttle)]
    #[test]
    fn a_stale_claim_never_lowers_the_trigger_word_in_shuttle_schedules() {
        crate::storage::atoll::testing::schedules(
            || a_stale_claim_never_lowers_the_trigger_word(1),
            2_000,
        );
    }

    /// The bookkeeping over the frames a checkpoint wrote is a run of them a unit: giving each page
    /// its durable place, then putting the checkpoint in charge, and letting go of their cut
    /// copies, each run walking no more frames than it holds, whatever its call site hands the
    /// helper; and the trees it saw created are forgotten as creations one a unit (the auditor's
    /// P2-CP-U1 and #4319; mutants: a phase walking them whole in one unit, a call site handing a
    /// helper the rest of the frames past its run).
    #[test]
    fn the_checkpoints_bookkeeping_walks_its_frames_a_run_a_unit() {
        let io: Arc<dyn crate::io::IO> = Arc::new(crate::io::MemoryIO::new());
        let db = open(&io);
        let in_charge = db.checkpointer().superblock();
        let id = MetaId {
            tree: super::super::meta::Tree::Map,
            class: page::SizeClass::new(0).unwrap(),
            level: 0,
            index: 0,
        };
        let frame =
            crate::storage::atoll::testing::drive(&io, || db.store().meta().get(id)).unwrap();
        // The checkpoint in charge again: its pages' places and its number stand as they are.
        let mut run = Run::new(in_charge.checkpoint, None);
        run.frames = (0..2 * FRAME_RUN + 1).map(|_| frame.clone()).collect();
        run.superblock = Some(in_charge.clone());
        run.phase = Phase::InCharge;
        let (in_charge_units, in_charge_walk) = units_and_walk(&mut run, &db, &in_charge);
        let after_in_charge = run.phase;
        run.phase = Phase::Uncut;
        run.frames_at = 0;
        let (uncut_units, uncut_walk) = units_and_walk(&mut run, &db, &in_charge);
        let after_uncut = run.phase;
        run.phase = Phase::Forget;
        run.mapped_roots = (0..3)
            .map(|i| PageId::new(page::SizeClass::new(0).unwrap(), 1 << 20 | i))
            .collect();
        let forget_units = units_in(&mut run, &db, &in_charge);
        drop((run, frame, db));
        assert_eq!(
            (in_charge_units, after_in_charge),
            (4, Phase::After),
            "three runs, then the checkpoint put in charge"
        );
        assert_eq!(
            (uncut_units, after_uncut),
            (4, Phase::Files),
            "three runs, then done"
        );
        assert_eq!(forget_units, 4, "three roots, then the next phase");
        assert_eq!(
            (in_charge_walk, uncut_walk),
            (FRAME_RUN, FRAME_RUN),
            "the most frames a unit's helper walked, giving places and letting cut copies go"
        );
    }

    /// The registry of trees created is read a run of roots a unit, from where the last run ended,
    /// to the end the cut took: every creation registered by then read once, each the snapshot
    /// does not see set aside, and the walk ends on that inventory although a full run of trees is
    /// created past its end after every unit (the auditor's P2-CP-U1, #4319 and #4326; mutants:
    /// the registry read whole in a unit, a run read from the registry's start again, the walk
    /// read past the end it took).
    #[test]
    fn the_registry_of_trees_created_is_read_a_run_a_unit_to_the_end_the_cut_took() {
        let io: Arc<dyn crate::io::IO> = Arc::new(crate::io::MemoryIO::new());
        let db = open(&io);
        let in_charge = db.checkpointer().superblock();
        let class = page::SizeClass::new(0).unwrap();
        let store = db.store().clone();
        let roots: Vec<PageId> = (0..2 * CREATION_RUN as u64 + 1)
            .map(|i| PageId::new(class, 1 << 20 | i))
            .collect();
        // Created by a transaction no snapshot sees: its start past every commit.
        for &root in &roots {
            store.created_unheld(root, 0, u64::MAX);
        }
        let end = store.last_creation();
        let registered = end.map_or(0, |end| store.creations_after(None, end, usize::MAX).len());
        let mut run = Run::new(in_charge.checkpoint, None);
        run.phase = Phase::Creations;
        run.creations_end = end;
        let mut later = Vec::new();
        let (mut units, mut most) = (0, 0);
        unit::take_walked();
        while run.phase == Phase::Creations && units < 1 << 12 {
            run.step(db.checkpointer(), db.transactions(), &in_charge)
                .unwrap();
            units += 1;
            most = most.max(unit::take_walked());
            // Trees created after the cut, past its end, a full run of them.
            for _ in 0..CREATION_RUN {
                let root = PageId::new(class, 1 << 21 | later.len() as u64);
                store.created_unheld(root, 0, u64::MAX);
                later.push(root);
            }
        }
        let after = run.phase;
        let set_aside = roots.iter().filter(|r| run.set_aside.contains(r)).count();
        let later_read = later.iter().filter(|r| run.set_aside.contains(r)).count();
        let read = run.set_aside.len() + run.mapped_roots.len();
        for &root in roots.iter().chain(&later) {
            store.forget_creation(root);
        }
        drop((run, store, db));
        assert_eq!(after, Phase::Dirty, "the registry's walk did not end");
        assert_eq!(
            set_aside,
            roots.len(),
            "creations no snapshot sees set aside"
        );
        assert_eq!(later_read, 0, "creations past the cut's end read");
        assert_eq!(read, registered, "creations read against those registered");
        assert!(most <= CREATION_RUN, "a unit walked {most} creations");
        assert_eq!(
            units,
            registered.div_ceil(CREATION_RUN),
            "units to read {registered} creations"
        );
    }
}
