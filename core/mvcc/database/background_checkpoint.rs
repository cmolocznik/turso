//! Background checkpoints: a pool of threads that runs the auto passive checkpoints of the
//! databases given it, so no commit pays for one.
//!
//! A commit that leaves its database's logical log past the checkpoint threshold hands the
//! checkpoint to its store's pool and returns. One pool serves any number of databases on a
//! bounded number of threads, which it starts as work comes in and stops once idle. A database
//! waits in the pool's queue at most once and is checkpointed by one thread at a time. The queue is
//! served in turn: a database still past its threshold after a slice of checkpoints goes back to
//! the end of it while another waits. The pool holds a database only by a weak reference and opens
//! a connection for each turn, so it neither keeps a closed database alive nor counts as one of its
//! connections between turns.
//!
//! A passive checkpoint truncates the log only when no commit landed after its snapshot, which a
//! checkpoint running beside writers rarely sees. So once a checkpoint has had to keep the log, the
//! pool looks at the log again on a timer, and once it has stayed the same size for `QUIET_FOR`
//! checkpoints again, and that one truncates it. A turn that fails, or that finds another
//! checkpoint running on its database, is retried after a delay that doubles each time, on a timer,
//! so it holds no thread. Should a pool fall so far behind that a database's uncheckpointed log
//! reaches `BEHIND` thresholds (`far_behind`), or its turns keep failing, that database's commits
//! checkpoint inline until it is back under, each first waiting for a checkpoint already running
//! on the database to end, so the log stays bounded whatever the pool's state and however long a
//! checkpoint takes.
//!
//! Every store starts on the process's shared pool (`CheckpointPool::shared`) and can be given
//! another: one per storage device, say.
//!
//! An encrypted database, or one read through an external page codec, keeps its inline checkpoint:
//! the key material lives only on the connections given it, and the pool would have to hold it for
//! the database's lifetime.
use crate::mvcc::persistent_storage::DurableStorage;
use crate::storage::wal::CheckpointMode;
use crate::Database;
use arc_swap::ArcSwap;
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::cmp::Ordering as Order;
use std::collections::{BinaryHeap, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

/// How long a kept log must stay the same size before it is checkpointed to truncate it.
const QUIET_FOR: Duration = Duration::from_millis(100);

/// How early a timer may fire, so that timers due close together fire on one wakeup.
const TIMER_SLACK: Duration = Duration::from_millis(10);

/// How long a thread goes on checkpointing a database still past its threshold while another
/// database waits.
const SLICE: Duration = Duration::from_millis(50);

/// The delay before retrying a failed or deferred turn, doubled for each further one in a row, up
/// to `RETRY_LONGEST`.
const RETRY_FIRST: Duration = Duration::from_millis(10);
const RETRY_LONGEST: Duration = Duration::from_secs(1);

/// The uncheckpointed log, in checkpoint thresholds, at which a database's commits checkpoint
/// inline again.
const BEHIND: u64 = 4;

/// Whether `storage`'s uncheckpointed log has reached `BEHIND` thresholds, past which its commits
/// no longer leave the checkpoint to anyone else.
pub(crate) fn far_behind(storage: &dyn DurableStorage) -> bool {
    let threshold = storage.checkpoint_threshold();
    threshold > 0 && storage.uncheckpointed_bytes() >= BEHIND.saturating_mul(threshold as u64)
}

/// How long an idle thread waits for work before it stops.
const KEEP_ALIVE: Duration = Duration::from_secs(10);

static CHECKPOINTS: AtomicU64 = AtomicU64::new(0);
static FAILURES: AtomicU64 = AtomicU64::new(0);
static MICROS: AtomicU64 = AtomicU64::new(0);
static MICROS_MAX: AtomicU64 = AtomicU64::new(0);

/// Process-wide background checkpoint counts, over every pool: (checkpoints, failures, total µs,
/// longest µs).
pub fn background_checkpoint_stats() -> (u64, u64, u64, u64) {
    (
        CHECKPOINTS.load(Ordering::Relaxed),
        FAILURES.load(Ordering::Relaxed),
        MICROS.load(Ordering::Relaxed),
        MICROS_MAX.load(Ordering::Relaxed),
    )
}

// A member's phase. Every change but a commit's `ASKED` is made under its pool's lock.
/// Not in the pool: no checkpoint owed, no timer armed.
const IDLE: u8 = 0;
/// In the queue, waiting for a thread.
const QUEUED: u8 = 1;
/// A thread is checkpointing the database.
const RUNNING: u8 = 2;
/// The last turn left a log a checkpoint could not truncate; a timer looks at it again.
const QUIET: u8 = 3;
/// A timer queues the database again after a turn that failed or was deferred.
const RETRY: u8 = 4;
/// The thread that ran a turn decides what comes next.
const DECIDING: u8 = 5;
/// Set by a commit asking for a checkpoint. Being a read-modify-write, it orders the commit's log
/// write before the look the thread deciding a turn's end takes at the log.
const ASKED: u8 = 0x80;

/// A pool of threads that runs the background checkpoints of the databases given it, at most
/// `thread_limit` at a time. Clones are handles to the same pool.
#[derive(Clone)]
pub struct CheckpointPool {
    pool: Arc<Pool>,
}

/// What a pool is doing, and what it has done since it was made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckpointPoolStats {
    /// The most checkpoints the pool runs at a time.
    pub thread_limit: usize,
    /// Its threads now, and the most it has had at a time.
    pub threads: usize,
    pub threads_peak: usize,
    /// The threads checkpointing now.
    pub busy: usize,
    /// The databases waiting for a thread.
    pub queued: usize,
    /// The databases the pool will look at again on a timer: a kept log to truncate once its
    /// writes pause, or a turn to retry.
    pub timed: usize,
    /// Checkpoints that ran, and that failed.
    pub checkpoints: u64,
    pub failures: u64,
    /// Turns given up because another checkpoint was running on the database.
    pub deferrals: u64,
    /// Commits that checkpointed inline because the pool had fallen behind on their database.
    pub assists: u64,
    /// Turns that panicked. The database's commits checkpoint inline from then on.
    pub panics: u64,
    /// Time spent checkpointing, and the longest checkpoint, in µs.
    pub checkpoint_micros: u64,
    pub checkpoint_micros_max: u64,
    /// Time databases waited in the queue for a thread, and the longest wait, in µs.
    pub queue_micros: u64,
    pub queue_micros_max: u64,
}

struct Pool {
    schedule: Mutex<Schedule>,
    /// Idle threads wait here for work.
    work: Condvar,
    /// The idle thread keeping the timers waits here for the next one.
    tick: Condvar,
    thread_limit: AtomicUsize,
    keep_alive: Duration,
    /// How long a kept log must stay the same size before it is checkpointed to truncate it.
    quiet_for: Duration,
    /// The databases in the queue, read without the lock.
    queued: AtomicUsize,
    checkpoints: AtomicU64,
    failures: AtomicU64,
    deferrals: AtomicU64,
    assists: AtomicU64,
    panics: AtomicU64,
    checkpoint_micros: AtomicU64,
    checkpoint_micros_max: AtomicU64,
    queue_micros: AtomicU64,
    queue_micros_max: AtomicU64,
}

#[derive(Default)]
struct Schedule {
    ready: VecDeque<Arc<Member>>,
    /// Earliest first. An entry whose member has since armed another timer, or left its timed
    /// phase, is stale and dropped when reached.
    timers: BinaryHeap<Timer>,
    /// The members whose timer is live.
    timed: usize,
    threads: usize,
    threads_peak: usize,
    /// Threads waiting on `work`, not yet woken.
    waiting: usize,
    /// Whether a thread waits on `tick` for the next timer, not yet woken.
    keeper: bool,
    spawned: u64,
    armed: u64,
}

struct Timer {
    at: Instant,
    /// Orders timers due at the same instant by when they were armed.
    seq: u64,
    member: Arc<Member>,
    generation: u64,
}

impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}

impl Eq for Timer {}

impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<Order> {
        Some(self.cmp(other))
    }
}

impl Ord for Timer {
    /// Reversed, so that the heap's top is the earliest.
    fn cmp(&self, other: &Self) -> Order {
        (other.at, other.seq).cmp(&(self.at, self.seq))
    }
}

/// A store's place in its pool.
struct Member {
    enabled: AtomicBool,
    /// The store's database, set by its first request; `None` inside when the database needs key
    /// material and keeps its inline checkpoint.
    database: OnceLock<Option<Weak<Database>>>,
    storage: OnceLock<Weak<dyn DurableStorage>>,
    phase: AtomicU8,
    /// Set once the store is dropped or moved to another pool.
    closed: AtomicBool,
    /// Taken only under the pool's lock.
    slot: Mutex<Slot>,
    checkpoints: AtomicU64,
    failures: AtomicU64,
    deferrals: AtomicU64,
    assists: AtomicU64,
    #[cfg(test)]
    hooks: Mutex<TestHooks>,
}

#[derive(Default)]
struct Slot {
    /// The generation of the member's timer; an entry of an earlier one is stale.
    generation: u64,
    /// The log's offset when a checkpoint kept it, or at the last look since.
    kept: u64,
    /// Turns in a row that failed or were deferred.
    strikes: u32,
    queued_at: Option<Instant>,
}

/// How a turn ended.
enum Outcome {
    Done,
    Failed,
    /// Another checkpoint was running on the database.
    Deferred,
    /// The database is gone.
    Gone,
    Panicked,
}

#[cfg(test)]
#[derive(Default)]
struct TestHooks {
    injector: Option<Arc<dyn crate::mvcc::yield_points::YieldInjector>>,
    failure_injector: Option<Arc<dyn crate::mvcc::yield_points::FailureInjector>>,
}

impl CheckpointPool {
    /// A pool that runs at most `thread_limit` checkpoints at a time. With a limit of zero every
    /// checkpoint runs inline.
    pub fn new(thread_limit: usize) -> Self {
        Self::with_timing(thread_limit, KEEP_ALIVE, QUIET_FOR)
    }

    pub(crate) fn with_timing(
        thread_limit: usize,
        keep_alive: Duration,
        quiet_for: Duration,
    ) -> Self {
        Self {
            pool: Arc::new(Pool {
                schedule: Mutex::new(Schedule::default()),
                work: Condvar::new(),
                tick: Condvar::new(),
                thread_limit: AtomicUsize::new(thread_limit),
                keep_alive,
                quiet_for,
                queued: AtomicUsize::new(0),
                checkpoints: AtomicU64::new(0),
                failures: AtomicU64::new(0),
                deferrals: AtomicU64::new(0),
                assists: AtomicU64::new(0),
                panics: AtomicU64::new(0),
                checkpoint_micros: AtomicU64::new(0),
                checkpoint_micros_max: AtomicU64::new(0),
                queue_micros: AtomicU64::new(0),
                queue_micros_max: AtomicU64::new(0),
            }),
        }
    }

    /// The process's shared pool, which every store starts on. It runs a quarter of the available
    /// parallelism at a time, at least one checkpoint and at most eight.
    pub fn shared() -> Self {
        static SHARED: OnceLock<CheckpointPool> = OnceLock::new();
        SHARED
            .get_or_init(|| {
                let threads = std::thread::available_parallelism()
                    .map_or(1, |threads| (threads.get() / 4).clamp(1, 8));
                CheckpointPool::new(threads)
            })
            .clone()
    }

    pub fn thread_limit(&self) -> usize {
        self.pool.thread_limit.load(Ordering::Acquire)
    }

    /// Threads past a lower limit stop once their turn ends. At zero the pool lets go of the
    /// databases waiting in it, and their commits checkpoint inline.
    pub fn set_thread_limit(&self, thread_limit: usize) {
        let pool = &self.pool;
        pool.thread_limit.store(thread_limit, Ordering::Release);
        let mut schedule = pool.schedule.lock();
        if thread_limit == 0 {
            for member in std::mem::take(&mut schedule.ready) {
                member.phase.store(IDLE, Ordering::Release);
            }
            pool.queued.store(0, Ordering::Relaxed);
            for timer in std::mem::take(&mut schedule.timers) {
                let mut slot = timer.member.slot.lock();
                if timer.member.timer_is_live(&slot, timer.generation) {
                    slot.generation += 1;
                    timer.member.phase.store(IDLE, Ordering::Release);
                }
            }
            schedule.timed = 0;
        }
        pool.work.notify_all();
        pool.tick.notify_all();
        schedule.waiting = 0;
        schedule.keeper = false;
        let room = thread_limit.saturating_sub(schedule.threads);
        for _ in 0..room.min(schedule.ready.len()) {
            if !pool.spawn(&mut schedule) {
                break;
            }
        }
    }

    pub fn stats(&self) -> CheckpointPoolStats {
        let pool = &self.pool;
        let schedule = pool.schedule.lock();
        CheckpointPoolStats {
            thread_limit: pool.thread_limit.load(Ordering::Acquire),
            threads: schedule.threads,
            threads_peak: schedule.threads_peak,
            busy: schedule.threads - schedule.waiting - usize::from(schedule.keeper),
            queued: schedule.ready.len(),
            timed: schedule.timed,
            checkpoints: pool.checkpoints.load(Ordering::Relaxed),
            failures: pool.failures.load(Ordering::Relaxed),
            deferrals: pool.deferrals.load(Ordering::Relaxed),
            assists: pool.assists.load(Ordering::Relaxed),
            panics: pool.panics.load(Ordering::Relaxed),
            checkpoint_micros: pool.checkpoint_micros.load(Ordering::Relaxed),
            checkpoint_micros_max: pool.checkpoint_micros_max.load(Ordering::Relaxed),
            queue_micros: pool.queue_micros.load(Ordering::Relaxed),
            queue_micros_max: pool.queue_micros_max.load(Ordering::Relaxed),
        }
    }
}

impl std::fmt::Debug for CheckpointPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointPool")
            .field("thread_limit", &self.thread_limit())
            .finish()
    }
}

impl Pool {
    /// Asks for a checkpoint of `member`'s database. False when the pool cannot take it.
    fn request(self: &Arc<Self>, member: &Arc<Member>) -> bool {
        let phase = member.phase.fetch_or(ASKED, Ordering::AcqRel) & !ASKED;
        if matches!(phase, QUEUED | RUNNING | RETRY) {
            return true;
        }
        let mut schedule = self.schedule.lock();
        let mut slot = member.slot.lock();
        let phase = member.phase.load(Ordering::Acquire) & !ASKED;
        if matches!(phase, QUEUED | RUNNING | RETRY) {
            return true;
        }
        if member.closed.load(Ordering::Acquire) || self.thread_limit.load(Ordering::Acquire) == 0 {
            member.phase.store(IDLE, Ordering::Release);
            return false;
        }
        if phase == QUIET {
            self.cancel_timer(&mut schedule, &mut slot);
        }
        if self.enqueue(&mut schedule, member, &mut slot) {
            return true;
        }
        schedule.ready.pop_back();
        self.queued.store(schedule.ready.len(), Ordering::Relaxed);
        member.phase.store(IDLE, Ordering::Release);
        false
    }

    /// Takes `member` out of the pool for good. A turn running on it ends idle.
    fn close(&self, member: &Arc<Member>) {
        member.closed.store(true, Ordering::Release);
        let mut schedule = self.schedule.lock();
        let mut slot = member.slot.lock();
        match member.phase.load(Ordering::Acquire) & !ASKED {
            QUEUED => {
                schedule.ready.retain(|queued| !Arc::ptr_eq(queued, member));
                self.queued.store(schedule.ready.len(), Ordering::Relaxed);
                member.phase.store(IDLE, Ordering::Release);
            }
            QUIET | RETRY => {
                self.cancel_timer(&mut schedule, &mut slot);
                member.phase.store(IDLE, Ordering::Release);
            }
            _ => {}
        }
    }

    /// Puts `member` at the end of the queue and sees that a thread will take it. False when no
    /// thread can.
    fn enqueue(
        self: &Arc<Self>,
        schedule: &mut Schedule,
        member: &Arc<Member>,
        slot: &mut Slot,
    ) -> bool {
        member.phase.store(QUEUED, Ordering::Release);
        schedule.ready.push_back(member.clone());
        self.queued.store(schedule.ready.len(), Ordering::Relaxed);
        if !self.wake_for_work(schedule) {
            return false;
        }
        slot.queued_at = Some(Instant::now());
        true
    }

    /// Wakes an idle thread, or starts one under the limit, for the queue. False when no thread
    /// will serve it.
    fn wake_for_work(self: &Arc<Self>, schedule: &mut Schedule) -> bool {
        if schedule.waiting > 0 && self.work.notify_one() {
            schedule.waiting -= 1;
            return true;
        }
        if schedule.keeper && self.tick.notify_one() {
            schedule.keeper = false;
            return true;
        }
        if schedule.threads < self.thread_limit.load(Ordering::Acquire) && self.spawn(schedule) {
            return true;
        }
        schedule.threads > 0
    }

    fn spawn(self: &Arc<Self>, schedule: &mut Schedule) -> bool {
        schedule.spawned += 1;
        let pool = self.clone();
        let spawned = std::thread::Builder::new()
            .name(format!("turso-ckpt-{}", schedule.spawned))
            .spawn(move || pool.serve());
        match spawned {
            Ok(_) => {
                schedule.threads += 1;
                schedule.threads_peak = schedule.threads_peak.max(schedule.threads);
                true
            }
            Err(err) => {
                tracing::warn!("MVCC background checkpoint thread did not start: {err}");
                false
            }
        }
    }

    /// Arms `member`'s timer to fire `after` from now, in `phase`.
    fn arm(
        &self,
        schedule: &mut Schedule,
        member: &Arc<Member>,
        slot: &mut Slot,
        phase: u8,
        after: Duration,
    ) {
        let at = Instant::now() + after;
        slot.generation += 1;
        schedule.armed += 1;
        let earliest = schedule.timers.peek().map(|timer| timer.at);
        schedule.timers.push(Timer {
            at,
            seq: schedule.armed,
            member: member.clone(),
            generation: slot.generation,
        });
        schedule.timed += 1;
        member.phase.store(phase, Ordering::Release);
        if earliest.is_some_and(|earliest| earliest <= at) && schedule.keeper {
            return;
        }
        // The keeper waits for a later timer, or no thread keeps the timers.
        if schedule.keeper {
            if self.tick.notify_one() {
                schedule.keeper = false;
            }
        } else if schedule.waiting > 0 && self.work.notify_one() {
            schedule.waiting -= 1;
        }
    }

    fn cancel_timer(&self, schedule: &mut Schedule, slot: &mut Slot) {
        slot.generation += 1;
        schedule.timed -= 1;
        if schedule.timed == 0 {
            schedule.timers.clear();
        }
    }

    /// The earliest live timer, dropping the stale entries before it.
    fn next_timer(schedule: &mut Schedule) -> Option<Instant> {
        while let Some(timer) = schedule.timers.peek() {
            let live = timer
                .member
                .timer_is_live(&timer.member.slot.lock(), timer.generation);
            if live {
                return Some(timer.at);
            }
            schedule.timers.pop();
        }
        None
    }

    /// Fires the timers due: a retry is queued, and a kept log that has stayed the same size since
    /// the last look is queued to be truncated.
    fn fire_timers(self: &Arc<Self>, schedule: &mut Schedule) {
        let due = Instant::now() + TIMER_SLACK;
        while schedule.timers.peek().is_some_and(|timer| timer.at <= due) {
            let Some(Timer {
                member, generation, ..
            }) = schedule.timers.pop()
            else {
                break;
            };
            let mut slot = member.slot.lock();
            if !member.timer_is_live(&slot, generation) {
                continue;
            }
            schedule.timed -= 1;
            if member.phase.load(Ordering::Acquire) & !ASKED == RETRY {
                self.enqueue(schedule, &member, &mut slot);
                continue;
            }
            // The log's storage alone is read here, so no database is dropped under the lock.
            let offset = member
                .storage
                .get()
                .and_then(Weak::upgrade)
                .map(|storage| storage.logical_log_offset());
            match offset {
                Some(offset) if offset > 0 && offset != slot.kept => {
                    slot.kept = offset;
                    self.arm(schedule, &member, &mut slot, QUIET, self.quiet_for);
                }
                Some(offset) if offset > 0 => {
                    self.enqueue(schedule, &member, &mut slot);
                }
                _ => member.phase.store(IDLE, Ordering::Release),
            }
        }
        if schedule.timed == 0 {
            schedule.timers.clear();
        }
    }

    /// A pool thread: takes the timers due and the queue in turn, and stops once idle for
    /// `keep_alive`, or past the thread limit.
    fn serve(self: Arc<Self>) {
        let mut schedule = self.schedule.lock();
        loop {
            self.fire_timers(&mut schedule);
            if schedule.threads > self.thread_limit.load(Ordering::Acquire) {
                break;
            }
            if let Some(member) = schedule.ready.pop_front() {
                self.queued.store(schedule.ready.len(), Ordering::Relaxed);
                self.start(&member);
                let outcome = MutexGuard::unlocked(&mut schedule, || {
                    catch_unwind(AssertUnwindSafe(|| self.turn(&member))).unwrap_or_else(|_| {
                        tracing::error!(
                            "MVCC background checkpoint panicked; the database checkpoints inline from now on"
                        );
                        Outcome::Panicked
                    })
                });
                self.finish(&mut schedule, &member, outcome);
                continue;
            }
            if !schedule.keeper {
                if let Some(at) = Self::next_timer(&mut schedule) {
                    schedule.keeper = true;
                    if self.tick.wait_until(&mut schedule, at).timed_out() {
                        schedule.keeper = false;
                    }
                    continue;
                }
            }
            schedule.waiting += 1;
            if self
                .work
                .wait_for(&mut schedule, self.keep_alive)
                .timed_out()
            {
                schedule.waiting -= 1;
                if schedule.ready.is_empty() && (schedule.keeper || schedule.timed == 0) {
                    break;
                }
            }
        }
        schedule.threads -= 1;
    }

    fn start(&self, member: &Member) {
        let mut slot = member.slot.lock();
        if let Some(queued_at) = slot.queued_at.take() {
            let micros = queued_at.elapsed().as_micros() as u64;
            self.queue_micros.fetch_add(micros, Ordering::Relaxed);
            self.queue_micros_max.fetch_max(micros, Ordering::Relaxed);
        }
        member.phase.store(RUNNING, Ordering::Release);
    }

    /// Checkpoints `member`'s database until it is no longer past its threshold, or for a slice
    /// while another database waits. Runs without the pool's lock, and drops every handle to the
    /// database before it returns.
    fn turn(&self, member: &Member) -> Outcome {
        let Some(db) = member
            .database
            .get()
            .and_then(Option::as_ref)
            .and_then(Weak::upgrade)
        else {
            return Outcome::Gone;
        };
        let Some(store) = db.get_mv_store().as_ref().cloned() else {
            return Outcome::Gone;
        };
        let conn = match db.connect_internal(None, None) {
            Ok(conn) => conn,
            Err(err) => {
                tracing::info!("MVCC background checkpoint could not connect: {err}");
                self.count(member, None);
                return Outcome::Failed;
            }
        };
        #[cfg(test)]
        member.hooks.lock().install(&conn);
        let began = Instant::now();
        loop {
            let started = Instant::now();
            let result = conn.checkpoint(CheckpointMode::Passive {
                upper_bound_inclusive: None,
            });
            // Another checkpoint is running on the database: it gets the turn.
            if matches!(result, Err(crate::LimboError::Busy)) {
                return self.defer(member);
            }
            self.count(member, Some((started.elapsed(), result.is_ok())));
            if let Err(err) = result {
                tracing::info!("MVCC background checkpoint failed: {err}");
                return Outcome::Failed;
            }
            if !owes_checkpoint(store.storage.as_ref())
                || (began.elapsed() >= SLICE && self.queued.load(Ordering::Relaxed) > 0)
            {
                return Outcome::Done;
            }
        }
    }

    fn defer(&self, member: &Member) -> Outcome {
        member.deferrals.fetch_add(1, Ordering::Relaxed);
        self.deferrals.fetch_add(1, Ordering::Relaxed);
        Outcome::Deferred
    }

    /// Counts a checkpoint that took `took` and succeeded or not; `None` for a turn that failed
    /// before checkpointing.
    fn count(&self, member: &Member, checkpoint: Option<(Duration, bool)>) {
        let succeeded = match checkpoint {
            Some((took, succeeded)) => {
                let micros = took.as_micros() as u64;
                MICROS.fetch_add(micros, Ordering::Relaxed);
                MICROS_MAX.fetch_max(micros, Ordering::Relaxed);
                self.checkpoint_micros.fetch_add(micros, Ordering::Relaxed);
                self.checkpoint_micros_max
                    .fetch_max(micros, Ordering::Relaxed);
                succeeded
            }
            None => false,
        };
        let (global, pool, own) = if succeeded {
            (&CHECKPOINTS, &self.checkpoints, &member.checkpoints)
        } else {
            (&FAILURES, &self.failures, &member.failures)
        };
        global.fetch_add(1, Ordering::Relaxed);
        pool.fetch_add(1, Ordering::Relaxed);
        own.fetch_add(1, Ordering::Release);
    }

    /// Decides what follows `member`'s turn: the end of the queue while its database is still past
    /// its threshold, a timer to truncate a kept log once its writes pause or to retry, or idle.
    fn finish(self: &Arc<Self>, schedule: &mut Schedule, member: &Arc<Member>, outcome: Outcome) {
        // Every commit that asked during the turn asked before this swap, and its log write is in
        // what is read below; every later one finds the decision made.
        member.phase.swap(DECIDING, Ordering::AcqRel);
        let mut slot = member.slot.lock();
        if matches!(outcome, Outcome::Panicked) {
            self.panics.fetch_add(1, Ordering::Relaxed);
            member.enabled.store(false, Ordering::Release);
        }
        let storage = member.storage.get().and_then(Weak::upgrade).filter(|_| {
            member.enabled.load(Ordering::Acquire)
                && !member.closed.load(Ordering::Acquire)
                && self.thread_limit.load(Ordering::Acquire) > 0
        });
        let Some(storage) = storage else {
            member.phase.store(IDLE, Ordering::Release);
            return;
        };
        match outcome {
            Outcome::Done => {
                slot.strikes = 0;
                if owes_checkpoint(storage.as_ref()) {
                    self.enqueue(schedule, member, &mut slot);
                    return;
                }
                let offset = storage.logical_log_offset();
                if offset > 0 {
                    slot.kept = offset;
                    self.arm(schedule, member, &mut slot, QUIET, self.quiet_for);
                } else {
                    member.phase.store(IDLE, Ordering::Release);
                }
            }
            Outcome::Failed | Outcome::Deferred => {
                slot.strikes = slot.strikes.saturating_add(1);
                let delay = RETRY_FIRST
                    .saturating_mul(1 << (slot.strikes - 1).min(16))
                    .min(RETRY_LONGEST);
                self.arm(schedule, member, &mut slot, RETRY, delay);
            }
            Outcome::Gone | Outcome::Panicked => member.phase.store(IDLE, Ordering::Release),
        }
    }
}

/// Whether the log holds uncheckpointed bytes past its threshold. At a threshold of zero an empty
/// log is not past it.
fn owes_checkpoint(storage: &dyn DurableStorage) -> bool {
    storage.should_checkpoint() && storage.uncheckpointed_bytes() > 0
}

impl Member {
    fn new(enabled: bool) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
            database: OnceLock::new(),
            storage: OnceLock::new(),
            phase: AtomicU8::new(IDLE),
            closed: AtomicBool::new(false),
            slot: Mutex::new(Slot::default()),
            checkpoints: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            deferrals: AtomicU64::new(0),
            assists: AtomicU64::new(0),
            #[cfg(test)]
            hooks: Mutex::new(TestHooks::default()),
        }
    }

    /// A member for the same store in another pool.
    fn moved(&self) -> Self {
        let member = Self::new(self.enabled.load(Ordering::Acquire));
        if let Some(database) = self.database.get() {
            let _ = member.database.set(database.clone());
        }
        if let Some(storage) = self.storage.get() {
            let _ = member.storage.set(storage.clone());
        }
        member
    }

    /// Records the store's database on its first request. False when the database needs key
    /// material, or cannot be found.
    fn register(
        &self,
        storage: &Arc<dyn DurableStorage>,
        database: impl FnOnce() -> Option<Arc<Database>>,
    ) -> bool {
        if let Some(database) = self.database.get() {
            return database.is_some();
        }
        let Some(db) = database() else {
            return false;
        };
        let _ = self.storage.set(Arc::downgrade(storage));
        self.database
            .get_or_init(|| (!db.needs_key_material()).then(|| Arc::downgrade(&db)))
            .is_some()
    }

    fn timer_is_live(&self, slot: &Slot, generation: u64) -> bool {
        slot.generation == generation
            && matches!(self.phase.load(Ordering::Acquire) & !ASKED, QUIET | RETRY)
    }
}

/// A store's background checkpoint: whether it is on, and the pool that runs it.
pub(crate) struct BackgroundCheckpoint {
    binding: ArcSwap<Binding>,
}

struct Binding {
    pool: CheckpointPool,
    member: Arc<Member>,
}

impl BackgroundCheckpoint {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            binding: ArcSwap::from_pointee(Binding {
                pool: CheckpointPool::shared(),
                member: Arc::new(Member::new(enabled)),
            }),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.binding.load().member.enabled.load(Ordering::Acquire)
    }

    /// Off, commits past the threshold checkpoint inline again.
    pub(crate) fn set_enabled(&self, enabled: bool) {
        self.binding
            .load()
            .member
            .enabled
            .store(enabled, Ordering::Release);
    }

    pub(crate) fn pool(&self) -> CheckpointPool {
        self.binding.load().pool.clone()
    }

    pub(crate) fn set_pool(&self, pool: CheckpointPool) {
        let previous = self.binding.load_full();
        if Arc::ptr_eq(&previous.pool.pool, &pool.pool) {
            return;
        }
        self.binding.store(Arc::new(Binding {
            pool,
            member: Arc::new(previous.member.moved()),
        }));
        previous.pool.pool.close(&previous.member);
    }

    /// Hands a commit's checkpoint to the pool. False when the background checkpoint is off, the
    /// database keeps its inline checkpoint, or the pool cannot take it; `database` finds it on the
    /// first request.
    pub(crate) fn request(
        &self,
        storage: &Arc<dyn DurableStorage>,
        database: impl FnOnce() -> Option<Arc<Database>>,
    ) -> bool {
        let binding = self.binding.load();
        let member = &binding.member;
        if !member.enabled.load(Ordering::Acquire) || !member.register(storage, database) {
            return false;
        }
        binding.pool.pool.request(member)
    }

    /// Counts a commit that checkpointed inline because the pool had fallen `BEHIND`.
    pub(crate) fn count_assist(&self) {
        let binding = self.binding.load();
        if binding.member.enabled.load(Ordering::Acquire) {
            binding.member.assists.fetch_add(1, Ordering::Relaxed);
            binding.pool.pool.assists.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    pub(crate) fn probe(&self) -> BackgroundCheckpointProbe {
        BackgroundCheckpointProbe {
            member: self.binding.load().member.clone(),
        }
    }
}

impl std::fmt::Debug for BackgroundCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let binding = self.binding.load();
        f.debug_struct("BackgroundCheckpoint")
            .field("enabled", &binding.member.enabled.load(Ordering::Acquire))
            .field("pool", &binding.pool)
            .finish()
    }
}

impl Drop for BackgroundCheckpoint {
    fn drop(&mut self) {
        let binding = self.binding.load();
        binding.pool.pool.close(&binding.member);
    }
}

/// A view of one store's place in its pool that outlives the store, for tests.
#[cfg(test)]
pub(crate) struct BackgroundCheckpointProbe {
    member: Arc<Member>,
}

#[cfg(test)]
impl BackgroundCheckpointProbe {
    pub(crate) fn checkpoints(&self) -> u64 {
        self.member.checkpoints.load(Ordering::Acquire)
    }

    pub(crate) fn failures(&self) -> u64 {
        self.member.failures.load(Ordering::Acquire)
    }

    pub(crate) fn deferrals(&self) -> u64 {
        self.member.deferrals.load(Ordering::Acquire)
    }

    pub(crate) fn assists(&self) -> u64 {
        self.member.assists.load(Ordering::Acquire)
    }

    /// Whether the pool holds the database: queued, checkpointing it, or with a timer armed.
    pub(crate) fn scheduled(&self) -> bool {
        self.member.phase.load(Ordering::Acquire) & !ASKED != IDLE
    }

    /// Gives every later turn's connection `injector`.
    pub(crate) fn set_injector(
        &self,
        injector: Option<Arc<dyn crate::mvcc::yield_points::YieldInjector>>,
    ) {
        self.member.hooks.lock().injector = injector;
    }

    /// Gives every later turn's connection `injector`.
    pub(crate) fn set_failure_injector(
        &self,
        injector: Option<Arc<dyn crate::mvcc::yield_points::FailureInjector>>,
    ) {
        self.member.hooks.lock().failure_injector = injector;
    }
}

#[cfg(test)]
impl TestHooks {
    /// Gives a turn's new connection the injectors set, if any.
    fn install(&self, conn: &crate::Connection) {
        if let Some(injector) = &self.injector {
            conn.set_yield_injector(Some(injector.clone()));
        }
        if let Some(injector) = &self.failure_injector {
            conn.set_failure_injector(Some(injector.clone()));
        }
    }
}

#[cfg(test)]
mod tests;
