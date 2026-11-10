//! The executor (DESIGN.md 22): every background job of a database is a task one executor of the
//! database's own steps, which owns its scheduling and every wake. This is its core (22.16 step
//! 6, first part): a task's header and its state word (22.3), the run queue by class (22.4), the
//! workers that run it, and the named claims callers make (22.7), proved on test jobs; no job of
//! the database moves onto it yet.
//!
//! A task is stepped by one runner at a time. Its header holds its state word, its class and weak
//! references to the executor and to its job, and a waker holds the header alone, so a waker left
//! on a completion keeps neither the job nor the database alive. Every move into or out of
//! `QUEUED` happens under the queue's mutex, with the link or unlink it implies, so a task is
//! linked exactly while its word says so; the moves among `IDLE`, `RUNNING`, `RUNNING|NOTIFIED`
//! and `DONE` are compare-exchanges. A wake while a step runs sets `NOTIFIED`, and the step's end
//! then requeues the task instead of resting it, so no wake is lost and none doubles the task.

use self::sync::{lock, AtomicBool, AtomicU8, Mutex, Ordering};
use crate::io::{Completion, Delivery, Stepped, IO};
use crate::Result;
use std::collections::VecDeque;
use std::sync::{Arc, Weak};

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::storage::atoll::locks::{Mutex, MutexGuard};
    pub(super) use crate::sync::atomic::{AtomicBool, AtomicU8, Ordering};
    pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock()
    }
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicBool, AtomicU8, Ordering};
    pub(super) use loom::sync::{Mutex, MutexGuard};
    pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().unwrap()
    }
}

mod signal;
#[cfg(test)]
mod tests;

pub(crate) use signal::{Signal, Watched};

/// What one step of a task says (22.1).
pub(crate) enum Step {
    /// The job is over: nothing steps it again, and a later wake finds it done.
    Done,
    /// More to do now, after the others queued have run.
    Again,
    /// Nothing to do until `c` finishes, whose finish wakes the task.
    Wait(Completion),
    /// Nothing to do until one of these signals moves from the count read before the look (22.2).
    Idle(Watched),
}

/// A job the executor steps (22.1): one unit of its work per step, never waiting inside one; its
/// progress lives in its own state, which only the step touches.
pub(crate) trait Task: Send {
    fn step(&mut self) -> Result<Step>;
}

/// A task's class, in the order runners take them (22.4): strictly by class, first in first out
/// within one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Class {
    /// A writer waits for it.
    Relief = 0,
    /// Its own trigger fired.
    Due = 1,
    /// Collection, compaction of idle slots.
    Maintenance = 2,
}

const CLASSES: usize = 3;

/// Not queued, not running: a wake queues it.
const IDLE: u8 = 0;
/// Linked into its class's list, under the queue's mutex.
const QUEUED: u8 = 1;
/// A runner steps it.
const RUNNING: u8 = 2;
/// Beside [`RUNNING`]: woken while it ran, so its step's end queues it again.
const NOTIFIED: u8 = 4;
/// Its job is over.
const DONE: u8 = 8;
/// The executor closed it.
const CLOSED: u8 = 16;

/// A task as wakers and the queue know it (22.3): no reference to its job's state but a weak one,
/// none to the executor but a weak one.
struct Header {
    state: AtomicU8,
    /// Its class, read and written under the queue's mutex only.
    class: AtomicU8,
    shared: Weak<Shared>,
    job: Weak<Job>,
}

/// A task's job, held by the executor's table while the task lives.
struct Job {
    header: Arc<Header>,
    task: Mutex<Box<dyn Task>>,
    /// The signals it may watch, which take it off their lists when it goes.
    watches: Vec<Arc<Signal>>,
}

impl std::task::Wake for Header {
    fn wake(self: Arc<Self>) {
        wake(&self);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        wake(self);
    }
}

/// Wakes `header`'s task (22.3): a running one is marked to run again, an idle one is queued under
/// the mutex with its link, any other is left as it is.
fn wake(header: &Arc<Header>) {
    let mut state = header.state.load(Ordering::Acquire);
    loop {
        match state {
            RUNNING => match header.state.compare_exchange(
                RUNNING,
                RUNNING | NOTIFIED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(now) => state = now,
            },
            IDLE => {
                let Some(shared) = header.shared.upgrade() else {
                    return;
                };
                let mut queue = lock(&shared.queue);
                match header.state.compare_exchange(
                    IDLE,
                    QUEUED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        queue.link(header);
                        return;
                    }
                    Err(now) => state = now,
                }
            }
            _ => return,
        }
    }
}

/// The run queue (22.3): one list per class, and the workers parked with nothing to claim.
struct Queue {
    lists: [VecDeque<Arc<Header>>; CLASSES],
    idle: VecDeque<crate::thread::Thread>,
    /// The tasks the executor holds, which every list has room for, so a link never allocates.
    tasks: usize,
    closing: bool,
}

impl Queue {
    /// Links a task just moved to `QUEUED` at the back of its class, and unparks one idle worker.
    fn link(&mut self, header: &Arc<Header>) {
        let class = header.class.load(Ordering::Relaxed) as usize;
        self.lists[class].push_back(header.clone());
        if let Some(worker) = self.idle.pop_front() {
            worker.unpark();
        }
    }

    /// The head of the highest non-empty class, moved to `RUNNING`.
    fn claim(&mut self) -> Option<Arc<Header>> {
        if self.closing {
            return None;
        }
        let header = self.lists.iter_mut().find_map(VecDeque::pop_front)?;
        let was = header.state.swap(RUNNING, Ordering::AcqRel);
        assert_eq!(was, QUEUED, "a task claimed that was not queued");
        Some(header)
    }
}

struct Shared {
    queue: Mutex<Queue>,
    /// Set once close begins, before it wakes the workers: each pass of a worker's loop reads it,
    /// so a worker that never comes to park, on a backend that keeps delivering, still leaves.
    closed: AtomicBool,
    /// Every task's job, until the task is done or closed.
    jobs: Mutex<Vec<Arc<Job>>>,
    io: Arc<dyn IO>,
}

impl Shared {
    /// Claims the next task and steps it once; whether there was one.
    fn run_one(self: &Arc<Self>) -> bool {
        let Some(header) = lock(&self.queue).claim() else {
            return false;
        };
        let Some(job) = header.job.upgrade() else {
            // Removed with its subject while queued: nothing to step.
            return true;
        };
        let step = lock(&job.task).step();
        self.end(&header, step);
        true
    }

    /// The end of a step (22.3), from `RUNNING` or `RUNNING|NOTIFIED`.
    fn end(self: &Arc<Self>, header: &Arc<Header>, step: Result<Step>) {
        match step {
            // An error ends the task as its job's end does: nothing restarts it.
            Ok(Step::Done) | Err(_) => self.finish(header),
            Ok(Step::Again) => self.requeue(header),
            Ok(Step::Wait(c)) => {
                // Registered while running: a finish from now on marks the task notified.
                c.set_waker(&std::task::Waker::from(header.clone()));
                self.rest(header);
            }
            Ok(Step::Idle(Watched(watched))) => {
                // Registered while running: a notify from now on marks the task notified, and
                // one between its look and its registration has moved a count.
                let mut moved = false;
                for (signal, read) in watched.iter().flatten() {
                    moved |= !signal.register(header, *read);
                }
                if moved {
                    self.requeue(header);
                } else {
                    self.rest(header);
                }
            }
        }
    }

    /// A task that waits: from `RUNNING` to `IDLE` by one compare-exchange, or, woken while it
    /// ran, queued again.
    fn rest(&self, header: &Arc<Header>) {
        match header
            .state
            .compare_exchange(RUNNING, IDLE, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(CLOSED) => {}
            Err(_) => self.requeue(header),
        }
    }

    /// Queues a task again at the back of its class, unless the executor closed it.
    fn requeue(&self, header: &Arc<Header>) {
        let mut queue = lock(&self.queue);
        if header.state.load(Ordering::Acquire) == CLOSED {
            return;
        }
        // Only this runner moves the task out of running, and close moves it under this mutex;
        // a wake meanwhile either marked it first, which the swap replaces, or finds it queued.
        let was = header.state.swap(QUEUED, Ordering::AcqRel);
        assert!(
            was & RUNNING != 0,
            "a task queued again that was not running: {was}"
        );
        queue.link(header);
    }

    /// Ends a task: its word says done whether or not it was notified, and its job goes.
    fn finish(&self, header: &Arc<Header>) {
        let mut state = header.state.load(Ordering::Acquire);
        while state != CLOSED {
            match header
                .state
                .compare_exchange(state, DONE, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(now) => state = now,
            }
        }
        if let Some(job) = header.job.upgrade() {
            for signal in &job.watches {
                signal.unlist(header);
            }
        }
        lock(&self.jobs).retain(|job| !Arc::ptr_eq(&job.header, header));
        lock(&self.queue).tasks -= 1;
    }
}

/// A worker's loop (22.7): claims and steps tasks by class, and on a pumped backend steps it once
/// after each task step and, with nothing to claim, while it delivers; once it holds nothing and
/// nothing is queued, parks until a link or the backend's next request wakes it.
#[turso_macros::atoll_entry]
fn work(shared: &Arc<Shared>) {
    let pumped = shared.io.delivery() == Delivery::Pumped;
    let me = crate::thread::current();
    let ready = crate::io::unparking(me.clone());
    loop {
        if shared.closed.load(Ordering::Acquire) {
            return;
        }
        let stepped = shared.run_one();
        if pumped {
            match shared.io.try_step(&ready) {
                Err(_) => crate::io::stepping_failed("an executor's worker"),
                Ok(Stepped::Delivered) => continue,
                // Refused at creation, as Atoll refuses it at open.
                Ok(Stepped::Unsaid) => {
                    unreachable!("a backend that says what its steps find answered Unsaid")
                }
                Ok(Stepped::Empty) if stepped => continue,
                Ok(Stepped::Empty) => {}
            }
        } else if stepped {
            continue;
        }
        {
            let mut queue = lock(&shared.queue);
            if queue.closing {
                return;
            }
            if queue.lists.iter().any(|list| !list.is_empty()) {
                continue;
            }
            queue.idle.push_back(me.clone());
        }
        crate::storage::atoll::park::thread();
        // Woken by the backend, or spuriously: its node goes, so a link wakes another worker.
        lock(&shared.queue).idle.retain(|t| t.id() != me.id());
    }
}

/// A database's executor: its tasks, its queue and its workers.
pub(crate) struct Executor {
    shared: Arc<Shared>,
    workers: Vec<crate::thread::JoinHandle<()>>,
}

/// A task as its creator names it.
#[derive(Clone)]
pub(crate) struct TaskRef(Arc<Header>);

impl TaskRef {
    /// Wakes the task, as a completion it waits on would.
    pub(crate) fn wake(&self) {
        wake(&self.0);
    }

    /// Whether its job is over.
    pub(crate) fn is_done(&self) -> bool {
        self.0.state.load(Ordering::Acquire) == DONE
    }
}

impl Executor {
    /// An executor stepping a pumped `io` between its tasks' steps, with `workers` threads of its
    /// own; none leaves the stepping to [`run_one`](Self::run_one)'s callers.
    pub(crate) fn new(io: Arc<dyn IO>, workers: usize) -> Result<Self> {
        crate::storage::atoll::db::refuse_a_backend_that_would_poll(&*io)?;
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                lists: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
                idle: VecDeque::with_capacity(workers),
                tasks: 0,
                closing: false,
            }),
            closed: AtomicBool::new(false),
            jobs: Mutex::new(Vec::new()),
            io,
        });
        let mut executor = Self {
            shared,
            workers: Vec::with_capacity(workers),
        };
        for n in 0..workers {
            let shared = executor.shared.clone();
            let handle = crate::thread::Builder::new()
                .name(format!("atoll-worker-{n}"))
                .spawn(move || work(&shared))
                .map_err(|e| {
                    crate::LimboError::InternalError(format!(
                        "an executor's worker did not start: {e}"
                    ))
                })?;
            executor.workers.push(handle);
        }
        Ok(executor)
    }

    /// Adds `task` in `class`, queued for its first step: every list makes room for it now, so
    /// no wake allocates.
    pub(crate) fn spawn(&self, class: Class, task: Box<dyn Task>) -> TaskRef {
        self.spawn_watching(class, task, &[])
    }

    /// [`spawn`](Self::spawn) for a task that may watch `watches`, whose lists make room for it.
    pub(crate) fn spawn_watching(
        &self,
        class: Class,
        task: Box<dyn Task>,
        watches: &[&Arc<Signal>],
    ) -> TaskRef {
        for signal in watches {
            signal.declare();
        }
        let job = Arc::new_cyclic(|job| Job {
            header: Arc::new(Header {
                state: AtomicU8::new(QUEUED),
                class: AtomicU8::new(class as u8),
                shared: Arc::downgrade(&self.shared),
                job: job.clone(),
            }),
            task: Mutex::new(task),
            watches: watches.iter().map(|s| (*s).clone()).collect(),
        });
        let header = job.header.clone();
        lock(&self.shared.jobs).push(job);
        let mut queue = lock(&self.shared.queue);
        queue.tasks += 1;
        let tasks = queue.tasks;
        for list in &mut queue.lists {
            list.reserve(tasks.saturating_sub(list.len()));
        }
        if queue.closing {
            header.state.store(CLOSED, Ordering::Release);
        } else {
            queue.link(&header);
        }
        TaskRef(header)
    }

    /// Claims the next task by class and steps it once, on the calling thread; whether there was
    /// one. A caller's runner (22.7), and the deterministic one.
    pub(crate) fn run_one(&self) -> bool {
        self.shared.run_one()
    }

    /// Closes every task, so none is claimed or linked again, wakes the idle workers and joins
    /// them once their steps in progress end; then drops every job.
    pub(crate) fn close(mut self) {
        {
            let mut queue = lock(&self.shared.queue);
            queue.closing = true;
            self.shared.closed.store(true, Ordering::Release);
            for list in &mut queue.lists {
                for header in list.drain(..) {
                    header.state.store(CLOSED, Ordering::Release);
                }
            }
            for job in lock(&self.shared.jobs).iter() {
                job.header.state.store(CLOSED, Ordering::Release);
            }
            for worker in queue.idle.drain(..) {
                worker.unpark();
            }
        }
        for worker in self.workers.drain(..) {
            // A worker that panicked ended the process (22.11): a join returns its loop's end.
            let _ = worker.join();
        }
        // Each task off every list it declared, outside the queue's mutex (22.3's lock order).
        for job in lock(&self.shared.jobs).drain(..) {
            for signal in &job.watches {
                signal.unlist(&job.header);
            }
        }
    }
}
