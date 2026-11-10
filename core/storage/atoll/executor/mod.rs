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

use self::sync::{lock, AtomicBool, AtomicPtr, AtomicU8, Mutex, Ordering};
use crate::io::{Completion, Delivery, Stepped, IO};
use crate::Result;
use std::collections::VecDeque;
use std::sync::{Arc, Weak};

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::storage::atoll::locks::{Mutex, MutexGuard};
    pub(super) use crate::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, Ordering};
    pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock()
    }
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, Ordering};
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
    /// Nothing to do until `c` finishes, which another job's work brings: the task holds a ticket
    /// of its own class on that job while it waits (22.4), and that job ranks after it.
    WaitOn(TaskRef, Completion),
}

/// A job the executor steps (22.1): one unit of its work per step, never waiting inside one; its
/// progress lives in its own state, which only the step touches.
pub(crate) trait Task: Send {
    fn step(&mut self, cx: &Cx) -> Result<Step>;
}

/// What a step may ask of the executor about its own task.
pub(crate) struct Cx<'a> {
    header: &'a Arc<Header>,
    shared: &'a Arc<Shared>,
}

impl Cx<'_> {
    /// The task's own trigger is gone, as the step found after reading `due` at `read`: its own
    /// level drops to maintenance, unless a trigger came since, which moved the count and keeps
    /// it (22.4); whether it dropped.
    pub(crate) fn clear_due(&self, due: &Signal, read: u64) -> bool {
        crate::race::point("executor.clearing");
        let mut queue = lock(&self.shared.queue);
        if due.read() != read {
            return false;
        }
        {
            let mut demand = lock(&self.header.demand);
            if demand.own == Class::Due {
                demand.own = Class::Maintenance;
            }
        }
        queue.reclass(self.header);
        true
    }
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

impl Class {
    fn of(n: u8) -> Self {
        match n {
            0 => Class::Relief,
            1 => Class::Due,
            _ => Class::Maintenance,
        }
    }
}

/// A task's demand (22.4), under the queue's mutex: its own level, the live tickets on it of each
/// class above maintenance, and the ticket it holds on another job while a logical wait stands.
struct Demand {
    own: Class,
    tickets: [u32; 2],
    waiting_on: Option<(Arc<Header>, Class)>,
}

impl Demand {
    /// The highest of its demands.
    fn class(&self) -> Class {
        if self.tickets[Class::Relief as usize] > 0 {
            Class::Relief
        } else if self.tickets[Class::Due as usize] > 0 && self.own != Class::Relief {
            Class::Due
        } else {
            self.own
        }
    }

    fn add(&mut self, class: Class) {
        if class != Class::Maintenance {
            self.tickets[class as usize] += 1;
        }
    }

    fn remove(&mut self, class: Class) {
        if class != Class::Maintenance {
            let n = &mut self.tickets[class as usize];
            assert!(*n > 0, "a ticket dropped that was never taken");
            *n -= 1;
        }
    }
}

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
    /// Its class, the highest of its demands, read and written under the queue's mutex only.
    class: AtomicU8,
    /// Its demand, under the queue's mutex.
    demand: Mutex<Demand>,
    /// Its job's place in the order jobs wait on each other (22.4): a wait names only a job
    /// ranked after it, so no cycle of waits forms and a move of tickets ends.
    rank: u8,
    shared: Weak<Shared>,
    job: Weak<Job>,
}

/// A task's job, held by the executor's table while the task lives.
struct Job {
    header: Arc<Header>,
    /// The task, owned here between steps and by its runner during one: the runner that moved
    /// the task to `RUNNING` takes it out and puts it back before the step's end publishes the
    /// next state, so it steps it holding no executor mutex (22.2) and no lock guards the task.
    task: AtomicPtr<Box<dyn Task>>,
    /// The signals it may watch, which take it off their lists when it goes.
    watches: Vec<Arc<Signal>>,
}

impl Job {
    /// The task, out of the job for its runner's step.
    fn take_task(&self) -> Box<Box<dyn Task>> {
        let held = self.task.swap(std::ptr::null_mut(), Ordering::Acquire);
        assert!(!held.is_null(), "a task taken by two runners");
        // SAFETY: a non-null pointer here came from `Box::into_raw` at the spawn or at a step's
        // put-back, and the swap made this runner its only holder.
        unsafe { Box::from_raw(held) }
    }

    /// The task back in the job, before the step's end publishes the next state.
    fn put_task(&self, task: Box<Box<dyn Task>>) {
        self.task.store(Box::into_raw(task), Ordering::Release);
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        let held = self.task.swap(std::ptr::null_mut(), Ordering::Acquire);
        if !held.is_null() {
            // SAFETY: as in `take_task`; the job's last handle is gone, so no runner holds it.
            drop(unsafe { Box::from_raw(held) });
        }
    }
}

impl std::task::Wake for Header {
    fn wake(self: Arc<Self>) {
        wake(&self);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        wake(self);
    }
}

/// Wakes `header`'s task, an operation of its own: then invokes the external runners its link
/// moved, once every executor mutex is let go (22.3).
fn wake(header: &Arc<Header>) {
    let moved = wake_nested(header);
    if moved > 0 {
        if let Some(shared) = header.shared.upgrade() {
            shared.drain(moved);
        }
    }
}

/// Wakes `header`'s task inside another operation (22.3): a running one is marked to run again,
/// an idle one is queued under the mutex with its link, any other is left as it is. How many
/// external runners its link moved, for the operation to invoke once it lets every mutex go.
fn wake_nested(header: &Arc<Header>) -> usize {
    let mut state = header.state.load(Ordering::Acquire);
    loop {
        match state {
            RUNNING => match header.state.compare_exchange(
                RUNNING,
                RUNNING | NOTIFIED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return 0,
                Err(now) => state = now,
            },
            IDLE => {
                let Some(shared) = header.shared.upgrade() else {
                    return 0;
                };
                let mut queue = lock(&shared.queue);
                match header.state.compare_exchange(
                    IDLE,
                    QUEUED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return queue.link(header),
                    Err(now) => state = now,
                }
            }
            _ => return 0,
        }
    }
}

#[cfg(not(atoll_loom))]
crate::thread::thread_local! {
    /// Whether this thread is inside a task's step or a waker a drain invokes, where no runner
    /// may enter the executor (22.7).
    static INSIDE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(atoll_loom)]
loom::thread_local! {
    static INSIDE: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

/// This thread inside a step or an invoked waker, until dropped.
struct Inside(bool);

impl Inside {
    fn enter() -> Self {
        Self(INSIDE.with(|inside| inside.replace(true)))
    }

    /// Whether this thread is inside a step or an invoked waker.
    fn here() -> bool {
        INSIDE.with(std::cell::Cell::get)
    }
}

impl Drop for Inside {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        INSIDE.with(|inside| inside.set(self.0));
    }
}

/// A runner's node among the idle runners (22.3): a worker's thread, parked, or an external
/// runner's last waker, which only a drain invokes, after every executor mutex is let go.
enum Node {
    Thread(crate::thread::Thread),
    External(Option<std::task::Waker>),
}

/// Which list a node is on: a node is on at most one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum On {
    Nowhere,
    Idle,
    ToWake,
}

/// A node and where it is; a node given back leaves its slot for the next.
struct Slot {
    node: Option<Node>,
    on: On,
}

/// The run queue (22.3): one list per class, and the runners with nothing to claim.
struct Queue {
    lists: [VecDeque<Arc<Header>>; CLASSES],
    /// The tasks the executor holds, which every list has room for, so a link never allocates.
    tasks: usize,
    /// Every runner's node by its number, each runner bringing its own; a slot given back is
    /// reused.
    nodes: Vec<Slot>,
    free: Vec<usize>,
    /// The idle runners' nodes, first in first out, with room for every node.
    idle: VecDeque<usize>,
    /// External nodes a link took off the idle runners, first in first out, each waiting for a
    /// drain to invoke its waker, with room for every node.
    to_wake: VecDeque<usize>,
    closing: bool,
}

impl Queue {
    fn new(workers: usize) -> Self {
        Self {
            lists: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
            tasks: 0,
            nodes: Vec::with_capacity(workers),
            free: Vec::new(),
            idle: VecDeque::with_capacity(workers),
            to_wake: VecDeque::new(),
            closing: false,
        }
    }

    /// Links a task just moved to `QUEUED` at the back of its class, and takes one idle runner's
    /// node off the list; how many external nodes it moved.
    fn link(&mut self, header: &Arc<Header>) -> usize {
        let class = header.class.load(Ordering::Relaxed) as usize;
        self.lists[class].push_back(header.clone());
        self.wake_one_idle()
    }

    /// Takes the first idle runner's node off the list: a thread is unparked, an external node
    /// moved to the tail of the to-wake list; how many external nodes it moved.
    fn wake_one_idle(&mut self) -> usize {
        let Some(n) = self.idle.pop_front() else {
            return 0;
        };
        let slot = &mut self.nodes[n];
        match &slot.node {
            Some(Node::Thread(thread)) => {
                slot.on = On::Nowhere;
                thread.unpark();
                0
            }
            Some(Node::External(_)) => {
                slot.on = On::ToWake;
                self.to_wake.push_back(n);
                1
            }
            None => unreachable!("an idle runner's node given back"),
        }
    }

    /// A runner's node, provisioned with it: both lists keep room for every node.
    fn add_node(&mut self, node: Node) -> usize {
        let n = match self.free.pop() {
            Some(n) => n,
            None => {
                self.nodes.push(Slot {
                    node: None,
                    on: On::Nowhere,
                });
                self.nodes.len() - 1
            }
        };
        self.nodes[n].node = Some(node);
        let live = self.nodes.len() - self.free.len();
        self.idle.reserve(live.saturating_sub(self.idle.len()));
        self.to_wake
            .reserve(live.saturating_sub(self.to_wake.len()));
        n
    }

    /// Takes node `n` off whichever list holds it.
    fn unlist(&mut self, n: usize) {
        let list = match self.nodes[n].on {
            On::Nowhere => return,
            On::Idle => &mut self.idle,
            On::ToWake => &mut self.to_wake,
        };
        if let Some(at) = list.iter().position(|&m| m == n) {
            list.remove(at);
        }
        self.nodes[n].on = On::Nowhere;
    }

    /// Gives node `n` back: off its list, its slot free; what it held, to drop after the mutex.
    fn remove_node(&mut self, n: usize) -> Option<Node> {
        self.unlist(n);
        self.free.push(n);
        self.nodes[n].node.take()
    }

    /// Node `n` among the idle runners, at the tail, unless it is on a list already.
    fn idle_node(&mut self, n: usize) {
        if self.nodes[n].on == On::Nowhere {
            self.nodes[n].on = On::Idle;
            self.idle.push_back(n);
        }
    }

    /// Swaps `waker` into external node `n` and places it among the idle runners, unless a link
    /// already moved it, whose drain then invokes this waker; the waker it replaces, to drop after
    /// the mutex.
    fn register_external(&mut self, n: usize, waker: std::task::Waker) -> Option<std::task::Waker> {
        let Some(Node::External(slot)) = &mut self.nodes[n].node else {
            unreachable!("a waker registered on a node that is not external");
        };
        let replaced = slot.replace(waker);
        self.idle_node(n);
        replaced
    }

    /// The head of the highest non-empty class, moved to `RUNNING`; a logical wait it stood in
    /// is over, and the ticket it held goes.
    fn claim(&mut self) -> Option<Arc<Header>> {
        if self.closing {
            return None;
        }
        let header = self.lists.iter_mut().find_map(VecDeque::pop_front)?;
        let was = header.state.swap(RUNNING, Ordering::AcqRel);
        assert_eq!(was, QUEUED, "a task claimed that was not queued");
        self.end_wait(&header);
        Some(header)
    }

    /// Claims `header` by name (22.7): a queued task is unlinked and claimed, an idle one is moved
    /// straight to `RUNNING` (its claimer has its own reason to run it), and one running, done or
    /// closed is left as it is; whether it was claimed. A logical wait it stood in is over.
    fn claim_named(&mut self, header: &Arc<Header>) -> bool {
        if self.closing {
            return false;
        }
        // Under the mutex no other move leaves `QUEUED` or `IDLE`: each takes it (22.3).
        match header.state.load(Ordering::Acquire) {
            QUEUED => {
                let class = header.class.load(Ordering::Relaxed) as usize;
                let list = &mut self.lists[class];
                let at = list.iter().position(|h| Arc::ptr_eq(h, header));
                let Some(at) = at else {
                    return false;
                };
                list.remove(at);
                header.state.store(RUNNING, Ordering::Release);
            }
            IDLE => header.state.store(RUNNING, Ordering::Release),
            _ => return false,
        }
        self.end_wait(header);
        true
    }

    /// Gives back the ticket `header` holds on another job, if a logical wait of its stood.
    fn end_wait(&mut self, header: &Arc<Header>) {
        let waited = lock(&header.demand).waiting_on.take();
        if let Some((target, class)) = waited {
            lock(&target.demand).remove(class);
            self.reclass(&target);
        }
    }

    /// Sets `header`'s class to the highest of its demands: a queued task moves to the back of
    /// its new class's list, and the ticket its logical wait holds moves with it, so the job it
    /// waits on carries its class too (22.4).
    fn reclass(&mut self, header: &Arc<Header>) {
        let (new, waiting) = {
            let demand = lock(&header.demand);
            (demand.class(), demand.waiting_on.clone())
        };
        let old = Class::of(header.class.load(Ordering::Relaxed));
        if new == old {
            return;
        }
        header.class.store(new as u8, Ordering::Relaxed);
        if header.state.load(Ordering::Acquire) == QUEUED {
            let list = &mut self.lists[old as usize];
            if let Some(at) = list.iter().position(|h| Arc::ptr_eq(h, header)) {
                list.remove(at);
            }
            self.lists[new as usize].push_back(header.clone());
        }
        if let Some((target, _)) = waiting {
            lock(&header.demand).waiting_on = Some((target.clone(), new));
            {
                let mut demand = lock(&target.demand);
                demand.remove(old);
                demand.add(new);
            }
            self.reclass(&target);
        }
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
        self.step(header);
        true
    }

    /// Claims `header` by name and steps it once; whether it was claimed.
    fn run_named(self: &Arc<Self>, header: &Arc<Header>) -> bool {
        if !lock(&self.queue).claim_named(header) {
            return false;
        }
        self.step(header.clone());
        true
    }

    /// Steps a task its runner claimed, once.
    fn step(self: &Arc<Self>, header: Arc<Header>) {
        let Some(job) = header.job.upgrade() else {
            // Removed with its subject while queued: nothing to step.
            return;
        };
        let cx = Cx {
            header: &header,
            shared: self,
        };
        let mut task = job.take_task();
        let step = {
            let _inside = Inside::enter();
            task.step(&cx)
        };
        job.put_task(task);
        let moved = self.end(&header, &job, step);
        self.drain(moved);
    }

    /// The end of a step (22.3), from `RUNNING` or `RUNNING|NOTIFIED`; a task the executor closed
    /// meanwhile ends no other way. How many external runners its links moved.
    fn end(self: &Arc<Self>, header: &Arc<Header>, job: &Job, step: Result<Step>) -> usize {
        if let Ok(Step::Idle(Watched(watched))) = &step {
            // Its job declared each, so the lists have room for it: registering allocates nothing.
            for (signal, _) in watched.iter().flatten() {
                assert!(
                    job.watches.iter().any(|w| Arc::ptr_eq(w, signal)),
                    "a step watching a signal its job did not declare"
                );
            }
        }
        if header.state.load(Ordering::Acquire) == CLOSED {
            // Close takes it off every list and drops its job, on this thread too when the step
            // dropped the executor; it is registered on none and finished by none.
            return 0;
        }
        match step {
            // An error ends the task as its job's end does: nothing restarts it.
            Ok(Step::Done) | Err(_) => {
                self.finish(header);
                0
            }
            Ok(Step::Again) => self.requeue(header),
            Ok(Step::Wait(c)) => {
                // Registered while running: a finish from now on marks the task notified.
                c.set_waker(&std::task::Waker::from(header.clone()));
                self.rest(header)
            }
            Ok(Step::WaitOn(target, c)) => {
                // Before any mutex: a refusal passing one ends the process (22.11).
                assert!(
                    Weak::ptr_eq(&header.shared, &target.0.shared),
                    "a wait naming a job of another executor (22.4)"
                );
                assert!(
                    header.rank < target.0.rank,
                    "a wait naming a job not after its own (22.4)"
                );
                {
                    let mut queue = lock(&self.queue);
                    let class = Class::of(header.class.load(Ordering::Relaxed));
                    lock(&header.demand).waiting_on = Some((target.0.clone(), class));
                    lock(&target.0.demand).add(class);
                    queue.reclass(&target.0);
                }
                // Taking the ticket wakes the job it names.
                wake(&target.0);
                c.set_waker(&std::task::Waker::from(header.clone()));
                self.rest(header)
            }
            Ok(Step::Idle(Watched(watched))) => {
                // Registered while running: a notify from now on marks the task notified, and
                // one between its look and its registration has moved a count.
                let mut moved = false;
                for (signal, read) in watched.iter().flatten() {
                    moved |= !signal.register(header, *read, self);
                }
                if moved {
                    self.requeue(header)
                } else {
                    self.rest(header)
                }
            }
        }
    }

    /// A task that waits: from `RUNNING` to `IDLE` by one compare-exchange, or, woken while it
    /// ran, queued again; how many external runners its link moved.
    fn rest(&self, header: &Arc<Header>) -> usize {
        match header
            .state
            .compare_exchange(RUNNING, IDLE, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(CLOSED) => 0,
            Err(_) => self.requeue(header),
        }
    }

    /// Queues a task again at the back of its class, unless the executor closed it; how many
    /// external runners its link moved.
    fn requeue(&self, header: &Arc<Header>) -> usize {
        let mut queue = lock(&self.queue);
        if header.state.load(Ordering::Acquire) == CLOSED {
            return 0;
        }
        // Only this runner moves the task out of running, and close moves it under this mutex;
        // a wake meanwhile either marked it first, which the swap replaces, or finds it queued.
        let was = header.state.swap(QUEUED, Ordering::AcqRel);
        assert!(
            was & RUNNING != 0,
            "a task queued again that was not running: {was}"
        );
        queue.link(header)
    }

    /// Invokes up to `moved` external runners' wakers, the count an operation's links moved, once
    /// that operation has let every executor mutex go: each is popped from the head of the
    /// to-wake list under the queue's mutex and woken after it (22.3). A drain stops at its count
    /// or at an empty list, so every node moved is invoked by some drain and no drain does more
    /// than its own moves.
    fn drain(&self, mut moved: usize) {
        if moved == 0 {
            return;
        }
        crate::race::point("executor.draining");
        while moved > 0 {
            let waker = {
                let mut queue = lock(&self.queue);
                let Some(n) = queue.to_wake.pop_front() else {
                    return;
                };
                let slot = &mut queue.nodes[n];
                slot.on = On::Nowhere;
                match &mut slot.node {
                    Some(Node::External(waker)) => waker.take(),
                    _ => unreachable!("a node on the to-wake list that is not external"),
                }
            };
            moved -= 1;
            if let Some(waker) = waker {
                let _inside = Inside::enter();
                waker.wake();
            }
        }
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
                signal.leave(header);
            }
        }
        let gone = {
            let mut jobs = lock(&self.jobs);
            let at = jobs.iter().position(|job| Arc::ptr_eq(&job.header, header));
            at.map(|at| jobs.swap_remove(at))
        };
        // After the table's mutex: a job's drop may run anything its task holds (22.2).
        drop(gone);
        let mut queue = lock(&self.queue);
        queue.end_wait(header);
        queue.tasks -= 1;
    }
}

/// A worker's loop (22.7): claims and steps tasks by class, and on a pumped backend steps it once
/// after each task step and, with nothing to claim, while it delivers; once it holds nothing and
/// nothing is queued, parks until a link or the backend's next request wakes it. Closed, it takes
/// back from the backend the registration its steps left.
#[turso_macros::atoll_entry]
fn work(shared: &Arc<Shared>) {
    let pumped = shared.io.delivery() == Delivery::Pumped;
    let me = crate::thread::current();
    let place = crate::io::ReadyPlace::default();
    let ready = crate::io::unparking(me.clone());
    let mut registered = false;
    let node = lock(&shared.queue).add_node(Node::Thread(me.clone()));
    loop {
        if shared.closed.load(Ordering::Acquire) {
            break;
        }
        let stepped = shared.run_one();
        if pumped {
            match shared.io.try_step(place.key(), &ready) {
                Err(_) => crate::io::stepping_failed("an executor's worker"),
                Ok(Stepped::Delivered) => continue,
                // Refused at creation, as Atoll refuses it at open.
                Ok(Stepped::Unsaid) => {
                    unreachable!("a backend that says what its steps find answered Unsaid")
                }
                Ok(Stepped::Empty) => {
                    registered = true;
                    if stepped {
                        continue;
                    }
                }
            }
        } else if stepped {
            continue;
        }
        {
            let mut queue = lock(&shared.queue);
            if queue.closing {
                break;
            }
            if queue.lists.iter().any(|list| !list.is_empty()) {
                continue;
            }
            queue.idle_node(node);
        }
        crate::storage::atoll::park::thread();
        // Woken by the backend, or spuriously: its node goes, so a link wakes another runner.
        lock(&shared.queue).unlist(node);
    }
    let gone = lock(&shared.queue).remove_node(node);
    drop(gone);
    if registered {
        shared.io.withdraw(place.key());
    }
}

/// A database's executor: its tasks, its queue and its workers.
pub(crate) struct Executor {
    shared: Arc<Shared>,
    workers: Vec<crate::thread::JoinHandle<()>>,
}

/// An embedder thread's runner (22.7): it steps one task at a time, and with none to claim leaves
/// its waker on its node among the idle runners, which a link moves to the to-wake list and an
/// operation's drain invokes once every executor mutex is let go. Dropped, its node goes, off
/// whichever list holds it.
pub(crate) struct Runner {
    shared: Arc<Shared>,
    node: usize,
}

impl Runner {
    /// Claims the next task by class and steps it once; with none to claim, registers `waker` on
    /// this runner's node among the idle runners, so the next link wakes it. Whether it stepped
    /// one. Never from inside a step or a waker the executor invoked.
    pub(crate) fn run_one(&self, waker: &std::task::Waker) -> bool {
        assert!(
            !Inside::here(),
            "a runner entered the executor from inside a step or a waker"
        );
        // Cloned before any executor mutex is taken, and what it replaces dropped after.
        let waker = waker.clone();
        let (claimed, replaced) = {
            let mut queue = lock(&self.shared.queue);
            match queue.claim() {
                Some(header) => (Some(header), None),
                None if queue.closing => (None, None),
                None => (None, queue.register_external(self.node, waker)),
            }
        };
        drop(replaced);
        match claimed {
            Some(header) => {
                self.shared.step(header);
                true
            }
            None => false,
        }
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        let gone = lock(&self.shared.queue).remove_node(self.node);
        drop(gone);
    }
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

    /// A writer's ticket on this task (22.4): its demand rises to `class` while the ticket lives,
    /// and taking it wakes the task.
    pub(crate) fn ticket(&self, class: Class) -> Ticket {
        if let Some(shared) = self.0.shared.upgrade() {
            let mut queue = lock(&shared.queue);
            lock(&self.0.demand).add(class);
            queue.reclass(&self.0);
        }
        wake(&self.0);
        Ticket {
            header: self.0.clone(),
            class,
        }
    }

    /// The task's own trigger fired (22.4): `due` moves first, then its own level becomes due,
    /// then it and every task listed on `due` is woken; a clear that read `due` before this sees
    /// it moved and keeps the level.
    pub(crate) fn trigger(&self, due: &Signal) {
        due.bump();
        if let Some(shared) = self.0.shared.upgrade() {
            let mut queue = lock(&shared.queue);
            {
                let mut demand = lock(&self.0.demand);
                if demand.own == Class::Maintenance {
                    demand.own = Class::Due;
                }
            }
            queue.reclass(&self.0);
        }
        due.wake_listed();
        wake(&self.0);
    }

    /// Its class now.
    #[cfg(test)]
    pub(crate) fn class(&self) -> Class {
        Class::of(self.0.class.load(Ordering::Relaxed))
    }
}

/// A writer's demand on a task (22.4), owned by the writer: dropped (admitted, cancelled, or the
/// connection gone), it lowers that demand, and one ticket dropped never clears another's.
pub(crate) struct Ticket {
    header: Arc<Header>,
    class: Class,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        if let Some(shared) = self.header.shared.upgrade() {
            let mut queue = lock(&shared.queue);
            lock(&self.header.demand).remove(self.class);
            queue.reclass(&self.header);
        }
    }
}

impl Executor {
    /// An executor stepping a pumped `io` between its tasks' steps, with `workers` threads of its
    /// own; none leaves the stepping to [`run_one`](Self::run_one)'s callers.
    pub(crate) fn new(io: Arc<dyn IO>, workers: usize) -> Result<Self> {
        crate::storage::atoll::db::refuse_a_backend_that_would_poll(&*io)?;
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue::new(workers)),
            closed: AtomicBool::new(false),
            jobs: Mutex::new(Vec::new()),
            io,
        });
        let mut executor = Self {
            shared,
            workers: Vec::with_capacity(workers),
        };
        // A start that fails drops the executor, which closes and joins the workers begun.
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
        self.spawn_ranked(class, 0, task, watches)
    }

    /// [`spawn_watching`](Self::spawn_watching) for a job ranked `rank` in the order jobs wait
    /// on each other (22.4): its logical waits name only jobs ranked after it.
    pub(crate) fn spawn_ranked(
        &self,
        class: Class,
        rank: u8,
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
                demand: Mutex::new(Demand {
                    own: class,
                    tickets: [0; 2],
                    waiting_on: None,
                }),
                rank,
                shared: Arc::downgrade(&self.shared),
                job: job.clone(),
            }),
            task: AtomicPtr::new(Box::into_raw(Box::new(task))),
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
        let moved = if queue.closing {
            header.state.store(CLOSED, Ordering::Release);
            0
        } else {
            queue.link(&header)
        };
        drop(queue);
        self.shared.drain(moved);
        TaskRef(header)
    }

    /// A runner of an embedder's thread (22.7), with its node among the idle runners.
    pub(crate) fn runner(&self) -> Runner {
        let node = lock(&self.shared.queue).add_node(Node::External(None));
        Runner {
            shared: self.shared.clone(),
            node,
        }
    }

    /// Claims the next task by class and steps it once, on the calling thread; whether there was
    /// one. A caller's runner (22.7), and the deterministic one.
    pub(crate) fn run_one(&self) -> bool {
        self.shared.run_one()
    }

    /// Claims `task` by name and steps it once, on the calling thread (22.7): queued, it is taken
    /// out of its list; idle, it runs for its claimer's own reason; running, done or closed, it is
    /// left. Whether it was stepped.
    pub(crate) fn run_named(&self, task: &TaskRef) -> bool {
        assert!(
            std::ptr::eq(task.0.shared.as_ptr(), Arc::as_ptr(&self.shared)),
            "a task claimed by name on another executor"
        );
        self.shared.run_named(&task.0)
    }

    /// Closes the executor, as dropping it does.
    pub(crate) fn close(self) {
        drop(self);
    }

    /// Closes every task, so none is claimed or linked again, wakes the idle workers and joins
    /// them once their steps in progress end, all but the calling thread when a step of its own
    /// dropped the executor (it leaves once that step ends); then drops every job, after the
    /// table's mutex.
    fn shut(&mut self) {
        let moved = {
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
            let mut moved = 0;
            while !queue.idle.is_empty() {
                moved += queue.wake_one_idle();
            }
            moved
        };
        self.shared.drain(moved);
        let me = crate::thread::current().id();
        for worker in self.workers.drain(..) {
            if worker.thread().id() == me {
                continue;
            }
            // A worker that panicked ended the process (22.11): a join returns its loop's end.
            let _ = worker.join();
        }
        let jobs = std::mem::take(&mut *lock(&self.shared.jobs));
        // Each task off every list it declared, outside the queue's mutex (22.3's lock order).
        for job in &jobs {
            for signal in &job.watches {
                signal.leave(&job.header);
            }
        }
        drop(jobs);
    }
}

impl Drop for Executor {
    /// Closes the executor: a started one, or one whose start failed after some of its workers
    /// began, leaves no worker running.
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.shut();
    }
}
