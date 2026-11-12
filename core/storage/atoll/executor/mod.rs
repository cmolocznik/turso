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
use super::requests::Requests;
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

    /// Gives up what the task waits for of other jobs' work, the executor closing (22.12 step
    /// 3): once, on the closing thread, every step ended and outside any, before close waits for
    /// any request. A task waiting on no other job's work has nothing to give up.
    fn abandon(&mut self) {}
}

/// What a step may ask of the executor about its own task.
pub(crate) struct Cx<'a> {
    header: &'a Arc<Header>,
    shared: &'a Arc<Shared>,
}

impl Cx<'_> {
    /// The task this step is a step of, as its creator names it: for a step that registers it to
    /// be woken, as a guarded state's contender.
    pub(crate) fn task(&self) -> TaskRef {
        TaskRef(self.header.clone())
    }

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

/// Each job's place in the order jobs wait on each other (22.4): rollback, then collection, then
/// the checkpoint, then its image slots, then the preparers. A job's logical wait names only a
/// job after it (`Step::WaitOn`); rollback, collection and the image slots take their places when
/// they become tasks.
pub(crate) mod rank {
    pub(crate) const CHECKPOINT: u8 = 2;
    pub(crate) const PREPARER: u8 = 4;
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

    /// Its task's logical waits given up at close (22.12 step 3). A task out of its job is the
    /// one whose own step is closing the executor: that step ends it, and nothing more is done
    /// for it (22.12 step 5).
    fn abandon(&self) {
        let held = self.task.swap(std::ptr::null_mut(), Ordering::Acquire);
        if held.is_null() {
            return;
        }
        // SAFETY: as in `take_task`; the swap made this thread its only holder.
        let mut task = unsafe { Box::from_raw(held) };
        task.abandon();
        self.put_task(task);
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
    /// The executors this thread is counted in by a call in progress ([`Calling`]), innermost
    /// last, by the address of each one's shared part: a close of one made from inside those
    /// calls does not wait for them.
    static CALLING: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(atoll_loom)]
loom::thread_local! {
    static INSIDE: std::cell::Cell<bool> = std::cell::Cell::new(false);
    static CALLING: std::cell::RefCell<Vec<usize>> = std::cell::RefCell::new(Vec::new());
}

/// A call into an executor counted in its queue until it ends, close waiting for every one but
/// the closing thread's own (22.7, 22.12): an external call stepping a task, or a removal letting
/// its task's job go.
struct Calling<'a> {
    shared: &'a Shared,
}

impl<'a> Calling<'a> {
    /// Counts a call whose claim the caller made under the queue's mutex, `queue` that guard.
    fn enter(shared: &'a Shared, queue: &mut Queue) -> Self {
        queue.calls += 1;
        CALLING.with(|calling| calling.borrow_mut().push(shared.address()));
        Self { shared }
    }
}

impl Drop for Calling<'_> {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        let ended = CALLING.with(|calling| calling.borrow_mut().pop());
        assert_eq!(
            ended,
            Some(self.shared.address()),
            "calls into executors ended out of order"
        );
        let closer = {
            let mut queue = lock(&self.shared.queue);
            queue.calls -= 1;
            queue.closer.take()
        };
        // The closer looks again, under the mutex, at the calls left.
        if let Some(closer) = closer {
            closer.unpark();
        }
    }
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

/// Whether this thread is inside a step or a waker an executor invoked: a wait made there is the
/// step's own, which the step hands its executor (`Step::Wait`, `Step::WaitOn`), never a runner
/// of its own (22.7).
pub(crate) fn inside() -> bool {
    Inside::here()
}

/// A caller enters the executor only from outside every step and every waker the executor
/// invokes (22.7): a runner entered from one would run a step inside another. Workers enter
/// through their own loop, never through here.
fn refuse_entry_from_inside() {
    assert!(
        !Inside::here(),
        "a runner entered the executor from inside a step or a waker"
    );
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
    /// External calls stepping a task now, which close waits for, and the closer parked while
    /// any is.
    calls: usize,
    closer: Option<crate::thread::Thread>,
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
            calls: 0,
            closer: None,
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

    /// Gives node `n` back: off its list, its slot free. A node that was not idle may carry a
    /// wake a link gave it (chosen for the to-wake list, or woken and not yet acted on): with work
    /// still queued, that wake goes on to another idle runner, so a runner leaving never strands
    /// the work it was woken for (the auditor's P1-EX-H1). What the node held, to drop after the
    /// mutex, and how many external nodes the handoff moved, for the caller to drain after it.
    fn remove_node(&mut self, n: usize) -> (Option<Node>, usize) {
        let woken = self.nodes[n].on != On::Idle;
        self.unlist(n);
        self.free.push(n);
        let node = self.nodes[n].node.take();
        let queued = self.lists.iter().any(|list| !list.is_empty());
        let moved = if woken && queued && !self.closing {
            self.wake_one_idle()
        } else {
            0
        };
        (node, moved)
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
    /// Whether workers of the executor's own run its tasks: with none, whoever waits on its work
    /// runs it (22.7).
    driven: bool,
}

impl Shared {
    /// The address that names this executor among a thread's calls ([`Calling`]).
    fn address(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    /// Claims the next task and steps it once; whether there was one. A step a worker takes is
    /// joined by close; any other, `counted`, is among the calls close waits for, counted under
    /// the claim's own hold of the queue's mutex (22.12 step 2).
    fn run_one(self: &Arc<Self>, counted: bool) -> bool {
        let (header, _call) = {
            let mut queue = lock(&self.queue);
            let Some(header) = queue.claim() else {
                return false;
            };
            let call = counted.then(|| Calling::enter(self, &mut queue));
            (header, call)
        };
        self.step(header);
        true
    }

    /// Claims `header` by name and steps it once, a call close waits for, counted under the
    /// claim's own hold of the queue's mutex (22.12 step 2); whether it was claimed.
    fn run_named(self: &Arc<Self>, header: &Arc<Header>) -> bool {
        let _call = {
            let mut queue = lock(&self.queue);
            if !queue.claim_named(header) {
                return false;
            }
            Calling::enter(self, &mut queue)
        };
        self.step(header.clone());
        true
    }

    /// Runs the executor on the calling thread, `me`, until `c` finishes (22.7): one task step at
    /// a time by class, a pumped backend stepped once after each and, with none to claim, while
    /// it delivers; with nothing to claim and nothing delivered, parks among the idle runners
    /// until a link, a submission or `c`'s finish wakes it, `c`'s waker being registered already.
    fn run_until(self: &Arc<Self>, c: &Completion, me: &crate::thread::Thread) {
        let pumped = self.io.delivery() == Delivery::Pumped;
        let place = crate::io::ReadyPlace::default();
        let ready = crate::io::unparking(me.clone());
        let mut registered = false;
        let node = lock(&self.queue).add_node(Node::Thread(me.clone()));
        while !c.finished() {
            let stepped = self.run_one(true);
            if pumped {
                match self.io.try_step(place.key(), &ready) {
                    Err(_) => crate::io::stepping_failed("a wait on executor work"),
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
                let mut queue = lock(&self.queue);
                if !queue.closing && queue.lists.iter().any(|list| !list.is_empty()) {
                    continue;
                }
                queue.idle_node(node);
            }
            if !c.finished() {
                crate::storage::atoll::park::thread();
            }
            // Woken, or spuriously: its node goes, so a link wakes another runner.
            lock(&self.queue).unlist(node);
        }
        let (gone, moved) = lock(&self.queue).remove_node(node);
        drop(gone);
        self.drain(moved);
        if registered {
            self.io.withdraw(place.key());
        }
    }

    /// Steps a task its runner claimed, once. The whole unit is inside (22.7): the step, its end,
    /// the drain and the job's last drop with its task's destructor, the guard declared before the
    /// job so it outlives it, so no runner enters the executor from any of them, and a panic in
    /// any of them ends the process as the guard drops, on every road that steps a task (22.11).
    fn step(self: &Arc<Self>, header: Arc<Header>) {
        let _inside = Inside::enter();
        let Some(job) = header.job.upgrade() else {
            // Removed with its subject while queued: nothing to step.
            return;
        };
        let cx = Cx {
            header: &header,
            shared: self,
        };
        let mut task = job.take_task();
        let step = task.step(&cx);
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
            // Closed or removed while it ran: registered on none and finished by none.
            self.release_removed(header);
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
            Ok(_) => 0,
            Err(CLOSED) => {
                self.release_removed(header);
                0
            }
            Err(_) => self.requeue(header),
        }
    }

    /// Queues a task again at the back of its class, unless it was closed or removed; how many
    /// external runners its link moved.
    fn requeue(&self, header: &Arc<Header>) -> usize {
        let mut queue = lock(&self.queue);
        if header.state.load(Ordering::Acquire) == CLOSED {
            drop(queue);
            self.release_removed(header);
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
        self.release_closed(header);
    }

    /// A task its step's end found closed: one removed lets its job go here; one close closed is
    /// left to close, which gives up its waits before it drops the job (22.12 step 3). A task
    /// removed before close began and read closed only once it had is left to close too.
    fn release_removed(&self, header: &Arc<Header>) {
        if !self.closed.load(Ordering::Acquire) {
            self.release_closed(header);
        }
    }

    /// A task done, closed or removed lets its job go: out of the table, off every signal it
    /// declared, the logical wait it stood in given back and its count gone, then the job
    /// dropped inside, after every mutex, since its drop may run anything its task holds (22.2,
    /// 22.3). Whoever takes the table's entry does it, once: a step's end, a removal or close;
    /// the job's last drop is a step's when one still holds it.
    fn release_closed(&self, header: &Arc<Header>) {
        let gone = {
            let mut jobs = lock(&self.jobs);
            let at = jobs.iter().position(|job| Arc::ptr_eq(&job.header, header));
            at.map(|at| jobs.swap_remove(at))
        };
        let Some(job) = gone else {
            return;
        };
        for signal in &job.watches {
            signal.leave(header);
        }
        {
            let mut queue = lock(&self.queue);
            queue.end_wait(header);
            queue.tasks -= 1;
        }
        let _inside = Inside::enter();
        drop(job);
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
        let stepped = shared.run_one(false);
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
    let (gone, moved) = lock(&shared.queue).remove_node(node);
    drop(gone);
    shared.drain(moved);
    if registered {
        shared.io.withdraw(place.key());
    }
}

/// A database's executor: its tasks, its queue and its workers.
pub(crate) struct Executor {
    shared: Arc<Shared>,
    workers: Vec<crate::thread::JoinHandle<()>>,
    /// Dropped last: the end of the frame its `Drop` opens, so the jobs close drops, with their
    /// tasks' destructors, and every field after them drop inside it (22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

/// An embedder thread's runner (22.7): it steps one task at a time, and with none to claim leaves
/// its waker on its node among the idle runners, which a link moves to the to-wake list and an
/// operation's drain invokes once every executor mutex is let go. Dropped, its node goes, off
/// whichever list holds it.
pub(crate) struct Runner {
    shared: Arc<Shared>,
    node: usize,
    /// Dropped last: the end of the frame its `Drop` opens, so the host's waker its node held
    /// drops inside it (22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

impl Runner {
    /// Claims the next task by class and steps it once; with none to claim, registers `waker` on
    /// this runner's node among the idle runners, so the next link wakes it. Whether it stepped
    /// one. Never from inside a step or a waker the executor invoked.
    #[turso_macros::atoll_entry]
    pub(crate) fn run_one(&self, waker: &std::task::Waker) -> bool {
        refuse_entry_from_inside();
        // Cloned before any executor mutex is taken, and what it replaces dropped after.
        let waker = waker.clone();
        let (claimed, replaced) = {
            let mut queue = lock(&self.shared.queue);
            match queue.claim() {
                Some(header) => (
                    Some((header, Calling::enter(&self.shared, &mut queue))),
                    None,
                ),
                None if queue.closing => (None, None),
                None => (None, queue.register_external(self.node, waker)),
            }
        };
        drop(replaced);
        match claimed {
            Some((header, call)) => {
                self.shared.step(header);
                drop(call);
                true
            }
            None => false,
        }
    }
}

impl Runner {
    /// With nothing queued, swaps `waker` into this runner's node among the idle runners, so the
    /// next link wakes it, and says so; with something queued, leaves it and says so, so its host
    /// polls again at once. Under the queue's mutex.
    fn rest_or_queued(&self, waker: &std::task::Waker) -> bool {
        // Cloned before the mutex, and what it replaces dropped after.
        let waker = waker.clone();
        let (queued, replaced) = {
            let mut queue = lock(&self.shared.queue);
            if queue.closing || queue.lists.iter().any(|list| !list.is_empty()) {
                (!queue.closing, None)
            } else {
                (false, queue.register_external(self.node, waker))
            }
        };
        drop(replaced);
        queued
    }
}

/// A cooperative wait on this executor's work (22.7): each poll runs one task step and, on a
/// pumped backend, one IO step, then, unfinished, has its waker registered on the completion and,
/// with nothing queued, on its own node among the idle runners, or wakes it at once while
/// something is queued, so its host has control back after each unit. With workers it only
/// waits.
pub(crate) struct Wait<'a> {
    executor: &'a Executor,
    completion: crate::io::CompletionFuture,
    /// The wait's runner, with its node among the idle runners, while it may still run the
    /// executor: none with workers, and none once the wait is ready, so a link after that wakes
    /// a live runner rather than this finished one.
    runner: Option<Runner>,
    /// Dropped last: the end of the frame its `Drop` opens, so the host's wakers the completion
    /// and the runner held drop inside it, the completion's after the body (22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

impl Wait<'_> {
    /// Gives the runner's node and the backend registration back, the wait's own and no longer
    /// needed: at its ready, and at its drop if it never was.
    fn release(&mut self) {
        drop(self.runner.take());
        if let Some(key) = self.completion.ready_key() {
            if self.executor.shared.io.delivery() == Delivery::Pumped {
                self.executor.shared.io.withdraw(key);
            }
        }
    }
}

impl std::future::Future for Wait<'_> {
    type Output = Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        // An entry (22.11): the host's waker, cloned, invoked or dropped here, panics into a
        // frame that ends the process, as every waker Atoll runs does.
        let _frame = crate::storage::atoll::unwind::Frame::enter();
        refuse_entry_from_inside();
        // SAFETY: `completion` is never moved while pinned, here or by the drop, and no other
        // field is pinned. There is no Unpin implementation for the enclosing future.
        let this = unsafe { self.get_unchecked_mut() };
        let completion = unsafe { std::pin::Pin::new_unchecked(&mut this.completion) };
        if let std::task::Poll::Ready(ended) = completion.poll(cx) {
            this.release();
            return std::task::Poll::Ready(ended);
        }
        let Some(runner) = &this.runner else {
            // Workers run the executor: the wait only waits.
            return std::task::Poll::Pending;
        };
        let shared = &this.executor.shared;
        let stepped = runner.run_one(cx.waker());
        let delivered = shared.io.delivery() == Delivery::Pumped && {
            // Polled pending, the completion's future has its key: the backend's too.
            let key = this
                .completion
                .ready_key()
                .expect("a pending completion future without its key");
            match shared.io.try_step(key, cx.waker()) {
                Err(_) => crate::io::stepping_failed("a wait on executor work"),
                Ok(Stepped::Delivered) => true,
                Ok(Stepped::Empty) => false,
                // Refused at creation, as Atoll refuses it at open.
                Ok(Stepped::Unsaid) => {
                    unreachable!("a backend that says what its steps find answered Unsaid")
                }
            }
        };
        // A runner that stepped rests its node or polls again; one that did not has its node.
        let again = this.completion.completion().finished()
            || delivered
            || (stepped && runner.rest_or_queued(cx.waker()));
        if again {
            // Invoked as a drain invokes a waker: a host entering the executor from it is
            // refused (22.7).
            let _inside = Inside::enter();
            cx.waker().wake_by_ref();
        }
        std::task::Poll::Pending
    }
}

impl Drop for Wait<'_> {
    /// Gives the runner's node and the backend registration back, if the wait never was ready.
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {
        self.release();
    }
}

impl Drop for Runner {
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {
        let (gone, moved) = lock(&self.shared.queue).remove_node(self.node);
        drop(gone);
        self.shared.drain(moved);
    }
}

/// A task as its creator names it.
#[derive(Clone)]
pub(crate) struct TaskRef(Arc<Header>);

impl TaskRef {
    /// Removes the task with its subject (22.3: a log closed, a rollback finished): closed under
    /// the queue's mutex and unlinked if queued, so no runner claims or links it again, then off
    /// every signal it declared and out of the table, its job dropped, by this caller, or by the
    /// runner stepping it as that step ends. Its header goes with the last completion holding its
    /// waker, and a wake of it does nothing. A task done, closed or removed already, or one whose
    /// executor is closing, is left to its end or to close.
    pub(crate) fn remove(&self) {
        let Some(shared) = self.0.shared.upgrade() else {
            return;
        };
        let counted = {
            let mut queue = lock(&shared.queue);
            if queue.closing {
                return;
            }
            // Under the mutex only a step's end moves the task out of running, and only by
            // compare-exchange: whichever exchange comes second sees the other. So the loop runs
            // at most once more for each move that one end makes (to idle, notified, done).
            let mut state = self.0.state.load(Ordering::Acquire);
            loop {
                if state == DONE || state == CLOSED {
                    return;
                }
                match self.0.state.compare_exchange(
                    state,
                    CLOSED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(now) => state = now,
                }
            }
            if state == QUEUED {
                let class = self.0.class.load(Ordering::Relaxed) as usize;
                let list = &mut queue.lists[class];
                let at = list.iter().position(|h| Arc::ptr_eq(h, &self.0));
                list.remove(at.expect("a queued task linked in no list"));
            }
            // A running task's end lets its job go when it finds the task closed. Otherwise the
            // job is this call's to let go, counted under the mutex that read close not begun,
            // until its drop has ended: close waits for it as for any call.
            (state & RUNNING == 0).then(|| Calling::enter(&shared, &mut queue))
        };
        if let Some(_counted) = counted {
            shared.release_closed(&self.0);
        }
    }

    /// Claims the task by name and steps it once, on the calling thread, on its own executor
    /// (22.7): queued, it is unlinked and claimed; idle, it runs for the caller's own reason;
    /// running, done or closed, it is left. Whether it was stepped. An entry, as
    /// [`Executor::run_named`] is, and never from inside a step or a waker the executor invoked.
    #[turso_macros::atoll_entry]
    pub(crate) fn run_once(&self) -> bool {
        refuse_entry_from_inside();
        let Some(shared) = self.0.shared.upgrade() else {
            return false;
        };
        shared.run_named(&self.0)
    }

    /// Whether `other` names this task.
    pub(crate) fn same(&self, other: &TaskRef) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Whether it is queued: work it has to do, waiting for a runner. One load of its state.
    pub(crate) fn queued(&self) -> bool {
        self.0.state.load(Ordering::Acquire) == QUEUED
    }

    /// Whether workers of its executor's own step it: with none, whoever waits on its work runs
    /// the executor (22.7). False once the executor is gone.
    pub(crate) fn driven(&self) -> bool {
        self.0.shared.upgrade().is_some_and(|shared| shared.driven)
    }

    /// Wakes the task, as a completion it waits on would.
    pub(crate) fn wake(&self) {
        wake(&self.0);
    }

    /// Whether its job is over.
    pub(crate) fn is_done(&self) -> bool {
        self.0.state.load(Ordering::Acquire) == DONE
    }

    /// Whether it will never step again: done, or closed (removed, or its executor closing).
    pub(crate) fn is_over(&self) -> bool {
        matches!(self.0.state.load(Ordering::Acquire), DONE | CLOSED)
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
            driven: workers > 0,
        });
        let mut executor = Self {
            shared,
            workers: Vec::with_capacity(workers),
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
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
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        }
    }

    /// Claims the next task by class and steps it once, on the calling thread; whether there was
    /// one. A caller's runner (22.7), and the deterministic one; an entry, so a refusal in it ends
    /// the process at its own frame (22.11).
    #[turso_macros::atoll_entry]
    pub(crate) fn run_one(&self) -> bool {
        refuse_entry_from_inside();
        self.shared.run_one(true)
    }

    /// Claims `task` by name and steps it once, on the calling thread (22.7): queued, it is taken
    /// out of its list; idle, it runs for its claimer's own reason; running, done or closed, it is
    /// left. Whether it was stepped. An entry, as [`run_one`](Self::run_one) is.
    #[turso_macros::atoll_entry]
    pub(crate) fn run_named(&self, task: &TaskRef) -> bool {
        refuse_entry_from_inside();
        assert!(
            std::ptr::eq(task.0.shared.as_ptr(), Arc::as_ptr(&self.shared)),
            "a task claimed by name on another executor"
        );
        self.shared.run_named(&task.0)
    }

    /// Waits for `c`, which this executor's work finishes, holding the calling thread (22.7): with
    /// workers it only waits; without, it runs the executor until `c` finishes. Never from inside
    /// a step or a waker the executor invoked.
    #[turso_macros::atoll_entry]
    pub(crate) fn wait(&self, c: &Completion) {
        // A wait, which the ring's delivery thread never makes (22.5).
        crate::io::refuse_on_delivery_thread("a wait on executor work");
        refuse_entry_from_inside();
        if c.finished() {
            return;
        }
        let me = crate::thread::current();
        // Registered before every test below: a finish after it unparks this thread.
        c.set_waker(&crate::io::unparking(me.clone()));
        if !self.workers.is_empty() {
            while !c.finished() {
                crate::storage::atoll::park::thread();
            }
            return;
        }
        self.shared.run_until(c, &me);
    }

    /// The cooperative form of [`wait`](Self::wait), for a caller that polls.
    pub(crate) fn wait_async(&self, c: Completion) -> Wait<'_> {
        Wait {
            executor: self,
            completion: std::future::IntoFuture::into_future(c),
            runner: self.workers.is_empty().then(|| self.runner()),
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        }
    }

    /// Closes the executor, as dropping it does.
    pub(crate) fn close(self) {
        drop(self);
    }

    /// Closes the executor in 22.12's order: `durable`, the end each open log had when close
    /// began made durable, waited for while every job still runs (step 1); closing published,
    /// every step in progress ended and every worker joined (2); each task's logical waits given
    /// up (3); every request `requests` counts retired (4); every job dropped (5). A `durable`
    /// that failed ends its wait at once, close goes on through every later step, and the failure
    /// is logged and returned after the last. Never from inside a step or a waker the executor
    /// invoked.
    pub(crate) fn close_after(mut self, durable: &Completion, requests: &Requests) -> Result<()> {
        self.close_in_order(durable, requests);
        drop(self);
        match durable.get_error() {
            None => Ok(()),
            Some(e) => {
                tracing::error!("the logs were not made durable before close: {e}");
                Err(crate::LimboError::CompletionError(e))
            }
        }
    }

    /// Steps 1 to 5 of close (22.12); dropping the executor afterwards finds nothing left.
    #[turso_macros::atoll_entry]
    fn close_in_order(&mut self, durable: &Completion, requests: &Requests) {
        self.wait(durable);
        self.shut(Some(requests));
    }

    /// Closes the executor of a database whose open did not finish, in close's order from step 2
    /// (22.12), with no log to make durable: closing published and every step ended, waits given
    /// up, every request `requests` counts retired (which stops the IO thread), every job dropped
    /// (DESIGN.md 22.18, D1a §4).
    pub(crate) fn close_unopened(mut self, requests: &Requests) {
        self.shut_unopened(requests);
        drop(self);
    }

    #[turso_macros::atoll_entry]
    fn shut_unopened(&mut self, requests: &Requests) {
        self.shut(Some(requests));
    }

    /// Closes every task, so none is claimed or linked again, wakes the idle workers and joins
    /// them once their steps in progress end, all but the calling thread when a step of its own
    /// dropped the executor (it leaves once that step ends); takes every job out of the table,
    /// gives up each task's logical waits on this thread, waits for every request `requests`
    /// counts to retire, and drops every job, after the table's mutex. A second close finds
    /// nothing left to do.
    fn shut(&mut self, requests: Option<&Requests>) {
        // A close joins workers and waits for calls, which the ring's delivery thread never does:
        // what they wait for may be its own next delivery (22.5).
        crate::io::refuse_on_delivery_thread("an executor's close");
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
                close_unless_done(&job.header);
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
        // Every call ends before the jobs go, an external call's step and a removal's job drop, but
        // this thread's own, when one of them dropped the executor: no call starts once closing is
        // published.
        let mine = self.shared.address();
        let own = CALLING.with(|calling| calling.borrow().iter().filter(|&&c| c == mine).count());
        loop {
            {
                let mut queue = lock(&self.shared.queue);
                if queue.calls <= own {
                    break;
                }
                queue.closer = Some(crate::thread::current());
            }
            crate::race::point("executor.close_waits");
            // Unparked by a call's end, or spuriously: either way it looks again.
            crate::storage::atoll::park::thread();
        }
        let jobs = std::mem::take(&mut *lock(&self.shared.jobs));
        // Each task off every list it declared, outside the queue's mutex (22.3's lock order).
        for job in &jobs {
            for signal in &job.watches {
                signal.leave(&job.header);
            }
        }
        // Each task's logical waits given up, every step ended and outside any, so no task waits
        // for a producer close stopped; inside, so one entering the executor from its
        // abandonment is refused (22.12 step 3).
        {
            let _inside = Inside::enter();
            for job in &jobs {
                job.abandon();
            }
        }
        // No request lands on a file once close returns (22.12 step 4).
        if let Some(requests) = requests {
            requests.drain(&*self.shared.io);
        }
        drop(jobs);
    }
}

/// Closes a task its executor's close finds in its table, unless the task's step's end set it done
/// first: a worker's end sets the word done and lets the job go after, so a task close meets
/// between the two has ended, and stays done.
fn close_unless_done(header: &Header) {
    let mut state = header.state.load(Ordering::Acquire);
    while state != DONE && state != CLOSED {
        match header
            .state
            .compare_exchange(state, CLOSED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return,
            Err(now) => state = now,
        }
    }
}

impl Drop for Executor {
    /// Closes the executor: a started one, or one whose start failed after some of its workers
    /// began, leaves no worker running.
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {
        self.shut(None);
    }
}
