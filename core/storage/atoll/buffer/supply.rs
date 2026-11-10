//! The pool's one supply (DESIGN.md 9.1.3): every change that can turn a waiter's miss into a hit
//! is made inside a [`Supply::change`] scope that the change arms, and every wait is a
//! [`Ticket`] taken before the look it follows. The scope's close publishes the cells' completions
//! and hands their wakers to the thread's notification batch ([`notify`]), and no waker runs while
//! a scope is open on the thread. A remote waiter wakes on every change; a stepped waiter, whose
//! producer is the pool's IO at its backend, on a landing of that IO alone, which its steps bring:
//! no other change, its own caller's included, sends it back to look without a step.

use super::cell::{CompletionCell, CompletionDomain};
use crate::io::Completion;
use crate::storage::atoll::notify;
use crate::storage::atoll::page::SizeClass;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use std::cell::RefCell;
use std::marker::PhantomData;

/// The two kinds of wait a caller may need: one its waiter steps the backend for, finished by the
/// next landing of the pool's IO, and one another thread finishes, by any change, which a
/// synchronous waiter blocks on.
const STEPPED: usize = 0;
const REMOTE: usize = 1;

pub struct Supply {
    /// The shortage gate holder's phase-A demand and the frame handed to it.
    pub handoff: Arc<Handoff>,
    /// The gate holder's own admission wait while its demand stands: finished by the offer that
    /// hands it a frame (DESIGN.md 9.1.3, phase A's grant).
    grant: CompletionCell,
    /// The checkpoint's memory demand and the frame handed to it, served before the gate
    /// holder's (DESIGN.md 9.1.3, *Maintenance keeps what it owns*): one, as one checkpoint images
    /// at a time.
    pub upkeep: Handoff,
    /// The checkpoint's own wait while its demand stands: finished by the offer that hands it a
    /// frame.
    upkeep_grant: CompletionCell,
    cells: [CompletionCell; 2],
    /// The rare path's gate's one loser cell (DESIGN.md 9.1.3, rule 4): a caller that fails to
    /// take the gate waits on it alone; the holder's let-go finishes it.
    gate: CompletionCell,
    /// The domain the cells' completions are read under: the supply's own.
    domain: CompletionDomain,
    /// Scopes made and not yet through their close, armed or not: what the orphan check counts as
    /// a producer between its change and its wake.
    #[cfg(test)]
    alive: std::sync::atomic::AtomicUsize,
}

/// One open scope of a pool on this thread: the pool's supply, whether a change armed it, and
/// whether one was a landing of the pool's IO.
struct Open {
    supply: usize,
    armed: bool,
    landed: bool,
}

#[cfg(not(atoll_loom))]
crate::thread::thread_local! {
    /// The scopes open on this thread, one per pool at most.
    static OPEN: RefCell<Vec<Open>> = const { RefCell::new(Vec::new()) };
}

#[cfg(atoll_loom)]
loom::thread_local! {
    static OPEN: RefCell<Vec<Open>> = RefCell::new(Vec::new());
}

/// A change scope's event, borrowed for the closure alone: a function that makes supply takes it
/// and [`arms`](Event::arm) it on the branch that makes its change. Not `Send`: it names a scope
/// of this thread.
pub struct Event<'s> {
    supply: usize,
    _scope: PhantomData<(&'s Supply, *const ())>,
}

impl Event<'_> {
    /// The change was made: the scope publishes to remote waiters as it closes.
    pub fn arm(&self) {
        self.mark(false);
    }

    /// The change was a landing of the pool's IO, a request leaving its backend: the scope
    /// publishes to every waiter as it closes, stepped ones too.
    pub fn land(&self) {
        self.mark(true);
    }

    fn mark(&self, landed: bool) {
        OPEN.with(|open| {
            let mut open = open.borrow_mut();
            let scope = open
                .iter_mut()
                .find(|s| s.supply == self.supply)
                .expect("an event of a scope that is not open");
            scope.armed = true;
            scope.landed |= landed;
        });
    }
}

/// A registration on the supply taken before a look: both kinds, so the wait the look decides on
/// can be of the kind its producers need with no registration after the look.
pub struct Ticket {
    stepped: Completion,
    remote: Completion,
}

impl Ticket {
    /// Finished: a change was published after the ticket was taken (or the wait was also
    /// registered elsewhere and finished there).
    pub fn moved(&self) -> bool {
        self.stepped.finished() || self.remote.finished()
    }

    /// The wait for a waiter that steps the backend: a producer of its own is the pool's IO at
    /// its backend. Finished by the next landing alone.
    pub fn stepped(self) -> Completion {
        self.stepped
    }

    /// The wait for a waiter whose producers are all other callers' acts.
    pub fn remote(self) -> Completion {
        self.remote
    }
}

impl Supply {
    pub fn new(epochs: std::sync::Arc<crate::storage::atoll::epoch::Epochs>) -> Self {
        Self {
            handoff: Arc::default(),
            grant: CompletionCell::default(),
            upkeep: Handoff::default(),
            upkeep_grant: CompletionCell::default(),
            cells: Default::default(),
            gate: CompletionCell::default(),
            domain: CompletionDomain::new(epochs),
            #[cfg(test)]
            alive: Default::default(),
        }
    }

    /// Hands `frame` of `class`, just freed or set aside from a run, to the gate holder's demand
    /// of `class` if one stands, finishing the holder's own admission wait: false if none stands,
    /// when the caller frees the frame as before.
    pub fn offer(&self, class: SizeClass, frame: u32) -> bool {
        // The checkpoint first: its progress frees what the gate holder may be waiting for.
        if self.upkeep.offer(class, frame) {
            crate::sync::atomic::fence(Ordering::SeqCst);
            if let Some(grant) = self.upkeep_grant.take(&self.domain) {
                notify::finish(&grant, Ok(0));
            }
            return true;
        }
        if !self.handoff.offer(class, frame) {
            return false;
        }
        // Pairs with the holder's fence after it posts its grant: either this take finds it, or
        // the holder's look after the post finds the frame handed.
        crate::sync::atomic::fence(Ordering::SeqCst);
        if let Some(grant) = self.grant.take(&self.domain) {
            notify::finish(&grant, Ok(0));
        }
        true
    }

    /// Posts `grant`, the gate holder's own admission wait, for the offer that hands it a frame;
    /// the one posted before taken back. Under the shortage queue's mutex.
    pub fn post_grant(&self, grant: Completion) {
        drop(self.grant.take(&self.domain));
        self.grant.put(grant);
    }

    /// Takes the posted grant back: its holder adopted its frame, or left. Under the shortage
    /// queue's mutex.
    pub fn take_grant(&self) {
        drop(self.grant.take(&self.domain));
    }

    /// Whether a demand of `class` stands, the checkpoint's or the gate holder's: a run formed
    /// for that class sets a frame aside for it.
    pub fn wants(&self, class: SizeClass) -> bool {
        self.upkeep.wants(class) || self.handoff.wants(class)
    }

    /// [`post_grant`](Self::post_grant) for the checkpoint's demand. By its stepper alone.
    pub fn post_upkeep_grant(&self, grant: Completion) {
        drop(self.upkeep_grant.take(&self.domain));
        self.upkeep_grant.put(grant);
    }

    /// [`take_grant`](Self::take_grant) for the checkpoint's demand. By its stepper alone.
    pub fn take_upkeep_grant(&self) {
        drop(self.upkeep_grant.take(&self.domain));
    }

    fn id(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

    fn scope_open(&self) -> bool {
        let id = self.id();
        OPEN.with(|open| open.borrow().iter().any(|s| s.supply == id))
    }

    /// A ticket on both kinds. Refused while a scope of this pool is open on the thread: that
    /// scope would finish it.
    pub fn ticket(&self) -> Ticket {
        let domain = &self.domain;
        crate::turso_assert!(
            !self.scope_open(),
            "a supply ticket taken inside a change scope of its pool"
        );
        Ticket {
            stepped: self.cells[STEPPED].current_with(domain, crate::storage::atoll::notify::wait),
            remote: self.cells[REMOTE].current_with(domain, crate::storage::atoll::notify::remote_wait),
        }
    }

    /// Runs `f` in a change scope of this pool: a new one, or the one open on this thread, which
    /// `f`'s event then arms. A new scope, armed by its close, publishes the remote cell's
    /// completion, and the stepped cell's too when the event was a landing (`Event::land`: a
    /// request leaving its backend, or an image-bank lease given back), and hands their wakers to
    /// the thread's notification batch.
    pub fn change<R>(&self, f: impl FnOnce(&Event<'_>) -> R) -> R {
        let domain = &self.domain;
        let id = self.id();
        let event = Event {
            supply: id,
            _scope: PhantomData,
        };
        if self.scope_open() {
            return f(&event);
        }
        OPEN.with(|open| {
            open.borrow_mut().push(Open {
                supply: id,
                armed: false,
                landed: false,
            })
        });
        notify::enter();
        #[cfg(test)]
        self.alive.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let closing = Closing { supply: self };
        let result = f(&event);
        let (armed, landed) = OPEN.with(|open| {
            open.borrow()
                .iter()
                .find(|s| s.supply == id)
                .map_or((false, false), |s| (s.armed, s.landed))
        });
        if armed {
            // Both taken and published before either kind's wakers run (DESIGN.md 9.1.3, rule 1).
            let taken = [
                landed.then(|| self.cells[STEPPED].take(domain)).flatten(),
                self.cells[REMOTE].take(domain),
            ];
            let wakers = taken.map(|c| c.map(|c| c.finish_deferred(Ok(0))));
            for w in wakers.into_iter().flatten() {
                notify::hold(w);
            }
        }
        drop(closing);
        result
    }

    /// The wait of a caller about to try the gate, taken before its compare-and-swap: if it loses,
    /// this is all it waits on, the holder letting go within its own call. Of the remote kind:
    /// its only producer is another caller. The fence orders the registration before the try, as
    /// [`gate_let_go`](Self::gate_let_go) orders the word freed before its take: of a loser's read
    /// of the gate and the holder's take of the cell, one sees the other.
    pub fn gate_wait(&self) -> Completion {
        let wait = self
            .gate
            .current_with(&self.domain, crate::storage::atoll::notify::remote_wait);
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        wait
    }

    /// The gate's word was freed: every caller that lost it is woken, through the thread's
    /// notification batch. Not a supply change: a holder that found nothing wakes no other
    /// waiter, and never itself, as it never registers on the gate it holds.
    pub fn gate_let_go(&self) {
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        if let Some(wait) = self.gate.take(&self.domain) {
            notify::finish(&wait, Ok(0));
        }
    }

    /// Scopes made and not yet through their close.
    #[cfg(test)]
    pub fn alive(&self) -> usize {
        self.alive.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Clears the scope's record and leaves the thread's notification batch as the scope closes,
/// after its publication: the held wakers then run if no other scope is open on the thread. An
/// unwind through a change is fatal, as every unwind through the pool's state is.
struct Closing<'s> {
    supply: &'s Supply,
}

impl Drop for Closing<'_> {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        let id = self.supply.id();
        OPEN.with(|open| {
            let mut open = open.borrow_mut();
            let at = open
                .iter()
                .position(|s| s.supply == id)
                .expect("the scope closing is open");
            open.swap_remove(at);
        });
        #[cfg(test)]
        self.supply
            .alive
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        notify::leave();
    }
}

#[cfg(all(test, not(atoll_loom), not(shuttle)))]
mod tests {
    use super::*;
    use crate::storage::atoll::epoch::Epochs;
    use crate::storage::atoll::locks::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    fn supply() -> Supply {
        Supply::new(Arc::new(Epochs::new()))
    }

    /// A waker that counts its wakes and runs `then` on each, on the waking thread.
    struct Counting {
        woken: AtomicUsize,
        then: Mutex<Option<Box<dyn FnMut() + Send>>>,
    }

    impl Wake for Counting {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.woken.fetch_add(1, Ordering::SeqCst);
            if let Some(then) = self.then.lock().as_mut() {
                then();
            }
        }
    }

    fn counting(then: Option<Box<dyn FnMut() + Send>>) -> (Arc<Counting>, Waker) {
        let c = Arc::new(Counting {
            woken: AtomicUsize::new(0),
            then: Mutex::new(then),
        });
        (c.clone(), Waker::from(c))
    }

    #[test]
    fn a_scope_no_change_armed_finishes_no_ticket() {
        let supply = supply();
        let ticket = supply.ticket();
        supply.change(|_| {});
        assert!(!ticket.moved(), "an unarmed scope finished a ticket");
        assert_eq!(supply.alive(), 0);
    }

    #[test]
    fn an_armed_scope_publishes_both_kinds_before_any_waker_runs() {
        let supply = supply();
        let ticket = supply.ticket();
        let remote = ticket.remote.clone();
        let saw = Arc::new(AtomicUsize::new(0));
        let (stepped_waker, waker) = {
            let (remote, saw) = (remote.clone(), saw.clone());
            counting(Some(Box::new(move || {
                // The other kind is already published when either kind's waker runs.
                if remote.finished() {
                    saw.fetch_add(1, Ordering::SeqCst);
                }
            })))
        };
        ticket.stepped.set_waker(&waker);
        supply.change(|event| event.land());
        assert_eq!(stepped_waker.woken.load(Ordering::SeqCst), 1);
        assert_eq!(
            saw.load(Ordering::SeqCst),
            1,
            "a waker ran before both kinds published"
        );
        assert!(remote.finished());
    }

    /// A stepped wait's producer is the pool's IO at its backend: only a landing finishes it, so
    /// no other change, its own caller's act of letting a page go included, sends its waiter back
    /// to look without a step (the auditor's root-growth pressure, #3850). A remote wait wakes on
    /// every change, a landing included.
    #[test]
    fn only_a_landing_finishes_a_stepped_wait_and_every_change_a_remote_one() {
        let supply = supply();
        let ticket = supply.ticket();
        let (stepped, remote) = (ticket.stepped.clone(), ticket.remote.clone());
        supply.change(|event| event.arm());
        assert!(
            !stepped.finished(),
            "a change other than a landing finished a stepped wait"
        );
        assert!(remote.finished(), "a change did not finish a remote wait");
        let ticket = supply.ticket();
        assert!(!ticket.moved(), "a new ticket was finished");
        let (stepped, remote) = (ticket.stepped.clone(), ticket.remote.clone());
        supply.change(|event| event.land());
        assert!(
            stepped.finished() && remote.finished(),
            "a landing finished not both kinds"
        );
        assert_eq!(supply.alive(), 0);
    }

    #[test]
    fn a_joined_change_arms_the_open_scope_and_it_publishes_once_as_it_closes() {
        let supply = supply();
        let ticket = supply.ticket();
        let (counter, waker) = counting(None);
        ticket.remote.set_waker(&waker);
        supply.change(|_| {
            // A release made inside the scope, by a destructor or a callee, joins it.
            supply.change(|event| event.arm());
            supply.change(|_| {});
            assert_eq!(
                counter.woken.load(Ordering::SeqCst),
                0,
                "a joined change woke before its scope closed"
            );
            assert!(
                !ticket.moved(),
                "a joined change published before its scope closed"
            );
        });
        assert_eq!(counter.woken.load(Ordering::SeqCst), 1);
        assert!(ticket.moved());
        assert_eq!(supply.alive(), 0);
    }

    #[test]
    #[should_panic(expected = "a supply ticket taken inside a change scope of its pool")]
    fn a_ticket_is_refused_while_a_scope_of_its_pool_is_open_on_the_thread() {
        let supply = supply();
        // The record a scope keeps, made by hand: the refusal's panic must not unwind through a
        // real scope (an unwind through a change is fatal).
        OPEN.with(|o| {
            o.borrow_mut().push(Open {
                supply: supply.id(),
                armed: false,
                landed: false,
            })
        });
        supply.ticket();
    }

    #[test]
    fn a_ticket_is_taken_inside_another_pools_scope() {
        let (supply, other) = (supply(), supply());
        other.change(|_| {
            let _ = supply.ticket();
        });
    }

    #[test]
    fn a_second_pools_waker_runs_only_when_the_threads_last_scope_closes() {
        let (a, b) = (Arc::new(supply()), Arc::new(supply()));
        let ticket = b.ticket();
        let reentered = Arc::new(AtomicUsize::new(0));
        let (counter, waker) = {
            let (a, reentered) = (a.clone(), reentered.clone());
            counting(Some(Box::new(move || {
                // A non-blocking waker that enters pool A again on this thread: A's scope must
                // be closed by now, so its ticket is not refused.
                assert!(!notify::open(), "a waker ran while a scope was open");
                let _ = a.ticket();
                reentered.fetch_add(1, Ordering::SeqCst);
            })))
        };
        ticket.remote.set_waker(&waker);
        a.change(|_| {
            b.change(|event| event.arm());
            assert!(ticket.moved(), "B's result was not published at B's close");
            assert_eq!(
                counter.woken.load(Ordering::SeqCst),
                0,
                "B's waker ran inside A's scope"
            );
        });
        assert_eq!(reentered.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_logical_signal_finished_inside_a_scope_wakes_after_it_closes() {
        let supply = supply();
        let wait = crate::storage::atoll::notify::remote_wait();
        let (counter, waker) = counting(None);
        wait.set_waker(&waker);
        supply.change(|_| {
            notify::finish(&wait, Ok(0));
            assert!(wait.finished(), "the result is published at once");
            assert_eq!(
                counter.woken.load(Ordering::SeqCst),
                0,
                "woken inside the scope"
            );
        });
        assert_eq!(counter.woken.load(Ordering::SeqCst), 1);
        // Outside a scope, at once.
        let again = crate::storage::atoll::notify::wait();
        let (counter, waker) = counting(None);
        again.set_waker(&waker);
        notify::finish(&again, Err(crate::CompletionError::Aborted));
        assert_eq!(counter.woken.load(Ordering::SeqCst), 1);
        assert!(again.failed());
    }

    #[test]
    #[should_panic(expected = "a deferred finish of a completion the engine did not make")]
    fn a_deferred_finish_of_a_request_is_refused() {
        let request = Completion::new_sync(|_| {});
        let _ = request.finish_deferred(Ok(0));
    }

    #[test]
    #[should_panic(expected = "a deferred finish of a completion the engine did not make")]
    fn a_deferred_finish_of_a_plain_wait_is_refused() {
        let _ = Completion::new_remote_wait().finish_deferred(Ok(0));
    }

    #[test]
    #[should_panic(expected = "an engine's wait added to a group")]
    fn an_engines_wait_is_refused_by_a_group() {
        let mut group = crate::io::CompletionGroup::new(|_| {});
        group.add(&supply().ticket().remote());
    }

    #[test]
    fn waits_finished_outside_any_scope_by_wakers_do_not_nest_deliveries() {
        // A chain of engine waits, each waker finishing the next with no scope open (as an epoch's
        // notification does): one delivery runs them in turn (the auditor's direct-signal chain).
        let depth = Arc::new(AtomicUsize::new(0));
        let deepest = Arc::new(AtomicUsize::new(0));
        let waits: Vec<_> = (0..24).map(|_| crate::storage::atoll::notify::wait()).collect();
        for i in 0..waits.len() {
            let next = waits.get(i + 1).cloned();
            let (d, m) = (depth.clone(), deepest.clone());
            let (_, waker) = counting(Some(Box::new(move || {
                let now = d.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(now, Ordering::SeqCst);
                if let Some(next) = &next {
                    notify::finish(next, Ok(0));
                }
                d.fetch_sub(1, Ordering::SeqCst);
            })));
            waits[i].set_waker(&waker);
        }
        notify::finish(&waits[0], Ok(0));
        assert!(waits.iter().all(Completion::finished));
        assert_eq!(deepest.load(Ordering::SeqCst), 1, "deliveries nested");
    }

    #[test]
    fn a_waker_that_closes_scopes_of_its_own_does_not_nest_deliveries() {
        // A chain of wakers, each opening and closing a scope that wakes the next: delivered one
        // after another by the outermost delivery, never inside one another.
        let supply = Arc::new(supply());
        let depth = Arc::new(AtomicUsize::new(0));
        let deepest = Arc::new(AtomicUsize::new(0));
        const CHAIN: usize = 24;
        fn link(
            supply: Arc<Supply>,
            depth: Arc<AtomicUsize>,
            deepest: Arc<AtomicUsize>,
            left: usize,
        ) {
            if left == 0 {
                return;
            }
            let wait = supply.ticket().remote();
            let (s, d, m) = (supply.clone(), depth.clone(), deepest.clone());
            let (_, waker) = counting(Some(Box::new(move || {
                let now = d.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(now, Ordering::SeqCst);
                link(s.clone(), d.clone(), m.clone(), left - 1);
                s.change(|event| event.arm());
                d.fetch_sub(1, Ordering::SeqCst);
            })));
            wait.set_waker(&waker);
        }
        link(supply.clone(), depth.clone(), deepest.clone(), CHAIN);
        supply.change(|event| event.arm());
        assert_eq!(deepest.load(Ordering::SeqCst), 1, "deliveries nested");
    }
}

/// The shortage gate holder's phase-A demand and the frame handed to it (DESIGN.md 9.1.3,
/// *Tenure*, phase A): empty, a class wanted, or that class and a frame handed. The holder names,
/// adopts and withdraws under the shortage queue's mutex; a free or a run's hand-over hands a
/// frame by one compare-and-swap from the class alone. So a frame goes once, to the demand that
/// stood, and a demand withdrawn takes whatever was handed to it with it: nothing is left behind.
#[derive(Default)]
pub struct Handoff(AtomicU64);

/// The handed flag, above the class's index plus one; the frame in the high half.
const HANDED: u64 = 1 << 8;

fn wanted(class: SizeClass) -> u64 {
    class.index() as u64 + 1
}

impl Handoff {
    /// Whether a demand of `class` stands, nothing handed to it yet.
    pub fn wants(&self, class: SizeClass) -> bool {
        self.0.load(Ordering::SeqCst) == wanted(class)
    }

    /// The frame handed for the holder's demand of `class`, taken; else the demand named, or
    /// left standing. Under the shortage queue's mutex.
    pub fn want(&self, class: SizeClass) -> Option<u32> {
        let mut seen = self.0.load(Ordering::SeqCst);
        loop {
            let next = if seen & HANDED != 0 {
                debug_assert_eq!(
                    seen & 0xff,
                    wanted(class),
                    "a frame handed to another demand"
                );
                0
            } else {
                debug_assert!(seen == 0 || seen == wanted(class), "another demand stands");
                wanted(class)
            };
            // SeqCst: before the holder's look that follows, so a free either sees the demand or
            // puts its frame where that look, or the supply event the free signals, finds it.
            match self
                .0
                .compare_exchange(seen, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) if seen & HANDED != 0 => return Some((seen >> 32) as u32),
                Ok(_) => return None,
                Err(now) => seen = now,
            }
        }
    }

    /// Hands `frame` of `class` to the demand of `class` that stands, if one does: false
    /// otherwise, when the caller frees the frame as before.
    pub fn offer(&self, class: SizeClass, frame: u32) -> bool {
        let handed = wanted(class) | HANDED | (u64::from(frame) << 32);
        self.wants(class)
            && self
                .0
                .compare_exchange(wanted(class), handed, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
    }

    /// Whether a frame was handed to the demand and not yet adopted or withdrawn.
    pub fn handed(&self) -> bool {
        self.0.load(Ordering::SeqCst) & HANDED != 0
    }

    /// Withdraws the demand: the frame handed to it, if one was, for the caller to adopt or
    /// free. Under the shortage queue's mutex.
    pub fn withdraw(&self) -> Option<u32> {
        let was = self.0.swap(0, Ordering::SeqCst);
        (was & HANDED != 0).then_some((was >> 32) as u32)
    }
}
