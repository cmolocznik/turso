//! Memory-shortage admission (DESIGN.md 9.1.3, *A shortage never waits under a hold*). A claim
//! made inside an operation that finds no frame does not wait there, under the operation's own
//! latches and Steps, whose let-go on the way out would finish its wait and send it round with
//! nothing gained. It records `Short` and returns a yield; the operation unwinds to its top, the
//! [`Owner`], which holds nothing of the pool, and the owner reserves the frame there: it takes its
//! place in a first-in-first-out queue, waits for its turn, and, holding the gate, claims a frame
//! as any caller holding nothing does. The frame is kept for the claim it was reserved for, which
//! takes it when the operation runs again; the gate passes on when the operation ends.

use super::supply::{Handoff, Supply};
use super::{BufferPool, PageId, Purpose, SizeClass};
use crate::io::{Completion, WakerKey};
use crate::storage::atoll::locks::Mutex;
use crate::storage::atoll::notify;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::Result;
use std::cell::{Cell, RefCell};

// ------------------------------------------------------------------------------------ queue

/// The operations waiting for memory, first in first out; the head holds the gate. Each place is
/// its operation's tenure cell (DESIGN.md 9.1.3, *Revoking an inactive tenure*): its turn, `R`, its
/// published set and barrier, whether its owner's call runs, and the signal of that. A mutex held
/// for a push, a pop, a removal or one cell's change and nothing else, taken on the shortage path
/// alone; every change of a cell's state is made under it, with its signal.
#[derive(Default)]
pub(super) struct Queue {
    slots: Mutex<Slots>,
    /// The pool's supply, whose handoff word holds the gate holder's phase-A demand and what a
    /// free or a run's hand-over handed to it, and whose grant cell its own admission wait
    /// (DESIGN.md 9.1.3, *Tenure*, phase A); named, adopted and withdrawn under the mutex alone.
    /// None for a queue of its own, which takes no demand.
    supply: Option<Arc<Supply>>,
}

/// A doubly linked list in a slab: a removal anywhere is O(1), and a slot freed is reused.
#[derive(Default)]
struct Slots {
    nodes: Vec<Node>,
    free: Option<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    /// Places handed out, for the order: raised by `checked_add`, never wrapped.
    joined: u64,
}

struct Node {
    /// Raised each time the slot is freed or its place passes to another operation, so a stale
    /// place names nothing.
    generation: u64,
    /// The waiter's turn: finished when it reaches the head. `None` while the slot is free.
    turn: Option<Completion>,
    prev: Option<usize>,
    next: Option<usize>,
    /// The connection whose operation holds the place, if any.
    conn: Option<Arc<Conn>>,
    /// Whether its owner's call runs (`Running`) or not (`Suspended`).
    running: bool,
    /// Unfinished while the owner's call runs, finished as it returns: what a newcomer of the
    /// same connection waits on.
    signal: Option<Completion>,
    /// `R`, reserved for the claim of exactly its class and purpose, not yet taken.
    reserved: Option<(SizeClass, Purpose, u32)>,
    /// The demand the gate holder waits for in phase A, named in the queue's handoff.
    demand: Option<(SizeClass, Purpose)>,
    /// A handed frame's quiet wait for a read (phase A): one per place, registered once with the
    /// epochs, finished by them or by the place's end.
    quiet: Option<Completion>,
    /// The holder's own admission wait while its demand stands (phase A's grant): finished by the
    /// offer that hands it a frame, by `under`'s producer, or by the place's end.
    grant: Option<Completion>,
    /// The wait the holder's claim found, and the key of the proxy registered on it that finishes
    /// `grant`: withdrawn when the grant is no longer waited for.
    under: Option<(Completion, WakerKey)>,
    /// Whether the tenure published a protected set (phase B).
    published: bool,
    /// The epochs' passing of the publication.
    barrier: Option<Completion>,
}

impl Node {
    fn vacant(generation: u64) -> Self {
        Node {
            generation,
            turn: None,
            prev: None,
            next: None,
            conn: None,
            running: false,
            signal: None,
            reserved: None,
            demand: None,
            quiet: None,
            grant: None,
            under: None,
            published: false,
            barrier: None,
        }
    }
}

/// A place in the queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Place {
    slot: usize,
    generation: u64,
}

/// A connection, as the identity its operations' tenures carry: one operation of a connection
/// that comes short takes the place of another of its own suspended in the queue (DESIGN.md
/// 9.1.3, *Revoking an inactive tenure*).
#[derive(Default)]
pub(crate) struct Conn {
    /// Its operations' places in the queue: read alone at a transaction's end, which then has
    /// nothing to cancel when it is 0.
    live: std::sync::atomic::AtomicUsize,
}

impl Conn {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Whether any of its operations holds a place in a shortage queue.
    pub(crate) fn queued(&self) -> bool {
        self.live.load(std::sync::atomic::Ordering::Acquire) > 0
    }
}

/// What a tenure held when it ended or was revoked, let go after the queue's mutex: `R` freed, its
/// set retired, and its own admission completions and signal finished.
#[must_use]
pub(super) struct Released {
    reserved: Option<(SizeClass, Purpose, u32)>,
    published: bool,
    completions: Vec<Completion>,
    next_turn: Option<Completion>,
    /// The proxy registered on the wait the place's claim found, withdrawn.
    under: Option<(Completion, WakerKey)>,
}

impl Released {
    /// Lets go what a place held with no pool behind it, for a test of the queue alone, its `R`
    /// dropped with it: there is no pool to give it back to.
    #[cfg(test)]
    fn pass_reserved(mut self) {
        self.reserved = None;
        self.pass();
    }

    /// Lets go what a place held with no pool behind it, for a test of the queue alone.
    #[cfg(test)]
    fn pass(self) {
        assert!(self.reserved.is_none() && !self.published);
        if let Some((under, key)) = self.under {
            under.withdraw_key(key);
        }
        for c in self.completions.iter().chain(&self.next_turn) {
            notify::finish(c, Ok(0));
        }
    }

    fn let_go(self, pool: &BufferPool) {
        if let Some((under, key)) = self.under {
            under.withdraw_key(key);
        }
        if let Some((class, _, frame)) = self.reserved {
            pool.give_back_reserved(class, frame);
        }
        if self.published {
            pool.protect(&[]);
        }
        for c in self.completions.iter().chain(&self.next_turn) {
            notify::finish(c, Ok(0));
        }
    }
}

/// What a same-connection newcomer found.
enum Revoked {
    /// A suspended operation of its connection, whose place it took: what that one held.
    Took(Place, Released),
    /// A running one: the signal its call's return finishes.
    Running(Completion),
    /// None of its connection's.
    None,
}

impl Queue {
    /// Joins the queue's tail, the owner's call running. `turn` is finished when the place reaches
    /// the head: at once if the queue was empty.
    pub(super) fn join(&self, turn: Completion, conn: Option<Arc<Conn>>) -> Place {
        let (place, head) = {
            let mut s = self.slots.lock();
            s.joined = s
                .joined
                .checked_add(1)
                .expect("the shortage queue's count of places wrapped");
            if let Some(c) = &conn {
                c.live.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            }
            let tail = s.tail;
            let slot = match s.free {
                Some(slot) => {
                    s.free = s.nodes[slot].next;
                    slot
                }
                None => {
                    s.nodes.push(Node::vacant(0));
                    s.nodes.len() - 1
                }
            };
            let generation = s.nodes[slot].generation;
            s.nodes[slot] = Node {
                turn: Some(turn.clone()),
                prev: tail,
                conn,
                running: true,
                signal: Some(notify::remote_wait()),
                ..Node::vacant(generation)
            };
            match tail {
                Some(t) => s.nodes[t].next = Some(slot),
                None => s.head = Some(slot),
            }
            s.tail = Some(slot);
            (Place { slot, generation }, s.head == Some(slot))
        };
        if head {
            notify::finish(&turn, Ok(0));
        }
        place
    }

    fn live(s: &Slots, place: Place) -> bool {
        s.nodes[place.slot].generation == place.generation && s.nodes[place.slot].turn.is_some()
    }

    /// Whether `place` holds the gate: at the head, its turn finished. The turn is finished only
    /// once the place before it let go of its `R` and its published set, so a new head never
    /// publishes before the old one's set is retired, nor runs on the old one's frame.
    pub(super) fn holds_gate(&self, place: Place) -> bool {
        let s = self.slots.lock();
        Self::live(&s, place)
            && s.head == Some(place.slot)
            && s.nodes[place.slot]
                .turn
                .as_ref()
                .is_some_and(Completion::finished)
    }

    /// The owner's call begins: its place `Running` with a fresh signal, or `false` if the place
    /// is gone, revoked for a newcomer of its connection or cancelled with its transaction; the
    /// owner then touches nothing of it and starts its step afresh.
    pub(super) fn enter(&self, place: Place) -> bool {
        let mut s = self.slots.lock();
        if !Self::live(&s, place) {
            return false;
        }
        let node = &mut s.nodes[place.slot];
        node.running = true;
        node.signal = Some(notify::remote_wait());
        true
    }

    /// The owner's call returns: its place `Suspended`, the call's signal finished after the
    /// state is published, so no later entry's signal is finished by this exit.
    pub(super) fn exit(&self, place: Place) {
        let signal = {
            let mut s = self.slots.lock();
            if !Self::live(&s, place) {
                return;
            }
            let node = &mut s.nodes[place.slot];
            node.running = false;
            node.signal.take()
        };
        if let Some(signal) = signal {
            notify::finish(&signal, Ok(0));
        }
    }

    /// For the queue of a pool: the demands its gate holders name go on `supply`'s handoff.
    pub(super) fn new(supply: Arc<Supply>) -> Self {
        Self {
            slots: Mutex::default(),
            supply: Some(supply),
        }
    }

    fn supply(&self) -> &Supply {
        self.supply.as_deref().expect("a demand on a pool's queue")
    }

    fn handoff(&self) -> &Handoff {
        &self.supply().handoff
    }

    /// The grant posted for `node` taken back, with the proxy on its claim's wait: its demand is
    /// met or gone. Under the mutex; what it returns is let go after it ([`Done::let_go`]).
    fn grant_done(&self, node: &mut Node) -> Done {
        let grant = node.grant.take();
        if grant.is_some() {
            self.supply().take_grant();
        }
        Done {
            grant,
            under: node.under.take(),
        }
    }

    /// `R` of `place`, the gate holder in phase A: reserved already, or handed by a free or a
    /// run's hand-over and adopted here; else its demand named, so a frame of its class freed from
    /// here on is handed to it, never to the free lists where a fast claim would take it first. One
    /// acquisition: a frame handed between a look and the naming is adopted, never named over.
    pub(super) fn want(&self, place: Place, class: SizeClass, purpose: Purpose) -> Option<u32> {
        let (frame, done) = {
            let mut s = self.slots.lock();
            debug_assert!(Self::live(&s, place) && s.head == Some(place.slot));
            let node = &mut s.nodes[place.slot];
            if let Some((_, _, frame)) = node.reserved {
                (Some(frame), self.grant_done(node))
            } else {
                match self.handoff().want(class) {
                    Some(frame) => {
                        node.reserved = Some((class, purpose, frame));
                        node.demand = None;
                        (Some(frame), self.grant_done(node))
                    }
                    None => {
                        node.demand = Some((class, purpose));
                        (None, Done::default())
                    }
                }
            }
        };
        done.let_go();
        frame
    }

    /// Records `reserved`, the frame the place's own look found, as its `R`, and withdraws its
    /// demand: the frame back if one was handed to the place meanwhile, for the caller to free.
    pub(super) fn reserve(&self, place: Place, reserved: (SizeClass, Purpose, u32)) -> Option<u32> {
        let (back, done) = {
            let mut s = self.slots.lock();
            debug_assert!(Self::live(&s, place));
            let node = &mut s.nodes[place.slot];
            let done = self.grant_done(node);
            let handed = if node.demand.take().is_some() {
                self.handoff().withdraw()
            } else {
                None
            };
            let back = match handed {
                // Handed one meanwhile: the place keeps that, its own find goes back.
                Some(frame) => {
                    debug_assert!(node.reserved.is_none());
                    node.reserved = Some((reserved.0, reserved.1, frame));
                    Some(reserved.2)
                }
                None if node.reserved.is_some() => Some(reserved.2),
                None => {
                    node.reserved = Some(reserved);
                    None
                }
            };
            (back, done)
        };
        done.let_go();
        back
    }

    /// The holder's own admission wait while its demand stands, for `under`, the wait its claim
    /// found: finished by the offer that hands it a frame or by `under`'s producer, whichever comes
    /// first, with `under`'s result; of `under`'s kind, so a waiter that must step the backend for
    /// it still does. The proxy on the wait it replaces is withdrawn, so a place holds one
    /// registration on a producer at most, however often it waits (the auditor's P2-S6-3).
    pub(super) fn wait(&self, place: Place, under: Completion) -> Completion {
        let (grant, replaced, handed, proxy, key) = {
            let mut s = self.slots.lock();
            debug_assert!(Self::live(&s, place) && s.nodes[place.slot].demand.is_some());
            let node = &mut s.nodes[place.slot];
            // The same producer, still waited for: the same admission wait, so every caller that
            // took it before keeps a wait something finishes, and nothing is allocated again.
            if let (Some(grant), Some((on, _))) = (&node.grant, &node.under) {
                if !grant.finished() && on.addr().is_some() && on.addr() == under.addr() {
                    return grant.clone();
                }
            }
            let replaced = self.grant_done(node);
            let grant = if under.is_remote_wait() {
                notify::remote_wait()
            } else {
                notify::wait()
            };
            let proxy = std::sync::Arc::new(Proxy {
                grant: grant.clone(),
                under: under.clone(),
            });
            // Keyed by the grant's allocation, which this call, the node, a `Done` or a
            // `Released` keeps alive through every withdrawal of the key: no other registration
            // can carry it meanwhile, whatever becomes of the proxy (the auditor's #4113).
            let key = WakerKey::new(grant.addr().expect("an engine wait")).expect("a pointer");
            self.supply().post_grant(grant.clone());
            node.grant = Some(grant.clone());
            node.under = Some((under.clone(), key));
            // Pairs with the offer's fence: a frame handed before the grant was posted found no
            // grant to finish, and is seen here (the auditor's post gap, #4098).
            crate::sync::atomic::fence(crate::sync::atomic::Ordering::SeqCst);
            (grant, replaced, self.handoff().handed(), proxy, key)
        };
        // The wait it replaces is finished, so a caller that took it looks again: the place now
        // waits on another producer.
        replaced.let_go();
        // Registered after the mutex: a producer already finished runs every waker it holds
        // here, on this thread, which must hold nothing then (the auditor's #4104).
        crate::race::point("shortage.proxy_register");
        under.set_keyed_waker(key, &std::task::Waker::from(proxy));
        // An end, a revoke or a replacement between the mutex and the registration withdrew
        // nothing, the registration being later: withdrawn here instead.
        let current = {
            let s = self.slots.lock();
            Self::live(&s, place)
                && s.nodes[place.slot]
                    .under
                    .as_ref()
                    .is_some_and(|(_, k)| *k == key)
        };
        if !current {
            under.withdraw_key(key);
        }
        if handed {
            notify::finish(&grant, Ok(0));
        }
        grant
    }

    /// The place's quiet wait for its handed frame, while it stands unfinished.
    pub(super) fn quiet(&self, place: Place) -> Option<Completion> {
        let s = self.slots.lock();
        if !Self::live(&s, place) {
            return None;
        }
        s.nodes[place.slot]
            .quiet
            .as_ref()
            .filter(|q| !q.finished())
            .cloned()
    }

    /// Records `quiet`, registered with the epochs, as the place's quiet wait: finished by the
    /// place's end if the epochs have not.
    pub(super) fn set_quiet(&self, place: Place, quiet: Completion) {
        let mut s = self.slots.lock();
        debug_assert!(Self::live(&s, place));
        s.nodes[place.slot].quiet = Some(quiet);
    }

    /// `R`, taken by the claim of exactly its class and purpose.
    fn take_reserved(&self, place: Place, class: SizeClass, purpose: Purpose) -> Option<u32> {
        let mut s = self.slots.lock();
        if !Self::live(&s, place) {
            return None;
        }
        let node = &mut s.nodes[place.slot];
        match node.reserved {
            Some((c, p, frame)) if c == class && p == purpose => {
                node.reserved = None;
                Some(frame)
            }
            _ => None,
        }
    }

    /// Records that the place published its set, and the barrier its publication opened.
    pub(super) fn published(&self, place: Place, barrier: Option<Completion>) {
        let mut s = self.slots.lock();
        debug_assert!(Self::live(&s, place));
        let node = &mut s.nodes[place.slot];
        node.published = true;
        node.barrier = barrier;
    }

    /// The place's publication barrier, while it is not passed.
    pub(super) fn barrier(&self, place: Place) -> Option<Completion> {
        let s = self.slots.lock();
        if !Self::live(&s, place) {
            return None;
        }
        s.nodes[place.slot]
            .barrier
            .as_ref()
            .filter(|b| !b.finished())
            .cloned()
    }

    /// Unlinks `slot` under the lock and frees it: what it held, and the next place's turn if it
    /// held the gate.
    fn remove(&self, s: &mut Slots, slot: usize) -> Released {
        let (prev, next) = (s.nodes[slot].prev, s.nodes[slot].next);
        let was_head = s.head == Some(slot);
        match prev {
            Some(p) => s.nodes[p].next = next,
            None => s.head = next,
        }
        match next {
            Some(n) => s.nodes[n].prev = prev,
            None => s.tail = prev,
        }
        let generation = s.nodes[slot]
            .generation
            .checked_add(1)
            .expect("a shortage slot's generation wrapped");
        let free = s.free;
        let node = std::mem::replace(
            &mut s.nodes[slot],
            Node {
                next: free,
                ..Node::vacant(generation)
            },
        );
        s.free = Some(slot);
        // A frame handed to its demand meanwhile goes back with `R`: the two never both stand.
        let handed = node.demand.and_then(|(class, purpose)| {
            self.handoff()
                .withdraw()
                .map(|frame| (class, purpose, frame))
        });
        if node.grant.is_some() {
            self.supply().take_grant();
        }
        if let Some(c) = &node.conn {
            c.live.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
        Released {
            reserved: node.reserved.or(handed),
            published: node.published,
            // Its own turn too: an owner waiting for its admission wakes and finds the place
            // gone.
            completions: node
                .turn
                .into_iter()
                .filter(|t| !t.finished())
                .chain(node.signal)
                .chain(node.barrier)
                .chain(node.quiet)
                .chain(node.grant)
                .collect(),
            next_turn: if was_head {
                next.and_then(|n| s.nodes[n].turn.clone())
            } else {
                None
            },
            under: node.under,
        }
    }

    /// Leaves the queue (the tenure's end): what it held, to let go, the next place getting the
    /// gate if this one held it; nothing for a place already gone, revoked for a newcomer of its
    /// connection or cancelled with its transaction, whose holdings were let go there.
    pub(super) fn end(&self, place: Place) -> Option<Released> {
        let mut s = self.slots.lock();
        Self::live(&s, place).then(|| self.remove(&mut s, place.slot))
    }

    /// For a newcomer of `conn` that came short: the place of a suspended operation of its
    /// connection, taken with its turn and the gate if held, what that one held handed back to
    /// let go, and its own admission completions with it so it wakes and starts afresh; or the
    /// signal of a running one, to wait on; or nothing of its connection's.
    fn revoke_for(&self, conn: &Arc<Conn>, turn: &Completion) -> Revoked {
        let mut s = self.slots.lock();
        let mut at = s.head;
        let mut found = None;
        while let Some(i) = at {
            if s.nodes[i]
                .conn
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(c, conn))
            {
                found = Some(i);
                if !s.nodes[i].running {
                    break;
                }
            }
            at = s.nodes[i].next;
        }
        let Some(slot) = found else {
            return Revoked::None;
        };
        if s.nodes[slot].running {
            let signal = s.nodes[slot]
                .signal
                .clone()
                .expect("a running place's signal");
            return Revoked::Running(signal);
        }
        let generation = s.nodes[slot]
            .generation
            .checked_add(1)
            .expect("a shortage slot's generation wrapped");
        let head = s.head == Some(slot);
        let node = &mut s.nodes[slot];
        let old_turn = node.turn.replace(turn.clone());
        node.generation = generation;
        node.running = true;
        let signal = node.signal.replace(notify::remote_wait());
        // The newcomer names its own demand once it holds the gate; a frame handed to this one
        // goes back with `R`.
        let handed = node.demand.take().and_then(|(class, purpose)| {
            self.handoff()
                .withdraw()
                .map(|frame| (class, purpose, frame))
        });
        let grant = node.grant.take();
        if grant.is_some() {
            self.supply().take_grant();
        }
        let released = Released {
            reserved: node.reserved.take().or(handed),
            published: std::mem::take(&mut node.published),
            completions: old_turn
                .into_iter()
                .chain(signal)
                .chain(node.barrier.take())
                .chain(node.quiet.take())
                .chain(grant)
                .collect(),
            // At the head, the newcomer's turn, finished once what the place held is let go.
            next_turn: head.then(|| turn.clone()),
            under: node.under.take(),
        };
        Revoked::Took(Place { slot, generation }, released)
    }

    /// A transaction's end, which makes no demand of its own: every suspended place of `conn`
    /// ends, removed with no replacement; a running one is the caller's own and ends when its
    /// call returns.
    pub(super) fn cancel(&self, conn: &Arc<Conn>) -> Vec<Released> {
        let mut s = self.slots.lock();
        let mut at = s.head;
        let mut gone = Vec::new();
        while let Some(i) = at {
            at = s.nodes[i].next;
            let mine = s.nodes[i]
                .conn
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(c, conn));
            if mine && !s.nodes[i].running {
                gone.push(self.remove(&mut s, i));
            }
        }
        gone
    }

    /// The places in the queue, for a test of its bound.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        let s = self.slots.lock();
        let mut n = 0;
        let mut at = s.head;
        while let Some(i) = at {
            n += 1;
            at = s.nodes[i].next;
        }
        n
    }
}

// ------------------------------------------------------------------------------------ owner

/// An operation's top: what calls it again until it is done, holding nothing of the pool
/// between calls. It keeps the operation's tenure across those calls, and its connection's
/// identity, if it has one (none for open, recovery, the cut, close and the checkpointer, which
/// no embedder suspends).
#[derive(Default)]
pub(crate) struct Owner {
    tenure: Option<Box<Tenure>>,
    conn: Option<Arc<Conn>>,
}

/// An operation's place in the shortage queue (DESIGN.md 9.1.3, *Tenure*): one demand per tenure.
/// A phase-B step that comes short again ends the tenure and queues anew (*Ending*). What
/// another party may take, `R` and the published set, is the place's, in the queue.
struct Tenure {
    pool: Arc<BufferPool>,
    place: Place,
    /// The demand phase A has yet to reserve: looked for at each call, the operation running
    /// again only once it is reserved.
    demand: Option<(SizeClass, Purpose)>,
    /// The pages the step held when it came short, published once phase A is done.
    held: Vec<u64>,
}

/// What a claim inside an operation that found no frame records for its top: the pool and its
/// demand.
struct Short {
    pool: Arc<BufferPool>,
    class: SizeClass,
    purpose: Purpose,
    /// The pages the step held when it came short, which it needs again.
    held: Vec<u64>,
}

crate::thread::thread_local! {
    /// The owner whose operation runs on this thread now, if any.
    static OWNER: Cell<*mut Owner> = const { Cell::new(std::ptr::null_mut()) };
    /// The demand a claim of the running operation came short of.
    static SHORT: RefCell<Option<Short>> = const { RefCell::new(None) };
    /// The pool whose change gate, closed by a cut, refused a change of the running operation.
    static GATE: RefCell<Option<Arc<BufferPool>>> = const { RefCell::new(None) };
}

/// Restores the owner installed before, however the call ends.
struct Installed(*mut Owner);

impl Drop for Installed {
    fn drop(&mut self) {
        OWNER.with(|o| o.set(self.0));
    }
}

impl Owner {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The top of an operation of connection `conn`.
    pub(crate) fn of(conn: Arc<Conn>) -> Self {
        Self {
            tenure: None,
            conn: Some(conn),
        }
    }

    /// Runs one call of the operation `op` as this owner's. A call that came short reserves the
    /// frame it needs here, holding nothing, and returns the wait that reservation needs, or a
    /// yield once the frame is reserved, so the caller calls again; a call that is done or failed
    /// ends the tenure.
    pub(crate) fn call<T>(
        &mut self,
        op: impl FnOnce() -> Result<IOResult<T>>,
    ) -> Result<IOResult<T>> {
        if let Some(t) = self.tenure.as_deref() {
            if !t.pool.shortage.enter(t.place) {
                // Revoked for a newcomer of its connection, or cancelled with its transaction:
                // what it held was let go there; the step starts afresh.
                self.tenure = None;
            }
        }
        let out = self.run(op);
        if let Some(t) = self.tenure.as_deref() {
            t.pool.shortage.exit(t.place);
        }
        out
    }

    fn run<T>(&mut self, op: impl FnOnce() -> Result<IOResult<T>>) -> Result<IOResult<T>> {
        if let Some(t) = self.tenure.as_deref_mut() {
            // The operation runs again only at its turn, with its demand's frame reserved and
            // its publication's barrier passed, however early it is called.
            if let Some(wait) = t.advance()? {
                return Ok(IOResult::IO(IOCompletions(wait)));
            }
        }
        let out = {
            // Read through only by `take_reserved` while `op` runs, which reaches nothing else of
            // this owner.
            let _installed = Installed(OWNER.with(|o| o.replace(self as *mut Owner)));
            op()
        };
        match out {
            Ok(IOResult::IO(c)) => {
                if let Some(pool) = GATE.with(|g| g.borrow_mut().take()) {
                    // Holding nothing now: the gate's reopening waited for, the cut stepped here
                    // if no one else steps it.
                    debug_assert!(SHORT.with(|s| s.borrow().is_none()));
                    return match pool.wait_for_gate()? {
                        IOResult::Done(()) => {
                            Ok(IOResult::IO(IOCompletions(Completion::new_yield())))
                        }
                        IOResult::IO(wait) => Ok(IOResult::IO(wait)),
                    };
                }
                match SHORT.with(|s| s.borrow_mut().take()) {
                    Some(short) => self.reserve(short),
                    None => Ok(IOResult::IO(c)),
                }
            }
            done => {
                debug_assert!(SHORT.with(|s| s.borrow().is_none()));
                debug_assert!(GATE.with(|g| g.borrow().is_none()));
                self.end();
                done
            }
        }
    }

    /// Takes the operation's place in the queue, records its demand, and admits it as far as it
    /// can now ([`Tenure::advance`]): the wait that stands, or a yield once the demand's frame is
    /// reserved, so the caller calls again. A newcomer of a connection whose other operation is
    /// suspended in the queue takes that one's place; behind its own running one it waits for
    /// that call to return.
    fn reserve<T>(&mut self, short: Short) -> Result<IOResult<T>> {
        crate::storage::atoll::park::holds::count_is_zero("a shortage reserved under a hold");
        if self.tenure.is_some() {
            // The operation runs only once its demand is reserved: a call that came short again
            // did so in phase B, at a memory wait. Everything goes, `R` first, then the set, then
            // the gate, and the operation queues again (DESIGN.md 9.1.3, *Ending*), its phase A
            // once more holding and protecting nothing.
            debug_assert!(
                self.tenure.as_ref().is_some_and(|t| t.demand.is_none()),
                "an operation ran before its demand was reserved"
            );
            self.end();
        }
        let queue = &short.pool.shortage;
        let turn = notify::remote_wait();
        let place = match self.conn.as_ref().filter(|c| c.queued()) {
            Some(conn) => match queue.revoke_for(conn, &turn) {
                Revoked::Took(place, released) => {
                    released.let_go(&short.pool);
                    place
                }
                Revoked::Running(signal) => return Ok(IOResult::IO(IOCompletions(signal))),
                Revoked::None => queue.join(turn, self.conn.clone()),
            },
            None => queue.join(turn, self.conn.clone()),
        };
        let t = self.tenure.insert(Box::new(Tenure {
            pool: short.pool.clone(),
            place,
            demand: Some((short.class, short.purpose)),
            held: short.held,
        }));
        let wait = t.advance()?.unwrap_or_else(Completion::new_yield);
        Ok(IOResult::IO(IOCompletions(wait)))
    }

    /// Ends the tenure, if any: the unused frame given back, the set retired, the place left, the
    /// gate passed on. Also where the operation it served is dropped unfinished (a cursor parked
    /// at its statement's reset). A tenure revoked or cancelled meanwhile was let go there.
    pub(crate) fn end(&mut self) {
        if let Some(t) = self.tenure.take() {
            if let Some(released) = t.pool.shortage.end(t.place) {
                released.let_go(&t.pool);
            }
        }
    }

    /// The class of the frame reserved for the operation, for a test.
    #[cfg(test)]
    pub(crate) fn reserved_class(&self) -> Option<usize> {
        let t = self.tenure.as_ref()?;
        let s = t.pool.shortage.slots.lock();
        if !Queue::live(&s, t.place) {
            return None;
        }
        s.nodes[t.place.slot].reserved.map(|(c, _, _)| c.size())
    }

    /// Whether the owner waits in the queue or holds the gate, for a test.
    #[cfg(test)]
    pub(crate) fn queued(&self) -> bool {
        self.tenure.is_some()
    }

    /// Runs one call of `op` on `this` as the operation of the owner `this` keeps at `slot`,
    /// [`call`](Self::call) as a top that keeps its owner among its own state does: the owner is
    /// moved out of `this` while `op` runs, which never reaches it, and back after.
    pub(crate) fn call_in<S, T>(
        this: &mut S,
        slot: fn(&mut S) -> &mut Owner,
        op: impl FnOnce(&mut S) -> Result<IOResult<T>>,
    ) -> Result<IOResult<T>> {
        let mut owner = std::mem::take(slot(this));
        let out = owner.call(|| op(this));
        let placeholder = std::mem::replace(slot(this), owner);
        debug_assert!(
            placeholder.tenure.is_none(),
            "an operation reached its own owner"
        );
        // It holds no tenure and no connection: nothing to end or let go.
        std::mem::forget(placeholder);
        out
    }
}

/// Ends every suspended tenure of `conn` in `pool`'s queue (DESIGN.md 9.1.3): a transaction's
/// end, which makes no demand of its own, so no sibling cursor keeps the gate, `R` or a protected
/// set past the transaction it served. One load when the connection holds no place.
pub(crate) fn cancel(pool: &BufferPool, conn: &Arc<Conn>) {
    if !conn.queued() {
        return;
    }
    for released in pool.shortage.cancel(conn) {
        released.let_go(pool);
    }
}

impl Tenure {
    /// Advances the tenure toward its operation running again: the wait that stands first, or
    /// none once the operation may run. Holding the gate, it reserves the demand's frame holding
    /// and protecting nothing of the pool (phase A), looking again at each call until it has it,
    /// and only then keeps the pages the step held resident (phase B): the demand's run may need
    /// the frame of a page the step held, which the step then reads in again.
    fn advance(&mut self) -> Result<Option<Completion>> {
        let queue = &self.pool.shortage;
        if !queue.holds_gate(self.place) {
            let s = queue.slots.lock();
            let turn = s.nodes[self.place.slot]
                .turn
                .clone()
                .expect("a live place's turn");
            return Ok(Some(turn));
        }
        if let Some((class, purpose)) = self.demand {
            // Phase A: `R` adopted if a free or a run's hand-over handed it, else the demand
            // named, so a frame of its class freed from here goes to this place; then an ordinary
            // claim, as by a caller holding nothing, with nothing protected, which finds a frame
            // freed before.
            let frame = match queue.want(self.place, class, purpose) {
                Some(frame) => frame,
                None => {
                    let claimed = {
                        let _installed = Installed(OWNER.with(|o| o.replace(std::ptr::null_mut())));
                        self.pool.claim(class, purpose, &[])
                    };
                    match claimed? {
                        IOResult::Done(found) => {
                            if let Some(extra) = queue.reserve(self.place, (class, purpose, found))
                            {
                                // Handed one meanwhile: the place keeps that.
                                self.pool.give_back_reserved(class, extra);
                            }
                            queue
                                .want(self.place, class, purpose)
                                .expect("the place's R, reserved")
                        }
                        IOResult::IO(IOCompletions(under)) => {
                            match queue.want(self.place, class, purpose) {
                                Some(frame) => frame,
                                // Its own wait: the frame handed to it wakes it, whatever the
                                // claim's wait was (the auditor's P2-S6-3).
                                None => return Ok(Some(queue.wait(self.place, under))),
                            }
                        }
                    }
                }
            };
            // A handed frame keeps its quiet stamp: a read goes into it only once every step that
            // could still read what it held has ended (DESIGN.md 9.1.3, phase A). One wait per
            // place, registered once, finished by the epochs or by the place's end.
            let at = self.pool.quiet_at(frame, class);
            let epochs = self.pool.store.epochs();
            if purpose == Purpose::Read && at > epochs.known_horizon() {
                if let Some(quiet) = queue.quiet(self.place) {
                    return Ok(Some(quiet));
                }
                let quiet = notify::remote_wait();
                if epochs.wait_for(at, quiet.clone()) {
                    queue.set_quiet(self.place, quiet.clone());
                    return Ok(Some(quiet));
                }
            }
            self.demand = None;
            // Phase B: the pages the step held kept resident from here, and relied on once every
            // scan that may have read the set before it is over: the epochs passed its
            // publication.
            let held = std::mem::take(&mut self.held);
            self.pool.protect(&held);
            // Nothing to keep, nothing to wait for: no scan can evict a page the step needs.
            let mut barrier = None;
            if !held.is_empty() {
                let epochs = self.pool.store.epochs();
                let b = notify::remote_wait();
                if epochs.wait_for(epochs.stamp(), b.clone()) {
                    barrier = Some(b);
                }
            }
            queue.published(self.place, barrier);
        }
        Ok(queue.barrier(self.place))
    }
}

/// Finishes a holder's own admission wait when the producer it is registered on finishes, with
/// that producer's result (DESIGN.md 9.1.3, phase A's grant): it schedules and returns.
pub(super) struct Proxy {
    pub(super) grant: Completion,
    pub(super) under: Completion,
}

impl std::task::Wake for Proxy {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        let result = match self.under.get_error() {
            Some(e) => Err(e),
            None => Ok(0),
        };
        notify::finish(&self.grant, result);
    }
}

/// What a place's met or replaced demand let go, after the queue's mutex: its admission wait
/// finished, so every caller that took it looks again, and the proxy withdrawn.
#[derive(Default)]
struct Done {
    grant: Option<Completion>,
    under: Option<(Completion, WakerKey)>,
}

impl Done {
    fn let_go(self) {
        if let Some((under, key)) = self.under {
            under.withdraw_key(key);
        }
        if let Some(grant) = self.grant {
            notify::finish(&grant, Ok(0));
        }
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.end();
    }
}

#[cfg(test)]
crate::thread::thread_local! {
    /// For a test: while set, every claim of an owner's operation not given a frame reserved for
    /// it comes short, as in a pool whose every frame another caller holds.
    static SCARCE: Cell<bool> = const { Cell::new(false) };
}

/// Runs `f` with memory scarce for the owners' operations it runs ([`SCARCE`]), for a test that
/// a shortage reaches an operation's top.
#[cfg(test)]
pub(crate) fn scarce<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            SCARCE.with(|s| s.set(self.0));
        }
    }
    let _restore = Restore(SCARCE.with(|s| s.replace(true)));
    f()
}

/// Whether a claim of the running owner's operation comes short whatever the pool holds.
#[cfg(test)]
pub(super) fn scarce_now() -> bool {
    owned() && SCARCE.with(Cell::get)
}

/// Runs `f` as no owner's operation: a claim inside it never comes short for, nor takes `R` of, the
/// operation running on this thread, which is restored however `f` ends. For maintenance a
/// foreground operation helps along (the collector's step after a begin): its waits are its own,
/// never a demand of the operation that ran it (DESIGN.md 9.1.3, *Maintenance keeps what it owns*).
pub(crate) fn unowned<T>(f: impl FnOnce() -> T) -> T {
    let _installed = Installed(OWNER.with(|o| o.replace(std::ptr::null_mut())));
    f()
}

/// Whether the claim running on this thread runs inside an owner's operation, which comes short
/// rather than waiting on memory.
pub(super) fn owned() -> bool {
    OWNER.with(|o| !o.get().is_null())
}

/// The frame reserved for this exact claim by the running operation's owner, taken.
pub(super) fn take_reserved(class: SizeClass, purpose: Purpose) -> Option<u32> {
    let owner = OWNER.with(Cell::get);
    if owner.is_null() {
        return None;
    }
    // SAFETY: the owner installed itself for the duration of its call, which is still running
    // and touches the owner only after the operation returns.
    let owner = unsafe { &*owner };
    let t = owner.tenure.as_ref()?;
    t.pool.shortage.take_reserved(t.place, class, purpose)
}

/// Records that the change gate of `pool`, closed by a cut, refused a change of the running
/// operation, and returns the yield the change hands back once it has let every latch and the
/// gate go: its top, holding nothing, waits for the gate's reopening and steps the cut if no one
/// else does ([`BufferPool::wait_for_gate`]). Outside any owner's operation the yield is all.
pub(crate) fn gate_closed<T>(pool: &Arc<BufferPool>) -> IOResult<T> {
    if owned() {
        GATE.with(|g| *g.borrow_mut() = Some(pool.clone()));
    }
    IOResult::IO(IOCompletions(Completion::new_yield()))
}

/// Records that the running operation came short of a frame of `class` for `purpose`, and
/// returns the yield its claim hands back: its top reserves the frame.
pub(super) fn short<T>(
    pool: &Arc<BufferPool>,
    class: SizeClass,
    purpose: Purpose,
    held: &[PageId],
) -> IOResult<T> {
    let held = held.iter().map(|p| p.raw()).collect();
    SHORT.with(|s| {
        *s.borrow_mut() = Some(Short {
            pool: pool.clone(),
            class,
            purpose,
            held,
        })
    });
    IOResult::IO(IOCompletions(Completion::new_yield()))
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod tests {
    use super::{notify, Conn, Queue};

    /// The auditor's P2-S6-3 (impl-s6ab-v1 review): the gate holder waits on its own admission
    /// wait, whatever wait its claim found (here a foreign one nothing finishes). The offer that
    /// hands it a frame finishes that wait and leaves the foreign one alone; however often it
    /// waits, it keeps one registration on the producer, withdrawn when its demand is met; its
    /// wait is of the producer's kind; and a producer that fails finishes it with that error.
    #[test]
    fn the_gate_holder_waits_on_its_own_wait_which_a_grant_or_its_producer_finishes() {
        use super::super::supply::Supply;
        use super::{Arc, Purpose, SizeClass};
        use crate::CompletionError;
        let epochs = Arc::new(crate::storage::atoll::epoch::Epochs::new());
        let supply = Arc::new(Supply::new(epochs));
        let q = Queue::new(supply.clone());
        let c = SizeClass::new(0).unwrap();
        let p = q.join(notify::remote_wait(), None);
        assert_eq!(q.want(p, c, Purpose::New), None);
        // A foreign wait, as the rare gate's: nothing here finishes it.
        let foreign = notify::remote_wait();
        let mut waits = Vec::new();
        for _ in 0..32 {
            waits.push(q.wait(p, foreign.clone()));
        }
        let registrations = foreign.wakers();
        let one_wait = waits.iter().all(|w| w.addr() == waits[0].addr());
        let own_pending = !waits[31].finished();
        let remote_kind = waits[31].is_remote_wait();
        let handed = supply.offer(c, 7);
        let finished_by_grant = waits[31].finished();
        let foreign_untouched = !foreign.finished();
        let adopted = q.want(p, c, Purpose::New);
        let registrations_after = foreign.wakers();
        let released = q.end(p).expect("a live place");
        released.pass_reserved();

        // Another producer: the wait it replaces is finished, so a caller that took it looks
        // again, and the new one stands.
        let p = q.join(notify::remote_wait(), None);
        assert_eq!(q.want(p, c, Purpose::New), None);
        let first = notify::remote_wait();
        let second = notify::remote_wait();
        let before = q.wait(p, first.clone());
        let after = q.wait(p, second.clone());
        let replaced_finished = before.finished() && !after.finished();
        let released = q.end(p).expect("a live place");
        released.pass_reserved();
        notify::finish(&first, Ok(0));
        notify::finish(&second, Ok(0));

        // A stepped producer that fails: the holder's wait is stepped too, and fails with it.
        let p = q.join(notify::remote_wait(), None);
        assert_eq!(q.want(p, c, Purpose::New), None);
        let stepped = notify::wait();
        let own = q.wait(p, stepped.clone());
        let stepped_kind = !own.is_remote_wait();
        notify::finish(&stepped, Err(CompletionError::Aborted));
        let failed_with_it = own.finished() && own.get_error() == Some(CompletionError::Aborted);
        let released = q.end(p).expect("a live place");
        released.pass_reserved();
        notify::finish(&foreign, Ok(0));

        assert_eq!(
            registrations, 1,
            "one registration on the producer, however often it waits"
        );
        assert!(one_wait, "a wait on the same producer was made again");
        assert!(
            replaced_finished,
            "a replaced wait was left unfinished, or the new one finished"
        );
        assert!(
            own_pending,
            "the holder's own wait finished before anything happened"
        );
        assert!(
            remote_kind,
            "the holder's wait is not of its remote producer's kind"
        );
        assert!(handed, "the demand did not stand");
        assert!(
            finished_by_grant,
            "the grant left the holder's own wait unfinished"
        );
        assert!(foreign_untouched, "the grant finished the foreign wait");
        assert_eq!(adopted, Some(7), "the handed frame was not adopted");
        assert_eq!(
            registrations_after, 0,
            "the proxy stayed on the producer once its demand was met"
        );
        assert!(
            stepped_kind,
            "the holder's wait is not of its stepped producer's kind"
        );
        assert!(
            failed_with_it,
            "the producer's failure did not reach the holder's wait"
        );
    }

    /// A place that ends between its wait's post and the proxy's registration, which runs after
    /// the queue's mutex, leaves no registration on the producer: the wait withdraws it itself.
    #[test]
    fn a_place_ended_before_its_proxy_registers_leaves_no_registration_on_the_producer() {
        use super::super::supply::Supply;
        use super::{Arc, Purpose, SizeClass};
        let epochs = Arc::new(crate::storage::atoll::epoch::Epochs::new());
        let supply = Arc::new(Supply::new(epochs));
        let q = Arc::new(Queue::new(supply.clone()));
        let c = SizeClass::new(0).unwrap();
        let p = q.join(notify::remote_wait(), None);
        assert_eq!(q.want(p, c, Purpose::New), None);
        let producer = notify::remote_wait();
        let ended = std::rc::Rc::new(std::cell::Cell::new(false));
        {
            let (q, ended) = (q.clone(), ended.clone());
            crate::race::at("shortage.proxy_register", move || {
                q.end(p).expect("a live place").pass();
                ended.set(true);
            });
        }
        let own = q.wait(p, producer.clone());
        let left = producer.wakers();
        notify::finish(&producer, Ok(0));
        assert!(ended.get(), "the place did not end at the registration");
        assert!(own.finished(), "the place's end left its wait unfinished");
        assert_eq!(
            left, 0,
            "a registration stayed on the producer of an ended place"
        );
    }

    /// The auditor's P2-S6-1 (impl-s6a-v1 review): a frame handed between the holder's look and
    /// its naming the demand again is adopted by that naming, which leaves no demand standing, so
    /// a later free is not handed to a demand already met (DESIGN.md 9.1.3, phase A).
    #[test]
    fn a_frame_handed_before_the_demand_is_named_again_is_adopted_and_no_demand_is_left() {
        use super::super::supply::Supply;
        use super::{Arc, Purpose, SizeClass};
        let epochs = Arc::new(crate::storage::atoll::epoch::Epochs::new());
        let supply = Arc::new(Supply::new(epochs));
        let handoff = supply.handoff.clone();
        let q = Queue::new(supply.clone());
        let c = SizeClass::new(0).unwrap();
        let turn = notify::remote_wait();
        let p = q.join(turn, None);
        assert_eq!(q.want(p, c, Purpose::New), None, "no frame to adopt yet");
        // A free hands R1 while the holder looks.
        assert!(handoff.offer(c, 1), "the demand stood");
        let adopted = q.want(p, c, Purpose::New);
        let again = q.want(p, c, Purpose::New);
        // A further free finds no demand: it goes to the free lists.
        let stale = handoff.offer(c, 2);
        let released = q.end(p).expect("a live place");
        let back = released.reserved;
        assert_eq!(adopted, Some(1), "the handed frame was not adopted");
        assert_eq!(again, Some(1), "R changed once adopted");
        assert!(!stale, "a frame was handed to a demand already met");
        assert_eq!(
            back.map(|r| r.2),
            Some(1),
            "R did not go back with the place"
        );
        for c in released.completions.iter().chain(&released.next_turn) {
            notify::finish(c, Ok(0));
        }
    }

    /// The queue is first in first out: a place's turn is finished when it reaches the head, the
    /// gate passes to the next place when the head leaves, and a place left from the middle
    /// passes nothing.
    #[test]
    fn the_shortage_queue_hands_the_gate_on_in_order() {
        let q = Queue::default();
        let (a, b, c) = (
            notify::remote_wait(),
            notify::remote_wait(),
            notify::remote_wait(),
        );
        let pa = q.join(a.clone(), None);
        let pb = q.join(b.clone(), None);
        let pc = q.join(c.clone(), None);
        assert!(a.finished() && !b.finished() && !c.finished());
        assert!(q.holds_gate(pa) && !q.holds_gate(pb));
        // B leaves from the middle: its own turn is finished, so a waiter on it wakes and finds the
        // place gone; the gate does not pass, C stays behind A.
        q.end(pb).expect("a live place").pass();
        assert!(b.finished() && !c.finished() && q.holds_gate(pa));
        q.end(pa).expect("a live place").pass();
        assert!(c.finished() && q.holds_gate(pc));
        q.end(pc).expect("a live place").pass();
        assert_eq!(q.len(), 0);
        // A freed slot is reused under a new generation: the old place names nothing, and ends
        // nothing.
        let d = notify::remote_wait();
        let pd = q.join(d.clone(), None);
        assert!(d.finished() && q.holds_gate(pd) && !q.holds_gate(pa));
        assert!(q.end(pa).is_none() && q.holds_gate(pd));
        q.end(pd).expect("a live place").pass();
    }

    /// A newcomer of a connection takes the place of its connection's suspended operation, the
    /// gate with it, and wakes that one; behind its connection's running operation it waits on
    /// that call's signal; a transaction's end cancels the connection's suspended places and
    /// leaves its running one (DESIGN.md 9.1.3, *Revoking an inactive tenure*).
    #[test]
    fn a_connections_newcomer_takes_its_suspended_place_and_waits_on_its_running_one() {
        let q = Queue::default();
        let (c, d) = (Conn::new(), Conn::new());
        let old = notify::remote_wait();
        let held = q.join(old.clone(), Some(c.clone()));
        let other = q.join(notify::remote_wait(), Some(d.clone()));
        assert!(c.queued() && d.queued());
        // Running: a newcomer of its connection waits for its call to return.
        let new = notify::remote_wait();
        let signal = match q.revoke_for(&c, &new) {
            super::Revoked::Running(signal) => signal,
            _ => panic!("the running place was not waited on"),
        };
        assert!(!signal.finished());
        q.exit(held);
        assert!(signal.finished(), "the call's return finishes its signal");
        // Suspended: the newcomer takes its place and the gate; the old owner is woken and finds
        // its place gone.
        let (place, released) = match q.revoke_for(&c, &new) {
            super::Revoked::Took(place, released) => (place, released),
            _ => panic!("the suspended place was not taken"),
        };
        released.pass();
        assert!(new.finished() && q.holds_gate(place));
        assert!(
            !q.enter(held),
            "the old owner entered the place taken from it"
        );
        assert!(
            matches!(
                q.revoke_for(&d, &notify::remote_wait()),
                super::Revoked::Running(_)
            ),
            "the other connection's place is its own"
        );
        // A transaction's end: the suspended place of its connection goes, the running one stays.
        q.exit(place);
        assert_eq!(q.cancel(&c).len(), 1);
        assert!(!c.queued() && q.len() == 1);
        assert!(
            q.cancel(&d).is_empty(),
            "a running place is its caller's own"
        );
        q.exit(other);
        q.cancel(&d).into_iter().for_each(super::Released::pass);
        assert!(!d.queued() && q.len() == 0);
    }

    #[test]
    fn audit_call_in_releases_its_temporary_connection_reference() {
        use crate::sync::Arc;
        use crate::types::IOResult;
        struct Operation {
            owner: super::Owner,
        }
        let conn = super::Conn::new();
        let mut op = Operation {
            owner: super::Owner::of(conn.clone()),
        };
        let before = Arc::strong_count(&conn);
        for _ in 0..64 {
            let done = super::Owner::call_in(&mut op, |o| &mut o.owner, |_| Ok(IOResult::Done(())));
            assert!(matches!(done, Ok(IOResult::Done(()))));
        }
        let after = Arc::strong_count(&conn);
        drop(op);
        let after_drop = Arc::strong_count(&conn);
        drop(conn);
        println!("connection references before={before}, after_64_calls={after}, after_owner_drop={after_drop}");
        assert_eq!(
            after, before,
            "call_in leaked a connection reference on every operation"
        );
        assert_eq!(
            after_drop, 1,
            "the operation kept its connection alive after being dropped"
        );
    }

    #[test]
    fn audit_cancel_finishes_the_cancelled_owners_admission_turn() {
        let q = super::Queue::default();
        let head_conn = super::Conn::new();
        let tail_conn = super::Conn::new();
        let head = q.join(super::notify::remote_wait(), Some(head_conn));
        let turn = super::notify::remote_wait();
        let tail = q.join(turn.clone(), Some(tail_conn.clone()));
        q.exit(tail);
        for released in q.cancel(&tail_conn) {
            released.pass();
        }
        let woke = turn.finished();
        let empty_tail = !tail_conn.queued();
        q.end(head).unwrap().pass();
        drop(q);
        drop(tail_conn);
        println!("cancelled admission turn_finished={woke}, removed={empty_tail}");
        assert!(
            woke,
            "cancel removed a suspended place without finishing its admission turn"
        );
    }

    #[test]
    fn audit_the_next_gate_holder_cannot_enter_before_old_resources_are_released() {
        let q = super::Queue::default();
        let head = q.join(super::notify::remote_wait(), None);
        let turn = super::notify::remote_wait();
        let tail = q.join(turn.clone(), None);
        q.exit(tail);
        let releasing = q.end(head).unwrap();
        let gate_available_before_release = q.holds_gate(tail);
        let notified_before_release = turn.finished();
        releasing.pass();
        q.end(tail).unwrap().pass();
        drop(q);
        println!("new_head_available_before_release={gate_available_before_release}, notified={notified_before_release}");
        assert!(
            !gate_available_before_release,
            "the next tenure can enter while the old tenure still owns R and its protected set"
        );
    }

    #[test]
    fn audit_the_old_head_cannot_clear_the_next_heads_published_pages() {
        use crate::io::{MemoryIO, IO};
        use crate::storage::atoll::buffer::source::LocalSource;
        use crate::storage::atoll::buffer::{BufferConfig, BufferPool, MIN_BUDGET};
        use crate::storage::atoll::page::{SizeClass, CLASS_COUNT};
        use crate::storage::atoll::store::{PageStore, StoreConfig};
        use crate::sync::Arc;
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let store = PageStore::open(
            io.clone(),
            "/db/audit-handoff",
            StoreConfig {
                file_bytes: 1 << 30,
                meta_pages: 1 << 10,
            },
            [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
            0,
        );
        let pool = BufferPool::new(
            BufferConfig::with_budget(MIN_BUDGET),
            Arc::new(LocalSource::new(store.clone())),
            store,
        )
        .unwrap();
        let class = SizeClass::new(0).unwrap();
        let page = crate::storage::atoll::testing::drive(&io, || pool.allocate(class)).unwrap();
        let (pid, frame) = (page.pid(), page.hint() - 1);
        drop(page);
        let q = &pool.shortage;
        let head = q.join(super::notify::remote_wait(), None);
        q.published(head, None);
        pool.protect(&[pid.raw()]);
        let tail = q.join(super::notify::remote_wait(), None);
        q.exit(tail);
        let releasing = q.end(head).unwrap();
        let mut next = super::Tenure {
            pool: pool.clone(),
            place: tail,
            demand: Some((class, super::Purpose::New)),
            held: vec![pid.raw()],
        };
        // Before the old head lets go, the next is not admitted: it waits for its turn and
        // publishes nothing the old head's release could clear.
        let held_back = matches!(next.advance(), Ok(Some(_)));
        releasing.let_go(&pool);
        // Its turn finished after the old `R` and set are let go: admitted now, it publishes.
        let admitted = next.advance().is_ok();
        let protected_after_old_release = pool.protected(frame);
        if let Some(released) = q.end(tail) {
            released.let_go(&pool);
        }
        drop(next);
        drop(pool);
        drop(io);
        println!(
            "held_back={held_back} admitted={admitted} new_set_after_old_release={protected_after_old_release}"
        );
        assert!(
            held_back,
            "the next head was admitted while the old head still held its R and its set"
        );
        assert!(
            admitted && protected_after_old_release,
            "the old head cleared the new heads protected set"
        );
    }
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod admission_audit_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc as StdArc,
    };
    use std::task::{Wake, Waker};

    #[test]
    fn every_returned_admission_wait_finishes_when_its_producer_and_place_end() {
        let epochs = Arc::new(crate::storage::atoll::epoch::Epochs::new());
        let supply = Arc::new(Supply::new(epochs));
        let q = Queue::new(supply.clone());
        let c = SizeClass::new(0).unwrap();
        let p = q.join(notify::remote_wait(), None);
        let named = q.want(p, c, Purpose::New).is_none();
        let foreign = notify::remote_wait();
        let calls: Vec<_> = (0..32)
            .map(|_| StdArc::new(Calls(AtomicUsize::new(0))))
            .collect();
        let mut waits = Vec::new();
        for call in &calls {
            let waker = Waker::from(call.clone());
            let wait = q.wait(p, foreign.clone());
            wait.set_waker(&waker);
            waits.push(wait);
        }
        let distinct = waits
            .iter()
            .filter_map(Completion::addr)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let registered = foreign.wakers();
        notify::finish(&foreign, Ok(0));
        let after_producer = waits.iter().filter(|w| w.finished()).count();
        q.end(p).expect("a live place").pass();
        drop((q, supply, foreign));
        let after_end = waits.iter().filter(|w| w.finished()).count();
        let wakes: Vec<_> = calls.iter().map(|c| c.0.load(Ordering::Relaxed)).collect();
        drop(waits);
        eprintln!("ADMISSION_REPLACEMENT named={named} distinct={distinct} registered={registered} finished_after_producer={after_producer} finished_after_end={after_end} wakes={wakes:?}");
        assert!(named && registered == 1);
        assert_eq!(
            after_producer, 32,
            "older returned admissions lost their only producer"
        );
        assert_eq!(
            after_end, 32,
            "a completed operation left its returned wait pending"
        );
    }

    #[test]
    fn a_frame_handed_before_the_wait_is_posted_is_not_missed() {
        let epochs = Arc::new(crate::storage::atoll::epoch::Epochs::new());
        let supply = Arc::new(Supply::new(epochs));
        let q = Queue::new(supply.clone());
        let c = SizeClass::new(0).unwrap();
        let p = q.join(notify::remote_wait(), None);
        // The production advance's second want still saw no grant; a free then runs whole
        // before Queue::wait acquires the mutex and posts its completion.
        let empty = q.want(p, c, Purpose::New).is_none();
        let handed = supply.offer(c, 7);
        let foreign = notify::remote_wait();
        let own = q.wait(p, foreign.clone());
        let pending_after_post = !own.finished();
        let foreign_pending = !foreign.finished();
        let released = q.end(p).expect("a live place");
        let returned = released.reserved.map(|r| r.2);
        released.pass_reserved();
        notify::finish(&foreign, Ok(0));
        drop((q, supply, own, foreign));
        eprintln!("ADMISSION_POST_GAP empty={empty} handed={handed} pending_after_post={pending_after_post} foreign_pending={foreign_pending} returned={returned:?}");
        assert!(empty && handed && foreign_pending && returned == Some(7));
        assert!(
            !pending_after_post,
            "a grant before registration left the admission asleep"
        );
    }

    struct Calls(AtomicUsize);
    impl Wake for Calls {
        fn wake(self: StdArc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &StdArc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod admission_drop_audit_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc as StdArc,
    };
    use std::task::{Wake, Waker};
    struct ChecksDrop {
        queue: StdArc<Queue>,
        held: StdArc<AtomicUsize>,
    }
    impl Wake for ChecksDrop {
        fn wake(self: StdArc<Self>) {}
    }
    impl Drop for ChecksDrop {
        fn drop(&mut self) {
            if self.queue.slots.try_lock().is_none() {
                self.held.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    #[test]
    fn replacing_an_admission_does_not_drop_an_observers_waker_under_the_queue_mutex() {
        let epochs = Arc::new(crate::storage::atoll::epoch::Epochs::new());
        let supply = Arc::new(Supply::new(epochs));
        let q = StdArc::new(Queue::new(supply.clone()));
        let c = SizeClass::new(0).unwrap();
        let p = q.join(notify::remote_wait(), None);
        let named = q.want(p, c, Purpose::New).is_none();
        let foreign = notify::remote_wait();
        let held = StdArc::new(AtomicUsize::new(0));
        let first = q.wait(p, foreign.clone());
        let waker = Waker::from(StdArc::new(ChecksDrop {
            queue: q.clone(),
            held: held.clone(),
        }));
        first.set_waker(&waker);
        drop((first, waker));
        let second = q.wait(p, foreign.clone());
        let drops_under_lock = held.load(Ordering::Relaxed);
        q.end(p).expect("a live place").pass();
        notify::finish(&foreign, Ok(0));
        drop((second, foreign, q, supply));
        eprintln!("ADMISSION_WAKER_DROP named={named} drops_under_queue_lock={drops_under_lock}");
        assert!(named);
        assert_eq!(
            drops_under_lock, 0,
            "a scheduler's waker destructor ran under the shortage queue mutex"
        );
    }

    /// The exits that do let an admission wait go, a replacing producer, a grant then adoption,
    /// and the place's end, each run the caller's waker destructor after the queue's mutex, never
    /// under it.
    #[test]
    fn no_exit_drops_an_observers_waker_under_the_queue_mutex() {
        let epochs = Arc::new(crate::storage::atoll::epoch::Epochs::new());
        let supply = Arc::new(Supply::new(epochs));
        let q = StdArc::new(Queue::new(supply.clone()));
        let c = SizeClass::new(0).unwrap();
        let held = StdArc::new(AtomicUsize::new(0));
        let observe = |wait: Completion| {
            let waker = Waker::from(StdArc::new(ChecksDrop {
                queue: q.clone(),
                held: held.clone(),
            }));
            wait.set_waker(&waker);
        };
        // A replacing producer.
        let p = q.join(notify::remote_wait(), None);
        assert!(q.want(p, c, Purpose::New).is_none());
        let (one, two) = (notify::remote_wait(), notify::remote_wait());
        observe(q.wait(p, one.clone()));
        let second = q.wait(p, two.clone());
        q.end(p).expect("a live place").pass();
        drop(second);
        // A grant, then adoption.
        let p = q.join(notify::remote_wait(), None);
        assert!(q.want(p, c, Purpose::New).is_none());
        observe(q.wait(p, one.clone()));
        assert!(supply.offer(c, 3));
        assert_eq!(q.want(p, c, Purpose::New), Some(3));
        q.end(p).expect("a live place").pass_reserved();
        // The place's end with its wait standing.
        let p = q.join(notify::remote_wait(), None);
        assert!(q.want(p, c, Purpose::New).is_none());
        observe(q.wait(p, two.clone()));
        q.end(p).expect("a live place").pass();
        notify::finish(&one, Ok(0));
        notify::finish(&two, Ok(0));
        let drops_under_lock = held.load(Ordering::Relaxed);
        drop((one, two, q, supply));
        assert_eq!(
            drops_under_lock, 0,
            "a scheduler's waker destructor ran under the shortage queue mutex"
        );
    }
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod admission_register_audit_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc as StdArc,
    };
    use std::task::{Wake, Waker};
    struct Looks {
        q: StdArc<Queue>,
        calls: StdArc<AtomicUsize>,
        held: StdArc<AtomicUsize>,
    }
    impl Wake for Looks {
        fn wake(self: StdArc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &StdArc<Self>) {
            self.calls.fetch_add(1, Ordering::Relaxed);
        }
    }
    impl Drop for Looks {
        fn drop(&mut self) {
            if self.q.slots.try_lock().is_none() {
                self.held.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    #[test]
    fn registering_a_proxy_cannot_drop_other_producer_waiters_under_the_queue_mutex() {
        let mut results = Vec::new();
        for engine in [false, true] {
            let epochs = Arc::new(crate::storage::atoll::epoch::Epochs::new());
            let supply = Arc::new(Supply::new(epochs));
            let q = StdArc::new(Queue::new(supply.clone()));
            let class = SizeClass::new(0).unwrap();
            let p = q.join(notify::remote_wait(), None);
            let named = q.want(p, class, Purpose::New).is_none();
            let foreign = if engine {
                notify::remote_wait()
            } else {
                Completion::new_remote_wait()
            };
            let calls = StdArc::new(AtomicUsize::new(0));
            let held = StdArc::new(AtomicUsize::new(0));
            let waker = Waker::from(StdArc::new(Looks {
                q: q.clone(),
                calls: calls.clone(),
                held: held.clone(),
            }));
            foreign.set_waker(&waker);
            drop(waker);
            // The real Completion result is published, but its finisher has not taken its
            // existing waiter list yet. The admission's registration on that source runs now.
            let (queue, under) = (q.clone(), foreign.clone());
            crate::race::at("completion.published", move || {
                let _own = queue.wait(p, under);
            });
            if engine {
                notify::finish(&foreign, Ok(0));
            } else {
                foreign.complete(0);
            }
            let reached = !crate::race::pending("completion.published");
            let observation = (
                engine,
                named,
                reached,
                calls.load(Ordering::Relaxed),
                held.load(Ordering::Relaxed),
            );
            q.end(p).expect("live").pass();
            drop((foreign, q, supply));
            results.push(observation);
        }
        eprintln!("ADMISSION_REGISTER_DROP cases={results:?}");
        for (engine, named, reached, calls, held) in results {
            assert!(named && reached);
            assert_eq!(calls, 1, "engine={engine}: reach the actual foreign waker");
            assert_eq!(held,0,"engine={engine}: foreign source registration dropped an unrelated observer under slots");
        }
    }
}
