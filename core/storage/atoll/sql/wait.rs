//! The wait graph (DESIGN.md §17, R2): one per process, over transaction groups, so a cycle of
//! waits across attached databases is a cycle in it.
//!
//! - **Groups.** A group is a connection's transactions across its attached Atoll databases. Its id
//!   is its node's index and a generation bumped at every begin and end, so an edge never names a
//!   later group; its age is drawn from one counter for the process, since each database has a
//!   clock of its own.
//! - **Records.** Each waiting statement publishes a record: its edge, a state (waiting, flagged,
//!   over) and the completion it parks on. Records sit under their group's mutex. A publisher
//!   always walks after publishing, so of two waiters closing one cycle at least one sees it: had
//!   each missed the other's record, each one's walk would come before the other's publish, which
//!   comes before that one's walk.
//! - **Victims.** On a cycle through it, the walker checks every hop again, then flags the member
//!   with the largest age holding no gate exclusive, else the largest: a compare-and-swap of that
//!   member's record on the cycle from waiting to flagged, so a record whose wait ended is never
//!   flagged. The flagger takes the record's parked completion under its mutex and completes it;
//!   a waiter puts its completion there under the same mutex before it reads the state, so no
//!   flag is lost.

use self::sync::{lock, AtomicBool, AtomicU32, AtomicU64, AtomicU8, Mutex, Ordering};
use crate::io::Completion;
use crate::storage::atoll::txn::{Blocker, Transactions};
use crate::sync::Arc;
use std::collections::HashSet;

/// The engine's primitives, or loom's.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::storage::atoll::locks::Mutex;
    pub(super) use crate::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};

    pub(super) fn lock<T>(m: &Mutex<T>) -> impl std::ops::DerefMut<Target = T> + '_ {
        m.lock()
    }
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
    pub(super) use loom::sync::Mutex;

    pub(super) fn lock<T>(m: &Mutex<T>) -> impl std::ops::DerefMut<Target = T> + '_ {
        m.lock().unwrap()
    }
}

/// A transaction group: its node's index in the high 32 bits, the node's generation in the low.
pub type GroupId = u64;

/// Where a waiting transaction's holders are named: a database's transactions and write gate.
pub trait Holders: Send + Sync {
    /// The group of `blocker`'s transaction while it runs.
    fn holder(&self, blocker: Blocker) -> Option<GroupId>;

    /// The group owning the gate's exclusive take numbered `take`, while it is in the gate.
    fn take_group(&self, _take: u64) -> Option<GroupId> {
        None
    }

    /// The group of `blocker`'s transaction while it holds the gate shared.
    fn shared_group(&self, _blocker: Blocker) -> Option<GroupId> {
        None
    }

    /// Clears the gate's pending take numbered `take`, for a victim that is not stepping.
    fn clear_take(&self, _take: u64) {}
}

impl Holders for Transactions {
    fn holder(&self, blocker: Blocker) -> Option<GroupId> {
        Transactions::holder(self, blocker).filter(|&g| g != 0)
    }

    fn take_group(&self, take: u64) -> Option<GroupId> {
        Holders::holder(self, Transactions::take_holder(self, take)?)
    }

    fn shared_group(&self, blocker: Blocker) -> Option<GroupId> {
        Transactions::holds_shared(self, blocker)
            .then(|| Holders::holder(self, blocker))
            .flatten()
    }

    fn clear_take(&self, take: u64) {
        Transactions::clear_take(self, take)
    }
}

impl Holders for super::AtollDb {
    fn holder(&self, blocker: Blocker) -> Option<GroupId> {
        Holders::holder(self.atoll().transactions(), blocker)
    }

    fn take_group(&self, take: u64) -> Option<GroupId> {
        self.atoll().transactions().take_group(take)
    }

    fn shared_group(&self, blocker: Blocker) -> Option<GroupId> {
        self.atoll().transactions().shared_group(blocker)
    }

    fn clear_take(&self, take: u64) {
        Holders::clear_take(self.atoll().transactions(), take)
    }
}

/// What a waiting statement waits on, in the database whose transactions and gate `db` names.
#[derive(Clone)]
pub enum Edge {
    /// The end of `blocker`'s transaction.
    Row {
        db: Arc<dyn Holders>,
        blocker: Blocker,
    },
    /// The end of the gate's exclusive take numbered `take`: a shared taker, or a later exclusive
    /// taker, behind it.
    Take { db: Arc<dyn Holders>, take: u64 },
    /// The release of each of `holders`' shared holds: the drain of the pending take `take`, the
    /// waiter's own, which a flagger clears if the waiter is the victim.
    Drain {
        db: Arc<dyn Holders>,
        holders: Vec<Blocker>,
        take: u64,
    },
}

impl Edge {
    fn targets(&self) -> Vec<GroupId> {
        match self {
            Edge::Row { db, blocker } => db.holder(*blocker).into_iter().collect(),
            Edge::Take { db, take } => db.take_group(*take).into_iter().collect(),
            Edge::Drain { db, holders, .. } => {
                holders.iter().filter_map(|b| db.shared_group(*b)).collect()
            }
        }
    }
}

const WAITING: u8 = 0;
const FLAGGED: u8 = 1;
const OVER: u8 = 2;

/// A waiting statement's wait.
pub struct WaitRecord {
    group: GroupId,
    edge: Edge,
    state: AtomicU8,
    parked: Mutex<Option<Completion>>,
}

impl WaitRecord {
    pub fn flagged(&self) -> bool {
        self.state.load(Ordering::Acquire) == FLAGGED
    }

    /// Whether its waiter has parked on it, for a test to order a wake after the park.
    #[cfg(test)]
    pub fn parked(&self) -> bool {
        lock(&self.parked).is_some()
    }
}

struct Node {
    generation: AtomicU32,
    born: AtomicU64,
    /// Gates the group holds exclusive: a member holding one is a victim only if every member does.
    exclusive: AtomicU32,
    doomed: AtomicBool,
    waits: Mutex<Vec<Arc<WaitRecord>>>,
}

/// A group was chosen to break a cycle of waits: its transactions roll back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadlock;

/// The wait graph of a process.
pub struct WaitGraph {
    nodes: Mutex<Vec<Arc<Node>>>,
    free: Mutex<Vec<u32>>,
    ages: AtomicU64,
}

impl Default for WaitGraph {
    fn default() -> Self {
        Self {
            nodes: Mutex::new(Vec::new()),
            free: Mutex::new(Vec::new()),
            ages: AtomicU64::new(0),
        }
    }
}

/// The process's wait graph, which every Atoll connection's waits publish in.
pub fn graph() -> &'static WaitGraph {
    static GRAPH: std::sync::OnceLock<WaitGraph> = std::sync::OnceLock::new();
    GRAPH.get_or_init(WaitGraph::default)
}

/// A connection's place in the graph, from its open to its close, holding its node so that its
/// groups begin and end without the graph's lock.
pub struct Seat {
    index: u32,
    node: Arc<Node>,
}

fn index_of(id: GroupId) -> u32 {
    (id >> 32) as u32
}

fn generation_of(id: GroupId) -> u32 {
    id as u32
}

impl WaitGraph {
    /// A node for a new connection.
    pub fn seat(&self) -> Seat {
        if let Some(index) = lock(&self.free).pop() {
            return Seat {
                index,
                node: self.node(index),
            };
        }
        let mut nodes = lock(&self.nodes);
        let node = Arc::new(Node {
            generation: AtomicU32::new(0),
            born: AtomicU64::new(0),
            exclusive: AtomicU32::new(0),
            doomed: AtomicBool::new(false),
            waits: Mutex::new(Vec::new()),
        });
        nodes.push(node.clone());
        Seat {
            index: (nodes.len() - 1) as u32,
            node,
        }
    }

    /// Gives a closed connection's node back.
    pub fn unseat(&self, seat: Seat) {
        lock(&self.free).push(seat.index);
    }

    fn node(&self, index: u32) -> Arc<Node> {
        lock(&self.nodes)[index as usize].clone()
    }

    /// The node of `id`, if its group is the one running there.
    fn live(&self, id: GroupId) -> Option<Arc<Node>> {
        let node = lock(&self.nodes).get(index_of(id) as usize)?.clone();
        (node.generation.load(Ordering::Acquire) == generation_of(id)).then_some(node)
    }

    /// Begins the seat's next group, younger than every group begun before.
    pub fn begin(&self, seat: &Seat) -> GroupId {
        let node = &seat.node;
        node.born.store(
            self.ages.fetch_add(1, Ordering::AcqRel) + 1,
            Ordering::Release,
        );
        node.doomed.store(false, Ordering::Release);
        node.exclusive.store(0, Ordering::Release);
        let generation = node.generation.fetch_add(1, Ordering::AcqRel) + 1;
        (seat.index as u64) << 32 | generation as u64
    }

    /// Ends group `id`: its records are over, and every statement of it still parked is woken,
    /// to fail.
    pub fn end(&self, id: GroupId) {
        if let Some(node) = self.live(id) {
            Self::end_on(&node, id);
        }
    }

    fn end_on(node: &Node, id: GroupId) {
        if node
            .generation
            .compare_exchange(
                generation_of(id),
                generation_of(id).wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let records = std::mem::take(&mut *lock(&node.waits));
        for r in records {
            r.state
                .compare_exchange(WAITING, OVER, Ordering::AcqRel, Ordering::Acquire)
                .ok();
            if let Some(c) = lock(&r.parked).take() {
                c.complete(0);
            }
        }
    }

    /// Whether group `id` was chosen as a victim: it rolls back at its next step.
    #[cfg(test)]
    pub fn doomed(&self, id: GroupId) -> bool {
        self.live(id)
            .is_some_and(|n| n.doomed.load(Ordering::Acquire))
    }

    /// Notes that group `id` took (`held`) or gave up a gate exclusive.
    #[cfg(test)]
    pub fn exclusive(&self, id: GroupId, held: bool) {
        if let Some(node) = self.live(id) {
            if held {
                node.exclusive.fetch_add(1, Ordering::AcqRel);
            } else {
                node.exclusive.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    /// Publishes a wait of group `id` on `edge`, then looks for a cycle through it. `Deadlock` if
    /// it closes one whose victim is `id`, which then waits no more.
    pub fn publish(&self, id: GroupId, edge: Edge) -> Result<Arc<WaitRecord>, Deadlock> {
        let node = self.live(id).ok_or(Deadlock)?;
        if node.doomed.load(Ordering::Acquire) {
            return Err(Deadlock);
        }
        let record = Arc::new(WaitRecord {
            group: id,
            edge,
            state: AtomicU8::new(WAITING),
            parked: Mutex::new(None),
        });
        lock(&node.waits).push(record.clone());
        if let Some(cycle) = self.cycle(&record) {
            self.break_cycle(&cycle);
        }
        if record.flagged() {
            self.withdraw(&record).ok();
            return Err(Deadlock);
        }
        Ok(record)
    }

    /// Parks `record`'s statement on `c`: `Deadlock` if it was flagged, when it must not park.
    pub fn park(&self, record: &WaitRecord, c: &Completion) -> Result<(), Deadlock> {
        *lock(&record.parked) = Some(c.clone());
        if record.flagged() {
            lock(&record.parked).take();
            return Err(Deadlock);
        }
        Ok(())
    }

    /// Ends `record`'s wait: `Deadlock` if it was flagged first. A record its group's end
    /// closed ends quietly.
    pub fn withdraw(&self, record: &Arc<WaitRecord>) -> Result<(), Deadlock> {
        let state =
            match record
                .state
                .compare_exchange(WAITING, OVER, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => OVER,
                Err(now) => now,
            };
        lock(&record.parked).take();
        if let Some(node) = self.live(record.group) {
            lock(&node.waits).retain(|r| !Arc::ptr_eq(r, record));
        }
        if state == FLAGGED {
            Err(Deadlock)
        } else {
            Ok(())
        }
    }

    /// The waiting records of group `id`, if it is live.
    fn waits_of(&self, id: GroupId) -> Vec<Arc<WaitRecord>> {
        match self.live(id) {
            Some(node) => lock(&node.waits)
                .iter()
                .filter(|r| r.state.load(Ordering::Acquire) == WAITING)
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    /// A cycle of waiting records through `from`, if there is one: each record waits on the group
    /// of the next, the last on `from`'s.
    fn cycle(&self, from: &Arc<WaitRecord>) -> Option<Vec<Arc<WaitRecord>>> {
        let mut seen = HashSet::new();
        let mut path = vec![from.clone()];
        self.search(from.group, &mut path, &mut seen)
    }

    fn search(
        &self,
        home: GroupId,
        path: &mut Vec<Arc<WaitRecord>>,
        seen: &mut HashSet<GroupId>,
    ) -> Option<Vec<Arc<WaitRecord>>> {
        let last = path.last().expect("a record").clone();
        for target in last.edge.targets() {
            if target == home {
                return Some(path.clone());
            }
            if !seen.insert(target) {
                continue;
            }
            for next in self.waits_of(target) {
                path.push(next);
                if let Some(cycle) = self.search(home, path, seen) {
                    return Some(cycle);
                }
                path.pop();
            }
        }
        None
    }

    /// Checks every hop of `cycle` again, then flags its victim and wakes it.
    fn break_cycle(&self, cycle: &[Arc<WaitRecord>]) {
        let n = cycle.len();
        let holds = (0..n).all(|i| {
            cycle[i].state.load(Ordering::Acquire) == WAITING
                && cycle[i].edge.targets().contains(&cycle[(i + 1) % n].group)
        });
        if !holds {
            return;
        }
        let age = |r: &Arc<WaitRecord>| -> Option<(u64, bool)> {
            let node = self.live(r.group)?;
            Some((
                node.born.load(Ordering::Acquire),
                node.exclusive.load(Ordering::Acquire) == 0,
            ))
        };
        let members: Vec<_> = cycle.iter().filter_map(|r| Some((r, age(r)?))).collect();
        let victim = members
            .iter()
            .filter(|(_, (_, free))| *free)
            .max_by_key(|(_, (born, _))| *born)
            .or_else(|| members.iter().max_by_key(|(_, (born, _))| *born));
        let Some((record, _)) = victim else {
            return;
        };
        if record
            .state
            .compare_exchange(WAITING, FLAGGED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Its wait ended: the cycle is gone.
            return;
        }
        if let Some(node) = self.live(record.group) {
            node.doomed.store(true, Ordering::Release);
        }
        // A pending take of the victim's blocks the others' shared takes: clear it.
        if let Edge::Drain { db, take, .. } = &record.edge {
            db.clear_take(*take);
        }
        if let Some(c) = lock(&record.parked).take() {
            c.complete(0);
        }
    }
}

#[cfg(test)]
mod tests;
