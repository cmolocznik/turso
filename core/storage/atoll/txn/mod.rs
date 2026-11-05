//! Transactions and visibility (DESIGN.md §11; OSIC, Alhomssi and Leis, VLDB 2023).
//!
//! - **Slots.** A transaction holds one of W slots from its begin to its end, so a slot runs one
//!   transaction at a time.
//! - **Clock.** One clock issues start, snapshot and commit timestamps, each unique.
//! - **Commit logs.** Each slot lists the (start, commit) pairs of its committed transactions, in
//!   order. A version written by (slot s, start t) is visible to a snapshot taken at T when the
//!   last transaction of s to commit before T started at or after t. A slot's transactions run one
//!   after another, so that one and every earlier one committed before T; an aborted transaction's
//!   versions are gone before its slot runs another. A snapshot caches that start per slot.
//! - **Commit.** The commit timestamp is drawn, the Commit record appended and the pair entered in
//!   the slot's commit log, which its one writer publishes by its length, readers searching up to
//!   it. A snapshot is the published horizon: every commit timestamped at or below it is in its
//!   slot's log. A commit raises the horizon once it is published, as far as every commit below
//!   has been, and is acknowledged once the horizon covers it, waiting only on committers still
//!   publishing a lower timestamp: so a snapshot taken after the acknowledgement sees it, and no
//!   reader waits ([`horizon`]).
//! - **A transaction's records** carry its start timestamp as the transaction's id and chain to its
//!   previous group by where that group starts, so its records are found from its last one. Its
//!   slot's cell in the writer registry holds its first and last groups from its first group's
//!   reservation to its Commit's ([`super::log::Writers`]).
//! - **The write gate** ([`gate`]): a transaction takes it shared to write and exclusive to write
//!   the schema, and every end of the transaction releases it, before its slot is freed. A database
//!   whose writers must hold it ([`Transactions::require_gate`]) refuses a commit that wrote
//!   without it.
//! - **Creating a tree** is a change of the transaction's: the root's creation record leads the
//!   group that makes the root ([`Transactions::create_tree`]). Undoing it, by a rollback or in
//!   recovery, puts the root on a plain list, after every later change to the tree is undone, and
//!   frees nothing: the transaction's trees on the list are freed once it has ended.

use self::sync::{fence, AtomicU64, Ordering};
use super::btree::Tree;
use super::log::{framed_len, Admitted, GroupStart, Header, Kind, Log, Lsn, TxnTail, Writers};
use super::page::{PageId, SizeClass};
use super::segmented::Segmented;
use super::stack::Stack;
use crate::io::Completion;
use crate::sync::Arc;
use crossbeam_utils::CachePadded;
use horizon::Horizon;

/// The engine's atomics, or loom's.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicU64, Ordering};
}

mod ends;
mod gate;
mod horizon;
use crate::types::{IOCompletions, IOResult};
use crate::{LimboError, Result};
pub use gate::Taken;

/// A timestamp of the transaction clock. 0 is before every transaction.
pub type Ts = u64;

/// A statement's number within its transaction: 0 before the first.
pub type Statement = u32;

/// A commit's record: its timestamp.
pub const COMMIT: Kind = Kind::new(16).unwrap();

/// The record of a transaction that ended without committing, with no undo left: every change it
/// made put back. Empty.
pub const END: Kind = Kind::new(19).unwrap();

/// A tree's creation in a transaction: the root, then the root of the plain list its undo puts it
/// on, each 8 bytes little-endian. It leads the group that makes the root.
pub const CREATE: Kind = Kind::new(25).unwrap();

/// The root a creation record names, and the list its undo puts it on.
pub fn creation(payload: &[u8]) -> Option<(PageId, PageId)> {
    let word = |at: usize| {
        payload
            .get(at..at + 8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
    };
    let (root, list) = (PageId::from_raw(word(0)?), PageId::from_raw(word(8)?));
    let valid = |p: PageId| SizeClass::new(p.class().index()).is_some() && !p.is_temporary();
    (payload.len() == 16 && valid(root) && valid(list)).then_some((root, list))
}

/// A tree a transaction created and still holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Created {
    /// The statement that created it: a rollback past it undoes the creation.
    pub statement: Statement,
    pub root: PageId,
    /// The plain list the creation's undo puts the root on, keyed by its big-endian bytes.
    pub list: PageId,
}

/// The commit timestamp a Commit record's payload carries.
pub fn commit_ts(payload: &[u8]) -> Option<Ts> {
    payload.try_into().ok().map(u64::from_le_bytes)
}

/// Drawn from by every commit and read by every begin, so on a line of its own.
#[derive(Debug)]
pub struct Clock(CachePadded<AtomicU64>);

impl Clock {
    /// A clock whose timestamps are all above `last`.
    pub fn after(last: Ts) -> Self {
        Self(CachePadded::new(AtomicU64::new(last)))
    }

    /// A new timestamp, above every one issued before.
    pub fn next(&self) -> Ts {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// The last timestamp issued.
    pub fn now(&self) -> Ts {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Clone)]
pub struct TxnConfig {
    /// Transaction slots: the most transactions open at once. By default 100, PostgreSQL's
    /// default `max_connections`, since a PostgreSQL session holds at most one transaction.
    pub slots: usize,
}

impl Default for TxnConfig {
    fn default() -> Self {
        Self { slots: 100 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// Each statement reads a snapshot taken as it starts.
    ReadCommitted,
    /// Every statement reads the snapshot taken as the transaction began.
    Snapshot,
}

#[derive(Default)]
struct Pair {
    start: AtomicU64,
    commit: AtomicU64,
}

/// A slot's committed transactions, written by the slot's transaction alone as it commits.
struct CommitLog {
    /// Every transaction of the slot that started at or before this committed before any
    /// snapshot taken now.
    base: Ts,
    /// The (start, commit) pairs of the slot's later committed transactions, in order: both rise.
    entries: Segmented<Pair>,
    /// The pairs published: written after them, read before them.
    len: AtomicU64,
}

impl CommitLog {
    fn new(base: Ts) -> Self {
        Self {
            base,
            entries: Segmented::default(),
            len: AtomicU64::new(0),
        }
    }

    /// Publishes the pair of the slot's transaction that started at `start` and committed at
    /// `commit`: for that transaction alone.
    fn push(&self, start: Ts, commit: Ts) {
        // Relaxed: the slot's own count, and the pair published by its release below.
        let n = self.len.load(Ordering::Relaxed);
        let pair = self.entries.get_or_make(n);
        pair.start.store(start, Ordering::Relaxed);
        pair.commit.store(commit, Ordering::Relaxed);
        self.len.store(n + 1, Ordering::Release);
    }

    fn pair(&self, i: u64) -> (Ts, Ts) {
        let pair = self.entries.get(i).expect("a published pair");
        // Relaxed: read after the acquire of `len` that counted it.
        (
            pair.start.load(Ordering::Relaxed),
            pair.commit.load(Ordering::Relaxed),
        )
    }

    /// How many of the published pairs `before` holds for, they holding for a prefix.
    fn count(&self, before: impl Fn((Ts, Ts)) -> bool) -> u64 {
        let (mut lo, mut hi) = (0, self.len.load(Ordering::Acquire));
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if before(self.pair(mid)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Whether the slot's transaction that started at `start` committed.
    fn committed(&self, start: Ts) -> bool {
        if start <= self.base {
            return true;
        }
        let n = self.count(|(s, _)| s < start);
        n < self.len.load(Ordering::Acquire) && self.pair(n).0 == start
    }

    /// The start of the slot's last transaction committed at or before `at`.
    fn last_at(&self, at: Ts) -> Ts {
        match self.count(|(_, commit)| commit <= at) {
            0 => self.base,
            n => self.pair(n - 1).0,
        }
    }
}

pub struct Transactions {
    clock: Arc<Clock>,
    log: Arc<Log>,
    /// Per slot, published by its commits and read by every snapshot that looks at the slot, so
    /// each on a line of its own.
    slots: Box<[CachePadded<CommitLog>]>,
    /// Per slot, the start of the transaction it runs; 0 for none. Each written by its slot's
    /// transactions alone, so on a line of its own.
    running: Box<[CachePadded<AtomicU64>]>,
    horizon: Horizon,
    /// The transactions that have logged a group and not yet their Commit or End.
    writers: Arc<Writers>,
    /// A bit per slot, set while it is free.
    free: Box<[AtomicU64]>,
    /// Waiting for a free slot, woken to look again whenever one is freed.
    slot_waits: Stack<Completion>,
    /// Per slot, its transactions' end waiters ([`ends`]).
    ends: Box<[CachePadded<ends::Ends>]>,
    /// Per slot, the group its transaction belongs to; 0 for none.
    owner: Box<[CachePadded<AtomicU64>]>,
    gate: gate::Gate,
    /// Whether every transaction that writes must hold the gate.
    gated: std::sync::atomic::AtomicBool,
}

/// What a transaction holds of the write gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hold {
    None,
    Shared,
    /// An exclusive take in the word, still draining; `shared` if it held shared before.
    Pending {
        word: u64,
        shared: bool,
    },
    Exclusive {
        word: u64,
        shared: bool,
    },
}

/// The transaction a write met with a version still open: what it waits for the end of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Blocker {
    pub slot: u16,
    pub start: Ts,
}

/// Where the writer of a version stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Writer {
    Open,
    Committed,
    /// Neither: it ended without committing, and its versions should be gone.
    Gone,
}

/// A transaction, from its begin to its commit or abort.
pub struct Txn {
    slot: u16,
    start: Ts,
    isolation: Isolation,
    /// The current statement's snapshot.
    snapshot: Ts,
    /// The current statement: 0 before the first.
    command: Statement,
    /// Per slot, the start of its last transaction committed before the snapshot, once looked up.
    seen: Vec<Option<Ts>>,
    /// Where the transaction's last group starts; `None` while it has written nothing.
    tail: Option<GroupStart>,
    /// Where the log stood at the snapshot: what a read-only commit waits to be durable.
    log_at: Lsn,
    /// The trees the transaction created and holds, oldest first.
    created: Vec<Created>,
    /// The trees whose creation was undone, each on its list: to free once the transaction has
    /// ended.
    abandoned: Vec<PageId>,
    hold: Hold,
    /// Whether it wrote the schema: its commit, under an exclusive hold, raises the schema
    /// timestamp.
    wrote_schema: bool,
    /// Whether it has taken its snapshot: an exclusive taker begun without one takes it after the
    /// drain.
    snapped: bool,
    /// Whether its database requires writers to hold the write gate
    /// ([`Transactions::require_gate`]): then it logs only under a hold.
    gated: bool,
    /// Its first and last entries in its slot's version arena, once it has one: what a rollback
    /// walks back (DESIGN.md §11.5).
    replaced: Option<(arena::Ptr, arena::Ptr)>,
}

impl Txn {
    pub fn slot(&self) -> u16 {
        self.slot
    }

    /// The start timestamp, which also names the transaction.
    pub fn start(&self) -> Ts {
        self.start
    }

    pub fn command(&self) -> Statement {
        self.command
    }

    /// Moves the transaction on to statement `command`, as if it had run every one before: for a
    /// test of the last statement number.
    #[cfg(all(test, not(shuttle), not(atoll_loom)))]
    pub(crate) fn skip_to(&mut self, command: Statement) {
        self.command = command;
    }

    /// The header of the transaction's next record of `kind`.
    pub fn header(&self, kind: Kind) -> Header {
        Header {
            kind,
            flags: 0,
            txn: self.start,
            prev: self.tail,
        }
    }

    /// Notes the transaction's group appended at `start`.
    pub fn logged(&mut self, start: GroupStart) {
        assert!(
            !self.gated
                || matches!(
                    self.hold,
                    Hold::Shared | Hold::Exclusive { .. } | Hold::Pending { shared: true, .. }
                ),
            "a write without the write gate"
        );
        debug_assert!(self.tail.is_none_or(|t| t < start));
        self.tail = Some(start);
    }

    /// Where the transaction's last group starts.
    pub fn tail(&self) -> Option<GroupStart> {
        self.tail
    }

    pub fn wrote(&self) -> bool {
        self.tail.is_some()
    }

    /// The trees the transaction created and holds, oldest first.
    pub fn created(&self) -> &[Created] {
        &self.created
    }

    /// The trees whose creation was undone so far.
    pub fn abandoned(&self) -> &[PageId] {
        &self.abandoned
    }

    pub fn hold(&self) -> Hold {
        self.hold
    }

    /// Notes that the transaction wrote the schema.
    pub fn wrote_schema(&mut self) {
        self.wrote_schema = true;
    }

    pub fn snapped(&self) -> bool {
        self.snapped
    }

    /// The current statement's snapshot.
    pub fn snapshot(&self) -> Ts {
        self.snapshot
    }

    /// The number of its exclusive take of the gate while that take is pending.
    pub fn pending_take(&self) -> Option<u64> {
        match self.hold {
            Hold::Pending { word, .. } => Some(gate::take_of(word)),
            _ => None,
        }
    }
}

/// A whole rollback in progress: step it with [`Transactions::rollback`].
pub struct Abort {
    txn: Option<Txn>,
    undo: Rollback,
    admitted: Option<Admitted>,
    abandoned: Vec<PageId>,
}

impl Abort {
    pub fn new(txn: Txn) -> Self {
        Self {
            txn: Some(txn),
            undo: Rollback::after(0),
            admitted: None,
            abandoned: Vec::new(),
        }
    }

    /// Once the rollback is done, the trees whose creation it or an earlier statement rollback
    /// undid: each on its list, its transaction's End appended before them.
    pub fn abandoned(&self) -> &[PageId] {
        &self.abandoned
    }
}

/// A commit in progress: step it with [`Transactions::commit`].
pub struct Commit {
    txn: Option<Txn>,
    admitted: Option<Admitted>,
    ts: Option<Ts>,
    durable: Lsn,
    abandoned: Vec<PageId>,
    published: Option<Box<dyn FnOnce(Ts) + Send>>,
}

impl Commit {
    pub fn new(txn: Txn) -> Self {
        Self {
            txn: Some(txn),
            admitted: None,
            ts: None,
            durable: 0,
            abandoned: Vec::new(),
            published: None,
        }
    }

    /// Runs `hook` with the commit timestamp of a commit that wrote, once the horizon covers it
    /// and before the write gate is released: what a taker of the gate after the commit must see,
    /// such as the stamps of the trees it wrote and the header fields it set (DESIGN.md §17).
    pub fn on_published(mut self, hook: impl FnOnce(Ts) + Send + 'static) -> Self {
        self.published = Some(Box::new(hook));
        self
    }

    /// Once the commit is done, the trees whose creation a statement rollback undid: each on its
    /// list, the Commit appended before them.
    pub fn abandoned(&self) -> &[PageId] {
        &self.abandoned
    }
}

impl Transactions {
    /// `config.slots` slots over `clock`, every version already in the database committed, the
    /// writers entered in `writers`.
    pub fn new(
        config: &TxnConfig,
        clock: Arc<Clock>,
        log: Arc<Log>,
        writers: Arc<Writers>,
    ) -> Self {
        assert!(
            (1..=u16::MAX as usize + 1).contains(&config.slots),
            "{} transaction slots",
            config.slots
        );
        let base = clock.now();
        let words = config.slots.div_ceil(64);
        Self {
            slots: (0..config.slots)
                .map(|_| CachePadded::new(CommitLog::new(base)))
                .collect(),
            running: (0..config.slots)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            horizon: Horizon::new(config.slots, base),
            writers,
            free: (0..words)
                .map(|w| {
                    let bits = (config.slots - w * 64).min(64);
                    AtomicU64::new(if bits == 64 {
                        u64::MAX
                    } else {
                        (1 << bits) - 1
                    })
                })
                .collect(),
            slot_waits: Stack::default(),
            ends: (0..config.slots)
                .map(|_| CachePadded::new(ends::Ends::default()))
                .collect(),
            owner: (0..config.slots)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            gate: gate::Gate::new(config.slots),
            gated: std::sync::atomic::AtomicBool::new(false),
            clock,
            log,
        }
    }

    /// From now on, every transaction that writes must hold the write gate: a commit of one that
    /// wrote without it is refused. Turso's SQL layer requires it; a key-value user need not.
    pub fn require_gate(&self) {
        self.gated.store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn clock(&self) -> &Arc<Clock> {
        &self.clock
    }

    /// What `txn`'s next change is logged as: its group enters the writer registry as it is
    /// reserved.
    pub fn tail(&self, txn: &Txn) -> TxnTail {
        TxnTail::new(txn.start, txn.tail, self.writers.clone(), txn.slot)
    }

    /// Appends a record of `txn`'s own of `kind`, outside any tree, within `admitted`'s room, and
    /// returns its LSN.
    pub fn append(
        &self,
        txn: &mut Txn,
        admitted: &mut Admitted,
        kind: Kind,
        payload: &[&[u8]],
    ) -> Lsn {
        let (writers, slot, id) = (&self.writers, txn.slot, txn.start);
        let (start, lsn) = self
            .log
            .append_with(admitted, &txn.header(kind), payload, |at| {
                writers.reserved(slot, id, at)
            });
        txn.logged(start);
        lsn
    }

    /// A free slot, taken.
    fn take_slot(&self) -> Option<u16> {
        for (w, word) in self.free.iter().enumerate() {
            let mut bits = word.load(Ordering::Acquire);
            while bits != 0 {
                let bit = bits.trailing_zeros();
                match word.compare_exchange_weak(
                    bits,
                    bits & !(1 << bit),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Some((w * 64) as u16 + bit as u16),
                    Err(now) => bits = now,
                }
            }
        }
        None
    }

    /// Begins a transaction of no group once a slot is free.
    pub fn begin(&self, isolation: Isolation) -> Result<IOResult<Txn>> {
        self.begin_in(isolation, 0)
    }

    /// Begins a transaction of `group` once a slot is free: a wait on it is a wait on the group
    /// ([`holder`](Self::holder)).
    pub fn begin_in(&self, isolation: Isolation, group: u64) -> Result<IOResult<Txn>> {
        Ok(match self.begin_slot_in(isolation, group)? {
            IOResult::Done(mut txn) => {
                self.take_snapshot(&mut txn);
                IOResult::Done(txn)
            }
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// Takes the snapshot of `txn`, begun without one ([`begin_slot_in`](Self::begin_slot_in)).
    pub fn take_snapshot(&self, txn: &mut Txn) {
        assert!(!txn.snapped, "a second snapshot of one transaction");
        txn.snapshot = self.horizon.at();
        txn.seen.fill(None);
        txn.log_at = self.log.end();
        txn.snapped = true;
    }

    /// Begins a transaction of `group` once a slot is free, without a snapshot: an exclusive
    /// taker takes its slot before it marks itself pending, and its snapshot after the drain.
    pub fn begin_slot_in(&self, isolation: Isolation, group: u64) -> Result<IOResult<Txn>> {
        let slot = match self.take_slot() {
            Some(slot) => slot,
            None => {
                let c = Completion::new_remote_wait();
                self.slot_waits.push(c.clone());
                // Pairs with the fence of a slot freed: either it finds this waiting, or this
                // finds the slot. A completion left behind is completed later, for no one.
                fence(Ordering::SeqCst);
                match self.take_slot() {
                    Some(slot) => slot,
                    None => return Ok(IOResult::IO(IOCompletions(c))),
                }
            }
        };
        let start = self.clock.next();
        ends::seat(
            &self.running[slot as usize],
            &self.owner[slot as usize],
            start,
            group,
        );
        // The snapshot, when it is taken, before the log's end: every commit it covers is in the
        // log by then.
        Ok(IOResult::Done(Txn {
            slot,
            start,
            isolation,
            snapshot: 0,
            command: 0,
            seen: vec![None; self.slots.len()],
            tail: None,
            log_at: 0,
            created: Vec::new(),
            abandoned: Vec::new(),
            hold: Hold::None,
            wrote_schema: false,
            snapped: false,
            gated: self.gated.load(std::sync::atomic::Ordering::Acquire),
            replaced: None,
        }))
    }

    /// Takes the gate shared for `txn`, which is to write. Behind another's exclusive take it
    /// registers `c` on that take's end and reports it; the caller takes again once `c` is done.
    /// A transaction whose snapshot is older than the schema timestamp fails with
    /// `SchemaConflict`.
    pub fn take_shared(&self, txn: &mut Txn, c: &Completion) -> Result<Taken> {
        match txn.hold {
            Hold::None => {}
            // Its own exclusive take, pending with no shared hold under it: a shared take would
            // wait behind it forever. The statement that made it gives it up when it stops
            // waiting (R15), before another writes.
            Hold::Pending { shared: false, .. } => {
                return Err(LimboError::InternalError(
                    "a shared take behind the transaction's own pending exclusive take".into(),
                ))
            }
            _ => return Ok(Taken::Done),
        }
        let taken = self.gate.take_shared(txn.slot, txn.start, c);
        if taken == Taken::Done {
            if txn.snapped && txn.snapshot < self.gate.schema() {
                self.gate.release_shared(txn.slot, txn.start);
                return Err(LimboError::SchemaConflict);
            }
            txn.hold = Hold::Shared;
        }
        Ok(taken)
    }

    /// Takes the gate exclusive for `txn`: its take put in the word, pending, then held once every
    /// other shared holder has left. Behind another's take, or with holders left, it registers `c`
    /// and reports them; the caller takes again once `c` is done, its take staying pending
    /// meanwhile ([`give_up`](Self::give_up)). A taker with no snapshot takes it once held; one
    /// with a snapshot older than the schema timestamp fails with `SchemaConflict`.
    pub fn take_exclusive(&self, txn: &mut Txn, c: &Completion) -> Result<Taken> {
        let (word, shared) = match txn.hold {
            Hold::Exclusive { .. } => return Ok(Taken::Done),
            Hold::Pending { word, shared } => (word, shared),
            hold => {
                let shared = hold == Hold::Shared;
                match self.gate.begin_exclusive(txn.slot, c) {
                    Ok(word) => {
                        txn.hold = Hold::Pending { word, shared };
                        (word, shared)
                    }
                    Err(taken) => return Ok(taken),
                }
            }
        };
        match self.gate.drain(txn.slot, word, c) {
            Taken::Done => {}
            Taken::Lost => {
                txn.hold = if shared { Hold::Shared } else { Hold::None };
                return Ok(Taken::Lost);
            }
            other => return Ok(other),
        }
        txn.hold = Hold::Exclusive { word, shared };
        if !txn.snapped {
            self.take_snapshot(txn);
        } else if txn.snapshot < self.gate.schema() {
            return Err(LimboError::SchemaConflict);
        }
        Ok(Taken::Done)
    }

    /// Hands back `txn`'s exclusive hold, its schema write refused before it wrote anything,
    /// waking whoever waits behind it; a shared hold it had before stays.
    pub fn hand_back_exclusive(&self, txn: &mut Txn) {
        if let Hold::Exclusive { word, shared } = txn.hold {
            self.gate.release_exclusive(word, None);
            txn.hold = if shared { Hold::Shared } else { Hold::None };
        }
    }

    /// Gives up `txn`'s pending exclusive take, waking whoever waits behind it; a shared hold it
    /// had before stays.
    pub fn give_up(&self, txn: &mut Txn) {
        if let Hold::Pending { word, shared } = txn.hold {
            self.gate.clear(word);
            txn.hold = if shared { Hold::Shared } else { Hold::None };
        }
    }

    /// Clears the exclusive take numbered `take` from the gate while it is still pending, for a
    /// victim of the wait graph that is not stepping: its taker finds it lost.
    pub fn clear_take(&self, take: u64) {
        let w = self.gate.word();
        if w != 0 && gate::take_of(w) == take && !gate::held(w) {
            self.gate.clear(w);
        }
    }

    /// The transaction owning the exclusive take numbered `take`, while it is in the gate.
    pub fn take_holder(&self, take: u64) -> Option<Blocker> {
        let slot = self.gate.take_owner(take)?;
        let start = self.running[slot as usize].load(Ordering::Acquire);
        (start != 0).then_some(Blocker { slot, start })
    }

    /// Whether `blocker`'s transaction holds the gate shared.
    pub fn holds_shared(&self, blocker: Blocker) -> bool {
        self.gate.holds_shared(blocker.slot, blocker.start)
    }

    /// Releases whatever `txn` holds of the gate, at its end, before its slot is freed; `schema`
    /// is its commit timestamp when it wrote the schema under an exclusive hold.
    fn release_hold(&self, txn: &mut Txn, schema: Option<Ts>) {
        match txn.hold {
            Hold::None => {}
            Hold::Shared => self.gate.release_shared(txn.slot, txn.start),
            Hold::Pending { word, shared } | Hold::Exclusive { word, shared } => {
                let schema = matches!(txn.hold, Hold::Exclusive { .. })
                    .then_some(schema)
                    .flatten();
                self.gate.release_exclusive(word, schema);
                if shared {
                    self.gate.release_shared(txn.slot, txn.start);
                }
            }
        }
        txn.hold = Hold::None;
    }

    /// Creates an empty tree of `class` in `txn`'s current statement, its root's creation record
    /// leading the group that makes it. A rollback past the statement, or recovery's undo of the
    /// transaction, puts the root on the plain tree `list`, keyed by its big-endian bytes. Step it
    /// until done.
    pub fn create_tree(
        &self,
        versions: &Versions,
        txn: &mut Txn,
        class: SizeClass,
        list: PageId,
    ) -> Result<IOResult<PageId>> {
        assert!(txn.command > 0, "a tree created before the first statement");
        let tail = self.tail(txn);
        let (pool, store) = versions.pool_and_store();
        let made =
            Tree::create_logged(pool, store, class, &tail, CREATE, &list.raw().to_le_bytes())?;
        let (root, start) = match made {
            IOResult::Done(made) => made,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        txn.logged(start);
        txn.created.push(Created {
            statement: txn.command,
            root,
            list,
        });
        Ok(IOResult::Done(root))
    }

    /// Starts `txn`'s next statement. It sees what the transaction's earlier statements wrote, and
    /// under read committed, a snapshot taken now.
    pub fn statement(&self, txn: &mut Txn) -> Result<()> {
        txn.command = txn.command.checked_add(1).ok_or_else(|| {
            LimboError::InternalError(format!(
                "a transaction of more than {} statements",
                Statement::MAX
            ))
        })?;
        if txn.isolation == Isolation::ReadCommitted && txn.command > 1 {
            txn.snapshot = self.horizon.at();
            txn.seen.fill(None);
            txn.log_at = self.log.end();
        }
        Ok(())
    }

    /// Whether the version written by the transaction that started at `start` in `slot` is visible
    /// to `txn`'s current statement. Every version of `txn` itself is, its current statement's
    /// included, as Turso's VDBE expects of a cursor (DESIGN.md §11.1).
    pub fn visible(&self, txn: &mut Txn, slot: u16, start: Ts) -> bool {
        debug_assert!(txn.snapped, "a read before the transaction's snapshot");
        if slot == txn.slot && start == txn.start {
            return true;
        }
        let snapshot = txn.snapshot;
        let last = *txn.seen[slot as usize]
            .get_or_insert_with(|| self.slots[slot as usize].last_at(snapshot));
        start <= last
    }

    /// Commits `op`'s transaction (DESIGN.md §11.4). It is visible to every snapshot taken after it
    /// is done, which is once the horizon covers its commit timestamp and the log is durable up to
    /// its Commit record. A transaction that wrote nothing appends nothing, and is done once the
    /// log is durable up to where it stood at its snapshot. Returns the commit timestamp, `None`
    /// for a transaction that wrote nothing.
    pub fn commit(&self, op: &mut Commit) -> Result<IOResult<Option<Ts>>> {
        if let Some(txn) = &op.txn {
            if txn.wrote() && op.admitted.is_none() {
                op.admitted = Some(match self.log.admit(framed_len(8))? {
                    IOResult::Done(a) => a,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                });
            }
            let mut txn = op.txn.take().expect("a transaction");
            op.abandoned = std::mem::take(&mut txn.abandoned);
            assert!(
                !txn.wrote()
                    || txn.hold != Hold::None
                    || !self.gated.load(std::sync::atomic::Ordering::Acquire),
                "a commit of a transaction that wrote without the write gate"
            );
            op.durable = match op.admitted.take() {
                None => txn.log_at,
                Some(mut admitted) => {
                    let ts = self.horizon.draw(txn.slot, &self.clock);
                    // Leaves the registry as its room is reserved: a checkpoint whose redo point
                    // passes the Commit never lists it as needing undo.
                    let (writers, slot, id) = (&self.writers, txn.slot, txn.start);
                    let (_, lsn) = self.log.append_with(
                        &mut admitted,
                        &txn.header(COMMIT),
                        &[&ts.to_le_bytes()],
                        |_| writers.finished(slot, id),
                    );
                    self.slots[txn.slot as usize].push(txn.start, ts);
                    self.horizon.published(txn.slot, &self.clock);
                    op.ts = Some(ts);
                    lsn
                }
            };
            // The gate after the horizon covers the commit, and after what a taker of it after the
            // commit must see, before the slot is freed.
            if let (Some(ts), Some(hook)) = (op.ts, op.published.take()) {
                hook(ts);
            }
            let schema = op.ts.filter(|_| txn.wrote_schema);
            self.release_hold(&mut txn, schema);
            self.release(txn.slot);
        }
        if let Some(ts) = op.ts {
            if let Some(wait) = self.horizon.wait(ts, &self.clock) {
                return Ok(IOResult::IO(IOCompletions(wait)));
            }
        }
        Ok(match self.log.wait_durable(op.durable)? {
            IOResult::Done(()) => IOResult::Done(op.ts),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// Ends `txn`, which wrote nothing, without committing it. One that wrote is rolled back
    /// ([`rollback`](Self::rollback)).
    pub fn abort(&self, mut txn: Txn) {
        assert!(
            !txn.wrote(),
            "aborting a transaction that wrote: roll it back"
        );
        self.release_hold(&mut txn, None);
        self.release(txn.slot);
    }

    /// Rolls `op`'s transaction back whole (DESIGN.md §11.5): every change it made put back, its
    /// End appended, and its slot freed with no commit pair. Nothing waits for the log to be
    /// durable: a rollback a crash loses is undone again by recovery. Step it until done.
    pub fn rollback(&self, versions: &Versions, op: &mut Abort) -> Result<IOResult<()>> {
        let txn = op
            .txn
            .as_mut()
            .expect("a rollback stepped after it was done");
        if let IOResult::IO(io) = rollback::undo(self, versions, txn, &mut op.undo)? {
            return Ok(IOResult::IO(io));
        }
        if txn.wrote() {
            if op.admitted.is_none() {
                op.admitted = Some(match self.log.admit(framed_len(0))? {
                    IOResult::Done(a) => a,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                });
            }
            let mut admitted = op.admitted.take().expect("admitted");
            // Leaves the registry as its room is reserved, as a Commit does.
            let (writers, slot, id) = (&self.writers, txn.slot, txn.start);
            self.log
                .append_with(&mut admitted, &txn.header(END), &[], |_| {
                    writers.finished(slot, id)
                });
        }
        let mut txn = op.txn.take().expect("a transaction");
        op.abandoned = std::mem::take(&mut txn.abandoned);
        // Every version it wrote is gone: the slot's next transaction may commit, and writers
        // behind the gate may go on.
        self.release_hold(&mut txn, None);
        self.release(txn.slot);
        Ok(IOResult::Done(()))
    }

    /// Rolls `txn` back to where it stood after its statement `to`: every change of the
    /// statements since put back. It then starts its next statement, numbered past every one
    /// rolled back, which are never used again. Step it until done.
    pub fn rollback_to(
        &self,
        versions: &Versions,
        txn: &mut Txn,
        op: &mut Rollback,
    ) -> Result<IOResult<()>> {
        assert!(
            op.to() <= txn.command,
            "rolling back to a statement not yet run"
        );
        if let IOResult::IO(io) = rollback::undo(self, versions, txn, op)? {
            return Ok(IOResult::IO(io));
        }
        // Its keys are put back with the transaction still running: whoever waited on it judges
        // them again.
        self.ends[txn.slot as usize].wake();
        self.statement(txn)?;
        Ok(IOResult::Done(()))
    }

    /// Where the transaction that started at `start` in `slot` stands, for a version it wrote.
    pub fn writer(&self, slot: u16, start: Ts) -> Writer {
        // The slot first: a committing transaction enters its pair before it leaves the slot, so a
        // transaction gone from the slot by this read has its pair in the log by the next.
        let running = self.running[slot as usize].load(Ordering::Acquire);
        if self.slots[slot as usize].committed(start) {
            Writer::Committed
        } else if running == start {
            Writer::Open
        } else {
            Writer::Gone
        }
    }

    /// Registers `c` to be completed at the end of `blocker`'s transaction, or when a statement
    /// rollback of it puts keys back. The caller then steps its write again, and parks on `c` only
    /// if it waits on the same blocker still. A wait reuses its completion while it is unfinished.
    pub fn wait_end(&self, blocker: Blocker, c: &Completion) {
        self.ends[blocker.slot as usize].register(c);
    }

    /// Joins `txn` to `group` in the wait graph, before it writes or takes the gate: a wait on
    /// it from then on is a wait on the group (DESIGN.md §17, §3.1).
    pub fn join(&self, txn: &Txn, group: u64) {
        self.owner[txn.slot as usize].store(group, Ordering::Release);
    }

    /// The group of `blocker`'s transaction while it runs; `None` once it has ended.
    pub fn holder(&self, blocker: Blocker) -> Option<u64> {
        let slot = blocker.slot as usize;
        ends::holder(&self.running[slot], &self.owner[slot], blocker.start)
    }

    fn release(&self, slot: u16) {
        ends::vacate(&self.running[slot as usize], &self.owner[slot as usize]);
        self.free[slot as usize / 64].fetch_or(1 << (slot % 64), Ordering::AcqRel);
        // Pairs with the fence of a waiter: either this finds it, or it finds the slot.
        fence(Ordering::SeqCst);
        for c in self.slot_waits.take_all() {
            c.complete(0);
        }
        self.ends[slot as usize].wake();
    }
}

pub mod arena;
pub mod rollback;
pub mod versioned;

use rollback::Rollback;
use versioned::Versions;

#[cfg(test)]
mod tests;

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    /// A slot's transaction publishes its pair as a reader searches the slot's log: a pair found
    /// is found whole, and the commits found rise with what the reader saw.
    #[test]
    fn a_pair_found_in_a_commit_log_is_read_whole() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let log = Arc::new(CommitLog::new(0));
            log.push(1, 2);
            let writer = {
                let log = log.clone();
                thread::spawn(move || log.push(3, 4))
            };
            let committed = log.committed(3);
            let last = log.last_at(4);
            writer.join().unwrap();
            assert!(
                last == 1 || last == 3,
                "a pair read before it was whole: {last}"
            );
            assert!(!committed || last == 3, "found committed, then not");
            assert_eq!(log.last_at(3), 1);
        });
    }
}
