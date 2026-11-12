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
//! - **A transaction's records** are its commit's alone (audit 17): its changes, kept by key until
//!   then, go out in parts ahead of its Commit when they outgrow a log chunk, each carrying its
//!   start timestamp as the transaction's id. Nothing a transaction does before its commit reaches
//!   the log, and one that ends without committing leaves none.
//! - **The write gate** ([`gate`]): a transaction takes it shared to write and exclusive to write
//!   the schema, and every end of the transaction releases it, before its slot is freed. A database
//!   whose writers must hold it ([`Transactions::require_gate`]) refuses a commit that wrote
//!   without it.
//! - **Creating a tree** is a change of the transaction's ([`Transactions::create_tree`]): no
//!   checkpoint maps the tree until one's snapshot sees the creation. Undoing it, by a rollback,
//!   puts the root on a plain list, after every later change to the tree is undone, and frees
//!   nothing: the transaction's trees on the list are freed once it has ended.

use self::sync::{fence, AtomicU64, Ordering};
use super::btree::Tree;
use super::buffer::cell::{CompletionCell, CompletionDomain};
use super::epoch::Epochs;
use super::log::{framed_len, Admitted, GroupStart, Header, Kind, Log, LogWait, Lsn, BLOCK};
use super::page::{PageId, SizeClass};
use super::segmented::Segmented;
use super::stack::Stack;
use crate::io::Completion;
use crate::sync::{Arc, OnceLock};
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
mod snapshots;
use crate::types::{IOCompletions, IOResult};
use crate::{LimboError, Result};
pub use gate::Taken;

/// A timestamp of the transaction clock. 0 is before every transaction.
pub type Ts = u64;

/// A statement's number within its transaction: 0 before the first.
pub type Statement = u32;

/// A commit's record: its timestamp.
pub const COMMIT: Kind = Kind::new(16).unwrap();

/// A part of a transaction's changes published before its Commit, a chunk once it filled (audit
/// 17, DESIGN.md §11.4.1): the transaction's start and where the part begins in its changes, each 8
/// bytes little-endian, then the part. It names no transaction in its header, so it is in no
/// transaction's chain, and a transaction with no Commit after it is never replayed.
pub const SPILL: Kind = Kind::new(28).unwrap();

/// Changes made outside any transaction and published at once (audit 17), such as a sequence's
/// value (DESIGN.md §17, R8): changes as a transaction's are written ([`changes`]).
pub const RUN: Kind = Kind::new(29).unwrap();

/// In a slot's reserved position: its commit is taking room in the log, where not yet known.
const RESERVING: u64 = 1 << 63;
/// A cut registered before marking a reservation for its publisher to wake.
const RESERVATION_WAITED: u64 = 1 << 62;
const RESERVING_WAITED: u64 = RESERVING | RESERVATION_WAITED;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reservation {
    None,
    Reserving,
    Position(Lsn),
}

/// A checkpoint's cut under way ([`Transactions::cut`]).
#[derive(Debug, Default)]
pub struct Cut {
    /// The log position replay starts from, and the clock the horizon must reach.
    at: Option<(Lsn, Ts)>,
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
    payload
        .get(..8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
}

/// What a Commit record's payload carries past its timestamp (audit 17, DESIGN.md §11.4.1): where
/// the part it holds begins in the transaction's changes, and that part, the rest having gone
/// before in [`SPILL`] records.
pub fn commit_changes(payload: &[u8]) -> Option<(u64, &[u8])> {
    let from = u64::from_le_bytes(payload.get(8..16)?.try_into().unwrap());
    Some((from, &payload[16..]))
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
    /// Per slot, a timestamp at or below the snapshot its transaction reads ([`snapshots`]). Read
    /// by a checkpoint judging what every running snapshot sees, so each on a line of its own.
    snapshots: Box<[CachePadded<AtomicU64>]>,
    /// Per slot, where the commit under way first took room in the log for its changes (a
    /// spilled part or its Commit), plus one; [`RESERVING`] while it takes it; 0 for none. Read by
    /// a checkpoint's cut ([`Transactions::cut`]), so each on a line of its own.
    reserved: Box<[CachePadded<AtomicU64>]>,
    /// Cuts waiting for a reservation's first position to be published.
    reservation_wait: CompletionCell,
    /// Completion announcements, made only on the first wait for a reservation.
    reservation_completions: OnceLock<CompletionDomain>,
    horizon: Horizon,
    /// The snapshot of the checkpoint under way, `Ts::MAX` for none: no reserve a version it reads
    /// needs is given back while it runs.
    checkpoint_at: AtomicU64,
    /// A bit per slot, set while it is free.
    free: Box<[AtomicU64]>,
    /// Waiting for a free slot, woken to look again whenever one is freed.
    slot_waits: Stack<Completion>,
    /// Per slot, its transactions' end waiters ([`ends`]).
    ends: Box<[CachePadded<ends::Ends>]>,
    /// Per slot, the group its transaction belongs to; 0 for none.
    owner: Box<[CachePadded<AtomicU64>]>,
    /// Per slot, the start of the transaction left in it ([`abandon`](Self::abandon)); 0 for none.
    abandoned: Box<[AtomicU64]>,
    /// Why the database failed, for good until it is reopened: a transaction left where it stood,
    /// its rollback unable to finish ([`abandon`](Self::abandon)).
    failure: crate::storage::atoll::once::SetOnce<LimboError>,
    gate: gate::Gate,
    /// Whether every transaction that writes must hold the gate.
    gated: std::sync::atomic::AtomicBool,
    /// Per slot, the changes buffer its transactions keep from one to the next, with a chunk's
    /// memory (DESIGN.md §11.4.1).
    kept: Box<[Kept]>,
    /// Bytes of a chunk of changes: the log's chunk, the largest single write, or less where the
    /// log's buffer could not admit one with the block before it padded.
    redo_chunk: usize,
}

/// A slot's changes buffer between its transactions: read and written only by the slot's
/// transaction, at its begin and at its end before the slot is freed, which orders one
/// transaction's end before the next one's begin.
struct Kept(std::cell::UnsafeCell<Option<changes::Changes>>);

// SAFETY: as its doc says, one slot's transaction at a time reaches it, the slot's freeing and
// taking ordering the hand-over.
unsafe impl Sync for Kept {}

impl Kept {
    /// # Safety
    /// The caller holds the slot.
    unsafe fn take(&self) -> Option<changes::Changes> {
        (*self.0.get()).take()
    }

    /// # Safety
    /// The caller holds the slot.
    unsafe fn put(&self, kept: changes::Changes) {
        *self.0.get() = Some(kept);
    }
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
    /// Left where it stood, its rollback unable to finish ([`Transactions::abandon`]): its
    /// versions stay, never another's, and the database has failed.
    Abandoned,
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
    /// Its waits on the log across its spills (DESIGN.md 22.4).
    log_wait: LogWait,
    /// Whether its database requires writers to hold the write gate
    /// ([`Transactions::require_gate`]): then it changes anything only under a hold.
    gated: bool,
    /// Its first and last entries in its slot's version arena, once it has one: what a rollback
    /// walks back (DESIGN.md §11.5).
    replaced: Option<(arena::Ptr, arena::Ptr)>,
    /// Its changes by key, which its commit publishes (audit 17).
    changes: changes::Changes,
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
        self.changes.moved();
    }

    /// The header of the transaction's record of `kind`.
    pub fn header(&self, kind: Kind) -> Header {
        Header {
            kind,
            flags: 0,
            txn: self.start,
            prev: None,
        }
    }

    /// Whether it holds a change: its commit publishes them, and enters it in its slot's commit
    /// log, which judges every version it wrote committed.
    pub fn wrote(&self) -> bool {
        !self.changes.is_empty()
    }

    /// The trees the transaction created and holds, oldest first.
    pub fn created(&self) -> &[Created] {
        &self.created
    }

    /// Its changes by key, as its commit publishes them.
    pub fn changes(&self) -> &changes::Changes {
        &self.changes
    }

    /// Notes a change its current statement made, by key.
    pub fn changed(&mut self, note: impl FnOnce(Statement, &mut changes::Changes)) {
        assert!(
            !self.gated
                || matches!(
                    self.hold,
                    Hold::Shared | Hold::Exclusive { .. } | Hold::Pending { shared: true, .. }
                ),
            "a write without the write gate"
        );
        note(self.command, &mut self.changes);
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
    abandoned: Vec<PageId>,
}

impl Abort {
    pub fn new(txn: Txn) -> Self {
        Self {
            txn: Some(txn),
            undo: Rollback::after(0),
            abandoned: Vec::new(),
        }
    }

    /// Once the rollback is done, the trees whose creation it or an earlier statement rollback
    /// undid: each on its list.
    pub fn abandoned(&self) -> &[PageId] {
        &self.abandoned
    }

    /// The transaction its rollback could not finish, to be left where it stands
    /// ([`Transactions::abandon`]).
    pub fn take_txn(&mut self) -> Option<Txn> {
        self.txn.take()
    }
}

/// A commit in progress: step it with [`Transactions::commit`].
pub struct Commit {
    txn: Option<Txn>,
    admitted: Option<Admitted>,
    /// Its waits on the log across its steps: the commit record's admission, then durability.
    log_wait: LogWait,
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
            log_wait: LogWait::default(),
            ts: None,
            durable: 0,
            abandoned: Vec::new(),
            published: None,
        }
    }

    /// Runs `hook` with the commit timestamp of a commit that wrote, once it is published and
    /// before the write gate is released: what a taker of the gate after the commit must see, such
    /// as the stamps of the trees it wrote and the header fields it set (DESIGN.md §17). Published
    /// is not covered: the horizon may still be below the timestamp while a commit below it on
    /// another slot draws, and a snapshot taken then misses the commit
    /// ([`Transactions::wait_covered`]).
    pub fn on_published(mut self, hook: impl FnOnce(Ts) + Send + 'static) -> Self {
        self.published = Some(Box::new(hook));
        self
    }

    /// Whether the commit has been published: its pair is in its slot's commit log, so what it
    /// wrote is visible to every snapshot at or above its timestamp and stays so, whatever its wait
    /// for durability then meets. A snapshot taken now is at or above it only once the horizon
    /// covers it ([`Transactions::wait_covered`]).
    pub fn published(&self) -> bool {
        self.ts.is_some()
    }

    /// The commit timestamp, once the commit is published; `None` before, and for a transaction
    /// that wrote nothing.
    pub fn ts(&self) -> Option<Ts> {
        self.ts
    }

    /// Whether the transaction is over and its slot freed: its commit published, or, having
    /// written nothing, ended without one. What is left is the commit's waits.
    pub fn ended(&self) -> bool {
        self.txn.is_none()
    }

    /// The transaction of a commit that failed before it ended, to be rolled back
    /// ([`Abort::new`]): nothing it wrote was published.
    pub fn take_txn(&mut self) -> Option<Txn> {
        self.txn.take()
    }

    /// Once the commit is done, the trees whose creation a statement rollback undid: each on its
    /// list, the Commit appended before them.
    pub fn abandoned(&self) -> &[PageId] {
        &self.abandoned
    }
}

impl Transactions {
    /// `config.slots` slots over `clock`, every version already in the database committed.
    pub fn new(config: &TxnConfig, clock: Arc<Clock>, log: Arc<Log>) -> Self {
        assert!(
            (1..=u16::MAX as usize + 1).contains(&config.slots),
            "{} transaction slots",
            config.slots
        );
        let base = clock.now();
        let words = config.slots.div_ceil(64);
        let redo_chunk = {
            let most = log.chunk_bytes().min(log.buffer_bytes() - BLOCK);
            (most - most % BLOCK) as usize
        };
        // Free chunks kept: as many as the log's buffer holds unflushed, which a transaction
        // spilling as fast as the device takes them cycles through, and one more.
        let chunks = changes::Chunks::new(
            redo_chunk,
            (log.buffer_bytes() as usize).div_ceil(redo_chunk) + 1,
        );
        Self {
            slots: (0..config.slots)
                .map(|_| CachePadded::new(CommitLog::new(base)))
                .collect(),
            running: (0..config.slots)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            snapshots: (0..config.slots)
                .map(|_| CachePadded::new(AtomicU64::new(snapshots::NONE)))
                .collect(),
            reserved: (0..config.slots)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            reservation_wait: CompletionCell::default(),
            reservation_completions: OnceLock::new(),
            horizon: Horizon::new(config.slots, base),
            checkpoint_at: AtomicU64::new(Ts::MAX),
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
            abandoned: (0..config.slots).map(|_| AtomicU64::new(0)).collect(),
            failure: Default::default(),
            gate: gate::Gate::new(config.slots),
            gated: std::sync::atomic::AtomicBool::new(false),
            kept: (0..config.slots)
                .map(|_| {
                    Kept(std::cell::UnsafeCell::new(Some(
                        changes::Changes::in_chunks(Some(chunks.clone()), redo_chunk, None),
                    )))
                })
                .collect(),
            redo_chunk,
            clock,
            log,
        }
    }

    /// Gives `changes`, of the transaction ending in `slot`, to the slot's next transaction,
    /// before the slot is freed.
    fn keep(&self, slot: u16, mut changes: changes::Changes) {
        changes.recycle();
        // SAFETY: the transaction ending holds the slot until it is freed.
        unsafe { self.kept[slot as usize].put(changes) };
    }

    /// Spills `txn`'s sealed chunks of changes to the log (DESIGN.md §11.4.1): each a [`SPILL`]
    /// record, its slot marked with where its first part lies. Step it until done, holding no
    /// latch.
    pub fn spill(&self, txn: &mut Txn) -> Result<IOResult<()>> {
        while let Some(len) = txn.changes.sealed_len() {
            // Its record in whole blocks, and the rest of the block before it.
            let room = framed_len(16 + len).next_multiple_of(BLOCK) + BLOCK;
            let mut admitted = match self.log.admit(&mut txn.log_wait, room)? {
                IOResult::Done(a) => a,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let (from, len, first, mut block) = txn.changes.take_sealed();
            let words = &mut block.bytes_mut()[changes::FRONT - 16..changes::FRONT];
            words[..8].copy_from_slice(&txn.start.to_le_bytes());
            words[8..].copy_from_slice(&from.to_le_bytes());
            let header = Header {
                kind: SPILL,
                flags: 0,
                txn: 0,
                prev: None,
            };
            self.reserving(txn.slot);
            let (at, lsn) = self
                .log
                .publish(&mut admitted, &header, block, 16 + len, |at| {
                    self.reserved_at(txn.slot, at)
                });
            txn.changes.spilled(changes::Part {
                from,
                len: len as u64,
                first,
                at,
                lsn,
            });
        }
        Ok(IOResult::Done(()))
    }

    /// Where the log is held back for the parts of transactions still running: the first part's
    /// record of each, at most; the log's end if none.
    pub fn held_from(&self) -> Lsn {
        self.reserved
            .iter()
            .map(|mark| mark.load(Ordering::SeqCst))
            .filter_map(|word| match decode_reservation(word) {
                Reservation::Position(at) => Some(at),
                Reservation::None | Reservation::Reserving => None,
            })
            .fold(self.log.end(), Lsn::min)
    }

    /// From now on, every transaction that writes must hold the write gate: a commit of one that
    /// wrote without it is refused. Turso's SQL layer requires it; a key-value user need not.
    pub fn require_gate(&self) {
        self.gated.store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    pub fn clock(&self) -> &Arc<Clock> {
        &self.clock
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
        txn.snapshot = snapshots::take(&self.snapshots[txn.slot as usize], || self.horizon.at());
        txn.seen.fill(None);
        txn.changes.moved();
        txn.log_at = self.log.end();
        txn.snapped = true;
    }

    /// Begins a transaction of `group` once a slot is free, without a snapshot: an exclusive
    /// taker takes its slot before it marks itself pending, and its snapshot after the drain.
    pub fn begin_slot_in(&self, isolation: Isolation, group: u64) -> Result<IOResult<Txn>> {
        self.refuse_if_failed()?;
        let slot = match self.take_slot() {
            Some(slot) => slot,
            None => {
                let c = crate::storage::atoll::notify::remote_wait();
                self.slot_waits.push(c.clone());
                // Pairs with the fence of a slot freed, or of the database failing: either it
                // finds this waiting, or this finds the slot or the failure. A completion left
                // behind is completed later, for no one.
                fence(Ordering::SeqCst);
                match self.take_slot() {
                    Some(slot) => slot,
                    None => {
                        self.refuse_if_failed()?;
                        return Ok(IOResult::IO(IOCompletions(c)));
                    }
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
            log_at: 0,
            created: Vec::new(),
            abandoned: Vec::new(),
            hold: Hold::None,
            wrote_schema: false,
            snapped: false,
            log_wait: LogWait::default(),
            gated: self.gated.load(std::sync::atomic::Ordering::Acquire),
            replaced: None,
            // SAFETY: the slot was just taken.
            changes: unsafe { self.kept[slot as usize].take() }
                .unwrap_or_else(|| changes::Changes::in_chunks(None, self.redo_chunk, None)),
        }))
    }

    /// Takes the gate shared for `txn`, which is to write. Behind another's exclusive take it
    /// registers `c` on that take's end and reports it; the caller takes again once `c` is done.
    /// A transaction whose snapshot is older than the schema timestamp fails with
    /// `SchemaConflict`.
    pub fn take_shared(&self, txn: &mut Txn, c: &Completion) -> Result<Taken> {
        self.refuse_if_failed()?;
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
        } else {
            // Registered behind a take, which a failed database's abandoned transaction never
            // lets go: the failure, read after the registration's fence, is the answer.
            self.refuse_if_failed()?;
        }
        Ok(taken)
    }

    /// Takes the gate exclusive for `txn`: its take put in the word, pending, then held once every
    /// other shared holder has left. Behind another's take, or with holders left, it registers `c`
    /// and reports them; the caller takes again once `c` is done, its take staying pending
    /// meanwhile ([`give_up`](Self::give_up)). A taker with no snapshot takes it once held; one
    /// with a snapshot older than the schema timestamp fails with `SchemaConflict`.
    pub fn take_exclusive(&self, txn: &mut Txn, c: &Completion) -> Result<Taken> {
        self.refuse_if_failed()?;
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
                    Err(taken) => {
                        self.refuse_if_failed()?;
                        return Ok(taken);
                    }
                }
            }
        };
        match self.gate.drain(txn.slot, word, c) {
            Taken::Done => {}
            Taken::Lost => {
                txn.hold = if shared { Hold::Shared } else { Hold::None };
                return Ok(Taken::Lost);
            }
            other => {
                // As for a shared take: waiting on holders a failed database never lets go.
                self.refuse_if_failed()?;
                return Ok(other);
            }
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

    /// Creates an empty tree of `class` in `txn`'s current statement: its creation is one of the
    /// transaction's changes, which its commit publishes (audit 17), and no checkpoint maps the
    /// tree until one's snapshot sees the creation (audit 18, review 8). A rollback past the
    /// statement puts the root on the plain tree `list`, keyed by its big-endian bytes. Step it
    /// until done.
    pub fn create_tree(
        &self,
        versions: &Versions,
        txn: &mut Txn,
        class: SizeClass,
        list: PageId,
    ) -> Result<IOResult<PageId>> {
        assert!(txn.command > 0, "a tree created before the first statement");
        let (pool, store) = versions.pool_and_store();
        let (slot, start) = (txn.slot, txn.start);
        let root = match Tree::create_versioned_by(pool, class, |page| {
            store.created(page, slot, start)
        })? {
            IOResult::Done(root) => root,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        txn.changed(|statement, changes| changes.create(statement, root, list));
        txn.created.push(Created {
            statement: txn.command,
            root,
            list,
        });
        Ok(IOResult::Done(root))
    }

    /// Publishes `changes`, made outside any transaction, in one [`RUN`] record (audit 17). Done
    /// once it is appended; durable with the log. Step it until done.
    pub fn publish_alone(
        &self,
        wait: &mut LogWait,
        changes: &changes::Changes,
    ) -> Result<IOResult<()>> {
        let bytes = changes.bytes();
        let mut admitted = match self.log.admit(wait, framed_len(bytes.len()))? {
            IOResult::Done(a) => a,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let header = Header {
            kind: RUN,
            flags: 0,
            txn: 0,
            prev: None,
        };
        self.log.append(&mut admitted, &header, &[bytes]);
        Ok(IOResult::Done(()))
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
        txn.changes.moved();
        if txn.isolation == Isolation::ReadCommitted && txn.command > 1 {
            txn.snapshot = self.horizon.at();
            snapshots::rise(&self.snapshots[txn.slot as usize], txn.snapshot);
            txn.seen.fill(None);
            txn.log_at = self.log.end();
        }
        Ok(())
    }

    /// Whether the version written by the transaction that started at `start` in `slot` is visible
    /// to `txn`'s current statement. Every version of `txn` itself is, its current statement's
    /// included, as Turso's VDBE expects of a cursor (DESIGN.md §11.1).
    #[inline]
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

    /// `slot`'s commit is about to take room in the log, unless it already took some.
    fn reserving(&self, slot: u16) {
        let mark = &self.reserved[slot as usize];
        if decode_reservation(mark.load(Ordering::Relaxed)) == Reservation::None {
            mark.store(RESERVING, Ordering::SeqCst);
        }
    }

    /// `slot`'s commit took room in the log at `at`: its first, unless it took some before.
    fn reserved_at(&self, slot: u16, at: GroupStart) {
        crate::race::point("txn.before_reserved_at");
        let mark = &self.reserved[slot as usize];
        match decode_reservation(mark.load(Ordering::Relaxed)) {
            Reservation::Position(_) => return,
            Reservation::Reserving => {}
            Reservation::None => panic!("a position published without its reservation"),
        }
        let position = encode_reservation(at.get());
        match mark.compare_exchange(RESERVING, position, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => {}
            Err(word) => {
                assert_eq!(word, RESERVING_WAITED, "only a cut marks a reservation");
                assert!(
                    mark.compare_exchange(word, position, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok(),
                    "the slot's publisher alone replaces its reservation"
                );
                self.wake_reservation_waiters();
            }
        }
    }

    /// A checkpoint's cut (audit 18): `(L, C)`, every commit at or below `C` published, every
    /// Commit before `L` with a timestamp at or below `C`, and every part and Commit of a commit
    /// above `C` at or after `L`. Replay from `L` of the commits above `C` then brings a state as
    /// of `C` up to the log's end. A commit at or below `C` may have parts before `L` and its
    /// Commit past it: one that cleared its mark before the scan below reached its slot, its
    /// timestamp drawn before the clock is read. Replay skips it whatever it finds of it
    /// ([`Gather`](super::replay::Gather)). Step it until done.
    ///
    /// `L` is the log's end, or where an earlier commit under way took its first room. A commit
    /// marks its slot before it takes room and replaces the mark with the position as it takes
    /// it, nothing between, so the cut meeting a mark waits for its position. `C` is the clock as
    /// it stands then, and the horizon is waited for up to it: a commit before `L` was not under
    /// way, so it drew its timestamp before; a commit drawing later was under way, its position at
    /// or past its mark, or took room after the end was read. A commit at or below `C` drew its
    /// timestamp after its last change, so every change it made is in the pages the cut finds
    /// next; one the horizon covers past `C` may still be changing pages, and is not `C`'s.
    pub fn cut(&self, op: &mut Cut) -> Result<IOResult<(Lsn, Ts)>> {
        let (l, n) = match op.at {
            Some(at) => at,
            None => {
                let mut l = self.log.end();
                for mark in self.reserved.iter() {
                    let word = mark.load(Ordering::SeqCst);
                    match decode_reservation(word) {
                        Reservation::None => {}
                        Reservation::Reserving => {
                            return Ok(IOResult::IO(IOCompletions(
                                self.wait_for_reservation(mark, word),
                            )))
                        }
                        Reservation::Position(at) => l = l.min(at),
                    }
                }
                *op.at.insert((l, self.clock.now()))
            }
        };
        if let Some(wait) = self.horizon.wait(n, &self.clock) {
            return Ok(IOResult::IO(IOCompletions(wait)));
        }
        Ok(IOResult::Done((l, n)))
    }

    /// The clock now: above every timestamp drawn so far.
    pub fn clock_now(&self) -> Ts {
        self.clock.now()
    }

    /// Whether the slot's transaction that started at `start` committed at or below `at`.
    pub fn committed_by(&self, slot: u16, start: Ts, at: Ts) -> bool {
        self.slots
            .get(slot as usize)
            .is_some_and(|log| start <= log.last_at(at))
    }

    /// The checkpoint cut at `c` is under way: what its snapshot reads keeps its room.
    pub fn hold_checkpoint(&self, c: Ts) {
        self.checkpoint_at.store(c, Ordering::SeqCst);
    }

    /// The checkpoint under way is done.
    pub fn release_checkpoint(&self) {
        self.checkpoint_at.store(Ts::MAX, Ordering::SeqCst);
    }

    /// At or below every snapshot running now or taken from now on, the horizon being at or above
    /// `at`: what a checkpoint cut at `at` judges a versioned leaf's image by ([`versioned::AsOf`]).
    pub fn oldest_snapshot(&self, at: Ts) -> Ts {
        snapshots::oldest(self.snapshots.iter().map(|s| &**s), at)
    }

    /// At or below every snapshot running now or taken from now on, and the cut of the checkpoint
    /// under way: an arena entry whose transaction committed at or below it is followed by no one
    /// (DESIGN.md §11.5).
    /// The slots transactions run in.
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    pub fn collect_cutoff(&self) -> Ts {
        self.oldest_snapshot(self.horizon.at())
            .min(self.checkpoint_at.load(Ordering::SeqCst))
    }

    /// Whether the slot's transaction that started at `start` committed at or below `at`, or
    /// ended before one that did: every snapshot at or above `at` sees what it wrote, or nothing
    /// names what it appended.
    pub fn ended_by(&self, slot: u16, start: Ts, at: Ts) -> bool {
        self.slots
            .get(slot as usize)
            .is_some_and(|log| start <= log.last_at(at))
    }

    /// Whether every snapshot taken from now on sees the version written by the slot's transaction
    /// that started at `start`, and so does the checkpoint under way: no checkpoint needs any
    /// version below it.
    pub fn seen_from_now(&self, slot: u16, start: Ts) -> bool {
        let at = self
            .horizon
            .at()
            .min(self.checkpoint_at.load(Ordering::SeqCst));
        self.slots
            .get(slot as usize)
            .is_some_and(|log| start <= log.last_at(at))
    }

    /// Commits `op`'s transaction (DESIGN.md §11.4): ends it ([`end_commit`](Self::end_commit)),
    /// then waits for the horizon to cover its commit timestamp and for the log to be durable up to
    /// its Commit record, after which it is visible to every snapshot taken. A transaction that
    /// wrote nothing appends nothing, and is done once the log is durable up to where it stood at
    /// its snapshot. Returns the commit timestamp, `None` for a transaction that wrote nothing.
    pub fn commit(&self, op: &mut Commit) -> Result<IOResult<Option<Ts>>> {
        if let IOResult::IO(io) = self.end_commit(op)? {
            return Ok(IOResult::IO(io));
        }
        if let Some(ts) = op.ts {
            if let Some(wait) = self.horizon.wait(ts, &self.clock) {
                return Ok(IOResult::IO(IOCompletions(wait)));
            }
        }
        Ok(match self.log.wait_durable(&mut op.log_wait, op.durable)? {
            IOResult::Done(()) => IOResult::Done(op.ts),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// The part of `op`'s commit that ends its transaction: a transaction that wrote has every
    /// filled chunk go out and its Commit record appended, and is published; then its gate and slot
    /// are let go. Done once [`Commit::ended`], the waits for coverage and durability left to
    /// [`commit`](Self::commit). A failure is met before the transaction ended: nothing it wrote was
    /// published, and the transaction is still in `op` ([`Commit::take_txn`]). On a failed database
    /// it is that failure ([`abandon`](Self::abandon)).
    pub fn end_commit(&self, op: &mut Commit) -> Result<IOResult<()>> {
        if let Some(txn) = op.txn.as_mut() {
            // A failed database commits nothing until it is reopened: a transaction open when it
            // failed, on keys of its own, is refused here and rolls back, as a begin is refused
            // (the auditor's #4484). A commit past this look before the failure was set finishes.
            self.refuse_if_failed()?;
            if txn.wrote() {
                // Every chunk filled goes out first, in parts; the last, in the Commit record.
                if let IOResult::IO(io) = self.spill(txn)? {
                    return Ok(IOResult::IO(io));
                }
                if op.admitted.is_none() {
                    let (_, tail) = txn.changes.tail();
                    op.admitted = Some(
                        match self
                            .log
                            .admit(&mut op.log_wait, framed_len(16 + tail.len()))?
                        {
                            IOResult::Done(a) => a,
                            IOResult::IO(io) => return Ok(IOResult::IO(io)),
                        },
                    );
                }
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
                    self.reserving(txn.slot);
                    let slot = txn.slot;
                    let (from, tail) = txn.changes.tail();
                    let (_, lsn) = self.log.append_with(
                        &mut admitted,
                        &txn.header(COMMIT),
                        &[&ts.to_le_bytes(), &from.to_le_bytes(), tail],
                        |at| self.reserved_at(slot, at),
                    );
                    self.slots[txn.slot as usize].push(txn.start, ts);
                    self.horizon.published(txn.slot, &self.clock);
                    op.ts = Some(ts);
                    lsn
                }
            };
            // The gate after the commit is published and after what a taker of it after the
            // commit must see, before the slot is freed. The horizon may not cover the commit yet
            // (`Transactions::wait_covered`): a taker's snapshot below it misses its writes, and a
            // write of a key it wrote then finds a version committed after the snapshot.
            if let (Some(ts), Some(hook)) = (op.ts, op.published.take()) {
                hook(ts);
            }
            let schema = op.ts.filter(|_| txn.wrote_schema);
            self.release_hold(&mut txn, schema);
            self.keep(txn.slot, txn.changes);
            self.release(txn.slot);
        }
        Ok(IOResult::Done(()))
    }

    /// `None` once every snapshot taken from now on sees the commit at `ts`, the horizon covering
    /// it; until then, the completion to wait on, to look again once it is done. A published
    /// commit's horizon may lag it while a commit below it on another slot is still drawing.
    pub fn wait_covered(&self, ts: Ts) -> Option<Completion> {
        self.horizon.wait(ts, &self.clock)
    }

    /// Holds the horizon where it stands, or lets it go, for a test ([`Horizon::hold`]).
    #[cfg(all(test, not(atoll_loom)))]
    pub(crate) fn hold_horizon(&self, hold: bool) {
        if hold {
            self.horizon.hold();
        } else {
            self.horizon.let_go(&self.clock);
        }
    }

    /// Ends `txn`, which wrote nothing, without committing it. One that wrote is rolled back
    /// ([`rollback`](Self::rollback)).
    pub fn abort(&self, mut txn: Txn) {
        assert!(
            !txn.wrote(),
            "aborting a transaction that wrote: roll it back"
        );
        self.release_hold(&mut txn, None);
        self.keep(txn.slot, txn.changes);
        self.release(txn.slot);
    }

    /// Rolls `op`'s transaction back whole (DESIGN.md §11.5): every change it made put back, and its
    /// slot freed with no commit pair. It logs nothing: the log holds nothing of a transaction
    /// before its commit (audit 17). Step it until done.
    pub fn rollback(&self, versions: &Versions, op: &mut Abort) -> Result<IOResult<()>> {
        let txn = op
            .txn
            .as_mut()
            .expect("a rollback stepped after it was done");
        if let IOResult::IO(io) = rollback::undo(self, versions, txn, &mut op.undo)? {
            return Ok(IOResult::IO(io));
        }
        let mut txn = op.txn.take().expect("a transaction");
        op.abandoned = std::mem::take(&mut txn.abandoned);
        // Every version it wrote is gone: the slot's next transaction may commit, and writers
        // behind the gate may go on.
        self.release_hold(&mut txn, None);
        self.keep(txn.slot, txn.changes);
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
        txn.changes.back_to(op.to());
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
            if self.abandoned[slot as usize].load(Ordering::Acquire) == start {
                Writer::Abandoned
            } else {
                Writer::Open
            }
        } else {
            Writer::Gone
        }
    }

    /// Why the database failed, once a transaction was left where it stood
    /// ([`abandon`](Self::abandon)); `None` while it has not.
    pub fn failure(&self) -> Option<LimboError> {
        self.failure.get()
    }

    /// The failure a judgment of an abandoned transaction's version returns: set before the
    /// abandoned start it read ([`abandon`](Self::abandon)).
    pub fn failed(&self) -> LimboError {
        self.failure.get().unwrap_or_else(|| {
            LimboError::InternalError("an abandoned transaction with no failure".into())
        })
    }

    fn refuse_if_failed(&self) -> Result<()> {
        match self.failure.get() {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }

    /// Leaves `txn`, whose rollback could not finish (`failure`: an undo that could not read back
    /// a part its log never made durable, or could not put a key back), where it stands
    /// (DESIGN.md §11.5): its slot stays seated, its hold of the gate stays, and so do the versions
    /// it wrote, which no later transaction of the slot can take for its own. The database has
    /// failed from then on, for good until it is reopened, whose recovery has no commit of the
    /// transaction to replay: every begin, take of the gate and judgment of a version the
    /// transaction wrote returns the failure, and whoever waits on a slot, the gate or one of its
    /// versions now is woken to find it.
    pub fn abandon(&self, txn: Txn, failure: LimboError) {
        let slot = txn.slot as usize;
        // The failure first: whoever reads the abandoned start reads it ([`failed`](Self::failed)).
        self.failure.set(failure);
        self.abandoned[slot].store(txn.start, Ordering::SeqCst);
        // Pairs with the fence after each waiter's registration: either the wake finds it, or it
        // reads the failure or the abandoned start.
        fence(Ordering::SeqCst);
        for c in self.slot_waits.take_all() {
            c.complete(0);
        }
        self.gate.wake_all();
        self.ends[slot].wake();
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

    /// Registers before marking, so publication either finishes this wait or defeats the mark.
    fn wait_for_reservation(&self, mark: &AtomicU64, word: u64) -> Completion {
        let domain = self
            .reservation_completions
            .get_or_init(|| CompletionDomain::new(Arc::new(Epochs::new())));
        let wait = self
            .reservation_wait
            .current_with(domain, crate::storage::atoll::notify::remote_wait);
        crate::race::point("txn.reservation_wait");
        if mark
            .compare_exchange(
                word,
                word | RESERVATION_WAITED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            // The reservation changed after the look: no wait on the old signal is needed.
            return Completion::new_yield();
        }
        crate::race::point("txn.reservation_marked");
        wait
    }

    /// Publication took a marked reservation; every cut registered on this signal looks again.
    fn wake_reservation_waiters(&self) {
        let domain = self
            .reservation_completions
            .get()
            .expect("a marked reservation has its completion domain");
        if let Some(wait) = self.reservation_wait.take(domain) {
            crate::storage::atoll::notify::finish(&wait, Ok(0));
        }
    }

    fn release(&self, slot: u16) {
        // Never reached while reserving: taking log space through publishing its position is
        // infallible and does not yield; a panic there aborts with Admitted still alive.
        self.reserved[slot as usize].store(0, Ordering::SeqCst);
        snapshots::vacate(&self.snapshots[slot as usize]);
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

/// The same representation is used by the cut, log retention and the slot's publisher.
#[inline]
fn decode_reservation(word: u64) -> Reservation {
    match word {
        0 => Reservation::None,
        RESERVING | RESERVING_WAITED => Reservation::Reserving,
        position => {
            crate::turso_assert!(position < RESERVATION_WAITED, "invalid reservation word");
            Reservation::Position(position - 1)
        }
    }
}

/// Leave the two flag bits clear; check before adding so an overflow cannot become `None`.
#[inline]
fn encode_reservation(at: Lsn) -> u64 {
    crate::turso_assert!(
        at < RESERVATION_WAITED - 1,
        "reservation position exhausted"
    );
    at + 1
}

pub mod arena;
pub mod changes;
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
