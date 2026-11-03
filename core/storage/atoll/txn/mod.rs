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

use self::sync::{fence, AtomicU64, Ordering};
use super::log::{framed_len, Admitted, GroupStart, Header, Kind, Log, Lsn, TxnTail, Writers};
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

mod horizon;
use crate::types::{IOCompletions, IOResult};
use crate::{LimboError, Result};

/// A timestamp of the transaction clock. 0 is before every transaction.
pub type Ts = u64;

/// A commit's record: its timestamp.
pub const COMMIT: Kind = Kind::new(16).unwrap();

/// The record of a transaction that ended without committing, with no undo left: every change it
/// made put back. Empty.
pub const END: Kind = Kind::new(19).unwrap();

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
    command: u16,
    /// Per slot, the start of its last transaction committed before the snapshot, once looked up.
    seen: Vec<Option<Ts>>,
    /// Where the transaction's last group starts; `None` while it has written nothing.
    tail: Option<GroupStart>,
    /// Where the log stood at the snapshot: what a read-only commit waits to be durable.
    log_at: Lsn,
}

impl Txn {
    pub fn slot(&self) -> u16 {
        self.slot
    }

    /// The start timestamp, which also names the transaction.
    pub fn start(&self) -> Ts {
        self.start
    }

    pub fn command(&self) -> u16 {
        self.command
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
}

/// A whole rollback in progress: step it with [`Transactions::rollback`].
pub struct Abort {
    txn: Option<Txn>,
    undo: Rollback,
    admitted: Option<Admitted>,
}

impl Abort {
    pub fn new(txn: Txn) -> Self {
        Self {
            txn: Some(txn),
            undo: Rollback::after(0),
            admitted: None,
        }
    }
}

/// A commit in progress: step it with [`Transactions::commit`].
pub struct Commit {
    txn: Option<Txn>,
    admitted: Option<Admitted>,
    ts: Option<Ts>,
    durable: Lsn,
}

impl Commit {
    pub fn new(txn: Txn) -> Self {
        Self {
            txn: Some(txn),
            admitted: None,
            ts: None,
            durable: 0,
        }
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
            clock,
            log,
        }
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
        let (start, lsn) = self.log.append_with(admitted, &txn.header(kind), payload, |at| {
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

    /// Begins a transaction once a slot is free.
    pub fn begin(&self, isolation: Isolation) -> Result<IOResult<Txn>> {
        let slot = match self.take_slot() {
            Some(slot) => slot,
            None => {
                let c = Completion::new_wait();
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
        self.running[slot as usize].store(start, Ordering::Release);
        // The snapshot before the log's end: every commit it covers is in the log by then.
        let snapshot = self.horizon.at();
        Ok(IOResult::Done(Txn {
            slot,
            start,
            isolation,
            snapshot,
            command: 0,
            seen: vec![None; self.slots.len()],
            tail: None,
            log_at: self.log.end(),
        }))
    }

    /// Starts `txn`'s next statement. It sees what the transaction's earlier statements wrote, and
    /// under read committed, a snapshot taken now.
    pub fn statement(&self, txn: &mut Txn) -> Result<()> {
        txn.command = txn.command.checked_add(1).ok_or_else(|| {
            LimboError::InternalError(format!(
                "a transaction of more than {} statements",
                u16::MAX
            ))
        })?;
        if txn.isolation == Isolation::ReadCommitted && txn.command > 1 {
            txn.snapshot = self.horizon.at();
            txn.seen.fill(None);
            txn.log_at = self.log.end();
        }
        Ok(())
    }

    /// Whether the version written by the transaction that started at `start` in `slot`, in its
    /// statement `command`, is visible to `txn`'s current statement.
    pub fn visible(&self, txn: &mut Txn, slot: u16, start: Ts, command: u16) -> bool {
        if slot == txn.slot && start == txn.start {
            return command < txn.command;
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
            let txn = op.txn.take().expect("a transaction");
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
    pub fn abort(&self, txn: Txn) {
        assert!(!txn.wrote(), "aborting a transaction that wrote: roll it back");
        self.release(txn.slot);
    }

    /// Rolls `op`'s transaction back whole (DESIGN.md §11.5): every change it made put back, its
    /// End appended, and its slot freed with no commit pair. Nothing waits for the log to be
    /// durable: a rollback a crash loses is undone again by recovery. Step it until done.
    pub fn rollback(&self, versions: &Versions, op: &mut Abort) -> Result<IOResult<()>> {
        let txn = op.txn.as_mut().expect("a rollback stepped after it was done");
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
        let txn = op.txn.take().expect("a transaction");
        // Every version it wrote is gone: the slot's next transaction may commit.
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
        assert!(op.to() <= txn.command, "rolling back to a statement not yet run");
        if let IOResult::IO(io) = rollback::undo(self, versions, txn, op)? {
            return Ok(IOResult::IO(io));
        }
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

    fn release(&self, slot: u16) {
        self.running[slot as usize].store(0, Ordering::Release);
        self.free[slot as usize / 64].fetch_or(1 << (slot % 64), Ordering::AcqRel);
        // Pairs with the fence of a waiter: either this finds it, or it finds the slot.
        fence(Ordering::SeqCst);
        for c in self.slot_waits.take_all() {
            c.complete(0);
        }
    }
}

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
