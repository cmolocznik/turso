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
//!   the slot's commit log under the slot's lock, which a snapshot reading that slot's log takes
//!   too. A snapshot taken after the commit timestamp was drawn therefore sees the pair, and one
//!   taken before reads the transaction as not committed, which for it is so.
//! - **A transaction's records** carry its start timestamp as the transaction's id and chain to its
//!   previous record, so its records are found from its last one.

use super::log::{framed_len, Admitted, Header, Kind, Log, Lsn};
use crate::io::Completion;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{LimboError, Result};
use std::collections::VecDeque;

/// A timestamp of the transaction clock. 0 is before every transaction.
pub type Ts = u64;

/// A commit's record: its timestamp.
pub const COMMIT: Kind = Kind::new(16).unwrap();

pub fn owns(kind: Kind) -> bool {
    kind == COMMIT
}

/// The commit timestamp a Commit record's payload carries.
pub fn commit_ts(payload: &[u8]) -> Option<Ts> {
    payload.try_into().ok().map(u64::from_le_bytes)
}

#[derive(Debug)]
pub struct Clock(AtomicU64);

impl Clock {
    /// A clock whose timestamps are all above `last`.
    pub fn after(last: Ts) -> Self {
        Self(AtomicU64::new(last))
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

struct CommitLog {
    /// Every transaction of the slot that started at or before this committed before any
    /// snapshot taken now.
    base: Ts,
    /// The (start, commit) pairs of the slot's later committed transactions, in order.
    entries: VecDeque<(Ts, Ts)>,
}

impl CommitLog {
    /// Whether the slot's transaction that started at `start` committed.
    fn committed(&self, start: Ts) -> bool {
        start <= self.base
            || self
                .entries
                .binary_search_by_key(&start, |&(s, _)| s)
                .is_ok()
    }

    /// The start of the slot's last transaction committed before `at`.
    fn last_before(&self, at: Ts) -> Ts {
        match self.entries.partition_point(|&(_, commit)| commit < at) {
            0 => self.base,
            n => self.entries[n - 1].0,
        }
    }
}

pub struct Transactions {
    clock: Arc<Clock>,
    log: Arc<Log>,
    slots: Box<[Mutex<CommitLog>]>,
    /// Per slot, the start of the transaction it runs; 0 for none.
    running: Box<[AtomicU64]>,
    free: Mutex<Vec<u16>>,
    /// Done when a slot is next freed, if anyone waits for one.
    freed: Mutex<Option<Completion>>,
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
    /// The transaction's last record; 0 while it has written nothing.
    last: Lsn,
    /// Where the log stood at the snapshot: what a read-only commit waits to be durable.
    log_at: Lsn,
    /// Entries the transaction put in the list of retired pages.
    retired: u32,
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
            prev: self.last,
        }
    }

    /// Notes the transaction's record appended up to `lsn`.
    pub fn logged(&mut self, lsn: Lsn) {
        debug_assert!(lsn > self.last);
        self.last = lsn;
    }

    pub fn wrote(&self) -> bool {
        self.last != 0
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
    /// `config.slots` slots over `clock`, every version already in the database committed.
    pub fn new(config: &TxnConfig, clock: Arc<Clock>, log: Arc<Log>) -> Self {
        assert!(
            (1..=u16::MAX as usize + 1).contains(&config.slots),
            "{} transaction slots",
            config.slots
        );
        let base = clock.now();
        Self {
            slots: (0..config.slots)
                .map(|_| {
                    Mutex::new(CommitLog {
                        base,
                        entries: VecDeque::new(),
                    })
                })
                .collect(),
            running: (0..config.slots).map(|_| AtomicU64::new(0)).collect(),
            free: Mutex::new((0..config.slots).rev().map(|s| s as u16).collect()),
            freed: Mutex::new(None),
            clock,
            log,
        }
    }

    pub fn clock(&self) -> &Arc<Clock> {
        &self.clock
    }

    /// Begins a transaction once a slot is free.
    pub fn begin(&self, isolation: Isolation) -> Result<IOResult<Txn>> {
        // Waiting starts before the look, so a slot freed in between is not missed.
        let wait = self
            .freed
            .lock()
            .get_or_insert_with(Completion::new_wait)
            .clone();
        let Some(slot) = self.free.lock().pop() else {
            return Ok(IOResult::IO(IOCompletions(wait)));
        };
        let start = self.clock.next();
        self.running[slot as usize].store(start, Ordering::Release);
        Ok(IOResult::Done(Txn {
            slot,
            start,
            isolation,
            snapshot: start,
            command: 0,
            seen: vec![None; self.slots.len()],
            last: 0,
            log_at: self.log.end(),
            retired: 0,
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
            txn.snapshot = self.clock.next();
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
            .get_or_insert_with(|| self.slots[slot as usize].lock().last_before(snapshot));
        start <= last
    }

    /// Commits `op`'s transaction (DESIGN.md §11.4). It is visible to every snapshot taken after
    /// its commit timestamp, and done once the log is durable up to its Commit record. A
    /// transaction that wrote nothing appends nothing, and is done once the log is durable up to
    /// where it stood at its snapshot. Returns the commit timestamp, `None` for a transaction that
    /// wrote nothing.
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
                    let mut commits = self.slots[txn.slot as usize].lock();
                    let ts = self.clock.next();
                    let lsn =
                        self.log
                            .append(&mut admitted, &txn.header(COMMIT), &[&ts.to_le_bytes()]);
                    commits.entries.push_back((txn.start, ts));
                    op.ts = Some(ts);
                    lsn
                }
            };
            self.release(txn.slot);
        }
        Ok(match self.log.wait_durable(op.durable)? {
            IOResult::Done(()) => IOResult::Done(op.ts),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// Ends `txn` without committing it. It must have written nothing: rolling back what a
    /// transaction wrote comes with undo.
    pub fn abort(&self, txn: Txn) {
        assert!(
            !txn.wrote(),
            "aborting a transaction that wrote: rollback comes with undo"
        );
        self.release(txn.slot);
    }

    /// Where the transaction that started at `start` in `slot` stands, for a version it wrote.
    pub fn writer(&self, slot: u16, start: Ts) -> Writer {
        // The slot first: a committing transaction enters its pair before it leaves the slot, so a
        // transaction gone from the slot by this read has its pair in the log by the next.
        let running = self.running[slot as usize].load(Ordering::Acquire);
        if self.slots[slot as usize].lock().committed(start) {
            Writer::Committed
        } else if running == start {
            Writer::Open
        } else {
            Writer::Gone
        }
    }

    fn release(&self, slot: u16) {
        self.running[slot as usize].store(0, Ordering::Release);
        self.free.lock().push(slot);
        let waiting = self.freed.lock().take();
        if let Some(c) = waiting {
            c.complete(0);
        }
    }
}

pub mod retired;
pub mod versioned;

#[cfg(test)]
mod tests;
