//! AtollCursor's reads (DESIGN.md §17): a Turso cursor over one SQL tree, as its transaction's
//! current statement sees it.
//!
//! - **Keys.** A rowid table's key is its rowid ([`key::rowid`]) and its value the row's record. An
//!   index's key is its encoded columns, then its rowid where it has one, and its value the index
//!   record; a UNIQUE index's key leaves the rowid out when no column is NULL. A WITHOUT ROWID
//!   table's key is its encoded key columns, and its value the record.
//! - **Seeks.** A seek into a table seeks its rowid's key. One into an index encodes the columns the
//!   seek key holds: each column's encoding is self-delimiting, so that is a prefix `p` of every
//!   key it matches, and GE p seeks `Ge p`, GT p `Ge succ(p)`, LE p `Lt succ(p)` and LT p `Lt p`,
//!   `succ(p)` bounding the keys that start with p ([`key::successor`]). A seek key holding the
//!   rowid of a UNIQUE index whose keys leave it out seeks on the columns, then compares the rowid
//!   of the one entry they name. An eq-only seek is found only on an entry the key matches.
//! - **Encoding versions.** A tree whose recorded encoding is not this build's is read by scans
//!   alone: a seek into it is refused, naming REINDEX, and so is a write into it.
//! - **Writes.** An insert puts the row, or the index entry, under its key; one into a UNIQUE
//!   index whose key leaves the rowid out is refused over a visible entry of another row. A key
//!   longer than the tree holds fails its statement (R7). A write behind another transaction's
//!   open version, or behind one committed after the snapshot, comes back as such, and stepping
//!   the same write again goes on with it. The statement that created a tree writes it plainly
//!   ([`VersionedTree::plain_put`]).

use super::catalog::{Entry, Layout};
use super::key;
use super::AtollDb;
use crate::error::{ConstraintKind, ConstraintViolation};
use crate::numeric::Numeric;
use crate::storage::atoll::btree::{Cursor, Kept, Mode, Near, Outcome, Put, Remove, Seek};
use crate::storage::atoll::buffer::Owner;
use crate::storage::atoll::locks::{Mutex, MutexGuard};
use crate::storage::atoll::txn::versioned::{
    Guard, GuardOp, Guarded, VersionedCursor, VersionedTree, Write, WriteOp, HEADER,
};
use crate::storage::atoll::txn::Txn;
use crate::storage::btree::BTreeKey;
use crate::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use crate::sync::Arc;
use crate::types::{
    IOCompletions, IOResult, ImmutableRecord, IndexInfo, RecordBuf, SeekKey, SeekOp, SeekResult,
    ValueBlob, ValueRef,
};
use crate::{LimboError, Result};
use std::borrow::Cow;
use std::cmp::Ordering;

/// A row's bytes as a record's payload: the same allocation where the crate allocates with the
/// global allocator.
#[cfg(not(nightly))]
fn payload_of(bytes: Vec<u8>) -> ValueBlob {
    bytes
}

#[cfg(nightly)]
fn payload_of(bytes: Vec<u8>) -> ValueBlob {
    let mut payload = crate::alloc::vec![];
    payload.extend_from_slice(&bytes);
    payload
}

/// A record's payload as a row's bytes again: [`payload_of`]'s inverse.
#[cfg(not(nightly))]
fn bytes_of(payload: ValueBlob) -> Vec<u8> {
    payload
}

#[cfg(nightly)]
fn bytes_of(payload: ValueBlob) -> Vec<u8> {
    payload.to_vec()
}

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// The transaction a connection's cursors read in, none between transactions, the statement
/// rollback it owes, if any (R19), and the connection's identity, which the memory-shortage
/// tenures of its operations carry (DESIGN.md 9.1.3, *Revoking an inactive tenure*). The
/// transaction is reached only through [`enter`](Self::enter), [`enter_now`](Self::enter_now),
/// [`observe`](Self::observe) and [`take`](Self::take); a rollback it owes is finished by
/// [`settle`](Self::settle), at the top of the connection's next call (ASYNC1 D2e (iii-b)).
pub struct TxnSlot {
    slot: Mutex<Slot>,
    /// Whether `slot.owed` holds a rollback: written under the lock with it, read without the lock
    /// by a statement step's entry and by a kept probe.
    owing: AtomicBool,
    conn: Arc<crate::storage::atoll::buffer::Conn>,
}

/// The transaction and the statement rollback it owes, under one lock.
struct Slot {
    txn: Option<Txn>,
    /// The statement rollback the transaction owes (R19): recorded where a statement's rollback
    /// was not run to its end, finished at the top of the connection's next call.
    owed: Option<Box<OwedRollback>>,
}

impl TxnSlot {
    /// A connection's slot, holding `txn`.
    pub fn new(txn: Option<Txn>) -> TxnCell {
        Arc::new(Self {
            slot: Mutex::new(Slot { txn, owed: None }),
            owing: AtomicBool::new(false),
            conn: crate::storage::atoll::buffer::Conn::new(),
        })
    }

    /// The connection's identity.
    pub(in crate::storage::atoll) fn conn(&self) -> &Arc<crate::storage::atoll::buffer::Conn> {
        &self.conn
    }

    /// The transaction, for an operation that reads or changes its data. Refused while it owes a
    /// statement rollback, which the top of the connection's call finishes first
    /// ([`settle`](Self::settle)); one that failed refuses with its failure.
    #[inline]
    pub(in crate::storage::atoll) fn enter(&self) -> std::result::Result<Entered<'_>, Refused<'_>> {
        let slot = self.slot.lock();
        if slot.owed.is_some() {
            return Err(Refused(slot));
        }
        Ok(Entered(slot))
    }

    /// [`enter`](Self::enter) for a synchronous form at the top of a call: a rollback owed is
    /// finished first, driven to its end here outside a cooperative frame. In one, where such a
    /// form never meets one, a rollback owed is an error naming `site`, never a park.
    #[inline]
    pub(in crate::storage::atoll) fn enter_now(&self, site: &'static str) -> Result<Entered<'_>> {
        if self.owes() {
            self.settle_now(site)?;
        }
        Ok(self.enter()?)
    }

    /// [`settle`](Self::settle) for a synchronous form at the top of a call, as
    /// [`enter_now`](Self::enter_now) settles.
    #[inline]
    pub(in crate::storage::atoll) fn settle_now(&self, site: &'static str) -> Result<()> {
        match self.settle()? {
            IOResult::Done(()) => Ok(()),
            IOResult::IO(_) => Err(LimboError::InternalError(format!(
                "{site}: a synchronous form met a statement rollback owed in a cooperative frame"
            ))),
        }
    }

    /// The transaction as it stands, a statement rollback maybe owed in it: for what reads only
    /// what that rollback leaves as it is, or changes what it does not touch.
    pub(in crate::storage::atoll) fn observe(&self) -> Observed<'_> {
        Observed {
            slot: self.slot.lock(),
            owing: &self.owing,
        }
    }

    /// Takes the transaction out to end it, with the statement rollback it owes.
    pub(in crate::storage::atoll) fn take(&self) -> Option<(Txn, Option<Box<OwedRollback>>)> {
        let mut slot = self.slot.lock();
        let txn = slot.txn.take()?;
        let owed = slot.owed.take();
        if owed.is_some() {
            self.owing.store(false, AtomicOrdering::Release);
        }
        Some((txn, owed))
    }

    /// The slot as it stands, for a test that sets up or ends the transaction directly.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn lock_for_test(&self) -> Observed<'_> {
        self.observe()
    }

    /// [`lock_for_test`](Self::lock_for_test) if the slot is free.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn try_lock_for_test(&self) -> Option<Observed<'_>> {
        Some(Observed {
            slot: self.slot.try_lock()?,
            owing: &self.owing,
        })
    }

    /// Whether the transaction owes a statement rollback: one load.
    #[inline]
    pub(in crate::storage::atoll) fn owes(&self) -> bool {
        self.owing.load(AtomicOrdering::Acquire)
    }

    /// Finishes the statement rollback the transaction owes, under its own top, at the top of a
    /// call of the connection, where none of its operations runs. In a cooperative frame, one
    /// step, returning the IO it waits for; outside one, driven to its end here. Refused inside an
    /// operation: its shortage would wait for its own caller (ASYNC1 D2e (iii-b)).
    #[inline]
    pub(in crate::storage::atoll) fn settle(&self) -> Result<IOResult<()>> {
        if !self.owes() {
            return Ok(IOResult::Done(()));
        }
        self.settle_owed()
    }

    /// [`settle`](Self::settle) with a statement rollback owed.
    #[cold]
    #[inline(never)]
    fn settle_owed(&self) -> Result<IOResult<()>> {
        if crate::storage::atoll::buffer::owner_installed() {
            return Err(LimboError::InternalError(
                "a statement rollback owed would be finished inside an operation of its connection"
                    .to_string(),
            ));
        }
        let inside = crate::storage::atoll::cooperative::inside();
        loop {
            let (out, io) = {
                let mut slot = self.slot.lock();
                let Slot { txn, owed } = &mut *slot;
                let Some(o) = owed.as_deref_mut() else {
                    return Ok(IOResult::Done(()));
                };
                let txn = txn
                    .as_mut()
                    .expect("a statement rollback owed with no transaction");
                let io = o.io();
                let out = o.step(txn);
                if let Ok(IOResult::Done(())) = out {
                    *owed = None;
                    self.owing.store(false, AtomicOrdering::Release);
                }
                (out, io)
            };
            match out {
                Ok(IOResult::IO(IOCompletions(c))) if !inside => {
                    // Waited for here, as `drive_over` waits; the rollback judges the completion
                    // at its next step.
                    if c.finished() {
                        crate::storage::atoll::park::yield_now();
                    } else if let Some(io) = io {
                        crate::storage::atoll::park::wait(io.as_ref(), &c);
                    }
                }
                out => return out,
            }
        }
    }
}

/// The transaction once entered: nothing owed in it, and nothing can be recorded while it is held.
pub(in crate::storage::atoll) struct Entered<'a>(MutexGuard<'a, Slot>);

/// An entry refused while the transaction owes a statement rollback, still holding the slot: its
/// error is made under that lock as it reaches a caller that returns it.
pub(in crate::storage::atoll) struct Refused<'a>(MutexGuard<'a, Slot>);

impl From<Refused<'_>> for LimboError {
    #[cold]
    #[inline(never)]
    fn from(refused: Refused<'_>) -> Self {
        refused
            .0
            .owed
            .as_deref()
            .expect("an entry refused under the guard that found a statement rollback owed")
            .refusal()
    }
}

impl From<Refused<'_>> for Box<LimboError> {
    #[cold]
    #[inline(never)]
    fn from(refused: Refused<'_>) -> Self {
        Box::new(refused.into())
    }
}

impl std::ops::Deref for Entered<'_> {
    type Target = Option<Txn>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0.txn
    }
}

impl std::ops::DerefMut for Entered<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.txn
    }
}

/// The transaction as it stands, a statement rollback maybe owed in it ([`TxnSlot::observe`]).
pub(in crate::storage::atoll) struct Observed<'a> {
    slot: MutexGuard<'a, Slot>,
    owing: &'a AtomicBool,
}

impl std::ops::Deref for Observed<'_> {
    type Target = Option<Txn>;

    fn deref(&self) -> &Self::Target {
        &self.slot.txn
    }
}

impl std::ops::DerefMut for Observed<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slot.txn
    }
}

/// What recording a statement rollback did with the one owed before ([`Observed::owe`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::atoll) enum Owing {
    /// None was owed: this one is.
    Fresh,
    /// One owed to the same or an earlier statement stays, its progress with it.
    Kept,
    /// One owed to a later statement was dropped, and this one, to the earlier, made.
    Restarted,
    /// One whose rollback failed stays, failed.
    Failed,
}

impl Observed<'_> {
    /// Records in the open transaction a rollback to after statement `n`, owed until the top of
    /// the connection's next call finishes it; with one owed already, the earlier point is kept
    /// (DESIGN.md: one rollback owed per transaction at most). `None` with no transaction open.
    pub(in crate::storage::atoll) fn owe(
        &mut self,
        n: crate::storage::atoll::txn::Statement,
        db: &Arc<AtollDb>,
        conn: &Arc<crate::storage::atoll::buffer::Conn>,
    ) -> Option<Owing> {
        let txn = self.slot.txn.as_ref()?;
        crate::turso_assert!(
            n <= txn.command(),
            "a statement rollback owed to a statement not yet run"
        );
        let owing = match self.slot.owed.as_deref() {
            None => Owing::Fresh,
            Some(owed) if owed.failed() => return Some(Owing::Failed),
            Some(owed) if owed.to().is_some_and(|to| to <= n) => return Some(Owing::Kept),
            Some(_) => Owing::Restarted,
        };
        self.slot.owed = Some(Box::new(OwedRollback::new(n, db.clone(), conn.clone())));
        self.owing.store(true, AtomicOrdering::Release);
        Some(owing)
    }

    /// The statement rollback the transaction owes, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn owed_for_test(&self) -> Option<&OwedRollback> {
        self.slot.owed.as_deref()
    }

    /// Whether the rollback owed failed, and the transaction's identity: what a statement that met
    /// it marks ([`OwedRollback::failed`]).
    pub(in crate::storage::atoll) fn failed_in(&self) -> Option<(u16, u64)> {
        let txn = self.slot.txn.as_ref()?;
        self.slot
            .owed
            .as_deref()
            .filter(|o| o.failed())
            .map(|_| (txn.slot(), txn.start()))
    }
}

/// A statement rollback its transaction owes (R19, ASYNC1 D2e (iii-b)): recorded where a
/// statement's rollback was not run to its end, finished at the top of the connection's next call,
/// under its own top, which keeps its tenure and pages across its returns. It owns the completion
/// its last step returned and judges it before it steps again, as a driver does: a statement's
/// copy of that completion only wakes the statement.
pub(in crate::storage::atoll) struct OwedRollback {
    state: Owed,
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    serial: u64,
    /// Dropped last (DESIGN.md 22.11).
    _frame: crate::storage::atoll::unwind::FrameEnd,
}

enum Owed {
    /// Under way: the rollback, its top, and the completion its last step returned until a step
    /// has looked at it.
    Rolling {
        rollback: crate::storage::atoll::txn::rollback::Rollback,
        owner: Owner,
        waiting: Option<crate::io::Completion>,
        db: Arc<AtollDb>,
    },
    /// Its rollback failed: the transaction refuses every operation until it ends.
    Failed(LimboError),
}

#[cfg(test)]
crate::thread::thread_local! {
    /// The steps owed statement rollbacks took on this thread: each a call of `rollback_to`.
    pub(crate) static OWED_STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The owed statement rollbacks that failed on this thread.
    pub(crate) static OWED_FAILED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Whether the next step of an owed statement rollback on this thread fails, as injected by a
    /// test: the one synthetic failure, of a kind no real fault makes `undo` return.
    pub(crate) static OWED_FAIL_NEXT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The serials owed statement rollbacks are made with, for tests to tell them apart.
#[cfg(all(test, not(any(shuttle, atoll_loom))))]
static OWED_SERIALS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl OwedRollback {
    fn new(
        n: crate::storage::atoll::txn::Statement,
        db: Arc<AtollDb>,
        conn: Arc<crate::storage::atoll::buffer::Conn>,
    ) -> Self {
        #[allow(unused_mut)]
        let mut owner = Owner::of(conn);
        #[cfg(test)]
        owner.set_serves_owed(true);
        Self {
            state: Owed::Rolling {
                rollback: crate::storage::atoll::txn::rollback::Rollback::after(n),
                owner,
                waiting: None,
                db,
            },
            #[cfg(all(test, not(any(shuttle, atoll_loom))))]
            serial: OWED_SERIALS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1,
            _frame: crate::storage::atoll::unwind::FrameEnd::default(),
        }
    }

    /// The last statement it keeps; none once it failed.
    fn to(&self) -> Option<crate::storage::atoll::txn::Statement> {
        match &self.state {
            Owed::Rolling { rollback, .. } => Some(rollback.to()),
            Owed::Failed(_) => None,
        }
    }

    /// Whether its rollback failed.
    pub(in crate::storage::atoll) fn failed(&self) -> bool {
        matches!(self.state, Owed::Failed(_))
    }

    /// The error an operation that meets it is refused with.
    #[cold]
    #[inline(never)]
    fn refusal(&self) -> LimboError {
        match &self.state {
            Owed::Failed(e) => e.clone(),
            Owed::Rolling { .. } => LimboError::InternalError(
                "an operation met a statement rollback its call did not finish first".to_string(),
            ),
        }
    }

    fn io(&self) -> Option<Arc<dyn crate::io::IO>> {
        match &self.state {
            Owed::Rolling { db, .. } => Some(db.io().clone()),
            Owed::Failed(_) => None,
        }
    }

    /// One step: its last completion judged first (unfinished, the same wait; failed, the rollback
    /// over and never stepped again; finished, cleared), then one call of the rollback over its own
    /// top.
    fn step(&mut self, txn: &mut Txn) -> Result<IOResult<()>> {
        let Owed::Rolling {
            rollback,
            owner,
            waiting,
            db,
        } = &mut self.state
        else {
            return Err(self.refusal());
        };
        let judged = match waiting.as_ref() {
            Some(c) if !c.finished() => return Ok(IOResult::IO(IOCompletions(c.clone()))),
            Some(c) => c.get_error().map(LimboError::CompletionError),
            None => None,
        };
        *waiting = None;
        #[cfg(test)]
        let judged = judged.or_else(|| {
            OWED_FAIL_NEXT
                .with(|f| f.replace(false))
                .then(|| LimboError::InternalError("injected statement rollback failure".into()))
        });
        let out = match judged {
            Some(e) => Err(e),
            None => {
                #[cfg(test)]
                OWED_STEPS.with(|n| n.set(n.get() + 1));
                owner.call(|| {
                    db.atoll()
                        .transactions()
                        .rollback_to(db.versions(), txn, rollback)
                })
            }
        };
        match out {
            Ok(IOResult::IO(IOCompletions(c))) => {
                *waiting = Some(c.clone());
                Ok(IOResult::IO(IOCompletions(c)))
            }
            Ok(IOResult::Done(())) => Ok(IOResult::Done(())),
            Err(e) => {
                #[cfg(test)]
                OWED_FAILED.with(|n| n.set(n.get() + 1));
                self.state = Owed::Failed(e.clone());
                Err(e)
            }
        }
    }

    /// Its rollback, top and wait, for a commit's ending to finish it as its own first step; its
    /// failure, if it failed.
    pub(in crate::storage::atoll) fn into_parts(
        mut self,
    ) -> std::result::Result<
        (
            crate::storage::atoll::txn::rollback::Rollback,
            Owner,
            Option<crate::io::Completion>,
        ),
        LimboError,
    > {
        let handed = LimboError::InternalError("a statement rollback handed over".to_string());
        match std::mem::replace(&mut self.state, Owed::Failed(handed)) {
            Owed::Rolling {
                rollback,
                owner,
                waiting,
                ..
            } => {
                #[cfg(test)]
                let owner = {
                    let mut owner = owner;
                    owner.set_serves_owed(false);
                    owner
                };
                Ok((rollback, owner, waiting))
            }
            Owed::Failed(e) => Err(e),
        }
    }

    /// Its serial, for a test to see it kept across returns or made anew.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn serial_for_test(&self) -> u64 {
        self.serial
    }

    /// Whether it waits on a completion that has not finished, and whether its top holds a
    /// tenure, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn waiting_for_test(&self) -> Option<(bool, bool, bool)> {
        match &self.state {
            Owed::Rolling { waiting, owner, .. } => Some((
                waiting.as_ref().is_some_and(|c| !c.finished()),
                waiting.as_ref().is_some_and(|c| c.get_error().is_some()),
                owner.queued(),
            )),
            Owed::Failed(_) => None,
        }
    }

    /// The keys its rollback put back so far, for a test to see it part-way.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn put_back_for_test(&self) -> u64 {
        match &self.state {
            Owed::Rolling { rollback, .. } => rollback.put_back(),
            Owed::Failed(_) => 0,
        }
    }

    /// The frame and metadata page its top keeps, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(in crate::storage::atoll) fn keeps_for_test(
        &self,
    ) -> Option<(Option<u32>, Option<crate::storage::atoll::meta::MetaId>)> {
        match &self.state {
            Owed::Rolling { owner, .. } => Some((owner.kept_frame(), owner.kept_meta())),
            Owed::Failed(_) => None,
        }
    }
}

/// An owed rollback's state the transaction held, its top's tenure and keeps let go inside a frame
/// (DESIGN.md 22.11).
impl Drop for OwedRollback {
    #[turso_macros::atoll_entry]
    fn drop(&mut self) {}
}

/// A connection's [`TxnSlot`], shared with its cursors.
pub type TxnCell = Arc<TxnSlot>;

/// A key above every encoded key: no column's encoding begins with 0xff, its first byte being a
/// NULL's (0x01 or 0xfe) or a tag between them, inverted or not.
const ABOVE_ENCODED: [u8; 1] = [0xff];

#[cfg(test)]
crate::thread::thread_local! {
    /// Cursors built on this thread, for tests to see a statement take up the ones it parked.
    pub(crate) static CURSORS_BUILT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// A cursor over one SQL tree.
pub struct AtollCursor {
    db: Arc<AtollDb>,
    txn: TxnCell,
    /// The tree id, which Turso calls the root page.
    id: u64,
    entry: Entry,
    /// Whether the tree's keys are in this build's encoding.
    current: bool,
    index: Option<Arc<IndexInfo>>,
    tree: VersionedTree,
    cursor: VersionedCursor,
    seeking: Option<Seeking>,
    /// The buffer the last seek's key was in, kept for the next.
    seek_key: Vec<u8>,
    /// The entry's record, once read.
    record: Record,
    null_row: bool,
    /// Whether the transaction's current statement created the tree, and writes it plainly.
    plain: bool,
    /// The write under way, kept across IO and waits.
    writing: Option<Writing>,
    /// Buffers the next write's key and entry are built in: the last write's, or the row the
    /// cursor showed before it, handed back.
    spare: (Vec<u8>, Vec<u8>),
    /// The leaf the last write reached, where the cursor stands after an insert.
    reached: Near,
    /// The last probe's key and its look at the key's leaf ([`VersionedTree::probe`]): the rowid
    /// read and the write of that key that follow it answer from the look while its leaf is
    /// unchanged, reaching no leaf again.
    probed: Vec<u8>,
    kept: Option<Kept>,
    /// The tree's rowid counter, once asked for: the tree's while the cursor is over it.
    counter: Option<Arc<super::rowid::Counter>>,
    /// The key of the entry the cursor stood at, found gone when it read again: it stands at the
    /// entry after it, or at none, read as no row until it moves.
    left: Option<Vec<u8>>,
    /// The key a [`refresh`](Self::refresh) seeks, kept across IO.
    refreshing: Option<Vec<u8>>,
    /// A foreign-key guard under way, kept across IO and waits.
    guarding: Option<Guarding>,
    /// Whether rewinds and moves on walk the versions the statement does not see, each holding
    /// its key, as rows (R10, [`VersionedTree::unseen_first`]).
    unseen: bool,
    /// The top of the operation under way, which keeps its memory-shortage tenure across IO
    /// (DESIGN.md 9.1.3, *The top and the owner*): a cursor runs one operation at a time.
    owner: Owner,
}

/// The record of the row a cursor rests on, once read, and the allocation the next is built in.
#[derive(Default)]
struct Record {
    read: Option<ImmutableRecord>,
    /// Whether the record's bytes are the row's value lent by the cursor
    /// ([`VersionedCursor::lend_value`]), to be given back.
    lent: bool,
    /// The allocation the next copied record is built in.
    buf: Option<RecordBuf>,
}

impl Record {
    /// The record of the row `cursor` rests on, its value read: a value out of line taken whole,
    /// one in the entry copied.
    fn of(&mut self, cursor: &mut VersionedCursor) -> Result<&ImmutableRecord> {
        if self.read.is_none() {
            let record = match cursor.lend_value() {
                Some(bytes) => {
                    self.lent = true;
                    ImmutableRecord::from_bin_record(payload_of(bytes))
                }
                None => {
                    let value = cursor.value().expect("a row");
                    let buf = self.buf.take().unwrap_or_else(RecordBuf::alloc);
                    let mut record = ImmutableRecord::from_buf(buf);
                    record.start_serialization(value)?;
                    record
                }
            };
            self.read = Some(record);
        }
        Ok(self.read.as_ref().expect("a record"))
    }

    /// Forgets the record read at the row: lent bytes go back to `cursor`, a copy's allocation is
    /// kept for the next.
    #[inline]
    fn forget(&mut self, cursor: &mut VersionedCursor) {
        let Some(record) = self.read.take() else {
            return;
        };
        if std::mem::take(&mut self.lent) {
            cursor.give_back(bytes_of(record.into_payload()));
        } else {
            self.buf = Some(record.retire());
        }
    }
}

/// What a foreign-key guard judges the keys of (R10).
pub enum GuardKey<'a> {
    /// Every key of the tree.
    All,
    /// A rowid table's row.
    Rowid(i64),
    /// The keys whose first columns are these values.
    Columns(&'a ImmutableRecord),
}

/// A foreign-key guard of every key that starts with a prefix (R10).
struct Guarding {
    prefix: Vec<u8>,
    guarded: Guarded,
    /// Over the tree's entries, each by its newest version, whatever the snapshot sees.
    entries: Cursor,
    sought: bool,
    /// The guard of the key `entries` is at, under way.
    op: Option<GuardOp>,
}

enum Writing {
    Versioned(WriteOp, Mode),
    PlainPut(Put, Mode),
    PlainRemove(Remove),
}

/// A seek under way, kept so the call made again after IO goes on with it.
struct Seeking {
    op: SeekOp,
    /// The encoded columns the seek key holds, or a table's rowid key.
    prefix: Vec<u8>,
    /// For a UNIQUE index whose keys leave the rowid out: the seek key's rowid.
    suffix: Option<i64>,
    /// The landing needs one move on, under way.
    moving_on: bool,
    /// Landed on an entry whose value the judgment reads, being read.
    loading: bool,
}

/// A move of the versioned cursor.
enum Move<'a> {
    Seek(&'a [u8], Seek),
    Next,
    Prev,
}

/// What a landing came to.
enum Verdict {
    Found,
    NotFound,
    /// The entry the columns name is on the wrong side of the rowid: one move on.
    MoveOn,
}

impl AtollCursor {
    /// A cursor over tree `id`, whose catalog row is `entry`, reading in `txn`. `index` describes
    /// an index's or WITHOUT ROWID table's keys, and `encoding` is the encoding this build writes
    /// for them.
    pub fn new(
        db: Arc<AtollDb>,
        txn: TxnCell,
        id: u64,
        entry: Entry,
        index: Option<Arc<IndexInfo>>,
        encoding: u32,
    ) -> Self {
        #[cfg(test)]
        CURSORS_BUILT.with(|n| n.set(n.get() + 1));
        // The cursor's operations carry its connection's identity (DESIGN.md 9.1.3).
        let txn_conn = txn.conn().clone();
        let a = db.atoll();
        let tree = VersionedTree::open(
            db.io().clone(),
            a.pool().clone(),
            a.store().clone(),
            entry.root,
        );
        Self {
            current: entry.layout == Layout::Table || entry.encoding == encoding,
            db,
            txn,
            id,
            entry,
            index,
            tree,
            cursor: VersionedCursor::new(),
            seeking: None,
            seek_key: Vec::new(),
            record: Record::default(),
            null_row: false,
            plain: false,
            writing: None,
            spare: Default::default(),
            reached: Near::NONE,
            probed: Vec::new(),
            kept: None,
            counter: None,
            left: None,
            refreshing: None,
            guarding: None,
            unseen: false,
            owner: Owner::of(txn_conn),
        }
    }

    /// From now on the cursor rewinds and moves on over the versions its statement does not see
    /// that hold their key, as rows, newest first within a key (R10).
    pub fn walk_unseen(&mut self) {
        self.unseen = true;
    }

    /// What the unseen version the cursor rests on comes to for a foreign-key guard whose scan
    /// matched it (R10): a wait while its writer is open, a conflict once it has committed, and
    /// nothing once it was rolled back, its undo having put back the version below it, which the
    /// walk reaches next; the database's failure once its writer was left where it stood.
    pub fn unseen_hit(&self) -> Result<Guard> {
        let Some(v) = self.cursor.unseen() else {
            return Ok(Guard::Clear);
        };
        let txns = self.db.atoll().transactions();
        Ok(match txns.writer(v.slot, v.ts) {
            crate::storage::atoll::txn::Writer::Open => {
                Guard::Wait(crate::storage::atoll::txn::Blocker {
                    slot: v.slot,
                    start: v.ts,
                })
            }
            crate::storage::atoll::txn::Writer::Committed => Guard::Conflict,
            crate::storage::atoll::txn::Writer::Gone => Guard::Clear,
            crate::storage::atoll::txn::Writer::Abandoned => return Err(txns.failed()),
        })
    }

    fn step_unseen(&mut self, first: bool) -> Result<IOResult<()>> {
        let mut guard = self.txn.enter()?;
        let txn = guard.as_mut().ok_or_else(outside)?;
        let (txns, versions) = (self.db.atoll().transactions(), self.db.versions());
        let moved = if first {
            self.tree
                .unseen_first(txns, versions, txn, &mut self.cursor)?
        } else {
            self.tree
                .unseen_next(txns, versions, txn, &mut self.cursor)?
        };
        if let IOResult::IO(io) = moved {
            return Ok(IOResult::IO(io));
        }
        self.record.forget(&mut self.cursor);
        self.null_row = false;
        self.left = None;
        Ok(IOResult::Done(()))
    }

    /// From now on the cursor's writes are plain: its tree was created by the transaction's
    /// current statement.
    pub fn write_plainly(&mut self) {
        self.plain = true;
    }

    /// The catalog row of the tree it is over.
    pub fn entry(&self) -> Entry {
        self.entry
    }

    /// Holds the tree's rowid counter, from `rowids` the first time; kept, since a cursor is over
    /// one tree for its life.
    pub fn hold_counter(&mut self, rowids: &super::rowid::Rowids) {
        if self.counter.is_none() {
            self.counter = Some(rowids.counter(self.entry.root));
        }
    }

    /// The counter [`hold_counter`](Self::hold_counter) holds.
    pub fn counter_held(&self) -> &super::rowid::Counter {
        self.counter.as_deref().expect("a counter held")
    }

    /// Ready for another statement's use of its tree: at no row, at the leaf it stood at, with no
    /// move, read or write under way, writing plainly if that statement created the tree, and
    /// describing its keys by `index`. Its buffers and its last probe's look are kept: the look
    /// answers only while its leaf is unchanged.
    pub fn reuse(&mut self, index: Option<Arc<IndexInfo>>, plain: bool) {
        self.index = index;
        self.plain = plain;
        self.seeking = None;
        self.record.forget(&mut self.cursor);
        self.null_row = false;
        self.writing = None;
        self.left = None;
        self.refreshing = None;
        self.guarding = None;
        self.unseen = false;
        let near = self.cursor.near();
        self.cursor.rest_nowhere(near);
    }

    /// Whether the operation under way holds a memory-shortage tenure, for a test.
    #[cfg(test)]
    pub(crate) fn shortage_queued(&self) -> bool {
        self.owner.queued()
    }

    /// The frame and metadata page its operation's owner keeps, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(crate) fn keeps_for_test(
        &self,
    ) -> (Option<u32>, Option<crate::storage::atoll::meta::MetaId>) {
        (self.owner.kept_frame(), self.owner.kept_meta())
    }

    /// Ends the operation under way, as its statement's reset does to a cursor it keeps: the
    /// memory-shortage tenure it held let go, its place and the gate passed on.
    pub fn end_operation(&mut self) {
        self.owner.end();
    }

    /// One call of the cursor's operation `op` as its owner's: a shortage `op` comes to is
    /// reserved here, holding nothing, and its tenure kept with the cursor until `op` is done.
    #[inline]
    fn owned<T>(
        &mut self,
        op: impl FnOnce(&mut Self) -> Result<IOResult<T>>,
    ) -> Result<IOResult<T>> {
        Owner::call_in(self, |c| &mut c.owner, op)
    }

    /// Starts from `near`, the leaf a cursor over the tree last stood at.
    pub fn start_near(&mut self, near: Near) {
        self.cursor.set_near(near);
        self.reached = near;
    }

    /// The leaf it last read or wrote at.
    pub fn near(&self) -> Near {
        self.cursor.near()
    }

    /// Forgets the record read at the row, to read it again.
    pub fn invalidate_record(&mut self) {
        self.record.forget(&mut self.cursor);
    }

    /// The tree's root.
    pub fn root(&self) -> crate::storage::atoll::page::PageId {
        self.entry.root
    }

    /// The tree as plain records, for its rowid counter's seed.
    pub fn plain_tree(&self) -> &crate::storage::atoll::btree::Tree {
        self.tree.tree()
    }

    fn step(&mut self, to: Move<'_>) -> Result<IOResult<bool>> {
        let mut guard = self.txn.enter()?;
        let txn = guard.as_mut().ok_or_else(outside)?;
        let (txns, versions) = (self.db.atoll().transactions(), self.db.versions());
        let moved = match to {
            Move::Seek(key, seek) => {
                self.tree
                    .seek(txns, versions, txn, &mut self.cursor, key, seek)?
            }
            Move::Next => self.tree.next(txns, versions, txn, &mut self.cursor)?,
            Move::Prev => self.tree.prev(txns, versions, txn, &mut self.cursor)?,
        };
        if let IOResult::Done(_) = moved {
            self.record.forget(&mut self.cursor);
            self.null_row = false;
            self.left = None;
        }
        Ok(moved)
    }

    /// Moves to the first row.
    pub fn rewind(&mut self) -> Result<IOResult<()>> {
        if self.unseen {
            return self.owned(|c| c.step_unseen(true));
        }
        done!(self.owned(|c| c.step(Move::Seek(&[], Seek::Ge))));
        Ok(IOResult::Done(()))
    }

    /// Moves to the last row.
    pub fn last(&mut self) -> Result<IOResult<()>> {
        let above = match self.entry.layout {
            Layout::Table => key::rowid(i64::MAX).to_vec(),
            _ => ABOVE_ENCODED.to_vec(),
        };
        done!(self.owned(|c| c.step(Move::Seek(&above, Seek::Le))));
        Ok(IOResult::Done(()))
    }

    /// Moves to the next row: from an entry found gone, the one after it, where it already stands.
    pub fn next(&mut self) -> Result<IOResult<()>> {
        if self.unseen {
            return self.owned(|c| c.step_unseen(false));
        }
        if self.left.take().is_some() {
            self.record.forget(&mut self.cursor);
            return Ok(IOResult::Done(()));
        }
        if self.tree.next_judged(&mut self.cursor) {
            self.record.forget(&mut self.cursor);
            self.null_row = false;
            return Ok(IOResult::Done(()));
        }
        let batched = {
            let mut guard = self.txn.enter()?;
            let txn = guard.as_mut().ok_or_else(outside)?;
            self.tree
                .next_batched(self.db.atoll().transactions(), txn, &mut self.cursor)
        };
        match batched {
            Some(Ok(())) => {
                self.record.forget(&mut self.cursor);
                self.null_row = false;
                return Ok(IOResult::Done(()));
            }
            Some(Err(e)) => return Err(e),
            None => {}
        }
        done!(self.owned(|c| c.step(Move::Next)));
        Ok(IOResult::Done(()))
    }

    /// Moves to the previous row: from an entry found gone, the last one before its key.
    pub fn prev(&mut self) -> Result<IOResult<()>> {
        if let Some(key) = self.left.take() {
            if let IOResult::IO(io) = self.owned(|c| c.step(Move::Seek(&key, Seek::Lt)))? {
                self.left = Some(key);
                return Ok(IOResult::IO(io));
            }
            return Ok(IOResult::Done(()));
        }
        done!(self.owned(|c| c.step(Move::Prev)));
        Ok(IOResult::Done(()))
    }

    /// Whether the cursor is at no row.
    pub fn is_empty(&self) -> bool {
        self.null_row || self.left.is_some() || self.cursor.key().is_none()
    }

    /// The key the cursor stands at, or the one it found gone.
    pub fn position(&self) -> Option<Vec<u8>> {
        self.left
            .clone()
            .or_else(|| self.cursor.key().map(<[u8]>::to_vec))
    }

    /// Reads its entry again in the view the transaction now has, after another statement of its
    /// connection changed the tree (DESIGN.md §17, §3), or at `key` for a cursor bound again to a
    /// tree a Clear replaced: at the same key it reads the entry as it now is; a key gone leaves
    /// it at the entry after it, read as no row until its next move, which stays there, as
    /// SQLite's cursor steps past a deleted row. A cursor at no row, or on a null row, stays.
    pub fn refresh(&mut self, key: Option<Vec<u8>>) -> Result<IOResult<()>> {
        if self.null_row {
            return Ok(IOResult::Done(()));
        }
        let Some(key) = self.refreshing.take().or(key).or_else(|| self.position()) else {
            return Ok(IOResult::Done(()));
        };
        if let IOResult::IO(io) = self.owned(|c| c.step(Move::Seek(&key, Seek::Ge)))? {
            self.refreshing = Some(key);
            return Ok(IOResult::IO(io));
        }
        self.left = (self.cursor.key() != Some(key.as_slice())).then_some(key);
        Ok(IOResult::Done(()))
    }

    pub fn set_null_flag(&mut self, flag: bool) {
        self.null_row = flag;
    }

    pub fn get_null_flag(&self) -> bool {
        self.null_row
    }

    /// The row's rowid: a table's key, or an index record's last column where the index has one.
    #[inline]
    pub fn rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        if self.null_row || self.left.is_some() {
            return Ok(IOResult::Done(None));
        }
        if self.entry.layout == Layout::Table {
            return match self.cursor.key() {
                Some(key) => Ok(IOResult::Done(Some(key::rowid_of(key)?))),
                None => Ok(IOResult::Done(None)),
            };
        }
        if self.is_empty() {
            return Ok(IOResult::Done(None));
        }
        if !self.index.as_ref().is_some_and(|i| i.has_rowid) {
            return Ok(IOResult::Done(None));
        }
        done!(self.load());
        let record = self.record_now()?.expect("a row");
        Ok(IOResult::Done(Some(last_rowid(record)?)))
    }

    /// Whether the row's record is where the cursor holds it, for [`payload`](Self::payload): no
    /// row, or the value in the entry or read, and not lent to a record.
    #[inline]
    pub fn lends_payload(&self) -> bool {
        self.is_empty() || self.cursor.value_held()
    }

    /// The row's record bytes where the cursor holds them, with no record built; `None` for no
    /// row. Only where [`lends_payload`](Self::lends_payload).
    #[inline]
    pub fn payload(&self) -> Option<&[u8]> {
        if self.is_empty() {
            return None;
        }
        self.cursor.value()
    }

    /// The row's record. Step it until done.
    pub fn record(&mut self) -> Result<IOResult<Option<&ImmutableRecord>>> {
        if !self.is_empty() {
            done!(self.load());
        }
        Ok(IOResult::Done(self.record_now()?))
    }

    /// Reads the row's value where it is out of line and not read yet, as its owner's operation;
    /// one in the entry or read already is there.
    #[inline]
    fn load(&mut self) -> Result<IOResult<()>> {
        if !self.cursor.unread() {
            return self.tree.load(&mut self.cursor);
        }
        self.owned(|c| c.tree.load(&mut c.cursor))
    }

    /// The row's record, its value read.
    fn record_now(&mut self) -> Result<Option<&ImmutableRecord>> {
        if self.is_empty() {
            return Ok(None);
        }
        self.record.of(&mut self.cursor).map(Some)
    }

    /// Moves to the row `op` finds from `key`.
    pub fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        if let (None, SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }) =
            (&self.seeking, &key, op)
        {
            if self.absent_by_probe(&key::rowid(*rowid))? {
                return Ok(IOResult::Done(SeekResult::NotFound));
            }
        }
        if self.seeking.is_none() {
            self.seeking = Some(self.plan(key, op)?);
        }
        // Held apart while it runs, so its key is read where it lies.
        let mut s = self.seeking.take().expect("a seek");
        let out = self.owned(|c| c.seek_planned(&mut s));
        match out {
            Ok(IOResult::IO(_)) => self.seeking = Some(s),
            _ => self.seek_key = s.prefix,
        }
        out
    }

    /// Whether the last probe's look answers that the statement reads `key` absent: the probe was
    /// of `key`, its leaf is unchanged since, and the newest version there is none, or a tombstone
    /// the statement sees. Then the cursor stands at no row, at that leaf, as a seek finding no
    /// row leaves it. Any other answer is the seek's.
    fn absent_by_probe(&mut self, key: &[u8]) -> Result<bool> {
        let Some(kept) = self.kept.as_ref().filter(|_| self.probed == key) else {
            return Ok(false);
        };
        // The look was taken before any statement rollback the transaction owes now, which may
        // change its leaf: refused as an entry is (ASYNC1 D2e (iii-b)).
        if self.txn.owes() {
            drop(self.txn.enter()?);
        }
        if !self.tree.tree().holds_kept(kept) {
            return Ok(false);
        }
        let absent = match kept.present() {
            None => true,
            Some((entry, _)) => {
                let v = crate::storage::atoll::txn::versioned::Version::decode(
                    entry
                        .get(..crate::storage::atoll::txn::versioned::HEADER)
                        .unwrap_or(&[]),
                )
                .ok_or_else(|| LimboError::Corrupt("a version header does not decode".into()))?;
                let mut guard = self.txn.enter()?;
                let txn = guard.as_mut().ok_or_else(outside)?;
                v.deleted && self.db.atoll().transactions().visible(txn, v.slot, v.ts)
            }
        };
        if absent {
            let near = kept.near();
            self.cursor.rest_nowhere(near);
            self.record.forget(&mut self.cursor);
            self.null_row = false;
            self.left = None;
        }
        Ok(absent)
    }

    fn plan(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<Seeking> {
        let mut prefix = std::mem::take(&mut self.seek_key);
        prefix.clear();
        let suffix = match key {
            SeekKey::TableRowId(rowid) => {
                if self.entry.layout != Layout::Table {
                    return Err(LimboError::InternalError(format!(
                        "a rowid seek into tree {}, which is no rowid table",
                        self.id
                    )));
                }
                prefix.extend_from_slice(&key::rowid(rowid));
                None
            }
            SeekKey::IndexKey(record) => {
                if !self.current {
                    return Err(LimboError::InternalError(format!(
                        "tree {} holds keys of another encoding, which a seek cannot read: \
                         REINDEX it",
                        self.id
                    )));
                }
                let info = self.index.as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "a key seek into tree {} with no key",
                        self.id
                    ))
                })?;
                let values = record.iter()?.collect::<Result<Vec<ValueRef<'_>>>>()?;
                if values.len() > info.key_info.len() {
                    return Err(LimboError::InternalError(format!(
                        "a seek key of {} columns into a key of {}",
                        values.len(),
                        info.key_info.len()
                    )));
                }
                let (columns, suffix) = match values.split_last() {
                    Some((rowid, columns))
                        if info.is_unique
                            && info.has_rowid
                            && values.len() == info.num_cols
                            && !columns.iter().any(|v| matches!(v, ValueRef::Null)) =>
                    {
                        (columns, Some(integer(rowid)?))
                    }
                    _ => (values.as_slice(), None),
                };
                key::encode(columns.iter().cloned(), &info.key_info, &mut prefix)?;
                suffix
            }
        };
        Ok(Seeking {
            op,
            prefix,
            suffix,
            moving_on: false,
            loading: false,
        })
    }

    fn seek_planned(&mut self, s: &mut Seeking) -> Result<IOResult<SeekResult>> {
        loop {
            let (op, table) = (s.op, self.entry.layout == Layout::Table);
            if s.loading {
                done!(self.tree.load(&mut self.cursor));
                s.loading = false;
                match self.judge(s)? {
                    Verdict::Found => return Ok(IOResult::Done(SeekResult::Found)),
                    Verdict::NotFound => return Ok(IOResult::Done(SeekResult::NotFound)),
                    Verdict::MoveOn => s.moving_on = true,
                }
            }
            if s.moving_on {
                let forward = matches!(op, SeekOp::GE { .. } | SeekOp::GT);
                let found = done!(self.step(if forward { Move::Next } else { Move::Prev }));
                // Past the entry the columns name: every entry now is on the op's side of the key.
                return Ok(IOResult::Done(if found && !op.eq_only() {
                    SeekResult::Found
                } else {
                    SeekResult::NotFound
                }));
            }
            let p = s.prefix.as_slice();
            let found = if table {
                let seek = match op {
                    SeekOp::GE { .. } => Seek::Ge,
                    SeekOp::GT => Seek::Gt,
                    SeekOp::LE { .. } => Seek::Le,
                    SeekOp::LT => Seek::Lt,
                };
                done!(self.step(Move::Seek(p, seek)))
            } else {
                // Up to the last key starting with p; with no bound, the last key of all.
                let up_to = || match key::successor(p) {
                    Some(q) => (Cow::Owned(q), Seek::Lt),
                    None => (Cow::Borrowed(&ABOVE_ENCODED[..]), Seek::Le),
                };
                let (target, seek) = match op {
                    SeekOp::GE { .. } => (Cow::Borrowed(p), Seek::Ge),
                    SeekOp::GT if s.suffix.is_some() => (Cow::Borrowed(p), Seek::Ge),
                    SeekOp::GT => match key::successor(p) {
                        Some(q) => (Cow::Owned(q), Seek::Ge),
                        // Every key starts with the empty prefix: none is above them.
                        None => (Cow::Borrowed(&ABOVE_ENCODED[..]), Seek::Gt),
                    },
                    SeekOp::LE { .. } => up_to(),
                    SeekOp::LT if s.suffix.is_some() => up_to(),
                    SeekOp::LT => (Cow::Borrowed(p), Seek::Lt),
                };
                done!(self.step(Move::Seek(&target, seek)))
            };
            if !found {
                return Ok(IOResult::Done(SeekResult::NotFound));
            }
            // The judgment reads an index landing's rowid from its record.
            if !table && s.suffix.is_some() {
                s.loading = true;
                continue;
            }
            match self.judge(s)? {
                Verdict::Found => return Ok(IOResult::Done(SeekResult::Found)),
                Verdict::NotFound => return Ok(IOResult::Done(SeekResult::NotFound)),
                Verdict::MoveOn => s.moving_on = true,
            }
        }
    }

    /// Judges the entry a seek landed on.
    fn judge(&mut self, s: &Seeking) -> Result<Verdict> {
        let (op, prefix, suffix) = (s.op, s.prefix.as_slice(), s.suffix);
        let key = self.cursor.key().expect("a landing");
        if self.entry.layout == Layout::Table {
            return Ok(if !op.eq_only() || key == prefix {
                Verdict::Found
            } else {
                Verdict::NotFound
            });
        }
        let named = key.starts_with(prefix);
        let Some(rowid) = suffix else {
            return Ok(if !op.eq_only() || named {
                Verdict::Found
            } else {
                Verdict::NotFound
            });
        };
        if !named {
            return Ok(if op.eq_only() {
                Verdict::NotFound
            } else {
                Verdict::Found
            });
        }
        let record = self.record_now()?.expect("a landing");
        let cmp = last_rowid(record)?.cmp(&rowid);
        let satisfied = match op {
            SeekOp::GE { .. } => cmp != Ordering::Less,
            SeekOp::GT => cmp == Ordering::Greater,
            SeekOp::LE { .. } => cmp != Ordering::Greater,
            SeekOp::LT => cmp == Ordering::Less,
        };
        Ok(match (satisfied, op.eq_only()) {
            (true, false) => Verdict::Found,
            (true, true) if cmp == Ordering::Equal => Verdict::Found,
            (true, true) => Verdict::NotFound,
            (false, _) => Verdict::MoveOn,
        })
    }
}

impl AtollCursor {
    /// The key, entry and put mode of the row or index entry `key` names, built in the cursor's
    /// spare buffers: the entry is [`HEADER`] bytes for the version header, then the value.
    fn entry_of(&mut self, key: &BTreeKey<'_>) -> Result<(Vec<u8>, Vec<u8>, Mode)> {
        let (mut k, mut entry) = std::mem::take(&mut self.spare);
        k.clear();
        entry.clear();
        entry.resize(HEADER, 0);
        match key {
            BTreeKey::TableRowId((rowid, record)) => {
                let record = record.ok_or_else(|| {
                    LimboError::InternalError("a table row written with no record".into())
                })?;
                k.extend_from_slice(&key::rowid(*rowid));
                entry.extend_from_slice(record.get_payload());
                Ok((k, entry, Mode::Upsert))
            }
            BTreeKey::IndexKey(record) => {
                if !self.current {
                    return Err(LimboError::InternalError(format!(
                        "tree {} holds keys of another encoding, which cannot be written: \
                         REINDEX it",
                        self.id
                    )));
                }
                let info = self.index.as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "a key written into tree {} with no key",
                        self.id
                    ))
                })?;
                let values = record.iter()?.collect::<Result<Vec<ValueRef<'_>>>>()?;
                let suffixless = info.is_unique
                    && info.has_rowid
                    && values.len() == info.num_cols
                    && !values[..values.len() - 1]
                        .iter()
                        .any(|v| matches!(v, ValueRef::Null));
                let columns = if suffixless {
                    &values[..values.len() - 1]
                } else {
                    &values[..]
                };
                key::encode(columns.iter().cloned(), &info.key_info, &mut k)?;
                let mode = if suffixless {
                    Mode::Insert
                } else {
                    Mode::Upsert
                };
                entry.extend_from_slice(record.get_payload());
                Ok((k, entry, mode))
            }
        }
    }

    /// Writes the row or index entry `key` names, and leaves the cursor at it, as a b-tree's
    /// insert does: the VDBE reads the row an UPDATE wrote back through the cursor, which shows
    /// it from the bytes the write stored. Step it until done; a write behind another transaction
    /// comes back as [`Write::Wait`] or [`Write::Conflict`].
    pub fn insert(&mut self, key: &BTreeKey<'_>) -> Result<IOResult<Write>> {
        if self.writing.is_none() {
            let (k, entry, mode) = self.entry_of(key)?;
            let max = self.tree.max_key();
            if k.len() > max {
                let len = k.len();
                self.spare = (k, entry);
                return Err(LimboError::InternalError(format!(
                    "a key of {len} bytes into tree {}, which holds keys of at most {max}",
                    self.id
                )));
            }
            let kept = self
                .kept
                .take()
                .filter(|_| self.probed == k)
                .or_else(|| self.cursor.kept_at(&k));
            self.writing = Some(if self.plain {
                let guard = self.txn.enter()?;
                let txn = guard.as_ref().ok_or_else(outside)?;
                Writing::PlainPut(self.tree.plain_put(txn, k, entry, mode), mode)
            } else {
                Writing::Versioned(
                    WriteOp::put_entry(k, entry, mode)
                        .near(self.cursor.near())
                        .kept(kept),
                    mode,
                )
            });
        }
        self.owned(Self::write)
    }

    /// A write path's probe of `key`, judged by its newest version as a write would judge it
    /// (A3.12, A4.9; [`VersionedTree::probe`]). Step it until done.
    pub fn probe(
        &mut self,
        key: &[u8],
    ) -> Result<IOResult<crate::storage::atoll::txn::versioned::Probe>> {
        self.owned(|c| c.probe_step(key))
    }

    fn probe_step(
        &mut self,
        key: &[u8],
    ) -> Result<IOResult<crate::storage::atoll::txn::versioned::Probe>> {
        let mut lock = self.txn.enter()?;
        let txn = lock.as_mut().ok_or_else(outside)?;
        let near = self.cursor.near();
        self.kept = None;
        let probed = self.tree.probe(
            self.db.atoll().transactions(),
            txn,
            key,
            near,
            &mut self.kept,
        )?;
        if self.kept.is_some() {
            self.probed.clear();
            self.probed.extend_from_slice(key);
        }
        Ok(probed)
    }

    /// The keys a foreign-key guard of `key` judges start with this (R10): a table's rowid key, or
    /// the encoded columns of an index or WITHOUT ROWID table, as a seek on them encodes them;
    /// empty for every key. A tree of another encoding refuses it, naming REINDEX, as it refuses a
    /// seek, so a guard never misses a key.
    pub fn guard_prefix(&self, key: &GuardKey<'_>) -> Result<Vec<u8>> {
        match key {
            GuardKey::All => Ok(Vec::new()),
            GuardKey::Rowid(rowid) => {
                if self.entry.layout != Layout::Table {
                    return Err(LimboError::InternalError(format!(
                        "a rowid guard of tree {}, which is no rowid table",
                        self.id
                    )));
                }
                Ok(key::rowid(*rowid).to_vec())
            }
            GuardKey::Columns(record) => {
                if !self.current {
                    return Err(LimboError::InternalError(format!(
                        "tree {} holds keys of another encoding, which a foreign-key guard cannot \
                         read: REINDEX it",
                        self.id
                    )));
                }
                let info = self.index.as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "a column guard of tree {}, which has no key columns",
                        self.id
                    ))
                })?;
                let values = record.iter()?.collect::<Result<Vec<ValueRef<'_>>>>()?;
                if values.len() > info.key_info.len() {
                    return Err(LimboError::InternalError(format!(
                        "a guard of {} columns over a key of {}",
                        values.len(),
                        info.key_info.len()
                    )));
                }
                let mut prefix = Vec::new();
                key::encode(values.into_iter(), &info.key_info, &mut prefix)?;
                Ok(prefix)
            }
        }
    }

    /// Guards, after its transaction wrote, every key of the tree that starts with `prefix`, the
    /// tree's every key for an empty one (R10, [`VersionedTree::guard`]): what the first key
    /// judged comes to, or [`Guard::Clear`] once none is. A wait keeps the guard at its key, which
    /// stepping it again judges afresh. Step it until done.
    pub fn guard(&mut self, prefix: &[u8], guarded: Guarded) -> Result<IOResult<Guard>> {
        self.owned(|c| c.guard_step(prefix, guarded))
    }

    fn guard_step(&mut self, prefix: &[u8], guarded: Guarded) -> Result<IOResult<Guard>> {
        let mut lock = self.txn.enter()?;
        let txn = lock.as_mut().ok_or_else(outside)?;
        let (txns, versions) = (self.db.atoll().transactions(), self.db.versions());
        let g = self.guarding.get_or_insert_with(|| Guarding {
            prefix: prefix.to_vec(),
            guarded,
            entries: Cursor::new(),
            sought: false,
            op: None,
        });
        loop {
            if let Some(op) = g.op.as_mut() {
                match done!(self.tree.guard(txns, versions, txn, op)) {
                    Guard::Clear => g.op = None,
                    Guard::Wait(blocker) => {
                        op.again();
                        return Ok(IOResult::Done(Guard::Wait(blocker)));
                    }
                    Guard::Conflict => {
                        self.guarding = None;
                        return Ok(IOResult::Done(Guard::Conflict));
                    }
                }
            }
            let found = if g.sought {
                done!(self.tree.tree().next(&mut g.entries))
            } else {
                let found = done!(self.tree.tree().seek(&mut g.entries, &g.prefix, Seek::Ge));
                g.sought = true;
                found
            };
            let at = found
                .then(|| g.entries.key().zip(g.entries.value()))
                .flatten()
                .filter(|(key, _)| key.starts_with(&g.prefix));
            let Some((key, value)) = at else {
                self.guarding = None;
                return Ok(IOResult::Done(Guard::Clear));
            };
            let head = value
                .get(..crate::storage::atoll::txn::versioned::HEADER)
                .unwrap_or(&[]);
            g.op = Some(GuardOp::from_head(key, head, g.guarded)?);
        }
    }

    /// Drops a write that came back `Write::Wait`, which wrote nothing, so that the cursor's next
    /// write starts afresh.
    pub fn abandon_write(&mut self) {
        self.writing = None;
    }

    /// Deletes the row or index entry the cursor is at. Step it until done.
    pub fn delete(&mut self) -> Result<IOResult<Write>> {
        self.kept = None;
        if self.writing.is_none() {
            let at = self
                .cursor
                .key()
                .filter(|_| !self.null_row && self.left.is_none())
                .ok_or_else(|| LimboError::InternalError("a delete at no row".into()))?;
            let (mut k, entry) = std::mem::take(&mut self.spare);
            k.clear();
            k.extend_from_slice(at);
            self.writing = Some(if self.plain {
                let remove = Remove::new(&k);
                self.spare = (k, entry);
                Writing::PlainRemove(remove)
            } else {
                let kept = self.cursor.kept_at(&k);
                Writing::Versioned(
                    WriteOp::delete_into(k, entry)
                        .near(self.cursor.near())
                        .kept(kept),
                    Mode::Update,
                )
            });
        }
        self.owned(Self::write)
    }

    fn write(&mut self) -> Result<IOResult<Write>> {
        let mut guard = self.txn.enter()?;
        let txn = guard.as_mut().ok_or_else(outside)?;
        let (txns, versions) = (self.db.atoll().transactions(), self.db.versions());
        // A chunk of changes a plain write filled goes to the log before the next changes
        // anything (DESIGN.md §11.4.1).
        if txn.changes().has_sealed() {
            if let IOResult::IO(io) = txns.spill(txn)? {
                return Ok(IOResult::IO(io));
            }
        }
        let (written, mode) = match self.writing.as_mut().expect("a write") {
            Writing::Versioned(op, mode) => (self.tree.write(txns, versions, txn, op)?, *mode),
            Writing::PlainPut(put, mode) => (
                match self.tree.tree().put(put)? {
                    IOResult::Done(outcome) => IOResult::Done(Write::Done(outcome)),
                    IOResult::IO(io) => IOResult::IO(io),
                },
                *mode,
            ),
            Writing::PlainRemove(remove) => (
                match self.tree.tree().remove(remove)? {
                    IOResult::Done(true) => IOResult::Done(Write::Done(Outcome::Updated)),
                    IOResult::Done(false) => IOResult::Done(Write::Done(Outcome::Unchanged)),
                    IOResult::IO(io) => IOResult::IO(io),
                },
                Mode::Update,
            ),
        };
        let IOResult::Done(write) = written else {
            return Ok(written);
        };
        match &self.writing {
            Some(Writing::Versioned(op, _)) => self.reached = op.reached(),
            Some(Writing::PlainPut(put, _)) => {
                self.reached = put.reached();
                // A plain write is its statement's change by key like any other: replay puts it
                // in the tree as a committed version.
                if matches!(write, Write::Done(Outcome::Inserted | Outcome::Updated)) {
                    let root = self.tree.root();
                    let (key, entry) = put.key_and_entry();
                    txn.changed(|s, c| c.put_plainly(s, root, key, &entry[HEADER..]));
                }
            }
            Some(Writing::PlainRemove(remove)) => {
                if write == Write::Done(Outcome::Updated) {
                    let root = self.tree.root();
                    txn.changed(|s, c| c.delete_plainly(s, root, remove.key()));
                }
            }
            None => {}
        }
        if let Write::Wait(_) = write {
            // Kept: stepped again once the writer has ended.
            return Ok(IOResult::Done(write));
        }
        self.record.forget(&mut self.cursor);
        // An insert's row is where the cursor now stands, shown from the entry it stored; any
        // write's buffers serve the next.
        let (k, entry, put) = match self.writing.take().expect("a write") {
            Writing::Versioned(op, _) => {
                let put = !op.deletes();
                let (k, entry) = op.into_buffers();
                (k, entry, put)
            }
            Writing::PlainPut(put, _) => {
                let (k, entry) = put.into_buffers();
                (k, entry.unwrap_or_default(), true)
            }
            Writing::PlainRemove(_) => (Vec::new(), Vec::new(), false),
        };
        match write {
            Write::Done(Outcome::Inserted | Outcome::Updated) if put && entry.len() >= HEADER => {
                self.spare = self.cursor.place_entry(k, entry, HEADER, self.reached);
                self.null_row = false;
                self.left = None;
            }
            _ if !k.is_empty() || !entry.is_empty() => self.spare = (k, entry),
            _ => {}
        }
        if mode == Mode::Insert && write == Write::Done(Outcome::Unchanged) {
            return Err(LimboError::ConstraintViolation(Box::new(
                ConstraintViolation::new(
                    ConstraintKind::Unique,
                    format!("UNIQUE constraint failed: tree {}", self.id),
                ),
            )));
        }
        Ok(IOResult::Done(write))
    }
}

fn outside() -> LimboError {
    LimboError::InternalError("an Atoll cursor used outside a transaction".into())
}

/// An integer column's value.
fn integer(value: &ValueRef<'_>) -> Result<i64> {
    match value {
        ValueRef::Numeric(Numeric::Integer(i)) => Ok(*i),
        other => Err(LimboError::InternalError(format!(
            "a rowid that is no integer: {other:?}"
        ))),
    }
}

/// The rowid an index record ends with.
fn last_rowid(record: &ImmutableRecord) -> Result<i64> {
    let last = record
        .last_value()
        .ok_or_else(|| LimboError::Corrupt("an index record with no column".into()))??;
    integer(&last)
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
