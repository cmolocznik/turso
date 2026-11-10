//! A cursor over a versioned tree as a transaction's statement sees it (DESIGN.md §11.2, §17).
//!
//! It moves a plain [`Cursor`], which copies each leaf entry out, head and value, a value out of
//! line left unread: its head is in the leaf. Each entry is judged by its newest version: one the
//! statement sees is its row, or no row if it is a tombstone; one it does not is followed down its
//! chain ([`VersionedTree::follow_to`]) to the version it does. A key with no version the statement
//! sees, or whose version is a tombstone, is passed over in the direction of the move. Like the
//! plain cursor it holds no latch between steps.
//!
//! A row's value out of line is read only when asked for ([`VersionedTree::load`]), so a move that
//! needs only keys, and an entry passed over, read no value page. Reading it later needs no check
//! of the leaf: the pages of a version a statement sees stay while it may read them (DESIGN.md
//! §6.5), a committed one's and an earlier statement's retired when replaced, and only a version
//! the statement itself wrote, which it never sees, freed.
//!
//! Walking unseen ([`VersionedTree::unseen_first`]), it rests instead on each version the
//! statement does not see that holds its key, newest first within a key and keys in order, with
//! that version's value as the row: what R10's guard of a child table with no index on its
//! foreign key reads, so its scan compares each such version as the probe compares a row.

use super::{corrupt_header, Chain, Transactions, Txn, Version, VersionedTree, Versions, HEADER};
use crate::storage::atoll::btree::value::{Read, ValueRead, ValueRef};
use crate::storage::atoll::btree::{Cursor, Kept, Near, Seek, StoredValue};
use crate::types::IOResult;
use crate::{LimboError, Result};
use std::sync::Arc;

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// A position in a versioned tree: a key and the value of the version the statement sees.
pub struct VersionedCursor {
    cursor: Cursor,
    /// The move under way, kept across steps.
    moving: Option<Move>,
    /// The chain of the entry the plain cursor is at, being followed: its key and where it stands.
    chain: Option<(Vec<u8>, Chain)>,
    entry: Shown,
    /// The buffers of the last row held apart, for the next.
    spare: (Vec<u8>, Vec<u8>),
    /// The key of the seek under way.
    target: Vec<u8>,
    /// The walk over versions the statement does not see, once begun.
    unseen: Option<Unseen>,
    /// Where the row's value is.
    value: Value,
    /// The row's value read out of line, or the allocation kept for the next.
    body: Vec<u8>,
    /// Its transaction's count of changes as its last move began: the entries the plain cursor
    /// copied ahead are as the statement reads them only while it is unchanged.
    edits: u64,
    /// The plain cursor's batch judged at once, under the transaction's lock: forward steps take
    /// the rows it found with no lock while the transaction's view and the batch stay the same.
    judged: Judged,
}

/// Which entries of a batch, by slot from `from`, are rows a statement shows as they lie (the
/// newest version, seen, no tombstone), judged at the transaction's view `view` (read where
/// `of` is) for the batch counted `batch`.
#[derive(Default)]
struct Judged {
    from: usize,
    rows: Vec<bool>,
    batch: u64,
    view: u64,
    of: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
}

/// Where the value of the row a cursor rests on is.
#[derive(Default)]
enum Value {
    /// In the entry the row shows, or there is no row.
    #[default]
    InEntry,
    /// Out of line, not read: by its reference.
    Unread(ValueRef),
    /// Out of line, being read.
    Reading(ValueRead),
    /// Read, in the cursor's body.
    Read,
    /// Read, and lent to the caller until given back.
    Lent,
}

/// The row a cursor rests on.
#[derive(Default)]
enum Shown {
    #[default]
    None,
    /// The plain cursor's entry: the newest version, which the statement sees, read where the
    /// plain cursor holds it, its value past the version header.
    Plain,
    /// A row held apart: a version below the newest, or the one the statement just wrote: its
    /// key, and the bytes its value is in from the offset given.
    Held(Vec<u8>, Vec<u8>, usize),
    /// A version below the newest whose value is out of line: its key.
    Apart(Vec<u8>),
}

/// Where a walk over unseen versions stands.
enum Unseen {
    /// On to the plain cursor's next key, or its first.
    Key { first: bool },
    /// Judging a version of `key`: one the statement sees ends the key; a tombstone goes on below.
    Judge {
        key: Vec<u8>,
        v: Version,
        value: UnseenValue,
    },
    /// On to the version below `v` of `key`, through the arena entry `v` names.
    Below { key: Vec<u8>, v: Version },
    /// Resting on `v`, which the cursor's entry shows.
    Shown { v: Version },
}

/// An unseen version's value: read, or out of line and being read.
enum UnseenValue {
    Read(Vec<u8>),
    OutOfLine(ValueRead),
}

#[derive(Clone, Copy)]
enum Move {
    /// The plain cursor's seek from the cursor's target, not yet done.
    Seek(Seek),
    /// On past the plain cursor's entry, forward or backward.
    Step { forward: bool },
}

impl Move {
    fn forward(self) -> bool {
        match self {
            Move::Seek(seek) => matches!(seek, Seek::Ge | Seek::Gt),
            Move::Step { forward } => forward,
        }
    }
}

impl Default for VersionedCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionedCursor {
    pub fn new() -> Self {
        Self {
            cursor: Cursor::lazy(),
            moving: None,
            chain: None,
            entry: Shown::None,
            spare: Default::default(),
            target: Vec::new(),
            unseen: None,
            value: Value::InEntry,
            body: Vec::new(),
            edits: 0,
            judged: Judged::default(),
        }
    }

    #[inline]
    pub fn key(&self) -> Option<&[u8]> {
        match &self.entry {
            Shown::None => None,
            Shown::Plain => self.cursor.key(),
            Shown::Held(k, _, _) | Shown::Apart(k) => Some(k),
        }
    }

    /// Its leaf's look at `key`, where the plain cursor rests on that key's entry as its leaf
    /// holds it: what a write of the key it stands at goes in from.
    pub fn kept_at(&self, key: &[u8]) -> Option<Kept> {
        (self.cursor.key()? == key)
            .then(|| self.cursor.kept())
            .flatten()
    }

    /// Whether the row's value is where [`value`](Self::value) reads it: in the entry, or read, and
    /// not lent.
    pub fn value_held(&self) -> bool {
        matches!(self.value, Value::InEntry | Value::Read)
    }

    /// Whether the row's value is out of line and not read yet: [`VersionedTree::load`] reads it.
    #[inline]
    pub fn unread(&self) -> bool {
        matches!(self.value, Value::Unread(_) | Value::Reading(_))
    }

    /// The value of the version the statement sees, without its header. A value out of line is
    /// read first ([`VersionedTree::load`]).
    pub fn value(&self) -> Option<&[u8]> {
        match (&self.entry, &self.value) {
            (Shown::None, _) => None,
            (_, Value::Read) => Some(&self.body),
            (_, Value::Unread(_) | Value::Reading(_)) => {
                panic!("a row's value out of line asked for before it is read")
            }
            (_, Value::Lent) => panic!("a row's value asked for while it is lent"),
            (Shown::Plain, Value::InEntry) => self.cursor.value().and_then(|v| v.get(HEADER..)),
            (Shown::Held(_, v, from), Value::InEntry) => v.get(*from..),
            (Shown::Apart(_), Value::InEntry) => {
                unreachable!("a row apart keeps its value out of line")
            }
        }
    }

    /// The row's value, read out of line, handed over whole: the cursor keeps no copy until it is
    /// given back. `None` where the value is in the row's entry.
    pub fn lend_value(&mut self) -> Option<Vec<u8>> {
        if !matches!(self.value, Value::Read) {
            return None;
        }
        self.value = Value::Lent;
        Some(std::mem::take(&mut self.body))
    }

    /// Takes back a value [lent](Self::lend_value): the row's again while the cursor has not
    /// moved since, else an allocation kept for the next read.
    pub fn give_back(&mut self, bytes: Vec<u8>) {
        if matches!(self.value, Value::Lent) {
            self.body = bytes;
            self.value = Value::Read;
        } else if bytes.capacity() > self.body.capacity() {
            self.body = bytes;
        }
    }

    /// The leaf it last read or was placed at.
    pub fn near(&self) -> Near {
        self.cursor.near()
    }

    /// Tries `near` before its next descent.
    pub fn set_near(&mut self, near: Near) {
        self.cursor.set_near(near);
    }

    /// Stands at `key`, whose entry `txn`'s statement just wrote as `entry`, the value from
    /// `from` on: the row reads as that, the statement's own version, and a move from it starts
    /// from the key, as after a seek to it, trying `near` first. Gives back buffers for the
    /// caller's next entry: the row it showed before, or its spares.
    pub fn place_entry(
        &mut self,
        key: Vec<u8>,
        entry: Vec<u8>,
        from: usize,
        near: Near,
    ) -> (Vec<u8>, Vec<u8>) {
        self.cursor.place(&key, near);
        self.moving = None;
        self.chain = None;
        self.unseen = None;
        self.value = Value::InEntry;
        match std::mem::replace(&mut self.entry, Shown::Held(key, entry, from)) {
            Shown::Held(k, v, _) => (k, v),
            Shown::Apart(k) => (k, std::mem::take(&mut self.spare.1)),
            _ => std::mem::take(&mut self.spare),
        }
    }

    /// Stands at no row, at the leaf `near`, as a seek that found none: a move from it finds no
    /// row, and the next seek tries `near` first.
    pub fn rest_nowhere(&mut self, near: Near) {
        self.cursor.set_near(near);
        self.unseen = None;
        self.rest(Shown::None);
    }

    /// The unseen version a walk over them rests on.
    pub fn unseen(&self) -> Option<Version> {
        match self.unseen {
            Some(Unseen::Shown { v }) => Some(v),
            _ => None,
        }
    }
}

impl VersionedTree {
    /// Moves `cursor` to the first row `seek` lands on from `key` that `txn`'s current statement
    /// sees. Step it until done; then whether it found one.
    pub fn seek(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        cursor: &mut VersionedCursor,
        key: &[u8],
        seek: Seek,
    ) -> Result<IOResult<bool>> {
        if cursor.moving.is_none() {
            cursor.target.clear();
            cursor.target.extend_from_slice(key);
            cursor.moving = Some(Move::Seek(seek));
            cursor.chain = None;
            cursor.cursor.set_lazy(true);
            cursor.edits = txn.changes().edits();
        }
        self.settle(txns, versions, txn, cursor)
    }

    /// The row after the cursor's that `txn`'s current statement sees. Step it until done.
    pub fn next(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        cursor: &mut VersionedCursor,
    ) -> Result<IOResult<bool>> {
        self.step(txns, versions, txn, cursor, true)
    }

    /// The row before the cursor's that `txn`'s current statement sees. Step it until done.
    pub fn prev(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        cursor: &mut VersionedCursor,
    ) -> Result<IOResult<bool>> {
        self.step(txns, versions, txn, cursor, false)
    }

    fn step(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        cursor: &mut VersionedCursor,
        forward: bool,
    ) -> Result<IOResult<bool>> {
        if cursor.moving.is_none() {
            if matches!(cursor.entry, Shown::None) {
                return Ok(IOResult::Done(false));
            }
            cursor.moving = Some(Move::Step { forward });
            cursor.chain = None;
            cursor.cursor.set_lazy(true);
            // A change of the statement's own may be one it sees, which the entries copied ahead
            // do not hold.
            let edits = txn.changes().edits();
            if edits != cursor.edits {
                cursor.cursor.drop_batch();
                cursor.edits = edits;
            }
        }
        self.settle(txns, versions, txn, cursor)
    }

    /// The next row of a forward scan from the plain cursor's batch, while the statement has
    /// changed nothing since the batch was read and the cursor rests on a newest version: `None`
    /// where the batch has no entry left, or where the entry it takes is not the row the
    /// statement sees, the move then under way for [`next`](Self::next) to finish.
    #[inline]
    pub fn next_batched(
        &self,
        txns: &Transactions,
        txn: &mut Txn,
        cursor: &mut VersionedCursor,
    ) -> Option<Result<()>> {
        if cursor.moving.is_some()
            || cursor.unseen.is_some()
            || !matches!(cursor.entry, Shown::Plain)
            || txn.changes().edits() != cursor.edits
        {
            return None;
        }
        Self::judge(txns, txn, cursor);
        if !cursor.cursor.next_batched() {
            return None;
        }
        let value = cursor.cursor.value().expect("an entry");
        let Some(v) = Version::decode(value.get(..HEADER).unwrap_or(&[])) else {
            return Some(Err(corrupt_header()));
        };
        cursor.value = Value::InEntry;
        if !txns.visible(txn, v.slot, v.ts) {
            let key = cursor.cursor.key().expect("an entry").to_vec();
            cursor.chain = Some((key, Chain::from(v)));
            cursor.moving = Some(Move::Step { forward: true });
            return None;
        }
        if v.deleted {
            cursor.moving = Some(Move::Step { forward: true });
            return None;
        }
        if let Some(reference) = self.tree().unread_reference(&cursor.cursor) {
            cursor.value = Value::Unread(reference);
        }
        Some(Ok(()))
    }

    /// Judges the plain cursor's batch, unless judged at this batch and view: which of its entries
    /// are rows the statement shows as they lie.
    fn judge(txns: &Transactions, txn: &mut Txn, cursor: &mut VersionedCursor) {
        let (view, of) = txn.changes().view();
        let j = &mut cursor.judged;
        let batch = cursor.cursor.batch_seq();
        if j.batch == batch && j.view == view && j.of.as_ref().is_some_and(|o| Arc::ptr_eq(o, of)) {
            return;
        }
        if !j.of.as_ref().is_some_and(|o| Arc::ptr_eq(o, of)) {
            j.of = Some(of.clone());
        }
        j.rows.clear();
        j.batch = batch;
        j.view = view;
        let Some((from, left)) = cursor.cursor.batch_ahead() else {
            return;
        };
        j.from = from;
        for pos in from..from + left {
            let row = cursor
                .cursor
                .batch_value(pos)
                .and_then(|v| Version::decode(v.get(..HEADER)?))
                .is_some_and(|v| !v.deleted && txns.visible(txn, v.slot, v.ts));
            j.rows.push(row);
        }
    }

    /// [`next_batched`](Self::next_batched) with no lock: the next entry of the batch where its
    /// judgment holds, at the same batch and the transaction's view unchanged, and found it a row
    /// the statement shows. Whether it took one.
    #[inline]
    pub fn next_judged(&self, cursor: &mut VersionedCursor) -> bool {
        if cursor.moving.is_some()
            || cursor.unseen.is_some()
            || !matches!(cursor.entry, Shown::Plain)
        {
            return false;
        }
        let j = &cursor.judged;
        let Some(of) = &j.of else {
            return false;
        };
        if j.batch != cursor.cursor.batch_seq()
            || of.load(std::sync::atomic::Ordering::Relaxed) != j.view
        {
            return false;
        }
        let Some((pos, _)) = cursor.cursor.batch_ahead() else {
            return false;
        };
        if !pos
            .checked_sub(j.from)
            .and_then(|i| j.rows.get(i))
            .copied()
            .unwrap_or(false)
            || !cursor.cursor.next_batched()
        {
            return false;
        }
        cursor.value = Value::InEntry;
        if let Some(reference) = self.tree().unread_reference(&cursor.cursor) {
            cursor.value = Value::Unread(reference);
        }
        true
    }

    /// Runs the move under way until the plain cursor rests on a row the statement sees, or on
    /// none.
    fn settle(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        cursor: &mut VersionedCursor,
    ) -> Result<IOResult<bool>> {
        loop {
            let moving = cursor.moving.expect("a move under way");
            if let Some((_, chain)) = &mut cursor.chain {
                let found = done!(self.follow_to(txns, versions, txn, chain));
                let (key, _) = cursor.chain.take().expect("a chain");
                match found {
                    Some(StoredValue::Inline(value)) => {
                        return Ok(IOResult::Done(cursor.rest(Shown::Held(key, value, 0))))
                    }
                    Some(StoredValue::OutOfLine(reference)) => {
                        let found = cursor.rest(Shown::Apart(key));
                        cursor.value = Value::Unread(reference);
                        return Ok(IOResult::Done(found));
                    }
                    None => {}
                }
                // No row for this key: on in the move's direction.
                cursor.moving = Some(Move::Step {
                    forward: moving.forward(),
                });
                continue;
            }
            let found = match moving {
                Move::Seek(seek) => {
                    done!(self.tree().seek(&mut cursor.cursor, &cursor.target, seek))
                }
                Move::Step { forward: true } => done!(self.tree().next(&mut cursor.cursor)),
                Move::Step { forward: false } => done!(self.tree().prev(&mut cursor.cursor)),
            };
            if !found {
                return Ok(IOResult::Done(cursor.rest(Shown::None)));
            }
            let value = cursor.cursor.value().expect("an entry");
            let v =
                Version::decode(value.get(..HEADER).unwrap_or(&[])).ok_or_else(corrupt_header)?;
            if !txns.visible(txn, v.slot, v.ts) {
                let key = cursor.cursor.key().expect("an entry").to_vec();
                cursor.chain = Some((key, Chain::from(v)));
                continue;
            }
            if v.deleted {
                cursor.moving = Some(Move::Step {
                    forward: moving.forward(),
                });
                continue;
            }
            // The newest version, shown where the plain cursor holds it.
            let unread = self.tree().unread_reference(&cursor.cursor);
            let found = cursor.rest(Shown::Plain);
            if let Some(reference) = unread {
                cursor.value = Value::Unread(reference);
            }
            return Ok(IOResult::Done(found));
        }
    }
}

impl VersionedTree {
    /// Begins a walk of `cursor` over the versions `txn`'s current statement does not see that
    /// hold their key, at the first. Step it until done; then whether it rests on one.
    pub fn unseen_first(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        cursor: &mut VersionedCursor,
    ) -> Result<IOResult<bool>> {
        // A move under way is this walk's, stepped again after IO.
        if cursor.moving.is_none() {
            cursor.unseen = Some(Unseen::Key { first: true });
            cursor.moving = Some(Move::Step { forward: true });
            cursor.chain = None;
            // An unseen version's pages may go under a later read: read each through its leaf.
            cursor.cursor.set_lazy(false);
        }
        self.walk_unseen(txns, versions, txn, cursor)
    }

    /// The walk's next unseen version holding its key. Step it until done.
    pub fn unseen_next(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        cursor: &mut VersionedCursor,
    ) -> Result<IOResult<bool>> {
        if cursor.moving.is_none() {
            let Some(Unseen::Shown { v }) = cursor.unseen else {
                return Ok(IOResult::Done(false));
            };
            let key = cursor.key().expect("a shown version").to_vec();
            cursor.unseen = Some(Unseen::Below { key, v });
            cursor.moving = Some(Move::Step { forward: true });
            cursor.cursor.set_lazy(false);
        }
        self.walk_unseen(txns, versions, txn, cursor)
    }

    fn walk_unseen(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        cursor: &mut VersionedCursor,
    ) -> Result<IOResult<bool>> {
        loop {
            match cursor.unseen.as_mut().expect("a walk under way") {
                Unseen::Key { first } => {
                    let found = if *first {
                        done!(self.tree().seek(&mut cursor.cursor, &[], Seek::Ge))
                    } else {
                        done!(self.tree().next(&mut cursor.cursor))
                    };
                    if !found {
                        cursor.unseen = None;
                        return Ok(IOResult::Done(cursor.rest(Shown::None)));
                    }
                    let key = cursor.cursor.key().expect("an entry").to_vec();
                    let value = cursor.cursor.value().expect("an entry");
                    let v = Version::decode(value.get(..HEADER).unwrap_or(&[]))
                        .ok_or_else(corrupt_header)?;
                    let value = UnseenValue::Read(value[HEADER..].to_vec());
                    cursor.unseen = Some(Unseen::Judge { key, v, value });
                }
                Unseen::Judge { key, v, value } => {
                    if txns.visible(txn, v.slot, v.ts) {
                        cursor.unseen = Some(Unseen::Key { first: false });
                        continue;
                    }
                    if v.deleted {
                        let (key, v) = (std::mem::take(key), *v);
                        cursor.unseen = Some(Unseen::Below { key, v });
                        continue;
                    }
                    let read = match value {
                        UnseenValue::Read(read) => std::mem::take(read),
                        UnseenValue::OutOfLine(reading) => {
                            // A replaced version's pages stay while a snapshot may read it
                            // (DESIGN.md §6.5): nothing frees them under this read.
                            match done!(reading.step(&self.pool, &|| true)) {
                                Read::Value(read) => read,
                                Read::Changed | Read::Unheld => {
                                    return Err(LimboError::Corrupt(format!(
                                        "the out-of-line value of the version {v:?} is gone"
                                    )))
                                }
                            }
                        }
                    };
                    let (key, v) = (std::mem::take(key), *v);
                    cursor.unseen = Some(Unseen::Shown { v });
                    cursor.moving = None;
                    cursor.chain = None;
                    cursor.value = Value::InEntry;
                    cursor.entry = Shown::Held(key, read, 0);
                    return Ok(IOResult::Done(true));
                }
                Unseen::Below { key, v } => {
                    let v = *v;
                    let Some(earlier) = done!(versions.below(&v)) else {
                        cursor.unseen = Some(Unseen::Key { first: false });
                        continue;
                    };
                    let e = Version::decode(&earlier.head).ok_or_else(corrupt_header)?;
                    let value = match earlier.value {
                        StoredValue::Inline(value) => UnseenValue::Read(value),
                        StoredValue::OutOfLine(reference) => {
                            UnseenValue::OutOfLine(ValueRead::new(reference))
                        }
                    };
                    let key = std::mem::take(key);
                    cursor.unseen = Some(Unseen::Judge { key, v: e, value });
                }
                Unseen::Shown { .. } => unreachable!("a walk resting on a version is moved on"),
            }
        }
    }
}

impl VersionedTree {
    /// Reads the value of the row `cursor` rests on, where its move left it unread. Step it until
    /// done.
    pub fn load(&self, cursor: &mut VersionedCursor) -> Result<IOResult<()>> {
        loop {
            match &mut cursor.value {
                Value::Unread(reference) => {
                    let read = ValueRead::reusing(*reference, std::mem::take(&mut cursor.body));
                    cursor.value = Value::Reading(read);
                }
                Value::Reading(read) => {
                    // The row's version is one the statement sees: nothing frees its pages under
                    // this read.
                    match done!(read.step(&self.pool, &|| true)) {
                        Read::Value(bytes) => {
                            cursor.body = bytes;
                            cursor.value = Value::Read;
                        }
                        Read::Changed | Read::Unheld => {
                            return Err(LimboError::Corrupt(format!(
                                "the out-of-line value {:?} of a version a snapshot reads is gone",
                                read.reference()
                            )))
                        }
                    }
                }
                _ => return Ok(IOResult::Done(())),
            }
        }
    }
}

impl VersionedCursor {
    /// Ends the move at `entry`, or at no row: whether it found one.
    fn rest(&mut self, entry: Shown) -> bool {
        self.moving = None;
        self.chain = None;
        self.value = Value::InEntry;
        match std::mem::replace(&mut self.entry, entry) {
            Shown::Held(k, v, _) => self.spare = (k, v),
            Shown::Apart(k) => self.spare.0 = k,
            _ => {}
        }
        !matches!(self.entry, Shown::None)
    }
}
