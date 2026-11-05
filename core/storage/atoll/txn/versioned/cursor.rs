//! A cursor over a versioned tree as a transaction's statement sees it (DESIGN.md §11.2, §17).
//!
//! It moves a plain [`Cursor`], which copies each leaf entry out whole, head and value, a value out
//! of line read through the leaf that holds it. Each entry is judged by its newest version: one the
//! statement sees is its row, or no row if it is a tombstone; one it does not is followed down its
//! chain ([`VersionedTree::follow`]) to the version it does. A key with no version the statement
//! sees, or whose version is a tombstone, is passed over in the direction of the move. Like the
//! plain cursor it holds no latch between steps.
//!
//! Walking unseen ([`VersionedTree::unseen_first`]), it rests instead on each version the
//! statement does not see that holds its key, newest first within a key and keys in order, with
//! that version's value as the row: what R10's guard of a child table with no index on its
//! foreign key reads, so its scan compares each such version as the probe compares a row.

use super::{corrupt_header, Chain, Transactions, Txn, Version, VersionedTree, Versions, HEADER};
use crate::storage::atoll::btree::value::{Read, ValueRead};
use crate::storage::atoll::btree::{Cursor, Near, Seek, StoredValue};
use crate::types::IOResult;
use crate::{LimboError, Result};

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// A position in a versioned tree: a key and the value of the version the statement sees.
#[derive(Default)]
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

impl VersionedCursor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn key(&self) -> Option<&[u8]> {
        match &self.entry {
            Shown::None => None,
            Shown::Plain => self.cursor.key(),
            Shown::Held(k, _, _) => Some(k),
        }
    }

    /// The value of the version the statement sees, without its header.
    pub fn value(&self) -> Option<&[u8]> {
        match &self.entry {
            Shown::None => None,
            Shown::Plain => self.cursor.value().and_then(|v| v.get(HEADER..)),
            Shown::Held(_, v, from) => v.get(*from..),
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
        match std::mem::replace(&mut self.entry, Shown::Held(key, entry, from)) {
            Shown::Held(k, v, _) => (k, v),
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
        }
        self.settle(txns, versions, txn, cursor)
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
                let found = done!(self.follow(txns, versions, txn, chain));
                let (key, _) = cursor.chain.take().expect("a chain");
                if let Some(value) = found {
                    return Ok(IOResult::Done(cursor.rest(Shown::Held(key, value, 0))));
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
            if v.deleted() {
                cursor.moving = Some(Move::Step {
                    forward: moving.forward(),
                });
                continue;
            }
            // The newest version, shown where the plain cursor holds it.
            return Ok(IOResult::Done(cursor.rest(Shown::Plain)));
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
                    if v.deleted() {
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

impl VersionedCursor {
    /// Ends the move at `entry`, or at no row: whether it found one.
    fn rest(&mut self, entry: Shown) -> bool {
        self.moving = None;
        self.chain = None;
        if let Shown::Held(k, v, _) = std::mem::replace(&mut self.entry, entry) {
            self.spare = (k, v);
        }
        !matches!(self.entry, Shown::None)
    }
}
