//! A cursor over a versioned tree as a transaction's statement sees it (DESIGN.md §11.2, §17).
//!
//! It moves a plain [`Cursor`], which copies each leaf entry out whole, head and value, a value out
//! of line read through the leaf that holds it. Each entry is judged by its newest version: one the
//! statement sees is its row, or no row if it is a tombstone; one it does not is followed down its
//! chain ([`VersionedTree::follow`]) to the version it does. A key with no version the statement
//! sees, or whose version is a tombstone, is passed over in the direction of the move. Like the
//! plain cursor it holds no latch between steps.

use super::{corrupt_header, Chain, Transactions, Txn, Version, VersionedTree, Versions, HEADER};
use crate::storage::atoll::btree::{Cursor, Seek};
use crate::types::IOResult;
use crate::Result;

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
    entry: Option<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone)]
enum Move {
    /// The plain cursor's seek, not yet done.
    Seek(Seek, Vec<u8>),
    /// On past the plain cursor's entry, forward or backward.
    Step { forward: bool },
}

impl Move {
    fn forward(&self) -> bool {
        match self {
            Move::Seek(seek, _) => matches!(seek, Seek::Ge | Seek::Gt),
            Move::Step { forward } => *forward,
        }
    }
}

impl VersionedCursor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.entry.as_ref().map(|(k, _)| k.as_slice())
    }

    /// The value of the version the statement sees, without its header.
    pub fn value(&self) -> Option<&[u8]> {
        self.entry.as_ref().map(|(_, v)| v.as_slice())
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
            cursor.moving = Some(Move::Seek(seek, key.to_vec()));
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
            if cursor.entry.is_none() {
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
            let moving = cursor.moving.clone().expect("a move under way");
            if let Some((key, chain)) = &mut cursor.chain {
                let found = done!(self.follow(txns, versions, txn, key, chain));
                let (key, _) = cursor.chain.take().expect("a chain");
                if let Some(value) = found {
                    return Ok(IOResult::Done(cursor.rest(Some((key, value)))));
                }
                // No row for this key: on in the move's direction.
                cursor.moving = Some(Move::Step {
                    forward: moving.forward(),
                });
                continue;
            }
            let found = match &moving {
                Move::Seek(seek, key) => done!(self.tree().seek(&mut cursor.cursor, key, *seek)),
                Move::Step { forward: true } => done!(self.tree().next(&mut cursor.cursor)),
                Move::Step { forward: false } => done!(self.tree().prev(&mut cursor.cursor)),
            };
            if !found {
                return Ok(IOResult::Done(cursor.rest(None)));
            }
            let key = cursor.cursor.key().expect("an entry").to_vec();
            let value = cursor.cursor.value().expect("an entry");
            let v =
                Version::decode(value.get(..HEADER).unwrap_or(&[])).ok_or_else(corrupt_header)?;
            if !txns.visible(txn, v.slot, v.ts) {
                cursor.chain = Some((key, Chain::from(v)));
                continue;
            }
            if v.deleted() {
                cursor.moving = Some(Move::Step {
                    forward: moving.forward(),
                });
                continue;
            }
            let value = value[HEADER..].to_vec();
            return Ok(IOResult::Done(cursor.rest(Some((key, value)))));
        }
    }
}

impl VersionedCursor {
    /// Ends the move at `entry`, or at no row: whether it found one.
    fn rest(&mut self, entry: Option<(Vec<u8>, Vec<u8>)>) -> bool {
        self.moving = None;
        self.chain = None;
        self.entry = entry;
        self.entry.is_some()
    }
}
