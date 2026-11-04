//! The system trees and the catalog (DESIGN.md §17).
//!
//! A database's system trees sit at fixed roots, the first page numbers of the 4 KiB class. A new
//! database allocates them first, and the page store hands out its lowest free number
//! ([`PageStore::allocate`](crate::storage::atoll::store::PageStore::allocate)). Opening a
//! database creates whichever is missing, in order. A creation that a crash cut short therefore
//! completes at the same pages: [`Tree::create`] adopts its root in the group that makes it, and
//! recovery frees a page left unadopted.
//!
//! The catalog maps a tree id, which is what `sqlite_schema.rootpage` holds, to the tree's root
//! and layout. It is versioned, so a DDL rolled back leaves no change, and an older snapshot reads
//! the root it saw. Tree 1 is `sqlite_schema`.

use crate::io::IO;
use crate::storage::atoll::btree::{Cursor, Mode, Seek, Tree};
use crate::storage::atoll::buffer::BufferPool;
use crate::storage::atoll::page::{PageId, SizeClass};
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::PageStore;
use crate::storage::atoll::txn::versioned::{Get, VersionedTree, Versions, Write, WriteOp};
use crate::storage::atoll::txn::{Transactions, Ts, Txn};
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};

/// A system tree, by its fixed root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum System {
    /// Tree id to root and layout; versioned.
    Catalog = 0,
    /// The database header's fields, one row each; versioned.
    Header = 1,
    /// Trees dropped by a transaction, to free once no snapshot sees them; versioned.
    Dropped = 2,
    /// Trees whose creation was undone, to free once their transaction has ended; plain.
    Abandoned = 3,
    /// Sequence values, written outside any transaction; plain.
    Sequences = 4,
    /// `sqlite_schema`, tree 1; versioned.
    Schema = 5,
}

impl System {
    /// Every system tree, in the order a new database creates them.
    pub const ALL: [System; 6] = [
        System::Catalog,
        System::Header,
        System::Dropped,
        System::Abandoned,
        System::Sequences,
        System::Schema,
    ];

    /// The class of every system tree's nodes.
    pub fn class() -> SizeClass {
        SizeClass::new(0).expect("the 4 KiB class")
    }

    pub fn root(self) -> PageId {
        PageId::new(Self::class(), self as u64)
    }
}

/// The tree id of `sqlite_schema`.
pub const SCHEMA_ID: u64 = 1;

/// What a tree's keys are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// A rowid table: 8-byte keys.
    Table = 1,
    /// An index: encoded columns, then the rowid where the index has one.
    Index = 2,
    /// A WITHOUT ROWID table: its encoded key columns.
    WithoutRowid = 3,
}

impl Layout {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Layout::Table),
            2 => Some(Layout::Index),
            3 => Some(Layout::WithoutRowid),
            _ => None,
        }
    }
}

/// A tree's catalog row:
///
/// ```text
/// root: u64 | layout: u8 | encoding: u32 | created_by: u64
/// ```
///
/// `encoding` is the key-encoding version of a tree with encoded keys, 0 for a rowid table, and
/// `created_by` the start of the transaction that created a sequence, 0 for any other tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub root: PageId,
    pub layout: Layout,
    pub encoding: u32,
    pub created_by: Ts,
}

const ENTRY: usize = 8 + 1 + 4 + 8;

impl Entry {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(ENTRY);
        out.extend(self.root.raw().to_le_bytes());
        out.push(self.layout as u8);
        out.extend(self.encoding.to_le_bytes());
        out.extend(self.created_by.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bad = || LimboError::Corrupt(format!("a catalog row of {} bytes", bytes.len()));
        if bytes.len() != ENTRY {
            return Err(bad());
        }
        Ok(Self {
            root: PageId::from_raw(u64::from_le_bytes(bytes[0..8].try_into().unwrap())),
            layout: Layout::from_byte(bytes[8]).ok_or_else(bad)?,
            encoding: u32::from_le_bytes(bytes[9..13].try_into().unwrap()),
            created_by: u64::from_le_bytes(bytes[13..21].try_into().unwrap()),
        })
    }
}

/// A tree id as a catalog key: big-endian, so the catalog's order is the ids'.
pub fn key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

/// The catalog of a database.
pub struct Catalog {
    tree: VersionedTree,
}

impl Catalog {
    pub fn open(io: Arc<dyn IO>, pool: Arc<BufferPool>, store: Arc<PageStore>) -> Self {
        Self {
            tree: VersionedTree::open(io, pool, store, System::Catalog.root()),
        }
    }

    pub fn tree(&self) -> &VersionedTree {
        &self.tree
    }

    /// Tree `id`'s row as `txn`'s current statement reads it. Step it until done.
    pub fn get(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        op: &mut Get,
    ) -> Result<IOResult<Option<Entry>>> {
        Ok(match self.tree.get(txns, versions, txn, op)? {
            IOResult::Done(None) => IOResult::Done(None),
            IOResult::Done(Some(v)) => IOResult::Done(Some(Entry::decode(&v)?)),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// Puts tree `id`'s row in `txn`, or deletes it: build `op` with [`Catalog::put_op`] or
    /// [`Catalog::delete_op`]. Step it until done.
    pub fn write(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        op: &mut WriteOp,
    ) -> Result<IOResult<Write>> {
        self.tree.write(txns, versions, txn, op)
    }

    pub fn put_op(id: u64, entry: &Entry, mode: Mode) -> WriteOp {
        WriteOp::put(&key(id), entry.encode(), mode)
    }

    pub fn delete_op(id: u64) -> WriteOp {
        WriteOp::delete(&key(id))
    }

    /// The largest tree id the catalog holds an entry for, whatever its version: a new id is drawn
    /// above it. Step it until done.
    pub fn largest_id(&self, cursor: &mut Cursor) -> Result<IOResult<u64>> {
        Ok(
            match self.tree.tree().seek(cursor, &key(u64::MAX), Seek::Le)? {
                IOResult::Done(false) => IOResult::Done(0),
                IOResult::Done(true) => {
                    let k = cursor.key().expect("an entry");
                    let id = k.try_into().map(u64::from_be_bytes).map_err(|_| {
                        LimboError::Corrupt(format!("a catalog key of {} bytes", k.len()))
                    })?;
                    IOResult::Done(id)
                }
                IOResult::IO(io) => IOResult::IO(io),
            },
        )
    }
}

/// Creates whichever system trees a database lacks, at their fixed roots, in order. A database
/// whose lowest pages hold anything else is refused. Step it until done.
pub fn ensure_system_trees(
    io: &Arc<dyn IO>,
    pool: &Arc<BufferPool>,
    store: &Arc<PageStore>,
) -> Result<IOResult<()>> {
    for system in System::ALL {
        let root = system.root();
        let entry = match store.entry(root)? {
            IOResult::Done(e) => e,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        if entry != FREE {
            continue;
        }
        let made = match Tree::create(io, pool, store, System::class())? {
            IOResult::Done(pid) => pid,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        if made != root {
            return Err(LimboError::Corrupt(format!(
                "the system tree {system:?} was made at {made:?}, not at its root {root:?}"
            )));
        }
    }
    Ok(IOResult::Done(()))
}

#[cfg(test)]
mod tests;
