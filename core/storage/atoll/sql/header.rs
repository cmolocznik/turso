//! The database header's fields (DESIGN.md §17): rows of the versioned header tree, one per field,
//! so two transactions setting different fields never conflict. A field with no row has its
//! default.
//!
//! Reads outside any transaction read the committed header, which commits publish field by field:
//! a field takes the value of the newest commit that wrote it, whatever order commits publish in.

use super::catalog::System;
use crate::io::IO;
use crate::storage::atoll::btree::Mode;
use crate::storage::atoll::buffer::BufferPool;
use crate::storage::atoll::store::PageStore;
use crate::storage::atoll::txn::versioned::{Get, VersionedTree, Versions, Write, WriteOp};
use crate::storage::atoll::txn::{Transactions, Ts, Txn};
use crate::sync::{Arc, Mutex};
use crate::types::IOResult;
use crate::{LimboError, Result};

/// A field of the database header that Atoll keeps. The page size, the database size, the
/// freelist and the file format's version numbers have other sources (DESIGN.md §17).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    SchemaCookie = 1,
    SchemaFormat = 2,
    DefaultCacheSize = 3,
    TextEncoding = 4,
    UserVersion = 5,
    ApplicationId = 6,
    IncrementalVacuum = 7,
}

impl Field {
    pub const ALL: [Field; 7] = [
        Field::SchemaCookie,
        Field::SchemaFormat,
        Field::DefaultCacheSize,
        Field::TextEncoding,
        Field::UserVersion,
        Field::ApplicationId,
        Field::IncrementalVacuum,
    ];

    /// The value of a field no transaction has written: SQLite's for a new database, schema format
    /// 4 and text encoding 1, UTF-8.
    pub fn default(self) -> u32 {
        match self {
            Field::SchemaFormat => 4,
            Field::TextEncoding => 1,
            _ => 0,
        }
    }

    fn key(self) -> [u8; 1] {
        [self as u8]
    }

    fn index(self) -> usize {
        self as usize - 1
    }
}

/// The header tree of a database.
pub struct Header {
    tree: VersionedTree,
    /// Per field, the value and the commit timestamp of the newest commit that published it.
    committed: [Mutex<(Ts, u32)>; 7],
}

impl Header {
    pub fn open(io: Arc<dyn IO>, pool: Arc<BufferPool>, store: Arc<PageStore>) -> Self {
        Self {
            tree: VersionedTree::open(io, pool, store, System::Header.root()),
            committed: Field::ALL.map(|f| Mutex::new((0, f.default()))),
        }
    }

    /// `field` as `txn`'s current statement reads it. Step it until done.
    pub fn get(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        field: Field,
        op: &mut Get,
    ) -> Result<IOResult<u32>> {
        Ok(match self.tree.get(txns, versions, txn, op)? {
            IOResult::Done(None) => IOResult::Done(field.default()),
            IOResult::Done(Some(v)) => IOResult::Done(decode(field, &v)?),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// A read of `field`, for [`Header::get`].
    pub fn get_op(field: Field) -> Get {
        Get::new(&field.key())
    }

    /// Sets `field` in `txn`: build `op` with [`Header::put_op`]. Step it until done.
    pub fn write(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        op: &mut WriteOp,
    ) -> Result<IOResult<Write>> {
        self.tree.write(txns, versions, txn, op)
    }

    pub fn put_op(field: Field, value: u32) -> WriteOp {
        WriteOp::put(&field.key(), value.to_le_bytes().to_vec(), Mode::Upsert)
    }

    /// The committed value of `field`, for a read outside any transaction.
    pub fn committed(&self, field: Field) -> u32 {
        self.committed[field.index()].lock().1
    }

    /// Publishes `value` of `field`, which the commit at `ts` wrote: it stands unless a newer
    /// commit's value already does.
    pub fn publish(&self, field: Field, ts: Ts, value: u32) {
        let mut cell = self.committed[field.index()].lock();
        if ts > cell.0 {
            *cell = (ts, value);
        }
    }
}

fn decode(field: Field, bytes: &[u8]) -> Result<u32> {
    bytes.try_into().map(u32::from_le_bytes).map_err(|_| {
        LimboError::Corrupt(format!(
            "the header field {field:?} holds {} bytes",
            bytes.len()
        ))
    })
}

#[cfg(test)]
mod tests;
