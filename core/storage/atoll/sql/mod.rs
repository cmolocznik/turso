//! Turso's SQL on Atoll (DESIGN.md §17).
//!
//! - [`catalog`]: the system trees at their fixed roots, and the catalog of tree ids.
//! - [`key`]: keys of SQL trees as byte strings in Turso's order.
//! - [`header`]: the database header's fields as rows.
//! - [`rowid`]: NewRowid's counters.

pub mod catalog;
pub mod header;
pub mod key;
pub mod rowid;

use self::catalog::{ensure_system_trees, key, Catalog, Entry, Layout, System, SCHEMA_ID};
use self::header::{Field, Header};
use self::rowid::Rowids;
use super::btree::{Cursor, Mode};
use super::db::{Atoll, AtollConfig};
use super::drive;
use super::txn::versioned::{Get, Versions, Write};
use super::txn::{Commit, Isolation};
use crate::io::IO;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::{LimboError, Result};

/// An Atoll database as Turso's SQL layer uses it.
pub struct AtollDb {
    io: Arc<dyn IO>,
    atoll: Atoll,
    versions: Versions,
    catalog: Catalog,
    header: Header,
    rowids: Rowids,
    /// The next tree id to draw, above every id the catalog held at open and every one drawn
    /// since. Not transactional: an id a rolled-back transaction drew is not drawn again while the
    /// database is open, and may be after a restart, when nothing durable names it.
    next_id: AtomicU64,
}

/// A database closed cleanly leaves its log durable to its end: the writes no commit flushed, a
/// sequence's or a drain's, survive the close (DESIGN.md §17, R8).
impl Drop for AtollDb {
    fn drop(&mut self) {
        let log = self.atoll.log().clone();
        let end = log.end();
        if let Err(e) = drive(&self.io, || log.wait_durable(end)) {
            tracing::error!("flushing the log of a database closing failed: {e}");
        }
    }
}

impl AtollDb {
    /// Opens the database at `path`, creating it, and its system trees, if it does not exist, and
    /// completing a creation a crash cut short.
    pub fn open(io: Arc<dyn IO>, path: &str, config: AtollConfig) -> Result<Self> {
        let slots = config.txn.slots;
        let atoll = Atoll::open(io.clone(), path, config)?;
        drive(&io, || {
            ensure_system_trees(&io, atoll.pool(), atoll.store())
        })?;
        let versions = Versions::new(
            io.clone(),
            atoll.pool().clone(),
            atoll.store().clone(),
            slots,
        );
        let catalog = Catalog::open(io.clone(), atoll.pool().clone(), atoll.store().clone());
        let header = Header::open(io.clone(), atoll.pool().clone(), atoll.store().clone());
        let db = Self {
            io,
            atoll,
            versions,
            catalog,
            header,
            rowids: Rowids::default(),
            next_id: AtomicU64::new(0),
        };
        db.ensure_schema_row()?;
        db.load_committed_header()?;
        let mut cursor = Cursor::new();
        let largest = drive(&db.io, || db.catalog.largest_id(&mut cursor))?;
        db.next_id
            .store(largest.max(SCHEMA_ID) + 1, Ordering::SeqCst);
        Ok(db)
    }

    /// Writes `sqlite_schema`'s catalog row, in a transaction of its own, unless it is there.
    fn ensure_schema_row(&self) -> Result<()> {
        let txns = self.atoll.transactions();
        let mut txn = drive(&self.io, || txns.begin(Isolation::Snapshot))?;
        txns.statement(&mut txn)?;
        let mut get = Get::new(&key(SCHEMA_ID));
        let have = drive(&self.io, || {
            self.catalog.get(txns, &self.versions, &mut txn, &mut get)
        })?;
        if have.is_some() {
            txns.abort(txn);
            return Ok(());
        }
        let entry = Entry {
            root: System::Schema.root(),
            layout: Layout::Table,
            encoding: 0,
            created_by: 0,
        };
        let mut op = Catalog::put_op(SCHEMA_ID, &entry, Mode::Insert);
        let write = drive(&self.io, || {
            self.catalog.write(txns, &self.versions, &mut txn, &mut op)
        })?;
        if !matches!(write, Write::Done(_)) {
            return Err(LimboError::InternalError(format!(
                "writing sqlite_schema's catalog row at open came to {write:?}"
            )));
        }
        let mut commit = Commit::new(txn);
        drive(&self.io, || txns.commit(&mut commit))?;
        Ok(())
    }

    /// Reads every header field at a snapshot taken now and publishes it as committed, at that
    /// transaction's start, below every commit to come.
    fn load_committed_header(&self) -> Result<()> {
        let txns = self.atoll.transactions();
        let mut txn = drive(&self.io, || txns.begin(Isolation::Snapshot))?;
        txns.statement(&mut txn)?;
        for field in Field::ALL {
            let mut get = Header::get_op(field);
            let value = drive(&self.io, || {
                self.header
                    .get(txns, &self.versions, &mut txn, field, &mut get)
            })?;
            self.header.publish(field, txn.start(), value);
        }
        txns.abort(txn);
        Ok(())
    }

    pub fn atoll(&self) -> &Atoll {
        &self.atoll
    }

    pub fn versions(&self) -> &Versions {
        &self.versions
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn rowids(&self) -> &Rowids {
        &self.rowids
    }

    /// A new tree id, never drawn before while the database has been open.
    pub fn draw_tree_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}
