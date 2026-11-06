//! Turso's SQL on Atoll (DESIGN.md §17).
//!
//! - [`catalog`]: the system trees at their fixed roots, and the catalog of tree ids.
//! - [`cursor`]: AtollCursor, a Turso cursor over one SQL tree.
//! - [`key`]: keys of SQL trees as byte strings in Turso's order.
//! - [`header`]: the database header's fields as rows.
//! - [`rowid`]: NewRowid's counters.
//! - [`lifecycle`]: trees created, dropped and cleared in a transaction, and their drain.
//! - [`mode`]: Atoll mode, the interface Turso's sites call.
//! - [`sequence`]: sequence values, changed outside any transaction.
//! - [`trees`]: per-tree commit stamps, for `BusySnapshot`.
//! - [`wait`]: the wait graph, over every transaction group of the process.

pub mod catalog;
pub mod cursor;
pub mod header;
pub mod integrity;
pub mod key;
pub mod lifecycle;
pub mod mode;
pub mod rowid;
pub mod sequence;
pub mod trees;
pub mod wait;

use self::catalog::{ensure_system_trees, key, Catalog, Entry, Layout, System, SCHEMA_ID};
use self::header::{Field, Header};
use self::rowid::Rowids;
use super::btree::{Cursor, Drain, Mode};
use super::db::{Atoll, AtollConfig};
use super::drive;
use super::page::PageId;
use super::txn::versioned::{Get, VersionedTree, Versions, Write};
use super::txn::{Commit, Isolation, Taken};
use crate::io::IO;
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::{LimboError, Result};

/// An Atoll database as Turso's SQL layer uses it.
pub struct AtollDb {
    io: Arc<dyn IO>,
    atoll: Atoll,
    versions: Versions,
    catalog: Catalog,
    header: Header,
    rowids: Rowids,
    /// The dropped list, written by transactions.
    dropped: VersionedTree,
    stamps: self::trees::TreeStamps,
    /// Trees handed over to drain, the one being drained, and whether a drainer is at work.
    draining: Mutex<Vec<PageId>>,
    current: Mutex<Option<(PageId, Drain, bool)>>,
    drainer: AtomicBool,
    /// The next tree id to draw, above every id the catalog held at open and every one drawn
    /// since. Not transactional: an id a rolled-back transaction drew is not drawn again while the
    /// database is open, and may be after a restart, when nothing durable names it.
    next_id: AtomicU64,
    /// The slot a begin next collects beside its own, in turn.
    collect_next: std::sync::atomic::AtomicUsize,
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
    /// Opens the database at `path`, creating it, and its system trees, if it does not exist,
    /// completing a creation a crash cut short, and draining every tree on the abandoned and
    /// dropped lists.
    pub fn open(io: Arc<dyn IO>, path: &str, config: AtollConfig) -> Result<Self> {
        let slots = config.txn.slots;
        let atoll = Atoll::open(io.clone(), path, config)?;
        // Turso's SQL writes only under the write gate (DESIGN.md §17), its open's own included.
        atoll.transactions().require_gate();
        let mut made = false;
        drive(&io, || {
            ensure_system_trees(atoll.pool(), atoll.store(), &mut made)
        })?;
        let versions = Versions::new(
            io.clone(),
            atoll.pool().clone(),
            atoll.store().clone(),
            slots,
        );
        // System trees made, for a database made from nothing or one a crash left before its first
        // checkpoint, are in a checkpoint before any write is admitted: replay finds every tree it
        // writes (audit 18, review 9).
        if made {
            let checkpointer = atoll.checkpointer();
            let ticket = checkpointer.request_full();
            drive(&io, || checkpointer.step_full(ticket, atoll.transactions()))?;
        }
        let catalog = Catalog::open(io.clone(), atoll.pool().clone(), atoll.store().clone());
        let header = Header::open(io.clone(), atoll.pool().clone(), atoll.store().clone());
        let dropped = VersionedTree::open(
            io.clone(),
            atoll.pool().clone(),
            atoll.store().clone(),
            System::Dropped.root(),
        );
        let db = Self {
            io,
            atoll,
            versions,
            catalog,
            header,
            rowids: Rowids::default(),
            dropped,
            stamps: self::trees::TreeStamps::default(),
            draining: Mutex::new(Vec::new()),
            current: Mutex::new(None),
            drainer: AtomicBool::new(false),
            next_id: AtomicU64::new(0),
            collect_next: std::sync::atomic::AtomicUsize::new(0),
        };
        db.drain_lists()?;
        db.drop_unnamed_sequences()?;
        db.ensure_schema_row()?;
        db.load_committed_header()?;
        let mut cursor = Cursor::new();
        let largest = drive(&db.io, || db.catalog.largest_id(&mut cursor))?;
        db.next_id
            .store(largest.max(SCHEMA_ID) + 1, Ordering::SeqCst);
        Ok(db)
    }

    /// Writes `sqlite_schema`'s catalog row, in a transaction of its own holding the write gate
    /// exclusive, as every schema writer does, unless it is there.
    fn ensure_schema_row(&self) -> Result<()> {
        let txns = self.atoll.transactions();
        let mut txn = drive(&self.io, || txns.begin(Isolation::Snapshot))?;
        txns.statement(&mut txn)?;
        let taken = txns.take_exclusive(&mut txn, &crate::io::Completion::new_wait())?;
        if taken != Taken::Done {
            return Err(LimboError::InternalError(format!(
                "the write gate at open, with nothing else open, came to {taken:?}"
            )));
        }
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

    pub fn io(&self) -> &Arc<dyn IO> {
        &self.io
    }

    pub fn atoll(&self) -> &Atoll {
        &self.atoll
    }

    pub fn versions(&self) -> &Versions {
        &self.versions
    }

    /// A begin's step of the version arenas' collection (DESIGN.md §11.5): a page of the slot it
    /// took and one of the next slot in turn. Nothing waits: a step that would read a page in
    /// starts the read and leaves the page to the next begin.
    pub(crate) fn collect_after_begin(&self, slot: u16) -> Result<()> {
        let txns = self.atoll.transactions();
        let other = self
            .collect_next
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % txns.slot_count();
        for s in [slot, other as u16] {
            let _freed = self.versions.collect(txns, s)?;
        }
        Ok(())
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

    pub fn dropped(&self) -> &VersionedTree {
        &self.dropped
    }

    pub fn stamps(&self) -> &self::trees::TreeStamps {
        &self.stamps
    }

    /// A new tree id, never drawn before while the database has been open.
    pub fn draw_tree_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}
