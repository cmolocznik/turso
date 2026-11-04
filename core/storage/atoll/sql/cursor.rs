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
use crate::storage::atoll::btree::{Mode, Outcome, Put, Remove, Seek};
use crate::storage::atoll::txn::versioned::{VersionedCursor, VersionedTree, Write, WriteOp};
use crate::storage::atoll::txn::Txn;
use crate::storage::btree::BTreeKey;
use crate::sync::{Arc, Mutex};
use crate::types::{
    IOResult, ImmutableRecord, IndexInfo, RecordBuf, SeekKey, SeekOp, SeekResult, ValueRef,
};
use crate::{LimboError, Result};
use std::cmp::Ordering;

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// The transaction a connection's cursors read in: none between transactions.
pub type TxnCell = Arc<Mutex<Option<Txn>>>;

/// A key above every encoded key: no column's encoding begins with 0xff, its first byte being a
/// NULL's (0x01 or 0xfe) or a tag between them, inverted or not.
const ABOVE_ENCODED: [u8; 1] = [0xff];

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
    /// The entry's record, once read.
    record: Option<ImmutableRecord>,
    null_row: bool,
    /// Whether the transaction's current statement created the tree, and writes it plainly.
    plain: bool,
    /// The write under way, kept across IO and waits.
    writing: Option<Writing>,
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
            record: None,
            null_row: false,
            plain: false,
            writing: None,
        }
    }

    /// From now on the cursor's writes are plain: its tree was created by the transaction's
    /// current statement.
    pub fn write_plainly(&mut self) {
        self.plain = true;
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Forgets the record read at the row, to read it again.
    pub fn invalidate_record(&mut self) {
        self.record = None;
    }

    /// The tree's root.
    pub fn root(&self) -> crate::storage::atoll::page::PageId {
        self.entry.root
    }

    /// The tree as plain records, for its rowid counter's seed.
    pub fn plain_tree(&self) -> &crate::storage::atoll::btree::Tree {
        self.tree.tree()
    }

    pub fn index_info(&self) -> Option<&Arc<IndexInfo>> {
        self.index.as_ref()
    }

    fn step(&mut self, to: Move<'_>) -> Result<IOResult<bool>> {
        let mut guard = self.txn.lock();
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
            self.record = None;
            self.null_row = false;
        }
        Ok(moved)
    }

    /// Moves to the first row.
    pub fn rewind(&mut self) -> Result<IOResult<()>> {
        done!(self.step(Move::Seek(&[], Seek::Ge)));
        Ok(IOResult::Done(()))
    }

    /// Moves to the last row.
    pub fn last(&mut self) -> Result<IOResult<()>> {
        let above = match self.entry.layout {
            Layout::Table => key::rowid(i64::MAX).to_vec(),
            _ => ABOVE_ENCODED.to_vec(),
        };
        done!(self.step(Move::Seek(&above, Seek::Le)));
        Ok(IOResult::Done(()))
    }

    pub fn next(&mut self) -> Result<IOResult<()>> {
        done!(self.step(Move::Next));
        Ok(IOResult::Done(()))
    }

    pub fn prev(&mut self) -> Result<IOResult<()>> {
        done!(self.step(Move::Prev));
        Ok(IOResult::Done(()))
    }

    /// Whether the cursor is at no row.
    pub fn is_empty(&self) -> bool {
        self.null_row || self.cursor.key().is_none()
    }

    pub fn set_null_flag(&mut self, flag: bool) {
        self.null_row = flag;
    }

    pub fn get_null_flag(&self) -> bool {
        self.null_row
    }

    /// The row's rowid: a table's key, or an index record's last column where the index has one.
    pub fn rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        if self.is_empty() {
            return Ok(IOResult::Done(None));
        }
        if self.entry.layout == Layout::Table {
            let key = self.cursor.key().expect("a row");
            return Ok(IOResult::Done(Some(key::rowid_of(key)?)));
        }
        if !self.index.as_ref().is_some_and(|i| i.has_rowid) {
            return Ok(IOResult::Done(None));
        }
        let record = self.record_now()?.expect("a row");
        Ok(IOResult::Done(Some(last_rowid(record)?)))
    }

    /// The row's record.
    pub fn record(&mut self) -> Result<IOResult<Option<&ImmutableRecord>>> {
        Ok(IOResult::Done(self.record_now()?))
    }

    fn record_now(&mut self) -> Result<Option<&ImmutableRecord>> {
        if self.is_empty() {
            return Ok(None);
        }
        if self.record.is_none() {
            let value = self.cursor.value().expect("a row");
            self.record = Some(ImmutableRecord::copy_payload(value, RecordBuf::alloc())?);
        }
        Ok(self.record.as_ref())
    }

    /// Moves to the row `op` finds from `key`.
    pub fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        if self.seeking.is_none() {
            self.seeking = Some(self.plan(key, op)?);
        }
        let out = self.seek_planned();
        if !matches!(out, Ok(IOResult::IO(_))) {
            self.seeking = None;
        }
        out
    }

    fn plan(&self, key: SeekKey<'_>, op: SeekOp) -> Result<Seeking> {
        let (prefix, suffix) = match key {
            SeekKey::TableRowId(rowid) => {
                if self.entry.layout != Layout::Table {
                    return Err(LimboError::InternalError(format!(
                        "a rowid seek into tree {}, which is no rowid table",
                        self.id
                    )));
                }
                (key::rowid(rowid).to_vec(), None)
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
                let mut prefix = Vec::new();
                key::encode(columns.iter().cloned(), &info.key_info, &mut prefix)?;
                (prefix, suffix)
            }
        };
        Ok(Seeking {
            op,
            prefix,
            suffix,
            moving_on: false,
        })
    }

    fn seek_planned(&mut self) -> Result<IOResult<SeekResult>> {
        let s = self.seeking.as_ref().expect("a seek");
        let (op, table) = (s.op, self.entry.layout == Layout::Table);
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
        let (p, suffixed) = (s.prefix.clone(), s.suffix.is_some());
        let succ = key::successor(&p);
        // Up to the last key starting with p; with no bound, the last key of all.
        let up_to = || match &succ {
            Some(q) => (q.clone(), Seek::Lt),
            None => (ABOVE_ENCODED.to_vec(), Seek::Le),
        };
        let (target, seek) = if table {
            let seek = match op {
                SeekOp::GE { .. } => Seek::Ge,
                SeekOp::GT => Seek::Gt,
                SeekOp::LE { .. } => Seek::Le,
                SeekOp::LT => Seek::Lt,
            };
            (p.clone(), seek)
        } else {
            match op {
                SeekOp::GE { .. } => (p.clone(), Seek::Ge),
                SeekOp::GT if suffixed => (p.clone(), Seek::Ge),
                SeekOp::GT => match &succ {
                    Some(q) => (q.clone(), Seek::Ge),
                    // Every key starts with the empty prefix: none is above them.
                    None => (ABOVE_ENCODED.to_vec(), Seek::Gt),
                },
                SeekOp::LE { .. } => up_to(),
                SeekOp::LT if suffixed => up_to(),
                SeekOp::LT => (p.clone(), Seek::Lt),
            }
        };
        if !done!(self.step(Move::Seek(&target, seek))) {
            return Ok(IOResult::Done(SeekResult::NotFound));
        }
        match self.judge()? {
            Verdict::Found => Ok(IOResult::Done(SeekResult::Found)),
            Verdict::NotFound => Ok(IOResult::Done(SeekResult::NotFound)),
            Verdict::MoveOn => {
                self.seeking.as_mut().expect("a seek").moving_on = true;
                self.seek_planned()
            }
        }
    }

    /// Judges the entry a seek landed on.
    fn judge(&mut self) -> Result<Verdict> {
        let s = self.seeking.as_ref().expect("a seek");
        let (op, prefix, suffix) = (s.op, s.prefix.clone(), s.suffix);
        let key = self.cursor.key().expect("a landing");
        if self.entry.layout == Layout::Table {
            return Ok(if !op.eq_only() || key == prefix.as_slice() {
                Verdict::Found
            } else {
                Verdict::NotFound
            });
        }
        let named = key.starts_with(&prefix);
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
    /// The key, value and put mode of the entry `key` names.
    fn entry_of(&self, key: &BTreeKey<'_>) -> Result<(Vec<u8>, Vec<u8>, Mode)> {
        match key {
            BTreeKey::TableRowId((rowid, record)) => {
                let record = record.ok_or_else(|| {
                    LimboError::InternalError("a table row written with no record".into())
                })?;
                Ok((
                    key::rowid(*rowid).to_vec(),
                    record.get_payload().to_vec(),
                    Mode::Upsert,
                ))
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
                let mut k = Vec::new();
                key::encode(columns.iter().cloned(), &info.key_info, &mut k)?;
                let mode = if suffixless {
                    Mode::Insert
                } else {
                    Mode::Upsert
                };
                Ok((k, record.get_payload().to_vec(), mode))
            }
        }
    }

    /// Writes the row or index entry `key` names. Step it until done; a write behind another
    /// transaction comes back as [`Write::Wait`] or [`Write::Conflict`].
    pub fn insert(&mut self, key: &BTreeKey<'_>) -> Result<IOResult<Write>> {
        if self.writing.is_none() {
            let (k, value, mode) = self.entry_of(key)?;
            let max = self.tree.max_key();
            if k.len() > max {
                return Err(LimboError::InternalError(format!(
                    "a key of {} bytes into tree {}, which holds keys of at most {max}",
                    k.len(),
                    self.id
                )));
            }
            self.writing = Some(if self.plain {
                let guard = self.txn.lock();
                let txn = guard.as_ref().ok_or_else(outside)?;
                Writing::PlainPut(self.tree.plain_put(txn, &k, value, mode), mode)
            } else {
                Writing::Versioned(WriteOp::put(&k, value, mode), mode)
            });
        }
        self.write()
    }

    /// Deletes the row or index entry the cursor is at. Step it until done.
    pub fn delete(&mut self) -> Result<IOResult<Write>> {
        if self.writing.is_none() {
            let k = self
                .cursor
                .key()
                .filter(|_| !self.null_row)
                .ok_or_else(|| LimboError::InternalError("a delete at no row".into()))?
                .to_vec();
            self.writing = Some(if self.plain {
                Writing::PlainRemove(Remove::new(&k))
            } else {
                Writing::Versioned(WriteOp::delete(&k), Mode::Update)
            });
        }
        self.write()
    }

    fn write(&mut self) -> Result<IOResult<Write>> {
        let mut guard = self.txn.lock();
        let txn = guard.as_mut().ok_or_else(outside)?;
        let (txns, versions) = (self.db.atoll().transactions(), self.db.versions());
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
        if let Write::Wait(_) = write {
            // Kept: stepped again once the writer has ended.
            return Ok(IOResult::Done(write));
        }
        self.writing = None;
        self.record = None;
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
