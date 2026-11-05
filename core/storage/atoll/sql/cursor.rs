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
use crate::storage::atoll::txn::versioned::{
    Guard, GuardOp, Guarded, VersionedCursor, VersionedTree, Write, WriteOp, HEADER,
};
use crate::storage::atoll::txn::Txn;
use crate::storage::btree::BTreeKey;
use crate::sync::{Arc, Mutex};
use crate::types::{
    IOResult, ImmutableRecord, IndexInfo, RecordBuf, SeekKey, SeekOp, SeekResult, ValueRef,
};
use crate::{LimboError, Result};
use std::borrow::Cow;
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
    record: Option<ImmutableRecord>,
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
            record: None,
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
    /// walk reaches next.
    pub fn unseen_hit(&self) -> Guard {
        let Some(v) = self.cursor.unseen() else {
            return Guard::Clear;
        };
        match self.db.atoll().transactions().writer(v.slot, v.ts) {
            crate::storage::atoll::txn::Writer::Open => {
                Guard::Wait(crate::storage::atoll::txn::Blocker {
                    slot: v.slot,
                    start: v.ts,
                })
            }
            crate::storage::atoll::txn::Writer::Committed => Guard::Conflict,
            crate::storage::atoll::txn::Writer::Gone => Guard::Clear,
        }
    }

    fn step_unseen(&mut self, first: bool) -> Result<IOResult<()>> {
        let mut guard = self.txn.lock();
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
        self.record = None;
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
        self.record = None;
        self.null_row = false;
        self.writing = None;
        self.left = None;
        self.refreshing = None;
        self.guarding = None;
        self.unseen = false;
        let near = self.cursor.near();
        self.cursor.rest_nowhere(near);
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
            self.left = None;
        }
        Ok(moved)
    }

    /// Moves to the first row.
    pub fn rewind(&mut self) -> Result<IOResult<()>> {
        if self.unseen {
            return self.step_unseen(true);
        }
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

    /// Moves to the next row: from an entry found gone, the one after it, where it already stands.
    pub fn next(&mut self) -> Result<IOResult<()>> {
        if self.unseen {
            return self.step_unseen(false);
        }
        if self.left.take().is_some() {
            self.record = None;
            return Ok(IOResult::Done(()));
        }
        done!(self.step(Move::Next));
        Ok(IOResult::Done(()))
    }

    /// Moves to the previous row: from an entry found gone, the last one before its key.
    pub fn prev(&mut self) -> Result<IOResult<()>> {
        if let Some(key) = self.left.take() {
            if let IOResult::IO(io) = self.step(Move::Seek(&key, Seek::Lt))? {
                self.left = Some(key);
                return Ok(IOResult::IO(io));
            }
            return Ok(IOResult::Done(()));
        }
        done!(self.step(Move::Prev));
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
        if let IOResult::IO(io) = self.step(Move::Seek(&key, Seek::Ge))? {
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
        let out = self.seek_planned(&mut s);
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
                let mut guard = self.txn.lock();
                let txn = guard.as_mut().ok_or_else(outside)?;
                v.deleted && self.db.atoll().transactions().visible(txn, v.slot, v.ts)
            }
        };
        if absent {
            let near = kept.near();
            self.cursor.rest_nowhere(near);
            self.record = None;
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
        })
    }

    fn seek_planned(&mut self, s: &mut Seeking) -> Result<IOResult<SeekResult>> {
        loop {
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
            let kept = self.kept.take().filter(|_| self.probed == k);
            self.writing = Some(if self.plain {
                let guard = self.txn.lock();
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
        self.write()
    }

    /// A write path's probe of `key`, judged by its newest version as a write would judge it
    /// (A3.12, A4.9; [`VersionedTree::probe`]). Step it until done.
    pub fn probe(
        &mut self,
        key: &[u8],
    ) -> Result<IOResult<crate::storage::atoll::txn::versioned::Probe>> {
        let mut lock = self.txn.lock();
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
        let mut lock = self.txn.lock();
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
                Writing::Versioned(
                    WriteOp::delete_into(k, entry).near(self.cursor.near()),
                    Mode::Update,
                )
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
        self.record = None;
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
