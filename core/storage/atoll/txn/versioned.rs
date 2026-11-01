//! Versioned trees (DESIGN.md §11.2, §11.3).
//!
//! Every entry starts with a version header, kept in the leaf:
//!
//! ```text
//! slot: u16 | ts: u64 | command: u16 | updates: u8 | flags: u8
//! ```
//!
//! naming the transaction that wrote the version (its slot and start) and the statement within it.
//! The leaf holds the newest version. Each slot keeps a version index per node class: a temporary
//! tree of the versions its transactions replaced, keyed
//!
//! ```text
//! ts | tree | key | command
//! ```
//!
//! by the version that replaced it: the replacing transaction's start, the tree's root, the key,
//! and the replacing statement. A reader starts from the newest version and, while it is not
//! visible, follows the chain into the writer's version index. The statement is in the key so that
//! a statement changing a row an earlier statement of its transaction changed keeps that earlier
//! version, which it still reads.
//!
//! A writer judges the newest version first: another slot's open transaction makes it wait, and,
//! under snapshot isolation, one committed after its snapshot is a conflict. It then puts the
//! version it replaces into its version index, and writes over the newest version only if that is
//! still the one it judged.

use super::{Isolation, Transactions, Ts, Txn, Writer};
use crate::io::IO;
use crate::storage::atoll::btree::{Lookup, Mode, Outcome, Put, Stored, StoredValue, Tree};
use crate::storage::atoll::buffer::BufferPool;
use crate::storage::atoll::page::{PageId, SizeClass, CLASS_COUNT};
use crate::storage::atoll::store::PageStore;
use crate::sync::{Arc, Mutex};
use crate::types::IOResult;
use crate::{LimboError, Result};

/// Bytes of the version header.
pub const HEADER: usize = 14;

/// The largest node class (64 KiB, the most 16-bit slots address).
const MAX_NODE_CLASS: usize = 4;

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    pub slot: u16,
    pub ts: Ts,
    pub command: u16,
    /// Updates while the oldest snapshot stayed the same: what turns a tuple fat.
    pub updates: u8,
    pub flags: u8,
}

impl Version {
    /// The key was deleted: this version says it is absent.
    pub const DELETED: u8 = 1;
    /// The key had no entry before this version: there is no earlier one to follow.
    pub const FIRST: u8 = 2;

    pub fn encode(&self) -> [u8; HEADER] {
        let mut out = [0u8; HEADER];
        out[0..2].copy_from_slice(&self.slot.to_le_bytes());
        out[2..10].copy_from_slice(&self.ts.to_le_bytes());
        out[10..12].copy_from_slice(&self.command.to_le_bytes());
        out[12] = self.updates;
        out[13] = self.flags;
        out
    }

    pub fn decode(head: &[u8]) -> Option<Self> {
        let head: &[u8; HEADER] = head.try_into().ok()?;
        Some(Self {
            slot: u16::from_le_bytes([head[0], head[1]]),
            ts: u64::from_le_bytes(head[2..10].try_into().unwrap()),
            command: u16::from_le_bytes([head[10], head[11]]),
            updates: head[12],
            flags: head[13],
        })
    }

    fn deleted(&self) -> bool {
        self.flags & Self::DELETED != 0
    }

    fn first(&self) -> bool {
        self.flags & Self::FIRST != 0
    }
}

/// A version index entry's first byte: the version replaced was no entry at all.
const ABSENT: u8 = 1;

/// The version index key of the version replaced by `(ts, command)`'s write of `key` in `tree`.
fn index_key(ts: Ts, tree: PageId, key: &[u8], command: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(18 + key.len());
    out.extend_from_slice(&ts.to_be_bytes());
    out.extend_from_slice(&tree.raw().to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&command.to_be_bytes());
    out
}

/// A replaced version as its index entry holds it: none, or its header and inline value.
fn encode_entry(stored: Option<&Stored>) -> Result<Vec<u8>> {
    let Some(s) = stored else {
        return Ok(vec![ABSENT]);
    };
    let StoredValue::Inline(value) = &s.value else {
        return Err(out_of_line());
    };
    let mut out = vec![0];
    out.extend_from_slice(&s.head);
    out.extend_from_slice(value);
    Ok(out)
}

fn decode_entry(entry: &[u8]) -> Result<Option<Stored>> {
    let corrupt = || LimboError::Corrupt("a version index entry does not decode".into());
    match entry.first() {
        Some(&ABSENT) => Ok(None),
        Some(0) if entry.len() > HEADER => Ok(Some(Stored {
            head: entry[1..1 + HEADER].to_vec(),
            value: StoredValue::Inline(entry[1 + HEADER..].to_vec()),
        })),
        _ => Err(corrupt()),
    }
}

fn out_of_line() -> LimboError {
    LimboError::InternalError(
        "a versioned value too long for its leaf: versions of values out of line need the pages \
         they replace kept until no snapshot reads them"
            .into(),
    )
}

/// The version indexes of every slot.
pub struct Versions {
    io: Arc<dyn IO>,
    pool: Arc<BufferPool>,
    store: Arc<PageStore>,
    /// Per slot and node class, the root of its version index, once created.
    roots: Box<[Mutex<[Option<PageId>; CLASS_COUNT]>]>,
}

impl Versions {
    pub fn new(
        io: Arc<dyn IO>,
        pool: Arc<BufferPool>,
        store: Arc<PageStore>,
        slots: usize,
    ) -> Self {
        Self {
            io,
            pool,
            store,
            roots: (0..slots)
                .map(|_| Mutex::new([None; CLASS_COUNT]))
                .collect(),
        }
    }

    /// The node class of the version index for trees of `class`: one larger, so its keys hold a
    /// tree's longest key and the 18 bytes before and after it.
    fn index_class(class: SizeClass) -> SizeClass {
        SizeClass::new((class.index() + 1).min(MAX_NODE_CLASS)).expect("a class")
    }

    /// `slot`'s version index for trees of `class`, created if it has none.
    fn index(&self, slot: u16, class: SizeClass) -> Result<IOResult<Tree>> {
        let class = Self::index_class(class);
        let existing = self.roots[slot as usize].lock()[class.index()];
        let root = match existing {
            Some(root) => root,
            None => {
                let root = done!(Tree::create_temporary(
                    &self.io,
                    &self.pool,
                    &self.store,
                    class
                ));
                let mut roots = self.roots[slot as usize].lock();
                match roots[class.index()] {
                    // Created meanwhile by another reader: this one is never used.
                    Some(other) => {
                        self.store.orphaned(root);
                        other
                    }
                    None => {
                        roots[class.index()] = Some(root);
                        root
                    }
                }
            }
        };
        Ok(IOResult::Done(Tree::open(
            self.io.clone(),
            self.pool.clone(),
            self.store.clone(),
            root,
        )))
    }
}

/// A tree whose entries are versioned.
pub struct VersionedTree {
    tree: Tree,
}

/// What a write came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    Done(Outcome),
    /// The newest version is another transaction's, still open, in this slot: wait for it to end
    /// and write again.
    Wait(u16),
    /// The newest version was committed after the snapshot: under snapshot isolation the
    /// transaction cannot write it.
    Conflict,
}

impl VersionedTree {
    pub fn open(
        io: Arc<dyn IO>,
        pool: Arc<BufferPool>,
        store: Arc<PageStore>,
        root: PageId,
    ) -> Self {
        Self {
            tree: Tree::open(io, pool, store, root).with_head(HEADER),
        }
    }

    pub fn root(&self) -> PageId {
        self.tree.root()
    }

    pub fn tree(&self) -> &Tree {
        &self.tree
    }

    /// The longest key: the tree's, and what its version index keys hold.
    pub fn max_key(&self) -> usize {
        let index = Versions::index_class(self.tree.root().class());
        let index_max = crate::storage::atoll::btree::node::Limits::of(index).max_key - 18;
        self.tree.limits().max_key.min(index_max)
    }

    /// The value of `key` that `txn`'s current statement reads. Step it until done.
    pub fn get(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        key: &[u8],
    ) -> Result<IOResult<Option<Vec<u8>>>> {
        let Some(mut stored) = done!(self.tree.stored(key)) else {
            return Ok(IOResult::Done(None));
        };
        loop {
            let v = Version::decode(&stored.head).ok_or_else(corrupt_header)?;
            if txns.visible(txn, v.slot, v.ts, v.command) {
                if v.deleted() {
                    return Ok(IOResult::Done(None));
                }
                return match stored.value {
                    StoredValue::Inline(value) => Ok(IOResult::Done(Some(value))),
                    StoredValue::OutOfLine(_) => Err(out_of_line()),
                };
            }
            if v.first() {
                return Ok(IOResult::Done(None));
            }
            let index = done!(versions.index(v.slot, self.tree.root().class()));
            let mut lookup = Lookup::new(&index_key(v.ts, self.root(), key, v.command));
            let Some(entry) = done!(index.lookup(&mut lookup)) else {
                return Err(LimboError::Corrupt(format!(
                    "the version {v:?} of a key replaced nothing in its version index"
                )));
            };
            match decode_entry(&entry)? {
                Some(earlier) => {
                    if Version::decode(&earlier.head) == Some(v) {
                        return Err(LimboError::Corrupt(format!(
                            "the version {v:?} replaced itself in its version index"
                        )));
                    }
                    stored = earlier;
                }
                None => return Ok(IOResult::Done(None)),
            }
        }
    }

    /// Puts `key`'s value in `txn`. Step it until done.
    pub fn put(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        key: &[u8],
        value: &[u8],
        mode: Mode,
    ) -> Result<IOResult<Write>> {
        self.write(txns, versions, txn, key, Some(value), mode)
    }

    /// Deletes `key` in `txn`: [`Outcome::Updated`] if `txn` read it present, else
    /// [`Outcome::Unchanged`]. Step it until done.
    pub fn delete(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        key: &[u8],
    ) -> Result<IOResult<Write>> {
        self.write(txns, versions, txn, key, None, Mode::Update)
    }

    fn write(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        key: &[u8],
        value: Option<&[u8]>,
        mode: Mode,
    ) -> Result<IOResult<Write>> {
        assert!(key.len() <= self.max_key(), "a key of {} bytes", key.len());
        if let Some(v) = value {
            if !self.tree.limits().inline(key.len(), HEADER + v.len()) {
                return Err(out_of_line());
            }
        }
        loop {
            let stored = done!(self.tree.stored(key));
            let newest = match &stored {
                None => None,
                Some(s) => Some(Version::decode(&s.head).ok_or_else(corrupt_header)?),
            };
            let own = newest.is_some_and(|v| v.slot == txn.slot && v.ts == txn.start);
            if let Some(v) = newest.filter(|_| !own) {
                match txns.writer(v.slot, v.ts) {
                    Writer::Open => return Ok(IOResult::Done(Write::Wait(v.slot))),
                    Writer::Gone => {
                        return Err(LimboError::Corrupt(format!(
                            "the version {v:?} is the newest, and its writer neither committed \
                             nor is open"
                        )))
                    }
                    Writer::Committed => {
                        if txn.isolation == Isolation::Snapshot
                            && !txns.visible(txn, v.slot, v.ts, v.command)
                        {
                            return Ok(IOResult::Done(Write::Conflict));
                        }
                    }
                }
            }
            let present = newest.is_some_and(|v| !v.deleted());
            match (mode, present) {
                (Mode::Insert, true) | (Mode::Update, false) => {
                    return Ok(IOResult::Done(Write::Done(Outcome::Unchanged)))
                }
                _ => {}
            }
            // The version replaced, unless this statement already replaced one of the key's.
            if !(own && newest.is_some_and(|v| v.command == txn.command)) {
                let index = done!(versions.index(txn.slot, self.tree.root().class()));
                let entry = encode_entry(stored.as_ref())?;
                let mut put = Put::new(
                    &index_key(txn.start, self.root(), key, txn.command),
                    entry,
                    Mode::Upsert,
                );
                done!(index.put(&mut put));
            }
            // Still no earlier version if the key had none, or only this statement's.
            let same_statement = own && newest.is_some_and(|v| v.command == txn.command);
            let mut flags = 0;
            if stored.is_none() || (same_statement && newest.is_some_and(|v| v.first())) {
                flags |= Version::FIRST;
            }
            if value.is_none() {
                flags |= Version::DELETED;
            }
            let head = Version {
                slot: txn.slot,
                ts: txn.start,
                command: txn.command,
                updates: 0,
                flags,
            }
            .encode();
            let mut put = Put::with_head(
                key,
                head.to_vec(),
                value.unwrap_or_default().to_vec(),
                Mode::Upsert,
            )
            .expecting(stored.as_ref())
            .in_transaction(txn.start, txn.last);
            match done!(self.tree.put(&mut put)) {
                // Another version came first: judge it.
                Outcome::Changed => continue,
                _ => {
                    txn.logged(put.lsn());
                    return Ok(IOResult::Done(Write::Done(if present {
                        Outcome::Updated
                    } else {
                        Outcome::Inserted
                    })));
                }
            }
        }
    }
}

fn corrupt_header() -> LimboError {
    LimboError::Corrupt("a version header does not decode".into())
}

#[cfg(test)]
mod tests;
