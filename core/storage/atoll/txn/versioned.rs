//! Versioned trees (DESIGN.md §11.2, §11.3).
//!
//! Every entry starts with a version header, kept in the leaf:
//!
//! ```text
//! slot: u16 | ts: u64 | command: u32 | updates: u8 | flags: u8
//! ```
//!
//! naming the transaction that wrote the version (its slot and start) and the statement within it.
//! The leaf holds the newest version. Each slot keeps a version index per node class: a temporary
//! tree of the versions its transactions replaced, keyed
//!
//! ```text
//! ts | command | tree | key
//! ```
//!
//! by the version that replaced it: the replacing transaction's start, the replacing statement,
//! the tree's root and the key. A reader starts from the newest version and, while it is not
//! visible, follows the chain into the writer's version index. The statement is in the key so that
//! a statement changing a row an earlier statement of its transaction changed keeps that earlier
//! version, which it still reads, and before the tree so that a transaction's entries, read
//! backward, come newest statement first, as rollback puts them back ([`super::rollback`]).
//!
//! A writer judges the newest version first: another slot's open transaction makes it wait, and,
//! under snapshot isolation, one committed after its snapshot is a conflict. It then puts the
//! version it replaces into its version index, and writes over the newest version only if that is
//! still the one it judged.
//!
//! A value too long for its leaf goes out of line, and an index entry holds the replaced version's
//! reference to it. The pages of a version replaced are retired, kept while a snapshot may read it
//! (DESIGN.md §6.5), unless only the replacing statement wrote that version: no one reads it, since
//! others follow the chain to what the statement found, so its pages go at once. The change then
//! logs the entry its statement found as what undoes it, which a rollback put back already.

use super::{Blocker, Isolation, Statement, Transactions, Ts, Txn, Writer};
use crate::io::IO;
use crate::storage::atoll::btree::value::{Read, ValueRead, ValueRef};
use crate::storage::atoll::btree::{Lookup, Mode, Outcome, Put, Stored, StoredValue, Tree};
use crate::storage::atoll::buffer::BufferPool;
use crate::storage::atoll::page::{PageId, SizeClass, CLASS_COUNT};
use crate::storage::atoll::store::PageStore;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};

/// Bytes of the version header.
pub const HEADER: usize = 16;

/// Bytes of a version index key before the tree's key: the replacing transaction's start, its
/// statement and the tree's root.
pub(super) const INDEX_PREFIX: usize = 8 + 4 + 8;

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
    pub command: Statement,
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
        out[10..14].copy_from_slice(&self.command.to_le_bytes());
        out[14] = self.updates;
        out[15] = self.flags;
        out
    }

    pub fn decode(head: &[u8]) -> Option<Self> {
        let head: &[u8; HEADER] = head.try_into().ok()?;
        Some(Self {
            slot: u16::from_le_bytes([head[0], head[1]]),
            ts: u64::from_le_bytes(head[2..10].try_into().unwrap()),
            command: u32::from_le_bytes(head[10..14].try_into().unwrap()),
            updates: head[14],
            flags: head[15],
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
/// A version index entry's first byte: the version replaced holds its value out of line, and the
/// reference to it follows its header.
const OUT_OF_LINE: u8 = 2;

/// The version index key of the version replaced by `(ts, command)`'s write of `key` in `tree`.
pub(super) fn index_key(ts: Ts, command: Statement, tree: PageId, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(INDEX_PREFIX + key.len());
    out.extend_from_slice(&ts.to_be_bytes());
    out.extend_from_slice(&command.to_be_bytes());
    out.extend_from_slice(&tree.raw().to_be_bytes());
    out.extend_from_slice(key);
    out
}

/// What a version index key names: the replacing transaction's start and statement, the tree and
/// the key.
pub(super) fn index_key_parts(key: &[u8]) -> Option<(Ts, Statement, PageId, &[u8])> {
    if key.len() < INDEX_PREFIX {
        return None;
    }
    Some((
        u64::from_be_bytes(key[0..8].try_into().unwrap()),
        u32::from_be_bytes(key[8..12].try_into().unwrap()),
        PageId::from_raw(u64::from_be_bytes(key[12..20].try_into().unwrap())),
        &key[INDEX_PREFIX..],
    ))
}

/// A replaced version as its index entry holds it: none, or its header, then its inline value or
/// the reference to its value out of line.
fn encode_entry(stored: Option<&Stored>) -> Vec<u8> {
    let Some(s) = stored else {
        return vec![ABSENT];
    };
    let mut out = Vec::new();
    match &s.value {
        StoredValue::Inline(value) => {
            out.push(0);
            out.extend_from_slice(&s.head);
            out.extend_from_slice(value);
        }
        StoredValue::OutOfLine(reference) => {
            out.push(OUT_OF_LINE);
            out.extend_from_slice(&s.head);
            out.extend_from_slice(&reference.encode());
        }
    }
    out
}

pub(super) fn decode_entry(entry: &[u8]) -> Result<Option<Stored>> {
    let corrupt = || LimboError::Corrupt("a version index entry does not decode".into());
    match entry.first() {
        Some(&ABSENT) => Ok(None),
        Some(&kind @ (0 | OUT_OF_LINE)) if entry.len() > HEADER => {
            let rest = &entry[1 + HEADER..];
            let value = if kind == OUT_OF_LINE {
                StoredValue::OutOfLine(ValueRef::decode(rest).ok_or_else(corrupt)?)
            } else {
                StoredValue::Inline(rest.to_vec())
            };
            Ok(Some(Stored {
                head: entry[1..1 + HEADER].to_vec(),
                value,
            }))
        }
        _ => Err(corrupt()),
    }
}

/// The version indexes of every slot.
pub struct Versions {
    io: Arc<dyn IO>,
    pool: Arc<BufferPool>,
    store: Arc<PageStore>,
    /// Per slot and node class, the root of its version index once created, set once by
    /// compare-and-swap: its raw id plus one, 0 for none.
    roots: Box<[[AtomicU64; CLASS_COUNT]]>,
    /// A test's stop for a writer ([`Versions::stop_next_writer`]): 0, [`STOP`] or [`STOPPED`].
    #[cfg(test)]
    stop: crate::sync::atomic::AtomicU8,
}

/// The next writer to read a key whose newest version is another transaction's is to stop.
#[cfg(test)]
const STOP: u8 = 1;
/// A writer is stopped.
#[cfg(test)]
const STOPPED: u8 = 2;

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
                .map(|_| std::array::from_fn(|_| AtomicU64::new(0)))
                .collect(),
            #[cfg(test)]
            stop: crate::sync::atomic::AtomicU8::new(0),
        }
    }

    /// The roots of `slot`'s version indexes made so far, one per node class at most.
    pub(super) fn index_roots(&self, slot: u16) -> Vec<PageId> {
        self.roots[slot as usize]
            .iter()
            .filter_map(|cell| match cell.load(Ordering::Acquire) {
                0 => None,
                raw => Some(PageId::from_raw(raw - 1)),
            })
            .collect()
    }

    /// The version index rooted at `root`.
    pub(super) fn open_index(&self, root: PageId) -> Tree {
        Tree::open(self.io.clone(), self.pool.clone(), self.store.clone(), root)
    }

    /// The plain tree rooted at `root`.
    pub(super) fn open_plain(&self, root: PageId) -> Tree {
        Tree::open(self.io.clone(), self.pool.clone(), self.store.clone(), root)
    }

    pub(super) fn pool_and_store(&self) -> (&Arc<BufferPool>, &Arc<PageStore>) {
        (&self.pool, &self.store)
    }

    /// The versioned tree rooted at `root`.
    pub(super) fn open_tree(&self, root: PageId) -> VersionedTree {
        VersionedTree::open(self.io.clone(), self.pool.clone(), self.store.clone(), root)
    }

    /// The node class of the version index for trees of `class`: one larger, so its keys hold a
    /// tree's longest key and the [`INDEX_PREFIX`] bytes before it.
    fn index_class(class: SizeClass) -> SizeClass {
        SizeClass::new((class.index() + 1).min(MAX_NODE_CLASS)).expect("a class")
    }

    /// `slot`'s version index for trees of `class`, created if it has none.
    fn index(&self, slot: u16, class: SizeClass) -> Result<IOResult<Tree>> {
        let class = Self::index_class(class);
        let cell = &self.roots[slot as usize][class.index()];
        let root = match cell.load(Ordering::Acquire) {
            0 => {
                let root = done!(Tree::create_temporary(
                    &self.io,
                    &self.pool,
                    &self.store,
                    class
                ));
                match cell.compare_exchange(0, root.raw() + 1, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => root,
                    // Created meanwhile by another reader: this one is never used.
                    Err(other) => {
                        self.store.orphaned(root);
                        PageId::from_raw(other - 1)
                    }
                }
            }
            raw => PageId::from_raw(raw - 1),
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
    pool: Arc<BufferPool>,
}

/// What a write came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    Done(Outcome),
    /// The newest version is another transaction's, still open: wait for it to end and write
    /// again ([`Transactions::wait_end`]).
    Wait(Blocker),
    /// The newest version was committed after the snapshot: under snapshot isolation the
    /// transaction cannot write it.
    Conflict,
}

/// What a probe of a key on a write path found, judged by the key's newest version (DESIGN.md
/// §17): what a uniqueness or rowid check goes by, since a version the snapshot cannot see may be
/// committed before this transaction commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// The newest version, the transaction's own or one it may act on, says the key is present.
    Present,
    /// It says the key is absent, or there is none.
    Absent,
    /// The newest version is another transaction's, still open: wait for it to end and probe
    /// again ([`Transactions::wait_end`]).
    Wait(Blocker),
    /// The newest version was committed after the snapshot: under snapshot isolation the
    /// transaction cannot act on it.
    Conflict,
}

/// A read of one key's value as a transaction's statement sees it: step it with
/// [`VersionedTree::get`] until done.
pub struct Get {
    key: Vec<u8>,
    /// The newest version, seen and visible, holding its value out of line: read through the
    /// leaf, which tells whether that version is still the newest, by its head.
    newest: Option<(Lookup, Vec<u8>)>,
    /// The key's chain, followed from a newest version the statement does not see.
    chain: Option<Chain>,
}

/// Following a key's chain from a version the statement does not see, through the version index
/// of each writer, to the newest version it does: step it with [`VersionedTree::follow`].
struct Chain {
    /// The version whose predecessor is looked up next.
    at: Version,
    /// An earlier version's value out of line, read from where its index entry names it.
    reading: Option<ValueRead>,
}

impl Chain {
    fn from(at: Version) -> Self {
        Self { at, reading: None }
    }
}

impl Get {
    pub fn new(key: &[u8]) -> Self {
        Self {
            key: key.to_vec(),
            newest: None,
            chain: None,
        }
    }
}

/// A put or delete of one key in a transaction: step it with [`VersionedTree::write`] until done.
pub struct WriteOp {
    key: Vec<u8>,
    /// `None` deletes.
    value: Option<Vec<u8>>,
    mode: Mode,
    /// The tree's put under way, kept across steps so a value out of line is written once, and
    /// whether the key was present when the put was judged.
    put: Option<(Put, bool)>,
}

impl WriteOp {
    pub fn put(key: &[u8], value: Vec<u8>, mode: Mode) -> Self {
        Self {
            key: key.to_vec(),
            value: Some(value),
            mode,
            put: None,
        }
    }

    /// Deletes `key`: [`Outcome::Updated`] if the transaction reads it present, else
    /// [`Outcome::Unchanged`].
    pub fn delete(key: &[u8]) -> Self {
        Self {
            key: key.to_vec(),
            value: None,
            mode: Mode::Update,
            put: None,
        }
    }
}

impl VersionedTree {
    pub fn open(
        io: Arc<dyn IO>,
        pool: Arc<BufferPool>,
        store: Arc<PageStore>,
        root: PageId,
    ) -> Self {
        Self {
            tree: Tree::open(io, pool.clone(), store, root).with_head(HEADER),
            pool,
        }
    }

    pub fn root(&self) -> PageId {
        self.tree.root()
    }

    pub fn tree(&self) -> &Tree {
        &self.tree
    }

    /// The longest key: the tree's, and what its version index keys hold.
    /// Probes `key` on a write path, judged by its newest version as a write would judge it: a
    /// version another transaction holds open is waited for, and under snapshot isolation one
    /// committed after the snapshot is a conflict. Step it until done.
    pub fn probe(&self, txns: &Transactions, txn: &mut Txn, key: &[u8]) -> Result<IOResult<Probe>> {
        // A version found gone: rolled back since it was read, unless it is still there.
        let mut gone = None;
        loop {
            let stored = done!(self.tree.stored(key));
            let Some(v) = stored
                .as_ref()
                .map(|s| Version::decode(&s.head).ok_or_else(corrupt_header))
                .transpose()?
            else {
                return Ok(IOResult::Done(Probe::Absent));
            };
            if v.slot != txn.slot || v.ts != txn.start {
                match txns.writer(v.slot, v.ts) {
                    Writer::Open => {
                        let blocker = Blocker {
                            slot: v.slot,
                            start: v.ts,
                        };
                        return Ok(IOResult::Done(Probe::Wait(blocker)));
                    }
                    Writer::Gone if gone != Some(v) => {
                        gone = Some(v);
                        continue;
                    }
                    Writer::Gone => {
                        return Err(LimboError::Corrupt(format!(
                            "the version {v:?} is the newest, and its writer neither committed \
                             nor is open"
                        )))
                    }
                    Writer::Committed => {
                        if txn.isolation == Isolation::Snapshot && !txns.visible(txn, v.slot, v.ts)
                        {
                            return Ok(IOResult::Done(Probe::Conflict));
                        }
                    }
                }
            }
            return Ok(IOResult::Done(if v.deleted() {
                Probe::Absent
            } else {
                Probe::Present
            }));
        }
    }

    /// The plain put of `key` by `txn`'s current statement into a tree that statement created:
    /// no other transaction can see the tree, so the entry needs no earlier version and no index
    /// entry, and an undo of the creation frees the tree whole (DESIGN.md §17). Every write the
    /// creating statement makes into its tree is plain.
    pub fn plain_put(&self, txn: &Txn, key: &[u8], value: Vec<u8>, mode: Mode) -> Put {
        let head = Version {
            slot: txn.slot,
            ts: txn.start,
            command: txn.command,
            updates: 0,
            flags: Version::FIRST,
        }
        .encode();
        Put::with_head(key, head.to_vec(), value, mode)
    }

    pub fn max_key(&self) -> usize {
        let index = Versions::index_class(self.tree.root().class());
        let index_max =
            crate::storage::atoll::btree::node::Limits::of(index).max_key - INDEX_PREFIX;
        self.tree.limits().max_key.min(index_max)
    }

    /// The value of `op`'s key that `txn`'s current statement reads. Step it until done.
    pub fn get(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        op: &mut Get,
    ) -> Result<IOResult<Option<Vec<u8>>>> {
        loop {
            if let Some((lookup, head)) = &mut op.newest {
                let value = done!(self.tree.lookup(lookup));
                let judged = std::mem::take(head);
                op.newest = None;
                // Still the version judged: its value. Another came since: judge it.
                match value {
                    Some(v) if v.starts_with(&judged) => {
                        return Ok(IOResult::Done(Some(v[HEADER..].to_vec())))
                    }
                    _ => continue,
                }
            }
            if let Some(chain) = &mut op.chain {
                return self.follow(txns, versions, txn, &op.key, chain);
            }
            let Some(stored) = done!(self.tree.stored(&op.key)) else {
                return Ok(IOResult::Done(None));
            };
            let v = Version::decode(&stored.head).ok_or_else(corrupt_header)?;
            if !txns.visible(txn, v.slot, v.ts) {
                op.chain = Some(Chain::from(v));
                continue;
            }
            if v.deleted() {
                return Ok(IOResult::Done(None));
            }
            match stored.value {
                StoredValue::Inline(value) => return Ok(IOResult::Done(Some(value))),
                StoredValue::OutOfLine(_) => op.newest = Some((Lookup::new(&op.key), stored.head)),
            }
        }
    }

    /// The value of `key` that `txn`'s current statement reads, following its chain from a
    /// version the statement does not see: `None` for a key it reads as absent. Step it until
    /// done.
    fn follow(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        key: &[u8],
        chain: &mut Chain,
    ) -> Result<IOResult<Option<Vec<u8>>>> {
        loop {
            if let Some(read) = &mut chain.reading {
                // A replaced version's pages stay while a snapshot may read it (DESIGN.md §6.5):
                // nothing frees them under this read.
                let read = done!(read.step(&self.pool, &|| true));
                let reference = chain.reading.take().expect("a read").reference();
                return match read {
                    Read::Value(v) => Ok(IOResult::Done(Some(v))),
                    Read::Changed | Read::Unheld => Err(LimboError::Corrupt(format!(
                        "the out-of-line value {reference:?} of a version a snapshot reads is gone"
                    ))),
                };
            }
            let v = chain.at;
            if v.first() {
                return Ok(IOResult::Done(None));
            }
            let index = done!(versions.index(v.slot, self.tree.root().class()));
            let mut lookup = Lookup::new(&index_key(v.ts, v.command, self.root(), key));
            let Some(entry) = done!(index.lookup(&mut lookup)) else {
                return Err(LimboError::Corrupt(format!(
                    "the version {v:?} of a key replaced nothing in its version index"
                )));
            };
            let Some(earlier) = decode_entry(&entry)? else {
                return Ok(IOResult::Done(None));
            };
            let e = Version::decode(&earlier.head).ok_or_else(corrupt_header)?;
            if e == v {
                return Err(LimboError::Corrupt(format!(
                    "the version {v:?} replaced itself in its version index"
                )));
            }
            if !txns.visible(txn, e.slot, e.ts) {
                chain.at = e;
                continue;
            }
            if e.deleted() {
                return Ok(IOResult::Done(None));
            }
            match earlier.value {
                StoredValue::Inline(value) => return Ok(IOResult::Done(Some(value))),
                StoredValue::OutOfLine(reference) => {
                    chain.reading = Some(ValueRead::new(reference))
                }
            }
        }
    }

    /// Puts or deletes `op`'s key in `txn`. Step it until done.
    pub fn write(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        op: &mut WriteOp,
    ) -> Result<IOResult<Write>> {
        assert!(
            op.key.len() <= self.max_key(),
            "a key of {} bytes",
            op.key.len()
        );
        // A version found gone: rolled back since it was read, unless it is still there.
        let mut gone = None;
        loop {
            if let Some((put, present)) = &mut op.put {
                let present = *present;
                match done!(self.tree.put(put)) {
                    // Another version came first: judge it.
                    Outcome::Changed => {
                        op.put = None;
                        continue;
                    }
                    _ => {
                        let (put, _) = op.put.take().expect("a put");
                        txn.logged(put.group().expect("a versioned tree's change is logged"));
                        return Ok(IOResult::Done(Write::Done(if present {
                            Outcome::Updated
                        } else {
                            Outcome::Inserted
                        })));
                    }
                }
            }
            let stored = done!(self.tree.stored(&op.key));
            let newest = match &stored {
                None => None,
                Some(s) => Some(Version::decode(&s.head).ok_or_else(corrupt_header)?),
            };
            let own = newest.is_some_and(|v| v.slot == txn.slot && v.ts == txn.start);
            if let Some(v) = newest.filter(|_| !own) {
                #[cfg(test)]
                versions.stop_here();
                match txns.writer(v.slot, v.ts) {
                    Writer::Open => {
                        let blocker = Blocker {
                            slot: v.slot,
                            start: v.ts,
                        };
                        return Ok(IOResult::Done(Write::Wait(blocker)));
                    }
                    // A rollback puts back every version before its slot is free: read again.
                    Writer::Gone if gone != Some(v) => {
                        gone = Some(v);
                        continue;
                    }
                    Writer::Gone => {
                        return Err(LimboError::Corrupt(format!(
                            "the version {v:?} is the newest, and its writer neither committed \
                             nor is open"
                        )))
                    }
                    Writer::Committed => {
                        if txn.isolation == Isolation::Snapshot && !txns.visible(txn, v.slot, v.ts)
                        {
                            return Ok(IOResult::Done(Write::Conflict));
                        }
                    }
                }
            }
            let present = newest.is_some_and(|v| !v.deleted());
            match (op.mode, present) {
                (Mode::Insert, true) | (Mode::Update, false) => {
                    return Ok(IOResult::Done(Write::Done(Outcome::Unchanged)))
                }
                _ => {}
            }
            let same_statement = own && newest.is_some_and(|v| v.command == txn.command);
            let index = done!(versions.index(txn.slot, self.tree.root().class()));
            let at = index_key(txn.start, txn.command, self.root(), &op.key);
            // The version replaced, unless this statement already replaced one of the key's: then
            // what the statement found, which undoing this change puts back.
            let found = if same_statement {
                let Some(entry) = done!(index.lookup(&mut Lookup::new(&at))) else {
                    return Err(LimboError::Corrupt(format!(
                        "a statement's version of a key replaced nothing in its version index: \
                         {newest:?}"
                    )));
                };
                Some(decode_entry(&entry)?)
            } else {
                done!(index.put(&mut Put::new(
                    &at,
                    encode_entry(stored.as_ref()),
                    Mode::Upsert
                )));
                None
            };
            // Still no earlier version if the key had none, or only this statement's.
            let mut flags = 0;
            if stored.is_none() || (same_statement && newest.is_some_and(|v| v.first())) {
                flags |= Version::FIRST;
            }
            if op.value.is_none() {
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
            let put = Put::with_head(
                &op.key,
                head.to_vec(),
                op.value.clone().unwrap_or_default(),
                Mode::Upsert,
            )
            .expecting(stored.as_ref())
            .in_transaction(txns.tail(txn));
            let put = match &found {
                // A version only this statement wrote is read by no one: its pages go.
                Some(found) => put.undoing(found.as_ref()),
                // Another's may be read by a snapshot: they stay, retired.
                None => put.retiring(),
            };
            op.put = Some((put, present));
        }
    }
}

fn corrupt_header() -> LimboError {
    LimboError::Corrupt("a version header does not decode".into())
}

/// A writer stopped between reading a key and looking at the transaction whose version it read,
/// for a test to change the world there: no schedule makes a rollback finish in that window.
#[cfg(test)]
impl Versions {
    /// Stops the next writer to read a key whose newest version is another transaction's, before
    /// it looks at that transaction, until [`Versions::go_on`].
    fn stop_next_writer(&self) {
        self.stop.store(STOP, Ordering::Release);
    }

    /// Waits until a writer is stopped.
    fn stopped_writer(&self) {
        while self.stop.load(Ordering::Acquire) != STOPPED {
            crate::thread::yield_now();
        }
    }

    /// Lets the stopped writer go on.
    fn go_on(&self) {
        self.stop.store(0, Ordering::Release);
    }

    /// Where a writer stops, if the next one is to.
    fn stop_here(&self) {
        let stop = self
            .stop
            .compare_exchange(STOP, STOPPED, Ordering::AcqRel, Ordering::Acquire);
        if stop.is_ok() {
            while self.stop.load(Ordering::Acquire) == STOPPED {
                crate::thread::yield_now();
            }
        }
    }
}

mod cursor;
pub use cursor::VersionedCursor;

#[cfg(test)]
mod tests;
