//! Versioned trees (DESIGN.md §11.2, §11.3).
//!
//! Every entry starts with a version header, kept in the leaf:
//!
//! ```text
//! slot: u16 | flags: u8 | updates: u8 | command: u32 | ts: u64 | prev: u64
//! ```
//!
//! naming the transaction that wrote the version (its slot and start), the statement within it,
//! and where the version below it lies: an entry of the writer's slot's version arena
//! ([`super::arena`]), or none for a key that had no entry before. The leaf holds the newest
//! version. A reader starts from it and, while a version is not visible, follows `prev` to the
//! entry holding the one below: a pointer, never a search.
//!
//! A writer judges the newest version first: another slot's open transaction makes it wait, and,
//! under snapshot isolation, one committed after its snapshot is a conflict. It then appends what
//! it replaces to its arena, and writes over the newest version only if that is still the one it
//! judged. A statement writing a key again keeps the entry its first write made, which holds what
//! the statement found: no one reads the versions between, every read resolving to that.
//!
//! A value too long for its leaf goes out of line, and an arena entry holds the replaced version's
//! reference to it. The pages of a version replaced are retired, kept while a snapshot may read it
//! (DESIGN.md §6.5), unless only the replacing statement wrote that version: no one reads it, since
//! others follow the chain to what the statement found, so its pages go at once. The change then
//! logs the entry its statement found as what undoes it, which a rollback put back already.

use super::arena::{Arenas, Ptr};
use super::{Blocker, Isolation, Statement, Transactions, Ts, Txn, Writer};
use crate::io::IO;
use crate::storage::atoll::btree::value::{Read, ValueRead, ValueRef};
use crate::storage::atoll::btree::{
    Kept, Lookup, Mode, Near, Outcome, Put, Stored, StoredValue, Tree,
};
use crate::storage::atoll::buffer::BufferPool;
use crate::storage::atoll::page::PageId;
use crate::storage::atoll::store::PageStore;
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};

/// Bytes of the version header.
pub const HEADER: usize = 24;

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
    /// The arena entry holding the version below this one: `None` if the key had no entry before.
    pub prev: Option<Ptr>,
}

impl Version {
    /// The key was deleted: this version says it is absent.
    pub const DELETED: u8 = 1;

    pub fn encode(&self) -> [u8; HEADER] {
        let mut out = [0u8; HEADER];
        out[0..2].copy_from_slice(&self.slot.to_le_bytes());
        out[2] = self.flags;
        out[3] = self.updates;
        out[4..8].copy_from_slice(&self.command.to_le_bytes());
        out[8..16].copy_from_slice(&self.ts.to_le_bytes());
        out[16..24].copy_from_slice(&self.prev.map_or(0, Ptr::raw).to_le_bytes());
        out
    }

    pub fn decode(head: &[u8]) -> Option<Self> {
        let head: &[u8; HEADER] = head.try_into().ok()?;
        Some(Self {
            slot: u16::from_le_bytes([head[0], head[1]]),
            flags: head[2],
            updates: head[3],
            command: u32::from_le_bytes(head[4..8].try_into().unwrap()),
            ts: u64::from_le_bytes(head[8..16].try_into().unwrap()),
            prev: Ptr::from_raw(u64::from_le_bytes(head[16..24].try_into().unwrap())),
        })
    }

    fn deleted(&self) -> bool {
        self.flags & Self::DELETED != 0
    }
}

/// A version index entry's first byte: the version replaced was no entry at all.
const ABSENT: u8 = 1;
/// A version index entry's first byte: the version replaced holds its value out of line, and the
/// reference to it follows its header.
const OUT_OF_LINE: u8 = 2;

/// The first byte of an arena entry's version replaced, for the entry `leaf` as its leaf stores it
/// and whether its value is out of line; [`ABSENT`] for none. The leaf's bytes follow it whole.
fn entry_kind(leaf: Option<(&[u8], bool)>) -> u8 {
    match leaf {
        None => ABSENT,
        Some((_, false)) => 0,
        Some((_, true)) => OUT_OF_LINE,
    }
}

/// A replaced version as an arena entry holds it: none, or its header, then its inline value or
/// the reference to its value out of line. A write appends the same bytes in pieces, from its
/// look at the leaf.
#[cfg(test)]
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
    let corrupt = || LimboError::Corrupt("an arena entry's version does not decode".into());
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

/// Every slot's version arena, and what reading and rolling back through them needs.
pub struct Versions {
    io: Arc<dyn IO>,
    pool: Arc<BufferPool>,
    store: Arc<PageStore>,
    arenas: Arenas,
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
            arenas: Arenas::new(pool.clone(), slots),
            pool,
            store,
            #[cfg(test)]
            stop: crate::sync::atomic::AtomicU8::new(0),
        }
    }

    /// The slots' arenas.
    pub(super) fn arenas(&self) -> &Arenas {
        &self.arenas
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

    /// The version below `v`, a version of a key, as the arena entry `v` names holds it: `None`
    /// for a key that had no entry before. Step it until done.
    pub(super) fn below(&self, v: &Version) -> Result<IOResult<Option<Stored>>> {
        let Some(ptr) = v.prev else {
            return Ok(IOResult::Done(None));
        };
        let entry = done!(self.arenas.read(ptr));
        let earlier = decode_entry(&entry.replaced)?;
        if let Some(e) = &earlier {
            if Version::decode(&e.head).ok_or_else(corrupt_header)? == *v {
                return Err(LimboError::Corrupt(format!(
                    "the version {v:?} replaced itself in its arena"
                )));
            }
        }
        Ok(IOResult::Done(earlier))
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

/// Which versions a foreign-key guard waits for or conflicts on, of those its snapshot cannot
/// see (R10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Guarded {
    /// Versions absent of the key: a child's guard on its parent's key, which a tombstone or a key
    /// change could leave without it.
    Absent,
    /// Versions holding the key: a removed parent key's guard on the children, one of which could
    /// still name it.
    Present,
}

/// What a guard of one key found among its versions the snapshot cannot see.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Guard {
    /// None of them is one it judges.
    Clear,
    /// The first it judges is another transaction's, still open: wait for it to end and guard
    /// again ([`Transactions::wait_end`]).
    Wait(Blocker),
    /// The first it judges was committed after the snapshot.
    Conflict,
}

/// A guard of one key under way: step it with [`VersionedTree::guard`] until done.
pub struct GuardOp {
    key: Vec<u8>,
    guarded: Guarded,
    /// The version judged next: the newest, read from the leaf when `None`.
    at: Option<Version>,
    /// Whether `at` is the newest.
    newest: bool,
    /// A newest version found gone: rolled back since it was read, unless it is still there.
    gone: Option<Version>,
}

impl GuardOp {
    pub fn new(key: &[u8], guarded: Guarded) -> Self {
        Self {
            key: key.to_vec(),
            guarded,
            at: None,
            newest: true,
            gone: None,
        }
    }

    /// The guard of `key`, whose newest version a read of its leaf found to be `head`.
    pub fn from_head(key: &[u8], head: &[u8], guarded: Guarded) -> Result<Self> {
        let mut op = Self::new(key, guarded);
        op.at = Some(Version::decode(head).ok_or_else(corrupt_header)?);
        Ok(op)
    }

    /// Judges the key again from its newest version, as after a wait on one of its writers.
    pub fn again(&mut self) {
        self.at = None;
        self.newest = true;
        self.gone = None;
    }
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
/// Its key and entry are buffers the caller may take back once it is done
/// ([`WriteOp::into_buffers`]), to build its next write in.
pub struct WriteOp {
    key: Vec<u8>,
    /// A put's entry: [`HEADER`] bytes the version header goes in, then the value, built once, in
    /// the bytes the leaf and the log copy. A delete's: bytes its header and the value it keeps
    /// are written into.
    entry: Vec<u8>,
    deletes: bool,
    mode: Mode,
    /// The tree's put under way, kept across steps so a value out of line is written once, and
    /// whether the key was present when the put was judged.
    put: Option<(Put, bool)>,
    /// The leaf tried first, then the leaf the write reached.
    near: Near,
    /// A look at the key's leaf a probe on its behalf made, redeemed instead of looking again.
    kept: Option<Kept>,
}

impl WriteOp {
    pub fn put(key: &[u8], value: Vec<u8>, mode: Mode) -> Self {
        let mut entry = value;
        entry.splice(0..0, [0; HEADER]);
        Self::put_entry(key.to_vec(), entry, mode)
    }

    /// Puts `entry`'s value under `key`: `entry` holds [`HEADER`] bytes for the version header,
    /// then the value.
    pub fn put_entry(key: Vec<u8>, entry: Vec<u8>, mode: Mode) -> Self {
        assert!(
            entry.len() >= HEADER,
            "a put's entry with no room for its header"
        );
        Self {
            key,
            entry,
            deletes: false,
            mode,
            put: None,
            near: Near::NONE,
            kept: None,
        }
    }

    /// Deletes `key`: [`Outcome::Updated`] if the transaction reads it present, else
    /// [`Outcome::Unchanged`].
    pub fn delete(key: &[u8]) -> Self {
        Self::delete_into(key.to_vec(), Vec::new())
    }

    /// [`delete`](Self::delete), its entry built in `spare`.
    pub fn delete_into(key: Vec<u8>, spare: Vec<u8>) -> Self {
        Self {
            key,
            entry: spare,
            deletes: true,
            mode: Mode::Update,
            put: None,
            near: Near::NONE,
            kept: None,
        }
    }

    /// Whether it deletes its key.
    pub fn deletes(&self) -> bool {
        self.deletes
    }

    /// Its key, and the entry it wrote once done: a put's as the leaf stores it inline, its
    /// version header then its value.
    pub fn into_buffers(self) -> (Vec<u8>, Vec<u8>) {
        (self.key, self.entry)
    }

    /// The put's buffers back from `put`, once it is done or must be built again: the entry, whole
    /// unless its value went out of line, when the copy kept for that is the entry.
    fn take_back(&mut self, put: Put) {
        let (key, entry) = put.into_buffers();
        self.key = key;
        if let Some(entry) = entry {
            self.entry = entry;
        }
    }

    /// Tries `near` before a descent.
    pub fn near(mut self, near: Near) -> Self {
        self.near = near;
        self
    }

    /// The leaf the write reached: where its key is, unless the leaf has changed since.
    pub fn reached(&self) -> Near {
        self.near
    }

    /// Starts from `kept`, the look at the key's leaf a probe of the key made
    /// ([`VersionedTree::probe`]): the write's look, while the leaf is unchanged since.
    pub fn kept(mut self, kept: Option<Kept>) -> Self {
        self.kept = kept;
        self
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

    /// Probes `key` on a write path, judged by its newest version as a write would judge it: a
    /// version another transaction holds open is waited for, and under snapshot isolation one
    /// committed after the snapshot is a conflict. Step it until done.
    /// The leaf at `near` is tried first. Done present or absent, its look at the leaf is kept in
    /// `kept`, for the read and the write of the key that follow ([`WriteOp::kept`]).
    pub fn probe(
        &self,
        txns: &Transactions,
        txn: &mut Txn,
        key: &[u8],
        near: Near,
        kept: &mut Option<Kept>,
    ) -> Result<IOResult<Probe>> {
        // A version found gone: rolled back since it was read, unless it is still there.
        let mut gone = None;
        loop {
            let looked = done!(self.tree.look(key, near));
            let newest = match looked.present() {
                None => None,
                Some((b, _)) => Some(
                    Version::decode(b.get(..HEADER).ok_or_else(corrupt_header)?)
                        .ok_or_else(corrupt_header)?,
                ),
            };
            let Some(v) = newest else {
                *kept = Some(looked.keep());
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
            *kept = Some(looked.keep());
            return Ok(IOResult::Done(if v.deleted() {
                Probe::Absent
            } else {
                Probe::Present
            }));
        }
    }

    /// Guards `op`'s key after `txn` wrote (R10): its versions the snapshot cannot see are judged
    /// newest first, down to the one it sees, as a rollback of each could bring back the one
    /// below it. The first that says what `op` guards against waits while its writer is open, and
    /// is a conflict once it has committed; the others are passed over, and so is every version
    /// the snapshot sees, its own included. Step it until done.
    pub fn guard(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
        op: &mut GuardOp,
    ) -> Result<IOResult<Guard>> {
        loop {
            let v = match op.at {
                Some(v) => v,
                None => {
                    let stored = done!(self.tree.stored(&op.key));
                    let Some(stored) = stored else {
                        return Ok(IOResult::Done(Guard::Clear));
                    };
                    let v = Version::decode(&stored.head).ok_or_else(corrupt_header)?;
                    op.at = Some(v);
                    op.newest = true;
                    v
                }
            };
            if txns.visible(txn, v.slot, v.ts) {
                return Ok(IOResult::Done(Guard::Clear));
            }
            let holds = !v.deleted();
            if holds == (op.guarded == Guarded::Present) {
                match txns.writer(v.slot, v.ts) {
                    Writer::Open => {
                        return Ok(IOResult::Done(Guard::Wait(Blocker {
                            slot: v.slot,
                            start: v.ts,
                        })))
                    }
                    Writer::Committed => return Ok(IOResult::Done(Guard::Conflict)),
                    // Rolled back since it was read: its undo put back the entry below it.
                    Writer::Gone if op.newest && op.gone != Some(v) => {
                        op.gone = Some(v);
                        op.at = None;
                        continue;
                    }
                    Writer::Gone => {
                        return Err(LimboError::Corrupt(format!(
                            "the version {v:?} of a key a guard judges is gone, and its writer \
                             neither committed nor is open"
                        )))
                    }
                }
            }
            // The key had no entry before: the snapshot reads it absent.
            let Some(earlier) = done!(versions.below(&v)) else {
                return Ok(IOResult::Done(Guard::Clear));
            };
            op.at = Some(Version::decode(&earlier.head).ok_or_else(corrupt_header)?);
            op.newest = false;
        }
    }

    /// The plain put of `key` by `txn`'s current statement into a tree that statement created, its
    /// `entry` holding [`HEADER`] bytes for the version header and then the value:
    /// no other transaction can see the tree, so the entry needs no earlier version and no arena
    /// entry, and an undo of the creation frees the tree whole (DESIGN.md §17). Every write the
    /// creating statement makes into its tree is plain.
    pub fn plain_put(&self, txn: &Txn, key: Vec<u8>, mut entry: Vec<u8>, mode: Mode) -> Put {
        assert!(
            entry.len() >= HEADER,
            "a put's entry with no room for its header"
        );
        let head = Version {
            slot: txn.slot,
            ts: txn.start,
            command: txn.command,
            updates: 0,
            flags: 0,
            prev: None,
        }
        .encode();
        entry[..HEADER].copy_from_slice(&head);
        Put::with_owned(key, entry, HEADER, mode)
    }

    /// The longest key: the tree's. An arena page holds an entry of any key a tree of any class
    /// holds ([`super::arena::CLASS`]).
    pub fn max_key(&self) -> usize {
        self.tree.limits().max_key
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
                return self.follow(txns, versions, txn, chain);
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

    /// The value of the chain's key that `txn`'s current statement reads, following it from a
    /// version the statement does not see: `None` for a key it reads as absent. Step it until
    /// done.
    fn follow(
        &self,
        txns: &Transactions,
        versions: &Versions,
        txn: &mut Txn,
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
            let Some(earlier) = done!(versions.below(&v)) else {
                return Ok(IOResult::Done(None));
            };
            let e = Version::decode(&earlier.head).ok_or_else(corrupt_header)?;
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
        // The look a put just built goes in from, once.
        let mut from = None;
        loop {
            if let Some((put, present)) = &mut op.put {
                let present = *present;
                let outcome = match from.take() {
                    Some(looked) => done!(self.tree.put_looked(put, looked)),
                    None => done!(self.tree.put(put)),
                };
                let (put, _) = op.put.take().expect("a put");
                // Another version came first: judge it.
                if outcome == Outcome::Changed {
                    op.take_back(put);
                    continue;
                }
                return Ok(IOResult::Done(Write::Done(
                    self.wrote(txn, op, put, present),
                )));
            }
            // One look serves the judgment and the write: the put goes in at its leaf if the leaf
            // is still as the look found it. A probe's look, its leaf unchanged, is that look.
            let looked = match op.kept.take().and_then(|k| self.tree.redeem(k)) {
                Some(looked) => looked,
                None => done!(self.tree.look(&op.key, op.near)),
            };
            op.near = looked.near();
            let leaf_entry = looked.present();
            let newest = match leaf_entry {
                None => None,
                Some((b, _)) => Some(
                    Version::decode(b.get(..HEADER).ok_or_else(corrupt_header)?)
                        .ok_or_else(corrupt_header)?,
                ),
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
            // A statement writing the key again keeps the entry its first write made, which holds
            // what it found: what undoing this change puts back. Any other write appends what it
            // replaces, and its version names that entry unless the key had none.
            let (found, prev) = match newest.filter(|v| own && v.command == txn.command) {
                Some(v) => (Some(done!(versions.below(&v))), v.prev),
                None => {
                    // Its kind, then the entry as the leaf stores it, copied from the look.
                    let kind = [entry_kind(leaf_entry)];
                    let entry = leaf_entry.map_or(&[][..], |(b, _)| b);
                    let ptr = done!(versions.arenas().append_pieces(
                        txn,
                        self.root(),
                        &op.key,
                        &[&kind, entry]
                    ));
                    (None, leaf_entry.is_some().then_some(ptr))
                }
            };
            let flags = match op.deletes {
                false => 0,
                true => Version::DELETED,
            };
            let head = Version {
                slot: txn.slot,
                ts: txn.start,
                command: txn.command,
                updates: 0,
                flags,
                prev,
            }
            .encode();
            let mut entry = std::mem::take(&mut op.entry);
            if op.deletes {
                // A delete keeps an inline value where it lies, so only the head changes: a
                // reader below it reads that value with the head it replaced. A value out of line
                // goes, its pages retired.
                entry.clear();
                entry.extend_from_slice(&head);
                if let Some((stored, false)) = leaf_entry {
                    entry.extend_from_slice(stored.get(HEADER..).ok_or_else(corrupt_header)?);
                }
            } else {
                entry[..HEADER].copy_from_slice(&head);
                if !self.tree.limits().inline(op.key.len(), entry.len()) {
                    // A value going out of line leaves the put's entry: kept whole here, for a put
                    // built again.
                    op.entry = entry.clone();
                }
            }
            let put = Put::with_owned(std::mem::take(&mut op.key), entry, HEADER, Mode::Upsert)
                .expecting_entry(leaf_entry)
                .in_transaction(txns.tail(txn));
            let put = match &found {
                // A version only this statement wrote is read by no one: its pages go.
                Some(found) => put.undoing(found.as_ref()),
                // Another's may be read by a snapshot: they stay, retired.
                None => put.retiring(),
            };
            op.put = Some((put, present));
            from = Some(looked);
        }
    }

    /// A write's put done: its leaf and group noted, its buffers back in `op`, and what it came to.
    fn wrote(&self, txn: &mut Txn, op: &mut WriteOp, put: Put, present: bool) -> Outcome {
        op.near = put.reached();
        txn.logged(put.group().expect("a versioned tree's change is logged"));
        op.take_back(put);
        if present {
            Outcome::Updated
        } else {
            Outcome::Inserted
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
        self.stop
            .store(STOP, crate::sync::atomic::Ordering::Release);
    }

    /// Waits until a writer is stopped.
    fn stopped_writer(&self) {
        while self.stop.load(crate::sync::atomic::Ordering::Acquire) != STOPPED {
            crate::thread::yield_now();
        }
    }

    /// Lets the stopped writer go on.
    fn go_on(&self) {
        self.stop.store(0, crate::sync::atomic::Ordering::Release);
    }

    /// Where a writer stops, if the next one is to.
    fn stop_here(&self) {
        let stop = self.stop.compare_exchange(
            STOP,
            STOPPED,
            crate::sync::atomic::Ordering::AcqRel,
            crate::sync::atomic::Ordering::Acquire,
        );
        if stop.is_ok() {
            while self.stop.load(crate::sync::atomic::Ordering::Acquire) == STOPPED {
                crate::thread::yield_now();
            }
        }
    }
}

mod cursor;
pub use cursor::VersionedCursor;

#[cfg(test)]
mod tests;
