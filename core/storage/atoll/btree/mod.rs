//! B+-trees (DESIGN.md §13).
//!
//! - [`node`]: the slotted node layout.
//! - [`record`]: the log records of node changes, applied the same way when made and replayed.
//! - [`value`]: values too large for a node, in blob pages.
//!
//! **Latching** is optimistic lock coupling (LeanStore, ICDE 2018 §IV-F). A descent fixes each
//! node without a lock and checks afterwards that the parent did not change while the child was
//! found, so a reader takes no latch at all. A writer latches only its leaf, and checks under the
//! latch that the leaf still covers its key. A structure change (a split, a merge, the root growing
//! or shrinking) is its own step: it descends again and latches exactly the nodes it changes, after
//! which the operation that needed it starts over. A latch that cannot be taken at once is a
//! cooperative yield, like everywhere in Turso, and no latch is held across a returned `IOResult`.
//!
//! **Hints.** A child entry keeps the frame its child was last found in; a descent that finds the
//! hint stale refreshes it if it can latch the parent at once, writing nothing a reader trusts. A
//! leaf keeps its right neighbour the same way, and a scan follows that hint only when the frame
//! it names still holds the neighbour, checked by the neighbour's tree, level and lower fence.
//!
//! **Atomicity.** A structure change is one log group: the new node's image, the split node's
//! truncation, the parent's new entry, and the adoption of the new page, or the merged node's
//! image, the parent's removed entry and the abandonment of the emptied node. A crash keeps all of
//! it or none ([`super::log::Log::append_group`]); a page allocated for a change the crash undid
//! is freed by recovery ([`super::store`]'s provisional pages).
//!
//! **The root** never moves, so the root page names the tree. When it splits, its contents move to
//! a new child and the root becomes an inner node over it, which then splits like any child. When
//! it is left with one child, the child's contents move up.
//!
//! **Merging.** A node used less than a quarter after a removal merges with a neighbour if the two
//! fit in three quarters of a node: the merged node then has room for the largest entry, so the
//! next insert never splits it again at once.

pub mod node;
pub mod record;
pub mod value;

pub use drain::{Drain, Drained};

#[cfg(test)]
mod tests;

use super::buffer::{BufferPool, Exclusive, Optimistic};
use super::log::{framed_len, Admitted, GroupStart, Header, Kind, Log, Lsn, TxnTail};
use super::page::{self, PageId, SizeClass};
use super::shared::{Bytes, BytesMut};
use super::store::{PageStore, StoreRecord};
use crate::io::{Completion, IO};
use crate::sync::atomic::{AtomicU32, Ordering};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::{LimboError, Result};
use node::{Layout, Limits, View};
use record::{Change, Compensation, Record, COMPENSATION};
use value::{Read, ValueRead, ValueRef, ValueWrite};

fn yield_now<T>() -> Result<IOResult<T>> {
    Ok(IOResult::IO(IOCompletions(Completion::new_yield())))
}

/// What a descent came to.
enum Found<'p> {
    /// The node at the level asked for that covers the key, and its parent with the position
    /// routing to it, unless it is the root.
    Node {
        node: Optimistic<'p>,
        parent: Option<(Optimistic<'p>, usize)>,
    },
    /// Something changed under the descent.
    Restart,
    /// The tree has no level that high: it shrank since the caller looked.
    Shorter,
    Io(IOCompletions),
}

crate::thread::thread_local! {
    /// A page of the thread's own a merge builds its node in, kept between merges.
    static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

macro_rules! found {
    ($e:expr) => {
        match $e? {
            Found::Node { node, parent } => (node, parent),
            Found::Restart | Found::Shorter => return yield_now(),
            Found::Io(io) => return Ok(IOResult::IO(io)),
        }
    };
}

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

// Declared after the macros it uses, which macro_rules scopes by position.
mod drain;

/// How a descent routes at inner nodes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Route {
    /// To the child covering the key.
    At,
    /// To the child covering the keys just above it.
    Above,
}

/// A leaf a caller reached last, kept by the caller and tried before a descent
/// ([`Tree::descend_to_leaf`]): its page and frame hint, or none. The caller's own, so no thread
/// writes what another reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Near(Option<(PageId, u32)>);

#[cfg(test)]
crate::thread::thread_local! {
    /// Descents a caller's leaf spared on this thread, for tests to see which way a look went.
    pub(crate) static NEAR_TAKEN: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Leaves reached on this thread, by a caller's leaf or a descent, for tests to count an
    /// operation's positionings.
    pub(crate) static LEAF_REACHES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Splits begun on this thread, for tests to tell an operation that split a node.
    pub(crate) static SPLITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

impl Near {
    pub const NONE: Self = Self(None);

    fn of(leaf: &Optimistic<'_>) -> Self {
        Self(Some((leaf.pid(), leaf.hint())))
    }

    /// The leaf's page.
    pub fn page(self) -> Option<PageId> {
        self.0.map(|(pid, _)| pid)
    }
}

/// Log space admitted for a change; none for a temporary tree.
pub struct Grant(Option<Admitted>);

pub struct Tree {
    io: Arc<dyn IO>,
    pool: Arc<BufferPool>,
    store: Arc<PageStore>,
    root: PageId,
    root_hint: AtomicU32,
    limits: Limits,
    /// Bytes at the start of every value kept in the leaf even when the rest goes out of line:
    /// a version header the leaf must show without reading the value.
    head: usize,
}

/// What a put should do, and what it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Only if the key is absent.
    Insert,
    /// Only if the key is present.
    Update,
    Upsert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Inserted,
    Updated,
    /// An insert found the key present, or an update found it absent; nothing changed.
    Unchanged,
    /// The entry was not the one the put expected ([`Put::expecting`]); nothing changed.
    Changed,
}

impl Tree {
    /// A new, empty tree whose nodes are of `class`: its root page, which names it.
    pub fn create(
        io: &Arc<dyn IO>,
        pool: &Arc<BufferPool>,
        store: &Arc<PageStore>,
        class: SizeClass,
    ) -> Result<IOResult<PageId>> {
        check_budget(store.log(), class, Limits::of(class))?;
        Self::create_in(io, pool, store, class, false)
    }

    /// A new, empty temporary tree (DESIGN.md §6.4), whose nodes and values are temporary pages:
    /// nothing it does is logged, and it does not outlive the process.
    pub fn create_temporary(
        io: &Arc<dyn IO>,
        pool: &Arc<BufferPool>,
        store: &Arc<PageStore>,
        class: SizeClass,
    ) -> Result<IOResult<PageId>> {
        Self::create_in(io, pool, store, class, true)
    }

    fn create_in(
        io: &Arc<dyn IO>,
        pool: &Arc<BufferPool>,
        store: &Arc<PageStore>,
        class: SizeClass,
        temporary: bool,
    ) -> Result<IOResult<PageId>> {
        let adopt_len = PageStore::group_record_len(1);
        let init_len = framed_len(8 + 4 + class.size());
        let mut grant = if temporary {
            Grant(None)
        } else {
            Grant(Some(done!(store.log().admit(init_len + adopt_len))))
        };
        let mut page = if temporary {
            done!(pool.allocate_temporary(class))
        } else {
            done!(pool.allocate(class))
        };
        let pid = page.pid();
        let image = empty_root(&mut page);
        let tree = Self::open(io.clone(), pool.clone(), store.clone(), pid);
        let lsns = tree.log_batch(
            &mut grant,
            &[Record {
                pid,
                change: Change::Init { image: &image },
            }],
            &[StoreRecord::Adopt { pids: vec![pid] }],
        );
        page.changed(lsns[0]);
        Ok(IOResult::Done(pid))
    }

    /// A new, empty tree whose nodes are of `class`, made in a transaction: one group of the
    /// transaction's record of `kind`, whose payload is the root and then `extra`, the root's image
    /// and the root's adoption, so no crash leaves the root adopted with no chain naming it.
    /// Returns the root and where the group starts.
    pub fn create_logged(
        pool: &Arc<BufferPool>,
        store: &Arc<PageStore>,
        class: SizeClass,
        tail: &TxnTail,
        kind: Kind,
        extra: &[u8],
    ) -> Result<IOResult<(PageId, GroupStart)>> {
        check_budget(store.log(), class, Limits::of(class))?;
        let lead_len = framed_len(8 + extra.len());
        let adopt_len = PageStore::group_record_len(1);
        let init_len = framed_len(8 + 4 + class.size());
        let mut admitted = done!(store.log().admit(lead_len + init_len + adopt_len));
        let mut page = done!(pool.allocate(class));
        let pid = page.pid();
        let image = empty_root(&mut page);
        let lead = [&pid.raw().to_le_bytes()[..], extra].concat();
        let init = Record {
            pid,
            change: Change::Init { image: &image },
        };
        let init_header = Header {
            kind: init.kind(),
            flags: 0,
            txn: 0,
            prev: None,
        };
        let init = init.encode();
        let adopt = StoreRecord::Adopt { pids: vec![pid] };
        let (adopt_header, adopt_payload) = PageStore::group_record(&adopt);
        let pieces: [[&[u8]; 1]; 3] = [[&lead], [&init], [&adopt_payload]];
        let records = [
            (tail.header(kind), &pieces[0][..]),
            (init_header, &pieces[1][..]),
            (adopt_header, &pieces[2][..]),
        ];
        let (start, lsns) = store
            .log()
            .append_group_with(&mut admitted, &records, |at| tail.reserved(at));
        store.grouped(&adopt, start.get(), lsns[2]);
        page.changed(lsns[1]);
        Ok(IOResult::Done((pid, start)))
    }

    /// The tree whose root is `root`.
    pub fn open(
        io: Arc<dyn IO>,
        pool: Arc<BufferPool>,
        store: Arc<PageStore>,
        root: PageId,
    ) -> Self {
        Self {
            io,
            pool,
            store,
            root,
            root_hint: AtomicU32::new(0),
            limits: Limits::of(root.class()),
            head: 0,
        }
    }

    /// The tree with every value starting with `head` bytes kept in the leaf. Keys are shorter by
    /// as much.
    pub fn with_head(mut self, head: usize) -> Self {
        self.head = head;
        self.limits = self.limits.with_head(head);
        self
    }

    pub fn head(&self) -> usize {
        self.head
    }

    pub fn root(&self) -> PageId {
        self.root
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Whether the tree is temporary: its root, and so every page of it, is a temporary page.
    pub fn temporary(&self) -> bool {
        self.root.is_temporary()
    }

    /// The log the tree's changes go to; `None` for a temporary tree.
    fn journal(&self) -> Option<&Arc<Log>> {
        (!self.temporary()).then(|| self.store.log())
    }

    /// Admits `len` bytes of log for a change; a temporary tree needs none.
    fn admit(&self, len: u64) -> Result<IOResult<Grant>> {
        Ok(match self.journal() {
            Some(log) => match log.admit(len)? {
                IOResult::Done(a) => IOResult::Done(Grant(Some(a))),
                IOResult::IO(io) => IOResult::IO(io),
            },
            None => IOResult::Done(Grant(None)),
        })
    }

    /// A new node page, of the tree's kind.
    fn allocate(&self) -> Result<IOResult<Exclusive<'_>>> {
        if self.temporary() {
            self.pool.allocate_temporary(self.class())
        } else {
            self.pool.allocate(self.class())
        }
    }

    fn class(&self) -> SizeClass {
        self.root.class()
    }

    /// Bytes of log a structure change takes at most: two node images, two records carrying a
    /// key, and the adoption or abandonment of one page.
    fn smo_len(&self) -> u64 {
        smo_len(self.class(), self.limits)
    }

    /// Appends the records of one change: alone, or as a group when there are several. Returns
    /// the LSNs of the node records, in order. A temporary tree appends nothing: its node changes
    /// get temporary LSNs, and its pages need no adoption.
    fn log_batch(
        &self,
        grant: &mut Grant,
        nodes: &[Record<'_>],
        store: &[StoreRecord],
    ) -> Vec<Lsn> {
        self.log_as(grant, None, nodes, store).1
    }

    /// As [`log_batch`](Self::log_batch), with the first node record in transaction `txn`, its
    /// group entered in the writer registry as it is reserved. Returns where the records start
    /// too; `None` in a temporary tree.
    fn log_as(
        &self,
        grant: &mut Grant,
        txn: Option<&TxnTail>,
        nodes: &[Record<'_>],
        store: &[StoreRecord],
    ) -> (Option<GroupStart>, Vec<Lsn>) {
        let (start, lsns, _) = self.log_marked(grant, txn, nodes, store, &[]);
        (start, lsns)
    }

    /// As [`log_as`](Self::log_as) for one node record alone, its pieces copied straight into
    /// the log: nothing is assembled on the way. Returns where it starts and its LSN.
    fn log_one(
        &self,
        grant: &mut Grant,
        txn: Option<&TxnTail>,
        record: &Record<'_>,
    ) -> (Option<GroupStart>, Lsn) {
        let Some(admitted) = &mut grant.0 else {
            return (None, self.pool.temporary_lsn());
        };
        let header = match txn {
            Some(t) => t.header(record.kind()),
            None => Header {
                kind: record.kind(),
                flags: 0,
                txn: 0,
                prev: None,
            },
        };
        // A transaction's group enters the writer registry before its copy completes.
        let reserved = |at: GroupStart| {
            if let Some(t) = txn {
                t.reserved(at);
            }
        };
        let log = self.store.log();
        let (start, lsn) =
            record.with_pieces(|pieces| log.append_with(admitted, &header, pieces, reserved));
        (Some(start), lsn)
    }

    /// As [`log_as`](Self::log_as), with the records of retired marks ([`PageStore::marks`])
    /// after the rest; returns too where they start and end, to change the marks with.
    fn log_marked(
        &self,
        grant: &mut Grant,
        txn: Option<&TxnTail>,
        nodes: &[Record<'_>],
        store: &[StoreRecord],
        marks: &[(Header, Vec<u8>)],
    ) -> (Option<GroupStart>, Vec<Lsn>, Option<(Lsn, Lsn)>) {
        let Some(admitted) = &mut grant.0 else {
            assert!(marks.is_empty(), "a temporary tree retires nothing");
            return (
                None,
                nodes.iter().map(|_| self.pool.temporary_lsn()).collect(),
                None,
            );
        };
        let mut payloads: Vec<(Header, Vec<u8>)> = nodes
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let header = match txn.filter(|_| i == 0) {
                    Some(t) => t.header(r.kind()),
                    None => Header {
                        kind: r.kind(),
                        flags: 0,
                        txn: 0,
                        prev: None,
                    },
                };
                (header, r.encode())
            })
            .collect();
        payloads.extend(store.iter().map(PageStore::group_record));
        payloads.extend(marks.iter().cloned());
        let pieces: Vec<[&[u8]; 1]> = payloads.iter().map(|(_, p)| [p.as_slice()]).collect();
        let records: Vec<(Header, &[&[u8]])> = payloads
            .iter()
            .zip(&pieces)
            .map(|((h, _), p)| (*h, &p[..]))
            .collect();
        let log = self.store.log();
        // A transaction's group enters the writer registry before its copy completes.
        let reserved = |at: GroupStart| {
            if let Some(t) = txn {
                t.reserved(at);
            }
        };
        let (start, lsns) = if records.len() == 1 {
            let (start, lsn) = log.append_with(admitted, &records[0].0, records[0].1, reserved);
            (start, vec![lsn])
        } else {
            log.append_group_with(admitted, &records, reserved)
        };
        for (s, &lsn) in store.iter().zip(&lsns[nodes.len()..]) {
            self.store.grouped(s, start.get(), lsn);
        }
        // The marks start where the record before them ends, which a node's always is.
        let marked = (!marks.is_empty()).then(|| {
            let first = nodes.len() + store.len();
            (lsns[first - 1], *lsns.last().expect("a record"))
        });
        (Some(start), lsns[..nodes.len()].to_vec(), marked)
    }

    // ---------------------------------------------------------------- descending

    /// The leaf covering `key` by `route`, for a caller that needs no parent: `near`, the leaf the
    /// caller last reached, if it still answers the key, else a descent. That leaf is in its
    /// frame, a leaf of this tree not abandoned by a merge (the frame holding it says so:
    /// [`BufferPool::fix_optimistic_at`]), and its fences answer the key, so it is the one leaf
    /// that does: a split or merge moving them changes its version, which every read of it
    /// validates. A leaf holds the keys above its lower fence up to its upper fence, so it answers
    /// a key at it ([`Route::At`]) between the two, the upper included, and the keys just above
    /// it ([`Route::Above`]) from the lower fence, included, to below the upper.
    fn descend_to_leaf(&self, key: &[u8], route: Route, near: Near) -> Result<Found<'_>> {
        #[cfg(test)]
        LEAF_REACHES.with(|n| n.set(n.get() + 1));
        if let Some((pid, hint)) = near.0 {
            if let Some(leaf) = self.pool.fix_optimistic_at(pid, hint) {
                let answers = leaf.read(|p| -> Option<bool> {
                    let v = View::new(p);
                    if !(v.is_node() && v.root() == self.root && v.is_leaf()) {
                        return Some(false);
                    }
                    match route {
                        Route::At => v.covers(key),
                        Route::Above => v.covers_above(key),
                    }
                });
                if answers == Some(Some(true)) {
                    #[cfg(test)]
                    NEAR_TAKEN.with(|n| n.set(n.get() + 1));
                    return Ok(Found::Node {
                        node: leaf,
                        parent: None,
                    });
                }
            }
        }
        self.descend(key, 0, route)
    }

    /// Descends optimistically to the node at `level` covering `key`.
    fn descend(&self, key: &[u8], level: u8, route: Route) -> Result<Found<'_>> {
        let mut node = match self.pool.fix_optimistic(self.root, Some(&self.root_hint))? {
            IOResult::Done(n) => n,
            IOResult::IO(io) => return Ok(Found::Io(io)),
        };
        let mut parent = None;
        loop {
            let read = node.read(|p| {
                let v = View::new(p);
                if !v.is_node() || v.root() != self.root {
                    return None;
                }
                // Reached by a descent its parent validated, the node holds what the parent
                // routed here: a split or merge that moves its fences changes the parent too,
                // and the parent is validated once the child is fixed. So an inner node's fences
                // are not compared on the way; the node the descent stops at checks its own.
                let at = v.level();
                if at <= level {
                    let reaches = match route {
                        Route::At => v.covers(key)?,
                        Route::Above => v.covers(key)? || v.lower_is(key)?,
                    };
                    if !reaches {
                        return Some(Err((at, v.lower()?, v.upper()?)));
                    }
                    return Some(Ok((at, None)));
                }
                let count = v.count()?;
                let pos = match route {
                    Route::At => v.route(key)?,
                    Route::Above => v.search_above(key)?,
                };
                Some(Ok((at, Some((pos, v.child_of(pos, count)?)))))
            });
            let Some(read) = read else {
                return Ok(Found::Restart);
            };
            let Some(read) = read else {
                return Err(LimboError::Corrupt(format!(
                    "{:?} is not a node of the B-tree rooted at {:?}",
                    node.pid(),
                    self.root
                )));
            };
            let (at, down) =
                match read {
                    Ok(step) => step,
                    Err((at, lower, upper)) => {
                        return Err(LimboError::Corrupt(format!(
                        "a descent to {key:?} reached {:?} at level {at}, holding {lower:?} to \
                         {upper:?}, under {:?}",
                        node.pid(),
                        parent.as_ref().map(|(p, pos): &(Optimistic<'_>, usize)| (p.pid(), *pos))
                    )))
                    }
                };
            let Some((pos, (child, hint))) = down else {
                return Ok(if at == level {
                    Found::Node { node, parent }
                } else {
                    Found::Shorter
                });
            };
            let cell = AtomicU32::new(hint);
            // Read in only while the node still names it.
            let fixed = match self
                .pool
                .fix_optimistic_held(child, Some(&cell), &|| node.validate())
            {
                Ok(IOResult::Done(Some(c))) => c,
                Ok(IOResult::Done(None)) => return Ok(Found::Restart),
                Ok(IOResult::IO(io)) => return Ok(Found::Io(io)),
                // The child was freed since its entry was read.
                Err(LimboError::Corrupt(_)) if !node.validate() => return Ok(Found::Restart),
                Err(e) => return Err(e),
            };
            if !node.validate() {
                return Ok(Found::Restart);
            }
            // Relaxed: this thread's own cell, written by the fix above.
            let found = cell.load(Ordering::Relaxed);
            if found != hint {
                if let Some(mut ex) = node.clone().upgrade() {
                    node::set_child_hint(&mut ex.page_mut(), pos, found);
                    node = ex.release_unchanged();
                }
            }
            parent = Some((node, pos));
            node = fixed;
        }
    }

    // ---------------------------------------------------------------- reading

    /// Whether the leaf `pid` is still in the frame `hint` names, unchanged since `version`.
    fn holds(&self, pid: PageId, hint: u32, version: u64) -> bool {
        self.pool
            .fix_optimistic_at(pid, hint)
            .is_some_and(|l| l.version() == version)
    }

    /// The value of `key`, if present. Step it until done.
    pub fn lookup(&self, op: &mut Lookup) -> Result<IOResult<Option<Vec<u8>>>> {
        loop {
            if let (Some(read), Some((pid, hint, version))) = (&mut op.read, op.held) {
                match done!(read.step(&self.pool, &|| self.holds(pid, hint, version))) {
                    Read::Value(v) => {
                        op.read = None;
                        op.failed = None;
                        let mut value = std::mem::take(&mut op.head);
                        value.extend_from_slice(&v);
                        return Ok(IOResult::Done(Some(value)));
                    }
                    Read::Changed => op.failed = op.read.take().map(|r| r.reference()),
                    Read::Unheld => {}
                }
                op.held = None;
            }
            let (leaf, _) = found!(self.descend_to_leaf(&op.key, Route::At, Near::NONE));
            let Some(found) = leaf.read(|p| {
                let v = View::new(p);
                let (pos, exact) = v.search(&op.key)?;
                if !exact {
                    return Some(None);
                }
                Some(Some(v.value(pos)?))
            }) else {
                continue;
            };
            match found.ok_or_else(|| corrupt_node(leaf.pid()))? {
                None => return Ok(IOResult::Done(None)),
                Some((value, false)) => return Ok(IOResult::Done(Some(value))),
                Some((stored, true)) => {
                    let (head, reference) = self
                        .split_reference(&stored)
                        .ok_or_else(|| corrupt_node(leaf.pid()))?;
                    if op.failed == Some(reference) {
                        return Err(corrupt_value(reference));
                    }
                    // The value of a read under way goes on from where it was.
                    if op.read.as_ref().is_none_or(|r| r.reference() != reference) {
                        op.head = head.to_vec();
                        op.read = Some(ValueRead::new(reference));
                    }
                    op.held = Some((leaf.pid(), leaf.hint(), leaf.version()));
                }
            }
        }
    }

    /// `key`'s entry as the leaf holds it, with nothing read out of line: its head, and the rest
    /// of its value or the reference to it. Step it until done.
    pub fn stored(&self, key: &[u8]) -> Result<IOResult<Option<Stored>>> {
        self.stored_near(key, Near::NONE)
    }

    /// [`stored`](Self::stored), tried first at `near`.
    pub fn stored_near(&self, key: &[u8], near: Near) -> Result<IOResult<Option<Stored>>> {
        let looked = done!(self.look(key, near));
        Ok(IOResult::Done(self.stored_of(&looked)?))
    }

    /// The entry a look found, as [`stored`](Self::stored) gives it.
    pub fn stored_of(&self, looked: &Looked<'_>) -> Result<Option<Stored>> {
        Ok(match &looked.present {
            None => None,
            Some((stored, false)) => Some(Stored {
                head: stored[..self.head.min(stored.len())].to_vec(),
                value: StoredValue::Inline(stored[self.head.min(stored.len())..].to_vec()),
            }),
            Some((stored, true)) => {
                let (head, reference) = self
                    .split_reference(stored)
                    .ok_or_else(|| corrupt_node(looked.leaf.pid()))?;
                Some(Stored {
                    head: head.to_vec(),
                    value: StoredValue::OutOfLine(reference),
                })
            }
        })
    }

    /// An out-of-line value as the leaf stores it: its head, and the reference to the rest.
    fn split_reference<'v>(&self, stored: &'v [u8]) -> Option<(&'v [u8], ValueRef)> {
        let head = stored.get(..self.head)?;
        Some((head, ValueRef::decode(&stored[self.head..])?))
    }

    // ---------------------------------------------------------------- writing

    /// Puts the value of a key. Step it until done.
    pub fn put(&self, op: &mut Put) -> Result<IOResult<Outcome>> {
        loop {
            // Whether the key is there, and what an out-of-line value it holds takes: learned
            // before writing anything, and holding still once the leaf is latched at the version
            // the look read.
            let Looked {
                leaf,
                pos,
                exact,
                present,
            } = done!(self.look(&op.key, op.near));
            op.near = Near::of(&leaf);
            if let Some(done) = self.put_from(op, leaf, pos, exact, present)? {
                return Ok(done);
            }
        }
    }

    /// The leaf of `key` looked at, tried first at `near`: where the key is or goes, and its
    /// entry, the leaf held optimistically at the version read. Step it until done.
    pub fn look(&self, key: &[u8], near: Near) -> Result<IOResult<Looked<'_>>> {
        let mut near = near;
        loop {
            let (leaf, _) = found!(self.descend_to_leaf(key, Route::At, near));
            near = Near::NONE;
            let Some(seen) = leaf.read(|p| look_at(View::new(p), key)) else {
                continue;
            };
            let Some((pos, exact, present)) = seen.ok_or_else(|| corrupt_node(leaf.pid()))? else {
                // The leaf split since the descent read its parent.
                continue;
            };
            return Ok(IOResult::Done(Looked {
                leaf,
                pos,
                exact,
                present,
            }));
        }
    }

    /// The look `kept` was, while its leaf is unchanged since in the frame it was in: `None` once
    /// the leaf changed or left the frame, when the caller looks again. Equal latch versions of a
    /// frame holding one page mean no change between ([`Optimistic::version`]), so the leaf still
    /// covers the key, which is at the kept position or goes there, with the kept entry.
    pub fn redeem(&self, kept: Kept) -> Option<Looked<'_>> {
        let leaf = self.pool.fix_optimistic_at(kept.pid, kept.hint)?;
        (leaf.version() == kept.version).then_some(Looked {
            leaf,
            pos: kept.pos,
            exact: kept.exact,
            present: kept.present,
        })
    }

    /// Whether the leaf of `kept` is unchanged since its look, as [`redeem`](Self::redeem) would
    /// find it.
    pub fn holds_kept(&self, kept: &Kept) -> bool {
        self.holds(kept.pid, kept.hint, kept.version)
    }

    /// [`put`](Self::put) from `looked`, a look at the key's leaf this step made: written there if
    /// the leaf is still as the look found it, else put as by `put`. Step it until done.
    pub fn put_looked(&self, op: &mut Put, looked: Looked<'_>) -> Result<IOResult<Outcome>> {
        let Looked {
            leaf,
            pos,
            exact,
            present,
        } = looked;
        op.near = Near::of(&leaf);
        match self.put_from(op, leaf, pos, exact, present)? {
            Some(done) => Ok(done),
            None => self.put(op),
        }
    }

    /// The rest of a put, from a look at its leaf: `None` to look again.
    fn put_from(
        &self,
        op: &mut Put,
        leaf: Optimistic<'_>,
        pos: usize,
        exact: bool,
        present: Option<(Vec<u8>, bool)>,
    ) -> Result<Option<IOResult<Outcome>>> {
        macro_rules! step {
            ($e:expr) => {
                match $e? {
                    IOResult::Done(v) => v,
                    IOResult::IO(io) => return Ok(Some(IOResult::IO(io))),
                }
            };
        }
        match (op.mode, &present) {
            (Mode::Insert, Some(_)) | (Mode::Update, None) => {
                return Ok(Some(IOResult::Done(Outcome::Unchanged)))
            }
            _ => {}
        }
        assert_eq!(op.head_len, self.head, "a put's head is not the tree's");
        if !step!(op.old_pages(&self.pool, self.head, present.as_ref(), &|| leaf.validate())) {
            return Ok(None);
        }
        if let Some((reference, pages)) = &mut op.restoring {
            if pages.is_none() {
                // Retired, so no page of it is freed under this read.
                match step!(value::pages(&self.pool, *reference, &|| true)) {
                    Ok(p) => *pages = Some(p),
                    Err(_) => return Err(corrupt_value(*reference)),
                }
            }
        }
        let out_of_line =
            op.restoring.is_some() || !self.limits.inline(op.key.len(), self.head + op.value_len);
        if out_of_line && op.restoring.is_none() {
            let write = op.write.get_or_insert_with(|| {
                ValueWrite::new(
                    op.entry.split_off(op.head_len),
                    self.io.generate_random_number() as u64,
                    self.store.clone(),
                )
            });
            step!(write.step(&self.pool, self.journal()));
        }
        // As the leaf stores it: the entry itself, or its head and the reference to a value out
        // of line.
        let reference = match (&op.write, &op.restoring) {
            (Some(w), _) => Some(w.reference()),
            (None, Some((reference, _))) => Some(*reference),
            (None, None) => None,
        };
        let referred;
        let stored: &[u8] = match reference {
            Some(r) => {
                referred = [&op.entry[..op.head_len], &r.encode()].concat();
                &referred
            }
            None => &op.entry,
        };
        let adopt = op.write.as_ref().map(|w| w.pages()).unwrap_or_default();
        let replaced = op.old.as_ref().map(|o| o.1.clone()).unwrap_or_default();
        let (abandon, retire) = if op.retire_old {
            (Vec::new(), replaced)
        } else {
            (replaced, Vec::new())
        };
        let reinstate = op
            .restoring
            .as_ref()
            .and_then(|(_, pages)| pages.clone())
            .unwrap_or_default();
        // Marks change in the group, under latches taken once the leaf's is held.
        let marks = if retire.is_empty() && reinstate.is_empty() {
            None
        } else {
            assert!(
                self.journal().is_some(),
                "a temporary tree retires and reinstates nothing"
            );
            Some(step!(self.store.marks(&retire, &reinstate)))
        };
        let mark_records = marks.as_ref().map(|m| m.records()).unwrap_or_default();
        let marks_len: u64 = mark_records
            .iter()
            .map(|(_, payload)| framed_len(payload.len()))
            .sum();
        // What a transaction's update logs to undo it: the entry its statement found, borrowed.
        let found: Option<(&[u8], bool)> = match &op.found {
            Some(found) => found.as_ref().map(|(b, o)| (b.as_slice(), *o)),
            None => present.as_ref().map(|(b, o)| (b.as_slice(), *o)),
        };
        // A transaction's change names the tree, and an update of one the value's length and
        // the entry undoing it puts back.
        let undo = match (&op.compensation, &op.txn, &present) {
            (Some(_), ..) => COMPENSATION,
            (None, Some(_), Some(_)) => 8 + 4 + 1 + found.map_or(0, |(b, _)| b.len()),
            (None, Some(_), None) => 8,
            (None, None, _) => 0,
        };
        let len = framed_len(8 + 3 + op.key.len() + stored.len() + undo)
            + store_len(&adopt)
            + store_len(&abandon)
            + marks_len;
        if op.admitted.as_ref().is_none_or(|(_, l)| *l < len) {
            op.admitted = None;
            op.admitted = Some((step!(self.admit(len)), len));
        }
        // Latched at the version the look read, so the leaf is as the look found it: still
        // covering the key, which is at `pos` or goes there, with the entry `present`.
        let Some(mut ex) = leaf.upgrade() else {
            return yield_now().map(Some);
        };
        let v = View::new(ex.page());
        let now = &present;
        let need = match now {
            None => v.entry_size(&op.key, stored.len()),
            Some((old, _)) => Some(stored.len().saturating_sub(old.len())),
        }
        .ok_or_else(|| corrupt_node(ex.pid()))?;
        if v.free().ok_or_else(|| corrupt_node(ex.pid()))? < need {
            drop(ex);
            // A split admits its own log space; holding this meanwhile could leave too
            // little of the log buffer for it.
            op.admitted = None;
            step!(self.make_room(&op.key, 0, need));
            return Ok(None);
        }
        let pid = ex.pid();
        if op.expected.as_ref().is_some_and(|e| e != now) {
            drop(ex);
            return Ok(Some(IOResult::Done(Outcome::Changed)));
        }
        let locked = match &marks {
            Some(m) => match m.lock(&self.store) {
                Some(locked) => Some(locked),
                // Held, or grown since: let the leaf go, and make the marks again.
                None => {
                    drop(ex);
                    return yield_now().map(Some);
                }
            },
            None => None,
        };
        let in_txn = op.txn.is_some();
        let change = match (now, op.compensation) {
            (Some(_), Some(of)) => Change::Restore {
                key: &op.key,
                value: stored,
                out_of_line,
                of,
            },
            (None, Some(_)) => {
                drop(ex);
                return Err(LimboError::InternalError(
                    "a compensation put back a key it found no entry of".into(),
                ));
            }
            // A transaction's first change of the key, its value inline before and after and
            // as long: the head, and the bytes that differ, changed in place. What undoes it is
            // the entry it replaced, which is what its statement found.
            (Some((old, false)), None)
                if in_txn
                    && op.found.is_none()
                    && !out_of_line
                    && self.head > 0
                    && old.len() == stored.len() =>
            {
                let h = self.head;
                let (was, is) = (&old[h..], &stored[h..]);
                let same = was.iter().zip(is).take_while(|(a, b)| a == b).count();
                let tail = was[same..]
                    .iter()
                    .rev()
                    .zip(is[same..].iter().rev())
                    .take_while(|(a, b)| a == b)
                    .count();
                let differ = same..was.len() - tail;
                Change::Patch {
                    key: &op.key,
                    head: &stored[..h],
                    at: same,
                    bytes: &is[differ.clone()],
                    root: self.root,
                    old_head: &old[..h],
                    old_bytes: &was[differ],
                }
            }
            (Some(_), None) => Change::Update {
                key: &op.key,
                value: stored,
                out_of_line,
                undo: in_txn.then_some((self.root, found)),
            },
            (None, None) => Change::Insert {
                key: &op.key,
                value: stored,
                out_of_line,
                undo: in_txn.then_some(self.root),
            },
        };
        let record = Record { pid, change };
        record.apply_at(&mut ex.page_mut(), Some((pos, exact)))?;
        let mut store = Vec::new();
        if !adopt.is_empty() {
            store.push(StoreRecord::Adopt { pids: adopt });
        }
        if !abandon.is_empty() {
            store.push(StoreRecord::Abandon {
                pids: abandon.clone(),
            });
        }
        let (mut admitted, _) = op.admitted.take().expect("admitted");
        let (group, lsn) = if store.is_empty() && mark_records.is_empty() {
            self.log_one(&mut admitted, op.txn.as_ref(), &record)
        } else {
            let (group, lsns, marked) = self.log_marked(
                &mut admitted,
                op.txn.as_ref(),
                &[record],
                &store,
                &mark_records,
            );
            if let (Some(locked), Some((start, end))) = (locked, marked) {
                locked.commit(&self.store, start, end);
            }
            (group, lsns[0])
        };
        op.group = group.filter(|_| op.txn.is_some());
        ex.changed(lsn);
        op.lsn = lsn;
        drop(ex);
        if let Some(w) = op.write.as_mut() {
            w.adopted();
        }
        // Pages retired stay: a snapshot may still read the version they hold.
        if !abandon.is_empty() {
            self.free_abandoned(abandon)?;
        }
        op.old = None;
        op.write = None;
        Ok(Some(IOResult::Done(if exact {
            Outcome::Updated
        } else {
            Outcome::Inserted
        })))
    }

    /// Removes `key`; whether it was there. Step it until done.
    pub fn remove(&self, op: &mut Remove) -> Result<IOResult<bool>> {
        loop {
            if let Some(level) = op.merging {
                done!(self.merge(&op.key, level, &mut op.merging));
                if op.merging.is_none() {
                    return Ok(IOResult::Done(true));
                }
                continue;
            }
            let (leaf, _) = found!(self.descend_to_leaf(&op.key, Route::At, Near::NONE));
            let Some(seen) = leaf.read(|p| look(View::new(p), &op.key)) else {
                continue;
            };
            let Some(present) = seen.ok_or_else(|| corrupt_node(leaf.pid()))? else {
                continue;
            };
            let Some(present) = present else {
                return Ok(IOResult::Done(false));
            };
            if op.expected.as_ref().is_some_and(|e| *e != present) {
                return Ok(IOResult::Done(false));
            }
            if !done!(op.old_pages(&self.pool, self.head, &present, &|| leaf.validate())) {
                continue;
            }
            let mut abandon = op.old.as_ref().map(|o| o.1.clone()).unwrap_or_default();
            abandon.extend(&op.also);
            let unit = op.compensation.map_or(0, |_| COMPENSATION);
            let len = framed_len(8 + unit + op.key.len()) + store_len(&abandon);
            if op.admitted.as_ref().is_none_or(|(_, l)| *l < len) {
                op.admitted = None;
                op.admitted = Some((done!(self.admit(len)), len));
            }
            let Some(mut ex) = leaf.upgrade() else {
                return yield_now();
            };
            let v = View::new(ex.page());
            let (pos, exact) = v.search(&op.key).ok_or_else(|| corrupt_node(ex.pid()))?;
            let now = exact.then(|| v.value(pos)).flatten();
            if v.covers(&op.key) != Some(true) || now.as_ref() != Some(&present) {
                drop(ex);
                continue;
            }
            let change = match op.compensation {
                Some(of) => Change::Unput { key: &op.key, of },
                None => Change::Remove { key: &op.key },
            };
            let record = Record {
                pid: ex.pid(),
                change,
            };
            record.apply(&mut ex.page_mut())?;
            let store = if abandon.is_empty() {
                Vec::new()
            } else {
                vec![StoreRecord::Abandon { pids: abandon }]
            };
            let (mut admitted, _) = op.admitted.take().expect("admitted");
            let (group, lsns) = self.log_as(&mut admitted, op.txn.as_ref(), &[record], &store);
            op.group = group.filter(|_| op.txn.is_some());
            ex.changed(lsns[0]);
            op.lsn = lsns[0];
            let underfull = self.underfull(&View::new(ex.page()));
            drop(ex);
            if let Some((_, old)) = op.old.take() {
                self.free_abandoned(old)?;
            }
            self.free_abandoned(std::mem::take(&mut op.also))?;
            if !underfull {
                return Ok(IOResult::Done(true));
            }
            op.merging = Some(0);
        }
    }

    /// Frees pages just abandoned, handing what cannot be freed at once to the checkpointer.
    fn free_abandoned(&self, pids: Vec<PageId>) -> Result<()> {
        let mut pids = pids.into_iter();
        while let Some(pid) = pids.next() {
            if let IOResult::IO(_) = self.pool.free_unreferenced(pid)? {
                self.store.orphaned(pid);
                pids.for_each(|p| self.store.orphaned(p));
                break;
            }
        }
        Ok(())
    }

    fn underfull<P: Bytes + ?Sized>(&self, v: &View<'_, P>) -> bool {
        let used = v.page().len() - v.free().unwrap_or(0) - node::SLOTS;
        used < self.limits.capacity / 4
    }

    // ---------------------------------------------------------------- structure changes

    /// Splits the node at `level` covering `key` until it has `need` bytes free, splitting its
    /// ancestors first where they lack room for the separator.
    fn make_room(&self, key: &[u8], level: u8, need: usize) -> Result<IOResult<()>> {
        loop {
            match done!(self.split(key, level, need)) {
                Split::Done => return Ok(IOResult::Done(())),
                Split::Again => {}
                Split::ParentFull(need) => done!(self.make_room(key, level + 1, need)),
            }
        }
    }

    /// One split of the node at `level` covering `key`, if it lacks `need` bytes.
    fn split(&self, key: &[u8], level: u8, need: usize) -> Result<IOResult<Split>> {
        #[cfg(test)]
        SPLITS.with(|n| n.set(n.get() + 1));
        let mut admitted = done!(self.admit(self.smo_len()));
        let (node, parent) = match self.descend(key, level, Route::At)? {
            Found::Node { node, parent } => (node, parent),
            // The node to split merged away: the caller looks again.
            Found::Shorter => return Ok(IOResult::Done(Split::Done)),
            Found::Restart => return yield_now(),
            Found::Io(io) => return Ok(IOResult::IO(io)),
        };
        let Some((parent, pos)) = parent else {
            return self.grow(node, need, &mut admitted);
        };
        let Some(mut p) = parent.upgrade() else {
            return yield_now();
        };
        let Some(mut n) = node.upgrade() else {
            return yield_now();
        };
        let pv = View::new(p.page());
        let nv = View::new(n.page());
        let untouched = |p: Exclusive<'_>, n: Exclusive<'_>, outcome| {
            p.release_unchanged();
            n.release_unchanged();
            Ok(IOResult::Done(outcome))
        };
        if pv.child(pos).map(|c| c.0) != Some(n.pid()) || nv.covers(key) != Some(true) {
            return untouched(p, n, Split::Again);
        }
        if nv.free().ok_or_else(|| corrupt_node(n.pid()))? >= need {
            return untouched(p, n, Split::Done);
        }
        let count = nv.count().ok_or_else(|| corrupt_node(n.pid()))?;
        let node::Shape {
            root,
            level,
            lower,
            upper,
            next,
        } = node::shape(n.page());
        let at = nv.search(key).ok_or_else(|| corrupt_node(n.pid()))?.0;
        let right = |keep: usize| {
            if level == 0 {
                keep..count
            } else {
                keep + 1..count
            }
        };
        // Each half with its own fences, so its own prefix: what it takes, whole.
        let capacity = self.limits.capacity;
        let fits = |keep: usize, sep: &[u8]| {
            let half = |range, lower: Option<&[u8]>, upper: Option<&[u8]>| {
                let layout = Layout {
                    root,
                    level,
                    lower,
                    upper,
                    next,
                };
                nv.entries_size(range, layout.prefix_len())
                    .is_some_and(|size| layout.fences_len() + size <= capacity)
            };
            half(0..keep, lower.as_deref(), Some(sep))
                && half(right(keep), Some(sep), upper.as_deref())
        };
        let (keep, sep) = split_point(&nv, count, level, at, upper.is_none(), key, fits)
            .ok_or_else(|| corrupt_node(n.pid()))?;
        let sep_need = pv
            .entry_size(&sep, node::CHILD)
            .ok_or_else(|| corrupt_node(p.pid()))?;
        if pv.free().ok_or_else(|| corrupt_node(p.pid()))? < sep_need {
            return untouched(p, n, Split::ParentFull(sep_need));
        }
        let mut r = done!(self.allocate());
        // The right half's entries copied straight from the node, their keys cut again.
        let mut page = r.page_mut();
        node::build(
            &mut page,
            &Layout {
                root,
                level,
                lower: Some(&sep),
                upper: upper.as_deref(),
                next,
            },
            [],
        );
        if !node::append_from(&mut page, &nv, right(keep)) {
            return Err(corrupt_node(n.pid()));
        }
        let image = node::image(r.page());
        let split = Record {
            pid: n.pid(),
            change: Change::Split {
                right: r.pid(),
                keep,
                sep: &sep,
            },
        };
        split.apply(&mut n.page_mut())?;
        let branch = Record {
            pid: p.pid(),
            change: Change::Branch {
                left: n.pid(),
                right: r.pid(),
                sep: &sep,
            },
        };
        branch.apply(&mut p.page_mut())?;
        let init = Record {
            pid: r.pid(),
            change: Change::Init { image: &image },
        };
        let lsns = self.log_batch(
            &mut admitted,
            &[init, split, branch],
            &[StoreRecord::Adopt {
                pids: vec![r.pid()],
            }],
        );
        r.changed(lsns[0]);
        n.changed(lsns[1]);
        p.changed(lsns[2]);
        // Hints the records leave at 0.
        if level == 0 {
            node::set_next_hint(&mut n.page_mut(), r.hint());
        }
        let at = View::new(p.page()).search(&sep).unwrap().0;
        node::set_child_hint(&mut p.page_mut(), at, n.hint());
        node::set_child_hint(&mut p.page_mut(), at + 1, r.hint());
        Ok(IOResult::Done(Split::Done))
    }

    /// Makes room in the root, which cannot move: its contents go to a new child, and it becomes
    /// an inner node over that child alone, which the next split splits like any other.
    fn grow(
        &self,
        root: Optimistic<'_>,
        need: usize,
        admitted: &mut Grant,
    ) -> Result<IOResult<Split>> {
        let Some(mut root) = root.upgrade() else {
            return yield_now();
        };
        let v = View::new(root.page());
        if v.free().ok_or_else(|| corrupt_node(root.pid()))? >= need {
            root.release_unchanged();
            return Ok(IOResult::Done(Split::Done));
        }
        let level = v.level();
        let mut child = done!(self.allocate());
        let image = node::image(root.page());
        node::apply_image(&mut child.page_mut(), &image).ok_or_else(|| corrupt_node(root.pid()))?;
        node::build(
            &mut root.page_mut(),
            &Layout {
                root: self.root,
                level: level + 1,
                lower: None,
                upper: None,
                next: (child.pid(), child.hint()),
            },
            [],
        );
        let root_image = node::image(root.page());
        let lsns = self.log_batch(
            admitted,
            &[
                Record {
                    pid: child.pid(),
                    change: Change::Init { image: &image },
                },
                Record {
                    pid: self.root,
                    change: Change::Init { image: &root_image },
                },
            ],
            &[StoreRecord::Adopt {
                pids: vec![child.pid()],
            }],
        );
        child.changed(lsns[0]);
        root.changed(lsns[1]);
        Ok(IOResult::Done(Split::Again))
    }

    /// Merges the node at `level` covering `key` with a neighbour if it is underfull and the two
    /// fit; then its parent, if that is left underfull, and the root, if left with one child.
    /// `merging` holds the level still to look at, `None` once done.
    fn merge(&self, key: &[u8], level: u8, merging: &mut Option<u8>) -> Result<IOResult<()>> {
        let mut admitted = done!(self.admit(self.smo_len()));
        let (node, parent) = match self.descend(key, level, Route::At)? {
            Found::Node {
                node,
                parent: Some(parent),
            } => (node, parent),
            // The root, or a level the tree no longer has: nothing to merge.
            Found::Node { parent: None, .. } | Found::Shorter => {
                *merging = None;
                return Ok(IOResult::Done(()));
            }
            Found::Restart => return yield_now(),
            Found::Io(io) => return Ok(IOResult::IO(io)),
        };
        let (parent, pos) = parent;
        let Some(look) = parent.read(|p| {
            let v = View::new(p);
            let count = v.count()?;
            // Pairs by their left position: with the right neighbour first, then the left. The
            // left of a pair goes into the right, which keeps its place in the parent.
            let pairs: Vec<Pair> = [pos, pos.wrapping_sub(1)]
                .into_iter()
                .filter(|&l| l < count)
                .map(|l| Some((l, v.child(l)?, v.child(l + 1)?)))
                .collect::<Option<_>>()?;
            Some(pairs)
        }) else {
            return yield_now();
        };
        let pairs = look.ok_or_else(|| corrupt_node(parent.pid()))?;
        if pairs.is_empty() {
            // The parent has this child alone: the root then shrinks, and any other parent
            // merges at its own level.
            if parent.pid() == self.root {
                return self.shrink(parent, node, merging, &mut admitted);
            }
            *merging = Some(level + 1);
            return Ok(IOResult::Done(()));
        }
        for (left_pos, (left, left_hint), (right, right_hint)) in pairs {
            match done!(self.merge_pair(
                &parent,
                &node,
                pos,
                level,
                (left_pos, left, left_hint, right, right_hint),
                &mut admitted
            )) {
                Some(next) => {
                    *merging = next;
                    return Ok(IOResult::Done(()));
                }
                None => continue,
            }
        }
        *merging = None;
        Ok(IOResult::Done(()))
    }

    /// Merges one pair of children of `parent`, the left into the right, if the node at `pos`
    /// is underfull and the two fit. `None` if they do not; else the level to look at next.
    #[allow(clippy::too_many_arguments)]
    fn merge_pair(
        &self,
        parent: &Optimistic<'_>,
        node: &Optimistic<'_>,
        pos: usize,
        level: u8,
        (left_pos, left, left_hint, right, right_hint): (usize, PageId, u32, PageId, u32),
        admitted: &mut Grant,
    ) -> Result<IOResult<Option<Option<u8>>>> {
        let fix = |pid: PageId, hint: u32| -> Result<IOResult<Optimistic<'_>>> {
            if pid == node.pid() {
                return Ok(IOResult::Done(node.clone()));
            }
            let cell = AtomicU32::new(hint);
            match self.pool.fix_optimistic(pid, Some(&cell)) {
                Err(LimboError::Corrupt(_)) if !parent.validate() => yield_now(),
                r => r,
            }
        };
        let l = done!(fix(left, left_hint));
        let r = done!(fix(right, right_hint));
        let (Some(mut p), Some(l), Some(mut r)) =
            (parent.clone().upgrade(), l.upgrade(), r.upgrade())
        else {
            return yield_now();
        };
        let (pv, lv, rv) = (
            View::new(p.page()),
            View::new(l.page()),
            View::new(r.page()),
        );
        let related = pv.child(left_pos).map(|c| c.0) == Some(l.pid())
            && pv.child(left_pos + 1).map(|c| c.0) == Some(r.pid())
            && lv.level() == level
            && rv.level() == level;
        // Nothing changed: the parent stays at its version for the other pair.
        let untouched = |p: Exclusive<'_>, l: Exclusive<'_>, r: Exclusive<'_>| {
            p.release_unchanged();
            l.release_unchanged();
            r.release_unchanged();
        };
        if !related {
            untouched(p, l, r);
            return yield_now();
        }
        if !self.underfull(if pos == left_pos { &lv } else { &rv }) {
            untouched(p, l, r);
            return Ok(IOResult::Done(Some(None)));
        }
        let sep = pv.key(left_pos).ok_or_else(|| corrupt_node(p.pid()))?;
        let (left_shape, right_shape) = (node::shape(l.page()), node::shape(r.page()));
        let (root, lower) = (left_shape.root, left_shape.lower);
        let (upper, next) = (right_shape.upper, right_shape.next);
        let layout = Layout {
            root,
            level,
            lower: lower.as_deref(),
            upper: upper.as_deref(),
            next,
        };
        // Both nodes' entries, and an inner node's separator over the left's rightmost child,
        // under the merged fences.
        let prefix = layout.prefix_len();
        let (lcount, rcount) = (
            lv.count().ok_or_else(|| corrupt_node(l.pid()))?,
            rv.count().ok_or_else(|| corrupt_node(r.pid()))?,
        );
        let separator = if level > 0 {
            (node::SLOT + sep.len() + node::CHILD).checked_sub(prefix)
        } else {
            Some(0)
        };
        let size = lv
            .entries_size(0..lcount, prefix)
            .zip(rv.entries_size(0..rcount, prefix))
            .zip(separator)
            .map(|((l, r), s)| layout.fences_len() + l + r + s)
            .ok_or_else(|| corrupt_node(r.pid()))?;
        if size > self.limits.capacity * 3 / 4 {
            untouched(p, l, r);
            return Ok(IOResult::Done(None));
        }
        // Built in a page of the thread's own, as the right node still holds what goes into it,
        // then copied over it once.
        let built = SCRATCH.with(|scratch| {
            let mut scratch = scratch.borrow_mut();
            scratch.clear();
            scratch.resize(r.page().len(), 0);
            let merged = scratch.as_mut_slice();
            node::build(merged, &layout, []);
            let (child, hint) = lv.next();
            let whole = node::append_from(merged, &lv, 0..lcount)
                && (level == 0 || {
                    let count = View::new(&*merged).count().unwrap();
                    node::insert(merged, count, &sep, &node::child_value(child, hint), false)
                })
                && node::append_from(merged, &rv, 0..rcount);
            whole
        });
        if !built {
            return Err(corrupt_node(r.pid()));
        }
        SCRATCH.with(|scratch| {
            let merged = scratch.borrow();
            let mut page = r.page_mut();
            page.write(page::HEADER_SIZE, &merged[page::HEADER_SIZE..]);
            page::set_kind(&mut page, page::kind(merged.as_slice()));
        });
        let image = node::image(r.page());
        let remove = Record {
            pid: p.pid(),
            change: Change::Remove { key: &sep },
        };
        remove.apply(&mut p.page_mut())?;
        let lsns = self.log_batch(
            admitted,
            &[
                Record {
                    pid: r.pid(),
                    change: Change::Init { image: &image },
                },
                remove,
            ],
            &[StoreRecord::Abandon {
                pids: vec![l.pid()],
            }],
        );
        r.changed(lsns[0]);
        p.changed(lsns[1]);
        let at = View::new(p.page()).search(&sep).unwrap().0;
        node::set_child_hint(&mut p.page_mut(), at, r.hint());
        let pv = View::new(p.page());
        // A root left with one child shrinks, which a merge at this level finds; any other
        // parent left underfull merges at its own level.
        let next = if p.pid() == self.root && pv.count() == Some(0) {
            Some(level)
        } else if p.pid() != self.root && self.underfull(&pv) {
            Some(level + 1)
        } else {
            None
        };
        l.abandon();
        drop((p, r));
        let freed = l.pid();
        if let IOResult::IO(_) = self.pool.free(l)? {
            self.store.orphaned(freed);
        }
        Ok(IOResult::Done(Some(next)))
    }

    /// The root has one child left: the child's contents move up into it.
    fn shrink(
        &self,
        parent: Optimistic<'_>,
        child: Optimistic<'_>,
        merging: &mut Option<u8>,
        admitted: &mut Grant,
    ) -> Result<IOResult<()>> {
        if parent.pid() != self.root {
            *merging = None;
            return Ok(IOResult::Done(()));
        }
        let (Some(mut root), Some(child)) = (parent.upgrade(), child.upgrade()) else {
            return yield_now();
        };
        let rv = View::new(root.page());
        if rv.count() != Some(0) || rv.next().0 != child.pid() {
            return yield_now();
        }
        let image = node::image(child.page());
        node::apply_image(&mut root.page_mut(), &image).ok_or_else(|| corrupt_node(child.pid()))?;
        let lsns = self.log_batch(
            admitted,
            &[Record {
                pid: self.root,
                change: Change::Init { image: &image },
            }],
            &[StoreRecord::Abandon {
                pids: vec![child.pid()],
            }],
        );
        root.changed(lsns[0]);
        child.abandon();
        drop(root);
        let freed = child.pid();
        if let IOResult::IO(_) = self.pool.free(child)? {
            self.store.orphaned(freed);
        }
        *merging = None;
        Ok(IOResult::Done(()))
    }

    // ---------------------------------------------------------------- scanning

    /// Moves `cursor` as `seek` says. Step it until done; then whether it found an entry.
    pub fn seek(&self, cursor: &mut Cursor, key: &[u8], seek: Seek) -> Result<IOResult<bool>> {
        if cursor.pending.is_none() {
            let mut target = std::mem::take(&mut cursor.spare.2);
            target.clear();
            target.extend_from_slice(key);
            cursor.pending = Some((seek, target));
            cursor.leaf = None;
            cursor.at = None;
        }
        self.run(cursor)
    }

    /// The entry after the cursor's. Step it until done.
    pub fn next(&self, cursor: &mut Cursor) -> Result<IOResult<bool>> {
        if cursor.pending.is_none() && !cursor.step(Seek::Gt) {
            return Ok(IOResult::Done(false));
        }
        self.run(cursor)
    }

    /// The entry before the cursor's. Step it until done.
    pub fn prev(&self, cursor: &mut Cursor) -> Result<IOResult<bool>> {
        if cursor.pending.is_none() && !cursor.step(Seek::Lt) {
            return Ok(IOResult::Done(false));
        }
        self.run(cursor)
    }

    fn run(&self, cursor: &mut Cursor) -> Result<IOResult<bool>> {
        loop {
            if let (Some((key, head, read)), Some((pid, hint, version))) =
                (&mut cursor.reading, cursor.held)
            {
                match done!(read.step(&self.pool, &|| self.holds(pid, hint, version))) {
                    Read::Value(v) => {
                        let mut value = std::mem::take(head);
                        value.extend_from_slice(&v);
                        cursor.entry = Some((std::mem::take(key), value));
                        cursor.reading = None;
                        cursor.held = None;
                        cursor.pending = None;
                        cursor.failed = None;
                        return Ok(IOResult::Done(true));
                    }
                    Read::Changed => {
                        let failed = read.reference();
                        cursor.reading = None;
                        if cursor.failed == Some(failed) {
                            return Err(corrupt_value(failed));
                        }
                        cursor.failed = Some(failed);
                    }
                    Read::Unheld => {}
                }
                cursor.held = None;
            }
            let (seek, target) = cursor.pending.take().expect("a seek pending");
            // Put back for every way out but an entry or the end.
            let pending = |cursor: &mut Cursor, target| cursor.pending = Some((seek, target));
            let cached = cursor.leaf.and_then(|(pid, hint, version)| {
                self.pool
                    .fix_optimistic_at(pid, hint)
                    .filter(|l| l.version() == version)
            });
            // One slot along from the entry, in the leaf it was read from.
            let at = cursor.at.filter(|_| cached.is_some());
            let leaf = match &cached {
                // The leaf of the last entry is as it was: no descent.
                Some(leaf) => leaf.clone(),
                None => {
                    let route = if seek == Seek::Gt {
                        Route::Above
                    } else {
                        Route::At
                    };
                    let found = match self.descend_to_leaf(&target, route, cursor.near) {
                        Ok(found) => found,
                        Err(e) => {
                            pending(cursor, target);
                            return Err(e);
                        }
                    };
                    match found {
                        Found::Node { node, .. } => node,
                        Found::Restart | Found::Shorter => {
                            pending(cursor, target);
                            return yield_now();
                        }
                        Found::Io(io) => {
                            pending(cursor, target);
                            return Ok(IOResult::IO(io));
                        }
                    }
                }
            };
            // Into the buffers of the entry before, or kept ones.
            let (mut key, mut value) = match cursor.entry.take() {
                Some(buffers) => buffers,
                None => (
                    std::mem::take(&mut cursor.spare.0),
                    std::mem::take(&mut cursor.spare.1),
                ),
            };
            let read = leaf.read(|p| {
                scan_leaf(
                    View::new(p),
                    seek,
                    &target,
                    cached.is_some(),
                    at,
                    &mut key,
                    &mut value,
                )
            });
            let Some(step) = read else {
                // Changed while read: a writer holds it for a moment.
                cursor.near = Near::NONE;
                cursor.spare.0 = key;
                cursor.spare.1 = value;
                cursor.leaf = None;
                cursor.at = None;
                pending(cursor, target);
                return yield_now();
            };
            let step = match step {
                Some(step) => step,
                None => {
                    pending(cursor, target);
                    return Err(corrupt_node(leaf.pid()));
                }
            };
            cursor.near = Near::of(&leaf);
            if !matches!(step, Scan::Entry(..)) {
                cursor.spare.0 = std::mem::take(&mut key);
                cursor.spare.1 = std::mem::take(&mut value);
            }
            match step {
                Scan::Entry(out_of_line, pos) => {
                    let at = (leaf.pid(), leaf.hint(), leaf.version());
                    cursor.leaf = Some(at);
                    // A value out of line is read through the leaf, and found again by its key if
                    // the read is lost: no step from it is taken by position.
                    cursor.at = (!out_of_line).then_some(pos);
                    if !out_of_line {
                        cursor.entry = Some((key, value));
                        cursor.spare.2 = target;
                        cursor.reading = None;
                        return Ok(IOResult::Done(true));
                    }
                    pending(cursor, target);
                    let (head, reference) = self
                        .split_reference(&value)
                        .ok_or_else(|| corrupt_node(leaf.pid()))?;
                    // The value of a read under way goes on from where it was.
                    if cursor
                        .reading
                        .as_ref()
                        .is_none_or(|(k, _, r)| *k != key || r.reference() != reference)
                    {
                        cursor.reading = Some((key, head.to_vec(), ValueRead::new(reference)));
                    }
                    cursor.held = Some(at);
                }
                Scan::End => {
                    cursor.reading = None;
                    cursor.entry = None;
                    cursor.spare.2 = target;
                    cursor.leaf = None;
                    cursor.at = None;
                    return Ok(IOResult::Done(false));
                }
                Scan::Stale => {
                    cursor.leaf = None;
                    cursor.at = None;
                    pending(cursor, target);
                }
                Scan::Beyond(pid, hint, fence) => {
                    // A leaf holds the keys up to its upper fence. A descent above the target
                    // goes right of a separator equal to it, so the leaf it finds ends past the
                    // target; a descent from the target, or the leaf of the last entry, which
                    // may end at that entry, ends at or past it, and the next step is above
                    // that end. Each step thus moves the scan on, so it ends. A fence that does
                    // not is a tree that does not hold together.
                    let advances = match seek {
                        Seek::Gt if cached.is_none() => fence > target,
                        _ => fence >= target,
                    };
                    if !advances {
                        let e = stuck_scan(leaf.pid(), seek, &target, &fence);
                        pending(cursor, target);
                        return Err(e);
                    }
                    cursor.leaf = None;
                    cursor.at = None;
                    cursor.spare.2 = target;
                    // The next leaf by its hint, if its frame still holds it; else a descent.
                    if let Some(n) = self.pool.fix_optimistic_at(pid, hint) {
                        let neighbour = n.read(|p| {
                            let v = View::new(p);
                            v.is_node()
                                && v.root() == self.root
                                && v.is_leaf()
                                && v.lower_is(&fence) == Some(true)
                        });
                        if neighbour == Some(true) {
                            cursor.leaf = Some((pid, hint, n.version()));
                        }
                    }
                    cursor.pending = Some((Seek::Gt, fence));
                }
                Scan::Before(fence) => {
                    // A leaf's lower fence lies below every key it covers.
                    if fence >= target {
                        let e = stuck_scan(leaf.pid(), seek, &target, &fence);
                        pending(cursor, target);
                        return Err(e);
                    }
                    cursor.leaf = None;
                    cursor.at = None;
                    cursor.spare.2 = target;
                    cursor.pending = Some((Seek::Le, fence));
                }
            }
        }
    }

    // ---------------------------------------------------------------- checking

    /// Checks the whole tree: every node's structure, the fences of each child against its
    /// parent's entries, levels, and the root's. Step it until done.
    pub fn check(&self, op: &mut Check) -> Result<IOResult<Stats>> {
        if op.stack.is_empty() && op.stats.nodes == 0 {
            op.stack.push(Expected {
                pid: self.root,
                lower: None,
                upper: None,
                level: None,
            });
        }
        while let Some(Expected {
            pid,
            lower,
            upper,
            level,
        }) = op.stack.last().cloned()
        {
            let page = done!(self.pool.fix_optimistic(pid, None));
            // A copy, checked once it is known whole: the page changed meanwhile otherwise.
            let Some(bytes) = page.read(|p| p.copied(0..p.len())) else {
                continue;
            };
            op.stack.pop();
            let v = View::new(&bytes);
            let fail = |what: String| {
                LimboError::Corrupt(format!("B-tree {:?}, {pid:?}: {what}", self.root))
            };
            node::check(&bytes).map_err(fail)?;
            if v.root() != self.root {
                return Err(fail("belongs to another tree".into()));
            }
            if level.is_some_and(|l| l != v.level()) {
                return Err(fail(format!(
                    "at level {} under a parent expecting {level:?}",
                    v.level()
                )));
            }
            if v.lower().unwrap() != lower || v.upper().unwrap() != upper {
                return Err(fail("fences differ from the parent's".into()));
            }
            op.stats.nodes += 1;
            op.stats.pages.push(pid);
            let count = v.count().unwrap();
            if v.is_leaf() {
                op.stats.entries += count;
                for i in 0..count {
                    let (value, out) = v.value(i).unwrap();
                    if out {
                        op.stats.values.push(
                            self.split_reference(&value)
                                .ok_or_else(|| fail(format!("entry {i}: a bad reference")))?
                                .1,
                        );
                    }
                }
                continue;
            }
            // Children in reverse, so the leftmost is checked first.
            let mut bound = upper.clone();
            for i in (0..=count).rev() {
                let child = v.child(i).unwrap().0;
                let lo = if i == 0 { lower.clone() } else { v.key(i - 1) };
                op.stack.push(Expected {
                    pid: child,
                    lower: lo.clone(),
                    upper: bound.clone(),
                    level: Some(v.level() - 1),
                });
                bound = lo;
            }
        }
        Ok(IOResult::Done(std::mem::take(&mut op.stats)))
    }
}

/// What the look at a leaf found: `None` if it no longer covers the key; else the key's value
/// and whether it is out of line, if present.
type Look = Option<Option<(Vec<u8>, bool)>>;

/// What [`look`] finds, with where the key is or goes in the leaf and whether it is there.
type LookAt = Option<(usize, bool, Option<(Vec<u8>, bool)>)>;

fn look_at<P: Bytes + ?Sized>(v: View<'_, P>, key: &[u8]) -> Option<LookAt> {
    if !v.covers(key)? {
        return Some(None);
    }
    let (pos, exact) = v.search(key)?;
    let value = match exact {
        true => Some(v.value(pos)?),
        false => None,
    };
    Some(Some((pos, exact, value)))
}

fn look<P: Bytes + ?Sized>(v: View<'_, P>, key: &[u8]) -> Option<Look> {
    if !v.covers(key)? {
        return Some(None);
    }
    let (pos, exact) = v.search(key)?;
    if !exact {
        return Some(Some(None));
    }
    Some(Some(Some(v.value(pos)?)))
}

fn store_len(pids: &[PageId]) -> u64 {
    if pids.is_empty() {
        0
    } else {
        PageStore::group_record_len(pids.len())
    }
}

fn smo_len(class: SizeClass, limits: Limits) -> u64 {
    2 * framed_len(8 + 4 + class.size())
        + 2 * framed_len(8 + 16 + limits.max_key)
        + store_len(&[PageId::from_raw(0)])
}

/// Builds an empty root, a leaf naming itself, on the newly allocated `page`; returns its image.
fn empty_root(page: &mut Exclusive<'_>) -> Vec<u8> {
    let pid = page.pid();
    node::build(
        &mut page.page_mut(),
        &Layout {
            root: pid,
            level: 0,
            lower: None,
            upper: None,
            next: (PageId::from_raw(0), 0),
        },
        [],
    );
    node::image(page.page())
}

fn check_budget(log: &Log, class: SizeClass, limits: Limits) -> Result<()> {
    let need = smo_len(class, limits);
    if need > log.buffer_bytes() {
        return Err(LimboError::InternalError(format!(
            "B-tree nodes of {} bytes need {need} bytes of log per split, more than the {} byte log buffer",
            class.size(),
            log.buffer_bytes()
        )));
    }
    Ok(())
}

/// Where to split the node `v` of `count` entries at `level`, whose upper fence is infinite if
/// `rightmost`, for an insert at `at` of `key`: the entries the left keeps, and the separator.
/// Appending at the right edge of the tree, the left keeps everything, so keys inserted in order
/// fill nodes; otherwise the entries split in half by bytes. A split is taken only where both
/// halves `fit`, fences included: a left keeping everything gains an upper fence, which a long
/// separator can make too much for it. Then the split nearest the middle that fits; `None` if
/// none does. Only the keys a candidate's separator comes from are copied out.
fn split_point<P: Bytes + ?Sized>(
    v: &View<'_, P>,
    count: usize,
    level: u8,
    at: usize,
    rightmost: bool,
    key: &[u8],
    fits: impl Fn(usize, &[u8]) -> bool,
) -> Option<(usize, Vec<u8>)> {
    let n = count;
    let try_at = |keep: usize, sep: Vec<u8>| fits(keep, &sep).then_some((keep, sep));
    if level == 0 {
        let appending = at == n && v.compare_key(n - 1, key)? == std::cmp::Ordering::Less;
        if appending && (rightmost || n == 1) {
            if let Some(split) = try_at(n, node::separator(&v.key(n - 1)?, key)) {
                return Some(split);
            }
        }
        if n == 1 {
            // The new key goes before the one entry, into a node of its own.
            return try_at(0, node::separator(key, &v.key(0)?));
        }
        // Between two entries, from the middle out.
        let half = half(v, n)?.clamp(1, n - 1);
        return outward(half, 1..n)
            .find_map(|keep| try_at(keep, node::separator(&v.key(keep - 1)?, &v.key(keep)?)));
    }
    // An inner node: entry `keep` moves up as the separator.
    if rightmost && at == n {
        if let Some(split) = try_at(n - 1, v.key(n - 1)?) {
            return Some(split);
        }
    }
    let half = half(v, n)?.min(n - 1);
    outward(half, 0..n).find_map(|keep| try_at(keep, v.key(keep)?))
}

/// The positions of `range`, from `start` out, nearest first.
fn outward(start: usize, range: std::ops::Range<usize>) -> impl Iterator<Item = usize> {
    (0..range.len()).flat_map(move |d| {
        let below = start.checked_sub(d).filter(|p| range.contains(p));
        let above = (d > 0).then_some(start + d).filter(|p| range.contains(p));
        below.into_iter().chain(above)
    })
}

/// The first entry past half the bytes of the `count` entries of `v`.
fn half<P: Bytes + ?Sized>(v: &View<'_, P>, count: usize) -> Option<usize> {
    let mut total = 0;
    for i in 0..count {
        total += v.entry_len(i)?;
    }
    let mut acc = 0;
    for i in 0..count {
        acc += v.entry_len(i)?;
        if 2 * acc >= total {
            return Some(i + 1);
        }
    }
    Some(count)
}

fn corrupt_node(pid: PageId) -> LimboError {
    LimboError::Corrupt(format!("B-tree node {pid:?} does not read back"))
}

/// A scan step that would not move on: the leaf's fence does not lie past where the scan stands.
fn stuck_scan(pid: PageId, seek: Seek, target: &[u8], fence: &[u8]) -> LimboError {
    LimboError::Corrupt(format!(
        "a scan seeking {seek:?} from {target:?} in leaf {pid:?} was sent to fence {fence:?}"
    ))
}

fn corrupt_value(r: ValueRef) -> LimboError {
    LimboError::Corrupt(format!(
        "the value at {:?} (id {:x}) does not read back",
        r.pid, r.id
    ))
}

/// What one split came to.
enum Split {
    Done,
    /// The tree changed: look again.
    Again,
    /// The parent has no room for the separator, which takes this many bytes.
    ParentFull(usize),
}

/// A point lookup.
pub struct Lookup {
    key: Vec<u8>,
    /// The head of the out-of-line value being read.
    head: Vec<u8>,
    read: Option<ValueRead>,
    /// The leaf the value's reference was read from, while it is known to hold it: page, frame
    /// hint and version.
    held: Option<(PageId, u32, u64)>,
    /// A reference whose value read back as changed: reading it again the same is corruption.
    failed: Option<ValueRef>,
}

impl Lookup {
    pub fn new(key: &[u8]) -> Self {
        Self {
            key: key.to_vec(),
            head: Vec::new(),
            read: None,
            held: None,
            failed: None,
        }
    }
}

/// A look at a key's leaf, held optimistically at the version read: where the key is or goes,
/// whether it is there, and its entry as the leaf holds it. A guard of the step that made it: it
/// is used, or dropped, before the step returns.
pub struct Looked<'a> {
    leaf: Optimistic<'a>,
    pos: usize,
    exact: bool,
    present: Option<(Vec<u8>, bool)>,
}

impl Looked<'_> {
    /// The leaf looked at, as a hint for the next look ([`Near`]).
    pub fn near(&self) -> Near {
        Near::of(&self.leaf)
    }

    /// The look kept past its step, holding no latch: given back by [`Tree::redeem`] while its leaf
    /// is unchanged.
    pub fn keep(self) -> Kept {
        Kept {
            pid: self.leaf.pid(),
            hint: self.leaf.hint(),
            version: self.leaf.version(),
            pos: self.pos,
            exact: self.exact,
            present: self.present,
        }
    }

    /// The key's entry as the leaf stores it, and whether its value is out of line; `None` for
    /// none.
    pub fn present(&self) -> Option<(&[u8], bool)> {
        self.present.as_ref().map(|(b, out)| (b.as_slice(), *out))
    }
}

/// A look kept past the step that made it ([`Looked::keep`]): the leaf's page and frame, the latch
/// version the look read, where the key is or goes, and its entry. A later step of the same caller
/// redeems it instead of reaching the leaf again ([`Tree::redeem`]).
#[derive(Debug)]
pub struct Kept {
    pid: PageId,
    hint: u32,
    version: u64,
    pos: usize,
    exact: bool,
    present: Option<(Vec<u8>, bool)>,
}

impl Kept {
    /// The key's entry as the look found it, and whether its value is out of line.
    pub fn present(&self) -> Option<(&[u8], bool)> {
        self.present.as_ref().map(|(b, out)| (b.as_slice(), *out))
    }

    /// The leaf looked at.
    pub fn near(&self) -> Near {
        Near(Some((self.pid, self.hint)))
    }
}

/// An entry as its leaf holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub head: Vec<u8>,
    pub value: StoredValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredValue {
    Inline(Vec<u8>),
    OutOfLine(ValueRef),
}

/// A put of one key's value.
pub struct Put {
    key: Vec<u8>,
    /// The entry as the leaf stores it inline: the tree's head, kept in the leaf whatever the
    /// value's length, then the value, until a value out of line takes it, leaving the head.
    entry: Vec<u8>,
    head_len: usize,
    value_len: usize,
    mode: Mode,
    write: Option<ValueWrite>,
    /// The out-of-line value being replaced: its reference and pages.
    old: Option<(Vec<u8>, Vec<PageId>)>,
    admitted: Option<(Grant, u64)>,
    lsn: Lsn,
    /// The entry the put expects to replace: its stored value and whether out of line, or `None`
    /// for an absent key.
    expected: Option<Option<(Vec<u8>, bool)>>,
    /// The transaction the change belongs to, and where its group starts once logged.
    txn: Option<TxnTail>,
    group: Option<GroupStart>,
    /// What the change puts back, as a compensation record.
    compensation: Option<Compensation>,
    /// The pages of the out-of-line value it replaces are retired, not abandoned: kept while a
    /// snapshot may read that version (DESIGN.md §6.5).
    retire_old: bool,
    /// What a transaction's update logs to undo it, when not the entry it replaces: the entry its
    /// statement found, or `None` for none (DESIGN.md §12, step 5).
    found: Option<Option<(Vec<u8>, bool)>>,
    /// The out-of-line value it puts back as it stands, by its reference, with its pages once
    /// learned: retired, and reinstated by the change.
    restoring: Option<(ValueRef, Option<Vec<PageId>>)>,
    /// The leaf tried first, then the leaf it reached.
    near: Near,
}

/// `stored` as the leaf keeps it: its bytes, and whether its value is out of line.
fn leaf_entry(stored: &Stored) -> (Vec<u8>, bool) {
    match &stored.value {
        StoredValue::Inline(v) => ([stored.head.as_slice(), v].concat(), false),
        StoredValue::OutOfLine(r) => ([stored.head.as_slice(), &r.encode()].concat(), true),
    }
}

impl Put {
    pub fn new(key: &[u8], value: Vec<u8>, mode: Mode) -> Self {
        Self::with_head(key, Vec::new(), value, mode)
    }

    /// A put into a tree whose values start with a head of `head`'s length.
    pub fn with_head(key: &[u8], head: Vec<u8>, value: Vec<u8>, mode: Mode) -> Self {
        let head_len = head.len();
        let entry = match head.is_empty() {
            true => value,
            false => [head, value].concat(),
        };
        Self::with_entry(key, entry, head_len, mode)
    }

    /// A put of `entry`, the head of a tree whose values start with one of `head_len` bytes,
    /// then the value: built once, in the bytes the leaf and the log copy.
    pub fn with_entry(key: &[u8], entry: Vec<u8>, head_len: usize, mode: Mode) -> Self {
        Self::with_owned(key.to_vec(), entry, head_len, mode)
    }

    /// [`with_entry`](Self::with_entry), the key in a buffer of the caller's, which
    /// [`into_buffers`](Self::into_buffers) gives back.
    pub fn with_owned(key: Vec<u8>, entry: Vec<u8>, head_len: usize, mode: Mode) -> Self {
        Self {
            key,
            value_len: entry.len() - head_len,
            head_len,
            entry,
            mode,
            write: None,
            old: None,
            admitted: None,
            lsn: 0,
            expected: None,
            txn: None,
            group: None,
            compensation: None,
            retire_old: false,
            found: None,
            restoring: None,
            near: Near::NONE,
        }
    }

    /// Tries `near` before a descent.
    pub fn near(mut self, near: Near) -> Self {
        self.near = near;
        self
    }

    /// The leaf the put reached: where the key it wrote is, unless the leaf has changed since.
    pub fn reached(&self) -> Near {
        self.near
    }

    /// Its key, and its entry while whole: `None` once its value went out of line, taking the
    /// entry's bytes past its head.
    pub fn into_buffers(self) -> (Vec<u8>, Option<Vec<u8>>) {
        // A value gone out of line took the entry's bytes past its head, whether or not its
        // write is still held.
        let whole = self.restoring.is_none() && self.entry.len() == self.head_len + self.value_len;
        (self.key, whole.then_some(self.entry))
    }

    /// Puts `entry` back as [`Tree::stored`] read it: a value out of line by its reference, its
    /// pages reinstated (DESIGN.md §6.5).
    pub fn restoring(key: &[u8], entry: &Stored, mode: Mode) -> Self {
        match &entry.value {
            StoredValue::Inline(v) => Self::with_head(key, entry.head.clone(), v.clone(), mode),
            StoredValue::OutOfLine(r) => {
                let mut put = Self::with_head(key, entry.head.clone(), Vec::new(), mode);
                put.restoring = Some((*r, None));
                put
            }
        }
    }

    /// Retires the pages of the out-of-line value it replaces instead of abandoning them.
    pub fn retiring(mut self) -> Self {
        self.retire_old = true;
        self
    }

    /// Logs `found`, the entry the transaction's statement found, as what undoes the change.
    pub fn undoing(mut self, found: Option<&Stored>) -> Self {
        self.found = Some(found.map(leaf_entry));
        self
    }

    /// Changes nothing, and reports [`Outcome::Changed`], unless the entry is `stored`, as
    /// [`Tree::stored`] read it, or absent for `None`.
    pub fn expecting(mut self, stored: Option<&Stored>) -> Self {
        self.expected = Some(stored.map(leaf_entry));
        self
    }

    /// [`expecting`](Self::expecting) the entry a [`Looked`] found, as the leaf stores it.
    pub fn expecting_entry(mut self, entry: Option<(&[u8], bool)>) -> Self {
        self.expected = Some(entry.map(|(b, out)| (b.to_vec(), out)));
        self
    }

    /// Logs the change as `tail`'s transaction's, after its previous group, with what undoes it.
    pub fn in_transaction(mut self, tail: TxnTail) -> Self {
        self.txn = Some(tail);
        self
    }

    /// Where the transaction's group holding the change starts, once logged in a transaction.
    pub fn group(&self) -> Option<GroupStart> {
        self.group
    }

    /// Logs the change as `tail`'s compensation `of` (DESIGN.md §11.5), a key's entry put back
    /// over the one there, which [`expecting`](Self::expecting) names.
    pub fn compensating(mut self, tail: TxnTail, of: Compensation) -> Self {
        self.txn = Some(tail);
        self.compensation = Some(of);
        self
    }

    /// Where the change's record ends, once made; 0 if nothing changed. The change is durable
    /// once the log is durable up to here.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Learns the pages of the out-of-line value `present` holds, if it holds one; false if they
    /// no longer belong to it, so the key must be looked at again.
    fn old_pages(
        &mut self,
        pool: &Arc<BufferPool>,
        head: usize,
        present: Option<&(Vec<u8>, bool)>,
        holder: &dyn Fn() -> bool,
    ) -> Result<IOResult<bool>> {
        old_pages(&mut self.old, pool, head, present, holder)
    }
}

fn old_pages(
    old: &mut Option<(Vec<u8>, Vec<PageId>)>,
    pool: &Arc<BufferPool>,
    head: usize,
    present: Option<&(Vec<u8>, bool)>,
    holder: &dyn Fn() -> bool,
) -> Result<IOResult<bool>> {
    let Some((stored, true)) = present else {
        *old = None;
        return Ok(IOResult::Done(true));
    };
    let reference = &stored[head.min(stored.len())..];
    if old.as_ref().is_some_and(|(r, _)| r == reference) {
        return Ok(IOResult::Done(true));
    }
    let parsed = ValueRef::decode(reference)
        .ok_or_else(|| LimboError::Corrupt("a bad value reference".into()))?;
    Ok(match value::pages(pool, parsed, holder)? {
        IOResult::Done(Ok(pages)) => {
            *old = Some((reference.to_vec(), pages));
            IOResult::Done(true)
        }
        IOResult::Done(Err(_)) => IOResult::Done(false),
        IOResult::IO(io) => IOResult::IO(io),
    })
}

/// A removal of one key.
pub struct Remove {
    key: Vec<u8>,
    old: Option<(Vec<u8>, Vec<PageId>)>,
    admitted: Option<(Grant, u64)>,
    /// The level of a merge still to try after the removal.
    merging: Option<u8>,
    lsn: Lsn,
    /// The entry the removal expects: its stored value and whether out of line.
    expected: Option<(Vec<u8>, bool)>,
    /// The transaction the removal belongs to, and where its group starts once logged.
    txn: Option<TxnTail>,
    group: Option<GroupStart>,
    compensation: Option<Compensation>,
    /// Pages abandoned in the removal's group, and freed after it.
    also: Vec<PageId>,
}

impl Remove {
    pub fn new(key: &[u8]) -> Self {
        Self {
            key: key.to_vec(),
            old: None,
            admitted: None,
            merging: None,
            lsn: 0,
            expected: None,
            txn: None,
            group: None,
            compensation: None,
            also: Vec::new(),
        }
    }

    /// Abandons `pids` in the removal's group and frees them after it: pages what the entry named
    /// leaves behind, gone with it.
    pub fn abandoning(mut self, pids: Vec<PageId>) -> Self {
        self.also = pids;
        self
    }

    /// Logs the removal as `tail`'s compensation `of` (DESIGN.md §11.5), of a key that had no
    /// entry. Removes nothing, and reports false, unless the key's entry is `entry`, as
    /// [`Tree::stored`] read it.
    pub fn compensating(mut self, tail: TxnTail, of: Compensation, entry: &Stored) -> Self {
        self.expected = Some(match &entry.value {
            StoredValue::Inline(v) => ([entry.head.as_slice(), v].concat(), false),
            StoredValue::OutOfLine(r) => ([entry.head.as_slice(), &r.encode()].concat(), true),
        });
        self.txn = Some(tail);
        self.compensation = Some(of);
        self
    }

    /// Where the transaction's group holding the removal starts, once logged in a transaction.
    pub fn group(&self) -> Option<GroupStart> {
        self.group
    }

    /// Where the removal's record ends, once made; 0 if the key was absent.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    fn old_pages(
        &mut self,
        pool: &Arc<BufferPool>,
        head: usize,
        present: &(Vec<u8>, bool),
        holder: &dyn Fn() -> bool,
    ) -> Result<IOResult<bool>> {
        old_pages(&mut self.old, pool, head, Some(present), holder)
    }
}

/// Where a seek lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seek {
    /// The first key at or above.
    Ge,
    /// The first key above.
    Gt,
    /// The last key at or below.
    Le,
    /// The last key below.
    Lt,
}

/// A position in a tree: an entry, copied out. It holds no latch; moving it finds the leaf again
/// by its frame hint, without a descent while the leaf is unchanged.
#[derive(Default)]
pub struct Cursor {
    entry: Option<(Vec<u8>, Vec<u8>)>,
    pending: Option<(Seek, Vec<u8>)>,
    /// The leaf of the entry: page, frame hint, and the version it was read at.
    leaf: Option<(PageId, u32, u64)>,
    /// Where the entry is in `leaf`, while the leaf is the one it was read from: a step to the
    /// next or previous entry of a leaf unchanged since is one slot along, with no search.
    at: Option<usize>,
    /// Buffers kept from earlier entries and targets, so a scan copies each row into memory it
    /// already has: an entry's key and value, and a target.
    spare: (Vec<u8>, Vec<u8>, Vec<u8>),
    /// The key, the head and the read of an out-of-line value.
    reading: Option<(Vec<u8>, Vec<u8>, ValueRead)>,
    /// The leaf the value's reference was read from, while it is known to hold it: page, frame
    /// hint and version.
    held: Option<(PageId, u32, u64)>,
    failed: Option<ValueRef>,
    /// The leaf it last read or was placed at, tried before a descent once `leaf` no longer holds.
    near: Near,
}

impl Cursor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stands at `key`, with no leaf to step from and its value unread: the next move seeks from
    /// the key, as from an entry a seek found there, trying `near` first.
    pub fn place(&mut self, key: &[u8], near: Near) {
        let (mut k, mut v) = match self.entry.take() {
            Some(buffers) => buffers,
            None => (
                std::mem::take(&mut self.spare.0),
                std::mem::take(&mut self.spare.1),
            ),
        };
        k.clear();
        k.extend_from_slice(key);
        v.clear();
        self.entry = Some((k, v));
        self.pending = None;
        self.leaf = None;
        self.at = None;
        self.reading = None;
        self.held = None;
        self.near = near;
    }

    /// The leaf it last read or was placed at.
    pub fn near(&self) -> Near {
        self.near
    }

    /// Tries `near` before its next descent.
    pub fn set_near(&mut self, near: Near) {
        self.near = near;
    }

    /// Sets the move past the entry, `Gt` or `Lt`, pending: false with no entry. The target is
    /// the entry's key, copied into a kept buffer.
    fn step(&mut self, seek: Seek) -> bool {
        let Some((key, _)) = &self.entry else {
            return false;
        };
        let mut target = std::mem::take(&mut self.spare.2);
        target.clear();
        target.extend_from_slice(key);
        self.pending = Some((seek, target));
        true
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.entry.as_ref().map(|(k, _)| k.as_slice())
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.entry.as_ref().map(|(_, v)| v.as_slice())
    }
}

/// What a scan of one leaf found.
enum Scan {
    /// The entry, its key and its value or reference copied out: whether the value is out of
    /// line, and where the entry is in the leaf.
    Entry(bool, usize),
    /// Nothing more in this direction.
    End,
    /// Past the leaf's upper end: the next leaf's hint and the fence to seek above.
    Beyond(PageId, u32, Vec<u8>),
    /// A leaf found by its hint no longer answers this seek.
    Stale,
    /// Before the leaf's lower end: the fence to seek at or below.
    Before(Vec<u8>),
}

/// The entry `seek` from `target` lands on in the leaf `v`, its key and value copied into `key`
/// and `value`. A leaf found by its hint (`cached`) must hold where the seek lands: cover the
/// target, or, seeking above it, start right there. A step from the entry at `at` of the same
/// leaf, unchanged since, is the slot beside it.
fn scan_leaf<P: Bytes + ?Sized>(
    v: View<'_, P>,
    seek: Seek,
    target: &[u8],
    cached: bool,
    at: Option<usize>,
    key: &mut Vec<u8>,
    value: &mut Vec<u8>,
) -> Option<Scan> {
    if !v.is_leaf() {
        return None;
    }
    let count = v.count()?;
    let pos = match (at, seek) {
        (Some(p), Seek::Gt) => Some(p + 1),
        (Some(p), Seek::Lt) => p.checked_sub(1),
        _ => {
            if cached && !(v.covers(target)? || seek == Seek::Gt && v.lower_is(target)?) {
                return Some(Scan::Stale);
            }
            let (pos, exact) = v.search(target)?;
            match seek {
                Seek::Ge => Some(pos),
                Seek::Gt => Some(pos + exact as usize),
                Seek::Le => (pos + exact as usize).checked_sub(1),
                Seek::Lt => pos.checked_sub(1),
            }
        }
    };
    match pos {
        Some(p) if p < count => {
            v.key_into(p, key)?;
            let out = v.value_into(p, value)?;
            Some(Scan::Entry(out, p))
        }
        Some(_) => Some(match v.upper()? {
            None => Scan::End,
            Some(u) => {
                let (pid, hint) = v.next();
                Scan::Beyond(pid, hint, u)
            }
        }),
        None => Some(match v.lower()? {
            None => Scan::End,
            Some(l) => Scan::Before(l),
        }),
    }
}

/// A check of a whole tree in progress.
#[derive(Default)]
pub struct Check {
    stack: Vec<Expected>,
    stats: Stats,
}

/// A node a check is still to look at, and what its parent says it must be.
#[derive(Clone)]
struct Expected {
    pid: PageId,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    level: Option<u8>,
}

/// A pair of neighbouring children by the left's position, with each one's page and frame hint.
type Pair = (usize, (PageId, u32), (PageId, u32));

/// What a check counted.
#[derive(Debug, Default)]
pub struct Stats {
    pub nodes: usize,
    pub entries: usize,
    /// Every node's page.
    pub pages: Vec<PageId>,
    /// Every out-of-line value's reference.
    pub values: Vec<ValueRef>,
}
