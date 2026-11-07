//! Replaying published changes (audit 17): every committed transaction's changes, in the order
//! their Commit records lie in the log, and the changes published alone, applied through the
//! trees by key.
//!
//! [`Gather`] reads the log's records and hands over each body of changes once it is whole: a
//! Commit's, joined to the parts it spilled ahead of it, or a run's. A part, or the Commit, begins
//! where it says in the transaction's changes, and cuts what was joined before it there: a
//! statement rolled back after its changes were spilled leaves them behind it, and the changes
//! after it begin where they began (DESIGN.md §11.4.1). A part is kept as where it lies, never its
//! bytes: a body is read back from the log part by part as its changes are applied ([`Body`]), so
//! recovery holds a part and the change running over its end at a time, as a writer holds a chunk
//! and the change it is writing. A transaction whose Commit never came
//! leaves its parts behind, never applied. The log is read from a checkpoint's redo start, past
//! which a commit at or below the checkpoint's snapshot may have spilled its first parts (the cut,
//! [`Transactions::cut`](super::txn::Transactions::cut)): its changes are in the pages, so it is
//! skipped whatever is found of it, and nothing of a transaction whose first part is missing is
//! kept. A commit above the snapshot has every part past the redo start, and a missing one is
//! corruption.
//!
//! [`Apply`] applies a body: a put sets the key's value as a settled version
//! ([`Version::SETTLED`]), or plainly in a plain tree; a delete removes the key; a creation makes
//! an empty tree at the root it names, which must be free; a drain frees whole a tree this replay
//! made, or one the mapping it opened with holds, and removes its list entry, and of any other root
//! only removes that entry, since a creation undone never reaches the log and its root may be a
//! node of another tree here. Puts and deletes by key make replaying a body again change nothing.

use super::btree::{Drain, Drained, Mode, Put, Remove, Tree};
use super::buffer::BufferPool;
use super::drive;
use super::log::Kind;
use super::log::{Lsn, Rereader};
use super::page::PageId;
use super::store::PageStore;
use super::txn::changes::{change_len, Change, Decode};
use super::txn::versioned::{Version, HEADER};
use super::txn::{commit_changes, COMMIT, RUN, SPILL};
use crate::io::IO;
use crate::sync::Arc;
use crate::{LimboError, Result};
use std::collections::{HashMap, HashSet};

/// The changes of committed transactions and runs, gathered whole from the log's records read
/// from a checkpoint's redo start.
pub struct Gather {
    /// The checkpoint's snapshot: a commit at or below it is in the pages.
    snapshot: u64,
    /// Per transaction, by its start, the parts it spilled so far.
    spilled: HashMap<u64, Spilled>,
}

/// What is kept of a transaction's spilled parts.
enum Spilled {
    /// Where its changes lie, from their start.
    Parts(Vec<PartAt>),
    /// Nothing: its first part is before the redo start, which only a commit at or below the
    /// snapshot leaves.
    Headless,
}

/// A spilled part as kept: the record at `at`, whose `whole` bytes of changes begin at `from` in
/// its transaction's, of which the first `len` count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PartAt {
    from: u64,
    len: u64,
    whole: u64,
    at: Lsn,
}

/// The bytes a spilled part's record holds before its changes: its transaction's start and the
/// changes' position.
const PART_FRONT: usize = 16;

/// A body of changes: a committed transaction's parts, read back from the log as its changes are
/// handed over, then the changes its Commit carried; or a run's.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Body {
    parts: Vec<PartAt>,
    tail: Vec<u8>,
}

/// Where a body's spilled parts are read back from: the log ([`Rereader`]).
pub trait Parts {
    /// The payload of the record outside any group at `at`, `len` bytes long.
    fn payload(&mut self, at: Lsn, len: usize) -> Result<&[u8]>;
}

impl Parts for Rereader {
    fn payload(&mut self, at: Lsn, len: usize) -> Result<&[u8]> {
        Rereader::payload(self, at, len)
    }
}

impl Body {
    /// Hands `f` each change, oldest first, each part read back from `parts`: at most one part's
    /// bytes and one change held at once.
    pub fn changes(
        &self,
        parts: &mut dyn Parts,
        f: &mut dyn FnMut(Change<'_>) -> Result<()>,
    ) -> Result<()> {
        let mut carry = Vec::new();
        for p in &self.parts {
            let payload = parts.payload(p.at, PART_FRONT + p.whole as usize)?;
            let bytes = payload
                .get(PART_FRONT..)
                .and_then(|b| b.get(..p.len as usize))
                .ok_or_else(|| {
                    LimboError::Corrupt(format!(
                        "a published part at {} reads back short of its {} bytes",
                        p.at, p.len
                    ))
                })?;
            feed(&mut carry, bytes, f)?;
        }
        feed(&mut carry, &self.tail, f)?;
        if !carry.is_empty() {
            return Err(LimboError::Corrupt(
                "a published body ends inside a change".into(),
            ));
        }
        Ok(())
    }

    /// A body whose changes are `bytes`, held whole: a run's.
    pub fn whole(bytes: Vec<u8>) -> Self {
        Self {
            parts: Vec::new(),
            tail: bytes,
        }
    }

    /// Bytes of its changes.
    #[cfg(test)]
    pub fn len(&self) -> u64 {
        self.parts.iter().map(|p| p.len).sum::<u64>() + self.tail.len() as u64
    }
}

/// Hands `f` every change that `carry` and then `bytes` hold whole, and keeps in `carry` the start
/// of one `bytes` ends inside.
fn feed(
    carry: &mut Vec<u8>,
    mut bytes: &[u8],
    f: &mut dyn FnMut(Change<'_>) -> Result<()>,
) -> Result<()> {
    if !carry.is_empty() {
        // The change carried over is completed from the front of `bytes`, its head first.
        let need = match change_len(carry) {
            Some(n) => n,
            None => {
                let take = (super::txn::changes::HEAD - carry.len()).min(bytes.len());
                carry.extend_from_slice(&bytes[..take]);
                bytes = &bytes[take..];
                match change_len(carry) {
                    Some(n) => n,
                    None => return Ok(()),
                }
            }
        };
        let take = (need - carry.len()).min(bytes.len());
        carry.extend_from_slice(&bytes[..take]);
        bytes = &bytes[take..];
        if carry.len() < need {
            return Ok(());
        }
        for change in Decode::new(carry) {
            f(change?)?;
        }
        carry.clear();
    }
    let mut whole = 0;
    while let Some(n) = change_len(&bytes[whole..]) {
        if whole + n > bytes.len() {
            break;
        }
        whole += n;
    }
    for change in Decode::new(&bytes[..whole]) {
        f(change?)?;
    }
    carry.extend_from_slice(&bytes[whole..]);
    Ok(())
}

impl Gather {
    /// Gathers what was published after the checkpoint at `snapshot`.
    pub fn new(snapshot: u64) -> Self {
        Self {
            snapshot,
            spilled: HashMap::new(),
        }
    }

    /// The body of changes the record of `kind` from transaction `txn`, at `at`, completes, with
    /// its commit timestamp: a Commit's, joined to its spilled parts, or a run's, which has none;
    /// for a commit at or below the snapshot, whose changes are in the pages, empty. `None` for a
    /// part kept until its Commit, and for a record of any other kind.
    pub fn record(
        &mut self,
        kind: Kind,
        txn: u64,
        at: Lsn,
        payload: &[u8],
    ) -> Result<Option<(Option<u64>, Body)>> {
        let corrupt = |what: &str| LimboError::Corrupt(format!("a published {what}"));
        if kind == SPILL {
            let word = |at: usize| {
                payload
                    .get(at..at + 8)
                    .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            };
            let (start, from) = (
                word(0).ok_or_else(|| corrupt("part with no transaction"))?,
                word(8).ok_or_else(|| corrupt("part with no offset"))?,
            );
            let whole = (payload.len() - PART_FRONT) as u64;
            let part = PartAt {
                from,
                len: whole,
                whole,
                at,
            };
            match self.spilled.get_mut(&start) {
                // Its first part read: one beginning past the start had those before it spilled
                // ahead of the redo start.
                None if from == 0 => {
                    self.spilled.insert(start, Spilled::Parts(vec![part]));
                }
                None => {
                    self.spilled.insert(start, Spilled::Headless);
                }
                Some(Spilled::Headless) => {}
                Some(Spilled::Parts(parts)) => join(parts, part).map_err(|end| {
                    corrupt(&format!(
                        "part at {from} of transaction {start}, whose parts so far end at {end}"
                    ))
                })?,
            }
            return Ok(None);
        }
        if kind == COMMIT {
            let (from, part) =
                commit_changes(payload).ok_or_else(|| corrupt("commit's changes"))?;
            let ts = super::txn::commit_ts(payload).ok_or_else(|| corrupt("commit's timestamp"))?;
            let spilled = self.spilled.remove(&txn);
            if ts <= self.snapshot {
                return Ok(Some((Some(ts), Body::default())));
            }
            let mut parts = match spilled {
                Some(Spilled::Parts(parts)) => parts,
                None if from == 0 => Vec::new(),
                Some(Spilled::Headless) | None => {
                    return Err(corrupt(&format!(
                        "commit of transaction {txn} at {ts}, past the checkpoint's {}, whose \
                         first part is before the redo start",
                        self.snapshot
                    )))
                }
            };
            let tail = PartAt {
                from,
                len: 0,
                whole: 0,
                at,
            };
            join(&mut parts, tail).map_err(|end| {
                corrupt(&format!(
                    "commit of transaction {txn} whose part begins at {from}, after parts ending \
                     at {end}"
                ))
            })?;
            let tail = part.to_vec();
            return Ok(Some((Some(ts), Body { parts, tail })));
        }
        if kind == RUN {
            return Ok(Some((None, Body::whole(payload.to_vec()))));
        }
        Ok(None)
    }

    /// The transactions whose parts are kept with no Commit read yet.
    #[cfg(test)]
    pub fn unfinished(&self) -> usize {
        self.spilled.len()
    }
}

/// Joins `part` to the parts joined so far: at their end, or cutting them where it begins, past
/// which a statement rolled back. `Err` with where they end if it begins past it, a gap. They lie
/// end to end from 0, in order, so a cut finds its place by halving: a part is joined in constant
/// time, and a cut in the logarithm of the parts plus those it drops.
fn join(parts: &mut Vec<PartAt>, part: PartAt) -> std::result::Result<(), u64> {
    let end = parts.last().map_or(0, |p| p.from + p.len);
    if part.from > end {
        return Err(end);
    }
    if part.from < end {
        parts.truncate(parts.partition_point(|p| p.from < part.from));
        if let Some(last) = parts.last_mut() {
            last.len = last.len.min(part.from - last.from);
        }
    }
    if part.len > 0 {
        parts.push(part);
    }
    Ok(())
}

/// Applies bodies of changes to the trees.
pub struct Apply {
    io: Arc<dyn IO>,
    pool: Arc<BufferPool>,
    store: Arc<PageStore>,
    /// The trees this replay made and has not drained.
    made: HashSet<PageId>,
    /// The roots of drains the replay meets that the mapping it opened with holds: freed whole
    /// once (audit 18, review 5).
    opened: HashSet<PageId>,
}

impl Apply {
    pub fn new(io: Arc<dyn IO>, pool: Arc<BufferPool>, store: Arc<PageStore>) -> Self {
        Self {
            io,
            pool,
            store,
            made: HashSet::new(),
            opened: HashSet::new(),
        }
    }

    /// Drains of `roots`, which the mapping it opened with holds, free the trees whole.
    pub fn opened_with(mut self, roots: impl IntoIterator<Item = PageId>) -> Self {
        self.opened.extend(roots);
        self
    }

    fn tree(&self, root: PageId, plain: bool) -> Tree {
        let tree = Tree::open(self.io.clone(), self.pool.clone(), self.store.clone(), root);
        match plain {
            true => tree,
            false => tree.with_head(HEADER).with_reserves(),
        }
    }

    /// Applies `body`'s changes in order, its parts read back from `parts` ([`Body::changes`]).
    pub fn apply(&mut self, body: &Body, parts: &mut dyn Parts) -> Result<()> {
        body.changes(parts, &mut |change| self.change(change))
    }

    /// Applies one change.
    fn change(&mut self, change: Change<'_>) -> Result<()> {
        match change {
            Change::Create { root, .. } => {
                drive(&self.io, || Tree::create_at(&self.pool, root))?;
                self.made.insert(root);
            }
            Change::Put {
                tree,
                plain,
                key,
                value,
                ..
            } => {
                let mut put = match plain {
                    true => Put::new(key, value.to_vec(), Mode::Upsert),
                    false => Put::with_head(
                        key,
                        Version::SETTLED.encode().to_vec(),
                        value.to_vec(),
                        Mode::Upsert,
                    ),
                };
                let t = self.tree(tree, plain);
                drive(&self.io, || t.put(&mut put))?;
            }
            Change::Delete {
                tree, plain, key, ..
            } => {
                let mut remove = Remove::new(key);
                let t = self.tree(tree, plain);
                drive(&self.io, || t.remove(&mut remove))?;
            }
            Change::Drain { root, list } => {
                let key = root.raw().to_be_bytes();
                let list = self.tree(list, true);
                if self.made.remove(&root) || self.opened.remove(&root) {
                    let t = self.tree(root, true);
                    let mut op = Drain::new(&key);
                    while drive(&self.io, || t.drain(&list, &mut op))? == Drained::More {}
                } else {
                    let mut remove = Remove::new(&key);
                    drive(&self.io, || list.remove(&mut remove))?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
