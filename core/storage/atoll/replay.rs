//! Replaying published changes (audit 17): every committed transaction's changes, in the order
//! their Commit records lie in the log, and the changes published alone, applied through the
//! trees by key.
//!
//! [`Gather`] reads the log's records and hands over each body of changes once it is whole: a
//! Commit's, joined to the parts it spilled ahead of it, or a run's. A transaction whose Commit
//! never came leaves its parts behind, never applied.
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
use super::page::PageId;
use super::store::PageStore;
use super::txn::changes::{Change, Decode};
use super::txn::versioned::{Version, HEADER};
use super::txn::{commit_changes, COMMIT, RUN, SPILL};
use crate::io::IO;
use crate::sync::Arc;
use crate::{LimboError, Result};
use std::collections::{HashMap, HashSet};

/// The changes of committed transactions and runs, gathered whole from the log's records.
#[derive(Default)]
pub struct Gather {
    /// Per transaction, by its start, the parts it spilled so far.
    spilled: HashMap<u64, Vec<u8>>,
}

impl Gather {
    pub fn new() -> Self {
        Self::default()
    }

    /// The body of changes the record of `kind` from transaction `txn` completes, with its commit
    /// timestamp: a Commit's, joined to its spilled parts, or a run's, which has none. `None` for a
    /// part kept until its Commit, and for a record of any other kind.
    pub fn record(
        &mut self,
        kind: Kind,
        txn: u64,
        payload: &[u8],
    ) -> Result<Option<(Option<u64>, Vec<u8>)>> {
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
            let parts = self.spilled.entry(start).or_default();
            if parts.len() as u64 != from {
                return Err(corrupt(&format!(
                    "part at {from} of transaction {start}, whose parts so far end at {}",
                    parts.len()
                )));
            }
            parts.extend_from_slice(&payload[16..]);
            return Ok(None);
        }
        if kind == COMMIT {
            let (total, from, part) =
                commit_changes(payload).ok_or_else(|| corrupt("commit's changes"))?;
            let mut body = self.spilled.remove(&txn).unwrap_or_default();
            if body.len() as u64 != from {
                return Err(corrupt(&format!(
                    "commit of transaction {txn} whose part begins at {from}, after parts ending \
                     at {}",
                    body.len()
                )));
            }
            body.extend_from_slice(part);
            debug_assert_eq!(body.len() as u64, total);
            let ts = super::txn::commit_ts(payload).ok_or_else(|| corrupt("commit's timestamp"))?;
            return Ok(Some((Some(ts), body)));
        }
        if kind == RUN {
            return Ok(Some((None, payload.to_vec())));
        }
        Ok(None)
    }

    /// The transactions whose parts are kept with no Commit read yet.
    pub fn unfinished(&self) -> usize {
        self.spilled.len()
    }
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

    /// Applies `body`'s changes in order.
    pub fn apply(&mut self, body: &[u8]) -> Result<()> {
        for change in Decode::new(body) {
            match change? {
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
        }
        Ok(())
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
