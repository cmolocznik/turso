//! Freeing a whole tree nothing reads or writes any more (DESIGN.md §17): one on a list of trees
//! whose creation was undone, or that were dropped.
//!
//! The drain walks by position, never by key: from the root to an inner node's first child, or,
//! once it has no key left, to the child after its keys, every node naming the root as its tree.
//! Removing children from the left moves no fence, so a tree half drained is never checked by its
//! fences. The drain's whole state is the tree: one a crash cut short goes on from the tree the
//! crash left. Each step is one log group, of at least two records, at most 255 and at most one
//! log chunk of bytes:
//!
//! - a leaf whose values' pages fit one group: the branch record removing it from its parent and
//!   the abandonment of the leaf with every page of its values;
//! - a leaf whose values' pages do not: removals of its entries with values out of line, from its
//!   first, with the abandonment of their pages;
//! - an inner node with no child left: the branch record removing it, and its abandonment;
//! - the root, once it is a leaf or has no child: the removal of the tree's entry on its list with
//!   the abandonment of the root and its values' pages, so a root page taken again by another tree
//!   never reads as one listed.
//!
//! A page is abandoned in the group that removes what names it, never freed after a check: one
//! abandoned while a write of it is in flight is freed later by the checkpointer, and until then
//! it is still allocated and still carries its value's id. Recovery frees a page a crash left
//! abandoned.
//!
//! One value's pages always fit one group with its entry's removal: a row is at most
//! `Value::MAX_BLOB_LENGTH`, 10^9 bytes, so 120 chunks and a directory, an abandonment of about
//! 1 KB, beside a key of at most about 941 bytes, and a log chunk is at least the 4 KiB block. An
//! entry that does not fit is refused as corruption.

use super::*;
use node::NO_CHILD;
use value::REF;

/// What a step of a drain came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drained {
    /// A group freed part of the tree: step again.
    More,
    /// The tree is freed whole and its entry on the list removed.
    Gone,
}

/// Freeing a tree whole: step it with [`Tree::drain`] until the tree is gone.
pub struct Drain {
    /// The tree's key on its list.
    key: Vec<u8>,
    /// The last group's removal of that key, once begun.
    last: Option<Remove>,
}

impl Drain {
    /// Draining the tree whose entry on its list is keyed `key`.
    pub fn new(key: &[u8]) -> Self {
        Self {
            key: key.to_vec(),
            last: None,
        }
    }
}

/// A node the walk reached.
enum Seen {
    Child(PageId),
    /// A leaf's entries: key, stored value, and whether the value is out of line.
    Leaf(Vec<(Vec<u8>, Vec<u8>, bool)>),
    /// An inner node with no child left.
    Childless,
}

/// The most records a group holds: a record's place in it is a byte.
const RECORDS: usize = u8::MAX as usize;

/// How many of `entries`' removals, each its key's length and its value's page count, one group
/// takes, in order: as many as fit in a log chunk of `chunk` bytes beside the abandonment of their
/// pages, and fewer than [`RECORDS`], so that the abandonment has its place among the records.
fn removals_fitting(entries: impl IntoIterator<Item = (usize, usize)>, chunk: u64) -> usize {
    let (mut len, mut take, mut pages) = (0, 0, 0);
    for (key_len, p) in entries {
        let more = framed_len(8 + key_len);
        if take + 1 == RECORDS || len + more + PageStore::group_record_len(pages + p) > chunk {
            break;
        }
        (len, take, pages) = (len + more, take + 1, pages + p);
    }
    take
}

impl Tree {
    /// One group of freeing this tree, which nothing else reads or writes, whose entry on `list`
    /// is `op`'s key. Step it until it reports the tree gone.
    pub fn drain(&self, list: &Tree, op: &mut Drain) -> Result<IOResult<Drained>> {
        if let Some(last) = &mut op.last {
            if !done!(list.remove(last)) {
                return Err(LimboError::Corrupt(format!(
                    "the tree {:?} drained is not on its list",
                    self.root
                )));
            }
            return Ok(IOResult::Done(Drained::Gone));
        }
        let (pid, parent, seen) = done!(self.walk());
        let chunk = self.store.log().chunk_bytes();
        // The record removing the node from where it is named, and that naming it.
        let unnamed = match parent {
            Some(_) => framed_len(8 + 8),
            None => framed_len(8 + op.key.len()),
        };
        let entries = match seen {
            Seen::Child(_) => unreachable!("the walk stops at a leaf or a node with no child"),
            Seen::Childless => Vec::new(),
            Seen::Leaf(entries) => entries,
        };
        // Every page of each value out of line: the page or directory, then the chunks.
        let mut valued = Vec::new();
        for (key, stored, out_of_line) in entries {
            if !out_of_line {
                continue;
            }
            let reference = stored
                .len()
                .checked_sub(REF)
                .and_then(|at| ValueRef::decode(&stored[at..]))
                .ok_or_else(|| corrupt_node(pid))?;
            let pages = match done!(value::pages(&self.pool, reference, &|| true)) {
                Ok(pages) => pages,
                Err(_) => {
                    return Err(LimboError::Corrupt(format!(
                        "a value of the tree {:?} drained lost its pages",
                        self.root
                    )))
                }
            };
            let alone = framed_len(8 + key.len()) + PageStore::group_record_len(pages.len());
            if alone > chunk {
                return Err(LimboError::Corrupt(format!(
                    "an entry of the tree {:?} needs a group of {alone} bytes, past the {chunk} \
                     byte log chunk",
                    self.root
                )));
            }
            valued.push((key, pages));
        }
        let total: usize = valued.iter().map(|(_, p)| p.len()).sum();
        if unnamed + PageStore::group_record_len(1 + total) <= chunk {
            let values = valued.into_iter().flat_map(|(_, p)| p).collect();
            return match parent {
                Some(parent) => self.unbranch(parent, pid, values),
                None => {
                    let pids = std::iter::once(pid).chain(values).collect();
                    op.last = Some(Remove::new(&op.key).abandoning(pids));
                    self.drain(list, op)
                }
            };
        }
        // Removals of the first entries with values, as many as fit.
        let take = removals_fitting(valued.iter().map(|(key, p)| (key.len(), p.len())), chunk);
        valued.truncate(take);
        self.remove_values(pid, valued)
    }

    /// Walks from the root, by position, to the first leaf or inner node with no child: that node,
    /// its parent and what it holds.
    fn walk(&self) -> Result<IOResult<(PageId, Option<PageId>, Seen)>> {
        let (mut pid, mut parent) = (self.root, None);
        loop {
            let node = done!(self.pool.fix_optimistic(pid, None));
            let Some(seen) = node.read(|p| {
                let v = View::new(p);
                if !v.is_node() || v.root() != self.root {
                    return None;
                }
                let count = v.count()?;
                Some(if v.is_leaf() {
                    let entry = |i| {
                        let (value, out_of_line) = v.value(i)?;
                        Some((v.key(i)?, value, out_of_line))
                    };
                    Seen::Leaf((0..count).map(entry).collect::<Option<_>>()?)
                } else if count > 0 {
                    Seen::Child(v.child(0)?.0)
                } else if v.next().0 != NO_CHILD {
                    Seen::Child(v.next().0)
                } else {
                    Seen::Childless
                })
            }) else {
                // Changed under the read, by a checkpoint's hint: read it again.
                continue;
            };
            match seen {
                None => return Err(corrupt_node(pid)),
                Some(Seen::Child(child)) => (parent, pid) = (Some(pid), child),
                Some(seen) => return Ok(IOResult::Done((pid, parent, seen))),
            }
        }
    }

    /// Removes `node`, with every page of its values, `values`, from `parent` in one group.
    fn unbranch(
        &self,
        parent: PageId,
        node: PageId,
        values: Vec<PageId>,
    ) -> Result<IOResult<Drained>> {
        let mut abandoned = vec![node];
        abandoned.extend(&values);
        let mut grant = done!(self.admit(framed_len(8 + 8) + store_len(&abandoned)));
        let mut p = done!(self.pool.fix_exclusive(parent, None));
        let n = done!(self.pool.fix_exclusive(node, None));
        let w = View::new(n.page());
        if !w.is_node() || w.root() != self.root {
            return Err(corrupt_node(node));
        }
        let v = View::new(p.page());
        let names = v.is_node()
            && !v.is_leaf()
            && v.root() == self.root
            && match v.count() {
                Some(0) => v.next().0 == node,
                Some(_) => v.child(0).map(|c| c.0) == Some(node),
                None => false,
            };
        if !names {
            return Err(corrupt_node(parent));
        }
        let record = Record {
            pid: parent,
            change: Change::Unbranch { child: node },
        };
        record.apply(&mut p.page_mut())?;
        let lsns = self.log_batch(
            &mut grant,
            &[record],
            &[StoreRecord::Abandon { pids: abandoned }],
        );
        p.changed(lsns[0]);
        drop(p);
        n.abandon();
        if let IOResult::IO(_) = self.pool.free(n)? {
            self.store.orphaned(node);
        }
        self.free_abandoned(values)?;
        Ok(IOResult::Done(Drained::More))
    }

    /// Removes the entries `valued` from the leaf `leaf`, each with every page of its value, in one
    /// group.
    fn remove_values(
        &self,
        leaf: PageId,
        valued: Vec<(Vec<u8>, Vec<PageId>)>,
    ) -> Result<IOResult<Drained>> {
        let pages: Vec<PageId> = valued.iter().flat_map(|(_, p)| p.iter().copied()).collect();
        let len = valued
            .iter()
            .map(|(key, _)| framed_len(8 + key.len()))
            .sum::<u64>()
            + store_len(&pages);
        let mut grant = done!(self.admit(len));
        let mut l = done!(self.pool.fix_exclusive(leaf, None));
        let v = View::new(l.page());
        if !v.is_node() || !v.is_leaf() || v.root() != self.root {
            return Err(corrupt_node(leaf));
        }
        let records: Vec<Record<'_>> = valued
            .iter()
            .map(|(key, _)| Record {
                pid: leaf,
                change: Change::Remove { key },
            })
            .collect();
        for record in &records {
            record.apply(&mut l.page_mut())?;
        }
        let lsns = self.log_batch(
            &mut grant,
            &records,
            &[StoreRecord::Abandon {
                pids: pages.clone(),
            }],
        );
        l.changed(*lsns.last().expect("a record"));
        drop(l);
        self.free_abandoned(pages)?;
        Ok(IOResult::Done(Drained::More))
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
