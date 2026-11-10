//! Freeing a whole tree nothing reads or writes any more (DESIGN.md §17): one on a list of trees
//! whose creation was undone, or that were dropped.
//!
//! The drain walks by position, never by key: from the root to an inner node's first child, or,
//! once it has no key left, to the child after its keys, every node naming the root as its tree.
//! Removing children from the left moves no fence, so a tree half drained is never checked by its
//! fences. The drain's whole state is the tree. Each step lies wholly before a checkpoint's cut or
//! wholly after it (the store's change gate), and frees one node:
//!
//! - a leaf: removed from its parent, then freed with every page of its values;
//! - an inner node with no child left: removed from its parent, then freed;
//! - the root, once it is a leaf or has no child: the tree's entry removed from its list, then
//!   the root freed with its values' pages, so a root page taken again by another tree never reads
//!   as one listed.
//!
//! A page whose free meets a latched metadata page is handed to the checkpointer, which frees it.

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

impl Tree {
    /// One step of freeing this tree, which nothing else reads or writes, whose entry on `list`
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
        let Some(change) = self.store.change() else {
            return Ok(crate::storage::atoll::buffer::gate_closed(&self.pool));
        };
        let (pid, parent, seen) = done!(self.walk());
        let entries = match seen {
            Seen::Child(_) => unreachable!("the walk stops at a leaf or a node with no child"),
            Seen::Childless => Vec::new(),
            Seen::Leaf(entries) => entries,
        };
        // Every page of each value out of line: the page or directory, then the chunks.
        let mut values = Vec::new();
        for (_, stored, out_of_line) in entries {
            if !out_of_line {
                continue;
            }
            let reference = stored
                .len()
                .checked_sub(REF)
                .and_then(|at| ValueRef::decode(&stored[at..]))
                .ok_or_else(|| corrupt_node(pid))?;
            match done!(value::pages(&self.pool, reference, &|| true)) {
                Ok(pages) => values.extend(pages),
                Err(_) => {
                    return Err(LimboError::Corrupt(format!(
                        "a value of the tree {:?} drained lost its pages",
                        self.root
                    )))
                }
            }
        }
        match parent {
            Some(parent) => match done!(self.unbranch(parent, pid, values)) {
                Ok(drained) => Ok(IOResult::Done(drained)),
                Err(refused) => {
                    // Its latches let go: the change gate too, then room for the copies, and the
                    // step again.
                    drop(change);
                    done!(self.pool.wait_for_room(refused));
                    Ok(IOResult::Done(Drained::More))
                }
            },
            None => {
                drop(change);
                let pids = std::iter::once(pid).chain(values).collect();
                op.last = Some(Remove::new(&op.key).abandoning(pids));
                self.drain(list, op)
            }
        }
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

    /// Removes `node` from `parent`, then frees it with every page of its values, `values`.
    /// `None` if there was no room for the copies a checkpoint's cut needs of the two: nothing
    /// changed.
    fn unbranch(
        &self,
        parent: PageId,
        node: PageId,
        values: Vec<PageId>,
    ) -> Result<IOResult<std::result::Result<Drained, Refused>>> {
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
        // The node is freed once unlinked: its copy too, as a cut may still write it.
        if let Err(refused) = self.pool.keep(&[&p, &n]) {
            p.release_unchanged();
            n.release_unchanged();
            return Ok(IOResult::Done(Err(refused)));
        }
        Record {
            pid: parent,
            change: Change::Unbranch { child: node },
        }
        .apply(&mut p.page_mut())?;
        p.changed();
        drop(p);
        n.abandon();
        if let IOResult::IO(_) = self.pool.free(n)? {
            self.store.orphaned(node);
        }
        self.free_abandoned(values)?;
        Ok(IOResult::Done(Ok(Drained::More)))
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
