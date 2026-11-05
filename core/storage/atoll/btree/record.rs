//! The change one step of a B+-tree makes to one node, applied under the node's latch
//! ([`Record::apply`]). Nothing is logged of it (audit 18): a checkpoint writes the nodes as they
//! stand at its cut, and recovery replays the transactions' changes by key, never a node's.

use super::super::page::PageId;
use super::super::shared::BytesMut;
use super::node::{self, child_value, View};
use crate::{LimboError, Result};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change<'a> {
    Insert {
        key: &'a [u8],
        value: &'a [u8],
        out_of_line: bool,
    },
    Remove {
        key: &'a [u8],
    },
    Update {
        key: &'a [u8],
        value: &'a [u8],
        out_of_line: bool,
    },
    /// The left half of a split: the entries before `keep`, up to `sep`, the node's right
    /// neighbour or rightmost child then `right`.
    Split {
        right: PageId,
        keep: usize,
        sep: &'a [u8],
    },
    /// A parent taking a split: `sep` routes to `left`, and the child after it, which was `left`,
    /// is `right`.
    Branch {
        left: PageId,
        right: PageId,
        sep: &'a [u8],
    },
    /// An inner node's first child removed, or, once it has no key, the child after its keys,
    /// which leaves it [`node::NO_CHILD`].
    Unbranch {
        child: PageId,
    },
    /// A reserving leaf's reserves given back for `keys`, each a length (u16) and a key
    /// ([`node::clear_reserve`]): room no checkpoint needs any more.
    Settle {
        keys: &'a [u8],
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<'a> {
    pub pid: PageId,
    pub change: Change<'a>,
}

fn corrupt(what: &str) -> LimboError {
    LimboError::Corrupt(format!("a B-tree {what} record does not apply"))
}

impl Record<'_> {
    /// Makes the change on `page`, the node `pid` as the change found it. Frame hints it stores
    /// are 0; the caller sets them where it knows them.
    pub fn apply<P: BytesMut + ?Sized>(&self, page: &mut P) -> Result<()> {
        self.apply_at(page, None)
    }

    /// [`apply`](Self::apply), with where the change's key is or goes in the node, and whether
    /// it is there, when the caller knows: what a search found under the latch the change is made
    /// under, which a search here would find again. Without it, it searches.
    pub fn apply_at<P: BytesMut + ?Sized>(
        &self,
        page: &mut P,
        at: Option<(usize, bool)>,
    ) -> Result<()> {
        // An insert's key must lie between the fences too; the others are found by search alone.
        let found = |page: &P, key: &[u8], fenced: bool| -> Option<(usize, bool)> {
            let v = View::new(page);
            match at {
                Some(found) => {
                    debug_assert_eq!(v.search(key), Some(found));
                    debug_assert!(!fenced || v.covers(key) == Some(true));
                    Some(found)
                }
                None if fenced && v.covers(key) != Some(true) => None,
                None => v.search(key),
            }
        };
        match &self.change {
            Change::Insert {
                key,
                value,
                out_of_line,
            } => match found(page, key, true) {
                Some((pos, false)) => {
                    if node::insert(page, pos, key, value, *out_of_line) {
                        Ok(())
                    } else {
                        Err(corrupt("insert"))
                    }
                }
                _ => Err(corrupt("insert")),
            },
            Change::Remove { key } => match found(page, key, false) {
                Some((pos, true)) => {
                    node::remove(page, pos);
                    Ok(())
                }
                _ => Err(corrupt("remove")),
            },
            Change::Update {
                key,
                value,
                out_of_line,
            } => match found(page, key, false) {
                Some((pos, true)) if node::set_value(page, pos, value, *out_of_line) => Ok(()),
                _ => Err(corrupt("update")),
            },
            Change::Split { right, keep, sep } => split(page, *right, *keep, sep),
            Change::Unbranch { child } => unbranch(page, *child),
            Change::Settle { keys } => settle(page, keys),
            Change::Branch { left, right, sep } => {
                let v = View::new(&*page);
                let (pos, exact) = v.search(sep).ok_or_else(|| corrupt("branch"))?;
                if exact
                    || v.covers(sep) != Some(true)
                    || v.child(pos).map(|c| c.0) != Some(*left)
                    || !node::insert(page, pos, sep, &child_value(*left, 0), false)
                {
                    return Err(corrupt("branch"));
                }
                node::set_child(page, pos + 1, *right, 0);
                Ok(())
            }
        }
    }
}

/// Gives back the reserves of `keys`, each a length and a key, in the reserving leaf `page`.
fn settle<P: BytesMut + ?Sized>(page: &mut P, mut keys: &[u8]) -> Result<()> {
    if !View::new(&*page).reserving() {
        return Err(corrupt("settle"));
    }
    while !keys.is_empty() {
        let len = keys
            .get(..2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
            .ok_or_else(|| corrupt("settle"))?;
        let key = keys.get(2..2 + len).ok_or_else(|| corrupt("settle"))?;
        match View::new(&*page).search(key) {
            Some((pos, true)) => {
                node::clear_reserve(page, pos);
            }
            _ => return Err(corrupt("settle")),
        }
        keys = &keys[2 + len..];
    }
    Ok(())
}

/// Removes `child` from the inner node `page`: its first keyed child, or, once it has no key, the
/// child after its keys.
fn unbranch<P: BytesMut + ?Sized>(page: &mut P, child: PageId) -> Result<()> {
    let v = View::new(&*page);
    if !v.is_node() || v.is_leaf() || child == node::NO_CHILD {
        return Err(corrupt("unbranch"));
    }
    match v.count() {
        Some(0) if v.next().0 == child => {
            node::set_next(page, node::NO_CHILD, 0);
            Ok(())
        }
        Some(n) if n > 0 && v.child(0).map(|c| c.0) == Some(child) => {
            node::remove(page, 0);
            Ok(())
        }
        _ => Err(corrupt("unbranch")),
    }
}

/// Keeps the first `keep` entries of the node in `page`, up to `sep`: a leaf's right neighbour
/// becomes `right`, and an inner node's rightmost child becomes the child of entry `keep`, whose
/// key `sep` is. Checked and made in place.
fn split<P: BytesMut + ?Sized>(page: &mut P, right: PageId, keep: usize, sep: &[u8]) -> Result<()> {
    let v = View::new(&*page);
    let count = v.count().ok_or_else(|| corrupt("split"))?;
    let at_most = |i: usize| {
        v.compare_key(i, sep)
            .is_some_and(|o| o != Ordering::Greater)
    };
    let between = v.covers(sep) == Some(true) && v.upper_is(sep) == Some(false);
    let level = v.level();
    let valid = keep <= count
        && between
        && (keep == 0 || at_most(keep - 1))
        && if level == 0 {
            keep == count || v.compare_key(keep, sep) == Some(Ordering::Greater)
        } else {
            keep < count && v.compare_key(keep, sep) == Some(Ordering::Equal)
        };
    if !valid {
        return Err(corrupt("split"));
    }
    let next = if level == 0 {
        (right, 0)
    } else {
        (v.child(keep).ok_or_else(|| corrupt("split"))?.0, 0)
    };
    node::split_off(page, keep, sep, next).ok_or_else(|| corrupt("split"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An unbranch removes an inner node's first child, then, with no key left, the child after
    /// its keys, and refuses any other child, a leaf, and "no child" itself.
    #[test]
    fn an_unbranch_removes_the_first_child_then_the_last_and_nothing_else() {
        let pid = PageId::from_raw(77);
        let record = |child| Record {
            pid,
            change: Change::Unbranch { child },
        };
        let (a, b, c) = (
            PageId::from_raw(1),
            PageId::from_raw(2),
            PageId::from_raw(3),
        );
        let layout = |level, next| node::Layout {
            root: pid,
            level,
            lower: None,
            upper: None,
            next: (next, 0),
            reserving: false,
        };
        let entries = [
            node::Entry::child(b"m".to_vec(), a, 0),
            node::Entry::child(b"t".to_vec(), b, 0),
        ];
        let mut page = vec![0u8; 4096];
        node::build(page.as_mut_slice(), &layout(1, c), &entries);
        assert!(
            record(b).apply(page.as_mut_slice()).is_err(),
            "not the first"
        );
        assert!(record(c).apply(page.as_mut_slice()).is_err(), "keys left");
        for (child, left) in [(a, 1), (b, 0)] {
            record(child).apply(page.as_mut_slice()).unwrap();
            let v = View::new(page.as_slice());
            assert_eq!((v.count(), v.next().0), (Some(left), c));
        }
        record(c).apply(page.as_mut_slice()).unwrap();
        assert_eq!(View::new(page.as_slice()).next().0, node::NO_CHILD);
        assert!(
            record(c).apply(page.as_mut_slice()).is_err(),
            "no child left"
        );
        assert!(record(node::NO_CHILD).apply(page.as_mut_slice()).is_err());
        let mut leaf = vec![0u8; 4096];
        node::build(leaf.as_mut_slice(), &layout(0, PageId::from_raw(0)), []);
        assert!(record(a).apply(leaf.as_mut_slice()).is_err(), "a leaf");
    }
}
