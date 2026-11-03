//! Log records of B+-tree nodes. Each changes one node and is applied the same way when made and
//! when replayed ([`Record::apply`]), so recovery rebuilds exactly what ran.
//!
//! ```text
//! init:   pid | image                         a node built whole (node::image)
//! insert: pid | out: u8 | key_len: u16 | key | value
//! remove: pid | key
//! update: pid | out: u8 | key_len: u16 | key | value
//! split:  pid | right: u64 | keep: u16 | sep  the left half of a split: entries before `keep`,
//!                                             up to `sep`
//! branch: pid | left: u64 | right: u64 | sep  a parent taking a split: `sep` routes to `left`,
//!                                             and the child after it, which was `left`, is `right`
//! txn insert: pid | root: u64 | out: u8 | key_len: u16 | key | value
//! txn update: pid | root: u64 | out: u8 | key_len: u16 | key | value_len: u32 | value | old
//! restore:    pid | root: u64 | statement: u16 | undid: u64 | out: u8 | key_len: u16 | key | value
//! unput:      pid | root: u64 | statement: u16 | undid: u64 | key
//! ```
//!
//! A transaction's insert and update also name the tree, by its root, and an update carries the
//! value it replaces: what undoes them, by tree and key, after splits and merges (DESIGN.md §7.1).
//! Their header names the transaction and chains to its previous group.
//!
//! A restore puts a key's entry back and an unput removes a key that had none: the compensation
//! records of putting back what a transaction's statement changed (DESIGN.md §11.5). Each names
//! the unit it completed, the tree, the statement and the key, and, from recovery, the change it
//! undid ([`Compensation`]). They are redone like any change and never undone.
//!
//! A change to several nodes is a group of these ([`super::super::log::Log::append_group`]).

use super::super::log::{GroupStart, Kind};
use super::super::page::PageId;
use super::super::shared::BytesMut;
use super::node::{self, child_value, View};
use crate::{LimboError, Result};
use std::cmp::Ordering;

pub const INIT: Kind = Kind::new(10).unwrap();
pub const INSERT: Kind = Kind::new(11).unwrap();
pub const REMOVE: Kind = Kind::new(12).unwrap();
pub const UPDATE: Kind = Kind::new(13).unwrap();
pub const SPLIT: Kind = Kind::new(14).unwrap();
pub const BRANCH: Kind = Kind::new(15).unwrap();
pub const TXN_INSERT: Kind = Kind::new(17).unwrap();
pub const TXN_UPDATE: Kind = Kind::new(18).unwrap();
pub const RESTORE: Kind = Kind::new(21).unwrap();
pub const UNPUT: Kind = Kind::new(22).unwrap();

pub fn owns(kind: Kind) -> bool {
    (INIT.get()..=BRANCH.get()).contains(&kind.get())
        || [TXN_INSERT, TXN_UPDATE, RESTORE, UNPUT].contains(&kind)
}

/// What a compensation record completed: every change its transaction's `statement` made to its
/// key in the tree rooted at `root` put back, or, from recovery, the change whose group starts at
/// `undid` undone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Compensation {
    pub root: PageId,
    pub statement: u16,
    pub undid: Option<GroupStart>,
}

/// Bytes a compensation's unit takes in its record.
pub const COMPENSATION: usize = 8 + 2 + 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change<'a> {
    Init {
        image: &'a [u8],
    },
    Insert {
        key: &'a [u8],
        value: &'a [u8],
        out_of_line: bool,
        /// For a transaction's insert: the tree's root.
        undo: Option<PageId>,
    },
    Remove {
        key: &'a [u8],
    },
    Update {
        key: &'a [u8],
        value: &'a [u8],
        out_of_line: bool,
        /// For a transaction's update: the tree's root, and the value it replaces.
        undo: Option<(PageId, &'a [u8])>,
    },
    /// A key's entry put back, as `of` says.
    Restore {
        key: &'a [u8],
        value: &'a [u8],
        out_of_line: bool,
        of: Compensation,
    },
    /// A key that had no entry removed again, as `of` says.
    Unput {
        key: &'a [u8],
        of: Compensation,
    },
    Split {
        right: PageId,
        keep: usize,
        sep: &'a [u8],
    },
    Branch {
        left: PageId,
        right: PageId,
        sep: &'a [u8],
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

impl<'a> Record<'a> {
    pub fn kind(&self) -> Kind {
        match self.change {
            Change::Init { .. } => INIT,
            Change::Insert { undo: None, .. } => INSERT,
            Change::Insert { undo: Some(_), .. } => TXN_INSERT,
            Change::Remove { .. } => REMOVE,
            Change::Update { undo: None, .. } => UPDATE,
            Change::Update { undo: Some(_), .. } => TXN_UPDATE,
            Change::Split { .. } => SPLIT,
            Change::Branch { .. } => BRANCH,
            Change::Restore { .. } => RESTORE,
            Change::Unput { .. } => UNPUT,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.pid.raw().to_le_bytes().to_vec();
        match &self.change {
            Change::Init { image } => out.extend_from_slice(image),
            Change::Insert {
                key,
                value,
                out_of_line,
                undo,
            } => {
                if let Some(root) = undo {
                    out.extend(root.raw().to_le_bytes());
                }
                out.push(*out_of_line as u8);
                out.extend((key.len() as u16).to_le_bytes());
                out.extend_from_slice(key);
                out.extend_from_slice(value);
            }
            Change::Update {
                key,
                value,
                out_of_line,
                undo,
            } => {
                if let Some((root, _)) = undo {
                    out.extend(root.raw().to_le_bytes());
                }
                out.push(*out_of_line as u8);
                out.extend((key.len() as u16).to_le_bytes());
                out.extend_from_slice(key);
                if let Some((_, old)) = undo {
                    out.extend((value.len() as u32).to_le_bytes());
                    out.extend_from_slice(value);
                    out.extend_from_slice(old);
                } else {
                    out.extend_from_slice(value);
                }
            }
            Change::Remove { key } => out.extend_from_slice(key),
            Change::Restore {
                key,
                value,
                out_of_line,
                of,
            } => {
                encode_compensation(&mut out, of);
                out.push(*out_of_line as u8);
                out.extend((key.len() as u16).to_le_bytes());
                out.extend_from_slice(key);
                out.extend_from_slice(value);
            }
            Change::Unput { key, of } => {
                encode_compensation(&mut out, of);
                out.extend_from_slice(key);
            }
            Change::Split { right, keep, sep } => {
                out.extend(right.raw().to_le_bytes());
                out.extend((*keep as u16).to_le_bytes());
                out.extend_from_slice(sep);
            }
            Change::Branch { left, right, sep } => {
                out.extend(left.raw().to_le_bytes());
                out.extend(right.raw().to_le_bytes());
                out.extend_from_slice(sep);
            }
        }
        out
    }

    /// The record of `kind`, or `None` if the kind is not a B-tree node's.
    pub fn decode(kind: Kind, payload: &'a [u8]) -> Result<Option<Self>> {
        if !owns(kind) {
            return Ok(None);
        }
        let bad = || {
            LimboError::Corrupt(format!(
                "a B-tree record of kind {} does not decode",
                kind.get()
            ))
        };
        let word = |at: usize| -> Result<u64> {
            Ok(u64::from_le_bytes(
                payload.get(at..at + 8).ok_or_else(bad)?.try_into().unwrap(),
            ))
        };
        let pid = PageId::from_raw(word(0)?);
        if kind == RESTORE || kind == UNPUT {
            let of = Compensation {
                root: PageId::from_raw(word(8)?),
                statement: u16::from_le_bytes(
                    payload.get(16..18).ok_or_else(bad)?.try_into().unwrap(),
                ),
                undid: GroupStart::decode(word(18)?).ok_or_else(bad)?,
            };
            let rest = &payload[8 + COMPENSATION..];
            if kind == UNPUT {
                return Ok(Some(Self {
                    pid,
                    change: Change::Unput { key: rest, of },
                }));
            }
            let out_of_line = match rest.first() {
                Some(0) => false,
                Some(1) => true,
                _ => return Err(bad()),
            };
            let len =
                u16::from_le_bytes(rest.get(1..3).ok_or_else(bad)?.try_into().unwrap()) as usize;
            let key = rest.get(3..3 + len).ok_or_else(bad)?;
            return Ok(Some(Self {
                pid,
                change: Change::Restore {
                    key,
                    value: &rest[3 + len..],
                    out_of_line,
                    of,
                },
            }));
        }
        let txn = kind == TXN_INSERT || kind == TXN_UPDATE;
        let root = if txn {
            Some(PageId::from_raw(word(8)?))
        } else {
            None
        };
        let rest = &payload[if txn { 16 } else { 8 }..];
        let keyed = || -> Result<(&'a [u8], &'a [u8], bool)> {
            let out_of_line = match rest.first() {
                Some(0) => false,
                Some(1) => true,
                _ => return Err(bad()),
            };
            let len =
                u16::from_le_bytes(rest.get(1..3).ok_or_else(bad)?.try_into().unwrap()) as usize;
            let key = rest.get(3..3 + len).ok_or_else(bad)?;
            Ok((key, &rest[3 + len..], out_of_line))
        };
        let change = match kind {
            k if k == INIT => Change::Init { image: rest },
            k if k == INSERT || k == TXN_INSERT => {
                let (key, value, out_of_line) = keyed()?;
                Change::Insert {
                    key,
                    value,
                    out_of_line,
                    undo: root,
                }
            }
            k if k == UPDATE => {
                let (key, value, out_of_line) = keyed()?;
                Change::Update {
                    key,
                    value,
                    out_of_line,
                    undo: None,
                }
            }
            k if k == TXN_UPDATE => {
                let (key, rest, out_of_line) = keyed()?;
                let len =
                    u32::from_le_bytes(rest.get(..4).ok_or_else(bad)?.try_into().unwrap()) as usize;
                Change::Update {
                    key,
                    value: rest.get(4..4 + len).ok_or_else(bad)?,
                    out_of_line,
                    undo: Some((root.expect("a root"), &rest[4 + len..])),
                }
            }
            k if k == REMOVE => Change::Remove { key: rest },
            k if k == SPLIT => Change::Split {
                right: PageId::from_raw(word(8)?),
                keep: u16::from_le_bytes(payload.get(16..18).ok_or_else(bad)?.try_into().unwrap())
                    as usize,
                sep: &payload[18..],
            },
            _ => Change::Branch {
                left: PageId::from_raw(word(8)?),
                right: PageId::from_raw(word(16)?),
                sep: payload.get(24..).ok_or_else(bad)?,
            },
        };
        Ok(Some(Self { pid, change }))
    }

    /// Makes the change on `page`, the node `pid` as the change found it. Frame hints it stores
    /// are 0; the caller sets them where it knows them.
    pub fn apply<P: BytesMut + ?Sized>(&self, page: &mut P) -> Result<()> {
        match &self.change {
            Change::Init { image } => node::apply_image(page, image).ok_or_else(|| corrupt("init")),
            Change::Insert {
                key,
                value,
                out_of_line,
                ..
            } => {
                let v = View::new(&*page);
                match v.search(key) {
                    Some((pos, false)) if v.covers(key) == Some(true) => {
                        if node::insert(page, pos, key, value, *out_of_line) {
                            Ok(())
                        } else {
                            Err(corrupt("insert"))
                        }
                    }
                    _ => Err(corrupt("insert")),
                }
            }
            Change::Remove { key } | Change::Unput { key, .. } => {
                match View::new(&*page).search(key) {
                    Some((pos, true)) => {
                        node::remove(page, pos);
                        Ok(())
                    }
                    _ => Err(corrupt("remove")),
                }
            }
            Change::Restore {
                key,
                value,
                out_of_line,
                ..
            } => match View::new(&*page).search(key) {
                Some((pos, true)) if node::set_value(page, pos, value, *out_of_line) => Ok(()),
                _ => Err(corrupt("restore")),
            },
            Change::Update {
                key,
                value,
                out_of_line,
                ..
            } => match View::new(&*page).search(key) {
                Some((pos, true)) if node::set_value(page, pos, value, *out_of_line) => Ok(()),
                _ => Err(corrupt("update")),
            },
            Change::Split { right, keep, sep } => split(page, *right, *keep, sep),
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

fn encode_compensation(out: &mut Vec<u8>, of: &Compensation) {
    out.extend(of.root.raw().to_le_bytes());
    out.extend(of.statement.to_le_bytes());
    out.extend(GroupStart::encode(of.undid).to_le_bytes());
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

    /// A compensation reads back as written, and refuses being cut short.
    #[test]
    fn a_compensation_reads_back_as_written() {
        let pid = PageId::from_raw(77);
        let of = |undid| Compensation {
            root: PageId::from_raw(5),
            statement: 513,
            undid,
        };
        for undid in [None, Some(GroupStart::at(0)), Some(GroupStart::at(1 << 30))] {
            for change in [
                Change::Restore {
                    key: b"key",
                    value: b"head and value",
                    out_of_line: false,
                    of: of(undid),
                },
                Change::Unput {
                    key: b"key",
                    of: of(undid),
                },
            ] {
                let record = Record { pid, change };
                let bytes = record.encode();
                assert_eq!(
                    Record::decode(record.kind(), &bytes).unwrap(),
                    Some(record.clone())
                );
                assert!(
                    Record::decode(record.kind(), &bytes[..8 + COMPENSATION - 1]).is_err(),
                    "{:?} cut short decoded",
                    record.kind()
                );
            }
        }
    }
}
