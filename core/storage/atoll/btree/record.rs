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
//! txn update: pid | root: u64 | out: u8 | key_len: u16 | key | value_len: u32 | value | found: u8 |
//!             old
//! restore:    pid | root: u64 | statement: u32 | undid: u64 | out: u8 | key_len: u16 | key | value
//! unput:      pid | root: u64 | statement: u32 | undid: u64 | key
//! unbranch:   pid | child: u64            an inner node losing its first child, or, with no key
//!                                         left, the child after its keys ([`super::Drain`])
//! ```
//!
//! A transaction's insert and update also name the tree, by its root, and an update carries the
//! entry its statement found, which undoing it puts back: what undoes them, by tree and key, after
//! splits and merges (DESIGN.md §7.1). That entry is the one the update replaced, unless its
//! statement had changed the key already, whose earlier versions are gone (DESIGN.md §12, step 5).
//! `found` is 0 for an entry whose value is inline, 1 for one out of line, and 2 for none, `old`
//! then being empty. Their header names the transaction and chains to its previous group.
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
pub const UNBRANCH: Kind = Kind::new(26).unwrap();
pub const TXN_PATCH: Kind = Kind::new(27).unwrap();
pub const SETTLE: Kind = Kind::new(30).unwrap();

pub fn owns(kind: Kind) -> bool {
    (INIT.get()..=BRANCH.get()).contains(&kind.get())
        || [
            TXN_INSERT, TXN_UPDATE, RESTORE, UNPUT, UNBRANCH, TXN_PATCH, SETTLE,
        ]
        .contains(&kind)
}

/// What a compensation record completed: every change its transaction's `statement` made to its
/// key in the tree rooted at `root` put back, or, from recovery, the change whose group starts at
/// `undid` undone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Compensation {
    pub root: PageId,
    pub statement: u32,
    pub undid: Option<GroupStart>,
}

/// Bytes a compensation's unit takes in its record.
pub const COMPENSATION: usize = 8 + 4 + 8;

/// `found` of a transaction's update whose statement found no entry.
const FOUND_NONE: u8 = 2;

/// The entry a transaction's statement found for a key, which undoing its update puts back: as the
/// leaf stores it, and whether its value is out of line; `None` for none.
pub type Found<'a> = Option<(&'a [u8], bool)>;

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
        /// For a transaction's update: the tree's root, and the entry its statement found.
        undo: Option<(PageId, Found<'a>)>,
    },
    /// A transaction's change of a key's entry in place, its value inline and its length kept:
    /// the new head, and `bytes` at `at` of the value past the head, with what undoes it, the
    /// tree's root and the old head and bytes. A delete's has no bytes: the tombstone keeps the
    /// value it replaced. Only a statement's first change of a key is a patch, so what it undoes
    /// to is the entry the statement found.
    Patch {
        key: &'a [u8],
        head: &'a [u8],
        at: usize,
        bytes: &'a [u8],
        root: PageId,
        old_head: &'a [u8],
        old_bytes: &'a [u8],
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
            Change::Unbranch { .. } => UNBRANCH,
            Change::Settle { .. } => SETTLE,
            Change::Restore { .. } => RESTORE,
            Change::Unput { .. } => UNPUT,
            Change::Patch { .. } => TXN_PATCH,
        }
    }

    /// The payload [`encode`](Self::encode) makes, handed to `f` in pieces in order, with the
    /// key and values borrowed: an insert or an update is a few scalars on the stack and its
    /// byte strings, so a row's bytes go from the caller straight to where `f` copies them. Any
    /// other change is encoded.
    pub fn with_pieces<R>(&self, f: impl FnOnce(&[&[u8]]) -> R) -> R {
        // pid, root, flag and key length; a value's length; a found entry's flag. A patch's pid,
        // root, key length, head length, offset and length are no more.
        let mut scalars = [0u8; 8 + 8 + 1 + 2 + 4 + 1];
        let mut n = 0;
        let mut push = |scalars: &mut [u8], bytes: &[u8]| {
            scalars[n..n + bytes.len()].copy_from_slice(bytes);
            n += bytes.len();
            n
        };
        push(&mut scalars, &self.pid.raw().to_le_bytes());
        match &self.change {
            Change::Insert {
                key,
                value,
                out_of_line,
                undo,
            } => {
                if let Some(root) = undo {
                    push(&mut scalars, &root.raw().to_le_bytes());
                }
                push(&mut scalars, &[*out_of_line as u8]);
                let a = push(&mut scalars, &(key.len() as u16).to_le_bytes());
                f(&[&scalars[..a], key, value])
            }
            Change::Update {
                key,
                value,
                out_of_line,
                undo: None,
            } => {
                push(&mut scalars, &[*out_of_line as u8]);
                let a = push(&mut scalars, &(key.len() as u16).to_le_bytes());
                f(&[&scalars[..a], key, value])
            }
            Change::Update {
                key,
                value,
                out_of_line,
                undo: Some((root, found)),
            } => {
                push(&mut scalars, &root.raw().to_le_bytes());
                push(&mut scalars, &[*out_of_line as u8]);
                let a = push(&mut scalars, &(key.len() as u16).to_le_bytes());
                let b = push(&mut scalars, &(value.len() as u32).to_le_bytes());
                match found {
                    Some((old, old_out_of_line)) => {
                        let c = push(&mut scalars, &[*old_out_of_line as u8]);
                        f(&[
                            &scalars[..a],
                            key,
                            &scalars[a..b],
                            value,
                            &scalars[b..c],
                            old,
                        ])
                    }
                    None => {
                        let c = push(&mut scalars, &[FOUND_NONE]);
                        f(&[&scalars[..a], key, &scalars[a..b], value, &scalars[b..c]])
                    }
                }
            }
            Change::Patch {
                key,
                head,
                at,
                bytes,
                root,
                old_head,
                old_bytes,
            } => {
                push(&mut scalars, &root.raw().to_le_bytes());
                push(&mut scalars, &(key.len() as u16).to_le_bytes());
                push(&mut scalars, &[head.len() as u8]);
                push(&mut scalars, &(*at as u16).to_le_bytes());
                let a = push(&mut scalars, &(bytes.len() as u16).to_le_bytes());
                f(&[&scalars[..a], key, head, bytes, old_head, old_bytes])
            }
            _ => f(&[&self.encode()]),
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
                if let Some((_, found)) = undo {
                    out.extend((value.len() as u32).to_le_bytes());
                    out.extend_from_slice(value);
                    match found {
                        Some((old, old_out_of_line)) => {
                            out.push(*old_out_of_line as u8);
                            out.extend_from_slice(old);
                        }
                        None => out.push(FOUND_NONE),
                    }
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
            Change::Unbranch { child } => out.extend(child.raw().to_le_bytes()),
            Change::Settle { keys } => out.extend_from_slice(keys),
            Change::Patch {
                key,
                head,
                at,
                bytes,
                root,
                old_head,
                old_bytes,
            } => {
                out.extend(root.raw().to_le_bytes());
                out.extend((key.len() as u16).to_le_bytes());
                out.push(head.len() as u8);
                out.extend((*at as u16).to_le_bytes());
                out.extend((bytes.len() as u16).to_le_bytes());
                for piece in [*key, *head, *bytes, *old_head, *old_bytes] {
                    out.extend_from_slice(piece);
                }
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
                statement: u32::from_le_bytes(
                    payload.get(16..20).ok_or_else(bad)?.try_into().unwrap(),
                ),
                undid: GroupStart::decode(word(20)?).ok_or_else(bad)?,
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
        if kind == TXN_PATCH {
            let root = PageId::from_raw(word(8)?);
            let u16_at = |at: usize| -> Result<usize> {
                let b = payload.get(at..at + 2).ok_or_else(bad)?;
                Ok(u16::from_le_bytes([b[0], b[1]]) as usize)
            };
            let key_len = u16_at(16)?;
            let head_len = *payload.get(18).ok_or_else(bad)? as usize;
            let at = u16_at(19)?;
            let len = u16_at(21)?;
            let mut cut = 23;
            let mut take = |n: usize| -> Result<&'a [u8]> {
                let piece = payload.get(cut..cut + n).ok_or_else(bad)?;
                cut += n;
                Ok(piece)
            };
            let (key, head, bytes) = (take(key_len)?, take(head_len)?, take(len)?);
            let (old_head, old_bytes) = (take(head_len)?, take(len)?);
            if cut != payload.len() {
                return Err(bad());
            }
            return Ok(Some(Self {
                pid,
                change: Change::Patch {
                    key,
                    head,
                    at,
                    bytes,
                    root,
                    old_head,
                    old_bytes,
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
                let old = rest.get(5 + len..).ok_or_else(bad)?;
                let found = match rest[4 + len] {
                    0 => Some((old, false)),
                    1 => Some((old, true)),
                    FOUND_NONE if old.is_empty() => None,
                    _ => return Err(bad()),
                };
                Change::Update {
                    key,
                    value: &rest[4..4 + len],
                    out_of_line,
                    undo: Some((root.expect("a root"), found)),
                }
            }
            k if k == REMOVE => Change::Remove { key: rest },
            k if k == SETTLE => Change::Settle { keys: rest },
            k if k == UNBRANCH && payload.len() == 16 => Change::Unbranch {
                child: PageId::from_raw(word(8)?),
            },
            k if k == UNBRANCH => return Err(bad()),
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
        self.apply_at(page, None)
    }

    /// [`apply`](Self::apply), with where the change's key is or goes in the node, and whether
    /// it is there, when the caller knows: what a search found under the latch the change is made
    /// under, which a search here would find again. Replay passes none, and searches.
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
            Change::Init { image } => node::apply_image(page, image).ok_or_else(|| corrupt("init")),
            Change::Insert {
                key,
                value,
                out_of_line,
                ..
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
            Change::Remove { key } | Change::Unput { key, .. } => match found(page, key, false) {
                Some((pos, true)) => {
                    node::remove(page, pos);
                    Ok(())
                }
                _ => Err(corrupt("remove")),
            },
            Change::Restore {
                key,
                value,
                out_of_line,
                ..
            } => match found(page, key, false) {
                Some((pos, true)) if node::set_value(page, pos, value, *out_of_line) => Ok(()),
                _ => Err(corrupt("restore")),
            },
            Change::Update {
                key,
                value,
                out_of_line,
                ..
            } => match found(page, key, false) {
                Some((pos, true)) if node::set_value(page, pos, value, *out_of_line) => Ok(()),
                _ => Err(corrupt("update")),
            },
            Change::Patch {
                key,
                head,
                at,
                bytes,
                ..
            } => match found(page, key, false) {
                Some((pos, true)) if node::patch(page, pos, head, *at, bytes) => Ok(()),
                _ => Err(corrupt("patch")),
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

fn encode_compensation(out: &mut Vec<u8>, of: &Compensation) {
    out.extend(of.root.raw().to_le_bytes());
    out.extend(of.statement.to_le_bytes());
    out.extend(GroupStart::encode(of.undid).to_le_bytes());
}

/// Keeps the first `keep` entries of the node in `page`, up to `sep`: a leaf's right neighbour
/// becomes `right`, and an inner node's rightmost child becomes the child of entry `keep`, whose
/// key `sep` is. Checked and made in place.
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

    /// Every change's pieces, joined, are its encoding, byte for byte.
    #[test]
    fn a_records_pieces_are_its_encoding() {
        let (pid, root) = (PageId::from_raw(0x0102_0304_0506_0708), PageId::from_raw(5));
        let of = Compensation {
            root,
            statement: 513,
            undid: Some(GroupStart::at(80)),
        };
        let mut changes = vec![
            Change::Init { image: b"image" },
            Change::Remove { key: b"key" },
            Change::Restore {
                key: b"key",
                value: b"value",
                out_of_line: true,
                of,
            },
            Change::Unput { key: b"key", of },
            Change::Split {
                right: root,
                keep: 3,
                sep: b"s",
            },
            Change::Unbranch { child: root },
            Change::Patch {
                key: b"a key",
                head: b"new head",
                at: 3,
                bytes: b"new",
                root,
                old_head: b"old head",
                old_bytes: b"old",
            },
            Change::Patch {
                key: b"a key",
                head: b"new head",
                at: 0,
                bytes: b"",
                root,
                old_head: b"old head",
                old_bytes: b"",
            },
        ];
        for out_of_line in [false, true] {
            for undo in [None, Some(root)] {
                changes.push(Change::Insert {
                    key: b"a key",
                    value: b"head and value",
                    out_of_line,
                    undo,
                });
            }
            for undo in [
                None,
                Some((root, None)),
                Some((root, Some((&b"old"[..], false)))),
                Some((root, Some((&b"old reference"[..], true)))),
            ] {
                changes.push(Change::Update {
                    key: b"a key",
                    value: b"head and new value",
                    out_of_line,
                    undo,
                });
            }
        }
        for change in changes {
            let record = Record { pid, change };
            let joined = record.with_pieces(|pieces| pieces.concat());
            assert_eq!(joined, record.encode(), "{:?}", record.change);
        }
    }

    /// A patch reads back as written and refuses being cut short or running on; applied, it changes
    /// the entry's head and the bytes it names, in place, and refuses a value out of line or too
    /// short to hold them.
    #[test]
    fn a_patch_reads_back_and_changes_its_bytes_in_place() {
        let pid = PageId::from_raw(77);
        let record = |head, at, bytes, old_head, old_bytes| Record {
            pid,
            change: Change::Patch {
                key: b"k",
                head,
                at,
                bytes,
                root: PageId::from_raw(5),
                old_head,
                old_bytes,
            },
        };
        let r = record(&b"HEAD"[..], 2, &b"xy"[..], &b"head"[..], &b"cd"[..]);
        let bytes = r.encode();
        assert_eq!(Record::decode(TXN_PATCH, &bytes).unwrap(), Some(r.clone()));
        assert!(Record::decode(TXN_PATCH, &bytes[..bytes.len() - 1]).is_err());
        assert!(Record::decode(TXN_PATCH, &[bytes.as_slice(), &[0]].concat()).is_err());
        let layout = node::Layout {
            root: pid,
            level: 0,
            lower: None,
            upper: None,
            next: (PageId::from_raw(0), 0),
            reserving: false,
        };
        let mut page = vec![0u8; 4096];
        node::build(
            page.as_mut_slice(),
            &layout,
            &[
                node::Entry {
                    key: b"j".to_vec(),
                    value: b"headabcdef".to_vec(),
                    out_of_line: false,
                },
                node::Entry {
                    key: b"k".to_vec(),
                    value: b"headabcdef".to_vec(),
                    out_of_line: false,
                },
                node::Entry {
                    key: b"l".to_vec(),
                    value: b"headabcdef".to_vec(),
                    out_of_line: false,
                },
            ],
        );
        r.apply(page.as_mut_slice()).unwrap();
        let v = View::new(page.as_slice());
        assert_eq!(v.value(1), Some((b"HEADabxyef".to_vec(), false)));
        assert_eq!(v.value(0), Some((b"headabcdef".to_vec(), false)));
        assert_eq!(v.value(2), Some((b"headabcdef".to_vec(), false)));
        // Past the value's end, and a key with no entry.
        assert!(
            record(&b"HEAD"[..], 5, &b"xyz"[..], &b"head"[..], &b"cde"[..])
                .apply(page.as_mut_slice())
                .is_err()
        );
        let mut out = vec![0u8; 4096];
        node::build(
            out.as_mut_slice(),
            &layout,
            &[node::Entry {
                key: b"k".to_vec(),
                value: b"head and ref".to_vec(),
                out_of_line: true,
            }],
        );
        assert!(r.apply(out.as_mut_slice()).is_err(), "a value out of line");
    }

    /// A compensation reads back as written, and refuses being cut short.
    #[test]
    fn a_compensation_reads_back_as_written() {
        let pid = PageId::from_raw(77);
        let of = |undid| Compensation {
            root: PageId::from_raw(5),
            // Past 16 bits, so the round trip covers every byte of the number.
            statement: (1 << 16) + 513,
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

    /// An unbranch reads back as written, removes an inner node's first child, then, with no key
    /// left, the child after its keys, and refuses any other child, a leaf, and "no child" itself.
    #[test]
    fn an_unbranch_removes_the_first_child_then_the_last_and_nothing_else() {
        let pid = PageId::from_raw(77);
        let record = |child| Record {
            pid,
            change: Change::Unbranch { child },
        };
        let bytes = record(PageId::from_raw(3)).encode();
        assert_eq!(
            Record::decode(UNBRANCH, &bytes).unwrap(),
            Some(record(PageId::from_raw(3)))
        );
        assert!(Record::decode(UNBRANCH, &bytes[..15]).is_err());
        assert!(Record::decode(UNBRANCH, &[bytes.as_slice(), &[0]].concat()).is_err());
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

    /// A transaction's update reads back with the entry its statement found, inline, out of line
    /// or none, and refuses a kind of entry it does not know.
    #[test]
    fn a_transactions_update_reads_back_with_the_entry_its_statement_found() {
        let pid = PageId::from_raw(77);
        let root = PageId::from_raw(5);
        for found in [
            Some((&b"head and old value"[..], false)),
            Some((&b"head and reference"[..], true)),
            None,
        ] {
            let record = Record {
                pid,
                change: Change::Update {
                    key: b"key",
                    value: b"head and new value",
                    out_of_line: true,
                    undo: Some((root, found)),
                },
            };
            let bytes = record.encode();
            assert_eq!(Record::decode(TXN_UPDATE, &bytes).unwrap(), Some(record));
            let mut bad = bytes.clone();
            let at = 16 + 3 + 3 + 4 + b"head and new value".len();
            bad[at] = FOUND_NONE + 1;
            assert!(Record::decode(TXN_UPDATE, &bad).is_err(), "{found:?}");
        }
        // None names no entry, so nothing may follow it.
        let mut trailing = Record {
            pid,
            change: Change::Update {
                key: b"key",
                value: b"v",
                out_of_line: false,
                undo: Some((root, None)),
            },
        }
        .encode();
        trailing.push(0);
        assert!(Record::decode(TXN_UPDATE, &trailing).is_err());
    }
}
