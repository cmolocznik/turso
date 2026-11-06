//! A transaction's own log (DESIGN.md §7, §11; audit 17): the changes its statements made, by
//! key, in the order made, kept with it until its commit publishes them whole.
//!
//! A change is appended with plain stores, under no shared write. A statement rolled back drops
//! its changes and every later one's, from the mark where it began. A change is:
//!
//! ```text
//! kind: u8 | key_len: u16 | value_len: u32 | tree: u64 | key | value
//! ```
//!
//! `tree` is the tree's root, which never moves. A put sets the key's value and a delete removes
//! the key, whose value is then empty, in a versioned tree, whose entries carry a version header,
//! or a plain one. A creation makes an empty tree at `tree`, the root of the plain list its undo
//! would put it on as its value; a drain frees the tree at `tree` whole and removes it from the
//! plain list rooted at its value. Replay applies them in order through the trees: a put is a put
//! and a delete a delete, so replaying one again changes nothing.

use super::Statement;
use crate::storage::atoll::page::PageId;
use crate::{LimboError, Result};

const PUT: u8 = 1;
const DELETE: u8 = 2;
const CREATE: u8 = 3;
const DRAIN: u8 = 4;
/// A put or delete in a plain tree.
const PLAIN: u8 = 0x80;
/// A put or delete made plainly in a versioned tree by the statement that created it.
const PLAINLY: u8 = 0x40;
/// Bytes of a change before its key.
const HEAD: usize = 1 + 2 + 4 + 8;

/// One change, read back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change<'a> {
    Put {
        tree: PageId,
        plain: bool,
        /// Made plainly by the statement that created the versioned tree: replayed as any put,
        /// and undone with the tree, never by key.
        plainly: bool,
        key: &'a [u8],
        value: &'a [u8],
    },
    Delete {
        tree: PageId,
        plain: bool,
        plainly: bool,
        key: &'a [u8],
    },
    /// An empty tree made at `root`, which the list rooted at `list` takes back if it is undone.
    Create { root: PageId, list: PageId },
    /// The tree at `root` freed whole, and its entry on the list rooted at `list` removed.
    Drain { root: PageId, list: PageId },
}

impl Change<'_> {
    pub fn tree(&self) -> PageId {
        match self {
            Change::Put { tree, .. } | Change::Delete { tree, .. } => *tree,
            Change::Create { root, .. } | Change::Drain { root, .. } => *root,
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            Change::Put { key, .. } | Change::Delete { key, .. } => key,
            Change::Create { .. } | Change::Drain { .. } => &[],
        }
    }
}

/// A transaction's changes.
#[derive(Debug, Default)]
pub struct Changes {
    bytes: Vec<u8>,
    /// Each statement that changed anything, and where its changes begin, in order.
    marks: Vec<(Statement, usize)>,
    /// Every change made and every rollback to a mark, counted: it only rises.
    edits: u64,
    /// The transaction's view moved: every change, rollback, statement and snapshot, counted, and
    /// read without the transaction's lock by a cursor that judged rows at one count.
    views: u64,
    view: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Changes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Statement `statement` put `value` under `key` in the versioned tree rooted at `tree`.
    pub fn put(&mut self, statement: Statement, tree: PageId, key: &[u8], value: &[u8]) {
        self.append(statement, PUT, tree, key, &[value]);
    }

    /// [`put`](Self::put), the value in pieces, copied in in order.
    pub fn put_pieces(&mut self, statement: Statement, tree: PageId, key: &[u8], value: &[&[u8]]) {
        self.append(statement, PUT, tree, key, value);
    }

    /// Statement `statement` deleted `key` from the versioned tree rooted at `tree`.
    pub fn delete(&mut self, statement: Statement, tree: PageId, key: &[u8]) {
        self.append(statement, DELETE, tree, key, &[]);
    }

    /// Statement `statement`, which created the versioned tree rooted at `tree`, put `value`
    /// under `key` there plainly, with no version below.
    pub fn put_plainly(&mut self, statement: Statement, tree: PageId, key: &[u8], value: &[u8]) {
        self.append(statement, PUT | PLAINLY, tree, key, &[value]);
    }

    /// Statement `statement`, which created the versioned tree rooted at `tree`, removed `key`
    /// there plainly.
    pub fn delete_plainly(&mut self, statement: Statement, tree: PageId, key: &[u8]) {
        self.append(statement, DELETE | PLAINLY, tree, key, &[]);
    }

    /// Statement `statement` put `value` under `key` in the plain tree rooted at `tree`.
    pub fn plain_put(&mut self, statement: Statement, tree: PageId, key: &[u8], value: &[u8]) {
        self.append(statement, PUT | PLAIN, tree, key, &[value]);
    }

    /// Statement `statement` deleted `key` from the plain tree rooted at `tree`.
    pub fn plain_delete(&mut self, statement: Statement, tree: PageId, key: &[u8]) {
        self.append(statement, DELETE | PLAIN, tree, key, &[]);
    }

    /// Statement `statement` made an empty tree at `root`, which the list rooted at `list` takes
    /// back if the creation is undone.
    pub fn create(&mut self, statement: Statement, root: PageId, list: PageId) {
        self.append(statement, CREATE, root, &[], &[&list.raw().to_le_bytes()]);
    }

    /// The tree at `root` was freed whole and its entry on the list rooted at `list` removed,
    /// outside any statement.
    pub fn drained(&mut self, root: PageId, list: PageId) {
        self.append(0, DRAIN, root, &[], &[&list.raw().to_le_bytes()]);
    }

    fn append(
        &mut self,
        statement: Statement,
        kind: u8,
        tree: PageId,
        key: &[u8],
        value: &[&[u8]],
    ) {
        let value_len: usize = value.iter().map(|p| p.len()).sum();
        assert!(
            key.len() <= u16::MAX as usize && value_len <= u32::MAX as usize,
            "a change of a {} byte key and a {value_len} byte value",
            key.len()
        );
        // Statement 0 is outside any statement: such changes are published alone, never rolled
        // back to a mark.
        if statement != 0 && self.marks.last().is_none_or(|&(s, _)| s != statement) {
            debug_assert!(self.marks.last().is_none_or(|&(s, _)| s < statement));
            self.marks.push((statement, self.bytes.len()));
        }
        self.edits += 1;
        self.moved();
        self.bytes.reserve(HEAD + key.len() + value_len);
        self.bytes.push(kind);
        self.bytes
            .extend_from_slice(&(key.len() as u16).to_le_bytes());
        self.bytes
            .extend_from_slice(&(value_len as u32).to_le_bytes());
        self.bytes.extend_from_slice(&tree.raw().to_le_bytes());
        self.bytes.extend_from_slice(key);
        for piece in value {
            self.bytes.extend_from_slice(piece);
        }
    }

    /// Drops the changes of every statement after `statement`: a rollback to it, or of the whole
    /// transaction for 0.
    pub fn back_to(&mut self, statement: Statement) {
        self.edits += 1;
        self.moved();
        let keep = self.marks.partition_point(|&(s, _)| s <= statement);
        if let Some(&(_, at)) = self.marks.get(keep) {
            self.bytes.truncate(at);
        }
        self.marks.truncate(keep);
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// How many changes and rollbacks there have been: equal counts mean the transaction changed
    /// nothing between.
    pub fn edits(&self) -> u64 {
        self.edits
    }

    /// The transaction's view moved on: a change, a rollback, a statement or a snapshot.
    pub fn moved(&mut self) {
        self.views += 1;
        self.view
            .store(self.views, std::sync::atomic::Ordering::Relaxed);
    }

    /// The count of the view's moves, and where it is read without the transaction's lock.
    pub fn view(&self) -> (u64, &std::sync::Arc<std::sync::atomic::AtomicU64>) {
        (self.views, &self.view)
    }

    /// The bytes it holds room for: what it grows past only by allocating.
    pub fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    /// Every change of a statement after `statement`, oldest first: its statement and where it
    /// starts, which [`at`](Self::at) reads.
    pub fn since(&self, statement: Statement) -> Vec<(Statement, usize)> {
        let first = self.marks.partition_point(|&(s, _)| s <= statement);
        let mut out = Vec::new();
        for (i, &(s, from)) in self.marks.iter().enumerate().skip(first) {
            let to = self
                .marks
                .get(i + 1)
                .map_or(self.bytes.len(), |&(_, at)| at);
            let mut at = from;
            for change in Decode::new(&self.bytes[from..to]) {
                let change = change.expect("a transaction's own changes decode");
                out.push((s, at));
                at += HEAD + change.key().len() + value_len(&change);
            }
        }
        out
    }

    /// The change that starts at `at`, where [`since`](Self::since) found it.
    pub fn at(&self, at: usize) -> Change<'_> {
        Decode::new(&self.bytes[at..])
            .next()
            .expect("a change where one starts")
            .expect("a transaction's own changes decode")
    }

    /// The changes as their commit publishes them, back to back.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The changes, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = Change<'_>> {
        Decode::new(&self.bytes).map(|c| c.expect("a transaction's own changes decode"))
    }
}

/// The bytes a change's value takes in its record.
fn value_len(change: &Change<'_>) -> usize {
    match change {
        Change::Put { value, .. } => value.len(),
        Change::Delete { .. } => 0,
        Change::Create { .. } | Change::Drain { .. } => 8,
    }
}

/// The changes `bytes` holds back to back, oldest first, each `Corrupt` where the bytes do not
/// hold one together.
pub struct Decode<'a> {
    bytes: &'a [u8],
    failed: bool,
}

impl<'a> Decode<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            failed: false,
        }
    }
}

impl<'a> Iterator for Decode<'a> {
    type Item = Result<Change<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.is_empty() || self.failed {
            return None;
        }
        let corrupt = |why: &str| LimboError::Corrupt(format!("a published change {why}"));
        let b = self.bytes;
        let Some(head) = b.get(..HEAD) else {
            self.failed = true;
            return Some(Err(corrupt("is cut short in its head")));
        };
        let kind = head[0];
        let key_len = u16::from_le_bytes([head[1], head[2]]) as usize;
        let value_len = u32::from_le_bytes(head[3..7].try_into().unwrap()) as usize;
        let tree = PageId::from_raw(u64::from_le_bytes(head[7..15].try_into().unwrap()));
        let Some(rest) = b.get(HEAD..HEAD + key_len + value_len) else {
            self.failed = true;
            return Some(Err(corrupt("is cut short in its key or value")));
        };
        let (key, value) = rest.split_at(key_len);
        let (plain, plainly) = (kind & PLAIN != 0, kind & PLAINLY != 0);
        let change = match kind & !(PLAIN | PLAINLY) {
            PUT if !(plain && plainly) => Change::Put {
                tree,
                plain,
                plainly,
                key,
                value,
            },
            DELETE if value.is_empty() && !(plain && plainly) => Change::Delete {
                tree,
                plain,
                plainly,
                key,
            },
            CREATE if !plain && !plainly && key.is_empty() && value.len() == 8 => Change::Create {
                root: tree,
                list: PageId::from_raw(u64::from_le_bytes(value.try_into().unwrap())),
            },
            DRAIN if !plain && !plainly && key.is_empty() && value.len() == 8 => Change::Drain {
                root: tree,
                list: PageId::from_raw(u64::from_le_bytes(value.try_into().unwrap())),
            },
            _ => {
                self.failed = true;
                return Some(Err(corrupt(&format!(
                    "of kind {kind} with a {key_len} byte key and a {value_len} byte value"
                ))));
            }
        };
        self.bytes = &b[HEAD + key_len + value_len..];
        Some(Ok(change))
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
