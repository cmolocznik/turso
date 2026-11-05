//! Rolling a running transaction back (DESIGN.md §11.5; audit 17), from its own changes.
//!
//! A transaction's changes ([`super::changes`]) name every key its statements wrote, in order,
//! each by the statement that wrote it, so walked back from the last they come newest statement
//! first. A key is put back only once the leaf, under its latch, holds this transaction's version
//! from that statement: then the version that one replaced goes back whole, version header and
//! all, from the arena entry its header names, or the key is removed if it names none, the key
//! having had no entry. That is one compensation record naming the unit it completed: the
//! transaction, the statement, the tree and the key. A statement writing a key again keeps the
//! entry its first write made, which holds what it found, so the key is put back once, at its
//! newest change, and its earlier changes find the leaf holding another version.
//!
//! Nothing is removed from the arena: a reader may be following one of its entries.
//!
//! The trees the transaction created are one more lane, by the statement that created each: a
//! creation is undone after every change of its statement and later, so after every change to its
//! tree. Undoing one puts the root on its plain list, keyed by its big-endian bytes, with the
//! transaction's start as the value. The put is idempotent, and the list's entries are freed only
//! once the transaction has ended (DESIGN.md §17).

use super::changes::Change as Changed;
use super::versioned::{decode_entry, Version, VersionedTree, Versions, HEADER};
use super::{creation, Statement, Transactions, Ts, Txn, CREATE};
use crate::io::IO;
use crate::storage::atoll::btree::record::{Change, Compensation, Record};
use crate::storage::atoll::btree::value::ValueRef;
use crate::storage::atoll::btree::{Mode, Outcome, Put, Remove, Stored, StoredValue, Tree};
use crate::storage::atoll::buffer::BufferPool;
use crate::storage::atoll::drive;
use crate::storage::atoll::log::{framed_len, GroupStart, Log, LogConfig, TxnTail, Writers};
use crate::storage::atoll::page::PageId;
use crate::storage::atoll::store::PageStore;
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// Where putting back the entry being worked on stands.
enum Step {
    /// Reading the key's entry, to check it is still the statement's.
    Look,
    Put(Put),
    Remove(Remove),
    /// Done with the entry: on to the one before.
    Next,
}

/// What a rollback is working on.
enum Working {
    /// The arena entry being put back, and where that stands.
    Entry(Step),
    /// Putting the transaction's last creation on its list.
    Creation(Put),
}

/// A rollback in progress: every key a statement after `to` changed, put back, and every tree a
/// statement after `to` created put on its list. Step it with [`undo`].
pub struct Rollback {
    to: Statement,
    /// The changes of statements after `to` still to put back, oldest first, each by its
    /// statement and where it starts in the transaction's changes: `None` before the walk begins.
    left: Option<Vec<(Statement, usize)>>,
    working: Option<Working>,
}

impl Rollback {
    /// Puts back what every statement after `to` changed: 0 for the whole transaction.
    pub fn after(to: Statement) -> Self {
        Self {
            to,
            left: None,
            working: None,
        }
    }

    /// The last statement kept.
    pub fn to(&self) -> Statement {
        self.to
    }
}

/// Steps `op`, rolling `txn` back as far as `op` says. Done once every key is put back and every
/// creation undone; the transaction's tail then runs through their compensation records.
pub fn undo(
    txns: &Transactions,
    versions: &Versions,
    txn: &mut Txn,
    op: &mut Rollback,
) -> Result<IOResult<()>> {
    if op.left.is_none() {
        op.left = Some(txn.changes.since(op.to));
    }
    loop {
        let step = match op.working.take() {
            Some(Working::Entry(step)) => step,
            Some(Working::Creation(mut put)) => {
                let created = *txn.created.last().expect("a creation being undone");
                match versions.open_plain(created.list).put(&mut put)? {
                    IOResult::IO(io) => {
                        op.working = Some(Working::Creation(put));
                        return Ok(IOResult::IO(io));
                    }
                    IOResult::Done(Outcome::Inserted | Outcome::Unchanged) => {
                        txn.created.pop();
                        txn.abandoned.push(created.root);
                        continue;
                    }
                    IOResult::Done(other) => {
                        return Err(LimboError::InternalError(format!(
                            "putting tree {:?} on its list came to {other:?}",
                            created.root
                        )))
                    }
                }
            }
            None => {
                let newest = op.left.as_ref().and_then(|l| l.last()).map(|&(s, _)| s);
                let creation = txn.created.last().filter(|c| c.statement > op.to);
                match (newest, creation) {
                    // A change of the creation's statement or later first: its tree's changes are
                    // undone before the tree is.
                    (Some(statement), Some(c)) if statement >= c.statement => Step::Look,
                    (_, Some(c)) => {
                        op.working = Some(Working::Creation(abandoning(c.root, txn.start)));
                        continue;
                    }
                    (Some(_), None) => Step::Look,
                    (None, None) => return Ok(IOResult::Done(())),
                }
            }
        };
        let &(statement, at) = op
            .left
            .as_ref()
            .and_then(|l| l.last())
            .expect("a change being put back");
        let (root, key) = match txn.changes.at(at) {
            Changed::Put {
                tree,
                plain: false,
                plainly: false,
                key,
                ..
            }
            | Changed::Delete {
                tree,
                plain: false,
                plainly: false,
                key,
            } => (tree, key.to_vec()),
            // A plain tree's change, a write made plainly and a creation are a statement's only
            // with its own tree, which the creation's lane abandons whole.
            _ => {
                op.left.as_mut().expect("a walk").pop();
                continue;
            }
        };
        let tree = versions.open_tree(root);
        let next = match step {
            Step::Look => {
                op.working = Some(Working::Entry(Step::Look));
                let current = done!(tree.tree().stored(&key));
                let current = current.and_then(|c| {
                    let v = Version::decode(&c.head)?;
                    ((v.slot, v.ts, v.command) == (txn.slot, txn.start, statement))
                        .then_some((c, v))
                });
                match current {
                    // Put back already, at a later change of the statement.
                    None => Step::Next,
                    Some((current, v)) => {
                        let replaced = match v.prev {
                            None => None,
                            Some(ptr) => {
                                decode_entry(&done!(versions.arenas().read(ptr)).replaced)?
                            }
                        };
                        let of = Compensation {
                            root,
                            statement,
                            undid: None,
                        };
                        match &replaced {
                            // A value out of line goes back by its reference, its pages kept.
                            Some(replaced) => Step::Put(
                                Put::restoring(&key, replaced, Mode::Update)
                                    .expecting(Some(&current))
                                    .compensating(txns.tail(txn), of),
                            ),
                            None => Step::Remove(Remove::new(&key).compensating(
                                txns.tail(txn),
                                of,
                                &current,
                            )),
                        }
                    }
                }
            }
            Step::Put(mut put) => match tree.tree().put(&mut put)? {
                IOResult::IO(io) => {
                    op.working = Some(Working::Entry(Step::Put(put)));
                    return Ok(IOResult::IO(io));
                }
                // Changed since the look: look again.
                IOResult::Done(Outcome::Changed) => Step::Look,
                IOResult::Done(_) => {
                    txn.logged(put.group().expect("a compensation is logged"));
                    Step::Next
                }
            },
            Step::Remove(mut remove) => match tree.tree().remove(&mut remove)? {
                IOResult::IO(io) => {
                    op.working = Some(Working::Entry(Step::Remove(remove)));
                    return Ok(IOResult::IO(io));
                }
                IOResult::Done(true) => {
                    txn.logged(remove.group().expect("a compensation is logged"));
                    Step::Next
                }
                IOResult::Done(false) => Step::Look,
            },
            Step::Next => {
                op.left.as_mut().expect("a walk").pop();
                op.working = None;
                continue;
            }
        };
        op.working = Some(Working::Entry(next));
    }
}

/// The put of the tree rooted at `root`, created by the transaction that started at `start`, on
/// its list.
fn abandoning(root: PageId, start: Ts) -> Put {
    Put::new(
        &root.raw().to_be_bytes(),
        start.to_le_bytes().to_vec(),
        Mode::Insert,
    )
}

/// Undoes loser `txn` in recovery (DESIGN.md §12), back along its chain from its last group at
/// `last`, then appends its End. Each group is read whole and checked before its undo is used.
///
/// A change is undone only if the key's entry is still exactly the one it wrote, version header
/// included. A rollback before the crash put back the entry its statement found, and the
/// statement's number is never written again, so none of that statement's entries is there any
/// more; another transaction's change since left its own. Each undo here is logged as a
/// compensation naming the change it undid, in the chain's order, so a recovery that finds one
/// goes on from before the change it names: every change after that one was undone or found done.
/// So nothing but the log and the tree is needed to skip exactly the work done, nothing reads a
/// version index, which does not survive a crash, and the losers may be undone in any order.
#[allow(clippy::too_many_arguments)]
pub(crate) fn undo_loser(
    io: &Arc<dyn IO>,
    base: &str,
    config: &LogConfig,
    pool: &Arc<BufferPool>,
    store: &Arc<PageStore>,
    writers: &Arc<Writers>,
    txn: u64,
    last: GroupStart,
) -> Result<()> {
    let corrupt = |why: String| LimboError::Corrupt(format!("undoing transaction {txn}: {why}"));
    // Recovery's own slot in the registry: one loser at a time, each finished by its End.
    const SLOT: u16 = 0;
    let mut tail = Some(last);
    let mut at = Some(last);
    // The change an earlier recovery's undo got back to, once its compensation is read.
    let mut undone = None;
    while let Some(group) = at {
        let records = Log::read_group(io.clone(), base, config, group)?;
        let r = records
            .iter()
            .find(|r| r.header.txn == txn)
            .ok_or_else(|| {
                corrupt(format!(
                    "its chain names the group at {}, not its",
                    group.get()
                ))
            })?;
        at = r.header.prev;
        if at.is_some_and(|prev| prev >= group) {
            return Err(corrupt(format!(
                "its chain runs forward at {}",
                group.get()
            )));
        }
        if undone == Some(group) {
            continue;
        }
        // A creation: every later change to the tree is undone already, the chain running newest
        // first. The put needs no compensation: a later one jumping past it is logged after it.
        if r.header.kind == CREATE {
            let (root, list) = creation(&r.payload).ok_or_else(|| {
                corrupt(format!(
                    "the creation record at {} does not decode",
                    group.get()
                ))
            })?;
            let list = Tree::open(io.clone(), pool.clone(), store.clone(), list);
            let mut put = abandoning(root, txn);
            match drive(io, || list.put(&mut put))? {
                Outcome::Inserted | Outcome::Unchanged => continue,
                other => {
                    return Err(corrupt(format!(
                        "putting tree {root:?} on its list came to {other:?}"
                    )))
                }
            }
        }
        let Some(record) = Record::decode(r.header.kind, &r.payload)? else {
            continue;
        };
        // A patch: undone while the leaf holds the version it made, its head and the bytes it
        // wrote, by the entry it replaced, which its statement found.
        if let Change::Patch {
            key,
            head,
            at,
            bytes,
            root,
            old_head,
            old_bytes,
        } = record.change
        {
            let statement = Version::decode(head)
                .ok_or_else(|| corrupt("a patch whose head is no version header".into()))?
                .command;
            let tree = VersionedTree::open(io.clone(), pool.clone(), store.clone(), root);
            let of = Compensation {
                root,
                statement,
                undid: Some(group),
            };
            loop {
                let current = drive(io, || tree.tree().stored(key))?;
                let Some(current) = current.filter(|c| {
                    c.head == head
                        && matches!(&c.value, StoredValue::Inline(v)
                            if v.get(at..at + bytes.len()) == Some(bytes))
                }) else {
                    break;
                };
                let StoredValue::Inline(value) = &current.value else {
                    unreachable!("filtered to an inline value");
                };
                let mut was = value.clone();
                was[at..at + old_bytes.len()].copy_from_slice(old_bytes);
                let entry = Stored {
                    head: old_head.to_vec(),
                    value: StoredValue::Inline(was),
                };
                let tail_now = TxnTail::new(txn, tail, writers.clone(), SLOT);
                let mut put = Put::restoring(key, &entry, Mode::Update)
                    .expecting(Some(&current))
                    .compensating(tail_now, of);
                if drive(io, || tree.tree().put(&mut put))? == Outcome::Changed {
                    continue;
                }
                tail = Some(
                    put.group()
                        .ok_or_else(|| corrupt("a compensation not logged".into()))?,
                );
                break;
            }
            continue;
        }
        // What the change wrote, as the leaf stores it, and the entry its statement found, which
        // undoing it puts back.
        let (root, key, wrote, found) = match record.change {
            Change::Insert {
                key,
                value,
                out_of_line,
                undo: Some(root),
            } => (root, key, (value, out_of_line), None),
            Change::Update {
                key,
                value,
                out_of_line,
                undo: Some((root, found)),
            } => (root, key, (value, out_of_line), found),
            // Compensations are redone, never undone. One from an earlier recovery's undo, which
            // went back along the chain to the change it names, says to go on from before that.
            Change::Restore { of, .. } | Change::Unput { of, .. } => {
                if let Some(change) = of.undid {
                    if change >= group {
                        return Err(corrupt(format!(
                            "the compensation at {} names a change after it",
                            group.get()
                        )));
                    }
                    undone = Some(change);
                    at = Some(change);
                }
                continue;
            }
            Change::Patch { .. } => unreachable!("a patch is undone above"),
            // Changes outside any transaction were not its.
            Change::Insert { undo: None, .. }
            | Change::Update { undo: None, .. }
            | Change::Init { .. }
            | Change::Remove { .. }
            | Change::Split { .. }
            | Change::Branch { .. }
            | Change::Unbranch { .. } => continue,
        };
        let statement = Version::decode(wrote.0.get(..HEADER).unwrap_or(&[]))
            .ok_or_else(|| corrupt("a change whose value holds no version header".into()))?
            .command;
        let tree = VersionedTree::open(io.clone(), pool.clone(), store.clone(), root);
        let of = Compensation {
            root,
            statement,
            undid: Some(group),
        };
        loop {
            let current = drive(io, || tree.tree().stored(key))?;
            let still = current.as_ref().is_some_and(|c| match &c.value {
                StoredValue::Inline(v) => !wrote.1 && [c.head.as_slice(), v].concat() == wrote.0,
                StoredValue::OutOfLine(r) => {
                    wrote.1 && [c.head.as_slice(), &r.encode()].concat() == wrote.0
                }
            });
            if !still {
                break;
            }
            let current = current.expect("an entry");
            let tail_now = TxnTail::new(txn, tail, writers.clone(), SLOT);
            let logged = match found {
                Some((old, out_of_line)) => {
                    let entry = found_entry(old, out_of_line).ok_or_else(|| {
                        corrupt("the entry a statement found does not decode".into())
                    })?;
                    // A value out of line goes back by its reference, its pages kept; the one the
                    // change wrote, if out of line, is abandoned.
                    let mut put = Put::restoring(key, &entry, Mode::Update)
                        .expecting(Some(&current))
                        .compensating(tail_now, of);
                    match drive(io, || tree.tree().put(&mut put))? {
                        Outcome::Changed => continue,
                        _ => put.group(),
                    }
                }
                None => {
                    let mut remove = Remove::new(key).compensating(tail_now, of, &current);
                    match drive(io, || tree.tree().remove(&mut remove))? {
                        false => continue,
                        true => remove.group(),
                    }
                }
            };
            tail = Some(logged.ok_or_else(|| corrupt("a compensation not logged".into()))?);
            break;
        }
    }
    let log = store.log();
    let mut admitted = drive(io, || log.admit(framed_len(0)))?;
    let end = TxnTail::new(txn, tail, writers.clone(), SLOT).header(super::END);
    log.append_with(&mut admitted, &end, &[], |_| writers.finished(SLOT, txn));
    Ok(())
}

/// The entry a statement found, as a transaction's update logs it.
fn found_entry(old: &[u8], out_of_line: bool) -> Option<Stored> {
    let (head, rest) = (old.get(..HEADER)?, &old[HEADER.min(old.len())..]);
    Some(Stored {
        head: head.to_vec(),
        value: if out_of_line {
            StoredValue::OutOfLine(ValueRef::decode(rest)?)
        } else {
            StoredValue::Inline(rest.to_vec())
        },
    })
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
