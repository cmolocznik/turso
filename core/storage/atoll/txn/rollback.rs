//! Rolling a running transaction back (DESIGN.md §11.5; audit 17), from its own changes.
//!
//! A transaction's changes ([`super::changes`]) name every key its statements wrote, in order,
//! each by the statement that wrote it, so walked back from the last they come newest statement
//! first. A key is put back only once the leaf, under its latch, holds this transaction's version
//! from that statement: then the version that one replaced goes back whole, version header and
//! all, from the arena entry its header names, or the key is removed if it names none, the key
//! having had no entry. Nothing is logged: what a rollback undoes never reached the log, a
//! transaction's changes being published at its commit. A statement writing a key again keeps the
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
use super::versioned::{decode_entry, restored_reserve, Version, Versions};
use super::{Statement, Transactions, Ts, Txn};
use crate::storage::atoll::btree::{Mode, Outcome, Put, Remove};
use crate::storage::atoll::page::PageId;
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
/// creation undone.
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
                        let mut replaced = match v.prev {
                            None => None,
                            Some(ptr) => {
                                decode_entry(&done!(versions.arenas().read(ptr)).replaced)?
                            }
                        };
                        if let Some(r) = &mut replaced {
                            restored_reserve(&mut r.head, txns, txn)?;
                        }
                        match &replaced {
                            // A value out of line goes back by its reference, its pages kept.
                            Some(replaced) => Step::Put(
                                Put::restoring(&key, replaced, Mode::Update)
                                    .expecting(Some(&current)),
                            ),
                            None => Step::Remove(Remove::new(&key).expecting(&current)),
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
                IOResult::Done(_) => Step::Next,
            },
            Step::Remove(mut remove) => match tree.tree().remove(&mut remove)? {
                IOResult::IO(io) => {
                    op.working = Some(Working::Entry(Step::Remove(remove)));
                    return Ok(IOResult::IO(io));
                }
                IOResult::Done(true) => Step::Next,
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

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
