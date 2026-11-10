//! Rolling a running transaction back (DESIGN.md §11.5; audit 17), from its own changes.
//!
//! A transaction's changes ([`super::changes`]) name every key its statements wrote, in order,
//! each by the statement that wrote it, so walked back from the last they come newest statement
//! first. A key is put back only once the leaf, under its latch, holds this transaction's version
//! from that statement: then the version that one replaced goes back whole, version header and
//! all, from the arena entry its header names, or the key is removed if it names none, the key
//! having had no entry. Nothing is logged: the changes undone are cut from the transaction's, and
//! what of them was spilled already is cut from it by the next part or the Commit, which begins
//! where they began (DESIGN.md §11.4.1). The changes are read back newest first, from the chunks in
//! memory and then from the parts spilled, each read back from the log once it is durable past
//! the part. A statement writing a key again keeps the
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

use super::changes::{starts, Keyed, Source};
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

/// Where putting back the entry being worked on stands. One per rollback, held in place while it
/// steps: boxing a put or a removal would allocate on every key put back.
#[allow(clippy::large_enum_variant)]
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
    /// Where the changes to put back begin; `None` before the walk begins.
    from: Option<u64>,
    /// Where those not yet walked are read back from, oldest first.
    sources: Vec<Source>,
    /// The changes of the source being walked still to put back, oldest first, by where each
    /// starts in the transaction's changes.
    left: Vec<u64>,
    /// The part being walked, read back from the log: where it begins, and its bytes.
    read: Option<(u64, Vec<u8>)>,
    working: Option<Working>,
}

impl Rollback {
    /// Puts back what every statement after `to` changed: 0 for the whole transaction.
    pub fn after(to: Statement) -> Self {
        Self {
            to,
            from: None,
            sources: Vec::new(),
            left: Vec::new(),
            read: None,
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
    if op.from.is_none() {
        let from = txn.changes.begin_after(op.to).unwrap_or(txn.changes.len());
        op.from = Some(from);
        op.sources = txn.changes.sources_back(from);
        op.sources.reverse();
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
                done!(walk_on(txns, txn, op));
                let newest = op.left.last().map(|&at| txn.changes.statement_at(at));
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
        let at = *op.left.last().expect("a change being put back");
        let statement = txn.changes.statement_at(at);
        let keyed = match &op.read {
            Some((base, bytes)) => Keyed::decode(&bytes[(at - base) as usize..])?,
            None => txn.changes.keyed_at(at)?,
        };
        let (root, key) = match keyed {
            Keyed {
                by_key: true,
                tree,
                key,
                ..
            } => (tree, key.to_vec()),
            // A plain tree's change, a write made plainly and a creation are a statement's only
            // with its own tree, which the creation's lane abandons whole.
            _ => {
                op.left.pop();
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
                op.left.pop();
                op.working = None;
                continue;
            }
        };
        op.working = Some(Working::Entry(next));
    }
}

/// Once the changes of the source being walked are all put back, reads the next source's: a chunk
/// in memory, or a part read back from the log once it is durable past it. Done with `op.left`
/// holding some, or with every source walked.
fn walk_on(txns: &Transactions, txn: &Txn, op: &mut Rollback) -> Result<IOResult<()>> {
    let from = op.from.expect("a walk begun");
    while op.left.is_empty() {
        let Some(&source) = op.sources.last() else {
            op.read = None;
            return Ok(IOResult::Done(()));
        };
        match source {
            Source::Memory(i) => {
                let (base, first, bytes) = txn.changes.chunk(i);
                op.left = starts(bytes, base, first, from)?;
                op.read = None;
            }
            Source::Spilled(part) => {
                done!(txns.log.wait_durable(part.lsn));
                let record = txns.log.read_record(part.at)?;
                let payload = &record.payload;
                let word = |at: usize| {
                    payload
                        .get(at..at + 8)
                        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                };
                let bytes = payload.get(16..16 + part.len as usize);
                let (Some(start), Some(begins), Some(bytes)) = (word(0), word(8), bytes) else {
                    return Err(LimboError::Corrupt(format!(
                        "transaction {}'s part at {} read back short",
                        txn.start,
                        part.at.get()
                    )));
                };
                if record.header.kind != super::SPILL || start != txn.start || begins != part.from {
                    return Err(LimboError::Corrupt(format!(
                        "transaction {}'s part at {} read back as another's",
                        txn.start,
                        part.at.get()
                    )));
                }
                op.left = starts(bytes, part.from, part.first, from)?;
                op.read = Some((part.from, bytes.to_vec()));
            }
        }
        op.sources.pop();
    }
    Ok(IOResult::Done(()))
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
