//! Rolling a running transaction back (DESIGN.md §11.5), from its version indexes.
//!
//! A transaction's entries in its slot's version index of one node class, read backward from its
//! last, come newest statement first ([`super::versioned`]); the indexes of every class the slot
//! has are merged by statement, so the whole transaction goes back newest statement first. Each
//! entry names a key one of its statements changed and the entry that statement replaced.
//!
//! An index entry is put before the leaf write it is for, which may never happen: a write that
//! found another transaction's version open, or a conflict, stops between the two. So a key is put
//! back only once the leaf, under its latch, holds this transaction's version from that statement.
//! Then the entry it replaced goes back whole, version header and all, or the key is removed if
//! it had none, in one compensation record that names the unit it completed: the transaction, the
//! statement, the tree and the key. Statements a key had after this one were put back before it,
//! newest first, so the leaf holds this statement's version again unless the write never happened.
//!
//! Nothing is removed from the indexes: a reader may be following one of their entries.

use super::versioned::{decode_entry, index_key_parts, Version, VersionedTree, Versions, HEADER};
use super::{Transactions, Ts, Txn};
use crate::io::IO;
use crate::storage::atoll::btree::record::{Change, Compensation, Record};
use crate::storage::atoll::btree::value::ValueRef;
use crate::storage::atoll::btree::{
    Cursor, Mode, Outcome, Put, Remove, Seek, Stored, StoredValue, Tree,
};
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

/// What a statement replaced of a key: an entry of a version index.
struct Replaced {
    statement: u16,
    tree: PageId,
    key: Vec<u8>,
    /// The entry the statement's first change of the key replaced; `None` for none.
    entry: Option<Stored>,
}

/// One version index read backward: the entry its cursor is at, still to put back.
struct Lane {
    index: Tree,
    cursor: Cursor,
    positioned: bool,
    at: Option<Replaced>,
}

/// Where putting back the entry of the lane being worked on stands.
enum Step {
    /// Reading the key's entry, to check it is still the statement's.
    Look,
    Put(Put),
    Remove(Remove),
    /// Done with the entry: the lane moves to the one before.
    Next,
}

/// A rollback in progress: every key a statement after `to` changed, put back. Step it with
/// [`undo`].
pub struct Rollback {
    to: u16,
    lanes: Option<Vec<Lane>>,
    /// The lane being worked on, and where that stands.
    working: Option<(usize, Step)>,
}

impl Rollback {
    /// Puts back what every statement after `to` changed: 0 for the whole transaction.
    pub fn after(to: u16) -> Self {
        Self {
            to,
            lanes: None,
            working: None,
        }
    }

    /// The last statement kept.
    pub fn to(&self) -> u16 {
        self.to
    }
}

impl Lane {
    /// The entry the cursor is at, if it is one of `start`'s statements after `to`.
    fn read(&mut self, start: Ts, to: u16) -> Result<()> {
        self.at = match (self.cursor.key(), self.cursor.value()) {
            (Some(key), Some(value)) => {
                let (ts, statement, tree, key) = index_key_parts(key).ok_or_else(|| {
                    LimboError::Corrupt("a version index key does not decode".into())
                })?;
                if ts != start || statement <= to {
                    None
                } else {
                    Some(Replaced {
                        statement,
                        tree,
                        key: key.to_vec(),
                        entry: decode_entry(value)?,
                    })
                }
            }
            _ => None,
        };
        Ok(())
    }
}

/// Steps `op`, rolling `txn` back as far as `op` says. Done once every key is put back; the
/// transaction's tail then runs through their compensation records.
pub fn undo(
    txns: &Transactions,
    versions: &Versions,
    txn: &mut Txn,
    op: &mut Rollback,
) -> Result<IOResult<()>> {
    let lanes = op.lanes.get_or_insert_with(|| {
        versions
            .index_roots(txn.slot)
            .into_iter()
            .map(|root| Lane {
                index: versions.open_index(root),
                cursor: Cursor::new(),
                positioned: false,
                at: None,
            })
            .collect()
    });
    // Each lane at its transaction's last entry: the last key below the next start's.
    for lane in lanes.iter_mut().filter(|l| !l.positioned) {
        let past = (txn.start + 1).to_be_bytes();
        done!(lane.index.seek(&mut lane.cursor, &past, Seek::Lt));
        lane.positioned = true;
        lane.read(txn.start, op.to)?;
    }
    loop {
        let (i, step) = match op.working.take() {
            Some(working) => working,
            None => {
                let newest = lanes
                    .iter()
                    .enumerate()
                    .filter_map(|(i, l)| l.at.as_ref().map(|r| (r.statement, i)))
                    .max();
                match newest {
                    Some((_, i)) => (i, Step::Look),
                    None => return Ok(IOResult::Done(())),
                }
            }
        };
        let lane = &mut lanes[i];
        let replaced = lane.at.as_ref().expect("a lane at an entry");
        let tree = versions.open_tree(replaced.tree);
        let next = match step {
            Step::Look => {
                op.working = Some((i, Step::Look));
                let current = done!(tree.tree().stored(&replaced.key));
                let current = current.filter(|c| {
                    Version::decode(&c.head).is_some_and(|v| {
                        (v.slot, v.ts, v.command) == (txn.slot, txn.start, replaced.statement)
                    })
                });
                match current {
                    // The write the entry was put for never happened, or was put back already.
                    None => Step::Next,
                    Some(current) => {
                        let of = Compensation {
                            root: replaced.tree,
                            statement: replaced.statement,
                            undid: None,
                        };
                        match &replaced.entry {
                            // A value out of line goes back by its reference, its pages kept.
                            Some(entry) => Step::Put(
                                Put::restoring(&replaced.key, entry, Mode::Update)
                                    .expecting(Some(&current))
                                    .compensating(txns.tail(txn), of),
                            ),
                            None => Step::Remove(Remove::new(&replaced.key).compensating(
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
                    op.working = Some((i, Step::Put(put)));
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
                    op.working = Some((i, Step::Remove(remove)));
                    return Ok(IOResult::IO(io));
                }
                IOResult::Done(true) => {
                    txn.logged(remove.group().expect("a compensation is logged"));
                    Step::Next
                }
                IOResult::Done(false) => Step::Look,
            },
            Step::Next => {
                op.working = Some((i, Step::Next));
                done!(lane.index.prev(&mut lane.cursor));
                lane.read(txn.start, op.to)?;
                op.working = None;
                continue;
            }
        };
        op.working = Some((i, next));
    }
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
        let Some(record) = Record::decode(r.header.kind, &r.payload)? else {
            continue;
        };
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
            // Changes outside any transaction were not its.
            Change::Insert { undo: None, .. }
            | Change::Update { undo: None, .. }
            | Change::Init { .. }
            | Change::Remove { .. }
            | Change::Split { .. }
            | Change::Branch { .. } => continue,
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
