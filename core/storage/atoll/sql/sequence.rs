//! Sequence values (DESIGN.md §17, R8). `nextval` and `setval` act outside any transaction, as in
//! PostgreSQL: a value drawn is never drawn again, whatever becomes of the transaction that drew
//! it.
//!
//! A sequence's catalog row names no tree of its own. Its value is one row of the plain sequences
//! tree, keyed by the sequence's tree id and the start of the transaction that created it, which
//! no other transaction has; so an id drawn again after a restart never meets a stale row. A
//! change is a compare-and-put: the value computed from the row read is put only if the row still
//! holds what was read, and the row is read again otherwise. Creating a sequence writes its
//! catalog row in the transaction and puts its value row at once; dropping it deletes the catalog
//! row. A value row that no committed catalog row names, of a creation rolled back or a drop
//! committed, is removed at open.

use super::catalog::{key, Entry, Layout, System};
use super::AtollDb;
use crate::storage::atoll::btree::{Cursor, Mode, Outcome, Put, Remove, Seek, StoredValue, Tree};
use crate::storage::atoll::log::LogWait;
use crate::storage::atoll::txn::changes::Changes;
use crate::storage::atoll::txn::versioned::{Get, Write, WriteOp};
use crate::storage::atoll::txn::{Isolation, Ts, Txn};
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

/// A sequence's value: the last drawn or set, and whether `nextval` has returned it, as Turso's
/// backing table holds them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Value {
    pub value: i64,
    pub is_called: bool,
}

impl Value {
    fn encode(self) -> Vec<u8> {
        let mut out = self.value.to_le_bytes().to_vec();
        out.push(self.is_called as u8);
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        match bytes {
            [v @ .., called @ (0 | 1)] if v.len() == 8 => Ok(Self {
                value: i64::from_le_bytes(v.try_into().unwrap()),
                is_called: *called == 1,
            }),
            _ => Err(LimboError::Corrupt(format!(
                "a sequence value of {} bytes",
                bytes.len()
            ))),
        }
    }
}

/// The key of the value row of the sequence `id` created by the transaction that started at
/// `created_by`.
pub fn value_key(id: u64, created_by: Ts) -> [u8; 16] {
    let mut out = [0; 16];
    out[..8].copy_from_slice(&id.to_be_bytes());
    out[8..].copy_from_slice(&created_by.to_be_bytes());
    out
}

/// Creating a sequence in a transaction: step it with [`AtollDb::create_sequence`] until done.
pub struct Create {
    id: u64,
    value: Value,
    row: Option<WriteOp>,
    put: Option<Put>,
    /// The value row put, published as a change of its own.
    publishing: Option<Changes>,
    /// Its wait on the log for the publication's room.
    log_wait: LogWait,
}

impl Create {
    pub fn new(id: u64, value: Value) -> Self {
        Self {
            id,
            value,
            row: None,
            put: None,
            publishing: None,
            log_wait: LogWait::default(),
        }
    }
}

/// A change of a sequence's value: step it with [`AtollDb::change_sequence`] until done.
pub struct Change {
    key: [u8; 16],
    /// The put of the value computed, expecting the row it was computed from, and that value.
    put: Option<(Put, Value)>,
    /// The value put, and its publication as a change of its own.
    publishing: Option<(Changes, Value)>,
    /// Its wait on the log for the publication's room.
    log_wait: LogWait,
}

impl Change {
    /// A change of the value row keyed `key` ([`value_key`]).
    pub fn new(key: [u8; 16]) -> Self {
        Self {
            key,
            put: None,
            publishing: None,
            log_wait: LogWait::default(),
        }
    }
}

/// A value row put outside any transaction, as the change replay applies (audit 17).
fn value_change(key: &[u8], value: Value) -> Changes {
    let mut changes = Changes::new();
    changes.plain_put(0, System::Sequences.root(), key, &value.encode());
    changes
}

impl AtollDb {
    fn sequences(&self) -> Tree {
        self.open_plain(System::Sequences.root())
    }

    /// Creates the sequence `op`'s id in `txn`, starting at `op`'s value: its catalog row in the
    /// transaction, its value row at once. A `Write` other than done is the caller's to wait on or
    /// fail with; stepping the same op again goes on from where it stood.
    pub fn create_sequence(&self, txn: &mut Txn, op: &mut Create) -> Result<IOResult<Write>> {
        let txns = self.atoll.transactions();
        let entry = Entry {
            root: System::Sequences.root(),
            layout: Layout::Sequence,
            encoding: 0,
            created_by: txn.start(),
        };
        let row = op
            .row
            .get_or_insert_with(|| super::catalog::Catalog::put_op(op.id, &entry, Mode::Insert));
        match done!(self.catalog.write(txns, &self.versions, txn, row)) {
            Write::Done(Outcome::Inserted) => {}
            Write::Done(other) => {
                return Err(LimboError::InternalError(format!(
                    "creating sequence {}'s catalog row came to {other:?}",
                    op.id
                )))
            }
            other => return Ok(IOResult::Done(other)),
        }
        if op.publishing.is_none() {
            let key = value_key(op.id, txn.start());
            let put = op
                .put
                .get_or_insert_with(|| Put::new(&key, op.value.encode(), Mode::Insert));
            match done!(self.sequences().put(put)) {
                Outcome::Inserted => op.publishing = Some(value_change(&key, op.value)),
                other => {
                    return Err(LimboError::InternalError(format!(
                        "putting sequence {}'s value came to {other:?}",
                        op.id
                    )))
                }
            }
        }
        done!(txns.publish_alone(
            &mut op.log_wait,
            op.publishing.as_ref().expect("a value row put")
        ));
        Ok(IOResult::Done(Write::Done(Outcome::Inserted)))
    }

    /// Changes a sequence's value to what `next` computes from it, outside any transaction, by a
    /// compare-and-put, so concurrent changes each see the value the one before them put. `next`
    /// may be called more than once and must compute from its argument alone. Returns the value
    /// put. Step it until done.
    pub fn change_sequence(
        &self,
        op: &mut Change,
        next: &mut dyn FnMut(Value) -> Result<Value>,
    ) -> Result<IOResult<Value>> {
        let tree = self.sequences();
        loop {
            if let Some((changes, new)) = &op.publishing {
                done!(self
                    .atoll
                    .transactions()
                    .publish_alone(&mut op.log_wait, changes));
                let new = *new;
                op.publishing = None;
                return Ok(IOResult::Done(new));
            }
            if op.put.is_none() {
                let stored = done!(tree.stored(&op.key)).ok_or_else(|| {
                    LimboError::InternalError("a sequence with no value row".into())
                })?;
                let value = match &stored.value {
                    StoredValue::Inline(v) => Value::decode(v)?,
                    _ => return Err(LimboError::Corrupt("a sequence value out of line".into())),
                };
                let new = next(value)?;
                let put = Put::new(&op.key, new.encode(), Mode::Update).expecting(Some(&stored));
                op.put = Some((put, new));
            }
            let (put, new) = op.put.as_mut().expect("a put");
            let outcome = done!(tree.put(put));
            let new = *new;
            op.put = None;
            match outcome {
                Outcome::Updated => op.publishing = Some((value_change(&op.key, new), new)),
                // Changed since it was read: read it again.
                Outcome::Changed => continue,
                other => {
                    return Err(LimboError::InternalError(format!(
                        "changing a sequence's value came to {other:?}"
                    )))
                }
            }
        }
    }

    /// Removes every value row no committed catalog row names, at open.
    pub(super) fn drop_unnamed_sequences(&self) -> Result<()> {
        let txns = self.atoll.transactions();
        let tree = self.sequences();
        let mut txn = super::drive(&self.io, || txns.begin(Isolation::Snapshot))?;
        txns.statement(&mut txn)?;
        let mut cursor = Cursor::new();
        let mut found = super::drive(&self.io, || tree.seek(&mut cursor, &[], Seek::Ge))?;
        let mut stale = Vec::new();
        while found {
            let k = cursor.key().expect("an entry");
            if k.len() != 16 {
                return Err(LimboError::Corrupt(format!(
                    "a sequence key of {} bytes",
                    k.len()
                )));
            }
            let id = u64::from_be_bytes(k[..8].try_into().unwrap());
            let created_by = u64::from_be_bytes(k[8..].try_into().unwrap());
            let mut get = Get::new(&key(id));
            let row = super::drive(&self.io, || {
                self.catalog.get(txns, &self.versions, &mut txn, &mut get)
            })?;
            let named =
                row.is_some_and(|e| e.layout == Layout::Sequence && e.created_by == created_by);
            if !named {
                stale.push(k.to_vec());
            }
            found = super::drive(&self.io, || tree.next(&mut cursor))?;
        }
        txns.abort(txn);
        for k in stale {
            let mut remove = Remove::new(&k);
            super::drive(&self.io, || tree.remove(&mut remove))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
