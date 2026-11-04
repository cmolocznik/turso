//! Tree lifecycles (DESIGN.md §17): a tree created, dropped or cleared in a transaction, and the
//! drain that frees the trees on the abandoned and dropped lists.
//!
//! - **Creating.** A tree created in a transaction whose creation is undone, by a rollback or in
//!   recovery, is put on the abandoned list ([`Transactions::create_tree`]). The commit or rollback
//!   that ends the transaction hands it over with [`AtollDb::abandoned`], and it drains then.
//! - **Dropping.** Destroy deletes a tree's catalog row and writes its root's row on the dropped
//!   list, both versioned writes of the transaction, so its rollback undoes both. Clear points the
//!   row at a new tree created in the transaction, and lists the old root as Destroy does.
//! - **Draining a dropped tree** waits until no snapshot can see it. While the database runs that
//!   needs the oldest snapshot, which garbage collection brings (DESIGN.md §18). At open it holds
//!   for every row on the list, since recovery undid the losers', so opening drains both lists.
//!
//! [`Transactions::create_tree`]: crate::storage::atoll::txn::Transactions::create_tree

use super::catalog::{Catalog, Entry, System};
use super::AtollDb;
use crate::storage::atoll::btree::{Cursor, Drain, Drained, Mode, Outcome, Seek, Tree};
use crate::storage::atoll::page::{PageId, SizeClass};
use crate::storage::atoll::txn::versioned::{Get, Write, WriteOp};
use crate::storage::atoll::txn::Txn;
use crate::sync::atomic::Ordering;
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

/// Dropping or clearing a tree in a transaction: step it with [`AtollDb::destroy`] or
/// [`AtollDb::clear`] until done. A `Write` other than done is the caller's to wait on or fail
/// with, as for any write; stepping the same op again goes on from where it stood.
pub struct Destroy {
    id: u64,
    get: Get,
    entry: Option<Entry>,
    /// Clear's new tree, once created.
    made: Option<PageId>,
    /// The catalog's write, then the dropped list's.
    unlist: Option<WriteOp>,
    unlisted: bool,
    list: Option<WriteOp>,
}

impl Destroy {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            get: Get::new(&super::catalog::key(id)),
            entry: None,
            made: None,
            unlist: None,
            unlisted: false,
            list: None,
        }
    }
}

/// A dropped-list row: the tree id and layout, for diagnostics.
fn dropped_row(id: u64, entry: &Entry) -> Vec<u8> {
    let mut out = id.to_le_bytes().to_vec();
    out.push(entry.layout as u8);
    out
}

impl AtollDb {
    /// Creates an empty tree of `class` in `txn`'s current statement: a rollback past the
    /// statement puts it on the abandoned list. Step it until done.
    pub fn create_tree(&self, txn: &mut Txn, class: SizeClass) -> Result<IOResult<PageId>> {
        self.atoll
            .transactions()
            .create_tree(&self.versions, txn, class, System::Abandoned.root())
    }

    /// Drops tree `op`'s id in `txn`: its catalog row deleted and its root listed as dropped.
    pub fn destroy(&self, txn: &mut Txn, op: &mut Destroy) -> Result<IOResult<Write>> {
        self.retire(txn, op, false)
    }

    /// Clears tree `op`'s id in `txn`: its catalog row pointed at a new, empty tree of the same
    /// class, and its old root listed as dropped.
    pub fn clear(&self, txn: &mut Txn, op: &mut Destroy) -> Result<IOResult<Write>> {
        self.retire(txn, op, true)
    }

    fn retire(&self, txn: &mut Txn, op: &mut Destroy, replace: bool) -> Result<IOResult<Write>> {
        let txns = self.atoll.transactions();
        let entry = match op.entry {
            Some(entry) => entry,
            None => {
                let entry = done!(self.catalog.get(txns, &self.versions, txn, &mut op.get))
                    .ok_or_else(|| {
                        LimboError::InternalError(format!("no tree {} to drop", op.id))
                    })?;
                *op.entry.insert(entry)
            }
        };
        if replace && op.made.is_none() {
            op.made = Some(done!(self.create_tree(txn, entry.root.class())));
        }
        if !op.unlisted {
            let unlist = op.unlist.get_or_insert_with(|| match op.made {
                Some(root) => Catalog::put_op(op.id, &Entry { root, ..entry }, Mode::Update),
                None => Catalog::delete_op(op.id),
            });
            match done!(self.catalog.write(txns, &self.versions, txn, unlist)) {
                Write::Done(Outcome::Updated) => op.unlisted = true,
                Write::Done(other) => {
                    return Err(LimboError::InternalError(format!(
                        "dropping tree {}'s catalog row came to {other:?}",
                        op.id
                    )))
                }
                other => return Ok(IOResult::Done(other)),
            }
        }
        let list = op.list.get_or_insert_with(|| {
            WriteOp::put(
                &entry.root.raw().to_be_bytes(),
                dropped_row(op.id, &entry),
                Mode::Insert,
            )
        });
        match done!(self.dropped.write(txns, &self.versions, txn, list)) {
            Write::Done(Outcome::Inserted) => Ok(IOResult::Done(Write::Done(Outcome::Updated))),
            Write::Done(other) => Err(LimboError::InternalError(format!(
                "listing the dropped tree {:?} came to {other:?}",
                entry.root
            ))),
            other => Ok(IOResult::Done(other)),
        }
    }

    /// Hands over the trees a commit or rollback abandoned, each on the abandoned list, to drain.
    pub fn abandoned(&self, roots: &[PageId]) {
        self.draining.lock().extend_from_slice(roots);
    }

    /// Drains every tree handed over. One drainer works at a time: a call while another is at work
    /// returns at once, and a drain cut short by IO goes on at the next call.
    pub fn drain(&self) -> Result<IOResult<()>> {
        if self
            .drainer
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Ok(IOResult::Done(()));
        }
        let out = self.drain_claimed();
        self.drainer.store(false, Ordering::Release);
        out
    }

    fn drain_claimed(&self) -> Result<IOResult<()>> {
        let list = self.open_plain(System::Abandoned.root());
        loop {
            let mut current = self.current.lock();
            if current.is_none() {
                let Some(root) = self.draining.lock().pop() else {
                    return Ok(IOResult::Done(()));
                };
                *current = Some((root, Drain::new(&root.raw().to_be_bytes())));
            }
            let (root, op) = current.as_mut().expect("a tree being drained");
            let root = *root;
            if done!(self.open_plain(root).drain(&list, op)) == Drained::Gone {
                *current = None;
                self.rowids.forget(root);
                self.stamps.forget(root);
            }
        }
    }

    /// Drains every tree on both lists, at open, where every row on them is committed.
    pub(super) fn drain_lists(&self) -> Result<()> {
        for system in [System::Abandoned, System::Dropped] {
            let list = self.open_plain(system.root());
            loop {
                let mut cursor = Cursor::new();
                let found = super::drive(&self.io, || list.seek(&mut cursor, &[], Seek::Ge))?;
                if !found {
                    break;
                }
                let key = cursor.key().expect("an entry").to_vec();
                let root = key
                    .as_slice()
                    .try_into()
                    .map(|k| PageId::from_raw(u64::from_be_bytes(k)))
                    .map_err(|_| {
                        LimboError::Corrupt(format!("a {system:?} list key of {} bytes", key.len()))
                    })?;
                let tree = self.open_plain(root);
                let mut op = Drain::new(&key);
                while super::drive(&self.io, || tree.drain(&list, &mut op))? == Drained::More {}
                self.rowids.forget(root);
            }
        }
        Ok(())
    }

    pub(super) fn open_plain(&self, root: PageId) -> Tree {
        Tree::open(
            self.io.clone(),
            self.atoll.pool().clone(),
            self.atoll.store().clone(),
            root,
        )
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
