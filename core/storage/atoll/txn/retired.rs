//! The list of retired pages: pages of out-of-line values a versioned write replaced, which stay
//! while a snapshot may read the version they hold.
//!
//! A versioned write that replaces an out-of-line value leaves its pages in place and enters them
//! here, in its transaction: a rollback, or the undo of a transaction a crash left open, takes the
//! entry back out with the change it belonged to. Garbage collection frees an entry's pages once
//! no snapshot is older than its transaction's commit, and recovery frees every entry left, since
//! no snapshot survives a crash.
//!
//! The list is a logged tree whose root is page 0 of the smallest class: the first page a new
//! database allocates, created at every open until it exists. A root never moves, so it stays
//! there. Entries are keyed `start | slot | sequence`, the transaction and its count of entries,
//! and hold the page ids.

use super::Ts;
use crate::io::IO;
use crate::storage::atoll::btree::Tree;
use crate::storage::atoll::buffer::BufferPool;
use crate::storage::atoll::page::{PageId, SizeClass};
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::PageStore;
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};

pub fn root() -> PageId {
    PageId::new(SizeClass::new(0).expect("a class"), 0)
}

/// Creates the list's tree if the database has none yet.
pub fn ensure(
    io: &Arc<dyn IO>,
    pool: &Arc<BufferPool>,
    store: &Arc<PageStore>,
) -> Result<IOResult<()>> {
    match store.entry(root())? {
        IOResult::Done(FREE) => {}
        IOResult::Done(_) => return Ok(IOResult::Done(())),
        IOResult::IO(io) => return Ok(IOResult::IO(io)),
    }
    let created = match Tree::create(io, pool, store, root().class())? {
        IOResult::Done(r) => r,
        IOResult::IO(io) => return Ok(IOResult::IO(io)),
    };
    if created != root() {
        return Err(LimboError::Corrupt(format!(
            "the list of retired pages was created at {created:?}, not at {:?}",
            root()
        )));
    }
    Ok(IOResult::Done(()))
}

pub fn open(io: Arc<dyn IO>, pool: Arc<BufferPool>, store: Arc<PageStore>) -> Tree {
    Tree::open(io, pool, store, root())
}

/// The key of the `sequence`th entry of the transaction that started at `start` in `slot`.
pub fn key(start: Ts, slot: u16, sequence: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(14);
    out.extend_from_slice(&start.to_be_bytes());
    out.extend_from_slice(&slot.to_be_bytes());
    out.extend_from_slice(&sequence.to_be_bytes());
    out
}

/// The transaction an entry's key names: its start and slot.
pub fn owner(key: &[u8]) -> Option<(Ts, u16)> {
    Some((
        u64::from_be_bytes(key.get(..8)?.try_into().ok()?),
        u16::from_be_bytes(key.get(8..10)?.try_into().ok()?),
    ))
}

pub fn encode(pids: &[PageId]) -> Vec<u8> {
    pids.iter().flat_map(|p| p.raw().to_le_bytes()).collect()
}

pub fn decode(value: &[u8]) -> Option<Vec<PageId>> {
    (value.len() % 8 == 0).then(|| {
        value
            .chunks_exact(8)
            .map(|c| PageId::from_raw(u64::from_le_bytes(c.try_into().unwrap())))
            .collect()
    })
}
