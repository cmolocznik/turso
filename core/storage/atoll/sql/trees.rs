//! Per-tree commit stamps (DESIGN.md §17): for each tree, the newest commit that wrote it, so a
//! DDL that reads a tree's rows in a transaction with an older snapshot fails with `BusySnapshot`
//! rather than building from rows the snapshot misses.
//!
//! A commit raises the stamps of the trees it wrote in its published hook, once the horizon covers
//! it and before it releases the write gate; a DDL reads them after its exclusive take drained the
//! writers, so it sees every drained commit's raise. Stamps are in memory, keyed by root, and
//! forgotten when a tree is freed or dropped, so the map is bounded by the live trees; after a
//! restart every snapshot is above the clock's base, so an empty map is exact.

use crate::storage::atoll::locks::Mutex;
use crate::storage::atoll::page::PageId;
use crate::storage::atoll::txn::Ts;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use std::collections::HashMap;

/// A tree's stamp: the newest commit timestamp that wrote it.
pub type Stamp = Arc<AtomicU64>;

#[derive(Default)]
pub struct TreeStamps {
    map: Mutex<HashMap<PageId, Stamp>>,
}

impl TreeStamps {
    /// The stamp of the tree rooted at `root`, for a writer to raise at its commit.
    pub fn stamp(&self, root: PageId) -> Stamp {
        self.map
            .lock()
            .entry(root)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    /// Raises `stamps` to the commit at `ts` that wrote their trees.
    pub fn raise(stamps: &[Stamp], ts: Ts) {
        for s in stamps {
            s.fetch_max(ts, Ordering::Release);
        }
    }

    /// Whether a commit after `snapshot` wrote the tree rooted at `root`.
    pub fn written_since(&self, root: PageId, snapshot: Ts) -> bool {
        self.map
            .lock()
            .get(&root)
            .is_some_and(|s| s.load(Ordering::Acquire) > snapshot)
    }

    /// Forgets the stamp of a tree freed or dropped.
    pub fn forget(&self, root: PageId) {
        self.map.lock().remove(&root);
    }

    pub fn len(&self) -> usize {
        self.map.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
