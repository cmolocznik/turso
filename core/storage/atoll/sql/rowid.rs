//! NewRowid's counters (DESIGN.md §17): one per rowid table, in memory.
//!
//! A counter is seeded at its first use from the tree's largest key in the leaf, whatever that
//! entry's version: open, committed or a tombstone, so no rowid any transaction holds is drawn.
//! Drawing takes one above the largest drawn, seeded or raised past, so concurrent inserters never
//! draw the same rowid, and an insert with an explicit rowid raises the counter past it. A counter
//! that holds no rowid, its tree empty at the seed and nothing drawn or raised past since, draws 1,
//! as SQLite's first rowid; once it holds one, it draws one above it, however low, as SQLite's next
//! rowid after a largest of -1 is 0. Past `i64::MAX` the counter draws nothing, and the caller
//! probes random rowids, as SQLite does.
//!
//! Unlike SQLite, which takes the largest rowid at each insert, a counter never goes back: a rowid
//! freed at the top of a table is not drawn again while the database is open, as under Turso's
//! MVCC. Counters are not durable: after a restart they are seeded again.

use super::key;
use crate::storage::atoll::btree::{Cursor, Seek, Tree};
use crate::storage::atoll::page::PageId;
use crate::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::IOResult;
use crate::Result;
use std::collections::HashMap;

/// The counter of one tree: the largest rowid drawn, seeded or raised past, once it holds one.
#[derive(Debug)]
pub struct Counter {
    /// The largest rowid; `i64::MIN` while `holds` is clear, when it stands for none.
    last: AtomicI64,
    /// Whether `last` holds a rowid. It matters only while `last` is `i64::MIN`: a draw never
    /// leaves it there, so it is set before `last` takes `i64::MIN` as a rowid, and read only then.
    holds: AtomicBool,
    seeded: AtomicBool,
}

/// Every rowid table's counter, by its root: a tree a Clear replaced gets a fresh one, seeded from
/// the empty tree, as SQLite's next rowid starts again after one.
#[derive(Default)]
pub struct Rowids {
    counters: Mutex<HashMap<PageId, Arc<Counter>>>,
}

/// Seeding a counter: step it with [`Counter::seed`] until done.
#[derive(Default)]
pub struct Seed {
    cursor: Cursor,
}

#[cfg(test)]
crate::thread::thread_local! {
    /// Counters looked up on this thread, for tests to see a cursor keep its own.
    pub(crate) static LOOKUPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

impl Rowids {
    pub fn counter(&self, root: PageId) -> Arc<Counter> {
        #[cfg(test)]
        LOOKUPS.with(|n| n.set(n.get() + 1));
        self.counters
            .lock()
            .entry(root)
            .or_insert_with(|| {
                Arc::new(Counter {
                    last: AtomicI64::new(i64::MIN),
                    holds: AtomicBool::new(false),
                    seeded: AtomicBool::new(false),
                })
            })
            .clone()
    }

    /// Forgets the counter of a tree that is gone.
    pub fn forget(&self, root: PageId) {
        self.counters.lock().remove(&root);
    }
}

impl Counter {
    pub fn seeded(&self) -> bool {
        self.seeded.load(Ordering::Acquire)
    }

    /// Seeds the counter from `tree`'s largest key, unless it is seeded. Step it until done.
    pub fn seed(&self, tree: &Tree, op: &mut Seed) -> Result<IOResult<()>> {
        if self.seeded() {
            return Ok(IOResult::Done(()));
        }
        match tree.seek(&mut op.cursor, &key::rowid(i64::MAX), Seek::Le)? {
            // An empty tree leaves the counter holding no rowid, so its first draw is 1.
            IOResult::Done(false) => {}
            // A seed another thread found meanwhile, or a rowid raised past since, stands.
            IOResult::Done(true) => self.raise(key::rowid_of(op.cursor.key().expect("an entry"))?),
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
        self.seeded.store(true, Ordering::Release);
        Ok(IOResult::Done(()))
    }

    /// A new rowid, one above the largest drawn, seeded or raised past, as SQLite's: `None` once
    /// `i64::MAX` is taken, when the caller probes random rowids. The counter must be seeded.
    pub fn draw(&self) -> Option<i64> {
        assert!(self.seeded(), "a rowid drawn from a counter not seeded");
        let mut last = self.last.load(Ordering::Acquire);
        loop {
            let next = if last == i64::MIN && !self.holds.load(Ordering::Acquire) {
                1
            } else {
                last.checked_add(1)?
            };
            match self
                .last
                .compare_exchange_weak(last, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(next),
                Err(now) => last = now,
            }
        }
    }

    /// Notes an insert with the explicit rowid `rowid`: no later draw returns it or below. The
    /// counter must be seeded, or this would hide the tree's larger keys from the seed.
    pub fn inserted(&self, rowid: i64) {
        assert!(
            self.seeded(),
            "an explicit rowid noted on a counter not seeded"
        );
        self.raise(rowid);
    }

    /// Raises the counter to `rowid`, unless it holds a larger one.
    fn raise(&self, rowid: i64) {
        // Set first: a draw that reads `last` at `i64::MIN` reads it as a rowid once this is set.
        self.holds.store(true, Ordering::Release);
        self.last.fetch_max(rowid, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests;
