use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::Mode;
use crate::storage::atoll::page::SizeClass;
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::sql::AtollDb;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::versioned::{VersionedTree, Write, WriteOp};
use crate::storage::atoll::txn::{Commit, Isolation, Txn};

const BASE: &str = "/db/rowid";

struct Db {
    io: Arc<dyn IO>,
    db: AtollDb,
    tree: VersionedTree,
}

fn open() -> Db {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
    let a = db.atoll();
    let root = drive(&io, || {
        Tree::create(&io, a.pool(), a.store(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let tree = VersionedTree::open(io.clone(), a.pool().clone(), a.store().clone(), root);
    Db { io, db, tree }
}

impl Db {
    fn begin(&self) -> Txn {
        let txns = self.db.atoll().transactions();
        let mut t = drive(&self.io, || txns.begin(Isolation::Snapshot)).unwrap();
        crate::storage::atoll::testing::share(txns, &mut t);
        txns.statement(&mut t).unwrap();
        t
    }

    fn write(&self, txn: &mut Txn, rowid: i64, value: Option<&str>) {
        let k = key::rowid(rowid);
        let mut op = match value {
            Some(v) => WriteOp::put(&k, v.as_bytes().to_vec(), Mode::Upsert),
            None => WriteOp::delete(&k),
        };
        let txns = self.db.atoll().transactions();
        let w = drive(&self.io, || {
            self.tree.write(txns, self.db.versions(), txn, &mut op)
        })
        .unwrap();
        assert!(matches!(w, Write::Done(_)), "{rowid}: {w:?}");
    }

    fn commit(&self, txn: Txn) {
        let txns = self.db.atoll().transactions();
        let mut op = Commit::new(txn);
        drive(&self.io, || txns.commit(&mut op)).unwrap();
    }

    fn seeded(&self) -> Arc<Counter> {
        let counter = self.db.rowids().counter(self.tree.root());
        let mut op = Seed::default();
        drive(&self.io, || counter.seed(self.tree.tree(), &mut op)).unwrap();
        counter
    }
}

/// A counter seeds from the largest key in the leaf, whatever its version: a committed row, a
/// tombstone committed since, an open transaction's insert. So it draws above them all.
#[test]
fn a_counter_seeds_from_the_largest_key_whatever_its_version() {
    let db = open();
    let mut t = db.begin();
    for r in 1..=10 {
        db.write(&mut t, r, Some("v"));
    }
    db.write(&mut t, 60, Some("v"));
    db.commit(t);
    let mut t = db.begin();
    db.write(&mut t, 60, None);
    db.commit(t);
    let mut open = db.begin();
    db.write(&mut open, 50, Some("open"));
    let counter = db.seeded();
    assert_eq!(counter.draw(), Some(61), "above the tombstone");
    // A second seed changes nothing.
    let again = db.seeded();
    assert!(Arc::ptr_eq(&counter, &again));
    assert_eq!(again.draw(), Some(62));
    db.commit(open);
}

/// An empty tree's first rowid is 1, a tree of negative rowids draws one above its largest, an
/// explicit rowid raises the counter, and once i64::MAX is drawn nothing more is.
#[test]
fn draws_start_at_1_follow_the_largest_rise_past_explicit_rowids_and_end_at_max() {
    let db = open();
    let counter = db.seeded();
    assert_eq!(counter.draw(), Some(1));
    counter.inserted(-7);
    assert_eq!(
        counter.draw(),
        Some(2),
        "a lower explicit rowid changes nothing"
    );
    counter.inserted(100);
    assert_eq!(counter.draw(), Some(101));
    counter.inserted(i64::MAX - 1);
    assert_eq!(counter.draw(), Some(i64::MAX));
    assert_eq!(counter.draw(), None);
    assert_eq!(counter.draw(), None);
    // A tree whose rows are all negative.
    let other = open();
    let mut t = other.begin();
    for r in [-9, -5] {
        other.write(&mut t, r, Some("v"));
    }
    other.commit(t);
    assert_eq!(other.seeded().draw(), Some(-4));
}

/// A counter seeded from an empty tree holds no rowid: an explicit rowid raised past first, however
/// low, is its largest, so it draws one above it, as SQLite's next rowid after a largest of -1 is
/// 0; `i64::MIN` raised past is a rowid too.
#[test]
fn an_empty_trees_counter_draws_one_above_an_explicit_rowid_however_low() {
    let db = open();
    let counter = db.seeded();
    counter.inserted(-1);
    assert_eq!(counter.draw(), Some(0));
    assert_eq!(counter.draw(), Some(1));
    let other = open();
    let counter = other.seeded();
    counter.inserted(i64::MIN);
    assert_eq!(counter.draw(), Some(i64::MIN + 1));
}

/// Twelve threads draw and raise at once: every rowid drawn is drawn once, and each is above every
/// explicit rowid raised before it was drawn by the same thread.
fn draws_from_many_threads_never_repeat(threads: usize, each: usize) {
    let counter = Arc::new(Counter {
        last: AtomicI64::new(i64::MIN),
        holds: AtomicBool::new(false),
        seeded: AtomicBool::new(false),
    });
    // Seeded by two threads at once, from different largest keys: the larger stands.
    let seeders: Vec<_> = [5i64, 9]
        .into_iter()
        .map(|largest| {
            let c = counter.clone();
            crate::thread::spawn(move || {
                c.raise(largest);
                c.seeded.store(true, Ordering::Release);
            })
        })
        .collect();
    for s in seeders {
        s.join().unwrap();
    }
    let drawers: Vec<_> = (0..threads)
        .map(|t| {
            let c = counter.clone();
            crate::thread::spawn(move || {
                let mut got = Vec::new();
                for i in 0..each {
                    if i % 7 == 3 {
                        let explicit = (t * each + i) as i64 * 3;
                        c.inserted(explicit);
                        let next = c.draw().unwrap();
                        assert!(next > explicit, "{next} drawn after {explicit} was raised");
                        got.push(next);
                    } else {
                        got.push(c.draw().unwrap());
                    }
                }
                got
            })
        })
        .collect();
    let mut all: Vec<i64> = drawers
        .into_iter()
        .flat_map(|d| d.join().unwrap())
        .collect();
    let n = all.len();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), n, "a rowid drawn twice");
    assert!(all[0] > 9, "a draw at or below the larger seed: {}", all[0]);
}

#[test]
fn draws_from_twelve_threads_never_repeat() {
    draws_from_many_threads_never_repeat(12, 5000);
}

#[cfg(shuttle)]
#[test]
fn draws_never_repeat_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(|| draws_from_many_threads_never_repeat(3, 6), 1000);
}
