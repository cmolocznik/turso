//! Trees created, dropped and cleared in transactions, rolled back, committed and crashed: every
//! tree is either named by the catalog, on a list, or free.

use super::*;
use crate::io::crash::CrashIO;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::{value, Check};
use crate::storage::atoll::page::CLASS_COUNT;
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::sql::catalog::{key, Layout};
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::rollback::Rollback;
use crate::storage::atoll::txn::versioned::VersionedTree;
use crate::storage::atoll::txn::{Abort, Commit, Isolation};
use crate::sync::Arc;
use std::collections::BTreeSet;

const BASE: &str = "/db/lifecycle";

fn run<T>(io: &Arc<dyn IO>, op: impl FnMut() -> Result<IOResult<T>>) -> T {
    drive(io, op).unwrap()
}

struct Db {
    io: Arc<dyn IO>,
    db: AtollDb,
}

impl Db {
    fn open(io: Arc<dyn IO>) -> Self {
        let db = AtollDb::open(io.clone(), BASE, config()).unwrap();
        Self { io, db }
    }

    fn begin(&self) -> Txn {
        let txns = self.db.atoll().transactions();
        let mut t = run(&self.io, || txns.begin(Isolation::Snapshot));
        txns.statement(&mut t).unwrap();
        t
    }

    fn statement(&self, txn: &mut Txn) {
        self.db.atoll().transactions().statement(txn).unwrap();
    }

    fn tree(&self, root: PageId) -> VersionedTree {
        let a = self.db.atoll();
        VersionedTree::open(self.io.clone(), a.pool().clone(), a.store().clone(), root)
    }

    fn put(&self, txn: &mut Txn, root: PageId, k: &str, v: &str) {
        let mut op = WriteOp::put(k.as_bytes(), v.as_bytes().to_vec(), Mode::Upsert);
        let txns = self.db.atoll().transactions();
        let tree = self.tree(root);
        let w = run(&self.io, || {
            tree.write(txns, self.db.versions(), txn, &mut op)
        });
        assert!(matches!(w, Write::Done(_)), "{k}: {w:?}");
    }

    fn get(&self, txn: &mut Txn, root: PageId, k: &str) -> Option<String> {
        let mut op = Get::new(k.as_bytes());
        let txns = self.db.atoll().transactions();
        let tree = self.tree(root);
        run(&self.io, || {
            tree.get(txns, self.db.versions(), txn, &mut op)
        })
        .map(|v| String::from_utf8(v).unwrap())
    }

    /// A table `id` created in `txn`'s current statement, with `rows` rows of long values, so
    /// some go out of line.
    fn table(&self, txn: &mut Txn, id: u64, rows: usize) -> PageId {
        let root = run(&self.io, || {
            self.db.create_tree(txn, SizeClass::new(0).unwrap())
        });
        let entry = Entry {
            root,
            layout: Layout::Table,
            encoding: 0,
            created_by: 0,
        };
        let mut op = Catalog::put_op(id, &entry, Mode::Insert);
        let txns = self.db.atoll().transactions();
        let w = run(&self.io, || {
            self.db
                .catalog()
                .write(txns, self.db.versions(), txn, &mut op)
        });
        assert!(matches!(w, Write::Done(Outcome::Inserted)), "{w:?}");
        for r in 0..rows {
            let v = format!(
                "{id}-{r}-{}",
                "v".repeat(if r % 3 == 0 { 1500 } else { 20 })
            );
            self.put(txn, root, &format!("r{r:04}"), &v);
        }
        root
    }

    /// Tree `id`'s catalog row as `txn` reads it.
    fn row_in(&self, txn: &mut Txn, id: u64) -> Option<Entry> {
        let mut get = Get::new(&key(id));
        let txns = self.db.atoll().transactions();
        run(&self.io, || {
            self.db
                .catalog()
                .get(txns, self.db.versions(), txn, &mut get)
        })
    }

    /// Tree `id`'s catalog row as a transaction begun now reads it.
    fn row(&self, id: u64) -> Option<Entry> {
        let mut t = self.begin();
        let row = self.row_in(&mut t, id);
        self.db.atoll().transactions().abort(t);
        row
    }

    fn destroy(&self, txn: &mut Txn, id: u64, clear: bool) {
        let mut op = Destroy::new(id);
        let w = run(&self.io, || {
            if clear {
                self.db.clear(txn, &mut op)
            } else {
                self.db.destroy(txn, &mut op)
            }
        });
        assert_eq!(w, Write::Done(Outcome::Updated));
    }

    fn commit(&self, txn: Txn) -> Vec<PageId> {
        let mut op = Commit::new(txn);
        run(&self.io, || self.db.atoll().transactions().commit(&mut op));
        op.abandoned().to_vec()
    }

    fn rollback(&self, txn: Txn) -> Vec<PageId> {
        let mut op = Abort::new(txn);
        let txns = self.db.atoll().transactions();
        run(&self.io, || txns.rollback(self.db.versions(), &mut op));
        op.abandoned().to_vec()
    }

    fn rollback_to(&self, txn: &mut Txn, to: u32) {
        let mut op = Rollback::after(to);
        let txns = self.db.atoll().transactions();
        run(&self.io, || {
            txns.rollback_to(self.db.versions(), txn, &mut op)
        });
    }

    fn hand_over(&self, roots: &[PageId]) {
        self.db.abandoned(roots);
        run(&self.io, || self.db.drain());
    }

    /// The roots on a system list, read plainly.
    fn listed(&self, system: System) -> Vec<PageId> {
        let list = self.db.open_plain(system.root());
        let mut cursor = Cursor::new();
        let mut out = Vec::new();
        let mut more = run(&self.io, || list.seek(&mut cursor, &[], Seek::Ge));
        while more {
            let k = cursor.key().unwrap();
            out.push(PageId::from_raw(u64::from_be_bytes(k.try_into().unwrap())));
            more = run(&self.io, || list.next(&mut cursor));
        }
        out
    }

    /// Every page of the whole versioned tree rooted at `root`.
    fn pages(&self, root: PageId) -> BTreeSet<PageId> {
        self.pages_of(self.tree(root).tree())
    }

    fn pages_of(&self, tree: &Tree) -> BTreeSet<PageId> {
        let mut check = Check::default();
        let stats = run(&self.io, || tree.check(&mut check));
        let mut out: BTreeSet<PageId> = stats.pages.iter().copied().collect();
        for &v in &stats.values {
            let pages = run(&self.io, || {
                value::pages(self.db.atoll().pool(), v, &|| true)
            });
            out.extend(pages.unwrap_or_else(|_| panic!("{v:?} lost its pages")));
        }
        out
    }

    fn free(&self, pid: PageId) -> bool {
        run(&self.io, || self.db.atoll().store().entry(pid)) == FREE
            || self.db.atoll().store().orphans().contains(&pid)
    }

    /// Checks the pages allocated are exactly the system trees', those of the trees the catalog
    /// names among `ids` and of every tree on a list, and those handed to the checkpointer to
    /// free: none leaked, none freed that is in use.
    fn check_whole(&self, ids: &[u64], label: &str) {
        let mut expected = BTreeSet::new();
        for system in System::ALL {
            expected.extend(match system {
                System::Abandoned | System::Sequences => {
                    self.pages_of(&self.db.open_plain(system.root()))
                }
                _ => self.pages(system.root()),
            });
        }
        for &id in ids {
            if let Some(row) = self.row(id) {
                expected.extend(self.pages(row.root));
            }
        }
        for system in [System::Abandoned, System::Dropped] {
            for root in self.listed(system) {
                expected.extend(self.pages(root));
            }
        }
        expected.extend(self.db.atoll().store().orphans());
        let mut allocated = BTreeSet::new();
        for c in 0..CLASS_COUNT {
            let (mut number, mut free_run) = (0, 0);
            // A run this long free is past every page: numbers are taken lowest first.
            while free_run < 512 {
                let pid = PageId::new(SizeClass::new(c).unwrap(), number);
                if run(&self.io, || self.db.atoll().store().entry(pid)) != FREE {
                    allocated.insert(pid);
                    free_run = 0;
                } else {
                    free_run += 1;
                }
                number += 1;
            }
        }
        let leaked: Vec<_> = allocated.difference(&expected).collect();
        let lost: Vec<_> = expected.difference(&allocated).collect();
        assert!(
            leaked.is_empty() && lost.is_empty(),
            "{label}: allocated and unreachable {leaked:?}; reachable and free {lost:?}"
        );
    }
}

/// A statement rollback and a whole rollback each put the trees they undo on the abandoned list;
/// handed over once the transaction ends, they drain, while a tree whose creation stands keeps
/// its rows.
#[test]
fn undone_creations_drain_once_handed_over_and_kept_ones_stay() {
    let db = Db::open(Arc::new(MemoryIO::new()));
    let mut t = db.begin();
    let kept = db.table(&mut t, 10, 40);
    db.statement(&mut t);
    let undone = db.table(&mut t, 11, 40);
    let pages = db.pages(undone);
    db.rollback_to(&mut t, 1);
    assert_eq!(t.abandoned(), &[undone]);
    assert!(db.row_in(&mut t, 11).is_none());
    let abandoned = db.commit(t);
    assert_eq!(abandoned, vec![undone]);
    assert_eq!(db.listed(System::Abandoned), vec![undone]);
    db.hand_over(&abandoned);
    assert!(db.listed(System::Abandoned).is_empty());
    assert!(pages.iter().all(|&p| db.free(p)), "{undone:?} not freed");
    assert_eq!(db.row(10).map(|r| r.root), Some(kept));
    let mut t = db.begin();
    assert!(db.get(&mut t, kept, "r0003").unwrap().starts_with("10-3-"));
    let whole = db.table(&mut t, 12, 40);
    let abandoned = db.rollback(t);
    assert_eq!(abandoned, vec![whole]);
    db.hand_over(&abandoned);
    assert!(db.listed(System::Abandoned).is_empty());
    db.check_whole(&[10, 11, 12], "after both");
}

/// A drop lists the root in its transaction, and its rollback unlists it with the catalog row
/// back; a committed drop leaves the tree to a snapshot taken before it, and the next open drains
/// it.
#[test]
fn a_drop_lists_the_root_its_rollback_unlists_it_and_an_open_drains_it() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Db::open(io.clone());
    let mut t = db.begin();
    let root = db.table(&mut t, 10, 40);
    db.commit(t);
    let pages = db.pages(root);
    let mut t = db.begin();
    db.destroy(&mut t, 10, false);
    assert!(db.row_in(&mut t, 10).is_none());
    assert!(db.listed(System::Dropped).contains(&root));
    assert!(db.rollback(t).is_empty());
    assert_eq!(db.row(10).map(|r| r.root), Some(root));
    assert!(
        db.listed(System::Dropped).is_empty(),
        "the rollback unlists it"
    );
    let mut before = db.begin();
    let mut t = db.begin();
    db.destroy(&mut t, 10, false);
    db.commit(t);
    assert!(db.row(10).is_none());
    assert_eq!(db.row_in(&mut before, 10).map(|r| r.root), Some(root));
    assert!(db
        .get(&mut before, root, "r0009")
        .unwrap()
        .starts_with("10-9-"));
    db.db.atoll().transactions().abort(before);
    assert!(pages.iter().all(|&p| !db.free(p)), "drained while running");
    drop(db);
    let db = Db::open(io);
    assert!(db.listed(System::Dropped).is_empty());
    assert!(
        pages.iter().all(|&p| db.free(p)),
        "{root:?} not drained at open"
    );
    db.check_whole(&[10], "reopened");
}

/// A clear points the row at a new, empty tree and lists the old root; rolled back, the row names
/// the old tree again and the new one drains; committed, the next open drains the old one.
#[test]
fn a_clear_points_the_row_at_a_new_tree_and_lists_the_old() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Db::open(io.clone());
    let mut t = db.begin();
    let old = db.table(&mut t, 10, 40);
    db.commit(t);
    let mut t = db.begin();
    db.destroy(&mut t, 10, true);
    let new = db.row_in(&mut t, 10).unwrap().root;
    assert_ne!(new, old);
    assert!(
        db.get(&mut t, new, "r0001").is_none(),
        "the new tree is empty"
    );
    let abandoned = db.rollback(t);
    assert_eq!(abandoned, vec![new]);
    assert_eq!(db.row(10).map(|r| r.root), Some(old));
    db.hand_over(&abandoned);
    assert!(db.free(new));
    let mut t = db.begin();
    db.destroy(&mut t, 10, true);
    let new = db.row_in(&mut t, 10).unwrap().root;
    db.put(&mut t, new, "fresh", "row");
    db.commit(t);
    let pages = db.pages(old);
    drop(db);
    let db = Db::open(io);
    assert!(
        pages.iter().all(|&p| db.free(p)),
        "{old:?} not drained at open"
    );
    let row = db.row(10).unwrap();
    assert_eq!(row.root, new);
    let mut t = db.begin();
    assert_eq!(db.get(&mut t, new, "fresh").as_deref(), Some("row"));
    db.commit(t);
    db.check_whole(&[10], "reopened");
}

/// Crashed anywhere through creations, a drop and a clear rolled back, and a drop committed
/// after: every image opens to the trees committed, with every other tree drained, so nothing
/// leaks, and the drop's commit decides whether its tree is there.
#[test]
fn crashes_through_ddl_and_its_rollback_leave_every_tree_named_listed_or_free() {
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash_io.clone();
        let db = Db::open(io);
        let mut t = db.begin();
        let (ten, eleven) = (db.table(&mut t, 10, 30), db.table(&mut t, 11, 30));
        db.commit(t);
        run(&db.io, || {
            db.db.atoll().log().wait_durable(db.db.atoll().log().end())
        });
        crash_io.capture(|path| path.starts_with(&format!("{BASE}.log.")), 3, seed);
        let mut t = db.begin();
        db.table(&mut t, 12, 30);
        db.statement(&mut t);
        db.destroy(&mut t, 10, false);
        db.destroy(&mut t, 11, true);
        let new = db.row_in(&mut t, 11).unwrap().root;
        for r in 0..30 {
            db.put(&mut t, new, &format!("n{r:04}"), &"w".repeat(1200));
        }
        let abandoned = db.rollback(t);
        assert_eq!(abandoned.len(), 2);
        db.hand_over(&abandoned);
        let mut t = db.begin();
        db.destroy(&mut t, 11, false);
        db.commit(t);
        let mut images = crash_io.captured();
        images.push(crash_io.crash(seed));
        let label = format!("seed {seed}: {} images", images.len());
        let mut gone = 0;
        for (i, image) in images.into_iter().enumerate() {
            let db = Db::open(Arc::new(image));
            assert_eq!(db.row(10).map(|r| r.root), Some(ten), "{label}: image {i}");
            assert!(db.row(12).is_none(), "{label}: image {i}");
            match db.row(11) {
                Some(row) => assert_eq!(row.root, eleven, "{label}: image {i}"),
                None => gone += 1,
            }
            assert!(
                db.listed(System::Abandoned).is_empty(),
                "{label}: image {i}"
            );
            assert!(db.listed(System::Dropped).is_empty(), "{label}: image {i}");
            db.check_whole(&[10, 11, 12], &format!("{label}: image {i}"));
        }
        assert!(gone >= 1, "{label}: no image with the drop committed");
    }
}
