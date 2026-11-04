//! Per-tree stamps raised by the commits that write their trees, before the write gate is free,
//! and read by a DDL whose snapshot is older once it holds the gate.

use super::*;
use crate::io::{Completion, MemoryIO, IO};
use crate::storage::atoll::btree::Mode;
use crate::storage::atoll::page::SizeClass;
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::sql::AtollDb;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::versioned::{VersionedTree, Write, WriteOp};
use crate::storage::atoll::txn::{Abort, Commit, Isolation, Taken, Txn};
use crate::sync::atomic::AtomicUsize;

struct Db {
    io: Arc<dyn IO>,
    db: Arc<AtollDb>,
}

impl Db {
    fn open() -> Self {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Arc::new(AtollDb::open(io.clone(), "/db/trees", config()).unwrap());
        Self { io, db }
    }

    fn begin(&self) -> Txn {
        let txns = self.db.atoll().transactions();
        let mut t = drive(&self.io, || txns.begin(Isolation::Snapshot)).unwrap();
        txns.statement(&mut t).unwrap();
        t
    }

    fn tree(&self) -> VersionedTree {
        let a = self.db.atoll();
        let root = drive(&self.io, || {
            crate::storage::atoll::btree::Tree::create(
                &self.io,
                a.pool(),
                a.store(),
                SizeClass::new(0).unwrap(),
            )
        })
        .unwrap();
        VersionedTree::open(self.io.clone(), a.pool().clone(), a.store().clone(), root)
    }

    fn write(&self, txn: &mut Txn, tree: &VersionedTree) {
        let txns = self.db.atoll().transactions();
        let c = Completion::new_wait();
        assert_eq!(txns.take_shared(txn, &c).unwrap(), Taken::Done);
        let mut op = WriteOp::put(b"k", b"v".to_vec(), Mode::Upsert);
        let w = drive(&self.io, || {
            tree.write(txns, self.db.versions(), txn, &mut op)
        })
        .unwrap();
        assert!(matches!(w, Write::Done(_)));
    }

    /// Commits `txn`, raising `trees`' stamps in its published hook; `check` runs in the hook too.
    fn commit(&self, txn: Txn, trees: &[&VersionedTree], check: impl FnOnce(Ts) + Send + 'static) {
        let stamps: Vec<Stamp> = trees
            .iter()
            .map(|t| self.db.stamps().stamp(t.root()))
            .collect();
        let mut op = Commit::new(txn).on_published(move |ts| {
            TreeStamps::raise(&stamps, ts);
            check(ts);
        });
        drive(&self.io, || self.db.atoll().transactions().commit(&mut op)).unwrap();
    }
}

/// A commit that wrote raises the stamps of the trees it wrote, in its hook, while it still holds
/// the gate; a read-only commit and a rollback run no hook.
#[test]
fn a_commit_that_wrote_raises_its_trees_stamps_while_it_holds_the_gate() {
    let db = Db::open();
    let (x, y) = (db.tree(), db.tree());
    let txns = db.db.atoll().transactions();
    let before = db.begin();
    let mut w = db.begin();
    db.write(&mut w, &x);
    // Another transaction tries the gate from inside the hook: the committer still holds it.
    let other = Arc::new(std::sync::Mutex::new(Some(db.begin())));
    let calls = Arc::new(AtomicUsize::new(0));
    let (held, counted, dbi) = (other.clone(), calls.clone(), db.db.clone());
    db.commit(w, &[&x], move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        let txns = dbi.atoll().transactions();
        let mut guard = held.lock().unwrap();
        let t = guard.as_mut().unwrap();
        let c = Completion::new_wait();
        let taken = txns.take_exclusive(t, &c).unwrap();
        assert!(
            matches!(taken, Taken::Holders(_)),
            "the gate free before the hook: {taken:?}"
        );
        txns.give_up(t);
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(db.db.stamps().written_since(x.root(), before.snapshot()));
    assert!(!db.db.stamps().written_since(y.root(), before.snapshot()));
    txns.abort(other.lock().unwrap().take().unwrap());
    let ran = Arc::new(AtomicUsize::new(0));
    let r = ran.clone();
    db.commit(db.begin(), &[&y], move |_| {
        r.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(
        ran.load(Ordering::SeqCst),
        0,
        "a read-only commit ran its hook"
    );
    let mut rb = db.begin();
    db.write(&mut rb, &y);
    let mut abort = Abort::new(rb);
    drive(&db.io, || txns.rollback(db.db.versions(), &mut abort)).unwrap();
    assert!(!db.db.stamps().written_since(y.root(), before.snapshot()));
    txns.abort(before);
}

/// A DDL whose snapshot is older than a commit that wrote a tree sees the tree's stamp past its
/// snapshot once it holds the gate exclusive; one begun after the commit does not.
#[test]
fn a_ddl_with_an_older_snapshot_sees_a_commit_it_missed_once_it_holds_the_gate() {
    let db = Db::open();
    let x = db.tree();
    let mut ddl = db.begin();
    let mut w = db.begin();
    db.write(&mut w, &x);
    let txns = db.db.atoll().transactions();
    let c = Completion::new_wait();
    assert!(matches!(
        txns.take_exclusive(&mut ddl, &c).unwrap(),
        Taken::Holders(_)
    ));
    db.commit(w, &[&x], |_| {});
    assert!(c.finished());
    assert_eq!(txns.take_exclusive(&mut ddl, &c).unwrap(), Taken::Done);
    assert!(
        db.db.stamps().written_since(x.root(), ddl.snapshot()),
        "BusySnapshot"
    );
    txns.abort(ddl);
    let mut later = db.begin();
    let c = Completion::new_wait();
    assert_eq!(txns.take_exclusive(&mut later, &c).unwrap(), Taken::Done);
    assert!(!db.db.stamps().written_since(x.root(), later.snapshot()));
    txns.abort(later);
}

/// A tree's stamp is forgotten once the tree drains, so the map holds the live trees alone.
#[test]
fn a_drained_tree_forgets_its_stamp() {
    let db = Db::open();
    let txns = db.db.atoll().transactions();
    let mut t = db.begin();
    let root = drive(&db.io, || {
        db.db.create_tree(&mut t, SizeClass::new(0).unwrap())
    })
    .unwrap();
    db.db.stamps().stamp(root);
    assert_eq!(db.db.stamps().len(), 1);
    let mut abort = Abort::new(t);
    drive(&db.io, || txns.rollback(db.db.versions(), &mut abort)).unwrap();
    db.db.abandoned(abort.abandoned());
    drive(&db.io, || db.db.drain()).unwrap();
    assert!(db.db.stamps().is_empty(), "a drained tree's stamp kept");
}
