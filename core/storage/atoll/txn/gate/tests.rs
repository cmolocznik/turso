//! The write gate over real transactions: shared holders and exclusive takers waiting on one
//! another, PENDING kept until given up, the schema timestamp, every end releasing the gate, and
//! many threads writing and taking it exclusive at once.

use super::super::versioned::{VersionedTree, Versions, Write, WriteOp};
use super::super::{Abort, Commit, Hold, Isolation, Transactions, TxnConfig};
use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::{Mode, Tree};
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET, WRITEBACK_BYTES};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::page::SizeClass;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::Txn;
use crate::sync::atomic::AtomicUsize;
use crate::sync::Arc;

struct Db {
    io: Arc<dyn IO>,
    db: Atoll,
    versions: Versions,
    tree: VersionedTree,
}

fn open(slots: usize) -> Db {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let config = AtollConfig {
        log: LogConfig {
            segment_bytes: 1 << 20,
            buffer_bytes: 256 << 10,
            chunk_bytes: 16 << 10,
            writes_in_flight: 3,
            direct: true,
        },
        store: StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        buffer: BufferConfig {
            budget: MIN_BUDGET,
            checkpoint_shards: 8,
            writeback_bytes: WRITEBACK_BYTES,
        },
        log_budget: Some(512 << 10),
        txn: TxnConfig { slots },
    };
    let db = Atoll::open(io.clone(), "/db/gate", config).unwrap();
    let root = drive(&io, || {
        Tree::create(&io, db.pool(), db.store(), SizeClass::new(0).unwrap())
    })
    .unwrap();
    let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), slots);
    let tree = VersionedTree::open(io.clone(), db.pool().clone(), db.store().clone(), root);
    db.transactions().require_gate();
    Db {
        io,
        db,
        versions,
        tree,
    }
}

impl Db {
    fn txns(&self) -> &Transactions {
        self.db.transactions()
    }

    fn begin(&self) -> Txn {
        let mut t = drive(&self.io, || self.txns().begin(Isolation::Snapshot)).unwrap();
        self.txns().statement(&mut t).unwrap();
        t
    }

    /// A transaction with a slot and no snapshot yet.
    fn begin_slot(&self) -> Txn {
        drive(&self.io, || {
            self.txns().begin_slot_in(Isolation::Snapshot, 0)
        })
        .unwrap()
    }

    fn shared(&self, txn: &mut Txn) -> (Taken, Completion) {
        let c = Completion::new_wait();
        (self.txns().take_shared(txn, &c).unwrap(), c)
    }

    fn exclusive(&self, txn: &mut Txn) -> (Taken, Completion) {
        let c = Completion::new_wait();
        (self.txns().take_exclusive(txn, &c).unwrap(), c)
    }

    fn put(&self, txn: &mut Txn, key: &str, value: &str) {
        let mut op = WriteOp::put(key.as_bytes(), value.as_bytes().to_vec(), Mode::Upsert);
        let w = drive(&self.io, || {
            self.tree.write(self.txns(), &self.versions, txn, &mut op)
        })
        .unwrap();
        assert!(matches!(w, Write::Done(_)), "{key}: {w:?}");
    }

    fn get(&self, txn: &mut Txn, key: &str) -> Option<String> {
        let mut op = crate::storage::atoll::txn::versioned::Get::new(key.as_bytes());
        drive(&self.io, || {
            self.tree.get(self.txns(), &self.versions, txn, &mut op)
        })
        .unwrap()
        .map(|v| String::from_utf8(v).unwrap())
    }

    fn commit(&self, txn: Txn) {
        let mut op = Commit::new(txn);
        drive(&self.io, || self.txns().commit(&mut op)).unwrap();
    }

    fn rollback(&self, txn: Txn) {
        let mut op = Abort::new(txn);
        drive(&self.io, || self.txns().rollback(&self.versions, &mut op)).unwrap();
    }

    /// Whether an exclusive take would hold the gate at once: nobody else holds or takes it.
    fn free(&self) -> bool {
        let mut t = self.begin();
        let (taken, _) = self.exclusive(&mut t);
        let free = taken == Taken::Done;
        self.txns().give_up(&mut t);
        self.txns().abort(t);
        free
    }
}

/// A shared holder keeps an exclusive taker draining until it commits, which wakes the taker;
/// a shared taker then waits behind the held take until its commit, which wakes it.
#[test]
fn shared_holders_and_exclusive_takers_wait_on_each_other_and_wake_each_other() {
    let db = open(4);
    let mut w = db.begin();
    assert_eq!(db.shared(&mut w).0, Taken::Done);
    db.put(&mut w, "k", "w");
    let mut e = db.begin_slot();
    let (taken, c) = db.exclusive(&mut e);
    assert_eq!(taken, Taken::Holders(vec![(w.slot(), w.start())]));
    assert!(matches!(e.hold(), Hold::Pending { .. }));
    // A writer's snapshot comes after the gate (DESIGN.md §17, §6): one taken behind the take
    // would conflict with what the exclusive taker commits.
    let mut late = db.begin_slot();
    let (behind, cl) = db.shared(&mut late);
    assert!(
        matches!(behind, Taken::Behind { owner, .. } if owner == e.slot()),
        "{behind:?}"
    );
    // The late taker's mark, set and cleared, woke the drain: it takes again, still behind the
    // holder.
    assert!(
        c.finished(),
        "the drain not woken by the mark a late taker cleared"
    );
    let (taken, c) = db.exclusive(&mut e);
    assert_eq!(taken, Taken::Holders(vec![(w.slot(), w.start())]));
    assert!(!c.finished());
    db.commit(w);
    assert!(c.finished(), "the drain not woken by the holder's commit");
    assert_eq!(db.exclusive(&mut e).0, Taken::Done);
    assert_eq!(
        db.get(&mut e, "k").as_deref(),
        Some("w"),
        "its snapshot after the drain"
    );
    db.put(&mut e, "k", "e");
    assert!(!cl.finished());
    db.commit(e);
    assert!(
        cl.finished(),
        "a shared taker behind the take not woken by its commit"
    );
    assert_eq!(db.shared(&mut late).0, Taken::Done);
    db.txns().take_snapshot(&mut late);
    db.put(&mut late, "k", "late");
    db.commit(late);
    assert!(db.free());
}

/// An upgrader's own shared hold is not waited for; another holder's is, and its rollback wakes
/// the upgrader.
#[test]
fn an_upgrader_drains_the_other_holders_but_not_itself() {
    let db = open(4);
    let mut up = db.begin();
    assert_eq!(db.shared(&mut up).0, Taken::Done);
    db.put(&mut up, "a", "up");
    let mut other = db.begin();
    assert_eq!(db.shared(&mut other).0, Taken::Done);
    db.put(&mut other, "b", "other");
    let (taken, c) = db.exclusive(&mut up);
    assert_eq!(taken, Taken::Holders(vec![(other.slot(), other.start())]));
    db.rollback(other);
    assert!(
        c.finished(),
        "the upgrader not woken by the other's rollback"
    );
    assert_eq!(db.exclusive(&mut up).0, Taken::Done);
    assert!(matches!(up.hold(), Hold::Exclusive { shared: true, .. }));
    db.commit(up);
    assert!(db.free());
}

/// A pending take that did not drain stays pending, holding its place: new shared takers wait
/// behind it until it is given up, which wakes them (R15). Its own taker cannot take the gate
/// shared under it, which would wait behind itself: that is refused by name.
#[test]
fn a_pending_take_holds_its_place_until_given_up() {
    let db = open(4);
    let mut w = db.begin();
    assert_eq!(db.shared(&mut w).0, Taken::Done);
    let mut e = db.begin();
    assert!(matches!(db.exclusive(&mut e).0, Taken::Holders(_)));
    let mut late = db.begin();
    let (behind, c) = db.shared(&mut late);
    assert!(matches!(behind, Taken::Behind { .. }));
    assert!(
        matches!(db.exclusive(&mut e).0, Taken::Holders(_)),
        "still pending"
    );
    let err = db
        .txns()
        .take_shared(&mut e, &Completion::new_wait())
        .unwrap_err();
    assert!(err.to_string().contains("own pending exclusive"), "{err}");
    db.txns().give_up(&mut e);
    assert_eq!(e.hold(), Hold::None);
    assert!(c.finished(), "a give-up woke nobody");
    assert_eq!(db.shared(&mut late).0, Taken::Done);
    db.txns().abort(e);
    db.txns().abort(late);
    db.commit(w);
    assert!(db.free());
}

/// A commit that wrote the schema under an exclusive hold fails every shared take and exclusive
/// take of a transaction whose snapshot is older; a transaction begun after it takes the gate.
#[test]
fn a_schema_write_fails_older_snapshots_at_their_take() {
    let db = open(4);
    let mut old = db.begin();
    let mut old_ddl = db.begin();
    let mut e = db.begin_slot();
    assert_eq!(db.exclusive(&mut e).0, Taken::Done);
    db.put(&mut e, "schema", "v2");
    e.wrote_schema();
    db.commit(e);
    let c = Completion::new_wait();
    assert!(matches!(
        db.txns().take_shared(&mut old, &c),
        Err(crate::LimboError::SchemaConflict)
    ));
    assert_eq!(old.hold(), Hold::None);
    assert!(matches!(
        db.txns().take_exclusive(&mut old_ddl, &c),
        Err(crate::LimboError::SchemaConflict)
    ));
    db.txns().abort(old);
    db.rollback(old_ddl);
    let mut fresh = db.begin();
    assert_eq!(db.shared(&mut fresh).0, Taken::Done);
    db.commit(fresh);
    assert!(db.free());
}

/// Every end releases what the transaction holds: a commit with no writes and one of an exclusive
/// holder, an abort, a rollback, and the abort of a pending take.
#[test]
fn every_end_releases_the_gate() {
    let db = open(4);
    let mut t = db.begin();
    assert_eq!(db.shared(&mut t).0, Taken::Done);
    db.commit(t);
    assert!(db.free(), "a commit with no writes");
    let mut t = db.begin();
    assert_eq!(db.exclusive(&mut t).0, Taken::Done);
    db.commit(t);
    assert!(db.free(), "a commit of an exclusive holder");
    let mut t = db.begin();
    assert_eq!(db.shared(&mut t).0, Taken::Done);
    db.txns().abort(t);
    assert!(db.free(), "an abort");
    let mut t = db.begin();
    assert_eq!(db.exclusive(&mut t).0, Taken::Done);
    db.put(&mut t, "k", "v");
    db.rollback(t);
    assert!(db.free(), "a rollback of an exclusive holder");
    let mut w = db.begin();
    assert_eq!(db.shared(&mut w).0, Taken::Done);
    let mut p = db.begin();
    assert!(matches!(db.exclusive(&mut p).0, Taken::Holders(_)));
    db.txns().abort(p);
    db.commit(w);
    assert!(db.free(), "the abort of a pending take");
}

/// A gated database refuses the commit of a transaction that wrote without the gate.
#[test]
#[should_panic(expected = "without the write gate")]
fn a_write_without_the_gate_is_refused_at_commit() {
    let db = open(4);
    let mut t = db.begin();
    db.put(&mut t, "k", "v");
    db.commit(t);
}

/// Writers and exclusive takers on many threads, each waiting on what the gate names and taking
/// again once woken: no writer is ever inside while an exclusive holder is, every thread
/// finishes, and the gate is left free.
fn writers_and_exclusive_takers_all_finish(threads: usize, rounds: usize) -> usize {
    let db = Arc::new(open(threads + 1));
    let inside = Arc::new(AtomicUsize::new(0));
    let waits = Arc::new(AtomicUsize::new(0));
    let park = |c: &Completion, waits: &AtomicUsize| {
        waits.fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !c.finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "a gate wait never woken"
            );
            crate::thread::yield_now();
        }
    };
    let workers: Vec<_> = (0..threads)
        .map(|w| {
            let (db, inside, waits) = (db.clone(), inside.clone(), waits.clone());
            crate::thread::spawn(move || {
                let exclusive = w % 6 == 0;
                for r in 0..rounds {
                    let mut t = db.begin_slot();
                    loop {
                        let (taken, c) = if exclusive {
                            db.exclusive(&mut t)
                        } else {
                            db.shared(&mut t)
                        };
                        match taken {
                            Taken::Done => break,
                            Taken::Behind { .. } | Taken::Holders(_) => park(&c, &waits),
                            Taken::Lost => panic!("a take lost with no victim"),
                        }
                    }
                    if !t.snapped() {
                        db.txns().take_snapshot(&mut t);
                    }
                    db.txns().statement(&mut t).unwrap();
                    if exclusive {
                        assert_eq!(
                            inside.load(crate::sync::atomic::Ordering::SeqCst),
                            0,
                            "a writer inside the held gate"
                        );
                        db.put(&mut t, "schema", &format!("{w}-{r}"));
                        t.wrote_schema();
                        db.commit(t);
                    } else {
                        inside.fetch_add(1, crate::sync::atomic::Ordering::SeqCst);
                        db.put(&mut t, &format!("w{w}"), &format!("{r}"));
                        inside.fetch_sub(1, crate::sync::atomic::Ordering::SeqCst);
                        if r % 4 == 3 {
                            db.rollback(t);
                        } else {
                            db.commit(t);
                        }
                    }
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    assert!(db.free(), "the gate left taken");
    waits.load(crate::sync::atomic::Ordering::Relaxed)
}

#[test]
fn writers_and_exclusive_takers_all_finish_on_twelve_threads() {
    let waits = writers_and_exclusive_takers_all_finish(12, 60);
    println!("{waits} gate waits");
    assert!(waits > 0, "no take waited");
}

#[cfg(shuttle)]
#[test]
fn writers_and_exclusive_takers_all_finish_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || {
            writers_and_exclusive_takers_all_finish(3, 2);
        },
        300,
    );
}
