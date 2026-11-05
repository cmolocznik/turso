use super::*;
use crate::io::{Completion, MemoryIO};
use crate::storage::atoll::btree::value;
use crate::storage::atoll::btree::Seek;
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::{Kind, LogConfig};
use crate::storage::atoll::btree::node;
use crate::storage::atoll::page::{self, SizeClass};
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::shared::Bytes;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::{Commit, TxnConfig};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const BASE: &str = "/db/versioned";

struct Db {
    io: Arc<dyn IO>,
    db: Atoll,
    versions: Versions,
    tree: VersionedTree,
}

fn open() -> Db {
    open_with(4)
}

/// A database whose transactions have `slots` slots.
fn open_with(slots: usize) -> Db {
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
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
        },
        log_budget: Some(512 << 10),
        txn: TxnConfig { slots },
    };
    let db = Atoll::open(io.clone(), BASE, config).unwrap();
    let mut root = None;
    drive(&io, || {
        root = Some(
            match Tree::create(&io, db.pool(), db.store(), SizeClass::new(0).unwrap())? {
                IOResult::Done(r) => r,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            },
        );
        Ok(IOResult::Done(()))
    })
    .unwrap();
    let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), slots);
    let tree = VersionedTree::open(
        io.clone(),
        db.pool().clone(),
        db.store().clone(),
        root.unwrap(),
    );
    Db {
        io,
        db,
        versions,
        tree,
    }
}

fn run<T>(io: &Arc<dyn IO>, mut op: impl FnMut() -> Result<IOResult<T>>) -> T {
    let mut out = None;
    drive(io, || {
        out = Some(match op()? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        });
        Ok(IOResult::Done(()))
    })
    .unwrap();
    out.unwrap()
}

/// `s`, then `pad` more bytes.
fn padded(s: &str, pad: usize) -> String {
    format!("{s}{}", ".".repeat(pad))
}

/// Long enough to go out of line in the tree's 4 KiB nodes, in a page of its own.
const LARGE: usize = 12 << 10;

impl Db {
    fn txns(&self) -> &Transactions {
        self.db.transactions()
    }

    /// The pages of the out-of-line value `key`'s newest version holds; none if inline.
    fn pages_of(&self, key: &str) -> Vec<PageId> {
        let stored = run(&self.io, || self.tree.tree().stored(key.as_bytes())).expect("an entry");
        match stored.value {
            StoredValue::Inline(_) => Vec::new(),
            StoredValue::OutOfLine(r) => run(&self.io, || {
                Ok(match value::pages(self.db.pool(), r, &|| true)? {
                    IOResult::Done(Ok(pages)) => IOResult::Done(pages),
                    IOResult::Done(Err(_)) => panic!("{key}: its value's pages are gone"),
                    IOResult::IO(io) => IOResult::IO(io),
                })
            }),
        }
    }

    fn retired(&self, pid: PageId) -> bool {
        run(&self.io, || self.db.store().retired(pid))
    }

    /// Whether `pid` is free, or handed over to be freed.
    fn freed(&self, pid: PageId) -> bool {
        run(&self.io, || self.db.store().entry(pid)) == FREE
            || self.db.store().orphans().contains(&pid)
    }

    fn begin(&self, isolation: Isolation) -> Txn {
        let mut txn = run(&self.io, || self.txns().begin(isolation));
        self.txns().statement(&mut txn).unwrap();
        txn
    }

    fn get(&self, txn: &mut Txn, key: &str) -> Option<String> {
        self.get_bytes(txn, key)
            .map(|v| String::from_utf8(v).unwrap())
    }

    fn get_bytes(&self, txn: &mut Txn, key: &str) -> Option<Vec<u8>> {
        let mut op = Get::new(key.as_bytes());
        run(&self.io, || {
            self.tree.get(self.txns(), &self.versions, txn, &mut op)
        })
    }

    fn put(&self, txn: &mut Txn, key: &str, value: &str, mode: Mode) -> Write {
        self.put_bytes(txn, key, value.as_bytes().to_vec(), mode)
    }

    fn put_bytes(&self, txn: &mut Txn, key: &str, value: Vec<u8>, mode: Mode) -> Write {
        let mut op = WriteOp::put(key.as_bytes(), value, mode);
        run(&self.io, || {
            self.tree.write(self.txns(), &self.versions, txn, &mut op)
        })
    }

    fn delete(&self, txn: &mut Txn, key: &str) -> Write {
        let mut op = WriteOp::delete(key.as_bytes());
        run(&self.io, || {
            self.tree.write(self.txns(), &self.versions, txn, &mut op)
        })
    }

    fn commit(&self, txn: Txn) {
        let mut op = Commit::new(txn);
        run(&self.io, || self.txns().commit(&mut op));
    }
}

const DONE_INSERTED: Write = Write::Done(Outcome::Inserted);
const DONE_UPDATED: Write = Write::Done(Outcome::Updated);

#[test]
fn a_snapshot_reads_the_versions_committed_before_it_through_the_chain() {
    let db = open();
    let mut w = db.begin(Isolation::Snapshot);
    assert_eq!(db.put(&mut w, "k", "v1", Mode::Insert), DONE_INSERTED);
    db.commit(w);
    let mut old = db.begin(Isolation::Snapshot);
    let mut fresh = db.begin(Isolation::ReadCommitted);
    for (i, value) in ["v2", "v3"].into_iter().enumerate() {
        let mut w = db.begin(Isolation::Snapshot);
        assert_eq!(
            db.put(&mut w, "k", value, Mode::Update),
            DONE_UPDATED,
            "{i}"
        );
        db.commit(w);
    }
    let mut w = db.begin(Isolation::Snapshot);
    assert_eq!(db.delete(&mut w, "k"), DONE_UPDATED);
    // Not committed yet: every reader still reads v3 or older.
    db.txns().statement(&mut fresh).unwrap();
    assert_eq!(db.get(&mut fresh, "k").as_deref(), Some("v3"));
    db.commit(w);
    assert_eq!(
        db.get(&mut old, "k").as_deref(),
        Some("v1"),
        "two versions back"
    );
    assert_eq!(
        db.get(&mut fresh, "k").as_deref(),
        Some("v3"),
        "within a statement"
    );
    db.txns().statement(&mut fresh).unwrap();
    assert_eq!(db.get(&mut fresh, "k"), None, "deleted");
    let mut w = db.begin(Isolation::Snapshot);
    assert_eq!(db.get(&mut w, "k"), None);
    assert_eq!(db.put(&mut w, "k", "v4", Mode::Insert), DONE_INSERTED);
    db.commit(w);
    assert_eq!(db.get(&mut old, "k").as_deref(), Some("v1"));
    let mut later = db.begin(Isolation::Snapshot);
    assert_eq!(db.get(&mut later, "k").as_deref(), Some("v4"));
    // A key inserted after a snapshot does not exist for it, with no index entry to follow.
    let mut w = db.begin(Isolation::Snapshot);
    db.put(&mut w, "new", "x", Mode::Insert);
    db.commit(w);
    assert_eq!(db.get(&mut old, "new"), None);
}

#[test]
fn a_statement_reads_its_own_writes_as_soon_as_it_makes_them() {
    let db = open();
    let mut w = db.begin(Isolation::Snapshot);
    db.put(&mut w, "k", "s1", Mode::Insert);
    assert_eq!(
        db.get(&mut w, "k").as_deref(),
        Some("s1"),
        "its own statement's insert"
    );
    db.txns().statement(&mut w).unwrap();
    assert_eq!(db.get(&mut w, "k").as_deref(), Some("s1"));
    db.put(&mut w, "k", "s2a", Mode::Update);
    db.put(&mut w, "k", "s2b", Mode::Update);
    assert_eq!(
        db.get(&mut w, "k").as_deref(),
        Some("s2b"),
        "a row this statement changed twice reads as it last wrote it"
    );
    db.txns().statement(&mut w).unwrap();
    assert_eq!(db.get(&mut w, "k").as_deref(), Some("s2b"));
    let mut other = db.begin(Isolation::Snapshot);
    assert_eq!(
        db.get(&mut other, "k"),
        None,
        "open, to another transaction"
    );
    db.commit(w);
    let mut later = db.begin(Isolation::Snapshot);
    assert_eq!(db.get(&mut later, "k").as_deref(), Some("s2b"));
    assert_eq!(db.get(&mut other, "k"), None);
    // A row committed before, changed by two statements: the chain passes both to reach it.
    let mut w = db.begin(Isolation::Snapshot);
    db.put(&mut w, "k", "t1", Mode::Update);
    db.txns().statement(&mut w).unwrap();
    db.put(&mut w, "k", "t2", Mode::Update);
    assert_eq!(db.get(&mut later, "k").as_deref(), Some("s2b"));
    assert_eq!(db.get(&mut w, "k").as_deref(), Some("t2"));
}

#[test]
fn a_writer_waits_for_an_open_writer_and_conflicts_with_a_later_commit() {
    let db = open();
    let mut a = db.begin(Isolation::Snapshot);
    db.put(&mut a, "k", "a", Mode::Insert);
    db.commit(a);
    let mut snapshot = db.begin(Isolation::Snapshot);
    let mut committed = db.begin(Isolation::ReadCommitted);
    let mut b = db.begin(Isolation::Snapshot);
    assert_eq!(db.put(&mut b, "k", "b", Mode::Update), DONE_UPDATED);
    let blocker = Blocker {
        slot: b.slot(),
        start: b.start(),
    };
    assert_eq!(
        db.put(&mut snapshot, "k", "s", Mode::Update),
        Write::Wait(blocker)
    );
    assert_eq!(
        db.put(&mut committed, "k", "c", Mode::Update),
        Write::Wait(blocker)
    );
    db.commit(b);
    assert_eq!(
        db.put(&mut snapshot, "k", "s", Mode::Update),
        Write::Conflict,
        "first committer wins"
    );
    assert_eq!(db.put(&mut committed, "k", "c", Mode::Update), DONE_UPDATED);
    db.commit(committed);
    let mut later = db.begin(Isolation::Snapshot);
    assert_eq!(db.get(&mut later, "k").as_deref(), Some("c"));
}

#[test]
fn a_transactions_records_chain_and_carry_their_tree() {
    let db = open();
    let mut w = db.begin(Isolation::Snapshot);
    let mut groups = Vec::new();
    for key in ["a", "b", "c"] {
        db.put(&mut w, key, "v", Mode::Insert);
        groups.push(w.tail().unwrap());
    }
    db.txns().statement(&mut w).unwrap();
    db.put(&mut w, "a", "w", Mode::Update);
    groups.push(w.tail().unwrap());
    let start = w.start();
    db.commit(w);
    let mut reader = crate::storage::atoll::log::Log::reader(
        db.io.clone(),
        BASE,
        &LogConfig {
            segment_bytes: 1 << 20,
            buffer_bytes: 256 << 10,
            chunk_bytes: 16 << 10,
            writes_in_flight: 3,
            direct: true,
        },
        0,
    );
    let mut chain = Vec::new();
    while let Some(r) = reader.next_record().unwrap() {
        if r.header.txn == start {
            chain.push((r.header.prev, r.group));
        }
    }
    let mut prev = None;
    for (i, &(p, group)) in chain.iter().enumerate().take(4) {
        assert_eq!((p, group), (prev, groups[i].get()), "record {i}");
        prev = Some(groups[i]);
    }
    assert_eq!(chain.len(), 5, "four changes and the commit");
}

impl Db {
    fn rollback(&self, txn: Txn) {
        let mut op = crate::storage::atoll::txn::Abort::new(txn);
        run(&self.io, || self.txns().rollback(&self.versions, &mut op));
    }

    fn rollback_to(&self, txn: &mut Txn, to: Statement) {
        let mut op = crate::storage::atoll::txn::rollback::Rollback::after(to);
        run(&self.io, || {
            self.txns().rollback_to(&self.versions, txn, &mut op)
        });
    }

    /// `key` as a transaction begun now reads it in `tree`.
    fn read_in(&self, tree: &VersionedTree, key: &str) -> Option<String> {
        let mut txn = self.begin(Isolation::Snapshot);
        let mut op = Get::new(key.as_bytes());
        let v = run(&self.io, || {
            tree.get(self.txns(), &self.versions, &mut txn, &mut op)
        });
        self.commit(txn);
        v.map(|v| String::from_utf8(v).unwrap())
    }

    fn put_in(&self, tree: &VersionedTree, txn: &mut Txn, key: &str, value: &str, mode: Mode) {
        let mut op = WriteOp::put(key.as_bytes(), value.as_bytes().to_vec(), mode);
        run(&self.io, || {
            tree.write(self.txns(), &self.versions, txn, &mut op)
        });
    }

    /// The kinds and statements of `start`'s records, in log order.
    fn chain(&self, start: Ts) -> Vec<(Kind, Option<Statement>)> {
        use crate::storage::atoll::btree::record::{Change, Record};
        drive(&self.io, || self.db.log().wait_durable(self.db.log().end())).unwrap();
        let mut reader = crate::storage::atoll::log::Log::reader(
            self.io.clone(),
            BASE,
            &LogConfig {
                segment_bytes: 1 << 20,
                buffer_bytes: 256 << 10,
                chunk_bytes: 16 << 10,
                writes_in_flight: 3,
                direct: true,
            },
            0,
        );
        let mut out = Vec::new();
        while let Some(r) = reader.next_record().unwrap() {
            if r.header.txn != start {
                continue;
            }
            let statement = match Record::decode(r.header.kind, &r.payload).unwrap() {
                Some(Record {
                    change: Change::Restore { of, .. } | Change::Unput { of, .. },
                    ..
                }) => Some(of.statement),
                _ => None,
            };
            out.push((r.header.kind, statement));
        }
        out
    }
}

/// A second versioned tree, of a larger node class: a transaction writing both has a version
/// index of each class.
fn second_tree(db: &Db) -> VersionedTree {
    let root = run(&db.io, || {
        Tree::create(
            &db.io,
            db.db.pool(),
            db.db.store(),
            SizeClass::new(1).unwrap(),
        )
    });
    VersionedTree::open(
        db.io.clone(),
        db.db.pool().clone(),
        db.db.store().clone(),
        root,
    )
}

#[test]
fn a_rollback_puts_back_every_key_newest_statement_first_and_frees_its_slot() {
    use crate::storage::atoll::btree::record::{RESTORE, UNPUT};
    use crate::storage::atoll::txn::END;
    let db = open();
    let other = second_tree(&db);
    let mut setup = db.begin(Isolation::Snapshot);
    for k in ["a", "b", "c"] {
        db.put(&mut setup, k, &format!("{k}0"), Mode::Insert);
    }
    db.put_in(&other, &mut setup, "o", "o0", Mode::Insert);
    db.commit(setup);
    let mut t = db.begin(Isolation::Snapshot);
    db.put(&mut t, "a", "a1", Mode::Update);
    db.delete(&mut t, "b");
    db.put(&mut t, "x", "x1", Mode::Insert);
    db.put_in(&other, &mut t, "o", "o1", Mode::Update);
    db.txns().statement(&mut t).unwrap();
    db.put(&mut t, "a", "a2", Mode::Update);
    db.put(&mut t, "c", "c2", Mode::Update);
    db.put(&mut t, "x", "x2", Mode::Update);
    db.txns().statement(&mut t).unwrap();
    db.put_in(&other, &mut t, "o", "o3", Mode::Update);
    db.put_in(&other, &mut t, "p", "p3", Mode::Insert);
    let start = t.start();
    db.rollback(t);
    let read = |k: &str| db.read_in(&db.tree, k);
    assert_eq!(
        ["a", "b", "c", "x"].map(read),
        [
            Some("a0".into()),
            Some("b0".into()),
            Some("c0".into()),
            None
        ]
    );
    assert_eq!(
        ["o", "p"].map(|k| db.read_in(&other, k)),
        [Some("o0".into()), None]
    );
    // Nothing of it left in a leaf: a key it inserted has no entry at all.
    assert_eq!(run(&db.io, || db.tree.tree().stored(b"x")), None);
    assert_eq!(run(&db.io, || other.tree().stored(b"p")), None);
    // One compensation per key each statement changed, newest statement first across both
    // trees, then End.
    let chain = db.chain(start);
    let compensations: Vec<Statement> = chain.iter().filter_map(|(_, s)| *s).collect();
    assert_eq!(compensations, vec![3, 3, 2, 2, 2, 1, 1, 1, 1], "{chain:?}");
    let unputs = chain.iter().filter(|(k, _)| *k == UNPUT).count();
    let restores = chain.iter().filter(|(k, _)| *k == RESTORE).count();
    assert_eq!((restores, unputs), (7, 2), "x and p had no entry before");
    assert_eq!(chain.last().map(|(k, _)| *k), Some(END));
    // Its slot is free: all four can begin.
    let all: Vec<Txn> = (0..4).map(|_| db.begin(Isolation::Snapshot)).collect();
    for txn in all {
        db.commit(txn);
    }
}

#[test]
fn a_statement_that_changed_a_key_again_and_again_is_put_back_once_to_what_it_found() {
    let db = open();
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "k", "v0", Mode::Insert);
    db.commit(setup);
    let mut t = db.begin(Isolation::Snapshot);
    for v in ["v1", "v2", "v3"] {
        db.put(&mut t, "k", v, Mode::Update);
    }
    let start = t.start();
    db.rollback(t);
    assert_eq!(db.read_in(&db.tree, "k").as_deref(), Some("v0"));
    let compensations: Vec<Statement> = db.chain(start).iter().filter_map(|(_, s)| *s).collect();
    assert_eq!(compensations, vec![1]);
}

#[test]
fn a_rollback_to_a_statement_keeps_what_came_before_and_frees_a_key_it_put_back() {
    let db = open();
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "a", "a0", Mode::Insert);
    db.commit(setup);
    let mut t = db.begin(Isolation::Snapshot);
    db.put(&mut t, "a", "a1", Mode::Update);
    db.txns().statement(&mut t).unwrap();
    db.put(&mut t, "a", "a2", Mode::Update);
    db.put(&mut t, "n", "n2", Mode::Insert);
    // Changed twice by the statement rolled back: put back once, to statement 1's.
    db.put(&mut t, "a", "a2b", Mode::Update);
    db.rollback_to(&mut t, 1);
    assert!(t.command() > 2, "statement {} reused", t.command());
    assert_eq!(db.get(&mut t, "a").as_deref(), Some("a1"));
    assert_eq!(db.get(&mut t, "n"), None);
    // The key it put back to nothing is free for another transaction at once.
    let mut u = db.begin(Isolation::Snapshot);
    assert_eq!(db.put(&mut u, "n", "u", Mode::Insert), DONE_INSERTED);
    db.commit(u);
    db.put(&mut t, "a", "a3", Mode::Update);
    db.commit(t);
    assert_eq!(db.read_in(&db.tree, "a").as_deref(), Some("a3"));
    assert_eq!(db.read_in(&db.tree, "n").as_deref(), Some("u"));
}

#[test]
fn a_rollback_leaves_a_key_it_waited_on_to_the_transaction_that_wrote_it() {
    let db = open();
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "k", "k0", Mode::Insert);
    db.commit(setup);
    let mut u = db.begin(Isolation::Snapshot);
    db.put(&mut u, "k", "u1", Mode::Update);
    let mut t = db.begin(Isolation::Snapshot);
    assert!(matches!(
        db.put(&mut t, "k", "t1", Mode::Update),
        Write::Wait(_)
    ));
    // The entry a write appends before its leaf write, left as when the leaf changed between the
    // two and the write then found that change open: naming what `t` would have replaced, with no
    // version of `t` in the leaf.
    let committed = Stored {
        head: Version {
            slot: 0,
            ts: 1,
            command: 1,
            updates: 0,
            flags: 0,
            prev: None,
        }
        .encode()
        .to_vec(),
        value: StoredValue::Inline(b"k0".to_vec()),
    };
    run(&db.io, || {
        db.versions.arenas().append(
            &mut t,
            db.tree.root(),
            b"k",
            &encode_entry(Some(&committed)),
        )
    });
    db.rollback(t);
    db.commit(u);
    assert_eq!(db.read_in(&db.tree, "k").as_deref(), Some("u1"));
}

/// Puts `key`'s value in `txn`, waiting out another transaction's open version of it.
fn put_waiting(db: &Db, txn: &mut Txn, key: &str, value: &str) {
    loop {
        match db.put(txn, key, value, Mode::Update) {
            Write::Done(_) => return,
            Write::Wait(_) => crate::thread::yield_now(),
            Write::Conflict => panic!("a conflict under read committed"),
        }
    }
}

/// One transaction writes every key and rolls back, `rounds` times, while another updates some of
/// them, waiting out the first, and a third reads them all through their chains: no read ever
/// sees a value the rolled-back one wrote, and nothing reads as corrupt, a version rolled back
/// under a writer that read it included. Every transaction takes the keys in one order, so no
/// wait closes a cycle.
fn rolled_back_values_are_never_read(rounds: usize, pad: usize) {
    const KEYS: [&str; 4] = ["k0", "k1", "k2", "k3"];
    let db = Arc::new(open());
    let mut setup = db.begin(Isolation::Snapshot);
    for k in KEYS {
        db.put(&mut setup, k, &padded("c", pad), Mode::Insert);
    }
    db.commit(setup);
    let roller = {
        let db = db.clone();
        crate::thread::spawn(move || {
            for i in 0..rounds {
                let mut t = db.begin(Isolation::ReadCommitted);
                for k in KEYS {
                    put_waiting(&db, &mut t, k, &padded(&format!("dead{i}"), pad));
                }
                db.txns().statement(&mut t).unwrap();
                let again = padded(&format!("dead{i}again"), pad);
                put_waiting(&db, &mut t, KEYS[i % KEYS.len()], &again);
                db.rollback(t);
            }
        })
    };
    let updater = {
        let db = db.clone();
        crate::thread::spawn(move || {
            for i in 0..rounds {
                let mut t = db.begin(Isolation::ReadCommitted);
                for k in &KEYS[..2] {
                    put_waiting(&db, &mut t, k, &padded(&format!("u{i}"), pad));
                }
                db.commit(t);
            }
        })
    };
    let reader = {
        let db = db.clone();
        crate::thread::spawn(move || {
            for _ in 0..2 * rounds {
                let mut t = db.begin(Isolation::Snapshot);
                for k in KEYS {
                    let v = db.get(&mut t, k);
                    assert!(
                        v.as_deref().is_some_and(|v| !v.starts_with("dead")),
                        "{k} read as {v:?}"
                    );
                }
                db.commit(t);
            }
        })
    };
    for t in [roller, updater, reader] {
        t.join().unwrap();
    }
    let last = padded(&format!("u{}", rounds - 1), pad);
    let c = padded("c", pad);
    assert_eq!(
        KEYS.map(|k| db.read_in(&db.tree, k)),
        [Some(last.clone()), Some(last), Some(c.clone()), Some(c)]
    );
}

#[test]
fn rolled_back_values_are_never_read_on_threads() {
    rolled_back_values_are_never_read(200, 0);
}

#[cfg(shuttle)]
#[test]
fn rolled_back_values_are_never_read_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(|| rolled_back_values_are_never_read(3, 0), 300);
}

/// [`rolled_back_values_are_never_read`] with every value out of line: a reader follows a chain
/// to a replaced version's value while the writer that replaced it rolls back and puts it back.
#[test]
fn rolled_back_values_out_of_line_are_never_read_on_threads() {
    rolled_back_values_are_never_read(100, LARGE);
}

#[cfg(shuttle)]
#[test]
fn rolled_back_values_out_of_line_are_never_read_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(|| rolled_back_values_are_never_read(2, LARGE), 300);
}

/// Values too long for a leaf keep their versions: a snapshot reads the one replaced since from
/// its version index, its pages kept and retired. A statement that writes one again frees the one
/// it wrote before, which no one reads, the statement itself reading what it found; a rollback
/// puts back the value its statement found by its reference, no longer retired, and frees the one
/// it wrote.
#[test]
fn values_out_of_line_keep_their_versions_and_free_what_no_one_reads() {
    let db = open();
    let big = |tag: &str| padded(tag, LARGE);
    let mut setup = db.begin(Isolation::Snapshot);
    assert_eq!(
        db.put(&mut setup, "k", &big("v1"), Mode::Insert),
        DONE_INSERTED
    );
    db.commit(setup);
    let v1 = db.pages_of("k");
    assert!(!v1.is_empty(), "v1 stayed in its leaf");
    let mut before = db.begin(Isolation::Snapshot);
    let mut w = db.begin(Isolation::Snapshot);
    assert_eq!(db.put(&mut w, "k", &big("v2"), Mode::Update), DONE_UPDATED);
    assert!(
        v1.iter().all(|&p| db.retired(p)),
        "v1's pages are not retired"
    );
    db.commit(w);
    assert_eq!(db.get(&mut before, "k"), Some(big("v1")));
    db.commit(before);
    assert_eq!(db.read_in(&db.tree, "k"), Some(big("v2")));
    let v2 = db.pages_of("k");
    let mut t = db.begin(Isolation::Snapshot);
    assert_eq!(db.put(&mut t, "k", &big("v3"), Mode::Update), DONE_UPDATED);
    let v3 = db.pages_of("k");
    assert!(
        v2.iter().all(|&p| db.retired(p)),
        "v2's pages are not retired"
    );
    assert_eq!(db.put(&mut t, "k", &big("v4"), Mode::Update), DONE_UPDATED);
    assert!(
        v3.iter().all(|&p| db.freed(p)),
        "a version only its own statement wrote kept its pages"
    );
    // The statement reads what it last wrote, through the leaf that holds it, though the pages of
    // the version before were freed; the next one reads the same.
    assert_eq!(db.get(&mut t, "k"), Some(big("v4")));
    db.txns().statement(&mut t).unwrap();
    assert_eq!(db.get(&mut t, "k"), Some(big("v4")));
    let v4 = db.pages_of("k");
    db.rollback(t);
    assert_eq!(db.read_in(&db.tree, "k"), Some(big("v2")));
    assert!(
        v2.iter().all(|&p| !db.retired(p)),
        "v2's pages stayed retired"
    );
    assert!(
        v4.iter().all(|&p| db.freed(p)),
        "the value rolled back kept its pages"
    );
}

/// A writer reads a key whose newest version is another transaction's, and that one rolls back
/// before the writer looks at it: the writer finds it gone, reads the key again, and writes over
/// the entry the rollback put back. The writer is stopped between the read and the look, so the
/// whole rollback falls there.
fn a_writer_reads_again_past_a_version_rolled_back_under_it() {
    let db = Arc::new(open());
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "k", "c", Mode::Insert);
    db.commit(setup);
    let mut t = db.begin(Isolation::ReadCommitted);
    assert!(matches!(
        db.put(&mut t, "k", "dead", Mode::Update),
        Write::Done(_)
    ));
    db.versions.stop_next_writer();
    let writer = {
        let db = db.clone();
        crate::thread::spawn(move || {
            let mut u = db.begin(Isolation::ReadCommitted);
            put_waiting(&db, &mut u, "k", "w");
            db.commit(u);
        })
    };
    db.versions.stopped_writer();
    db.rollback(t);
    db.versions.go_on();
    writer.join().unwrap();
    assert_eq!(db.read_in(&db.tree, "k").as_deref(), Some("w"));
}

/// A writer looks at a key, and another transaction writes the key before the writer's put goes
/// in: the put finds the entry changed, the writer judges the key again with its own key and
/// entry, waits out the other's version while it is open, and writes over it once it commits,
/// with a value inline or, `pad` past a node's longest, out of line. The writer is stopped between
/// its look and its judgment, so the other's write falls there.
fn a_writer_judges_again_a_key_written_since_its_look(pad: usize) {
    let db = Arc::new(open());
    let w = padded("w", pad);
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "k", "c", Mode::Insert);
    db.commit(setup);
    db.versions.stop_next_writer();
    let writer = {
        let db = db.clone();
        crate::thread::spawn(move || {
            let mut u = db.begin(Isolation::ReadCommitted);
            put_waiting(&db, &mut u, "k", &padded("w", pad));
            db.commit(u);
        })
    };
    db.versions.stopped_writer();
    let mut t = db.begin(Isolation::ReadCommitted);
    assert!(matches!(
        db.put(&mut t, "k", "t", Mode::Update),
        Write::Done(_)
    ));
    db.versions.go_on();
    db.commit(t);
    writer.join().unwrap();
    assert_eq!(db.read_in(&db.tree, "k"), Some(w));
}

#[test]
fn a_writer_judges_again_a_key_written_since_its_look_on_threads() {
    a_writer_judges_again_a_key_written_since_its_look(0);
}

#[test]
fn a_writer_judges_again_a_key_written_since_its_look_out_of_line_on_threads() {
    a_writer_judges_again_a_key_written_since_its_look(LARGE);
}

#[cfg(shuttle)]
#[test]
fn a_writer_judges_again_a_key_written_since_its_look_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || a_writer_judges_again_a_key_written_since_its_look(0),
        50,
    );
}

#[test]
fn a_writer_reads_again_past_a_version_rolled_back_under_it_on_threads() {
    a_writer_reads_again_past_a_version_rolled_back_under_it();
}

#[cfg(shuttle)]
#[test]
fn a_writer_reads_again_past_a_version_rolled_back_under_it_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        a_writer_reads_again_past_a_version_rolled_back_under_it,
        50,
    );
}

impl Db {
    /// Every row `txn`'s current statement sees, in key order, by a cursor moving `forward` or
    /// backward from the tree's end.
    fn scan(&self, txn: &mut Txn, forward: bool) -> Vec<(String, Vec<u8>)> {
        let mut c = VersionedCursor::new();
        let (from, seek): (&[u8], _) = if forward {
            (b"", Seek::Ge)
        } else {
            (&[0xff; 8], Seek::Le)
        };
        let mut found = run(&self.io, || {
            self.tree
                .seek(self.txns(), &self.versions, txn, &mut c, from, seek)
        });
        let mut out = Vec::new();
        while found {
            out.push((
                String::from_utf8(c.key().unwrap().to_vec()).unwrap(),
                c.value().unwrap().to_vec(),
            ));
            found = run(&self.io, || {
                if forward {
                    self.tree.next(self.txns(), &self.versions, txn, &mut c)
                } else {
                    self.tree.prev(self.txns(), &self.versions, txn, &mut c)
                }
            });
        }
        if !forward {
            out.reverse();
        }
        out
    }

    /// The key `seek` lands on from `key` for `txn`'s current statement.
    fn seek_key(&self, txn: &mut Txn, key: &str, seek: Seek) -> Option<String> {
        let mut c = VersionedCursor::new();
        let found = run(&self.io, || {
            self.tree.seek(
                self.txns(),
                &self.versions,
                txn,
                &mut c,
                key.as_bytes(),
                seek,
            )
        });
        found.then(|| String::from_utf8(c.key().unwrap().to_vec()).unwrap())
    }
}

/// A cursor sees each key as a point read does, in key order, both ways: through a history of
/// commits, rollbacks, deletions and values out of line, for snapshots taken all along it, a
/// transaction still open, one reading its own writes, and read committed. Every seek lands on the
/// key the point reads say it should.
#[test]
fn a_cursor_sees_each_key_as_a_point_read_does_in_order_both_ways() {
    for seed in 0..4u64 {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let db = open_with(16);
        let keys: Vec<String> = (0..48).map(|i| format!("k{i:03}")).collect();
        let mut setup = db.begin(Isolation::Snapshot);
        for key in keys.iter().step_by(2) {
            db.put(&mut setup, key, "v0", Mode::Insert);
        }
        db.commit(setup);
        let mut views = Vec::new();
        for round in 0..6 {
            views.push(db.begin(Isolation::Snapshot));
            let mut w = db.begin(Isolation::Snapshot);
            for key in &keys {
                match rng.random_range(0..6) {
                    0 => {
                        db.delete(&mut w, key);
                    }
                    1 => {
                        let value = padded(&format!("r{round}"), LARGE);
                        db.put(&mut w, key, &value, Mode::Upsert);
                    }
                    2 | 3 => {
                        db.put(&mut w, key, &format!("r{round}"), Mode::Upsert);
                    }
                    _ => {}
                }
            }
            if round % 3 == 2 {
                db.rollback(w);
            } else {
                db.commit(w);
            }
        }
        // Open, and writing: others see none of it, it sees all of it.
        let mut open = db.begin(Isolation::Snapshot);
        for key in keys.iter().step_by(3) {
            if rng.random_bool(0.5) {
                db.delete(&mut open, key);
            } else {
                db.put(&mut open, key, "open", Mode::Upsert);
            }
        }
        views.push(db.begin(Isolation::Snapshot));
        views.push(db.begin(Isolation::ReadCommitted));
        views.push(open);
        let mut distinct = std::collections::BTreeSet::new();
        for (i, view) in views.iter_mut().enumerate() {
            let model: Vec<(String, Vec<u8>)> = keys
                .iter()
                .filter_map(|k| db.get_bytes(view, k).map(|v| (k.clone(), v)))
                .collect();
            let label = format!("seed {seed}, view {i}");
            assert_eq!(db.scan(view, true), model, "{label}: forward");
            assert_eq!(db.scan(view, false), model, "{label}: backward");
            let present: Vec<&String> = model.iter().map(|(k, _)| k).collect();
            for probe in keys.iter().chain([&"k".to_string(), &"l".to_string()]) {
                let want = |seek: Seek| -> Option<String> {
                    let at = |k: &&&String| match seek {
                        Seek::Ge => **k >= probe,
                        Seek::Gt => **k > probe,
                        Seek::Le => **k <= probe,
                        Seek::Lt => **k < probe,
                    };
                    match seek {
                        Seek::Ge | Seek::Gt => present.iter().find(at),
                        Seek::Le | Seek::Lt => present.iter().rev().find(at),
                    }
                    .map(|k| (*k).clone())
                };
                for seek in [Seek::Ge, Seek::Gt, Seek::Le, Seek::Lt] {
                    assert_eq!(
                        db.seek_key(view, probe, seek),
                        want(seek),
                        "{label}: {seek:?} {probe}"
                    );
                }
            }
            distinct.insert(model);
        }
        // The views differ from one another, so the cursor met every kind of chain.
        assert!(
            distinct.len() >= 6,
            "seed {seed}: only {} distinct views",
            distinct.len()
        );
        for view in views {
            db.rollback(view);
        }
    }
}

/// Snapshot readers scan every key, both ways, while one writer commits a new generation of all of
/// them in each transaction and another writes some of them and rolls back: each scan reads one
/// generation whole, every key present once, in order, and never a value rolled back. Every
/// writer takes the keys in one order, so no wait closes a cycle.
fn cursors_read_whole_snapshots_while_writers_commit_and_roll_back(
    rounds: usize,
    readers: usize,
    pad: usize,
) {
    const KEYS: usize = 12;
    let keys: Arc<Vec<String>> = Arc::new((0..KEYS).map(|i| format!("k{i:02}")).collect());
    let db = Arc::new(open_with(readers + 4));
    let mut setup = db.begin(Isolation::Snapshot);
    for k in keys.iter() {
        db.put(&mut setup, k, &padded("g0", pad), Mode::Insert);
    }
    db.commit(setup);
    let writer = {
        let (db, keys) = (db.clone(), keys.clone());
        crate::thread::spawn(move || {
            for i in 1..=rounds {
                let mut t = db.begin(Isolation::ReadCommitted);
                for k in keys.iter() {
                    put_waiting(&db, &mut t, k, &padded(&format!("g{i}"), pad));
                }
                db.commit(t);
            }
        })
    };
    let roller = {
        let (db, keys) = (db.clone(), keys.clone());
        crate::thread::spawn(move || {
            for i in 0..rounds {
                let mut t = db.begin(Isolation::ReadCommitted);
                for k in keys.iter().skip(i % 3).step_by(2) {
                    put_waiting(&db, &mut t, k, &padded(&format!("dead{i}"), pad));
                }
                db.rollback(t);
            }
        })
    };
    let scanners: Vec<_> = (0..readers)
        .map(|r| {
            let (db, keys) = (db.clone(), keys.clone());
            crate::thread::spawn(move || {
                for n in 0..2 * rounds {
                    let mut t = db.begin(Isolation::Snapshot);
                    let rows = db.scan(&mut t, (r + n) % 2 == 0);
                    let got: Vec<&String> = rows.iter().map(|(k, _)| k).collect();
                    assert_eq!(got, keys.iter().collect::<Vec<_>>(), "keys of one scan");
                    let first = &rows[0].1;
                    for (k, v) in &rows {
                        let text = String::from_utf8_lossy(v);
                        assert!(!text.starts_with("dead"), "{k} read a rolled-back value");
                        assert_eq!(v, first, "{k}: a scan read two generations");
                    }
                    db.commit(t);
                }
            })
        })
        .collect();
    for t in [writer, roller].into_iter().chain(scanners) {
        t.join().unwrap();
    }
}

/// [`cursors_read_whole_snapshots_while_writers_commit_and_roll_back`] on twelve threads: ten
/// scanning, a writer and a roller.
#[test]
fn cursors_read_whole_snapshots_while_writers_commit_and_roll_back_on_threads() {
    cursors_read_whole_snapshots_while_writers_commit_and_roll_back(100, 10, 0);
}

/// The same with every value out of line: a scan reads a replaced value through its version index
/// while the writer that replaced it commits or rolls back.
#[test]
fn cursors_read_whole_snapshots_out_of_line_while_writers_commit_and_roll_back_on_threads() {
    cursors_read_whole_snapshots_while_writers_commit_and_roll_back(40, 10, LARGE);
}

#[cfg(shuttle)]
#[test]
fn cursors_read_whole_snapshots_while_writers_commit_and_roll_back_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || cursors_read_whole_snapshots_while_writers_commit_and_roll_back(2, 2, 0),
        300,
    );
}

#[cfg(shuttle)]
#[test]
fn cursors_read_whole_snapshots_out_of_line_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || cursors_read_whole_snapshots_while_writers_commit_and_roll_back(2, 2, LARGE),
        300,
    );
}

/// Writes `key` under read committed, parking on its writer's end whenever it meets one open, as
/// the SQL layer does: it registers, writes again, and parks only while it still waits on the
/// same writer, reusing its wait. Returns how often it parked.
fn put_parking(db: &Db, txn: &mut Txn, key: &str, value: &str) -> usize {
    let mut parks = 0;
    let mut wait: Option<(Blocker, Completion)> = None;
    loop {
        match db.put(txn, key, value, Mode::Update) {
            Write::Done(_) => return parks,
            Write::Conflict => panic!("a conflict under read committed"),
            Write::Wait(blocker) => match &wait {
                Some((b, c)) if *b == blocker && !c.finished() => {
                    parks += 1;
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
                    while !c.finished() {
                        assert!(std::time::Instant::now() < deadline, "a wait never woken");
                        crate::thread::yield_now();
                    }
                }
                _ => {
                    let c = Completion::new_wait();
                    db.txns().wait_end(blocker, &c);
                    wait = Some((blocker, c));
                }
            },
        }
    }
}

/// A wait registered on a writer's end is woken by its commit, by its rollback, and by a
/// statement rollback that puts the key back while it runs on; until then, judged again, the
/// write still waits on the same writer.
#[test]
fn a_wait_on_a_writer_is_woken_by_its_commit_rollback_and_statement_rollback() {
    let db = open();
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "k", "c0", Mode::Insert);
    db.commit(setup);
    for end in ["commit", "rollback", "statement rollback"] {
        let mut a = db.begin(Isolation::Snapshot);
        db.txns().statement(&mut a).unwrap();
        assert!(matches!(
            db.put(&mut a, "k", "a", Mode::Update),
            Write::Done(_)
        ));
        let mut b = db.begin(Isolation::ReadCommitted);
        let blocker = Blocker {
            slot: a.slot(),
            start: a.start(),
        };
        assert_eq!(db.put(&mut b, "k", "b", Mode::Update), Write::Wait(blocker));
        let c = Completion::new_wait();
        db.txns().wait_end(blocker, &c);
        assert_eq!(db.put(&mut b, "k", "b", Mode::Update), Write::Wait(blocker));
        assert!(!c.finished(), "{end}: woken before it");
        let still = match end {
            "commit" => {
                db.commit(a);
                None
            }
            "rollback" => {
                db.rollback(a);
                None
            }
            _ => {
                db.rollback_to(&mut a, 1);
                Some(a)
            }
        };
        assert!(c.finished(), "{end}: no waiter woken");
        assert!(
            matches!(db.put(&mut b, "k", "b", Mode::Update), Write::Done(_)),
            "{end}"
        );
        db.commit(b);
        if let Some(a) = still {
            db.commit(a);
        }
    }
}

/// A blocker's holder is its transaction's group while it runs, and none once it has ended, even
/// with its slot running another group's transaction.
#[test]
fn a_blockers_holder_is_its_group_while_it_runs() {
    let db = open_with(1);
    let a = run(&db.io, || db.txns().begin_in(Isolation::Snapshot, 7));
    let first = Blocker {
        slot: a.slot(),
        start: a.start(),
    };
    assert_eq!(db.txns().holder(first), Some(7));
    db.txns().abort(a);
    assert_eq!(db.txns().holder(first), None);
    let b = run(&db.io, || db.txns().begin_in(Isolation::Snapshot, 9));
    assert_eq!(b.slot(), first.slot, "one slot, taken again");
    assert_eq!(db.txns().holder(first), None, "the slot's next group");
    let second = Blocker {
        slot: b.slot(),
        start: b.start(),
    };
    assert_eq!(db.txns().holder(second), Some(9));
    db.txns().abort(b);
}

/// Writers on a few hot keys in one order, parking on one another, rolling back whole and to an
/// earlier statement, and committing: every wait parked is woken, and every writer finishes.
fn parked_writers_all_finish(threads: usize, rounds: usize) -> usize {
    const KEYS: [&str; 3] = ["h0", "h1", "h2"];
    let db = Arc::new(open_with(threads));
    let mut setup = db.begin(Isolation::Snapshot);
    for k in KEYS {
        db.put(&mut setup, k, "0", Mode::Insert);
    }
    db.commit(setup);
    let workers: Vec<_> = (0..threads)
        .map(|w| {
            let db = db.clone();
            crate::thread::spawn(move || {
                let mut parks = 0;
                for r in 0..rounds {
                    let mut t = db.begin(Isolation::ReadCommitted);
                    db.txns().statement(&mut t).unwrap();
                    for k in KEYS {
                        parks += put_parking(&db, &mut t, k, &format!("{w}-{r}"));
                    }
                    match (w + r) % 3 {
                        0 => db.commit(t),
                        1 => db.rollback(t),
                        _ => {
                            db.rollback_to(&mut t, 1);
                            db.commit(t);
                        }
                    }
                }
                parks
            })
        })
        .collect();
    workers.into_iter().map(|w| w.join().unwrap()).sum()
}

#[test]
fn parked_writers_all_finish_on_twelve_threads() {
    let parks = parked_writers_all_finish(12, 40);
    println!("{parks} parks");
    assert!(parks > 0, "no writer parked");
}

#[cfg(shuttle)]
#[test]
fn parked_writers_all_finish_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || {
            parked_writers_all_finish(3, 2);
        },
        500,
    );
}

fn probe(db: &Db, txn: &mut Txn, key: &str) -> Probe {
    run(&db.io, || {
        db.tree
            .probe(db.txns(), txn, key.as_bytes(), Near::NONE, &mut None)
    })
}

/// A probe judges a key by its newest version: absent with none, present or absent by the
/// transaction's own version, a wait on another's open version, and, once that commits, a
/// conflict under snapshot isolation and the committed version's word under read committed; a
/// version rolled back leaves the key as it was.
#[test]
fn a_probe_judges_a_key_by_its_newest_version() {
    let db = open();
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "kept", "v", Mode::Insert);
    db.put(&mut setup, "doomed", "v", Mode::Insert);
    db.commit(setup);
    let mut snap = db.begin(Isolation::Snapshot);
    let mut rc = db.begin(Isolation::ReadCommitted);
    assert_eq!(probe(&db, &mut snap, "none"), Probe::Absent);
    assert_eq!(probe(&db, &mut snap, "kept"), Probe::Present);
    let mut own = db.begin(Isolation::Snapshot);
    db.put(&mut own, "mine", "v", Mode::Insert);
    db.delete(&mut own, "kept");
    assert_eq!(
        probe(&db, &mut own, "mine"),
        Probe::Present,
        "its own insert"
    );
    assert_eq!(
        probe(&db, &mut own, "kept"),
        Probe::Absent,
        "its own delete"
    );
    db.rollback(own);
    assert_eq!(
        probe(&db, &mut snap, "mine"),
        Probe::Absent,
        "a rolled-back insert"
    );
    assert_eq!(
        probe(&db, &mut snap, "kept"),
        Probe::Present,
        "a rolled-back delete"
    );
    let mut other = db.begin(Isolation::Snapshot);
    db.put(&mut other, "new", "v", Mode::Insert);
    db.delete(&mut other, "doomed");
    let blocker = Blocker {
        slot: other.slot(),
        start: other.start(),
    };
    assert_eq!(probe(&db, &mut snap, "new"), Probe::Wait(blocker));
    assert_eq!(probe(&db, &mut rc, "doomed"), Probe::Wait(blocker));
    db.commit(other);
    assert_eq!(probe(&db, &mut snap, "new"), Probe::Conflict);
    assert_eq!(probe(&db, &mut snap, "doomed"), Probe::Conflict);
    db.txns().statement(&mut rc).unwrap();
    assert_eq!(probe(&db, &mut rc, "new"), Probe::Present);
    assert_eq!(probe(&db, &mut rc, "doomed"), Probe::Absent);
    db.commit(snap);
    db.commit(rc);
}

/// A delete changes the head alone and leaves an inline value in the tombstone, and a change of a
/// value to one as long changes only the bytes that differ: each logs one patch, a snapshot taken
/// before reads the value it replaced, and a rollback puts it back whole.
/// A write names the leaf it reached, where its key is, and the next write and probe try it
/// first: keys written in order miss it once at the start and once after each split.
#[test]
fn a_write_names_the_leaf_it_reached_and_the_next_one_starts_there() {
    use crate::storage::atoll::btree::{Cursor, NEAR_TAKEN};
    let db = open();
    let mut t = db.begin(Isolation::Snapshot);
    let key = |n: u64| format!("key-{n:05}");
    let (mut near, mut misses) = (Near::NONE, 0);
    let writes = 2_000;
    for n in 0..writes {
        let taken = NEAR_TAKEN.with(|c| c.get());
        let mut op = WriteOp::put(key(n).as_bytes(), vec![b'v'; 40], Mode::Insert).near(near);
        let wrote = run(&db.io, || {
            db.tree.write(db.txns(), &db.versions, &mut t, &mut op)
        });
        assert_eq!(wrote, DONE_INSERTED);
        misses += (NEAR_TAKEN.with(|c| c.get()) == taken) as u64;
        near = op.reached();
        // The leaf named is the one a seek finds the key in.
        let mut cursor = Cursor::new();
        assert!(run(&db.io, || {
            db.tree
                .tree()
                .seek(&mut cursor, key(n).as_bytes(), Seek::Ge)
        }));
        assert_eq!(cursor.key(), Some(key(n).as_bytes()));
        assert_eq!(near.page(), cursor.near().page(), "{}", key(n));
        // A probe from it finds the statement's own version.
        let taken = NEAR_TAKEN.with(|c| c.get());
        let probed = run(&db.io, || {
            db.tree
                .probe(db.txns(), &mut t, key(n).as_bytes(), near, &mut None)
        });
        assert_eq!(probed, Probe::Present, "{}", key(n));
        assert_eq!(NEAR_TAKEN.with(|c| c.get()), taken + 1, "{}", key(n));
    }
    let mut leaves = 0;
    let mut last = None;
    let mut cursor = Cursor::new();
    let mut more = run(&db.io, || db.tree.tree().seek(&mut cursor, b"", Seek::Ge));
    while more {
        if last != Some(cursor.near()) {
            leaves += 1;
            last = Some(cursor.near());
        }
        more = run(&db.io, || db.tree.tree().next(&mut cursor));
    }
    assert!(leaves >= 10, "only {leaves} leaves");
    assert!(misses <= leaves + 1, "{misses} misses over {leaves} leaves");
    db.commit(t);
}

#[test]
fn deletes_and_changes_in_place_log_patches_and_keep_what_snapshots_read() {
    use crate::storage::atoll::btree::record::{Change, Record, TXN_PATCH};
    let db = open();
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "d", "value-AAAA", Mode::Insert);
    db.put(&mut setup, "u", "value-AAAA", Mode::Insert);
    db.commit(setup);
    let mut before = db.begin(Isolation::Snapshot);
    assert_eq!(db.get(&mut before, "d").as_deref(), Some("value-AAAA"));
    let mut t = db.begin(Isolation::Snapshot);
    assert_eq!(db.delete(&mut t, "d"), Write::Done(Outcome::Updated));
    db.txns().statement(&mut t).unwrap();
    assert_eq!(
        db.put(&mut t, "u", "value-ABBA", Mode::Update),
        Write::Done(Outcome::Updated)
    );
    // The tombstone holds the value it replaced, past a head that says deleted.
    let stored = run(&db.io, || db.tree.tree().stored(b"d")).expect("a tombstone");
    let head = Version::decode(&stored.head).unwrap();
    assert_eq!(head.flags & Version::DELETED, Version::DELETED);
    assert_eq!(stored.value, StoredValue::Inline(b"value-AAAA".to_vec()));
    // Both logged as patches: the delete with no bytes, the change with the two that differ.
    let kinds: Vec<Kind> = db.chain(t.start()).into_iter().map(|(k, _)| k).collect();
    assert_eq!(kinds, vec![TXN_PATCH, TXN_PATCH]);
    drive(&db.io, || db.db.log().wait_durable(db.db.log().end())).unwrap();
    let mut reader = crate::storage::atoll::log::Log::reader(
        db.io.clone(),
        BASE,
        &LogConfig {
            segment_bytes: 1 << 20,
            buffer_bytes: 256 << 10,
            chunk_bytes: 16 << 10,
            writes_in_flight: 3,
            direct: true,
        },
        0,
    );
    let mut patches = Vec::new();
    while let Some(r) = reader.next_record().unwrap() {
        if r.header.txn != t.start() {
            continue;
        }
        if let Some(Record {
            change:
                Change::Patch {
                    key,
                    at,
                    bytes,
                    old_bytes,
                    ..
                },
            ..
        }) = Record::decode(r.header.kind, &r.payload).unwrap()
        {
            patches.push((key.to_vec(), at, bytes.to_vec(), old_bytes.to_vec()));
        }
    }
    assert_eq!(
        patches,
        vec![
            (b"d".to_vec(), 10, vec![], vec![]),
            (b"u".to_vec(), 7, b"BB".to_vec(), b"AA".to_vec()),
        ]
    );
    // The snapshot from before reads what both replaced; the transaction reads its own.
    assert_eq!(db.get(&mut before, "d").as_deref(), Some("value-AAAA"));
    assert_eq!(db.get(&mut before, "u").as_deref(), Some("value-AAAA"));
    assert_eq!(db.get(&mut t, "d"), None);
    assert_eq!(db.get(&mut t, "u").as_deref(), Some("value-ABBA"));
    db.rollback(t);
    let mut after = db.begin(Isolation::Snapshot);
    assert_eq!(db.get(&mut after, "d").as_deref(), Some("value-AAAA"));
    assert_eq!(db.get(&mut after, "u").as_deref(), Some("value-AAAA"));
    db.commit(after);
    db.commit(before);
}

/// A write of a key with no entry before appends nothing to its slot's arena, and a write
/// replacing an entry, a tombstone included, appends what it replaced; a rollback puts every key
/// back from the transaction's changes, the inserted ones removed.
#[test]
fn an_insert_appends_nothing_to_the_arena_and_its_rollback_removes_it() {
    use crate::storage::atoll::txn::arena::APPENDS;
    let appends = || APPENDS.with(|n| n.get());
    let db = open();
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "a", "1", Mode::Insert);
    db.put(&mut setup, "gone", "x", Mode::Insert);
    db.commit(setup);
    let mut deleter = db.begin(Isolation::Snapshot);
    assert_eq!(db.delete(&mut deleter, "gone"), DONE_UPDATED);
    db.commit(deleter);
    let mut t = db.begin(Isolation::Snapshot);
    let before = appends();
    assert_eq!(db.put(&mut t, "b", "2", Mode::Insert), DONE_INSERTED);
    assert_eq!(appends(), before, "an insert");
    assert_eq!(db.put(&mut t, "a", "11", Mode::Upsert), DONE_UPDATED);
    assert_eq!(appends(), before + 1, "an update");
    assert_eq!(db.put(&mut t, "gone", "back", Mode::Upsert), DONE_INSERTED);
    assert_eq!(appends(), before + 2, "a write over a tombstone");
    db.txns().statement(&mut t).unwrap();
    assert_eq!(db.put(&mut t, "c", "3", Mode::Insert), DONE_INSERTED);
    assert_eq!(db.put(&mut t, "b", "22", Mode::Upsert), DONE_UPDATED);
    db.rollback_to(&mut t, 1);
    assert_eq!(
        db.get(&mut t, "c"),
        None,
        "inserted by the statement rolled back"
    );
    assert_eq!(
        db.get(&mut t, "b").as_deref(),
        Some("2"),
        "the earlier statement's"
    );
    db.rollback(t);
    for (key, want) in [("a", Some("1")), ("b", None), ("c", None), ("gone", None)] {
        assert_eq!(db.read_in(&db.tree, key).as_deref(), want, "{key}");
    }
}

impl Db {
    /// The tree's root, a leaf, as of the snapshot taken at `at`.
    fn root_as_of(&self, at: Ts) -> std::result::Result<Vec<u8>, Overflow> {
        let root = self.tree.root();
        let page = run(&self.io, || self.db.pool().fix_exclusive(root, None));
        let mut bytes = vec![0u8; page.page().len()];
        page.page().copy_to(0, &mut bytes);
        drop(page);
        let mut as_of = AsOf::new(&bytes[..], at).unwrap();
        run(&self.io, || as_of.step(self.txns(), &self.versions));
        as_of.image().unwrap()
    }
}

#[test]
fn a_leaf_as_of_a_snapshot_holds_each_key_as_the_snapshot_reads_it_settled() {
    use crate::storage::atoll::btree::node::View;
    let db = open();
    let mut setup = db.begin(Isolation::Snapshot);
    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")] {
        db.put(&mut setup, key, value, Mode::Insert);
    }
    db.put_bytes(&mut setup, "big", vec![7; LARGE], Mode::Insert);
    db.commit(setup);
    let mut deleter = db.begin(Isolation::Snapshot);
    db.delete(&mut deleter, "e");
    db.commit(deleter);
    let big = match run(&db.io, || db.tree.tree().stored(b"big")).unwrap().value {
        StoredValue::OutOfLine(r) => r,
        StoredValue::Inline(_) => panic!("a large value inline"),
    };
    let mut reader = db.begin(Isolation::Snapshot);
    let at = reader.snapshot;
    // After the snapshot: committed, twice over for c and across statements for a.
    let mut t1 = db.begin(Isolation::Snapshot);
    db.put(&mut t1, "a", "11", Mode::Update);
    db.delete(&mut t1, "b");
    db.put(&mut t1, "c", "33", Mode::Update);
    db.put(&mut t1, "big", "small", Mode::Update);
    db.put(&mut t1, "g", "7", Mode::Insert);
    db.txns().statement(&mut t1).unwrap();
    db.put(&mut t1, "a", "111", Mode::Update);
    db.commit(t1);
    let mut t2 = db.begin(Isolation::Snapshot);
    db.put(&mut t2, "c", "333", Mode::Update);
    db.put(&mut t2, "e", "55", Mode::Insert);
    db.commit(t2);
    // And still running.
    let mut t3 = db.begin(Isolation::Snapshot);
    db.put(&mut t3, "d", "44", Mode::Update);
    db.put(&mut t3, "h", "8", Mode::Insert);

    let image = db.root_as_of(at).expect("a leaf as it was fits");
    let entries = View::new(&image[..]).entries().unwrap();
    let keys: Vec<&[u8]> = entries.iter().map(|e| &e.key[..]).collect();
    assert_eq!(
        keys,
        [&b"a"[..], b"b", b"big", b"c", b"d"],
        "every key the snapshot reads, and no other"
    );
    for e in &entries {
        let key = std::str::from_utf8(&e.key).unwrap();
        assert_eq!(
            e.value[..HEADER],
            Version::SETTLED.encode(),
            "{key}: settled"
        );
        match key {
            "big" => {
                assert!(e.out_of_line, "{key}: by its reference");
                assert_eq!(e.value[HEADER..], big.encode(), "{key}");
            }
            _ => {
                assert!(!e.out_of_line, "{key}");
                assert_eq!(
                    Some(&e.value[HEADER..]),
                    db.get_bytes(&mut reader, key).as_deref(),
                    "{key}"
                );
            }
        }
    }
    assert_eq!(db.get_bytes(&mut reader, "big"), Some(vec![7; LARGE]));
    let layout = |p: &[u8]| {
        let v = View::new(p);
        (v.root(), v.level(), v.next(), node::shape(p))
    };
    let page = run(&db.io, || db.db.pool().fix_exclusive(db.tree.root(), None));
    let mut live = vec![0u8; page.page().len()];
    page.page().copy_to(0, &mut live);
    drop(page);
    assert_eq!(layout(&image), layout(&live), "the leaf's layout");
    assert_eq!(image[..page::HEADER_SIZE], live[..page::HEADER_SIZE]);
    db.rollback(t3);
}

#[test]
fn a_leaf_as_of_a_snapshot_that_does_not_fit_its_page_says_by_how_much() {
    use crate::storage::atoll::btree::node::{Limits, View};
    let db = open();
    let wide = |s: &str| padded(s, 700);
    let mut setup = db.begin(Isolation::Snapshot);
    for key in ["x0", "x1", "x2", "x3"] {
        db.put(&mut setup, key, &wide(key), Mode::Insert);
    }
    db.commit(setup);
    // An update that shrinks every row, still open, and an insert committed into the room it
    // freed before the snapshot.
    let mut shrink = db.begin(Isolation::Snapshot);
    for key in ["x0", "x1", "x2", "x3"] {
        db.put(&mut shrink, key, "-", Mode::Update);
    }
    let mut fill = db.begin(Isolation::Snapshot);
    for key in ["y0", "y1", "y2", "y3"] {
        db.put(&mut fill, key, &wide(key), Mode::Insert);
    }
    db.commit(fill);
    let reader = db.begin(Isolation::Snapshot);
    let page = run(&db.io, || db.db.pool().fix_exclusive(db.tree.root(), None));
    assert!(View::new(page.page()).is_leaf(), "one leaf");
    drop(page);
    let capacity = Limits::of(SizeClass::new(0).unwrap()).capacity;
    let entry = |key: &str| node::SLOT + key.len() + HEADER + wide(key).len();
    let needs: usize = ["x0", "x1", "x2", "x3", "y0", "y1", "y2", "y3"]
        .into_iter()
        .map(entry)
        .sum();
    assert_eq!(
        db.root_as_of(reader.snapshot),
        Err(Overflow { needs, capacity })
    );
    db.rollback(shrink);
}
