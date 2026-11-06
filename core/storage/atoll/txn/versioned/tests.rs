use super::*;
use crate::io::{Completion, MemoryIO};
use crate::storage::atoll::btree::node;
use crate::storage::atoll::btree::value;
use crate::storage::atoll::btree::Seek;
use crate::storage::atoll::buffer::{BufferConfig, MIN_DB_BUDGET};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::{Kind, LogConfig};
use crate::storage::atoll::page::{self, SizeClass};
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::shared::{Bytes, BytesMut};
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
    open_sized(
        slots,
        MIN_DB_BUDGET,
        SizeClass::new(0).unwrap(),
        Some(512 << 10),
    )
}

/// [`open_with`], on a pool of `budget` bytes, the tree's nodes of `class`, and a log recovery
/// replays `log_budget` of.
fn open_sized(slots: usize, budget: usize, class: SizeClass, log_budget: Option<u64>) -> Db {
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
            budget,
            writeback_bytes: crate::storage::atoll::buffer::WRITEBACK_BYTES,
            kept_bytes: None,
        },
        log_budget,
        checkpoint_write_bytes: None,
        txn: TxnConfig { slots },
    };
    let db = Atoll::open(io.clone(), BASE, config).unwrap();
    let mut root = None;
    drive(&io, || {
        root = Some(match Tree::create_versioned(db.pool(), class)? {
            IOResult::Done(r) => r,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        });
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
        self.db.store().retired(pid)
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

/// A checkpoint's image of a versioned leaf stands for the leaf only where every reader reads
/// the two alike. A snapshot older than the checkpoint's, still reading the version below the
/// leaf's, keeps the leaf owed: evicted, the leaf reads back as it was, never as the image, whose
/// versions are all settled. Once that snapshot ends, the next checkpoint's image stands for it.
#[test]
fn a_leaf_an_older_snapshot_reads_below_is_taken_for_an_image_only_once_it_ends() {
    let db = open();
    let mut w = db.begin(Isolation::Snapshot);
    assert_eq!(db.put(&mut w, "k", "v1", Mode::Insert), DONE_INSERTED);
    db.commit(w);
    let mut old = db.begin(Isolation::Snapshot);
    assert_eq!(db.get(&mut old, "k").as_deref(), Some("v1"));
    let mut w = db.begin(Isolation::Snapshot);
    assert_eq!(db.put(&mut w, "k", "v2", Mode::Update), DONE_UPDATED);
    db.commit(w);
    let cp = db.db.checkpointer();
    let ticket = cp.request_full();
    drive(&db.io, || cp.step_full(ticket, db.txns())).unwrap();
    let pool = db.db.pool();
    let mut sweep = pool.sweep();
    drive(&db.io, || sweep.step(pool)).unwrap();
    assert!(pool.evict_clean(db.tree.root()), "the leaf left its frame");
    assert_eq!(
        db.get(&mut old, "k").as_deref(),
        Some("v1"),
        "the older snapshot's version"
    );
    db.commit(old);
    let ticket = cp.request_full();
    drive(&db.io, || cp.step_full(ticket, db.txns())).unwrap();
    assert!(
        db.db.store().swap_slot(db.tree.root()).is_none(),
        "the leaf kept its swap copy"
    );
    assert!(pool.evict_clean(db.tree.root()), "the leaf settled, clean");
}

/// An older snapshot reads its versions while a writer refused room for kept copies steps the
/// checkpoint itself: the images of the leaves it reads below are not taken for the leaves, so
/// each, evicted, reads back as it stands, and the snapshot follows its chains to the versions
/// it saw (the regression above, under memory pressure). The leaves are 64 KiB, so 128 copies fill
/// the room of a 64 MiB pool; the writer changes the leaves of the first 12,000 keys, more than
/// that, and leaves the rest as the cut found them, the leaves an image could be taken for.
#[test]
fn an_older_snapshot_reads_its_versions_while_a_writer_refused_room_steps_the_checkpoint() {
    let db = open_sized(
        4,
        64 << 20,
        SizeClass::new(node::MAX_CLASS).unwrap(),
        Some(1 << 30),
    );
    let orphan = run(&db.io, || {
        Tree::create(db.db.pool(), SizeClass::new(0).unwrap())
    });
    let keys: Vec<String> = (0..20_000).map(|i| format!("k{i:06}")).collect();
    let changed = 12_000;
    let v = |round: u8, k: &str| padded(&format!("{k}:{round}"), 900);
    let mut w = db.begin(Isolation::Snapshot);
    for k in &keys {
        assert_eq!(db.put(&mut w, k, &v(1, k), Mode::Insert), DONE_INSERTED);
    }
    db.commit(w);
    let cp = db.db.checkpointer();
    let ticket = cp.request_full();
    drive(&db.io, || cp.step_full(ticket, db.txns())).unwrap();
    let mut old = db.begin(Isolation::Snapshot);
    assert_eq!(db.get(&mut old, &keys[0]), Some(v(1, &keys[0])));
    let mut w = db.begin(Isolation::Snapshot);
    for k in &keys {
        assert_eq!(db.put(&mut w, k, &v(2, k), Mode::Update), DONE_UPDATED);
    }
    db.commit(w);
    // The next checkpoint held after its cut, in its first edit of the orphan.
    db.db.store().orphaned(orphan);
    let frame = drive(&db.io, || db.db.store().leaf(orphan))
        .unwrap()
        .expect("the orphan's page-table leaf");
    let mut hold = Some(frame.try_lock().expect("the leaf is free"));
    let ticket = cp.request_full();
    assert!(
        matches!(cp.step_full(ticket, db.txns()).unwrap(), IOResult::IO(_)),
        "the checkpoint went on under a held latch"
    );
    assert!(cp.cut_taken().is_some(), "the hold came before the cut");
    let pool = db.db.pool();
    let mut w = db.begin(Isolation::Snapshot);
    for k in &keys[..changed] {
        let mut op = WriteOp::put(k.as_bytes(), v(3, k).into_bytes(), Mode::Update);
        // Bounded, so a writer waiting on what nothing advances fails rather than hangs: each
        // return images a page, lands a write or ends the cut.
        let mut out = None;
        for _ in 0..1 << 20 {
            if pool.kept_stats().refusals > 0 {
                if let Some(h) = hold.take() {
                    h.release_unchanged();
                }
            }
            match db
                .tree
                .write(db.txns(), &db.versions, &mut w, &mut op)
                .unwrap()
            {
                IOResult::Done(written) => {
                    out = Some(written);
                    break;
                }
                IOResult::IO(crate::types::IOCompletions(c)) => {
                    let mut steps = 0;
                    while !c.finished() {
                        db.io.step().unwrap();
                        steps += 1;
                        assert!(steps <= 64, "a wait no IO in flight ends");
                    }
                }
            }
        }
        assert_eq!(out, Some(DONE_UPDATED), "{k}");
    }
    assert!(hold.is_none(), "no write was refused room");
    let stats = pool.kept_stats();
    assert!(stats.peak <= stats.limit, "the copies: {stats:?}");
    db.commit(w);
    drive(&db.io, || cp.step_full(ticket, db.txns())).unwrap();
    drop(hold);
    drop(frame);
    // Every page of the tree in its copies alone.
    let mut sweep = pool.sweep();
    drive(&db.io, || sweep.step(pool)).unwrap();
    let mut check = crate::storage::atoll::btree::Check::default();
    let pages = drive(&db.io, || db.tree.tree().check(&mut check))
        .unwrap()
        .pages;
    let evicted = pages.iter().filter(|&&p| pool.evict_clean(p)).count();
    assert!(evicted > 128, "{evicted} of {} pages evicted", pages.len());
    for k in &keys {
        assert_eq!(
            db.get(&mut old, k),
            Some(v(1, k)),
            "the older snapshot's {k}"
        );
    }
    db.commit(old);
    let mut now = db.begin(Isolation::Snapshot);
    for (i, k) in keys.iter().enumerate().step_by(97) {
        let round = if i < changed { 3 } else { 2 };
        assert_eq!(db.get(&mut now, k), Some(v(round, k)), "the newest {k}");
    }
    db.commit(now);
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

    /// The kinds of `start`'s records, in log order.
    fn records(&self, start: Ts) -> Vec<Kind> {
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
            if r.header.txn == start {
                out.push(r.header.kind);
            }
        }
        out
    }
}

/// A second versioned tree, of a larger node class: a transaction writing both has a version
/// index of each class.
fn second_tree(db: &Db) -> VersionedTree {
    let root = run(&db.io, || {
        Tree::create_versioned(db.db.pool(), SizeClass::new(1).unwrap())
    });
    VersionedTree::open(
        db.io.clone(),
        db.db.pool().clone(),
        db.db.store().clone(),
        root,
    )
}

#[test]
fn a_rollback_puts_back_every_key_logs_nothing_and_frees_its_slot() {
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
    // The log holds nothing of a transaction before its commit, and a rollback adds nothing.
    assert_eq!(
        db.records(start),
        Vec::new(),
        "the rolled back transaction's records"
    );
    // Its slot is free: all four can begin.
    let all: Vec<Txn> = (0..4).map(|_| db.begin(Isolation::Snapshot)).collect();
    for txn in all {
        db.commit(txn);
    }
}

#[test]
fn a_statement_that_changed_a_key_again_and_again_is_put_back_to_what_it_found() {
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
    assert_eq!(
        db.records(start),
        Vec::new(),
        "the rolled back transaction's records"
    );
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
            reserve: 0,
            deleted: false,
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
            run(&self.io, || self.tree.load(&mut c));
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

    /// Every key `txn`'s current statement sees, in key order, its rows' values never asked for.
    fn keys(&self, txn: &mut Txn) -> Vec<String> {
        let mut c = VersionedCursor::new();
        let mut found = run(&self.io, || {
            self.tree
                .seek(self.txns(), &self.versions, txn, &mut c, b"", Seek::Ge)
        });
        let mut out = Vec::new();
        while found {
            assert!(c.unread(), "{:?}: its value out of line was read", c.key());
            out.push(String::from_utf8(c.key().unwrap().to_vec()).unwrap());
            found = run(&self.io, || {
                self.tree.next(self.txns(), &self.versions, txn, &mut c)
            });
        }
        out
    }

    /// Makes page `pid` no page of any value: a read of the value that names it finds it gone.
    fn unvalue(&self, pid: PageId) {
        let mut page = run(&self.io, || self.db.pool().fix_exclusive(pid, None));
        let mut id = [0u8; 8];
        page.page().copy_to(page::HEADER_SIZE, &mut id);
        id[0] ^= 0xff;
        page.page_mut().write(page::HEADER_SIZE, &id);
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
fn deletes_and_changes_in_place_keep_what_snapshots_read() {
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
    assert!(head.deleted);
    assert_eq!(stored.value, StoredValue::Inline(b"value-AAAA".to_vec()));
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
    fn root_as_of(&self, at: Ts) -> Vec<u8> {
        self.as_of(&self.page(self.tree.root()), at)
    }

    /// The leaf `page` as of the snapshot taken at `at`.
    fn as_of(&self, page: &[u8], at: Ts) -> Vec<u8> {
        let mut as_of = AsOf::new(page, at).unwrap();
        run(&self.io, || as_of.step(self.txns(), self.db.pool()));
        as_of.image().unwrap()
    }

    /// A copy of page `pid`.
    fn page(&self, pid: PageId) -> Vec<u8> {
        let page = run(&self.io, || self.db.pool().fix_exclusive(pid, None));
        let mut bytes = vec![0u8; page.page().len()];
        page.page().copy_to(0, &mut bytes);
        bytes
    }

    /// The tree's leaves, leftmost first, copied, its every node checked.
    fn leaves(&self) -> Vec<Vec<u8>> {
        let mut check = crate::storage::atoll::btree::Check::default();
        let stats = run(&self.io, || self.tree.tree().check(&mut check));
        stats
            .pages
            .into_iter()
            .map(|pid| self.page(pid))
            .filter(|p| node::View::new(&p[..]).is_leaf())
            .collect()
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

    let image = db.root_as_of(at);
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
fn an_open_shrink_keeps_its_room_so_its_leaf_as_of_a_snapshot_fits_and_its_rollback_splits_nothing()
{
    use crate::storage::atoll::btree::node::View;
    use crate::storage::atoll::btree::SPLITS;
    let splits = || SPLITS.with(|n| n.get());
    let db = open();
    let wide = |s: &str| padded(s, 700);
    let mut setup = db.begin(Isolation::Snapshot);
    for key in ["x0", "x1", "x2", "x3"] {
        db.put(&mut setup, key, &wide(key), Mode::Insert);
    }
    db.commit(setup);
    // An update that shrinks every row, still open, and inserts committed after it that would fit
    // in the room it freed.
    let mut shrink = db.begin(Isolation::Snapshot);
    for key in ["x0", "x1", "x2", "x3"] {
        db.put(&mut shrink, key, "-", Mode::Update);
    }
    let before = splits();
    let mut fill = db.begin(Isolation::Snapshot);
    for key in ["y0", "y1", "y2", "y3"] {
        db.put(&mut fill, key, &wide(key), Mode::Insert);
    }
    db.commit(fill);
    assert!(
        splits() > before,
        "the inserts took the room the shrink keeps"
    );
    let mut reader = db.begin(Isolation::Snapshot);
    let at = reader.snapshot;
    let leaves = db.leaves();
    assert!(leaves.len() > 1);
    let mut keys = Vec::new();
    for leaf in &leaves {
        for e in View::new(&db.as_of(leaf, at)[..]).entries().unwrap() {
            let key = String::from_utf8(e.key).unwrap();
            assert_eq!(
                db.get(&mut reader, &key).as_deref().map(str::as_bytes),
                Some(&e.value[HEADER..]),
                "{key}"
            );
            keys.push(key);
        }
    }
    assert_eq!(keys, ["x0", "x1", "x2", "x3", "y0", "y1", "y2", "y3"]);
    let before = splits();
    db.rollback(shrink);
    assert_eq!(splits(), before, "every row put back where it was");
    db.leaves();
    let mut after = db.begin(Isolation::Snapshot);
    for key in ["x0", "x1", "x2", "x3"] {
        assert_eq!(db.get(&mut after, key), Some(wide(key)), "{key}");
    }
}

#[test]
fn a_reserve_is_given_back_once_every_new_snapshot_sees_the_version_that_made_it() {
    use crate::storage::atoll::btree::node::View;
    use crate::storage::atoll::btree::SPLITS;
    let splits = || SPLITS.with(|n| n.get());
    let db = open();
    let wide = |s: &str| padded(s, 700);
    let mut setup = db.begin(Isolation::Snapshot);
    for key in ["x0", "x1", "x2", "x3"] {
        db.put(&mut setup, key, &wide(key), Mode::Insert);
    }
    db.commit(setup);
    let mut shrink = db.begin(Isolation::Snapshot);
    for key in ["x0", "x1", "x2", "x3"] {
        db.put(&mut shrink, key, "-", Mode::Update);
    }
    db.commit(shrink);
    let reserved = |db: &Db| View::new(&db.page(db.tree.root())[..]).reserved();
    let one = (HEADER + wide("x0").len()) - (HEADER + 1);
    assert_eq!(reserved(&db), 4 * one, "kept until a write needs the room");
    // A version put back that every snapshot sees keeps no reserve.
    let mut grow = db.begin(Isolation::Snapshot);
    db.put(&mut grow, "x0", &wide("x0"), Mode::Update);
    db.rollback(grow);
    assert_eq!(reserved(&db), 3 * one);
    let before = splits();
    let mut fill = db.begin(Isolation::Snapshot);
    for key in ["y0", "y1", "y2", "y3"] {
        db.put(&mut fill, key, &wide(key), Mode::Insert);
    }
    db.commit(fill);
    assert_eq!(splits(), before, "the room given back, not split for");
    assert_eq!(reserved(&db), 0);
    let mut reader = db.begin(Isolation::Snapshot);
    for key in ["x0", "x1", "x2", "x3"] {
        assert_eq!(db.get(&mut reader, key).as_deref(), Some("-"), "{key}");
    }
    for key in ["y0", "y1", "y2", "y3"] {
        assert_eq!(db.get(&mut reader, key), Some(wide(key)), "{key}");
    }
    db.leaves();
}

#[test]
fn a_write_keeps_the_reserve_of_its_own_version_and_of_one_its_statement_does_not_see() {
    let db = open();
    let reserve_of = |key: &[u8]| {
        let stored = run(&db.io, || db.tree.tree().stored(key)).unwrap();
        Version::decode(&stored.head).unwrap().reserve as usize
    };
    let mut setup = db.begin(Isolation::Snapshot);
    db.put(&mut setup, "x", &padded("x", 700), Mode::Insert);
    db.put(&mut setup, "o", &padded("o", 700), Mode::Insert);
    db.commit(setup);
    // Shrunk twice by one transaction: the second keeps the room the first kept.
    let mut own = db.begin(Isolation::Snapshot);
    db.put(&mut own, "o", &padded("o", 300), Mode::Update);
    db.txns().statement(&mut own).unwrap();
    db.put(&mut own, "o", "-", Mode::Update);
    assert_eq!(reserve_of(b"o"), (HEADER + 701) - (HEADER + 1));
    db.rollback(own);
    let mut w = db.begin(Isolation::ReadCommitted);
    let mut s = db.begin(Isolation::Snapshot);
    assert_eq!(db.put(&mut s, "x", "-", Mode::Update), DONE_UPDATED);
    db.commit(s);
    // Committed after `w`'s statement began, so not what it sees, yet written over.
    assert_eq!(db.put(&mut w, "x", "--", Mode::Update), DONE_UPDATED);
    let stored = run(&db.io, || db.tree.tree().stored(b"x")).unwrap();
    let reserve = |len: usize| (HEADER + 701) - (HEADER + len);
    assert_eq!(
        Version::decode(&stored.head).unwrap().reserve as usize,
        reserve(2),
        "room for the 701-byte row below the version it replaced"
    );
    db.rollback(w);
}

/// A move rests on a row whose value is out of line without reading it, so a scan of keys reads
/// no value page, and a newest version the reader does not see has its value never read: here the
/// pages of that version, and of a row the reader does see, are no value's, and the scan still
/// finds every key. A row's value is read when asked for: a version below, through its index
/// entry, whole; the row whose pages are gone, found gone.
#[test]
fn a_move_reads_no_value_out_of_line_until_its_row_is_asked_for() {
    let db = open();
    let big = |tag: &str| padded(tag, LARGE);
    let mut setup = db.begin(Isolation::Snapshot);
    for key in ["a", "b", "c"] {
        assert_eq!(
            db.put(&mut setup, key, &big(key), Mode::Insert),
            DONE_INSERTED
        );
    }
    db.commit(setup);
    let mut reader = db.begin(Isolation::Snapshot);
    assert_eq!(db.get_bytes(&mut reader, "a"), Some(big("a").into_bytes()));
    let mut w = db.begin(Isolation::Snapshot);
    assert_eq!(db.put(&mut w, "b", &big("b2"), Mode::Update), DONE_UPDATED);
    db.commit(w);
    let (b2, c) = (db.pages_of("b"), db.pages_of("c"));
    assert!(!b2.is_empty() && !c.is_empty(), "values in their leaf");
    for &pid in b2.iter().chain(&c) {
        db.unvalue(pid);
    }
    assert_eq!(db.keys(&mut reader), ["a", "b", "c"]);
    let mut cursor = VersionedCursor::new();
    for (key, value) in [("a", Some(big("a"))), ("b", Some(big("b"))), ("c", None)] {
        let found = run(&db.io, || {
            db.tree.seek(
                db.txns(),
                &db.versions,
                &mut reader,
                &mut cursor,
                key.as_bytes(),
                Seek::Ge,
            )
        });
        assert!(
            found && cursor.key() == Some(key.as_bytes()) && cursor.unread(),
            "{key}"
        );
        let loaded = drive(&db.io, || db.tree.load(&mut cursor));
        match value {
            Some(v) => {
                loaded.unwrap();
                assert_eq!(cursor.value(), Some(v.as_bytes()), "{key}");
            }
            None => assert!(
                matches!(loaded, Err(LimboError::Corrupt(_))),
                "{key}: {loaded:?}"
            ),
        }
    }
    db.commit(reader);
}

/// A row's value read out of line is lent whole and taken back: while the cursor stays it is the
/// row's value again; once the cursor has moved, only an allocation for the next row's.
#[test]
fn a_row_read_out_of_line_is_lent_whole_and_taken_back_while_the_cursor_stays() {
    let db = open();
    let big = |tag: &str| padded(tag, LARGE);
    let mut setup = db.begin(Isolation::Snapshot);
    for key in ["a", "b"] {
        assert_eq!(
            db.put(&mut setup, key, &big(key), Mode::Insert),
            DONE_INSERTED
        );
    }
    db.commit(setup);
    let mut reader = db.begin(Isolation::Snapshot);
    let mut c = VersionedCursor::new();
    assert!(run(&db.io, || {
        db.tree
            .seek(db.txns(), &db.versions, &mut reader, &mut c, b"", Seek::Ge)
    }));
    assert!(c.lend_value().is_none(), "lent before it is read");
    run(&db.io, || db.tree.load(&mut c));
    let lent = c.lend_value().expect("a value read");
    assert_eq!(lent, big("a").as_bytes());
    assert!(c.lend_value().is_none(), "lent twice");
    c.give_back(lent);
    assert_eq!(c.value(), Some(big("a").as_bytes()));
    let lent = c.lend_value().expect("a value given back");
    assert!(run(&db.io, || db.tree.next(
        db.txns(),
        &db.versions,
        &mut reader,
        &mut c
    )));
    assert!(c.unread(), "the next row read before it is asked for");
    c.give_back(lent);
    assert!(
        c.unread(),
        "a value given back after a move is the next row's"
    );
    run(&db.io, || db.tree.load(&mut c));
    assert_eq!(c.value(), Some(big("b").as_bytes()));
    db.commit(reader);
}

/// A walk over the versions a statement does not see reads each one's value out of line whole,
/// through the leaf that holds it: an open writer's version may lose its pages under a later read.
#[test]
fn an_unseen_walk_reads_each_version_out_of_line_whole() {
    let db = open();
    let big = |tag: &str| padded(tag, LARGE);
    let mut reader = db.begin(Isolation::Snapshot);
    assert_eq!(db.get_bytes(&mut reader, "k"), None);
    let mut w = db.begin(Isolation::Snapshot);
    assert_eq!(
        db.put(&mut w, "k", &big("open"), Mode::Insert),
        DONE_INSERTED
    );
    let mut c = VersionedCursor::new();
    // A move first, which leaves the row's value unread, then the walk.
    assert!(!run(&db.io, || {
        db.tree
            .seek(db.txns(), &db.versions, &mut reader, &mut c, b"", Seek::Ge)
    }));
    assert!(run(&db.io, || db.tree.unseen_first(
        db.txns(),
        &db.versions,
        &mut reader,
        &mut c
    )));
    assert!(!c.unread());
    assert_eq!(c.key(), Some(&b"k"[..]));
    assert_eq!(c.value(), Some(big("open").as_bytes()));
    db.rollback(w);
    db.commit(reader);
}

impl Db {
    /// Moves `c` for `txn`'s current statement, by a seek from `key` or a step: the key it rests
    /// on.
    fn move_to(
        &self,
        txn: &mut Txn,
        c: &mut VersionedCursor,
        to: Option<(&str, Seek)>,
        forward: bool,
    ) -> Option<String> {
        let found = run(&self.io, || match to {
            Some((key, seek)) => {
                self.tree
                    .seek(self.txns(), &self.versions, txn, c, key.as_bytes(), seek)
            }
            None if forward => self.tree.next(self.txns(), &self.versions, txn, c),
            None => self.tree.prev(self.txns(), &self.versions, txn, c),
        });
        found.then(|| String::from_utf8(c.key().unwrap().to_vec()).unwrap())
    }
}

/// A step takes the entries it copied ahead of it only while they are what its statement reads:
/// a key the transaction writes past it since is found, a step back returns to the key before,
/// and a seek elsewhere moves on from where it lands.
#[test]
fn a_step_takes_entries_copied_ahead_only_while_they_are_what_its_statement_reads() {
    let db = open();
    let mut setup = db.begin(Isolation::Snapshot);
    for key in ["k00", "k02", "k04", "k06", "k08", "k10", "k12"] {
        assert_eq!(db.put(&mut setup, key, "v", Mode::Insert), DONE_INSERTED);
    }
    db.commit(setup);
    let mut t = db.begin(Isolation::Snapshot);
    let mut c = VersionedCursor::new();
    let to = |t: &mut Txn, c: &mut VersionedCursor, at, forward| db.move_to(t, c, at, forward);
    assert_eq!(
        to(&mut t, &mut c, Some(("", Seek::Ge)), true).as_deref(),
        Some("k00")
    );
    // Each batch taken to its end doubles the next: k02 copies k04, k06 copies k08 and k10.
    for key in ["k02", "k04", "k06"] {
        assert_eq!(to(&mut t, &mut c, None, true).as_deref(), Some(key));
    }
    assert_eq!(db.put(&mut t, "k07", "own", Mode::Insert), DONE_INSERTED);
    assert_eq!(
        to(&mut t, &mut c, None, true).as_deref(),
        Some("k07"),
        "its own write"
    );
    assert_eq!(to(&mut t, &mut c, None, true).as_deref(), Some("k08"));
    assert_eq!(
        to(&mut t, &mut c, None, false).as_deref(),
        Some("k07"),
        "a step back"
    );
    assert_eq!(to(&mut t, &mut c, None, true).as_deref(), Some("k08"));
    assert_eq!(
        to(&mut t, &mut c, Some(("k01", Seek::Ge)), true).as_deref(),
        Some("k02")
    );
    assert_eq!(
        to(&mut t, &mut c, None, true).as_deref(),
        Some("k04"),
        "after a seek"
    );
    db.commit(t);
}
