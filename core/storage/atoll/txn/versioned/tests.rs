use super::*;
use crate::io::MemoryIO;
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::{LogConfig, Lsn};
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::{Commit, TxnConfig};

const BASE: &str = "/db/versioned";

struct Db {
    io: Arc<dyn IO>,
    db: Atoll,
    versions: Versions,
    tree: VersionedTree,
}

fn open() -> Db {
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
        txn: TxnConfig { slots: 4 },
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
    let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), 4);
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

impl Db {
    fn txns(&self) -> &Transactions {
        self.db.transactions()
    }

    fn begin(&self, isolation: Isolation) -> Txn {
        let mut txn = run(&self.io, || self.txns().begin(isolation));
        self.txns().statement(&mut txn).unwrap();
        txn
    }

    fn get(&self, txn: &mut Txn, key: &str) -> Option<String> {
        run(&self.io, || {
            self.tree
                .get(self.txns(), &self.versions, txn, key.as_bytes())
        })
        .map(|v| String::from_utf8(v).unwrap())
    }

    fn put(&self, txn: &mut Txn, key: &str, value: &str, mode: Mode) -> Write {
        run(&self.io, || {
            self.tree.put(
                self.txns(),
                &self.versions,
                txn,
                key.as_bytes(),
                value.as_bytes(),
                mode,
            )
        })
    }

    fn delete(&self, txn: &mut Txn, key: &str) -> Write {
        run(&self.io, || {
            self.tree
                .delete(self.txns(), &self.versions, txn, key.as_bytes())
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
fn a_statement_reads_its_transactions_earlier_statements_and_not_its_own() {
    let db = open();
    let mut w = db.begin(Isolation::Snapshot);
    db.put(&mut w, "k", "s1", Mode::Insert);
    assert_eq!(db.get(&mut w, "k"), None, "its own statement's insert");
    db.txns().statement(&mut w).unwrap();
    assert_eq!(db.get(&mut w, "k").as_deref(), Some("s1"));
    db.put(&mut w, "k", "s2a", Mode::Update);
    db.put(&mut w, "k", "s2b", Mode::Update);
    assert_eq!(
        db.get(&mut w, "k").as_deref(),
        Some("s1"),
        "a row this statement changed twice reads as it was before it"
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
    assert_eq!(db.get(&mut w, "k").as_deref(), Some("t1"));
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
    let slot = b.slot();
    assert_eq!(
        db.put(&mut snapshot, "k", "s", Mode::Update),
        Write::Wait(slot)
    );
    assert_eq!(
        db.put(&mut committed, "k", "c", Mode::Update),
        Write::Wait(slot)
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
    let mut ends: Vec<Lsn> = Vec::new();
    for key in ["a", "b", "c"] {
        db.put(&mut w, key, "v", Mode::Insert);
        ends.push(w.last);
    }
    db.txns().statement(&mut w).unwrap();
    db.put(&mut w, "a", "w", Mode::Update);
    ends.push(w.last);
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
            chain.push((r.header.prev, r.lsn));
        }
    }
    let mut prev = 0;
    for (i, &(p, lsn)) in chain.iter().enumerate().take(4) {
        assert_eq!((p, lsn), (prev, ends[i]), "record {i}");
        prev = lsn;
    }
    assert_eq!(chain.len(), 5, "four changes and the commit");
}
