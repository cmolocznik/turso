//! Replay against the database that published the changes (audit 17): a workload's committed
//! changes, read back from its log and replayed into a fresh database, leave every tree as the
//! first one holds it. And a transaction's parts are joined in order, or refused out of it.

use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::{Cursor, Seek};
use crate::storage::atoll::db::AtollConfig;
use crate::storage::atoll::log::Log;
use crate::storage::atoll::page::SizeClass;
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::sql::catalog::tests::config;
use crate::storage::atoll::sql::catalog::{Entry, Layout, System};
use crate::storage::atoll::sql::AtollDb;
use crate::storage::atoll::txn::changes::Changes;
use crate::storage::atoll::txn::versioned::{VersionedCursor, VersionedTree};
use crate::storage::atoll::txn::Isolation;
use crate::{ConnectOpts, Database, OpenOptions, SqliteDialect};

/// The test configuration with a log budget no checkpoint reaches, so every segment stays.
fn keeping_the_log() -> AtollConfig {
    let mut c = config();
    c.log_budget = Some(1 << 40);
    c
}

/// Every entry of the tree at `root` as a snapshot taken now reads it: a versioned tree's by the
/// version it sees, its value past the header; a plain tree's as stored.
fn contents(db: &AtollDb, root: PageId, plain: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (io, a) = (db.io(), db.atoll());
    let mut out = Vec::new();
    if plain {
        let tree = Tree::open(io.clone(), a.pool().clone(), a.store().clone(), root);
        let mut c = Cursor::new();
        let mut found = drive(io, || tree.seek(&mut c, &[], Seek::Ge)).unwrap();
        while found {
            out.push((c.key().unwrap().to_vec(), c.value().unwrap().to_vec()));
            found = drive(io, || tree.next(&mut c)).unwrap();
        }
        return out;
    }
    let txns = a.transactions();
    let mut txn = drive(io, || txns.begin(Isolation::Snapshot)).unwrap();
    txns.statement(&mut txn).unwrap();
    if !txn.snapped() {
        txns.take_snapshot(&mut txn);
    }
    let tree = VersionedTree::open(io.clone(), a.pool().clone(), a.store().clone(), root);
    let mut c = VersionedCursor::new();
    let versions = db.versions();
    let mut found = drive(io, || {
        tree.seek(txns, versions, &mut txn, &mut c, &[], Seek::Ge)
    })
    .unwrap();
    while found {
        drive(io, || tree.load(&mut c)).unwrap();
        out.push((c.key().unwrap().to_vec(), c.value().unwrap().to_vec()));
        found = drive(io, || tree.next(txns, versions, &mut txn, &mut c)).unwrap();
    }
    txns.abort(txn);
    out
}

/// Every tree the database holds: the system trees, and every tree a catalog row names.
fn trees(db: &AtollDb) -> Vec<(PageId, bool)> {
    let mut out: Vec<(PageId, bool)> = System::ALL
        .iter()
        .map(|s| (s.root(), matches!(s, System::Abandoned | System::Sequences)))
        .collect();
    for (_, row) in contents(db, System::Catalog.root(), false) {
        let entry = Entry::decode(&row).unwrap();
        if entry.layout != Layout::Sequence {
            out.push((entry.root, false));
        }
    }
    out
}

/// A workload of every kind of change SQL makes, committed, rolled back and rolled back to a
/// savepoint, read back from the log and replayed into a fresh database: every tree the first
/// database holds, read at a snapshot taken after, holds the same entries in the second, and the
/// second holds no tree the first does not.
#[test]
fn replaying_the_published_changes_rebuilds_every_tree() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let path = "/db/replay-source";
    let db = Database::open(
        io.clone(),
        path,
        OpenOptions::new(Arc::new(SqliteDialect)).atoll(keeping_the_log()),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let run = |sql: &str| conn.execute(sql).unwrap();
    let long = "x".repeat(3000);
    run("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT, c TEXT)");
    run("CREATE INDEX tc ON t (c)");
    for i in 0..300 {
        let b = if i % 50 == 0 {
            long.clone()
        } else {
            format!("r{i}")
        };
        run(&format!("INSERT INTO t VALUES ({i}, '{b}', 'c{}')", i % 17));
    }
    run("BEGIN");
    run("UPDATE t SET b = b || '-u' WHERE a % 3 = 0");
    run("DELETE FROM t WHERE a % 5 = 0");
    run("COMMIT");
    run("BEGIN");
    run("INSERT INTO t VALUES (1000, 'kept', 'c')");
    run("SAVEPOINT s");
    run("INSERT INTO t VALUES (1001, 'rolled back to s', 'c')");
    run("UPDATE t SET b = 'rolled back to s' WHERE a = 1");
    run("ROLLBACK TO s");
    run("INSERT INTO t VALUES (1002, 'kept after s', 'c')");
    run("COMMIT");
    run("BEGIN");
    run("CREATE TABLE u (x)");
    run("INSERT INTO u VALUES (1), (2)");
    run("ROLLBACK");
    run("CREATE TABLE w (x)");
    run("INSERT INTO w VALUES (1), (2), (3)");
    run("DELETE FROM w");
    run("INSERT INTO w VALUES (4)");
    run("CREATE TABLE d (x)");
    run("INSERT INTO d VALUES (1)");
    run("DROP TABLE d");
    run("CREATE TABLE z AS SELECT a, b FROM t WHERE a < 100");
    run("CREATE TABLE s (a INTEGER PRIMARY KEY AUTOINCREMENT, b)");
    run("INSERT INTO s (b) VALUES ('one'), ('two')");
    run("BEGIN");
    for i in 2000..2400 {
        run(&format!(
            "INSERT INTO t VALUES ({i}, '{long}', 'c{}')",
            i % 17
        ));
    }
    run("COMMIT");
    let source = db.atoll.as_ref().expect("Atoll mode").clone();
    // A sequence created in a transaction, its value row put at once, then changed outside any.
    {
        use crate::storage::atoll::sql::sequence::{self as seq, value_key};
        let txns = source.atoll().transactions();
        let mut t = drive(&io, || txns.begin(Isolation::Snapshot)).unwrap();
        crate::storage::atoll::testing::share(txns, &mut t);
        txns.statement(&mut t).unwrap();
        let start = seq::Value {
            value: 1,
            is_called: false,
        };
        let mut op = seq::Create::new(900, start);
        drive(&io, || source.create_sequence(&mut t, &mut op)).unwrap();
        let key = value_key(900, t.start());
        let mut commit = crate::storage::atoll::txn::Commit::new(t);
        drive(&io, || txns.commit(&mut commit)).unwrap();
        for _ in 0..3 {
            let mut op = seq::Change::new(key);
            let next = &mut |v: seq::Value| {
                Ok(seq::Value {
                    value: v.value + 1,
                    is_called: true,
                })
            };
            drive(&io, || source.change_sequence(&mut op, next)).unwrap();
        }
    }

    // Changes published alone are durable with the log, not when made: wait for all of it.
    let log = source.atoll().log().clone();
    drive(&io, || log.wait_durable(log.end())).unwrap();
    let mut reader = Log::reader(io.clone(), path, &keeping_the_log().log, 0);
    let mut gather = Gather::new();
    let (mut bodies, mut spilled) = (Vec::new(), 0);
    // Per transaction, its parts so far: a transaction spills each chunk it fills, at least as
    // many as its changes need past the one its Commit carries.
    let chunk = keeping_the_log().log.chunk_bytes as usize
        - crate::storage::atoll::txn::changes::FRONT
        - crate::storage::atoll::txn::changes::BACK;
    let mut parts = std::collections::HashMap::<u64, usize>::new();
    while let Some(r) = reader.next_record().unwrap() {
        if r.header.kind == SPILL {
            spilled += 1;
            let start = u64::from_le_bytes(r.payload[..8].try_into().unwrap());
            *parts.entry(start).or_default() += 1;
        }
        if let Some((_, body)) = gather
            .record(r.header.kind, r.header.txn, &r.payload)
            .unwrap()
        {
            if r.header.kind == COMMIT {
                let n = parts.remove(&r.header.txn).unwrap_or(0);
                assert!(
                    n + 1 >= body.len().div_ceil(chunk),
                    "a commit of {} bytes spilled {n} parts",
                    body.len()
                );
            }
            bodies.push(body);
        }
    }
    assert_eq!(gather.unfinished(), 0, "parts with no Commit");
    assert!(spilled > 0, "the large transaction spilled no part");

    let replayed = AtollDb::open(io.clone(), "/db/replayed", keeping_the_log()).unwrap();
    let a = replayed.atoll();
    let roots: Vec<PageId> = bodies
        .iter()
        .flat_map(|b| {
            Decode::new(b).filter_map(|c| match c.unwrap() {
                Change::Create { root, .. } => Some(root),
                _ => None,
            })
        })
        .collect();
    a.store().reserve(roots.iter().copied());
    let mut apply = Apply::new(io.clone(), a.pool().clone(), a.store().clone());
    for body in &bodies {
        apply.apply(body).unwrap();
    }
    a.store().release_reserved();

    let held = trees(&source);
    assert!(held.len() > System::ALL.len() + 4, "{} trees", held.len());
    for &(root, plain) in &held {
        let (want, got) = (
            contents(&source, root, plain),
            contents(&replayed, root, plain),
        );
        let show = |e: &(Vec<u8>, Vec<u8>)| {
            let v = String::from_utf8_lossy(&e.1[..e.1.len().min(40)]).to_string();
            (e.0.clone(), v)
        };
        let first = want.iter().zip(&got).position(|(w, g)| w != g);
        assert!(
            want == got,
            "tree {root:?}: {} entries in the source, {} replayed; first differing {:?}",
            want.len(),
            got.len(),
            first.map(|i| (show(&want[i]), show(&got[i])))
        );
    }
    assert_eq!(trees(&replayed), held, "the trees each holds");
}

/// A transaction's spilled parts are joined in order with its Commit's, each cutting what was
/// joined at its own start, refused past a gap, and left behind with no Commit; a run is its own
/// body; a record of another kind is none.
#[test]
fn parts_are_joined_by_position_and_refused_past_a_gap() {
    let mut changes = Changes::new();
    let root = PageId::new(SizeClass::new(0).unwrap(), 9);
    changes.put(1, root, b"k", &[7; 100]);
    let bytes = changes.bytes().to_vec();
    let (a, b) = bytes.split_at(40);
    let spill = |start: u64, from: u64, part: &[u8]| {
        [&start.to_le_bytes()[..], &from.to_le_bytes(), part].concat()
    };
    let commit =
        |from: u64, part: &[u8]| [&5u64.to_le_bytes()[..], &from.to_le_bytes(), part].concat();
    let mut g = Gather::new();
    assert_eq!(g.record(SPILL, 0, &spill(3, 0, a)).unwrap(), None);
    assert_eq!(g.unfinished(), 1);
    assert_eq!(
        g.record(COMMIT, 3, &commit(a.len() as u64, b)).unwrap(),
        Some((Some(5), bytes.clone())),
        "joined, with the Commit's timestamp"
    );
    assert_eq!(g.unfinished(), 0);
    assert!(
        g.record(SPILL, 0, &spill(4, 8, a)).is_err(),
        "a part past a gap"
    );
    // A statement rolled back after its part was spilled: what follows begins where it began.
    let mut g = Gather::new();
    g.record(SPILL, 0, &spill(4, 0, a)).unwrap();
    g.record(SPILL, 0, &spill(4, a.len() as u64, &[9; 30]))
        .unwrap();
    assert_eq!(
        g.record(COMMIT, 4, &commit(a.len() as u64, b)).unwrap(),
        Some((Some(5), bytes.clone())),
        "the rolled-back part cut by the Commit's"
    );
    let mut g = Gather::new();
    g.record(SPILL, 0, &spill(6, 0, &[9; 40])).unwrap();
    assert_eq!(
        g.record(COMMIT, 6, &commit(0, &bytes)).unwrap(),
        Some((Some(5), bytes.clone())),
        "a Commit beginning at 0 over a part rolled back"
    );
    assert_eq!(
        g.record(RUN, 0, &bytes).unwrap(),
        Some((None, bytes.clone()))
    );
    assert_eq!(
        g.record(crate::storage::atoll::checkpoint::META_INTENT, 0, &[])
            .unwrap(),
        None
    );
}

/// Replay frees whole a tree it made and then reads drained, its root with it; a drain of a root
/// it never made only takes the root off its list, leaving whatever the page holds here, such as a
/// tree of the system's.
#[test]
fn a_drain_frees_only_a_tree_replay_made() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = AtollDb::open(io.clone(), "/db/replay-drain", keeping_the_log()).unwrap();
    let a = db.atoll();
    let root = PageId::new(SizeClass::new(0).unwrap(), 4000);
    let dropped = System::Dropped.root();
    let mut made = Changes::new();
    made.create(1, root, System::Abandoned.root());
    for i in 0..200u32 {
        made.put(1, root, &i.to_be_bytes(), &[7; 100]);
    }
    made.put(1, dropped, &root.raw().to_be_bytes(), b"dropped");
    let mut drained = Changes::new();
    drained.drained(root, dropped);
    a.store().reserve([root]);
    let mut apply = Apply::new(io.clone(), a.pool().clone(), a.store().clone());
    apply.apply(made.bytes()).unwrap();
    assert_eq!(contents(&db, root, false).len(), 200);
    apply.apply(drained.bytes()).unwrap();
    assert!(contents(&db, dropped, false).is_empty(), "off its list");
    assert_eq!(
        drive(db.io(), || a.store().entry(root)).unwrap(),
        FREE,
        "its root freed"
    );
    a.store().release_reserved();
    let schema = System::Schema.root();
    let held = contents(&db, schema, false);
    let mut stray = Changes::new();
    stray.drained(schema, System::Abandoned.root());
    apply.apply(stray.bytes()).unwrap();
    assert_eq!(
        contents(&db, schema, false),
        held,
        "a tree replay never made"
    );
}
