//! Replay against the database that published the changes (audit 17): a workload's committed
//! changes, read back from its log and replayed into a fresh database, leave every tree as the
//! first one holds it. And a transaction's parts are joined in order, or refused out of it.

use super::*;
use crate::io::{MemoryIO, IO};
use crate::storage::atoll::btree::{Cursor, Seek};
use crate::storage::atoll::db::AtollConfig;
use crate::storage::atoll::log::{Log, BLOCK};
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
    drive(&io, || {
        log.wait_durable(
            &mut crate::storage::atoll::log::LogWait::default(),
            log.end(),
        )
    })
    .unwrap();
    let mut reader = Log::reader(io.clone(), path, &keeping_the_log().log, 0);
    let mut gather = Gather::new(0);
    let mut rereader = Log::rereader(io.clone(), path, &keeping_the_log().log);
    let (mut bodies, mut spilled) = (Vec::new(), 0);
    // Each spilled record's blocks, by where it starts: all a read back of it may read.
    let mut blocks = std::collections::HashMap::new();
    // Per transaction, its parts so far: a transaction spills each chunk it fills, at least as
    // many as its changes need past the one its Commit carries.
    let chunk = keeping_the_log().log.chunk_bytes as usize
        - crate::storage::atoll::txn::changes::FRONT
        - crate::storage::atoll::txn::changes::BACK;
    let mut parts = std::collections::HashMap::<u64, usize>::new();
    while let Some(r) = reader.next_record().unwrap() {
        if r.header.kind == SPILL {
            spilled += 1;
            blocks.insert(
                r.start,
                r.lsn.next_multiple_of(BLOCK) - r.start / BLOCK * BLOCK,
            );
            let start = u64::from_le_bytes(r.payload[..8].try_into().unwrap());
            *parts.entry(start).or_default() += 1;
        }
        if let Some((_, body)) = gather
            .record(r.header.kind, r.header.txn, r.start, &r.payload)
            .unwrap()
        {
            if r.header.kind == COMMIT {
                let n = parts.remove(&r.header.txn).unwrap_or(0);
                assert!(
                    n + 1 >= (body.len() as usize).div_ceil(chunk),
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
    let mut roots: Vec<PageId> = Vec::new();
    for b in &bodies {
        b.changes(&mut rereader, &mut |c| {
            if let Change::Create { root, .. } = c {
                roots.push(root);
            }
            Ok(())
        })
        .unwrap();
    }
    a.store().reserve(roots.iter().copied());
    let mut apply = Apply::new(io.clone(), a.pool().clone(), a.store().clone());
    for body in &bodies {
        apply.apply(body, &mut rereader).unwrap();
    }
    a.store().release_reserved();
    // Each part a body kept is read back twice, once per pass, and nothing else is read.
    let kept: u64 = bodies
        .iter()
        .flat_map(|b| &b.parts)
        .map(|p| blocks[&p.at])
        .sum();
    assert!(kept > 0, "no part kept");
    assert_eq!(rereader.read(), 2 * kept, "bytes read back");

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

/// Records fed to a gather at positions of their own, as a log holds them, and read back by them.
#[derive(Default)]
struct Fed {
    records: Vec<Vec<u8>>,
    reads: std::cell::Cell<usize>,
}

impl Fed {
    fn record(
        &mut self,
        g: &mut Gather,
        kind: crate::storage::atoll::log::Kind,
        txn: u64,
        payload: Vec<u8>,
    ) -> Result<Option<(Option<u64>, Body)>> {
        self.records.push(payload);
        let at = self.records.len() - 1;
        g.record(kind, txn, at as u64, &self.records[at])
    }

    /// `body`'s changes, each in its debug form, its parts read back from what was fed.
    fn changes(&self, body: &Body) -> Vec<String> {
        let mut out = Vec::new();
        body.changes(&mut FedParts(self), &mut |c| {
            out.push(format!("{c:?}"));
            Ok(())
        })
        .unwrap();
        out
    }
}

/// What was fed, read back by position.
struct FedParts<'a>(&'a Fed);

impl Parts for FedParts<'_> {
    fn payload(&mut self, at: Lsn, len: usize) -> Result<&[u8]> {
        self.0.reads.set(self.0.reads.get() + 1);
        let record = &self.0.records[at as usize];
        assert_eq!(record.len(), len, "the payload's length at {at}");
        Ok(record)
    }
}

/// Parts a run never has.
struct NoParts;

impl Parts for NoParts {
    fn payload(&mut self, at: Lsn, _: usize) -> Result<&[u8]> {
        unreachable!("a part at {at} in a run")
    }
}

fn decoded(bytes: &[u8]) -> Vec<String> {
    Decode::new(bytes)
        .map(|c| format!("{:?}", c.unwrap()))
        .collect()
}

/// A transaction's spilled parts are joined in order with its Commit's, each cutting what was
/// joined at its own start, refused past a gap, and left behind with no Commit; one whose first
/// part begins past its start is skipped at or below the snapshot and refused above it; a run is
/// its own body; a record of another kind is none. A body reads each part back once as its changes
/// are handed over, a change running over a part's end, its head included, handed over whole.
#[test]
fn parts_are_joined_by_position_and_refused_past_a_gap() {
    let mut changes = Changes::new();
    let root = PageId::new(SizeClass::new(0).unwrap(), 9);
    changes.put(1, root, b"k", &[7; 100]);
    changes.put(1, root, b"l", &[8; 60]);
    let bytes = changes.bytes().to_vec();
    let want = decoded(&bytes);
    let (a, b) = bytes.split_at(40);
    let spill = |start: u64, from: u64, part: &[u8]| {
        [&start.to_le_bytes()[..], &from.to_le_bytes(), part].concat()
    };
    let commit =
        |from: u64, part: &[u8]| [&5u64.to_le_bytes()[..], &from.to_le_bytes(), part].concat();
    let mut f = Fed::default();
    let mut g = Gather::new(0);
    assert_eq!(f.record(&mut g, SPILL, 0, spill(3, 0, a)).unwrap(), None);
    assert_eq!(g.unfinished(), 1);
    let (ts, body) = f
        .record(&mut g, COMMIT, 3, commit(a.len() as u64, b))
        .unwrap()
        .unwrap();
    assert_eq!(
        (ts, f.changes(&body)),
        (Some(5), want.clone()),
        "joined, with the Commit's timestamp"
    );
    assert_eq!(f.reads.get(), 1, "its one part read back once");
    assert_eq!(g.unfinished(), 0);
    // Parts cut inside the first change's head and inside the second change.
    let (h, rest) = bytes.split_at(5);
    let (m, t) = rest.split_at(115);
    f.record(&mut g, SPILL, 0, spill(9, 0, h)).unwrap();
    f.record(&mut g, SPILL, 0, spill(9, 5, m)).unwrap();
    let (_, body) = f
        .record(&mut g, COMMIT, 9, commit(120, t))
        .unwrap()
        .unwrap();
    assert_eq!(f.changes(&body), want, "changes across parts' ends");
    f.record(&mut g, SPILL, 0, spill(4, 0, a)).unwrap();
    assert!(
        f.record(&mut g, SPILL, 0, spill(4, a.len() as u64 + 8, a))
            .is_err(),
        "a part past a gap"
    );
    // Its first part before the redo start: nothing of it is kept, and its Commit is skipped at or
    // below the snapshot, refused above it.
    for snapshot in [5, 4] {
        let mut g = Gather::new(snapshot);
        assert_eq!(f.record(&mut g, SPILL, 0, spill(7, 8, a)).unwrap(), None);
        let got = f.record(&mut g, COMMIT, 7, commit(a.len() as u64 + 8, b));
        match snapshot {
            5 => assert_eq!(got.unwrap(), Some((Some(5), Body::default()))),
            _ => assert!(
                got.is_err(),
                "a commit above the snapshot, its first part missing"
            ),
        }
        assert_eq!(g.unfinished(), 0);
    }
    let mut g = Gather::new(4);
    assert!(
        f.record(&mut g, COMMIT, 8, commit(8, b)).is_err(),
        "a Commit past its start with no part, above the snapshot"
    );
    // A statement rolled back after its part was spilled: what follows begins where it began.
    let mut g = Gather::new(0);
    f.record(&mut g, SPILL, 0, spill(4, 0, a)).unwrap();
    f.record(&mut g, SPILL, 0, spill(4, a.len() as u64, &[9; 30]))
        .unwrap();
    let (_, body) = f
        .record(&mut g, COMMIT, 4, commit(a.len() as u64, b))
        .unwrap()
        .unwrap();
    assert_eq!(
        f.changes(&body),
        want,
        "the rolled-back part cut by the Commit's"
    );
    let mut g = Gather::new(0);
    f.record(&mut g, SPILL, 0, spill(6, 0, &[9; 40])).unwrap();
    let (_, body) = f
        .record(&mut g, COMMIT, 6, commit(0, &bytes))
        .unwrap()
        .unwrap();
    assert_eq!(
        f.changes(&body),
        want,
        "a Commit beginning at 0 over a part rolled back"
    );
    let (ts, body) = f.record(&mut g, RUN, 0, bytes.clone()).unwrap().unwrap();
    assert_eq!((ts, f.changes(&body)), (None, want));
    assert_eq!(
        f.record(
            &mut g,
            crate::storage::atoll::checkpoint::META_INTENT,
            0,
            Vec::new()
        )
        .unwrap(),
        None
    );
}

/// Parts with no Commit yet are kept as where they lie, never their bytes, however many (audit
/// 2026-11-06): their Commit's body reads each back once from the log, and what it hands over is
/// what the log holds then, not a copy taken as the part went by.
#[test]
fn parts_kept_for_a_commit_are_read_back_at_it_never_held() {
    let root = PageId::new(SizeClass::new(0).unwrap(), 9);
    let part = |i: u32, fill: u8| {
        let mut c = Changes::new();
        c.put(1, root, &i.to_be_bytes(), &[fill; 4000]);
        c.bytes().to_vec()
    };
    let mut f = Fed::default();
    let mut g = Gather::new(0);
    let mut from = 0u64;
    for i in 0..256u32 {
        let bytes = part(i, 1);
        let payload = [&3u64.to_le_bytes()[..], &from.to_le_bytes(), &bytes].concat();
        from += bytes.len() as u64;
        assert_eq!(f.record(&mut g, SPILL, 0, payload).unwrap(), None);
    }
    assert_eq!(g.unfinished(), 1);
    // The log's parts, rewritten in place after the gather went past them: only a read back sees
    // the new bytes.
    let mut want = Vec::new();
    for (i, record) in f.records.iter_mut().enumerate() {
        let bytes = part(i as u32, 2);
        record[16..].copy_from_slice(&bytes);
        want.extend(decoded(&bytes));
    }
    let commit = [&5u64.to_le_bytes()[..], &from.to_le_bytes()].concat();
    let (_, body) = f.record(&mut g, COMMIT, 3, commit).unwrap().unwrap();
    assert_eq!(body.len(), from);
    assert_eq!(f.changes(&body), want);
    assert_eq!(f.reads.get(), 256, "each part read back once");
}

/// A transaction's parts are joined at their end one at a time, and a statement rolled back cuts
/// them where its next part begins (audit 2026-11-06 follow-up): 2^16 parts, the audit's case, then
/// a part beginning halfway back, keep the first half and it.
#[test]
fn many_parts_join_end_to_end_and_a_cut_drops_those_past_it() {
    let n = 1u64 << 16;
    let mut f = Fed::default();
    let mut g = Gather::new(0);
    let spill = |from: u64| [&3u64.to_le_bytes()[..], &from.to_le_bytes(), &[0]].concat();
    for from in 0..n {
        assert_eq!(f.record(&mut g, SPILL, 0, spill(from)).unwrap(), None);
    }
    assert_eq!(f.record(&mut g, SPILL, 0, spill(n / 2)).unwrap(), None);
    let commit = [&5u64.to_le_bytes()[..], &(n / 2 + 1).to_le_bytes()].concat();
    let (_, body) = f.record(&mut g, COMMIT, 3, commit).unwrap().unwrap();
    let at: Vec<Lsn> = body.parts.iter().map(|p| p.at).collect();
    let want: Vec<Lsn> = (0..n / 2).chain([n]).collect();
    assert_eq!(at, want, "the parts kept");
    assert_eq!(body.len(), n / 2 + 1);
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
    let none = &mut NoParts;
    apply
        .apply(&Body::whole(made.bytes().to_vec()), none)
        .unwrap();
    assert_eq!(contents(&db, root, false).len(), 200);
    apply
        .apply(&Body::whole(drained.bytes().to_vec()), none)
        .unwrap();
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
    apply
        .apply(&Body::whole(stray.bytes().to_vec()), none)
        .unwrap();
    assert_eq!(
        contents(&db, schema, false),
        held,
        "a tree replay never made"
    );
}

/// A checkpoint's cut that read the log's end and met a committing slot's mark only once the
/// commit cleared it (audit 2026-11-06): its redo start lies past the commit's spilled parts and
/// before its Commit, its timestamp at or below the snapshot. Read from there, the commit is
/// skipped; at a snapshot below it, it is refused; read from the log's start, it is whole.
#[test]
fn a_commit_at_or_below_the_snapshot_whose_parts_precede_the_redo_start_is_skipped() {
    use crate::storage::atoll::log::LogConfig;
    use crate::storage::atoll::txn::{Clock, Commit, Transactions, TxnConfig};
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let path = "/db/replay-cut";
    let cfg = LogConfig {
        segment_bytes: 1 << 20,
        buffer_bytes: 64 << 10,
        chunk_bytes: 8 << 10,
        writes_in_flight: 3,
        direct: true,
    };
    let log = Log::open(io.clone(), path, cfg.clone(), 0).unwrap();
    let txns = Transactions::new(
        &TxnConfig { slots: 4 },
        Arc::new(Clock::after(0)),
        log.clone(),
    );
    let root = PageId::new(SizeClass::new(0).unwrap(), 0);
    let mut t = drive(&io, || txns.begin(Isolation::Snapshot)).unwrap();
    txns.statement(&mut t).unwrap();
    t.changed(|s, c| c.plain_put(s, root, b"key", &[7; 20_000]));
    drive(&io, || txns.spill(&mut t)).unwrap();
    assert!(!t.changes().parts().is_empty(), "no part spilled");
    let redo = log.end();
    let mut op = Commit::new(t);
    let ts = drive(&io, || txns.commit(&mut op)).unwrap().unwrap();
    drive(&io, || {
        log.wait_durable(
            &mut crate::storage::atoll::log::LogWait::default(),
            log.end(),
        )
    })
    .unwrap();
    assert!(redo < log.end(), "the Commit past the redo start");
    let read = |from, snapshot| -> Result<Vec<(Option<u64>, Body)>> {
        let mut g = Gather::new(snapshot);
        let mut reader = Log::reader(io.clone(), path, &cfg, from);
        let mut bodies = Vec::new();
        while let Some(r) = reader.next_record()? {
            bodies.extend(g.record(r.header.kind, r.header.txn, r.start, &r.payload)?);
        }
        assert_eq!(g.unfinished(), 0, "parts kept");
        Ok(bodies)
    };
    assert_eq!(read(redo, ts).unwrap(), vec![(Some(ts), Body::default())]);
    assert!(
        read(redo, ts - 1).is_err(),
        "a commit above the snapshot, its parts before the start"
    );
    let whole = read(0, ts - 1).unwrap();
    assert_eq!(whole.len(), 1);
    let mut puts = Vec::new();
    let mut parts = Log::rereader(io.clone(), path, &cfg);
    whole[0]
        .1
        .changes(&mut parts, &mut |c| {
            match c {
                Change::Put { key, value, .. } => puts.push((key.to_vec(), value.len())),
                other => panic!("{other:?}"),
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(puts, vec![(b"key".to_vec(), 20_000)]);
    // The parts' own blocks read, as the log holds them, and nothing past them.
    let mut reader = Log::reader(io.clone(), path, &cfg, 0);
    let mut blocks = 0;
    while let Some(r) = reader.next_record().unwrap() {
        if r.header.kind == SPILL {
            blocks += r.lsn.next_multiple_of(BLOCK) - r.start / BLOCK * BLOCK;
        }
    }
    assert!(blocks > 0);
    assert_eq!(parts.read(), blocks, "bytes read back");
}
