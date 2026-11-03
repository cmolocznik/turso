//! Rollback and undo under crashes (DESIGN.md §19): a crash image taken every few operations of a
//! workload recovers to exactly what was committed.

use super::*;
use crate::io::crash::CrashIO;
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET, WRITEBACK_BYTES};
use crate::storage::atoll::control::{Control, Superblock};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::page::SizeClass;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::txn::versioned::Write;
use crate::storage::atoll::txn::{Abort, Commit, Isolation, TxnConfig};
use std::collections::BTreeMap;

const BASE: &str = "/db/undo";

/// Long enough that a few hundred changes fill the log buffer, so it is flushed while they and
/// their rollback run, and the crash images fall between compensations; short enough to stay in
/// a leaf.
const VALUE: usize = 700;

fn config() -> AtollConfig {
    AtollConfig {
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
        txn: TxnConfig { slots: 4 },
    }
}

fn run<T>(io: &Arc<dyn IO>, mut op: impl FnMut() -> Result<IOResult<T>>) -> T {
    drive(io, || op()).unwrap()
}

fn value(tag: &str, i: usize) -> String {
    let head = format!("{tag}-{i}-");
    format!("{head}{}", "v".repeat(VALUE - head.len()))
}

struct Db {
    io: Arc<dyn IO>,
    db: Atoll,
    versions: Versions,
    /// Two trees of different node classes, so a transaction has a version index of each.
    trees: [VersionedTree; 2],
}

impl Db {
    fn create(io: Arc<dyn IO>) -> Self {
        let db = Atoll::open(io.clone(), BASE, config()).unwrap();
        let roots = [0, 1].map(|c| {
            run(&io, || {
                Tree::create(&io, db.pool(), db.store(), SizeClass::new(c).unwrap())
            })
        });
        Self::with(io, db, roots)
    }

    /// What a crash left, recovered.
    fn recover(io: Arc<dyn IO>, roots: [PageId; 2]) -> Self {
        let db = Atoll::open(io.clone(), BASE, config()).unwrap();
        assert!(db.recovered_end().is_some(), "no recovery");
        Self::with(io, db, roots)
    }

    fn with(io: Arc<dyn IO>, db: Atoll, roots: [PageId; 2]) -> Self {
        let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), 4);
        let trees = roots.map(|root| {
            VersionedTree::open(io.clone(), db.pool().clone(), db.store().clone(), root)
        });
        Self {
            io,
            db,
            versions,
            trees,
        }
    }

    fn roots(&self) -> [PageId; 2] {
        [self.trees[0].root(), self.trees[1].root()]
    }

    fn txns(&self) -> &Transactions {
        self.db.transactions()
    }

    fn begin(&self) -> Txn {
        let mut txn = run(&self.io, || self.txns().begin(Isolation::ReadCommitted));
        self.txns().statement(&mut txn).unwrap();
        txn
    }

    fn put(&self, txn: &mut Txn, tree: usize, key: &str, value: &str) {
        let write = run(&self.io, || {
            self.trees[tree].put(
                self.txns(),
                &self.versions,
                txn,
                key.as_bytes(),
                value.as_bytes(),
                Mode::Upsert,
            )
        });
        assert!(matches!(write, Write::Done(_)), "{key}: {write:?}");
    }

    fn delete(&self, txn: &mut Txn, tree: usize, key: &str) {
        run(&self.io, || {
            self.trees[tree].delete(self.txns(), &self.versions, txn, key.as_bytes())
        });
    }

    fn commit(&self, txn: Txn) {
        let mut op = Commit::new(txn);
        run(&self.io, || self.txns().commit(&mut op));
    }

    fn rollback(&self, txn: Txn) {
        let mut op = Abort::new(txn);
        run(&self.io, || self.txns().rollback(&self.versions, &mut op));
    }

    fn rollback_to(&self, txn: &mut Txn, to: u16) {
        let mut op = Rollback::after(to);
        run(&self.io, || {
            self.txns().rollback_to(&self.versions, txn, &mut op)
        });
    }

    fn durable(&self) {
        run(&self.io, || self.db.log().wait_durable(self.db.log().end()));
    }

    /// What a transaction begun now reads of `keys` in each tree.
    fn state(&self, keys: &[String]) -> BTreeMap<(usize, String), Option<String>> {
        let mut txn = self.begin();
        let mut out = BTreeMap::new();
        for tree in 0..2 {
            for key in keys {
                let v = run(&self.io, || {
                    self.trees[tree].get(self.txns(), &self.versions, &mut txn, key.as_bytes())
                });
                out.insert(
                    (tree, key.clone()),
                    v.map(|v| String::from_utf8(v).unwrap()),
                );
            }
        }
        self.commit(txn);
        out
    }

    fn checkpoint(&self) -> Superblock {
        let ticket = self.db.checkpointer().request_full();
        run(&self.io, || self.db.checkpointer().step_full(ticket));
        self.db.checkpointer().superblock()
    }
}

/// How many compensations of transaction `txn` the log of a crash image holds, read across the
/// gaps recoveries left after torn tails, as its newest superblock records them.
fn compensations_in(image: &CrashIO, txn: Ts) -> usize {
    let io: Arc<dyn IO> = Arc::new(image.crash(0));
    let (_, superblock) = Control::open(io.clone(), BASE).unwrap();
    let gaps = superblock.map_or_else(Vec::new, |s| s.continuations);
    let mut reader = Log::reader(io, BASE, &config().log, 0).following(&gaps);
    let mut n = 0;
    while let Some(r) = reader.next_record().unwrap() {
        let kind = r.header.kind;
        n += (r.header.txn == txn
            && (kind == crate::storage::atoll::btree::record::RESTORE
                || kind == crate::storage::atoll::btree::record::UNPUT)) as usize;
    }
    n
}

/// `keys` committed in both trees, durably.
fn committed(db: &Db, keys: &[String]) {
    let mut txn = db.begin();
    for (i, key) in keys.iter().enumerate() {
        for tree in 0..2 {
            db.put(&mut txn, tree, key, &value("committed", i));
        }
    }
    db.commit(txn);
}

/// What [`recover_all`] checked.
#[derive(Debug, Default)]
struct Recovered {
    recoveries: usize,
    /// Crashes in a recovery that fell between two of its own compensations.
    mid_undo: usize,
    /// Crashes in the recovery of one of those that fell between two of its own in turn.
    mid_undo_again: usize,
}

/// Where a recovery is crashed: just before each write to the log or the superblock completes,
/// the write in flight torn, and every few operations besides. A recovery's compensations reach
/// the log in a burst as its checkpoint forces it, so a crash between two of them is one in that
/// burst; the last images before the checkpoint is published hold them all, and none after it
/// reads them, since its redo point passed the gap they follow.
fn crashes_in_recovery(path: &str) -> bool {
    path == BASE || path.starts_with(&format!("{BASE}.log."))
}

/// Recovers every image to `expected`, and every `again`th one also through crashes while its
/// recovery undoes and checkpoints ([`crashes_in_recovery`]), each recovered in turn, and one of
/// them crashed again in its own recovery: the first that fell between two of the compensations
/// of `txn` that recovery logged, else the middle one.
#[allow(clippy::too_many_arguments)]
fn recover_all(
    images: Vec<CrashIO>,
    roots: [PageId; 2],
    keys: &[String],
    expected: &BTreeMap<(usize, String), Option<String>>,
    again: usize,
    txn: Ts,
    label: &str,
) -> Recovered {
    let between = |n: usize, from: usize, to: usize| from < n && n < to;
    let mut out = Recovered::default();
    for (i, image) in images.into_iter().enumerate() {
        let image = Arc::new(image);
        let crashed = (i % again == 0).then(|| image.crash(i as u64));
        let db = Db::recover(image, roots);
        assert_eq!(&db.state(keys), expected, "{label}: image {i}");
        out.recoveries += 1;
        drop(db);
        let Some(crashed) = crashed else {
            continue;
        };
        let crashed = Arc::new(crashed);
        let from = compensations_in(&crashed, txn);
        crashed.capture(crashes_in_recovery, 40, i as u64);
        drop(Db::recover(crashed.clone(), roots));
        let in_recovery = crashed.captured();
        assert!(
            !in_recovery.is_empty(),
            "{label}: image {i}: no image in recovery"
        );
        let counts: Vec<usize> = in_recovery
            .iter()
            .map(|image| compensations_in(image, txn))
            .collect();
        let to = counts.iter().copied().max().unwrap_or(from);
        out.mid_undo += counts.iter().filter(|&&n| between(n, from, to)).count();
        let deepest = counts
            .iter()
            .position(|&n| between(n, from, to))
            .unwrap_or(counts.len() / 2);
        for (j, image) in in_recovery.into_iter().enumerate() {
            let image = Arc::new(image);
            if j == deepest {
                // Crashed in recovery, and that recovery crashed in turn.
                image.capture(crashes_in_recovery, 40, j as u64);
                drop(Db::recover(image.clone(), roots));
                let deeper = image.captured();
                let again: Vec<usize> = deeper
                    .iter()
                    .map(|image| compensations_in(image, txn))
                    .collect();
                let to_again = again.iter().copied().max().unwrap_or(counts[j]);
                for (k, deeper) in deeper.into_iter().enumerate() {
                    out.mid_undo_again += (between(counts[j], from, to)
                        && between(again[k], counts[j], to_again))
                        as usize;
                    let db = Db::recover(Arc::new(deeper), roots);
                    assert_eq!(
                        &db.state(keys),
                        expected,
                        "{label}: image {i}, in recovery {j}, in recovery {k}"
                    );
                    out.recoveries += 1;
                }
            }
            let db = Db::recover(image, roots);
            assert_eq!(
                &db.state(keys),
                expected,
                "{label}: image {i}, in recovery {j}"
            );
            out.recoveries += 1;
        }
    }
    out
}

/// A transaction changes every key in several statements, one key again and again in one of
/// them, inserts keys that had none and deletes others, then rolls back. Crashed while it writes,
/// between any two of its compensations, and once its End is durable, it recovers to what was
/// committed before it, through recoveries crashed between two of their own compensations, and
/// the recoveries of those crashed between two of theirs.
#[test]
fn crashes_through_a_transaction_and_its_rollback_recover_what_was_committed() {
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash_io.clone();
        let db = Db::create(io);
        let keys: Vec<String> = (0..100).map(|i| format!("k{i:03}")).collect();
        let fresh: Vec<String> = (0..20).map(|i| format!("n{i:03}")).collect();
        committed(&db, &keys);
        let every: Vec<String> = keys.iter().chain(&fresh).cloned().collect();
        let expected = db.state(&every);
        crash_io.capture(|_| false, 6, seed);
        let mut t = db.begin();
        for (i, key) in keys.iter().enumerate() {
            db.put(&mut t, i % 2, key, &value("first", i));
        }
        for (i, key) in fresh.iter().enumerate() {
            db.put(&mut t, i % 2, key, &value("fresh", i));
        }
        db.txns().statement(&mut t).unwrap();
        for (i, key) in keys.iter().enumerate().step_by(2) {
            db.put(&mut t, (i + 1) % 2, key, &value("second", i));
        }
        for key in keys.iter().skip(1).step_by(4) {
            db.delete(&mut t, 1, key);
        }
        db.txns().statement(&mut t).unwrap();
        for round in 0..5 {
            db.put(&mut t, 0, &keys[0], &value("again", round));
        }
        let start = t.start();
        db.rollback(t);
        db.durable();
        let mut images = crash_io.captured();
        images.push(crash_io.crash(seed));
        let counts: Vec<usize> = images.iter().map(|i| compensations_in(i, start)).collect();
        let all = *counts.iter().max().unwrap();
        assert!(
            all > 100,
            "seed {seed}: {all} compensations durable at most"
        );
        assert!(
            counts.iter().any(|&n| n > 0 && n < all),
            "seed {seed}: no image between compensations: {counts:?}"
        );
        let label = format!(
            "seed {seed}, {} images, compensations {counts:?}",
            images.len()
        );
        let r = recover_all(images, db.roots(), &every, &expected, 1, start, &label);
        println!("{label}: {r:?}");
        assert!(
            r.mid_undo > 0 && r.mid_undo_again > 0,
            "{label}: no crash between two of a recovery's compensations: {r:?}"
        );
    }
}

/// A transaction changes `y`, then `x` in its next statement, and rolls back to between the two,
/// which puts `x` back; another transaction changes `x` and commits. Crashed from then on while
/// the first goes on writing, recovery undoes `y` and leaves the commit on `x`: the compensation
/// of `x` marks exactly that key of that statement done.
#[test]
fn a_key_put_back_and_changed_by_a_commit_since_keeps_that_commit_through_undo() {
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash_io.clone();
        let db = Db::create(io);
        let keys: Vec<String> = (0..60).map(|i| format!("k{i:03}")).collect();
        committed(&db, &keys);
        let (x, y) = (&keys[0], &keys[1]);
        let mut t = db.begin();
        db.put(&mut t, 0, y, &value("t-y", 1));
        db.txns().statement(&mut t).unwrap();
        db.put(&mut t, 0, x, &value("t-x", 2));
        db.rollback_to(&mut t, 1);
        let mut u = db.begin();
        db.put(&mut u, 0, x, &value("u-x", 3));
        db.commit(u);
        let expected = db.state(&keys);
        assert_eq!(expected[&(0, x.clone())], Some(value("u-x", 3)));
        crash_io.capture(|_| false, 4, seed);
        for (i, key) in keys.iter().enumerate().skip(2) {
            db.put(&mut t, 1, key, &value("later", i));
        }
        db.durable();
        let mut images = crash_io.captured();
        images.push(crash_io.crash(seed));
        let label = format!("seed {seed}, {} images", images.len());
        let r = recover_all(images, db.roots(), &keys, &expected, 4, t.start(), &label);
        println!("{label}: {r:?}");
        drop(t);
    }
}

/// A writer's one change is followed by segments' worth of others' commits and checkpoints whose
/// redo points pass it: segments before it are deleted, its own are kept, and a crash at any
/// point, a torn superblock included, undoes it from them. Nothing of it follows the redo point,
/// so only the writer table names it.
#[test]
fn a_writer_spanning_checkpoints_and_deleted_segments_is_undone_from_the_log_it_kept() {
    let crash_io = Arc::new(CrashIO::new(7));
    let io: Arc<dyn IO> = crash_io.clone();
    let db = Db::create(io.clone());
    let keys: Vec<String> = (0..4).map(|i| format!("k{i}")).collect();
    committed(&db, &keys);
    let expected = db.state(&keys);
    let segment = config().log.segment_bytes;
    let filler = vec![7u8; 32 << 10];
    let filler_kind = crate::storage::atoll::log::Kind::new(100).unwrap();
    // Commits of others, `bytes` of log, each a record outside any tree; the group of the first.
    let fill = |bytes: u64| {
        let end = db.db.log().end() + bytes;
        let mut first = None;
        while db.db.log().end() < end {
            let mut other = db.begin();
            let mut admitted = run(&db.io, || db.db.log().admit(framed_len(filler.len())));
            db.txns()
                .append(&mut other, &mut admitted, filler_kind, &[&filler]);
            first = first.or(other.tail());
            db.commit(other);
        }
        first.unwrap()
    };
    // Segments before the writer begins, deleted by the checkpoints after them.
    let before = fill(2 * segment);
    db.checkpoint();
    let mut t = db.begin();
    db.put(&mut t, 0, &keys[0], &value("early", 0));
    let first = t.tail().unwrap();
    // A crash image as each superblock is written, which tears it, and every so often.
    crash_io.capture(|path| path == BASE, 200, 7);
    let mut checkpoints = 0;
    while db.checkpoint().redo < first.get() + 2 * segment {
        fill(segment);
        checkpoints += 1;
    }
    db.durable();
    let mut images = crash_io.captured();
    images.push(crash_io.crash(7));
    assert!(
        checkpoints >= 2,
        "{checkpoints} checkpoints past the writer's first change"
    );
    assert!(
        Log::read_group(io.clone(), BASE, &config().log, before).is_err(),
        "the segments before the writer were kept"
    );
    assert!(
        Log::read_group(io.clone(), BASE, &config().log, first).is_ok(),
        "the writer's first change was deleted"
    );
    let label = format!("{} images", images.len());
    let r = recover_all(images, db.roots(), &keys, &expected, 3, t.start(), &label);
    println!("{label}: {r:?}");
    drop(t);
}
