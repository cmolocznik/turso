//! Rollback, and recovery under crashes (DESIGN.md §19; audit 18): a crash image taken every few
//! operations of a workload recovers to exactly what was committed.

use super::*;
use crate::io::crash::CrashIO;
use crate::io::IO;
use crate::storage::atoll::btree::Tree;
use crate::storage::atoll::btree::{value, Check, Cursor, Seek};
use crate::storage::atoll::buffer::{BufferConfig, MIN_DB_BUDGET, WRITEBACK_BYTES};
use crate::storage::atoll::control::Superblock;
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::drive;
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::page::SizeClass;
use crate::storage::atoll::page::CLASS_COUNT;
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::txn::versioned::VersionedTree;
use crate::storage::atoll::txn::versioned::{Get, Write, WriteOp};
use crate::storage::atoll::txn::{Abort, Commit, Isolation, TxnConfig};
use crate::sync::Arc;
use std::collections::{BTreeMap, BTreeSet};

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
            budget: MIN_DB_BUDGET,
            writeback_bytes: WRITEBACK_BYTES,
            kept_bytes: None,
        },
        log_budget: Some(512 << 10),
        checkpoint_write_bytes: None,
        checkpointing: crate::storage::atoll::checkpoint::Checkpointing::Writers,
        txn: TxnConfig { slots: 4 },
    }
}

fn run<T>(io: &Arc<dyn IO>, mut op: impl FnMut() -> Result<IOResult<T>>) -> T {
    drive(io, || op()).unwrap()
}

fn value(tag: &str, i: usize) -> String {
    sized(tag, i, VALUE)
}

/// Long enough to go out of line in a tree of either class: past a quarter of an 8 KiB node.
const LARGE: usize = 12 << 10;

fn large(tag: &str, i: usize) -> String {
    sized(tag, i, LARGE)
}

fn sized(tag: &str, i: usize, len: usize) -> String {
    let head = format!("{tag}-{i}-");
    format!("{head}{}", "v".repeat(len - head.len()))
}

struct Db {
    io: Arc<dyn IO>,
    db: Atoll,
    versions: Versions,
    /// Two trees of different node classes, so a transaction has a version index of each.
    trees: [VersionedTree; 2],
    /// The trees besides the two that a test made.
    more: Lifecycle,
}

/// What a test made besides the two trees: a plain list that undone creations go on, as the SQL
/// layer's abandoned list, the trees created and kept, and those a loser created.
#[derive(Clone, Default)]
struct Lifecycle {
    list: Option<PageId>,
    kept: Vec<PageId>,
    made: Vec<PageId>,
}

/// The roots a database's checks walk.
#[derive(Clone)]
struct Roots {
    trees: [PageId; 2],
    more: Lifecycle,
}

/// [`config`] with the log buffer at two chunks: under what the rollback of the few dozen changes
/// of VALUE's length a test makes appends, so the log is flushed while that rollback runs and the
/// crash images fall between its compensations.
fn flushing() -> AtollConfig {
    let mut config = config();
    config.log.buffer_bytes = 2 * config.log.chunk_bytes;
    config
}

impl Db {
    fn create(io: Arc<dyn IO>) -> Self {
        Self::create_with(io, config())
    }

    fn create_with(io: Arc<dyn IO>, config: AtollConfig) -> Self {
        let db = Atoll::open(io.clone(), BASE, config).unwrap();
        let roots = [0, 1].map(|c| {
            run(&io, || {
                Tree::create_versioned(db.pool(), SizeClass::new(c).unwrap())
            })
        });
        let made = Self::with(
            io,
            db,
            Roots {
                trees: roots,
                more: Lifecycle::default(),
            },
        );
        // Trees made outside any transaction are in no log: a checkpoint maps them before any
        // write is admitted, as a database's creation does its system trees.
        made.checkpoint();
        made
    }

    /// What a crash left, recovered.
    fn recover(io: Arc<dyn IO>, roots: Roots) -> Self {
        let db = Atoll::open(io.clone(), BASE, config()).unwrap();
        assert!(db.recovered_end().is_some(), "no recovery");
        Self::with(io, db, roots)
    }

    fn with(io: Arc<dyn IO>, db: Atoll, roots: Roots) -> Self {
        let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), 4);
        let trees = roots.trees.map(|root| {
            VersionedTree::open(io.clone(), db.pool().clone(), db.store().clone(), root)
        });
        Self {
            io,
            db,
            versions,
            trees,
            more: roots.more,
        }
    }

    fn roots(&self) -> Roots {
        Roots {
            trees: [self.trees[0].root(), self.trees[1].root()],
            more: self.more.clone(),
        }
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
        let mut op = WriteOp::put(key.as_bytes(), value.as_bytes().to_vec(), Mode::Upsert);
        let write = run(&self.io, || {
            self.trees[tree].write(self.txns(), &self.versions, txn, &mut op)
        });
        assert!(matches!(write, Write::Done(_)), "{key}: {write:?}");
    }

    fn delete(&self, txn: &mut Txn, tree: usize, key: &str) {
        let mut op = WriteOp::delete(key.as_bytes());
        run(&self.io, || {
            self.trees[tree].write(self.txns(), &self.versions, txn, &mut op)
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

    fn rollback_to(&self, txn: &mut Txn, to: Statement) {
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
                let mut op = Get::new(key.as_bytes());
                let v = run(&self.io, || {
                    self.trees[tree].get(self.txns(), &self.versions, &mut txn, &mut op)
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

    /// Checks that the pages allocated are exactly the trees' nodes and values, and those handed
    /// over to be freed: none leaked, none retired, none freed that is in use. The trees are the
    /// two, and those the test made: the list, every tree on it and every tree kept. A tree a
    /// loser created is allocated exactly when it is on the list, and empty there.
    fn check_whole(&self, label: &str) {
        let mut expected = BTreeSet::new();
        let open = |root| self.versions.open_tree(root);
        let mut more: Vec<VersionedTree> = self.more.kept.iter().map(|&r| open(r)).collect();
        let mut plain = Vec::new();
        if let Some(list) = self.more.list {
            let listed = self.listed(list);
            for &root in &self.more.made {
                let allocated = run(&self.io, || self.db.store().entry(root)) != FREE;
                assert_eq!(
                    allocated,
                    listed.contains(&root),
                    "{label}: the loser's tree {root:?} allocated {allocated}, listed {}",
                    listed.contains(&root)
                );
            }
            for &root in &listed {
                let mut check = Check::default();
                let stats = run(&self.io, || open(root).tree().check(&mut check));
                if self.more.made.contains(&root) {
                    assert_eq!(stats.entries, 0, "{label}: {root:?} listed with entries");
                }
            }
            more.extend(listed.into_iter().map(open));
            plain.push(self.versions.open_plain(list));
        }
        let trees = self.trees.iter().chain(&more).map(|t| t.tree());
        for tree in trees.chain(&plain) {
            let mut check = Check::default();
            let stats = run(&self.io, || tree.check(&mut check));
            expected.extend(stats.pages.iter().copied());
            for &v in &stats.values {
                let pages = run(&self.io, || value::pages(self.db.pool(), v, &|| true));
                expected.extend(pages.unwrap_or_else(|_| panic!("{label}: {v:?} lost its pages")));
            }
        }
        let mut allocated = BTreeSet::new();
        for c in 0..CLASS_COUNT {
            let class = SizeClass::new(c).unwrap();
            for number in 0..crate::storage::atoll::testing::page_numbers(self.db.store(), class) {
                let pid = PageId::new(class, number);
                if run(&self.io, || self.db.store().entry(pid)) != FREE {
                    allocated.insert(pid);
                }
            }
        }
        expected.extend(self.db.store().orphans());
        let leaked: Vec<_> = allocated.difference(&expected).collect();
        let lost: Vec<_> = expected.difference(&allocated).collect();
        assert!(
            leaked.is_empty() && lost.is_empty(),
            "{label}: pages allocated and unreachable {leaked:?}; reachable and free {lost:?}"
        );
    }

    /// Makes the plain list that undone creations go on.
    fn make_list(&mut self) -> PageId {
        let class = SizeClass::new(0).unwrap();
        let list = run(&self.io, || Tree::create(self.db.pool(), class));
        self.more.list = Some(list);
        // Made outside any transaction: in no log until a checkpoint maps it.
        self.checkpoint();
        list
    }

    /// A tree of `class` created in `txn`'s current statement, undone onto the list.
    fn create_tree(&self, txn: &mut Txn, class: usize) -> PageId {
        let (list, class) = (
            self.more.list.expect("a list"),
            SizeClass::new(class).unwrap(),
        );
        run(&self.io, || {
            self.txns().create_tree(&self.versions, txn, class, list)
        })
    }

    fn put_in(&self, txn: &mut Txn, root: PageId, key: &str, value: &str) {
        let mut op = WriteOp::put(key.as_bytes(), value.as_bytes().to_vec(), Mode::Upsert);
        let tree = self.versions.open_tree(root);
        let write = run(&self.io, || {
            tree.write(self.txns(), &self.versions, txn, &mut op)
        });
        assert!(matches!(write, Write::Done(_)), "{key}: {write:?}");
    }

    fn get_in(&self, txn: &mut Txn, root: PageId, key: &str) -> Option<String> {
        let mut op = Get::new(key.as_bytes());
        let tree = self.versions.open_tree(root);
        run(&self.io, || {
            tree.get(self.txns(), &self.versions, txn, &mut op)
        })
        .map(|v| String::from_utf8(v).unwrap())
    }

    /// The roots on `list`, each keyed by its big-endian bytes.
    fn listed(&self, list: PageId) -> BTreeSet<PageId> {
        let tree = self.versions.open_plain(list);
        let mut cursor = Cursor::new();
        let mut out = BTreeSet::new();
        let mut more = run(&self.io, || tree.seek(&mut cursor, &[], Seek::Ge));
        while more {
            let key = cursor.key().expect("an entry");
            out.insert(PageId::from_raw(u64::from_be_bytes(
                key.try_into().unwrap(),
            )));
            more = run(&self.io, || tree.next(&mut cursor));
        }
        out
    }

    fn entries(&self, root: PageId) -> usize {
        let mut check = Check::default();
        run(&self.io, || {
            self.versions.open_tree(root).tree().check(&mut check)
        })
        .entries
    }

    fn checkpoint(&self) -> Superblock {
        let ticket = self.db.checkpointer().request_full();
        run(&self.io, || {
            self.db.checkpointer().step_full(ticket, self.txns())
        });
        self.db.checkpointer().superblock()
    }
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

/// Where a recovery is crashed: just before each write to any of the database's files completes,
/// the write in flight torn, and every few operations besides: its log reopened, its checkpoint's
/// images, metadata and superblock.
fn crashes_in_recovery(path: &str) -> bool {
    path.starts_with(BASE)
}

/// Recovers every image to `expected`, and every `again`th one also through crashes while its
/// recovery runs ([`crashes_in_recovery`]), each recovered in turn, and the middle one crashed
/// again in its own recovery. Returns how many recoveries it checked.
fn recover_all(
    images: Vec<CrashIO>,
    roots: Roots,
    keys: &[String],
    expected: &BTreeMap<(usize, String), Option<String>>,
    again: usize,
    label: &str,
) -> usize {
    let mut recoveries = 0;
    let mut check = |db: &Db, what: String| {
        assert_eq!(&db.state(keys), expected, "{what}");
        db.check_whole(&what);
        recoveries += 1;
    };
    for (i, image) in images.into_iter().enumerate() {
        let image = Arc::new(image);
        let crashed = (i % again == 0).then(|| image.crash(i as u64));
        check(
            &Db::recover(image, roots.clone()),
            format!("{label}: image {i}"),
        );
        let Some(crashed) = crashed else {
            continue;
        };
        let crashed = Arc::new(crashed);
        crashed.capture(crashes_in_recovery, 40, i as u64);
        drop(Db::recover(crashed.clone(), roots.clone()));
        let in_recovery = crashed.captured();
        assert!(
            !in_recovery.is_empty(),
            "{label}: image {i}: no image in recovery"
        );
        let deepest = in_recovery.len() / 2;
        for (j, image) in in_recovery.into_iter().enumerate() {
            let image = Arc::new(image);
            if j == deepest {
                // Crashed in recovery, and that recovery crashed in turn.
                image.capture(crashes_in_recovery, 40, j as u64);
                drop(Db::recover(image.clone(), roots.clone()));
                for (k, deeper) in image.captured().into_iter().enumerate() {
                    check(
                        &Db::recover(Arc::new(deeper), roots.clone()),
                        format!("{label}: image {i}, in recovery {j}, in recovery {k}"),
                    );
                }
            }
            check(
                &Db::recover(image, roots.clone()),
                format!("{label}: image {i}, in recovery {j}"),
            );
        }
    }
    recoveries
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
        db.rollback(t);
        db.durable();
        let mut images = crash_io.captured();
        images.push(crash_io.crash(seed));
        let label = format!("seed {seed}, {} images", images.len());
        let n = recover_all(images, db.roots(), &every, &expected, 1, &label);
        assert!(n > 1, "{label}: {n} recoveries");
    }
}

/// As [`crashes_through_a_transaction_and_its_rollback_recover_what_was_committed`], with values
/// too long for their leaves: the transaction replaces committed ones, replaces its own from an
/// earlier statement, rewrites one again and again in one statement, deletes some and inserts new
/// ones. Its log goes out in few, long writes, so it is crashed just before each lands, torn, and
/// every few operations besides. Every image recovers to what was committed with no page leaked,
/// left retired or freed in use, and so does the rollback running on: the pages of every value it
/// wrote are freed, and the ones it replaced are kept.
#[test]
fn crashes_through_large_values_and_their_rollback_recover_what_was_committed_and_leak_nothing() {
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash_io.clone();
        let db = Db::create(io);
        let keys: Vec<String> = (0..12).map(|i| format!("k{i:03}")).collect();
        let fresh: Vec<String> = (0..4).map(|i| format!("n{i:03}")).collect();
        let mut txn = db.begin();
        for (i, key) in keys.iter().enumerate() {
            let v = if i % 3 == 0 {
                value("committed", i)
            } else {
                large("committed", i)
            };
            for tree in 0..2 {
                db.put(&mut txn, tree, key, &v);
            }
        }
        db.commit(txn);
        let every: Vec<String> = keys.iter().chain(&fresh).cloned().collect();
        let expected = db.state(&every);
        db.check_whole(&format!("seed {seed}: committed"));
        crash_io.capture(crashes_in_recovery, 12, seed);
        let mut t = db.begin();
        for (i, key) in keys.iter().enumerate() {
            db.put(&mut t, i % 2, key, &large("first", i));
        }
        for (i, key) in fresh.iter().enumerate() {
            db.put(&mut t, i % 2, key, &large("fresh", i));
        }
        db.txns().statement(&mut t).unwrap();
        // Its own versions from the first statement, and committed ones.
        for key in keys.iter().skip(1).step_by(3) {
            db.delete(&mut t, 1, key);
        }
        db.txns().statement(&mut t).unwrap();
        for round in 0..4 {
            db.put(&mut t, 0, &keys[0], &large("again", round));
        }
        db.rollback(t);
        db.durable();
        assert_eq!(
            db.state(&every),
            expected,
            "seed {seed}: the rollback running on"
        );
        db.check_whole(&format!("seed {seed}: the rollback running on"));
        let mut images = crash_io.captured();
        images.push(crash_io.crash(seed));
        let label = format!("seed {seed}, {} images", images.len());
        let n = recover_all(images, db.roots(), &every, &expected, 3, &label);
        assert!(n > 1, "{label}: {n} recoveries");
    }
}

/// A value written since the last checkpoint and replaced by a commit is retired with no copy of
/// its pages on disk, so redo puts them back in dirty frames. Recovery frees what is still retired
/// (DESIGN.md §12, step 6), and those frames must go with the pages: a frame left dirty would be
/// written back by the closing checkpoint as a page that is free.
#[test]
fn a_value_retired_before_any_copy_of_its_pages_was_written_leaves_the_pool_when_recovery_frees_it()
{
    let crash_io = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash_io.clone();
    let db = Db::create(io);
    db.checkpoint();
    let key = "k".to_string();
    for tag in ["old", "new"] {
        let mut txn = db.begin();
        db.put(&mut txn, 0, &key, &large(tag, 0));
        db.commit(txn);
    }
    db.durable();
    let roots = db.roots();
    let image = Arc::new(crash_io.crash(0));
    drop(db);
    let db = Db::recover(image, roots.clone());
    assert_eq!(
        db.state(std::slice::from_ref(&key))[&(0, key.clone())],
        Some(large("new", 0))
    );
    db.check_whole("recovered");
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
        let n = recover_all(images, db.roots(), &keys, &expected, 4, &label);
        assert!(n > 1, "{label}: {n} recoveries");
        drop(t);
    }
}

/// A transaction past 65,535 statements (DESIGN.md §19): its statement numbers no longer fit 16
/// bits in its version headers, its arena entries, the statement a rollback stops at and its
/// compensations. One rolls back to a statement past the old limit and commits; another, as far
/// past it, writes and rolls back, crashed every few operations, and every image recovers to what
/// was committed, through recoveries crashed between two of their own compensations.
#[test]
fn a_transaction_past_65535_statements_rolls_back_to_one_commits_and_recovers() {
    const PAST: Statement = u16::MAX as Statement + 2;
    // The statement rolled back to ends in sixteen one bits, the next in sixteen zeros: read in
    // 16 bits, the statement undone would sort before the one kept.
    const KEPT: Statement = 2 * (u16::MAX as Statement + 1) - 1;
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash_io.clone();
        // The second transaction's rollback compensates 54 changes, each record carrying its
        // VALUE: 37,800 bytes or more, past the 32 KiB buffer, so it is flushed while they run.
        let db = Db::create_with(io, flushing());
        let keys: Vec<String> = (0..40).map(|i| format!("k{i:03}")).collect();
        committed(&db, &keys);
        // A statement that writes nothing costs only its number.
        let mut t = db.begin();
        while t.command() < KEPT {
            db.txns().statement(&mut t).unwrap();
        }
        for (i, key) in keys.iter().enumerate().take(20) {
            db.put(&mut t, i % 2, key, &value("kept", i));
        }
        let kept = t.command();
        assert_eq!(kept, KEPT);
        db.txns().statement(&mut t).unwrap();
        // Twice in one statement: undoing it puts back what the statement found, which only a
        // statement number read whole tells from the statement's own first write.
        for round in ["undone", "undone again"] {
            for (i, key) in keys.iter().enumerate() {
                db.put(&mut t, i % 2, key, &value(round, i));
            }
        }
        db.rollback_to(&mut t, kept);
        assert!(t.command() > kept + 1, "statement {} reused", t.command());
        db.commit(t);
        let expected = db.state(&keys);
        for (i, key) in keys.iter().enumerate() {
            let want = if i < 20 {
                value("kept", i)
            } else {
                value("committed", i)
            };
            assert_eq!(
                expected[&(i % 2, key.clone())],
                Some(want),
                "seed {seed}: {key}"
            );
            assert_eq!(
                expected[&((i + 1) % 2, key.clone())],
                Some(value("committed", i)),
                "seed {seed}: {key}"
            );
        }
        crash_io.capture(|_| false, 6, seed);
        let mut u = db.begin();
        while u.command() < PAST {
            db.txns().statement(&mut u).unwrap();
        }
        for (i, key) in keys.iter().enumerate() {
            db.put(&mut u, i % 2, key, &value("lost", i));
        }
        db.txns().statement(&mut u).unwrap();
        for (i, key) in keys.iter().enumerate().step_by(3) {
            db.put(&mut u, (i + 1) % 2, key, &value("lost again", i));
        }
        db.durable();
        let before = crash_io.crash(seed);
        db.rollback(u);
        db.durable();
        let mut images = crash_io.captured();
        images.push(before);
        images.push(crash_io.crash(seed));
        let label = format!("seed {seed}, {} images", images.len());
        // Every image crashed in its recovery too.
        let n = recover_all(images, db.roots(), &keys, &expected, 1, &label);
        assert!(n > 1, "{label}: {n} recoveries");
    }
}

/// A transaction's last statement is number 2^32 - 1: starting one more fails, and the
/// transaction still rolls back whole.
#[test]
fn a_statement_past_the_last_number_fails_and_the_transaction_rolls_back() {
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(0));
    let db = Db::create(io);
    let mut t = db.begin();
    db.put(&mut t, 0, "k", &value("v", 0));
    t.skip_to(Statement::MAX);
    let err = db.txns().statement(&mut t).unwrap_err();
    assert!(err.to_string().contains("statements"), "{err}");
    assert_eq!(t.command(), Statement::MAX);
    db.rollback(t);
    assert_eq!(db.state(&["k".to_string()])[&(0, "k".to_string())], None);
}

/// A rollback to an earlier statement puts on the list the trees its later statements created,
/// once their changes to them are undone, and keeps the rest with their earlier statements'
/// changes; a whole rollback puts every tree it created on the list. The commit or rollback that
/// ends the transaction hands over the trees on the list.
#[test]
fn undone_creations_go_on_the_list_and_the_rest_stay_with_their_earlier_changes() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let mut db = Db::create(io);
    let list = db.make_list();
    let mut t = db.begin();
    let a = db.create_tree(&mut t, 0);
    db.put_in(&mut t, a, "a1", "kept");
    db.txns().statement(&mut t).unwrap();
    db.put_in(&mut t, a, "a2", "undone");
    let b = db.create_tree(&mut t, 1);
    db.put_in(&mut t, b, "b2", "undone");
    db.put(&mut t, 0, "k", "undone");
    db.txns().statement(&mut t).unwrap();
    let c = db.create_tree(&mut t, 0);
    db.put_in(&mut t, b, "b3", "undone");
    db.put_in(&mut t, c, "c3", "undone");
    db.rollback_to(&mut t, 1);
    let held: Vec<PageId> = t.created().iter().map(|c| c.root).collect();
    assert_eq!(held, vec![a]);
    assert_eq!(t.abandoned(), &[c, b], "newest first");
    assert_eq!(db.listed(list), BTreeSet::from([b, c]));
    assert_eq!(db.get_in(&mut t, a, "a1").as_deref(), Some("kept"));
    assert_eq!(db.get_in(&mut t, a, "a2"), None);
    assert_eq!(
        (db.entries(b), db.entries(c)),
        (0, 0),
        "their changes undone"
    );
    let mut commit = Commit::new(t);
    run(&db.io, || db.txns().commit(&mut commit));
    assert_eq!(commit.abandoned(), &[c, b]);
    db.more.kept.push(a);
    let mut t = db.begin();
    let d = db.create_tree(&mut t, 0);
    db.txns().statement(&mut t).unwrap();
    db.put_in(&mut t, d, "d2", "undone");
    let e = db.create_tree(&mut t, 1);
    let mut abort = Abort::new(t);
    run(&db.io, || db.txns().rollback(&db.versions, &mut abort));
    assert_eq!(abort.abandoned(), &[e, d]);
    assert_eq!(db.listed(list), BTreeSet::from([b, c, d, e]));
    let mut t = db.begin();
    assert_eq!(db.get_in(&mut t, a, "a1").as_deref(), Some("kept"));
    db.commit(t);
    db.check_whole("after both");
}

/// A loser that created trees in several statements and wrote them, crashed anywhere, through
/// recoveries crashed in turn: none of its creations reached the log, so no root it made is on
/// the list or allocated and none leaks; a tree a committed transaction created stays off the list
/// with its rows; and what was committed reads back.
#[test]
fn a_losers_created_trees_are_gone_after_crashes_and_a_committed_ones_stay() {
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash_io.clone();
        let mut db = Db::create(io);
        db.make_list();
        let keys: Vec<String> = (0..40).map(|i| format!("k{i:03}")).collect();
        committed(&db, &keys);
        let mut t = db.begin();
        let kept = db.create_tree(&mut t, 0);
        db.put_in(&mut t, kept, "x", "kept");
        db.commit(t);
        db.durable();
        db.more.kept.push(kept);
        let expected = db.state(&keys);
        crash_io.capture(|_| false, 3, seed);
        let mut t = db.begin();
        for s in 0..4 {
            let root = db.create_tree(&mut t, s % 2);
            db.more.made.push(root);
            for (i, key) in keys.iter().enumerate() {
                db.put_in(&mut t, root, key, &value("made", i));
                db.put(&mut t, i % 2, key, &value("loser", i));
            }
            db.txns().statement(&mut t).unwrap();
        }
        db.durable();
        let mut images = crash_io.captured();
        images.push(crash_io.crash(seed));
        let label = format!("seed {seed}, {} images", images.len());
        let roots = db.roots();
        let n = recover_all(images, roots.clone(), &keys, &expected, 3, &label);
        assert!(n > 1, "{label}: {n} recoveries");
        // The last image holds every creation of the loser: none reached the log, so none is
        // on the list and none is allocated.
        let db = Db::recover(Arc::new(crash_io.crash(seed + 1)), roots);
        let listed = db.listed(db.more.list.unwrap());
        for root in &db.more.made {
            assert!(!listed.contains(root), "{label}: {root:?} listed");
            let entry = run(&db.io, || db.db.store().entry(*root));
            assert_eq!(entry, FREE, "{label}: {root:?} allocated");
        }
        let mut t = db.begin();
        assert_eq!(db.get_in(&mut t, kept, "x").as_deref(), Some("kept"));
        db.commit(t);
    }
}

/// A settle reaches the log: a crash after writes took the room it gave back recovers them, though
/// the leaf's copy on disk, written before, still keeps the reserves.
#[test]
fn a_crash_after_a_settle_recovers_the_writes_that_took_its_room() {
    let crash_io = Arc::new(CrashIO::new(0));
    let db = Db::create(crash_io.clone());
    let xs: Vec<String> = (0..4).map(|i| format!("x{i}")).collect();
    let ys: Vec<String> = (0..4).map(|i| format!("y{i}")).collect();
    let mut t = db.begin();
    for (i, key) in xs.iter().enumerate() {
        db.put(&mut t, 0, key, &value("wide", i));
    }
    db.commit(t);
    let mut shrink = db.begin();
    for key in &xs {
        db.put(&mut shrink, 0, key, "-");
    }
    db.commit(shrink);
    db.checkpoint();
    let mut fill = db.begin();
    for (i, key) in ys.iter().enumerate() {
        db.put(&mut fill, 0, key, &value("fill", i));
    }
    db.commit(fill);
    db.durable();
    let keys: Vec<String> = xs.iter().chain(&ys).cloned().collect();
    let expected = db.state(&keys);
    assert_eq!(
        expected[&(0, ys[3].clone())].as_deref(),
        Some(value("fill", 3).as_str())
    );
    let roots = db.roots();
    drop(db);
    let recovered = Db::recover(Arc::new(crash_io.crash(0)), roots);
    assert_eq!(recovered.state(&keys), expected);
    recovered.check_whole("recovered");
}

/// A transaction spills each chunk its changes fill while it runs (DESIGN.md §11.4.1), never
/// holding more than the chunk it fills and the one it spilled last; a checkpoint cut meanwhile
/// keeps the log from its first part. A crash before its commit recovers none of its changes, and
/// one after recovers all of them, the parts read from before the cut.
#[test]
fn a_transaction_spills_as_it_goes_and_a_checkpoint_between_keeps_its_parts() {
    let crash_io = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash_io.clone();
    let db = Db::create(io);
    let keys: Vec<String> = (0..120).map(|i| format!("k{i:03}")).collect();
    committed(&db, &keys[..20]);
    let before = db.state(&keys);
    let chunk = config().log.chunk_bytes as usize;
    let mut t = db.begin();
    let mut most = 0;
    for (i, key) in keys.iter().enumerate() {
        db.put(&mut t, i % 2, key, &value("spilled", i));
        most = most.max(t.changes().capacity());
    }
    let first = t
        .changes()
        .parts()
        .first()
        .copied()
        .expect("a part spilled before the commit");
    assert!(most <= 2 * chunk, "{most} bytes of chunks held");
    let superblock = db.checkpoint();
    assert!(
        superblock.redo <= first.at.get(),
        "the redo point {} past the first part at {}",
        superblock.redo,
        first.at.get()
    );
    db.durable();
    let uncommitted = crash_io.crash(1);
    db.commit(t);
    db.durable();
    let after = db.state(&keys);
    let committed = crash_io.crash(2);
    for (image, want, label) in [
        (uncommitted, &before, "before the commit"),
        (committed, &after, "after it"),
    ] {
        let recovered = Db::recover(Arc::new(image), db.roots());
        assert_eq!(&recovered.state(&keys), want, "{label}");
        recovered.check_whole(label);
    }
}
