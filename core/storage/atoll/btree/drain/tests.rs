//! Draining a tree whole (DESIGN.md §17): every page of it freed, a node a step, and a drain a crash
//! cut short gone on with from the tree the checkpoint in charge left.

use super::*;
use crate::io::crash::CrashIO;
use crate::io::MemoryIO;
use crate::storage::atoll::buffer::{BufferConfig, MIN_BUDGET, WRITEBACK_BYTES};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::LogConfig;
use crate::storage::atoll::page::CLASS_COUNT;
use crate::storage::atoll::radix::FREE;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
use crate::storage::atoll::txn::versioned::Versions;
use std::collections::BTreeSet;

const BASE: &str = "/db/drain";

fn config(chunk_bytes: u64) -> AtollConfig {
    AtollConfig {
        log: LogConfig {
            segment_bytes: 1 << 20,
            buffer_bytes: 256 << 10,
            chunk_bytes,
            writes_in_flight: 3,
            direct: true,
        },
        store: StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        buffer: BufferConfig {
            budget: MIN_BUDGET,
            writeback_bytes: WRITEBACK_BYTES,
        },
        log_budget: Some(512 << 10),
        txn: Default::default(),
    }
}

fn run<T>(io: &Arc<dyn IO>, op: impl FnMut() -> Result<IOResult<T>>) -> T {
    drive(io, op).unwrap()
}

struct Db {
    io: Arc<dyn IO>,
    db: Atoll,
    versions: Versions,
    list: Tree,
}

impl Db {
    /// A new database, with a plain list for the trees to drain.
    fn create(io: Arc<dyn IO>, chunk_bytes: u64) -> Self {
        let db = Atoll::open(io.clone(), BASE, config(chunk_bytes)).unwrap();
        let list = run(&io, || Tree::create(db.pool(), SizeClass::new(0).unwrap()));
        Self::with(io, db, list)
    }

    fn recover(io: Arc<dyn IO>, chunk_bytes: u64, list: PageId) -> Self {
        let db = Atoll::open(io.clone(), BASE, config(chunk_bytes)).unwrap();
        Self::with(io, db, list)
    }

    fn with(io: Arc<dyn IO>, db: Atoll, list: PageId) -> Self {
        let list = Tree::open(io.clone(), db.pool().clone(), db.store().clone(), list);
        let slots = config(0).txn.slots;
        let versions = Versions::new(io.clone(), db.pool().clone(), db.store().clone(), slots);
        Self {
            io,
            db,
            versions,
            list,
        }
    }

    /// A full checkpoint, done.
    fn checkpoint(&self) {
        let checkpointer = self.db.checkpointer();
        let ticket = checkpointer.request_full();
        run(&self.io, || {
            checkpointer.step_full(ticket, self.db.transactions(), &self.versions)
        });
    }

    /// How many levels the tree rooted at `root` has.
    fn levels(&self, root: PageId) -> u8 {
        let node = run(&self.io, || self.db.pool().fix_optimistic(root, None));
        node.read(|p| View::new(p).level()).expect("a stable read") + 1
    }

    fn tree(&self, root: PageId) -> Tree {
        Tree::open(
            self.io.clone(),
            self.db.pool().clone(),
            self.db.store().clone(),
            root,
        )
    }

    /// A tree of `class` holding `values`, keyed by their position, listed.
    fn listed_tree(&self, class: usize, values: &[Vec<u8>]) -> PageId {
        let class = SizeClass::new(class).unwrap();
        let root = run(&self.io, || Tree::create(self.db.pool(), class));
        let tree = self.tree(root);
        for (i, v) in values.iter().enumerate() {
            let mut put = Put::new(&(i as u64).to_be_bytes(), v.clone(), Mode::Insert);
            assert_eq!(run(&self.io, || tree.put(&mut put)), Outcome::Inserted);
        }
        let mut put = Put::new(&root.raw().to_be_bytes(), vec![7; 8], Mode::Insert);
        assert_eq!(run(&self.io, || self.list.put(&mut put)), Outcome::Inserted);
        root
    }

    /// Every page of the tree rooted at `root`, half drained or whole: its nodes, reached by
    /// their children as the drain reaches them, never by fences, and every page of its values.
    fn pages(&self, root: PageId) -> BTreeSet<PageId> {
        let (mut out, mut stack) = (BTreeSet::new(), vec![root]);
        while let Some(pid) = stack.pop() {
            out.insert(pid);
            let node = run(&self.io, || self.db.pool().fix_optimistic(pid, None));
            let (children, values) = node
                .read(|p| {
                    let v = View::new(p);
                    assert!(
                        v.is_node() && v.root() == root,
                        "{pid:?} is no node of {root:?}"
                    );
                    let count = v.count().unwrap();
                    if v.is_leaf() {
                        let refs = (0..count)
                            .filter_map(|i| v.value(i).filter(|(_, out)| *out))
                            .map(|(stored, _)| ValueRef::decode(&stored[stored.len() - REF..]))
                            .map(Option::unwrap)
                            .collect();
                        return (Vec::new(), refs);
                    }
                    let mut children: Vec<PageId> =
                        (0..count).map(|i| v.child(i).unwrap().0).collect();
                    children.extend(Some(v.next().0).filter(|&c| c != NO_CHILD));
                    (children, Vec::new())
                })
                .expect("a stable read");
            stack.extend(children);
            for v in values {
                let pages = run(&self.io, || value::pages(self.db.pool(), v, &|| true));
                out.extend(pages.unwrap_or_else(|_| panic!("{v:?} lost its pages")));
            }
        }
        out
    }

    /// The roots on the list.
    fn listed(&self) -> Vec<PageId> {
        let mut cursor = Cursor::new();
        let mut out = Vec::new();
        let mut more = run(&self.io, || self.list.seek(&mut cursor, &[], Seek::Ge));
        while more {
            let key = cursor.key().expect("an entry");
            out.push(PageId::from_raw(u64::from_be_bytes(
                key.try_into().unwrap(),
            )));
            more = run(&self.io, || self.list.next(&mut cursor));
        }
        out
    }

    /// Drains the tree rooted at `root`; returns the steps it took.
    fn drain(&self, root: PageId) -> usize {
        self.drain_with(root, false)
    }

    /// Drains `root`, a full checkpoint after each step when `checkpointed`, so the tree a step
    /// leaves is in charge while the drain runs, and a crash can fall inside any of them.
    fn drain_with(&self, root: PageId, checkpointed: bool) -> usize {
        let tree = self.tree(root);
        let mut op = Drain::new(&root.raw().to_be_bytes());
        let mut steps = 0;
        loop {
            steps += 1;
            let drained = run(&self.io, || tree.drain(&self.list, &mut op));
            if checkpointed {
                self.checkpoint();
            }
            if drained == Drained::Gone {
                return steps;
            }
        }
    }

    /// Every page allocated, by scanning the store: numbers are taken lowest first, so a run of
    /// free numbers this long is past every page.
    fn allocated(&self) -> BTreeSet<PageId> {
        let mut out = BTreeSet::new();
        for c in 0..CLASS_COUNT {
            let class = SizeClass::new(c).unwrap();
            for number in 0..crate::storage::atoll::testing::page_numbers(self.db.store(), class) {
                let pid = PageId::new(class, number);
                if run(&self.io, || self.db.store().entry(pid)) != FREE {
                    out.insert(pid);
                }
            }
        }
        out
    }

    /// Checks the pages allocated are exactly the list's, and those of every tree still on it,
    /// with those handed to the checkpointer to free: none leaked, none freed in use.
    fn check_nothing_leaked(&self, label: &str) {
        let mut expected = self.pages(self.list.root());
        for root in self.listed() {
            expected.extend(self.pages(root));
        }
        expected.extend(self.db.store().orphans());
        let allocated = self.allocated();
        let leaked: Vec<_> = allocated.difference(&expected).collect();
        let lost: Vec<_> = expected.difference(&allocated).collect();
        assert!(
            leaked.is_empty() && lost.is_empty(),
            "{label}: allocated and unreachable {leaked:?}; reachable and free {lost:?}"
        );
    }
}

fn bytes(i: usize, len: usize) -> Vec<u8> {
    (0..len).map(|j| (i * 31 + j) as u8).collect()
}

/// A tree of three levels whose leaves hold values inline, values of one page and a value of two
/// chunks, drained whole: every page it had is freed or handed to the checkpointer, inner nodes
/// emptied of children below the root included, its entry is gone from the list, and nothing
/// else is freed.
#[test]
fn a_tree_drains_whole_with_every_page_of_its_values() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Db::create(io, 16 << 10);
    let values: Vec<Vec<u8>> = (0..30_000)
        .map(|i| match i {
            15_000 => bytes(i, value::chunk() + 5),
            _ if i % 50 == 0 => bytes(i, 2000),
            _ => bytes(i, 40),
        })
        .collect();
    let keep = db.listed_tree(0, &values[..10]);
    let root = db.listed_tree(0, &values);
    let pages = db.pages(root);
    let mut check = Check::default();
    let stats = run(&db.io, || db.tree(root).check(&mut check));
    assert!(db.levels(root) >= 3, "{} levels", db.levels(root));
    assert!(stats.values.iter().any(|v| v.len as usize > value::chunk()));
    let others = db.pages(keep);
    let mut checked: BTreeSet<PageId> = stats.pages.iter().copied().collect();
    for &v in &stats.values {
        let pages = run(&db.io, || value::pages(db.db.pool(), v, &|| true));
        checked.extend(pages.unwrap_or_else(|_| panic!("{v:?} lost its pages")));
    }
    assert_eq!(
        pages, checked,
        "the walk by position reaches what the check does"
    );
    let steps = db.drain(root);
    assert_eq!(steps, stats.nodes, "a step per node");
    assert_eq!(db.listed(), vec![keep]);
    let orphans: BTreeSet<_> = db.db.store().orphans().into_iter().collect();
    for &pid in &pages {
        let free = run(&db.io, || db.db.store().entry(pid)) == FREE;
        assert!(free || orphans.contains(&pid), "{pid:?} left allocated");
    }
    assert_eq!(
        db.pages(keep),
        others,
        "another tree on the list was touched"
    );
    db.check_nothing_leaked("drained");
}

/// A drain crashed anywhere, a checkpoint after each of its steps: each image recovers the list
/// and the tree as the checkpoint in charge left them, the tree whole, partly drained or gone, with
/// nothing leaked; the drain goes on from there, and in the end the tree is gone, every page of it
/// freed and no other page touched. Images are taken before each superblock write, where a
/// checkpoint's pages are written and the one before is still in charge, and at operations between.
#[test]
fn a_drain_crashed_anywhere_goes_on_from_the_tree_the_checkpoint_in_charge_left() {
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash_io.clone();
        let db = Db::create(io, 4 << 10);
        let values: Vec<Vec<u8>> = (0..1500)
            .map(|i| bytes(i, if i % 2 == 0 { 1500 } else { 30 }))
            .collect();
        let root = db.listed_tree(0, &values);
        let list = db.list.root();
        let pages = db.pages(root).len();
        db.checkpoint();
        crash_io.capture(|path| path == BASE, 2, seed);
        let steps = db.drain_with(root, true);
        let mut images = crash_io.captured();
        images.push(crash_io.crash(seed));
        let label = format!("seed {seed}: {steps} steps, {} images", images.len());
        let (mut whole, mut partly, mut gone) = (0, 0, 0);
        for (i, image) in images.into_iter().enumerate() {
            let db = Db::recover(Arc::new(image), 4 << 10, list);
            match db.listed().as_slice() {
                [] => gone += 1,
                [r] if *r == root => {
                    db.check_nothing_leaked(&format!("{label}: image {i}, recovered"));
                    match db.pages(root).len() {
                        n if n == pages => whole += 1,
                        n if n < pages => partly += 1,
                        n => panic!("{label}: image {i}: {n} pages, of {pages}"),
                    }
                    db.drain(root);
                }
                other => panic!("{label}: image {i} lists {other:?}"),
            }
            assert!(db.listed().is_empty(), "{label}: image {i}");
            db.check_nothing_leaked(&format!("{label}: image {i}"));
        }
        println!("{label}: {whole} whole, {partly} partly drained, {gone} gone");
        assert!(
            partly > 1 && gone > 0,
            "{label}: {partly} partly drained, {gone} gone"
        );
    }
}
