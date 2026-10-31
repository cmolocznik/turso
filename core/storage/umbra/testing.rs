//! Test support: every IO backend the storage runs on, and a driver that steps an operation's
//! `IOResult` to the end the way the VDBE does.

use crate::io::{Buffer, Completion, File, OpenFlags, IO};
use crate::storage::umbra::buffer::{Anchor, BufferManager, ExclusiveGuard, DEFAULT_COOLING_SHARE};
use crate::storage::umbra::checkpoint;
use crate::storage::umbra::page::{self, PageId, SizeClass, HEADER_SIZE};
use crate::storage::umbra::store::PageStore;
use crate::storage::umbra::wal::{Record, Recovered, Wal};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::Result;

pub(super) struct Backend {
    pub name: &'static str,
    pub io: Arc<dyn IO>,
    /// Where its files go; held so the directory outlives the test.
    _dir: tempfile::TempDir,
    pub base: String,
}

impl Backend {
    /// A path for the file `name` on this backend.
    pub fn path(&self, name: &str) -> String {
        format!("{}-{name}", self.base)
    }
}

fn backend(name: &'static str, io: Arc<dyn IO>) -> Backend {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("db").to_str().unwrap().to_string();
    Backend {
        name,
        io,
        _dir: dir,
        base,
    }
}

/// Memory completing inside the submission; memory completing only at `step`; the syscall
/// backend on real files; and io_uring, where completions arrive at `step` from the kernel.
pub(super) fn backends() -> Vec<Backend> {
    let mut all = vec![backend("memory", Arc::new(crate::io::MemoryIO::new()))];
    #[cfg(feature = "io_memory_yield")]
    all.push(backend(
        "memory-deferred",
        Arc::new(crate::io::MemoryYieldIO::new()),
    ));
    all.push(backend(
        "syscall",
        Arc::new(crate::io::SyscallIO::new().unwrap()),
    ));
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    all.push(backend(
        "io_uring",
        Arc::new(crate::io::UringIO::new().unwrap()),
    ));
    all
}

/// Runs `op` until it is done, waiting on each completion it hands back.
pub(super) fn drive<T>(io: &Arc<dyn IO>, mut op: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
    loop {
        match op()? {
            IOResult::Done(v) => return Ok(v),
            IOResult::IO(IOCompletions(c)) => io.wait_for_completion(c)?,
        }
    }
}

/// Tears the second half of the copy of `pid` that `store`, opened at `store_base`, holds now, as
/// a crash mid-write would.
pub(super) fn corrupt(b: &Backend, store_base: &str, store: &PageStore, pid: PageId) {
    let slot = store.location(pid).expect("the page has a copy on disk");
    let path = format!("{store_base}.c{}", pid.class().index());
    let file = b.io.open_file(&path, OpenFlags::Create, false).unwrap();
    let size = pid.class().size() as u64;
    let junk = Arc::new(Buffer::new(vec![0xa5; pid.class().size() / 2]));
    let c = file
        .pwrite(slot * size + size / 2, junk, Completion::new_write(|_| {}))
        .unwrap();
    b.io.wait_for_completion(c).unwrap();
}

/// A tree of leaves under one root, changed through the log the way a transaction layer will.
pub(super) struct World<'a> {
    pub b: &'a Backend,
}

pub(super) fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

pub(super) const LEAVES: usize = 24;
/// Offset of a leaf's counter.
pub(super) const VALUE: usize = HEADER_SIZE + 64;

pub(super) fn slot(i: usize) -> usize {
    HEADER_SIZE + 8 * i
}

impl World<'_> {
    pub fn base(&self) -> String {
        self.b.path("db")
    }

    pub fn open(&self, budget: usize) -> (Arc<Wal>, Arc<BufferManager>, Recovered) {
        let opened =
            checkpoint::open(&self.b.io, &self.base(), budget, DEFAULT_COOLING_SHARE).unwrap();
        (opened.wal, opened.buffer, opened.recovered)
    }

    pub fn log(&self) -> Arc<dyn File> {
        let path = format!("{}.log", self.base());
        self.b
            .io
            .open_file(&path, OpenFlags::Create, false)
            .unwrap()
    }
}

/// Logs each page's change (a new page's creation, then the ranges written), stamps the pages
/// with the frame's LSN, lets them go and waits for durability.
/// A page a commit changed: held exclusively, whether it is new, and the byte ranges written.
pub(super) type Changed<'a> = (ExclusiveGuard<'a>, bool, Vec<(usize, usize)>);

pub(super) fn commit(io: &Arc<dyn IO>, wal: &Arc<Wal>, pages: Vec<Changed<'_>>) {
    let mut records = Vec::new();
    for (page, created, ranges) in &pages {
        if *created {
            records.push(Record::New { pid: page.pid() });
        }
        for &(start, end) in ranges {
            records.push(Record::Range {
                pid: page.pid(),
                offset: start as u32,
                bytes: page.durable_bytes(start..end),
            });
        }
    }
    let lsn = wal.append(&records).unwrap();
    for (mut page, _, _) in pages {
        page::set_lsn(&mut page, lsn);
    }
    drive(io, || wal.wait_durable(lsn)).unwrap();
}

/// Makes everything appended durable, as the next commit would.
pub(super) fn settle(io: &Arc<dyn IO>, wal: &Arc<Wal>) {
    let end = wal.end();
    drive(io, || wal.wait_durable(end)).unwrap();
}

pub(super) fn create(
    io: &Arc<dyn IO>,
    wal: &Arc<Wal>,
    bm: &BufferManager,
    anchor: &Anchor,
) -> PageId {
    let root = drive(io, || bm.allocate(anchor.owner(), anchor.swip(), class(0))).unwrap();
    let root_pid = root.pid();
    let mut pages = Vec::new();
    for i in 0..LEAVES {
        let mut leaf = drive(io, || {
            bm.allocate(root.owner(), root.swip(slot(i)), class(0))
        })
        .unwrap();
        leaf[VALUE..VALUE + 8].copy_from_slice(&0u64.to_le_bytes());
        leaf[VALUE + 8..VALUE + 1008].fill(i as u8);
        pages.push((leaf, true, vec![(VALUE, VALUE + 1008)]));
    }
    // The root's swips, which the log carries as page ids.
    pages.insert(0, (root, true, vec![(slot(0), slot(LEAVES))]));
    commit(io, wal, pages);
    root_pid
}

pub(super) fn bump(
    io: &Arc<dyn IO>,
    wal: &Arc<Wal>,
    bm: &BufferManager,
    anchor: &Anchor,
    i: usize,
    by: u64,
) {
    let root = drive(io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
    let mut leaf = drive(io, || bm.fix_exclusive(root.owner(), root.swip(slot(i)))).unwrap();
    let v = u64::from_le_bytes(leaf[VALUE..VALUE + 8].try_into().unwrap());
    leaf[VALUE..VALUE + 8].copy_from_slice(&(v + by).to_le_bytes());
    commit(io, wal, vec![(leaf, false, vec![(VALUE, VALUE + 8)])]);
}

pub(super) fn values(
    io: &Arc<dyn IO>,
    bm: &BufferManager,
    anchor: &Anchor,
    name: &str,
) -> Vec<u64> {
    let root = drive(io, || bm.fix_shared(anchor.owner(), anchor.swip())).unwrap();
    (0..LEAVES)
        .map(|i| {
            let leaf = drive(io, || bm.fix_shared(root.owner(), root.swip(slot(i)))).unwrap();
            assert!(
                leaf[VALUE + 8..VALUE + 1008].iter().all(|&b| b == i as u8),
                "{name}: leaf {i} torn"
            );
            u64::from_le_bytes(leaf[VALUE..VALUE + 8].try_into().unwrap())
        })
        .collect()
}
