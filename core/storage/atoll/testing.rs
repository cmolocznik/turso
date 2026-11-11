//! Test support: every IO backend the storage runs on, and a driver that steps an operation's
//! `IOResult` to the end the way the VDBE does.

#[cfg(not(shuttle))]
use super::buffer::BufferPool;
pub(super) use super::drive;
#[cfg(not(shuttle))]
use super::page::PageId;
use crate::io::IO;
use crate::sync::Arc;
#[cfg(not(shuttle))]
use crate::types::IOResult;

pub(super) struct Backend {
    pub name: &'static str,
    pub io: Arc<dyn IO>,
    /// Where its files go; held so the directory outlives the test.
    _dir: tempfile::TempDir,
    pub base: String,
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

/// A schedule test's workload seed, drawn from shuttle's generator, which the schedule a failure
/// prints records: replaying that schedule replays the workload too. 32 bits, as the stress tests'
/// seeds are small numbers the workloads scale by ten per thread.
#[cfg(shuttle)]
pub(super) fn workload_seed() -> u64 {
    use shuttle::rand::RngCore;
    shuttle::rand::thread_rng().next_u32().into()
}

/// Runs `test` under `iterations` random schedules and as many PCT schedules (depth 3), each task
/// on a stack of 8 MiB: shuttle's default is too small for the IO completions that run inside
/// their submission.
#[cfg(shuttle)]
pub(super) fn schedules(test: impl Fn() + Send + Sync + 'static, iterations: usize) {
    let test = std::sync::Arc::new(test);
    let config = || {
        let mut config = shuttle::Config::new();
        config.stack_size = 8 << 20;
        config
    };
    // A failing schedule shuttle printed, replayed exactly: its encoded form, in the file this
    // names, instead of the random and PCT schedules.
    if let Ok(path) = std::env::var("ATOLL_SHUTTLE_REPLAY") {
        let encoded = std::fs::read_to_string(&path).expect("the schedule's file");
        shuttle::Runner::new(
            shuttle::scheduler::ReplayScheduler::new_from_encoded(encoded.trim()),
            config(),
        )
        .run(move || test());
        return;
    }
    let t = test.clone();
    shuttle::Runner::new(
        shuttle::scheduler::RandomScheduler::new(iterations),
        config(),
    )
    .run(move || t());
    shuttle::Runner::new(
        shuttle::scheduler::PctScheduler::new(3, iterations),
        config(),
    )
    .run(move || test());
}

/// Takes the write gate shared for `txn`, as Turso's Transaction opcode does for a writing
/// statement: a test's writer needs it once its database requires the gate.
pub(super) fn share(txns: &super::txn::Transactions, txn: &mut super::txn::Txn) {
    let c = crate::io::Completion::new_wait();
    assert!(
        matches!(txns.take_shared(txn, &c).unwrap(), super::txn::Taken::Done),
        "a test writer took the gate behind another"
    );
}

/// The page numbers of `class` the store can have handed out: every page it holds is below the
/// page table's capacity at its height now. A test that finds a store's pages scans this far,
/// never stopping at a run of free numbers, which a drained tree leaves as long as it was.
pub(super) fn page_numbers(store: &super::store::PageStore, class: super::page::SizeClass) -> u64 {
    let map = super::meta::Tree::Map;
    super::radix::capacity(map, store.heights()[map as usize][class.index()])
}

/// The lib's test binary allocates through the system allocator, counting each thread's
/// allocations, so a test can count what an operation allocates on its own thread
/// ([`allocations`]).
#[cfg(not(any(shuttle, atoll_loom, miri)))]
mod counting {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    std::thread_local! {
        /// Allocations made on this thread. Initialized as a constant, with no destructor, so
        /// reading it never allocates.
        pub(super) static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    }

    struct Counting;

    fn count() {
        ALLOCATIONS.with(|n| n.set(n.get() + 1));
    }

    // SAFETY: every call goes to `System` with the arguments it was given.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            count();
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            count();
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            count();
            unsafe { System.realloc(ptr, layout, new_size) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: Counting = Counting;
}

/// Allocations this thread has made, a growth of one in place included.
#[cfg(not(any(shuttle, atoll_loom, miri)))]
pub(super) fn allocations() -> u64 {
    counting::ALLOCATIONS.with(|n| n.get())
}

/// Every Rust source file under Atoll, for the census tests.
#[cfg(not(any(shuttle, atoll_loom)))]
pub(super) fn atoll_sources() -> Vec<std::path::PathBuf> {
    use std::path::{Path, PathBuf};
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&atoll_dir(), &mut out);
    out.sort();
    out
}

/// Atoll's source directory.
#[cfg(not(any(shuttle, atoll_loom)))]
pub(super) fn atoll_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("storage/atoll")
}

// A file's code for a census is the crate's (`crate::source_test`), shared with the IO's.
#[cfg(not(any(shuttle, atoll_loom)))]
pub(super) use crate::source_test::code_of;

// The child-process tests' helpers are the crate's (`crate::child_test`), shared with the IO's.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(super) use crate::child_test::{in_child, record, recorded, Ended};

/// A mutant's pre-clone check can stop before a freed pointer is read. Catch that test panic
/// only in this test harness; production Atoll frames and checked guards still abort.
pub(super) fn catch_test_panic<T>(f: impl FnOnce() -> T) -> std::thread::Result<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
}

/// Pages another thread holds exclusively, taken as this thread passes a point: the pages a
/// step upgrades before the held one are read first, at their versions then.
#[cfg(not(shuttle))]
pub(super) struct HoldAt {
    taken: std::rc::Rc<std::cell::RefCell<Option<Held>>>,
}

/// The holder's thread, the versions it read before holding, and its let-go.
#[cfg(not(shuttle))]
type Held = (
    Vec<u64>,
    std::sync::mpsc::Sender<()>,
    std::thread::JoinHandle<()>,
);

#[cfg(not(shuttle))]
impl HoldAt {
    /// At `point`, `pick` names on the holder's thread the pages to read and those to hold.
    pub(super) fn set(
        point: &'static str,
        io: &Arc<dyn IO>,
        pool: &Arc<BufferPool>,
        pick: impl FnOnce(&Arc<dyn IO>, &Arc<BufferPool>) -> (Vec<PageId>, Vec<PageId>) + Send + 'static,
    ) -> Self {
        let taken = std::rc::Rc::new(std::cell::RefCell::new(None));
        let (io, pool, slot) = (io.clone(), pool.clone(), taken.clone());
        crate::race::at(point, move || {
            let (held, read) = std::sync::mpsc::channel();
            let (let_go, wait) = std::sync::mpsc::channel::<()>();
            let thread = std::thread::spawn(move || {
                let (watch, hold) = pick(&io, &pool);
                let versions = watch
                    .iter()
                    .map(|&pid| {
                        drive(&io, || pool.fix_optimistic(pid, None))
                            .unwrap()
                            .version()
                    })
                    .collect();
                let pages: Vec<_> = hold
                    .iter()
                    .map(|&pid| drive(&io, || pool.fix_exclusive(pid, None)).unwrap())
                    .collect();
                held.send(versions).unwrap();
                wait.recv().unwrap();
                for page in pages {
                    page.release_unchanged();
                }
            });
            *slot.borrow_mut() = Some((read.recv().unwrap(), let_go, thread));
        });
        Self { taken }
    }

    /// The versions read before the holds, once the point was passed.
    pub(super) fn versions(&self) -> Option<Vec<u64>> {
        self.taken.borrow().as_ref().map(|t| t.0.clone())
    }

    pub(super) fn let_go(self) {
        let (_, let_go, thread) = self.taken.borrow_mut().take().expect("the point passed");
        let_go.send(()).unwrap();
        thread.join().unwrap();
    }
}

/// Steps `op` until it ends or passes `point`: what the step that passed it came to, not
/// driven further.
#[cfg(not(shuttle))]
pub(super) fn step_past<T>(
    io: &Arc<dyn IO>,
    point: &'static str,
    mut op: impl FnMut() -> crate::Result<IOResult<T>>,
) -> Option<IOResult<T>> {
    let mut passed = None;
    drive(io, || {
        let out = op()?;
        if crate::race::pending(point) {
            return Ok(match out {
                IOResult::Done(_) => IOResult::Done(()),
                IOResult::IO(io) => IOResult::IO(io),
            });
        }
        passed = Some(out);
        Ok(IOResult::Done(()))
    })
    .unwrap();
    passed
}

crate::thread::thread_local! {
    /// Run, and taken, as a database on this thread begins to close: a test's pause in the close.
    pub(crate) static CLOSE_PAUSE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs the pause a test set on this thread for a database's close, if it set one.
pub(crate) fn pause_close() {
    if let Some(pause) = CLOSE_PAUSE.with(|p| p.borrow_mut().take()) {
        pause();
    }
}
