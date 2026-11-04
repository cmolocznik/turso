//! Test support: every IO backend the storage runs on, and a driver that steps an operation's
//! `IOResult` to the end the way the VDBE does.

pub(super) use super::drive;
use crate::io::IO;
use crate::sync::Arc;

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
