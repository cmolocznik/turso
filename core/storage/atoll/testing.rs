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

/// A file's code without its comments and the contents of its string literals, which may name
/// what a census looks for (a test's stand-in crate does) without using it. Line breaks stay, so
/// a finding's line is the source's.
#[cfg(not(any(shuttle, atoll_loom)))]
pub(super) fn code_of(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '/' if chars.peek() == Some(&'/') => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '"' => {
                out.push('"');
                while let Some(c) = chars.next() {
                    match c {
                        '\\' => {
                            chars.next();
                        }
                        '"' => break,
                        '\n' => out.push('\n'),
                        _ => {}
                    }
                }
                out.push('"');
            }
            // A char literal holding a quote, or a lifetime: neither opens a string.
            '\'' => {
                out.push('\'');
                let mut ahead = chars.clone();
                match (ahead.next(), ahead.next(), ahead.next()) {
                    (Some('\\'), Some(_), Some('\'')) | (Some(_), Some('\''), _) => {
                        let n = if chars.peek() == Some(&'\\') { 3 } else { 2 };
                        for _ in 0..n {
                            chars.next();
                        }
                        out.push('\'');
                    }
                    _ => {}
                }
            }
            _ => out.push(c),
        }
    }
    out
}

/// How a test's child process ended, and what it printed.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(super) struct Ended {
    pub aborted: bool,
    pub code: Option<i32>,
    pub stderr: String,
    /// The child's own directory ([`child_dir`]), kept until this is dropped.
    pub dir: tempfile::TempDir,
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
const CHILD: &str = "ATOLL_CHILD_TEST";

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
const CHILD_DIR: &str = "ATOLL_CHILD_DIR";

/// In a child process [`in_child`] started, a directory of its own, which the parent reads after.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(super) fn child_dir() -> std::path::PathBuf {
    std::env::var(CHILD_DIR)
        .expect("a child's directory")
        .into()
}

/// Runs `body` in a child process, this test binary running only `test` (its path below the
/// crate, `module_path!()` joined with the test's name), and says how the child ended: for a test
/// whose panic passes a guard of shared state, which ends the process (DESIGN.md 22.11). The
/// child dumps no core. In the child itself, `body` runs and this returns `None`.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(super) fn in_child(test: &str, body: impl FnOnce()) -> Option<Ended> {
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    let test = test.strip_prefix("turso_core::").unwrap_or(test);
    if std::env::var(CHILD).as_deref() == Ok(test) {
        body();
        return None;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([test, "--exact", "--nocapture", "--test-threads", "1"])
        .env(CHILD, test)
        .env(CHILD_DIR, dir.path())
        .env("RUST_BACKTRACE", "0");
    // SAFETY: setrlimit is async-signal-safe, and the closure touches nothing else.
    unsafe {
        command.pre_exec(|| {
            let none = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::setrlimit(libc::RLIMIT_CORE, &none) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let out = command.output().expect("the child runs");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("running 1 test"),
        "the child ran no test named {test}:\n{stdout}"
    );
    Some(Ended {
        aborted: out.status.signal() == Some(libc::SIGABRT),
        code: out.status.code(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        dir,
    })
}

/// An observer's record, appended to `name` in the child's directory and synced, so it survives
/// the child's abort.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(super) fn record(name: &str, what: &str) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(child_dir().join(name))
        .unwrap();
    f.write_all(what.as_bytes()).unwrap();
    f.write_all(b"\n").unwrap();
    f.sync_all().unwrap();
}

/// What the child recorded in `name`.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(super) fn recorded(ended: &Ended, name: &str) -> String {
    std::fs::read_to_string(ended.dir.path().join(name)).unwrap_or_default()
}
