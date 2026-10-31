//! Test support: every IO backend the storage runs on, and a driver that steps an operation's
//! `IOResult` to the end the way the VDBE does.

use crate::io::{Buffer, Completion, OpenFlags, IO};
use crate::storage::umbra::page::PageId;
use crate::storage::umbra::store::PageStore;
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

/// Tears the second half of the copy of `pid` the store holds now, as a crash mid-write would.
pub(super) fn corrupt(b: &Backend, store: &PageStore, pid: PageId) {
    let slot = store.location(pid).expect("the page has a copy on disk");
    let file =
        b.io.open_file(
            &format!("{}.c{}", b.path("pages"), pid.class().index()),
            OpenFlags::Create,
            false,
        )
        .unwrap();
    let size = pid.class().size() as u64;
    let junk = Arc::new(Buffer::new(vec![0xa5; pid.class().size() / 2]));
    let c = file
        .pwrite(slot * size + size / 2, junk, Completion::new_write(|_| {}))
        .unwrap();
    b.io.wait_for_completion(c).unwrap();
}
