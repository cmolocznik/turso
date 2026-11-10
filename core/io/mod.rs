use crate::alloc::DynBoxedSlice;
use crate::storage::buffer_pool::ArenaBuffer;
use crate::storage::sqlite3_ondisk::WAL_FRAME_HEADER_SIZE;
use crate::sync::Arc;
use crate::turso_assert;
use crate::{BufferPool, CompletionError, LimboError, Result};
use bitflags::bitflags;
use cfg_block::cfg_block;
use rand::{Rng, RngCore};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::ptr::NonNull;
use std::sync::LazyLock;
use std::{fmt::Debug, pin::Pin};
use turso_macros::AtomicEnum;

cfg_block! {
    #[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))] {
        mod io_uring;
        #[cfg(feature = "fs")]
        pub use io_uring::UringIO;
    }

    #[cfg(all(target_family = "unix", not(miri)))] {
        mod unix;
        #[cfg(feature = "fs")]
        pub use unix::UnixIO;
        pub use unix::UnixIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(all(target_os = "windows", not(miri)))] {
        mod windows_lock;
        mod windows;
        #[cfg(feature = "fs")]
        pub use windows::WindowsIO;
        pub use windows::WindowsIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }

    #[cfg(all(target_os = "windows", feature = "experimental_win_iocp", not(miri)))] {
        mod win_iocp;
        #[cfg(feature = "fs")]
        pub use win_iocp::WindowsIOCP;
    }

    #[cfg(any(not(any(target_family = "unix", target_os = "windows")), miri))] {
        mod generic;
        pub use generic::GenericIO as PlatformIO;
        pub use PlatformIO as SyscallIO;
    }
}

crate::thread::thread_local! {
    /// Set on a ring's delivery thread for its whole life (DESIGN.md 22.5).
    static DELIVERING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Ends the thread with a panic if it is a ring's delivery thread, which never makes a call that can
/// wait: nothing else would deliver what it waits for, and every delivery behind it would wait on
/// the file system, on another holder of a lock, or on time (DESIGN.md 22.5; the calls ruled are
/// listed in `delivery_matrix.rs`). A panic there ends the process.
pub(crate) fn refuse_on_delivery_thread(what: &str) {
    assert!(
        !DELIVERING.with(|d| d.get()),
        "{what} on the ring's delivery thread, which never makes a call that can wait"
    );
}

/// Ends the process for an error of a backend's stepping itself, not of a request, which the
/// request's completion carries (DESIGN.md 22.5, 22.11): a backend whose step fails can no longer
/// deliver or retire what it accepted, so its waiters can neither wait on, since nothing would
/// deliver, nor return, since the backend may still own the buffers they lent it. `during` names
/// the wait it ended in.
pub(crate) fn stepping_failed(during: &'static str) -> ! {
    fatal(during)
}

/// The site of the process's [`fatal`] end, kept for a core dump to show: written just before the
/// abort and read by nothing while the process runs.
static FATAL_SITE: std::sync::atomic::AtomicPtr<u8> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());
static FATAL_SITE_LEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Ends the process at once (DESIGN.md 22.11). Nothing is formatted, probed or written first: each
/// can wait, on a lock another thread holds while it waits on this one, on a reader that has
/// stopped reading, or on a file system that does not answer, and the process must end whatever
/// standard error is. `site` is kept where a core dump shows it, and the error in its caller's
/// frame.
pub(crate) fn fatal(site: &'static str) -> ! {
    FATAL_SITE_LEN.store(site.len(), std::sync::atomic::Ordering::Relaxed);
    FATAL_SITE.store(
        site.as_ptr().cast_mut(),
        std::sync::atomic::Ordering::Relaxed,
    );
    std::process::abort()
}

#[cfg(any(test, feature = "io_crash"))]
pub mod crash;
mod memory;
#[cfg(feature = "io_memory_yield")]
mod memory_yield;
#[cfg(feature = "fs")]
mod vfs;
pub use memory::MemoryIO;
#[cfg(feature = "io_memory_yield")]
pub use memory_yield::MemoryYieldIO;
#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod callback_census;
pub mod clock;
mod common;
mod completions;
#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod delivery_matrix;
#[cfg(any(
    test,
    all(target_os = "windows", feature = "experimental_win_iocp", not(miri))
))]
mod iocp_packet;
#[cfg(test)]
mod retire_tests;
#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod step_census;
#[cfg(test)]
pub(crate) mod wait_tests;
pub use clock::Clock;
pub use completions::*;

/// Platform-independent file identity, analogous to SQLite's `struct unixFileId`.
/// On Unix: (st_dev, st_ino). On Windows: (dwVolumeSerialNumber, nFileIndex).
/// On non-filesystem backends: synthetic hash-based identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId {
    pub dev: u64,
    pub ino: u64,
}

impl FileId {
    /// Synthetic identity from a path hash, for backends without real inodes
    /// (MemoryIO, OPFS, simulators).
    pub fn from_path_hash(path: &str) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut hasher);
        FileId {
            dev: 0,
            ino: hasher.finish(),
        }
    }
}

/// Return the OS-level file identity for a path.
#[cfg(unix)]
pub fn get_file_id(path: &str) -> Result<FileId, std::io::Error> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(path)?;
    Ok(FileId {
        dev: m.dev(),
        ino: m.ino(),
    })
}

#[cfg(windows)]
pub fn get_file_id(path: &str) -> Result<FileId, std::io::Error> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };
    let file = std::fs::File::open(path)?;
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let ret = unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) };
    if ret == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(FileId {
        dev: info.dwVolumeSerialNumber as u64,
        ino: (info.nFileIndexHigh as u64) << 32 | info.nFileIndexLow as u64,
    })
}

#[cfg(not(any(unix, windows)))]
pub fn get_file_id(path: &str) -> Result<FileId, std::io::Error> {
    Ok(FileId::from_path_hash(path))
}

/// Controls which sync mechanism to use for durability.
/// `FullFsync` only has effect on Apple platforms (uses F_FULLFSYNC fcntl).
/// On other platforms, both variants behave the same (regular fsync).
#[derive(Debug, Clone, Copy, PartialEq, Eq, AtomicEnum)]
pub enum FileSyncType {
    /// Regular fsync - flushes to disk but may not flush disk write cache on macOS.
    Fsync,
    /// Full fsync - on macOS uses F_FULLFSYNC to flush disk write cache.
    /// On other platforms, behaves the same as Fsync.
    FullFsync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedWalLockKind {
    LinuxOfd,
    ProcessScopedFcntl,
}

pub trait SharedWalMappedRegion: Send + Sync {
    fn ptr(&self) -> NonNull<u8>;
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait File: Send + Sync {
    fn lock_file(&self, exclusive: bool) -> Result<()>;
    fn unlock_file(&self) -> Result<()>;
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion>;
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion>;
    /// Sync file data&metadata to disk.
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion>;
    /// Makes the data written so far durable, and the metadata only as far as reading that data
    /// back needs it (`fdatasync`). Backends without it sync everything.
    fn sync_data(&self, c: Completion) -> Result<Completion> {
        self.sync(c, FileSyncType::Fsync)
    }
    /// Whether [`pwrite_durable`](Self::pwrite_durable) is supported.
    fn supports_durable_writes(&self) -> bool {
        false
    }
    /// Writes `buffer` at `pos` and completes `c` only once the data is durable, with no separate
    /// sync (`RWF_DSYNC`: a FUA write where the device has one).
    fn pwrite_durable(
        &self,
        _pos: u64,
        _buffer: Arc<Buffer>,
        _c: Completion,
    ) -> Result<Completion> {
        Err(crate::LimboError::InternalError(
            "durable writes are not supported for the given IO implementation".into(),
        ))
    }
    /// Reserves disk blocks for `[pos, pos + len)`, growing the file to cover it, so writes there
    /// cannot fail for want of space.
    fn allocate(&self, _pos: u64, _len: u64) -> Result<()> {
        Err(crate::LimboError::InternalError(
            "allocate is not supported for the given IO implementation".into(),
        ))
    }
    /// Writes `buffers` one after another from `pos`. This default writes each buffer as a part of
    /// its own: the submission accepts or refuses `c` whole (DESIGN.md 22.5). Refused at its first
    /// part, it accepted nothing; refused at a later one, it stops there and `c` carries the
    /// refusal as its error, owned until the parts already accepted retire.
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        if buffers.is_empty() {
            c.deliver(Ok(0));
            return Ok(c);
        }
        if buffers.len() == 1 {
            return self.pwrite(pos, buffers[0].clone(), c);
        }
        let parts = Arc::new(Parts::new(c.clone()));
        let mut pos = pos;
        for (i, buf) in buffers.into_iter().enumerate() {
            let len = buf.len() as u64;
            parts.begin();
            let part = {
                let parts = parts.clone();
                Completion::new_write(move |res| parts.finished(res))
            };
            match self.pwrite(pos, buf, part) {
                Ok(part) => {
                    if i == 0 {
                        c.accept();
                    }
                    let parts = parts.clone();
                    part.on_retired(move || parts.retired());
                }
                Err(e) if i == 0 => return Err(e),
                Err(e) => {
                    parts.refused(match e {
                        LimboError::CompletionError(e) => e,
                        _ => CompletionError::IOError(std::io::ErrorKind::Other, "pwritev"),
                    });
                    break;
                }
            }
            pos += len;
        }
        parts.submitted();
        Ok(c)
    }
    fn size(&self) -> Result<u64>;
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion>;

    /// Optional method implemented by the IO which supports "partial" files (e.g. file with "holes")
    /// This method is used in sync engine only for now (in partial sync mode) and never used in the core database code
    ///
    /// The hole is the contiguous file region which is not allocated by the file-system
    /// If there is a single byte which is allocated within a given range - method must return false in this case
    // todo: need to add custom completion type?
    fn has_hole(&self, _pos: usize, _len: usize) -> Result<bool> {
        Err(crate::LimboError::InternalError(
            "has_hole is not supported for the given IO implementation".into(),
        ))
    }
    /// Optional method implemented by the IO which supports "partial" files (e.g. file with "holes")
    /// This method is used in sync engine only for now (in partial sync mode) and never used in the core database code
    // todo: need to add custom completion type?
    fn punch_hole(&self, _pos: usize, _len: usize) -> Result<()> {
        Err(crate::LimboError::InternalError(
            "punch_hole is not supported for the given IO implementation".into(),
        ))
    }

    fn shared_wal_lock_byte(
        &self,
        _offset: u64,
        _exclusive: bool,
        _kind: SharedWalLockKind,
    ) -> Result<()> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination byte locking is not supported for this file".into(),
        ))
    }

    fn shared_wal_try_lock_byte(
        &self,
        _offset: u64,
        _exclusive: bool,
        _kind: SharedWalLockKind,
    ) -> Result<bool> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination byte locking is not supported for this file".into(),
        ))
    }

    /// Probe whether the caller could hold an exclusive lock without leaving
    /// any lock state changed on return.
    fn shared_wal_probe_exclusive_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        let locked = self.shared_wal_try_lock_byte(offset, true, kind)?;
        if locked {
            self.shared_wal_unlock_byte(offset, kind)?;
        }
        Ok(locked)
    }

    /// Probe whether the caller's existing shared lock can become exclusive,
    /// restoring that shared lock before returning.
    fn shared_wal_probe_exclusive_while_shared_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.shared_wal_unlock_byte(offset, kind)?;
        let probe = match self.shared_wal_probe_exclusive_byte(offset, kind) {
            Ok(probe) => probe,
            Err(err) => {
                self.shared_wal_lock_byte(offset, false, kind)?;
                return Err(err);
            }
        };
        self.shared_wal_lock_byte(offset, false, kind)?;
        Ok(probe)
    }

    fn shared_wal_unlock_byte(&self, _offset: u64, _kind: SharedWalLockKind) -> Result<()> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination byte unlocking is not supported for this file".into(),
        ))
    }

    fn shared_wal_set_len(&self, _len: u64) -> Result<()> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination resizing is not supported for this file".into(),
        ))
    }

    fn shared_wal_map(&self, _offset: u64, _len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        Err(crate::LimboError::InternalError(
            "shared WAL coordination memory mapping is not supported for this file".into(),
        ))
    }
}

pub struct TempFile {
    pub(crate) file: Arc<dyn File>,
}

impl TempFile {
    pub fn new(io: &Arc<dyn IO>) -> Result<Self> {
        #[cfg(not(target_family = "wasm"))]
        {
            Ok(TempFile {
                file: Arc::new(DiskTempFile::new(io.clone())),
            })
        }
        // on WASM in browser we do not support temp files (as we pre-register db files in advance and can't easily create a new one)
        // so, for now, we use in-memory IO for tempfiles in WASM
        #[cfg(target_family = "wasm")]
        {
            use crate::MemoryIO;

            let memory_io = Arc::new(MemoryIO::new());
            let memory_file = memory_io.open_file("tursodb_temp_file", OpenFlags::Create, false)?;
            Ok(TempFile { file: memory_file })
        }
    }

    /// Creates a TempFile respecting the temp_store setting.
    /// When temp_store is Memory, uses in-memory storage.
    /// When temp_store is Default or File, uses file-based storage when
    /// available. In `no-fs` builds, temp storage always falls back to memory.
    pub fn with_temp_store(io: &Arc<dyn IO>, temp_store: crate::TempStore) -> Result<Self> {
        #[cfg(not(target_family = "wasm"))]
        {
            #[cfg(not(feature = "fs"))]
            {
                let _ = (io, temp_store);
                let memory_io = Arc::new(MemoryIO::new());
                let memory_file =
                    memory_io.open_file("tursodb_temp_file", OpenFlags::Create, false)?;
                Ok(TempFile { file: memory_file })
            }
            #[cfg(feature = "fs")]
            {
                if matches!(temp_store, crate::TempStore::Memory) {
                    let memory_io = Arc::new(MemoryIO::new());
                    let memory_file =
                        memory_io.open_file("tursodb_temp_file", OpenFlags::Create, false)?;
                    return Ok(TempFile { file: memory_file });
                }
                // Fall through to file-based for Default and File modes
                Self::new(io)
            }
        }
        #[cfg(target_family = "wasm")]
        {
            // WASM always uses memory, ignore temp_store setting
            let _ = temp_store;
            Self::new(io)
        }
    }
}

impl core::ops::Deref for TempFile {
    type Target = Arc<dyn File>;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

/// A temp file on disk that is created on its first write, as SQLite opens
/// a temp database's file only when the pager first writes a page
/// (`pager_write_pagelist`). Most ephemeral tables, sorters and hash tables
/// fit in memory and never write, so they never touch the file system.
#[cfg(not(target_family = "wasm"))]
struct DiskTempFile {
    io: Arc<dyn IO>,
    created: crate::sync::OnceLock<CreatedTempFile>,
}

#[cfg(not(target_family = "wasm"))]
struct CreatedTempFile {
    file: Arc<dyn File>,
    /// Removes the directory when dropped. Declared after `file` so the file
    /// closes before Windows removes its directory.
    #[cfg_attr(not(test), allow(dead_code, reason = "held for its Drop side effect"))]
    dir: tempfile::TempDir,
}

#[cfg(not(target_family = "wasm"))]
impl DiskTempFile {
    fn new(io: Arc<dyn IO>) -> Self {
        Self {
            io,
            created: crate::sync::OnceLock::new(),
        }
    }

    fn file(&self) -> Result<&Arc<dyn File>> {
        if let Some(created) = self.created.get() {
            return Ok(&created.file);
        }
        let dir = tempfile::tempdir().map_err(|e| crate::error::io_error(e, "tempdir"))?;
        let path = dir.path().join("tursodb_temp_file");
        let path = path.to_str().ok_or_else(|| {
            crate::LimboError::InternalError("temp file path is not valid UTF-8".to_string())
        })?;
        let file = self.io.open_file(path, OpenFlags::Create, false)?;
        Ok(&self
            .created
            .get_or_init(|| CreatedTempFile { file, dir })
            .file)
    }
}

#[cfg(not(target_family = "wasm"))]
impl File for DiskTempFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file()?.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        self.file()?.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.file()?.pread(pos, c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.file()?.pwrite(pos, buffer, c)
    }

    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        self.file()?.pwritev(pos, buffers, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        match self.created.get() {
            Some(created) => created.file.sync(c, sync_type),
            None => {
                c.deliver(Ok(0));
                Ok(c)
            }
        }
    }

    fn size(&self) -> Result<u64> {
        match self.created.get() {
            Some(created) => created.file.size(),
            None => Ok(0),
        }
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.file()?.truncate(len, c)
    }
}

#[cfg(all(test, feature = "fs", not(target_family = "wasm")))]
mod temp_file_tests {
    use super::*;

    #[test]
    fn creates_the_file_on_first_write_and_removes_it_on_drop() {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().expect("platform IO must initialize"));
        let temp_file = DiskTempFile::new(io.clone());
        assert!(temp_file.created.get().is_none());
        assert_eq!(temp_file.size().unwrap(), 0);
        let c = temp_file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync);
        io.wait_for_completion(c.unwrap()).unwrap();
        assert!(temp_file.created.get().is_none());

        let buffer = Arc::new(Buffer::new(vec![7; 512]));
        let c = temp_file.pwrite(0, buffer, Completion::new_write(|_| {}));
        io.wait_for_completion(c.unwrap()).unwrap();
        assert_eq!(temp_file.size().unwrap(), 512);
        let temp_dir = temp_file.created.get().unwrap().dir.path().to_owned();
        assert!(temp_dir.exists());

        drop(temp_file);
        assert!(!temp_dir.exists());
    }

    #[test]
    fn ephemeral_table_opens_a_temp_file_only_when_it_spills() {
        let io = Arc::new(TempFileCountingIO::default());
        let db = crate::Database::open_file(
            io.clone(),
            "ephemeral-temp-file.db",
            Arc::new(crate::SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        let count_union = |rows: i64| -> i64 {
            let sql = format!(
                "SELECT count(*) FROM (\
                 SELECT printf('%02500d', value) FROM generate_series(1, {rows}) \
                 UNION SELECT printf('%02500d', value) FROM generate_series(1, {rows}))"
            );
            let mut stmt = conn.prepare(sql).unwrap();
            let mut count = None;
            stmt.run_with_row_callback(|row| {
                count = Some(row.get::<i64>(0).unwrap());
                Ok(())
            })
            .unwrap();
            count.unwrap()
        };

        assert_eq!(count_union(10), 10);
        assert_eq!(io.temp_files_opened.load(Ordering::Acquire), 0);

        assert_eq!(count_union(4_000), 4_000);
        assert!(io.temp_files_opened.load(Ordering::Acquire) > 0);
    }

    #[derive(Default)]
    struct TempFileCountingIO {
        inner: MemoryIO,
        temp_files_opened: crate::sync::atomic::AtomicUsize,
    }

    use crate::sync::atomic::Ordering;
    use crate::{MonotonicInstant, WallClockInstant};

    impl Clock for TempFileCountingIO {
        fn current_time_monotonic(&self) -> MonotonicInstant {
            self.inner.current_time_monotonic()
        }
        fn current_time_wall_clock(&self) -> WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }

    impl IO for TempFileCountingIO {
        fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
            if path.ends_with("tursodb_temp_file") {
                self.temp_files_opened.fetch_add(1, Ordering::AcqRel);
            }
            self.inner.open_file(path, flags, direct)
        }
        fn remove_file(&self, path: &str) -> Result<()> {
            self.inner.remove_file(path)
        }
        fn file_id(&self, path: &str) -> Result<FileId> {
            self.inner.file_id(path)
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct OpenFlags(i32);

// OpenFlags is a newtype over i32, which is inherently Send+Sync.
// The assertion below verifies this at compile time.
crate::assert::assert_send_sync!(OpenFlags);

bitflags! {
    impl OpenFlags: i32 {
        const None = 0b00000000;
        const Create = 0b0000001;
        const ReadOnly = 0b0000010;
        const NoLock = 0b0000100;
        /// Bypasses the OS page cache (`O_DIRECT`). Every read and write on the file must then
        /// use a buffer address, an offset and a length that are multiples of
        /// [`DIRECT_IO_ALIGNMENT`].
        const Direct = 0b0001000;
    }
}

/// The alignment `O_DIRECT` IO needs: the logical block size of 4Kn devices, the largest in use.
pub const DIRECT_IO_ALIGNMENT: usize = 4096;

impl Default for OpenFlags {
    fn default() -> Self {
        Self::Create
    }
}

/// Who delivers a backend's requests, which says how a blocking caller waits for one (DESIGN.md
/// 22.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Every request inside its submission: none is left to deliver once its submission returns.
    /// What a caller still waits for, another thread finishes, and the caller parks until it does.
    Inline,
    /// A thread of the backend's own delivers every request (the ring's): a caller parks until it
    /// does, and stepping does nothing.
    Thread,
    /// A request is delivered only inside `step`: a caller steps the backend until what it waits
    /// for finishes.
    Pumped,
}

/// What one step of a pumped backend found ([`IO::try_step`], DESIGN.md 22.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stepped {
    /// It delivered a request: whatever waits may have moved, and another step may deliver more.
    Delivered,
    /// It held nothing to deliver, and registered the waker it was handed, which it wakes as soon
    /// as it holds something.
    Empty,
    /// It stepped and cannot say what it found: a backend that does not tell.
    Unsaid,
}

/// A waiter's key for its registration with a pumped backend (DESIGN.md 22.5): the address of
/// something the waiter owns and keeps in place while its registration may stand, a blocking
/// wait's [`ReadyPlace`] or a pinned future's own completion key. A backend keeps one
/// registration per key, with the last waker given, and a waiter takes back exactly its own by its
/// key before that place goes: two live waiters never share an address, and an address used again
/// belongs to a waiter whose predecessor has withdrawn. Two futures of one task share the task's
/// waker, and each withdraws only what it registered. No key is minted, so none runs out.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReadyKey(std::num::NonZeroUsize);

impl ReadyKey {
    /// The key of a pinned future, the address of its own completion key.
    pub(crate) fn of_place(key: crate::io::WakerKey) -> Self {
        Self(key)
    }
}

/// What a blocking waiter keeps in place, unmoved, for as long as its registration with a pumped
/// backend may stand, withdrawing it before the place goes: its address is the waiter's key.
#[derive(Default)]
pub struct ReadyPlace {
    /// A byte, so that two places alive at once never share an address.
    _byte: u8,
}

impl ReadyPlace {
    pub fn key(&self) -> ReadyKey {
        let at = std::ptr::from_ref(self) as usize;
        ReadyKey(std::num::NonZeroUsize::new(at).expect("a place at address zero"))
    }
}

/// The registrations a pumped backend holds, one per key, under the exclusion its submissions
/// take; a submission takes them all and wakes them after it (DESIGN.md 22.5). Every waker it
/// gives back is the caller's to drop after the exclusion.
#[derive(Default)]
pub struct Registrations(Vec<(ReadyKey, std::task::Waker)>);

impl Registrations {
    /// Registers `waker`, cloned before the exclusion was taken, under `key`: a key already held
    /// keeps its waker unless this one wakes another task. What is left over.
    pub fn register(&mut self, key: ReadyKey, waker: std::task::Waker) -> Option<std::task::Waker> {
        match self.0.iter_mut().find(|(held, _)| *held == key) {
            Some((_, held)) if held.will_wake(&waker) => Some(waker),
            Some((_, held)) => Some(std::mem::replace(held, waker)),
            None => {
                self.0.push((key, waker));
                None
            }
        }
    }

    /// Takes back `key`'s registration.
    pub fn withdraw(&mut self, key: ReadyKey) -> Option<std::task::Waker> {
        let at = self.0.iter().position(|(held, _)| *held == key)?;
        Some(self.0.swap_remove(at).1)
    }

    /// Every registration, for a submission to wake after the exclusion.
    pub fn take(&mut self) -> Vec<(ReadyKey, std::task::Waker)> {
        std::mem::take(&mut self.0)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A waker that unparks `thread`: a blocking waiter's, for what it waits on and for the backend's
/// next work.
pub(crate) fn unparking(thread: crate::thread::Thread) -> std::task::Waker {
    struct Unpark(crate::thread::Thread);
    impl std::task::Wake for Unpark {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.unpark();
        }
    }
    std::task::Waker::from(std::sync::Arc::new(Unpark(thread)))
}

pub trait IO: Clock + Send + Sync {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>>;

    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        self.open_file(path, OpenFlags::Create | OpenFlags::NoLock, false)
    }

    // remove_file is used in the sync-engine
    fn remove_file(&self, path: &str) -> Result<()>;

    /// Makes the files created in or removed from the directory `path` so far durable. Backends
    /// with no directories to sync do nothing.
    fn sync_directory(&self, _path: &str) -> Result<()> {
        Ok(())
    }

    /// Renames the file `from` to `to`, replacing any file `to`, in one step: no moment has
    /// neither name. Durable once their directory is synced. `false`, with nothing done, from a
    /// backend that cannot.
    fn rename_file(&self, _from: &str, _to: &str) -> Result<bool> {
        Ok(false)
    }

    /// Whether this IO backend can back host-filesystem shared WAL coordination.
    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }

    fn step(&self) -> Result<()> {
        Ok(())
    }

    /// Who delivers this backend's requests. A backend that does not say is pumped, which is
    /// right for any backend: a waiter steps it until what it waits for finishes.
    fn delivery(&self) -> Delivery {
        Delivery::Pumped
    }

    /// One step of a pumped backend that says what it found. `Empty` only after registering
    /// `ready` under `key` and the exclusion its submissions take, so a request submitted after the look
    /// wakes it: a waiter then parks until the backend holds something or what it waits on moves,
    /// and spends nothing on a backend that holds nothing for it. A backend that cannot say steps
    /// and answers `Unsaid`, and its waiters yield between steps.
    fn try_step(&self, key: ReadyKey, ready: &std::task::Waker) -> Result<Stepped> {
        let _ = (key, ready);
        self.step()?;
        Ok(Stepped::Unsaid)
    }

    /// Whether this backend's steps say what they found: its `try_step` answers `Delivered` or
    /// `Empty`, never `Unsaid`. Atoll runs over a pumped backend only if it does, so none of its
    /// waits polls (DESIGN.md 22.5); outside Atoll a wait over one that does not yields between
    /// its steps, as Turso's waits always have.
    fn says_what_a_step_found(&self) -> bool {
        false
    }

    /// Takes back the registration a step that answered `Empty` left under `key`, if the backend
    /// still holds it: every waiter that registered withdraws as its wait ends, so a backend holds
    /// registrations only for waits under way, never one per wait it ever had, and a waiter takes
    /// back its own alone, whoever shares its waker. A backend that registers nothing has nothing
    /// to take back.
    fn withdraw(&self, key: ReadyKey) {
        let _ = key;
    }

    /// Blocks the calling thread until `c` finishes (DESIGN.md 22.5): a wait another thread
    /// completes, or a request of a backend that delivers inline or on a thread of its own, parks
    /// until it is finished; a pumped backend is stepped while it delivers, and once it holds
    /// nothing the waiter parks until `c` finishes or the backend holds something, whoever submits
    /// it (a backend that cannot say is stepped with the processor yielded between steps). Its
    /// result is its owner's to read. A step that fails ends the process, the backend having lost
    /// what it accepted.
    fn wait(&self, c: &Completion) {
        refuse_on_delivery_thread("a wait for a completion");
        if c.finished() {
            return;
        }
        if c.is_remote_wait() || self.delivery() != Delivery::Pumped {
            c.block_on();
            return;
        }
        let (place, ready) = (ReadyPlace::default(), unparking(crate::thread::current()));
        let mut registered = false;
        while !c.finished() {
            match self.try_step(place.key(), &ready) {
                Err(_) => stepping_failed("a wait for a completion"),
                Ok(Stepped::Delivered) => {}
                // What `c` waits on may be another thread's to submit or finish.
                Ok(Stepped::Unsaid) => crate::thread::yield_now(),
                Ok(Stepped::Empty) => {
                    registered = true;
                    // Registered before the test: a finish after it unparks this thread.
                    c.set_waker(&ready);
                    if !c.finished() {
                        crate::thread::park();
                    }
                }
            }
        }
        if registered {
            self.withdraw(place.key());
        }
    }

    fn cancel(&self, c: &[Completion]) -> Result<()> {
        c.iter().for_each(|c| c.abort());
        Ok(())
    }

    /// Waits until no backend owns `c`: for a request, until it is retired, or at once if it was
    /// never accepted; for a group, until every request beneath it is (DESIGN.md 22.5). A
    /// cancelled request is finished at once and still owned until its backend lets it go, with
    /// the buffers it reads into or writes from: retirement, not finishing, says they are free. On
    /// a pumped backend that holds nothing, the waiter parks until the request retires (a finish
    /// elsewhere still running its callback, a carrier retiring) or the backend holds something.
    /// The caller is the owner of what it waits for, and has stopped submitting to it.
    fn wait_retired(&self, c: &Completion) -> Result<()> {
        self.drain_completions(std::slice::from_ref(c))
    }

    /// [`wait_retired`](Self::wait_retired) on each of `completions`, each request beneath them
    /// counted once. Used after `cancel()`, so cancelled requests have given back their buffers
    /// before the caller goes on. Only the completions passed are waited for: other threads keep
    /// submitting meanwhile.
    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        refuse_on_delivery_thread("a wait for retirement");
        let pumped = self.delivery() == Delivery::Pumped;
        // One waker for the whole drain, taken back from the backend when the drain ends.
        let place = ReadyPlace::default();
        let ready = pumped.then(|| unparking(crate::thread::current()));
        let mut registered = false;
        let mut seen = std::collections::HashSet::new();
        for c in completions {
            for request in c.requests(&mut seen) {
                if let Some(ready) = &ready {
                    let mut observed = false;
                    while request.is_owned() {
                        match self.try_step(place.key(), ready) {
                            Err(_) => stepping_failed("a wait for retirement"),
                            Ok(Stepped::Delivered) => {}
                            Ok(Stepped::Unsaid) => crate::thread::yield_now(),
                            Ok(Stepped::Empty) => {
                                registered = true;
                                // Observed before the test: a retirement after it unparks this
                                // thread, and stays observed until it comes.
                                if !observed {
                                    let thread = crate::thread::current();
                                    request.on_retired(move || thread.unpark());
                                    observed = true;
                                }
                                if request.is_owned() {
                                    crate::thread::park();
                                }
                            }
                        }
                    }
                } else if request.is_owned() {
                    // Whoever delivers it, on another thread, lets it go and wakes this one.
                    let thread = crate::thread::current();
                    request.on_retired(move || thread.unpark());
                    while request.is_owned() {
                        crate::thread::park();
                    }
                }
            }
        }
        if registered {
            self.withdraw(place.key());
        }
        Ok(())
    }

    /// [`wait`](Self::wait), then `c`'s own result.
    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        self.wait(&c);
        match c.get_error() {
            Some(e) => Err(e.into()),
            None => Ok(()),
        }
    }

    fn generate_random_number(&self) -> i64 {
        rand::rng().random()
    }

    /// Fill `dest` with random data.
    fn fill_bytes(&self, dest: &mut [u8]) {
        rand::rng().fill_bytes(dest);
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        Arc::new(MemoryIO::new())
    }

    fn register_fixed_buffer(&self, _ptr: NonNull<u8>, _len: usize) -> Result<u32> {
        Err(crate::LimboError::InternalError(
            "unsupported operation".to_string(),
        ))
    }

    /// Yield the current thread to the scheduler.
    /// Used for backoff in contended lock acquisition.
    fn yield_now(&self) {
        crate::thread::yield_now();
    }

    /// Sleep for the specified duration.
    /// Used for progressive backoff in contended lock acquisition.
    fn sleep(&self, duration: std::time::Duration) {
        refuse_on_delivery_thread("a sleep");
        crate::thread::sleep(duration);
    }

    /// Return the file identity for the given path.
    /// Default uses OS-level metadata; non-filesystem backends override
    /// with synthetic hash-based identity.
    fn file_id(&self, path: &str) -> Result<FileId> {
        refuse_on_delivery_thread("a file's identity");
        get_file_id(path).map_err(|e| {
            crate::LimboError::InternalError(format!(
                "failed to get file identity for '{path}': {e}"
            ))
        })
    }
}

/// The parts of a write [`File::pwritev`]'s default makes one buffer at a time, and the write they
/// carry out. Each count holds the submitter's token until every part is submitted, so the
/// parent neither finishes nor retires while parts are still being added. The parent finishes
/// once every part has, with the first error or the bytes written, and retires once every part
/// has: a part finishes before it retires, and the finishing token goes first.
struct Parts {
    parent: Completion,
    unfinished: crate::sync::atomic::AtomicUsize,
    owned: crate::sync::atomic::AtomicUsize,
    written: crate::sync::atomic::AtomicUsize,
    error: std::sync::OnceLock<CompletionError>,
}

impl Parts {
    fn new(parent: Completion) -> Self {
        use crate::sync::atomic::AtomicUsize;
        Self {
            parent,
            unfinished: AtomicUsize::new(1),
            owned: AtomicUsize::new(1),
            written: AtomicUsize::new(0),
            error: std::sync::OnceLock::new(),
        }
    }

    /// A part about to be submitted.
    fn begin(&self) {
        use crate::sync::atomic::Ordering;
        self.unfinished.fetch_add(1, Ordering::AcqRel);
        self.owned.fetch_add(1, Ordering::AcqRel);
    }

    /// A part's callback.
    fn finished(&self, res: std::result::Result<i32, CompletionError>) {
        use crate::sync::atomic::Ordering;
        match res {
            Ok(n) => {
                self.written.fetch_add(n as usize, Ordering::AcqRel);
            }
            Err(e) => {
                let _ = self.error.set(e);
            }
        }
        self.one_finished();
    }

    fn one_finished(&self) {
        use crate::sync::atomic::Ordering;
        if self.unfinished.fetch_sub(1, Ordering::AcqRel) == 1 {
            match self.error.get() {
                Some(e) => self.parent.error(*e),
                None => self
                    .parent
                    .complete(self.written.load(Ordering::Acquire) as i32),
            }
        }
    }

    /// A part retired, or let go by its backend.
    fn retired(&self) {
        use crate::sync::atomic::Ordering;
        if self.owned.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.parent.retire();
        }
    }

    /// A part refused whole: it never runs, and the parent carries the refusal.
    fn refused(&self, e: CompletionError) {
        let _ = self.error.set(e);
        self.one_finished();
        self.retired();
    }

    /// Every part is submitted: the submitter's tokens go, the finishing one first.
    fn submitted(&self) {
        self.one_finished();
        self.retired();
    }
}

/// Batches multiple vectored writes for submission.
pub struct WriteBatch<'a> {
    file: Arc<dyn File>,
    ops: Vec<WriteOp<'a>>,
}

struct WriteOp<'a> {
    pos: u64,
    bufs: &'a [Arc<Buffer>],
}

impl<'a> WriteBatch<'a> {
    pub fn new(file: Arc<dyn File>) -> Self {
        Self {
            file,
            ops: Vec::new(),
        }
    }

    #[inline]
    pub fn writev(&mut self, pos: u64, bufs: &'a [Arc<Buffer>]) {
        if !bufs.is_empty() {
            self.ops.push(WriteOp { pos, bufs });
        }
    }

    /// Total bytes across all operations.
    #[inline]
    pub fn total_bytes(&self) -> usize {
        self.ops
            .iter()
            .map(|op| op.bufs.iter().map(|b| b.len()).sum::<usize>())
            .sum()
    }

    /// Submit all writes. Returns completions caller must wait on. Each
    /// write is added to `group`, when given, before it is submitted.
    #[inline]
    pub fn submit(self, mut group: Option<&mut CompletionGroup>) -> Result<Vec<Completion>> {
        let mut completions = Vec::with_capacity(self.ops.len());
        for WriteOp { pos, bufs } in self.ops {
            let total_len = bufs.iter().map(|b| b.len()).sum::<usize>() as i32;
            let c = Completion::new_write(move |res| {
                let Ok(bytes_written) = res else {
                    return;
                };
                turso_assert!(
                    bytes_written == total_len,
                    "pwritev wrote {bytes_written} bytes, expected {total_len}"
                );
            });
            if let Some(group) = group.as_deref_mut() {
                group.add(&c);
            }
            completions.push(self.file.pwritev(pos, bufs.to_vec(), c)?);
        }
        Ok(completions)
    }

    /// Returns the file for fsync after writes complete.
    #[inline]
    pub const fn file(&self) -> &Arc<dyn File> {
        &self.file
    }
}

pub struct BufferData(Pin<Box<std::cell::UnsafeCell<[u8]>>>);

// SAFETY: this type exposes byte references shared, or mutable only through an exclusive owner
// borrow. A raw mutable pointer grants no aliasing permission; Buffer's callers must still
// exclude overlapping byte access. UnsafeCell preserves writable provenance, not exclusion.
unsafe impl Sync for BufferData {}

impl BufferData {
    fn new(bytes: Vec<u8>) -> Self {
        let bytes = Box::into_raw(bytes.into_boxed_slice());
        // SAFETY: UnsafeCell is transparent, including the slice metadata. This consumes the
        // allocation without copying it, and the original byte layout remains its deallocation.
        let bytes = unsafe { Box::from_raw(bytes as *mut std::cell::UnsafeCell<[u8]>) };
        Self(Pin::new(bytes))
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.0.get().len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.0.get().cast::<u8>().cast_const()
    }
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.0.get().cast::<u8>()
    }
}

impl Debug for BufferData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SAFETY: a shared byte view for this formatting borrow; no pointer is used to write.
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }.fmt(f)
    }
}

#[derive(Clone)]
pub enum SharedBufferData {
    Full(Arc<DynBoxedSlice<u8>>),
    View(SharedBufferView),
}

#[derive(Clone)]
pub struct SharedBufferView {
    data: Arc<DynBoxedSlice<u8>>,
    start: usize,
}

impl SharedBufferView {
    fn new(data: Arc<DynBoxedSlice<u8>>, start: usize) -> Self {
        assert!(
            start <= data.len(),
            "SharedBufferData::new_view: start ({start}) > data.len() ({})",
            data.len()
        );
        Self { data, start }
    }

    pub fn len(&self) -> usize {
        self.data.len() - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data.as_ref().as_ref()[self.start..]
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ref().as_ptr().add(self.start) }
    }
}

impl SharedBufferData {
    pub fn new(data: Arc<DynBoxedSlice<u8>>) -> Self {
        Self::Full(data)
    }

    pub fn new_view(data: Arc<DynBoxedSlice<u8>>, start: usize) -> Self {
        Self::View(SharedBufferView::new(data, start))
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Full(data) => data.len(),
            Self::View(view) => view.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Full(data) => data.as_ref().as_ref(),
            Self::View(view) => view.as_slice(),
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Full(data) => data.as_ref().as_ptr(),
            Self::View(view) => view.as_ptr(),
        }
    }
}

pub enum Buffer {
    Heap(BufferData),
    Shared(SharedBufferData),
    /// A heap buffer with a logical start offset: only `data[start..]` is
    /// exposed via [`Buffer::as_slice`] / [`Buffer::len`]. Used to skip a
    /// pre-allocated prefix without shifting bytes in memory before I/O.
    HeapView {
        data: BufferData,
        start: usize,
    },
    Pooled(ArenaBuffer),
    Region(RegionBuffer),
}

/// Bytes in memory someone else owns, such as an aligned block for direct IO. The owner is kept
/// alive while the buffer exists.
pub struct RegionBuffer {
    ptr: NonNull<u8>,
    len: usize,
    _owner: Arc<dyn std::any::Any + Send + Sync>,
}

// SAFETY: the bytes are reached only through the buffer, and `RegionBuffer::new`'s caller
// promises they stay valid while the owner lives.
unsafe impl Send for RegionBuffer {}
unsafe impl Sync for RegionBuffer {}

impl RegionBuffer {
    /// # Safety
    ///
    /// `ptr..ptr + len` must stay valid for as long as `owner` lives, and nothing else may write
    /// those bytes while an IO holding the buffer is in flight.
    pub unsafe fn new(
        owner: Arc<dyn std::any::Any + Send + Sync>,
        ptr: NonNull<u8>,
        len: usize,
    ) -> Self {
        Self {
            ptr,
            len,
            _owner: owner,
        }
    }
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pooled(p) => write!(f, "Pooled(len={})", p.logical_len()),
            Self::Region(r) => write!(f, "Region(len={})", r.len),
            Self::Heap(buf) => write!(f, "{buf:?}: {}", buf.len()),
            Self::Shared(buf) => write!(f, "Shared(len={})", buf.len()),
            Self::HeapView { data, start } => {
                write!(
                    f,
                    "HeapView({start}..{}, view_len={})",
                    data.len(),
                    data.len() - start
                )
            }
        }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        match self {
            Self::Heap(buf) | Self::HeapView { data: buf, .. } => {
                let underlying_len = buf.len();
                TEMP_BUFFER_CACHE.with(|cache| {
                    let mut cache = cache.borrow_mut();
                    // take ownership of the buffer by swapping it with a dummy
                    let buffer = std::mem::replace(buf, BufferData::new(Vec::new()));
                    cache.return_buffer(buffer, underlying_len);
                });
            }
            Self::Pooled(_) | Self::Shared(_) | Self::Region(_) => {}
        }
    }
}

impl Buffer {
    pub fn new(data: Vec<u8>) -> Self {
        tracing::trace!("buffer::new({:?})", data);
        Self::Heap(BufferData::new(data))
    }

    pub fn new_shared(data: Arc<DynBoxedSlice<u8>>) -> Self {
        Self::Shared(SharedBufferData::new(data))
    }

    pub fn new_shared_data(data: SharedBufferData) -> Self {
        Self::Shared(data)
    }

    /// Wraps `data` so that only bytes `[start..]` are visible via
    /// [`Buffer::as_slice`] / [`Buffer::len`]. The skipped prefix lives in
    /// memory but is never read by the I/O layer — useful when a caller
    /// has pre-allocated optional framing room at the front of a buffer
    /// and wants to elide it on a particular write without a memmove.
    pub fn new_with_start(data: Vec<u8>, start: usize) -> Self {
        assert!(
            start <= data.len(),
            "Buffer::new_with_start: start ({start}) > data.len() ({})",
            data.len()
        );
        Self::HeapView {
            data: BufferData::new(data),
            start,
        }
    }

    /// Returns the index of the underlying `Arena` if it was registered with
    /// io_uring. Only for use with `UringIO` backend.
    pub fn fixed_id(&self) -> Option<u32> {
        match self {
            Self::Heap(..) | Self::HeapView { .. } | Self::Shared(..) | Self::Region(..) => None,
            Self::Pooled(buf) => buf.fixed_id(),
        }
    }

    pub fn new_pooled(buf: ArenaBuffer) -> Self {
        Self::Pooled(buf)
    }

    pub fn new_region(region: RegionBuffer) -> Self {
        Self::Region(region)
    }

    pub fn new_temporary(size: usize) -> Self {
        TEMP_BUFFER_CACHE.with(|cache| {
            if let Some(buffer) = cache.borrow_mut().get_buffer(size) {
                Self::Heap(buffer)
            } else {
                Self::Heap(BufferData::new(vec![0; size]))
            }
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Heap(buf) => buf.len(),
            Self::Shared(buf) => buf.len(),
            Self::HeapView { data, start } => data.len() - *start,
            Self::Pooled(buf) => buf.logical_len(),
            Self::Region(r) => r.len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Heap(buf) => {
                // SAFETY: The buffer is guaranteed to be valid for the lifetime of the slice
                unsafe { std::slice::from_raw_parts(buf.as_ptr(), buf.len()) }
            }
            Self::Shared(buf) => buf.as_slice(),
            Self::HeapView { data, start } => {
                // SAFETY: `start` was bounds-checked at construction; the buffer
                // is valid for the lifetime of the returned slice.
                unsafe {
                    std::slice::from_raw_parts(data.as_ptr().add(*start), data.len() - *start)
                }
            }
            Self::Pooled(buf) => buf,
            // SAFETY: valid while the owner lives, which the buffer keeps alive.
            Self::Region(r) => unsafe { std::slice::from_raw_parts(r.ptr.as_ptr(), r.len) },
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Heap(buf) => buf.as_ptr(),
            Self::Shared(buf) => buf.as_ptr(),
            Self::HeapView { data, start } => unsafe { data.as_ptr().add(*start) },
            Self::Pooled(buf) => buf.as_ptr(),
            Self::Region(r) => r.ptr.as_ptr(),
        }
    }
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut u8 {
        match self {
            Self::Heap(buf) => buf.as_mut_ptr(),
            Self::Shared(_) => panic!("Buffer::Shared is immutable"),
            Self::HeapView { data, start } => unsafe { data.as_mut_ptr().add(*start) },
            Self::Pooled(buf) => buf.as_mut_ptr(),
            Self::Region(r) => r.ptr.as_ptr(),
        }
    }

    #[inline]
    pub fn is_pooled(&self) -> bool {
        matches!(self, Self::Pooled(..))
    }

    #[inline]
    pub fn is_heap(&self) -> bool {
        matches!(self, Self::Heap(..) | Self::HeapView { .. })
    }
}

crate::thread::thread_local! {
    /// thread local cache to re-use temporary buffers to prevent churn when pool overflows
    pub static TEMP_BUFFER_CACHE: RefCell<TempBufferCache> = RefCell::new(TempBufferCache::new());
}

#[cfg(test)]
mod buffer_tests {
    use super::*;

    fn shared_bytes(bytes: &[u8]) -> Arc<DynBoxedSlice<u8>> {
        let mut data =
            <crate::alloc::DynVec<u8> as crate::alloc::TursoVecInExt<
                u8,
                crate::alloc::DynAllocator,
            >>::try_with_capacity_in(bytes.len(), crate::alloc::DynAllocator::default())
            .expect("failed to allocate shared buffer test data");
        data.extend_from_slice(bytes);
        Arc::new(data.into_boxed_slice())
    }

    #[test]
    fn shared_buffer_exposes_arc_bytes() {
        let data = shared_bytes(&[1, 2, 3, 4]);
        let buffer = Buffer::new_shared(data.clone());

        assert_eq!(buffer.len(), 4);
        assert_eq!(buffer.as_slice(), &[1, 2, 3, 4]);
        assert_eq!(buffer.as_ptr(), data.as_ref().as_ptr());
        assert!(!buffer.is_heap());
        assert!(!buffer.is_pooled());
    }

    #[test]
    fn shared_buffer_view_exposes_tail_without_copying() {
        let data = shared_bytes(&[0, 1, 2, 3, 4]);
        let shared = SharedBufferData::new_view(data.clone(), 2);
        let buffer = Buffer::new_shared_data(shared.clone());

        assert_eq!(shared.len(), 3);
        assert_eq!(shared.as_slice(), &[2, 3, 4]);
        assert_eq!(shared.as_ptr(), unsafe { data.as_ref().as_ptr().add(2) });
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.as_slice(), &[2, 3, 4]);
        assert_eq!(buffer.as_ptr(), shared.as_ptr());
    }
    #[test]
    fn heap_and_view_mutable_pointers_keep_write_permission_without_copying() {
        for start in [0, 8] {
            let bytes = vec![0; 512 + start];
            let allocation = bytes.as_ptr();
            let buffer = if start == 0 {
                Buffer::new(bytes)
            } else {
                Buffer::new_with_start(bytes, start)
            };
            assert_eq!(buffer.as_ptr(), unsafe { allocation.add(start) });
            buffer.as_mut_slice().fill(71);
            assert_eq!(buffer.len(), 512);
            assert!(buffer.as_slice().iter().all(|&byte| byte == 71));
            assert_eq!(buffer.as_ptr(), unsafe { allocation.add(start) });
        }
    }

    #[test]
    fn a_pooled_mutable_pointer_does_not_start_from_a_shared_byte_slice() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let pool = BufferPool::begin_init(&io, 64 * 1024);
        pool.finalize_with_page_size(4096).unwrap();
        let buffer = pool.get_page();
        assert!(buffer.is_pooled());
        // SAFETY: this test owns the frame and no other byte view exists while it is written.
        unsafe { buffer.as_mut_ptr().write(53) };
        assert_eq!(buffer.as_slice()[0], 53);
    }
}

/// A cache for temporary or any additional `Buffer` allocations beyond
/// what the `BufferPool` has room for, or for use before the pool is
/// fully initialized.
pub(crate) struct TempBufferCache {
    /// The `[Database::page_size]` at the time the cache is initiated.
    page_size: usize,
    /// Cache of buffers of size `self.page_size`.
    page_buffers: Vec<BufferData>,
    /// Cache of buffers of size `self.page_size` + WAL_FRAME_HEADER_SIZE.
    wal_frame_buffers: Vec<BufferData>,
    /// Maximum number of buffers that will live in each cache.
    max_cached: usize,
}

impl TempBufferCache {
    const DEFAULT_MAX_CACHE_SIZE: usize = 256;

    fn new() -> Self {
        Self {
            page_size: BufferPool::DEFAULT_PAGE_SIZE,
            page_buffers: Vec::with_capacity(8),
            wal_frame_buffers: Vec::with_capacity(8),
            max_cached: Self::DEFAULT_MAX_CACHE_SIZE,
        }
    }

    /// If the `[Database::page_size]` is set, any temporary buffers that might
    /// exist prior need to be cleared and new `page_size` needs to be saved.
    pub fn reinit_cache(&mut self, page_size: usize) {
        self.page_buffers.clear();
        self.wal_frame_buffers.clear();
        self.page_size = page_size;
    }

    fn get_buffer(&mut self, size: usize) -> Option<BufferData> {
        match size {
            sz if sz == self.page_size => self.page_buffers.pop(),
            sz if sz == (self.page_size + WAL_FRAME_HEADER_SIZE) => self.wal_frame_buffers.pop(),
            _ => None,
        }
    }

    fn return_buffer(&mut self, buff: BufferData, len: usize) {
        let sz = self.page_size;
        let cache = match len {
            n if n.eq(&sz) => &mut self.page_buffers,
            n if n.eq(&(sz + WAL_FRAME_HEADER_SIZE)) => &mut self.wal_frame_buffers,
            _ => return,
        };
        if self.max_cached > cache.len() {
            cache.push(buff);
        }
    }
}

// Runtime-registrable Rust IO backends, resolved by `Database::io_for_vfs`.
#[allow(clippy::type_complexity)]
static IO_REGISTRY: LazyLock<parking_lot::Mutex<HashMap<String, Arc<dyn IO>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

const BUILTIN_VFS_NAMES: &[&str] = &["memory", "syscall", "io_uring", "experimental_win_iocp"];

/// Register a named Rust IO backend.
///
/// Once registered, it can be used via [`crate::Database::io_for_vfs`] or through
/// any language binding's `vfs=` parameter (Go DSN, Python kwarg, etc.).
///
/// Re-registering the same name replaces the previous backend. Registered
/// names take precedence over C VFS extensions and built-in backends
/// (`"memory"`, `"syscall"`, `"io_uring"`), so registering a built-in name
/// will shadow the default implementation.
///
/// # Errors
///
/// Returns [`crate::LimboError::InvalidArgument`] if `name` is empty.
pub fn register_io(name: &str, io: Arc<dyn IO>) -> crate::Result<()> {
    if name.is_empty() {
        return Err(crate::LimboError::InvalidArgument(
            "IO backend name must not be empty".into(),
        ));
    }
    if BUILTIN_VFS_NAMES.contains(&name) {
        tracing::warn!("registered IO backend \"{name}\" shadows a built-in VFS");
    }
    IO_REGISTRY.lock().insert(name.to_string(), io);
    Ok(())
}

/// Remove a registered Rust IO backend by name.
///
/// Returns `true` if an entry was removed, `false` if the name was not found.
pub fn unregister_io(name: &str) -> bool {
    IO_REGISTRY.lock().remove(name).is_some()
}

/// Look up a registered Rust IO backend by name.
pub fn get_registered_io(name: &str) -> Option<Arc<dyn IO>> {
    IO_REGISTRY.lock().get(name).cloned()
}

/// List all registered Rust IO backend names.
pub fn list_registered_io() -> Vec<String> {
    IO_REGISTRY.lock().keys().cloned().collect()
}

#[cfg(test)]
mod io_registry_tests {
    use super::*;

    #[test]
    fn register_and_retrieve() {
        let io = Arc::new(MemoryIO::new());
        register_io("ioreg::retrieve", io).unwrap();
        assert!(get_registered_io("ioreg::retrieve").is_some());
        assert!(get_registered_io("nonexistent").is_none());
        unregister_io("ioreg::retrieve");
    }

    #[test]
    fn re_register_replaces() {
        let io1 = Arc::new(MemoryIO::new());
        let io2 = Arc::new(MemoryIO::new());
        register_io("ioreg::replace", io1).unwrap();
        register_io("ioreg::replace", io2).unwrap();
        let count = list_registered_io()
            .into_iter()
            .filter(|n| n == "ioreg::replace")
            .count();
        assert_eq!(count, 1, "should not duplicate entries");
        unregister_io("ioreg::replace");
    }

    #[test]
    fn unregister_returns_false_for_missing() {
        assert!(!unregister_io("ioreg::never_registered"));
    }

    #[test]
    fn unregister_removes() {
        let io = Arc::new(MemoryIO::new());
        register_io("ioreg::removable", io).unwrap();
        assert!(unregister_io("ioreg::removable"));
        assert!(get_registered_io("ioreg::removable").is_none());
    }

    #[test]
    fn list_includes_registered() {
        let io = Arc::new(MemoryIO::new());
        register_io("ioreg::listed", io).unwrap();
        assert!(list_registered_io().contains(&"ioreg::listed".to_string()));
        unregister_io("ioreg::listed");
    }

    #[test]
    fn empty_name_returns_error() {
        let result = register_io("", Arc::new(MemoryIO::new()));
        assert!(result.is_err());
    }
}

#[cfg(all(shuttle, test))]
mod shuttle_tests {
    use std::path::PathBuf;

    use super::*;
    use crate::io::{Buffer, Completion, OpenFlags, IO};
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::sync::Arc;
    use crate::thread;

    /// Factory trait for creating IO implementations in tests.
    /// Allows the same test logic to run against different IO backends.
    trait IOFactory: Send + Sync + 'static {
        fn create(&self) -> Arc<dyn IO>;
        /// Returns a unique temp directory path for this factory instance.
        fn temp_dir(&self) -> PathBuf;
    }

    struct MemoryIOFactory {
        id: u64,
    }

    impl MemoryIOFactory {
        fn new() -> Self {
            use crate::sync::atomic::AtomicU64;
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            Self {
                id: COUNTER.fetch_add(1, Ordering::SeqCst),
            }
        }
    }

    impl IOFactory for MemoryIOFactory {
        fn create(&self) -> Arc<dyn IO> {
            Arc::new(MemoryIO::new())
        }
        fn temp_dir(&self) -> PathBuf {
            format!("mem_{}", self.id).into()
        }
    }

    #[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
    struct PlatformIOFactory {
        temp_dir: tempfile::TempDir,
    }

    #[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
    impl PlatformIOFactory {
        fn new() -> Self {
            Self {
                temp_dir: tempfile::tempdir().unwrap(),
            }
        }
    }

    #[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
    impl IOFactory for PlatformIOFactory {
        fn create(&self) -> Arc<dyn IO> {
            Arc::new(PlatformIO::new().unwrap())
        }
        fn temp_dir(&self) -> PathBuf {
            self.temp_dir.path().to_path_buf()
        }
    }

    #[cfg(all(
        target_os = "windows",
        feature = "experimental_win_iocp",
        feature = "fs",
        not(miri)
    ))]
    struct WinIOCPFactory {
        temp_dir: tempfile::TempDir,
    }

    #[cfg(all(
        target_os = "windows",
        feature = "experimental_win_iocp",
        feature = "fs",
        not(miri)
    ))]
    impl WinIOCPFactory {
        fn new() -> Self {
            Self {
                temp_dir: tempfile::tempdir().unwrap(),
            }
        }
    }

    #[cfg(all(
        target_os = "windows",
        feature = "experimental_win_iocp",
        feature = "fs",
        not(miri)
    ))]
    impl IOFactory for WinIOCPFactory {
        fn create(&self) -> Arc<dyn IO> {
            Arc::new(WindowsIOCP::new().unwrap())
        }
        fn temp_dir(&self) -> PathBuf {
            self.temp_dir.path().to_path_buf()
        }
    }

    /// Macro to generate shuttle tests for all IO implementations.
    /// Creates a test for MemoryIO, and conditionally for PlatformIO.
    macro_rules! shuttle_io_test {
        ($test_name:ident, $test_impl:ident) => {
            pastey::paste! {
                #[test]
                fn [<shuttle_ $test_name _memory>]() {
                    shuttle::check_random(|| $test_impl(MemoryIOFactory::new()), 1000);
                }

                #[cfg(all(target_family = "unix", feature = "fs", not(miri)))]
                #[test]
                fn [<shuttle_ $test_name _platform>]() {
                    shuttle::check_random(|| $test_impl(PlatformIOFactory::new()), 1000);
                }

                // No ring variant: the ring's requests are delivered by a thread of its own, which
                // waits in the kernel, outside any schedule the model explores.

                #[cfg(all(target_os = "windows", feature = "experimental_win_iocp", feature = "fs", not(miri)))]
                #[test]
                fn [<shuttle_ $test_name _win_iocp>]() {
                    shuttle::check_random(|| $test_impl(WinIOCPFactory::new()), 1000);
                }

            }
        };
    }

    /// Helper to wait for a completion synchronously and assert it succeeded.
    fn wait_completion_ok(io: &dyn IO, c: &Completion) {
        io.wait_for_completion(c.clone()).unwrap();
        assert!(c.succeeded(), "completion failed: {:?}", c.get_error());
        assert!(!c.failed());
        assert!(c.finished());
        assert!(c.get_error().is_none());
    }

    /// Helper to wait for a completion synchronously without asserting success.
    #[allow(dead_code)]
    fn wait_completion(io: &dyn IO, c: &Completion) {
        io.wait_for_completion(c.clone()).unwrap();
        assert!(c.finished());
    }

    /// Test concurrent file creation from multiple threads.
    fn test_concurrent_file_creation_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let base = factory.temp_dir();
        let mut handles = vec![];
        const NUM_THREADS: usize = 3;

        for i in 0..NUM_THREADS {
            let io = io.clone();
            let base = base.clone();
            handles.push(thread::spawn(move || {
                let path = base.join(format!("test_file_{}.db", i));
                let file = io
                    .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
                    .unwrap();
                assert!(file.size().unwrap() == 0);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(concurrent_file_creation, test_concurrent_file_creation_impl);

    /// Test concurrent writes to different offsets in the same file.
    fn test_concurrent_writes_different_offsets_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];
        const NUM_THREADS: usize = 3;

        for i in 0..NUM_THREADS {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let data = vec![i as u8; 100];
                let buf = Arc::new(Buffer::new(data));
                let pos = (i * 100) as u64;

                let c = Completion::new_write(|_| {});
                let c = file.pwrite(pos, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify file size accounts for all writes
        let expected_size = (NUM_THREADS * 100) as u64;
        assert_eq!(file.size().unwrap(), expected_size);

        // Read back and verify each segment contains correct data
        for i in 0..NUM_THREADS {
            let read_buf = Arc::new(Buffer::new_temporary(100));
            let pos = (i * 100) as u64;
            let c = Completion::new_read(read_buf.clone(), |_| None);
            let c = file.pread(pos, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);

            let expected = vec![i as u8; 100];
            assert_eq!(
                read_buf.as_slice(),
                expected.as_slice(),
                "data mismatch at offset {}",
                pos
            );
        }
    }

    shuttle_io_test!(
        concurrent_writes_different_offsets,
        test_concurrent_writes_different_offsets_impl
    );

    /// Test concurrent reads and writes to the same file.
    fn test_concurrent_read_write_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // First write some initial data
        let initial_data = vec![0xAA; 1000];
        let buf = Arc::new(Buffer::new(initial_data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Spawn readers
        for _ in 0..2 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let read_buf = Arc::new(Buffer::new_temporary(100));
                let c = Completion::new_read(read_buf.clone(), |_| None);
                let c = file.pread(0, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                // All bytes read should be 0xAA (initial data at offset 0)
                assert!(
                    read_buf.as_slice().iter().all(|&b| b == 0xAA),
                    "read buffer should contain initial data 0xAA"
                );
            }));
        }

        // Spawn a writer
        {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let data = vec![0xBB; 100];
                let buf = Arc::new(Buffer::new(data));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite(500, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify the write at offset 500 succeeded
        let read_buf = Arc::new(Buffer::new_temporary(100));
        let c = Completion::new_read(read_buf.clone(), |_| None);
        let c = file.pread(500, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);
        assert!(
            read_buf.as_slice().iter().all(|&b| b == 0xBB),
            "data at offset 500 should be 0xBB"
        );
    }

    shuttle_io_test!(concurrent_read_write, test_concurrent_read_write_impl);

    /// Test that completion callbacks are invoked correctly under concurrency.
    fn test_completion_callbacks_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let callback_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        const NUM_WRITES: usize = 3;

        for i in 0..NUM_WRITES {
            let file = file.clone();
            let io = io.clone();
            let count = callback_count.clone();
            handles.push(thread::spawn(move || {
                let data = vec![i as u8; 50];
                let buf = Arc::new(Buffer::new(data));
                let count_clone = count.clone();
                let c = Completion::new_write(move |res| {
                    assert!(res.is_ok());
                    count_clone.fetch_add(1, Ordering::SeqCst);
                });
                let c = file.pwrite((i * 50) as u64, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(callback_count.load(Ordering::SeqCst), NUM_WRITES);
    }

    shuttle_io_test!(
        completion_callbacks_concurrent,
        test_completion_callbacks_concurrent_impl
    );

    /// Test concurrent truncate operations.
    fn test_concurrent_truncate_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write initial data
        let initial = vec![0xFF; 5000];
        let buf = Arc::new(Buffer::new(initial));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Spawn threads that truncate to different sizes
        for i in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let truncate_size = ((i + 1) * 1000) as u64;
                let c = Completion::new_trunc(|_| {});
                let c = file.truncate(truncate_size, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Size should be one of the truncate values
        let final_size = file.size().unwrap();
        assert!(final_size == 1000 || final_size == 2000 || final_size == 3000);
    }

    shuttle_io_test!(concurrent_truncate, test_concurrent_truncate_impl);

    /// Test pwritev with concurrent reads.
    fn test_pwritev_with_concurrent_reads_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write initial data so reads have something to return
        let initial = vec![0x11; 2000];
        let buf = Arc::new(Buffer::new(initial));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Spawn a pwritev thread
        {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let bufs = vec![
                    Arc::new(Buffer::new(vec![0x22; 100])),
                    Arc::new(Buffer::new(vec![0x33; 100])),
                    Arc::new(Buffer::new(vec![0x44; 100])),
                ];
                let c = Completion::new_write(|_| {});
                let c = file.pwritev(0, bufs, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        // Spawn reader threads
        for _ in 0..2 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let buf = Arc::new(Buffer::new_temporary(100));
                let c = Completion::new_read(buf.clone(), |_| None);
                let c = file.pread(0, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                // Data should be either initial (0x11) or from pwritev (0x22)
                // depending on race ordering
                let first_byte = buf.as_slice()[0];
                assert!(
                    first_byte == 0x11 || first_byte == 0x22,
                    "first byte should be 0x11 or 0x22, got {:#x}",
                    first_byte
                );
                // All 100 bytes should be consistent
                assert!(
                    buf.as_slice().iter().all(|&b| b == first_byte),
                    "all bytes should be the same value"
                );
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // After all threads complete, verify pwritev data is present
        let read_buf = Arc::new(Buffer::new_temporary(300));
        let c = Completion::new_read(read_buf.clone(), |_| None);
        let c = file.pread(0, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        // Should have 0x22 for first 100, 0x33 for next 100, 0x44 for last 100
        assert!(
            read_buf.as_slice()[..100].iter().all(|&b| b == 0x22),
            "bytes 0-99 should be 0x22"
        );
        assert!(
            read_buf.as_slice()[100..200].iter().all(|&b| b == 0x33),
            "bytes 100-199 should be 0x33"
        );
        assert!(
            read_buf.as_slice()[200..300].iter().all(|&b| b == 0x44),
            "bytes 200-299 should be 0x44"
        );
    }

    shuttle_io_test!(
        pwritev_with_concurrent_reads,
        test_pwritev_with_concurrent_reads_impl
    );

    /// Test concurrent access to multiple files.
    fn test_concurrent_multifile_access_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let base = factory.temp_dir();

        let mut handles = vec![];
        const NUM_FILES: usize = 3;

        for i in 0..NUM_FILES {
            let io = io.clone();
            let base = base.clone();
            handles.push(thread::spawn(move || {
                let path = base.join(format!("file_{}.db", i));
                let file = io
                    .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
                    .unwrap();

                // Write to file
                let data = vec![i as u8; 200];
                let buf = Arc::new(Buffer::new(data.clone()));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite(0, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                // Read back and verify
                let read_buf = Arc::new(Buffer::new_temporary(200));
                let c = Completion::new_read(read_buf.clone(), |_| None);
                let c = file.pread(0, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                assert_eq!(read_buf.as_slice(), data.as_slice());
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(
        concurrent_multifile_access,
        test_concurrent_multifile_access_impl
    );

    /// Test file locking under concurrent access.
    fn test_file_locking_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        // Multiple threads try to lock/unlock
        for _ in 0..3 {
            let file = file.clone();
            handles.push(thread::spawn(move || {
                // Exclusive lock
                file.lock_file(true).unwrap();
                thread::yield_now();
                file.unlock_file().unwrap();

                // Shared lock
                file.lock_file(false).unwrap();
                thread::yield_now();
                file.unlock_file().unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(file_locking_concurrent, test_file_locking_concurrent_impl);

    /// Test reading past end of file returns zero bytes.
    fn test_read_past_eof_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write 100 bytes
        let data = vec![0xAA; 100];
        let buf = Arc::new(Buffer::new(data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Multiple threads try to read past EOF
        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let read_buf = Arc::new(Buffer::new_temporary(100));
                let bytes_read = Arc::new(AtomicUsize::new(999));
                let bytes_read_clone = bytes_read.clone();
                let c = Completion::new_read(read_buf, move |res| {
                    if let Ok((_, n)) = res {
                        bytes_read_clone.store(n as usize, Ordering::SeqCst);
                    }
                    None
                });
                let c = file.pread(200, c).unwrap(); // Past EOF
                                                     // Reading past EOF succeeds with 0 bytes read
                wait_completion_ok(io.as_ref(), &c);
                assert_eq!(bytes_read.load(Ordering::SeqCst), 0);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(read_past_eof, test_read_past_eof_impl);

    /// Test empty write operations.
    fn test_empty_write_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                // Empty buffer write
                let buf = Arc::new(Buffer::new(vec![]));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite(0, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(file.size().unwrap(), 0);
    }

    shuttle_io_test!(empty_write, test_empty_write_impl);

    /// Test sync operations under concurrency.
    fn test_concurrent_sync_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write some data first
        let data = vec![0xFF; 1000];
        let buf = Arc::new(Buffer::new(data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Multiple sync calls concurrently
        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let c = Completion::new_sync(|_| {});
                let c = file.sync(c, FileSyncType::Fsync).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(concurrent_sync, test_concurrent_sync_impl);

    /// Test concurrent open of the same file returns same file instance.
    fn test_concurrent_open_same_file_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("shared.db");

        // Create file first
        let _ = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        for _ in 0..3 {
            let io = io.clone();
            let path = path.clone();
            handles.push(thread::spawn(move || {
                let file = io
                    .open_file(path.to_str().unwrap(), OpenFlags::None, false)
                    .unwrap();
                thread::yield_now();
                // Write a byte to prove we got a valid file
                let buf = Arc::new(Buffer::new(vec![0xAA]));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite(0, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(
        concurrent_open_same_file,
        test_concurrent_open_same_file_impl
    );

    /// Test file removal while concurrent access.
    fn test_file_remove_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let base = factory.temp_dir();

        // Create multiple files
        for i in 0..3 {
            let path = base.join(format!("remove_{}.db", i));
            let file = io
                .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
                .unwrap();
            let buf = Arc::new(Buffer::new(vec![0xFF; 100]));
            let c = Completion::new_write(|_| {});
            let c = file.pwrite(0, buf, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);
        }

        let mut handles = vec![];

        // Remove files concurrently
        for i in 0..3 {
            let io = io.clone();
            let base = base.clone();
            handles.push(thread::spawn(move || {
                let path = base.join(format!("remove_{}.db", i));
                io.remove_file(path.to_str().unwrap()).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(file_remove_concurrent, test_file_remove_concurrent_impl);

    /// Test write spanning multiple internal pages.
    fn test_large_write_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        // Multiple threads write large buffers that span multiple pages
        for i in 0..2 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                // Write 10000 bytes (spans multiple 4096-byte pages)
                let data = vec![(i + 1) as u8; 10000];
                let buf = Arc::new(Buffer::new(data));
                let c = Completion::new_write(|_| {});
                let c = file.pwrite((i * 10000) as u64, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(file.size().unwrap(), 20000);

        // Read back and verify each segment contains correct data
        for i in 0..2 {
            let read_buf = Arc::new(Buffer::new_temporary(10000));
            let pos = (i * 10000) as u64;
            let c = Completion::new_read(read_buf.clone(), |_| None);
            let c = file.pread(pos, c).unwrap();
            wait_completion_ok(io.as_ref(), &c);

            let expected_byte = (i + 1) as u8;
            assert!(
                read_buf.as_slice().iter().all(|&b| b == expected_byte),
                "all bytes at offset {} should be {:#x}",
                pos,
                expected_byte
            );
        }
    }

    shuttle_io_test!(large_write_concurrent, test_large_write_concurrent_impl);

    /// Test has_hole and punch_hole under concurrency.
    /// Note: Only runs on MemoryIO as hole operations are not supported on all backends.
    fn test_hole_operations_concurrent_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write data spanning multiple pages (at least 3 pages = 12288 bytes)
        let data = vec![0xFF; 16384];
        let buf = Arc::new(Buffer::new(data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Thread 1: punch holes
        {
            let file = file.clone();
            handles.push(thread::spawn(move || {
                // Punch hole in middle page (page-aligned)
                file.punch_hole(4096, 4096).unwrap();
            }));
        }

        // Thread 2: check for holes
        {
            let file = file.clone();
            handles.push(thread::spawn(move || {
                // Check various regions
                let has_hole = file.has_hole(0, 4096).unwrap();
                assert!(!has_hole);
                let _ = file.has_hole(4096, 4096).unwrap();
                let has_hole = file.has_hole(8192, 4096).unwrap();
                assert!(!has_hole);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    // hole_operations only runs on MemoryIO since not all backends support holes
    #[test]
    fn shuttle_hole_operations_concurrent_memory() {
        shuttle::check_random(
            || test_hole_operations_concurrent_impl(MemoryIOFactory::new()),
            1000,
        );
    }

    /// Test that partial reads work correctly at EOF boundary.
    fn test_partial_read_at_eof_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        // Write exactly 150 bytes
        let data = vec![0xAB; 150];
        let buf = Arc::new(Buffer::new(data));
        let c = Completion::new_write(|_| {});
        let c = file.pwrite(0, buf, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let mut handles = vec![];

        // Multiple threads try to read 100 bytes starting at offset 100
        // Should only get 50 bytes back
        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let read_buf = Arc::new(Buffer::new_temporary(100));
                let bytes_read = Arc::new(AtomicUsize::new(999));
                let bytes_read_clone = bytes_read.clone();
                let c = Completion::new_read(read_buf.clone(), move |res| {
                    if let Ok((_, n)) = res {
                        bytes_read_clone.store(n as usize, Ordering::SeqCst);
                    }
                    None
                });
                let c = file.pread(100, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);

                // Should read exactly 50 bytes (150 - 100)
                assert_eq!(bytes_read.load(Ordering::SeqCst), 50);
                // Verify the bytes read are correct
                assert_eq!(&read_buf.as_slice()[..50], &[0xAB; 50]);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(partial_read_at_eof, test_partial_read_at_eof_impl);

    /// Test empty pwritev.
    fn test_empty_pwritev_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let mut handles = vec![];

        for _ in 0..3 {
            let file = file.clone();
            let io = io.clone();
            handles.push(thread::spawn(move || {
                let bufs: Vec<Arc<Buffer>> = vec![];
                let c = Completion::new_write(|_| {});
                let c = file.pwritev(0, bufs, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(empty_pwritev, test_empty_pwritev_impl);

    /// Test error case: opening non-existent file without Create flag.
    fn test_open_nonexistent_without_create_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let base = factory.temp_dir();

        let mut handles = vec![];

        for i in 0..3 {
            let io = io.clone();
            let base = base.clone();
            handles.push(thread::spawn(move || {
                let path = base.join(format!("nonexistent_{}.db", i));
                let result = io.open_file(path.to_str().unwrap(), OpenFlags::None, false);
                assert!(result.is_err());
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    shuttle_io_test!(
        open_nonexistent_without_create,
        test_open_nonexistent_without_create_impl
    );

    /// Test concurrent writes to overlapping regions.
    /// This tests that the final state is consistent (one of the writes wins).
    fn test_concurrent_overlapping_writes_impl<F: IOFactory>(factory: F) {
        let io = factory.create();
        let path = factory.temp_dir().join("test.db");
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();

        let write_complete = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Multiple threads write to the same offset
        for i in 0..3 {
            let file = file.clone();
            let io = io.clone();
            let write_complete = write_complete.clone();
            handles.push(thread::spawn(move || {
                let data = vec![(i + 1) as u8; 100];
                let buf = Arc::new(Buffer::new(data));
                let write_complete_clone = write_complete.clone();
                let c = Completion::new_write(move |_| {
                    write_complete_clone.fetch_add(1, Ordering::SeqCst);
                });
                let c = file.pwrite(0, buf, c).unwrap();
                wait_completion_ok(io.as_ref(), &c);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All writes should have completed
        assert_eq!(write_complete.load(Ordering::SeqCst), 3);

        // Read back and verify we got one of the written values
        let read_buf = Arc::new(Buffer::new_temporary(100));
        let c = Completion::new_read(read_buf.clone(), |_| None);
        let c = file.pread(0, c).unwrap();
        wait_completion_ok(io.as_ref(), &c);

        let first_byte = read_buf.as_slice()[0];
        assert!(first_byte == 1 || first_byte == 2 || first_byte == 3);

        // All 100 bytes should be the same value
        assert!(read_buf.as_slice().iter().all(|&b| b == first_byte));
    }

    shuttle_io_test!(
        concurrent_overlapping_writes,
        test_concurrent_overlapping_writes_impl
    );
}
