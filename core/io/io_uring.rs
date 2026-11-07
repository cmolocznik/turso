#![allow(clippy::arc_with_non_send_sync)]

use super::{
    common, Completion, CompletionInner, File, OpenFlags, SharedWalLockKind, SharedWalMappedRegion,
    IO,
};
use crate::error::io_error;
use crate::io::clock::{Clock, DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::unix::{
    unix_shared_wal_lock_byte, unix_shared_wal_map, unix_shared_wal_unlock_byte,
};
use crate::storage::wal::CKPT_BATCH_PAGES;
use crate::sync::Mutex;
use crate::turso_assert;
use crate::{CompletionError, LimboError, Result};
use rustix::fs::{self, FlockOperation};
use std::ptr::NonNull;
use std::{
    collections::{HashMap, VecDeque},
    io::ErrorKind,
    ops::Deref,
    os::{
        fd::{AsFd, FromRawFd, OwnedFd},
        unix::io::AsRawFd,
    },
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
};
use tracing::{debug, trace, warn};

/// Size of the io_uring submission and completion queues
const ENTRIES: u32 = 512;

/// Number of Vec<Box<[iovec]>> we preallocate on initialization
const IOVEC_POOL_SIZE: usize = 64;

/// Maximum number of iovec entries per writev operation.
/// IOV_MAX is typically 1024
const MAX_IOVEC_ENTRIES: usize = CKPT_BATCH_PAGES;

/// Completions the delivery thread takes in one pass: one completion queue's worth, twice the
/// submission queue, as the kernel sizes it by default.
const CQ_BATCH: usize = 2 * ENTRIES as usize;

/// One memory arena for DB pages and another for WAL frames
const ARENA_COUNT: usize = 2;

/// user_data tag for cancellation operations
const CANCEL_TAG: u64 = 1;

/// user_data tag of the standing poll on the ring's eventfd: not a request, never counted as one.
const EVENTFD_TAG: u64 = 2;

/// Probed io_uring opcode support. Opcodes that are not supported by the
/// running kernel fall back to synchronous POSIX syscalls.
struct UringCapabilities {
    ftruncate: bool,
}

/// The io_uring backend (DESIGN.md 22.5). A delivery thread of the ring's own is the one thread that
/// waits in the kernel and delivers every request submitted to the ring, by any thread: a waiter
/// parks on its request and the thread wakes it; nobody steps the ring.
pub struct UringIO {
    shared: Handle,
}

/// The ring and what its handles and its delivery thread share. The thread holds this, never a
/// handle: it runs while a handle (the IO, or a file of it) lives or a request is outstanding.
struct Shared {
    ring: io_uring::IoUring,
    /// The queues' state. Held only to touch the queues, never across the kernel wait or a
    /// callback: the `_shared()` queue accessors need no other queue object to exist, which
    /// holding it ensures.
    state: Mutex<RingState>,
    caps: UringCapabilities,
    /// The IO and its files alive.
    handles: AtomicUsize,
    /// Requests accepted and not yet retired, queued in the kernel or past a full submission
    /// queue.
    requests: AtomicUsize,
    /// Written by the last handle's drop: its standing poll ends the delivery thread's wait.
    eventfd: OwnedFd,
    /// An error number the delivery thread's next wait returns instead of waiting, for a test.
    #[cfg(test)]
    injected_wait: std::sync::atomic::AtomicI32,
}

unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

/// A handle on the ring: the IO, or one of its files.
struct Handle(Arc<Shared>);

impl Handle {
    fn new(shared: &Arc<Shared>) -> Self {
        shared.handles.fetch_add(1, Ordering::AcqRel);
        Self(shared.clone())
    }
}

impl Deref for Handle {
    type Target = Shared;
    fn deref(&self) -> &Shared {
        &self.0
    }
}

impl Drop for Handle {
    /// The last one wakes the delivery thread, so that it ends once nothing is outstanding. It
    /// joins nothing: dropped inside a callback, on the delivery thread itself, it lets the thread
    /// end after its batch.
    fn drop(&mut self) {
        if self.0.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            let one = 1u64.to_ne_bytes();
            // SAFETY: an 8-byte write from a live buffer to the eventfd this holds open. A write
            // that fails leaves the counter as it was: it cannot, short of the counter reaching
            // its maximum, which one write per ring never does.
            unsafe { libc::write(self.0.eventfd.as_raw_fd(), one.as_ptr().cast(), one.len()) };
        }
    }
}

impl Shared {
    /// A file's submission takes `c`: the ring owns it, and its buffer, until its delivery.
    fn accept(&self, c: &Completion) {
        c.accept();
        self.requests.fetch_add(1, Ordering::AcqRel);
    }
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}
crate::assert::assert_send_sync!(UringIO);

struct RingState {
    writev_states: HashMap<u64, WritevState>,
    overflow: VecDeque<io_uring::squeue::Entry>,
    iov_pool: IovecPool,
    free_arenas: [Option<(NonNull<u8>, usize)>; ARENA_COUNT],
}

impl RingState {
    /// SAFETY: caller must guarantee no other `SubmissionQueue` exists for
    /// `ring`. Holding the `RingState` Mutex satisfies that invariant.
    unsafe fn flush_overflow(&mut self, ring: &io_uring::IoUring) {
        if self.overflow.is_empty() {
            return;
        }
        let mut sq = ring.submission_shared();
        while !self.overflow.is_empty() {
            if sq.is_full() {
                break;
            }
            let entry = self.overflow.pop_front().expect("checked not empty");
            if sq.push(&entry).is_err() {
                self.overflow.push_front(entry);
                break;
            }
        }
    }

    /// Queues `entry`, in the submission queue or past it in the overflow, and enters what is
    /// queued: the request is accepted once queued, whatever entering it comes to (DESIGN.md
    /// 22.5). SAFETY: caller must guarantee no other `SubmissionQueue` exists for `ring` (i.e.
    /// caller holds the `RingState` Mutex).
    unsafe fn submit_entry(&mut self, ring: &io_uring::IoUring, entry: &io_uring::squeue::Entry) {
        trace!("submit_entry({:?})", entry);
        self.flush_overflow(ring);
        let pushed = {
            let mut sq = ring.submission_shared();
            sq.push(entry).is_ok()
        };
        if !pushed {
            self.overflow.push_back(entry.clone());
        }
        enter(ring);
    }

    /// SAFETY: same contract as `submit_entry`.
    unsafe fn submit_cancel_urgent(
        &mut self,
        ring: &io_uring::IoUring,
        entry: &io_uring::squeue::Entry,
    ) {
        let pushed = {
            let mut sq = ring.submission_shared();
            sq.push(entry).is_ok()
        };
        if !pushed {
            self.overflow.push_front(entry.clone());
        }
        enter(ring);
    }

    /// Queues the standing poll on the eventfd, rearmed after each time it fires. SAFETY: same
    /// contract as `submit_entry`.
    unsafe fn arm_eventfd(&mut self, ring: &io_uring::IoUring, eventfd: &OwnedFd) {
        let poll = io_uring::opcode::PollAdd::new(
            io_uring::types::Fd(eventfd.as_raw_fd()),
            libc::POLLIN as u32,
        )
        .build()
        .user_data(EVENTFD_TAG);
        self.submit_entry(ring, &poll);
    }
}

/// Enters the requests queued in the submission queue into the kernel. Entries already queued are
/// accepted requests: a refusal the kernel says may pass (interrupted, out of resources, a full
/// completion queue) leaves them queued for the next entry, which the next submission or wait
/// makes. Any other means the ring can no longer deliver or retire what it accepted, and ends the
/// process (DESIGN.md 22.11): returning would leave every waiter on those requests waiting.
fn enter(ring: &io_uring::IoUring) {
    #[cfg(test)]
    if tests::ENTER_HELD.with(|h| h.get()) {
        return;
    }
    if let Err(e) = ring.submit() {
        match e.raw_os_error() {
            Some(libc::EINTR | libc::EAGAIN | libc::EBUSY) => {}
            _ => ring_lost(&e, "io_uring_enter"),
        }
    }
}

/// Ends the process: the ring cannot deliver or retire the requests it accepted.
fn ring_lost(e: &std::io::Error, during: &str) -> ! {
    eprintln!(
        "io_uring: {during} failed ({e}): the ring can no longer deliver the requests it accepted"
    );
    std::process::abort()
}

/// preallocated vec of iovec arrays to avoid allocations during writev operations
struct IovecPool {
    pool: Vec<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>>,
}

impl IovecPool {
    fn new() -> Self {
        let pool = (0..IOVEC_POOL_SIZE)
            .map(|_| {
                Box::new(
                    [libc::iovec {
                        iov_base: std::ptr::null_mut(),
                        iov_len: 0,
                    }; MAX_IOVEC_ENTRIES],
                )
            })
            .collect();
        Self { pool }
    }

    #[inline(always)]
    fn acquire(&mut self) -> Option<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>> {
        self.pool.pop()
    }

    #[inline(always)]
    fn release(&mut self, iovec: Box<[libc::iovec; MAX_IOVEC_ENTRIES]>) {
        if self.pool.len() < IOVEC_POOL_SIZE {
            self.pool.push(iovec);
        }
    }
}

impl UringIO {
    pub fn new() -> Result<Self> {
        Self::with_entries(ENTRIES)
    }

    /// A ring of `entries` submission entries, its delivery thread started. If the thread or its
    /// eventfd cannot be set up, the ring is torn down whole and the error says so.
    fn with_entries(entries: u32) -> Result<Self> {
        #[cfg(test)]
        tests::refuse(tests::Setup::Ring)?;
        let ring = io_uring::IoUring::new(entries).map_err(explain_setup_failure)?;
        // RL_MEMLOCK cap is typically 8MB, the current design is to have one large arena
        // registered at startup and therefore we can simply use the zero index, falling back
        // to similar logic as the existing buffer pool for cases where it is over capacity.
        #[cfg(test)]
        tests::refuse(tests::Setup::Registration)?;
        ring.submitter()
            .register_buffers_sparse(ARENA_COUNT as u32)
            .map_err(|e| io_error(e, "register_buffers"))?;
        // Probe supported opcodes so we can fall back to POSIX for unsupported ones.
        let mut probe = io_uring::register::Probe::new();
        let caps = if ring.submitter().register_probe(&mut probe).is_ok() {
            UringCapabilities {
                ftruncate: probe.is_supported(io_uring::opcode::Ftruncate::CODE),
            }
        } else {
            UringCapabilities { ftruncate: false }
        };
        if !caps.ftruncate {
            warn!("io_uring: IORING_OP_FTRUNCATE not supported by kernel, using POSIX fallback");
        }
        #[cfg(test)]
        tests::refuse(tests::Setup::Eventfd)?;
        // SAFETY: a plain eventfd(2); its descriptor is owned from here on.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(LimboError::IoBackendUnavailable(format!(
                "io_uring: the delivery thread's eventfd: {}",
                std::io::Error::last_os_error()
            )));
        }
        // SAFETY: `fd` is a fresh descriptor nothing else owns.
        let eventfd = unsafe { OwnedFd::from_raw_fd(fd) };
        let shared = Arc::new(Shared {
            ring,
            state: Mutex::new(RingState {
                overflow: VecDeque::new(),
                writev_states: HashMap::default(),
                iov_pool: IovecPool::new(),
                free_arenas: [const { None }; ARENA_COUNT],
            }),
            caps,
            handles: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            eventfd,
            #[cfg(test)]
            injected_wait: std::sync::atomic::AtomicI32::new(0),
        });
        {
            let mut state = shared.state.lock();
            // SAFETY: holding `state` Mutex.
            unsafe { state.arm_eventfd(&shared.ring, &shared.eventfd) };
        }
        let io = Self {
            shared: Handle::new(&shared),
        };
        #[cfg(test)]
        tests::refuse(tests::Setup::Thread)?;
        // A thread of the std library's, never a model's: it waits in the kernel, outside any
        // schedule a model explores.
        std::thread::Builder::new()
            .name("turso-io-uring".into())
            .spawn(move || deliver(shared))
            .map_err(|e| {
                LimboError::IoBackendUnavailable(format!(
                    "io_uring: the delivery thread did not start: {e}"
                ))
            })?;
        debug!("Using IO backend 'io-uring'");
        Ok(io)
    }
}

fn explain_setup_failure(e: std::io::Error) -> LimboError {
    match e.kind() {
        ErrorKind::Unsupported => LimboError::IoBackendUnavailable(format!(
            "io_uring_setup: not supported (host kernel version {})",
            read_kernel_setting("osrelease").unwrap_or_else(|| "unknown".to_string())
        )),
        ErrorKind::PermissionDenied => LimboError::IoBackendUnavailable(permission_denied_message(
            read_kernel_setting("io_uring_disabled").as_deref(),
        )),
        _ => io_error(e, "io_uring_setup"),
    }
}

fn read_kernel_setting(name: &str) -> Option<String> {
    let setting = std::fs::read_to_string(format!("/proc/sys/kernel/{name}")).ok()?;
    Some(setting.trim().to_string())
}

fn permission_denied_message(disabled: Option<&str>) -> String {
    match disabled {
        Some(setting) => format!("io_uring_setup: permission denied (io_uring_disabled={setting})"),
        None => "io_uring_setup: permission denied".to_string(),
    }
}

/// State to track an ongoing writev operation in
/// the case of a partial write.
struct WritevState {
    /// File descriptor/id of the file we are writing to
    file_id: io_uring::types::Fd,
    /// absolute file offset for next submit
    file_pos: u64,
    /// current buffer index in `bufs`
    current_buffer_idx: usize,
    /// intra-buffer offset
    current_buffer_offset: usize,
    /// total bytes written so far
    total_written: usize,
    /// cache the sum of all buffer lengths for the total expected write
    total_len: usize,
    /// buffers to write
    bufs: Vec<Arc<crate::Buffer>>,
    /// we keep the last iovec allocation alive until final CQE
    last_iov_allocation: Option<Box<[libc::iovec; MAX_IOVEC_ENTRIES]>>,
}

impl WritevState {
    fn new(file: &UringFile, pos: u64, bufs: Vec<Arc<crate::Buffer>>) -> Self {
        let file_id = file.file.as_raw_fd();
        let total_len = bufs.iter().map(|b| b.len()).sum();
        Self {
            file_id: io_uring::types::Fd(file_id),
            file_pos: pos,
            current_buffer_idx: 0,
            current_buffer_offset: 0,
            total_written: 0,
            bufs,
            last_iov_allocation: None,
            total_len,
        }
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        self.total_len - self.total_written
    }

    /// Advance (idx, off, pos) after written bytes
    #[inline(always)]
    fn advance(&mut self, written: u64) {
        let mut remaining = written;
        while remaining > 0 {
            let current_buf_len = self.bufs[self.current_buffer_idx].len();
            let left = current_buf_len - self.current_buffer_offset;
            if remaining < left as u64 {
                self.current_buffer_offset += remaining as usize;
                self.file_pos += remaining;
                remaining = 0;
            } else {
                remaining -= left as u64;
                self.file_pos += left as u64;
                self.current_buffer_idx += 1;
                self.current_buffer_offset = 0;
            }
        }
        self.total_written += written as usize;
    }

    #[inline(always)]
    /// Free the allocation that keeps the iovec array alive while writev is ongoing
    fn free_last_iov(&mut self, pool: &mut IovecPool) {
        if let Some(allocation) = self.last_iov_allocation.take() {
            pool.release(allocation);
        }
    }
}

impl RingState {
    #[cfg(debug_assertions)]
    fn debug_check_fixed(&self, idx: u32, ptr: *const u8, len: usize) {
        let (base, blen) = self.free_arenas[idx as usize].expect("slot not registered");
        let start = base.as_ptr() as usize;
        let end = start + blen;
        let p = ptr as usize;
        turso_assert!(
            p >= start && p + len <= end,
            "Fixed operation, pointer out of registered range"
        );
    }

    /// Submit or resubmit a writev operation. SAFETY: caller must hold the
    /// `RingState` Mutex (so no other `SubmissionQueue` exists for `ring`).
    unsafe fn submit_writev(&mut self, ring: &io_uring::IoUring, key: u64, mut st: WritevState) {
        st.free_last_iov(&mut self.iov_pool);

        let mut iov_allocation = self.iov_pool.acquire().unwrap_or_else(|| {
            Box::new(
                [libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                }; MAX_IOVEC_ENTRIES],
            )
        });

        let mut iov_count = 0;
        let mut last_end: Option<(*const u8, usize)> = None;

        for (idx, buffer) in st.bufs.iter().enumerate().skip(st.current_buffer_idx) {
            let mut ptr = buffer.as_ptr();
            let mut len = buffer.len();
            if idx == st.current_buffer_idx && st.current_buffer_offset != 0 {
                turso_assert!(
                    st.current_buffer_offset <= len,
                    "writev state offset out of bounds"
                );
                ptr = ptr.add(st.current_buffer_offset);
                len -= st.current_buffer_offset;
            }
            if let Some((last_ptr, last_len)) = last_end {
                if last_ptr.add(last_len) == ptr {
                    iov_allocation[iov_count - 1].iov_len += len;
                    last_end = Some((last_ptr, last_len + len));
                    continue;
                }
            }
            iov_allocation[iov_count] = libc::iovec {
                iov_base: ptr as *mut _,
                iov_len: len,
            };
            last_end = Some((ptr, len));
            iov_count += 1;
            if iov_count >= MAX_IOVEC_ENTRIES {
                break;
            }
        }

        let ptr = iov_allocation.as_ptr() as *mut libc::iovec;
        st.last_iov_allocation = Some(iov_allocation);
        let entry = io_uring::opcode::Writev::new(st.file_id, ptr, iov_count as u32)
            .offset(st.file_pos)
            .build()
            .user_data(key);
        self.writev_states.insert(key, st);
        self.submit_entry(ring, &entry)
    }

    /// Handle a writev CQE: resubmits the rest of a partial write, or returns how the whole
    /// write ended, for its callback to run once the lock is released. SAFETY: caller must hold
    /// the `RingState` Mutex.
    unsafe fn handle_writev_completion(
        &mut self,
        ring: &io_uring::IoUring,
        mut state: WritevState,
        user_data: u64,
        result: i32,
    ) -> Option<std::result::Result<i32, CompletionError>> {
        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            tracing::error!("writev failed (user_data: {}): {}", user_data, err);
            state.free_last_iov(&mut self.iov_pool);
            return Some(Err(CompletionError::IOError(err.kind(), "pwritev")));
        }

        let written = result;

        if written == 0 && state.remaining() > 0 {
            state.free_last_iov(&mut self.iov_pool);
            return Some(Err(CompletionError::ShortWrite));
        }
        state.advance(written as u64);

        match state.remaining() {
            0 => {
                tracing::debug!(
                    "writev operation completed: wrote {} bytes",
                    state.total_written
                );
                state.free_last_iov(&mut self.iov_pool);
                return Some(Ok(state.total_written as i32));
            }
            remaining => {
                tracing::trace!(
                    "resubmitting writev operation for user_data {}: wrote {} bytes, remaining {}",
                    user_data,
                    written,
                    remaining
                );
                self.submit_writev(ring, user_data, state);
                // Progress wake: the future is parked on the parent
                // completion's waker, but `complete()` only fires on the
                // final chunk. Without this, intermediate-chunk completions
                // never wake the future, the resubmitted chunks pile up,
                // and the task deadlocks.
                wake_user_data(user_data);
            }
        }
        None
    }
}

impl IO for UringIO {
    fn supports_shared_wal_coordination(&self) -> bool {
        true
    }

    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        super::refuse_on_delivery_thread("a file's open");
        let file = common::open_unix(path, flags)?;
        let uring_file = Arc::new(UringFile {
            shared: Handle::new(&self.shared.0),
            file,
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            && !flags.intersects(OpenFlags::ReadOnly | OpenFlags::NoLock)
        {
            uring_file.lock_file(true)?;
        }
        Ok(uring_file)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        super::refuse_on_delivery_thread("a file's removal");
        std::fs::remove_file(path).map_err(|e| io_error(e, "remove_file"))?;
        Ok(())
    }

    fn rename_file(&self, from: &str, to: &str) -> Result<bool> {
        super::refuse_on_delivery_thread("a rename");
        std::fs::rename(from, to).map_err(|e| io_error(e, "rename_file"))?;
        Ok(true)
    }

    fn sync_directory(&self, path: &str) -> Result<()> {
        super::refuse_on_delivery_thread("a directory sync");
        common::sync_directory(path)
    }

    fn cancel(&self, completions: &[Completion]) -> Result<()> {
        let mut state = self.shared.state.lock();
        for c in completions {
            c.abort();
            // dont want to leak the refcount bump with `get_key`/into_raw here, so we use as_ptr
            let e = io_uring::opcode::AsyncCancel::new(Arc::as_ptr(c.get_inner()) as u64)
                .build()
                .user_data(CANCEL_TAG);
            // SAFETY: holding `state` Mutex.
            unsafe { state.submit_cancel_urgent(&self.shared.ring, &e) };
        }
        Ok(())
    }

    /// Nothing to do: the delivery thread delivers every request.
    fn step(&self) -> Result<()> {
        Ok(())
    }

    /// Parks until `c` finishes, the delivery thread waking this thread.
    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        super::refuse_on_delivery_thread("a wait for a completion");
        c.block_on();
        match c.get_error() {
            Some(e) => Err(e.into()),
            None => Ok(()),
        }
    }

    /// Parks until no request beneath `completions` is owned, the delivery thread waking this
    /// thread as each retires.
    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        super::refuse_on_delivery_thread("a wait for retirement");
        let mut seen = std::collections::HashSet::new();
        for c in completions {
            for request in c.requests(&mut seen) {
                if !request.is_owned() {
                    continue;
                }
                let thread = crate::thread::current();
                request.on_retired(move || thread.unpark());
                while request.is_owned() {
                    crate::thread::park();
                }
            }
        }
        Ok(())
    }

    fn register_fixed_buffer(&self, ptr: std::ptr::NonNull<u8>, len: usize) -> Result<u32> {
        turso_assert!(
            len % 512 == 0,
            "fixed buffer length must be logical block aligned"
        );
        let mut state = self.shared.state.lock();
        let slot =
            state.free_arenas.iter().position(|e| e.is_none()).ok_or({
                crate::error::CompletionError::UringIOError("no free fixed buffer slots")
            })?;
        unsafe {
            self.shared
                .ring
                .submitter()
                .register_buffers_update(
                    slot as u32,
                    &[libc::iovec {
                        iov_base: ptr.as_ptr() as *mut libc::c_void,
                        iov_len: len,
                    }],
                    None,
                )
                .map_err(|e| io_error(e, "register_buffers_update"))?
        };
        state.free_arenas[slot] = Some((ptr, len));
        Ok(slot as u32)
    }
}

/// The delivery thread (DESIGN.md 22.5): the one thread that waits in the kernel. Before each wait it
/// moves what queued past a full submission queue into the kernel; it waits for one completion,
/// takes at most one completion queue of entries under the mutex, resubmitting the rest of a
/// partial write there, lets the mutex go, delivers each, and waits again. It ends once no handle
/// lives and no request is outstanding. A delivery that panics ends the process: nothing else
/// would deliver the ring's requests, so returning would strand every waiter on them.
fn deliver(shared: Arc<Shared>) {
    super::DELIVERING.with(|d| d.set(true));
    let _frame = DeliveryFrame;
    let mut batch = Vec::with_capacity(CQ_BATCH);
    loop {
        {
            let mut state = shared.state.lock();
            // SAFETY: holding `state` Mutex.
            unsafe { state.flush_overflow(&shared.ring) };
        }
        if shared.handles.load(Ordering::Acquire) == 0
            && shared.requests.load(Ordering::Acquire) == 0
        {
            return;
        }
        #[cfg(test)]
        let waited = match shared.injected_wait.swap(0, Ordering::AcqRel) {
            0 => shared.ring.submit_and_wait(1),
            errno => Err(std::io::Error::from_raw_os_error(errno)),
        };
        #[cfg(not(test))]
        let waited = shared.ring.submit_and_wait(1);
        if let Err(e) = waited {
            match e.raw_os_error() {
                // A signal cut the wait short.
                Some(libc::EINTR) => continue,
                // A full completion queue, or no room to enter what is queued: what is complete
                // is taken below, and the wait is tried again.
                Some(libc::EBUSY | libc::EAGAIN) => {}
                _ => ring_lost(&e, "io_uring_enter, the delivery thread's wait"),
            }
        }
        shared.take_batch(&mut batch);
        for (key, result) in batch.drain(..) {
            completion_from_key(key).deliver(result);
            shared.requests.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Held by the delivery thread for its whole life: a panic unwinding it, from a callback or the
/// thread's own code, ends the process before anyone could see the ring without its deliverer.
struct DeliveryFrame;

impl Drop for DeliveryFrame {
    fn drop(&mut self) {
        if std::thread::panicking() {
            std::process::abort();
        }
    }
}

impl Shared {
    /// Takes at most a completion queue of entries, under the mutex: a partial write's rest is
    /// resubmitted here, the eventfd's poll rearmed, a cancel's own entry skipped; every other
    /// entry ends a request, added to `batch` for its delivery once the mutex is let go.
    fn take_batch(&self, batch: &mut Vec<(u64, std::result::Result<i32, CompletionError>)>) {
        let mut state = self.state.lock();
        while batch.len() < CQ_BATCH {
            // SAFETY: holding `state` Mutex; no other CompletionQueue exists.
            let mut cq = unsafe { self.ring.completion_shared() };
            let Some(cqe) = cq.next() else {
                return;
            };
            drop(cq);
            let user_data = cqe.user_data();
            match user_data {
                CANCEL_TAG => continue,
                EVENTFD_TAG => {
                    let mut count = [0u8; 8];
                    // SAFETY: an 8-byte read into a live buffer from the eventfd held open; a
                    // nonblocking read finding nothing changes nothing.
                    unsafe { libc::read(self.eventfd.as_raw_fd(), count.as_mut_ptr().cast(), 8) };
                    // SAFETY: holding `state` Mutex.
                    unsafe { state.arm_eventfd(&self.ring, &self.eventfd) };
                    continue;
                }
                _ => {}
            }
            let result = cqe.result();
            turso_assert!(
                user_data != 0,
                "user_data must not be zero, we dont submit linked timeouts that would cause this"
            );
            if let Some(wstate) = state.writev_states.remove(&user_data) {
                // SAFETY: still holding `state` Mutex.
                let ended = unsafe {
                    state.handle_writev_completion(&self.ring, wstate, user_data, result)
                };
                if let Some(ended) = ended {
                    batch.push((user_data, ended));
                }
                continue;
            }
            batch.push((
                user_data,
                if result < 0 {
                    let err = std::io::Error::from_raw_os_error(-result);
                    Err(CompletionError::IOError(err.kind(), "io_uring_cqe"))
                } else {
                    Ok(result)
                },
            ));
        }
    }
}

impl Clock for UringIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

#[inline(always)]
/// use the callback pointer as the user_data for the operation as is
/// common practice for io_uring to prevent more indirection
fn get_key(c: Completion) -> u64 {
    Arc::into_raw(c.get_inner().clone()) as u64
}

#[inline(always)]
/// convert the user_data back to an Completion pointer
fn completion_from_key(key: u64) -> Completion {
    let c_inner = unsafe { Arc::from_raw(key as *const CompletionInner) };
    Completion {
        inner: Some(c_inner),
    }
}

/// Wake the waker registered on the completion identified by `key` (and on
/// its parent group, if any) without consuming the kernel's `Arc::into_raw`
/// reference. Used to fire a progress wake when an operation is resubmitted
/// (e.g., a writev split across chunks) so the future re-polls and continues
/// draining the CQ.
fn wake_user_data(key: u64) {
    let ptr = key as *const CompletionInner;
    // SAFETY: kernel owns one strong ref via the Arc::into_raw'd `key`. We
    // re-materialize the Arc to clone it, then re-leak the original so the
    // kernel's ref count is unchanged when this function returns.
    let kernel_ref = unsafe { Arc::from_raw(ptr) };
    let cloned = kernel_ref.clone();
    let _ = Arc::into_raw(kernel_ref);
    Completion {
        inner: Some(cloned),
    }
    .wake_progress();
}

pub struct UringFile {
    shared: Handle,
    file: std::fs::File,
}

impl Deref for UringFile {
    type Target = std::fs::File;
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

unsafe impl Send for UringFile {}
unsafe impl Sync for UringFile {}
crate::assert::assert_send_sync!(UringFile);

impl File for UringFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.as_fd();
        // F_SETLK is a non-blocking lock. The lock will be released when the file is closed
        // or the process exits or after an explicit unlock.
        fs::fcntl_lock(
            fd,
            if exclusive {
                FlockOperation::NonBlockingLockExclusive
            } else {
                FlockOperation::NonBlockingLockShared
            },
        )
        .map_err(|e| {
            let io_error = std::io::Error::from(e);
            let message = match io_error.kind() {
                ErrorKind::WouldBlock => {
                    "Failed locking file. File is locked by another process".to_string()
                }
                _ => format!("Failed locking file, {io_error}"),
            };
            LimboError::LockingError(message)
        })?;

        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        let fd = self.file.as_fd();
        fs::fcntl_lock(fd, FlockOperation::NonBlockingUnlock).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::from(e)
            ))
        })?;
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let r = c.as_read();
        let read_e = {
            let buf = r.buf();
            let ptr = buf.as_mut_ptr();
            let fd = io_uring::types::Fd(self.file.as_raw_fd());
            let len = buf.len();
            if let Some(idx) = buf.fixed_id() {
                trace!(
                    "pread_fixed(pos = {}, length = {}, idx = {})",
                    pos,
                    len,
                    idx
                );
                #[cfg(debug_assertions)]
                {
                    self.shared.state.lock().debug_check_fixed(idx, ptr, len);
                }
                io_uring::opcode::ReadFixed::new(fd, ptr, len as u32, idx as u16)
                    .offset(pos)
                    .build()
                    .user_data(get_key(c.clone()))
            } else {
                trace!("pread(pos = {}, length = {})", pos, len);
                io_uring::opcode::Read::new(fd, buf.as_mut_ptr(), len as u32)
                    .offset(pos)
                    .build()
                    .user_data(get_key(c.clone()))
            }
        };
        self.shared.accept(&c);
        let mut state = self.shared.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_entry(&self.shared.ring, &read_e) };
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let write = {
            let ptr = buffer.as_ptr();
            let len = buffer.len();
            let fd = io_uring::types::Fd(self.file.as_raw_fd());
            if let Some(idx) = buffer.fixed_id() {
                trace!(
                    "pwrite_fixed(pos = {}, length = {}, idx= {})",
                    pos,
                    len,
                    idx
                );
                #[cfg(debug_assertions)]
                {
                    self.shared.state.lock().debug_check_fixed(idx, ptr, len);
                }
                io_uring::opcode::WriteFixed::new(fd, ptr, len as u32, idx as u16)
                    .offset(pos)
                    .build()
                    .user_data(get_key(c.clone()))
            } else {
                trace!("pwrite(pos = {}, length = {})", pos, buffer.len());
                io_uring::opcode::Write::new(fd, ptr, len as u32)
                    .offset(pos)
                    .build()
                    .user_data(get_key(c.clone()))
            }
        };

        // Keep the buffer alive until the completion is processed. For non-fixed
        // buffers the SQE holds a raw pointer; without this the Arc would drop
        // here and the kernel could read freed memory.
        c.keep_write_buffer_alive(buffer);
        self.shared.accept(&c);
        let mut state = self.shared.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_entry(&self.shared.ring, &write) };
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: crate::io::FileSyncType) -> Result<Completion> {
        trace!("sync()");
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let sync = io_uring::opcode::Fsync::new(fd)
            .build()
            .user_data(get_key(c.clone()));
        self.shared.accept(&c);
        let mut state = self.shared.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_entry(&self.shared.ring, &sync) };
        Ok(c)
    }

    fn sync_data(&self, c: Completion) -> Result<Completion> {
        trace!("sync_data()");
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let sync = io_uring::opcode::Fsync::new(fd)
            .flags(io_uring::types::FsyncFlags::DATASYNC)
            .build()
            .user_data(get_key(c.clone()));
        self.shared.accept(&c);
        let mut state = self.shared.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_entry(&self.shared.ring, &sync) };
        Ok(c)
    }

    fn supports_durable_writes(&self) -> bool {
        true
    }

    /// A write with `RWF_DSYNC`: it completes once what it wrote is durable.
    fn pwrite_durable(
        &self,
        pos: u64,
        buffer: Arc<crate::Buffer>,
        c: Completion,
    ) -> Result<Completion> {
        let write = {
            let ptr = buffer.as_ptr();
            let len = buffer.len();
            let fd = io_uring::types::Fd(self.file.as_raw_fd());
            let dsync = libc::RWF_DSYNC as io_uring::types::RwFlags;
            trace!("pwrite_durable(pos = {}, length = {})", pos, len);
            if let Some(idx) = buffer.fixed_id() {
                #[cfg(debug_assertions)]
                {
                    self.shared.state.lock().debug_check_fixed(idx, ptr, len);
                }
                io_uring::opcode::WriteFixed::new(fd, ptr, len as u32, idx as u16)
                    .offset(pos)
                    .rw_flags(dsync)
                    .build()
                    .user_data(get_key(c.clone()))
            } else {
                io_uring::opcode::Write::new(fd, ptr, len as u32)
                    .offset(pos)
                    .rw_flags(dsync)
                    .build()
                    .user_data(get_key(c.clone()))
            }
        };
        // The SQE holds a raw pointer into the buffer until the completion is processed.
        c.keep_write_buffer_alive(buffer);
        self.shared.accept(&c);
        let mut state = self.shared.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_entry(&self.shared.ring, &write) };
        Ok(c)
    }

    fn allocate(&self, pos: u64, len: u64) -> Result<()> {
        super::refuse_on_delivery_thread("an allocation");
        common::allocate(&self.file, pos, len)
    }

    fn pwritev(
        &self,
        pos: u64,
        bufs: Vec<Arc<crate::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        tracing::trace!("pwritev(pos = {}, bufs.len() = {})", pos, bufs.len());

        let wstate = WritevState::new(self, pos, bufs);
        self.shared.accept(&c);
        let mut state = self.shared.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_writev(&self.shared.ring, get_key(c.clone()), wstate) };
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        Ok(self
            .file
            .metadata()
            .map_err(|e| io_error(e, "metadata"))?
            .len())
    }

    /// Frees the blocks of `[pos, pos + len)`: the range reads back as zeros and keeps the file's
    /// size.
    #[cfg(target_os = "linux")]
    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        super::refuse_on_delivery_thread("a hole's punch");
        use rustix::fs::{fallocate, FallocateFlags};
        fallocate(
            &self.file,
            FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE,
            pos as u64,
            len as u64,
        )
        .map_err(|e| crate::LimboError::CompletionError(e.into()))
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        if self.shared.caps.ftruncate {
            let truncate = io_uring::opcode::Ftruncate::new(fd, len)
                .build()
                .user_data(get_key(c.clone()));
            self.shared.accept(&c);
            let mut state = self.shared.state.lock();
            // SAFETY: holding `state` Mutex.
            unsafe { state.submit_entry(&self.shared.ring, &truncate) };
            Ok(c)
        } else {
            super::refuse_on_delivery_thread("a truncation without the ring's opcode");
            let result = self.file.set_len(len);
            match result {
                Ok(()) => {
                    trace!("file truncated to len=({})", len);
                    c.deliver(Ok(0));
                    Ok(c)
                }
                Err(e) => Err(io_error(e, "truncate")),
            }
        }
    }

    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, true, kind).map(|_| ())
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, false, kind)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        unix_shared_wal_unlock_byte(self.file.as_raw_fd(), offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.file
            .set_len(len)
            .map_err(|err| io_error(err, "resize shared WAL coordination file"))
    }

    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        unix_shared_wal_map(offset, len, self.file.as_raw_fd())
    }
}

impl Drop for UringFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::common;
    use std::cell::Cell;
    use std::io::Write as _;
    use std::sync::mpsc;
    use std::time::Duration;

    crate::thread::thread_local! {
        /// Set by a test on its own thread: its submissions queue entries without entering them.
        pub(super) static ENTER_HELD: Cell<bool> = const { Cell::new(false) };
        /// Set by a test on its own thread: the setup stage its next ring refuses at.
        static REFUSED_AT: Cell<Option<Setup>> = const { Cell::new(None) };
    }

    /// The stages of a ring's setup a test can refuse.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum Setup {
        Ring,
        Registration,
        Eventfd,
        Thread,
    }

    /// The refusal a test asked for at `stage`, as the kernel or the system would give it.
    pub(super) fn refuse(stage: Setup) -> Result<()> {
        if REFUSED_AT.with(|r| r.get()) != Some(stage) {
            return Ok(());
        }
        Err(match stage {
            Setup::Ring => explain_setup_failure(std::io::Error::from_raw_os_error(libc::EPERM)),
            Setup::Registration => io_error(
                std::io::Error::from_raw_os_error(libc::ENOMEM),
                "register_buffers",
            ),
            Setup::Eventfd => LimboError::IoBackendUnavailable(
                "io_uring: the delivery thread's eventfd: refused by the test".into(),
            ),
            Setup::Thread => LimboError::IoBackendUnavailable(
                "io_uring: the delivery thread did not start: refused by the test".into(),
            ),
        })
    }

    /// How long a test waits for a delivery before it calls the delivery lost: a delivery takes a
    /// read's latency, microseconds, so ten seconds is many thousand times it on a loaded machine.
    const BOUND: Duration = Duration::from_secs(10);

    fn file_of(dir: &tempfile::TempDir, io: &UringIO, len: usize) -> Arc<dyn File> {
        let path = dir.path().join("f");
        std::fs::write(&path, vec![7u8; len]).unwrap();
        io.open_file(path.to_str().unwrap(), OpenFlags::None, false)
            .unwrap()
    }

    /// A read whose callback sends on `tx` as it is delivered.
    fn read_telling(tx: &mpsc::Sender<i32>) -> Completion {
        let tx = tx.clone();
        Completion::new_read(Arc::new(crate::Buffer::new_temporary(512)), move |r| {
            let _ = tx.send(r.map(|(_, n)| n).unwrap_or(-1));
            None
        })
    }

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UringIO::new);
    }

    /// A request finished by its owner while the kernel still holds it (as a cancel finishes one)
    /// stays owned, with its buffer, until the delivery thread delivers its entry, and a drain parks
    /// until then (DESIGN.md 22.5). The read is from an empty pipe, so the kernel holds it until the
    /// test writes to the pipe: nothing else can end it.
    #[test]
    fn a_request_finished_while_the_kernel_holds_it_is_owned_until_delivered() {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("fifo");
        let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        // SAFETY: a plain mkfifo(3) on a fresh path.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        let io = Arc::new(UringIO::new().unwrap());
        let pipe = io
            .open_file(fifo.to_str().unwrap(), OpenFlags::NoLock, false)
            .unwrap();
        let c = Completion::new_read(Arc::new(crate::Buffer::new_temporary(1)), |_| None);
        let c = pipe.pread(0, c).unwrap();
        c.abort();
        assert!(c.finished());
        assert!(c.is_owned(), "let go while the kernel holds it");
        let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let drainer = {
            let (io, c, drained) = (io.clone(), c.clone(), drained.clone());
            std::thread::spawn(move || {
                io.drain_completions(std::slice::from_ref(&c)).unwrap();
                drained.store(true, Ordering::Release);
            })
        };
        // Long enough for the drainer to start and park, which takes microseconds; a drain that
        // did not wait would have returned well within it.
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !drained.load(Ordering::Acquire),
            "the drain returned while the kernel holds the read"
        );
        std::fs::OpenOptions::new()
            .write(true)
            .open(&fifo)
            .unwrap()
            .write_all(b"x")
            .unwrap();
        drainer.join().unwrap();
        assert!(!c.is_owned());
    }

    /// A request nobody waits on is delivered all the same: the delivery thread delivers every
    /// request, and nobody steps the ring.
    #[test]
    fn a_request_nobody_waits_on_is_delivered() {
        let dir = tempfile::tempdir().unwrap();
        let io = UringIO::new().unwrap();
        let file = file_of(&dir, &io, 512);
        let (tx, rx) = mpsc::channel();
        let c = file.pread(0, read_telling(&tx)).unwrap();
        assert_eq!(rx.recv_timeout(BOUND), Ok(512), "never delivered");
        drop(c);
    }

    /// A callback on the delivery thread may submit more: each is delivered in turn.
    #[test]
    fn a_callback_may_submit_more() {
        let dir = tempfile::tempdir().unwrap();
        let io = UringIO::new().unwrap();
        let file = file_of(&dir, &io, 512);
        let (tx, rx) = mpsc::channel();
        fn chain(file: Arc<dyn File>, left: u32, tx: mpsc::Sender<u32>) {
            let next = file.clone();
            let c = Completion::new_read(Arc::new(crate::Buffer::new_temporary(512)), move |_| {
                if left == 0 {
                    let _ = tx.send(0);
                } else {
                    chain(next.clone(), left - 1, tx.clone());
                }
                None
            });
            drop(file.pread(0, c).unwrap());
        }
        chain(file, 3, tx);
        assert_eq!(rx.recv_timeout(BOUND), Ok(0), "the chain stopped");
    }

    /// Waiters on several threads park, each on its own request, and the delivery thread wakes
    /// each as its request finishes.
    #[test]
    fn concurrent_waiters_are_each_woken() {
        let dir = tempfile::tempdir().unwrap();
        let io = Arc::new(UringIO::new().unwrap());
        let file = file_of(&dir, &io, 512);
        let waiters: Vec<_> = (0..8)
            .map(|_| {
                let (io, file) = (io.clone(), file.clone());
                std::thread::spawn(move || {
                    for _ in 0..64 {
                        let c = Completion::new_read(
                            Arc::new(crate::Buffer::new_temporary(512)),
                            |_| None,
                        );
                        let c = file.pread(0, c).unwrap();
                        io.wait_for_completion(c).unwrap();
                    }
                })
            })
            .collect();
        for w in waiters {
            w.join().unwrap();
        }
    }

    /// Requests queued past a full submission queue are moved into the kernel by the delivery
    /// thread before each of its waits: once one entry is entered, every one is delivered, with
    /// no submission after (mutant: the wait entered without moving them in).
    #[test]
    fn requests_queued_past_a_full_submission_queue_are_moved_in_before_each_wait() {
        let dir = tempfile::tempdir().unwrap();
        let io = UringIO::with_entries(4).unwrap();
        let file = file_of(&dir, &io, 512);
        let (tx, rx) = mpsc::channel();
        ENTER_HELD.with(|h| h.set(true));
        let held: Vec<_> = (0..12)
            .map(|_| file.pread(0, read_telling(&tx)).unwrap())
            .collect();
        assert!(
            !io.shared.state.lock().overflow.is_empty(),
            "nothing queued past the submission queue"
        );
        ENTER_HELD.with(|h| h.set(false));
        let last = file.pread(0, read_telling(&tx)).unwrap();
        for i in 0..13 {
            assert_eq!(
                rx.recv_timeout(BOUND),
                Ok(512),
                "{i} of 13 delivered, the rest left past the submission queue"
            );
        }
        drop((held, last));
    }

    /// The delivery thread ends once no handle lives and no request is outstanding: a request
    /// still in flight when the last handle goes is delivered first.
    #[test]
    fn the_delivery_thread_ends_once_its_handles_are_gone_and_nothing_is_outstanding() {
        let dir = tempfile::tempdir().unwrap();
        let io = UringIO::new().unwrap();
        let file = file_of(&dir, &io, 512);
        let shared = Arc::downgrade(&io.shared.0);
        let (tx, rx) = mpsc::channel();
        drop(file.pread(0, read_telling(&tx)).unwrap());
        drop((file, io));
        assert_eq!(
            rx.recv_timeout(BOUND),
            Ok(512),
            "outstanding, never delivered"
        );
        let since = std::time::Instant::now();
        while shared.upgrade().is_some() {
            assert!(
                since.elapsed() < BOUND,
                "the delivery thread outlived its handles"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// The last handle dropped with nothing in flight, the thread waiting in the kernel: the
    /// eventfd's poll ends the wait, and the thread ends.
    #[test]
    fn the_delivery_thread_ends_when_its_last_handle_goes_with_nothing_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        let io = UringIO::new().unwrap();
        let file = file_of(&dir, &io, 512);
        let shared = Arc::downgrade(&io.shared.0);
        let (tx, rx) = mpsc::channel();
        drop(file.pread(0, read_telling(&tx)).unwrap());
        assert_eq!(rx.recv_timeout(BOUND), Ok(512));
        // Delivered and let go: the thread waits on nothing but the eventfd.
        while io.shared.requests.load(Ordering::Acquire) > 0 {
            std::thread::yield_now();
        }
        drop((file, io));
        let since = std::time::Instant::now();
        while shared.upgrade().is_some() {
            assert!(
                since.elapsed() < BOUND,
                "the delivery thread outlived its handles"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// The last handle dropped inside a callback, on the delivery thread itself: the thread ends
    /// after its batch, joining nothing.
    #[test]
    fn the_last_handle_dropped_inside_a_callback_ends_the_thread_after_its_batch() {
        let dir = tempfile::tempdir().unwrap();
        let io = UringIO::new().unwrap();
        let file = file_of(&dir, &io, 512);
        let shared = Arc::downgrade(&io.shared.0);
        let held = Arc::new(crate::sync::Mutex::new(Some(file.clone())));
        let (tx, rx) = mpsc::channel();
        let c = {
            let held = held.clone();
            Completion::new_read(Arc::new(crate::Buffer::new_temporary(512)), move |_| {
                drop(held.lock().take());
                let _ = tx.send(());
                None
            })
        };
        drop(file.pread(0, c).unwrap());
        drop((file, io, held));
        assert_eq!(rx.recv_timeout(BOUND), Ok(()), "never delivered");
        let since = std::time::Instant::now();
        while shared.upgrade().is_some() {
            assert!(
                since.elapsed() < BOUND,
                "the delivery thread outlived its handles"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// A ring whose setup is refused at any stage is torn down whole and says why: the ring
    /// itself, its buffers' registration, the delivery thread's eventfd, the thread.
    #[test]
    fn a_refused_start_tears_the_ring_down_and_says_why() {
        for stage in [
            Setup::Ring,
            Setup::Registration,
            Setup::Eventfd,
            Setup::Thread,
        ] {
            REFUSED_AT.with(|r| r.set(Some(stage)));
            let refused = UringIO::new();
            REFUSED_AT.with(|r| r.set(None));
            let Err(e) = refused else {
                panic!("{stage:?}: a refused start opened a ring");
            };
            let said = e.to_string();
            let expected = match stage {
                Setup::Ring => "io_uring_setup",
                Setup::Registration => "register_buffers",
                Setup::Eventfd => "eventfd",
                Setup::Thread => "delivery thread did not start",
            };
            assert!(said.contains(expected), "{stage:?}: said {said}");
        }
        // And a start that is not refused still opens.
        drop(UringIO::new().unwrap());
    }

    /// A wait the kernel cuts short is waited again.
    #[test]
    fn an_interrupted_wait_is_waited_again() {
        let dir = tempfile::tempdir().unwrap();
        let io = UringIO::new().unwrap();
        let file = file_of(&dir, &io, 512);
        io.shared
            .injected_wait
            .store(libc::EINTR, Ordering::Release);
        // The first read wakes the thread, which delivers it and meets the interruption at its
        // next wait; the second is delivered only if the thread waited again.
        let (tx, rx) = mpsc::channel();
        drop(file.pread(0, read_telling(&tx)).unwrap());
        assert_eq!(rx.recv_timeout(BOUND), Ok(512));
        drop(file.pread(0, read_telling(&tx)).unwrap());
        assert_eq!(
            rx.recv_timeout(BOUND),
            Ok(512),
            "never delivered after EINTR"
        );
    }

    /// A callback that returns an error: no waiter sees the request finished before the callback
    /// returned, and the error is the result.
    #[test]
    fn a_waiter_sees_the_callbacks_error_only_once_it_returned() {
        let dir = tempfile::tempdir().unwrap();
        let io = Arc::new(UringIO::new().unwrap());
        let file = file_of(&dir, &io, 512);
        let returned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let c = {
            let returned = returned.clone();
            Completion::new_read(Arc::new(crate::Buffer::new_temporary(4096)), move |_| {
                std::thread::sleep(Duration::from_millis(20));
                returned.store(true, Ordering::Release);
                Some(CompletionError::ShortRead {
                    page_idx: 1,
                    expected: 4096,
                    actual: 512,
                })
            })
        };
        let c = file.pread(0, c).unwrap();
        let waited = io.wait_for_completion(c);
        assert!(
            returned.load(Ordering::Acquire),
            "the waiter saw it finished while its callback ran"
        );
        assert!(matches!(
            waited,
            Err(LimboError::CompletionError(
                CompletionError::ShortRead { .. }
            ))
        ));
    }

    /// What ends the process in a child test: the delivery thread asked to wait, or to make a
    /// synchronous file call, from a callback; and an error of the ring itself with a request
    /// outstanding. Each child waits a bounded time for the request, then exits on its own,
    /// which the parent reads as the thread having gone on.
    fn ends_the_process(
        test: &str,
        cause: &str,
        act: impl FnOnce(&UringIO, &Arc<dyn File>, &tempfile::TempDir),
    ) {
        let Some(ended) = crate::child_test::in_child(test, || {
            let dir = tempfile::tempdir().unwrap();
            let io = UringIO::new().unwrap();
            let file = file_of(&dir, &io, 512);
            act(&io, &file, &dir);
            std::thread::sleep(BOUND);
            std::process::exit(0);
        }) else {
            return;
        };
        assert!(
            ended.aborted,
            "{test}: the process went on (code {:?}): {}",
            ended.code, ended.stderr
        );
        assert!(!ended.hung, "{test}: hung");
        assert!(
            ended.stderr.contains(cause),
            "{test}: ended, but not for {cause:?}: {}",
            ended.stderr
        );
    }

    #[test]
    fn a_callback_that_waits_ends_the_process() {
        ends_the_process(
            "io::io_uring::tests::a_callback_that_waits_ends_the_process",
            "a wait for a completion on the ring's delivery thread",
            |_, file, _| {
                // Any IO's wait: what is refused is waiting on the delivery thread.
                let io: &'static UringIO = Box::leak(Box::new(UringIO::new().unwrap()));
                let other = Completion::new_wait();
                let c =
                    Completion::new_read(Arc::new(crate::Buffer::new_temporary(512)), move |_| {
                        let _ = io.wait_for_completion(other.clone());
                        None
                    });
                drop(file.pread(0, c).unwrap());
            },
        );
    }

    #[test]
    fn a_callback_that_syncs_a_directory_ends_the_process() {
        ends_the_process(
            "io::io_uring::tests::a_callback_that_syncs_a_directory_ends_the_process",
            "a directory sync on the ring's delivery thread",
            |_, file, dir| {
                let io: &'static UringIO = Box::leak(Box::new(UringIO::new().unwrap()));
                let path = dir.path().to_str().unwrap().to_string();
                let c =
                    Completion::new_read(Arc::new(crate::Buffer::new_temporary(512)), move |_| {
                        let _ = io.sync_directory(&path);
                        None
                    });
                drop(file.pread(0, c).unwrap());
            },
        );
    }

    #[test]
    fn an_error_of_the_ring_itself_ends_the_process() {
        ends_the_process(
            "io::io_uring::tests::an_error_of_the_ring_itself_ends_the_process",
            "the ring can no longer deliver the requests it accepted",
            |io, file, _| {
                io.shared
                    .injected_wait
                    .store(libc::EBADF, Ordering::Release);
                // The first read wakes the thread, which delivers it and meets the error at its
                // next wait.
                let (tx, rx) = mpsc::channel();
                drop(file.pread(0, read_telling(&tx)).unwrap());
                assert_eq!(rx.recv_timeout(BOUND), Ok(512));
                // Delivered only if the thread went on past the error.
                drop(file.pread(0, read_telling(&tx)).unwrap());
                if rx.recv_timeout(BOUND).is_ok() {
                    std::process::exit(0);
                }
            },
        );
    }

    #[test]
    fn permission_denied_names_the_kernel_setting() {
        assert_eq!(
            permission_denied_message(Some("2")),
            "io_uring_setup: permission denied (io_uring_disabled=2)"
        );
        assert_eq!(
            permission_denied_message(None),
            "io_uring_setup: permission denied"
        );
    }
}
