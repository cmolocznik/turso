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
    /// The completion queue entries a pass of the delivery thread visits at most: the queue's own
    /// size, as the kernel set it (twice the submission queue by default).
    pass: usize,
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
    super::fatal(format_args!(
        "io_uring: {during} failed ({e}): the ring can no longer deliver the requests it accepted"
    ))
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
        let shared = Shared::new(entries)?;
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

impl Shared {
    /// A ring of `entries` submission entries, its buffers registered and its eventfd's poll
    /// armed, with no thread delivering yet.
    fn new(entries: u32) -> Result<Arc<Self>> {
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
        let pass = ring.params().cq_entries() as usize;
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
            pass,
            #[cfg(test)]
            injected_wait: std::sync::atomic::AtomicI32::new(0),
        });
        {
            let mut state = shared.state.lock();
            // SAFETY: holding `state` Mutex.
            unsafe { state.arm_eventfd(&shared.ring, &shared.eventfd) };
        }
        Ok(shared)
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

    /// Handle a writev CQE: resubmits the rest of a partial write and says so, for its progress
    /// wake, or says how the whole write ended, for its delivery; either runs once the lock is
    /// released. SAFETY: caller must hold the `RingState` Mutex.
    unsafe fn handle_writev_completion(
        &mut self,
        ring: &io_uring::IoUring,
        mut state: WritevState,
        user_data: u64,
        result: i32,
    ) -> Taken {
        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            tracing::error!("writev failed (user_data: {}): {}", user_data, err);
            state.free_last_iov(&mut self.iov_pool);
            return Taken::Ended(
                user_data,
                Err(CompletionError::IOError(err.kind(), "pwritev")),
            );
        }

        let written = result;

        if written == 0 && state.remaining() > 0 {
            state.free_last_iov(&mut self.iov_pool);
            return Taken::Ended(user_data, Err(CompletionError::ShortWrite));
        }
        state.advance(written as u64);

        match state.remaining() {
            0 => {
                tracing::debug!(
                    "writev operation completed: wrote {} bytes",
                    state.total_written
                );
                state.free_last_iov(&mut self.iov_pool);
                Taken::Ended(user_data, Ok(state.total_written as i32))
            }
            remaining => {
                tracing::trace!(
                    "resubmitting writev operation for user_data {}: wrote {} bytes, remaining {}",
                    user_data,
                    written,
                    remaining
                );
                self.submit_writev(ring, user_data, state);
                Taken::Progress(user_data)
            }
        }
    }
}

/// An entry of the completion queue, taken under the ring's mutex and acted on once it is let go
/// (DESIGN.md 22.5).
enum Taken {
    /// A request ended with this result: its delivery.
    Ended(u64, std::result::Result<i32, CompletionError>),
    /// A partial write's rest was resubmitted: a progress wake for whoever waits on the write,
    /// whose own completion fires only on the last part. Without it, a future parked on the
    /// write is never polled while the parts complete.
    Progress(u64),
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

    /// Aborts each of `completions`, then asks the kernel to cancel each. The aborts run their
    /// callbacks with the ring's mutex let go, since a callback may submit its next request, which
    /// takes the mutex (DESIGN.md 22.5); each request stays the kernel's, with its buffer, until the
    /// delivery thread delivers its entry.
    fn cancel(&self, completions: &[Completion]) -> Result<()> {
        for c in completions {
            c.abort();
        }
        let mut state = self.shared.state.lock();
        for c in completions {
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

    /// The delivery thread delivers every request: a waiter parks until it has, and the trait's
    /// waits do that for this answer.
    fn delivery(&self) -> super::Delivery {
        super::Delivery::Thread
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
/// visits at most one completion queue of entries under the mutex, resubmitting the rest of a
/// partial write there, lets the mutex go, delivers each ended request and wakes each partial
/// write's waiter in the order taken, and waits again: entries left in the queue end that wait at
/// once. It ends once no handle
/// lives and no request is outstanding. A delivery that panics ends the process: nothing else
/// would deliver the ring's requests, so returning would strand every waiter on them.
fn deliver(shared: Arc<Shared>) {
    super::DELIVERING.with(|d| d.set(true));
    let _frame = DeliveryFrame;
    let mut batch = Vec::with_capacity(shared.pass);
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
        for taken in batch.drain(..) {
            match taken {
                Taken::Ended(key, result) => {
                    completion_from_key(key).deliver(result);
                    shared.requests.fetch_sub(1, Ordering::AcqRel);
                }
                // SAFETY: the kernel's reference is still held: the write's own delivery, the
                // only thing that lets it go, is made by this thread after this entry.
                Taken::Progress(key) => unsafe { wake_user_data(key) },
            }
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
    /// Takes the completion queue's entries under the mutex, at most [`Shared::pass`] of them.
    fn take_batch(&self, batch: &mut Vec<Taken>) {
        let mut state = self.state.lock();
        self.take(&mut state, batch, || {
            // SAFETY: the caller holds `state`'s Mutex; no other CompletionQueue exists, and
            // this one is gone before the entry is handled.
            let mut cq = unsafe { self.ring.completion_shared() };
            cq.next().map(|cqe| (cqe.user_data(), cqe.result()))
        });
    }

    /// Visits at most [`Shared::pass`] entries of `next`, whatever each is: a partial write's
    /// rest is resubmitted here and its progress wake added to `batch`, the eventfd's poll rearmed,
    /// a cancel's own entry skipped; every other entry ends a request, added to `batch` for its
    /// delivery once the mutex is let go. A resubmission or a rearmed poll the kernel completes at
    /// once adds an entry to the queue this pass is reading, so only counting every entry bounds
    /// how long the pass holds the mutex; what is left is the next pass's.
    fn take(
        &self,
        state: &mut RingState,
        batch: &mut Vec<Taken>,
        mut next: impl FnMut() -> Option<(u64, i32)>,
    ) {
        for _ in 0..self.pass {
            let Some((user_data, result)) = next() else {
                return;
            };
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
            turso_assert!(
                user_data != 0,
                "user_data must not be zero, we dont submit linked timeouts that would cause this"
            );
            if let Some(wstate) = state.writev_states.remove(&user_data) {
                // SAFETY: still holding `state` Mutex.
                batch.push(unsafe {
                    state.handle_writev_completion(&self.ring, wstate, user_data, result)
                });
                continue;
            }
            batch.push(Taken::Ended(
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
/// draining the CQ. Called with the ring's mutex let go: a waker may submit.
///
/// SAFETY: the kernel's reference for `key` is still held.
unsafe fn wake_user_data(key: u64) {
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
        super::refuse_on_delivery_thread("a file's lock");
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
        super::refuse_on_delivery_thread("a file's size");
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
        super::refuse_on_delivery_thread("a shared WAL byte's lock");
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, true, kind).map(|_| ())
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        super::refuse_on_delivery_thread("a shared WAL byte's lock");
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, false, kind)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        unix_shared_wal_unlock_byte(self.file.as_raw_fd(), offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        super::refuse_on_delivery_thread("a shared WAL file's resize");
        self.file
            .set_len(len)
            .map_err(|err| io_error(err, "resize shared WAL coordination file"))
    }

    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        super::refuse_on_delivery_thread("a shared WAL file's map");
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
    /// until then, taking no step (DESIGN.md 22.5). The read is from an empty pipe, so the kernel
    /// holds it until the test writes to the pipe: nothing else can end it.
    #[test]
    fn a_request_finished_while_the_kernel_holds_it_is_owned_until_delivered() {
        let dir = tempfile::tempdir().unwrap();
        let io = crate::io::wait_tests::StepCounting::new(Arc::new(UringIO::new().unwrap()));
        let (c, fifo) = held_read(&dir, &*io);
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
        write_to(&fifo);
        drainer.join().unwrap();
        assert!(!c.is_owned());
        assert_eq!(io.steps(), 0, "the drain stepped the ring");
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

    /// A read from an empty pipe on `io`, which the kernel holds until [`write_to`] writes to the
    /// pipe: nothing else can end it.
    fn held_read(dir: &tempfile::TempDir, io: &dyn IO) -> (Completion, std::path::PathBuf) {
        let fifo = dir.path().join("fifo");
        let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        // SAFETY: a plain mkfifo(3) on a fresh path.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        let pipe = io
            .open_file(fifo.to_str().unwrap(), OpenFlags::NoLock, false)
            .unwrap();
        let c = Completion::new_read(Arc::new(crate::Buffer::new_temporary(1)), |_| None);
        (pipe.pread(0, c).unwrap(), fifo)
    }

    /// Ends a [`held_read`].
    fn write_to(fifo: &std::path::Path) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(fifo)
            .unwrap()
            .write_all(b"x")
            .unwrap();
    }

    /// A blocking waiter on the ring parks and the delivery thread wakes it: a request nobody
    /// waits for delivered first, then the waiter's own, held by the kernel for a while, with no
    /// caller after it; the wait takes no step (DESIGN.md 22.5; mutant: the ring taken for a
    /// pumped backend, whose waiter steps while the kernel holds its read).
    #[test]
    fn a_waiter_parks_on_the_ring_and_takes_no_step() {
        let dir = tempfile::tempdir().unwrap();
        let ring = Arc::new(UringIO::new().unwrap());
        let file = file_of(&dir, &ring, 512);
        let io = crate::io::wait_tests::StepCounting::new(ring);
        let (tx, rx) = mpsc::channel();
        drop(file.pread(0, read_telling(&tx)).unwrap());
        assert_eq!(rx.recv_timeout(BOUND), Ok(512));
        let (c, fifo) = held_read(&dir, &*io);
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = {
            let (io, c) = (io.clone(), c.clone());
            std::thread::spawn(move || {
                io.wait(&c);
                let _ = done_tx.send(());
            })
        };
        // Long enough for the waiter to start and park, which takes microseconds.
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            done_rx.try_recv().is_err(),
            "the wait returned while the kernel holds the read"
        );
        write_to(&fifo);
        assert_eq!(
            done_rx.recv_timeout(BOUND),
            Ok(()),
            "the waiter was not woken"
        );
        waiter.join().unwrap();
        assert!(c.succeeded());
        assert_eq!(io.steps(), 0, "the waiter stepped the ring");
    }

    /// A waker that unparks the thread that made it.
    fn unparking() -> std::task::Waker {
        struct Unpark(std::thread::Thread);
        impl std::task::Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        std::task::Waker::from(Arc::new(Unpark(std::thread::current())))
    }

    /// A cooperative waiter on the ring is woken by the delivery thread: polled once, pending, it
    /// parks until its waker fires, with a request nobody waits for delivered first and no caller
    /// after it, and no step taken (DESIGN.md 22.5).
    #[test]
    fn a_cooperative_waiter_on_the_ring_is_woken_by_the_delivery_thread() {
        use std::future::Future as _;
        let dir = tempfile::tempdir().unwrap();
        let ring = Arc::new(UringIO::new().unwrap());
        let file = file_of(&dir, &ring, 512);
        let io = crate::io::wait_tests::StepCounting::new(ring);
        let (tx, rx) = mpsc::channel();
        drop(file.pread(0, read_telling(&tx)).unwrap());
        assert_eq!(rx.recv_timeout(BOUND), Ok(512));
        let (c, fifo) = held_read(&dir, &*io);
        let waker = unparking();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut wait = std::pin::pin!(crate::types::IOCompletions(c.clone()).wait_async(&*io));
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        write_to(&fifo);
        let since = std::time::Instant::now();
        loop {
            if wait.as_mut().poll(&mut cx).is_ready() {
                break;
            }
            assert!(since.elapsed() < BOUND, "the poller was never woken");
            std::thread::park_timeout(BOUND);
        }
        assert!(c.succeeded());
        assert_eq!(io.steps(), 0, "the cooperative waiter stepped the ring");
    }

    /// A non-blocking embedder's statement on the ring is woken by the delivery thread: each step
    /// that reports IO is stepped again only once the waker it was given fires, and the embedder
    /// takes no step of the IO (DESIGN.md 22.5).
    #[test]
    fn a_nonblocking_embedder_on_the_ring_is_woken_by_the_delivery_thread() {
        let dir = tempfile::tempdir().unwrap();
        let io = crate::io::wait_tests::StepCounting::new(Arc::new(UringIO::new().unwrap()));
        let path = dir.path().join("embedder.db");
        let db = crate::Database::open_file(
            io.clone(),
            path.to_str().unwrap(),
            Arc::new(crate::SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)")
            .unwrap();
        let steps_before = io.steps();
        let waker = unparking();
        let mut stmt = conn
            .prepare("INSERT INTO t VALUES (1, zeroblob(65536))")
            .unwrap();
        let since = std::time::Instant::now();
        let mut waits = 0;
        loop {
            match stmt.step_with_waker(&waker).unwrap() {
                crate::StepResult::Done => break,
                crate::StepResult::IO | crate::StepResult::Yield => {
                    waits += 1;
                    assert!(since.elapsed() < BOUND, "the embedder was never woken");
                    std::thread::park_timeout(BOUND);
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        assert!(
            waits > 0,
            "the statement never waited on the ring, so the test saw nothing"
        );
        assert_eq!(io.steps(), steps_before, "the embedder stepped the ring");
    }

    /// Statements on the ring take no step: each blocking wait of theirs parks until the delivery
    /// thread delivers what it waits for (mutant: the statement's wait stepping the IO in a loop,
    /// as before).
    #[test]
    fn statements_on_the_ring_take_no_step() {
        let dir = tempfile::tempdir().unwrap();
        let io = crate::io::wait_tests::StepCounting::new(Arc::new(UringIO::new().unwrap()));
        let path = dir.path().join("ring.db");
        let db = crate::Database::open_file(
            io.clone(),
            path.to_str().unwrap(),
            Arc::new(crate::SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y BLOB)")
            .unwrap();
        for i in 0..64 {
            conn.execute(format!("INSERT INTO t VALUES ({i}, zeroblob(4096))"))
                .unwrap();
        }
        let mut stmt = conn
            .prepare("SELECT count(*), sum(length(y)) FROM t")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(io.steps(), 0, "a statement stepped the ring");
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
    /// which the parent reads as the thread having gone on. The process ending is asserted alone;
    /// what it said, `cause`, only where standard error can carry it.
    fn ends_the_process(
        test: &str,
        cause: Option<&str>,
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
        if let Some(cause) = cause {
            assert!(
                ended.stderr.contains(cause),
                "{test}: ended, but not for {cause:?}: {}",
                ended.stderr
            );
        }
    }

    #[test]
    fn a_callback_that_waits_ends_the_process() {
        ends_the_process(
            "io::io_uring::tests::a_callback_that_waits_ends_the_process",
            Some("a wait for a completion on the ring's delivery thread"),
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
            Some("a directory sync on the ring's delivery thread"),
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
            Some("the ring can no longer deliver the requests it accepted"),
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

    /// The same with another thread holding standard error, as any print does, while it waits for
    /// a read the dead ring will never deliver: the error is written without the lock and the
    /// process ends (mutant: the message printed through the lock, which waits on its holder for
    /// ever).
    #[test]
    fn an_error_of_the_ring_ends_the_process_while_another_thread_holds_stderr() {
        ends_the_process(
            "io::io_uring::tests::an_error_of_the_ring_ends_the_process_while_another_thread_holds_stderr",
            Some("the ring can no longer deliver the requests it accepted"),
            |io, file, _| {
                io.shared
                    .injected_wait
                    .store(libc::EBADF, Ordering::Release);
                // Held to the child's end: were it let go when this returns, a print waiting on
                // it would go through and the abort would still come.
                std::mem::forget(std::io::stderr().lock());
                let (tx, rx) = mpsc::channel();
                drop(file.pread(0, read_telling(&tx)).unwrap());
                let _ = rx.recv_timeout(BOUND);
                drop(file.pread(0, read_telling(&tx)).unwrap());
                if rx.recv_timeout(BOUND).is_ok() {
                    std::process::exit(0);
                }
            },
        );
    }

    /// The same with standard error a full pipe whose reader has stopped reading: the message
    /// finds no room and the process ends all the same (DESIGN.md 22.11; mutant: the message
    /// written by a write that waits for room, which never comes).
    #[test]
    fn an_error_of_the_ring_ends_the_process_with_stderr_a_full_pipe() {
        ends_the_process(
            "io::io_uring::tests::an_error_of_the_ring_ends_the_process_with_stderr_a_full_pipe",
            None,
            |io, file, _| {
                crate::io::retire_tests::stderr_a_full_pipe();
                io.shared
                    .injected_wait
                    .store(libc::EBADF, Ordering::Release);
                let (tx, rx) = mpsc::channel();
                drop(file.pread(0, read_telling(&tx)).unwrap());
                let _ = rx.recv_timeout(BOUND);
                drop(file.pread(0, read_telling(&tx)).unwrap());
                if rx.recv_timeout(BOUND).is_ok() {
                    std::process::exit(0);
                }
            },
        );
    }

    /// A FIFO in `dir`, opened on `io`: a read from it stays the kernel's until something is
    /// written to it.
    fn fifo_of(dir: &tempfile::TempDir, io: &UringIO) -> (std::path::PathBuf, Arc<dyn File>) {
        let fifo = dir.path().join("fifo");
        let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        // SAFETY: a plain mkfifo(3) on a fresh path.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        let file = io
            .open_file(fifo.to_str().unwrap(), OpenFlags::NoLock, false)
            .unwrap();
        (fifo, file)
    }

    /// A cancelled request's callback runs with the ring's mutex let go: one that submits its next
    /// request, as any callback may, gets the mutex (DESIGN.md 22.5; mutant: the abort run under
    /// the mutex, where that submission waits for ever on the mutex its own thread holds).
    #[test]
    fn a_cancelled_requests_callback_may_submit_its_next_request() {
        let dir = tempfile::tempdir().unwrap();
        let io = Arc::new(UringIO::new().unwrap());
        let file = file_of(&dir, &io, 512);
        let (_, pipe) = fifo_of(&dir, &io);
        let (tx, rx) = mpsc::channel();
        let c = Completion::new_read(Arc::new(crate::Buffer::new_temporary(1)), move |_| {
            drop(file.pread(0, read_telling(&tx)).unwrap());
            None
        });
        let c = pipe.pread(0, c).unwrap();
        let canceller = {
            let (io, c) = (io.clone(), c.clone());
            std::thread::spawn(move || io.cancel(&[c]))
        };
        assert_eq!(
            rx.recv_timeout(BOUND),
            Ok(512),
            "the cancelled read's callback could not submit its next read"
        );
        canceller.join().unwrap().unwrap();
        // The kernel's cancel ends the read, and its entry retires it.
        io.drain_completions(std::slice::from_ref(&c)).unwrap();
    }

    /// A partial write's progress wake runs with the ring's mutex let go: a waker that submits a
    /// request, as an executor's may, gets the mutex (DESIGN.md 22.5; mutant: the wake made under
    /// the mutex, where that submission waits for ever on the mutex the delivery thread holds).
    /// The write is two pages to a FIFO with room for one, so the kernel ends its first part short
    /// and the ring resubmits the rest, which stays the kernel's until the test reads.
    #[test]
    fn a_partial_writes_progress_wake_may_submit() {
        use std::io::Read as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let io = Arc::new(UringIO::new().unwrap());
        let file = file_of(&dir, &io, 512);
        let (fifo, pipe) = fifo_of(&dir, &io);
        let nonblocking = |write: bool| {
            std::fs::OpenOptions::new()
                .read(!write)
                .write(write)
                .custom_flags(libc::O_NONBLOCK)
                .open(&fifo)
                .unwrap()
        };
        let mut reader = nonblocking(false);
        // Filled to its last page, then one page read back: room for one page.
        let mut filler = nonblocking(true);
        let page = [0u8; 4096];
        while filler.write(&page).is_ok() {}
        let mut back = [0u8; 4096];
        assert_eq!(reader.read(&mut back).unwrap(), page.len());

        struct SubmitOnWake {
            file: Arc<dyn File>,
            tx: std::sync::Mutex<Option<mpsc::Sender<i32>>>,
        }
        impl std::task::Wake for SubmitOnWake {
            fn wake(self: Arc<Self>) {
                if let Some(tx) = self.tx.lock().unwrap().take() {
                    drop(self.file.pread(0, read_telling(&tx)).unwrap());
                }
            }
        }
        let (tx, rx) = mpsc::channel();
        let w = Completion::new_write(|_| {});
        w.set_waker(&std::task::Waker::from(Arc::new(SubmitOnWake {
            file,
            tx: std::sync::Mutex::new(Some(tx)),
        })));
        let parts = (0..2)
            .map(|_| Arc::new(crate::Buffer::new_temporary(page.len())))
            .collect();
        let w = pipe.pwritev(0, parts, w).unwrap();
        assert_eq!(
            rx.recv_timeout(BOUND),
            Ok(512),
            "the progress wake's submission never ran"
        );
        assert!(!w.finished(), "the write ended whole: no part was short");
        // Read the FIFO out until the rest is written.
        let since = std::time::Instant::now();
        while !w.finished() {
            assert!(
                since.elapsed() < BOUND,
                "the write's rest was never written"
            );
            let _ = reader.read(&mut back);
        }
        assert!(w.succeeded());
    }

    /// A pass visits at most a completion queue's worth of entries, whatever each is: a cancel's
    /// own, the eventfd's, a partial write's part (DESIGN.md 22.5; mutant: only the entries that
    /// end a request counted, so a queue the pass keeps refilling, as a part's resubmission or a
    /// rearmed poll the kernel completes at once does, holds the mutex for as long as it is
    /// refilled). The ring here has no delivery thread; its entries come from a source that keeps
    /// coming, and what the pass submits stays queued.
    #[test]
    fn a_pass_visits_at_most_a_completion_queues_worth_of_entries() {
        let shared = Shared::new(4).unwrap();
        assert_eq!(shared.pass, shared.ring.params().cq_entries() as usize);
        // Never delivered here: the write's state is all the pass reads of it.
        let key = 0x1000;
        let part = Arc::new(crate::Buffer::new_temporary(4096));
        let mut state = shared.state.lock();
        state.writev_states.insert(
            key,
            WritevState {
                file_id: io_uring::types::Fd(-1),
                file_pos: 0,
                current_buffer_idx: 0,
                current_buffer_offset: 0,
                total_written: 0,
                total_len: part.len(),
                bufs: vec![part],
                last_iov_allocation: None,
            },
        );
        ENTER_HELD.with(|h| h.set(true));
        let kinds = [CANCEL_TAG, EVENTFD_TAG, key];
        let mut visited = 0;
        let mut batch = Vec::new();
        shared.take(&mut state, &mut batch, || {
            let entry = (visited < 100 * shared.pass).then(|| (kinds[visited % kinds.len()], 1));
            visited += 1;
            entry
        });
        ENTER_HELD.with(|h| h.set(false));
        assert_eq!(
            visited, shared.pass,
            "a pass visited {visited} entries, a queue holding {}",
            shared.pass
        );
        assert_eq!(
            batch.len(),
            shared.pass / kinds.len(),
            "the parts visited were not each a progress wake"
        );
    }

    /// Every method of the ring's IO and files, called on a thread marked as the delivery thread,
    /// is refused or allowed as `delivery_matrix.rs` rules: a refused one ends in the refusal, an
    /// allowed one returns (DESIGN.md 22.5; mutant: a refused method's refusal removed). Each
    /// call that could wait is made where it would not: on a finished request, for no time.
    #[test]
    fn each_method_on_the_delivery_thread_is_refused_or_allowed_as_ruled() {
        use crate::io::delivery_matrix::{Rule, FILE, IO as IO_METHODS};
        let dir = Arc::new(tempfile::tempdir().unwrap());
        let io = Arc::new(UringIO::new().unwrap());
        let file = file_of(&dir, &io, 4096);
        let opcode_truncates = io.shared.caps.ftruncate;
        let mut wrong = Vec::new();
        for (kind, ruled) in [("File", FILE), ("IO", IO_METHODS)] {
            for &(method, rule) in ruled {
                let (io, file, dir) = (io.clone(), file.clone(), dir.clone());
                let outcome = std::thread::spawn(move || {
                    crate::io::DELIVERING.with(|d| d.set(true));
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        call(method, &io, &file, &dir)
                    }))
                    .map_err(|p| {
                        p.downcast_ref::<String>()
                            .cloned()
                            .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                            .unwrap_or_default()
                    })
                })
                .join()
                .unwrap();
                let refused = match rule {
                    Rule::Refused => true,
                    Rule::Allowed(_) => false,
                    Rule::SubmittedOrRefused => !opcode_truncates,
                };
                let as_ruled = match &outcome {
                    Ok(()) => !refused,
                    Err(said) => refused && said.contains("on the ring's delivery thread"),
                };
                if !as_ruled {
                    wrong.push(format!("{kind}::{method} ({rule:?}): {outcome:?}"));
                }
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// Calls `method` on the ring's IO or `file`, as cheaply as it can be called.
    fn call(method: &str, io: &Arc<UringIO>, file: &Arc<dyn File>, dir: &tempfile::TempDir) {
        let path = |name: &str| dir.path().join(name).to_str().unwrap().to_string();
        let page = || Arc::new(crate::Buffer::new_temporary(4096));
        let write = || Completion::new_write(|_| {});
        // A request finished and never submitted: a wait on it, were it not refused, returns.
        let finished = || {
            let c = Completion::new_write(|_| {});
            c.complete(0);
            c
        };
        let kind = SharedWalLockKind::ProcessScopedFcntl;
        // The byte past the file's end each shared WAL call takes: a lock beyond the end is legal.
        let byte = 1 << 20;
        match method {
            "lock_file" => {
                let _ = file.lock_file(true);
            }
            "unlock_file" => {
                let _ = file.unlock_file();
            }
            "pread" => {
                let _ = file.pread(0, Completion::new_read(page(), |_| None));
            }
            "pwrite" => {
                let _ = file.pwrite(0, page(), write());
            }
            "sync" => {
                let _ = file.sync(Completion::new_sync(|_| {}), crate::io::FileSyncType::Fsync);
            }
            "sync_data" => {
                let _ = file.sync_data(Completion::new_sync(|_| {}));
            }
            "supports_durable_writes" => {
                let _ = file.supports_durable_writes();
            }
            "pwrite_durable" => {
                let _ = file.pwrite_durable(0, page(), write());
            }
            "allocate" => {
                let _ = file.allocate(0, 4096);
            }
            "pwritev" => {
                let _ = file.pwritev(0, vec![page()], write());
            }
            "size" => {
                let _ = file.size();
            }
            "truncate" => {
                let _ = file.truncate(4096, Completion::new_trunc(|_| {}));
            }
            "has_hole" => {
                let _ = file.has_hole(0, 4096);
            }
            "punch_hole" => {
                let _ = file.punch_hole(0, 4096);
            }
            "shared_wal_lock_byte" => {
                let _ = file.shared_wal_lock_byte(byte, false, kind);
            }
            "shared_wal_try_lock_byte" => {
                let _ = file.shared_wal_try_lock_byte(byte, false, kind);
            }
            "shared_wal_probe_exclusive_byte" => {
                let _ = file.shared_wal_probe_exclusive_byte(byte, kind);
            }
            "shared_wal_probe_exclusive_while_shared_byte" => {
                let _ = file.shared_wal_probe_exclusive_while_shared_byte(byte, kind);
            }
            "shared_wal_unlock_byte" => {
                let _ = file.shared_wal_unlock_byte(byte, kind);
            }
            "shared_wal_set_len" => {
                let _ = file.shared_wal_set_len(4096);
            }
            "shared_wal_map" => {
                let _ = file.shared_wal_map(0, 4096);
            }
            "open_file" => {
                let _ = io.open_file(&path("opened"), OpenFlags::Create, false);
            }
            "open_shared_wal_file" => {
                let _ = io.open_shared_wal_file(&path("shared"));
            }
            "remove_file" => {
                let _ = io.remove_file(&path("absent"));
            }
            "sync_directory" => {
                let _ = io.sync_directory(&path(""));
            }
            "rename_file" => {
                let _ = io.rename_file(&path("absent"), &path("renamed"));
            }
            "supports_shared_wal_coordination" => {
                let _ = io.supports_shared_wal_coordination();
            }
            "step" => {
                let _ = io.step();
            }
            "delivery" => {
                let _ = io.delivery();
            }
            "wait" => io.wait(&finished()),
            "cancel" => {
                let _ = io.cancel(&[finished()]);
            }
            "wait_retired" => {
                let _ = io.wait_retired(&finished());
            }
            "drain_completions" => {
                let _ = io.drain_completions(&[finished()]);
            }
            "wait_for_completion" => {
                let _ = io.wait_for_completion(finished());
            }
            "generate_random_number" => {
                let _ = io.generate_random_number();
            }
            "fill_bytes" => io.fill_bytes(&mut [0u8; 8]),
            "get_memory_io" => {
                let _ = io.get_memory_io();
            }
            "register_fixed_buffer" => {
                // Kept for the ring's life: the kernel holds the buffer once registered.
                let arena: &'static mut [u8] = Box::leak(vec![0u8; 4096].into_boxed_slice());
                let _ = io.register_fixed_buffer(NonNull::new(arena.as_mut_ptr()).unwrap(), 4096);
            }
            "yield_now" => io.yield_now(),
            "sleep" => io.sleep(Duration::ZERO),
            "file_id" => {
                let _ = io.file_id(&path("absent"));
            }
            _ => panic!("no call for {method}"),
        }
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
