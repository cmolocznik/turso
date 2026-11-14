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
    mem::ManuallyDrop,
    ops::Deref,
    os::{
        fd::{AsFd, FromRawFd, OwnedFd, RawFd},
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

/// The fatal site of a file's last drop on the delivery thread with requests outstanding (RING-FD1
/// §2): that thread would wait for entries only it can reap.
const REFUSED_DROP_ON_DELIVERY: &str =
    "io_uring: a file's last drop on the delivery thread, requests unretired";

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
    /// Where a file's last drop on this ring says it parked, waiting for its requests, for a test.
    #[cfg(test)]
    closer_notes: Mutex<Option<std::sync::mpsc::Sender<&'static str>>>,
    /// A gate the delivery thread waits at before its next pass, outside the ring's lock, for a
    /// test: so the test's offer finds the lock free.
    #[cfg(test)]
    pass_gate: Mutex<Option<tests::Gate>>,
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
    /// The open files, one slot each: the requests each accepted and has not yet reaped the last
    /// entry of, and the thread its last handle's drop waits on (RING-FD1 §1).
    files: FileSlots,
    /// The ring's eventfd, which a refusal to enter that may pass writes, so that the delivery
    /// thread's wait ends and its next one enters what is queued (RING-FD1 §2). Owned by
    /// [`Shared`], which outlives this state.
    wake: RawFd,
    /// Entering held, for a test (RING-FD1 §7): no `enter` enters anything, and the delivery
    /// thread's wait enters nothing. Set only before the delivery thread starts.
    #[cfg(test)]
    entry_held: bool,
    /// The overflow held where it is, for a test (ASYNC1 D2d §4): no flush moves it into the
    /// submission queue, so a queue the kernel has emptied stays empty beside it.
    #[cfg(test)]
    flush_held: bool,
    /// Where `take` tells each request entry it reaps, for a test.
    #[cfg(test)]
    take_notes: Option<std::sync::mpsc::Sender<tests::TakeNote>>,
}

/// The ring's open files, one slot each (RING-FD1 §1). A slot is taken at the file's open and freed
/// by its last handle's drop once no request the file accepted remains unreaped, so no accepted
/// request ever names a slot freed or reused.
#[derive(Default)]
struct FileSlots {
    slots: Vec<FileUsers>,
    free: Vec<usize>,
}

#[derive(Default)]
struct FileUsers {
    /// Requests accepted on the file whose last entry is not yet reaped.
    requests: usize,
    /// The thread the file's last handle's drop parked, waiting for `requests` to reach zero.
    closer: Option<std::thread::Thread>,
    /// Whether a file holds the slot.
    open: bool,
}

/// A file's slot among the ring's, made only by [`FileSlots::take`]. A request carries it in its
/// completion's backend word as `index + 1`, so zero is none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileSlot(usize);

impl FileSlot {
    fn word(self) -> usize {
        self.0 + 1
    }

    fn of_word(word: usize) -> Self {
        Self(word - 1)
    }
}

impl FileSlots {
    fn take(&mut self) -> FileSlot {
        let at = match self.free.pop() {
            Some(at) => at,
            None => {
                self.slots.push(FileUsers::default());
                self.slots.len() - 1
            }
        };
        self.slots[at].open = true;
        FileSlot(at)
    }

    fn count(&mut self, slot: FileSlot) {
        self.slots[slot.0].requests += 1;
    }

    /// One request of `slot`'s file reaped at its last entry: its closer, if the file has no
    /// request left and one waits.
    fn uncount(&mut self, slot: FileSlot) -> Option<std::thread::Thread> {
        let users = &mut self.slots[slot.0];
        turso_assert!(users.requests > 0, "a ring file's request reaped twice");
        users.requests -= 1;
        if users.requests == 0 {
            users.closer.take()
        } else {
            None
        }
    }

    fn requests(&self, slot: FileSlot) -> usize {
        self.slots[slot.0].requests
    }

    fn wait_as_closer(&mut self, slot: FileSlot, closer: std::thread::Thread) {
        self.slots[slot.0].closer = Some(closer);
    }

    fn free(&mut self, slot: FileSlot) {
        let users = &mut self.slots[slot.0];
        turso_assert!(
            users.requests == 0 && users.open,
            "a ring file's slot freed while its requests remain"
        );
        users.open = false;
        users.closer = None;
        self.free.push(slot.0);
    }
}

impl RingState {
    /// SAFETY: caller must guarantee no other `SubmissionQueue` exists for
    /// `ring`. Holding the `RingState` Mutex satisfies that invariant.
    unsafe fn flush_overflow(&mut self, ring: &io_uring::IoUring) {
        #[cfg(test)]
        if self.flush_held {
            return;
        }
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

    /// Whether an entry submitted now goes straight into the submission queue (ASYNC1 D2d §4): no
    /// overflow waits before it, and the queue has room. Under the mutex, which every pusher holds,
    /// so the answer holds until the caller lets it go.
    fn takes_an_entry_now(&self, ring: &io_uring::IoUring) -> bool {
        // SAFETY: the caller holds the `RingState` Mutex, so no other `SubmissionQueue` exists.
        self.overflow.is_empty() && !unsafe { ring.submission_shared() }.is_full()
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
        self.enter(ring);
    }

    /// Queues `entry`, a request of the file in `slot` whose completion is `c`, as
    /// [`submit_entry`](Self::submit_entry) does, counted on its file and its completion marked with
    /// the slot first: so the file's descriptor stays its own until the delivery thread reaps the
    /// request's last entry (RING-FD1 §1). SAFETY: same contract as `submit_entry`.
    unsafe fn submit_request(
        &mut self,
        ring: &io_uring::IoUring,
        entry: &io_uring::squeue::Entry,
        slot: FileSlot,
        c: &Completion,
    ) {
        self.mark_request(slot, c);
        self.submit_entry(ring, entry);
    }

    /// Queues the ring offer's `entry`, a request of the file in `slot` whose completion is `c`,
    /// straight into the submission queue and enters it once (ASYNC1 D2d §4): counted on its file
    /// and marked first, as [`submit_request`](Self::submit_request) does, then one push and one
    /// enter, no overflow touched. SAFETY: the caller holds the `RingState` Mutex and found under
    /// it that the queue takes an entry now ([`takes_an_entry_now`](Self::takes_an_entry_now)), so
    /// the push cannot be refused.
    unsafe fn submit_offered(
        &mut self,
        ring: &io_uring::IoUring,
        entry: &io_uring::squeue::Entry,
        slot: FileSlot,
        c: &Completion,
    ) {
        self.mark_request(slot, c);
        let pushed = {
            let mut sq = ring.submission_shared();
            sq.push(entry).is_ok()
        };
        turso_assert!(
            pushed,
            "an offered entry refused by a queue found to have room"
        );
        self.enter(ring);
    }

    /// Counts a request of the file in `slot` and marks its completion with the slot, before the
    /// request's first entry is queued (RING-FD1 §1).
    fn mark_request(&mut self, slot: FileSlot, c: &Completion) {
        self.files.count(slot);
        c.get_inner().set_backend_word(slot.word());
    }

    /// Tells a test each request entry `take` reaps: its key, whether it was the request's last, its
    /// file's count after `take`'s decision, and the raw result.
    #[cfg(test)]
    fn note_take(&self, key: u64, last: bool, result: i32) {
        if let Some(notes) = &self.take_notes {
            // SAFETY: the kernel's reference to the completion is still held, as in `reaped_last`.
            let inner = unsafe { &*(key as *const CompletionInner) };
            let word = inner.backend_word();
            let count = if word == 0 {
                0
            } else {
                self.files.requests(FileSlot::of_word(word))
            };
            let _ = notes.send(tests::TakeNote {
                key,
                last,
                count,
                result,
            });
        }
    }

    /// Whether a test holds entering (RING-FD1 §7); never outside a test build.
    fn entry_held(&self) -> bool {
        #[cfg(test)]
        {
            self.entry_held
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    /// Enters what is queued, as [`enter`] does, unless a test holds entering.
    fn enter(&self, ring: &io_uring::IoUring) {
        #[cfg(test)]
        if self.entry_held {
            return;
        }
        enter(ring, self.wake);
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
        self.enter(ring);
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
/// completion queue) leaves them queued for the next entry, which the delivery thread's next wait
/// makes. So that wait comes even with nothing else in flight, such a refusal writes `wake`, the
/// eventfd the delivery thread's standing poll watches (RING-FD1 §2). Any other refusal means the
/// ring can no longer deliver or retire what it accepted, and ends the process (DESIGN.md 22.11):
/// returning would leave every waiter on those requests waiting.
fn enter(ring: &io_uring::IoUring, wake: RawFd) {
    #[cfg(test)]
    if tests::ENTER_HELD.with(|h| h.get()) {
        return;
    }
    #[cfg(test)]
    let refused = tests::ENTER_REFUSED.with(|r| r.take());
    #[cfg(not(test))]
    let refused = None;
    let entered = match refused {
        Some(errno) => Err(std::io::Error::from_raw_os_error(errno)),
        None => ring.submit().map(|_| ()),
    };
    if let Err(e) = entered {
        match e.raw_os_error() {
            Some(libc::EINTR | libc::EAGAIN | libc::EBUSY) => {
                let one = 1u64.to_ne_bytes();
                // SAFETY: an 8-byte write from a live buffer to the eventfd the ring holds open. A
                // write that fails leaves the counter as it was: it cannot, short of the counter
                // reaching its maximum, which one write per refusal never does before a read.
                unsafe { libc::write(wake, one.as_ptr().cast(), one.len()) };
                #[cfg(test)]
                tests::REFUSALS_RUNG.fetch_add(1, Ordering::Relaxed);
            }
            _ => ring_lost("io_uring: io_uring_enter failed"),
        }
    }
}

/// Ends the process: the ring cannot deliver or retire the requests it accepted. `during` names
/// the call that failed.
fn ring_lost(during: &'static str) -> ! {
    super::fatal(during)
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
        Self::starting(entries, false)
    }

    /// A ring of `entries` whose entering is held from before its delivery thread starts, for a
    /// test (RING-FD1 §7): its eventfd's standing poll is entered first, by [`Shared::new`], so
    /// the delivery thread's held wait ends at a write to it.
    #[cfg(test)]
    pub(super) fn held_for_test(entries: u32) -> Result<Self> {
        Self::starting(entries, true)
    }

    /// (The requests the ring has accepted and not yet delivered, the requests its files count and
    /// have not yet reaped), for a test (ASYNC1 D2d §4).
    #[cfg(test)]
    pub(crate) fn counts_for_test(&self) -> (usize, usize) {
        let accepted = self.shared.requests.load(Ordering::SeqCst);
        let counted = self
            .shared
            .state
            .lock()
            .files
            .slots
            .iter()
            .map(|f| f.requests)
            .sum();
        (accepted, counted)
    }

    /// Runs `f` with the ring's lock held, for a test: a submission waits for it meanwhile, and an
    /// offer is declined (ASYNC1 D2d §4).
    #[cfg(test)]
    pub(crate) fn with_lock_held<T>(&self, f: impl FnOnce() -> T) -> T {
        let _held = self.shared.state.lock();
        f()
    }

    /// A ring of `entries`, and its delivery thread started; with entering held from the first if
    /// `held`, which only a test asks for.
    fn starting(entries: u32, held: bool) -> Result<Self> {
        let shared = Shared::new(entries)?;
        #[cfg(test)]
        {
            shared.state.lock().entry_held = held;
        }
        #[cfg(not(test))]
        let _ = held;
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
        #[cfg(test)]
        let caps = UringCapabilities {
            ftruncate: caps.ftruncate && !tests::NO_FTRUNCATE_OPCODE.with(|n| n.get()),
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
        let wake = eventfd.as_raw_fd();
        let shared = Arc::new(Shared {
            ring,
            state: Mutex::new(RingState {
                overflow: VecDeque::new(),
                writev_states: HashMap::default(),
                iov_pool: IovecPool::new(),
                free_arenas: [const { None }; ARENA_COUNT],
                files: FileSlots::default(),
                wake,
                #[cfg(test)]
                entry_held: false,
                #[cfg(test)]
                flush_held: false,
                #[cfg(test)]
                take_notes: None,
            }),
            caps,
            handles: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            eventfd,
            pass,
            #[cfg(test)]
            injected_wait: std::sync::atomic::AtomicI32::new(0),
            #[cfg(test)]
            closer_notes: Mutex::new(None),
            #[cfg(test)]
            pass_gate: Mutex::new(tests::NEXT_GATE.with(|g| g.take())),
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
    /// A file's last request reaped while its last handle's drop waits for it: that thread, to
    /// unpark (RING-FD1 §1).
    Closer(std::thread::Thread),
}

impl IO for UringIO {
    fn supports_shared_wal_coordination(&self) -> bool {
        true
    }

    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        super::refuse_on_delivery_thread("a file's open");
        let file = common::open_unix(path, flags)?;
        let slot = self.shared.state.lock().files.take();
        let uring_file = Arc::new(UringFile {
            shared: Handle::new(&self.shared.0),
            file: ManuallyDrop::new(file),
            slot,
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
    super::deliver_on_this_thread();
    let _frame = DeliveryFrame;
    let mut batch = Vec::with_capacity(shared.pass);
    loop {
        #[cfg(test)]
        shared.wait_at_gate();
        let held = {
            let mut state = shared.state.lock();
            // SAFETY: holding `state` Mutex.
            unsafe { state.flush_overflow(&shared.ring) };
            state.entry_held()
        };
        if shared.handles.load(Ordering::Acquire) == 0
            && shared.requests.load(Ordering::Acquire) == 0
        {
            return;
        }
        let waited = shared.wait_for_one(held);
        if let Err(e) = waited {
            match e.raw_os_error() {
                // A signal cut the wait short.
                Some(libc::EINTR) => continue,
                // A full completion queue, or no room to enter what is queued: what is complete
                // is taken below, and the wait is tried again.
                Some(libc::EBUSY | libc::EAGAIN) => {}
                _ => ring_lost("io_uring: the delivery thread's wait failed"),
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
                Taken::Closer(closer) => closer.unpark(),
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
    /// Waits at the gate a test put before this pass, if any, holding no lock: it says it is there,
    /// waits to be let through, at most [`tests::GATE_BOUND`], and says whether the test opened
    /// the gate or the bound ran out.
    #[cfg(test)]
    fn wait_at_gate(&self) {
        let gate = self.pass_gate.lock().take();
        if let Some(gate) = gate {
            let _ = gate.at.send(());
            let opened = gate.open.recv_timeout(tests::GATE_BOUND).is_ok();
            let _ = gate.passed.send(opened);
        }
    }

    /// The delivery thread's wait in the kernel: what is queued entered, and one completion waited
    /// for; with entering `held` for a test (RING-FD1 §7), only the completion, nothing entered.
    fn wait_for_one(&self, held: bool) -> std::io::Result<usize> {
        #[cfg(test)]
        {
            match self.injected_wait.swap(0, Ordering::AcqRel) {
                // SAFETY: nothing is entered, so no queue object is in use; this thread is the
                // ring's only waiter in the kernel.
                0 if held => unsafe {
                    self.ring.submitter().enter::<libc::sigset_t>(
                        0,
                        1,
                        tests::IORING_ENTER_GETEVENTS,
                        None,
                    )
                },
                0 => self.ring.submit_and_wait(1),
                errno => Err(std::io::Error::from_raw_os_error(errno)),
            }
        }
        #[cfg(not(test))]
        {
            let _ = held;
            self.ring.submit_and_wait(1)
        }
    }

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
                let taken = unsafe {
                    state.handle_writev_completion(&self.ring, wstate, user_data, result)
                };
                // A part whose rest was resubmitted keeps its file: the rest names its descriptor.
                let last = matches!(taken, Taken::Ended(..));
                if last {
                    Self::reaped_last(state, user_data, batch);
                }
                #[cfg(test)]
                state.note_take(user_data, last, result);
                batch.push(taken);
                continue;
            }
            Self::reaped_last(state, user_data, batch);
            #[cfg(test)]
            state.note_take(user_data, true, result);
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

impl Shared {
    /// The request `user_data` names had its last entry reaped: its file's count let go, and if
    /// that was the file's last request and its last handle's drop waits, that thread is unparked
    /// once the mutex is let go (RING-FD1 §1).
    fn reaped_last(state: &mut RingState, user_data: u64, batch: &mut Vec<Taken>) {
        // SAFETY: the kernel's reference to the completion is still held: its delivery, the only
        // thing that lets it go, comes later in this batch.
        let inner = unsafe { &*(user_data as *const CompletionInner) };
        let word = inner.backend_word();
        turso_assert!(word != 0, "a ring request reaped with no file slot");
        if let Some(closer) = state.files.uncount(FileSlot::of_word(word)) {
            batch.push(Taken::Closer(closer));
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
    /// Closed by the drop itself, once no request it accepted remains (RING-FD1 §2).
    file: ManuallyDrop<std::fs::File>,
    /// Its slot among the ring's files, which counts its requests.
    slot: FileSlot,
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
        let read_e = self.read_entry(pos, &c, |idx, ptr, len| {
            #[cfg(debug_assertions)]
            self.shared.state.lock().debug_check_fixed(idx, ptr, len);
            #[cfg(not(debug_assertions))]
            let _ = (idx, ptr, len);
        });
        self.shared.accept(&c);
        let mut state = self.shared.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_request(&self.shared.ring, &read_e, self.slot, &c) };
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let write = self.write_entry(pos, &buffer, &c, |idx, ptr, len| {
            #[cfg(debug_assertions)]
            self.shared.state.lock().debug_check_fixed(idx, ptr, len);
            #[cfg(not(debug_assertions))]
            let _ = (idx, ptr, len);
        });

        // Keep the buffer alive until the completion is processed. For non-fixed
        // buffers the SQE holds a raw pointer; without this the Arc would drop
        // here and the kernel could read freed memory.
        c.keep_write_buffer_alive(buffer);
        self.shared.accept(&c);
        let mut state = self.shared.state.lock();
        // SAFETY: holding `state` Mutex.
        unsafe { state.submit_request(&self.shared.ring, &write, self.slot, &c) };
        Ok(c)
    }

    /// The ring offer (ASYNC1 D2d §4): the read goes in on the calling thread only if the ring's
    /// lock is free and its submission queue takes the entry at once, with no overflow before it;
    /// otherwise nothing is done, no key made, nothing accepted or counted.
    fn try_pread_now(&self, pos: u64, c: &Completion) -> bool {
        let Some(mut state) = self.shared.state.try_lock() else {
            return false;
        };
        if !state.takes_an_entry_now(&self.shared.ring) {
            return false;
        }
        let entry = self.read_entry(pos, c, |idx, ptr, len| {
            #[cfg(debug_assertions)]
            state.debug_check_fixed(idx, ptr, len);
            #[cfg(not(debug_assertions))]
            let _ = (idx, ptr, len);
        });
        self.shared.accept(c);
        // SAFETY: holding `state` Mutex, under which the queue was found to take the entry.
        unsafe { state.submit_offered(&self.shared.ring, &entry, self.slot, c) };
        true
    }

    /// [`try_pread_now`](Self::try_pread_now) for a write: the ring keeps `buffer` alive until the
    /// write's delivery, as `pwrite` does.
    fn try_pwrite_now(&self, pos: u64, buffer: &Arc<crate::Buffer>, c: &Completion) -> bool {
        let Some(mut state) = self.shared.state.try_lock() else {
            return false;
        };
        if !state.takes_an_entry_now(&self.shared.ring) {
            return false;
        }
        let entry = self.write_entry(pos, buffer, c, |idx, ptr, len| {
            #[cfg(debug_assertions)]
            state.debug_check_fixed(idx, ptr, len);
            #[cfg(not(debug_assertions))]
            let _ = (idx, ptr, len);
        });
        c.keep_write_buffer_alive(buffer.clone());
        self.shared.accept(c);
        // SAFETY: as in `try_pread_now`.
        unsafe { state.submit_offered(&self.shared.ring, &entry, self.slot, c) };
        true
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
        unsafe { state.submit_request(&self.shared.ring, &sync, self.slot, &c) };
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
        unsafe { state.submit_request(&self.shared.ring, &sync, self.slot, &c) };
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
        unsafe { state.submit_request(&self.shared.ring, &write, self.slot, &c) };
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
        // Counted once, for the first part: its continuations name the same descriptor, which the
        // count keeps its file's until the last part's entry is reaped.
        state.mark_request(self.slot, &c);
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
            &*self.file,
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
            unsafe { state.submit_request(&self.shared.ring, &truncate, self.slot, &c) };
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
        // A lock that waits for the byte's other holder.
        super::refuse_on_any_delivering_thread("a shared WAL byte's lock");
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

impl UringFile {
    /// The entry of a read of `c`'s buffer at `pos`, its key made from `c`; `check_fixed` is told a
    /// fixed buffer's index, address and length first.
    fn read_entry(
        &self,
        pos: u64,
        c: &Completion,
        check_fixed: impl FnOnce(u32, *const u8, usize),
    ) -> io_uring::squeue::Entry {
        let r = c.as_read();
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
            check_fixed(idx, ptr, len);
            io_uring::opcode::ReadFixed::new(fd, ptr, len as u32, idx as u16)
                .offset(pos)
                .build()
                .user_data(get_key(c.clone()))
        } else {
            trace!("pread(pos = {}, length = {})", pos, len);
            io_uring::opcode::Read::new(fd, ptr, len as u32)
                .offset(pos)
                .build()
                .user_data(get_key(c.clone()))
        }
    }

    /// The entry of a write of `buffer` at `pos`, as [`read_entry`](Self::read_entry) makes a
    /// read's.
    fn write_entry(
        &self,
        pos: u64,
        buffer: &Arc<crate::Buffer>,
        c: &Completion,
        check_fixed: impl FnOnce(u32, *const u8, usize),
    ) -> io_uring::squeue::Entry {
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
            check_fixed(idx, ptr, len);
            io_uring::opcode::WriteFixed::new(fd, ptr, len as u32, idx as u16)
                .offset(pos)
                .build()
                .user_data(get_key(c.clone()))
        } else {
            trace!("pwrite(pos = {}, length = {})", pos, len);
            io_uring::opcode::Write::new(fd, ptr, len as u32)
                .offset(pos)
                .build()
                .user_data(get_key(c.clone()))
        }
    }
}

impl Drop for UringFile {
    /// Closes the descriptor only once no request this file accepted remains unreaped (RING-FD1 §2):
    /// waits for that, on this thread, then unlocks and closes before the file's handle on the ring
    /// goes, so whatever follows the drop follows the close. Each pass reads the count under the
    /// ring's mutex and parks only while it is not zero, registered for the unpark of the delivery
    /// thread that reaps the last. On that delivery thread, which would wait for entries only it can
    /// reap, a drop with requests outstanding ends the process.
    fn drop(&mut self) {
        loop {
            let mut state = self.shared.state.lock();
            if state.files.requests(self.slot) == 0 {
                state.files.free(self.slot);
                break;
            }
            if super::delivering() {
                #[cfg(test)]
                tests::say_refused(REFUSED_DROP_ON_DELIVERY);
                super::fatal(REFUSED_DROP_ON_DELIVERY);
            }
            state
                .files
                .wait_as_closer(self.slot, std::thread::current());
            drop(state);
            #[cfg(test)]
            if let Some(notes) = self.shared.closer_notes.lock().as_ref() {
                let _ = notes.send("parked");
            }
            std::thread::park();
        }
        self.unlock_file().expect("Failed to unlock file");
        // SAFETY: dropped once, here, and nothing reads `file` after.
        unsafe { ManuallyDrop::drop(&mut self.file) };
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

    /// How long a delivery thread waits at a test's gate before it goes on regardless.
    pub(super) const GATE_BOUND: Duration = BOUND;

    /// The gate a ring's delivery thread waits at before a pass, outside the ring's lock.
    pub(super) struct Gate {
        pub(super) at: mpsc::Sender<()>,
        pub(super) open: mpsc::Receiver<()>,
        /// Whether the thread went through because the test opened the gate, not because its
        /// bound ran out.
        pub(super) passed: mpsc::Sender<bool>,
    }

    /// A test's side of a gate.
    struct GateHandle {
        at: mpsc::Receiver<()>,
        open: mpsc::Sender<()>,
        passed: mpsc::Receiver<bool>,
    }

    impl GateHandle {
        /// Whether the delivery thread came to the gate. If it did, it waits there, outside the
        /// ring's lock, until the gate opens.
        fn reached(&self) -> bool {
            self.at.recv_timeout(BOUND).is_ok()
        }

        fn open(&self) {
            let _ = self.open.send(());
        }

        /// Whether the delivery thread went through the gate because it was opened, and not
        /// because its bound ran out.
        fn opened(&self) -> bool {
            self.passed.recv_timeout(BOUND) == Ok(true)
        }
    }

    fn gate() -> (Gate, GateHandle) {
        let (at, at_rx) = mpsc::channel();
        let (open_tx, open) = mpsc::channel();
        let (passed, passed_rx) = mpsc::channel();
        (
            Gate { at, open, passed },
            GateHandle {
                at: at_rx,
                open: open_tx,
                passed: passed_rx,
            },
        )
    }

    std::thread_local! {
        /// The gate the next ring made on this thread puts before its delivery thread's first pass.
        pub(super) static NEXT_GATE: Cell<Option<Gate>> = const { Cell::new(None) };
    }

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
        let (c, fifo, _pipe) = held_read(&dir, &*io);
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
    /// The pipe's file comes back too: its last drop waits for the read's last entry (RING-FD1), so
    /// a caller keeps it until it has written to the pipe.
    fn held_read(
        dir: &tempfile::TempDir,
        io: &dyn IO,
    ) -> (Completion, std::path::PathBuf, Arc<dyn File>) {
        let fifo = dir.path().join("fifo");
        let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        // SAFETY: a plain mkfifo(3) on a fresh path.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        let pipe = io
            .open_file(fifo.to_str().unwrap(), OpenFlags::NoLock, false)
            .unwrap();
        let c = Completion::new_read(Arc::new(crate::Buffer::new_temporary(1)), |_| None);
        (pipe.pread(0, c).unwrap(), fifo, pipe)
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
        let (c, fifo, _pipe) = held_read(&dir, &*io);
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
        let (c, fifo, _pipe) = held_read(&dir, &*io);
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
            None,
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
    /// a read the dead ring will never deliver: nothing is written before the abort, and the
    /// process ends (mutant: a message printed through the lock, which waits on its holder for
    /// ever).
    #[test]
    fn an_error_of_the_ring_ends_the_process_while_another_thread_holds_stderr() {
        ends_the_process(
            "io::io_uring::tests::an_error_of_the_ring_ends_the_process_while_another_thread_holds_stderr",
            None,
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

    /// The same with standard error a full pipe whose reader has stopped reading: nothing is
    /// written before the abort, and the process ends all the same (DESIGN.md 22.11; mutant: a
    /// message written to standard error by a write that waits for room, which never comes).
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

    /// How a callback cancels on the delivery thread in [`a_cancel_from_inside_a_finish_returns`].
    enum Cancels {
        /// A sync's callback cancels the sync itself.
        Itself,
        /// A group's callback, run as its only request is delivered, cancels the group.
        ItsGroup,
    }

    /// A callback on the delivery thread cancels the completion it is finishing, which IO::cancel
    /// allows there (DESIGN.md 22.5): the cancel returns at once, the completion finishes with the
    /// delivered result, and the ring delivers the next request (the auditor's P1-CANCEL-1;
    /// mutant: the callback run inside the result's initialization, where the cancel's abort
    /// waits for ever on the initialization its own thread is running). A ring left stuck is
    /// leaked before the test fails, so the failure does not wait on its delivery thread.
    fn a_cancel_from_inside_a_finish_returns(how: Cancels) {
        let dir = tempfile::tempdir().unwrap();
        let io = Arc::new(UringIO::new().unwrap());
        let file = file_of(&dir, &io, 512);
        let (entered, at_entry) = mpsc::channel();
        let (returned, at_return) = mpsc::channel();
        let own: Arc<std::sync::OnceLock<std::sync::Weak<Completion>>> = Default::default();
        let callback = {
            let (own, io) = (own.clone(), Arc::downgrade(&io));
            move |_: std::result::Result<i32, crate::CompletionError>| {
                let (c, io) = (own.get().unwrap().upgrade().unwrap(), io.upgrade().unwrap());
                entered.send(()).unwrap();
                io.cancel(std::slice::from_ref(c.as_ref())).unwrap();
                returned.send(()).unwrap();
            }
        };
        // What finishes, and the request the ring delivers to finish it: submitted only once the
        // callback can find what it cancels.
        let (finishing, request) = match how {
            Cancels::Itself => {
                let sync = Completion::new_sync(callback);
                (Arc::new(sync.clone()), sync)
            }
            Cancels::ItsGroup => {
                let mut group = crate::io::CompletionGroup::new(callback);
                let request = Completion::new_sync(|_| {});
                group.add(&request);
                (Arc::new(group.build()), request)
            }
        };
        own.set(Arc::downgrade(&finishing)).unwrap();
        drop(file.sync(request, crate::io::FileSyncType::Fsync).unwrap());
        let entered = at_entry.recv_timeout(BOUND).is_ok();
        let (tx, rx) = mpsc::channel();
        let next = file
            .sync(
                Completion::new_sync(move |_| tx.send(()).unwrap()),
                crate::io::FileSyncType::Fsync,
            )
            .unwrap();
        let returned = at_return.recv_timeout(BOUND).is_ok();
        let delivered = rx.recv_timeout(BOUND).is_ok();
        let finished = finishing.finished();
        if !(entered && returned && delivered && finished) {
            // The delivery thread is stuck in the callback: dropping the ring would join it.
            std::mem::forget((io, file, next, finishing));
            panic!(
                "entered={entered} cancel_returned={returned} next_delivered={delivered} \
                 own_finished={finished}"
            );
        }
        assert_eq!(
            finishing.get_error(),
            None,
            "the delivered result was replaced"
        );
    }

    #[test]
    fn a_callback_cancelling_its_own_completion_returns_and_the_ring_goes_on() {
        a_cancel_from_inside_a_finish_returns(Cancels::Itself);
    }

    #[test]
    fn a_groups_callback_cancelling_the_group_as_its_request_is_delivered_returns() {
        a_cancel_from_inside_a_finish_returns(Cancels::ItsGroup);
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
        // The test keeps the file's own handle: the waker's, dropped on the delivery thread, is
        // then never the file's last while the read it submits is unreaped (RING-FD1 §2).
        w.set_waker(&std::task::Waker::from(Arc::new(SubmitOnWake {
            file: file.clone(),
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
        use crate::io::delivery_matrix::{FILE, IO as IO_METHODS};
        let wrong = held_to(
            crate::io::deliver_on_this_thread,
            [("File", FILE), ("IO", IO_METHODS)],
            "on the ring's delivery thread",
        );
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// Every method of the ring's IO and files, called on a thread marked as a database's IO
    /// thread, is refused or allowed as `delivery_matrix.rs` rules for it: the calls that wait on
    /// the file system alone return, and a wait on a request, on a lock's other holder or on time
    /// ends in the refusal (ASYNC1 D2a, the auditor's #4839; mutants: the IO thread marked as the
    /// ring's delivery thread; its refusal of a wait removed).
    #[test]
    fn each_method_on_a_databases_io_thread_is_refused_or_allowed_as_ruled() {
        use crate::io::delivery_matrix::{IO_THREAD_FILE, IO_THREAD_IO};
        let wrong = held_to(
            crate::io::serve_on_this_thread_for_life,
            [("File", IO_THREAD_FILE), ("IO", IO_THREAD_IO)],
            "on a database's IO thread",
        );
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// Calls each method `ruled` on its own thread, given its role by `mark`, and names each whose
    /// outcome is not as ruled: a refused one ends in a refusal saying `refused_on`, an allowed one
    /// returns.
    fn held_to(
        mark: fn(),
        ruled: [(
            &'static str,
            &'static [(&'static str, crate::io::delivery_matrix::Rule)],
        ); 2],
        refused_on: &str,
    ) -> Vec<String> {
        use crate::io::delivery_matrix::Rule;
        let dir = Arc::new(tempfile::tempdir().unwrap());
        let io = Arc::new(UringIO::new().unwrap());
        let file = file_of(&dir, &io, 4096);
        let opcode_truncates = io.shared.caps.ftruncate;
        let mut wrong = Vec::new();
        for (kind, ruled) in ruled {
            for &(method, rule) in ruled {
                let (io, file, dir) = (io.clone(), file.clone(), dir.clone());
                let outcome = std::thread::spawn(move || {
                    mark();
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
                    Err(said) => refused && said.contains(refused_on),
                };
                if !as_ruled {
                    wrong.push(format!("{kind}::{method} ({rule:?}): {outcome:?}"));
                }
            }
        }
        wrong
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
            "try_pread_now" => {
                let _ = file.try_pread_now(0, &Completion::new_read(page(), |_| None));
            }
            "try_pwrite_now" => {
                let _ = file.try_pwrite_now(0, &page(), &write());
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
            "try_step" => {
                let place = crate::io::ReadyPlace::default();
                let _ = io.try_step(place.key(), std::task::Waker::noop());
                io.withdraw(place.key());
            }
            "says_what_a_step_found" => {
                let _ = io.says_what_a_step_found();
            }
            "withdraw" => {
                let place = crate::io::ReadyPlace::default();
                io.withdraw(place.key());
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

    // RING-FD1: a ring file's descriptor stays its own until every request it accepted is reaped.

    crate::thread::thread_local! {
        /// Set by a test on its own thread: the error number its next `enter` reports instead of
        /// entering.
        pub(super) static ENTER_REFUSED: Cell<Option<i32>> = const { Cell::new(None) };
        /// Set by a test on its own thread: the rings it makes take the kernel's truncate opcode as
        /// unsupported, so a truncation takes the synchronous fallback.
        pub(super) static NO_FTRUNCATE_OPCODE: Cell<bool> = const { Cell::new(false) };
    }

    /// Refusals to enter that may pass and wrote the ring's eventfd, every ring's, for a test's
    /// difference across one call.
    pub(super) static REFUSALS_RUNG: AtomicUsize = AtomicUsize::new(0);

    /// `IORING_ENTER_GETEVENTS` of `linux/io_uring.h`: the enter waits for completions.
    pub(super) const IORING_ENTER_GETEVENTS: u32 = 1;

    /// What `take` tells a test of each request entry it reaps (RING-FD1 §7).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct TakeNote {
        pub(super) key: u64,
        pub(super) last: bool,
        pub(super) count: usize,
        pub(super) result: i32,
    }

    /// Writes a refusal's site to standard error just before the process ends there, for a child
    /// test's parent to read: two plain writes, nothing formatted.
    pub(super) fn say_refused(site: &'static str) {
        // SAFETY: writes from live buffers to the process's standard error.
        unsafe {
            libc::write(2, site.as_ptr().cast(), site.len());
            libc::write(2, b"\n".as_ptr().cast(), 1);
        }
    }

    impl UringIO {
        /// A ring held as [`held_for_test`](UringIO::held_for_test) holds it, its delivery thread
        /// waiting at a gate before its first pass.
        fn gated_for_test(entries: u32) -> (Self, GateHandle) {
            let (gate, handle) = gate();
            NEXT_GATE.with(|g| g.set(Some(gate)));
            (Self::held_for_test(entries).unwrap(), handle)
        }

        /// Puts a gate before the delivery thread's next pass, and writes the ring's eventfd so the
        /// thread leaves its wait and comes to it.
        fn gate_next_pass_for_test(&self) -> GateHandle {
            let (gate, handle) = gate();
            *self.shared.pass_gate.lock() = Some(gate);
            let one = 1u64.to_ne_bytes();
            // SAFETY: an 8-byte write from a live buffer to the eventfd the ring holds open.
            unsafe { libc::write(self.shared.eventfd.as_raw_fd(), one.as_ptr().cast(), 8) };
            handle
        }

        /// Enters exactly `n` of the queued entries while entering is held: the test's one-shot entry.
        fn enter_once_for_test(&self, n: u32) -> usize {
            let _state = self.shared.state.lock();
            // SAFETY: holding the mutex, so no other submission queue object exists.
            unsafe {
                self.shared
                    .ring
                    .submitter()
                    .enter::<libc::sigset_t>(n, 0, 0, None)
            }
            .unwrap()
        }

        /// Lets entering go: the flag cleared, and the eventfd written so that the delivery
        /// thread's held wait ends and its next wait enters what is queued.
        /// Holds the overflow where it is, or lets it go.
        fn hold_flush_for_test(&self, held: bool) {
            self.shared.state.lock().flush_held = held;
        }

        /// Enters what the submission queue holds, as the kernel's own consumption of it would,
        /// past the entering held and with no flush of the overflow.
        fn enter_queue_for_test(&self) -> usize {
            let _state = self.shared.state.lock();
            self.shared.ring.submit().unwrap()
        }

        fn release_entry_for_test(&self) {
            self.shared.state.lock().entry_held = false;
            let one = 1u64.to_ne_bytes();
            // SAFETY: an 8-byte write from a live buffer to the eventfd the ring holds open.
            unsafe { libc::write(self.shared.eventfd.as_raw_fd(), one.as_ptr().cast(), 8) };
        }

        /// The entries queued and not yet consumed by the kernel, and those past it in the overflow.
        fn pending_for_test(&self) -> (usize, usize) {
            let state = self.shared.state.lock();
            // SAFETY: holding the mutex.
            let sq = unsafe { self.shared.ring.submission_shared() };
            (sq.len(), state.overflow.len())
        }

        /// The requests each open file counts, by slot.
        fn file_requests_for_test(&self) -> Vec<usize> {
            let state = self.shared.state.lock();
            state
                .files
                .slots
                .iter()
                .filter(|u| u.open)
                .map(|u| u.requests)
                .collect()
        }

        fn on_take_for_test(&self) -> mpsc::Receiver<TakeNote> {
            let (tx, rx) = mpsc::channel();
            self.shared.state.lock().take_notes = Some(tx);
            rx
        }

        fn on_closer_for_test(&self, tx: mpsc::Sender<&'static str>) {
            *self.shared.closer_notes.lock() = Some(tx);
        }
    }

    /// A ring file over `path`, as `open_file` makes one but unlocked and of its own type, so a
    /// test reads its descriptor's number.
    fn ring_file(io: &UringIO, path: &std::path::Path) -> Arc<UringFile> {
        let file = common::open_unix(path.to_str().unwrap(), OpenFlags::NoLock).unwrap();
        let slot = io.shared.state.lock().files.take();
        Arc::new(UringFile {
            shared: Handle::new(&io.shared.0),
            file: ManuallyDrop::new(file),
            slot,
        })
    }

    fn filled(dir: &tempfile::TempDir, name: &str, byte: u8, len: usize) -> std::path::PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, vec![byte; len]).unwrap();
        path
    }

    /// Drops `file` on a helper thread, which says `returned` once the drop has returned.
    fn drop_on_helper(
        file: Arc<UringFile>,
        tx: mpsc::Sender<&'static str>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            drop(file);
            let _ = tx.send("returned");
        })
    }

    /// Whether `c` finished within [`BOUND`], looked at once a millisecond.
    fn finished_within(c: &Completion) -> bool {
        let since = std::time::Instant::now();
        while !c.finished() {
            if since.elapsed() > BOUND {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        true
    }

    /// The next note of `key`'s entries, within [`BOUND`].
    fn note_of(notes: &mpsc::Receiver<TakeNote>, key: u64) -> Option<TakeNote> {
        let since = std::time::Instant::now();
        while since.elapsed() < BOUND {
            match notes.recv_timeout(BOUND) {
                Ok(n) if n.key == key => return Some(n),
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
        None
    }

    /// The pending read's file's last drop, a file opened meanwhile, then the entry: the read's bytes,
    /// the two descriptors' numbers, and the drop's two notes (RING-FD1 §7.1-2).
    fn drop_reopen_enter(
        ring: &UringIO,
        a: Arc<UringFile>,
        b_path: &std::path::Path,
        c: &Completion,
        into: &Arc<crate::Buffer>,
    ) -> (
        Vec<u8>,
        i32,
        i32,
        Option<&'static str>,
        Option<&'static str>,
        bool,
    ) {
        let (tx, rx) = mpsc::channel();
        ring.on_closer_for_test(tx.clone());
        let fd_a = a.as_raw_fd();
        let helper = drop_on_helper(a, tx);
        let first = rx.recv_timeout(BOUND).ok();
        let b = std::fs::File::open(b_path).unwrap();
        let fd_b = b.as_raw_fd();
        ring.release_entry_for_test();
        let finished = finished_within(c);
        // A drop that returned without parking sends nothing more.
        let second = match first {
            Some("parked") => rx.recv_timeout(BOUND).ok(),
            _ => None,
        };
        helper.join().unwrap();
        drop(b);
        (
            into.as_slice().to_vec(),
            fd_a,
            fd_b,
            first,
            second,
            finished,
        )
    }

    /// RING-FD1 §7.1: a read held unentered in the submission queue keeps its file's descriptor its
    /// own: the file's last drop waits until the read's entry is reaped, so a file opened meanwhile
    /// takes another number, and the read reads its own file's bytes (mutant `close-at-once`: the
    /// drop closes at once, the next open takes the number, and the read returns the other file's).
    #[test]
    fn a_read_pending_in_the_queue_keeps_its_files_descriptor_until_its_entry_is_reaped() {
        let dir = tempfile::tempdir().unwrap();
        let ring = UringIO::held_for_test(ENTRIES).unwrap();
        let a = ring_file(&ring, &filled(&dir, "a", b'O', 512));
        let b_path = filled(&dir, "b", b'S', 512);
        let into = Arc::new(crate::Buffer::new_temporary(512));
        let c = a
            .pread(0, Completion::new_read(into.clone(), |_| None))
            .unwrap();
        let pending = ring.pending_for_test();
        let (bytes, fd_a, fd_b, first, second, finished) =
            drop_reopen_enter(&ring, a, &b_path, &c, &into);
        drop(ring);
        assert_eq!(
            pending,
            (1, 0),
            "the read unconsumed in the queue before the drop"
        );
        assert!(finished, "the read never finished");
        assert!(
            bytes.iter().all(|&x| x == b'O'),
            "the read's bytes are its own file's: {:?}",
            &bytes[..8]
        );
        assert_ne!(
            fd_b, fd_a,
            "the file opened meanwhile took the read's descriptor number"
        );
        assert_eq!(
            (first, second),
            (Some("parked"), Some("returned")),
            "the drop parked, and returned only after the read's entry"
        );
    }

    /// RING-FD1 §7.2: the same with the read past a full submission queue, in the overflow, until
    /// the delivery thread's loop moves and enters it (mutant `close-at-once`).
    #[test]
    fn a_read_in_the_overflow_keeps_its_files_descriptor_until_its_entry_is_reaped() {
        let dir = tempfile::tempdir().unwrap();
        let ring = UringIO::held_for_test(4).unwrap();
        let c_file = ring_file(&ring, &filled(&dir, "c", b'C', 512));
        let fill: Vec<Completion> = (0..4)
            .map(|_| {
                let into = Arc::new(crate::Buffer::new_temporary(512));
                c_file
                    .pread(0, Completion::new_read(into, |_| None))
                    .unwrap()
            })
            .collect();
        let a = ring_file(&ring, &filled(&dir, "a", b'O', 512));
        let b_path = filled(&dir, "b", b'S', 512);
        let into = Arc::new(crate::Buffer::new_temporary(512));
        let c = a
            .pread(0, Completion::new_read(into.clone(), |_| None))
            .unwrap();
        let pending = ring.pending_for_test();
        let (bytes, fd_a, fd_b, first, second, finished) =
            drop_reopen_enter(&ring, a, &b_path, &c, &into);
        let filled_finished = fill.iter().all(finished_within);
        drop((c_file, ring));
        assert_eq!(
            pending,
            (4, 1),
            "the queue full and the read in the overflow before the drop"
        );
        assert!(finished && filled_finished, "a read never finished");
        assert!(
            bytes.iter().all(|&x| x == b'O'),
            "the read's bytes are its own file's: {:?}",
            &bytes[..8]
        );
        assert_ne!(
            fd_b, fd_a,
            "the file opened meanwhile took the read's descriptor number"
        );
        assert_eq!(
            (first, second),
            (Some("parked"), Some("returned")),
            "the drop parked, and returned only after the read's entry"
        );
    }

    /// D2d §4.2: the ring offer takes a read only with its lock free, no overflow waiting and room
    /// in its queue; declined, it has done nothing: the ring's accepted requests, its files'
    /// counts, its queue and its overflow as they were, and the request not the ring's. The cases:
    /// its lock held; its queue full; an overflow behind the full queue; and an overflow beside a
    /// queue the kernel has emptied, its flush held (mutants `accept-before-try`, the request
    /// accepted before the lock is tried; `full-overflows`, the queue's room not looked at and the
    /// entry put past a full queue into the overflow, as the blocking road does;
    /// `past-the-overflow`, the overflow not looked at).
    #[test]
    fn the_ring_offer_takes_a_read_only_with_its_lock_free_and_room_in_its_queue() {
        let dir = tempfile::tempdir().unwrap();
        let ring = UringIO::held_for_test(4).unwrap();
        let file = ring_file(&ring, &filled(&dir, "o", b'O', 512));
        let read = || Completion::new_read(Arc::new(crate::Buffer::new_temporary(512)), |_| None);
        let seen = || {
            (
                ring.counts_for_test(),
                ring.pending_for_test(),
                ring.file_requests_for_test(),
            )
        };
        let declined = |offer: &dyn Fn(&Completion) -> bool| {
            let before = seen();
            let c = read();
            (offer(&c), c.is_owned(), seen() == before)
        };
        let held = declined(&|c| ring.with_lock_held(|| file.try_pread_now(0, c)));
        // The queue full, entering held: four reads fill it.
        let fill: Vec<Completion> = (0..4).map(|_| file.pread(0, read()).unwrap()).collect();
        let full = declined(&|c| file.try_pread_now(0, c));
        // One more read waits in the overflow.
        let over = file.pread(0, read()).unwrap();
        let behind = declined(&|c| file.try_pread_now(0, c));
        // The kernel takes the queue's four, the overflow held beside the emptied queue.
        ring.hold_flush_for_test(true);
        let entered = ring.enter_queue_for_test();
        let emptied = fill.iter().all(finished_within);
        // The four let go by the delivery thread: the ring then counts the overflow's read alone.
        let since = std::time::Instant::now();
        while ring.counts_for_test() != (1, 1) && since.elapsed() < BOUND {
            std::thread::sleep(Duration::from_millis(1));
        }
        let counts = ring.counts_for_test();
        let pending = ring.pending_for_test();
        let beside = declined(&|c| file.try_pread_now(0, c));
        // The overflow and entering let go: the last read delivered; then an offer with the lock
        // free, room and no overflow goes in.
        ring.hold_flush_for_test(false);
        ring.release_entry_for_test();
        let delivered = finished_within(&over);
        // The delivery thread at a gate before its next pass, outside the ring's lock, through the
        // one offer, and let through before the offered read's delivery, which it makes (#5557).
        let gate = ring.gate_next_pass_for_test();
        let outside = gate.reached();
        let c = read();
        let taken = file.try_pread_now(0, &c);
        gate.open();
        let free = (taken, finished_within(&c));
        drop((file, ring));
        let opened = gate.opened();
        assert_eq!(
            [held, full, behind, beside],
            [(false, false, true); 4],
            "(taken, the request the ring's, the ring as it was) with its lock held, its queue \
             full, an overflow behind the full queue, and an overflow beside an empty one: an \
             offer it declined moved the ring"
        );
        assert_eq!(
            (entered, emptied, counts, pending),
            (4, true, (1, 1), (0, 1)),
            "(entered, the four reads delivered, (accepted, counted), (queued, overflow)) once the \
             kernel took the queue"
        );
        assert!(delivered, "the overflow's read never finished");
        assert!(
            outside,
            "the delivery thread never waited at its gate, outside the ring's lock"
        );
        assert_eq!(
            free,
            (true, true),
            "(taken, delivered) with its lock free and room"
        );
        assert!(
            opened,
            "the delivery thread went past its gate without the test opening it"
        );
    }

    /// D2d §4.2: an offered write, taken with the ring's lock free and room in its queue, entering
    /// held: accepted by the ring and counted on its file until its delivery, its buffer held by
    /// the ring once the caller has let go of its own, and its bytes in the file once entering is
    /// let go (mutant `offered-write-unkept`: the ring keeps no hold on the buffer). The test keeps
    /// a hold of its own on the buffer to the write's delivery, so that no write, a mutant's
    /// included, reads freed memory; the ring's hold is the one besides it. The delivery thread
    /// waits at a gate before its first pass, outside the ring's lock, through the offer and the
    /// snapshot, so the lock is free when the offer tries it (#5556).
    #[test]
    fn the_ring_offer_writes_a_buffer_it_keeps_until_its_delivery() {
        let dir = tempfile::tempdir().unwrap();
        let path = filled(&dir, "w", b'O', 4096);
        let (ring, gate) = UringIO::gated_for_test(4);
        let outside = gate.reached();
        let file = ring_file(&ring, &path);
        let buffer = Arc::new(crate::Buffer::new_temporary(4096));
        buffer.as_mut_slice().fill(b'W');
        // The test's own hold, to the write's delivery.
        let safety = buffer.clone();
        let c = Completion::new_write(|_| {});
        let taken = file.try_pwrite_now(0, &buffer, &c);
        drop(buffer);
        let in_flight = (
            Arc::strong_count(&safety),
            c.is_owned(),
            ring.counts_for_test(),
            ring.pending_for_test(),
        );
        gate.open();
        ring.release_entry_for_test();
        let delivered = finished_within(&c);
        let bytes = std::fs::read(&path).unwrap();
        drop((c, safety, file, ring));
        let opened = gate.opened();
        assert!(
            outside,
            "the delivery thread never waited at its gate, outside the ring's lock"
        );
        assert!(taken, "the offered write was not taken");
        assert_eq!(
            in_flight,
            (2, true, (1, 1), (1, 0)),
            "(the buffer's holds, the test's and the ring's; the request the ring's; (accepted, \
             counted); (queued, overflow)) before entering: the ring did not keep the offered \
             write and its buffer"
        );
        assert!(delivered, "the offered write never finished");
        assert!(
            bytes.iter().all(|&b| b == b'W'),
            "the offered write's bytes: {:?}",
            &bytes[..8]
        );
        assert!(
            opened,
            "the delivery thread went past its gate without the test opening it"
        );
    }

    /// RING-FD1 §7.3: a vectored write too long for one entry keeps its file counted through its
    /// continuation: `take`, reaping the first part with the continuation held unentered, finds the
    /// file still counting the write, and lets it go only at the last part (mutant
    /// `continuation-lets-go`: the count let go at the partial entry).
    #[test]
    fn a_partial_writes_continuation_keeps_its_file_counted_until_its_last_part() {
        let dir = tempfile::tempdir().unwrap();
        let ring = UringIO::held_for_test(ENTRIES).unwrap();
        let path = filled(&dir, "w", 0, 0);
        let file = ring_file(&ring, &path);
        // Every other buffer of twice as many, so no two written are adjacent in memory.
        let all: Vec<Arc<crate::Buffer>> = (0..2 * (MAX_IOVEC_ENTRIES + 1))
            .map(|i| {
                let b = Arc::new(crate::Buffer::new_temporary(512));
                b.as_mut_slice().fill(i as u8);
                b
            })
            .collect();
        let bufs: Vec<Arc<crate::Buffer>> = all.iter().step_by(2).cloned().collect();
        let notes = ring.on_take_for_test();
        let c = file
            .pwritev(0, bufs.clone(), Completion::new_write(|_| {}))
            .unwrap();
        let key = Arc::as_ptr(c.get_inner()) as u64;
        let entered = ring.enter_once_for_test(1);
        let first = note_of(&notes, key).map(|n| (n.last, n.count));
        let pending = ring.pending_for_test();
        // Everything is looked at before entering is let go, and asserted only once the write
        // has ended and its file has dropped, so a failure never leaves the file's drop waiting
        // on an entry the hold keeps unentered.
        ring.release_entry_for_test();
        let last = note_of(&notes, key).map(|n| (n.last, n.count));
        let finished = finished_within(&c);
        let written = std::fs::read(&path).unwrap();
        drop((file, ring));
        let expected: Vec<u8> = bufs.iter().flat_map(|b| b.as_slice().to_vec()).collect();
        assert_eq!(entered, 1, "the first part entered alone");
        assert_eq!(
            first,
            Some((false, 1)),
            "the first part's entry: partial, and the file still counting the write"
        );
        assert_eq!(
            pending.0, 1,
            "the continuation unentered while entering is held"
        );
        assert_eq!(
            last,
            Some((true, 0)),
            "the last part's entry: the write let go"
        );
        assert!(finished, "the write never finished");
        assert!(written == expected, "the file's bytes are the buffers'");
    }

    /// RING-FD1 §7.4: a read cancelled while it is still unentered keeps its file's descriptor until
    /// its own entry is reaped: the file's last drop is still parked while entering is held (mutant
    /// `cancel-lets-go`: the count let go at the abort, so the drop returns at once). The buffer is
    /// read only after the read's own raw entry, and holds the file's bytes or the untouched fill.
    #[test]
    fn a_cancelled_reads_file_keeps_its_descriptor_until_the_reads_own_entry() {
        let dir = tempfile::tempdir().unwrap();
        let ring = UringIO::held_for_test(ENTRIES).unwrap();
        let a = ring_file(&ring, &filled(&dir, "a", b'O', 512));
        let b_path = filled(&dir, "b", b'S', 512);
        let into = Arc::new(crate::Buffer::new_temporary(512));
        into.as_mut_slice().fill(b'F');
        let notes = ring.on_take_for_test();
        let c = a
            .pread(0, Completion::new_read(into.clone(), |_| None))
            .unwrap();
        let key = Arc::as_ptr(c.get_inner()) as u64;
        ring.cancel(std::slice::from_ref(&c)).unwrap();
        let (tx, rx) = mpsc::channel();
        ring.on_closer_for_test(tx.clone());
        let helper = drop_on_helper(a, tx);
        // The drop's first note, taken while entering is still held, so the read's entry cannot
        // have been reaped; asserted only once the hold is let go and the helper has joined.
        let first = rx.recv_timeout(BOUND).ok();
        let b = std::fs::File::open(&b_path).unwrap();
        ring.release_entry_for_test();
        let reaped = note_of(&notes, key);
        // A drop that returned without parking sends nothing more.
        let second = match first {
            Some("parked") => rx.recv_timeout(BOUND).ok(),
            _ => None,
        };
        helper.join().unwrap();
        let bytes = into.as_slice().to_vec();
        drop((b, ring));
        assert_eq!(
            first,
            Some("parked"),
            "the drop parked while the cancelled read was unconsumed"
        );
        let reaped = reaped.expect("the read's own entry reaped");
        assert!(
            reaped.last && (reaped.result == 512 || reaped.result == -libc::ECANCELED),
            "the read's raw result: {}",
            reaped.result
        );
        assert!(
            bytes.iter().all(|&x| x == b'O') || bytes.iter().all(|&x| x == b'F'),
            "the buffer holds the file's bytes or the untouched fill: {:?}",
            &bytes[..8]
        );
        assert_eq!(
            second,
            Some("returned"),
            "the drop returned after the read's entry"
        );
    }

    /// RING-FD1 §7.5 (child): each of the ten request variants counts on its file while it is
    /// unreaped, and all are let go once reaped (mutant `sync-uncounted`: the sync queued without its
    /// count). The variants are pread, pwrite and pwrite_durable, each also from a fixed buffer, sync,
    /// sync_data, the truncate opcode and pwritev. The child prints the counts before it lets entering
    /// go, for a mutant whose later reap ends the process, and whether the kernel has the truncate
    /// opcode: without it the opcode is not covered here, and the parent says so.
    #[cfg(not(miri))]
    #[test]
    fn every_request_opcode_counts_on_its_file_until_reaped() {
        let ended = crate::child_test::in_child(
            concat!(
                module_path!(),
                "::every_request_opcode_counts_on_its_file_until_reaped"
            ),
            || {
                let dir = tempfile::tempdir().unwrap();
                let ring = Arc::new(UringIO::held_for_test(ENTRIES).unwrap());
                let io: Arc<dyn IO> = ring.clone();
                let pool = crate::storage::buffer_pool::BufferPool::begin_init(
                    &io,
                    crate::storage::buffer_pool::BufferPool::TEST_ARENA_SIZE,
                );
                pool.finalize_with_page_size(4096).unwrap();
                let file = ring_file(&ring, &filled(&dir, "f", 1, 4096));
                let plain = || Arc::new(crate::Buffer::new_temporary(4096));
                let fixed: Vec<Arc<crate::Buffer>> =
                    (0..3).map(|_| Arc::new(pool.get_page())).collect();
                let count = |ring: &UringIO| ring.file_requests_for_test()[0];
                let mut cs = Vec::new();
                let mut counts = Vec::new();
                cs.push(
                    file.pread(0, Completion::new_read(plain(), |_| None))
                        .unwrap(),
                );
                counts.push(("pread", count(&ring)));
                cs.push(
                    file.pread(0, Completion::new_read(fixed[0].clone(), |_| None))
                        .unwrap(),
                );
                counts.push(("pread fixed", count(&ring)));
                cs.push(
                    file.pwrite(0, plain(), Completion::new_write(|_| {}))
                        .unwrap(),
                );
                counts.push(("pwrite", count(&ring)));
                cs.push(
                    file.pwrite(0, fixed[1].clone(), Completion::new_write(|_| {}))
                        .unwrap(),
                );
                counts.push(("pwrite fixed", count(&ring)));
                cs.push(
                    file.pwrite_durable(0, plain(), Completion::new_write(|_| {}))
                        .unwrap(),
                );
                counts.push(("pwrite_durable", count(&ring)));
                cs.push(
                    file.pwrite_durable(0, fixed[2].clone(), Completion::new_write(|_| {}))
                        .unwrap(),
                );
                counts.push(("pwrite_durable fixed", count(&ring)));
                cs.push(
                    file.sync(Completion::new_sync(|_| {}), crate::io::FileSyncType::Fsync)
                        .unwrap(),
                );
                counts.push(("sync", count(&ring)));
                cs.push(file.sync_data(Completion::new_sync(|_| {})).unwrap());
                counts.push(("sync_data", count(&ring)));
                cs.push(file.truncate(4096, Completion::new_trunc(|_| {})).unwrap());
                counts.push(("truncate", count(&ring)));
                cs.push(
                    file.pwritev(0, vec![plain()], Completion::new_write(|_| {}))
                        .unwrap(),
                );
                counts.push(("pwritev", count(&ring)));
                let ftruncate = ring.shared.caps.ftruncate;
                let ids: Vec<Option<u32>> = fixed.iter().map(|b| b.fixed_id()).collect();
                eprintln!("FIXED {ids:?}");
                eprintln!("COUNTS ftruncate={ftruncate} {counts:?}");
                ring.release_entry_for_test();
                let finished = cs.iter().all(finished_within);
                eprintln!(
                    "AFTER finished={finished} {:?}",
                    ring.file_requests_for_test()
                );
                drop((cs, fixed, file, pool, io, ring));
            },
        );
        let Some(ended) = ended else { return };
        let line = |prefix: &str| {
            ended
                .stderr
                .lines()
                .find(|l| l.starts_with(prefix))
                .unwrap_or("none")
                .to_string()
        };
        let (fixed, counts, after) = (line("FIXED"), line("COUNTS"), line("AFTER"));
        assert_eq!(
            fixed, "FIXED [Some(0), Some(0), Some(0)]",
            "each fixed variant's buffer is the ring's registered arena's"
        );
        // Without the kernel's truncate opcode the truncation is the synchronous fallback, which
        // counts nothing; whether the opcode was covered is the child's COUNTS line's to say.
        let t = if counts.contains("ftruncate=true") {
            9
        } else {
            8
        };
        let want = format!(
            "[(\"pread\", 1), (\"pread fixed\", 2), (\"pwrite\", 3), (\"pwrite fixed\", 4), \
             (\"pwrite_durable\", 5), (\"pwrite_durable fixed\", 6), (\"sync\", 7), \
             (\"sync_data\", 8), (\"truncate\", {t}), (\"pwritev\", {})]",
            t + 1
        );
        assert!(
            counts.ends_with(&want),
            "each variant counted once on its file while unreaped: {counts}, child: {:?}",
            ended.code
        );
        assert_eq!(
            after, "AFTER finished=true [0]",
            "every request let go once reaped"
        );
        assert!(
            !ended.aborted && !ended.hung && ended.code == Some(0),
            "the child ended cleanly"
        );
    }

    /// RING-FD1 §7.5: a truncation on a ring without the kernel's opcode is the synchronous
    /// fallback, not a ring request, and counts nothing on its file.
    #[test]
    fn a_truncation_without_the_rings_opcode_counts_nothing() {
        let dir = tempfile::tempdir().unwrap();
        NO_FTRUNCATE_OPCODE.with(|n| n.set(true));
        let ring = UringIO::held_for_test(ENTRIES);
        NO_FTRUNCATE_OPCODE.with(|n| n.set(false));
        let ring = ring.unwrap();
        let path = filled(&dir, "f", 1, 4096);
        let file = ring_file(&ring, &path);
        let c = file.truncate(512, Completion::new_trunc(|_| {})).unwrap();
        let counted = ring.file_requests_for_test();
        let (opcode, finished, succeeded) =
            (ring.shared.caps.ftruncate, c.finished(), c.succeeded());
        drop((file, ring));
        assert!(!opcode, "the ring took the truncate opcode as supported");
        assert_eq!(counted, [0], "the synchronous truncate counted on its file");
        assert!(
            finished && succeeded,
            "the synchronous truncate finished at once"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            512,
            "the file truncated"
        );
    }

    /// RING-FD1 §7.6: a file with no request outstanding, and one whose request was reaped, closes
    /// at once on its last drop, without parking.
    #[test]
    fn a_file_with_no_request_outstanding_closes_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let ring = UringIO::new().unwrap();
        let (tx, rx) = mpsc::channel();
        ring.on_closer_for_test(tx.clone());
        let fresh = ring_file(&ring, &filled(&dir, "a", b'O', 512));
        drop_on_helper(fresh, tx.clone()).join().unwrap();
        let used = ring_file(&ring, &filled(&dir, "b", b'O', 512));
        let c = used.pread(0, read_telling(&mpsc::channel().0)).unwrap();
        let finished = finished_within(&c);
        drop_on_helper(used, tx).join().unwrap();
        let seen: Vec<_> = rx.try_iter().collect();
        assert!(finished, "the read never finished");
        assert_eq!(seen, ["returned", "returned"], "neither drop parked");
    }

    /// RING-FD1 §7.7 (child): a file's last handle dropped on the ring's delivery thread, by a
    /// callback, while another of its requests is unreaped ends the process at its named refusal:
    /// the thread could only wait for entries it alone reaps (mutant `delivery-drop-unrefused`: the
    /// drop parks on the delivery thread, which the child reports by name).
    #[cfg(not(miri))]
    #[test]
    fn a_files_last_drop_on_the_delivery_thread_with_requests_unreaped_ends_the_process() {
        let ended = crate::child_test::in_child(
            concat!(
                module_path!(),
                "::a_files_last_drop_on_the_delivery_thread_with_requests_unreaped_ends_the_process"
            ),
            || {
                let dir = tempfile::tempdir().unwrap();
                let ring = UringIO::new().unwrap();
                let (_held, _fifo, pipe) = held_read(&dir, &ring);
                let (tx, rx) = mpsc::channel();
                ring.on_closer_for_test(tx);
                let other = file_of(&dir, &ring, 512);
                let last = crate::sync::Mutex::new(Some(pipe));
                let c =
                    Completion::new_read(Arc::new(crate::Buffer::new_temporary(512)), move |_| {
                        drop(last.lock().take());
                        None
                    });
                drop(other.pread(0, c).unwrap());
                if rx.recv_timeout(BOUND).is_ok() {
                    eprintln!("PARKED ON THE DELIVERY THREAD");
                    std::process::exit(3);
                }
                eprintln!("THE DROP NEITHER PARKED NOR ENDED THE PROCESS");
                std::process::exit(4);
            },
        );
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted && ended.stderr.contains(REFUSED_DROP_ON_DELIVERY),
            "the child ended {:?} (aborted {}, hung {}): {}",
            ended.code,
            ended.aborted,
            ended.hung,
            ended.stderr
        );
    }

    /// RING-FD1 §7.8: a refusal to enter that may pass, with nothing else in flight, wakes the
    /// delivery thread through its eventfd, whose next wait enters the request (mutant
    /// `refusal-unrung`: no eventfd write, and the request waits unentered).
    #[test]
    fn a_refusal_to_enter_that_may_pass_wakes_the_delivery_thread_to_enter() {
        let dir = tempfile::tempdir().unwrap();
        let ring = UringIO::new().unwrap();
        let file = file_of(&dir, &ring, 512);
        let before = REFUSALS_RUNG.load(Ordering::Relaxed);
        ENTER_REFUSED.with(|r| r.set(Some(libc::EAGAIN)));
        let c = file
            .pread(
                0,
                Completion::new_read(Arc::new(crate::Buffer::new_temporary(512)), |_| None),
            )
            .unwrap();
        let rung = REFUSALS_RUNG.load(Ordering::Relaxed) - before;
        if rung == 0 {
            // So the read is reaped whatever came of the refusal, and the file's drop returns.
            ring.enter_once_for_test(1);
        }
        let finished = finished_within(&c);
        drop((file, ring));
        assert_eq!(
            rung, 1,
            "the refusal wrote the delivery thread's eventfd once"
        );
        assert!(finished, "the read never finished");
    }
}
