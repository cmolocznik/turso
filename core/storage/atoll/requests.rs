//! Atoll's count of the requests on its files that a backend still owns (DESIGN.md 22.5, 22.12).
//!
//! A database opens every file through [`Counted`], an IO that wraps its embedder's and counts
//! every request on those files before it is submitted, from whatever submits it: a statement, a
//! job, opening or a callback. A request is uncounted when its submission accepted nothing, or
//! once its backend retires it, which may be long after it finished: a cancelled read the kernel
//! still holds counts until the kernel gives it back. A request a callback submits is counted
//! inside that callback, before the request whose callback it is retires, so the count does not
//! pass through zero while a chain of requests goes on. Close waits for the count to reach zero
//! ([`Requests::drain`]) and seals it there, so no request lands on a file after close returns:
//! one submitted after is refused, whatever submits it.

use crate::io::clock::{Clock, MonotonicInstant, WallClockInstant};
use crate::io::{
    Buffer, Completion, Delivery, File, FileId, FileSyncType, MemoryIO, OpenFlags,
    SharedWalLockKind, SharedWalMappedRegion, Stepped, IO,
};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::Result;
use std::ptr::NonNull;

/// The requests on a database's files submitted and not retired, in one word with close's marks:
/// bit 0 says close waits on the word ([`CLOSING`]), bit 1 that close found it at zero and sealed
/// it ([`SEALED`]), and the rest count the requests ([`ONE`] each). Every change is one atomic
/// step on the word, so a change made while close waits wakes it, and a submission either counts
/// before the seal, which close then waits for, or reads it and is refused.
pub(super) struct Requests {
    word: AtomicU64,
    /// The thread close waits on, set before it marks the word.
    closer: crate::sync::OnceLock<crate::thread::Thread>,
}

/// Close waits on the word.
const CLOSING: u64 = 1;
/// Close has found no request left: none is accepted again.
const SEALED: u64 = 1 << 1;
/// One request submitted and not retired.
const ONE: u64 = 1 << 2;
/// The requests submitted and not retired, out of a word.
fn outstanding_in(word: u64) -> u64 {
    word >> 2
}

impl Default for Requests {
    fn default() -> Self {
        Self {
            word: AtomicU64::new(0),
            closer: crate::sync::OnceLock::new(),
        }
    }
}

impl Requests {
    /// Counts the request `submit` submits for as long as its backend owns it; refused, never
    /// submitted, once close has sealed the count.
    fn submit(
        self: &Arc<Self>,
        physical: Completion,
        submit: impl FnOnce() -> Result<Completion>,
    ) -> Result<Completion> {
        if self.word.fetch_add(ONE, Ordering::AcqRel) & SEALED != 0 {
            // A close looking again meanwhile saw it counted, and looks once more.
            self.changed(self.word.fetch_sub(ONE, Ordering::AcqRel));
            return Err(crate::LimboError::InternalError(
                "a request on a file of a closed database".to_string(),
            ));
        }
        // Counted and not yet the backend's: a close that looks now finds the backend empty and
        // parks, and the submission wakes it (`IO::try_step`).
        crate::race::point("requests.counted");
        let submitted = match submit() {
            Ok(c) => {
                let requests = self.clone();
                physical.on_retired(move || requests.retired());
                Ok(c)
            }
            Err(e) => {
                self.changed(self.word.fetch_sub(ONE, Ordering::AcqRel));
                Err(e)
            }
        };
        submitted
    }

    /// A request counted has retired.
    fn retired(&self) {
        let was = self.word.fetch_sub(ONE, Ordering::AcqRel);
        assert!(outstanding_in(was) > 0, "a request uncounted twice");
        self.changed(was);
    }

    /// A change made to the word, which read `was`: close, if it waits, looks again.
    fn changed(&self, was: u64) {
        if was & CLOSING != 0 {
            if let Some(closer) = self.closer.get() {
                closer.unpark();
            }
        }
    }

    /// The requests submitted and not retired.
    pub(super) fn outstanding(&self) -> usize {
        outstanding_in(self.word.load(Ordering::Acquire)) as usize
    }

    /// Close's wait for its requests (DESIGN.md 22.12, step 4), until every request counted has
    /// retired. On a pumped backend it steps while the backend delivers, and once the backend
    /// holds nothing it parks until a change to the word (a retirement, a request a callback
    /// counts meanwhile) or the backend's next request wakes it (`IO::try_step`); on any other it
    /// parks. Every backend retires every request it accepted (the kernel answers each, a pumped
    /// backend delivers each when stepped, a finish under way returns), so the wait ends. A step
    /// that fails has lost the means to retire them, and ends the process (22.11): close can
    /// neither wait on nor return while the backend may still own its requests' buffers.
    pub(super) fn drain(&self, io: &dyn IO) {
        // Close waits: never on a ring's delivery thread, the one that would deliver what it waits
        // for (DESIGN.md 22.5).
        crate::io::refuse_on_delivery_thread("a database's close");
        let me = self.closer.get_or_init(crate::thread::current);
        assert_eq!(
            me.id(),
            crate::thread::current().id(),
            "a database closed from two threads"
        );
        // SeqCst: a change the read below misses reads the mark.
        self.word.fetch_or(CLOSING, Ordering::SeqCst);
        let pumped = io.delivery() == Delivery::Pumped;
        let place = crate::io::ReadyPlace::default();
        let ready = crate::io::unparking(me.clone());
        let mut registered = false;
        loop {
            let word = self.word.load(Ordering::SeqCst);
            // At zero, sealed in the same step that ends the wait: a submission counted since the
            // read fails the exchange and is waited for.
            if outstanding_in(word) == 0 {
                if self
                    .word
                    .compare_exchange(word, SEALED, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
                continue;
            }
            if !pumped {
                crate::storage::atoll::park::thread();
                continue;
            }
            match io.try_step(place.key(), &ready) {
                Err(_) => crate::io::stepping_failed("a database's close"),
                Ok(Stepped::Delivered) => {}
                // Refused at open (`refuse_a_backend_that_would_poll`).
                Ok(Stepped::Unsaid) => {
                    unreachable!("a backend that says what its steps find answered Unsaid")
                }
                Ok(Stepped::Empty) => {
                    registered = true;
                    crate::storage::atoll::park::thread();
                }
            }
        }
        if registered {
            io.withdraw(place.key());
        }
    }
}

/// An IO whose files count their requests in a database's [`Requests`]. Every other call goes to
/// the IO it wraps.
pub(super) struct Counted {
    io: Arc<dyn IO>,
    requests: Arc<Requests>,
}

impl Counted {
    pub(super) fn new(io: Arc<dyn IO>, requests: Arc<Requests>) -> Self {
        Self { io, requests }
    }
}

impl Clock for Counted {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.io.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.io.current_time_wall_clock()
    }
}

impl IO for Counted {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let file = self.io.open_file(path, flags, direct)?;
        Ok(Arc::new(CountedFile {
            file,
            requests: self.requests.clone(),
        }))
    }

    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        let file = self.io.open_shared_wal_file(path)?;
        Ok(Arc::new(CountedFile {
            file,
            requests: self.requests.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.io.remove_file(path)
    }

    fn sync_directory(&self, path: &str) -> Result<()> {
        self.io.sync_directory(path)
    }

    fn rename_file(&self, from: &str, to: &str) -> Result<bool> {
        self.io.rename_file(from, to)
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        self.io.supports_shared_wal_coordination()
    }

    fn step(&self) -> Result<()> {
        self.io.step()
    }

    fn try_step(&self, key: crate::io::ReadyKey, ready: &std::task::Waker) -> Result<Stepped> {
        self.io.try_step(key, ready)
    }

    fn says_what_a_step_found(&self) -> bool {
        self.io.says_what_a_step_found()
    }

    fn withdraw(&self, key: crate::io::ReadyKey) {
        self.io.withdraw(key)
    }

    fn delivery(&self) -> crate::io::Delivery {
        self.io.delivery()
    }

    fn wait(&self, c: &Completion) {
        self.io.wait(c)
    }

    fn cancel(&self, c: &[Completion]) -> Result<()> {
        self.io.cancel(c)
    }

    fn wait_retired(&self, c: &Completion) -> Result<()> {
        self.io.wait_retired(c)
    }

    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        self.io.drain_completions(completions)
    }

    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        self.io.wait_for_completion(c)
    }

    fn generate_random_number(&self) -> i64 {
        self.io.generate_random_number()
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.io.fill_bytes(dest)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        self.io.get_memory_io()
    }

    fn register_fixed_buffer(&self, ptr: NonNull<u8>, len: usize) -> Result<u32> {
        self.io.register_fixed_buffer(ptr, len)
    }

    fn yield_now(&self) {
        self.io.yield_now()
    }

    fn sleep(&self, duration: std::time::Duration) {
        self.io.sleep(duration)
    }

    fn file_id(&self, path: &str) -> Result<FileId> {
        self.io.file_id(path)
    }
}

/// A file whose requests count in its database's [`Requests`]. Every other call goes to the file
/// it wraps.
struct CountedFile {
    file: Arc<dyn File>,
    requests: Arc<Requests>,
}

impl File for CountedFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.requests.submit(c.clone(), || self.file.pread(pos, c))
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.requests
            .submit(c.clone(), || self.file.pwrite(pos, buffer, c))
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.requests
            .submit(c.clone(), || self.file.sync(c, sync_type))
    }

    fn sync_data(&self, c: Completion) -> Result<Completion> {
        self.requests.submit(c.clone(), || self.file.sync_data(c))
    }

    fn supports_durable_writes(&self) -> bool {
        self.file.supports_durable_writes()
    }

    fn pwrite_durable(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.requests
            .submit(c.clone(), || self.file.pwrite_durable(pos, buffer, c))
    }

    fn allocate(&self, pos: u64, len: u64) -> Result<()> {
        self.file.allocate(pos, len)
    }

    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        self.requests
            .submit(c.clone(), || self.file.pwritev(pos, buffers, c))
    }

    fn size(&self) -> Result<u64> {
        self.file.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.requests
            .submit(c.clone(), || self.file.truncate(len, c))
    }

    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        self.file.has_hole(pos, len)
    }

    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        self.file.punch_hole(pos, len)
    }

    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        self.file.shared_wal_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.file.shared_wal_try_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_probe_exclusive_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.file.shared_wal_probe_exclusive_byte(offset, kind)
    }

    fn shared_wal_probe_exclusive_while_shared_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.file
            .shared_wal_probe_exclusive_while_shared_byte(offset, kind)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        self.file.shared_wal_unlock_byte(offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.file.shared_wal_set_len(len)
    }

    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        self.file.shared_wal_map(offset, len)
    }
}

#[cfg(test)]
mod tests;
