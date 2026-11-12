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
//!
//! A request whose owner needs another after it, a metadata or superblock write's sync, is made
//! through [`CountedFile::write_then`]: its follow-on is made at its retirement, counted before
//! the write is uncounted, and submitted by the database's one IO thread ([`dispatch`]), so no
//! callback submits.

mod dispatch;

use dispatch::Dispatch;
pub(super) use dispatch::{Chain, Held};

use crate::io::clock::{Clock, MonotonicInstant, WallClockInstant};
use crate::io::{
    Buffer, Completion, Delivery, File, FileId, FileSyncType, MemoryIO, OpenFlags,
    SharedWalLockKind, SharedWalMappedRegion, Stepped, IO,
};
use crate::storage::atoll::locks::Mutex;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::{CompletionError, LimboError, Result};
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
    /// The records follow-ons are made in, set once by [`Counted::new`]: owned here and by the IO
    /// thread while it runs, and reaching these requests back only weakly.
    dispatch: crate::sync::OnceLock<Arc<Dispatch>>,
    /// The IO thread, stopped and joined by close's drain once it seals, or by the last drop.
    io_thread: Mutex<Option<crate::thread::JoinHandle<()>>>,
    /// What the embedder handed the database to drop once its files are closed: the IO that opens
    /// them, every file and every request on them holds these requests, and a file drops its
    /// descriptor before it lets them go ([`CountedFile`]), so the last of them drops this after the
    /// last descriptor of the database has closed, and with it every lock the process held on its
    /// files (R17), whatever held the database last.
    _held: Option<Arc<dyn std::any::Any + Send + Sync>>,
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
        Self::holding(None)
    }
}

impl Requests {
    /// No request yet, holding `held` until the last file and request of the database has gone.
    pub(super) fn holding(held: Option<Arc<dyn std::any::Any + Send + Sync>>) -> Self {
        Self {
            word: AtomicU64::new(0),
            closer: crate::sync::OnceLock::new(),
            dispatch: crate::sync::OnceLock::new(),
            io_thread: Mutex::new(None),
            _held: held,
        }
    }

    /// Counts the request `submit` submits, and runs `then` at its retirement before uncounting
    /// it: Counted's one observer, registered at acceptance. A refused submission runs nothing
    /// and leaves the count as it was.
    fn submit_with(
        self: &Arc<Self>,
        physical: Completion,
        submit: impl FnOnce() -> Result<Completion>,
        then: impl FnOnce() + Send + 'static,
    ) -> Result<Completion> {
        if self.word.fetch_add(ONE, Ordering::AcqRel) & SEALED != 0 {
            self.changed(self.word.fetch_sub(ONE, Ordering::AcqRel));
            return Err(crate::LimboError::InternalError(
                "a request on a file of a closed database".to_string(),
            ));
        }
        crate::race::point("requests.counted");
        match submit() {
            Ok(c) => {
                let requests = self.clone();
                physical.on_retired(move || {
                    then();
                    requests.retired();
                });
                Ok(c)
            }
            Err(e) => {
                self.changed(self.word.fetch_sub(ONE, Ordering::AcqRel));
                Err(e)
            }
        }
    }

    /// Counts one request to be submitted later, as a follow-on is (D1a §3.2, count B): refused
    /// once close has sealed the count.
    fn count_one(&self) -> bool {
        if self.word.fetch_add(ONE, Ordering::AcqRel) & SEALED != 0 {
            self.changed(self.word.fetch_sub(ONE, Ordering::AcqRel));
            return false;
        }
        true
    }

    /// Lets go a count [`count_one`](Self::count_one) took for a request never accepted.
    fn uncount_one(&self) {
        self.retired();
    }

    /// Submits a request already counted ([`count_one`](Self::count_one)), and runs `then` at its
    /// retirement before uncounting it. Refused, the count stays for its caller to let go.
    fn submit_counted(
        self: &Arc<Self>,
        submit: impl FnOnce() -> Result<Completion>,
        then: impl FnOnce() + Send + 'static,
        physical: Completion,
    ) -> Result<()> {
        let c = submit()?;
        drop(c);
        let requests = self.clone();
        physical.on_retired(move || {
            then();
            requests.retired();
        });
        Ok(())
    }

    /// Counts the request `submit` submits for as long as its backend owns it; refused, never
    /// submitted, once close has sealed the count.
    fn submit(
        self: &Arc<Self>,
        physical: Completion,
        submit: impl FnOnce() -> Result<Completion>,
    ) -> Result<Completion> {
        // Refused once sealed: a close looking again meanwhile saw it counted, and looks once
        // more. Counted and not yet the backend's: a close that looks now finds the backend empty
        // and parks, and the submission wakes it (`IO::try_step`).
        self.submit_with(physical, submit, || {})
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
    #[cfg(test)]
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
            crate::race::point("requests.drain_looked");
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
        // Sealed: nothing is counted again, so the IO thread has nothing left to submit. It stops
        // here, inside close's step 4, before the executor drops its jobs.
        self.stop_io_thread();
    }

    /// Stops the IO thread and joins it, or, on that thread itself (the last observer dropping
    /// the last hold on these requests there), lets it end on its own once this returns. Once
    /// stopped, a later call finds nothing.
    fn stop_io_thread(&self) {
        let Some(handle) = self.io_thread.lock().take() else {
            return;
        };
        if let Some(dispatch) = self.dispatch.get() {
            dispatch.stop();
        }
        if handle.thread().id() != crate::thread::current().id() {
            // A panic on the IO thread ended the process (its loop is an Atoll entry): a join
            // returns its loop's end.
            let _ = handle.join();
        }
    }

    /// The dispatch the records live in, once [`Counted::new`] made it.
    fn dispatch(&self) -> &Arc<Dispatch> {
        self.dispatch
            .get()
            .expect("the requests of a database opened through Counted")
    }

    /// The records at rest, for a test's exact count of them.
    #[cfg(test)]
    pub(super) fn records_at_rest(&self) -> usize {
        self.dispatch().at_rest()
    }

    /// Holds the IO thread before its next claim of a record, until the sender sends or drops.
    #[cfg(test)]
    pub(super) fn hold_io_thread(&self) -> std::sync::mpsc::Sender<()> {
        self.dispatch().hold_next_claim()
    }

    /// The dispatch, weakly, for a test's look after every owner has gone.
    #[cfg(test)]
    pub(super) fn dispatch_weak(&self) -> crate::sync::Weak<Dispatch> {
        Arc::downgrade(self.dispatch())
    }
}

impl Drop for Requests {
    /// The last resort when no drain ran: the IO thread stopped and joined, every follow-on still
    /// READY finished as `Aborted` by it on its way out, as nothing is left to count it.
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.stop_io_thread();
    }
}

/// An IO whose files count their requests in a database's [`Requests`]. Every other call goes to
/// the IO it wraps.
pub(super) struct Counted {
    io: Arc<dyn IO>,
    requests: Arc<Requests>,
}

impl Counted {
    /// Wraps `io`, counting in `requests`, whose records it makes and whose IO thread it starts:
    /// a thread that does not start fails the open, before any file is opened.
    pub(super) fn new(io: Arc<dyn IO>, requests: Arc<Requests>) -> Result<Self> {
        let dispatch = Arc::new(Dispatch::new(&requests));
        #[cfg(test)]
        MADE.with(|m| {
            *m.borrow_mut() = Some((Arc::downgrade(&dispatch), Arc::downgrade(&requests)))
        });
        let set = requests.dispatch.set(dispatch.clone());
        assert!(set.is_ok(), "one Counted per database's requests");
        let handle = Dispatch::spawn(&dispatch)?;
        *requests.io_thread.lock() = Some(handle);
        Ok(Self { io, requests })
    }

    /// Opens `path` as a counted file its owner holds typed: the one kind of file a follow-on is
    /// made on ([`CountedFile::write_then`]).
    pub(super) fn open_counted(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> Result<Arc<CountedFile>> {
        let file = self.io.open_file(path, flags, direct)?;
        Ok(Arc::new(CountedFile {
            file,
            requests: self.requests.clone(),
        }))
    }

    /// A record of the metadata cache's writes, taken at rest.
    pub(super) fn take_meta_write_record(&self) -> Result<Held> {
        self.requests.dispatch().take(dispatch::META_WRITES)
    }

    /// The superblock's record, taken at rest.
    pub(super) fn take_superblock_record(&self) -> Result<Held> {
        self.requests.dispatch().take(dispatch::SUPERBLOCK)
    }

    /// The requests it counts in.
    #[cfg(test)]
    pub(super) fn requests(&self) -> &Arc<Requests> {
        &self.requests
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
pub(super) struct CountedFile {
    /// The only holder of the file it wraps, so its drop closes the descriptor: declared before
    /// `requests`, so the descriptor closes before the requests, and what they hold, go.
    file: Arc<dyn File>,
    requests: Arc<Requests>,
}

impl CountedFile {
    /// Writes `buffer` at `pos` as the first request of a chain in `held` (DESIGN.md 22.18, D1a
    /// §2), counted once and submitted through the file this wraps. At its retirement its sync,
    /// when owed (the write not durable, and it and `chain` wanting one), is made into the same
    /// record, counted before the write is uncounted, and submitted by the IO thread. `chain` is
    /// told the write's outcome (a short write an error), the sync's, and the chain's end once
    /// every request of it has retired; a write refused ends the chain with its error before this
    /// returns it.
    pub(super) fn write_then(
        self: &Arc<Self>,
        held: Held,
        pos: u64,
        buffer: Arc<Buffer>,
        durable: bool,
        chain: Arc<dyn Chain>,
    ) -> Result<Completion> {
        let dispatch = self.requests.dispatch().clone();
        let i = held.index();
        dispatch.start(&held, chain, durable);
        let len = buffer.len();
        let weak = Arc::downgrade(&dispatch);
        let c = Completion::new_write(move |res| {
            if let Some(dispatch) = weak.upgrade() {
                dispatch.written(i, crate::storage::atoll::whole(len, res));
            }
        });
        let then = {
            let (dispatch, file) = (dispatch.clone(), Arc::downgrade(self));
            move || dispatch.after_write(i, &file)
        };
        let physical = c.clone();
        let submitted = self.requests.submit_with(
            physical,
            || {
                if durable {
                    self.file.pwrite_durable(pos, buffer, c)
                } else {
                    self.file.pwrite(pos, buffer, c)
                }
            },
            then,
        );
        submitted.inspect_err(|e| dispatch.refused(held, io_failure(e)))
    }
}

/// A submission's error, as a request's outcome.
fn io_failure(e: &LimboError) -> CompletionError {
    match e {
        LimboError::CompletionError(e) => *e,
        _ => CompletionError::IOError(std::io::ErrorKind::Other, "a request's submission refused"),
    }
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

/// A counted IO over `io` for a test that builds an owner on its own, outside an open: its IO
/// thread is stopped and joined by its requests' last drop.
#[cfg(test)]
pub(super) fn counted_for_test(io: Arc<dyn IO>) -> Arc<Counted> {
    Arc::new(Counted::new(io, Arc::new(Requests::default())).expect("a test's IO thread"))
}

/// The owner of a counted IO a test builds on its own (DESIGN.md 22.18, D1a §4): its drop drains
/// its requests, stepping a pumped backend until every counted request has retired, which seals
/// them and stops the IO thread. Dropped after the owners built over it.
#[cfg(test)]
pub(super) struct Standalone {
    pub(super) counted: Arc<Counted>,
    pub(super) io: Arc<dyn IO>,
}

#[cfg(test)]
impl Standalone {
    pub(super) fn new(raw: Arc<dyn IO>) -> Self {
        let counted = counted_for_test(raw);
        let io: Arc<dyn IO> = counted.clone();
        Self { counted, io }
    }

    pub(super) fn requests(&self) -> &Arc<Requests> {
        self.counted.requests()
    }
}

#[cfg(test)]
impl Drop for Standalone {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.counted.requests().drain(&*self.io);
    }
}

#[cfg(test)]
crate::thread::thread_local! {
    /// The dispatch and the requests of this thread's last [`Counted`], for a test's look after
    /// every owner has gone.
    static MADE: std::cell::RefCell<Option<(crate::sync::Weak<Dispatch>, crate::sync::Weak<Requests>)>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod followon_tests;
#[cfg(test)]
mod tests;
