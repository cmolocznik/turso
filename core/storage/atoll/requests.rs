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
#[cfg(all(test, not(any(shuttle, atoll_loom, miri))))]
pub(super) use dispatch::SlotPause;
pub(super) use dispatch::{
    Chain, FrameOwner, FrameRefused, Held, LogWork, OpensFile, Place, Shape, Source,
};

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
    /// The IO threads, stopped and joined by close's drain once it seals, or by the last drop.
    io_threads: Mutex<Vec<crate::thread::JoinHandle<()>>>,
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
            io_threads: Mutex::new(Vec::new()),
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
        // Close waits: never on a ring's delivery thread or a database's IO thread, the ones that
        // would deliver what it waits for (DESIGN.md 22.5).
        crate::io::refuse_on_any_delivering_thread("a database's close");
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
                    crate::race::point("requests.drain_sealed");
                    break;
                }
                continue;
            }
            if !pumped {
                crate::race::point("requests.drain_parks");
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
                    crate::race::point("requests.drain_parks");
                    crate::storage::atoll::park::thread();
                }
            }
        }
        if registered {
            io.withdraw(place.key());
        }
        // Sealed: nothing is counted again, so the IO threads have nothing left to submit. They
        // stop here, inside close's step 4, before the executor drops its jobs.
        self.stop_io_threads();
    }

    /// Stops the IO threads and joins each, but the one this runs on (the last observer dropping
    /// the last hold on these requests there), which ends on its own once this returns. Once
    /// stopped, a later call finds none.
    fn stop_io_threads(&self) {
        let handles = std::mem::take(&mut *self.io_threads.lock());
        if handles.is_empty() {
            return;
        }
        if let Some(dispatch) = self.dispatch.get() {
            dispatch.stop();
        }
        crate::race::point("requests.stopping");
        let me = crate::thread::current().id();
        for handle in handles {
            if handle.thread().id() != me {
                // A panic on an IO thread ended the process (its loop is an Atoll entry): a join
                // returns its loop's end.
                let _ = handle.join();
            }
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

    /// The queued records at rest, for a test's exact count of them.
    #[cfg(test)]
    pub(super) fn queued_at_rest(&self) -> usize {
        self.dispatch().queued_at_rest()
    }

    /// The frame records at rest, for a test's exact count of them.
    #[cfg(test)]
    pub(super) fn frames_at_rest(&self) -> usize {
        self.dispatch().frames_at_rest()
    }

    /// The slot records at rest, for a test's exact count of them.
    #[cfg(all(test, not(any(shuttle, atoll_loom, miri))))]
    pub(super) fn slots_at_rest(&self) -> usize {
        self.dispatch().slots_at_rest()
    }

    /// Let-goes of slot `j`'s rest wait that found a taker waiting.
    #[cfg(all(test, not(any(shuttle, atoll_loom, miri))))]
    pub(super) fn slot_rest_woke(&self, j: usize) -> u64 {
        self.dispatch().slot_rest_woke(j)
    }

    /// Pauses the next slot record take or release to reach `at` (D2c §3.8): the record there, and
    /// the sender that lets it go on.
    #[cfg(all(test, not(any(shuttle, atoll_loom, miri))))]
    pub(super) fn pause_slot(
        &self,
        at: dispatch::SlotPause,
    ) -> (
        std::sync::mpsc::Receiver<usize>,
        std::sync::mpsc::Sender<()>,
    ) {
        self.dispatch().pause_slot(at)
    }

    /// Pauses the next queued frame request between its backend call's return and the branch
    /// after it, for a test that orders its own look after that branch (#4900).
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn pause_frame_returned(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<usize>,
        std::sync::mpsc::Sender<()>,
    ) {
        self.dispatch().pause_frame_returned()
    }

    /// A note of the next queued frame request's branch after its backend call, once run.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn on_frame_served(&self) -> std::sync::mpsc::Receiver<usize> {
        self.dispatch().on_frame_served()
    }

    /// Holds the IO thread before its next claim of a record, until the sender sends or drops.
    #[cfg(test)]
    pub(super) fn hold_io_thread(&self) -> std::sync::mpsc::Sender<()> {
        self.dispatch().hold_next_claim()
    }

    /// A note of an IO thread's next idle, sent once it has marked itself idle.
    #[cfg(test)]
    pub(super) fn on_io_idle(&self) -> std::sync::mpsc::Receiver<()> {
        self.dispatch().on_idle()
    }

    /// Holds the next IO thread woken before its take, until the sender sends or drops.
    #[cfg(test)]
    pub(super) fn hold_next_take(&self) -> std::sync::mpsc::Sender<()> {
        self.dispatch().hold_next_take()
    }

    /// Whether a request made on this thread is queued.
    #[cfg(test)]
    pub(super) fn queues_here(&self) -> bool {
        self.dispatch().queues_here()
    }

    /// The dispatch, weakly, for a test's look after every owner has gone.
    #[cfg(test)]
    pub(super) fn dispatch_weak(&self) -> crate::sync::Weak<Dispatch> {
        Arc::downgrade(self.dispatch())
    }
}

impl Drop for Requests {
    /// The last resort when no drain ran: the IO threads stopped and joined, every request still
    /// READY finished as `Aborted` by one on its way out, as nothing is left to count it.
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.stop_io_threads();
    }
}

/// An IO whose files count their requests in a database's [`Requests`]. Every other call goes to
/// the IO it wraps.
pub(super) struct Counted {
    io: Arc<dyn IO>,
    requests: Arc<Requests>,
}

impl Counted {
    /// Wraps `io` for a database opened synchronously: one IO thread and no queued records.
    pub(super) fn new(io: Arc<dyn IO>, requests: Arc<Requests>) -> Result<Self> {
        Self::with_shape(io, requests, Shape::default())
    }

    /// Wraps `io`, counting in `requests`, whose records it makes in `shape` and whose IO threads
    /// it starts: a thread that does not start fails the open, before any file is opened, the
    /// threads already started stopped and joined with the requests.
    pub(super) fn with_shape(
        io: Arc<dyn IO>,
        requests: Arc<Requests>,
        shape: Shape,
    ) -> Result<Self> {
        if !(1..=dispatch::MOST_THREADS).contains(&shape.io_threads) {
            return Err(LimboError::InvalidArgument(format!(
                "{} IO threads: a database takes 1 to {}",
                shape.io_threads,
                dispatch::MOST_THREADS
            )));
        }
        if shape.meta_reads > dispatch::MOST_RECORDS - dispatch::QUEUED {
            return Err(LimboError::InvalidArgument(format!(
                "{} metadata read records: a database takes at most {}",
                shape.meta_reads,
                dispatch::MOST_RECORDS - dispatch::QUEUED
            )));
        }
        if shape.frames > dispatch::MOST_RECORDS - dispatch::QUEUED - shape.meta_reads {
            return Err(LimboError::InvalidArgument(format!(
                "{} frame records beside {} metadata read records: a database takes at most {} in all",
                shape.frames,
                shape.meta_reads,
                dispatch::MOST_RECORDS - dispatch::QUEUED
            )));
        }
        if shape.slots > dispatch::MOST_RECORDS - dispatch::QUEUED - shape.meta_reads - shape.frames
        {
            return Err(LimboError::InvalidArgument(format!(
                "{} slot records beside {} metadata read and {} frame records: a database takes \
                 at most {} in all",
                shape.slots,
                shape.meta_reads,
                shape.frames,
                dispatch::MOST_RECORDS - dispatch::QUEUED
            )));
        }
        let dispatch = Arc::new(Dispatch::new(&requests, shape));
        #[cfg(test)]
        MADE.with(|m| {
            *m.borrow_mut() = Some((Arc::downgrade(&dispatch), Arc::downgrade(&requests)))
        });
        let set = requests.dispatch.set(dispatch.clone());
        assert!(set.is_ok(), "one Counted per database's requests");
        for k in 0..dispatch.io_threads() {
            let handle = Dispatch::spawn(&dispatch, k)?;
            requests.io_threads.lock().push(handle);
        }
        Ok(Self { io, requests })
    }

    /// Whether a request made on this thread is queued (D2a §1).
    pub(super) fn queues(&self) -> bool {
        self.requests.dispatch().queues()
    }

    /// Whether the database was opened async (D2a §1).
    pub(super) fn opened_async(&self) -> bool {
        self.requests.dispatch().opened_async()
    }

    /// The metadata reads' records: one per read credit the cache may make.
    pub(super) fn meta_read_records(&self) -> usize {
        self.requests.dispatch().meta_reads()
    }

    /// The frame records: one per grain of the pool the database was opened with (D2b §1).
    pub(super) fn frame_records(&self) -> usize {
        self.requests.dispatch().frame_records()
    }

    /// The pool, as its frame records' owner: set once at its open, before any frame can queue.
    pub(super) fn attach_frames_owner(&self, owner: crate::sync::Weak<dyn FrameOwner>) {
        self.requests.dispatch().attach_frames_owner(owner);
    }

    /// Queues the pool's request `c` for `place` in the record of `held`'s frame (D2b §3, §4);
    /// refused before accepting it, the held record back.
    pub(super) fn queue_frame(
        &self,
        held: crate::storage::atoll::buffer::HeldRecord,
        place: Place,
        c: Completion,
    ) -> std::result::Result<(), FrameRefused> {
        self.requests
            .dispatch()
            .queue_frame(&self.requests, held, place, c)
    }

    /// Queues a rollback's read-back read `c` at `pos` of the log segment `file`, in transaction
    /// slot `slot`'s record (D2c §2): done once queued, or the record's rest wait while its last
    /// read is not yet let go.
    pub(super) fn queue_slot_read(
        &self,
        slot: usize,
        file: Arc<CountedFile>,
        pos: u64,
        c: Completion,
    ) -> Result<crate::types::IOResult<()>> {
        self.requests
            .dispatch()
            .queue_slot_read(&self.requests, slot, file, pos, c)
    }

    /// Queues the read `c` at `pos` of `source` under `credit`, in its record (D2a §3, §4).
    pub(super) fn queue_meta_read(
        &self,
        credit: crate::storage::atoll::meta::Credit,
        source: Source,
        pos: u64,
        c: Completion,
    ) -> Result<()> {
        self.requests
            .dispatch()
            .queue_read(&self.requests, credit, source, pos, c)
    }

    /// Opens `path` as a counted file its owner holds typed: the one kind of file a follow-on is
    /// made on ([`CountedFile::write_then`]).
    pub(super) fn open_counted(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> Result<Arc<CountedFile>> {
        refuse_in_frame("a file's open");
        let file = self.io.open_file(path, flags, direct)?;
        Ok(Arc::new(CountedFile {
            file,
            requests: self.requests.clone(),
        }))
    }

    /// The log these requests' dispatch serves (D1b): one per database, set once at its open.
    pub(super) fn attach_log(&self, log: crate::sync::Weak<dyn LogWork>) {
        self.requests.dispatch().attach_log(log);
    }

    /// The log's ring (D1b §3.1), from a retirement observer running before its request is
    /// uncounted.
    pub(super) fn ring_log(&self) {
        self.requests.dispatch().ring_log(&self.requests);
    }

    /// The log's ring by a caller inside a cooperative frame (D2c §1): refused once the database
    /// is closing.
    pub(super) fn ring_log_from_frame(&self) -> Result<()> {
        self.requests.dispatch().ring_log_from_frame(&self.requests)
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

    /// A note of the IO thread's end.
    #[cfg(test)]
    pub(super) fn on_io_exit(&self) -> std::sync::mpsc::Receiver<()> {
        self.requests.dispatch().on_exit()
    }

    /// The log's doorbell bit in the IO thread's word, after the records (D1b §3.1).
    #[cfg(test)]
    pub(super) fn log_bit() -> usize {
        dispatch::LOG
    }

    /// The log door's bits now (D1b §3.1): `RUNG` 2, `SERVING` 1.
    #[cfg(test)]
    pub(super) fn log_door(&self) -> u8 {
        self.requests.dispatch().door()
    }

    /// The log units the IO thread has run, counted there (D1b §4).
    #[cfg(test)]
    pub(super) fn log_units(&self) -> usize {
        self.requests.dispatch().units()
    }

    /// The log units wholly ended, each one's count let go (D1b §4).
    #[cfg(test)]
    pub(super) fn log_units_ended(&self) -> usize {
        self.requests.dispatch().units_ended()
    }

    /// A note of each log unit's end from the next on: the door then, and the requests counted.
    #[cfg(test)]
    pub(super) fn on_log_unit_end(&self) -> std::sync::mpsc::Receiver<(u8, usize)> {
        self.requests.dispatch().on_unit_end()
    }

    /// Pauses the next quiet log unit's end between its door going quiet and its count's release:
    /// the door and the count there, and the sender that lets it go on.
    #[cfg(test)]
    pub(super) fn pause_log_end_gap(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<(u8, usize)>,
        std::sync::mpsc::Sender<()>,
    ) {
        self.requests.dispatch().pause_end_gap()
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
        refuse_in_frame("a file's open_file");
        let file = self.io.open_file(path, flags, direct)?;
        Ok(Arc::new(CountedFile {
            file,
            requests: self.requests.clone(),
        }))
    }

    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        refuse_in_frame("a file's open_shared_wal_file");
        let file = self.io.open_shared_wal_file(path)?;
        Ok(Arc::new(CountedFile {
            file,
            requests: self.requests.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        refuse_in_frame("a file's remove_file");
        self.io.remove_file(path)
    }

    fn sync_directory(&self, path: &str) -> Result<()> {
        refuse_in_frame("a file's sync_directory");
        self.io.sync_directory(path)
    }

    fn rename_file(&self, from: &str, to: &str) -> Result<bool> {
        refuse_in_frame("a file's rename_file");
        self.io.rename_file(from, to)
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        self.io.supports_shared_wal_coordination()
    }

    fn step(&self) -> Result<()> {
        refuse_in_frame("a blocking host's step");
        self.io.step()
    }

    fn try_step(&self, key: crate::io::ReadyKey, ready: &std::task::Waker) -> Result<Stepped> {
        refuse_in_frame("a poll's step");
        self.io.try_step(key, ready)
    }

    fn says_what_a_step_found(&self) -> bool {
        self.io.says_what_a_step_found()
    }

    fn withdraw(&self, key: crate::io::ReadyKey) {
        refuse_in_frame("a waker's withdrawal");
        self.io.withdraw(key)
    }

    fn delivery(&self) -> crate::io::Delivery {
        self.io.delivery()
    }

    fn wait(&self, c: &Completion) {
        refuse_in_frame("a wait for a completion");
        crate::io::refuse_on_any_delivering_thread("a wait for a completion");
        self.io.wait(c)
    }

    fn cancel(&self, c: &[Completion]) -> Result<()> {
        refuse_in_frame("a backend's cancel");
        self.io.cancel(c)
    }

    fn wait_retired(&self, c: &Completion) -> Result<()> {
        refuse_in_frame("a wait for retirement");
        crate::io::refuse_on_any_delivering_thread("a wait for retirement");
        self.io.wait_retired(c)
    }

    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        refuse_in_frame("a wait for retirement");
        crate::io::refuse_on_any_delivering_thread("a wait for retirement");
        self.io.drain_completions(completions)
    }

    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        refuse_in_frame("a wait for a completion");
        crate::io::refuse_on_any_delivering_thread("a wait for a completion");
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
        refuse_in_frame("a buffer's registration");
        self.io.register_fixed_buffer(ptr, len)
    }

    fn yield_now(&self) {
        self.io.yield_now()
    }

    fn sleep(&self, duration: std::time::Duration) {
        refuse_in_frame("a sleep");
        crate::io::refuse_on_any_delivering_thread("a sleep");
        self.io.sleep(duration)
    }

    fn file_id(&self, path: &str) -> Result<FileId> {
        refuse_in_frame("a file's identity");
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
    /// Submits `submit`'s request through the file this wraps, counted once; `then` runs at the
    /// request's retirement, before it is uncounted (D1b §2): the log's writes and sync members,
    /// whose retirement observers ring the log's doorbell while their request still counts. A
    /// refused submission runs nothing and is uncounted, never accepted.
    pub(super) fn submit_then(
        &self,
        physical: Completion,
        submit: impl FnOnce(&dyn File) -> Result<Completion>,
        then: impl FnOnce() + Send + 'static,
    ) -> Result<Completion> {
        refuse_in_frame("a counted file's request");
        self.requests
            .submit_with(physical, || submit(&*self.file), then)
    }

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
        refuse_in_frame("a counted file's write");
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

/// Ends the suite, in a test build, where a cooperative frame reaches `what` in place: a call that
/// may wait on its caller's thread, in a system call, for a request or for time (DESIGN.md 22.18,
/// *Which calls can block*; D2a §5; every method is ruled in `requests/frame_census.rs`). A route
/// that reaches one is to be queued in its owner's record. A release build runs it.
#[inline]
fn refuse_in_frame(what: &str) {
    #[cfg(test)]
    assert!(
        !crate::storage::atoll::cooperative::inside(),
        "{what} run in place inside a cooperative frame, where it is queued (DESIGN.md 22.18)"
    );
    #[cfg(not(test))]
    let _ = what;
}

impl Drop for CountedFile {
    /// Its descriptor closes here (unlocked and closed by the file it wraps), never inside a
    /// cooperative frame (DESIGN.md 22.18: every holder of a file drops on a blocking host).
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        refuse_in_frame("a counted file's close");
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
        refuse_in_frame("a counted file's lock");
        self.file.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        refuse_in_frame("a counted file's unlock");
        self.file.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        refuse_in_frame("a counted file's read");
        self.requests.submit(c.clone(), || self.file.pread(pos, c))
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        refuse_in_frame("a counted file's write");
        self.requests
            .submit(c.clone(), || self.file.pwrite(pos, buffer, c))
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        refuse_in_frame("a counted file's sync");
        self.requests
            .submit(c.clone(), || self.file.sync(c, sync_type))
    }

    fn sync_data(&self, c: Completion) -> Result<Completion> {
        refuse_in_frame("a counted file's sync");
        self.requests.submit(c.clone(), || self.file.sync_data(c))
    }

    fn supports_durable_writes(&self) -> bool {
        self.file.supports_durable_writes()
    }

    fn pwrite_durable(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        refuse_in_frame("a counted file's write");
        self.requests
            .submit(c.clone(), || self.file.pwrite_durable(pos, buffer, c))
    }

    fn allocate(&self, pos: u64, len: u64) -> Result<()> {
        refuse_in_frame("a counted file's allocation");
        self.file.allocate(pos, len)
    }

    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        refuse_in_frame("a counted file's write");
        self.requests
            .submit(c.clone(), || self.file.pwritev(pos, buffers, c))
    }

    fn size(&self) -> Result<u64> {
        refuse_in_frame("a counted file's size");
        self.file.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        refuse_in_frame("a counted file's truncation");
        self.requests
            .submit(c.clone(), || self.file.truncate(len, c))
    }

    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        refuse_in_frame("a counted file's hole probe");
        self.file.has_hole(pos, len)
    }

    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        refuse_in_frame("a counted file's hole punch");
        self.file.punch_hole(pos, len)
    }

    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        refuse_in_frame("a counted file's shared WAL lock");
        crate::io::refuse_on_any_delivering_thread("a shared WAL byte's lock");
        self.file.shared_wal_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        refuse_in_frame("a counted file's shared WAL lock");
        self.file.shared_wal_try_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_probe_exclusive_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        refuse_in_frame("a counted file's shared WAL probe");
        self.file.shared_wal_probe_exclusive_byte(offset, kind)
    }

    fn shared_wal_probe_exclusive_while_shared_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        refuse_in_frame("a counted file's shared WAL probe");
        // Takes its shared lock back with a wait for the byte's other holder.
        crate::io::refuse_on_any_delivering_thread("a shared WAL byte's lock");
        self.file
            .shared_wal_probe_exclusive_while_shared_byte(offset, kind)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        refuse_in_frame("a counted file's shared WAL unlock");
        self.file.shared_wal_unlock_byte(offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        refuse_in_frame("a counted file's shared WAL resize");
        self.file.shared_wal_set_len(len)
    }

    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        refuse_in_frame("a counted file's shared WAL map");
        self.file.shared_wal_map(offset, len)
    }
}

/// A counted IO over `io` for a test that builds an owner on its own, outside an open: its IO
/// thread is stopped and joined by its requests' last drop.
#[cfg(test)]
pub(super) fn counted_for_test(io: Arc<dyn IO>) -> Arc<Counted> {
    Arc::new(Counted::new(io, Arc::new(Requests::default())).expect("a test's IO thread"))
}

/// A counted IO over `io` for a database opened async (D2a): `meta_reads` metadata read records and
/// `io_threads` IO threads, stopped and joined by its requests' last drop.
#[cfg(test)]
pub(super) fn counted_async_for_test(
    io: Arc<dyn IO>,
    meta_reads: usize,
    io_threads: usize,
) -> Arc<Counted> {
    Arc::new(
        Counted::with_shape(
            io,
            Arc::new(Requests::default()),
            Shape {
                opened_async: true,
                io_threads,
                meta_reads,
                frames: 0,
                slots: 0,
            },
        )
        .expect("a test's IO threads"),
    )
}

/// [`counted_async_for_test`] with `frames` frame records, for a pool of that many grains (D2b):
/// the frame tests' own, native only.
#[cfg(all(test, not(any(shuttle, atoll_loom))))]
pub(super) fn counted_async_with_frames_for_test(
    io: Arc<dyn IO>,
    meta_reads: usize,
    frames: usize,
    io_threads: usize,
) -> Arc<Counted> {
    Arc::new(
        Counted::with_shape(
            io,
            Arc::new(Requests::default()),
            Shape {
                opened_async: true,
                io_threads,
                meta_reads,
                frames,
                slots: 0,
            },
        )
        .expect("a test's IO threads"),
    )
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
    /// The pool's request in the last frame record queued on this thread, for a test to cancel
    /// as a backend's cancel would (D2b §7).
    pub(super) static LAST_FRAME_REQUEST: std::cell::RefCell<Option<Completion>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod followon_tests;
#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod frame_census;
#[cfg(test)]
mod tests;
#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod tree_tests;
