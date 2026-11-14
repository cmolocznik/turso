//! The log's follow-on work on the IO thread's unit (D1b §7), over a probe whose files record each
//! read, write and sync at their own methods, with the thread, the depth of backend calls on it and the
//! IO thread's unit then, and hold what a test names until it answers. Every wait is bounded, and
//! every Atoll object is let go before an assertion.

use super::tests::{settled, settled_by, SETTLE};
use super::*;
use crate::io::{Buffer, Completion, File, FileSyncType, MemoryIO, OpenFlags, IO};
use crate::storage::atoll::locks::Mutex;
use crate::storage::atoll::requests::{Requests, SlotPause};
use crate::storage::atoll::testing::drive;
use crate::sync::atomic::AtomicBool;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::time::Instant;

const BASE: &str = "/db/unit";
/// The database's IO thread, by name.
const IO_THREAD: &str = "atoll-io";
/// The door's bits (D1b §3.1).
const RUNG: u8 = 1 << 1;
const SERVING: u8 = 1 << 0;

/// A backend call: which, on which file and thread, at what depth of backend calls on that thread,
/// and in which of the IO thread's log units (its count then).
#[derive(Clone, Debug)]
struct Call {
    what: &'static str,
    path: String,
    thread: String,
    depth: usize,
    unit: usize,
}

crate::thread::thread_local! {
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// What the probe's files do and what they saw.
struct Probe {
    durable: bool,
    hold_writes: AtomicBool,
    hold_syncs: AtomicBool,
    /// Holds the syncs the IO thread makes alone: a unit's groups, never the preparer's.
    hold_io_syncs: AtomicBool,
    /// Holds the reads the IO thread makes: each delivered at once, and still owned until the test
    /// lets it go.
    hold_io_reads: AtomicBool,
    queue: Mutex<VecDeque<(Completion, i32)>>,
    /// The reads delivered and still owned, oldest first.
    held_reads: Mutex<Vec<Completion>>,
    calls: Mutex<Vec<Call>>,
    refuse_sync_of: Mutex<Option<String>>,
    counted: Mutex<Option<Weak<Counted>>>,
    /// Writes longer than a block write half of it in whole blocks, at least one, and say so.
    halve: AtomicBool,
    /// Writes delivered only when the IO is stepped, one a step, and a step saying what it found,
    /// a waiter registered at a step that found none woken by the next write: a pumped
    /// backend's. Set before the log opens, so the database is shaped for it.
    defer: AtomicBool,
    deferred: Mutex<VecDeque<(Completion, i32)>>,
    registered: Mutex<Vec<(crate::io::ReadyKey, std::task::Waker)>>,
    /// The write calls made since a test set the count to 0, and the one of them, numbered from
    /// 1, that is held, refused, or writes nothing.
    write_calls: AtomicUsize,
    hold_call: AtomicUsize,
    refuse_call: AtomicUsize,
    zero_call: AtomicUsize,
}

impl Probe {
    fn hold_all(&self, on: bool) {
        self.hold_writes.store(on, Ordering::SeqCst);
        self.hold_syncs.store(on, Ordering::SeqCst);
    }

    fn hold(&self, c: Completion, result: i32) -> Result<Completion> {
        c.accept();
        self.queue.lock().push_back((c.clone(), result));
        Ok(c)
    }

    /// The reads delivered and still owned.
    fn reads_held(&self) -> usize {
        self.held_reads
            .lock()
            .iter()
            .filter(|c| c.is_owned())
            .count()
    }

    /// Retires every read delivered and still owned.
    fn let_reads_go(&self) {
        let held = std::mem::take(&mut *self.held_reads.lock());
        for c in held {
            if c.is_owned() {
                c.retire();
            }
        }
    }

    /// The oldest request held, taken out: the test answers it, then lets it go.
    fn oldest(&self) -> Option<(Completion, i32)> {
        self.queue.lock().pop_front()
    }

    /// What each request held asks for, oldest first: a write's length, or 0 for a sync.
    fn queued(&self) -> Vec<i32> {
        self.queue.lock().iter().map(|(_, r)| *r).collect()
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().clone()
    }

    fn clear(&self) {
        self.calls.lock().clear();
    }

    fn syncs(&self) -> usize {
        self.calls
            .lock()
            .iter()
            .filter(|c| c.what == "sync")
            .count()
    }

    /// The IO thread's log units run so far, once the log is watched.
    fn unit(&self) -> usize {
        self.counted
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .map_or(0, |c| c.log_units())
    }
}

/// MemoryIO, which delivers inline, behind the probe.
struct UnitIO {
    inner: Arc<MemoryIO>,
    probe: Arc<Probe>,
}

impl UnitIO {
    fn new(durable: bool) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryIO::new()),
            probe: Arc::new(Probe {
                durable,
                hold_writes: AtomicBool::new(false),
                hold_syncs: AtomicBool::new(false),
                hold_io_syncs: AtomicBool::new(false),
                hold_io_reads: AtomicBool::new(false),
                queue: Mutex::new(VecDeque::new()),
                held_reads: Mutex::new(Vec::new()),
                calls: Mutex::new(Vec::new()),
                refuse_sync_of: Mutex::new(None),
                counted: Mutex::new(None),
                halve: AtomicBool::new(false),
                defer: AtomicBool::new(false),
                deferred: Mutex::new(VecDeque::new()),
                registered: Mutex::new(Vec::new()),
                write_calls: AtomicUsize::new(0),
                hold_call: AtomicUsize::new(0),
                refuse_call: AtomicUsize::new(0),
                zero_call: AtomicUsize::new(0),
            }),
        })
    }
}

impl crate::io::Clock for UnitIO {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for UnitIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        Ok(Arc::new(UnitFile {
            file: self.inner.open_file(path, flags, direct)?,
            path: path.to_string(),
            probe: self.probe.clone(),
        }))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn sync_directory(&self, path: &str) -> Result<()> {
        self.inner.sync_directory(path)
    }
    fn rename_file(&self, from: &str, to: &str) -> Result<bool> {
        self.inner.rename_file(from, to)
    }
    fn delivery(&self) -> crate::io::Delivery {
        if self.probe.defer.load(Ordering::SeqCst) {
            return crate::io::Delivery::Pumped;
        }
        self.inner.delivery()
    }
    fn step(&self) -> Result<()> {
        if self.probe.defer.load(Ordering::SeqCst) {
            self.probe.deliver_one();
            return Ok(());
        }
        self.inner.step()
    }
    fn try_step(
        &self,
        key: crate::io::ReadyKey,
        ready: &std::task::Waker,
    ) -> Result<crate::io::Stepped> {
        if !self.probe.defer.load(Ordering::SeqCst) {
            return self.inner.try_step(key, ready);
        }
        if self.probe.deliver_one() {
            return Ok(crate::io::Stepped::Delivered);
        }
        // Registered under the lock a write takes to wake: one queued after the look above finds
        // the registration.
        let mut registered = self.probe.registered.lock();
        if !self.probe.deferred.lock().is_empty() {
            return Ok(crate::io::Stepped::Delivered);
        }
        registered.retain(|(k, _)| *k != key);
        registered.push((key, ready.clone()));
        Ok(crate::io::Stepped::Empty)
    }
    fn says_what_a_step_found(&self) -> bool {
        self.probe.defer.load(Ordering::SeqCst) || self.inner.says_what_a_step_found()
    }
    fn withdraw(&self, key: crate::io::ReadyKey) {
        if self.probe.defer.load(Ordering::SeqCst) {
            self.probe.registered.lock().retain(|(k, _)| *k != key);
            return;
        }
        self.inner.withdraw(key)
    }
}

struct UnitFile {
    file: Arc<dyn File>,
    path: String,
    probe: Arc<Probe>,
}

impl UnitFile {
    /// Runs `f`, the backend's own call, recorded with its thread, its depth among backend calls
    /// on that thread and the IO thread's unit then.
    fn call<R>(&self, what: &'static str, f: impl FnOnce() -> R) -> R {
        let depth = DEPTH.with(|d| {
            d.set(d.get() + 1);
            d.get()
        });
        self.probe.calls.lock().push(Call {
            what,
            path: self.path.clone(),
            thread: this_thread(),
            depth,
            unit: self.probe.unit(),
        });
        let r = f();
        DEPTH.with(|d| d.set(d.get() - 1));
        r
    }
}

impl File for UnitFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }
    fn pread(&self, at: u64, c: Completion) -> Result<Completion> {
        self.call("pread", || {
            if !(self.probe.hold_io_reads.load(Ordering::SeqCst) && this_thread() == IO_THREAD) {
                return self.file.pread(at, c);
            }
            // Delivered now, its callback through `c`, and `c` still owned until the test lets it
            // go: a backend that owns a request after delivering it, as a ring's entry is owned
            // until it is reaped. Listed only once delivered, so a test that finds it listed finds
            // the delivery's callbacks run.
            c.accept();
            let owner = c.clone();
            let inner = Completion::new_read(c.as_read().buf_arc(), move |res| {
                match res {
                    Ok((_, n)) => owner.complete(n),
                    Err(e) => owner.error(e),
                }
                None
            });
            // The probe's memory backend refuses nothing: a refusal here is the fixture broken, and
            // no request it accepted may be finished or retired as though it ended.
            if self.file.pread(at, inner).is_err() {
                crate::io::fatal("log unit tests: the probe's memory backend refused a read");
            }
            self.probe.held_reads.lock().push(c.clone());
            Ok(c)
        })
    }
    fn pwrite(&self, at: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        if let Some(shaped) = self.shaped("pwrite", at, &buffer, &c) {
            return shaped;
        }
        let len = buffer.len();
        self.call("pwrite", || {
            if self.probe.hold_writes.load(Ordering::SeqCst) {
                // The bytes land at once; the request is held until the test answers it.
                drop(
                    self.file
                        .pwrite(at, buffer.clone(), Completion::new_write(|_| {}))?,
                );
                c.keep_write_buffer_alive(buffer);
                self.probe.hold(c, len as i32)
            } else {
                self.file.pwrite(at, buffer, c)
            }
        })
    }
    fn pwrite_durable(&self, at: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        if let Some(shaped) = self.shaped("pwrite_durable", at, &buffer, &c) {
            return shaped;
        }
        let len = buffer.len();
        self.call("pwrite_durable", || {
            if self.probe.hold_writes.load(Ordering::SeqCst) {
                drop(self.file.pwrite_durable(
                    at,
                    buffer.clone(),
                    Completion::new_write(|_| {}),
                )?);
                c.keep_write_buffer_alive(buffer);
                self.probe.hold(c, len as i32)
            } else {
                self.file.pwrite_durable(at, buffer, c)
            }
        })
    }
    fn supports_durable_writes(&self) -> bool {
        self.probe.durable
    }
    fn sync(&self, c: Completion, how: FileSyncType) -> Result<Completion> {
        if self.probe.refuse_sync_of.lock().as_deref() == Some(self.path.as_str()) {
            return self.call("sync_refused", || {
                Err(LimboError::CompletionError(CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "a probe's sync refused",
                )))
            });
        }
        self.call("sync", || {
            let held = self.probe.hold_syncs.load(Ordering::SeqCst)
                || (self.probe.hold_io_syncs.load(Ordering::SeqCst) && this_thread() == IO_THREAD);
            if held {
                self.probe.hold(c, 0)
            } else {
                self.file.sync(c, how)
            }
        })
    }
    fn size(&self) -> Result<u64> {
        self.file.size()
    }
    fn truncate(&self, n: u64, c: Completion) -> Result<Completion> {
        self.file.truncate(n, c)
    }
}

impl UnitFile {
    /// A write the probe shapes, when a test asks it to: refused; or written in part, half of it
    /// in whole blocks, or nothing, and said so, then delivered at once as memory delivers, held
    /// for the test, or deferred to a step. `None` when the write goes to memory as it is.
    fn shaped(
        &self,
        what: &'static str,
        at: u64,
        buffer: &Arc<Buffer>,
        c: &Completion,
    ) -> Option<Result<Completion>> {
        let probe = &self.probe;
        let n = probe.write_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let refused = probe.refuse_call.load(Ordering::SeqCst) == n;
        let zero = probe.zero_call.load(Ordering::SeqCst) == n;
        let held = probe.hold_call.load(Ordering::SeqCst) == n;
        let halve = probe.halve.load(Ordering::SeqCst);
        let defer = probe.defer.load(Ordering::SeqCst);
        if !(refused || zero || held || halve || defer) {
            return None;
        }
        Some(self.call(what, || {
            if refused {
                return Err(LimboError::CompletionError(CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "a probe's write refused",
                )));
            }
            let len = buffer.len();
            let wrote = if zero {
                0
            } else if halve && len > BLOCK as usize {
                (len / 2 / BLOCK as usize * BLOCK as usize).max(BLOCK as usize)
            } else {
                len
            };
            if wrote > 0 {
                let part = Arc::new(Buffer::new(buffer.as_slice()[..wrote].to_vec()));
                drop(self.file.pwrite(at, part, Completion::new_write(|_| {}))?);
            }
            c.keep_write_buffer_alive(buffer.clone());
            if held {
                return probe.hold(c.clone(), wrote as i32);
            }
            if defer {
                c.accept();
                probe.deferred.lock().push_back((c.clone(), wrote as i32));
                let woken = std::mem::take(&mut *probe.registered.lock());
                for (_, waker) in woken {
                    waker.wake();
                }
                return Ok(c.clone());
            }
            c.deliver(Ok(wrote as i32));
            Ok(c.clone())
        }))
    }
}

impl Probe {
    /// Delivers the oldest deferred write, as a pumped backend's step does: whether there was one.
    fn deliver_one(&self) -> bool {
        let next = self.deferred.lock().pop_front();
        let Some((c, n)) = next else {
            return false;
        };
        c.complete(n);
        c.retire();
        true
    }
}

fn this_thread() -> String {
    crate::thread::current()
        .name()
        .unwrap_or("unnamed")
        .to_string()
}

/// The calls made on a thread named `thread`: which, and at what depth.
fn on(calls: &[Call], thread: &str) -> Vec<(&'static str, usize)> {
    calls
        .iter()
        .filter(|c| c.thread == thread)
        .map(|c| (c.what, c.depth))
        .collect()
}

fn small() -> LogConfig {
    LogConfig {
        segment_bytes: 64 << 10,
        buffer_bytes: 32 << 10,
        chunk_bytes: 8 << 10,
        writes_in_flight: 3,
        direct: true,
    }
}

fn header() -> Header {
    Header {
        kind: Kind::new(1).unwrap(),
        flags: 0,
        txn: 1,
        prev: None,
    }
}

/// A log opened alone on `io`, its probe watching its IO thread's units.
fn open(io: &Arc<UnitIO>, config: LogConfig) -> AloneLog {
    let log = Log::open(io.clone(), BASE, config, 0).unwrap();
    *io.probe.counted.lock() = Some(Arc::downgrade(log.segments.counted_io()));
    log
}

/// Appends a record of `payload` bytes, admitted for itself.
fn append(log: &Arc<Log>, io: &Arc<dyn IO>, payload: usize) -> Lsn {
    let p = vec![7u8; payload];
    let mut admitted = drive(io, || {
        log.admit(&mut LogWait::default(), framed_len(payload))
    })
    .unwrap();
    log.append(&mut admitted, &header(), &[&p])
}

/// Runs the preparer until it has nothing left to step, with nothing held.
fn prepare(log: &Log) {
    for _ in 0..10_000 {
        if log.run_preparer().is_none() {
            return;
        }
    }
}

/// A first block appended and made durable, the next segment prepared and the units ended:
/// whether they ended.
fn durable_start(log: &Arc<Log>, io: &Arc<dyn IO>) -> bool {
    while log.end() < BLOCK {
        append(log, io, 996);
    }
    let end = log.end();
    drive(io, || log.wait_durable(&mut LogWait::default(), end)).unwrap();
    prepare(log);
    settled(log)
}

/// Answers and lets go of every request the probe holds, then of every one the log's units make
/// once those retire, until the units, quiet, make none: whether that ended within one [`SETTLE`]
/// for the whole of it.
fn answer_all(log: &Log, probe: &Probe) -> bool {
    let deadline = Instant::now() + SETTLE;
    loop {
        while let Some((c, r)) = probe.oldest() {
            c.complete(r);
            c.retire();
            if Instant::now() >= deadline {
                return false;
            }
        }
        if !settled_by(log, deadline) {
            return false;
        }
        if probe.queued().is_empty() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
    }
}

/// What a closer decided at its first look past the count: to park on it, or to seal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decided {
    Parks,
    Sealed,
}

/// A closer on a thread of its own, draining `requests` as close's step 4 does, with what it was
/// seen to do: its first decision, told at the point it parks or at its successful seal, whichever
/// it reaches first, and its end. `sealed` runs at the seal.
struct Closer {
    decided: mpsc::Receiver<Decided>,
    done: mpsc::Receiver<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Closer {
    fn start(
        requests: Arc<Requests>,
        io: Arc<dyn IO>,
        sealed: impl FnOnce() + Send + 'static,
    ) -> Self {
        let (decided_tx, decided) = mpsc::channel();
        let (done_tx, done) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let parks = decided_tx.clone();
            crate::race::at("requests.drain_parks", move || {
                let _ = parks.send(Decided::Parks);
            });
            crate::race::at("requests.drain_sealed", move || {
                sealed();
                let _ = decided_tx.send(Decided::Sealed);
            });
            requests.drain(&*io);
            let _ = done_tx.send(());
        });
        Self {
            decided,
            done,
            thread: Some(thread),
        }
    }

    /// Its next decision, within [`SETTLE`].
    fn next(&self) -> Option<Decided> {
        self.decided.recv_timeout(SETTLE).ok()
    }

    /// Whether it ended within [`SETTLE`]: joined if it did, and left running if not, so a closer
    /// that never seals is a failure to report, not a wait.
    fn ended(&mut self) -> bool {
        let done = self.done.recv_timeout(SETTLE).is_ok();
        if done {
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
        done
    }
}

/// D1b §7.1: on a backend that delivers inline, files that do not write durably: a commit's one
/// write is its caller's only backend call, at depth 1; the sync it owes at the write's retirement
/// is made by the IO thread's unit, at depth 1 there; and the commit is durable once that unit has
/// ended, read through the unit's end alone, the caller making no further flush or wait (mutant:
/// `written` starts the sync, which then runs on the caller at depth 2).
#[test]
fn a_commits_sync_is_made_by_the_io_threads_unit_and_its_caller_makes_one_call() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, small());
    let mut quiet = durable_start(&log, &dyn_io);
    io.probe.clear();
    let lsn = append(&log, &dyn_io, 100);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, lsn);
    quiet &= settled(&log);
    let durable = log.durable() >= lsn;
    let calls = io.probe.calls();
    drop(wait);
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert_eq!(
        on(&calls, &this_thread()),
        vec![("pwrite", 1)],
        "the caller's backend calls: {calls:?}"
    );
    assert_eq!(
        on(&calls, IO_THREAD),
        vec![("sync", 1)],
        "the IO thread's backend calls: {calls:?}"
    );
    assert!(durable, "the commit not durable once the unit ended");
}

/// D1b §7.2: a write's callback submits nothing: answered and still owned, the write leaves no
/// sync submitted, no write planned and the log's door quiet; the sync appears once it retires
/// (mutant: `written` rings, and the unit that ring wakes starts the sync before the retirement).
#[test]
fn a_writes_callback_submits_nothing_and_its_sync_starts_once_it_retires() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, small());
    let mut quiet = durable_start(&log, &dyn_io);
    io.probe.hold_all(true);
    let a = append(&log, &dyn_io, 100);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, a);
    let write = io.probe.oldest();
    let held_write = write.as_ref().is_some_and(|(_, r)| *r > 0);
    if let Some((c, r)) = &write {
        c.complete(*r);
    }
    let door_answered = log.segments.counted_io().log_door();
    let queued_answered = io.probe.queued();
    if let Some((c, _)) = &write {
        c.retire();
    }
    quiet &= settled(&log);
    let queued_retired = io.probe.queued();
    quiet &= answer_all(&log, &io.probe);
    let durable = log.durable() >= a;
    drop(write);
    drop(wait);
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(held_write, "no write held");
    assert_eq!(door_answered, 0, "the door at the write's answer");
    assert_eq!(
        queued_answered,
        Vec::<i32>::new(),
        "a sync or a write made at the write's answer"
    );
    assert_eq!(
        queued_retired,
        vec![0],
        "the sync not started once the write retired"
    );
    assert!(durable);
}

/// D1b §7.3: the count across the hand-off, at two hold points, the IO thread held before its
/// claim so no unit starts: between the ring and Counted's uncount of the write, the write's count
/// and the door's (2); after the uncount, the door's (1), and a closer started there parks on it,
/// sealing only once the IO thread, let go, has run the unit, the log durable to its end (mutant:
/// the observer attached by `on_retired` after an ordinary submission, today's road: the write is
/// uncounted before the ring, so the first point reads 1).
#[test]
fn a_writes_count_spans_its_hand_off_to_the_unit_and_a_close_waits_for_the_unit() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, small());
    let quiet = durable_start(&log, &dyn_io);
    io.probe.hold_all(true);
    let a = append(&log, &dyn_io, 100);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, a);
    let write = io.probe.oldest();
    let requests = log.segments.counted_io().requests().clone();
    let hold = requests.hold_io_thread();
    let at_ring = std::rc::Rc::new(std::cell::Cell::new(None));
    {
        let (at_ring, requests) = (at_ring.clone(), requests.clone());
        crate::race::at("log.write_retired.rang", move || {
            at_ring.set(Some(requests.outstanding()));
        });
    }
    if let Some((c, r)) = &write {
        c.complete(*r);
        c.retire();
    }
    let after_uncount = requests.outstanding();
    let mut closer = Closer::start(requests.clone(), dyn_io.clone(), || {});
    let first = closer.next();
    // Let go with nothing held: the unit's sync lands inline.
    io.probe.hold_all(false);
    drop(hold);
    let then = (first == Some(Decided::Parks))
        .then(|| closer.next())
        .flatten();
    let closed = closer.ended();
    let durable = log.durable() >= a;
    drop(write);
    drop(wait);
    drop(requests);
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert_eq!(
        at_ring.get(),
        Some(2),
        "the requests counted between the ring and the write's uncount"
    );
    assert_eq!(after_uncount, 1, "the requests counted after the uncount");
    assert_eq!(
        first,
        Some(Decided::Parks),
        "the closer's decision with the door's count held"
    );
    assert_eq!(then, Some(Decided::Sealed), "the closer once let go");
    assert!(closed, "the closer did not end");
    assert!(durable, "the log not durable to its end");
}

/// A write held and its sync, then the next write: the sync and the write the first write's
/// retirement makes, both held, with the commits they serve.
fn a_sync_and_a_write_held(io: &Arc<UnitIO>, log: &Arc<Log>) -> (Lsn, LogWait, bool) {
    let dyn_io: Arc<dyn IO> = io.clone();
    let mut quiet = durable_start(log, &dyn_io);
    io.probe.hold_all(true);
    let a = append(log, &dyn_io, 100);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, a);
    // One write in flight on files that do not write durably: this one waits for the first.
    let b = append(log, &dyn_io, 100);
    let _ = log.wait_durable(&mut wait, b);
    if let Some((c, r)) = io.probe.oldest() {
        c.complete(r);
        c.retire();
    }
    quiet &= settled(log);
    (b, wait, quiet)
}

/// D1b §7.4 (a): a ring while the door is `RUNG` takes no count: with the IO thread held before
/// its claim, the sync's last member and the next write retire, each ringing, and the requests
/// count the door's one alone (mutant: every ring counts, which reads 2).
#[test]
fn a_ring_while_the_door_is_rung_takes_no_count() {
    let io = UnitIO::new(false);
    let log = open(&io, small());
    let (b, wait, mut quiet) = a_sync_and_a_write_held(&io, &log);
    let queued = io.probe.queued();
    let requests = log.segments.counted_io().requests().clone();
    let hold = requests.hold_io_thread();
    let sync = io.probe.oldest();
    let write = io.probe.oldest();
    for (c, r) in sync.iter().chain(write.iter()) {
        c.complete(*r);
        c.retire();
    }
    let counted = requests.outstanding();
    let door = log.segments.counted_io().log_door();
    drop(hold);
    quiet &= answer_all(&log, &io.probe);
    let durable = log.durable() >= b;
    drop((sync, write, wait, requests));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(
        queued.len() == 2 && queued[0] == 0 && queued[1] > 0,
        "a sync and a write held: {queued:?}"
    );
    assert_eq!(door, RUNG, "the door with its unit not yet claimed");
    assert_eq!(counted, 1, "the requests counted after two rings");
    assert!(durable);
}

/// D1b §7.4 (b): a ring while the door is `SERVING`, the unit held at its end, runs one more unit,
/// and that unit does the work rung: the first unit, rung by a sync's last member with nothing
/// landed past it, makes nothing; a write landing and retiring meanwhile rings; let go, the units
/// run are 2 and the second made the sync (mutant: the end stores QUIET whatever it finds, so the
/// work rung is left and the log stops short of durable).
#[test]
fn a_ring_during_a_unit_runs_one_more_unit_which_does_the_work_rung() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, small());
    let mut quiet = durable_start(&log, &dyn_io);
    io.probe.hold_all(true);
    let a = append(&log, &dyn_io, 100);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, a);
    if let Some((c, r)) = io.probe.oldest() {
        c.complete(r);
        c.retire();
    }
    quiet &= settled(&log);
    let sync = io.probe.oldest();
    let counted = log.segments.counted_io().clone();
    let (arrived, go) = log.holds.set("unit.ended");
    let units_before = counted.log_units();
    if let Some((c, _)) = &sync {
        c.complete(0);
        c.retire();
    }
    let reached = arrived.recv_timeout(SETTLE).is_ok();
    let in_unit = io.probe.queued();
    let b = append(&log, &dyn_io, 100);
    let _ = log.wait_durable(&mut wait, b);
    let write = io.probe.oldest();
    if let Some((c, r)) = &write {
        c.complete(*r);
        c.retire();
    }
    let door_during = counted.log_door();
    drop(go);
    quiet &= settled(&log);
    let units = counted.log_units() - units_before;
    let after = io.probe.queued();
    quiet &= answer_all(&log, &io.probe);
    let durable = log.durable() >= b;
    drop((sync, write, wait, counted));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(reached, "the unit never reached its end");
    assert_eq!(in_unit, Vec::<i32>::new(), "the first unit made something");
    assert_eq!(door_during, RUNG | SERVING, "the door rung while serving");
    assert_eq!(units, 2, "the units run");
    assert_eq!(after, vec![0], "the second unit did not make the sync");
    assert!(durable, "the log not durable to its end");
}

/// D1b §7.4 (c): the end's gap, between the door going quiet and its count's release: there the
/// door reads `00` and the requests count 1, and a closer started there parks, sealing only after
/// the release (mutant: the end lets the count go before its exchange, so the gap reads the door
/// `01` and the count 0, and the closer seals while the door still serves).
#[test]
fn a_unit_goes_quiet_before_its_count_goes_and_a_close_waits_for_the_count() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, small());
    let mut quiet = durable_start(&log, &dyn_io);
    io.probe.hold_all(true);
    let a = append(&log, &dyn_io, 100);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, a);
    if let Some((c, r)) = io.probe.oldest() {
        c.complete(r);
        c.retire();
    }
    quiet &= settled(&log);
    let sync = io.probe.oldest();
    let requests = log.segments.counted_io().requests().clone();
    let (seen, go) = log.segments.counted_io().pause_log_end_gap();
    // The sync's last member rings a unit with nothing left to do: it ends quiet. Its observer
    // rings the unit before the sync retires, so the IO thread is held until the sync has
    // retired: the gap then counts the unit's own request alone, on any schedule.
    let hold = requests.hold_io_thread();
    if let Some((c, _)) = &sync {
        c.complete(0);
        c.retire();
    }
    drop(hold);
    let gap = seen.recv_timeout(SETTLE).ok();
    let mut closer = Closer::start(requests.clone(), dyn_io.clone(), || {});
    let first = closer.next();
    drop(go);
    let then = (first == Some(Decided::Parks))
        .then(|| closer.next())
        .flatten();
    let closed = closer.ended();
    drop((sync, wait, requests));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert_eq!(gap, Some((0, 1)), "the door and the count in the end's gap");
    assert_eq!(
        first,
        Some(Decided::Parks),
        "the closer's decision in the end's gap"
    );
    assert_eq!(then, Some(Decided::Sealed), "the closer once let go");
    assert!(closed, "the closer did not end");
}

/// Resets the plan's counters.
fn reset(counts: &Counts) {
    for c in [
        &counts.chunk_loads,
        &counts.pads_to,
        &counts.pads_raced,
        &counts.extent_locks,
        &counts.extent_visits,
        &counts.writes,
        &counts.turns,
    ] {
        c.store(0, Ordering::SeqCst);
    }
}

/// A first write held, padded to its block for its commit's wait, the next segment prepared and
/// the calls cleared first: with one write in flight at most, the caller's later flushes find no
/// write can start, so the next plan is the unit's that the held write's retirement rings.
fn one_write_held(io: &Arc<UnitIO>, config: LogConfig) -> (AloneLog, LogWait) {
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(io, config);
    prepare(&log);
    io.probe.clear();
    io.probe.hold_all(true);
    let a = append(&log, &dyn_io, 100);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, a);
    (log, wait)
}

/// D1b §7.5 (a): one plan's chunk loads are bounded by the ring: 8 writes held, so no write can
/// start, while 64 more chunks are appended and the caller's turns, taken meanwhile, load no chunk
/// and take no lock; once all 8 retire, with the IO thread held so one unit answers them, that
/// unit's one plan loads at most `ring_slots` chunks and makes 8 writes (mutants: the horizon
/// computed again for each write, from that write's start, which loads 484 or more; the scan made
/// before the look at the writes in flight).
#[test]
fn one_plans_chunk_loads_are_at_most_the_rings() {
    let chunk = 8 << 10;
    let config = LogConfig {
        segment_bytes: 1 << 20,
        buffer_bytes: 76 * chunk,
        chunk_bytes: chunk,
        writes_in_flight: 8,
        direct: true,
    };
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, config.clone());
    prepare(&log);
    io.probe.clear();
    io.probe.hold_all(true);
    while log.end() < 8 * chunk + 100 {
        append(&log, &dyn_io, 996);
    }
    let mut wait = LogWait::default();
    let first = log.end();
    let _ = log.wait_durable(&mut wait, first);
    let held: Vec<_> = std::iter::from_fn(|| io.probe.oldest()).collect();
    // No write can start: the caller's turns meanwhile scan and lock nothing.
    reset(&log.counts);
    let from = log.end();
    while log.end() < from + 64 * chunk {
        append(&log, &dyn_io, 996);
        log.flush_one_turn();
    }
    let full = [
        log.counts.turns.load(Ordering::SeqCst),
        log.counts.chunk_loads.load(Ordering::SeqCst),
        log.counts.extent_locks.load(Ordering::SeqCst),
    ];
    let requests = log.segments.counted_io().requests().clone();
    let hold = requests.hold_io_thread();
    for (c, r) in &held {
        c.complete(*r);
        c.retire();
    }
    reset(&log.counts);
    let units_before = log.segments.counted_io().log_units();
    drop(hold);
    let mut quiet = settled(&log);
    let units = log.segments.counted_io().log_units() - units_before;
    let loads = log.counts.chunk_loads.load(Ordering::SeqCst);
    let writes = log.counts.writes.load(Ordering::SeqCst);
    quiet &= answer_all(&log, &io.probe);
    let durable = log.durable() >= first;
    drop((held, wait, requests));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(full[0] > 0, "no turn taken while no write could start");
    assert_eq!(
        (full[1], full[2]),
        (0, 0),
        "(chunk loads, extents' locks) while no write could start, over {} turns",
        full[0]
    );
    assert_eq!(units, 1, "the units run");
    assert_eq!(writes, 8, "the unit's writes");
    assert!(
        (64..=ring_slots(&config)).contains(&loads),
        "{loads} chunk loads for 64 complete chunks, the ring {}",
        ring_slots(&config)
    );
    assert!(durable);
}

/// D1b §7.5 (b): padding makes one attempt per unit: `pad` told `Raced` twice, the first unit's
/// attempt asks for another turn and ends owed, the next likewise, and the third pads: three units,
/// three attempts, the padded write made (mutant: `continue` on `Raced`, three attempts in one
/// unit).
#[test]
fn padding_makes_one_attempt_per_unit() {
    let config = LogConfig {
        writes_in_flight: 1,
        ..small()
    };
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let (log, mut wait) = one_write_held(&io, config);
    let b = append(&log, &dyn_io, 100);
    let _ = log.wait_durable(&mut wait, b);
    let w0 = io.probe.oldest();
    reset(&log.counts);
    log.counts.force_raced.store(2, Ordering::SeqCst);
    let units_before = log.segments.counted_io().log_units();
    if let Some((c, r)) = &w0 {
        c.complete(*r);
        c.retire();
    }
    let mut quiet = settled(&log);
    let units = log.segments.counted_io().log_units() - units_before;
    let raced = log.counts.pads_raced.load(Ordering::SeqCst);
    let padded = log.counts.pads_to.load(Ordering::SeqCst);
    let queued = io.probe.queued();
    quiet &= answer_all(&log, &io.probe);
    let durable = log.durable() >= b;
    drop((w0, wait));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert_eq!((units, raced, padded), (3, 2, 1), "(units, raced, padded)");
    assert_eq!(queued, vec![BLOCK as i32], "the padded write");
    assert!(durable);
}

/// D1b §7.5 (c): the plan's horizon is captured once: padded, then more than a chunk appended
/// meanwhile and waited on, the plan writes nothing past the padded horizon and pads once, and the
/// new records are the next turn's, a unit of their own (mutant: the horizon taken again after the
/// pad, which writes the chunk appended meanwhile in the same turn).
#[test]
fn a_plans_horizon_is_captured_once() {
    let config = LogConfig {
        writes_in_flight: 2,
        ..small()
    };
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let (log, mut wait) = one_write_held(&io, config);
    // A second write held: no write can start until both retire.
    let a2 = append(&log, &dyn_io, 100);
    let _ = log.wait_durable(&mut wait, a2);
    let b = append(&log, &dyn_io, 100);
    let _ = log.wait_durable(&mut wait, b);
    let held: Vec<_> = std::iter::from_fn(|| io.probe.oldest()).collect();
    let requests = log.segments.counted_io().requests().clone();
    let hold = requests.hold_io_thread();
    let (padded_at, padded_go) = log.holds.set("plan.padded");
    let (ended_at, ended_go) = log.holds.set("unit.ended");
    reset(&log.counts);
    for (c, r) in &held {
        c.complete(*r);
        c.retire();
    }
    drop(hold);
    let reached_pad = padded_at.recv_timeout(SETTLE).is_ok();
    // More than a chunk appended and waited on while the plan holds the turn: whole blocks a
    // horizon taken again would reach, and a wait that asks for another turn.
    let from = log.end();
    while log.end() < from + log.config.chunk_bytes + BLOCK {
        append(&log, &dyn_io, 996);
    }
    let c_end = log.end();
    let mut wait_c = LogWait::default();
    let _ = log.wait_durable(&mut wait_c, c_end);
    drop(padded_go);
    let reached_end = ended_at.recv_timeout(SETTLE).is_ok();
    let in_unit = io.probe.queued();
    let pads_in_unit = log.counts.pads_to.load(Ordering::SeqCst);
    drop(ended_go);
    let mut quiet = settled(&log);
    let after = io.probe.queued();
    quiet &= answer_all(&log, &io.probe);
    let durable = log.durable() >= c_end;
    drop((held, wait, wait_c, requests));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(reached_pad && reached_end, "the plan's holds not reached");
    assert_eq!(held_len(&in_unit), (1, BLOCK as i32), "the unit's writes");
    assert_eq!(pads_in_unit, 1, "the unit's pads");
    assert!(
        after.len() >= 2 && after[0] == BLOCK as i32,
        "the new records not the next turn's: {after:?}"
    );
    assert!(durable);
}

/// How many writes, and the bytes of the first.
fn held_len(queued: &[i32]) -> (usize, i32) {
    (queued.len(), queued.first().copied().unwrap_or(0))
}

/// D1b §7.5 (d): a unit takes one turn: a turn asked for again inside each of the first three
/// turns, each unit ends owed after its one turn and the next takes the next, four units of one
/// turn each (mutant: the unit flushes until no turn is owed, four turns in one unit).
#[test]
fn a_unit_takes_one_turn_and_a_turn_asked_for_inside_it_is_the_next_units() {
    let config = LogConfig {
        writes_in_flight: 1,
        ..small()
    };
    let io = UnitIO::new(true);
    let (log, wait) = one_write_held(&io, config);
    let w0 = io.probe.oldest();
    reset(&log.counts);
    log.counts.ask_in_turns.store(3, Ordering::SeqCst);
    let units_before = log.segments.counted_io().log_units();
    if let Some((c, r)) = &w0 {
        c.complete(*r);
        c.retire();
    }
    let mut quiet = settled(&log);
    let units = log.segments.counted_io().log_units() - units_before;
    let turns = log.counts.turns.load(Ordering::SeqCst);
    quiet &= answer_all(&log, &io.probe);
    drop((w0, wait));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert_eq!((units, turns), (4, 4), "(units, turns)");
}

/// D1b §7.6: files that write durably make no sync: a durable write's callback paused between its
/// landing and the durable position's raise while an earlier write's retirement rings a unit, and
/// the unit, run to its end there, starts no sync, nor does any later one (mutant: the unit's sync
/// start without the durable gate, which syncs what landed past the durable position).
#[test]
fn files_that_write_durably_make_no_sync() {
    let config = LogConfig {
        writes_in_flight: 2,
        ..small()
    };
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let (log, mut wait) = one_write_held(&io, config);
    let a2 = append(&log, &dyn_io, 100);
    let _ = log.wait_durable(&mut wait, a2);
    let w1 = io.probe.oldest();
    let w2 = io.probe.oldest();
    if let Some((c, r)) = &w1 {
        c.complete(*r);
    }
    let paused = std::rc::Rc::new(std::cell::Cell::new(None));
    if let Some((first, _)) = &w1 {
        let (first, inner, probe, paused) = (
            first.clone(),
            (*log).clone(),
            io.probe.clone(),
            paused.clone(),
        );
        crate::race::at("log.written.landed", move || {
            first.retire();
            let quiet = settled(&inner);
            paused.set(Some((quiet, probe.syncs())));
        });
    }
    if let Some((c, r)) = &w2 {
        c.complete(*r);
        c.retire();
    }
    let mut quiet = settled(&log);
    quiet &= answer_all(&log, &io.probe);
    let syncs = io.probe.syncs();
    let durable = log.durable() >= a2;
    drop((w1, w2, wait));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert_eq!(
        paused.get(),
        Some((true, 0)),
        "(the unit ended, the syncs) at the paused callback"
    );
    assert_eq!(syncs, 0, "the syncs made");
    assert!(durable);
}

/// Writes to the end of the first segment and past it, landed while a sync the IO thread made is
/// held: what a group over both segments needs, and that sync, its claim held.
fn landed_across_a_segment(io: &Arc<UnitIO>, log: &Arc<Log>) -> (Option<(Completion, i32)>, bool) {
    let dyn_io: Arc<dyn IO> = io.clone();
    let segment = log.segments.size;
    while log.end() < segment - 2 * BLOCK {
        append(log, &dyn_io, 996);
    }
    let end = log.end();
    drive(&dyn_io, || log.wait_durable(&mut LogWait::default(), end)).unwrap();
    prepare(log);
    let mut quiet = settled(log);
    io.probe.hold_io_syncs.store(true, Ordering::SeqCst);
    let a = append(log, &dyn_io, 996);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, a);
    quiet &= settled(log);
    let sync = io.probe.oldest();
    while log.end() < segment + 1024 {
        append(log, &dyn_io, 996);
    }
    let b = log.end();
    let _ = log.wait_durable(&mut wait, b);
    let deadline = Instant::now() + SETTLE;
    while log.landed.at() < b && Instant::now() < deadline {
        log.flush_one_turn();
        quiet &= settled_by(log, deadline);
    }
    quiet &= log.landed.at() >= b;
    (sync, quiet)
}

/// D1b §7.7: a sync member refused in a group of two, its first member still held by the backend:
/// the log fails at the refusal, before that member is answered; answered, it retires, every
/// counted request is let go, the door ends `00`, and a close seals at zero and ends. Run in a
/// child process, so a refusal that ends the process is the parent's ordinary failure, with the
/// child's own words (mutants: the group aborted whole with its first member outstanding, which
/// ends the child on "callback called before all completions finished"; the refusal not failing
/// the log, which then fails only once the held member is answered).
#[cfg(unix)]
#[test]
fn a_refused_sync_member_fails_the_log_and_leaves_nothing_counted() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::a_refused_sync_member_fails_the_log_and_leaves_nothing_counted"
        ),
        || {
            let io = UnitIO::new(false);
            let dyn_io: Arc<dyn IO> = io.clone();
            let log = open(&io, small());
            let (sync, mut quiet) = landed_across_a_segment(&io, &log);
            let is_sync = sync.as_ref().is_some_and(|(_, r)| *r == 0);
            // The group's first member is held as the earlier sync was; its second is refused.
            *io.probe.refuse_sync_of.lock() = Some(log.segments.path(1));
            let calls_before = io.probe.calls().len();
            if let Some((c, _)) = &sync {
                c.complete(0);
                c.retire();
            }
            quiet &= settled(&log);
            let group: Vec<_> = io.probe.calls()[calls_before..]
                .iter()
                .filter(|c| c.what.starts_with("sync"))
                .map(|c| (c.what, c.path.clone()))
                .collect();
            let failed_at_refusal = log.failure().is_some();
            let held = io.probe.queued();
            io.probe.hold_io_syncs.store(false, Ordering::SeqCst);
            quiet &= answer_all(&log, &io.probe);
            let door = log.segments.counted_io().log_door();
            let requests = log.segments.counted_io().requests().clone();
            let mut closer = Closer::start(requests.clone(), dyn_io.clone(), || {});
            let closed = closer.ended();
            // Read once the close has ended: a quiet door may still be letting its count go.
            let counted = requests.outstanding();
            drop((sync, requests));
            drop(log);
            assert!(quiet, "the writes across the segment did not land");
            assert!(is_sync, "no sync held");
            assert_eq!(
                group.iter().map(|g| g.0).collect::<Vec<_>>(),
                vec!["sync", "sync_refused"],
                "the group's members: {group:?}"
            );
            assert_eq!(held, vec![0], "the group's first member, held");
            assert!(
                failed_at_refusal,
                "the log not failed at the refusal, its first member still held"
            );
            assert_eq!(door, 0, "the door after the failure");
            assert_eq!(counted, 0, "the requests counted after the failure");
            assert!(closed, "the close did not end");
        },
    );
    if let Some(ended) = ended {
        assert!(
            !ended.aborted && !ended.hung && ended.code == Some(0),
            "the child ended {:?} (aborted {}, hung {}): {}",
            ended.code,
            ended.aborted,
            ended.hung,
            ended.stderr
        );
    }
}

/// D1b §7.8 (#4744): a wide sync group: files that do not write durably, segments of one chunk
/// and a buffer of 70 of them; a sync the IO thread made held while writes land across 70
/// segments, and, released, the next group at the backend has at least 64 members and at most
/// `M` = 71, past one word's records, the commits durable to the end and the log's bit 33.
#[test]
fn a_sync_group_wider_than_a_word_is_made_whole() {
    let chunk = 8 << 10;
    let config = LogConfig {
        segment_bytes: chunk,
        buffer_bytes: 70 * chunk,
        chunk_bytes: chunk,
        writes_in_flight: 70,
        direct: true,
    };
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, config);
    prepare(&log);
    io.probe.hold_io_syncs.store(true, Ordering::SeqCst);
    let a = append(&log, &dyn_io, 100);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, a);
    let mut quiet = settled(&log);
    let sync = io.probe.oldest();
    let target = 69 * chunk + BLOCK;
    let deadline = Instant::now() + SETTLE;
    while log.end() < target && Instant::now() < deadline {
        append(&log, &dyn_io, 996);
        prepare(&log);
    }
    let end = log.end();
    let _ = log.wait_durable(&mut wait, end);
    while log.landed.at() < end && Instant::now() < deadline {
        log.flush_one_turn();
        prepare(&log);
        quiet &= settled_by(&log, deadline);
    }
    quiet &= settled_by(&log, deadline);
    let segments_landed =
        log.segments.number(log.landed.at() - 1) - log.segments.number(log.durable()) + 1;
    io.probe.hold_io_syncs.store(false, Ordering::SeqCst);
    let units_before = log.segments.counted_io().log_units();
    if let Some((c, _)) = &sync {
        c.complete(0);
        c.retire();
    }
    quiet &= settled(&log);
    quiet &= answer_all(&log, &io.probe);
    let group = groups_after(&io.probe.calls(), units_before)
        .first()
        .copied()
        .unwrap_or(0);
    let durable = log.durable() >= end;
    drop((sync, wait));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(segments_landed >= 64, "{segments_landed} segments landed");
    assert!((64..=71).contains(&group), "{group} members in the group");
    assert!(durable, "the commits not durable to the end");
    assert_eq!(Counted::log_bit(), 33, "the log's bit");
}

/// D1b §7.9 (B5): padding past the buffer, the cap: one segment of one block is the whole buffer,
/// so `M` = 2. A sync the IO thread made is held; 128 records of 32 bytes, all from one grant of
/// 4096 bytes, are each padded to their block while an admission waits, written and retired before
/// the next, so the bytes landed and not durable pass the buffer; released, every group at the
/// backend has at most 2 members, and the log becomes durable to its end over successive groups
/// (mutant: the cap removed, the next group 127 members).
#[test]
fn padding_past_the_buffer_leaves_every_sync_group_within_its_cap() {
    let config = LogConfig {
        segment_bytes: BLOCK,
        buffer_bytes: BLOCK,
        chunk_bytes: BLOCK,
        writes_in_flight: 1,
        direct: true,
    };
    let payload = (0..BLOCK as usize)
        .find(|&p| framed_len(p) == 32)
        .expect("a payload framed in 32 bytes");
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, config);
    prepare(&log);
    let mut grant = drive(&dyn_io, || log.admit(&mut LogWait::default(), BLOCK)).unwrap();
    // Waits for room the grant holds: someone waits, so each record is padded to its block.
    let mut waiter = LogWait::default();
    let mut waiting = log.admit(&mut waiter, 32);
    let mut admission_waits = matches!(waiting, Ok(IOResult::IO(_)));
    io.probe.hold_io_syncs.store(true, Ordering::SeqCst);
    let p = vec![5u8; payload];
    let deadline = Instant::now() + SETTLE;
    let mut quiet = true;
    for _ in 0..128 {
        log.append(&mut grant, &header(), &[&p]);
        let end = log.end();
        while log.landed.at() < end && Instant::now() < deadline {
            // A wait woken only calls again: called again, it waits again while no room is free.
            if !log.admitters.waiting() {
                waiting = log.admit(&mut waiter, 32);
                admission_waits &= matches!(waiting, Ok(IOResult::IO(_)));
            }
            prepare(&log);
            log.flush_one_turn();
            quiet &= settled_by(&log, deadline);
        }
    }
    let landed = log.landed.at();
    let past = landed - log.durable();
    let sync = io.probe.oldest();
    io.probe.hold_io_syncs.store(false, Ordering::SeqCst);
    let units_before = log.segments.counted_io().log_units();
    if let Some((c, _)) = &sync {
        c.complete(0);
        c.retire();
    }
    quiet &= settled(&log);
    quiet &= answer_all(&log, &io.probe);
    let groups = groups_after(&io.probe.calls(), units_before);
    let durable = log.durable() >= landed;
    drop((sync, waiting, waiter, grant));
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(admission_waits, "the admission did not wait");
    assert!(
        past > BLOCK,
        "{past} bytes landed and not durable, the buffer {BLOCK}"
    );
    assert!(
        !groups.is_empty() && groups.iter().all(|&g| g <= 2),
        "the groups' members: {groups:?}"
    );
    assert!(durable, "the log not durable to its end");
}

/// D1b §7.10 (a): a plan with no record published in place takes the extents' lock no time, over
/// turns that plan writes (mutant: the lock taken by every plan).
#[test]
fn a_plan_with_no_extent_queued_takes_no_lock() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, small());
    let mut quiet = durable_start(&log, &dyn_io);
    reset(&log.counts);
    for _ in 0..8 {
        let lsn = append(&log, &dyn_io, 996);
        drive(&dyn_io, || log.wait_durable(&mut LogWait::default(), lsn)).unwrap();
    }
    quiet &= settled(&log);
    let locks = log.counts.extent_locks.load(Ordering::SeqCst);
    let writes = log.counts.writes.load(Ordering::SeqCst);
    drop(log);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(writes >= 8, "{writes} writes planned");
    assert_eq!(locks, 0, "the extents' lock taken");
}

/// D1b §7.10 (b): the extents read after the horizon's capture: a record of a chunk's two blocks,
/// published in place while the unit's plan is held after its start and before its capture, is
/// written from its own block, and reads back whole (mutant: the extents counted before the
/// capture, which writes the ring's bytes over the record's place, a whole chunk being complete).
#[test]
fn a_record_published_in_place_before_the_capture_is_written_from_its_block() {
    let config = LogConfig {
        writes_in_flight: 1,
        ..small()
    };
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let (log, mut wait) = one_write_held(&io, config.clone());
    let w0 = io.probe.oldest();
    reset(&log.counts);
    let (at_capture, go) = log.holds.set("plan.capture");
    if let Some((c, r)) = &w0 {
        c.complete(*r);
        c.retire();
    }
    let reached = at_capture.recv_timeout(SETTLE).is_ok();
    // Framed in two blocks, the chunk: complete with nobody waiting, so the plan writes it at once.
    let p: Vec<u8> = (0..(2 * BLOCK as usize - record::HEADER - 128))
        .map(|i| (i % 251) as u8)
        .collect();
    let mut block = AlignedBlock::zeroed(config.chunk_bytes as usize);
    block.bytes_mut()[record::HEADER..record::HEADER + p.len()].copy_from_slice(&p);
    let room = framed_len(p.len()).next_multiple_of(BLOCK) + BLOCK;
    let mut admitted = drive(&dyn_io, || log.admit(&mut LogWait::default(), room)).unwrap();
    let (_, lsn) = log.publish(&mut admitted, &header(), block, p.len(), |_| {});
    let _ = log.wait_durable(&mut wait, lsn);
    drop(go);
    let mut quiet = settled(&log);
    io.probe.hold_all(false);
    quiet &= answer_all(&log, &io.probe);
    let deadline = Instant::now() + SETTLE;
    while log.durable() < lsn && Instant::now() < deadline {
        log.flush_one_turn();
        quiet &= settled_by(&log, deadline);
    }
    let durable = log.durable() >= lsn;
    let locks = log.counts.extent_locks.load(Ordering::SeqCst);
    drop((w0, wait, admitted));
    drop(log);
    let mut reader = Log::reader(dyn_io.clone(), BASE, &config, 0);
    let mut found = false;
    while let Ok(Some(r)) = reader.next_record() {
        found |= r.payload == p;
    }
    drop(reader);
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(reached, "the plan's hold not reached");
    assert!(durable, "the record not durable");
    assert!(locks >= 1, "the extents' lock never taken");
    assert!(
        found,
        "the record published in place did not read back whole"
    );
}

/// D1b §7.10 (c): the extents of one plan, several, one of them split across a segment's end: the
/// unit's plan, held after its start while they are published in place, takes the extents' lock
/// once and examines each extent a bounded number of times, at least once each and at most once
/// per write it plans beside once per extent, and every record reads back whole (mutant: today's
/// road, the lock taken again and the queue walked from its front for every write).
#[test]
fn one_plan_over_several_extents_takes_their_lock_once() {
    let config = LogConfig {
        writes_in_flight: 8,
        ..small()
    };
    let segment = config.segment_bytes;
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open(&io, config.clone());
    while log.end() < segment - 4 * BLOCK {
        append(&log, &dyn_io, 996);
    }
    let end = log.end();
    drive(&dyn_io, || log.wait_durable(&mut LogWait::default(), end)).unwrap();
    prepare(&log);
    let mut quiet = settled(&log);
    io.probe.clear();
    io.probe.hold_all(true);
    let a = append(&log, &dyn_io, 100);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, a);
    let w0 = io.probe.oldest();
    reset(&log.counts);
    let (at_capture, go) = log.holds.set("plan.capture");
    if let Some((c, r)) = &w0 {
        c.complete(*r);
        c.retire();
    }
    let reached = at_capture.recv_timeout(SETTLE).is_ok();
    // One-block records up to the segment's last block, then one of two blocks across its end,
    // then one more.
    let payload = |blocks: u64, seed: u64| -> Vec<u8> {
        // Short of the blocks by more than the trailer and its alignment: framed in exactly them.
        let len = (blocks * BLOCK) as usize - record::HEADER - 128;
        (0..len)
            .map(|i| ((i as u64 * 7 + seed) % 251) as u8)
            .collect()
    };
    let mut published = Vec::new();
    let mut lsn = 0;
    let mut publish = |p: Vec<u8>| {
        let mut block = AlignedBlock::zeroed(config.chunk_bytes as usize);
        block.bytes_mut()[record::HEADER..record::HEADER + p.len()].copy_from_slice(&p);
        let room = framed_len(p.len()).next_multiple_of(BLOCK) + BLOCK;
        let mut admitted = drive(&dyn_io, || log.admit(&mut LogWait::default(), room)).unwrap();
        lsn = log
            .publish(&mut admitted, &header(), block, p.len(), |_| {})
            .1;
        published.push(p);
    };
    let mut seed = 0;
    while log.end().next_multiple_of(BLOCK) + BLOCK < segment {
        seed += 1;
        publish(payload(1, seed));
    }
    let across = log.end().next_multiple_of(BLOCK);
    publish(payload(2, 101));
    publish(payload(1, 102));
    let _ = log.wait_durable(&mut wait, lsn);
    drop(go);
    quiet &= settled(&log);
    let in_plan = [
        log.counts.extent_locks.load(Ordering::SeqCst),
        log.counts.extent_visits.load(Ordering::SeqCst),
        log.counts.writes.load(Ordering::SeqCst),
    ];
    io.probe.hold_all(false);
    quiet &= answer_all(&log, &io.probe);
    let deadline = Instant::now() + SETTLE;
    while log.durable() < lsn && Instant::now() < deadline {
        log.flush_one_turn();
        quiet &= settled_by(&log, deadline);
    }
    let durable = log.durable() >= lsn;
    let extents = published.len();
    drop((w0, wait));
    drop(log);
    let mut reader = Log::reader(dyn_io.clone(), BASE, &config, 0);
    let mut read = Vec::new();
    while let Ok(Some(r)) = reader.next_record() {
        read.push(r.payload);
    }
    drop(reader);
    let found = published.iter().filter(|p| read.contains(p)).count();
    assert!(quiet, "the log's units did not end within {SETTLE:?}");
    assert!(reached, "the plan's hold not reached");
    assert!(
        across < segment && across + 2 * BLOCK > segment && extents >= 3,
        "no record across the segment's end: at {across}, {extents} extents"
    );
    let [locks, visits, writes] = in_plan;
    assert_eq!(locks, 1, "the extents' locks of the one plan");
    assert!(
        visits >= extents && visits <= writes + extents,
        "{visits} extent visits for {extents} extents and {writes} writes"
    );
    assert!(durable, "the records not durable");
    assert_eq!(
        found, extents,
        "the records published in place read back whole"
    );
}

/// D1b §7.11 (a): the log dropped before its write's observer runs: the observer rings nothing,
/// the door stays `00`, and the requests count nothing once the write retires (mutant: the
/// observer rings whether the log lives, the IO thread held so the ring stays seen).
#[test]
fn a_write_retiring_after_its_log_is_gone_rings_nothing() {
    let io = UnitIO::new(true);
    let config = LogConfig {
        writes_in_flight: 1,
        ..small()
    };
    let (log, wait) = one_write_held(&io, config);
    let write = io.probe.oldest();
    let counted = log.segments.counted_io().clone();
    let requests = counted.requests().clone();
    let hold = requests.hold_io_thread();
    drop(wait);
    drop(log);
    if let Some((c, r)) = &write {
        c.complete(*r);
        c.retire();
    }
    let door = counted.log_door();
    let left = requests.outstanding();
    drop(hold);
    drop((write, requests, counted));
    assert_eq!(door, 0, "the door after a write of a log gone retired");
    assert_eq!(left, 0, "the requests counted");
}

/// D1b §7.11 (b): the observer rings while the log lives, the IO thread held before its claim; the
/// log is then dropped and the thread let go: the unit finds no log, ends `00` and lets its count
/// go, read from its end's note, and a close then seals (mutant: the unit returns at the missing
/// log without leaving `SERVING`, its note `01` with the count 1).
#[test]
fn a_unit_rung_for_a_log_since_gone_ends_quiet_and_lets_its_count_go() {
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let config = LogConfig {
        writes_in_flight: 1,
        ..small()
    };
    let (log, wait) = one_write_held(&io, config);
    let write = io.probe.oldest();
    let counted = log.segments.counted_io().clone();
    let requests = counted.requests().clone();
    let hold = requests.hold_io_thread();
    let ended = counted.on_log_unit_end();
    if let Some((c, r)) = &write {
        c.complete(*r);
        c.retire();
    }
    let rung = (counted.log_door(), requests.outstanding());
    drop(wait);
    drop(log);
    drop(hold);
    let note = ended.recv_timeout(SETTLE).ok();
    // A close is tried only on a note that says nothing is left: anything else would wait on it.
    let closed = note == Some((0, 0)) && {
        let mut closer = Closer::start(requests.clone(), dyn_io.clone(), || {});
        closer.ended()
    };
    drop((write, requests, counted));
    assert_eq!(rung, (RUNG, 1), "the door and the count once rung");
    assert_eq!(note, Some((0, 0)), "the unit's end: the door and the count");
    assert!(closed, "the close did not end");
}

/// D1b §7.11 (c): the last owner of the requests dropped with only the door's work left, the IO
/// thread held before its claim, then let go as the drop stops it: its unit finds the requests
/// gone, and no dispatch, requests or thread is left (mutant: the unit takes its requests through
/// an `expect`, which ends the child process).
#[cfg(unix)]
#[test]
fn the_requests_dropped_with_only_the_doors_work_left_leave_nothing() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::the_requests_dropped_with_only_the_doors_work_left_leave_nothing"
        ),
        || {
            let io = UnitIO::new(true);
            let config = LogConfig {
                writes_in_flight: 1,
                ..small()
            };
            let (log, wait) = one_write_held(&io, config);
            let write = io.probe.oldest();
            let requests = Arc::downgrade(log.segments.counted_io().requests());
            let dispatch = log.segments.counted_io().requests().dispatch_weak();
            let exit = log.segments.counted_io().on_io_exit();
            let hold = log.segments.counted_io().requests().hold_io_thread();
            if let Some((c, r)) = &write {
                c.complete(*r);
                c.retire();
            }
            drop((write, wait));
            let (stopping_tx, stopping) = mpsc::channel();
            let (gone_tx, gone) = mpsc::channel();
            let dropper = std::thread::spawn(move || {
                crate::race::at("requests.stopping", move || {
                    let _ = stopping_tx.send(());
                });
                drop(log);
                let _ = gone_tx.send(());
            });
            let stopping = stopping.recv_timeout(SETTLE).is_ok();
            drop(hold);
            let exited = exit.recv_timeout(SETTLE).is_ok();
            let dropped = gone.recv_timeout(SETTLE).is_ok();
            if dropped {
                let _ = dropper.join();
            }
            let left = (requests.upgrade().is_some(), dispatch.upgrade().is_some());
            drop(io);
            assert!(stopping, "the drop never stopped the IO thread");
            assert!(exited && dropped, "the IO thread or the drop did not end");
            assert_eq!(left, (false, false), "(the requests, the dispatch) left");
        },
    );
    if let Some(ended) = ended {
        assert!(
            !ended.aborted && !ended.hung && ended.code == Some(0),
            "the child ended {:?} (aborted {}, hung {}): {}",
            ended.code,
            ended.aborted,
            ended.hung,
            ended.stderr
        );
    }
}

/// D1b §7.14 (#4776): the log's last owner is the unit: the unit held just after it took the log,
/// the opener drops its log, a close started then does not seal while the unit holds it, and once
/// let go the unit drops the log on the IO thread, before the door's end lets its count go, so the
/// seal finds the log gone, and the thread's cleanup is allowed there (mutants: the opener's
/// executor kept in the log, whose close on the IO thread ends the child; the unit's count let go
/// before it runs, so the close seals while the unit holds the log).
#[cfg(unix)]
#[test]
fn a_unit_that_is_its_logs_last_owner_drops_it_on_the_io_thread_before_its_count_goes() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::a_unit_that_is_its_logs_last_owner_drops_it_on_the_io_thread_before_its_count_goes"
        ),
        || {
            let io = UnitIO::new(true);
            let dyn_io: Arc<dyn IO> = io.clone();
            let config = LogConfig {
                writes_in_flight: 1,
                ..small()
            };
            let (log, wait) = one_write_held(&io, config);
            let write = io.probe.oldest();
            let (dropped_tx, dropped_on) = mpsc::channel();
            *log.keepsake.lock() = Some(Box::new(DropsOn(dropped_tx)));
            let weak = Arc::downgrade(&*log);
            let requests = log.segments.counted_io().requests().clone();
            let (arrived, go) = log.holds.set("unit.started");
            if let Some((c, r)) = &write {
                c.complete(*r);
                c.retire();
            }
            let reached = arrived.recv_timeout(SETTLE).is_ok();
            drop((write, wait));
            drop(log);
            let at_seal = Arc::new(Mutex::new(None));
            let mut closer = {
                let (weak, at_seal) = (weak.clone(), at_seal.clone());
                Closer::start(requests.clone(), dyn_io.clone(), move || {
                    *at_seal.lock() = Some(weak.upgrade().is_none());
                })
            };
            let first = closer.next();
            drop(go);
            let then = (first == Some(Decided::Parks))
                .then(|| closer.next())
                .flatten();
            let thread = dropped_on.recv_timeout(SETTLE).ok();
            let closed = closer.ended();
            let gone_at_seal = *at_seal.lock();
            let gone = weak.upgrade().is_none();
            drop((requests, io, dyn_io));
            assert!(reached, "the unit never took the log");
            assert_eq!(
                first,
                Some(Decided::Parks),
                "the close's decision while the unit held the log"
            );
            assert_eq!(then, Some(Decided::Sealed), "the close once let go");
            assert_eq!(
                thread.as_deref(),
                Some(IO_THREAD),
                "where the log was dropped"
            );
            assert_eq!(gone_at_seal, Some(true), "the log at the seal");
            assert!(closed && gone, "the close did not end, or the log is left");
        },
    );
    if let Some(ended) = ended {
        assert!(
            !ended.aborted && !ended.hung && ended.code == Some(0),
            "the child ended {:?} (aborted {}, hung {}): {}",
            ended.code,
            ended.aborted,
            ended.hung,
            ended.stderr
        );
    }
}

/// The sync groups the IO thread's units made after unit `after`, each its members at the backend,
/// in order: a group is one unit's syncs.
fn groups_after(calls: &[Call], after: usize) -> Vec<usize> {
    let mut groups: Vec<usize> = Vec::new();
    let mut unit = None;
    for c in calls {
        if c.what == "sync" && c.thread == IO_THREAD && c.unit > after {
            if unit == Some(c.unit) {
                *groups.last_mut().expect("a group begun") += 1;
            } else {
                unit = Some(c.unit);
                groups.push(1);
            }
        }
    }
    groups
}

/// Tells, as it is dropped, the name of the thread that drops it.
struct DropsOn(mpsc::Sender<String>);

impl Drop for DropsOn {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        let _ = self.0.send(this_thread());
    }
}

// D2c (i): the log's caller roads in a cooperative frame of a database opened async. A caller's
// flush there rings the log's door, and its IO thread's unit takes the turns.

/// A log opened in a database opened async, its probe watching its IO thread's units, with an
/// executor of `workers` workers.
fn open_async(io: &Arc<UnitIO>, config: LogConfig, workers: usize) -> AloneLog {
    let log = Log::open_async(io.clone(), BASE, config, workers).unwrap();
    *io.probe.counted.lock() = Some(Arc::downgrade(log.segments.counted_io()));
    log
}

/// `f` polled once inside a cooperative frame.
fn in_frame<T>(f: impl FnOnce() -> T) -> T {
    let _frame = crate::storage::atoll::cooperative::Frame::enter();
    f()
}

/// `f` polled inside a frame until done, each wait waited for outside it.
fn in_frames<T>(io: &Arc<dyn IO>, mut f: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
    loop {
        match in_frame(&mut f)? {
            IOResult::Done(t) => return Ok(t),
            IOResult::IO(c) => io.wait(&c.0),
        }
    }
}

/// A first block appended and made durable outside any frame, in place as ever, and the units
/// ended: whether they ended. The executor's worker prepares the segments.
fn durable_start_async(log: &Arc<Log>, io: &Arc<dyn IO>) -> bool {
    while log.end() < BLOCK {
        append(log, io, 996);
    }
    let end = log.end();
    drive(io, || log.wait_durable(&mut LogWait::default(), end)).unwrap();
    settled(log)
}

/// Whether `calls` hold a call named `what`.
fn has(calls: &[(&'static str, usize)], what: &str) -> bool {
    calls.iter().any(|(w, _)| *w == what)
}

/// The child's end, which must be clean: run to its end with no abort and code 0.
fn child_ended_cleanly(ended: Option<crate::storage::atoll::testing::Ended>) {
    if let Some(ended) = ended {
        assert!(
            !ended.aborted && !ended.hung && ended.code == Some(0),
            "the child ended {:?} (aborted {}, hung {}): {}",
            ended.code,
            ended.aborted,
            ended.hung,
            ended.stderr
        );
    }
}

/// D2c §3.1 (child): an admission in a frame that must wait for the buffer's room rings the log's
/// door: it returns IO with no backend call on the polling thread, the IO thread's unit syncs what
/// is appended, and the admission is granted once that lands (mutant `admit-flushes-in-place`: the
/// admission flushes in the frame, and the frame census ends the child at the segment's write).
#[cfg(all(unix, not(miri)))]
#[test]
fn an_admission_in_a_frame_that_must_wait_rings_the_log_and_is_granted_once_its_flush_lands() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::an_admission_in_a_frame_that_must_wait_rings_the_log_and_is_granted_once_its_flush_lands"
        ),
        || {
            let io = UnitIO::new(false);
            let dyn_io: Arc<dyn IO> = io.clone();
            let log = open_async(&io, small(), 1);
            let quiet = durable_start_async(&log, &dyn_io);
            append(&log, &dyn_io, 100);
            let held = log.hold_buffer_room();
            let units = log.segments.counted_io().log_units();
            io.probe.clear();
            let mut wait = LogWait::default();
            // The IO thread held across the first poll: the log's sync, which alone advances
            // durability, is its unit's, so nothing that poll rechecks can be granted yet.
            let hold = log.segments.counted_io().requests().hold_io_thread();
            let first = in_frame(|| log.admit(&mut wait, framed_len(100)));
            let first_is_io = matches!(first, Ok(IOResult::IO(_)));
            let in_place = on(&io.probe.calls(), &this_thread());
            drop(hold);
            let granted = in_frames(&dyn_io, || log.admit(&mut wait, framed_len(100))).is_ok();
            let settled_after = settled(&log);
            let calls = io.probe.calls();
            let ran = log.segments.counted_io().log_units() > units;
            drop((first, held, wait));
            drop(log);
            assert!(quiet && settled_after, "the units did not end within {SETTLE:?}");
            assert!(first_is_io, "the admission did not return its wait");
            assert_eq!(in_place, vec![], "the polling thread's backend calls in the frame");
            assert!(granted, "the admission never granted");
            assert!(ran, "no unit of the log's ran");
            assert!(
                has(&on(&calls, IO_THREAD), "sync"),
                "the IO thread made no sync: {calls:?}"
            );
            assert!(
                on(&calls, &this_thread()).is_empty(),
                "the polling thread made backend calls: {calls:?}"
            );
        },
    );
    child_ended_cleanly(ended);
}

/// D2c §3.2 (child): `wait_durable` in a frame rings the log's door and returns its registration;
/// it is done once the unit's sync lands, and the polling thread makes no backend call (mutant
/// `wait-flushes-in-place`: the frame census ends the child at the segment's write).
#[cfg(all(unix, not(miri)))]
#[test]
fn a_durability_wait_in_a_frame_rings_the_log_and_is_done_once_its_sync_lands() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::a_durability_wait_in_a_frame_rings_the_log_and_is_done_once_its_sync_lands"
        ),
        || {
            let io = UnitIO::new(false);
            let dyn_io: Arc<dyn IO> = io.clone();
            let log = open_async(&io, small(), 1);
            let quiet = durable_start_async(&log, &dyn_io);
            let lsn = append(&log, &dyn_io, 100);
            io.probe.clear();
            let mut wait = LogWait::default();
            // The IO thread held across the first poll: the log's sync, which alone advances
            // durability, is its unit's, so nothing that poll rechecks can be granted yet.
            let hold = log.segments.counted_io().requests().hold_io_thread();
            let first = in_frame(|| log.wait_durable(&mut wait, lsn));
            let first_is_io = matches!(first, Ok(IOResult::IO(_)));
            let in_place = on(&io.probe.calls(), &this_thread());
            drop(hold);
            let done = in_frames(&dyn_io, || log.wait_durable(&mut wait, lsn)).is_ok();
            let durable = log.durable() >= lsn;
            let settled_after = settled(&log);
            let calls = io.probe.calls();
            drop((first, wait));
            drop(log);
            assert!(
                quiet && settled_after,
                "the units did not end within {SETTLE:?}"
            );
            assert!(first_is_io, "the wait did not return its registration");
            assert_eq!(
                in_place,
                vec![],
                "the polling thread's backend calls in the frame"
            );
            assert!(
                done && durable,
                "the wait never done, or done short of its mark"
            );
            assert!(
                has(&on(&calls, IO_THREAD), "sync"),
                "the IO thread made no sync: {calls:?}"
            );
            assert!(
                on(&calls, &this_thread()).is_empty(),
                "the polling thread made backend calls: {calls:?}"
            );
        },
    );
    child_ended_cleanly(ended);
}

/// D2c §1: a job's one turn of the flush, asked in a frame with its mark not yet durable, is
/// refused by name: units run on workers, never in a frame.
#[test]
fn a_jobs_turn_of_the_flush_asked_in_a_frame_is_refused_by_name() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    let lsn = append(&log, &dyn_io, 100);
    let pending = log.durable() < lsn && log.failure().is_none();
    let mut wait = LogWait::default();
    let refused = in_frame(|| log.wait_durable_turn(&mut wait, lsn))
        .err()
        .map(|e| e.to_string());
    let outside = drive(&dyn_io, || log.wait_durable(&mut wait, lsn)).is_ok();
    let settled_after = settled(&log);
    drop(wait);
    drop(log);
    assert!(
        quiet && settled_after,
        "the units did not end within {SETTLE:?}"
    );
    assert!(
        pending,
        "the mark already durable, or the log failed, before the turn"
    );
    assert!(
        refused.as_deref().is_some_and(
            |e| e.contains("a job's turn of the log's flush asked in a cooperative frame")
        ),
        "the turn in a frame: {refused:?}"
    );
    assert!(outside, "the same wait outside a frame never done");
}

/// The room of one append, one group of two and one publish, each of `payload` bytes, admitted
/// outside any frame.
fn three_admissions(log: &Arc<Log>, io: &Arc<dyn IO>, payload: usize) -> [Admitted; 3] {
    let one = framed_len(payload);
    let publish = framed_len(payload).next_multiple_of(BLOCK) + BLOCK;
    [one, 2 * one, publish]
        .map(|bytes| drive(io, || log.admit(&mut LogWait::default(), bytes)).unwrap())
}

/// An append, a group of two and a publish of `p`, each in its own room of `rooms`: their last
/// positions.
fn the_three_appends(
    log: &Arc<Log>,
    rooms: &mut [Admitted; 3],
    p: &[u8],
    config: &LogConfig,
) -> [Lsn; 3] {
    let [one, group, publish] = rooms;
    let appended = log.append(one, &header(), &[p]);
    let (_, grouped) = log.append_group_with(group, &[(header(), &[p]), (header(), &[p])], |_| {});
    let mut block = AlignedBlock::zeroed(config.chunk_bytes as usize);
    block.bytes_mut()[record::HEADER..record::HEADER + p.len()].copy_from_slice(p);
    let (_, published) = log.publish(publish, &header(), block, p.len(), |_| {});
    [appended, *grouped.last().unwrap(), published]
}

/// D2c §3.3 (child): with a waiter present, an append, a group and a publish in a frame each ring
/// the log's door and submit nothing on the polling thread; the IO thread's unit, once let go,
/// writes and syncs them, and the waiter is done (mutant `publish-flushes-in-place`: the frame
/// census ends the child at the publish's write).
#[cfg(all(unix, not(miri)))]
#[test]
fn an_append_a_group_and_a_publish_in_a_frame_with_a_waiter_ring_and_submit_nothing() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::an_append_a_group_and_a_publish_in_a_frame_with_a_waiter_ring_and_submit_nothing"
        ),
        || {
            let io = UnitIO::new(false);
            let dyn_io: Arc<dyn IO> = io.clone();
            let config = small();
            let log = open_async(&io, config.clone(), 1);
            let quiet = durable_start_async(&log, &dyn_io);
            let mut rooms = three_admissions(&log, &dyn_io, 100);
            let lsn = append(&log, &dyn_io, 100);
            let counted = log.segments.counted_io().clone();
            let hold = counted.requests().hold_io_thread();
            io.probe.clear();
            let mut waiter = LogWait::default();
            let registered = matches!(
                in_frame(|| log.wait_durable(&mut waiter, lsn)),
                Ok(IOResult::IO(_))
            );
            let p = vec![9u8; 100];
            let ends = in_frame(|| the_three_appends(&log, &mut rooms, &p, &config));
            let in_place = on(&io.probe.calls(), &this_thread());
            let door = counted.log_door();
            let failure = log.failure();
            drop(hold);
            let last = *ends.iter().max().unwrap();
            let done = in_frames(&dyn_io, || log.wait_durable(&mut waiter, last)).is_ok();
            let settled_after = settled(&log);
            let calls = io.probe.calls();
            drop((rooms, waiter, counted));
            drop(log);
            assert!(
                quiet && settled_after,
                "the units did not end within {SETTLE:?}"
            );
            assert!(registered, "the waiter did not register");
            assert_eq!(
                in_place,
                vec![],
                "the polling thread's backend calls in the frame"
            );
            assert_eq!(door, RUNG, "the door while its unit is held");
            assert_eq!(failure, None, "the log failed");
            assert!(done, "the waiter never done");
            assert!(
                has(&on(&calls, IO_THREAD), "sync"),
                "the IO thread made no sync: {calls:?}"
            );
            assert!(
                on(&calls, &this_thread()).is_empty(),
                "the polling thread made backend calls: {calls:?}"
            );
        },
    );
    child_ended_cleanly(ended);
}

/// D2c §3.4: with the database's count sealed, an append, a group and a publish in a frame, each
/// with a waiter present, return their positions and fail the log: the next `wait_durable` and
/// the next admission return that failure. The waiter is an admission whose own ring was refused.
/// No worker, so nothing but the test's calls makes the log's IO after the seal (mutant
/// `ring-error-dropped`: the refusal ignored, and nothing fails).
#[test]
fn a_refused_ring_of_an_append_a_group_or_a_publish_fails_the_log() {
    let mut seen = Vec::new();
    for which in 0..3 {
        let io = UnitIO::new(false);
        let dyn_io: Arc<dyn IO> = io.clone();
        let config = small();
        let log = open_async(&io, config.clone(), 0);
        let quiet = durable_start_async(&log, &dyn_io);
        let mut rooms = three_admissions(&log, &dyn_io, 100);
        let quiet = quiet && settled(&log);
        let counted = log.segments.counted_io().clone();
        let counted_io: Arc<dyn IO> = counted.clone();
        counted.requests().drain(&*counted_io);
        let held = log.hold_buffer_room();
        let refused = in_frame(|| log.admit(&mut LogWait::default(), framed_len(100)))
            .err()
            .map(|e| e.to_string());
        let waiting = log.admitting();
        let p = vec![5u8; 100];
        let [one, group, publish] = &mut rooms;
        let at = in_frame(|| match which {
            0 => log.append(one, &header(), &[&p]),
            1 => *log
                .append_group_with(group, &[(header(), &[&p]), (header(), &[&p])], |_| {})
                .1
                .last()
                .unwrap(),
            _ => {
                let mut block = AlignedBlock::zeroed(config.chunk_bytes as usize);
                block.bytes_mut()[record::HEADER..record::HEADER + p.len()].copy_from_slice(&p);
                log.publish(publish, &header(), block, p.len(), |_| {}).1
            }
        });
        let failed = log.failure().is_some();
        let durable = in_frame(|| log.wait_durable(&mut LogWait::default(), at))
            .err()
            .map(|e| e.to_string());
        let admit = in_frame(|| log.admit(&mut LogWait::default(), framed_len(10)))
            .err()
            .map(|e| e.to_string());
        drop((rooms, held, counted, counted_io));
        drop(log);
        seen.push((which, quiet, refused, waiting, failed, durable, admit));
    }
    for (which, quiet, refused, waiting, failed, durable, admit) in seen {
        assert!(quiet, "{which}: the units did not end within {SETTLE:?}");
        assert!(
            refused
                .as_deref()
                .is_some_and(|e| e.contains("a closed database's log")),
            "{which}: the waiter's own admission: {refused:?}"
        );
        assert!(waiting, "{which}: no admitter left waiting");
        let refusal = "the log's ring refused at a closed database";
        assert_eq!(
            (
                failed,
                durable.as_deref().is_some_and(|e| e.contains(refusal)),
                admit.as_deref().is_some_and(|e| e.contains(refusal))
            ),
            (true, true, true),
            "{which}: (the log failed, wait_durable refused, the next admission refused): {durable:?} {admit:?}"
        );
    }
}

/// D2c §3.5: a caller's ring takes its count before it moves the door. While the door is rung,
/// a ring takes one count and lets it go, the requests' count back as it was; on a sealed count
/// a ring returns the closed error and leaves the door quiet (mutant `ring-before-count`: the
/// door moved first, left rung with no count after the refusal).
#[test]
fn a_callers_ring_counts_before_the_door_and_a_refused_one_leaves_the_door_quiet() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    // The preparer at rest first: a request of the preparation the first block made due, in
    // flight, would count among the requests' outstanding; with everything appended already
    // durable, its finish's turn of the flush writes nothing.
    let rested = preparer_resting_within(&log);
    let counted = log.segments.counted_io().clone();
    let requests = counted.requests().clone();
    let hold = requests.hold_io_thread();
    let first = counted.ring_log_from_frame().is_ok();
    let (door_rung, count_rung) = (counted.log_door(), requests.outstanding());
    let second = counted.ring_log_from_frame().is_ok();
    let (door_again, count_again) = (counted.log_door(), requests.outstanding());
    drop(hold);
    let quiet = quiet && settled(&log);
    let counted_io: Arc<dyn IO> = counted.clone();
    requests.drain(&*counted_io);
    let refused = counted.ring_log_from_frame().err().map(|e| e.to_string());
    let (door_refused, count_refused) = (counted.log_door(), requests.outstanding());
    drop((requests, counted, counted_io));
    drop(log);
    assert!(quiet, "the units did not end within {SETTLE:?}");
    assert!(rested, "the preparer did not rest within {SETTLE:?}");
    assert!(first && second, "a ring on an open database refused");
    assert_eq!(door_rung, RUNG, "the door after the first ring");
    assert_eq!(
        (door_again, count_again),
        (RUNG, count_rung),
        "(the door, the count) after a ring of a rung door"
    );
    assert!(
        refused
            .as_deref()
            .is_some_and(|e| e.contains("a closed database's log")),
        "the ring on a sealed count: {refused:?}"
    );
    assert_eq!(
        (door_refused, count_refused),
        (0, 0),
        "(the door, the count) after the refused ring"
    );
}

/// D2c §3.6: an admission in a frame at the prepared end waits on the preparer: its first poll is
/// IO with the preparer's demand registered, while the worker's preparation is held at its first
/// write or sync; once that is answered, the worker's step moves the prepared end and finishes the
/// admission's wait, and the admission is then granted. This thread steps the preparer never.
#[test]
fn an_admission_in_a_frame_waits_on_the_preparer_its_worker_steps() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    // The preparer at rest first: the preparation the first block made due published and its
    // finish over, so the next one it starts is the test's to hold.
    let rested = preparer_resting_within(&log);
    // Every write and sync held; the room left before the prepared end taken; one admission past
    // it, outside any frame, makes the preparer due, and the worker's step is held at its first
    // write or sync, so the prepared end cannot move until the test answers.
    io.probe.hold_all(true);
    let held = log.hold_prepared_room();
    let prepared_held = log.room.prepared_end();
    let mut outside = LogWait::default();
    let made_due = matches!(
        log.admit(&mut outside, framed_len(100)),
        Ok(IOResult::IO(_))
    );
    let preparing = held_within(&io.probe);
    let claims = super::preparer::CLAIMS.with(|n| n.get());
    let mut wait = LogWait::default();
    let first = in_frame(|| log.admit(&mut wait, framed_len(100)));
    let first_is_io = matches!(first, Ok(IOResult::IO(_)));
    let first_wait = match &first {
        Ok(IOResult::IO(c)) => Some(c.0.clone()),
        _ => None,
    };
    let demanded = wait.demand.is_some();
    let stepped_in_frame = super::preparer::CLAIMS.with(|n| n.get()) - claims;
    let prepared_before = log.room.prepared_end();
    // Let go: the worker's preparation ends, moves the prepared end and wakes the admitters, with
    // the room still held; the buffer's own bound keeps that room's grant from fitting, so the
    // room goes back only after, and then the admission is granted.
    io.probe.hold_all(false);
    let answered = answer_all(&log, &io.probe);
    // The wait first: the worker's step publishes the prepared end before it wakes the
    // admitters, so a wait seen finished orders the read of the prepared end after it.
    let woke = first_wait.as_ref().is_some_and(finished_within);
    let prepared_after = log.room.prepared_end();
    drop(held);
    let granted = in_frames(&dyn_io, || log.admit(&mut wait, framed_len(100))).is_ok();
    let stepped_here = super::preparer::CLAIMS.with(|n| n.get()) - claims;
    let settled_after = settled(&log);
    drop((first, first_wait, wait, outside));
    drop(log);
    assert!(
        quiet && settled_after,
        "the units did not end within {SETTLE:?}"
    );
    assert!(rested, "the preparer did not rest within {SETTLE:?}");
    assert!(
        made_due && preparing,
        "the worker's preparation not held: (made due {made_due}, held {preparing})"
    );
    assert_eq!(
        prepared_before, prepared_held,
        "the prepared end moved before the test answered"
    );
    assert!(
        first_is_io && demanded,
        "the frame's admission did not wait on the preparer: (IO {first_is_io}, demand {demanded})"
    );
    assert_eq!(stepped_in_frame, 0, "the preparer stepped in the frame");
    assert!(answered, "the held requests not answered within {SETTLE:?}");
    assert!(
        woke,
        "the frame's wait not finished by the worker's preparation"
    );
    assert!(
        prepared_after > prepared_before,
        "the worker's step did not move the prepared end: {prepared_before} then {prepared_after}"
    );
    assert!(granted, "the admission never granted");
    assert_eq!(
        stepped_here, 0,
        "the preparer stepped on the polling thread"
    );
}

/// Whether the log's preparer rests within [`SETTLE`], looked at once a millisecond: more than a
/// segment prepared ahead of what is admitted, and then its task idle. The prepared end is
/// published inside a running step, and a step with more to do is queued again without passing
/// idle, so idle read after it comes after that finish's turn of the flush was taken. With more
/// than a segment prepared ahead and no admission waiting past the prepared end, no later step
/// starts a preparation: the task rests, and no worker writes or moves the prepared end until an
/// admission brings the reach within a segment of it.
fn preparer_resting_within(log: &Log) -> bool {
    let deadline = Instant::now() + SETTLE;
    loop {
        let (ahead, segment) = log.prepared_ahead();
        if ahead > segment && log.preparer().idle() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Whether `c` finished within [`SETTLE`], looked at once a millisecond: for a wait another thread
/// finishes.
fn finished_within(c: &Completion) -> bool {
    let deadline = Instant::now() + SETTLE;
    while !c.finished() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

/// Whether the probe holds a request within [`SETTLE`], looked at once a millisecond: for a
/// request another thread makes.
fn held_within(probe: &Probe) -> bool {
    let deadline = Instant::now() + SETTLE;
    while probe.queued().is_empty() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

/// Whether the probe holds a delivered read within [`SETTLE`], looked at once a millisecond: for a
/// read the IO thread makes.
fn read_held_within(probe: &Probe) -> bool {
    let deadline = Instant::now() + SETTLE;
    while probe.reads_held() == 0 {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

/// D2c §3.6 (child): in a frame, a log whose preparer no worker drives is the named error, never
/// a step in place: the preparer's steps on this thread are unchanged (mutant
/// `frame-runs-preparer`: the step runs, and the frame census ends the child at the segment
/// file's open).
#[cfg(all(unix, not(miri)))]
#[test]
fn an_undriven_preparer_found_in_a_frame_is_an_error_and_never_a_step() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::an_undriven_preparer_found_in_a_frame_is_an_error_and_never_a_step"
        ),
        || {
            let io = UnitIO::new(false);
            let dyn_io: Arc<dyn IO> = io.clone();
            let log = open_async(&io, small(), 0);
            let quiet = durable_start_async(&log, &dyn_io);
            let held = log.hold_prepared_room();
            let claims = super::preparer::CLAIMS.with(|n| n.get());
            let refused = in_frame(|| log.admit(&mut LogWait::default(), framed_len(100)))
                .err()
                .map(|e| e.to_string());
            let stepped_here = super::preparer::CLAIMS.with(|n| n.get()) - claims;
            drop(held);
            let settled_after = settled(&log);
            drop(log);
            assert!(
                quiet && settled_after,
                "the units did not end within {SETTLE:?}"
            );
            assert!(
                refused.as_deref().is_some_and(|e| e.contains(
                    "the log's preparer has no worker driving it, found in a cooperative frame"
                )),
                "the admission in a frame: {refused:?}"
            );
            assert_eq!(stepped_here, 0, "the preparer stepped in the frame");
        },
    );
    child_ended_cleanly(ended);
}

/// A log in a database opened async over the real ring, its files under `dir` and written
/// buffered, with one worker.
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
fn open_async_on_the_ring(dir: &tempfile::TempDir) -> (Arc<dyn IO>, AloneLog) {
    let ring: Arc<dyn IO> = Arc::new(crate::io::UringIO::new().unwrap());
    let base = dir.path().join("ring");
    let config = LogConfig {
        direct: false,
        ..small()
    };
    let log = Log::open_async(ring.clone(), base.to_str().unwrap(), config, 1).unwrap();
    (ring, log)
}

/// D2c §3.10 (child): §3.1 over the real ring: an admission in a frame that must wait returns IO,
/// the frame census holds, and it is granted once the IO thread's flush lands.
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
#[test]
fn over_the_ring_an_admission_in_a_frame_that_must_wait_rings_the_log_and_is_granted() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::over_the_ring_an_admission_in_a_frame_that_must_wait_rings_the_log_and_is_granted"
        ),
        || {
            let dir = tempfile::tempdir().unwrap();
            let (ring, log) = open_async_on_the_ring(&dir);
            let quiet = durable_start_async(&log, &ring);
            // On the ring a write is durable once delivered, so any flush's write raises
            // durability, the IO thread's or a worker's. The preparer at rest first, its finish's
            // turn of the flush taken with everything appended already durable, and then the IO
            // thread held across the first poll: nothing writes, so nothing that poll rechecks
            // can be granted yet.
            let rested = preparer_resting_within(&log);
            append(&log, &ring, 100);
            let held = log.hold_buffer_room();
            let mut wait = LogWait::default();
            let hold = log.segments.counted_io().requests().hold_io_thread();
            let first = in_frame(|| log.admit(&mut wait, framed_len(100)));
            let first_is_io = matches!(first, Ok(IOResult::IO(_)));
            drop(hold);
            let granted = in_frames(&ring, || log.admit(&mut wait, framed_len(100))).is_ok();
            let settled_after = settled(&log);
            drop((first, held, wait));
            drop(log);
            assert!(
                quiet && settled_after,
                "the units did not end within {SETTLE:?}"
            );
            assert!(rested, "the preparer did not rest within {SETTLE:?}");
            assert!(first_is_io, "the admission did not return its wait");
            assert!(granted, "the admission never granted");
        },
    );
    child_ended_cleanly(ended);
}

/// D2c §3.10 (child): §3.3 over the real ring: with a waiter present and the IO thread held, an
/// append, a group and a publish in a frame ring the door and the frame census holds; let go, the
/// unit makes them durable and the waiter is done.
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
#[test]
fn over_the_ring_an_append_a_group_and_a_publish_in_a_frame_ring_and_submit_nothing() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::over_the_ring_an_append_a_group_and_a_publish_in_a_frame_ring_and_submit_nothing"
        ),
        || {
            let dir = tempfile::tempdir().unwrap();
            let (ring, log) = open_async_on_the_ring(&dir);
            let config = LogConfig {
                direct: false,
                ..small()
            };
            let quiet = durable_start_async(&log, &ring);
            // The preparer at rest first, as in §3.1 over the ring: no worker's flush writes, and
            // so makes nothing durable, while the IO thread is held.
            let rested = preparer_resting_within(&log);
            let mut rooms = three_admissions(&log, &ring, 100);
            let lsn = append(&log, &ring, 100);
            let counted = log.segments.counted_io().clone();
            let hold = counted.requests().hold_io_thread();
            let mut waiter = LogWait::default();
            let registered = matches!(
                in_frame(|| log.wait_durable(&mut waiter, lsn)),
                Ok(IOResult::IO(_))
            );
            let p = vec![9u8; 100];
            let ends = in_frame(|| the_three_appends(&log, &mut rooms, &p, &config));
            let door = counted.log_door();
            let failure = log.failure();
            drop(hold);
            let last = *ends.iter().max().unwrap();
            let done = in_frames(&ring, || log.wait_durable(&mut waiter, last)).is_ok();
            let settled_after = settled(&log);
            drop((rooms, waiter, counted));
            drop(log);
            assert!(
                quiet && settled_after,
                "the units did not end within {SETTLE:?}"
            );
            assert!(rested, "the preparer did not rest within {SETTLE:?}");
            assert!(registered, "the waiter did not register");
            assert_eq!(door, RUNG, "the door while its unit is held");
            assert_eq!(failure, None, "the log failed");
            assert!(done, "the waiter never done");
        },
    );
    child_ended_cleanly(ended);
}

/// D2c §3.7 (child): a rollback's read-back in a frame of a database opened async: its read queued
/// in the transaction slot's record and made on the IO thread, the polling thread making no backend
/// call, its bytes exact. The record is held until the read's request has wholly retired, the
/// request still counted, and is then at rest with the count let go (mutants `readback-in-place`:
/// the read made in place, which the frame census ends the child at; `uncount-before-rest`: the
/// count let go before the record rests).
#[test]
fn a_rollbacks_read_back_in_a_frame_is_queued_in_its_slots_record_and_read_on_the_io_thread() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::a_rollbacks_read_back_in_a_frame_is_queued_in_its_slots_record_and_read_on_the_io_thread"
        ),
        || {
            let io = UnitIO::new(false);
            let dyn_io: Arc<dyn IO> = io.clone();
            let log = open_async(&io, small(), 1);
            let quiet = durable_start_async(&log, &dyn_io);
            let (at, end, payload) = durable_record(&log, &dyn_io, 300, 1);
            // The preparer at rest and the log's units over once the record is durable: no
            // request but the test's own is counted from here.
            let rested = preparer_resting_within(&log) && settled(&log);
            let requests = log.segments.counted_io().requests().clone();
            io.probe.clear();
            let mut back = ReadBack::default();
            // The IO thread held across the first poll, so the read is still queued when it
            // returns; the read's release paused before its record rests.
            let hold = requests.hold_io_thread();
            let (paused, go) = requests.pause_slot(SlotPause::BeforeRest);
            let first = in_frame(|| log.read_record(&mut back, 1, at, end));
            let read = queued_wait(&first);
            drop(hold);
            let at_pause = paused.recv_timeout(SETTLE).ok();
            let retired = read
                .as_ref()
                .is_some_and(|c| c.finished() && !c.is_owned());
            let held = (requests.slots_at_rest(), requests.outstanding());
            let _ = go.send(());
            let got = in_frames(&dyn_io, || log.read_record(&mut back, 1, at, end))
                .map(|(_, range)| back.bytes(range).to_vec());
            let let_go = slots_let_go(&requests);
            let calls = io.probe.calls();
            drop((first, read, back, requests));
            drop(log);
            assert!(quiet, "the units did not end within {SETTLE:?}");
            assert!(
                rested,
                "the preparer did not rest, or the log's units did not end, within {SETTLE:?}"
            );
            assert_eq!(
                at_pause,
                Some(1),
                "the release of slot 1's record never came before its rest"
            );
            assert!(
                retired,
                "the read's request not wholly retired at its record's release"
            );
            assert_eq!(
                held,
                (Log::ASYNC_SLOTS - 1, 1),
                "(the slot records at rest, the requests counted) while the record is held"
            );
            assert_eq!(
                got.as_deref().ok(),
                Some(&payload[..]),
                "the record's payload read back"
            );
            assert!(
                let_go,
                "the slot records not at rest, or the count not let go, within {SETTLE:?}"
            );
            assert!(
                has(&on(&calls, IO_THREAD), "pread"),
                "the IO thread made no read: {calls:?}"
            );
            assert!(
                on(&calls, &this_thread()).is_empty(),
                "the polling thread made backend calls: {calls:?}"
            );
        },
    );
    child_ended_cleanly(ended);
}

/// D2c §3.7, the record held until the read physically ends: a read-back's read that its backend
/// has delivered and still owns keeps its slot record held, its request counted and unretired, and
/// the read-back on that read, reading no other region. Only the backend's letting the read go
/// retires the request, rests the record and lets the read-back read on, the record's bytes exact
/// (mutant `slot-ends-at-delivery`: the request ended from the read's callback, at its delivery).
/// The backend is MemoryIO behind the probe, which delivers the read inline and holds its
/// retirement as a backend that still owns a delivered request, as a ring does until its reap; no
/// kernel ring is involved.
#[test]
fn a_slot_record_stays_held_until_the_backend_lets_go_of_its_delivered_read() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    let (at, end, payload) = durable_record_across_a_segment(&log, &dyn_io);
    // The preparer at rest and the log's units over once the record is durable: no
    // request but the test's own is counted from here.
    let rested = preparer_resting_within(&log) && settled(&log);
    let segment = small().segment_bytes;
    let across = at.get() / segment != (end - 1) / segment;
    let requests = log.segments.counted_io().requests().clone();
    io.probe.clear();
    io.probe.hold_io_reads.store(true, Ordering::SeqCst);
    let mut back = ReadBack::default();
    // The IO thread is held until the first poll has returned, so the poll returns the read's own
    // request on any schedule: a read already delivered would return its retirement's wait.
    let hold = requests.hold_io_thread();
    let first = in_frame(|| log.read_record(&mut back, 0, at, end));
    let read = queued_wait(&first);
    drop(hold);
    let delivered = read_held_within(&io.probe);
    // Only the first read held: a later one is never held, so the one let-go below leaves no read
    // owned.
    io.probe.hold_io_reads.store(false, Ordering::SeqCst);
    let owned = read.as_ref().is_some_and(|c| c.finished() && c.is_owned());
    let held = (requests.slots_at_rest(), requests.outstanding());
    let next = in_frame(|| log.read_record(&mut back, 0, at, end));
    let next_said = said(&next);
    let wait = queued_wait(&next);
    let on_its_read = wait.as_ref().is_some_and(|w| !w.finished()) && back.reading();
    let reads_while_owned = io
        .probe
        .calls()
        .iter()
        .filter(|c| c.what == "pread")
        .count();
    io.probe.let_reads_go();
    // The request retires from the carrier's retirement, which the IO thread may register on only
    // after this let-go: it is looked at once the read-back's wait for it has finished.
    let woke = wait.as_ref().is_some_and(finished_within);
    let retired = read.as_ref().is_some_and(|c| c.finished() && !c.is_owned());
    let let_go_then = slots_let_go(&requests);
    let got = in_frames(&dyn_io, || log.read_record(&mut back, 0, at, end))
        .map(|(_, range)| back.bytes(range).to_vec());
    let let_go = slots_let_go(&requests);
    drop((first, read, next, wait, back, requests));
    drop(log);
    assert!(quiet, "the units did not end within {SETTLE:?}");
    assert!(
        rested,
        "the preparer did not rest, or the log's units did not end, within {SETTLE:?}"
    );
    assert!(
        across,
        "the record from {} to {end} lies in one segment",
        at.get()
    );
    assert!(
        delivered,
        "the backend held no delivered read within {SETTLE:?}"
    );
    assert!(
        owned,
        "the read's request retired, or not finished, while its backend still owns its delivered \
         read"
    );
    assert_eq!(
        held,
        (Log::ASYNC_SLOTS - 1, 1),
        "(the slot records at rest, the requests counted) while the backend owns the delivered read"
    );
    assert!(
        on_its_read,
        "the read-back's next poll while its delivered read is owned: {next_said}"
    );
    assert_eq!(
        reads_while_owned, 1,
        "the reads that reached the backend while the delivered read is owned"
    );
    assert!(
        woke,
        "the read-back's wait not finished once the backend let its read go"
    );
    assert!(
        retired,
        "the read's request not retired once the backend let its read go"
    );
    assert!(
        let_go_then,
        "the slot records not at rest, or the count not let go, once the backend let the read go"
    );
    assert_eq!(
        got.as_deref().ok(),
        Some(&payload[..]),
        "the record's payload read back"
    );
    assert!(
        let_go,
        "the slot records not at rest, or the count not let go, within {SETTLE:?}"
    );
}

/// D2c §3.8 (a): a read-back's next read, made once its last read's request has retired and before
/// that read's slot record rests, is never admitted by the retirement: its queue returns the
/// record's rest wait, which the release finishes once the record rests, and the read is then
/// queued and served, the record's bytes exact (mutant `admit-at-retirement`: the take refused
/// with an error, which the queue returns there).
#[test]
fn a_read_backs_next_read_waits_for_its_slot_record_to_rest_not_for_its_last_reads_retirement() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    let (at, end, payload) = durable_record_across_a_segment(&log, &dyn_io);
    // The preparer at rest and the log's units over once the record is durable: no
    // request but the test's own is counted from here.
    let rested = preparer_resting_within(&log) && settled(&log);
    let segment = small().segment_bytes;
    let across = at.get() / segment != (end - 1) / segment;
    let requests = log.segments.counted_io().requests().clone();
    let mut back = ReadBack::default();
    let hold = requests.hold_io_thread();
    let (paused, go) = requests.pause_slot(SlotPause::BeforeRest);
    let first = in_frame(|| log.read_record(&mut back, 0, at, end));
    let first_queued = queued_wait(&first).is_some();
    drop(hold);
    let at_pause = paused.recv_timeout(SETTLE).ok();
    let next = in_frame(|| log.read_record(&mut back, 0, at, end));
    let next_said = said(&next);
    let wait = queued_wait(&next);
    let waiting = wait.as_ref().is_some_and(|w| !w.finished()) && !back.reading();
    let held = (
        requests.slots_at_rest(),
        requests.outstanding(),
        requests.slot_rest_woke(0),
    );
    let _ = go.send(());
    let woke = wait.as_ref().is_some_and(finished_within);
    let got = in_frames(&dyn_io, || log.read_record(&mut back, 0, at, end))
        .map(|(_, range)| back.bytes(range).to_vec());
    let let_go = slots_let_go(&requests);
    let let_goes = requests.slot_rest_woke(0);
    drop((first, next, wait, back, requests));
    drop(log);
    assert!(quiet, "the units did not end within {SETTLE:?}");
    assert!(
        rested,
        "the preparer did not rest, or the log's units did not end, within {SETTLE:?}"
    );
    assert!(
        across,
        "the record from {} to {end} lies in one segment",
        at.get()
    );
    assert!(first_queued, "the first read not returned queued");
    assert_eq!(
        at_pause,
        Some(0),
        "the release of slot 0's record never came before its rest"
    );
    assert!(
        waiting,
        "the next read's queue while its record is held: {next_said}"
    );
    assert_eq!(
        held,
        (Log::ASYNC_SLOTS - 1, 1, 0),
        "(the slot records at rest, the requests counted, the let-goes that woke a taker) while \
         the record is held"
    );
    assert!(woke, "the rest wait not finished once the record rested");
    assert_eq!(let_goes, 1, "the rest wait's let-goes that woke a taker");
    assert_eq!(
        got.as_deref().ok(),
        Some(&payload[..]),
        "the record's payload read back"
    );
    assert!(
        let_go,
        "the slot records not at rest, or the count not let go, within {SETTLE:?}"
    );
}

/// D2c §3.8 (b): a read-back dropped with its read in flight leaves its slot record held until that
/// read's release makes it rest: a new transaction's read-back in the same slot, its first read
/// queued while the release is paused before the rest, is returned the record's rest wait, and is
/// queued and served once the record rests (mutant `admit-at-retirement`).
#[test]
fn a_read_back_dropped_with_its_read_in_flight_leaves_its_slot_to_the_next_once_its_record_rests() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    let (at, end, payload) = durable_record(&log, &dyn_io, 300, 7);
    // The preparer at rest and the log's units over once the record is durable: no
    // request but the test's own is counted from here.
    let rested = preparer_resting_within(&log) && settled(&log);
    let requests = log.segments.counted_io().requests().clone();
    let hold = requests.hold_io_thread();
    let mut dropped = ReadBack::default();
    let first = in_frame(|| log.read_record(&mut dropped, 0, at, end));
    let first_queued = queued_wait(&first).is_some();
    drop((first, dropped));
    let (paused, go) = requests.pause_slot(SlotPause::BeforeRest);
    drop(hold);
    let at_pause = paused.recv_timeout(SETTLE).ok();
    let mut back = ReadBack::default();
    let next = in_frame(|| log.read_record(&mut back, 0, at, end));
    let next_said = said(&next);
    let wait = queued_wait(&next);
    let waiting = wait.as_ref().is_some_and(|w| !w.finished()) && !back.reading();
    let held = (requests.slots_at_rest(), requests.outstanding());
    let _ = go.send(());
    let woke = wait.as_ref().is_some_and(finished_within);
    let got = in_frames(&dyn_io, || log.read_record(&mut back, 0, at, end))
        .map(|(_, range)| back.bytes(range).to_vec());
    let let_go = slots_let_go(&requests);
    drop((next, wait, back, requests));
    drop(log);
    assert!(quiet, "the units did not end within {SETTLE:?}");
    assert!(
        rested,
        "the preparer did not rest, or the log's units did not end, within {SETTLE:?}"
    );
    assert!(
        first_queued,
        "the dropped read-back's read not returned queued"
    );
    assert_eq!(
        at_pause,
        Some(0),
        "the release of slot 0's record never came before its rest"
    );
    assert!(
        waiting,
        "the next read-back's queue while the record is held: {next_said}"
    );
    assert_eq!(
        held,
        (Log::ASYNC_SLOTS - 1, 1),
        "(the slot records at rest, the requests counted) while the record is held"
    );
    assert!(woke, "the rest wait not finished once the record rested");
    assert_eq!(
        got.as_deref().ok(),
        Some(&payload[..]),
        "the record's payload read back"
    );
    assert!(
        let_go,
        "the slot records not at rest, or the count not let go, within {SETTLE:?}"
    );
}

/// D2c §3.8 (c): a read-back whose read is cancelled before an IO thread takes it gives the record
/// up once that read's request retires, and the read's release is paused before the record rests:
/// the slot's next read-back is returned the record's rest wait, and is queued and served once the
/// record rests; the cancelled read reached no backend (mutant `admit-at-retirement`).
#[test]
fn a_read_back_given_up_after_a_cancel_leaves_its_slot_to_the_next_once_its_record_rests() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    let (at, end, payload) = durable_record(&log, &dyn_io, 300, 9);
    // The preparer at rest and the log's units over once the record is durable: no
    // request but the test's own is counted from here.
    let rested = preparer_resting_within(&log) && settled(&log);
    let requests = log.segments.counted_io().requests().clone();
    io.probe.clear();
    let hold = requests.hold_io_thread();
    let mut cancelled = ReadBack::default();
    let first = in_frame(|| log.read_record(&mut cancelled, 0, at, end));
    let read = queued_wait(&first);
    if let Some(c) = &read {
        c.abort();
    }
    let (paused, go) = requests.pause_slot(SlotPause::BeforeRest);
    drop(hold);
    let at_pause = paused.recv_timeout(SETTLE).ok();
    let given_up = in_frame(|| log.read_record(&mut cancelled, 0, at, end))
        .err()
        .map(|e| e.to_string());
    let mut back = ReadBack::default();
    let next = in_frame(|| log.read_record(&mut back, 0, at, end));
    let next_said = said(&next);
    let wait = queued_wait(&next);
    let waiting = wait.as_ref().is_some_and(|w| !w.finished()) && !back.reading();
    let held = (requests.slots_at_rest(), requests.outstanding());
    let _ = go.send(());
    let woke = wait.as_ref().is_some_and(finished_within);
    let got = in_frames(&dyn_io, || log.read_record(&mut back, 0, at, end))
        .map(|(_, range)| back.bytes(range).to_vec());
    let let_go = slots_let_go(&requests);
    let reads = io
        .probe
        .calls()
        .iter()
        .filter(|c| c.what == "pread")
        .count();
    drop((first, read, cancelled, next, wait, back, requests));
    drop(log);
    assert!(quiet, "the units did not end within {SETTLE:?}");
    assert!(
        rested,
        "the preparer did not rest, or the log's units did not end, within {SETTLE:?}"
    );
    assert_eq!(
        at_pause,
        Some(0),
        "the release of slot 0's record never came before its rest"
    );
    assert!(
        given_up.is_some(),
        "the read-back whose read was cancelled did not give its record up"
    );
    assert!(
        waiting,
        "the next read-back's queue while the record is held: {next_said}"
    );
    assert_eq!(
        held,
        (Log::ASYNC_SLOTS - 1, 1),
        "(the slot records at rest, the requests counted) while the record is held"
    );
    assert!(woke, "the rest wait not finished once the record rested");
    assert_eq!(
        got.as_deref().ok(),
        Some(&payload[..]),
        "the record's payload read back"
    );
    assert!(
        let_go,
        "the slot records not at rest, or the count not let go, within {SETTLE:?}"
    );
    assert_eq!(reads, 1, "the reads that reached the backend");
}

/// D2c §3.8, the rest wait's order: a taker registered on a slot's rest wait while its record is
/// held is not finished before the record rests. At a pause in the release just before the rest,
/// its wait is unfinished and a take tried there is refused with a wait, admitting nothing; once
/// the record rests the wait is finished and the read is queued and served (mutant
/// `rest-wait-before-rest`: the let-go before the rest, which finishes the wait before the pause).
#[test]
fn a_slots_rest_wait_is_finished_only_once_its_record_rests() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    let (at, end, payload) = durable_record(&log, &dyn_io, 300, 13);
    // The preparer at rest and the log's units over once the record is durable: no
    // request but the test's own is counted from here.
    let rested = preparer_resting_within(&log) && settled(&log);
    let requests = log.segments.counted_io().requests().clone();
    let hold = requests.hold_io_thread();
    let mut dropped = ReadBack::default();
    let first = in_frame(|| log.read_record(&mut dropped, 0, at, end));
    let first_queued = queued_wait(&first).is_some();
    drop((first, dropped));
    let mut back = ReadBack::default();
    let registered = in_frame(|| log.read_record(&mut back, 0, at, end));
    let wait = queued_wait(&registered);
    let registered_waiting = wait.as_ref().is_some_and(|w| !w.finished()) && !back.reading();
    let (paused, go) = requests.pause_slot(SlotPause::BeforeRest);
    drop(hold);
    let at_pause = paused.recv_timeout(SETTLE).ok();
    let early = wait.as_ref().is_some_and(Completion::finished);
    let retried = in_frame(|| log.read_record(&mut back, 0, at, end));
    let refused = queued_wait(&retried).is_some() && !back.reading();
    let held = (requests.slots_at_rest(), requests.outstanding());
    let _ = go.send(());
    let woke = wait.as_ref().is_some_and(finished_within);
    // Each wait bounded: the mutant's lost let-go would leave this read-back's wait unfinished.
    let got = in_frames_within(|| log.read_record(&mut back, 0, at, end))
        .map(|got| got.map(|(_, range)| back.bytes(range).to_vec()));
    let let_go = slots_let_go(&requests);
    drop((registered, retried, wait, back, requests));
    drop(log);
    assert!(quiet, "the units did not end within {SETTLE:?}");
    assert!(
        rested,
        "the preparer did not rest, or the log's units did not end, within {SETTLE:?}"
    );
    assert!(
        first_queued,
        "the dropped read-back's read not returned queued"
    );
    assert!(
        registered_waiting,
        "the read-back's take while the record is held not returned a wait"
    );
    assert_eq!(
        at_pause,
        Some(0),
        "the release of slot 0's record never came before its rest"
    );
    assert!(
        !early,
        "the rest wait finished while the record is not at rest"
    );
    assert!(
        refused,
        "a take while the record is not at rest not refused with a wait"
    );
    assert_eq!(
        held,
        (Log::ASYNC_SLOTS - 1, 1),
        "(the slot records at rest, the requests counted) while the record is held"
    );
    assert!(woke, "the rest wait not finished once the record rested");
    assert_eq!(
        got.as_ref().and_then(|got| got.as_deref().ok()),
        Some(&payload[..]),
        "the record's payload read back, each wait finished within {SETTLE:?}"
    );
    assert!(
        let_go,
        "the slot records not at rest, or the count not let go, within {SETTLE:?}"
    );
}

/// D2c §3.8 (d), (e): a take that found its slot record held, and registers on the rest wait only
/// once the release has made the record rest and before its let-go, takes the record at its second
/// attempt. That wait, left registered, is finished by the let-go for no one and admits nothing
/// twice: one record taken, one request counted, each read made once (mutant
/// `rest-wait-no-second-take`: the wait returned once registered).
#[test]
fn a_take_registered_after_its_record_rests_takes_it_and_its_wait_left_admits_nothing_twice() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    let (at, end, payload) = durable_record(&log, &dyn_io, 300, 17);
    // The preparer at rest and the log's units over once the record is durable: no
    // request but the test's own is counted from here.
    let rested = preparer_resting_within(&log) && settled(&log);
    let requests = log.segments.counted_io().requests().clone();
    io.probe.clear();
    let hold = requests.hold_io_thread();
    let mut dropped = ReadBack::default();
    let first = in_frame(|| log.read_record(&mut dropped, 0, at, end));
    let first_queued = queued_wait(&first).is_some();
    drop((first, dropped));
    let (rest_paused, rest_go) = requests.pause_slot(SlotPause::BeforeRest);
    drop(hold);
    let at_rest = rest_paused.recv_timeout(SETTLE).ok();
    // The taker, on a thread of its own: its first take refused, it pauses before it registers.
    let (registering, register) = requests.pause_slot(SlotPause::Registering);
    let taker_log: Arc<Log> = Arc::clone(&log);
    let taker = std::thread::spawn(move || {
        let mut back = ReadBack::default();
        let polled = in_frame(|| taker_log.read_record(&mut back, 0, at, end));
        let queued = queued_wait(&polled).is_some() && back.reading();
        drop(polled);
        (back, queued)
    });
    let at_registering = registering.recv_timeout(SETTLE).ok();
    let (let_go_paused, let_go_go) = requests.pause_slot(SlotPause::BeforeLetGo);
    let _ = rest_go.send(());
    let at_let_go = let_go_paused.recv_timeout(SETTLE).ok();
    let _ = register.send(());
    let (back, taken) = match taker.join() {
        Ok((back, queued)) => (Some(back), queued),
        Err(_) => (None, false),
    };
    let one_taken = (requests.slots_at_rest(), requests.outstanding());
    let _ = let_go_go.send(());
    let got = back.map(|mut back| {
        in_frames(&dyn_io, || log.read_record(&mut back, 0, at, end))
            .map(|(_, range)| back.bytes(range).to_vec())
    });
    let let_go = slots_let_go(&requests);
    let let_goes = requests.slot_rest_woke(0);
    let reads = io
        .probe
        .calls()
        .iter()
        .filter(|c| c.what == "pread")
        .count();
    drop(requests);
    drop(log);
    assert!(quiet, "the units did not end within {SETTLE:?}");
    assert!(
        rested,
        "the preparer did not rest, or the log's units did not end, within {SETTLE:?}"
    );
    assert!(
        first_queued,
        "the dropped read-back's read not returned queued"
    );
    assert_eq!(
        (at_rest, at_registering, at_let_go),
        (Some(0), Some(0), Some(0)),
        "(the release before the rest, the take before it registers, the release before the \
         let-go) never reached"
    );
    assert!(
        taken,
        "the take registered after its record rested did not take it at its second attempt"
    );
    assert_eq!(
        one_taken,
        (Log::ASYNC_SLOTS - 1, 2),
        "(the slot records at rest, the requests counted) once the record was taken again, the \
         last read's count not yet let go"
    );
    assert_eq!(
        got.as_ref().and_then(|got| got.as_deref().ok()),
        Some(&payload[..]),
        "the record's payload read back"
    );
    assert!(
        let_go,
        "the slot records not at rest, or the count not let go, within {SETTLE:?}"
    );
    assert_eq!(
        let_goes, 1,
        "the let-goes that found the wait left registered"
    );
    assert_eq!(reads, 2, "the reads that reached the backend");
}

/// D2c §3.9: a read-back's read cancelled before an IO thread takes it reaches no backend: the IO
/// thread ends it with no call, its slot record comes back to rest and its count is let go, and
/// the read-back's next step gives the record up with the cancel (mutant `slot-cancel-submits`: the
/// cancelled read submitted all the same).
#[test]
fn a_read_back_cancelled_before_its_take_reaches_no_backend_and_gives_its_record_and_count_back() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let log = open_async(&io, small(), 1);
    let quiet = durable_start_async(&log, &dyn_io);
    let (at, end, _) = durable_record(&log, &dyn_io, 300, 19);
    // The preparer at rest and the log's units over once the record is durable: no
    // request but the test's own is counted from here.
    let rested = preparer_resting_within(&log) && settled(&log);
    let requests = log.segments.counted_io().requests().clone();
    io.probe.clear();
    let hold = requests.hold_io_thread();
    let mut back = ReadBack::default();
    let first = in_frame(|| log.read_record(&mut back, 0, at, end));
    let read = queued_wait(&first);
    if let Some(c) = &read {
        c.abort();
    }
    drop(hold);
    let let_go = slots_let_go(&requests);
    let given_up = in_frames(&dyn_io, || log.read_record(&mut back, 0, at, end))
        .err()
        .map(|e| e.to_string());
    let calls = io.probe.calls();
    drop((first, read, back, requests));
    drop(log);
    assert!(quiet, "the units did not end within {SETTLE:?}");
    assert!(
        rested,
        "the preparer did not rest, or the log's units did not end, within {SETTLE:?}"
    );
    assert!(
        !calls.iter().any(|c| c.what == "pread"),
        "a read cancelled before its take reached the backend: {calls:?}"
    );
    assert!(
        let_go,
        "the slot records not at rest, or the count not let go, within {SETTLE:?}"
    );
    assert!(
        given_up.is_some(),
        "the read-back's step after the cancel did not give its record up"
    );
}

/// D2c §3.10 (child): §3.7 over the real ring: a rollback's read-back in a frame, of a record lying
/// across two segments, each read queued in the slot's record and made by the IO thread, the frame
/// census holding; its bytes exact, the record at rest and the count let go after.
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
#[test]
fn over_the_ring_a_rollbacks_read_back_in_a_frame_is_queued_in_its_slots_record() {
    let ended = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::over_the_ring_a_rollbacks_read_back_in_a_frame_is_queued_in_its_slots_record"
        ),
        || {
            let dir = tempfile::tempdir().unwrap();
            let (ring, log) = open_async_on_the_ring(&dir);
            let quiet = durable_start_async(&log, &ring);
            let (at, end, payload) = durable_record_across_a_segment(&log, &ring);
            // The preparer at rest and the log's units over once the record is durable: no
            // request but the test's own is counted from here.
            let rested = preparer_resting_within(&log) && settled(&log);
            let requests = log.segments.counted_io().requests().clone();
            let mut back = ReadBack::default();
            let got = in_frames(&ring, || log.read_record(&mut back, 1, at, end))
                .map(|(_, range)| back.bytes(range).to_vec());
            let let_go = slots_let_go(&requests);
            drop((back, requests));
            drop(log);
            assert!(quiet, "the units did not end within {SETTLE:?}");
            assert!(
                rested,
                "the preparer did not rest, or the log's units did not end, within {SETTLE:?}"
            );
            assert_eq!(
                got.as_deref().ok(),
                Some(&payload[..]),
                "the record's payload read back"
            );
            assert!(
                let_go,
                "the slot records not at rest, or the count not let go, within {SETTLE:?}"
            );
        },
    );
    child_ended_cleanly(ended);
}

/// A record of `len` payload bytes counting up from `fill`, appended outside any group and made
/// durable: where it starts and ends, and its payload.
fn durable_record(
    log: &Arc<Log>,
    io: &Arc<dyn IO>,
    len: usize,
    fill: u8,
) -> (GroupStart, Lsn, Vec<u8>) {
    let payload: Vec<u8> = (0..len).map(|i| fill.wrapping_add(i as u8)).collect();
    let mut admitted = drive(io, || log.admit(&mut LogWait::default(), framed_len(len))).unwrap();
    let (at, end) = log.append_with(&mut admitted, &header(), &[&payload], |_| {});
    drop(admitted);
    drive(io, || log.wait_durable(&mut LogWait::default(), end)).unwrap();
    (at, end, payload)
}

/// [`durable_record`] lying across the end of the log's first segment, so it is read back in two
/// reads, one a segment.
fn durable_record_across_a_segment(log: &Arc<Log>, io: &Arc<dyn IO>) -> (GroupStart, Lsn, Vec<u8>) {
    let segment = small().segment_bytes;
    while segment - log.end() % segment > 2 * BLOCK {
        append(log, io, 996);
    }
    durable_record(log, io, 3 * BLOCK as usize, 5)
}

/// `f` polled inside a frame until done, each wait waited for outside it for at most [`SETTLE`],
/// looked at once a millisecond: `None` once a wait is not finished by then, so a wait no one
/// finishes fails its test instead of hanging it.
fn in_frames_within<T>(mut f: impl FnMut() -> Result<IOResult<T>>) -> Option<Result<T>> {
    loop {
        match in_frame(&mut f) {
            Ok(IOResult::Done(t)) => return Some(Ok(t)),
            Ok(IOResult::IO(c)) => {
                if !finished_within(&c.0) {
                    return None;
                }
            }
            Err(e) => return Some(Err(e)),
        }
    }
}

/// What a poll that returned `IO` waits on, if it did.
fn queued_wait<T>(polled: &Result<IOResult<T>>) -> Option<Completion> {
    match polled {
        Ok(IOResult::IO(c)) => Some(c.0.clone()),
        _ => None,
    }
}

/// What a poll came to, for a failure's message.
fn said<T>(polled: &Result<IOResult<T>>) -> String {
    match polled {
        Ok(IOResult::IO(_)) => "IO".to_string(),
        Ok(IOResult::Done(_)) => "done".to_string(),
        Err(e) => e.to_string(),
    }
}

/// Whether every slot record is at rest and no request is counted, within [`SETTLE`], looked at
/// once a millisecond: for a release the IO thread makes.
fn slots_let_go(requests: &Requests) -> bool {
    let deadline = Instant::now() + SETTLE;
    while requests.slots_at_rest() != Log::ASYNC_SLOTS || requests.outstanding() != 0 {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

// ASYNC1 D2d (ii): over a pumped backend the database's IO thread steps what its units submit, so a
// flush rung from a frame lands with no one waiting for it and no one else stepping.

/// D2d §6.1 test 2, over `backend`, with no worker: a durability wait in a frame rings the log's
/// door while the IO thread is held before its idle with its registration withdrawn, so only the
/// step it owes after the log's bit can drive the unit's requests, and is dropped at once. Once the
/// append's first block is durable the test steps nothing, so only the IO thread steps the backend.
/// At its next idle the unit's write and its sync have landed and nothing is counted (mutants
/// `no-io-driver` and `owed-only-for-queued`, which owes no step after the log's bit: the log is
/// short of the append there).
fn the_io_thread_drives_a_unit_rung_from_a_frame_whose_wait_is_dropped(backend: Arc<dyn IO>) {
    let log = Log::open_async(backend.clone(), BASE, small(), 0).unwrap();
    let quiet = durable_start_async(&log, &backend);
    let lsn = append(&log, &backend, 100);
    let requests = log.segments.counted_io().requests().clone();
    let held = crate::storage::atoll::testing::io_thread_held_before_idle_unregistered(&requests);
    let reached = held.is_some();
    let idle = requests.on_io_idle();
    let mut wait = LogWait::default();
    let rang = matches!(
        in_frame(|| log.wait_durable(&mut wait, lsn)),
        Ok(IOResult::IO(_))
    );
    drop(wait);
    drop(held);
    let idled = idle.recv_timeout(SETTLE).is_ok();
    let at_idle = (log.durable() >= lsn, requests.outstanding());
    drop(log);
    assert!(quiet, "the units did not end within {SETTLE:?}");
    assert!(reached, "the IO thread never reached its pause before idle");
    assert!(rang, "the wait in the frame returned no registration");
    assert!(idled, "the IO thread's idle note");
    assert_eq!(
        at_idle,
        (true, 0),
        "(the log durable to the append, requests counted) at the IO thread's idle: the unit's \
         write or sync left in the backend"
    );
}

#[test]
fn over_crash_io_the_io_thread_drives_a_unit_rung_from_a_frame_whose_wait_is_dropped() {
    the_io_thread_drives_a_unit_rung_from_a_frame_whose_wait_is_dropped(Arc::new(
        crate::io::crash::CrashIO::new(10),
    ));
}

#[cfg(feature = "io_memory_yield")]
#[test]
fn over_deferred_memory_the_io_thread_drives_a_unit_rung_from_a_frame_whose_wait_is_dropped() {
    the_io_thread_drives_a_unit_rung_from_a_frame_whose_wait_is_dropped(Arc::new(
        crate::io::MemoryYieldIO::new(),
    ));
}

// ASYNC1 D2d (v): a log write its backend writes short is continued, part by part, by the IO
// thread's unit, and lands once, at the whole (D2D v1.4).

/// The parts a write of `len` bytes from `from` is submitted in when every write longer than a
/// block writes half of it in whole blocks: each part asks for the rest.
fn halving_parts(from: Lsn, len: u64) -> Vec<(Lsn, u64)> {
    let mut parts = Vec::new();
    let (mut at, mut left) = (from, len);
    loop {
        parts.push((at, left));
        let wrote = if left > BLOCK {
            (left / 2 / BLOCK * BLOCK).max(BLOCK)
        } else {
            left
        };
        if wrote == left {
            return parts;
        }
        at += wrote;
        left -= wrote;
    }
}

/// A log over `io` with one write in flight and chunks of 32 KiB, its first block durable, its
/// preparer at rest, every request of that retired and its units ended, and its counts, notes
/// and the probe's write calls started from none: the log, where it is durable to, whether all of
/// that came within [`SETTLE`], and each record's rests so far.
fn short_write_log(io: &Arc<UnitIO>) -> (AloneLog, Lsn, bool, Vec<usize>) {
    short_write_log_opened(io, false)
}

/// [`short_write_log`], the log opened async when `opened_async`, its first block made durable
/// as such a log's is.
fn short_write_log_opened(
    io: &Arc<UnitIO>,
    opened_async: bool,
) -> (AloneLog, Lsn, bool, Vec<usize>) {
    let dyn_io: Arc<dyn IO> = io.clone();
    let config = LogConfig {
        chunk_bytes: 32 << 10,
        writes_in_flight: 1,
        ..small()
    };
    let (log, quiet) = if opened_async {
        let log = open_async(io, config, 1);
        let quiet = durable_start_async(&log, &dyn_io);
        (log, quiet)
    } else {
        let log = open(io, config);
        let quiet = durable_start(&log, &dyn_io);
        (log, quiet)
    };
    // Before the counts start: the preparer at rest; every request of the warm-up retired, each
    // retirement's ring made before its request is uncounted; and the units those rings start
    // ended.
    let rested = preparer_resting_within(&log);
    let requests = log.segments.counted_io().requests().clone();
    let retired = within_settle(|| requests.outstanding() == 0);
    let quiet = quiet && rested && retired && settled(&log);
    let from = log.durable();
    reset(&log.counts);
    log.counts.parts.lock().clear();
    log.counts.delivered_on.lock().clear();
    log.counts.landings.store(0, Ordering::SeqCst);
    log.counts.rings.store(0, Ordering::SeqCst);
    io.probe.clear();
    io.probe.write_calls.store(0, Ordering::SeqCst);
    let rests = log.writes_for_test().3;
    (log, from, quiet, rests)
}

/// Records of 996 bytes appended until 13,000 bytes past `from`: the last one's end, and how
/// many. Waited for, they are written as a write of 12 KiB of whole blocks or more and one of the
/// padded last block, within one chunk.
fn append_past(log: &Arc<Log>, io: &Arc<dyn IO>, from: Lsn) -> (Lsn, usize) {
    let (mut end, mut appended) = (from, 0);
    while end < from + 13_000 {
        end = append(log, io, 996);
        appended += 1;
    }
    (end, appended)
}

/// The records read back from the log's start, each its payload's length if the payload is all
/// 7s and 0 if not; or the reader's error, with how many records it read first.
fn read_back(io: &Arc<UnitIO>) -> std::result::Result<Vec<usize>, String> {
    let mut reader = Log::reader(io.clone(), BASE, &small(), 0);
    let mut lens = Vec::new();
    loop {
        match reader.next_record() {
            Ok(Some(r)) => {
                let p = &r.payload;
                lens.push(if p.iter().all(|&b| b == 7) {
                    p.len()
                } else {
                    0
                });
            }
            Ok(None) => return Ok(lens),
            Err(e) => return Err(format!("{e}, after {} records", lens.len())),
        }
    }
}

/// Whether `done` came true within [`SETTLE`], looked at once a millisecond.
fn within_settle(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + SETTLE;
    while !done() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

/// The writes' state read with the IO thread held before its idle: (records at rest, owed, in
/// flight, each record's rests since `before`), and whether the hold was reached.
fn writes_at_idle(log: &Log, before: &[usize]) -> ((u32, u32, usize, Vec<usize>), bool) {
    let held = crate::storage::atoll::testing::io_thread_held_before_idle(log.requests_for_test());
    let reached = held.is_some();
    let (free, owed, in_flight, rests) = log.writes_for_test();
    drop(held);
    let since = rests.iter().zip(before).map(|(r, b)| r - b).collect();
    ((free, owed, in_flight, since), reached)
}

/// The parts noted, as the writes they make: each write's parts, the halving of the first, the
/// next write starting where the last ended; `None` if the parts are not such writes.
fn halved_writes(parts: &[(Lsn, u64)]) -> Option<Vec<Vec<(Lsn, u64)>>> {
    let mut writes = Vec::new();
    let mut i = 0;
    while i < parts.len() {
        let (from, len) = parts[i];
        let write = halving_parts(from, len);
        if parts.get(i..i + write.len()) != Some(&write[..]) {
            return None;
        }
        i += write.len();
        if parts.get(i).is_some_and(|&(next, _)| next != from + len) {
            return None;
        }
        writes.push(write);
    }
    Some(writes)
}

/// D2d v1.4 T1: writes its backend writes half of at a time land whole, in parts: 13,000 bytes of
/// records or more waited for are a write from the ring of 12 KiB or more, in parts of 4 KiB and
/// more, and a padded last block.
/// Each part asks for the rest, from where the last stopped. At the first part's retirement, the
/// IO thread's unit held at its start, the record is owed and its one place held. Every part is
/// submitted with that one write in flight, and every rest by the IO thread's unit, outside any
/// part's callback or observer. Each write lands once; the records appended read back, every one
/// whole; and at the end the record is at rest, rested once a write. The durability wait is
/// bounded, so a lost rest fails an assertion rather than hanging (mutants: `log-short-fails`, a
/// short part failing the log; `log-rest-in-callback`, the rest submitted from the part's
/// callback; `log-place-let-go`, the record rested at a short part). Over memory delivering
/// inline, in a database opened synchronously; and over the probe's pumped backend, delivering
/// only at a step and saying what it found, in a database opened async, whose IO thread and
/// executor worker step it, so a part's callback runs on whichever of them stepped.
fn a_write_written_short_is_continued_part_by_part_and_lands_once(pumped: bool) {
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    io.probe.defer.store(pumped, Ordering::SeqCst);
    let (log, from, quiet, before) = short_write_log_opened(&io, pumped);
    let records_before = read_back(&io);
    io.probe.halve.store(true, Ordering::SeqCst);
    let (arrived, go) = log.holds.set("unit.started");
    let (end, appended) = append_past(&log, &dyn_io, from);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, end);
    let at_unit = arrived.recv_timeout(SETTLE).is_ok();
    let at_first_retirement = log.writes_for_test();
    let parts_at_unit = log.counts.parts.lock().len();
    drop(go);
    let durable_in_time = within_settle(|| log.durable() >= end);
    let waited = durable_in_time.then(|| drive(&dyn_io, || log.wait_durable(&mut wait, end)));
    drop(wait);
    let settled = settled(&log);
    let (state, reached) = writes_at_idle(&log, &before);
    io.probe.halve.store(false, Ordering::SeqCst);
    let parts = log.counts.parts.lock().clone();
    let delivered_on = log.counts.delivered_on.lock().clone();
    let (writes, landings) = (
        log.counts.writes.load(Ordering::SeqCst),
        log.counts.landings.load(Ordering::SeqCst),
    );
    let failure = log.failure();
    let lens = read_back(&io);
    drop(log);
    assert!(
        quiet && settled,
        "the log's units did not end within {SETTLE:?}"
    );
    assert!(reached, "the IO thread never reached its pause before idle");
    assert!(
        failure.is_none(),
        "the log failed at a short part: {failure:?}"
    );
    // Read only if the unit started with the first part alone submitted; whether it did is
    // asserted last.
    if at_unit && parts_at_unit == 1 {
        assert_eq!(
            (
                at_first_retirement.0,
                at_first_retirement.1,
                at_first_retirement.2
            ),
            (0, 1, 1),
            "(records at rest, owed, writes in flight) at the first part's retirement, the unit \
             held: a short part's retirement let its record and its place go"
        );
    }
    assert!(
        durable_in_time && waited.is_some_and(|w| w.is_ok()),
        "the write never became durable within {SETTLE:?}"
    );
    let asked: Vec<(Lsn, u64)> = parts.iter().map(|p| (p.from, p.len)).collect();
    assert_eq!(
        asked.first().map(|p| p.0),
        Some(from),
        "the first part's start"
    );
    let Some(halved) = halved_writes(&asked) else {
        panic!(
            "the parts asked for are not each the rest of a write from where the last stopped: \
             {asked:?}"
        );
    };
    assert_eq!(
        halved.len(),
        writes,
        "the writes the parts make, against the writes planned"
    );
    assert!(
        halved.iter().any(|w| w.len() >= 3),
        "no write from the ring continued twice: {asked:?}"
    );
    assert_eq!(
        parts
            .iter()
            .map(|p| (p.in_place, p.in_flight))
            .collect::<Vec<_>>(),
        vec![(false, 1); parts.len()],
        "(written from a published block, writes in flight) at each part's submission"
    );
    // A write's first part is its plan's; every later part is a rest.
    let mut rests = Vec::new();
    let mut i = 0;
    for w in &halved {
        rests.extend(
            parts[i + 1..i + w.len()]
                .iter()
                .map(|p| (p.thread.as_str(), p.depth)),
        );
        i += w.len();
    }
    assert_eq!(
        rests,
        vec![(IO_THREAD, 0); asked.len() - halved.len()],
        "(thread, part callbacks and observers under way) at each rest's submission: a rest \
         submitted inside a part's delivery"
    );
    assert_eq!(
        landings, writes,
        "the writes' landings, against the writes planned"
    );
    assert_eq!(
        state,
        (1, 0, 0, vec![writes]),
        "(records at rest, owed, writes in flight, the record's rests) at the end"
    );
    let (Ok(before), Ok(after)) = (&records_before, &lens) else {
        panic!("the log did not read back: {records_before:?}, then {lens:?}");
    };
    assert_eq!(
        (&after[..before.len()], &after[before.len()..]),
        (&before[..], &vec![996; appended][..]),
        "the records read back, before and after the appends: the appended not all there, or not \
         all whole"
    );
    // The first part, submitted by the waiting thread's flush: inline, delivered there; pumped,
    // delivered by one of the database's own threads, its IO thread's driver or its executor's
    // worker, which step the backend while that thread waits without stepping.
    let first = (
        parts[0].thread.as_str(),
        delivered_on.first().map(String::as_str),
    );
    let by_the_database = first
        .1
        .is_some_and(|t| t == IO_THREAD || t.starts_with("atoll-worker-"));
    assert!(
        first.0 != IO_THREAD
            && if pumped {
                by_the_database
            } else {
                first.1 == Some(first.0)
            },
        "(the first part's submitter, its callback's thread): {first:?}"
    );
    assert!(
        at_unit && parts_at_unit == 1,
        "the IO thread's unit started with {parts_at_unit} parts submitted, not the first alone \
         (started: {at_unit})"
    );
}

#[test]
fn a_write_written_short_inline_is_continued_part_by_part_and_lands_once() {
    a_write_written_short_is_continued_part_by_part_and_lands_once(false);
}

#[test]
fn a_write_written_short_over_a_pumped_backend_opened_async_is_continued_part_by_part() {
    a_write_written_short_is_continued_part_by_part_and_lands_once(true);
}

/// D2d v1.4 T1, a record published in place: its write, from the record's own block, is
/// continued from that block, part by part, and the block is let go once the record rests.
#[test]
fn a_write_from_a_record_published_in_place_is_continued_from_its_block() {
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let (log, from, quiet, before) = short_write_log(&io);
    io.probe.halve.store(true, Ordering::SeqCst);
    let p = vec![7u8; 11_000];
    let mut block = AlignedBlock::zeroed(32 << 10);
    block.bytes_mut()[record::HEADER..record::HEADER + p.len()].copy_from_slice(&p);
    let room = framed_len(p.len()).next_multiple_of(BLOCK) + BLOCK;
    let mut admitted = drive(&dyn_io, || log.admit(&mut LogWait::default(), room)).unwrap();
    let (at, lsn) = log.publish(&mut admitted, &header(), block, p.len(), |_| {});
    drop(admitted);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, lsn);
    let durable_in_time = within_settle(|| log.durable() >= lsn);
    let waited = durable_in_time.then(|| drive(&dyn_io, || log.wait_durable(&mut wait, lsn)));
    drop(wait);
    let settled = settled(&log);
    let (state, reached) = writes_at_idle(&log, &before);
    io.probe.halve.store(false, Ordering::SeqCst);
    let parts = log.counts.parts.lock().clone();
    let failure = log.failure();
    let lens = read_back(&io);
    drop(log);
    assert!(
        quiet && settled,
        "the log's units did not end within {SETTLE:?}"
    );
    assert!(reached, "the IO thread never reached its pause before idle");
    assert!(failure.is_none(), "the log failed: {failure:?}");
    assert!(
        durable_in_time && waited.is_some_and(|w| w.is_ok()),
        "the record never became durable within {SETTLE:?}"
    );
    let from_block: Vec<&PartNote> = parts.iter().filter(|p| p.from >= at.get()).collect();
    assert!(
        from_block.iter().all(|p| p.in_place && p.in_flight == 1),
        "a part of the in-place write submitted without its record's block, or with other than \
         its one place in flight: {from_block:?}"
    );
    let in_place: Vec<(Lsn, u64)> = from_block.iter().map(|p| (p.from, p.len)).collect();
    assert!(in_place.len() >= 3, "the in-place write's parts: {parts:?}");
    assert_eq!(
        in_place,
        halving_parts(in_place[0].0, in_place[0].1),
        "the in-place write's parts, each the rest from where the last stopped"
    );
    assert_eq!(
        (state.0, state.1, state.2),
        (1, 0, 0),
        "(at rest, owed, in flight) at the end"
    );
    assert!(
        lens.as_ref().is_ok_and(|l| l.contains(&11_000)),
        "the record published in place did not read back whole: {lens:?} from {from}"
    );
}

/// D2d v1.4 T2: with files that do not write durably, no sync is made for a write until its last
/// part is delivered: its second part held after the first landed half, the IO thread's units
/// rung and run while it is held make no sync; once it is let go and the write is whole, the
/// first sync after it makes the log durable to the append (mutant `log-lands-a-part`: each part
/// landing its own range, which the unit's sync start syncs while the rest is held).
#[test]
fn no_sync_is_made_for_a_write_before_its_last_part_is_delivered() {
    let io = UnitIO::new(false);
    let dyn_io: Arc<dyn IO> = io.clone();
    let (log, from, quiet, before) = short_write_log(&io);
    io.probe.halve.store(true, Ordering::SeqCst);
    io.probe.hold_call.store(2, Ordering::SeqCst);
    let (end, _) = append_past(&log, &dyn_io, from);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, end);
    // The second part held; then the doorbell rung twice more, each ring a unit.
    let held = within_settle(|| io.probe.queued().len() == 1);
    for _ in 0..2 {
        log.ring();
        settled(&log);
    }
    let syncs_while_held = io.probe.syncs();
    let landed_while_held = log.counts.landings.load(Ordering::SeqCst);
    let calls_at_release = io.probe.calls().len();
    io.probe.hold_call.store(0, Ordering::SeqCst);
    let answered = answer_all(&log, &io.probe);
    let waited = drive(&dyn_io, || log.wait_durable(&mut wait, end));
    let settled = settled(&log);
    let (state, reached) = writes_at_idle(&log, &before);
    io.probe.halve.store(false, Ordering::SeqCst);
    let calls = io.probe.calls();
    let durable = log.durable();
    drop((wait, log));
    assert!(
        quiet && settled && answered,
        "the log's units did not end within {SETTLE:?}"
    );
    assert!(reached, "the IO thread never reached its pause before idle");
    assert!(held, "the write's second part was never held");
    assert_eq!(
        (syncs_while_held, landed_while_held),
        (0, 0),
        "(syncs, landings) while the write's second part was held: a sync began before the \
         write was whole"
    );
    let first_sync = calls.iter().position(|c| c.what == "sync");
    assert!(
        first_sync.is_some_and(|s| s >= calls_at_release),
        "the first sync came before the write's held part was let go: {calls:?}"
    );
    assert!(
        waited.is_ok() && durable >= end,
        "durable to {durable}, short of {end}"
    );
    assert_eq!(
        (state.0, state.1, state.2),
        (1, 0, 0),
        "(at rest, owed, in flight) at the end"
    );
}

/// D2d v1.4 T3: a part that writes nothing fails the log with a short write, the write never
/// lands, and its record rests once.
#[test]
fn a_part_that_writes_nothing_fails_the_log_and_rests_its_record() {
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let (log, from, quiet, before) = short_write_log(&io);
    io.probe.zero_call.store(1, Ordering::SeqCst);
    let (end, _) = append_past(&log, &dyn_io, from);
    let waited = drive(&dyn_io, || log.wait_durable(&mut LogWait::default(), end));
    let settled = settled(&log);
    let (state, reached) = writes_at_idle(&log, &before);
    io.probe.zero_call.store(0, Ordering::SeqCst);
    let (failure, landings) = (log.failure(), log.counts.landings.load(Ordering::SeqCst));
    drop(log);
    assert!(
        quiet && settled,
        "the log's units did not end within {SETTLE:?}"
    );
    assert!(reached, "the IO thread never reached its pause before idle");
    assert!(
        waited.is_err(),
        "the wait ended well past a part that wrote nothing"
    );
    assert_eq!(
        (failure, landings),
        (Some(CompletionError::ShortWrite), 0),
        "(the log's failure, landings)"
    );
    assert_eq!(
        state,
        (1, 0, 0, vec![1]),
        "(at rest, owed, in flight, rests) at the end"
    );
}

/// D2d v1.4 T4: the log failing while a write's record is owed: its first part written short and
/// the record owed, the IO thread's unit held at its start, the log failed, then the unit let go:
/// it rests the record, submitting nothing (mutant `log-owed-kept-on-failure`: the unit's return
/// on a failed log before its owed pass, which leaves the record owed).
#[test]
fn a_record_owed_when_the_log_fails_is_rested_by_the_unit() {
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let (log, from, quiet, before) = short_write_log(&io);
    io.probe.halve.store(true, Ordering::SeqCst);
    let (arrived, go) = log.holds.set("unit.started");
    let (end, _) = append_past(&log, &dyn_io, from);
    let mut wait = LogWait::default();
    let _ = log.wait_durable(&mut wait, end);
    let at_unit = arrived.recv_timeout(SETTLE).is_ok();
    let owed_then = log.writes_for_test();
    log.fail_for_test();
    let calls_before = io.probe.write_calls.load(Ordering::SeqCst);
    drop(go);
    let settled = settled(&log);
    let (state, reached) = writes_at_idle(&log, &before);
    let calls_after = io.probe.write_calls.load(Ordering::SeqCst);
    io.probe.halve.store(false, Ordering::SeqCst);
    drop((wait, log));
    assert!(
        quiet && settled,
        "the log's units did not end within {SETTLE:?}"
    );
    assert!(reached, "the IO thread never reached its pause before idle");
    assert!(at_unit, "the IO thread's unit never started");
    assert_eq!(
        (owed_then.1, owed_then.2),
        (1, 1),
        "(owed, in flight) with the unit held: the first part did not leave the record owed"
    );
    assert_eq!(
        state,
        (1, 0, 0, vec![1]),
        "(at rest, owed, in flight, rests) once the unit ran: an owed record stayed held after \
         the log failed"
    );
    assert_eq!(
        calls_after, calls_before,
        "a part submitted on a failed log"
    );
}

/// D2d v1.4 T5: a refused part rests its record, once, with no ring, the log failed: (a) a first
/// part, refused on the waiting thread's own flush, outside any unit; (b) a rest, refused in the
/// IO thread's unit (mutant `log-refused-kept`: the refusal only failing the log, the record kept
/// and its place with it).
fn a_refused_part_rests_its_record_once_with_no_ring(continuation: bool) {
    let io = UnitIO::new(true);
    let dyn_io: Arc<dyn IO> = io.clone();
    let (log, from, quiet, before) = short_write_log(&io);
    io.probe.halve.store(continuation, Ordering::SeqCst);
    io.probe
        .refuse_call
        .store(if continuation { 2 } else { 1 }, Ordering::SeqCst);
    let (end, _) = append_past(&log, &dyn_io, from);
    let waited = drive(&dyn_io, || log.wait_durable(&mut LogWait::default(), end));
    let settled = settled(&log);
    let (state, reached) = writes_at_idle(&log, &before);
    io.probe.refuse_call.store(0, Ordering::SeqCst);
    io.probe.halve.store(false, Ordering::SeqCst);
    let (failure, rings) = (log.failure(), log.counts.rings.load(Ordering::SeqCst));
    drop(log);
    assert!(
        quiet && settled,
        "the log's units did not end within {SETTLE:?}"
    );
    assert!(reached, "the IO thread never reached its pause before idle");
    assert!(
        waited.is_err() && failure.is_some(),
        "the log did not fail at a refused part"
    );
    assert_eq!(
        state,
        (1, 0, 0, vec![1]),
        "(at rest, owed, in flight, rests) after the refusal"
    );
    // A continuation's one ring is the first part's observer's, which owed it.
    assert_eq!(rings, usize::from(continuation), "the doorbell's rings");
}

#[test]
fn a_refused_first_part_rests_its_record_once_with_no_ring() {
    a_refused_part_rests_its_record_once_with_no_ring(false);
}

#[test]
fn a_refused_continuation_rests_its_record_once_with_no_ring() {
    a_refused_part_rests_its_record_once_with_no_ring(true);
}

/// D2d v1.4 §4, the log's physical-call census. The log calls a file only where it opens its
/// segments, submits a write's part (`submit_part`), submits a sync member (`submit_sync`), and
/// reads a record. No callback or retirement observer it hands over (the argument of a
/// completion's making or of `on_retired`, and the last argument of `submit_then`) reaches any of
/// those through any function of the log it calls, followed call by call: no callback or observer
/// submits (mutant `log-rest-in-callback`: the part's callback calling `submit_part`).
#[test]
fn no_callback_or_observer_of_the_log_reaches_a_file_call_or_a_submission() {
    let path = crate::storage::atoll::testing::atoll_dir().join("log/mod.rs");
    let text = std::fs::read_to_string(&path).unwrap();
    let code = crate::source_test::without_test_modules(&crate::source_test::code_of(&text));
    // Each function of the file: its name and its body, to the next function.
    let starts: Vec<(usize, String)> = code
        .match_indices("fn ")
        .filter(|(at, _)| *at == 0 || !code.as_bytes()[at - 1].is_ascii_alphanumeric())
        .map(|(at, _)| (at, identifier_at(&code, at + 3)))
        .filter(|(_, name)| !name.is_empty())
        .collect();
    let mut bodies: std::collections::BTreeMap<String, Vec<std::ops::Range<usize>>> =
        std::collections::BTreeMap::new();
    for (i, (at, name)) in starts.iter().enumerate() {
        let end = starts.get(i + 1).map_or(code.len(), |(next, _)| *next);
        bodies.entry(name.clone()).or_default().push(*at..end);
    }
    let caller = |at: usize| -> String {
        starts
            .iter()
            .rev()
            .find(|(start, _)| *start <= at)
            .map(|(_, name)| name.clone())
            .unwrap_or_default()
    };
    let mut wrong = Vec::new();
    // The file calls, each where it is ruled.
    let mut files = 0;
    for (call, sites) in [
        (".pread(", &["read_record"][..]),
        (".pwrite(", &["open_segments", "submit_part"][..]),
        (".pwrite_durable(", &["submit_part"][..]),
        (".pwritev(", &[][..]),
        (".sync(", &[][..]),
        (".sync_data(", &["open_segments", "submit_sync"][..]),
        (".truncate(", &[][..]),
    ] {
        for (at, _) in code.match_indices(call) {
            files += 1;
            let site = caller(at);
            if !sites.contains(&site.as_str()) {
                wrong.push(format!("{call} in {site}, not where it is ruled"));
            }
        }
    }
    // What a callback or an observer may never reach.
    let forbidden = ["open_segments", "submit_part", "submit_sync", "read_record"];
    let mut handed = Vec::new();
    for opener in [
        "Completion::new_read(",
        "Completion::new_write(",
        "Completion::new_sync(",
        "on_retired(",
    ] {
        for (at, _) in code.match_indices(opener) {
            handed.push((caller(at), arguments(&code, at + opener.len())));
        }
    }
    for (at, _) in code.match_indices(".submit_then(") {
        let args = arguments(&code, at + ".submit_then(".len());
        // The observer: the last argument that is not empty, a trailing comma leaving one that is.
        let mut depth = 0;
        let mut from = args.start;
        let mut spans = Vec::new();
        for (i, ch) in code[args.clone()].char_indices() {
            match ch {
                '(' | '{' | '[' => depth += 1,
                ')' | '}' | ']' => depth -= 1,
                ',' if depth == 0 => {
                    spans.push(from..args.start + i);
                    from = args.start + i + 1;
                }
                _ => {}
            }
        }
        spans.push(from..args.end);
        let observer = spans
            .into_iter()
            .rev()
            .find(|span| !code[span.clone()].trim().is_empty())
            .expect("submit_then's arguments");
        handed.push((caller(at), observer));
    }
    for (site, span) in &handed {
        // The functions of the file it calls, and theirs, followed until none is new.
        let mut reached: std::collections::BTreeSet<String> = called(&code, span.clone(), &bodies);
        let mut frontier: Vec<String> = reached.iter().cloned().collect();
        while let Some(f) = frontier.pop() {
            for body in &bodies[&f] {
                for g in called(&code, body.clone(), &bodies) {
                    if reached.insert(g.clone()) {
                        frontier.push(g);
                    }
                }
            }
        }
        for f in forbidden {
            if reached.contains(f) {
                wrong.push(format!(
                    "a callback or an observer handed over in {site} reaches {f}"
                ));
            }
        }
    }
    assert!(
        files > 0 && !handed.is_empty() && wrong.is_empty(),
        "the log's file calls and submissions reached off their ruled sites: {wrong:#?} ({files} \
         file calls, {} callbacks and observers)",
        handed.len()
    );
}

/// The identifier starting at `at` in `code`.
fn identifier_at(code: &str, at: usize) -> String {
    code[at..]
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// The span of the argument list that opens just before `open`, to its closing parenthesis.
fn arguments(code: &str, open: usize) -> std::ops::Range<usize> {
    let mut depth = 1;
    for (i, ch) in code[open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            return open..open + i;
        }
    }
    open..code.len()
}

/// The functions of the file called in `span`: each identifier followed by an opening parenthesis
/// that names one.
fn called(
    code: &str,
    span: std::ops::Range<usize>,
    bodies: &std::collections::BTreeMap<String, Vec<std::ops::Range<usize>>>,
) -> std::collections::BTreeSet<String> {
    let text = &code[span];
    let bytes = text.as_bytes();
    let mut out = std::collections::BTreeSet::new();
    let mut i = 0;
    while i < bytes.len() {
        let starts = (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_')
            && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_'));
        if !starts {
            i += 1;
            continue;
        }
        let name = identifier_at(text, i);
        i += name.len();
        let next = text[i..].trim_start().chars().next();
        if next == Some('(') && bodies.contains_key(&name) {
            out.insert(name);
        }
    }
    out
}
