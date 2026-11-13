//! A cooperative frame's metadata loads, queued (DESIGN.md 22.18; ASYNC1 D2a §6): in a database
//! opened async, a miss inside a frame queues its read in its credit's record, the read's cold file
//! is opened and its directory synced as the read's first stage on an IO thread, and the backend
//! sees only a carrier of the IO thread's own. Every backend call is recorded with its thread.

use super::*;
use crate::io::clock::{Clock, MonotonicInstant, WallClockInstant};
use crate::io::{Buffer, Delivery, FileSyncType, MemoryIO, Stepped};
use crate::storage::atoll::cooperative::Frame;
use crate::storage::atoll::requests::{counted_async_for_test, counted_for_test};
use crate::storage::atoll::testing::drive;
#[cfg(all(unix, not(miri)))]
use crate::storage::atoll::testing::in_child;
use std::sync::mpsc;
use std::thread::ThreadId;

/// Every wait of these tests: one deadline for the whole operation.
const SETTLE: std::time::Duration = std::time::Duration::from_secs(20);

const BASE: &str = "/db/queued";

/// A backend call: which, and on which thread.
#[derive(Clone, Debug)]
struct Call {
    what: &'static str,
    thread: ThreadId,
    name: String,
}

/// MemoryIO, which delivers inline, with every open, directory sync and read recorded with its
/// thread, opens it can refuse, reads it can block inside their call, and reads whose retirement
/// it can hold: their callbacks run at once, and they retire only when let go.
#[derive(Default)]
struct Probe {
    inner: MemoryIO,
    calls: Mutex<Vec<Call>>,
    refuse_open: AtomicBool,
    /// Reads accepted and then given back refused, as an extension's or the Windows ring's are.
    refuse_reads: AtomicBool,
    hold_retirement: AtomicBool,
    held: Mutex<Vec<Completion>>,
    /// The next read's block: the thread that entered it is sent, and it goes on once let go.
    block: Mutex<Option<(mpsc::Sender<ThreadId>, mpsc::Receiver<()>)>>,
}

impl Probe {
    fn new() -> Arc<Self> {
        Arc::default()
    }

    fn record(&self, what: &'static str) {
        let thread = crate::thread::current();
        self.calls.lock().push(Call {
            what,
            thread: thread.id(),
            name: thread.name().unwrap_or("unnamed").to_string(),
        });
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().clone()
    }

    /// The calls made on `thread`.
    fn on(&self, thread: ThreadId) -> Vec<&'static str> {
        self.calls()
            .into_iter()
            .filter(|c| c.thread == thread)
            .map(|c| c.what)
            .collect()
    }

    fn count(&self, what: &str) -> usize {
        self.calls().iter().filter(|c| c.what == what).count()
    }

    /// Blocks the next read inside its call: the thread that enters it, and the sender that lets
    /// it go on.
    fn block_next_read(&self) -> (mpsc::Receiver<ThreadId>, mpsc::Sender<()>) {
        let (entered_tx, entered) = mpsc::channel();
        let (go, go_rx) = mpsc::channel();
        *self.block.lock() = Some((entered_tx, go_rx));
        (entered, go)
    }

    /// Retires every read whose retirement it holds.
    fn let_go(&self) {
        let held = std::mem::take(&mut *self.held.lock());
        for c in held {
            c.retire();
        }
    }
}

impl Clock for Probe {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for Probe {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.record("open");
        if self.refuse_open.load(Ordering::SeqCst) {
            return Err(LimboError::InternalError(
                "a probe's open refused".to_string(),
            ));
        }
        Ok(Arc::new(ProbeFile {
            file: self.inner.open_file(path, flags, direct)?,
            probe: self as *const Probe as usize,
        }))
    }
    fn sync_directory(&self, path: &str) -> Result<()> {
        self.record("sync_directory");
        self.inner.sync_directory(path)
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn delivery(&self) -> Delivery {
        self.inner.delivery()
    }
    fn step(&self) -> Result<()> {
        self.inner.step()
    }
    fn try_step(&self, key: crate::io::ReadyKey, ready: &std::task::Waker) -> Result<Stepped> {
        self.inner.try_step(key, ready)
    }
    fn says_what_a_step_found(&self) -> bool {
        self.inner.says_what_a_step_found()
    }
    fn withdraw(&self, key: crate::io::ReadyKey) {
        self.inner.withdraw(key)
    }
}

/// A probe's file: its reads recorded, blocked or held as its probe says.
struct ProbeFile {
    file: Arc<dyn File>,
    /// Its probe, which outlives every file it opens in these tests (each holds the probe).
    probe: usize,
}

impl ProbeFile {
    fn probe(&self) -> &Probe {
        // SAFETY: each test holds its probe until every file it opened has dropped.
        unsafe { &*(self.probe as *const Probe) }
    }
}

impl File for ProbeFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }
    fn pread(&self, at: u64, c: Completion) -> Result<Completion> {
        let probe = self.probe();
        probe.record("pread");
        let block = probe.block.lock().take();
        if let Some((entered, go)) = block {
            let _ = entered.send(crate::thread::current().id());
            let _ = go.recv();
        }
        if probe.refuse_reads.load(Ordering::SeqCst) {
            c.accept();
            c.refuse();
            return Err(LimboError::InternalError(
                "a probe's read refused".to_string(),
            ));
        }
        if probe.hold_retirement.load(Ordering::SeqCst) {
            // The callback now, as a delivery that keeps the buffer would run it; the retirement
            // when the probe lets it go. A fresh file reads nothing.
            c.accept();
            c.complete(0);
            probe.held.lock().push(c.clone());
            return Ok(c);
        }
        self.file.pread(at, c)
    }
    fn pwrite(&self, at: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.file.pwrite(at, buffer, c)
    }
    fn sync(&self, c: Completion, how: FileSyncType) -> Result<Completion> {
        self.file.sync(c, how)
    }
    fn size(&self) -> Result<u64> {
        self.file.size()
    }
    fn truncate(&self, n: u64, c: Completion) -> Result<Completion> {
        self.file.truncate(n, c)
    }
}

fn id(index: u64) -> MetaId {
    MetaId {
        tree: Tree::Map,
        class: SizeClass::new(0).unwrap(),
        level: 0,
        index,
    }
}

fn cache(counted: &Arc<Counted>, budget: usize) -> Arc<MetaCache> {
    Arc::new(MetaCache::new(
        counted.clone(),
        BASE,
        budget,
        Arc::new(Epochs::new()),
        0,
    ))
}

/// One look at page `index` inside a cooperative frame.
fn in_frame(cache: &Arc<MetaCache>, index: u64) -> Result<IOResult<MetaRef>> {
    let _frame = Frame::enter();
    cache.get(id(index))
}

/// The completion the last queued read was made with: the owner's, never handed to a backend.
fn last_read() -> Option<Completion> {
    LAST_READ.with(|r| r.borrow_mut().take())
}

/// Whether `done` came true within [`SETTLE`], looked at once a millisecond.
fn settle(mut done: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + SETTLE;
    while !done() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    true
}

/// D2a §6.1 (child): in a database opened async, a miss inside a frame on a cold file returns IO
/// with no backend call on the polling thread; the file's open, its directory's sync and the read
/// are made on an IO thread, in that order, and the page then reads (mutant: `queues()` false, so
/// the load runs in place and the census ends the child).
#[cfg(all(unix, not(miri)))]
#[test]
fn a_frames_metadata_load_on_a_cold_file_is_opened_synced_and_read_on_an_io_thread() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_frames_metadata_load_on_a_cold_file_is_opened_synced_and_read_on_an_io_thread"
        ),
        || {
            let probe = Probe::new();
            let counted = counted_async_for_test(probe.clone(), 4, 1);
            let io: Arc<dyn IO> = counted.clone();
            let cache = cache(&counted, 4);
            let me = crate::thread::current().id();
            let first = in_frame(&cache, 0);
            let polled = probe.on(me);
            let returned_io = matches!(first, Ok(IOResult::IO(_)));
            let served = drive(&io, || cache.get(id(0))).is_ok();
            let calls = probe.calls();
            drop((first, cache, io, counted));
            let elsewhere: Vec<(&str, &str)> = calls
                .iter()
                .filter(|c| c.thread != me)
                .map(|c| (c.what, c.name.as_str()))
                .collect();
            assert!(returned_io, "a frame's miss returned the page");
            assert!(
                polled.is_empty(),
                "backend calls on the polling thread: {polled:?}"
            );
            assert_eq!(
                elsewhere,
                [
                    ("open", "atoll-io"),
                    ("sync_directory", "atoll-io"),
                    ("pread", "atoll-io")
                ],
                "the read's stages and its read, by thread"
            );
            assert!(served, "the page not served");
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

/// D2a §6.1 over the ring (the auditor's #4839, child): in a database opened async over io_uring, a
/// frame's miss on a cold file is opened, locked, synced and read on the IO thread, which waits on
/// the file system where the ring's delivery thread never may, and the page then reads (mutant: the
/// IO thread marked as the ring's delivery thread, so the ring refuses its open and the child ends).
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
#[test]
fn a_frames_cold_load_over_the_ring_is_opened_and_read_on_an_io_thread() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_frames_cold_load_over_the_ring_is_opened_and_read_on_an_io_thread"
        ),
        || {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().join("db").to_str().unwrap().to_string();
            let ring: Arc<dyn IO> = Arc::new(crate::io::UringIO::new().unwrap());
            let counted = counted_async_for_test(ring, 4, 1);
            let io: Arc<dyn IO> = counted.clone();
            let cache = Arc::new(MetaCache::new(
                counted.clone(),
                &base,
                4,
                Arc::new(Epochs::new()),
                0,
            ));
            let first = in_frame(&cache, 0);
            let returned_io = matches!(first, Ok(IOResult::IO(_)));
            let served = drive(&io, || cache.get(id(0))).is_ok();
            let resident = cache.resident();
            drop((first, cache, io, counted));
            assert!(returned_io, "a frame's miss returned the page");
            assert!(served, "the page not served");
            assert_eq!(resident, 1, "pages resident once served");
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

/// D2a §6.9 (child): a synchronous database's miss inside a frame runs in place, and the census of
/// in-place calls in a frame ends the process at its first, the file's open, naming it (mutants:
/// the census removed, so the child ends 0; the rule without its mode arm, so the synchronous
/// database queues, finds no record for its credit and ends 0 with the load's error).
#[cfg(all(unix, not(miri)))]
#[test]
fn a_synchronous_databases_miss_in_a_frame_runs_in_place_and_the_census_ends_the_process() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_synchronous_databases_miss_in_a_frame_runs_in_place_and_the_census_ends_the_process"
        ),
        || {
            let probe = Probe::new();
            let counted = counted_for_test(probe.clone());
            let cache = cache(&counted, 4);
            let first = in_frame(&cache, 0);
            drop((first, cache, counted));
        },
    );
    if let Some(ended) = ended {
        assert!(
            ended.aborted
                && ended
                    .stderr
                    .contains("a file's open run in place inside a cooperative frame"),
            "the child ended {:?} (aborted {}, hung {}): {}",
            ended.code,
            ended.aborted,
            ended.hung,
            ended.stderr
        );
    }
}

/// D2a §6.3: a queued read cancelled before an IO thread takes it runs no syscall, not even its
/// cold file's open, and its record, its credit and its count are all let go (mutant: the claim
/// ignores the finished request, so its open and its read reach the backend).
#[test]
fn a_read_cancelled_before_its_take_runs_no_syscall_and_lets_everything_go() {
    let probe = Probe::new();
    let counted = counted_async_for_test(probe.clone(), 4, 1);
    let cache = cache(&counted, 4);
    let hold = counted.requests().hold_io_thread();
    let first = in_frame(&cache, 0);
    let read = last_read().expect("the queued read");
    read.abort();
    drop(hold);
    let requests = counted.requests().clone();
    let settled = settle(|| {
        requests.outstanding() == 0 && cache.credits.free() == 4 && requests.queued_at_rest() == 4
    });
    let (opens, reads) = (probe.count("open"), probe.count("pread"));
    let retired = !read.is_owned();
    drop((first, read, cache, counted, requests));
    assert_eq!(
        (opens, reads),
        (0, 0),
        "(opens, reads) of a read cancelled before its take"
    );
    assert!(
        settled,
        "the record, the credit or the count was not let go"
    );
    assert!(retired, "the cancelled request still owned");
}

/// D2a §6.4: a queued read whose backend runs its callback at once and keeps its buffer: the page
/// reads, and the record, the credit and the count stay held until the backend retires it, then
/// all three go and the owner's request retires (mutant: the request ended when its submission
/// returned, not at its carrier's retirement).
#[test]
fn a_queued_reads_record_and_credit_are_held_until_its_backend_retires_it() {
    let probe = Probe::new();
    probe.hold_retirement.store(true, Ordering::SeqCst);
    let counted = counted_async_for_test(probe.clone(), 4, 1);
    let io: Arc<dyn IO> = counted.clone();
    let cache = cache(&counted, 4);
    let first = in_frame(&cache, 0);
    let read = last_read().expect("the queued read");
    let requests = counted.requests().clone();
    let held = settle(|| probe.held.lock().len() == 1);
    let served = drive(&io, || cache.get(id(0))).is_ok();
    let at_callback = (
        requests.queued_at_rest(),
        cache.credits.free(),
        requests.outstanding(),
        read.is_owned(),
    );
    probe.let_go();
    let settled = settle(|| requests.outstanding() == 0);
    let after = (
        requests.queued_at_rest(),
        cache.credits.free(),
        requests.outstanding(),
        read.is_owned(),
    );
    drop((first, read, cache, io, counted, requests));
    assert!(
        held && served,
        "the read not held by its backend, or the page not served"
    );
    assert_eq!(
        at_callback,
        (3, 3, 1, true),
        "(records at rest, credits free, requests counted, the request owned) with the buffer \
         still the backend's"
    );
    assert!(settled, "the retirement left the request counted");
    assert_eq!(
        after,
        (4, 4, 0, false),
        "(records at rest, credits free, requests counted, the request owned) after the retirement"
    );
}

/// D2a §6.5: 128 pages asked under one read credit, each ask in a frame: one read with the backend,
/// the other 127 refused a credit and waiting, then every page served with never more than one
/// read with the backend (the auditor's metadata probe, queued; mutant: a refused miss queuing
/// under a forged credit, whose record is held).
#[test]
fn loads_in_frames_under_one_read_credit_keep_one_read_with_the_backend() {
    const PAGES: u64 = 128;
    let probe = Probe::new();
    probe.hold_retirement.store(true, Ordering::SeqCst);
    let counted = counted_async_for_test(probe.clone(), 1, 1);
    let cache = cache(&counted, 1);
    let asks: Vec<_> = (0..PAGES).map(|i| in_frame(&cache, i)).collect();
    let held = settle(|| probe.held.lock().len() == 1);
    let failed = asks.iter().filter(|a| a.is_err()).count();
    let at_asks = (probe.count("pread"), cache.resident(), failed);
    drop(asks);
    let (mut served, mut most) = (0, 0);
    for i in 0..PAGES {
        let ok = settle(|| match in_frame(&cache, i) {
            Ok(IOResult::Done(_)) => true,
            Ok(IOResult::IO(_)) => {
                most = most.max(probe.held.lock().len());
                probe.let_go();
                false
            }
            Err(_) => false,
        });
        served += usize::from(ok);
    }
    // A carrier is recorded held just after its callback runs, so the last page can read before
    // its carrier is: let go until none is counted.
    let requests = counted.requests().clone();
    let settled = settle(|| {
        probe.let_go();
        requests.outstanding() == 0
    });
    let (free, at_rest) = (cache.credits.free(), requests.queued_at_rest());
    // Every carrier retired, and its hold on the requests with it, before any assertion.
    probe.let_go();
    drop((cache, counted, requests, probe));
    assert!(held, "the first read not held by its backend");
    assert_eq!(
        at_asks,
        (1, 1, 0),
        "(reads, frames, failed asks) after 128 asks under one credit"
    );
    assert_eq!(served, PAGES as usize, "pages served");
    assert_eq!(most, 1, "reads with the backend at most while served");
    assert!(settled, "requests left counted");
    assert_eq!(
        (free, at_rest),
        (1, 1),
        "(credits free, records at rest) at the end"
    );
}

/// D2a §6.6: two IO threads; the first read blocked inside its call on one of them, a second read
/// is served by the other meanwhile, and the first then finishes (mutant: one IO thread, so the
/// second waits behind the first's syscall).
#[test]
fn a_second_io_thread_serves_a_read_while_the_first_is_blocked_in_its_call() {
    let probe = Probe::new();
    let counted = counted_async_for_test(probe.clone(), 2, 2);
    let io: Arc<dyn IO> = counted.clone();
    let cache = cache(&counted, 2);
    // The cold file opened first, outside any frame, so each read's first call is its read.
    let _ = drive(&io, || cache.get(id(2)));
    let (entered, go) = probe.block_next_read();
    let first = in_frame(&cache, 0);
    let blocked = entered.recv_timeout(SETTLE).ok();
    let second = in_frame(&cache, 1);
    let second_read = last_read().expect("the second queued read");
    let served = settle(|| second_read.finished());
    let second_thread = probe
        .calls()
        .iter()
        .filter(|c| c.what == "pread")
        .next_back()
        .map(|c| c.thread);
    let _ = go.send(());
    let first_served = drive(&io, || cache.get(id(0))).is_ok();
    drop((first, second, second_read, cache, io, counted));
    assert!(blocked.is_some(), "the first read never entered its call");
    assert!(served, "the second read waited behind the first's call");
    assert_ne!(second_thread, blocked, "both reads on one thread");
    assert!(first_served, "the first page not served once let go");
}

/// D2a §6.6 (#4822): two reads queued in one leaf word before either IO thread takes one, the
/// threads stopped before their take meanwhile (each woken thread at a hold, the other idle): the
/// first take keeps one record and puts the other back, waking the idle thread if one is, and the
/// other thread serves it while the first is blocked inside its read's call (mutant: a take that
/// keeps the whole word, so the other read waits behind the first's call).
#[test]
fn two_reads_in_one_leaf_word_are_served_by_two_io_threads() {
    let probe = Probe::new();
    // 30 read records make a tree of two levels with two IO threads; records 0 and 1 share a leaf.
    let counted = counted_async_for_test(probe.clone(), 30, 2);
    let io: Arc<dyn IO> = counted.clone();
    let cache = cache(&counted, 30);
    // The cold file opened first, outside any frame, so each read's first call is its read.
    let _ = drive(&io, || cache.get(id(2)));
    let requests = counted.requests().clone();
    let holds = [requests.hold_next_take(), requests.hold_next_take()];
    let (entered, go) = probe.block_next_read();
    let first = in_frame(&cache, 0);
    let first_read = last_read().expect("the first queued read");
    let second = in_frame(&cache, 1);
    let second_read = last_read().expect("the second queued read");
    // Both bits in the leaf before any take; then both threads let go at once, whichever of
    // them each hold stopped.
    drop(holds);
    let blocked = entered.recv_timeout(SETTLE).ok();
    let other_served = settle(|| first_read.finished() || second_read.finished());
    let _ = go.send(());
    let both = settle(|| first_read.finished() && second_read.finished());
    let reads_by_thread: Vec<ThreadId> = probe
        .calls()
        .iter()
        .filter(|c| c.what == "pread")
        .skip(1)
        .map(|c| c.thread)
        .collect();
    drop((
        first,
        second,
        first_read,
        second_read,
        cache,
        io,
        counted,
        requests,
    ));
    assert!(blocked.is_some(), "no read entered its call");
    assert!(
        other_served,
        "the other read of the leaf waited behind the blocked one's call"
    );
    assert!(both, "a page not served once let go");
    assert_eq!(reads_by_thread.len(), 2, "the two reads' calls");
    assert_ne!(
        reads_by_thread[0], reads_by_thread[1],
        "both reads on one thread"
    );
}

/// D2a §6.7: a cold file whose open the backend refuses: the queued read finishes with that error,
/// the page is not served, and the record, the credit and the count are let go (mutant: the
/// stage's error dropped, so the read never finishes).
#[test]
fn a_cold_files_refused_open_is_the_reads_error_and_lets_everything_go() {
    let probe = Probe::new();
    probe.refuse_open.store(true, Ordering::SeqCst);
    let counted = counted_async_for_test(probe.clone(), 4, 1);
    let io: Arc<dyn IO> = counted.clone();
    let cache = cache(&counted, 4);
    let first = in_frame(&cache, 0);
    let read = last_read().expect("the queued read");
    let finished = settle(|| read.finished());
    let error = read.get_error();
    // One look, never a wait: the page is not served.
    let served = matches!(cache.get(id(0)), Ok(IOResult::Done(_)));
    let requests = counted.requests().clone();
    let settled = settle(|| requests.outstanding() == 0);
    let state = (cache.credits.free(), requests.queued_at_rest());
    drop((first, read, cache, io, counted, requests));
    assert!(finished, "the read never finished");
    assert!(error.is_some(), "the refused open was not the read's error");
    assert!(!served, "a page served whose file never opened");
    assert!(settled, "the request left counted");
    assert_eq!(state, (4, 4), "(credits free, records at rest)");
}

/// What a closer decided first: to wait for a request counted, or to seal at once.
#[derive(Debug, PartialEq, Eq)]
enum Decided {
    Parks,
    Sealed,
}

/// D2a §6.8: a queued read not yet taken is counted from its acceptance, so a close that drains
/// meanwhile parks on it, and seals only once the IO thread has run it (mutant: the count taken at
/// the take, so the close seals before it and the read is refused).
#[test]
fn a_close_waits_for_a_queued_read_not_yet_taken() {
    let probe = Probe::new();
    let counted = counted_async_for_test(probe.clone(), 4, 1);
    let cache = cache(&counted, 4);
    let hold = counted.requests().hold_io_thread();
    let first = in_frame(&cache, 0);
    let read = last_read().expect("the queued read");
    let (decided_tx, decided) = mpsc::channel();
    let closer = {
        let requests = counted.requests().clone();
        let raw: Arc<dyn IO> = probe.clone();
        std::thread::spawn(move || {
            let parks = decided_tx.clone();
            crate::race::at("requests.drain_parks", move || {
                let _ = parks.send(Decided::Parks);
            });
            crate::race::at("requests.drain_sealed", move || {
                let _ = decided_tx.send(Decided::Sealed);
            });
            requests.drain(&*raw);
        })
    };
    let first_decision = decided.recv_timeout(SETTLE).ok();
    drop(hold);
    let then = decided.recv_timeout(SETTLE).ok();
    let closed = settle(|| closer.is_finished());
    if closed {
        let _ = closer.join();
    }
    let error = read.get_error();
    drop((first, read, cache, counted));
    assert_eq!(
        first_decision,
        Some(Decided::Parks),
        "the close's decision with the read queued"
    );
    assert_eq!(then, Some(Decided::Sealed), "the close once the read ran");
    assert!(closed, "the close did not end");
    assert!(error.is_none(), "the queued read ended with {error:?}");
}

/// D2a §6.4b (#4820, #4823): a cancel's callback in flight while the read's carrier finishes and
/// retires: the owner's request is not yet retired, so its record, its credit and its count stay
/// held until the callback returns, and then all three go (mutant: the record, the credit and the
/// count let go at the carrier's retirement, before the owner's request has retired).
#[test]
fn a_cancels_callback_in_flight_holds_the_record_until_it_returns() {
    let probe = Probe::new();
    let counted = counted_async_for_test(probe.clone(), 4, 1);
    let cache = cache(&counted, 4);
    let requests = counted.requests().clone();
    let (entered, go) = probe.block_next_read();
    let first = in_frame(&cache, 0);
    let read = last_read().expect("the queued read");
    let reached = entered.recv_timeout(SETTLE).is_ok();
    // The cancel's callback held at its publishing point, on a thread of its own.
    let (inside_tx, inside) = mpsc::channel();
    let (out_tx, out) = mpsc::channel::<()>();
    let canceller = {
        let read = read.clone();
        std::thread::spawn(move || {
            crate::race::at("meta.load_publishing", move || {
                let _ = inside_tx.send(());
                let _ = out.recv_timeout(SETTLE);
            });
            read.abort();
        })
    };
    let held = inside.recv_timeout(SETTLE).is_ok();
    // The backend's read lets go: the carrier's callback finds the owner's claimed, and the
    // carrier retires, all on the IO thread, whose next idle follows its serve's return.
    let idle = requests.on_io_idle();
    let _ = go.send(());
    let carrier_ended = idle.recv_timeout(SETTLE).is_ok();
    let while_held = (
        requests.queued_at_rest(),
        cache.credits.free(),
        requests.outstanding(),
        read.is_owned(),
    );
    drop(out_tx);
    let joined = canceller.join().is_ok();
    let settled = settle(|| requests.outstanding() == 0);
    let after = (
        requests.queued_at_rest(),
        cache.credits.free(),
        requests.outstanding(),
        read.is_owned(),
    );
    drop((first, read, cache, counted, requests));
    assert!(
        reached && held && joined,
        "the read or the cancel's callback never held"
    );
    assert!(carrier_ended, "the carrier's read never ended");
    assert_eq!(
        while_held,
        (3, 3, 1, true),
        "(records at rest, credits free, requests counted, the request owned) with the cancel's \
         callback still running"
    );
    assert!(settled, "the callback's return left the request counted");
    assert_eq!(
        after,
        (4, 4, 0, false),
        "(records at rest, credits free, requests counted, the request owned) after it returned"
    );
}

/// D2a §3 (#4825): the cache dropped while its read is queued and not yet taken, the read's file
/// cold: the read's open stage finds its owner gone, so the read finishes with an error, no call
/// reaches the backend, and its record, its credit and its count are all let go (mutant: an owner
/// gone left unanswered, so the read stays queued and counted).
#[test]
fn a_read_whose_cache_is_gone_before_its_take_ends_with_no_call() {
    let probe = Probe::new();
    let counted = counted_async_for_test(probe.clone(), 4, 1);
    let cache = cache(&counted, 4);
    let credits = cache.credits.clone();
    let hold = counted.requests().hold_io_thread();
    let first = in_frame(&cache, 0);
    let read = last_read().expect("the queued read");
    drop((first, cache));
    drop(hold);
    let requests = counted.requests().clone();
    let finished = settle(|| read.finished());
    let error = read.get_error();
    let settled = settle(|| requests.outstanding() == 0);
    let state = (
        probe.count("open"),
        probe.count("pread"),
        credits.free(),
        requests.queued_at_rest(),
        read.is_owned(),
    );
    drop((read, credits, counted, requests));
    assert!(finished, "the read never finished");
    assert!(
        error.is_some(),
        "the read of a cache gone ended without an error"
    );
    assert!(settled, "the read left counted");
    assert_eq!(
        state,
        (0, 0, 4, 4, false),
        "(opens, reads, credits free, records at rest, the request owned)"
    );
}

/// D2a §3 (#4831): a queued read whose carrier its backend still owns keeps its requests, and what
/// they hold for the embedder, alive after every other owner of the database has gone; both drop
/// at the carrier's retirement, and the IO thread is joined then (mutant: the request holding its
/// requests only weakly, so they drop with the last handle while the backend owns the carrier).
#[test]
fn a_queued_read_keeps_its_requests_alive_until_its_carrier_retires() {
    use crate::storage::atoll::requests::{Requests, Shape};
    let probe = Probe::new();
    probe.hold_retirement.store(true, Ordering::SeqCst);
    let held: Arc<dyn std::any::Any + Send + Sync> = Arc::new(());
    let held_weak = Arc::downgrade(&held);
    let requests = Arc::new(Requests::holding(Some(held)));
    let weak = Arc::downgrade(&requests);
    let counted = Arc::new(
        Counted::with_shape(
            probe.clone(),
            requests,
            Shape {
                opened_async: true,
                io_threads: 1,
                meta_reads: 4,
                frames: 0,
            },
        )
        .expect("a test's IO thread"),
    );
    let cache = cache(&counted, 4);
    let (entered, go) = probe.block_next_read();
    let first = in_frame(&cache, 0);
    let read = last_read().expect("the queued read");
    let inside = entered.recv_timeout(SETTLE).is_ok();
    // Armed while the IO thread is inside this read's call, so the idle it tells of follows the
    // call's return and the end of the take that made it: the carrier accepted and its callback
    // run, and the thread's own holds on the request let go (#4838).
    let idle = counted.requests().on_io_idle();
    drop(go);
    let accepted = inside && idle.recv_timeout(SETTLE).is_ok() && probe.held.lock().len() == 1;
    drop((first, cache, counted));
    let while_owned = (
        weak.upgrade().is_some(),
        held_weak.upgrade().is_some(),
        read.is_owned(),
    );
    probe.let_go();
    let after = (
        weak.upgrade().is_some(),
        held_weak.upgrade().is_some(),
        read.is_owned(),
    );
    drop(read);
    assert!(
        accepted,
        "the read never entered its call, or its carrier not held by its backend"
    );
    assert_eq!(
        while_owned,
        (true, true, true),
        "(the requests, what they hold, the request owned) with the carrier the backend's"
    );
    assert_eq!(
        after,
        (false, false, false),
        "(the requests, what they hold, the request owned) after the carrier retired"
    );
}

/// D2a §3 (#4817, #4834; child): the owner's request cancelled while its IO thread is inside the
/// backend's read call, before the backend accepts and gives back what it was handed, refused: the
/// backend's refusal touches only the carrier, so the owner's request has one outcome, the cancel,
/// and the record, the credit and the count are all let go (mutant: the owner's request handed to
/// the backend itself, whose refusal of a request the cancel already finished ends the process).
#[cfg(all(unix, not(miri)))]
#[test]
fn a_backends_refusal_after_the_owners_cancel_touches_only_the_carrier() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_backends_refusal_after_the_owners_cancel_touches_only_the_carrier"
        ),
        || {
            let probe = Probe::new();
            probe.refuse_reads.store(true, Ordering::SeqCst);
            let counted = counted_async_for_test(probe.clone(), 4, 1);
            let cache = cache(&counted, 4);
            let requests = counted.requests().clone();
            // The cold file opened first, outside any frame, so the next call is the read.
            probe.refuse_reads.store(false, Ordering::SeqCst);
            let io: Arc<dyn IO> = counted.clone();
            let _ = drive(&io, || cache.get(id(1)));
            probe.refuse_reads.store(true, Ordering::SeqCst);
            let (entered, go) = probe.block_next_read();
            let first = in_frame(&cache, 0);
            let read = last_read().expect("the queued read");
            let inside = entered.recv_timeout(SETTLE).is_ok();
            // The cancel's callback runs to its end on this thread before the backend goes on.
            read.abort();
            let _ = go.send(());
            let settled = settle(|| requests.outstanding() == 0);
            let state = (
                read.get_error(),
                cache.credits.free(),
                requests.queued_at_rest(),
                read.is_owned(),
            );
            drop((first, read, cache, io, counted, requests));
            assert!(inside, "the read never entered its call");
            assert!(settled, "the refused read left counted");
            assert_eq!(
                state,
                (Some(CompletionError::Aborted), 4, 4, false),
                "(the request's one outcome, credits free, records at rest, the request owned)"
            );
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
