//! The pool's frames, queued (DESIGN.md 22.18; ASYNC1 D2b §7): in a database opened async, a load
//! or a write-out a cooperative frame makes is queued in its frame's record, a cold file opened as
//! its first stage on an IO thread, and the backend sees only a carrier of the IO thread's own.
//! Every backend call is recorded with its file and its thread.

use super::*;
use crate::io::clock::{Clock, MonotonicInstant, WallClockInstant};
use crate::io::{Delivery, FileSyncType, MemoryIO, OpenFlags, Stepped, IO};
use crate::storage::atoll::cooperative::Frame;
use crate::storage::atoll::page::{HEADER_SIZE, MIN_PAGE_SIZE};
use crate::storage::atoll::requests::{
    counted_async_with_frames_for_test, counted_for_test, Counted,
};
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::drive;
#[cfg(all(unix, not(miri)))]
use crate::storage::atoll::testing::in_child;
use crate::LimboError;
use std::thread::ThreadId;

/// Every wait of these tests: one deadline for the whole operation.
const SETTLE: std::time::Duration = std::time::Duration::from_secs(20);

/// The metadata cache's pages, and its read records in a database opened async.
const META_PAGES: usize = 1 << 10;

/// The error a probe's backend finishes a request with when told to: none of the pool's own.
const INJECTED: CompletionError =
    CompletionError::IOError(std::io::ErrorKind::BrokenPipe, "a probe's injected error");

fn class0() -> SizeClass {
    SizeClass::new(0).unwrap()
}

/// A backend call: which, on which file, and on which thread.
#[derive(Clone, Debug)]
struct Call {
    what: &'static str,
    path: String,
    thread: ThreadId,
    name: String,
}

/// What the probe does with a read or a write on a file whose path begins with its prefix.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Rule {
    /// Its callback at once, its retirement held until the probe lets it go.
    Hold,
    /// Finished at once with [`INJECTED`], and retired: a backend's error.
    Fail,
    /// Finished at once with [`INJECTED`], its retirement held until the probe lets it go: a
    /// backend's error whose request it still owns.
    HoldFail,
}

/// MemoryIO, which delivers inline, with every open, directory sync, read and write recorded with
/// its file and thread; opens under a prefix it can refuse; and reads and writes under a prefix it
/// can hold or fail ([`Rule`]).
#[derive(Default)]
struct Probe {
    inner: MemoryIO,
    calls: Mutex<Vec<Call>>,
    refuse_open: Mutex<Option<String>>,
    rule: Mutex<Option<(String, Rule)>>,
    held: Mutex<Vec<Completion>>,
}

impl Probe {
    fn new() -> Arc<Self> {
        Arc::default()
    }

    fn record(&self, what: &'static str, path: &str) {
        let thread = crate::thread::current();
        self.calls.lock().push(Call {
            what,
            path: path.to_string(),
            thread: thread.id(),
            name: thread.name().unwrap_or("unnamed").to_string(),
        });
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().clone()
    }

    fn forget_calls(&self) {
        self.calls.lock().clear();
    }

    /// The calls on files under `prefix`, and the directory syncs, as (call, thread name).
    fn under(&self, prefix: &str) -> Vec<(&'static str, String)> {
        self.calls()
            .into_iter()
            .filter(|c| c.what == "sync_directory" || c.path.starts_with(prefix))
            .map(|c| (c.what, c.name))
            .collect()
    }

    /// The calls made on `thread`.
    fn on(&self, thread: ThreadId) -> Vec<(&'static str, String)> {
        self.calls()
            .into_iter()
            .filter(|c| c.thread == thread)
            .map(|c| (c.what, c.path))
            .collect()
    }

    fn refuse_opens_under(&self, prefix: Option<&str>) {
        *self.refuse_open.lock() = prefix.map(str::to_string);
    }

    fn rule_under(&self, prefix: &str, rule: Option<Rule>) {
        *self.rule.lock() = rule.map(|r| (prefix.to_string(), r));
    }

    fn rule_for(&self, path: &str) -> Option<Rule> {
        self.rule
            .lock()
            .as_ref()
            .filter(|(prefix, _)| path.starts_with(prefix.as_str()))
            .map(|(_, rule)| *rule)
    }

    /// The requests whose retirement it holds, still owned.
    fn holding(&self) -> usize {
        self.held.lock().iter().filter(|c| c.is_owned()).count()
    }

    /// Retires every request whose retirement it holds.
    fn let_go(&self) {
        let held = std::mem::take(&mut *self.held.lock());
        for c in held {
            if c.is_owned() {
                c.retire();
            }
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
        self.record("open", path);
        let refused = self
            .refuse_open
            .lock()
            .as_ref()
            .is_some_and(|prefix| path.starts_with(prefix.as_str()));
        if refused {
            return Err(LimboError::InternalError(
                "a probe's open refused".to_string(),
            ));
        }
        Ok(Arc::new(ProbeFile {
            file: self.inner.open_file(path, flags, direct)?,
            path: path.to_string(),
            probe: self as *const Probe as usize,
        }))
    }
    fn sync_directory(&self, path: &str) -> Result<()> {
        self.record("sync_directory", path);
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

/// A probe's file: its reads and writes recorded, and held or failed as its probe's rule says.
struct ProbeFile {
    file: Arc<dyn File>,
    path: String,
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
        probe.record("pread", &self.path);
        match probe.rule_for(&self.path) {
            None => self.file.pread(at, c),
            Some(Rule::Fail) => {
                c.accept();
                c.deliver(Err(INJECTED));
                Ok(c)
            }
            Some(Rule::HoldFail) => {
                c.accept();
                c.error(INJECTED);
                probe.held.lock().push(c.clone());
                Ok(c)
            }
            Some(Rule::Hold) => {
                // The read now, its callback through `c`; `c`'s retirement when the probe lets it
                // go.
                c.accept();
                let owner = c.clone();
                let inner = Completion::new_read(c.as_read().buf_arc(), move |res| {
                    match res {
                        Ok((_, n)) => owner.complete(n),
                        Err(e) => owner.error(e),
                    }
                    None
                });
                drop(self.file.pread(at, inner)?);
                probe.held.lock().push(c.clone());
                Ok(c)
            }
        }
    }
    fn pwrite(&self, at: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        let probe = self.probe();
        probe.record("pwrite", &self.path);
        match probe.rule_for(&self.path) {
            None => self.file.pwrite(at, buffer, c),
            Some(Rule::Fail) => {
                c.accept();
                c.deliver(Err(INJECTED));
                Ok(c)
            }
            Some(Rule::HoldFail) => {
                c.accept();
                c.error(INJECTED);
                probe.held.lock().push(c.clone());
                Ok(c)
            }
            Some(Rule::Hold) => {
                c.accept();
                let owner = c.clone();
                let inner = Completion::new_write(move |res| match res {
                    Ok(n) => owner.complete(n),
                    Err(e) => owner.error(e),
                });
                drop(self.file.pwrite(at, buffer, inner)?);
                probe.held.lock().push(c.clone());
                Ok(c)
            }
        }
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

/// A pool over `io` at `base`, of the smallest budget, opened async or not: its counted IO, with
/// a frame record per grain and a metadata read record per page of its cache when async.
fn pool_on(io: Arc<dyn IO>, base: &str, opened_async: bool) -> (Arc<Counted>, Arc<BufferPool>) {
    let counted = if opened_async {
        counted_async_with_frames_for_test(io, META_PAGES, MIN_BUDGET / MIN_PAGE_SIZE, 1)
    } else {
        counted_for_test(io)
    };
    let store = PageStore::open(
        counted.clone(),
        base,
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: META_PAGES,
        },
        [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        0,
    );
    let pool = BufferPool::new(
        BufferConfig::with_budget(MIN_BUDGET),
        Arc::new(LocalSource::new(store.clone())),
        store,
    )
    .unwrap();
    (counted, pool)
}

/// The data files' prefix and the temporary files' at `base`.
fn data(base: &str) -> String {
    format!("{base}.c0.")
}

fn temporary(base: &str) -> String {
    format!("{base}.t0")
}

/// Writes `version` into `page` beside its id, after the header and at its end.
fn stamp<P: crate::storage::atoll::shared::BytesMut + ?Sized>(
    page: &mut P,
    pid: PageId,
    version: u64,
) {
    page.put_u64(HEADER_SIZE, pid.raw());
    page.put_u64(HEADER_SIZE + 8, version);
    let end = page.len();
    page.put_u64(end - 8, version);
}

/// The version stamped in `page`, if it is stamped as `pid` and whole.
fn stamped<P: crate::storage::atoll::shared::Bytes + ?Sized>(page: &P, pid: PageId) -> Option<u64> {
    let version = page.u64_at(HEADER_SIZE + 8);
    (page.u64_at(HEADER_SIZE) == pid.raw() && page.u64_at(page.len() - 8) == version)
        .then_some(version)
}

/// A new page of class 0, stamped and changed, left resident and dirty: its id and frame.
fn dirty_page(pool: &Arc<BufferPool>, io: &Arc<dyn IO>, temporary: bool) -> (PageId, u32) {
    let mut g = if temporary {
        drive(io, || pool.allocate_temporary(class0())).unwrap()
    } else {
        drive(io, || pool.allocate(class0())).unwrap()
    };
    let (pid, frame) = (g.pid(), g.frame);
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    drop(g);
    (pid, frame)
}

/// Starts writing the page in `frame` out, as an eviction does, and queues its writeback.
fn start_writing_out(pool: &Arc<BufferPool>, frame: u32) {
    let hold = pool
        .latch_to_evict(class0(), frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .expect("the page is held by no one");
    let out = pool.start_write_out(hold);
    pool.write_out(class0(), vec![out]);
}

/// Polls `f` inside a cooperative frame until it is done, waiting outside the frame between polls,
/// so every request a poll makes is queued: a wait's own error is the operation's, as in
/// `drive`, and a wait still unfinished once [`SETTLE`] has passed is an error of its own.
fn in_frames<T>(io: &Arc<dyn IO>, mut f: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
    let deadline = std::time::Instant::now() + SETTLE;
    loop {
        let polled = {
            let _frame = Frame::enter();
            f()
        };
        match polled? {
            IOResult::Done(t) => return Ok(t),
            IOResult::IO(IOCompletions(c)) => {
                while !c.finished() {
                    if std::time::Instant::now() >= deadline {
                        return Err(LimboError::InternalError(
                            "a frame's wait did not finish".to_string(),
                        ));
                    }
                    io.step()?;
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                // As `drive`: a wait that ended in an error is the operation's error.
                if let Some(e) = c.get_error() {
                    return Err(LimboError::CompletionError(e));
                }
            }
        }
    }
}

/// Settles the pool's writebacks, each step of them inside a cooperative frame.
fn settle_in_frames(pool: &Arc<BufferPool>, io: &Arc<dyn IO>) -> Result<()> {
    in_frames(io, || pool.settle_writes())
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

/// A page whose newest copy is a swap copy another store wrote: `base`'s store over `probe`, opened
/// async or not, with its data files cold. The page's id, the pool and its counted IO.
fn page_in_a_cold_file(
    probe: &Arc<Probe>,
    base: &str,
    opened_async: bool,
) -> (PageId, Arc<Counted>, Arc<BufferPool>) {
    let io: Arc<dyn IO> = probe.clone();
    let (first_counted, first) = pool_on(io.clone(), base, false);
    let first_io: Arc<dyn IO> = first_counted.clone();
    let (pid, frame) = dirty_page(&first, &first_io, false);
    start_writing_out(&first, frame);
    drive(&first_io, || first.settle_writes()).unwrap();
    let slot = first
        .store
        .swap_slot(pid)
        .expect("the page written out to a swap slot");
    drop((first, first_io, first_counted));
    let (counted, pool) = pool_on(io, base, opened_async);
    pool.store.set_swap(pid, slot);
    (pid, counted, pool)
}

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

/// D2b §7.1 (child): in a database opened async, a frame's miss on a page whose copy is in a cold
/// data file returns IO with no backend call on the polling thread; the data file's open, its
/// directory's sync and the read are made on an IO thread, in that order, and the page then reads
/// (mutant `frame-queues-false`: the hook submits in place, and the census ends the child).
#[cfg(all(unix, not(miri)))]
#[test]
fn a_frames_miss_on_a_cold_data_file_is_opened_synced_and_read_on_an_io_thread() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_frames_miss_on_a_cold_data_file_is_opened_synced_and_read_on_an_io_thread"
        ),
        || {
            let probe = Probe::new();
            let base = "/db/frame-cold";
            let (pid, counted, pool) = page_in_a_cold_file(&probe, base, true);
            let io: Arc<dyn IO> = counted.clone();
            probe.forget_calls();
            let me = crate::thread::current().id();
            let first = {
                let _frame = Frame::enter();
                pool.fix_exclusive(pid, None)
                    .map(|r| matches!(r, IOResult::IO(_)))
            };
            let polled = probe.on(me);
            let read =
                in_frames(&io, || pool.fix_exclusive(pid, None)).map(|g| stamped(g.page(), pid));
            let on_data = probe.under(&data(base));
            drop((pool, io, counted));
            assert_eq!(first.ok(), Some(true), "a frame's miss returned the page");
            assert!(
                polled.is_empty(),
                "backend calls on the polling thread: {polled:?}"
            );
            let data_calls: Vec<_> = on_data
                .iter()
                .skip_while(|(what, _)| *what == "sync_directory")
                .collect();
            let io_thread = "atoll-io".to_string();
            assert_eq!(
                data_calls.first(),
                Some(&&("open", io_thread.clone())),
                "the data file's first call: {on_data:?}"
            );
            assert!(
                data_calls.contains(&&("sync_directory", io_thread.clone()))
                    && data_calls.last() == Some(&&("pread", io_thread)),
                "the data file's open, its directory's sync and the read, by thread: {on_data:?}"
            );
            assert_eq!(read.ok(), Some(Some(1)), "the page read back");
        },
    );
    child_ended_cleanly(ended);
}

/// D2b §7.2 (child): a synchronous database's miss inside a frame, its page-table leaf resident,
/// runs in place, and the census ends the process at the data file's open, naming it (mutant: the
/// rule without its mode arm, so the synchronous database queues, finds no frame record and ends
/// 0 with the load's error).
#[cfg(all(unix, not(miri)))]
#[test]
fn a_synchronous_databases_frame_miss_on_a_data_page_ends_the_process_at_its_open() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_synchronous_databases_frame_miss_on_a_data_page_ends_the_process_at_its_open"
        ),
        || {
            let probe = Probe::new();
            let (pid, counted, pool) = page_in_a_cold_file(&probe, "/db/frame-sync", false);
            let io: Arc<dyn IO> = counted.clone();
            // The page-table leaf resident, so the first call in place is the data file's open.
            let leaf = drive(&io, || pool.store.leaf(pid)).unwrap();
            let first = {
                let _frame = Frame::enter();
                pool.fix_exclusive(pid, None)
                    .map(|r| matches!(r, IOResult::IO(_)))
            };
            drop((first, leaf, pool, io, counted));
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

/// D2b §7.3 (child): a dirty page's write-out in a frame of an async database is queued, its write
/// made on the IO thread. The write-out then gives its frame up, but the frame is freed only at
/// the write's retirement, which the probe holds: until then its record is held and a claim's wait
/// on the pool's room is not finished, and the retirement frees it and finishes that wait (mutant
/// `write-out-in-place`: the write-out submits in place in the frame, and the census ends the
/// child).
#[cfg(all(unix, not(miri)))]
#[test]
fn a_write_out_in_a_frame_is_queued_and_its_frame_freed_at_the_writes_retirement() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_write_out_in_a_frame_is_queued_and_its_frame_freed_at_the_writes_retirement"
        ),
        || {
            let probe = Probe::new();
            let base = "/db/frame-write-out";
            let io: Arc<dyn IO> = probe.clone();
            let (counted, pool) = pool_on(io, base, true);
            let io: Arc<dyn IO> = counted.clone();
            let (pid, frame) = dirty_page(&pool, &io, false);
            probe.rule_under(&data(base), Some(Rule::Hold));
            probe.forget_calls();
            let me = crate::thread::current().id();
            start_writing_out(&pool, frame);
            let settled = settle_in_frames(&pool, &io);
            let polled = probe.on(me);
            let writes: Vec<_> = probe
                .calls()
                .into_iter()
                .filter(|c| c.what == "pwrite")
                .map(|c| c.name)
                .collect();
            let h = pool.header(frame);
            // The write landed and the write-out gave the frame up; its record still holds it.
            let before = (probe.holding(), h.status() == FREE, h.record_held());
            // A claim waiting for room meanwhile.
            let wait = pool.released_wait();
            let waited_before = wait.finished();
            probe.let_go();
            let freed = settle(|| !pool.header(frame).record_held());
            let woken = wait.finished();
            let swap = pool.store.swap_slot(pid);
            let at_rest = counted.requests().frames_at_rest();
            let failure = pool.failed().err();
            drop((wait, pool, io, counted));
            assert!(settled.is_ok(), "the writeback did not settle: {settled:?}");
            assert!(
                polled.is_empty(),
                "backend calls on the polling thread: {polled:?}"
            );
            assert_eq!(writes, ["atoll-io"], "the write, by thread");
            assert_eq!(
                before,
                (1, true, true),
                "(writes held at the backend, the frame given up, its record held) once the \
                 write landed"
            );
            assert!(
                !waited_before,
                "the claim's wait finished before the retirement"
            );
            assert!(
                freed,
                "the frame's record not let go at the write's retirement"
            );
            assert!(woken, "the retirement left the claim waiting");
            assert!(swap.is_some(), "the page has no swap copy");
            assert_eq!(at_rest, MIN_BUDGET / MIN_PAGE_SIZE, "frame records at rest");
            assert!(failure.is_none(), "the pool failed: {failure:?}");
        },
    );
    child_ended_cleanly(ended);
}

/// D2b §7.4a (child): a temporary page's first write-out, in a frame of an async database: the cold
/// open of its class's file and the write on the IO thread, then its read-back as a queued read on
/// the IO thread, from the file the write installed (mutant: the temporary write-out submitted in
/// place in a frame, and the census ends the child at the open).
#[cfg(all(unix, not(miri)))]
#[test]
fn a_temporary_pages_first_write_out_and_read_back_in_frames_run_on_an_io_thread() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_temporary_pages_first_write_out_and_read_back_in_frames_run_on_an_io_thread"
        ),
        || {
            let probe = Probe::new();
            let base = "/db/frame-temporary";
            let io: Arc<dyn IO> = probe.clone();
            let (counted, pool) = pool_on(io, base, true);
            let io: Arc<dyn IO> = counted.clone();
            let (pid, frame) = dirty_page(&pool, &io, true);
            probe.forget_calls();
            let me = crate::thread::current().id();
            start_writing_out(&pool, frame);
            let settled = settle_in_frames(&pool, &io);
            let evicted = pool.table.find(&pool.frames.headers, pid.raw()).is_none();
            let read =
                in_frames(&io, || pool.fix_exclusive(pid, None)).map(|g| stamped(g.page(), pid));
            let polled = probe.on(me);
            let on_temporary: Vec<_> = probe
                .calls()
                .into_iter()
                .filter(|c| c.path.starts_with(&temporary(base)))
                .map(|c| (c.what, c.name))
                .collect();
            drop((pool, io, counted));
            assert!(settled.is_ok(), "the writeback did not settle: {settled:?}");
            assert!(
                evicted,
                "the temporary page still resident after its write-out"
            );
            assert!(
                polled.is_empty(),
                "backend calls on the polling thread: {polled:?}"
            );
            let io_thread = || "atoll-io".to_string();
            assert_eq!(
                on_temporary,
                [
                    ("open", io_thread()),
                    ("pwrite", io_thread()),
                    ("pread", io_thread())
                ],
                "the temporary file's calls, by thread"
            );
            assert_eq!(read.ok(), Some(Some(1)), "the page read back");
        },
    );
    child_ended_cleanly(ended);
}

/// D2b §7.5: a queued load cancelled before an IO thread takes it runs no syscall, not even its
/// cold data file's open, and the frame is freed at its retirement (mutant: the claim ignoring the
/// cancel, so the open and the read reach the backend).
#[test]
fn a_load_cancelled_before_its_take_runs_no_syscall_and_frees_its_frame() {
    let probe = Probe::new();
    let base = "/db/frame-cancel";
    let (pid, counted, pool) = page_in_a_cold_file(&probe, base, true);
    let io: Arc<dyn IO> = counted.clone();
    // The page-table leaf resident, so the load's own request is the one queued.
    let leaf = drive(&io, || pool.store.leaf(pid)).unwrap();
    probe.forget_calls();
    let hold = counted.requests().hold_io_thread();
    let first = {
        let _frame = Frame::enter();
        pool.fix_exclusive(pid, None)
            .map(|r| matches!(r, IOResult::IO(_)))
    };
    let frame = pool.table.find(&pool.frames.headers, pid.raw());
    // The load's own request, as a backend's cancel would finish it.
    let cancelled = last_frame_request();
    if let Some(c) = &cancelled {
        c.abort();
    }
    drop(hold);
    let requests = counted.requests().clone();
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    let let_go = settle(|| requests.outstanding() == 0 && requests.frames_at_rest() == records);
    let freed = frame.is_some_and(|f| !pool.header(f).record_held());
    let on_data: Vec<_> = probe
        .calls()
        .into_iter()
        .filter(|c| c.path.starts_with(&data(base)))
        .map(|c| c.what)
        .collect();
    let was_queued = cancelled.is_some();
    drop((cancelled, leaf, pool, io, counted, requests));
    assert_eq!(first.ok(), Some(true), "a frame's miss returned the page");
    assert!(was_queued, "no frame request was queued");
    assert!(on_data.is_empty(), "calls on the data file: {on_data:?}");
    assert!(let_go, "the record or the count was not let go");
    assert!(freed, "the frame's record still held");
}

/// The last frame request queued on this thread, for a test to cancel as a backend's cancel would.
fn last_frame_request() -> Option<Completion> {
    crate::storage::atoll::requests::LAST_FRAME_REQUEST.with(|r| r.borrow_mut().take())
}

/// D2b §7.11: an in-place cold open that fails, with no frame: (i) a temporary page's first
/// write-out, (ii) a data page's read from a cold data file. Each returns or settles the error, the
/// frame comes back (free after the load, resident and dirty after the write-out), the pool's count
/// comes back, and the frame's record is free (mutant: the give-back skipped on the in-place
/// refusal arm, so the record stays held).
#[test]
fn an_in_place_cold_open_that_fails_gives_the_frame_and_its_record_back() {
    // (i) The temporary page's first write-out.
    let probe = Probe::new();
    let base = "/db/frame-open-fails";
    let io: Arc<dyn IO> = probe.clone();
    let (counted, pool) = pool_on(io, base, false);
    let io: Arc<dyn IO> = counted.clone();
    let (pid, frame) = dirty_page(&pool, &io, true);
    probe.refuse_opens_under(Some(&temporary(base)));
    start_writing_out(&pool, frame);
    let settled = drive(&io, || pool.settle_writes());
    let write_out = (
        pool.failed().is_err(),
        pool.header(frame).status() == RESIDENT && pool.header(frame).pid() == pid.raw(),
        pool.header(frame).dirty(),
        pool.requests.load(Ordering::Acquire),
        pool.header(frame).record_held(),
    );
    drop((pool, io, counted));
    // (ii) The data page's read from a cold data file.
    let probe = Probe::new();
    let base = "/db/frame-open-fails-read";
    let (pid, counted, pool) = page_in_a_cold_file(&probe, base, false);
    let io: Arc<dyn IO> = counted.clone();
    probe.refuse_opens_under(Some(&data(base)));
    let read = drive(&io, || pool.fix_exclusive(pid, None)).map(|_| ());
    let load = (
        read.is_err(),
        pool.table.find(&pool.frames.headers, pid.raw()).is_none(),
        pool.requests.load(Ordering::Acquire),
        (0..pool.memory.grains()).all(|f| !pool.header(f).record_held()),
    );
    drop((pool, io, counted));
    assert!(settled.is_ok(), "the writeback did not settle: {settled:?}");
    assert_eq!(
        write_out,
        (true, true, true, 0, false),
        "the write-out: (pool failed, page resident in its frame, dirty, requests, record held)"
    );
    assert_eq!(
        load,
        (true, true, 0, true),
        "the load: (error, off the table, requests, every record free)"
    );
}

/// D2b §7.12: a queued load and a queued write-out whose pool drops before the IO thread takes
/// them each end with the owner-gone error; the records are at rest and the count is back.
#[test]
fn a_queued_request_whose_pool_dropped_before_its_take_ends_with_an_error() {
    let probe = Probe::new();
    let base = "/db/frame-owner-gone";
    let (pid, counted, pool) = page_in_a_cold_file(&probe, base, true);
    let io: Arc<dyn IO> = counted.clone();
    let leaf = drive(&io, || pool.store.leaf(pid)).unwrap();
    // A temporary page, whose number is no data page's: the cold page stays the load's.
    let (_, frame) = dirty_page(&pool, &io, true);
    let hold = counted.requests().hold_io_thread();
    // The load.
    let loading = {
        let _frame = Frame::enter();
        pool.fix_exclusive(pid, None)
            .map(|r| matches!(r, IOResult::IO(_)))
    };
    let load = last_frame_request();
    // The write-out.
    start_writing_out(&pool, frame);
    let writing = {
        let _frame = Frame::enter();
        pool.settle_writes().map(|r| matches!(r, IOResult::IO(_)))
    };
    let write = last_frame_request();
    drop((leaf, pool));
    drop(hold);
    let requests = counted.requests().clone();
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    let let_go = settle(|| requests.outstanding() == 0 && requests.frames_at_rest() == records);
    let errors = [&load, &write].map(|c| c.as_ref().and_then(Completion::get_error));
    drop((load, write, io, counted, requests));
    assert_eq!(
        (loading.ok(), writing.ok()),
        (Some(true), Some(true)),
        "(the load, the write-out) waiting on their queued requests"
    );
    assert!(let_go, "the records or the count were not let go");
    assert!(
        errors.iter().all(Option::is_some),
        "a request whose pool dropped ended without an error: {errors:?}"
    );
}

/// D2b §7.13: queued frame requests are counted at their queueing. With the IO thread held before
/// its take, a cold load and a write-out queued in frames are both counted and READY, and a close
/// that drains meanwhile waits for both; with the take released it finishes once both are let go
/// (mutant `count-at-take`: the count moved from the queueing to the take, so the close does not
/// wait).
#[test]
fn a_close_waits_for_queued_frame_requests_not_yet_taken() {
    let probe = Probe::new();
    let base = "/db/frame-close";
    let (pid, counted, pool) = page_in_a_cold_file(&probe, base, true);
    let io: Arc<dyn IO> = counted.clone();
    let leaf = drive(&io, || pool.store.leaf(pid)).unwrap();
    // A temporary page, whose number is no data page's: the cold page stays the load's.
    let (_, frame) = dirty_page(&pool, &io, true);
    let hold = counted.requests().hold_io_thread();
    let loading = {
        let _frame = Frame::enter();
        pool.fix_exclusive(pid, None)
            .map(|r| matches!(r, IOResult::IO(_)))
    };
    start_writing_out(&pool, frame);
    let writing = {
        let _frame = Frame::enter();
        pool.settle_writes().map(|r| matches!(r, IOResult::IO(_)))
    };
    let requests = counted.requests().clone();
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    let counted_before = (requests.outstanding(), records - requests.frames_at_rest());
    // The close drains on another thread: its decision with both queued, and its seal.
    let (decided_tx, decided) = std::sync::mpsc::channel();
    let closer = {
        let requests = requests.clone();
        let raw: Arc<dyn IO> = probe.clone();
        std::thread::spawn(move || {
            let parks = decided_tx.clone();
            crate::race::at("requests.drain_parks", move || {
                let _ = parks.send("parks");
            });
            crate::race::at("requests.drain_sealed", move || {
                let _ = decided_tx.send("sealed");
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
    let at_rest = requests.frames_at_rest();
    drop((leaf, pool, io, counted, requests));
    assert_eq!(
        (loading.ok(), writing.ok()),
        (Some(true), Some(true)),
        "(the load, the write-out) waiting on their queued requests"
    );
    assert_eq!(
        counted_before,
        (2, 2),
        "(requests counted, frame records taken) before the take"
    );
    assert_eq!(
        first_decision,
        Some("parks"),
        "the close's decision with both queued"
    );
    assert_eq!(then, Some("sealed"), "the close once both ran");
    assert!(closed, "the close did not end");
    assert_eq!(at_rest, records, "frame records at rest after the close");
}

/// What a queued failure's case ended with (D2b §7.14).
#[derive(Debug, PartialEq)]
struct Ended {
    /// The error the waiter or the settlement saw.
    error: Option<CompletionError>,
    resident: bool,
    dirty: bool,
    swap: bool,
    /// The pool's own count of its requests, and its records held.
    requests: usize,
    records_held: usize,
    failed: bool,
}

/// D2b §7.14 (a, b): a queued load whose stage open the backend refuses, and one whose read the
/// backend fails: the waiter gets the error (the injected one, exactly, for the backend's), and the
/// frame unloads; the count comes back and every record is free (mutants: `callback-forwards-ok`,
/// the carrier completing the pool's request with the whole page's bytes on a backend error, so the
/// page reads; `stage-error-swallowed`, so the waiter's wait never finishes and its bound reports
/// it).
#[test]
fn a_queued_loads_stage_or_backend_failure_is_its_waiters_error_and_unloads_the_frame() {
    let mut cases = Vec::new();
    for backend in [false, true] {
        let probe = Probe::new();
        let base = if backend {
            "/db/frame-load-fails-read"
        } else {
            "/db/frame-load-fails-open"
        };
        let (pid, counted, pool) = page_in_a_cold_file(&probe, base, true);
        let io: Arc<dyn IO> = counted.clone();
        let leaf = drive(&io, || pool.store.leaf(pid)).unwrap();
        if backend {
            probe.rule_under(&data(base), Some(Rule::Fail));
        } else {
            probe.refuse_opens_under(Some(&data(base)));
        }
        let read = in_frames(&io, || pool.fix_exclusive(pid, None)).map(|_| ());
        let requests = counted.requests().clone();
        let records = MIN_BUDGET / MIN_PAGE_SIZE;
        let let_go = settle(|| requests.outstanding() == 0 && requests.frames_at_rest() == records);
        let ended = Ended {
            error: match &read {
                Err(LimboError::CompletionError(e)) => Some(*e),
                _ => None,
            },
            resident: pool.table.find(&pool.frames.headers, pid.raw()).is_some(),
            dirty: false,
            swap: false,
            requests: pool.requests.load(Ordering::Acquire),
            records_held: (0..pool.memory.grains())
                .filter(|&f| pool.header(f).record_held())
                .count(),
            failed: pool.failed().is_err(),
        };
        cases.push((backend, read.is_err(), let_go, ended));
        drop((leaf, pool, io, counted, requests));
    }
    // The stage's error as the IO thread finishes the request with it; a waiter whose bound ran out
    // instead has no completion error at all (#4895).
    let stage =
        CompletionError::IOError(std::io::ErrorKind::Other, "a request's submission refused");
    for (backend, errored, let_go, ended) in cases {
        assert!(errored, "backend {backend}: the load did not fail");
        let (want, what) = if backend {
            (INJECTED, "the backend's error, forwarded")
        } else {
            (stage, "the stage's error, forwarded")
        };
        assert_eq!(
            ended.error,
            Some(want),
            "backend {backend}: the waiter's error: {what}, not its own bound running out"
        );
        assert!(
            let_go,
            "backend {backend}: the record or the count not let go"
        );
        assert_eq!(
            (
                ended.resident,
                ended.requests,
                ended.records_held,
                ended.failed
            ),
            (false, 0, 0, false),
            "backend {backend}: (resident, requests, records held, pool failed)"
        );
    }
}

/// D2b §7.14 (c, d): a queued write-out whose stage open the backend refuses, and one whose write
/// the backend fails: once the writeback settles the pool is failed, with the injected error for
/// the backend's; the page stays resident and dirty in its frame with no swap copy; the count comes
/// back and every record is free (mutant `callback-forwards-ok`: the carrier completing the write
/// with the whole page's bytes, so the page gets a swap copy and the pool does not fail).
#[test]
fn a_queued_write_outs_stage_or_backend_failure_fails_the_pool_and_keeps_the_page() {
    let mut cases = Vec::new();
    for (backend, temporary_page) in [(false, false), (true, false), (false, true), (true, true)] {
        let probe = Probe::new();
        let base = format!("/db/frame-write-fails-{backend}-{temporary_page}");
        let io: Arc<dyn IO> = probe.clone();
        let (counted, pool) = pool_on(io, &base, true);
        let io: Arc<dyn IO> = counted.clone();
        let (pid, frame) = dirty_page(&pool, &io, temporary_page);
        let prefix = if temporary_page {
            temporary(&base)
        } else {
            data(&base)
        };
        if backend {
            probe.rule_under(&prefix, Some(Rule::Fail));
        } else {
            probe.refuse_opens_under(Some(&prefix));
        }
        let swap_written = pool.swap_written();
        start_writing_out(&pool, frame);
        let settled = settle_in_frames(&pool, &io);
        let requests = counted.requests().clone();
        let records = MIN_BUDGET / MIN_PAGE_SIZE;
        let let_go = settle(|| requests.outstanding() == 0 && requests.frames_at_rest() == records);
        let h = pool.header(frame);
        let ended = Ended {
            error: pool.failure.get(),
            resident: h.status() == RESIDENT && h.pid() == pid.raw(),
            dirty: h.dirty(),
            swap: !temporary_page && pool.store.swap_slot(pid).is_some()
                || temporary_page && pool.temp.has_copy(pid),
            requests: pool.requests.load(Ordering::Acquire),
            records_held: (0..pool.memory.grains())
                .filter(|&f| pool.header(f).record_held())
                .count(),
            failed: pool.failed().is_err(),
        };
        let swap_unchanged = pool.swap_written() == swap_written;
        cases.push((
            backend,
            temporary_page,
            settled.is_ok(),
            let_go,
            swap_unchanged,
            ended,
        ));
        drop((pool, io, counted, requests));
    }
    for (backend, temporary_page, settled, let_go, swap_unchanged, ended) in cases {
        let case = format!("backend {backend}, temporary {temporary_page}");
        assert!(settled, "{case}: the writeback did not settle");
        assert!(let_go, "{case}: the record or the count not let go");
        if backend {
            assert_eq!(
                ended.error,
                Some(INJECTED),
                "{case}: the backend's error, forwarded"
            );
        }
        assert_eq!(
            (
                ended.failed,
                ended.resident,
                ended.dirty,
                ended.swap,
                ended.requests,
                ended.records_held
            ),
            (true, true, true, false, 0, 0),
            "{case}: (pool failed, resident, dirty, a copy marked, requests, records held)"
        );
        assert!(
            swap_unchanged,
            "{case}: swap bytes counted for a failed write"
        );
    }
}

/// D2b §7.6: the end's order. A write-out's frame, freed at its request's release, takes a new
/// request into the same record at once, from inside that release, just after the frame's record
/// is let go: the dispatch record is already at rest then, so the queueing is accepted, and the
/// new request runs to its own end (mutant `frame-retired-first`: the frame's record let go before
/// the dispatch record, so the queueing finds the record held and is refused).
#[test]
fn a_frame_freed_at_its_requests_release_queues_a_new_request_into_the_same_record_at_once() {
    use crate::storage::atoll::requests::Place;
    let probe = Probe::new();
    let base = "/db/frame-end-order";
    let io: Arc<dyn IO> = probe.clone();
    let (counted, pool) = pool_on(io, base, true);
    let io: Arc<dyn IO> = counted.clone();
    let (pid, frame) = dirty_page(&pool, &io, false);
    probe.rule_under(&data(base), Some(Rule::Hold));
    start_writing_out(&pool, frame);
    let settled = settle_in_frames(&pool, &io);
    let slot = pool.store.swap_slot(pid);
    // From inside the release, once the frame's record is let go: a new request into its record.
    let queued: Arc<Mutex<Option<(std::result::Result<(), String>, Completion)>>> =
        Arc::new(Mutex::new(None));
    if let Some(slot) = slot {
        let (pool, counted, queued) = (pool.clone(), counted.clone(), queued.clone());
        crate::race::at("requests.frame_retired", move || {
            let held = pool.header(frame).hold_record(frame, class0());
            let into = pool.memory.region.buffer(frame, class0().size());
            let c = Completion::new_read(into, |_| None);
            let place = Place::new(false, false, class0(), slot);
            let outcome = match counted.queue_frame(held, place, c.clone()) {
                Ok(()) => Ok(()),
                Err(refused) => {
                    pool.give_back(refused.held);
                    Err(refused.error.to_string())
                }
            };
            *queued.lock() = Some((outcome, c));
        });
    }
    probe.let_go();
    let reached = !crate::race::pending("requests.frame_retired");
    let second = queued.lock().take();
    let served = second
        .as_ref()
        .is_some_and(|(_, c)| settle(|| c.finished()));
    probe.let_go();
    let requests = counted.requests().clone();
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    let let_go = settle(|| {
        requests.outstanding() == 0
            && requests.frames_at_rest() == records
            && !pool.header(frame).record_held()
    });
    let outcome = second.map(|(outcome, c)| (outcome, c.succeeded()));
    let failure = pool.failed().err();
    drop((pool, io, counted, requests));
    assert!(settled.is_ok(), "the writeback did not settle: {settled:?}");
    assert!(slot.is_some(), "the page has no swap copy");
    assert!(reached, "the release never let the frame's record go");
    assert_eq!(
        outcome,
        Some((Ok(()), true)),
        "the new request into the same record, and its read"
    );
    assert!(served, "the new request never ran");
    assert!(let_go, "the records or the count were not let go");
    assert!(failure.is_none(), "the pool failed: {failure:?}");
}

/// D2b §7.4b: no source opens or reads a file. Each builds its read and hands it to the pool's hook,
/// which holds the frame's record before any open (mutant: `TempPages::read` opening its class's
/// file before the hook, today's order, which this census names).
#[test]
fn no_page_source_opens_or_reads_a_file_itself() {
    use crate::source_test::{code_of, methods_of};
    let read = |path: &str| {
        code_of(&std::fs::read_to_string(format!("{}/{path}", env!("CARGO_MANIFEST_DIR"))).unwrap())
    };
    let (source, temp) = (
        read("storage/atoll/buffer/source.rs"),
        read("storage/atoll/buffer/temp.rs"),
    );
    let bodies: Vec<(&str, String, &str)> = [
        (
            "LocalLocated",
            methods_of(&source, "impl Located for LocalLocated"),
            "read",
        ),
        (
            "LocalLocated",
            methods_of(&source, "impl LocalLocated"),
            "read_copy",
        ),
        ("TempPages", methods_of(&temp, "impl TempPages"), "read"),
    ]
    .into_iter()
    .filter_map(|(owner, methods, name)| {
        methods
            .into_iter()
            .find(|(m, _)| m == name)
            .map(|(m, body)| (owner, m, body))
    })
    .collect();
    let calls = [
        "file(",
        "data_file(",
        "open_file",
        "file_at(",
        "pread(",
        "pwrite(",
    ];
    let found: Vec<(&str, String, &str)> = bodies
        .iter()
        .flat_map(|(owner, m, body)| {
            calls
                .iter()
                .filter(|c| body.contains(**c))
                .map(move |c| (*owner, m.clone(), *c))
        })
        .collect();
    assert_eq!(bodies.len(), 3, "the sources' read methods read");
    assert!(
        found.is_empty(),
        "a source opens or reads a file itself: {found:?}"
    );
}

/// D2b §7.7: the admission. The production sites that hold a frame's record are the load's hook
/// and the write-out's submission, and each lets the held record go on every arm, by queueing it,
/// keeping it until its request retires, or giving it back (mutant: one refusal arm dropping the
/// token, which this census names).
#[test]
fn every_held_frame_record_reaches_one_of_its_three_consumers() {
    use crate::source_test::{code_of, methods_of, without_test_modules};
    let read = |path: &str| {
        without_test_modules(&code_of(
            &std::fs::read_to_string(format!("{}/{path}", env!("CARGO_MANIFEST_DIR"))).unwrap(),
        ))
    };
    let (pool, clean) = (
        read("storage/atoll/buffer/mod.rs"),
        read("storage/atoll/buffer/clean.rs"),
    );
    // Every production file of the pool: its test files left out, and any test module inline.
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("storage/atoll/buffer");
    let mut sites: Vec<String> = Vec::new();
    for path in crate::source_test::rust_sources(&dir) {
        let name = path.file_name().unwrap().to_str().unwrap().to_string();
        if name.ends_with("tests.rs") {
            continue;
        }
        let code = without_test_modules(&code_of(&std::fs::read_to_string(&path).unwrap()));
        sites.extend(code.matches(".hold_record(").map(|_| name.clone()));
    }
    let body = |code: &str, header: &str, name: &str| {
        methods_of(code, header)
            .into_iter()
            .find(|(m, _)| m == name)
            .map(|(_, b)| b.to_string())
            .unwrap_or_default()
    };
    let hook = body(&pool, "impl source::Submit for LoadSubmit<'_>", "read");
    let load = body(&pool, "impl BufferPool", "load");
    let submit = body(&clean, "impl Writeback", "submit");
    let needs: [(&str, &str, &str); 8] = [
        ("the hook", &hook, "queue_frame(held"),
        ("the hook", &hook, "self.parked.set(Some(refused.held))"),
        ("the hook", &hook, "self.parked.set(Some(held))"),
        ("the load", &load, "self.until_retired(held"),
        ("the load", &load, "self.give_back(held)"),
        ("the write-out", &submit, "queue_frame(record"),
        ("the write-out", &submit, "pool.give_back(refused.held)"),
        ("the write-out", &submit, "pool.give_back(record)"),
    ];
    let missing: Vec<_> = needs
        .iter()
        .filter(|(_, body, needed)| !body.contains(needed))
        .map(|(site, _, needed)| (*site, *needed))
        .collect();
    let kept = submit.contains("pool.until_retired(record");
    assert_eq!(
        sites,
        ["clean.rs", "mod.rs"],
        "the production sites that hold a frame's record"
    );
    assert!(
        missing.is_empty(),
        "a held record let go by no consumer: {missing:?}"
    );
    assert!(
        kept,
        "the write-out's in-place request keeps no record until it retires"
    );
}

/// D2b §7.8 over the ring (child): in a database opened async over io_uring, a frame's miss on a
/// page in a cold data file and a dirty page's write-out in a frame are opened, read and written
/// on the IO thread, which may wait on the file system where the ring's delivery thread never may
/// (mutant `io-thread-as-ring`: the IO thread marked as the ring's delivery thread, so the ring
/// refuses the data file's open and the child ends).
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
#[test]
fn a_frames_cold_load_and_write_out_over_the_ring_run_on_an_io_thread() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_frames_cold_load_and_write_out_over_the_ring_run_on_an_io_thread"
        ),
        || {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().join("db").to_str().unwrap().to_string();
            let ring: Arc<dyn IO> = Arc::new(crate::io::UringIO::new().unwrap());
            // A copy in a swap slot, written in place by a store of its own.
            let (first_counted, first) = pool_on(ring.clone(), &base, false);
            let first_io: Arc<dyn IO> = first_counted.clone();
            let (pid, frame) = dirty_page(&first, &first_io, false);
            start_writing_out(&first, frame);
            drive(&first_io, || first.settle_writes()).unwrap();
            let slot = first.store.swap_slot(pid).expect("a swap copy");
            drop((first, first_io, first_counted));
            let (counted, pool) = pool_on(ring, &base, true);
            let io: Arc<dyn IO> = counted.clone();
            pool.store.set_swap(pid, slot);
            let read =
                in_frames(&io, || pool.fix_exclusive(pid, None)).map(|g| stamped(g.page(), pid));
            // A temporary page's write-out in a frame: its cold file's open and the write.
            let (other, frame) = dirty_page(&pool, &io, true);
            start_writing_out(&pool, frame);
            let settled = settle_in_frames(&pool, &io);
            let written = pool.temp.has_copy(other);
            let failure = pool.failed().err();
            drop((pool, io, counted));
            assert_eq!(read.ok(), Some(Some(1)), "the page read back");
            assert!(settled.is_ok(), "the writeback did not settle: {settled:?}");
            assert!(written, "the temporary page has no copy");
            assert!(failure.is_none(), "the pool failed: {failure:?}");
        },
    );
    child_ended_cleanly(ended);
}

/// D2b §7.14, held (#4890): a queued load and a queued write-out whose backend fails them and still
/// owns their carrier. The load's waiter has the injected error and the frame unloads; the
/// write-out's settlement fails the pool and keeps the page; meanwhile the dispatch record, the
/// frame's record and the database's count of the request all stay held, until the carrier retires,
/// when each is let go once (mutant `early-release-on-error`: a request whose carrier failed ended
/// at its submission's return, before the carrier retired).
#[test]
fn a_queued_failure_holds_its_records_until_its_carrier_retires() {
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    // The load.
    let probe = Probe::new();
    let base = "/db/frame-fails-held-read";
    let (pid, counted, pool) = page_in_a_cold_file(&probe, base, true);
    let io: Arc<dyn IO> = counted.clone();
    let leaf = drive(&io, || pool.store.leaf(pid)).unwrap();
    probe.rule_under(&data(base), Some(Rule::HoldFail));
    let read = in_frames(&io, || pool.fix_exclusive(pid, None)).map(|_| ());
    let requests = counted.requests().clone();
    let held = |pool: &Arc<BufferPool>| {
        (0..pool.memory.grains())
            .filter(|&f| pool.header(f).record_held())
            .count()
    };
    let load_held = (
        probe.holding(),
        requests.outstanding(),
        records - requests.frames_at_rest(),
        held(&pool),
        pool.table.find(&pool.frames.headers, pid.raw()).is_none(),
    );
    probe.let_go();
    let load_let_go = settle(|| {
        requests.outstanding() == 0 && requests.frames_at_rest() == records && held(&pool) == 0
    });
    probe.let_go();
    let load_after = (
        requests.outstanding(),
        requests.frames_at_rest(),
        held(&pool),
    );
    let load_error = match &read {
        Err(LimboError::CompletionError(e)) => Some(*e),
        _ => None,
    };
    drop((leaf, pool, io, counted, requests));
    // The write-out.
    let probe = Probe::new();
    let base = "/db/frame-fails-held-write";
    let io: Arc<dyn IO> = probe.clone();
    let (counted, pool) = pool_on(io, base, true);
    let io: Arc<dyn IO> = counted.clone();
    let (pid, frame) = dirty_page(&pool, &io, false);
    probe.rule_under(&data(base), Some(Rule::HoldFail));
    start_writing_out(&pool, frame);
    let settled = settle_in_frames(&pool, &io);
    let requests = counted.requests().clone();
    let h = pool.header(frame);
    let write_held = (
        probe.holding(),
        requests.outstanding(),
        records - requests.frames_at_rest(),
        h.record_held(),
        pool.failure.get(),
        h.status() == RESIDENT && h.pid() == pid.raw() && h.dirty(),
        pool.store.swap_slot(pid).is_some(),
    );
    probe.let_go();
    let write_let_go = settle(|| {
        requests.outstanding() == 0
            && requests.frames_at_rest() == records
            && !pool.header(frame).record_held()
    });
    probe.let_go();
    let write_after = (
        requests.outstanding(),
        requests.frames_at_rest(),
        held(&pool),
    );
    drop((pool, io, counted, requests));
    assert_eq!(
        load_error,
        Some(INJECTED),
        "the load's waiter has the backend's error"
    );
    assert_eq!(
        load_held,
        (1, 1, 1, 1, true),
        "the load, its carrier held: (carriers held, requests counted, dispatch records taken, \
         frame records held, off the table)"
    );
    assert!(
        load_let_go,
        "the load's records or count not let go at the carrier's retirement"
    );
    assert_eq!(
        load_after,
        (0, records, 0),
        "the load let go more than once"
    );
    assert!(settled.is_ok(), "the writeback did not settle: {settled:?}");
    assert_eq!(
        write_held,
        (1, 1, 1, true, Some(INJECTED), true, false),
        "the write-out, its carrier held: (carriers held, requests counted, dispatch records \
         taken, frame record held, the pool's failure, the page dirty and resident, a swap copy)"
    );
    assert!(
        write_let_go,
        "the write-out's records or count not let go at the carrier's retirement"
    );
    assert_eq!(
        write_after,
        (0, records, 0),
        "the write-out let go more than once"
    );
}
