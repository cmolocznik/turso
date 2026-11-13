//! The pool's frames, queued (DESIGN.md 22.18; ASYNC1 D2b §7): in a database opened async, a load
//! or a write-out a cooperative frame makes is queued in its frame's record, a cold file opened as
//! its first stage on an IO thread, and the backend sees only a carrier of the IO thread's own.
//! Every backend call is recorded with its file and its thread.

use super::*;
use crate::io::clock::{Clock, MonotonicInstant, WallClockInstant};
use crate::io::{Delivery, FileSyncType, MemoryIO, OpenFlags, Stepped, IO};
use crate::storage::atoll::cooperative::Frame;
use crate::storage::atoll::db::Opening;
use crate::storage::atoll::page::{HEADER_SIZE, MIN_PAGE_SIZE};
use crate::storage::atoll::requests::{
    counted_async_with_frames_for_test, counted_for_test, Counted, Requests, Turn,
};
use crate::storage::atoll::store::StoreConfig;
#[cfg(all(unix, not(miri)))]
use crate::storage::atoll::testing::in_child;
use crate::storage::atoll::testing::{drive, io_thread_held_before_idle_unregistered};
use crate::storage::atoll::tests::{config, ending_inside_a_block, BASE};
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
    /// Itself, for each file it opens to hold.
    me: Weak<Probe>,
    inner: MemoryIO,
    calls: Mutex<Vec<Call>>,
    refuse_open: Mutex<Option<String>>,
    rule: Mutex<Option<(String, Rule)>>,
    held: Mutex<Vec<Completion>>,
    /// The files it opened and that have not yet dropped, by path.
    live: Mutex<std::collections::HashMap<String, usize>>,
}

impl Probe {
    fn new() -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            ..Self::default()
        })
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

    /// The files it opened at `path` that have not yet dropped.
    fn live(&self, path: &str) -> usize {
        self.live.lock().get(path).copied().unwrap_or(0)
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
        let file = self.inner.open_file(path, flags, direct)?;
        *self.live.lock().entry(path.to_string()).or_default() += 1;
        Ok(Arc::new(ProbeFile {
            file,
            path: path.to_string(),
            probe: self.me.upgrade().expect("a probe opening a file is alive"),
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
    /// Its probe, held so the probe outlives the file: a file an IO thread drops after the test has
    /// let go of everything else still finds the probe's count of its files.
    probe: Arc<Probe>,
}

impl ProbeFile {
    fn probe(&self) -> &Probe {
        &self.probe
    }
}

impl Drop for ProbeFile {
    fn drop(&mut self) {
        let mut live = self.probe().live.lock();
        let open = live
            .get_mut(&self.path)
            .expect("a probe's file counted at its open");
        *open -= 1;
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
            // Held before it is finished: a look that finds `c` finished finds it held (#4900).
            Some(Rule::HoldFail) => {
                c.accept();
                probe.held.lock().push(c.clone());
                c.error(INJECTED);
                Ok(c)
            }
            Some(Rule::Hold) => {
                // Held, then the read now, its callback through `c`; `c`'s retirement when the
                // probe lets it go.
                c.accept();
                probe.held.lock().push(c.clone());
                let owner = c.clone();
                let inner = Completion::new_read(c.as_read().buf_arc(), move |res| {
                    match res {
                        Ok((_, n)) => owner.complete(n),
                        Err(e) => owner.error(e),
                    }
                    None
                });
                // The probe's own backend refuses nothing: a refusal here is the fixture broken,
                // and no request it accepted may be finished or retired as though it ended.
                if self.file.pread(at, inner).is_err() {
                    crate::io::fatal("frame_tests: the probe's memory backend refused a read");
                }
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
                probe.held.lock().push(c.clone());
                c.error(INJECTED);
                Ok(c)
            }
            Some(Rule::Hold) => {
                c.accept();
                probe.held.lock().push(c.clone());
                let owner = c.clone();
                let inner = Completion::new_write(move |res| match res {
                    Ok(n) => owner.complete(n),
                    Err(e) => owner.error(e),
                });
                if self.file.pwrite(at, buffer, inner).is_err() {
                    crate::io::fatal("frame_tests: the probe's memory backend refused a write");
                }
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
    let (mut pids, counted, pool) = pages_in_a_cold_file(probe.clone(), base, opened_async, 1);
    (pids.remove(0), counted, pool)
}

/// [`page_in_a_cold_file`] for `n` pages over `io`, any backend.
fn pages_in_a_cold_file(
    io: Arc<dyn IO>,
    base: &str,
    opened_async: bool,
    n: usize,
) -> (Vec<PageId>, Arc<Counted>, Arc<BufferPool>) {
    let (first_counted, first) = pool_on(io.clone(), base, false);
    let first_io: Arc<dyn IO> = first_counted.clone();
    let written: Vec<_> = (0..n)
        .map(|_| dirty_page(&first, &first_io, false))
        .collect();
    for &(_, frame) in &written {
        start_writing_out(&first, frame);
    }
    drive(&first_io, || first.settle_writes()).unwrap();
    let slots: Vec<_> = written
        .iter()
        .map(|&(pid, _)| {
            let slot = first
                .store
                .swap_slot(pid)
                .expect("the page written out to a swap slot");
            (pid, slot)
        })
        .collect();
    drop((first, first_io, first_counted));
    let (counted, pool) = pool_on(io, base, opened_async);
    for &(pid, slot) in &slots {
        pool.store.set_swap(pid, slot);
    }
    (
        slots.into_iter().map(|(pid, _)| pid).collect(),
        counted,
        pool,
    )
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
/// child). The write is let go only once the IO thread has run the branch after its backend call,
/// which installs the release on the write's retirement, so the release and its wake run on the
/// test thread inside `let_go` (#4922).
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
            let requests = counted.requests().clone();
            let (settled, paused, ran) =
                past_the_branch(&requests, || settle_in_frames(&pool, &io));
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
            drop((wait, pool, io, counted, requests));
            assert!(settled.is_ok(), "the writeback did not settle: {settled:?}");
            assert_eq!(
                (paused, ran),
                (true, true),
                "the IO thread reached the pause after the write's backend call, then the branch's end"
            );
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
/// the dispatch record, so the queueing finds the record held and is refused). The carrier is let
/// go only once the IO thread has run the branch after its backend call, which installs the
/// release on its retirement, the write's waiter having returned first (#4918).
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
    let requests = counted.requests().clone();
    let (settled, paused, ran) = past_the_branch(&requests, || settle_in_frames(&pool, &io));
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
    assert_eq!(
        (paused, ran),
        (true, true),
        "the IO thread reached the pause after the write's backend call, then the branch's end"
    );
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

/// Runs `ask` with the next queued frame request paused between its backend call's return and the
/// branch after it, then lets the IO thread on and waits until that branch has run, whichever arm it
/// took (#4900): `ask` returns while the IO thread has not yet run it, and a look after this sees
/// what it left. `ask`'s result, and whether the IO thread reached the pause and then the branch's
/// end, each within [`SETTLE`].
fn past_the_branch<T>(requests: &Requests, ask: impl FnOnce() -> T) -> (T, bool, bool) {
    let (returned, go) = requests.pause_frame_returned();
    let served = requests.on_frame_served();
    let out = ask();
    let paused = returned.recv_timeout(SETTLE).is_ok();
    let _ = go.send(());
    let ran = served.recv_timeout(SETTLE).is_ok();
    (out, paused, ran)
}

/// D2b §7.14, held (#4890): a queued load and a queued write-out whose backend fails them and still
/// owns their carrier. The load's waiter has the injected error and the frame unloads; the
/// write-out's settlement fails the pool and keeps the page; meanwhile the dispatch record, the
/// frame's record and the database's count of the request all stay held, until the carrier retires,
/// when each is let go once (mutant `early-release-on-error`: a request whose carrier failed ended
/// at its submission's return, before the carrier retired). Each look waits for the IO thread to
/// have run the branch after its backend call, the waiter having returned first (#4900).
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
    let requests = counted.requests().clone();
    let (read, load_paused, load_ran) = past_the_branch(&requests, || {
        in_frames(&io, || pool.fix_exclusive(pid, None)).map(|_| ())
    });
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
    let requests = counted.requests().clone();
    let (settled, write_paused, write_ran) =
        past_the_branch(&requests, || settle_in_frames(&pool, &io));
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
        (load_paused, load_ran, write_paused, write_ran),
        (true, true, true, true),
        "the IO thread reached the pause after each backend call, then the branch's end: (load \
         paused, load's branch run, write-out paused, write-out's branch run)"
    );
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

/// The log's own segment 0 of the database at [`BASE`]: once a recovery has opened its log, only
/// the log, its transactions and its checkpointer hold it, none of the open's other owners.
fn log_segment() -> String {
    format!("{BASE}.log.{:016x}", 0)
}

/// A database that ends inside a block, reopened over `probe`, whose recovery fails at its full
/// checkpoint's write to the log (RING-FD1 §5, test 9). The log's reads and writes fail and stay
/// held from recovery's superblock write on, which comes after its scan has read the log and the
/// log has opened its segment: the checkpoint's log write ends in an error the probe still owns.
/// The open, and its `work()`'s error.
fn a_recovery_failing_at_its_logs_write(probe: &Arc<Probe>) -> (Opening, Option<String>) {
    let io: Arc<dyn IO> = probe.clone();
    ending_inside_a_block(io.clone());
    let opening = Opening::new(io, BASE, config(), None).unwrap();
    {
        let probe = probe.clone();
        crate::race::at("control.write_and_wait", move || {
            probe.rule_under(&format!("{BASE}.log."), Some(Rule::HoldFail));
        });
    }
    let failed = opening.work().err().map(|e| e.to_string());
    (opening, failed)
}

/// `close`, which drops an open on this thread, with the log's segment looked at when the open's
/// drain first looks at its requests; the probe then lets its held requests go, so the drain ends.
/// The segment's files open then and after `close`, and `close`'s value.
fn the_log_segment_across_the_drain<T>(
    probe: &Arc<Probe>,
    close: impl FnOnce() -> T,
) -> (Option<usize>, usize, T) {
    let at_drain = std::rc::Rc::new(std::cell::Cell::new(None));
    {
        let (probe, at_drain) = (probe.clone(), at_drain.clone());
        crate::race::at("requests.drain_looked", move || {
            at_drain.set(Some(probe.live(&log_segment())));
            probe.let_go();
        });
    }
    let value = close();
    (at_drain.get(), probe.live(&log_segment()), value)
}

/// RING-FD1 §5, test 9 (a, b): a recovery that fails with its log's write held keeps its owners in
/// the open, whose drop drains first. The log's segment is open after the failure and at the
/// drain's look, and closed once the open has dropped (mutants: `opening-fields-first`, the open
/// without its drop, its fields dropped before its executor's drain; `partial-locals`, recovery's
/// owners its locals, gone as it returns. Each closes the segment before the drain).
#[test]
fn a_failed_recoverys_owners_drop_only_after_the_opens_drain() {
    let probe = Probe::new();
    let (opening, failed) = a_recovery_failing_at_its_logs_write(&probe);
    let (held, open) = (probe.holding(), probe.live(&log_segment()));
    let (at_drain, after, ()) = the_log_segment_across_the_drain(&probe, || drop(opening));
    let left = probe.holding();
    assert_eq!(
        (failed.is_some(), held > 0),
        (true, true),
        "(the recovery failed, a log request held): {failed:?}"
    );
    assert_eq!(
        (open, at_drain, after),
        (1, Some(1), 0),
        "(the log's segment open after the failure, at the drain's look, after the drop)"
    );
    assert_eq!(left, 0, "a held request left after the drop");
}

/// RING-FD1 §5, test 9 (c): `finish` after the failed recovery returns its error and puts the
/// unfinished recovery back in the open, whose drop drains before the recovery's owners go: the
/// log's segment is open at the drain's look, and closed after (mutant `finish-unrestored`: the
/// unfinished recovery dropped as `finish`'s local before the open's drain).
#[test]
fn a_finish_after_a_failed_recovery_drops_its_owners_only_after_the_drain() {
    let probe = Probe::new();
    let (opening, failed) = a_recovery_failing_at_its_logs_write(&probe);
    let (held, open) = (probe.holding(), probe.live(&log_segment()));
    let (at_drain, after, finished) =
        the_log_segment_across_the_drain(&probe, || opening.finish().err().map(|e| e.to_string()));
    let left = probe.holding();
    assert_eq!(
        (failed.is_some(), held > 0),
        (true, true),
        "(the recovery failed, a log request held): {failed:?}"
    );
    assert!(
        finished
            .as_deref()
            .is_some_and(|e| e.contains("before recovery is done")),
        "finish after the failed recovery: {finished:?}"
    );
    assert_eq!(
        (open, at_drain, after),
        (1, Some(1), 0),
        "(the log's segment open after the failure, at the drain's look, after the drop)"
    );
    assert_eq!(left, 0, "a held request left after the drop");
}

// ASYNC1 D2d (ii): over a pumped backend the database's IO thread steps what it submits, so a
// queued frame request reaches its end with no one waiting for it and no one else stepping.

/// D2d §6.1 test 1, over `backend`: a frame's load whose wait is dropped, a load cancelled once the
/// backend has accepted its read, and a write-out whose wait is dropped, each queued in a frame
/// while the IO thread is held before its idle with its registration withdrawn, so only the step it
/// owes after the record can drive the request. The pool has no workers, and once a request is
/// queued the test steps nothing. At the IO thread's next idle each request has ended: the frame
/// records at rest and nothing counted (mutant `no-io-driver`: the loop never steps, and the
/// request is left in the backend there).
fn the_io_thread_drives_the_frame_requests_no_one_waits_for(backend: Arc<dyn IO>) {
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    let base = "/db/frame-driven";
    let (pids, counted, pool) = pages_in_a_cold_file(backend, base, true, 2);
    let io: Arc<dyn IO> = counted.clone();
    // The page-table leaves resident, so each load's own request is the one queued.
    let leaves: Vec<_> = pids
        .iter()
        .map(|&pid| drive(&io, || pool.store.leaf(pid)).unwrap())
        .collect();
    let requests = counted.requests().clone();
    let at_rest = || (requests.frames_at_rest(), requests.outstanding());
    // A load, its wait dropped with the poll's result.
    let held = io_thread_held_before_idle_unregistered(&requests);
    let reached = held.is_some();
    let idle = requests.on_io_idle();
    let load = {
        let _frame = Frame::enter();
        pool.fix_exclusive(pids[0], None)
            .map(|r| matches!(r, IOResult::IO(_)))
    };
    drop(held);
    let idled = idle.recv_timeout(SETTLE).is_ok();
    let after_load = at_rest();
    // A load cancelled after its take: its carrier accepted by the backend, then its own request
    // finished as a backend's cancel finishes it, and its wait dropped.
    let held = io_thread_held_before_idle_unregistered(&requests);
    let reached = reached && held.is_some();
    let idle = requests.on_io_idle();
    let (returned, go) = requests.pause_frame_returned();
    let second = {
        let _frame = Frame::enter();
        pool.fix_exclusive(pids[1], None)
            .map(|r| matches!(r, IOResult::IO(_)))
    };
    drop(held);
    let accepted = returned.recv_timeout(SETTLE).is_ok();
    let cancelled = last_frame_request();
    if let Some(c) = &cancelled {
        c.abort();
    }
    let finished_at_cancel = cancelled.as_ref().is_some_and(|c| c.finished());
    drop(cancelled);
    let _ = go.send(());
    let idled = idled && idle.recv_timeout(SETTLE).is_ok();
    let after_cancel = at_rest();
    // A write-out of the page the first load read, changed, its settlement's wait dropped with the
    // poll's result.
    let frame = {
        let mut g = drive(&io, || pool.fix_exclusive(pids[0], None)).unwrap();
        stamp(&mut g.page_mut(), pids[0], 2);
        g.changed();
        g.frame
    };
    start_writing_out(&pool, frame);
    let held = io_thread_held_before_idle_unregistered(&requests);
    let reached = reached && held.is_some();
    let idle = requests.on_io_idle();
    let write_out = {
        let _frame = Frame::enter();
        pool.settle_writes().map(|r| matches!(r, IOResult::IO(_)))
    };
    drop(held);
    let idled = idled && idle.recv_timeout(SETTLE).is_ok();
    let after_write = at_rest();
    drop((leaves, pool, io, counted, requests));
    assert!(reached, "the IO thread never reached its pause before idle");
    assert_eq!(
        (load.ok(), second.ok(), write_out.ok()),
        (Some(true), Some(true), Some(true)),
        "a frame's miss or settlement returned no wait"
    );
    assert!(
        accepted && finished_at_cancel,
        "the cancelled load's carrier not accepted, or its request not finished by the cancel"
    );
    assert!(idled, "the IO thread's idle notes");
    assert_eq!(
        [after_load, after_cancel, after_write],
        [(records, 0); 3],
        "(frame records at rest, requests counted) at the IO thread's idle after the load, the \
         cancelled load and the write-out: a request no one waits for left in the backend"
    );
}

#[test]
fn over_crash_io_the_io_thread_drives_the_frame_requests_no_one_waits_for() {
    the_io_thread_drives_the_frame_requests_no_one_waits_for(Arc::new(
        crate::io::crash::CrashIO::new(9),
    ));
}

#[cfg(feature = "io_memory_yield")]
#[test]
fn over_deferred_memory_the_io_thread_drives_the_frame_requests_no_one_waits_for() {
    the_io_thread_drives_the_frame_requests_no_one_waits_for(Arc::new(
        crate::io::MemoryYieldIO::new(),
    ));
}

/// D2d §6.1 test 5, over CrashIO: three loads queued by three frames' misses while the IO thread
/// is held, its registration withdrawn, are served one at a time, with a step of the backend
/// between any two, so records coming one after another never keep the backend unstepped (mutant
/// `step-only-on-empty`: the records, then the steps).
#[test]
fn over_a_pumped_backend_the_io_thread_steps_between_any_two_records_it_serves() {
    let backend: Arc<dyn IO> = Arc::new(crate::io::crash::CrashIO::new(11));
    let (pids, counted, pool) = pages_in_a_cold_file(backend, "/db/frame-fair", true, 3);
    let io: Arc<dyn IO> = counted.clone();
    // The page-table leaves resident, so each miss queues the page's own load.
    let leaves: Vec<_> = pids
        .iter()
        .map(|&pid| drive(&io, || pool.store.leaf(pid)).unwrap())
        .collect();
    let requests = counted.requests().clone();
    let held = io_thread_held_before_idle_unregistered(&requests);
    let reached = held.is_some();
    let idle = requests.on_io_idle();
    let queued: Vec<_> = pids
        .iter()
        .map(|&pid| {
            let _frame = Frame::enter();
            pool.fix_exclusive(pid, None)
                .map(|r| matches!(r, IOResult::IO(_)))
                .ok()
        })
        .collect();
    requests.record_io_turns();
    drop(held);
    let idled = idle.recv_timeout(SETTLE).is_ok();
    let turns = requests.io_turns();
    drop((leaves, pool, io, counted, requests));
    let served = turns.iter().filter(|t| **t == Turn::Served).count();
    assert!(
        reached && idled,
        "(held before idle, idle again): {turns:?}"
    );
    assert_eq!(queued, [Some(true); 3], "a frame's miss returned no wait");
    assert!(served >= 3, "fewer than three records served: {turns:?}");
    assert!(
        !turns.windows(2).any(|w| w == [Turn::Served, Turn::Served]),
        "two records served with no step of the backend between them: {turns:?}"
    );
}

// ASYNC1 D2d (iii): a frame's write-out its backend writes short is continued in its record, part
// by part, by the IO thread, from the same region.

/// One write a halving backend took under its prefix.
#[derive(Clone, Debug)]
struct Part {
    path: String,
    pos: u64,
    wrote: usize,
    /// The thread that submitted it.
    thread: String,
    /// The frame services under way on that thread then: 1 for a part the IO thread's own service
    /// of the record submits.
    services: usize,
    /// The frame records at rest then.
    at_rest: usize,
}

/// A backend over `inner` whose files under its prefix write half of each write they are asked, in
/// whole direct-IO blocks and a block at least, as a direct file's short write lands; or nothing
/// once `zero_from` such writes have been taken. Each recorded, and the first one's bytes kept.
struct Halving {
    /// Itself, for each file it opens to hold.
    me: Weak<Halving>,
    inner: Arc<dyn IO>,
    prefix: String,
    zero_from: Mutex<Option<usize>>,
    parts: Mutex<Vec<Part>>,
    /// The bytes the first write under the prefix was asked to write.
    first_asked: Mutex<Option<Vec<u8>>>,
    requests: Mutex<Option<Weak<Requests>>>,
}

/// What a halving backend writes of a write of `len` bytes: half, in whole direct-IO blocks, a
/// block at least.
fn halved(len: usize) -> usize {
    let block = crate::io::DIRECT_IO_ALIGNMENT;
    (len / 2 / block * block).max(block).min(len)
}

/// The parts a halving backend writes a region of `len` bytes in.
fn halved_parts(len: usize) -> Vec<usize> {
    let mut parts = Vec::new();
    let mut rest = len;
    while rest > 0 {
        parts.push(halved(rest));
        rest -= halved(rest);
    }
    parts
}

/// The page class the halving tests write: 16 KiB, so every part and its rest stay whole blocks.
fn halving_class() -> SizeClass {
    SizeClass::new(2).unwrap()
}

impl Halving {
    fn new(inner: Arc<dyn IO>, prefix: &str) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            inner,
            prefix: prefix.to_string(),
            zero_from: Mutex::new(None),
            parts: Mutex::new(Vec::new()),
            first_asked: Mutex::new(None),
            requests: Mutex::new(None),
        })
    }

    /// The requests whose frame records each part's record reads.
    fn watch(&self, requests: &Arc<Requests>) {
        *self.requests.lock() = Some(Arc::downgrade(requests));
    }

    fn parts(&self) -> Vec<Part> {
        self.parts.lock().clone()
    }

    fn first_asked(&self) -> Option<Vec<u8>> {
        self.first_asked.lock().clone()
    }

    /// Forgets every write taken so far, and writes nothing from the `zero_from`th write on.
    fn forget(&self, zero_from: Option<usize>) {
        self.parts.lock().clear();
        *self.first_asked.lock() = None;
        *self.zero_from.lock() = zero_from;
    }
}

impl Clock for Halving {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for Halving {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(HalvingFile {
            file,
            path: path.to_string(),
            io: self
                .me
                .upgrade()
                .expect("a backend opening a file is alive"),
        }))
    }
    fn sync_directory(&self, path: &str) -> Result<()> {
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

struct HalvingFile {
    file: Arc<dyn File>,
    path: String,
    io: Arc<Halving>,
}

impl File for HalvingFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }
    fn pread(&self, at: u64, c: Completion) -> Result<Completion> {
        self.file.pread(at, c)
    }
    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<crate::io::Buffer>,
        c: Completion,
    ) -> Result<Completion> {
        if !self.path.starts_with(&self.io.prefix) {
            return self.file.pwrite(pos, buffer, c);
        }
        let at_rest = self
            .io
            .requests
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .map_or(usize::MAX, |requests| requests.frames_at_rest());
        let wrote = {
            let mut parts = self.io.parts.lock();
            let zero_from = *self.io.zero_from.lock();
            let wrote = if zero_from.is_some_and(|z| parts.len() >= z) {
                0
            } else {
                halved(buffer.len())
            };
            if parts.is_empty() {
                *self.io.first_asked.lock() = Some(buffer.as_slice().to_vec());
            }
            parts.push(Part {
                path: self.path.clone(),
                pos,
                wrote,
                thread: crate::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_string(),
                services: crate::storage::atoll::requests::FrameService::here(),
                at_rest,
            });
            wrote
        };
        if wrote == 0 {
            c.accept();
            c.deliver(Ok(0));
            return Ok(c);
        }
        // Aligned, as a direct file's write must be: `wrote` is whole blocks.
        let half =
            Arc::new(crate::storage::atoll::aligned::AlignedBlock::zeroed(wrote)).region(0, wrote);
        half.as_mut_slice()
            .copy_from_slice(&buffer.as_slice()[..wrote]);
        self.file.pwrite(pos, half, c)
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

/// A pool over a halving backend over `inner`, opened async, and a page of the halving class
/// written and changed with its write-out started: the backend (its writes so far forgotten, and
/// writing nothing from the `zero_from`th write on), the counted IO and the pool. A first page's
/// write-out has run to its end before, in frames, so the second's first poll queues the write
/// itself, every file and page it needs at hand.
fn halving_write_out(
    inner: Arc<dyn IO>,
    base: &str,
    zero_from: Option<usize>,
) -> (Arc<Halving>, Arc<Counted>, Arc<BufferPool>) {
    let halving = Halving::new(inner, &format!("{base}.c{}.", halving_class().index()));
    let (counted, pool) = pool_on(halving.clone(), base, true);
    halving.watch(counted.requests());
    let io: Arc<dyn IO> = counted.clone();
    let first = dirty_page_of(&pool, &io, halving_class());
    let second = dirty_page_of(&pool, &io, halving_class());
    start_writing_out_of(&pool, halving_class(), first);
    settle_in_frames(&pool, &io).expect("the first write-out settles");
    drop(crate::storage::atoll::testing::io_thread_held_before_idle(
        counted.requests(),
    ));
    halving.forget(zero_from);
    start_writing_out_of(&pool, halving_class(), second);
    (halving, counted, pool)
}

/// A new page of `class`, stamped and changed, left resident and dirty: its frame.
fn dirty_page_of(pool: &Arc<BufferPool>, io: &Arc<dyn IO>, class: SizeClass) -> u32 {
    let mut g = drive(io, || pool.allocate(class)).unwrap();
    let pid = g.pid();
    stamp(&mut g.page_mut(), pid, 1);
    g.changed();
    g.frame
}

/// [`start_writing_out`] for a page of `class`.
fn start_writing_out_of(pool: &Arc<BufferPool>, class: SizeClass, frame: u32) {
    let hold = pool
        .latch_to_evict(class, frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .expect("the page is held by no one");
    let out = pool.start_write_out(hold);
    pool.write_out(class, vec![out]);
}

/// The `len` bytes at `pos` of `path`, read from `backend` itself and stepped to their end.
fn read_back(backend: &Arc<dyn IO>, path: &str, pos: u64, len: usize) -> Vec<u8> {
    let file = backend.open_file(path, OpenFlags::None, false).unwrap();
    let into = Arc::new(crate::io::Buffer::new_temporary(len));
    let c = file
        .pread(pos, Completion::new_read(into.clone(), |_| None))
        .unwrap();
    while !c.finished() {
        backend.step().unwrap();
    }
    into.as_slice().to_vec()
}

/// D2d §6.2, over `inner`: a write-out queued in a frame, which a backend writes half of at each
/// call. Asserted: the page's bytes whole in its file; the parts contiguous, each submitted by the
/// IO thread's own service of the record, never inside another part's, with the record held from
/// the first to the last; and the record and the count let go at the end (mutants
/// `short-forwarded`, the callback finishing the pool's request with a part's count: the page's
/// rest is not written; `continue-from-observer` inline, the part's observer serving the rest
/// itself: the next part is submitted inside its service).
fn a_short_write_out_is_continued_in_its_record(inner: Arc<dyn IO>, name: &str) {
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    let base = &format!("/db/frame-halving-{name}");
    let (halving, counted, pool) = halving_write_out(inner.clone(), base, None);
    let io: Arc<dyn IO> = counted.clone();
    let settled = settle_in_frames(&pool, &io);
    let failure = pool.failed().err();
    let requests = counted.requests().clone();
    // Read with the IO thread held before its idle, owing no step and finding no bit: the last
    // part's branch has run, and its retirement's observer with it.
    let held = crate::storage::atoll::testing::io_thread_held_before_idle(&requests);
    let end = (
        held.is_some(),
        requests.frames_at_rest(),
        requests.outstanding(),
    );
    drop(held);
    let parts = halving.parts();
    let page = halving.first_asked().unwrap_or_default();
    let written = parts
        .first()
        .map(|p| read_back(&inner, &p.path, p.pos, page.len()));
    drop((pool, io, counted, requests));
    assert_eq!(
        (
            settled.is_ok(),
            failure.is_none(),
            written.as_ref() == Some(&page)
        ),
        (true, true, true),
        "(the write-out settled, the pool unfailed, the page's bytes whole in its file): a short \
         part's rest not written ({settled:?}, {failure:?}, {} parts)",
        parts.len()
    );
    let first = parts[0].pos;
    let mut at = first;
    for p in &parts {
        assert_eq!(p.pos, at, "the parts are not contiguous: {parts:?}");
        at += p.wrote as u64;
    }
    assert_eq!(at - first, page.len() as u64, "the parts' bytes: {parts:?}");
    assert_eq!(
        parts.iter().map(|p| p.wrote).collect::<Vec<_>>(),
        halved_parts(halving_class().size()),
        "the page halved to its end: {parts:?}"
    );
    assert!(
        parts
            .iter()
            .all(|p| (p.thread.as_str(), p.services) == ("atoll-io", 1)),
        "(the thread, the frame services under way) of each part: a part submitted off the IO \
         thread's own service of the record: {parts:?}"
    );
    assert!(
        parts.iter().all(|p| p.at_rest == records - 1),
        "the record not held from the first part to the last: {parts:?}"
    );
    assert_eq!(
        end,
        (true, records, 0),
        "(the IO thread held before its idle, frame records at rest, requests counted) at the end"
    );
}

#[test]
fn over_an_inline_backend_a_short_write_out_is_continued_in_its_record() {
    a_short_write_out_is_continued_in_its_record(Arc::new(MemoryIO::new()), "inline");
}

#[test]
fn over_a_pumped_backend_a_short_write_out_is_continued_in_its_record() {
    a_short_write_out_is_continued_in_its_record(
        Arc::new(crate::io::crash::CrashIO::new(31)),
        "pumped",
    );
}

/// The first part of a write-out over a halving CrashIO submitted by the IO thread, the thread
/// then paused before its step, and that part delivered by the test thread's own step, so its
/// callback and its retirement run there; `between` runs next, before the thread is let go. What
/// the IO thread's next idle finds: the parts, the frame records at rest, the requests counted,
/// whether the thread reached its pause, and whether the idle came.
fn first_part_delivered_by_the_test(
    base: &str,
    between: impl FnOnce(),
) -> (Vec<Part>, usize, usize, bool, bool) {
    let crash = Arc::new(crate::io::crash::CrashIO::new(32));
    let (halving, counted, pool) = halving_write_out(crash.clone(), base, None);
    let requests = counted.requests().clone();
    let (there, release) = requests.pause_io_before_step();
    let _ = {
        let _frame = Frame::enter();
        pool.settle_writes().map(|r| matches!(r, IOResult::IO(_)))
    };
    let reached = there.recv_timeout(SETTLE).is_ok();
    crash.step().unwrap();
    between();
    let idle = requests.on_io_idle();
    drop(release);
    let idled = idle.recv_timeout(SETTLE).is_ok();
    let seen = (
        halving.parts(),
        requests.frames_at_rest(),
        requests.outstanding(),
        reached,
        idled,
    );
    drop((pool, counted, requests));
    seen
}

/// D2d §6.2, the part-thread oracle over a pumped backend: the first part delivered on the test
/// thread, its callback and observer run there; every part is still submitted by the IO thread's
/// own service, and the pool's request, unfinished once that short part has retired, finishes at
/// the whole with no error (mutant `continue-from-observer`, the observer serving the rest itself:
/// the second part is submitted on the test thread).
#[test]
fn a_short_parts_rest_is_submitted_by_the_io_thread_whoever_delivered_the_part() {
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    let request = std::cell::RefCell::new(None);
    let after_first = std::cell::Cell::new(None);
    let (parts, at_rest, counted, reached, idled) =
        first_part_delivered_by_the_test("/db/frame-halving-thread", || {
            let c = last_frame_request();
            after_first.set(c.as_ref().map(Completion::finished));
            *request.borrow_mut() = c;
        });
    let at_end = request
        .borrow()
        .as_ref()
        .map(|c| (c.finished(), c.get_error()));
    assert!(reached && idled, "(paused before its step, idle again)");
    assert_eq!(
        (after_first.get(), at_end),
        (Some(false), Some((true, None))),
        "(the pool's request finished once its first, short part retired; finished, and its \
         error, at the end): a short part finished the request"
    );
    assert!(
        parts
            .iter()
            .all(|p| (p.thread.as_str(), p.services) == ("atoll-io", 1)),
        "(the thread, the frame services under way) of each part: a part submitted off the IO \
         thread's own service of the record: {parts:?}"
    );
    assert_eq!(
        parts.iter().map(|p| p.wrote).collect::<Vec<_>>(),
        halved_parts(halving_class().size()),
        "the page halved to its end: {parts:?}"
    );
    assert_eq!(
        (at_rest, counted),
        (records, 0),
        "(frame records at rest, requests counted) at the end"
    );
}

/// D2d §6.2: a write-out whose pool request is cancelled once its first part has retired short,
/// the record published again for the rest: the IO thread's service of it submits nothing more,
/// and the record and the count come back (mutant `rest-after-cancel`: the rest is submitted all
/// the same).
#[test]
fn a_write_out_cancelled_between_its_parts_submits_no_more() {
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    let cancelled = std::cell::Cell::new(false);
    let (parts, at_rest, counted, reached, idled) =
        first_part_delivered_by_the_test("/db/frame-halving-cancel", || {
            if let Some(c) = last_frame_request() {
                c.abort();
                cancelled.set(c.finished());
            }
        });
    assert!(reached && idled, "(paused before its step, idle again)");
    assert!(cancelled.get(), "the write-out's request was not cancelled");
    assert_eq!(
        parts.len(),
        1,
        "the parts of a write-out cancelled after its first: a part submitted after its request \
         was cancelled: {parts:?}"
    );
    assert_eq!(
        (at_rest, counted),
        (records, 0),
        "(frame records at rest, requests counted) at the end"
    );
}

/// D2d §6.2, over an inline backend whose second part writes nothing: the write-out ends with a
/// short write's error, finished once, and its record and count come back.
#[test]
fn a_part_that_writes_nothing_ends_the_write_out_with_a_short_writes_error() {
    let records = MIN_BUDGET / MIN_PAGE_SIZE;
    let (halving, counted, pool) =
        halving_write_out(Arc::new(MemoryIO::new()), "/db/frame-halving-zero", Some(1));
    let io: Arc<dyn IO> = counted.clone();
    let settled = settle_in_frames(&pool, &io).is_ok();
    let failure = pool.failed().err().map(|e| format!("{e:?}"));
    let requests = counted.requests().clone();
    // As above: read once the IO thread is held before its idle.
    let held = crate::storage::atoll::testing::io_thread_held_before_idle(&requests);
    let end = (
        held.is_some(),
        requests.frames_at_rest(),
        requests.outstanding(),
    );
    drop(held);
    let parts = halving.parts();
    drop((pool, io, counted, requests));
    assert!(settled, "the write-out's settlement");
    assert!(
        failure.as_deref().is_some_and(|e| e.contains("ShortWrite")),
        "the pool's failure: {failure:?}"
    );
    assert_eq!(
        parts.iter().map(|p| p.wrote).collect::<Vec<_>>(),
        [halved(halving_class().size()), 0],
        "the parts: {parts:?}"
    );
    assert_eq!(
        end,
        (true, records, 0),
        "(the IO thread held before its idle, frame records at rest, requests counted) at the end"
    );
}

// ASYNC1 D2d (iv): over the ring, a frame's request whose file is open goes in from the polling
// thread when the ring's lock is free (the ring offer), and takes the hop to the IO thread
// otherwise; a request whose file is cold is never offered.

/// `n` pages of class 0 written by a first, synchronous pool over a ring at a fresh directory, and
/// a pool opened async over the same ring with the pages' page-table leaves resident: the
/// directory, the ring, the pages, the leaves, the counted IO and the pool.
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
#[allow(clippy::type_complexity)]
fn ring_pages(
    n: usize,
) -> (
    tempfile::TempDir,
    Arc<crate::io::UringIO>,
    Vec<PageId>,
    Vec<Option<crate::storage::atoll::meta::MetaRef>>,
    Arc<Counted>,
    Arc<BufferPool>,
) {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("db").to_str().unwrap().to_string();
    let ring = Arc::new(crate::io::UringIO::new().unwrap());
    let (pids, counted, pool) = pages_in_a_cold_file(ring.clone(), &base, true, n);
    let io: Arc<dyn IO> = counted.clone();
    let leaves = pids
        .iter()
        .map(|&pid| drive(&io, || pool.store.leaf(pid)).unwrap())
        .collect();
    (dir, ring, pids, leaves, counted, pool)
}

/// D2d §6.3, over the ring: a frame's load whose data file the first load opened, made with the
/// ring's lock free, goes in from the polling thread: the ring takes it at its offer and the IO
/// thread serves no record for it; the page reads back (mutant `hop-always`: nothing is offered,
/// and the IO thread serves it).
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
#[test]
fn over_the_ring_a_warm_frame_load_goes_in_from_the_polling_thread() {
    let (_dir, _ring, pids, leaves, counted, pool) = ring_pages(2);
    let io: Arc<dyn IO> = counted.clone();
    let requests = counted.requests().clone();
    let first =
        in_frames(&io, || pool.fix_exclusive(pids[0], None)).map(|g| stamped(g.page(), pids[0]));
    let before = requests.frame_roads();
    let second =
        in_frames(&io, || pool.fix_exclusive(pids[1], None)).map(|g| stamped(g.page(), pids[1]));
    let after = requests.frame_roads();
    drop((leaves, pool, io, counted, requests));
    assert_eq!(
        (first.ok(), second.ok()),
        (Some(Some(1)), Some(Some(1))),
        "the pages read back"
    );
    assert_eq!(
        (after.0 - before.0, after.1 - before.1),
        (1, 0),
        "(offered, served by the IO thread) for a warm frame load over a free ring: the request \
         took the hop"
    );
}

/// D2d §6.3, over the ring: the same load made while the ring's lock is held: the offer is
/// declined and the request takes the hop, served by the IO thread once the lock is let go; the
/// page reads back.
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
#[test]
fn over_the_ring_a_warm_frame_load_takes_the_hop_while_the_rings_lock_is_held() {
    let (_dir, ring, pids, leaves, counted, pool) = ring_pages(2);
    let io: Arc<dyn IO> = counted.clone();
    let requests = counted.requests().clone();
    let first =
        in_frames(&io, || pool.fix_exclusive(pids[0], None)).map(|g| stamped(g.page(), pids[0]));
    let before = requests.frame_roads();
    let polled = ring.with_lock_held(|| {
        let _frame = Frame::enter();
        pool.fix_exclusive(pids[1], None)
            .map(|r| matches!(r, IOResult::IO(_)))
    });
    let offered_while_held = requests.frame_roads().0 - before.0;
    let second =
        in_frames(&io, || pool.fix_exclusive(pids[1], None)).map(|g| stamped(g.page(), pids[1]));
    let after = requests.frame_roads();
    drop((leaves, pool, io, counted, requests));
    assert_eq!(polled.ok(), Some(true), "the frame's miss returned no wait");
    assert_eq!(
        (first.ok(), second.ok()),
        (Some(Some(1)), Some(Some(1))),
        "the pages read back"
    );
    assert_eq!(
        (offered_while_held, after.0 - before.0, after.1 - before.1),
        (0, 0, 1),
        "(offered while the lock was held, offered, served by the IO thread): a held ring took the \
         request at its offer"
    );
}

/// D2d §6.3, over the ring (child): a frame's load whose data file is still cold is not offered:
/// the IO thread opens the file and serves the load; the page reads back (mutant `cold-opens`: the
/// offer looks the file up as the IO thread's first stage does, opening it in the frame, and the
/// frame census ends the child).
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
#[test]
fn over_the_ring_a_cold_frame_load_is_not_offered() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::over_the_ring_a_cold_frame_load_is_not_offered"
        ),
        || {
            let (_dir, _ring, pids, leaves, counted, pool) = ring_pages(1);
            let io: Arc<dyn IO> = counted.clone();
            let requests = counted.requests().clone();
            let before = requests.frame_roads();
            let read = in_frames(&io, || pool.fix_exclusive(pids[0], None))
                .map(|g| stamped(g.page(), pids[0]));
            let after = requests.frame_roads();
            drop((leaves, pool, io, counted, requests));
            assert_eq!(read.ok(), Some(Some(1)), "the page read back");
            assert_eq!(
                (after.0 - before.0, after.1 - before.1),
                (0, 1),
                "(offered, served by the IO thread) for a cold frame load: a cold file offered"
            );
        },
    );
    child_ended_cleanly(ended);
}

/// ASYNC1 D2e §2.2b over a backend that still owns a load's request after its callback (#5206,
/// #5211): a loader A and a joiner B enter frames of a database opened async while its IO thread
/// is held, so both find the load queued; released, the IO thread makes the read, its callback at
/// once and its retirement held, and the test goes on only once the IO thread has run the branch
/// after its backend call, which registers the request's retirement. Landed or failed, the frame
/// is then kept both by its record and by its load's group, and either order of their two releases
/// lets it go exactly once: the frame evictable, or the failed load's frame freed, only after the
/// second.
#[cfg(all(unix, not(miri)))]
#[test]
fn a_loads_group_and_its_held_request_let_the_frame_go_in_either_order() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_loads_group_and_its_held_request_let_the_frame_go_in_either_order"
        ),
        || {
            for (fails, group_first) in [(false, true), (false, false), (true, true), (true, false)]
            {
                let probe = Probe::new();
                let base = format!("/db/group-held-{fails}-{group_first}");
                let (pid, counted, pool) = page_in_a_cold_file(&probe, &base, true);
                let io: Arc<dyn IO> = counted.clone();
                // The page-table leaf resident, so a frame's first miss is the data page's.
                let leaf = drive(&io, || pool.store.leaf(pid)).unwrap();
                probe.rule_under(
                    &data(&base),
                    Some(if fails { Rule::HoldFail } else { Rule::Hold }),
                );
                let requests = counted.requests().clone();
                let held_thread = io_thread_held_before_idle_unregistered(&requests);
                let reached = held_thread.is_some();
                let fix = |owner: &mut Owner| {
                    let _frame = Frame::enter();
                    owner
                        .call(|| {
                            Ok(match pool.fix_optimistic(pid, None)? {
                                IOResult::Done(_) => IOResult::Done(()),
                                IOResult::IO(io) => IOResult::IO(io),
                            })
                        })
                        .map(|r| matches!(r, IOResult::IO(_)))
                };
                let (mut a, mut b) = (Owner::new(), Owner::new());
                let waited = (fix(&mut a).ok(), fix(&mut b).ok());
                let frame = pool
                    .table
                    .find(&pool.frames.headers, pid.raw())
                    .expect("the loading page's frame");
                let kept = (a.kept_frame(), b.kept_frame());
                let ((), returned, served) = past_the_branch(&requests, || drop(held_thread));
                let ended_load = settle(|| {
                    probe.holding() == 1
                        && pool.header(frame).status() == if fails { FREE } else { RESIDENT }
                });
                let held = pool.header(frame).record_held();
                let free_times = |pool: &BufferPool| {
                    pool.memory
                        .free_frames(class0())
                        .into_iter()
                        .filter(|&f| f == frame)
                        .count()
                };
                let group_release = |a: Owner, b: Owner| drop((a, b));
                let first = if group_first {
                    group_release(a, b);
                    (pool.header(frame).record_keeps(), free_times(&pool))
                } else {
                    probe.let_go();
                    let seen = (pool.header(frame).record_keeps(), free_times(&pool));
                    group_release(a, b);
                    seen
                };
                if group_first {
                    probe.let_go();
                }
                let last = (pool.header(frame).record_keeps(), free_times(&pool));
                let evicted = !fails && pool.evict_clean(pid);
                drop((leaf, pool, io, counted, requests));
                let case = format!("failed {fails}, group first {group_first}");
                assert!(
                    reached,
                    "{case}: the IO thread never reached its pause before idle"
                );
                assert!(
                    returned && served,
                    "{case}: the IO thread's branch after its backend call, (returned, served)"
                );
                assert_eq!(waited, (Some(true), Some(true)), "{case}: (A, B) waited");
                assert_eq!(kept, (Some(frame), Some(frame)), "{case}: (A, B) kept");
                assert!(
                    ended_load,
                    "{case}: the load never ended at its held request"
                );
                assert!(
                    held,
                    "{case}: the request's record let go before its retirement"
                );
                assert_eq!(
                    first,
                    (true, 0),
                    "{case}: after the first release, (kept, frees)"
                );
                assert_eq!(
                    last,
                    (false, usize::from(fails)),
                    "{case}: after the second release, (kept, frees)"
                );
                assert_eq!(
                    evicted, !fails,
                    "{case}: the landed page evicted at the end"
                );
            }
        },
    );
    child_ended_cleanly(ended);
}
