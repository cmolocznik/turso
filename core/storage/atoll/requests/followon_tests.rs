//! A request's follow-on, made at its retirement (DESIGN.md 22.18; ASYNC1 D1a §6): the metadata
//! cache's and the superblock's syncs, each made into its write's record at the write's
//! retirement, counted before the write is uncounted, and submitted by the IO thread. Every
//! backend call is recorded at the backend's own method, with its thread and its depth there.

use super::dispatch::{FAIL_SPAWN, META_WRITES};
use super::*;
use crate::io::clock::{Clock, MonotonicInstant, WallClockInstant};
use crate::io::crash::CrashIO;
use crate::storage::atoll::aligned::AlignedBlock;
use crate::storage::atoll::control::FAIL_BEFORE_WAIT;
use crate::storage::atoll::control::{Control, Superblock};
use crate::storage::atoll::db::Opening;
use crate::storage::atoll::epoch::Epochs;
use crate::storage::atoll::meta::{MetaCache, MetaId, MetaRef, Tree, LAST_WRITE};
use crate::storage::atoll::page::{SizeClass, CLASS_COUNT};
use crate::storage::atoll::testing::drive;
#[cfg(all(unix, not(miri)))]
use crate::storage::atoll::testing::in_child;
use crate::storage::atoll::tests::{config, ending_inside_a_block, BASE};
use crate::sync::atomic::AtomicUsize;
use crate::sync::Weak;
use std::sync::mpsc;

const BOUND: std::time::Duration = std::time::Duration::from_secs(15);

/// What a probe's file does: its calls as asked, or one of them refused or failed.
const NORMAL: usize = 0;
const SYNC_REFUSED: usize = 1;
const WRITE_REFUSED: usize = 2;
const WRITE_SHORT: usize = 3;
const WRITE_FAILS: usize = 4;

/// A backend call: which, on which thread (by name), at what depth of backend calls.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Call {
    what: &'static str,
    thread: String,
    depth: usize,
}

crate::thread::thread_local! {
    static DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// MemoryIO, which delivers inline, with files that do or do not write durably, a mode, and a
/// record of every write and sync made at its files' own methods.
struct ProbeIO {
    inner: Arc<MemoryIO>,
    durable: bool,
    mode: Arc<AtomicUsize>,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl ProbeIO {
    fn new(durable: bool) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryIO::new()),
            durable,
            mode: Arc::new(AtomicUsize::new(NORMAL)),
            calls: Arc::default(),
        })
    }

    fn set(&self, mode: usize) {
        self.mode.store(mode, Ordering::SeqCst);
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().clone()
    }

    fn clear(&self) {
        self.calls.lock().clear();
    }
}

impl Clock for ProbeIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for ProbeIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        Ok(Arc::new(ProbeFile {
            file: self.inner.open_file(path, flags, direct)?,
            durable: self.durable,
            mode: self.mode.clone(),
            calls: self.calls.clone(),
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

struct ProbeFile {
    file: Arc<dyn File>,
    durable: bool,
    mode: Arc<AtomicUsize>,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl ProbeFile {
    /// Runs `f`, the backend's own call, recorded with its thread and its depth among backend
    /// calls on that thread.
    fn call<R>(&self, what: &'static str, f: impl FnOnce() -> R) -> R {
        let depth = DEPTH.with(|d| {
            d.set(d.get() + 1);
            d.get()
        });
        let thread = crate::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_string();
        self.calls.lock().push(Call {
            what,
            thread,
            depth,
        });
        let r = f();
        DEPTH.with(|d| d.set(d.get() - 1));
        r
    }
}

impl ProbeFile {
    /// A write the mode makes fail, refused or delivered short or with an error; `None` for one
    /// made as asked.
    fn fault(
        &self,
        what: &'static str,
        buffer: &Buffer,
        c: &Completion,
    ) -> Option<Result<Completion>> {
        match self.mode.load(Ordering::SeqCst) {
            WRITE_REFUSED => Some(Err(injected("a probe's write refused"))),
            WRITE_SHORT => Some(self.call(what, || {
                c.complete(buffer.len() as i32 - 512);
                Ok(c.clone())
            })),
            WRITE_FAILS => Some(self.call(what, || {
                c.error(CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "a probe's write failed",
                ));
                Ok(c.clone())
            })),
            _ => None,
        }
    }
}

fn injected(at: &'static str) -> LimboError {
    LimboError::CompletionError(CompletionError::IOError(std::io::ErrorKind::Other, at))
}

impl File for ProbeFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }
    fn pread(&self, at: u64, c: Completion) -> Result<Completion> {
        self.file.pread(at, c)
    }
    fn pwrite(&self, at: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.fault("pwrite", &buffer, &c)
            .unwrap_or_else(|| self.call("pwrite", || self.file.pwrite(at, buffer, c)))
    }
    fn pwrite_durable(&self, at: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.fault("pwrite_durable", &buffer, &c)
            .unwrap_or_else(|| {
                self.call("pwrite_durable", || self.file.pwrite_durable(at, buffer, c))
            })
    }
    fn supports_durable_writes(&self) -> bool {
        self.durable
    }
    fn sync(&self, c: Completion, how: FileSyncType) -> Result<Completion> {
        if self.mode.load(Ordering::SeqCst) == SYNC_REFUSED {
            return Err(injected("a probe's sync refused"));
        }
        self.call("sync", || self.file.sync(c, how))
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

fn cache(counted: &Arc<Counted>, base: &str) -> Arc<MetaCache> {
    Arc::new(MetaCache::new(
        counted.clone(),
        base,
        16,
        Arc::new(Epochs::new()),
        0,
    ))
}

/// Page 0 of `cache` loaded, changed and cut for checkpoint 1: ready to be written for it.
fn dirty_page(cache: &Arc<MetaCache>, io: &Arc<dyn IO>) -> MetaRef {
    let frame = drive(io, || cache.get(id(0))).unwrap();
    frame
        .try_lock()
        .expect("the loaded page is free")
        .commit(cache);
    drop(cache.begin_cut(1));
    frame
}

fn superblock(seq: u64) -> Superblock {
    Superblock {
        seq,
        redo: 0,
        segment_bytes: 1 << 20,
        file_bytes: 1 << 30,
        database_id: [3; 16],
        heights: [[1; CLASS_COUNT]; crate::storage::atoll::meta::TREES],
        clock: 0,
        snapshot: 0,
        checkpoint: 0,
        continuations: Vec::new(),
    }
}

/// The calls made on a thread named `thread`.
fn on(calls: &[Call], thread: &str) -> Vec<(&'static str, usize)> {
    calls
        .iter()
        .filter(|c| c.thread == thread)
        .map(|c| (c.what, c.depth))
        .collect()
}

fn this_thread() -> String {
    crate::thread::current()
        .name()
        .unwrap_or("unnamed")
        .to_string()
}

/// A metadata write on a file that does not write durably, on a backend that delivers inline: the
/// caller makes one backend call, the write, at depth 1, and the sync, made at the write's
/// retirement, is run by the IO thread; the write's wait ends once both have retired, the copy then
/// the one that counts (D1a §6.1; mutant: the observer submits the sync itself, on the caller).
#[test]
fn a_metadata_writes_sync_is_made_at_its_retirement_and_run_by_the_io_thread() {
    let probe = ProbeIO::new(false);
    let alone = Standalone::new(probe.clone());
    let cache = cache(&alone.counted, "/db/followon-meta");
    let frame = dirty_page(&cache, &alone.io);
    probe.clear();
    let wait = cache
        .write_for(&frame, 1)
        .unwrap()
        .expect("a write started");
    let waited = crate::storage::atoll::park::wait_for_completion(&*alone.io, wait.clone());
    let calls = probe.calls();
    let written_for = frame.last_written_for();
    cache.end_cut(1, std::slice::from_ref(&frame));
    let me = this_thread();
    drop((wait, frame, cache));
    drop(alone);
    assert!(waited.is_ok(), "the write's wait: {waited:?}");
    assert_eq!(
        on(&calls, &me),
        vec![("pwrite", 1)],
        "the caller's backend calls: {calls:?}"
    );
    assert_eq!(
        on(&calls, "atoll-io"),
        vec![("sync", 1)],
        "the IO thread's backend calls"
    );
    assert_eq!(written_for, 1, "the copy written for checkpoint 1");
}

/// The same for the superblock: the caller writes it, the IO thread syncs it, its durable wait is
/// told `Ok` at the chain's physical end, and the next superblock finds the one record at rest
/// (D1a §6.2; mutant: the observer submits the sync itself).
#[test]
fn a_superblocks_sync_is_run_by_the_io_thread_and_its_wait_told_at_the_chains_end() {
    let probe = ProbeIO::new(false);
    let alone = Standalone::new(probe.clone());
    let (control, _) = Control::open(alone.counted.clone(), "/db/followon-control").unwrap();
    probe.clear();
    let mut ends = Vec::new();
    for seq in 1..=2 {
        let durable = control.write(&superblock(seq)).unwrap();
        ends.push(crate::storage::atoll::park::wait_for_completion(&*alone.io, durable).is_ok());
    }
    let calls = probe.calls();
    let me = this_thread();
    drop(control);
    drop(alone);
    assert_eq!(ends, vec![true, true], "each superblock's durable wait");
    assert_eq!(
        on(&calls, &me),
        vec![("pwrite", 1), ("pwrite", 1)],
        "the caller's backend calls: {calls:?}"
    );
    assert_eq!(
        on(&calls, "atoll-io"),
        vec![("sync", 1), ("sync", 1)],
        "the IO thread's backend calls"
    );
}

/// A chain's state is told to its owner, and its end once.
#[derive(Default)]
struct Told {
    written: Mutex<Vec<std::result::Result<(), CompletionError>>>,
    ended: Mutex<Vec<std::result::Result<(), CompletionError>>>,
    durable: bool,
}

impl Chain for Told {
    fn written(&self, res: std::result::Result<(), CompletionError>) -> bool {
        let ok = res.is_ok();
        self.written.lock().push(res);
        ok && !self.durable
    }
    fn synced(&self, _: std::result::Result<(), CompletionError>) {}
    fn ended(&self, outcome: std::result::Result<(), CompletionError>) {
        self.ended.lock().push(outcome);
    }
}

fn page() -> Arc<Buffer> {
    Arc::new(AlignedBlock::zeroed(4096)).region(0, 4096)
}

/// Each count a chain takes is let go once (D1a §6.4, P1-V2-COUNT): a durable write's chain
/// counts 1 then 0, one that fails 1 then 0, and a write with its sync 1, then 2 while the sync is
/// counted and the write not yet let go, then 1, then 0. Run in a child: the defect, a chain's end
/// that lets a count go too, trips `Requests::retired`'s assert, which ends the process (mutant:
/// the end uncounts).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_chains_counts_are_each_let_go_once() {
    let ended = in_child(
        concat!(module_path!(), "::a_chains_counts_are_each_let_go_once"),
        || {
            for durable in [true, false] {
                // A durable write, then a write whose sync follows: on a stepped backend.
                let crash = Arc::new(CrashIO::new(0));
                crash.set_durable_writes(durable);
                let alone = Standalone::new(crash.clone());
                let file = alone
                    .counted
                    .open_counted("/db/counts", OpenFlags::Create, false)
                    .unwrap();
                let told = Arc::new(Told {
                    durable,
                    ..Told::default()
                });
                let held = alone.counted.take_meta_write_record().unwrap();
                drop(
                    file.write_then(held, 0, page(), durable, told.clone())
                        .unwrap(),
                );
                let mut counts = vec![alone.requests().outstanding()];
                if !durable {
                    let requests = alone.requests().clone();
                    let seen = Arc::new(AtomicUsize::new(0));
                    let at = seen.clone();
                    crate::race::at("requests.followon_published", move || {
                        at.store(requests.outstanding(), Ordering::SeqCst);
                    });
                    crash.step().unwrap();
                    counts.push(seen.load(Ordering::SeqCst));
                    counts.push(alone.requests().outstanding());
                }
                alone.requests().drain(&*alone.io);
                counts.push(alone.requests().outstanding());
                let ended = told.ended.lock().clone();
                drop(file);
                drop(alone);
                let want = if durable {
                    vec![1, 0]
                } else {
                    vec![1, 2, 1, 0]
                };
                assert_eq!(counts, want, "the counts, durable {durable}");
                assert_eq!(ended, vec![Ok(())], "the chain's end, durable {durable}");
            }
            // A write that fails, on a backend that delivers inline.
            let probe = ProbeIO::new(false);
            probe.set(WRITE_FAILS);
            let alone = Standalone::new(probe.clone());
            let file = alone
                .counted
                .open_counted("/db/counts-failed", OpenFlags::Create, false)
                .unwrap();
            let told = Arc::new(Told::default());
            let held = alone.counted.take_meta_write_record().unwrap();
            drop(
                file.write_then(held, 0, page(), false, told.clone())
                    .unwrap(),
            );
            let count = alone.requests().outstanding();
            let ended = told.ended.lock().clone();
            drop(file);
            drop(alone);
            assert_eq!(count, 0, "a failed write's count");
            assert_eq!(ended.len(), 1, "the failed chain's end");
            assert!(ended[0].is_err(), "the failed chain's outcome");
        },
    );
    if let Some(ended) = ended {
        assert!(
            !ended.aborted && ended.code == Some(0),
            "the child ended {:?} (aborted {}): {}",
            ended.code,
            ended.aborted,
            ended.stderr
        );
    }
}

/// A sync whose submission the backend refuses ends its chain with that error: the write's wait is
/// told it, every record is at rest again, and the count is back at zero (D1a §6.5; mutant: a
/// refused follow-on leaves its record held).
#[test]
fn a_refused_sync_ends_its_chain_and_its_record_rests() {
    let probe = ProbeIO::new(false);
    let counted = counted_for_test(probe.clone());
    let io: Arc<dyn IO> = counted.clone();
    let requests = counted.requests().clone();
    let cache = cache(&counted, "/db/followon-refused-sync");
    let frame = dirty_page(&cache, &io);
    probe.set(SYNC_REFUSED);
    let wait = cache
        .write_for(&frame, 1)
        .unwrap()
        .expect("a write started");
    // Every count retired: the IO thread has served the sync, refused.
    requests.drain(&*io);
    let after = (
        wait.finished(),
        wait.get_error().is_some(),
        requests.records_at_rest(),
        requests.outstanding(),
        frame.last_written_for(),
    );
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop((wait, frame, cache, io, counted, requests));
    assert_eq!(
        after,
        (true, true, 33, 0, 0),
        "(the wait told, with an error, records at rest, counted, written for)"
    );
}

/// A write the backend refuses ends its chain before any callback can run: the chain is told the
/// refusal's error, its write never, its record is at rest and nothing is counted; through the
/// metadata cache the error is returned, the page's wait taken off it, and the next write starts
/// afresh (D1a §6.6, #4721; mutants: no cleanup of a refused first request; the refusal's outcome
/// left `Ok`).
#[test]
fn a_refused_write_ends_its_chain_with_its_error_and_its_record_rests() {
    let probe = ProbeIO::new(true);
    let alone = Standalone::new(probe.clone());
    let file = alone
        .counted
        .open_counted("/db/followon-refused-chain", OpenFlags::Create, false)
        .unwrap();
    let told = Arc::new(Told {
        durable: true,
        ..Told::default()
    });
    probe.set(WRITE_REFUSED);
    let held = alone.counted.take_meta_write_record().unwrap();
    let returned = file
        .write_then(held, 0, page(), true, told.clone())
        .is_err();
    let chain = (
        returned,
        told.written.lock().len(),
        told.ended.lock().clone(),
        alone.requests().records_at_rest(),
        alone.requests().outstanding(),
    );
    probe.set(NORMAL);
    let cache = cache(&alone.counted, "/db/followon-refused-write");
    let frame = dirty_page(&cache, &alone.io);
    probe.clear();
    probe.set(WRITE_REFUSED);
    let refused = cache.write_for(&frame, 1).err().map(|e| e.to_string());
    let at_rest = alone.requests().records_at_rest();
    let counted = alone.requests().outstanding();
    probe.set(NORMAL);
    let next = cache.write_for(&frame, 1).unwrap();
    let next_done = next.as_ref().is_none_or(Completion::finished);
    let calls = probe.calls();
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop((next, frame, cache, file));
    drop(alone);
    assert_eq!(
        chain,
        (
            true,
            0,
            vec![Err(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "a probe's write refused"
            ))],
            33,
            0
        ),
        "(the error returned, writes told, the chain's end, records at rest, counted)"
    );
    assert!(
        refused.as_deref().is_some_and(|e| e.contains("refused")),
        "the refusal returned: {refused:?}"
    );
    assert_eq!(
        (at_rest, counted),
        (33, 0),
        "(records at rest, counted) after the refusal"
    );
    assert!(next_done, "the next write started afresh and ended");
    assert_eq!(
        calls.iter().map(|c| c.what).collect::<Vec<_>>(),
        vec!["pwrite_durable"],
        "the backend calls: the refused write made none"
    );
}

/// A close started as a write retires, before its sync is counted, waits across the hand-off: the
/// write is still counted when the closer looks, so it parks, and it ends only once the sync has
/// retired, the write's wait told `Ok` (D1a §6.7; mutant: the write let go before its sync is
/// counted, so the closer seals in the gap and the sync is refused).
#[test]
fn a_close_waits_across_a_writes_hand_off_to_its_sync() {
    let probe = ProbeIO::new(false);
    let counted = counted_for_test(probe.clone());
    let io: Arc<dyn IO> = counted.clone();
    let requests = counted.requests().clone();
    let cache = cache(&counted, "/db/followon-close");
    let frame = dirty_page(&cache, &io);
    let looked = Arc::new(Mutex::new(None));
    let closer: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::default();
    {
        let (requests, io, looked, closer) =
            (requests.clone(), io.clone(), looked.clone(), closer.clone());
        crate::race::at("requests.followon_counting", move || {
            let (tx, rx) = mpsc::channel();
            let handle = std::thread::spawn(move || {
                let seen = requests.clone();
                crate::race::at("requests.drain_looked", move || {
                    let _ = tx.send(seen.outstanding());
                });
                requests.drain(&*io);
            });
            *looked.lock() = rx.recv_timeout(BOUND).ok();
            *closer.lock() = Some(handle);
        });
    }
    let wait = cache
        .write_for(&frame, 1)
        .unwrap()
        .expect("a write started");
    let waited = crate::storage::atoll::park::wait_for_completion(&*io, wait.clone());
    let joined = closer.lock().take().map(|h| h.join().is_ok());
    let seen = *looked.lock();
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop((wait, frame, cache, io, counted, requests));
    assert_eq!(
        seen,
        Some(1),
        "the requests counted as the closer looked in the hand-off"
    );
    assert_eq!(joined, Some(true), "the closer ended");
    assert!(waited.is_ok(), "the write's wait: {waited:?}");
}

/// A write after close has sealed the requests is refused: its chain is told the refusal, its
/// write never, and its record is at rest; through the metadata cache the refusal is returned
/// (D1a §6.8, #4721).
#[test]
fn a_write_after_close_sealed_is_refused_and_its_record_rests() {
    let probe = ProbeIO::new(false);
    let counted = counted_for_test(probe.clone());
    let io: Arc<dyn IO> = counted.clone();
    let requests = counted.requests().clone();
    let cache = cache(&counted, "/db/followon-sealed");
    let frame = dirty_page(&cache, &io);
    let file = counted
        .open_counted("/db/followon-sealed-chain", OpenFlags::Create, false)
        .unwrap();
    requests.drain(&*io);
    let told = Arc::new(Told::default());
    let held = counted.take_meta_write_record().unwrap();
    let returned = file
        .write_then(held, 0, page(), false, told.clone())
        .is_err();
    let chain = (
        returned,
        told.written.lock().len(),
        told.ended.lock().clone(),
        requests.records_at_rest(),
    );
    let refused = cache.write_for(&frame, 1).err().map(|e| e.to_string());
    let at_rest = requests.records_at_rest();
    drop((frame, cache, file, io, counted, requests));
    assert_eq!(
        chain,
        (
            true,
            0,
            vec![Err(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "a request's submission refused"
            ))],
            33
        ),
        "(the error returned, writes told, the chain's end, records at rest)"
    );
    assert!(
        refused
            .as_deref()
            .is_some_and(|e| e.contains("closed database")),
        "the refusal: {refused:?}"
    );
    assert_eq!(at_rest, 33, "records at rest");
}

/// A record let go while its old chain's last holder is paused just after REST is taken at once by
/// the next owner; the old chain is told from what it took, and the new chain is untouched until its
/// own end (D1a §6.9; mutant: the end read from the record after REST).
#[test]
fn a_record_let_go_is_reused_and_its_old_chain_told_from_what_it_took() {
    let requests = Arc::new(Requests::default());
    let counted = Counted::new(Arc::new(MemoryIO::new()), requests.clone()).unwrap();
    let dispatch = requests.dispatch().clone();
    let (old, new) = (Arc::new(Told::default()), Arc::new(Told::default()));
    let held = dispatch.take(META_WRITES).unwrap();
    let first = held.index();
    dispatch.start(&held, old.clone(), true);
    let reused = Arc::new(Mutex::new(None));
    {
        let (dispatch, new, reused) = (dispatch.clone(), new.clone(), reused.clone());
        crate::race::at("requests.record_rest", move || {
            let held = dispatch.take(META_WRITES).unwrap();
            dispatch.start(&held, new.clone(), true);
            *reused.lock() = Some(held);
        });
    }
    dispatch.refused(held, CompletionError::Aborted);
    let reused = reused.lock().take();
    let seen = (
        old.ended.lock().clone(),
        new.ended.lock().clone(),
        reused.as_ref().map(Held::index),
        requests.records_at_rest(),
    );
    // Let the new chain go, if it is still the record's.
    let pending = new.ended.lock().is_empty();
    if let (Some(held), true) = (reused, pending) {
        dispatch.refused(held, CompletionError::Aborted);
    }
    drop((dispatch, counted, requests));
    assert_eq!(
        seen,
        (vec![Err(CompletionError::Aborted)], vec![], Some(first), 32),
        "(the old chain's end, the new one's, the record reused, records at rest)"
    );
}

/// A follow-on held READY while the IO thread is held is served once it is let go, its file kept by
/// its owner: the sync runs on the IO thread and the write's wait is told `Ok` (D1a §6.11 a').
#[test]
fn a_ready_follow_on_is_served_once_the_io_thread_is_let_go() {
    let probe = ProbeIO::new(false);
    let alone = Standalone::new(probe.clone());
    let cache = cache(&alone.counted, "/db/followon-held");
    let frame = dirty_page(&cache, &alone.io);
    probe.clear();
    let release = alone.requests().hold_io_thread();
    let wait = cache
        .write_for(&frame, 1)
        .unwrap()
        .expect("a write started");
    let before = (wait.finished(), alone.requests().outstanding());
    drop(release);
    let waited = crate::storage::atoll::park::wait_for_completion(&*alone.io, wait.clone());
    let calls = probe.calls();
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop((wait, frame, cache));
    drop(alone);
    assert_eq!(
        before,
        (false, 1),
        "(the wait told, counted) while the IO thread was held"
    );
    assert!(waited.is_ok(), "the write's wait: {waited:?}");
    assert_eq!(
        on(&calls, "atoll-io"),
        vec![("sync", 1)],
        "the IO thread's backend calls"
    );
}

/// A stop reaches an IO thread already parked: past its idle mark it looks at the word no more, so
/// only the stop's unpark ends it (#4724; mutant: a stop that does not unpark). The thread is made
/// here with both notes armed before it starts, and nothing unparks it before the stop, so no
/// earlier token can stand in for the stop's (#4734). A thread the stop did not reach is unparked
/// once the bound has passed and joined, before the assertion.
#[test]
fn a_stop_ends_an_io_thread_parked_idle() {
    let requests = Arc::new(Requests::default());
    let dispatch = Arc::new(super::Dispatch::new(&requests, Shape::default()));
    let idled = dispatch.on_idle();
    let exit = dispatch.on_exit();
    let thread = super::Dispatch::spawn(&dispatch, 0).unwrap();
    let idle = idled.recv_timeout(BOUND).is_ok();
    dispatch.stop();
    let exited = exit.recv_timeout(BOUND).is_ok();
    if !exited {
        dispatch.unpark(0);
    }
    let joined = thread.join().is_ok();
    drop((dispatch, requests));
    assert_eq!(
        (idle, exited, joined),
        (true, true, true),
        "(idle, ended by the stop, joined)"
    );
}

/// The dispatch and the requests this thread's last counted IO made, weakly.
fn made() -> (Weak<super::Dispatch>, Weak<Requests>) {
    MADE.with(|m| m.borrow().clone())
        .expect("a counted IO made on this thread")
}

/// A failed open with a sync READY whose file is gone (D1a §6.11 a): a reopening's recovery
/// writes a superblock to record its log's gap, the IO thread held before its claim from that write
/// on, and fails after the write, its sync READY. Both `work()` callers return the failure; a third
/// thread drops the opening, whose drain waits for the sync; a fourth lets the IO thread go once
/// that drain has looked. The sync's file went with recovery's control, so it ends `Aborted`, its
/// count let go, and the drain seals, stops the thread and joins it: nothing is left (mutant: the
/// stop and join skipped at both sites, drain's and the requests' drop; the test then stops the
/// thread itself).
#[test]
fn a_failed_open_with_a_ready_sync_whose_file_is_gone_leaves_nothing() {
    let probe = ProbeIO::new(false);
    let io: Arc<dyn IO> = probe.clone();
    ending_inside_a_block(io.clone());
    let opening = Opening::new(io.clone(), BASE, config(), None).unwrap();
    let (dispatch, requests) = made();
    let release: std::rc::Rc<std::cell::RefCell<Option<mpsc::Sender<()>>>> = Default::default();
    {
        let (dispatch, release) = (dispatch.clone(), release.clone());
        crate::race::at("control.write_and_wait", move || {
            if let Some(dispatch) = dispatch.upgrade() {
                *release.borrow_mut() = Some(dispatch.hold_next_claim());
            }
        });
    }
    FAIL_BEFORE_WAIT.with(|f| f.set(true));
    let first = opening.work().err().map(|e| e.to_string());
    let injected = !FAIL_BEFORE_WAIT.with(|f| f.replace(false));
    let release = release.borrow_mut().take();
    let held = release.is_some();
    let opening = Arc::new(opening);
    let second = {
        let opening = opening.clone();
        std::thread::spawn(move || opening.work().err().map(|e| e.to_string()))
            .join()
            .unwrap()
    };
    let opening = Arc::into_inner(opening).expect("the opening's one owner");
    let (looked_tx, looked_rx) = mpsc::channel();
    let dropper = std::thread::spawn(move || {
        crate::race::at("requests.drain_looked", move || {
            let _ = looked_tx.send(());
        });
        drop(opening);
    });
    let releaser = std::thread::spawn(move || {
        let looked = looked_rx.recv_timeout(BOUND).is_ok();
        drop(release);
        looked
    });
    let looked = releaser.join().unwrap();
    let dropped = dropper.join().is_ok();
    let left = dispatch.upgrade();
    let exited = left.as_ref().map(|d| d.exited.load(Ordering::Acquire));
    let observed = (left.is_some(), requests.upgrade().is_some());
    // A thread no stop reached, stopped here and its end waited for, before the assertion.
    if let Some(dispatch) = left {
        let exit = dispatch.on_exit();
        dispatch.stop();
        drop(dispatch);
        let _ = exit.recv_timeout(BOUND);
    }
    let left = observed;
    drop(io);
    assert_eq!(
        (injected, held),
        (true, true),
        "(the superblock's wait failed, the IO thread held at its write)"
    );
    assert!(
        first
            .as_deref()
            .is_some_and(|e| e.contains("failure injected")),
        "the first work(): {first:?}"
    );
    assert!(
        second
            .as_deref()
            .is_some_and(|e| e.contains("not tried again")),
        "the second work(): {second:?}"
    );
    assert_eq!(
        (looked, dropped),
        (true, true),
        "(the drop's drain looked, the drop ended)"
    );
    assert_eq!(
        (left, exited),
        ((false, false), None),
        "((a dispatch left, requests left), the thread's exit flag)"
    );
}

/// An IO thread that does not start fails the counted IO's making, before any file is opened
/// (D1a §6.11 b).
#[test]
fn an_io_thread_that_does_not_start_fails_the_open() {
    FAIL_SPAWN.with(|f| f.set(Some(0)));
    let refused = Opening::new(ProbeIO::new(false), BASE, config(), None)
        .err()
        .map(|e| e.to_string());
    let (dispatch, requests) = made();
    let left = (dispatch.upgrade().is_some(), requests.upgrade().is_some());
    assert!(
        refused
            .as_deref()
            .is_some_and(|e| e.contains("IO thread did not start")),
        "the error: {refused:?}"
    );
    assert_eq!(left, (false, false), "(a dispatch left, requests left)");
}

/// A chain whose sync callback panics.
struct Panics;
impl Chain for Panics {
    fn written(&self, res: std::result::Result<(), CompletionError>) -> bool {
        res.is_ok()
    }
    fn synced(&self, _: std::result::Result<(), CompletionError>) {
        panic!("a sync's callback failed");
    }
    fn ended(&self, _: std::result::Result<(), CompletionError>) {}
}

/// A chain whose sync callback closes the database, on the IO thread that runs it.
struct Closes {
    requests: Arc<Requests>,
    io: Arc<dyn IO>,
}
impl Chain for Closes {
    fn written(&self, res: std::result::Result<(), CompletionError>) -> bool {
        res.is_ok()
    }
    fn synced(&self, _: std::result::Result<(), CompletionError>) {
        self.requests.drain(&*self.io);
    }
    fn ended(&self, _: std::result::Result<(), CompletionError>) {}
}

/// A sync's callback that panics on the IO thread ends the process: its pass is an Atoll entry
/// (D1a §6.11 c).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_panic_in_a_callback_the_io_thread_runs_ends_the_process() {
    let ended = in_child(
        concat!(
            module_path!(),
            "::a_panic_in_a_callback_the_io_thread_runs_ends_the_process"
        ),
        || {
            let probe = ProbeIO::new(false);
            let counted = counted_for_test(probe.clone());
            let file = counted
                .open_counted("/db/panics", OpenFlags::Create, false)
                .unwrap();
            let held = counted.take_meta_write_record().unwrap();
            drop(
                file.write_then(held, 0, page(), false, Arc::new(Panics))
                    .unwrap(),
            );
            // The IO thread runs the sync, whose callback ends the process.
            loop {
                crate::thread::park();
            }
        },
    );
    if let Some(ended) = ended {
        assert!(
            ended.aborted && ended.stderr.contains("a sync's callback failed"),
            "the child ended {:?} (aborted {}): {}",
            ended.code,
            ended.aborted,
            ended.stderr
        );
    }
}

/// A close made on the IO thread, from a callback it runs, is refused, and the process ends: the
/// thread is a delivering one (D1a §6.11 d).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_close_on_the_io_thread_is_refused() {
    let ended = in_child(
        concat!(module_path!(), "::a_close_on_the_io_thread_is_refused"),
        || {
            let probe = ProbeIO::new(false);
            let counted = counted_for_test(probe.clone());
            let io: Arc<dyn IO> = counted.clone();
            let file = counted
                .open_counted("/db/closes", OpenFlags::Create, false)
                .unwrap();
            let held = counted.take_meta_write_record().unwrap();
            let chain = Arc::new(Closes {
                requests: counted.requests().clone(),
                io,
            });
            drop(file.write_then(held, 0, page(), false, chain).unwrap());
            loop {
                crate::thread::park();
            }
        },
    );
    if let Some(ended) = ended {
        assert!(
            ended.aborted
                && ended
                    .stderr
                    .contains("a database's close on a database's IO thread"),
            "the child ended {:?} (aborted {}): {}",
            ended.code,
            ended.aborted,
            ended.stderr
        );
    }
}

/// A write that lands short, and one whose backend delivers an error, each end their chain with an
/// error and make no sync: the page's copy written is not the one that counts (D1a §6.12 a, b).
#[test]
fn a_short_or_failed_metadata_write_ends_with_its_error_and_no_sync() {
    let mut seen = Vec::new();
    for mode in [WRITE_SHORT, WRITE_FAILS] {
        let probe = ProbeIO::new(false);
        let alone = Standalone::new(probe.clone());
        let weak = alone.requests().dispatch_weak();
        let cache = cache(&alone.counted, "/db/followon-short");
        let frame = dirty_page(&cache, &alone.io);
        probe.clear();
        probe.set(mode);
        let wait = cache
            .write_for(&frame, 1)
            .unwrap()
            .expect("a write started");
        let waited = crate::storage::atoll::park::wait_for_completion(&*alone.io, wait.clone());
        let calls: Vec<_> = probe.calls().iter().map(|c| c.what).collect();
        let written_for = frame.last_written_for();
        cache.end_cut(1, std::slice::from_ref(&frame));
        drop((wait, frame, cache));
        drop(alone);
        seen.push((
            waited.is_err(),
            calls,
            written_for,
            weak.upgrade().is_none(),
        ));
    }
    assert_eq!(
        seen,
        vec![
            (true, vec!["pwrite"], 0, true),
            (true, vec!["pwrite"], 0, true)
        ],
        "(the wait failed, the backend calls, written for, the dispatch gone) short, then failed"
    );
}

/// A metadata write cancelled while its backend still owns it ends its chain with `Aborted` only
/// once the backend lets it go, and makes no sync (D1a §6.12 c).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_cancelled_metadata_write_ends_only_once_its_backend_lets_it_go() {
    let crash = Arc::new(CrashIO::new(0));
    crash.set_durable_writes(false);
    let alone = Standalone::new(crash.clone());
    let cache = cache(&alone.counted, "/db/followon-cancel");
    let frame = dirty_page(&cache, &alone.io);
    let wait = cache
        .write_for(&frame, 1)
        .unwrap()
        .expect("a write started");
    let write = LAST_WRITE
        .with(|w| w.borrow_mut().take())
        .expect("the write's request");
    alone.io.cancel(std::slice::from_ref(&write)).unwrap();
    let before = (
        write.finished(),
        write.is_owned(),
        wait.finished(),
        alone.requests().outstanding(),
    );
    crash.step().unwrap();
    let after = (
        wait.finished(),
        wait.get_error(),
        alone.requests().outstanding(),
        crash.in_flight(),
    );
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop((write, wait, frame, cache));
    drop(alone);
    assert_eq!(
        before,
        (true, true, false, 1),
        "(cancelled, still owned, the wait told, counted) before the backend let it go"
    );
    assert_eq!(
        after,
        (true, Some(CompletionError::Aborted), 0, 0),
        "(the wait told, its error, counted, in flight) once it let it go"
    );
}

/// A metadata write on a file that writes durably makes no sync and no follow-on: its success
/// effects run at its own retirement, and the dispatch goes with its owners (D1a §6.12 d, e).
#[test]
fn a_durable_metadata_write_needs_no_sync() {
    let probe = ProbeIO::new(true);
    let alone = Standalone::new(probe.clone());
    let weak = alone.requests().dispatch_weak();
    let cache = cache(&alone.counted, "/db/followon-durable");
    let frame = dirty_page(&cache, &alone.io);
    probe.clear();
    let wait = cache
        .write_for(&frame, 1)
        .unwrap()
        .expect("a write started");
    let at_return = wait.finished();
    let calls: Vec<_> = probe.calls().iter().map(|c| c.what).collect();
    let written_for = frame.last_written_for();
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop((wait, frame, cache));
    drop(alone);
    assert!(
        at_return,
        "the durable write's wait told at its own retirement"
    );
    assert_eq!(calls, vec!["pwrite_durable"], "the backend calls");
    assert_eq!(written_for, 1, "written for checkpoint 1");
    assert!(
        weak.upgrade().is_none(),
        "the dispatch left after its owners"
    );
}
