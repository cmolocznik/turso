use super::*;
use crate::io::{Buffer, Clock, Delivery, FileSyncType, MemoryIO};
use crate::race;
use crate::{MonotonicInstant, WallClockInstant};
use std::sync::mpsc;
use std::task::{Wake, Waker};

#[test]
fn a_loading_frame_supplies_one_pending_wait_before_its_file_opens() {
    let paused = pause_load(0);
    let calls = Arc::new(Calls(AtomicUsize::new(0)));
    let waker = Waker::from(calls.clone());
    let mut ready = 0;
    let mut pending = 0;
    let mut last: Option<IOCompletions> = None;
    let mut same_wait = true;
    let rounds = if cfg!(miri) { 64 } else { 10_000 };
    for _ in 0..rounds {
        match paused.cache.get(id(0)).unwrap() {
            IOResult::IO(wait) => {
                if let Some(previous) = &last {
                    same_wait &= previous.0.ptr_eq(&wait.0);
                } else {
                    // will_wake may give false negatives; register once and check identity.
                    wait.set_waker(Some(&waker));
                }
                if wait.finished() {
                    ready += 1;
                } else {
                    pending += 1;
                }
                last = Some(wait);
            }
            IOResult::Done(_) => panic!("the loader is paused before opening its file"),
        }
    }
    let before = calls.0.load(Ordering::Relaxed);
    let opens_before = paused.io.opens.load(Ordering::Relaxed);
    paused.resume.send(()).unwrap();
    let owner = paused.owner.join().unwrap();
    let finished = last.as_ref().is_some_and(|c| c.finished());
    let after = calls.0.load(Ordering::Relaxed);
    // Complete every actor and release the cache before a negative-build assertion fails.
    if let Some(wait) = &last {
        if !wait.finished() {
            wait.0.complete(0);
        }
    }
    drop(last);
    drop(owner);
    drop(paused.cache);
    println!("metadata unchanged load: ready={ready}, pending={pending}, wakes_before={before}, wakes_after={after}");
    assert_eq!(opens_before, 0);
    assert!(same_wait, "all readers of this load share one completion");
    assert_eq!(ready, 0);
    assert_eq!(pending, rounds);
    assert_eq!(before, 0);
    assert!(finished);
    assert_eq!(after, 1);
}

#[test]
fn a_finish_before_registration_returns_the_page_and_bounds_the_unused_wait() {
    let Paused {
        cache,
        io: _,
        resume,
        owner,
    } = pause_load(0);
    race::at("meta.loading_seen", move || {
        resume.send(()).unwrap();
        drop(owner.join().unwrap());
    });
    let result = cache.get(id(0)).unwrap();
    assert!(!race::pending("meta.loading_seen"));
    let was_ready = matches!(&result, IOResult::Done(_));
    let frame = match result {
        IOResult::Done(frame) => frame,
        IOResult::IO(wait) => {
            // The no-recheck mutant leaves the late event pending. Clean it before failing.
            if !wait.finished() {
                wait.0.complete(0);
            }
            let f = match cache.get(id(0)).unwrap() {
                IOResult::Done(f) => f,
                _ => panic!("load finished"),
            };
            f
        }
    };
    let late = frame
        .load_work
        .get(&cache.completions)
        .unwrap()
        .expect("the install followed the empty take");
    let unused_pending = !late.finished();
    let handles_with_frame = late.handles();
    for _ in 0..if cfg!(miri) { 4 } else { 100 } {
        let got = cache.get(id(0)).unwrap();
        assert!(matches!(got, IOResult::Done(_)));
        let same = frame.load_work.get(&cache.completions).unwrap().unwrap();
        assert!(same.ptr_eq(&late), "a READY frame constructed another wait");
    }
    drop(frame);
    drop(cache);
    let handles_after_drop = late.handles();
    drop(late);
    assert!(
        was_ready,
        "a caller returned the late wait instead of the READY page"
    );
    assert!(unused_pending);
    assert_eq!(
        handles_with_frame, 2,
        "one cell reference and this observer"
    );
    assert_eq!(
        handles_after_drop, 1,
        "the frame released its sole event reference"
    );
}

#[test]
fn every_load_error_finishes_an_existing_registration() {
    for failure in [1, 2, 3, 4] {
        let paused = pause_load(failure);
        let calls = Arc::new(Calls(AtomicUsize::new(0)));
        let wait = match paused.cache.get(id(0)).unwrap() {
            IOResult::IO(c) => c,
            _ => panic!("the loader has not started"),
        };
        wait.set_waker(Some(&Waker::from(calls.clone())));
        let pending_before = !wait.finished();
        paused.resume.send(()).unwrap();
        let result = paused.owner.join().unwrap();
        let owner_error = result.is_err();
        let finished_after = wait.finished();
        let wakes_after = calls.0.load(Ordering::Relaxed);
        let reader_error = paused.cache.get(id(0)).is_err();
        if !wait.finished() {
            wait.0.complete(0);
        }
        drop(wait);
        drop(result);
        drop(paused.cache);
        println!("metadata failure stage={failure}, pending_before={pending_before}, finished_after={finished_after}, wakes={wakes_after}");
        assert!(pending_before);
        assert!(owner_error && reader_error);
        assert!(finished_after, "error stage {failure} lost its wait");
        assert_eq!(wakes_after, 1);
    }
}

#[test]
fn a_failed_load_before_registration_returns_the_error() {
    for failure in [1, 2, 3, 4] {
        let Paused {
            cache,
            io: _,
            resume,
            owner,
        } = pause_load(failure);
        race::at("meta.loading_seen", move || {
            resume.send(()).unwrap();
            assert!(owner.join().unwrap().is_err());
        });
        let result = cache.get(id(0));
        let was_error = result.is_err();
        let reached = !race::pending("meta.loading_seen");
        if let Ok(IOResult::IO(wait)) = &result {
            // A missing recheck must fail an assertion, not strand cleanup on its late event.
            if !wait.finished() {
                wait.0.complete(0);
            }
        }
        drop(result);
        drop(cache);
        assert!(reached);
        assert!(
            was_error,
            "error stage {failure} returned a wait after FAILED"
        );
    }
}

#[test]
fn a_reader_whose_cell_moves_rechecks_the_finished_load() {
    let Paused {
        cache,
        io: _,
        resume,
        owner,
    } = pause_load(0);
    let first = match cache.get(id(0)).unwrap() {
        IOResult::IO(c) => c,
        _ => panic!("the loader has not started"),
    };
    race::at("cell.announced", move || {
        resume.send(()).unwrap();
        drop(owner.join().unwrap());
    });
    let got = cache.get(id(0)).unwrap();
    let reached = !race::pending("cell.announced");
    let ready = matches!(&got, IOResult::Done(_));
    let finished = first.finished();
    drop(got);
    drop(first);
    drop(cache);
    assert!(
        reached,
        "the actual protected cell read was not interrupted"
    );
    assert!(ready, "a moved cell must not leave this reader parked");
    assert!(finished);
}

#[test]
fn a_load_completed_inline_needs_no_logical_event() {
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io.clone());
    let frame = match cache.get(id(0)).unwrap() {
        IOResult::Done(frame) => frame,
        _ => panic!("MemoryIO finishes inside pread"),
    };
    assert!(frame.loaded.get(&cache.completions).unwrap().is_none());
    assert!(frame.load_work.get(&cache.completions).unwrap().is_none());
    assert_eq!(io.reads.load(Ordering::Relaxed), 1);
}

/// Allocation readout for the real inline path. The old-source build is measured separately;
/// a synthetic extra allocation in this test would not be a valid old-code comparison.
#[cfg(not(miri))]
#[test]
fn count_inline_metadata_load_allocations() {
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io);
    // Warm the IO file and registry outside the interval.
    drop(cache.get(id(0)).unwrap());
    let before = crate::storage::atoll::testing::allocations();
    let pages = 128;
    for n in 1..=pages {
        let loaded = cache.get(id(n)).unwrap();
        assert!(matches!(loaded, IOResult::Done(_)));
    }
    let count = crate::storage::atoll::testing::allocations() - before;
    println!("META_INLINE_ALLOCATIONS pages={pages} allocation_calls={count}");
}

struct Paused {
    cache: Arc<MetaCache>,
    io: Arc<TestIO>,
    resume: mpsc::Sender<()>,
    owner: std::thread::JoinHandle<Result<IOResult<MetaRef>>>,
}

fn pause_load(failure: usize) -> Paused {
    let io = Arc::new(TestIO::new(failure));
    let cache = cache(io.clone());
    let (published_tx, published_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let owner = {
        let cache = cache.clone();
        std::thread::spawn(move || {
            race::at("meta.before_load", move || {
                published_tx.send(()).unwrap();
                receive(&resume_rx);
            });
            cache.get(id(0))
        })
    };
    receive(&published_rx);
    Paused {
        cache,
        io,
        resume: resume_tx,
        owner,
    }
}

fn receive<T>(rx: &mpsc::Receiver<T>) -> T {
    #[cfg(miri)]
    {
        rx.recv().unwrap()
    }
    #[cfg(not(miri))]
    {
        rx.recv_timeout(std::time::Duration::from_secs(20)).unwrap()
    }
}

fn cache(io: Arc<TestIO>) -> Arc<MetaCache> {
    Arc::new(MetaCache::new(
        io,
        "meta-lazy-test",
        16,
        Arc::new(Epochs::new()),
        0,
    ))
}

fn id(index: u64) -> MetaId {
    MetaId {
        tree: Tree::Map,
        class: SizeClass::new(0).unwrap(),
        level: 0,
        index,
    }
}

struct Calls(AtomicUsize);
impl Wake for Calls {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct TestIO {
    inner: Arc<MemoryIO>,
    failure: Arc<AtomicUsize>,
    opens: AtomicUsize,
    reads: Arc<AtomicUsize>,
}
impl TestIO {
    fn new(failure: usize) -> Self {
        Self {
            inner: Arc::new(MemoryIO::new()),
            failure: Arc::new(AtomicUsize::new(failure)),
            opens: AtomicUsize::new(0),
            reads: Arc::new(AtomicUsize::new(0)),
        }
    }
}
impl Clock for TestIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl IO for TestIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        if self.failure.load(Ordering::Relaxed) == 1 {
            return Err(injected("metadata open"));
        }
        let file = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(TestFile {
            file,
            failure: self.failure.clone(),
            reads: self.reads.clone(),
        }))
    }
    fn sync_directory(&self, path: &str) -> Result<()> {
        if self.failure.load(Ordering::Relaxed) == 2 {
            Err(injected("metadata directory sync"))
        } else {
            self.inner.sync_directory(path)
        }
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
    fn try_step(
        &self,
        key: crate::io::ReadyKey,
        ready: &std::task::Waker,
    ) -> Result<crate::io::Stepped> {
        self.inner.try_step(key, ready)
    }
    fn says_what_a_step_found(&self) -> bool {
        self.inner.says_what_a_step_found()
    }
    fn withdraw(&self, key: crate::io::ReadyKey) {
        self.inner.withdraw(key)
    }
}
struct TestFile {
    file: Arc<dyn File>,
    failure: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
}
impl File for TestFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }
    fn pread(&self, at: u64, c: Completion) -> Result<Completion> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        match self.failure.load(Ordering::Relaxed) {
            3 => Err(injected("metadata pread")),
            4 => {
                // Accepted IO, then an error delivered through the real read callback.
                c.error(CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "metadata read",
                ));
                Ok(c)
            }
            _ => self.file.pread(at, c),
        }
    }
    fn pwrite(&self, at: u64, bytes: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.file.pwrite(at, bytes, c)
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
fn injected(at: &'static str) -> LimboError {
    LimboError::CompletionError(CompletionError::IOError(std::io::ErrorKind::Other, at))
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_write_waits_for_a_hold_without_copying_on_each_poll() {
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io.clone());
    let frame = match cache.get(id(0)).unwrap() {
        IOResult::Done(frame) => frame,
        IOResult::IO(_) => panic!("the initial MemoryIO load completes inline"),
    };
    // Mark the real page dirty before the paused hold; imaging releases this dirty pin.
    frame
        .try_lock()
        .expect("the loaded page is free")
        .commit(&cache);
    let hold = frame.try_lock().expect("the loaded page is free");
    // Taken before the cut, this hold has no saved cut copy. The checkpointer must wait.
    cache.begin_cut(1);
    let mut error = None;
    let first = match cache.write_for(&frame, 1) {
        Ok(c) => c,
        Err(e) => {
            error = Some(e.to_string());
            None
        }
    };
    let calls = Arc::new(Calls(AtomicUsize::new(0)));
    let waker = Waker::from(calls.clone());
    if let Some(first) = &first {
        first.set_waker(&waker);
    }
    let proper_kind = first
        .as_ref()
        .is_some_and(|c| c.is_remote_wait() && c.is_engine_wait());
    let before_wakes = calls.0.load(Ordering::Relaxed);
    let mut ready = 0;
    let mut pending = 0;
    let mut same_wait = true;
    let mut last = None;
    // The first call may create one logical wait. Repeated early polls must reuse it and
    // allocate no image of a page that is still held by exactly the same writer.
    let before = crate::storage::atoll::testing::allocations();
    for _ in 0..32 {
        match cache.write_for(&frame, 1) {
            Ok(Some(wait)) => {
                same_wait &= first.as_ref().is_some_and(|c| c.ptr_eq(&wait));
                if wait.finished() {
                    ready += 1;
                } else {
                    pending += 1;
                }
                last = Some(wait);
            }
            Ok(None) => {
                error = Some("the held page was reported written".into());
                break;
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    let allocations = crate::storage::atoll::testing::allocations() - before;
    let while_held_wakes = calls.0.load(Ordering::Relaxed);
    drop(hold);
    let finished_after = first.as_ref().is_some_and(Completion::finished);
    let after_wakes = calls.0.load(Ordering::Relaxed);
    // Clean up even a missing-notification mutant before checking its oracle.
    if let Some(c) = &first {
        if !c.finished() {
            c.complete(0);
        }
    }
    if let Some(c) = &last {
        if !c.finished() {
            c.complete(0);
        }
    }
    let mut written = false;
    for _ in 0..16 {
        match cache.write_for(&frame, 1) {
            Ok(None) => {
                written = true;
                break;
            }
            Ok(Some(c)) => {
                if !c.finished() {
                    io.wait(&c);
                }
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop(first);
    drop(last);
    drop(waker);
    drop(calls);
    drop(frame);
    drop(cache);
    drop(io);
    println!("META_HELD_WRITE ready={ready} pending={pending} allocations={allocations} same_wait={same_wait} before_wakes={before_wakes} while_held_wakes={while_held_wakes} after_wakes={after_wakes} finished_after={finished_after} written={written} error={error:?}");
    assert!(
        error.is_none() && written,
        "the actual metadata image must write after release"
    );
    assert_eq!(
        (ready, pending),
        (0, 32),
        "a held metadata page needs a pending release wait"
    );
    assert!(
        proper_kind,
        "the holder is another producer, so this is a remote engine wait"
    );
    assert!(same_wait, "repeated held polls share the same release wait");
    assert_eq!(
        (before_wakes, while_held_wakes),
        (0, 0),
        "no wake while the holder stays paused"
    );
    assert_eq!(
        allocations, 0,
        "held polls must not allocate checkpoint copies"
    );
    assert!(
        finished_after && after_wakes == 1,
        "letting go finishes the captured wait once"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn every_metadata_hold_release_finishes_its_wait_after_unlocking() {
    struct Observe {
        frame: MetaRef,
        calls: AtomicUsize,
        held_when_woken: AtomicUsize,
    }
    impl Wake for Observe {
        fn wake(self: std::sync::Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if self.frame.is_held() {
                self.held_when_woken.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io.clone());
    let frame = match cache.get(id(0)).unwrap() {
        IOResult::Done(f) => f,
        _ => panic!("inline load"),
    };
    let mut outcomes = Vec::new();
    for kind in 0..3 {
        let hold = frame.try_lock().expect("the preceding hold ended");
        let wait = frame.wait_while_held();
        let observe = Arc::new(Observe {
            frame: frame.clone(),
            calls: AtomicUsize::new(0),
            held_when_woken: AtomicUsize::new(0),
        });
        let waker = Waker::from(observe.clone());
        wait.set_waker(&waker);
        let pending = !wait.finished();
        match kind {
            0 => drop(hold),
            1 => hold.release_unchanged(),
            _ => hold.commit(&cache),
        }
        let finished = wait.finished();
        let calls = observe.calls.load(Ordering::Relaxed);
        let held = observe.held_when_woken.load(Ordering::Relaxed);
        if !wait.finished() {
            wait.complete(0);
        }
        outcomes.push((pending, finished, calls, held));
        drop(wait);
        drop(waker);
        drop(observe);
    }
    // The commit case owns a dirty pin; finish its real image before dropping the cache.
    cache.begin_cut(1);
    let mut written = false;
    let mut error = None;
    for _ in 0..16 {
        match cache.write_for(&frame, 1) {
            Ok(None) => {
                written = true;
                break;
            }
            Ok(Some(c)) => {
                if !c.finished() {
                    io.wait(&c);
                }
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop(frame);
    drop(cache);
    drop(io);
    assert!(
        written && error.is_none(),
        "the commit case's dirty pin was written"
    );
    assert_eq!(
        outcomes,
        vec![(true, true, 1, 0); 3],
        "drop, unchanged release and commit each unlock before waking exactly once"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_release_before_the_wait_mark_does_not_leave_an_orphan() {
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io.clone());
    let frame = match cache.get(id(0)).unwrap() {
        IOResult::Done(f) => f,
        _ => panic!("inline load"),
    };
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let owner = {
        let frame = frame.clone();
        std::thread::spawn(move || {
            let hold = frame.try_lock().expect("first hold");
            let _ = ready_tx.send(());
            let _ = release_rx.recv();
            hold.release_unchanged();
            let _ = done_tx.send(());
        })
    };
    let started = ready_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .is_ok();
    race::at("meta.wait_before_mark", move || {
        let _ = release_tx.send(());
        let _ = done_rx.recv_timeout(std::time::Duration::from_secs(10));
    });
    let wait = frame.wait_while_held();
    let reached = !race::pending("meta.wait_before_mark");
    let ready = wait.finished();
    let joined = owner.join().is_ok();
    let free = !frame.is_held();
    if !wait.finished() {
        wait.complete(0);
    }
    drop(wait);
    drop(frame);
    drop(cache);
    drop(io);
    assert!(
        started && reached && joined && free,
        "the real holder released between registration and marking"
    );
    assert!(
        ready,
        "a lost mark must not return a wait whose producer has already left"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn an_uncontended_metadata_hold_creates_no_release_signal() {
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io.clone());
    let frame = match cache.get(id(0)).unwrap() {
        IOResult::Done(f) => f,
        _ => panic!("inline load"),
    };
    // Warm the path and its test thread's machinery before measuring only the holds.
    frame.try_lock().expect("free").release_unchanged();
    let before = crate::storage::atoll::testing::allocations();
    for _ in 0..32 {
        frame.try_lock().expect("free").release_unchanged();
    }
    let allocations = crate::storage::atoll::testing::allocations() - before;
    let unused = matches!(frame.released.get(&frame.release_completions), Ok(None));
    let free_wait = frame.wait_while_held();
    let ready = free_wait.finished();
    let still_unused = matches!(frame.released.get(&frame.release_completions), Ok(None));
    drop(free_wait);
    drop(frame);
    drop(cache);
    drop(io);
    assert!(
        unused && still_unused,
        "unmarked releases and a free-page recheck never construct a wait"
    );
    assert!(ready);
    assert_eq!(
        allocations, 0,
        "the uncontended hold path adds no allocation"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_release_signal_outlives_its_cache() {
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io.clone());
    let frame = match cache.get(id(0)).unwrap() {
        IOResult::Done(f) => f,
        _ => panic!("inline load"),
    };
    let hold = frame.try_lock().expect("free");
    drop(cache);
    let wait = frame.wait_while_held();
    let pending = !wait.finished();
    hold.release_unchanged();
    let finished = wait.finished();
    if !wait.finished() {
        wait.complete(0);
    }
    drop(wait);
    drop(frame);
    drop(io);
    assert!(
        pending && finished,
        "the frame owns its release domain independently of the cache"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_recovery_owed_metadata_hold_notifies_its_waiters() {
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io.clone());
    let frame = match cache.get(id(0)).unwrap() {
        IOResult::Done(f) => f,
        _ => panic!("inline load"),
    };
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (go_tx, go_rx) = mpsc::sync_channel(1);
    let owner = {
        let cache = cache.clone();
        let frame = frame.clone();
        std::thread::spawn(move || {
            race::at("meta.owe_held", move || {
                let _ = ready_tx.send(());
                let _ = go_rx.recv();
            });
            cache.owe(&frame).is_some()
        })
    };
    let started = ready_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .is_ok();
    let wait = frame.wait_while_held();
    let pending = !wait.finished();
    let calls = Arc::new(Calls(AtomicUsize::new(0)));
    let waker = Waker::from(calls.clone());
    wait.set_waker(&waker);
    let _ = go_tx.send(());
    let joined = owner.join().unwrap_or(false);
    let finished = wait.finished();
    let wakes = calls.0.load(Ordering::Relaxed);
    if !wait.finished() {
        wait.complete(0);
    }
    cache.begin_cut(1);
    let written = finish_metadata_image(&cache, &frame, 1);
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop(waker);
    drop(calls);
    drop(wait);
    drop(frame);
    drop(cache);
    drop(io);
    assert!(
        started && joined && written,
        "the real owe holder was paused and its dirty pin was written"
    );
    assert!(
        pending && finished && wakes == 1,
        "owe must use the notifying latch owner"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_load_publication_hold_notifies_its_waiters() {
    let io = Arc::new(TestIO::new(0));
    let old = cache(io.clone());
    let frame = match old.get(id(0)).unwrap() {
        IOResult::Done(f) => f,
        _ => panic!("inline load"),
    };
    let mut change = frame.try_lock().expect("free");
    change.page().write(128, b"stored bytes");
    change.commit(&old);
    old.begin_cut(1);
    let seeded = finish_metadata_image(&old, &frame, 1);
    old.end_cut(1, std::slice::from_ref(&frame));
    old.checkpointed(1, std::slice::from_ref(&frame));
    drop(frame);
    drop(old);
    let cache = Arc::new(MetaCache::new(
        io.clone(),
        "meta-lazy-test",
        16,
        Arc::new(Epochs::new()),
        1,
    ));
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (go_tx, go_rx) = mpsc::sync_channel(1);
    let owner = {
        let cache = cache.clone();
        std::thread::spawn(move || {
            race::at("meta.load_held", move || {
                let _ = ready_tx.send(());
                let _ = go_rx.recv();
            });
            cache.get(id(0))
        })
    };
    let started = ready_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .is_ok();
    let frame = {
        let step = cache.epochs.enter();
        cache.files[id(0).file_index()]
            .frames
            .get(0)
            .and_then(|cell| cell.get(&step, &cache.cells))
    };
    let wait = frame.as_ref().map(|f| f.wait_while_held());
    let pending = wait.as_ref().is_some_and(|c| !c.finished());
    let _ = go_tx.send(());
    let joined = owner.join().ok();
    let finished = wait.as_ref().is_some_and(Completion::finished);
    let bytes = frame.as_ref().and_then(|f| {
        f.read(|b| {
            let mut out = [0; 12];
            b.copy_to(128, &mut out);
            out
        })
    });
    if let Some(wait) = &wait {
        if !wait.finished() {
            wait.complete(0);
        }
    }
    drop(joined);
    drop(wait);
    drop(frame);
    drop(cache);
    drop(io);
    assert!(
        seeded && started,
        "the real load callback took the publication latch on a stored page"
    );
    assert!(
        pending && finished,
        "the load holder's release must finish its signal"
    );
    assert_eq!(bytes, Some(*b"stored bytes"));
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_saved_metadata_cut_can_be_written_while_its_live_page_is_held() {
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io.clone());
    let frame = match cache.get(id(0)).unwrap() {
        IOResult::Done(f) => f,
        _ => panic!("inline load"),
    };
    frame.try_lock().expect("free").commit(&cache);
    cache.begin_cut(1);
    // This hold keeps the cut before editing the live page, so the immutable image can proceed.
    let hold = frame.try_lock().expect("free");
    let kept = frame.cut_for.load(Ordering::Acquire) == 1;
    let written_while_held = finish_metadata_image(&cache, &frame, 1);
    let signal_unused = matches!(frame.released.get(&frame.release_completions), Ok(None));
    hold.release_unchanged();
    cache.end_cut(1, std::slice::from_ref(&frame));
    cache.checkpointed(1, std::slice::from_ref(&frame));
    cache.begin_cut(2);
    let cleaned = finish_metadata_image(&cache, &frame, 2);
    cache.end_cut(2, std::slice::from_ref(&frame));
    drop(frame);
    drop(cache);
    drop(io);
    assert!(
        kept && written_while_held && cleaned,
        "a saved cut stays writable independently of the live hold"
    );
    assert!(
        signal_unused,
        "an image taken from the saved cut has no dependency on that hold"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
fn finish_metadata_image(cache: &Arc<MetaCache>, frame: &MetaRef, n: u64) -> bool {
    for _ in 0..16 {
        match cache.write_for(frame, n) {
            Ok(None) => return true,
            Ok(Some(c)) if c.finished() => {}
            // Test IO delivers inline. Never wait on a missing producer in a negative build.
            Ok(Some(c)) => {
                c.complete(0);
                return false;
            }
            Err(_) => return false,
        }
    }
    false
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn audit_a_metadata_load_paused_before_open_does_not_repoll_an_idle_pumped_backend() {
    let trace = metadata_loading_wait_trace("meta.before_load");
    assert_metadata_loading_wait(trace);
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_loader_is_remote_until_its_read_is_accepted() {
    let trace = metadata_loading_wait_trace("meta.before_read_submit");
    assert_metadata_loading_wait(trace);
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[derive(Debug)]
struct LoadTrace {
    at: &'static str,
    started: bool,
    engine: bool,
    remote: bool,
    queued_before: usize,
    queued_paused: usize,
    pending: usize,
    early_ready: usize,
    idle_wakes: usize,
    owner_ok: bool,
    naturally_resumed: bool,
    cleaned: bool,
    error: Option<String>,
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
struct LoadWake {
    calls: AtomicUsize,
    ready: AtomicBool,
}
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
impl Wake for LoadWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.ready.store(true, Ordering::Release);
    }
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
fn metadata_loading_wait_trace(at: &'static str) -> LoadTrace {
    use crate::io::crash::CrashIO;
    use std::future::Future;
    use std::task::{Context, Poll};
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let cache = Arc::new(MetaCache::new(
        io.clone(),
        "/db/loading-async-audit",
        16,
        Arc::new(Epochs::new()),
        0,
    ));
    let (published_tx, published_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let owner = {
        let cache = cache.clone();
        std::thread::spawn(move || {
            race::at(at, move || {
                let _ = published_tx.send(());
                let _ = resume_rx.recv();
            });
            cache.get(id(0))
        })
    };
    let started = published_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .is_ok();
    let first = match cache.get(id(0)) {
        Ok(IOResult::IO(wait)) => wait.0,
        _ => Completion::new_yield(),
    };
    let queued_before = crash.in_flight();
    let remote = first.is_remote_wait();
    let engine = first.is_engine_wait();
    let wake = Arc::new(LoadWake {
        calls: AtomicUsize::new(0),
        ready: AtomicBool::new(false),
    });
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(IOCompletions(first.clone()).wait_async(io.as_ref()));
    let mut pending = 0;
    let mut early_ready = 0;
    // These extra polls also model a host polling a task because some unrelated future woke.
    // Clear its prior ready token before each poll, preserving any wake produced by that poll.
    for _ in 0..32 {
        wake.ready.store(false, Ordering::Release);
        match future.as_mut().poll(&mut cx) {
            Poll::Pending => pending += 1,
            Poll::Ready(_) => early_ready += 1,
        }
    }
    let idle_wakes = wake.calls.load(Ordering::Relaxed);
    let queued_paused = crash.in_flight();
    let _ = resume_tx.send(());
    let joined = owner.join();
    let owner_ok = matches!(&joined, Ok(Ok(_)));
    let mut naturally_resumed = false;
    let mut error = None;
    let mut new_future = false;
    for _ in 0..64 {
        // Once suspended, only a real wake permits another poll. A newly constructed future
        // gets its first poll immediately so it can register its own dependency.
        if !new_future && !wake.ready.swap(false, Ordering::AcqRel) {
            break;
        }
        new_future = false;
        match future.as_mut().poll(&mut cx) {
            Poll::Pending => {}
            Poll::Ready(Ok(())) => match cache.get(id(0)) {
                Ok(IOResult::Done(frame)) => {
                    drop(frame);
                    naturally_resumed = true;
                    break;
                }
                Ok(IOResult::IO(wait)) => {
                    future = Box::pin(wait.wait_async(io.as_ref()));
                    new_future = true;
                }
                Err(e) => {
                    error = Some(e.to_string());
                    break;
                }
            },
            Poll::Ready(Err(e)) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    drop(future);
    // Cleanup is deliberately after the wake-driven-progress measurement.
    for _ in 0..256 {
        if crash.in_flight() == 0 {
            break;
        }
        if let Err(e) = io.step() {
            error = Some(e.to_string());
            break;
        }
    }
    let cleaned = crash.in_flight() == 0;
    if !first.finished() {
        first.complete(0);
    }
    drop(joined);
    drop(first);
    drop(waker);
    drop(wake);
    drop(cache);
    drop(io);
    drop(crash);
    let trace = LoadTrace {
        at,
        started,
        engine,
        remote,
        queued_before,
        queued_paused,
        pending,
        early_ready,
        idle_wakes,
        owner_ok,
        naturally_resumed,
        cleaned,
        error,
    };
    println!("METADATA_ASYNC_WAKE_DRIVEN {trace:?}");
    trace
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
fn assert_metadata_loading_wait(trace: LoadTrace) {
    assert!(
        trace.started && trace.owner_ok && trace.cleaned && trace.engine && trace.error.is_none(),
        "the real metadata loader was paused, then cleaned: {trace:?}"
    );
    assert_eq!(
        (
            trace.queued_before,
            trace.queued_paused,
            trace.pending,
            trace.early_ready
        ),
        (0, 0, 32, 0),
        "no request was available while another actor was paused: {trace:?}"
    );
    assert_eq!(trace.idle_wakes,0,"a metadata loader paused before submission must not make an empty pumped backend poll itself: {trace:?}");
    assert!(
        trace.remote,
        "preparing a load is another actor's work: {trace:?}"
    );
    assert!(trace.naturally_resumed,"accepted pumped IO must finish with real wakes, without unsolicited polls or an external IO driver: {trace:?}");
    let _ = trace.at;
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn inline_metadata_completion_stays_ready_and_ready_reads_allocate_no_wait() {
    let io = Arc::new(TestIO::new(0));
    let cache = cache(io.clone());
    let first = cache.get(id(0));
    let mut ready = false;
    let mut error = None;
    match first {
        Ok(IOResult::Done(frame)) => {
            ready = frame.status.load(Ordering::Acquire) == READY;
            drop(frame);
        }
        Ok(IOResult::IO(wait)) => {
            if !wait.finished() {
                wait.0.complete(0);
            }
        }
        Err(e) => error = Some(e.to_string()),
    }
    let before = crate::storage::atoll::testing::allocations();
    let mut ready_reads = 0;
    for _ in 0..32 {
        match cache.get(id(0)) {
            Ok(IOResult::Done(frame)) => {
                ready_reads += 1;
                drop(frame);
            }
            Ok(IOResult::IO(wait)) => {
                if !wait.finished() {
                    wait.0.complete(0);
                }
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }
    let allocations = crate::storage::atoll::testing::allocations() - before;
    let empty = {
        let step = cache.epochs.enter();
        cache.files[id(0).file_index()]
            .frames
            .get(0)
            .and_then(|cell| cell.get(&step, &cache.cells))
            .is_some_and(|f| {
                matches!(f.loaded.get(&cache.completions), Ok(None))
                    && matches!(f.load_work.get(&cache.completions), Ok(None))
            })
    };
    drop(cache);
    drop(io);
    assert!(
        ready && ready_reads == 32 && error.is_none(),
        "an inline callback's READY cannot be overwritten by a later accepted-read publication"
    );
    assert!(
        empty,
        "ready metadata reads construct neither kind of load signal"
    );
    assert_eq!(
        allocations, 0,
        "ready metadata reads must not allocate logical waits"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_callback_hands_its_waiters_from_io_to_its_own_publication() {
    use crate::io::crash::CrashIO;
    use std::future::Future;
    use std::task::{Context, Poll};
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let cache = Arc::new(MetaCache::new(
        io.clone(),
        "/db/publishing-audit",
        16,
        Arc::new(Epochs::new()),
        0,
    ));
    let reading = match cache.get(id(0)).unwrap() {
        IOResult::IO(wait) => Some(wait.0),
        IOResult::Done(f) => {
            drop(f);
            None
        }
    };
    let queued = crash.in_flight();
    let old_wake = Arc::new(LoadWake {
        calls: AtomicUsize::new(0),
        ready: AtomicBool::new(false),
    });
    let old_waker = Waker::from(old_wake.clone());
    if let Some(c) = &reading {
        c.set_waker(&old_waker);
    }
    let (paused_tx, paused_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let worker = {
        let io = io.clone();
        std::thread::spawn(move || {
            race::at("meta.load_publishing", move || {
                let _ = paused_tx.send(());
                let _ = resume_rx.recv();
            });
            io.step()
        })
    };
    let paused = paused_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .is_ok();
    let handed_over = reading.as_ref().is_some_and(Completion::finished)
        && old_wake.ready.load(Ordering::Acquire);
    let work = match cache.get(id(0)).unwrap() {
        IOResult::IO(wait) => Some(wait.0),
        IOResult::Done(f) => {
            drop(f);
            None
        }
    };
    let remote = work
        .as_ref()
        .is_some_and(|c| c.is_remote_wait() && !c.finished());
    let work = work.unwrap_or_else(Completion::new_yield);
    let wake = Arc::new(LoadWake {
        calls: AtomicUsize::new(0),
        ready: AtomicBool::new(false),
    });
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(IOCompletions(work.clone()).wait_async(io.as_ref()));
    let mut pending = 0;
    if remote {
        for _ in 0..32 {
            wake.ready.store(false, Ordering::Release);
            if matches!(future.as_mut().poll(&mut cx), Poll::Pending) {
                pending += 1;
            }
        }
    }
    let idle_wakes = wake.calls.load(Ordering::Relaxed);
    let _ = resume_tx.send(());
    let joined = worker.join();
    let finished = work.finished();
    let woken = wake.ready.swap(false, Ordering::AcqRel);
    let delivered = woken && matches!(future.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    drop(future);
    for _ in 0..256 {
        if crash.in_flight() == 0 {
            break;
        }
        let _ = io.step();
    }
    let ready = match cache.get(id(0)).unwrap() {
        IOResult::Done(f) => {
            drop(f);
            true
        }
        IOResult::IO(c) => {
            if !c.finished() {
                c.0.complete(0);
            }
            false
        }
    };
    let worker_ok = matches!(&joined, Ok(Ok(())));
    let cleaned = crash.in_flight() == 0;
    if !work.finished() {
        work.complete(0);
    }
    if let Some(c) = &reading {
        if !c.finished() {
            c.complete(0);
        }
    }
    drop(joined);
    drop(reading);
    drop(work);
    drop(waker);
    drop(wake);
    drop(old_waker);
    drop(old_wake);
    drop(cache);
    drop(io);
    drop(crash);
    assert!(
        queued == 1 && paused && worker_ok && cleaned,
        "the real queued read entered its paused callback and cleaned up"
    );
    assert!(
        handed_over,
        "callback entry finishes the old physical-read signal before publication work"
    );
    assert!(
        remote && pending == 32 && idle_wakes == 0,
        "a paused publication actor must not cause empty IO repolls"
    );
    assert!(
        finished && delivered && ready,
        "publication's final status wakes its remote waiters and returns the page"
    );
}

#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn registration_after_a_load_changes_phase_rechecks_its_new_producer() {
    use crate::io::crash::CrashIO;
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let cache = Arc::new(MetaCache::new(
        io.clone(),
        "/db/phase-register-audit",
        16,
        Arc::new(Epochs::new()),
        0,
    ));
    let reading = match cache.get(id(0)).unwrap() {
        IOResult::IO(c) => Some(c.0),
        IOResult::Done(f) => {
            drop(f);
            None
        }
    };
    let (start_tx, start_rx) = mpsc::sync_channel(1);
    let (paused_tx, paused_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let worker = {
        let io = io.clone();
        std::thread::spawn(move || {
            let _ = start_rx.recv();
            race::at("meta.load_publishing", move || {
                let _ = paused_tx.send(());
                let _ = resume_rx.recv();
            });
            io.step()
        })
    };
    let cancel_start = start_tx.clone();
    let paused = Arc::new(AtomicBool::new(false));
    {
        let paused = paused.clone();
        race::at("meta.loading_seen", move || {
            let _ = start_tx.send(());
            paused.store(
                paused_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .is_ok(),
                Ordering::Release,
            );
        });
    }
    let result = cache.get(id(0)).unwrap();
    let reached = !race::pending("meta.loading_seen");
    let (changed, late) = match result {
        IOResult::IO(c) => (c.finished() && c.is_explicit_yield(), Some(c.0)),
        IOResult::Done(f) => {
            drop(f);
            (false, None)
        }
    };
    let new_wait = match cache.get(id(0)).unwrap() {
        IOResult::IO(c) => Some(c.0),
        IOResult::Done(f) => {
            drop(f);
            None
        }
    };
    let new_remote = new_wait
        .as_ref()
        .is_some_and(|c| c.is_remote_wait() && !c.finished());
    let _ = cancel_start.try_send(());
    let _ = resume_tx.send(());
    let joined = worker.join();
    for _ in 0..256 {
        if crash.in_flight() == 0 {
            break;
        }
        let _ = io.step();
    }
    let finished = new_wait.as_ref().is_some_and(Completion::finished);
    if let Some(c) = &new_wait {
        if !c.finished() {
            c.complete(0);
        }
    }
    if let Some(c) = &reading {
        if !c.finished() {
            c.complete(0);
        }
    }
    if let Some(c) = &late {
        if !c.finished() {
            c.complete(0);
        }
    }
    let was_paused = paused.load(Ordering::Acquire);
    let worker_ok = matches!(&joined, Ok(Ok(())));
    let cleaned = crash.in_flight() == 0;
    drop(joined);
    drop(reading);
    drop(new_wait);
    drop(late);
    drop(paused);
    drop(cache);
    drop(io);
    drop(crash);
    assert!(
        reached && was_paused && worker_ok && cleaned,
        "the physical read changed phase between the status look and registration"
    );
    assert!(
        changed,
        "a registration in the old IO phase must recheck and yield on its changed producer"
    );
    assert!(
        new_remote && finished,
        "the next registration waits on the publishing actor and is completed there"
    );
}

/// A metadata write's end is told at its request's retirement, not at its callback: a write
/// cancelled, its callback run aborted while its backend still owns it, leaves the write's wait
/// unfinished until the backend delivers it, then fails it (DESIGN.md 22.1, 22.13; mutant: the
/// wait told in the callback).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_writes_wait_ends_when_its_request_retires() {
    let crash = Arc::new(crate::io::crash::CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let cache = Arc::new(MetaCache::new(
        io.clone(),
        "meta-retire-test",
        16,
        Arc::new(Epochs::new()),
        0,
    ));
    let frame = crate::storage::atoll::testing::drive(&io, || cache.get(id(0))).unwrap();
    frame
        .try_lock()
        .expect("the loaded page is free")
        .commit(&cache);
    cache.begin_cut(1);
    let wait = cache
        .write_for(&frame, 1)
        .unwrap()
        .expect("a write started");
    let write = LAST_WRITE
        .with(|w| w.borrow_mut().take())
        .expect("the write's request");
    write.abort();
    let told_at_abort = wait.finished();
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    let told_at_retirement = wait.finished();
    let failed = wait.get_error().is_some();
    drop(frame);
    drop(cache);
    assert!(
        !told_at_abort,
        "the write's wait ended while its backend still owned it"
    );
    assert!(told_at_retirement, "the retirement told the wait nothing");
    assert!(failed, "a cancelled write's wait ended without its failure");
}

/// A metadata write on a file not written durably ends at the retirement of its last request,
/// whichever of the write's and its sync's that is: on a backend two threads step, the sync can
/// retire while the write's own request is still owned, paused after its callback submitted the
/// sync, and the write's place stays held until the write retires too (DESIGN.md 22.1, 22.13; the
/// auditor's P2-META-R1 probe; mutant: the sync's retirement telling alone).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_metadata_sync_cannot_end_the_write_while_the_writes_request_is_owned() {
    use crate::storage::atoll::requests::{Counted, Requests};
    use crate::storage::atoll::testing::drive;
    use std::sync::mpsc;
    const BOUND: std::time::Duration = std::time::Duration::from_secs(15);
    let crash = Arc::new(crate::io::crash::CrashIO::new(0));
    crash.set_durable_writes(false);
    let requests = Arc::new(Requests::default());
    let io: Arc<dyn IO> = Arc::new(Counted::new(crash.clone(), requests.clone()));
    let cache = Arc::new(MetaCache::new(
        io.clone(),
        "meta-sync-before-write",
        16,
        Arc::new(Epochs::new()),
        0,
    ));
    let frame = drive(&io, || cache.get(id(0))).unwrap();
    frame
        .try_lock()
        .expect("the loaded page is free")
        .commit(&cache);
    drop(cache.begin_cut(1));
    let wait = cache
        .write_for(&frame, 1)
        .unwrap()
        .expect("a write started");
    let write = LAST_WRITE
        .with(|w| w.borrow_mut().take())
        .expect("the write's request");
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel::<()>();
    let parent = {
        let crash = crash.clone();
        std::thread::spawn(move || {
            // The write's callback has submitted the sync; its own request is still owned, its
            // completion published before it retires.
            crate::race::at("completion.published", move || {
                let _ = paused_tx.send(());
                let _ = resume_rx.recv_timeout(BOUND);
            });
            crash.step()
        })
    };
    let paused = paused_rx.recv_timeout(BOUND).is_ok();
    let before = (
        write.finished(),
        write.is_owned(),
        wait.finished(),
        requests.outstanding(),
        crash.in_flight(),
    );
    let synced = paused && before.4 == 1 && crash.step().is_ok();
    let after_sync = (
        write.is_owned(),
        wait.finished(),
        requests.outstanding(),
        crash.in_flight(),
    );
    let _ = resume_tx.send(());
    let parent_ended = parent.join().is_ok_and(|r| r.is_ok());
    requests.drain(&*io);
    let after_both = (
        !write.is_owned(),
        wait.finished(),
        wait.get_error(),
        requests.outstanding(),
    );
    cache.end_cut(1, std::slice::from_ref(&frame));
    drop((write, wait, frame, cache, io, requests, crash));
    assert!(paused && synced && parent_ended, "the steps did not run");
    assert_eq!(
        before,
        (true, true, false, 2, 1),
        "the write's callback published with its sync submitted"
    );
    assert_eq!(
        after_sync,
        (true, false, 1, 0),
        "the sync retired with the write's request still owned"
    );
    assert_eq!(after_both, (true, true, None, 0), "both retired");
}

/// 128 metadata pages asked of a cache whose budget gives it one read credit: the first miss takes
/// the credit and its read goes to the backend, the other 127 register on the credits' give-back
/// and return IO, making no frame and issuing no read; the first read's retirement tells them all,
/// and each page is then served in turn with never more than one read with the backend (DESIGN.md
/// 22.18, a metadata read credit; the auditor's metadata probe; mutant: a miss reading with no
/// credit).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn metadata_loads_under_one_read_credit_keep_one_read_with_the_backend() {
    use crate::io::crash::CrashIO;
    const PAGES: u64 = 128;
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let cache = Arc::new(MetaCache::new(
        io,
        "meta-credits",
        1,
        Arc::new(Epochs::new()),
        0,
    ));
    let waits: Vec<Option<Completion>> = (0..PAGES)
        .map(|i| match cache.get(id(i)) {
            Ok(IOResult::IO(IOCompletions(c))) => Some(c),
            _ => None,
        })
        .collect();
    let unfinished = |w: &Option<Completion>| w.as_ref().is_some_and(|c| !c.finished());
    let at_asks = (
        crash.in_flight(),
        cache.resident(),
        waits.iter().skip(1).filter(|w| unfinished(w)).count(),
    );
    // The first read lands and retires: its credit back, every miss told.
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    let told = waits
        .iter()
        .skip(1)
        .all(|w| w.as_ref().is_some_and(Completion::finished));
    let (mut most, mut served) = (0, 0);
    for i in 0..PAGES {
        loop {
            match cache.get(id(i)) {
                Ok(IOResult::Done(frame)) => {
                    served += 1;
                    drop(frame);
                    break;
                }
                Ok(IOResult::IO(_)) if crash.in_flight() > 0 => {
                    most = most.max(crash.in_flight());
                    crash.step().unwrap();
                }
                _ => break,
            }
        }
    }
    let free = cache.credits.free();
    // Every assertion with the cache dropped.
    drop((waits, cache));
    assert_eq!(
        at_asks,
        (1, 1, 127),
        "(reads with the backend, frames, misses waiting on the credits) after the asks"
    );
    assert!(told, "the first read's retirement left a miss waiting");
    assert_eq!(served, PAGES, "pages served");
    assert_eq!(most, 1, "reads with the backend at most while served");
    assert_eq!(free, 1, "the credit not given back after the last read");
}

/// A metadata read cancelled, its callback run aborted while the backend still owns it, keeps its
/// credit: a second page's miss finds none free and waits, issuing no read, until the backend
/// retires the first; then its read goes to the backend and its page is served (DESIGN.md 22.18, a
/// metadata read credit, given back at the retirement and never when the read finishes; mutant:
/// given back as the read's callback runs).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_cancelled_metadata_read_keeps_its_credit_until_its_backend_retires_it() {
    use crate::io::crash::CrashIO;
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let cache = Arc::new(MetaCache::new(
        io,
        "meta-credit-cancel",
        1,
        Arc::new(Epochs::new()),
        0,
    ));
    let first = cache.get(id(0));
    let read = LAST_READ
        .with(|r| r.borrow_mut().take())
        .expect("the first page's read");
    read.abort();
    let second = match cache.get(id(1)) {
        Ok(IOResult::IO(IOCompletions(c))) => Some(c),
        _ => None,
    };
    let at_cancel = (
        read.is_owned(),
        crash.in_flight(),
        second.as_ref().is_some_and(|c| !c.finished()),
    );
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    let told = second.as_ref().is_some_and(Completion::finished);
    let then = (
        matches!(cache.get(id(1)), Ok(IOResult::IO(_))),
        crash.in_flight(),
    );
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    let served = matches!(cache.get(id(1)), Ok(IOResult::Done(_)));
    // Every assertion with the cache dropped.
    drop((first, read, second, cache));
    assert_eq!(
        at_cancel,
        (true, 1, true),
        "(the cancelled read owned, reads with the backend, the second miss waiting)"
    );
    assert!(
        told,
        "the cancelled read's retirement left the second miss waiting"
    );
    assert_eq!(
        then,
        (true, 1),
        "(the second miss reading, reads with the backend) after the retirement"
    );
    assert!(served, "the second page not served");
}

/// A miss that finds the cache's one read credit taken by a read whose caller does not poll it
/// again waits on the credits' give-back as a waiter that steps the backend, which is what gives a
/// credit back: waited on alone, as a blocking caller waits, its steps bring that read in, its
/// retirement wakes the miss, and the miss is then served (the auditor's #4580; mutant: the
/// credits' wait of the remote kind, which no wait steps the backend for, so the read stays queued).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_miss_waiting_on_the_read_credits_steps_the_backend_that_gives_one_back() {
    use crate::io::crash::CrashIO;
    use crate::storage::atoll::testing::drive;
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let cache = Arc::new(MetaCache::new(
        io.clone(),
        "meta-credit-stepped",
        1,
        Arc::new(Epochs::new()),
        0,
    ));
    // The first page's read takes the credit; its caller never polls it again.
    let first = cache.get(id(0));
    let second = match cache.get(id(1)) {
        Ok(IOResult::IO(IOCompletions(c))) => Some(c),
        _ => None,
    };
    let queued = crash.in_flight();
    let stepped = second.as_ref().is_some_and(|c| !c.is_remote_wait());
    // A wait of the remote kind would block here with the read queued: it is not waited on.
    if let Some(c) = second.as_ref().filter(|_| stepped) {
        io.wait(c);
    }
    let told = second.as_ref().is_some_and(Completion::finished);
    let served = told && drive(&io, || cache.get(id(1))).is_ok();
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    // Every assertion with the cache dropped.
    drop((first, second, cache));
    assert_eq!(
        queued, 1,
        "reads with the backend while the second miss waited"
    );
    assert!(
        stepped,
        "the credits' wait is of the remote kind: no waiter steps the backend that gives one back"
    );
    assert!(told, "the second miss's wait stayed unfinished");
    assert!(served, "the second page not served");
}

/// A miss waiting on the read credits while the credit's owner is paused before it submits its
/// read, the backend holding nothing: the miss's wait looks at the backend once, finds it empty and
/// parks registered, taking no further step; the owner's submission wakes it, its steps bring that
/// read in, and the read's retirement gives it the credit (the auditor's #4583).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_miss_waiting_on_a_credit_whose_read_is_not_submitted_parks_until_the_submission() {
    use crate::io::crash::CrashIO;
    use crate::storage::atoll::testing::drive;
    use std::time::{Duration, Instant};
    const BOUND: Duration = Duration::from_secs(60);
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    let cache = Arc::new(MetaCache::new(
        io.clone(),
        "meta-credit-unsubmitted",
        1,
        Arc::new(Epochs::new()),
        0,
    ));
    let (paused_tx, paused_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let owner = {
        let cache = cache.clone();
        std::thread::spawn(move || {
            race::at("meta.before_read_submit", move || {
                let _ = paused_tx.send(());
                let _ = go_rx.recv_timeout(BOUND);
            });
            matches!(cache.get(id(0)), Ok(IOResult::IO(_)))
        })
    };
    let paused = paused_rx.recv_timeout(BOUND).is_ok();
    let second = match cache.get(id(1)) {
        Ok(IOResult::IO(IOCompletions(c))) => Some(c),
        _ => None,
    };
    let stepped = second.as_ref().is_some_and(|c| !c.is_remote_wait());
    let steps = Arc::new(AtomicUsize::new(0));
    {
        let steps = steps.clone();
        crash.on_step(Some(Arc::new(move || {
            steps.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })));
    }
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = second.clone().filter(|_| stepped).map(|c| {
        let io = io.clone();
        std::thread::spawn(move || {
            io.wait(&c);
            let _ = done_tx.send(());
        })
    });
    // Parked: registered with the backend, which holds nothing.
    let began = Instant::now();
    while waiter.is_some() && crash.registered() == 0 && began.elapsed() < BOUND {
        std::thread::yield_now();
    }
    let parked = (crash.registered(), steps.load(Ordering::Relaxed));
    let _ = go_tx.send(());
    let submitted = owner.join().unwrap_or(false);
    let woken = done_rx.recv_timeout(BOUND).is_ok();
    let joined = waiter.map_or(false, |w| w.join().is_ok());
    crash.on_step(None);
    let served = woken && drive(&io, || cache.get(id(1))).is_ok();
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    // Every assertion with the cache dropped.
    drop((second, cache));
    assert!(
        paused,
        "the credit's owner never paused before its submission"
    );
    assert!(
        stepped,
        "the credits' wait is of the remote kind: no waiter steps the backend that gives one back"
    );
    assert_eq!(
        parked,
        (1, 1),
        "(registrations with the empty backend, steps taken) while the owner had not submitted"
    );
    assert!(submitted, "the owner's read was not submitted");
    assert!(
        woken && joined,
        "the submission did not wake the miss's wait"
    );
    assert!(served, "the second page not served");
}

/// A collection of metadata pages keeps each frame it found across its caller's polls, the ones
/// still loading among them: another operation's miss between two polls, putting the cache over its
/// budget, evicts none of them, and the collection then finishes reading only the page it had no
/// credit for (DESIGN.md 22.18: a get_all keeps the frames it has pinned; the auditor's #4573 and
/// #4578; mutant: a collection that lets its frames go when it waits).
#[cfg(all(not(shuttle), not(atoll_loom), not(miri)))]
#[test]
fn a_collection_keeps_its_frames_across_polls_so_no_eviction_between_takes_one() {
    use crate::io::crash::CrashIO;
    use crate::storage::atoll::testing::drive;
    let crash = Arc::new(CrashIO::new(0));
    let io: Arc<dyn IO> = crash.clone();
    // Two pages kept, so two read credits.
    let cache = Arc::new(MetaCache::new(
        io.clone(),
        "meta-pins",
        2,
        Arc::new(Epochs::new()),
        0,
    ));
    let ids = [id(0), id(1), id(2)];
    let mut pins = Collection::default();
    // The first two read under the credits, the third refused one.
    let first = matches!(pins.collect(&cache, &ids), Ok(IOResult::IO(_)));
    let while_loading = (pins.held(), crash.in_flight());
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    // Another operation between the polls: a page outside the collection, its put over the budget.
    let other = drive(&io, || cache.get(id(3))).is_ok();
    let resident = cache.resident();
    // The third page's read, the only one.
    let second = matches!(pins.collect(&cache, &ids), Ok(IOResult::IO(_)));
    let reading = crash.in_flight();
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    let collected = match pins.collect(&cache, &ids) {
        Ok(IOResult::Done(frames)) => Some(frames.len()),
        _ => None,
    };
    let left = pins.held();
    // Every assertion with the cache dropped.
    drop((pins, cache));
    assert!(first, "the first poll did not wait");
    assert_eq!(
        while_loading,
        (2, 2),
        "(frames held, reads with the backend) while the first two loaded"
    );
    assert!(other, "the other operation's page not served");
    assert_eq!(
        resident, 3,
        "the other operation's miss evicted a page the collection had found"
    );
    assert!(
        second && reading == 1,
        "the second poll read something but the page it had no credit for"
    );
    assert_eq!(collected, Some(3), "the collection did not finish");
    assert_eq!(
        left, 0,
        "the collection kept frames after it handed them over"
    );
}
