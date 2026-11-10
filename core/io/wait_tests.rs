//! Who waits how (DESIGN.md 22.5): a blocking waiter parks on a backend that delivers inline or on
//! a thread of its own, and steps a pumped one until what it waits for finishes.

use super::crash::CrashIO;
use super::*;
use crate::sync::atomic::{AtomicUsize, Ordering};

/// An IO that counts its steps and forwards everything else, saying who delivers as the IO it
/// wraps does: a waiter that steps a backend that needs no step shows up in the count.
pub(crate) struct StepCounting {
    pub(crate) io: Arc<dyn IO>,
    pub(crate) steps: AtomicUsize,
}

impl StepCounting {
    pub(crate) fn new(io: Arc<dyn IO>) -> Arc<Self> {
        Arc::new(Self {
            io,
            steps: AtomicUsize::new(0),
        })
    }

    pub(crate) fn steps(&self) -> usize {
        self.steps.load(Ordering::SeqCst)
    }
}

/// A pumped backend whose steps cannot say what they found: the trait's defaults for `try_step`
/// and `says_what_a_step_found`, over a crash IO.
pub(crate) struct Unsaying(pub(crate) Arc<CrashIO>);

impl Clock for Unsaying {
    fn current_time_monotonic(&self) -> clock::MonotonicInstant {
        self.0.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> clock::WallClockInstant {
        self.0.current_time_wall_clock()
    }
}

impl IO for Unsaying {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.0.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.0.remove_file(path)
    }

    fn step(&self) -> Result<()> {
        self.0.step()
    }
}

impl Clock for StepCounting {
    fn current_time_monotonic(&self) -> clock::MonotonicInstant {
        self.io.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> clock::WallClockInstant {
        self.io.current_time_wall_clock()
    }
}

impl IO for StepCounting {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.io.open_file(path, flags, direct)
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

    fn step(&self) -> Result<()> {
        self.steps.fetch_add(1, Ordering::SeqCst);
        self.io.step()
    }

    fn try_step(&self, key: crate::io::ReadyKey, ready: &std::task::Waker) -> Result<Stepped> {
        self.steps.fetch_add(1, Ordering::SeqCst);
        self.io.try_step(key, ready)
    }

    fn says_what_a_step_found(&self) -> bool {
        self.io.says_what_a_step_found()
    }

    fn withdraw(&self, key: crate::io::ReadyKey) {
        self.io.withdraw(key)
    }

    fn delivery(&self) -> Delivery {
        self.io.delivery()
    }

    fn cancel(&self, c: &[Completion]) -> Result<()> {
        self.io.cancel(c)
    }

    fn file_id(&self, path: &str) -> Result<FileId> {
        self.io.file_id(path)
    }
}

fn read(len: usize) -> Completion {
    Completion::new_read(Arc::new(Buffer::new_temporary(len)), |_| None)
}

/// Runs `wait` on another thread, false if it has not returned within `bound`: a wait that never
/// returns fails the test rather than hanging it.
fn returns_within(bound: std::time::Duration, wait: impl FnOnce() + Send + 'static) -> bool {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        wait();
        let _ = tx.send(());
    });
    rx.recv_timeout(bound).is_ok()
}

/// A wait's bound: the waits here end within milliseconds, so ten seconds is a hang.
const BOUND: std::time::Duration = std::time::Duration::from_secs(10);

/// On a pumped backend a waiter steps until its own request is delivered, however many steps
/// that takes: here the waiter's read is submitted by the callback of one ahead of it, so the first
/// step can deliver only the one ahead (mutant: one step, then park, which leaves the waiter's
/// read for a step nobody takes).
#[test]
fn a_pumped_wait_steps_past_a_request_queued_ahead_until_its_own_is_delivered() {
    let io = Arc::new(CrashIO::new(1));
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let mine = read(512);
    let ahead = {
        let (file, mine) = (file.clone(), mine.clone());
        Completion::new_read(Arc::new(Buffer::new_temporary(512)), move |_| {
            drop(file.pread(0, mine.clone()).unwrap());
            None
        })
    };
    let ahead = file.pread(0, ahead).unwrap();
    let waiter = (io.clone(), mine.clone());
    assert!(
        returns_within(BOUND, move || waiter.0.wait(&waiter.1)),
        "the wait did not step its own request in"
    );
    assert!(ahead.finished() && mine.finished());
    assert_eq!(io.in_flight(), 0);
}

#[test]
#[cfg(not(any(shuttle, atoll_loom)))]
fn a_pumped_async_wait_requests_another_poll_until_its_request_finishes() {
    use crate::types::IOCompletions;
    use std::future::Future;
    use std::task::{Context, Poll, Wake, Waker};

    struct Wakes(AtomicUsize);

    impl Wake for Wakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let backend = Arc::new(CrashIO::new(1));
    let io = StepCounting::new(backend.clone());
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let mine = read(512);
    let ahead = {
        let (file, mine) = (file.clone(), mine.clone());
        Completion::new_read(Arc::new(Buffer::new_temporary(512)), move |_| {
            drop(file.pread(0, mine.clone()).unwrap());
            None
        })
    };
    let ahead = file.pread(0, ahead).unwrap();
    let wakes = std::sync::Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(IOCompletions(mine.clone()).wait_async(&*io));
    for poll in 1..=4 {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => {
                result.unwrap();
                assert!(ahead.finished() && mine.finished());
                assert_eq!(backend.in_flight(), 0);
                assert_eq!(io.steps(), 2);
                return;
            }
            Poll::Pending => assert!(
                wakes.0.swap(0, Ordering::SeqCst) > 0,
                "poll {poll} left a request queued with nobody to poll its waiter again"
            ),
        }
    }
    panic!("the async wait did not finish within four polls");
}

/// A blocking waiter's key is the address of the place it keeps: two places live at once are two
/// keys, so each registers and withdraws its own (DESIGN.md 22.5; the auditor's P2-E1c-K1;
/// mutant: every place under one key).
#[test]
fn every_live_place_is_a_key_of_its_own() {
    let backend = CrashIO::new(1);
    let (one, two) = (ReadyPlace::default(), ReadyPlace::default());
    let waker = unparking(crate::thread::current());
    let steps = [
        backend.try_step(one.key(), &waker).unwrap(),
        backend.try_step(two.key(), &waker).unwrap(),
        backend.try_step(one.key(), &waker).unwrap(),
    ];
    let both = backend.registered();
    backend.withdraw(one.key());
    let left = backend.registered();
    backend.withdraw(two.key());
    assert_eq!(steps, [Stepped::Empty; 3]);
    assert_ne!(one.key(), two.key());
    assert_eq!(
        one.key(),
        ReadyKey(std::num::NonZeroUsize::new(std::ptr::from_ref(&one) as usize).unwrap()),
        "a key is its place's address, no count that could wrap"
    );
    assert_eq!(
        both, 2,
        "one registration per place, however often it looks"
    );
    assert_eq!(left, 1, "the other place's registration kept");
    assert_eq!(backend.registered(), 0);
}

/// Counts the wakes of an async wait's host.
struct HostWakes(AtomicUsize);

impl std::task::Wake for HostWakes {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// An async wait on a pumped backend that holds nothing registers with the backend and the
/// completion and asks for no poll: its host polls again only once one of them wakes it. Ready or
/// dropped, it takes its registration back (DESIGN.md 22.5; the auditor's P2-E1b-A1; mutants: an
/// empty step that asks for another poll, a CPU spent on empty steps; a wait that leaves its
/// registration behind).
#[test]
fn a_pumped_async_wait_on_an_empty_backend_is_polled_again_only_when_woken() {
    use crate::types::IOCompletions;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let backend = Arc::new(CrashIO::new(1));
    let wakes = std::sync::Arc::new(HostWakes(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let finished_elsewhere = Completion::new_wait();
    let mut future =
        std::pin::pin!(IOCompletions(finished_elsewhere.clone()).wait_async(&*backend));
    let first = future.as_mut().poll(&mut context).is_pending();
    let parked = (wakes.0.load(Ordering::SeqCst), backend.registered());
    finished_elsewhere.complete(0);
    let woken = wakes.0.load(Ordering::SeqCst);
    let ready = matches!(future.as_mut().poll(&mut context), Poll::Ready(Ok(())));
    let after_ready = backend.registered();
    let dropped = {
        let unfinished = Completion::new_wait();
        let mut future = Box::pin(IOCompletions(unfinished.clone()).wait_async(&*backend));
        let pending = future.as_mut().poll(&mut context).is_pending();
        let held = backend.registered();
        drop(future);
        (pending, held, backend.registered())
    };
    assert!(first, "pending while unfinished");
    assert_eq!(parked, (0, 1), "(wakes, registrations) after an empty step");
    assert_eq!(woken, 1, "woken once by the finish");
    assert!(ready);
    assert_eq!(
        after_ready, 0,
        "a registration left behind by a wait that ended"
    );
    assert_eq!(
        dropped,
        (true, 1, 0),
        "a registration left behind by a wait dropped"
    );
}

/// Two async waits of one host task share its waker: each registers under its own key, so the
/// first ending takes back its own registration alone, and a request submitted later still wakes
/// the host for the second (DESIGN.md 22.5; the auditor's #4204 order; mutant: registrations
/// kept one per waker, which the first wait's end takes away from the second).
#[test]
fn two_async_waits_sharing_a_waker_each_take_back_only_their_own_registration() {
    use crate::types::IOCompletions;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let backend = Arc::new(CrashIO::new(1));
    let file = backend
        .open_file("/db/f", OpenFlags::Create, false)
        .unwrap();
    let wakes = std::sync::Arc::new(HostWakes(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let (one, two) = (Completion::new_wait(), Completion::new_wait());
    let mut first = std::pin::pin!(IOCompletions(one.clone()).wait_async(&*backend));
    let mut second = std::pin::pin!(IOCompletions(two.clone()).wait_async(&*backend));
    let pending = [
        first.as_mut().poll(&mut context).is_pending(),
        second.as_mut().poll(&mut context).is_pending(),
    ];
    let both = backend.registered();
    one.complete(0);
    let first_done = matches!(first.as_mut().poll(&mut context), Poll::Ready(Ok(())));
    let left = backend.registered();
    let before = wakes.0.load(Ordering::SeqCst);
    let request = {
        let two = two.clone();
        Completion::new_sync(move |_| two.complete(0))
    };
    let request = file.sync(request, FileSyncType::Fsync).unwrap();
    let woken = wakes.0.load(Ordering::SeqCst) - before;
    let mut polls = 0;
    let second_done = loop {
        polls += 1;
        match second.as_mut().poll(&mut context) {
            Poll::Ready(result) => break result.is_ok(),
            Poll::Pending if polls < 4 => {}
            Poll::Pending => break false,
        }
    };
    assert_eq!(pending, [true, true]);
    assert_eq!(both, 2, "one registration per wait");
    assert!(first_done);
    assert_eq!(left, 1, "the second wait's registration kept");
    assert_eq!(woken, 1, "the submission woke the host for the second wait");
    assert!(second_done && request.finished());
    assert_eq!(backend.registered(), 0);
}

/// On a backend that delivers inline, what a caller still waits for another thread finishes:
/// the waiter parks until it does and never steps (mutant: the backend taken for pumped, which
/// steps a backend with nothing to deliver for as long as the other thread takes).
#[test]
fn an_inline_backends_waiter_parks_until_another_thread_finishes_it() {
    let io = StepCounting::new(Arc::new(MemoryIO::new()));
    assert_eq!(io.delivery(), Delivery::Inline);
    let c = Completion::new_wait();
    let finisher = {
        let c = c.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            c.complete(0);
        })
    };
    io.wait(&c);
    finisher.join().unwrap();
    assert!(c.finished());
    assert_eq!(
        io.steps(),
        0,
        "the waiter stepped a backend that delivers inline"
    );
}

/// A backend that says it delivers inline does: every request is finished and let go when its
/// submission returns.
#[test]
fn a_backend_that_says_it_delivers_inline_does() {
    let dir = tempfile::tempdir().unwrap();
    let mut backends: Vec<(Arc<dyn IO>, String)> =
        vec![(Arc::new(MemoryIO::new()), "/f".to_string())];
    #[cfg(all(unix, not(miri)))]
    backends.push((
        Arc::new(PlatformIO::new().unwrap()),
        dir.path().join("f").display().to_string(),
    ));
    for (io, path) in backends {
        assert_eq!(io.delivery(), Delivery::Inline);
        let file = io.open_file(&path, OpenFlags::Create, false).unwrap();
        let buffer = Arc::new(Buffer::new_temporary(512));
        let submitted = [
            file.pwrite(0, buffer.clone(), Completion::new_write(|_| {}))
                .unwrap(),
            file.pwritev(
                512,
                vec![buffer.clone(), buffer.clone()],
                Completion::new_write(|_| {}),
            )
            .unwrap(),
            file.pread(0, read(512)).unwrap(),
            file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
                .unwrap(),
            file.truncate(512, Completion::new_trunc(|_| {})).unwrap(),
        ];
        for c in &submitted {
            assert!(
                c.succeeded() && !c.is_owned(),
                "a request left to deliver after its submission returned"
            );
        }
    }
}

#[test]
#[cfg(not(any(shuttle, atoll_loom)))]
fn an_async_remote_wait_does_not_pump_or_wake_itself() {
    use crate::types::IOCompletions;
    use std::future::Future;
    use std::task::{Context, Poll, Wake, Waker};

    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let backend = Arc::new(CrashIO::new(1));
    let io = StepCounting::new(backend.clone());
    let completion = Completion::new_remote_wait();
    assert!(completion.is_remote_wait());
    assert_eq!(backend.in_flight(), 0);
    let wakes = std::sync::Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let (polls, idle_wakes, idle_steps, finish_wakes, ready) = {
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(IOCompletions(completion.clone()).wait_async(&*io));
        let (mut polls, mut idle_wakes) = (0, 0);
        for _ in 0..64 {
            assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
            polls += 1;
            let got = wakes.0.swap(0, Ordering::SeqCst);
            idle_wakes += got;
            if got == 0 {
                break;
            }
        }
        let idle_steps = io.steps();
        let finisher = {
            let completion = completion.clone();
            std::thread::spawn(move || completion.complete(0))
        };
        finisher.join().unwrap();
        let finish_wakes = wakes.0.swap(0, Ordering::SeqCst);
        let ready = matches!(future.as_mut().poll(&mut context), Poll::Ready(Ok(())));
        (polls, idle_wakes, idle_steps, finish_wakes, ready)
    };
    let outstanding = backend.in_flight();
    drop(completion);
    drop(io);
    drop(backend);
    drop(waker);
    println!("REMOTE_ASYNC_WAIT polls={polls} idle_wakes={idle_wakes} steps={idle_steps} outstanding={outstanding} finish_wakes={finish_wakes} ready={ready}");
    assert_eq!(outstanding, 0);
    assert!(
        ready && finish_wakes > 0,
        "the real finisher wakes and finishes its waiter"
    );
    assert_eq!(
        (polls, idle_wakes, idle_steps),
        (1, 0, 0),
        "a remote wait has no IO to pump and waits for its finisher"
    );
}

#[cfg(not(any(shuttle, atoll_loom)))]
mod cancelled_wait_audit {
    use super::*;
    use crate::types::IOCompletions;
    use std::future::{Future, IntoFuture};
    use std::task::{Context, Wake, Waker};

    struct Calls(AtomicUsize);
    impl Wake for Calls {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn task() -> std::sync::Arc<Calls> {
        std::sync::Arc::new(Calls(AtomicUsize::new(0)))
    }

    #[test]
    fn cancelled_async_waits_do_not_remain_registered() {
        let io = MemoryIO::new();
        let completion = Completion::new_remote_wait();
        let tasks: Vec<_> = (0..32).map(|_| task()).collect();
        let mut pending = 0;
        for task in &tasks {
            let waker = Waker::from(task.clone());
            let mut cx = Context::from_waker(&waker);
            let mut future = std::pin::pin!(IOCompletions(completion.clone()).wait_async(&io));
            pending += usize::from(future.as_mut().poll(&mut cx).is_pending());
        }
        let registered_after_drop = completion.wakers();
        let refs_after_drop: Vec<_> = tasks.iter().map(std::sync::Arc::strong_count).collect();
        completion.complete(0);
        let stale_wakes = tasks
            .iter()
            .map(|t| t.0.load(Ordering::SeqCst))
            .sum::<usize>();
        drop(completion);
        drop(io);
        eprintln!("AUDIT cancelled32 pending={pending} live_futures=0 registered_after_drop={registered_after_drop} refs={refs_after_drop:?} stale_wakes={stale_wakes}");
        assert_eq!(
            registered_after_drop, 0,
            "cancelled async wait registrations retained by a held-state completion"
        );
        assert_eq!(stale_wakes, 0, "cancelled async tasks were woken");
    }

    #[test]
    fn one_async_wait_replaces_changed_context_waker() {
        let io = MemoryIO::new();
        let completion = Completion::new_remote_wait();
        let tasks: Vec<_> = (0..32).map(|_| task()).collect();
        let (registered, pending) = {
            let mut future = std::pin::pin!(IOCompletions(completion.clone()).wait_async(&io));
            let mut pending = 0;
            for task in &tasks {
                let waker = Waker::from(task.clone());
                let mut cx = Context::from_waker(&waker);
                pending += usize::from(future.as_mut().poll(&mut cx).is_pending());
            }
            (completion.wakers(), pending)
        };
        completion.complete(0);
        let wakes: Vec<_> = tasks.iter().map(|t| t.0.load(Ordering::SeqCst)).collect();
        drop(completion);
        drop(io);
        eprintln!("AUDIT changed32 pending={pending} live_futures_at_measure=1 registered={registered} wakes_after_drop={wakes:?}");
        assert_eq!(
            registered, 1,
            "one async wait retained old context wakers rather than replacing its registration"
        );
    }

    #[test]
    fn an_async_wait_wakes_only_its_latest_context() {
        let io = MemoryIO::new();
        let completion = Completion::new_remote_wait();
        let tasks: Vec<_> = (0..32).map(|_| task()).collect();
        let (registered, ready, wakes) = {
            let mut future = std::pin::pin!(IOCompletions(completion.clone()).wait_async(&io));
            for task in &tasks {
                let waker = Waker::from(task.clone());
                let mut cx = Context::from_waker(&waker);
                let _ = future.as_mut().poll(&mut cx);
            }
            let registered = completion.wakers();
            completion.complete(0);
            let waker = Waker::from(tasks.last().unwrap().clone());
            let ready = future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready();
            (
                registered,
                ready,
                tasks
                    .iter()
                    .map(|t| t.0.load(Ordering::SeqCst))
                    .collect::<Vec<_>>(),
            )
        };
        drop(completion);
        drop(io);
        eprintln!("AUDIT latest registered={registered} ready={ready} wakes={wakes:?}");
        assert_eq!(registered, 1);
        assert!(
            ready && wakes[..31].iter().all(|n| *n == 0) && wakes[31] == 1,
            "the last Context must receive the completion's wake"
        );
    }

    #[test]
    fn cancelling_one_async_wait_keeps_the_other_wait_registered() {
        let io = MemoryIO::new();
        let completion = Completion::new_remote_wait();
        let (a, b) = (task(), task());
        let (wa, wb) = (Waker::from(a.clone()), Waker::from(b.clone()));
        let mut future_b = std::pin::pin!(IOCompletions(completion.clone()).wait_async(&io));
        let (both, one) = {
            let mut future_a = std::pin::pin!(IOCompletions(completion.clone()).wait_async(&io));
            let _ = future_a.as_mut().poll(&mut Context::from_waker(&wa));
            let _ = future_b.as_mut().poll(&mut Context::from_waker(&wb));
            (completion.wakers(), completion.finished())
        };
        let after_drop = completion.wakers();
        completion.complete(0);
        let ready = future_b
            .as_mut()
            .poll(&mut Context::from_waker(&wb))
            .is_ready();
        let (aw, bw) = (a.0.load(Ordering::SeqCst), b.0.load(Ordering::SeqCst));
        eprintln!("AUDIT distinct both={both} finished_before={one} after_cancel={after_drop} ready={ready} wakes=({aw},{bw})");
        assert_eq!((both, after_drop), (2, 1));
        assert!(!one && ready && aw == 0 && bw == 1);
    }

    #[test]
    fn a_progress_wake_allows_the_same_async_wait_to_register_again() {
        let io = MemoryIO::new();
        let completion = Completion::new_remote_wait();
        let a = task();
        let waker = Waker::from(a.clone());
        let (after_progress, registered, ready) = {
            let mut future = std::pin::pin!(IOCompletions(completion.clone()).wait_async(&io));
            let _ = future.as_mut().poll(&mut Context::from_waker(&waker));
            completion.wake();
            let after_progress = a.0.load(Ordering::SeqCst);
            let _ = future.as_mut().poll(&mut Context::from_waker(&waker));
            let registered = completion.wakers();
            completion.complete(0);
            let ready = future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready();
            (after_progress, registered, ready)
        };
        let final_wakes = a.0.load(Ordering::SeqCst);
        eprintln!("AUDIT progress first_wakes={after_progress} registered_again={registered} ready={ready} final_wakes={final_wakes}");
        assert_eq!((after_progress, registered, final_wakes), (1, 1, 2));
        assert!(ready);
    }

    #[test]
    fn direct_completion_future_replaces_changed_context_waker() {
        let completion = Completion::new_remote_wait();
        let tasks: Vec<_> = (0..32).map(|_| task()).collect();
        let (registered, pending, ready) = {
            let mut future = std::pin::pin!(completion.clone().into_future());
            let mut pending = 0;
            for task in &tasks {
                let waker = Waker::from(task.clone());
                pending += usize::from(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending(),
                );
            }
            let registered = completion.wakers();
            completion.complete(0);
            let waker = Waker::from(tasks.last().unwrap().clone());
            let ready = future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready();
            (registered, pending, ready)
        };
        let wakes: Vec<_> = tasks.iter().map(|t| t.0.load(Ordering::SeqCst)).collect();
        drop(completion);
        eprintln!("AUDIT direct_changed32 pending={pending} registered={registered} ready={ready} wakes={wakes:?}");
        assert_eq!(
            registered, 1,
            "one Completion future retained every old context waker"
        );
        assert!(pending == 32 && ready && wakes[..31].iter().all(|n| *n == 0) && wakes[31] == 1);
    }

    #[test]
    fn cancelled_direct_completion_futures_unregister() {
        let completion = Completion::new_remote_wait();
        let tasks: Vec<_> = (0..32).map(|_| task()).collect();
        let mut pending = 0;
        for task in &tasks {
            let waker = Waker::from(task.clone());
            let mut future = std::pin::pin!(completion.clone().into_future());
            pending += usize::from(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending(),
            );
        }
        let registered = completion.wakers();
        completion.complete(0);
        let wakes = tasks
            .iter()
            .map(|t| t.0.load(Ordering::SeqCst))
            .sum::<usize>();
        drop(completion);
        eprintln!("AUDIT direct_cancel32 pending={pending} live_futures=0 registered={registered} stale_wakes={wakes}");
        assert_eq!((pending, registered, wakes), (32, 0, 0));
    }

    #[test]
    fn completion_await_uses_the_keyed_future_and_keeps_completion_small() {
        let completion = Completion::new_remote_wait();
        let a = task();
        let waker = Waker::from(a.clone());
        let (pending, registered, ready) = {
            let awaited = completion.clone();
            let mut future = std::pin::pin!(async move { awaited.await });
            let pending = future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending();
            let registered = completion.wakers();
            completion.complete(0);
            let ready = future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready();
            (pending, registered, ready)
        };
        let size = std::mem::size_of::<Completion>();
        eprintln!("AUDIT await pending={pending} registered={registered} ready={ready} completion_bytes={size}");
        assert!(pending && registered == 1 && ready && size == std::mem::size_of::<usize>());
    }
}

#[cfg(shuttle)]
#[test]
fn shuttle_async_wait_latest_context_is_woken_if_its_poll_is_pending() {
    use std::future::Future;
    use std::task::{Context, Wake, Waker};
    struct Calls(AtomicUsize);
    impl Wake for Calls {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    shuttle::check_random_with_seed(
        || {
            let io = MemoryIO::new();
            let completion = Completion::new_remote_wait();
            let a = std::sync::Arc::new(Calls(AtomicUsize::new(0)));
            let b = std::sync::Arc::new(Calls(AtomicUsize::new(0)));
            let wa = Waker::from(a.clone());
            let wb = Waker::from(b.clone());
            let mut future =
                std::pin::pin!(crate::types::IOCompletions(completion.clone()).wait_async(&io));
            assert!(future
                .as_mut()
                .poll(&mut Context::from_waker(&wa))
                .is_pending());
            let producer = {
                let c = completion.clone();
                crate::thread::spawn(move || c.complete(0))
            };
            let pending = future
                .as_mut()
                .poll(&mut Context::from_waker(&wb))
                .is_pending();
            producer.join().unwrap();
            if pending {
                assert!(
                    b.0.load(Ordering::SeqCst) > 0,
                    "the latest pending Context was not woken"
                );
                assert!(future
                    .as_mut()
                    .poll(&mut Context::from_waker(&wb))
                    .is_ready());
            }
        },
        0x6f11_2026,
        1000,
    );
}
