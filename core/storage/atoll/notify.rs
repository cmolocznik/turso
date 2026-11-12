//! Logical notifications held while a change scope is open on the thread (DESIGN.md 9.1.3).
//!
//! Every wait Atoll makes ([`wait`], [`remote_wait`]) hands its wakers here, whoever finishes or
//! nudges it: its result is published at once, and its wakers run at once if no change scope of
//! any pool is open on this thread, else when the thread's last one closes, so one event's results
//! are all published before any of its wakers runs. A waker Atoll delivers schedules and returns:
//! an Atoll entry or a park while this thread delivers ends the process (DESIGN.md 9.1.3,
//! *Re-entry from a waker*). Requests and groups never come here: their callbacks, their groups'
//! counts and their retirement stay where their backend finishes them.

use crate::io::{Completion, Wakers};
use crate::CompletionError;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

#[cfg(not(atoll_loom))]
crate::thread::thread_local! {
    /// Change scopes open on this thread, of every pool.
    static OPEN: Cell<usize> = const { Cell::new(0) };
    /// Whether this thread is running held wakers now: a waker that opens and closes a scope then
    /// leaves what it held to that delivery, which runs it next, rather than delivering in its
    /// own frame, so the stack never nests deliveries.
    static DELIVERING: Cell<bool> = const { Cell::new(false) };
    /// Wakers held until the thread's last scope closes, in the order held. Its storage is kept
    /// from one delivery to the next.
    static HELD: RefCell<VecDeque<Wakers>> = const { RefCell::new(VecDeque::new()) };
}

#[cfg(atoll_loom)]
loom::thread_local! {
    static OPEN: Cell<usize> = Cell::new(0);
    static DELIVERING: Cell<bool> = Cell::new(false);
    static HELD: RefCell<VecDeque<Wakers>> = RefCell::new(VecDeque::new());
}

/// A wait Atoll makes, of the stepped kind: every way its wakers run goes through [`hold`]
/// (DESIGN.md 9.1.3), so none runs inside a change scope or outside the delivery flag. Every
/// wait Atoll makes is one of these two; a plain completion stays the backends' and the callers'.
pub(crate) fn wait() -> Completion {
    Completion::new_engine_wait(hold)
}

/// A wait Atoll makes, of the remote kind ([`wait`]).
pub(crate) fn remote_wait() -> Completion {
    Completion::new_engine_remote_wait(hold)
}

/// What takers that found a holder in the way wait on, finished at the holder's let-go (DESIGN.md
/// 9.1.3, *No retry without a change*): any number share one wait. A taker registers with
/// [`wait`](Self::wait) and then tries again, so a let-go in between either finds the registration
/// or is seen let go; the holder calls [`let_go`](Self::let_go) after it lets go. A let-go no one
/// waits for is one swap, and takes no lock. The wait is of the remote kind, its let-go another
/// caller's act, unless made [`stepped`](Self::stepped).
pub(crate) struct LetGo {
    waiting: crate::storage::atoll::locks::Mutex<Option<Completion>>,
    waited: crate::sync::atomic::AtomicBool,
    /// The wait's kind: [`remote_wait`], or [`wait`] for a let-go its taker's own steps of the
    /// backend can bring.
    make: fn() -> Completion,
    /// Let-goes that found a taker waiting, for a test.
    #[cfg(test)]
    woke: crate::sync::atomic::AtomicU64,
}

impl Default for LetGo {
    fn default() -> Self {
        Self {
            waiting: Default::default(),
            waited: Default::default(),
            make: remote_wait,
            #[cfg(test)]
            woke: Default::default(),
        }
    }
}

impl LetGo {
    /// One whose let-go is a backend's: a request's retirement, which a taker waiting on a pumped
    /// backend brings by stepping it (the metadata cache's read credits, DESIGN.md 22.18). Its
    /// wait is of the stepped kind.
    pub(crate) fn stepped() -> Self {
        Self {
            make: wait,
            ..Default::default()
        }
    }

    /// The wait the next let-go finishes, registered: try again after it.
    pub(crate) fn wait(&self) -> Completion {
        let wait = self.waiting.lock().get_or_insert_with(self.make).clone();
        // Read by the let-go's swap: a let-go before it, its hold already released, is seen by
        // the taker's next try; one after it finds the wait.
        self.waited
            .swap(true, crate::sync::atomic::Ordering::AcqRel);
        wait
    }

    /// The holder has let go: whoever registered looks again.
    pub(crate) fn let_go(&self) {
        if self
            .waited
            .swap(false, crate::sync::atomic::Ordering::AcqRel)
        {
            #[cfg(test)]
            self.woke
                .fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
            let waiting = self.waiting.lock().take();
            if let Some(c) = waiting {
                finish(&c, Ok(0));
            }
        }
    }

    /// Let-goes that found a taker waiting, for a test.
    #[cfg(test)]
    pub(crate) fn woke(&self) -> u64 {
        self.woke.load(crate::sync::atomic::Ordering::Relaxed)
    }
}

/// Ends the process if this thread is running held wakers: a waker Atoll delivers schedules and
/// returns, and never enters Atoll or blocks (DESIGN.md 9.1.3, *Re-entry from a waker*). Read by
/// every entry's frame and every park.
#[inline]
pub(crate) fn refuse_while_delivering(what: &'static str) {
    if DELIVERING.with(Cell::get) {
        delivering_refused(what);
    }
}

/// A panic, so the message reaches standard error; it unwinds into the delivery, whose guard
/// ends the process ([`deliver`]).
#[cold]
#[inline(never)]
fn delivering_refused(what: &'static str) -> ! {
    panic!("{what}")
}

/// Finishes `wait`, an engine's wait, with `result`; its wakers run now, or when this thread's
/// last scope closes.
pub fn finish(wait: &Completion, result: Result<i32, CompletionError>) {
    hold(wait.finish_deferred(result));
}

/// Holds `wakers` and, with no scope open on this thread, delivers what is held now; with one open,
/// the thread's last scope to close delivers them.
pub fn hold(wakers: Wakers) {
    if wakers.is_empty() {
        return;
    }
    HELD.with(|held| held.borrow_mut().push_back(wakers));
    if OPEN.with(Cell::get) == 0 {
        deliver();
    }
}

/// Whether a change scope of any pool is open on this thread.
#[cfg(test)]
pub fn open() -> bool {
    OPEN.with(Cell::get) > 0
}

/// Whether a change scope of any pool is open on this thread: never at a park.
pub(crate) fn scope_open() -> bool {
    OPEN.with(Cell::get) > 0
}

/// A change scope opens on this thread.
pub(crate) fn enter() {
    OPEN.with(|open| open.set(open.get() + 1));
}

/// A change scope closes on this thread, its results published. If it was the last and no
/// delivery is running on the thread, every waker held runs, one batch at a time, each taken out
/// before it runs (no borrow is held across a waker), with no scope open; what a waker holds
/// meanwhile joins the queue, and the stack never holds two deliveries.
pub(crate) fn leave() {
    let left = OPEN.with(|open| {
        let n = open.get();
        debug_assert!(n > 0, "a change scope closed that was not open");
        open.set(n - 1);
        n - 1
    });
    if left == 0 {
        deliver();
    }
}

/// Runs the wakers held, one batch at a time, each taken out before it runs, with no scope open:
/// unless a delivery is running on this thread already, which runs what is held next, so the stack
/// never holds two deliveries, whether the first began at a scope's close or at a wait finished
/// with no scope open (the auditor's direct-signal chain). The flag is cleared by a guard, which
/// ends the process first if a waker unwinds: the wakers still held would never run.
fn deliver() {
    if DELIVERING.with(|d| d.replace(true)) {
        return;
    }
    struct Delivering;
    impl Drop for Delivering {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            DELIVERING.with(|d| d.set(false));
        }
    }
    let _delivering = Delivering;
    while let Some(wakers) = HELD.with(|held| held.borrow_mut().pop_front()) {
        wakers.wake();
    }
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod tests {
    use super::{enter, leave, remote_wait, DELIVERING};
    use crate::io::{Completion, WakerKey};
    use crate::CompletionError;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    /// A waker that schedules only: it counts its runs and notes any run outside a delivery.
    #[derive(Default)]
    struct Seen {
        runs: AtomicUsize,
        undelivered: AtomicBool,
    }

    impl Wake for Seen {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if !DELIVERING.with(Cell::get) {
                self.undelivered.store(true, Ordering::Relaxed);
            }
            self.runs.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Seen {
        fn runs(&self) -> usize {
            self.runs.load(Ordering::Relaxed)
        }
    }

    /// Every way an engine wait's wakers can be run (DESIGN.md 9.1.3, *Every wake of an engine
    /// wait goes through the boundary*).
    #[derive(Clone, Copy, Debug)]
    enum Signal {
        Finish,
        Complete,
        Error,
        Abort,
        Wake,
        Progress,
        LatePlain,
        LateKeyed,
    }

    const SIGNALS: [Signal; 8] = [
        Signal::Finish,
        Signal::Complete,
        Signal::Error,
        Signal::Abort,
        Signal::Wake,
        Signal::Progress,
        Signal::LatePlain,
        Signal::LateKeyed,
    ];

    /// Registers `waker` on `c` and gives `c` the signal; a late registration finishes `c` first.
    fn give(signal: Signal, c: &Completion, waker: &Waker) {
        match signal {
            Signal::LatePlain => {
                super::finish(c, Ok(0));
                c.set_waker(waker);
            }
            Signal::LateKeyed => {
                super::finish(c, Ok(0));
                c.set_keyed_waker(WakerKey::new(1).unwrap(), waker);
            }
            _ => {
                c.set_waker(waker);
                match signal {
                    Signal::Finish => super::finish(c, Ok(0)),
                    Signal::Complete => c.complete(0),
                    Signal::Error => c.error(CompletionError::ShortRead {
                        page_idx: 0,
                        expected: 1,
                        actual: 0,
                    }),
                    Signal::Abort => c.abort(),
                    Signal::Wake => c.wake(),
                    Signal::Progress => c.wake_progress(),
                    Signal::LatePlain | Signal::LateKeyed => unreachable!(),
                }
            }
        }
    }

    fn finishes(signal: Signal) -> bool {
        !matches!(signal, Signal::Wake | Signal::Progress)
    }

    #[test]
    fn every_wake_of_an_engine_wait_waits_for_an_open_scope_and_runs_once_delivered() {
        for signal in SIGNALS {
            let c = remote_wait();
            let seen = Arc::new(Seen::default());
            let waker = Waker::from(seen.clone());
            enter();
            give(signal, &c, &waker);
            assert_eq!(
                seen.runs(),
                0,
                "{signal:?}: a waker ran inside the open scope"
            );
            leave();
            assert_eq!(seen.runs(), 1, "{signal:?}: the scope's close ran it once");
            assert!(!seen.undelivered.load(Ordering::Relaxed), "{signal:?}");
            assert_eq!(
                c.finished(),
                finishes(signal),
                "{signal:?}: a nudge finished it"
            );
        }
    }

    #[test]
    fn every_wake_of_an_engine_wait_outside_a_scope_runs_once_at_once_delivered() {
        for signal in SIGNALS {
            let c = remote_wait();
            let seen = Arc::new(Seen::default());
            give(signal, &c, &Waker::from(seen.clone()));
            assert_eq!(seen.runs(), 1, "{signal:?}");
            assert!(!seen.undelivered.load(Ordering::Relaxed), "{signal:?}");
            assert_eq!(c.finished(), finishes(signal), "{signal:?}");
        }
    }

    #[test]
    fn a_finished_engine_wait_polled_is_ready_with_its_result_and_wakes_nothing_in_a_scope() {
        use std::future::{Future, IntoFuture};
        for failed in [false, true] {
            let c = remote_wait();
            if failed {
                super::finish(
                    &c,
                    Err(CompletionError::ShortRead {
                        page_idx: 0,
                        expected: 1,
                        actual: 0,
                    }),
                );
            } else {
                super::finish(&c, Ok(0));
            }
            let seen = Arc::new(Seen::default());
            let waker = Waker::from(seen.clone());
            let mut cx = std::task::Context::from_waker(&waker);
            let mut future = std::pin::pin!(c.into_future());
            enter();
            let polled = future.as_mut().poll(&mut cx);
            assert_eq!(seen.runs(), 0, "the poll woke a waker inside the scope");
            leave();
            match polled {
                std::task::Poll::Ready(r) => assert_eq!(r.is_err(), failed),
                std::task::Poll::Pending => panic!("a finished wait polled Pending"),
            }
            assert!(!seen.undelivered.load(Ordering::Relaxed));
        }
    }

    #[cfg(not(miri))]
    #[test]
    fn ready_async_waits_and_repeated_cancelled_waits_allocate_nothing() {
        use crate::types::IOCompletions;
        use std::future::Future;

        let io = crate::io::MemoryIO::new();
        let pending = remote_wait();
        let ready = Completion::new_yield();
        let seen = Arc::new(Seen::default());
        let waker = Waker::from(seen.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        {
            let mut future = std::pin::pin!(IOCompletions(pending.clone()).wait_async(&io));
            let _ = future.as_mut().poll(&mut cx);
        }
        let before = crate::storage::atoll::testing::allocations();
        let mut pending_polls = 0;
        for _ in 0..32 {
            let mut future = std::pin::pin!(IOCompletions(pending.clone()).wait_async(&io));
            pending_polls += usize::from(future.as_mut().poll(&mut cx).is_pending());
        }
        let pending_allocations = crate::storage::atoll::testing::allocations() - before;
        let before = crate::storage::atoll::testing::allocations();
        let mut ready_polls = 0;
        for _ in 0..32 {
            let mut future = std::pin::pin!(IOCompletions(ready.clone()).wait_async(&io));
            ready_polls += usize::from(future.as_mut().poll(&mut cx).is_ready());
        }
        let ready_allocations = crate::storage::atoll::testing::allocations() - before;
        let registered = pending.wakers();
        super::finish(&pending, Ok(0));
        let wakes = seen.runs();
        let completion_bytes = std::mem::size_of::<Completion>();
        let future_bytes = std::mem::size_of::<crate::io::CompletionFuture>();
        let io_future_bytes =
            std::mem::size_of::<crate::types::IOCompletionAsync<'_, crate::io::MemoryIO>>();
        drop((pending, ready, io, waker, seen));
        println!("ASYNC_WAIT_ALLOC pending_polls={pending_polls} ready_polls={ready_polls} pending_allocations={pending_allocations} ready_allocations={ready_allocations} registered={registered} wakes={wakes} completion_bytes={completion_bytes} future_bytes={future_bytes} io_future_bytes={io_future_bytes}");
        assert_eq!((pending_polls, ready_polls), (32, 32));
        assert_eq!(
            (pending_allocations, ready_allocations, registered, wakes),
            (0, 0, 0, 0)
        );
        let pointer_bytes = std::mem::size_of::<usize>();
        assert_eq!(
            (completion_bytes, future_bytes, io_future_bytes),
            (pointer_bytes, 2 * pointer_bytes, 3 * pointer_bytes)
        );
    }

    /// The finish against the registration, each paused at the step between its two halves while
    /// the other runs whole on another thread: the waker runs once, delivered, either way.
    #[test]
    fn a_finish_racing_a_registration_runs_the_waker_once_delivered_in_every_order() {
        use crate::race;
        for keyed in [false, true] {
            // The registrar paused after registering, before it tests the result.
            let c = remote_wait();
            let seen = Arc::new(Seen::default());
            let waker = Waker::from(seen.clone());
            let finisher = c.clone();
            race::at("completion.registered", move || {
                std::thread::spawn(move || super::finish(&finisher, Ok(0)))
                    .join()
                    .unwrap();
            });
            if keyed {
                c.set_keyed_waker(WakerKey::new(1).unwrap(), &waker);
            } else {
                c.set_waker(&waker);
            }
            assert!(!race::pending("completion.registered"));
            assert_eq!(seen.runs(), 1, "keyed={keyed}: registrar paused");
            assert!(!seen.undelivered.load(Ordering::Relaxed));

            // The finisher paused after publishing, before it takes the wakers.
            let c = remote_wait();
            let seen = Arc::new(Seen::default());
            let waker = Waker::from(seen.clone());
            let registrar = c.clone();
            race::at("completion.published", move || {
                std::thread::spawn(move || {
                    if keyed {
                        registrar.set_keyed_waker(WakerKey::new(1).unwrap(), &waker);
                    } else {
                        registrar.set_waker(&waker);
                    }
                })
                .join()
                .unwrap();
            });
            super::finish(&c, Ok(0));
            assert!(!race::pending("completion.published"));
            assert_eq!(seen.runs(), 1, "keyed={keyed}: finisher paused");
            assert!(!seen.undelivered.load(Ordering::Relaxed));
        }
    }

    /// A waker's clone and drop never run under the completion's lock (DESIGN.md 9.1.3): a raw
    /// waker whose clone and drop look at that lock sees it free every time, for a registration,
    /// a duplicate registration (its clone dropped unused) and a keyed one.
    #[test]
    fn a_waker_is_cloned_and_its_unused_clone_dropped_outside_the_completions_lock() {
        use std::task::{RawWaker, RawWakerVTable};
        thread_local! {
            static TARGET: std::cell::RefCell<Option<Completion>> = const { std::cell::RefCell::new(None) };
            static UNDER_LOCK: Cell<usize> = const { Cell::new(0) };
            static CALLS: Cell<usize> = const { Cell::new(0) };
        }
        fn look() {
            CALLS.with(|n| n.set(n.get() + 1));
            let held = TARGET.with(|t| t.borrow().as_ref().is_some_and(Completion::registry_held));
            if held {
                UNDER_LOCK.with(|n| n.set(n.get() + 1));
            }
        }
        fn clone_raw(_: *const ()) -> RawWaker {
            look();
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        fn wake_raw(_: *const ()) {
            look();
        }
        fn wake_by_ref_raw(_: *const ()) {}
        fn drop_raw(_: *const ()) {
            look();
        }
        static VTABLE: RawWakerVTable =
            RawWakerVTable::new(clone_raw, wake_raw, wake_by_ref_raw, drop_raw);
        let c = remote_wait();
        TARGET.with(|t| *t.borrow_mut() = Some(c.clone()));
        // SAFETY: the vtable's functions touch no data through the pointer, which is null.
        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
        c.set_waker(&waker);
        c.set_waker(&waker);
        c.set_keyed_waker(WakerKey::new(1).unwrap(), &waker);
        c.set_keyed_waker(WakerKey::new(1).unwrap(), &waker);
        let registered = c.wakers();
        let (calls, under_lock) = (CALLS.with(Cell::get), UNDER_LOCK.with(Cell::get));
        // Everything let go before any assertion, so a failing one unwinds through nothing of
        // Atoll's.
        TARGET.with(|t| t.borrow_mut().take());
        drop((c, waker));
        assert_eq!(registered, 2);
        assert!(calls >= 6, "the vtable was called {calls} times");
        assert_eq!(under_lock, 0, "a clone or drop ran under the lock");
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn a_waker_that_enters_atoll_ends_the_process() {
        use crate::storage::atoll::testing::in_child;
        let ended = in_child(
            concat!(
                module_path!(),
                "::a_waker_that_enters_atoll_ends_the_process"
            ),
            || {
                struct Enters;
                impl Wake for Enters {
                    fn wake(self: Arc<Self>) {
                        let _frame = crate::storage::atoll::unwind::Frame::enter();
                    }
                }
                let c = remote_wait();
                c.set_waker(&Waker::from(Arc::new(Enters)));
                super::finish(&c, Ok(0));
            },
        );
        let Some(ended) = ended else { return };
        assert!(ended.aborted, "{:?}\n{}", ended.code, ended.stderr);
        assert!(
            ended
                .stderr
                .contains("an Atoll entry from a waker Atoll delivers"),
            "{}",
            ended.stderr
        );
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn a_waker_that_parks_ends_the_process() {
        use crate::storage::atoll::testing::in_child;
        let ended = in_child(
            concat!(module_path!(), "::a_waker_that_parks_ends_the_process"),
            || {
                struct Parks;
                impl Wake for Parks {
                    fn wake(self: Arc<Self>) {
                        crate::storage::atoll::park::thread();
                    }
                }
                let c = remote_wait();
                c.set_waker(&Waker::from(Arc::new(Parks)));
                super::finish(&c, Ok(0));
            },
        );
        let Some(ended) = ended else { return };
        assert!(ended.aborted, "{:?}\n{}", ended.code, ended.stderr);
        assert!(
            ended
                .stderr
                .contains("a park on a thread delivering held wakers"),
            "{}",
            ended.stderr
        );
    }

    #[cfg(all(unix, not(miri)))]
    #[test]
    fn a_park_inside_an_open_change_scope_ends_the_process() {
        use crate::storage::atoll::testing::in_child;
        let ended = in_child(
            concat!(
                module_path!(),
                "::a_park_inside_an_open_change_scope_ends_the_process"
            ),
            || {
                let _frame = crate::storage::atoll::unwind::Frame::enter();
                enter();
                crate::storage::atoll::park::thread();
            },
        );
        let Some(ended) = ended else { return };
        assert!(ended.aborted, "{:?}\n{}", ended.code, ended.stderr);
        assert!(
            ended
                .stderr
                .contains("a thread parked with a change scope open"),
            "{}",
            ended.stderr
        );
    }
}
