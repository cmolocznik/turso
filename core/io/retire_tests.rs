//! A request's ownership apart from its finishing (DESIGN.md 22.5): what a drain waits for, a
//! group's aggregate, submissions that accept or refuse whole.

use super::crash::{CrashIO, Faults};
use super::*;
use crate::sync::atomic::{AtomicUsize, Ordering};

fn read(len: usize) -> Completion {
    Completion::new_read(Arc::new(Buffer::new_temporary(len)), |_| None)
}

fn write() -> Completion {
    Completion::new_write(|_| {})
}

fn buffer(len: usize) -> Arc<Buffer> {
    Arc::new(Buffer::new_temporary(len))
}

/// Runs `wait` on another thread, failing if it has not returned within `bound`: a wait that
/// never returns fails the test rather than hanging it.
fn returns_within(bound: std::time::Duration, wait: impl FnOnce() + Send + 'static) -> bool {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        wait();
        let _ = tx.send(());
    });
    rx.recv_timeout(bound).is_ok()
}

/// A cancel finishes a request a pumped backend still holds: the drain after it waits for the
/// backend to let it go, not for the finish (mutant: drained to finishing, the buffer still the
/// backend's).
#[test]
fn a_cancelled_request_is_drained_to_its_retirement() {
    let io = CrashIO::new(1);
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let c = file.pread(0, read(512)).unwrap();
    io.cancel(std::slice::from_ref(&c)).unwrap();
    assert!(c.finished() && c.is_owned());
    io.drain_completions(std::slice::from_ref(&c)).unwrap();
    assert!(!c.is_owned(), "drained while the backend still owns it");
    assert_eq!(io.in_flight(), 0);
}

/// A group's ownership is the requests' beneath it: an outer group that lists only an inner
/// group, whose own completion no backend ever takes, is owned while a write under the inner one
/// is (mutant: a flat walk that takes the inner group for a request never submitted).
#[test]
fn a_drain_walks_a_group_down_to_every_request_beneath_it() {
    let io = CrashIO::new(2);
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let mut inner = CompletionGroup::new(|_| {});
    let w = write();
    inner.add(&w);
    drop(file.pwrite(0, buffer(512), w.clone()).unwrap());
    let inner = inner.build();
    let mut outer = CompletionGroup::new(|_| {});
    outer.add(&inner);
    let outer = outer.build();
    io.cancel(std::slice::from_ref(&w)).unwrap();
    assert!(outer.finished(), "the cancel did not finish the groups");
    io.wait_retired(&outer).unwrap();
    assert!(
        !w.is_owned(),
        "the drain left the write under the inner group owned"
    );
    assert_eq!(io.in_flight(), 0);
}

/// A drain from the outermost group reaches a request two groups down after every handle to the
/// groups between is gone: the cancel finishes them and drops each one's link to its group, and
/// the request is still the backend's (mutant: a group holding the groups it lists as weakly as
/// its requests, so the walk finds nothing beneath them).
#[test]
fn a_drain_reaches_a_request_beneath_groups_nobody_holds() {
    let io = CrashIO::new(5);
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let w = write();
    let mut inner = CompletionGroup::new(|_| {});
    inner.add(&w);
    drop(file.pwrite(0, buffer(512), w.clone()).unwrap());
    let inner = inner.build();
    let mut middle = CompletionGroup::new(|_| {});
    middle.add(&inner);
    let middle = middle.build();
    let mut outer = CompletionGroup::new(|_| {});
    outer.add(&middle);
    let outer = outer.build();
    let (inner_gone, middle_gone) = (
        Arc::downgrade(inner.get_inner()),
        Arc::downgrade(middle.get_inner()),
    );
    drop((inner, middle));
    io.cancel(std::slice::from_ref(&w)).unwrap();
    assert!(outer.finished() && w.is_owned());
    assert!(
        inner_gone.upgrade().is_none() && middle_gone.upgrade().is_none(),
        "a group between is still held, so the test reaches nothing it means to"
    );
    let mut seen = std::collections::HashSet::new();
    assert_eq!(
        outer.requests(&mut seen).len(),
        1,
        "the walk lost the write beneath the groups nobody holds"
    );
    io.wait_retired(&outer).unwrap();
    assert!(
        !w.is_owned(),
        "the drain returned with the write beneath it still owned"
    );
    assert_eq!(io.in_flight(), 0);
}

/// A request added to a group and never accepted (its submission refused, or never made) holds
/// no drain: no backend owns it (mutant: the drain waiting for a retirement nobody will make).
#[test]
fn a_child_never_accepted_holds_no_drain() {
    let io = Arc::new(CrashIO::new(3));
    let file = io
        .open_file("/db/f", OpenFlags::Create | OpenFlags::Direct, true)
        .unwrap();
    let mut group = CompletionGroup::new(|_| {});
    let refused = read(DIRECT_IO_ALIGNMENT + 1);
    group.add(&refused);
    assert!(file.pread(0, refused.clone()).is_err());
    let never = read(DIRECT_IO_ALIGNMENT);
    group.add(&never);
    group.cancel();
    let completions = group.completions().to_vec();
    let drainer = io.clone();
    assert!(
        returns_within(std::time::Duration::from_secs(10), move || drainer
            .drain_completions(&completions)
            .unwrap()),
        "the drain waits on requests nobody accepted"
    );
}

/// A request reached twice by one drain, directly and through its group, is waited for once.
#[test]
fn a_drain_counts_a_request_reached_twice_once() {
    let io = CrashIO::new(4);
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let mut group = CompletionGroup::new(|_| {});
    let w = write();
    group.add(&w);
    drop(file.pwrite(0, buffer(512), w.clone()).unwrap());
    let g = group.build();
    let mut seen = std::collections::HashSet::new();
    let mut found = g.requests(&mut seen);
    found.extend(w.requests(&mut seen));
    assert_eq!(found.len(), 1);
    io.drain_completions(&[g, w.clone()]).unwrap();
    assert!(!w.is_owned());
}

/// What waits for a retirement runs once nobody owns the request: at once for one never
/// submitted or given back, at the retirement for one accepted.
#[test]
fn what_waits_for_a_retirement_runs_once_nobody_owns_the_request() {
    let ran = Arc::new(AtomicUsize::new(0));
    let counter = |ran: &Arc<AtomicUsize>| {
        let ran = ran.clone();
        move || {
            ran.fetch_add(1, Ordering::SeqCst);
        }
    };
    let c = read(512);
    c.on_retired(counter(&ran));
    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "not run for a request never submitted"
    );
    let c = read(512);
    c.accept();
    c.on_retired(counter(&ran));
    assert_eq!(ran.load(Ordering::SeqCst), 1, "run while a backend owns it");
    c.refuse();
    assert_eq!(
        ran.load(Ordering::SeqCst),
        2,
        "not run when it was given back"
    );
    let c = read(512);
    c.accept();
    c.on_retired(counter(&ran));
    c.abort();
    assert_eq!(ran.load(Ordering::SeqCst), 2, "run at its finishing");
    // Registered once finished, as a cancelled request the backend still holds is.
    c.on_retired(counter(&ran));
    assert_eq!(
        ran.load(Ordering::SeqCst),
        2,
        "run at once for a finished request a backend still owns"
    );
    c.retire();
    assert_eq!(ran.load(Ordering::SeqCst), 4, "not run at its retirement");
}

/// Which of a cancellation's callback and the backend's delivery of the request ends first.
#[derive(Clone, Copy, Debug)]
enum FirstToEnd {
    /// The delivery, while the cancellation still runs the callback it claimed.
    Delivery,
    /// The cancellation's callback, before the backend delivers.
    Callback,
}

/// A cancellation claims a request's finish and runs its callback while the pumped backend
/// delivers the request: the delivery returns at once whichever ends first, the request stays
/// owned until both have, and it retires once, its callback run once and its result the
/// cancellation's (audit cancel-claim-v1, P1-CC1; mutant: a delivery that lost the callback
/// retires at once, before that callback returned).
fn a_cancellation_and_a_delivery_meet_once(first: FirstToEnd) {
    use std::sync::mpsc;
    use std::time::Duration;
    const BOUND: Duration = Duration::from_secs(20);
    let io = Arc::new(CrashIO::new(1));
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let go_rx = std::sync::Mutex::new(go_rx);
    let calls = Arc::new(AtomicUsize::new(0));
    let c = {
        let calls = calls.clone();
        Completion::new_sync(move |res| {
            calls.fetch_add(1, Ordering::SeqCst);
            if res.is_err() {
                entered_tx.send(()).unwrap();
                go_rx.lock().unwrap().recv().unwrap();
            }
        })
    };
    let c = file.sync(c, FileSyncType::Fsync).unwrap();
    assert!(
        c.is_owned() && !c.finished(),
        "the pumped backend holds the sync"
    );
    let retired = Arc::new(AtomicUsize::new(0));
    {
        let retired = retired.clone();
        c.on_retired(move || {
            retired.fetch_add(1, Ordering::SeqCst);
        });
    }
    let cancel = {
        let c = c.clone();
        std::thread::spawn(move || c.abort())
    };
    entered_rx
        .recv_timeout(BOUND)
        .expect("the cancellation entered the callback");
    let deliver = {
        let io = io.clone();
        move || io.step().unwrap()
    };
    let (delivered, mid) = match first {
        FirstToEnd::Delivery => {
            // On another thread, so a delivery that waited for the callback is seen.
            let delivered = returns_within(BOUND, deliver);
            let mid = (c.is_owned(), retired.load(Ordering::SeqCst));
            assert!(
                !c.finished(),
                "{first:?}: published before its callback returned"
            );
            go_tx.send(()).unwrap();
            cancel.join().unwrap();
            (delivered, mid)
        }
        FirstToEnd::Callback => {
            go_tx.send(()).unwrap();
            cancel.join().unwrap();
            let mid = (c.is_owned(), retired.load(Ordering::SeqCst));
            (returns_within(BOUND, deliver), mid)
        }
    };
    assert!(delivered, "{first:?}: the delivery did not return");
    assert_eq!(
        mid,
        (true, 0),
        "{first:?}: (owned, retirements) before the second end"
    );
    assert_eq!(
        retired.load(Ordering::SeqCst),
        1,
        "{first:?}: retirements once both ended"
    );
    assert!(!c.is_owned(), "{first:?}: owned once both ended");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "{first:?}: callbacks run");
    assert!(
        matches!(c.get_error(), Some(crate::CompletionError::Aborted)),
        "{first:?}: the result kept: {:?}",
        c.get_error()
    );
    assert_eq!(io.in_flight(), 0, "{first:?}: operations in flight");
}

#[test]
fn a_delivery_meeting_a_cancellation_still_in_its_callback_leaves_it_the_retirement() {
    a_cancellation_and_a_delivery_meet_once(FirstToEnd::Delivery);
}

#[test]
fn a_delivery_after_a_cancellations_callback_returned_retires_the_request() {
    a_cancellation_and_a_delivery_meet_once(FirstToEnd::Callback);
}

/// What a waiter waits for on a pumped backend.
#[derive(Clone, Copy, Debug)]
enum Waits {
    /// The request's result ([`IO::wait`]).
    Result,
    /// Its retirement ([`IO::wait_retired`]).
    Retirement,
}

/// What a waiter waits on.
#[derive(Clone, Copy, Debug)]
enum Through {
    /// The request itself.
    Request,
    /// A group holding it, whose own finish is not yet claimed.
    Group,
    /// A request carried through it ([`Completion::carry`]), which its callback finishes and its
    /// retirement retires.
    Carried,
}

/// A pumped backend has delivered a request whose callback a cancellation runs, paused, on
/// another thread: only that finish can publish its result and retire it. A waiter for either,
/// on the request, on a group holding it or on a request carried through it, looks once, finds
/// the backend empty and parks, and the finish wakes it; ended, it has taken back the
/// registration its look left (audit chain-fixes-v1, P2-CF1, the auditor's group and carry
/// controls, and P2-X69-2; mutants: the waiter steps on an empty backend until the finish lands, a
/// core spent on empty steps; a wait that leaves its registration with the backend).
fn a_pumped_waiter_parks_while_only_a_finish_elsewhere_can_end_its_wait(
    waits: Waits,
    through: Through,
) {
    use std::sync::mpsc;
    use std::time::Duration;
    const BOUND: Duration = Duration::from_secs(20);
    let crash = Arc::new(CrashIO::new(1));
    let io = super::wait_tests::StepCounting::new(crash.clone());
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let go_rx = std::sync::Mutex::new(go_rx);
    let carried = Completion::new_sync(|_| {});
    let c = {
        let carried = carried.clone();
        Completion::new_sync(move |res| {
            if res.is_err() {
                entered_tx.send(()).unwrap();
                go_rx.lock().unwrap().recv().unwrap();
            }
            if let Through::Carried = through {
                match res {
                    Ok(n) => carried.complete(n),
                    Err(e) => carried.error(e),
                }
            }
        })
    };
    let mut group = CompletionGroup::new(|_| {});
    group.add(&c);
    let group = group.build();
    let c = match through {
        Through::Carried => carried.carry(|| file.sync(c, FileSyncType::Fsync)),
        Through::Request | Through::Group => file.sync(c, FileSyncType::Fsync),
    }
    .unwrap();
    let cancel = {
        let c = c.clone();
        std::thread::spawn(move || c.abort())
    };
    entered_rx
        .recv_timeout(BOUND)
        .expect("the cancellation entered the callback");
    crash.step().unwrap();
    assert!(
        crash.in_flight() == 0 && c.is_owned() && !c.finished(),
        "the backend let the request go, its callback still running"
    );
    assert!(!group.finished(), "the group's own finish is not claimed");
    let target = match through {
        Through::Request => c.clone(),
        Through::Group => group.clone(),
        Through::Carried => carried.clone(),
    };
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = {
        let io = io.clone();
        std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            match waits {
                Waits::Result => io.wait(&target),
                Waits::Retirement => io.wait_retired(&target).unwrap(),
            }
            done_tx.send(()).unwrap();
        })
    };
    started_rx.recv().unwrap();
    // Long enough for the waiter to reach its wait, which takes microseconds, and for a waiter
    // that steps to step many times.
    std::thread::sleep(Duration::from_millis(100));
    let mid = (io.steps(), done_rx.try_recv().is_ok());
    go_tx.send(()).unwrap();
    cancel.join().unwrap();
    let ended = done_rx.recv_timeout(BOUND).is_ok();
    waiter.join().unwrap();
    // Nothing was submitted after its look: only the waiter itself takes its registration back.
    let left = crash.registered();
    assert_eq!(
        mid,
        (1, false),
        "{waits:?} through {through:?}: (looks, returned) while the finish was paused"
    );
    assert_eq!(
        left, 0,
        "{waits:?} through {through:?}: a registration left behind by the wait"
    );
    assert!(
        ended,
        "{waits:?} through {through:?}: the finish did not wake the waiter"
    );
    assert!(
        c.finished() && !c.is_owned() && group.finished(),
        "{waits:?} through {through:?}: the request and its group ended"
    );
    if let Through::Carried = through {
        assert!(
            carried.finished() && !carried.is_owned(),
            "{waits:?}: the carried request ended"
        );
    }
    assert_eq!(
        io.steps(),
        1,
        "{waits:?} through {through:?}: looks taken in all"
    );
}

#[test]
fn a_pumped_wait_for_a_result_only_a_finish_elsewhere_can_publish_parks() {
    a_pumped_waiter_parks_while_only_a_finish_elsewhere_can_end_its_wait(
        Waits::Result,
        Through::Request,
    );
}

#[test]
fn a_pumped_wait_for_a_retirement_only_a_finish_elsewhere_can_make_parks() {
    a_pumped_waiter_parks_while_only_a_finish_elsewhere_can_end_its_wait(
        Waits::Retirement,
        Through::Request,
    );
}

#[test]
fn a_pumped_wait_on_a_group_for_its_result_only_a_finish_elsewhere_can_publish_parks() {
    a_pumped_waiter_parks_while_only_a_finish_elsewhere_can_end_its_wait(
        Waits::Result,
        Through::Group,
    );
}

#[test]
fn a_pumped_wait_on_a_group_for_its_retirement_only_a_finish_elsewhere_can_make_parks() {
    a_pumped_waiter_parks_while_only_a_finish_elsewhere_can_end_its_wait(
        Waits::Retirement,
        Through::Group,
    );
}

#[test]
fn a_pumped_wait_on_a_carried_request_for_its_result_only_its_carriers_finish_can_publish_parks() {
    a_pumped_waiter_parks_while_only_a_finish_elsewhere_can_end_its_wait(
        Waits::Result,
        Through::Carried,
    );
}

#[test]
fn a_pumped_wait_on_a_carried_request_for_its_retirement_only_its_carriers_finish_can_make_parks() {
    a_pumped_waiter_parks_while_only_a_finish_elsewhere_can_end_its_wait(
        Waits::Retirement,
        Through::Carried,
    );
}

/// A waiter parked on an empty pumped backend steps the request another thread submits next,
/// whose callback finishes what it waits for: the submission wakes it (DESIGN.md 22.5; mutant:
/// a backend that registers no waiter, or a submission that wakes none, leaves it parked).
fn a_pumped_wait_on_an_empty_backend_steps_the_request_submitted_next(backend: Arc<dyn IO>) {
    use std::sync::mpsc;
    use std::time::Duration;
    const BOUND: Duration = Duration::from_secs(20);
    let io = super::wait_tests::StepCounting::new(backend.clone());
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let waited = Completion::new_sync(|_| {});
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = {
        let (io, waited) = (io.clone(), waited.clone());
        std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            io.wait(&waited);
            done_tx.send(()).unwrap();
        })
    };
    started_rx.recv().unwrap();
    // Long enough for the waiter to look and park.
    std::thread::sleep(Duration::from_millis(100));
    let parked = (io.steps(), done_rx.try_recv().is_ok());
    let next = {
        let waited = waited.clone();
        Completion::new_sync(move |res| match res {
            Ok(n) => waited.complete(n),
            Err(e) => waited.error(e),
        })
    };
    let next = file.sync(next, FileSyncType::Fsync).unwrap();
    let woke = done_rx.recv_timeout(BOUND).is_ok();
    if !woke {
        // A waiter nothing woke: step it out here so the thread ends.
        while !waited.finished() {
            backend.step().unwrap();
        }
        waiter.thread().unpark();
    }
    waiter.join().unwrap();
    assert_eq!(
        parked,
        (1, false),
        "(looks, returned) before the submission"
    );
    assert!(woke, "the submission did not wake the parked waiter");
    assert!(next.finished() && !next.is_owned() && waited.finished());
}

#[test]
fn a_pumped_wait_on_an_empty_crash_io_steps_the_request_submitted_next() {
    a_pumped_wait_on_an_empty_backend_steps_the_request_submitted_next(Arc::new(CrashIO::new(2)));
}

#[cfg(feature = "io_memory_yield")]
#[test]
fn a_pumped_wait_on_an_empty_yielding_memory_io_steps_the_request_submitted_next() {
    a_pumped_wait_on_an_empty_backend_steps_the_request_submitted_next(Arc::new(
        MemoryYieldIO::new(),
    ));
}

/// A file whose `pwrite` refuses its `refuse_at`th call and passes the rest to a file of a
/// backend, so that `pwritev` takes the trait's default.
struct RefusingAt {
    file: Arc<dyn File>,
    calls: AtomicUsize,
    refuse_at: usize,
}

impl File for RefusingAt {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.file.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.file.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.file.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == self.refuse_at {
            return Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::Other,
                "refused",
            )));
        }
        self.file.pwrite(pos, buffer, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.file.sync(c, sync_type)
    }
    fn size(&self) -> Result<u64> {
        self.file.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.file.truncate(len, c)
    }
}

fn refusing_at(io: &CrashIO, refuse_at: usize) -> RefusingAt {
    RefusingAt {
        file: io.open_file("/db/f", OpenFlags::Create, false).unwrap(),
        calls: AtomicUsize::new(0),
        refuse_at,
    }
}

/// A vectored write whose first part is accepted and second refused is accepted whole: it
/// carries the refusal as its error once its accepted part has landed, and is owned until that
/// part retires.
#[test]
fn a_vectored_write_refused_after_its_first_part_is_accepted_whole() {
    let io = CrashIO::new(5);
    let file = refusing_at(&io, 1);
    let c = file
        .pwritev(0, vec![buffer(512), buffer(512), buffer(512)], write())
        .expect("a submission that accepted a part refused");
    assert!(c.is_owned() && !c.finished());
    assert_eq!(
        io.in_flight(),
        1,
        "a part past the refused one was submitted"
    );
    io.step().unwrap();
    assert!(
        matches!(c.get_error(), Some(CompletionError::IOError(_, "refused"))),
        "the refusal not reported: {:?}",
        c.get_error()
    );
    assert!(!c.is_owned(), "owned once its only accepted part retired");
}

/// Refused at its first part, a vectored write accepted nothing.
#[test]
fn a_vectored_write_refused_at_its_first_part_accepted_nothing() {
    let io = CrashIO::new(6);
    let file = refusing_at(&io, 0);
    let c = write();
    assert!(file
        .pwritev(0, vec![buffer(512), buffer(512)], c.clone())
        .is_err());
    assert!(!c.is_owned() && !c.finished());
    assert_eq!(io.in_flight(), 0);
}

/// A vectored write whose part fails as it lands finishes with that error, and retires once
/// every part has.
#[test]
fn a_vectored_write_whose_part_fails_finishes_with_its_error() {
    let io = CrashIO::new(7);
    io.set_faults(Faults {
        write: 1.0,
        ..Faults::default()
    });
    let file = refusing_at(&io, usize::MAX);
    let c = file
        .pwritev(0, vec![buffer(512), buffer(512)], write())
        .unwrap();
    while io.in_flight() > 0 {
        io.step().unwrap();
    }
    assert!(
        c.failed(),
        "a failed part left the write unfinished or whole"
    );
    assert!(!c.is_owned());
}

/// A request carried out through another is owned until the other retires, and given back
/// whole if the other is refused.
#[test]
fn a_request_carried_through_another_is_owned_until_the_other_retires() {
    let io = CrashIO::new(8);
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let outer = read(512);
    let inner = {
        let outer = outer.clone();
        Completion::new_read(Arc::new(Buffer::new_temporary(512)), move |r| {
            match r {
                Ok((_, n)) => outer.complete(n),
                Err(e) => outer.error(e),
            }
            None
        })
    };
    let inner = outer.carry(|| file.pread(0, inner)).unwrap();
    assert!(outer.is_owned() && inner.is_owned());
    outer.abort();
    assert!(
        outer.is_owned(),
        "a finish let go of what the backend holds"
    );
    io.step().unwrap();
    assert!(!inner.is_owned() && !outer.is_owned());
    // Refused: a misaligned read on a direct file is turned away before the backend takes it.
    let direct = io
        .open_file("/db/d", OpenFlags::Create | OpenFlags::Direct, true)
        .unwrap();
    let outer = read(512);
    let inner = read(DIRECT_IO_ALIGNMENT + 1);
    assert!(outer.carry(|| direct.pread(0, inner)).is_err());
    assert!(!outer.is_owned() && !outer.finished());
}

/// The yielding memory IO owns every request it defers until the step that delivers it.
#[cfg(feature = "io_memory_yield")]
#[test]
fn a_deferred_request_is_owned_until_the_step_that_delivers_it() {
    let io = MemoryYieldIO::new();
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let c = file.pwrite(0, buffer(512), write()).unwrap();
    c.abort();
    assert!(c.is_owned());
    io.drain_completions(std::slice::from_ref(&c)).unwrap();
    assert!(!c.is_owned());
}

/// A request a queueing layer accepted and then hands on to another backend is retired by the
/// backend that delivers it, an inline one included, as its submission returns.
#[test]
fn a_request_handed_on_is_retired_by_the_backend_that_delivers_it() {
    let io = MemoryIO::new();
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let c = read(512);
    // The layer's submission: accepted and held.
    c.accept();
    // Its step: handed on.
    let c = file.pread(0, c).unwrap();
    assert!(c.finished());
    assert!(
        !c.is_owned(),
        "the inline backend that delivered it did not let it go"
    );
}

/// An inline backend delivers inside the submission and lets the request go at its return.
#[test]
fn an_inline_request_is_retired_when_its_submission_returns() {
    let io = MemoryIO::new();
    let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
    let c = file.pwrite(0, buffer(512), write()).unwrap();
    assert!(c.finished() && !c.is_owned());
    let c = file
        .pwritev(0, vec![buffer(512), buffer(512)], write())
        .unwrap();
    assert!(c.finished() && !c.is_owned());
}

/// A step hook that fails the first step and, should anyone step again, ends the child with code
/// 0 and says so: one failed step is enough for the rule, so a second is the defect.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn failing_steps(io: &CrashIO) {
    let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    io.on_step(Some(Arc::new(move || {
        if failed.swap(true, Ordering::SeqCst) {
            crate::child_test::record("backend-step", "retried");
            std::process::exit(0);
        }
        crate::child_test::record("backend-step", "failed");
        Err(crate::LimboError::InternalError(
            "the backend's stepping failed".into(),
        ))
    })));
}

/// Runs `wait` on a read a CrashIO accepted, in a child whose steps fail, and asserts the child
/// ended by abort, in the wait `during` names.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn a_failed_step_under(test: &str, during: &str, wait: fn(&CrashIO, &Completion) -> Result<()>) {
    let Some(ended) = crate::child_test::in_child(test, || {
        let io = CrashIO::new(6);
        let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
        let c = file.pread(0, read(512)).unwrap();
        failing_steps(&io);
        let returned = wait(&io, &c);
        eprintln!("the wait returned {returned:?}, owned: {}", c.is_owned());
    }) else {
        return;
    };
    assert!(
        ended.aborted && !ended.hung,
        "a failed step under {during} did not end the process: hung {}, code {:?}: {}",
        ended.hung,
        ended.code,
        ended.stderr
    );
}

/// Makes this process's standard error what a host's is whose reader has stopped reading: a
/// blocking pipe with no room, where a write waits for ever. The pipe's read end stays open and
/// unread, so a write neither fails nor raises SIGPIPE. Proved full: a nonblocking write of one byte
/// finds no room.
#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    not(any(shuttle, atoll_loom, miri))
))]
pub(crate) fn stderr_a_full_pipe() {
    let mut fds = [0; 2];
    // SAFETY: pipe(2) into a two-descriptor array; fcntl, write and dup2 on the descriptors it
    // made, writing from live buffers.
    unsafe {
        assert_eq!(libc::pipe(fds.as_mut_ptr()), 0);
        let [read_end, write_end] = fds;
        let flags = libc::fcntl(write_end, libc::F_GETFL);
        assert_eq!(
            libc::fcntl(write_end, libc::F_SETFL, flags | libc::O_NONBLOCK),
            0
        );
        let page = [0u8; 4096];
        while libc::write(write_end, page.as_ptr().cast(), page.len()) > 0 {}
        let byte = [0u8; 1];
        while libc::write(write_end, byte.as_ptr().cast(), 1) > 0 {}
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EAGAIN),
            "the pipe was not filled"
        );
        assert_eq!(libc::fcntl(write_end, libc::F_SETFL, flags), 0);
        assert_eq!(libc::dup2(write_end, 2), 2);
        libc::close(write_end);
        // Never read and never closed: the child ends with it.
        let _ = read_end;
    }
}

/// A step that fails ends the process while another thread holds standard error and waits for the
/// failing one, as a host that prints and then joins its workers does: nothing is written before the
/// abort (DESIGN.md 22.11; mutant: a message printed through the lock, which waits on
/// its holder while the holder waits on it, so the child never ends).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_step_that_fails_ends_the_process_while_another_thread_holds_stderr() {
    let Some(ended) = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_step_that_fails_ends_the_process_while_another_thread_holds_stderr"
        ),
        || {
            let io = Arc::new(CrashIO::new(7));
            let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
            let c = file.pread(0, read(512)).unwrap();
            failing_steps(&io);
            let _stderr = std::io::stderr().lock();
            let waiter = std::thread::spawn(move || io.wait_for_completion(c));
            let _ = waiter.join();
        },
    ) else {
        return;
    };
    assert!(
        ended.aborted && !ended.hung,
        "the failed step did not end the process: hung {}, code {:?}",
        ended.hung,
        ended.code
    );
}

/// A step that fails ends the process with standard error a full pipe whose reader has stopped
/// reading: nothing is written before the abort, and the process ends all the same (DESIGN.md
/// 22.11; mutant: a message written to standard error by a write that waits for room, which never
/// comes).
#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    not(any(shuttle, atoll_loom, miri))
))]
#[test]
fn a_step_that_fails_ends_the_process_with_stderr_a_full_pipe() {
    let Some(ended) = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_step_that_fails_ends_the_process_with_stderr_a_full_pipe"
        ),
        || {
            let io = CrashIO::new(8);
            let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
            let c = file.pread(0, read(512)).unwrap();
            failing_steps(&io);
            stderr_a_full_pipe();
            let _ = io.wait_for_completion(c);
        },
    ) else {
        return;
    };
    assert!(
        ended.aborted && !ended.hung,
        "the failed step did not end the process: hung {}, code {:?}",
        ended.hung,
        ended.code
    );
}

/// A step that fails under a wait for finishing ends the process (DESIGN.md 22.5): the backend
/// cannot deliver what it accepted, so the wait can neither go on nor return while the backend may
/// own the request's buffer (mutant: the error returned to the waiter, or the step tried again).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_step_that_fails_under_a_wait_for_finishing_ends_the_process() {
    a_failed_step_under(
        concat!(
            module_path!(),
            "::a_step_that_fails_under_a_wait_for_finishing_ends_the_process"
        ),
        "a wait for a completion",
        |io, c| io.wait_for_completion(c.clone()),
    );
}

/// The same under a wait for retirement (mutant: the error returned to the drainer, whose caller
/// then frees what the backend may still own).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_step_that_fails_under_a_wait_for_retirement_ends_the_process() {
    a_failed_step_under(
        concat!(
            module_path!(),
            "::a_step_that_fails_under_a_wait_for_retirement_ends_the_process"
        ),
        "a wait for retirement",
        |io, c| io.drain_completions(std::slice::from_ref(c)),
    );
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_backend_step_failure_ends_an_async_wait() {
    let Some(ended) = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_backend_step_failure_ends_an_async_wait"
        ),
        || {
            use std::future::Future;
            let io = CrashIO::new(9);
            let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
            let request = file.pread(0, read(512)).unwrap();
            assert!(request.is_owned() && !request.finished());
            failing_steps(&io);
            let mut future = std::pin::pin!(crate::types::IOCompletions(request).wait_async(&io));
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            crate::child_test::record("target", "ready");
            #[cfg(any(target_os = "linux", target_os = "android"))]
            stderr_a_full_pipe();
            let _ = future.as_mut().poll(&mut context);
            crate::child_test::record("target", "returned");
            std::process::exit(0);
        },
    ) else {
        return;
    };
    assert_eq!(
        crate::child_test::recorded(&ended, "backend-step"),
        "failed\n"
    );
    assert_eq!(crate::child_test::recorded(&ended, "target"), "ready\n");
    assert!(
        ended.aborted && !ended.hung,
        "the async wait did not abort: hung {}, code {:?}",
        ended.hung,
        ended.code
    );
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_backend_step_failure_ends_a_statements_yield_pump() {
    let Some(ended) = crate::child_test::in_child(
        concat!(
            module_path!(),
            "::a_backend_step_failure_ends_a_statements_yield_pump"
        ),
        || {
            let io = Arc::new(CrashIO::new(10));
            let db = crate::Database::open(
                io.clone(),
                "/db/main",
                crate::OpenOptions::new(Arc::new(crate::SqliteDialect)),
            )
            .unwrap();
            let conn = db.connect(crate::ConnectOpts::new()).unwrap();
            let statement = conn.prepare("SELECT 1").unwrap();
            let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
            let request = file.pread(0, read(512)).unwrap();
            assert!(request.is_owned() && !request.finished());
            failing_steps(&io);
            crate::child_test::record("target", "ready");
            #[cfg(any(target_os = "linux", target_os = "android"))]
            stderr_a_full_pipe();
            let _ = statement.wait_io();
            crate::child_test::record("target", "returned");
            std::process::exit(0);
        },
    ) else {
        return;
    };
    assert_eq!(
        crate::child_test::recorded(&ended, "backend-step"),
        "failed\n"
    );
    assert_eq!(crate::child_test::recorded(&ended, "target"), "ready\n");
    assert!(
        ended.aborted && !ended.hung,
        "the statement's pump did not abort: hung {}, code {:?}",
        ended.hung,
        ended.code
    );
}
