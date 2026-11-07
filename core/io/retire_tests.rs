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
            eprintln!("stepped again after a failed step");
            std::process::exit(0);
        }
        Err(crate::LimboError::InternalError(
            "the backend's stepping failed".into(),
        ))
    })));
}

/// Runs `wait` on a read a CrashIO accepted, in a child whose steps fail, and asserts the child
/// ended by abort naming `during`.
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
        ended.aborted
            && ended
                .stderr
                .contains(&format!("{during}: the IO's step failed")),
        "a failed step under {during} did not end the process: {:?} {}",
        ended.code,
        ended.stderr
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
