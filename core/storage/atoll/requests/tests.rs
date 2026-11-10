use super::*;
use crate::io::crash::CrashIO;
use crate::io::{MemoryIO, OpenFlags, DIRECT_IO_ALIGNMENT};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::locks::Mutex;
use crate::storage::atoll::log::{framed_len, Header, Kind};
use crate::storage::atoll::testing::drive;
use crate::sync::atomic::AtomicBool;
use crate::sync::atomic::AtomicUsize;
use crate::IOResult;

fn counted(io: Arc<dyn IO>) -> (Arc<dyn IO>, Arc<Requests>) {
    let requests = Arc::new(Requests::default());
    (Arc::new(Counted::new(io, requests.clone())), requests)
}

fn read(len: usize) -> Completion {
    Completion::new_read(Arc::new(Buffer::new_temporary(len)), |_| None)
}

/// A request counts from before its submission until its backend retires it, not until it
/// finishes: a cancelled read a pumped backend still holds counts until a step lets it go.
#[test]
fn a_request_counts_until_its_backend_retires_it_not_until_it_finishes() {
    let crash = Arc::new(CrashIO::new(1));
    let (io, requests) = counted(crash.clone());
    let file = io.open_file("/db/r", OpenFlags::Create, false).unwrap();
    let c = file.pread(0, read(512)).unwrap();
    assert_eq!(requests.outstanding(), 1);
    io.cancel(std::slice::from_ref(&c)).unwrap();
    assert!(c.finished(), "the cancel did not finish it");
    assert!(
        c.is_owned(),
        "a cancelled request the backend holds is not owned"
    );
    assert_eq!(requests.outstanding(), 1, "uncounted at its cancel");
    while crash.in_flight() > 0 {
        io.step().unwrap();
    }
    assert!(!c.is_owned());
    assert_eq!(requests.outstanding(), 0, "still counted once retired");
}

/// A request delivered inside its submission counts while its callback runs, as one counted
/// before it was submitted does, and not once the submission has returned.
#[test]
fn an_inline_request_counts_during_its_submission_and_not_after() {
    let (io, requests) = counted(Arc::new(MemoryIO::new()));
    let file = io.open_file("/db/r", OpenFlags::Create, false).unwrap();
    let seen = Arc::new(AtomicUsize::new(usize::MAX));
    let c = {
        let (seen, requests) = (seen.clone(), requests.clone());
        Completion::new_write(move |_| seen.store(requests.outstanding(), Ordering::SeqCst))
    };
    drop(
        file.pwrite(0, Arc::new(Buffer::new_temporary(512)), c)
            .unwrap(),
    );
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "not counted while its callback ran inside its submission"
    );
    assert_eq!(requests.outstanding(), 0);
}

/// A request a callback submits counts inside that callback, before the request whose callback
/// it is retires: the count never passes through zero while the chain goes on.
#[test]
fn a_request_a_callback_submits_counts_before_its_parent_retires() {
    let crash = Arc::new(CrashIO::new(2));
    let (io, requests) = counted(crash.clone());
    let file = io.open_file("/db/r", OpenFlags::Create, false).unwrap();
    let child_submitted = Arc::new(AtomicBool::new(false));
    let parent = {
        let (file, child_submitted) = (file.clone(), child_submitted.clone());
        Completion::new_write(move |_| {
            drop(file.pread(0, read(512)).unwrap());
            child_submitted.store(true, Ordering::SeqCst);
        })
    };
    let parent = file
        .pwrite(0, Arc::new(Buffer::new_temporary(512)), parent)
        .unwrap();
    // As the parent retires, after Atoll's own count of it has gone.
    let at_retirement = Arc::new(AtomicUsize::new(usize::MAX));
    {
        let (at_retirement, requests) = (at_retirement.clone(), requests.clone());
        parent.on_retired(move || at_retirement.store(requests.outstanding(), Ordering::SeqCst));
    }
    while !child_submitted.load(Ordering::SeqCst) {
        io.step().unwrap();
    }
    assert_eq!(
        at_retirement.load(Ordering::SeqCst),
        1,
        "the count passed through zero as the parent retired"
    );
    while crash.in_flight() > 0 {
        io.step().unwrap();
    }
    assert_eq!(requests.outstanding(), 0);
}

/// A submission refused whole accepted nothing: its request is uncounted at once, and no backend
/// owns it.
#[test]
fn a_refused_submission_is_uncounted_at_once() {
    let (io, requests) = counted(Arc::new(CrashIO::new(3)));
    let file = io
        .open_file("/db/r", OpenFlags::Create | OpenFlags::Direct, true)
        .unwrap();
    // Misaligned for direct IO: refused before anything is queued.
    let c = read(DIRECT_IO_ALIGNMENT + 1);
    assert!(file.pread(0, c.clone()).is_err());
    assert_eq!(requests.outstanding(), 0);
    assert!(!c.is_owned() && !c.finished());
}

/// A database closes only once every request on its files has retired (DESIGN.md 22.12, step
/// 4): its log's writes still in flight on a pumped backend when its last handle goes are
/// delivered by the close, and nothing is left for the backend to land later.
#[test]
fn a_database_closes_once_every_request_on_its_files_has_retired() {
    for seed in 0..4 {
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let db = Atoll::open(io.clone(), "/db/close", AtollConfig::small()).unwrap();
        let log = db.log().clone();
        let payload = [7u8; 100];
        let mut admitted = drive(&io, || log.admit(framed_len(payload.len()))).unwrap();
        let header = Header {
            kind: Kind::new(1).unwrap(),
            flags: 0,
            txn: 0,
            prev: None,
        };
        let lsn = log.append(&mut admitted, &header, &[&payload]);
        drop(admitted);
        // Asked for and left: its writes are in flight, and nobody steps them.
        let IOResult::IO(_) = log.wait_durable(lsn).unwrap() else {
            panic!("seed {seed}: durable with nothing stepped");
        };
        assert!(crash.in_flight() > 0, "seed {seed}: nothing in flight");
        drop(log);
        drop(db);
        assert_eq!(
            crash.in_flight(),
            0,
            "seed {seed}: a request outlived its database's close"
        );
    }
}

/// The block opening with the line `head` in `code`, to its closing brace at the start of a line.
#[cfg(not(any(shuttle, atoll_loom)))]
fn block<'a>(code: &'a str, head: &str) -> &'a str {
    let start = code
        .find(head)
        .unwrap_or_else(|| panic!("no `{head}` found"));
    let end = code[start..].find("\n}").expect("an unclosed block") + start;
    &code[start..end]
}

/// The methods a trait or an impl block declares at its first level.
#[cfg(not(any(shuttle, atoll_loom)))]
fn methods(block: &str) -> std::collections::BTreeSet<String> {
    block
        .lines()
        .filter_map(|l| l.strip_prefix("    fn "))
        .map(|l| {
            l.chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect()
        })
        .collect()
}

/// [`Counted`] and its files forward every method of the traits they implement: a method left to
/// the trait's default would run the default instead of the wrapped backend's own (a cancel that
/// never reaches the kernel, a directory never synced), with nothing to say so.
#[cfg(not(any(shuttle, atoll_loom)))]
#[test]
fn the_counting_io_and_its_files_forward_every_method() {
    use crate::storage::atoll::testing::{atoll_dir, code_of};
    let core = atoll_dir().join("../..");
    let read = |p: &str| code_of(&std::fs::read_to_string(core.join(p)).unwrap());
    let io = read("io/mod.rs");
    let clock = read("io/clock.rs");
    let here = read("storage/atoll/requests.rs");
    for (trait_code, trait_head, impl_head, known) in [
        (
            &io,
            "pub trait IO: Clock + Send + Sync {",
            "impl IO for Counted {",
            "step",
        ),
        (
            &io,
            "pub trait File: Send + Sync {",
            "impl File for CountedFile {",
            "pread",
        ),
        (
            &clock,
            "pub trait Clock {",
            "impl Clock for Counted {",
            "current_time_monotonic",
        ),
    ] {
        let wanted = methods(block(trait_code, trait_head));
        let forwarded = methods(block(&here, impl_head));
        assert!(wanted.contains(known), "`{trait_head}` read as {wanted:?}");
        assert_eq!(
            forwarded, wanted,
            "`{impl_head}` does not forward exactly the methods of `{trait_head}`"
        );
    }
}

/// A step hook that fails the first step and, should anyone step again, ends the child with code
/// 0 and says so: one failed step is enough for the rule, so a second is the defect.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn failing_steps(crash: &CrashIO) {
    let failed = Arc::new(AtomicBool::new(false));
    crash.on_step(Some(Arc::new(move || {
        if failed.swap(true, Ordering::SeqCst) {
            eprintln!("stepped again after a failed step");
            std::process::exit(0);
        }
        Err(crate::LimboError::InternalError(
            "the backend's stepping failed".into(),
        ))
    })));
}

/// A step that fails while close waits for a request ends the process (DESIGN.md 22.11, 22.12):
/// the backend has lost the means to retire it, so close can neither wait on nor return while the
/// request's buffer may still be the backend's (mutant: the failure logged and the wait gone on).
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
#[test]
fn a_step_that_fails_while_close_waits_ends_the_process() {
    let Some(ended) = crate::storage::atoll::testing::in_child(
        concat!(
            module_path!(),
            "::a_step_that_fails_while_close_waits_ends_the_process"
        ),
        || {
            let crash = Arc::new(CrashIO::new(4));
            let (io, requests) = counted(crash.clone());
            let file = io.open_file("/db/r", OpenFlags::Create, false).unwrap();
            let c = file.pread(0, read(512)).unwrap();
            assert!(c.is_owned());
            failing_steps(&crash);
            requests.drain(&*io);
            eprintln!("close returned with the request still owned");
        },
    ) else {
        return;
    };
    assert!(
        ended.aborted,
        "the failed step did not end the process: {:?} {}",
        ended.code, ended.stderr
    );
}

/// Close parks on a backend that needs no step until the retirement that brings the count to zero
/// wakes it (DESIGN.md 22.5, 22.12): on the ring, with a read the kernel holds until the test
/// writes to its pipe, close waits without a step and returns once the read is delivered (mutants:
/// close stepping a backend that needs none; the last retirement waking no one).
#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[test]
fn close_parks_on_the_ring_until_its_last_request_retires() {
    use std::io::Write as _;
    let dir = tempfile::tempdir().unwrap();
    let ring =
        crate::io::wait_tests::StepCounting::new(Arc::new(crate::io::UringIO::new().unwrap()));
    let (io, requests) = counted(ring.clone());
    let fifo = dir.path().join("fifo");
    let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    // SAFETY: a plain mkfifo(3) on a fresh path.
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    let pipe = io
        .open_file(fifo.to_str().unwrap(), OpenFlags::NoLock, false)
        .unwrap();
    let c = pipe.pread(0, read(1)).unwrap();
    assert_eq!(requests.outstanding(), 1);
    let (tx, rx) = std::sync::mpsc::channel();
    let closer = {
        let (requests, io) = (requests.clone(), io.clone());
        std::thread::spawn(move || {
            requests.drain(&*io);
            let _ = tx.send(());
        })
    };
    // Long enough for close to start and park, which takes microseconds.
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        rx.try_recv().is_err(),
        "close returned while the kernel holds a read"
    );
    std::fs::OpenOptions::new()
        .write(true)
        .open(&fifo)
        .unwrap()
        .write_all(b"x")
        .unwrap();
    assert_eq!(
        rx.recv_timeout(std::time::Duration::from_secs(10)),
        Ok(()),
        "close was not woken by its last request's retirement"
    );
    closer.join().unwrap();
    assert!(!c.is_owned());
    assert_eq!(requests.outstanding(), 0);
    assert_eq!(ring.steps(), 0, "close stepped the ring");
}

/// Where the cancellation of close's request is paused.
#[derive(Clone, Copy, Debug)]
enum Paused {
    /// In its callback, before the callback submits a request of its own.
    InCallback,
    /// Inside that submission, the request counted and not yet the backend's (the auditor's
    /// #4177).
    CountedNotQueued,
}

/// Close on a pumped backend that has let a request go while a cancellation, paused on another
/// thread, still runs its callback: close looks once, finds the backend empty and parks, however
/// many requests are counted; a request submitted meanwhile wakes it and it steps that one; the
/// callback, resumed, submits a request of its own (or finishes submitting the one it counted),
/// which close steps too; close returns once all retired (audit chain-fixes-v1, P2-CF1, and the
/// auditor's #4177; mutants: close stepping an empty backend spends the pause on empty steps;
/// close parking whatever the backend holds, or not woken by a submission, never steps the new
/// requests).
fn close_parks_on_an_empty_backend_and_steps_what_is_submitted(paused: Paused) {
    use std::sync::mpsc;
    use std::time::Duration;
    const BOUND: Duration = Duration::from_secs(20);
    let crash = Arc::new(CrashIO::new(1));
    let stepping = crate::io::wait_tests::StepCounting::new(crash.clone());
    let (io, requests) = counted(stepping.clone());
    let file = io.open_file("/db/close", OpenFlags::Create, false).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let go_rx = Mutex::new(go_rx);
    let pause: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
        entered_tx.send(()).unwrap();
        go_rx.lock().recv().unwrap();
    });
    let child = Arc::new(Mutex::new(None::<Completion>));
    let c = {
        let (file, child, pause) = (file.clone(), child.clone(), pause.clone());
        Completion::new_sync(move |res| {
            if res.is_err() {
                if let Paused::InCallback = paused {
                    pause();
                }
                let next = file
                    .pwrite(
                        0,
                        Arc::new(Buffer::new_temporary(512)),
                        Completion::new_write(|_| {}),
                    )
                    .unwrap();
                *child.lock() = Some(next);
            }
        })
    };
    let c = file.sync(c, crate::io::FileSyncType::Fsync).unwrap();
    let cancel = {
        let c = c.clone();
        std::thread::spawn(move || {
            if let Paused::CountedNotQueued = paused {
                crate::race::at("requests.counted", move || pause());
            }
            c.abort()
        })
    };
    entered_rx
        .recv_timeout(BOUND)
        .expect("the cancellation reached its pause");
    crash.step().unwrap();
    assert!(crash.in_flight() == 0 && c.is_owned());
    let counted = match paused {
        Paused::InCallback => 1,
        Paused::CountedNotQueued => 2,
    };
    assert_eq!(requests.outstanding(), counted, "{paused:?}: counted");
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let closer = {
        let (requests, io) = (requests.clone(), io.clone());
        std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            requests.drain(&*io);
            done_tx.send(()).unwrap();
        })
    };
    started_rx.recv().unwrap();
    // Long enough for close to reach its wait, which takes microseconds, and for a close that
    // steps to step many times.
    std::thread::sleep(Duration::from_millis(100));
    let mid = (stepping.steps(), done_rx.try_recv().is_ok());
    // A request submitted while close waits, the finish still paused: close wakes and steps it.
    let (landed_tx, landed_rx) = mpsc::channel();
    let other = file
        .pwrite(
            512,
            Arc::new(Buffer::new_temporary(512)),
            Completion::new_write(move |_| {
                let _ = landed_tx.send(());
            }),
        )
        .unwrap();
    let stepped_for_it = landed_rx.recv_timeout(BOUND).is_ok();
    go_tx.send(()).unwrap();
    cancel.join().unwrap();
    let ended = done_rx.recv_timeout(BOUND).is_ok();
    if !ended {
        // A close that never steps the callback's request: step it here so the closer returns.
        while requests.outstanding() > 0 {
            crash.step().unwrap();
        }
    }
    closer.join().unwrap();
    let child = child.lock().take();
    assert_eq!(
        mid,
        (1, false),
        "{paused:?}: (looks, returned) while the finish was paused"
    );
    assert!(
        stepped_for_it,
        "{paused:?}: close did not step a request submitted while it waited"
    );
    assert!(
        !other.is_owned(),
        "{paused:?}: the request submitted while close waited"
    );
    assert!(
        ended,
        "{paused:?}: close did not return once every request could retire"
    );
    let child = child.expect("the callback submitted its request");
    assert!(
        child.finished() && !child.is_owned(),
        "{paused:?}: the callback's request"
    );
    assert!(!c.is_owned(), "{paused:?}: the cancelled request");
    assert_eq!(requests.outstanding(), 0);
}

#[test]
fn close_parks_while_only_a_finish_elsewhere_can_retire_and_steps_what_it_submits() {
    close_parks_on_an_empty_backend_and_steps_what_is_submitted(Paused::InCallback);
}

#[test]
fn close_parks_while_a_counted_request_is_not_yet_submitted_and_steps_it_once_it_is() {
    close_parks_on_an_empty_backend_and_steps_what_is_submitted(Paused::CountedNotQueued);
}

struct AuditGroupFile(Arc<dyn File>);
impl File for AuditGroupFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.0.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.0.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let mut group = crate::io::CompletionGroup::new(|_| {});
        group.add(&c);
        // The group holds the request: what the submission hands back is the same one.
        let _ = self.0.pread(pos, c)?;
        Ok(group.build())
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.0.pwrite(pos, buffer, c)
    }
    fn sync(&self, c: Completion, ty: FileSyncType) -> Result<Completion> {
        self.0.sync(c, ty)
    }
    fn size(&self) -> Result<u64> {
        self.0.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.0.truncate(len, c)
    }
}

#[test]
fn audit_a_returned_group_cannot_uncount_the_original_request_still_owned_by_crash_io() {
    let crash = Arc::new(CrashIO::new(1));
    let requests = Arc::new(Requests::default());
    let backend = crash
        .open_file("/db/group-return", OpenFlags::Create, false)
        .unwrap();
    let file = CountedFile {
        file: Arc::new(AuditGroupFile(backend)),
        requests: requests.clone(),
    };
    let returned = file.pread(0, read(512)).unwrap();
    let before = (
        requests.outstanding(),
        crash.in_flight(),
        returned.finished(),
    );
    while crash.in_flight() > 0 {
        crash.step().unwrap();
    }
    let after = requests.outstanding();
    drop((returned, file, requests, crash));
    println!("ORIGINAL_REQUEST_COUNT before={before:?} after={after}");
    assert_eq!(
        before,
        (1, 1, false),
        "a returned logical group hid the accepted physical request"
    );
    assert_eq!(after, 0);
}
