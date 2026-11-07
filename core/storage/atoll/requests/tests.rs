use super::*;
use crate::io::crash::CrashIO;
use crate::io::{MemoryIO, OpenFlags, DIRECT_IO_ALIGNMENT};
use crate::storage::atoll::db::{Atoll, AtollConfig};
use crate::storage::atoll::log::{framed_len, Header, Kind};
use crate::storage::atoll::testing::drive;
use crate::sync::atomic::AtomicBool;
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
        ended.aborted
            && ended
                .stderr
                .contains("a database's close: the IO's step failed"),
        "the failed step did not end the process: {:?} {}",
        ended.code,
        ended.stderr
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
