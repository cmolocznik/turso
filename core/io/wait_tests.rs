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
