//! Child-process tests: a test whose panic passes a guard of shared state ends its process
//! (DESIGN.md 22.11), and so does one on a ring's delivery thread (22.5), so it runs in a child and
//! the parent reads how the child ended.

/// How a test's child process ended, and what it printed.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(crate) struct Ended {
    pub aborted: bool,
    /// It had not ended within [`CHILD_BOUND`] and was killed.
    pub hung: bool,
    pub code: Option<i32>,
    pub stderr: String,
    /// The child's own directory ([`child_dir`]), kept until this is dropped.
    pub dir: tempfile::TempDir,
}

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
const CHILD: &str = "TURSO_CHILD_TEST";

#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
const CHILD_DIR: &str = "TURSO_CHILD_DIR";

/// How long a child may run before it counts as hung and is killed: a hang is the defect some
/// child tests look for, and the slowest child measured takes under a second, so two minutes
/// leaves a loaded machine a hundredfold.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
const CHILD_BOUND: std::time::Duration = std::time::Duration::from_secs(120);

/// How often the parent looks whether its child has ended: a hundredth of the fastest child.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
const CHILD_POLL: std::time::Duration = std::time::Duration::from_millis(1);

/// In a child process [`in_child`] started, a directory of its own, which the parent reads after.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(crate) fn child_dir() -> std::path::PathBuf {
    std::env::var(CHILD_DIR)
        .expect("a child's directory")
        .into()
}

/// Runs `body` in a child process, this test binary running only `test` (its path below the
/// crate, `module_path!()` joined with the test's name), and says how the child ended: for a test
/// whose panic passes a guard of shared state, which ends the process (DESIGN.md 22.11). The
/// child dumps no core, and one still running after [`CHILD_BOUND`] is killed. In the child
/// itself, `body` runs and this returns `None`.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(crate) fn in_child(test: &str, body: impl FnOnce()) -> Option<Ended> {
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    let test = test.strip_prefix("turso_core::").unwrap_or(test);
    if std::env::var(CHILD).as_deref() == Ok(test) {
        body();
        return None;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([test, "--exact", "--nocapture", "--test-threads", "1"])
        .env(CHILD, test)
        .env(CHILD_DIR, dir.path())
        .env("RUST_BACKTRACE", "0");
    // SAFETY: setrlimit is async-signal-safe, and the closure touches nothing else.
    unsafe {
        command.pre_exec(|| {
            let none = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::setrlimit(libc::RLIMIT_CORE, &none) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("the child runs");
    // Each pipe is read on its own thread, so a child that fills one never waits on the parent.
    let read = |mut pipe: Box<dyn std::io::Read + Send>| {
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            pipe.read_to_end(&mut bytes).unwrap();
            String::from_utf8_lossy(&bytes).into_owned()
        })
    };
    let stdout = read(Box::new(child.stdout.take().unwrap()));
    let stderr = read(Box::new(child.stderr.take().unwrap()));
    let since = std::time::Instant::now();
    let (status, hung) = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break (status, false);
        }
        if since.elapsed() > CHILD_BOUND {
            child.kill().unwrap();
            break (child.wait().unwrap(), true);
        }
        std::thread::sleep(CHILD_POLL);
    };
    let (stdout, stderr) = (stdout.join().unwrap(), stderr.join().unwrap());
    assert!(
        stdout.contains("running 1 test"),
        "the child ran no test named {test}:\n{stdout}"
    );
    Some(Ended {
        aborted: status.signal() == Some(libc::SIGABRT),
        hung,
        code: status.code(),
        stderr,
        dir,
    })
}

/// An observer's record, appended to `name` in the child's directory and synced, so it survives
/// the child's abort.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(crate) fn record(name: &str, what: &str) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(child_dir().join(name))
        .unwrap();
    f.write_all(what.as_bytes()).unwrap();
    f.write_all(b"\n").unwrap();
    f.sync_all().unwrap();
}

/// What the child recorded in `name`.
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
pub(crate) fn recorded(ended: &Ended, name: &str) -> String {
    std::fs::read_to_string(ended.dir.path().join(name)).unwrap_or_default()
}
