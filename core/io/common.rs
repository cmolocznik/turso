pub const ENV_DISABLE_FILE_LOCK: &str = "LIMBO_DISABLE_FILE_LOCK";

/// Opens `path` for the unix backends. With [`OpenFlags::Direct`](super::OpenFlags::Direct) the
/// file bypasses the page cache: `O_DIRECT` on Linux, `F_NOCACHE` on Apple platforms.
#[cfg(all(target_family = "unix", not(miri)))]
pub fn open_unix(path: &str, flags: super::OpenFlags) -> crate::Result<std::fs::File> {
    use super::OpenFlags;
    let mut options = std::fs::File::options();
    options.read(true);
    if !flags.contains(OpenFlags::ReadOnly) {
        options.write(true);
        options.create(flags.contains(OpenFlags::Create));
    }
    #[cfg(target_os = "linux")]
    if flags.contains(OpenFlags::Direct) {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECT);
    }
    let file = options
        .open(path)
        .map_err(|e| crate::error::io_error(e, "open"))?;
    #[cfg(target_vendor = "apple")]
    if flags.contains(OpenFlags::Direct) {
        use std::os::fd::AsRawFd;
        // SAFETY: a plain fcntl on a descriptor we own.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
            return Err(crate::error::io_error(
                std::io::Error::last_os_error(),
                "fcntl(F_NOCACHE)",
            ));
        }
    }
    Ok(file)
}

/// `fsync` on the directory `path`, which makes the files created in or removed from it durable.
#[cfg(all(target_family = "unix", not(miri)))]
pub fn sync_directory(path: &str) -> crate::Result<()> {
    std::fs::File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| crate::error::io_error(e, "sync directory"))
}

/// `fallocate` of `[pos, pos + len)` in its default mode: blocks reserved, the size grown.
#[cfg(all(target_os = "linux", not(miri)))]
pub fn allocate(file: &std::fs::File, pos: u64, len: u64) -> crate::Result<()> {
    use rustix::fs::{fallocate, FallocateFlags};
    fallocate(file, FallocateFlags::empty(), pos, len)
        .map_err(|e| crate::LimboError::CompletionError(e.into()))
}

#[cfg(test)]
pub mod tests {
    use crate::{Result, IO};
    use std::process::{Command, Stdio};
    use tempfile::NamedTempFile;

    fn run_test_parent_process<T: IO>(create_io: fn() -> Result<T>) {
        let temp_file: NamedTempFile = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_str().unwrap().to_string();

        // Parent process opens the file
        let io1 = create_io().expect("Failed to create IO");
        let _file1 = io1
            .open_file(&path, crate::io::OpenFlags::None, false)
            .expect("Failed to open file in parent process");

        let current_exe = std::env::current_exe().expect("Failed to get current executable path");

        // The child runs the whole test binary and then leaves through
        // `process::exit`, so its destructors never run and any temp file
        // another test had in flight is orphaned. Point it at a directory we
        // own so those files disappear with it.
        let child_temp_dir = tempfile::tempdir().expect("Failed to create child temp dir");

        // Spawn a child process and try to open the same file
        let child = Command::new(current_exe)
            .env("RUST_TEST_CHILD_PROCESS", "1")
            .env("RUST_TEST_FILE_PATH", &path)
            .env("TMPDIR", child_temp_dir.path())
            .env("TMP", child_temp_dir.path())
            .env("TEMP", child_temp_dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to spawn child process");

        let output = child.wait_with_output().expect("Failed to wait on child");
        assert!(
            !output.status.success(),
            "Child process should have failed to open the file"
        );
    }

    fn run_test_child_process<T: IO>(create_io: fn() -> Result<T>) -> Result<()> {
        if std::env::var("RUST_TEST_CHILD_PROCESS").is_ok() {
            let path = std::env::var("RUST_TEST_FILE_PATH")?;
            let io = create_io()?;
            match io.open_file(&path, crate::io::OpenFlags::None, false) {
                Ok(_) => std::process::exit(0),
                Err(_) => std::process::exit(1),
            }
        }
        Ok(())
    }

    pub fn test_multiple_processes_cannot_open_file<T: IO>(create_io: fn() -> Result<T>) {
        run_test_child_process(create_io).unwrap();
        run_test_parent_process(create_io);
    }
}
