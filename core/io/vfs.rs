use super::{Buffer, Completion, File, FileSyncType, OpenFlags, IO};
use crate::ext::VfsMod;
use crate::io::clock::{Clock, DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::CompletionInner;
use crate::sync::Arc;
use crate::{LimboError, Result};
use std::ffi::{c_void, CString};
use std::ptr::NonNull;
use turso_ext::{BufferRef, IOCallback, ResultCode, SendPtr, VfsFileImpl, VfsImpl};

impl Clock for VfsMod {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for VfsMod {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let c_path = CString::new(path).map_err(|_| {
            LimboError::ExtensionError("Failed to convert path to CString".to_string())
        })?;
        let ctx = self.ctx as *mut c_void;
        let vfs = unsafe { &*self.ctx };
        let file = unsafe { (vfs.open)(ctx, c_path.as_ptr(), flags.0, direct) };
        if file.is_null() {
            return Err(LimboError::ExtensionError("File not found".to_string()));
        }
        Ok(Arc::new(turso_ext::VfsFileImpl::new(file, self.ctx)?))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        let c_path = CString::new(path).map_err(|_| {
            LimboError::ExtensionError("Failed to convert path to CString".to_string())
        })?;
        let ctx = self.ctx as *mut c_void;
        let vfs = unsafe { &*self.ctx };
        let result = unsafe { (vfs.remove)(ctx, c_path.as_ptr()) };
        if !result.is_ok() {
            return Err(LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn step(&self) -> Result<()> {
        if self.ctx.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.ctx };
        let result = unsafe { (vfs.run_once)(vfs.vfs) };
        if !result.is_ok() {
            // The extension can no longer deliver what it accepted (DESIGN.md 22.5).
            super::stepping_failed(
                &LimboError::ExtensionError(result.to_string()),
                "an extension VFS's run_once",
            );
        }
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        if self.ctx.is_null() {
            return -1;
        }
        let vfs = unsafe { &*self.ctx };
        unsafe { (vfs.gen_random_number)() }
    }
}

impl VfsMod {
    #[allow(dead_code)] // used in FFI call
    fn get_current_time(&self) -> String {
        if self.ctx.is_null() {
            return "".to_string();
        }
        unsafe {
            let vfs = &*self.ctx;
            let chars = (vfs.current_time)();
            let cstr = CString::from_raw(chars as *mut _);
            cstr.to_string_lossy().into_owned()
        }
    }
}

/// # Safety
/// the callback wrapper in the extension library is FnOnce, so we know
/// that the into_raw/from_raw contract will hold
unsafe extern "C" fn callback_fn(result: i32, ctx: SendPtr) {
    let completion = Completion {
        inner: (Some(Arc::from_raw(ctx.inner().as_ptr() as *mut CompletionInner))),
    };
    completion.deliver(Ok(result));
}

/// Hands `c` to the extension, accepted from before the call (the extension may deliver it inside
/// it) and given back whole if the extension refuses it: an extension that refuses a request
/// drops its callback unrun.
fn submit(c: &Completion, call: impl FnOnce(IOCallback) -> ResultCode, what: &str) -> Result<()> {
    c.accept();
    // The callback's own reference, which `callback_fn` takes back when it runs.
    let raw = Arc::into_raw(c.get_inner().clone());
    let cb = IOCallback::new(callback_fn, unsafe {
        NonNull::new_unchecked(raw as *mut c_void)
    });
    if call(cb).is_error() {
        // The extension dropped the callback unrun, which releases nothing: its reference is
        // taken back here, once, or the request and its buffers would never be freed.
        // SAFETY: `raw` came from `Arc::into_raw` above, and a refusing extension never runs the
        // callback that would take it back.
        drop(unsafe { Arc::from_raw(raw) });
        c.refuse();
        return Err(LimboError::ExtensionError(format!("{what} failed")));
    }
    Ok(())
}

impl File for VfsFileImpl {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.lock)(self.file, exclusive) };
        if result.is_ok() {
            return Err(LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.unlock)(self.file) };
        if result.is_ok() {
            return Err(LimboError::ExtensionError(result.to_string()));
        }
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let r = c.as_read();
        let buf = r.buf();
        let len = buf.len();
        let vfs = unsafe { &*self.vfs };
        submit(
            &c,
            |cb| unsafe {
                (vfs.read)(
                    self.file,
                    BufferRef::new(buf.as_mut_ptr(), len),
                    pos as i64,
                    cb,
                )
            },
            "pread",
        )?;
        Ok(c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        let len = buffer.len();
        let ptr = buffer.as_ptr() as *mut u8;
        // Keep the buffer alive until the VFS completion fires — the extension
        // may process the write asynchronously after this function returns.
        c.keep_write_buffer_alive(buffer);
        submit(
            &c,
            |cb| unsafe { (vfs.write)(self.file, BufferRef::new(ptr, len), pos as i64, cb) },
            "pwrite",
        )?;
        Ok(c)
    }

    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        submit(&c, |cb| unsafe { (vfs.sync)(self.file, cb) }, "sync")?;
        Ok(c)
    }

    fn size(&self) -> Result<u64> {
        let vfs = unsafe { &*self.vfs };
        let result = unsafe { (vfs.size)(self.file) };
        if result < 0 {
            Err(LimboError::ExtensionError("size failed".to_string()))
        } else {
            Ok(result as u64)
        }
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        if self.vfs.is_null() {
            return Err(LimboError::ExtensionError("VFS is null".to_string()));
        }
        let vfs = unsafe { &*self.vfs };
        submit(
            &c,
            |cb| unsafe { (vfs.truncate)(self.file, len as i64, cb) },
            "truncate",
        )?;
        Ok(c)
    }
}

impl Drop for VfsMod {
    fn drop(&mut self) {
        if self.ctx.is_null() {
            return;
        }
        unsafe {
            let _ = Box::from_raw(self.ctx as *mut VfsImpl);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read() -> (Completion, crate::sync::Weak<Buffer>) {
        let buffer = Arc::new(Buffer::new_temporary(512));
        let weak = Arc::downgrade(&buffer);
        (Completion::new_read(buffer, |_| None), weak)
    }

    /// Whether the request and its buffer are both freed once the caller lets the request go.
    fn freed(c: Completion, buffer: crate::sync::Weak<Buffer>) -> bool {
        let inner = Arc::downgrade(c.get_inner());
        drop(c);
        inner.upgrade().is_none() && buffer.upgrade().is_none()
    }

    /// A refusal gives back the reference the dropped callback held, so the request and its buffer
    /// are freed once the caller lets them go (mutant: the reference left with the callback, which
    /// keeps both for ever).
    #[test]
    fn a_refused_request_gives_back_the_reference_its_callback_held() {
        let (c, buffer) = read();
        // The callback is dropped unrun as the closure returns.
        let refused = submit(&c, |_cb| ResultCode::Error, "pread");
        assert!(refused.is_err() && !c.is_owned() && !c.finished());
        assert!(freed(c, buffer), "the refused request was never freed");
    }

    /// A request delivered inside its submission, and one delivered after it, are each freed once
    /// their callback has run: the callback takes its reference back once.
    #[test]
    fn an_accepted_request_is_freed_once_its_callback_has_run() {
        let (c, buffer) = read();
        submit(
            &c,
            |cb| {
                unsafe { (cb.callback)(512, cb.ctx) };
                ResultCode::OK
            },
            "pread",
        )
        .unwrap();
        assert!(c.finished() && !c.is_owned());
        assert!(freed(c, buffer), "the inline request was never freed");

        let (c, buffer) = read();
        let mut held = None;
        submit(
            &c,
            |cb| {
                held = Some(cb);
                ResultCode::OK
            },
            "pread",
        )
        .unwrap();
        assert!(c.is_owned() && !c.finished());
        let cb = held.take().unwrap();
        unsafe { (cb.callback)(512, cb.ctx) };
        assert!(c.finished() && !c.is_owned());
        assert!(freed(c, buffer), "the deferred request was never freed");
    }
}
