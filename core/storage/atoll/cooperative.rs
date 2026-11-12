//! The cooperative frame (DESIGN.md 22.18, *Two kinds of host*; ASYNC1 D2a §1): a `Future::poll`
//! and every call beneath it, marked on its thread while its guard lives. In a database opened
//! async, a request made inside one is queued in its owner's record for the IO threads
//! ([`super::requests`]), so no syscall waits on the polling thread. Frames nest (a poll that runs
//! an executor's wait runs a task's step inside it), so the mark is a depth. Tests enter frames;
//! the polls that enter them in production are D2e's.

use std::cell::Cell;
#[cfg(test)]
use std::marker::PhantomData;

crate::thread::thread_local! {
    /// The frames open on this thread.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// A cooperative frame open on this thread until it drops. Left on the thread that entered it.
#[cfg(test)]
pub(crate) struct Frame(PhantomData<*const ()>);

#[cfg(test)]
impl Frame {
    pub(crate) fn enter() -> Self {
        DEPTH.with(|d| d.set(d.get() + 1));
        Self(PhantomData)
    }
}

#[cfg(test)]
impl Drop for Frame {
    fn drop(&mut self) {
        super::unwind::abort_if_unwinding();
        DEPTH.with(|d| {
            let n = d.get();
            debug_assert!(n > 0, "a cooperative frame left that was not entered");
            d.set(n - 1);
        });
    }
}

/// Whether this thread is inside a cooperative frame.
pub(crate) fn inside() -> bool {
    DEPTH.with(|d| d.get() > 0)
}
