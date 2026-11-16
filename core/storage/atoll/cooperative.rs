//! The cooperative frame (DESIGN.md 22.18, *Two kinds of host*; ASYNC1 D2a §1): a `Future::poll`
//! and every call beneath it, marked on its thread while its guard lives. In a database opened
//! async, a request made inside one is queued in its owner's record for the IO threads
//! ([`super::requests`]), so no syscall waits on the polling thread. The outermost frame carries a
//! budget of work units, which every frame inside it shares (ASYNC1 D2e (iv)). Frames nest only in
//! tests, so the mark is a depth; the polls that enter them in production are D2e (v)'s.

use std::cell::Cell;
#[cfg(test)]
use std::marker::PhantomData;

crate::thread::thread_local! {
    /// The frames open on this thread.
    static DEPTH: Cell<u32> = const { Cell::new(0) };
    /// What is left of the outermost frame's budget, in work units (ASYNC1 D2e (iv)).
    static BUDGET: Cell<u32> = const { Cell::new(0) };
}

#[cfg(test)]
crate::thread::thread_local! {
    /// The VDBE instructions dispatched on this thread, over every program state, for a test: a
    /// count beside the budget, not the budget (ASYNC1 D2e (iv), proof 1).
    pub(crate) static DISPATCHED: Cell<u64> = const { Cell::new(0) };
    /// The program states continued on this thread, for a test: each one already running at the
    /// poll's step of its first dispatch in a call, counted once a call, over every program state.
    /// That first dispatch is made whatever the budget.
    pub(crate) static CONTINUED: Cell<u64> = const { Cell::new(0) };
    /// The program states that have dispatched in this thread's current call, by address, for a
    /// test: what `CONTINUED` has already seen in the call.
    pub(crate) static DISPATCHED_IN_CALL: std::cell::RefCell<Vec<usize>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// The dispatches made on this thread while the frame's budget was spent, for a test.
    pub(crate) static FREE: Cell<u64> = const { Cell::new(0) };
    /// The units charged to the budget, for a test: what every charge was asked to take.
    pub(crate) static CHARGED: Cell<u64> = const { Cell::new(0) };
    /// What a test does with a subprogram's wait before its parent parks it, once.
    pub(crate) static BEFORE_PARENT_PARKS: std::cell::RefCell<Option<ParentParks>> =
        const { std::cell::RefCell::new(None) };
    /// What a test does with a stopped step's parked wait before the step sets its waker on it,
    /// once.
    pub(crate) static BEFORE_WAKER_IS_SET: std::cell::RefCell<Option<ParentParks>> =
        const { std::cell::RefCell::new(None) };
    /// The `vm_steps` of each periodic check on this thread, in order, for a test (IV-2's proof).
    pub(crate) static CHECKS: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// What a test does with a subprogram's wait before its parent parks it.
#[cfg(test)]
pub(crate) type ParentParks = Box<dyn FnOnce(&crate::Completion)>;

/// A cooperative frame open on this thread until it drops. Left on the thread that entered it.
#[cfg(test)]
pub(crate) struct Frame(PhantomData<*const ()>);

#[cfg(test)]
impl Frame {
    /// The budget of a cooperative frame by default, in work units: one VDBE instruction, or one
    /// iteration of a loop inside an instruction or a facade call that can run long. A unit is not a
    /// time. Measured on dev-id1 (D2E (iv) addendum 1): about 13 ns for a scan's, 39 ns for an
    /// insert's and 202 ns for a point lookup's, so an embedder can turn a time into units.
    pub(crate) const DEFAULT_BUDGET: u32 = 2048;

    pub(crate) fn enter() -> Self {
        Self::with_budget(Self::DEFAULT_BUDGET)
    }

    /// A frame whose budget, if it is the outermost, is `units`; a frame inside another shares the
    /// outer's.
    pub(crate) fn with_budget(units: u32) -> Self {
        DEPTH.with(|d| {
            if d.get() == 0 {
                BUDGET.with(|b| b.set(units));
            }
            d.set(d.get() + 1)
        });
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

/// What is left of this thread's frame budget, called inside a frame.
#[inline]
pub(crate) fn remaining() -> u32 {
    BUDGET.with(Cell::get)
}

/// Charges `units` to this thread's frame budget, called inside a frame: whether it is spent.
#[inline]
pub(crate) fn charge(units: u64) -> bool {
    #[cfg(test)]
    CHARGED.with(|c| c.set(c.get() + units));
    BUDGET.with(|b| {
        let left = u64::from(b.get()).saturating_sub(units);
        b.set(left as u32);
        left == 0
    })
}

/// Whether this thread is inside a cooperative frame whose budget is spent: a loop over ready rows
/// steps again only while it is not (ASYNC1 D2e (iv)).
pub(crate) fn poll_budget_spent() -> bool {
    inside() && remaining() == 0
}
