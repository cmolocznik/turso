//! A unit of a job's work (DESIGN.md 22.1): a step does at most one, and a unit visits at most one
//! page and never waits, blocks or parks. A unit under way is marked on its thread while its guard
//! lives: every park refuses it ([`super::park`]), and debug builds count the pages it visits
//! ([`visit`]) and assert at most one as it ends. Tests read the most any unit visited. A log call
//! a unit makes (an admission, a wait for durability) runs no preparation of the log's: that is
//! its preparer's, a task of its own (DESIGN.md 22.9 item 2), which the unit's step waits on.

use std::cell::Cell;
use std::marker::PhantomData;

crate::thread::thread_local! {
    /// Whether a unit is under way on this thread.
    static INSIDE: Cell<bool> = const { Cell::new(false) };
}

#[cfg(debug_assertions)]
crate::thread::thread_local! {
    /// The pages the unit under way on this thread has visited.
    static VISITS: Cell<u32> = const { Cell::new(0) };
}

#[cfg(test)]
crate::thread::thread_local! {
    /// The most pages any unit visited on this thread, and the units run, since the last read.
    static MOST: Cell<u32> = const { Cell::new(0) };
    static UNITS: Cell<u64> = const { Cell::new(0) };
    /// The entries of bookkeeping walked on this thread, since the last read.
    static WALKED: Cell<usize> = const { Cell::new(0) };
}

/// A unit under way on this thread until it drops. Left on the thread that entered it.
pub(crate) struct Unit(PhantomData<*const ()>);

impl Unit {
    pub(crate) fn enter() -> Self {
        assert!(
            !INSIDE.with(|i| i.replace(true)),
            "a unit of a job's work entered inside another"
        );
        #[cfg(debug_assertions)]
        VISITS.with(|v| v.set(0));
        Self(PhantomData)
    }
}

impl Drop for Unit {
    fn drop(&mut self) {
        INSIDE.with(|i| i.set(false));
        #[cfg(debug_assertions)]
        {
            let n = VISITS.with(|v| v.replace(0));
            #[cfg(test)]
            {
                MOST.with(|m| m.set(m.get().max(n)));
                UNITS.with(|u| u.set(u.get() + 1));
            }
            if !crate::thread::panicking() {
                assert!(n <= 1, "a unit of a job's work visited {n} pages");
            }
        }
    }
}

/// Whether a unit is under way on this thread.
pub(crate) fn inside() -> bool {
    INSIDE.with(Cell::get)
}

/// Counts a page the unit under way on this thread visits, if one is: read, written, imaged,
/// freed, synced or erased, or reached while building an image (DESIGN.md 22.1).
#[inline]
pub(crate) fn visit() {
    #[cfg(debug_assertions)]
    if inside() {
        VISITS.with(|v| v.set(v.get() + 1));
    }
}

/// Counts an entry of bookkeeping walked on this thread, where the walk is done (a frame a
/// checkpoint wrote, a tree's creation): a test reads how many a unit walked, whatever run its
/// caller handed the walk (DESIGN.md 22.1).
#[inline]
pub(crate) fn walked() {
    #[cfg(test)]
    WALKED.with(|w| w.set(w.get() + 1));
}

/// The entries of bookkeeping walked on this thread, since the last call.
#[cfg(test)]
pub(crate) fn take_walked() -> usize {
    WALKED.with(|w| w.replace(0))
}

/// The most pages any unit visited on this thread, and the units run, since the last call.
#[cfg(test)]
pub(crate) fn most_and_units() -> (u32, u64) {
    (MOST.with(|m| m.replace(0)), UNITS.with(|u| u.replace(0)))
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod tests {
    use crate::io::{Completion, MemoryIO};

    /// A unit never waits (DESIGN.md 22.1): a wait inside one is refused before it waits, even on
    /// a completion already finished (mutant: the park census's check gone).
    #[test]
    #[should_panic(expected = "a park inside a unit of a job's work")]
    fn a_wait_inside_a_unit_is_refused() {
        let io = MemoryIO::new();
        let _unit = super::Unit::enter();
        crate::storage::atoll::park::wait(&io, &Completion::new_yield());
    }
}
