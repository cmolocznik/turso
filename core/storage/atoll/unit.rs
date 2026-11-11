//! A unit of a job's work (DESIGN.md 22.1): a step does at most one, and a unit visits at most one
//! page and never waits, blocks or parks. A unit under way is marked on its thread while its guard
//! lives: every park refuses it ([`super::park`]), and debug builds count the pages it visits
//! ([`visit`]) and assert at most one as it ends. Tests read the most any unit visited.
//!
//! The one exception until the segment preparer is a task of its own (22.16 step 8): a log call a
//! unit makes (an admission, a wait for durability) may run the log's segment preparation, the
//! interim adapter any caller runs, within the unit's budget of one start and one finish, whoever's
//! segments they are: a start is a file created and allocated (on an inline backend its whole
//! zero-fill and data sync) or a spare renamed and its directory synced; a finish is the directory
//! sync that publishes the segment. The budget is kept in every build ([`may_start_segment`],
//! [`may_finish_segment`]): past it the log offers the unit no start and no finish and leaves the
//! step to the next caller.

use std::cell::Cell;
use std::marker::PhantomData;

crate::thread::thread_local! {
    /// Whether a unit is under way on this thread.
    static INSIDE: Cell<bool> = const { Cell::new(false) };
    /// Whether the unit under way on this thread has started a log segment, and finished one.
    static STARTED: Cell<bool> = const { Cell::new(false) };
    static FINISHED: Cell<bool> = const { Cell::new(false) };
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
    /// Log segments units on this thread started and finished preparing, since the last read.
    static STARTS: Cell<u32> = const { Cell::new(0) };
    static FINISHES: Cell<u32> = const { Cell::new(0) };
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
        STARTED.with(|s| s.set(false));
        FINISHED.with(|f| f.set(false));
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

/// Whether this thread may start preparing a log segment: outside a unit always, inside one until
/// it has started its one.
pub(crate) fn may_start_segment() -> bool {
    !inside() || !STARTED.with(Cell::get)
}

/// Whether this thread may finish preparing a log segment: outside a unit always, inside one until
/// it has finished its one.
pub(crate) fn may_finish_segment() -> bool {
    !inside() || !FINISHED.with(Cell::get)
}

/// The unit under way on this thread, if any, starts preparing a log segment: its budget's start.
pub(crate) fn segment_started() {
    if inside() {
        assert!(
            !STARTED.with(|s| s.replace(true)),
            "a unit of a job's work started preparing a second log segment"
        );
        #[cfg(test)]
        STARTS.with(|s| s.set(s.get() + 1));
    }
}

/// The unit under way on this thread, if any, finishes preparing a log segment: its budget's
/// finish.
pub(crate) fn segment_finished() {
    if inside() {
        assert!(
            !FINISHED.with(|f| f.replace(true)),
            "a unit of a job's work finished preparing a second log segment"
        );
        #[cfg(test)]
        FINISHES.with(|f| f.set(f.get() + 1));
    }
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

/// The log segments units on this thread started and finished preparing, since the last call.
#[cfg(test)]
pub(crate) fn segments() -> (u32, u32) {
    (
        STARTS.with(|s| s.replace(0)),
        FINISHES.with(|f| f.replace(0)),
    )
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
