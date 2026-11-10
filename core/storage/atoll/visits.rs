//! The pages a unit of a job's work visits (DESIGN.md 22.1): a page read, written, imaged, freed
//! or zero-filled, one page reached while building an image, one sync, one run of a table's
//! entries. Debug builds count them on the thread running the unit and assert at most one; tests
//! read the most any unit visited.
//!
//! The one exception until the segment preparer is a task of its own (22.16 step 8): a log call a
//! unit makes (an admission, a wait for durability) may run the log's segment preparation, the
//! interim adapter any caller runs, within the unit's budget of one start and one finish: a
//! segment started (its file created and allocated, on an inline backend its whole zero-fill and
//! data sync, a spare's directory sync) and a segment finished (its directory sync), whoever's
//! segment either is. The budget is kept in every build: past it the log offers the unit no
//! start and no finish, and leaves the step it needs to the next caller (`Log::prepare`). Debug
//! builds also count the starts and finishes a unit takes and assert at most one of each.

/// What the unit under way has spent of its budget of log preparation steps.
#[derive(Clone, Copy)]
struct Spent {
    started: bool,
    finished: bool,
}

crate::thread::thread_local! {
    /// What the unit under way on this thread has spent of its budget of one start and one finish
    /// of a log segment, if a unit is under way: in every build.
    static SPENT: std::cell::Cell<Option<Spent>> = const { std::cell::Cell::new(None) };
}

#[cfg(debug_assertions)]
crate::thread::thread_local! {
    /// The visits of the unit under way on this thread, if one is.
    static VISITS: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
    /// The log segments the unit under way started preparing.
    static SEGMENTS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// The log segments the unit under way finished preparing.
    static FINISHES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
crate::thread::thread_local! {
    /// The most visits any unit made on this thread since the last read.
    static MOST: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Units run on this thread since the last read.
    static UNITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The most segments any unit started preparing on this thread since the last read.
    static MOST_SEGMENTS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// The most segments any unit finished preparing on this thread since the last read.
    static MOST_FINISHES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Counts a visit of the unit under way on this thread, if one is.
pub(crate) fn visit() {
    #[cfg(debug_assertions)]
    VISITS.with(|v| {
        if let Some(n) = v.get() {
            v.set(Some(n + 1));
        }
    });
}

/// Spends the budget of the unit under way on this thread, if one is, on the log segment it starts,
/// and counts it.
pub(crate) fn segment() {
    SPENT.with(|s| {
        if let Some(spent) = s.get() {
            s.set(Some(Spent {
                started: true,
                ..spent
            }));
        }
    });
    #[cfg(debug_assertions)]
    if VISITS.with(|v| v.get().is_some()) {
        SEGMENTS.with(|s| s.set(s.get() + 1));
    }
}

/// Spends the budget of the unit under way on this thread, if one is, on the log segment it
/// finishes, and counts it.
pub(crate) fn finish() {
    SPENT.with(|s| {
        if let Some(spent) = s.get() {
            s.set(Some(Spent {
                finished: true,
                ..spent
            }));
        }
    });
    #[cfg(debug_assertions)]
    if VISITS.with(|v| v.get().is_some()) {
        FINISHES.with(|s| s.set(s.get() + 1));
    }
}

/// Whether this thread may start a log segment: outside a unit always, inside one until it has
/// started its one.
pub(crate) fn may_start_segment() -> bool {
    SPENT.with(|s| !s.get().is_some_and(|spent| spent.started))
}

/// Whether this thread may finish a log segment: outside a unit always, inside one until it has
/// finished its one.
pub(crate) fn may_finish_segment() -> bool {
    SPENT.with(|s| !s.get().is_some_and(|spent| spent.finished))
}

/// A unit under way, until it drops: its budget of one log segment, and in debug builds its
/// visits and segments counted, asserted at most one each as it drops.
pub(crate) struct Counted(());

impl Counted {
    pub(crate) fn enter() -> Self {
        SPENT.with(|s| {
            s.set(Some(Spent {
                started: false,
                finished: false,
            }))
        });
        #[cfg(debug_assertions)]
        {
            VISITS.with(|v| v.set(Some(0)));
            SEGMENTS.with(|s| s.set(0));
            FINISHES.with(|s| s.set(0));
        }
        Self(())
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        SPENT.with(|s| s.set(None));
        #[cfg(debug_assertions)]
        {
            let n = VISITS.with(|v| v.take()).unwrap_or(0);
            let segments = SEGMENTS.with(|s| s.replace(0));
            let finishes = FINISHES.with(|s| s.replace(0));
            #[cfg(test)]
            {
                MOST.with(|m| m.set(m.get().max(n)));
                UNITS.with(|u| u.set(u.get() + 1));
                MOST_SEGMENTS.with(|m| m.set(m.get().max(segments)));
                MOST_FINISHES.with(|m| m.set(m.get().max(finishes)));
            }
            if !crate::thread::panicking() {
                assert!(n <= 1, "a unit of a job's work visited {n} pages");
                assert!(
                    segments <= 1,
                    "a unit of a job's work started preparing {segments} log segments"
                );
                assert!(
                    finishes <= 1,
                    "a unit of a job's work finished preparing {finishes} log segments"
                );
            }
        }
    }
}

/// The most pages any unit visited on this thread, and the units run, since the last call.
#[cfg(test)]
pub(crate) fn most_and_units() -> (u32, u64) {
    (MOST.with(|m| m.replace(0)), UNITS.with(|u| u.replace(0)))
}

/// The most log segments any unit started preparing on this thread since the last call.
#[cfg(test)]
pub(crate) fn most_segments() -> u32 {
    MOST_SEGMENTS.with(|m| m.replace(0))
}

/// The most log segments any unit finished preparing on this thread since the last call.
#[cfg(test)]
pub(crate) fn most_finishes() -> u32 {
    MOST_FINISHES.with(|m| m.replace(0))
}
