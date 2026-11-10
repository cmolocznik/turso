//! Every place Atoll blocks its thread (DESIGN.md 9.1.3, *Parks*): an IO wait, a wait for a
//! completion, a thread's park. Each refuses while the thread delivers held wakers (a waker
//! schedules and returns, and never blocks) and, in debug builds, asserts that no change scope is
//! open on the thread, so nothing is held across a park and a park delivers nothing. The callers
//! that actually block come here; a cooperative method that returns a wait never does. A census
//! test (below) finds no blocking call anywhere else in the Atoll tree.

use crate::io::{Completion, IO};
use crate::storage::atoll::notify;
use crate::Result;

#[inline]
fn before(what: &'static str) {
    notify::refuse_while_delivering(what);
    debug_assert!(
        !notify::scope_open(),
        "a thread parked with a change scope open, its wakers held across the park"
    );
    debug_assert_eq!(
        holds::count(),
        0,
        "a thread parked holding a Step or the change gate (W1, DESIGN.md 9.1.3)"
    );
}

/// The stack-scoped holds of the pool this thread has alive (DESIGN.md 9.1.3, *What W1 counts*):
/// Steps, which every page guard carries, and the change gate. Counted in debug builds only, by
/// each hold's making and its drop, which run on one thread (each is `!Send`); asserted zero at
/// every park and at every wait on memory. A release build keeps no count.
pub(crate) mod holds {
    #[cfg(all(debug_assertions, not(atoll_loom)))]
    crate::thread::thread_local! {
        static HELD: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// A hold made on this thread.
    #[inline]
    pub(crate) fn raise() {
        #[cfg(all(debug_assertions, not(atoll_loom)))]
        HELD.with(|h| h.set(h.get() + 1));
    }

    /// A hold of this thread dropped.
    #[inline]
    pub(crate) fn lower() {
        #[cfg(all(debug_assertions, not(atoll_loom)))]
        HELD.with(|h| {
            let n = h.get();
            debug_assert!(n > 0, "a hold dropped that was not counted");
            h.set(n - 1);
        });
    }

    /// Asserts, in debug builds, that this thread holds none: the place a wait on memory is
    /// taken (W1).
    #[inline]
    #[track_caller]
    pub(crate) fn count_is_zero(what: &'static str) {
        debug_assert_eq!(count(), 0, "{what} (W1, DESIGN.md 9.1.3)");
    }

    /// The holds this thread has alive; zero in a release build.
    #[inline]
    pub(crate) fn count() -> usize {
        #[cfg(all(debug_assertions, not(atoll_loom)))]
        return HELD.with(std::cell::Cell::get);
        #[cfg(not(all(debug_assertions, not(atoll_loom))))]
        0
    }
}

/// Steps `io` until `c` finishes.
pub(crate) fn wait(io: &dyn IO, c: &Completion) {
    before("an IO wait on a thread delivering held wakers");
    io.wait(c)
}

/// Steps `io` until `c` finishes, and returns its error.
pub(crate) fn wait_for_completion(io: &dyn IO, c: Completion) -> Result<()> {
    before("a wait for a completion on a thread delivering held wakers");
    io.wait_for_completion(c)
}

/// Parks this thread until it is unparked.
pub(crate) fn thread() {
    before("a park on a thread delivering held wakers");
    crate::thread::park()
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod tests {
    use crate::storage::atoll::testing::{atoll_dir, atoll_sources, code_of};

    /// Blocking calls the census looks for, and the files allowed to make them: this module, and
    /// `requests.rs`, whose `impl IO` passes each call through to the IO it wraps (a backend, not
    /// a park).
    const BLOCKING: &[&str] = &[
        "thread::park(",
        "io.wait(",
        ".wait_for_completion(",
        ".block_on(",
        "Condvar",
        "park_timeout(",
    ];

    /// A file's production code: none of a test-only file, and a file's code up to its inline
    /// test module, which by the house layout comes last.
    fn production(file: &str, code: &str) -> Option<String> {
        let name = file.rsplit('/').next().unwrap_or(file);
        if name == "tests.rs"
            || name.ends_with("_tests.rs")
            || name == "testing.rs"
            || name == "bench.rs"
            || file.contains("/tests/")
            || file.starts_with("tests/")
        {
            return None;
        }
        let mut end = code.len();
        let mut at = 0;
        while let Some(i) = code[at..].find("#[cfg(") {
            let start = at + i;
            let line_end = code[start..].find('\n').map_or(code.len(), |e| start + e);
            let attr = &code[start..line_end];
            let next = code[line_end..].trim_start();
            if attr.contains("test") && next.starts_with("mod tests {") {
                end = start;
                break;
            }
            at = line_end;
        }
        Some(code[..end].to_string())
    }

    #[test]
    fn no_blocking_call_in_atoll_is_made_outside_park() {
        let dir = atoll_dir();
        let mut found = Vec::new();
        let mut scanned = 0;
        for path in atoll_sources() {
            let file = path.strip_prefix(&dir).unwrap().display().to_string();
            if file == "park.rs" || file == "locks.rs" {
                continue;
            }
            let Some(code) = production(&file, &code_of(&std::fs::read_to_string(&path).unwrap()))
            else {
                continue;
            };
            scanned += 1;
            for (n, line) in code.lines().enumerate() {
                for call in BLOCKING {
                    if line.contains(call) {
                        let allowed = file == "requests.rs"
                            && (line.trim() == "self.io.wait(c)"
                                || line.trim() == "self.io.wait_for_completion(c)");
                        if !allowed {
                            found.push(format!("{file}:{}: {}", n + 1, line.trim()));
                        }
                    }
                }
            }
        }
        assert!(scanned > 50, "the census read {scanned} files");
        assert!(
            found.is_empty(),
            "blocking calls outside park:\n{}",
            found.join("\n")
        );
    }

    /// Every hold W1 counts is `!Send`, so its making and its drop run on one thread and a
    /// thread-local count is the count of that thread's holds: a type that is `Send` makes the
    /// call below ambiguous and the build fails.
    #[test]
    fn every_hold_w1_counts_stays_on_its_thread() {
        trait NotSend<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotSend<()> for T {}
        impl<T: ?Sized + Send> NotSend<u8> for T {}
        <crate::storage::atoll::epoch::Step<'static> as NotSend<_>>::check();
        <crate::storage::atoll::store::Change<'static> as NotSend<_>>::check();
        <crate::storage::atoll::buffer::Optimistic<'static> as NotSend<_>>::check();
        <crate::storage::atoll::buffer::Exclusive<'static> as NotSend<_>>::check();
    }

    /// The census's control: a planted call in a production line is found, one in a test module
    /// is not.
    #[test]
    fn the_blocking_census_sees_a_planted_call() {
        let planted = "fn f() {\n    crate::thread::park();\n}\n#[cfg(test)]\nmod tests {\n    fn g() { io.wait(&c); }\n}\n";
        let code = production("planted.rs", planted).unwrap();
        assert!(code.contains("thread::park("));
        assert!(!code.contains("io.wait("));
        assert!(production("buffer/tests.rs", planted).is_none());
    }
}
