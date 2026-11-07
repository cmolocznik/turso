//! A panic in Atoll ends the process before anything it left half made can be read (DESIGN.md
//! 22.11, section 7.5). Builds Turso ships abort on a panic already; for a build that unwinds,
//! every destructor that lets Atoll's shared state go, publishes it, cleans it up or wakes anyone
//! for it calls [`abort_if_unwinding`] first, before it does any of that.

/// Ends the process if this thread is unwinding: the panic, its message already printed, passes
/// through a guard of shared state, which no other thread may then see let go.
#[inline]
pub(crate) fn abort_if_unwinding() {
    if crate::thread::panicking() {
        abort();
    }
}

#[cold]
#[inline(never)]
fn abort() -> ! {
    eprintln!(
        "Atoll: a panic reached a guard of shared state; ending the process (DESIGN.md 22.11)"
    );
    std::process::abort()
}

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod tests {
    use crate::storage::atoll::testing::{atoll_dir, atoll_sources, code_of};

    /// Destructors that let go only what they own, or hold memory alive without guarding any
    /// change: letting them go while a panic unwinds shows no other thread anything half made.
    /// Every other `Drop` in Atoll calls [`super::abort_if_unwinding`] first.
    const EXEMPT: &[(&str, &str, &str)] = &[
        (
            "aligned.rs",
            "AlignedBlock",
            "frees its own block, or lends it back unread",
        ),
        ("arc_cell.rs", "ArcCell", "drops its last reference"),
        ("arc_cell.rs", "Counted", "a test's count of live values"),
        (
            "buffer/cell.rs",
            "CompletionCell",
            "drops its last reference",
        ),
        ("buffer/memory.rs", "Mapping", "unmaps its own memory"),
        ("epoch.rs", "Registry", "frees its own chunks"),
        (
            "epoch.rs",
            "Entry",
            "frees its thread's slot, which guards no state",
        ),
        ("epoch.rs", "Epochs", "frees its own waits"),
        (
            "epoch.rs",
            "Step",
            "leaves an epoch: memory kept alive, no change guarded",
        ),
        ("epoch.rs", "Retired", "frees what it retired"),
        ("once.rs", "Baton", "drops the value it holds"),
        ("once.rs", "SetOnce", "drops the value it holds"),
        (
            "pins.rs",
            "Pinned",
            "unpins: memory kept alive, no change guarded",
        ),
        ("pins.rs", "PinCell", "drops its last reference"),
        ("pins.rs", "Counted", "a test's count of live values"),
        ("segmented.rs", "Segmented", "frees its own segments"),
        ("stack.rs", "Stack", "frees its own nodes"),
        ("stack.rs", "Popped", "frees its own node"),
        ("stack.rs", "FreeList", "frees its own nodes"),
    ];

    /// Each `impl Drop` in Atoll's source: its file, its type, and the first statement of its
    /// `drop`.
    fn drops() -> Vec<(String, String, String)> {
        let dir = atoll_dir();
        let mut out = Vec::new();
        for path in atoll_sources() {
            let file = path.strip_prefix(&dir).unwrap().display().to_string();
            let code = code_of(&std::fs::read_to_string(&path).unwrap());
            let mut rest = code.as_str();
            while let Some(at) = rest.find("Drop for ") {
                let after = &rest[at + "Drop for ".len()..];
                let ty: String = after
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
                    .collect();
                let body = after
                    .find("fn drop(&mut self)")
                    .expect("an impl Drop has a drop");
                let open = body + after[body..].find('{').unwrap() + 1;
                let first = after[open..]
                    .split(';')
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ");
                out.push((file.clone(), ty, first));
                rest = &after[open..];
            }
        }
        out
    }

    /// Every destructor in Atoll that lets shared state go, publishes it, cleans it up or wakes
    /// anyone for it ends the process first on a thread that unwinds; the rest are named here
    /// with why they need not, and a new one fails until it is one or the other.
    #[test]
    fn every_destructor_that_lets_shared_state_go_aborts_first_when_unwinding() {
        let drops = drops();
        assert!(
            drops.len() >= 40,
            "the census found {} destructors",
            drops.len()
        );
        let mut wrong = Vec::new();
        let mut checked = 0;
        for (file, ty, first) in &drops {
            let exempt = EXEMPT.iter().any(|(f, t, _)| f == file && t == ty);
            let checks = first.ends_with("abort_if_unwinding()");
            match (checks, exempt) {
                (true, false) => checked += 1,
                (false, true) => {}
                (true, true) => wrong.push(format!("{file}: {ty} is exempt yet checks")),
                (false, false) => wrong.push(format!(
                    "{file}: {ty}'s drop begins `{first}`, not the check, and is not exempt"
                )),
            }
        }
        for (file, ty, _) in EXEMPT {
            if !drops.iter().any(|(f, t, _)| f == file && t == ty) {
                wrong.push(format!("{file}: {ty} is exempt but has no destructor"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
        assert!(checked >= 21, "{checked} destructors check");
    }

    /// Tests in Atoll that panic, and how each runs: in a child process when the panic passes a
    /// guard of shared state, which ends the process, else as an ordinary test.
    const PANICKING: &[(&str, &str, &str)] = &[
        (
            "locks.rs",
            "a_panic_while_holding_an_atoll_lock_ends_the_process_before_it_is_let_go",
            "child",
        ),
        (
            "guarded.rs",
            "a_panic_while_holding_a_guarded_state_ends_the_process_before_a_contender_wakes",
            "child",
        ),
        (
            "latch.rs",
            "a_panic_while_holding_the_latch_ends_the_process_before_it_is_let_go",
            "child",
        ),
        (
            "buffer/kept/tests.rs",
            "a_change_that_asked_for_no_room_fails_the_invariant_check",
            "child",
        ),
        (
            "sql/trees/tests.rs",
            "a_write_without_the_gate_fails_at_the_write",
            "child",
        ),
        (
            "txn/gate/tests.rs",
            "a_write_without_the_gate_is_refused_at_commit",
            "child",
        ),
    ];

    /// Every `should_panic`, `catch_unwind` and child-process test in Atoll is listed with its
    /// kind, a child-process test calls `in_child`, and no `catch_unwind` is left outside tests.
    #[test]
    fn every_panicking_test_runs_as_its_kind_and_atoll_catches_no_unwind() {
        let dir = atoll_dir();
        let mut found = Vec::new();
        for path in atoll_sources() {
            let file = path.strip_prefix(&dir).unwrap().display().to_string();
            if file == "unwind.rs" {
                continue;
            }
            let code = code_of(&std::fs::read_to_string(&path).unwrap());
            for marker in ["should_panic", "catch_unwind", "in_child("] {
                let mut rest = code.as_str();
                while let Some(at) = rest.find(marker) {
                    let after = &rest[at..];
                    let name_at = after.find("fn ").map(|i| i + 3);
                    // The test a marker belongs to: the next fn for an attribute, the enclosing
                    // one for a call.
                    let name = if marker == "should_panic" {
                        name_at.map(|i| {
                            after[i..]
                                .chars()
                                .take_while(|c| c.is_alphanumeric() || *c == '_')
                                .collect::<String>()
                        })
                    } else {
                        let before = &code[..code.len() - rest.len() + at];
                        before.rfind("fn ").map(|i| {
                            before[i + 3..]
                                .chars()
                                .take_while(|c| c.is_alphanumeric() || *c == '_')
                                .collect::<String>()
                        })
                    };
                    found.push((file.clone(), marker, name.unwrap_or_default()));
                    rest = &after[marker.len()..];
                }
            }
        }
        let mut wrong = Vec::new();
        for (file, marker, name) in &found {
            if file == "testing.rs" {
                continue;
            }
            let listed = PANICKING
                .iter()
                .find(|(f, n, _)| f == file && n == name)
                .map(|(_, _, kind)| *kind);
            match (*marker, listed) {
                ("catch_unwind", _) => wrong.push(format!("{file}: {name} catches an unwind")),
                ("in_child(", Some("child")) | ("should_panic", Some("ordinary")) => {}
                _ => wrong.push(format!("{file}: {name} ({marker}) is not listed as such")),
            }
        }
        for (file, name, kind) in PANICKING {
            let marker = if *kind == "child" {
                "in_child("
            } else {
                "should_panic"
            };
            if !found
                .iter()
                .any(|(f, m, n)| f == file && *m == marker && n == name)
            {
                wrong.push(format!("{file}: {name} is listed as {kind} but is not one"));
            }
        }
        assert!(found.len() >= PANICKING.len(), "the census found {found:?}");
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}
