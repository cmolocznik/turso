//! Every IO step outside tests, by the function that takes it (DESIGN.md 22.5): a blocking caller
//! waits through `IO::wait`, which parks where nothing needs a step, so what steps the IO is a pump
//! (one step at a host's request, or a yield's one turn), a forward, or a wait of the trait's own
//! for a pumped backend. A step anywhere else is a blocking caller stepping in a loop of its own,
//! which spins on a backend whose step does nothing.
//!
//! A step is `step` or `try_step` (which also says what it found). The scan finds a step by its
//! receiver's name: `io`, `.io`, `io()` or `_io()`, which is how
//! every IO is reached in the workspace's production code. The trait's own waits step `self` and
//! are the implementation, not callers.

use crate::source_test::{code_of, rust_sources, without_test_modules};

/// Each step taken, with why it is no blocking wait.
const STEPS: &[(&str, &str, &str)] = &[
    (
        "core/stats.rs",
        "refresh_analyze_stats",
        "a yield's one turn of the IO between steps of the scan; anything else waits",
    ),
    (
        "core/types.rs",
        "poll",
        "a cooperative poll's pump: one step per poll, on a pumped backend alone",
    ),
    (
        "core/statement.rs",
        "wait_io",
        "a yield, which leaves nothing to wait for: one turn of the IO",
    ),
    (
        "core/storage/atoll/executor/mod.rs",
        "poll",
        "a cooperative wait on executor work: one IO step per poll, beside its one task step",
    ),
    (
        "core/storage/atoll/executor/mod.rs",
        "run_until",
        "a blocking wait on executor work running the executor: a worker's pump, parking once \
         nothing is claimed and the backend is empty",
    ),
    (
        "core/storage/atoll/executor/mod.rs",
        "work",
        "a worker's pump: one step per task step, and with nothing to claim, steps while the \
         backend delivers and parks once it is empty",
    ),
    (
        "core/storage/atoll/requests.rs",
        "drain",
        "close's wait for its count on a pumped backend; it parks on the others",
    ),
    (
        "core/storage/atoll/requests.rs",
        "step",
        "the counting IO forwards its embedder's step",
    ),
    (
        "core/storage/atoll/requests.rs",
        "try_step",
        "the counting IO forwards its embedder's step that says what it found",
    ),
    (
        "core/storage/atoll/requests/dispatch.rs",
        "step",
        "a database's IO thread over its pumped backend: a step owed after each record it serves \
         and each step that delivered, and a park once a step finds the backend empty",
    ),
    (
        "sdk-kit/src/rsapi.rs",
        "run_io",
        "the host's pump: one step at its request",
    ),
    (
        "bindings/javascript/src/lib.rs",
        "io_loop_sync",
        "the host's pump: one step at its request",
    ),
    (
        "bindings/javascript/src/lib.rs",
        "compute",
        "the host's asynchronous pump: one step at its request",
    ),
];

/// The workspace's production sources: every Rust file outside the trees and files that hold
/// only tests, benches, examples, simulators and fuzzers.
fn production_sources(root: &std::path::Path) -> Vec<(String, std::path::PathBuf)> {
    let skipped_dirs = [
        "target",
        "node_modules",
        "testing",
        "tests",
        "benches",
        "examples",
        "fuzz",
        "perf",
    ];
    let mut out = Vec::new();
    for top in std::fs::read_dir(root).unwrap() {
        let top = top.unwrap().path();
        let name = top.file_name().unwrap().to_string_lossy().into_owned();
        if !top.is_dir() || skipped_dirs.contains(&name.as_str()) || name.starts_with('.') {
            continue;
        }
        for path in rust_sources(&top) {
            let file = path.strip_prefix(root).unwrap().display().to_string();
            let test_only = file.split('/').any(|part| skipped_dirs.contains(&part))
                || file.ends_with("tests.rs")
                || file.ends_with("_test.rs")
                || file.ends_with("/build.rs");
            if !test_only {
                out.push((file, path));
            }
        }
    }
    out
}

/// Whether the code ending just before a `.step()` or `.try_step(` names an IO: a name `io`, or
/// one ending in `_io`, read or called.
fn names_an_io(before: &str) -> bool {
    let name = before.strip_suffix("()").unwrap_or(before);
    let start = name
        .rfind(|c: char| !(c.is_alphanumeric() || c == '_'))
        .map_or(0, |i| i + 1);
    let name = &name[start..];
    name == "io" || name.ends_with("_io")
}

#[test]
fn every_io_step_outside_tests_is_a_pump_a_forward_or_a_pumped_wait() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let mut found = Vec::new();
    let mut scanned = 0;
    for (file, path) in production_sources(root) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        scanned += 1;
        let code = without_test_modules(&code_of(&text));
        let steps = code
            .match_indices(".step()")
            .chain(code.match_indices(".try_step("));
        for (at, _) in steps {
            let before = code[..at].trim_end();
            if !names_an_io(before) {
                continue;
            }
            let caller: String = code[..at]
                .rfind("fn ")
                .map(|i| {
                    code[i + 3..]
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect()
                })
                .unwrap_or_default();
            found.push((file.clone(), caller));
        }
    }
    let mut listed: Vec<(String, String)> = STEPS
        .iter()
        .map(|(f, c, _)| (f.to_string(), c.to_string()))
        .collect();
    found.sort();
    listed.sort();
    // The scan reaches the crates it must: the steps listed are found where they are, each in
    // its own crate (core, sdk-kit, the JavaScript binding), so a walk that missed a tree fails.
    assert!(
        STEPS
            .iter()
            .all(|(file, _, _)| found.iter().any(|(f, _)| f == file)),
        "the census scanned {scanned} files and found {found:?}"
    );
    assert_eq!(
        found, listed,
        "an IO step outside tests not listed as a pump, a forward or a pumped wait, or one listed \
         that is gone"
    );
}
