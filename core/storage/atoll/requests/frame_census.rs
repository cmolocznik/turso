//! What each method of `Counted` and `CountedFile` does inside a cooperative frame (DESIGN.md
//! 22.18, *Which calls can block*; ASYNC1 D2a §5, the auditor's #4839 and #4847). A call that can
//! wait on its caller's thread (a system call, a backend's mutex its delivery holds, a wait for a
//! request, a sleep, a blocking host's step) ends the suite in a test build, since a route that reaches one in a frame is to be
//! queued in its owner's record; every other is allowed for the reason given. Every method of
//! `File` and `IO` is ruled, and `Counted` and `CountedFile` each forward every one themselves,
//! calling `refuse_in_frame` exactly in those refused: a method left to the trait's default would
//! run past the wrapper's count and its census both.

use crate::source_test::{code_of, methods_of};

/// How a method is ruled inside a cooperative frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InFrame {
    /// It can wait on its caller's thread: refused, which ends the suite.
    Refused,
    /// It cannot wait, for the reason given.
    Allowed(&'static str),
}

use InFrame::*;

const NO_CALL: &str = "answers from what the IO holds, making no system call";
const OFFER: &str = "the ring offer: a try of the ring's lock, and one submission and one \
                     non-waiting enter only when it is free and its queue has room (ASYNC1 D2d §4)";
const RANDOM: &str = "the thread's own generator, seeded once from the kernel's entropy, which \
                      waits only before the system's first seeding at boot";

/// Every method of `File`, ruled for `CountedFile` inside a frame.
const FILE: &[(&str, InFrame)] = &[
    ("lock_file", Refused),
    ("unlock_file", Refused),
    ("pread", Refused),
    ("pwrite", Refused),
    ("try_pread_now", Allowed(OFFER)),
    ("try_pwrite_now", Allowed(OFFER)),
    ("sync", Refused),
    ("sync_data", Refused),
    ("supports_durable_writes", Allowed(NO_CALL)),
    ("pwrite_durable", Refused),
    ("allocate", Refused),
    ("pwritev", Refused),
    ("size", Refused),
    ("truncate", Refused),
    ("has_hole", Refused),
    ("punch_hole", Refused),
    ("shared_wal_lock_byte", Refused),
    ("shared_wal_try_lock_byte", Refused),
    ("shared_wal_probe_exclusive_byte", Refused),
    ("shared_wal_probe_exclusive_while_shared_byte", Refused),
    ("shared_wal_unlock_byte", Refused),
    ("shared_wal_set_len", Refused),
    ("shared_wal_map", Refused),
];

/// Every method of `IO`, ruled for `Counted` inside a frame.
const IO: &[(&str, InFrame)] = &[
    ("open_file", Refused),
    ("open_shared_wal_file", Refused),
    ("remove_file", Refused),
    ("sync_directory", Refused),
    ("rename_file", Refused),
    ("supports_shared_wal_coordination", Allowed(NO_CALL)),
    // A step, either kind, runs the backend's delivery on the caller's thread, under whatever the
    // backend locks: in a database opened async the IO thread drives what it submitted, and a frame
    // steps nothing (D2d; #4851).
    ("step", Refused),
    ("try_step", Refused),
    ("says_what_a_step_found", Allowed(NO_CALL)),
    ("withdraw", Refused),
    ("delivery", Allowed(NO_CALL)),
    ("wait", Refused),
    // The ring's takes its mutex, which its delivery thread holds while it delivers, and enters the
    // kernel to queue the cancels (#4847).
    ("cancel", Refused),
    ("wait_retired", Refused),
    ("drain_completions", Refused),
    ("wait_for_completion", Refused),
    ("generate_random_number", Allowed(RANDOM)),
    ("fill_bytes", Allowed(RANDOM)),
    (
        "get_memory_io",
        Allowed("a memory backend, holding no file"),
    ),
    // The ring's takes the same mutex and updates the kernel's table of buffers (#4847).
    ("register_fixed_buffer", Refused),
    ("yield_now", Allowed("a yield returns")),
    ("sleep", Refused),
    ("file_id", Refused),
];

/// Every method of `File` and `IO` is ruled for a frame, and `CountedFile` and `Counted` forward
/// each themselves, calling `refuse_in_frame` in exactly the ones refused (mutants: a refused
/// method's call removed; a method left to the trait's default).
#[test]
fn every_method_of_counted_is_ruled_for_a_frame_and_refused_there_as_ruled() {
    let read = |path: &str| {
        code_of(&std::fs::read_to_string(format!("{}/{path}", env!("CARGO_MANIFEST_DIR"))).unwrap())
    };
    let (io, mine) = (read("io/mod.rs"), read("storage/atoll/requests.rs"));
    let mut wrong = Vec::new();
    let mut refusing = 0;
    for (traits, counted, ruled) in [
        ("pub trait File", "impl File for CountedFile {", FILE),
        ("pub trait IO", "impl IO for Counted {", IO),
    ] {
        let mut declared: Vec<String> = methods_of(&io, traits)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        let mut listed: Vec<String> = ruled.iter().map(|(name, _)| name.to_string()).collect();
        declared.sort();
        listed.sort();
        assert_eq!(
            declared, listed,
            "{traits}: a method not ruled for a frame, or one ruled that is gone"
        );
        let forwarded = methods_of(&mine, counted);
        let mut names: Vec<&str> = forwarded.iter().map(|(name, _)| name.as_str()).collect();
        names.sort();
        assert_eq!(
            names,
            listed.iter().map(String::as_str).collect::<Vec<_>>(),
            "{counted}: a method left to the trait's default, or one that is not the trait's"
        );
        for (name, body) in &forwarded {
            let rule = ruled.iter().find(|(n, _)| n == name).unwrap().1;
            let refuses = body.contains("refuse_in_frame(");
            refusing += usize::from(refuses);
            if refuses != (rule == Refused) {
                wrong.push(format!("{counted} {name} ({rule:?}): refuses {refuses}"));
            }
        }
    }
    // The scan read every refused forward's body as refusing: 36 here.
    let refused = FILE.iter().chain(IO).filter(|(_, r)| *r == Refused).count();
    assert_eq!(
        (refusing, refused),
        (36, 36),
        "(forwards read refusing, refused)"
    );
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Every forward of `Counted` and `CountedFile` whose method `delivery_matrix.rs` refuses on a
/// database's IO thread refuses it there itself, whatever the backend beneath (#4851): a wait on a
/// request, on a lock's other holder or on time never reaches a backend from that thread, so the
/// rule holds over a backend whose own call carries no guard (mutant: a refused forward's guard
/// removed).
#[test]
fn every_forward_the_io_thread_may_not_make_refuses_it_there() {
    use crate::io::delivery_matrix::{Rule, IO_THREAD_FILE, IO_THREAD_IO};
    let mine = code_of(
        &std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/storage/atoll/requests.rs"
        ))
        .unwrap(),
    );
    let mut wrong = Vec::new();
    let mut guarding = 0;
    for (counted, ruled) in [
        ("impl File for CountedFile {", IO_THREAD_FILE),
        ("impl IO for Counted {", IO_THREAD_IO),
    ] {
        let forwarded = methods_of(&mine, counted);
        for &(name, rule) in ruled {
            let Some((_, body)) = forwarded.iter().find(|(n, _)| n == name) else {
                wrong.push(format!("{counted} {name}: not forwarded"));
                continue;
            };
            let guards = body.contains("refuse_on_any_delivering_thread(");
            guarding += usize::from(guards);
            if guards != (rule == Rule::Refused) {
                wrong.push(format!("{counted} {name} ({rule:?}): guards {guards}"));
            }
        }
    }
    // The waits, the sleep, and the shared WAL's two calls that wait for a holder: 7 here.
    assert_eq!(guarding, 7, "forwards read guarding");
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Every site that queues a request into one of the dispatch's records for its IO threads, by the
/// function that publishes it, with what admits the request there: the queued-request census (a
/// site outside the list fails it, and so does one listed and gone).
const QUEUED_SITES: &[(&str, &str)] = &[
    (
        "after_write",
        "a chain's follow-on, made at its first request's retirement in the record held for the chain",
    ),
    (
        "queue_read",
        "a metadata read, under its read credit, whose index is the record's",
    ),
    (
        "queue_frame",
        "a frame request, under its frame's held record",
    ),
    (
        "frame_part_retired",
        "a write-out's rest, made at its short part's retirement in the frame request's own \
         record, held from the first part to the last (D2d §3)",
    ),
    (
        "queue_slot_read",
        "a rollback's read-back read, under its transaction slot's record taken at rest",
    ),
];

/// Every record the dispatch publishes to its IO threads is published by one of the sites listed,
/// each inside `impl Dispatch`, and each site listed still publishes one.
#[test]
fn every_site_that_queues_a_request_for_the_io_threads_is_listed() {
    let code = code_of(
        &std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/storage/atoll/requests/dispatch.rs"
        ))
        .unwrap(),
    );
    let publishes = |body: &str| body.matches(".publish(&self.bits").count();
    let sites: Vec<(String, usize)> = methods_of(&code, "impl Dispatch {")
        .into_iter()
        .filter_map(|(name, body)| (publishes(body) > 0).then(|| (name, publishes(body))))
        .collect();
    let mut found: Vec<&str> = sites.iter().map(|(name, _)| name.as_str()).collect();
    let mut listed: Vec<&str> = QUEUED_SITES.iter().map(|(name, _)| *name).collect();
    found.sort();
    listed.sort();
    assert_eq!(
        found, listed,
        "a site that queues a request for the IO threads not listed, or one listed that no longer does"
    );
    assert!(
        sites.iter().all(|(_, n)| *n == 1),
        "a site publishing more than one record: {sites:?}"
    );
    assert_eq!(
        publishes(&code),
        sites.len(),
        "a record published outside `impl Dispatch`"
    );
}

/// Functions that make pages cold and step a statement in a frame with the IO thread left free,
/// each with why its stops need no hold. A cold read stops a call in a frame only while it is
/// unfinished when the statement looks whether to park, and a free IO thread may finish it first:
/// alone the IO thread is the slower, under load the statement's thread is.
const COLD_STEPS_WITH_THE_IO_THREAD_FREE: &[(&str, &str)] = &[];

/// Functions with a match arm for a framed step's IO stop and none for its yield, each with why
/// no yield reaches it. A look-again yield an instruction returns reaches the statement's caller
/// as a yield, and so does the stop for the budget of a nested program (a trigger's, stepped in
/// the poll's own dispatch loop). A helper statement, stepped with `step()`, has no stop for the
/// budget.
/// Atoll's functions that step a statement with `step()` and have a match arm for its IO stop and
/// none for its yield, by file and name, each with why no yield reaches it. A step may come back `Yield` to its caller outside
/// a frame as inside one (`StepResult::Yield`'s contract), and a loop that ends there fails a healthy
/// run (YIELD-STEP1).
const STEP_ARMS_WITHOUT_A_YIELD_ARM: &[(&str, &str)] = &[(
    "sql/mode/tests/poll_budget.rs: a_call_after_a_stop_for_the_budget_checks_before_its_first_instruction",
    "a statement outside Atoll mode that runs no nested program, so no instruction of it returns \
     a yield, and its match ends the test at any result it has no arm for",
)];

const IO_ARMS_WITHOUT_A_YIELD_ARM: &[(&str, &str)] = &[(
    "a_call_after_a_stop_for_the_budget_checks_before_its_first_instruction",
    "a statement outside Atoll mode that runs no nested program, so no instruction of it returns \
     a yield, and its match ends the test at any result it has no arm for",
)];

/// A test that steps a statement in a cooperative frame (`Statement::step_in_frame`) makes the
/// stops it asserts on (the auditor's #5966, after 4.1's first gate): one that makes pages cold
/// holds the IO thread through the call, and one with an arm for an IO stop has one for a yield.
/// Every function that breaks either rule is listed with its reason, and every one listed still
/// breaks it.
#[test]
fn every_test_that_steps_in_a_frame_makes_its_stops_certain() {
    let (mut steppers, mut found) = (0, Vec::new());
    let core = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for path in crate::source_test::rust_sources(core) {
        let text = std::fs::read_to_string(&path).unwrap();
        let code = code_of(&text);
        let (stepping, broken) = stops_left_to_chance(&code);
        steppers += stepping;
        found.extend(broken);
        // The rule for a step outside a frame holds Atoll's code, named by file and function.
        if let Ok(atoll) = path.strip_prefix(core.join("storage/atoll")) {
            found.extend(
                step_arms_without_a_yield_arm(&code)
                    .into_iter()
                    .map(|name| {
                        (
                            LeftToChance::StepArmWithoutAYieldArm,
                            format!("{}: {name}", atoll.display()),
                        )
                    }),
            );
        }
    }
    assert!(steppers > 0, "the census found no step in a frame at all");
    for (rule, listed) in [
        (
            LeftToChance::ColdStepWithTheIoThreadFree,
            COLD_STEPS_WITH_THE_IO_THREAD_FREE,
        ),
        (
            LeftToChance::IoArmWithoutAYieldArm,
            IO_ARMS_WITHOUT_A_YIELD_ARM,
        ),
        (
            LeftToChance::StepArmWithoutAYieldArm,
            STEP_ARMS_WITHOUT_A_YIELD_ARM,
        ),
    ] {
        let mut found: Vec<&str> = found
            .iter()
            .filter(|(r, _)| *r == rule)
            .map(|(_, name)| name.as_str())
            .collect();
        let mut listed: Vec<&str> = listed.iter().map(|(name, _)| *name).collect();
        found.sort();
        listed.sort();
        assert_eq!(
            found, listed,
            "{rule:?}: a function that steps in a frame and is not listed, or one listed that no longer does"
        );
    }
}

/// The census finds each rule broken in a planted file: the shape of 4.1's first schema-parse
/// test, a cold step reached through a helper, and neither in a function that holds the IO thread
/// itself or steps through one that does.
#[test]
fn the_framed_step_census_sees_a_planted_stop_left_to_chance() {
    let planted = code_of(
        r#"
        fn chill(db: &Database) {
            atoll.pool().evict_all_clean();
        }
        fn polled(stmt: &mut Statement) -> Result<StepResult> {
            stmt.step_in_frame(Waker::noop())
        }
        fn held(db: &Database, stmt: &mut Statement) -> Option<Result<StepResult>> {
            let hold = io_thread_held_before_idle_unregistered(atoll.requests_for_test())?;
            let step = stmt.step_in_frame(Waker::noop());
            drop(hold);
            Some(step)
        }
        fn the_first_gates_shape() {
            let chilled = chill(&db);
            loop {
                match x.step_in_frame(Waker::noop()) {
                    Ok(crate::StepResult::IO) => stops += 1,
                    Ok(other) => break,
                    Err(e) => break,
                }
            }
        }
        fn cold_through_a_helper() {
            chill(&db);
            assert!(matches!(polled(&mut x), Ok(StepResult::IO)));
        }
        fn warm_with_no_arm_for_a_yield() {
            match polled(&mut x) {
                Ok(StepResult::Row | StepResult::IO) => {}
                other => panic!(),
            }
        }
        fn holding_through_its_helper() {
            chill(&db);
            match held(&db, &mut x) {
                Some(Ok(StepResult::IO | StepResult::Yield)) => {}
                other => panic!(),
            }
        }
        fn holding_itself() {
            chill(&db);
            let hold = io_thread_held_before_idle(atoll.requests_for_test());
            let first = polled(&mut x);
        }
        fn stepping_nothing_in_a_frame() {
            chill(&db);
            match x.step() {
                Ok(StepResult::IO) => {}
                other => panic!(),
            }
        }
        "#,
    );
    let (stepping, mut broken) = stops_left_to_chance(&planted);
    broken.sort();
    assert_eq!(
        stepping, 7,
        "polled, held and the five that step through them or themselves"
    );
    assert_eq!(
        broken,
        [
            (
                LeftToChance::ColdStepWithTheIoThreadFree,
                "cold_through_a_helper".to_string()
            ),
            (
                LeftToChance::ColdStepWithTheIoThreadFree,
                "the_first_gates_shape".to_string()
            ),
            (
                LeftToChance::IoArmWithoutAYieldArm,
                "the_first_gates_shape".to_string()
            ),
            (
                LeftToChance::IoArmWithoutAYieldArm,
                "warm_with_no_arm_for_a_yield".to_string()
            ),
        ]
    );
}

/// Functions that count the full checkpoints asked for beside a checkpointing thread and do not
/// wait for the thread as they write, each with why the count is not the thread's speed to
/// decide.
const FULL_COUNTS_LEFT_TO_THE_THREAD: &[(&str, &str)] = &[];

/// A test that opens its database with a checkpointing thread and reads how many full
/// checkpoints were asked for waits for the thread as it writes (the auditor's #6000). A
/// transaction's end asks for a full checkpoint once the redo point trails by twice the log
/// budget, and whether it comes to that is the thread's speed to decide unless the writer waits
/// for the thread's checkpoint: alone the thread keeps up, under load it does not. Every
/// function that reads the count with no such wait is listed with its reason, and every one
/// listed still does.
#[test]
fn every_count_of_full_checkpoints_beside_a_checkpointing_thread_waits_for_the_thread() {
    let (mut readers, mut found) = (0, Vec::new());
    let core = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for path in crate::source_test::rust_sources(core) {
        let text = std::fs::read_to_string(&path).unwrap();
        let (reading, unwaited) = full_counts_left_to_the_thread(&code_of(&text));
        readers += reading;
        found.extend(unwaited);
    }
    assert!(
        readers > 0,
        "the census found no count of full checkpoints beside a checkpointing thread"
    );
    let mut found: Vec<&str> = found.iter().map(String::as_str).collect();
    let mut listed: Vec<&str> = FULL_COUNTS_LEFT_TO_THE_THREAD
        .iter()
        .map(|(name, _)| *name)
        .collect();
    found.sort();
    listed.sort();
    assert_eq!(
        found, listed,
        "a function that counts full checkpoints beside a checkpointing thread with no wait for it and is not listed, or one listed that no longer does"
    );
}

/// The census finds the count read with no wait in a planted file, passes the same function once
/// it waits, and passes a count read with no checkpointing thread.
#[test]
fn the_full_count_census_sees_a_planted_count_left_to_the_thread() {
    let planted = |wait: &str| {
        code_of(&format!(
            r#"
        fn a_count_beside_a_thread() {{
            let db = open_with(&io, path, threaded());
            let fulls = atoll.checkpointer().fulls_requested();
            for _ in 0..rows {{
                conn.execute(insert).unwrap();
                {wait}
            }}
            assert_eq!(atoll.checkpointer().fulls_requested(), fulls);
        }}
        fn a_count_with_no_thread() {{
            let db = open(&io, path);
            let fulls = atoll.checkpointer().fulls_requested();
            assert_eq!(atoll.checkpointer().fulls_requested(), fulls);
        }}
        "#
        ))
    };
    assert_eq!(
        full_counts_left_to_the_thread(&planted("")),
        (1, vec!["a_count_beside_a_thread".to_string()]),
        "the census passes a count left to the thread"
    );
    let waiting = "while atoll.checkpointer().lag() >= budget { std::thread::yield_now(); }";
    assert_eq!(
        full_counts_left_to_the_thread(&planted(waiting)),
        (1, Vec::new()),
        "the census refuses a count whose writer waits for the thread"
    );
}

/// How many functions of one file's `code` read the count of full checkpoints asked for with a
/// checkpointing thread in the same function, and each that holds no wait for the thread.
fn full_counts_left_to_the_thread(code: &str) -> (usize, Vec<String>) {
    let (mut readers, mut unwaited) = (0, Vec::new());
    for (name, body) in functions_of(code) {
        let beside_a_thread = body.contains("threaded()") || body.contains("Checkpointing::Thread");
        if !beside_a_thread || !body.contains(".fulls_requested()") {
            continue;
        }
        readers += 1;
        if !waits_for_the_thread(body) {
            unwaited.push(name.to_string());
        }
    }
    (readers, unwaited)
}

/// Whether `body` holds a loop that goes on while the checkpointer's lag is too large: a `while`
/// whose condition reads `.lag()`.
fn waits_for_the_thread(body: &str) -> bool {
    body.match_indices("while ").any(|(at, _)| {
        let condition = &body[at..];
        let condition = &condition[..condition.find('{').unwrap_or(condition.len())];
        condition.contains(".lag()")
    })
}

/// How a function that steps a statement in a frame leaves a stop it reads to chance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LeftToChance {
    /// It makes pages cold and steps with the IO thread free to finish the read first.
    ColdStepWithTheIoThreadFree,
    /// It has a match arm for an IO stop and none for a yield.
    IoArmWithoutAYieldArm,
    /// It steps a statement with `step()` itself, and has a match arm for an IO stop and none for
    /// a yield.
    StepArmWithoutAYieldArm,
}

/// How many functions of one file's `code` step a statement in a frame, themselves or through
/// another function of the file, and each that breaks a rule. A function that takes the hold of
/// the IO thread itself covers every step it makes or reaches.
fn stops_left_to_chance(code: &str) -> (usize, Vec<(LeftToChance, String)>) {
    let functions = functions_of(code);
    if !functions.iter().any(|(_, body)| steps_itself(body)) {
        return (0, Vec::new());
    }
    let calls: Vec<Vec<usize>> = functions
        .iter()
        .map(|(_, body)| {
            (0..functions.len())
                .filter(|&j| calls_function(body, functions[j].0))
                .collect()
        })
        .collect();
    // Each property spreads from a function to its callers until nothing changes.
    let spread = |holds_stop: bool, own: &dyn Fn(&str) -> bool| -> Vec<bool> {
        let stopped = |i: usize| holds_stop && holds_the_io_thread(functions[i].1);
        let mut has: Vec<bool> = (0..functions.len())
            .map(|i| !stopped(i) && own(functions[i].1))
            .collect();
        loop {
            let before = has.clone();
            for i in 0..functions.len() {
                if !stopped(i) && calls[i].iter().any(|&j| j != i && before[j]) {
                    has[i] = true;
                }
            }
            if has == before {
                return has;
            }
        }
    };
    let steps = spread(false, &steps_itself);
    let steps_unheld = spread(true, &steps_itself);
    let cold = spread(false, &|body| body.contains(".evict_all"));
    let mut broken = Vec::new();
    for (i, (name, body)) in functions.iter().enumerate() {
        if cold[i] && steps_unheld[i] {
            broken.push((LeftToChance::ColdStepWithTheIoThreadFree, name.to_string()));
        }
        if steps[i] && has_an_arm_for_io(body) && !body.contains("StepResult::Yield") {
            broken.push((LeftToChance::IoArmWithoutAYieldArm, name.to_string()));
        }
    }
    (steps.iter().filter(|s| **s).count(), broken)
}

/// The functions of one file's `code` that step a statement with `step()` themselves and have a
/// match arm for its IO stop and none for its yield.
fn step_arms_without_a_yield_arm(code: &str) -> Vec<String> {
    functions_of(code)
        .into_iter()
        .filter(|(_, body)| {
            body.contains(".step()")
                && has_an_arm_for_io(body)
                && !body.contains("StepResult::Yield")
        })
        .map(|(name, _)| name.to_string())
        .collect()
}

fn steps_itself(body: &str) -> bool {
    body.contains(".step_in_frame(")
}

fn holds_the_io_thread(body: &str) -> bool {
    body.contains("io_thread_held_before_idle")
}

/// Whether `body` calls a function or method named `name`: the name, then `(`, after anything
/// that is not part of a longer name, and not where a nested function of that name is declared.
fn calls_function(body: &str, name: &str) -> bool {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    body.match_indices(name).any(|(at, _)| {
        body[at + name.len()..].starts_with('(')
            && !body[..at].chars().next_back().is_some_and(ident)
            && !body[..at].ends_with("fn ")
    })
}

/// Whether `body` has a match arm whose pattern names an IO stop: `StepResult::IO`, then the
/// pattern's closing parentheses, then `=>` or another alternative.
fn has_an_arm_for_io(body: &str) -> bool {
    body.match_indices("StepResult::IO").any(|(at, found)| {
        let after =
            body[at + found.len()..].trim_start_matches(|c: char| c == ')' || c.is_whitespace());
        after.starts_with("=>") || after.starts_with('|')
    })
}

/// Every function `code` declares, at any depth, by name, with its body, braces included. A
/// function declared inside another is in the outer's body too.
fn functions_of(code: &str) -> Vec<(&str, &str)> {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    let (mut functions, mut from) = (Vec::new(), 0);
    while let Some(at) = code[from..].find("fn ").map(|i| from + i) {
        from = at + 3;
        if code[..at].chars().next_back().is_some_and(ident) {
            continue;
        }
        let name = &code[from..];
        let name = &name[..name.find(|c: char| !ident(c)).unwrap_or(name.len())];
        if name.is_empty() {
            continue;
        }
        // The body opens at the first brace outside the signature's parentheses and brackets; a
        // semicolon there first is a declaration with none.
        let mut depth = 0i32;
        for (i, c) in code[from..].char_indices() {
            match c {
                '(' | '[' => depth += 1,
                ')' | ']' => depth -= 1,
                ';' if depth == 0 => break,
                '{' if depth == 0 => {
                    let open = from + i;
                    functions.push((name, &code[open..open + braced(&code[open..])]));
                    break;
                }
                _ => {}
            }
        }
    }
    functions
}

/// The length of the braced block `code` opens with, its closing brace included.
fn braced(code: &str) -> usize {
    let mut depth = 0usize;
    for (i, c) in code.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return i + 1;
                }
            }
            _ => {}
        }
    }
    panic!("a block that never closes")
}
