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
const RANDOM: &str = "the thread's own generator, seeded once from the kernel's entropy, which \
                      waits only before the system's first seeding at boot";

/// Every method of `File`, ruled for `CountedFile` inside a frame.
const FILE: &[(&str, InFrame)] = &[
    ("lock_file", Refused),
    ("unlock_file", Refused),
    ("pread", Refused),
    ("pwrite", Refused),
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
