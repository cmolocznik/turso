//! What each method of `File` and `IO` does on a ring's delivery thread (DESIGN.md 22.5). The
//! thread never makes a call that can wait: nothing else would deliver what it waits for, and every
//! delivery behind it would wait too. So every method that can wait (on the file system's data, its
//! metadata or its names, on another holder of a lock, on time, or on a request) is refused there,
//! and every other is allowed for the reason given. Every method of the two traits is ruled: one
//! added to either fails the census below until it is. The ring's own test calls each on a thread
//! marked as the delivery thread and holds the ring to these rulings
//! (`io_uring::tests::each_method_on_the_delivery_thread_is_refused_or_allowed_as_ruled`), and the
//! callback census refuses a callback that calls a refused method.

use crate::source_test::code_of;

/// How a method is ruled on the delivery thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rule {
    /// It can wait: refused, which ends the process.
    Refused,
    /// It cannot wait, for the reason given.
    Allowed(&'static str),
    /// A submission where the kernel has the ring's opcode for it; refused where the file system's
    /// own call stands in for the opcode.
    SubmittedOrRefused,
}

use Rule::*;

const SUBMISSION: &str = "a submission: queued on the ring and entered, never waited for";
const RELEASE: &str = "a lock's release, which waits for no other holder; a file's drop makes it \
                       wherever the drop lands, a callback's included";
const NO_CALL: &str = "answers from what the IO holds, making no system call";
const RANDOM: &str = "the thread's own generator, seeded once from the kernel's entropy, which \
                      waits only before the system's first seeding at boot";

/// Every method of `File`, ruled.
pub(crate) const FILE: &[(&str, Rule)] = &[
    ("lock_file", Refused),
    ("unlock_file", Allowed(RELEASE)),
    ("pread", Allowed(SUBMISSION)),
    ("pwrite", Allowed(SUBMISSION)),
    ("sync", Allowed(SUBMISSION)),
    ("sync_data", Allowed(SUBMISSION)),
    ("supports_durable_writes", Allowed(NO_CALL)),
    ("pwrite_durable", Allowed(SUBMISSION)),
    ("allocate", Refused),
    ("pwritev", Allowed(SUBMISSION)),
    ("size", Refused),
    ("truncate", SubmittedOrRefused),
    (
        "has_hole",
        Allowed("the ring answers it unsupported, making no system call"),
    ),
    ("punch_hole", Refused),
    ("shared_wal_lock_byte", Refused),
    ("shared_wal_try_lock_byte", Refused),
    ("shared_wal_probe_exclusive_byte", Refused),
    ("shared_wal_probe_exclusive_while_shared_byte", Refused),
    ("shared_wal_unlock_byte", Allowed(RELEASE)),
    ("shared_wal_set_len", Refused),
    ("shared_wal_map", Refused),
];

/// Every method of `IO`, ruled.
pub(crate) const IO: &[(&str, Rule)] = &[
    ("open_file", Refused),
    ("open_shared_wal_file", Refused),
    ("remove_file", Refused),
    ("sync_directory", Refused),
    ("rename_file", Refused),
    ("supports_shared_wal_coordination", Allowed(NO_CALL)),
    ("delivery", Allowed(NO_CALL)),
    (
        "step",
        Allowed("nothing to do on the ring: its delivery thread delivers"),
    ),
    (
        "cancel",
        Allowed(
            "aborts, running the callbacks with the ring's mutex let go, then queues the \
             kernel's cancels, never waited for",
        ),
    ),
    ("wait", Refused),
    ("wait_retired", Refused),
    ("drain_completions", Refused),
    ("wait_for_completion", Refused),
    ("generate_random_number", Allowed(RANDOM)),
    ("fill_bytes", Allowed(RANDOM)),
    (
        "get_memory_io",
        Allowed("a memory backend, holding no file"),
    ),
    (
        "register_fixed_buffer",
        Allowed("an update of the kernel's table of buffers, which waits for no request"),
    ),
    ("yield_now", Allowed("a yield returns")),
    ("sleep", Refused),
    ("file_id", Refused),
];

/// The names of the methods refused outright.
pub(crate) fn refused() -> impl Iterator<Item = &'static str> {
    FILE.iter()
        .chain(IO)
        .filter(|(_, rule)| *rule == Refused)
        .map(|(name, _)| *name)
}

/// The methods a trait of `core/io/mod.rs` declares, read from its source.
fn methods_of(code: &str, header: &str) -> Vec<String> {
    let start = code
        .find(header)
        .unwrap_or_else(|| panic!("no {header:?} in core/io/mod.rs"));
    let open = start + code[start..].find('{').unwrap();
    let mut depth = 0usize;
    let mut end = open;
    for (i, c) in code[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = open + i;
                    break;
                }
            }
            _ => {}
        }
    }
    // Only the trait's own level: a default body's braces are deeper.
    let body = &code[open + 1..end];
    let mut methods = Vec::new();
    let mut depth = 0usize;
    for (i, c) in body.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            'f' if depth == 0
                && body[i..].starts_with("fn ")
                && (i == 0 || !body.as_bytes()[i - 1].is_ascii_alphanumeric()) =>
            {
                methods.push(
                    body[i + 3..]
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect(),
                );
            }
            _ => {}
        }
    }
    methods
}

#[test]
fn every_method_of_file_and_io_is_ruled_for_the_delivery_thread() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/io/mod.rs")).unwrap();
    let code = code_of(&source);
    for (header, ruled) in [("pub trait File", FILE), ("pub trait IO", IO)] {
        let mut declared = methods_of(&code, header);
        let mut listed: Vec<String> = ruled.iter().map(|(name, _)| name.to_string()).collect();
        // The scan reads what it must: `File`'s first method and `IO`'s wait are among them.
        assert!(
            declared
                .iter()
                .any(|m| m == "lock_file" || m == "wait_for_completion"),
            "{header}: the scan read no method where some are: {declared:?}"
        );
        declared.sort();
        listed.sort();
        assert_eq!(
            declared, listed,
            "{header}: a method not ruled for the delivery thread, or one ruled that is gone"
        );
    }
}
