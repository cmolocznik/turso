//! What each method of `File` and `IO` does on a ring's delivery thread and on a database's IO
//! thread (DESIGN.md 22.5; ASYNC1 D2a). The ring's delivery thread never makes a call that can
//! wait: nothing else would deliver what it waits for, and every delivery behind it would wait too.
//! So every method that can wait (on the file system's data, its metadata or its names, on another
//! holder of a lock, on time, or on a request) is refused there, and every other is allowed for the
//! reason given. A database's IO thread waits on the file system, which is its work, and on nothing
//! else: a request may be one it delivers, a lock's other holder may be waiting on one of its
//! requests, and every request queued behind it waits out the time with it. Every method of the two
//! traits is ruled for both: one added to either fails the census below until it is. The ring's own
//! tests call each on a thread marked for each role and hold the ring to these rulings
//! (`io_uring::tests::each_method_on_the_delivery_thread_is_refused_or_allowed_as_ruled` and
//! `each_method_on_a_databases_io_thread_is_refused_or_allowed_as_ruled`), and the callback census
//! refuses a callback that calls a method the delivery thread refuses.

use crate::source_test::{code_of, methods_of};

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
    ("try_pread_now", Allowed(SUBMISSION)),
    ("try_pwrite_now", Allowed(SUBMISSION)),
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
        "try_step",
        Allowed("the trait's default: a step, which does nothing on the ring, answering Unsaid"),
    ),
    ("says_what_a_step_found", Allowed(NO_CALL)),
    (
        "withdraw",
        Allowed("the trait's default: the ring registers no waiter, so takes none back"),
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

const FILE_SYSTEM: &str = "a wait on the file system alone, which is the IO thread's work";
const IO_SUBMISSION: &str = "a submission, or the file system's own call where the backend makes \
                             one, which is the IO thread's work";
const NON_BLOCKING_LOCK: &str = "a lock that never waits: it is taken, or refused, at once";

/// Every method of `File`, ruled for a database's IO thread.
pub(crate) const IO_THREAD_FILE: &[(&str, Rule)] = &[
    ("lock_file", Allowed(NON_BLOCKING_LOCK)),
    ("unlock_file", Allowed(RELEASE)),
    ("pread", Allowed(IO_SUBMISSION)),
    ("pwrite", Allowed(IO_SUBMISSION)),
    ("try_pread_now", Allowed(IO_SUBMISSION)),
    ("try_pwrite_now", Allowed(IO_SUBMISSION)),
    ("sync", Allowed(IO_SUBMISSION)),
    ("sync_data", Allowed(IO_SUBMISSION)),
    ("supports_durable_writes", Allowed(NO_CALL)),
    ("pwrite_durable", Allowed(IO_SUBMISSION)),
    ("allocate", Allowed(FILE_SYSTEM)),
    ("pwritev", Allowed(IO_SUBMISSION)),
    ("size", Allowed(FILE_SYSTEM)),
    ("truncate", Allowed(IO_SUBMISSION)),
    ("has_hole", Allowed(FILE_SYSTEM)),
    ("punch_hole", Allowed(FILE_SYSTEM)),
    // Waits for the byte's other holder.
    ("shared_wal_lock_byte", Refused),
    ("shared_wal_try_lock_byte", Allowed(NON_BLOCKING_LOCK)),
    (
        "shared_wal_probe_exclusive_byte",
        Allowed("a lock that never waits, let go at once"),
    ),
    // Takes its shared lock back with a wait for the byte's other holder.
    ("shared_wal_probe_exclusive_while_shared_byte", Refused),
    ("shared_wal_unlock_byte", Allowed(RELEASE)),
    ("shared_wal_set_len", Allowed(FILE_SYSTEM)),
    ("shared_wal_map", Allowed(FILE_SYSTEM)),
];

/// Every method of `IO`, ruled for a database's IO thread.
pub(crate) const IO_THREAD_IO: &[(&str, Rule)] = &[
    ("open_file", Allowed(FILE_SYSTEM)),
    ("open_shared_wal_file", Allowed(FILE_SYSTEM)),
    ("remove_file", Allowed(FILE_SYSTEM)),
    ("sync_directory", Allowed(FILE_SYSTEM)),
    ("rename_file", Allowed(FILE_SYSTEM)),
    ("supports_shared_wal_coordination", Allowed(NO_CALL)),
    ("delivery", Allowed(NO_CALL)),
    (
        "step",
        Allowed("nothing to do on the ring: its delivery thread delivers"),
    ),
    (
        "try_step",
        Allowed("the trait's default: a step, which does nothing on the ring, answering Unsaid"),
    ),
    ("says_what_a_step_found", Allowed(NO_CALL)),
    (
        "withdraw",
        Allowed("the trait's default: the ring registers no waiter, so takes none back"),
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
    ("file_id", Allowed(FILE_SYSTEM)),
];

/// The names of the methods refused outright.
pub(crate) fn refused() -> impl Iterator<Item = &'static str> {
    FILE.iter()
        .chain(IO)
        .filter(|(_, rule)| *rule == Refused)
        .map(|(name, _)| *name)
}

#[test]
fn every_method_of_file_and_io_is_ruled_for_the_delivery_thread_and_a_databases_io_thread() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/io/mod.rs")).unwrap();
    let code = code_of(&source);
    for (header, role, ruled) in [
        ("pub trait File", "the delivery thread", FILE),
        ("pub trait IO", "the delivery thread", IO),
        ("pub trait File", "a database's IO thread", IO_THREAD_FILE),
        ("pub trait IO", "a database's IO thread", IO_THREAD_IO),
    ] {
        let mut declared: Vec<String> = methods_of(&code, header)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
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
            "{header}: a method not ruled for {role}, or one ruled that is gone"
        );
    }
}
