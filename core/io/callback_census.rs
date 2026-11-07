//! The census of every callback Turso and Atoll construct (DESIGN.md 22.5): on the ring a callback
//! runs on the delivery thread, which never waits and never makes a synchronous file call, since
//! nothing else would deliver what it waits for and every delivery behind it would wait on the
//! file system. Each callback is listed with what it does; one not listed, or one listed and gone,
//! fails, and so does one whose own code waits or calls the file system synchronously. What a
//! callback calls further is the delivery thread's to refuse at run time
//! (`refuse_on_delivery_thread`), and `CrashIO` records any synchronous file call a delivered
//! callback makes.

use crate::source_test::{code_of, rust_sources, without_test_modules};

/// Every callback constructed outside tests: its file below `core`, the function it is built in,
/// its constructor, and what it does. One function may build several of a kind; each is a row.
const CALLBACKS: &[(&str, &str, &str, &str)] = &[
    (
        "database.rs",
        "new_header_read_completion",
        "new_read",
        "reports a short read of the header",
    ),
    (
        "incremental/vdbe_maintenance/delta_cursor.rs",
        "load_next_batch",
        "new_read",
        "reports a short read",
    ),
    ("incremental/view.rs", "start_spill", "new_write", "nothing"),
    (
        "io/mod.rs",
        "pwritev",
        "new_write",
        "counts a part of a vectored write into its parent",
    ),
    (
        "io/mod.rs",
        "submit",
        "new_write",
        "asserts the whole batch was written",
    ),
    (
        "mvcc/database/checkpoint_state_machine.rs",
        "step_inner",
        "new_sync",
        "nothing",
    ),
    (
        "mvcc/database/checkpoint_state_machine.rs",
        "step_inner",
        "new_sync",
        "nothing",
    ),
    (
        "mvcc/database/mod.rs",
        "bootstrap_issue_metadata_io",
        "new_trunc",
        "nothing",
    ),
    (
        "mvcc/database/mod.rs",
        "maybe_complete_interrupted_checkpoint_nonblock",
        "new_sync",
        "nothing",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "frame_and_pwrite_tx",
        "new_write",
        "asserts the frame was written whole",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "free",
        "new_trunc",
        "logs an error",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "prepare",
        "new_sync",
        "nothing",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "read_exact_at",
        "new_read",
        "appends the bytes read to a buffer",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "read_more_data",
        "new_read",
        "appends the bytes read to a buffer",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "reset_to_fresh_header",
        "new",
        "nothing (a group)",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "reset_to_fresh_header",
        "new_trunc",
        "logs an error",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "sync",
        "new",
        "nothing (a group)",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "sync",
        "new_sync",
        "logs",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "sync",
        "new_sync",
        "records the log synced",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "truncate",
        "new",
        "nothing (a group)",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "truncate_to_zero",
        "new_trunc",
        "logs an error",
    ),
    (
        "mvcc/persistent_storage/logical_log.rs",
        "write_header",
        "new_write",
        "asserts the header was written whole",
    ),
    (
        "storage/atoll/buffer/temp.rs",
        "read",
        "new_read",
        "hands a temporary page's read, or its short read, to its waiter",
    ),
    (
        "storage/atoll/buffer/temp.rs",
        "write",
        "new_write",
        "records a temporary page written and hands the result on",
    ),
    (
        "storage/atoll/control.rs",
        "judge",
        "new_read",
        "records how many superblock bytes were read",
    ),
    (
        "storage/atoll/control.rs",
        "write_encoded",
        "new_sync",
        "finishes the superblock's write",
    ),
    (
        "storage/atoll/control.rs",
        "write_encoded",
        "new_write",
        "finishes the superblock's write, failed",
    ),
    (
        "storage/atoll/control.rs",
        "write_encoded",
        "new_write",
        "submits the superblock's sync",
    ),
    (
        "storage/atoll/log/mod.rs",
        "open_segments",
        "new_sync",
        "nothing",
    ),
    (
        "storage/atoll/log/mod.rs",
        "submit_sync",
        "new",
        "raises the durable position (a group)",
    ),
    (
        "storage/atoll/log/mod.rs",
        "submit_sync",
        "new_sync",
        "nothing (a group's member)",
    ),
    (
        "storage/atoll/log/mod.rs",
        "submit_write",
        "new_write",
        "records the write landed, wakes, starts the next sync and flush",
    ),
    (
        "storage/atoll/log/mod.rs",
        "zero_fill",
        "new_sync",
        "moves the preparation's word to owed and wakes every waiter",
    ),
    (
        "storage/atoll/log/mod.rs",
        "zero_fill",
        "new_write",
        "submits the next zero-fill write",
    ),
    (
        "storage/atoll/log/reader.rs",
        "payload",
        "new_read",
        "records how many bytes were read",
    ),
    (
        "storage/atoll/log/reader.rs",
        "read_more",
        "new_read",
        "records how many bytes were read",
    ),
    (
        "storage/atoll/meta.rs",
        "load",
        "new_read",
        "chooses a metadata page's copy, fills its frame, finishes its load",
    ),
    (
        "storage/atoll/meta.rs",
        "write_for",
        "new_sync",
        "finishes a metadata page's write",
    ),
    (
        "storage/atoll/meta.rs",
        "write_for",
        "new_write",
        "submits a metadata page's sync, or finishes its write",
    ),
    (
        "storage/atoll/mod.rs",
        "whole_write",
        "new_write",
        "finishes the caller's completion with a whole write's result",
    ),
    (
        "storage/atoll/store.rs",
        "erase_freed",
        "new_sync",
        "nothing",
    ),
    (
        "storage/atoll/store.rs",
        "read_slot_into",
        "new_read",
        "verifies a page read and hands it to its waiter",
    ),
    (
        "storage/atoll/store.rs",
        "sync_data_files",
        "new_sync",
        "nothing",
    ),
    (
        "storage/atoll/store.rs",
        "write_slot",
        "new_write",
        "hands a page write's result to its waiter",
    ),
    (
        "storage/atoll/stream.rs",
        "read_kept",
        "new_read",
        "reports the read's error",
    ),
    (
        "storage/atoll/stream.rs",
        "write_kept",
        "new_sync",
        "nothing",
    ),
    (
        "storage/btree.rs",
        "balance_non_root",
        "new",
        "nothing (a group)",
    ),
    (
        "storage/database.rs",
        "read_page",
        "new_read",
        "decodes a page into the caller's buffer and finishes the caller's read",
    ),
    (
        "storage/database.rs",
        "read_page",
        "new_read",
        "verifies a page's checksum and finishes the caller's read",
    ),
    (
        "storage/pager.rs",
        "checkpoint_inner",
        "new_read",
        "records how many header bytes were read",
    ),
    ("storage/pager.rs", "checkpoint_inner", "new_trunc", "logs"),
    (
        "storage/pager.rs",
        "commit_wal_inner",
        "new",
        "nothing (a group)",
    ),
    ("storage/pager.rs", "initialize", "new", "nothing (a group)"),
    (
        "storage/pager.rs",
        "subjournal_page_if_required",
        "new_write",
        "records the page in the savepoint and advances its offset",
    ),
    (
        "storage/pager.rs",
        "try_spill_dirty_pages",
        "new",
        "nothing (a group)",
    ),
    (
        "storage/shared_wal_coordination.rs",
        "begin_sync",
        "new_sync",
        "nothing",
    ),
    (
        "storage/sqlite3_ondisk.rs",
        "begin_read_page",
        "new_read",
        "finishes a page's read into the page, or unlocks it on a short read",
    ),
    (
        "storage/sqlite3_ondisk.rs",
        "begin_read_wal_frame",
        "new_read",
        "decodes a frame and finishes a page's read",
    ),
    (
        "storage/sqlite3_ondisk.rs",
        "begin_read_wal_frame_raw",
        "new_read",
        "decodes a frame and hands it on",
    ),
    (
        "storage/sqlite3_ondisk.rs",
        "begin_sync",
        "new_sync",
        "records the sync ended",
    ),
    (
        "storage/sqlite3_ondisk.rs",
        "begin_write_btree_page",
        "new_write",
        "marks the page clean",
    ),
    (
        "storage/sqlite3_ondisk.rs",
        "begin_write_wal_header",
        "new_write",
        "asserts the header was written whole",
    ),
    (
        "storage/sqlite3_ondisk.rs",
        "read_header",
        "new_read",
        "parses the WAL's header into the reader's state",
    ),
    (
        "storage/sqlite3_ondisk.rs",
        "submit_one_chunk",
        "new_read",
        "scans a chunk of WAL frames into the reader's state",
    ),
    (
        "storage/sqlite3_ondisk.rs",
        "write_pages_vectored",
        "new_write",
        "records a run written, or its error, and the batch's end",
    ),
    (
        "storage/subjournal.rs",
        "read_page",
        "new_read",
        "finishes a page's read from the subjournal",
    ),
    (
        "storage/subjournal.rs",
        "read_page_number",
        "new_read",
        "reports a short read",
    ),
    ("storage/subjournal.rs", "truncate", "new_trunc", "nothing"),
    (
        "storage/wal.rs",
        "append_frames_vectored",
        "new_write",
        "tags the appended pages with their frames",
    ),
    (
        "storage/wal.rs",
        "checkpoint_inner",
        "new",
        "nothing (a group)",
    ),
    (
        "storage/wal.rs",
        "prepare_wal_finish",
        "new_sync",
        "marks the WAL initialized",
    ),
    (
        "storage/wal.rs",
        "prepare_wal_start",
        "new",
        "nothing (a group)",
    ),
    (
        "storage/wal.rs",
        "prepare_wal_start",
        "new_trunc",
        "logs an error",
    ),
    (
        "storage/wal.rs",
        "read_database_identity_from_storage",
        "new_read",
        "records how many bytes were read",
    ),
    (
        "storage/wal.rs",
        "read_exact_bytes_from_file",
        "new_read",
        "records how many bytes were read",
    ),
    (
        "storage/wal.rs",
        "read_frames_batch",
        "new_read",
        "finishes a page's read from a frame and tags it",
    ),
    (
        "storage/wal.rs",
        "sync",
        "new_sync",
        "records the WAL synced",
    ),
    ("storage/wal.rs", "truncate_log", "new_sync", "logs"),
    ("storage/wal.rs", "truncate_log", "new_trunc", "logs"),
    ("storage/wal.rs", "write_frame_raw", "new_write", "nothing"),
    (
        "vdbe/hash_table.rs",
        "grace_load_next_probe_chunk",
        "new_read",
        "copies a spilled chunk in and records its state",
    ),
    (
        "vdbe/hash_table.rs",
        "load_spilled_partition",
        "new_read",
        "copies a spilled partition in and records its state",
    ),
    (
        "vdbe/hash_table.rs",
        "spill_multiple_partitions",
        "new_write",
        "records the partitions' spill state",
    ),
    (
        "vdbe/hash_table.rs",
        "spill_partition",
        "new_write",
        "records the partition's spill state",
    ),
    (
        "vdbe/hash_table.rs",
        "spill_probe_partition",
        "new_write",
        "records the partition's spill state",
    ),
    (
        "vdbe/hash_table.rs",
        "spilled_key_equals",
        "new_read",
        "copies a spilled key in and records its state",
    ),
    (
        "vdbe/sorter.rs",
        "init_chunk_heap",
        "new",
        "nothing (a group)",
    ),
    (
        "vdbe/sorter.rs",
        "read",
        "new_read",
        "copies a sorted chunk's bytes in and records its state",
    ),
    (
        "vdbe/sorter.rs",
        "write",
        "new_write",
        "records a sorted chunk's write state",
    ),
    (
        "vdbe/vacuum.rs",
        "start_temp_batch_reads",
        "new",
        "nothing (a group)",
    ),
];

/// The constructors of a callback, by the name a row gives them.
const CONSTRUCTORS: &[(&str, &str)] = &[
    ("Completion::new_read(", "new_read"),
    ("Completion::new_write(", "new_write"),
    ("Completion::new_sync(", "new_sync"),
    ("Completion::new_trunc(", "new_trunc"),
    ("CompletionGroup::new(", "new"),
];

/// What a callback's own code may not call: a wait, or a synchronous file call.
const REFUSED: &[&str] = &[
    "wait_for_completion(",
    "drain_completions(",
    "wait_retired(",
    "block_on(",
    ".step()",
    "sync_directory(",
    "open_file(",
    "remove_file(",
    "rename_file(",
    ".allocate(",
    "set_len(",
    "sync_all(",
];

/// A file a census reads no callback from: tests, and the census's own helpers.
fn is_test_file(relative: &str) -> bool {
    relative.ends_with("tests.rs")
        || relative.contains("/tests/")
        || relative.starts_with("tests/")
        || [
            "storage/atoll/testing.rs",
            "storage/atoll/bench.rs",
            "child_test.rs",
            "source_test.rs",
            "io/callback_census.rs",
        ]
        .contains(&relative)
}

/// The text from `from` to the parenthesis that closes the one just before it.
fn to_close(code: &str, from: usize) -> &str {
    let mut depth = 1usize;
    for (i, c) in code[from..].char_indices() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => {
                depth -= 1;
                if depth == 0 {
                    return &code[from..from + i];
                }
            }
            _ => {}
        }
    }
    &code[from..]
}

/// A callback's own code: the constructor's arguments, and, where the callback is a binding the
/// arguments name, that binding's initializer in the same function.
fn callback_code<'a>(function: &'a str, arguments: &'a str) -> String {
    let named = arguments
        .rsplit(',')
        .next()
        .map(str::trim)
        .filter(|n| !n.is_empty() && n.chars().all(|c| c.is_alphanumeric() || c == '_'));
    let mut code = arguments.to_string();
    if let Some(name) = named {
        for binding in [
            format!("let {name} "),
            format!("let {name}:"),
            format!("let mut {name} "),
        ] {
            if let Some(at) = function.find(&binding) {
                let rest = &function[at..];
                let mut depth = 0i32;
                let end = rest
                    .char_indices()
                    .find(|&(_, c)| {
                        match c {
                            '(' | '[' | '{' => depth += 1,
                            ')' | ']' | '}' => depth -= 1,
                            _ => {}
                        }
                        c == ';' && depth == 0
                    })
                    .map_or(rest.len(), |(i, _)| i);
                code.push('\n');
                code.push_str(&rest[..end]);
                break;
            }
        }
    }
    code
}

#[test]
fn every_callback_is_listed_and_none_waits_or_calls_the_file_system() {
    let core = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut found = Vec::new();
    let mut refused = Vec::new();
    for path in rust_sources(core) {
        let relative = path.strip_prefix(core).unwrap().display().to_string();
        if is_test_file(&relative) {
            continue;
        }
        let code = without_test_modules(&code_of(&std::fs::read_to_string(&path).unwrap()));
        for (constructor, kind) in CONSTRUCTORS {
            for (at, _) in code.match_indices(constructor) {
                let before = &code[..at];
                let fn_at = before.rfind("fn ").unwrap_or(0);
                let function: String = code[fn_at + 3..]
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                let arguments = to_close(&code, at + constructor.len());
                let own = callback_code(&code[fn_at..], arguments);
                for call in REFUSED {
                    if own.contains(call) {
                        refused.push(format!("{relative}: {function}'s callback calls {call}"));
                    }
                }
                found.push((relative.clone(), function, kind.to_string()));
            }
        }
    }
    let mut listed: Vec<(String, String, String)> = CALLBACKS
        .iter()
        .map(|(f, func, k, _)| (f.to_string(), func.to_string(), k.to_string()))
        .collect();
    found.sort();
    listed.sort();
    // The census sees the callbacks it must: the log's data-sync callback is one.
    assert!(
        found
            .iter()
            .any(|(f, func, k)| f == "storage/atoll/log/mod.rs"
                && func == "zero_fill"
                && k == "new_sync"),
        "the census read no callback where one is: {} found",
        found.len()
    );
    assert_eq!(
        found, listed,
        "a callback not listed with what it does, or one listed that is gone"
    );
    assert!(refused.is_empty(), "{refused:#?}");
}
