//! A panic in Atoll ends the process before anything it left half made can be read (DESIGN.md
//! 22.11, section 7.5). Builds Turso ships abort on a panic already; for a build that unwinds,
//! every destructor that lets Atoll's shared state go, publishes it, cleans it up or wakes anyone
//! for it calls [`abort_if_unwinding`] first, before it does any of that.
//!
//! Every entry into Atoll is a frame (`#[turso_macros::atoll_entry]`): entered on a thread that
//! unwinds, it ends the process before it takes anything, and a panic that unwinds out of it,
//! from its body or from the drop of anything it owns, ends the process at its edge. So no
//! unwind that leaves Atoll returns to its caller with what it left half made, and any unwind a
//! guard sees began inside the frame that took the guard.

/// Ends the process if this thread is unwinding: the panic, its message already printed, passes
/// through a guard of shared state, which no other thread may then see let go. It says where (the
/// caller, a guard's drop) and on which thread first.
#[inline]
#[track_caller]
pub(crate) fn abort_if_unwinding() {
    if crate::thread::panicking() {
        abort(std::panic::Location::caller());
    }
}

/// A frame, opened by every entry into Atoll: entered on a thread that unwinds, it ends the
/// process at once; dropped while one unwinds out of the entry, it ends it then.
pub(crate) struct Frame(());

impl Frame {
    #[inline]
    pub(crate) fn enter() -> Self {
        abort_if_unwinding();
        crate::storage::atoll::notify::refuse_while_delivering(
            "an Atoll entry from a waker Atoll delivers, which schedules and returns",
        );
        Self(())
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        abort_if_unwinding();
    }
}

/// The last field of a facade type that owns Atoll state: its `Drop`, an entry, opens the frame
/// before the fields drop, in the order they are declared, and this, dropped after them all,
/// ends the process if one of their drops panicked.
#[derive(Default)]
pub(crate) struct FrameEnd(());

impl Drop for FrameEnd {
    fn drop(&mut self) {
        abort_if_unwinding();
    }
}

/// Ends the process at once, writing nothing first: a write takes stderr's lock, which another
/// thread may hold while it waits for this one, and even a raw `write(2)` blocks on a full pipe, so
/// the process might never end. In tests alone, whose harness captures the panic's message and
/// loses it to the abort, one `write(2)` of where and on which thread, from a buffer on the stack,
/// goes first (no lock, no allocation; a line too long is cut short).
#[cold]
#[inline(never)]
fn abort(at: &std::panic::Location<'_>) -> ! {
    #[cfg(not(all(test, unix)))]
    let _ = at;
    #[cfg(all(test, unix))]
    {
        use std::fmt::Write;
        struct Line {
            buf: [u8; 512],
            len: usize,
        }
        impl Write for Line {
            fn write_str(&mut self, s: &str) -> std::fmt::Result {
                let n = s.len().min(self.buf.len() - self.len);
                self.buf[self.len..self.len + n].copy_from_slice(&s.as_bytes()[..n]);
                self.len += n;
                Ok(())
            }
        }
        let mut line = Line {
            buf: [0; 512],
            len: 0,
        };
        let thread = std::thread::current();
        let _ = writeln!(
            line,
            "atoll: a panic unwound through {}:{} on thread {}; the process ends",
            at.file(),
            at.line(),
            thread.name().unwrap_or("<unnamed>")
        );
        // SAFETY: a write of `len` initialized bytes from a live buffer to the standard error.
        let _ = unsafe { libc::write(2, line.buf.as_ptr().cast(), line.len) };
    }
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
        (
            "buffer/shortage.rs",
            "Installed",
            "restores the thread's owner pointer, guarding no shared state",
        ),
        (
            "buffer/upkeep.rs",
            "Installed",
            "restores the checkpoint's demand installed before; it lets no shared state go",
        ),
        (
            "buffer/shortage.rs",
            "Restore",
            "restores a test's scarce-memory switch, guarding no shared state",
        ),
        ("arc_cell.rs", "Counted", "a test's count of live values"),
        (
            "buffer/cell.rs",
            "CompletionCell",
            "drops its last reference",
        ),
        (
            "buffer/cell/transfer_tests.rs",
            "DropCount",
            "a test's count of live completions",
        ),
        (
            "buffer/cell/transfer_tests.rs",
            "OnDrop",
            "a test's owned destructor callback, which changes no database state",
        ),
        (
            "buffer/frame_tests.rs",
            "ProbeFile",
            "a test's count of its probe's live files",
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
            "WaitList",
            "frees the waits a wake took out of the lists",
        ),
        (
            "epoch.rs",
            "Step",
            "leaves an epoch: memory kept alive, no change guarded",
        ),
        (
            "epoch.rs",
            "CompletionSlot",
            "keeps a completion alive, guards no change, clears before dropping its reference",
        ),
        ("epoch.rs", "Retired", "frees what it retired"),
        (
            "latch.rs",
            "HandsBack",
            "a test's holder: hands its hold back through the release, which checks",
        ),
        (
            "buffer/shortage.rs",
            "ChecksDrop",
            "a test's waker recording whether its drop ran under the queue's mutex",
        ),
        (
            "buffer/shortage.rs",
            "Looks",
            "a test's waker recording its wakes and whether its drop ran under the queue's mutex",
        ),
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
        ("stack.rs", "Chain", "frees its own nodes"),
        ("stack.rs", "Gathered", "frees its own nodes"),
        (
            "unit.rs",
            "Unit",
            "ends a unit's mark and its count of page visits, which guard no state",
        ),
        ("stack.rs", "Popped", "frees its own node"),
        ("stack.rs", "FreeList", "frees its own nodes"),
        (
            "unwind.rs",
            "PanicsOnDrop",
            "a test's value whose drop panics",
        ),
        (
            "unwind.rs",
            "EntersOnDrop",
            "a test's caller outside Atoll entering it from a drop",
        ),
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
                // A drop written as an entry opens a frame first, which checks.
                let framed = after[..body].contains("#[turso_macros::atoll_entry]");
                let first = if framed {
                    "abort_if_unwinding()".to_string()
                } else {
                    after[open..]
                        .split(';')
                        .next()
                        .unwrap()
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                };
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
            drops.len() >= 46,
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
        assert!(checked >= 27, "{checked} destructors check");
    }

    /// Each function with a body in `code`: its name and its body. Nested functions are found
    /// too, each on its own.
    fn bodies(code: &str) -> Vec<(String, &str)> {
        let mut out = Vec::new();
        let mut at = 0;
        while let Some(found) = code[at..].find("fn ") {
            let start = at + found;
            at = start + "fn ".len();
            if code[..start]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_alphanumeric() || c == '_')
            {
                continue;
            }
            let name: String = code[at..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                continue;
            }
            // The body opens at the first brace outside the signature's parentheses and
            // brackets; a semicolon there first is a declaration with none.
            let rest = &code[at..];
            let (mut depth, mut open) = (0i32, None);
            for (i, c) in rest.char_indices() {
                match c {
                    '(' | '[' => depth += 1,
                    ')' | ']' => depth -= 1,
                    '{' if depth == 0 => {
                        open = Some(i);
                        break;
                    }
                    ';' if depth == 0 => break,
                    _ => {}
                }
            }
            let Some(open) = open else { continue };
            let mut depth = 0;
            let close = rest[open..]
                .char_indices()
                .find(|&(_, c)| {
                    match c {
                        '{' => depth += 1,
                        '}' => depth -= 1,
                        _ => {}
                    }
                    depth == 0
                })
                .map(|(i, _)| open + i)
                .expect("a body closes");
            out.push((name, &rest[open + 1..close]));
        }
        out
    }

    /// A checked hold lets go of what it guards through a release primitive: each `fn unlock*`
    /// the latch defines, and the drop of a lock's inner guard (`locks.rs`). Every function that
    /// calls one ends the process first on a thread that unwinds, whether it is the hold's drop
    /// or another release path, such as a hold handed back unchanged.
    #[test]
    fn every_release_path_of_a_checked_hold_aborts_first_when_unwinding() {
        let dir = atoll_dir();
        let latch = code_of(&std::fs::read_to_string(dir.join("latch.rs")).unwrap());
        let mut primitives: Vec<String> = bodies(&latch)
            .into_iter()
            .filter(|(name, _)| name.starts_with("unlock"))
            .map(|(name, _)| format!(".{name}("))
            .collect();
        primitives.sort();
        primitives.dedup();
        assert!(
            primitives.len() >= 2,
            "the latch's releases: {primitives:?}"
        );
        primitives.push("ManuallyDrop::drop(&mut self.0)".to_string());
        let (mut sites, mut wrong) = (Vec::new(), Vec::new());
        for path in atoll_sources() {
            let file = path.strip_prefix(&dir).unwrap().display().to_string();
            let code = code_of(&std::fs::read_to_string(&path).unwrap());
            for (name, body) in bodies(&code) {
                if !primitives.iter().any(|p| body.contains(p.as_str())) {
                    continue;
                }
                let first = body
                    .split(';')
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ");
                if !first.ends_with("abort_if_unwinding()") {
                    wrong.push(format!("{file}: {name} releases but begins `{first}`"));
                }
                sites.push(format!("{file}: {name}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
        // The latch's releases and the locks' guard drop, at the least.
        assert!(
            ["latch.rs: ", "locks.rs: "]
                .iter()
                .all(|file| sites.iter().any(|s| s.starts_with(file))),
            "the census found {sites:?}"
        );
    }

    /// Tests in Atoll that panic, and how each runs: in a child process when the panic passes a
    /// guard of shared state, which ends the process, else as an ordinary test.
    const PANICKING: &[(&str, &str, &str)] = &[
        (
            "log/unit_tests.rs",
            "over_the_ring_an_admission_in_a_frame_that_must_wait_rings_the_log_and_is_granted",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "over_the_ring_an_append_a_group_and_a_publish_in_a_frame_ring_and_submit_nothing",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "a_rollbacks_read_back_in_a_frame_is_queued_in_its_slots_record_and_read_on_the_io_thread",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "over_the_ring_a_rollbacks_read_back_in_a_frame_is_queued_in_its_slots_record",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "an_admission_in_a_frame_that_must_wait_rings_the_log_and_is_granted_once_its_flush_lands",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "a_durability_wait_in_a_frame_rings_the_log_and_is_done_once_its_sync_lands",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "an_append_a_group_and_a_publish_in_a_frame_with_a_waiter_ring_and_submit_nothing",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "an_undriven_preparer_found_in_a_frame_is_an_error_and_never_a_step",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "a_refused_sync_member_fails_the_log_and_leaves_nothing_counted",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "the_requests_dropped_with_only_the_doors_work_left_leave_nothing",
            "child",
        ),
        (
            "log/unit_tests.rs",
            "a_unit_that_is_its_logs_last_owner_drops_it_on_the_io_thread_before_its_count_goes",
            "child",
        ),
        (
            "buffer/frame_tests.rs",
            "a_frames_miss_on_a_cold_data_file_is_opened_synced_and_read_on_an_io_thread",
            "child",
        ),
        (
            "buffer/frame_tests.rs",
            "a_synchronous_databases_frame_miss_on_a_data_page_ends_the_process_at_its_open",
            "child",
        ),
        (
            "buffer/frame_tests.rs",
            "a_write_out_in_a_frame_is_queued_and_its_frame_freed_at_the_writes_retirement",
            "child",
        ),
        (
            "buffer/frame_tests.rs",
            "a_temporary_pages_first_write_out_and_read_back_in_frames_run_on_an_io_thread",
            "child",
        ),
        (
            "buffer/frame_tests.rs",
            "a_frames_cold_load_and_write_out_over_the_ring_run_on_an_io_thread",
            "child",
        ),
        (
            "buffer/frame_tests.rs",
            "over_the_ring_a_cold_frame_load_is_not_offered",
            "child",
        ),
        (
            "meta/queued_tests.rs",
            "a_frames_metadata_load_on_a_cold_file_is_opened_synced_and_read_on_an_io_thread",
            "child",
        ),
        (
            "meta/queued_tests.rs",
            "a_frames_cold_load_over_the_ring_is_opened_and_read_on_an_io_thread",
            "child",
        ),
        (
            "meta/queued_tests.rs",
            "a_synchronous_databases_miss_in_a_frame_runs_in_place_and_the_census_ends_the_process",
            "child",
        ),
        (
            "meta/queued_tests.rs",
            "a_backends_refusal_after_the_owners_cancel_touches_only_the_carrier",
            "child",
        ),
        (
            "requests/followon_tests.rs",
            "a_chains_counts_are_each_let_go_once",
            "child",
        ),
        (
            "requests/followon_tests.rs",
            "a_panic_in_a_callback_the_io_thread_runs_ends_the_process",
            "child",
        ),
        (
            "requests/followon_tests.rs",
            "a_close_on_the_io_thread_is_refused",
            "child",
        ),
        (
            "epoch/tests.rs",
            "a_bounded_reclaim_takes_and_hands_back_a_run_and_leaves_the_rest",
            "child",
        ),
        (
            "sql/mode/tests.rs",
            "a_checkpoint_unit_visits_at_most_one_page",
            "child",
        ),
        ("unit.rs", "a_wait_inside_a_unit_is_refused", "ordinary"),
        (
            "buffer/supply.rs",
            "a_ticket_is_refused_while_a_scope_of_its_pool_is_open_on_the_thread",
            "ordinary",
        ),
        (
            "buffer/supply.rs",
            "a_deferred_finish_of_a_request_is_refused",
            "ordinary",
        ),
        (
            "buffer/supply.rs",
            "a_deferred_finish_of_a_plain_wait_is_refused",
            "ordinary",
        ),
        (
            "buffer/supply.rs",
            "an_engines_wait_is_refused_by_a_group",
            "ordinary",
        ),
        (
            "buffer/tests.rs",
            "a_read_returned_neither_handed_over_nor_done_ends_the_process",
            "child",
        ),
        (
            "notify.rs",
            "a_waker_that_enters_atoll_ends_the_process",
            "child",
        ),
        ("notify.rs", "a_waker_that_parks_ends_the_process", "child"),
        (
            "executor/tests.rs",
            "a_step_watching_a_signal_its_job_did_not_declare_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_runner_entering_from_inside_a_step_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_wait_naming_a_job_of_another_executor_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_named_claim_on_another_executor_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_caller_entering_from_inside_a_step_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_named_claim_from_inside_a_step_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_tasks_destructor_entering_an_executor_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_wait_on_executor_work_from_the_delivery_thread_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_host_waker_panicking_in_a_poll_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_host_entering_from_the_polls_own_wake_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_host_waker_panicking_as_a_cancelled_wait_drops_it_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_host_waker_panicking_as_its_runner_drops_it_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_tasks_destructor_panicking_as_close_drops_its_job_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_tasks_destructor_panicking_as_its_last_step_ends_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_task_entering_the_executor_from_its_abandonment_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "closing_an_executor_on_the_rings_delivery_thread_ends_the_process",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_wait_with_workers_ready_in_a_frame_withdraws_nothing_from_a_pumped_backend",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_wait_with_workers_dropped_in_a_frame_withdraws_nothing_from_a_pumped_backend",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_wait_with_workers_ready_in_a_frame_withdraws_nothing_from_deferred_memory",
            "child",
        ),
        (
            "executor/tests.rs",
            "a_wait_with_workers_dropped_in_a_frame_withdraws_nothing_from_deferred_memory",
            "child",
        ),
        (
            "notify.rs",
            "a_park_inside_an_open_change_scope_ends_the_process",
            "child",
        ),
        (
            "requests/tests.rs",
            "a_step_that_fails_while_close_waits_ends_the_process",
            "child",
        ),
        (
            "unwind.rs",
            "a_panic_in_an_entry_ends_the_process_at_its_edge",
            "child",
        ),
        (
            "unwind.rs",
            "an_owned_parameter_the_entry_ignores_drops_inside_its_frame",
            "child",
        ),
        (
            "unwind.rs",
            "an_owned_parameter_the_entry_only_borrows_drops_inside_its_frame",
            "child",
        ),
        (
            "unwind.rs",
            "a_borrowed_parameter_drops_in_its_caller_outside_the_frame",
            "child",
        ),
        (
            "unwind.rs",
            "an_owned_parameter_drops_inside_its_frame_whatever_its_neighbours_are_named",
            "child",
        ),
        (
            "unwind.rs",
            "an_entry_made_while_unwinding_ends_the_process_before_it_runs",
            "child",
        ),
        (
            "unwind.rs",
            "a_facade_value_whose_field_panics_in_its_drop_ends_the_process",
            "child",
        ),
        (
            "locks.rs",
            "a_panic_while_holding_an_atoll_lock_ends_the_process_before_it_is_let_go",
            "child",
        ),
        (
            "locks.rs",
            "a_panic_formatting_an_atoll_locks_value_ends_the_process_before_it_is_let_go",
            "child",
        ),
        (
            "guarded.rs",
            "a_panic_while_holding_a_guarded_state_ends_the_process_before_a_contender_wakes",
            "child",
        ),
        (
            "unwind.rs",
            "the_abort_waits_on_no_lock_another_thread_holds",
            "child",
        ),
        (
            "latch.rs",
            "a_panic_while_holding_the_latch_ends_the_process_before_it_is_let_go",
            "child",
        ),
        (
            "latch.rs",
            "a_hold_handed_back_unchanged_while_unwinding_ends_the_process_before_it_is_let_go",
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
            "txn/tests.rs",
            "a_reservation_position_cannot_use_a_flag_bit",
            "ordinary",
        ),
        (
            "txn/tests.rs",
            "an_overflowing_reservation_position_is_refused_before_addition",
            "ordinary",
        ),
        (
            "txn/tests.rs",
            "a_waited_bit_without_a_reservation_is_refused",
            "ordinary",
        ),
        (
            "txn/tests.rs",
            "a_reservation_cannot_also_have_position_bits",
            "ordinary",
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

    /// The files of the modules code outside Atoll can reach: Atoll's root and the modules it
    /// makes public. Every other module is Atoll's alone, so the compiler refuses a path into one
    /// from outside, however it is spelled or grouped.
    const FACADE_FILES: &[&str] = &["mod.rs", "db.rs", "sql/mod.rs", "sql/mode.rs"];

    /// The modules code outside Atoll may reach.
    const FACADE_MODULES: &[&str] = &["db", "sql", "mode"];

    /// The facade's types: each method, trait method and destructor of each is an entry
    /// (DESIGN.md 22.11).
    const FACADE_TYPES: &[&str] = &[
        "AtollDb",
        "ConnTxn",
        "SqlCursor",
        "Ending",
        "CreateOp",
        "RetireOp",
        "RefusingStorage",
    ];

    /// Facade types that own Atoll state: each drops it inside a frame, its last field the end.
    const OWNING: &[&str] = &[
        "AtollDb",
        "ConnTxn",
        "SqlCursor",
        "Ending",
        "CreateOp",
        "RetireOp",
    ];

    /// Plain data the facade passes in or out, which code outside Atoll may name: it holds no
    /// Atoll state, so its methods are no entries.
    const PLAIN: &[&str] = &[
        "AtollConfig",
        "Begin",
        "BufferConfig",
        "Checkpointing",
        "GuardKey",
        "Guarded",
        "LogConfig",
        "NO_PAGE_CAP",
        "PAGE",
        "Probed",
        "SCHEMA_ID",
        "Statement",
        "StoreConfig",
        "TxnConfig",
    ];

    /// An item of a module or of an impl, at its own level.
    #[derive(Debug)]
    struct Item<'a> {
        /// Its attributes, whole.
        attrs: &'a str,
        /// Its visibility: `""` for private.
        vis: &'a str,
        /// The keyword of its kind: `fn`, `impl`, `struct`, `use`, `mod`, ...
        kind: &'a str,
        /// Its name; an impl's, the type it is for.
        name: String,
        /// The item up to its body, or whole if it has none.
        head: &'a str,
        /// Its body, inside its braces.
        body: Option<&'a str>,
        /// Whether it is an impl of a trait.
        of_trait: bool,
    }

    const BRACED: &[&str] = &[
        "fn",
        "impl",
        "struct",
        "enum",
        "trait",
        "mod",
        "union",
        "macro_rules!",
    ];

    fn item<'a>(text: &'a str, body: Option<&'a str>) -> Option<Item<'a>> {
        let start = text.len() - text.trim_start().len();
        let mut rest = &text[start..];
        while rest.starts_with('#') {
            let mut depth = 0;
            let end = rest
                .char_indices()
                .find(|&(_, c)| {
                    depth += (c == '[') as i32 - (c == ']') as i32;
                    c == ']' && depth == 0
                })
                .map(|(i, _)| i + 1)?;
            rest = rest[end..].trim_start();
        }
        let attrs = &text[start..text.len() - rest.len()];
        let (vis, after) = if rest.starts_with("pub(") {
            let close = rest.find(')')? + 1;
            (&rest[..close], rest[close..].trim_start())
        } else if let Some(after) = rest.strip_prefix("pub ") {
            ("pub", after.trim_start())
        } else {
            ("", rest)
        };
        let mut words = after.split_whitespace();
        let mut kind = words.next()?;
        while matches!(kind, "const" | "unsafe" | "async" | "extern" | "default")
            && words
                .clone()
                .next()
                .is_some_and(|w| matches!(w, "fn" | "impl" | "unsafe" | "trait"))
        {
            kind = words.next()?;
        }
        let head = &after[..after
            .find('{')
            .filter(|_| body.is_some())
            .unwrap_or(after.len())];
        let of_trait = kind == "impl" && head.contains(" for ");
        let name: String = if kind == "impl" {
            let target = match head.rsplit_once(" for ") {
                Some((_, ty)) => ty,
                None => {
                    &head[head
                        .find('>')
                        .filter(|_| head[4..].starts_with('<'))
                        .map_or(4, |i| i + 1)..]
                }
            };
            target
                .trim()
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect()
        } else {
            let mut name = words.next().unwrap_or("");
            if kind == "fn" && name == "fn" {
                name = words.next().unwrap_or("");
            }
            name.chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect()
        };
        Some(Item {
            attrs,
            vis,
            kind,
            name,
            head,
            body,
            of_trait,
        })
    }

    /// The items of `code` at its own level.
    fn items(code: &str) -> Vec<Item<'_>> {
        let mut out = Vec::new();
        let (mut depth, mut start, mut open) = (0i32, 0usize, 0usize);
        for (i, c) in code.char_indices() {
            match c {
                '{' => {
                    if depth == 0 {
                        open = i;
                    }
                    depth += 1;
                }
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        // A braced item ends at its brace; any other at its semicolon.
                        let head = item(&code[start..open], None);
                        if head.is_some_and(|h| BRACED.contains(&h.kind)) {
                            out.extend(item(&code[start..i], Some(&code[open + 1..i])));
                            start = i + 1;
                        }
                    }
                }
                ';' if depth == 0 => {
                    out.extend(item(&code[start..i], None));
                    start = i + 1;
                }
                _ => {}
            }
        }
        out
    }

    /// `body` split at its commas outside any brackets: a struct's fields, an enum's variants.
    fn split_top(body: &str) -> Vec<&str> {
        let (mut out, mut depth, mut start) = (Vec::new(), 0i32, 0);
        for (i, c) in body.char_indices() {
            match c {
                '<' | '(' | '[' | '{' => depth += 1,
                '>' | ')' | ']' | '}' => depth -= 1,
                ',' if depth == 0 => {
                    out.push(&body[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        out.push(&body[start..]);
        out.into_iter().filter(|f| !f.trim().is_empty()).collect()
    }

    /// Whether code outside Atoll can reach an item of `file` with visibility `vis`, given the
    /// path to the item is open to it.
    fn reaches_outside(file: &str, vis: &str) -> bool {
        match vis {
            "" | "pub(self)" => false,
            // Above Atoll's root is `storage`.
            "pub(super)" => file == "mod.rs",
            _ => !vis.starts_with("pub(in crate::storage::atoll"),
        }
    }

    /// The names a `use` brings in: each path's last segment, or what it is renamed.
    fn brought_in(head: &str) -> Vec<String> {
        head.trim_start_matches("use")
            .split(['{', '}', ','])
            .filter_map(|path| {
                let path = path.trim();
                let words: Vec<&str> = path.split_whitespace().collect();
                let name = match words.as_slice() {
                    [_, "as", alias] => *alias,
                    [path] => path.rsplit("::").next().unwrap(),
                    _ => return None,
                };
                (!name.is_empty()).then(|| name.to_string())
            })
            .collect()
    }

    /// The names of the types, traits and aliases Atoll's source defines.
    fn atoll_types() -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        for path in atoll_sources() {
            let code = code_of(&std::fs::read_to_string(&path).unwrap());
            let words: Vec<&str> = code.split_whitespace().collect();
            for w in words.windows(2) {
                if matches!(w[0], "struct" | "enum" | "type" | "trait" | "union") {
                    let name: String = w[1]
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect();
                    if name.starts_with(|c: char| c.is_ascii_uppercase()) {
                        out.insert(name);
                    }
                }
            }
        }
        out
    }

    /// The facade is all code outside Atoll can reach, and each of its parts is what it may be.
    /// Atoll's root makes no module public but `db` and `sql`, and `sql` none but `mode`; in
    /// those, every item visible outside Atoll (`pub`, `pub(crate)`, a path above Atoll) is a
    /// facade type, plain data, a function that is an entry, or a name of one brought in. Every
    /// such method of a facade type, every trait method it implements and its destructor is an
    /// entry; each facade type that owns Atoll state drops it in a frame its last field ends; and
    /// no signature code outside Atoll can call names a type of Atoll's but the facade's and plain
    /// data, so nothing it returns hands out Atoll's internals.
    #[test]
    fn the_facade_is_framed_and_is_all_code_outside_atoll_reaches() {
        let dir = atoll_dir();
        let defined = atoll_types();
        let (mut wrong, mut entries, mut visible) = (Vec::new(), 0, 0);
        let allowed = |name: &str| FACADE_TYPES.contains(&name) || PLAIN.contains(&name);
        for file in FACADE_FILES {
            let code = code_of(&std::fs::read_to_string(dir.join(file)).unwrap());
            let all = items(&code);
            // Names this file brings in from outside Atoll: a signature naming one names Turso's.
            let outside: Vec<String> = all
                .iter()
                .filter(|i| i.kind == "use")
                .filter(|i| {
                    let path = i.head.trim_start_matches("use").trim_start();
                    !path.starts_with("crate::storage::atoll")
                        && !path.starts_with("super")
                        && !path.starts_with("self")
                })
                .flat_map(|i| brought_in(i.head))
                .collect();
            // A signature's words that are types of Atoll's other than the facade's or plain data.
            let leaks = |signature: &str| -> Vec<String> {
                signature
                    .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .filter(|w| {
                        defined.contains(*w) && !allowed(w) && !outside.iter().any(|o| o == w)
                    })
                    .map(String::from)
                    .collect()
            };
            for i in &all {
                if i.kind == "impl" {
                    let facade = FACADE_TYPES.contains(&i.name.as_str());
                    for m in items(i.body.unwrap_or_default())
                        .iter()
                        .filter(|m| m.kind == "fn")
                    {
                        if !(i.of_trait || reaches_outside(file, m.vis)) {
                            continue;
                        }
                        let framed = m.attrs.contains("#[turso_macros::atoll_entry]");
                        entries += framed as usize;
                        if facade && !framed {
                            wrong.push(format!("{file}: {}::{} is not an entry", i.name, m.name));
                        }
                        if facade || PLAIN.contains(&i.name.as_str()) {
                            for leak in leaks(m.head) {
                                wrong.push(format!(
                                    "{file}: {}::{} hands out {leak}, not the facade",
                                    i.name, m.name
                                ));
                            }
                        }
                    }
                    continue;
                }
                if !reaches_outside(file, i.vis) {
                    continue;
                }
                visible += 1;
                match i.kind {
                    "mod" if FACADE_MODULES.contains(&i.name.as_str()) => {}
                    "fn" => {
                        let framed = i.attrs.contains("#[turso_macros::atoll_entry]");
                        entries += framed as usize;
                        if !framed {
                            wrong.push(format!("{file}: {} is not an entry", i.name));
                        }
                        for leak in leaks(i.head) {
                            wrong.push(format!(
                                "{file}: {} hands out {leak}, not the facade",
                                i.name
                            ));
                        }
                    }
                    "use" => {
                        for name in brought_in(i.head) {
                            if !allowed(&name) {
                                wrong.push(format!("{file}: brings in {name} for outside Atoll"));
                            }
                        }
                    }
                    "struct" | "enum" if allowed(&i.name) => {
                        // A field open to code outside Atoll, or any variant, hands out its type.
                        for field in split_top(i.body.unwrap_or_default()) {
                            let Some(f) = item(field, None) else { continue };
                            // A variant hands out what it carries, past its name.
                            let carried = if i.kind == "enum" {
                                f.head
                                    .trim_start_matches(|c: char| c.is_alphanumeric() || c == '_')
                            } else {
                                f.head
                            };
                            if i.kind == "enum" || reaches_outside(file, f.vis) {
                                for leak in leaks(carried) {
                                    wrong.push(format!(
                                        "{file}: {} {} hands out {leak}, not the facade",
                                        i.kind, i.name
                                    ));
                                }
                            }
                        }
                    }
                    "const" | "static" | "type" if allowed(&i.name) => {}
                    _ => wrong.push(format!(
                        "{file}: {} {} is open to code outside Atoll and is not the facade",
                        i.kind, i.name
                    )),
                }
            }
            for ty in OWNING {
                let Some(def) = all.iter().find(|i| i.kind == "struct" && i.name == *ty) else {
                    continue;
                };
                let fields = def.body.unwrap_or_default();
                let last = fields.split(',').map(str::trim).rfind(|f| !f.is_empty());
                if !last.is_some_and(|l| l.contains("_frame:") && l.contains("FrameEnd")) {
                    wrong.push(format!("{file}: {ty}'s last field is not its frame's end"));
                }
                let dropped = all.iter().any(|i| {
                    i.kind == "impl"
                        && i.name == *ty
                        && i.head.contains("Drop for")
                        && i.body
                            .is_some_and(|b| b.contains("#[turso_macros::atoll_entry]"))
                });
                if !dropped {
                    wrong.push(format!("{file}: {ty} has no framed destructor"));
                }
            }
        }
        for ty in FACADE_TYPES {
            if !FACADE_FILES.iter().any(|f| {
                let code = code_of(&std::fs::read_to_string(dir.join(f)).unwrap());
                items(&code)
                    .iter()
                    .any(|i| i.name == *ty && i.kind != "impl")
            }) {
                wrong.push(format!(
                    "{ty} is listed in the facade but defined in none of its files"
                ));
            }
        }
        assert!(entries >= 80, "{entries} entries found");
        assert!(
            visible >= 20,
            "{visible} items open to code outside Atoll found"
        );
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    /// Every framed destructor in Atoll is its type's, whose last field ends the frame, and every
    /// type whose last field ends a frame has a framed destructor: its body and every field it
    /// drops after the body drop inside the frame, so a panic in any of them ends the process
    /// (22.11). A type and its destructor are read at one level of one file.
    #[test]
    fn every_framed_destructor_ends_its_frame_with_its_types_last_field() {
        let dir = atoll_dir();
        let (mut framed, mut wrong) = (Vec::new(), Vec::new());
        for path in atoll_sources() {
            let file = path.strip_prefix(&dir).unwrap().display().to_string();
            let code = code_of(&std::fs::read_to_string(&path).unwrap());
            let mut levels = vec![items(&code)];
            while let Some(level) = levels.pop() {
                let drops: Vec<&str> = level
                    .iter()
                    .filter(|i| {
                        i.kind == "impl"
                            && i.head.contains("Drop for")
                            && i.body
                                .is_some_and(|b| b.contains("#[turso_macros::atoll_entry]"))
                    })
                    .map(|i| i.name.as_str())
                    .collect();
                let ending: Vec<&str> = level
                    .iter()
                    .filter(|i| {
                        i.kind == "struct"
                            && i.body.is_some_and(|fields| {
                                fields
                                    .split(',')
                                    .map(str::trim)
                                    .rfind(|f| !f.is_empty())
                                    .is_some_and(|l| {
                                        l.contains("_frame:") && l.contains("FrameEnd")
                                    })
                            })
                    })
                    .map(|i| i.name.as_str())
                    .collect();
                for ty in &drops {
                    framed.push(format!("{file}: {ty}"));
                    if !ending.contains(ty) {
                        wrong.push(format!(
                            "{file}: {ty} has a framed destructor, its last field no frame's end"
                        ));
                    }
                }
                for ty in &ending {
                    if !drops.contains(ty) {
                        wrong.push(format!(
                            "{file}: {ty}'s last field ends a frame its destructor never opens"
                        ));
                    }
                }
                for i in &level {
                    if let (true, Some(body)) = (i.kind == "mod", i.body) {
                        levels.push(items(body));
                    }
                }
            }
        }
        // The facade's six owning types, the executor's teardown entries, and this file's test
        // value, at the least.
        assert!(framed.len() >= 10, "the census found {framed:?}");
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    /// A value whose drop panics, for the entries' tests.
    struct PanicsOnDrop;

    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("a parameter's drop panicked");
        }
    }

    /// Entries as the facade writes them, for proof 9.
    struct Probe;

    impl Probe {
        #[turso_macros::atoll_entry]
        fn panics(&self) {
            panic!("an entry panicked");
        }

        #[turso_macros::atoll_entry]
        fn ignores(&self, _: PanicsOnDrop) {}

        /// A parameter named as the attribute names the next one in the signature.
        #[turso_macros::atoll_entry]
        fn collides(&self, __atoll_arg2: u8, owned: PanicsOnDrop) {}

        #[turso_macros::atoll_entry]
        fn borrows(&self, owned: PanicsOnDrop) {
            let _ = &owned;
        }

        #[turso_macros::atoll_entry]
        fn refers(&self, _: &PanicsOnDrop) {}

        #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
        #[turso_macros::atoll_entry]
        fn records(&self) {
            crate::storage::atoll::testing::record("seen", "entered");
        }

        #[turso_macros::atoll_entry]
        fn fails(&self) -> crate::Result<()> {
            Err(crate::LimboError::InternalError("an ordinary error".into()))
        }
    }

    /// A facade value owning Atoll state: a framed destructor, a field whose drop panics, and its
    /// frame's end last.
    struct Owning {
        _panics: PanicsOnDrop,
        _frame: super::FrameEnd,
    }

    impl Drop for Owning {
        #[turso_macros::atoll_entry]
        fn drop(&mut self) {}
    }

    /// Code outside Atoll entering it from a destructor, as an application's unwind dropping a
    /// connection with a transaction open does.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    struct EntersOnDrop;

    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    impl Drop for EntersOnDrop {
        fn drop(&mut self) {
            Probe.records();
        }
    }

    /// A panic that unwinds out of an entry ends the process at its edge (proof 9).
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn a_panic_in_an_entry_ends_the_process_at_its_edge() {
        let Some(ended) = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_panic_in_an_entry_ends_the_process_at_its_edge"
            ),
            || Probe.panics(),
        ) else {
            return;
        };
        assert!(
            ended.aborted,
            "not aborted: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended.stderr.contains("an entry panicked"),
            "{}",
            ended.stderr
        );
    }

    /// An owned parameter the entry never names drops inside its frame: its panic ends the
    /// process (proof 9; a parameter left unbound would drop after the frame).
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn an_owned_parameter_the_entry_ignores_drops_inside_its_frame() {
        let Some(ended) = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::an_owned_parameter_the_entry_ignores_drops_inside_its_frame"
            ),
            || Probe.ignores(PanicsOnDrop),
        ) else {
            return;
        };
        assert!(
            ended.aborted,
            "not aborted: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended.stderr.contains("a parameter's drop panicked"),
            "{}",
            ended.stderr
        );
    }

    /// A parameter named as the attribute names another cannot keep that one out of the frame:
    /// the names the attribute adds are its own, so the owned parameter still drops inside it.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn an_owned_parameter_drops_inside_its_frame_whatever_its_neighbours_are_named() {
        let Some(ended) = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::an_owned_parameter_drops_inside_its_frame_whatever_its_neighbours_are_named"
            ),
            || Probe.collides(0, PanicsOnDrop),
        ) else {
            return;
        };
        assert!(
            ended.aborted,
            "not aborted: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended.stderr.contains("a parameter's drop panicked"),
            "{}",
            ended.stderr
        );
    }

    /// An owned parameter the entry only borrows drops inside its frame too.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn an_owned_parameter_the_entry_only_borrows_drops_inside_its_frame() {
        let Some(ended) = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::an_owned_parameter_the_entry_only_borrows_drops_inside_its_frame"
            ),
            || Probe.borrows(PanicsOnDrop),
        ) else {
            return;
        };
        assert!(
            ended.aborted,
            "not aborted: {:?}\n{}",
            ended.code, ended.stderr
        );
    }

    /// A parameter taken by reference is the caller's: it drops in the caller, after the call,
    /// outside the frame, and its panic is the caller's ordinary one.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn a_borrowed_parameter_drops_in_its_caller_outside_the_frame() {
        let Some(ended) = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_borrowed_parameter_drops_in_its_caller_outside_the_frame"
            ),
            || {
                let p = PanicsOnDrop;
                Probe.refers(&p);
            },
        ) else {
            return;
        };
        assert!(!ended.aborted, "aborted:\n{}", ended.stderr);
        assert_eq!(ended.code, Some(101), "{}", ended.stderr);
        assert!(
            ended.stderr.contains("a parameter's drop panicked"),
            "{}",
            ended.stderr
        );
    }

    /// An entry made while the thread unwinds ends the process before its body runs: a drop of
    /// code outside Atoll during an application's unwind enters it, and the body's record is never
    /// written (proof 9).
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn an_entry_made_while_unwinding_ends_the_process_before_it_runs() {
        let Some(ended) = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::an_entry_made_while_unwinding_ends_the_process_before_it_runs"
            ),
            || {
                let _entering = EntersOnDrop;
                panic!("an application panicked");
            },
        ) else {
            return;
        };
        let seen = crate::storage::atoll::testing::recorded(&ended, "seen");
        assert!(
            ended.aborted,
            "not aborted: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended.stderr.contains("an application panicked"),
            "{}",
            ended.stderr
        );
        assert!(
            !seen.contains("entered"),
            "the entry ran while unwinding: {seen:?}"
        );
    }

    /// A facade value whose field panics as it drops ends the process at its frame's end: the
    /// fields drop inside the frame its destructor opens (proof 9).
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn a_facade_value_whose_field_panics_in_its_drop_ends_the_process() {
        let Some(ended) = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_facade_value_whose_field_panics_in_its_drop_ends_the_process"
            ),
            || {
                drop(Owning {
                    _panics: PanicsOnDrop,
                    _frame: super::FrameEnd::default(),
                })
            },
        ) else {
            return;
        };
        assert!(
            ended.aborted,
            "not aborted: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended.stderr.contains("a parameter's drop panicked"),
            "{}",
            ended.stderr
        );
    }

    /// An ordinary error returns through a frame as it would without one.
    /// The abort waits on nothing another thread can hold. A thread holding stderr waits for
    /// one that panics through a checked guard, with a panic hook that prints nothing, as an
    /// embedder's may not: the process still ends, by abort. Were the abort to write first, it
    /// would wait for stderr, its holder for it, and the process would never end.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn the_abort_waits_on_no_lock_another_thread_holds() {
        let Some(ended) = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::the_abort_waits_on_no_lock_another_thread_holds"
            ),
            || {
                std::panic::set_hook(Box::new(|_| {}));
                let lock = crate::storage::atoll::locks::Mutex::new(());
                let started = std::sync::Barrier::new(2);
                let stderr = std::io::stderr().lock();
                std::thread::scope(|s| {
                    let holder = s.spawn(|| {
                        let _held = lock.lock();
                        started.wait();
                        panic!("a holder failed");
                    });
                    started.wait();
                    let _ = holder.join();
                });
                drop(stderr);
            },
        ) else {
            return;
        };
        assert!(
            ended.aborted,
            "the child did not abort: hung {}, code {:?}\n{}",
            ended.hung, ended.code, ended.stderr
        );
    }

    #[test]
    fn an_ordinary_error_returns_through_a_frame() {
        assert!(Probe.fails().is_err());
    }
}
