# This fork

Upstream Turso (`tursodatabase/turso`) with the changes below on top. Since 2026-10-29 nothing goes
upstream: the fork keeps up with upstream and keeps its own changes.

- `main` mirrors upstream `main`. It only fast-forwards and holds no commit of ours.
- `workreef`, the fork's default branch, is upstream plus every change below
  (`git log --no-merges main..workreef`).
- `fork/sync-upstream.sh` merges `main` into `workreef` four times a day, at 05:17, 11:17,
  17:17 and 23:17 Asia/Kuala_Lumpur (`fork/turso-sync.timer`, a systemd user timer). It
  fetches upstream, fast-forwards `main`, merges it, and commits and pushes the merge only once
  the merged tree compiles with its tests, passes the core and FTS suites and clippy, passes
  `ivf_crash`'s kill rounds under WAL, MVCC and MVCC with four writers, and passes `mvcc_rolling`'s
  over a logical log that rolls. A conflict or a failed check leaves `workreef` as it was and
  alerts. Each merge's commit message names the upstream
  commits it took and the checks it passed; each run's log and verdict are in
  `/home/me/cognition-data/turso-sync`, and a `hold` file there skips runs while a timed
  measurement needs the machine.
- `fork/repro` holds the harnesses the entries below cite (`ivf_crash`, `mvcc_reopen`,
  `read_changes`, `wal_growth` and the rest), a crate of its own on the compiler WorkReef
  cognition builds with.

Each entry is one change of ours: when it landed and in which commits, what it changes, why, and
what proves it. A change lands here in the commit that makes it. When upstream takes a change or
fixes the same thing its own way, the entry moves to "Taken upstream".

## Fixes

### Lost wakeup in `Completion::set_waker`
2026-10-28 `c07ac5ffc`, 2026-10-29 `c2e1ee261`, 2026-10-30 `0829e6128` and `9046b8b59`; upstream PR
#10948 holds the same change as `a571f0ed3`, `480f7230d` and `74e3aa403`.
`core/io/completions.rs`. `set_waker` tested `finished()` and then stored the waker under the
context lock, so a completion finished on another thread between the two woke no one, and the task
that registered was never polled again: a hang that happens now and then. The waker is now stored
first and the completion tested after.
Test: `io::completions::tests::shuttle_set_waker_racing_complete_always_wakes`, run by
`make test-shuttle`. It runs every interleaving of `set_waker` against `complete` on another
thread; on upstream `main` it finds the one that loses the wakeup. Shuttle cannot see the std
`OnceLock` holding a completion's result, so under `cfg(shuttle)` `Context::set_waker` yields
before its store.
Storing first, `set_waker` now reaches `Context::wake` for a completion already finished, where it
used to wake with no lock held, and `wake` woke the waker under the context lock: a waker that
registers again on the same completion from its wake, as polling the completion inline does,
deadlocked. The waker is now taken under the lock and woken once the lock is free.
Test: `io::completions::tests::a_waker_registering_again_from_its_wake_does_not_deadlock` (a
finished completion's waker registers again from its wake three times; under the lock, it
deadlocked).
A completion held one waker, and a second task's waker replaced it. The replaced one was woken so
that its task could register again, which replaced the other: two tasks waiting on one completion,
as two statements of one connection do on the pager's in-flight spill write, woke each other until
it finished. Polled from their wakes they overflowed the stack; on upstream `main`, which wakes the
replaced waker under the context lock, they deadlock. The PR's automated review found it.
`0829e6128` woke the replaced waker once the lock was free, and `9046b8b59` replaced that: the
context keeps a waker for each task waiting on the completion, one per task by `Waker::will_wake`,
the first held inline so that a single waiter costs what it did. Registering wakes no task, and
every wake of the completion takes all its wakers and wakes them once the lock is free. A task that
stops waiting leaves its waker until that wake.
Tests: `io::completions::tests::two_tasks_waiting_on_one_completion_are_each_woken_once_when_it_finishes`
(on `0829e6128` the second registration wakes the first task; on upstream `main` it deadlocks) and
`a_completion_lets_its_wakers_go_when_it_wakes`.

### The checkpoint trigger counts only uncheckpointed log bytes
2026-10-28 `3776ed6be`.
`core/mvcc/persistent_storage/mod.rs`, `core/mvcc/database/checkpoint_state_machine.rs`. A
passive checkpoint truncates the logical log only when no commit landed after its snapshot, so
under steady writes the log keeps its size; `should_checkpoint` compared that size with the
threshold, so once tripped it stayed tripped and every later commit started another checkpoint.
The storage now keeps a checkpoint floor, the offset the last checkpoint covered (sampled with its
snapshot, raised once the durable boundary has advanced, reset by any truncate), and counts only
the bytes past it. Truncation is unchanged: it still tests the highest appended commit.
Test: `mvcc::database::tests::test_passive_checkpoint_that_keeps_the_log_counts_only_uncovered_bytes`.
Upstream `main`: "316 checkpointed bytes of a 373-byte log still counted against the threshold".
On 0.8.3, 2 writers at a 256 KB threshold: inline writer commit p99 165-194 ms → 9.4-12.8 ms. On
upstream `main` the same A/B shows no gain: 15 idle runs each of `fork/repro`'s `mvcc_inline` (2
writers, 4,000 commits, 256 KB threshold) put the writer commit p99 median at 13.5 ms without the
fix and 14.1 ms with it.

### An exact rowid seek walked every shadowed B-tree row after its target
2026-10-28 `6387b9142`, 2026-10-30 `9060c9eca`; upstream PR #10949 holds both, as `d94a7d320` and
`4813f6096`.
`core/mvcc/cursor.rs`. An eq-only `TableRowId` seek that reached a B-tree row shadowed by a newer
MVCC version advanced to the next B-tree row, and kept going while rows were shadowed. Over a
checkpointed table whose rows were all updated, point lookups went quadratic: on upstream `main`
(`e9a8b0d25`), 2,000, 4,000 and 8,000 lookups took 222, 920 and 3,571 ms, against 16, 31 and 66 ms
with the fix (`fork/repro`'s `shadow_seek`). An exact seek has at most one B-tree match, so it now
stops when that row is shadowed or the row reached is not its target; the MVCC side still supplies
the newer version.
Test: `mvcc::database::tests::test_exact_rowid_seek_over_shadowed_btree_rows`, which counts each
exact seek's B-tree steps through the cursor's yield hook: the seek for row 1 takes 155 on upstream
`main` and 2 with the fix.
The PR's automated review found a visible target checked twice, once by the rule and again by the
seek's match; `9060c9eca` folds the rule into that match, so a row is checked once. With a
shadowed target left to advance again, the seek for row 1 takes 306 steps.

### An overflow chain was copied into its payload a byte at a time
2026-10-28 `5b3da8244`.
`core/storage/btree.rs`, `process_overflow_read`. Each page of a record's overflow chain was
appended through `try_extend` over a byte iterator, which on stable is `Vec::extend` over
`Copied<slice::Iter<u8>>` and compiled to a loop writing one byte at a time, the payload regrowing
as pages came in: ivf_rq queries over 5.6 KB block rows spent about 14% of their time in it. The
payload is now reserved at its full size when the chain is first read, and each page is copied in
with `extend_from_slice` after a fallible `try_reserve`, as `copy_payload_range` does. At 256 probes
over glove-100 the blocks' reads took 3,535 µs a query instead of 4,524. No behaviour changes, so
no new test; every read of an overflowing record goes through it.

### An MVCC database killed between a checkpoint's two truncations would not open
2026-10-29 `f9a068905`.
`core/mvcc/database/mod.rs`, the recovery of an interrupted checkpoint. A Truncate checkpoint (the
default under MVCC) empties the logical log, syncs it, and then truncates the WAL, so a crash
between the two leaves an empty log beside WAL frames the checkpoint had already backfilled into
the synced database file. Opening such a database failed with "WAL has committed frames but
logical log header is missing" unless passive checkpoints were enabled, where the same state is
the steady one. On upstream `main` (`6d8f8b18a`) `fork/repro`'s `mvcc_reopen` (an MVCC table
written by a process killed at a random 0.1 to 1.5 s, PRAGMA wal_checkpoint(TRUNCATE) every few
commits) left a database that would not open within 3, 6 and 7 kills on three seeds. Upstream's
passive checkpoint (`7e8aa5c58`) had made recovery accept this state; `b80c92fb9` then refused it
again outside passive mode. Nothing in it is lost or left to replay, so recovery now backfills the
WAL and writes a fresh header whatever the mode. With the fix the three seeds pass 200 kills each.
Test: `mvcc::database::tests::test_reopen_after_crash_between_logical_log_and_wal_truncation`, which
stops a checkpoint once the log is empty and the WAL file still holds its frames, and reopens.
Found by the ivf_rq kill harness below.

### Under MVCC a statement that reads no database stranded its connection's transaction
2026-10-30 `763113a6a`.
`core/vdbe/mod.rs` (`ProgramState::can_autocommit_now`), `core/vdbe/execute.rs`,
`core/connection.rs`, `core/statement.rs`. Under MVCC a connection's statements share one implicit
transaction, which in autocommit mode the last of them to finish ends. Every root statement counted
toward that, and only the statements that had run a Transaction instruction could end it, so a
statement that reads no database held the transaction open for the others and could not end it
itself: `SELECT 1`, or an OPTIMIZE INDEX stepped on a connection of its own. When it finished last,
the transaction stayed open, the connection's later reads kept that transaction's snapshot, and a
write deferred to it was never committed. While such a statement ran, every read on its connection
shared one snapshot and every write on it waited for the statement to finish.

A statement now takes part in the transaction from its first Transaction instruction until it
finishes (`Connection::take_txn_part`, `ProgramState::txn_part`), and only the statements taking
part are counted (`Connection::n_active_txn_statements`). A COMMIT or ROLLBACK takes part while it
ends the transaction, so a COMMIT waiting mid-commit keeps another statement from ending the
transaction under it.

Tests, in `core/vdbe/statement_lifecycle_tests.rs`, all under MVCC:
- `test_writer_beside_a_statement_reading_no_table_commits_at_its_halt`
- `test_reads_beside_a_statement_reading_no_table_see_commits_made_between_them`
- `test_reader_ends_the_transaction_beside_a_statement_reading_no_table`
- `test_writer_after_a_finished_read_commits_at_its_halt`
- `test_read_of_a_pragma_function_ends_the_transaction_it_began`: the helper statement a
  table-valued pragma runs takes no part
- `test_reader_dropped_while_commit_waits_leaves_the_transaction_to_it`

`a_rebuild_beside_an_open_write_waits_for_it_and_is_made` (ivf_rq, the entry "An ivf_rq rebuild
under MVCC claims only where no other write is unseen") again steps OPTIMIZE INDEX on the connection
it reads from, where it found this.

Fail-first, one mutation at a time over the core lib tests at `763113a6a`, 6 of 6 red:
- every root statement counting, as before: the first three tests and the ivf_rq test;
- a Transaction instruction taking no part: 4 tests, upstream's sibling-reader test among them;
- COMMIT taking no part: the COMMIT test;
- a statement that finished keeping its part until reset: 6 tests;
- a helper statement taking a part: the pragma test;
- a part never uncounted: most of the MVCC suite.
A seventh, a reset releasing the part, changed no test: every road that resets a running
statement has released it already, and a statement that restarts rightly keeps it. That line is not
in the fix.

### Under MVCC a writer beside a reader deferred its commit, and a rollback could take it back
2026-10-30 `40bdb6de4`, `95635eeab`.
`core/mvcc/database/mod.rs`, `core/mvcc/cursor.rs`, `core/vdbe/mod.rs`, `core/vdbe/execute.rs`,
`core/connection.rs`, `core/statement.rs`, `core/storage/pager.rs`. A writer that finished while
other statements of its connection read the shared implicit MVCC transaction deferred its commit to
the last of them. Its caller saw success before the write was durable or visible to other
connections, and if the transaction then ended in a rollback the write was lost: a reader dropped
mid-scan, or a later writer failing without a statement savepoint. Upstream pinned both losses in
tests (`test_mvcc_completed_writer_changes_lost_when_last_reader_abandoned`,
`..._when_joining_writer_errors`) and described this fix in a FIXME in `halt`. The transaction also
held the exclusive transaction as long as any statement read it, so every other connection's writer
waited for the reader to finish.

A writer now commits at its own halt, as SQLite's does. If other statements still take part in the
transaction when the commit lands, it goes on for them in a successor
(`MvStore::begin_successor_tx`). The successor's begin timestamp is drawn while the writer still
holds the exclusive transaction, and nothing has committed since the readers' snapshot (the
exclusive transaction refuses a stale one), so the successor sees exactly what they saw, the
writer's rows included. It reads through the same WAL read mark and takes over the
blocking-checkpoint read lock. Each MVCC cursor reads through its transaction's lineage
(`MvccTxnLineage`) and moves to the successor before its next read. A write asserts that its
transaction has not gone on: while a statement writes, another writer of its connection is busy and
a COMMIT or ROLLBACK is refused.
- **A writer that wrote nothing** gives up the exclusive transaction, and the readers keep the
  transaction as it was.
- **A writer's rollback** beside readers undoes its own writes alone, and they go on in a successor
  with the view they had before it wrote (`Connection::rollback_writes_continued`). ROLLBACK, and an
  `OR ROLLBACK` conflict, still end the whole transaction, and a statement still reading fails at its
  next step, as SQLite's do.
- **A commit dropped mid-way** beside readers hands them a successor too, and only the writer's other
  writes are undone: the readers keep their read transactions in every database, where the abandoned
  commit's cleanup used to end them (`CommitState::cleanup_abandoned_mvcc_commit`).
- **The decision** is taken when the commit lands or is dropped, from the statements taking part
  then, and a commit step that yields and runs again draws no second successor. A transaction kept
  for readers who left while the commit waited is ended with it.
- **While a commit is in flight**, until it has ended or handed on its transaction, the connection's
  other root statements yield (`Statement::wait_for_sibling_commit`). A sequence's inner commit is its
  own statement's.
- **Attached databases**: an attached database's transaction goes on in a successor of its own,
  walked in database order. Beside an MVCC main every attached database runs MVCC (ATTACH makes a
  fresh one MVCC), and TEMP, the one database that uses WAL, keeps its read transaction for the
  readers.

Tests, under MVCC, in `core/vdbe/statement_lifecycle_tests.rs` but the last two:
- `test_completed_writer_commits_beside_a_sibling_mvcc_reader` (upstream's deferral test, turned);
- `test_mvcc_completed_writer_changes_survive_an_abandoned_reader` and
  `test_mvcc_completed_writer_changes_survive_a_failing_joined_writer` (the two pinned losses);
- `test_reader_keeps_its_view_beside_a_committed_writer`: through a cursor it opens at each key and
  through one open since its start, the reader finds the writer's row and not a row another
  connection commits after it;
- `test_a_resumed_commit_draws_one_successor`, `test_reader_waits_while_a_sibling_commit_is_in_flight`,
  `test_reader_dropped_while_a_writer_commits_leaves_no_successor`;
- `test_writer_dropped_mid_write_beside_a_reader_rolls_back_only_its_rows`,
  `test_writer_writing_nothing_beside_a_reader_gives_up_the_exclusive_transaction`;
- `test_writer_dropped_mid_commit_beside_a_reader_leaves_it_its_transaction`,
  `test_commit_dropped_mid_commit_beside_a_reader_leaves_it_both_views`: a commit dropped while it
  waits mid-way;
- `test_successor_holds_a_blocking_checkpoint_off_until_its_reader_ends`: the successor holds MVCC's
  stop-the-world gate until its reader ends;
- `test_reader_keeps_its_attached_view_beside_a_committed_writer` (both cursors),
  `test_a_transaction_kept_for_a_reader_gone_during_the_commit_ends_with_it`,
  `test_reader_reads_on_in_an_attached_memory_database_beside_a_committed_writer`,
  `..._beside_a_failed_writer`, `test_reader_from_before_begin_reads_on_after_commit`;
- `test_reader_keeps_its_temp_read_transaction_beside_a_committed_writer` and
  `..._beside_a_failed_writer`: TEMP's read transaction stays with the reader and ends with it;
- `fts_writes_commit_with_a_writer_beside_a_reader` (FTS);
- in `core/mvcc/database/tests.rs`,
  `test_continued_commit_landing_after_the_other_left_leaves_no_successor` and
  `test_continued_commit_dropped_after_the_other_left_leaves_no_successor`: a commit that lands, or
  is dropped, after the statement it would have gone on for has left.
Fail-first, one mutation at a time over the core lib tests at `d36a04607`, 19 of 19 red:
- a writer deferring its commit to the readers beside it, as before: 15 tests;
- the commit leaving no successor, so the transaction ends under the readers: 9 tests;
- cursors reading on in the transaction they opened in: the two view tests;
- the successor not taking over the blocking-checkpoint read lock: the checkpoint test;
- a writer that wrote nothing keeping the exclusive transaction: its test;
- a writer's rollback ending the transaction under the readers: 2 tests;
- statements not waiting while a sibling commit is in flight: its test;
- a successor drawn whether or not a statement still reads: the two `test_continued_commit_*` tests;
- a commit step run again drawing a second successor: its test;
- the end of the commit leaving a transaction kept for readers who left: its test;
- an attached database's commit leaving no successor: the two attached tests;
- the attached commits taken by first entry instead of in database order: the two attached tests
  hang;
- a commit counting as in flight until its state machine is dropped: upstream's
  `test_auto_checkpoint_refreshes_index_metadata_after_schema_change` hangs;
- a sequence's inner commit holding its own statement off: the two sequence tests hang;
- ROLLBACK leaving the readers a successor: its test;
- the abandoned commit's cleanup tearing the transaction down whatever the commit handed on: 2 tests;
- a commit handed on leaving the abandoned writer's other writes in place: 3 tests;
- a commit, or a rollback, ending TEMP's read transaction under the readers: one TEMP test each.
  At first these two changed no test: the tests named for an attached WAL database attached an
  in-memory database, and that runs MVCC. `95635eeab` added the TEMP tests, which fail under them,
  and named the in-memory tests for what they test.

### A yield point of one state machine was a point of another's too
2026-10-30 `f96c8fea8`. `core/mvcc/yield_points.rs`, `core/mvcc/yield_hooks.rs` and each family of
yield points. A `YieldPoint` was its ordinal and its family's point count, so two families with the
same count shared every point. `CommitYieldPoint` and `CursorYieldPoint` both have nine:
`LogRecordPrepared` was `SeekStart` and `BeforeFinishCommittedTx` was `ExistsBtreeFallback`.
Without `fts`, `IndexMethodYieldPoint`'s two points were `TransactionYieldPoint`'s. A test's
`FixedYieldInjector` matches points alone, so a test asking a commit to yield mid-way yielded at the
first cursor point of the same number, an INSERT's existence check, before the commit began. Every
such test of a commit in flight held a statement mid-write instead. Each family now names its state
machine (`YieldPointMarker::DOMAIN`, `YieldDomain`), and a point is its domain, ordinal and count.
The concurrent simulator's plans read the ordinal and the count as before.

Found by the fail-first of the entry before: a resumed commit that drew a second successor past the
exclusive transaction changed no test, because no test's commit ever resumed there.

Tests, in `core/mvcc/database/tests.rs`:
- `test_insert_in_middle_commit_of_create_index_returns_err` (upstream's) yields where it says, mid
  commit of the CREATE INDEX, where an INSERT beginning is refused Busy;
- `test_insert_begun_during_create_index_build_is_refused_at_commit` keeps the case the test reached
  before, at the cursor point it yielded at: an INSERT begun while the index builds is refused at
  its COMMIT.
Fail-first at `95635eeab`: with every family's points taking the commit's domain again, so that
points of different machines collide, upstream's test fails, 1 of the core lib suite's 2,673.

### Under writes that never pause, neither log stayed bounded
2026-10-31 `e3736211a`, `06477fa5b`, `7db9477d9`, `2ea9a3362`.
`core/storage/wal.rs`, `core/mvcc/database/checkpoint_state_machine.rs`,
`core/mvcc/database/mod.rs`, `core/mvcc/database/background_checkpoint.rs`. In a two-minute round
of `fork/repro`'s `mvcc_rolling` (four writers committing without pause under synchronous NORMAL, a
64 KB threshold, the background checkpoint on), the rolled logical log grew to 1.96 GB and the WAL
to 4.66 GB, both about linearly. Three causes, each fixed:
- **A WAL restart waited for every reader of the database file.** A writer restarted the WAL only
  while no other connection of its process read the database file under read mark 0:
  `try_restart_log_for_write` upgraded the writer's own lock on read mark 0. Those readers read the
  database file alone, and a backfill still needs read mark 0 exclusively, so a restart cannot
  change what they read. Beside transactions that overlap without pause, as an MVCC database's do,
  some reader always held read mark 0 once the WAL was fully backfilled. A reader in another
  process never stopped the restart (upstream's `restart_ignores_db_file_readers`), and now one in
  the same process does not either, as in SQLite's `walRestartLog`. Test:
  `storage::wal::test::test_db_file_reader_of_another_connection_allows_wal_restart_but_stops_backfill`
  (before: the write appended at frame 13 after frame 12).
- **A checkpoint's frames waited for a backfill that never came.** A passive checkpoint backfills
  its frames right after its commit, and readers of the database file refuse that while they read.
  The next checkpoint then began on a WAL not fully backfilled and appended to it. A passive
  checkpoint now first backfills what an earlier one left, then takes its snapshot, so that its
  write restarts the WAL. Test:
  `mvcc::database::tests::test_a_passive_checkpoint_backfills_what_the_last_one_left_and_restarts_the_wal`
  (before: from frame 7 to frame 14, with no restart).
- **Commits outran the checkpoint.** A commit with the log `BEHIND` thresholds behind checkpointed
  inline only when no checkpoint ran, and under writes that never pause one nearly always does. It
  now waits for the checkpoint running on its database to end, parked as a group-commit waiter is,
  unless that checkpoint is stepped on its own thread. It then checkpoints inline if the log is
  still behind. Test:
  `mvcc::database::background_checkpoint::tests::test_a_commit_far_behind_waits_for_the_checkpoint_running_on_its_database`
  (with the wait taken out, 1,000 commits returned while the pool's checkpoint was held, ending
  306,063 bytes behind at a 4 KB threshold).
Measured on mt1 (cores 0-9 of a Xeon E5-2640 v4), in rounds of about 60 s with `mvcc_rolling`'s
`THRESHOLD` (`26b125fde`), alternating before and after, each round begun at a load under 12 of the
40 CPUs. At the 64 KB threshold, two rounds each: before, the log reached 20.6 and 22.5 MB, the WAL
572 and 814 MB, at 25.2k and 31.2k rows a second; after, 0.4 MB, 1.2 and 0.7 MB, at 37.8k and 38.3k.
At the default threshold (4,120,000 bytes), four rounds each: before, the log reached 30.8-43.9 MB
and the WAL 488 MB-1.28 GB; after, 19.6-23.4 MB and 15.8-22.5 MB. The rows a second were level,
33.7k, 38.3k and 39.0k before against 38.0k, 39.8k and 41.5k after, with one round each way at
about 25k while another job raised the load past 20. On this laptop, whose disk stalls fsyncs for
seconds under load (33 s once during the round), the writers now wait the stalls out: in a
two-minute round, 5.4k rows a second against 81k, with the log at most 0.5 MB and the WAL 45 MB
against 1.96 GB and 4.66 GB.

### A checkpoint asked for beside a running one did nothing and reported success
2026-10-31 `3bd2d58fc`. `core/mvcc/database/checkpoint_state_machine.rs`,
`core/mvcc/database/background_checkpoint.rs`. Under passive checkpoints one checkpoint runs on a
database at a time, and one that found another running returned at once with an empty result. That
suits a commit's own checkpoint, whose work the running one does. But `PRAGMA wal_checkpoint`,
`Connection::checkpoint` and the checkpoint before a journal mode change got the same result, and
the pragma reported it as not busy: a TRUNCATE that did nothing reported success, the logical log
left as it was. The blocking path, whose lock refuses a second checkpoint, reports Busy (upstream's
`test_flag_off_truncate_busy_when_lock_contended`). The background checkpoint makes the overlap
common. WorkReef cognition's erasure check found it once it turned the pool on: after TRUNCATE
checkpoints that reported success, the planted text had not reached the database file, and the
erased words were still in the logical log. Now only a commit's own checkpoint leaves the work to
the running one; any other reports Busy. A pool turn that meets a running checkpoint gives way to it
on that Busy, in place of a check before its checkpoint that left a window between the two.
Tests: `mvcc::database::tests::test_passive_truncate_beside_a_running_checkpoint_reports_busy`. A
passive checkpoint stopped after its first step holds the database. On another connection, PRAGMA
wal_checkpoint(TRUNCATE) and `Connection::checkpoint` must each report Busy and leave the log as it
was, and once the running one ends a TRUNCATE empties the log. On the code before, the pragma
returned success. The pool's `test_a_turn_beside_another_checkpoint_waits_for_it` now meets the
running checkpoint at its gate. Both mutations are red, one at a time: the gate's empty result
restored fails both tests, and a pool turn that counts Busy as a failure fails the pool's.

### Beside writers, a TRUNCATE left what came before it in the logical log
2026-10-31 `eeeeedc1e`. `core/mvcc/database/checkpoint_state_machine.rs`,
`core/mvcc/persistent_storage/logical_log.rs`, `core/mvcc/persistent_storage/mod.rs`,
`core/vdbe/execute.rs`. Under passive checkpoints writers commit while a checkpoint runs. When a
commit landed after a TRUNCATE's snapshot, the TRUNCATE kept the log segment holding it, and with it
every frame from before the TRUNCATE, and still reported success. A deletion checkpointed that way
left the deleted rows' values in the log's files until some later checkpoint freed the segment, and
nothing told the caller when that happened. WorkReef cognition's erasure check found it beside a
writer committing every 10 ms: 20 runs of 20 left the erased values in the log.

Now an explicit TRUNCATE runs checkpoint passes until the log holds no segment of the generation it
began beside, or an older one:
- The first pass empties the WAL and seals that segment, or readies a file to roll into, and a later
  pass frees it. The TRUNCATE returns once nothing committed before it began is left in the WAL or
  the log.
- The later passes write only what was committed after the first one's snapshot, so they run as
  passive passes: a reader of the WAL, which a TRUNCATE must wait out, does not hold them back. A
  first version ran every pass as a TRUNCATE. Beside a writer committing every millisecond,
  WorkReef's check then stayed Busy for 30 s, where a single pass had got through.
- It holds `checkpoint_in_progress` from its first pass to its last, so no other checkpoint runs in
  between.
- A pass that leaves the log as it found it can free no more, as when a commit made before the
  TRUNCATE has not finished. The TRUNCATE then reports Busy, never a success that leaves those frames
  in the files. Stepped again after Busy, the same `PRAGMA wal_checkpoint` goes on toward its first
  attempt's goal, so commits made since it began do not hold it back. A new call of
  `Connection::checkpoint` starts afresh.

The log counts the segments it begins, a roll or a truncate beginning the next
(`LogicalLog::generation`), and reports which it holds (`DurableStorage::logical_log_hold`). A
storage that does not count them reads as one segment. A commit's own checkpoint and the pool's are
passive, and unchanged.

Tests:
- `storage::erasure_tests::a_truncating_checkpoint_beside_a_writer_leaves_no_deleted_value_in_any_file`:
  rows are inserted, half of them checkpointed into the file, and deleted, and a TRUNCATE runs beside
  a writer that commits once each of its passes has taken its snapshot. No deleted value is left in
  the database file, the WAL or any of the log's files, and every live value and every row
  committed beside it is found. On the code before, 200 deleted values were left in the log.
- `mvcc::database::rolling_log_tests::test_a_truncate_that_cannot_free_what_came_before_it_reports_busy`:
  a commit still being made, below every other, keeps any snapshot from covering them. The TRUNCATE
  rolls the log, can free no more and reports Busy, its frames kept; once the commit is made, the
  next TRUNCATE empties the log. On the code before, it reported success.
- `mvcc::database::rolling_log_tests::test_a_truncates_later_passes_run_beside_a_reader_of_the_wal`:
  a reader begins on another connection once a later pass has written the WAL, and the TRUNCATE
  completes beside it. With every pass a TRUNCATE, it reported Busy.
- `mvcc::database::rolling_log_tests::test_a_busy_truncate_stepped_again_goes_on_toward_its_first_goal`:
  a TRUNCATE statement that reported Busy is stepped again after more commits and finishes in one
  pass. Started afresh, it took more.
- Upstream's `test_passive_truncate_keeps_log_frames_committed_after_snapshot` now checks that the
  first pass keeps the commit that landed after its snapshot and that the next pass checkpoints it,
  leaving the log empty. With upstream's bug (#8076) restored, it still fails.

Each mutation is red, one at a time: no goal; the goal one generation short; success where a pass
moved nothing; the gate taken again for each pass; the later passes run as TRUNCATEs; a busy retry
that forgets its goal; later passes that do not look at the log; and upstream's #8076 bug.

## Additions

### Background checkpoint (`PRAGMA mvcc_background_checkpoint`)
2026-10-28 `b207b3dec`; 2026-10-30 `1c934b336`, one pool for many databases.
`core/mvcc/database/background_checkpoint.rs`, wired in `core/mvcc/database/mod.rs`; the pragma
in `sqlite/parser/src/ast.rs`, `core/pragma.rs`, `core/translate/pragma.rs`, `core/connection.rs`.
On, the commit that crosses the MVCC checkpoint threshold hands the checkpoint to its store's
checkpoint pool and returns, so no writer pays for one. Off by default, since the pool's threads
step the IO backend beside the host's own. Encrypted databases and ones read through an external
page codec keep their inline checkpoint, so key material stays on the connections given it.

A pool (`CheckpointPool`) runs the checkpoints of any number of databases on a bounded number of
threads. Every store starts on the process's shared pool, which runs a quarter of the available
parallelism at a time, at least one checkpoint and at most eight; a store can be given another
(`MvStore::set_checkpoint_pool`), one per storage device for instance. Threads start as work comes
in and stop after ten idle seconds. A database waits in the queue at most once and is checkpointed
by one thread at a time; a commit asking while it is queued or checkpointed sets a bit and takes no
lock. The queue is served in turn: a database still past its threshold after 50 ms of checkpoints
goes back to the end of it while another waits. The pool holds a database by a weak reference and
opens a connection for each turn, without the connect-time statistics query, so it neither keeps a
closed database alive nor counts as one of its connections between turns. An attached database
hands its checkpoints to its store's pool too.

What a thread used to sleep for is a timer, and an idle thread keeps them all, so the pool waits on
one timer whatever the number of databases; timers due within 10 ms fire together. After a turn
that left a log a checkpoint could not truncate, the pool looks at the log 100 ms later and, once it
has stayed the same size, checkpoints to empty it. A turn that fails, or finds another checkpoint
running on its database, is retried 10 ms later, the delay doubling up to a second, holding no
thread. Once a database's uncheckpointed log reaches four thresholds, whether the pool fell behind
or its turns fail, its commits hand nothing to the pool. Each waits for a checkpoint running on the
database on another thread to end, then checkpoints inline if the log is still that far behind
(`7db9477d9`, "Under writes that never pause, neither log stayed bounded"). A turn that panics is
caught: the pool counts it, turns that database's background checkpoint off, and goes on serving
the others. A pool whose thread limit is zero lets go of its databases and their commits checkpoint
inline. `CheckpointPool::stats` reports the threads, the queue, the timers, and the checkpoints,
failures, deferrals, inline checkpoints and panics, with the time spent checkpointing and waiting in
the queue.

Two defects of the thread per store are gone. At `mvcc_checkpoint_threshold = 0` an empty log counts
as past the threshold, so the thread checkpointed it forever; the pool stops once the log is covered.
And its test failed 2 times in 160 runs: after a checkpoint that emptied the log, commits under the
threshold leave a small log that no request follows, by design, and the test waited for an empty
log. The test now holds the pool's checkpoint after its snapshot and commits behind it, which makes
the checkpoint keep the log, and waits for the pool to empty it once the writes pause.

Tests, in `core/mvcc/database/background_checkpoint/tests.rs`, each on a pool of its own:
- `test_background_checkpoint_runs_off_the_commit_and_truncates_when_writes_pause`,
  `test_a_kept_log_waits_for_its_writes_to_pause`, `test_background_checkpoint_is_off_by_default`;
- `test_one_pool_checkpoints_many_databases_on_its_threads`: 24 databases on two threads;
- `test_databases_wait_in_the_queue_while_every_thread_is_busy`: three databases ask while the one
  thread is held in a turn: they wait in the queue, no second thread starts, and one dropped while
  it waits leaves the queue at once;
- `test_a_database_dropped_with_a_timer_armed_leaves_the_timers`;
- `test_a_database_past_its_threshold_after_a_slice_gives_way_to_another`;
- `test_a_failing_checkpoint_is_retried_later_and_holds_no_thread`, through a failure point at
  `CheckpointYieldPoint::BeforeCollectTableRows`;
- `test_commits_checkpoint_inline_while_the_pool_is_far_behind_on_their_database`,
  `test_a_turn_beside_another_checkpoint_waits_for_it`,
  `test_at_a_threshold_of_zero_the_pool_stops_once_the_log_is_covered`;
- `test_a_dropped_database_leaves_its_pool_and_the_idle_threads_stop`,
  `test_a_store_moved_to_another_pool_leaves_the_first`, `test_a_turn_that_panics_is_contained`,
  `test_a_pool_without_threads_leaves_checkpoints_to_the_commits`,
  `test_an_attached_database_checkpoints_on_its_pool`.
A flake hunt runs every test whose path names rolling or background_checkpoint, 8 runs side by side,
25 times each. The first found `test_a_failing_checkpoint_is_retried_later_and_holds_no_thread`
failing 2 times in 200: it let the pool's turns succeed before the commits it expected to
checkpoint inline, so a turn could checkpoint first and no commit had to. Its turns now fail until
those commits are done. On mt1 at `26b125fde`, 200 of 200 runs passed. On this laptop the hunt fails
whole batches at once, every process at the same instant, during its disk's fsync stalls (one
checkpoint took 23.9 s), so a hunt there does not measure the tests: 600 of 600 runs passed in one
hour at `95635eeab`, and 176 of 200 and 151 of 200 in others.
Fail-first, one mutation at a time over the pool's and the rolling log's tests at `f5a429b2e`,
12 of 12 red:
- a database past its threshold keeps the thread whoever waits: the slice test;
- a failed or deferred turn is retried at once, without backing off: the retry test and the kept
  log's;
- commits never checkpoint inline however far behind the pool is: the retry test and the inline
  one;
- a turn checkpoints beside another checkpoint: its test;
- threads start past the limit: the queue test;
- at a threshold of zero an empty log is past it: the zero-threshold test;
- a kept log is never looked at again: 4 tests;
- a dropped or moved store stays in the pool: the timer test and the queue test;
- a pool without threads still takes requests: its test;
- only the main database hands its checkpoints off: the attached database's test;
- a turn that panics takes its thread down: its test;
- a kept log is checkpointed at each look, whether or not writes went on: the kept log's test.

mt1, the bench's `mvcc_control` with the thread per store (2 writers, a maintainer rewriting 64-row
batches, a reader; 256 KB threshold), 20 runs each way, all passing: writer commit p99 19.9 ms inline
→ 5.3 ms with the thread (medians of the runs; worst run 25.3 → 6.9 ms), worst commit 28.7 → 9.1 ms
(medians), and the log 0 bytes after writes pause in all 20 runs against 160 KB left inline. Every
commit over 100 ms is matched by an fsync of about its length in the same run. `ivf_mvcc`
(8 writers × 4,000 vectors of 128 dimensions under maintenance) passed its 520 checks in all 7
runs, 97-110 s each with the thread against 137-211 s inline.

### The logical log rolls under writes that never pause
2026-10-30 `fdcdb1259`, `dd5a85501`; `fork/repro`'s `mvcc_rolling`, `f5a429b2e`.
`core/mvcc/persistent_storage/logical_log.rs`, `core/mvcc/persistent_storage/mod.rs`, recovery and
the completion of an interrupted checkpoint in `core/mvcc/database/mod.rs`,
`core/storage/journal_mode.rs`. A passive checkpoint truncates the logical log only when no commit
landed after its snapshot, which under writes that never pause never happens: the log kept every
frame it was ever given, so it grew without bound, and recovery replayed all of it. In three
rounds of up to 30 s of `fork/repro`'s `mvcc_rolling` at `95635eeab` (four writers committing
without pause, a 64 KB threshold), the log reached 850 MB with the roll taken out and 377 MB with
it: the roll alone keeps the log in few files, not in small ones. "Under writes that never pause,
neither log stayed bounded" bounds their size.

The log of every MVCC database's default storage (`Storage::new_rolling`) now rolls through four
files, `.db-log` and `.db-log.1` to `.db-log.3`. A checkpoint that has to keep frames seals the
segment being written, at once or, while a frame is being written, as soon as it lands, and the
next frame goes to a free file. That segment's header holds where it sits in the log
(`LogSegment`, in the header's reserved bytes, under flag bit 0): its number, which no file has
held before; its predecessor's number; the log offset and running CRC its predecessor ended with,
which it starts at and chains from; and the newest commit before it. A later checkpoint that
covers every commit of a sealed segment truncates its file, which a later roll reuses; once every
commit is covered the log resets as before, into a numbered segment. The fsync of a commit also
covers the frames written before a roll, which are in the sealed segment's file. The first roll
readies a file, creating and fsyncing it, and the next rolls into it. A log whose frames carry
portable changes does not roll, as their readers follow a single file. Offsets the checkpoint
threshold counts run across segments.

Recovery reads every file's header and begins at the newest segment, going back through the
predecessors while a segment's prior commits include one no checkpoint covered; a needed
predecessor that is missing fails the open. It replays those segments as one log. A segment that
does not begin where its predecessor ended, with the CRC it ended with, was written past a torn
tail: it is not replayed, its file is freed, and the next segment takes a number past it. Completing
an interrupted checkpoint at open rewrites the newest segment's header, in its own file. As a
leftover `.db-log` already does, a log file in any slot keeps a WAL-mode database from opening.

Tests:
- `core/mvcc/persistent_storage/rolling_tests.rs`, over a log of its own:
  `a_log_that_has_to_keep_frames_rolls_and_the_next_checkpoint_frees_it` (200 checkpoints that each
  keep a commit: two segments, under 2 KB), `recovery_replays_every_segment_and_writing_goes_on_in_the_newest`,
  `a_sealed_segment_torn_before_its_successor_ends_the_log`,
  `recovery_refuses_a_log_whose_needed_older_segment_is_missing`,
  `recovery_begins_after_the_segments_a_checkpoint_covered`,
  `a_recovery_follows_the_chain_past_a_successor_left_by_a_torn_segment`,
  `a_roll_asked_while_a_frame_is_written_happens_once_it_lands`,
  `a_reset_after_rolling_outranks_a_segment_freed_before_it`, and
  `a_sync_after_a_roll_makes_the_sealed_segment_durable`, on files that lose at a power loss what
  no sync covered; `an_encrypted_log_rolls_and_recovery_decrypts_every_segment`, whose segments
  each have a salt of their own;
- `core/mvcc/database/rolling_log_tests.rs`, through passive checkpoints that each see a commit land
  after their snapshot: `test_a_log_every_checkpoint_has_to_keep_rolls_and_stays_bounded` (60
  checkpoints, then a restart), `test_recovery_replays_a_log_rolled_over_segments`,
  `test_a_restart_after_the_first_file_was_freed_keeps_the_newer_segments`,
  `test_a_database_beside_log_files_left_by_another_refuses_to_open`,
  `test_a_rolled_log_reopened_without_passive_checkpoints_is_cleared_by_a_blocking_one`;
- `test_the_pool_keeps_the_log_bounded_under_writes_that_never_pause`, in the background
  checkpoint's tests: 50 pool checkpoints that each keep an 8 KB commit, then a restart.
Fail-first, one mutation at a time over the pool's and the rolling log's tests at `f5a429b2e`,
16 of 16 red:
- the log never rolls: 14 tests;
- recovery reads the first file alone: 9 tests;
- completing an interrupted checkpoint rewrites the newest header into the first file: 2 tests;
- a segment's predecessor is the latest segment that begins before it: the torn-successor test;
- a segment written past a torn tail is replayed: 2 tests;
- a missing predecessor recovery needs is passed over: the missing-segment test;
- numbering starts over after a recovery: 3 tests;
- a sync covers only the segment being written: the sync test;
- the log rolls under a frame being written, or a roll asked then never happens: the
  frame-in-flight test, under each;
- sealed segments are never freed: 7 tests;
- a reset after rolling writes an unnumbered header: the reset test;
- a rolled segment does not carry the newest commit before it: 7 tests;
- a segment at offset zero is followed back like any other: 3 tests;
- the open counts the first log file alone: the test of a database beside another's log files;
- recovery leaves the files it does not replay as they are: 2 tests.
`dd5a85501`'s two tests, at `d36a04607`, 4 of 4 red: the readers of the segments after the first
decrypting nothing, and a rolled segment taking its predecessor's salt, fail the encrypted test; a
checkpoint that clears the whole log keeping the sealed segments fails the blocking-checkpoint
test; recovery forgetting the sealed segments before the one it goes on writing in fails it and a
recovery test.
Killed mid-write by `fork/repro`'s `mvcc_rolling` at `26b125fde`, 30 rounds of up to 3 s and 3 of
up to 30 s all reopened with `PRAGMA integrity_check` ok and every reported commit held, the log's
files at most 534 KB; with the roll taken out, 3 rounds of up to 30 s held up to 49 MB. The upstream
sync runs the short rounds (`d36a04607`).

### Logical-log fsync stats
2026-10-28 `dde381edb`.
`core/mvcc/persistent_storage/mod.rs`. `mvcc::persistent_storage::logical_log_sync_stats()`
returns the count, total and longest µs of logical-log fsyncs. A durable commit cannot be faster
than its fsync, so a commit no slower than the longest fsync waited on the disk.

### The `ivf_rq` index method
2026-10-28 to 2026-10-29: `4174498fa` (the index and its registration), `303f05e78` (the MVCC
cursor's forward bound), `effd5c1db` (filtered queries), and the commits after them that touch
`core/index_method/ivf_rq.rs` (`git log main..workreef -- core/index_method/ivf_rq.rs`).
`core/index_method/ivf_rq.rs`, registered in `core/index_method/mod.rs` and `core/ext/mod.rs`;
its rowid walks use a forward bound (`CursorTrait::set_forward_bound`, implemented by the MVCC
cursor in `core/mvcc/cursor.rs`) so they stop at their range's end instead of stepping through
the rows after it. A RaBitQ IVF vector index over rowid backing tables, transactional under MVCC.
Tests: `index_method::ivf_rq::tests`.

Two defects found by reading it, each reproduced by a test that failed before its fix:
- A deleted or updated vector's code stayed in its packed block, and that stale copy counted toward
  the 1-bit bar the live entries must reach: a query near 16 vectors packed and then updated away
  returned no rows. A delete now names the block in the vector's dead row, a query skips those
  copies before they score, and maintenance takes a block off the dead rows when it rewrites it
  and compacts a block holding 8 stale copies.
- The fast scan summed in 16 bits, which wraps past 1024 padded dimensions: at 3,072 dimensions the
  query's own vector summed to -88.5 instead of 400.7. It sums in 32 bits.

Four gaps, each closed with tests that fail with the gap put back:
- Any WHERE term the pattern did not cover kept the query off the index. An index method can now
  declare `filtered_queries` (`core/index_method/mod.rs`): the planner keeps the query's ORDER BY
  and LIMIT after those terms, passes the cursor one more value, and tells it of each row they
  accept (`Insn::IndexMethodAccept`, calling `IndexMethodCursor::query_accept`;
  `core/translate/main_loop/open.rs`). ivf_rq gives out rows in order of exact distance, in rounds
  of four times the rows and partitions of the last, until no row still to come can displace one of
  the LIMIT nearest accepted. The planner (`find_table_access_plan` in
  `core/translate/optimizer/mod.rs`) plans the query without the pattern first and uses the
  pattern only where the method's cost estimate, given the share of rows the plan keeps
  (`IndexMethodCostContext::filter_selectivity`), is below that plan's cost plus its sort: a rowid
  lookup and a selective indexed filter keep their plans. Single-table SELECTs only. A DISTINCT or
  aggregate query keeps its plan without the index, by the fix for an index method's LIMIT above;
  ivf_rq's `a_distinct_query_applies_its_limit_after_its_distinct` covers it, and an ivf_rq LIMIT
  of 0 or below now gives no rows or every row, where it gave one.
- Building was quadratic: every partition came from bisecting splits of one. A statement inserting
  1,024 rows or more into an index trained for under a quarter of the partitions they call for,
  CREATE INDEX over a populated table among them, now rebuilds it: k-means over a sample, balanced
  on the sample so no partition starts past the split bound or under the retirement bound, then one
  pass coding every vector into packed blocks.
- Maintenance ran only on OPTIMIZE INDEX, one job per call. Every statement that writes the index
  now does the maintenance its writes made due, up to one job per row written (40 statements of 100
  random rows needed 0.47 a row; one job per 64 rows left all 4,000 rows in one partition), and one
  OPTIMIZE INDEX does every job. A job claims its partition first and keeps an undo log; on a
  write-write conflict it reverts and leaves the rows to the writer holding them.
- Partitions drifted: a split left its neighbours' entries where they were, entries that stayed kept
  codes against the old centroid, and nothing merged. A split now rechecks its 8 nearest neighbours,
  moving each entry to its nearest partition and recoding those that stay, and a partition left
  under a sixteenth of the split bound retires into its neighbours. Over a uniform blob of 3,000
  vectors, 1.7% end up outside their nearest partition, against 5.3% rechecking 4 neighbours and
  9.7% rechecking none.

The query path, measured on glove-100 (1,183,514 vectors, 2,000 queries, one query at a time on the
laptop's performance core 4, alternating builds), after ScaNN, the RaBitQ library and FAISS were run
on the same data on mt1 (`/home/me/cognition-data/ann`): at the same 5.9% of the data scanned ScaNN
reached 0.907 recall@10 and ivf_rq 0.885, so the partitions were not what lost; the cost of each
scanned vector was, about 3.5 ns for ScaNN on mt1 against over 100 for ivf_rq.
- A candidate's code was a point read of its vector's row, about 1 µs each at this size: at 64
  probes and a rerank of ten times the LIMIT, 2,930 of the query's 5,523 µs. A block now holds each
  entry's extended code after its scan groups and the query refines in memory, and the vector's row
  no longer vouches for the entry: the dead rows, read for each partition before its blocks, name
  every stale copy, as the tests of revised and deleted vectors check. 181 to 364 queries a second,
  and 85 to 123 at 256 probes. `a_query_refines_its_candidates_without_a_point_read` counts the
  point reads a query makes (264 for 263 refined candidates before, the model's generation alone
  after) and `a_block_unpacks_to_the_codes_it_packed` checks the codes round-trip. The
  filtered-query cost estimate prices a code at the planner's price of a table row scaled by its
  bytes against its vector's (222 codes to a page at 768 dimensions) and a rerank at one page.
- A block is decoded and scored where its cursor holds it, from the page's own bytes when the cell
  fits its page (`walk_held`, `held_payload`, `version_and_codes`; one walk, `walk`, serves the
  key-only, held and copied reads). The counters and timers cost a load each unless `MEASURE` is
  on; on, they were about 7% of a query.

Then, measured on that core against ScaNN and the RaBitQ library run on it too
(`/home/me/cognition-data/tmp/ann/samecore*.sh`, results in `samecore/`), queries a second at
recall@10 0.90 / 0.95, from 585 / 336:
- Each entry carries a 7-bit extended RaBitQ code whose top bit is its 1-bit code, stored once; an
  entry is scored from its 7-bit code as soon as its 1-bit estimate raised by its error bound
  reaches the worst of the `refine * LIMIT` best 7-bit estimates so far, the RaBitQ library's order,
  and those best are reranked exactly: 688 / 376. At a refine of one the query finds 97% of the
  exact top ten, where 4-bit codes found 89%.
- The bound test runs over a group of 32 entries at once, AVX2 in f64 with the scalar test's
  decisions, and the lower bits' product is an AVX2 kernel: 796 / 439.
- CREATE INDEX writes each code past its first 1,024 rows once, straight into its block, where
  every row was written as a tail entry and then again: the file is 704 MB, all of it in use, where
  it was 1,262 MB with 558 MB free: 817 / 451.
- A partition's bookkeeping, dead rows, tail and blocks lie under one key range,
  `partition << 42 | kind << 40 | id`, read with one seek where it took three: 857 / 472.
- The blocks of the partitions queries probe are held in memory, shared by the index's cursors,
  within `cache_mib` (default 256): each partition's under a version its bookkeeping draws afresh
  at every rewrite, so a snapshot scores only blocks of its own version. Past the budget a
  partition is held only when queries probed it lately more often than the one it would push out.
  WAL queries no longer probe retired partitions: 947 / 525.
- Partitions number four times the square root of the rows, as the library's IVF index has them,
  trained by spherical k-means over unit vectors, 20 Lloyd iterations seeded from distinct samples
  past 64 centroids, and a rebuild codes its vectors over the cores: 1,248 / 691 (the square root,
  spherical: 1,044-1,100 / 544-590; four times it, plain k-means: 1,074 / 582).
- The centroids are also held in tiles of 64, dimension-major, so the nearest partition to a group
  of 16 vectors is found a tile at a time and a query ranks from the tiles' products: builds 3x
  faster (26 s against 81-83), queries unchanged.
- The rowids and 7-bit parts of a group's members that reach the bar are prefetched before the
  first is refined: 4 to 7% faster at every setting, 1,393 / 756.
- A fast-scanned member is summed exactly only within the quantization's error of the bar, where
  the exact sum can drop it; elsewhere its fast-scan sum serves its 7-bit estimate, as the library
  takes it (`a_query_sums_exactly_only_the_members_near_the_bar`): 1,436 / 762.
- The start of each held block is prefetched when a probe finds the blocks held, overlapping the
  reads of the partition's dead rows and tail: 10 to 17% faster, 1,585 / 872.
- The ranking reads the centroids rounded to bfloat16, half the bytes: 1,622 / 880.
- The ranking reads them rounded to int8 at each centroid's own scale, a quarter of the bytes, and
  ranks the best quarter again as many as it takes, and thirty-two, exactly before it cuts
  (`a_query_probes_the_partitions_nearest_it_exactly`): within the runs' spread of bfloat16.
- A query probes its partitions nearest first, so its bar rises soonest and fewer entries of the
  later partitions are refined: 7 to 11% faster at every setting, 1,766 / 881.
- A partition's bookkeeping, dead rows and tail are held beside its blocks with the snapshot they
  were read under, and a probe under another snapshot scores the partition from memory, reading no
  page, while no transaction writing the index is open and both snapshots are at or past the last
  commit that wrote the partition. Inserts and deletes leave the version alone, so two writers into
  one partition never write one row, and the version can't say whether its tail or dead rows moved.
  Each transaction is entered in a record every attachment of the index shares before its first
  write to the partitions table reaches a page, and taken off as it ends, raising the partitions it
  wrote to the commit position then: the WAL's last committed frame under its checkpoint sequence,
  or a timestamp drawn from the MVCC clock. A rolled-back one is raised too, as a rollback can follow
  a commit made durable before its statement was dropped; a WAL writer's rows are not held, its
  snapshot counting frames a rollback gives back. Off in attached databases and under multiprocess
  WAL, whose other processes' commits run no hook here. The next probe is resolved and the start of
  its blocks prefetched while each is scored, as the page reads used to hide that latency: 21 to 32%
  faster at every setting, 2,159 / 1,136
  (`held_rows_serve_later_snapshots_until_a_commit_writes_their_partition`, which also checks a
  commit inside a transaction, a rollback, OPTIMIZE INDEX and a checkpoint that restarts the WAL,
  `a_snapshot_never_takes_rows_held_under_a_later_one`, `held_rows_come_and_go_with_their_blocks`).
ScaNN runs 3,121 / 1,365 and the RaBitQ library at 7 bits 2,808 / 1,272 on the same core: ivf_rq is
1.3x / 1.1x behind the library, from 4.8x / 3.8x. The index is 181 bytes a vector at 100
dimensions, the library's 140.

An index created `WITH (rerank = 0)` answers with its best 7-bit estimates at their estimated
distances and reads no table row, so the table's vectors need not be in memory for a query. At a
refine of one it returns the rerank's own rows, which the rerank only orders, 3 to 8% faster (Cohere
at 48 probes 3,726-3,737 a second against 3,447-3,478); at more, recall falls
(`a_query_without_the_rerank_answers_from_its_estimates_and_reads_no_table_row`).

Vectors are padded to a multiple of 32 rather than to a power of two, rotated as the library's
FhtKac rotator does where the width is not one: four rounds of sign flips, each followed by a
transform of the largest power-of-two span, leading and trailing in turn, and a step of a Kac walk.
A 768-dimensional code is 692 bytes, not 916. On a 200,000-vector slice of Cohere at 768
dimensions the index is 213 MB instead of 264, recall at a refine of two is unchanged to the third
place, and queries run 8 to 17% faster at each setting: 1,752-1,796 a second at 48 probes (0.908)
against 1,579-1,671, 1,302-1,340 at 96 (0.955) against 1,176-1,225.
`a_rotation_pads_to_a_multiple_of_32_and_keeps_lengths_and_products`. With the changes after it,
the Cohere slice runs 3,803 / 2,394 queries a second at recall@10 0.90 / 0.95 on one core, against
3,774 / 2,785 for the RaBitQ library at 7 bits and 1,942 / 1,061 for ScaNN; at 768 dimensions the
bfloat16 ranking alone was worth 24% at 0.90, the ranking having read 6 MB of f32 centroids a query,
probing nearest first 12% more, and the held rows 4 to 29%, more the more partitions a query
probes.

A statement's maintenance shares the partitions where it found work its jobs did not cover with the
index's other cursors, and the next statements survey as many of them as they may do jobs, so that
work no longer waits for a statement that writes its partition or for OPTIMIZE INDEX
(`work_a_statement_left_due_is_done_by_the_next`, which left "partition 5 has a tail of 16"
before).

At 4 KB pages a 64-entry block of 128-dimensional codes is 8.4 KB and of 768-dimensional ones 44 KB,
so every block read assembles an overflow chain, and a 3 KB 768-dimensional table row takes a page
to itself; at 64 KB pages each fits one page. The page size is the database's, so it is the
application's choice; the measurements above are at 64 KB.

Building without the balancing, or splitting only past the split bound, gives more recall a probe
but slower queries at each recall target (Cohere at 0.90: 2,540 and 2,839 against 3,007), so the
balancing stays. The library's higher recall at the same probes is its probes scanning more: over
the same Cohere vectors FAISS's spherical k-means, as its runs train it, leaves its lists
unbalanced, and at 48 probes they hold 4.3% of the vectors against ivf_rq's 2.9%; at the same share
scanned ivf_rq's partitions find more of the top ten (0.910 against 0.901 near 2.9%, 0.957 against
0.951 near 5.8%), and its estimates lose none of them (`tmp/ann/partcmp/partcmp.py`). Still owed: the
rest of the gap at one query at a time, which is the cost of each scanned vector (about 56 ns all in
at 768 dimensions against the library's 30); the build takes 26 s on glove-100 over the laptop's
cores. A probe reads from pages only the partitions an open transaction has written (see
`OPTIMIZE INDEX` in steps).

### `OPTIMIZE INDEX` in steps
2026-10-29 `634a20562`, `076aaa87b`, `71e92960a`, `e24c165f0`, `869453d67`.
`core/index_method/optimize_steps.rs`, `Insn::IndexMethodOptimizeSteps`, translated in
`translate_optimize` (`core/translate/index.rs`), `Connection::connect_beside`. An index method can
declare `optimize_in_steps` (`core/index_method/mod.rs`); ivf_rq does. Outside a transaction,
`OPTIMIZE INDEX` of such indexes then holds no transaction of its own: it opens a connection beside
the statement's, with its key, page codec, synchronous level and automatic WAL maintenance, and
runs the method's work there one step to a transaction (`IndexMethodCursor::optimize_step`):
`BEGIN CONCURRENT` under MVCC, `BEGIN IMMEDIATE` under WAL, the first step a read transaction that
only finds the work. It yields between steps. A step that finds the write lock held reports busy
to the statement's busy handler and goes on from where it was; one whose commit loses to a
concurrent transaction is done again, up to 16 in a row. Inside a transaction, or for an index in
the temp database or an attached database with a key or codec of its own, it does all the work in
the statement's transaction, as before. Dropping the statement drops the step connection, which
rolls back a step in flight.

Before, `OPTIMIZE INDEX` did every job in one write transaction: under WAL every writer waited for
all of it, and under MVCC it held the exclusive transaction, so every concurrent commit made
during it failed. ivf_rq's steps do up to 16 jobs and start none past 10 ms once they have done
one; the first surveys every partition and does no job, and each later one surveys at most 16 of
the partitions known to be moving and 16 of those runs left work due in. Tests:
`optimize_gives_way_to_writers_between_its_steps` (a writer stepped beside it commits between its
steps; failed before with "ended without once giving way to a writer"),
`an_optimize_dropped_between_steps_leaves_the_rest_to_the_next` (failed before: "did all its work
in one step"), `optimize_inside_a_transaction_does_its_work_there`.

Measured on glove-100 (355,054 rows indexed, then 118,351 more inserted with statement maintenance
off, so 4.8-5.1 thousand jobs were due; 4 KB pages, synchronous NORMAL; a writer committing 20
one-row transactions a second and two readers at 128 probes beside it, on efficiency cores;
`ivfrq-load optimize`, `tmp/ann/optimize.sh`, `tmp/ann/load/optimize/*.wal2.*`, `*.mvcc2.*`). Under
WAL, in one transaction OPTIMIZE took 4.5 s and the writer's longest transaction 4.1 s, blocked for
the whole run (11.8 s of 17.8 s in an earlier run); in steps it took 17.3 s, the writer kept its pace
(347 transactions, none failed, p50 1.9 ms, p99 233 ms, longest 630 ms, against p99 17 ms and a
longest 22 ms before OPTIMIZE started), and the readers ran 1,597 queries a second against 878.
Under MVCC in one transaction OPTIMIZE took 10.8 s, and the `BEGIN CONCURRENT` writer beside it
committed once, 3,975 of its commits failing against OPTIMIZE's exclusive transaction, while the
readers ran 200 queries a second; in steps it took 7.8 s, the writer committed all 156 of its
transactions at its pace (p99 16 ms, longest 19.5 ms, none failed) and the readers ran 2,015. Every
commit emptied the other connections' page caches until a read kept the pages no other connection
wrote (below). On in-memory files (`IO=memory`), which take this host's disk out, that took stepped
OPTIMIZE under WAL from 6.9 and 8.5 s to 5.1 and 7.7 s, and the readers beside it from 1,365 and
1,201 queries a second to 1,898 and 1,789. The same runs showed a writer waiting on the write lock
while OPTIMIZE stepped: p99 532 ms to 1.7 s, longest 1.2 to 2.0 s, against 233 ms and 630 ms on the
file. A step took the lock again a moment after its commit, while a writer it had refused slept in
its busy handler. The WAL now counts the write locks its connections take and the times one is
refused (`Wal::write_lock_turns`), and a step that refused a writer leaves the lock until another
connection takes it, or for 100 ms, the default busy handler's longest sleep (`GIVE_WAY`). In
memory, three runs each: the writer's p99 24.3-25.9 ms and longest 34 ms, against p99 1.1-2.7 s and
longest 1.4-2.7 s, keeping its pace (126-134 transactions against 47-76), with OPTIMIZE taking
6.3-6.7 s against 5.0 and the readers beside it unchanged or faster.
`a_writer_refused_during_a_step_of_optimize_takes_the_lock_before_the_next_steps`: a writer on its
own thread inserting every 2 ms beside OPTIMIZE INDEX on another waited, before, while 68 steps
took the lock (340 and 352 with no pause between its inserts); now at most 2.

A statement's busy handler budget spanned all its waits, so a stepped OPTIMIZE INDEX gave up after a
few waits that each kept within its busy timeout. Each step's taking of the lock now ends its wait
for the busy handler (`ProgramState::lock_wait_over`), and the next wait starts afresh:
`a_stepped_optimize_waits_for_each_writer_within_its_busy_timeout` (a 100 ms timeout and a writer
holding the lock 30 ms at each of 8 steps; it failed at the third). Without the reset the fairness
test passes, but the run's own busy sleeps reach 100 ms and it takes 77 s instead of 4.

A step takes the partitions no other
transaction holds: up to 16 of those known to be moving and 16 of those left due, the latter
from where the last step stopped, passing over any another transaction holds or committed past
the step's snapshot; a step that finds all the work left held does none and waits, and after 64
such steps in a row the statement ends (`OptimizeProgress`). The first steps took the 16
lowest-numbered due partitions whatever held them, and a step that could do none of their jobs
ended the run: beside the writer, OPTIMIZE stopped after 0.8 s with 96,645 tail entries left
(`optimize_works_around_the_partitions_an_open_transaction_holds`, which left 30 tails unpacked
outside the 16 partitions a writer's open transaction held).

Under MVCC a maintenance job could write a partition's rows that another transaction had committed
past the job's snapshot and then lose at commit, first committer winning, failing the whole
transaction, a writer's included; the engine's write-time check sees only uncommitted versions. A
job now leaves for later a partition another open transaction has written, or one committed past
its snapshot (`IvfRqCursor::claimable`), by the index's record of its writers, which now also
records the generation row and keeps across a schema reload the highest position it knew
(`FLOORS`). `concurrent_writers_maintaining_the_index_all_commit`: three `BEGIN CONCURRENT`
writers inserting and deleting, their statements interleaved at random, all commit; with every
job claiming its partitions a writer's COMMIT failed with WriteWriteConflict, as the writer beside
the stepped OPTIMIZE did. A delete or update of a vector that maintenance moved after the writing
transaction's snapshot still failed with WriteWriteConflict until 2026-10-30 (below, "A write of an
`ivf_rq` index that meets maintenance leaves the vector to it").

Held rows of a partition no open writer has written now serve other snapshots
(`an_open_writer_keeps_from_the_held_rows_only_the_partitions_it_wrote`: a full-probe query beside
a one-row insert read 46 partitions' pages before, one after). `a_random_workload_keeps_every_answer_exact`
runs three connections' transactions and autocommit statements, inserting, deleting, updating,
rolling back and querying, beside OPTIMIZE INDEX in steps, interleaved at random under WAL and
MVCC, and checks each answer against the exact top ten of its snapshot and own writes (741 queries
over the two default seeds of each mode; IVF_RQ_WORKLOAD_SEEDS for more). With the held rows served
whatever writer was open, it failed at once.

### A read keeps the cached pages no other connection wrote
2026-10-29 `4ddd4353a`.
`core/storage/wal.rs` (`FramePages`, `ReadChanges`, `WalCoordination::read_changes`,
`WalFile::keep_unread_changes`), `core/storage/pager.rs` (`Pager::forget_changes`). A read that
found the WAL changed since its connection's last read cleared the connection's whole page cache,
so each commit or checkpoint of one connection cost every other connection all its cached pages.
The in-process WAL now records the page each frame holds, for the current generation and the last
8 that a restart retired (up to 2^18 frames), and a read evicts only the pages of the frames
between its connection's previous snapshot and its new one. Within a generation a committed frame
never changes and a checkpoint copies frames to the database file as they are, so those pages are
all that changed. A read still clears the whole cache when a frame in that range was never recorded
(frames recovered when the WAL was opened), when a generation it spans is no longer kept, when more
than 2^16 frames or more pages than the cache holds changed, when one of the pages is dirty, locked
or pinned, under the process-shared WAL, and after `reload_wal_after_external_restore`.

A checkpoint that restarts the WAL (`restart_log`, `try_restart_log_before_write`) moves its
connection's snapshot into the new generation without a read. MVCC's checkpoint calls the WAL's
checkpoint directly when it has nothing of its own to write, so after another connection's
checkpoint wrote and backfilled frames, the restarting connection's snapshot skipped frames it had
never read. While any change cleared the whole cache, the next change cleared those pages too (on
upstream `main` the randomized workload below passes 40 seeds in each mode); kept, they were read
stale ("inconsistent overflow chain observed during payload read"). The connection now keeps what
its restart skipped, and its next read drops those pages.

Tests, in `core/storage/pager.rs`: `a_read_keeps_the_pages_another_connection_did_not_write`
(a read after a one-row update kept 3 of 156 cached pages before; WAL and MVCC, within a generation
and across a restart), `a_restart_past_frames_a_connection_never_read_drops_their_pages` (read the
rows' old versions without the restart's changes kept), and
`reads_beside_writes_and_checkpoints_see_every_commit`: four connections writing rows up to two
pages long, reading in and out of transactions, rolling back and checkpointing in every mode,
interleaved at random with 64-page caches and checked against a model of each snapshot
(`READ_CHANGES_SEEDS`, default 2 a mode). It failed at MVCC seed 0 before the restart's changes
were kept, and fails with a retired generation's frames skipped. `fork/repro`'s `read_changes` runs
the same workload through the public API: 300 seeds in each mode pass.

Measured with `tmp/ann/ivfrq/src/bin/page-reads.rs`: two readers looking up random rows of a
200,000-row table (200-byte bodies, 20,000-page caches) beside a writer updating one row a
transaction, on efficiency cores 6-8, 20 s a run, against 9c6a6b3d5 with the two alternating. On
in-memory files (`IO=memory`), three rounds: under WAL, with the writer at 20 transactions a second
the readers ran 878,604-1,071,885 lookups a second against 618,256-738,833 (p99 2.4-2.9 µs against
3.9-4.7), and with the writer unthrottled 708,223-783,524 against 352,957-441,632, the writer
committing 12-25% more (1.32-1.49 million in 20 s against 1.06-1.30 million). On a file
(synchronous NORMAL), two rounds: 995,167 and 1,057,091 against 683,438 and 661,707 at 20 a second,
972,745 and 977,055 against 619,041 and 570,100 unthrottled. Under MVCC a commit writes no WAL
frames and only checkpoints change the readers' pages: 472,691 against 472,770 at 20 a second on
the file, and with the writer unthrottled and a checkpoint every 256 KB 141,169-243,115 against
159,846-212,798 in memory. On the file both builds now and then had reads stall 10-17 s under MVCC
with the writer unthrottled (4 runs of 22 with the change, 1 of 22 without), with slow fsyncs
beside them; on in-memory files neither build's longest read passed 19 ms.

In `ivfrq-load optimize` on in-memory files (glove-100, 355,054 rows indexed and 118,351 more
waiting, stepped), the readers ran 690 and 729 queries a second against 384 and 418 beside the
writer before OPTIMIZE, 1,789 and 1,898 against 1,201 and 1,365 during it (p99 6.3-6.9 ms against
13-14) and 3,162 and 3,304 against 2,725 and 2,859 after it under WAL; under MVCC 1,147 against 988
during it, OPTIMIZE taking 8.9 s against 10.1. On the file (710,108 rows and 236,702 more) the
disk decided the runs: the writer stalled 1.1 and 1.3 s in the build without the change and 17.6
and 22.5 s in the one with it, which no in-memory run showed.

### Checking an `ivf_rq` index against its table
2026-10-29 `22d5d2322`, `56decd6c8`, `f79b852dd`.
`ivf_rq::check(connection, index)` (`core/index_method/ivf_rq.rs`) reads an index whole, in the
connection's open transaction or one of its own, and reports each way it disagrees with its table
or with itself: a row with a vector and no row in the index, or the reverse; a code missing from
where the vector's row in the index says, or held there twice; a copy in a block that is neither
the live code nor on its vector's dead row; a dead row naming a block that holds no copy, or the
block that holds the live code; a code that is not its row's vector coded against the centroid
version it names, or that names a version the index does not hold; bookkeeping whose packed count
differs from its blocks, a block past the next sequence, a move range outside the blocks, and a
retired partition that holds anything. It holds a location per row and reads each indexed row's
vector once. `PRAGMA integrity_check` walks the backing tables' B-trees but skips index-method
indexes' contents. Tests: `the_check_finds_the_index_consistent_after_every_kind_of_write` (WAL and
MVCC, every live code coded again and compared), `the_check_reports_each_way_the_index_can_disagree_with_its_table`
(seven corruptions, each reported), and the check now ends `a_random_workload_keeps_every_answer_exact`
and `concurrent_writers_maintaining_the_index_all_commit`.

`fork/repro`'s `ivf_crash` kills a process writing an index (inserts, updates, deletes, alone and in
transactions it commits or rolls back, stepped OPTIMIZE INDEX, checkpoints; a real file, synchronous
NORMAL) at a random 0.2-2.5 s, reopens the database, and checks it: `PRAGMA integrity_check`,
`ivf_rq::check`, and the top ten of three queries, every candidate reranked, against a brute-force
scan. One thread: 200 of 200 rounds under WAL (to 351,834 rows) and 200 of 200 under MVCC (to
542,741 rows). Four writers beside a thread running OPTIMIZE INDEX: 60 of 60 under WAL (one round
went 2 s without a commit, one thread in an fsync of this host's disk) and 30 of 30 under MVCC
(13,016 commits, 609 transactions rolled back on conflicts, 327 OPTIMIZE runs).

Its workload found, before any kill, that a split rolled back with its transaction left the
partition it opened among those the next statements survey (`SharedState::due`), and the model,
reloaded without the split, held no centroid for it. `Model::state` called such a partition live,
so a statement found it live and empty, retired it and wrote past the end of the model's states:
the process panicked. A partition the model holds no centroid for is now retired as far as any job
goes, but for an untrained index's origin, and a retirement of one the model does not hold is an
error rather than a panic. `transactions_rolled_back_over_maintenance_leave_the_index_consistent`
(one connection, a sixth of its transactions rolled back) failed within 2 s before the fix.

The partitions table holds four kinds of rows under one key range, and only a partition's
bookkeeping filled all eight of its columns: a dead row held one value and a tail entry or block
two, the rest reading NULL, while the backing table declares every column NOT NULL. `PRAGMA
integrity_check` reported NULL values in every ivf_rq database with a tail, a block or a stale
copy. Every row now fills the columns its kind does not use with zeros; readers read by position
and are unchanged. `the_check_finds_the_index_consistent_after_every_kind_of_write` asserts
`PRAGMA integrity_check`; without the fill it fails with "NULL value in
__turso_internal_v_ivf_partitions.c1".

### A write of an `ivf_rq` index that meets maintenance leaves the vector to it
2026-10-30 `966402498`, `bbfcec22e` (the first's subject overclaims: see "Still open" below).
`core/index_method/ivf_rq.rs`. Maintenance moves and recodes vectors' rows beside open
transactions: each statement does the maintenance its writes made due, and OPTIMIZE INDEX runs in
steps on a connection of its own. Under MVCC a delete or update of a vector that maintenance had
moved after the writing transaction's snapshot, or was moving, failed with a write-write conflict,
though no other write touched the table's row.
`deletes_and_updates_of_vectors_maintenance_moved_after_the_snapshot_commit`: a `BEGIN CONCURRENT`
transaction holds its snapshot while OPTIMIZE INDEX packs and moves a 1,500-row backlog, then
deletes half the rows whose codes moved and updates the other half; before, its DELETE failed with
WriteWriteConflict.

A write of the index now reads its rowid's redo rows first, in a fourth backing table keyed by rowid
and sequence and usually empty. With none it writes as before, keeping each row's prior state. A
write that meets a row maintenance changed after the snapshot, or holds, undoes what it wrote and
writes the rowid's first redo row instead: a key nothing else writes, recording its transaction's
MVCC begin timestamp. While a rowid has redo rows, each write of it adds one more and writes nothing
else. A query passes over any code of such a rowid and reads its vector from the table, in every
round of a filtered query and with or without the rerank. The rowids are held across snapshots as a
partition's rows are, so a query over held partitions still reads no page.

Maintenance settles redo rows before any other job, reading a batch of them per job it may do. A
run that may do no job, like the first step of OPTIMIZE INDEX, still reads one batch, so OPTIMIZE
INDEX ends as soon as none is left. A settle removes the redo rows first, which claims the rowid
from any other run settling it, then kills the code and codes the table row's vector afresh where it
has one. A settle that meets a row another transaction holds is undone, and the run goes on without
that rowid. A run passes over its own transaction's redo rows, whose settle could only meet what
their writes met. A job that codes a vector afresh and finds its table row gone, or without a
vector, kills the code when the rowid has redo rows. A rebuild leaves such rowids to their settles.

Tests, each failing with its part of the change taken out (2026-10-30, nine mutations, each red and
then restored from HEAD by hash):
- the conflict test above;
- `rows_left_to_maintenance_are_answered_from_their_table_rows_until_settled`: WAL and MVCC; 120
  rows are left to maintenance, then deleted and updated while settles are held. Every answer is
  exact and the check is consistent; OPTIMIZE INDEX then settles them all. It fails when queries
  score these rows' codes, or do not read their vectors, or when OPTIMIZE INDEX ends with redo
  rows left;
- `a_filtered_query_reads_rows_left_to_maintenance_from_the_table`;
- `a_job_finding_a_row_left_to_maintenance_gone_kills_its_code`: a split's moves kill all forty;
- `a_rebuild_leaves_rows_with_redo_rows_to_their_settle`;
- `held_rows_serve_later_snapshots_until_a_commit_writes_their_partition` and the other seek-count
  tests, which failed while every query read the redo rows from pages;
- `optimize_settles_every_row_left_to_maintenance`: 1,100 rowids, more than one step settles; it
  fails when OPTIMIZE INDEX ends with redo rows left;
- `optimize_with_nothing_due_ends_at_its_first_step`: under WAL, beside a transaction holding the
  write lock. Before the first step read the redo rows, OPTIMIZE INDEX with nothing due took a
  second step, which waits for the write lock; that hung `a_random_workload_keeps_every_answer_exact`;
- the conflict test's one-row updates, made while their transaction has 750 rows left to maintenance,
  read at most 2 rows by key each. Without the pass over a transaction's own redo rows, one read
  4,162: its maintenance tried to settle them and met its own writes.

Still open, from `fork/repro`'s `ivf_crash`: four writers and OPTIMIZE INDEX under MVCC, 12 kill
rounds on seed 4242, each passing its checks. Before the fix: 700 conflicts in 11,475 commits.
After: 533 in 11,591, with 11 writes left to maintenance. All the remaining conflicts happen at the
COMMIT of an explicit `BEGIN CONCURRENT` transaction; none at a statement. In a 6-second traced run,
921 of 937 were the MVCC store refusing a concurrent commit while a non-CONCURRENT write transaction
held its exclusive lock (`CommitState::Initial`). Under MVCC an autocommit write, `BEGIN` or `BEGIN
IMMEDIATE` begins such a transaction (`TransactionMode::Write`), and eight in ten of the harness's
writes are autocommit. The other 16 lost first-committer validation on index rows that their own
statements' maintenance wrote (bookkeeping rows, the generation row, vectors' rows). They include two
transactions each inserting a row the other had inserted and not yet committed, which the store
detects only at commit. The next entry closes that class.

### ivf_rq's writes refuse rows other transactions wrote unseen, and its jobs wait only on jobs
2026-10-30 `c01598a1e` (a first check, apart from the write, replaced), `957f67921`, `858f2d542`.
`core/mvcc/database/mod.rs`, `core/mvcc/cursor.rs`, `core/storage/btree.rs`,
`core/index_method/ivf_rq.rs`. Under MVCC, the store checks an insert, a version over a row only the
B-tree holds, and a delete of such a row only at commit. A write of an index row inside a writer's
transaction could therefore lose there, failing the whole transaction, though a maintenance job
could have been undone or the writer's change left to maintenance. Two writers in `BEGIN
CONCURRENT` transactions beside OPTIMIZE INDEX lost 14 commits in 10 s this way. A cursor can now
ask for refusing writes (`CursorTrait::refuse_unseen_writes`): its inserts, versions over B-tree
rows, and deletes of table rows check, under the row's lock, for a write the transaction's snapshot
does not include (`chain_has_unseen_write`), and fail at once with WriteWriteConflict, writing
nothing (`MvStore::insert_row`, `insert_btree_resident_row`, `delete_row`). ivf_rq asks it of its
backing tables' cursors. Other tables keep their optimistic writes.
`test_refusing_writes_fail_at_the_write_over_unseen_writes` fails with the check disabled. The
first version of the check ran apart from the insert, so another thread could insert between the
two.

A job left for later every partition an open transaction had written. So under MVCC, writers that
kept transactions open on the same partitions kept maintenance off them for good: four writers in
`BEGIN CONCURRENT` transactions left an index of one partition and 425,658 tail entries after 12
rounds of `ivf_crash`. Writes of table rows write fresh index rows, plus the rows of a vector a job
moves, which the refusing writes now catch. The index's record of its writers therefore keeps apart
the partitions maintenance jobs wrote. A job now leaves for later only a partition another
transaction's job holds, or one where a job committed past its snapshot.
`maintenance_works_the_partitions_an_open_writer_wrote` (OPTIMIZE INDEX beside a writer whose open
transaction inserted 1,000 rows across the backlog's partitions) fails under the old rule. A redo
row's write is now entered in that record like any other. Without it, the conflict test's query
served redo rows held from before the writes and failed with "a vector's row names a row the table
lacks".

Measured with `ivf_crash` on `858f2d542`, 12 kill rounds each on seed 4242, every round passing its
checks:

| writers | commits | conflicts | left to maintenance | index at the end |
|---|---|---|---|---|
| 2, every write `BEGIN CONCURRENT` | 5,512 | 0 | 16 | 43,128 rows, 438 partitions |
| 4, every write `BEGIN CONCURRENT` | 19,790 | 1, an update statement | 70 | 150,278 rows, 29 partitions |
| 4, eight in ten autocommit | 13,432 | 712, 711 at COMMIT | 16 | 90,993 rows, 921 partitions |

Still open:
- **The exclusive lock.** The 711 commit conflicts are the MVCC store refusing a concurrent commit
  while an exclusive transaction holds its lock. An exclusive transaction skips conflict
  validation, so the store cannot let a concurrent commit land beside it. The rule for writers
  under MVCC is that every write runs in `BEGIN CONCURRENT`; `ivf_crash` takes `CONCURRENT=1` for
  that.
- **Splits starve under four contending writers: 29 partitions for 150,278 rows.** Every split
  writes the model's generation row, so splits serialize across the index, and a transaction whose
  snapshot predates the latest split cannot split at all. Over 8 traced seconds:
  - 19,344 splits were refused at their claims, 5,199 of them on the model alone;
  - 1,337 of the 1,390 started were undone at a write, on the generation and centroid rows and
    bookkeeping rows;
  - 51 finished.

  Two writers keep up. The generation row is gone ("ivf_rq splits of different partitions commit
  side by side" below), and a split of a partition every writer fills no longer starves ("An ivf_rq
  split marks its neighbours instead of claiming them").

### OPTIMIZE INDEX in steps rebuilds an `ivf_rq` index its rows outgrew
2026-10-30 `41f8dd865`. `core/index_method/ivf_rq.rs`. A rebuild was chosen only after a survey of
every partition, and only the first step of OPTIMIZE INDEX, which does no job, surveyed them all.
So a stepped OPTIMIZE INDEX split an outgrown index a partition at a time: run alone on a
four-writer `ivf_crash` database, it took 206 s to split 29 partitions holding 150,278 rows into
1,259. A run that surveyed every partition now records whether a rebuild is due; the next step
surveys them all again and makes it, and OPTIMIZE INDEX does not end while one is due. On another
such database (`ivf_crash optimize`), 10 partitions holding 87,891 rows became 1,185 in 1.2 s,
consistent and with no work left. `optimize_in_steps_rebuilds_an_index_its_rows_outgrew` (WAL and
MVCC, 3,000 rows in one partition's tail) fails without the change.

### No byte of a deleted row stays in the database's files
2026-10-30 `f29ba6995`, `39fc410c2`, `d99dec0ce`. `core/storage/btree.rs`, `core/storage/pager.rs`,
`core/translate/pragma.rs`, `core/translate/index.rs`. A deleted row's bytes used to stay in the
database file:
- in the freed range of its page, of which only the first four bytes became a freeblock header;
- in the gaps that defragmentation, a root split and a rebalance's trim left behind;
- on freed pages, which went to the freelist as they were;
- on new pages, which took a pooled buffer's old bytes to disk.

Erasure now holds at three levels:
- **Inside a page, always.** A b-tree page zeroes every byte a delete frees, fragments and
  freeblock headers included, every byte defragmentation or a root split moves a cell off, and the
  pointers a rebalance trims. A page made empty is zeroed whole, and a new page starts from zeroed
  memory. This costs a memset per freed cell, and every b-tree test exercises it: core 2,603 and
  FTS 2,643 pass.
- **Freed pages, under `PRAGMA secure_delete = ON`.** Each page the database frees is also zeroed
  and written back, as in SQLite. `FAST` and `OFF` leave freed pages as they were. The setting holds
  for every connection to the database, because a checkpoint writes the pages of other
  connections' deletes. It resets to `OFF` at open.
- **Index methods, through `REINDEX idx`.** It makes an `ivf_rq` or FTS index afresh, under WAL and
  MVCC: the backing tables are dropped with every page they held, created again and filled from the
  table's rows.
  - ivf_rq trains its centroids from the rows left. A centroid is a mean of the vectors it was
    trained over, and ivf_rq never removes centroid rows.
  - FTS indexes only the documents left.
  - Without REINDEX, a deleted vector's code stays in its block until compaction, as a deleted
    document stays in its segment until a merge.

`PRAGMA wal_checkpoint(TRUNCATE)` copies the WAL into the database file and truncates it to 0
bytes; under MVCC it first checkpoints the logical log into the b-trees and takes from the log every
frame committed before it began. Under passive checkpoints beside writers, the log keeps only the
commits that came after. So erasing rows takes:

```sql
PRAGMA secure_delete = ON;          -- once per open of the database
DELETE FROM memories WHERE ...;
REINDEX memories_embedding;         -- each ivf_rq and FTS index of the table
PRAGMA wal_checkpoint(TRUNCATE);
```

Under passive checkpoints the TRUNCATE reports Busy while another checkpoint runs, while a reader
of the WAL keeps its first pass from emptying it, or while a commit made before it keeps it from
freeing what came before it. Set a busy timeout: the statement then retries itself and keeps its
goal, where a new statement starts afresh.

`core/storage/erasure_tests.rs` reads the checkpointed files back. Its checker walks every b-tree,
overflow chain and freelist page of the database file and reports every nonzero byte that none of
them holds. The tests:
- `deleted_rows_leave_no_byte_in_the_files_after_a_truncating_checkpoint`: rows are inserted,
  rewritten and deleted, many checkpointed into the file before they go, and every value carries
  markers, one at a blob's end on an overflow page. Under WAL and MVCC the file holds no stray byte,
  the WAL and the log are empty, no dead marker is in any file and every live one is found.
- `freed_pages_keep_their_bytes_unless_secure_delete_is_on`: the same under `OFF` leaves dead
  markers and stray bytes on the freed pages and nowhere else, so the checker and the scan are seen
  to find them.
- `dropped_and_cleared_tables_leave_no_byte_in_the_file`,
  `pages_taken_off_the_freelist_leave_no_pointer_behind`, and the page test
  `defragmentation_keeps_no_copy_of_a_cell_it_moved` on the full and the fast path.
- `ivf_rq` and FTS `reindex_makes_the_index_afresh_and_leaves_none_of_the_old_one_in_the_file`:
  half the rows are deleted and the index made again. The new index shares no row with the old
  one and answers exactly from the rows left, and none of the old codes, dead rows, centroids or
  segment files is found in the file.

Fail-first: removing each zeroing in turn fails a test, 12 of 12, and so does a REINDEX that
refills without dropping. Two zeroings no test could fail turned out redundant, an emptied page
and a parent taking its child's cells being zero already, and went.

What the files hold is all this covers:
- the pending-byte page, past 1 GiB, and pointer-map pages rely on a new page starting zeroed,
  which no test reaches;
- memory, the page cache and MVCC's row versions until their garbage collection, holds a deleted
  row until it is reused;
- a file system or device that keeps freed blocks may keep what a truncation released.

### ivf_rq splits of different partitions commit side by side
2026-10-30 `5cf12df1a`, `78caecfae`, `28726660e`, `00a233dce`. `core/index_method/ivf_rq.rs`. Every
split, retirement and rebuild wrote the model's one generation row and claimed the whole model. So
splits ran one at a time across the index, and a transaction whose snapshot predated the last split
could not split at all.

Each partition now has a directory row in the centroids table, at key `-2 - p`: its current centroid
version, its state, and a tag drawn afresh by each change to either.
- **Sharing the model.** This process's writers record which partitions' parts of the model they
  change, with the commit position, kept in position order. A cursor whose snapshot no change
  separates from the model it holds reads nothing. Otherwise it reads the directory rows of the
  partitions in doubt alone and compares their tags. Where writers are not recorded here, another
  process or a whole-index change, it reads every directory row.
- **Claims.** A split claims its partition, its neighbours and the slot it opens, the lowest
  retired or vacant slot no other job holds, in one locked check. Concurrent splits of partitions
  apart therefore open different slots and write no row in common. A split rolled back leaves its
  slot vacant for the next.
- **WAL writers.** A WAL writer's model is never held, as rows it reads are not: its snapshot
  position counts its own frames, which a rollback reuses. REINDEX and DROP INDEX mark every
  partition changed, so a cursor holding the model they replace reads the whole directory.
- **Format.** An index of the earlier format is refused, with a hint to REINDEX it. `check()`
  verifies every directory row against its partition's bookkeeping and centroids.

Tests:
- `splits_of_partitions_apart_in_open_transactions_both_commit`: MVCC, two clusters, two open
  transactions each growing a partition of its own past the split bound. 62 partitions become 64;
  on the old code 63, the second split refused at its claim.
- `a_rolled_back_split_leaves_a_vacant_slot_a_later_split_takes`,
  `where_writers_are_not_recorded_a_query_reads_the_whole_directory` (multiprocess WAL),
  `an_index_of_the_earlier_format_is_refused_until_reindexed`,
  `a_held_model_doubts_the_partitions_changed_past_its_snapshot_and_no_others`, and `check()` cases
  for a deleted directory row and a wrong state.
- `a_query_refines_its_candidates_without_a_point_read` reads no directory row after a warm-up
  query.
- `a_model_read_while_a_reindex_ran_is_not_used_after_it` (WAL and MVCC): a reader reads the model
  while a REINDEX that leaves fewer partitions runs, then inserts 200 rows after it commits; the
  index checks consistent and a fresh connection finds the exact top ten.

Fail-first, one mutation at a time over the module's 69 tests. Each fails a test, 9 of 9:
- doubting none of the partitions changed past the snapshot: 28 tests, the held-model test among
  them;
- doubting none an open writer of this process changed: `concurrent_writers_maintaining_the_index_all_commit`
  and `transactions_rolled_back_over_maintenance_leave_the_index_consistent`;
- holding a WAL writer's model: `an_index_of_the_earlier_format_is_refused_until_reindexed` and the
  rollback test;
- a REINDEX that does not mark every partition changed: the REINDEX reader test, with 149 check
  findings, such as a directory row naming a centroid version the index does not hold;
- a split taking a slot another job holds: `a_rolled_back_split_leaves_a_vacant_slot_a_later_split_takes`
  and `splits_of_partitions_apart_in_open_transactions_both_commit`;
- a held model keeping a partition whose directory row is gone: the rollback test;
- a REINDEX keeping the shared model: `reindex_makes_the_index_afresh_and_leaves_none_of_the_old_one_in_the_file`;
- a partition changed again keeping its earlier place in the order, or the boundary taking changes
  at the snapshot itself: the held-model test.

Measured with `ivf_crash` on seed 4242, 12 kill rounds under four writers whose every write is
`BEGIN CONCURRENT`. The build before these commits (`b21b5637c`) and the build after them ran
alternately on the same eight cores, every round passing its checks:

| build | commits | OPTIMIZE INDEX runs finished | index at the end |
|---|---|---|---|
| before, first run | 19,210 | 3 | 147,193 rows, 5 partitions |
| after, first run | 14,810 | 31 | 111,756 rows, 2 partitions |
| before, second run | 17,234 | 8 | 135,199 rows, 4 partitions |
| after, second run | 15,035 | 29 | 114,477 rows, 1 partition |

The same run ended at 29 partitions on `858f2d542`, and an after-run not alternated with a before
ended at 85: a run whose first splits get through keeps splitting, and one whose don't stays at a
few partitions. With one writer, 30 rounds on seed 2, the index ended at 184,573 rows in 1,442
partitions before and 165,085 rows in 1,364 after, in 66 s and 63 s.

A split of a partition every writer fills still starved. It claimed its partition, its eight
nearest neighbours and a slot. In an index of a few partitions that is every partition, while each
writer's statement packs the tails it wrote. A trace of an after-run counted, per round of 0.5 to
2.3 s:
- split claims refused 12 to 3,571 times against 0 to 2 made, none for want of a slot and none
  undone at a write;
- pack claims made 74 to 621 times and refused 851 to 25,344 times.

OPTIMIZE INDEX tried the rebuild such an index is due, and its claim was refused 18 to 869 times a
round. The three rebuilds it claimed in one round each failed at a write, which ended the
statement. The next two entries close both.

### Waits sleep instead of spinning
2026-10-30 `8059358cb`, `c595227a6`, `aa59e4aa3`, `00a233dce`. `core/statement.rs`,
`core/incremental_blob.rs`, `core/vdbe/mod.rs`, `core/vdbe/execute.rs`,
`core/index_method/optimize_steps.rs`, `sdk-kit/src/rsapi.rs`, `sdk-kit/src/timer.rs`. Three waits
each spun a core for as long as they lasted:
- **Busy waits in blocking runners.** A statement waiting on a lock returns `StepResult::Sleep` with
  its busy handler's next delay. `run_ignore_rows`, `run_collect_rows`, `run_with_row_callback`,
  `run_one_step_blocking`, the incremental blob's step and the SDK kit in sync mode answered it by
  stepping the IO, which returns at once on a backend with nothing pending. The JDBC driver steps
  through `run_one_step_blocking`. Against a held write lock and a 60 ms busy timeout,
  `run_ignore_rows` stepped the IO 1,244,558 times. They now sleep the delay through `IO::sleep`.
  Every backend's monotonic clock is real time, simulators included.
- **Busy waits in async mode.** The engine wakes the caller as it returns Sleep, so the `turso`
  crate's statement was polled again at once: 28,337 polls over a 60 ms wait. The SDK kit now
  records when the delay ends, has one timer thread wake the waker then, and answers steps before
  that with IO without reaching the engine. A caller that passes no waker still steps again when it
  chooses.
- **OPTIMIZE INDEX giving way.** Under WAL a step that refused a writer the write lock leaves the
  lock to it until the writer takes it or 100 ms pass. The step waited by yielding: beside a writer
  inserting at a steady rate it yielded 760,514 times in 5 s. It now asks for a 1 ms sleep between
  looks, which the statement reports as Sleep (`ProgramStep::Sleep`).

Tests: `blocking_runners_sleep_out_a_busy_wait`, `blob_open_sleeps_out_a_busy_wait`, the SDK kit's
`sync_steps_sleep_out_a_busy_wait` and the `turso` crate's
`a_busy_wait_is_polled_again_when_each_delay_ends` each bound the steps or polls of a 60 ms busy
wait. Every OPTIMIZE INDEX in the ivf_rq tests runs through a helper that fails past ten thousand
yields, and `a_writer_refused_during_a_step_of_optimize_takes_the_lock_before_the_next_steps`
fails there with the yielding wait. Fail-first: putting back each of the six waits fails its test,
6 of 6 (the counts above, and 888,173 steps in the SDK kit's sync mode).

Not covered: a caller of the C API in async mode passes no waker, so it still steps again when it
chooses; the other language bindings' own step loops are as upstream left them.

### An ivf_rq split marks its neighbours instead of claiming them
2026-10-30 `38846b4f2`. `core/index_method/ivf_rq.rs`. A split claimed its partition, its eight
nearest neighbours and the slot it opens, which in an index of a few partitions is every partition,
so under four writers a split of a partition every writer filled starved ("ivf_rq splits of
different partitions commit side by side" above).

A split now claims its partition and the slot it opens alone. On each neighbour it writes a mark: a
row of the partitions table of the bookkeeping kind, numbered past the bookkeeping by one plus the
slot it opened. A mark is entered as a write of the index and as no job's claim, and no other job
writes it, so it holds the neighbour against no other transaction's job. The neighbour's next job
of any kind takes its marks up:
- a live partition checks all its blocks again from the first, as the split's write of its
  bookkeeping used to make it;
- one with no block goes straight to the job's plan;
- the job's writes remove the marks.

Surveys, `settled()`, the due list OPTIMIZE INDEX steps over, the rebuild's clear, the query probe
and `check()` tell a mark from bookkeeping, and `check()` counts them.

Tests:
- `a_split_beside_a_neighbour_another_transaction_holds_is_made_and_marks_it` (MVCC): one
  transaction holds a pack of the partition nearest one that another transaction grows past the
  split bound. The split commits, and the grower takes up every mark but the held neighbour's. The
  holder then packs the neighbour again, and OPTIMIZE INDEX leaves no mark. The grown partition is
  found by inserting the grower's rows in a probe transaction that rolls back, as the index ranks
  partitions by its loss, not by distance.
- `a_query_of_a_marked_partition_scores_the_blocks_held_for_it`: a query whose answer is twelve rows
  packed in one partition, around a mark planted beside a row coded into the partition's tail.
- `optimize_in_steps_rebuilds_an_index_its_rows_outgrew` plants a mark on a slot the rebuild leaves
  vacant and finds it gone.

Fail-first, one mutation at a time over the module's tests, 7 of 7:
- a split claiming its neighbours again: the held-neighbour test;
- a job taking up a mark without checking the partition's blocks again: 4 tests,
  `vectors_live_in_their_nearest_partition` among them;
- marks left by the job that took them up: OPTIMIZE INDEX never settled, and 18 tests had failed
  when the run reached its 900 s bound;
- a survey reading a mark as bookkeeping: 17 tests;
- a rebuild keeping the marks: the rebuild test;
- a mark entered as a job's claim on the neighbour: the held-neighbour test, the holder's pack
  refused;
- a query reading a mark as bookkeeping: the marked-partition query test. Two weaker forms of it had
  passed under this mutation. Ranked by dot product, the top ten at a partition's members need not
  lie in that partition. And a mark planted through SQL is no write the index's record sees, so the
  query's held rows of the partition went on serving it, and it never read the mark.

Measured with `ivf_crash` (`THREADS=4 CONCURRENT=1`, MVCC, 12 kill rounds on seed 4242), three
builds alternating on the same eight cores, every round passing its checks: the build before this
entry (`4cff6126d`), this commit's, and the next entry's (`01435fa75`).

| build | index at the end | commits | failures (conflicts) | OPTIMIZE INDEX runs finished |
|---|---|---|---|---|
| before, run 1 | 137,217 rows, 49 partitions | 18,024 | 10 (3) | 8 |
| before, run 2 | 92,490 rows, 33 partitions | 12,155 | 9 (4) | 7 |
| before, run 3 | 98,426 rows, 29 partitions | 12,813 | 6 (3) | 8 |
| marks, run 1 | 131,403 rows, 1,167 partitions | 17,030 | 6 (2) | 44 |
| marks, run 2 | 135,181 rows, 1,188 partitions | 17,816 | 7 (3) | 55 |
| marks, run 3 | 81,853 rows, 822 partitions | 10,843 | 3 (2) | 48 |
| marks and the rebuild entry, run 1 | 135,407 rows, 1,186 partitions | 17,251 | 0 (0) | 94 |
| marks and the rebuild entry, run 2 | 183,442 rows, 1,376 partitions | 23,728 | 0 (0) | 133 |
| marks and the rebuild entry, run 3 | 228,732 rows, 1,532 partitions | 28,866 | 1 (1) | 169 |

The partitions trained for those row counts are about 1,140 to 1,910. The failures past the
conflicts are OPTIMIZE INDEX statements failing, its rebuilds at a write, which the next entry
closes. With one writer, 30 rounds on seed 2, the index ended at 152,542 rows in 1,296 partitions
before and at 134,689 rows in 1,232 partitions with both entries, in 66 s and 64 s.

### An ivf_rq rebuild under MVCC claims only where no other write is unseen
2026-10-30 `a6bcf1e1b`, `01435fa75`. `core/index_method/ivf_rq.rs`,
`core/index_method/optimize_steps.rs`, `core/index_method/mod.rs`, `fork/repro`. A rebuild rewrites
every row of the index with no undo, so under MVCC it failed at the first row another transaction
had written, open or committed past its snapshot. That failed OPTIMIZE INDEX, and a statement
inserting `BULK_AT` rows or more, whose own maintenance can choose the rebuild, failed with it.
Under four writers every rebuild claimed met such a write (the entry before last).

A rebuild is now claimed only where no other transaction has written the index unseen, redo rows
aside (`Writes::quiet`): none open has written another row of it or holds a job, and none that ended
wrote one past the rebuild's snapshot. The claim marks its writer rebuilding in the same lock. Until
the rebuild is made, every other write of the index is left to redo, as one that meets a job is,
and no other maintenance starts.
- **OPTIMIZE INDEX** announces a rebuild due that it cannot claim. The announcement holds the other
  writes to redo too, and the next steps, each under a fresh snapshot, wait for the transactions
  that wrote the index to end. A waiting step does nothing else, reports
  `OptimizeProgress::Draining`, sleeps 1 ms before the next, and counts among the 64 steps that may
  wait in a row. The rebuild comes before the redo rows, which keep coming.
- **An announcement lapses** when its connection is gone or when the steps have not renewed it
  within `REBUILD_LEASE` (1 s), so an OPTIMIZE INDEX left unstepped holds no writer longer.
- **A statement's maintenance** leaves a rebuild it cannot claim to OPTIMIZE INDEX.

Tests, all under MVCC:
- `a_rebuild_beside_an_open_write_waits_for_it_and_is_made`: OPTIMIZE INDEX, stepped beside an open
  transaction that rewrote a row, announces the rebuild and sleeps between steps. A write made
  meanwhile is left to redo. A writer of twenty rows left open starts no maintenance, and once the
  first transaction commits the rebuild is made beside it.
- `optimize_waiting_on_an_open_write_ends_with_the_rebuild_due`: past the steps waiting in a row
  OPTIMIZE INDEX ends, and writes are coded again.
- `an_announced_rebuild_left_unstepped_lapses`: past its lease, and once its statement is dropped.
- `a_statement_beside_an_open_write_leaves_its_rebuild_to_optimize`: an INSERT of 2,000 rows into
  an outgrown index beside an open write commits and leaves the rebuild to OPTIMIZE INDEX. In a
  quiet index it makes the rebuild, and a write after it is coded while its transaction is open.

Fail-first, one mutation at a time over the `index_method` tests, 10 of 10:
- the claim taking no account of others' unseen writes, the rebuild as it was: all four, with
  `WriteWriteConflict` from OPTIMIZE INDEX and from the INSERT;
- writes beside an announced rebuild made, not left to redo: 2 tests;
- other transactions' maintenance going on beside it: the open writer's settle of another's redo
  row kept the rebuild from being claimed;
- a rebuild made holding writes to redo until its transaction ends: the statement test's quiet
  case;
- an announcement that never lapses: the lapse test;
- one that outlives its connection: 2 tests;
- OPTIMIZE INDEX announcing none: 2 tests;
- waiting steps not counted among those waiting in a row: OPTIMIZE INDEX never ended;
- waiting steps not sleeping: 3 tests;
- the redo rows before a rebuild due: a waiting step settled a redo row.

The first test found that a statement reading no database stranded the implicit MVCC transaction
a sibling opened beside it: a read made on the test's connection while that connection's OPTIMIZE
INDEX ran opened the transaction, and after both had ended the connection's reads still saw that
read's snapshot, without the rebuild. It stepped OPTIMIZE INDEX on a connection of its own until
`763113a6a` fixed that ("Under MVCC a statement that reads no database stranded its connection's
transaction", under Fixes).

Measured in the table of the entry before (`ivf_crash` now reports `optimize_failures` and
`rebuilds`): in each of the three four-writer runs with both entries, no OPTIMIZE INDEX failed and
the one rebuild due was made in the first round, when the index first held about 20,000 rows. In
that round 1,298 to 1,572 writes were left to redo while the rebuild waited and was made, against 0
to 9 in each later round.

## Taken upstream

### An index method's LIMIT applied before an aggregate or DISTINCT
Ours 2026-10-28 `e6fd95447`; upstream took it the same day as `ad8e4895f`, the same patch.
`index_method::fts::tests::fts_limit_applies_after_aggregates_and_distinct` came with it.

### A value an index method provides failed the collation lookup
Ours 2026-10-28 `917f0acbd`; upstream took it the same day as `c6f1cafdd`, the same patch.
`index_method::fts::tests::fts_score_from_the_index_has_no_column_collation` came with it.

## Considered and not taken

### Parking a commit that waits on a commit dependency
Instead of returning an explicit yield, park the waiting commit on a completion that the
transaction resolving its last dependency completes. On a multi-threaded host this cut user CPU
36% in our workload. It breaks hosts that run every connection cooperatively on one thread: the
writer being waited on can only run once the host regains control, which an explicit yield gives
it and a parked wait does not. Upstream pins this down in
`connect_async_yields_instead_of_spinning_on_preparing_commit_dependency`, which hangs with the
change.

### `PRAGMA wal_autocheckpoint` intercept
Our 0.8.3 copy intercepted `wal_autocheckpoint` to turn the WAL auto-checkpoint off per
connection. Upstream `main` implements the pragma.

### SQLite's auto-checkpoint trigger
Under WAL a commit runs the passive auto-checkpoint once more than the threshold (1,000 frames) is
not yet backfilled (`WalFile::should_checkpoint`); SQLite runs it after every commit once the WAL
holds the threshold. A reader on an older snapshot stops a passive checkpoint short of the WAL's
end, and the WAL restarts only after a whole backfill, so beside overlapping readers Turso's WAL
grows without bound. `fork/repro`'s `wal_growth` (one writer inserting a 200-byte row a
transaction, readers counting the table, 6 s, synchronous NORMAL) restarted the WAL 0-4 times and
grew it to 74,693-253,953 frames (0.3-1.0 GB); with SQLite's trigger the same build restarted it
9-26 times and held it to 1,068-1,102 frames. The trigger costs the writer a checkpoint and its two
fsyncs after nearly every commit while readers hold the WAL: 23,842 commits against 74,601 beside
one reader, 9,051 against 219,790 beside four. Not taken. Four writers and no reader starve both
engines: neither restarted in most 6 s runs, and SQLite 3.51.2 reached 341,741-576,519 frames.
