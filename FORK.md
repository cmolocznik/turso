# This fork

Upstream Turso (`tursodatabase/turso`, `main`) with the changes below on top, on two branches:

- `main` holds the fixes, which are headed upstream: one commit each with its test, listed by
  `git log upstream/main..main`.
- `feat/additions` is `main` plus the additions, listed by `git log main..feat/additions`.
- `fix/set-waker-lost-wakeup`, `fix/mvcc-checkpoint-trigger`, `fix/mvcc-exact-rowid-seek`,
  `fix/index-method-limit-before-aggregates` and `fix/index-method-column-collation` each hold one
  of `main`'s fixes alone on `upstream/main`.

This file is only on `feat/additions`, so `main`'s commits can go upstream as they are. Keep it in
step with the two logs: a change lands here when it lands, with what it fixes and what proves it.

## Fixes

### Lost wakeup in `Completion::set_waker`
`core/io/completions.rs`. `set_waker` tested `finished()` and then stored the waker under the
context lock, so a completion finished on another thread between the two woke no one, and the task
that registered was never polled again: a hang that happens now and then. The waker is now stored
first and the completion tested after.
Test: `io::completions::tests::shuttle_set_waker_racing_complete_always_wakes`, run by
`make test-shuttle`. It runs every interleaving of `set_waker` against `complete` on another
thread; on upstream `main` it finds the one that loses the wakeup. Shuttle cannot see the std
`OnceLock` holding a completion's result, so under `cfg(shuttle)` `Context::set_waker` yields
before its store.

### The checkpoint trigger counts only uncheckpointed log bytes
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
upstream `main` the same A/B shows no gain: 15 idle runs each of `fork-repro`'s `mvcc_inline` (2
writers, 4,000 commits, 256 KB threshold) put the writer commit p99 median at 13.5 ms without the
fix and 14.1 ms with it.

### An exact rowid seek walked every shadowed B-tree row after its target
`core/mvcc/cursor.rs`. An eq-only `TableRowId` seek that reached a B-tree row shadowed by a newer
MVCC version advanced to the next B-tree row, and kept going while rows were shadowed. Over a
checkpointed table whose rows were all updated, point lookups went quadratic: on upstream `main`
(`e9a8b0d25`), 2,000, 4,000 and 8,000 lookups took 222, 920 and 3,571 ms, against 16, 31 and 66 ms
with the fix (`fork-repro`'s `shadow_seek`). An exact seek has at most one B-tree match, so it now
stops when that row is shadowed or the row reached is not its target; the MVCC side still supplies
the newer version.
Test: `mvcc::database::tests::test_exact_rowid_seek_over_shadowed_btree_rows`, which counts each
exact seek's B-tree steps through the cursor's yield hook: the seek for row 1 takes 155 on upstream
`main` and 2 with the fix.

### An index method's LIMIT applied before an aggregate or DISTINCT
`core/translate/optimizer/mod.rs`. A pattern with LIMIT applies the query's LIMIT to the rows the
index gives out, and the single-table planner gave one to any query whose WHERE and LIMIT matched
it, so an aggregate or DISTINCT ran over the first LIMIT matches. On upstream `main`
(`818977eb4`), over 20 FTS matches, `SELECT count(*) ... WHERE fts_match(...) LIMIT 5` returned 5,
and `SELECT DISTINCT cat ... WHERE fts_match(...) LIMIT 2` returned one of the two categories. A
pattern with LIMIT now serves only a query whose rows reach the LIMIT as they leave the WHERE
clause. A window function was never affected: its query reads the table through a subquery
without the LIMIT.
Test: `index_method::fts::tests::fts_limit_applies_after_aggregates_and_distinct`.

### A value an index method provides failed the collation lookup
`core/translate/collate.rs`, `core/translate/plan.rs`. The planner puts a value an index method
provides, such as an FTS score, in place of the expression it computes, as a column numbered past
the table's own. Every collation decision looks a referenced column up in its table
(`referenced_column_collation`), which has no such column, so the statement failed: on upstream
`main` (`818977eb4`), `SELECT DISTINCT fts_score(...) ... WHERE fts_match(...)`, a UNION of two such
SELECTs and an IN subquery over the score each failed to prepare with "column not found". The value
now has no column collation, as the expression translator already treats it.
Test: `index_method::fts::tests::fts_score_from_the_index_has_no_column_collation`.

## Additions

### Background checkpoint (`PRAGMA mvcc_background_checkpoint`)
`core/mvcc/database/background_checkpoint.rs`, wired in `core/mvcc/database/mod.rs`; the pragma
in `sqlite/parser/src/ast.rs`, `core/pragma.rs`, `core/translate/pragma.rs`, `core/connection.rs`.
On, the commit that crosses the MVCC checkpoint threshold hands the checkpoint to a per-store
thread and returns, so no writer pays for one. The thread holds the database by a weak reference,
opens a connection per checkpoint, runs the same passive checkpoint, and stops when its store is
dropped. After a checkpoint that had to keep the log, it checkpoints again once the log has stayed
the same size for 100 ms, and that one truncates it.
Off by default, since the thread steps the IO backend beside the host's own threads. Encrypted
databases and ones read through an external page codec keep their inline checkpoint, so key
material stays on the connections given it. Under writes that never pause the log still grows;
bounding that needs log rotation.
Tests: `test_background_checkpoint_runs_off_the_commit_and_truncates_when_writes_pause`,
`test_background_checkpoint_is_off_by_default`, `test_background_checkpoint_thread_exits_with_its_database`.
mt1, this fork, the bench's `mvcc_control` (2 writers, a maintainer rewriting 64-row batches, a
reader; 256 KB threshold), 20 runs each way, all passing: writer commit p99 19.9 ms inline → 5.3
ms with the thread (medians of the runs; worst run 25.3 → 6.9 ms), worst commit 28.7 → 9.1 ms
(medians), and the log 0 bytes after writes pause in all 20 runs against 160 KB left inline. Every
commit over 100 ms is matched by an fsync of about its length in the same run. `ivf_mvcc`
(8 writers × 4,000 vectors of 128 dimensions under maintenance) passed its 520 checks in all 7
runs, 97-110 s each with the thread against 137-211 s inline.

### Logical-log fsync stats
`core/mvcc/persistent_storage/mod.rs`. `mvcc::persistent_storage::logical_log_sync_stats()`
returns the count, total and longest µs of logical-log fsyncs. A durable commit cannot be faster
than its fsync, so a commit no slower than the longest fsync waited on the disk.

### The `ivf_rq` index method
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
