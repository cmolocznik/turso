# This fork

Upstream Turso (`tursodatabase/turso`, `main`) with the changes below on top, on two branches:

- `main` holds the fixes, which are headed upstream: one commit each with its test, listed by
  `git log upstream/main..main`.
- `feat/additions` is `main` plus the additions, listed by `git log main..feat/additions`.
- `fix/set-waker-lost-wakeup`, `fix/mvcc-checkpoint-trigger`, `fix/mvcc-exact-rowid-seek`,
  `fix/index-method-limit-before-aggregates`, `fix/index-method-column-collation` and
  `fix/btree-overflow-copy` each hold one of `main`'s fixes alone on `upstream/main`.

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

### An overflow chain was copied into its payload a byte at a time
`core/storage/btree.rs`, `process_overflow_read`. Each page of a record's overflow chain was
appended through `try_extend` over a byte iterator, which on stable is `Vec::extend` over
`Copied<slice::Iter<u8>>` and compiled to a loop writing one byte at a time, the payload regrowing
as pages came in: ivf_rq queries over 5.6 KB block rows spent about 14% of their time in it. The
payload is now reserved at its full size when the chain is first read, and each page is copied in
with `extend_from_slice` after a fallible `try_reserve`, as `copy_payload_range` does. At 256 probes
over glove-100 the blocks' reads took 3,535 µs a query instead of 4,524. No behaviour changes, so
no new test; every read of an overflowing record goes through it. Its PR waits for Clay's go.

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
ScaNN runs 3,121 / 1,365 and the RaBitQ library at 7 bits 2,808 / 1,272 on the same core: ivf_rq is
1.7x / 1.45x behind the library, from 4.8x / 3.8x. The index is 181 bytes a vector at 100
dimensions, the library's 140.

Vectors are padded to a multiple of 32 rather than to a power of two, rotated as the library's
FhtKac rotator does where the width is not one: four rounds of sign flips, each followed by a
transform of the largest power-of-two span, leading and trailing in turn, and a step of a Kac walk.
A 768-dimensional code is 692 bytes, not 916. On a 200,000-vector slice of Cohere at 768
dimensions the index is 213 MB instead of 264, recall at a refine of two is unchanged to the third
place, and queries run 8 to 17% faster at each setting: 1,752-1,796 a second at 48 probes (0.908)
against 1,579-1,671, 1,302-1,340 at 96 (0.955) against 1,176-1,225.
`a_rotation_pads_to_a_multiple_of_32_and_keeps_lengths_and_products`. With the changes after it,
the Cohere slice runs 3,019 / 1,779 queries a second at recall@10 0.90 / 0.95 on one core, against
3,774 / 2,785 for the RaBitQ library at 7 bits and 1,942 / 1,061 for ScaNN; at 768 dimensions the
bfloat16 ranking alone was worth 24% at 0.90, the ranking having read 6 MB of f32 centroids a query.

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
balancing stays. Still owed: the rest of the gap to the library at one query at a time (1.25x to
1.7x), and its higher recall at the same probes and partition count (on Cohere at 48 probes 0.931
against 0.916 with splits only past the bound), unexplained; the build takes 26 s on glove-100
over the laptop's cores.

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
