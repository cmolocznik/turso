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
the stepped OPTIMIZE did. Still open under MVCC: a delete or update of a vector that maintenance
moved after the deleting transaction's snapshot fails with WriteWriteConflict, as any write to a
row a concurrent transaction changed does; `BEGIN CONCURRENT` clients retry.

Held rows of a partition no open writer has written now serve other snapshots
(`an_open_writer_keeps_from_the_held_rows_only_the_partitions_it_wrote`: a full-probe query beside
a one-row insert read 46 partitions' pages before, one after). `a_random_workload_keeps_every_answer_exact`
runs three connections' transactions and autocommit statements, inserting, deleting, updating,
rolling back and querying, beside OPTIMIZE INDEX in steps, interleaved at random under WAL and
MVCC, and checks each answer against the exact top ten of its snapshot and own writes (741 queries
over the two default seeds of each mode; IVF_RQ_WORKLOAD_SEEDS for more). With the held rows served
whatever writer was open, it failed at once.

### A read keeps the cached pages no other connection wrote
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
were kept, and fails with a retired generation's frames skipped. `fork-repro`'s `read_changes` runs
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
