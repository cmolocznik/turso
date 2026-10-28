# This fork

Upstream Turso (`tursodatabase/turso`, `main`) with the changes below on top, on two branches:

- `main` holds the fixes, which are headed upstream: one commit each with its test, listed by
  `git log upstream/main..main`.
- `feat/additions` is `main` plus the additions, listed by `git log main..feat/additions`.
- `fix/set-waker-lost-wakeup`, `fix/mvcc-checkpoint-trigger` and `fix/mvcc-exact-rowid-seek` each
  hold one of `main`'s fixes alone on `upstream/main`.

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
On 0.8.3, 2 writers at a 256 KB threshold: inline writer commit p99 165-194 ms → 9.4-12.8 ms.

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
