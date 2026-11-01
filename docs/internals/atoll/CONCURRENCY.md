# Atoll concurrency, from first principles

A proposal replacing every concurrency mechanism in Atoll. Nothing here is built yet. Once
approved, it replaces DESIGN.md §9.2-§9.4 (latching and replacement), the latching part of §13,
§18, and the waits of §11.3.

## 1. What must hold

1. **No reader waits for a writer**, at the page level or the row level. A read waits only for IO.
2. **No writer waits for a reader.**
3. **Nothing blocks or spins.** A thread never parks in Atoll and never loops waiting for another
   thread. Turso runs connections cooperatively, on any thread and on one: a thread that waits
   for another connection on the same thread waits forever.
4. **A panic releases everything its thread held.**
5. **No action is taken on unvalidated data.** Bytes read without a latch decide nothing (no IO,
   no write, no allocation, no free) until they are validated.
6. **Every identity is reused only when no one can still hold it.** Frames, page ids, memory
   batches, temporary pages, version index entries, retired pages and commit log entries.
7. **Every mechanism is proved by deterministic interleavings** before anything is built on it.

## 2. Why the current design fails

Five bugs in two days of stress, all with one shape: a thread acts on an object whose identity
changed under it, and the path that acted did not re-check it.

| Bug | Identity that changed | Principle missing |
|---|---|---|
| A drain moved another class's pages | a memory batch's class | 6: a batch went to another class while a thread still held its old class |
| A recovered value was lost | a frame's page (same drain) | 6 |
| A new page collided with a stale read-in | a page id, freed and handed out again | 5: IO on a child id read through an unvalidated parent; 6: the id was reused at once |
| A panic hung the checkpointer | a frame latched before any guard existed | 3 and 4: spin waits, latches outside guards |
| A backward scan livelocks | (under investigation) | 7: found by stress, not by construction |

The mechanisms are standard ones (LeanStore's optimistic latches, frame hints, size classes).
What is missing is a small set of rules that every path follows, and a way to prove them.

## 3. Every shared object has one discipline

Each object belongs to exactly one of four classes, named in its type's documentation:

1. **Immutable once published.** Log records, page copies on disk, a committed version,
   out-of-line value pages once linked, commit log entries. Read freely; never changed; reclaimed
   through epochs (section 4).
2. **One writer, optimistic readers.** B-tree nodes, page table and free-space pages, frame
   headers. A writer holds the object's exclusive latch; readers read without a latch and
   validate its version afterwards. There is no shared latch mode at all.
3. **Owned by one context at a time.** A transaction's state, an operation in progress, a
   writeback, a recovery partition, a slot's version index while its transaction writes. Handed
   over explicitly; no synchronization while owned.
4. **Short critical sections.** A mutex around a few instructions of bookkeeping (a free list, a
   waiter list). Never held across IO, a yield, a callback or another lock, except in the one
   global order section 8 lists. Each is named in that list.

Anything that fits none of these is a design error.

## 4. Epochs: when an identity may be reused

- **Participants.** Every connection stepping Atoll is a participant. It announces the global
  epoch when it enters a step (one call into Atoll that returns `Done` or `IO`) and clears the
  announcement when it leaves. Two atomic stores per step.
- **Retiring.** Freeing a page id, a temporary page, a version index entry or a retired page
  puts it on a limbo list stamped with the current epoch.
- **Reclaiming.** An object retired at epoch `e` is reused once every participant's announcement
  is above `e`, or clear. The epoch advances when the limbo lists hold enough to reclaim; the
  check is one pass over the participants.
- **What it guarantees.** Within one step, nothing a participant read can be handed to another
  page, class or transaction. That alone rules out the drain, lost-value and new-page bugs above.
- **Across steps,** a participant holds only names with versions (page id, frame, version), and
  validates all three before use. A frame's version only rises, so a frame that held another page
  in between never validates. A cursor kept open for an hour pins nothing.
- **Bounded:** a step never blocks, so a slow participant delays reclamation by at most one step.
- **Frames and memory batches are checked, not retired.** A frame's identity is its latch
  version, which only rises, and every use validates it. A batch is used only through its frames,
  and every action on a frame drawn from a batch (an eviction, a drain) checks under the frame's
  latch that the page it holds is of the class it was drawn for. A stale name acts on nothing, so
  neither needs an epoch. Retiring batches was built and measured: the claim that empties a batch
  cannot use it before its own step ends, and another class's claim takes it back first, so two
  classes passed one batch back and forth forever (two single-threaded crash tests hung).

## 5. Latches

- **One kind.** A 64-bit word: version and an exclusive bit. Taken with one compare-and-swap;
  released by a guard, which exists from the instruction that took it, so a panic releases it.
- **Busy means yield.** A taker that finds it held returns a yield to its caller. No spin anywhere.
- **No shared mode.** Every read is optimistic: copy, then validate. That covers B-tree nodes,
  out-of-line value pages (immutable once linked, so a failed validation means the page moved or
  was evicted: read it again by name) and writeback copies (copy optimistically; a write under
  way makes the copy fail validation, and it is copied again).
- **Exclusive holds are for changing a page and for changing what a frame holds** (load, evict,
  free). Never across IO.

## 6. The buffer manager

- **Translation.** The hash table maps page ids to frames; frame hints are checked by page id and
  version on use. Unchanged.
- **Validate before IO.** A miss on a page reached through another page starts IO only after the
  page it came from is validated. A descent that finds a child missing validates the parent, then
  reads the child in.
- **Size classes.** Memory moves between classes only as whole batches, and a batch is drained
  only by evicting its pages; live pages are never relocated. A dirty page is written out: its
  frame is held until the write lands, and whoever wants the page waits on that write, so a page
  that keeps changing still leaves.
  An emptied batch goes at once to the class it was drained for (§4).
- **Allocation.** A page id is reused only after an epoch (section 4), so a reader still holding
  the old id can only find it free. A freed number joins the waiting numbers before it reads as
  free in the page table, so no allocation takes it in between.
- **Ids held across steps.** A reader that keeps a page id across steps (a cursor reading a large
  value, a lookup) reads the page in only after checking, in the same step, that the page that
  names it still does (`fix_optimistic_held`). An id freed after the check is not reused before
  that step ends, so the read finds it free; one freed before fails the check. No copy of a freed
  page stays in memory to meet a new page under its number.
- **Metadata pages** (page table, free space) follow the same discipline as tree nodes: versioned
  latch, optimistic reads. A change takes the latches of every page it changes, in id order,
  without waiting, or none of them, and yields. A page write's completion only records that it
  landed: the remap that follows latches metadata pages, so the writeback does it when next
  stepped, and a writeback stays queued until every write it submitted is settled.

## 7. B-trees

- **Optimistic lock coupling,** with validation before IO and before acting on a child.
- **Structure changes** latch exactly the nodes they change, top-down, after a descent that
  validated them; a busy latch restarts the change. Freed nodes go through epochs.
- **Cursors** keep the key and the leaf's (page id, hint, version). Moving backward re-descends to
  the largest key below the current one rather than following fences, so no inconsistency can
  turn a step into a loop; an inconsistency is reported as corruption.

## 8. Everything else

- **The log.** Appenders reserve with one atomic add and announce their copy in a slot array
  (unchanged; already lock-free). Admission and durability waits are completions.
- **Checkpoints and writeback** own their work while stepping it and hold nothing across IO.
- **Recovery.** Each window is immutable once read; each partition is owned by the one thread
  applying it.
- **Transactions.**
  - A slot is owned by its transaction.
  - Its commit log has one writer, the slot's transaction, which appends and then publishes the
    new length with a release store. Readers search up to the published length. No lock.
  - **The published horizon.** A snapshot takes the horizon H: every commit timestamped at or
    below H is in its slot's commit log, and none above is visible to it. A commit draws its
    timestamp, publishes its pair, and then advances H past its timestamp once every lower
    timestamp is published. It waits, as a completion, only on committers still publishing a
    lower one, and is acknowledged after that, so a snapshot taken after the acknowledgement sees
    it. Readers never wait; a committer waits only for another committer's few instructions.
  - A writer that meets an open writer waits on that transaction's end completion. Before it
    waits, it records the edge in the slot wait-for graph and checks for a cycle (at most W
    slots). A cycle aborts its youngest transaction. Under BEGIN CONCURRENT it fails at once.
  - Version indexes: written only by their slot's transaction, read optimistically by everyone;
    entries are removed by garbage collection through epochs.
- **The one lock order** for the short critical sections that remain. Latches are never waited
  for, so they are in no order; the metadata pages' rule (bytes, then state) went with their
  reader-writer lock.

## 9. Proof

- **Deterministic interleavings.** Turso's sync layer runs on `shuttle` under `cfg(shuttle)`, and
  Atoll uses it. Each module gets schedule tests over its invariants: the buffer manager (pages
  keep their bytes through eviction, class moves and frees), B-trees (a sorted-map model through
  concurrent puts, removes, splits, merges and scans), transactions (snapshots see exactly the
  commits before them; no lost update; deadlocks always broken), recovery.
- **Contention stress.** Every concurrent test also runs 12 copies at once on the 16-core host,
  with a run count sized to the failure rate being ruled out.
- **Done** means both are clean, with the counts reported. A component that has only passed
  alone is not done.

## 10. Order of work

1. Epochs, the one latch kind with guards, yields instead of spins.
2. The buffer manager: validation before IO, epoch-deferred ids and batches, no relocation, no
   shared reads, metadata pages latched the same way. Schedule tests.
3. B-trees on top: schedule tests and stress, then the scan livelock found or proved gone.
4. Transactions: lock-free commit logs, waits as completions, deadlock detection. Schedule tests.
5. Then step 7 resumes from what is committed, re-proved under the new rules.
