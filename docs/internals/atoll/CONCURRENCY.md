# Atoll concurrency, from first principles

How Atoll runs concurrently. It replaces every earlier mechanism: DESIGN.md §9.2-§9.4 (latching
and replacement), the latching part of §13, §18, and the waits of §11.3. Step 1 of the order of
work (§10) is built; step 2 is not finished, and steps 3 to 5 are not started.

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
   validate its version afterwards. There is no shared latch mode at all. A reader may overlap a
   writer, so both touch the object's bytes only through atomic accesses, relaxed loads and
   stores: a plain read overlapping a write is a data race, undefined behaviour even when
   validation then throws its result away.
3. **Owned by one context at a time.** A transaction's state, an operation in progress, a
   writeback, a recovery partition, a slot's version index while its transaction writes. Handed
   over explicitly; no synchronization while owned.
4. **Lock-free shared state.** A free list, a waiter list, a map or a counter shared between
   threads changes by atomic operations, never under a lock a thread can be descheduled holding:
   each is named with its design (§4, §6). Mutexes that remain until their part is rebuilt are
   short critical sections, never held across IO, a yield, a callback or another lock except in
   the one global order section 8 lists, and each is named in that list.

Anything that fits none of these is a design error.

## 4. Epochs: when an identity may be reused

Read-copy update's grace periods (McKenney and Slingwine, 1998; Desnoyers et al., "User-Level
Implementations of Read-Copy Update", IEEE TPDS 2012): exact, with the step that holds a wait back
waking it. Lock-free throughout: no thread waits for another to enter or leave a step, scan,
retire, reclaim or register a wait. The fences around an announcement and a scan are
crossbeam-epoch's (0.9, `internal.rs`); its way of advancing the epoch is not used, for the reason
in the last point.

- **Participants.** Every thread stepping an instance is a participant. Its first step takes a
  slot in the instance's registry: a free one by compare-and-swap, or the next unused one. The
  registry is a chain of chunks of slots, each slot on a cache line of its own; a full chain gets
  another chunk with one compare-and-swap. Chunks are freed only with the registry, so a scan
  reads them with no lock and nothing to reclaim. When the thread exits its slot is released for
  the next thread to join, so the registry holds as many slots as threads have stepped at once,
  not every thread that ever did.
- **Announcing.** A thread entering a step (one call into Atoll that returns `Done` or `IO`)
  announces the global epoch in its slot's word: an acquire load, a store and a sequentially
  consistent fence. Leaving, it swaps the word clear, one atomic exchange, which also returns the
  flag a wait it held back left there. Nothing shared is read or written on the way out. Steps
  nest; only the outermost announces.
- **Retiring.** Freeing a page id or a temporary page, or giving up a frame's memory, advances
  the global epoch by one and stamps the object with the new value: one atomic add. Retired page
  ids and temporary pages are pushed onto a lock-free stack. A reclaim takes the whole stack with
  one swap into a heap ordered by stamp and hands out what the horizon has passed. The heap
  belongs to the reclaim under way: another finds it taken and hands out nothing this time,
  without waiting.
- **Reclaiming.** The horizon is the lowest announcement, or none while no step is under way. An
  object stamped `s` is reused once the horizon is at or above `s`: every step under way entered
  after it was retired. That is exact, so no step that has ended is ever waited for. A scan
  fences, reads every announcement, and takes an acquire fence after. Either it sees a step's
  announcement, or that step's reads see the object already unlinked; a step it sees clear has
  finished every read. A step that announced `s` or above loaded the epoch from the retiring add
  or later, so its reads see the object unlinked too.
- **The known horizon.** Every scan raises a shared lower bound to the lower of the horizon it
  found and the epoch it read before its fence. Everything stamped at or below it was unlinked
  before that scan; every step under way then was seen at or above it, and every step entered
  since either was seen by a later scan or sees those objects unlinked. So an object stamped at
  or below it is reused with one load and no scan. A page read in takes a frame quiet by it if
  one is free, and scans only if none is. A reclaim scans only once as many objects and reclaims
  have come since its last scan as a scan reads slots, so a scan costs a constant per object.
- **Waiting.** A claim that needs an object not yet reusable does not yield and retry. It
  pushes a wait for that epoch onto a lock-free list, then flags the slot of every step announced
  below the epoch, and returns a completion. The flag and the step's end both change the slot's
  one word, so either the end returns the flag, and the step wakes the waits, or the flag finds
  the slot clear and the step is not holding the wait back. A wake takes the whole list with one
  swap, flags the holders it finds, notifies every wait no holder holds back, and pushes the rest
  back. Having pushed them back it flags again, and goes round once more if one is reached now: a
  flagged step that ended while they were out of the list found none to wake, and pushing back
  after a wake's swap sees that step's end. A step no wait flagged reads nothing on the way out.
  A claim's wait is the pool's completion for a frame freed too, so whichever comes first ends it.
- **Grace periods.** Waiting for every step under way now to end is a retirement of nothing:
  advance the epoch and wait for the horizon to reach it. A claim that finds every frame it could
  take latched or held by steps under way waits for one.
- **Bounded in steps:** a step never blocks, so a participant delays reclamation by at most the
  step it is in. That is a bound in steps, not in time or memory: a thread descheduled mid-step,
  or a step that runs long, holds reclamation back for as long, and retired objects pile up
  meanwhile. Whatever needs them waits on a completion, and the stalled step's end wakes it.
- **Not crossbeam's epoch advance.** crossbeam-epoch advances the global epoch only once every
  pinned thread has seen it, tries every 128 pins, and frees garbage two advances after it was
  retired. That makes the check one load, and suits garbage nobody waits for. Memory a step needs
  back is another matter: a frame given up in a thread's last step becomes reusable only after
  two advances, and the second waits for that thread's next step to end. With exact stamps it is
  reusable as soon as the steps that could read it have ended, and the known horizon keeps the
  check to one load as well.
- **Verified by loom, Miri and shuttle** (§9). loom checks the orderings, with the objects the
  tests read and destroy as plain memory and a retired object's stamp, a wait's epoch and a
  chunk's slots as loom-checked cells; Miri checks the unsafe pointer code; shuttle checks the
  interleavings.
- **What it guarantees.** Within one step, nothing a participant read can be handed to another
  page, class or transaction. That alone rules out the drain, lost-value and new-page bugs above.
- **Across steps,** a participant holds only names with versions (page id, frame, version), and
  validates all three before use. A frame's version only rises, so a frame that held another page
  in between never validates. A cursor kept open for an hour pins nothing.
- **Frames and memory batches are checked, not retired.** A frame's identity is its latch
  version, which only rises, and every use validates it. A batch is used only through its frames,
  and every action on a frame drawn from a batch (an eviction, a drain) checks under the frame's
  latch that the page it holds is of the class it was drawn for. A stale name acts on nothing, so
  neither needs an epoch. Retiring batches was built and measured: the claim that empties a batch
  cannot use it before its own step ends, and another class's claim takes it back first, so two
  classes passed one batch back and forth forever (two single-threaded crash tests hung).
- **A page is read into a frame only once its memory is quiet.** A frame given up stamps every
  4 KiB grain it spans with the epoch. A new page is written through the frame's writer, whose
  stores are atomic, so a stale reader of the memory's last page races with nothing, and any free
  frame will do. An IO reading a page in writes the memory plainly, straight from the device, so
  it takes only a frame whose grains were all given up before every step under way began; with
  only stale ones free, the claim waits for the earliest of them to be quiet (Waiting, above).
  Memory still goes to its class at once, so nothing like the batch ping-pong above can start.

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
  version on use. It takes no lock: each bucket's chain is Michael's list (2002) linked through
  the frame headers, a link holding the next frame, a mark and a tag. Removing a frame marks its
  own link, so nothing is linked in after it, and a walk unlinks every marked frame it meets.
  Every write to a link raises its tag, and a walk reads a frame's link and page, then reads
  again the word that led it there, starting over if it changed: so it never follows a frame
  that left for another chain, and never misses a page that stays. A page goes in at its chain's
  head by compare-and-swap against the head its walk of the whole chain began from, so it is in
  one frame at most. A removal returns once nothing links the frame.
- **Validate before IO.** A miss on a page reached through another page starts IO only after the
  page it came from is validated. A descent that finds a child missing validates the parent, then
  reads the child in.
- **Size classes.** Memory moves between classes only as whole batches, and a batch is drained
  only by evicting its pages; live pages are never relocated. A dirty page is written out: its
  frame is held until the write lands, and whoever wants the page waits on that write, so a page
  that keeps changing still leaves.
  An emptied batch goes at once to the class it was drained for (§4).
- **The allocator takes no lock.** Every change to a batch is a compare-and-swap of one word:
  the class holding it, whether it drains, whether a release owns it, the frames taken, and the
  class it is drained for. A take first counts a frame in that word, only while its class holds
  the batch and it does not drain, then claims a free bit; the free bits never fall below the
  frames counted and not yet claimed, so one is there. The free that empties a draining batch,
  or the drain that finds it empty, is the one exchange that marks it released, so one thread
  hands it on. Per class, bitmaps say which batches it holds, which are live and which may have a
  free frame: hints its word confirms. A hint whose loss would hide memory is never lost: a free
  marks its batch after counting the frame back, and a take that clears the mark rechecks the
  count, the mark's setting and clearing being acquire-release exchanges of one word, so one sees
  the other; and a batch lent while its run is being reserved is drained either by the reserving
  drain or by the lending, across paired fences. Free batches are a bitmap in which aligned runs of up
  to four lie in one word, so a run is taken with one compare-and-swap, from the smallest free
  run that holds it as a buddy allocator would, and coalesces as its bits are set. The run held
  for a large class is one word too, counting down the batches it still misses; every holder of
  a batch in it is drained until it is whole, the class itself included.
- **IO in flight and the pool's wake** are completion cells: one pointer each, put and taken by
  the frame's holder or anyone finishing the wake, read by anyone in a step for a reference of
  their own. A completion taken out is dropped through the epochs, once no step that could have
  read the pointer is left. The pool's first error is set once by compare-and-swap.
- **The writeback queue and each writeback's landed writes** are stacks whose items are taken
  all at once (`stack.rs`): a push is a compare-and-swap of the head, a take one swap of it, so no
  thread reads a node another may free and no epoch is needed. A thread stepping writebacks takes
  them all, steps each and puts back those not done; another finds none to step, as it found
  them held before. A count of those queued, held ones included, says whether any is left.
- **The writeback buffer's ring** takes no lock (`staging.rs`). Its head and tail are positions
  that only rise, a lap apart at most; a lease is a compare-and-swap of the head, and a wrap a
  lease of the end's remainder given back at once. A lease given back records its end at the
  unit it starts in, and its giver moves the tail past every lease back at it, the record and
  the tail each written and the other read across paired fences, so room given back is never
  left behind the tail.
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
- **The one lock order** for the short critical sections that remain.
  - Latches are never waited for, and some mutexes are only ever tried, as ownership of a piece
    of work (§3, class 3): a checkpointer's state, a writeback, the page store's erasure and its
    tree growth, a reclaim of retired objects. Neither is in any order: busy means a yield, or for
    a reclaim, nothing reclaimed this time. An owner may hold its mutex across IO it submits.
  - The mutexes that are waited for nest only as follows, outer first:
    - the metadata cache's map of pages, then its order of arrival, then a page's state, then
      the dirty list;
    - the page store's erasing list, then its unerased slots or its open intents;
    - the log's flush, then its admission;
    - a transaction slot, then the log (step 4 replaces the slot's lock with a commit log of one
      writer);
    - a version root, then the page store's orphans.
  - None is held across IO, while a callback runs, or while a completion is completed, except
    the log's flush and a transaction slot: an append can submit log writes, whose completions
    run inside the submission on some backends. Both go with step 4.
  - Every other mutex is taken alone, nesting nothing.
  - Every acquisition in the engine was read for this list (2026-11-01): no two mutexes are taken
    in both orders.

## 9. Proof

- **Shuttle schedules.** Turso's sync layer runs on `shuttle` under `cfg(shuttle)`, and Atoll uses
  it. Each module gets schedule tests over its invariants: the buffer manager (pages keep their
  bytes through eviction, class moves and frees), B-trees (a sorted-map model through concurrent
  puts, removes, splits, merges and scans), transactions (snapshots see exactly the commits
  before them; no lost update; deadlocks always broken), recovery. A schedule test runs 300
  random and 300 PCT schedules: a sample of the interleavings, not all of them. Its name ends in
  `_in_shuttle_schedules`, so `make test-shuttle` runs it, and a workload that keeps some
  operations apart says which races it leaves out.
- **loom** (`--cfg atoll_loom`) explores the C11 model's reorderings as well as interleavings, in
  models of a few threads. Plain data one thread writes and others reach through a published
  pointer (an object a step reads, a retired object's stamp, a wait's epoch, a chunk's slots) is
  a loom-checked cell under loom, so a read not ordered after its write fails. loom models
  `fence(SeqCst)` and treats sequentially consistent accesses as acquire-release, which errs
  toward false alarms. It hands a read-modify-write the latest write in its run, so it cannot
  show that one needs an acquire to see another thread's earlier write. And it let through one
  interleaving shuttle caught at once (the page table's walk without its re-read), which smaller
  probes did not reproduce, so no part rests on loom alone for its interleavings.
- **Miri** (`cargo +nightly-2025-10-01 miri test`, `-Zmiri-many-seeds`) runs threaded tests,
  checking every access for undefined behaviour, a use after free among them, and data races, at
  the interleavings its scheduler draws. In eight seeds it caught none of six weakened
  publications that loom's checked cells catch, the tests' own synchronization ordering the
  accesses in the schedules it drew; it is the instrument for pointers and reference counts.
  cargo-miri's test binary is a stub that records the tree it was built in and compiles that
  tree's sources when it runs, so a Miri run gets a tree and a target of its own, never copied,
  and a mutant is applied in that tree.
- **Contention stress.** Every concurrent test also runs 12 copies at once on the 16-core host,
  with a run count sized to the failure rate being ruled out.
- **Fail-first by mutation.** Every ordering, fence and check a lock-free part relies on is
  weakened or removed in turn, and the suite must fail. A mutant nothing catches is either an
  ordering or check something else already supplies, which then goes or is named as kept, or a
  blind spot named here with the argument that covers it. The blind spots, orderings C11 needs
  that no instrument here could show are needed:
  - A step's end swaps its word acquire-release. The flag it reads was set after the wait was
    pushed; only the acquire orders the wake's taking of the list after that push, so the wake
    finds the wait.
  - A wait is pushed acquire-release. Put back after a wake took the list, it reads the swap of a
    step's end that found the list empty; only the acquire orders the recheck that follows after
    that step's end, so the recheck sees the step gone and finishes the wait.
  - A batch lent while its run is being reserved: the reserving drain writes the reservation and
    reads the batch's state, the lending writes the state and reads the reservation, and only a
    sequentially consistent fence on each side forbids both reading the old value. loom let the
    removal of either fence through.
  Orderings found redundant and kept, each saying what supplies it: a frame's claim is acquire,
  though the batch word's acquire-release chain already orders the freer before the next taker;
  and a tag is raised on both kinds of write to a link.
- **Done** means all of them are clean, with the counts reported. A component that has only
  passed alone is not done.

## 10. Order of work

1. Epochs, the one latch kind with guards, yields instead of spins. Built.
2. The buffer manager: validation before IO, epoch-deferred ids, batches checked under the latch
   (§4), no relocation, no shared reads, metadata pages latched the same way, writes settled in
   steps, page bytes read and written only through atomic accesses (§3). Schedule tests.
3. B-trees on top: schedule tests and stress, then the scan livelock found or proved gone. Found:
   a split appending past the rightmost leaf kept every entry in the left half, which then could
   not take its new upper fence, and the writer panicked with the tree half changed; scanners
   looped on it, and the test hung before printing the panic. A split now takes a point where both
   halves fit; a scan step that does not move on, and a descent that lands outside its key, are
   reported as corruption.
4. Transactions: lock-free commit logs, waits as completions, deadlock detection, and a log flush
   that holds no mutex across its writes. Schedule tests.
5. Then step 7 resumes from what is committed, re-proved under the new rules.
6. Lock-free throughout (2026-11-02), one part at a time, each with its design here, loom, Miri
   where it holds pointers, shuttle, stress and mutants before it is done: the epochs; then the
   buffer pool (its page table, allocator, IO in flight, wake, first error, writeback queue,
   landed writes and ring done; its temporary pages next); the page store's sets and maps; the
   metadata cache; the log's admission and flush; writeback's landed list; transactions; the
   cold paths last. Mutexes only ever tried (§8) are ownership, not locks, and stay.
