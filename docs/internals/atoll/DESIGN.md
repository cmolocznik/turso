# Atoll

Atoll is a storage mode for Turso beside the SQLite pager and MVCC. LeanStore is its baseline, as
described in Leis, "LeanStore: A High-Performance Storage Engine for NVMe SSDs" (PVLDB 17, 2024).
Its buffer manager, its page writes and its log depart from LeanStore where the requirements below
force them to; section 2 lists each departure and why.

The code is in `core/storage/atoll`. Section 20 says which parts are built.

## 1. What it has to do

- **Size.** One design from 100 MB to 100 TB. Memory follows the working set. Nothing held in
  memory grows with the size of the database, except summaries of a few bytes per gigabyte.
- **Storage tiers.** Local disk can be the whole store, or a cache in front of object storage.
- **Compute on app servers.**
  - Query computation can move to app servers. An app server holds only what fits in a bounded
    memory budget and never replicates the whole database.
  - A page it lacks comes over the network from a database node or from object storage, instead of
    from disk.
  - Its writes either commit optimistically from the app server, or go to a database node.
- **Replication** between servers and devices.
- **Durability**, chosen per database:
  - local disk;
  - local disk with object storage;
  - a Raft cluster, whose log becomes the log (added later).
- **PostgreSQL workloads:**
  - many concurrent writers;
  - read committed and snapshot isolation, with serializable later;
  - long-running reads, and transactions larger than memory;
  - crash safety;
  - commit latency that checkpoints and page writeback do not move.
- **Turso's SQL layer:** everything keeps working:
  - rowid and WITHOUT ROWID tables;
  - every index and index method;
  - the SQLite and PostgreSQL frontends.
- **Torn writes.** Any write can tear, at any size. Nothing depends on the OS or the device writing
  atomically.
- **Erasure.** Deleted data leaves the disk promptly.
- **No SQLite file compatibility.**

## 2. Baseline and departures

From LeanStore 2024 we take:

- 4 KB pages;
- optimistic latching with versioned latches;
- B-tree nodes with fence keys, prefix truncation and key heads;
- MVCC: Ordered Snapshot Instant Commit (OSIC), with per-tuple version headers, a per-worker version
  index, and a graveyard index for deleted tuples;
- continuous checkpoints;
- parallel recovery in the ARIES phases;
- write-aware page replacement (WATT).

Where Atoll departs, and why:

| Atoll | LeanStore | Why |
| --- | --- | --- |
| A hash table from page id to frame, made fast by frame hints that are checked on use (section 9) | Pointer swizzling (2018–2023), then vmcache | **Swizzling** allows one reference per page, makes eviction understand page contents, and is hard to synchronize (LeanStore 2024 §2.1). **vmcache** needs virtual memory as large as the database (128 TiB with 48-bit addresses), plus per-page state for all of it. It also pays a page fault and a TLB flush per eviction, and needs a kernel module to scale. **Hints** cost what swizzling costs on a hit, and the frame memory can sit on huge pages. |
| Pages written out of place through a page table | Pages written in place | Writes can tear. Out of place, the old copy stays whole until the new one is durable, at one write per writeback. In place would need a second copy written first, which is twice the writes. Immutable page copies are also what object storage and remote readers fetch. |
| One log for the database, filled in parallel | One log per worker thread | Raft replicates one ordered log. App servers and replicas follow one stream. Optimistic commits are validated against one order of commits. |
| Size classes, 4 KiB to 8 MiB, for large values (Umbra) | 4 KB pages only | A large value is one IO into contiguous memory. |
| Transaction slots | Worker threads | Turso has no fixed worker threads. A slot is held by one transaction at a time, which is the property OSIC needs from a worker. |

## 3. Shapes of a deployment

- **Database node.** It holds the database on local disk and writes the log. It serves pages and the
  log to others.
- **App server.** The same engine with a memory budget and no disk. A page it lacks comes from a
  database node or from object storage. It follows the log to keep its pages current (section 15).
- **Replica, server or device.** A follower of the log holding all of the database or part of it
  (section 16).

Durability is chosen per database:

| Mode | A commit is acknowledged when | Losing the node's disk loses |
| --- | --- | --- |
| Local | its log record is durable on local disk | everything not backed up elsewhere |
| Local with object storage | its log record is durable on local disk. The log and page copies are uploaded continuously; a setting can make commits wait for the upload. | the log not yet uploaded, unless commits wait for it |
| Raft (later) | a majority of the cluster has its log record durable | nothing, while a majority survives |

## 4. Files

```text
{db}               control file: two 4 KiB superblocks, written in turn
{db}.c{k}.{n}      data file n of size class k, each at most a fixed size
{db}.map{k}.{l}    page-table pages of size class k at level l
{db}.free{k}.{l}   free-space pages of size class k at level l
{db}.log.{n}       log segments by number, and the next one prepared
{db}.temp{k}       temporary pages of size class k, deleted at every open
```

- **Data files** are split into files of a fixed size (a creation parameter), so none exceeds what a
  file system allows. ext4, for one, caps a file at 16 TiB.
- **The superblock.** The newest that reads back whole is in charge. It holds:
  - a magic number and the format version;
  - the database id (128 random bits);
  - the creation parameters;
  - the checkpoint number and the redo point;
  - per class, where the page-table root and the free-space root are;
  - the clock floor;
  - a CRC32C.

  It is written with a durable write. A torn superblock leaves the other one in charge.

## 5. Pages

- **Size classes:** 4 KiB × 2^k for k = 0..11, so 4 KiB to 8 MiB. B-tree nodes are 4 KiB unless a
  tree is created with a larger class. Larger classes hold large values.
- **Header (24 bytes):** `lsn: u64 | crc32c: u32 | class: u8 | kind: u8 | flags: u16 | pid: u64`.
  - The checksum covers the whole page with its own field taken as zero.
  - It is checked on every read from disk, from a node and from object storage.
  - A copy that fails it is torn, or corrupt, or a stale slot. It is never read as valid.
- **Kinds:**
  - page-table directory and map;
  - free-space directory and bitmap;
  - transaction slot table;
  - B-tree inner and leaf;
  - blob;
  - version index and graveyard (temporary, section 11).

  Each kind provides how to replay its log records and how to check its structure.

## 6. The page store

### 6.1 Page ids, slots and the page table

A page id is `class: 8 bits | number: 56 bits`. A slot is a place in a class's data files. Every
writeback goes to a fresh slot, so a page's id never changes and its slot does.

The page table maps each page number to a `u64`: free (the number is unused), allocated (no copy
yet), or the slot of its copy. That is 8 bytes, 0.2% of a 4 KiB page. A free entry is a free
number, so the page table also allocates page numbers, lowest first, which keeps it dense after
churn. Where pages live in object storage, a second table beside it says where (section 14).

### 6.2 Metadata pages

The page table and free space are radix trees of 4 KiB metadata pages, one of each per class
(`core/storage/atoll/radix.rs`, `meta.rs`).

- **Leaves.** In the page table, 509 entries per page. In free space, a bit per slot, 32,576 per
  page.
- **Above the leaves,** a `u64` per child: how many units below it are in use. A free number or
  slot is found without reading pages that have none. A page of zeros is all free, which is how
  every page starts.
- **Lookups** go straight to the leaf a page number falls in; only allocation descends.
- **Growing.** A tree grows a level at the top, the old root becoming child 0 of the new one. A
  page's place in its file is fixed by its level and index.
- **Two places per page.** A metadata page has two places side by side. It is always written to
  the place that does not hold its newest durable copy, so a torn write leaves that copy whole, and
  a read takes both places and keeps the newest whole copy. This costs twice the space of the
  metadata, 0.4% of the data, and needs no free space, no parent pointers and no cascade of writes
  up the tree.
- **A cache of their own.** Metadata pages live outside the buffer pool, because a buffer miss
  needs the page table.
- **Logged first.** Every change is logged first, under the latches of the pages it changes, so
  changes reach a page in log order. A page is written only once the log is durable up to its LSN.

### 6.3 The life of a slot

1. **Intent.** A writeback batch takes free slots, marked in use, and logs an intent naming them.
   The intent is durable before any of those writes starts.
2. **Write.** Each copy is durable when its write completes: a write with `RWF_DSYNC`, or a write
   followed by `fdatasync`.
3. **Remap.** `Remap { pid, new, old }` is logged. The entry points at `new`, and `old` is freed in
   the same change.
4. **Erase.** Once the record that freed `old` is durable, `old` is erased (a hole punched, or
   zeros written). Only then may it be taken again.

After a crash, frees logged after the point where the log restarted wait for a checkpoint past it
(section 7.4), since a recovery from before that checkpoint cannot read them.

- **Copies ahead of the log.** A copy may hold changes whose log records are not durable yet. Its
  remap is appended after it lands, behind every record it holds, so a remap that survives a crash
  names a copy whose records survived too.
- **What the redo point waits for.** A checkpoint's redo point never passes:
  - a metadata change not yet written;
  - a free not yet erased;
  - an intent with slots not yet remapped or given back.

  So recovery always replays what it must finish.
- **What recovery finishes.** It erases what replayed records freed. A replayed intent first cancels
  any earlier replayed free of the same slots: they were erased before they were taken again. Slots
  of intents no remap matched are given back and erased, so a crash leaves no copy that nothing
  refers to.

### 6.4 Temporary pages (step 7)

Version index and graveyard pages (section 11), and the spill of large sorts, live only as long as
the process does. They are never logged. When evicted they go to the temporary files, which are
deleted at every open, and recovery never reads them.

## 7. The log

The log is the database. Page copies, the page table and every cache are materializations of it
that bound how much of it must be replayed.

### 7.1 Records

```text
len: u32 | kind: u8 | flags: u8 | reserved: u16 | txn: u64 | prev: u64 | payload | crc32c: u32 | padding to 8
```

- **Alignment and CRC.** Records start at multiples of 16 bytes. The CRC covers the record's
  position, its header and its payload, so a record read at another position, a torn record, or
  bytes a crash left behind never read as valid.
- **LSN.** A record's LSN is the position just past its end.
- **Chaining.** `prev` is the previous record of the same transaction, so a transaction's records
  are found from its last one.
- **Page records** carry the page id: New, Free, Remap, write intent, blob write, and the B-tree
  operations (insert, update with the changed columns, delete-mark, remove, split, merge, new root).
  - Redo is physical per page.
  - Records that change a tuple also carry what undoes it: the old values of the changed columns, or
    the key to remove. Undo applies these logically, by tree and key, so it works after splits and
    merges.
  - Logging undo costs about 20% more log volume (Haubenschild et al. §4: 2,230 against 1,850 bytes
    per TPC-C transaction).
- **Transaction records:** Begin, Commit (with its timestamp), End, and compensation (the redo of one
  undo step, naming the next record to undo).
- **Structure changes** (split, merge, new root) are system operations. They are redo-only, logged
  atomically as one record, and never undone by a user transaction's rollback.

### 7.2 Filling the log in parallel

- **Reserving.** The log buffer is a ring of aligned chunks. An appender reserves space with one
  atomic add on the insert position and copies its record in, in parallel with other appenders.
- **Knowing what is complete.** Each appender announces, in a small array, where its copy in
  progress starts. It does so before reserving, with sequentially consistent ordering. A flusher
  reads the insert position and then the array, so it sees every copy still running below that
  position. It writes only the prefix every appender has finished.
- **Write-ahead rule.** A change's record is appended before the page latch is released, and the
  page's LSN is set to it.
- **Never blocking mid-change.** Space is never waited for in the middle of a change. An operation
  waits, if it must, before its first latch, until the unflushed bytes are below the buffer's size.

### 7.3 Flushing and group commit

- **Durable writes** (Linux with direct IO): each write carries `RWF_DSYNC` and is durable when it
  completes. Several may be in flight. The durable position is the end of the contiguous prefix of
  completed writes.
- **Write then sync** (anywhere else): write, then `fdatasync`, with the next write overlapping the
  sync in flight.
- **Triggers.** A flush starts when a commit waits on it, when the buffer fills, or when the page
  cleaner needs durability. There are no timers.
- **Padding.** Every flush ends on a 4 KiB boundary. When a waited-on record sits in a partial
  block, the block is padded to its end; padding has no CRC and is checked by its own position
  instead. Durable bytes are never rewritten, so a torn write cannot take committed records with
  it.

### 7.4 Segments

- **Layout.** The log is a sequence of segment files of S bytes. S is a creation parameter,
  256 MiB by default: at the 1 GB/s log rate Umbra measured (VLDB 2022 §4), that is a quarter of a
  second. LSN `l` lives in segment `l / S`, and records may span segments.
- **Preparation.** Segments are created, written full of zeros and synced, with their directory,
  before any record goes in, so later writes change no file metadata. Background work keeps the
  next one ready.
- **Removal.** A segment is deleted, which also erases it, once:
  - it is wholly below the redo point;
  - and below the first record of the oldest transaction still writing, whose undo may need it;
  - and, with object storage, it has been uploaded.
- **Reopening after a crash.** Appending resumes at the block boundary after the log's end. The
  rest of that segment is zeroed and later segments removed, so nothing a crash left beyond the
  end is ever read as log. Bytes between the end and that boundary stay unreadable, so nothing
  appended is acknowledged until a checkpoint past it is durable.
- **Readers.** Recovery, replicas, app servers and the archiver read segments by position. A segment
  no longer local is fetched from object storage.

### 7.5 Failure

Any failed log write or sync makes the log refuse further appends and fails every waiter. The
database accepts no writes until it restarts and recovers.

## 8. Commit latency

A commit appends its record and waits for durability: one device write in the local modes, one
round to a majority under Raft. Page writeback goes to other files and never waits on the log path.

## 9. The buffer manager

Built in `core/storage/atoll/buffer`.

### 9.1 Frames

- **One region.** The pool is one anonymous region of the configured size, mapped without
  reserving memory and backed by 2 MiB huge pages where the OS allows. It is carved into 2 MiB
  batches, and each batch is lent to one size class at a time: 512 frames of 4 KiB, down to one of
  2 MiB. A 4 MiB or 8 MiB frame takes an aligned run of 2 or 4 batches from a buddy allocator.
- **Frame ids.** A frame is named by the 4 KiB grain it starts at, as a `u32`, so one pool holds
  at most 16 TiB. The id names the same memory whichever class holds it.
- **Never unmapped.** A freed frame is reused as it is. Memory moves between classes a whole batch
  at a time, so no huge page is ever split.
- **Moving memory.** A batch moves once every frame in it is free. A batch to be moved is marked
  draining, which keeps new frames out of it. Its pages then move to free frames of the same class
  elsewhere (a copy, the hash entry repointed, the old frame's version bumped), or are evicted, or
  are written back first.
- **Headers.** Frame headers sit in their own array, one per 4 KiB, which is never unmapped, so a
  hint however stale always reads a header. Each header holds:
  - the versioned latch, whose version also rises on eviction;
  - the page id and the frame's state (free, loading, resident);
  - the page's LSN and the LSN of its newest copy written back. The page is dirty while the first
    is past the second. Checkpoints need no first-unwritten LSN per page, because their passes
    record the log's end before writing a shard (section 10);
  - writeback flags;
  - the link to the next frame in its hash chain;
  - WATT's timestamps.

  That is 104 bytes per 4 KiB frame, 2.5%.
- **Cost of memory.** Untouched memory costs nothing: the region, the headers and the hash table
  take physical memory only when used.
- **Minimum.** A pool is at least 10 MiB: one 8 MiB frame and one batch beside it.

### 9.2 Finding a page

1. **The table.** One hash table maps page id to frame, for pages resident or loading, and is the
   only translation. A bucket holds the first frame of its chain and a lock bit; each header links
   the next frame. That is 8 bytes per 4 KiB frame.
   - Inserts and removals lock the bucket, for a few instructions.
   - Lookups take no lock. A frame can change chains under a lookup, so what it returns is checked
     against the frame's latch and page id, and an absence is checked again under the lock by
     whoever acts on it.
2. **Hints.** A reference to a page carries a frame hint beside the page id: the frame plus one,
   with 0 for none. A hint is never trusted. Following it latches the frame and checks that it
   holds the page. If it does, the access is a hit with no hash lookup.
3. **Stale hints.** A stale or missing hint costs one hash lookup, and the hint is then refreshed
   with an atomic store. Hints are never cleaned up, so eviction knows nothing of who refers to a
   page, and any number of references may point at one page.

### 9.3 Fixing a page

- **Optimistic:** read the version, read the page, validate the version. What is read may be
  mid-change, so code reading optimistically checks every offset before use and never panics on
  garbage. An optimistic fix upgrades to exclusive if the version is unchanged.
- **Shared** and **exclusive.** A sole shared holder may upgrade.
- **No waiting on a latch.** A busy latch is a cooperative yield. A page being loaded is a wait on
  its read. A full pool is a wait on the next frame freed.
- **No guard is held across a VDBE yield.** A cursor keeps the frame, the version it saw, its
  position and its key. On resuming it validates the version, and if the page changed it seeks again
  by key.
- **Changes.** Every change is logged before the latch is released and reported with the end of
  its record, which becomes the page's LSN and makes the page dirty.
- **A miss** asks the page source to locate the page, which may wait on IO with nothing claimed.
  It then claims a frame, links it as loading, and reads the page into it. A reader that finds the
  page loading waits on that read.
- **Prefetch** starts reads for pages not in memory and never waits: a page whose location needs a
  read first, or for which no frame is free at once, is skipped.

### 9.4 Replacement

WATT (Vöhringer and Leis, PVLDB 2023; LeanStore 2024 §2.2), with the paper's tuned defaults:

- each frame records the epochs of its last 8 accesses and its last 4 changes;
- a page's value is its best subfrequency (accesses over the time since the oldest of them), with
  the most recent access alone damped by 0.1. Changes count again, weighted by 4;
- the epoch advances each time a quarter of the pool is evicted;
- eviction takes the least value among 8 sampled frames as its threshold, then looks at 64 more:
  clean ones at or below it are evicted, and dirty ones are written back, then evicted if still
  clean.

The paper divides by `now - t`, which is zero within the current epoch. Here elapsed time counts
the current epoch as one, so pages used this epoch still rank by how often.

**Between size classes.** When a class has no free frame and no free batch is left, each class's
threshold is divided by its page size. If another class's value per byte is lower, the requesting
class takes that class's emptiest batch; otherwise it evicts its own pages. Value per byte is
GreedyDual-Size's rule with every miss costing the same (Cao and Irani, 1997). A class of 4 or
8 MiB pages reserves an aligned run, and the batches in it drain into it as they empty. One drain is
in progress at a time.

Eviction removes the page from the hash table and bumps the frame's version, so every hint to the
frame goes stale.

### 9.5 Writing back

- A writeback takes dirty frames of one class and marks them writing, so they are neither evicted
  nor dropped meanwhile. The page store takes fresh slots for them under a logged intent (section
  6.3).
- Each page is copied under a shared latch, held only for the copy, and the copy is written. A page
  changed while its copy is in flight stays dirty.
- No log flush comes first. A copy's remap is logged after the copy lands, so a remap that
  survives a crash names a copy whose records survived too.
- A writeback is stepped by whoever needs frames, or by a clean pass over a range of frames, which
  is what checkpoints run (section 10).
- A failed page write, like a failed log write, stops the pool until the database is reopened.

### 9.6 Page sources

A miss asks a page source for page P in two steps:

1. **Locate:** whatever lookup the read needs first, which may wait on IO. Nothing is claimed yet.
2. **Read** into the claimed frame. What locate found must still hold. For the local store, the
   page-table leaf stays pinned in between, so every change to the page's entry reaches it. Once
   the pool has claimed the page, nothing can change its entry until the read is done.

The local store's source reads the slot the page table names, verifies its checksum and page id,
and gives a page with no copy yet as zeros. A copy that fails verification is corruption, never
retried.

Still to come, for app servers and replicas (section 15):

- a read for page P no older than log position X;
- answers carrying the source's log position, and the versions a reader at X needs;
- sources that are a database node, or object storage;
- batched misses, because a network fetch costs about 100 µs–1 ms against about 50 µs for NVMe.

## 10. Checkpoints

Checkpoints are continuous (LeanStore 2024 §2.7, Haubenschild et al. §3.4).

1. **Shards.** Frames are split into S shards; S is 128 by default, which Haubenschild found
   smoother than 10.
2. **Passes.** Each time 1/S of the log budget has been appended, the cleaner writes back every
   dirty page of the next shard, round robin. Before it starts, it records the log's end as that
   shard's checkpointed position.
3. **Redo point.** The minimum checkpointed position over all shards. When it moves, a new superblock
   is written.
4. **Log budget.** The log budget is how much log recovery may have to replay. It is configured,
   and by default scales with the pool: 0.4 times the pool, the ratio of Haubenschild's example (a
   20 GB WAL for a 50 GB pool).
5. **No stalls.** Writing a page latches it only long enough to copy it. A page being written may
   still change, and is clean afterwards only if it did not.

## 11. Transactions and versions

The MVCC is OSIC (Alhomssi and Leis, VLDB 2023; LeanStore 2024 §2.6).

### 11.1 Slots, the clock and visibility

- **Slots.** W transaction slots; W is the most transactions open at once. A transaction holds a
  slot from begin to end, so a slot runs one transaction at a time.
- **Clock.** One global atomic clock issues start timestamps and commit timestamps.
- **Commit logs.** Each slot keeps a commit log: a fixed array of its recent (start, commit) pairs,
  in order.
  - A version written by (slot s, start t) is visible to a snapshot at time T when the last
    transaction of s committed before T started at or after t.
  - A snapshot caches that answer per slot, so a check consults another slot only for versions that
    slot wrote.

### 11.2 Tuples and versions

- **Header.** Every entry of every B-tree, table or index, starts with a 14-byte header:

  ```text
  slot: u16 | ts: u64 | command: u16 | updates: u8 | flags: u8
  ```

  - `ts` is the writer's start timestamp.
  - `command` is the statement within the writer's transaction.
  - `updates` counts updates while the oldest snapshot stays the same.
  - `flags` mark a deleted entry or a lock with no new data.
- **Old versions.**
  - **Version index.** Each slot keeps a version index, a B-tree of temporary pages keyed
    `slot | ts | tree | key`. An update stores its delta there, or the whole old value when the size
    changes. Inserts go to the right edge, and garbage collection is a range delete.
  - **Fat tuples.** A tuple updated very often keeps its recent deltas inline instead (OSIC's fat
    tuples).
- **Graveyard.** A deleted entry that no running OLTP snapshot can see, but an older snapshot still
  can, moves to the tree's graveyard. OLTP never reads the graveyard, and long-running reads merge it
  in (OSIC §3.2).
- **No durability for versions.** Versions are never logged or recovered: no snapshot survives a
  crash.

### 11.3 Writing

- **Waiting on an open writer.** If the newest version of an entry belongs to another slot's open
  transaction, a writer waits for it to end, as PostgreSQL does. Under BEGIN CONCURRENT semantics it
  fails at once instead.
- **A newer committed version.** If the newest version was committed after this snapshot:
  - snapshot isolation fails the transaction (first committer wins);
  - read committed reads that version and has the statement check it again (PostgreSQL's rule for
    UPDATE).
- **Row locks.** FOR UPDATE writes a lock-only header. FOR SHARE needs a shared lock table, which
  comes later.
- **Deadlocks.** Waits form a graph over slots; a cycle aborts its youngest transaction.

### 11.4 Commit

1. Draw the commit timestamp and append the Commit record.
2. Enter the timestamp in the slot's commit log. The transaction is visible from here.
3. Acknowledge once the durability mode is met.

A transaction that saw this commit has its own commit later in the one log, so it can never be
durable without this one. A read-only transaction acknowledges only once the log is durable up to
where it stood at its snapshot.

### 11.5 Rollback and garbage collection

- **Rollback.** A running transaction rolls back from its versions in the version index, logging a
  compensation record per step. Savepoints and statement rollback stop at a remembered point.
- **Garbage collection** is cooperative, done between transactions (OSIC §3.5). Watermarks give the
  oldest snapshot, the oldest OLTP snapshot and the newest OLAP snapshot.
  - Versions below them are dropped by range deletes on the version indexes.
  - Deleted entries are removed, or moved to the graveyard.

### 11.6 Isolation levels

- **Read committed** (PostgreSQL's default), and **repeatable read** as snapshot isolation.
- **Serializable** comes later: PostgreSQL-compatible SSI, or precision locking.
- **In the SQLite frontend,** BEGIN and BEGIN CONCURRENT both mean snapshot isolation; every
  transaction may write concurrently.

## 12. Recovery

1. Read the newest whole superblock.
2. **Analysis**, from the redo point, in parallel by segment:
   - collect remaps, write intents and transactions;
   - find the log's end: the first record that fails its CRC at its own position.
3. **Redo**, in parallel by page id range:
   - each page starts from its newest copy whose remap is durable, or else the copy the checkpoint's
     page table names;
   - records newer than the page's LSN are applied.
4. **Erase** the slots of intents that got no remap.
5. **Undo** every transaction with no Commit or End, from its records, following `prev` back from
   its last one. Compensation records are logged, and steps already compensated are skipped.
6. **Restart the clock** above every logged timestamp. Every header left in a page is then a
   committed version.
7. **Checkpoint.**

Recovery reads at most the log budget, plus the undo of open transactions. Haubenschild et al.
replay 100 GB of log in 38 s on 40 threads (§4).

## 13. B+-trees

- **Nodes** are slotted (LeanStore 2023 §4):
  - fence keys, fixed at split and merge;
  - the fences' common prefix stripped from every key;
  - a 4-byte head of each key stored as an integer in its slot;
  - a hint array.
- **Node size.** 4 KiB unless the tree chose a larger class.
  - Values larger than a quarter of a node go out of line, into a blob page of the smallest class
    that holds them.
  - Values beyond the largest class go into a tree of blob pages.
- **Child entries** hold the child's page id and a frame hint (section 9.2).
- **Neighbour hints.** A leaf may also hold its right neighbour's page id as a hint, which scans
  check against the neighbour's lower fence. Since hints are never trusted, splits and merges need
  not keep them right.
- **Keys** are byte strings compared with `memcmp`.
  - **Rowid tables:** 8 bytes, big-endian with the sign bit flipped.
  - **Indexes and WITHOUT ROWID tables:** the index columns encoded so that byte order is SQLite's
    order:
    - exact for BINARY, NOCASE and RTRIM, descending columns, and SQLite's mixed integer and real
      comparison;
    - a tree with a user-defined collation compares with Turso's record comparator instead.

    Index entries keep the original record too, since encoding loses case and the integer/real
    distinction.
  - **Proof:** a property test compares the encoding's order with `compare_immutable` on random
    records.
- **Latching:** optimistic lock coupling. A writer latches only its leaf, and a split restarts from
  the root and latches the nodes it changes (LeanStore 2018 §IV-F).
- **Later:** contention splits and merging of sparse neighbours.

## 14. Object storage

- **What lives there:**
  - **Sealed log segments.** The open segment's tail is also uploaded in chunks when commits wait for
    the upload.
  - **Page copies,** packed by the cleaner into large immutable objects. Uploading one is followed by
    a logged remap naming its remote place.
  - **A manifest per checkpoint:** the redo point, and where the page-table roots are.
- **Local slots are a cache.** A page whose copy is uploaded may lose its local slot whenever space
  is needed.
- **Restart anywhere.** Fetch the newest manifest and replay the log from its redo point. Losing the
  local disk loses nothing that was uploaded.
- **Point in time and branches.** Kept manifests give point-in-time recovery: a manifest plus replay.
  A branch is a new log from a point, sharing every existing object.
- **Garbage collection.** An object is deleted once no kept manifest refers to a copy in it. Objects
  that are mostly dead copies are rewritten; this never touches the read path.

## 15. App servers

- **Same engine.** An app server runs the same engine with a memory budget and no disk. Queries
  whose working memory would exceed the budget run on a database node instead.
- **Following the log.** It follows the log stream and applies each record to the page only if it
  holds that page. From the commit records it builds the slots' commit logs. Its queries read at the
  log position it has applied.
- **Fetching a page.** A page fetched for position X comes with the versions of its entries that a
  reader at X needs: the old values of changes not committed at X. They come from the node's version
  index.
  - The page is valid at X if its last change is at or before X.
  - If it is newer, the app server first catches up to the page's position.
- **Evicting** a page is free, because an app server's pages are never dirty.
- **Writes**, in either of two modes:
  - **Forwarded:** the statement runs on a database node.
  - **Optimistic:**
    - The transaction runs on the app server. Its writes go to the node as row operations by tree
      and key, never as pages, together with its snapshot timestamp.
    - The node runs them as a transaction at that snapshot. It fails if a row it writes has a newer
      committed version, and it checks unique and foreign keys again.
    - A serializable transaction would also send the keys and ranges it read.
    - The commit returns as a log position. The app server sees its own writes once it has applied
      the log that far.

## 16. Replication and Raft

- **Replicas.** A replica, server or device, is a follower of the log holding all of the database or
  part of it. It works like an app server, with its own disk.
- **Raft** (later):
  - the Raft log is the log, and the leader is the node that writes;
  - a commit is acknowledged once a majority holds it durable;
  - followers are replicas, and app servers follow the leader.
- **Not covered: devices that write while offline.** Two writers' pages diverge, so syncing them
  needs a logical, row-level layer with conflict handling on top of this.

## 17. Turso's SQL on it

The SQL layer touches storage in these places:

- `CursorTrait` (`core/storage/btree.rs`);
- about 25 pager operations, called from the VDBE and the connection;
- the database header fields;
- the meaning of `sqlite_schema.rootpage`.

MVCC plugs into the same places with 38 explicit branches in `core/vdbe/execute.rs`.

- **Mode.** The database gets a third mode beside WAL and MVCC. The `Database` holds an `AtollStore`,
  and a connection's transactions and savepoints go to it. An app server opens a database in this
  mode with a remote page source.
- **Cursor.** `AtollCursor` implements `CursorTrait` over table and index trees. It takes and
  returns SQLite record bytes, as every cursor does.
- **Tree ids.** `sqlite_schema.rootpage` holds tree ids. Tree 1 is `sqlite_schema`, because Turso
  assumes page 1 in about 60 places. A catalog tree maps each tree id to its root, node class and key
  layout.
- **DDL.**
  - CreateBtree allocates a tree id.
  - Destroy and Clear drop or empty a tree; garbage collection frees its pages once no snapshot can
    see them.
- **Header fields.** Schema cookie, user version, application id and the rest are rows in a system
  tree, versioned like data.
- **NewRowid.** A per-tree counter, seeded from the tree's largest key, safe for concurrent inserters.
- **Routing pager calls.** Where the VDBE and the connection call the pager (transactions, header,
  CreateBtree/Destroy/Clear, savepoints, checkpoint, integrity check, page counts), calls go through
  a storage-mode interface rather than a third set of branches beside MVCC's.
- **What changes meaning.**
  - `PRAGMA page_size` reports the node size.
  - `page_count` and `freelist_count` come from the page store.
  - The `dbpage` table is not supported.
  - VACUUM compacts trees.
  - `integrity_check` checks the trees, the page table and free space.
- **Index methods** (ivf_rq, fts and the rest) store their data in ordinary rowid and index trees
  through cursors, so they run unchanged. They get the snapshot timestamp as their snapshot identity.
- **PostgreSQL frontend.** It opens MVCC today and turns BEGIN into BEGIN CONCURRENT. It gains a
  switch to open Atoll databases.

## 18. Concurrency inside the engine

Turso runs a connection's statements on whatever thread steps them. IO completes either inside the
submission or later from `IO::step`. Therefore:

- **Callbacks** take no lock a submitter can hold, and nothing holds a lock across a submission.
- **Waiting.** Where waiting is unavoidable, the caller gets a completion (`IOResult`) and steps
  again.
- **Shared state** is atomics and short critical sections.
- **Background work** (page cleaning, checkpoints, garbage collection, segment preparation, uploads)
  is stepped like any operation. Connections drive it, or threads the embedder provides; Turso core
  starts no threads.

## 19. Proving it

- **Crash testing.** `core/io/crash.rs` reorders completions and injects IO errors and running out
  of space. It produces what a crash at any moment could leave: synced writes whole, everything else
  torn per sector.
- **Crash points.** Small workloads crash at every IO point; larger ones at random IO points.
- **Model.** After every recovery, the database is compared with a model of committed state:
  - committed transactions are all there, and uncommitted ones are not;
  - every snapshot read matches the model.
- **Followers.** An app server or replica at log position X reads exactly the committed state at X.
- **Isolation.** Turso's `concurrent-simulator` runs Elle over SQL histories.
- **Property tests:**
  - key encoding order against `compare_immutable`;
  - log and page decoders against corrupted input, which must report corruption and never panic.
- **TLA+** (Turso has a `tlaplus` directory): commit and visibility, slot reuse, optimistic commits
  from app servers, and recovery.
- **Benchmarks** against PostgreSQL on the same machine:
  - TPC-C, TATP and YCSB;
  - commit latency percentiles while the cleaner runs;
  - B-tree lookups with 100%, 50% and 5% of pages in memory, frame hints against hash lookups.

## 20. Order of work

1. **IO.** Direct IO, `fdatasync`, durable writes, preallocation, directory sync, and the crash IO.
   Done.
2. **Log.** Records, parallel fill, flushing, segments, failure, and readers by position.
   Done (`core/storage/atoll/log`).
3. **Page store.** Page kinds; the page table and free space as logged metadata pages; write
   intents and erasure; the control file. Done (`store.rs`, `meta.rs`, `radix.rs`, `control.rs`).
   Temporary pages move to step 7, which first needs them.
4. **Buffer manager.** Frames on huge pages, lent to size classes in batches; the hash table and
   frame hints; WATT replacement; fixes and prefetch; the page source for the local store;
   writeback and clean passes. Done (`buffer/`).
5. **Checkpoints and recovery** over the new pieces.
6. **B+-trees.**
7. **Transactions.** Slots, clock, commit logs, version headers, version index, graveyard, conflicts
   and waits, rollback, undo in recovery, garbage collection.
8. **Turso integration.**
   - the storage-mode interface;
   - `AtollCursor`;
   - the catalog;
   - header rows;
   - the PostgreSQL frontend switch.
9. **Object storage.** Uploads, remote page copies, manifests, restart anywhere, garbage collection.
10. **App servers and replicas.**
    - remote page sources;
    - following the log;
    - versions shipped with pages;
    - optimistic and forwarded writes.
11. **Raft.**

## References

- Leis. LeanStore: A High-Performance Storage Engine for NVMe SSDs. PVLDB 17(12), 2024.
- Alhomssi, Leis. Scalable and Robust Snapshot Isolation for High-Performance Storage Engines.
  PVLDB 16(6), 2023.
- Haubenschild, Sauer, Neumann, Leis. Rethinking Logging, Checkpoints, and Recovery for
  High-Performance Storage Engines. SIGMOD 2020.
- Alhomssi, Haubenschild, Leis. The Evolution of LeanStore. BTW 2023.
- Leis, Haubenschild, Kemper, Neumann. LeanStore: In-Memory Data Management Beyond Main Memory.
  ICDE 2018.
- Vöhringer, Leis. Write-Aware Timestamp Tracking: Effective and Efficient Page Replacement for
  Modern Hardware. PVLDB 16(11), 2023.
- Leis, Alhomssi, Ziegler, Loeck, Dietrich. Virtual-Memory Assisted Buffer Management. SIGMOD 2023.
- Neumann, Freitag. Umbra: A Disk-Based System with In-Memory Performance. CIDR 2020.
- Freitag, Kemper, Neumann. Memory-Optimized Multi-Version Concurrency Control for Disk-Based
  Database Systems. PVLDB 15(11), 2022.
- Mohan et al. ARIES. TODS 1992.
