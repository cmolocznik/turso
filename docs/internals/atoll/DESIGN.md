# Atoll

Atoll is a storage mode for Turso beside the SQLite pager and MVCC. LeanStore is its baseline, as
described in Leis, "LeanStore: A High-Performance Storage Engine for NVMe SSDs" (PVLDB 17, 2024).
Its buffer manager, its page writes and its log depart from LeanStore where the requirements below
force them to; section 2 lists each departure and why.

The code is in `core/storage/atoll`. Section 20 says which parts are built.

[UNIFIED_STORAGE_DESIGN.md](UNIFIED_STORAGE_DESIGN.md) proposes analytical execution with
DataFusion and DuckDB, Vortex column storage, transactional event streams, and OpenTelemetry
storage on the same foundation. It describes future work separately from the implementation
status here.

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
  - commit latency that checkpoints and page writeback do not wait in front of (section 8).
- **Turso's SQL layer:** everything keeps working:
  - rowid and WITHOUT ROWID tables;
  - every index and index method;
  - the SQLite and PostgreSQL frontends.
- **Torn writes.** Any write can tear, at any size. Nothing depends on the OS or the device writing
  atomically.
- **Erasure.** Deleted data leaves the files promptly (section 6.3). That is the file system's view:
  an SSD remaps writes, so the physical blocks can keep old data until the device reuses them, and
  the log, temporary files and replicas keep copies until they are removed themselves. A spare
  log segment keeps the records of its earlier life until new ones overwrite them (section 7.4),
  a segment's worth of logging later. Data that
  must not be recoverable from the device needs encryption with per-subject keys destroyed on
  deletion, which is not designed yet.
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
| Garbage collected when a slot is next claimed (section 11.5) | Each worker collects its own garbage between its transactions | Slots are not threads, and Turso runs no code between a step returning `Done` and the client's next call: collecting after a commit would hold the slot through the client's idle time. |

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
{db}.retired{k}.{l} retired-page marks of size class k at level l
{db}.log.{n}       log segments by number, and the next one prepared
{db}.temp{k}       temporary pages of size class k, deleted at every open
```

- **Data files** are split into files of a fixed size (a creation parameter), so none exceeds what a
  file system allows. ext4, for one, caps a file at 16 TiB.
- **The superblock.** The newest that reads back whole is in charge. It holds:
  - a magic number and the format version;
  - the database id (128 random bits);
  - the creation parameters;
  - its write sequence, the checkpoint in charge and its snapshot `C`, and the redo point `L`
    (section 10);
  - per class, the levels of its page table and free space as of the checkpoint's cut, which place
    their roots;
  - the clock as of the cut;
  - where the newest record of the gap chain starts in the log (section 7.4); until the chain
    lands (section 20), the gaps themselves, as many as fit: 248;
  - a CRC32C.

  It is written with a durable write. A torn superblock leaves the other one in charge. A
  superblock whole but of a version the build does not know fails the open rather than leave the
  one before it in charge.

## 5. Pages

- **Size classes:** 4 KiB × 2^k for k = 0..11, so 4 KiB to 8 MiB. B-tree nodes are 4 KiB unless a
  tree is created with a larger class. Larger classes hold large values.
- **Header (24 bytes):** `count: u64 | crc32c: u32 | class: u8 | kind: u8 | flags: u16 | pid: u64`.
  - `count` is the page's change count when the copy was taken (section 9.1).
  - The checksum covers the whole page with its own field taken as zero.
  - It is checked on every read from disk, from a node and from object storage.
  - A copy that fails it is torn, or corrupt, or a stale slot. It is never read as valid.
- **Kinds:**
  - page-table directory and map;
  - free-space directory and bitmap;
  - transaction slot table;
  - B-tree inner and leaf;
  - blob;
  - version arena, todo list and graveyard (temporary, section 11).

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
  slot is found without reading pages that have none, and so is every retired page. A page of
  zeros is all free, which is how every page starts.
- **Lookups** go straight to the leaf a page number falls in; only allocation descends.
- **Growing.** A tree grows a level at the top, the old root becoming child 0 of the new one. A
  page's place in its file is fixed by its level and index.
- **Two places per page.** A metadata page has two places side by side. It is always written to
  the place that does not hold its newest durable copy, so a torn write leaves that copy whole, and
  a read takes the newest whole copy numbered at or below the checkpoint in charge (section 10).
  This costs twice the space of the metadata, 0.4% of the data, and needs no free space, no parent
  pointers and no cascade of writes up the tree.
- **A cache of their own.** Metadata pages live outside the buffer pool, because a buffer miss
  needs the page table.
- **Changed live, written as of a cut.** A change takes the latches of the pages it changes, and
  is never logged. A checkpoint's cut keeps each page as it stood at its first change after the
  cut, and the checkpoint writes that copy (section 10).

### 6.3 The life of a slot

Page numbers and slots are taken and freed in memory, and nothing about them is logged
(`core/storage/atoll/store.rs`). A crash loses what memory held, and the checkpoint in charge is
the state.

1. **Taken.** A slot is taken for a write: a swap copy (section 9.5) or a checkpoint's image. A
   taken slot is claimed in memory until its write lands, so no other write takes it.
2. **Named.** A swap copy is named in an in-memory map, and a miss reads it first. An image is
   named in the page table as of its checkpoint's cut, and as the page stands once it lands.
3. **Freed.** A slot no copy needs any more is freed only once a durable checkpoint no longer
   names it: the slot of a copy a checkpoint replaced, or of a page freed, waits for the
   checkpoint begun after. A swap copy's slot, which no checkpoint names, waits only for every
   step that may have looked it up.
4. **Erased.** A freed slot is erased (a hole punched, or zeros written), each file synced once,
   before it is taken again: a deleted page's bytes leave the disk.

- **Numbers.** A page number freed is taken again only once a checkpoint begun after its free is
  durable, since the checkpoint in charge may name the page, and every step that may hold it has
  ended.
- **Pages nothing names.** A page an operation took and gave up, or a node a merge emptied whose
  free had to wait, is handed to the checkpointer (orphaned), which frees it in its next
  checkpoint.

### 6.4 Temporary pages

Version index, todo list and graveyard pages (section 11), and the spill of large sorts, live only
as long as the process does. They are never logged. When evicted they go to the temporary files,
which are deleted at every open, and recovery never reads them. Built in `buffer/temp.rs`:

- **Ids.** The top bit of a page number names a temporary page; the page store never sees one.
- **Place.** Each class has one temporary file, and a page's copy sits at its number times its
  size. A freed number is handed out again, and a page that was never written reads as zeros.
- **Checkpoints** pass over temporary pages: they are written only when evicted, and never owed an
  image.
- **Trees.** A B-tree whose root is a temporary page is a temporary tree
  (`Tree::create_temporary`): its nodes and out-of-line values are temporary pages.

### 6.5 Retired pages

The pages of an out-of-line value that a versioned write replaced stay, copies and all, while a
snapshot may still read the version they hold (section 11.2). The page store marks them retired,
in memory:

- **Marked with the change**, under the leaf's latch, and cleared by the rollback that puts the
  value back, which frees the pages of the version it removes.
- **Freed** by garbage collection, once every snapshot sees the change that replaced the value
  (section 11.5).
- **A crash** loses the marks, and needs none: a checkpoint maps a value's pages only where an image
  it writes as of its snapshot names the value (section 10), so pages only an older version named
  are free in the checkpoint, and no snapshot survives to read them.

## 7. The log

The log is the database. Page copies, the page table and every cache are materializations of it
that bound how much of it must be replayed.

### 7.1 Records

```text
len: u32 | kind: u8 | flags: u8 | index: u8 | size: u8 | txn: u64 | prev: u64 | payload | crc32c: u32 | padding to 16
```

- **Alignment and CRC.** Records start at multiples of 16 bytes. The CRC covers the record's
  position, its header and its payload, so a record read at another position, a torn record, or
  bytes a crash left behind never read as valid.
- **LSN.** A record's LSN is the position just past its end.
- **What is logged** is what recovery replays (audit 17): a transaction's changes by key, in
  parts published as its chunks fill, while it runs (`SPILL`, section 11.4.1), the last part in the
  Commit with its timestamp (`COMMIT`); changes made outside any transaction, such as a sequence's
  or a drain's, in a run of their own (`RUN`); a checkpoint's metadata intent (section 10). A
  transaction rolled back leaves its parts with no Commit, never replayed, and its rollback logs
  nothing. No page change is logged: a checkpoint writes the pages as
  of its snapshot, and recovery replays the changes past it (section 12).
- **Not built yet:** the record header still carries `prev` and a group's index and size, which
  nothing writes since page records went; they go with the log's next format change.

### 7.2 Filling the log in parallel

- **Reserving.** The log buffer is a ring of aligned chunks, each a frame the pool lends for as
  long as the log is open (section 9.1). By default it admits what the device may be writing at
  once, `writes_in_flight` chunks, and holds the chunk filling while those land: a stall longer
  than they take makes appenders wait for the device, as a full page cache makes WAL's writers
  wait, and every byte more would be a byte less of cache. An appender reserves space with one
  atomic add on the insert position and copies its record in, in parallel with other appenders.
- **Knowing what is complete.** Each appender announces, in a small array, where its copy in
  progress starts. It does so before reserving, with sequentially consistent ordering. A flusher
  reads the insert position and then the array, so it sees every copy still running below that
  position. It writes only the prefix every appender has finished.
- **In place.** A record built in a block of its own, a transaction's chunk of changes (section
  11.4.1), takes room at the next block boundary, the rest of the block before it padded in the
  ring, and the block is handed to the log: the flusher writes it from the block, never through the
  ring, and the write holds the block until it lands. The ring's chunks over those positions count
  it as copied and are left alone.
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
- **Padding is admitted.** Padding moves every record admitted and not yet appended along with it.
  Admission checked that those records fit in the prepared segments from where the log stood
  then, so padding that would push one past them waits until the next segment is prepared.
  Otherwise the record lands in a segment whose zeros are still being written, and the zeros land
  on it.

### 7.4 Segments

- **Layout.** The log is a sequence of segment files of S bytes. S is a creation parameter,
  256 MiB by default: at the 1 GB/s log rate Umbra measured (VLDB 2022 §4), that is a quarter of a
  second. LSN `l` lives in segment `l / S`, and records may span segments.
- **Preparation.** A segment is whole, allocated and synced, with its directory, before any record
  goes in, so later writes change no file metadata. Background work keeps the next one ready.
  - **From a spare.** A segment released (below) is renamed a spare, `{db}.log.spare{k}`, while
    one of the two spare names is free, and the next segment prepared is a spare renamed again:
    no byte is written to prepare it. Two, because preparation stays one segment ahead and so
    takes one spare per segment filled, and a release may land before the spare it follows is
    taken.
  - **Fresh.** With no spare, or a backend that cannot rename, a segment is created and written
    full of zeros. Only a database's first segments, and those after a crash removed the spares,
    cost these writes.
  - **Why a spare's old bytes are harmless.** A record's CRC covers its position (section 7.1)
    and padding names its own, so what a segment held before reads as the end of the log wherever
    it now lies, as a torn record does.
  - **Opening** takes back the spares on disk; a spare whose size is not a segment's is removed.
- **Removal.** A segment is released, kept as a spare or else deleted, once it is wholly below the
  redo point `L` the superblock in charge names (section 10), and, with object storage, once it
  has been uploaded. Nothing of a running transaction is in the log (section 11.4), so no undo
  holds a segment back.

  A segment holding a record of the gap chain that the superblock in charge still reaches stays
  too.
- **Reopening after a crash.** Appending resumes at the block boundary after the log's end.
  - The rest of that segment is zeroed and later segments removed, durably, so nothing a crash
    left beyond the end is ever read as log.
  - Bytes between the end and that boundary stay unreadable: a reader stops at the end.
  - Recovery then records the gap, before it changes anything else (section 12):
    1. once the zeroing is durable, it writes and syncs a gap record holding the new gap;
    2. it writes and syncs the superblock naming that record;
    3. only then does it make any other change.
  - A gap recorded before the zeroing lands would lead the next reader past it, into records the
    crash left there.
- **The gap chain.** Gaps live in a chain of records in the log, so their number has no limit, and
  opening recovers after any number of interrupted recoveries.
  - **Records.** Each holds at most one log chunk, links and framing included: its gaps, and the
    positions of the records 1, 2, 4 and so on before it, at most 64.
  - **Finding a gap.** A reader that reaches the end of what reads back whole at position p finds
    the gap starting there in O(log n) record reads, holding O(log n) positions. It validates each
    link's direction and each gap's range.
  - **How long a gap stays.** Until the redo point passes it, since recovery reads from there and
    goes past it too. A checkpoint rewrites the chain with only the gaps still above that floor. A record
    stays, with the segment holding it, until a durable superblock names its replacement.
  - **Status.** It comes with garbage collection (section 20). Until then the superblock holds up to
    248 gaps, and a gap stays until the redo point passes it.
- **Readers.** Recovery, replicas, app servers and the archiver read segments by position. A segment
  no longer local is fetched from object storage.

### 7.5 Failure

Any failed log write or sync makes the log refuse further appends and fails every waiter. The
database accepts no writes until it restarts and recovers.

A panic in Atoll ends the process (section 22.11), before anything it left half made can be read.
A thread that panics holding a latch, or between applying a change and recording what the change
leaves to collect (a todo item, section 11.5), can leave the log, a page and a slot's todo list out
of step, and nothing in the process may read them after that. Every build Turso ships already
aborts on a panic (`panic = "abort"`); where a build unwinds, every guard that releases shared state
aborts before releasing it, and every entry into Atoll carries an abort guard that an unwind cannot
pass. Recovery
at the next open then frees every page still marked retired and removes every tombstone left behind
(section 12), which also covers a rollback that could not finish.

### 7.6 Groups

A group is records appended under one reservation, read whole or not at all (`Log::append_group`,
`log/reader.rs`). Since page records went, nothing appends one; the machinery goes with the log's
next format change (section 7.1).

## 8. Commit latency

A commit appends its record and waits for durability: one device write in the local modes, one
round to a majority under Raft. Page writeback goes to other files and never waits on the log path,
so a commit waits on no page write. Both still share the device's queues and bandwidth, the file
system and the CPU, so writeback can lengthen a commit's write; section 19 measures commit latency
percentiles while the cleaner runs, rather than assuming it.

## 9. The buffer manager

Built in `core/storage/atoll/buffer`.

### 9.1 Frames

- **One region.** The pool is one anonymous region of the configured size, mapped without
  reserving memory and backed by huge pages where the OS allows. A huge page is the kernel's PMD
  size, read from `/sys/kernel/mm/transparent_hugepage/hpage_pmd_size`: 2 MiB with 4 KiB base
  pages, 32 MiB with 16 KiB (LoongArch's default), 512 MiB with 64 KiB (RHEL's aarch64). The
  region starts on one, only the whole huge pages inside it are advised, and the rest keeps base
  pages, so no huge page reaches past the region and none of its memory is committed past the
  configured size; a region smaller than a huge page keeps base pages throughout. It is carved
  into 2 MiB batches, a whole number to a huge page, and each batch is lent to one size class at
  a time: 512 frames of 4 KiB, down to one of 2 MiB. A 4 MiB or 8 MiB frame takes an aligned run
  of 2 or 4 batches from a buddy allocator.
- **One memory** (Clay's ruling of 2026-11-05). The region is the engine's memory: what does not
  cache a page is lent from it (`buffer/lent.rs`), a frame at a time, for a copy a checkpoint's
  cut keeps (section 10), a checkpoint's image of a node page until its write lands, and a chunk
  of the log's ring for as long as the log is open. A lent frame holds no page and is in no hash
  chain, so nothing evicts or moves it, a hint to it fails, and a batch holding one drains only
  once it is given back. A frame of the log's ring is marked lent for good, and a batch holding
  one is never chosen to drain: the ring's batch is the emptiest of its class, and a drain of it
  waits for good (measured: a long transaction's commit, needing frames of another class for its
  checkpoint's images, stalled there). Only a quiet frame is lent, as its holder writes it plainly; it goes back
  when its block drops, from whatever thread or IO callback drops it. Beside the region are what
  is fixed per process (code, stacks, connection and statement state), what scales with the
  region (the headers and the hash table), and three working memories, each bounded where it
  arises: the image of a value page being written and the copy it is made from (two pages of its
  class), the metadata pages a cut keeps (section 10, item 11), and the chunks of transactions'
  changes (section 11.4.1), a chunk per slot that has written, those a transaction fills before its
  next write, and the log's buffer's worth on their way to the device. Not built: a value page's
  image written from its frame, and metadata pages in lent frames.
- **Frame ids.** A frame is named by the 4 KiB grain it starts at, as a `u32`, so one pool holds
  at most 16 TiB. The id names the same memory whichever class holds it.
- **Never unmapped.** A freed frame is reused as it is. Memory moves between classes a whole batch
  at a time in the pool's own bookkeeping, with nothing unmapped or advised away, so no huge page
  is ever split.
- **Bytes as atomic words.** Optimistic readers may overlap a writer, so a frame's bytes are read
  and written only as relaxed 64-bit atomics (`shared.rs`; CONCURRENCY.md §3). A page is read by
  its IO straight into its frame, which needs the frame's memory quiet: no step that could still
  read its last page under way (CONCURRENCY.md §4). Nothing is copied on the way in.
- **Moving memory.** A batch moves once every frame in it is free. A batch to be moved is marked
  draining, which keeps new frames out of it. Its pages are then evicted: a clean page at once, a
  dirty one written out, its frame held until the write lands while whoever wants the page waits
  on that write as on a read. A page that keeps changing therefore still leaves, and no page ever
  moves to another frame (CONCURRENCY.md §6).
- **Headers.** Frame headers sit in their own array, one per 4 KiB, which is never unmapped, so a
  hint however stale always reads a header. Each header holds:
  - the versioned latch, whose version also rises on eviction;
  - the page id and the frame's state (free, loading, resident, being written out, lent);
  - the page's change count, the count its newest copy written holds (a swap copy or its
    checkpoint copy), and the count its checkpoint copy holds. The page is clean, and may be
    dropped, while the first two agree, and owed an image while the first and third differ
    (section 10); the checkpoint its cut found it owed, while that one runs;
  - writeback flags;
  - the link to the next frame in its hash chain;
  - WATT's timestamps;
  - the global epoch of the last step to fix the page.

  That is 104 bytes per 4 KiB frame, 2.5%.
- **Cost of memory.** Untouched memory costs nothing: the region, the headers and the hash table
  take physical memory only when used.
- **Minimum.** A pool is at least 10 MiB: one 8 MiB frame and one batch beside it. A database's
  budget holds that beside its log's ring, in whole batches, as a batch lent to the ring's class
  holds no other class's pages: 12 MiB for any ring of one batch, as every ring of chunks up to
  256 KiB with the default writes in flight is. A smaller one is refused at open.

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
- **Changes.** Every change is reported before the latch is released, which counts it and makes the
  page dirty; a pending page's first change after a cut first keeps its bytes for the checkpoint
  (section 10).
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
  clean ones at or below it are evicted, and dirty ones are written out of their frames, then
  evicted (section 9.5).

The paper divides by `now - t`, which is zero within the current epoch. Here elapsed time counts
the current epoch as one, so pages used this epoch still rank by how often.

**Between size classes.** When a class has no free frame and no free batch is left, it weighs
evicting its own least valuable page, its threshold divided by its page size, against draining a
batch of another class: of each class with a batch that can go to it (for 4 and 8 MiB pages, one
whose aligned run lies inside the region), the emptiest such batch, valued by the pages draining it
evicts, per byte it frees. A page read in the current epoch is in use, whatever its value: a page
read in again starts with no history and looks the coldest, and one operation needing it and a page
of another class would each evict the other for good. So is a page a step under way may hold: a fix
marks the page with the global epoch (CONCURRENCY.md §4), at or above its step's announcement, so a
page marked at or above the horizon may be held. A class holding memory drains a batch only if no
page in it is in use and it gives up less per byte than evicting its own. A class holding none must
drain some batch, perhaps one in use: it takes the batch with the fewest pages a step may hold, then
the fewest in use, then the least value. Draining a page the requesting step holds, the other page
of its operation, would only send that page's next fix to drain the memory back. Steps that ended
since the global epoch last rose look under way too, so the mark errs toward keeping a page.
Sampling and the choice skip batches already draining, and batches holding a frame lent for good.
Value per byte is
GreedyDual-Size's rule with every miss costing the same (Cao and Irani, 1997). A class of 4 or
8 MiB pages reserves an aligned run, and the batches in it drain into it as they empty. One drain is
in progress at a time.

Eviction removes the page from the hash table and bumps the frame's version, so every hint to the
frame goes stale.

### 9.5 Writing back

- A writeback takes dirty frames of one class and marks them writing, so they are neither evicted
  nor dropped meanwhile. Each takes a fresh swap slot (section 6.3), and on landing
  the slot is named the page's swap copy, the one it replaces given up. A swap copy is never made
  durable: it only lets the frame go.
- A page written out of its frame (a draining batch, an eviction) is held by its write-out until
  the write lands, so it is sealed in its frame and written from there: no copy, and no memory
  beside the frame. Whoever wants the page meanwhile waits on the write, as on a read. Eviction
  writes a dirty page out (one memory): a copy to write from would take a frame of its own, which
  a full pool has none of to spare.
- Only a test's sweep writes a page that stays in its frame, which may change while it is
  written: it is copied optimistically into a block of its own, checked against its latch, and the
  copy is written; a page changed while its copy is in flight stays dirty. The copies in flight
  stay within `writeback_bytes` (at least the largest page, two of it by default); a writeback
  finding no room submits no more until writes land.
- A write's completion only records that it landed and wakes the pool's waiters; the frame's new
  state is settled the next time the writeback is stepped.
- A writeback is stepped by whoever needs frames. Checkpoints write their images themselves
  (section 10), and never through a writeback.
- A failed page write, like a failed log write, stops the pool until the database is reopened.

### 9.6 Page sources

A miss asks a page source for page P in two steps:

1. **Locate:** whatever lookup the read needs first, which may wait on IO. Nothing is claimed yet.
2. **Read** into the claimed frame. What locate found must still hold. For the local store, the
   page-table leaf stays held in between, so every change to the page's entry reaches it. Once
   the pool has claimed the page, nothing can change its entry until the read is done.

The local store's source reads the slot the page table names, verifies its checksum and page id,
and gives a page with no copy yet as zeros. A copy that fails verification is corruption, never
retried.

Still to come, for app servers and replicas (section 15):

- a read for page P no older than log position X;
- answers carrying the source's log position, and, from a database node, the versions a reader at
  X needs and the history of the page's key range from the lower of the holder's floors up
  (section 15). Object storage holds page copies alone, so a page read from it is brought forward
  by the log and takes that history from a node, or the read runs there;
- sources that are a database node, or object storage;
- batched misses, because a network fetch costs about 100 µs–1 ms against about 50 µs for NVMe.

## 10. Checkpoints

A checkpoint (audit 18) writes every page owed an image as of one snapshot `C`, the metadata as of
the same cut, and a superblock naming them, so recovery replays the log from the cut's position
`L`, applying only the commits above `C` (section 12). Pages are never logged. Built in
`core/storage/atoll/checkpoint.rs`, the pool's part in `buffer/mod.rs` (the cut, the copy hook,
settling), `buffer/kept.rs` (the room for kept copies) and `buffer/lent.rs` (their frames), the
store's in `store.rs` (the cut's metadata edits, the frees that wait for a checkpoint).

1. **Owed and clean are two facts.** A frame counts its changes. It is clean while a slot holds
   exactly its bytes, its swap copy or its checkpoint copy, and only a clean frame may be dropped.
   It is owed an image while it differs from its checkpoint copy: changed since, or last imaged
   unsettled. A page evicted while it is owed goes to a swap slot, which a miss reads first and no
   checkpoint or recovery reads.
2. **The cut**, with the store's change gate closed, so every change of several pages lies wholly
   before it or wholly after: `L` and `C` (section 11.4: `C` is the clock as the cut reads it, the
   horizon waited for up to it); every resident page owed an image marked pending, under its
   latch; then every page with a swap copy; the metadata cut. Eviction goes on meanwhile, so every
   page owed stays listed until it is imaged or kept, and one read in again comes back pending.
   Trees whose creation `C` does not see are set aside; pages handed to the checkpointer before
   the cut (orphans) are taken then, and freed first.
3. **The copy hook.** A pending page's first change or free after the cut keeps its bytes for the
   checkpoint. A value's page freed while a cut is in effect is not freed: an image the cut owes
   may name the value as the cut found it, and reads its pages. It waits, named by nothing, for the
   next checkpoint. A value's page is written only before a leaf names its value, so no image of a
   cut that found it unwritten names it, and its change keeps nothing; nor does a child hint's,
   which every reader checks, nor the checkpoint's own free of an orphan its cut left out.

   **The room for kept copies** (Clay's ruling of 2026-11-05) bounds the copies of data pages; the
   metadata's are accounted apart (item 11). Each copy is in a frame the pool lends (section 9.1),
   charged to a room of `max(R, min(B/8, 256 MiB))` bytes, B the budget and R the largest request a
   change makes, three node pages of 64 KiB, an initial choice that `kept_bytes` overrides. The
   ruling's floor was a page of the largest class, P; it was set while the copies lived beside the
   frames, and with them in the frames it would be most of a small pool. A charge is given back only
   when its copy drops and gives its frame back, wherever it then is. A change latches every page it
   will change and asks for room for the copies they need before its first edit, all or none; an
   edit of a page the cut owes that asked for none fails an invariant check rather than copy past
   the limit. A request that finds no frame of its class free without an eviction is turned down as
   one past the room is: the change lets go of what it holds, takes a frame holding nothing, which
   may evict a page, and goes on with it. Only node pages are kept, 64 KiB at most, and a change
   asks for three at most, so every request fits. Refused, it lets go of its latches and the change
   gate, and the writer steps the checkpoint itself (or, with the checkpointing thread, waits for
   the room it frees: item 9), as an embedded database may have one connection and no other thread,
   until the room takes what it asked for, and no further. While a writer waits, the checkpoint
   images kept copies first. How many images that takes is not bounded by the request: with pages of
   several sizes, or other writers taking the room given back, it can take more; the time writers
   wait is measured, the longest wait with it. While someone else steps the checkpoint, the writer
   waits on the room, woken by every copy given back and by every stepper leaving. A free never
   waits: with no room, the page goes to the checkpointer unchanged since the cut, imaged as it
   stands, and the next checkpoint frees it.
4. **Images.** Each pending node page is written to a fresh slot: a versioned leaf as `C` reads it
   (each key at the version `C` sees, settled, deleted keys gone), any other as it stood at the
   cut. A node of a tree set aside is left out. Then each pending value page is written if an
   image written names its value, and left out if none does. Every page owed an image is a node or
   a value's, its writer setting its kind before the change that makes it owed; any other is
   corrupt. The page table names each copy as of the cut, and as it stands
   once the copy has landed. An image settles its page, which is then owed nothing, clean, and its
   swap copy given up, when it is the page as it stands, or when every version the leaf holds is
   seen by the oldest snapshot running and by `C`, so that the image reads to every reader as the
   leaf does: a snapshot older than `C` that reads below a version the leaf holds would read the
   image's settled version instead. Each slot publishes a timestamp at or below its transaction's
   snapshot, 0 while it takes one, before it reads the horizon, so a checkpoint reading the slots
   after its cut misses only snapshots taken after, at or above `C`.
5. **Metadata.** A record naming the metadata pages it writes is made durable first; each page is
   then written as of the cut, numbered with the checkpoint, to the place not holding the copy in
   charge.
6. **Durable.** Each data file is synced once, and the log made durable to its end.
7. **The superblock** `(n, C, L, clock, heights, continuations)` is written durably: checkpoint
   `n` is in charge.
8. **After.** The frees that waited for `n` are done: numbers reusable once no step holds them,
   slots erased before they are taken again, each file synced once. The creations it mapped are
   forgotten, the log before `L` released, and `C` let go.
9. **When.** A checkpoint begins once half the log budget has been appended since the last began,
   or when a full one is asked for. The log budget is how much log recovery may have to replay; by
   default 0.4 times the pool, the ratio of Haubenschild's example.

   **Who** (Clay's ruling of 2026-11-05: off the commit path). Where the platform has threads, a
   thread of the database's own steps every checkpoint (`Checkpointing::Thread`), waiting on its
   writes so they go as fast as the device takes them, and parks while none is due. A writing
   statement and a transaction's end only wake it once one is due, doing none of its work; a
   writer refused room for kept copies waits for the room the thread frees. Past twice the log
   budget behind the log's end, and as far behind the first part of a transaction still running
   (section 11.4.1), a transaction's end waits for a full checkpoint, as it always has, but the
   thread does it. The database starts the thread once open and ends and joins it
   when it closes; should the thread end otherwise, writers step checkpoints again. With
   `Checkpointing::Writers`, for a platform without threads or a test that steps checkpoints
   itself, a writing statement steps the checkpointer only once one is due (section 17), a
   refused writer steps it itself, and a transaction's end past twice the budget does the full
   checkpoint itself. Every holder of the checkpoint's state takes it through one guard
   (`StateHold`), which lets it go and then wakes the thread and the writers waiting for room, so
   no hold leaves the thread parked with a checkpoint due. Section 22 moves both modes onto the
   executor's workers and callers.
10. **Failure.** A checkpoint that cannot finish leaves the one in charge in charge. What it marked
    imaged, and the page table entries it named as they stand, are not undone, so none runs again
    until the database is opened again and recovers from the one in charge. Its cut ends and its
    copies are dropped; a writer that was stepping it for room goes on, as commits do, and the
    failure is told to whoever steps checkpoints next. A write that lands short is a failure.
11. **Memory**, in the one region (section 9.1) or beside it, each amount accounted: the kept
    copies and the images of node pages in lent frames; the image write buffers in flight within
    `checkpoint_write_bytes`, one page of the largest class by default, beside the 32 writes, so a
    large page's writes go fewer at once (32 small writes keep a device busy up to pages of
    256 KiB; a larger write is itself several of the device's commands); the page being imaged,
    one at a time, read live and built into its image beside the region (two pages of its class
    at most); and the metadata pages kept as a cut found them, outside the room. No one waits for
    those, so the checkpoint always reaches the writes that drop them; they are a page each, kept
    once a cut, so they grow with the metadata, which nothing here bounds. Each is reported with
    its peak (the bytes lent by use among them), with the refusals, the time writers waited and
    the longest wait, and the checkpoint's pages, bytes and stepping time.

## 11. Transactions and versions

The MVCC is OSIC (Alhomssi and Leis, VLDB 2023; LeanStore 2024 §2.6).

### 11.1 Slots, the clock and visibility

Built in `txn/mod.rs`, except what garbage collection brings (section 20):
- the clock's ceiling and reserve, and the timestamp a rollback draws;
- the commit-log latch, and the compaction of commit logs with its test of whether a writer
  committed;
- the snapshot words and the watermarks;
- the OLTP and OLAP classes;
- a begin collecting its slot's garbage;
- 32-bit statement numbers, which are 16 bits until then.

Remote snapshots come with step 10.

- **Slots.** There are W transaction slots. W is the most transactions open at once, 100 by default,
  PostgreSQL's default `max_connections`. A transaction holds a slot from begin to end, so a slot
  runs one transaction at a time. A begin takes the lowest free slot, or waits for one if every slot
  is taken, and runs a step of collection on it before its transaction starts (section 11.5).
- **Clock.** One global atomic clock issues timestamps, each unique: starts, commits, and the
  timestamps rollbacks draw (section 11.5). A transaction's start names it in its log records,
  which chain to its previous record.
  - **Ceiling.** Timestamps stop at 2^62 − 2, the largest a snapshot word holds (below). That is
    146 years at a billion a second.
  - **Reserve.** An ordinary draw, by a begin or a commit, is refused when W or fewer timestamps
    remain, so that every slot can still roll back.
    - The check and the draw are one compare-and-swap on the clock. A refused or failed draw
      consumes nothing, and clears any drawing state it installed.
    - A rollback draws from the reserve, once across all its resumptions, and keeps the timestamp
      in its state.
  - **Recovery** undoes a database at the ceiling, since undo draws no timestamp, and rejects any
    value above it.
- **Commit logs.** Each slot keeps a commit log: its committed (start, commit) pairs, in order.
  - A version written by (slot s, start t) is visible to a snapshot at time T when the last
    transaction of s committed at or before T started at or after t.
  - A snapshot caches that answer per slot, so a check consults another slot only for versions that
    slot wrote.
  - **Writing.** A slot's log changes only under its latch, a versioned latch only ever tried: a
    commit appending its pair, a compaction publishing its prefix, and a tail being sealed (below).
    A commit takes it before it marks its slot, and holds it until it has published its pair by
    the log's length (section 11.4). Readers search up to that length, and take no latch.
  - **Compaction** keeps only the pairs a snapshot can still ask for (OSIC §3.1):
    - the base, at or below which every transaction of the slot committed;
    - the predecessor of each final snapshot published;
    - the predecessor of each provisional bound, and every pair above it;
    - the predecessor of each remote holder's floor (below), and every pair above it. The holder
      may use any snapshot from its floor up without announcing it.
    - the predecessor of a horizon sampled before the words are scanned, and every pair above it.
  - **Why the keep set covers later snapshots.** Any snapshot that appears after the scan is at or
    above either the sample or a bound kept with its whole suffix. A begin that loads its final
    snapshot before the sample and publishes it after was provisional at the scan, or loaded at
    or above the sample. When a snapshot was published says nothing about how old it is.
  - **Retention has no bound.** These pairs are unbounded in number while any of three things
    lasts:
    - a publication is stalled;
    - a commit has drawn its timestamp but not published its pair, holding the horizon back while
      other slots publish above it;
    - a remote floor is old.

    Retention is reported with its cause and age, and no bound in W is claimed.
  - **Segments.** The log is a chain of segments, and a pair is immutable once published.
    - **Build.** A compaction cuts at a segment boundary below the published length. It builds a
      compacted prefix of the segments before the cut outside the latch, over as many bounded
      steps as it takes, holding owning references to the segments it captured. A slot's builds are
      serialized under its collector claim (section 11.5).
    - **Publication.** Under the log's latch, a build first checks that the log's prefix is still
      the one it captured, and discards itself if not. Otherwise it swaps the prefix for its
      compacted one, an O(1) step.
    - **The suffix.** The segments after the cut, where appends land, are shared, not copied, so no
      append is lost. Readers search the prefix, then the suffix, and the old prefix goes through
      the epochs.
    - **Tails.** A partly filled tail segment that holds pairs to reclaim is sealed under the
      latch, so the next append opens a new one.
  - **When, and at what cost.** A compaction is due once max(K, W) pairs have been added since the
    last, K being the pairs that one kept.
    - It sorts the sampled words once and merges them with the log's N pairs: O(N + W log W), which
      is O(log W) per commit on average.
    - A slot's log is compacted whether the slot is free or occupied. Its builds are serialized
      under its collector claim, and only their publication and the sealing of a tail take the
      latch.
    - A background compaction of an idle slot runs when the slot's protection changes, and is
      charged to the maintenance driver (section 11.5).
  - **Whether a writer committed.** A dropped pair can no longer say whether its transaction
    committed, and no threshold may stand in for it. A version rolled back and read as committed
    could report a conflict, or a key as present to an insert, without reaching a conditional
    leaf check.
    - The writer of a version is open if the slot's running word is its start. The running word is
      read first.
    - Otherwise the leaf is read again under its latch, and the version was committed if it is
      still there, since a rollback puts every version back before its slot is free.
    - A writer found gone, its version no longer there and the writer not running, is what a
      write retries on (section 11.3).
- **The published horizon.** A snapshot is the horizon H: every commit timestamped at or below H
  is in its slot's commit log. A commit raises H once its pair is published, as far as every
  commit below it has been, and is acknowledged once H covers it. So a snapshot taken after the
  acknowledgement sees it, and no reader waits. A commit marks its slot before it draws its
  timestamp, so H never passes one not yet published (CONCURRENCY.md §8).
- **Snapshot words.** Each slot publishes one word for garbage collection. V1 lays it out as:
  - bits 0–61, the snapshot plus one, so that 0 is a slot without one;
  - bit 62, an OLAP flag;
  - bit 63, an active flag, set once the snapshot is final rather than a provisional bound.

  Publication works as follows:
  - **A begin** loads the floor word and stores it as its provisional bound, then passes a
    sequentially consistent fence. It then loads the floor word again and the horizon, and stores
    the larger as its final snapshot.
  - **A scanner**, computing watermarks or compacting, loads the horizon and raises the floor word
    to it, then passes a sequentially consistent fence. Only then does it load the words.
  - **Why it holds.** Each side is a store, a fence and a load (CONCURRENCY.md §3), so one of the
    two fences comes first. Either the scan finds the provisional bound, or the begin finds the
    floor at or above the sample and takes a snapshot at or above it. A floor is a horizon a
    scanner read, so every commit at or below it is published.
  - A provisional bound counts as a snapshot that old.
  - Under read committed, a statement updates the word the same way once the previous statement's
    cleanup has finished. A retained read context (UNIFIED_STORAGE_DESIGN.md §4.1) keeps its
    snapshot published until its cleanup has finished (UNIFIED_STORAGE_DESIGN.md §8.2).
  - The word is cleared once the transaction reads no more.
- **Watermarks.**
  - **What a scan gives.** One scan of the words and the remote floors gives the oldest snapshot
    and the oldest OLTP snapshot. Each is the horizon sampled before the scan where nothing is
    older. The remote floors count in the population that the costs of scans and compactions are
    given for.
  - **Caching.** They are cached for everyone, and a stale value only delays collection.
  - **When a scan runs.** A pass refreshes them by a scan with probability 1/W. OSIC refreshes
    after each transaction (§3.1), where Atoll runs nothing.
  - **On an empty pass.** A pass that finds nothing eligible also scans, but only if whatever gave
    the cached minimum, a slot's word, a remote floor or the horizon, has changed since. Until it
    does, no scan can raise the watermark. Scanning on every empty pass would scan on nearly every
    begin, since a pass usually finds the previous transaction's garbage still seen by older
    snapshots.
  - **The newest OLAP snapshot**, which OSIC uses to prune the middle of fat tuples' chains
    (§3.3), comes with fat tuples (section 11.2).
- **OLTP and OLAP.** A transaction's class is fixed at its begin, before its snapshot is
  published, and never changes.
  - OLAP is read-only, enforced in storage: no writes to versioned trees and no row locks, though
    temporary trees may be written. A writer would need the graveyard to see a conflicting delete
    (section 11.2). A transaction that wants to write starts a new one.
  - Every read of an OLAP transaction merges the graveyard: scans, point lookups and fetches from
    an index to its row.
  - A long OLTP transaction is never demoted: its writes would miss tombstones that have moved.
  - An analytical read inside an OLTP transaction is OLTP. How SQL declares the class comes with
    step 8 (section 20) and the user documentation, with any difference between frontends on the
    `Dialect` trait.
  - What a snapshot holds back is reported with its class and age. A limit ends the transaction
    with an explicit error, and never weakens its snapshot.
- **Remote snapshots.** Followers, both app servers and replicas (sections 15 and 16), hold leases
  at the primary.
  - **Registration takes a fresh snapshot.** A holder's first snapshot is taken fresh at the
    primary, through the snapshot-word protocol. No snapshot older than one issued at that moment
    is ever admitted, so there is no historical admission to validate, in either class.
  - **Floors.** The holder then keeps a floor per class, starting at that snapshot.
    - The primary counts each floor in its watermarks: an OLTP floor in both of them, an OLAP
      floor in the oldest snapshot.
    - Compaction keeps a floor's whole suffix (above).
    - The holder issues snapshots only at or above its acknowledged floor. It raises the floor
      only through a new acknowledged registration, and never past its oldest live read context,
      cleanup not yet finished included.
    - Optimistic remote writers hold OLTP floors.
  - **Losing a lease.** A floor's expiry or limit ends the executions that depend on it with an
    explicit error, never a newer snapshot. A primary restart, or a change of leader, invalidates
    every lease, since temporary history does not survive it.
  - **When a follower may read.** With each snapshot S, the primary returns where its log stood at
    S: the transaction's `log_at`. Every commit at or below S has its Commit record before that
    position, so a follower reads at S only once it has applied the log through it.
  - Built with step 10 (section 20).
- **Statements.** A transaction sees what its own earlier statements wrote, and not what its
  current statement writes. Under read committed each statement takes a new snapshot. Statements
  are numbered by a 32-bit counter, the width of PostgreSQL's command counter, and a transaction
  past 2^32 − 1 statements fails.

### 11.2 Tuples and versions

Built in `txn/versioned.rs` and `txn/arena.rs`, except what garbage collection brings (section
20) and the graveyard.

- **Header.** Every entry of every B-tree, table or index, starts with a 24-byte header:

  ```text
  slot: u16 | flags: u8 | updates: u8 | command: u32 | ts: u64 | prev: u64
  ```

  - `ts` is the writer's start timestamp.
  - `command` is the statement within the writer's transaction.
  - `updates` counts updates while the oldest snapshot stays the same, for fat tuples; it is 0
    until they are built (section 20).
  - `flags` mark a deleted entry or a lock with no new data.
  - `prev` names the arena entry holding the version below this one (below), 0 for a key that had
    no entry before.
- **Old versions.** Where Atoll departs from OSIC's version index, a B-tree each worker keeps
  (Alhomssi and Leis §3.1): a version below the newest is found by a pointer, not a search.
  - **Version arenas.** Each slot keeps an arena: an append-only run of temporary pages of 64 KiB
    (`txn/arena.rs`). A write appends one entry before its leaf write: the statement, the tree's
    root, the key, and the entry the leaf held, header and all, or that it held none. An
    insert's entry is its key alone, which rollback reads.
    - The version the write makes names the entry in `prev`: a reader whose snapshot does not see
      it follows the pointer, with no search, and an insert of a new key names none, so a reader
      learns the key was absent from the header alone.
    - Appending is one exclusive fix of the slot's last page and a copy: no tree, no descent, no
      split. A page is taken when the last one is full. An entry is written by its slot's
      transaction alone and never changes once a version names it, so readers read it
      optimistically, with no latch.
    - A statement writing a key again keeps the entry its first write made, which holds what the
      statement found: every read, the statement's own included, resolves to that, so the
      versions in between are read by no one.
    - An entry's page holds the largest entry of any node class (a key and an inline entry, each
      under a quarter of a 64 KiB node), so no key is shorter for its version's sake: a versioned
      tree's longest key is the tree's own (section 13).
    - Entries hold the whole version replaced; deltas come next (section 20).
  - **Tombstones keep an inline value.** A delete changes the version header alone, so the leaf
    and the log change by 24 bytes and a key, not a row (section 7.1), and a reader below the
    delete reads that value with the header it replaced. The space goes when garbage collection
    removes the tombstone (section 11.5). A value out of line goes at the delete, its pages
    retired.
  - **Reading.** A reader starts from the leaf's version and follows `prev` while a version is not
    visible to it. A version with none ends the chain.
  - **Values out of line.** An entry holds the replaced version's header and either its value or
    the reference of a value out of line. Those pages are then retired (section 6.5). A version its
    own statement replaces again is the exception: no reader can reach it, since every read, the
    statement's own included, resolves to what the statement found (a statement sees only what
    earlier statements wrote, section 11.1). So its pages are abandoned at once.
  - **Fat tuples.** A tuple updated very often would keep its recent deltas inline instead (OSIC's
    fat tuples, §3.3), with the pruning of a chain's middle that the newest OLAP snapshot allows.
    They come with deltas (section 20). OSIC judges the `updates` count against the oldest snapshot
    it was counted under, a 2-byte field this header lacks; it goes into the V1 header when fat
    tuples are designed. A tree may choose its head length, as a property of the tree, but never to
    keep a tree of an older format readable: there is one format.
- **Graveyard.** Each versioned tree has one: a temporary tree keyed by the tree's key and a
  generation, a class larger than the tree's. It holds tombstones as their leaf held them,
  header and all, one entry for each deletion of a key still needed. OSIC keys it as the tree is
  (§3.2), where a newer tombstone of a key would have to wait for an older one to leave.
  - **Moving.** A committed tombstone moves once the oldest OLTP snapshot sees its delete, that
    is, once the deleting writer's start is visible to it; an open or rolled-back delete never
    moves.
    - **One step under two latches.** A move holds the tree leaf's latch, then the graveyard
      leaf's, both tried, with room in the graveyard leaf and log space prepared first. It
      checks that the tombstone is still the key's newest version, and takes the generation after
      the key's newest in the graveyard. It inserts the tombstone there, then logs its removal from
      the leaf, behind the delete's commit.
    - **Starting over.** A latch it cannot take, or preparation it lacks, lets the tree leaf go and
      starts over. No latch is held across IO.
    - **Why generations follow deletions.** An insert over the tombstone takes the leaf's latch too,
      so it lands either before the move, which then moves nothing, or after it. A key's later
      tombstone exists only after such an insert. So generations follow the order of deletions
      however collectors are scheduled.
  - **Cleanup**, once the oldest snapshot sees the delete, removes the graveyard copy. The leaf and
    the graveyard never hold the same version where anyone can see it: the duplicate exists only
    inside the move.
  - **Reading.** OLTP never reads the graveyard; every read of an OLAP transaction does
    (section 11.1).
    - A key resolves by the leaf's chain first, then its graveyard entries newest generation first,
      each with its chain, stopping at the first version visible to the snapshot.
    - It yields one row across both trees. A reader that reads the leaf before a move, and the
      graveyard after it, meets the same version twice.
    - A graveyard entry of a key is older than the leaf's entry, or, met across a move, the same
      version; anything else is corruption. A graveyard entry is never dropped because the leaf
      holds a newer incarnation of the key.
  - **Scans** merge per frozen range: a scan reads a leaf's fence keys, keeps them, and probes the
    graveyard with exactly those bounds, since a merge may change the leaf's.
  - **Followers**, app servers and replicas alike, keep a graveyard of their own (sections 15
    and 16).
    - A removal record carries the exact version removed.
    - A follower moves that version into its own graveyard before applying the removal whenever
      a snapshot it may take, any at or above its OLAP floor, still reads it: it may take one
      without announcing it (section 11.1), so open snapshots alone do not decide.
    - It assigns its own generations, which keep the primary's order, since removals reach it in
      log order.
  - OSIC covers neither several deletions of one key nor a key in both trees; these rules do.
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
- **A version found gone**, no longer in the leaf and its writer not running, means the key
  changed since the writer read it, and the writer yields and reads it again (section 11.1). A
  version can leave and come back, when a rollback puts it back, so finding it gone again is no
  corruption. This comes with the compaction of commit logs (section 20). Until then no pair is
  dropped, a committed version always reads as committed, and a writer reads the key again at
  once, reporting a version found gone twice as corruption.
- **OLAP.** An OLAP transaction writes no versioned tree and takes no row lock (section 11.1).

### 11.4 Commit

1. Take the slot's commit-log latch, by trying: a busy latch is a yield before anything has
   changed. Mark the slot, draw the commit timestamp and append the Commit record.
2. Publish the pair in the slot's commit log, let the latch go, and raise the horizon. The
   transaction is visible to every snapshot taken once the horizon covers it.
3. Free the slot. Its garbage waits for the slot's next claim or a background pass
   (section 11.5).
4. Acknowledge once the horizon covers it and the durability mode is met.

A transaction that saw this commit has its own commit later in the one log, so it can never be
durable without this one. A read-only transaction acknowledges only once the log is durable up to
where it stood at its snapshot.

**Changes by key** (audit 17). A transaction keeps its changes by key as it makes them
(`txn/changes.rs`): each put and delete with its tree and statement, each tree it creates, with
plain stores into chunks of its own (section 11.4.1). A statement rolled back drops its changes.
Each chunk filled goes to the log as it fills, in a Spill record carrying the transaction's start;
the Commit record carries the last after its timestamp. Changes made outside any transaction, a sequence's
value and a tree drained, go out at once in Run records. Replaying the bodies in log order through
the trees (`replay.rs`) rebuilds every tree: the replay oracle checks it against the database that
published them, and recovery replays them past the checkpoint in charge (section 12).

Step 1's latch comes with garbage collection (section 20). Until then a commit marks its slot
without it.

#### 11.4.1 The redo buffer (Clay's scope of 2026-11-06)

The changes are encoded once, into chunks of the log's chunk size, the largest single write, so a
full chunk goes to the device in one write. Measured before this design (200,000 rows of 100
bytes, transactions of 100 rows): the change buffer's appends cost 1.7% of an insert's
instructions (about 240 a row: the encoding's per-field growth checks, and the buffer's
reallocations as it doubles), the commit 1.2% (about 160, almost all the CRC), the flush 0.6%.
This removes part of the ordered-write gap to WAL, not all of it: splits and version management
stay as they are.

- **Chunks.** A chunk is heap memory, aligned for direct IO, of one size for the database, lent
  from a free list the database keeps and given back to it by whoever drops it last: the
  transaction, or the log once its write from it has landed. It is not a frame of the pool: lent
  from a pool sized to its minimum, the chunks idle slots keep took the frames a checkpoint's images
  wait for, and the checkpoint never ended (measured: 3 runs in 8 of the SQL tests hung or fell
  behind). A change starts in a chunk only if
  its head and key fit in what is left of it, so the key of every change is whole in one chunk; a
  value longer than what is left runs on into the next chunks. A chunk keeps room at its front for
  the header of the record it becomes and at its back for the record's CRC and the padding to its
  block's end, so it is published where it lies.
- **No growth, no allocation per transaction.** Each slot keeps its changes buffer from
  transaction to transaction, a chunk and its lists' room with it: a transaction that fits in the
  chunk allocates nothing and touches nothing shared for its buffer. A chunk filled is sealed, and
  the next taken from the free list. The free list keeps as many chunks as the log's buffer holds
  unflushed, and one more; the heap so holds a chunk per slot that has written, those a transaction
  fills before its next write, and the log's buffer's worth.
- **Spilled as they fill.** A sealed chunk is published at once, while its transaction runs, as a
  `SPILL` record of the transaction's start, the position in its changes where the part begins,
  and the part. Published means: room reserved at the next block boundary, the rest of the block
  before it padded, the record's header and CRC written into the chunk, and the chunk handed to
  the log. The flusher writes it from the chunk itself, never through the ring; the chunk goes
  back to the pool when its write has landed. If the log cannot admit it without waiting, it waits
  in the chunk until the next write begins (before any latch) or the commit, either of which may
  wait. A transaction so holds one chunk, plus any filled by one change longer than a chunk.
- **The commit** copies what is in the slot's chunk into its Commit record in the ring, a copy of
  at most one chunk; commits smaller than a block share blocks there, as group commit needs. The
  Commit record carries the commit timestamp and where its part begins.
- **No position held while a transaction is still writing.** A chunk takes its room in the log
  only once it is full, with every byte of it written: its record is complete when its room is
  taken, so nothing an idle transaction holds leaves a hole that keeps later commits from being
  durable.

Three obligations come with publishing parts before the commit:

1. **A checkpoint keeps the parts of a transaction that commits after its cut.** A slot is marked
   with where its transaction's first part lies when that part takes room (as a commit's first
   part was before), and the mark stays until the transaction ends. The cut's redo point is at or
   below every mark, so recovery reads every part of a transaction that commits past the cut, and
   the segments holding them stay. A transaction open long after spilling so holds the log back,
   as an open transaction holds a WAL; the end of a transaction waits for a full checkpoint only
   when one could bring the redo point within twice the budget, never because a mark holds it.
   A transaction that clears its mark before the cut's scan reaches its slot drew its timestamp
   before the cut reads the clock: at or below `C`, its parts may lie before the redo point and
   its Commit past it. Recovery skips it whatever it finds of it, keeps nothing of a transaction
   whose first part it never read, and refuses a commit above `C` missing a part (audit
   2026-11-06).
2. **A savepoint rollback leaves out what it discarded, even on disk.** Every part carries the
   position in the transaction's changes where it begins. A rollback to a statement whose changes
   begin before what has been spilled continues from that position: the next part, or the Commit,
   begins there. Recovery joins parts by position, each part cutting what was joined at its own
   start before adding to it, and refuses a part that would leave a gap. Bytes spilled past a
   rollback point are so overwritten or cut off by the Commit's part, and never replayed; the parts
   of a transaction rolled back whole have no Commit and are never applied. The rollback reads back
   the changes it puts back: from the chunks in memory, and the spilled parts from the log, once
   the log is durable up to them, one part at a time, newest first.
3. **An acknowledged commit has every part durable.** A transaction's parts are published before
   its Commit takes room, so they lie before it in the log, and the log is durable as a prefix: a
   commit acknowledged once the log is durable up to its Commit has every part durable. A chunk
   handed to the log is the log's until the write from it lands; the write holds it, so even a log
   dropped mid-write leaves it alive until the device is done with it.

Redo only does not remove what live snapshots and rollback read: versions keep the old values they
replace in the version arenas (section 11.2).

**Not built yet:** the commit still copies its last chunk into the ring, however full; publishing
a full last chunk in place, as a part is, waits on its measurement.

### 11.5 Rollback and garbage collection

Rollback is built in `txn/rollback.rs` and `txn/mod.rs`, except three parts, which come with
garbage collection (section 20): the timestamp and cutoff of the end's steps 1 to 3, the
re-recording of a tombstone put back, and the background rollback of a transaction abandoned open.
Until then the end frees the slot at once. Of garbage collection, the version indexes' drop by range
is built (`txn/arena.rs`, below); the todo lists, retired references, tombstones and commit-log pairs
are not built yet.

- **Rollback.** A running transaction rolls back from its own changes by key (audit 17), which
  name every key its statements wrote, in order, each by its statement, so walked back they come
  newest statement first across every tree. Nothing is read from the log. A write appends an arena
  entry only when it replaces an entry: an insert of a key with none costs no arena entry at all.
  - For each key a statement changed, the leaf's latch is taken and the key's entry checked: it is
    put back only if the leaf holds that statement's version. A statement writing a key again
    keeps the entry its first write made, so the key is put back once, at its newest change.
  - Then the entry the version's header names below it goes back whole from the arena, version
    header and all, or the key is removed if the header names none.
  - A write the creating statement made plainly into its own tree is never put back by key: the
    tree goes whole when the creation is undone.
  - An entry put back that holds a value out of line clears its pages' retired mark, and the pages
    of the version removed are freed (section 6.5).
  - **Nothing logged.** Putting a key back is logged nowhere: the changes undone never reached the
    log (section 11.4), and a crash loses the running transaction with memory.
  - **The end.** Once undo, and the cleanup of its retained read contexts, are done, a full
    rollback:
    1. marks its slot, draws a timestamp e (section 11.1) and publishes its cutoff, its start and
       e, in the slot's state;
    2. clears the slot's horizon mark, since a mark holding e keeps the horizon below e;
    3. tries to advance the horizon and wakes its waiters, any wait for earlier committers
       happening here, after the mark is cleared, with no latch held;
    4. frees the slot, without a commit pair.

    Every aborted version is gone before the slot is freed: once the slot's next transaction
    commits, the visibility rule would show one left behind as committed.
  - **Savepoints and statement rollback** stop at a statement. Statement numbers only rise: those
    rolled back are never used again, no arena entry is removed, since a reader may be following
    one, and the changes of the statements rolled back are dropped from the transaction's own.
  - **A tombstone put back.** A rollback that puts back a tombstone another transaction committed
    re-records that transaction's todo item (below).
    - The item goes in the deleting slot's list, under the deleting transaction's start and
      statement, the key it had. Its tombstone part is pending again, and its revision is set anew.
      It keeps the item's retirement progress, or is recreated as tombstone-only if the item is
      gone.
    - The rollback adds the item to the tree, and then lowers the slot's rewind word, an atomic
      minimum. The slot's
      collector swaps the rewind word out at the start of each step and moves its cursors back to
      it.
    - The item is eligible by the deleting version, and never waits for the rolling-back
      transaction.
    - A tombstone the rolling-back transaction itself deleted, in an earlier statement, needs
      nothing: that statement's own item still stands.
  - **Before it starts,** whatever else works on the transaction has stopped and cleaned up, and
    the capacity rollback needs (log room, memory, IO) is not given to work that could hold it
    (UNIFIED_STORAGE_DESIGN.md §4.1, §8.2, §11.2).
  - **Abandoned.** A transaction dropped open would leave its slot running, and writers that meet
    its versions would wait forever. The embedder or adapter hands it to a background rollback
    instead, as an operation dropped with pages in hand hands them to the checkpointer
    (section 6.3).
- **Garbage collection** is cooperative (OSIC §3.1, §3.5). Its work belongs to the slot whose
  transactions made it, so a dropped connection loses none.
  - **Todo lists.** Beside its version arena, each slot keeps a todo list, a temporary tree keyed
    `ts | command | tree | key`.
    - It holds one net item per transaction, statement, tree and key. A start and a statement
      belong to one live transaction of the slot, so a statement writing a key again updates the
      same item.
    - An item holds a retired reference, if the statement replaced a value others could reach
      (section 6.5); a tombstone as the leaf holds it, if the statement's last
      change of the key left a delete; or both.
    - It also holds a revision: a number drawn when it was last set, which never repeats.
    - This is OSIC's tombstone index (§3.2), widened to retired references.
  - **Recorded where the change lands.** The item goes into slot-owned state under the leaf's
    latch, in room reserved before the latch, and into the todo tree at a safe point. Nothing that
    can do IO or fail sits between a change and its item. Once its change is in, a put can still
    fail freeing what it abandoned, and a remove can still return for IO, merging.
  - **Rollback** removes the items of the changes it undoes. The retired marks are cleared by the
    rollback itself (section 6.5); the item is separate.
  - **The collector claim.** Each slot has a collector claim: one word taken by compare-and-swap,
    separate from a transaction's ownership of the slot.
    - **Held only within a step**, never across a returned IOResult.
    - **Progress lives in the slot**, read and written only under the claim: the slot's two cursors,
      the next page of an item being freed, and a compaction being built (section 11.1).
    - **So nothing is lost or repeated.** A pass that is dropped, or no longer polled, loses
      nothing, and no free is repeated. A store free either happens whole or returns IO having
      changed nothing, and progress records a page only once it is freed.
  - **Who collects.**
    - **A begin** takes the lowest free slot by the usual exchange, runs one step of a pass on it
      under its claim, and only then hands it to its transaction. It also runs one step on the
      next slot in turn, in the same way; that slot may be in use, or its own. (Built for the
      arenas of section 11.2 only.)
    - **Background passes** take the claim of any slot, free or occupied. The owner meanwhile
      appends its own entries above the eligible range: the keys are disjoint, and the trees' node
      latches order the changes.
    - **Never right after an acknowledgement.** Turso runs no code between a step returning `Done`
      and the client's next call. Collecting in the commit would add latency once durability is
      met, and fresh garbage is mostly not yet eligible, since older snapshots still see it.
  - **Work per step.** A step does at most one log chunk of records, and visits or drops at most one
    leaf of entries. Frees resume per page, so a large value spans steps.
    - Background passes are driven by a maintenance driver the embedder provides, a thread or its
      connections. Atoll starts no thread for them, so without a driver nothing is collected in
      the background, only at begins. The executor of section 22 is to be that driver (22.9
      item 3).
    - Twice the rate garbage arrives is the driver's pacing target, not a guarantee.
  - **What is guaranteed.** Fair, repeated, bounded visits eventually drain a finite eligible
    backlog once arrivals cease, commit-log pairs included. That holds provided IO makes progress
    and no protection still holds the garbage. No round count is promised; one would have to be
    derived from the work outstanding.
  - **Two cursors.** A slot's items lie in timestamp order, and what is eligible rises with the
    timestamp.
    - One cursor works at the oldest snapshot. It frees retired references, and removes tombstones
      and their graveyard copies.
    - The other moves tombstones, at the oldest OLTP snapshot.
    - Each stops at the first item not yet eligible under its own watermark, so a blocked pass costs
      O(1). An item records which of its parts is done.
    - Nothing waits on anything but its watermark. A tombstone with an open transaction's version
      on top is finished at once, since only that transaction's rollback can put it back, and that
      rollback re-records it (above).
  - **Completion is conditional.** A collector removes an item, or marks a part done, only by a
    conditional change expecting the revision it read when it decided.
    - One that loses that race to a restoration reads the item again.
    - A page it already freed stays freed: its slot-owned progress is the authority. It merges that
      progress into the item at the new revision rather than freeing the page again.
  - **Version indexes** are dropped by range, without reading the entries. Built: each slot's
    arena pages go oldest first, a page once every snapshot (the checkpoint's cut among them) sees
    the transaction that last appended to it (each page records that transaction's start), never
    the page the slot appends to. A begin takes the step, for the slot it took and the next slot in
    turn, freeing twice as many pages at most as the slot took since its last step; the slot's own
    transaction appends meanwhile only to its last page, so the collection needs no more of the
    slot than its claim. A rolled-back transaction's pages go once a later transaction of the slot
    is seen; the extension below, through the slot's latest rollback, is not built.
    - The range takes every entry whose `ts` is at or below the slot's last committed start that the
      oldest snapshot sees.
    - It extends through the start of the slot's latest rollback, once every published snapshot is
      at or above that rollback's timestamp e. Every earlier commit of the slot is below e, since
      it committed before that transaction began. So the earlier committed entries in the range
      are needed by no one, and a reader that could still follow a rolled-back entry took its
      snapshot before e.
    - Without that extension, a slot whose last transactions rolled back would keep their entries
      until it next commits, and for good if it never does.
    - The range covers transactions aborted, statements rolled back and writes that never
      happened. A drop frees the index's own pages and nothing its entries point to, since no
      entry spills (section 11.2).
  - **Retired references** are freed once the oldest snapshot sees the change that retired them,
    through the buffer pool (section 6.5): a value's chunks before its directory page, each page
    once. Progress is kept per page, since a freed number can be handed out again and retired by
    another write, and a retry that freed a page if still retired would free the new value's.
  - **Tombstones** are removed from their leaf once the oldest snapshot sees their delete, and
    moved to the graveyard once the oldest OLTP snapshot does (section 11.2).
    - A removal is a remove outside any transaction that expects exactly that version, logged
      nowhere: the snapshot of a checkpoint under way is among those it waits for, and an image
      drops a key its snapshot reads deleted anyway (section 10).
    - Where the leaf's newest version is another committed one, the tombstone has left the leaf
      already and that part is finished.
    - A moved tombstone's item stays until the oldest snapshot sees the delete, and its cleanup then
      removes the graveyard copy.
  - **A crash** loses the todo lists and the retired marks with memory, and needs neither: a
    recovered database holds no tombstone and no page only an older version named (sections 6.5,
    12).

### 11.6 Isolation levels

- **Read committed** (PostgreSQL's default), and **repeatable read** as snapshot isolation.
- **Serializable** comes later: PostgreSQL-compatible SSI, or precision locking.
- **In the SQLite frontend,** BEGIN and BEGIN CONCURRENT both mean snapshot isolation; every
  transaction may write concurrently.

## 12. Recovery

Built in `core/storage/atoll/recovery.rs` and `replay.rs`. Opening a database (`db.rs`) creates it
if there is no superblock, writing one with checkpoint 0, and recovers it otherwise; a database
whose system trees an open makes writes a checkpoint before any write (section 17).

1. **The checkpoint in charge.** Read the newest whole superblock. The metadata in charge is each
   page's copy numbered at or below its checkpoint. A metadata page a later checkpoint, cut short,
   named in its intent record may hold a torn copy in its other place: it is owed a write, and the
   next checkpoint, which takes the same number, writes over it.
2. **Scan** the log from `L` to its end: the roots that replayed creations name, kept from every
   other allocation; which drained roots the mapping holds; the clock; the intent records; and
   where the log ends, the first record that does not read back whole. Gaps the superblock records
   (section 7.4) are followed. If reopening past the end would leave a gap the superblock has no
   room for, recovery fails here, before it changes anything.
3. **Replay**, in log order: every committed body whose commit timestamp is above `C`, joined from
   its parts, and every run (`Apply`). A body at or below `C` is in the checkpoint's pages already,
   and is skipped even with its first parts before `L` (section 11.4.1, obligation 1). Until its
   Commit, a transaction's parts are kept as where they lie, never their bytes, joined at their end
   or cut by a rollback by halving; at its Commit each is read back alone, in one read of exactly
   the blocks it lies in, so recovery holds one part and one change at a time, as a writer does.
   A put lands settled, since no snapshot survives a crash; a delete removes its key; a drain of a
   tree the replay made, or the mapping held, frees it whole.
4. **Record the gap.** If the log reopens past its end, it is zeroed from there durably and the gap
   recorded in a superblock before anything is appended.
5. **Restart the clock** above every timestamp read: the superblock's clock, every transaction's
   start and every commit timestamp replayed. Every version left in a page is committed.
6. **Checkpoint.** A full checkpoint makes what was recovered the state in charge.

No analysis of losers, no undo, no compensation, no page LSN: nothing of a transaction reaches the
log before its commit (section 11.4), and no page change is logged at all.

**Threads.** Recovery starts no threads. Any number of threads may call `Opening::work`: one
recovers while the rest wait for it.

Recovery reads at most the log budget, plus what was appended while the last checkpoint ran, twice.

## 13. B+-trees

Built in `core/storage/atoll/btree`, except the roots' kind and the conditional remove, which come
with garbage collection (step 7).

- **Nodes** are slotted (LeanStore, BTW 2023 §4; `btree/node.rs`):
  - fence keys: the lower exclusive, the upper inclusive, a missing one infinite;
  - the fences' common prefix stripped from every key;
  - a 4-byte head of each key in its slot, so a search compares integers until heads tie;
  - a hint array of 16 heads sampled over the slots, one cache line, narrowing the binary search.
    An insert rewrites only the hints from the first it can have moved, while their spacing holds
    (LeanStore, ICDE 2018), so an append rewrites one at most.

  ```text
  slot: off: u16 | key_len: u16 | value_len: u16 (top bit: out of line) | head: u32
  ```

  A node names its tree by its root's page id, and holds its level, its right neighbour (a leaf) or
  rightmost child (an inner node), with a frame hint beside it.
- **Roots.** A root records whether its tree is versioned or plain, and keeps it through every
  growth and shrink. A transaction's changes name the tree by its root alone: replay meets a
  root's trees in log order, a drain between them, and a root freed is taken again only after a
  checkpoint begun after its free, so a replay never makes a second tree at a root it freed
  (sections 6.3, 12).
- **Node size.** 4 KiB unless the tree chose a larger class, up to 64 KiB, the most 16-bit slots
  address.
  - Every entry, slot included, takes at most a quarter of the node past its header, so any two
    entries fit beside the fences and a split always makes room. Keys are at most that less a
    slot and a value reference: 957 bytes in a 4 KiB node, and 933 in a versioned tree's, whose
    leaf entries keep their 24-byte version header beside the key.
  - **Values out of line** (`btree/value.rs`). A value whose entry would pass a quarter goes into a
    blob page of the smallest class that holds it. Beyond the largest class it is cut into 8 MiB
    chunks under a directory page; a billion bytes, SQLite's limit, takes 120 chunks. Every page
    of a value carries a random id, so a reader that followed a reference before the value was
    replaced finds out and reads the leaf again. Its pages are written provisional, in log records
    of at most one log chunk, and adopted by the record that puts the reference in the leaf. A
    value replaced in a plain tree is abandoned in the group that replaces it; in a versioned
    tree it is retired instead (section 6.5).
- **Child entries** hold the child's page id and a frame hint (section 9.2). A descent that finds a
  hint stale refreshes it if it can latch the parent at once, keeping the latch's version, since no
  reader trusts a hint.
- **Neighbour hints.** A leaf holds its right neighbour's page id and frame hint. A scan follows it
  only if that frame still holds the neighbour, of the same tree, a leaf, whose lower fence is this
  leaf's upper, and not marked abandoned by a merge; otherwise it descends with the fence. Splits
  and merges need not keep the hints right.
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
- **Latching:** optimistic lock coupling (LeanStore, ICDE 2018 §IV-F).
- **The caller's leaf.** A cursor, a put and a transaction's write keep the leaf they last reached,
  with its frame hint, and try it before a descent: a leaf of the tree that answers the key, by
  the route the descent would take, is the one leaf that does. It answers a key at it from above
  its lower fence to its upper, included, and the keys just above one from its lower fence,
  included, to below its upper. The caller holds the hint, so nothing another thread reads is
  written. An append's split keeps every entry of the left leaf where it is: it writes the new
  upper fence and compacts only when the fence would not fit.
- **A look kept.** A look at a key's leaf (where the key is or goes, and its entry) can be kept past
  its step with the leaf's page, frame and the latch version it read. Kept, it holds no latch; it
  is given back while the frame still holds the leaf at that version, since equal versions mean no
  change between, and a put goes in from it as from the look. A probe of a key keeps its look, so
  the read and the write of the key that follow reach no leaf again (section 17).
  - A descent fixes each node without a lock and validates the parent after finding the child, so
    readers take no latch. A child freed meanwhile fails the parent's validation, never the read.
  - A writer latches its leaf alone and checks under the latch that the leaf covers its key.
  - A structure change is its own step: it descends again and latches exactly the nodes it changes,
    then the operation that needed it starts over. A busy latch is a cooperative yield, and no
    latch is held across a returned `IOResult`.
- **Structure changes**, each inside the store's change gate, so a checkpoint's cut finds it whole or
  not begun (section 10):
  - **Split:** the new right node's image, the left's truncation to its first entries and a new
    upper fence, and the parent's new separator, with the old pointer moved to the new node. A
    parent without room is split first. Separators are the shortest key between the halves.
    Appending at the right edge of the tree, the left keeps everything, so keys inserted in order
    fill nodes, unless its new upper fence would not fit beside them; a split is taken only where
    both halves fit, else nearest the middle. Both halves' fences share at least the node's
    prefix, so each entry moves as one run of bytes: the right half is appended straight from the
    node, and the left is cut down in place.
  - **The root never moves**, so its page names the tree. When it splits, its contents move to a new
    child and it becomes an inner node over that child, which then splits like any other; when it
    has one child left, the child's contents move up.
  - **Merge:** a node used less than a quarter after a removal merges with a neighbour, the right
    one first, if the two fit in three quarters of a node, which leaves room for the largest entry.
    The left goes into the right, and the parent loses the separator between them; a parent left
    underfull merges next, and a parent with one child merges at its own level. The merged fences
    may share less, so keys grow: the node is built in a page of the thread's own from both nodes
    and copied over the right one.
- **Records** (`btree/record.rs`): init (a node's image), insert, remove, update, split and branch.
  Each changes one node and is applied by the same code when made and when replayed.
- **A conditional remove** outside any transaction is garbage collection's: it removes a key only
  if the leaf still holds exactly the version it expects, and its record carries that version
  (section 11.5).
- **Later:** contention splits.

## 14. Object storage

- **What lives there:**
  - **Sealed log segments.** The open segment's tail is also uploaded in chunks when commits wait for
    the upload.
  - **Page copies,** packed by the cleaner into large immutable objects. Uploading one is followed by
    naming its remote place in the next checkpoint's metadata.
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

Built with step 10 (section 20). The rules below are requirements on its design, which goes to
review in full before it is built.

- **Same engine.** An app server runs the same engine with a memory budget and no disk. Queries
  whose working memory would exceed the budget run on a database node instead.
- **Following the log.** It follows the log stream and applies each record to the page only if it
  holds that page. From the commit records it builds the slots' commit logs. Its queries read at the
  log position it has applied.
- **Fetching a page.** A page fetched for position X comes with the versions of its entries that a
  reader at X needs: the old values of changes not committed at X. They come from the node's version
  index.
  - The page is valid at X if its last change is at or before X.
  - If it is newer, the app server first catches up to the page's position. If it is older, as a
    copy from object storage may be, the log is applied to it up to the app server's position
    first, its segments read by position (section 7.4).
  - **History the leaf no longer shows.** A fetch, or a refetch after an eviction, also supplies
    the history over the page's key range that any snapshot at or above the lower of the app
    server's floors may read: the graveyard entries, those of keys the leaf no longer holds
    included, and the chains of predecessors behind every version. A holder may take any snapshot
    from its floor up without announcing it (section 11.1), so the history covers that whole
    interval, not only the snapshots open at the fetch. Otherwise the key or range is read at the
    primary, at the same snapshot.
  - **Where it goes.** The history is cut at or past both the page's position and the app server's.
    It replaces the app server's graveyard entries over the page's range, in the primary's order,
    and removals replayed after it take later generations. A replayed removal of a version the
    graveyard already holds moves nothing, so no deletion is lost or recorded twice. Since the cut
    is at or past the app server's position, the entries it replaces are among those it brings,
    or needed by no snapshot at or above the floor.
  - **Chains.** The chains a fetch supplies, and the predecessors replay adds from the entries the
    log's updates carry, are kept with the leaf or graveyard entry they hang from. They count in
    the budget, leave memory with it, and are supplied again by a refetch.
  - Replay alone cannot supply it: the app server applies no record to a page it does not hold,
    so a key deleted and moved before its leaf is first fetched leaves nothing in its own
    graveyard. The snapshot's lease keeps that history at the primary (section 11.1).
- **Snapshots are leased** at the primary (section 11.1).
  - An app server's first snapshot is taken fresh there. It then holds a floor per class that only
    rises. The primary counts its floors in its watermarks, and its compaction keeps each floor's
    predecessor and every pair above it.
  - An OLAP floor does not hold back the primary's moves of tombstones into its graveyard, and a
    move is logged as the tombstone's removal from its leaf. An app server that replays that
    removal on a page it holds, while a snapshot at or above its OLAP floor may still read the
    version, first moves the version into a graveyard of its own, as a replica does (section 16).
  - Every read at an OLAP snapshot merges that graveyard, as reads at the primary merge theirs
    (section 11.2).
  - A query reads at a leased snapshot only once the app server has applied the log through the
    position the primary returned with it.
  - A lease that expires, or is lost when the primary restarts or the leader changes, ends the
    queries that depend on it with an explicit error.
- **Evicting** a page of the database writes nothing: the app server never writes one back, since
  the primary holds every change it replays. After an eviction a refetch supplies the page's
  history again (above), as long as the lease holds.
- **An app server's graveyard** is memory like any other, counted in its budget. With no disk, an
  evicted graveyard page is dropped, never written to a temporary file (section 6.4).
  - Dropping one marks the key range it covers as lacking its history. The next read at an OLAP
    snapshot over that range fetches the range's history again, or reads at the primary, even
    while the range's leaves stay resident.
  - A graveyard page with no copy is never read as empty.
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
  - Its snapshots are leased at the primary like an app server's (section 11.1).
  - It keeps its own version arenas, from the entries the log's updates carry, and its own
    graveyards (section 11.2). A removal the primary's collection logs carries the exact version
    removed. A replica moves that version into its own graveyard before applying the removal while
    a snapshot at or above its OLAP floor may still read it (section 11.2).
  - A replica holding part of the database, which fetches a page it lacks, takes the page's
    history with it by the app server's rule (section 15).
- **Raft** (later):
  - the Raft log is the log, and the leader is the node that writes;
  - a commit is acknowledged once a majority holds it durable;
  - the cluster's other members are replicas, and app servers follow the leader.
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
- **What a connection keeps.** Per tree, the catalog row its transaction read and the leaf its last
  cursor stood at, and per header field the word its transaction read, so a statement's cursor
  open and its schema check read nothing. A transaction's snapshot sees no other transaction's
  write, so what it read holds until a write of its own or a rollback of its statements, each of
  which the catalog and the header count; a row or word read by another transaction is not used.
  The leaf is a hint (section 13), kept across transactions.
- **One reach per row.** An insert's KeyProbe keeps its look at the key's leaf (section 13).
  NotExists answers from it when the newest version there is none, or a tombstone the statement
  sees, and the Insert writes from it, while the leaf is unchanged; any other answer is a seek's.
- **What a statement keeps.** At its reset a statement parks each Atoll cursor in its slot: what
  it waited on is withdrawn from the wait graph, and the leaf it stood at is left to the
  connection. The next execution's open of the same tree in the slot takes the cursor up again:
  at no row, with no move or write under way, its catalog row checked again against the one the
  statement reads, and built afresh where they differ. An open of another tree drops it. A cursor
  keeps its tree's rowid counter, so an insert looks it up once.
- **Rows built once.** A write's key and entry are buffers the cursor hands round: the entry is
  built once, with room for the version header, then stored in the leaf and the log from there,
  and the cursor shows the row it wrote from that entry, handing back the row it showed before
  for the next write. An insert that splits no node allocates nothing.
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
  is stepped like any operation. Connections drive it, or threads the embedder provides. The one
  thread Atoll starts is a database's checkpointing thread, where the platform has threads (Clay's
  ruling of 2026-11-05, section 10 item 9). Section 22 designs the executor that is to run all of
  it, its wakes owned in one place.

## 19. Proving it

- **Crash testing.** `core/io/crash.rs` reorders completions and injects IO errors and running out
  of space. It produces what a crash at any moment could leave: synced writes whole, everything else
  torn per sector.
- **Crash points.** Small workloads crash at every IO point; larger ones at random IO points.
- **Model.** After every recovery, the database is compared with a model of committed state:
  - committed transactions are all there, and uncommitted ones are not;
  - every snapshot read matches the model.
- **Rollback and recovery.** Crash images every few IO operations through each of these, and
  through the recovery of each, recovered to exactly what was committed, checkpoints running
  between (`txn/rollback/tests.rs`, `tests.rs`):
  - a rollback, and a rollback to a savepoint after which another transaction changes and commits
    a key put back;
  - a transaction's created trees, gone after a crash or a rollback, a committed one's kept;
  - a settle, and the writes that took its room;
  - all of it with values out of line, replaced, rewritten in one statement, deleted and
    inserted: every recovery leaves each page reachable or free.
- **Checkpoints.** A crash anywhere, checkpoints stepping between changes, recovers a plain tree
  exactly as the checkpoint in charge cut it (`btree/tests.rs`), and a drain from the tree the
  checkpoint in charge left (`btree/drain/tests.rs`). A cut's snapshot is the clock it read,
  never a commit drawn after (`txn/tests.rs`). A checkpoint that fails leaves the one in charge,
  and none runs until a reopen recovers (`tests.rs`).
- **The page store.** A number freed is taken again only once a checkpoint begun after its free is
  in charge; a slot a checkpoint stops naming holds its copy until that checkpoint is in charge,
  then is freed and erased; a latched metadata page makes its users yield (`store/tests.rs`).
  - **The clock.** With W = 1 and exactly one timestamp left, a begin is refused and a rollback
    still draws; also concurrent draws at the boundary. A rollback's mark is cleared before its
    slot is freed, so a new owner's mark is never cleared by the old one.
  - **Statements.** A transaction past 65,535 statements rolls back to savepoints, commits, and is
    recovered after a crash. That covers the widening in headers and index keys, and in the
    statement a rollback to a savepoint stops at.
  - **The graveyard**, against the model:
    - OLAP scans and point reads through moves, several deletions of one key, and merges changing
      fences in the middle of a scan.
    - A collector delayed after reading an old tombstone, while that tombstone is overwritten and a
      newer one moves.
    - OLAP writes refused.
    - Every graveyard copy is removed once the oldest snapshot sees its delete.
    - A follower keeps a moved tombstone while a snapshot at or above its OLAP floor may read it.
  - **D.**
    - A reservation paused before its item is buffered while a collector advances.
    - A flush with no scan under way, then a checkpoint before the next scan.
    - A flush behind a scan's position, then the scan publishing.
    - The next minimum in an evicted leaf, the scan reading the temporary file while an item is
      flushed and its pending entry cleared.
  - **Leases.**
    - A request for a historical snapshot gets a fresh one.
    - A registration racing the end of the last protecting reader, and the collection after it.
  - **Index entries.** At the largest class, maximum keys with values out of line, and values at
    the inline threshold, never spill.
  - **Crashes** through all of it:
    - a recovered database holds no tombstone and no page only an older version named, whatever
      the collection had reached;
    - more gaps than one record holds, and crashes around each step of publishing the chain.
  - **Failure.**
    - A panic in the middle of a change ends the process, and the next open recovers.
    - A transaction abandoned open is rolled back in the background.
- **Followers.** An app server or replica at log position X reads exactly the committed state at X.
  - An app server holds an OLAP snapshot and has never fetched a key's leaf. The primary deletes
    the key and moves its tombstone, and the app server then fetches the leaf. A point read and a
    scan at the snapshot still find the key, also when the key was inserted again after the move.
  - The same with the leaf fetched, evicted, and fetched again after the move.
  - The same with a graveyard page evicted while its leaf stays resident.
  - A leaf fetched while only a newer snapshot is open, the key deleted and moved between the
    floor and that snapshot, and a query then taking a snapshot between the floor and the delete.
  - The same with the leaf already resident when the removal is replayed, so the version moves
    into the local graveyard though no open snapshot needs it.
  - A partial replica fetching a page it lacks, in the first case's schedule.
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

CONCURRENCY.md rebuilds the concurrency of steps 4 to 6 before step 7 goes on; its own order of
work says which parts are done.

1. **IO.** Direct IO, `fdatasync`, durable writes, preallocation, directory sync, and the crash IO.
   Done.
2. **Log.** Records, parallel fill, flushing, segments, failure, and readers by position.
   Done (`core/storage/atoll/log`).
3. **Page store.** Page kinds; the page table and free space as logged metadata pages; write
   intents and erasure; the control file. Done (`store.rs`, `meta.rs`, `radix.rs`, `control.rs`).
   Temporary pages move to step 7, which first needs them.
4. **Buffer manager.** Frames on huge pages, lent to size classes in batches; the hash table and
   frame hints; WATT replacement; fixes and prefetch; the page source for the local store;
   writeback. Done (`buffer/`).
5. **Checkpoints and recovery** over the new pieces: opening a database, checkpoints, recovery.
   Built first as continuous checkpoints over shard passes with physical redo of page records;
   replaced by the plain checkpoint and replay of the logical changes (audits 17, 18:
   `checkpoint.rs`, `recovery.rs`, `replay.rs`, `db.rs`).
6. **B+-trees.** Nodes, values out of line, optimistic lock coupling, splits, merges and root
   changes, and cursors. Done (`btree/`). Key encoding moves to step 8, with the cursor that encodes.
7. **Transactions.** Temporary pages and temporary trees (done: `buffer/temp.rs`); slots, clock,
   commit logs, visibility, commit and restarting the clock (done: `txn/mod.rs`); version headers,
   version arenas of whole versions, conflicts and waits (done: `txn/versioned.rs`,
   `txn/arena.rs`); rollback (done: `txn/rollback.rs`); values out of line in versioned trees,
   and retired pages (done:
   `txn/versioned.rs`, `store.rs`). Next, garbage collection, without the newest OLAP snapshot as a
   watermark:
   - the clock's ceiling and reserve, and 32-bit statement numbers (sections 7.1, 11.1, 11.2);
   - the timestamp a rollback draws, the cutoff it publishes, and the end's order of release
     (section 11.5);
   - snapshot words, watermarks, OLTP and OLAP (section 11.1);
   - the commit-log latch, and segmented compaction with its test of whether a writer committed,
     and a writer's retry on a version found gone (sections 11.1, 11.3);
   - todo lists with revisions, the collector claim and its passes, and the two cursors
     (section 11.5);
   - the drops, frees, removals and restorations those passes make (section 11.5);
   - the graveyard, its moves and cleanup, and OLAP reads through it, with no index entry spilling
     (section 11.2);
   - root kinds, and the conditional remove (section 13);
   - the gap chain (section 7.4), and the checkpoint's rewrite of it (section 10);
   - the abort guard on every entry into Atoll (sections 7.5 and 22.11);
   - the background rollback of a transaction abandoned open (section 11.5).

   Remote snapshots, and followers' own graveyards, come with step 10.

   Then update deltas and fat tuples, with the pruning of a chain's middle that the newest OLAP
   snapshot allows (OSIC §3.3).
8. **Turso integration.**
   - the storage-mode interface;
   - `AtollCursor`, and the key encoding for SQL records (section 13);
   - the catalog;
   - header rows;
   - the PostgreSQL frontend switch.
9. **Object storage.** Uploads, remote page copies, manifests, restart anywhere, garbage collection.
10. **App servers and replicas.**
    - remote page sources;
    - following the log;
    - versions shipped with pages, and the history of a page's key range from the lower of a
      holder's floors up (section 15);
    - leases for remote snapshots, and followers' own graveyards (sections 11.1, 11.2);
    - optimistic and forwarded writes.
11. **Raft.**
12. **Streams** (section 21): a partition's batch append and read, recovery, and whole-segment
    retention (built: `stream.rs`); then segment index files, the stream's memory budget, the
    catalog in trees, transactions with streams, and consumer and producer state.
13. **The executor** (section 22): designed on 2026-11-06 and revised after seven audits; built
    through step 2 of its own order of work, 22.16.

## 21. Streams (Clay's spec of 2026-11-06)

A stream is a kind of storage beside the trees: immutable batches of messages appended to the
partitions of a stream, whose bytes as appended are their permanent copy. The trees keep what
changes around them: stream definitions, each partition's segment catalog, consumer positions and
their ownership epochs, producers' sequence numbers, a queue's delivery state, and the tables a
consumer updates. A message is never inserted into a tree, never versioned, and never rewritten by
a checkpoint.

### 21.1 A partition is a log of its own

Each partition is a log as section 7 builds it, under a base of its own
(`{db}.stream.{stream}.{partition}`): segment files prepared ahead and written whole, durable writes,
padding per flush, readers by position, and failure as there. Its records are batches, of one kind,
`BATCH`. The payload path is the producer's batch, to its final place in a segment, to a consumer's
lease of it, and the payload is written once.

- **Batch framing.** A batch's payload is read from disk as it is sent to a consumer, so storage
  and transport share one framing:

  ```text
  base offset: u64 | count: u32 | flags: u32 | first time: u64 | last time: u64 |
  producer: u64 | sequence: u64 | messages | ends: count × u32
  ```

  `ends` gives each message's end within the messages, so one is found without a scan; it follows
  them, so a batch is built in one pass, its count known only at its end. The record's
  CRC (section 7.1) covers the batch; a torn batch ends the partition's log as a torn record ends
  the log. Times are the producer's, in microseconds; `producer` and `sequence` let a retry be told
  from a new batch (section 21.6).
- **Built where it is written.** A producer builds a batch in a chunk the stream lends
  (`append_batch(partition, batch)`), framed with room for the record's header and CRC, as a
  transaction's chunk is (section 11.4.1). A batch of at least a block is published in place: the
  log writes it from the chunk, and the chunk goes back once its write lands. A smaller one is copied
  into the partition's ring, so small batches share blocks.
- **Offsets are assigned at publication, in order per partition.** Publishing takes the
  partition's lock, writes the next offset into the batch as its base, takes the batch's room in the
  log, and lets go: offsets rise with log position, and none is held by a batch not yet published.
  The lock is taken once a batch. The append is done once the partition's log is durable past the
  batch, and answers its offsets.

### 21.2 Reading

`read_batch(partition, offset, bytes)` answers a lease of whole batches, from the one holding
`offset`, up to `bytes` (at least the one): one buffer the stream lends, filled by one read of the
segment, and one reference for the whole lease, never one a message. A message past the end, or
before what is retained, is answered as such. To find the batch, each segment has a sparse index of
base offset and time to log position, an entry a chunk of log, so a lookup reads at most a chunk
past its entry: the active segment's in memory, built as batches are published; a completed
segment's written beside it when it completes, and read when a read needs it. A live consumer reads
at the partition's durable end and never holds a database snapshot.

### 21.3 Memory

A stream's chunks and read buffers come from a budget of its own, given when the database opens,
from the heap as transactions' chunks do, and not from the pool's frames: a large ingest or a
replay waits for its budget, as an appender past the log's buffer waits, and never takes the trees'
cache. A partition holds its ring, its active segment's index and a bounded number of completed
segments' indexes read for reads: retained history costs no memory.

### 21.4 Retention

Each partition keeps its history by a policy of its own, by age or by size, apart from the
recovery log, whose segments the checkpoint releases (section 10). A whole completed segment is
released once the catalog records the partition's first retained offset past it durably, no lease
or read under way reads it, and no transaction yet to be recovered names an extent in it (section
21.6). A read before the first retained offset is answered as expired.

### 21.5 Recovery

A partition reopens from what its files hold: its segments, read from the catalog's first retained
segment, and each whole batch read in order, its offsets following the last. The log ends at the
first torn or missing batch, and appending resumes at the next block, the gap between recorded in
the partition's catalog so a reader passes it, as the superblock records the log's (section 7.4).

### 21.6 With transactions

The step after the first, designed here so the first leaves room for it:

- **One transaction, tables and streams.** `begin; check ownership and position; update tables;
  append; advance position; commit`. The batch is written to its partition before the commit, as a
  prepared extent with no offset, and the transaction's redo carries a reference to it (partition,
  log position, length), not its bytes.
- **The commit decision waits for the batch.** The Commit is made durable only once the partition's
  log is durable past the prepared extent, so recovery never accepts a commit whose batch is
  missing. That is a durability dependency: batching and pipelining hide it, and its latency is
  measured.
- **Offsets at publication.** At the commit, the partition assigns the extent its offsets, in
  publication order, by a small record in its log naming the extent; readers see the batch from
  then. A transaction that never commits leaves an extent no record names, which readers never see
  and retention releases with its segment.
- **Catalog before redo goes.** Before a checkpoint releases the redo that names an extent, the
  catalog records the extent's offsets and commit, so recovery finds it without that redo.
- **Consumers.** A consumer group's position and its ownership epoch are rows of a tree, advanced
  in the transaction that processes what it read. A retried transaction then has no duplicate
  effect inside the database; an effect outside it (a payment, a request) needs its own idempotence.
- **Producers.** A producer's last sequence per partition is a row: a batch whose sequence is not
  past it is a retry, answered with the offsets it got.
- **Queues.** A work queue shares the payload storage and adds claims, delivery timeouts, and
  acknowledgements out of order, as rows; reclaiming waits for what is unacknowledged.

**Order.** First: partitioned batch append and read, recovery, and whole-segment retention. Then
transactions with streams, and consumer and producer state. Replication, failover and Kafka's
protocol are layers above.

**Built** (`stream.rs`): a partition's batch append, in place or through the ring, its offsets
assigned at publication and done once durable; reads of whole batches by offset from a sparse index;
recovery from the partition's files, the gaps a recovery leaves recorded in its own file; retention
of whole segments by offset. The own file holds two copies, each update written over the one not in
charge: a new partition writes its first durably, its name too, before it takes a batch, so a torn
update always leaves the other (a file of one copy with none whole is a creation a crash cut short,
before any batch). A read holds the partition's segments for as long as it reads: retention that
moved the first offset deletes the segments only once the reads that read the state before it are
done, never under one, which would read as the stream's end. Measured (200,000 messages of 100
bytes, its log in memory, each append waited on): appending 663 instructions a message in batches
of 100, 606 in batches of 1,000, 1,280 in batches of 10, whose batches go through the ring; reading
121, 106 and 275. A SQL insert of the same row costs 13,858.

**Not built yet:** completed segments' index files (the sparse index is in memory, an entry a chunk
of every segment retained); the stream's own memory budget (chunks come from a free list bounded by
the log's buffer, and a read copies each batch into a heap buffer of its own); the catalog in trees,
and streams of several partitions; leases that read with no copy; and section 21.6.

## 22. The executor (designed 2026-11-06, revised after seven audits; not built)

Every background job of a database runs as a task on one executor of the database's own, which
owns their scheduling and every wake. A job says what it waits on and whose work that wait needs;
it never says who wakes it. Each of these has one owner: a task's run state (22.3); its wakes
(22.2); the demand behind its class (22.4); the delivery of every IO request, and its buffers until
the backend lets them go (22.5); whoever waits on a job's work, which runs it when no worker does
(22.7); a panic, which ends the process (22.11); and close (22.12).

Two kinds of wait are kept apart throughout. A **physical** wait is on an IO request the backend
holds: it ends when the backend delivers the request, whoever waits. A **logical** wait is on a
condition a job produces (admission to the log, room for kept copies, a full checkpoint): it ends
only when that job runs. Every wait has a producer that will run, and 22.5 and 22.7 say who.

**Why.** Background work has four drivers today, each with its own wake protocol, kept by hand:

| Job | Driven by | Woken by |
| --- | --- | --- |
| Checkpoints (section 10) | a thread of the database's own, or the writers | every holder of the checkpoint's state, through `StateHold`, once it lets the state go; never the thread itself |
| The store's erasure (section 6.3) | the checkpoint, as its last phase (`erase_freed`) | the checkpoint |
| Version collection (section 11.5) | each begin: one step on its own slot and one on the next slot in turn, which may be in use or the same slot | nothing: no collection runs in the background, only at begins |
| Log segments ahead (section 7.4) | admission and flush, then completion callbacks, under a claim | whoever lets the claim go looks again; a segment prepared wakes admission and retries the flush |

Other work runs in the foreground and stays there (22.9 item 6): a transaction's end drains the
trees it abandoned (`AtollDb::drain`); callers evict, drain frame classes and write back under
pressure (sections 9.4, 9.5); opening and recovery drive their own IO. Each of their waits still
needs a named producer once background work moves.

A protocol kept by hand fails silently. On 2026-11-06 a test's probe of the checkpoint's state let
it go without unparking the thread, and the guard an integrity check holds unparked it before
letting the state go: either way the thread found the state held, parked, and nothing woke it with
a checkpoint due. A targeted build of that protocol failed 5 of 40 runs of the threaded kept test
where the fix passed 40 of 40 (`StateHold`, 22.2).

The same day's sustained run (4,000,000 rows of 200 bytes, a 384 MiB cgroup, the staged binary
`perf-base7`) counted 185 s inside the checkpoint's `State::step` over a 232 s test, and 8.75 s
between writers' refusals for room and their admissions. Both are wall time: the 185 s include
every write `UnixIO` made synchronously, the syncs, and the thread's time descheduled, and the
process used 150.6 s of CPU over the whole test on all its threads. They show the one driver busy
most of the run; they do not show how much of it was CPU, which 22.8 measures before choosing
workers.

### 22.1 Tasks

- **A task** is a state machine the executor steps: `step(&mut self, cx) -> Result<Step>`, where a
  step returns `Done`; `Again`, more to do now, after others have run; `Wait(w)`, on one or more
  completions, naming the job whose work a logical wait needs (22.4); or `Idle(s)`, nothing to do
  until one of the signals in `s` moves (22.2).
- **A step does at most one unit of its job's work, and a unit visits at most one page.** Its
  work is counted in what it touches, not in what it produces: a page read, written, imaged, freed
  or zero-filled; one page reached while building an image (a version chain's arena page, an
  out-of-line value's directory page); one sync; one run of entries scanned from a table, at most
  as many as one page of that table holds; one deferred free; one erased slot. A job's own
  bookkeeping is kept incremental for the same reason (22.9 item 1). Where a unit is one indivisible
  thing larger than a page, it says so with its bound: one log record, at most the log buffer
  (section 7); one flush turn, the writes the log plans at once, at most those it allows in flight
  (a step runs one turn, never `Log::flush`'s loop over turns). Each job keeps a cursor, so a
  step resumes where the last ended, mid-image included. A step returns once its unit is done on
  every backend, a synchronous one included: its work is bounded by its unit, never by how quickly
  IO completes. That bounds work, not time: a synchronous write or sync inside a step takes what
  the device takes. Debug builds count page visits per step and assert the bound.
- **A step never waits on another task, thread or completion.** A lock is taken only as section 18
  allows, never held across a returned step, and never held while waking anyone (22.2). On an
  inline backend (22.5) a submission is its own IO, finished before it returns.
- **IO in flight across steps.** On any other backend a step that submits a request keeps it in
  its job's flight set and returns `Again` while another independent unit is ready and the set has
  room; it returns `Wait` on the set's completions only when it is full, when its next unit depends
  on a request in flight, or when nothing else is ready. A step submits at most one request of its
  own (a flush turn is the stated exception above). The set's room is the job's own limit (the
  checkpoint's `WRITES` and `checkpoint_write_bytes`), and a request holds its place until it
  retires (22.5), so smaller steps keep the device's queue as deep as today's checkpoint keeps it
  and no more requests are owned than the limit.
- **Progress lives in the job's own state**, read and written only by the task while it runs
  (22.3). What survives close is each job's to say (22.9): durable state that recovery reads, or
  state the database keeps outside the task.
- **One instance per job and subject**: one checkpoint coordinator, one segment preparer per open
  log, one collection task per slot, one rollback per abandoned transaction, a fixed number of
  image slots. 22.13 bounds them.

### 22.2 Wakes

A task is woken by one of three things, each with one owner. Two kinds of wake target are kept
apart. An **executor target** is a task's header or a parked runner's thread: waking one is a word
exchange, a link into the run queue under its mutex, or an unpark, which takes no lock but the
queue's, runs no code of anyone else's and cannot panic. An **arbitrary waker** is a
`std::task::Waker` from outside the executor (an embedder's future, a foreground thread's wait); it
may run code. Signal and contender lists hold executor targets only; arbitrary wakers are held by
completions, and by the external runners' nodes of the idle list, which are woken only after every
executor mutex is released (22.3). An arbitrary waker is never cloned, invoked or dropped while an
executor mutex is held: each of the three can run its owner's code. A completion's own registration
clones the waker under the completion's own mutex, which is not an executor mutex; nothing
registers a waker on a completion while it holds an executor mutex.

1. **A completion.** `Completion::set_waker` registers before it tests whether the completion
   finished. Finishing is as today (`core/io/completions.rs`): the completion's callback runs
   first, and an error it returns (a short read, a page that fails its check) is the completion's
   result; then the result is published; then the wakers are moved into a local and woken with no
   lock held. No waiter sees the completion finished before its callback has returned. Finishing is
   logical; the backend's release of the request and its buffers is a separate event (22.5).
   - **A waker only schedules.** It takes no lock a submitter or a step can hold, runs no job code,
     and does not panic: executor targets by construction, an embedder's wakers by contract. One
     that panics unwinds through an Atoll frame and ends the process (22.11).
2. **A signal**: "something a task watches changed". A 64-bit counter and one list of the tasks
   registered on it, under the signal's own mutex.
   - **Notify**: publish the change (the job's own state, by a release store or under its own
     lock), then increment the counter with release ordering, then, under the mutex, wake each
     listed task and empty the list. The tasks are executor targets, so they are woken under the
     mutex; the lock order is a signal's mutex, then the queue's, and the queue's mutex never takes
     a signal's. A notify's work is at most its list, one entry per task that can watch the signal,
     however often notifies arrive.
   - **Wait**: a task reads each counter with acquire ordering, then checks its condition. Unmet,
     it returns `Idle` with each signal and the count it read. For each, the executor takes the
     mutex, adds the task unless it is already listed, releases the mutex, and reads the counter
     again; if any moved, the task is requeued at once. The acquire read pairs with the release
     increment: a task that reads a new count reads the change published before it. The mutex
     orders the registration against the notifier's walk of the list: a notify whose increment the
     second read missed finds the task listed.
   - **"Moved" is inequality**, never "greater". The counter moves once per notify; it would have
     to come all the way round between a task's two reads to hide a notify, 2^64 notifies, 584
     years at one per nanosecond. That is an assumption on the database's lifetime, stated as one,
     not a proof for unlimited lifetimes.
   - **A threshold signal** carries one more word: the least threshold among the tasks listed, kept
     under the mutex with the list (lowered by each registration, raised to "none" when a notify
     empties the list). The handshake, with every order stated:
     - a producer publishes its change with the ordering it already uses (a snapshot's release
       store, the horizon's sequentially consistent `fetch_max`, the checkpoint's sequentially
       consistent store), then issues a sequentially consistent fence, then loads the threshold
       word; if a threshold is there, it computes the value whole (for the cutoff,
       `collect_cutoff`, whose own scan begins with its fence) and notifies when the value reached
       it;
     - a task stores its threshold under the mutex, issues a sequentially consistent fence, then
       computes the value whole and requeues itself if it already reached the threshold.

     Between two fences, one of the pair always sees the other: either the producer loads the
     threshold, or the task computes a value that includes the change. Where the value is the least
     of several parts, each part's producer fences after its own change and computes the whole
     after that, so of two producers changing different parts at once, the later to fence sees
     both. No producer's publication is replaced by an exchange: the fence is added after it, its
     cost (one fence per publication; a fence and a scan when a task waits) measured with the
     statement cost (22.16 step 10).
   - **Wait-any and storage.** A step names at most the signals its job declares (the checkpoint:
     due, full requested, room wanted; 22.9). Registrations are not removed when the task runs
     again: a stale one costs at most one spurious wake, after which the task checks and registers
     again. A list is a fixed array of one entry per task that can watch the signal, sized when
     such a task is created, so registering and notifying allocate nothing. A signal lives no
     longer than the subject whose change it announces, and a task removed with its subject is
     taken off every list it declared (22.3).
3. **A guarded state**: state another holder may hold. `Guarded<T>` owns the lock and its
   contenders; `Hold` is its guard. (Atoll's page latches have a `Held` of their own; these are
   different types.)
   - **The state is reachable only through the guard.** `T` is a private field of `Guarded`'s
     module and `try_hold` its one accessor, so taking it any other way does not compile.
     (Built: `guarded.rs`, whose test compiles a reach for the field and expects it refused.)
   - **Only a contender is woken.** A taker that finds the state held may register as a contender
     and then tries again; if it now holds the state, it removes its own registration. A holder is
     never a contender of its own hold, so a task letting go of state it took wakes nobody but those
     who wanted it: a task that finds nothing to do cannot wake itself and requeue forever.
     Contenders are executor targets (the checkpoint's task, and an integrity check's parked
     thread), kept in a fixed array sized by the takers that may contend: two, the checkpoint and
     one integrity check, which holds the write gate exclusive, so no second runs beside it. Until
     the checkpoint becomes a task (22.16 step 7) its contender is the checkpointing thread, parked,
     and the checkpointer's hold, let go, also wakes the writers waiting for room: under
     `Checkpointing::Writers` a writer refused room whose relief found the state held steps the
     checkpoint itself once it is let go.
   - **A release** lets the state go, then takes the contenders' mutex, wakes each contender and
     empties the list. The lock order is the state, then its contenders' mutex, then the queue's.
   - The checkpoint's state, an integrity check's hold of it, and the maintenance gate (22.9 item 3)
     are guarded states. The checkpoint's `wake_waiters`, whose waiters are completions with
     arbitrary wakers, takes the list into a local and completes it with no lock held (the guard
     once taken in a `for` loop's expression lived for the loop). A step that moves what they wait
     for only marks its hold of the state as owing them a wake; the hold wakes them as it is let go,
     after the state, so a waker may take the state itself.
- **No timers** (section 7.3). Every wait is a completion, a signal or a guarded state. Pacing,
  where a job has one, counts arrivals (22.9 item 3). Foreground timeouts (a busy timeout) and
  future leases keep their own clocks; they are not the executor's. A simulator's delayed IO
  advances on its own simulated clock as it is stepped (22.5).

### 22.3 The run queue

- **The task header** holds a task's state word, its demand (22.4), its links in the run queue and
  its id. A waker holds an `Arc` of the header and nothing else; the header holds the executor's
  shared part only weakly and no reference to its job's state or its database. A waker left on a
  completion keeps only the header alive, never a cycle back to the job or the database (22.12).
- **The state word** is one atomic: `Idle`, `Queued`, `Running`, `Running|Notified`, `Done`,
  `Closed`.
- **Every move into or out of `Queued` happens under the executor's queue mutex, together with the
  link or unlink it implies.** So a task is linked exactly while its word says `Queued`, and no
  claim, close or class change can see one without the other. Moves among `Idle`, `Running`,
  `Running|Notified` and `Done` are compare-exchanges and need no mutex.
  - **Wake**: from `Running`, a compare-exchange to `Running|Notified`. From `Idle`: under the
    mutex, `Idle` to `Queued` and link into the list of the task's class (22.4), and take an idle
    runner's node off its list (below). From any other state, nothing.
  - **Claim**: under the mutex, unlink the head of the highest non-empty class and move it
    `Queued` to `Running`; the move acquires everything the previous runner published.
  - **End of a step**, from the word the runner last read: `Done` to `Done` whether or not
    `Notified`, by compare-exchange, so a later wake finds it done and a waker left on a completion
    cannot bring it back; `Wait` or `Idle`, its registrations made while still `Running`, from
    `Running` to `Idle` by compare-exchange; `Again`, or `Wait` or `Idle` from `Running|Notified`,
    to `Queued` under the mutex and relinked at the back. Each publishes the job's state with
    release ordering. A compare-exchange that fails because a wake set `Notified` meanwhile takes
    the `Queued` path instead, so a wake during a step is never lost and never doubles the task.
  - **Close**: under the mutex, any state to `Closed`, unlinked if queued; a running task's end
    fails on `Closed` and the runner drops its result. A wake of `Closed` does nothing.
  - **Removal**: a task removed with its subject (a log closed, a rollback finished) is closed,
    taken off every signal list it declared and every contender list it entered, and its header
    is freed once the last completion holding its waker is delivered. Headers are never reused, so
    an old waker can never wake a new task; 22.13 bounds the headers alive.
- **The run queue** is one intrusive FIFO list per class, linked through the headers, so it has no
  capacity to exhaust, allocates nothing, and holds each task at most once.
- **Idle runners.** A runner that finds every list empty links its own node into the idle runners
  under the queue mutex, releases it, then waits (22.5). Every runner brings one node, provisioned
  with it (22.13). A node is internal (a worker's or a blocking caller's thread, parked) or
  external (an embedder thread's `run_one`, or a cooperative caller polling an executor completion:
  22.7), carrying the arbitrary `Waker` its runner last registered. An external runner clones the
  `Waker` it was handed before it takes any executor mutex; under the queue mutex it swaps the clone
  into its node, taking out the one it replaces, which it drops only after the operation has
  released every executor mutex. A link takes at most one idle
  node off the list under the mutex. An internal node's thread is unparked there, an executor
  target (22.2); an unpark that lands before the park returns it at once.
- **An external node is never woken under an executor mutex.** A link that takes one moves it, still
  under the queue mutex, onto the tail of the executor's to-wake list, a first-in-first-out list
  intrusive through the node itself (a node is on at most one list), and adds one to the count of
  nodes moved by the outermost operation it belongs to. Every outermost operation that can link (a
  task's `Waker::wake`, a notify, a release, a ticket taken or dropped, the end of a step, close)
  ends, after releasing every executor mutex it took, with a drain of at most as many nodes as it
  moved: under the queue mutex it pops the node at the head, takes the `Waker` out of it
  (`Option::take`) and marks it off every list, releases the mutex, invokes the `Waker`, and repeats
  until its count is spent or the list is empty. A nested call (a link inside a notify) moves but
  never drains. The list never holds more nodes than the unspent counts of the operations still to
  drain, because a drain stops only with its count spent or the list empty; so every node moved is
  invoked by some drain, and each drain's work is bounded by its own moves, however often a host
  re-registers and is moved again. Because every drain pops the head, a node is selected before any
  node moved after it, so no node waits behind arrivals that keep coming: each is popped after at
  most the nodes ahead of it, then invoked by the drain that popped it as that drain goes on. Two
  drains at once may invoke in either order (one pops, releases the mutex and is preempted while the
  other pops the next and invokes it first); each node is still invoked once, and nothing serializes
  the invocations to order them. After the pop the drainer holds only the `Waker`, so a runner may
  cancel and free its node at any time: a cancel unlinks the node, under the queue mutex, from
  whichever list holds it, taking its `Waker` out to drop after the mutex is released, and the count
  that covered it is spent on another node or forfeits at an empty list.
- **Lock order**: a signal's or a guarded state's contender mutex, then the queue's. Nothing takes a
  signal or contender mutex while holding the queue's: removing a task closes it under the queue
  mutex, releases it, then takes it off its signal and contender lists. A caller that already
  holds the queue mutex wakes through the queue-locked form of the link, never through the general
  wake.

### 22.4 Classes and demand

- **Classes**, in the order a runner takes them:
  1. **Relief**: a writer waits for it, directly or through another job's wait.
  2. **Due**: its own trigger fired (a checkpoint due by the log, a segment needed ahead), or a due
     task waits for it.
  3. **Maintenance**: collection, compaction of idle slots.

  Strictly by class, and within a class first in first out: a task requeued after a step goes to
  the back, so every task queued in a class is stepped once before any is stepped twice, and a task
  linked into a higher class is the next claimed.
- **A task's class is the highest of its demands**, kept in its header under the queue mutex: its
  own level (due while its trigger stands, maintenance otherwise), and the count of live tickets at
  each class above it. Relief while any relief ticket lives; else due while its own trigger stands
  or any due ticket lives; else maintenance. A trigger's notify sets the task's own level as it
  wakes it, so a checkpoint made due waits in the due list, not the maintenance one. The task
  clears its own level when its step finds the trigger gone, and only if, under the queue mutex, the
  *due* signal's counter still reads what the step read before it looked: a trigger that came after
  the look keeps its level. One ticket dropped never clears
  another's demand or the task's own. The class changes when a demand does, not when the task next
  runs: a queued task changing class moves between lists under the mutex.
- **A writer's wait holds a ticket on the task whose work it waits for**: a writer refused room
  for kept copies, on the checkpoint, with the bytes it asked for; a transaction's end waiting for
  a full checkpoint, on the checkpoint; a writer waiting on an abandoned transaction's versions, on
  its rollback; a writer waiting to be admitted to the log, on that log's preparer. The ticket is a
  value the waiter owns. Taking it raises the count and wakes the task; dropping it (admitted,
  cancelled, or the connection gone) lowers it. There is no thread-local record: what the room
  counts as pressing is the largest request among the room tickets alive, so admitting a smaller
  writer no longer clears a larger one still waiting.
- **A task's logical wait carries its own class.** A logical wait names one job, or the image slots
  as a group, never more. A task whose wait needs another job's work (the
  checkpoint's intent record waits on its log's admission, so on that log's preparer) holds a
  ticket on that job while the wait stands, of the waiting task's class, moved by the executor
  whenever that class changes, which may move the next. So a checkpoint a writer waits for makes
  the preparer it waits for relief too, and a due checkpoint makes it due. A wait on its own
  requests in flight needs no ticket: the backend delivers them whatever the class (22.5).
- **Jobs wait on each other in one fixed order**: rollback, then collection, then the checkpoint,
  then its image slots, then preparers. A job's wait may name only a job after it, and debug builds
  assert it, so moving a ticket stops within four jobs and no cycle of waits can form. Today's
  jobs use rollback to checkpoint, rollback to preparer and checkpoint to preparer; collection of
  real pages (it may need room) and image slots are placed now, before they are built. A new job
  takes its place in the order in the design before it is enabled. Flushing the log is no job:
  whoever waits to be admitted or for durability flushes inline (section 7.4).
- **Starvation.** Maintenance runs when no relief or due task is queued, so a load that keeps
  relief queued starves collection: reported (22.14), and the begins' inline steps keep collecting
  at their pace (22.9 item 3). No job a writer waits for can be starved by the classes above it: it
  carries the writer's relief through its tickets.

### 22.5 Delivering the IO

Every request has one owner of its delivery from submission until the backend lets it go, and no
waiter, blocking or cooperative, depends on someone else happening to step the IO. Each backend
says which owner that is:

- **Inline**: every request is delivered inside its submission (`UnixIO`, `MemoryIO`, the generic
  and Windows IO). Nothing is left to deliver; `step` does nothing.
- **The ring** (io_uring): **a delivery thread of the ring's own** is the one thread that waits in
  the kernel. Before each wait it moves requests queued past a full submission queue (the ring's
  overflow) into the kernel, so no accepted request is left outside it; it waits for one
  completion, takes at most one completion queue of entries (1,024, twice the 512-entry submission
  queue, the kernel's default) into a fixed buffer under the ring's mutex, handling a partial
  write's resubmission there as today, releases the mutex, delivers each entry, and waits again. The
  mutex is held only to touch the queues, never across the kernel wait or a callback. Every request
  submitted to the ring, by any thread, is delivered by it, so a synchronous waiter, a cooperative
  future, an executor runner and a request nobody waits for are all delivered alike, with no
  leadership to hand over and no waiter that must step the ring. Only the delivery thread takes
  entries from the completion queue, into its one buffer: no other thread delivers ring requests,
  so there is one dispatch route and one batch in delivery at a time.
  - **Callbacks on the delivery thread are short and never wait**: they may submit requests, but
    never wait for one and never make a synchronous file call (a sync, a directory sync, a create,
    an allocation). `IO::wait`, `IO::wait_retired`, a blocking remote wait and close refuse on the
    delivery thread (asserted), since nothing else would deliver what they wait for. A census test
    lists every callback Turso and Atoll construct, with what it does. Today's control and metadata
    callbacks, which submit the next sync, are of this kind; the log's segment preparation is not:
    its `prepared` callback syncs the segments' directory, looks for the next segment (which may
    create and allocate a file) and retries the flush, synchronously. From 22.16 step 3, before the
    delivery thread is built, the log's interim adapter (below) moves that work out of the callback.
  - **A callback that panics on the delivery thread ends the process**: each delivery runs in a
    frame (22.11), from the callback through the result's publication and the wakes to the
    request's retirement. The thread cannot hand a panic to anyone, and a ring left without its
    deliverer would strand every waiter on it.
  - **The thread never returns on an error.** An error of one request is that request's result,
    delivered through its completion. An error of the ring itself, from its kernel wait or from
    moving queued requests in, is retried where the kernel says it may be (an interrupted wait; a
    busy or full completion queue, which the thread drains first and then retries); any other
    (a ring the kernel no longer recognizes, a fault in its memory) leaves accepted requests the
    thread can no longer deliver or retire, so it ends the process with the error printed (22.11),
    rather than return and leave every waiter and close waiting on requests nobody will deliver.
  - **Its lifetime.** The thread holds the ring's shared state, never the handles the embedder
    holds. It runs while any handle to the ring (the IO and its files) lives or any request is
    outstanding; the last handle's drop writes the ring's eventfd, whose standing poll ends the
    kernel wait, and the thread exits once nothing is outstanding. A last handle dropped on the
    delivery thread itself, inside a callback, lets it exit after its batch instead of joining
    itself. The eventfd's entry is not a request and is never counted as one. If the thread or its
    eventfd cannot be set up, the ring is torn down whole and the open falls back (22.6).
- **Pumped**: requests are delivered only inside `step` (`CrashIO`, the yielding memory IO, the
  simulator's IO, an extension's VFS, the experimental Windows completion port). The backend owns
  each request as queued until a step takes it, and the stepping thread owns it while delivering
  it; it says how many are queued, or that it cannot say. A step may deliver none when the
  backend's own clock has not reached a request (the simulator's latency): stepping advances that
  clock, and its CPU is the simulator's time model. A backend that cannot say how many are queued
  (a VFS) is stepped while anyone waits on one of its requests; its readiness is its own contract.
  A request's error is delivered through its completion; an error of the stepping itself (an
  extension's `run_once`, the completion port's) means the backend cannot account for what it
  accepted, and ends the process as the ring's does.

**The log's interim adapter** (22.16 step 3, until the preparers of step 8). The log's callbacks
only submit and record; the synchronous work they did is run by the callers that wait on the log.

- **A completion means call again.** The completion `admit` or `wait_durable` returns as its IO
  says only that the caller should call again, never that room was granted or the position
  reached. Room is reported only by `admit` returning `Done` from a grant, durability only by
  `wait_durable` returning `Done` after it reads the durable position at or past its mark, and the
  log's failure only by either returning it. So any producer may finish a waiting caller's
  completion to bring it back without reporting anything false. Today's branch in `wait_durable`
  that returns `Done` for a completion found finished after its flush goes: the call reads the
  durable position instead. Admission completions already mean this (admitters are woken to try
  again). Every caller calls again after its IO finishes without an error: the checkpoint's `done!`,
  a transaction's admissions and commit, rollback, a stream's append and its durability, and `drive`
  (opening, recovery, a database's drop). One whose IO finishes with an error may return that error
  at once, as `drive` does; it is the log's failure, which every waiter is told. No caller takes a
  completion finished without an error as room or durability. A census test lists every call site of
  `admit` and `wait_durable`, the facade's included, with how it calls again and what it does with
  an error, and fails on one it does not list.
- **The preparation's word**, one beside the log, says where the segment being prepared stands:
  *done* (none under way); *needed* (a segment must be prepared and nobody has started it); *in
  flight* (its zero-fill writes or its data sync are submitted); *owed* (its data sync landed: the
  directory sync, the prepared end's publication, the admitters' wake, the look for the next
  segment and the flush padding waited on are owed). Starting a preparation (a spare's rename, a
  file's create and allocation) and finishing one (the directory sync) are synchronous file calls,
  so only a caller runs them. The word replaces today's preparation claim (`solo.rs`'s `Claim`, held
  from the start through `prepared`), and it says who owns the preparation at each point. A caller
  that finds the word needed or owed, or done with less than a segment prepared ahead of what is
  admitted, takes it by compare-and-swap into a taken state, *starting* from done or needed,
  *finishing* from owed, so one caller owns each step and any other goes on; in flight is owned by
  the preparation's requests, never by a caller, so no second preparation starts while one is under
  way. A starter makes the segment; a spare then needs only its directory synced, so the starter
  moves the word straight from starting to finishing and runs that too; for a fresh file it moves
  the word from starting to in flight before it submits the first zero-fill write, which hands the
  preparation to the requests (on an inline backend the whole fill and its sync can land inside that
  submission). The data sync's callback hands it on, from in flight to owed. A finisher syncs the
  directory, publishes the prepared end, moves the word from finishing to done, which ends its
  ownership, and only then wakes the admitters, looks for the next segment (taking the word again to
  start it if needed) and flushes, in today's order. Taking the word is no lock: no step waits for
  it, nothing synchronous runs under a lock of the log's, and every wake comes after the word is let
  go. For a fresh file the directory is synced only from owed, which only the data sync's landing
  sets, so no caller runs the continuation ahead of the sync it depends on; a spare is the one
  exception, since its file is whole and synced already (section 7.4), so its rename leaves only the
  directory sync.
- **Callbacks only submit and record**: the next zero-fill write, the next sync, the landed and
  durable positions. Where today's callback would start a preparation (its flush meets padding
  waiting for a segment) it moves the word from done to needed; where today's `prepared` would run,
  the data sync's callback moves it from in flight to owed. A flush, whoever runs it, only ever
  moves the word; it runs no step.
- **A move to needed or owed wakes every waiter.** Whoever makes it, a callback or a caller, then
  takes every durable wait, whatever its mark, and every admission wait off their stacks and
  finishes each, so every waiting caller comes back. A wait registers by pushing its completion
  onto its stack and then, across a sequentially consistent fence, reading the word; the mover
  stores the word and then, across one, takes the stacks: the pairing `log/waits.rs` and
  `log/room.rs` already use, so either the mover takes the wait or the waiter reads the word. The
  completions are finished after they are taken, with no log lock or executor mutex held and the
  word let go (the stacks are lock-free). A move to in flight or done wakes no one: nothing has
  become runnable. There are at most two such moves per segment prepared, so these wakes are
  bounded by the segments the log prepares.
- **A wake that puts waits back looks again.** The durable position's wake (`log/waits.rs`) takes
  every wait off the stack, finishes those whose mark is reached and puts the others back, so for a
  moment the others are on no stack: a move or a failure whose take runs then would miss them.
  Putting a wait back is a registration, and it ends as one does: after putting waits back, across a
  sequentially consistent fence, the wake reads the log's failure and the word, and the position as
  today. A failure, it takes the stack again and fails every wait on it; a word needed or owed, it
  takes the stack again and finishes every wait on it, for a retry; a position raised meanwhile, it
  goes round again as today. The mover and the failure store, then fence, then take, so either their
  take finds a wait put back or the wake reads what they stored. The admitters' wake puts nothing
  back (every admitter comes back), and a mover's or a failure's take finishes everything it takes.
- **A call**, `admit` or `wait_durable`, in order: registers its wait; flushes; takes and runs a
  step it finds runnable, in its own frame; then reads again: `admit` its room, `wait_durable` the
  durable position, either the log's failure. A caller whose own flush or step moved the word, and
  so finished its own completion, comes back once more, for a call. So a dependency that appears
  after a caller has parked (its flight set full at the call, so its own flush never reached the
  padding, and a later write's callback the first to meet it, with the next segment in flight or
  not yet started) wakes it when the dependency becomes runnable, and its next call runs the step.
  Close's first step (22.12) is such a caller.
- **The log's failure** finishes every wait with the failure, as today; a caller that comes back
  reads it and returns the error. Once the log has failed no preparation step begins work: a
  caller reads the failure after it takes the word, and a starter or finisher whose synchronous call
  (a create, an allocation, a rename, the directory sync) was running when another request failed
  reads it again when the call returns, before it submits or publishes; seeing it, the step lets the
  word go where it stands and does nothing more. A publication that races a failure landing after
  that read is harmless: a failed log plans no write and admits nothing.

That is today's foreground `prepare_ahead` path made the only path. The preparer task replaces the
adapter (22.9 item 2): it then runs both steps, a move to needed or owed notifies it instead of
waking the waiters, and the waiters are woken by their marks and by room, still calling again. The
ring becomes the default only after that (22.16).

(Built, step 3a: the word is `log/preparation.rs` and replaces the preparation's claim; the wake
that puts waits back looks again at the failure and the word (`log/waits.rs`); a census test lists
the eleven calls of `admit` and `wait_durable` outside tests with how each calls again and what it
does with an error. `CrashIO` records every synchronous file call made from inside a callback its
step delivers, and none is made over eight segments, fresh and spares. Loom models the word's
handshake with a waiter and the look after a put-back.)

**Retirement is not finishing.** A request is retired when its backend lets go of it and of every
buffer it reads into or writes from, after its callback has returned. A request owns those buffers
until then, through references the backend holds; a completion finished early (a cancel aborts the
completion before the kernel has given the request back) does not end that ownership.

- **A completion's ownership has three states**: not submitted, accepted, retired. It is not
  submitted from its creation until a submission accepts it; a submission that accepts nothing
  leaves it not submitted. Only an accepted completion is owned by a backend; a not submitted one
  owns no buffer of a backend's, and `IO::wait_retired` returns at once for it. That holds for a
  physical request only. A group (a `CompletionGroup`, nested or not) is never itself submitted:
  its ownership is the aggregate of its descendants, accepted while any physical request beneath it
  is accepted and not retired, and retired when every one has retired or was never accepted.
  `IO::wait_retired` on a group walks it down to its physical requests and waits for each accepted
  one, so a child added and never accepted (the sorter adds a child before its read is submitted)
  does not hold the drain, and an inner group whose own completion was never submitted still holds
  it while a request under it is in flight. The aggregate is kept as retirement metadata of its
  own, never by a group holding its children's completions, which would keep their buffers past
  retirement and, with a child's reference to its parent, can form a cycle; a drain that meets one
  physical request twice counts it once. A drain is made by the owner of what it drains after it
  has stopped submitting to it, for the whole tree beneath it: no request can be accepted after the
  drain looked at it, and draining again, or a leaf never submitted, costs nothing.
- **Every backend reports retirement** separately from finishing, through the completion: inline
  at the submission's return, pumped when its step has delivered it, the ring when the request's
  own entry has been delivered. A submission either returns an error having accepted nothing, or
  accepts the request and returns success: a ring entry queued but not yet in the kernel is
  accepted (today `submit_entry` can queue an entry and still return an error). A later refusal to
  enter it that the kernel says may pass leaves it queued for the next entry; any other is an error
  of the ring itself, which ends the process (22.11), since the ring can no longer account for what
  it accepted.
- **`IO::wait_retired(c)`** waits for retirement as `IO::wait` waits for finishing, with the same
  parking. `IO::drain_completions`, whose promise is that cancelled requests have given back their
  buffers, waits for retirement, so the write-ahead log's cancelled checkpoint reads, the pager's
  spill failures and the sorter's cancelled group keep that promise on the ring.
- **Atoll counts its requests**: every request on an Atoll file is counted before it is
  submitted, from a statement, a job, opening or a callback, and uncounted at retirement or when a
  submission accepted nothing. A request a callback submits is counted inside that callback, before
  the request whose callback it is retires, so the count cannot pass through zero while the chain
  goes on. Close waits for the count (22.12).
- A callback reaches job state only through a weak reference or what its completion owns.

(Built, step 3b: a request's ownership is a word on its completion (`io/completions.rs`), moved by
`accept`, `refuse` and `retire`, and `on_retired` runs what waits for the retirement once no backend
owns the request. Every backend's delivery is `deliver`, which runs the callback and then retires:
an inline backend inside the submission, so the request is let go as the submission returns; a
queueing backend (the ring, `CrashIO`, the yielding memory IO, a VFS, the completion port, the
simulator's delayed files and memory IO, the integration tests' queued IO, OPFS) accepts at its
queue and retires at its delivery, and
retires a request it drops undelivered (a cancel's leftover, a power loss). A layer that queues a
request and then hands it to another backend leaves it accepted, and the one that delivers it
retires it. A layer that carries a caller's request out through one of its own (a page transform's
read, the partial-sync storage's writes) accepts the caller's before submitting and retires it when
its own retires (`Completion::carry`). A group keeps a list of what it lists: each request weakly,
since the backend that owns one holds it and a request gone is owned by no one, and each nested
group's own list strongly, never the group's completion, since a request can still be owned after
every handle to the groups above it is gone; so the walk reaches it without holding a child's
buffers, and a drain meets each request once. The trait's default vectored write submits its buffers as parts that carry the
write: refused at its first part it accepted nothing; refused at a later one it stops there, is
accepted, and carries the refusal as its error once its accepted parts have finished, owned until they
retire; a part that fails finishes the write with its error. On the ring a request is accepted once
queued; a refusal to enter it the kernel says may pass leaves it queued for the next entry, and any
other ends the process, the ring's rule of 22.11 applied at the submission until the delivery thread
holds the ring's waits. `IO::wait_retired` and `drain_completions` step until no request beneath what
they were given is owned. Atoll's count is `requests.rs`: a database opens every file through an IO
that wraps its embedder's and counts each request on them, and the database's last drop waits for
the count before it returns (22.12, step 4); a census holds the wrapper to forwarding every method of
the traits it implements.)

(Built, step 3c: the ring's state is shared by its handles, the IO and its files, and by its
delivery thread, `turso-io-uring`, which holds no handle. Before each wait it moves the overflow
into the submission queue under the mutex; it waits for one completion; under the mutex it takes at
most a completion queue's worth (1,024 for the 512-entry ring), resubmitting a partial write's rest,
rearming its eventfd's poll and skipping a cancel's own entry; with the mutex let go it delivers each
and lets its count go. Its whole life is a frame whose drop ends the process if it unwinds. A wait
the kernel interrupts is waited again; a busy or full completion queue is drained, then waited
again; any other error of the ring ends the process with the error printed. The last handle's drop
writes the eventfd, whose standing poll ends the wait, and the thread ends once no handle lives and
no request is counted; nothing joins it, so a last handle dropped inside a callback lets it end
after its batch. A refusal of the ring, its buffers' registration, the eventfd or the thread fails
the open whole and says which. `step` does nothing on the ring; `IO::wait_for_completion` parks on
the completion and `drain_completions` until each request retires. On the delivery thread
`refuse_on_delivery_thread` refuses every IO's wait and drain, a blocking remote wait, Atoll's
close, and on the ring a file's open, removal, rename, directory sync, allocation, hole punch and a
truncation without the ring's opcode; a refusal there is a panic, which ends the process. The census
(`io/callback_census.rs`) lists the 90 callbacks Turso and Atoll construct outside tests, each with
what it does, and refuses one whose own code (its arguments, or the binding they name) waits or calls
the file system; what a callback calls further is refused at run time. The ring's shuttle variant of
the IO tests is gone: a thread waiting in the kernel is outside any schedule shuttle explores. Until
step 3d puts every blocking caller on `IO::wait`, a caller that loops on `step` spins on the ring
while the thread delivers.)

(Corrected after the audit of step 3b: an error of a backend's stepping itself, not of a request,
ends the process wherever it is met (`stepping_failed`): in the trait's waits and drains, in
Atoll's close, in an extension VFS's `run_once` and in the completion port's own error, since the
backend can no longer retire what it accepted and its waiters can neither wait on nor return while
it may own their buffers. A VFS submission the extension refuses takes back the reference its
dropped callback held. The completion port refuses a read or write whose byte count an `i32`
cannot carry before it accepts it, and delivers every packet it consumes, a count or error code it
cannot convert as the request's error. The concurrent simulator's files accept each request as
they queue it and deliver and retire it at the step.)

**Who waits how.** Every caller that waits for IO is one of three kinds:

- **Blocking**: it holds its thread until the completion finishes, through `IO::wait(c)`. An
  inline completion is already done; for a ring request the thread parks on the completion's waker
  and the delivery thread wakes it; a pumped request is
  stepped until it finishes; a remote wait blocks as now; a logical wait on executor work runs the
  executor when no worker does (22.7). Blocking callers today: `IO::wait_for_completion`,
  `Statement::step_io` and the `run_*` helpers over it, the statement's reset and RETURNING loops,
  Atoll's `drive` (opening and recovery), the checkpointing thread, the SDK kit's `step_statement`
  without async IO, the PostgreSQL frontend's statement loops, and `tools/dbhash`, each through
  `IO::wait`; and `IO::drain_completions`, through `IO::wait_retired`. None steps the IO in a loop
  of its own.
- **Cooperative**: it returns `Pending` and is polled again when its waker fires
  (`IOCompletionAsync::poll`, the SDK kit's async branch). On the ring and inline backends the
  delivery owner wakes it; it needs no step. On a pumped backend its caller steps, as test harnesses
  do. A logical wait on executor work runs a bounded share of the queue on each poll when no worker
  does (22.7). Cooperative callers never block in `IO::wait`.
- **Pumps**: one step at the host's request (the JavaScript binding's `io_loop_sync`, the SDK kit's
  `run_io`, `core/stats.rs`'s yield): unchanged, a no-op on the ring and inline backends.

**Runners and a pumped backend.** On a pumped backend, a runner (a worker, an embedder's thread, a
caller running the executor) steps the backend once after every task step while any request is
queued, and with no task to claim steps it until none is queued, parking only then: a queue that
never empties of ready tasks therefore never stops the IO a waiting task needs. One step per task
step matches the IO's service to the unit of work each step does. Two runners stepping one pumped
backend is safe: a request a step took is delivered by that step's thread, whose callback wakes its
task, which links it and wakes a parked runner. Every route that runs a callback (the delivery
thread, a pumped step, a submission delivered inline) runs it inside a frame: the delivery thread
and a runner's step are frames of their own, and an inline callback runs inside its submitter's.

### 22.6 io_uring as Linux's default

- **Clay's ruling of 2026-11-06.** Turso builds io_uring behind a feature and makes the
  synchronous `UnixIO` its platform IO; on Linux the default becomes io_uring, the feature on by
  default.
- **What falls back, and to what**, by cause:
  - the ring refused at setup (the kernel lacks it, a sandbox's seccomp policy, the
    `io_uring_disabled` setting), or its delivery thread refused: `UnixIO`, recorded with the
    reason;
  - fixed buffers refused (sparse registration needs Linux 5.13; a locked-memory limit): the ring
    without fixed buffers, recorded. Atoll's IO uses none: its regions (`Buffer::Region`) carry no
    fixed buffer id, and the old page arenas already fall back to unregistered memory;
  - an operation the kernel's probe lacks: that operation's POSIX path, as `ftruncate` has today;
  - any other error at setup fails the open, and an error after setup is an IO error: no fallback
    hides an unrelated fault, and nothing reopens under another backend.
- **Said where the embedder reads it**: the backend in use and, if it fell back, why.
- **An explicit choice stands.** A database opened on an IO the embedder chose keeps it: `syscall`
  still means `UnixIO` (`SyscallIO` stops being an alias of the platform IO), a VFS stays a VFS.
- **The default switches only once** the delivery owners, retirement and the three kinds of waiter
  (22.5) are built and proved, no callback does synchronous file work (the census of 22.5, which
  needs the preparer moved to a task first), and the ring, still opt-in, has passed on mt1's real
  filesystems (ext4, xfs, btrfs) with direct IO and Atoll's alignment, short writes, concurrent
  waiters, injected setup, thread and registration refusals, a locked-memory limit, and buffer
  lifetimes under cancellation, the write-ahead log's, pager's and sorter's cancellations drained to
  retirement among them; and the Atoll suite, Turso's own suites, its integration tests and the
  crash tests pass on it, since the default reaches every database, not only Atoll's. The cost of
  delivery on another thread is measured then too: a foreground read's latency on the ring against
  `UnixIO`'s and against today's ring, whose waiter delivers its own completion. If the park costs
  what matters, a waiter delivering its own ready entries is designed then, with its own batch
  storage and dispatch route, not before.

### 22.7 Who runs the steps

One loop takes the next task by class and steps it (22.3). Four runners call it:

- **Workers**: threads of the database's own; idle ones park (22.5). How many: 22.8.
- **Callers**: no worker, for a platform without threads, shuttle, loom, and tests that step jobs
  themselves. **Whoever waits on executor work runs the executor.** A logical wait on a job (room, a
  full checkpoint, admission once the preparer is a task, an abandoned transaction's versions) is an
  executor completion: a blocking wait on it acts as a runner (22.5) until it finishes, claiming
  queued tasks by class and, on a pumped backend, stepping it between task steps (22.5), and with
  none queued parking among the idle runners and on the completion; a cooperative poll of it runs
  one task step and, on a pumped backend, one IO step, then, unfinished, registers its `Waker` on
  the completion and on its external node in the idle runners (22.3), wakes its own waker if
  anything is still queued, and returns `Pending`, so its host has control back after each unit and
  polls again when one of those wakes it: the completion finishing, or a task linked while its node
  is idle. Neither enters
  the executor from inside a step or a waker. Either runs only as much as today's
  `Checkpointing::Writers` makes a writer run. The
  waiter's ticket (22.4) makes what it waits for, and what that waits for, the first claimed.
  Besides waiting: a writing statement whose checkpoint is due runs one step of the queue, and a
  begin runs one step of its slot's collection and one of the next slot's, each a named claim: under
  the queue mutex, a `Queued` task is unlinked and claimed, an `Idle` one is claimed straight to
  `Running` (the begin has its own reason: its own arrival), and a `Running` one is left to its
  runner. With workers, waiters only wait: the workers run what they need, and connections never run
  background work. Callers' background work advances only while connections call: a task queued with
  no caller waiting on it stays queued until one comes, which the queue's ages report (22.14).
- **Embedder**: threads the embedder provides (section 18) call `run_one`, which steps one task, or
  with none to claim registers the embedder's waker on its external node among the idle runners and
  returns, so a link wakes it, after the executor's mutexes are released (22.3). Requests are
  delivered by their owners (22.5), so the embedder drives no IO but a pumped backend's, which it
  steps as a worker would. `run_one` is safe from any number of threads, is never entered from
  inside a step or a waker (asserted), and close waits for every call in progress to return.
- **Deterministic**: for the simulator and replayed schedules: one runner on the same queue, strict
  by class and first in first out within one, over the simulator's seeded IO. The seed decides when
  requests complete and when callers come; the queue decides the rest. Replay is claimed over
  simulated IO only: over real IO, completions arrive when the device delivers them.

### 22.8 How many workers

One, as a first experiment, not a chosen optimum. The checkpoint's gate, cut, metadata and
superblock are serial; only its images are divisible (a page's image depends on that page and the
cut alone). The checkpoint is profiled through the sustained run on io_uring twice: once its steps
are units with IO in flight across them (22.16 step 5), and once it runs on the executor (step 7),
keeping apart what the 185 s mixed: its thread's CPU, its waits in syscalls and on the device, each
class's queue delay, completions per delivery pass, and page visits per step. The number of
workers, and whether image slots pay, are derived from those profiles and stated with them: as many
as keep the device's queue full at the measured cost per image and per write, no more than the
cores the database is given. The embedder can always set it.

### 22.9 The jobs

1. **The checkpoint coordinator.** Section 10's state machine as one task.
   - **Units**: closing the gate; the cut's scan of the pool and its inventory of kept and swapped
     copies, a run of frames at a time; one orphan; building one image, a page visited at a time:
     the leaf or node, then each arena page its version chains reach (`AsOf`), then each directory
     page of an out-of-line value it names, the image's cursor kept in `Imaging` between steps;
     one image written; the intent record, its size checked against the log buffer before it is
     built, then built, admitted and appended as one record, then its durability; one metadata page
     written, the dirty set walked in order through the ordered set the run keeps (today
     `MetaCache::begin_cut` takes, sorts and dedups the whole dirty list, and the phase gathers
     every id and frame at once: both become walks); one data file synced; the superblock
     written; one deferred free (`PageStore::after`); one slot erased. Today `Run::step` runs phase
     after phase in one call, and `image_pages` goes on while writes complete inline: on `MemoryIO`
     one call imaged 4,044 pages of a 4,000-row table. Both stop after one unit.
   - **Writes in flight**: image writes are its flight set (22.1), up to `WRITES` and
     `checkpoint_write_bytes`, each holding its place until it retires; metadata writes and syncs
     likewise. Node images still come before
     the value pages whose writing depends on them, and every barrier of 22.10 waits for the whole
     set.
   - **Due**: a writing statement, after its appends, compares the log's end with the threshold
     (where the log ended when the last checkpoint began, plus a share of the budget) as `step_due`
     does today, and when past it notifies *due*, which sets the checkpoint's own level (22.4).
     The task compares the same two itself before it returns `Idle` and goes on if past, so a
     database already due when the task is created, or after a checkpoint ends, needs no further
     append. Nothing arms an edge: a notify with no task waiting costs the counter's increment,
     paid only by statements between the crossing and the checkpoint's start.
   - **Full requested**: `request_full` raises the count asked for, then notifies. A ticket is
     answered by a checkpoint begun after the request, as now.
   - **Room wanted**: the room tickets (22.4): kept pages' images first while any lives.
   - Its state is a guarded state (22.2); it contends for it when an integrity check holds it.
   - **Image slots**, only if 22.8 calls for them: a fixed number of tasks, each building one
     page's image and writing it, owning the image from its building until its write retires and
     the coordinator takes the result, its bytes counted within `checkpoint_write_bytes` until
     then. The coordinator waits on them (after it in 22.4's order); naming each copy in the page
     table stays the coordinator's.
2. **The segment preparer**, one per open log: the database's, and each open stream partition's
   (section 21.1). It moves all of section 7.4's preparation into one task and removes the callback
   chain in the same change, so no build has two producers for one continuation: a spare renamed and
   its directory synced, or a fresh segment created, allocated, zero-filled with one write in flight
   at a time, as today (a deeper set waits for a measurement), and synced; then its directory
   synced, the prepared end published, admission woken, the next segment looked for, and the flush
   that padding waited on retried. It is due when admission comes within a segment of the prepared
   end, and relief through admitters' tickets. Its failure fails the log: every durable and
   admission waiter is told (section 7.5).
3. **Collection** (section 11.5): one maintenance task per slot.
   - **One collector per slot is the task itself.** A begin's inline step is a named claim of its
     slot's task and of the next slot's (22.7). Inline and background collection are one code path
     and cannot overlap, so the slot's collector claim goes; the step reads the cutoff after it is
     claimed, never before.
   - **IO is waited for, never dropped.** A step that must read a page in returns `Wait` on the
     read; its delivery requeues the task. Today the begin starts the read, discards it, and a
     claim it lost reads as nothing to do.
   - **Wakes**, on what makes a page eligible, never on pacing: *page linked* (the slot linked a
     new page, so the page before it stops being the one appended to), and the *cutoff* threshold
     signal (22.2): a task whose oldest page is not yet eligible registers the cutoff that would
     make it so, or, while the transaction that last appended to it still runs, that transaction's
     end. Every change to a part of the cutoff notifies when what was registered is reached
     (`collect_cutoff`: the least of the oldest published snapshot, the horizon where none is
     older, and the checkpoint's hold): a snapshot taken (its final publication; the first, of
     zero, only lowers it), raised for a read-committed statement (`snapshots::rise`), or vacated
     at a transaction's end; the horizon moving; the checkpoint releasing its cut; and the
     transaction's end. Each producer keeps its publication as it is (a snapshot's release store,
     the horizon's `fetch_max`, the checkpoint's store; `snapshots::take` keeps its own fence
     between publishing zero and reading the horizon) and adds the handshake of 22.2 after it: a
     fence, then one load of the threshold word. Its cost is measured with the statement cost
     (22.16 step 10), with a task waiting and without.
   - **Pacing** is the begins' alone: each page a slot takes earns two pages of inline collection,
     saturating at the slot's pages taken and not yet freed. The task is unpaced: at maintenance
     class it runs while eligible work remains, so a backlog left after the last change drains with
     no further begin.
   - **An error** ends that slot's task as `Done`, reported, until the database is opened again:
     `Done` dominates every wake (22.3), so nothing restarts it, and no timer retries it.
   - **The maintenance gate.** When collection comes to free real pages (retired values and
     tombstones, section 11.5, not built), each such step takes the gate shared and an integrity
     check takes it exclusive for its census, after its full checkpoint, never waiting on executor
     work while it holds it. A page freed between the census of the page map and the reading of
     retired bits would otherwise be reported as corruption. Today's arena collection frees
     temporary pages only, and takes no gate.
4. **Rollback of an abandoned transaction** (section 11.5, not built): one task each, relief while
   a writer holds a ticket on it. Its design must say what of its progress survives close, and keep
   22.4's order.
5. **Later**: uploads (section 14), page cleaning, stream segment index files (section 21),
   applying the log on a follower (section 15). Each takes its place in 22.4's order first.
6. **Foreground, and staying there**:
   - draining abandoned trees and publishing their runs (`AtollDb::drain`), part of when a
     transaction's end returns;
   - eviction, draining frame classes, and writeback under pressure (sections 9.4, 9.5);
   - a statement waiting for another transaction (section 17, R34): the wait graph keeps its
     records, cancellation, blockers and busy and deadlock outcomes; the executor never runs a
     connection's statement, and a job's completion only wakes the statement's owner;
   - waits for a transaction slot, the write gate or an epoch, whose producers are foreground
     holders, and waits for a frame, whose producer is a foreground holder or, for a frame held by
     a request in flight, that request's delivery owner (22.5).

### 22.10 What it must keep

The executor changes who runs each transition, never its order or its meaning:

- A writer refused room lets its page latches and the change gate go before asking for relief,
  and no runner executes its statement. Room's completion means "try again", not bytes granted;
  releases, a cut's cleanup, a change of runner and a checkpoint's failure all wake it.
- A transaction's end waits for a full checkpoint after its commit is durable and its trees are
  drained, and only when both the checkpoint's lag behind the log's end and its lag behind the
  first part of a transaction still running exceed twice the budget (`sql/mode.rs`, section
  11.4.1): no checkpoint could bring the redo point nearer than that part.
- A full checkpoint's ticket is answered by a checkpoint begun after the request, published before
  anyone is notified.
- Images of node pages before the value pages they name; the intent record durable before any
  metadata write; data files and the log durable before the superblock; deferred frees, erasure and
  the log's release only after the superblock is durable. The cut's gate and visibility, and the
  oldest snapshot's protection, as section 10.
- A checkpoint that fails abandons its cut, gate and protection, leaves the superblock in charge,
  records the failure, and runs no checkpoint until the database is opened again; waiters for a
  full checkpoint and later callers see that failure, while the room it held is given back. A log
  or page write failure fails as section 7.5 says. These stay distinct outcomes, never one failed
  wake.
- Preparation: whole segments, short writes treated as failures, the file synced, then its
  directory, then the prepared end published, admission woken, and the flush that padding awaited
  retried; a reused spare's directory made durable before it is used.
- A database closed cleanly leaves its log durable to its end, so a sequence's or a drain's writes
  that no commit flushed survive the close (R8; `AtollDb`'s drop today).

### 22.11 Failure

- **A task's error** is its job's (22.9, 22.10). An ordinary error (a `Result::Err`, busy, a
  deadlock, an IO error delivered through a request) stays an ordinary result everywhere: nothing
  below turns one into an abort.
- **A panic in Atoll ends the process, before anything it left half made can be read**, and the
  next open recovers (section 12). The whole process ends, other databases in it and on its ring
  included: this is not isolation per database. Every build Turso ships already does this on any
  panic: its release profile sets `panic = "abort"`, and the official and library release profiles
  inherit it. Those are the workspace's profiles; an application that builds Turso as a dependency
  decides its own panic strategy, so for builds that unwind (tests, and such applications) the
  rules below give the same outcome. Keeping a database alive in a process that unwinds needed
  poisoned guards held forever, failure epilogues owed across threads, rules for panics inside an
  unwind, a close for state nobody may touch, and poison that survives eviction; each audit of
  those found a further case. These rules replace them.
- **The guard is the boundary.** Every destructor that lets shared state go, publishes it, cleans
  it up or wakes anyone for it aborts first, before doing any of that, when its thread is
  unwinding (`std::thread::panicking()`): a page latch held exclusive (and the wrappers that clean
  up or publish before their latch is released: a frame being loaded, a page being written out),
  the release paths that hand a latch back unchanged, the checkpoint's state, the write gate and
  the change gate, metadata frames held for editing, a log reservation (whose drop gives back room
  and wakes admitters), and the maintenance gate. So in an unwinding build no other thread can see
  a latch free, a gate open, room given back or a contender woken between the panic and the end
  of the process; a native abort build never unwinds at all. Every lock that protects Atoll's
  shared state is one of Atoll's own: its latches and gates, and its wrappers of parking_lot's and
  std's mutexes, read-write locks and condition variables, the executor's queue, signal and
  contender mutexes included, each of whose guards makes the same check before it releases. The
  abort itself takes no lock and writes nothing first: a write to stderr takes its lock, which
  another thread may hold while it waits for the panicking one, and the process would never end. A
  census test lists every type in Atoll with such a destructor and fails on one without the check.
  A second lists every function that calls a release primitive (each `unlock*` of the latch, the
  drop of a lock's inner guard) and fails on one that does not check first, so a release other than
  a destructor, a latch handed back unchanged among them, checks too. No method of a lock wrapper
  (a `try_lock`, a read or write guard, a trait's method such as formatting) reaches the lock
  beneath but to wrap the guard it takes at once in a checked one, by a census of `locks.rs`:
  formatting a lock's value takes the checked guard. Two mechanisms keep every other lock out of
  Atoll. `clippy::disallowed_types`, denied in Atoll by `core/clippy.toml`, resolves a path to the
  item it names: a name brought in, `crate::sync`'s re-exports among them, to the lock it names, but
  a type alias to the alias, known by the path that defines it. So the file lists the lock and guard
  types of parking_lot and std and every alias of one that Atoll could name, and a test that follows
  aliases of aliases and names brought in under new ones, to a fixpoint, fails on one not listed. A
  second test scans Atoll's source for the spellings.
  (Built: `unwind.rs`, `locks.rs` and their censuses, with a child-process test of each check: a
  panic under a latch, a hold handed back unchanged while unwinding, a panic under a lock, a lock's
  value whose formatting panics, a guarded state, and the abort while another thread holds stderr
  and waits for the panicking one.)
- **The check is exact because no Atoll frame is entered while its thread unwinds.** Every entry
  into Atoll is a frame (below), and a frame entered while `std::thread::panicking()` is true (an
  application's unwind dropping a connection with an Atoll transaction open, a statement holding
  Atoll state, or the last handle of an Atoll database) aborts at once, before it takes anything.
  A drop with nothing to hand Atoll (no transaction open, no Atoll state) does not enter it. So
  any unwind a guard sees began inside the frame that took the guard, and a guard dropped during
  an unwind is always one a panic is passing through.
- **Frames, for a panic that holds no guard.** A panic between two guards (a change applied and
  logged under its latch, the latch let go, and the panic before what the change leaves to collect
  is recorded, section 7.5) leaves state out of step without any guard held. Every entry carries
  `#[turso_macros::atoll_entry]`, which opens its body with `let __atoll_frame =
  unwind::Frame::enter();`: entering aborts if the thread already unwinds, and the frame, a guard
  dropped last, aborts if a panic unwinds out of the body. The attribute moves every parameter the
  method owns by value into a local declared after the frame, bound by the parameter's own pattern
  (`_` and `mut` included), whether the body uses it, only borrows it or ignores it, so it drops
  inside the frame; the body's value is bound to a local too, so the temporaries of its last
  expression drop inside it. The names it adds are hygienic, so no parameter of the method can
  shadow one or be shadowed by one. A receiver taken by value would drop after the frame, and an
  `async fn`'s future owns its parameters before the frame is entered; both are refused when the
  code compiles. A parameter taken by reference is the caller's, dropped in the caller's
  code after the call. `return`, `?` and an ordinary error keep their meaning. A facade type that
  owns Atoll state drops it inside a frame: its `Drop` is an entry, and its last field is an
  `unwind::FrameEnd`, which, dropped after every other field, aborts if one of their drops panicked.
  The entries: every method, trait method and destructor of the facade (`AtollDb`'s `open` and
  `Drop`; `ConnTxn`; `SqlCursor`; the operations the VDBE holds across steps, `Ending`, `CreateOp`
  and `RetireOp`; `RefusingStorage`) and its functions (`probe`, `config_for`, `tree_id`,
  `full_checkpoint`, `drive`), through which alone a statement's step reaches Atoll; the
  checkpointing thread's loop; and, as the executor is built, a runner's step and `run_one`, and
  each delivery on any backend, from the callback through the result's publication and the wakes to
  the request's retirement (22.5). Opening and recovery are `AtollDb::open`, close its `Drop`.
  `Atoll` and a stream partition are reached from no code outside Atoll yet, so they are not facade:
  a stream partition becomes one with its first caller outside Atoll. Until its consumer, the
  stream catalog, is built, `stream` is compiled for tests alone, as is `floors` until the
  collector: a production build carries only what production calls, and an item whose only callers
  are tests is compiled for tests alone.
- **The facade returns no Atoll code to run later.** What it returns is plain data or a facade value
  (a `SqlCursor`, an `Ending`, the `AtollDb` whose accessors of Atoll's parts are `pub(in
  crate::storage::atoll)`, as are the facade types' other internals, so code elsewhere in the crate
  cannot reach them). Below the facade every module is Atoll's own: its root makes none public but
  `db` and `sql`, and `sql` none but `mode`, so the compiler refuses a path from outside into any
  other, however it is spelled or grouped. The plain data defined elsewhere in Atoll that the facade
  passes (`Statement`, `Guarded`, `GuardKey`, `SCHEMA_ID`) is brought into `sql::mode` and named
  there. A census test reads those modules' files: every item in them open to code outside Atoll
  (`pub`, `pub(crate)`, a path above Atoll) is a facade type, plain data, a function that is an
  entry, or a name of one brought in; every such method of a facade type, every trait method and
  destructor of one is an entry; a facade type owning Atoll state has its frame's end as its last
  field; and no signature, open field or variant there names a type of Atoll's but the facade's and
  plain data, so nothing the facade hands out reaches Atoll's internals.
  Tests inside the crate that call Atoll's internals directly run without frames, as today. A
  guard's check does not depend on a frame, so a test whose panic unwinds past a checked guard ends
  the process: it runs in a child process and expects the abort before the guard lets go. Today's
  `latch.rs` test that catches a panic while `Held` lives and expects the latch released becomes
  one that expects the abort with the latch still held, and the `should_panic` test in
  `buffer/kept/tests.rs` whose debug assertion fires with a metadata hold and an exclusive page
  alive becomes a child-process test. A test whose panic crosses neither a guard nor a frame stays
  an ordinary `should_panic` or catching test. A census lists every `should_panic` and
  `catch_unwind` in Atoll with which of the two it is. The checks are never turned off for tests:
  proof 9 exercises them.
- **Atoll catches no unwind.** No `catch_unwind` in `core/storage/atoll` outside tests.
- **Errors that end the process besides panics**: the ring's delivery thread meeting an error
  it cannot retry (22.5), and a pumped backend whose own stepping fails rather than a request of
  it. Either has lost the means to retire what it accepted; returning would strand every waiter
  on it and break close's promise. Each is stated where it arises and is no silent abort.
- **Section 7.5** says a panic ends the process. A panic is a defect in Atoll; recovery is the
  path crash tests already prove, and the process's supervisor or embedder decides what runs next.
  An application whose panic unwinds through live Atoll state (a test that fails while holding a
  connection mid-transaction) ends with an abort, after its panic message has been printed.

### 22.12 Close

The database closes when its last handle drops. Connections hold it, so by then no connection is
left: no admission, no transaction end, no statement. A transaction ended in its connection's drop
ran while the executor still ran. Nothing the executor or a waker holds keeps the database alive
(22.3). Then, in order:

1. **The log made durable** to the end each open log had when close began, while every job still
   runs: a flush may need a segment the preparer has yet to make. Every record a statement wrote is
   before that end; a record a job appends after it (a checkpoint's intent) belongs to work step 3
   abandons, which recovery treats as a crash would. A failure (a failed log, section 7.5) ends the
   wait at once, since the log fails every durable waiter; it is kept, close goes on through every
   later step, and the failure is logged and returned to an explicit close only after step 5.
   Until the preparers exist (22.16 step 8), close's wait is a `wait_durable` caller like any
   other: it comes back and runs an owed preparation step itself (22.5).
2. **Closing published**: no task is claimed or linked again, and every step in progress ends; its
   runner sees closing and leaves.
3. **Logical waits abandoned**: each task is closed, and its job's abandonment runs on the closing
   thread, outside any step: a checkpoint under way is abandoned as a failed one is (its cut, gate
   and protection released), a segment not yet published as prepared is left for the next open to
   prepare again, collection has nothing to undo. No task waits for another job's work after this
   step, so nothing waits for a producer that close stopped. Registrations are removed with the
   tasks (22.3).
4. **Physical requests retired**: the closing thread waits until Atoll's count of its files'
   requests submitted and not retired reaches zero (22.5), stepping a pumped backend meanwhile.
   Every backend retires every request it accepted (the kernel answers every request it was given,
   with a result or an error; a pumped backend delivers each when stepped), so the wait ends; a
   device that never answers holds close as it holds any write. A request cancelled or abandoned by
   a statement long gone counts until its backend lets it go, so no write lands on a file after
   close returns, and a database opened again in the same process cannot have its pages
   overwritten by the one before. A job's continuation submits nothing once closing is published;
   a backend's own work on a request (the rest of a partial write, a cancel) goes on until the
   request retires.
5. **State dropped, runners joined**: every job's state is dropped, every worker joined, and every
   embedder call in progress has returned.

Close at a step boundary leaves the files in a state the crash tests cover, and the next open
recovers from the superblock in charge. An IO error while draining is logged and the drain goes on.
A panic during close ends the process (22.11).

### 22.13 Bounds

- **Tasks**: one checkpoint coordinator, plus one preparer per open log (the database's and each
  open partition's), plus one collection task per slot (W), plus at most one rollback per slot,
  plus a fixed number of image slots (I). Open logs are bounded only by what the embedder opens,
  each already holding a log's memory; an embedder that wants a fixed ceiling bounds the
  partitions it opens. A header is fixed in size; tasks are created and freed with their
  subjects, never per wake.
- **Headers alive**: the tasks alive, plus removed ones still named by something that outlives
  their removal: a request in flight (one per request) or a logical completion a removed task
  waited on (one per such wait). Lists are woken under their own mutex and never detached (22.2),
  so no list being woken holds a header past its removal.
- **The run queue, the idle runners and the to-wake list** are intrusive lists: each runner
  (worker, embedder thread, or caller running the executor) brings its own node, provisioned with
  it (a caller's with its connection; a connection runs one wait at a time), and takes it off when
  it stops waiting, is cancelled or drops. No capacity, no allocation. A drain wakes at most the
  nodes its own operation moved (22.3).
- **Signal and contender lists**: one fixed array each, of one entry per task that may watch it or
  per declared taker (22.2). A notify or a release wakes at most that array, however often they
  come.
- **Wakers per completion**: an executor task waits on its own requests with one waker each, kept
  inline. A completion with more than one waiter (a foreground thread beside a task, or several
  waiters on one executor completion) spills Turso's waker storage to the heap on each
  registration; that is the completion's waiters' allocation, not the executor's.
- **Tickets**: one per writer waiting; one per task in a logical wait, which names one job, or the
  image slots as a group (at most I).
- **Flight sets**: each job's own limit: `WRITES` (32) and `checkpoint_write_bytes` for the
  checkpoint, the preparer's one write in flight as today (22.9 item 2). A request holds its place
  until it retires, not merely until it finishes, so a cancelled request still owning its buffer
  still counts. Image bytes are counted from building until the coordinator takes them.
- **Per ring**: one delivery thread and its stack, an eventfd and its standing poll, and one fixed
  buffer of one completion queue's entries, owned by the delivery thread alone (22.5). Rings are per
  IO, as many as the embedder opens. The ring's overflow of submissions past a full submission queue
  is the submitters' and is not capped by the queue's size: IO allocation is outside the executor's
  promise.
- **No allocation by the executor per wake or per wait**: its words, queue, lists, tickets and
  flight sets are sized when their tasks, subjects and runners are created. Measured with an
  allocation counter over repeated cycles of every class, both kinds of wait, promotion,
  concurrent notifiers, and subjects created and removed.
- Section 10 item 11's metadata copies stay proportional to metadata, as there; the executor adds
  nothing that grows with the database.

### 22.14 What is reported

Per class, the tasks queued and the age of the oldest. Per job, steps, page visits, CPU time
stepping and wall time stepping apart, and time waiting per kind of wait. Per runner, time parked.
Per ring, delivery passes and requests delivered, and requests submitted and not retired. The IO
backend in use and, if it fell back, why. Each slot whose collection ended on an error. These say
whether a job is CPU-bound, IO-bound or starved, which 22.8 needs.

### 22.15 Proving it

Each invariant has a test that fails with the defect reinstated (the mutant named), and reports how
many cases it ran; a timeout is not a kill. Models use instrumented atomics, locks and threads
through explicit loom abstractions, as the epoch models do: building production code under
`cfg(loom)` alone instruments nothing (`core/sync.rs` keeps std atomics there). A model of one part
does not stand for the composition: each item below runs its schedules through the parts as they
compose, the real `Completion`, latch and backends included where named.

1. **No lost wake.** Loom: the end of a step (mutant: a load then a store); a signal's acquire read,
   registration and second read (mutants: relaxed counter, which reads a new count over an old
   condition; no second read); the threshold handshake with its two fences (mutants: either fence
   removed; the producer's publication made an exchange with no fence after it; the producer
   skipping its fence when it finds no threshold), its least threshold kept under the mutex with
   the list (mutant: a reset outside the mutex erasing a newer registration), and two producers
   changing different parts of a composite value (mutant: a producer computing the whole before
   its fence); notifies arriving without pause while tasks register, each notify waking at most its
   list (mutant: a notify that loops while notifies keep coming); a runner linking its node idle
   against a link (mutant: link without taking an idle runner); a guarded state's release and its
   contenders, including a taker that succeeds on its retry and removes itself, and a stale
   contender woken late; wait-any over three signals, each source notified alone; a task removed
   while it is listed, closed under the queue mutex and unlisted after it (mutant: the queue mutex
   held while taking a signal's, a deadlock loom reports). Shuttle: workers, wakes and releases with
   real `std::task::Waker`s.
2. **One owner, one queue entry.** Loom over the actual queue, not the word alone: `Queued` exactly
   while linked, nothing terminal linked, no link after close, one runner per task; claims, named
   claims of `Idle` and `Queued` tasks, ends, class moves and close racing wakes (mutants: the
   `Idle` to `Queued` exchange made before taking the mutex; a claim that checks then stores;
   relaxed publication, caught by loom's cell checks; `Done` ending as `Idle`).
3. **A step's work is its unit, and the IO stays deep.** Page visits counted per step and asserted,
   and the total of a phase's work against its size: a leaf naming many out-of-line values, each
   with its directory page, and versions chained across many arena pages; a large dirty set and
   swap inventory; a flush with many writes run from a step (mutants: the 4,000-row phase loop that
   images 4,044 pages in one call; an image built whole in one step; the dirty set taken and sorted
   whole; a flush looping over its turns). With a ring, the checkpoint keeps its flight set full
   while each step submits one write (mutant: `Wait` after every submission, one write in flight);
   a cancelled write holds its place until it retires (mutant: the place freed at finishing,
   letting twice the limit be owned), and a task waiting for a place is woken by the retirement
   (mutant: waiting on the already-finished completion). A flag set for the length of a step,
   reset on unwind, with `IO::wait`, blocking and parking asserting outside it and `run_one`
   refusing to nest (mutant: a step that calls `IO::wait`).
4. **Relief reaches the writer.** Deterministic runner: a relief checkpoint waiting on admission
   while independent relief tasks stay queued, the preparer still running (mutant: no ticket for
   the dependency); demand changed after the waiting task has returned `Wait`, through every hop,
   and dropped again; several parents and the preparer's own due level at once (mutant: one ticket
   dropped clearing another's); a due notify placing the checkpoint in the due list (mutant: the
   notify wakes without setting its level); a trigger arriving between a step's look and its clear
   (mutant: the clear not checked against the counter, losing the newer level); the image slots
   waited on as a group, each carrying the coordinator's class; two refusals of different sizes, the
   smaller admitted first (mutant: one shared "pressing" cleared); a wait naming an earlier job
   asserting.
5. **Order of claims.** Strict by class, first in first out within one, a task linked into a higher
   class claimed next (mutants: requeue at the head; a lower class's turn finished first).
6. **Every request delivered by its owner**, on each backend. The ring, with no thread stepping it:
   a synchronous waiter, a cooperative future and a nonblocking embedder, each with an unrelated
   request delivered first and no later caller (mutant: delivery left to whichever waiter steps the
   ring, a pass bounded, as in this design's first revision); requests nobody waits on; requests
   queued in the overflow while the queue was full (mutant: the wait entered without moving them
   in); the pass bounded while a relief task is ready under a stream of submissions; a callback that
   submits more; a callback that waits or makes a synchronous file call (asserted, never a hang);
   the census of callbacks, failing on one that does file work synchronously (mutant: today's
   `prepared`); the last handle dropped from outside and from inside a callback (mutants: the thread
   holding a handle, so the drop never comes; the drop joining its own thread); a refused start. A
   completion whose callback returns an error: the error is the result, and no waiter, whether it
   registered before, during or after the callback, sees the completion finished before the callback
   returned (mutant: the result published before the callback runs, which reports success for a
   short read). A pumped backend, with its runners: `CrashIO` with an unrelated request queued
   (mutant: one step, then park); a waiting preparer's write queued while other tasks stay ready
   forever (mutant: stepping only with nothing to claim); two runners stepping it while one
   delivers; the simulator's delayed requests. Every backend: partial writes, errors, a submission
   that queues and then fails to enter (left queued for the next entry where the kernel says the
   refusal may pass, ending the process otherwise; never a refused submission that was accepted), a vectored write whose first part is accepted and second refused (its
   accepted part owned and retired, its error reported); every blocking caller of 22.5 run through
   `IO::wait`; cancelled requests in the write-ahead log, the pager and the sorter drained to
   retirement (mutant: drained to finishing, the buffer still the kernel's), the sorter's group with
   a child added and its read refused, and a child never submitted (mutant: the drain waiting for
   retirement from a request nobody accepted); an outer group listing only an inner group whose own
   completion was never submitted while a write under it is accepted, its buffer still the backend's
   (mutant: a flat walk that takes the inner group as never submitted); an error of the ring itself
   injected into the delivery thread with accepted requests outstanding (mutant: the thread
   returning, its requests then never delivered); a pumped backend's own step failing; injected
   setup, thread and registration refusals. From step 3, the log's callbacks with the interim
   adapter: a fresh segment needed with an admitter waiting, and with none until later, file and
   directory synced before the prepared end is published (mutant: the owed work left in the
   callback, caught by the census); only a durable waiter, no admitter and no later statement,
   which must become durable, in three cases: its own flush meets padding; its flight set is full
   at the call, so its flush never reaches the padding, and a later write's callback is the first
   to meet it while the next segment is in flight; and the same with that callback the first to
   find a segment needed (mutants: the waiters finished only through a separate progress
   completion, which leaves the durable waiter parked; `Done` for a finished completion without
   reading the durable position, which reports durability below its target; a wait registered
   after the word is read, which misses a move between). Padding met while the data sync is in
   flight wakes no one until the sync lands (mutants: the directory synced before the data sync
   has landed; a completion finished with the word still taken or a log lock held). A spare
   renamed; a memory IO that delivers the whole fill and its sync inside the starter's first
   submission (mutant: the word set in flight after that submission, so the sync's landing finds
   it still taken and is lost). A durable wake holding a waiter off the stack while a move to
   needed, and in another schedule to owed, takes the stacks, and while a failure takes them
   (mutant: the wake putting the waiter back and looking only at the position, which leaves it
   parked). Another request's failure injected during a starter's create and during a finisher's
   directory sync: nothing is submitted after, and every waiter gets the error. The census of
   callers fails on a call site that does not call again.
7. **Close loses nothing and waits for nothing it stopped**: at every step boundary; with the
   checkpoint waiting on admission and the preparer queued with nothing submitted (mutant: close
   waiting for every task's completion to finish); with a write submitted, delivered by the kernel
   but not yet taken, mid-resubmission, cancelled and finished but not retired (mutant: close
   waiting on finished rather than retired), an inline request (mutant: counted after its
   submission returned), a request a callback submits as its parent retires (mutant: counted after
   the parent's retirement, the count passing through zero), a read a dropped statement abandoned,
   and a callback running on the delivery thread; with a late waker. A buffer canary proves none
   freed before its request retired, nothing writes a file after close returns, no task runs after
   close, and reopening matches the model: a database closed with a write still in the kernel and
   reopened at once in the same process, the old write delayed until after the reopen's first
   write (mutant: close returning before retirement, the old write then landing over the new). A
   clean close with a sequence's and a drain's records and no later commit, reopened: the records
   are there (mutant: no final flush, or the flush after the preparer stopped). A failed log during
   close's flush ends the wait with its error, and close still retires every request before it
   returns that error (mutants: the flush waiting on a log that failed; close returning at its
   first error).
8. **Releases.** (a) A compile-fail test that the guarded state has no other accessor. (b) A release
   lets go, then wakes contenders only (mutants: wake before release; no wake; a holder woken by
   its own release, whether it held at its second try or at its first while still listed from an
   earlier call). (c) The checkpoint's waiters, completions with arbitrary wakers, are woken with
   no lock held: a waker that waits for a full checkpoint again does not block (mutant: the
   waiters' mutex held through the loop), and one that takes the checkpoint's state finds it free
   (mutant: the waiters woken by the step, under the state's hold).
9. **A panic ends the process before anything it left can be read.** Each in a child process the
   test spawns, with the real latch and guards, the parent then reopening the database and comparing
   it with the model. The child ends by abort, and an observer thread in it, which records to a file
   and syncs whatever it sees, has seen nothing between the panic and the abort: no latch the
   panicking thread held free, no gate open, no room given back, no contender woken (mutant: an
   abort only at the outer frame, which lets the inner guards go first; a wrapper that cleans up
   before its latch's check). Cases: a panic inside a statement's step, a facade method and its
   `Drop` (a field whose drop panics), a facade iterator's `next`, a runner's step, `run_one`, a
   delivery on the ring (in the callback, and in a waker after it), a pumped step, opening, recovery
   and close; a panic in a local dropped after the frame's last statement, in a capture, and in an
   owned parameter the body ignores (mutant: the attribute leaving a `_` parameter unbound, so it
   drops after the frame) and one it only borrows, and one beside a parameter named as the attribute
   names its own (mutant: names that are not hygienic, so the parameter is never moved), with a borrowed `&T` whose drop runs in the
   caller after the call, outside the rule; a panic past a checked guard in a test without a frame
   (mutant: the check made only inside a frame); a panic holding one of the executor's own mutexes
   and a parking_lot lock wrapped by Atoll (mutant: a raw parking_lot guard, which lets the state go
   before the abort); a latch handed back unchanged while unwinding (mutant: the release without the
   check); a lock's value whose formatting panics (mutant: formatting through the raw lock); the
   abort while another thread holds stderr and joins the panicking one (mutant: a message written
   before the abort, which waits on stderr for ever); an embedder's waker that panics; an application's unwind dropping a connection
   with an Atoll transaction open, which aborts on entry (mutant: the entry let through, its guards
   then unable to tell the old unwind from a new one). A `Result::Err` through every frame returns
   normally (mutant: a frame that aborts on an error). The census tests fail on a destructor that
   releases shared state without the check, on a release path without it, on a wrapper method that
   reaches its raw lock, on an alias of a lock, at any depth, missing from the lint's list, and on a
   facade type, method (`pub(crate)` ones included) or trait implementation without its frame, or a
   facade signature handing out an Atoll type. A path from outside into one of Atoll's own modules
   does not compile. A `catch_unwind` inside Atoll fails a census too.
10. **A quiet backlog drains.** Inline and background collection racing with no begin after
    (mutant: a claim lost read as nothing to do); a page linked into a slot that already had credit
    (mutant: woken only when credit rises from zero); each part of the cutoff changing alone, with
    no transaction ending and no later begin: the checkpoint releasing its cut, a snapshot raised
    for a read-committed statement, a snapshot's final publication after its first of zero, the
    horizon moving (mutants: each producer's notify removed; each producer's fence removed), over
    the actual `collect_cutoff`; a slot ended by an error stays ended through wakes.
11. **The maintenance gate**, once collection frees real pages: collection at each boundary of an
    integrity check's census finds no false corruption, over every path that frees a real page
    (mutant: no gate).
12. **Runners agree.** The checkpoint, collection and segment tests under workers, callers and the
    deterministic runner alike, judged by the same durability and model oracle, not the same order
    of steps. In callers mode: a fresh segment needed at a checkpoint's intent and at a writer's
    admission with no worker and no later statement (mutant: a waiter that returns the executor's
    completion without running the queue); two callers each running the task the other waits on; a
    cooperative poll returning to its host after one unit and polled again only when its `Waker`
    is invoked (mutant: a poll that runs the queue empty); a sleeping host whose only work is a
    prerequisite queued by a physical completion, with no worker: the host's `Waker` is invoked by a
    drain after every executor mutex was released (mutants: the host's node treated as a thread and
    unparked; its `Waker` invoked inside the signal or queue mutex; a nested drain); several links
    in one notify moving several nodes, each invoked once; one node re-registered and moved again
    without end, each operation draining at most what it moved (mutant: drain until empty); two
    nodes under arrivals that never stop, the older popped first (mutant: a last-in-first-out list,
    which leaves it waiting), and with two drains at once each node invoked exactly once, in
    whichever order the drains run (only a single drain asserts the order of invocation); a cancel
    racing a pop (mutant: the `Waker` reached through the node after the mutex); a host `Waker`
    whose clone and drop record whether an executor mutex is held (mutants: a registration cloning
    it under the queue mutex; a cancel or a replacement dropping it there); a caller cancelled
    while its node is idle. A 12-way stress of the workers, reported apart from replays.

### 22.16 Order of work

1. **`StateHold` (built, a92a1d667), then its amendments (built)**: `Guarded` with the state
   reachable only through it, contenders woken under their mutex instead of the thread,
   `wake_waiters` waking its completions with no lock held, the state let go first; proof 8.
2. **The panic boundary** (22.11): the check in every destructor that releases shared state, every
   lock in Atoll on its own wrappers, with their census; `#[atoll_entry]` on every entry, an entry
   made while unwinding aborting; the facade, every other module Atoll's own behind it, returning no
   Atoll code to run later, with its census; no `catch_unwind` in Atoll; the tests that panic past a
   checked guard moved to child processes, with their census; section 7.5's new rule; proof 9.
   Before the delivery thread, whose deliveries it frames, and before any job moves. Built.
3. **Delivering the IO**: the log's interim adapter first, with its preparation word, completions
   that mean call again and the census of their callers, so no callback does file work from this
   step on; a completion's three ownership states and a group's aggregate over its physical requests
   and retirement reported apart from finishing, submissions that accept or refuse whole (a vectored
   write's accepted parts included), `IO::wait_retired` and the drains on it, and Atoll's count; the
   ring's delivery thread as the one drainer, with its callbacks' rules and census, its frames, its
   errors, overflow and lifetime; pumped ownership, one step per task step, and a failing step; the
   three kinds of waiter, with every blocking caller of 22.5 on `IO::wait`; proof 6. The ring stays
   opt-in.
4. **The checkpoint's units** on today's driver: cursors per phase and inside an image, the dirty
   set walked in order, the intent record's size checked first, a flush turn per step, one unit per
   step on every backend, the flight set kept full and released at retirement; proof 3; the
   sustained run against before.
5. **Profile on the ring**, still opt-in (22.8), of step 4's checkpoint, with the interim adapter.
6. **The executor's core**: headers and words, the run queue and intrusive idle nodes, signals,
   threshold signals and their handshake, contenders, levels with the counter-checked clear, tickets
   and the order of jobs, the lock order, external nodes with their `Waker`s cloned and dropped
   outside every mutex, the first-in-first-out to-wake list and its budgeted drains, executor
   completions with the callers' blocking and bounded cooperative rules, close with its final flush,
   logical abandonment and physical drain, the four runners; proofs 1, 2, 4, 5, 7 and 12's callers
   and external-host cases on test jobs, no real job moved.
7. **The checkpoint onto it**: one worker replaces `Checkpointing::Thread`, callers replace
   `Checkpointing::Writers`; every checkpoint and kept test under each runner; the sustained run
   against step 4's, and the profile again.
8. **The segment preparers**, every open log, replacing the interim adapter and the callback chain
   in the same change; the callers' fresh-segment cases of proof 12 first.
9. **io_uring as the default**: the census of callbacks clean, its fallbacks, the report, explicit
   choices kept, and the validation and latency measurement of 22.6, run over Turso's own suites as
   well as Atoll's, since the default reaches every database; then the default.
10. **Collection**: a task per slot, begins as named claims, IO waited for, the page-linked signal,
    and every producer of the cutoff on the threshold handshake, its fence measured against the
    statement cost with a task waiting and without; proof 10. The maintenance gate and proof 11
    with the first collection of real pages, after its place in 22.4's order is proved.
11. **Image slots**, only if the profiles of steps 5 and 7 show the images are the CPU; measured
    against step 7, sustained writes and foreground latency both.

### 22.17 Not chosen

- **Rust futures on an external runtime.** A future has a size the compiler knows, Turso's
  `Completion` already implements `Future`, and a future needs no runtime of its own, so neither
  memory nor embedders rule them out. Plain state machines are chosen for what an audit and a model
  check need: each step's unit and each wait are written out, and loom models them as they are.
- **A waiting thread leading the ring, with leadership handed over** (this design's first
  revision). It kept a synchronous waiter's own delivery on its own thread, but every repair of it
  added a rule: a follower promoted after its request was delivered must not block in the kernel;
  a cooperative future has no blocked thread to hand leadership to, so a bounded pass left its
  request undelivered; and every caller that steps the ring, in Turso and its SDKs, had to join one
  protocol. A delivery thread makes delivery the backend's property instead. Its cost, a wake
  across threads for a synchronous waiter whose request the device has not finished before it
  parks, is measured before the default switches (22.6).
- **Waiters delivering ready entries beside the delivery thread** (the second and third
  revisions): a second dispatch route and a batch buffer per drainer, for a latency nobody has yet
  measured. Designed again only if 22.6's measurement asks for it.
- **Keeping a database alive in the process after a panic** (the second and third revisions):
  guards poisoned and held forever, failure epilogues owed across threads and run before each API
  returned, waker batches caught per waker, a close for state nobody may touch, files quarantined
  against reopening. Every build Turso ships aborts on a panic already; the machinery served only
  builds that unwind, and each audit of it found a further case (a panic inside an unwind, an
  epilogue taken by another thread, poison lost when a frame was evicted). One abort replaces it.
- **An abort only at the entry frame** (the fourth revision): in a build that unwinds, the inner
  guards are let go before the frame is reached, and another thread can read and commit what the
  panic left; the guard that releases is where the abort has to be.
- **Frozen rounds for the deterministic runner**, which stepped a round's tasks before a higher
  class linked meanwhile: one queue discipline for every runner keeps one oracle for proofs 4 and 5.
- **A thread per job**: one wake protocol per job, the situation this replaces.
- **A share of the workers kept for maintenance**: it would take steps from a writer waiting for
  room. Starvation is reported instead (22.4).

## References

- Leis. LeanStore: A High-Performance Storage Engine for NVMe SSDs. PVLDB 17(12), 2024.
- Alhomssi, Leis. Scalable and Robust Snapshot Isolation for High-Performance Storage Engines.
  PVLDB 16(6), 2023. [doi:10.14778/3583140.3583157](https://doi.org/10.14778/3583140.3583157)
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
