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
- **What is logged** is what recovery replays (audit 17): a transaction's changes by key, kept in
  memory until its commit and published then, in parts ahead of its Commit when they outgrow a log
  chunk (`SPILL`), the last part in the Commit with its timestamp (`COMMIT`); changes made outside
  any transaction, such as a sequence's or a drain's, in a run of their own (`RUN`); a
  checkpoint's metadata intent (section 10). Nothing of a transaction reaches the log before its
  commit, and a rollback logs nothing. No page change is logged: a checkpoint writes the pages as
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

A panic stops writes the same way. A thread that panics holding a latch, or between applying a
change and recording what the change leaves to collect (a todo item, section 11.5), can leave the
log, a page and a slot's todo list out of step, so the database refuses writes until it reopens.
Recovery then frees every page still marked retired and removes every tombstone left behind
(section 12), which also covers a rollback that could not finish. The stop on a panic comes with
garbage collection (section 20).

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
  once it is given back. Only a quiet frame is lent, as its holder writes it plainly; it goes back
  when its block drops, from whatever thread or IO callback drops it. Beside the region are what
  is fixed per process (code, stacks, connection and statement state), what scales with the
  region (the headers and the hash table), and three working memories, each bounded where it
  arises: the image of a value page being written and the copy it is made from (two pages of its
  class), the metadata pages a cut keeps (section 10, item 11), and a transaction's changes, held
  until its commit writes them to the log. Not built: a value page's image written from its frame,
  metadata pages in lent frames, and a transaction's changes past a chunk in temporary pages.
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
Sampling and the choice skip batches already draining. Value per byte is
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

   **The room for kept copies** (Clay's ruling of 2026-11-05) bounds the copies of data pages;
   the metadata's are accounted apart (item 11). Each copy is in a frame the pool lends (section
   9.1), charged to a room of `max(R, min(B/8, 256 MiB))` bytes, B the budget and R the largest
   request a change makes, three node pages of 64 KiB, an initial choice that `kept_bytes`
   overrides. The ruling's floor was a page of the largest class, P; it was set while the copies
   lived beside the frames, and with them in the frames it would be most of a small pool. A charge
   is given back only when its copy drops and gives its frame back, wherever it then is. A change latches every page it will change and asks for room for the
   copies they need before its first edit, all or none; an edit of a page the cut owes that asked
   for none fails an invariant check rather than copy past the limit. A request that finds no frame
   of its class free without an eviction is turned down as one past the room is: the change lets
   go of what it holds, takes a frame holding nothing, which may evict a page, and goes on with
   it. Only node pages are kept,
   64 KiB at most, and a change asks for three at most, so every request fits. Refused, it lets go
   of its latches and the change gate, and the writer steps the checkpoint itself, as an embedded
   database may have one connection and no other thread, until the room takes what it asked for,
   and no further. While a writer waits, the checkpoint images kept copies first. How many images
   that takes is not bounded by the request: with pages of several sizes, or other writers taking
   the room given back, it can take more; the time writers wait is measured, the longest wait with
   it. While someone else steps the checkpoint, the writer waits on the room, woken by every copy
   given back and by every stepper leaving. A free never waits: with no room, the page goes to the
   checkpointer unchanged since the cut, imaged as it stands, and the next checkpoint frees it.
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
   default 0.4 times the pool, the ratio of Haubenschild's example. A writing statement steps the
   checkpointer only once one is due (section 17).
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
plain stores into a buffer of its own. A statement rolled back drops its changes. The Commit
record carries them after its timestamp; what does not fit one log chunk goes ahead of it in Spill
records, each carrying the transaction's start. Changes made outside any transaction, a sequence's
value and a tree drained, go out at once in Run records. Replaying the bodies in log order through
the trees (`replay.rs`) rebuilds every tree: the replay oracle checks it against the database that
published them, and recovery replays them past the checkpoint in charge (section 12).

Step 1's latch comes with garbage collection (section 20). Until then a commit marks its slot
without it.

### 11.5 Rollback and garbage collection

Rollback is built in `txn/rollback.rs` and `txn/mod.rs`, except three parts, which come with
garbage collection (section 20): the timestamp and cutoff of the end's steps 1 to 3, the
re-recording of a tombstone put back, and the background rollback of a transaction abandoned open.
Until then the end frees the slot at once. Garbage collection itself is the next part of step 7 (section 20).

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
      under its claim, and only then hands it to its transaction. It also takes the claim of one
      idle slot and runs one step of a pass there, in the same way.
    - **Background passes** take the claim of any slot, free or occupied. The owner meanwhile
      appends its own entries above the eligible range: the keys are disjoint, and the trees' node
      latches order the changes.
    - **Never right after an acknowledgement.** Turso runs no code between a step returning `Done`
      and the client's next call. Collecting in the commit would add latency once durability is
      met, and fresh garbage is mostly not yet eligible, since older snapshots still see it.
  - **Work per step.** A step does at most one log chunk of records, and visits or drops at most one
    leaf of entries. Frees resume per page, so a large value spans steps.
    - Background passes are driven by a maintenance driver the embedder provides, a thread or its
      connections, as checkpoints are. Atoll starts no threads, so without a driver nothing is
      collected.
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
  - **Version indexes** are dropped by range, without reading the entries.
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
   its parts, and every run (`Apply`). A body at or below `C` is in the checkpoint's pages already.
   A put lands settled, since no snapshot survives a crash; a delete removes its key; a drain of a
   tree the replay made, or the mapping held, frees it whole.
4. **Record the gap.** If the log reopens past its end, it is zeroed from there durably and the gap
   recorded in a superblock before anything is appended.
5. **Restart the clock** above every timestamp read: the superblock's clock, every transaction's
   start and every commit timestamp replayed. Every version left in a page is committed.
6. **Checkpoint.** A full checkpoint makes what was recovered the state in charge.

No analysis of losers, no undo, no compensation, no page LSN: nothing of a transaction reaches the
log before its commit (section 11.4), and no page change is logged at all.

**Threads.** Turso core starts no threads. Any number of threads may call `Opening::work`: one
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
    - A panic in the middle of a change refuses writes until the database reopens.
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
   - fail-stop on a panic in the middle of a change (section 7.5);
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
