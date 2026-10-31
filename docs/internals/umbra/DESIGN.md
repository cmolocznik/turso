# Umbra storage

A storage mode for Turso beside the SQLite pager and MVCC, after Umbra (Neumann and Freitag,
CIDR 2020) and LeanStore. This document is the design for the production engine. The code on the
`umbra` branch is a prototype of parts of it (`core/storage/umbra`): size classes and the page
header, the versioned latch, an out-of-place page store whose page table is held whole in memory,
a buffer manager with swizzling and cooling, a redo-only log, and checkpoints of that page table.
Section 16 says in what order each piece is replaced.

## 1. What it has to do

- One design from 100 MB to 100 TB. Memory follows the working set: nothing held in memory grows
  with the size of the database except per-file summaries of a few bytes per gigabyte.
- Local disk works as a cache; object storage can be the main store (section 14).
- Run PostgreSQL workloads: many concurrent writers, read committed and snapshot isolation
  (serializable later), long-running reads, transactions larger than memory, crash safety, and
  commit latency that checkpoints and page writeback do not move.
- Everything Turso's SQL layer has keeps working: rowid tables, WITHOUT ROWID tables, every index
  and index method, both the SQLite and the PostgreSQL frontends.
- No SQLite file compatibility. A database is in this format or it is not.
- Deleted data leaves the disk promptly: a page copy's disk space is erased when it is freed, and
  log segments are erased when they are recycled.

## 2. The main choices

| Choice | Instead of | Why |
| --- | --- | --- |
| Variable-size pages; the smallest class is 4 KiB and B-tree nodes default to 16 KiB | Umbra's 64 KiB smallest page | A random read of a database larger than memory, and a random update's writeback, both cost one page. At 64 KiB that is 4 times 16 KiB and 8 times PostgreSQL's 8 KiB. Large records still get large pages. |
| Pages written out of place through a page table (as the prototype does) | In-place writes with full-page images in the log (PostgreSQL) or a doublewrite buffer (InnoDB) | No page is ever torn and the log carries no page images. Immutable page copies are also what object storage needs. |
| The page table and the free-space maps are ordinary logged pages in the buffer pool | The whole page table in memory, rebuilt by scanning at open (the prototype) | 100 TB of 16 KiB pages is 6.4 billion entries. |
| Swizzled child pointers, plus a table of resident pages by page id | Swizzling alone; vmcache | A swip allows one reference per page. Undo pointers, and any later graph index, need lookups by id. vmcache needs virtual address space as large as the database, and Linux gives 128 TiB with 48-bit addresses (vmcache §3.6). |
| One log for the whole database, filled in parallel, with group commit | One log per worker (Haubenschild et al., SIGMOD 2020) | One total order keeps replication, point-in-time recovery and recovery itself simple, and makes it safe to show a commit before it is durable (section 9.6). The cost is measured: in that paper a shared log (Aether) peaked at 180K TPC-C transactions per second against 857K for per-worker logs. 180K is still 4 times what Umbra's authors measured for PostgreSQL (VLDB 2022 §4). We revisit this only if measurements on our hardware show the log as the limit. |
| Undo kept in logged undo pages in the buffer pool, old versions rebuilt from it (as InnoDB does) | Umbra's in-memory-only versions (Freitag et al., VLDB 2022) | Umbra aborts a writer that runs out of version memory and asks the user to rerun it as a bulk operation (§3.2.3). A PostgreSQL replacement must run an UPDATE of a billion rows and a day-long report. Undo pages spill to disk like any page, and the log can be cut however long a transaction runs. |
| Visibility through each worker slot's commit order and one global clock (Alhomssi and Leis, VLDB 2023) | PostgreSQL's snapshot of running transaction ids | A snapshot is one integer; a visibility check consults another slot's commit order only for versions that slot wrote. |
| A page copy is durable before its remap is logged, and every write is announced in the log first | Copies that become durable at the next checkpoint (the prototype) | A crash leaves every remapped copy readable, and no copy that nothing refers to is left on disk holding data. |
| Log writes go with O_DIRECT into pre-zeroed segments, and each flush is padded to 4 KiB | One growing file with holes punched in it (the prototype) | An append never changes file metadata, so a commit is one device write. Bytes already durable are never written again, so a torn write cannot take committed records with it. |
| Background work (page cleaning, checkpoints, purge, preparing log segments) is stepped like any operation, by connections or by threads the embedder provides | Threads started by the engine | Turso core starts no threads. A server can still dedicate threads to it. |

## 3. Files

```text
{db}              control file: two 4 KiB superblocks, written in turn
{db}-data/c{k}    one data file per size class k
{db}-log/{n}      log segments by number, and prepared spares
```

The newest superblock that reads back whole is in charge. It holds: a magic number and format
version, the database id (128 random bits), the creation parameters (class range, log segment
size), the checkpoint number, the redo point, per class the slot of the page-table root and of the
free-space root, the clock floor (above every timestamp ever logged), and a CRC32C. A superblock is
written with O_DIRECT and is durable when the write completes.

## 4. Pages

- **Size classes:** 4 KiB × 2^k for k = 0..11, so 4 KiB to 8 MiB. Each class has its own data file
  and its own virtual memory region in the buffer pool (Umbra §2.1).
- **Header (24 bytes):** `lsn: u64 | crc32c: u32 | class: u8 | kind: u8 | flags: u16 | pid: u64`.
  The checksum covers the whole page with its own field taken as zero. It is checked on every read;
  a copy that fails is reported as corruption with its page id and slot, never repaired silently.
- **Kinds:** page-table directory, page-table map, free-space directory, free-space bitmap,
  transaction slot table, undo, B-tree inner, B-tree leaf, blob. Each kind provides three things:
  how to find the swips in its pages, how to replay its log records, and how to check its structure.

## 5. The page store

### 5.1 Page ids, slots and the page table

A page id is `class: 8 bits | number: 56 bits`. A slot is a place in a class's data file. Every
writeback goes to a fresh slot, so a page's id never changes and its slot does.

The page table maps each page to its state:

- `0`: the number is free;
- `1`: allocated, with no copy on disk yet;
- `slot + 2`: the slot holding the current copy.

Because a free entry is a free number, the page table also allocates page numbers. Numbers are
reused lowest first, which keeps the table dense after churn.

The table is a radix tree of pages, one tree per class: a root, directory pages and map pages,
64 KiB each. A map page holds 8,189 entries. A directory entry holds a map page's slot and its
count of free entries, so a free number is found without reading map pages that have none. Three
levels cover 5.5 × 10^11 pages per class.

Page-table pages are not in the page table themselves. Each is found through the slot its parent
records, and when resident through the resident table (section 7.2). The roots' slots are in the
superblock.

### 5.2 Free space

Free slots are tracked the same way: per class, a radix tree of bitmap pages (one bit per slot,
524,096 slots in a 64 KiB bitmap page) under directory pages that record each bitmap's free count.

Allocation takes free slots near an allocation cursor. A batch of writebacks takes adjacent slots
where it can, so the batch goes out as one large write. When nothing is free, the file grows
geometrically: by an eighth of its size, at least one batch, preallocated with `fallocate`.

### 5.3 The life of a slot

1. **Intent.** A writeback batch takes free slots and logs a write intent naming them. The intent
   is durable before any of those writes starts.
2. **Write.** Each copy is written and is durable when its write completes: FUA writes
   (`RWF_DSYNC`) under O_DIRECT, or else the whole batch's writes followed by one `fdatasync`.
3. **Remap.** `Remap { pid, new, old }` is logged. The map entry points at `new` and its bit is
   set.
4. **Free.** Once the log is durable past that remap, `old` is erased (a hole punched, or zeros
   written where the file system refuses) and its bit cleared.

Out-of-place writes relax the write-ahead rule for data pages. A copy may hold changes whose log
records are not durable yet. That is harmless: its remap is appended after it lands, behind every
record the copy holds, so a remap that survives a crash names a copy whose records survived too
(as the prototype's buffer manager already relies on).

Recovery replays remaps. Any slot named by an intent that got no remap before the crash is
erased and stays free, so a crash leaves no unreferenced copy holding data.

Slots belong to one class, so disk space is reused only by pages of the same size (Umbra §2.5).
Erasing a freed slot also returns its disk blocks to the file system.

## 6. The log

### 6.1 Records

```text
len: u32 | kind: u8 | flags: u8 | reserved: u16 | txn: u64 | payload | crc32c: u32 | padding to 8
```

- **Alignment and CRC.** Records are 8-byte aligned. The CRC covers the record's position, its
  header and its payload, so bytes left in a recycled segment never read as a valid record.
- **LSN.** A record's LSN is the position just past its end, so `durable >= lsn` means the record
  is durable.
- **Page records** carry the page id: New, Free, Remap, write intent, the B-tree operations
  (insert, update with the changed columns, delete-mark, remove, split, merge, new root), undo
  append, and blob write. Redo is physical per page. Undo is logical, by tree and key, so it still
  works after splits and merges.
- **Transaction records:** Begin (the slot table entry), Commit with its timestamp, End, and
  compensation (the redo of one undo step, carrying the next undo record to process).
- **Structure changes** (split, merge, new root) are system operations: redo-only, logged
  atomically as one record, and never undone by a user transaction's rollback.

### 6.2 Filling the log in parallel

- **Reserving.** The log buffer is a ring of aligned chunks. An appender reserves space with one
  atomic add on the insert position and copies its record in, in parallel with other appenders.
- **Knowing what is complete.** Each appender announces, in a small array, the position its
  in-progress copy starts at. It does so before reserving, with sequentially consistent ordering.
  A flusher that reads the insert position and then the array therefore sees every copy below
  that position that is still running. It writes only the prefix every appender has finished.
- **Write-ahead rule.** A change's record is appended before the page latch is released, and the
  page's LSN set to it.
- **Never blocking mid-change.** Space is never waited for in the middle of a change. Before an
  operation takes its first latch it waits, if it must, until the unflushed bytes are below the
  buffer's size. Appends inside the operation then take a new chunk if needed.

### 6.3 Flushing and group commit

- **Mode A** (Linux, with O_DIRECT working): writes carry `RWF_DSYNC`, so each is durable when it
  completes and no separate sync is needed. Several writes may be in flight. The durable LSN is the
  end of the contiguous prefix of completed writes.
- **Mode B** (anywhere else): write, then `fdatasync`, with the next write overlapping the sync in
  flight, as the prototype does.
- **Triggers.** A flush starts when a commit waits on it, when the buffer fills, or when the page
  cleaner needs durability. There are no timers.
- **Padding.** Every flush ends on a 4 KiB boundary and the gap is a padding record. The next
  flush starts in a fresh block, so durable bytes are never rewritten.
- **IO priority.** Log writes go out at the highest IO priority and page writeback at the lowest
  (io_uring's per-request priority). A commit's latency is then one log write and does not depend
  on what the page cleaner is doing.

### 6.4 Segments

- **Size.** The log is a sequence of segment files of S bytes, a creation parameter with a default
  of 256 MiB. At the 1 GB/s log rate Umbra measured (VLDB 2022 §4), a segment lasts about a
  quarter of a second; at ordinary rates it lasts minutes. LSN `l` lives in segment `l / S`.
  Records may span segments.
- **Preparation.** Segments are written full of zeros before use, so later writes into them change
  no file metadata. At least one spare is kept ready by background work. If the log ever catches up
  with the spares, new operations wait before their first latch, as in section 6.2.
- **Recycling.** A segment entirely below the redo point (and, with archiving, already archived) is
  zero-filled, which erases it, and renamed to become a spare.

### 6.5 Failure

Any failed log write or sync makes the log refuse further appends and fails every waiter. The
database then accepts no writes until it is restarted and recovers. After a failed `fsync`,
nothing about the data in flight can be trusted.

## 7. The buffer manager

### 7.1 What stays from the prototype

- Per-class virtual memory regions, each as large as the pool's budget.
- `MADV_DONTNEED` on eviction.
- Swizzled swips and LeanStore's cooling.
- Versioned latches.
- Asynchronous loads whose completion everyone who reaches the page shares.
- No blocking: a busy latch is a yield, and a full pool is a wait on the next eviction.

### 7.2 What changes

- **Resident table.** A partitioned hash table maps page id to frame for every resident page. A
  swizzled swip is the fast path for the page's single owner. Everything else finds the page by id
  here: undo pointers, page-table and free-space pages, and the page cleaner. Cooling becomes a
  mark on the frame (LeanStore 2023 §6) instead of a second hash table.
- **Compact frames.** One state-and-latch word, the page id, the LSN of the first unwritten change,
  the parent, the count of resident children, and one IO slot. This replaces the prototype's
  mutexes and vector per frame.
- **Page kinds find their own swips.** A copy for writeback has every swizzled child replaced by its
  id, so a page can be written while its children stay resident. A parent is still never evicted
  while it has resident children.
- **Fix modes.**
  - Optimistic: read the version and validate it afterwards, for B-tree descent.
  - Shared and exclusive, with upgrade.
  - Prefetch: start loads of upcoming children without waiting, for scans.
- **No guard is held across a VDBE yield.** A cursor keeps its frame, the version it saw, its
  position and its key. On resuming it validates the version; if the page moved, it seeks again by
  key.
- **Partitioned eviction.** Pages are split into partitions by page id, each with its own cooling
  queue and lock.
- **Page cleaner.** Writes back continuously (section 8).
- **IO priorities.** Reads a query waits on come first, then cleaner writes, then prefetch.

## 8. Checkpoints

Checkpointing is continuous (Haubenschild et al. §3.4).

1. **Shards.** Frames are split into S shards; S defaults to 128, which that paper found smoother
   than 10.
2. **Passes.** Each time 1/S of the log budget has been appended, the cleaner writes back every
   dirty page of the next shard, round robin. Before it starts, it records the log's end as that
   shard's checkpointed LSN.
3. **Redo point.** The minimum checkpointed LSN over all shards, and over the superblock's own
   unwritten root remaps.
4. **The pass is a checkpoint.** When the redo point has moved, a new superblock is written with it
   and the current root slots. Segments wholly below the redo point are recycled.
5. **Log budget.** The log budget is the bytes of log recovery may have to replay. It is
   configured, and by default scales with the pool (Haubenschild §3.7): 0.4 times the pool, the
   ratio of that paper's example (a 20 GB WAL for a 50 GB pool).
6. **Writing a page** latches it only long enough to copy it. A page being written may still change,
   and is clean afterwards only if it did not. Undo is in pages, so a long transaction does not
   hold the log back.

## 9. Transactions and versions

### 9.1 Slots and the clock

- **Slots.** There are W transaction slots, W configured as the most transactions that can be open
  at once. A connection takes a slot when its transaction begins and gives it back when it ends, so
  a slot runs one transaction at a time.
- **Slot table.** Each slot's state (its active transaction, start timestamp and undo head) lives
  in slot table pages, which are logged.
- **Clock.** One global atomic clock issues start timestamps and commit timestamps.
- **Commit order.** Each slot keeps an in-memory list of its recent (start, commit) pairs, in order
  (OSIC §3.1). The list is bounded: an entry is kept only while some snapshot still needs it.
- **Visibility.** A version written by (slot s, start t) is visible to a snapshot taken at time T
  when the last transaction slot s committed before T started at or after t. Each snapshot caches
  that answer per slot.

### 9.2 Version header

Every B-tree leaf entry, in tables and indexes alike, starts with a header:

```text
ts: u64 | undo: u64 | slot: u16 | flags: u8 | command: u32
```

- `ts` is the writer's start timestamp, and `undo` points at the undo record for the previous
  version.
- `flags` mark a deleted entry or a lock with no new data.
- `command` is the statement within the writer's transaction, which PostgreSQL semantics need.
- An entry visible to every snapshot that has no older version can be frozen: its header shrinks
  to one flags byte, so cold data pays almost nothing for versioning.

### 9.3 Writing

- **Insert:** a new entry with this transaction's header, and an undo record that removes it.
- **Update:** the changed columns' old values and the old header go into an undo record. The entry
  is changed in place, with a new header pointing at that record.
- **Delete:** the entry is marked deleted, again with an undo record. It stays until purge.
- **Conflicts.**
  - If the entry's newest version belongs to an open transaction in another slot, the writer waits
    for it to end. Under BEGIN CONCURRENT semantics it fails at once instead.
  - If the newest version was committed after this snapshot, snapshot isolation fails the
    transaction. Read committed reads that version and has the statement check it again
    (PostgreSQL's rule for UPDATE).
- **Row locks.** FOR UPDATE writes a lock-only header. FOR SHARE needs a shared lock table in
  memory and comes later.
- **Deadlocks.** Waits form a graph over slots; a cycle aborts its youngest transaction.

### 9.4 Undo pages

- **Ownership.** Each slot owns a chain of undo pages, appended to by its transaction.
- **Records.** An undo record holds the tree, the key, the old header, the old values, and the
  transaction's previous undo record.
- **References.** A version header names an undo record by page id and offset. Those pages are
  found through the resident table, never through swips: many entries refer to one undo page.
- **Durability.** Undo pages are logged like every page. The undo append's record precedes the
  change it undoes, so a durable change always has a durable undo, and a crash brings back
  everything needed to roll back.

### 9.5 Reading

- **Snapshots.** Snapshot isolation takes one snapshot per transaction; read committed takes one per
  statement.
- **Checking an entry.** A frozen header, or a writer the commit order shows committed before the
  snapshot, is visible with no further reads. Otherwise the reader follows the undo pointer and
  rebuilds the previous version in a private copy, and repeats.

### 9.6 Commit

1. Draw the commit timestamp and append the Commit record.
2. Enter the timestamp in the slot's commit order. The transaction is visible from here.
3. Wait until the log is durable through the Commit record, then acknowledge.

A transaction that saw this commit has its own commit record later in the one log, so it can never
be durable without it. A read-only transaction acknowledges only once the log is durable up to
where it stood when its snapshot was taken, so nothing it returned can be lost in a crash. That is
usually durable already.

### 9.7 Rollback

Rollback walks the transaction's undo chain backward. Each record is applied through the tree by
key, and each step logs a compensation record. Savepoints and statement rollback stop at a
remembered undo position.

### 9.8 Purge

Purge runs as background work.

- **What it removes.** Deleted entries that no snapshot can see, and older versions below the
  oldest active snapshot. It freezes headers and frees whole undo pages.
- **Long reads.** An OLAP read holds back only the versions it can see. A graveyard index for
  deleted entries (OSIC §3.2), which keeps scans of hot ranges fast meanwhile, comes later.

### 9.9 Isolation levels

- Read committed (PostgreSQL's default), and repeatable read as snapshot isolation.
- Serializable comes later: PostgreSQL-compatible SSI, or HyPer's precision locking (Freitag et al.
  §3.3.2).
- In the SQLite frontend, BEGIN and BEGIN CONCURRENT both mean snapshot isolation; every
  transaction may write concurrently.

### 9.10 Bulk loading

Loads and CREATE INDEX build pages directly. Their versions are tracked by epoch per page rather
than per row, after Umbra's bulk mode (§3.2). This comes later.

## 10. Recovery

1. Read the newest whole superblock.
2. **Analysis**, from the redo point:
   - collect remaps and write intents and the newest LSN per page;
   - find the log's end, which is the first record that fails its CRC at its own position.
3. **Redo**, in parallel by page id range:
   - each page starts from its newest copy whose remap is durable, or else the copy the checkpoint's
     page table names;
   - records newer than the page's LSN are applied.

   Page-table, free-space, slot table and undo pages are redone like any page.
4. **Erase** the slots of intents that got no remap.
5. **Undo:** roll back every transaction the slot table shows as open, from its undo pages, logging
   compensation records.
6. **Restart the clock** above every logged timestamp. Every surviving header is then a committed
   version.
7. **Checkpoint.**

Recovery reads at most the log budget. Haubenschild et al. replay 100 GB of log in 38 s on 40
threads (§4).

## 11. B+-trees

- **Node size.** Each tree has a node class, 16 KiB by default.
  - A leaf whose record does not fit gets the smallest class that holds it (Umbra §2.4).
  - Values larger than a quarter of a node go out of line into blob pages.
  - Values larger than the largest class go into a tree of blob pages.
- **Slotted nodes** (LeanStore 2023 §4):
  - fence keys, fixed when the node is split or merged;
  - the fences' common prefix stripped from every key;
  - a 4-byte head of each key stored as an integer in its slot;
  - a hint array.
- **Keys** are byte strings compared with `memcmp`.
  - **Rowid tables:** 8 bytes, big-endian with the sign bit flipped.
  - **Indexes and WITHOUT ROWID tables:** the index columns encoded so that byte order is
    SQLite's order:
    - exact for BINARY, NOCASE and RTRIM, for descending columns, and for SQLite's mixed integer
      and real comparison;
    - a tree with a user-defined collation compares with Turso's record comparator instead.

    Index entries keep the original record beside the encoded key, since encoding loses case and
    the integer/real distinction.
  - **Proof:** a property test compares the encoding's order with `compare_immutable` on random
    records.
- **Latching:** optimistic lock coupling. A writer latches only its leaf. A split restarts from the
  root and latches the nodes it changes (LeanStore 2018 §IV-F).
- **No sibling links.** A scan reaches the next leaf through its upper fence, or by keeping the
  parent's version (Umbra §2.4).
- **Structure changes** are system operations (section 6.1).
- **Later:** contention splits and merging of sparse neighbours (LeanStore 2023 §4).

## 12. Turso's SQL on it

The SQL layer touches storage in these places:

- `CursorTrait` (`core/storage/btree.rs`);
- about 25 pager operations, called from the VDBE and the connection;
- the database header fields;
- the meaning of `sqlite_schema.rootpage`.

MVCC plugs into the same places with 38 explicit branches in `core/vdbe/execute.rs`.

- **Mode.** The database gets a third mode beside WAL and MVCC. The `Database` holds an
  `UmbraStore`, and a connection's transactions and savepoints go to it.
- **Cursor.** `UmbraCursor` implements `CursorTrait` over table and index trees. It takes and
  returns SQLite record bytes, as every cursor does.
- **Tree ids.** `sqlite_schema.rootpage` holds tree ids. Tree 1 is `sqlite_schema`, because Turso
  assumes page 1 in about 60 places. A catalog tree maps each tree id to its root, node class and
  key layout.
- **DDL.**
  - CreateBtree allocates a tree id.
  - Destroy and Clear drop or empty a tree; purge frees its pages once no snapshot can see them.
- **Header fields.** Schema cookie, user version, application id and the rest are rows in a system
  tree, versioned like data.
- **NewRowid.** A per-tree counter, seeded from the tree's largest key, safe for concurrent
  inserters.
- **Routing pager calls.** Where the VDBE and the connection call the pager (transactions, header,
  CreateBtree/Destroy/Clear, savepoints, checkpoint, integrity check, page counts), calls are routed
  through a storage-mode interface rather than a third set of `if` branches beside MVCC's.
- **What changes meaning.**
  - `PRAGMA page_size` reports the node size.
  - `page_count` and `freelist_count` come from the page store.
  - The `dbpage` table is not supported.
  - In-place VACUUM compacts trees; space is reused anyway.
  - `integrity_check` checks the trees, the page table and the free-space maps.
- **Index methods** (ivf_rq, fts and the rest) store their data in ordinary rowid and index trees
  through cursors, so they run unchanged. They get the snapshot timestamp as their snapshot
  identity.
- **PostgreSQL frontend.** It opens MVCC today and turns BEGIN into BEGIN CONCURRENT. It gains a
  switch to open umbra databases.

## 13. Concurrency inside the engine

Turso runs a connection's statements on whatever thread steps them, and IO completes either inside
the submission or later from `IO::step`. Therefore:

- **Callbacks** take no lock a submitter can hold, and nothing holds a lock across a submission.
- **No waiting on a lock.** Where waiting is unavoidable, the caller gets a completion (`IOResult`)
  and steps again.
- **Engine state** is shared through atomics and short critical sections.
- **Latches** are versioned latches. A cooperative yield replaces blocking, because the holder may
  be another connection on the same thread.

## 14. Object storage

Later, but the format leaves room for it.

- **Page copies.** Page copies are immutable, so the page cleaner can pack them into large objects.
  A page-table entry can then name a local slot or a place in a remote object, and local data files
  become a cache: clean local copies may be dropped.
- **Log.** Closed log segments are uploaded before they are recycled. Durability can then be local
  `fsync`, or acknowledgement by a remote log service.
- **Uses.** A checkpoint's superblock and page-table roots, plus the log after it, give
  point-in-time recovery and read replicas.
- **Garbage collection.** Remote objects are collected by counting, per checkpoint, the copies that
  refer to them.

## 15. Proving it

- **Fault-injecting IO.** A deterministic implementation of Turso's `IO` reorders completions,
  tears writes at 512-byte and 4 KiB boundaries, drops writes not yet synced at a crash, and
  injects EIO and ENOSPC.
- **Crash points.** Small workloads crash at every IO point; larger ones at random IO points.
- **Model.** After every recovery the database is compared with a model of committed state.
  Committed transactions are all there, uncommitted ones are not, and every snapshot read matches
  the model.
- **Isolation.** Turso's `concurrent-simulator` runs Elle over SQL histories.
- **Property tests:** key encoding order against `compare_immutable`; log and page decoders
  against corrupted input, which must report corruption and never panic.
- **TLA+** (Turso has a `tlaplus` directory): the commit and visibility protocol, slot reuse, and
  recovery.
- **Benchmarks** against PostgreSQL on the same machine: TPC-C, TATP and YCSB, and commit latency
  percentiles while the page cleaner runs.

## 16. Order of work

Each step replaces the prototype's piece, builds and is clippy-clean, and lands with its tests.

1. **IO:** direct IO (`OpenFlags::Direct`), `fdatasync`, writes durable on completion
   (`RWF_DSYNC`), preallocation, directory sync, and the fault-injecting IO above
   (`core/io/crash.rs`). Done.
2. **Log:** records, parallel fill, flushing modes, segments, failure.
3. **Page store:**
   - page kinds;
   - the page table and the free-space maps as logged pages;
   - write intents and erasure;
   - the control file.
4. **Buffer manager:**
   - the resident table and compact frames;
   - partitioned eviction;
   - page kinds' swips;
   - optimistic fixes and prefetch;
   - the page cleaner;
   - IO priorities, kept only if measurements show the device honours them.
5. **Checkpoints and redo** over the new pieces.
6. **B+-trees.**
7. **Transactions:** slots, clock, version headers, undo pages, conflicts and waits, rollback,
   undo in recovery, purge.
8. **Turso integration:** the storage-mode interface, `UmbraCursor`, the catalog, header rows, and
   the PostgreSQL frontend switch.
9. **Object storage.**

## References

- Neumann, Freitag. Umbra: A Disk-Based System with In-Memory Performance. CIDR 2020.
- Freitag, Kemper, Neumann. Memory-Optimized Multi-Version Concurrency Control for Disk-Based
  Database Systems. PVLDB 15(11), 2022.
- Haubenschild, Sauer, Neumann, Leis. Rethinking Logging, Checkpoints, and Recovery for
  High-Performance Storage Engines. SIGMOD 2020.
- Alhomssi, Leis. Scalable and Robust Snapshot Isolation for High-Performance Storage Engines.
  PVLDB 16(6), 2023.
- Leis, Alhomssi, Ziegler, Loeck, Dietrich. Virtual-Memory Assisted Buffer Management. SIGMOD 2023.
- Leis, Haubenschild, Kemper, Neumann. LeanStore: In-Memory Data Management Beyond Main Memory.
  ICDE 2018.
- Alhomssi, Haubenschild, Leis. The Evolution of LeanStore. BTW 2023.
- Mohan et al. ARIES. TODS 1992.
