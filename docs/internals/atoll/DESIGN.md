# Atoll

Atoll is a storage mode for Turso beside the SQLite pager and MVCC. LeanStore is its baseline, as
described in Leis, "LeanStore: A High-Performance Storage Engine for NVMe SSDs" (PVLDB 17, 2024).
Its buffer manager, its page writes and its log depart from LeanStore where the requirements below
force them to; section 2 lists each departure and why.

The code in `core/storage/atoll` is a prototype of parts of this design:

- size classes and the page header;
- the versioned latch;
- an out-of-place page store whose page table is held whole in memory;
- a buffer manager with swizzling;
- a redo-only log;
- checkpoints of that page table.

Section 20 says in what order each piece is replaced.

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
{db}-data/c{k}.{n} data file n of size class k, each at most a fixed size
{db}-temp/c{k}     temporary pages of size class k, deleted at every open
{db}-log/{n}       log segments by number, and prepared spares
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

The page table maps each page number to:

- **free** — the number is unused;
- **allocated** — no copy yet;
- **a local slot**, a remote place (an object and an offset), or both.

An entry is 16 bytes, 0.4% of a 4 KiB page. Because a free entry is a free number, the page table
also allocates page numbers, lowest first, which keeps it dense after churn.

The table is a radix tree of pages per class: a root, directory pages, and map pages. A directory
entry records a map page's slot and how many free entries it has, so a free number is found without
reading map pages that have none. Page-table pages are found through their parent's entry, and the
roots through the superblock. The table is consulted only on a buffer miss, and its map pages are
almost always in memory.

### 6.2 Free space

Per class, a radix tree of bitmap pages (one bit per slot), under directory pages that record each
bitmap's free count. Allocation takes free slots near a cursor, and adjacent slots for a batch, so
the batch goes out as one write. A full class grows geometrically, by an eighth of its size and at
least one batch, preallocated with `fallocate`.

### 6.3 The life of a slot

1. **Intent.** A writeback batch takes free slots and logs a write intent naming them. The intent is
   durable before any of those writes starts.
2. **Write.** Each copy is durable when its write completes: a write with `RWF_DSYNC`, or the batch
   followed by one `fdatasync`.
3. **Remap.** `Remap { pid, new, old }` is logged, the map entry points at `new`, and its bit is set.
4. **Free.** Once the log is durable past that remap, `old` is erased (a hole punched, or zeros
   written) and its bit cleared.

A copy may hold changes whose log records are not durable yet. Its remap is appended after it lands,
behind every record it holds, so a remap that survives a crash names a copy whose records survived
too. If a crash leaves an intent with no remap, recovery erases its slots.

### 6.4 Temporary pages

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

- **Alignment and CRC.** Records are 8-byte aligned. The CRC covers the record's position, its
  header and its payload, so bytes left in a recycled segment never read as a valid record.
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
- **Padding.** Every flush ends on a 4 KiB boundary, and the gap is a padding record. Durable bytes
  are never rewritten, so a torn write cannot take committed records with it.

### 7.4 Segments

- **Layout.** The log is a sequence of segment files of S bytes. S is a creation parameter,
  256 MiB by default: at the 1 GB/s log rate Umbra measured (VLDB 2022 §4), that is a quarter of a
  second. LSN `l` lives in segment `l / S`, and records may span segments.
- **Preparation.** Segments are written full of zeros before use, so later writes change no file
  metadata. Background work keeps a spare ready.
- **Recycling.** A segment may be zero-filled and reused once:
  - it is wholly below the redo point;
  - and below the first record of the oldest transaction still writing, whose undo may need it;
  - and, with object storage, it has been uploaded.
- **Readers.** Recovery, replicas, app servers and the archiver read segments by position. A segment
  no longer local is fetched from object storage.

### 7.5 Failure

Any failed log write or sync makes the log refuse further appends and fails every waiter. The
database accepts no writes until it restarts and recovers.

## 8. Commit latency

A commit appends its record and waits for durability: one device write in the local modes, one
round to a majority under Raft. Page writeback goes to other files and never waits on the log path.

## 9. The buffer manager

### 9.1 Frames

- **Memory.** Each size class has an anonymous memory region carved into frames. The regions share
  one budget and are backed by 2 MiB huge pages where the OS allows. This is possible because a
  frame's memory is never unmapped on eviction: a freed frame is reused as it is. Memory moves
  between classes in 2 MiB batches.
- **Headers.** Frame headers live in a separate array that is never unmapped:
  - a state word (the versioned latch, plus states for free, loading and resident; its version rises
    on every exclusive release and every eviction);
  - the page id;
  - the LSN of the first unwritten change;
  - WATT's access timestamps;
  - one IO slot.

### 9.2 Finding a page

1. **The table.** One concurrent hash table maps page id to frame, for resident pages only: about
   16 bytes per frame. It is the only translation.
2. **Hints.** A reference to a page (a B-tree child entry, a version pointer) carries a frame hint
   beside the page id. A hint is never trusted. Following it reads the frame's state word and checks
   that the frame holds that page id. If it does, the access is a hit with no hash lookup, touching
   the same cache line swizzling would.
3. **Stale hints.** A stale or missing hint costs one hash lookup, and the hint is then refreshed
   with an atomic store. Hints are never cleaned up, so eviction knows nothing of who refers to a
   page, and any number of references may point at one page.

### 9.3 Fixing a page

- **Optimistic:** read the version, read the page, validate the version. Used for B-tree descent.
- **Shared** and **exclusive**, with upgrade.
- **No waiting on a latch.** A busy latch is a cooperative yield; a full pool is a wait on the next
  eviction.
- **No guard is held across a VDBE yield.** A cursor keeps the frame, the version it saw, its
  position and its key. On resuming it validates the version, and if the page changed it seeks again
  by key.

### 9.4 Replacement

WATT (LeanStore 2024 §2.2):

- each frame records its last few read and write timestamps;
- eviction samples candidates and evicts those of lowest value;
- a dirty page's value is weighted by the cost of writing it.

Eviction removes the page from the hash table and bumps the frame's version, so every hint to the
frame goes stale.

### 9.5 Page sources

A miss asks a page source for page P no older than log position X. The answer is:

- the bytes;
- the page's LSN;
- the source's own log position;
- for remote readers, the versions a reader at X needs (section 15).

The sources are:

- **the page store**, on a database node;
- **a database node**, for app servers and replicas. It answers with the bytes, or with where the
  current copy is in object storage;
- **object storage**, for immutable copies.

Misses are batched, and scans prefetch upcoming leaves, because a network fetch costs about
100 µs–1 ms against about 50 µs for NVMe.

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
3. **Page store.**
   - page kinds;
   - the page table and free space as logged pages;
   - write intents and erasure;
   - temporary pages;
   - the control file.
4. **Buffer manager.**
   - frames on huge pages;
   - the hash table and frame hints;
   - WATT replacement;
   - fixes and prefetch;
   - the page source for the local store;
   - the page cleaner.
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
