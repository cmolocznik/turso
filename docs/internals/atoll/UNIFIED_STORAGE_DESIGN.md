# Atoll: analytical execution, streams, and observability

This proposal defines the direction and required contracts for extending Atoll into the
transactional storage layer for OLTP, analytical queries, durable event streams, and
OpenTelemetry data. It chooses a narrow first prototype and an order of work; it does not
describe implemented support or commit to a public API, SQL syntax, or file format.

[DESIGN.md](DESIGN.md) describes the storage foundation and its implementation status.
[CONCURRENCY.md](CONCURRENCY.md) defines the concurrency rules and takes precedence wherever
it replaces that design. The extensions proposed here must satisfy those rules. In particular,
sharing storage with another execution engine must preserve cooperative IO, bounded memory,
validated reads, and safe reuse of identities.

This roadmap does not expand the current Atoll foundation milestone or change its acceptance
gates. The next design deliverable is the engine-neutral execution read context and batch-scan
contract in sections 4 and 8, with the acceptance tests in section 12. Additional persistent
formats require their own design and compatibility review before implementation.

## 1. What we want

The intended foundation is Atoll's transactions, snapshots, shared catalog, and durable storage.
Several execution and ingestion interfaces would use those facilities:

- Turso SQL serves OLTP through the existing SQLite and PostgreSQL frontends.
- DataFusion and potentially DuckDB serve analytical SQL over the same logical data.
- A stream interface appends and reads ordered records and stores consumer progress.
- An OTLP interface ingests logs, traces, metrics, and, later, profiles.

The useful property is a common transaction boundary. Within one Atoll database, an application
could consume an event, update a table, publish another event, and record its consumer position
in one transaction. An analytical query could join application tables and events at one snapshot.
Observability queries could correlate those records with logs, spans, and metric exemplars.

This does not require every workload to use one physical layout. It requires the layouts to
share the same transaction, visibility, catalog, durability, and reclamation rules.

## 2. Proposed architecture

```text
Turso SQL             DataFusion / DuckDB       Stream API         OTLP ingestion
    |                         |                     |                    |
    +-------------------------+---------------------+--------------------+
                              |
           shared transactions, snapshots, catalog, and access methods
                              |
          +-------------------+-------------------+
          |                   |                   |
     row B+-trees      column segments       append segments
          |               (Vortex)                |
          +-------------------+-------------------+
                              |
              Atoll page store, buffer pool, log, and recovery
                              |
                  local disk / object storage / replication
```

The lower storage tiers and replication retain the status and limits described in DESIGN.md.
Drawing them here does not make the planned remote or Raft support available.

Engine adapters and protocol implementations should live outside `core/storage/atoll`. Atoll
exposes generic access and transaction operations; it should not need DataFusion plans, DuckDB
objects, OTLP messages, Kafka protocol state, or PostgreSQL-specific behavior. Analytical and
observability dependencies should be optional for an application embedding Turso for OLTP.

## 3. Existing foundations and proposed additions

Atoll already has page size classes from 4 KiB to 8 MiB, a generic page-kind identifier,
out-of-place page writes, logged blob writes, and provisional-page adoption. Its B+-tree nodes
use classes up to 64 KiB because their slots have 16-bit offsets. Larger column and append
segments can be separate page owners rather than new B+-tree node formats.

The transaction implementation and Turso integration are still being developed. Their current
status belongs in DESIGN.md and CONCURRENCY.md. This proposal depends on completing and proving
the remaining transaction, recovery, and integration work recorded there.

The additions would include:

- Catalog entries describing logical relations, physical layouts, and their versions.
- Snapshot-aware parallel batch scans alongside row cursors.
- Immutable column segments and their publication and compaction protocols.
- Ordered append storage, consumer progress, and retention.
- Engine adapters, telemetry ingestion, and signal-specific query operations.
- Shared resource accounting and workload limits.

Umbra is relevant to the physical layout choice. Its original design uses PAX leaf pages, with
columns grouped within each page. The paper notes that disk reads still fetch every attribute
and proposes other layouts for cold data. Vortex is a candidate for that role in Atoll, rather
than a reason to change the layout of every mutable row page. See [Umbra, section 2.4][umbra].

## 4. The common transaction and catalog contract

Sharing a page store alone is insufficient. Every interface needs the following guarantees:

1. **One snapshot per statement or transaction, according to its isolation level.** Every table
   and every parallel task in a query uses that snapshot. A worker must not independently take
   a newer snapshot when it begins scanning.
2. **Read your own writes.** A relational query inside a transaction sees that transaction's
   earlier statements' writes, not its current statement's writes. Sequential stream consumption
   additionally restricts its input to a stable prefix, as defined in section 9.1.
3. **Schema and data agree.** Planning, execution, and prepared-plan invalidation account for
   catalog versions. Compaction cannot silently change the meaning of a relation reference.
4. **Common writes.** Engine adapters submit writes through the transaction and relation layer
   that maintains constraints, indexes, and other required SQL effects. They do not bypass it
   by editing pages directly.
5. **Common commit and durability.** An adapter cannot acknowledge a write before Atoll's chosen
   durability requirement is met. Read completion must also preserve Atoll's durability rules.
6. **Common reclamation.** Dropping a table, replacing segments, or expiring stream data accounts
   for active snapshots, in-flight work, and any retained remote references.

These guarantees initially apply within one Atoll database and transaction domain. Atomic
transactions across independent databases, external systems, or future distributed partitions
need an additional protocol. A shared engine does not supply that protocol automatically.

The catalog should let an analytical query scan both row tables and columnar relations. For
example, a customer lookup table can remain in a B+-tree while an events relation uses column
segments. A separate analytical projection of a mutable table is also possible, but its storage
cost and freshness must be explicit.

### 4.1 An execution read context

Create one retained, immutable read context for each execution, before its storage tasks begin.
Every scan and parallel task in that execution uses it. The context identifies:

- The statement snapshot, according to the transaction's isolation level.
- The transaction identity and statement cutoff for reading its own earlier writes.
- A compatible catalog view and the logical relation identities used by the query.
- A WAL durability dependency covering the committed changes the execution can observe.

A timestamp alone is not this context. Workers may keep private visibility caches, but they
must not acquire newer snapshots or read a changing parent transaction's statement number.
The first adapter permits only one active statement on the parent transaction: it cannot
advance to another statement, commit, or roll back until this execution's storage tasks and
cleanup have finished. An adapter must not report successful read completion before the
context's durability dependency is met.

Prepared plans retain logical relation identities and schema dependencies, not an execution's
snapshot or unprotected page references. Each execution obtains its read context anew. Under
read committed, repeated executions take fresh snapshots; under snapshot isolation, they use
the transaction's retained snapshot while applying the current statement cutoff. Validate
catalog dependencies before producing output. The first adapter rejects an incompatible
schema change and requires replanning; it does not silently use a replacement table with the
same name.

An executing query retains the catalog and relation versions it needs until its storage tasks
finish. Its read context can retain MVCC history for the query's lifetime. That lifetime is
separate from an epoch-protected Atoll step: no step guard or frame reference stays active
across a yield merely because the query remains open. Section 8 defines partition coverage,
result ownership, and when execution cleanup is complete.

## 5. Integrating analytical execution

### 5.1 Start with DataFusion

DataFusion's custom `TableProvider`, `ExecutionPlan`, and batch-stream interfaces let a source
describe projections, filters, limits, statistics, and parallel scan partitions. Planning should
describe the scan; execution produces the data. See [custom table providers][datafusion].

The adapter binds the execution read context from section 4.1 to every scan. The first adapter
is read-only and returns owned Arrow batches from ordinary row tables. Atoll's internal
interface expresses the required operations without depending on DataFusion types. Column
segments and scans combining physical layouts come later.

The initial scalar boundary is nullable signed 64-bit integers, 64-bit floating-point values,
UTF-8 text with binary collation, and byte strings. Each requested column has one declared
batch type; actual SQLite storage classes must match that type without coercion. Column
affinity or values in the first batch do not prove that later values match. Unsupported values,
heterogeneous storage classes, and unsupported collations produce an explicit query error,
even if discovered in a later batch. Partial output from a failed query is not a successful
result. A future type-preserving representation can extend this boundary.

Initially DataFusion evaluates all predicates and the global limit. The storage scan selects
the visible row version before producing the projected values; it does not stop after a limit
on physical entries, invisible versions, or rows that have not passed residual predicates.
Partition-local limits do not establish a global result limit. Do not advertise exact counts,
ordering, or statistics until they are proved for the selected snapshot and partition scheme.

Later filter support must distinguish exact evaluation from conservative pruning. Min/max
statistics can discard a segment only when doing so cannot remove a matching visible row.
Unsupported predicates remain in the execution engine. Ordering and partition statistics
advertised to the optimizer must remain true after visibility filtering and combining layouts.

DataFusion is the recommended first integration because its Rust implementation and composable
interfaces offer control over execution and resource use. This is an engineering preference,
not a measured performance result.

### 5.2 DuckDB as another adapter

DuckDB's storage-extension interface includes catalog attachment and transaction-manager
creation. An Atoll storage extension could expose relations, scans, and eventually writes while
delegating their storage transactions to Atoll. See the [storage-extension interface][duckdb-storage].

A scan function is enough for a read-only experiment. A database attachment needs the deeper
catalog and transaction integration. In either case, integration must account for the C++/Rust
boundary, cancellation, buffer ownership, and supported DuckDB versions.

The first milestone does not require both analytical engines. Build a shared storage contract
and prove one adapter before maintaining a second.

### 5.3 SQL behavior and routing

Consistent storage visibility and identical SQL behavior are separate requirements. Engines
can differ in casts, integer and floating-point operations, collations, functions, null handling,
and accepted syntax. DuckDB explicitly has its own [SQL dialect][duckdb-dialect]. Selecting a
parser dialect does not by itself make execution semantics match Turso.

Begin with explicit analytical execution. Automatic routing can follow for queries whose
behavior we can preserve and test. One candidate is to lower supported Turso logical operations
into DataFusion plans while retaining Turso's name resolution and dialect rules. Unsupported
operations can remain with Turso or require explicit analytical execution, according to the
chosen interface. Routing must never silently change query results.

Users should be able to inspect the selected engine, physical layouts, and pruning decisions
through explain output. The exact interface and routing cost model remain open.

## 6. Vortex inside Atoll storage

Vortex separates encoded arrays, serialized layouts, and the source supplying data segments.
A complete `.vortex` file places these pieces in one file, but layouts can use other block
storage. See [layouts][vortex-layouts] and [serialization][vortex-serialization].

| Integration | Use |
| --- | --- |
| A complete Vortex file in each Atoll page | A possible experiment; repeated file metadata and whole-page reads may be costly. |
| Vortex layouts and encoded segments backed by Atoll pages | Recommended direction for native columnar storage. |
| External Vortex files referenced by Atoll | Useful for import, export, and external datasets; requires an explicit ownership and retention model. |

The proposed native representation has a descriptor for a group of rows. It records the logical
row identities, schema and format versions, visibility information, pruning statistics, and
references to column chunks. A segment source resolves Vortex segment references to Atoll
pages or page ranges. The precise encoding of these references is not decided here.

Vortex already has file integrations for [DataFusion][vortex-datafusion] and
[DuckDB][vortex-duckdb]. They are useful components and examples. Adapting them to Atoll still
requires snapshot-aware segment access and any mutable-data merge.

### 6.1 Separate row, compression, and IO granularity

Keep these choices independently tunable:

- The row range sharing visibility and pruning metadata.
- The amount of column data compressed together.
- The bytes fetched and cached in one Atoll page.

Putting every column into one large page improves locality but forces a narrow projection to
read unrelated columns on a cache miss. Prefer independently readable column chunks. Small
segments can share pages when the saved space justifies the extra reads; a large segment can
span pages. A scan must be able to fetch its required segments without reconstructing an
entire table or opaque multi-page value.

Measure candidate page classes, including 64 KiB, 256 KiB, and 1 MiB. Account for the Atoll
header, Vortex metadata, buffer alignment, and unused space from size-class rounding. These
are benchmark candidates, not chosen defaults. Object storage may justify different packing
and prefetch choices from local NVMe.

### 6.2 Immutable data and durable publication

The first columnar implementation should build immutable segments. Updates and deletes go to
a mutable structure; compaction writes replacements.

Initially use logged encoded bytes and Atoll's provisional-page adoption mechanism. Build
pages while they are unreachable, then publish the descriptor and adopt its pages in a
recoverable change. A crash must leave either a valid published representation or reclaimable
unpublished pages. Recovery must not need to invoke a compressor to reproduce historical bytes.

A publication protocol must also define concurrent readers and writers, not only crash recovery.
Readers retain a logically valid representation while replacements are installed. Old pages
become reusable only after snapshot and memory-lifetime rules allow it.

The Atoll wrapper needs its own versioning and corruption checks. Select a frozen Vortex
wire-format edition and supported component set explicitly. Upgrading a dependency must retain
read support for existing databases, including any custom encodings we introduce. Rolling
upgrades must constrain writers to formats supported by their intended readers. See
[Vortex compatibility][vortex-versioning].

## 7. Mutable tables and analytical projections

There are two distinct starting points:

- An append-oriented analytical relation whose completed batches become immutable segments.
- An analytical representation of a table that OLTP transactions continue to modify.

The first is simpler and should precede automatic conversion of arbitrary row tables. For the
second, a candidate design is an immutable base plus visible updates and deletes in a mutable
structure. A scan selects one row version at its snapshot and suppresses replaced base rows
before applying predicates to their values.

For each query snapshot, every required logical input must contribute exactly once through
the selected representation or the remaining input path. Coverage identifies committed inputs,
not simply writer start timestamps or event times. It may require per-partition ranges and
exceptions rather than one scalar watermark. Representation publication, visible updates,
indexes, and the query's retained descriptor versions must agree on that coverage. The same
invariant applies to derived telemetry in section 10.3; it does not require every access method
to use the same physical coverage algorithm.

For example, a base row has `status = 'error'`, but a visible update changes it to `'ok'`.
A query filtering for errors must not return the old base row just because the update itself
does not match the predicate. Pruning and indexes must account for changes outside the base.

The conversion protocol must specify:

- Stable logical row identities and how indexes resolve them across compaction.
- Which committed changes a base includes and which remain in the mutable structure.
- Transactions still open when a base is built, including those committing after its boundary.
- Atomic publication of the representation and its coverage metadata.
- How older snapshots find an appropriate representation and needed versions.
- When old descriptors, row versions, and segments can be reclaimed.

A timestamp comparison against the writer's start time is not enough to establish coverage.
The protocol must follow Atoll's actual commit visibility rules. Exact coverage and catch-up
algorithms remain open and need deterministic concurrency and crash tests.

SQLite columns can contain different storage classes. A columnar representation must preserve
them, along with supported collations and comparisons. Typed subarrays with storage-class
information are one candidate. Explicitly typed analytical relations are a narrower first
implementation. No conversion may silently coerce an existing table's values.

## 8. Batch execution, IO, and memory ownership

Analytical scans produce projected batches, visibility masks where needed, and logical row
identities. Compression can reduce IO even with row output, but reconstructing a SQLite record
for every row before filtering loses much of the possible CPU benefit. Start with batch scans
and add supported filter and aggregation operations as their semantics are proved.

### 8.1 Partition coverage and owned results

For one execution read context, all scan partitions together return each selected logical row
exactly once, using its visible version. The first row-table implementation partitions by
disjoint logical key ranges. It does not assign ownership by leaf-page identity: a split,
merge, or page replacement cannot move a row into a second partition or out of every partition.
When segment scans are added, all workers use a compatible retained descriptor and coverage
view. One worker must not scan an old segment while another independently discovers its
replacement and returns the same rows again.

Atoll's shared frame bytes are accessed atomically and validated, and guards do not survive a
yield. A Vortex or Arrow array cannot retain a borrowed slice of a frame that can be reused.
Initially copy validated data into owned, correctly aligned buffers. Include the dictionaries
and metadata needed to interpret a returned batch in its owned state. Any later direct
buffer-sharing scheme needs an explicit ownership and reclamation proof.

Returned batches remain valid independently of the scan, including after cancellation. Their
memory remains charged until the final owner releases it, not merely until the scan returns.
Account for these buffers, decompression, operator state, and spill space in addition to the
buffer pool. A bounded page cache with unbounded decoded batches is not a bounded engine.

### 8.2 Cooperative IO and execution cleanup

All underlying IO continues through Turso's IO and `IOResult` stepping. An adapter translates
completions into its execution runtime's polling model without blocking a thread needed to
complete that IO or another cooperative operation. Completion racing with waiter registration
must not lose a wakeup. The embedder and adapter must drive IO even on a single-thread executor;
Atoll itself starts no engine threads.

Each concurrently executing Atoll step enters and leaves epoch protection correctly. A task
must not clear another active task's announcement. Retained read contexts may be shared, but
thread-local step guards cannot move between threads or survive a yield or `Pending`. On
resumption, operation state contains only owned data or names that are revalidated before use.

Every execution termination path uses the same owned cleanup protocol: normal completion,
query errors, explicit cancellation, downstream early stops such as a satisfied limit, and
result-stream drop. Stop unnecessary work and ask remaining tasks to stop, but submitted IO
can still finish. A limit need not exhaust unvisited rows; it must still settle work already
submitted.

The embedder or adapter retains an execution owner that drives completions and cleanup even
when the result consumer stops polling or drops its stream. Dropping a stream must not block
the thread needed for that cleanup. Continue until no task or late callback can access released
operation, transaction, descriptor, or IO-buffer state. Only then is storage cleanup complete
and the execution's retained read context released. The parent transaction can then take its
next action according to its success or error state. Already-returned owned batches remain
valid and accounted for. Section 11 requires capacity for this cleanup even when a query
reaches its resource limits.

## 9. Transactional event streams

An append access method stores partitioned records with ordered offsets, batched writes, lookup
by offset or time, and sequential reads. Packed append segments should avoid one indexed row
and one IO per message. Sealed segments may use Vortex where analytical access and compression
justify it; the first append representation need not be columnar.

Stream metadata includes partition identity, offset allocation and readable progress, retained
ranges, consumer positions, and producer deduplication state where supported. Those structures
use ordinary Atoll transactions. A stream partition is a logical ordering unit; it does not
imply a separate WAL or consensus group.

Stream and partition identities are not reused when a name is deleted and recreated. Consumer
positions, ownership generations, and producer retry state identify that particular history,
not just a name and numeric offset. Expired producer deduplication state must not silently turn
an old sequence into a new append under the same producer identity.

### 9.1 Ordered visibility

Offset allocation and transaction commit can occur in different orders. If transaction A
reserves offset 100 and stays open while transaction B commits offset 101, a consumer that
advances past 101 must not later miss A's committed record at 100.

The first stream prototype uses reserved offsets and a stable, exclusive readable frontier F:
all reservations below F are resolved, and the required durability has been reached. Aborted
reservations can leave gaps. Assigning offsets through commit ordering is a possible later
alternative, not part of this prototype.

There are two distinct read contracts:

- **Relational scans** apply ordinary Atoll snapshot and statement visibility. If B commits
  offset 101 and an application-row update together, a SQL snapshot that sees B sees both,
  even while the sequential consumption frontier is blocked by A at 100. Relational scans
  also apply the earlier-statement read-your-writes rules from section 4.
- **Sequential consumption** reads the retained stable prefix. It obtains the version of F
  visible at its read context's snapshot, not the latest frontier from another snapshot, and
  reads records below that bound. A frontier publication must be visible only to snapshots
  that can see every committed record it covers. Consumption deliberately excludes records
  above the frontier, including the consumer transaction's own unresolved appends.

In the example, an old snapshot that can see 101 but cannot see A's later commit at 100 must
not acquire a newer F, skip snapshot-invisible 100, and advance past 101. Keep frontier state
versioned with the records' commit dependencies. After processing the required records and
accounting for valid gaps, the consumer's next offset must be at most the F read at that
snapshot. Advancing it also conditionally validates the expected position and ownership,
as described in section 9.2.

An unresolved reservation can delay consumption without hiding committed rows from SQL.
Bound unresolved reservations and their retained resources through section 11's admission
policy. The durable reservation, abort-resolution, and frontier-publication protocol still
needs specification and crash tests before stream implementation. Kafka's read-committed
consumers similarly have a last stable offset bounded by open transactions. See
[Kafka consumer semantics][kafka-consumer].

A consumer reads successive bounded transactions rather than retaining one snapshot
indefinitely. Consumer progress, MVCC visibility, durability, and the consumption frontier are
different pieces of state.

### 9.2 Atomic processing and delivery

Within one transaction domain, a processor could atomically:

1. Verify the expected consumer position and ownership generation.
2. Apply changes to application tables.
3. Append output events.
4. Advance the consumer position.

Retries after a crash then observe whether that transaction committed. The first stream
implementation keeps consumer position and ownership generation in one conditionally updated
record, scoped to the stream and partition identities. Validation is a conditional transaction
write, not just a snapshot read: a consumer whose ownership has changed must conflict or fail,
even if it read the old owner at its snapshot. Idempotent producers need durable producer
identities, sequence handling, and an explicit policy for expired retry state.

This can provide exactly-once effects inside the Atoll transaction domain under the defined
retry and ownership protocol. External side effects still need an outbox, idempotency, or a
separate coordination protocol.

Kafka wire compatibility, consumer-group coordination, partition leadership, replication, and
distributed transactions are separate features. They are not supplied by adding an append
method. Initially target Kafka-like storage and transactional processing within one database.

### 9.3 Retention and compaction

Define retention independently of consumer positions. A consumer whose position has expired
must receive an explicit result rather than silently continuing from a different history.
Key-based compaction, if supported, preserves offset ordering while allowing missing records.

Expiry should remove whole eligible segments and their indexes where possible. It must account
for active reads and retained representations. Measure append throughput and write amplification
against sequential-log storage: WAL bytes, page writes, indexes, compaction, and replication all
contribute to the cost.

## 10. OpenTelemetry ingestion and representation

An OTLP endpoint accepts batched telemetry from SDKs or Collectors. It validates requests,
preserves signal semantics, applies admission limits, and commits accepted data into Atoll.
Acknowledgement follows the configured durability requirement; compression, indexing, and
rollups can proceed after ingestion.

Support OTLP transport and response rules deliberately, including partial acceptance, retryable
failures, and backpressure. An ambiguous failure can cause a client to resend accepted data.
OTLP does not provide a universal record identity that makes every retry deduplicable. See the
[OTLP specification][otlp].

| Signal | Proposed storage | Main access paths |
| --- | --- | --- |
| Logs | Batched append storage and compressed column segments | Tenant, time, resource/service, selected attributes, text search. |
| Traces | Append-oriented spans with nested events and links | Trace ID lookup, service/time filters, duration, status, trace assembly. |
| Metrics | Typed time-series blocks and derived rollups | Series selection, time ranges, rates, histogram aggregation. |
| Profiles | Dictionary-encoded stacks and sample blocks | Time, service, stack aggregation, trace correlation. |

Preserve resources, instrumentation scopes, schema information, typed attributes, optional
fields, and signal-specific flags. Repeated resource and scope values are candidates for
dictionary encoding. Frequently queried fields can become typed columns while arbitrary
attributes retain their types without creating a schema column or index for every key.

Profiles belong in the longer-term scope, but their current OTel specification is alpha.
Keep its representation versioned and adaptable. See [OTel profiles][otel-profiles].

### 10.1 Logs and traces

Logs preserve event time and observed time separately, severity, body, attributes, and optional
trace/span context. The body need not be a string. Traces preserve span and parent identity,
timing, kind, status, attributes, events, and links. A trace consists of multiple spans, which
can arrive late or out of order; a trace ID alone is not a unique span identity.

Use time-oriented segment pruning for broad searches and dedicated trace-ID lookup for trace
retrieval. Text search and selected attribute indexes are additional access methods over the
same logical records. Index construction and freshness must be visible to the query planner.
See the [OTel logs data model][otel-logs].

### 10.2 Metrics

Metrics require a representation and operators that preserve their meaning. Keep metric and
series identity, units, resource/scope information, typed values, point attributes, timestamps,
start timestamps, aggregation temporality, monotonicity where applicable, and exemplars.
Support the distinctions between gauges, sums, histograms, exponential histograms, and summaries.

Aggregation must account for:

- Delta versus cumulative reporting and resets identified by the appropriate state and times.
- Late or out-of-order samples, duplicates, and conflicting writers.
- Histogram boundary or scale compatibility and correct bucket aggregation.
- The difference between mergeable distributions and non-mergeable reported quantiles.
- What resolution and information a rollup discards.

For example, adding successive cumulative counter values double-counts measurements. Averaging
reported p99 values does not produce a p99 for the combined population. Define rate, alignment,
temporality conversion, and rollup behavior before advertising query compatibility. Vortex
provides encodings; it does not define these time-series operations. See the
[OTel metrics data model][otel-metrics].

Series dictionaries, label indexes, and rollup state must be disk-backed or bounded. Per-tenant
cardinality limits and admission rules prevent arbitrary labels from creating unbounded memory
or metadata work.

### 10.3 Publication, freshness, and correlation

Raw ingestion and derived representations use the coverage invariant from section 7. For a
query's snapshot, each required logical input contributes exactly once through the selected
representation or the remaining visible input path. Coverage identifies committed ingestion
records or ranges, possibly per partition with exceptions; event timestamps alone do not
identify that input set. This accounts for representations, not for whether OTLP retries were
deduplicated.

A query must know which inputs are in completed column segments, indexes, or rollups and which
remain in the unprocessed tail or correction state. Publish the representation and its coverage
atomically, and retain the descriptor versions needed by older snapshots. Combining a rollup
with remaining inputs also requires the signal-specific operations from section 10.2; arbitrary
aggregates cannot be corrected simply by adding another aggregate to them.

A current-data query combines a complete derived representation with its remaining visible
inputs exactly once, or uses a complete raw path. Use raw fallback only while it remains
complete for the requested result. A query intentionally using only a derived view reports its
freshness boundary and resolution; it must not silently present an incomplete result as current.
Ingestion progress is not an event-time watermark: older events may still arrive.

Retention and late-data acceptance are one contract. If a rollup survives after its raw inputs
expire, define whether late input for that window is rejected, excluded under an explicit
policy, or accepted for correction. Accepted corrections must retain the necessary raw input
or equivalent durable correction state until the replacement result and coverage are published
and older readers no longer need them. Expiry must not delete an accepted correction before
publication. If correction cannot be computed from the retained state, do not acknowledge the
input as successfully incorporated.

Correlate signals through trace IDs, span IDs, resource attributes, exemplars, and application
identifiers. SQL can join telemetry with domain events and application tables. Telemetry
exported from another process still arrives asynchronously; storing it in Atoll does not make
its arrival atomic with the originating application's transaction.

SQL is the first general query surface. PromQL compatibility, trace and log query APIs,
dashboards, and alert evaluation are additional layers with their own semantics and tests.

## 11. Retention, deletion, and resource control

### 11.1 Retention and deletion

Choose retention separately for stream records, logs, spans, raw metric samples, rollups,
and profiles, subject to their coverage and correction dependencies. Define whether each
policy uses event time or ingestion time and what happens when late data belongs to an expired
range. Keep required dictionaries and indexes alive for as long as their records remain readable.

Each policy states whether it expires logical records or removes only a physical representation.
Retained rollups are not a lossless replacement for expired raw records. Raw expiry must obey
section 10.3's correction-publication rule and must not leave a query claiming a complete raw
fallback when one no longer exists.

A tombstone makes data logically absent; compressed segments may still contain its bytes.
Compaction and reclamation must reconcile retention, active snapshots, remote manifests, and
the erasure requirements in DESIGN.md. Do not claim prompt byte erasure until that policy is
specified for every retained copy. Existing database and sidecar compatibility remains required
when adding formats and metadata.

### 11.2 Admission, accounting, and progress

One engine needs shared admission and accounting across:

- Buffer frames, owned scan batches, decompression, and query operator state.
- Parallel tasks, ingestion batches, prefetch, and IO in flight.
- Join/sort spill, temporary storage, compaction, indexing, and uploads.
- Transaction slots, retained versions, and unresolved stream reservations.
- WAL admission, durability flushes, and per-tenant ingestion or series growth.

Enforcement is part of the first read-only adapter, not a later optimization. Define and enforce
per-query owned-memory and spill limits, shared task and IO limits, and a policy for query
duration and retained versions. Charge copies and decompression before allocating them, account
for buffer alignment and rounding, and keep returned batches charged until their last owner
releases them. Background work and ingestion need their own limits within the shared allowance.

Reserve capacity outside analytical allowances for IO completion, cancellation cleanup,
rollback, and other transactions. A query must not consume the resources required to stop it.
When a limit is reached, either yield on work that can independently release capacity or fail
with an explicit resource error; do not wait while retaining the only resources needed to
make that work progress. Cleanup follows section 8.2, including late completions. Cancellation
does not make unfinished steps, submitted IO, or retained output buffers disappear.

Long scans and telemetry bursts must not consume all memory, transaction slots, or IO capacity.
Scan-aware caching and separate workload budgets should preserve OLTP latency; measure that
interference rather than assuming separate executors provide isolation. Budget values,
reservation sizes, and scheduling policy require measurements before choosing defaults.

### 11.3 Tenant and authorization boundary

The first prototype is trusted and single-tenant within one database. It does not expose a
multitenant ingestion service or claim tenant isolation.

Before multitenant use, adapters must receive tenant identity from authenticated request
context or trusted configuration, not from an OTLP attribute or another caller-controlled
record field. SQL, stream, and telemetry interfaces must enforce the same authorization policy
for logical relations and histories. These checks belong at the common access boundary, not
only in one executor's filters. The authentication and authorization interfaces remain a
separate design decision.

## 12. Order of work and acceptance gates

These are future milestones, not additions to the completed work in DESIGN.md. The existing
buffer, B-tree, transaction, and recovery acceptance gates remain prerequisites; this proposal
does not mark any of them complete.

1. **Complete the Atoll transaction foundation.** Finish and validate the concurrency, recovery,
   catalog, and Turso integration needed for shared snapshots and writes.
2. **Read-only DataFusion adapter.** Specify the engine-neutral read context and batch-scan
   contract first, including acquisition, partition coverage, ownership, cleanup, and errors.
   Then implement row-table scans with the scalar boundary in section 5.1 and resource limits
   from the start. Pass the row-only demonstration below before adding another layout.
3. **Immutable Vortex segments.** Add typed analytical relations, page-backed segments, pruning,
   durable publication, and scans. Repeat the common-contract tests across representation
   changes. Prove base/update coverage separately before exposing transparent projections of
   arbitrary OLTP tables.
4. **Transactional append storage.** Specify and prove reserved offsets, snapshot-compatible
   consumption frontiers, producer retries, retention, and conditional ownership checks. Then
   prove atomic consume/update/publish/progress within one database.
5. **OTLP logs and traces.** Reuse append storage, add signal schemas and lookup paths, and query
   both recent and compressed data. Independent schema/ingestion experiments can happen earlier.
6. **Metrics.** Add bounded series indexing, typed metric blocks, temporality-aware operations,
   late-data corrections and retention, and recoverable rollups.
7. **Additional engines and interfaces.** Evaluate DuckDB, broader automatic SQL routing,
   multitenant access, observability query compatibility, Kafka compatibility, and profiles as
   separate increments.

### 12.1 First proof: a read-only join over row tables

Use two ordinary row tables with supported scalar types. One stores application data; the other
stores event rows. A Turso transaction updates an application row and inserts an event row.
The event needs no native stream offsets or special append representation.

The read-only DataFusion query joins those tables through parallel scans. Acceptance requires:

- Every execution uses one retained read context. A concurrent observer sees both effects of
  the committed transaction or neither, never one table at an older snapshot than the other.
  Compare the result multiset with serial row reads at the same snapshot.
- A query inside a transaction sees earlier statements' writes, not current-statement writes.
  Repeated prepared executions use fresh snapshots under read committed and the retained
  transaction snapshot under snapshot isolation, with the appropriate statement cutoff.
- Prepared-plan reuse rejects incompatible catalog changes before output. Already executing
  scans retain the catalog versions they need. Concurrent splits, merges, and deletes do not
  duplicate or omit logical rows across partitions.
- Supported values round-trip without coercion; unsupported storage classes and collations
  return explicit errors. Filtering and the final global limit remain in DataFusion.
- Memory and IO limits are enforced. With parallel scans and pending IO, test explicit
  cancellation, late type and IO errors, a satisfied limit, and result-stream drop. Cleanup
  progresses on a single-thread executor without further consumer polling, including late
  callbacks. The parent transaction can take its required next action, and retained output
  batches remain valid and charged until release.
- Crash recovery preserves the committed row update and event together. Run the transaction
  foundation's crash checks before claiming the combined demonstration is crash safe.

Vortex, transparent projections, native append storage, automatic SQL routing, OTLP, and a
second analytical engine are excluded from this first proof. Once it passes, repeat the relevant
snapshot, coverage, cancellation, and crash tests with immutable Vortex segments and their
descriptor publication, then with mutable/base combinations.

## 13. How to prove the design

Correctness checks must cover the common storage contract and each adapter:

- Compare row and batch result multisets with a model at the same snapshots under concurrent
  writes and physical partition changes. Stagger workers across atomic multi-table commits.
- Check earlier-statement own writes, repeated prepared executions under both isolation modes,
  catalog changes, and the prohibition on advancing a parent transaction during active scans.
- Check the read-completion durability dependency even when batches are produced in parallel.
- Round-trip supported scalar types and reject unsupported or heterogeneous values, including
  when the first incompatible value occurs in a later batch. When adding pushdown, put invisible
  versions and residual-filter failures before a matching row under a global limit.
- Exercise a changed base row that stops matching a predicate, deletes, and conflicting updates.
- Crash at segment construction, adoption, descriptor replacement, and coverage publication.
- Check that compaction preserves row identity, index results, and reads from older snapshots.
- Reproduce the offset-100/101 commit inversion. A relational join must see B's application
  update and event together even while consumption waits for A. An old consumer snapshot must
  not use a newer frontier to skip A's later-visible record.
- Check aborted and compacted offset gaps, restarts, obsolete consumer ownership, stream
  deletion/recreation, and expired producer retry state; none may silently change a position's
  history or skip a record that can still become visible within the retained contract.
- Crash atomic consume/update/publish/progress transactions and compare all effects with a model.
- Check OTLP acceptance/retry behavior, signal round trips, late and duplicate telemetry, metric
  resets, and histogram/rollup results against explicit expected values. Race raw expiry with
  accepted late corrections and crash before their replacement coverage is published.
- Corrupt segment metadata and unsupported format versions; report errors without using invalid
  references or silently changing stored values.
- Verify old databases remain readable across format and dependency upgrades.
- Force memory pressure, errors, cancellation, early limits, and result-stream drop during IO,
  decompression, joins, and background work. Race completion with waiter registration, deliver
  callbacks late and out of order, and stop polling the result stream. Check independent cleanup
  progress and output-buffer accounting until final release, including when analytical memory
  and spill allowances are exhausted.

Use deterministic interleavings and crash IO for publication and lifetime protocols. Permanent
regressions belong in the narrowest existing harness that can express them. SQL compatibility
claims need comparisons with the relevant dialect and supported engine, rather than assuming
that successful parsing establishes equivalent results.

Measure narrow and wide projections, selective and full scans, joins between row and column
storage, and point fetches from compressed data. Include local and planned remote storage,
datasets larger than memory, compression and copy cost, size-class slack, and metadata usage.
Measure ingestion and consumer catch-up alongside OLTP commit latency, version retention,
write amplification, compaction, and query spill. No throughput or latency target is established
by this proposal.

## 14. Decisions still to make

The first prototype's read-only, row-only, trusted single-tenant scope and the contracts above
are the starting decisions. The remaining choices include:

- The user-facing analytical execution interface and SQL semantics beyond the initial boundary.
- The concrete engine-neutral read-context and batch-scan interfaces: creation, catalog
  retention, partition description, polling, durability completion, cancellation, and errors.
- The shared catalog's relation, type, and physical-layout metadata.
- The base/update coverage and catch-up algorithm needed for transparent projections; separate
  typed analytical relations come first.
- Column chunk sizes, segment packing, statistics granularity, and an initial Vortex edition.
- The durable reservation and frontier-publication protocol for the selected stream model,
  including producer retry state, ownership changes, expiry, and retention.
- OTLP transport scope, signal-specific duplicate policy, and raw/derived freshness behavior.
- Metric temporality normalization, admissible late-data corrections, and rollup definitions.
- Concrete workload budgets and reserved capacity, scheduling policy, deletion guarantees, and
  measured acceptable OLTP interference.
- Authentication and common authorization before exposing multitenant interfaces.
- Scaling beyond one database's ordered WAL and transaction domain.

## References

- [Atoll storage design](DESIGN.md) and [concurrency rules](CONCURRENCY.md).
- [Umbra: A Disk-Based System with In-Memory Performance][umbra].
- [DataFusion custom table providers][datafusion].
- [DuckDB storage-extension interface][duckdb-storage] and [SQL dialect][duckdb-dialect].
- [Vortex layouts][vortex-layouts], [serialization][vortex-serialization], and
  [wire-format compatibility][vortex-versioning].
- [Vortex DataFusion integration][vortex-datafusion] and [DuckDB integration][vortex-duckdb].
- [Kafka consumer semantics][kafka-consumer] and [log design][kafka-design].
- [OTLP specification][otlp], [logs data model][otel-logs],
  [metrics data model][otel-metrics], and [profiles][otel-profiles].

[umbra]: https://www.vldb.org/cidrdb/papers/2020/p29-neumann-cidr20.pdf
[datafusion]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html
[duckdb-storage]: https://github.com/duckdb/duckdb/blob/main/src/include/duckdb/storage/storage_extension.hpp
[duckdb-dialect]: https://www.duckdb.org/2026/08/20/duckdb-20-peg-parser
[vortex-layouts]: https://docs.vortex.dev/concepts/layouts
[vortex-serialization]: https://docs.vortex.dev/developer-guide/internals/serialization
[vortex-versioning]: https://docs.vortex.dev/specs/versioning
[vortex-datafusion]: https://docs.vortex.dev/user-guide/datafusion
[vortex-duckdb]: https://docs.vortex.dev/user-guide/duckdb
[kafka-consumer]: https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
[kafka-design]: https://kafka.apache.org/20/design/design/
[otlp]: https://opentelemetry.io/docs/specs/otlp/
[otel-logs]: https://opentelemetry.io/docs/specs/otel/logs/data-model/
[otel-metrics]: https://opentelemetry.io/docs/specs/otel/metrics/data-model/
[otel-profiles]: https://opentelemetry.io/docs/specs/otel/profiles/
