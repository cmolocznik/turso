//! IVF partitions with RaBitQ codes over dense f32 vectors, kept in core-owned backing tables, so
//! every change to the index commits in the transaction that changed its row, under WAL or MVCC.
//!
//! > CREATE INDEX t_idx ON t USING ivf_rq (embedding) WITH (nprobes = 16, refine = 4)
//! > SELECT vector_distance_dot(embedding, ?) AS distance FROM t ORDER BY distance LIMIT ?
//!
//! A write touches only rows owned by its own vector, so concurrent BEGIN CONCURRENT writers of
//! different rows never conflict. An insert codes the vector against its nearest partition's
//! centroid and writes two rows: a tail entry keyed by partition and rowid, and the vector's own
//! row, naming where its code lives. A delete removes the vector's row,
//! and its tail entry if it has one. A code packed in a block stays there until maintenance
//! rewrites the block, so the delete names the block in the vector's dead row, and a query skips
//! that stale copy before it can raise the bar the live entries are held to.
//!
//! A query reads a partition's bookkeeping first. The blocks of the partitions queries probe are
//! held in memory, shared by the index's cursors, each partition's under the version its
//! bookkeeping held when they were read: a query whose snapshot holds that version scores them
//! there and reads only the partition's dead rows and tail from pages. Its bookkeeping, dead rows
//! and tail are held beside the blocks with the snapshot they were read under, and serve a probe
//! under another snapshot, which then reads no page, while no transaction writing the index is
//! open and both snapshots are past the last commit that wrote the partition, as the index's
//! writers record it ([`Writes`]); not where other processes write the database. The memory is
//! bounded by the index's `cache_mib` parameter (default [`DEFAULT_CACHE_MIB`]), not by the index's
//! size: the least recently probed partition gives way first.
//!
//! A statement that writes the index does the maintenance its writes made due before they are
//! released, at most one job for each row it wrote, leaving the work it finds due past that to the
//! next statement's, and `OPTIMIZE INDEX t_idx` does every job there is. Maintenance conflicts
//! only with a write to a row it changes, and on a conflict it undoes its job and leaves the rows
//! to the writer holding them. Its jobs: pack a partition's tail into blocks of [`BLOCK`]; rewrite
//! a block holding [`COMPACT_AT`] stale copies without them; split a partition grown past an
//! eighth of the partition count, and two blocks, by 2-means over its members' vectors read back
//! from the table, so partitions stay near four times the square root of the row count, marking
//! it and its [`NEIGHBOURS`] nearest partitions moving, so moves recheck every entry there, move
//! each to its nearest partition and recode those that stay against their partition's new
//! centroid; retire a partition left with under a sixteenth of the split bound, moving its entries
//! to their nearest partitions; and rebuild, training partitions afresh by k-means over a sample
//! of every vector, when a statement inserting [`BULK_AT`] rows or more finds the index trained
//! for under a quarter of the partitions its rows call for. CREATE INDEX over [`BULK_AT`] rows or
//! more writes tail entries for its first rows only and rebuilds over all of them, so each of the
//! rest is coded into its block once. Every rewrite of a block takes it off the dead rows of the
//! copies it drops. A split never overwrites a centroid: it writes new versions, and every entry
//! names the version it was coded against, so an insert committed from a snapshot older than the
//! split still scores correctly until maintenance recodes it. The index starts as one partition
//! about the origin.
//!
//! Each vector carries a 7-bit extended RaBitQ code whose top bit is its 1-bit code. The 1-bit
//! codes are interleaved 32 vectors at a time for a fast scan over per-query nibble tables of
//! bytes. A query scores from its 7-bit code, where its block lies, each entry whose 1-bit
//! estimate raised by its error bound reaches the worst of the `refine * limit` best 7-bit
//! estimates so far, and reranks those best exactly from the table's vectors; an index created
//! `WITH (rerank = 0)` answers with those best estimates and their estimated distances instead,
//! reading no table row.
//!
//! Three backing tables, every key an integer so a seek compares no payload. Partitions holds each
//! partition's rows under one key range, `partition << 42 | kind << 40 | id`, so a query reads a
//! partition with one seek: its bookkeeping (next block sequence, packed size, entries in the open
//! block, the blocks still to move, state, the size a refused split waits for, and a version each
//! rewrite of its blocks draws afresh), its dead rows (rowid -> the blocks holding a stale copy of
//! the vector's code), its tail entries (rowid -> version, code) and its blocks (sequence ->
//! version, codes). Vectors maps a rowid to the location of its code. Centroids holds
//! `partition << 24 | version` -> centroid, and -1 -> a generation drawn afresh by each
//! maintenance change to them, under which cursors share the loaded model. Only maintenance writes
//! blocks, centroids and bookkeeping.
use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use crate::numeric::Numeric;
use crate::schema::Type;
use crate::types::IOResultOr;
use crate::{
    index_method::{
        parse_patterns, BackingColumn, BackingSchema, BackingStoreOp, BackingTable, IndexMethod,
        IndexMethodAttachment, IndexMethodConfiguration, IndexMethodContext, IndexMethodCostContext,
        IndexMethodCostEstimate, IndexMethodCursor, IndexMethodDefinition, IndexMethodSnapshotIdentity,
        IndexMethodTransactionMode,
    },
    storage::{
        btree::{BTreeKey, CursorTrait},
        wal::Wal,
    },
    sync::Arc,
    types::{IOResult, ImmutableRecord, ImmutableRecordRef, SeekKey, SeekOp, SeekResult},
    vdbe::Register,
    vector::{
        operations,
        vector_types::{Vector, VectorType},
    },
    Connection, LimboError, MvStore, Result, Value, ValueRef,
};
use turso_parser::ast;

pub const IVF_RQ_INDEX_METHOD_NAME: &str = "ivf_rq";

/// A process-wide probe count for measurement sweeps; zero leaves the index's own `nprobes`.
pub static NPROBES_OVERRIDE: AtomicUsize = AtomicUsize::new(0);
/// A process-wide rerank factor for measurement sweeps; zero leaves the index's own `refine`.
pub static REFINE_OVERRIDE: AtomicUsize = AtomicUsize::new(0);
/// A process-wide anisotropy threshold in hundredths for measurement (100 turns it off); zero leaves
/// the index's own `anisotropy`.
pub static ANISOTROPY_OVERRIDE: AtomicUsize = AtomicUsize::new(0);
/// Nonzero ranks partitions to probe by the query's inner product with their centroids rather than
/// by distance, for measurement.
pub static PROBE_BY_PRODUCT: AtomicUsize = AtomicUsize::new(0);
/// A process-wide rerank switch for measurement sweeps: 1 reranks, 2 answers from the 7-bit
/// estimates; zero leaves the index's own `rerank`.
pub static RERANK_OVERRIDE: AtomicUsize = AtomicUsize::new(0);
/// Maintenance done in this process: packs, splits, entries recoded, stale entries dropped, and
/// the summed and longest split durations, for measurement.
pub static PACKS: AtomicUsize = AtomicUsize::new(0);
/// Maintenance jobs done.
pub static JOBS: AtomicUsize = AtomicUsize::new(0);
pub static MOVES: AtomicUsize = AtomicUsize::new(0);
pub static SPLITS: AtomicUsize = AtomicUsize::new(0);
pub static RECODED: AtomicUsize = AtomicUsize::new(0);
pub static DROPPED: AtomicUsize = AtomicUsize::new(0);
/// Blocks rewritten without their stale copies.
pub static COMPACTS: AtomicUsize = AtomicUsize::new(0);
/// Retirements started and finished.
pub static RETIRES: AtomicUsize = AtomicUsize::new(0);
/// Jobs reverted because another transaction held a row they wrote.
pub static CONFLICTS: AtomicUsize = AtomicUsize::new(0);
/// Rebuilds of the whole index.
pub static REBUILDS: AtomicUsize = AtomicUsize::new(0);
pub static SPLIT_MICROS: AtomicUsize = AtomicUsize::new(0);
pub static SPLIT_MICROS_MAX: AtomicUsize = AtomicUsize::new(0);
/// Whether queries and inserts record the counts and timings below, for measurement; while it
/// is off each costs one load.
pub static MEASURE: AtomicBool = AtomicBool::new(false);
/// Entries a query estimated from 1-bit codes, and those it went on to score from 7-bit codes.
pub static SCANNED: AtomicUsize = AtomicUsize::new(0);
pub static REFINED: AtomicUsize = AtomicUsize::new(0);
/// Entries whose raised 1-bit estimate from the fast scan reached a query's bar.
pub static CANDIDATES: AtomicUsize = AtomicUsize::new(0);
/// Partitions a query found held in memory under its snapshot's version, and those it read from
/// pages.
pub static CACHE_HITS: AtomicUsize = AtomicUsize::new(0);
pub static CACHE_MISSES: AtomicUsize = AtomicUsize::new(0);
/// Probes a query served from memory alone, the partition's rows held under an earlier snapshot.
pub static ROW_HITS: AtomicUsize = AtomicUsize::new(0);
/// A process-wide byte budget for every index's held partitions, for measurement sweeps;
/// `usize::MAX` leaves each index its own.
pub static CACHE_BYTES_OVERRIDE: AtomicUsize = AtomicUsize::new(usize::MAX);
/// Nanoseconds queries spent in each phase (scoring includes the 7-bit refine), and the records
/// they read, for measurement.
pub static READ_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static SCORE_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static RECORDS: AtomicUsize = AtomicUsize::new(0);
pub static RERANK_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static OPEN_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static INIT_NANOS: AtomicUsize = AtomicUsize::new(0);
/// Nanoseconds inserts and deletes spent coding vectors and changing the tables, and the inserts.
pub static ENCODE_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static CHANGE_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static INSERTS: AtomicUsize = AtomicUsize::new(0);

/// The most codes one block packs.
const BLOCK: usize = 64;
/// The tail length at which maintenance packs a partition, merging it into the partition's open
/// block, so a partition holds full blocks, at most one open block, and a short tail.
const PACK_AT: usize = 8;
/// The stale copies a block holds at which maintenance rewrites it without them.
const COMPACT_AT: usize = 8;
/// The most tail entries held with a partition's rows: one with more is about to be packed.
const HELD_TAIL: usize = 2 * PACK_AT;
const SEED: u64 = 0x9e37_79b9_7f4a_7c15;
/// The rounds of sign flips and transforms a rotation makes over a width that is a power of two,
/// and over any other, an even number.
const ROUNDS: usize = 3;
const KAC_ROUNDS: usize = 4;
/// The most centroids k-means seeds by k-means++.
const SEEDED_CENTROIDS: usize = 64;
/// Lloyd iterations: a rebuild's training over its sample, and a split's 2-means.
const TRAIN_ITERATIONS: usize = 20;
const SPLIT_ITERATIONS: usize = 8;
/// The rows a filtered query is taken to ask for when its LIMIT is not a literal, for its cost
/// estimate: the depth most vector searches ask for.
const ASSUMED_LIMIT: f64 = 10.0;
/// The codes a page of blocks holds, for a filtered query's cost estimate, in the planner's pages:
/// it prices a table row at a fiftieth of a page whatever the row's width, and a code is priced at
/// that rate scaled by its bytes against its vector's. A 768-dimensional vector's code, with its
/// rowid, factors and 7-bit parts, is 692 bytes, and the vector 3,072: 50 * 3,072 / 692.
const CODES_PER_PAGE: f64 = 222.0;
/// The pages a rerank reads per vector, for a filtered query's cost estimate: its row in the table.
const FETCH_PAGES: f64 = 1.0;
/// The bytes at the start of each held block a probe prefetches, for codes of `dims` padded
/// dimensions: a full block's count, rowids, gains and slacks, and its first scan group.
fn held_prefetch(dims: usize) -> usize {
    Layout::new(BLOCK, dims / 8).scan() + Layout::new(BLOCK, dims / 8).group_bytes()
}
/// The memory, in MiB, an index holds the blocks of the partitions queries probe in, unless its
/// `cache_mib` parameter says otherwise.
const DEFAULT_CACHE_MIB: usize = 256;
/// The rounds a rebuild balances its trained partitions over.
const BALANCE_ROUNDS: usize = 4;
/// The vector-centroid products below which an assignment pass runs on one core.
const PARALLEL_PRODUCTS: usize = 1 << 16;
/// Bits per dimension past the sign: the extended code is 7 bits, its top bit the 1-bit code.
const EX_BITS: usize = 6;
/// The extended code's values, `0..LEVELS`, each standing for itself less `CENTRE`.
const LEVELS: u32 = 1 << (EX_BITS + 1);
const CENTRE: f64 = (LEVELS - 1) as f64 / 2.0;
/// Vectors scored together by one pass over the fast-scan tables.
const GROUP: usize = 32;
/// The fewest members a group needs to be fast-scanned rather than summed one by one.
const SCAN_MEMBERS: usize = 8;
/// The RaBitQ confidence multiplier on the 1-bit estimate's error.
const EPSILON: f64 = 1.9;
/// Scales tried when choosing a vector's 7-bit grid.
const GRID_SCALES: usize = 48;

const PARTITIONS: usize = 0;
const VECTORS: usize = 1;
const CENTROIDS: usize = 2;
/// The partitions table's columns are positional: a bookkeeping row's eight fields, a dead row's
/// blocks, a tail entry's or block's version and codes.
const STORES: [(&str, &[(&str, Type)]); 3] = [
    ("partitions", &[("c0", Type::Integer), ("c1", Type::Blob), ("c2", Type::Integer), ("c3", Type::Integer), ("c4", Type::Integer), ("c5", Type::Integer), ("c6", Type::Integer), ("c7", Type::Integer)]),
    ("vectors", &[("location", Type::Integer)]),
    ("centroids", &[("value", Type::Blob)]),
];
/// The centroids table's rowid for the generation.
const GENERATION: i64 = -1;
const PARTITION_BITS: u32 = 21;
const ROWID_BITS: u32 = 40;
const VERSION_BITS: u32 = 24;
/// The kinds of a partition's rows, in key order: so a query reads the dead rows before the
/// blocks whose stale copies they name.
const PART_ROW: i64 = 0;
const DEAD_ROW: i64 = 1;
const TAIL_ROW: i64 = 2;
const BLOCK_ROW: i64 = 3;
const PARTITION_SHIFT: u32 = ROWID_BITS + 2;

fn error(message: &str) -> Box<LimboError> {
    Box::new(LimboError::InternalError(format!("ivf_rq: {message}")))
}

fn partition_in_range(partition: i64) -> Result<()> {
    if !(0..1 << PARTITION_BITS).contains(&partition) {
        return Err(*error("partition out of range"));
    }
    Ok(())
}

fn rowid_in_range(rowid: i64) -> Result<()> {
    if !(0..1 << ROWID_BITS).contains(&rowid) {
        return Err(*error("rows indexed by ivf_rq need rowids in 0..2^40"));
    }
    Ok(())
}

/// The key of a partition's row of `kind` with `id`, a rowid or a block's sequence.
fn partition_key(partition: i64, kind: i64, id: i64) -> Result<i64> {
    partition_in_range(partition)?;
    Ok(partition << PARTITION_SHIFT | kind << ROWID_BITS | id)
}

/// The partition a row of the partitions table belongs to, and the kind of row it is.
fn partition_of(key: i64) -> i64 {
    key >> PARTITION_SHIFT
}

fn kind_of(key: i64) -> i64 {
    (key >> ROWID_BITS) & 3
}

/// The keys of a partition's rows of `kind`, or from `kind` on: the start, and the first past them.
fn rows_of(partition: i64, kind: i64) -> Result<(i64, i64)> {
    let start = partition_key(partition, kind, 0)?;
    Ok((start, start + (1 << ROWID_BITS)))
}

fn rows_from(partition: i64, kind: i64) -> Result<(i64, i64)> {
    Ok((partition_key(partition, kind, 0)?, partition_key(partition, 0, 0)?.saturating_add(1 << PARTITION_SHIFT)))
}

/// A partition's bookkeeping row's key, the first of its rows.
fn part_key(partition: i64) -> Result<i64> {
    partition_key(partition, PART_ROW, 0)
}

/// A packed block's key: its partition, then its sequence within the partition.
fn block_key(partition: i64, sequence: i64) -> Result<i64> {
    if !(0..1 << 32).contains(&sequence) {
        return Err(*error("block sequence out of range"));
    }
    partition_key(partition, BLOCK_ROW, sequence)
}

/// A tail entry's key: its partition, then the vector's rowid.
fn tail_key(partition: i64, rowid: i64) -> Result<i64> {
    rowid_in_range(rowid)?;
    partition_key(partition, TAIL_ROW, rowid)
}

/// A dead row's key: the partition of the block holding the stale copy, then the vector's rowid.
fn dead_key(block: i64, rowid: i64) -> Result<i64> {
    rowid_in_range(rowid)?;
    partition_key(partition_of(block), DEAD_ROW, rowid)
}

fn centroid_key(partition: i64, version: i64) -> Result<i64> {
    partition_in_range(partition)?;
    if !(0..1 << VERSION_BITS).contains(&version) {
        return Err(*error("centroid version out of range"));
    }
    Ok(partition << VERSION_BITS | version)
}

/// Where a vector's code lives, as its row records it: a packed block's key, or a tail key negated
/// past zero.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Location {
    Block(i64),
    Tail(i64),
}

impl Location {
    fn encode(self) -> i64 {
        match self {
            Location::Block(key) => key,
            Location::Tail(key) => -1 - key,
        }
    }
    fn decode(value: i64) -> Self {
        if value >= 0 {
            Location::Block(value)
        } else {
            Location::Tail(-1 - value)
        }
    }
}

#[derive(Debug, PartialEq)]
struct Ordered(f64, i64);
impl Eq for Ordered {}
impl PartialOrd for Ordered {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ordered {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0).then(self.1.cmp(&other.1))
    }
}

/// Whether this CPU runs the AVX2 and FMA variants of the kernels below. Each kernel is one
/// body, inlined into a plain and an AVX2 wrapper, with independent accumulators so it vectorizes
/// without reassociating a float sum.
fn wide() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

macro_rules! kernel {
    ($name:ident, $wide:ident, $body:ident, ($($arg:ident: $ty:ty),*) -> $out:ty) => {
        fn $name($($arg: $ty),*) -> $out {
            #[cfg(target_arch = "x86_64")]
            if wide() {
                // SAFETY: the CPU has AVX2 and FMA.
                return unsafe { $wide($($arg),*) };
            }
            $body($($arg),*)
        }
        #[cfg(target_arch = "x86_64")]
        #[target_feature(enable = "avx2,fma")]
        unsafe fn $wide($($arg: $ty),*) -> $out {
            $body($($arg),*)
        }
    };
}

const LANES: usize = 16;

#[inline(always)]
fn l2_body(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0.0f32; LANES];
    let (head, tail) = a.split_at(a.len() / LANES * LANES);
    for (x, y) in head.chunks_exact(LANES).zip(b.chunks_exact(LANES)) {
        for k in 0..LANES {
            let d = x[k] - y[k];
            acc[k] += d * d;
        }
    }
    let mut total: f32 = acc.iter().sum();
    for (x, y) in tail.iter().zip(&b[head.len()..]) {
        total += (x - y) * (x - y);
    }
    total
}

#[inline(always)]
fn dot_body(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0.0f32; LANES];
    let (head, tail) = a.split_at(a.len() / LANES * LANES);
    for (x, y) in head.chunks_exact(LANES).zip(b.chunks_exact(LANES)) {
        for k in 0..LANES {
            acc[k] += x[k] * y[k];
        }
    }
    let mut total: f32 = acc.iter().sum();
    for (x, y) in tail.iter().zip(&b[head.len()..]) {
        total += x * y;
    }
    total
}

/// A rotated coordinate's extended code at scale `t`: `floor(v t)` raised by half the levels, so
/// the code's top bit is the coordinate's sign, within the levels.
#[inline(always)]
fn grid_code(v: f32, t: f32) -> f32 {
    ((v * t).floor() + (LEVELS / 2) as f32).clamp(0.0, (LEVELS - 1) as f32)
}

/// The direction-fit of the 7-bit grid at scale `t`: the code's product with `x` and its squared
/// length.
#[inline(always)]
fn fit_body(x: &[f32], t: f32) -> (f32, f32) {
    let (mut along, mut length) = ([0.0f32; LANES], [0.0f32; LANES]);
    let (head, tail) = x.split_at(x.len() / LANES * LANES);
    for chunk in head.chunks_exact(LANES) {
        for k in 0..LANES {
            let value = grid_code(chunk[k], t) - CENTRE as f32;
            along[k] += chunk[k] * value;
            length[k] += value * value;
        }
    }
    let (mut a, mut l): (f32, f32) = (along.iter().sum(), length.iter().sum());
    for v in tail {
        let value = grid_code(*v, t) - CENTRE as f32;
        a += v * value;
        l += value * value;
    }
    (a, l)
}

/// A fast Walsh-Hadamard transform of `x`, whose length is a power of two, scaled by `scale`.
#[inline(always)]
fn walsh_body(x: &mut [f32], scale: f32) {
    let mut h = 1;
    while h < x.len() {
        for block in x.chunks_exact_mut(2 * h) {
            let (low, high) = block.split_at_mut(h);
            for (a, b) in low.iter_mut().zip(high.iter_mut()) {
                let (p, q) = (*a, *b);
                *a = p + q;
                *b = p - q;
            }
        }
        h *= 2;
    }
    x.iter_mut().for_each(|a| *a *= scale);
}

#[inline(always)]
fn hadamard_body(x: &mut [f32], signs: &[f32], scale: f32) {
    for round in signs.chunks_exact(x.len()) {
        x.iter_mut().zip(round).for_each(|(a, s)| *a *= s);
        walsh_body(x, scale);
    }
}

/// Each round's sign flips over the whole of `x`, a transform of its leading `span`, or in the
/// next round its trailing one, and a step of a Kac walk, which sums and differences the halves of
/// `x`; the growth of lengths the steps make, the square root of two each, is undone at the end.
#[inline(always)]
fn kac_hadamard_body(x: &mut [f32], signs: &[f32], span: usize, scale: f32) {
    let (width, half) = (x.len(), x.len() / 2);
    for (round, flips) in signs.chunks_exact(width).enumerate() {
        x.iter_mut().zip(flips).for_each(|(a, s)| *a *= s);
        let at = if round % 2 == 0 { 0 } else { width - span };
        walsh_body(&mut x[at..at + span], scale);
        let (low, high) = x.split_at_mut(half);
        for (a, b) in low.iter_mut().zip(high.iter_mut()) {
            let (p, q) = (*a, *b);
            *a = p + q;
            *b = p - q;
        }
    }
    let rescale = 0.5f32.powi((signs.len() / width / 2) as i32);
    x.iter_mut().for_each(|a| *a *= rescale);
}

kernel!(l2, l2_wide, l2_body, (a: &[f32], b: &[f32]) -> f32);
kernel!(dot32, dot_wide, dot_body, (a: &[f32], b: &[f32]) -> f32);
kernel!(fit, fit_wide, fit_body, (x: &[f32], t: f32) -> (f32, f32));
kernel!(hadamard, hadamard_wide, hadamard_body, (x: &mut [f32], signs: &[f32], scale: f32) -> ());
kernel!(kac_hadamard, kac_hadamard_wide, kac_hadamard_body, (x: &mut [f32], signs: &[f32], span: usize, scale: f32) -> ());

/// The centroids a tile holds: enough independent multiply-adds a dimension to fill the AVX2 units.
const TILE: usize = 64;
/// The vectors [`Model::nearest_of`] runs over each tile while it is in cache.
const NEAREST_GROUP: usize = 16;

/// `v`'s products with a tile's centroids, the tile laid out dimension by dimension.
#[inline(always)]
fn tile_products_body(v: &[f32], tile: &[f32], out: &mut [f32; TILE]) {
    let mut acc = [0.0f32; TILE];
    for (x, column) in v.iter().zip(tile.chunks_exact(TILE)) {
        for k in 0..TILE {
            acc[k] += x * column[k];
        }
    }
    *out = acc;
}

kernel!(tile_products, tile_products_wide, tile_products_body, (v: &[f32], tile: &[f32], out: &mut [f32; TILE]) -> ());

/// [`tile_products_body`] over a tile of int8 values, each centroid's products still to be scaled.
#[inline(always)]
fn tile_products_bytes_body(v: &[f32], tile: &[i8], out: &mut [f32; TILE]) {
    let mut acc = [0.0f32; TILE];
    for (x, column) in v.iter().zip(tile.chunks_exact(TILE)) {
        for k in 0..TILE {
            acc[k] += x * column[k] as f32;
        }
    }
    *out = acc;
}

kernel!(tile_products_bytes, tile_products_bytes_wide, tile_products_bytes_body, (v: &[f32], tile: &[i8], out: &mut [f32; TILE]) -> ());

/// `x` in units of `scale`, rounded to int8.
fn quantized(x: f32, scale: f32) -> i8 {
    match scale > 0.0 {
        true => (x / scale).round().clamp(-127.0, 127.0) as i8,
        false => 0,
    }
}

/// A model's centroids in tiles of [`TILE`], each tile dimension-major, so a vector's products with
/// a tile's centroids are one pass over its dimensions. A tile's slots past the last centroid are
/// zero. `bytes` holds the same values rounded to int8 in units of each centroid's `scales`, its
/// largest value over 127, which a query ranks the partitions over, reading a quarter of the bytes.
#[derive(Debug, Clone, Default)]
struct Tiles {
    dims: usize,
    values: Vec<f32>,
    bytes: Vec<i8>,
    scales: Vec<f32>,
}

impl Tiles {
    /// Set partition `p`'s centroid, adding a tile when `p` lies past the last.
    fn set(&mut self, p: usize, c: &[f32]) {
        self.dims = c.len();
        let (tile, lane) = (p / TILE, p % TILE);
        let size = self.dims * TILE;
        if self.values.len() < (tile + 1) * size {
            self.values.resize((tile + 1) * size, 0.0);
            self.bytes.resize((tile + 1) * size, 0);
            self.scales.resize((tile + 1) * TILE, 0.0);
        }
        let scale = c.iter().fold(0.0f32, |m, x| m.max(x.abs())) / 127.0;
        self.scales[p] = scale;
        for (d, x) in c.iter().enumerate() {
            self.values[tile * size + d * TILE + lane] = *x;
            self.bytes[tile * size + d * TILE + lane] = quantized(*x, scale);
        }
    }

    /// The tiles in partition order, each with its first partition.
    fn tiles(&self) -> impl Iterator<Item = (usize, &[f32])> + '_ {
        self.values.chunks_exact((self.dims * TILE).max(1)).enumerate().map(|(t, tile)| (t * TILE, tile))
    }

    /// The tiles of int8 values in partition order, each with its first partition.
    fn bytes(&self) -> impl Iterator<Item = (usize, &[i8])> + '_ {
        self.bytes.chunks_exact((self.dims * TILE).max(1)).enumerate().map(|(t, tile)| (t * TILE, tile))
    }
}

/// A randomized Hadamard rotation of vectors padded to a multiple of 32: seeded sign flips, each
/// followed by a fast Walsh-Hadamard transform, normalized so lengths are kept. A width that is a
/// power of two is transformed whole in each of [`ROUNDS`] rounds; any other, as the RaBitQ
/// library rotates it, by its largest power-of-two span, leading and trailing in turn, in each of
/// [`KAC_ROUNDS`] rounds, each round followed by a step of a Kac walk across its halves. A
/// 768-dimensional vector is so coded in 768 dimensions rather than 1,024.
#[derive(Debug, Clone)]
struct Rotation {
    padded: usize,
    /// The largest power of two within `padded`, which each transform spans.
    span: usize,
    signs: Vec<f32>,
}

impl Rotation {
    /// The width vectors of `dims` dimensions are padded to.
    fn padded(dims: usize) -> usize {
        dims.next_multiple_of(32).max(32)
    }

    fn new(dims: usize) -> Self {
        let padded = Rotation::padded(dims);
        let span = 1 << padded.ilog2();
        let rounds = if span == padded { ROUNDS } else { KAC_ROUNDS };
        let mut state = SEED;
        let signs = (0..rounds * padded)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                if state & 1 == 0 {
                    1.0
                } else {
                    -1.0
                }
            })
            .collect();
        Self { padded, span, signs }
    }

    fn apply(&self, v: &[f32]) -> Vec<f32> {
        let mut x = vec![0.0f32; self.padded];
        x[..v.len()].copy_from_slice(v);
        let scale = 1.0 / (self.span as f32).sqrt();
        match self.span == self.padded {
            true => hadamard(&mut x, &self.signs, scale),
            false => kac_hadamard(&mut x, &self.signs, self.span, scale),
        }
        x
    }

    fn code_bytes(&self) -> usize {
        self.padded / 8
    }
}

/// A vector's code: its rowid; what its 1-bit estimate is scaled by, what its error bound grows
/// with, and what its 7-bit estimate is scaled by, each folding in the residual's length and the
/// code's correction (the inner product of the true unit residual with its code); the rotated
/// signs; and the six lower bits of each dimension's 7-bit code, four dimensions to three bytes.
#[derive(Clone)]
struct Entry {
    rowid: i64,
    gain: f32,
    slack: f32,
    scale: f32,
    bits: Vec<u8>,
    ex: Vec<u8>,
}

/// A block, laid out so a query reads it in place: count u16; rowids i64[n]; gains and slacks
/// f32[n] each; then per group of 32 entries, for each nibble position j of the 1-bit code, 16
/// bytes whose low nibbles are entries 0..16's nibble j and high nibbles entries 16..32's; then
/// each entry's 7-bit parts.
struct Layout {
    count: usize,
    positions: usize,
    code_bytes: usize,
}

impl Layout {
    fn new(count: usize, code_bytes: usize) -> Self {
        Self { count, positions: code_bytes * 2, code_bytes }
    }
    fn groups(&self) -> usize {
        self.count.div_ceil(GROUP)
    }
    fn group_bytes(&self) -> usize {
        self.positions * 16
    }
    fn rowids(&self) -> usize {
        2
    }
    fn gains(&self) -> usize {
        self.rowids() + 8 * self.count
    }
    fn slacks(&self) -> usize {
        self.gains() + 4 * self.count
    }
    fn scan(&self) -> usize {
        self.slacks() + 4 * self.count
    }
    /// Each entry's 7-bit parts: its scale, then its lower bits.
    fn extras(&self) -> usize {
        self.scan() + self.groups() * self.group_bytes()
    }
    fn extra_bytes(&self) -> usize {
        4 + EX_BITS * self.code_bytes
    }
    fn len(&self) -> usize {
        self.extras() + self.count * self.extra_bytes()
    }
}

fn f32_at(payload: &[u8], at: usize) -> f32 {
    f32::from_le_bytes(payload[at..at + 4].try_into().unwrap())
}

fn i64_at(payload: &[u8], at: usize) -> i64 {
    i64::from_le_bytes(payload[at..at + 8].try_into().unwrap())
}

fn block_count(payload: &[u8]) -> Result<usize> {
    Ok(u16::from_le_bytes(payload.get(..2).ok_or_else(|| LimboError::Corrupt("ivf_rq: short block".into()))?.try_into().unwrap()) as usize)
}

fn pack(entries: &[Entry]) -> Result<Vec<u8>> {
    let code_bytes = entries.first().map_or(0, |e| e.bits.len());
    let layout = Layout::new(entries.len(), code_bytes);
    let mut out = vec![0u8; layout.len()];
    out[..2].copy_from_slice(&(entries.len() as u16).to_le_bytes());
    for (n, e) in entries.iter().enumerate() {
        if e.ex.len() != EX_BITS * code_bytes {
            return Err(*error("an entry to pack lacks its 7-bit code"));
        }
        out[layout.extras() + n * layout.extra_bytes()..][..layout.extra_bytes()].copy_from_slice(&extra(e));
        out[layout.rowids() + 8 * n..][..8].copy_from_slice(&e.rowid.to_le_bytes());
        out[layout.gains() + 4 * n..][..4].copy_from_slice(&e.gain.to_le_bytes());
        out[layout.slacks() + 4 * n..][..4].copy_from_slice(&e.slack.to_le_bytes());
        let group = layout.scan() + (n / GROUP) * layout.group_bytes();
        let (slot, shift) = (n % 16, if n % GROUP < 16 { 0 } else { 4 });
        for j in 0..layout.positions {
            let nibble = (e.bits[j / 2] >> (4 * (j & 1))) & 0xf;
            out[group + j * 16 + slot] |= nibble << shift;
        }
    }
    Ok(out)
}

/// The code width a block of `count` entries and `len` bytes was packed at.
fn block_code_bytes(payload: &[u8]) -> Result<usize> {
    let count = block_count(payload)?;
    let fixed = Layout::new(count, 0).len();
    let groups = count.div_ceil(GROUP).max(1);
    let coded = payload.len().checked_sub(fixed).ok_or_else(|| LimboError::Corrupt("ivf_rq: short block".into()))?;
    // Each code byte takes 32 bytes of each scan group and EX_BITS bytes of each entry's lower bits.
    let per_code_byte = groups * 32 + EX_BITS * count;
    if count == 0 || coded % per_code_byte != 0 {
        return Err(LimboError::Corrupt("ivf_rq: block length differs from its count".into()));
    }
    Ok(coded / per_code_byte)
}

/// A block's entries, with their 7-bit parts.
fn unpack(payload: &[u8]) -> Result<Vec<Entry>> {
    let count = block_count(payload)?;
    let code_bytes = block_code_bytes(payload)?;
    let layout = Layout::new(count, code_bytes);
    if payload.len() != layout.len() {
        return Err(LimboError::Corrupt("ivf_rq: block length differs from its count".into()));
    }
    Ok((0..count)
        .map(|n| {
            let group = layout.scan() + (n / GROUP) * layout.group_bytes();
            let (slot, shift) = (n % 16, if n % GROUP < 16 { 0 } else { 4 });
            let mut bits = vec![0u8; code_bytes];
            for j in 0..layout.positions {
                let nibble = (payload[group + j * 16 + slot] >> shift) & 0xf;
                bits[j / 2] |= nibble << (4 * (j & 1));
            }
            let extra = &payload[layout.extras() + n * layout.extra_bytes()..][..layout.extra_bytes()];
            Entry {
                rowid: i64_at(payload, layout.rowids() + 8 * n),
                gain: f32_at(payload, layout.gains() + 4 * n),
                slack: f32_at(payload, layout.slacks() + 4 * n),
                scale: f32_at(extra, 0),
                bits,
                ex: extra[4..].to_vec(),
            }
        })
        .collect())
}

/// An entry's 7-bit parts as its block holds them: its scale, then its lower bits.
fn extra(entry: &Entry) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + entry.ex.len());
    out.extend_from_slice(&entry.scale.to_le_bytes());
    out.extend_from_slice(&entry.ex);
    out
}

/// The rotated query's product with an entry's lower bits: the 7-bit code's part below the sign.
fn lower_product(rotated: &[f32], ex: &[u8]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: AVX2 is present, and `ex` holds six bytes for each eight dimensions of `rotated`.
        return unsafe { lower_product_avx2(rotated, ex) };
    }
    lower_product_scalar(rotated, ex)
}

fn lower_product_scalar(rotated: &[f32], ex: &[u8]) -> f64 {
    // Eight dimensions a step, their six-bit values unpacked from two 24-bit words: the even steps
    // in the first eight lanes, the odd in the last eight.
    let mut sums = [0.0f32; 16];
    for (step, (bytes, q)) in ex.chunks_exact(6).zip(rotated.chunks_exact(8)).enumerate() {
        let lanes = &mut sums[8 * (step % 2)..][..8];
        let low = bytes[0] as u32 | (bytes[1] as u32) << 8 | (bytes[2] as u32) << 16;
        let high = bytes[3] as u32 | (bytes[4] as u32) << 8 | (bytes[5] as u32) << 16;
        for k in 0..4 {
            lanes[k] += q[k] * ((low >> (6 * k)) & 63) as f32;
            lanes[4 + k] += q[4 + k] * ((high >> (6 * k)) & 63) as f32;
        }
    }
    sums.iter().map(|&s| s as f64).sum()
}

/// [`lower_product_scalar`] eight dimensions to a register: each 32-bit lane takes the 24-bit
/// word its dimension lies in and shifts its six bits down, and the lanes add as the scalar ones
/// do, the even steps in one register and the odd in another.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn lower_product_avx2(rotated: &[f32], ex: &[u8]) -> f64 {
    use std::arch::x86_64::*;
    debug_assert!(rotated.len() % 8 == 0 && ex.len() == rotated.len() / 8 * 6);
    let words = _mm256_setr_epi8(0, 1, 2, -1, 0, 1, 2, -1, 0, 1, 2, -1, 0, 1, 2, -1, 3, 4, 5, -1, 3, 4, 5, -1, 3, 4, 5, -1, 3, 4, 5, -1);
    let shifts = _mm256_setr_epi32(0, 6, 12, 18, 0, 6, 12, 18);
    let mask = _mm256_set1_epi32(63);
    let mut sums = [_mm256_setzero_ps(); 2];
    let steps = rotated.len() / 8;
    for step in 0..steps {
        // Eight bytes read from the step's six, but the last step's bytes copied out first, so no
        // read passes the code's end.
        let mut last = [0u8; 8];
        let bytes = match step + 1 < steps {
            true => ex.as_ptr().add(6 * step),
            false => {
                last[..6].copy_from_slice(&ex[6 * step..6 * step + 6]);
                last.as_ptr()
            }
        };
        let both = _mm256_broadcastsi128_si256(_mm_loadl_epi64(bytes as *const __m128i));
        let values = _mm256_and_si256(_mm256_srlv_epi32(_mm256_shuffle_epi8(both, words), shifts), mask);
        let q = _mm256_loadu_ps(rotated.as_ptr().add(8 * step));
        sums[step % 2] = _mm256_add_ps(sums[step % 2], _mm256_mul_ps(q, _mm256_cvtepi32_ps(values)));
    }
    let mut lanes = [0.0f32; 16];
    _mm256_storeu_ps(lanes.as_mut_ptr(), sums[0]);
    _mm256_storeu_ps(lanes.as_mut_ptr().add(8), sums[1]);
    lanes.iter().map(|&s| s as f64).sum()
}

const _: () = assert!(EX_BITS == 6, "lower_product unpacks, and code packs, six lower bits");

/// The 7-bit code's product with the rotated query, `signed` being the query summed over the
/// entry's set sign bits and `lower` its product with the lower bits: the code's values are
/// `code - CENTRE`. An entry's 7-bit estimate is its centre's product plus its scale times this.
fn refined_product(signed: f64, lower: f64, sum: f64) -> f64 {
    (1u32 << EX_BITS) as f64 * signed + lower - CENTRE * sum
}

/// The query summed over one member's set code bits, exactly: its table value at each nibble
/// position, from `codes`, one group's fast-scan bytes, and `table`, the query's 16 unquantized
/// values per position. Four independent sums keep the adds from waiting on each other.
fn exact_sum(codes: &[u8], table: &[f32], slot: usize) -> f64 {
    let (lane, shift) = (slot % 16, if slot < 16 { 0 } else { 4 });
    let mut sums = [0.0f32; 4];
    for (j, (bytes, values)) in codes.chunks_exact(16).zip(table.chunks_exact(16)).enumerate() {
        let bytes: &[u8; 16] = bytes.try_into().expect("16 bytes a position");
        let values: &[f32; 16] = values.try_into().expect("16 values a position");
        sums[j % 4] += values[((bytes[lane] >> shift) & 0xf) as usize];
    }
    ((sums[0] + sums[1]) + (sums[2] + sums[3])) as f64
}

/// Ask for `bytes` to be brought into the caches ahead of their use.
fn prefetch(bytes: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    for line in bytes.chunks(64) {
        // SAFETY: a prefetch reads nothing and cannot fault; the address lies within `bytes`.
        unsafe { std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(line.as_ptr() as *const i8) };
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = bytes;
}

/// Sum each group's 32 entries' table values over every nibble position: `codes` is one group's
/// fast-scan bytes, `table` the query's 16 quantized values per position. A sum can pass 65535 once
/// a code has more than 257 positions, so it is kept in 32 bits.
fn scan_group(codes: &[u8], table: &[u8], out: &mut [u32; GROUP]) {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: AVX2 is present, and both slices hold 16 bytes for each of an even count of positions.
        unsafe { scan_group_avx2(codes, table, out) };
        return;
    }
    scan_group_scalar(codes, table, out);
}

fn scan_group_scalar(codes: &[u8], table: &[u8], out: &mut [u32; GROUP]) {
    out.fill(0);
    for (c, t) in codes.chunks_exact(16).zip(table.chunks_exact(16)) {
        for b in 0..16 {
            out[b] += t[(c[b] & 0xf) as usize] as u32;
            out[16 + b] += t[(c[b] >> 4) as usize] as u32;
        }
    }
}

/// The steps of two positions each that a 16-bit lane sums before it is widened: a step adds two
/// bytes, at most 510, so 128 of them stay within 65535.
#[cfg(target_arch = "x86_64")]
const SCAN_STEPS_PER_WIDEN: usize = 128;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_group_avx2(codes: &[u8], table: &[u8], out: &mut [u32; GROUP]) {
    use std::arch::x86_64::*;
    debug_assert!(codes.len() == table.len() && codes.len() % 32 == 0);
    let mask = _mm256_set1_epi8(0x0f);
    let mut totals = [_mm256_setzero_si256(); 4];
    for chunk in (0..codes.len()).step_by(32 * SCAN_STEPS_PER_WIDEN) {
        let (mut low, mut high) = (_mm256_setzero_si256(), _mm256_setzero_si256());
        for at in (chunk..codes.len().min(chunk + 32 * SCAN_STEPS_PER_WIDEN)).step_by(32) {
            // Two positions per register: one in each 128-bit lane, as the shuffle works per lane.
            let c = _mm256_loadu_si256(codes.as_ptr().add(at) as *const __m256i);
            let t = _mm256_loadu_si256(table.as_ptr().add(at) as *const __m256i);
            let first = _mm256_shuffle_epi8(t, _mm256_and_si256(c, mask));
            let second = _mm256_shuffle_epi8(t, _mm256_and_si256(_mm256_srli_epi16(c, 4), mask));
            low = _mm256_add_epi16(low, _mm256_cvtepu8_epi16(_mm256_castsi256_si128(first)));
            low = _mm256_add_epi16(low, _mm256_cvtepu8_epi16(_mm256_extracti128_si256(first, 1)));
            high = _mm256_add_epi16(high, _mm256_cvtepu8_epi16(_mm256_castsi256_si128(second)));
            high = _mm256_add_epi16(high, _mm256_cvtepu8_epi16(_mm256_extracti128_si256(second, 1)));
        }
        // Entries 0..8, 8..16, 16..24 and 24..32, each widened into its 32-bit totals.
        totals[0] = _mm256_add_epi32(totals[0], _mm256_cvtepu16_epi32(_mm256_castsi256_si128(low)));
        totals[1] = _mm256_add_epi32(totals[1], _mm256_cvtepu16_epi32(_mm256_extracti128_si256(low, 1)));
        totals[2] = _mm256_add_epi32(totals[2], _mm256_cvtepu16_epi32(_mm256_castsi256_si128(high)));
        totals[3] = _mm256_add_epi32(totals[3], _mm256_cvtepu16_epi32(_mm256_extracti128_si256(high, 1)));
    }
    for (k, total) in totals.iter().enumerate() {
        _mm256_storeu_si256(out.as_mut_ptr().add(8 * k) as *mut __m256i, *total);
    }
}

/// What a block's 1-bit bound test reads from the query: the block's centroid product, the scan's
/// quantization (step, base and worst-case error), the query's summed components and spread, and
/// the bar.
struct Bound {
    centre: f64,
    step: f64,
    base: f64,
    error: f64,
    sum: f64,
    spread: f64,
    bar: f64,
}

impl Bound {
    /// A member's 1-bit estimate raised by its error bound and, from the fast scan's `quantized`
    /// sum, by the scan's quantization error.
    #[inline(always)]
    fn raised(&self, gain: f64, slack: f64, quantized: u32) -> f64 {
        let chosen = self.step * quantized as f64 + self.base;
        self.centre + gain * (2.0 * chosen - self.sum) + self.spread * slack + 2.0 * gain * self.error
    }
}

/// The members of a fast-scanned group whose raised estimate is not below the bar, one bit each:
/// [`Bound::raised`] for all 32 at once, deciding as it does.
fn reaching(sums: &[u32; GROUP], gains: &[f32; GROUP], slacks: &[f32; GROUP], bound: &Bound) -> u32 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: AVX2 is present.
        return unsafe { reaching_avx2(sums, gains, slacks, bound) };
    }
    reaching_scalar(sums, gains, slacks, bound)
}

fn reaching_scalar(sums: &[u32; GROUP], gains: &[f32; GROUP], slacks: &[f32; GROUP], bound: &Bound) -> u32 {
    let reaches = |slot: usize| bound.raised(gains[slot] as f64, slacks[slot] as f64, sums[slot]).partial_cmp(&bound.bar) != Some(std::cmp::Ordering::Less);
    (0..GROUP).filter(|&slot| reaches(slot)).fold(0, |mask, slot| mask | 1 << slot)
}

/// [`reaching_scalar`] four members to a register, each step the scalar's in the scalar's order, so
/// every member is decided alike.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn reaching_avx2(sums: &[u32; GROUP], gains: &[f32; GROUP], slacks: &[f32; GROUP], bound: &Bound) -> u32 {
    use std::arch::x86_64::*;
    let [step, base, error, sum, spread, centre, bar] = [bound.step, bound.base, bound.error, bound.sum, bound.spread, bound.centre, bound.bar].map(|v| _mm256_set1_pd(v));
    let two = _mm256_set1_pd(2.0);
    let mut mask = 0u32;
    for at in (0..GROUP).step_by(4) {
        let gain = _mm256_cvtps_pd(_mm_loadu_ps(gains.as_ptr().add(at)));
        let slack = _mm256_cvtps_pd(_mm_loadu_ps(slacks.as_ptr().add(at)));
        let quantized = _mm256_cvtepi32_pd(_mm_loadu_si128(sums.as_ptr().add(at) as *const __m128i));
        let chosen = _mm256_add_pd(_mm256_mul_pd(step, quantized), base);
        let estimate = _mm256_add_pd(centre, _mm256_mul_pd(gain, _mm256_sub_pd(_mm256_mul_pd(two, chosen), sum)));
        let raised = _mm256_add_pd(_mm256_add_pd(estimate, _mm256_mul_pd(spread, slack)), _mm256_mul_pd(_mm256_mul_pd(two, gain), error));
        mask |= (_mm256_movemask_pd(_mm256_cmp_pd::<_CMP_NLT_UQ>(raised, bar)) as u32) << at;
    }
    mask
}

/// A query's fast-scan tables: per nibble position, the rotated query summed over each nibble's
/// set bits, shifted by the position's least sum and quantized to a byte on one common step.
struct ScanTable {
    bytes: Vec<u8>,
    /// The same sums unquantized, for an exact 1-bit sum where a candidate is refined.
    exact: Vec<f32>,
    step: f64,
    base: f64,
    /// The worst-case error the quantization adds to a summed estimate, before scaling.
    error: f64,
}

impl ScanTable {
    fn new(rotated: &[f32]) -> Self {
        let sums: Vec<[f64; 16]> = rotated
            .chunks_exact(4)
            .map(|q| {
                let mut s = [0.0f64; 16];
                for m in 1..16usize {
                    s[m] = s[m & (m - 1)] + q[m.trailing_zeros() as usize] as f64;
                }
                s
            })
            .collect();
        let lows: Vec<f64> = sums.iter().map(|s| s.iter().copied().fold(f64::INFINITY, f64::min)).collect();
        let range = sums.iter().zip(&lows).map(|(s, lo)| s.iter().map(|v| v - lo).fold(0.0, f64::max)).fold(0.0, f64::max);
        let step = if range > 0.0 { range / 255.0 } else { 1.0 };
        let bytes = sums.iter().zip(&lows).flat_map(|(s, lo)| s.map(|v| ((v - lo) / step).round().min(255.0) as u8)).collect();
        let exact = sums.iter().flat_map(|s| s.map(|v| v as f32)).collect();
        Self { bytes, exact, step, base: lows.iter().sum(), error: step * 0.5 * sums.len() as f64 }
    }
}

/// The partitions' centroids and how a vector is assigned to one. With an anisotropy weight `eta`
/// above one, a vector goes to the centroid minimizing its residual's squared length plus
/// `eta - 1` times the residual's square along the vector itself: an error parallel to the vector
/// distorts its high inner products, the ones a search must rank, and is weighted the more.
///
/// Each partition has a current centroid version; earlier versions are kept for entries coded
/// against them. Partition 0 at version 0 with no stored centroid is the untrained origin. Only a
/// live partition is assigned vectors.
#[derive(Debug, Clone, Default)]
struct Model {
    centroids: Vec<Vec<f32>>,
    /// Each centroid's squared length.
    squares: Vec<f32>,
    /// Each partition's current version.
    versions: Vec<i64>,
    /// Superseded centroids, by partition and version.
    older: HashMap<(i64, i64), Vec<f32>>,
    /// Each partition's state, from its bookkeeping.
    states: Vec<State>,
    eta: f32,
    /// The current centroids again, in tiles: every change to `centroids` changes them.
    tiles: Tiles,
}

impl Model {
    fn new(centroids: Vec<Vec<f32>>, eta: f32) -> Self {
        let squares = centroids.iter().map(|c| dot32(c, c)).collect();
        let versions = vec![0; centroids.len()];
        let states = vec![State::Live; centroids.len()];
        let mut tiles = Tiles::default();
        for (p, c) in centroids.iter().enumerate() {
            tiles.set(p, c);
        }
        Self { centroids, squares, versions, older: HashMap::new(), states, eta, tiles }
    }

    /// Set a partition's centroid at a version, keeping the one it supersedes.
    fn place(&mut self, partition: usize, version: i64, c: Vec<f32>) {
        let square = dot32(&c, &c);
        self.tiles.set(partition, &c);
        if partition == self.centroids.len() {
            self.centroids.push(c);
            self.squares.push(square);
            self.versions.push(version);
            self.states.push(State::Live);
            return;
        }
        let previous = std::mem::replace(&mut self.centroids[partition], c);
        self.older.insert((partition as i64, self.versions[partition]), previous);
        self.squares[partition] = square;
        self.versions[partition] = version;
    }

    fn version(&self, partition: i64) -> i64 {
        self.versions.get(partition as usize).copied().unwrap_or(0)
    }

    fn state(&self, partition: i64) -> State {
        self.states.get(partition as usize).copied().unwrap_or_default()
    }

    /// The live partitions, at least one: an untrained index is its origin.
    fn live(&self) -> usize {
        self.states.iter().filter(|s| **s == State::Live).count().max(1)
    }

    /// The centroid an entry of `partition` was coded against at `version`; none for the origin.
    fn centroid(&self, partition: i64, version: i64) -> Result<Option<&[f32]>> {
        if let Some(current) = self.centroids.get(partition as usize).filter(|_| self.versions[partition as usize] == version) {
            return Ok(Some(current));
        }
        if let Some(older) = self.older.get(&(partition, version)) {
            return Ok(Some(older));
        }
        if partition == 0 && version == 0 {
            return Ok(None);
        }
        Err(*error("an entry names a centroid version the index does not hold"))
    }

    fn nearest(&self, v: &[f32]) -> usize {
        self.nearest_of(&[v])[0]
    }

    /// The nearest live partition to each of `group`'s vectors by the loss above, the first of
    /// equals: each tile's products with the whole group are taken while the tile is in cache.
    fn nearest_of(&self, group: &[&[f32]]) -> Vec<usize> {
        let lengths: Vec<(f32, f32)> = group
            .iter()
            .map(|v| {
                let square = dot32(v, v);
                (square, square.sqrt().max(f32::MIN_POSITIVE))
            })
            .collect();
        let mut best = vec![(f32::INFINITY, 0); group.len()];
        let mut products = [0.0f32; TILE];
        for (first, tile) in self.tiles.tiles() {
            let lanes = (self.centroids.len() - first).min(TILE);
            for (n, v) in group.iter().enumerate() {
                tile_products(v, tile, &mut products);
                let (square, length) = lengths[n];
                for (k, product) in products[..lanes].iter().enumerate() {
                    let i = first + k;
                    if self.states[i] != State::Live {
                        continue;
                    }
                    let mut loss = square + self.squares[i] - 2.0 * product;
                    if self.eta > 1.0 {
                        let along = length - product / length;
                        loss += (self.eta - 1.0) * along * along;
                    }
                    if loss < best[n].0 {
                        best[n] = (loss, i);
                    }
                }
            }
        }
        best.into_iter().map(|(_, i)| i).collect()
    }

    /// The partitions a query may probe, every one not retired, each with what ranks it: its
    /// distance from `q` less `q`'s squared length, or its product with `q` negated, the products
    /// taken over the centroids rounded to int8, within a few thousandths of the exact ones.
    fn ranked(&self, q: &[f32], by_product: bool) -> Vec<(f32, i64)> {
        let mut ranked = Vec::with_capacity(self.centroids.len());
        let mut products = [0.0f32; TILE];
        for (first, tile) in self.tiles.bytes() {
            tile_products_bytes(q, tile, &mut products);
            for (k, product) in products[..(self.centroids.len() - first).min(TILE)].iter().enumerate() {
                let i = first + k;
                if self.states[i] != State::Retired {
                    let product = product * self.tiles.scales[i];
                    ranked.push((if by_product { -product } else { self.squares[i] - 2.0 * product }, i as i64));
                }
            }
        }
        ranked
    }

    /// What ranks partition `p` for `q`, exactly, as [`Self::ranked`] gives it approximately.
    fn rank(&self, q: &[f32], p: usize, by_product: bool) -> f32 {
        let product = dot32(&self.centroids[p], q);
        if by_product {
            -product
        } else {
            self.squares[p] - 2.0 * product
        }
    }

    /// The `take` partitions not retired that rank nearest `q`, nearest first, so a query's bar
    /// rises soonest: every partition is ranked over the int8 centroids, and the best quarter again
    /// as many as are taken, and thirty-two, again exactly, so the rounding, a few thousandths,
    /// moves none across the cut.
    fn probes(&self, q: &[f32], take: usize, by_product: bool) -> Vec<(f32, i64)> {
        let mut ranked = self.ranked(q, by_product);
        let order = |a: &(f32, i64), b: &(f32, i64)| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1));
        if take >= ranked.len() {
            ranked.sort_unstable_by(order);
            return ranked;
        }
        let near = (take + take / 4 + 32).min(ranked.len());
        ranked.select_nth_unstable_by(near - 1, order);
        ranked.truncate(near);
        for entry in &mut ranked {
            entry.0 = self.rank(q, entry.1 as usize, by_product);
        }
        if take > 0 {
            ranked.select_nth_unstable_by(take - 1, order);
        }
        ranked.truncate(take);
        ranked.sort_unstable_by(order);
        ranked
    }

    /// Code a vector against its nearest partition's current centroid: the partition, the
    /// centroid's version and the code.
    fn encode(&self, rotation: &Rotation, rowid: i64, v: &[f32]) -> (i64, i64, Entry) {
        if self.centroids.is_empty() {
            return (0, 0, code(rotation, rowid, v.to_vec()));
        }
        self.encode_in(rotation, rowid, v, self.nearest(v))
    }

    /// Code a vector against partition `p`'s current centroid.
    fn encode_in(&self, rotation: &Rotation, rowid: i64, v: &[f32], p: usize) -> (i64, i64, Entry) {
        let residual = v.iter().zip(&self.centroids[p]).map(|(a, b)| a - b).collect();
        (p as i64, self.versions[p], code(rotation, rowid, residual))
    }
}

/// The RaBitQ code of a residual.
fn code(rotation: &Rotation, rowid: i64, residual: Vec<f32>) -> Entry {
    let norm = residual.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();
    let code_bytes = rotation.code_bytes();
    let mut bits = vec![0u8; code_bytes];
    let mut ex = vec![0u8; code_bytes * EX_BITS];
    if norm == 0.0 {
        return Entry { rowid, gain: 0.0, slack: 0.0, scale: 0.0, bits, ex };
    }
    let unit: Vec<f32> = residual.iter().map(|x| (*x as f64 / norm) as f32).collect();
    let rotated = rotation.apply(&unit);
    let codes = grid(&rotated);
    let mut absolute = 0.0f64;
    let mut refined = 0.0f64;
    for (i, (x, c)) in rotated.iter().zip(&codes).enumerate() {
        bits[i / 8] |= ((c >> EX_BITS) as u8) << (i % 8);
        // Four dimensions to three bytes.
        let at = (i / 4) * 3;
        let word = (c & ((1 << EX_BITS) - 1)) << (EX_BITS * (i % 4));
        ex[at] |= word as u8;
        ex[at + 1] |= (word >> 8) as u8;
        ex[at + 2] |= (word >> 16) as u8;
        absolute += x.abs() as f64;
        refined += *x as f64 * (*c as f64 - CENTRE);
    }
    let root = (rotation.padded as f64).sqrt();
    // The corrections: the unit residual's products with its 1-bit code, unit length, and with
    // its 7-bit code.
    let factor = (absolute / root).max(f64::MIN_POSITIVE);
    let refined = refined.max(f64::MIN_POSITIVE);
    Entry {
        rowid,
        gain: (norm / (factor * root)) as f32,
        slack: (norm * (1.0 - factor * factor).max(0.0).sqrt() / factor) as f32,
        scale: (norm / refined) as f32,
        bits,
        ex,
    }
}

fn dot(a: &[f32], b: &[f32]) -> f64 {
    dot32(a, b) as f64
}

fn blob(bytes: &[u8]) -> Result<Value> {
    Value::from_slice(bytes).map_err(|_| LimboError::InternalError("ivf_rq: allocation".into()))
}

fn integer(record: &ImmutableRecord, at: usize) -> Result<i64> {
    match record.get_value(at)? {
        ValueRef::Numeric(Numeric::Integer(n)) => Ok(n),
        _ => Err(LimboError::Corrupt("ivf_rq: expected an integer".into())),
    }
}

fn bytes_at(record: &ImmutableRecord, at: usize) -> Result<&[u8]> {
    match record.get_value(at)? {
        ValueRef::Blob(bytes) => Ok(bytes),
        _ => Err(LimboError::Corrupt("ivf_rq: expected a blob".into())),
    }
}

fn record(values: &[Value]) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(values, values.len())
}

fn blob_record(bytes: &[u8]) -> Result<ImmutableRecord> {
    record(&[blob(bytes)?])
}

/// A fresh generation: distinct across processes and rolled-back changes with near certainty.
fn generation() -> i64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos()));
    hasher.finish() as i64
}

fn centroid_record(c: &[f32]) -> Result<ImmutableRecord> {
    let bytes: Vec<u8> = c.iter().flat_map(|x| x.to_le_bytes()).collect();
    blob_record(&bytes)
}

/// A dead row's blocks: each packed block that holds a stale copy of the vector's code.
fn dead_blocks(bytes: &[u8]) -> Result<Vec<i64>> {
    if bytes.len() % 8 != 0 {
        return Err(LimboError::Corrupt("ivf_rq: dead row length is not a count of blocks".into()));
    }
    Ok(bytes.chunks_exact(8).map(|b| i64::from_le_bytes(b.try_into().unwrap())).collect())
}

fn dead_record(blocks: &[i64]) -> Result<ImmutableRecord> {
    let bytes: Vec<u8> = blocks.iter().flat_map(|b| b.to_le_bytes()).collect();
    blob_record(&bytes)
}

/// A resumable read of one row by rowid.
#[derive(Clone, Copy)]
enum Get {
    Seek,
    Read,
}

fn get_row(cursor: &mut Box<dyn CursorTrait>, rowid: i64, state: &mut Get) -> IOResultOr<Option<ImmutableRecord>> {
    #[cfg(test)]
    if matches!(*state, Get::Seek) {
        POINT_READS.with(|count| count.set(count.get() + 1));
    }
    loop {
        match *state {
            Get::Seek => match cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })? {
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
                IOResult::Done(SeekResult::Found) => *state = Get::Read,
                IOResult::Done(_) => return Ok(IOResult::Done(None)),
            },
            Get::Read => {
                let found = match cursor.record()? {
                    IOResult::Done(found) => found.ok_or_else(|| error("positioned cursor has no record"))?.clone(),
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                };
                *state = Get::Seek;
                return Ok(IOResult::Done(Some(found)));
            }
        }
    }
}

/// A resumable walk over the rows with rowids in a range.
#[derive(Clone, Copy)]
enum Walk {
    Seek,
    Next,
    Read,
}

/// The next row of the walk over `start..end`, or none once it is past the end.
/// Step a walk over the keys in `start..end` to its next row: the row's key, or none past the range.
/// With `read`, the row's record is read before the walk moves on and left held by the cursor.
fn walk(cursor: &mut Box<dyn CursorTrait>, start: i64, end: i64, state: &mut Walk, read: bool) -> IOResultOr<Option<i64>> {
    loop {
        match *state {
            Walk::Seek => {
                cursor.set_forward_bound(Some(end));
                let found = match cursor.seek(SeekKey::TableRowId(start), SeekOp::GE { eq_only: false })? {
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    IOResult::Done(found) => found,
                };
                #[cfg(test)]
                WALK_SEEKS.with(|count| count.set(count.get() + 1));
                match found {
                    SeekResult::Found => *state = Walk::Read,
                    SeekResult::TryAdvance => *state = Walk::Next,
                    SeekResult::NotFound => return Ok(IOResult::Done(None)),
                }
            }
            Walk::Next => {
                if let IOResult::IO(io) = cursor.next()? {
                    return Ok(IOResult::IO(io));
                }
                *state = Walk::Read;
            }
            Walk::Read => {
                if !cursor.has_record() {
                    return Ok(IOResult::Done(None));
                }
                let rowid = match cursor.rowid()? {
                    IOResult::Done(rowid) => rowid.ok_or_else(|| error("positioned cursor has no rowid"))?,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                };
                if rowid >= end {
                    return Ok(IOResult::Done(None));
                }
                // Read the record before moving on, so a retry after I/O reads this row again: in
                // place for a cell inside its page, assembled and kept for one that overflows it.
                if read {
                    if let IOResult::IO(io) = cursor.record_payload()? {
                        return Ok(IOResult::IO(io));
                    }
                }
                *state = Walk::Next;
                return Ok(IOResult::Done(Some(rowid)));
            }
        }
    }
}

/// The next rowid of the walk over `start..end`, its record left unread, or none past the end.
fn walk_key(cursor: &mut Box<dyn CursorTrait>, start: i64, end: i64, state: &mut Walk) -> IOResultOr<Option<i64>> {
    walk(cursor, start, end, state, false)
}

/// The next rowid of the walk over `start..end`, its record read and held by the cursor.
fn walk_held(cursor: &mut Box<dyn CursorTrait>, start: i64, end: i64, state: &mut Walk) -> IOResultOr<Option<i64>> {
    walk(cursor, start, end, state, true)
}

/// The serialized record a walk left its cursor on, for decoding in place: [`walk_held`] has read
/// it, so it is held without I/O.
fn held_payload(cursor: &mut Box<dyn CursorTrait>) -> Result<&[u8]> {
    match cursor.record_payload()? {
        IOResult::Done(Some(payload)) => Ok(payload),
        IOResult::Done(None) => Err(*error("positioned cursor has no record")),
        IOResult::IO(_) => Err(*error("a walk's record was not read before it was held")),
    }
}

/// A block or tail entry's centroid version and codes, decoded in place.
fn version_and_codes<'r>(record: &'r ImmutableRecordRef<'_>) -> Result<(i64, &'r [u8])> {
    match record.get_two_values(0, 1)? {
        (ValueRef::Numeric(Numeric::Integer(version)), ValueRef::Blob(codes)) => Ok((version, codes)),
        _ => Err(LimboError::Corrupt("ivf_rq: a block's record is not a version and codes".into())),
    }
}

/// The record a walk left its cursor on: [`walk_held`] has read it, so it is held without I/O.
fn held_record(cursor: &mut Box<dyn CursorTrait>) -> Result<&ImmutableRecord> {
    match cursor.record()? {
        IOResult::Done(Some(record)) => Ok(record),
        IOResult::Done(None) => Err(*error("positioned cursor has no record")),
        IOResult::IO(_) => Err(*error("a walk's record was not read before it was held")),
    }
}

/// [`walk_held`], with an owned copy of the row's record.
fn walk_row(cursor: &mut Box<dyn CursorTrait>, start: i64, end: i64, state: &mut Walk) -> IOResultOr<Option<(i64, ImmutableRecord)>> {
    match walk_held(cursor, start, end, state)? {
        IOResult::Done(Some(rowid)) => Ok(IOResult::Done(Some((rowid, held_record(cursor)?.clone())))),
        IOResult::Done(None) => Ok(IOResult::Done(None)),
        IOResult::IO(io) => Ok(IOResult::IO(io)),
    }
}

/// One write to one backing table, resumable.
enum Step {
    /// Write the row, replacing any with its rowid.
    Put { store: usize, rowid: i64, row: ImmutableRecord, at: Progress },
    /// Delete the row; it must exist unless `required` is false.
    Remove { store: usize, rowid: i64, required: bool, at: Progress },
}

/// How far a step has gone: its row is sought, then (when the step's prior state is kept) read,
/// then written.
enum Progress {
    Seek,
    Read,
    Write(Option<ImmutableRecord>),
}

/// Apply a step. With `undo`, the step that restores the row's prior state is pushed once the
/// write is done; a write another transaction holds fails with [`LimboError::WriteWriteConflict`]
/// and changes nothing.
fn apply_step(stores: &mut [Box<dyn CursorTrait>], step: &mut Step, undo: Option<&mut Vec<Step>>) -> IOResultOr<()> {
    let keep = undo.is_some();
    let (store, rowid) = match step {
        Step::Put { store, rowid, .. } | Step::Remove { store, rowid, .. } => (*store, *rowid),
    };
    let cursor = &mut stores[store];
    loop {
        let (at, required) = match step {
            Step::Put { at, .. } => (at, None),
            Step::Remove { at, required, .. } => (at, Some(*required)),
        };
        match at {
            Progress::Seek => match cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })? {
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
                IOResult::Done(SeekResult::Found) => *at = if keep { Progress::Read } else { Progress::Write(None) },
                IOResult::Done(_) => match required {
                    Some(true) => return Err(error("removed row is absent")),
                    Some(false) => return Ok(IOResult::Done(())),
                    None => *at = Progress::Write(None),
                },
            },
            Progress::Read => {
                let prior = match cursor.record()? {
                    IOResult::Done(found) => found.ok_or_else(|| error("positioned cursor has no record"))?.clone(),
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                };
                *at = Progress::Write(Some(prior));
            }
            Progress::Write(_) => break,
        }
    }
    let written = match step {
        Step::Put { row, .. } => cursor.insert(&BTreeKey::new_table_rowid(rowid, Some(row)))?,
        Step::Remove { .. } => cursor.delete()?,
    };
    if let IOResult::IO(io) = written {
        return Ok(IOResult::IO(io));
    }
    if let Some(undo) = undo {
        let (Step::Put { at, .. } | Step::Remove { at, .. }) = step;
        let Progress::Write(prior) = std::mem::replace(at, Progress::Seek) else {
            unreachable!("the step was written")
        };
        undo.push(match prior {
            Some(row) => put(store, rowid, row),
            None => remove(store, rowid),
        });
    }
    Ok(IOResult::Done(()))
}

fn put(store: usize, rowid: i64, row: ImmutableRecord) -> Step {
    Step::Put { store, rowid, row, at: Progress::Seek }
}

fn remove(store: usize, rowid: i64) -> Step {
    Step::Remove { store, rowid, required: true, at: Progress::Seek }
}

/// Whether an error is a write to a row another transaction holds.
fn conflicted(error: &LimboError) -> bool {
    matches!(error, LimboError::WriteWriteConflict)
}


/// The 7-bit code of a rotated unit vector: values on the half-integer grid -63.5..=63.5 at the
/// scale whose code points closest to the vector's direction. A value's sign is the vector's.
fn grid(x: &[f32]) -> Vec<u32> {
    let peak = x.iter().fold(0.0f32, |m, v| m.max(v.abs()));
    let code = |v: f32, t: f32| grid_code(v, t) as u32;
    let mut best = (f32::NEG_INFINITY, 1.0f32);
    if peak > 0.0 {
        let top = (LEVELS / 2) as f32 / peak;
        for s in 0..GRID_SCALES {
            let t = top * (0.15 + 1.35 * s as f32 / (GRID_SCALES - 1) as f32);
            let (along, length) = fit(x, t);
            let cosine = along / length.sqrt();
            if cosine > best.0 {
                best = (cosine, t);
            }
        }
    }
    x.iter().map(|v| code(*v, best.1)).collect()
}

/// Lloyd's k-means over `sample`, seeded by k-means++ for a few centroids and, as FAISS seeds its
/// training, by distinct sampled vectors for many, where k-means++ would make a pass over the
/// sample for each. Over vectors of unit length, as embeddings are, each centroid is the direction
/// of its members' mean (spherical k-means): the partitions a query probes, nearest first, are
/// then those its products with the members are highest in.
fn kmeans(sample: &[Vec<f32>], k: usize, iterations: usize, eta: f32) -> Vec<Vec<f32>> {
    let k = k.min(sample.len()).max(1);
    let spherical = sample.iter().all(|v| (dot32(v, v) - 1.0).abs() < 1e-3);
    let mut state = SEED ^ 0x5bd1_e995;
    let mut random = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    if k > SEEDED_CENTROIDS {
        // Distinct sampled vectors, by a partial shuffle of the sample's indices.
        let mut order: Vec<usize> = (0..sample.len()).collect();
        for i in 0..k {
            let j = i + (random() as usize) % (sample.len() - i);
            order.swap(i, j);
        }
        let centroids = order[..k].iter().map(|i| sample[*i].clone()).collect();
        return lloyd(sample, centroids, iterations, eta, spherical);
    }
    let mut centroids = vec![sample[(random() as usize) % sample.len()].clone()];
    let mut nearest: Vec<f32> = sample.iter().map(|v| l2(v, &centroids[0])).collect();
    while centroids.len() < k {
        let total: f64 = nearest.iter().map(|d| *d as f64).sum();
        let mut target = (random() as f64 / u64::MAX as f64) * total;
        let mut chosen = sample.len() - 1;
        for (i, d) in nearest.iter().enumerate() {
            target -= *d as f64;
            if target <= 0.0 {
                chosen = i;
                break;
            }
        }
        let c = sample[chosen].clone();
        for (v, d) in sample.iter().zip(nearest.iter_mut()) {
            *d = d.min(l2(v, &c));
        }
        centroids.push(c);
    }
    lloyd(sample, centroids, iterations, eta, spherical)
}

/// Lloyd's iterations over `sample` from `centroids`.
fn lloyd(sample: &[Vec<f32>], mut centroids: Vec<Vec<f32>>, iterations: usize, eta: f32, spherical: bool) -> Vec<Vec<f32>> {
    let (k, dims) = (centroids.len(), sample[0].len());
    for _ in 0..iterations {
        let assigned = nearest_all(&Model::new(centroids.clone(), eta), sample);
        let mut sums = vec![vec![0.0f64; dims]; k];
        let mut counts = vec![0usize; k];
        for (v, p) in sample.iter().zip(assigned) {
            counts[p] += 1;
            sums[p].iter_mut().zip(v).for_each(|(s, x)| *s += *x as f64);
        }
        for p in 0..k {
            if counts[p] == 0 {
                continue;
            }
            let length = match spherical {
                true => sums[p].iter().map(|x| x * x).sum::<f64>().sqrt().max(f64::MIN_POSITIVE),
                false => counts[p] as f64,
            };
            for d in 0..dims {
                centroids[p][d] = (sums[p][d] / length) as f32;
            }
        }
    }
    centroids
}

/// Each vector's nearest live partition, the vectors split over the cores when there are enough
/// products to pay for the threads.
fn nearest_all(model: &Model, vectors: &[Vec<f32>]) -> Vec<usize> {
    let nearest = |part: &[Vec<f32>]| nearest_all_in(model, part);
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    if threads == 1 || vectors.len() * model.centroids.len() < PARALLEL_PRODUCTS {
        return nearest(vectors);
    }
    let chunk = vectors.len().div_ceil(threads);
    std::thread::scope(|scope| {
        vectors
            .chunks(chunk)
            .map(|part| scope.spawn(move || nearest(part)))
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|h| h.join().expect("assignment worker"))
            .collect()
    })
}

/// Each vector's nearest live partition on this thread, [`NEAREST_GROUP`] vectors at a time.
fn nearest_all_in(model: &Model, vectors: &[Vec<f32>]) -> Vec<usize> {
    vectors.chunks(NEAREST_GROUP).flat_map(|group| model.nearest_of(&group.iter().map(Vec::as_slice).collect::<Vec<_>>())).collect()
}

/// Each vector coded against its nearest live partition, as [`Model::encode`] does, the vectors
/// split over the cores when there are enough products to pay for the threads.
fn encode_all(model: &Model, rotation: &Rotation, rowids: &[i64], vectors: &[Vec<f32>]) -> Vec<(i64, i64, Entry)> {
    let code = |rowids: &[i64], vectors: &[Vec<f32>]| -> Vec<(i64, i64, Entry)> {
        if model.centroids.is_empty() {
            return rowids.iter().zip(vectors).map(|(rowid, v)| model.encode(rotation, *rowid, v)).collect();
        }
        let nearest = nearest_all_in(model, vectors);
        rowids.iter().zip(vectors).zip(nearest).map(|((rowid, v), p)| model.encode_in(rotation, *rowid, v, p)).collect()
    };
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    if threads == 1 || vectors.len() * model.centroids.len().max(1) < PARALLEL_PRODUCTS {
        return code(rowids, vectors);
    }
    let chunk = vectors.len().div_ceil(threads);
    std::thread::scope(|scope| {
        rowids
            .chunks(chunk)
            .zip(vectors.chunks(chunk))
            .map(|(rowids, vectors)| scope.spawn(move || code(rowids, vectors)))
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|h| h.join().expect("coding worker"))
            .collect()
    })
}

/// The centroids 2-means splits `vectors` into, unless the smaller half would take under a
/// sixteenth of them: a split that lopsided leaves the partition about as large as it was.
fn halves(vectors: &[Vec<f32>], eta: f32) -> Option<[Vec<f32>; 2]> {
    if vectors.len() < 2 {
        return None;
    }
    let model = Model::new(kmeans(vectors, 2, SPLIT_ITERATIONS, eta), eta);
    let first = nearest_all(&model, vectors).iter().filter(|p| **p == 0).count();
    if first.min(vectors.len() - first) * 16 < vectors.len() {
        return None;
    }
    <[Vec<f32>; 2]>::try_from(model.centroids).ok()
}

/// The size past which a partition of an index with `live` live partitions is split: twice the
/// size of a partition when the partitions number [`PARTITIONS_PER_ROOT`] times the square root of
/// the row count, `live` over its square, and at least two blocks.
fn split_bound(live: usize) -> i64 {
    (2 * live / (PARTITIONS_PER_ROOT * PARTITIONS_PER_ROOT)).max(2 * BLOCK) as i64
}

/// Balance trained `centroids` by the sample's assignment, each sampled vector standing for
/// `vectors / sample.len()` of the table's: split every partition the sample puts at three
/// quarters of the split bound or more by 2-means over its sampled members, and drop every one it
/// puts under the retirement bound, a sixteenth of the split bound, its members going to their
/// next nearest, until a round changes nothing. Maintenance then has no split or retirement to
/// start a rebuilt index with. The margin under the bound is two standard errors of the share of a
/// partition at the bound when 32 vectors are sampled per partition.
fn balance(sample: &[Vec<f32>], mut centroids: Vec<Vec<f32>>, vectors: usize, eta: f32) -> Vec<Vec<f32>> {
    let scale = vectors as f64 / sample.len().max(1) as f64;
    // A partition whose split was refused as lopsided is not tried again.
    let mut whole = vec![false; centroids.len()];
    for _ in 0..BALANCE_ROUNDS {
        let mut members: Vec<Vec<usize>> = vec![Vec::new(); centroids.len()];
        for (i, p) in nearest_all(&Model::new(centroids.clone(), eta), sample).into_iter().enumerate() {
            members[p].push(i);
        }
        let bound = split_bound(centroids.len()) as f64;
        let mut left = centroids.len();
        let mut kept = Vec::with_capacity(centroids.len());
        let mut kept_whole = Vec::with_capacity(centroids.len());
        let mut changed = false;
        for ((centroid, members), whole) in centroids.into_iter().zip(members).zip(whole) {
            let share = members.len() as f64 * scale;
            if share * 16.0 < bound && left > 1 {
                left -= 1;
                changed = true;
                continue;
            }
            if !whole && share * 4.0 >= bound * 3.0 {
                let sampled: Vec<Vec<f32>> = members.iter().map(|i| sample[*i].clone()).collect();
                if let Some(halves) = halves(&sampled, eta) {
                    kept.extend(halves);
                    kept_whole.extend([false, false]);
                    changed = true;
                    continue;
                }
                kept.push(centroid);
                kept_whole.push(true);
                continue;
            }
            kept.push(centroid);
            kept_whole.push(whole);
        }
        centroids = kept;
        whole = kept_whole;
        if !changed {
            break;
        }
    }
    centroids
}

/// The value of a literal LIMIT, for a filtered query's cost estimate.
fn literal_rows(expr: &ast::Expr) -> Option<f64> {
    match crate::util::parse_signed_number(expr).ok()? {
        Value::Numeric(Numeric::Integer(n)) if n >= 0 => Some(n as f64),
        _ => None,
    }
}

fn dense(value: Option<&[u8]>) -> Result<Vec<f32>> {
    let blob = value.ok_or_else(|| LimboError::InternalError("ivf_rq: value must be a vector blob".into()))?;
    let vector = Vector::from_slice_owned(blob)?;
    if !matches!(vector.vector_type, VectorType::Float32Dense) {
        return Err(LimboError::InternalError("ivf_rq: value must be a dense f32 vector".into()));
    }
    Ok(vector.as_f32_slice().to_vec())
}

#[derive(Debug)]
pub struct IvfRqIndexMethod;

#[derive(Debug)]
pub struct IvfRqIndexMethodAttachment {
    configuration: IndexMethodConfiguration,
    patterns: Vec<ast::Select>,
    shared: Shared,
}

impl IndexMethod for IvfRqIndexMethod {
    fn attach(&self, configuration: &IndexMethodConfiguration) -> Result<Arc<dyn IndexMethodAttachment>> {
        let column = &configuration.columns[0].name;
        let table = &configuration.table_name;
        let budget = match configuration.parameters.get("cache_mib") {
            Some(Value::Numeric(Numeric::Integer(n))) if *n >= 0 => *n as usize,
            _ => DEFAULT_CACHE_MIB,
        };
        let cache = PartitionCache { budget: budget.saturating_mul(1 << 20), ..PartitionCache::default() };
        Ok(Arc::new(IvfRqIndexMethodAttachment {
            configuration: configuration.clone(),
            shared: Arc::new(std::sync::Mutex::new(SharedState { cache, ..SharedState::default() })),
            patterns: parse_patterns(&[
                &format!("SELECT vector_distance_dot({column}, ?) as distance FROM {table} ORDER BY distance LIMIT ?"),
                &format!("SELECT vector_distance_dot(?, {column}) as distance FROM {table} ORDER BY distance LIMIT ?"),
            ])?,
        }))
    }
}

impl IndexMethodAttachment for IvfRqIndexMethodAttachment {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a> {
        IndexMethodDefinition {
            method_name: IVF_RQ_INDEX_METHOD_NAME,
            table_name: &self.configuration.table_name,
            index_name: &self.configuration.index_name,
            patterns: self.patterns.as_slice(),
            backing_btree: false,
            results_materialized: true,
            filtered_queries: true,
            mvcc_support: super::IndexMethodMvccSupport::TransactionalBackingStore,
        }
    }
    fn init(&self) -> Result<Box<dyn IndexMethodCursor>> {
        Ok(Box::new(IvfRqCursor::new(self.configuration.clone(), self.shared.clone())))
    }
    fn backing_schema(&self) -> BackingSchema {
        backing_schema(&self.configuration)
    }
}

/// The index's backing tables, one per store, named after the index.
fn backing_schema(configuration: &IndexMethodConfiguration) -> BackingSchema {
    let tables = STORES
        .iter()
        .map(|(suffix, columns)| {
            BackingTable::new(
                format!("{}_{suffix}", configuration.index_name),
                columns.iter().map(|(name, kind)| BackingColumn::new(*name, *kind)).collect(),
            )
        })
        .collect();
    BackingSchema::new(tables, Vec::new())
}

/// What the index's cursors share: the model last loaded or written, under its generation; the
/// partitions known to be moving, which any writer's maintenance moves along; those a run left work
/// due in, its jobs spent, which the next statements' maintenance surveys beside its own; the held
/// partitions; and the index's [`Writes`], under its database's incarnation.
#[derive(Default, Debug)]
struct SharedState {
    model: Option<(i64, Arc<Model>)>,
    moving: BTreeSet<i64>,
    due: BTreeSet<i64>,
    cache: PartitionCache,
    writes: Option<(u64, SharedWrites)>,
}

/// The blocks of partitions as queries read them, each partition's under the version of its
/// bookkeeping it was read at, within a byte budget, the least recently probed giving way first.
/// Until a partition first has to give way every partition read is held; from then on, one is
/// held only when queries have lately probed it more often than the partition it would push out,
/// so probes spread evenly over more than the budget holds copy nothing in vain, and the partitions
/// queries keep coming back to stay.
/// A version is drawn afresh by each write of the bookkeeping, which every change to a partition's
/// blocks makes in its own transaction, so a partition's blocks are the same for every snapshot
/// that holds a version, and a version a rolled-back write drew is never seen again.
#[derive(Default, Debug)]
struct PartitionCache {
    budget: usize,
    bytes: usize,
    tick: u64,
    entries: HashMap<(i64, i64), (Arc<HeldBlocks>, u64)>,
    order: BTreeMap<u64, (i64, i64)>,
    /// Whether a partition has had to give way, and how often each partition was probed lately:
    /// the counts halve whenever the probes counted reach ten times the partitions held.
    full: bool,
    probed: HashMap<(i64, i64), u32>,
    counted: usize,
    /// Each held partition's rows but its blocks, which come and go with the blocks they were read
    /// with.
    rows: HashMap<i64, Arc<HeldRows>>,
}

/// A partition's blocks: each one's key, centroid version and the range of its codes in `codes`.
#[derive(Default, Debug)]
struct HeldBlocks {
    blocks: Vec<(i64, i64, std::ops::Range<usize>)>,
    codes: Vec<u8>,
}

impl HeldBlocks {
    fn push(&mut self, key: i64, version: i64, codes: &[u8]) {
        let at = self.codes.len();
        self.codes.extend_from_slice(codes);
        self.blocks.push((key, version, at..self.codes.len()));
    }

    /// Start the start of each block, for codes of `dims` padded dimensions, on its way to the
    /// cache.
    fn prefetch(&self, dims: usize) {
        let ahead = held_prefetch(dims);
        for (_, _, codes) in &self.blocks {
            let start = &self.codes[codes.clone()];
            prefetch(&start[..start.len().min(ahead)]);
        }
    }
}

/// A partition's rows but its blocks as a probe read them under `snapshot`: its bookkeeping's
/// version, the stale copies its dead rows name, as rowid and block, and its tail entries, each
/// one's key, centroid version and the range of its codes in `codes`. They serve a probe under
/// another snapshot when [`Writes::serves`] finds that the two see the same rows of the partition.
#[derive(Debug)]
struct HeldRows {
    snapshot: Position,
    version: i64,
    dead: Vec<(i64, i64)>,
    tail: Vec<(i64, i64, std::ops::Range<usize>)>,
    codes: Vec<u8>,
}

impl HeldRows {
    fn new(snapshot: Position, version: i64) -> Self {
        HeldRows { snapshot, version, dead: Vec::new(), tail: Vec::new(), codes: Vec::new() }
    }

    fn push_tail(&mut self, key: i64, version: i64, codes: &[u8]) {
        let at = self.codes.len();
        self.codes.extend_from_slice(codes);
        self.tail.push((key, version, at..self.codes.len()));
    }

    /// The bytes they take, counted against the budget with the blocks.
    fn bytes(&self) -> usize {
        self.codes.len() + 16 * self.dead.len() + 32 * self.tail.len()
    }
}

impl PartitionCache {
    fn budget(&self) -> usize {
        match CACHE_BYTES_OVERRIDE.load(Ordering::Relaxed) {
            usize::MAX => self.budget,
            bytes => bytes,
        }
    }

    /// Count a probe of `partition` under `version`: how often it was probed lately.
    fn probe(&mut self, partition: i64, version: i64) -> u32 {
        self.counted += 1;
        if self.counted >= 10 * self.entries.len().max(64) {
            self.counted = 0;
            self.probed.retain(|_, count| {
                *count /= 2;
                *count > 0
            });
        }
        let count = self.probed.entry((partition, version)).or_default();
        *count = count.saturating_add(1);
        *count
    }

    /// The blocks held for `partition` under `version`, now the most recently probed.
    fn get(&mut self, partition: i64, version: i64) -> Option<Arc<HeldBlocks>> {
        self.probe(partition, version);
        let (blocks, tick) = self.entries.get_mut(&(partition, version))?;
        self.order.remove(tick);
        self.tick += 1;
        *tick = self.tick;
        self.order.insert(self.tick, (partition, version));
        Some(blocks.clone())
    }

    /// Whether blocks about to be read for `partition` under `version`, which [`Self::get`] did not
    /// hold, are to be held: until a partition first had to give way, or when this one was probed
    /// lately more often than the least recently probed held partition.
    fn admits(&mut self, partition: i64, version: i64) -> bool {
        if self.budget() == 0 {
            return false;
        }
        let count = self.probed.get(&(partition, version)).copied().unwrap_or(0);
        let victim = self.order.first_key_value().and_then(|(_, oldest)| self.probed.get(oldest)).copied().unwrap_or(0);
        !self.full || count > victim
    }

    /// Hold `blocks` for `partition` under `version`, letting the least recently probed go until
    /// they fit; blocks larger than the whole budget are not held.
    fn insert(&mut self, partition: i64, version: i64, blocks: HeldBlocks) {
        let size = blocks.codes.len();
        if size > self.budget() || self.entries.contains_key(&(partition, version)) {
            return;
        }
        self.make_room(size);
        self.tick += 1;
        self.bytes += size;
        self.order.insert(self.tick, (partition, version));
        self.entries.insert((partition, version), (Arc::new(blocks), self.tick));
    }

    /// Hold `rows` for `partition` beside its blocks held under their version, in place of any held
    /// before; none are held without those blocks.
    fn hold(&mut self, partition: i64, rows: HeldRows) {
        if let Some(old) = self.rows.remove(&partition) {
            self.bytes -= old.bytes();
        }
        let size = rows.bytes();
        if size > self.budget() || !self.entries.contains_key(&(partition, rows.version)) {
            return;
        }
        self.make_room(size);
        if self.entries.contains_key(&(partition, rows.version)) {
            self.bytes += size;
            self.rows.insert(partition, Arc::new(rows));
        }
    }

    /// Let the least recently probed partitions go, with their rows, until `size` more bytes fit.
    fn make_room(&mut self, size: usize) {
        let budget = self.budget();
        while self.bytes + size > budget {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            self.full = true;
            if let Some((evicted, _)) = self.entries.remove(&oldest) {
                self.bytes -= evicted.codes.len();
            }
            if self.rows.get(&oldest.0).is_some_and(|rows| rows.version == oldest.1) {
                let rows = self.rows.remove(&oldest.0).expect("rows held");
                self.bytes -= rows.bytes();
            }
        }
    }
}

/// What a probe knows of a partition's blocks once its bookkeeping is read: held, so they are
/// scored from memory; to be read from pages and held under the version; or neither, when the
/// partition has no bookkeeping or the budget is none.
enum Blocks {
    Unread,
    Held(Arc<HeldBlocks>),
    Holding(HeldBlocks, i64),
}

type Shared = Arc<std::sync::Mutex<SharedState>>;

/// A snapshot's place in commit order, compared as a pair: `(checkpoint sequence, frame)` under
/// WAL, and `(MVCC_POSITIONS, begin timestamp)` under MVCC, past every WAL position, as a database
/// switched to MVCC sees every commit made under WAL. A snapshot sees every commit at or before it.
type Position = (u64, u64);
const MVCC_POSITIONS: u64 = 1 << 32;

/// A snapshot's position, unless the snapshot names none: no WAL, or an MVCC transaction the store
/// does not track, whose cursor still records its writes.
fn position(snapshot: IndexMethodSnapshotIdentity) -> Option<Position> {
    match snapshot {
        IndexMethodSnapshotIdentity::Wal { checkpoint_sequence: u32::MAX, .. } => None,
        IndexMethodSnapshotIdentity::Wal { checkpoint_sequence, max_frame } => Some((checkpoint_sequence as u64, max_frame)),
        IndexMethodSnapshotIdentity::Mvcc { begin_timestamp: u64::MAX, .. } => None,
        IndexMethodSnapshotIdentity::Mvcc { begin_timestamp, .. } => Some((MVCC_POSITIONS, begin_timestamp)),
    }
}

/// What every attachment of one index in this process knows of the transactions writing it, so the
/// rows a probe held under one snapshot can serve another. A transaction is entered in `open`
/// before its first write to the partitions table reaches a page, and taken off when it ends, each
/// partition it wrote then raised in `changed` to the commit position. Its commit is visible before
/// that, and a snapshot can't tell whether a write it sees was committed before it or after, so
/// rows held under one snapshot serve another only while no writer is open and both are at or past
/// the partition's `changed` position: every commit that wrote it precedes both, and a writer that
/// begins after the check commits too late for either to see.
#[derive(Debug)]
struct Writes {
    open: HashMap<usize, Writer>,
    changed: HashMap<i64, Position>,
    /// The commit position when the record began, standing for every partition not in `changed`:
    /// commits before it went unrecorded.
    floor: Position,
}

/// A transaction writing the index, entered under its connection: the partitions it wrote, its
/// statements writing the index now, and whether one of them staged its writes.
#[derive(Debug)]
struct Writer {
    connection: std::sync::Weak<Connection>,
    partitions: HashSet<i64>,
    statements: usize,
    staged: bool,
}

impl Writes {
    fn changed(&self, partition: i64) -> Position {
        self.changed.get(&partition).copied().unwrap_or(self.floor)
    }

    fn raise(&mut self, partitions: HashSet<i64>, to: Position) {
        for partition in partitions {
            let changed = self.changed(partition).max(to);
            self.changed.insert(partition, changed);
        }
    }

    /// Take off the writers whose connection is gone, their transaction ended unheard: their
    /// partitions are raised to `now`, past any commit they made.
    fn settle(&mut self, now: impl FnOnce() -> Position) {
        if self.open.values().all(|writer| writer.connection.strong_count() > 0) {
            return;
        }
        let now = now();
        let gone: Vec<usize> = self.open.iter().filter(|(_, writer)| writer.connection.strong_count() == 0).map(|(key, _)| *key).collect();
        for key in gone {
            if let Some(writer) = self.open.remove(&key) {
                self.raise(writer.partitions, now);
            }
        }
    }

    /// Whether rows of `partition` held under `held` serve a probe under `snapshot`.
    fn serves(&mut self, partition: i64, held: Position, snapshot: Position, now: impl FnOnce() -> Position) -> bool {
        self.settle(now);
        let changed = self.changed(partition);
        self.open.is_empty() && held >= changed && snapshot >= changed
    }

    /// Whether rows of `partition` read under `snapshot` may be held: no writer is open, so none of
    /// them is a writer's own, and the snapshot is at or past the partition's last commit, so they
    /// can serve another.
    fn holds(&mut self, partition: i64, snapshot: Position, now: impl FnOnce() -> Position) -> bool {
        self.settle(now);
        self.open.is_empty() && snapshot >= self.changed(partition)
    }
}

type SharedWrites = std::sync::Arc<std::sync::Mutex<Writes>>;

/// Each index's [`Writes`], by its database's incarnation and its name: a schema read afresh
/// attaches the index again, and the new attachment's cursors must see the old one's writers.
static WRITES: std::sync::LazyLock<std::sync::Mutex<HashMap<(u64, String), std::sync::Weak<std::sync::Mutex<Writes>>>>> = std::sync::LazyLock::new(Default::default);

/// Where a cursor reads the latest commit position: its database's WAL, or its MVCC store.
enum Clock {
    Wal(Arc<dyn Wal>),
    Mvcc(Arc<MvStore>),
}

impl Clock {
    /// A position at or past every commit made so far: the WAL's last committed frame, read before
    /// its checkpoint sequence, so a restart between the two can only make it later; or a timestamp
    /// drawn now, past every commit's.
    fn now(&self) -> Position {
        match self {
            Clock::Wal(wal) => {
                let frame = wal.get_max_frame_in_wal();
                (wal.get_checkpoint_seq() as u64, frame)
            }
            Clock::Mvcc(store) => (MVCC_POSITIONS, store.get_begin_timestamp()),
        }
    }
}

/// How a cursor records its writes in the index's [`Writes`] and its queries use and hold
/// partitions' rows across snapshots: its clock, its connection, which keys its transaction's
/// writes, its snapshot, unless that names no position, and whether rows it reads may be held. A
/// WAL writer's snapshot counts the frames it has written, which a rollback hands to later commits,
/// so rows it reads are not held.
struct Sharing {
    writes: SharedWrites,
    clock: Clock,
    connection: std::sync::Weak<Connection>,
    snapshot: Option<Position>,
    holds: bool,
}

impl Sharing {
    fn key(&self) -> usize {
        self.connection.as_ptr() as usize
    }
}

enum Load {
    /// Read the generation: a model cached under it is the stored model, so nothing else is read.
    Generation(Get),
    /// Read the centroids, then each partition's state.
    Walk(Walk),
    Parts { partition: usize, get: Get },
}

/// A delete in progress: read the vector's row, then remove it and its tail entry, or name its
/// packed block in its dead row.
enum Change {
    Idle,
    Remove { rowid: i64, get: Get },
    Bury { rowid: i64, block: i64, get: Get },
    Apply(VecDeque<Step>),
}

#[derive(Clone, Copy)]
enum Stage {
    Blocks,
    Tail,
}

enum Search {
    Init,
    /// Read a partition's rows in key order, up to `end`: its bookkeeping, whose version says
    /// whether its blocks are held; its dead rows, so its stale copies are known before its blocks
    /// are scored; its tail entries; and its blocks, unless they are held. Where its blocks are to
    /// be held, its other rows are kept in `rows`, to be held beside them.
    Probe { probes: VecDeque<i64>, partition: i64, walk: Walk, end: i64, blocks: Blocks, rows: Option<HeldRows> },
    Rerank { candidates: VecDeque<i64>, rowid: Option<i64> },
}

/// An entry a pack read: where it was, the centroid version it was coded against, its code.
struct Tailed {
    location: Location,
    version: i64,
    entry: Entry,
}

/// What maintenance knows of a partition's blocks: the next sequence, the packed entries, how many
/// the last block holds when it is not full (zero when it is), the blocks still to check for
/// entries now nearer another partition (sequences `move_next..move_end`, set by a split of this
/// partition or of a neighbour, and by a retirement), the partition's [`State`], and the size at
/// which a split refused as lopsided is tried again (zero when none was).
#[derive(Clone, Copy, Default, Debug)]
struct Part {
    next: i64,
    size: i64,
    open: i64,
    move_next: i64,
    move_end: i64,
    state: State,
    retry: i64,
    /// Drawn afresh by each write of the bookkeeping, which every job that changes the partition's
    /// blocks makes: blocks read under one version are the partition's while the row holds it.
    version: i64,
}

/// A partition is live, retiring (its entries move to the live partitions nearest them, and none
/// is coded into it, while queries still probe it) or retired (empty and never probed, its slot
/// free for the next split).
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
enum State {
    #[default]
    Live,
    Retiring,
    Retired,
}

impl State {
    fn code(self) -> i64 {
        match self {
            State::Live => 0,
            State::Retiring => 1,
            State::Retired => 2,
        }
    }

    fn decode(code: i64) -> Result<Self> {
        match code {
            0 => Ok(State::Live),
            1 => Ok(State::Retiring),
            2 => Ok(State::Retired),
            _ => Err(LimboError::Corrupt("ivf_rq: unknown partition state".into())),
        }
    }
}

impl Part {
    fn record(&self) -> Result<ImmutableRecord> {
        record(&[self.next, self.size, self.open, self.move_next, self.move_end, self.state.code(), self.retry, self.version].map(Value::from_i64))
    }

    fn parse(found: &ImmutableRecord) -> Result<Self> {
        let field = |at| integer(found, at);
        Ok(Part { next: field(0)?, size: field(1)?, open: field(2)?, move_next: field(3)?, move_end: field(4)?, state: State::decode(field(5)?)?, retry: field(6)?, version: field(7)? })
    }

    fn moving(&self) -> bool {
        self.move_next < self.move_end
    }
}

type Parts = HashMap<i64, Part>;

/// One maintenance job. Each touches a bounded number of rows, except a rebuild, which recodes the
/// whole index once it holds four times the vectors its partitions were trained for.
#[derive(Clone, Copy, Debug)]
enum Job {
    /// Merge up to [`MOVE_BATCH`] tail entries into the partition's open block.
    Pack(i64),
    /// Choose the partition's two new centroids from a sample of its members and write them; its
    /// entries and those of the partitions nearest it move later, block by block.
    Split(i64),
    /// Check a block of a partition marked moving: an entry now nearer another live partition
    /// moves there, and the rest are recoded against the partition's current centroid.
    Move(i64),
    /// Rewrite the block with this key without the stale copies it holds.
    Compact(i64),
    /// Start retiring a partition too small to keep, or retire one a retirement has emptied.
    Retire(i64),
    /// Train the partitions afresh by k-means over a sample of every vector, and recode them all.
    Rebuild,
}

/// What a survey read of the partitions maintenance chooses from: their tails, bookkeeping and the
/// stale copies their blocks hold. A statement's maintenance reads the partitions it wrote, those
/// known to be moving and those earlier runs left work due in; `OPTIMIZE INDEX` reads them all.
#[derive(Default)]
struct Survey {
    tails: HashMap<i64, usize>,
    parts: Parts,
    /// Stale copies per block, from the dead rows, in key order: a partition's blocks are a range.
    stale: BTreeMap<i64, usize>,
    /// The partitions read.
    read: BTreeSet<i64>,
    /// The vectors the index holds, when the survey read them all.
    vectors: Option<i64>,
}

impl Survey {
    fn forget(&mut self, partition: i64) {
        self.tails.remove(&partition);
        self.parts.remove(&partition);
        let blocks: Vec<i64> = self.stale_in(partition).map(|(block, _)| block).collect();
        for block in blocks {
            self.stale.remove(&block);
        }
    }

    /// The stale copies each of a partition's blocks holds.
    fn stale_in(&self, partition: i64) -> impl Iterator<Item = (i64, usize)> + '_ {
        let start = partition << PARTITION_SHIFT | BLOCK_ROW << ROWID_BITS;
        self.stale.range(start..(partition + 1) << PARTITION_SHIFT).map(|(block, count)| (*block, *count))
    }

    fn part(&self, partition: i64) -> Part {
        self.parts.get(&partition).copied().unwrap_or_default()
    }

    fn tail(&self, partition: i64) -> usize {
        self.tails.get(&partition).copied().unwrap_or(0)
    }

    /// The entries a partition holds that are live: packed and tail entries, less stale copies.
    fn live(&self, partition: i64) -> i64 {
        let stale: usize = self.stale_in(partition).map(|(_, count)| count).sum();
        self.part(partition).size + self.tail(partition) as i64 - stale as i64
    }
}

/// Where a survey is: the partitions still to read, and how far into the current one. Each
/// partition's bookkeeping, dead rows and tail are read; its blocks are not.
enum Surveying {
    /// Read every partition of the model, from `partition` on.
    All { partition: i64, walk: Walk },
    /// Read one partition at a time.
    Some { pending: VecDeque<i64>, walk: Walk },
}

/// A maintenance run: the jobs it may still do, what its surveys found, and where it starts.
struct Run {
    budget: usize,
    survey: Survey,
    /// Whether the run surveys every partition.
    all: bool,
    /// The partitions a run that does not survey them all starts from.
    first: Vec<i64>,
    /// The rows a CREATE INDEX statement gave the index, which the run starts by rebuilding over.
    build: Option<Vec<i64>>,
}

/// Maintenance: survey, choose a job, read the entries it concerns, check each against its
/// vector's row, read the vectors it recodes, write, and survey again what the job changed, until
/// no job is left or the run's budget is spent. A job's first write claims it (the bookkeeping row
/// of its partition, or the generation); a write another transaction holds stops the job, and the
/// writes it made are reverted, so a run never commits half a job.
enum Upkeep {
    Idle,
    Load,
    Survey(Surveying),
    /// Read the entries of `job`: the open block and the tail for a pack, every member for a
    /// split's sample, a block for a move or a compaction, every vector for a rebuild.
    Read { job: Job, part: Part, stage: Stage, walk: Walk, get: Get, rows: Vec<Tailed> },
    /// Read the bookkeeping of the live partitions nearest one being split, which the split marks
    /// moving, so their entries now nearer a new centroid move to it.
    Neighbours { job: Job, part: Part, rows: Vec<Tailed>, members: i64, neighbours: Vec<i64>, at: usize, get: Get, parts: Vec<(i64, Part)> },
    Check { job: Job, part: Part, rows: Vec<Tailed>, at: usize, get: Get, sorted: Sorted },
    /// Read the dead row of each stale copy a rewritten block drops, to take the block off it.
    Unbury { job: Job, part: Part, sorted: Sorted, at: usize, get: Get },
    Fetch { job: Job, part: Part, sorted: Sorted, vectors: Vec<Vec<f32>> },
    /// Write the job's steps in order, keeping each row's prior state until the job is done.
    Apply { job: Job, steps: VecDeque<Step>, undo: Vec<Step> },
    /// Restore the rows the abandoned job wrote, latest first.
    Revert { undo: Vec<Step> },
    /// A rebuild, writing `steps` before it goes on to its next phase.
    Rebuild { phase: Rebuilding, steps: VecDeque<Step> },
}

/// A rebuild: read every vector's rowid, train partitions afresh by k-means over a sample of the
/// vectors, remove every block, tail entry and dead row, recode every vector into the trained
/// partitions, writing each partition's blocks as they fill, and write the bookkeeping, the
/// centroids and the generation. Centroid rows are never removed, so an entry a concurrent writer
/// coded against an older model still scores; packs recode it.
enum Rebuilding {
    Rowids { walk: Walk, rowids: Vec<i64> },
    Sample { rowids: Vec<i64>, sample: Vec<i64>, vectors: Vec<Vec<f32>>, get: Get },
    /// Collect the keys of every block, tail entry and dead row, to remove.
    Clear { rowids: Vec<i64>, model: Box<Model>, walk: Walk, keys: Vec<i64> },
    /// Read the vectors from `at` on a batch at a time, code each batch over the cores, and write
    /// each partition's blocks as they fill.
    Code { rowids: Vec<i64>, model: Box<Model>, at: usize, get: Get, stream: Stream, batch: Vec<Vec<f32>> },
    /// Every write is made: the model under its new generation is the index's.
    Done { model: Box<Model>, generation: i64 },
}

/// A rebuild's first phase over `rowids`, every vector of the index: reading the vectors of every
/// n-th, n spreading the sample over the whole table.
fn sampling(rowids: Vec<i64>) -> Rebuilding {
    let wanted = (TRAIN_PER_PARTITION * trained_partitions(rowids.len())).min(TRAIN_SAMPLE_MAX).min(rowids.len()).max(1);
    let sample = rowids.iter().step_by((rowids.len() / wanted).max(1)).take(wanted).copied().collect();
    Rebuilding::Sample { rowids, sample, vectors: Vec::new(), get: Get::Seek }
}

/// Codes written into packed blocks as they come: a partition's block is written once full, and
/// what is left of it becomes its open block at the end.
#[derive(Default)]
struct Stream {
    open: HashMap<i64, Vec<Entry>>,
    parts: HashMap<i64, Part>,
}

impl Stream {
    /// Add an entry coded against `version` of `partition`, writing the block it fills.
    fn add(&mut self, partition: i64, version: i64, entry: Entry, steps: &mut VecDeque<Step>) -> Result<()> {
        let open = self.open.entry(partition).or_default();
        open.push(entry);
        if open.len() == BLOCK {
            let entries = std::mem::take(open);
            self.write(partition, version, entries, steps)?;
        }
        Ok(())
    }

    fn write(&mut self, partition: i64, version: i64, entries: Vec<Entry>, steps: &mut VecDeque<Step>) -> Result<()> {
        let part = self.parts.entry(partition).or_default();
        let key = block_key(partition, part.next)?;
        steps.push_back(put(PARTITIONS, key, record(&[Value::from_i64(version), blob(&pack(&entries)?)?])?));
        for e in &entries {
            steps.push_back(put(VECTORS, e.rowid, record(&[Value::from_i64(Location::Block(key).encode())])?));
        }
        part.next += 1;
        part.size += entries.len() as i64;
        part.open = if entries.len() < BLOCK { entries.len() as i64 } else { 0 };
        Ok(())
    }

    /// Write each partition's open block and the bookkeeping of every partition of `model`, in the
    /// state the model gives it: those it trained are live, the slots past them retired.
    fn finish(mut self, model: &Model, steps: &mut VecDeque<Step>) -> Result<()> {
        let mut open: Vec<(i64, Vec<Entry>)> = self.open.drain().filter(|(_, entries)| !entries.is_empty()).collect();
        open.sort_unstable_by_key(|(p, _)| *p);
        for (p, entries) in open {
            self.write(p, model.version(p), entries, steps)?;
        }
        for p in 0..model.centroids.len() as i64 {
            let mut part = self.parts.get(&p).copied().unwrap_or_default();
            part.state = model.state(p);
            steps.push_back(part_step(p, part)?);
        }
        Ok(())
    }
}

/// The partitions trained for `vectors` vectors: [`PARTITIONS_PER_ROOT`] times their square root,
/// none holding under a block of them, so a partition holds about half the bound past which one is
/// split.
fn trained_partitions(vectors: usize) -> usize {
    ((PARTITIONS_PER_ROOT as f64 * (vectors as f64).sqrt()).round() as usize).min(vectors / BLOCK).max(1)
}

/// A job's entries, sorted by what their check found.
#[derive(Default)]
struct Sorted {
    /// Current.
    live: Vec<Entry>,
    /// To be recoded from the table's vector.
    recode: Vec<i64>,
    /// Every tail entry and block the job read and replaces.
    removed: Vec<Location>,
    /// Entries read from blocks, live or stale.
    from_blocks: i64,
    /// The centroid version of the block a move read.
    version: i64,
    /// Live entries a pack read from the open block, with that block's key.
    kept: Vec<(i64, i64)>,
    /// Stale copies dropped from a block the job rewrites, as rowid and block.
    buried: Vec<(i64, i64)>,
    /// The writes that take those blocks off the copies' dead rows.
    unbury: Vec<Step>,
    /// The partitions a split marks moving, with their bookkeeping.
    neighbours: Vec<(i64, Part)>,
    /// The members of a partition being split.
    members: i64,
}

/// Members a split samples for its 2-means.
const SPLIT_SAMPLE: usize = 256;
/// The most entries a pack or move rewrites in one run.
const MOVE_BATCH: usize = 64;
/// The live partitions nearest a partition being split that the split marks moving.
const NEIGHBOURS: usize = 8;
/// The partitions known to be moving that a statement's maintenance surveys beside its own.
const HINTED: usize = 4;
/// The tail entries a survey of one partition counts before it stops.
const SURVEYED_TAIL: usize = 2 * MOVE_BATCH;
/// The vectors at which an index trained for under a quarter of the partitions it holds vectors for
/// is rebuilt, and the rows a statement inserts that make its maintenance check for it.
const BULK_AT: usize = 1024;
/// The partitions an index is trained to and kept at, per square root of its rows: finer
/// partitions, as the RaBitQ library's IVF index uses, let a query reach its recall scanning less
/// of the index, for a few more probes.
const PARTITIONS_PER_ROOT: usize = 4;
/// The vectors a rebuild reads before coding them together over the cores.
const CODE_BATCH: usize = 4096;
/// The sampled vectors a rebuild trains each partition from, and the most it samples, those for
/// the 4,096 partitions of 2^20 rows.
const TRAIN_PER_PARTITION: usize = 64;
const TRAIN_SAMPLE_MAX: usize = 262_144;

#[cfg(test)]
thread_local! {
    /// The rows maintenance surveys have read on this thread.
    static SURVEYED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
thread_local! {
    /// The query rounds started on this thread.
    static QUERY_ROUNDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// The point reads made on this thread, and the entries queries on it scored from 7-bit codes.
    static POINT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static QUERY_REFINES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// The exact 1-bit sums queries on this thread took, and the table rows their reranks read.
    static EXACT_SUMS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static TABLE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// The seeks walks made on this thread, the partitions queries on it probed, and the blocks
    /// they read from pages.
    static WALK_SEEKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static BLOCK_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Count a row a survey read, for the tests' bound on maintenance work.
fn surveyed() {
    #[cfg(test)]
    SURVEYED.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
thread_local! {
    /// Whether statements on this thread maintain the index, which tests turn off to leave a backlog,
    /// and the most jobs a statement's maintenance on it does, which tests lower to leave work due.
    static STATEMENT_UPKEEP: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
    static STATEMENT_BUDGET: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
}

fn statement_upkeep() -> bool {
    #[cfg(test)]
    return STATEMENT_UPKEEP.with(|on| on.get());
    #[cfg(not(test))]
    true
}

/// The jobs the maintenance of a statement that wrote `written` rows of the index may do: one for
/// each.
fn statement_budget(written: usize) -> usize {
    #[cfg(test)]
    return written.min(STATEMENT_BUDGET.with(|budget| budget.get()));
    #[cfg(not(test))]
    written
}

/// What a filtered query has given out: the rowids, so a later round gives none out again, and the
/// `limit` nearest rows the query's other terms accepted, the farthest on top.
struct Filter {
    limit: usize,
    round: u32,
    /// Whether the current round probes every live partition, and whether it kept every row of the
    /// index, leaving no later round anything to find.
    probed_all: bool,
    whole: bool,
    /// The distance of the farthest row the current round kept, past which lie the rows of its
    /// partitions it did not keep; infinite when it kept them all.
    reach: f64,
    given: HashSet<i64>,
    accepted: BinaryHeap<Ordered>,
}

impl Filter {
    /// Whether no row still to come can displace one of the `limit` nearest accepted rows: the
    /// round's next row at `next`, or with none left the rows past the round's reach, is no nearer
    /// than the farthest of them. A row an earlier, narrower round accepted can lie past the reach
    /// of the current one.
    fn settled(&self, next: Option<f64>) -> bool {
        self.accepted.len() >= self.limit && self.accepted.peek().is_some_and(|farthest| next.unwrap_or(self.reach) >= farthest.0)
    }
}

struct Query {
    vector: Vec<f32>,
    /// The query as the engine's vector, for the exact rerank.
    asked: Vector<'static>,
    /// The query under the index's rotation, its summed components, and what an entry's slack
    /// is scaled by into the reach of its 1-bit estimate's error bound: the bound's confidence times
    /// the query's length over the root of one less than the rotation's width.
    rotated: Vec<f32>,
    sum: f64,
    spread: f64,
    scan: ScanTable,
    /// The rows the query asks for; the rows the current round asks for, more for each earlier
    /// round of a filtered query; and the 7-bit estimates the round keeps.
    wanted: usize,
    limit: usize,
    keep: usize,
    /// The query's product with each centroid version it has met.
    centres: HashMap<(i64, i64), f64>,
    /// The stale copies the probed partitions' dead rows name, as rowid and block.
    dead: HashSet<(i64, i64)>,
    /// The `keep` best 7-bit estimates of live entries so far, negated: the top is the bar an
    /// entry's raised 1-bit estimate must reach.
    estimates: BinaryHeap<Ordered>,
    results: BinaryHeap<Ordered>,
}

impl Query {
    fn bar(&self) -> f64 {
        match self.estimates.peek() {
            Some(worst) if self.estimates.len() >= self.keep => -worst.0,
            _ => f64::NEG_INFINITY,
        }
    }

    fn centre(&mut self, model: &Model, partition: i64, version: i64) -> Result<f64> {
        if let Some(c) = self.centres.get(&(partition, version)) {
            return Ok(*c);
        }
        let c = model.centroid(partition, version)?.map_or(0.0, |c| dot(&self.vector, c));
        self.centres.insert((partition, version), c);
        Ok(c)
    }
}

pub struct IvfRqCursor {
    configuration: IndexMethodConfiguration,
    nprobes: usize,
    refine: usize,
    /// Whether a query reranks its best estimates exactly from the table's vectors; without, it
    /// answers with the estimates and their estimated distances, reading no table row.
    rerank: bool,
    /// The anisotropic threshold T in hundredths: the weight on an error along the vector is
    /// `(D - 1) T^2 / (1 - T^2)` times the weight across it. Zero assigns by plain distance.
    anisotropy: usize,
    schema: BackingSchema,
    stores: Vec<Box<dyn CursorTrait>>,
    main: Option<Box<dyn CursorTrait>>,
    pending: VecDeque<BackingStoreOp>,
    model: Arc<Model>,
    shared: Shared,
    rotation: Option<Rotation>,
    loaded: bool,
    load: Load,
    change: Change,
    search: Search,
    query: Option<Query>,
    /// A filtered query's state; none for a query no other term filters.
    filter: Option<Filter>,
    answer: VecDeque<(i64, f64)>,
    upkeep: Upkeep,
    run: Option<Run>,
    /// The partitions this statement wrote, and the rows it wrote, for its maintenance.
    touched: BTreeSet<i64>,
    written: usize,
    inserted: usize,
    /// The rows the CREATE INDEX statement creating the index has inserted: past the first
    /// [`BULK_AT`] they are only recorded, for the statement's rebuild to code.
    building: Option<Vec<i64>>,
    /// Whether the cursor runs under MVCC, where a concurrent writer can code against a model older
    /// than the one its snapshot holds.
    concurrent: bool,
    /// The model and generation before a job that changes them, until the job is written.
    saved: Option<(Arc<Model>, Option<i64>)>,
    /// The partitions the job being planned changes, to survey again once it is written.
    dirty: Vec<i64>,
    generation: Option<i64>,
    /// How this statement's queries use and hold partitions' rows across snapshots, none where they
    /// don't; the partitions its writes entered in [`Writes`] for its transaction; and whether its
    /// statement is entered there as writing the index.
    sharing: Option<Sharing>,
    recorded: HashSet<i64>,
    writing: bool,
}

impl IvfRqCursor {
    fn new(configuration: IndexMethodConfiguration, shared: Shared) -> Self {
        let schema = backing_schema(&configuration);
        let parameter = |name: &str, default: usize| match configuration.parameters.get(name) {
            Some(Value::Numeric(Numeric::Integer(n))) if *n >= 0 => *n as usize,
            _ => default,
        };
        Self {
            nprobes: parameter("nprobes", 16).max(1),
            refine: parameter("refine", 4).max(1),
            rerank: parameter("rerank", 1) != 0,
            anisotropy: parameter("anisotropy", 0).min(99),
            configuration,
            schema,
            stores: Vec::new(),
            main: None,
            pending: VecDeque::new(),
            model: Arc::new(Model::default()),
            shared,
            rotation: None,
            loaded: false,
            load: Load::Generation(Get::Seek),
            change: Change::Idle,
            search: Search::Init,
            query: None,
            filter: None,
            answer: VecDeque::new(),
            upkeep: Upkeep::Idle,
            run: None,
            touched: BTreeSet::new(),
            written: 0,
            inserted: 0,
            building: None,
            concurrent: false,
            saved: None,
            dirty: Vec::new(),
            generation: None,
            sharing: None,
            recorded: HashSet::new(),
            writing: false,
        }
    }

    fn drive_pending(&mut self) -> IOResultOr<()> {
        while let Some(op) = self.pending.front_mut() {
            match op.step()? {
                IOResult::Done(()) => {
                    self.pending.pop_front();
                }
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
        Ok(IOResult::Done(()))
    }

    fn open_stores(&mut self, context: &IndexMethodContext) -> Result<()> {
        if self.stores.is_empty() {
            for table in &self.schema.tables {
                self.stores.push(context.open_table_cursor(&table.table_name())?);
            }
        }
        if self.main.is_none() {
            self.main = Some(context.open_table_cursor(&self.configuration.table_name)?);
        }
        Ok(())
    }

    fn rotation(&mut self, dims: usize) -> Rotation {
        if self.rotation.as_ref().is_none_or(|r| r.padded != Rotation::padded(dims)) {
            self.rotation = Some(Rotation::new(dims));
        }
        self.rotation.clone().expect("rotation built")
    }

    /// The anisotropy weight for vectors of `dims` dimensions.
    fn eta(&self, dims: usize) -> f32 {
        let hundredths = match ANISOTROPY_OVERRIDE.load(Ordering::Relaxed) {
            0 => self.anisotropy,
            100.. => 0,
            n => n,
        };
        if hundredths == 0 {
            return 0.0;
        }
        let t = hundredths as f32 / 100.0;
        (dims as f32 - 1.0) * t * t / (1.0 - t * t)
    }

    fn publish(&self) {
        if let Some(g) = self.generation {
            self.shared.lock().expect("index state").model = Some((g, self.model.clone()));
        }
    }

    /// The size past which a partition is split.
    fn split_bound(&self) -> i64 {
        split_bound(self.model.live())
    }

    /// Read the centroids into the model, unless the model cached under the stored generation is
    /// the one this snapshot holds.
    fn load(&mut self) -> IOResultOr<()> {
        if self.loaded {
            return Ok(IOResult::Done(()));
        }
        loop {
            let meta = &mut self.stores[CENTROIDS];
            match self.load {
                Load::Generation(mut get) => {
                    let found = match get_row(meta, GENERATION, &mut get)? {
                        IOResult::Done(found) => found,
                        IOResult::IO(io) => {
                            self.load = Load::Generation(get);
                            return Ok(IOResult::IO(io));
                        }
                    };
                    let stored = found.as_ref().map(|r| integer(r, 0)).transpose()?;
                    let cached = self.shared.lock().expect("index state").model.as_ref().filter(|(g, _)| Some(*g) == stored).map(|(_, m)| m.clone());
                    if let Some(model) = cached {
                        self.load = Load::Generation(Get::Seek);
                        self.model = model;
                        self.generation = stored;
                        self.loaded = true;
                        return Ok(IOResult::Done(()));
                    }
                    self.model = Arc::new(Model::default());
                    self.generation = stored;
                    self.load = Load::Walk(Walk::Seek);
                }
                Load::Walk(mut walk) => {
                    let row = match walk_row(meta, 0, i64::MAX, &mut walk)? {
                        IOResult::Done(row) => row,
                        IOResult::IO(io) => {
                            self.load = Load::Walk(walk);
                            return Ok(IOResult::IO(io));
                        }
                    };
                    self.load = Load::Walk(walk);
                    let Some((key, found)) = row else {
                        self.load = Load::Parts { partition: 0, get: Get::Seek };
                        continue;
                    };
                    let (partition, version) = (key >> VERSION_BITS, key & ((1 << VERSION_BITS) - 1));
                    let centroid = dense(Some(bytes_at(&found, 0)?))?;
                    let model = Arc::make_mut(&mut self.model);
                    if partition as usize > model.centroids.len() {
                        return Err(LimboError::Corrupt("ivf_rq: centroid partitions are not dense".into()).into());
                    }
                    model.place(partition as usize, version, centroid);
                }
                Load::Parts { partition, mut get } => {
                    if partition >= self.model.centroids.len() {
                        self.load = Load::Generation(Get::Seek);
                        self.loaded = true;
                        if let Some(dims) = self.model.centroids.first().map(Vec::len) {
                            let eta = self.eta(dims);
                            Arc::make_mut(&mut self.model).eta = eta;
                        }
                        self.publish();
                        return Ok(IOResult::Done(()));
                    }
                    let found = match get_row(&mut self.stores[PARTITIONS], part_key(partition as i64)?, &mut get)? {
                        IOResult::Done(found) => found,
                        IOResult::IO(io) => {
                            self.load = Load::Parts { partition, get };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    if let Some(found) = found {
                        Arc::make_mut(&mut self.model).states[partition] = Part::parse(&found)?.state;
                    }
                    self.load = Load::Parts { partition: partition + 1, get: Get::Seek };
                }
            }
        }
    }

    /// Drive the delete in progress to its end.
    fn drive_change(&mut self) -> IOResultOr<()> {
        loop {
            match std::mem::replace(&mut self.change, Change::Idle) {
                Change::Idle => return Ok(IOResult::Done(())),
                Change::Remove { rowid, mut get } => {
                    let found = match get_row(&mut self.stores[VECTORS], rowid, &mut get)? {
                        IOResult::Done(found) => found.ok_or_else(|| error("deleted vector has no row"))?,
                        IOResult::IO(io) => {
                            self.change = Change::Remove { rowid, get };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    self.written += 1;
                    self.change = match Location::decode(integer(&found, 0)?) {
                        Location::Tail(key) => {
                            self.touched.insert(partition_of(key));
                            Change::Apply(VecDeque::from([remove(VECTORS, rowid), remove(PARTITIONS, key)]))
                        }
                        Location::Block(block) => {
                            self.touched.insert(partition_of(block));
                            Change::Bury { rowid, block, get: Get::Seek }
                        }
                    };
                }
                Change::Bury { rowid, block, mut get } => {
                    let key = dead_key(block, rowid)?;
                    let found = match get_row(&mut self.stores[PARTITIONS], key, &mut get)? {
                        IOResult::Done(found) => found,
                        IOResult::IO(io) => {
                            self.change = Change::Bury { rowid, block, get };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    let mut blocks = match &found {
                        Some(r) => dead_blocks(bytes_at(r, 0)?)?,
                        None => Vec::new(),
                    };
                    blocks.push(block);
                    self.change = Change::Apply(VecDeque::from([remove(VECTORS, rowid), put(PARTITIONS, key, dead_record(&blocks)?)]));
                }
                Change::Apply(mut steps) => {
                    let Some(step) = steps.front_mut() else {
                        return Ok(IOResult::Done(()));
                    };
                    self.record(step);
                    match apply_step(&mut self.stores, step, None)? {
                        IOResult::Done(()) => {
                            steps.pop_front();
                            self.change = Change::Apply(steps);
                        }
                        IOResult::IO(io) => {
                            self.change = Change::Apply(steps);
                            return Ok(IOResult::IO(io));
                        }
                    }
                }
            }
        }
    }

    /// Set up how this statement records its writes and its queries use and hold partitions' rows
    /// across snapshots: not in an attached database, nor where other processes write the
    /// database, whose commits run no hook here.
    fn share(&mut self, context: &IndexMethodContext) -> Result<()> {
        self.sharing = None;
        self.recorded.clear();
        self.writing = false;
        if context.database().id != crate::MAIN_DB_ID {
            return Ok(());
        }
        let connection = context.connection()?;
        if connection.db.opts.enable_multiprocess_wal {
            return Ok(());
        }
        let (clock, holds) = match context.snapshot() {
            IndexMethodSnapshotIdentity::Mvcc { .. } => match connection.mv_store_for_db(crate::MAIN_DB_ID) {
                Some(store) => (Clock::Mvcc(store), true),
                None => return Ok(()),
            },
            IndexMethodSnapshotIdentity::Wal { .. } => match connection.get_pager_from_database_index(&crate::MAIN_DB_ID)?.wal.clone() {
                Some(wal) => (Clock::Wal(wal), context.transaction_mode() == IndexMethodTransactionMode::Read),
                None => return Ok(()),
            },
        };
        let writes = self.writes(context.database().incarnation, &clock);
        let snapshot = position(context.snapshot());
        self.sharing = Some(Sharing { writes, clock, connection: std::sync::Arc::downgrade(&connection), snapshot, holds: holds && snapshot.is_some() });
        Ok(())
    }

    /// The index's [`Writes`], found by its database's incarnation and its name, and kept with the
    /// attachment's state.
    fn writes(&self, incarnation: u64, clock: &Clock) -> SharedWrites {
        let mut shared = self.shared.lock().expect("index state");
        if let Some((at, writes)) = &shared.writes {
            if *at == incarnation {
                return writes.clone();
            }
        }
        let mut registry = WRITES.lock().expect("index writes");
        registry.retain(|_, writes| writes.strong_count() > 0);
        let key = (incarnation, self.configuration.index_name.clone());
        let writes = match registry.get(&key).and_then(std::sync::Weak::upgrade) {
            Some(writes) => writes,
            None => {
                let writes = std::sync::Arc::new(std::sync::Mutex::new(Writes { open: HashMap::new(), changed: HashMap::new(), floor: clock.now() }));
                registry.insert(key, std::sync::Arc::downgrade(&writes));
                writes
            }
        };
        shared.writes = Some((incarnation, writes.clone()));
        writes
    }

    /// Enter, before a step on the partitions table reaches a page, that this cursor's transaction
    /// writes the partition the step changes.
    fn record(&mut self, step: &Step) {
        let (Step::Put { store, rowid, .. } | Step::Remove { store, rowid, .. }) = step;
        if *store != PARTITIONS {
            return;
        }
        let partition = partition_of(*rowid);
        if self.recorded.contains(&partition) {
            return;
        }
        let Some(sharing) = &self.sharing else {
            return;
        };
        let mut writes = sharing.writes.lock().expect("index writes");
        let writer = writes.open.entry(sharing.key()).or_insert_with(|| Writer { connection: sharing.connection.clone(), partitions: HashSet::new(), statements: 0, staged: false });
        if !self.writing {
            writer.statements += 1;
            self.writing = true;
        }
        writer.partitions.insert(partition);
        drop(writes);
        self.recorded.insert(partition);
    }

    /// This cursor's statement staged its writes, or undid them: it no longer writes the index. A
    /// transaction none of whose statements staged a write to it, and none still writing it, has
    /// nothing to commit there, and comes off the open writers.
    fn finish_statement(&mut self, staged: bool) {
        self.recorded.clear();
        if !std::mem::take(&mut self.writing) {
            return;
        }
        let Some(sharing) = &self.sharing else {
            return;
        };
        let key = sharing.key();
        let mut writes = sharing.writes.lock().expect("index writes");
        if let Some(writer) = writes.open.get_mut(&key) {
            writer.statements = writer.statements.saturating_sub(1);
            writer.staged |= staged;
            if writer.statements == 0 && !writer.staged {
                writes.open.remove(&key);
            }
        }
    }

    /// This cursor's transaction has ended: the partitions it wrote are raised to the commit
    /// position now, past its commit, and it comes off the open writers. A rolled-back one is
    /// raised as well, as a rollback can follow a commit made durable before its statement was
    /// dropped.
    fn end_transaction(&mut self) {
        self.recorded.clear();
        self.writing = false;
        let Some(sharing) = &self.sharing else {
            return;
        };
        let mut writes = sharing.writes.lock().expect("index writes");
        if let Some(writer) = writes.open.remove(&sharing.key()) {
            writes.raise(writer.partitions, sharing.clock.now());
        }
    }

    /// The writes that give a vector its code at the end of its nearest partition's tail, and that
    /// partition.
    fn tail_steps(&mut self, rowid: i64, vector: &[f32]) -> Result<(i64, [Step; 2])> {
        let rotation = self.rotation(vector.len());
        let (partition, version, entry) = self.model.encode(&rotation, rowid, vector);
        let key = tail_key(partition, rowid)?;
        Ok((
            partition,
            [
                put(PARTITIONS, key, record(&[Value::from_i64(version), blob(&pack(std::slice::from_ref(&entry))?)?])?),
                put(VECTORS, rowid, record(&[Value::from_i64(Location::Tail(key).encode())])?),
            ],
        ))
    }
}

/// Count what a survey's walk found at `key`, a row of `kind` that is not a block, which the walk
/// has left `cursor` holding.
fn surveyed_row(survey: &mut Survey, cursor: &mut Box<dyn CursorTrait>, key: i64, kind: i64) -> Result<()> {
    let partition = partition_of(key);
    match kind {
        PART_ROW => {
            survey.parts.insert(partition, Part::parse(held_record(cursor)?)?);
        }
        DEAD_ROW => {
            for block in dead_blocks(bytes_at(held_record(cursor)?, 0)?)? {
                *survey.stale.entry(block).or_default() += 1;
            }
        }
        _ => *survey.tails.entry(partition).or_default() += 1,
    }
    Ok(())
}

/// Adds the time since its start to a counter when dropped, while [`MEASURE`] is on.
struct Timer<'a>(&'a AtomicUsize, Option<std::time::Instant>);

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.1 {
            self.0.fetch_add(started.elapsed().as_nanos() as usize, Ordering::Relaxed);
        }
    }
}

fn timer(counter: &AtomicUsize) -> Timer<'_> {
    Timer(counter, MEASURE.load(Ordering::Relaxed).then(std::time::Instant::now))
}

/// Count `n` on a measurement counter, while [`MEASURE`] is on.
fn tally(counter: &AtomicUsize, n: usize) {
    if MEASURE.load(Ordering::Relaxed) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

impl IndexMethodCursor for IvfRqCursor {
    fn create(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.pending.is_empty() {
            self.pending.push_back(context.create_backing_schema(&self.schema)?);
        }
        let created = self.drive_pending()?;
        // The statement goes on to insert the table's rows. A statement that does not maintain
        // the index writes them all.
        if matches!(created, IOResult::Done(())) && statement_upkeep() {
            self.building = Some(Vec::new());
        }
        Ok(created)
    }

    fn destroy(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.pending.is_empty() {
            self.pending.push_back(context.drop_backing_schema(&self.schema)?);
        }
        self.drive_pending()
    }

    fn open_read(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        let _timer = timer(&OPEN_NANOS);
        self.concurrent = matches!(context.snapshot(), IndexMethodSnapshotIdentity::Mvcc { .. });
        self.open_stores(context)?;
        self.share(context)?;
        self.load()
    }

    fn open_write(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        self.concurrent = matches!(context.snapshot(), IndexMethodSnapshotIdentity::Mvcc { .. });
        self.open_stores(context)?;
        self.share(context)?;
        self.load()
    }

    fn close(&mut self, _context: &IndexMethodContext) {
        self.stores.clear();
        self.main = None;
    }

    fn insert(&mut self, values: &[Register]) -> IOResultOr<()> {
        if matches!(self.change, Change::Idle) {
            let rowid = values[1].get_value().as_int().ok_or_else(|| error("rowid must be an integer"))?;
            if let Some(rowids) = self.building.as_mut() {
                rowids.push(rowid);
                // The statement's rebuild codes this row, and the rows before it, into blocks.
                if rowids.len() >= BULK_AT {
                    return Ok(IOResult::Done(()));
                }
            }
            let vector = dense(values[0].get_value().to_blob())?;
            let encoding = timer(&ENCODE_NANOS);
            let (partition, steps) = self.tail_steps(rowid, &vector)?;
            drop(encoding);
            tally(&INSERTS, 1);
            self.touched.insert(partition);
            self.written += 1;
            self.inserted += 1;
            self.change = Change::Apply(steps.into());
        }
        let _timer = timer(&CHANGE_NANOS);
        self.drive_change()
    }

    fn delete(&mut self, values: &[Register]) -> IOResultOr<()> {
        if matches!(self.change, Change::Idle) {
            let vector = dense(values[0].get_value().to_blob())?;
            self.rotation(vector.len());
            let rowid = values[1].get_value().as_int().ok_or_else(|| error("rowid must be an integer"))?;
            self.change = Change::Remove { rowid, get: Get::Seek };
        }
        self.drive_change()
    }

    fn query_start(&mut self, values: &[Register]) -> IOResultOr<bool> {
        if matches!(self.search, Search::Init) {
            let _timer = timer(&INIT_NANOS);
            // A negative LIMIT asks for every row.
            let limit = values[2].get_value().as_int().ok_or_else(|| error("limit must be an integer"))?;
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            if limit == 0 {
                self.answer.clear();
                return Ok(IOResult::Done(false));
            }
            let vector = dense(values[1].get_value().to_blob())?;
            // The planner passes a filtered query one more value, 1, after the captured ones.
            let filtered = values.get(3).and_then(|v| v.get_value().as_int()) == Some(1);
            let rotation = self.rotation(vector.len());
            let rotated = rotation.apply(&vector);
            let sum = rotated.iter().map(|x| *x as f64).sum();
            let length = rotated.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();
            let spread = EPSILON * length / ((rotated.len() - 1) as f64).sqrt();
            let scan = ScanTable::new(&rotated);
            let asked = Vector::from_slice_owned(values[1].get_value().to_blob().ok_or_else(|| error("query is not a vector"))?)?;
            self.query = Some(Query {
                vector,
                asked,
                rotated,
                sum,
                spread,
                scan,
                wanted: limit,
                limit,
                keep: limit,
                centres: HashMap::new(),
                dead: HashSet::new(),
                estimates: BinaryHeap::new(),
                results: BinaryHeap::new(),
            });
            self.filter = filtered.then(|| Filter { limit, round: 0, probed_all: false, whole: false, reach: f64::INFINITY, given: HashSet::new(), accepted: BinaryHeap::new() });
            self.search = self.round()?;
        }
        self.search()
    }

    fn query_next(&mut self) -> IOResultOr<bool> {
        // A round a filtered query started in an earlier call is waiting on I/O.
        if !matches!(self.search, Search::Init) {
            return self.search();
        }
        let given = self.answer.pop_front();
        let Some(filter) = self.filter.as_mut() else {
            return Ok(IOResult::Done(!self.answer.is_empty()));
        };
        if let Some((rowid, _)) = given {
            filter.given.insert(rowid);
        }
        if filter.settled(self.answer.front().map(|(_, distance)| *distance)) {
            self.answer.clear();
            return Ok(IOResult::Done(false));
        }
        if self.answer.is_empty() && !filter.whole {
            filter.round += 1;
            self.search = self.round()?;
            return self.search();
        }
        Ok(IOResult::Done(!self.answer.is_empty()))
    }

    fn query_accept(&mut self) -> Result<()> {
        let Some(filter) = self.filter.as_mut() else {
            return Err(*error("a row was accepted for a query no other term filters"));
        };
        let &(rowid, distance) = self.answer.front().ok_or_else(|| error("no current row"))?;
        filter.accepted.push(Ordered(distance, rowid));
        if filter.accepted.len() > filter.limit {
            filter.accepted.pop();
        }
        Ok(())
    }

    /// A filtered query's cost in the planner's units, a page read being one. It gives out about
    /// `limit / selectivity` rows before `limit` are accepted, in rounds of four times the rows and
    /// partitions of the last, and each round reads its partitions' blocks and fetches `refine`
    /// vectors for each row it asks for. The index is taken to hold the partitions a rebuild
    /// trains for the table's rows, as maintenance keeps it.
    fn estimate_cost(&self, context: &IndexMethodCostContext<'_>) -> Option<IndexMethodCostEstimate> {
        let selectivity = context.filter_selectivity?;
        let rows = context.base_table_rows.max(1.0);
        let partitions = trained_partitions(rows as usize) as f64;
        let limit = context.arguments.get(1).and_then(literal_rows).unwrap_or(ASSUMED_LIMIT).clamp(1.0, rows);
        let wanted = (limit / selectivity.max(1.0 / rows)).min(rows);
        let (mut asked, mut probes, mut cost) = (limit, self.nprobes as f64, 0.0);
        loop {
            let probed = probes.min(partitions);
            cost += probed * (rows / partitions) / CODES_PER_PAGE + self.refine as f64 * asked.min(rows) * FETCH_PAGES;
            if asked >= wanted || probed >= partitions && asked >= rows {
                break;
            }
            asked *= 4.0;
            probes *= 4.0;
        }
        Some(IndexMethodCostEstimate { estimated_cost: cost, estimated_rows: wanted.ceil() as u64 })
    }

    fn query_column(&mut self, _: usize) -> IOResultOr<Value> {
        let (_, distance) = self.answer.front().ok_or_else(|| error("no current row"))?;
        Ok(IOResult::Done(Value::from_f64(*distance)))
    }

    fn query_rowid(&mut self) -> IOResultOr<Option<i64>> {
        let (rowid, _) = self.answer.front().ok_or_else(|| error("no current row"))?;
        Ok(IOResult::Done(Some(*rowid)))
    }

    /// `OPTIMIZE INDEX`: survey every partition and do every job there is, until none is left.
    fn optimize(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.run.is_none() {
            self.run = Some(Run { budget: usize::MAX, survey: Survey::default(), all: true, first: Vec::new(), build: None });
        }
        self.maintain(context)
    }

    /// Before the statement's writes are released, do the maintenance they made due, chosen among
    /// the partitions it wrote, a few known to be moving, so moves finish whichever partitions
    /// writers touch, and as many as it may do jobs of those earlier runs left work due in, so work
    /// a statement found and had no job left for is done by the next: at most one job for each row
    /// the statement wrote, when statements of a hundred random rows cost about half a job a row to
    /// keep maintained, so a run ends with nothing due well before its bound.
    fn stage_statement_commit(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        let staged = self.stage(context)?;
        if matches!(staged, IOResult::Done(())) {
            self.finish_statement(true);
        }
        Ok(staged)
    }

    fn abort_statement(&mut self, _context: &IndexMethodContext) {
        self.finish_statement(false);
        self.forget_run();
        self.touched.clear();
        self.written = 0;
        self.inserted = 0;
        self.building = None;
    }

    fn on_transaction_committed(&mut self, _context: &IndexMethodContext) {
        self.end_transaction();
    }

    fn on_transaction_rolled_back(&mut self, _context: &IndexMethodContext) {
        self.end_transaction();
        self.forget_run();
    }

    fn on_savepoint_rolled_back(&mut self, _context: &IndexMethodContext) {
        self.forget_run();
    }
}

impl IvfRqCursor {
    /// Start the query's next round: probe the `nprobes` live partitions nearest the query for the
    /// rows it asks for, and for each earlier round of a filtered query four times as many of both,
    /// so a query whose other terms reject most rows reaches as far as it must. A query asking for
    /// every row probes every partition. Under MVCC, partitions retired since a concurrent writer
    /// coded against them are read too, for tail entries left there.
    fn round(&mut self) -> Result<Search> {
        let nprobes = match NPROBES_OVERRIDE.load(Ordering::Relaxed) {
            0 => self.nprobes,
            n => n,
        };
        let refine = match REFINE_OVERRIDE.load(Ordering::Relaxed) {
            0 => self.refine,
            n => n,
        };
        #[cfg(test)]
        QUERY_ROUNDS.with(|count| count.set(count.get() + 1));
        let widen = self.filter.as_ref().map_or(1, |filter| 4usize.saturating_pow(filter.round));
        let query = self.query.as_mut().ok_or_else(|| error("query state absent"))?;
        query.limit = query.wanted.saturating_mul(widen);
        query.keep = refine.saturating_mul(query.limit);
        query.estimates.clear();
        query.results.clear();
        let mut probed_all = true;
        let probes: VecDeque<i64> = if self.model.centroids.is_empty() {
            VecDeque::from([0])
        } else {
            let probeable = self.model.states.iter().filter(|s| **s != State::Retired).count();
            let take = match query.wanted {
                usize::MAX => probeable,
                _ => nprobes.saturating_mul(widen).min(probeable),
            };
            probed_all = take == probeable;
            let ranked = self.model.probes(&query.vector, take, PROBE_BY_PRODUCT.load(Ordering::Relaxed) != 0);
            // A retired partition holds no block, but under MVCC a writer coding against a
            // model older than its retirement can still have left a tail entry there, which
            // maintenance moves out; until it does, the query reads it. A WAL writer codes
            // against the latest model, which never assigns to a retired partition.
            let retired = (0..self.model.centroids.len() as i64).filter(|p| self.concurrent && self.model.states[*p as usize] == State::Retired);
            ranked.into_iter().map(|(_, p)| p).chain(retired).collect()
        };
        if let Some(filter) = self.filter.as_mut() {
            filter.probed_all = probed_all;
        }
        self.answer.clear();
        self.next_probe(probes)
    }

    /// Drive the query's round to its end: the answer then holds the round's rows in order of
    /// distance, less those an earlier round of a filtered query gave out, and a filtered round
    /// left with none goes on to the next.
    fn search(&mut self) -> IOResultOr<bool> {
        loop {
            match std::mem::replace(&mut self.search, Search::Init) {
                Search::Init => return Err(error("no query is in progress")),
                Search::Probe { probes, partition, mut walk, mut end, mut blocks, mut rows } => {
                    let reading = timer(&READ_NANOS);
                    let row = match walk_held(&mut self.stores[PARTITIONS], part_key(partition)?, end, &mut walk)? {
                        IOResult::Done(row) => row,
                        IOResult::IO(io) => {
                            self.search = Search::Probe { probes, partition, walk, end, blocks, rows };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    drop(reading);
                    let query = self.query.as_mut().ok_or_else(|| error("query state absent"))?;
                    let Some(key) = row else {
                        if let Blocks::Held(held) = &blocks {
                            for (key, version, codes) in &held.blocks {
                                let centre = query.centre(&self.model, partition, *version)?;
                                let _scoring = timer(&SCORE_NANOS);
                                score_block(query, &held.codes[codes.clone()], centre, Location::Block(*key))?;
                            }
                        }
                        let holding = match blocks {
                            Blocks::Holding(read, version) => Some((read, version)),
                            _ => None,
                        };
                        if holding.is_some() || rows.is_some() {
                            let mut shared = self.shared.lock().expect("index state");
                            if let Some((read, version)) = holding {
                                shared.cache.insert(partition, version, read);
                            }
                            if let (Some(rows), Some(sharing)) = (rows, &self.sharing) {
                                if sharing.writes.lock().expect("index writes").holds(partition, rows.snapshot, || sharing.clock.now()) {
                                    shared.cache.hold(partition, rows);
                                }
                            }
                        }
                        self.search = self.next_probe(probes)?;
                        continue;
                    };
                    // Decoded and scored where the cursor holds it, so no block is copied unless it
                    // is to be held.
                    let record = ImmutableRecordRef::from_bin_record(held_payload(&mut self.stores[PARTITIONS])?);
                    match kind_of(key) {
                        PART_ROW => {
                            if let Some(ValueRef::Numeric(Numeric::Integer(version))) = record.get_value_opt(7) {
                                let mut shared = self.shared.lock().expect("index state");
                                blocks = match shared.cache.get(partition, version) {
                                    Some(held) => {
                                        tally(&CACHE_HITS, 1);
                                        end = partition_key(partition, BLOCK_ROW, 0)?;
                                        held.prefetch(self.query.as_ref().map_or(0, |q| q.rotated.len()));
                                        Blocks::Held(held)
                                    }
                                    None if shared.cache.admits(partition, version) => {
                                        tally(&CACHE_MISSES, 1);
                                        Blocks::Holding(HeldBlocks::default(), version)
                                    }
                                    None => {
                                        tally(&CACHE_MISSES, 1);
                                        Blocks::Unread
                                    }
                                };
                                rows = match (&blocks, &self.sharing) {
                                    (Blocks::Held(_) | Blocks::Holding(..), Some(Sharing { snapshot: Some(snapshot), holds: true, .. })) => Some(HeldRows::new(*snapshot, version)),
                                    _ => None,
                                };
                            }
                        }
                        DEAD_ROW => {
                            let dead = match record.get_value_opt(0) {
                                Some(ValueRef::Blob(dead)) => dead_blocks(dead)?,
                                _ => return Err(LimboError::Corrupt("ivf_rq: a dead row holds no blocks".into()).into()),
                            };
                            let rowid = key & ((1 << ROWID_BITS) - 1);
                            if let Some(rows) = rows.as_mut() {
                                rows.dead.extend(dead.iter().map(|block| (rowid, *block)));
                            }
                            query.dead.extend(dead.into_iter().map(|block| (rowid, block)));
                        }
                        kind => {
                            tally(&RECORDS, 1);
                            let (version, codes) = version_and_codes(&record)?;
                            let source = match kind {
                                TAIL_ROW => {
                                    if rows.as_ref().is_some_and(|rows| rows.tail.len() >= HELD_TAIL) {
                                        rows = None;
                                    }
                                    if let Some(rows) = rows.as_mut() {
                                        rows.push_tail(key, version, codes);
                                    }
                                    Location::Tail(key)
                                }
                                _ => {
                                    #[cfg(test)]
                                    BLOCK_READS.with(|count| count.set(count.get() + 1));
                                    if let Blocks::Holding(read, _) = &mut blocks {
                                        read.push(key, version, codes);
                                    }
                                    Location::Block(key)
                                }
                            };
                            let centre = query.centre(&self.model, partition, version)?;
                            let _scoring = timer(&SCORE_NANOS);
                            score_block(query, codes, centre, source)?;
                        }
                    }
                    self.search = Search::Probe { probes, partition, walk, end, blocks, rows };
                }
                Search::Rerank { mut candidates, rowid } => {
                    let _timer = timer(&RERANK_NANOS);
                    let rerank = match RERANK_OVERRIDE.load(Ordering::Relaxed) {
                        0 => self.rerank,
                        n => n == 1,
                    };
                    let query = self.query.as_mut().ok_or_else(|| error("query state absent"))?;
                    if candidates.is_empty() && rowid.is_none() && !query.estimates.is_empty() {
                        let estimates = std::mem::take(&mut query.estimates);
                        match rerank {
                            true => candidates = estimates.into_iter().map(|o| o.1).collect(),
                            // The best estimates are the answer, at their estimated distances.
                            false => {
                                for estimate in estimates {
                                    query.results.push(estimate);
                                    if query.results.len() > query.limit {
                                        query.results.pop();
                                    }
                                }
                            }
                        }
                    }
                    let Some(current) = rowid.or_else(|| candidates.pop_front()) else {
                        let mut results: Vec<Ordered> = std::mem::take(&mut query.results).into_vec();
                        results.sort();
                        let short = results.len() < query.limit;
                        let reach = match short {
                            true => f64::INFINITY,
                            false => results.last().map_or(f64::INFINITY, |o| o.0),
                        };
                        self.answer = results.into_iter().map(|o| (o.1, o.0)).collect();
                        if let Some(filter) = self.filter.as_mut() {
                            // A round that probed every live partition and kept fewer rows than it
                            // asked for kept every row of the index.
                            filter.whole = filter.probed_all && short;
                            filter.reach = reach;
                            self.answer.retain(|(rowid, _)| !filter.given.contains(rowid));
                            if self.answer.is_empty() && !filter.whole && !filter.settled(None) {
                                filter.round += 1;
                                self.search = self.round()?;
                                continue;
                            }
                        }
                        return Ok(IOResult::Done(!self.answer.is_empty()));
                    };
                    #[cfg(test)]
                    TABLE_READS.with(|count| count.set(count.get() + 1));
                    let main = self.main.as_mut().ok_or_else(|| error("table cursor must be open"))?;
                    match main.seek(SeekKey::TableRowId(current), SeekOp::GE { eq_only: true })? {
                        IOResult::Done(SeekResult::Found) => {}
                        IOResult::Done(_) => return Err(error("a vector's row names a row the table lacks")),
                        IOResult::IO(io) => {
                            self.search = Search::Rerank { candidates, rowid: Some(current) };
                            return Ok(IOResult::IO(io));
                        }
                    }
                    let found = match main.record()? {
                        IOResult::Done(found) => found.ok_or_else(|| error("table row absent"))?,
                        IOResult::IO(io) => {
                            self.search = Search::Rerank { candidates, rowid: Some(current) };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    let column = self.configuration.columns[0].pos_in_table;
                    let ValueRef::Blob(bytes) = found.get_value(column)? else {
                        return Err(error("table value is not a vector"));
                    };
                    let stored = Vector::from_slice_owned(bytes)?;
                    let distance = operations::distance_dot::vector_distance_dot(&stored, &query.asked)?;
                    query.results.push(Ordered(distance, current));
                    if query.results.len() > query.limit {
                        query.results.pop();
                    }
                    self.search = Search::Rerank { candidates, rowid: None };
                }
            }
        }
    }

    /// Probe the next partitions, scoring from memory those whose held rows serve this snapshot,
    /// the next one's blocks started on their way to the cache while each is scored, until one
    /// must be read from pages; with none left, the rerank.
    fn next_probe(&mut self, mut probes: VecDeque<i64>) -> Result<Search> {
        let dims = self.query.as_ref().map_or(0, |q| q.rotated.len());
        let mut next = probes.pop_front().map(|p| (p, self.held(p)));
        while let Some((partition, held)) = next.take() {
            #[cfg(test)]
            PROBES.with(|count| count.set(count.get() + 1));
            let Some((rows, blocks)) = held else {
                let (_, end) = rows_from(partition, PART_ROW)?;
                return Ok(Search::Probe { probes, partition, walk: Walk::Seek, end, blocks: Blocks::Unread, rows: None });
            };
            next = probes.pop_front().map(|p| (p, self.held(p)));
            if let Some((_, Some((_, ahead)))) = &next {
                ahead.prefetch(dims);
            }
            self.score_held(partition, &rows, &blocks)?;
        }
        Ok(Search::Rerank { candidates: VecDeque::new(), rowid: None })
    }

    /// The rows held for `partition` and its blocks, held under their version, when the rows serve
    /// this cursor's snapshot, so the partition is scored from memory, reading no page.
    fn held(&self, partition: i64) -> Option<(Arc<HeldRows>, Arc<HeldBlocks>)> {
        let sharing = self.sharing.as_ref()?;
        let snapshot = sharing.snapshot?;
        let mut shared = self.shared.lock().expect("index state");
        let rows = shared.cache.rows.get(&partition).cloned()?;
        if !sharing.writes.lock().expect("index writes").serves(partition, rows.snapshot, snapshot, || sharing.clock.now()) {
            return None;
        }
        let blocks = shared.cache.get(partition, rows.version)?;
        Some((rows, blocks))
    }

    fn score_held(&mut self, partition: i64, rows: &HeldRows, blocks: &HeldBlocks) -> Result<()> {
        tally(&CACHE_HITS, 1);
        tally(&ROW_HITS, 1);
        let query = self.query.as_mut().ok_or_else(|| error("query state absent"))?;
        query.dead.extend(rows.dead.iter().copied());
        let tail = rows.tail.iter().map(|(key, version, codes)| (Location::Tail(*key), *version, &rows.codes[codes.clone()]));
        let packed = blocks.blocks.iter().map(|(key, version, codes)| (Location::Block(*key), *version, &blocks.codes[codes.clone()]));
        for (source, version, codes) in tail.chain(packed) {
            let centre = query.centre(&self.model, partition, version)?;
            let _scoring = timer(&SCORE_NANOS);
            score_block(query, codes, centre, source)?;
        }
        Ok(())
    }

    /// The maintenance [`IndexMethodCursor::stage_statement_commit`] does before the statement's
    /// writes are released.
    fn stage(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if let Some(rowids) = self.building.take().filter(|rowids| rowids.len() >= BULK_AT) {
            // A CREATE INDEX over BULK_AT rows or more: a rebuild over them clears the tail entries
            // the first wrote and codes every row into its block.
            self.run = Some(Run { budget: rowids.len(), survey: Survey::default(), all: false, first: Vec::new(), build: Some(rowids) });
            self.touched.clear();
            self.written = 0;
            self.inserted = 0;
        }
        if self.run.is_none() {
            if self.written == 0 || !statement_upkeep() {
                self.touched.clear();
                self.written = 0;
                self.inserted = 0;
                return Ok(IOResult::Done(()));
            }
            let budget = statement_budget(self.written);
            let mut first = std::mem::take(&mut self.touched);
            let shared = self.shared.lock().expect("index state");
            first.extend(shared.moving.iter().take(HINTED).copied());
            first.extend(shared.due.iter().take(budget).copied());
            drop(shared);
            // A statement that inserted many rows surveys every partition, so a rebuild is chosen
            // when its rows have outgrown the partitions.
            let all = self.inserted >= BULK_AT;
            self.run = Some(Run { budget, survey: Survey::default(), all, first: first.into_iter().collect(), build: None });
            self.written = 0;
            self.inserted = 0;
        }
        self.maintain(context)
    }

    /// Drop a run that will not finish, and the model a job it abandoned had changed; the model is
    /// read again on next use.
    fn forget_run(&mut self) {
        self.run = None;
        self.upkeep = Upkeep::Idle;
        self.dirty.clear();
        if let Some((model, generation)) = self.saved.take() {
            self.model = model;
            self.generation = generation;
        }
        self.loaded = false;
    }

    /// Keep the model and generation a job is about to change, to restore if the job is reverted.
    fn save_model(&mut self) {
        if self.saved.is_none() {
            self.saved = Some((self.model.clone(), self.generation));
        }
    }

    /// The size below which a live partition is retired: a sixteenth of the size past which one is
    /// split, so a split's halves are never small enough to retire.
    fn retire_bound(&self) -> i64 {
        self.split_bound() / 16
    }

    /// Drive the run in `self.run` to its end.
    fn maintain(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        loop {
            match std::mem::replace(&mut self.upkeep, Upkeep::Idle) {
                Upkeep::Idle => {
                    self.open_stores(context)?;
                    // OPTIMIZE INDEX opens no cursor before it maintains, and its writes are
                    // recorded like any other.
                    if self.sharing.is_none() {
                        self.share(context)?;
                    }
                    self.upkeep = Upkeep::Load;
                }
                Upkeep::Load => {
                    if let IOResult::IO(io) = self.load()? {
                        self.upkeep = Upkeep::Load;
                        return Ok(IOResult::IO(io));
                    }
                    let run = self.run.as_mut().ok_or_else(|| error("maintenance without a run"))?;
                    if let Some(rowids) = run.build.take() {
                        self.save_model();
                        self.upkeep = Upkeep::Rebuild { phase: sampling(rowids), steps: VecDeque::new() };
                        continue;
                    }
                    self.upkeep = Upkeep::Survey(match run.all {
                        true => Surveying::All { partition: 0, walk: Walk::Seek },
                        false => Surveying::Some { pending: std::mem::take(&mut run.first).into(), walk: Walk::Seek },
                    });
                }
                Upkeep::Survey(Surveying::All { partition, mut walk }) => {
                    if partition >= self.model.centroids.len().max(1) as i64 {
                        let run = self.run.as_mut().ok_or_else(|| error("maintenance without a run"))?;
                        let survey = &mut run.survey;
                        survey.read.extend(0..self.model.centroids.len().max(1) as i64);
                        survey.read.extend(survey.tails.keys().chain(survey.parts.keys()).copied());
                        let read: Vec<i64> = survey.read.iter().copied().collect();
                        survey.vectors = Some(read.iter().map(|p| survey.live(*p)).sum());
                        match self.next_job()? {
                            Some(upkeep) => self.upkeep = upkeep,
                            None => return Ok(IOResult::Done(())),
                        }
                        continue;
                    }
                    let (start, end) = (part_key(partition)?, partition_key(partition, BLOCK_ROW, 0)?);
                    let row = match walk_held(&mut self.stores[PARTITIONS], start, end, &mut walk)? {
                        IOResult::IO(io) => {
                            self.upkeep = Upkeep::Survey(Surveying::All { partition, walk });
                            return Ok(IOResult::IO(io));
                        }
                        IOResult::Done(row) => row,
                    };
                    let run = self.run.as_mut().ok_or_else(|| error("maintenance without a run"))?;
                    match row {
                        Some(key) => {
                            surveyed();
                            surveyed_row(&mut run.survey, &mut self.stores[PARTITIONS], key, kind_of(key))?;
                            self.upkeep = Upkeep::Survey(Surveying::All { partition, walk });
                        }
                        None => self.upkeep = Upkeep::Survey(Surveying::All { partition: partition + 1, walk: Walk::Seek }),
                    }
                }
                Upkeep::Survey(Surveying::Some { mut pending, mut walk }) => {
                    let Some(&partition) = pending.front() else {
                        match self.next_job()? {
                            Some(upkeep) => self.upkeep = upkeep,
                            None => return Ok(IOResult::Done(())),
                        }
                        continue;
                    };
                    let run = self.run.as_mut().ok_or_else(|| error("maintenance without a run"))?;
                    if matches!(walk, Walk::Seek) {
                        run.survey.forget(partition);
                    }
                    let (start, end) = (part_key(partition)?, partition_key(partition, BLOCK_ROW, 0)?);
                    let row = match walk_held(&mut self.stores[PARTITIONS], start, end, &mut walk)? {
                        IOResult::IO(io) => {
                            self.upkeep = Upkeep::Survey(Surveying::Some { pending, walk });
                            return Ok(IOResult::IO(io));
                        }
                        IOResult::Done(row) => row,
                    };
                    // A partition's tail is counted up to a bound: past it, a pack is due whatever
                    // the count, and walking a long tail once per job would make a backlog's
                    // maintenance quadratic.
                    let counted = row.is_some_and(|key| kind_of(key) == TAIL_ROW) && run.survey.tail(partition) >= SURVEYED_TAIL;
                    match row.filter(|_| !counted) {
                        Some(key) => {
                            surveyed();
                            surveyed_row(&mut run.survey, &mut self.stores[PARTITIONS], key, kind_of(key))?;
                        }
                        None => {
                            run.survey.read.insert(partition);
                            let part = run.survey.part(partition);
                            let mut shared = self.shared.lock().expect("index state");
                            match part.moving() || part.state != State::Live && run.survey.tail(partition) > 0 {
                                true => shared.moving.insert(partition),
                                false => shared.moving.remove(&partition),
                            };
                            drop(shared);
                            pending.pop_front();
                            walk = Walk::Seek;
                        }
                    }
                    self.upkeep = Upkeep::Survey(Surveying::Some { pending, walk });
                }
                Upkeep::Read { job, part, stage, mut walk, mut get, mut rows } => {
                    let partition = job_partition(job);
                    let current = self.model.version(partition);
                    match (job, stage) {
                        // A pack starts from the open block, when it holds entries of the current
                        // centroid.
                        (Job::Pack(_), Stage::Blocks) => {
                            if part.open > 0 {
                                let key = block_key(partition, part.next - 1)?;
                                let found = match get_row(&mut self.stores[PARTITIONS], key, &mut get)? {
                                    IOResult::Done(found) => found,
                                    IOResult::IO(io) => {
                                        self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                        return Ok(IOResult::IO(io));
                                    }
                                };
                                if let Some(found) = found.filter(|r| integer(r, 0).is_ok_and(|v| v == current)) {
                                    rows.extend(unpack(bytes_at(&found, 1)?)?.into_iter().map(|entry| Tailed { location: Location::Block(key), version: current, entry }));
                                }
                            }
                            self.upkeep = Upkeep::Read { job, part, stage: Stage::Tail, walk: Walk::Seek, get: Get::Seek, rows };
                        }
                        // A move checks the next block marked for it, and a retiring partition's
                        // tail once its blocks are done.
                        (Job::Move(_), Stage::Blocks) if part.moving() => {
                            let key = block_key(partition, part.move_next)?;
                            let found = match get_row(&mut self.stores[PARTITIONS], key, &mut get)? {
                                IOResult::Done(found) => found,
                                IOResult::IO(io) => {
                                    self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                    return Ok(IOResult::IO(io));
                                }
                            };
                            let mut sorted = Sorted::default();
                            if let Some(found) = found {
                                let version = integer(&found, 0)?;
                                sorted.version = version;
                                rows.extend(unpack(bytes_at(&found, 1)?)?.into_iter().map(|entry| Tailed { location: Location::Block(key), version, entry }));
                            }
                            self.upkeep = Upkeep::Check { job, part, rows, at: 0, get: Get::Seek, sorted };
                        }
                        (Job::Move(_), Stage::Blocks) => self.upkeep = Upkeep::Read { job, part, stage: Stage::Tail, walk: Walk::Seek, get: Get::Seek, rows },
                        (Job::Compact(key), Stage::Blocks) => {
                            let found = match get_row(&mut self.stores[PARTITIONS], key, &mut get)? {
                                IOResult::Done(found) => found,
                                IOResult::IO(io) => {
                                    self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                    return Ok(IOResult::IO(io));
                                }
                            };
                            let mut sorted = Sorted::default();
                            if let Some(found) = found {
                                let version = integer(&found, 0)?;
                                sorted.version = version;
                                rows.extend(unpack(bytes_at(&found, 1)?)?.into_iter().map(|entry| Tailed { location: Location::Block(key), version, entry }));
                            }
                            self.upkeep = Upkeep::Check { job, part, rows, at: 0, get: Get::Seek, sorted };
                        }
                        (Job::Retire(_), _) => {
                            let vectors = Vec::new();
                            self.upkeep = Upkeep::Fetch { job, part, sorted: Sorted::default(), vectors };
                        }
                        (Job::Split(_), Stage::Blocks) => {
                            let (start, end) = rows_of(partition, BLOCK_ROW)?;
                            match walk_row(&mut self.stores[PARTITIONS], start, end, &mut walk)? {
                                IOResult::IO(io) => {
                                    self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                    return Ok(IOResult::IO(io));
                                }
                                IOResult::Done(Some((key, found))) => {
                                    let version = integer(&found, 0)?;
                                    rows.extend(unpack(bytes_at(&found, 1)?)?.into_iter().map(|entry| Tailed { location: Location::Block(key), version, entry }));
                                    self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                }
                                IOResult::Done(None) => self.upkeep = Upkeep::Read { job, part, stage: Stage::Tail, walk: Walk::Seek, get, rows },
                            }
                        }
                        (Job::Rebuild, _) => return Err(error("a rebuild reads through its own stage")),
                        (_, Stage::Tail) => {
                            let (start, end) = rows_of(partition, TAIL_ROW)?;
                            let full = !matches!(job, Job::Split(_)) && rows.iter().filter(|r| matches!(r.location, Location::Tail(_))).count() >= MOVE_BATCH;
                            let row = if full {
                                IOResult::Done(None)
                            } else {
                                walk_row(&mut self.stores[PARTITIONS], start, end, &mut walk)?
                            };
                            match row {
                                IOResult::IO(io) => {
                                    self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                    return Ok(IOResult::IO(io));
                                }
                                IOResult::Done(Some((key, found))) => {
                                    let version = integer(&found, 0)?;
                                    let entry = unpack(bytes_at(&found, 1)?)?.pop().ok_or_else(|| error("empty tail entry"))?;
                                    rows.push(Tailed { location: Location::Tail(key), version, entry });
                                    self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                }
                                IOResult::Done(None) => {
                                    if let Job::Split(_) = job {
                                        // The split's centroids come from a spread sample of its members.
                                        let members = rows.len() as i64;
                                        let step = (rows.len() / SPLIT_SAMPLE).max(1);
                                        rows = rows.into_iter().step_by(step).collect();
                                        let neighbours = self.neighbours(partition);
                                        self.upkeep = Upkeep::Neighbours { job, part, rows, members, neighbours, at: 0, get: Get::Seek, parts: Vec::new() };
                                        continue;
                                    }
                                    self.upkeep = Upkeep::Check { job, part, rows, at: 0, get: Get::Seek, sorted: Sorted::default() };
                                }
                            }
                        }
                    }
                }
                Upkeep::Neighbours { job, part, rows, members, neighbours, at, mut get, mut parts } => {
                    let Some(&neighbour) = neighbours.get(at) else {
                        let sorted = Sorted { neighbours: parts, members, ..Sorted::default() };
                        self.upkeep = Upkeep::Check { job, part, rows, at: 0, get: Get::Seek, sorted };
                        continue;
                    };
                    let found = match get_row(&mut self.stores[PARTITIONS], part_key(neighbour)?, &mut get)? {
                        IOResult::IO(io) => {
                            self.upkeep = Upkeep::Neighbours { job, part, rows, members, neighbours, at, get, parts };
                            return Ok(IOResult::IO(io));
                        }
                        IOResult::Done(found) => found,
                    };
                    parts.push((neighbour, found.as_ref().map(Part::parse).transpose()?.unwrap_or_default()));
                    self.upkeep = Upkeep::Neighbours { job, part, rows, members, neighbours, at: at + 1, get: Get::Seek, parts };
                }
                Upkeep::Check { job, part, rows, at, mut get, mut sorted } => {
                    let Some(row) = rows.get(at) else {
                        self.upkeep = Upkeep::Unbury { job, part, sorted, at: 0, get: Get::Seek };
                        continue;
                    };
                    let found = match get_row(&mut self.stores[VECTORS], row.entry.rowid, &mut get)? {
                        IOResult::Done(found) => found,
                        IOResult::IO(io) => {
                            self.upkeep = Upkeep::Check { job, part, rows, at, get, sorted };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    if !matches!(job, Job::Split(_)) && !sorted.removed.contains(&row.location) {
                        sorted.removed.push(row.location);
                    }
                    if !matches!(job, Job::Split(_)) && matches!(row.location, Location::Block(_)) {
                        sorted.from_blocks += 1;
                    }
                    match found {
                        // An entry its vector's row no longer names is stale, and goes with the rows
                        // the job replaces.
                        Some(r) if Location::decode(integer(&r, 0)?) == row.location => {
                            match job {
                                Job::Pack(p) if row.version == self.model.version(p) => {
                                    if let Location::Block(key) = row.location {
                                        sorted.kept.push((row.entry.rowid, key));
                                    }
                                    sorted.live.push(row.entry.clone());
                                }
                                // A move needs each entry, to keep it where it is when it stays, and its vector,
                                // to see where it belongs.
                                Job::Move(_) => {
                                    sorted.live.push(row.entry.clone());
                                    sorted.recode.push(row.entry.rowid);
                                }
                                // A compaction keeps each live entry as it is.
                                Job::Compact(_) => sorted.live.push(row.entry.clone()),
                                _ => sorted.recode.push(row.entry.rowid),
                            }
                        }
                        _ => {
                            DROPPED.fetch_add(1, Ordering::Relaxed);
                            // A pack, move or compaction rewrites the block the stale copy was read
                            // from, so the copy leaves it and its dead row must no longer name it.
                            if let (Job::Pack(_) | Job::Move(_) | Job::Compact(_), Location::Block(key)) = (job, row.location) {
                                sorted.buried.push((row.entry.rowid, key));
                            }
                        }
                    }
                    self.upkeep = Upkeep::Check { job, part, rows, at: at + 1, get: Get::Seek, sorted };
                }
                Upkeep::Unbury { job, part, mut sorted, at, mut get } => {
                    let Some(&(rowid, block)) = sorted.buried.get(at) else {
                        let vectors = Vec::with_capacity(sorted.recode.len());
                        self.upkeep = Upkeep::Fetch { job, part, sorted, vectors };
                        continue;
                    };
                    let key = dead_key(block, rowid)?;
                    let found = match get_row(&mut self.stores[PARTITIONS], key, &mut get)? {
                        IOResult::Done(found) => found,
                        IOResult::IO(io) => {
                            self.upkeep = Upkeep::Unbury { job, part, sorted, at, get };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    let found = found.ok_or_else(|| error("a stale copy has no dead row"))?;
                    let mut blocks = dead_blocks(bytes_at(&found, 0)?)?;
                    blocks.retain(|b| *b != block);
                    sorted.unbury.push(match blocks.is_empty() {
                        true => remove(PARTITIONS, key),
                        false => put(PARTITIONS, key, dead_record(&blocks)?),
                    });
                    self.upkeep = Upkeep::Unbury { job, part, sorted, at: at + 1, get: Get::Seek };
                }
                Upkeep::Fetch { job, part, mut sorted, mut vectors } => {
                    let Some(&rowid) = sorted.recode.get(vectors.len()) else {
                        let unbury = std::mem::take(&mut sorted.unbury);
                        let mut steps = match job {
                            Job::Pack(p) => self.plan_pack(p, part, sorted, vectors)?,
                            Job::Split(p) => self.plan_split(p, part, sorted, &vectors)?,
                            Job::Move(p) => self.plan_move(p, part, sorted, vectors)?,
                            Job::Compact(block) => self.plan_compact(block, part, sorted)?,
                            Job::Retire(p) => self.plan_retire(p, part)?,
                            Job::Rebuild => return Err(error("a rebuild plans through its own stage")),
                        };
                        steps.extend(unbury);
                        self.upkeep = Upkeep::Apply { job, steps, undo: Vec::new() };
                        continue;
                    };
                    let main = self.main.as_mut().ok_or_else(|| error("table cursor must be open"))?;
                    match main.seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })? {
                        IOResult::Done(SeekResult::Found) => {}
                        IOResult::Done(_) => return Err(error("a vector's row names a row the table lacks")),
                        IOResult::IO(io) => {
                            self.upkeep = Upkeep::Fetch { job, part, sorted, vectors };
                            return Ok(IOResult::IO(io));
                        }
                    }
                    let found = match main.record()? {
                        IOResult::Done(found) => found.ok_or_else(|| error("table row absent"))?,
                        IOResult::IO(io) => {
                            self.upkeep = Upkeep::Fetch { job, part, sorted, vectors };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    let column = self.configuration.columns[0].pos_in_table;
                    let ValueRef::Blob(bytes) = found.get_value(column)? else {
                        return Err(error("table value is not a vector"));
                    };
                    vectors.push(dense(Some(bytes))?);
                    self.upkeep = Upkeep::Fetch { job, part, sorted, vectors };
                }
                Upkeep::Apply { job, mut steps, mut undo } => {
                    let Some(step) = steps.front_mut() else {
                        self.finish_job(job)?;
                        continue;
                    };
                    self.record(step);
                    match apply_step(&mut self.stores, step, Some(&mut undo)) {
                        Ok(IOResult::Done(())) => {
                            steps.pop_front();
                            self.upkeep = Upkeep::Apply { job, steps, undo };
                        }
                        Ok(IOResult::IO(io)) => {
                            self.upkeep = Upkeep::Apply { job, steps, undo };
                            return Ok(IOResult::IO(io));
                        }
                        Err(e) if conflicted(&e) => {
                            CONFLICTS.fetch_add(1, Ordering::Relaxed);
                            self.upkeep = Upkeep::Revert { undo };
                        }
                        Err(e) => return Err(e),
                    }
                }
                Upkeep::Rebuild { phase, mut steps } => {
                    // The steps a phase made are written before it goes on, and no walk is open
                    // while they are: a phase walks a store only to collect keys.
                    if let Some(step) = steps.front_mut() {
                        self.record(step);
                        if let IOResult::IO(io) = apply_step(&mut self.stores, step, None)? {
                            self.upkeep = Upkeep::Rebuild { phase, steps };
                            return Ok(IOResult::IO(io));
                        }
                        steps.pop_front();
                        self.upkeep = Upkeep::Rebuild { phase, steps };
                        continue;
                    }
                    match phase {
                        Rebuilding::Rowids { mut walk, mut rowids } => match walk_key(&mut self.stores[VECTORS], 0, i64::MAX, &mut walk)? {
                            IOResult::IO(io) => {
                                self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Rowids { walk, rowids }, steps };
                                return Ok(IOResult::IO(io));
                            }
                            IOResult::Done(Some(rowid)) => {
                                surveyed();
                                rowids.push(rowid);
                                self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Rowids { walk, rowids }, steps };
                            }
                            IOResult::Done(None) => self.upkeep = Upkeep::Rebuild { phase: sampling(rowids), steps },
                        },
                        Rebuilding::Sample { rowids, sample, mut vectors, mut get } => {
                            let Some(&rowid) = sample.get(vectors.len()) else {
                                let model = Box::new(self.rebuilt(&vectors, rowids.len()));
                                self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Clear { rowids, model, walk: Walk::Seek, keys: Vec::new() }, steps };
                                continue;
                            };
                            match self.table_vector(rowid, &mut get)? {
                                IOResult::IO(io) => {
                                    self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Sample { rowids, sample, vectors, get }, steps };
                                    return Ok(IOResult::IO(io));
                                }
                                IOResult::Done(vector) => vectors.push(vector),
                            }
                            self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Sample { rowids, sample, vectors, get: Get::Seek }, steps };
                        }
                        Rebuilding::Clear { rowids, model, mut walk, mut keys } => match walk_key(&mut self.stores[PARTITIONS], 0, i64::MAX, &mut walk)? {
                            IOResult::IO(io) => {
                                self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Clear { rowids, model, walk, keys }, steps };
                                return Ok(IOResult::IO(io));
                            }
                            IOResult::Done(Some(key)) => {
                                surveyed();
                                if kind_of(key) != PART_ROW {
                                    keys.push(key);
                                }
                                self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Clear { rowids, model, walk, keys }, steps };
                            }
                            IOResult::Done(None) => {
                                steps.extend(keys.into_iter().map(|key| remove(PARTITIONS, key)));
                                self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Code { rowids, model, at: 0, get: Get::Seek, stream: Stream::default(), batch: Vec::new() }, steps };
                            }
                        },
                        Rebuilding::Code { rowids, model, at, mut get, mut stream, mut batch } => {
                            if let Some(&rowid) = rowids.get(at + batch.len()).filter(|_| batch.len() < CODE_BATCH) {
                                match self.table_vector(rowid, &mut get)? {
                                    IOResult::IO(io) => {
                                        self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Code { rowids, model, at, get, stream, batch }, steps };
                                        return Ok(IOResult::IO(io));
                                    }
                                    IOResult::Done(vector) => batch.push(vector),
                                }
                                surveyed();
                                self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Code { rowids, model, at, get: Get::Seek, stream, batch }, steps };
                                continue;
                            }
                            if let Some(first) = batch.first() {
                                let rotation = self.rotation(first.len());
                                for (partition, version, entry) in encode_all(&model, &rotation, &rowids[at..at + batch.len()], &batch) {
                                    stream.add(partition, version, entry, &mut steps)?;
                                }
                                let at = at + batch.len();
                                batch.clear();
                                self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Code { rowids, model, at, get: Get::Seek, stream, batch }, steps };
                                continue;
                            }
                            stream.finish(&model, &mut steps)?;
                            for p in (0..model.centroids.len()).filter(|p| model.states[*p] == State::Live) {
                                steps.push_back(put(CENTROIDS, centroid_key(p as i64, model.versions[p])?, centroid_record(&model.centroids[p])?));
                            }
                            let next = generation();
                            steps.push_back(put(CENTROIDS, GENERATION, record(&[Value::from_i64(next)])?));
                            self.upkeep = Upkeep::Rebuild { phase: Rebuilding::Done { model, generation: next }, steps };
                        }
                        Rebuilding::Done { model, generation } => {
                            self.model = Arc::new(*model);
                            self.generation = Some(generation);
                            self.publish();
                            let mut shared = self.shared.lock().expect("index state");
                            shared.moving.clear();
                            shared.due.clear();
                            drop(shared);
                            REBUILDS.fetch_add(1, Ordering::Relaxed);
                            JOBS.fetch_add(1, Ordering::Relaxed);
                            self.saved = None;
                            let run = self.run.as_mut().ok_or_else(|| error("maintenance without a run"))?;
                            run.budget = run.budget.saturating_sub(1);
                            run.survey = Survey::default();
                            self.upkeep = Upkeep::Survey(Surveying::All { partition: 0, walk: Walk::Seek });
                        }
                    }
                }
                Upkeep::Revert { mut undo } => {
                    let Some(step) = undo.last_mut() else {
                        // Another writer holds a row this run needs; it maintains them itself.
                        self.forget_run();
                        self.loaded = true;
                        return Ok(IOResult::Done(()));
                    };
                    self.record(step);
                    match apply_step(&mut self.stores, step, None)? {
                        IOResult::Done(()) => {
                            undo.pop();
                            self.upkeep = Upkeep::Revert { undo };
                        }
                        IOResult::IO(io) => {
                            self.upkeep = Upkeep::Revert { undo };
                            return Ok(IOResult::IO(io));
                        }
                    }
                }
            }
        }
    }

    /// A vector of the indexed column of the table row `rowid`.
    fn table_vector(&mut self, rowid: i64, get: &mut Get) -> IOResultOr<Vec<f32>> {
        let main = self.main.as_mut().ok_or_else(|| error("table cursor must be open"))?;
        let found = match get_row(main, rowid, get)? {
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
            IOResult::Done(found) => found.ok_or_else(|| error("a vector's row names a row the table lacks"))?,
        };
        let ValueRef::Blob(bytes) = found.get_value(self.configuration.columns[0].pos_in_table)? else {
            return Err(error("table value is not a vector"));
        };
        Ok(IOResult::Done(dense(Some(bytes))?))
    }

    /// The model a rebuild trains for `vectors` vectors: k-means over `sample` for the partitions
    /// [`trained_partitions`] gives them, balanced, each at a version past any it had, partition 0
    /// past the untrained origin's. Every centroid the model held stays readable for entries coded
    /// against it, and slots past the trained ones are retired.
    fn rebuilt(&self, sample: &[Vec<f32>], vectors: usize) -> Model {
        let old = &self.model;
        let eta = sample.first().map_or(0.0, |v| self.eta(v.len()));
        let centroids = balance(sample, kmeans(sample, trained_partitions(vectors), TRAIN_ITERATIONS, eta), vectors, eta);
        let trained = centroids.len();
        let mut model = Model { older: old.older.clone(), eta, ..Model::default() };
        for (p, c) in old.centroids.iter().enumerate() {
            model.older.insert((p as i64, old.versions[p]), c.clone());
        }
        for p in 0..trained.max(old.centroids.len()) {
            let (centroid, version, state) = match centroids.get(p) {
                Some(c) => (c.clone(), if p < old.centroids.len() || p == 0 { old.version(p as i64) + 1 } else { 0 }, State::Live),
                None => (old.centroids[p].clone(), old.versions[p], State::Retired),
            };
            model.squares.push(dot32(&centroid, &centroid));
            model.tiles.set(p, &centroid);
            model.centroids.push(centroid);
            model.versions.push(version);
            model.states.push(state);
        }
        model
    }

    /// The job is written: count it, and survey again the partitions it changed before choosing
    /// the next.
    fn finish_job(&mut self, job: Job) -> Result<()> {
        JOBS.fetch_add(1, Ordering::Relaxed);
        self.saved = None;
        let run = self.run.as_mut().ok_or_else(|| error("maintenance without a run"))?;
        run.budget = run.budget.saturating_sub(1);
        let mut dirty = std::mem::take(&mut self.dirty);
        dirty.push(job_partition(job));
        dirty.sort_unstable();
        dirty.dedup();
        if std::env::var_os("IVF_RQ_TRACE").is_some() {
            eprintln!("upkeep: {} partitions, job {job:?}, dirty {dirty:?}", self.model.centroids.len());
        }
        self.upkeep = Upkeep::Survey(Surveying::Some { pending: dirty.into(), walk: Walk::Seek });
        Ok(())
    }

    /// The next job of the run, or none, ending the run, when its budget is spent or nothing is due.
    fn next_job(&mut self) -> Result<Option<Upkeep>> {
        let run = self.run.as_ref().ok_or_else(|| error("maintenance without a run"))?;
        let job = if run.budget == 0 { None } else { self.choose(&run.survey) };
        let Some(job) = job else {
            // The partitions the run read that it leaves work due in, its jobs spent, are the next
            // statements' to survey; those it leaves none in no longer are.
            let settled = self.settled(&run.survey);
            let read: Vec<(i64, bool)> = run.survey.read.iter().map(|p| (*p, self.choose_among(&run.survey, &BTreeSet::from([*p]), settled).is_some())).collect();
            let mut shared = self.shared.lock().expect("index state");
            for (p, due) in read {
                match due {
                    true => shared.due.insert(p),
                    false => shared.due.remove(&p),
                };
            }
            drop(shared);
            self.run = None;
            return Ok(None);
        };
        if let Job::Rebuild = job {
            self.save_model();
            return Ok(Some(Upkeep::Rebuild { phase: Rebuilding::Rowids { walk: Walk::Seek, rowids: Vec::new() }, steps: VecDeque::new() }));
        }
        let part = run.survey.part(job_partition(job));
        Ok(Some(Upkeep::Read { job, part, stage: Stage::Blocks, walk: Walk::Seek, get: Get::Seek, rows: Vec::new() }))
    }

    /// The job the survey's partitions most need, in order: a move (a block marked moving, or a
    /// retiring partition's tail), a retirement whose moves are done, a pack of the longest tail of
    /// at least [`PACK_AT`], a compaction of the block holding the most stale copies, at least
    /// [`COMPACT_AT`], the retirement of a partition too small to keep, a split of the largest
    /// partition past the bound.
    fn choose(&self, survey: &Survey) -> Option<Job> {
        if let Some(vectors) = survey.vectors {
            if vectors as usize >= BULK_AT && 4 * self.model.live() < trained_partitions(vectors as usize) {
                return Some(Job::Rebuild);
            }
        }
        self.choose_among(survey, &survey.read, self.settled(survey))
    }

    /// Whether no partition is moving: none the survey read, and none a run has found moving.
    fn settled(&self, survey: &Survey) -> bool {
        survey.parts.values().all(|part| !part.moving()) && self.shared.lock().expect("index state").moving.is_empty()
    }

    /// The job [`Self::choose`] picks among `partitions`, which the survey read: a partition too
    /// small to keep is retired only once no partition is moving, which `settled` says.
    fn choose_among(&self, survey: &Survey, partitions: &BTreeSet<i64>, settled: bool) -> Option<Job> {
        let read = || partitions.iter().copied();
        let live = |p: i64| self.model.state(p) == State::Live && survey.part(p).state == State::Live;
        // A tail long enough to stop a survey's count is packed before anything else: queries
        // read a tail entry by entry, and moves keep adding to their targets' tails.
        if let Some(p) = read().filter(|p| live(*p) && survey.tail(*p) >= SURVEYED_TAIL).max_by_key(|p| (survey.tail(*p), -*p)) {
            return Some(Job::Pack(p));
        }
        if let Some(p) = read().find(|p| survey.part(*p).moving() || survey.part(*p).state != State::Live && survey.tail(*p) > 0) {
            return Some(Job::Move(p));
        }
        if let Some(p) = read().find(|p| survey.part(*p).state == State::Retiring) {
            return Some(Job::Retire(p));
        }
        if let Some(p) = read().filter(|p| live(*p) && survey.tail(*p) >= PACK_AT).max_by_key(|p| (survey.tail(*p), -*p)) {
            return Some(Job::Pack(p));
        }
        if let Some((block, _)) = read().flat_map(|p| survey.stale_in(p)).filter(|(block, count)| *count >= COMPACT_AT && live(partition_of(*block))).max_by_key(|(block, count)| (*count, -*block)) {
            return Some(Job::Compact(block));
        }
        if settled && self.model.live() > 1 {
            if let Some(p) = read().find(|p| live(*p) && survey.live(*p) < self.retire_bound()) {
                return Some(Job::Retire(p));
            }
        }
        let size = |p: i64| survey.part(p).size + survey.tail(p) as i64;
        let bound = self.split_bound();
        read().filter(|p| live(*p) && size(*p) >= bound.max(survey.part(*p).retry)).max_by_key(|p| (size(*p), -*p)).map(Job::Split)
    }

    /// The live partitions nearest a partition's current centroid, which a split of it marks moving.
    fn neighbours(&self, partition: i64) -> Vec<i64> {
        let Some(centre) = self.model.centroids.get(partition as usize) else {
            return Vec::new();
        };
        let mut near: Vec<(f32, i64)> = (0..self.model.centroids.len() as i64)
            .filter(|p| *p != partition && self.model.state(*p) == State::Live)
            .map(|p| (l2(centre, &self.model.centroids[p as usize]), p))
            .collect();
        near.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        near.into_iter().take(NEIGHBOURS).map(|(_, p)| p).collect()
    }

    /// Merge a partition's current tail entries into its open block and write them as blocks from
    /// there on, give the entries coded against a superseded centroid fresh tail entries from their
    /// vectors, and remove every tail entry read. The partition's bookkeeping is written first: it
    /// claims the partition.
    fn plan_pack(&mut self, partition: i64, mut part: Part, sorted: Sorted, vectors: Vec<Vec<f32>>) -> Result<VecDeque<Step>> {
        let mut steps = removals(&sorted.removed);
        let mut live = sorted.live;
        live.sort_unstable_by_key(|e| e.rowid);
        let version = self.model.version(partition);
        // Entries already in the open block keep their row when they land in it again.
        let opened_here: std::collections::HashSet<(i64, i64)> = sorted.kept.iter().copied().collect();
        if sorted.removed.iter().any(|l| matches!(l, Location::Block(_))) {
            part.next -= 1;
            part.size -= sorted.from_blocks;
        }
        part.open = 0;
        for chunk in live.chunks(BLOCK) {
            let key = block_key(partition, part.next)?;
            steps.push_back(put(PARTITIONS, key, record(&[Value::from_i64(version), blob(&pack(chunk)?)?])?));
            for e in chunk {
                if !opened_here.contains(&(e.rowid, key)) {
                    steps.push_back(put(VECTORS, e.rowid, record(&[Value::from_i64(Location::Block(key).encode())])?));
                }
            }
            part.next += 1;
            part.open = if chunk.len() < BLOCK { chunk.len() as i64 } else { 0 };
        }
        part.size += live.len() as i64;
        for (rowid, vector) in sorted.recode.iter().zip(&vectors) {
            let (target, written) = self.tail_steps(*rowid, vector)?;
            steps.extend(written);
            self.dirty.push(target);
        }
        RECODED.fetch_add(sorted.recode.len(), Ordering::Relaxed);
        PACKS.fetch_add(1, Ordering::Relaxed);
        steps.push_front(part_step(partition, part)?);
        Ok(steps)
    }

    /// The first half of a split: 2-means over a sample of the partition's members gives it a new
    /// centroid version and opens a partition in the first retired slot, or past the last. Only
    /// centroids and bookkeeping are written: the partition and its nearest live neighbours are
    /// marked moving, and moves recode their entries block by block. A split whose smaller half
    /// would take under a sixteenth of the sample is not made; the partition is tried again once
    /// it has doubled. The generation is written first: it claims the model.
    fn plan_split(&mut self, partition: i64, mut part: Part, sorted: Sorted, vectors: &[Vec<f32>]) -> Result<VecDeque<Step>> {
        let started = std::time::Instant::now();
        let eta = vectors.first().map_or(0.0, |v| self.eta(v.len()));
        let Some([kept, split]) = halves(vectors, eta) else {
            part.retry = 2 * sorted.members.max(self.split_bound());
            return Ok(VecDeque::from([part_step(partition, part)?]));
        };
        self.save_model();
        // The untrained origin is partition 0's version 0, so its first centroid is version 1.
        let version = self.model.version(partition) + 1;
        let reused = (0..self.model.centroids.len()).find(|p| self.model.states[*p] == State::Retired);
        let opened = reused.unwrap_or_else(|| self.model.centroids.len().max(1)) as i64;
        let opened_version = reused.map_or(0, |p| self.model.versions[p] + 1);
        let next = generation();
        part.move_next = 0;
        part.move_end = part.next;
        part.open = 0;
        part.retry = 0;
        let mut steps = VecDeque::from([
            put(CENTROIDS, GENERATION, record(&[Value::from_i64(next)])?),
            put(CENTROIDS, centroid_key(partition, version)?, centroid_record(&kept)?),
            put(CENTROIDS, centroid_key(opened, opened_version)?, centroid_record(&split)?),
            part_step(partition, part)?,
        ]);
        // A retired slot holds no block: its retirement moved every entry out and removed them.
        steps.push_back(part_step(opened, Part::default())?);
        for (neighbour, mut near) in sorted.neighbours {
            if neighbour == opened {
                continue;
            }
            near.move_next = 0;
            near.move_end = near.next;
            steps.push_back(part_step(neighbour, near)?);
            self.dirty.push(neighbour);
        }
        let model = Arc::make_mut(&mut self.model);
        model.eta = eta;
        model.place(partition as usize, version, kept);
        model.place(opened as usize, opened_version, split);
        model.states[opened as usize] = State::Live;
        self.generation = Some(next);
        self.publish();
        self.dirty.push(opened);
        let micros = started.elapsed().as_micros() as usize;
        SPLITS.fetch_add(1, Ordering::Relaxed);
        SPLIT_MICROS.fetch_add(micros, Ordering::Relaxed);
        SPLIT_MICROS_MAX.fetch_max(micros, Ordering::Relaxed);
        Ok(steps)
    }

    /// Check one block marked moving, or a batch of a retiring partition's tail: an entry now
    /// nearest another live partition is recoded into that partition's tail, and one that stays is
    /// recoded against the partition's current centroid when its block was coded against an
    /// earlier one. A block nothing leaves and nothing changes in is not rewritten. The partition's
    /// bookkeeping is written first: it claims the partition.
    fn plan_move(&mut self, partition: i64, mut part: Part, sorted: Sorted, vectors: Vec<Vec<f32>>) -> Result<VecDeque<Step>> {
        let current = self.model.version(partition);
        let from_block = part.moving();
        let key = block_key(partition, part.move_next)?;
        if from_block {
            part.move_next += 1;
        }
        let rotation = vectors.first().map(|v| self.rotation(v.len()));
        let mut steps = VecDeque::new();
        let mut staying = Vec::new();
        let mut recoded = 0;
        let mut moved = 0;
        for (entry, vector) in sorted.live.into_iter().zip(&vectors) {
            let nearest = self.model.nearest(vector) as i64;
            if from_block && nearest == partition && self.model.state(partition) == State::Live {
                if sorted.version == current {
                    staying.push(entry);
                    continue;
                }
                let centroid = self.model.centroids.get(partition as usize).ok_or_else(|| error("a live partition has no centroid"))?;
                let residual: Vec<f32> = vector.iter().zip(centroid).map(|(a, b)| a - b).collect();
                let rotation = rotation.as_ref().ok_or_else(|| error("a recoded entry has no vector"))?;
                let fresh = code(rotation, entry.rowid, residual);
                steps.push_back(put(VECTORS, fresh.rowid, record(&[Value::from_i64(Location::Block(key).encode())])?));
                staying.push(fresh);
                recoded += 1;
            } else {
                let (target, written) = self.tail_steps(entry.rowid, vector)?;
                steps.extend(written);
                self.dirty.push(target);
                moved += 1;
            }
        }
        if from_block {
            let dropped = sorted.from_blocks - staying.len() as i64 - moved;
            if moved + dropped + recoded > 0 {
                steps.push_front(match staying.is_empty() {
                    true => remove(PARTITIONS, key),
                    false => put(PARTITIONS, key, record(&[Value::from_i64(current), blob(&pack(&staying)?)?])?),
                });
                part.size = (part.size - moved - dropped).max(0);
            }
        } else {
            steps.extend(removals(&sorted.removed));
        }
        MOVES.fetch_add(1, Ordering::Relaxed);
        RECODED.fetch_add((moved + recoded) as usize, Ordering::Relaxed);
        steps.push_front(part_step(partition, part)?);
        Ok(steps)
    }

    /// Rewrite a block without the stale copies it holds, or remove it when none of its entries is
    /// live. The live entries stay as they are, under the centroid version they were coded against,
    /// so their vectors' rows still name the block. The partition's bookkeeping is written first: it
    /// claims the partition.
    fn plan_compact(&mut self, block: i64, mut part: Part, sorted: Sorted) -> Result<VecDeque<Step>> {
        if sorted.buried.is_empty() {
            return Err(*error("a dead row names a block that holds no stale copy"));
        }
        let partition = partition_of(block);
        let staying = sorted.live;
        let mut steps = VecDeque::from([match staying.is_empty() {
            true => remove(PARTITIONS, block),
            false => put(PARTITIONS, block, record(&[Value::from_i64(sorted.version), blob(&pack(&staying)?)?])?),
        }]);
        part.size = (part.size - sorted.buried.len() as i64).max(0);
        // The open block is the partition's last; it stays open with the entries it keeps.
        if part.open > 0 && block == block_key(partition, part.next - 1)? {
            part.open = staying.len() as i64;
        }
        COMPACTS.fetch_add(1, Ordering::Relaxed);
        steps.push_front(part_step(partition, part)?);
        Ok(steps)
    }

    /// Start retiring a live partition: no entry is coded into it any longer, and its blocks are
    /// marked moving, so moves recode its entries into the live partitions nearest them while
    /// queries still probe it. A retiring partition whose moves are done is retired: emptied, never
    /// probed, its slot free for the next split. The generation is written first: it claims the
    /// model, which every cursor reloads to learn the partition's state.
    fn plan_retire(&mut self, partition: i64, mut part: Part) -> Result<VecDeque<Step>> {
        self.save_model();
        let state = match part.state {
            State::Live => {
                part.move_next = 0;
                part.move_end = part.next;
                State::Retiring
            }
            _ => {
                part.size = 0;
                part.open = 0;
                State::Retired
            }
        };
        part.state = state;
        let next = generation();
        Arc::make_mut(&mut self.model).states[partition as usize] = state;
        self.generation = Some(next);
        self.publish();
        RETIRES.fetch_add(1, Ordering::Relaxed);
        Ok(VecDeque::from([put(CENTROIDS, GENERATION, record(&[Value::from_i64(next)])?), part_step(partition, part)?]))
    }
}

/// The write of a partition's bookkeeping, under a fresh version.
fn part_step(partition: i64, part: Part) -> Result<Step> {
    Ok(put(PARTITIONS, part_key(partition)?, Part { version: generation(), ..part }.record()?))
}

/// The steps that remove the blocks and tail entries a job read.
fn removals(removed: &[Location]) -> VecDeque<Step> {
    removed
        .iter()
        .map(|l| match l {
            Location::Block(key) => remove(PARTITIONS, *key),
            Location::Tail(key) => remove(PARTITIONS, *key),
        })
        .collect()
}

fn job_partition(job: Job) -> i64 {
    match job {
        Job::Pack(p) | Job::Split(p) | Job::Move(p) | Job::Retire(p) => p,
        Job::Compact(block) => partition_of(block),
        Job::Rebuild => 0,
    }
}

/// Estimate a block's entries from their 1-bit codes, each group in one fast-scan pass (a sparse
/// group, a tail entry above all, summed exactly), and score from its 7-bit code, where the block
/// lies, each entry whose estimate raised by its error bound reaches the bar of the `keep` best
/// 7-bit estimates so far. A stale copy the partition's dead rows name is skipped before it can
/// raise the bar.
fn score_block(query: &mut Query, payload: &[u8], centre: f64, source: Location) -> Result<()> {
    let dims = query.rotated.len();
    let count = block_count(payload)?;
    let layout = Layout::new(count, dims / 8);
    if payload.len() != layout.len() {
        return Err(*error("block length differs from its count"));
    }
    let screened = match source {
        Location::Block(key) if !query.dead.is_empty() => Some(key),
        _ => None,
    };
    let mut bound = Bound { centre, step: query.scan.step, base: query.scan.base, error: query.scan.error, sum: query.sum, spread: query.spread, bar: query.bar() };
    let (mut sums, mut gains, mut slacks) = ([0u32; GROUP], [0f32; GROUP], [0f32; GROUP]);
    for group in 0..layout.groups() {
        let first = group * GROUP;
        let codes = &payload[layout.scan() + group * layout.group_bytes()..][..layout.group_bytes()];
        let members = GROUP.min(count - first);
        tally(&SCANNED, members);
        let scanned = members >= SCAN_MEMBERS;
        for slot in 0..members {
            gains[slot] = f32_at(payload, layout.gains() + 4 * (first + slot));
            slacks[slot] = f32_at(payload, layout.slacks() + 4 * (first + slot));
        }
        let present = u32::MAX >> (GROUP - members);
        // A scanned group's members are tested together against the bar the group starts with;
        // those that reach it are tested again against the bar as it rises.
        let mut left = present;
        if scanned {
            scan_group(codes, &query.scan.bytes, &mut sums);
            left &= reaching(&sums, &gains, &slacks, &bound);
        }
        let mut ahead = left;
        while ahead != 0 {
            let n = first + ahead.trailing_zeros() as usize;
            ahead &= ahead - 1;
            prefetch(&payload[layout.rowids() + 8 * n..][..8]);
            prefetch(&payload[layout.extras() + n * layout.extra_bytes()..][..layout.extra_bytes()]);
        }
        let exact = |slot: usize| {
            #[cfg(test)]
            EXACT_SUMS.with(|count| count.set(count.get() + 1));
            exact_sum(codes, &query.scan.exact, slot)
        };
        while left != 0 {
            let slot = left.trailing_zeros() as usize;
            left &= left - 1;
            let n = first + slot;
            let (gain, slack) = (gains[slot] as f64, slacks[slot] as f64);
            let reach = bound.spread * slack;
            // A scanned member's fast-scan sum lies within the quantization's error of its exact
            // sum: a member that clears the bar by more than twice that error, in the estimate's
            // terms, can't be dropped by the exact test, and its fast-scan sum serves its 7-bit
            // estimate; one nearer the bar, or in a group too small to scan, is summed exactly.
            let signed = match scanned {
                true => {
                    let raised = bound.raised(gain, slack, sums[slot]);
                    if raised < bound.bar {
                        continue;
                    }
                    (raised - 4.0 * gain * bound.error >= bound.bar).then(|| bound.step * sums[slot] as f64 + bound.base)
                }
                false => Some(exact(slot)),
            };
            let (signed, tested) = match signed {
                Some(signed) if scanned => (Some(signed), true),
                signed => (signed, false),
            };
            if !tested && signed.is_some_and(|signed| centre + gain * (2.0 * signed - bound.sum) + reach < bound.bar) {
                continue;
            }
            tally(&CANDIDATES, 1);
            let rowid = i64_at(payload, layout.rowids() + 8 * n);
            if screened.is_some_and(|key| query.dead.contains(&(rowid, key))) {
                continue;
            }
            let signed = match signed {
                Some(signed) => signed,
                None => {
                    let signed = exact(slot);
                    if centre + gain * (2.0 * signed - bound.sum) + reach < bound.bar {
                        continue;
                    }
                    signed
                }
            };
            tally(&REFINED, 1);
            #[cfg(test)]
            QUERY_REFINES.with(|count| count.set(count.get() + 1));
            let extra = &payload[layout.extras() + n * layout.extra_bytes()..][..layout.extra_bytes()];
            let lower = lower_product(&query.rotated, &extra[4..]);
            let estimate = centre + f32_at(extra, 0) as f64 * refined_product(signed, lower, query.sum);
            query.estimates.push(Ordered(-estimate, rowid));
            if query.estimates.len() > query.keep {
                query.estimates.pop();
            }
            bound.bar = query.bar();
        }
    }
    Ok(())
}

/// Self-checks of the codes and the scan, run by the layout bench (the vendored crate has no test build).
#[doc(hidden)]
#[cfg(test)]
mod tests {
    use super::*;

    fn random(state: &mut u64, dims: usize) -> Vec<f32> {
        (0..dims)
            .map(|_| {
                *state ^= *state << 13;
                *state ^= *state >> 7;
                *state ^= *state << 17;
                (*state >> 11) as f32 / (1u64 << 53) as f32 - 0.5
            })
            .collect()
    }

    fn entries(count: usize, dims: usize) -> (Rotation, Vec<Vec<f32>>, Vec<Entry>) {
        let rotation = Rotation::new(dims);
        let model = Model::default();
        let mut state = 7;
        let vectors: Vec<Vec<f32>> = (0..count).map(|_| random(&mut state, dims)).collect();
        let coded = vectors.iter().enumerate().map(|(n, v)| model.encode(&rotation, n as i64, v).2).collect();
        (rotation, vectors, coded)
    }

    fn query(q: &[f32], rotation: &Rotation, limit: usize, keep: usize) -> Query {
        let rotated = rotation.apply(q);
        let length = rotated.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
        Query {
            vector: q.to_vec(),
            asked: Vector::from_f32(q.to_vec()),
            sum: rotated.iter().map(|x| *x as f64).sum(),
            spread: EPSILON * length / ((rotated.len() - 1) as f64).sqrt(),
            scan: ScanTable::new(&rotated),
            rotated,
            wanted: limit,
            limit,
            keep,
            centres: HashMap::new(),
            dead: HashSet::new(),
            estimates: BinaryHeap::new(),
            results: BinaryHeap::new(),
        }
    }

    #[test]
    fn a_block_unpacks_to_the_codes_it_packed() {
        let (_, _, coded) = entries(45, 768);
        let back = unpack(&pack(&coded).unwrap()).unwrap();
        assert_eq!(back.len(), 45);
        for (a, b) in coded.iter().zip(&back) {
            assert_eq!((a.rowid, a.gain, a.slack, &a.bits), (b.rowid, b.gain, b.slack, &b.bits));
            assert_eq!((a.scale, &a.ex), (b.scale, &b.ex), "the 7-bit parts of entry {}", a.rowid);
        }
    }

    /// A query scores entries' 7-bit codes from the blocks and tail entries it read: past the
    /// model's generation it makes no point read, however many entries it refines, in a packed
    /// index with a tail and stale copies, and its answer is the exact top ten.
    #[test]
    fn a_query_refines_its_candidates_without_a_point_read() {
        use crate::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect};
        const DIMS: usize = 64;
        for mvcc in [false, true] {
            let db = Database::open_file_with_flags(
                Arc::new(MemoryIO::new()),
                &format!("ivf-rq-no-point-reads-{mvcc}.db"),
                OpenFlags::default(),
                DatabaseOpts::new().with_index_method(true),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            if mvcc {
                conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
            }
            conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
            let mut state = 41;
            let mut live: HashMap<i64, Vec<f32>> = HashMap::new();
            for id in 1..=600i64 {
                let v = random(&mut state, DIMS);
                conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
                live.insert(id, v);
            }
            for _ in 0..30 {
                conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            }
            // Stale copies in packed blocks, and fresh entries in the tails.
            for id in (3..=600i64).step_by(3) {
                let v = random(&mut state, DIMS);
                conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v))).unwrap();
                live.insert(id, v);
            }
            for id in (10..=600i64).step_by(10) {
                conn.execute(format!("DELETE FROM v WHERE id = {id}")).unwrap();
                live.remove(&id);
            }
            for _ in 0..3 {
                let q = random(&mut state, DIMS);
                let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&q));
                let (reads, refines) = (POINT_READS.with(|c| c.get()), QUERY_REFINES.with(|c| c.get()));
                let got = ids(&conn, &sql);
                let reads = POINT_READS.with(|c| c.get()) - reads;
                let refines = QUERY_REFINES.with(|c| c.get()) - refines;
                assert!(refines > 0, "mvcc={mvcc}: the query refined no candidate, so its reads measure nothing");
                assert!(reads <= 1, "mvcc={mvcc}: refining {refines} candidates made {reads} point reads past the generation's");
                let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(&q, v), *id)).collect();
                exact.sort_by(|a, b| b.0.total_cmp(&a.0));
                assert_eq!(got, exact[..10].iter().map(|(_, id)| *id).collect::<Vec<_>>(), "mvcc={mvcc}");
            }
        }
    }

    /// The bound test over a group and the lower bits' product decide and add exactly as their
    /// scalar forms do, the bar set among the members' raised estimates so they go both ways.
    #[test]
    fn the_group_bound_test_and_the_lower_product_match_their_scalar_forms() {
        let mut state = 31;
        for dims in [8, 128, 1024, 4096] {
            let rotated = random(&mut state, dims);
            let ex: Vec<u8> = random(&mut state, dims / 8 * 6).iter().map(|x| ((x + 0.5) * 256.0) as u8).collect();
            assert_eq!(lower_product(&rotated, &ex).to_bits(), lower_product_scalar(&rotated, &ex).to_bits(), "dims {dims}");
        }
        for round in 0..8 {
            let draw = |state: &mut u64, scale: f32| -> [f32; GROUP] { random(state, GROUP).iter().map(|x| (x + 0.5) * scale).collect::<Vec<_>>().try_into().unwrap() };
            let sums: [u32; GROUP] = draw(&mut state, 4000.0).map(|x| x as u32);
            let (gains, slacks) = (draw(&mut state, 0.01), draw(&mut state, 1.0));
            let mut bound = Bound { centre: 0.1, step: 0.002, base: -2.0, error: 0.05, sum: 0.3, spread: 0.02, bar: 0.0 };
            let mut raised: Vec<f64> = (0..GROUP).map(|slot| bound.raised(gains[slot] as f64, slacks[slot] as f64, sums[slot])).collect();
            raised.sort_by(f64::total_cmp);
            bound.bar = raised[4 * round % GROUP];
            let mask = reaching(&sums, &gains, &slacks, &bound);
            assert_eq!(mask, reaching_scalar(&sums, &gains, &slacks, &bound), "round {round}");
            assert!(mask.count_ones() as usize == GROUP - 4 * round % GROUP, "round {round}: {} members reach the bar", mask.count_ones());
        }
    }

    /// A rotation pads a vector to the next multiple of 32, keeps its length and its products, and
    /// spreads a basis vector over the width as a uniformly random rotation does, its largest
    /// share of the length under three times the log of the width (a random rotation's is about
    /// twice it, the identity's the whole width), whether or not the width is a power of two.
    #[test]
    fn a_rotation_pads_to_a_multiple_of_32_and_keeps_lengths_and_products() {
        for (dims, padded) in [(16, 32), (100, 128), (200, 224), (768, 768), (1000, 1024), (1536, 1536)] {
            let rotation = Rotation::new(dims);
            assert_eq!(rotation.padded, padded, "dims {dims}");
            let mut state = 17;
            let (a, b) = (random(&mut state, dims), random(&mut state, dims));
            let (ra, rb) = (rotation.apply(&a), rotation.apply(&b));
            let length = dot(&a, &a);
            assert!((dot(&ra, &ra) - length).abs() < 1e-4 * length, "dims {dims}: length {} against {length}", dot(&ra, &ra));
            assert!((dot(&ra, &rb) - dot(&a, &b)).abs() < 1e-4 * length, "dims {dims}: product {} against {}", dot(&ra, &rb), dot(&a, &b));
            for at in [0, dims - 1] {
                let mut basis = vec![0.0f32; dims];
                basis[at] = 1.0;
                let largest = rotation.apply(&basis).iter().map(|x| x * x).fold(0.0f32, f32::max) * padded as f32;
                assert!(largest < 3.0 * (padded as f32).ln(), "dims {dims}: dimension {at}'s largest share is {largest} times an even one");
            }
        }
    }

    /// The tiles give each vector the nearest live partition the direct products give it, and a
    /// query the same ranking of the partitions not retired as direct products over the centroids
    /// rounded to int8, after a centroid is placed over another and one past the last tile.
    #[test]
    fn the_tiles_assign_and_rank_as_the_direct_products_do() {
        for dims in [100, 768] {
            let mut state = 19;
            let vectors: Vec<Vec<f32>> = (0..40).map(|_| random(&mut state, dims)).collect();
            let mut model = Model::new((0..128).map(|_| random(&mut state, dims)).collect(), 0.0);
            model.place(3, 1, vectors[0].clone());
            model.place(128, 0, vectors[1].clone());
            model.states[7] = State::Retired;
            model.states[9] = State::Retiring;
            for v in &vectors {
                let live = (0..model.centroids.len()).filter(|i| model.states[*i] == State::Live);
                let direct = live.min_by(|a, b| l2(&model.centroids[*a], v).total_cmp(&l2(&model.centroids[*b], v))).unwrap();
                assert_eq!(model.nearest(v), direct, "dims {dims}");
            }
            assert_eq!((model.nearest(&vectors[0]), model.nearest(&vectors[1])), (3, 128), "dims {dims}");
            assert_eq!(nearest_all(&model, &vectors), vectors.iter().map(|v| model.nearest(v)).collect::<Vec<_>>(), "dims {dims}");
            let q = random(&mut state, dims);
            let order = |mut ranked: Vec<(f32, i64)>| {
                ranked.sort_by(|a, b| a.0.total_cmp(&b.0));
                ranked.into_iter().map(|(_, p)| p).collect::<Vec<_>>()
            };
            // The ranking reads the centroids rounded to int8 at each one's scale.
            let rounded = |c: &[f32]| {
                let scale = c.iter().fold(0.0f32, |m, x| m.max(x.abs())) / 127.0;
                (c.iter().map(|x| quantized(*x, scale) as f32).collect::<Vec<f32>>(), scale)
            };
            let direct = (0..model.centroids.len())
                .filter(|i| model.states[*i] != State::Retired)
                .map(|i| {
                    let (bytes, scale) = rounded(&model.centroids[i]);
                    (model.squares[i] - 2.0 * dot32(&bytes, &q) * scale, i as i64)
                })
                .collect();
            assert_eq!(order(model.ranked(&q, false)), order(direct), "dims {dims}");
        }
    }

    /// An index created with `rerank = 0` answers from its best 7-bit estimates and reads no table
    /// row: at a refine of one, the same ten rows a reranking index returns, each at an estimated
    /// distance near its exact one.
    #[test]
    fn a_query_without_the_rerank_answers_from_its_estimates_and_reads_no_table_row() {
        const DIMS: usize = 64;
        let mut answers = Vec::new();
        for rerank in [1, 0] {
            let (_db, conn) = database(&format!("ivf-rq-rerank-{rerank}.db"), false);
            let mut state = 139;
            let vectors: Vec<(i64, Vec<f32>)> = (1..=2000i64).map(|id| (id, random(&mut state, DIMS))).collect();
            insert_all(&conn, &vectors);
            conn.execute(format!("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 1, rerank = {rerank})")).unwrap();
            let q = random(&mut state, DIMS);
            top_ten(&conn, &q);
            let reads = TABLE_READS.with(|c| c.get());
            let answer: Vec<(i64, f64)> = rows(&conn, &format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&q)))
                .iter()
                .map(|row| (row[0].as_int().unwrap(), row[1].as_float()))
                .collect();
            let reads = TABLE_READS.with(|c| c.get()) - reads;
            let live: HashMap<i64, Vec<f32>> = vectors.into_iter().collect();
            answers.push((answer, reads, live, q));
        }
        let ((reranked, reranked_reads, _, _), (estimated, estimated_reads, live, q)) = (&answers[0], &answers[1]);
        assert!(*reranked_reads >= 10 && *estimated_reads == 0, "table rows read: {reranked_reads} reranking, {estimated_reads} without");
        let ids = |answer: &[(i64, f64)]| {
            let mut ids: Vec<i64> = answer.iter().map(|(id, _)| *id).collect();
            ids.sort_unstable();
            ids
        };
        assert_eq!(ids(reranked), ids(estimated));
        for (id, distance) in estimated {
            let exact = -dot(q, &live[id]);
            assert!((distance - exact).abs() < 0.1, "row {id}: estimated distance {distance}, exact {exact}");
        }
    }

    /// A query probes the partitions nearest it by the exact ranking, nearest first, though it ranks
    /// them all over centroids rounded to int8: the rounding moves none across the cut.
    #[test]
    fn a_query_probes_the_partitions_nearest_it_exactly() {
        const DIMS: usize = 100;
        let mut state = 23;
        let mut model = Model::new((0..1000).map(|_| random(&mut state, DIMS)).collect(), 0.0);
        model.states[5] = State::Retired;
        for _ in 0..50 {
            let q = random(&mut state, DIMS);
            let mut exact: Vec<(f32, i64)> = (0..1000).filter(|i| model.states[*i] != State::Retired).map(|i| (model.rank(&q, i, false), i as i64)).collect();
            exact.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            for take in [1, 10, 64] {
                let probes = model.probes(&q, take, false);
                assert!(probes.windows(2).all(|w| w[0].0 <= w[1].0), "take {take}: not nearest first");
                let mut probed: Vec<i64> = probes.into_iter().map(|(_, p)| p).collect();
                let mut nearest: Vec<i64> = exact[..take].iter().map(|(_, p)| *p).collect();
                probed.sort_unstable();
                nearest.sort_unstable();
                assert_eq!(probed, nearest, "take {take}");
            }
        }
    }


    /// At 3072 dimensions the scan widens its 16-bit sums several times within one group.
    #[test]
    fn the_simd_scan_sums_as_the_scalar_one_does() {
        for dims in [768, 3072] {
            let (rotation, _, coded) = entries(32, dims);
            let block = pack(&coded).unwrap();
            let layout = Layout::new(32, rotation.code_bytes());
            let codes = &block[layout.scan()..][..layout.group_bytes()];
            let mut state = 11;
            let table = ScanTable::new(&rotation.apply(&random(&mut state, dims)));
            let mut fast = [0u32; GROUP];
            scan_group(codes, &table.bytes, &mut fast);
            let mut slow = [0u32; GROUP];
            scan_group_scalar(codes, &table.bytes, &mut slow);
            assert_eq!(fast, slow, "dims {dims}");
        }
    }

    /// One block's scan refines every entry whose true product is among the best into the `keep`
    /// best 7-bit estimates, and those estimates are far closer to the truth than the entries' 1-bit
    /// ones.
    #[test]
    fn the_scan_keeps_the_true_best_and_the_seven_bit_estimate_beats_the_one_bit_one() {
        let (rotation, vectors, coded) = entries(64, 768);
        let mut state = 13;
        let q = random(&mut state, 768);
        let mut query = query(&q, &rotation, 10, 40);
        score_block(&mut query, &pack(&coded).unwrap(), 0.0, Location::Block(0)).unwrap();
        let mut truth: Vec<(f64, i64)> = vectors.iter().enumerate().map(|(n, v)| (dot(&q, v), n as i64)).collect();
        truth.sort_by(|a, b| b.0.total_cmp(&a.0));
        let kept: Vec<(f64, i64)> = query.estimates.iter().map(|o| (-o.0, o.1)).collect();
        assert_eq!(kept.len(), 40);
        for (_, rowid) in &truth[..10] {
            assert!(kept.iter().any(|(_, r)| r == rowid), "true top-10 row {rowid} is not among the best 7-bit estimates");
        }
        let rotated = rotation.apply(&q);
        let sum: f64 = rotated.iter().map(|x| *x as f64).sum();
        let (mut one, mut seven) = (0.0f64, 0.0f64);
        for (estimate, rowid) in &kept {
            let e = &coded[*rowid as usize];
            let signed: f64 = (0..rotated.len()).filter(|i| e.bits[i / 8] >> (i % 8) & 1 == 1).map(|i| rotated[i] as f64).sum();
            let true_product = dot(&q, &vectors[*rowid as usize]);
            one += (e.gain as f64 * (2.0 * signed - sum) - true_product).abs();
            seven += (estimate - true_product).abs();
        }
        assert!(seven < one / 16.0, "7-bit error {seven} against 1-bit {one}");
    }

    fn rows(conn: &Arc<crate::Connection>, sql: &str) -> Vec<Vec<Value>> {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            rows.push(row.get_values().cloned().collect());
            Ok(())
        })
        .unwrap();
        rows
    }

    fn literal(v: &[f32]) -> String {
        let parts: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
        format!("vector32('[{}]')", parts.join(","))
    }

    /// An ivf_rq index written through inserts, updates, deletes and maintenance answers each
    /// top-10 query with the rows an exact scan of the live vectors ranks first, in WAL and MVCC
    /// mode.
    #[test]
    fn the_index_answers_the_exact_top_ten_in_wal_and_mvcc() {
        use crate::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect};
        const DIMS: usize = 32;
        for mvcc in [false, true] {
            let db = Database::open_file_with_flags(
                Arc::new(MemoryIO::new()),
                &format!("ivf-rq-top-ten-{mvcc}.db"),
                OpenFlags::default(),
                DatabaseOpts::new().with_index_method(true),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            if mvcc {
                conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
            }
            conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)")
                .unwrap();
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)")
                .unwrap();
            let mut state = 17;
            let mut live: HashMap<i64, Vec<f32>> = HashMap::new();
            for id in 1..=300i64 {
                let v = random(&mut state, DIMS);
                conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v)))
                    .unwrap();
                live.insert(id, v);
            }
            for id in (5..=300i64).step_by(5) {
                let v = random(&mut state, DIMS);
                conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v)))
                    .unwrap();
                live.insert(id, v);
            }
            for id in (7..=300i64).step_by(7) {
                conn.execute(format!("DELETE FROM v WHERE id = {id}")).unwrap();
                live.remove(&id);
            }
            for _ in 0..20 {
                conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            }
            for _ in 0..5 {
                let q = random(&mut state, DIMS);
                let sql = format!(
                    "SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10",
                    literal(&q)
                );
                // The plan must query the index; a scan of the table would match the exact
                // ranking trivially.
                let plan = rows(&conn, &format!("EXPLAIN {sql}"));
                assert!(
                    plan.iter().any(|row| row[1].to_string() == "IndexMethodQuery"),
                    "mvcc={mvcc}: the query does not go through the index"
                );
                let got: Vec<i64> = rows(&conn, &sql)
                .iter()
                .map(|row| row[0].as_int().unwrap())
                .collect();
                let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(&q, v), *id)).collect();
                exact.sort_by(|a, b| b.0.total_cmp(&a.0));
                let expected: Vec<i64> = exact[..10].iter().map(|(_, id)| *id).collect();
                assert_eq!(got, expected, "mvcc={mvcc}");
            }
        }
    }

    /// The fast scan's 16-bit sums hold a wide code's best match: an entry equal to the query sums
    /// each position's largest byte, and past 1024 padded dimensions that total can pass 65535.
    #[test]
    fn the_fast_scan_sums_a_wide_codes_best_match_without_wrapping() {
        for dims in [768, 1536, 3072] {
            let (rotation, vectors, coded) = entries(32, dims);
            let block = pack(&coded).unwrap();
            let layout = Layout::new(32, rotation.code_bytes());
            let codes = &block[layout.scan()..][..layout.group_bytes()];
            let table = ScanTable::new(&rotation.apply(&vectors[0]));
            let (mut fast, mut slow) = ([0u32; GROUP], [0u32; GROUP]);
            scan_group(codes, &table.bytes, &mut fast);
            scan_group_scalar(codes, &table.bytes, &mut slow);
            for slot in 0..GROUP {
                let (lane, shift) = (slot % 16, if slot < 16 { 0 } else { 4 });
                let exact: f64 = (0..layout.positions).map(|j| table.exact[j * 16 + ((codes[j * 16 + lane] >> shift) & 0xf) as usize] as f64).sum();
                for (kernel, sums) in [("simd", &fast), ("scalar", &slow)] {
                    let summed = table.step * sums[slot] as f64 + table.base;
                    assert!((summed - exact).abs() <= table.error + 1e-3, "dims {dims} slot {slot} {kernel}: fast-scan sum {summed} against exact {exact}");
                }
            }
        }
    }

    /// A query near vectors that were packed into blocks and then updated away returns the live
    /// top ten: the stale entries their blocks keep must not raise the bar the live entries meet.
    #[test]
    fn a_query_near_vectors_updated_away_returns_the_live_top_ten() {
        use crate::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect};
        const DIMS: usize = 128;
        for mvcc in [false, true] {
            let db = Database::open_file_with_flags(
                Arc::new(MemoryIO::new()),
                &format!("ivf-rq-stale-bar-{mvcc}.db"),
                OpenFlags::default(),
                DatabaseOpts::new().with_index_method(true),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            if mvcc {
                conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
            }
            conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
            let mut state = 23;
            let q = random(&mut state, DIMS);
            let mut live: HashMap<i64, Vec<f32>> = HashMap::new();
            for id in 1..=216i64 {
                let v = if id > 200 { q.iter().zip(random(&mut state, DIMS)).map(|(a, n)| a + 0.002 * n).collect() } else { random(&mut state, DIMS) };
                conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
                live.insert(id, v);
            }
            for _ in 0..50 {
                conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            }
            for id in 201..=216i64 {
                let v = random(&mut state, DIMS);
                conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v))).unwrap();
                live.insert(id, v);
            }
            let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&q));
            let plan = rows(&conn, &format!("EXPLAIN {sql}"));
            assert!(plan.iter().any(|row| row[1].to_string() == "IndexMethodQuery"), "mvcc={mvcc}: the query does not go through the index");
            let got: Vec<i64> = rows(&conn, &sql).iter().map(|row| row[0].as_int().unwrap()).collect();
            let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(&q, v), *id)).collect();
            exact.sort_by(|a, b| b.0.total_cmp(&a.0));
            let expected: Vec<i64> = exact[..10].iter().map(|(_, id)| *id).collect();
            assert_eq!(got, expected, "mvcc={mvcc}");
        }
    }

    /// Revising vectors near a query round after round, with maintenance between the rounds, keeps
    /// each top ten exact; once maintenance has run out of work no block holds [`COMPACT_AT`] stale
    /// copies, and every dead row names blocks that still hold a copy of its vector that the
    /// vector's row does not point to.
    #[test]
    fn revisions_keep_the_top_ten_exact_and_every_dead_row_true() {
        use crate::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect};
        const DIMS: usize = 128;
        for mvcc in [false, true] {
            let db = Database::open_file_with_flags(
                Arc::new(MemoryIO::new()),
                &format!("ivf-rq-revisions-{mvcc}.db"),
                OpenFlags::default(),
                DatabaseOpts::new().with_index_method(true),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            if mvcc {
                conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
            }
            conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
            let mut state = 29;
            let q = random(&mut state, DIMS);
            let near = |state: &mut u64| -> Vec<f32> { q.iter().zip(random(state, DIMS)).map(|(a, n)| a + 0.002 * n).collect() };
            let exact_top = |live: &HashMap<i64, Vec<f32>>| -> Vec<i64> {
                let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(&q, v), *id)).collect();
                exact.sort_by(|a, b| b.0.total_cmp(&a.0));
                exact[..10].iter().map(|(_, id)| *id).collect()
            };
            let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(&q));
            let mut live: HashMap<i64, Vec<f32>> = HashMap::new();
            for id in 1..=300i64 {
                let v = if id > 280 { near(&mut state) } else { random(&mut state, DIMS) };
                conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
                live.insert(id, v);
            }
            for round in 0..4 {
                for _ in 0..30 {
                    conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
                }
                // Three rounds revise the vectors near the query again; the last moves them away.
                for id in 281..=300i64 {
                    let v = if round < 3 { near(&mut state) } else { random(&mut state, DIMS) };
                    conn.execute(format!("UPDATE v SET e = {} WHERE id = {id}", literal(&v))).unwrap();
                    live.insert(id, v);
                }
                let got: Vec<i64> = rows(&conn, &sql).iter().map(|row| row[0].as_int().unwrap()).collect();
                assert_eq!(got, exact_top(&live), "mvcc={mvcc} round {round}");
            }
            for _ in 0..100 {
                conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            }
            let got: Vec<i64> = rows(&conn, &sql).iter().map(|row| row[0].as_int().unwrap()).collect();
            assert_eq!(got, exact_top(&live), "mvcc={mvcc} after maintenance");
            let mut stale: HashMap<i64, usize> = HashMap::new();
            for row in partition_rows(&conn, DEAD_ROW) {
                let rowid = row[0].as_int().unwrap() & ((1 << ROWID_BITS) - 1);
                let blocks = dead_blocks(row[1].to_blob().unwrap()).unwrap();
                assert!(!blocks.is_empty(), "mvcc={mvcc}: the dead row of {rowid} names no block");
                let location = rows(&conn, &format!("SELECT location FROM __turso_internal_v_ivf_vectors WHERE rowid = {rowid}"));
                for block in blocks {
                    assert!(
                        location.iter().all(|r| r[0].as_int() != Some(Location::Block(block).encode())),
                        "mvcc={mvcc}: the dead row of {rowid} names block {block}, where its vector lives"
                    );
                    let payload = rows(&conn, &format!("SELECT c1 FROM __turso_internal_v_ivf_partitions WHERE rowid = {block}"));
                    let payload = payload.first().unwrap_or_else(|| panic!("mvcc={mvcc}: the dead row of {rowid} names missing block {block}"));
                    let entries = unpack(payload[0].to_blob().unwrap()).unwrap();
                    assert!(entries.iter().any(|e| e.rowid == rowid), "mvcc={mvcc}: block {block} holds no copy of {rowid}");
                    *stale.entry(block).or_default() += 1;
                }
            }
            assert!(stale.values().all(|count| *count < COMPACT_AT), "mvcc={mvcc}: blocks left with stale copies {stale:?}");
        }
    }

    fn database(name: &str, mvcc: bool) -> (Arc<crate::Database>, Arc<crate::Connection>) {
        use crate::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect};
        let db = Database::open_file_with_flags(Arc::new(MemoryIO::new()), name, OpenFlags::default(), DatabaseOpts::new().with_index_method(true), None, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB NOT NULL)").unwrap();
        (db, conn)
    }

    /// A vector near one of `centres`, the centre chosen by the generator.
    fn near(state: &mut u64, centres: &[Vec<f32>], spread: f32) -> Vec<f32> {
        let pick = ((random(state, 1)[0] + 0.5) * centres.len() as f32) as usize;
        let centre = &centres[pick.min(centres.len() - 1)];
        centre.iter().zip(random(state, centre.len())).map(|(c, n)| c + spread * n).collect()
    }

    /// One statement inserting every vector of `rows`.
    fn insert_all(conn: &Arc<crate::Connection>, rows: &[(i64, Vec<f32>)]) {
        let values: Vec<String> = rows.iter().map(|(id, v)| format!("({id}, {})", literal(v))).collect();
        conn.execute(format!("INSERT INTO v VALUES {}", values.join(","))).unwrap();
    }

    fn top_ten(conn: &Arc<crate::Connection>, q: &[f32]) -> Vec<i64> {
        rows(conn, &format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM v ORDER BY distance LIMIT 10", literal(q))).iter().map(|row| row[0].as_int().unwrap()).collect()
    }

    fn exact_top_ten(live: &HashMap<i64, Vec<f32>>, q: &[f32]) -> Vec<i64> {
        let mut exact: Vec<(f64, i64)> = live.iter().map(|(id, v)| (dot(q, v), *id)).collect();
        exact.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        exact.iter().take(10).map(|(_, id)| *id).collect()
    }

    /// The rows of one of index `v_ivf`'s backing tables: the rowid, then `columns`.
    fn backing(conn: &Arc<crate::Connection>, store: &str, columns: &str) -> Vec<Vec<Value>> {
        rows(conn, &format!("SELECT rowid, {columns} FROM __turso_internal_v_ivf_{store}"))
    }

    /// The index as its backing tables hold it: each partition's bookkeeping, tail length and current
    /// centroid version, and the stale copies per block.
    struct Stored {
        parts: HashMap<i64, Part>,
        tails: HashMap<i64, usize>,
        versions: HashMap<i64, i64>,
        stale: HashMap<i64, usize>,
    }

    /// The partitions table's rows of `kind`: the key, then the row's columns.
    fn partition_rows(conn: &Arc<crate::Connection>, kind: i64) -> Vec<Vec<Value>> {
        backing(conn, "partitions", "c0, c1, c2, c3, c4, c5, c6, c7").into_iter().filter(|r| kind_of(r[0].as_int().unwrap()) == kind).collect()
    }

    fn stored(conn: &Arc<crate::Connection>) -> Stored {
        let parts = partition_rows(conn, PART_ROW)
            .iter()
            .map(|r| {
                let f = |i: usize| r[i].as_int().unwrap();
                let part = Part { next: f(1), size: f(2), open: f(3), move_next: f(4), move_end: f(5), state: State::decode(f(6)).unwrap(), retry: f(7), version: f(8) };
                (partition_of(f(0)), part)
            })
            .collect();
        let mut tails = HashMap::new();
        for r in partition_rows(conn, TAIL_ROW) {
            *tails.entry(partition_of(r[0].as_int().unwrap())).or_default() += 1;
        }
        let mut versions: HashMap<i64, i64> = HashMap::new();
        for r in backing(conn, "centroids", "value") {
            let key = r[0].as_int().unwrap();
            if key >= 0 {
                let version = versions.entry(key >> VERSION_BITS).or_default();
                *version = (*version).max(key & ((1 << VERSION_BITS) - 1));
            }
        }
        let mut stale = HashMap::new();
        for r in partition_rows(conn, DEAD_ROW) {
            for block in dead_blocks(r[1].to_blob().unwrap()).unwrap() {
                *stale.entry(block).or_default() += 1;
            }
        }
        Stored { parts, tails, versions, stale }
    }

    impl Stored {
        fn part(&self, p: i64) -> Part {
            self.parts.get(&p).copied().unwrap_or_default()
        }

        fn tail(&self, p: i64) -> usize {
            self.tails.get(&p).copied().unwrap_or(0)
        }

        fn partitions(&self) -> Vec<i64> {
            let last = self.versions.keys().chain(self.parts.keys()).chain(self.tails.keys()).copied().max().unwrap_or(0);
            (0..=last).collect()
        }

        fn live(&self) -> Vec<i64> {
            self.partitions().into_iter().filter(|p| self.part(*p).state == State::Live).collect()
        }

        fn live_entries(&self, p: i64) -> i64 {
            let stale: usize = self.stale.iter().filter(|(block, _)| partition_of(**block) == p).map(|(_, count)| *count).sum();
            self.part(p).size + self.tail(p) as i64 - stale as i64
        }

        /// The jobs left, by the rules maintenance chooses them by.
        fn pending(&self) -> Vec<String> {
            let live = self.live();
            let split_bound = split_bound(live.len().max(1));
            let mut left = Vec::new();
            for p in self.partitions() {
                let part = self.part(p);
                if part.moving() {
                    left.push(format!("partition {p} is moving"));
                }
                if part.state != State::Live {
                    if self.tail(p) > 0 {
                        left.push(format!("{:?} partition {p} has a tail", part.state));
                    }
                    if part.state == State::Retiring {
                        left.push(format!("partition {p} is retiring"));
                    }
                    continue;
                }
                if self.tail(p) >= PACK_AT {
                    left.push(format!("partition {p} has a tail of {}", self.tail(p)));
                }
                if live.len() > 1 && self.live_entries(p) < split_bound / 16 {
                    left.push(format!("partition {p} holds {} live entries", self.live_entries(p)));
                }
                let size = part.size + self.tail(p) as i64;
                if size >= split_bound.max(part.retry) {
                    left.push(format!("partition {p} holds {size}"));
                }
            }
            for (block, count) in &self.stale {
                if *count >= COMPACT_AT {
                    left.push(format!("block {block} holds {count} stale copies"));
                }
            }
            left
        }
    }

    /// Plain inserts, with no OPTIMIZE INDEX, keep the index maintained: their statements pack the
    /// tails they grow and split the partitions they fill.
    #[test]
    fn plain_writes_keep_the_index_maintained() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (_db, conn) = database(&format!("ivf-rq-writes-{mvcc}.db"), mvcc);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let mut state = 31;
            let mut live = HashMap::new();
            for id in 1..=1500i64 {
                let v = random(&mut state, DIMS);
                conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
                live.insert(id, v);
            }
            let index = stored(&conn);
            let tails: usize = index.tails.values().sum();
            assert!(index.live().len() >= 8, "mvcc={mvcc}: {} live partitions after 1500 inserts", index.live().len());
            assert!(tails < 150, "mvcc={mvcc}: {tails} entries left in tails");
            for _ in 0..3 {
                let q = random(&mut state, DIMS);
                assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
            }
        }
    }

    /// Statements inserting a hundred rows each, with no OPTIMIZE INDEX, keep the index maintained:
    /// each pays for the maintenance its rows make due, so no partition grows to twice the split
    /// bound and work a statement leaves, the moves of a split late in it, is done by the next.
    #[test]
    fn statements_of_many_rows_keep_the_index_maintained() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (_db, conn) = database(&format!("ivf-rq-statements-{mvcc}.db"), mvcc);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let mut state = 61;
            let mut live = HashMap::new();
            let mut before: Vec<String> = Vec::new();
            for statement in 0..40i64 {
                let rows: Vec<(i64, Vec<f32>)> = (1..=100).map(|i| (statement * 100 + i, random(&mut state, DIMS))).collect();
                insert_all(&conn, &rows);
                live.extend(rows);
                let index = stored(&conn);
                let bound = split_bound(index.live().len().max(1));
                let biggest = index.live().iter().map(|p| index.part(*p).size + index.tail(*p) as i64).max().unwrap_or(0);
                assert!(biggest < 2 * bound, "mvcc={mvcc}: a partition holds {biggest} after statement {statement}, the split bound being {bound}");
                let left = index.pending();
                assert!(before.is_empty() || left.is_empty(), "mvcc={mvcc}: statements {} and {statement} both left work: {before:?}, then {left:?}", statement - 1);
                before = left;
            }
            let index = stored(&conn);
            assert!(index.live().len() >= trained_partitions(4000) / 2, "mvcc={mvcc}: {} live partitions for 4000 rows", index.live().len());
            for _ in 0..3 {
                let q = random(&mut state, DIMS);
                assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
            }
        }
    }

    /// Work a statement's maintenance found due but had no job left for is done by the next
    /// statement's, though that one writes none of the partitions the work lies in.
    #[test]
    fn work_a_statement_left_due_is_done_by_the_next() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (_db, conn) = database(&format!("ivf-rq-left-{mvcc}.db"), mvcc);
            let mut state = 127;
            let centres: Vec<Vec<f32>> = (0..12).map(|_| random(&mut state, DIMS).iter().map(|x| 3.0 * x).collect()).collect();
            let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, near(&mut state, &centres, 0.4))).collect();
            insert_all(&conn, &rows);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            assert!(stored(&conn).pending().is_empty(), "mvcc={mvcc}: {:?}", stored(&conn).pending());
            let mut next = 3000i64;
            let mut around = |state: &mut u64, c: usize, n: usize| -> Vec<(i64, Vec<f32>)> {
                (0..n)
                    .map(|_| {
                        next += 1;
                        (next, near(state, &centres[c..=c], 0.01))
                    })
                    .collect()
            };
            // One statement grows two tails past the pack length with one job to spend: it packs one.
            let mut two = around(&mut state, 0, 20);
            two.extend(around(&mut state, 1, 20));
            STATEMENT_BUDGET.with(|budget| budget.set(1));
            insert_all(&conn, &two);
            STATEMENT_BUDGET.with(|budget| budget.set(usize::MAX));
            let left = stored(&conn).pending();
            assert_eq!(left.len(), 1, "mvcc={mvcc}: the statement left {left:?}");
            // The next writes one row, into a third partition.
            insert_all(&conn, &around(&mut state, 2, 1));
            let left = stored(&conn).pending();
            assert!(left.is_empty(), "mvcc={mvcc}: the next statement left {left:?}");
        }
    }

    /// One OPTIMIZE INDEX does every job a backlog holds: over a trained index, statements that do
    /// not maintain it leave packs, splits and moves due, and after one call none is.
    #[test]
    fn one_optimize_finishes_every_job() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (_db, conn) = database(&format!("ivf-rq-backlog-{mvcc}.db"), mvcc);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let mut state = 37;
            let mut rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
            insert_all(&conn, &rows);
            STATEMENT_UPKEEP.with(|on| on.set(false));
            for statement in 0..15i64 {
                let more: Vec<(i64, Vec<f32>)> = (1..=100).map(|i| (3000 + statement * 100 + i, random(&mut state, DIMS))).collect();
                insert_all(&conn, &more);
                rows.extend(more);
            }
            STATEMENT_UPKEEP.with(|on| on.set(true));
            let backlog = stored(&conn).pending();
            assert!(backlog.len() > 1, "mvcc={mvcc}: the writes left {backlog:?} to finish");
            conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            let left = stored(&conn).pending();
            assert!(left.is_empty(), "mvcc={mvcc}: work left after one OPTIMIZE: {left:?}");
            let live: HashMap<i64, Vec<f32>> = rows.into_iter().collect();
            for _ in 0..3 {
                let q = random(&mut state, DIMS);
                assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
            }
        }
    }

    /// The rows maintenance surveys while it works off a backlog grow with the backlog, not with
    /// its square: doubling the rows one statement inserts less than triples them.
    #[test]
    fn maintenance_reads_grow_linearly_with_the_backlog() {
        const DIMS: usize = 16;
        let mut reads = Vec::new();
        for count in [1500i64, 3000] {
            let (_db, conn) = database(&format!("ivf-rq-linear-{count}.db"), false);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let mut state = 41;
            let rows: Vec<(i64, Vec<f32>)> = (1..=count).map(|id| (id, random(&mut state, DIMS))).collect();
            let before = SURVEYED.with(|c| c.get());
            insert_all(&conn, &rows);
            conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            reads.push(SURVEYED.with(|c| c.get()) - before);
            assert!(stored(&conn).pending().is_empty());
        }
        let ratio = reads[1] as f64 / reads[0] as f64;
        assert!(ratio < 3.0, "surveys read {reads:?} rows: doubling the backlog multiplied them by {ratio:.2}");
    }

    /// Once maintenance has run out of work, every block and tail entry is coded against its
    /// partition's current centroid: a move recodes the entries that stay, not only those it moves.
    #[test]
    fn every_entry_is_coded_against_its_partitions_current_centroid() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (_db, conn) = database(&format!("ivf-rq-recode-{mvcc}.db"), mvcc);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let mut state = 43;
            let centres: Vec<Vec<f32>> = (0..8).map(|_| random(&mut state, DIMS).iter().map(|x| 4.0 * x).collect()).collect();
            for id in 1..=2000i64 {
                let v = near(&mut state, &centres, 0.3);
                conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
            }
            conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            let index = stored(&conn);
            assert!(index.pending().is_empty(), "mvcc={mvcc}: {:?}", index.pending());
            assert!(index.live().len() > 4, "mvcc={mvcc}: the index split into {} partitions", index.live().len());
            for kind in [BLOCK_ROW, TAIL_ROW] {
                for r in partition_rows(&conn, kind) {
                    let (key, version) = (r[0].as_int().unwrap(), r[1].as_int().unwrap());
                    let current = index.versions.get(&partition_of(key)).copied().unwrap_or(0);
                    assert_eq!(version, current, "mvcc={mvcc}: row {key} of kind {kind} is coded against version {version} of partition {}", partition_of(key));
                }
            }
        }
    }

    /// Deleting every vector of half the clusters retires the partitions that held them: none is
    /// left live with fewer entries than the retirement bound, and the queries stay exact. A query
    /// probes the retired partitions only under MVCC, where a concurrent writer coding against an
    /// older model can leave a tail entry in one.
    #[test]
    fn emptied_partitions_retire() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (_db, conn) = database(&format!("ivf-rq-retire-{mvcc}.db"), mvcc);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let mut state = 47;
            let centres: Vec<Vec<f32>> = (0..8).map(|_| random(&mut state, DIMS).iter().map(|x| 4.0 * x).collect()).collect();
            let mut live = HashMap::new();
            for (c, centre) in centres.iter().enumerate() {
                let rows: Vec<(i64, Vec<f32>)> = (0..300).map(|i| ((c * 300 + i + 1) as i64, near(&mut state, std::slice::from_ref(centre), 0.2))).collect();
                insert_all(&conn, &rows);
                live.extend(rows);
            }
            conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            let before = stored(&conn).live().len();
            conn.execute("DELETE FROM v WHERE id <= 1200").unwrap();
            live.retain(|id, _| *id > 1200);
            conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            let index = stored(&conn);
            assert!(index.pending().is_empty(), "mvcc={mvcc}: {:?}", index.pending());
            let retired = index.parts.values().filter(|part| part.state == State::Retired).count();
            assert!(retired > 0, "mvcc={mvcc}: no partition of {before} retired");
            for centre in &centres {
                let q = near(&mut state, std::slice::from_ref(centre), 0.2);
                assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
            }
            let probes = PROBES.with(|c| c.get());
            top_ten(&conn, &near(&mut state, &centres, 0.2));
            let probed = PROBES.with(|c| c.get()) - probes;
            let wanted = index.live().len() + if mvcc { retired } else { 0 };
            assert_eq!(probed, wanted, "mvcc={mvcc}: {probed} partitions probed, {} live and {retired} retired", index.live().len());
        }
    }

    /// CREATE INDEX over a table that already holds 3000 rows trains its partitions by k-means over a
    /// sample of them, in the statement itself: afterwards about the square root of the rows are
    /// live partitions, every entry is packed, no job is due, and the queries are exact.
    #[test]
    fn create_index_over_rows_trains_its_partitions() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (_db, conn) = database(&format!("ivf-rq-build-{mvcc}.db"), mvcc);
            let mut state = 59;
            let centres: Vec<Vec<f32>> = (0..12).map(|_| random(&mut state, DIMS).iter().map(|x| 3.0 * x).collect()).collect();
            let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, near(&mut state, &centres, 0.4))).collect();
            insert_all(&conn, &rows);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let index = stored(&conn);
            let tails: usize = index.tails.values().sum();
            assert!(index.live().len() >= trained_partitions(3000) / 2, "mvcc={mvcc}: {} live partitions for 3000 rows", index.live().len());
            assert_eq!(tails, 0, "mvcc={mvcc}: entries left in tails");
            assert!(index.pending().is_empty(), "mvcc={mvcc}: {:?}", index.pending());
            let live: HashMap<i64, Vec<f32>> = rows.into_iter().collect();
            for _ in 0..3 {
                let q = near(&mut state, &centres, 0.4);
                assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q), "mvcc={mvcc}");
            }
        }
    }

    /// The estimates rank well enough that a query reranking only the rows it returns, at a refine
    /// of one, finds nearly all of the exact top ten: probing every partition, at least 95% of them
    /// over fifty queries of random vectors, whose best products lie closer together than the
    /// estimates' error, of 64 dimensions and of 200, which the rotation pads to 224.
    #[test]
    fn a_query_reranking_only_its_answer_finds_the_exact_top_ten() {
        for dims in [64, 200] {
            let (_db, conn) = database(&format!("ivf-rq-refine-one-{dims}.db"), false);
            let mut state = 97;
            let vectors: Vec<(i64, Vec<f32>)> = (1..=2000i64).map(|id| (id, random(&mut state, dims))).collect();
            insert_all(&conn, &vectors);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 1)").unwrap();
            let live: HashMap<i64, Vec<f32>> = vectors.into_iter().collect();
            let (mut found, queries) = (0, 50);
            for _ in 0..queries {
                let q = random(&mut state, dims);
                let exact = exact_top_ten(&live, &q);
                found += top_ten(&conn, &q).iter().filter(|id| exact.contains(id)).count();
            }
            let recall = found as f64 / (10 * queries) as f64;
            assert!(recall >= 0.95, "dims {dims}: recall@10 {recall} at a refine of one");
        }
    }

    /// A fast-scanned member is summed exactly only near the bar, where the exact sum can drop it:
    /// a query over a trained index refines more entries than it sums exactly, and still finds the
    /// exact top ten.
    #[test]
    fn a_query_sums_exactly_only_the_members_near_the_bar() {
        const DIMS: usize = 64;
        let (_db, conn) = database("ivf-rq-exact-sums.db", false);
        let mut state = 131;
        let vectors: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        insert_all(&conn, &vectors);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let live: HashMap<i64, Vec<f32>> = vectors.into_iter().collect();
        let q = random(&mut state, DIMS);
        top_ten(&conn, &q);
        let (sums, refines) = (EXACT_SUMS.with(|c| c.get()), QUERY_REFINES.with(|c| c.get()));
        assert_eq!(top_ten(&conn, &q), exact_top_ten(&live, &q));
        let (sums, refines) = (EXACT_SUMS.with(|c| c.get()) - sums, QUERY_REFINES.with(|c| c.get()) - refines);
        assert!(refines > 0 && sums < refines, "{sums} exact sums for {refines} refined entries");
    }

    /// The blocks a query read are held for the next: a second query over the same partitions reads
    /// no block from pages, until a pack rewrites them under a new version, when the next query reads
    /// those again, and every answer is the exact top ten.
    #[test]
    fn a_query_scores_held_partitions_from_memory_until_a_write_rewrites_them() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (_db, conn) = database(&format!("ivf-rq-held-{mvcc}.db"), mvcc);
            let mut state = 109;
            let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
            let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
            vectors.sort_by_key(|(id, _)| *id);
            insert_all(&conn, &vectors);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let q = random(&mut state, DIMS);
            let reads = |conn: &Arc<crate::Connection>, live: &HashMap<i64, Vec<f32>>| {
                let before = BLOCK_READS.with(|c| c.get());
                assert_eq!(top_ten(conn, &q), exact_top_ten(live, &q), "mvcc={mvcc}");
                BLOCK_READS.with(|c| c.get()) - before
            };
            assert!(reads(&conn, &live) > 0, "mvcc={mvcc}: the first query read no block");
            assert_eq!(reads(&conn, &live), 0, "mvcc={mvcc}: the second query read blocks its first had read");
            // Rows near the query land in the partitions it probes; their packs rewrite blocks there.
            let near: Vec<(i64, Vec<f32>)> = (3001..=3100i64).map(|id| (id, q.iter().zip(random(&mut state, DIMS)).map(|(a, n)| a + 0.05 * n).collect())).collect();
            insert_all(&conn, &near);
            conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            live.extend(near);
            assert!(reads(&conn, &live) > 0, "mvcc={mvcc}: the query after the packs read no block");
            assert_eq!(reads(&conn, &live), 0, "mvcc={mvcc}: the query after that read blocks again");
        }
    }

    /// A transaction that read the index keeps its snapshot's answers while another connection
    /// rewrites the partitions it probes, the blocks it holds being those of its own snapshot's
    /// version, and sees the rewrite once it ends.
    #[test]
    fn a_snapshot_keeps_its_answers_while_another_connection_rewrites_the_partitions() {
        const DIMS: usize = 16;
        let (db, writer) = database("ivf-rq-held-snapshot.db", false);
        let mut state = 113;
        let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
        vectors.sort_by_key(|(id, _)| *id);
        insert_all(&writer, &vectors);
        writer.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
        let reader = db.connect(crate::ConnectOpts::new()).unwrap();
        let q = random(&mut state, DIMS);
        let before = exact_top_ten(&live, &q);
        reader.execute("BEGIN").unwrap();
        assert_eq!(top_ten(&reader, &q), before);
        let near: Vec<(i64, Vec<f32>)> = (3001..=3100i64).map(|id| (id, q.iter().zip(random(&mut state, DIMS)).map(|(a, n)| a + 0.05 * n).collect())).collect();
        insert_all(&writer, &near);
        writer.execute("OPTIMIZE INDEX v_ivf").unwrap();
        live.extend(near);
        let after = exact_top_ten(&live, &q);
        assert_ne!(before, after, "the rows near the query do not change its top ten");
        assert_eq!(top_ten(&writer, &q), after, "the writer's answer");
        assert_eq!(top_ten(&reader, &q), before, "the reader's snapshot answer");
        reader.execute("COMMIT").unwrap();
        assert_eq!(top_ten(&reader, &q), after, "the reader's answer after its snapshot");
    }

    /// A partition's rows held under one snapshot serve the next query, which reads no page, until a
    /// commit writes the partition, when the next query reads it again and finds the row: committed
    /// on its own, in a transaction whose own query found it first, or after a checkpoint restarted
    /// the WAL, under WAL and under MVCC. While the transaction is open, probes read pages, and a
    /// rolled-back row is never found.
    #[test]
    fn held_rows_serve_later_snapshots_until_a_commit_writes_their_partition() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (db, writer) = database(&format!("ivf-rq-held-rows-{mvcc}.db"), mvcc);
            let mut state = 211;
            let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
            let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
            vectors.sort_by_key(|(id, _)| *id);
            insert_all(&writer, &vectors);
            writer.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let reader = db.connect(crate::ConnectOpts::new()).unwrap();
            let q = random(&mut state, DIMS);
            // The reader's answer, the exact top ten, and the seeks its probes made.
            let seeks = |live: &HashMap<i64, Vec<f32>>| {
                let before = WALK_SEEKS.with(|c| c.get());
                assert_eq!(top_ten(&reader, &q), exact_top_ten(live, &q), "mvcc={mvcc}");
                WALK_SEEKS.with(|c| c.get()) - before
            };
            // A row nearer the query than any other.
            let nearest = |id: i64| format!("INSERT INTO v VALUES ({id}, {})", literal(&q.iter().map(|a| 2.0 * a).collect::<Vec<f32>>()));
            let add = |live: &mut HashMap<i64, Vec<f32>>, id: i64| {
                live.insert(id, q.iter().map(|a| 2.0 * a).collect());
            };
            // The first query holds the rows; under MVCC its snapshot predates the index's record
            // of writes, begun by its own statement, so the second holds them again.
            seeks(&live);
            seeks(&live);
            assert_eq!(seeks(&live), 0, "mvcc={mvcc}: a query over held partitions read pages");

            writer.execute(nearest(3001)).unwrap();
            add(&mut live, 3001);
            assert!(seeks(&live) > 0, "mvcc={mvcc}: the query after a commit read no page");
            assert_eq!(seeks(&live), 0, "mvcc={mvcc}: the partitions read again were not held");

            writer.execute("BEGIN").unwrap();
            writer.execute(nearest(3002)).unwrap();
            let mut with = live.clone();
            add(&mut with, 3002);
            assert_eq!(top_ten(&writer, &q), exact_top_ten(&with, &q), "mvcc={mvcc}: the writer's own row");
            assert!(seeks(&live) > 0, "mvcc={mvcc}: a query took held rows while a writer was open");
            writer.execute("COMMIT").unwrap();
            assert!(seeks(&with) > 0, "mvcc={mvcc}: the query after the transaction read no page");
            assert_eq!(seeks(&with), 0, "mvcc={mvcc}: the partitions read after the transaction were not held");
            live = with;

            writer.execute("BEGIN").unwrap();
            writer.execute(nearest(3003)).unwrap();
            writer.execute("ROLLBACK").unwrap();
            seeks(&live);
            assert_eq!(seeks(&live), 0, "mvcc={mvcc}: the partitions read after the rollback were not held");

            // A tail left long enough to pack, and packed by OPTIMIZE INDEX alone.
            STATEMENT_UPKEEP.with(|on| on.set(false));
            for id in 3101..=3112i64 {
                writer.execute(nearest(id)).unwrap();
                add(&mut live, id);
            }
            STATEMENT_UPKEEP.with(|on| on.set(true));
            seeks(&live);
            assert_eq!(seeks(&live), 0, "mvcc={mvcc}: the partitions read after the inserts were not held");
            writer.execute("OPTIMIZE INDEX v_ivf").unwrap();
            assert!(seeks(&live) > 0, "mvcc={mvcc}: the query after OPTIMIZE INDEX read no page");
            assert_eq!(seeks(&live), 0, "mvcc={mvcc}: the partitions read after OPTIMIZE INDEX were not held");

            if !mvcc {
                writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
                writer.execute(nearest(3004)).unwrap();
                add(&mut live, 3004);
                assert!(seeks(&live) > 0, "the query after a checkpoint and a commit read no page");
                assert_eq!(seeks(&live), 0, "the partitions read after the checkpoint were not held");
            }
        }
    }

    /// A reader inside a transaction keeps its snapshot's answer while another connection commits a
    /// row near its query and a third holds the partitions' rows under its later snapshot, under WAL
    /// and under MVCC; once its transaction ends, the reader finds the row.
    #[test]
    fn a_snapshot_never_takes_rows_held_under_a_later_one() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (db, writer) = database(&format!("ivf-rq-later-rows-{mvcc}.db"), mvcc);
            let mut state = 223;
            let mut live: HashMap<i64, Vec<f32>> = (1..=3000i64).map(|id| (id, random(&mut state, DIMS))).collect();
            let mut vectors: Vec<(i64, Vec<f32>)> = live.iter().map(|(id, v)| (*id, v.clone())).collect();
            vectors.sort_by_key(|(id, _)| *id);
            insert_all(&writer, &vectors);
            writer.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let (reader, other) = (db.connect(crate::ConnectOpts::new()).unwrap(), db.connect(crate::ConnectOpts::new()).unwrap());
            let q = random(&mut state, DIMS);
            let before = exact_top_ten(&live, &q);
            reader.execute("BEGIN").unwrap();
            assert_eq!(top_ten(&reader, &q), before, "mvcc={mvcc}");
            let v: Vec<f32> = q.iter().map(|a| 2.0 * a).collect();
            writer.execute(format!("INSERT INTO v VALUES (3001, {})", literal(&v))).unwrap();
            live.insert(3001, v);
            let after = exact_top_ten(&live, &q);
            assert_ne!(before, after, "the row does not change the query's top ten");
            assert_eq!(top_ten(&other, &q), after, "mvcc={mvcc}: the third connection's answer");
            assert_eq!(top_ten(&other, &q), after, "mvcc={mvcc}: the third connection's answer from its held rows");
            assert_eq!(top_ten(&reader, &q), before, "mvcc={mvcc}: the reader took rows held under a later snapshot");
            reader.execute("COMMIT").unwrap();
            assert_eq!(top_ten(&reader, &q), after, "mvcc={mvcc}: the reader's answer after its transaction");
        }
    }

    /// A partition's rows are held only beside its blocks, count against the budget with them, and
    /// give way with them.
    #[test]
    fn held_rows_come_and_go_with_their_blocks() {
        let mut cache = PartitionCache { budget: 1000, ..PartitionCache::default() };
        let rows = |version: i64| {
            let mut rows = HeldRows::new((0, 1), version);
            rows.push_tail(5, 0, &[0u8; 68]);
            rows
        };
        cache.hold(0, rows(7));
        assert!(cache.rows.is_empty() && cache.bytes == 0, "rows held without their blocks");
        let mut blocks = HeldBlocks::default();
        blocks.push(0, 0, &[0u8; 300]);
        cache.insert(0, 7, blocks);
        cache.hold(0, rows(8));
        assert!(cache.rows.is_empty(), "rows held beside blocks of another version");
        cache.hold(0, rows(7));
        assert_eq!(cache.bytes, 300 + 100, "the rows' bytes are not counted");
        let mut more = HeldBlocks::default();
        more.push(0, 0, &[0u8; 700]);
        cache.insert(1, 7, more);
        assert!(cache.get(0, 7).is_none() && cache.rows.is_empty(), "the rows outlived their blocks");
        assert_eq!(cache.bytes, 700);
    }

    /// The held blocks stay within the budget, the least recently probed partition giving way
    /// first, and blocks larger than the whole budget are not held.
    #[test]
    fn the_held_partitions_stay_within_the_budget_least_recent_first() {
        let held = |bytes: usize| {
            let mut blocks = HeldBlocks::default();
            blocks.push(0, 0, &vec![0u8; bytes]);
            blocks
        };
        let mut cache = PartitionCache { budget: 1000, ..PartitionCache::default() };
        for partition in 0..4 {
            cache.insert(partition, 7, held(300));
        }
        assert!(cache.bytes <= 1000, "{} bytes held", cache.bytes);
        assert!(cache.get(0, 7).is_none(), "the oldest partition is still held");
        assert!(cache.get(1, 7).is_some() && cache.get(2, 7).is_some() && cache.get(3, 7).is_some());
        // Probing partition 1 again leaves 2 the least recently probed.
        assert!(cache.get(1, 7).is_some());
        cache.insert(4, 7, held(300));
        assert!(cache.get(2, 7).is_none() && cache.get(1, 7).is_some() && cache.get(4, 7).is_some());
        assert!(cache.get(4, 8).is_none(), "a version the partition was not held under");
        cache.insert(5, 7, held(1001));
        assert!(cache.get(5, 7).is_none(), "blocks larger than the budget are held");
        // A partition has given way: one is held only when probed lately more often than the least
        // recently probed held partition, 3, probed once.
        assert!(cache.get(6, 7).is_none());
        assert!(!cache.admits(6, 7), "a partition probed once is held over one probed more");
        cache.get(6, 7);
        cache.get(6, 7);
        assert!(cache.admits(6, 7), "a partition probed most is not held");
        assert!(!PartitionCache::default().admits(0, 0), "an empty budget holds a partition");
    }

    /// A query reads each partition it probes with one seek: its bookkeeping, dead rows, tail and
    /// blocks lie under one key range, read in one walk.
    #[test]
    fn a_query_reads_each_probed_partition_with_one_seek() {
        let (_db, conn) = database("ivf-rq-one-seek.db", false);
        let mut state = 107;
        let vectors: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, 16))).collect();
        insert_all(&conn, &vectors);
        // Nothing held, so every probe reads its partition from pages.
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 8, refine = 4, cache_mib = 0)").unwrap();
        // Tail entries and dead rows beside the blocks.
        for id in 3001..=3040i64 {
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&random(&mut state, 16)))).unwrap();
        }
        for id in (5..=3000i64).step_by(97) {
            conn.execute(format!("DELETE FROM v WHERE id = {id}")).unwrap();
        }
        let q = random(&mut state, 16);
        // The first query loads the model; the second finds it shared.
        top_ten(&conn, &q);
        let (seeks, probes) = (WALK_SEEKS.with(|c| c.get()), PROBES.with(|c| c.get()));
        top_ten(&conn, &q);
        let (seeks, probes) = (WALK_SEEKS.with(|c| c.get()) - seeks, PROBES.with(|c| c.get()) - probes);
        assert!(probes >= 8, "the query probed {probes} partitions");
        assert_eq!(seeks, probes, "{seeks} seeks for {probes} probed partitions");
    }

    /// CREATE INDEX over a populated table writes each vector's code once, into its block, so the
    /// file it leaves holds almost no free pages.
    #[test]
    fn create_index_over_rows_leaves_almost_no_free_pages() {
        const DIMS: usize = 16;
        let (_db, conn) = database("ivf-rq-build-once.db", false);
        let mut state = 103;
        let vectors: Vec<(i64, Vec<f32>)> = (1..=20_000i64).map(|id| (id, random(&mut state, DIMS))).collect();
        for chunk in vectors.chunks(2_000) {
            insert_all(&conn, chunk);
        }
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
        let pragma = |name: &str| rows(&conn, &format!("PRAGMA {name}"))[0][0].as_int().unwrap();
        let (pages, free) = (pragma("page_count"), pragma("freelist_count"));
        assert!(free * 20 <= pages, "{free} of {pages} pages free after the build");
    }

    /// A vector's row in the index names only where its code is, the code itself being in its block
    /// or tail entry, so each code is stored once: after a build and after single inserts.
    #[test]
    fn a_vectors_row_holds_only_where_its_code_is() {
        let (_db, conn) = database("ivf-rq-one-copy.db", false);
        let mut state = 101;
        let vectors: Vec<(i64, Vec<f32>)> = (1..=300i64).map(|id| (id, random(&mut state, 32))).collect();
        insert_all(&conn, &vectors);
        conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 64, refine = 4)").unwrap();
        for id in 301..=320i64 {
            conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&random(&mut state, 32)))).unwrap();
        }
        let stored = rows(&conn, "SELECT * FROM __turso_internal_v_ivf_vectors");
        assert_eq!(stored.len(), 320);
        for row in &stored {
            assert_eq!(row.len(), 1, "a vector's row holds {} values: {row:?}", row.len());
        }
    }

    /// Live vectors whose partition is not the live partition nearest them, of all live vectors.
    fn misplaced(conn: &Arc<crate::Connection>, live: &HashMap<i64, Vec<f32>>) -> (usize, usize) {
        let index = stored(conn);
        let mut centroids: Vec<(i64, Vec<f32>)> = Vec::new();
        for r in backing(conn, "centroids", "value") {
            let key = r[0].as_int().unwrap();
            let p = key >> VERSION_BITS;
            if key < 0 || index.part(p).state != State::Live || index.versions.get(&p) != Some(&(key & ((1 << VERSION_BITS) - 1))) {
                continue;
            }
            centroids.push((p, dense(r[1].to_blob()).unwrap()));
        }
        let mut wrong = 0;
        for r in backing(conn, "vectors", "location") {
            let rowid = r[0].as_int().unwrap();
            let p = match Location::decode(r[1].as_int().unwrap()) {
                Location::Block(key) | Location::Tail(key) => partition_of(key),
            };
            let v = &live[&rowid];
            let nearest = centroids.iter().min_by(|a, b| l2(&a.1, v).total_cmp(&l2(&b.1, v))).map(|(p, _)| *p).unwrap();
            wrong += (nearest != p) as usize;
        }
        (wrong, live.len())
    }

    /// Splits recheck the partitions beside them: once maintenance has run out of work over one
    /// uniform blob of vectors, where every partition borders many others, almost every vector lives
    /// in the live partition nearest it.
    #[test]
    fn vectors_live_in_their_nearest_partition() {
        const DIMS: usize = 16;
        for mvcc in [false, true] {
            let (_db, conn) = database(&format!("ivf-rq-nearest-{mvcc}.db"), mvcc);
            conn.execute("CREATE INDEX v_ivf ON v USING ivf_rq (e) WITH (nprobes = 1000, refine = 4)").unwrap();
            let mut state = 53;
            let mut live = HashMap::new();
            for id in 1..=3000i64 {
                let v = random(&mut state, DIMS);
                conn.execute(format!("INSERT INTO v VALUES ({id}, {})", literal(&v))).unwrap();
                live.insert(id, v);
            }
            conn.execute("OPTIMIZE INDEX v_ivf").unwrap();
            let (wrong, total) = misplaced(&conn, &live);
            eprintln!("mvcc={mvcc}: {wrong} of {total} vectors outside their nearest partition, {} partitions", stored(&conn).live().len());
            assert!(wrong * 100 < total * MISPLACED_PERCENT, "mvcc={mvcc}: {wrong} of {total} vectors outside their nearest partition");
        }
    }

    /// A database, a connection to it, and the rows of its table `f`.
    type Tagged = (Arc<crate::Database>, Arc<crate::Connection>, Vec<(i64, Vec<f32>)>);

    /// A table `f` of 3000 random vectors, each tagged with its id modulo 10, and rare when its id
    /// is a multiple of 200, under an ivf_rq index probing 16 partitions, about a third of them.
    fn tagged(name: &str, mvcc: bool) -> Tagged {
        let (db, conn) = database(name, mvcc);
        conn.execute("CREATE TABLE f(id INTEGER PRIMARY KEY, e BLOB NOT NULL, tag INTEGER NOT NULL, rare INTEGER NOT NULL)").unwrap();
        let mut state = 73;
        let rows: Vec<(i64, Vec<f32>)> = (1..=3000i64).map(|id| (id, random(&mut state, 16))).collect();
        let values: Vec<String> = rows.iter().map(|(id, v)| format!("({id}, {}, {}, {})", literal(v), id % 10, i64::from(id % 200 == 0))).collect();
        conn.execute(format!("INSERT INTO f VALUES {}", values.join(","))).unwrap();
        conn.execute("CREATE INDEX f_ivf ON f USING ivf_rq (e) WITH (nprobes = 16, refine = 4)").unwrap();
        (db, conn, rows)
    }

    /// The ids of the ten rows nearest `q` among those whose id `keep` accepts.
    fn exact_filtered(rows: &[(i64, Vec<f32>)], q: &[f32], keep: impl Fn(i64) -> bool) -> Vec<i64> {
        let mut exact: Vec<(f64, i64)> = rows.iter().filter(|(id, _)| keep(*id)).map(|(id, v)| (dot(q, v), *id)).collect();
        exact.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        exact.iter().take(10).map(|(_, id)| *id).collect()
    }

    fn opcodes(conn: &Arc<crate::Connection>, sql: &str) -> Vec<String> {
        rows(conn, &format!("EXPLAIN {sql}")).iter().map(|row| row[1].to_string()).collect()
    }

    fn ids(conn: &Arc<crate::Connection>, sql: &str) -> Vec<i64> {
        rows(conn, sql).iter().map(|row| row[0].as_int().unwrap()).collect()
    }

    /// A WHERE term the pattern does not cover no longer keeps a query off the index: the index
    /// gives out rows in order of distance and is told which the term accepts, and the answer is
    /// the exact top ten of the rows the term keeps, after no more than the four rounds a term
    /// keeping one row in ten needs.
    #[test]
    fn a_filtered_query_returns_the_exact_top_ten_of_the_rows_its_filter_keeps() {
        for mvcc in [false, true] {
            let (_db, conn, rows) = tagged(&format!("ivf-rq-filtered-{mvcc}.db"), mvcc);
            let mut state = 79;
            for _ in 0..3 {
                let q = random(&mut state, 16);
                let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM f WHERE tag = 3 ORDER BY distance LIMIT 10", literal(&q));
                let plan = opcodes(&conn, &sql);
                assert!(plan.iter().any(|op| op == "IndexMethodQuery") && plan.iter().any(|op| op == "IndexMethodAccept"), "mvcc={mvcc}: the filtered query does not go through the index: {plan:?}");
                let before = QUERY_ROUNDS.with(|count| count.get());
                let got = ids(&conn, &sql);
                let rounds = QUERY_ROUNDS.with(|count| count.get()) - before;
                assert_eq!(got, exact_filtered(&rows, &q, |id| id % 10 == 3), "mvcc={mvcc}");
                assert!(rounds <= 4, "mvcc={mvcc}: a filter keeping one row in ten took {rounds} rounds");
            }
        }
    }

    /// A filtered query is settled only when no row still to come can displace one of its accepted
    /// rows: a far row an earlier, narrower round accepted leaves it searching while the current
    /// round's next row is nearer, or, its rows all given out, while that far row lies past the
    /// round's reach, beyond which rows the round did not keep can still be nearer.
    #[test]
    fn a_far_row_an_earlier_round_accepted_does_not_settle_the_query() {
        let mut filter = Filter { limit: 2, round: 1, probed_all: true, whole: false, reach: 0.5, given: HashSet::new(), accepted: BinaryHeap::new() };
        filter.accepted.push(Ordered(0.9, 1));
        assert!(!filter.settled(Some(0.3)), "one accepted row cannot settle a query asking for two");
        filter.accepted.push(Ordered(0.1, 2));
        assert!(!filter.settled(Some(0.3)), "a nearer next row can still displace the far accepted row");
        assert!(filter.settled(Some(0.95)));
        assert!(!filter.settled(None), "rows past the round's reach can still be nearer than the far accepted row");
        filter.accepted.push(Ordered(0.2, 3));
        filter.accepted.pop();
        assert!(filter.settled(None), "with both accepted rows within the round's reach nothing past it can displace them");
    }

    /// A filter keeping one row in two hundred widens the search round by round until the rows the
    /// index gave out hold the ten nearest it keeps, so its answer is exact too.
    #[test]
    fn a_rare_filter_widens_the_search_until_its_answer_is_exact() {
        for mvcc in [false, true] {
            let (_db, conn, rows) = tagged(&format!("ivf-rq-rare-{mvcc}.db"), mvcc);
            let mut state = 83;
            for _ in 0..3 {
                let q = random(&mut state, 16);
                let sql = format!("SELECT id, vector_distance_dot(e, {}) AS distance FROM f WHERE rare = 1 ORDER BY distance LIMIT 10", literal(&q));
                assert!(opcodes(&conn, &sql).iter().any(|op| op == "IndexMethodAccept"), "mvcc={mvcc}: the filtered query does not go through the index");
                assert_eq!(ids(&conn, &sql), exact_filtered(&rows, &q, |id| id % 200 == 0), "mvcc={mvcc}");
            }
        }
    }

    /// The planner keeps a filtered query off the index where the plan without it costs less: a
    /// rowid lookup, and a filter a B-tree index serves and the planner takes to keep few rows.
    /// Each answer stays exact.
    #[test]
    fn a_filter_cheaper_served_without_the_index_keeps_its_plan() {
        for mvcc in [false, true] {
            let (_db, conn, rows) = tagged(&format!("ivf-rq-selective-{mvcc}.db"), mvcc);
            conn.execute("CREATE INDEX f_tag ON f(tag)").unwrap();
            let mut state = 89;
            let vector = random(&mut state, 16);
            let q = literal(&vector);
            let by_rowid = format!("SELECT id, vector_distance_dot(e, {q}) AS distance FROM f WHERE id = 1234 ORDER BY distance LIMIT 10");
            let by_tag = format!("SELECT id, vector_distance_dot(e, {q}) AS distance FROM f WHERE tag = 3 ORDER BY distance LIMIT 10");
            for sql in [&by_rowid, &by_tag] {
                let plan = opcodes(&conn, sql);
                assert!(!plan.iter().any(|op| op == "IndexMethodQuery"), "mvcc={mvcc}: {sql} goes through the index: {plan:?}");
            }
            assert_eq!(ids(&conn, &by_rowid), vec![1234], "mvcc={mvcc}");
            assert_eq!(ids(&conn, &by_tag), exact_filtered(&rows, &vector, |id| id % 10 == 3), "mvcc={mvcc}");
        }
    }

    /// The index applies the LIMIT to the rows it gives out, before a DISTINCT would thin them, so a
    /// DISTINCT query, filtered or not, is planned without it and applies its LIMIT after its
    /// DISTINCT; each answer is exact.
    #[test]
    fn a_distinct_query_applies_its_limit_after_its_distinct() {
        for mvcc in [false, true] {
            let (_db, conn, rows) = tagged(&format!("ivf-rq-distinct-{mvcc}.db"), mvcc);
            let mut state = 97;
            let vector = random(&mut state, 16);
            let q = literal(&vector);
            let all = format!("SELECT DISTINCT id, vector_distance_dot(e, {q}) AS distance FROM f ORDER BY distance LIMIT 10");
            let rare = format!("SELECT DISTINCT id, vector_distance_dot(e, {q}) AS distance FROM f WHERE rare = 1 ORDER BY distance LIMIT 10");
            for sql in [&all, &rare] {
                let plan = opcodes(&conn, sql);
                assert!(!plan.iter().any(|op| op == "IndexMethodQuery"), "mvcc={mvcc}: {sql} goes through the index: {plan:?}");
            }
            assert_eq!(ids(&conn, &all), exact_filtered(&rows, &vector, |_| true), "mvcc={mvcc}");
            assert_eq!(ids(&conn, &rare), exact_filtered(&rows, &vector, |id| id % 200 == 0), "mvcc={mvcc}");
        }
    }

    /// LIMIT 0 asks the index for no rows, and a negative LIMIT for every row, in order of distance
    /// from every partition, not only the probed ones.
    #[test]
    fn a_zero_limit_gives_no_rows_and_a_negative_one_every_row() {
        for mvcc in [false, true] {
            let (_db, conn, rows) = tagged(&format!("ivf-rq-limits-{mvcc}.db"), mvcc);
            let mut state = 101;
            let vector = random(&mut state, 16);
            let q = literal(&vector);
            let none = format!("SELECT id, vector_distance_dot(e, {q}) AS distance FROM f ORDER BY distance LIMIT 0");
            let every = format!("SELECT id, vector_distance_dot(e, {q}) AS distance FROM f ORDER BY distance LIMIT -1");
            for sql in [&none, &every] {
                assert!(opcodes(&conn, sql).iter().any(|op| op == "IndexMethodQuery"), "mvcc={mvcc}: {sql} does not go through the index");
            }
            assert!(ids(&conn, &none).is_empty(), "mvcc={mvcc}");
            let mut exact: Vec<(f64, i64)> = rows.iter().map(|(id, v)| (dot(&vector, v), *id)).collect();
            exact.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            assert_eq!(ids(&conn, &every), exact.iter().map(|(_, id)| *id).collect::<Vec<_>>(), "mvcc={mvcc}");
        }
    }

    /// The share of live vectors outside their nearest partition the nearest-partition test allows:
    /// over these 3000 vectors, rechecking 8 neighbours per split leaves 1.7%, 4 leave 5.3% and none
    /// 9.7%.
    const MISPLACED_PERCENT: usize = 3;
}
