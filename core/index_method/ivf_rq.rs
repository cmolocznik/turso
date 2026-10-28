//! IVF partitions with RaBitQ codes over dense f32 vectors, kept in core-owned backing tables, so
//! every change to the index commits in the transaction that changed its row, under WAL or MVCC.
//!
//! > CREATE INDEX t_idx ON t USING ivf_rq (embedding) WITH (nprobes = 16, refine = 4)
//! > SELECT vector_distance_dot(embedding, ?) AS distance FROM t ORDER BY distance LIMIT ?
//!
//! A write touches only rows owned by its own vector, so concurrent BEGIN CONCURRENT writers of
//! different rows never conflict. An insert codes the vector against its nearest partition's
//! centroid and writes two rows: a tail entry keyed by partition and rowid, and the vector's own
//! row (where its code lives, and its 4-bit code's lower bits). A delete removes the vector's row,
//! and its tail entry if it has one. A code packed in a block stays there until maintenance
//! rewrites the block, so the delete names the block in the vector's dead row, and a query skips
//! that stale copy before it can raise the bar the live entries are held to.
//!
//! `OPTIMIZE INDEX t_idx` is maintenance, bounded per run and itself a BEGIN CONCURRENT writer
//! that conflicts only with a write to a vector it moves or whose dead row it rewrites: it packs a
//! partition's tail into blocks of [`BLOCK`], rewrites a block holding [`COMPACT_AT`] stale copies
//! without them, and splits a partition grown past twice the partition count by 2-means over its
//! members' vectors read back from the table, so partitions stay near the square root of the row
//! count. Every rewrite of a block takes it off the dead rows of the copies it drops. A split
//! never overwrites a centroid: it writes new versions, and every entry names the version it was
//! coded against, so an insert committed from a snapshot older than the split still scores
//! correctly until maintenance recodes it. The index starts as one partition about the origin.
//!
//! Each vector carries a 4-bit extended RaBitQ code whose top bit is its 1-bit code. The 1-bit
//! codes are interleaved 32 vectors at a time for a fast scan over per-query nibble tables of
//! bytes. A query keeps the entries whose 1-bit estimate, raised by its error bound, reaches the
//! `limit`th best lowered one, scores those from their 4-bit codes in order of that bound until no
//! other can enter, and reranks the best `refine * limit` exactly from the table's vectors.
//!
//! Six backing tables, every key an integer so a seek compares no payload: blocks
//! (`partition << 32 | sequence` -> version, codes), tail (`partition << 40 | rowid` -> version,
//! code), vectors (rowid -> location, 4-bit correction and lower bits), centroids
//! (`partition << 24 | version` -> centroid; -1 -> a generation drawn afresh by each maintenance
//! change to them, under which cursors share the loaded model), parts (partition -> next block
//! sequence, packed size), dead (`partition << 40 | rowid` -> the blocks holding a stale copy of
//! the vector's code). Only maintenance writes blocks, centroids and parts.
use std::{
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::numeric::Numeric;
use crate::schema::Type;
use crate::types::IOResultOr;
use crate::{
    index_method::{
        parse_patterns, BackingColumn, BackingSchema, BackingStoreOp, BackingTable, IndexMethod,
        IndexMethodAttachment, IndexMethodConfiguration, IndexMethodContext, IndexMethodCursor,
        IndexMethodDefinition,
    },
    storage::btree::{BTreeKey, CursorTrait},
    sync::Arc,
    types::{IOResult, ImmutableRecord, SeekKey, SeekOp, SeekResult},
    vdbe::Register,
    vector::{
        operations,
        vector_types::{Vector, VectorType},
    },
    LimboError, Result, Value, ValueRef,
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
/// Maintenance done in this process: packs, splits, entries recoded, stale entries dropped, and
/// the summed and longest split durations, for measurement.
pub static PACKS: AtomicUsize = AtomicUsize::new(0);
/// Maintenance runs that found a job.
pub static JOBS: AtomicUsize = AtomicUsize::new(0);
pub static MOVES: AtomicUsize = AtomicUsize::new(0);
pub static SPLITS: AtomicUsize = AtomicUsize::new(0);
pub static RECODED: AtomicUsize = AtomicUsize::new(0);
pub static DROPPED: AtomicUsize = AtomicUsize::new(0);
/// Blocks rewritten without their stale copies.
pub static COMPACTS: AtomicUsize = AtomicUsize::new(0);
pub static SPLIT_MICROS: AtomicUsize = AtomicUsize::new(0);
pub static SPLIT_MICROS_MAX: AtomicUsize = AtomicUsize::new(0);
/// Entries a query estimated from 1-bit codes, and those it went on to score from 4-bit codes.
pub static SCANNED: AtomicUsize = AtomicUsize::new(0);
pub static REFINED: AtomicUsize = AtomicUsize::new(0);
/// Nanoseconds queries spent in each phase, and the records they read, for measurement.
pub static READ_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static SCORE_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static RECORDS: AtomicUsize = AtomicUsize::new(0);
pub static NEXT_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static SEEK_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static RERANK_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static OPEN_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static INIT_NANOS: AtomicUsize = AtomicUsize::new(0);
/// Nanoseconds inserts and deletes spent coding vectors and changing the tables, and the inserts.
pub static ENCODE_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static CHANGE_NANOS: AtomicUsize = AtomicUsize::new(0);
pub static INSERTS: AtomicUsize = AtomicUsize::new(0);

/// The most codes one block packs, and the tail length at which maintenance packs a partition.
const BLOCK: usize = 64;
/// The tail length at which maintenance packs a partition, merging it into the partition's open
/// block, so a partition holds full blocks, at most one open block, and a short tail.
const PACK_AT: usize = 8;
/// The stale copies a block holds at which maintenance rewrites it without them.
const COMPACT_AT: usize = 8;
const SEED: u64 = 0x9e37_79b9_7f4a_7c15;
const ROUNDS: usize = 3;
const KMEANS_ITERATIONS: usize = 8;
/// Bits per dimension past the sign: the extended code is 4 bits, its top bit the 1-bit code.
const EX_BITS: usize = 3;
/// Vectors scored together by one pass over the fast-scan tables.
const GROUP: usize = 32;
/// The fewest members a group needs to be fast-scanned rather than summed one by one.
const SCAN_MEMBERS: usize = 8;
/// The RaBitQ confidence multiplier on the 1-bit estimate's error.
const EPSILON: f64 = 1.9;
/// Scales tried when choosing a vector's 4-bit grid.
const GRID_SCALES: usize = 48;

const BLOCKS: usize = 0;
const TAIL: usize = 1;
const VECTORS: usize = 2;
const CENTROIDS: usize = 3;
const PARTS: usize = 4;
const DEAD: usize = 5;
const STORES: [(&str, &[(&str, Type)]); 6] = [
    ("blocks", &[("version", Type::Integer), ("codes", Type::Blob)]),
    ("tail", &[("version", Type::Integer), ("codes", Type::Blob)]),
    ("vectors", &[("location", Type::Integer), ("codes", Type::Blob)]),
    ("centroids", &[("value", Type::Blob)]),
    ("parts", &[("next", Type::Integer), ("size", Type::Integer), ("open", Type::Integer), ("move_next", Type::Integer), ("move_end", Type::Integer)]),
    ("dead", &[("blocks", Type::Blob)]),
];
/// The centroids table's rowid for the generation.
const GENERATION: i64 = -1;
const PARTITION_BITS: u32 = 23;
const ROWID_BITS: u32 = 40;
const VERSION_BITS: u32 = 24;

fn error(message: &str) -> Box<LimboError> {
    Box::new(LimboError::InternalError(format!("ivf_rq: {message}")))
}

fn partition_in_range(partition: i64) -> Result<()> {
    if !(0..1 << PARTITION_BITS).contains(&partition) {
        return Err(*error("partition out of range"));
    }
    Ok(())
}

/// A packed block's key: its partition, then its sequence within the partition.
fn block_key(partition: i64, sequence: i64) -> Result<i64> {
    partition_in_range(partition)?;
    if !(0..1 << 32).contains(&sequence) {
        return Err(*error("block sequence out of range"));
    }
    Ok(partition << 32 | sequence)
}

/// A tail entry's key: its partition, then the vector's rowid.
fn tail_key(partition: i64, rowid: i64) -> Result<i64> {
    partition_in_range(partition)?;
    if !(0..1 << ROWID_BITS).contains(&rowid) {
        return Err(*error("rows indexed by ivf_rq need rowids in 0..2^40"));
    }
    Ok(partition << ROWID_BITS | rowid)
}

/// A dead row's key: the partition of the block holding the stale copy, then the vector's rowid.
fn dead_key(block: i64, rowid: i64) -> Result<i64> {
    tail_key(block >> 32, rowid)
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

/// The direction-fit of the 4-bit grid at scale `t`: the code's product with `x` and its squared
/// length.
#[inline(always)]
fn fit_body(x: &[f32], t: f32) -> (f32, f32) {
    let (mut along, mut length) = ([0.0f32; LANES], [0.0f32; LANES]);
    let (head, tail) = x.split_at(x.len() / LANES * LANES);
    for chunk in head.chunks_exact(LANES) {
        for k in 0..LANES {
            let value = ((chunk[k] * t).floor() + 8.0).clamp(0.0, 15.0) - 7.5;
            along[k] += chunk[k] * value;
            length[k] += value * value;
        }
    }
    let (mut a, mut l): (f32, f32) = (along.iter().sum(), length.iter().sum());
    for v in tail {
        let value = ((v * t).floor() + 8.0).clamp(0.0, 15.0) - 7.5;
        a += v * value;
        l += value * value;
    }
    (a, l)
}

#[inline(always)]
fn hadamard_body(x: &mut [f32], signs: &[f32], scale: f32) {
    let padded = x.len();
    for round in signs.chunks_exact(padded) {
        x.iter_mut().zip(round).for_each(|(a, s)| *a *= s);
        let mut h = 1;
        while h < padded {
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
}

kernel!(l2, l2_wide, l2_body, (a: &[f32], b: &[f32]) -> f32);
kernel!(dot32, dot_wide, dot_body, (a: &[f32], b: &[f32]) -> f32);
kernel!(fit, fit_wide, fit_body, (x: &[f32], t: f32) -> (f32, f32));
kernel!(hadamard, hadamard_wide, hadamard_body, (x: &mut [f32], signs: &[f32], scale: f32) -> ());

/// A randomized Hadamard rotation of vectors padded to a power of two: seeded sign flips, each
/// followed by a fast Walsh-Hadamard transform, normalized so lengths are kept.
#[derive(Debug, Clone)]
struct Rotation {
    padded: usize,
    signs: Vec<f32>,
}

impl Rotation {
    fn new(dims: usize) -> Self {
        let padded = dims.next_power_of_two().max(8);
        let mut state = SEED;
        let signs = (0..ROUNDS * padded)
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
        Self { padded, signs }
    }

    fn apply(&self, v: &[f32]) -> Vec<f32> {
        let mut x = vec![0.0f32; self.padded];
        x[..v.len()].copy_from_slice(v);
        hadamard(&mut x, &self.signs, 1.0 / (self.padded as f32).sqrt());
        x
    }

    fn code_bytes(&self) -> usize {
        self.padded / 8
    }
}

/// A vector's code: its rowid, the residual's length, the 1-bit and 4-bit codes' corrections (each
/// the inner product of the true unit residual with its code), the rotated signs, and the three
/// lower bits of each dimension's 4-bit code, packed eight dimensions to three bytes.
#[derive(Clone)]
struct Entry {
    rowid: i64,
    norm: f32,
    factor: f32,
    refined: f32,
    bits: Vec<u8>,
    ex: Vec<u8>,
}

/// A block, laid out so a query reads it in place: count u16; rowids i64[n]; norms and factors
/// f32[n] each; then per group of 32 entries, for each nibble position j of the 1-bit code, 16
/// bytes whose low nibbles are entries 0..16's nibble j and high nibbles entries 16..32's.
struct Layout {
    count: usize,
    positions: usize,
}

impl Layout {
    fn new(count: usize, code_bytes: usize) -> Self {
        Self { count, positions: code_bytes * 2 }
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
    fn norms(&self) -> usize {
        self.rowids() + 8 * self.count
    }
    fn factors(&self) -> usize {
        self.norms() + 4 * self.count
    }
    fn scan(&self) -> usize {
        self.factors() + 4 * self.count
    }
    fn len(&self) -> usize {
        self.scan() + self.groups() * self.group_bytes()
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

fn pack(entries: &[Entry]) -> Vec<u8> {
    let code_bytes = entries.first().map_or(0, |e| e.bits.len());
    let layout = Layout::new(entries.len(), code_bytes);
    let mut out = vec![0u8; layout.len()];
    out[..2].copy_from_slice(&(entries.len() as u16).to_le_bytes());
    for (n, e) in entries.iter().enumerate() {
        out[layout.rowids() + 8 * n..][..8].copy_from_slice(&e.rowid.to_le_bytes());
        out[layout.norms() + 4 * n..][..4].copy_from_slice(&e.norm.to_le_bytes());
        out[layout.factors() + 4 * n..][..4].copy_from_slice(&e.factor.to_le_bytes());
        let group = layout.scan() + (n / GROUP) * layout.group_bytes();
        let (slot, shift) = (n % 16, if n % GROUP < 16 { 0 } else { 4 });
        for j in 0..layout.positions {
            let nibble = (e.bits[j / 2] >> (4 * (j & 1))) & 0xf;
            out[group + j * 16 + slot] |= nibble << shift;
        }
    }
    out
}

/// The code width a block of `count` entries and `len` bytes was packed at.
fn block_code_bytes(payload: &[u8]) -> Result<usize> {
    let count = block_count(payload)?;
    let fixed = Layout::new(count, 0).len();
    let groups = count.div_ceil(GROUP).max(1);
    let scanned = payload.len().checked_sub(fixed).ok_or_else(|| LimboError::Corrupt("ivf_rq: short block".into()))?;
    if count == 0 || scanned % (groups * 32) != 0 {
        return Err(LimboError::Corrupt("ivf_rq: block length differs from its count".into()));
    }
    Ok(scanned / (groups * 32))
}

/// A block's entries, without the 4-bit parts that live in the vectors table.
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
            Entry {
                rowid: i64_at(payload, layout.rowids() + 8 * n),
                norm: f32_at(payload, layout.norms() + 4 * n),
                factor: f32_at(payload, layout.factors() + 4 * n),
                refined: 0.0,
                bits,
                ex: Vec::new(),
            }
        })
        .collect())
}

/// An entry's extra record: its 4-bit correction, then its lower bits.
fn extra(entry: &Entry) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + entry.ex.len());
    out.extend_from_slice(&entry.refined.to_le_bytes());
    out.extend_from_slice(&entry.ex);
    out
}

/// The rotated query's product with an entry's lower bits: the 4-bit code's part below the sign.
fn lower_product(rotated: &[f32], ex: &[u8]) -> f64 {
    let mut lower = 0.0f64;
    for (bytes, q) in ex.chunks_exact(3).zip(rotated.chunks_exact(8)) {
        let word = bytes[0] as u32 | (bytes[1] as u32) << 8 | (bytes[2] as u32) << 16;
        let mut part = 0.0f32;
        for (k, x) in q.iter().enumerate() {
            part += *x * ((word >> (3 * k)) & 7) as f32;
        }
        lower += part as f64;
    }
    lower
}

/// The 4-bit estimate of the residual's product with the rotated query, `signed` being the query
/// summed over the entry's set sign bits: the code's values are `code - 7.5`.
fn refined_product(signed: f64, lower: f64, sum: f64, refined: f64) -> f64 {
    (8.0 * signed + lower - 7.5 * sum) / refined
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
/// against them. Partition 0 at version 0 with no stored centroid is the untrained origin.
#[derive(Debug, Clone, Default)]
struct Model {
    centroids: Vec<Vec<f32>>,
    /// Each centroid's squared length.
    squares: Vec<f32>,
    /// Each partition's current version.
    versions: Vec<i64>,
    /// Superseded centroids, by partition and version.
    older: HashMap<(i64, i64), Vec<f32>>,
    eta: f32,
}

impl Model {
    fn new(centroids: Vec<Vec<f32>>, eta: f32) -> Self {
        let squares = centroids.iter().map(|c| dot32(c, c)).collect();
        let versions = vec![0; centroids.len()];
        Self { centroids, squares, versions, older: HashMap::new(), eta }
    }

    /// Set a partition's centroid at a version, keeping the one it supersedes.
    fn place(&mut self, partition: usize, version: i64, c: Vec<f32>) {
        let square = dot32(&c, &c);
        if partition == self.centroids.len() {
            self.centroids.push(c);
            self.squares.push(square);
            self.versions.push(version);
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
        let square = dot32(v, v);
        let length = square.sqrt().max(f32::MIN_POSITIVE);
        let mut best = (f32::INFINITY, 0);
        for (i, (c, c_square)) in self.centroids.iter().zip(&self.squares).enumerate() {
            let product = dot32(c, v);
            let mut loss = square + c_square - 2.0 * product;
            if self.eta > 1.0 {
                let along = length - product / length;
                loss += (self.eta - 1.0) * along * along;
            }
            if loss < best.0 {
                best = (loss, i);
            }
        }
        best.1
    }

    /// Code a vector against its nearest partition's current centroid: the partition, the
    /// centroid's version and the code.
    fn encode(&self, rotation: &Rotation, rowid: i64, v: &[f32]) -> (i64, i64, Entry) {
        if self.centroids.is_empty() {
            return (0, 0, code(rotation, rowid, v.to_vec()));
        }
        let p = self.nearest(v);
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
        return Entry { rowid, norm: 0.0, factor: 1.0, refined: 1.0, bits, ex };
    }
    let unit: Vec<f32> = residual.iter().map(|x| (*x as f64 / norm) as f32).collect();
    let rotated = rotation.apply(&unit);
    let codes = grid(&rotated);
    let mut absolute = 0.0f64;
    let mut refined = 0.0f64;
    for (i, (x, c)) in rotated.iter().zip(&codes).enumerate() {
        bits[i / 8] |= ((c >> 3) as u8) << (i % 8);
        let at = (i / 8) * 3;
        let word = (c & 7) << (3 * (i % 8));
        ex[at] |= word as u8;
        ex[at + 1] |= (word >> 8) as u8;
        ex[at + 2] |= (word >> 16) as u8;
        absolute += x.abs() as f64;
        refined += *x as f64 * (*c as f64 - 7.5);
    }
    let factor = (absolute / (rotation.padded as f64).sqrt()).max(f64::MIN_POSITIVE);
    Entry { rowid, norm: norm as f32, factor: factor as f32, refined: refined.max(f64::MIN_POSITIVE) as f32, bits, ex }
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
fn walk_row(cursor: &mut Box<dyn CursorTrait>, start: i64, end: i64, state: &mut Walk) -> IOResultOr<Option<(i64, ImmutableRecord)>> {
    loop {
        match *state {
            Walk::Seek => match {
                cursor.set_forward_bound(Some(end));
                cursor.seek(SeekKey::TableRowId(start), SeekOp::GE { eq_only: false })?
            } {
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
                IOResult::Done(SeekResult::Found) => *state = Walk::Read,
                IOResult::Done(SeekResult::TryAdvance) => *state = Walk::Next,
                IOResult::Done(SeekResult::NotFound) => return Ok(IOResult::Done(None)),
            },
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
                let found = match cursor.record()? {
                    IOResult::Done(found) => found.ok_or_else(|| error("positioned cursor has no record"))?.clone(),
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                };
                *state = Walk::Next;
                return Ok(IOResult::Done(Some((rowid, found))));
            }
        }
    }
}

/// One write to one backing table, resumable.
enum Step {
    /// Write the row, replacing any with its rowid; `true` once its position is sought.
    Put(usize, i64, ImmutableRecord, bool),
    /// Delete the row; it must exist unless `required` is false. `true` once it is sought.
    Remove { store: usize, rowid: i64, required: bool, sought: bool },
}

fn apply_step(stores: &mut [Box<dyn CursorTrait>], step: &mut Step) -> IOResultOr<()> {
    match step {
        Step::Put(store, rowid, row, sought) => {
            let cursor = &mut stores[*store];
            if !*sought {
                if let IOResult::IO(io) = cursor.seek(SeekKey::TableRowId(*rowid), SeekOp::GE { eq_only: true })? {
                    return Ok(IOResult::IO(io));
                }
                *sought = true;
            }
            cursor.insert(&BTreeKey::new_table_rowid(*rowid, Some(row)))
        }
        Step::Remove { store, rowid, required, sought } => {
            let cursor = &mut stores[*store];
            if !*sought {
                match cursor.seek(SeekKey::TableRowId(*rowid), SeekOp::GE { eq_only: true })? {
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    IOResult::Done(SeekResult::Found) => *sought = true,
                    IOResult::Done(_) if *required => return Err(error("removed row is absent")),
                    IOResult::Done(_) => return Ok(IOResult::Done(())),
                }
            }
            cursor.delete()
        }
    }
}

fn put(store: usize, rowid: i64, row: ImmutableRecord) -> Step {
    Step::Put(store, rowid, row, false)
}

fn remove(store: usize, rowid: i64) -> Step {
    Step::Remove { store, rowid, required: true, sought: false }
}


/// The next rowid of the walk over `start..end`, its record left unread, or none past the end.
fn walk_key(cursor: &mut Box<dyn CursorTrait>, start: i64, end: i64, state: &mut Walk) -> IOResultOr<Option<i64>> {
    loop {
        match *state {
            Walk::Seek => match {
                cursor.set_forward_bound(Some(end));
                cursor.seek(SeekKey::TableRowId(start), SeekOp::GE { eq_only: false })?
            } {
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
                IOResult::Done(SeekResult::Found) => *state = Walk::Read,
                IOResult::Done(SeekResult::TryAdvance) => *state = Walk::Next,
                IOResult::Done(SeekResult::NotFound) => return Ok(IOResult::Done(None)),
            },
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
                *state = Walk::Next;
                return Ok(IOResult::Done(Some(rowid)));
            }
        }
    }
}

/// The 4-bit code of a rotated unit vector: values on the half-integer grid -7.5..=7.5 at the
/// scale whose code points closest to the vector's direction. A value's sign is the vector's.
fn grid(x: &[f32]) -> Vec<u32> {
    let peak = x.iter().fold(0.0f32, |m, v| m.max(v.abs()));
    let code = |v: f32, t: f32| ((v * t).floor() + 8.0).clamp(0.0, 15.0) as u32;
    let mut best = (f32::NEG_INFINITY, 1.0f32);
    if peak > 0.0 {
        let top = 8.0 / peak;
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

/// Lloyd's k-means over `sample`, seeded by k-means++, its assignment passes split over the cores.
fn kmeans(sample: &[Vec<f32>], k: usize, eta: f32) -> Vec<Vec<f32>> {
    let k = k.min(sample.len()).max(1);
    let dims = sample[0].len();
    let mut state = SEED ^ 0x5bd1_e995;
    let mut random = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
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
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let chunk = sample.len().div_ceil(threads);
    for _ in 0..KMEANS_ITERATIONS {
        let model = Model::new(centroids.clone(), eta);
        let partial: Vec<(Vec<Vec<f64>>, Vec<usize>)> = std::thread::scope(|scope| {
            sample
                .chunks(chunk)
                .map(|part| {
                    let model = &model;
                    scope.spawn(move || {
                        let mut sums = vec![vec![0.0f64; dims]; k];
                        let mut counts = vec![0usize; k];
                        for v in part {
                            let p = model.nearest(v);
                            counts[p] += 1;
                            sums[p].iter_mut().zip(v).for_each(|(s, x)| *s += *x as f64);
                        }
                        (sums, counts)
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().expect("k-means worker"))
                .collect()
        });
        for p in 0..k {
            let count: usize = partial.iter().map(|(_, c)| c[p]).sum();
            if count == 0 {
                continue;
            }
            for d in 0..dims {
                let s: f64 = partial.iter().map(|(sums, _)| sums[p][d]).sum();
                centroids[p][d] = (s / count as f64) as f32;
            }
        }
    }
    centroids
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
        Ok(Arc::new(IvfRqIndexMethodAttachment {
            configuration: configuration.clone(),
            shared: Shared::default(),
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

/// The model last loaded or written under each generation, shared by the index's cursors.
type Shared = Arc<std::sync::Mutex<Option<(i64, Arc<Model>)>>>;

enum Load {
    /// Read the generation: a model cached under it is the stored model, so nothing else is read.
    Generation(Get),
    Walk(Walk),
}

/// A delete in progress: read the vector's row, then remove it and its tail entry, or name its
/// packed block in its dead row.
enum Change {
    Idle,
    Remove { rowid: i64, get: Get },
    Bury { rowid: i64, block: i64, get: Get },
    Apply(VecDeque<Step>),
}

/// An entry a query kept from its 1-bit estimate: the bound it may reach, what its 4-bit estimate
/// needs, and where it was read, which the vector's row must still name.
struct Candidate {
    upper: f64,
    rowid: i64,
    centre: f64,
    norm: f64,
    signed: f64,
    source: Location,
}

#[derive(Clone, Copy)]
enum Stage {
    Blocks,
    Tail,
}

enum Search {
    Init,
    /// Read a partition's dead rows, so its stale copies are known before its blocks are scored.
    Dead { probes: VecDeque<i64>, partition: i64, walk: Walk },
    Probe { probes: VecDeque<i64>, partition: i64, stage: Stage, walk: Walk },
    Refine { order: VecDeque<Candidate>, get: Get },
    Rerank { candidates: VecDeque<i64>, rowid: Option<i64> },
}

/// An entry a pack read: where it was, the centroid version it was coded against, its code.
struct Tailed {
    location: Location,
    version: i64,
    entry: Entry,
}

/// What maintenance knows of a partition's blocks: the next sequence, the packed entries, how many
/// the last block holds when it is not full (zero when it is), and, after a split, the blocks
/// still to check for entries now nearer another partition: sequences `move_next..move_end`.
#[derive(Clone, Copy, Default)]
struct Part {
    next: i64,
    size: i64,
    open: i64,
    move_next: i64,
    move_end: i64,
}

impl Part {
    fn record(&self) -> Result<ImmutableRecord> {
        record(&[self.next, self.size, self.open, self.move_next, self.move_end].map(Value::from_i64))
    }

    fn moving(&self) -> bool {
        self.move_next < self.move_end
    }
}

type Parts = HashMap<i64, Part>;

/// The one job a maintenance run does. Each touches a bounded number of rows, so a run is a short
/// transaction that conflicts with a writer only on a vector both change, and retrying it is cheap.
#[derive(Clone, Copy, Debug)]
enum Job {
    /// Merge up to [`MOVE_BATCH`] tail entries into the partition's open block.
    Pack(i64),
    /// Choose the partition's two new centroids from a sample of its members and write them; the
    /// entries move later, batch by batch.
    Split(i64),
    /// Recode up to a block of entries coded against a superseded centroid into the partitions now
    /// nearest them.
    Move(i64),
    /// Rewrite the block with this key without the stale copies it holds.
    Compact(i64),
}

/// A maintenance run: count every partition's tail and read the bookkeeping, choose the job, read
/// the entries it concerns, check each against its vector's row, read the vectors it recodes, and
/// write.
enum Upkeep {
    Init,
    Load,
    Count { walk: Walk, tails: HashMap<i64, usize> },
    Parts { walk: Walk, tails: HashMap<i64, usize>, parts: Parts },
    /// Count the stale copies the dead rows name in each block.
    Dead { walk: Walk, tails: HashMap<i64, usize>, parts: Parts, stale: HashMap<i64, usize> },
    /// Read the entries of `job`: the open block and the tail for a pack, every member for a
    /// split's sample, the first superseded block or tail entries for a move, the block for a
    /// compaction.
    Read { job: Job, part: Part, stage: Stage, walk: Walk, get: Get, rows: Vec<Tailed> },
    Check { job: Job, part: Part, rows: Vec<Tailed>, at: usize, get: Get, sorted: Sorted },
    /// Read the dead row of each stale copy a rewritten block drops, to take the block off it.
    Unbury { job: Job, part: Part, sorted: Sorted, at: usize, get: Get },
    Fetch { job: Job, part: Part, sorted: Sorted, vectors: Vec<Vec<f32>> },
    Apply { steps: VecDeque<Step> },
}

/// A job's entries, sorted by what their check found.
#[derive(Default)]
struct Sorted {
    /// Current, with their extra codes as their vector's row holds them.
    live: Vec<(Entry, Vec<u8>)>,
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
}

/// Members a split samples for its 2-means.
const SPLIT_SAMPLE: usize = 256;
/// The most entries a pack or move rewrites in one run.
const MOVE_BATCH: usize = 64;

struct Query {
    vector: Vec<f32>,
    /// The query as the engine's vector, for the exact rerank.
    asked: Vector<'static>,
    /// The query under the index's rotation, its length and its summed components.
    rotated: Vec<f32>,
    length: f64,
    sum: f64,
    scan: ScanTable,
    limit: usize,
    keep: usize,
    /// The query's product with each centroid version it has met.
    centres: HashMap<(i64, i64), f64>,
    /// The stale copies the probed partitions' dead rows name, as rowid and block.
    dead: HashSet<(i64, i64)>,
    /// The `limit` best lowered 1-bit estimates of live entries, negated: the top is the bar an
    /// entry's raised estimate must reach.
    floors: BinaryHeap<Ordered>,
    candidates: Vec<Candidate>,
    estimates: BinaryHeap<Ordered>,
    results: BinaryHeap<Ordered>,
}

impl Query {
    fn bar(&self) -> f64 {
        if self.floors.len() < self.limit {
            f64::NEG_INFINITY
        } else {
            self.floors.peek().map_or(f64::NEG_INFINITY, |o| -o.0)
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
    answer: VecDeque<(i64, f64)>,
    upkeep: Upkeep,
    generation: Option<i64>,
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
            answer: VecDeque::new(),
            upkeep: Upkeep::Init,
            generation: None,
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
        if self.rotation.as_ref().is_none_or(|r| r.padded != dims.next_power_of_two().max(8)) {
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
            *self.shared.lock().expect("model cache") = Some((g, self.model.clone()));
        }
    }

    /// The size past which a partition is split: twice the partition count, so partitions stay
    /// near the square root of the row count.
    fn split_bound(&self) -> i64 {
        (2 * self.model.centroids.len()).max(2 * BLOCK) as i64
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
                    let cached = self.shared.lock().expect("model cache").as_ref().filter(|(g, _)| Some(*g) == stored).map(|(_, m)| m.clone());
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
                        self.load = Load::Generation(Get::Seek);
                        self.loaded = true;
                        if let Some(dims) = self.model.centroids.first().map(Vec::len) {
                            let eta = self.eta(dims);
                            Arc::make_mut(&mut self.model).eta = eta;
                        }
                        self.publish();
                        return Ok(IOResult::Done(()));
                    };
                    let (partition, version) = (key >> VERSION_BITS, key & ((1 << VERSION_BITS) - 1));
                    let centroid = dense(Some(bytes_at(&found, 0)?))?;
                    let model = Arc::make_mut(&mut self.model);
                    if partition as usize > model.centroids.len() {
                        return Err(LimboError::Corrupt("ivf_rq: centroid partitions are not dense".into()).into());
                    }
                    model.place(partition as usize, version, centroid);
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
                    self.change = match Location::decode(integer(&found, 0)?) {
                        Location::Tail(key) => Change::Apply(VecDeque::from([remove(VECTORS, rowid), remove(TAIL, key)])),
                        Location::Block(block) => Change::Bury { rowid, block, get: Get::Seek },
                    };
                }
                Change::Bury { rowid, block, mut get } => {
                    let key = dead_key(block, rowid)?;
                    let found = match get_row(&mut self.stores[DEAD], key, &mut get)? {
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
                    self.change = Change::Apply(VecDeque::from([remove(VECTORS, rowid), put(DEAD, key, dead_record(&blocks)?)]));
                }
                Change::Apply(mut steps) => {
                    let Some(step) = steps.front_mut() else {
                        return Ok(IOResult::Done(()));
                    };
                    match apply_step(&mut self.stores, step)? {
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

    /// The writes that give a vector its code at the end of its nearest partition's tail.
    fn tail_steps(&mut self, rowid: i64, vector: &[f32]) -> Result<[Step; 2]> {
        let rotation = self.rotation(vector.len());
        let (partition, version, entry) = self.model.encode(&rotation, rowid, vector);
        let key = tail_key(partition, rowid)?;
        Ok([
            put(TAIL, key, record(&[Value::from_i64(version), blob(&pack(std::slice::from_ref(&entry)))?])?),
            put(VECTORS, rowid, record(&[Value::from_i64(Location::Tail(key).encode()), blob(&extra(&entry))?])?),
        ])
    }
}

/// Adds the time since its start to a counter when dropped.
struct Timer<'a>(&'a AtomicUsize, std::time::Instant);

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(self.1.elapsed().as_nanos() as usize, Ordering::Relaxed);
    }
}

fn timer(counter: &AtomicUsize) -> Timer<'_> {
    Timer(counter, std::time::Instant::now())
}

impl IndexMethodCursor for IvfRqCursor {
    fn create(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.pending.is_empty() {
            self.pending.push_back(context.create_backing_schema(&self.schema)?);
        }
        self.drive_pending()
    }

    fn destroy(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.pending.is_empty() {
            self.pending.push_back(context.drop_backing_schema(&self.schema)?);
        }
        self.drive_pending()
    }

    fn open_read(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        let _timer = timer(&OPEN_NANOS);
        self.open_stores(context)?;
        self.load()
    }

    fn open_write(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        self.open_stores(context)?;
        self.load()
    }

    fn close(&mut self, _context: &IndexMethodContext) {
        self.stores.clear();
        self.main = None;
    }

    fn insert(&mut self, values: &[Register]) -> IOResultOr<()> {
        if matches!(self.change, Change::Idle) {
            let vector = dense(values[0].get_value().to_blob())?;
            let rowid = values[1].get_value().as_int().ok_or_else(|| error("rowid must be an integer"))?;
            let encoding = timer(&ENCODE_NANOS);
            let steps = self.tail_steps(rowid, &vector)?;
            drop(encoding);
            INSERTS.fetch_add(1, Ordering::Relaxed);
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
        loop {
            match std::mem::replace(&mut self.search, Search::Init) {
                Search::Init => {
                    let _timer = timer(&INIT_NANOS);
                    let vector = dense(values[1].get_value().to_blob())?;
                    let limit = values[2].get_value().as_int().ok_or_else(|| error("limit must be an integer"))?.max(0) as usize;
                    let rotation = self.rotation(vector.len());
                    let rotated = rotation.apply(&vector);
                    let sum = rotated.iter().map(|x| *x as f64).sum();
                    let length = rotated.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();
                    let scan = ScanTable::new(&rotated);
                    let nprobes = match NPROBES_OVERRIDE.load(Ordering::Relaxed) {
                        0 => self.nprobes,
                        n => n,
                    };
                    let refine = match REFINE_OVERRIDE.load(Ordering::Relaxed) {
                        0 => self.refine,
                        n => n,
                    };
                    let probes: VecDeque<i64> = if self.model.centroids.is_empty() {
                        VecDeque::from([0])
                    } else {
                        let mut ranked: Vec<(f32, i64)> = self
                            .model
                            .centroids
                            .iter()
                            .enumerate()
                            .map(|(i, c)| match PROBE_BY_PRODUCT.load(Ordering::Relaxed) {
                                0 => (l2(c, &vector), i as i64),
                                _ => (-dot32(c, &vector), i as i64),
                            })
                            .collect();
                        let take = nprobes.min(ranked.len());
                        ranked.select_nth_unstable_by(take - 1, |a, b| a.0.total_cmp(&b.0));
                        ranked.truncate(take);
                        ranked.into_iter().map(|(_, p)| p).collect()
                    };
                    let asked = Vector::from_slice_owned(values[1].get_value().to_blob().ok_or_else(|| error("query is not a vector"))?)?;
                    self.query = Some(Query {
                        vector,
                        asked,
                        rotated,
                        length,
                        sum,
                        scan,
                        limit: limit.max(1),
                        keep: (refine * limit).max(1),
                        centres: HashMap::new(),
                        dead: HashSet::new(),
                        floors: BinaryHeap::new(),
                        candidates: Vec::new(),
                        estimates: BinaryHeap::new(),
                        results: BinaryHeap::new(),
                    });
                    self.answer.clear();
                    self.search = self.next_probe(probes)?;
                }
                Search::Dead { probes, partition, mut walk } => {
                    let start = tail_key(partition, 0)?;
                    let reading = timer(&READ_NANOS);
                    let row = match walk_row(&mut self.stores[DEAD], start, start + (1 << ROWID_BITS), &mut walk)? {
                        IOResult::Done(row) => row,
                        IOResult::IO(io) => {
                            self.search = Search::Dead { probes, partition, walk };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    drop(reading);
                    let Some((key, found)) = row else {
                        self.search = Search::Probe { probes, partition, stage: Stage::Blocks, walk: Walk::Seek };
                        continue;
                    };
                    let query = self.query.as_mut().ok_or_else(|| error("query state absent"))?;
                    let rowid = key & ((1 << ROWID_BITS) - 1);
                    query.dead.extend(dead_blocks(bytes_at(&found, 0)?)?.into_iter().map(|block| (rowid, block)));
                    self.search = Search::Dead { probes, partition, walk };
                }
                Search::Probe { probes, partition, stage, mut walk } => {
                    let (store, start, end) = match stage {
                        Stage::Blocks => (BLOCKS, block_key(partition, 0)?, block_key(partition, 0)? + (1 << 32)),
                        Stage::Tail => (TAIL, tail_key(partition, 0)?, tail_key(partition, 0)? + (1 << ROWID_BITS)),
                    };
                    let reading = timer(&READ_NANOS);
                    let row = match walk_row(&mut self.stores[store], start, end, &mut walk)? {
                        IOResult::Done(row) => row,
                        IOResult::IO(io) => {
                            self.search = Search::Probe { probes, partition, stage, walk };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    drop(reading);
                    match (row, stage) {
                        (Some((key, found)), _) => {
                            RECORDS.fetch_add(1, Ordering::Relaxed);
                            let source = match stage {
                                Stage::Blocks => Location::Block(key),
                                Stage::Tail => Location::Tail(key),
                            };
                            let query = self.query.as_mut().ok_or_else(|| error("query state absent"))?;
                            let centre = query.centre(&self.model, partition, integer(&found, 0)?)?;
                            let _scoring = timer(&SCORE_NANOS);
                            score_block(query, bytes_at(&found, 1)?, centre, source)?;
                            self.search = Search::Probe { probes, partition, stage, walk };
                        }
                        (None, Stage::Blocks) => self.search = Search::Probe { probes, partition, stage: Stage::Tail, walk: Walk::Seek },
                        (None, Stage::Tail) => self.search = self.next_probe(probes)?,
                    }
                }
                Search::Refine { mut order, mut get } => {
                    let query = self.query.as_mut().ok_or_else(|| error("query state absent"))?;
                    let Some(candidate) = order.front() else {
                        self.search = Search::Rerank { candidates: VecDeque::new(), rowid: None };
                        continue;
                    };
                    // Candidates come in order of the bound they may reach: once one cannot enter
                    // the full set of 4-bit estimates, none after it can.
                    if query.estimates.len() >= query.keep && query.estimates.peek().is_some_and(|worst| -candidate.upper >= worst.0) {
                        self.search = Search::Rerank { candidates: VecDeque::new(), rowid: None };
                        continue;
                    }
                    let fetching = timer(&SEEK_NANOS);
                    let found = match get_row(&mut self.stores[VECTORS], candidate.rowid, &mut get)? {
                        IOResult::Done(found) => found,
                        IOResult::IO(io) => {
                            self.search = Search::Refine { order, get };
                            return Ok(IOResult::IO(io));
                        }
                    };
                    drop(fetching);
                    // An entry its vector's row no longer names is stale: deleted, revised or moved.
                    if let Some(found) = found.filter(|r| integer(r, 0).is_ok_and(|l| Location::decode(l) == candidate.source)) {
                        REFINED.fetch_add(1, Ordering::Relaxed);
                        let codes = bytes_at(&found, 1)?;
                        let refined = f32_at(codes, 0) as f64;
                        let lower = lower_product(&query.rotated, &codes[4..]);
                        let estimate = candidate.centre + candidate.norm * refined_product(candidate.signed, lower, query.sum, refined);
                        query.estimates.push(Ordered(-estimate, candidate.rowid));
                        if query.estimates.len() > query.keep {
                            query.estimates.pop();
                        }
                    }
                    order.pop_front();
                    self.search = Search::Refine { order, get: Get::Seek };
                }
                Search::Rerank { mut candidates, rowid } => {
                    let _timer = timer(&RERANK_NANOS);
                    let query = self.query.as_mut().ok_or_else(|| error("query state absent"))?;
                    if candidates.is_empty() && rowid.is_none() && !query.estimates.is_empty() {
                        candidates = std::mem::take(&mut query.estimates).into_iter().map(|o| o.1).collect();
                    }
                    let Some(current) = rowid.or_else(|| candidates.pop_front()) else {
                        let mut results: Vec<Ordered> = std::mem::take(&mut query.results).into_vec();
                        results.sort();
                        self.answer = results.into_iter().map(|o| (o.1, o.0)).collect();
                        self.search = Search::Init;
                        return Ok(IOResult::Done(!self.answer.is_empty()));
                    };
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

    fn query_next(&mut self) -> IOResultOr<bool> {
        let _ = self.answer.pop_front();
        Ok(IOResult::Done(!self.answer.is_empty()))
    }

    fn query_column(&mut self, _: usize) -> IOResultOr<Value> {
        let (_, distance) = self.answer.front().ok_or_else(|| error("no current row"))?;
        Ok(IOResult::Done(Value::from_f64(*distance)))
    }

    fn query_rowid(&mut self) -> IOResultOr<Option<i64>> {
        let (rowid, _) = self.answer.front().ok_or_else(|| error("no current row"))?;
        Ok(IOResult::Done(Some(*rowid)))
    }

    /// One maintenance run: one bounded job, chosen in order of moving superseded entries out,
    /// splitting a partition past the bound, then packing the longest tail. A run with nothing to
    /// do changes nothing.
    fn optimize(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        loop {
            match std::mem::replace(&mut self.upkeep, Upkeep::Init) {
                Upkeep::Init => {
                    self.open_stores(context)?;
                    self.upkeep = Upkeep::Load;
                }
                Upkeep::Load => {
                    if let IOResult::IO(io) = self.load()? {
                        self.upkeep = Upkeep::Load;
                        return Ok(IOResult::IO(io));
                    }
                    self.upkeep = Upkeep::Count { walk: Walk::Seek, tails: HashMap::new() };
                }
                Upkeep::Count { mut walk, mut tails } => match walk_key(&mut self.stores[TAIL], 0, i64::MAX, &mut walk)? {
                    IOResult::IO(io) => {
                        self.upkeep = Upkeep::Count { walk, tails };
                        return Ok(IOResult::IO(io));
                    }
                    IOResult::Done(Some(key)) => {
                        *tails.entry(key >> ROWID_BITS).or_default() += 1;
                        self.upkeep = Upkeep::Count { walk, tails };
                    }
                    IOResult::Done(None) => self.upkeep = Upkeep::Parts { walk: Walk::Seek, tails, parts: HashMap::new() },
                },
                Upkeep::Parts { mut walk, tails, mut parts } => match walk_row(&mut self.stores[PARTS], 0, i64::MAX, &mut walk)? {
                    IOResult::IO(io) => {
                        self.upkeep = Upkeep::Parts { walk, tails, parts };
                        return Ok(IOResult::IO(io));
                    }
                    IOResult::Done(Some((partition, found))) => {
                        let field = |at| integer(&found, at);
                        let part = Part { next: field(0)?, size: field(1)?, open: field(2)?, move_next: field(3)?, move_end: field(4)? };
                        parts.insert(partition, part);
                        self.upkeep = Upkeep::Parts { walk, tails, parts };
                    }
                    IOResult::Done(None) => self.upkeep = Upkeep::Dead { walk: Walk::Seek, tails, parts, stale: HashMap::new() },
                },
                Upkeep::Dead { mut walk, tails, parts, mut stale } => match walk_row(&mut self.stores[DEAD], 0, i64::MAX, &mut walk)? {
                    IOResult::IO(io) => {
                        self.upkeep = Upkeep::Dead { walk, tails, parts, stale };
                        return Ok(IOResult::IO(io));
                    }
                    IOResult::Done(Some((_, found))) => {
                        for block in dead_blocks(bytes_at(&found, 0)?)? {
                            *stale.entry(block).or_default() += 1;
                        }
                        self.upkeep = Upkeep::Dead { walk, tails, parts, stale };
                    }
                    IOResult::Done(None) => {
                        let Some(job) = self.choose(&tails, &parts, &stale) else {
                            return Ok(IOResult::Done(()));
                        };
                        JOBS.fetch_add(1, Ordering::Relaxed);
                        if matches!(job, Job::Move(_)) {
                            MOVES.fetch_add(1, Ordering::Relaxed);
                        }
                        let part = parts.get(&job_partition(job)).copied().unwrap_or_default();
                        if std::env::var_os("IVF_RQ_TRACE").is_some() {
                            eprintln!("upkeep: {} partitions, tail rows {}, job {job:?}", self.model.centroids.len(), tails.values().sum::<usize>());
                        }
                        self.upkeep = Upkeep::Read { job, part, stage: Stage::Blocks, walk: Walk::Seek, get: Get::Seek, rows: Vec::new() };
                    }
                },
                Upkeep::Read { job, part, stage, mut walk, mut get, mut rows } => {
                    let partition = job_partition(job);
                    let current = self.model.version(partition);
                    match (job, stage) {
                        // A pack starts from the open block, when it holds entries of the current
                        // centroid.
                        (Job::Pack(_), Stage::Blocks) => {
                            if part.open > 0 {
                                let key = block_key(partition, part.next - 1)?;
                                let found = match get_row(&mut self.stores[BLOCKS], key, &mut get)? {
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
                        // A move checks the next block of the split partition that predates the split.
                        (Job::Move(_), Stage::Blocks) => {
                            let key = block_key(partition, part.move_next)?;
                            let found = match get_row(&mut self.stores[BLOCKS], key, &mut get)? {
                                IOResult::Done(found) => found,
                                IOResult::IO(io) => {
                                    self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                    return Ok(IOResult::IO(io));
                                }
                            };
                            let mut sorted = Sorted::default();
                            if let Some(found) = found {
                                let version = integer(&found, 0)?;
                                if version != current {
                                    sorted.version = version;
                                    rows.extend(unpack(bytes_at(&found, 1)?)?.into_iter().map(|entry| Tailed { location: Location::Block(key), version, entry }));
                                }
                            }
                            self.upkeep = Upkeep::Check { job, part, rows, at: 0, get: Get::Seek, sorted };
                        }
                        (Job::Compact(key), Stage::Blocks) => {
                            let found = match get_row(&mut self.stores[BLOCKS], key, &mut get)? {
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
                        (Job::Split(_), Stage::Blocks) => {
                            let start = block_key(partition, 0)?;
                            match walk_row(&mut self.stores[BLOCKS], start, start + (1 << 32), &mut walk)? {
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
                        (_, Stage::Tail) => {
                            let start = tail_key(partition, 0)?;
                            let full = !matches!(job, Job::Split(_)) && rows.iter().filter(|r| matches!(r.location, Location::Tail(_))).count() >= MOVE_BATCH;
                            let row = if full {
                                IOResult::Done(None)
                            } else {
                                walk_row(&mut self.stores[TAIL], start, start + (1 << ROWID_BITS), &mut walk)?
                            };
                            match row {
                                IOResult::IO(io) => {
                                    self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                    return Ok(IOResult::IO(io));
                                }
                                IOResult::Done(Some((key, found))) => {
                                    let version = integer(&found, 0)?;
                                    if !matches!(job, Job::Move(_)) || version != current {
                                        let entry = unpack(bytes_at(&found, 1)?)?.pop().ok_or_else(|| error("empty tail entry"))?;
                                        rows.push(Tailed { location: Location::Tail(key), version, entry });
                                    }
                                    self.upkeep = Upkeep::Read { job, part, stage, walk, get, rows };
                                }
                                IOResult::Done(None) => {
                                    if let Job::Split(_) = job {
                                        // The split's centroids come from a spread sample of its members.
                                        let step = (rows.len() / SPLIT_SAMPLE).max(1);
                                        rows = rows.into_iter().step_by(step).collect();
                                    }
                                    self.upkeep = Upkeep::Check { job, part, rows, at: 0, get: Get::Seek, sorted: Sorted::default() };
                                }
                            }
                        }
                    }
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
                                    sorted.live.push((row.entry.clone(), bytes_at(&r, 1)?.to_vec()));
                                }
                                // A move needs each entry, to keep it where it is when it stays, and its vector,
                                // to see where it belongs.
                                Job::Move(_) => {
                                    sorted.live.push((row.entry.clone(), Vec::new()));
                                    sorted.recode.push(row.entry.rowid);
                                }
                                // A compaction keeps each live entry as it is.
                                Job::Compact(_) => sorted.live.push((row.entry.clone(), Vec::new())),
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
                    let found = match get_row(&mut self.stores[DEAD], key, &mut get)? {
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
                        true => remove(DEAD, key),
                        false => put(DEAD, key, dead_record(&blocks)?),
                    });
                    self.upkeep = Upkeep::Unbury { job, part, sorted, at: at + 1, get: Get::Seek };
                }
                Upkeep::Fetch { job, part, mut sorted, mut vectors } => {
                    let Some(&rowid) = sorted.recode.get(vectors.len()) else {
                        let unbury = std::mem::take(&mut sorted.unbury);
                        let mut steps = match job {
                            Job::Pack(p) => self.plan_pack(p, part, sorted, vectors)?,
                            Job::Split(p) => self.plan_split(p, part, &vectors)?,
                            Job::Move(p) => self.plan_move(p, part, sorted, vectors)?,
                            Job::Compact(block) => self.plan_compact(block, part, sorted)?,
                        };
                        steps.extend(unbury);
                        self.upkeep = Upkeep::Apply { steps };
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
                Upkeep::Apply { mut steps } => {
                    let Some(step) = steps.front_mut() else {
                        return Ok(IOResult::Done(()));
                    };
                    let applied = apply_step(&mut self.stores, step);
                    if let (Err(e), true) = (&applied, std::env::var_os("IVF_RQ_TRACE").is_some()) {
                        let what = match step {
                            Step::Put(store, rowid, _, _) => format!("put store {store} rowid {rowid}"),
                            Step::Remove { store, rowid, .. } => format!("remove store {store} rowid {rowid}"),
                        };
                        let earlier: Vec<String> = Vec::new();
                        eprintln!("upkeep step failed: {what}: {e}{}", earlier.join(""));
                    }
                    match applied? {
                        IOResult::Done(()) => {
                            steps.pop_front();
                            self.upkeep = Upkeep::Apply { steps };
                        }
                        IOResult::IO(io) => {
                            self.upkeep = Upkeep::Apply { steps };
                            return Ok(IOResult::IO(io));
                        }
                    }
                }
            }
        }
    }
}

impl IvfRqCursor {
    fn next_probe(&mut self, mut probes: VecDeque<i64>) -> Result<Search> {
        if let Some(partition) = probes.pop_front() {
            return Ok(Search::Dead { probes, partition, walk: Walk::Seek });
        }
        let query = self.query.as_mut().ok_or_else(|| error("query state absent"))?;
        let bar = query.bar();
        let mut order: Vec<Candidate> = std::mem::take(&mut query.candidates).into_iter().filter(|c| c.upper >= bar).collect();
        order.sort_unstable_by(|a, b| b.upper.total_cmp(&a.upper));
        Ok(Search::Refine { order: order.into(), get: Get::Seek })
    }

    /// The run's job: check a block of a partition being split, else pack the longest tail of at
    /// least [`PACK_AT`] (which also recodes tail entries coded against a superseded centroid), else
    /// compact the block holding the most stale copies, at least [`COMPACT_AT`], else split the
    /// largest partition past the bound.
    fn choose(&self, tails: &HashMap<i64, usize>, parts: &Parts, stale: &HashMap<i64, usize>) -> Option<Job> {
        if let Some(p) = parts.iter().filter(|(_, part)| part.moving()).map(|(p, _)| *p).min() {
            return Some(Job::Move(p));
        }
        if let Some((p, _)) = tails.iter().filter(|(_, count)| **count >= PACK_AT).max_by_key(|(p, count)| (**count, -**p)) {
            return Some(Job::Pack(*p));
        }
        if let Some((block, _)) = stale.iter().filter(|(_, count)| **count >= COMPACT_AT).max_by_key(|(block, count)| (**count, -**block)) {
            return Some(Job::Compact(*block));
        }
        let bound = self.split_bound();
        let size = |p: i64| parts.get(&p).map_or(0, |part| part.size) + *tails.get(&p).unwrap_or(&0) as i64;
        let (total, p) = (0..self.model.centroids.len().max(1) as i64).map(|p| (size(p), p)).max()?;
        (total >= bound).then_some(Job::Split(p))
    }

    /// Merge a partition's current tail entries into its open block and write them as blocks from
    /// there on, give the entries coded against a superseded centroid fresh tail entries from their
    /// vectors, and remove every tail entry read.
    fn plan_pack(&mut self, partition: i64, mut part: Part, sorted: Sorted, vectors: Vec<Vec<f32>>) -> Result<VecDeque<Step>> {
        let mut steps = removals(&sorted.removed);
        let mut live = sorted.live;
        live.sort_unstable_by_key(|(e, _)| e.rowid);
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
            let entries: Vec<Entry> = chunk.iter().map(|(e, _)| e.clone()).collect();
            steps.push_back(put(BLOCKS, key, record(&[Value::from_i64(version), blob(&pack(&entries))?])?));
            for (e, codes) in chunk {
                if !opened_here.contains(&(e.rowid, key)) {
                    steps.push_back(put(VECTORS, e.rowid, record(&[Value::from_i64(Location::Block(key).encode()), blob(codes)?])?));
                }
            }
            part.next += 1;
            part.open = if chunk.len() < BLOCK { chunk.len() as i64 } else { 0 };
        }
        part.size += live.len() as i64;
        for (rowid, vector) in sorted.recode.iter().zip(&vectors) {
            steps.extend(self.tail_steps(*rowid, vector)?);
        }
        RECODED.fetch_add(sorted.recode.len(), Ordering::Relaxed);
        PACKS.fetch_add(1, Ordering::Relaxed);
        steps.push_back(put(PARTS, partition, part.record()?));
        Ok(steps)
    }

    /// The first half of a split: 2-means over a sample of the partition's members gives it a new
    /// centroid version and opens the next partition. Only the centroids and the bookkeeping are
    /// written; the partition is marked moving, and moves recode its entries batch by batch.
    fn plan_split(&mut self, partition: i64, mut part: Part, vectors: &[Vec<f32>]) -> Result<VecDeque<Step>> {
        let started = std::time::Instant::now();
        if vectors.len() < 2 {
            return Ok(VecDeque::new());
        }
        let eta = self.eta(vectors[0].len());
        let halves = Model::new(kmeans(vectors, 2, eta), eta);
        if halves.centroids.len() < 2 || vectors.iter().map(|v| halves.nearest(v)).all(|side| side == halves.nearest(&vectors[0])) {
            return Ok(VecDeque::new());
        }
        // The untrained origin is partition 0's version 0, so its first centroid is version 1.
        let version = self.model.version(partition) + 1;
        let opened = self.model.centroids.len().max(1) as i64;
        let [kept, split] = <[Vec<f32>; 2]>::try_from(halves.centroids).map_err(|_| error("2-means returned other than two"))?;
        let next = generation();
        part.move_next = 0;
        part.move_end = part.next;
        part.open = 0;
        let steps = VecDeque::from([
            put(CENTROIDS, centroid_key(partition, version)?, centroid_record(&kept)?),
            put(CENTROIDS, centroid_key(opened, 0)?, centroid_record(&split)?),
            put(CENTROIDS, GENERATION, record(&[Value::from_i64(next)])?),
            put(PARTS, partition, part.record()?),
            put(PARTS, opened, Part::default().record()?),
        ]);
        let model = Arc::make_mut(&mut self.model);
        model.eta = eta;
        model.place(partition as usize, version, kept);
        model.place(opened as usize, 0, split);
        self.generation = Some(next);
        self.publish();
        let micros = started.elapsed().as_micros() as usize;
        SPLITS.fetch_add(1, Ordering::Relaxed);
        SPLIT_MICROS.fetch_add(micros, Ordering::Relaxed);
        SPLIT_MICROS_MAX.fetch_max(micros, Ordering::Relaxed);
        Ok(steps)
    }

    /// Check one block that predates the partition's split: an entry now nearest another
    /// partition is recoded into that partition's tail, and the block keeps the rest as they are,
    /// still scored against the centroid version they were coded under. A block nothing leaves is
    /// not rewritten.
    fn plan_move(&mut self, partition: i64, mut part: Part, sorted: Sorted, vectors: Vec<Vec<f32>>) -> Result<VecDeque<Step>> {
        let key = block_key(partition, part.move_next)?;
        part.move_next += 1;
        let mut steps = VecDeque::new();
        let mut staying = Vec::new();
        let mut moved = 0;
        for ((entry, _), vector) in sorted.live.into_iter().zip(&vectors) {
            if self.model.nearest(vector) as i64 == partition {
                staying.push(entry);
            } else {
                steps.extend(self.tail_steps(entry.rowid, vector)?);
                moved += 1;
            }
        }
        let dropped = sorted.from_blocks - staying.len() as i64 - moved;
        if moved + dropped > 0 {
            steps.push_front(match staying.is_empty() {
                true => remove(BLOCKS, key),
                false => put(BLOCKS, key, record(&[Value::from_i64(sorted.version), blob(&pack(&staying))?])?),
            });
            part.size = (part.size - moved - dropped).max(0);
        }
        MOVES.fetch_add(1, Ordering::Relaxed);
        RECODED.fetch_add(moved as usize, Ordering::Relaxed);
        steps.push_back(put(PARTS, partition, part.record()?));
        Ok(steps)
    }

    /// Rewrite a block without the stale copies it holds, or remove it when none of its entries is
    /// live. The live entries stay as they are, under the centroid version they were coded against,
    /// so their vectors' rows still name the block.
    fn plan_compact(&mut self, block: i64, mut part: Part, sorted: Sorted) -> Result<VecDeque<Step>> {
        if sorted.buried.is_empty() {
            return Err(*error("a dead row names a block that holds no stale copy"));
        }
        let partition = block >> 32;
        let staying: Vec<Entry> = sorted.live.into_iter().map(|(entry, _)| entry).collect();
        let mut steps = VecDeque::from([match staying.is_empty() {
            true => remove(BLOCKS, block),
            false => put(BLOCKS, block, record(&[Value::from_i64(sorted.version), blob(&pack(&staying))?])?),
        }]);
        part.size = (part.size - sorted.buried.len() as i64).max(0);
        // The open block is the partition's last; it stays open with the entries it keeps.
        if part.open > 0 && block == block_key(partition, part.next - 1)? {
            part.open = staying.len() as i64;
        }
        COMPACTS.fetch_add(1, Ordering::Relaxed);
        steps.push_back(put(PARTS, partition, part.record()?));
        Ok(steps)
    }
}

/// The steps that remove the blocks and tail entries a job read.
fn removals(removed: &[Location]) -> VecDeque<Step> {
    removed
        .iter()
        .map(|l| match l {
            Location::Block(key) => remove(BLOCKS, *key),
            Location::Tail(key) => remove(TAIL, *key),
        })
        .collect()
}

fn job_partition(job: Job) -> i64 {
    match job {
        Job::Pack(p) | Job::Split(p) | Job::Move(p) => p,
        Job::Compact(block) => block >> 32,
    }
}

/// Estimate a block's entries from their 1-bit codes, each group in one fast-scan pass (a sparse
/// group, a tail entry above all, summed exactly), and keep as candidates those whose estimate,
/// raised by its error bound, reaches the bar of the `limit` best lowered ones so far. A stale
/// copy the partition's dead rows name is skipped before it can raise the bar.
fn score_block(query: &mut Query, payload: &[u8], centre: f64, source: Location) -> Result<()> {
    let dims = query.rotated.len();
    let count = block_count(payload)?;
    let layout = Layout::new(count, dims / 8);
    if payload.len() != layout.len() {
        return Err(*error("block length differs from its count"));
    }
    let scale = 1.0 / (dims as f64).sqrt();
    let spread = EPSILON * query.length / ((dims - 1) as f64).sqrt();
    let screened = match source {
        Location::Block(key) if !query.dead.is_empty() => Some(key),
        _ => None,
    };
    let mut sums = [0u32; GROUP];
    for group in 0..layout.groups() {
        let codes = &payload[layout.scan() + group * layout.group_bytes()..][..layout.group_bytes()];
        let members = GROUP.min(count - group * GROUP);
        SCANNED.fetch_add(members, Ordering::Relaxed);
        let scanned = members >= SCAN_MEMBERS;
        if scanned {
            scan_group(codes, &query.scan.bytes, &mut sums);
        }
        let exact = |slot: usize| {
            let (lane, shift) = (slot % 16, if slot < 16 { 0 } else { 4 });
            (0..layout.positions).map(|j| query.scan.exact[j * 16 + ((codes[j * 16 + lane] >> shift) & 0xf) as usize] as f64).sum::<f64>()
        };
        for slot in 0..members {
            let n = group * GROUP + slot;
            let norm = f32_at(payload, layout.norms() + 4 * n) as f64;
            let factor = f32_at(payload, layout.factors() + 4 * n) as f64;
            let (chosen, quantized) = if scanned { (query.scan.step * sums[slot] as f64 + query.scan.base, query.scan.error) } else { (exact(slot), 0.0) };
            let estimate = centre + norm * (2.0 * chosen - query.sum) * scale / factor;
            let bound = norm * spread * ((1.0 - factor * factor).max(0.0) / (factor * factor)).sqrt();
            let slack = norm * 2.0 * quantized * scale / factor;
            if estimate + bound + slack < query.bar() {
                continue;
            }
            let rowid = i64_at(payload, layout.rowids() + 8 * n);
            if screened.is_some_and(|key| query.dead.contains(&(rowid, key))) {
                continue;
            }
            let signed = if scanned { exact(slot) } else { chosen };
            let estimate = centre + norm * (2.0 * signed - query.sum) * scale / factor;
            query.floors.push(Ordered(-(estimate - bound), rowid));
            if query.floors.len() > query.limit {
                query.floors.pop();
            }
            if estimate + bound >= query.bar() {
                query.candidates.push(Candidate { upper: estimate + bound, rowid, centre, norm, signed, source });
            }
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
        Query {
            vector: q.to_vec(),
            asked: Vector::from_f32(q.to_vec()),
            sum: rotated.iter().map(|x| *x as f64).sum(),
            length: rotated.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt(),
            scan: ScanTable::new(&rotated),
            rotated,
            limit,
            keep,
            centres: HashMap::new(),
            dead: HashSet::new(),
            floors: BinaryHeap::new(),
            candidates: Vec::new(),
            estimates: BinaryHeap::new(),
            results: BinaryHeap::new(),
        }
    }

    #[test]
    fn a_block_unpacks_to_the_codes_it_packed() {
        let (_, _, coded) = entries(45, 768);
        let back = unpack(&pack(&coded)).unwrap();
        assert_eq!(back.len(), 45);
        for (a, b) in coded.iter().zip(&back) {
            assert_eq!((a.rowid, a.norm, a.factor, &a.bits), (b.rowid, b.norm, b.factor, &b.bits));
        }
    }

    /// At 3072 dimensions the scan widens its 16-bit sums several times within one group.
    #[test]
    fn the_simd_scan_sums_as_the_scalar_one_does() {
        for dims in [768, 3072] {
            let (rotation, _, coded) = entries(32, dims);
            let block = pack(&coded);
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

    /// The block scan keeps every entry whose true product is among the best, and the 4-bit
    /// estimate built from a kept candidate and its extra row is far closer to the truth than the
    /// 1-bit one.
    #[test]
    fn the_scan_keeps_the_true_best_and_the_four_bit_estimate_beats_the_one_bit_one() {
        let (rotation, vectors, coded) = entries(64, 768);
        let mut state = 13;
        let q = random(&mut state, 768);
        let mut query = query(&q, &rotation, 10, 40);
        score_block(&mut query, &pack(&coded), 0.0, Location::Block(0)).unwrap();
        let mut truth: Vec<(f64, i64)> = vectors.iter().enumerate().map(|(n, v)| (dot(&q, v), n as i64)).collect();
        truth.sort_by(|a, b| b.0.total_cmp(&a.0));
        let bar = query.bar();
        let kept: Vec<i64> = query.candidates.iter().filter(|c| c.upper >= bar).map(|c| c.rowid).collect();
        for (_, rowid) in &truth[..10] {
            assert!(kept.contains(rowid), "true top-10 row {rowid} was dropped by the scan");
        }
        let padded = query.rotated.len();
        let (mut one, mut four) = (0.0f64, 0.0f64);
        for c in &query.candidates {
            let e = &coded[c.rowid as usize];
            let true_product = dot(&q, &vectors[c.rowid as usize]);
            let estimate_one = c.norm * (2.0 * c.signed - query.sum) / (padded as f64).sqrt() / e.factor as f64;
            let codes = extra(e);
            let lower = lower_product(&query.rotated, &codes[4..]);
            let estimate_four = c.norm * refined_product(c.signed, lower, query.sum, f32_at(&codes, 0) as f64);
            one += (estimate_one - true_product).abs();
            four += (estimate_four - true_product).abs();
        }
        assert!(!query.candidates.is_empty());
        assert!(four < one / 4.0, "4-bit error {four} against 1-bit {one}");
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
            let block = pack(&coded);
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
            for row in rows(&conn, "SELECT rowid, blocks FROM __turso_internal_v_ivf_dead") {
                let rowid = row[0].as_int().unwrap() & ((1 << ROWID_BITS) - 1);
                let blocks = dead_blocks(row[1].to_blob().unwrap()).unwrap();
                assert!(!blocks.is_empty(), "mvcc={mvcc}: the dead row of {rowid} names no block");
                let location = rows(&conn, &format!("SELECT location FROM __turso_internal_v_ivf_vectors WHERE rowid = {rowid}"));
                for block in blocks {
                    assert!(
                        location.iter().all(|r| r[0].as_int() != Some(Location::Block(block).encode())),
                        "mvcc={mvcc}: the dead row of {rowid} names block {block}, where its vector lives"
                    );
                    let payload = rows(&conn, &format!("SELECT codes FROM __turso_internal_v_ivf_blocks WHERE rowid = {block}"));
                    let payload = payload.first().unwrap_or_else(|| panic!("mvcc={mvcc}: the dead row of {rowid} names missing block {block}"));
                    let entries = unpack(payload[0].to_blob().unwrap()).unwrap();
                    assert!(entries.iter().any(|e| e.rowid == rowid), "mvcc={mvcc}: block {block} holds no copy of {rowid}");
                    *stale.entry(block).or_default() += 1;
                }
            }
            assert!(stale.values().all(|count| *count < COMPACT_AT), "mvcc={mvcc}: blocks left with stale copies {stale:?}");
        }
    }
}
