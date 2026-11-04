//! B+-tree nodes (DESIGN.md §13; LeanStore, BTW 2023 §4): slotted pages with fence keys, the
//! fences' common prefix stripped from every key, a 4-byte head of each key in its slot, and a
//! hint array over the heads.
//!
//! ```text
//! page header (24) | root: u64 | next: u64 | next_hint: u32 | count: u16 | level: u8 | fences: u8
//!   | data_start: u32 | space: u32 | prefix: u16 | lower: (off u16, len u16) | upper: (off u16, len u16)
//!   | 2 | hints: [u32; 16] | slots: [slot; count] | free | data
//! slot: off: u16 | key_len: u16 | value_len: u16 | head: u32
//! ```
//!
//! - `root` names the tree a node belongs to: its root page, which never moves.
//! - `next` is a leaf's right neighbour, a hint checked before use, or an inner node's rightmost
//!   child. `next_hint` is the frame hint beside it.
//! - The lower fence is exclusive and the upper inclusive; a missing fence is infinite. A node
//!   holds exactly the keys between its fences.
//! - An inner node's entry `i` routes keys up to its key, and the rightmost child takes the rest.
//!   Its value is the child's page id and frame hint.
//! - A leaf's value is inline, or a reference to blob pages (`value_len`'s top bit,
//!   [`super::value`]).
//! - Entries hold the key's suffix past the prefix, then the value; data grows down from the end
//!   of the page. Removing an entry leaves a hole that compaction reclaims.
//!
//! Slots and lengths are 16 bits, so the largest node is 64 KiB. Every entry, slot included, takes
//! at most a quarter of the space past the slots' start ([`Limits`]), so any two entries fit in a
//! node beside its fences, and a split always makes room.
//!
//! [`View`] reads a node that may change underneath it (an optimistic fix): every offset is
//! checked before use and nothing panics on garbage; what it read counts only once the fix
//! validates. The functions taking [`BytesMut`] change a node held exclusively, in a frame
//! through its writer, or in bytes of their own.

use super::super::page::{self, PageId, PageKind, SizeClass, HEADER_SIZE};
use super::super::shared::{Bytes, BytesMut};
use std::cmp::Ordering;

pub const LEAF: PageKind = PageKind(2);
pub const INNER: PageKind = PageKind(3);

const ROOT: usize = HEADER_SIZE;
const NEXT: usize = 32;
const NEXT_HINT: usize = 40;
const COUNT: usize = 44;
const LEVEL: usize = 46;
const FENCES: usize = 47;
const DATA_START: usize = 48;
const SPACE: usize = 52;
const PREFIX: usize = 56;
const LOWER: usize = 58;
const UPPER: usize = 62;
const HINTS: usize = 68;
/// Heads sampled at even distances over the slots, narrowing a binary search before it starts:
/// sixteen heads fill one 64-byte cache line (LeanStore's choice).
pub const HINT_COUNT: usize = 16;
/// Where the slots start: the end of the node's header.
pub const SLOTS: usize = HINTS + 4 * HINT_COUNT;
pub const SLOT: usize = 10;

const LOWER_INFINITE: u8 = 1;
const UPPER_INFINITE: u8 = 2;
/// `value_len`'s top bit: the value is a reference to blob pages.
const OUT_OF_LINE: u16 = 1 << 15;
/// An inner node's value: the child's page id and frame hint.
pub const CHILD: usize = 12;

/// The largest node class: slots hold 16-bit offsets.
pub const MAX_CLASS: usize = 4;

/// What fits in a node of one size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Bytes past the node's header: slots, data and fences.
    pub capacity: usize,
    /// The most an entry takes, its slot included: a quarter of the capacity, so that a node
    /// holding two entries and two fences of the longest keys is at most full.
    pub max_entry: usize,
    /// The longest key: an entry with it and an out-of-line value takes `max_entry`.
    pub max_key: usize,
}

impl Limits {
    pub fn of(class: SizeClass) -> Self {
        assert!(class.index() <= MAX_CLASS, "a node of {class:?}");
        let capacity = class.size() - SLOTS;
        let max_entry = capacity / 4;
        Self {
            capacity,
            max_entry,
            max_key: max_entry - SLOT - super::value::REF,
        }
    }

    /// The limits of a tree whose values keep `head` bytes in the leaf even out of line.
    pub fn with_head(mut self, head: usize) -> Self {
        self.max_key -= head;
        self
    }

    /// Whether a value goes inline beside `key_len` bytes of key: the entry fits `max_entry` with
    /// the whole key, so the choice does not depend on the node's prefix.
    pub fn inline(&self, key_len: usize, value_len: usize) -> bool {
        SLOT + key_len + value_len <= self.max_entry
    }
}

fn u16_at<P: Bytes + ?Sized>(p: &P, at: usize) -> usize {
    p.u16_at(at) as usize
}

fn put16<P: BytesMut + ?Sized>(p: &mut P, at: usize, v: usize) {
    debug_assert!(v <= u16::MAX as usize);
    p.put_u16(at, v as u16);
}

/// The first four bytes of a key as a big-endian integer, zero-padded: heads order like the keys,
/// and keys with equal heads are compared whole.
pub fn head(key: &[u8]) -> u32 {
    let mut b = [0u8; 4];
    let n = key.len().min(4);
    b[..n].copy_from_slice(&key[..n]);
    u32::from_be_bytes(b)
}

pub fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// The shortest key `s` with `left <= s < right`, for `left < right`: what separates two leaves
/// after a split. Shorter separators make inner nodes hold more.
pub fn separator(left: &[u8], right: &[u8]) -> Vec<u8> {
    debug_assert!(left < right);
    let common = common_prefix(left, right);
    if right.len() > common + 1 {
        right[..common + 1].to_vec()
    } else {
        left.to_vec()
    }
}

/// A slot read back.
#[derive(Debug, Clone, Copy)]
struct Slot {
    off: usize,
    key_len: usize,
    value_len: usize,
    out_of_line: bool,
    head: u32,
}

/// A run of a node's bytes: a fence, or an entry's key or value.
#[derive(Debug, Clone, Copy)]
struct Run {
    off: usize,
    len: usize,
}

/// A node read in place: in a frame, held optimistically or latched, or in bytes of its own. Every
/// method returns `None` where what it reads does not hold together, which only a concurrent
/// change (or corruption) can cause; what it returns is copied out.
pub struct View<'a, P: ?Sized>(&'a P);

impl<P: ?Sized> Clone for View<'_, P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<P: ?Sized> Copy for View<'_, P> {}

/// A fence read out of a node: a key, or infinite.
pub type Fence = Option<Vec<u8>>;

impl<'a, P: Bytes + ?Sized> View<'a, P> {
    pub fn new(page: &'a P) -> Self {
        Self(page)
    }

    pub fn page(&self) -> &'a P {
        self.0
    }

    pub fn is_node(&self) -> bool {
        matches!(page::kind(self.0), LEAF | INNER)
    }

    pub fn root(&self) -> PageId {
        PageId::from_raw(self.0.u64_at(ROOT))
    }

    pub fn level(&self) -> u8 {
        self.0.u8_at(LEVEL)
    }

    pub fn is_leaf(&self) -> bool {
        self.level() == 0
    }

    /// The right neighbour (a leaf's hint) or the rightmost child (an inner node's), and its
    /// frame hint.
    pub fn next(&self) -> (PageId, u32) {
        (
            PageId::from_raw(self.0.u64_at(NEXT)),
            self.0.u32_at(NEXT_HINT),
        )
    }

    fn data_start(&self) -> Option<usize> {
        let d = self.0.u32_at(DATA_START) as usize;
        (d <= self.0.len()).then_some(d)
    }

    pub fn count(&self) -> Option<usize> {
        let count = u16_at(self.0, COUNT);
        (SLOTS + count * SLOT <= self.data_start()?).then_some(count)
    }

    /// Bytes of data in use: entries and fences.
    pub fn space(&self) -> usize {
        self.0.u32_at(SPACE) as usize
    }

    fn slot(&self, i: usize) -> Option<Slot> {
        if i >= self.count()? {
            return None;
        }
        let at = SLOTS + i * SLOT;
        let b: [u8; SLOT] = self.0.bytes(at);
        let value = u16::from_le_bytes([b[4], b[5]]);
        let s = Slot {
            off: u16::from_le_bytes([b[0], b[1]]) as usize,
            key_len: u16::from_le_bytes([b[2], b[3]]) as usize,
            value_len: (value & !OUT_OF_LINE) as usize,
            out_of_line: value & OUT_OF_LINE != 0,
            head: u32::from_le_bytes(b[6..10].try_into().unwrap()),
        };
        (s.off + s.key_len + s.value_len <= self.0.len()).then_some(s)
    }

    /// A run, if it lies in the node.
    fn run(&self, off: usize, len: usize) -> Option<Run> {
        (off + len <= self.0.len()).then_some(Run { off, len })
    }

    fn copy(&self, run: Run) -> Vec<u8> {
        self.0.copied(run.off..run.off + run.len)
    }

    /// How the bytes of `run` order against `key`.
    fn order(&self, run: Run, key: &[u8]) -> Ordering {
        let n = run.len.min(key.len());
        self.0
            .compare(run.off, &key[..n])
            .then(run.len.cmp(&key.len()))
    }

    fn fence_run(&self, at: usize, infinite: u8) -> Option<Option<Run>> {
        if self.0.u8_at(FENCES) & infinite != 0 {
            return Some(None);
        }
        Some(Some(self.run(u16_at(self.0, at), u16_at(self.0, at + 2))?))
    }

    /// The exclusive lower fence.
    pub fn lower(&self) -> Option<Fence> {
        Some(self.fence_run(LOWER, LOWER_INFINITE)?.map(|r| self.copy(r)))
    }

    /// The inclusive upper fence.
    pub fn upper(&self) -> Option<Fence> {
        Some(self.fence_run(UPPER, UPPER_INFINITE)?.map(|r| self.copy(r)))
    }

    /// Where the bytes every key in the node starts with lie: the start of the lower fence.
    fn prefix_run(&self) -> Option<Run> {
        let len = u16_at(self.0, PREFIX);
        match self.fence_run(LOWER, LOWER_INFINITE)? {
            Some(lower) => (len <= lower.len).then_some(Run {
                off: lower.off,
                len,
            }),
            None => (len == 0).then_some(Run { off: 0, len: 0 }),
        }
    }

    /// Whether the lower fence is `key`, compared in place.
    pub fn lower_is(&self, key: &[u8]) -> Option<bool> {
        Some(
            self.fence_run(LOWER, LOWER_INFINITE)?
                .is_some_and(|l| self.order(l, key) == Ordering::Equal),
        )
    }

    /// Whether the upper fence is `key`, compared in place.
    pub fn upper_is(&self, key: &[u8]) -> Option<bool> {
        Some(
            self.fence_run(UPPER, UPPER_INFINITE)?
                .is_some_and(|u| self.order(u, key) == Ordering::Equal),
        )
    }

    /// The bytes every key in the node starts with, stripped from each.
    pub fn prefix(&self) -> Option<Vec<u8>> {
        Some(self.copy(self.prefix_run()?))
    }

    /// Entry `i`'s key past the prefix.
    pub fn suffix(&self, i: usize) -> Option<Vec<u8>> {
        let s = self.slot(i)?;
        Some(self.copy(Run {
            off: s.off,
            len: s.key_len,
        }))
    }

    /// Entry `i`'s whole key.
    pub fn key(&self, i: usize) -> Option<Vec<u8>> {
        let mut key = self.prefix()?;
        key.extend_from_slice(&self.suffix(i)?);
        Some(key)
    }

    /// Entry `i`'s value, and whether it is a reference to blob pages.
    pub fn value(&self, i: usize) -> Option<(Vec<u8>, bool)> {
        let s = self.slot(i)?;
        let value = self.copy(Run {
            off: s.off + s.key_len,
            len: s.value_len,
        });
        Some((value, s.out_of_line))
    }

    /// An inner node's child `i`, the rightmost at `i == count`, and its frame hint.
    pub fn child(&self, i: usize) -> Option<(PageId, u32)> {
        if i == self.count()? {
            return Some(self.next());
        }
        let s = self.slot(i)?;
        if s.value_len < CHILD {
            return None;
        }
        let at = s.off + s.key_len;
        Some((PageId::from_raw(self.0.u64_at(at)), self.0.u32_at(at + 8)))
    }

    /// Whether `key` lies between the fences.
    pub fn covers(&self, key: &[u8]) -> Option<bool> {
        let above = self
            .fence_run(LOWER, LOWER_INFINITE)?
            .is_none_or(|l| self.order(l, key) == Ordering::Less);
        let below = self
            .fence_run(UPPER, UPPER_INFINITE)?
            .is_none_or(|u| self.order(u, key) != Ordering::Less);
        Some(above && below)
    }

    fn hint(&self, i: usize) -> u32 {
        self.0.u32_at(HINTS + 4 * i)
    }

    /// The first position whose key is at least `key`, and whether it is equal: where `key` is,
    /// or would go.
    pub fn search(&self, key: &[u8]) -> Option<(usize, bool)> {
        let count = self.count()?;
        let prefix = self.prefix_run()?;
        let n = prefix.len.min(key.len());
        match self.0.compare(prefix.off, &key[..n]).reverse() {
            Ordering::Less => return Some((0, false)),
            Ordering::Greater => return Some((count, false)),
            Ordering::Equal if key.len() < prefix.len => return Some((0, false)),
            Ordering::Equal => {}
        }
        let suffix = &key[prefix.len..];
        let key_head = head(suffix);
        let (mut lo, mut hi) = (0, count);
        if count > HINT_COUNT * 2 {
            let dist = count / (HINT_COUNT + 1);
            let mut pos = 0;
            while pos < HINT_COUNT && self.hint(pos) < key_head {
                pos += 1;
            }
            let mut pos2 = pos;
            while pos2 < HINT_COUNT && self.hint(pos2) == key_head {
                pos2 += 1;
            }
            lo = pos * dist;
            if pos2 < HINT_COUNT {
                hi = ((pos2 + 1) * dist).min(count);
            }
        }
        while lo < hi {
            let mid = (lo + hi) / 2;
            let s = self.slot(mid)?;
            let ord = match s.head.cmp(&key_head) {
                Ordering::Equal => self.order(
                    Run {
                        off: s.off,
                        len: s.key_len,
                    },
                    suffix,
                ),
                o => o,
            };
            match ord {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Some((mid, true)),
            }
        }
        Some((lo, false))
    }

    /// The first position whose key is above `key`.
    pub fn search_above(&self, key: &[u8]) -> Option<usize> {
        let (pos, exact) = self.search(key)?;
        Some(pos + exact as usize)
    }

    /// Which child of an inner node routes `key`.
    pub fn route(&self, key: &[u8]) -> Option<usize> {
        Some(self.search(key)?.0)
    }

    /// Bytes free once compacted.
    pub fn free(&self) -> Option<usize> {
        let used = SLOTS + self.count()? * SLOT + self.space();
        self.0.len().checked_sub(used)
    }

    /// Bytes `key` with a value of `value_len` takes here, its slot included.
    pub fn entry_size(&self, key: &[u8], value_len: usize) -> Option<usize> {
        Some(SLOT + key.len().checked_sub(self.prefix_run()?.len)? + value_len)
    }

    /// Entry `i`'s whole key against `key`, compared in place.
    pub fn compare_key(&self, i: usize, key: &[u8]) -> Option<Ordering> {
        let prefix = self.prefix_run()?;
        let s = self.slot(i)?;
        let n = prefix.len.min(key.len());
        let order = self.0.compare(prefix.off, &key[..n]);
        if order != Ordering::Equal {
            return Some(order);
        }
        if key.len() < prefix.len {
            return Some(Ordering::Greater);
        }
        let suffix = Run {
            off: s.off,
            len: s.key_len,
        };
        Some(self.order(suffix, &key[prefix.len..]))
    }

    /// Entry `i`'s whole key and value: their length together.
    pub fn entry_len(&self, i: usize) -> Option<usize> {
        let s = self.slot(i)?;
        Some(self.prefix_run()?.len + s.key_len + s.value_len)
    }

    /// How many bytes, from the start, the lower fence shares with `other`.
    fn lower_shares(&self, other: &[u8]) -> Option<usize> {
        let Some(lower) = self.fence_run(LOWER, LOWER_INFINITE)? else {
            return Some(0);
        };
        let mut shared = 0;
        let mut differs = false;
        self.0
            .pieces(lower.off..lower.off + lower.len.min(other.len()), |b| {
                if differs {
                    return;
                }
                let same = common_prefix(b, &other[shared..]);
                shared += same;
                differs = same < b.len();
            });
        Some(shared)
    }

    /// Bytes the entries in `range` take, slots included, in a node whose keys share a prefix
    /// of `prefix` bytes.
    pub fn entries_size(&self, range: std::ops::Range<usize>, prefix: usize) -> Option<usize> {
        let mut size = 0;
        for i in range {
            size += (SLOT + self.entry_len(i)?).checked_sub(prefix)?;
        }
        Some(size)
    }

    /// Every entry: whole key, value, and whether the value is out of line.
    pub fn entries(&self) -> Option<Vec<Entry>> {
        (0..self.count()?)
            .map(|i| {
                let (value, out_of_line) = self.value(i)?;
                Some(Entry {
                    key: self.key(i)?,
                    value,
                    out_of_line,
                })
            })
            .collect()
    }
}

/// An entry taken out of a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub out_of_line: bool,
}

impl Entry {
    pub fn child(key: Vec<u8>, child: PageId, hint: u32) -> Self {
        Self {
            key,
            value: child_value(child, hint).to_vec(),
            out_of_line: false,
        }
    }
}

pub fn child_value(child: PageId, hint: u32) -> [u8; CHILD] {
    let mut v = [0u8; CHILD];
    v[..8].copy_from_slice(&child.raw().to_le_bytes());
    v[8..].copy_from_slice(&hint.to_le_bytes());
    v
}

/// What a node is made of, to build it whole.
pub struct Layout<'k> {
    pub root: PageId,
    pub level: u8,
    pub lower: Option<&'k [u8]>,
    pub upper: Option<&'k [u8]>,
    pub next: (PageId, u32),
}

impl Layout<'_> {
    /// How many bytes every key between the fences starts with.
    pub fn prefix_len(&self) -> usize {
        match (self.lower, self.upper) {
            (Some(l), Some(u)) => common_prefix(l, u),
            _ => 0,
        }
    }

    /// Bytes the fences take.
    pub fn fences_len(&self) -> usize {
        self.lower.map_or(0, <[u8]>::len) + self.upper.map_or(0, <[u8]>::len)
    }

    /// Bytes these entries take in a node with this layout, fences included.
    pub fn size<'e>(&self, entries: impl IntoIterator<Item = &'e Entry>) -> usize {
        let prefix = self.prefix_len();
        let fences = self.lower.map_or(0, <[u8]>::len) + self.upper.map_or(0, <[u8]>::len);
        fences
            + entries
                .into_iter()
                .map(|e| SLOT + e.key.len() - prefix + e.value.len())
                .sum::<usize>()
    }
}

/// Builds a node whole in `page`: its layout, then `entries`, which are in key order and fit.
pub fn build<'e, P: BytesMut + ?Sized>(
    page: &mut P,
    layout: &Layout<'_>,
    entries: impl IntoIterator<Item = &'e Entry>,
) {
    let size = page.len();
    page.fill(HEADER_SIZE..size, 0);
    page::set_kind(page, if layout.level == 0 { LEAF } else { INNER });
    page.put_u64(ROOT, layout.root.raw());
    page.put_u8(LEVEL, layout.level);
    let mut data = size;
    let mut fences = 0;
    for (at, fence, infinite) in [
        (UPPER, layout.upper, UPPER_INFINITE),
        (LOWER, layout.lower, LOWER_INFINITE),
    ] {
        match fence {
            Some(f) => {
                data -= f.len();
                page.write(data, f);
                put16(page, at, data);
                put16(page, at + 2, f.len());
            }
            None => fences |= infinite,
        }
    }
    page.put_u8(FENCES, fences);
    put16(page, PREFIX, layout.prefix_len());
    page.put_u32(DATA_START, data as u32);
    page.put_u32(SPACE, (size - data) as u32);
    set_next(page, layout.next.0, layout.next.1);
    for e in entries {
        let count = View::new(&*page).count().unwrap();
        assert!(
            insert(page, count, &e.key, &e.value, e.out_of_line),
            "building a node that does not fit"
        );
    }
}

/// A node's layout with its fences copied out, to rebuild it or a node beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape {
    pub root: PageId,
    pub level: u8,
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
    pub next: (PageId, u32),
}

/// The shape of the node in `page`.
pub fn shape<P: Bytes + ?Sized>(page: &P) -> Shape {
    let v = View::new(page);
    Shape {
        root: v.root(),
        level: v.level(),
        lower: v.lower().unwrap(),
        upper: v.upper().unwrap(),
        next: v.next(),
    }
}

/// The child an inner node is left with once a drain has removed its last ([`super::Drain`]). Its
/// class byte is no size class's, so it names no page.
pub const NO_CHILD: PageId = PageId::from_raw(u64::MAX);

pub fn set_next<P: BytesMut + ?Sized>(page: &mut P, pid: PageId, hint: u32) {
    page.put_u64(NEXT, pid.raw());
    page.put_u32(NEXT_HINT, hint);
}

pub fn set_next_hint<P: BytesMut + ?Sized>(page: &mut P, hint: u32) {
    page.put_u32(NEXT_HINT, hint);
}

/// Points an inner node's child `i` (the rightmost at `count`) at `child`.
pub fn set_child<P: BytesMut + ?Sized>(page: &mut P, i: usize, child: PageId, hint: u32) {
    let v = View::new(&*page);
    if i == v.count().unwrap() {
        return set_next(page, child, hint);
    }
    let s = v.slot(i).unwrap();
    page.write(s.off + s.key_len, &child_value(child, hint));
}

/// Stores the frame hint of an inner node's child `i`.
pub fn set_child_hint<P: BytesMut + ?Sized>(page: &mut P, i: usize, hint: u32) {
    let v = View::new(&*page);
    if i == v.count().unwrap() {
        return set_next_hint(page, hint);
    }
    let s = v.slot(i).unwrap();
    page.put_u32(s.off + s.key_len + 8, hint);
}

/// Inserts an entry at `pos`, compacting first if the free bytes are scattered; false if it does
/// not fit even then. `key` is whole and between the fences.
pub fn insert<P: BytesMut + ?Sized>(
    page: &mut P,
    pos: usize,
    key: &[u8],
    value: &[u8],
    out_of_line: bool,
) -> bool {
    let v = View::new(&*page);
    let count = v.count().unwrap();
    let prefix = v.prefix_run().unwrap().len;
    debug_assert!(pos <= count && key.len() >= prefix);
    let suffix = &key[prefix..];
    let need = suffix.len() + value.len();
    if v.free().unwrap() < SLOT + need {
        return false;
    }
    let slots_end = SLOTS + (count + 1) * SLOT;
    if (v.data_start().unwrap()) < slots_end + need {
        compact(page);
    }
    let data = page.u32_at(DATA_START) as usize - need;
    page.write(data, suffix);
    page.write(data + suffix.len(), value);
    page.put_u32(DATA_START, data as u32);
    page.put_u32(SPACE, (page.u32_at(SPACE) as usize + need) as u32);
    let at = SLOTS + pos * SLOT;
    page.copy_within(at..SLOTS + count * SLOT, at + SLOT);
    put16(page, at, data);
    put16(page, at + 2, suffix.len());
    put16(
        page,
        at + 4,
        value.len() | if out_of_line { OUT_OF_LINE as usize } else { 0 },
    );
    page.put_u32(at + 6, head(suffix));
    put16(page, COUNT, count + 1);
    update_hints(page);
    true
}

/// Appends the entries of `src` in `range` to the node in `dst`, after its own, which they lie
/// above, between its fences. Each is copied straight from `src`, its key cut again to `dst`'s
/// prefix: none is assembled on the way. False if one does not fit; those before it stay.
pub fn append_from<S: Bytes + ?Sized, D: BytesMut + ?Sized>(
    dst: &mut D,
    src: &View<'_, S>,
    range: std::ops::Range<usize>,
) -> bool {
    let (Some(from), Some(to)) = (src.prefix_run(), View::new(&*dst).prefix_run()) else {
        return false;
    };
    for i in range {
        let Some(s) = src.slot(i) else {
            return false;
        };
        let Some(suffix) = (from.len + s.key_len).checked_sub(to.len) else {
            return false;
        };
        let need = suffix + s.value_len;
        let v = View::new(&*dst);
        let count = v.count().unwrap();
        if v.free().unwrap() < SLOT + need {
            return false;
        }
        if v.data_start().unwrap() < SLOTS + (count + 1) * SLOT + need {
            compact(dst);
        }
        let data = dst.u32_at(DATA_START) as usize - need;
        let mut at = data;
        let mut copy = |range: std::ops::Range<usize>| {
            src.page().pieces(range, |b| {
                dst.write(at, b);
                at += b.len();
            })
        };
        if to.len >= from.len {
            // The key loses the bytes of `dst`'s prefix past `src`'s.
            copy(s.off + to.len - from.len..s.off + s.key_len + s.value_len);
        } else {
            // The key gains the bytes of `src`'s prefix past `dst`'s.
            copy(from.off + to.len..from.off + from.len);
            copy(s.off..s.off + s.key_len + s.value_len);
        }
        let mut first = [0u8; 4];
        let n = suffix.min(4);
        dst.copy_to(data, &mut first[..n]);
        let at = SLOTS + count * SLOT;
        put16(dst, at, data);
        put16(dst, at + 2, suffix);
        put16(
            dst,
            at + 4,
            s.value_len
                | if s.out_of_line {
                    OUT_OF_LINE as usize
                } else {
                    0
                },
        );
        dst.put_u32(at + 6, head(&first[..n]));
        put16(dst, COUNT, count + 1);
        dst.put_u32(DATA_START, data as u32);
        dst.put_u32(SPACE, (dst.u32_at(SPACE) as usize + need) as u32);
    }
    update_hints(dst);
    true
}

/// Keeps the first `keep` entries of the node in `page`, under a new upper fence `upper`, and
/// points it on to `next`: the left half of a split, made in place. The fences then share a
/// prefix at least as long, so each kept key is cut again where it lies. `None`, the node as it
/// was, if the node or `upper` do not hold for it.
pub fn split_off<P: BytesMut + ?Sized>(
    page: &mut P,
    keep: usize,
    upper: &[u8],
    next: (PageId, u32),
) -> Option<()> {
    let v = View::new(&*page);
    let count = v.count()?;
    let old = v.prefix_run()?.len;
    let new = match v.fence_run(LOWER, LOWER_INFINITE)? {
        Some(_) => v.lower_shares(upper)?,
        None => 0,
    };
    let cut = new.checked_sub(old)?;
    if keep > count || (0..keep).any(|i| v.slot(i).is_none_or(|s| s.key_len < cut)) {
        return None;
    }
    let kept: usize = (0..keep)
        .map(|i| v.slot(i).map_or(0, |s| s.key_len + s.value_len))
        .sum();
    let lower_len = v.fence_run(LOWER, LOWER_INFINITE)?.map_or(0, |r| r.len);
    for i in 0..keep {
        let s = View::new(&*page).slot(i)?;
        let at = SLOTS + i * SLOT;
        let mut first = [0u8; 4];
        let n = (s.key_len - cut).min(4);
        page.copy_to(s.off + cut, &mut first[..n]);
        put16(page, at, s.off + cut);
        put16(page, at + 2, s.key_len - cut);
        page.put_u32(at + 6, head(&first[..n]));
    }
    put16(page, COUNT, keep);
    put16(page, PREFIX, new);
    // The old upper fence is no longer one: compaction leaves it behind.
    let fences = page.u8_at(FENCES);
    page.put_u8(FENCES, fences | UPPER_INFINITE);
    page.put_u32(SPACE, (lower_len + kept - keep * cut) as u32);
    compact(page);
    let data = page.u32_at(DATA_START) as usize - upper.len();
    page.write(data, upper);
    put16(page, UPPER, data);
    put16(page, UPPER + 2, upper.len());
    page.put_u8(FENCES, fences & !UPPER_INFINITE);
    page.put_u32(DATA_START, data as u32);
    page.put_u32(SPACE, (page.u32_at(SPACE) as usize + upper.len()) as u32);
    set_next(page, next.0, next.1);
    update_hints(page);
    Some(())
}

/// Removes the entry at `pos`.
pub fn remove<P: BytesMut + ?Sized>(page: &mut P, pos: usize) {
    let v = View::new(&*page);
    let count = v.count().unwrap();
    let s = v.slot(pos).unwrap();
    page.put_u32(
        SPACE,
        (page.u32_at(SPACE) as usize - s.key_len - s.value_len) as u32,
    );
    let at = SLOTS + pos * SLOT;
    page.copy_within(at + SLOT..SLOTS + count * SLOT, at);
    put16(page, COUNT, count - 1);
    update_hints(page);
}

/// Replaces the value at `pos`; false, leaving the node as it was, if the new one does not fit.
/// The entry is rewritten in place: a value of another length moves the entry to the start of the
/// data, its old bytes left a hole.
pub fn set_value<P: BytesMut + ?Sized>(
    page: &mut P,
    pos: usize,
    value: &[u8],
    out_of_line: bool,
) -> bool {
    let v = View::new(&*page);
    let s = v.slot(pos).unwrap();
    let at = SLOTS + pos * SLOT;
    let lengths = value.len() | if out_of_line { OUT_OF_LINE as usize } else { 0 };
    if s.value_len == value.len() {
        page.write(s.off + s.key_len, value);
        put16(page, at + 4, lengths);
        return true;
    }
    if v.free().unwrap() + s.value_len < value.len() {
        return false;
    }
    let need = s.key_len + value.len();
    let slots_end = SLOTS + v.count().unwrap() * SLOT;
    let space = v.space();
    let data = if v.data_start().unwrap() >= slots_end + need {
        // Room before the data: the key moves there, the value after it.
        let data = v.data_start().unwrap() - need;
        page.copy_within(s.off..s.off + s.key_len, data);
        data
    } else {
        // The room is in holes: the key held aside, the old entry compacted away.
        KEY.with(|key| {
            let mut key = key.borrow_mut();
            key.resize(s.key_len, 0);
            page.copy_to(s.off, &mut key);
            put16(page, at + 2, 0);
            put16(page, at + 4, 0);
            page.put_u32(SPACE, (space - s.key_len - s.value_len) as u32);
            compact(page);
            let data = page.u32_at(DATA_START) as usize - need;
            page.write(data, &key);
            put16(page, at + 2, s.key_len);
            data
        })
    };
    page.write(data + s.key_len, value);
    put16(page, at, data);
    put16(page, at + 4, lengths);
    page.put_u32(DATA_START, data as u32);
    page.put_u32(SPACE, (space - s.value_len + value.len()) as u32);
    true
}

/// A run of data in a node, named for its compaction: a fence, or entry `i`'s key and value.
const UPPER_RUN: u16 = u16::MAX;
const LOWER_RUN: u16 = u16::MAX - 1;

crate::thread::thread_local! {
    /// Scratch for [`gather_runs`], kept between calls so a compaction allocates nothing.
    static RUNS: std::cell::RefCell<Vec<(u16, u16, u16)>> = const {
        std::cell::RefCell::new(Vec::new())
    };
    /// Scratch for [`set_value`]: a key held aside while its node compacts.
    static KEY: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Puts the node's runs of data (offset, length, which) in `runs`, in the order they pack toward
/// its end without holes: highest offset first. Packed so, each run only moves up, onto bytes
/// already moved or unused.
fn gather_runs<P: Bytes + ?Sized>(page: &P, runs: &mut Vec<(u16, u16, u16)>) {
    let v = View::new(page);
    let count = v.count().unwrap();
    runs.clear();
    for (at, infinite, which) in [
        (UPPER, UPPER_INFINITE, UPPER_RUN),
        (LOWER, LOWER_INFINITE, LOWER_RUN),
    ] {
        if page.u8_at(FENCES) & infinite == 0 {
            runs.push((page.u16_at(at), page.u16_at(at + 2), which));
        }
    }
    for i in 0..count {
        let s = v.slot(i).unwrap();
        runs.push((s.off as u16, (s.key_len + s.value_len) as u16, i as u16));
    }
    runs.sort_unstable_by(|a, b| b.0.cmp(&a.0));
}

/// Where the run `which` starts.
fn run_at(which: u16) -> usize {
    match which {
        UPPER_RUN => UPPER,
        LOWER_RUN => LOWER,
        i => SLOTS + i as usize * SLOT,
    }
}

/// Rewrites the data area without holes, in place.
pub fn compact<P: BytesMut + ?Sized>(page: &mut P) {
    let size = page.len();
    let data = RUNS.with(|runs| {
        let mut runs = runs.borrow_mut();
        gather_runs(&*page, &mut runs);
        let mut data = size;
        for &(off, len, which) in runs.iter() {
            data -= len as usize;
            if off as usize != data {
                page.copy_within(off as usize..off as usize + len as usize, data);
                put16(page, run_at(which), data);
            }
        }
        data
    });
    page.put_u32(DATA_START, data as u32);
    debug_assert_eq!(size - data, View::new(&*page).space());
}

fn update_hints<P: BytesMut + ?Sized>(page: &mut P) {
    let v = View::new(&*page);
    let count = v.count().unwrap();
    let dist = count / (HINT_COUNT + 1);
    let heads: [u32; HINT_COUNT] = std::array::from_fn(|i| {
        if count > HINT_COUNT * 2 {
            v.slot(dist * (i + 1)).unwrap().head
        } else {
            0
        }
    });
    for (i, h) in heads.into_iter().enumerate() {
        page.put_u32(HINTS + 4 * i, h);
    }
}

/// The node in `page` as a log record's payload: its header and slots, then its data, with no
/// holes, so that [`apply_image`] rebuilds it exactly. Read straight from the page, compacted as
/// it is copied out.
pub fn image<P: Bytes + ?Sized>(page: &P) -> Vec<u8> {
    let count = View::new(page).count().unwrap();
    let size = page.len();
    let head_end = SLOTS + count * SLOT;
    let space = View::new(page).space();
    let mut out = vec![0u8; 4 + head_end - HEADER_SIZE + space];
    out[..4].copy_from_slice(&((head_end - HEADER_SIZE) as u32).to_le_bytes());
    let (head, data) = out[4..].split_at_mut(head_end - HEADER_SIZE);
    page.copy_to(HEADER_SIZE, head);
    // The head as it stands at `at` in the page.
    let head_at = |at: usize| at - HEADER_SIZE;
    let left = RUNS.with(|runs| {
        let mut runs = runs.borrow_mut();
        gather_runs(page, &mut runs);
        let mut left = size;
        for &(off, len, which) in runs.iter() {
            let len = len as usize;
            left -= len;
            page.copy_to(off as usize, &mut data[left - (size - space)..][..len]);
            let at = head_at(run_at(which));
            head[at..at + 2].copy_from_slice(&(left as u16).to_le_bytes());
        }
        left
    });
    debug_assert_eq!(left, size - space);
    let at = head_at(DATA_START);
    head[at..at + 4].copy_from_slice(&(left as u32).to_le_bytes());
    out
}

/// Rebuilds a node from its [`image`]; `None` if the image does not describe a node of this
/// page's size.
pub fn apply_image<P: BytesMut + ?Sized>(page: &mut P, image: &[u8]) -> Option<()> {
    let head_len = u32::from_le_bytes(image.get(..4)?.try_into().ok()?) as usize;
    let head = image.get(4..4 + head_len)?;
    let data = &image[4 + head_len..];
    let size = page.len();
    if data.len() > size
        || HEADER_SIZE + head_len > size - data.len()
        || head_len < SLOTS - HEADER_SIZE
    {
        return None;
    }
    page.fill(HEADER_SIZE..size, 0);
    page.write(HEADER_SIZE, head);
    page.write(size - data.len(), data);
    let v = View::new(&*page);
    let consistent = v.data_start() == Some(size - data.len())
        && v.space() == data.len()
        && v.count() == Some((HEADER_SIZE + head_len - SLOTS) / SLOT)
        && (HEADER_SIZE + head_len - SLOTS).is_multiple_of(SLOT);
    if !consistent {
        return None;
    }
    let level = v.level();
    page::set_kind(page, if level == 0 { LEAF } else { INNER });
    check(&*page).ok()
}

/// Checks a node's structure: fences, prefix, slots in bounds and in key order between the
/// fences, heads and hints right, space accounted.
pub fn check<P: Bytes + ?Sized>(page: &P) -> Result<(), String> {
    let v = View::new(page);
    if !v.is_node() {
        return Err(format!("kind {:?} is not a node", page::kind(page)));
    }
    let count = v.count().ok_or("slots overlap the data")?;
    let (lower, upper) = (
        v.lower().ok_or("lower fence out of bounds")?,
        v.upper().ok_or("upper fence out of bounds")?,
    );
    if let (Some(l), Some(u)) = (&lower, &upper) {
        if l >= u {
            return Err("fences out of order".into());
        }
    }
    let prefix = v.prefix().ok_or("prefix out of bounds")?;
    let expected = match (&lower, &upper) {
        (Some(l), Some(u)) => common_prefix(l, u),
        _ => 0,
    };
    if prefix.len() != expected {
        return Err(format!(
            "prefix {} for fences sharing {expected}",
            prefix.len()
        ));
    }
    let mut space = lower.as_ref().map_or(0, Vec::len) + upper.as_ref().map_or(0, Vec::len);
    let mut last: Option<Vec<u8>> = None;
    for i in 0..count {
        let s = v.slot(i).ok_or_else(|| format!("slot {i} out of bounds"))?;
        if s.off < SLOTS + count * SLOT {
            return Err(format!("slot {i} points into the slots"));
        }
        let key = v.key(i).ok_or_else(|| format!("key {i} out of bounds"))?;
        if s.head != head(&key[prefix.len()..]) {
            return Err(format!("slot {i} has a stale head"));
        }
        if !v.covers(&key).unwrap() {
            return Err(format!("key {i} outside the fences"));
        }
        if last.as_ref().is_some_and(|l| *l >= key) {
            return Err(format!("key {i} out of order"));
        }
        if !v.is_leaf() && (s.value_len != CHILD || s.out_of_line) {
            return Err(format!("inner entry {i} holds no child"));
        }
        space += s.key_len + s.value_len;
        last = Some(key);
    }
    if space != v.space() {
        return Err(format!("space {} where entries take {space}", v.space()));
    }
    let dist = count / (HINT_COUNT + 1);
    for i in 0..HINT_COUNT {
        let expected = if count > HINT_COUNT * 2 {
            v.slot(dist * (i + 1)).unwrap().head
        } else {
            0
        };
        if v.hint(i) != expected {
            return Err(format!("hint {i} stale"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::collections::BTreeMap;

    fn class(i: usize) -> SizeClass {
        SizeClass::new(i).unwrap()
    }

    fn random_key(rng: &mut ChaCha8Rng, max: usize) -> Vec<u8> {
        // Few distinct bytes and shared starts, so heads tie often and prefixes form.
        let len = rng.random_range(0..max);
        (0..len)
            .map(|_| b"aab\0z"[rng.random_range(0..5)])
            .collect()
    }

    fn leaf<'k>(lower: Option<&'k [u8]>, upper: Option<&'k [u8]>) -> Layout<'k> {
        Layout {
            root: PageId::new(class(0), 1),
            level: 0,
            lower,
            upper,
            next: (PageId::from_raw(0), 0),
        }
    }

    #[test]
    fn a_node_keeps_entries_like_a_sorted_map_through_inserts_removes_updates_and_compaction() {
        for (seed, (lower, upper)) in [
            (None, None),
            (Some(&b"aa"[..]), Some(&b"aaz"[..])),
            (Some(&b"aab"[..]), Some(&b"aab\0z"[..])),
        ]
        .into_iter()
        .enumerate()
        {
            let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
            let mut page = vec![0u8; class(0).size()];
            build(&mut page, &leaf(lower, upper), []);
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            for op in 0..20_000 {
                let mut key = lower.map_or(Vec::new(), <[u8]>::to_vec);
                key.extend(random_key(&mut rng, 7));
                let v = View::new(&page);
                if !v.covers(&key).unwrap() {
                    continue;
                }
                let (pos, exact) = v.search(&key).unwrap();
                assert_eq!(exact, model.contains_key(&key), "op {op}");
                assert_eq!(pos, model.range(..key.clone()).count(), "op {op}");
                let value = vec![op as u8; rng.random_range(0..40)];
                match (exact, rng.random_range(0..3)) {
                    (false, _) => {
                        if insert(&mut page, pos, &key, &value, false) {
                            model.insert(key, value);
                        } else {
                            assert!(
                                View::new(&page).free().unwrap() < SLOT + key.len() + value.len()
                            );
                        }
                    }
                    (true, 0) => {
                        remove(&mut page, pos);
                        model.remove(&key);
                    }
                    (true, _) => {
                        if set_value(&mut page, pos, &value, false) {
                            model.insert(key, value);
                        }
                    }
                }
                check(&page).unwrap_or_else(|e| panic!("seed {seed} op {op}: {e}"));
            }
            let entries = View::new(&page).entries().unwrap();
            let expected: Vec<Entry> = model
                .into_iter()
                .map(|(key, value)| Entry {
                    key,
                    value,
                    out_of_line: false,
                })
                .collect();
            assert_eq!(entries, expected);
            // An image rebuilds the same node.
            let mut again = vec![0xffu8; page.len()];
            apply_image(&mut again, &image(&page)).unwrap();
            assert_eq!(View::new(&again).entries().unwrap(), entries);
            assert_eq!(shape(&again), shape(&page));
        }
    }

    #[test]
    fn searches_with_hints_match_a_scan() {
        let mut page = vec![0u8; class(2).size()];
        build(&mut page, &leaf(None, None), []);
        let mut keys: Vec<Vec<u8>> = (0..600u32)
            .map(|i| format!("key{:05}", i * 7).into_bytes())
            .collect();
        keys.sort();
        for k in &keys {
            let count = View::new(&page).count().unwrap();
            assert!(insert(&mut page, count, k, b"v", false));
        }
        check(&page).unwrap();
        let v = View::new(&page);
        for probe in 0..4300u32 {
            let k = format!("key{probe:05}").into_bytes();
            let expected = keys.partition_point(|x| *x < k);
            assert_eq!(
                v.search(&k),
                Some((expected, keys.get(expected) == Some(&k))),
                "{probe}"
            );
        }
    }

    #[test]
    fn separators_lie_between_the_keys_they_separate_and_are_short() {
        let mut rng = ChaCha8Rng::seed_from_u64(5);
        for _ in 0..100_000 {
            let a = random_key(&mut rng, 6);
            let b = random_key(&mut rng, 6);
            if a >= b {
                continue;
            }
            let s = separator(&a, &b);
            assert!(a <= s && s < b, "{a:?} {s:?} {b:?}");
            assert!(s.len() <= a.len().max(common_prefix(&a, &b) + 1));
        }
    }

    #[test]
    fn reading_any_bytes_as_a_node_never_panics() {
        let mut rng = ChaCha8Rng::seed_from_u64(9);
        let mut page = vec![0u8; class(0).size()];
        build(&mut page, &leaf(Some(b"a"), Some(b"q")), []);
        for i in 0..40u8 {
            let count = View::new(&page).count().unwrap();
            assert!(insert(&mut page, count, &[b'b', i], &[i; 30], false));
        }
        for _ in 0..20_000 {
            let mut torn = page.clone();
            for _ in 0..rng.random_range(1..12) {
                let at = rng.random_range(HEADER_SIZE..torn.len());
                torn[at] = rng.random();
            }
            if rng.random_range(0..4) == 0 {
                rng.fill(&mut torn[HEADER_SIZE..SLOTS + 40 * SLOT]);
            }
            let v = View::new(&torn);
            let key = random_key(&mut rng, 6);
            let _ = (
                v.search(&key),
                v.covers(&key),
                v.entries(),
                v.free(),
                v.prefix(),
            );
            if let Some(count) = v.count() {
                for i in 0..=count {
                    let _ = (v.key(i), v.value(i), v.child(i));
                }
            }
            let _ = check(&torn);
        }
    }
}
