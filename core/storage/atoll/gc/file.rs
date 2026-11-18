//! The retired file, `{db}.retired` (DESIGN.md §6.5, as the auditor accepted it in #6195): what a
//! checkpoint keeps of the retired lists so that a crash loses no retired value's pages.
//!
//! The file is two header places of [`BLOCK`] bytes, then blocks of [`BLOCK`] bytes, numbered from
//! 0 after the header places:
//!
//! ```text
//! header place: crc32c: u32 | magic: u32 | checkpoint: u64 | next list: u64 | first block: u64
//!               | lists: u64 | entries' crc32c: u32 | zeros
//! list block:   crc32c: u32 | magic: u32 | list: u64 | count: u64 | before: u64 | items: u32 | 0: u32
//!               | (start: u64 | position: u64 | reference: 24) * items | zeros
//! record block: crc32c: u32 | magic: u32 | checkpoint: u64 | next block: u64 | 0: u64 | 0: u64
//!               | (list: u64 | head at: u64, its bit 63 the head's form | last count: u64
//!                  | head block: u48 | last block: u48 | head freed: u32) * entries | zeros
//! ```
//!
//! - **A list block** holds items of one list, which list it is of, its count in that list (0 for
//!   the list's first block), and `before`, the number of the block before it in that list plus
//!   one, 0 for none. Every block's checksum covers its own number too, and a header place's its
//!   index, so a block read at another place fails it.
//! - **A record** holds each list's head and last block, in the order the open reads them: the owed
//!   list's lists first, then each slot's. A head on disk is its block, a place in that block, and
//!   the pages freed of the head's item. For a slot's list the place is the head's position (form
//!   1): the list's blocks hold only the items the image keeps, so a block's positions are not
//!   consecutive and no index can be reckoned from one; the open takes the block's items at or
//!   past it, and gives the freed pages to the first only if its position is the head's. For an
//!   owed list's part the place is the item's index in the block (form 0), since the owed list
//!   holds every item of the part's blocks from its head on. A position is written only against
//!   the items of its own block, written in the same process's numbering, and compared with
//!   nothing else: never with a list in memory, whose numbering starts again at the open. A
//!   block's number is below 2^48, and a head's place below 2^63. It lies in blocks of
//!   [`ENTRIES`] entries chained forward, each naming its checkpoint and the block after it (plus
//!   one, 0 for none), so it has no bound but the file's. A header place, written in turn, names
//!   its first block, its length in lists and the checksum of its entries in order (the auditor's
//!   #6229). The open checks each block of the chain to be of the header's checkpoint, and the
//!   entries against the checksum, which fails a chain followed in another order.
//! - **The walk back** from a list's last block stops at the head's block by its number, and checks
//!   every block it reads: its checksum, its list, and its count, one less than the block after
//!   it. A block reused for another list passes its own checksum and would otherwise be read as
//!   this list's. Any check that fails, or a block missing, fails the open as corrupt.
//! - **A block is free** once the newest durable record does not hold it: neither as one of its own
//!   blocks nor as a block of one of its lists between its head and its last ([`Blocks`]). So a
//!   crash before a checkpoint's superblock leaves the record before in charge and whole, whichever
//!   of the checkpoint's writes reached the disk.
//! - **The open** reads a record's lists, in its order, into the owed list ([`OwedRead`]), and
//!   marks each owed page retired as it reads; it frees nothing.

use super::super::btree::value::{self, ValueRef, REF};
use super::super::buffer::BufferPool;
use super::super::crc;
use super::super::page::PageId;
use super::super::store::PageStore;
use super::list::Lists;
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};
use std::collections::{BTreeMap, BTreeSet};

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// Bytes of a block.
pub const BLOCK: usize = 4096;
/// Bytes of an item in a block: as in a list's page.
pub const ITEM: usize = 16 + REF;
const BLOCK_HEAD: usize = 40;
/// Items a block holds.
pub const PER_BLOCK: usize = (BLOCK - BLOCK_HEAD) / ITEM;
const ENTRY: usize = 40;
/// A record's entries a block holds.
pub const ENTRIES: usize = (BLOCK - BLOCK_HEAD) / ENTRY;
const BLOCK_MAGIC: u32 = u32::from_le_bytes(*b"ATRB");
const RECORD_MAGIC: u32 = u32::from_le_bytes(*b"ATRR");
const HEADER_MAGIC: u32 = u32::from_le_bytes(*b"ATRH");

/// An item as the file holds it: the writer's start, its position in its list when written, and
/// the retired value's reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Item {
    pub start: u64,
    pub position: u64,
    pub reference: ValueRef,
}

/// A block, read back whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub list: u64,
    pub count: u64,
    /// The block before it in its list.
    pub before: Option<u64>,
    pub items: Vec<Item>,
}

/// A head on disk: its block, its place in that block, and the pages freed of the head's item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    pub block: u64,
    pub at: At,
    pub freed: u32,
}

/// A head's place in its block: the item's index (an owed list's part), or the head's position
/// in its slot's numbering (a slot's list), whose item is the block's first at or past it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum At {
    Index(u64),
    Position(u64),
}

impl Head {
    /// The index in `block`, the head's block, of the first item from the head on, and the pages
    /// freed of it: the head's own for an index, and for a position only if that item's position
    /// is the head's, so a head on an item the image left out gives them to none. A position past
    /// every item of the block, or positions that do not ascend, fail as corrupt.
    pub fn place_in(&self, list: u64, block: &Block) -> Result<(usize, u32)> {
        match self.at {
            At::Index(index) => {
                if index > block.items.len() as u64 {
                    return Err(corrupt_list(
                        list,
                        &format!(
                            "its head index {index} is past its block's {} items",
                            block.items.len()
                        ),
                    ));
                }
                Ok((index as usize, self.freed))
            }
            At::Position(position) => {
                if block
                    .items
                    .windows(2)
                    .any(|w| w[0].position >= w[1].position)
                {
                    return Err(corrupt_list(
                        list,
                        &format!("its head block {}'s positions do not ascend", self.block),
                    ));
                }
                let index = block.items.partition_point(|item| item.position < position);
                let Some(item) = block.items.get(index) else {
                    return Err(corrupt_list(
                        list,
                        &format!(
                            "its head's position {position} is past every item of its block {}",
                            self.block
                        ),
                    ));
                };
                let freed = if item.position == position {
                    self.freed
                } else {
                    0
                };
                Ok((index, freed))
            }
        }
    }
}

/// A list in a record: its id, its head, and its last block with that block's count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub list: u64,
    pub head: Head,
    pub last: u64,
    pub last_count: u64,
}

/// A record: its checkpoint, the id the next new list takes, and its lists in the order the open
/// reads them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub checkpoint: u64,
    pub next_list: u64,
    pub lists: Vec<Entry>,
}

/// The bytes of block `number`. Panics on more items than a block holds: the checkpoint fills a
/// block to [`PER_BLOCK`] and takes the next.
pub fn encode_block(number: u64, block: &Block) -> Vec<u8> {
    assert!(
        block.items.len() <= PER_BLOCK,
        "{} items for a block of {PER_BLOCK}",
        block.items.len()
    );
    let mut out = vec![0u8; BLOCK];
    out[4..8].copy_from_slice(&BLOCK_MAGIC.to_le_bytes());
    out[8..16].copy_from_slice(&block.list.to_le_bytes());
    out[16..24].copy_from_slice(&block.count.to_le_bytes());
    out[24..32].copy_from_slice(&block.before.map_or(0, |b| b + 1).to_le_bytes());
    out[32..36].copy_from_slice(&(block.items.len() as u32).to_le_bytes());
    for (i, item) in block.items.iter().enumerate() {
        let at = BLOCK_HEAD + i * ITEM;
        out[at..at + 8].copy_from_slice(&item.start.to_le_bytes());
        out[at + 8..at + 16].copy_from_slice(&item.position.to_le_bytes());
        out[at + 16..at + ITEM].copy_from_slice(&item.reference.encode());
    }
    seal(&mut out, number);
    out
}

/// Block `number` from its bytes, or why it fails as corrupt.
pub fn decode_block(number: u64, bytes: &[u8]) -> Result<Block> {
    let bad = |what: &str| LimboError::Corrupt(format!("retired file block {number}: {what}"));
    assert_whole(bytes, "a block");
    if !sealed(bytes, number) {
        return Err(bad("its checksum fails"));
    }
    if u32_at(bytes, 4) != BLOCK_MAGIC {
        return Err(bad("not a block"));
    }
    // Sealed, so written whole: more items than fit is a writer's fault the checksum covers.
    let items = u32_at(bytes, 32) as usize;
    if items > PER_BLOCK {
        return Err(bad("more items than a block holds"));
    }
    let items = (0..items)
        .map(|i| {
            let at = BLOCK_HEAD + i * ITEM;
            Item {
                start: u64_at(bytes, at),
                position: u64_at(bytes, at + 8),
                reference: ValueRef {
                    len: u64_at(bytes, at + 16),
                    pid: PageId::from_raw(u64_at(bytes, at + 24)),
                    id: u64_at(bytes, at + 32),
                },
            }
        })
        .collect();
    Ok(Block {
        list: u64_at(bytes, 8),
        count: u64_at(bytes, 16),
        before: u64_at(bytes, 24).checked_sub(1),
        items,
    })
}

/// What a header place holds: its checkpoint, the id the next new list takes, and its record's
/// first block, its length in lists, and the checksum of its entries in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub checkpoint: u64,
    pub next_list: u64,
    pub first: Option<u64>,
    pub lists: u64,
    pub sum: u32,
}

/// The header place a checkpoint writes: the one the newest durable superblock does not name.
pub fn header_place(named: u8) -> u8 {
    1 - named
}

/// The bytes of header place `place` holding `header`.
pub fn encode_header(place: u8, header: &Header) -> Vec<u8> {
    let mut out = vec![0u8; BLOCK];
    out[4..8].copy_from_slice(&HEADER_MAGIC.to_le_bytes());
    out[8..16].copy_from_slice(&header.checkpoint.to_le_bytes());
    out[16..24].copy_from_slice(&header.next_list.to_le_bytes());
    out[24..32].copy_from_slice(&header.first.map_or(0, |b| b + 1).to_le_bytes());
    out[32..40].copy_from_slice(&header.lists.to_le_bytes());
    out[40..44].copy_from_slice(&header.sum.to_le_bytes());
    seal(&mut out, place as u64);
    out
}

/// The header in place `place`, or why it fails as corrupt: a place the superblock names must
/// hold a whole header of the checkpoint it names.
pub fn decode_header(place: u8, checkpoint: u64, bytes: &[u8]) -> Result<Header> {
    let bad = |what: &str| LimboError::Corrupt(format!("retired file header {place}: {what}"));
    assert_whole(bytes, "a header place");
    if !sealed(bytes, place as u64) {
        return Err(bad("its checksum fails"));
    }
    if u32_at(bytes, 4) != HEADER_MAGIC {
        return Err(bad("not a header"));
    }
    if u64_at(bytes, 8) != checkpoint {
        return Err(bad(&format!(
            "the header of checkpoint {}, not of {checkpoint}",
            u64_at(bytes, 8)
        )));
    }
    Ok(Header {
        checkpoint,
        next_list: u64_at(bytes, 16),
        first: u64_at(bytes, 24).checked_sub(1),
        lists: u64_at(bytes, 32),
        sum: u32_at(bytes, 40),
    })
}

/// `record` laid out in blocks taken from `blocks`, [`ENTRIES`] entries a block, each block naming
/// its record's checkpoint and the block after it: the header naming them, and each block's
/// number and bytes. A checkpoint writes them all, and the header into [`header_place`], before
/// its one data sync.
pub fn write_record(record: &Record, blocks: &mut Blocks) -> (Header, Vec<(u64, Vec<u8>)>) {
    let chunks: Vec<&[Entry]> = record.lists.chunks(ENTRIES).collect();
    let numbers: Vec<u64> = chunks.iter().map(|_| blocks.take()).collect();
    let written = chunks
        .iter()
        .enumerate()
        .map(|(index, entries)| {
            let mut out = vec![0u8; BLOCK];
            out[4..8].copy_from_slice(&RECORD_MAGIC.to_le_bytes());
            out[8..16].copy_from_slice(&record.checkpoint.to_le_bytes());
            let next = numbers.get(index + 1).map_or(0, |n| n + 1);
            out[16..24].copy_from_slice(&next.to_le_bytes());
            for (i, e) in entries.iter().enumerate() {
                let at = BLOCK_HEAD + i * ENTRY;
                out[at..at + ENTRY].copy_from_slice(&encode_entry(e));
            }
            seal(&mut out, numbers[index]);
            (numbers[index], out)
        })
        .collect();
    let header = Header {
        checkpoint: record.checkpoint,
        next_list: record.next_list,
        first: numbers.first().copied(),
        lists: record.lists.len() as u64,
        sum: entries_sum(&record.lists),
    };
    (header, written)
}

/// The read of a record from its header, forward along its chain, its place kept across its
/// reads: each block checked to be of the header's checkpoint as the chain is followed, every
/// block but the last holding [`ENTRIES`] entries, and the entries read checked against the
/// header's checksum, which fails a chain followed in another order.
pub struct RecordRead {
    header: Header,
    next: Option<u64>,
    lists: Vec<Entry>,
    numbers: Vec<u64>,
}

impl RecordRead {
    pub fn new(header: Header) -> Self {
        Self {
            header,
            next: header.first,
            lists: Vec::new(),
            numbers: Vec::new(),
        }
    }

    /// The record, and the blocks it lies in: no checkpoint takes them while it is the record in
    /// charge. Step it until done.
    pub fn step(&mut self, source: &dyn Source) -> Result<IOResult<(Record, Vec<u64>)>> {
        let header = self.header;
        let bad = |what: String| {
            LimboError::Corrupt(format!(
                "retired file record of checkpoint {}: {what}",
                header.checkpoint
            ))
        };
        while (self.lists.len() as u64) < header.lists {
            let index = self.numbers.len();
            let Some(number) = self.next else {
                return Err(bad(format!(
                    "its chain ends after {index} blocks, short of its {} lists",
                    header.lists
                )));
            };
            let Some(bytes) = done!(source.block(number)) else {
                return Err(bad(format!(
                    "its block {index}, block {number}, is missing"
                )));
            };
            assert_whole(&bytes, "a record block");
            if !sealed(&bytes, number) {
                return Err(bad(format!("block {number}'s checksum fails")));
            }
            if u32_at(&bytes, 4) != RECORD_MAGIC || u64_at(&bytes, 8) != header.checkpoint {
                return Err(bad(format!(
                    "its block {index}, block {number}, is not a block of its record"
                )));
            }
            let entries = (header.lists - self.lists.len() as u64).min(ENTRIES as u64) as usize;
            self.lists.extend((0..entries).map(|i| {
                let at = BLOCK_HEAD + i * ENTRY;
                decode_entry(&bytes[at..at + ENTRY])
            }));
            self.numbers.push(number);
            self.next = u64_at(&bytes, 16).checked_sub(1);
        }
        if entries_sum(&self.lists) != header.sum {
            return Err(bad("its entries' checksum fails".to_string()));
        }
        let record = Record {
            checkpoint: header.checkpoint,
            next_list: header.next_list,
            lists: std::mem::take(&mut self.lists),
        };
        Ok(IOResult::Done((record, std::mem::take(&mut self.numbers))))
    }
}

/// A head's form bit in its place's word: set for a position.
const POSITION: u64 = 1 << 63;
/// Block numbers in an entry: 48 bits, an exbibyte of the file's blocks.
const BLOCK_NUMBERS: u64 = 1 << 48;

fn encode_entry(e: &Entry) -> [u8; ENTRY] {
    let (place, form) = match e.head.at {
        At::Index(index) => (index, 0),
        At::Position(position) => (position, POSITION),
    };
    assert!(
        place < POSITION && e.head.block < BLOCK_NUMBERS && e.last < BLOCK_NUMBERS,
        "list {}: its head's place {place:#x}, head block {} or last block {} past an entry's room",
        e.list,
        e.head.block,
        e.last
    );
    let at = place | form;
    let mut out = [0u8; ENTRY];
    out[..8].copy_from_slice(&e.list.to_le_bytes());
    out[8..16].copy_from_slice(&at.to_le_bytes());
    out[16..24].copy_from_slice(&e.last_count.to_le_bytes());
    out[24..30].copy_from_slice(&e.head.block.to_le_bytes()[..6]);
    out[30..36].copy_from_slice(&e.last.to_le_bytes()[..6]);
    out[36..40].copy_from_slice(&e.head.freed.to_le_bytes());
    out
}

fn decode_entry(bytes: &[u8]) -> Entry {
    let at = u64_at(bytes, 8);
    Entry {
        list: u64_at(bytes, 0),
        head: Head {
            block: u48_at(bytes, 24),
            at: match at & POSITION {
                0 => At::Index(at),
                _ => At::Position(at & !POSITION),
            },
            freed: u32_at(bytes, 36),
        },
        last: u48_at(bytes, 30),
        last_count: u64_at(bytes, 16),
    }
}

/// The checksum of a record's entries, in order.
fn entries_sum(lists: &[Entry]) -> u32 {
    lists
        .iter()
        .fold(0, |sum, e| crc::append(sum, &encode_entry(e)))
}

/// Where the blocks of the file are read from: a block's bytes, `None` past the file's end.
pub trait Source {
    fn block(&self, number: u64) -> Result<IOResult<Option<Vec<u8>>>>;
}

/// The walk back from a list's last block to its head's (DESIGN.md §6.5), its place kept across
/// its reads.
pub struct Walk {
    entry: Entry,
    /// The blocks read, last first, each checked.
    read: Vec<(u64, Block)>,
}

impl Walk {
    pub fn new(entry: Entry) -> Self {
        Self {
            entry,
            read: Vec::new(),
        }
    }

    /// Reads back from the list's last block to its head's, checking each block's list and count,
    /// and stopping at the head's block by its number: the blocks, head first. Step it until done.
    pub fn step(&mut self, source: &dyn Source) -> Result<IOResult<Vec<(u64, Block)>>> {
        let entry = self.entry;
        loop {
            let (number, count) = match self.read.last() {
                None => (entry.last, entry.last_count),
                Some((at, _)) if *at == entry.head.block => {
                    let mut blocks = std::mem::take(&mut self.read);
                    blocks.reverse();
                    return Ok(IOResult::Done(blocks));
                }
                Some((at, block)) => match (block.before, block.count.checked_sub(1)) {
                    (Some(before), Some(count)) => (before, count),
                    _ => {
                        return Err(corrupt_list(
                            entry.list,
                            &format!("block {at} names none before it short of the head's block"),
                        ))
                    }
                },
            };
            let Some(bytes) = done!(source.block(number)) else {
                return Err(corrupt_list(
                    entry.list,
                    &format!("block {number} is missing"),
                ));
            };
            let block = decode_block(number, &bytes)?;
            if block.list != entry.list || block.count != count {
                return Err(corrupt_list(
                    entry.list,
                    &format!(
                        "block {number} is list {}'s count {}, where its count {count} was \
                         looked for",
                        block.list, block.count
                    ),
                ));
            }
            self.read.push((number, block));
        }
    }
}

/// The free blocks of the file, in memory, as runs: a block is free once the newest durable record
/// holds it nowhere, and a block a checkpoint under way took is not free. The file grows at its end
/// when none is free. A take or a free changes the runs beside one block, so no checkpoint's
/// bookkeeping grows with what the record holds; only the open goes through what it holds, once.
#[derive(Debug, Default)]
pub struct Blocks {
    len: u64,
    /// Each run of free blocks, from its first to past its last.
    free: BTreeMap<u64, u64>,
}

impl Blocks {
    /// The blocks of a file `len` blocks long, `held` held by the record in charge: the runs
    /// between them, in one pass over `held`.
    pub fn new(len: u64, held: &BTreeSet<u64>) -> Self {
        let len = held.last().map_or(len, |&last| len.max(last + 1));
        let mut free = BTreeMap::new();
        let mut from = 0;
        for &block in held {
            if block > from {
                free.insert(from, block);
            }
            from = block + 1;
        }
        if from < len {
            free.insert(from, len);
        }
        Self { len, free }
    }

    /// A block to write: the lowest free, or a new one at the file's end.
    pub fn take(&mut self) -> u64 {
        match self.free.pop_first() {
            Some((block, end)) => {
                if block + 1 < end {
                    self.free.insert(block + 1, end);
                }
                block
            }
            None => {
                self.len += 1;
                self.len - 1
            }
        }
    }

    /// `block` is free again: no durable record holds it, and no checkpoint under way took it. A
    /// block freed while free is a fault of the bookkeeping, which would hand it out twice.
    pub fn free(&mut self, block: u64) {
        assert!(
            block < self.len,
            "block {block} freed, past the file's {} blocks",
            self.len
        );
        let (mut from, mut to) = (block, block + 1);
        if let Some((&start, &end)) = self.free.range(..=block).next_back() {
            assert!(
                end <= block,
                "block {block} freed while free, in the run {start}..{end}"
            );
            if end == block {
                self.free.remove(&start);
                from = start;
            }
        }
        if let Some(end) = self.free.remove(&(block + 1)) {
            to = end;
        }
        self.free.insert(from, to);
    }

    /// How many blocks the file has.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Every free block, for a test.
    #[cfg(test)]
    pub fn free_for_test(&self) -> BTreeSet<u64> {
        self.free
            .iter()
            .flat_map(|(&start, &end)| start..end)
            .collect()
    }
}

/// One list of a record as the open read it into the owed list: the record's entry for it, where
/// its items begin in the owed list, and the blocks they came from, each with the index of its
/// first item read and how many it held. Its first item alone may have pages freed already
/// (`entry.head.freed`): the pass starts that item's free from them, and the first checkpoint
/// after the open names this list again with its head mapped back from the owed list's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwedPart {
    pub entry: Entry,
    pub from: u64,
    pub blocks: Vec<(u64, u32, u32)>,
}

/// The open's read of a record into the owed list (DESIGN.md §6.5): its lists in the record's
/// order, each from its head, each item appended to the owed list and its pages marked retired,
/// the pages a list's head item had freed left unmarked. Its place is kept across its reads.
pub struct OwedRead {
    record: Record,
    /// The list being read, its blocks once walked, and the item next in them.
    list: usize,
    walk: Option<Walk>,
    blocks: Option<Vec<(u64, Block)>>,
    at: (usize, usize),
    /// The item being appended, once its pages are marked.
    marked: bool,
    parts: Vec<OwedPart>,
    /// Every page it marked retired.
    #[cfg(test)]
    retired: Vec<PageId>,
}

impl OwedRead {
    pub fn new(record: Record) -> Self {
        Self {
            record,
            list: 0,
            walk: None,
            blocks: None,
            at: (0, 0),
            marked: false,
            parts: Vec::new(),
            #[cfg(test)]
            retired: Vec::new(),
        }
    }

    /// Every page it marked retired.
    #[cfg(test)]
    pub fn marked(&self) -> &[PageId] {
        &self.retired
    }

    /// Reads the record into `lists`' owed list: the parts it read, in order. Step it until done.
    pub fn step(
        &mut self,
        source: &dyn Source,
        lists: &Lists,
        pool: &Arc<BufferPool>,
        store: &PageStore,
    ) -> Result<IOResult<Vec<OwedPart>>> {
        while self.list < self.record.lists.len() {
            let entry = self.record.lists[self.list];
            if self.blocks.is_none() {
                let walk = self.walk.get_or_insert_with(|| Walk::new(entry));
                let blocks = done!(walk.step(source));
                self.walk = None;
                let (head, freed) = entry.head.place_in(entry.list, &blocks[0].1)?;
                // Held by index from now on: the owed list holds every item from the head on.
                let entry = Entry {
                    head: Head {
                        at: At::Index(head as u64),
                        freed,
                        ..entry.head
                    },
                    ..entry
                };
                self.record.lists[self.list] = entry;
                self.parts.push(OwedPart {
                    entry,
                    from: lists.end(lists.owed()),
                    blocks: blocks
                        .iter()
                        .enumerate()
                        .map(|(k, (number, block))| {
                            let first = if k == 0 { head } else { 0 };
                            (*number, first as u32, (block.items.len() - first) as u32)
                        })
                        .collect(),
                });
                self.at = (0, head);
                self.blocks = Some(blocks);
            }
            let blocks = self.blocks.as_ref().expect("walked just above");
            // The head as the open holds it, by index, whether it was named so or by position.
            let head = self.record.lists[self.list].head;
            let (b, i) = self.at;
            let Some((_, block)) = blocks.get(b) else {
                self.blocks = None;
                self.list += 1;
                continue;
            };
            let Some(&item) = block.items.get(i) else {
                self.at = (b + 1, 0);
                continue;
            };
            if !self.marked {
                let pages = match done!(value::pages(pool, item.reference, &|| true)) {
                    Ok(pages) => pages,
                    Err(_) => {
                        return Err(corrupt_list(
                            entry.list,
                            &format!("the value of item {i} of its block {b} does not read back"),
                        ))
                    }
                };
                // A list's head item may have pages freed: its first chunks, since a value's
                // chunks go first and its directory last, so the directory names them all.
                let first = b == 0 && head.at == At::Index(i as u64);
                let freed = if first { head.freed as usize } else { 0 };
                for &pid in pages.iter().skip(1).chain(pages.first()).skip(freed) {
                    store.retire(pid);
                    #[cfg(test)]
                    self.retired.push(pid);
                }
                self.marked = true;
            }
            done!(lists.append_owed(item.start, item.reference));
            self.marked = false;
            self.at = (b, i + 1);
        }
        Ok(IOResult::Done(std::mem::take(&mut self.parts)))
    }
}

fn corrupt_list(list: u64, what: &str) -> LimboError {
    LimboError::Corrupt(format!("retired file list {list}: {what}"))
}

/// Asserts that `bytes`, read as `what`, are one block: every source of the file reads a whole
/// block or none, so another length is the caller's fault, not the file's.
fn assert_whole(bytes: &[u8], what: &str) {
    assert_eq!(
        bytes.len(),
        BLOCK,
        "{what} read as {} bytes, not a block of {BLOCK}",
        bytes.len()
    );
}

/// Writes the checksum of `bytes`, at `place`: over the place's number and every byte but the
/// checksum's own.
fn seal(bytes: &mut [u8], place: u64) {
    let sum = checksum(bytes, place);
    bytes[..4].copy_from_slice(&sum.to_le_bytes());
}

fn sealed(bytes: &[u8], place: u64) -> bool {
    u32_at(bytes, 0) == checksum(bytes, place)
}

fn checksum(bytes: &[u8], place: u64) -> u32 {
    crc::append(crc::of(&place.to_le_bytes()), &bytes[4..])
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}

fn u48_at(bytes: &[u8], at: usize) -> u64 {
    let mut word = [0u8; 8];
    word[..6].copy_from_slice(&bytes[at..at + 6]);
    u64::from_le_bytes(word)
}
