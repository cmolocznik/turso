//! The retired file wired to the checkpoint and the open (DESIGN.md §6.5): what a checkpoint keeps
//! of the retired lists, and what the open reads back into the owed list.
//!
//! - **The cut** reads every list's head and its first item's pages freed, under the closed gate,
//!   and keeps each slot's list pages from its head until the checkpoint has read them.
//! - **The checkpoint** takes each slot's items from the larger of its written mark and its cut
//!   head while their writer committed at or below `C`, writes them into new blocks after the
//!   slot's last, then its record and its header into the place the superblock in charge does not
//!   name. None of what it writes is a block the record in charge holds, so a kill before its
//!   superblock leaves that record whole, whichever of its writes reached the disk. A checkpoint
//!   with no new item and no head moved writes nothing and names the header before.
//! - **Once its superblock is durable** the written marks move and the blocks the record before
//!   held and the new one does not are free; a checkpoint that fails moves nothing.
//! - **The open** reads the record the superblock names into the owed list, before replay: the
//!   file holds only items whose writer committed at or below the snapshot, and replay applies only
//!   bodies committed above it, so no value is freed by both.

use super::super::aligned::AlignedBlock;
#[cfg(test)]
use super::super::btree::value::ValueRef;
use super::super::buffer::BufferPool;
use super::super::checkpoint::kept_by_image;
use super::super::control::Superblock;
use super::super::meta::TREES;
#[cfg(test)]
use super::super::page::PageId;
use super::super::page::CLASS_COUNT;
use super::super::requests::{Counted, CountedFile};
use super::super::store::{directory, PageStore};
use super::super::txn::{Transactions, Ts};
use super::collect::Collector;
use super::file::{
    self, At, Block, Blocks, Entry, Head, OwedPart, OwedRead, Record, RecordRead, Source, BLOCK,
    PER_BLOCK,
};
use super::list::{Item, Lists, Look, Reader};
use crate::io::{Completion, File, OpenFlags, IO};
use crate::storage::atoll::locks::Mutex;
use crate::storage::atoll::unit;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::{LimboError, Result};
use std::collections::{BTreeSet, VecDeque};

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// Writes of the file a checkpoint keeps in flight at most.
const WRITES: usize = 32;

/// The blocks a failed checkpoint took that one unit gives back: as many as one block of the file
/// holds numbers.
const GIVE_RUN: usize = BLOCK / std::mem::size_of::<u64>();

/// The retired lists of a database and its retired file, `{base}.retired`.
pub struct Retired {
    file: Arc<CountedFile>,
    pool: Arc<BufferPool>,
    lists: Lists,
    collector: Collector,
    state: Mutex<FileState>,
    /// The pages the open marked retired from the owed list.
    #[cfg(test)]
    marked: Mutex<Vec<PageId>>,
    /// The value of each item a checkpoint judged, once each time it judged it.
    #[cfg(test)]
    judged: Mutex<Vec<ValueRef>>,
}

/// What the record in charge holds, in memory. A checkpoint changes it by what it wrote and what
/// the heads passed, never by going through what it holds (the auditor's #6308, finding 3).
struct FileState {
    blocks: Blocks,
    next_list: u64,
    /// The header the superblock in charge names: its place and its checkpoint.
    in_charge: Option<(u8, u64)>,
    /// The record in charge's lists, and its own blocks.
    entries: Vec<Entry>,
    record: Vec<u64>,
    slots: Vec<SlotFile>,
    /// The lists of an earlier record the owed list holds, in its order, from the first its head
    /// has not passed.
    owed: VecDeque<OwedList>,
    /// What the checkpoint under way wrote, in charge once its superblock is durable.
    pending: Option<Pending>,
}

/// One slot's list in the file.
#[derive(Debug, Clone, Default)]
struct SlotFile {
    /// Its id, once a block of it is written.
    id: Option<u64>,
    /// The position past its last item in the file.
    written: u64,
    /// Its blocks from its head's to its last.
    segments: VecDeque<Segment>,
    /// Its last block and that block's count in the list.
    last: Option<(u64, u64)>,
    /// Where the last checkpoint's read of the list stopped, at `written`, with the list page
    /// it held: the next goes on from there while the head is behind it, since a give-back
    /// stops at the head. `None` after an open and after a failed checkpoint.
    place: Option<Reader>,
}

/// A list of an earlier record that the owed list holds: as the open read it, its count of items,
/// and how many of its blocks, holding how many items, the owed head has passed.
#[derive(Debug, Clone)]
struct OwedList {
    part: OwedPart,
    items: u64,
    passed: usize,
    passed_items: u64,
}

/// A block of a slot's list: its number and its last item's position. Its items are those the
/// image kept, so the block is wholly behind a head past that position, whatever positions lie
/// between its items.
#[derive(Debug, Clone, Copy)]
struct Segment {
    block: u64,
    last: u64,
}

/// A checkpoint's part of the file, kept in its run from its cut to its end: a free or two for
/// each of its parts, whatever it holds, and its flight set held to [`WRITES`].
#[derive(Default)]
pub struct RetiredRun {
    /// Each list's head and its first item's pages freed at the cut, the owed list's last.
    heads: Vec<(u64, u64)>,
    /// The slot being read and its place, the item read whose value is being judged, the items
    /// taken, slot after slot, and where each slot's end in them.
    slot: usize,
    reader: Option<Reader>,
    judging: Option<Judging>,
    items: Vec<Item>,
    ends: Vec<usize>,
    /// Where each slot's read stopped: past every item taken, those left out among them, with
    /// the list page it held.
    read_to: Vec<Reader>,
    /// The values of the items left out, for the test build's second judging of every item.
    #[cfg(test)]
    left: Vec<ValueRef>,
    #[cfg(test)]
    rechecked: usize,
    planned: bool,
    /// The blocks to write, in order, and the next to issue: each list block is built from
    /// `items` as it is issued, the record's blocks and the header lie in `bytes`.
    blocks: Vec<Planned>,
    bytes: Vec<u8>,
    next: usize,
    /// The writes issued and not yet retired: what each write's end completes, with its outcome,
    /// and what its request's retirement completes.
    flying: Vec<(Completion, Completion)>,
    synced: bool,
    /// The header it wrote: its place and checkpoint.
    wrote: Option<(u8, u64)>,
    /// Each slot's written mark and reading place past the items passed over, where nothing is
    /// written: they move once the superblock is durable, as a record's would.
    passed: Option<Vec<Reader>>,
    /// The blocks it took, and how many a failure has given back.
    taken: Vec<u64>,
    given: usize,
}

/// Where the judging of the value of the item read stands (DESIGN.md §6.5).
#[derive(Debug, Clone)]
enum Judging {
    /// Its directory page not yet judged.
    Item(Item),
    /// Its directory page judged `kept`, the directory not yet read for its chunks, which test
    /// builds judge too: those past the `freed` chunks the cut saw freed of it.
    #[cfg(test)]
    Directory {
        item: Item,
        kept: bool,
        freed: usize,
    },
    /// Its pages, the directory page first, judged up to `at`, which starts past those freed.
    #[cfg(test)]
    Chunks {
        item: Item,
        kept: bool,
        pages: Vec<PageId>,
        freed: usize,
        at: usize,
    },
}

/// A block a checkpoint writes.
#[derive(Debug, Clone, Copy)]
enum Planned {
    /// Block `number` of list `list` at `count` after `before`, of `len` items from `from` in the
    /// run's items.
    List {
        number: u64,
        list: u64,
        count: u64,
        before: Option<u64>,
        from: usize,
        len: usize,
    },
    /// The [`BLOCK`] bytes from `from` in the run's bytes, written at `at`.
    Bytes { at: u64, from: usize },
}

/// What a checkpoint changes of the state once its superblock is durable: its record, each slot's
/// list, the owed lists its head passed, and the blocks the record before held and its own does
/// not, which are then free.
struct Pending {
    header: (u8, u64),
    entries: Vec<Entry>,
    record: Vec<u64>,
    next_list: u64,
    slots: Vec<SlotChange>,
    /// The owed lists the head passed whole, and where it stands in the next: the list, its
    /// blocks passed and their items.
    owed_passed: usize,
    owed_head: Option<(usize, usize, u64)>,
    freed: Vec<u64>,
}

/// What a checkpoint changes of one slot's list: its id, mark, last block and place, the blocks
/// its head passed at the front, and its new blocks at the end.
struct SlotChange {
    id: Option<u64>,
    written: u64,
    last: Option<(u64, u64)>,
    place: Reader,
    passed: usize,
    segments: Vec<Segment>,
}

impl Retired {
    /// The retired file of a database being created, made empty with it: its first superblock
    /// names no header.
    pub fn create(
        counted: &Arc<Counted>,
        base: &str,
        pool: &Arc<BufferPool>,
        slots: usize,
    ) -> Result<Self> {
        let file = open(counted, base)?;
        counted.sync_directory(&directory(base))?;
        Ok(Self::empty(file, pool, slots))
    }

    /// The retired file of a database `superblock` is in charge of, with the record it names read
    /// into the owed list and each owed page marked retired.
    pub fn open(
        counted: &Arc<Counted>,
        base: &str,
        superblock: &Superblock,
        pool: &Arc<BufferPool>,
        store: &PageStore,
        slots: usize,
    ) -> Result<Self> {
        let file = open(counted, base)?;
        let retired = Self::empty(file, pool, slots);
        let Some((place, checkpoint)) = superblock.retired else {
            return Ok(retired);
        };
        let io: Arc<dyn IO> = counted.clone();
        let source = FileSource {
            io: &io,
            file: &retired.file,
            len: retired.file.size()?,
        };
        let bytes = match source.read(place as u64 * BLOCK as u64)? {
            Some(bytes) => bytes,
            None => {
                return Err(LimboError::Corrupt(format!(
                    "retired file header {place}: missing, where the superblock names it"
                )))
            }
        };
        let header = file::decode_header(place, checkpoint, &bytes)?;
        let mut read = RecordRead::new(header);
        let (record, numbers) = crate::storage::atoll::drive(&io, || read.step(&source))?;
        let mut owed = OwedRead::new(record.clone());
        let parts =
            crate::storage::atoll::drive(&io, || owed.step(&source, &retired.lists, pool, store))?;
        #[cfg(test)]
        {
            *retired.marked.lock() = owed.marked().to_vec();
        }
        retired.lists.set_owed_starts(
            parts
                .iter()
                .map(|p| (p.from, p.entry.head.freed as u64))
                .collect(),
        );
        let len = (source.len / BLOCK as u64).saturating_sub(2);
        {
            let mut state = retired.state.lock();
            let state = &mut *state;
            state.next_list = record.next_list;
            state.in_charge = Some((place, checkpoint));
            // As the open holds them, each part by index: the record in charge means these.
            state.entries = parts.iter().map(|part| part.entry).collect();
            state.record = numbers;
            state.owed = parts
                .into_iter()
                .map(|part| OwedList {
                    items: part.blocks.iter().map(|&(_, _, count)| count as u64).sum(),
                    part,
                    passed: 0,
                    passed_items: 0,
                })
                .collect();
            state.blocks = Blocks::new(len, &state.held());
        }
        Ok(retired)
    }

    fn empty(file: Arc<CountedFile>, pool: &Arc<BufferPool>, slots: usize) -> Self {
        Self {
            file,
            pool: pool.clone(),
            lists: Lists::new(pool.clone(), slots),
            collector: Collector::new(slots + 1),
            state: Mutex::new(FileState {
                blocks: Blocks::default(),
                next_list: 0,
                in_charge: None,
                entries: Vec::new(),
                record: Vec::new(),
                slots: vec![SlotFile::default(); slots],
                owed: VecDeque::new(),
                pending: None,
            }),
            #[cfg(test)]
            marked: Mutex::new(Vec::new()),
            #[cfg(test)]
            judged: Mutex::new(Vec::new()),
        }
    }

    pub fn lists(&self) -> &Lists {
        &self.lists
    }

    pub fn collector(&self) -> &Collector {
        &self.collector
    }

    /// A checkpoint's cut, under the store's closed gate: every list's head and progress, O(slots),
    /// and each slot's list pages kept from its head until the checkpoint has read them.
    pub fn cut(&self, run: &mut RetiredRun) {
        let owed = self.lists.owed();
        *run = RetiredRun {
            heads: (0..=owed).map(|list| self.collector.head(list)).collect(),
            ..RetiredRun::default()
        };
        for slot in 0..owed {
            self.lists.keep(slot, run.heads[slot as usize].0);
        }
    }

    /// One unit of the checkpoint's work on the file, for checkpoint `n` as of snapshot `c`, one
    /// page at most (DESIGN.md 22.1): a page of a slot's list read for the slot's next item, an
    /// item's value judged by the image, the plan of what it writes, which reads no page, or one
    /// write issued. `Done(true)` once each slot's items its snapshot sees committed are read and
    /// judged and the record and its header are written and landed, `Done(false)` once a unit is
    /// done and the next is ready. An item whose value the image leaves out ([`kept_by_image`] of
    /// its directory page) is passed over: no later checkpoint images that value.
    pub fn step(
        &self,
        run: &mut RetiredRun,
        txns: &Transactions,
        c: Ts,
        n: u64,
        heights: &[[u8; CLASS_COUNT]; TREES],
        store: &Arc<PageStore>,
    ) -> Result<IOResult<bool>> {
        if run.slot < self.lists.owed() as usize {
            done!(self.read_unit(run, txns, c, n, heights, store));
            return Ok(IOResult::Done(false));
        }
        if !run.planned {
            self.plan(run, n);
            for slot in 0..self.lists.owed() {
                self.lists.unkeep(slot);
            }
            run.planned = true;
            return Ok(IOResult::Done(false));
        }
        self.write_unit(run)
    }

    /// One unit of reading the slots' lists: the value of the item read judged, or a page of the
    /// slot's list read for its next item. A slot ends at the first item whose writer did not
    /// commit at or below `c`, or at its list's end; slots with nothing past where their read
    /// starts end in the same unit, with no page read.
    fn read_unit(
        &self,
        run: &mut RetiredRun,
        txns: &Transactions,
        c: Ts,
        n: u64,
        heights: &[[u8; CLASS_COUNT]; TREES],
        store: &Arc<PageStore>,
    ) -> Result<IOResult<()>> {
        if run.judging.is_some() {
            return self.judge_unit(run, n, heights, store);
        }
        if run.slot == 0 && run.read_to.capacity() == 0 {
            // Each slot's end and place, pushed a slot at a time: room for every slot at once.
            let slots = self.lists.owed() as usize;
            run.ends.reserve_exact(slots);
            run.read_to.reserve_exact(slots);
        }
        while run.slot < self.lists.owed() as usize {
            let slot = run.slot as u16;
            let reader = match &mut run.reader {
                Some(reader) => reader,
                None => {
                    let (written, place) = {
                        let state = self.state.lock();
                        let file = &state.slots[run.slot];
                        (file.written, file.place)
                    };
                    // The page the last read stopped on is the list's while the head is behind
                    // it: a give-back stops at the head, and a rollback cuts only past the mark.
                    let head = run.heads[run.slot].0;
                    run.reader.insert(match place {
                        Some(place) if head < place.at() => place,
                        _ => Reader::from(written.max(head)),
                    })
                }
            };
            let looked = reader.at() < self.lists.end(slot);
            if looked {
                unit::visit();
                let look = done!(self.lists.look(slot, reader));
                #[cfg(test)]
                LIST_READS.with(|n| n.set(n.get() + 1));
                match look {
                    Look::Further => return Ok(IOResult::Done(())),
                    Look::Item(item) if txns.committed_by(slot, item.start, c) => {
                        run.judging = Some(Judging::Item(item));
                        return Ok(IOResult::Done(()));
                    }
                    // Its writer open at the cut or committed above it: never passed.
                    Look::Item(_) | Look::End => {}
                }
            }
            let at = *reader;
            run.ends.push(run.items.len());
            run.read_to.push(at);
            run.slot += 1;
            run.reader = None;
            if looked {
                break;
            }
        }
        Ok(IOResult::Done(()))
    }

    /// One unit of judging the value of the item read: its directory page judged by the image, a
    /// page-table leaf read. Test builds then read a chunked value's directory in a unit of its
    /// own and judge each chunk a unit, asserting that it agrees with its directory page, and flag
    /// every page of a value left out, for the check at its free (DESIGN.md §6.5).
    fn judge_unit(
        &self,
        run: &mut RetiredRun,
        n: u64,
        heights: &[[u8; CLASS_COUNT]; TREES],
        store: &Arc<PageStore>,
    ) -> Result<IOResult<()>> {
        match run.judging.as_mut().expect("an item being judged") {
            Judging::Item(item) => {
                let item = *item;
                unit::visit();
                let kept = done!(kept_by_image(store, n, heights, item.reference.pid));
                #[cfg(test)]
                self.judged.lock().push(item.reference);
                #[cfg(test)]
                if item.reference.directory() {
                    // A head item's chunks are freed first: those the cut saw freed are free in
                    // the image whatever it keeps of the rest.
                    let (head, freed) = run.heads[run.slot];
                    let freed = if item.position == head {
                        freed as usize
                    } else {
                        0
                    };
                    run.judging = Some(Judging::Directory { item, kept, freed });
                    return Ok(IOResult::Done(()));
                }
                #[cfg(test)]
                if !kept {
                    store.flag_left_out(item.reference.pid);
                }
                run.judged(item, kept);
            }
            // The directory's read counts its own visit.
            #[cfg(test)]
            Judging::Directory { item, kept, freed } => {
                let (item, kept, freed) = (*item, *kept, *freed);
                let pages = match done!(super::super::btree::value::pages(
                    &self.pool,
                    item.reference,
                    &|| true
                )) {
                    Ok(pages) => pages,
                    Err(_) => {
                        return Err(LimboError::InternalError(format!(
                            "the pages of {:?} do not read",
                            item.reference
                        )))
                    }
                };
                let all_freed = 1 + freed >= pages.len();
                run.judging = Some(Judging::Chunks {
                    item,
                    kept,
                    pages,
                    freed,
                    at: 1 + freed,
                });
                if all_freed {
                    run.chunks_judged(store);
                }
            }
            #[cfg(test)]
            Judging::Chunks {
                item,
                kept,
                pages,
                at,
                ..
            } => {
                let pid = pages[*at];
                unit::visit();
                let page_kept = done!(kept_by_image(store, n, heights, pid));
                assert_eq!(
                    page_kept, *kept,
                    "{pid:?} of {:?}: kept {page_kept}, its directory page kept {kept}",
                    item.reference
                );
                *at += 1;
                if *at == pages.len() {
                    run.chunks_judged(store);
                }
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Lays out what checkpoint `n` writes, from the items `run` took and its cut's heads, and
    /// what it changes of the state once durable. Its work is in proportion to the lists, what it
    /// writes and what the heads passed since the checkpoint before, never to what the record
    /// holds (the auditor's #6308, finding 3).
    fn plan(&self, run: &mut RetiredRun, n: u64) {
        let mut state = self.state.lock();
        let state = &mut *state;
        let mut next_list = state.next_list;
        let mut entries = Vec::new();
        let mut freed = Vec::new();
        let (owed_passed, owed_head) = owed_entries(state, run, &mut entries, &mut freed);
        let mut slots = Vec::with_capacity(state.slots.len());
        let mut from = 0;
        for (slot, file) in state.slots.iter().enumerate() {
            tally();
            let mut change = SlotChange {
                id: file.id,
                written: file.written,
                last: file.last,
                place: run.read_to[slot],
                passed: 0,
                segments: Vec::new(),
            };
            let end = run.ends[slot];
            for chunk in (from..end).step_by(PER_BLOCK) {
                tally();
                let len = PER_BLOCK.min(end - chunk);
                let id = *change.id.get_or_insert_with(|| {
                    next_list += 1;
                    next_list - 1
                });
                let number = state.blocks.take();
                run.taken.push(number);
                let count = change.last.map_or(0, |(_, count)| count + 1);
                run.blocks.push(Planned::List {
                    number,
                    list: id,
                    count,
                    before: change.last.map(|(block, _)| block),
                    from: chunk,
                    len,
                });
                change.segments.push(Segment {
                    block: number,
                    last: run.items[chunk + len - 1].position,
                });
                change.last = Some((number, count));
                change.written = run.items[chunk + len - 1].position + 1;
            }
            change.written = change.written.max(run.read_to[slot].at());
            from = end;
            // Blocks wholly behind the head leave the list; items taken now lie past it.
            let (head, freed_pages) = run.heads[slot];
            while let Some(segment) = file.segments.get(change.passed) {
                if segment.last >= head {
                    break;
                }
                tally();
                freed.push(segment.block);
                change.passed += 1;
            }
            if let Some(segment) = file
                .segments
                .get(change.passed)
                .or_else(|| change.segments.first())
            {
                // Named by its position: the block holds an item at or past it (an old block not
                // wholly behind it, or a new one, whose items lie past the head), and the open
                // gives the freed pages to that item only if it is the head's own.
                let (last, last_count) = change.last.expect("a list with blocks has a last");
                entries.push(Entry {
                    list: change.id.expect("a list with blocks has an id"),
                    head: Head {
                        block: segment.block,
                        at: At::Position(head),
                        freed: freed_pages as u32,
                    },
                    last,
                    last_count,
                });
            }
            slots.push(change);
        }
        if run.items.is_empty() && entries == state.entries {
            run.passed = Some(run.read_to.clone());
            return;
        }
        let record = Record {
            checkpoint: n,
            next_list,
            lists: entries,
        };
        let (header, record_blocks) = file::write_record(&record, &mut state.blocks);
        let place = file::header_place(state.in_charge.map_or(1, |(place, _)| place));
        let numbers: Vec<u64> = record_blocks.iter().map(|(number, _)| *number).collect();
        run.taken.extend_from_slice(&numbers);
        for (number, bytes) in record_blocks {
            tally();
            run.blocks.push(Planned::Bytes {
                at: block_at(number),
                from: run.bytes.len(),
            });
            run.bytes.extend_from_slice(&bytes);
        }
        run.blocks.push(Planned::Bytes {
            at: place as u64 * BLOCK as u64,
            from: run.bytes.len(),
        });
        run.bytes
            .extend_from_slice(&file::encode_header(place, &header));
        // The record before's own blocks: this one replaces it.
        freed.extend_from_slice(&state.record);
        run.wrote = Some((place, n));
        state.pending = Some(Pending {
            header: (place, n),
            entries: record.lists,
            record: numbers,
            next_list,
            slots,
            owed_passed,
            owed_head,
            freed,
        });
    }

    /// One unit of the file's writes: a write that failed fails the checkpoint; else the next
    /// block's write issued, its place in the flight set held until its request retires,
    /// [`WRITES`] of them at most; else a wait for the first in flight. `Done(true)` once every
    /// write has landed.
    fn write_unit(&self, run: &mut RetiredRun) -> Result<IOResult<bool>> {
        let ended =
            |(end, retired): &(Completion, Completion)| end.finished() && retired.finished();
        if let Some(e) = run
            .flying
            .iter()
            .filter(|write| ended(write))
            .find_map(|(end, _)| end.get_error())
        {
            return Err(LimboError::CompletionError(e));
        }
        run.flying.retain(|write| !ended(write));
        if run.next < run.blocks.len() && run.flying.len() < WRITES {
            let (at, bytes) = match run.blocks[run.next] {
                Planned::List {
                    number,
                    list,
                    count,
                    before,
                    from,
                    len,
                } => {
                    let block = Block {
                        list,
                        count,
                        before,
                        items: run.items[from..from + len]
                            .iter()
                            .map(|item| file::Item {
                                start: item.start,
                                position: item.position,
                                reference: item.reference,
                            })
                            .collect(),
                    };
                    (block_at(number), file::encode_block(number, &block))
                }
                Planned::Bytes { at, from } => (at, run.bytes[from..from + BLOCK].to_vec()),
            };
            unit::visit();
            let mut block = AlignedBlock::zeroed(BLOCK);
            block.bytes_mut().copy_from_slice(&bytes);
            let block = Arc::new(block);
            let (write, end) = super::super::whole_write(BLOCK);
            let write = self.file.pwrite(at, block.region(0, BLOCK), write)?;
            let retired = crate::storage::atoll::notify::wait();
            let place = retired.clone();
            write.on_retired(move || place.complete(0));
            run.flying.push((end, retired));
            run.next += 1;
            return Ok(IOResult::Done(false));
        }
        Ok(match run.flying.first() {
            Some((end, _)) if !end.finished() => IOResult::IO(IOCompletions(end.clone())),
            Some((_, retired)) => IOResult::IO(IOCompletions(retired.clone())),
            None => IOResult::Done(true),
        })
    }

    /// The header the checkpoint wrote, for its superblock to name; `None` when it wrote nothing
    /// and the superblock names the header before.
    pub fn written(&self, run: &RetiredRun) -> Option<(u8, u64)> {
        run.wrote
    }

    /// The file's data sync, issued once with the data files' syncs when the checkpoint wrote to
    /// it; `None` when it wrote nothing or the sync is issued.
    pub fn sync(&self, run: &mut RetiredRun) -> Result<Option<Completion>> {
        if run.wrote.is_none() || run.synced {
            return Ok(None);
        }
        run.synced = true;
        Ok(Some(self.file.sync_data(Completion::new_sync(|_| {}))?))
    }

    /// The checkpoint's superblock is durable: what it wrote is in charge, and every block the
    /// record before held and the new one does not is free. Where it wrote nothing, its written
    /// marks and reading places still move past the items it passed over.
    pub fn durable(&self, run: &mut RetiredRun) {
        // What it took the record in charge now holds: none of it is given back.
        run.taken.clear();
        run.given = 0;
        let mut state = self.state.lock();
        let state = &mut *state;
        if run.wrote.take().is_none() {
            if let Some(places) = run.passed.take() {
                for (file, place) in state.slots.iter_mut().zip(places) {
                    tally();
                    file.written = place.at();
                    file.place = Some(place);
                }
            }
            return;
        }
        let pending = state.pending.take().expect("what the checkpoint wrote");
        for (file, change) in state.slots.iter_mut().zip(pending.slots) {
            tally();
            for _ in 0..change.passed {
                tally();
                file.segments.pop_front();
            }
            for segment in change.segments {
                tally();
                file.segments.push_back(segment);
            }
            file.id = change.id;
            file.written = change.written;
            file.last = change.last;
            file.place = Some(change.place);
        }
        for _ in 0..pending.owed_passed {
            tally();
            state.owed.pop_front();
        }
        if let Some((list, passed, passed_items)) = pending.owed_head {
            let owed = &mut state.owed[list - pending.owed_passed];
            owed.passed = passed;
            owed.passed_items = passed_items;
        }
        state.in_charge = Some(pending.header);
        state.entries = pending.entries;
        state.record = pending.record;
        state.next_list = pending.next_list;
        for block in pending.freed {
            tally();
            state.blocks.free(block);
        }
    }

    /// The checkpoint failed or the database closes on it: nothing it planned is in charge, no
    /// list page is kept for it, and each slot's read starts again from its head. The blocks it
    /// took are given back after, a run a unit ([`give_back_taken`](Self::give_back_taken)).
    pub fn failed(&self, run: &mut RetiredRun) {
        run.wrote = None;
        run.passed = None;
        for slot in 0..self.lists.owed() {
            self.lists.unkeep(slot);
        }
        let mut state = self.state.lock();
        state.pending = None;
        for file in state.slots.iter_mut() {
            file.place = None;
        }
    }

    /// One run of a failed checkpoint's taken blocks given back, [`GIVE_RUN`] at most: whether
    /// none was left to give, so the call did nothing.
    pub fn give_back_taken(&self, run: &mut RetiredRun) -> bool {
        if run.given == run.taken.len() {
            return true;
        }
        let end = run.taken.len().min(run.given + GIVE_RUN);
        let mut state = self.state.lock();
        for &block in &run.taken[run.given..end] {
            state.blocks.free(block);
        }
        run.given = end;
        false
    }

    /// How many times a checkpoint judged the item of the value `reference` names, for a test.
    #[cfg(test)]
    pub fn judgings(&self, reference: ValueRef) -> usize {
        self.judged
            .lock()
            .iter()
            .filter(|&&judged| judged == reference)
            .count()
    }

    /// Takes `blocks` blocks of the file into `run`, as its plan takes those it writes, for a test:
    /// which.
    #[cfg(test)]
    pub fn take_for_test(&self, run: &mut RetiredRun, blocks: usize) -> Vec<u64> {
        let mut state = self.state.lock();
        let taken: Vec<u64> = (0..blocks).map(|_| state.blocks.take()).collect();
        run.taken.extend_from_slice(&taken);
        taken
    }

    /// The blocks the record in charge holds, the free blocks, and the file's length in blocks,
    /// for a test.
    #[cfg(test)]
    pub fn blocks_for_test(&self) -> (BTreeSet<u64>, BTreeSet<u64>, u64) {
        let state = self.state.lock();
        (
            state.held(),
            state.blocks.free_for_test(),
            state.blocks.len(),
        )
    }

    /// How many items checkpoints have judged in all, for a test.
    #[cfg(test)]
    pub fn judged_in_all(&self) -> usize {
        self.judged.lock().len()
    }

    /// The blocks `run` planned to write, in order.
    #[cfg(test)]
    pub fn taken(&self, run: &RetiredRun) -> Vec<u64> {
        run.blocks
            .iter()
            .map(|planned| match *planned {
                Planned::List { number, .. } => number,
                Planned::Bytes { at, .. } => at / BLOCK as u64,
            })
            .collect()
    }

    /// The items the record in header place `place` of checkpoint `checkpoint` holds, each list
    /// from its head, read back from the file.
    #[cfg(test)]
    pub fn read_record(
        &self,
        io: &Arc<dyn IO>,
        place: u8,
        checkpoint: u64,
    ) -> Result<Vec<file::Item>> {
        let source = FileSource {
            io,
            file: &self.file,
            len: self.file.size()?,
        };
        let bytes = source
            .read(place as u64 * BLOCK as u64)?
            .ok_or_else(|| LimboError::Corrupt("no header".into()))?;
        let header = file::decode_header(place, checkpoint, &bytes)?;
        let mut read = RecordRead::new(header);
        let (record, _) = crate::storage::atoll::drive(io, || read.step(&source))?;
        let mut items = Vec::new();
        for entry in record.lists {
            let mut walk = file::Walk::new(entry);
            let blocks = crate::storage::atoll::drive(io, || walk.step(&source))?;
            let (head, _) = entry.head.place_in(entry.list, &blocks[0].1)?;
            for (k, (_, block)) in blocks.into_iter().enumerate() {
                let from = if k == 0 { head } else { 0 };
                items.extend(block.items.into_iter().skip(from));
            }
        }
        Ok(items)
    }

    /// One item `run` judged, judged again by the image once the checkpoint has made its last edit
    /// to the image's page table: every metadata page written, and the cut's view still kept, so
    /// what it reads is what the image holds. An item it wrote must still be kept, one it left out
    /// still not (#6288). A page-table leaf read; whether an item was left to judge.
    #[cfg(test)]
    pub fn recheck(
        &self,
        run: &mut RetiredRun,
        store: &Arc<PageStore>,
        n: u64,
        heights: &[[u8; CLASS_COUNT]; TREES],
    ) -> Result<IOResult<bool>> {
        let written = run.items.len();
        if run.rechecked == written + run.left.len() {
            return Ok(IOResult::Done(false));
        }
        let (reference, judged) = match run.rechecked.checked_sub(written) {
            None => (run.items[run.rechecked].reference, true),
            Some(i) => (run.left[i], false),
        };
        unit::visit();
        let kept = done!(kept_by_image(store, n, heights, reference.pid));
        assert_eq!(
            kept, judged,
            "{reference:?}: the image keeps it {kept} at its last edit, judged {judged} by the \
             Retired phase"
        );
        run.rechecked += 1;
        Ok(IOResult::Done(true))
    }

    /// Asserts, at the open's end, that every page the open marked retired from the owed list is
    /// still marked: a value replay freed was not one the owed list holds.
    #[cfg(test)]
    pub fn assert_owed_unfreed(&self, store: &PageStore) {
        for &pid in self.marked.lock().iter() {
            assert!(
                store.retired(pid),
                "{pid:?}, a page of the owed list, was freed by the open's replay"
            );
        }
    }
}

impl RetiredRun {
    /// Every page of the chunked value being judged that the cut did not see freed is judged:
    /// each flagged if the value was left out, for the check at its free, and the item judged.
    #[cfg(test)]
    fn chunks_judged(&mut self, store: &PageStore) {
        let Some(Judging::Chunks {
            item,
            kept,
            pages,
            freed,
            ..
        }) = self.judging.take()
        else {
            unreachable!("a chunked value being judged");
        };
        if !kept {
            for &pid in pages[..1]
                .iter()
                .chain(pages.get(1 + freed..).unwrap_or(&[]))
            {
                store.flag_left_out(pid);
            }
        }
        self.judged(item, kept);
    }

    /// The item read is judged, `kept` by the image or not: taken to be written, or passed over.
    fn judged(&mut self, item: Item, kept: bool) {
        self.judging = None;
        self.reader.as_mut().expect("the slot being read").advance();
        match kept {
            true => self.items.push(item),
            #[cfg(test)]
            false => self.left.push(item.reference),
            #[cfg(not(test))]
            false => {}
        }
    }
}

/// The record's entries for the owed lists the owed head has not passed, in their order, each
/// block the head passed since the checkpoint before put in `freed`: how many lists it passed
/// whole, and where it stands in the next (the list, its blocks passed and their items).
fn owed_entries(
    state: &FileState,
    run: &RetiredRun,
    entries: &mut Vec<Entry>,
    freed: &mut Vec<u64>,
) -> (usize, Option<(usize, usize, u64)>) {
    let (head, freed_pages) = run.heads[state.slots.len()];
    let mut passed = 0;
    let mut at = None;
    for (list, owed) in state.owed.iter().enumerate() {
        tally();
        let part = &owed.part;
        assert!(
            matches!(part.entry.head.at, At::Index(_)),
            "an owed part held by its position, where the open holds every part by index"
        );
        if part.from + owed.items <= head {
            for &(block, _, _) in &part.blocks[owed.passed..] {
                tally();
                freed.push(block);
            }
            passed += 1;
            continue;
        }
        let Some(offset) = head.checked_sub(part.from) else {
            entries.push(part.entry);
            continue;
        };
        let (mut blocks, mut items) = (owed.passed, owed.passed_items);
        while items + part.blocks[blocks].2 as u64 <= offset {
            tally();
            freed.push(part.blocks[blocks].0);
            items += part.blocks[blocks].2 as u64;
            blocks += 1;
        }
        let (block, first, _) = part.blocks[blocks];
        // The part's first item starts from the pages its list had freed before the crash.
        let freed_pages = match offset {
            0 => freed_pages.max(part.entry.head.freed as u64),
            _ => freed_pages,
        };
        entries.push(Entry {
            head: Head {
                block,
                at: At::Index((first + (offset - items) as u32) as u64),
                freed: freed_pages as u32,
            },
            ..part.entry
        });
        at = Some((list, blocks, items));
    }
    (passed, at)
}

impl FileState {
    /// Every block the record in charge holds: its own, each slot's list from its head's block to
    /// its last, and each owed list's from the first its head has not passed. Gone through whole
    /// once, at the open.
    fn held(&self) -> BTreeSet<u64> {
        let mut held = BTreeSet::new();
        for &block in &self.record {
            tally();
            held.insert(block);
        }
        for segment in self.slots.iter().flat_map(|file| &file.segments) {
            tally();
            held.insert(segment.block);
        }
        for owed in &self.owed {
            for &(block, _, _) in &owed.part.blocks[owed.passed..] {
                tally();
                held.insert(block);
            }
        }
        held
    }
}

/// Counts an element of the retired file's bookkeeping gone through on this thread, for a test.
#[inline]
fn tally() {
    #[cfg(test)]
    BOOKKEEPING.with(|n| n.set(n.get() + 1));
}

#[cfg(test)]
crate::thread::thread_local! {
    /// The elements of the retired file's bookkeeping gone through on this thread since the last
    /// read.
    static BOOKKEEPING: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The pages of slots' lists the Retired phase read on this thread since the last read.
    static LIST_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The pages of slots' lists the Retired phase read on this thread since the last call, for a
/// test.
#[cfg(test)]
pub fn take_list_reads() -> u64 {
    LIST_READS.with(|n| n.replace(0))
}

/// The elements of the retired file's bookkeeping gone through on this thread since the last
/// call, for a test.
#[cfg(test)]
pub fn take_bookkeeping() -> u64 {
    BOOKKEEPING.with(|n| n.replace(0))
}

/// Where block `number` lies in the file, behind the two header places.
fn block_at(number: u64) -> u64 {
    (2 + number) * BLOCK as u64
}

fn open(counted: &Arc<Counted>, base: &str) -> Result<Arc<CountedFile>> {
    counted.open_counted(
        &format!("{base}.retired"),
        OpenFlags::Create | OpenFlags::Direct,
        false,
    )
}

/// The file's blocks as the open reads them, each read waited for.
struct FileSource<'a> {
    io: &'a Arc<dyn IO>,
    file: &'a Arc<CountedFile>,
    len: u64,
}

impl FileSource<'_> {
    /// The 4 KiB at `at`, or `None` past the file's end.
    fn read(&self, at: u64) -> Result<Option<Vec<u8>>> {
        if at + BLOCK as u64 > self.len {
            return Ok(None);
        }
        let block = Arc::new(AlignedBlock::zeroed(BLOCK));
        let c = Completion::new_read(block.region(0, BLOCK), |res| res.err());
        crate::storage::atoll::park::wait_for_completion(
            self.io.as_ref(),
            self.file.pread(at, c)?,
        )?;
        Ok(Some(block.bytes().to_vec()))
    }
}

impl Source for FileSource<'_> {
    fn block(&self, number: u64) -> Result<IOResult<Option<Vec<u8>>>> {
        Ok(IOResult::Done(self.read(block_at(number))?))
    }
}
