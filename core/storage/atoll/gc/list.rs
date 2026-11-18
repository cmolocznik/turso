//! The lists of retired values (DESIGN.md §11.5; C, the auditor's #6174): per slot, one item for
//! each write that retired a value out of line, in the order of the slot's writes, in temporary
//! pages of the pool (DESIGN.md §6.4), under its one budget. An evicted page goes to the temporary
//! file and is read back in as any page is.
//!
//! An item is its writer's start, its position (how many of the slot's items came before it since
//! the open) and the retired value's reference: [`ITEM`] bytes. A page:
//!
//! ```text
//! page header (24) | prev page: u64 | next page: u64 | first: u64 | items ...
//! ```
//!
//! `prev page` and `next page` are the slot's pages before and after it, each its raw id plus one,
//! 0 for none; `first` is the position of its first item. A page holds [`PER_PAGE`] items.
//!
//! - **Appending.** The slot's transaction writes an item past the list's published end before its
//!   leaf change ([`Lists::append`]), and publishes it with one store once its put is done
//!   ([`Lists::publish`]). Written again before that, it lands on the same place. A statement
//!   rolled back moves the end back ([`Lists::cut_back`]). The append refuses an item out of the
//!   slot's order: its writer must be the slot's running transaction, and the item before it that
//!   writer's or an earlier one's, which has ended. The checkpoint's stop rule rests on that order.
//! - **Reading.** Only a rollback reuses a position: it cuts the end back, and the slot's next
//!   transaction appends there. So an item whose writer committed never changes. Items are read
//!   optimistically ([`Lists::read`]), the writer's start in one atomic load of an aligned word. A
//!   page that went to the temporary file comes back in by a read that returns IO: the reader holds
//!   no tree's latch, and waits for it as for any read.
//! - **Giving back.** The holder of the slot's collector claim gives back the pages wholly behind a
//!   position ([`Lists::give_back`]), never the page appends go to.

use super::super::btree::value::{ValueRef, REF};
use super::super::buffer::BufferPool;
use super::super::page::{self, PageId, PageKind, SizeClass, HEADER_SIZE};
use super::super::shared::{Bytes, BytesMut};
use super::super::txn::Ts;
use super::Writers;
use crate::io::Completion;
use crate::storage::atoll::locks::Mutex;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::{LimboError, Result};
use crossbeam_utils::CachePadded;

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// The lists' page class: 4 KiB, so a slot's last page costs little while the slot idles.
pub const CLASS: SizeClass = match SizeClass::new(0) {
    Some(c) => c,
    None => panic!("no 4 KiB class"),
};

/// A list page's kind.
pub const KIND: PageKind = PageKind(9);

/// Bytes of an item: its writer's start, its position, and the reference.
pub const ITEM: usize = 16 + REF;

const PREV: usize = HEADER_SIZE;
const NEXT: usize = PREV + 8;
const FIRST: usize = NEXT + 8;
const ITEMS: usize = FIRST + 8;

/// Items a page holds.
pub const PER_PAGE: u64 = ((page::MIN_PAGE_SIZE - ITEMS) / ITEM) as u64;

/// An item of a list: a value that a write of the slot's transaction started at `start` retired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Item {
    pub start: Ts,
    pub position: u64,
    pub reference: ValueRef,
}

impl Item {
    fn encode(&self) -> [u8; ITEM] {
        let mut out = [0u8; ITEM];
        out[..8].copy_from_slice(&self.start.to_le_bytes());
        out[8..16].copy_from_slice(&self.position.to_le_bytes());
        out[16..].copy_from_slice(&self.reference.encode());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let word = |i: usize| u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        (bytes.len() == ITEM).then_some(())?;
        Some(Self {
            start: word(0),
            position: word(8),
            reference: ValueRef::decode(&bytes[16..])?,
        })
    }
}

/// Every slot's list.
pub struct Lists {
    pool: Arc<BufferPool>,
    slots: Box<[CachePadded<SlotList>]>,
    /// The owed list's parts' first positions, each with the pages its first item had freed before
    /// the crash, in order: the pass starts that item's free from them (DESIGN.md §6.5).
    owed_starts: Mutex<Vec<(u64, u64)>>,
}

#[derive(Default)]
struct SlotList {
    /// The page appends go to: its raw id plus one, 0 before the first. Written by the slot's
    /// transaction alone.
    last: AtomicU64,
    /// The first page not given back: its raw id plus one, 0 before the first. Set by the first
    /// page's taker, then moved on by the holder of the collector's claim.
    first: AtomicU64,
    /// The position past the last item published: written by the slot's transaction alone,
    /// released, so a reader of the end reads every item before it.
    end: AtomicU64,
    /// The writer's start of the last item published, for the order an append keeps.
    last_start: AtomicU64,
    /// The position below which no page is given back: a checkpoint's cut head while it reads the
    /// list, `u64::MAX` otherwise. Set and read inside the store's change gate.
    kept: AtomicU64,
}

/// What one page of a read of a slot's list finds at a reader's place ([`Lists::look`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Look {
    /// The item at the place.
    Item(Item),
    /// The place is the list's published end.
    End,
    /// The place lies past the page read: the reader moved on to the next page.
    Further,
}

/// A reader's place in a slot's list: the position it reads next, and the page holding it once
/// found (its raw id plus one, 0 until then).
#[derive(Debug, Default, Clone, Copy)]
pub struct Reader {
    page: u64,
    at: u64,
}

impl Reader {
    /// A reader of the slot's list from position `at`, which no give-back has passed.
    pub fn from(at: u64) -> Self {
        Self { page: 0, at }
    }

    /// The position it reads next.
    pub fn at(&self) -> u64 {
        self.at
    }

    /// Moves on past the item it read.
    pub fn advance(&mut self) {
        self.at += 1;
    }
}

impl Lists {
    /// The lists of `slots` slots, and the owed list after them.
    pub fn new(pool: Arc<BufferPool>, slots: usize) -> Self {
        Self {
            pool,
            slots: (0..=slots)
                .map(|_| {
                    CachePadded::new(SlotList {
                        kept: AtomicU64::new(u64::MAX),
                        ..Default::default()
                    })
                })
                .collect(),
            owed_starts: Mutex::new(Vec::new()),
        }
    }

    /// Keeps `slot`'s list pages from position `at` on until [`unkeep`](Self::unkeep): a
    /// checkpoint's cut, which reads them after.
    pub fn keep(&self, slot: u16, at: u64) {
        self.slots[slot as usize].kept.store(at, Ordering::Release);
    }

    pub fn unkeep(&self, slot: u16) {
        self.slots[slot as usize]
            .kept
            .store(u64::MAX, Ordering::Release);
    }

    /// The owed list's parts' first positions and the pages each first item had freed.
    pub fn set_owed_starts(&self, starts: Vec<(u64, u64)>) {
        *self.owed_starts.lock() = starts;
    }

    /// The pages already freed of the item at `at` before any pass began it: those its part
    /// recorded, for the first item of a part of the owed list, and none otherwise.
    pub fn owed_start(&self, slot: u16, at: u64) -> u64 {
        if slot != self.owed() {
            return 0;
        }
        let starts = self.owed_starts.lock();
        match starts.binary_search_by_key(&at, |&(from, _)| from) {
            Ok(i) => starts[i].1,
            Err(_) => 0,
        }
    }

    /// The owed list's place among the lists, after every slot's: the items the open read from the
    /// retired file (DESIGN.md §6.5), whose writers are all gone.
    pub fn owed(&self) -> u16 {
        (self.slots.len() - 1) as u16
    }

    /// The position past `slot`'s last published item.
    pub fn end(&self, slot: u16) -> u64 {
        self.slots[slot as usize].end.load(Ordering::Acquire)
    }

    /// Writes the item of a write by `slot`'s transaction started at `start` that retires the value
    /// `reference` names, at the list's end, not yet published. Its write publishes it once its
    /// put is done ([`publish`](Self::publish)); called again before that, it writes over the same
    /// place. Step it until done.
    pub fn append(
        &self,
        slot: u16,
        start: Ts,
        reference: ValueRef,
        writers: &dyn Writers,
    ) -> Result<IOResult<()>> {
        let list = &self.slots[slot as usize];
        // Relaxed: the slot's transaction's own words.
        let last_start = list.last_start.load(Ordering::Relaxed);
        if !writers.running(slot, start) || last_start > start {
            return Err(LimboError::InternalError(format!(
                "an item of slot {slot}'s transaction started at {start} out of the slot's order: \
                 running {}, the item before it of the transaction started at {last_start}",
                writers.running(slot, start)
            )));
        }
        self.write(slot, start, reference)
    }

    /// Appends to the owed list, and publishes at once, an item the open read: the retired value
    /// `reference` names, which the transaction started at `start` retired. Step it until done.
    pub fn append_owed(&self, start: Ts, reference: ValueRef) -> Result<IOResult<()>> {
        let owed = self.owed();
        if let IOResult::IO(io) = self.write(owed, start, reference)? {
            return Ok(IOResult::IO(io));
        }
        self.publish(owed, start);
        Ok(IOResult::Done(()))
    }

    /// Writes an item at `slot`'s end, not yet published. Step it until done.
    fn write(&self, slot: u16, start: Ts, reference: ValueRef) -> Result<IOResult<()>> {
        let list = &self.slots[slot as usize];
        let at = list.end.load(Ordering::Relaxed);
        let item = Item {
            start,
            position: at,
            reference,
        };
        loop {
            let last = list.last.load(Ordering::Relaxed);
            let mut full = None;
            if let Some(pid) = last.checked_sub(1).map(PageId::from_raw) {
                let mut ex = done!(self.pool.fix_exclusive(pid, None));
                let first = ex.page().u64_at(FIRST);
                if at < first + PER_PAGE {
                    let from = ITEMS + ((at - first) as usize) * ITEM;
                    let mut w = ex.page_mut();
                    w.write(from + 8, &item.encode()[8..]);
                    // The writer's start in one atomic store, an aligned word: what a checkpoint
                    // reads at its stop, where a rollback may let the slot's next writer write
                    // over the item (DESIGN.md §6.5).
                    w.put_u64(from, start);
                    ex.changed();
                    return Ok(IOResult::Done(()));
                }
                // Held while the next page is taken, to name it.
                full = Some(ex);
            }
            // A page of its own, after the slot's last, its first item the one being written.
            let mut ex = done!(self.pool.allocate_temporary(CLASS));
            let mut w = ex.page_mut();
            page::set_kind(&mut w, KIND);
            w.put_u64(PREV, last);
            w.put_u64(NEXT, 0);
            w.put_u64(FIRST, at);
            ex.changed();
            let raw = ex.pid().raw() + 1;
            drop(ex);
            match full {
                // Named before the slot moves on: a reader that follows the link finds the page.
                Some(mut full) => {
                    full.page_mut().put_u64(NEXT, raw);
                    full.changed();
                }
                None => list.first.store(raw, Ordering::Release),
            }
            list.last.store(raw, Ordering::Release);
        }
    }

    /// Publishes the item [`append`](Self::append) wrote last for `slot`, of the transaction started
    /// at `start`: its write's put is done.
    pub fn publish(&self, slot: u16, start: Ts) {
        let list = &self.slots[slot as usize];
        list.last_start.store(start, Ordering::Relaxed);
        list.end.fetch_add(1, Ordering::Release);
    }

    /// Moves `slot`'s end back to `to`, past which every item is of the slot's running transaction
    /// and undone by its rollback: the pages wholly past `to` given back. Step it until done.
    pub fn cut_back(&self, slot: u16, to: u64) -> Result<IOResult<()>> {
        let list = &self.slots[slot as usize];
        let end = list.end.load(Ordering::Relaxed);
        if to > end {
            return Err(LimboError::InternalError(format!(
                "slot {slot}'s list cut back to {to}, past its end {end}"
            )));
        }
        list.end.store(to, Ordering::Release);
        loop {
            let Some(pid) = list
                .last
                .load(Ordering::Relaxed)
                .checked_sub(1)
                .map(PageId::from_raw)
            else {
                return Ok(IOResult::Done(()));
            };
            let page = done!(self.pool.fix_optimistic(pid, None));
            let Some((first, prev)) = page.read(|p| (p.u64_at(FIRST), p.u64_at(PREV))) else {
                return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
            };
            drop(page);
            if first <= to || prev == 0 {
                return Ok(IOResult::Done(()));
            }
            // Held across the free, so a free that returns IO has changed nothing.
            let mut before = done!(self.pool.fix_exclusive(PageId::from_raw(prev - 1), None));
            done!(self.pool.free_unreferenced(pid));
            before.page_mut().put_u64(NEXT, 0);
            before.changed();
            list.last.store(prev, Ordering::Release);
        }
    }

    /// The item at `reader`'s place in `slot`'s list, `None` at the published end. A page not in
    /// memory is read in: the call returns IO and is made again once the read lands. Step it until
    /// done.
    pub fn read(&self, slot: u16, reader: &mut Reader) -> Result<IOResult<Option<Item>>> {
        loop {
            match done!(self.look(slot, reader)) {
                Look::Item(item) => return Ok(IOResult::Done(Some(item))),
                Look::End => return Ok(IOResult::Done(None)),
                Look::Further => {}
            }
        }
    }

    /// One page of a read at `reader`'s place in `slot`'s list: the item there, the published
    /// end, or the reader moved on to the next page, the place lying past this one. It reads one
    /// page at most, none at the end (DESIGN.md 22.1). Step it until done.
    pub fn look(&self, slot: u16, reader: &mut Reader) -> Result<IOResult<Look>> {
        let list = &self.slots[slot as usize];
        if reader.at >= list.end.load(Ordering::Acquire) {
            return Ok(IOResult::Done(Look::End));
        }
        if reader.page == 0 {
            reader.page = list.first.load(Ordering::Acquire);
        }
        let Some(pid) = reader.page.checked_sub(1).map(PageId::from_raw) else {
            return Err(corrupt(slot, reader.at, "no page holds it"));
        };
        crate::race::point("gc.list_read");
        let page = done!(self.pool.fix_optimistic(pid, None));
        let at = reader.at;
        let Some((first, next, item)) = page.read(|p| {
            let first = p.u64_at(FIRST);
            let item = (first..first + PER_PAGE).contains(&at).then(|| {
                let from = ITEMS + ((at - first) as usize) * ITEM;
                p.copied(from..from + ITEM)
            });
            (first, p.u64_at(NEXT), item)
        }) else {
            // Written under its latch meanwhile, by an append past the end.
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        };
        if at < first {
            return Err(corrupt(slot, at, "its page was given back"));
        }
        let Some(bytes) = item else {
            reader.page = next;
            return Ok(IOResult::Done(Look::Further));
        };
        match Item::decode(&bytes) {
            Some(item) if item.position == at => Ok(IOResult::Done(Look::Item(item))),
            _ => Err(corrupt(slot, at, "its bytes are not its item")),
        }
    }

    /// Gives back `slot`'s pages wholly behind position `below`, never the page appends go to: for
    /// the holder of the slot's collector claim, `below` at or under every reader's place. How
    /// many. Step it until done.
    pub fn give_back(&self, slot: u16, below: u64) -> Result<IOResult<u64>> {
        let list = &self.slots[slot as usize];
        let below = below.min(list.kept.load(Ordering::Acquire));
        let mut given = 0;
        loop {
            let first = list.first.load(Ordering::Acquire);
            let Some(pid) = first.checked_sub(1).map(PageId::from_raw) else {
                return Ok(IOResult::Done(given));
            };
            if first == list.last.load(Ordering::Acquire) {
                return Ok(IOResult::Done(given));
            }
            let page = done!(self.pool.fix_optimistic(pid, None));
            let Some((from, next)) = page.read(|p| (p.u64_at(FIRST), p.u64_at(NEXT))) else {
                return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
            };
            drop(page);
            if from + PER_PAGE > below {
                return Ok(IOResult::Done(given));
            }
            if next == 0 {
                return Err(corrupt(
                    slot,
                    from,
                    "a page short of the last names no next",
                ));
            }
            done!(self.pool.free_unreferenced(pid));
            list.first.store(next, Ordering::Release);
            given += 1;
        }
    }

    /// The pages of `slot`'s list, first to last, for a test.
    #[cfg(test)]
    pub fn pages(&self, io: &Arc<dyn crate::io::IO>, slot: u16) -> Result<Vec<PageId>> {
        let mut out = Vec::new();
        let mut at = self.slots[slot as usize].first.load(Ordering::Acquire);
        while let Some(pid) = at.checked_sub(1).map(PageId::from_raw) {
            out.push(pid);
            let page = crate::storage::atoll::drive(io, || self.pool.fix_optimistic(pid, None))?;
            at = page
                .read(|p| p.u64_at(NEXT))
                .ok_or_else(|| corrupt(slot, 0, "a page not read whole"))?;
        }
        Ok(out)
    }
}

fn corrupt(slot: u16, at: u64, what: &str) -> LimboError {
    LimboError::Corrupt(format!("slot {slot}'s list at position {at}: {what}"))
}
