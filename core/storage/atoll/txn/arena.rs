//! Version arenas (DESIGN.md §11.2): each slot's record of what its transactions' writes replaced,
//! appended in order to temporary pages of its own.
//!
//! A write appends one entry before its leaf write: the statement, the tree's root, the key, and
//! the entry the leaf held, as a version index entry encodes it ([`super::versioned`]), or that it
//! held none. The version it writes names the entry in its header ([`Ptr`]), so a reader whose
//! snapshot does not see that version follows the pointer to the one below, with no search. A
//! transaction's entries lie in order between its first and its last: rollback walks them back
//! ([`super::rollback`]).
//!
//! Appending is one exclusive fix of the slot's page, the entry copied in and a new page taken when
//! it is full: no tree, no search. An entry is written only by its slot's transaction and never
//! changes once its version names it, so readers read it optimistically and need no latch.
//!
//! A page:
//!
//! ```text
//! page header (24) | prev page: u64 | next page: u64 | newest: u64 | last: u32 | end: u32
//!   | entries ...
//! ```
//!
//! `prev page` and `next page` are the slot's pages before and after this one, each its raw id plus
//! one, 0 for none; `newest` is the start of the last transaction that appended to it; `last` is
//! where the page's last entry starts, 0 for none; `end` is where the next one goes.
//!
//! **Collected by range** (DESIGN.md §11.5). A version's pointer is followed only by a reader whose
//! snapshot does not see that version, so an entry is needed only while some snapshot (a
//! checkpoint's cut among them) is below the commit of the transaction that appended it. A slot's
//! pages go oldest first: one whose newest transaction every snapshot sees, that is not the page
//! the slot appends to, is freed, under the slot's collector claim, a page a step. An entry is
//! 8-aligned:
//!
//! ```text
//! len: u32 | before: u32 | statement: u32 | key_len: u16 | replaced_len: u16 | root: u64 | key | replaced
//! ```
//!
//! `before` is where the page's previous entry starts, 0 for none: the one before is then the
//! previous page's last.

use super::{Statement, Ts, Txn};
use crate::storage::atoll::buffer::BufferPool;
use crate::storage::atoll::page::{self, PageId, PageKind, SizeClass};
use crate::storage::atoll::shared::{Bytes, BytesMut};
use crate::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// The arenas' page class: 64 KiB, so an entry of the largest node's longest key and largest
/// inline entry (each under a quarter of 64 KiB, `btree/node.rs`) fits in a page of its own.
pub const CLASS: SizeClass = match SizeClass::new(4) {
    Some(c) => c,
    None => panic!("no 64 KiB class"),
};

/// An arena page's kind.
pub const KIND: PageKind = PageKind(8);

const PREV: usize = page::HEADER_SIZE;
const NEXT: usize = PREV + 8;
const NEWEST: usize = NEXT + 8;
const LAST: usize = NEWEST + 8;
const END: usize = LAST + 4;
/// Where a page's entries start.
const ENTRIES: usize = END + 4;

/// Bytes of an entry before its key.
const ENTRY_HEAD: usize = 24;

/// Offsets are in units of this, the entries' alignment.
const ALIGN: usize = 8;
/// Bits a pointer gives the offset: a 64 KiB page in 8-byte units.
const OFFSET_BITS: u32 = 13;

/// Where an arena entry lies: its page, one of the arenas' temporary pages, and its offset there.
/// Never 0, which a version header keeps for none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ptr(u64);

impl Ptr {
    fn new(page: PageId, offset: usize) -> Self {
        debug_assert!(page.class() == CLASS && page.is_temporary());
        debug_assert!(offset.is_multiple_of(ALIGN) && offset < CLASS.size());
        let number = page.temporary_number();
        assert!(
            number < 1 << (64 - OFFSET_BITS - 1),
            "arena page {number} past what a pointer names"
        );
        Self(((number + 1) << OFFSET_BITS) | (offset / ALIGN) as u64)
    }

    /// The pointer `raw` stores, `None` for 0.
    pub fn from_raw(raw: u64) -> Option<Self> {
        (raw >> OFFSET_BITS != 0).then_some(Self(raw))
    }

    pub fn raw(self) -> u64 {
        self.0
    }

    fn page(self) -> PageId {
        PageId::temporary(CLASS, (self.0 >> OFFSET_BITS) - 1)
    }

    fn offset(self) -> usize {
        (self.0 & ((1 << OFFSET_BITS) - 1)) as usize * ALIGN
    }
}

/// An entry read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub statement: Statement,
    pub root: PageId,
    pub key: Vec<u8>,
    /// What the write replaced, as a version index entry encodes it.
    pub replaced: Vec<u8>,
}

#[cfg(test)]
crate::thread::thread_local! {
    /// Entries appended on this thread, for tests to count what a write appends.
    pub(crate) static APPENDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Arena pages taken on this thread, for tests to count what collection has to do.
    pub(crate) static PAGES_TAKEN: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Arena pages freed on this thread, for tests to count what a collection freed.
    pub(crate) static COLLECTED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Every slot's arena.
pub struct Arenas {
    pool: Arc<BufferPool>,
    /// Per slot, the page appends go to: its raw id plus one, 0 before the first. Written only by
    /// the slot's transaction, which owns it while it runs; released, as a collector reads it.
    pages: Box<[AtomicU64]>,
    /// Per slot, its oldest page not yet collected: raw id plus one, 0 before the first.
    oldest: Box<[AtomicU64]>,
    /// Per slot, its collector claim: taken within a step, never across a return.
    collecting: Box<[AtomicBool]>,
    /// Per slot, the pages it took since its collection's last step: twice as many is what the
    /// next step frees at most, the design's pacing target of twice the rate garbage arrives.
    taken: Box<[AtomicU64]>,
}

/// The entry at `ptr` in `pool`'s arena pages, for a reader with no [`Arenas`] of its own: the
/// checkpointer. Step it until done.
pub fn read(pool: &Arc<BufferPool>, ptr: Ptr) -> Result<IOResult<Entry>> {
    let (entry, _) = done!(read_with_before(pool, ptr));
    Ok(IOResult::Done(entry))
}

/// The entry at `ptr`, and where its page's entry before it starts, 0 for none.
fn read_with_before(pool: &Arc<BufferPool>, ptr: Ptr) -> Result<IOResult<(Entry, usize)>> {
    loop {
        let page = done!(pool.fix_optimistic(ptr.page(), None));
        // Changed meanwhile, by an append after it or an eviction: read it again.
        let Some(read) = page.read(|p| parse(p, ptr.offset())) else {
            continue;
        };
        return match read {
            Some(read) => Ok(IOResult::Done(read)),
            None => Err(LimboError::Corrupt(format!(
                "no arena entry at {} of {:?}",
                ptr.offset(),
                ptr.page()
            ))),
        };
    }
}

/// The entry at `at` of `page`, if it reads whole: `None` for bytes that do not hold together,
/// which only a change under an optimistic read (or corruption) leaves.
fn parse<P: Bytes + ?Sized>(page: &P, at: usize) -> Option<(Entry, usize)> {
    let size = page.len();
    if at < ENTRIES || at + ENTRY_HEAD > size {
        return None;
    }
    let len = page.u32_at(at) as usize;
    let before = page.u32_at(at + 4) as usize;
    let key_len = page.u16_at(at + 12) as usize;
    let replaced_len = page.u16_at(at + 14) as usize;
    if len != (ENTRY_HEAD + key_len + replaced_len).next_multiple_of(ALIGN) || at + len > size {
        return None;
    }
    let key_at = at + ENTRY_HEAD;
    Some((
        Entry {
            statement: page.u32_at(at + 8),
            root: PageId::from_raw(page.u64_at(at + 16)),
            key: page.copied(key_at..key_at + key_len),
            replaced: page.copied(key_at + key_len..key_at + key_len + replaced_len),
        },
        before,
    ))
}

impl Arenas {
    pub fn new(pool: Arc<BufferPool>, slots: usize) -> Self {
        Self {
            pool,
            pages: (0..slots).map(|_| AtomicU64::new(0)).collect(),
            oldest: (0..slots).map(|_| AtomicU64::new(0)).collect(),
            collecting: (0..slots).map(|_| AtomicBool::new(false)).collect(),
            taken: (0..slots).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Appends to `txn`'s slot's arena what the write of `key` in the tree rooted at `root` by
    /// `txn`'s current statement replaces, `replaced` as a version index entry encodes it.
    /// Returns where the entry lies. Step it until done; an IO handed back appended nothing.
    pub fn append(
        &self,
        txn: &mut Txn,
        root: PageId,
        key: &[u8],
        replaced: &[u8],
    ) -> Result<IOResult<Ptr>> {
        self.append_pieces(txn, root, key, &[replaced])
    }

    /// [`append`](Self::append), with `replaced` in pieces, copied in in order.
    pub fn append_pieces(
        &self,
        txn: &mut Txn,
        root: PageId,
        key: &[u8],
        pieces: &[&[u8]],
    ) -> Result<IOResult<Ptr>> {
        let replaced_len: usize = pieces.iter().map(|p| p.len()).sum();
        let len = (ENTRY_HEAD + key.len() + replaced_len).next_multiple_of(ALIGN);
        assert!(
            ENTRIES + len <= CLASS.size()
                && key.len() <= u16::MAX as usize
                && replaced_len <= u16::MAX as usize,
            "an arena entry of a {} byte key replacing {replaced_len} bytes",
            key.len(),
        );
        let cell = &self.pages[txn.slot as usize];
        #[cfg(test)]
        APPENDS.with(|n| n.set(n.get() + 1));
        loop {
            // Relaxed: the slot's transaction's own word.
            let current = cell
                .load(Ordering::Relaxed)
                .checked_sub(1)
                .map(PageId::from_raw);
            let mut full = None;
            if let Some(pid) = current {
                let mut ex = done!(self.pool.fix_exclusive(pid, None));
                let end = ex.page().u32_at(END) as usize;
                if end + len <= CLASS.size() {
                    let before = ex.page().u32_at(LAST);
                    let newest = ex.page().u64_at(NEWEST);
                    let mut w = ex.page_mut();
                    if txn.start > newest {
                        w.put_u64(NEWEST, txn.start);
                    }
                    let mut head = [0u8; ENTRY_HEAD];
                    head[0..4].copy_from_slice(&(len as u32).to_le_bytes());
                    head[4..8].copy_from_slice(&before.to_le_bytes());
                    head[8..12].copy_from_slice(&txn.command.to_le_bytes());
                    head[12..14].copy_from_slice(&(key.len() as u16).to_le_bytes());
                    head[14..16].copy_from_slice(&(replaced_len as u16).to_le_bytes());
                    head[16..24].copy_from_slice(&root.raw().to_le_bytes());
                    w.write(end, &head);
                    w.write(end + ENTRY_HEAD, key);
                    let mut at = end + ENTRY_HEAD + key.len();
                    for piece in pieces {
                        w.write(at, piece);
                        at += piece.len();
                    }
                    w.put_u32(LAST, end as u32);
                    w.put_u32(END, (end + len) as u32);
                    ex.changed();
                    let ptr = Ptr::new(pid, end);
                    txn.replaced = Some(match txn.replaced {
                        Some((first, _)) => (first, ptr),
                        None => (ptr, ptr),
                    });
                    return Ok(IOResult::Done(ptr));
                }
                // Held while the next page is taken, to name it.
                full = Some(ex);
            }
            // A page of its own, after the slot's last.
            let mut ex = done!(self.pool.allocate_temporary(CLASS));
            let prev = current.map_or(0, |p| p.raw() + 1);
            let mut w = ex.page_mut();
            page::set_kind(&mut w, KIND);
            w.put_u64(PREV, prev);
            w.put_u64(NEXT, 0);
            w.put_u64(NEWEST, 0);
            w.put_u32(LAST, 0);
            w.put_u32(END, ENTRIES as u32);
            ex.changed();
            let raw = ex.pid().raw() + 1;
            drop(ex);
            match full {
                // Named before the slot moves on: a collector that sees the move sees the name.
                Some(mut full) => {
                    full.page_mut().put_u64(NEXT, raw);
                    full.changed();
                }
                None => self.oldest[txn.slot as usize].store(raw, Ordering::Release),
            }
            self.taken[txn.slot as usize].fetch_add(1, Ordering::Relaxed);
            #[cfg(test)]
            PAGES_TAKEN.with(|n| n.set(n.get() + 1));
            cell.store(raw, Ordering::Release);
        }
    }

    /// Frees the slot's oldest pages while the transaction that last appended to each is one
    /// `seen` says every snapshot sees (its start given) and it is not the page the slot appends
    /// to: twice as many pages at most as the slot took since the last step, and one: how many it
    /// freed. Nothing waits: a step that would read a page in starts the read and stops there, and
    /// one that finds the slot being collected does nothing.
    pub fn collect(&self, slot: u16, seen: impl Fn(Ts) -> bool) -> Result<IOResult<u64>> {
        let slot = slot as usize;
        if self.collecting[slot]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(IOResult::Done(0));
        }
        struct Claim<'a>(&'a AtomicBool);
        impl Drop for Claim<'_> {
            fn drop(&mut self) {
                crate::storage::atoll::unwind::abort_if_unwinding();
                self.0.store(false, Ordering::Release);
            }
        }
        let _claim = Claim(&self.collecting[slot]);
        let allowance = 2 * self.taken[slot].swap(0, Ordering::Relaxed).max(1);
        let mut freed = 0;
        while freed < allowance {
            // The slot's page first: a page past it was not yet named when it was read.
            let current = self.pages[slot].load(Ordering::Acquire);
            let oldest = self.oldest[slot].load(Ordering::Acquire);
            if oldest == 0 || oldest == current {
                break;
            }
            let pid = PageId::from_raw(oldest - 1);
            let page = done!(self.pool.fix_optimistic(pid, None));
            let Some((next, newest)) = page.read(|p| (p.u64_at(NEXT), p.u64_at(NEWEST))) else {
                break;
            };
            drop(page);
            // Not linked yet as this collector reads it: nothing more to free now.
            if next == 0 || !seen(newest) {
                break;
            }
            done!(self.pool.free_unreferenced(pid));
            self.oldest[slot].store(next, Ordering::Release);
            freed += 1;
            #[cfg(test)]
            COLLECTED.with(|n| n.set(n.get() + 1));
        }
        Ok(IOResult::Done(freed))
    }

    /// The entry at `ptr`. Step it until done.
    pub fn read(&self, ptr: Ptr) -> Result<IOResult<Entry>> {
        read(&self.pool, ptr)
    }

    /// The entry at `ptr`, and where its page's entry before it starts, 0 for none.
    fn read_with_before(&self, ptr: Ptr) -> Result<IOResult<(Entry, usize)>> {
        read_with_before(&self.pool, ptr)
    }

    /// The entry before `ptr` in its slot's arena, `None` at the arena's first. Step it until
    /// done.
    pub fn before(&self, ptr: Ptr) -> Result<IOResult<Option<Ptr>>> {
        let (_, before) = done!(self.read_with_before(ptr));
        if before != 0 {
            return Ok(IOResult::Done(Some(Ptr::new(ptr.page(), before))));
        }
        loop {
            let page = done!(self.pool.fix_optimistic(ptr.page(), None));
            let Some(prev) = page.read(|p| p.u64_at(PREV)) else {
                continue;
            };
            let Some(prev) = prev.checked_sub(1).map(PageId::from_raw) else {
                return Ok(IOResult::Done(None));
            };
            let page = done!(self.pool.fix_optimistic(prev, None));
            let Some(last) = page.read(|p| p.u32_at(LAST) as usize) else {
                continue;
            };
            if last == 0 {
                return Err(LimboError::Corrupt(format!(
                    "the arena page {prev:?} before {:?} holds no entry",
                    ptr.page()
                )));
            }
            return Ok(IOResult::Done(Some(Ptr::new(prev, last))));
        }
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
