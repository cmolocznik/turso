//! The page store: pages written out of place, through the page table, into data files per size
//! class (`{base}.c{class}.{n}`) (DESIGN.md §6; audit 18).
//!
//! A page id names a page; a slot is a place in its class's data files. Every write of a page goes
//! to a fresh slot, so no write lands on a copy someone may still need. A page has up to two
//! copies on disk:
//!
//! - its **checkpoint copy**, which the page table names: the page as of the snapshot of the
//!   checkpoint that wrote it, settled ([`super::checkpoint`]);
//! - its **swap copy**, which only memory names: the page as it stood when it left the buffer
//!   pool while it differed from its checkpoint copy. A read takes the swap copy first.
//!
//! **Unlogged.** The page table and free space change in memory, and only checkpoints write them,
//! as they stood at the checkpoint's cut ([`super::meta`]). A page number taken or freed, a slot
//! taken: none is logged. A crash loses what memory held, and the checkpoint in charge is the
//! state again: swap slots and numbers taken since are free, numbers freed since are taken.
//!
//! **Frees wait.** A number freed while the changes of checkpoint `e` are made
//! ([`epoch`](PageStore::epoch)) is reused only once checkpoint `e` is durable: until then the
//! checkpoint in charge may name it, and a step may still hold it. A slot that holds a checkpoint
//! copy is freed once a durable checkpoint no longer names it, and is erased (a hole punched, or
//! zeros written) before it is taken again. A swap slot is freed once its page has another copy or
//! none, and once no step that may have looked it up is left.
//!
//! **Structure changes and the cut.** A change of several pages at once (a split, a merge, a
//! drain's step) enters the change gate ([`PageStore::change`]) before its first edit and leaves it
//! after its last; a checkpoint's cut closes the gate and waits for the changes inside, so each
//! change lies wholly before a cut or wholly after it (audit 18, review 4).
//!
//! **Retired pages** hold a version a snapshot may still read: the pages of an out-of-line value a
//! versioned write replaced (DESIGN.md §6.5). They are marked in memory only: a checkpoint maps a
//! value's pages only where an image it maps names the value.
//!
//! **Slots** are claimed in memory before they are taken: a swap copy's slot stays claimed, never
//! in the free space, so a crash frees it; a checkpoint copy's is marked in the free space, and the
//! checkpoint's own edit of its cut writes that.

use super::aligned::AlignedBlock;
use super::bits::GrowBits;
use super::epoch::{Epochs, Retired};
use super::meta::{MetaCache, MetaHold, MetaId, MetaRef, Tree, TREES};
use super::once::{SetOnce, TakeOnce};
use super::page::{self, PageId, SizeClass, CLASS_COUNT};
use super::radix::{self, ALLOCATED, FREE, SLOT_BASE};
use super::segmented::Segmented;
use super::shared::Writer;
use super::whole_write;
use crate::io::{Buffer, Completion, File, OpenFlags, IO};
use crate::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::{BTreeMap, HashMap, VecDeque};

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;

pub enum PageRead {
    /// A whole copy of the page.
    Valid(Arc<Buffer>),
    /// The page has no copy on disk, or its slot does not hold a whole copy of it.
    Invalid,
    Failed(CompletionError),
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Bytes per data file. The default of 1 TiB stays far below ext4's 16 TiB file limit and
    /// keeps a 100 TB database to about a hundred files.
    pub file_bytes: u64,
    /// Metadata pages held in memory before clean ones are dropped. The default of 65,536 (256 MiB)
    /// holds the page table of 64 TiB of 4 KiB pages.
    pub meta_pages: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            file_bytes: 1 << 40,
            meta_pages: 1 << 16,
        }
    }
}

/// A free that waits for a checkpoint ([`PageStore`]'s "frees wait").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deferred {
    /// A page number, reusable once the checkpoint is durable.
    Number(PageId),
    /// A slot holding a checkpoint copy, freed once the checkpoint, which no longer names it, is
    /// durable.
    Slot(SizeClass, u64),
}

/// Swap copies, sharded by page id: per page, its slot.
const SWAP_SHARDS: usize = 64;

/// In the change gate's word: a cut holds the gate, or waits for it.
const CUT: u64 = 1 << 63;

pub struct PageStore {
    io: Arc<dyn IO>,
    base: String,
    config: StoreConfig,
    meta: Arc<MetaCache>,
    /// Per class, its data files by number, each opened once.
    files: [Segmented<SetOnce<Arc<dyn File>>>; CLASS_COUNT],
    /// Levels of the page table (`[0]`) and free space (`[1]`) per class: written by the one
    /// growing a tree, and only rising.
    heights: [[AtomicU8; CLASS_COUNT]; TREES],
    /// Owned by the one growing a tree; another grower yields.
    growing: Mutex<()>,
    /// Where each class's next slot search starts: a hint.
    cursors: [AtomicU64; CLASS_COUNT],
    /// Where each class's next page number search starts: a hint, past the last number taken or
    /// at the lowest made reusable since.
    numbers: [AtomicU64; CLASS_COUNT],
    epochs: Arc<Epochs>,
    /// The checkpoint the changes made now belong to: the one after the checkpoint in charge, or
    /// after the one cut and not yet in charge.
    epoch: AtomicU64,
    /// Swap copies.
    swap: Box<[Mutex<HashMap<PageId, u64>>]>,
    /// Frees waiting for a checkpoint, by the checkpoint each waits for: each stays listed until
    /// it is done, so a free a yield interrupts is taken up where it was.
    deferred: Mutex<BTreeMap<u64, VecDeque<Deferred>>>,
    /// Numbers freed and not yet reusable, by class: no allocation takes one.
    unreusable: [GrowBits; CLASS_COUNT],
    /// Numbers whose wait for a checkpoint is over, waiting for every step that could hold them.
    freed_numbers: Retired<PageId>,
    /// Slots freed and not yet erased, by class: no allocation takes one.
    unerased: [GrowBits; CLASS_COUNT],
    /// Slots no durable checkpoint names, waiting for every step that may have looked them up.
    to_erase: Retired<(SizeClass, u64)>,
    /// Swap slots given up, waiting for every step that may have looked them up.
    swap_freed: Retired<(SizeClass, u64)>,
    /// The erasure in progress: owned by whoever steps it, and only ever tried.
    erasure: Mutex<Eraser>,
    /// Numbers kept for replay's tree creations, by class: no allocation takes one, and only
    /// [`allocate_at`](Self::allocate_at) does (audit 17).
    reserved: [GrowBits; CLASS_COUNT],
    /// Pages holding a version a snapshot may still read, by class.
    retired: [GrowBits; CLASS_COUNT],
    /// Slots claimed in memory, by class: a swap copy's for as long as it lives, a checkpoint
    /// copy's until the free space marks it. Every taker claims first, so no two take one slot.
    claimed: [GrowBits; CLASS_COUNT],
    /// Changes of several pages in flight, and [`CUT`] while a cut holds or wants the gate.
    gate: AtomicU64,
    /// Each tree a transaction created, by root, with its creator's slot and start: no
    /// checkpoint maps it before one's snapshot sees the creation (audit 18, review 8).
    creations: Mutex<HashMap<PageId, (u16, u64)>>,
    /// Pages no tree names any more whose free met a latched page: the checkpointer frees them,
    /// and leaves them out.
    orphans: Mutex<Vec<PageId>>,
}

/// The eraser's own: slots ready to erase, and the erasure in progress.
#[derive(Default)]
struct Eraser {
    ready: Vec<(SizeClass, u64)>,
    current: Option<Erasure>,
}

/// Freed slots being erased: holes punched or zeros written, then their files synced.
struct Erasure {
    slots: Vec<(SizeClass, u64)>,
    files: Vec<Arc<dyn File>>,
    /// The zero writes, then the syncs.
    pending: Vec<Completion>,
    synced: bool,
}

/// A change of several pages inside the change gate ([`PageStore::change`]): it leaves the gate
/// when dropped.
pub struct Change<'a> {
    gate: &'a AtomicU64,
}

impl Drop for Change<'_> {
    fn drop(&mut self) {
        self.gate.fetch_sub(1, Ordering::SeqCst);
    }
}

impl PageStore {
    /// A page store whose metadata is as checkpoint `in_charge` left it, its trees `heights` high.
    pub fn open(
        io: Arc<dyn IO>,
        base: &str,
        config: StoreConfig,
        heights: [[u8; CLASS_COUNT]; TREES],
        in_charge: u64,
    ) -> Arc<Self> {
        let epochs = Arc::new(Epochs::new());
        let meta = Arc::new(MetaCache::new(
            io.clone(),
            base,
            config.meta_pages,
            epochs.clone(),
            in_charge,
        ));
        Arc::new(Self {
            io,
            base: base.to_string(),
            config,
            meta,
            files: Default::default(),
            heights: heights.map(|h| h.map(|l| AtomicU8::new(l.max(1)))),
            growing: Mutex::new(()),
            cursors: Default::default(),
            numbers: Default::default(),
            epochs,
            epoch: AtomicU64::new(in_charge + 1),
            swap: (0..SWAP_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            deferred: Mutex::new(BTreeMap::new()),
            unreusable: Default::default(),
            freed_numbers: Retired::default(),
            unerased: Default::default(),
            to_erase: Retired::default(),
            swap_freed: Retired::default(),
            erasure: Mutex::new(Eraser::default()),
            reserved: Default::default(),
            retired: Default::default(),
            claimed: Default::default(),
            gate: AtomicU64::new(0),
            creations: Mutex::new(HashMap::new()),
            orphans: Mutex::new(Vec::new()),
        })
    }

    pub fn io(&self) -> &Arc<dyn IO> {
        &self.io
    }

    pub fn epochs(&self) -> &Arc<Epochs> {
        &self.epochs
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn meta(&self) -> &Arc<MetaCache> {
        &self.meta
    }

    /// The checkpoint the changes made now belong to.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Every tree's levels per class.
    pub fn heights(&self) -> [[u8; CLASS_COUNT]; TREES] {
        std::array::from_fn(|t| std::array::from_fn(|c| self.heights[t][c].load(Ordering::Acquire)))
    }

    fn height(&self, tree: Tree, class: SizeClass) -> u8 {
        self.heights[tree as usize][class.index()].load(Ordering::Acquire)
    }

    // ---------------------------------------------------------------- the change gate

    /// Enters the change gate for a change of several pages, before its first edit; `None` while
    /// a cut holds or wants the gate, when the change lets every latch go and yields.
    pub fn change(&self) -> Option<Change<'_>> {
        let mut word = self.gate.load(Ordering::SeqCst);
        loop {
            if word & CUT != 0 {
                return None;
            }
            match self.gate.compare_exchange_weak(
                word,
                word + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Some(Change { gate: &self.gate }),
                Err(now) => word = now,
            }
        }
    }

    /// Closes the gate for a cut: done once no change is inside. Step it until done, then
    /// [`open_gate`](Self::open_gate) after the cut.
    pub fn close_gate(&self) -> IOResult<()> {
        let word = self.gate.fetch_or(CUT, Ordering::SeqCst);
        if word & !CUT == 0 {
            IOResult::Done(())
        } else {
            busy()
        }
    }

    pub fn open_gate(&self) {
        self.gate.fetch_and(!CUT, Ordering::SeqCst);
    }

    // ---------------------------------------------------------------- files and reads

    fn slots_per_file(&self, class: SizeClass) -> u64 {
        (self.config.file_bytes / class.size() as u64).max(1)
    }

    /// The data file holding `slot` of `class`, and where in it the slot is.
    fn data_file(&self, class: SizeClass, slot: u64) -> Result<(Arc<dyn File>, u64)> {
        let per_file = self.slots_per_file(class);
        let (n, at) = (slot / per_file, (slot % per_file) * class.size() as u64);
        let cell = self.files[class.index()].get_or_make(n);
        if let Some(f) = cell.get() {
            return Ok((f, at));
        }
        let path = format!("{}.c{}.{n}", self.base, class.index());
        let file = self
            .io
            .open_file(&path, OpenFlags::Create | OpenFlags::Direct, false)?;
        // A checkpoint may name a slot in this file only once the file's creation is durable. Two
        // first users may both open it; the first to set it is kept.
        self.io.sync_directory(&directory(&self.base))?;
        Ok((cell.set(file), at))
    }

    /// The page table's entry for `pid` as it stands: [`FREE`], [`ALLOCATED`], or the slot of its
    /// checkpoint copy plus [`SLOT_BASE`].
    pub fn entry(self: &Arc<Self>, pid: PageId) -> Result<IOResult<u64>> {
        let class = pid.class();
        if pid.number() >= radix::capacity(Tree::Map, self.height(Tree::Map, class)) {
            return Ok(IOResult::Done(FREE));
        }
        let leaf = radix::path(Tree::Map, class, 1, pid.number())[0];
        Ok(match self.meta.get(leaf)? {
            IOResult::Done(f) => match f.read(|b| radix::map_entry(b, pid.number())) {
                Some(entry) => IOResult::Done(entry),
                None => busy(),
            },
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// The page-table leaf holding `pid`'s entry, which stays in the metadata cache while held, so
    /// every change to the entry reaches it; `None` if the page table does not reach `pid`, whose
    /// entry is then [`FREE`].
    pub fn leaf(self: &Arc<Self>, pid: PageId) -> Result<IOResult<Option<MetaRef>>> {
        let class = pid.class();
        if pid.number() >= radix::capacity(Tree::Map, self.height(Tree::Map, class)) {
            return Ok(IOResult::Done(None));
        }
        let leaf = radix::path(Tree::Map, class, 1, pid.number())[0];
        Ok(match self.meta.get(leaf)? {
            IOResult::Done(f) => IOResult::Done(Some(f)),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// Reads `slot` as a copy of `pid` into `into`, which is exactly the page's size and aligned
    /// for direct IO; `done` gets `into` once it holds a whole copy, `Invalid`, or the IO error.
    pub fn read_slot_into<F>(
        &self,
        pid: PageId,
        slot: u64,
        into: Arc<Buffer>,
        done: F,
    ) -> Result<Completion>
    where
        F: FnOnce(PageRead) + Send + 'static,
    {
        let class = pid.class();
        assert_eq!(
            into.len(),
            class.size(),
            "a read into a buffer not the page's size"
        );
        let (file, at) = self.data_file(class, slot)?;
        let done = TakeOnce::new(done);
        let keep = into.clone();
        let c = Completion::new_read(into, move |res| {
            let done = done.take()?;
            match res {
                Ok((_, n)) => {
                    let whole = n as usize == class.size() && page::verify(keep.as_slice(), pid);
                    done(if whole {
                        PageRead::Valid(keep.clone())
                    } else {
                        PageRead::Invalid
                    });
                    None
                }
                Err(e) => {
                    done(PageRead::Failed(e));
                    Some(e)
                }
            }
        });
        file.pread(at, c)
    }

    /// Writes `copy`, a whole page, to `slot` of `class`, not made durable: `landed` gets the
    /// outcome.
    pub fn write_slot<F>(
        &self,
        class: SizeClass,
        slot: u64,
        copy: Arc<Buffer>,
        landed: F,
    ) -> Result<Completion>
    where
        F: FnOnce(std::result::Result<(), CompletionError>) + Send + 'static,
    {
        let (file, at) = self.data_file(class, slot)?;
        let len = copy.len();
        let landed = TakeOnce::new(landed);
        let c = Completion::new_write(move |res| {
            if let Some(landed) = landed.take() {
                landed(match res {
                    Ok(n) if n as usize == len => Ok(()),
                    Ok(_) => Err(CompletionError::ShortWrite),
                    Err(e) => Err(e),
                });
            }
        });
        file.pwrite(at, copy, c)
    }

    // ---------------------------------------------------------------- swap copies

    fn swap_shard(&self, pid: PageId) -> &Mutex<HashMap<PageId, u64>> {
        let h = pid.raw().wrapping_mul(0x9e37_79b9_7f4a_7c15);
        &self.swap[(h >> 58) as usize % SWAP_SHARDS]
    }

    /// The slot of `pid`'s swap copy, if it has one: a read takes it before the checkpoint copy.
    pub fn swap_slot(&self, pid: PageId) -> Option<u64> {
        self.swap_shard(pid).lock().get(&pid).copied()
    }

    /// `slot`, which holds a whole copy of `pid` written since its frame changed last, is its swap
    /// copy now; the one it replaces is given up.
    pub fn set_swap(&self, pid: PageId, slot: u64) {
        if let Some(old) = self.swap_shard(pid).lock().insert(pid, slot) {
            self.give_up_swap(pid.class(), old);
        }
    }

    /// Every page with a swap copy: each differs from its checkpoint copy.
    pub fn swap_pages(&self) -> Vec<PageId> {
        self.swap
            .iter()
            .flat_map(|shard| shard.lock().keys().copied().collect::<Vec<_>>())
            .collect()
    }

    /// `pid` has no swap copy any more: its page is settled at a checkpoint, freed, or changed and
    /// held in memory again.
    pub fn drop_swap(&self, pid: PageId) {
        if let Some(old) = self.swap_shard(pid).lock().remove(&pid) {
            self.give_up_swap(pid.class(), old);
        }
    }

    /// A swap slot no copy needs: free once no step that may have looked it up is left.
    fn give_up_swap(&self, class: SizeClass, slot: u64) {
        self.swap_freed.retire(&self.epochs, (class, slot));
    }

    // ---------------------------------------------------------------- creations

    /// The tree rooted at `root` was created by the transaction of `slot` that started at
    /// `start`.
    pub fn created(&self, root: PageId, slot: u16, start: u64) {
        self.creations.lock().insert(root, (slot, start));
    }

    /// Every tree created and not yet seen by a durable checkpoint, with its creator.
    pub fn creations(&self) -> Vec<(PageId, u16, u64)> {
        self.creations
            .lock()
            .iter()
            .map(|(&root, &(slot, start))| (root, slot, start))
            .collect()
    }

    /// A durable checkpoint maps the tree rooted at `root`, or it is gone.
    pub fn forget_creation(&self, root: PageId) {
        self.creations.lock().remove(&root);
    }

    /// `pid` is named by nothing, and its free waits for the checkpointer.
    pub fn orphaned(&self, pid: PageId) {
        self.orphans.lock().push(pid);
    }

    /// The pages waiting to be freed.
    pub fn orphans(&self) -> Vec<PageId> {
        self.orphans.lock().clone()
    }

    /// Takes the pages waiting to be freed, for the checkpointer.
    pub fn take_orphans(&self) -> Vec<PageId> {
        std::mem::take(&mut *self.orphans.lock())
    }

    // ---------------------------------------------------------------- page numbers

    /// Makes reusable every number whose wait is over and that no step can hold any more.
    fn reclaim_numbers(&self) {
        for pid in self.freed_numbers.reclaim(&self.epochs) {
            self.unreusable[pid.class().index()].clear(pid.number());
            // The lowest free number first: the search goes back to it.
            self.numbers[pid.class().index()].fetch_min(pid.number(), Ordering::Relaxed);
        }
    }

    /// A new page of `class`: no copy until a checkpoint or a swap writes one. Step it until done.
    pub fn allocate(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<PageId>> {
        self.reclaim_numbers();
        loop {
            // Next fit: from past the last number taken, round to the start.
            let from = self.numbers[class.index()].load(Ordering::Relaxed);
            let number = match self.find_free(Tree::Map, class, from, false)? {
                IOResult::Done(Some(n)) => n,
                IOResult::Done(None) => match self.grow(Tree::Map, class)? {
                    IOResult::Done(()) => continue,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                },
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            match self.take_number(PageId::new(class, number))? {
                IOResult::Done(true) => {
                    self.numbers[class.index()].store(number + 1, Ordering::Relaxed);
                    return Ok(IOResult::Done(PageId::new(class, number)));
                }
                // Taken, or the tree grew, since the search: look again.
                IOResult::Done(false) => continue,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
    }

    /// Takes page `pid`, which must be free: replay's creation of a tree at the root its change
    /// names (audit 17). The page table grows to reach the number if it must. Step it until done.
    pub fn allocate_at(self: &Arc<Self>, pid: PageId) -> Result<IOResult<()>> {
        assert!(!pid.is_temporary(), "{pid:?} taken from the page table");
        self.reclaim_numbers();
        let class = pid.class();
        loop {
            if pid.number() >= radix::capacity(Tree::Map, self.height(Tree::Map, class)) {
                match self.grow(Tree::Map, class)? {
                    IOResult::Done(()) => continue,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                }
            }
            match self.take_number_at(pid, true)? {
                IOResult::Done(true) => return Ok(IOResult::Done(())),
                IOResult::Done(false) => {
                    return Err(LimboError::InternalError(format!(
                        "{pid:?} is taken, so no tree can be made there"
                    )))
                }
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
    }

    /// Takes `pid` if it is free and may be taken: whether it was.
    fn take_number(self: &Arc<Self>, pid: PageId) -> Result<IOResult<bool>> {
        self.take_number_at(pid, false)
    }

    /// Takes `pid` if it is free, and, unless `reserved_too`, not kept for replay: whether it was;
    /// `false` too if the tree grew since its height was read, for the caller to look again.
    fn take_number_at(self: &Arc<Self>, pid: PageId, reserved_too: bool) -> Result<IOResult<bool>> {
        let (class, number) = (pid.class(), pid.number());
        let height = self.height(Tree::Map, class);
        let ids = radix::path(Tree::Map, class, height, number);
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        debug_assert!(
            !pid.is_temporary(),
            "the page table reached temporary numbers"
        );
        let Some(mut locked) = Locked::new(&frames) else {
            return Ok(busy());
        };
        if self.height(Tree::Map, class) != height
            || radix::map_entry(locked.page_of(ids[0]), number) != FREE
            || self.unreusable[class.index()].get(number)
            || (!reserved_too && self.reserved[class.index()].get(number))
        {
            locked.release_unchanged();
            return Ok(IOResult::Done(false));
        }
        set_map(&mut locked, Tree::Map, number, FREE, ALLOCATED);
        locked.commit(&self.meta);
        Ok(IOResult::Done(true))
    }

    /// Keeps the numbers of `pids` from every allocation but [`allocate_at`](Self::allocate_at):
    /// the roots replay will make trees at, so that no node replay splits off takes one first
    /// (audit 17). [`release_reserved`](Self::release_reserved) gives them back.
    pub fn reserve(&self, pids: impl IntoIterator<Item = PageId>) {
        for pid in pids {
            self.reserved[pid.class().index()].set(pid.number());
        }
    }

    /// Gives back every number [`reserve`](Self::reserve) kept.
    pub fn release_reserved(&self) {
        for bits in &self.reserved {
            for n in bits.ones() {
                bits.clear(n);
            }
        }
    }

    /// Drops `pid`: its number is free, reusable once the changes of this checkpoint are durable;
    /// its checkpoint copy's slot is freed then too, and its swap copy goes now. Step it until
    /// done.
    pub fn free(self: &Arc<Self>, pid: PageId) -> Result<IOResult<()>> {
        let (class, number) = (pid.class(), pid.number());
        loop {
            let height = self.height(Tree::Map, class);
            let ids = radix::path(Tree::Map, class, height, number);
            let frames = match self.meta.get_all(&ids)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let Some(mut locked) = Locked::new(&frames) else {
                return Ok(busy());
            };
            if self.height(Tree::Map, class) != height {
                locked.release_unchanged();
                continue;
            }
            let entry = radix::map_entry(locked.page_of(ids[0]), number);
            if entry == FREE {
                locked.release_unchanged();
                return Err(LimboError::InternalError(format!(
                    "{pid:?} freed, but it is not taken"
                )));
            }
            // Marked before the entry is free, so no allocation takes the number between.
            self.unreusable[class.index()].set(number);
            set_map(&mut locked, Tree::Map, number, entry, FREE);
            let epoch = self.epoch();
            locked.commit(&self.meta);
            {
                let mut deferred = self.deferred.lock();
                let frees = deferred.entry(epoch).or_default();
                frees.push_back(Deferred::Number(pid));
                if let Some(slot) = entry.checked_sub(SLOT_BASE) {
                    frees.push_back(Deferred::Slot(class, slot));
                }
            }
            self.retired[class.index()].clear(number);
            self.drop_swap(pid);
            return Ok(IOResult::Done(()));
        }
    }

    /// Marks `pid` retired: a version a snapshot may still read is in it (DESIGN.md §6.5).
    pub fn retire(&self, pid: PageId) {
        self.retired[pid.class().index()].set(pid.number());
    }

    /// Takes the mark off `pid`: no snapshot reads it, or a rollback put its version back.
    pub fn reinstate(&self, pid: PageId) {
        self.retired[pid.class().index()].clear(pid.number());
    }

    pub fn retired(&self, pid: PageId) -> bool {
        self.retired[pid.class().index()].get(pid.number())
    }

    // ---------------------------------------------------------------- slots

    /// A free slot of `class`, claimed: in no free space, claimed by no one, not being erased.
    fn claim_slot(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<u64>> {
        for (c, slot) in self.swap_freed.reclaim(&self.epochs) {
            self.claimed[c.index()].clear(slot);
        }
        loop {
            let from = self.cursors[class.index()].load(Ordering::Relaxed);
            let slot = match self.find_free(Tree::Space, class, from, true)? {
                IOResult::Done(Some(s)) => s,
                IOResult::Done(None) => match self.grow(Tree::Space, class)? {
                    IOResult::Done(()) => continue,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                },
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            self.cursors[class.index()].store(slot + 1, Ordering::Relaxed);
            if self.claimed[class.index()].set(slot) {
                return Ok(IOResult::Done(slot));
            }
        }
    }

    /// A slot for a swap copy of a page of `class`: claimed in memory alone, so a crash frees it.
    /// [`set_swap`](Self::set_swap) names it once the copy is written. Step it until done.
    pub fn take_swap_slot(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<u64>> {
        self.claim_slot(class)
    }

    /// A slot claimed for a swap copy whose write failed: given up.
    pub fn untake_swap_slot(&self, class: SizeClass, slot: u64) {
        self.give_up_swap(class, slot);
    }

    /// A slot for a checkpoint copy of a page of `class`, in use in the free space as it stands:
    /// the checkpoint marks it in its cut too. Step it until done.
    pub fn take_copy_slot(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<u64>> {
        let slot = match self.claim_slot(class)? {
            IOResult::Done(s) => s,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        loop {
            let height = self.height(Tree::Space, class);
            let ids = radix::path(Tree::Space, class, height, slot);
            let frames = match self.meta.get_all(&ids)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let Some(mut locked) = Locked::new(&frames) else {
                return Ok(busy());
            };
            if self.height(Tree::Space, class) != height {
                locked.release_unchanged();
                continue;
            }
            set_space(&mut locked, class, slot, true);
            locked.commit(&self.meta);
            // The free space holds it now.
            self.claimed[class.index()].clear(slot);
            return Ok(IOResult::Done(slot));
        }
    }

    // ---------------------------------------------------------------- checkpoints

    /// The cut of checkpoint `n`, under a closed gate: the changes made from now on are checkpoint
    /// `n + 1`'s, and every metadata page keeps its state as of now for `n` to write. Returns the
    /// metadata pages dirty at the cut and every tree's height then.
    pub fn begin_cut(&self, n: u64) -> (Vec<MetaId>, [[u8; CLASS_COUNT]; TREES]) {
        let heights = self.heights();
        let dirty = self.meta.begin_cut(n);
        self.epoch.store(n + 1, Ordering::SeqCst);
        (dirty, heights)
    }

    /// Checkpoint `n`, cut at `heights`, wrote `pid`'s copy to `slot`: the page table names it as
    /// of the cut, and as it stands unless `pid` was freed since; the copy it replaces is freed
    /// as of the cut, and as it stands once `n` is durable. Returns the metadata pages edited.
    /// Step it until done.
    pub fn copied(
        self: &Arc<Self>,
        n: u64,
        heights: &[[u8; CLASS_COUNT]; TREES],
        pid: PageId,
        slot: u64,
    ) -> Result<IOResult<Vec<MetaId>>> {
        self.edit_cut(n, heights, pid, Some(slot))
    }

    /// Checkpoint `n`, cut at `heights`, leaves `pid` out: free as of the cut, and its copy's slot
    /// with it; as it stands, nothing changes. Returns the metadata pages edited. Step it until
    /// done.
    pub fn left_out(
        self: &Arc<Self>,
        n: u64,
        heights: &[[u8; CLASS_COUNT]; TREES],
        pid: PageId,
    ) -> Result<IOResult<Vec<MetaId>>> {
        self.edit_cut(n, heights, pid, None)
    }

    fn edit_cut(
        self: &Arc<Self>,
        n: u64,
        heights: &[[u8; CLASS_COUNT]; TREES],
        pid: PageId,
        slot: Option<u64>,
    ) -> Result<IOResult<Vec<MetaId>>> {
        let (class, number) = (pid.class(), pid.number());
        let cut_map = heights[Tree::Map as usize][class.index()];
        let cut_space = heights[Tree::Space as usize][class.index()];
        // The entry as of the cut names the slot to free; read first, then everything latched.
        let old = match self.cut_entry(n, cut_map, pid)? {
            IOResult::Done(e) => e,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let old_slot = old.checked_sub(SLOT_BASE);
        let live_map = self.height(Tree::Map, class);
        let mut ids = radix::path(Tree::Map, class, cut_map, number);
        ids.extend(radix::path(Tree::Map, class, live_map, number));
        for s in [slot, old_slot].into_iter().flatten() {
            ids.extend(radix::path(Tree::Space, class, cut_space, s));
        }
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let Some(mut locked) = Locked::new(&frames) else {
            return Ok(busy());
        };
        if self.height(Tree::Map, class) != live_map {
            locked.release_unchanged();
            return Ok(busy());
        }
        let path = radix::path(Tree::Map, class, cut_map, number);
        if locked.cut_of(path[0], |b| radix::map_entry(b, number)) != old {
            // A free since the read cannot change the cut: only this checkpoint edits it.
            locked.release_unchanged();
            return Err(LimboError::InternalError(format!(
                "{pid:?}'s entry as of the cut of checkpoint {n} moved under it"
            )));
        }
        let to = slot.map_or(FREE, |s| s + SLOT_BASE);
        set_map_cut(&mut locked, Tree::Map, class, cut_map, number, to);
        if let Some(s) = slot {
            set_space_cut(&mut locked, class, cut_space, s, true);
        }
        if let Some(o) = old_slot {
            set_space_cut(&mut locked, class, cut_space, o, false);
        }
        let live_path = radix::path(Tree::Map, class, live_map, number);
        let live = radix::map_entry(locked.page_of(live_path[0]), number);
        let mut deferred = self.deferred.lock();
        match (slot, live) {
            // Freed since the cut: the free waits for checkpoint n + 1, and so does the copy.
            (Some(s), FREE) => deferred
                .entry(n + 1)
                .or_default()
                .push_back(Deferred::Slot(class, s)),
            (Some(s), live) => {
                set_map(&mut locked, Tree::Map, number, live, s + SLOT_BASE);
                if let Some(o) = old_slot {
                    deferred
                        .entry(n)
                        .or_default()
                        .push_back(Deferred::Slot(class, o));
                }
            }
            // Left out: the page as it stands keeps its entry; a free of it later frees its slot.
            (None, _) => {}
        }
        drop(deferred);
        let edited = locked.ids.clone();
        locked.commit(&self.meta);
        Ok(IOResult::Done(edited))
    }

    /// `pid`'s page-table entry as of checkpoint `n`'s cut, at map height `height`.
    fn cut_entry(self: &Arc<Self>, n: u64, height: u8, pid: PageId) -> Result<IOResult<u64>> {
        let (class, number) = (pid.class(), pid.number());
        if number >= radix::capacity(Tree::Map, height) {
            return Ok(IOResult::Done(FREE));
        }
        let leaf = radix::path(Tree::Map, class, height, number)[0];
        let frame = match self.meta.get(leaf)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let read = frame.read_cut(
            n,
            |b| radix::map_entry(b, number),
            |b| radix::map_entry(b, number),
        );
        Ok(match read {
            Some(e) => IOResult::Done(e),
            None => busy(),
        })
    }

    /// Every slot whose free waits for a checkpoint: in use in the free space until then.
    pub fn deferred_slots(&self) -> std::collections::HashSet<(SizeClass, u64)> {
        self.deferred
            .lock()
            .values()
            .flatten()
            .filter_map(|&d| match d {
                Deferred::Slot(class, slot) => Some((class, slot)),
                Deferred::Number(_) => None,
            })
            .collect()
    }

    /// Checkpoint `n` is durable: every free that waited for it is done, its numbers reusable once
    /// no step holds them, its slots erased before they are taken again. Each free leaves the list
    /// only once it is done, so a yield leaves every one not yet done listed, to be taken up on
    /// the next call. Step it until done.
    pub fn after(self: &Arc<Self>, n: u64) -> Result<IOResult<()>> {
        loop {
            let next = {
                let deferred = self.deferred.lock();
                deferred
                    .iter()
                    .next()
                    .filter(|&(&e, _)| e <= n)
                    .map(|(&e, frees)| (e, *frees.front().expect("an empty list removed")))
            };
            let Some((e, d)) = next else {
                return Ok(IOResult::Done(()));
            };
            match d {
                Deferred::Number(pid) => self.freed_numbers.retire(&self.epochs, pid),
                Deferred::Slot(class, slot) => {
                    if let IOResult::IO(io) = self.free_slot(class, slot)? {
                        return Ok(IOResult::IO(io));
                    }
                }
            }
            // Only this call takes frees off the list; frees are added at the back.
            let mut deferred = self.deferred.lock();
            let frees = deferred
                .get_mut(&e)
                .expect("the list the free was read from");
            assert_eq!(frees.pop_front(), Some(d), "a free taken off by another");
            if frees.is_empty() {
                deferred.remove(&e);
            }
        }
    }

    /// `slot` holds no copy any durable checkpoint names: free in the free space as it stands,
    /// and erased before it is taken again.
    fn free_slot(self: &Arc<Self>, class: SizeClass, slot: u64) -> Result<IOResult<()>> {
        let height = self.height(Tree::Space, class);
        let ids = radix::path(Tree::Space, class, height, slot);
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let Some(mut locked) = Locked::new(&frames) else {
            return Ok(busy());
        };
        if self.height(Tree::Space, class) != height {
            locked.release_unchanged();
            return Ok(busy());
        }
        // Marked unerased before it is free, so no taker finds it between.
        self.unerased[class.index()].set(slot);
        set_space(&mut locked, class, slot, false);
        locked.commit(&self.meta);
        self.to_erase.retire(&self.epochs, (class, slot));
        Ok(IOResult::Done(()))
    }

    /// Syncs every data file opened: what a checkpoint wrote to them is durable once each is done.
    pub fn sync_data_files(&self) -> Result<Vec<Completion>> {
        let mut out = Vec::new();
        for class in &self.files {
            let mut files = Vec::new();
            class.each(|_, cell| {
                if let Some(f) = cell.get() {
                    files.push(f);
                }
            });
            for f in files {
                out.push(f.sync_data(Completion::new_sync(|_| {}))?);
            }
        }
        Ok(out)
    }

    // ---------------------------------------------------------------- erasing

    /// Erases every slot freed since a durable checkpoint stopped naming it and every step that
    /// may have looked it up has ended: holes punched, or zeros written where the file cannot
    /// punch them, then each file synced once; then the slots may be taken. Step it until done.
    pub fn erase_freed(&self) -> Result<IOResult<()>> {
        let Some(mut eraser) = self.erasure.try_lock() else {
            // Someone else is stepping it.
            return Ok(busy());
        };
        let eraser = &mut *eraser;
        loop {
            let Some(e) = eraser.current.as_mut() else {
                eraser.ready.extend(self.to_erase.reclaim(&self.epochs));
                if eraser.ready.is_empty() {
                    return Ok(IOResult::Done(()));
                }
                let slots = std::mem::take(&mut eraser.ready);
                eraser.current = Some(self.start_erasure(slots)?);
                continue;
            };
            if let Some(c) = e.pending.iter().find(|c| !c.finished()) {
                return Ok(IOResult::IO(IOCompletions(c.clone())));
            }
            if let Some(err) = e.pending.iter().find_map(|c| c.get_error()) {
                // Kept for the next try: a slot not erased is never taken.
                let failed = eraser.current.take().expect("an erasure");
                eraser.ready.extend(failed.slots);
                return Err(LimboError::CompletionError(err));
            }
            if !e.synced {
                e.synced = true;
                e.pending = e
                    .files
                    .iter()
                    .map(|f| f.sync_data(Completion::new_sync(|_| {})))
                    .collect::<Result<_>>()?;
                continue;
            }
            let done = eraser.current.take().expect("an erasure");
            for (class, slot) in done.slots {
                self.unerased[class.index()].clear(slot);
            }
            return Ok(IOResult::Done(()));
        }
    }

    /// Punches out `slots`, or starts writing zeros over each where its file cannot punch holes.
    fn start_erasure(&self, slots: Vec<(SizeClass, u64)>) -> Result<Erasure> {
        let mut files: BTreeMap<(SizeClass, u64), Arc<dyn File>> = BTreeMap::new();
        let mut pending = Vec::new();
        for &(class, slot) in &slots {
            let (file, at) = self.data_file(class, slot)?;
            if file.punch_hole(at as usize, class.size()).is_err() {
                let zeros = Arc::new(AlignedBlock::zeroed(class.size()));
                let (write, zeroed) = whole_write(class.size());
                drop(file.pwrite(at, zeros.region(0, class.size()), write)?);
                pending.push(zeroed);
            }
            files.insert((class, slot / self.slots_per_file(class)), file);
        }
        Ok(Erasure {
            slots,
            files: files.into_values().collect(),
            pending,
            synced: false,
        })
    }

    // ---------------------------------------------------------------- growth and search

    /// Adds a level to `tree` of `class`, the old root the new one's first child. Step it until
    /// done.
    fn grow(self: &Arc<Self>, tree: Tree, class: SizeClass) -> Result<IOResult<()>> {
        // Only tried, never waited for: it is ownership of the growth, held across its IO.
        let Some(_growing) = self.growing.try_lock() else {
            return Ok(busy());
        };
        let height = self.height(tree, class);
        let old_root = radix::root(tree, class, height);
        let new_root = radix::root(tree, class, height + 1);
        let frames = match self.meta.get_all(&[old_root, new_root])? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let Some(mut locked) = Locked::new(&frames) else {
            return Ok(busy());
        };
        if self.height(tree, class) != height {
            locked.release_unchanged();
            return Ok(IOResult::Done(()));
        }
        let old = locked.page_of(old_root);
        let used = if height == 1 {
            (0..radix::leaf_units(tree))
                .filter(|&u| radix::used(tree, old, u))
                .count() as u64
        } else {
            (0..radix::FANOUT).map(|c| radix::used_below(old, c)).sum()
        };
        radix::set_used_below(locked.page_of(new_root), 0, used);
        locked.commit(&self.meta);
        self.heights[tree as usize][class.index()].store(height + 1, Ordering::Release);
        Ok(IOResult::Done(()))
    }

    /// How many page ids of `class` are in use: the page table's root counts those below each of
    /// its children, or, a root that is a leaf, holds their entries.
    pub fn pages_in_use(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<u64>> {
        let height = self.height(Tree::Map, class);
        let root = radix::root(Tree::Map, class, height);
        Ok(match self.meta.get(root)? {
            IOResult::Done(f) => match f.read(|b| {
                if height == 1 {
                    (0..radix::MAP_ENTRIES)
                        .filter(|&n| radix::used(Tree::Map, b, n))
                        .count() as u64
                } else {
                    (0..radix::FANOUT).map(|c| radix::used_below(b, c)).sum()
                }
            }) {
                Some(n) => IOResult::Done(n),
                None => busy(),
            },
            IOResult::IO(io) => IOResult::IO(io),
        })
    }

    /// Walks one radix tree of one class for an integrity check (DESIGN.md §17, §12): every
    /// unit in use, in order, with its leaf's value, and every count read above the leaves.
    /// Only nodes a count says hold a unit in use are read, so a unit under a count of zero is
    /// not seen here: a page a tree reaches is looked up by [`entry`](Self::entry) as well. Step
    /// it until done.
    pub fn census(self: &Arc<Self>, op: &mut Census) -> Result<IOResult<()>> {
        let (tree, class) = (op.tree, op.class);
        let height = self.height(tree, class);
        let stack = op
            .stack
            .get_or_insert_with(|| vec![radix::root(tree, class, height)]);
        while let Some(&id) = stack.last() {
            let node = match self.meta.get(id)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let units = radix::leaf_units(tree);
            let read = node.read(|b| {
                if id.level == 0 {
                    let first = id.index * units;
                    let used: Vec<(u64, u64)> = (first..first + units)
                        .filter(|&u| radix::used(tree, b, u))
                        .map(|u| match tree {
                            Tree::Map => (u, radix::map_entry(b, u)),
                            Tree::Space => (u, ALLOCATED),
                        })
                        .collect();
                    (used, Vec::new())
                } else {
                    let counts: Vec<(u64, u64)> = (0..radix::FANOUT)
                        .map(|c| (c, radix::used_below(b, c)))
                        .filter(|&(_, n)| n > 0)
                        .collect();
                    (Vec::new(), counts)
                }
            });
            let Some((used, counts)) = read else {
                return Ok(busy());
            };
            stack.pop();
            op.units.extend(used);
            for &(c, n) in counts.iter().rev() {
                let child = MetaId {
                    tree,
                    class,
                    level: id.level - 1,
                    index: id.index * radix::FANOUT + c,
                };
                op.counts.push((child.level, child.index, n));
                stack.push(child);
            }
        }
        op.units.sort_unstable();
        Ok(IOResult::Done(()))
    }

    /// The first unit of `tree` at or after `from` free as it stands that may be taken: no slot
    /// claimed or not yet erased (with `skip_unerased`), no number freed and not yet reusable or
    /// kept for replay. Wraps to the start once.
    fn find_free(
        self: &Arc<Self>,
        tree: Tree,
        class: SizeClass,
        from: u64,
        skip_unerased: bool,
    ) -> Result<IOResult<Option<u64>>> {
        let height = self.height(tree, class);
        let mut from = from.min(radix::capacity(tree, height));
        let mut wrapped = from == 0;
        'search: loop {
            let mut level = height - 1;
            let mut index = 0u64;
            loop {
                let id = MetaId {
                    tree,
                    class,
                    level,
                    index,
                };
                let frame = match self.meta.get(id)? {
                    IOResult::Done(f) => f,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                };
                let node_first = index * radix::span(tree, level);
                if level == 0 {
                    let mut at = from.max(node_first);
                    let found = loop {
                        let Some(free) = frame.read(|b| radix::free_in_leaf(tree, b, index, at))
                        else {
                            return Ok(busy());
                        };
                        let Some(u) = free else {
                            break None;
                        };
                        if skip_unerased
                            && (self.unerased[class.index()].get(u)
                                || self.claimed[class.index()].get(u))
                        {
                            at = u + 1;
                            continue;
                        }
                        if tree == Tree::Map
                            && (self.unreusable[class.index()].get(u)
                                || self.reserved[class.index()].get(u))
                        {
                            at = u + 1;
                            continue;
                        }
                        break Some(u);
                    };
                    match found {
                        Some(u) => return Ok(IOResult::Done(Some(u))),
                        // Everything free here is still being erased: move past this leaf.
                        None => {
                            from = node_first + radix::leaf_units(tree);
                            if from >= radix::capacity(tree, height) {
                                if wrapped {
                                    return Ok(IOResult::Done(None));
                                }
                                wrapped = true;
                                from = 0;
                            }
                            continue 'search;
                        }
                    }
                }
                let from_child = if from > node_first {
                    radix::child(tree, level, from)
                } else {
                    0
                };
                let Some(room) = frame.read(|b| radix::child_with_room(tree, b, level, from_child))
                else {
                    return Ok(busy());
                };
                match room {
                    Some(c) => {
                        index = index * radix::FANOUT + c;
                        level -= 1;
                    }
                    None if !wrapped => {
                        wrapped = true;
                        from = 0;
                        continue 'search;
                    }
                    None => return Ok(IOResult::Done(None)),
                }
            }
        }
    }
}

fn busy<T>() -> IOResult<T> {
    IOResult::IO(IOCompletions(Completion::new_yield()))
}

/// The directory the files named from `base` are in.
pub(super) fn directory(base: &str) -> String {
    std::path::Path::new(base)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".".to_string())
}

/// Metadata pages latched together, in the order of their ids, or none of them.
struct Locked<'a> {
    ids: Vec<MetaId>,
    holds: Vec<MetaHold<'a>>,
    frames: Vec<&'a MetaRef>,
}

impl<'a> Locked<'a> {
    /// Latches every page of `frames`, or `None`, holding nothing, if one is held: the caller
    /// yields and tries again.
    fn new(frames: &'a [MetaRef]) -> Option<Self> {
        let mut order: Vec<&MetaRef> = frames.iter().collect();
        order.sort_by_key(|f| f.id);
        order.dedup_by_key(|f| f.id);
        let mut holds = Vec::with_capacity(order.len());
        for f in &order {
            match f.try_lock() {
                Some(hold) => holds.push(hold),
                None => {
                    // Nothing was written: readers are not sent back.
                    holds.into_iter().for_each(MetaHold::release_unchanged);
                    return None;
                }
            }
        }
        Some(Self {
            ids: order.iter().map(|f| f.id).collect(),
            holds,
            frames: order,
        })
    }

    /// Lets every latch go, nothing written.
    fn release_unchanged(self) {
        self.holds.into_iter().for_each(MetaHold::release_unchanged);
    }

    fn index(&self, id: MetaId) -> usize {
        self.ids
            .iter()
            .position(|&x| x == id)
            .unwrap_or_else(|| panic!("{id:?} is not latched"))
    }

    fn page_of(&mut self, id: MetaId) -> &mut Writer<'a> {
        let i = self.index(id);
        self.holds[i].page()
    }

    /// `f` over page `id` as it stood at the cut in effect, which a hold taken during a cut keeps.
    fn cut_of<R>(&mut self, id: MetaId, f: impl FnOnce(&mut [u8]) -> R) -> R {
        let i = self.index(id);
        self.holds[i]
            .with_cut(f)
            .unwrap_or_else(|| panic!("{id:?} has no copy as of the cut"))
    }

    /// Marks every page dirty and lets the latches go.
    fn commit(self, cache: &MetaCache) {
        for hold in self.holds {
            hold.commit(cache);
        }
    }
}

/// Sets `number`'s page-table entry from `from` to `to` as it stands, counting it in or out of use
/// above.
fn set_map(locked: &mut Locked<'_>, tree: Tree, number: u64, from: u64, to: u64) {
    let class = locked.frames[0].id.class;
    let height = locked
        .ids
        .iter()
        .filter(|id| id.tree == tree)
        .map(|id| id.level + 1)
        .max()
        .unwrap_or(1);
    let path = radix::path(tree, class, height, number);
    radix::set_map_entry(locked.page_of(path[0]), number, to);
    let delta = (to != FREE) as i64 - (from != FREE) as i64;
    for id in &path[1..] {
        if delta != 0 {
            radix::add_used_below(
                locked.page_of(*id),
                radix::child(tree, id.level, number),
                delta,
            );
        }
    }
}

/// Marks `slot` in use or free as it stands, counting it above.
fn set_space(locked: &mut Locked<'_>, class: SizeClass, slot: u64, used: bool) {
    let height = locked
        .ids
        .iter()
        .filter(|id| id.tree == Tree::Space && id.class == class)
        .map(|id| id.level + 1)
        .max()
        .unwrap_or(1);
    let path = radix::path(Tree::Space, class, height, slot);
    let leaf = locked.page_of(path[0]);
    let was = radix::space_bit(leaf, slot);
    assert_ne!(
        was,
        used,
        "slot {slot} of {class:?} is already {}",
        if used { "in use" } else { "free" }
    );
    radix::set_space_bit(leaf, slot, used);
    let delta = if used { 1 } else { -1 };
    for id in &path[1..] {
        radix::add_used_below(
            locked.page_of(*id),
            radix::child(Tree::Space, id.level, slot),
            delta,
        );
    }
}

/// Sets `number`'s entry in `tree` of `class`, `height` high, to `to` as it stood at the cut,
/// counting it in or out of use above.
fn set_map_cut(
    locked: &mut Locked<'_>,
    tree: Tree,
    class: SizeClass,
    height: u8,
    number: u64,
    to: u64,
) {
    let path = radix::path(tree, class, height, number);
    let from = locked.cut_of(path[0], |b| {
        let from = radix::map_entry(&*b, number);
        radix::set_map_entry(b, number, to);
        from
    });
    let delta = (to != FREE) as i64 - (from != FREE) as i64;
    if delta != 0 {
        for id in &path[1..] {
            let child = radix::child(tree, id.level, number);
            locked.cut_of(*id, |b| radix::add_used_below(b, child, delta));
        }
    }
}

/// Marks `slot` of `class` in use or free as it stood at the cut, the free space `height` high,
/// counting it above.
fn set_space_cut(locked: &mut Locked<'_>, class: SizeClass, height: u8, slot: u64, used: bool) {
    let path = radix::path(Tree::Space, class, height, slot);
    let was = locked.cut_of(path[0], |b| {
        let was = radix::space_bit(&*b, slot);
        radix::set_space_bit(b, slot, used);
        was
    });
    assert_ne!(
        was,
        used,
        "slot {slot} of {class:?} was already {} at the cut",
        if used { "in use" } else { "free" }
    );
    let delta = if used { 1 } else { -1 };
    for id in &path[1..] {
        let child = radix::child(Tree::Space, id.level, slot);
        locked.cut_of(*id, |b| radix::add_used_below(b, child, delta));
    }
}
pub struct Census {
    tree: Tree,
    class: SizeClass,
    /// The nodes still to read; `None` before the walk starts.
    stack: Option<Vec<MetaId>>,
    /// Each count read above the leaves: the level and index of the child it counts, and its count.
    counts: Vec<(u8, u64, u64)>,
    /// Every unit in use, in order, with its leaf's value: a page-table entry, or [`ALLOCATED`]
    /// for a bit in use.
    pub units: Vec<(u64, u64)>,
}

impl Census {
    pub fn new(tree: Tree, class: SizeClass) -> Self {
        Self {
            tree,
            class,
            stack: None,
            counts: Vec::new(),
            units: Vec::new(),
        }
    }

    /// Every count above the leaves that differs from the units in use below it, once walked.
    pub fn miscounts(&self) -> Vec<String> {
        self.counts
            .iter()
            .filter_map(|&(level, index, count)| {
                let span = radix::span(self.tree, level);
                let start = index * span;
                let below = self.units.partition_point(|&(u, _)| u < start);
                let held = self
                    .units
                    .partition_point(|&(u, _)| u < start.saturating_add(span))
                    - below;
                (held as u64 != count).then(|| {
                    format!(
                        "the {:?} of class {} counts {count} in use below node {index} of level \
                         {level}, which holds {held}",
                        self.tree,
                        self.class.index()
                    )
                })
            })
            .collect()
    }
}

/// Faults a test plants behind the store's back, for an integrity check to find.
#[cfg(test)]
impl PageStore {
    fn plant(
        self: &Arc<Self>,
        tree: Tree,
        class: SizeClass,
        unit: u64,
        change: impl FnOnce(&mut Locked<'_>, &[MetaId]),
    ) -> Result<IOResult<()>> {
        let ids = radix::path(tree, class, self.height(tree, class), unit);
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let Some(mut locked) = Locked::new(&frames) else {
            return Ok(busy());
        };
        change(&mut locked, &ids);
        locked.commit(&self.meta);
        Ok(IOResult::Done(()))
    }

    /// Sets `pid`'s page-table entry to `entry`, counted above.
    pub fn plant_entry(self: &Arc<Self>, pid: PageId, entry: u64) -> Result<IOResult<()>> {
        self.plant(Tree::Map, pid.class(), pid.number(), |locked, ids| {
            let from = radix::map_entry(locked.page_of(ids[0]), pid.number());
            set_map(locked, Tree::Map, pid.number(), from, entry);
        })
    }

    /// Marks `slot` of `class` in use or free, counted above.
    pub fn plant_slot(
        self: &Arc<Self>,
        class: SizeClass,
        slot: u64,
        used: bool,
    ) -> Result<IOResult<()>> {
        self.plant(Tree::Space, class, slot, |locked, _| {
            set_space(locked, class, slot, used)
        })
    }

    /// Sets the count the page table's level-1 node over `pid` keeps for the leaf holding it,
    /// leaving the leaf as it is: a miscount. The page table must have two levels at least.
    pub fn plant_miscount(self: &Arc<Self>, pid: PageId, count: u64) -> Result<IOResult<()>> {
        let number = pid.number();
        self.plant(Tree::Map, pid.class(), number, |locked, ids| {
            radix::set_used_below(
                locked.page_of(ids[1]),
                radix::child(Tree::Map, 1, number),
                count,
            );
        })
    }
}
