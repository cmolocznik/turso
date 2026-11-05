//! The page store: pages written out of place, through the page table, into data files per size
//! class (`{base}.c{class}.{n}`).
//!
//! A page id names a page; a slot is a place in its class's data files. Every write of a page goes
//! to a fresh slot, so no write ever lands on a copy someone may still need, and a crash at any
//! moment leaves a whole copy of every page. The life of a slot:
//!
//! 1. **Intent.** A batch of writebacks takes free slots and logs an intent naming them. The intent
//!    is durable before any of the writes starts, so recovery knows every slot a crash may have
//!    left a copy in.
//! 2. **Write.** Each copy is durable when its write completes.
//! 3. **Remap.** The page table points at the new slot, and the slot the page held before is freed
//!    in the same logged change.
//! 4. **Erase.** A freed slot is erased (a hole punched, or zeros written) once the change that
//!    freed it is durable, and only then may it be taken again. A checkpoint's redo point never
//!    passes a free not yet erased, so recovery replays every free it must finish erasing. Slots of
//!    intents that got no remap are released and erased by recovery.
//!
//! **Provisional pages.** A new page is provisional until a record adopts it: the record, or
//! group of records ([`Log::append_group`]), that makes it reachable. A page being dropped is
//! abandoned the same way, in the group that makes it unreachable, and freed afterwards. Recovery
//! frees every page still provisional, so a crash between allocating a page and linking it in, or
//! between unlinking it and freeing it, leaves nothing behind that nothing refers to. A
//! checkpoint's redo point never passes the record that made a page provisional while it is, so
//! recovery always sees it.
//!
//! **Retired pages** hold a version a snapshot may still read: the pages of an out-of-line value a
//! versioned write replaced (DESIGN.md §6.5). Each is marked retired by a record in the group that
//! replaces the value ([`PageStore::marks`]), and the compensation that puts the value back clears
//! the mark in its own group. Recovery frees every page still marked once undo is done
//! ([`PageStore::free_retired_pages`]), since no snapshot survives a crash.
//!
//! The page table, free space and retired marks are radix trees of metadata pages
//! ([`super::radix`], [`super::meta`]), changed only by the logged records below.

use super::aligned::AlignedBlock;
use super::bits::GrowBits;
use super::epoch::{Epochs, Retired};
use super::floors::{Floor, Floors};
use super::log::{framed_len, Admitted, Header, Kind, Log, Lsn, OwnedRecord};
use super::meta::{MetaCache, MetaHold, MetaId, MetaRef, Tree, TREES};
use super::once::{SetOnce, TakeOnce};
use super::page::{self, PageId, SizeClass, CLASS_COUNT};
use super::radix::{self, ALLOCATED, FREE, SLOT_BASE};
use super::segmented::Segmented;
use super::shared::{BytesMut, Writer};
use super::stack::Stack;
use super::{whole, whole_write};
use crate::io::{Buffer, Completion, File, OpenFlags, IO};
use crate::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use crate::sync::{Arc, Mutex, OnceLock};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::{BTreeMap, HashMap};

/// Log record kinds the page store owns.
pub mod kinds {
    use super::Kind;
    pub const NEW: Kind = Kind::new(1).unwrap();
    pub const FREE: Kind = Kind::new(2).unwrap();
    pub const INTENT: Kind = Kind::new(3).unwrap();
    pub const REMAP: Kind = Kind::new(4).unwrap();
    pub const RELEASE: Kind = Kind::new(5).unwrap();
    pub const GROW: Kind = Kind::new(6).unwrap();
    pub const ADOPT: Kind = Kind::new(7).unwrap();
    pub const ABANDON: Kind = Kind::new(8).unwrap();
    pub const RETIRE: Kind = Kind::new(23).unwrap();
    pub const REINSTATE: Kind = Kind::new(24).unwrap();

    pub fn owns(kind: Kind) -> bool {
        (1..=8).contains(&kind.get()) || kind == RETIRE || kind == REINSTATE
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreRecord {
    /// A page was created, with no copy yet. It is provisional until adopted.
    New { pid: PageId },
    /// These provisional pages are reachable now: logged in the group that links them in.
    Adopt { pids: Vec<PageId> },
    /// These pages are unreachable now and are freed next: logged in the group that unlinks them.
    Abandon { pids: Vec<PageId> },
    /// These pages, of one class, are retired: kept with their copies while a snapshot may read
    /// the version they hold. Logged in the group that replaces it ([`PageStore::marks`]).
    Retire { pids: Vec<PageId> },
    /// These retired pages, of one class, are reachable again: logged in the group that puts their
    /// value back.
    Reinstate { pids: Vec<PageId> },
    /// A page was dropped; its copy's slot, if it had one, is freed, and its retired mark cleared
    /// if it had one.
    Free {
        pid: PageId,
        old: Option<u64>,
        retired: bool,
    },
    /// These slots were taken for writes about to start.
    Intent { class: SizeClass, slots: Vec<u64> },
    /// A copy of `pid` landed in `new`; the slot it held before is freed.
    Remap {
        pid: PageId,
        new: u64,
        old: Option<u64>,
    },
    /// These slots were taken and are given back unused.
    Release { class: SizeClass, slots: Vec<u64> },
    /// A tree grew to `height` levels; `used` units are in use below the new root's first child.
    Grow {
        tree: Tree,
        class: SizeClass,
        height: u8,
        used: u64,
    },
}

const NONE: u64 = u64::MAX;

/// Slots freed by one record, and where that record ends.
type Freed = (Lsn, Vec<(SizeClass, u64)>);

/// The levels of a class's page table, free space and retired marks a record was logged under.
/// Recovery lays out the record's paths with them, not with the levels the trees have grown to
/// since.
pub type Heights = [u8; TREES];

impl StoreRecord {
    fn kind(&self) -> Kind {
        match self {
            Self::New { .. } => kinds::NEW,
            Self::Free { .. } => kinds::FREE,
            Self::Intent { .. } => kinds::INTENT,
            Self::Remap { .. } => kinds::REMAP,
            Self::Release { .. } => kinds::RELEASE,
            Self::Grow { .. } => kinds::GROW,
            Self::Adopt { .. } => kinds::ADOPT,
            Self::Abandon { .. } => kinds::ABANDON,
            Self::Retire { .. } => kinds::RETIRE,
            Self::Reinstate { .. } => kinds::REINSTATE,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let opt = |o: &Option<u64>| o.unwrap_or(NONE);
        match self {
            Self::New { pid } => out.extend(pid.raw().to_le_bytes()),
            Self::Adopt { pids }
            | Self::Abandon { pids }
            | Self::Retire { pids }
            | Self::Reinstate { pids } => {
                for p in pids {
                    out.extend(p.raw().to_le_bytes());
                }
            }
            Self::Free { pid, old, retired } => {
                out.extend(pid.raw().to_le_bytes());
                out.extend(opt(old).to_le_bytes());
                out.push(*retired as u8);
            }
            Self::Intent { class, slots } | Self::Release { class, slots } => {
                out.push(class.index() as u8);
                for s in slots {
                    out.extend(s.to_le_bytes());
                }
            }
            Self::Remap { pid, new, old } => {
                out.extend(pid.raw().to_le_bytes());
                out.extend(new.to_le_bytes());
                out.extend(opt(old).to_le_bytes());
            }
            Self::Grow {
                tree,
                class,
                height,
                used,
            } => {
                out.push(*tree as u8);
                out.push(class.index() as u8);
                out.push(*height);
                out.extend(used.to_le_bytes());
            }
        }
        out
    }

    pub fn decode(kind: Kind, payload: &[u8]) -> Option<Self> {
        let word = |at: usize| -> Option<u64> {
            Some(u64::from_le_bytes(
                payload.get(at..at + 8)?.try_into().ok()?,
            ))
        };
        let opt = |at: usize| word(at).map(|w| (w != NONE).then_some(w));
        let class = |at: usize| SizeClass::new(*payload.get(at)? as usize);
        let words = |from: usize| -> Option<Vec<u64>> {
            let rest = payload.get(from..)?;
            if rest.len() % 8 != 0 {
                return None;
            }
            Some(
                rest.chunks_exact(8)
                    .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                    .collect(),
            )
        };
        let slots = || words(1);
        let pids = || -> Option<Vec<PageId>> {
            let pids: Vec<PageId> = words(0)?.into_iter().map(PageId::from_raw).collect();
            (!pids.is_empty()).then_some(pids)
        };
        Some(match kind {
            k if k == kinds::NEW => Self::New {
                pid: PageId::from_raw(word(0)?),
            },
            k if k == kinds::FREE => Self::Free {
                pid: PageId::from_raw(word(0)?),
                old: opt(8)?,
                retired: match payload.get(16..)? {
                    [0] => false,
                    [1] => true,
                    _ => return None,
                },
            },
            k if k == kinds::INTENT => Self::Intent {
                class: class(0)?,
                slots: slots()?,
            },
            k if k == kinds::REMAP => Self::Remap {
                pid: PageId::from_raw(word(0)?),
                new: word(8)?,
                old: opt(16)?,
            },
            k if k == kinds::RELEASE => Self::Release {
                class: class(0)?,
                slots: slots()?,
            },
            k if k == kinds::ADOPT => Self::Adopt { pids: pids()? },
            k if k == kinds::ABANDON => Self::Abandon { pids: pids()? },
            k if k == kinds::RETIRE || k == kinds::REINSTATE => {
                let pids = pids()?;
                // One class: the levels logged with it are that class's.
                if pids.iter().any(|p| p.class() != pids[0].class()) {
                    return None;
                }
                if k == kinds::RETIRE {
                    Self::Retire { pids }
                } else {
                    Self::Reinstate { pids }
                }
            }
            k if k == kinds::GROW => Self::Grow {
                tree: match *payload.first()? {
                    0 => Tree::Map,
                    1 => Tree::Space,
                    2 => Tree::Retired,
                    _ => return None,
                },
                class: class(1)?,
                height: *payload.get(2)?,
                used: word(3)?,
            },
            _ => return None,
        })
    }

    /// The record's payload in the log: the levels it was logged under, then its fields.
    fn logged(&self, heights: Heights) -> Vec<u8> {
        let mut out = heights.to_vec();
        out.extend(self.encode());
        out
    }

    /// A payload from the log: the record and the levels it was logged under.
    pub fn decode_logged(kind: Kind, payload: &[u8]) -> Option<(Self, Heights)> {
        let heights: Heights = payload.get(..TREES)?.try_into().ok()?;
        Some((Self::decode(kind, &payload[TREES..])?, heights))
    }

    fn framed_len(&self) -> u64 {
        framed_len(TREES + self.encode().len())
    }
}

/// What a read of a page's copy found.
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

pub struct PageStore {
    io: Arc<dyn IO>,
    base: String,
    config: StoreConfig,
    /// Set once: at creation, or by recovery once it has replayed the page table and found where
    /// the log ends.
    log: OnceLock<Arc<Log>>,
    meta: Arc<MetaCache>,
    /// Per class, its data files by number, each opened once.
    files: [Segmented<SetOnce<Arc<dyn File>>>; CLASS_COUNT],
    /// Levels of the page table (`[0]`), free space (`[1]`) and retired marks (`[2]`) per class:
    /// written by the one growing a tree, or by recovery, and only rising.
    heights: [[AtomicU8; CLASS_COUNT]; TREES],
    /// Owned by the one growing a tree; another grower yields.
    growing: Mutex<()>,
    /// Where each class's next slot search starts: a hint.
    cursors: [AtomicU64; CLASS_COUNT],
    /// Slots freed and not yet erased, by where the record that freed them starts, with where it
    /// ends.
    /// Slots freed, waiting for the eraser to take them (see [`Eraser`]).
    freed: Stack<FreedSlot>,
    /// What each slot freed and not yet erased holds the redo bound at: where its free starts.
    frees: Floors,
    /// Where each intent whose slots are not all remapped or given back yet starts: held by its
    /// write plan, which counts what is left.
    intents: Floors,
    /// Write plans holding pages of each class's free space; free space grows only when none does.
    plans: [AtomicU32; CLASS_COUNT],
    /// The same slots, by class, which no allocation may take.
    unerased: [GrowBits; CLASS_COUNT],
    /// Only frees whose record ends at or before this are erased. After a crash it is where the
    /// log restarted, until a checkpoint past the restart is durable: a recovery from before that
    /// checkpoint cannot read what was appended since, and could still need the copies it freed.
    erase_limit: AtomicU64,
    /// While recovering, per class: slots of intents not yet matched by a remap or a release.
    pending_intents: [GrowBits; CLASS_COUNT],
    /// Provisional pages: where the group that made each provisional starts, and where the
    /// record that settled it (adopted or freed it) ends, once one did. A settled page stays
    /// until that record is durable: a crash before then still finds it provisional.
    provisional: Provisionals,
    /// Provisional pages their owner gave up on, waiting to be freed ([`take_orphans`]).
    ///
    /// [`take_orphans`]: Self::take_orphans
    orphans: Stack<PageId>,
    /// The erasure in progress, if any.
    /// The eraser: its slots freed and not yet erased, and the erasure in progress, if any.
    /// Only tried, never waited for: it is ownership of the erasing, held across its IO.
    erasure: Mutex<Eraser>,
    epochs: Arc<Epochs>,
    /// Page numbers freed and not yet reusable: a step that could hold one may still run.
    freed_numbers: Retired<PageId>,
    /// The same numbers, by class, which no allocation may take.
    unreusable: [GrowBits; CLASS_COUNT],
}

/// A slot freed, on its way to the eraser.
struct FreedSlot {
    /// Where the record that freed it starts and ends.
    start: Lsn,
    end: Lsn,
    class: SizeClass,
    slot: u64,
    floor: Floor,
}

/// The eraser's own: the slots freed and not yet erased, by where the record that freed them
/// starts, with where it ends, each holding its floor; and the erasure in progress.
#[derive(Default)]
struct Eraser {
    erasing: BTreeMap<Lsn, Freed>,
    floors: HashMap<(SizeClass, u64), Floor>,
    current: Option<Erasure>,
}

impl Eraser {
    /// Takes every slot freed since.
    fn take_freed(&mut self, freed: &Stack<FreedSlot>) {
        for f in freed.take_all() {
            self.erasing
                .entry(f.start)
                .or_insert((f.end, Vec::new()))
                .1
                .push((f.class, f.slot));
            self.floors.insert((f.class, f.slot), f.floor);
        }
    }
}

/// Freed slots being erased: holes punched or zeros written, then their files synced.
struct Erasure {
    ready: Vec<(Lsn, Freed)>,
    files: Vec<Arc<dyn File>>,
    /// The zero writes, then the syncs.
    pending: Vec<Completion>,
    synced: bool,
}

impl PageStore {
    /// A page store over `log`, its metadata trees as the superblock in charge left them
    /// (`heights`), or new.
    pub fn new(
        io: Arc<dyn IO>,
        base: &str,
        config: StoreConfig,
        log: Arc<Log>,
        heights: [[u8; CLASS_COUNT]; TREES],
    ) -> Arc<Self> {
        let store = Self::recovering(io, base, config, heights);
        store.end_recovery();
        assert!(store.log.set(log).is_ok());
        store
    }

    /// A page store to recover, its metadata trees as the superblock in charge left them. Its
    /// records are replayed with [`redo`](Self::redo) before the log is reopened, since only the
    /// replay finds where the log ends; then [`resume`](Self::resume) gives it the log.
    /// Until [`end_recovery`](Self::end_recovery), a metadata page with no whole copy reads as
    /// zeros: every change to it is in the log being replayed.
    pub fn recovering(
        io: Arc<dyn IO>,
        base: &str,
        config: StoreConfig,
        heights: [[u8; CLASS_COUNT]; TREES],
    ) -> Arc<Self> {
        let epochs = Arc::new(Epochs::new());
        let meta = Arc::new(MetaCache::new(
            io.clone(),
            base,
            config.meta_pages,
            epochs.clone(),
        ));
        meta.set_recovering(true);
        Arc::new(Self {
            io,
            base: base.to_string(),
            config,
            log: OnceLock::new(),
            meta,
            files: Default::default(),
            heights: heights.map(|h| h.map(|l| AtomicU8::new(l.max(1)))),
            growing: Mutex::new(()),
            cursors: Default::default(),
            freed: Stack::default(),
            frees: Floors::new(epochs.clone()),
            intents: Floors::new(epochs.clone()),
            plans: Default::default(),
            unerased: Default::default(),
            erase_limit: AtomicU64::new(u64::MAX),
            pending_intents: Default::default(),
            provisional: Provisionals::new(epochs.clone()),
            orphans: Stack::default(),
            erasure: Mutex::new(Eraser::default()),
            epochs,
            freed_numbers: Retired::default(),
            unreusable: Default::default(),
        })
    }

    pub fn io(&self) -> &Arc<dyn IO> {
        &self.io
    }

    /// The epochs every step on this database's pages announces in.
    pub fn epochs(&self) -> &Arc<Epochs> {
        &self.epochs
    }

    /// The path every file of the database is named after.
    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn meta(&self) -> &Arc<MetaCache> {
        &self.meta
    }

    pub fn log(&self) -> &Arc<Log> {
        self.log
            .get()
            .expect("the page store's log is given before anything is appended")
    }

    pub fn heights(&self) -> [[u8; CLASS_COUNT]; TREES] {
        std::array::from_fn(|t| std::array::from_fn(|c| self.heights[t][c].load(Ordering::Acquire)))
    }

    fn class_heights(&self, class: SizeClass) -> Heights {
        [
            self.height(Tree::Map, class),
            self.height(Tree::Space, class),
            self.height(Tree::Retired, class),
        ]
    }

    fn height(&self, tree: Tree, class: SizeClass) -> u8 {
        self.heights[tree as usize][class.index()].load(Ordering::Acquire)
    }

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
        // A remap may name a slot in this file only once the file's creation is durable. Two first
        // users may both open it; the first to set it is kept.
        self.io.sync_directory(&directory(&self.base))?;
        Ok((cell.set(file), at))
    }

    // ---------------------------------------------------------------- reading

    /// The page table's entry for `pid`: [`FREE`], [`ALLOCATED`], or a slot plus [`SLOT_BASE`].
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

    /// The slot holding `pid`'s current copy, if it has one.
    pub fn location(self: &Arc<Self>, pid: PageId) -> Result<IOResult<Option<u64>>> {
        Ok(match self.entry(pid)? {
            IOResult::Done(e) => IOResult::Done(e.checked_sub(SLOT_BASE)),
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

    /// Reads `slot` as a copy of `pid`; `done` gets the copy, `Invalid`, or the IO error.
    pub fn read_slot<F>(&self, pid: PageId, slot: u64, done: F) -> Result<Completion>
    where
        F: FnOnce(PageRead) + Send + 'static,
    {
        let size = pid.class().size();
        let block = Arc::new(AlignedBlock::zeroed(size));
        self.read_slot_into(pid, slot, block.region(0, size), done)
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

    // ---------------------------------------------------------------- page numbers

    /// A new page of `class`, logged; it has no copy until its first write, and is provisional
    /// until adopted ([`adopt`](Self::adopt), or [`StoreRecord::Adopt`] in a group).
    pub fn allocate(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<PageId>> {
        for pid in self.freed_numbers.reclaim(&self.epochs) {
            self.unreusable[pid.class().index()].clear(pid.number());
        }
        let record_len = StoreRecord::New {
            pid: PageId::new(class, 0),
        }
        .framed_len();
        let mut admitted = match self.log().admit(record_len)? {
            IOResult::Done(a) => a,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        loop {
            let number = match self.find_free(Tree::Map, class, 0, false)? {
                IOResult::Done(Some(n)) => n,
                IOResult::Done(None) => match self.grow(Tree::Map, class)? {
                    IOResult::Done(()) => continue,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                },
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let height = self.height(Tree::Map, class);
            let ids = radix::path(Tree::Map, class, height, number);
            let frames = match self.meta.get_all(&ids)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let pid = PageId::new(class, number);
            debug_assert!(
                !pid.is_temporary(),
                "the page table reached temporary numbers"
            );
            let Some(mut locked) = Locked::new(&frames) else {
                return Ok(busy());
            };
            let leaf = ids[0];
            if self.height(Tree::Map, class) != height
                || radix::map_entry(locked.page_of(leaf), number) != FREE
                || self.unreusable[class.index()].get(pid.number())
            {
                // Taken, or the tree grew, since the search: look again.
                continue;
            }
            let heights = [
                height,
                self.height(Tree::Space, class),
                self.height(Tree::Retired, class),
            ];
            let (start, lsn) = self.append(&mut admitted, &StoreRecord::New { pid }, heights);
            set_map(&mut locked, Tree::Map, number, FREE, ALLOCATED);
            locked.commit(&self.meta, start, lsn);
            self.made_provisional(pid, start);
            return Ok(IOResult::Done(pid));
        }
    }

    /// Drops `pid`: its number becomes free and its copy's slot, if any, is freed.
    pub fn free(self: &Arc<Self>, pid: PageId) -> Result<IOResult<()>> {
        self.free_page(pid, false)
    }

    /// Drops `pid`, which is retired, clearing its mark in the same record.
    pub fn free_retired(self: &Arc<Self>, pid: PageId) -> Result<IOResult<()>> {
        self.free_page(pid, true)
    }

    fn free_page(self: &Arc<Self>, pid: PageId, retired: bool) -> Result<IOResult<()>> {
        let class = pid.class();
        let record_len = StoreRecord::Free {
            pid,
            old: None,
            retired,
        }
        .framed_len();
        let mut admitted = match self.log().admit(record_len)? {
            IOResult::Done(a) => a,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let entry = match self.entry(pid)? {
            IOResult::Done(e) => e,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        assert_ne!(entry, FREE, "freeing {pid:?}, which is not allocated");
        let old = entry.checked_sub(SLOT_BASE);
        let heights = self.heights();
        let mut ids = radix::path(
            Tree::Map,
            class,
            self.height(Tree::Map, class),
            pid.number(),
        );
        if let Some(old) = old {
            ids.extend(radix::path(
                Tree::Space,
                class,
                self.height(Tree::Space, class),
                old,
            ));
        }
        if retired {
            ids.extend(radix::path(
                Tree::Retired,
                class,
                self.height(Tree::Retired, class),
                pid.number(),
            ));
        }
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let Some(mut locked) = Locked::new(&frames) else {
            return Ok(busy());
        };
        if self.heights() != heights {
            // A tree grew since the paths were taken.
            drop(locked);
            return self.free_page(pid, retired);
        }
        let leaf = radix::path(Tree::Map, class, 1, pid.number())[0];
        let current = radix::map_entry(locked.page_of(leaf), pid.number());
        assert_eq!(current, entry, "{pid:?} changed while being freed");
        let logged = [
            heights[Tree::Map as usize][class.index()],
            heights[Tree::Space as usize][class.index()],
            heights[Tree::Retired as usize][class.index()],
        ];
        // Waiting before it reads as free, so no allocation takes it in between.
        self.unreusable[class.index()].set(pid.number());
        let record = StoreRecord::Free { pid, old, retired };
        let (start, lsn) = self.append(&mut admitted, &record, logged);
        set_map(&mut locked, Tree::Map, pid.number(), entry, FREE);
        if let Some(old) = old {
            set_space(&mut locked, class, old, false);
        }
        if retired {
            set_mark(&mut locked, class, pid.number(), false);
        }
        locked.commit(&self.meta, start, lsn);
        if let Some(old) = old {
            self.freed((start, lsn), class, old);
        }
        self.settled(pid, lsn);
        // Its number waits for every step that could still hold it.
        self.freed_numbers.retire(&self.epochs, pid);
        Ok(IOResult::Done(()))
    }

    // ---------------------------------------------------------------- retired pages

    /// The marks of `retire` and `reinstate` to change in one group another owner appends
    /// (DESIGN.md §6.5): each class's retired marks grown to hold its pages, and the metadata
    /// pages they change held in memory. Made before the owner takes its latches, latched with
    /// [`Marks::lock`] while it holds them, and changed with [`LockedMarks::commit`] once its
    /// group is appended. Step it until done.
    pub fn marks(
        self: &Arc<Self>,
        retire: &[PageId],
        reinstate: &[PageId],
    ) -> Result<IOResult<Marks>> {
        for &pid in retire.iter().chain(reinstate) {
            let class = pid.class();
            while pid.number() >= radix::capacity(Tree::Retired, self.height(Tree::Retired, class))
            {
                match self.grow(Tree::Retired, class)? {
                    IOResult::Done(()) => {}
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                }
            }
        }
        let mut records = Vec::new();
        for class in SizeClass::all() {
            let of = |pids: &[PageId]| -> Vec<PageId> {
                pids.iter()
                    .copied()
                    .filter(|p| p.class() == class)
                    .collect()
            };
            let heights = self.class_heights(class);
            let (retire, reinstate) = (of(retire), of(reinstate));
            if !retire.is_empty() {
                records.push((StoreRecord::Retire { pids: retire }, heights));
            }
            if !reinstate.is_empty() {
                records.push((StoreRecord::Reinstate { pids: reinstate }, heights));
            }
        }
        let ids: Vec<MetaId> = records
            .iter()
            .flat_map(|(r, h)| record_pages(r, *h))
            .collect();
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        Ok(IOResult::Done(Marks { records, frames }))
    }

    /// Frees every page still marked retired, each through `free`, which clears its mark as it
    /// frees it (DESIGN.md §12, step 6): recovery's, once undo is done, since no snapshot survives
    /// a crash to read the versions they held. Recovery frees them through the buffer pool
    /// ([`BufferPool::free_retired`](super::buffer::BufferPool::free_retired)), since redo may have
    /// put one back in a frame. Step it until done.
    pub fn free_retired_pages(
        self: &Arc<Self>,
        mut free: impl FnMut(PageId) -> Result<IOResult<()>>,
    ) -> Result<IOResult<()>> {
        for class in SizeClass::all() {
            loop {
                let number = match self.find_used(Tree::Retired, class, 0)? {
                    IOResult::Done(Some(n)) => n,
                    IOResult::Done(None) => break,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                };
                match free(PageId::new(class, number))? {
                    IOResult::Done(()) => {}
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                }
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Whether `pid` is marked retired.
    #[cfg(test)]
    pub fn retired(self: &Arc<Self>, pid: PageId) -> Result<IOResult<bool>> {
        let class = pid.class();
        if pid.number() >= radix::capacity(Tree::Retired, self.height(Tree::Retired, class)) {
            return Ok(IOResult::Done(false));
        }
        let leaf = radix::path(Tree::Retired, class, 1, pid.number())[0];
        let frame = match self.meta.get(leaf)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        match frame.read(|b| radix::space_bit(b, pid.number())) {
            Some(retired) => Ok(IOResult::Done(retired)),
            None => Ok(busy()),
        }
    }

    /// The first unit of `tree` in use at or after `from`, or `None`.
    fn find_used(
        self: &Arc<Self>,
        tree: Tree,
        class: SizeClass,
        from: u64,
    ) -> Result<IOResult<Option<u64>>> {
        let height = self.height(tree, class);
        let mut from = from;
        'search: loop {
            if from >= radix::capacity(tree, height) {
                return Ok(IOResult::Done(None));
            }
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
                let found = if level == 0 {
                    frame.read(|b| radix::used_in_leaf(tree, b, index, from))
                } else {
                    let from_child = if from > node_first {
                        radix::child(tree, level, from)
                    } else {
                        0
                    };
                    frame.read(|b| radix::child_in_use(b, from_child))
                };
                let Some(found) = found else {
                    return Ok(busy());
                };
                match found {
                    Some(u) if level == 0 => return Ok(IOResult::Done(Some(u))),
                    Some(c) => {
                        index = index * radix::FANOUT + c;
                        level -= 1;
                    }
                    // Nothing in use from `from` on below this node: go on past it.
                    None => {
                        from = node_first + radix::span(tree, level);
                        continue 'search;
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------- provisional pages

    fn made_provisional(&self, pid: PageId, start: Lsn) {
        // A page abandoned before its adoption was durable keeps the earlier start: a crash
        // could still lose the adoption.
        self.provisional.made(pid, start);
    }

    fn settled(&self, pid: PageId, lsn: Lsn) {
        self.provisional.settled(pid, lsn);
    }

    /// The header and payload of an adoption or abandonment, to append in the group that links
    /// the pages in or out; then report where it landed with [`grouped`](Self::grouped).
    pub fn group_record(record: &StoreRecord) -> (Header, Vec<u8>) {
        assert!(
            matches!(
                record,
                StoreRecord::Adopt { .. } | StoreRecord::Abandon { .. }
            ),
            "only adoptions and abandonments go in another owner's group"
        );
        let header = Header {
            kind: record.kind(),
            flags: 0,
            txn: 0,
            prev: None,
        };
        // They change no metadata page, so no levels apply.
        (header, record.logged([0; TREES]))
    }

    /// Bytes an adoption or abandonment of `pages` pages takes in another owner's group.
    pub fn group_record_len(pages: usize) -> u64 {
        framed_len(TREES + 8 * pages)
    }

    /// Notes an adoption or abandonment appended in a group starting at `group`, ending at `lsn`.
    pub fn grouped(&self, record: &StoreRecord, group: Lsn, lsn: Lsn) {
        match record {
            StoreRecord::Adopt { pids } => pids.iter().for_each(|&p| self.settled(p, lsn)),
            StoreRecord::Abandon { pids } => {
                pids.iter().for_each(|&p| self.made_provisional(p, group))
            }
            _ => unreachable!("not a group record"),
        }
    }

    /// Adopts provisional pages on their own, for pages reachable from outside the log's records.
    pub fn adopt(&self, pids: &[PageId]) -> Result<IOResult<()>> {
        let record = StoreRecord::Adopt {
            pids: pids.to_vec(),
        };
        let (header, payload) = Self::group_record(&record);
        let mut admitted = match self.log().admit(framed_len(payload.len()))? {
            IOResult::Done(a) => a,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let lsn = self.log().append(&mut admitted, &header, &[&payload]);
        self.grouped(&record, lsn - framed_len(payload.len()), lsn);
        Ok(IOResult::Done(()))
    }

    /// The pages still provisional and not settled: what recovery frees once it has replayed
    /// the log.
    pub fn provisional_pages(&self) -> Vec<PageId> {
        let mut pids = self.provisional.unsettled();
        pids.sort();
        pids
    }

    /// Hands over a provisional page its owner gave up on: an operation dropped between
    /// allocating the page and adopting it, or between abandoning it and freeing it.
    pub fn orphaned(&self, pid: PageId) {
        self.orphans.push(pid);
    }

    /// The pages handed over and not yet taken to be freed, taken and handed over again.
    #[cfg(test)]
    pub fn orphans(&self) -> Vec<PageId> {
        let orphans = self.orphans.take_all();
        for &pid in &orphans {
            self.orphans.push(pid);
        }
        orphans
    }

    /// The pages handed over with [`orphaned`](Self::orphaned), for whoever frees them.
    pub fn take_orphans(&self) -> Vec<PageId> {
        self.orphans.take_all()
    }

    /// The first free unit of `tree` at or after `from`, wrapping around, or `None` if the tree is
    /// full. With `skip_unerased`, slots still being erased do not count as free.
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
                        if skip_unerased && self.unerased[class.index()].get(u) {
                            at = u + 1;
                            continue;
                        }
                        if tree == Tree::Map && self.unreusable[class.index()].get(u) {
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

    /// Adds a level to the top of `tree` for `class`. Free space grows only while no write plan
    /// holds its pages, whose paths would miss the new root.
    fn grow(self: &Arc<Self>, tree: Tree, class: SizeClass) -> Result<IOResult<()>> {
        if tree == Tree::Space && self.plans[class.index()].load(Ordering::Acquire) > 0 {
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        }
        let record_len = StoreRecord::Grow {
            tree,
            class,
            height: 0,
            used: 0,
        }
        .framed_len();
        let mut admitted = match self.log().admit(record_len)? {
            IOResult::Done(a) => a,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
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
        let record = StoreRecord::Grow {
            tree,
            class,
            height: height + 1,
            used,
        };
        let (start, lsn) = self.append(&mut admitted, &record, self.class_heights(class));
        radix::set_used_below(locked.page_of(new_root), 0, used);
        locked.commit(&self.meta, start, lsn);
        self.heights[tree as usize][class.index()].store(height + 1, Ordering::Release);
        Ok(IOResult::Done(()))
    }

    // ---------------------------------------------------------------- writing

    /// Plans the writeback of a copy of each of `pids`: see [`WritePlan`].
    pub fn plan_writes(self: &Arc<Self>, pids: Vec<PageId>) -> WritePlan {
        WritePlan {
            store: self.clone(),
            pids,
            admitted: None,
            slots: Vec::new(),
            intent: None,
            intent_start: 0,
            intent_floor: None,
            intent_open: 0,
            frames: Vec::new(),
            ready: false,
        }
    }

    fn append(
        &self,
        admitted: &mut Admitted,
        record: &StoreRecord,
        heights: Heights,
    ) -> (Lsn, Lsn) {
        let payload = record.logged(heights);
        let header = Header {
            kind: record.kind(),
            flags: 0,
            txn: 0,
            prev: None,
        };
        let lsn = self.log().append(admitted, &header, &[&payload]);
        (lsn - framed_len(payload.len()), lsn)
    }

    // ---------------------------------------------------------------- erasing

    fn freed(&self, (start, end): (Lsn, Lsn), class: SizeClass, slot: u64) {
        self.unerased[class.index()].set(slot);
        // The floor before the slot is queued: the redo bound keeps the free from here on.
        let floor = self.frees.hold(start);
        self.freed.push(FreedSlot {
            start,
            end,
            class,
            slot,
            floor,
        });
    }

    /// A slot taken by an intent replayed in recovery was erased before it was taken: forget any
    /// free of it replayed earlier, so erasing never reaches the copy written there since.
    fn retaken(&self, class: SizeClass, slot: u64) {
        if !self.unerased[class.index()].clear(slot) {
            return;
        }
        // Recovery replays alone, so the eraser is free.
        let mut eraser = self
            .erasure
            .try_lock()
            .expect("the eraser is idle in recovery");
        eraser.take_freed(&self.freed);
        for (_, slots) in eraser.erasing.values_mut() {
            slots.retain(|&s| s != (class, slot));
        }
        eraser.erasing.retain(|_, (_, slots)| !slots.is_empty());
        if let Some(floor) = eraser.floors.remove(&(class, slot)) {
            self.frees.release(floor);
        }
    }

    /// Erases every freed slot whose freeing is durable, and makes it free to take again. Done
    /// once everything erasable now is erased.
    ///
    /// A punched hole, like a write, is durable only once its file is synced: until then a crash
    /// can bring the old copy back. So every slot is punched (or overwritten with zeros) first,
    /// every file touched is synced, and only then do the slots count as erased. A failed write or
    /// sync leaves the slots to be erased again by the next call.
    pub fn erase_freed(&self) -> Result<IOResult<()>> {
        let Some(mut eraser) = self.erasure.try_lock() else {
            // Someone else is stepping it.
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        };
        let eraser = &mut *eraser;
        loop {
            let Some(e) = eraser.current.as_mut() else {
                eraser.take_freed(&self.freed);
                match self.start_erasure(&eraser.erasing)? {
                    Some(e) => eraser.current = Some(e),
                    None => return Ok(IOResult::Done(())),
                }
                continue;
            };
            if let Some(c) = e.pending.iter().find(|c| !c.finished()) {
                return Ok(IOResult::IO(IOCompletions(c.clone())));
            }
            if let Some(err) = e.pending.iter().find_map(|c| c.get_error()) {
                eraser.current = None;
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
            for (start, (_, slots)) in done.ready {
                if let Some((_, left)) = eraser.erasing.get_mut(&start) {
                    left.retain(|s| !slots.contains(s));
                    if left.is_empty() {
                        eraser.erasing.remove(&start);
                    }
                }
                for (class, slot) in slots {
                    self.unerased[class.index()].clear(slot);
                    if let Some(floor) = eraser.floors.remove(&(class, slot)) {
                        self.frees.release(floor);
                    }
                }
            }
            return Ok(IOResult::Done(()));
        }
    }

    /// Punches out every slot whose freeing is durable, or starts writing zeros over it where
    /// the file cannot punch holes; `None` if there is none.
    fn start_erasure(&self, erasing: &BTreeMap<Lsn, Freed>) -> Result<Option<Erasure>> {
        let durable = self
            .log()
            .durable()
            .min(self.erase_limit.load(Ordering::Acquire));
        let ready: Vec<(Lsn, Freed)> = erasing
            .iter()
            .take_while(|(_, (end, _))| *end <= durable)
            .map(|(&start, entry)| (start, entry.clone()))
            .collect();
        if ready.is_empty() {
            return Ok(None);
        }
        let mut files: BTreeMap<(SizeClass, u64), Arc<dyn File>> = BTreeMap::new();
        let mut pending = Vec::new();
        for (_, (_, slots)) in &ready {
            for &(class, slot) in slots {
                let (file, at) = self.data_file(class, slot)?;
                if file.punch_hole(at as usize, class.size()).is_err() {
                    let zeros = Arc::new(AlignedBlock::zeroed(class.size()));
                    let (write, zeroed) = whole_write(class.size());
                    drop(file.pwrite(at, zeros.region(0, class.size()), write)?);
                    pending.push(zeroed);
                }
                files.insert((class, slot / self.slots_per_file(class)), file);
            }
        }
        Ok(Some(Erasure {
            ready,
            files: files.into_values().collect(),
            pending,
            synced: false,
        }))
    }

    /// Lets erasing reach everything: a checkpoint past where the log restarted after a crash is
    /// durable.
    pub fn allow_erasing(&self) {
        self.erase_limit.store(u64::MAX, Ordering::Release);
    }

    // ---------------------------------------------------------------- recovery

    /// Gives a recovering store the log, reopened where the replay found it ends. Nothing freed by
    /// a record appended from here on is erased until [`allow_erasing`](Self::allow_erasing), once
    /// the checkpoint that ends recovery is durable: erasing what recovery frees stays ordered
    /// after the superblock that makes recovery's records part of the log every reader starts
    /// from.
    pub fn resume(&self, log: Arc<Log>) {
        self.erase_limit.store(log.end(), Ordering::Release);
        assert!(
            self.log.set(log).is_ok(),
            "the page store's log is given once"
        );
    }

    /// Replays a page-store record during recovery, unless the pages it changes already hold it.
    /// Safe to call again after it returned IO: pages already changed are skipped. Every record is
    /// replayed before anything new is appended: a new record would stamp the pages it changes
    /// with an LSN past every record still to replay, which would then be skipped.
    pub fn redo(self: &Arc<Self>, record: &OwnedRecord) -> Result<IOResult<()>> {
        let Some((r, heights)) = StoreRecord::decode_logged(record.header.kind, &record.payload)
        else {
            return Err(LimboError::Corrupt(format!(
                "a page store record at {} does not decode",
                record.start
            )));
        };
        let (start, lsn) = (record.start, record.lsn);
        if let StoreRecord::Grow {
            tree,
            class,
            height,
            ..
        } = &r
        {
            self.heights[*tree as usize][class.index()].fetch_max(*height, Ordering::AcqRel);
        }
        let ids = record_pages(&r, heights);
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        for frame in &frames {
            // A busy page is redone on the next step; one redone already is skipped by its LSN.
            if frame
                .redo(&self.meta, start, lsn, |page| apply(&r, frame.id, page))
                .is_none()
            {
                return Ok(busy());
            }
        }
        // What recovery must finish: erase what this record freed, settle its intents, and free
        // the pages it leaves provisional.
        let span = (start, lsn);
        match &r {
            StoreRecord::New { pid } => self.made_provisional(*pid, record.group),
            StoreRecord::Abandon { pids } => {
                for &p in pids {
                    self.made_provisional(p, record.group);
                }
            }
            StoreRecord::Adopt { pids } => {
                for &p in pids {
                    self.provisional.remove(p);
                }
            }
            StoreRecord::Free { pid, .. } => {
                self.provisional.remove(*pid);
                if let StoreRecord::Free { old: Some(old), .. } = &r {
                    self.freed(span, pid.class(), *old);
                }
            }
            StoreRecord::Remap { pid, new, old } => {
                self.pending_intents[pid.class().index()].clear(*new);
                if let Some(old) = old {
                    self.freed(span, pid.class(), *old);
                }
            }
            StoreRecord::Intent { class, slots } => {
                for &s in slots {
                    self.retaken(*class, s);
                    self.pending_intents[class.index()].set(s);
                }
            }
            StoreRecord::Release { class, slots } => {
                for &s in slots {
                    self.pending_intents[class.index()].clear(s);
                    self.freed(span, *class, s);
                }
            }
            _ => {}
        }
        Ok(IOResult::Done(()))
    }

    /// Releases the slots of intents no remap matched, so recovery leaves no copy behind that
    /// nothing refers to. Called once every record has been replayed.
    pub fn release_orphans(self: &Arc<Self>) -> Result<IOResult<()>> {
        for class in SizeClass::all() {
            let pending = &self.pending_intents[class.index()];
            let slots = pending.ones();
            if slots.is_empty() {
                continue;
            }
            match self.release(class, &slots)? {
                IOResult::Done(()) => {
                    for &s in &slots {
                        pending.clear(s);
                    }
                }
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Ends recovery: from now on a metadata page with no whole copy is corruption.
    pub fn end_recovery(&self) {
        self.meta.set_recovering(false);
    }

    /// Gives back taken, unused slots.
    fn release(self: &Arc<Self>, class: SizeClass, slots: &[u64]) -> Result<IOResult<()>> {
        let record = StoreRecord::Release {
            class,
            slots: slots.to_vec(),
        };
        let mut admitted = match self.log().admit(record.framed_len())? {
            IOResult::Done(a) => a,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let ids = record_pages(&record, self.class_heights(class));
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        if !self.release_locked(&frames, &mut admitted, class, slots) {
            return Ok(busy());
        }
        Ok(IOResult::Done(()))
    }

    /// Gives back `slots` under the latches of `frames`; false, with nothing done, while one is
    /// held.
    fn release_locked(
        &self,
        frames: &[MetaRef],
        admitted: &mut Admitted,
        class: SizeClass,
        slots: &[u64],
    ) -> bool {
        let Some(mut locked) = Locked::new(frames) else {
            return false;
        };
        let record = StoreRecord::Release {
            class,
            slots: slots.to_vec(),
        };
        let heights = heights_of(&locked.ids);
        let (start, lsn) = self.append(admitted, &record, heights);
        for &s in slots {
            set_space(&mut locked, class, s, false);
        }
        locked.commit(&self.meta, start, lsn);
        for &s in slots {
            self.freed((start, lsn), class, s);
        }
        true
    }

    // ---------------------------------------------------------------- checkpoints

    /// Where a checkpoint's redo point may be at most, as far as the page store is concerned: no
    /// change to a metadata page not yet on disk, no free not yet erased, and nothing that made a
    /// page provisional while it is, is before it.
    pub fn redo_bound(&self) -> Option<Lsn> {
        let provisional = self.provisional.least(self.log().durable());
        [
            self.meta.oldest_unwritten(),
            self.frees.least(),
            self.intents.least(),
            provisional,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Writes every metadata page changed before `before`.
    pub fn write_metadata(self: &Arc<Self>, before: Lsn) -> Result<IOResult<()>> {
        self.meta.write_dirty(self.log(), before)
    }
}

/// The writeback of a copy of each of a set of pages. Step it until it is ready, write each copy
/// with [`write`](Self::write), and [`settle`](Self::settle) each write once it lands.
///
/// Stepping admits log space for the intent and every remap, takes fresh slots for the copies and
/// logs the intent, holds in memory every metadata page the remaps will change (so a settle never
/// waits on IO), and waits until the intent is durable. A write's completion only reports that it
/// landed: the remap takes latches, which a completion may not wait for, so it is done in a step.
pub struct WritePlan {
    store: Arc<PageStore>,
    pids: Vec<PageId>,
    admitted: Option<Admitted>,
    slots: Vec<u64>,
    intent: Option<Lsn>,
    intent_start: Lsn,
    /// The intent's floor against the redo bound, held while any of its slots is not remapped
    /// or given back.
    intent_floor: Option<Floor>,
    intent_open: usize,
    frames: Vec<Vec<MetaRef>>,
    ready: bool,
}

impl WritePlan {
    /// One more of the intent's slots remapped or given back; the last gives its floor back.
    fn intent_settled(&mut self, store: &PageStore) {
        self.intent_open -= 1;
        if self.intent_open == 0 {
            if let Some(floor) = self.intent_floor.take() {
                store.intents.release(floor);
            }
        }
    }

    /// Where it stands, for a test that stalled to show.
    #[cfg(test)]
    pub fn describe(&self) -> String {
        format!(
            "admitted {} intent {:?} slots {} held {}/{} ready {}",
            self.admitted.is_some(),
            self.intent,
            self.slots.len(),
            self.frames.len(),
            self.pids.len(),
            self.ready
        )
    }

    pub fn step(&mut self) -> Result<IOResult<()>> {
        if self.ready {
            return Ok(IOResult::Done(()));
        }
        let store = self.store.clone();
        let class = match self.pids.first() {
            Some(pid) => pid.class(),
            None => {
                self.ready = true;
                return Ok(IOResult::Done(()));
            }
        };
        assert!(
            self.pids.iter().all(|p| p.class() == class),
            "a write plan spans size classes"
        );
        if self.admitted.is_none() {
            let intent = StoreRecord::Intent {
                class,
                slots: vec![0; self.pids.len()],
            };
            let remap = StoreRecord::Remap {
                pid: self.pids[0],
                new: 0,
                old: Some(0),
            };
            let release = StoreRecord::Release {
                class,
                slots: vec![0],
            };
            let bytes = intent.framed_len()
                + self.pids.len() as u64 * remap.framed_len().max(release.framed_len());
            self.admitted = Some(match store.log().admit(bytes)? {
                IOResult::Done(a) => a,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            });
        }
        if self.intent.is_none() {
            match self.take_slots(&store, class)? {
                IOResult::Done(()) => {}
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
        if self.frames.len() < self.pids.len() {
            match self.hold_remap_pages(&store, class)? {
                IOResult::Done(()) => {}
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
        match store.log().wait_durable(self.intent.expect("logged"))? {
            IOResult::Done(()) => {
                self.ready = true;
                Ok(IOResult::Done(()))
            }
            IOResult::IO(io) => Ok(IOResult::IO(io)),
        }
    }

    /// Finds free slots and takes them under one logged intent.
    fn take_slots(&mut self, store: &Arc<PageStore>, class: SizeClass) -> Result<IOResult<()>> {
        loop {
            let mut slots = Vec::with_capacity(self.pids.len());
            let mut from = store.cursors[class.index()].load(Ordering::Relaxed);
            while slots.len() < self.pids.len() {
                match store.find_free(Tree::Space, class, from, true)? {
                    IOResult::Done(Some(s)) => {
                        if !slots.contains(&s) {
                            slots.push(s);
                        }
                        from = s + 1;
                    }
                    IOResult::Done(None) => match store.grow(Tree::Space, class)? {
                        IOResult::Done(()) => {}
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    },
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                }
            }
            let record = StoreRecord::Intent {
                class,
                slots: slots.clone(),
            };
            let space_height = store.height(Tree::Space, class);
            let heights = [
                store.height(Tree::Map, class),
                space_height,
                store.height(Tree::Retired, class),
            ];
            let ids = record_pages(&record, heights);
            let frames = match store.meta.get_all(&ids)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let Some(mut locked) = Locked::new(&frames) else {
                return Ok(busy());
            };
            if store.height(Tree::Space, class) != space_height {
                continue;
            }
            let taken = slots.iter().any(|&s| {
                let leaf = radix::path(Tree::Space, class, 1, s)[0];
                radix::space_bit(locked.page_of(leaf), s) || store.unerased[class.index()].get(s)
            });
            if taken {
                // Someone took one since the search: look again.
                continue;
            }
            let admitted = self.admitted.as_mut().expect("admitted");
            let (start, lsn) = store.append(admitted, &record, heights);
            for &s in &slots {
                set_space(&mut locked, class, s, true);
            }
            locked.commit(&store.meta, start, lsn);
            // Held before the plan's pages are written back: until then they bound the redo point.
            self.intent_floor = Some(store.intents.hold(start));
            self.intent_open = slots.len();
            store.cursors[class.index()]
                .store(slots.iter().max().map_or(0, |s| s + 1), Ordering::Relaxed);
            self.slots = slots;
            self.intent = Some(lsn);
            self.intent_start = start;
            return Ok(IOResult::Done(()));
        }
    }

    /// Loads and holds, for each page, the page-table pages of its entry and the free-space pages
    /// of the slots its remap frees and its release would give back.
    fn hold_remap_pages(
        &mut self,
        store: &Arc<PageStore>,
        class: SizeClass,
    ) -> Result<IOResult<()>> {
        while self.frames.len() < self.pids.len() {
            let i = self.frames.len();
            let pid = self.pids[i];
            let entry = match store.entry(pid)? {
                IOResult::Done(e) => e,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            assert_ne!(entry, FREE, "writing back {pid:?}, which is not allocated");
            let mut ids = radix::path(
                Tree::Map,
                class,
                store.height(Tree::Map, class),
                pid.number(),
            );
            let space_height = store.height(Tree::Space, class);
            ids.extend(radix::path(Tree::Space, class, space_height, self.slots[i]));
            if let Some(old) = entry.checked_sub(SLOT_BASE) {
                ids.extend(radix::path(Tree::Space, class, space_height, old));
            }
            ids.sort();
            ids.dedup();
            let frames = match store.meta.get_all(&ids)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            if self.frames.is_empty() {
                store.plans[class.index()].fetch_add(1, Ordering::AcqRel);
            }
            self.frames.push(frames);
        }
        Ok(IOResult::Done(()))
    }

    /// The slot the `i`th page's copy goes to.
    pub fn slot(&self, i: usize) -> u64 {
        self.slots[i]
    }

    /// Writes the `i`th page's copy, which the caller sealed as `pids[i]` ([`page::seal`]), to its
    /// slot. `landed` gets how the
    /// write ended once the copy is durable or the write failed; then [`settle`](Self::settle) it.
    /// If the write cannot be submitted, `landed` never runs and the error is returned: settle the
    /// write as failed.
    pub fn write<F>(&self, i: usize, copy: Arc<Buffer>, landed: F) -> Result<Completion>
    where
        F: FnOnce(std::result::Result<(), CompletionError>) + Send + 'static,
    {
        assert!(self.ready, "writing before the plan is ready");
        let (pid, slot) = (self.pids[i], self.slots[i]);
        let (file, at) = self.store.data_file(pid.class(), slot)?;
        let len = copy.len();
        let landed = TakeOnce::new(landed);
        let c = Completion::new_write(move |res| {
            let res = whole(len, res);
            if let Some(landed) = landed.take() {
                landed(res);
            }
        });
        if file.supports_durable_writes() {
            return file.pwrite_durable(at, copy, c);
        }
        // Durable only once synced: the write lands with the sync. The file weakly: it holds the
        // IO the write waits in, so a strong hold would keep that IO alive for good once nothing
        // steps it; a file closed before its sync leaves the write not durable.
        let synced = c.clone();
        let sync_file = Arc::downgrade(&file);
        let wrote = Completion::new_write(move |res| match (res, sync_file.upgrade()) {
            (Ok(n), Some(sync_file)) => {
                let landed = synced.clone();
                let after = Completion::new_sync(move |r| match r {
                    Ok(_) => landed.complete(n),
                    Err(e) => landed.error(e),
                });
                if let Err(e) = sync_file.sync_data(after) {
                    synced.error(io_failure(&e));
                }
            }
            (Ok(_), None) => synced.error(CompletionError::Aborted),
            (Err(e), _) => synced.error(e),
        });
        drop(file.pwrite(at, copy, wrote)?);
        Ok(c)
    }

    /// Settles the `i`th write, which ended with `res`. Written, the page table points at its
    /// copy and the slot it replaces is freed; failed, its slot is given back. A yield while a
    /// metadata page it changes is latched: settle it again.
    pub fn settle(
        &mut self,
        i: usize,
        res: &std::result::Result<(), CompletionError>,
    ) -> IOResult<()> {
        let settled = match res {
            Ok(()) => self.remap(i),
            Err(_) => self.give_back(i),
        };
        if settled {
            IOResult::Done(())
        } else {
            busy()
        }
    }

    /// Points the page table at the `i`th copy and frees the slot it replaces; false, with
    /// nothing done, while a page it changes is latched.
    fn remap(&mut self, i: usize) -> bool {
        let store = self.store.clone();
        let pid = self.pids[i];
        let class = pid.class();
        let frames = &self.frames[i];
        let Some(mut locked) = Locked::new(frames) else {
            return false;
        };
        let leaf = radix::path(Tree::Map, class, 1, pid.number())[0];
        let entry = radix::map_entry(locked.page_of(leaf), pid.number());
        let old = entry.checked_sub(SLOT_BASE);
        let record = StoreRecord::Remap {
            pid,
            new: self.slots[i],
            old,
        };
        let admitted = self.admitted.as_mut().expect("admitted");
        let heights = heights_of(&locked.ids);
        let (start, lsn) = store.append(admitted, &record, heights);
        set_map(
            &mut locked,
            Tree::Map,
            pid.number(),
            entry,
            SLOT_BASE + self.slots[i],
        );
        if let Some(old) = old {
            set_space(&mut locked, class, old, false);
        }
        locked.commit(&store.meta, start, lsn);
        if let Some(old) = old {
            store.freed((start, lsn), class, old);
        }
        self.intent_settled(&store);
        true
    }

    /// Gives the `i`th slot back after its write failed; false, with nothing done, while a page
    /// it changes is latched.
    fn give_back(&mut self, i: usize) -> bool {
        let store = self.store.clone();
        let class = self.pids[i].class();
        let admitted = self.admitted.as_mut().expect("admitted");
        if !store.release_locked(&self.frames[i], admitted, class, &[self.slots[i]]) {
            return false;
        }
        self.intent_settled(&store);
        true
    }
}

impl Drop for WritePlan {
    fn drop(&mut self) {
        if let Some(pid) = self.pids.first() {
            if !self.frames.is_empty() {
                self.store.plans[pid.class().index()].fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

/// What a step that met a latched metadata page returns: try again shortly.
fn busy<T>() -> IOResult<T> {
    IOResult::IO(IOCompletions(Completion::new_yield()))
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

    fn page_of(&mut self, id: MetaId) -> &mut Writer<'a> {
        let i = self
            .ids
            .iter()
            .position(|&x| x == id)
            .unwrap_or_else(|| panic!("{id:?} is not latched"));
        self.holds[i].page()
    }

    /// Stamps every page with the change's LSN and marks it dirty.
    fn commit(self, cache: &MetaCache, start: Lsn, lsn: Lsn) {
        for mut hold in self.holds {
            let page = hold.page();
            let current = page::lsn(page);
            page::set_lsn(page, lsn.max(current));
            hold.commit(cache, start);
        }
    }
}

/// Sets `number`'s page-table entry from `from` to `to`, counting it in or out of use above.
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

/// Marks `slot` in use or free, counting it above.
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

/// Marks `number` of `class` retired or not, counting it above.
fn set_mark(locked: &mut Locked<'_>, class: SizeClass, number: u64, retired: bool) {
    let height = locked
        .ids
        .iter()
        .filter(|id| id.tree == Tree::Retired && id.class == class)
        .map(|id| id.level + 1)
        .max()
        .unwrap_or(1);
    let path = radix::path(Tree::Retired, class, height, number);
    let leaf = locked.page_of(path[0]);
    let was = radix::space_bit(leaf, number);
    assert_ne!(
        was,
        retired,
        "page {number} of {class:?} is already {}",
        if retired { "retired" } else { "not retired" }
    );
    radix::set_space_bit(leaf, number, retired);
    let delta = if retired { 1 } else { -1 };
    for id in &path[1..] {
        radix::add_used_below(
            locked.page_of(*id),
            radix::child(Tree::Retired, id.level, number),
            delta,
        );
    }
}

/// Retired marks to change in a group another owner appends: their records, and the metadata
/// pages those change, held. Made by [`PageStore::marks`].
pub struct Marks {
    /// A retire and a reinstate per class at most, each with the levels it is laid out at.
    records: Vec<(StoreRecord, Heights)>,
    frames: Vec<MetaRef>,
}

impl Marks {
    /// The records to append in the group, in order, after the owner's own.
    pub fn records(&self) -> Vec<(Header, Vec<u8>)> {
        self.records
            .iter()
            .map(|(r, h)| {
                let header = Header {
                    kind: r.kind(),
                    flags: 0,
                    txn: 0,
                    prev: None,
                };
                (header, r.logged(*h))
            })
            .collect()
    }

    /// Latches the marks' pages; or `None`, holding nothing, if one is held or a tree grew since
    /// the marks were made, whose paths they would miss: the owner lets its latches go, yields,
    /// and makes the marks again.
    pub fn lock<'a>(&'a self, store: &PageStore) -> Option<LockedMarks<'a>> {
        let locked = Locked::new(&self.frames)?;
        // A tree grows under its root's latch, which the marks hold: a height read now stays. A
        // mark lays out its retired marks' path only, so only their height matters.
        let at = Tree::Retired as usize;
        let grown = self
            .records
            .iter()
            .any(|(r, h)| store.height(Tree::Retired, record_class(r)) != h[at]);
        if grown {
            locked.release_unchanged();
            return None;
        }
        Some(LockedMarks {
            records: &self.records,
            locked,
        })
    }
}

/// The class a retire or reinstate is of.
fn record_class(r: &StoreRecord) -> SizeClass {
    match r {
        StoreRecord::Retire { pids } | StoreRecord::Reinstate { pids } => pids[0].class(),
        _ => unreachable!("marks are retires and reinstates"),
    }
}

/// Marks latched, to change once their records are appended.
pub struct LockedMarks<'a> {
    records: &'a [(StoreRecord, Heights)],
    locked: Locked<'a>,
}

impl LockedMarks<'_> {
    /// Sets and clears the marks, their records appended at `start..lsn`, and stamps their pages.
    pub fn commit(mut self, store: &PageStore, start: Lsn, lsn: Lsn) {
        for (r, _) in self.records {
            let (pids, retired) = match r {
                StoreRecord::Retire { pids } => (pids, true),
                StoreRecord::Reinstate { pids } => (pids, false),
                _ => unreachable!("marks are retires and reinstates"),
            };
            for pid in pids {
                set_mark(&mut self.locked, pid.class(), pid.number(), retired);
            }
        }
        self.locked.commit(&store.meta, start, lsn);
    }
}

/// The metadata pages a record logged under `heights` changes.
fn record_pages(
    r: &StoreRecord,
    [map_height, space_height, retired_height]: Heights,
) -> Vec<MetaId> {
    let map = |pid: &PageId| radix::path(Tree::Map, pid.class(), map_height, pid.number());
    let space = |class: SizeClass, slot: u64| radix::path(Tree::Space, class, space_height, slot);
    let mark = |pid: &PageId| radix::path(Tree::Retired, pid.class(), retired_height, pid.number());
    let mut ids = match r {
        StoreRecord::New { pid } => map(pid),
        StoreRecord::Free { pid, old, retired } => {
            let mut ids = map(pid);
            if let Some(old) = old {
                ids.extend(space(pid.class(), *old));
            }
            if *retired {
                ids.extend(mark(pid));
            }
            ids
        }
        StoreRecord::Remap { pid, old, .. } => {
            let mut ids = map(pid);
            if let Some(old) = old {
                ids.extend(space(pid.class(), *old));
            }
            ids
        }
        StoreRecord::Retire { pids } | StoreRecord::Reinstate { pids } => {
            pids.iter().flat_map(mark).collect()
        }
        StoreRecord::Intent { class, slots } | StoreRecord::Release { class, slots } => {
            slots.iter().flat_map(|&s| space(*class, s)).collect()
        }
        StoreRecord::Grow {
            tree,
            class,
            height,
            ..
        } => vec![radix::root(*tree, *class, *height)],
        StoreRecord::Adopt { .. } | StoreRecord::Abandon { .. } => Vec::new(),
    };
    ids.sort();
    ids.dedup();
    ids
}

/// The levels of each tree among latched metadata pages: the levels their paths were laid out
/// with.
fn heights_of(ids: &[MetaId]) -> Heights {
    let levels = |tree: Tree| {
        ids.iter()
            .filter(|id| id.tree == tree)
            .map(|id| id.level + 1)
            .max()
            .unwrap_or(1)
    };
    [
        levels(Tree::Map),
        levels(Tree::Space),
        levels(Tree::Retired),
    ]
}

/// What `r` does to the metadata page `id`, during recovery.
fn apply<P: BytesMut + ?Sized>(r: &StoreRecord, id: MetaId, page: &mut P) {
    let map = |page: &mut P, number: u64, from_used: bool, to: u64| {
        if id.level == 0 {
            radix::set_map_entry(page, number, to);
        } else {
            let delta = (to != FREE) as i64 - from_used as i64;
            if delta != 0 {
                radix::add_used_below(page, radix::child(Tree::Map, id.level, number), delta);
            }
        }
    };
    let space = |page: &mut P, slot: u64, used: bool| {
        if id.level == 0 {
            radix::set_space_bit(page, slot, used);
        } else {
            radix::add_used_below(
                page,
                radix::child(Tree::Space, id.level, slot),
                if used { 1 } else { -1 },
            );
        }
    };
    let mark = |page: &mut P, number: u64, retired: bool| {
        if id.level == 0 {
            radix::set_space_bit(page, number, retired);
        } else {
            radix::add_used_below(
                page,
                radix::child(Tree::Retired, id.level, number),
                if retired { 1 } else { -1 },
            );
        }
    };
    let on =
        |tree: Tree, unit: u64| id.tree == tree && unit / radix::span(tree, id.level) == id.index;
    match r {
        StoreRecord::New { pid } if on(Tree::Map, pid.number()) => {
            map(page, pid.number(), false, ALLOCATED)
        }
        StoreRecord::Free { pid, old, retired } => {
            if on(Tree::Map, pid.number()) {
                map(page, pid.number(), true, FREE);
            }
            if let Some(old) = old {
                if on(Tree::Space, *old) {
                    space(page, *old, false);
                }
            }
            if *retired && on(Tree::Retired, pid.number()) {
                mark(page, pid.number(), false);
            }
        }
        StoreRecord::Retire { pids } | StoreRecord::Reinstate { pids } => {
            let retired = matches!(r, StoreRecord::Retire { .. });
            for p in pids {
                if on(Tree::Retired, p.number()) {
                    mark(page, p.number(), retired);
                }
            }
        }
        StoreRecord::Remap { pid, new, old } => {
            if on(Tree::Map, pid.number()) {
                map(page, pid.number(), true, SLOT_BASE + new);
            }
            if let Some(old) = old {
                if on(Tree::Space, *old) {
                    space(page, *old, false);
                }
            }
        }
        StoreRecord::Intent { slots, .. } => {
            for &s in slots {
                if on(Tree::Space, s) {
                    space(page, s, true);
                }
            }
        }
        StoreRecord::Release { slots, .. } => {
            for &s in slots {
                if on(Tree::Space, s) {
                    space(page, s, false);
                }
            }
        }
        StoreRecord::Grow { used, .. } => radix::set_used_below(page, 0, *used),
        _ => {}
    }
}

fn io_failure(e: &LimboError) -> CompletionError {
    match e {
        LimboError::CompletionError(e) => *e,
        _ => CompletionError::IOError(std::io::ErrorKind::Other, "page store IO submission"),
    }
}

fn directory(base: &str) -> String {
    std::path::Path::new(base)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".".to_string())
}

mod provisional;
use provisional::Provisionals;

#[cfg(test)]
mod tests;
