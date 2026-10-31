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
//! The page table and free space are radix trees of metadata pages ([`super::radix`],
//! [`super::meta`]), changed only by the logged records below.

use super::aligned::AlignedBlock;
use super::log::{framed_len, Admitted, Header, Kind, Log, Lsn, OwnedRecord};
use super::meta::{MetaCache, MetaFrame, MetaId, Tree};
use super::page::{self, PageId, SizeClass, CLASS_COUNT};
use super::radix::{self, ALLOCATED, FREE, SLOT_BASE};
use crate::io::{Buffer, Completion, File, OpenFlags, IO};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Log record kinds the page store owns.
pub mod kinds {
    use super::Kind;
    pub const NEW: Kind = Kind::new(1).unwrap();
    pub const FREE: Kind = Kind::new(2).unwrap();
    pub const INTENT: Kind = Kind::new(3).unwrap();
    pub const REMAP: Kind = Kind::new(4).unwrap();
    pub const RELEASE: Kind = Kind::new(5).unwrap();
    pub const GROW: Kind = Kind::new(6).unwrap();

    pub fn owns(kind: Kind) -> bool {
        (1..=6).contains(&kind.get())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreRecord {
    /// A page was created, with no copy yet.
    New { pid: PageId },
    /// A page was dropped; its copy's slot, if it had one, is freed.
    Free { pid: PageId, old: Option<u64> },
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

/// The levels of a class's page table and free space a record was logged under. Recovery lays
/// out the record's paths with them, not with the levels the trees have grown to since.
pub type Heights = [u8; 2];

impl StoreRecord {
    fn kind(&self) -> Kind {
        match self {
            Self::New { .. } => kinds::NEW,
            Self::Free { .. } => kinds::FREE,
            Self::Intent { .. } => kinds::INTENT,
            Self::Remap { .. } => kinds::REMAP,
            Self::Release { .. } => kinds::RELEASE,
            Self::Grow { .. } => kinds::GROW,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let opt = |o: &Option<u64>| o.unwrap_or(NONE);
        match self {
            Self::New { pid } => out.extend(pid.raw().to_le_bytes()),
            Self::Free { pid, old } => {
                out.extend(pid.raw().to_le_bytes());
                out.extend(opt(old).to_le_bytes());
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
                out.push(matches!(tree, Tree::Space) as u8);
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
        let slots = || -> Option<Vec<u64>> {
            let rest = payload.get(1..)?;
            if rest.len() % 8 != 0 {
                return None;
            }
            Some(
                rest.chunks_exact(8)
                    .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                    .collect(),
            )
        };
        Some(match kind {
            k if k == kinds::NEW => Self::New {
                pid: PageId::from_raw(word(0)?),
            },
            k if k == kinds::FREE => Self::Free {
                pid: PageId::from_raw(word(0)?),
                old: opt(8)?,
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
            k if k == kinds::GROW => Self::Grow {
                tree: if *payload.first()? == 1 {
                    Tree::Space
                } else {
                    Tree::Map
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
        let heights = [*payload.first()?, *payload.get(1)?];
        Some((Self::decode(kind, &payload[2..])?, heights))
    }

    fn framed_len(&self) -> u64 {
        framed_len(2 + self.encode().len())
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
    log: Arc<Log>,
    meta: Arc<MetaCache>,
    files: Mutex<HashMap<(SizeClass, u64), Arc<dyn File>>>,
    /// Levels of the page table (`[0]`) and free space (`[1]`) per class.
    heights: Mutex<[[u8; CLASS_COUNT]; 2]>,
    growing: Mutex<()>,
    /// Where each class's next slot search starts.
    cursors: Mutex<[u64; CLASS_COUNT]>,
    /// Slots freed and not yet erased, by where the record that freed them starts, with where it
    /// ends.
    erasing: Mutex<BTreeMap<Lsn, Freed>>,
    /// Intents whose slots are not all remapped or given back yet: by where the intent starts,
    /// how many are left.
    open_intents: Mutex<BTreeMap<Lsn, usize>>,
    /// Write plans holding pages of each class's free space; free space grows only when none does.
    plans: Mutex<[u32; CLASS_COUNT]>,
    /// The same slots, which no allocation may take.
    unerased: Mutex<HashSet<(SizeClass, u64)>>,
    /// Only frees whose record ends at or before this are erased. After a crash it is where the
    /// log restarted, until a checkpoint past the restart is durable: a recovery from before that
    /// checkpoint cannot read what was appended since, and could still need the copies it freed.
    erase_limit: AtomicU64,
    /// While recovering: slots of intents not yet matched by a remap or a release.
    pending_intents: Mutex<HashMap<(SizeClass, u64), Lsn>>,
}

impl PageStore {
    /// A page store over `log`, its page table and free space as the superblock in charge left
    /// them (`heights`), or new.
    pub fn new(
        io: Arc<dyn IO>,
        base: &str,
        config: StoreConfig,
        log: Arc<Log>,
        heights: [[u8; CLASS_COUNT]; 2],
    ) -> Arc<Self> {
        let meta = Arc::new(MetaCache::new(io.clone(), base, config.meta_pages));
        Arc::new(Self {
            io,
            base: base.to_string(),
            config,
            log,
            meta,
            files: Mutex::new(HashMap::new()),
            heights: Mutex::new(heights.map(|h| h.map(|l| l.max(1)))),
            growing: Mutex::new(()),
            cursors: Mutex::new([0; CLASS_COUNT]),
            erasing: Mutex::new(BTreeMap::new()),
            open_intents: Mutex::new(BTreeMap::new()),
            plans: Mutex::new([0; CLASS_COUNT]),
            unerased: Mutex::new(HashSet::new()),
            erase_limit: AtomicU64::new(u64::MAX),
            pending_intents: Mutex::new(HashMap::new()),
        })
    }

    pub fn meta(&self) -> &Arc<MetaCache> {
        &self.meta
    }

    pub fn log(&self) -> &Arc<Log> {
        &self.log
    }

    pub fn heights(&self) -> [[u8; CLASS_COUNT]; 2] {
        *self.heights.lock()
    }

    fn class_heights(&self, class: SizeClass) -> Heights {
        let h = self.heights.lock();
        [
            h[Tree::Map as usize][class.index()],
            h[Tree::Space as usize][class.index()],
        ]
    }

    fn height(&self, tree: Tree, class: SizeClass) -> u8 {
        self.heights.lock()[tree as usize][class.index()]
    }

    fn slots_per_file(&self, class: SizeClass) -> u64 {
        (self.config.file_bytes / class.size() as u64).max(1)
    }

    /// The data file holding `slot` of `class`, and where in it the slot is.
    fn data_file(&self, class: SizeClass, slot: u64) -> Result<(Arc<dyn File>, u64)> {
        let per_file = self.slots_per_file(class);
        let (n, at) = (slot / per_file, (slot % per_file) * class.size() as u64);
        if let Some(f) = self.files.lock().get(&(class, n)) {
            return Ok((f.clone(), at));
        }
        let path = format!("{}.c{}.{n}", self.base, class.index());
        let file = self
            .io
            .open_file(&path, OpenFlags::Create | OpenFlags::Direct, false)?;
        // A remap may name a slot in this file only once the file's creation is durable.
        self.io.sync_directory(&directory(&self.base))?;
        self.files.lock().insert((class, n), file.clone());
        Ok((file, at))
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
            IOResult::Done(f) => IOResult::Done(f.read(|b| radix::map_entry(b, pid.number()))),
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

    /// Reads `slot` as a copy of `pid`; `done` gets the copy, `Invalid`, or the IO error.
    pub fn read_slot<F>(&self, pid: PageId, slot: u64, done: F) -> Result<Completion>
    where
        F: FnOnce(PageRead) + Send + 'static,
    {
        let class = pid.class();
        let (file, at) = self.data_file(class, slot)?;
        let block = Arc::new(AlignedBlock::zeroed(class.size()));
        let buffer = block.region(0, class.size());
        let done = Mutex::new(Some(done));
        let keep = buffer.clone();
        let c = Completion::new_read(buffer, move |res| {
            let done = done.lock().take()?;
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

    /// A new page of `class`, logged; it has no copy until its first write.
    pub fn allocate(self: &Arc<Self>, class: SizeClass) -> Result<IOResult<PageId>> {
        let record_len = StoreRecord::New {
            pid: PageId::new(class, 0),
        }
        .framed_len();
        let mut admitted = match self.log.admit(record_len)? {
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
            let mut locked = Locked::new(&frames);
            let leaf = ids[0];
            if self.height(Tree::Map, class) != height
                || radix::map_entry(locked.page_of(leaf), number) != FREE
            {
                // Taken, or the tree grew, since the search: look again.
                continue;
            }
            let heights = [height, self.height(Tree::Space, class)];
            let (start, lsn) = self.append(&mut admitted, &StoreRecord::New { pid }, heights);
            set_map(&mut locked, Tree::Map, number, FREE, ALLOCATED);
            locked.commit(&self.meta, start, lsn);
            return Ok(IOResult::Done(pid));
        }
    }

    /// Drops `pid`: its number becomes free and its copy's slot, if any, is freed.
    pub fn free(self: &Arc<Self>, pid: PageId) -> Result<IOResult<()>> {
        let class = pid.class();
        let record_len = StoreRecord::Free { pid, old: None }.framed_len();
        let mut admitted = match self.log.admit(record_len)? {
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
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let mut locked = Locked::new(&frames);
        if self.heights() != heights {
            // A tree grew since the paths were taken.
            drop(locked);
            return self.free(pid);
        }
        let leaf = radix::path(Tree::Map, class, 1, pid.number())[0];
        let current = radix::map_entry(locked.page_of(leaf), pid.number());
        assert_eq!(current, entry, "{pid:?} changed while being freed");
        let logged = [
            heights[Tree::Map as usize][class.index()],
            heights[Tree::Space as usize][class.index()],
        ];
        let (start, lsn) = self.append(&mut admitted, &StoreRecord::Free { pid, old }, logged);
        set_map(&mut locked, Tree::Map, pid.number(), entry, FREE);
        if let Some(old) = old {
            set_space(&mut locked, class, old, false);
        }
        locked.commit(&self.meta, start, lsn);
        if let Some(old) = old {
            self.freed((start, lsn), class, old);
        }
        Ok(IOResult::Done(()))
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
                        let Some(u) = frame.read(|b| radix::free_in_leaf(tree, b, index, at))
                        else {
                            break None;
                        };
                        if skip_unerased && self.unerased.lock().contains(&(class, u)) {
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
                match frame.read(|b| radix::child_with_room(tree, b, level, from_child)) {
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
        if tree == Tree::Space && self.plans.lock()[class.index()] > 0 {
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        }
        let record_len = StoreRecord::Grow {
            tree,
            class,
            height: 0,
            used: 0,
        }
        .framed_len();
        let mut admitted = match self.log.admit(record_len)? {
            IOResult::Done(a) => a,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let _growing = self.growing.lock();
        let height = self.height(tree, class);
        let old_root = radix::root(tree, class, height);
        let new_root = radix::root(tree, class, height + 1);
        let frames = match self.meta.get_all(&[old_root, new_root])? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let mut locked = Locked::new(&frames);
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
        self.heights.lock()[tree as usize][class.index()] = height + 1;
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
            prev: 0,
        };
        let lsn = self.log.append(admitted, &header, &[&payload]);
        (lsn - framed_len(payload.len()), lsn)
    }

    // ---------------------------------------------------------------- erasing

    fn freed(&self, (start, end): (Lsn, Lsn), class: SizeClass, slot: u64) {
        self.unerased.lock().insert((class, slot));
        self.erasing
            .lock()
            .entry(start)
            .or_insert((end, Vec::new()))
            .1
            .push((class, slot));
    }

    /// A slot taken by an intent replayed in recovery was erased before it was taken: forget any
    /// free of it replayed earlier, so erasing never reaches the copy written there since.
    fn retaken(&self, class: SizeClass, slot: u64) {
        if !self.unerased.lock().remove(&(class, slot)) {
            return;
        }
        let mut erasing = self.erasing.lock();
        for (_, slots) in erasing.values_mut() {
            slots.retain(|&s| s != (class, slot));
        }
        erasing.retain(|_, (_, slots)| !slots.is_empty());
    }

    fn intent_opened(&self, start: Lsn, slots: usize) {
        *self.open_intents.lock().entry(start).or_default() += slots;
    }

    fn intent_settled(&self, start: Lsn) {
        let mut open = self.open_intents.lock();
        let left = open.get_mut(&start).expect("an open intent");
        *left -= 1;
        if *left == 0 {
            open.remove(&start);
        }
    }

    /// Erases every freed slot whose freeing is durable, and makes it free to take again.
    ///
    /// A punched hole, like a write, is durable only once its file is synced: until then a crash
    /// can bring the old copy back. So every slot is punched (or overwritten with zeros) first,
    /// every file touched is synced, and only then do the slots count as erased.
    pub fn erase_freed(&self) -> Result<()> {
        let durable = self
            .log
            .durable()
            .min(self.erase_limit.load(Ordering::Acquire));
        let ready: Vec<(Lsn, Freed)> = {
            let erasing = self.erasing.lock();
            erasing
                .iter()
                .take_while(|(_, (end, _))| *end <= durable)
                .map(|(&start, entry)| (start, entry.clone()))
                .collect()
        };
        if ready.is_empty() {
            return Ok(());
        }
        let mut touched: BTreeMap<(SizeClass, u64), Arc<dyn File>> = BTreeMap::new();
        for (_, (_, slots)) in &ready {
            for &(class, slot) in slots {
                let (file, at) = self.data_file(class, slot)?;
                if file.punch_hole(at as usize, class.size()).is_err() {
                    let zeros = Arc::new(AlignedBlock::zeroed(class.size()));
                    let c = file.pwrite(
                        at,
                        zeros.region(0, class.size()),
                        Completion::new_write(|_| {}),
                    )?;
                    self.io.wait_for_completion(c)?;
                }
                touched.insert((class, slot / self.slots_per_file(class)), file);
            }
        }
        for file in touched.values() {
            let c = file.sync_data(Completion::new_sync(|_| {}))?;
            self.io.wait_for_completion(c)?;
        }
        let mut erasing = self.erasing.lock();
        let mut unerased = self.unerased.lock();
        for (start, (_, slots)) in ready {
            if let Some((_, left)) = erasing.get_mut(&start) {
                left.retain(|s| !slots.contains(s));
                if left.is_empty() {
                    erasing.remove(&start);
                }
            }
            for slot in slots {
                unerased.remove(&slot);
            }
        }
        Ok(())
    }

    /// Lets erasing reach everything: a checkpoint past where the log restarted after a crash is
    /// durable.
    pub fn allow_erasing(&self) {
        self.erase_limit.store(u64::MAX, Ordering::Release);
    }

    // ---------------------------------------------------------------- recovery

    /// Starts recovery of a log whose valid records end at `end`: metadata pages with no whole copy
    /// read as zeros, and nothing freed by a record after `end` is erased until
    /// [`allow_erasing`](Self::allow_erasing).
    pub fn begin_recovery(&self, end: Lsn) {
        self.meta.set_recovering(true);
        self.erase_limit.store(end, Ordering::Release);
    }

    /// Replays a page-store record during recovery, unless the pages it changes already hold it.
    /// Safe to call again after it returned IO: pages already changed are skipped.
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
            let mut heights = self.heights.lock();
            let h = &mut heights[*tree as usize][class.index()];
            *h = (*h).max(*height);
        }
        let ids = record_pages(&r, heights);
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        for frame in &frames {
            frame.redo(&self.meta, start, lsn, |page| apply(&r, frame.id, page));
        }
        // What recovery must finish: erase what this record freed, and settle its intents.
        let span = (start, lsn);
        match &r {
            StoreRecord::Free {
                pid,
                old: Some(old),
            } => self.freed(span, pid.class(), *old),
            StoreRecord::Remap { pid, new, old } => {
                self.pending_intents.lock().remove(&(pid.class(), *new));
                if let Some(old) = old {
                    self.freed(span, pid.class(), *old);
                }
            }
            StoreRecord::Intent { class, slots } => {
                for &s in slots {
                    self.retaken(*class, s);
                    self.pending_intents.lock().insert((*class, s), start);
                }
            }
            StoreRecord::Release { class, slots } => {
                for &s in slots {
                    self.pending_intents.lock().remove(&(*class, s));
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
        let mut by_class: BTreeMap<SizeClass, Vec<u64>> = BTreeMap::new();
        for &(class, slot) in self.pending_intents.lock().keys() {
            by_class.entry(class).or_default().push(slot);
        }
        for (class, mut slots) in by_class {
            slots.sort_unstable();
            match self.release(class, &slots)? {
                IOResult::Done(()) => {
                    let mut pending = self.pending_intents.lock();
                    for s in &slots {
                        pending.remove(&(class, *s));
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
        let mut admitted = match self.log.admit(record.framed_len())? {
            IOResult::Done(a) => a,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let ids = record_pages(&record, self.class_heights(class));
        let frames = match self.meta.get_all(&ids)? {
            IOResult::Done(f) => f,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        self.release_locked(&frames, &mut admitted, class, slots);
        Ok(IOResult::Done(()))
    }

    fn release_locked(
        &self,
        frames: &[Arc<MetaFrame>],
        admitted: &mut Admitted,
        class: SizeClass,
        slots: &[u64],
    ) {
        let mut locked = Locked::new(frames);
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
    }

    // ---------------------------------------------------------------- checkpoints

    /// Where a checkpoint's redo point may be at most, as far as the page store is concerned: no
    /// change to a metadata page not yet on disk, and no free not yet erased, is before it.
    pub fn redo_bound(&self) -> Option<Lsn> {
        [
            self.meta.oldest_unwritten(),
            self.erasing.lock().keys().next().copied(),
            self.open_intents.lock().keys().next().copied(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Writes every metadata page changed before `before`.
    pub fn write_metadata(self: &Arc<Self>, before: Lsn) -> Result<IOResult<()>> {
        self.meta.write_dirty(&self.log, before)
    }
}

/// The writeback of a copy of each of a set of pages. Step it until it is ready, then write each
/// copy with [`write`](Self::write).
///
/// Stepping admits log space for the intent and every remap, takes fresh slots for the copies and
/// logs the intent, holds in memory every metadata page the remaps will change (so a remap never
/// waits on IO inside a write's completion), and waits until the intent is durable.
pub struct WritePlan {
    store: Arc<PageStore>,
    pids: Vec<PageId>,
    admitted: Option<Admitted>,
    slots: Vec<u64>,
    intent: Option<Lsn>,
    intent_start: Lsn,
    frames: Vec<Vec<Arc<MetaFrame>>>,
    ready: bool,
}

impl WritePlan {
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
            self.admitted = Some(match store.log.admit(bytes)? {
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
        match store.log.wait_durable(self.intent.expect("logged"))? {
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
            let mut from = store.cursors.lock()[class.index()];
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
            let heights = [store.height(Tree::Map, class), space_height];
            let ids = record_pages(&record, heights);
            let frames = match store.meta.get_all(&ids)? {
                IOResult::Done(f) => f,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            let mut locked = Locked::new(&frames);
            if store.height(Tree::Space, class) != space_height {
                continue;
            }
            let taken = slots.iter().any(|&s| {
                let leaf = radix::path(Tree::Space, class, 1, s)[0];
                radix::space_bit(locked.page_of(leaf), s)
                    || store.unerased.lock().contains(&(class, s))
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
            store.intent_opened(start, slots.len());
            store.cursors.lock()[class.index()] = slots.iter().max().map_or(0, |s| s + 1);
            self.slots = slots;
            self.intent = Some(lsn);
            self.intent_start = start;
            return Ok(IOResult::Done(()));
        }
    }

    /// Loads and pins, for each page, the page-table pages of its entry and the free-space pages
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
                store.plans.lock()[class.index()] += 1;
            }
            for f in &frames {
                f.pin();
            }
            self.frames.push(frames);
        }
        Ok(IOResult::Done(()))
    }

    /// The slot the `i`th page's copy goes to.
    pub fn slot(&self, i: usize) -> u64 {
        self.slots[i]
    }

    /// Writes the `i`th page's copy, sealed as `pids[i]`, and remaps the page once it is durable.
    /// `done` gets the slot, or the error, after which the slot is given back.
    pub fn write<F>(
        this: &Arc<Mutex<Self>>,
        i: usize,
        copy: Arc<Buffer>,
        done: F,
    ) -> Result<Completion>
    where
        F: FnOnce(std::result::Result<u64, CompletionError>) + Send + 'static,
    {
        let (store, pid, slot) = {
            let plan = this.lock();
            assert!(plan.ready, "writing before the plan is ready");
            (plan.store.clone(), plan.pids[i], plan.slots[i])
        };
        page::seal(copy.as_mut_slice(), pid);
        let (file, at) = store.data_file(pid.class(), slot)?;
        let len = copy.len();
        let plan = this.clone();
        let done = Mutex::new(Some(done));
        let c = Completion::new_write(move |res| {
            let res = match res {
                Ok(n) if n as usize == len => Ok(()),
                Ok(_) => Err(CompletionError::ShortWrite),
                Err(e) => Err(e),
            };
            let mut plan = plan.lock();
            let res = res.map(|()| plan.remap(i));
            if res.is_err() {
                plan.give_back(i);
            }
            if let Some(done) = done.lock().take() {
                done(res.map(|()| slot));
            }
        });
        if file.supports_durable_writes() {
            return file.pwrite_durable(at, copy, c);
        }
        // Durable only once synced: the remap waits for the sync, and the caller waits for the
        // remap.
        let synced = c.clone();
        let sync_file = file.clone();
        let wrote = Completion::new_write(move |res| match res {
            Ok(n) => {
                let remap = synced.clone();
                let after = Completion::new_sync(move |r| match r {
                    Ok(_) => remap.complete(n),
                    Err(e) => remap.error(e),
                });
                if let Err(e) = sync_file.sync_data(after) {
                    synced.error(io_failure(&e));
                }
            }
            Err(e) => synced.error(e),
        });
        drop(file.pwrite(at, copy, wrote)?);
        Ok(c)
    }

    /// Points the page table at the `i`th copy and frees the slot it replaces.
    fn remap(&mut self, i: usize) {
        let store = self.store.clone();
        let pid = self.pids[i];
        let class = pid.class();
        let frames = &self.frames[i];
        let mut locked = Locked::new(frames);
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
        store.intent_settled(self.intent_start);
    }

    /// Gives the `i`th slot back after its write failed.
    fn give_back(&mut self, i: usize) {
        let store = self.store.clone();
        let class = self.pids[i].class();
        let admitted = self.admitted.as_mut().expect("admitted");
        store.release_locked(&self.frames[i], admitted, class, &[self.slots[i]]);
        store.intent_settled(self.intent_start);
    }
}

impl Drop for WritePlan {
    fn drop(&mut self) {
        for frames in &self.frames {
            for f in frames {
                f.unpin();
            }
        }
        if let Some(pid) = self.pids.first() {
            if !self.frames.is_empty() {
                self.store.plans.lock()[pid.class().index()] -= 1;
            }
        }
    }
}

/// Metadata pages latched together, in a fixed order so two changes never wait on each other.
struct Locked<'a> {
    ids: Vec<MetaId>,
    guards: Vec<crate::sync::RwLockWriteGuard<'a, Box<[u8]>>>,
    frames: Vec<&'a Arc<MetaFrame>>,
}

impl<'a> Locked<'a> {
    fn new(frames: &'a [Arc<MetaFrame>]) -> Self {
        let mut order: Vec<&Arc<MetaFrame>> = frames.iter().collect();
        order.sort_by_key(|f| f.id);
        order.dedup_by_key(|f| f.id);
        let guards = order.iter().map(|f| f.lock()).collect();
        Self {
            ids: order.iter().map(|f| f.id).collect(),
            guards,
            frames: order,
        }
    }

    fn page_of(&mut self, id: MetaId) -> &mut [u8] {
        let i = self
            .ids
            .iter()
            .position(|&x| x == id)
            .unwrap_or_else(|| panic!("{id:?} is not latched"));
        &mut self.guards[i]
    }

    /// Stamps every page with the change's LSN and marks it dirty.
    fn commit(self, cache: &MetaCache, start: Lsn, lsn: Lsn) {
        let Self { guards, frames, .. } = self;
        for (mut guard, frame) in guards.into_iter().zip(frames) {
            let current = page::lsn(&guard);
            page::set_lsn(&mut guard, lsn.max(current));
            drop(guard);
            frame.changed(cache, start);
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

/// The metadata pages a record logged under `heights` changes.
fn record_pages(r: &StoreRecord, [map_height, space_height]: Heights) -> Vec<MetaId> {
    let map = |pid: &PageId| radix::path(Tree::Map, pid.class(), map_height, pid.number());
    let space = |class: SizeClass, slot: u64| radix::path(Tree::Space, class, space_height, slot);
    let mut ids = match r {
        StoreRecord::New { pid } => map(pid),
        StoreRecord::Free { pid, old } | StoreRecord::Remap { pid, old, .. } => {
            let mut ids = map(pid);
            if let Some(old) = old {
                ids.extend(space(pid.class(), *old));
            }
            ids
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
    [levels(Tree::Map), levels(Tree::Space)]
}

/// What `r` does to the metadata page `id`, during recovery.
fn apply(r: &StoreRecord, id: MetaId, page: &mut [u8]) {
    let map = |page: &mut [u8], number: u64, from_used: bool, to: u64| {
        if id.level == 0 {
            radix::set_map_entry(page, number, to);
        } else {
            let delta = (to != FREE) as i64 - from_used as i64;
            if delta != 0 {
                radix::add_used_below(page, radix::child(Tree::Map, id.level, number), delta);
            }
        }
    };
    let space = |page: &mut [u8], slot: u64, used: bool| {
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
    let on =
        |tree: Tree, unit: u64| id.tree == tree && unit / radix::span(tree, id.level) == id.index;
    match r {
        StoreRecord::New { pid } if on(Tree::Map, pid.number()) => {
            map(page, pid.number(), false, ALLOCATED)
        }
        StoreRecord::Free { pid, old } => {
            if on(Tree::Map, pid.number()) {
                map(page, pid.number(), true, FREE);
            }
            if let Some(old) = old {
                if on(Tree::Space, *old) {
                    space(page, *old, false);
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

#[cfg(test)]
mod tests;
