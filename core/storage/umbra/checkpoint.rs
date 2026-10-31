//! Checkpoints: the page table made durable, so the log before a redo point can be cut, and
//! opening a database: load the newest checkpoint, recover the log after it, checkpoint again.
//!
//! **The map file** (`{base}.map`) holds the page table, in pages of the smallest size:
//!
//! - slots 0 and 1: two superblocks, written in turn; the newest that reads back whole wins, so a
//!   superblock torn by a crash leaves the one before it in charge;
//! - every other slot: *map pages*, each holding the slots of [`ENTRIES_PER_MAP`] pages of one
//!   size class, and *directory pages*, each holding the slots of as many map pages. A superblock
//!   lists every directory page. Map and directory pages are written out of place, like data.
//!
//! **A checkpoint** runs beside writers (ARIES's fuzzy checkpoint):
//!
//! 1. Its redo point is the oldest change some page in memory holds that no durable copy does,
//!    or the log's end when every page is clean. The page table's changed map pages are taken.
//! 2. The data files are synced: every copy the taken table names is durable.
//! 3. The changed map pages are written, then the directory pages naming them, then synced.
//! 4. The superblock is written over the older of the two and synced.
//! 5. Only now is anything superseded let go: data and map slots retired before the checkpoint
//!    began are erased (a hole punched, else zeros written) and reused, and the log before the
//!    redo point is cut.
//!
//! It is a state machine stepped like any other operation: each call returns `IOResult`.

use super::buffer::BufferManager;
use super::page::{self, PageId, SizeClass, CLASS_COUNT, HEADER_SIZE, MIN_PAGE_SIZE};
use super::store::{ClassSnapshot, PageStore, ENTRIES_PER_MAP};
use super::wal::{self, Lsn, Record, Recovered, Wal};
use crate::io::{Buffer, Completion, File, FileSyncType, OpenFlags, IO};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::BTreeSet;

const SUPERBLOCKS: u64 = 2;
const SUPER_MAGIC: u64 = u64::from_le_bytes(*b"UMBRASB1");
const PAGE: usize = MIN_PAGE_SIZE;

fn class0() -> SizeClass {
    SizeClass::new(0).expect("class 0")
}

/// Page ids of the map file's pages, sealed into them so a slot read back is known to hold the
/// page its reader expects. They live apart from data pages, in another file.
fn superblock_pid() -> PageId {
    PageId::new(class0(), (1 << 56) - 1)
}

fn map_pid(class: SizeClass, index: u64) -> PageId {
    PageId::new(class0(), (1 << 54) | ((class.index() as u64) << 48) | index)
}

fn directory_pid(class: SizeClass, index: u64) -> PageId {
    PageId::new(class0(), (1 << 55) | ((class.index() as u64) << 48) | index)
}

fn entries_page(entries: &[u64]) -> Arc<Buffer> {
    let buf = Arc::new(Buffer::new(vec![0; PAGE]));
    let bytes = buf.as_mut_slice();
    for (i, e) in entries.iter().enumerate() {
        bytes[HEADER_SIZE + 8 * i..HEADER_SIZE + 8 * i + 8].copy_from_slice(&e.to_le_bytes());
    }
    buf
}

fn entries_of(page: &[u8]) -> Vec<u64> {
    page[HEADER_SIZE..HEADER_SIZE + 8 * ENTRIES_PER_MAP]
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

fn wait(io: &Arc<dyn IO>, c: Completion) -> Result<()> {
    io.wait_for_completion(c)
}

/// What a superblock records.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Superblock {
    seq: u64,
    redo: Lsn,
    map_next_slot: u64,
    /// Per class: next page number, next data slot, and the slots of its directory pages.
    classes: Vec<(u64, u64, Vec<u64>)>,
}

impl Superblock {
    fn encode(&self) -> Arc<Buffer> {
        let mut words = vec![SUPER_MAGIC, self.seq, self.redo, self.map_next_slot];
        for (next_page, next_slot, dirs) in &self.classes {
            words.extend([*next_page, *next_slot, dirs.len() as u64]);
            words.extend(dirs);
        }
        assert!(
            HEADER_SIZE + 8 * words.len() <= PAGE,
            "{} directory pages overflow the superblock",
            self.classes.iter().map(|c| c.2.len()).sum::<usize>()
        );
        let buf = entries_page(&words);
        page::set_lsn(buf.as_mut_slice(), self.seq);
        buf
    }

    fn decode(page: &[u8]) -> Option<Self> {
        let mut words = page[HEADER_SIZE..]
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()));
        if words.next()? != SUPER_MAGIC {
            return None;
        }
        let (seq, redo, map_next_slot) = (words.next()?, words.next()?, words.next()?);
        let mut classes = Vec::with_capacity(CLASS_COUNT);
        for _ in 0..CLASS_COUNT {
            let (next_page, next_slot, dirs) = (words.next()?, words.next()?, words.next()?);
            let dirs = (0..dirs)
                .map(|_| words.next())
                .collect::<Option<Vec<_>>>()?;
            classes.push((next_page, next_slot, dirs));
        }
        Some(Self {
            seq,
            redo,
            map_next_slot,
            classes,
        })
    }
}

#[derive(Default)]
struct MapSpace {
    next_slot: u64,
    free: Vec<u64>,
    retired: Vec<u64>,
}

impl MapSpace {
    fn take(&mut self) -> u64 {
        self.free.pop().unwrap_or_else(|| {
            self.next_slot += 1;
            self.next_slot - 1
        })
    }
}

/// Where the map file's pages are: per class, map page index → slot plus one, and directory
/// page index → slot plus one.
#[derive(Default)]
struct Tables {
    maps: Vec<Vec<u64>>,
    dirs: Vec<Vec<u64>>,
}

/// A map or directory page a checkpoint wrote.
#[derive(Clone, Copy)]
enum Placed {
    Map(SizeClass, u64, u64),
    Directory(SizeClass, u64, u64),
}

enum Phase {
    SyncData(Completion),
    Maps(Vec<Completion>),
    Directories(Vec<Completion>),
    SyncMaps(Completion),
    Superblock(Completion),
    SyncSuperblock(Completion),
    Erase(Vec<Completion>),
}

struct Run {
    seq: u64,
    redo: Lsn,
    classes: Vec<ClassSnapshot>,
    placed: Arc<Mutex<Vec<std::result::Result<Placed, CompletionError>>>>,
    phase: Phase,
}

/// A checkpoint that completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpointed {
    pub seq: u64,
    pub redo: Lsn,
}

pub struct Checkpointer {
    file: Arc<dyn File>,
    space: Mutex<MapSpace>,
    tables: Mutex<Tables>,
    seq: AtomicU64,
    run: Mutex<Option<Run>>,
}

impl Checkpointer {
    /// Opens the map file at `path` and installs the newest checkpoint's page table into `store`.
    /// Returns its redo point, where recovery starts: zero for a database never checkpointed.
    fn open(io: &Arc<dyn IO>, path: &str, store: &PageStore) -> Result<(Self, Lsn)> {
        let file = io.open_file(path, OpenFlags::Create, false)?;
        let file_slots = file.size()? / PAGE as u64;
        let mut newest: Option<Superblock> = None;
        for slot in 0..SUPERBLOCKS.min(file_slots) {
            if let Some(page) = read_page(io, &file, superblock_pid(), slot)? {
                if let Some(sb) = Superblock::decode(&page) {
                    if newest.as_ref().is_none_or(|n| sb.seq > n.seq) {
                        newest = Some(sb);
                    }
                }
            }
        }
        let mut tables = Tables {
            maps: vec![Vec::new(); CLASS_COUNT],
            dirs: vec![Vec::new(); CLASS_COUNT],
        };
        let mut used: BTreeSet<u64> = (0..SUPERBLOCKS).collect();
        let sb = newest.unwrap_or_default();
        for (class, (next_page, next_slot, dirs)) in SizeClass::all().zip(&sb.classes) {
            let c = class.index();
            for (d, &dir_slot) in dirs.iter().enumerate() {
                tables.dirs[c].push(dir_slot + 1);
                used.insert(dir_slot);
                let page = read_page(io, &file, directory_pid(class, d as u64), dir_slot)?
                    .ok_or_else(|| torn("directory", class, d as u64))?;
                tables.maps[c].extend(entries_of(&page));
            }
            let mut copies = Vec::new();
            for (index, &map) in tables.maps[c].iter().enumerate().filter(|(_, &m)| m != 0) {
                used.insert(map - 1);
                let page = read_page(io, &file, map_pid(class, index as u64), map - 1)?
                    .ok_or_else(|| torn("map", class, index as u64))?;
                let first = index as u64 * ENTRIES_PER_MAP as u64;
                copies.extend(
                    entries_of(&page)
                        .into_iter()
                        .enumerate()
                        .filter(|&(_, e)| e != 0)
                        .map(|(i, e)| (first + i as u64, e - 1)),
                );
            }
            store.install(class, *next_page, *next_slot, &copies, false);
        }
        let next_slot = sb
            .map_next_slot
            .max(file_slots)
            .max(used.last().map_or(0, |&s| s + 1))
            .max(SUPERBLOCKS);
        // What no checkpoint refers to: pages of a checkpoint that never finished.
        let retired = (SUPERBLOCKS..next_slot)
            .filter(|s| !used.contains(s))
            .collect();
        let ckpt = Self {
            file,
            space: Mutex::new(MapSpace {
                next_slot,
                free: Vec::new(),
                retired,
            }),
            tables: Mutex::new(tables),
            seq: AtomicU64::new(sb.seq),
            run: Mutex::new(None),
        };
        Ok((ckpt, sb.redo))
    }

    /// Steps the checkpoint, starting one if none is running.
    pub fn checkpoint(&self, bm: &BufferManager, wal: &Arc<Wal>) -> Result<IOResult<Checkpointed>> {
        let mut slot = self.run.lock();
        let run = match slot.as_mut() {
            Some(run) => run,
            None => slot.insert(self.begin(bm, wal)?),
        };
        match self.advance(run, bm, wal) {
            Ok(IOResult::Done(done)) => {
                *slot = None;
                Ok(IOResult::Done(done))
            }
            Ok(io) => Ok(io),
            Err(e) => {
                // Started again from the beginning; what it had taken is put back.
                let run = slot.take().expect("running");
                for (class, snap) in SizeClass::all().zip(&run.classes) {
                    bm.store().redirty(class, snap.maps.iter().map(|m| m.0));
                }
                Err(e)
            }
        }
    }

    fn begin(&self, bm: &BufferManager, wal: &Arc<Wal>) -> Result<Run> {
        let end = wal.end();
        let redo = bm
            .oldest_unwritten_change()
            .map_or(end, |oldest| oldest.min(end));
        let store = bm.store();
        let classes = SizeClass::all().map(|c| store.snapshot(c)).collect();
        Ok(Run {
            seq: self.seq.load(Ordering::Acquire) + 1,
            redo,
            classes,
            placed: Arc::new(Mutex::new(Vec::new())),
            phase: Phase::SyncData(store.sync()?),
        })
    }

    fn advance(
        &self,
        run: &mut Run,
        bm: &BufferManager,
        wal: &Arc<Wal>,
    ) -> Result<IOResult<Checkpointed>> {
        loop {
            let next = match &run.phase {
                Phase::SyncData(c) => {
                    if let Some(io) = waiting(c)? {
                        return Ok(io);
                    }
                    let mut writes = Vec::new();
                    for (class, snap) in SizeClass::all().zip(&run.classes) {
                        for (index, entries) in &snap.maps {
                            let (class, index) = (class, *index);
                            writes.push(self.write_page(
                                map_pid(class, index),
                                entries_page(entries),
                                &run.placed,
                                move |slot| Placed::Map(class, index, slot),
                            )?);
                        }
                    }
                    Phase::Maps(writes)
                }
                Phase::Maps(writes) => {
                    if let Some(c) = pending(writes) {
                        return Ok(IOResult::IO(IOCompletions(c)));
                    }
                    let mut dirty = vec![BTreeSet::new(); CLASS_COUNT];
                    self.apply_placed(run, |placed, tables, space| {
                        if let Placed::Map(class, index, slot) = placed {
                            let maps = &mut tables.maps[class.index()];
                            if maps.len() <= index as usize {
                                maps.resize(index as usize + 1, 0);
                            }
                            if maps[index as usize] != 0 {
                                space.retired.push(maps[index as usize] - 1);
                            }
                            maps[index as usize] = slot + 1;
                            dirty[class.index()].insert(index / ENTRIES_PER_MAP as u64);
                        }
                    })?;
                    let mut writes = Vec::new();
                    for class in SizeClass::all() {
                        for &d in &dirty[class.index()] {
                            let page = {
                                let tables = self.tables.lock();
                                let maps = &tables.maps[class.index()];
                                let start = (d as usize * ENTRIES_PER_MAP).min(maps.len());
                                let end = (start + ENTRIES_PER_MAP).min(maps.len());
                                entries_page(&maps[start..end])
                            };
                            writes.push(self.write_page(
                                directory_pid(class, d),
                                page,
                                &run.placed,
                                move |slot| Placed::Directory(class, d, slot),
                            )?);
                        }
                    }
                    Phase::Directories(writes)
                }
                Phase::Directories(writes) => {
                    if let Some(c) = pending(writes) {
                        return Ok(IOResult::IO(IOCompletions(c)));
                    }
                    self.apply_placed(run, |placed, tables, space| {
                        if let Placed::Directory(class, d, slot) = placed {
                            let dirs = &mut tables.dirs[class.index()];
                            if dirs.len() <= d as usize {
                                dirs.resize(d as usize + 1, 0);
                            }
                            if dirs[d as usize] != 0 {
                                space.retired.push(dirs[d as usize] - 1);
                            }
                            dirs[d as usize] = slot + 1;
                        }
                    })?;
                    Phase::SyncMaps(
                        self.file
                            .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?,
                    )
                }
                Phase::SyncMaps(c) => {
                    if let Some(io) = waiting(c)? {
                        return Ok(io);
                    }
                    let sb = {
                        let tables = self.tables.lock();
                        Superblock {
                            seq: run.seq,
                            redo: run.redo,
                            map_next_slot: self.space.lock().next_slot,
                            classes: run
                                .classes
                                .iter()
                                .zip(&tables.dirs)
                                .map(|(snap, dirs)| {
                                    (
                                        snap.next_page,
                                        snap.next_slot,
                                        dirs.iter().map(|&s| s - 1).collect(),
                                    )
                                })
                                .collect(),
                        }
                    };
                    let buf = sb.encode();
                    page::seal(buf.as_mut_slice(), superblock_pid());
                    let at = (run.seq % SUPERBLOCKS) * PAGE as u64;
                    Phase::Superblock(self.file.pwrite(at, buf, Completion::new_write(|_| {}))?)
                }
                Phase::Superblock(c) => {
                    if let Some(io) = waiting(c)? {
                        return Ok(io);
                    }
                    Phase::SyncSuperblock(
                        self.file
                            .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?,
                    )
                }
                Phase::SyncSuperblock(c) => {
                    if let Some(io) = waiting(c)? {
                        return Ok(io);
                    }
                    // The checkpoint is durable: what it superseded may go.
                    self.seq.store(run.seq, Ordering::Release);
                    let mut erasing = bm.store().finish_checkpoint()?;
                    let slots = std::mem::take(&mut self.space.lock().retired);
                    for &slot in &slots {
                        let at = slot * PAGE as u64;
                        if self.file.punch_hole(at as usize, PAGE).is_err() {
                            let zeros = Arc::new(Buffer::new(vec![0; PAGE]));
                            erasing.push(self.file.pwrite(
                                at,
                                zeros,
                                Completion::new_write(|_| {}),
                            )?);
                        }
                    }
                    self.space.lock().free.extend(slots);
                    wal.release_before(run.redo)?;
                    Phase::Erase(erasing)
                }
                Phase::Erase(writes) => {
                    if let Some(c) = pending(writes) {
                        return Ok(IOResult::IO(IOCompletions(c)));
                    }
                    if let Some(e) = writes.iter().find_map(|c| c.get_error()) {
                        return Err(LimboError::CompletionError(e));
                    }
                    return Ok(IOResult::Done(Checkpointed {
                        seq: run.seq,
                        redo: run.redo,
                    }));
                }
            };
            run.phase = next;
        }
    }

    /// Writes a map file page to a fresh slot; `placed` hears where it landed.
    fn write_page(
        &self,
        pid: PageId,
        buf: Arc<Buffer>,
        placed: &Arc<Mutex<Vec<std::result::Result<Placed, CompletionError>>>>,
        what: impl Fn(u64) -> Placed + Send + Sync + 'static,
    ) -> Result<Completion> {
        page::seal(buf.as_mut_slice(), pid);
        let slot = self.space.lock().take();
        let into = placed.clone();
        let c = Completion::new_write(move |res| {
            into.lock().push(match res {
                Ok(n) if n as usize == PAGE => Ok(what(slot)),
                Ok(_) => Err(CompletionError::ShortWrite),
                Err(e) => Err(e),
            });
        });
        self.file.pwrite(slot * PAGE as u64, buf, c)
    }

    fn apply_placed(
        &self,
        run: &Run,
        mut apply: impl FnMut(Placed, &mut Tables, &mut MapSpace),
    ) -> Result<()> {
        let placed = std::mem::take(&mut *run.placed.lock());
        let mut tables = self.tables.lock();
        let mut space = self.space.lock();
        for p in placed {
            apply(
                p.map_err(LimboError::CompletionError)?,
                &mut tables,
                &mut space,
            );
        }
        Ok(())
    }

    /// The sequence number of the newest durable checkpoint.
    pub fn seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }
}

/// The wait for `c` while it is in flight, its error once it failed, nothing once it succeeded.
fn waiting(c: &Completion) -> Result<Option<IOResult<Checkpointed>>> {
    if !c.finished() {
        return Ok(Some(IOResult::IO(IOCompletions(c.clone()))));
    }
    match c.get_error() {
        Some(e) => Err(LimboError::CompletionError(e)),
        None => Ok(None),
    }
}

fn pending(writes: &[Completion]) -> Option<Completion> {
    writes.iter().find(|c| !c.finished()).cloned()
}

fn torn(what: &str, class: SizeClass, index: u64) -> LimboError {
    LimboError::Corrupt(format!(
        "the newest checkpoint's {what} page {index} of {class:?} does not read back whole"
    ))
}

fn read_page(
    io: &Arc<dyn IO>,
    file: &Arc<dyn File>,
    pid: PageId,
    slot: u64,
) -> Result<Option<Vec<u8>>> {
    let buf = Arc::new(Buffer::new_temporary(PAGE));
    let got = Arc::new(Mutex::new(None));
    let out = got.clone();
    let c = Completion::new_read(buf, move |res| match res {
        Ok((buf, n)) => {
            let whole = n as usize == PAGE && page::verify(buf.as_slice(), pid);
            *out.lock() = Some(whole.then(|| buf.as_slice().to_vec()));
            None
        }
        Err(e) => Some(e),
    });
    wait(io, file.pread(slot * PAGE as u64, c)?)?;
    let page = got.lock().take().flatten();
    Ok(page)
}

/// A database opened on this storage: everything recovery left consistent and checkpointed.
pub struct Opened {
    pub store: Arc<PageStore>,
    pub wal: Arc<Wal>,
    pub buffer: Arc<BufferManager>,
    pub checkpointer: Checkpointer,
    pub recovered: Recovered,
}

/// Opens the database at `base` (`{base}.pages.c*`, `{base}.log`, `{base}.map`): installs the
/// newest checkpoint's page table, recovers the log after its redo point, logs where recovery
/// wrote the pages it rebuilt, and checkpoints, which erases and frees whatever no page refers to
/// and cuts the recovered log.
pub fn open(io: &Arc<dyn IO>, base: &str, budget: usize, cooling_share: f64) -> Result<Opened> {
    let store = Arc::new(PageStore::open(io, &format!("{base}.pages"), false)?);
    let (checkpointer, redo) = Checkpointer::open(io, &format!("{base}.map"), &store)?;
    let log = io.open_file(&format!("{base}.log"), OpenFlags::Create, false)?;
    let mut recovered = wal::recover(io, &log, redo, &store, budget)?;
    let wal = Wal::open(io, log, redo, recovered.end)?;
    let written: Vec<Record> = recovered.take_written();
    if !written.is_empty() {
        let lsn = wal.append(&written)?;
        drive(io, || wal.wait_durable(lsn))?;
    }
    store.retire_unreferenced();
    let buffer = Arc::new(BufferManager::new(
        io.clone(),
        store.clone(),
        Some(wal.clone()),
        budget,
        cooling_share,
    )?);
    drive(io, || checkpointer.checkpoint(&buffer, &wal))?;
    Ok(Opened {
        store,
        wal,
        buffer,
        checkpointer,
        recovered,
    })
}

fn drive<T>(io: &Arc<dyn IO>, mut op: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
    loop {
        match op()? {
            IOResult::Done(v) => return Ok(v),
            IOResult::IO(IOCompletions(c)) => wait(io, c)?,
        }
    }
}

#[cfg(test)]
mod tests;
