//! The write-ahead log, on Turso's [`File`].
//!
//! A frame:
//!
//! ```text
//! magic: u32 | payload_len: u32 | payload | crc32c: u32
//! ```
//!
//! The CRC covers the frame's start offset, its header and its payload, so a frame read at any
//! other offset, a torn frame, or bytes past the end all read as the end of the log. A frame's
//! LSN is the offset just past it: a page stamped with LSN `l` may be written back once the log
//! is durable up to `l`.
//!
//! The payload is records about pages:
//!
//! - `NEW pid`: the page was created, zeroed.
//! - `RANGE pid offset len bytes`: bytes written at an offset in the page.
//! - `REMAP pid slot lsn`: the copy of the page holding every change up to `lsn` was written to
//!   `slot`. Pages are written out of place (see [`super::store`]), so this is how recovery finds
//!   a page's newest copy. It is appended once the copy has landed, after every record the copy
//!   holds, so a remap in the durable log never names a copy holding changes the log lost.
//!
//! No record holds a whole page: a copy is never overwritten, so none is ever torn, and recovery
//! starts from the newest copy that reads back whole as the copy its remap names. The record types
//! a transaction model needs (undo, logical operations) are added beside these by the layer that
//! owns them.
//!
//! **Durability.** At most one write and exactly one fsync are in flight, and they overlap. A
//! write takes every frame appended since the last one began; when it completes, the next write
//! starts at once if frames are waiting, while the fsync runs. An fsync covers the writes that had
//! completed when it started, so writes that land during it get the next fsync the moment it
//! ends; every caller waiting on an LSN an fsync covers is released by it. It is a chain of
//! completions, so it runs the same whether the backend completes inside the submission or later
//! from `IO::step`, and no lock is held across a submission.

use super::page::{self, PageId, HEADER_SIZE};
use super::store::{PageRead, PageStore};
use crate::io::{Buffer, Completion, File, FileSyncType, IO};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};
use std::collections::HashMap;

const FRAME_MAGIC: u32 = 0x554d_4252;
const FRAME_HEADER: usize = 8;
const FRAME_TRAILER: usize = 4;
const NEW: u8 = 1;
const RANGE: u8 = 2;
const REMAP: u8 = 3;

pub type Lsn = u64;

/// A change to one page, as logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    New {
        pid: PageId,
    },
    Range {
        pid: PageId,
        offset: u32,
        bytes: Vec<u8>,
    },
    Remap {
        pid: PageId,
        slot: u64,
        lsn: Lsn,
    },
}

impl Record {
    pub fn pid(&self) -> PageId {
        match self {
            Record::New { pid } | Record::Range { pid, .. } | Record::Remap { pid, .. } => *pid,
        }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Record::New { pid } => {
                out.push(NEW);
                out.extend_from_slice(&pid.raw().to_le_bytes());
            }
            Record::Remap { pid, slot, lsn } => {
                out.push(REMAP);
                out.extend_from_slice(&pid.raw().to_le_bytes());
                out.extend_from_slice(&slot.to_le_bytes());
                out.extend_from_slice(&lsn.to_le_bytes());
            }
            Record::Range { pid, offset, bytes } => {
                out.push(RANGE);
                out.extend_from_slice(&pid.raw().to_le_bytes());
                out.extend_from_slice(&offset.to_le_bytes());
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }

    fn decode_all(mut payload: &[u8]) -> Option<Vec<Record>> {
        fn take<'a>(p: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
            (p.len() >= n).then(|| {
                let (head, rest) = p.split_at(n);
                *p = rest;
                head
            })
        }
        let mut records = Vec::new();
        while !payload.is_empty() {
            let kind = take(&mut payload, 1)?[0];
            let pid = PageId::from_raw(u64::from_le_bytes(take(&mut payload, 8)?.try_into().ok()?));
            match kind {
                NEW => records.push(Record::New { pid }),
                REMAP => {
                    let slot = u64::from_le_bytes(take(&mut payload, 8)?.try_into().ok()?);
                    let lsn = u64::from_le_bytes(take(&mut payload, 8)?.try_into().ok()?);
                    records.push(Record::Remap { pid, slot, lsn });
                }
                RANGE => {
                    let offset = u32::from_le_bytes(take(&mut payload, 4)?.try_into().ok()?);
                    let len = u32::from_le_bytes(take(&mut payload, 4)?.try_into().ok()?) as usize;
                    if (offset as usize) < HEADER_SIZE || offset as usize + len > pid.class().size()
                    {
                        return None;
                    }
                    let bytes = take(&mut payload, len)?.to_vec();
                    records.push(Record::Range { pid, offset, bytes });
                }
                _ => return None,
            }
        }
        Some(records)
    }
}

fn frame_crc(start: u64, header: &[u8], payload: &[u8]) -> u32 {
    let crc = crc32c::crc32c(&start.to_le_bytes());
    let crc = crc32c::crc32c_append(crc, header);
    crc32c::crc32c_append(crc, payload)
}

fn encode_frame(start: u64, records: &[Record]) -> Vec<u8> {
    let mut payload = Vec::new();
    for r in records {
        r.encode(&mut payload);
    }
    let mut frame = Vec::with_capacity(FRAME_HEADER + payload.len() + FRAME_TRAILER);
    frame.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    let crc = frame_crc(start, &frame[..FRAME_HEADER], &payload);
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(&crc.to_le_bytes());
    frame
}

/// The frame at `bytes[0..]`, which sits at offset `at` in the log: its records and length, or
/// `None` at the end of the log. `Err(need)` when the frame runs past `bytes`, which need hold
/// `need` bytes to settle it.
fn parse_frame(at: u64, bytes: &[u8]) -> std::result::Result<Option<(Vec<Record>, usize)>, usize> {
    let Some(header) = bytes.get(..FRAME_HEADER) else {
        return Err(FRAME_HEADER);
    };
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != FRAME_MAGIC {
        return Ok(None);
    }
    let len = FRAME_HEADER
        + u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize
        + FRAME_TRAILER;
    let Some(frame) = bytes.get(..len) else {
        return Err(len);
    };
    let payload = &frame[FRAME_HEADER..len - FRAME_TRAILER];
    let crc = u32::from_le_bytes(frame[len - FRAME_TRAILER..].try_into().unwrap());
    if crc != frame_crc(at, header, payload) {
        return Ok(None);
    }
    Ok(Record::decode_all(payload).map(|records| (records, len)))
}

/// Waits for `c` the way Turso's open path does, by stepping the IO.
fn wait(io: &Arc<dyn IO>, c: Completion) -> Result<()> {
    io.wait_for_completion(c)
}

/// Reads `len` bytes at `at` (fewer at the end of the file).
fn read_at(io: &Arc<dyn IO>, file: &Arc<dyn File>, at: u64, len: usize) -> Result<Vec<u8>> {
    let buf = Arc::new(Buffer::new_temporary(len));
    let got = Arc::new(Mutex::new(Vec::new()));
    let out = got.clone();
    let c = Completion::new_read(buf, move |res| match res {
        Ok((buf, n)) => {
            *out.lock() = buf.as_slice()[..n.max(0) as usize].to_vec();
            None
        }
        Err(e) => Some(e),
    });
    let c = file.pread(at, c)?;
    wait(io, c)?;
    let bytes = std::mem::take(&mut *got.lock());
    Ok(bytes)
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Recovered {
    pub frames: u64,
    /// Pages the log has changes for.
    pub pages: u64,
    /// Changes skipped because the page's newest copy already held them.
    pub skipped: u64,
    /// Remaps naming a copy that did not read back whole as itself, passed over for an older one.
    pub stale_copies: u64,
    /// Pages recovery changed and wrote to fresh slots.
    pub rewritten: u64,
    pub end: Lsn,
    /// Copies recovery wrote, as remap records still to be logged.
    written: Vec<Record>,
}

/// Streams the frames of the log in `file`, calling `each(lsn, records)` for every valid frame,
/// and returns where the valid frames end.
fn stream(
    io: &Arc<dyn IO>,
    file: &Arc<dyn File>,
    mut each: impl FnMut(Lsn, Vec<Record>) -> Result<()>,
) -> Result<(u64, Lsn)> {
    let size = file.size()?;
    // Reads are as large as the largest page, so a frame of a page's worth of changes is read in
    // one piece more.
    let chunk =
        page::SizeClass::all().last().expect("classes").size() + FRAME_HEADER + FRAME_TRAILER;
    let mut window = Vec::new();
    let mut window_at = 0u64;
    let mut at = 0u64;
    let mut frames = 0u64;
    'log: while at < size {
        let mut need = FRAME_HEADER;
        let (records, len) = loop {
            let offset = (at - window_at) as usize;
            if window.len() < offset + need {
                let want = chunk.max(offset + need - window.len());
                let more = read_at(io, file, window_at + window.len() as u64, want)?;
                if more.is_empty() {
                    break 'log;
                }
                window.drain(..offset);
                window_at = at;
                window.extend_from_slice(&more);
                continue;
            }
            match parse_frame(at, &window[offset..]) {
                Ok(Some(frame)) => break frame,
                Ok(None) => break 'log,
                Err(more) => need = more,
            }
        };
        at += len as u64;
        frames += 1;
        each(at, records)?;
    }
    Ok((frames, at))
}

/// A page being brought up to date by recovery.
struct Redo {
    buf: Vec<u8>,
    /// Holds a whole page: its newest copy, or rebuilt from its creation.
    valid: bool,
    /// The LSN of the copy it started from.
    base: Lsn,
}

/// Recovers the log in `file` into `store` (ARIES analysis then redo), holding at most `budget`
/// bytes of pages at once. Analysis collects every page's remaps; redo starts each page the log
/// changes from its newest copy that reads back whole as the copy its remap names, and applies the
/// changes after it. Pages it changed are written out of place and synced; their remaps are in
/// [`Recovered`] for [`Wal::open_recovered`] to log before any old slot is reused.
fn recover(
    io: &Arc<dyn IO>,
    file: &Arc<dyn File>,
    store: &PageStore,
    budget: usize,
) -> Result<Recovered> {
    let mut remaps: HashMap<PageId, Vec<(u64, Lsn)>> = HashMap::new();
    let mut changed: HashMap<PageId, ()> = HashMap::new();
    let (frames, end) = stream(io, file, |_, records| {
        for r in records {
            match r {
                Record::Remap { pid, slot, lsn } => {
                    remaps.entry(pid).or_default().push((slot, lsn))
                }
                r => {
                    changed.insert(r.pid(), ());
                }
            }
        }
        Ok(())
    })?;
    let mut out = Recovered {
        frames,
        pages: changed.len() as u64,
        end,
        ..Recovered::default()
    };
    // Where every page's newest whole copy is, found as redo first needs it.
    let mut located: HashMap<PageId, Option<(u64, Lsn)>> = HashMap::new();
    let mut pages: HashMap<PageId, Redo> = HashMap::new();
    let mut held = 0usize;
    stream(io, file, |lsn, records| {
        for record in records {
            let pid = record.pid();
            if matches!(record, Record::Remap { .. }) {
                continue;
            }
            if !pages.contains_key(&pid) {
                if held + pid.class().size() > budget {
                    write_redone(io, store, &mut pages, &mut out.written)?;
                    held = 0;
                }
                let base =
                    newest_copy(io, store, pid, &remaps, &mut located, &mut out.stale_copies)?;
                pages.insert(pid, base);
                held += pid.class().size();
            }
            let redo = pages.get_mut(&pid).expect("inserted");
            if redo.valid && page::lsn(&redo.buf) >= lsn {
                out.skipped += 1;
                continue;
            }
            match record {
                Record::New { .. } => redo.buf.fill(0),
                Record::Range { offset, bytes, .. } => {
                    if !redo.valid {
                        return Err(LimboError::Corrupt(format!(
                            "page {pid:?} has changes in the log but no whole copy and no creation"
                        )));
                    }
                    redo.buf[offset as usize..offset as usize + bytes.len()]
                        .copy_from_slice(&bytes);
                }
                Record::Remap { .. } => unreachable!("skipped above"),
            }
            page::set_lsn(&mut redo.buf, lsn);
            redo.valid = true;
        }
        Ok(())
    })?;
    write_redone(io, store, &mut pages, &mut out.written)?;
    wait(io, store.sync()?)?;
    out.rewritten = out.written.len() as u64;
    // Install the page table: the copies recovery wrote, else each page's newest whole copy.
    let mut next_page = [0u64; page::CLASS_COUNT];
    let mut copies: Vec<Vec<(u64, u64)>> = vec![Vec::new(); page::CLASS_COUNT];
    let mut placed: HashMap<PageId, u64> = HashMap::new();
    for r in &out.written {
        if let Record::Remap { pid, slot, .. } = r {
            placed.insert(*pid, *slot);
        }
    }
    let all: Vec<PageId> = remaps.keys().chain(changed.keys()).copied().collect();
    for pid in all {
        let class = pid.class().index();
        next_page[class] = next_page[class].max(pid.number() + 1);
        if placed.contains_key(&pid) {
            continue;
        }
        if let Some((slot, _)) =
            newest_located(io, store, pid, &remaps, &mut located, &mut out.stale_copies)?
        {
            placed.insert(pid, slot);
        }
    }
    for (pid, slot) in placed {
        copies[pid.class().index()].push((pid.number(), slot));
    }
    for class in page::SizeClass::all() {
        store.restore(class, next_page[class.index()], &copies[class.index()]);
    }
    Ok(out)
}

/// The slot and LSN of `pid`'s newest copy that reads back whole as the copy its remap names.
fn newest_located(
    io: &Arc<dyn IO>,
    store: &PageStore,
    pid: PageId,
    remaps: &HashMap<PageId, Vec<(u64, Lsn)>>,
    located: &mut HashMap<PageId, Option<(u64, Lsn)>>,
    stale: &mut u64,
) -> Result<Option<(u64, Lsn)>> {
    if let Some(found) = located.get(&pid) {
        return Ok(*found);
    }
    let mut found = None;
    for &(slot, lsn) in remaps.get(&pid).into_iter().flatten().rev() {
        if let Some(buf) = read_copy(io, store, pid, slot)? {
            if page::lsn(&buf) == lsn {
                found = Some((slot, lsn));
                break;
            }
        }
        *stale += 1;
    }
    located.insert(pid, found);
    Ok(found)
}

/// `pid` as its newest whole copy left it, or zeroed and invalid if it has none.
fn newest_copy(
    io: &Arc<dyn IO>,
    store: &PageStore,
    pid: PageId,
    remaps: &HashMap<PageId, Vec<(u64, Lsn)>>,
    located: &mut HashMap<PageId, Option<(u64, Lsn)>>,
    stale: &mut u64,
) -> Result<Redo> {
    if let Some((slot, lsn)) = newest_located(io, store, pid, remaps, located, stale)? {
        if let Some(buf) = read_copy(io, store, pid, slot)? {
            return Ok(Redo {
                buf,
                valid: true,
                base: lsn,
            });
        }
    }
    Ok(Redo {
        buf: vec![0; pid.class().size()],
        valid: false,
        base: 0,
    })
}

fn read_copy(
    io: &Arc<dyn IO>,
    store: &PageStore,
    pid: PageId,
    slot: u64,
) -> Result<Option<Vec<u8>>> {
    let got = Arc::new(Mutex::new(None));
    let out = got.clone();
    let c = store.read_slot(pid, slot, move |read| {
        *out.lock() = Some(match read {
            PageRead::Valid(buf) => Ok(Some(buf.as_slice().to_vec())),
            PageRead::Invalid => Ok(None),
            PageRead::Failed(e) => Err(e),
        })
    })?;
    wait(io, c)?;
    let copy = got.lock().take().expect("the read completed")?;
    Ok(copy)
}

/// Writes every page recovery changed to a fresh slot, noting its remap; pages it only read are
/// dropped, their newest copy still current.
fn write_redone(
    io: &Arc<dyn IO>,
    store: &PageStore,
    pages: &mut HashMap<PageId, Redo>,
    written: &mut Vec<Record>,
) -> Result<()> {
    let landed = Arc::new(Mutex::new(Vec::new()));
    let mut writes = Vec::new();
    for (pid, redo) in pages.drain() {
        if !redo.valid || page::lsn(&redo.buf) == redo.base {
            continue;
        }
        let lsn = page::lsn(&redo.buf);
        let into = landed.clone();
        writes.push(
            store.write(pid, Arc::new(Buffer::new(redo.buf)), move |res| {
                into.lock()
                    .push(res.map(|slot| Record::Remap { pid, slot, lsn }));
            })?,
        );
    }
    for c in writes {
        wait(io, c)?;
    }
    for r in std::mem::take(&mut *landed.lock()) {
        written.push(r.map_err(LimboError::CompletionError)?);
    }
    Ok(())
}

struct Tail {
    /// Appended bytes not yet handed to a write, starting at `pending_at`.
    pending: Vec<u8>,
    pending_at: u64,
    end: u64,
    /// A write is in flight.
    writing: bool,
    /// Where the completed writes end.
    written: Lsn,
    /// The fsync in flight, and where the writes it covers end.
    syncing: Option<Lsn>,
    /// Callers waiting for the log to be durable up to an LSN.
    waiters: Vec<(Lsn, Completion)>,
    failed: Option<CompletionError>,
}

pub struct Wal {
    file: Arc<dyn File>,
    tail: Mutex<Tail>,
    durable: AtomicU64,
    syncs: AtomicU64,
    /// Writes started while an fsync was in flight.
    overlapped: AtomicU64,
}

impl Wal {
    /// Recovers the log into `store`, opens it for appending, and logs where recovery wrote the
    /// pages it rebuilt. Only once those remaps are durable may the slots of the copies they
    /// replace be reused, so the store's free slots are installed last.
    pub fn open_recovered(
        io: &Arc<dyn IO>,
        file: Arc<dyn File>,
        store: &PageStore,
        budget: usize,
    ) -> Result<(Arc<Self>, Recovered)> {
        let mut recovered = recover(io, &file, store, budget)?;
        let wal = Self::open(io, file, recovered.end)?;
        let written = std::mem::take(&mut recovered.written);
        if !written.is_empty() {
            let lsn = wal.append(&written)?;
            let mut done = wal.wait_durable(lsn)?;
            while let IOResult::IO(IOCompletions(c)) = done {
                wait(io, c)?;
                done = wal.wait_durable(lsn)?;
            }
        }
        store.recycle();
        Ok((wal, recovered))
    }

    /// Opens the log for appending at `end`, where [`recover`] found its valid frames end, and
    /// cuts whatever follows: a torn frame must not read as the start of the next one.
    pub fn open(io: &Arc<dyn IO>, file: Arc<dyn File>, end: Lsn) -> Result<Arc<Self>> {
        if file.size()? != end {
            wait(io, file.truncate(end, Completion::new_trunc(|_| {}))?)?;
            wait(
                io,
                file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?,
            )?;
        }
        Ok(Arc::new(Self {
            file,
            tail: Mutex::new(Tail {
                pending: Vec::new(),
                pending_at: end,
                end,
                writing: false,
                written: end,
                syncing: None,
                waiters: Vec::new(),
                failed: None,
            }),
            durable: AtomicU64::new(end),
            syncs: AtomicU64::new(0),
            overlapped: AtomicU64::new(0),
        }))
    }

    pub fn durable(&self) -> Lsn {
        self.durable.load(Ordering::Acquire)
    }

    pub fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// Writes that started while an fsync was in flight.
    pub fn overlapped(&self) -> u64 {
        self.overlapped.load(Ordering::Relaxed)
    }

    /// Appends one frame and returns its LSN. Nothing is written until someone waits for it or
    /// [`flush`](Self::flush) is called.
    pub fn append(&self, records: &[Record]) -> Result<Lsn> {
        let mut tail = self.tail.lock();
        if let Some(e) = tail.failed {
            return Err(LimboError::CompletionError(e));
        }
        let frame = encode_frame(tail.end, records);
        tail.end += frame.len() as u64;
        tail.pending.extend_from_slice(&frame);
        Ok(tail.end)
    }

    /// Done once the log is durable up to `lsn`; until then, the completion to wait on.
    pub fn wait_durable(self: &Arc<Self>, lsn: Lsn) -> Result<IOResult<()>> {
        match self.durable_completion(lsn) {
            None => Ok(IOResult::Done(())),
            Some(c) if c.finished() => match c.get_error() {
                Some(e) => Err(LimboError::CompletionError(e)),
                None => Ok(IOResult::Done(())),
            },
            Some(c) => Ok(IOResult::IO(IOCompletions(c))),
        }
    }

    /// A completion that finishes once the log is durable up to `lsn`, or `None` if it is.
    pub fn durable_completion(self: &Arc<Self>, lsn: Lsn) -> Option<Completion> {
        if self.durable() >= lsn {
            return None;
        }
        let c = Completion::new_wait();
        {
            let mut tail = self.tail.lock();
            if let Some(e) = tail.failed {
                c.error(e);
                return Some(c);
            }
            if self.durable() >= lsn {
                return None;
            }
            tail.waiters.push((lsn, c.clone()));
        }
        self.flush();
        Some(c)
    }

    /// Starts writing what is appended, unless a write is in flight: its completion starts the
    /// next one.
    pub fn flush(self: &Arc<Self>) {
        let (bytes, at) = {
            let mut tail = self.tail.lock();
            if tail.writing || tail.pending.is_empty() || tail.failed.is_some() {
                return;
            }
            tail.writing = true;
            if tail.syncing.is_some() {
                self.overlapped.fetch_add(1, Ordering::Relaxed);
            }
            let at = tail.pending_at;
            tail.pending_at = tail.end;
            (std::mem::take(&mut tail.pending), at)
        };
        let len = bytes.len() as u64;
        let me = self.clone();
        let written = Completion::new_write(move |res| match res {
            Ok(n) if n as u64 == len => me.written_to(at + len),
            Ok(_) => me.fail(CompletionError::ShortWrite),
            Err(e) => me.fail(e),
        });
        if let Err(e) = self.file.pwrite(at, Arc::new(Buffer::new(bytes)), written) {
            self.fail(io_failure(&e));
        }
    }

    fn written_to(self: &Arc<Self>, end: Lsn) {
        {
            let mut tail = self.tail.lock();
            tail.writing = false;
            tail.written = end;
        }
        self.sync();
        self.flush();
    }

    /// Starts an fsync covering the completed writes, unless one is in flight or they are
    /// durable: the fsync in flight starts the next when it ends.
    fn sync(self: &Arc<Self>) {
        let covers = {
            let mut tail = self.tail.lock();
            if tail.syncing.is_some() || tail.failed.is_some() || tail.written <= self.durable() {
                return;
            }
            tail.syncing = Some(tail.written);
            tail.written
        };
        let me = self.clone();
        let synced = Completion::new_sync(move |res| match res {
            Ok(_) => me.durable_to(covers),
            Err(e) => me.fail(e),
        });
        if let Err(e) = self.file.sync(synced, FileSyncType::Fsync) {
            self.fail(io_failure(&e));
        }
    }

    fn durable_to(self: &Arc<Self>, end: Lsn) {
        let released = {
            let mut tail = self.tail.lock();
            self.durable.store(end, Ordering::Release);
            self.syncs.fetch_add(1, Ordering::Relaxed);
            tail.syncing = None;
            let (done, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut tail.waiters)
                .into_iter()
                .partition(|(lsn, _)| *lsn <= end);
            tail.waiters = waiting;
            done
        };
        for (_, c) in released {
            c.complete(0);
        }
        // Writes that completed during this fsync.
        self.sync();
        self.flush();
    }

    /// Whether the bytes in flight reached the disk is unknown: nothing may follow them, and
    /// everyone waiting is told.
    fn fail(&self, e: CompletionError) {
        let waiters = {
            let mut tail = self.tail.lock();
            tail.failed = Some(e);
            tail.writing = false;
            tail.syncing = None;
            std::mem::take(&mut tail.waiters)
        };
        for (_, c) in waiters {
            c.error(e);
        }
    }
}

fn io_failure(e: &LimboError) -> CompletionError {
    match e {
        LimboError::CompletionError(e) => *e,
        _ => CompletionError::IOError(std::io::ErrorKind::Other, "log write submission"),
    }
}

#[cfg(test)]
mod tests;
