//! The write-ahead log.
//!
//! A transaction's changes reach the log only when it commits, as one frame, so everything in
//! the log is committed and recovery is redo only. A frame:
//!
//! ```text
//! magic: u32 | payload_len: u32 | payload | crc32c: u32
//! ```
//!
//! The CRC covers the frame's start offset, the header and the payload, so a frame read at any
//! other offset, a torn frame, or bytes past the end all read as the end of the log.
//!
//! A frame's LSN is the offset just past it: a page stamped with LSN `l` is safe to write back
//! once the log is durable up to `l`.
//!
//! The payload is a sequence of records, each about one page:
//!
//! - `IMAGE pid bytes`: the whole page. A page's first record after the redo point is always an
//!   image, so a write-back torn by a crash is rebuilt from the log, never read.
//! - `RANGE pid offset len bytes`: bytes written at an offset in the page.
//!
//! Durability follows one rule: while bytes are appended but not durable, exactly one write and
//! fsync is in flight, and the next starts the moment it ends if more were appended meanwhile.
//! Every waiter is released by the first fsync that covers its frame.

use crate::page::{self, PageId, HEADER_SIZE};
use crate::store::{PageStore, ReadError};
use parking_lot::{Condvar, Mutex};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const FRAME_MAGIC: u32 = 0x554d_4252;
const FRAME_HEADER: usize = 8;
const FRAME_TRAILER: usize = 4;
const IMAGE: u8 = 1;
const RANGE: u8 = 2;

pub type Lsn = u64;

/// A change to one page, as logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Image { pid: PageId, bytes: Vec<u8> },
    Range { pid: PageId, offset: u32, bytes: Vec<u8> },
}

impl Record {
    pub fn pid(&self) -> PageId {
        match self {
            Record::Image { pid, .. } | Record::Range { pid, .. } => *pid,
        }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Record::Image { pid, bytes } => {
                debug_assert_eq!(bytes.len(), pid.class().size());
                out.push(IMAGE);
                out.extend_from_slice(&pid.raw().to_le_bytes());
                out.extend_from_slice(bytes);
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
                IMAGE => {
                    let bytes = take(&mut payload, pid.class().size())?.to_vec();
                    records.push(Record::Image { pid, bytes });
                }
                RANGE => {
                    let offset = u32::from_le_bytes(take(&mut payload, 4)?.try_into().ok()?);
                    let len = u32::from_le_bytes(take(&mut payload, 4)?.try_into().ok()?) as usize;
                    if (offset as usize) < HEADER_SIZE || offset as usize + len > pid.class().size() {
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

/// The frames of a log, from its start to its first invalid frame.
fn scan(log: &[u8]) -> (Vec<(Lsn, Vec<Record>)>, u64) {
    let mut frames = Vec::new();
    let mut at = 0usize;
    loop {
        let Some(header) = log.get(at..at + FRAME_HEADER) else { break };
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let end = at + FRAME_HEADER + len + FRAME_TRAILER;
        if magic != FRAME_MAGIC || end > log.len() {
            break;
        }
        let payload = &log[at + FRAME_HEADER..end - FRAME_TRAILER];
        let crc = u32::from_le_bytes(log[end - FRAME_TRAILER..end].try_into().unwrap());
        if crc != frame_crc(at as u64, header, payload) {
            break;
        }
        let Some(records) = Record::decode_all(payload) else { break };
        frames.push((end as u64, records));
        at = end;
    }
    (frames, at as u64)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Recovered {
    pub frames: u64,
    pub pages: u64,
    /// Records skipped because the page on disk already held them.
    pub skipped: u64,
    pub end: Lsn,
}

#[derive(Debug)]
pub enum RecoveryError {
    Io(io::Error),
    /// A page whose copy on disk is invalid has no image in the log to rebuild it from.
    Unrecoverable(PageId),
}

impl From<io::Error> for RecoveryError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// Replays the log at `path` into `store`: every page the log names is brought up to its last
/// committed state, written back and synced. The log is left as it was, cut at its first invalid
/// frame.
pub fn recover(path: &Path, store: &PageStore) -> Result<Recovered, RecoveryError> {
    let log = match std::fs::read(path) {
        Ok(log) => log,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Recovered::default()),
        Err(e) => return Err(e.into()),
    };
    let (frames, end) = scan(&log);
    let mut by_page: BTreeMap<PageId, Vec<(Lsn, &Record)>> = BTreeMap::new();
    for (lsn, records) in &frames {
        for r in records {
            by_page.entry(r.pid()).or_default().push((*lsn, r));
        }
    }
    let mut out = Recovered {
        frames: frames.len() as u64,
        pages: by_page.len() as u64,
        end,
        ..Recovered::default()
    };
    for (pid, records) in by_page {
        let mut buf = vec![0u8; pid.class().size()];
        let mut valid = match store.read(pid, &mut buf) {
            Ok(()) => true,
            Err(ReadError::Invalid(_)) => false,
            Err(ReadError::Io(e)) => return Err(e.into()),
        };
        for (lsn, record) in records {
            if valid && page::lsn(&buf) >= lsn {
                out.skipped += 1;
                continue;
            }
            match record {
                Record::Image { bytes, .. } => buf.copy_from_slice(bytes),
                Record::Range { offset, bytes, .. } => {
                    if !valid {
                        return Err(RecoveryError::Unrecoverable(pid));
                    }
                    buf[*offset as usize..*offset as usize + bytes.len()].copy_from_slice(bytes);
                }
            }
            page::set_lsn(&mut buf, lsn);
            valid = true;
        }
        store.write(pid, &mut buf)?;
    }
    store.sync()?;
    Ok(out)
}

struct Tail {
    /// Appended bytes not yet handed to a write, starting at `pending_at`.
    pending: Vec<u8>,
    pending_at: u64,
    end: u64,
    flushing: bool,
    failed: Option<io::ErrorKind>,
}

pub struct Wal {
    file: File,
    tail: Mutex<Tail>,
    flushed: Condvar,
    durable: AtomicU64,
    redo_point: AtomicU64,
    syncs: AtomicU64,
}

impl Wal {
    /// Opens the log at `path` for appending after its last valid frame. Run [`recover`] first.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut log = Vec::new();
        io::Read::read_to_end(&mut &file, &mut log)?;
        let (_, end) = scan(&log);
        // Whatever follows the last valid frame is a torn frame: cut it so it cannot be read as
        // the start of the next one.
        file.set_len(end)?;
        file.sync_all()?;
        Ok(Self {
            file,
            tail: Mutex::new(Tail {
                pending: Vec::new(),
                pending_at: end,
                end,
                flushing: false,
                failed: None,
            }),
            flushed: Condvar::new(),
            durable: AtomicU64::new(end),
            redo_point: AtomicU64::new(0),
            syncs: AtomicU64::new(0),
        })
    }

    /// LSN up to which a page's changes are covered by the log from its start: a page whose LSN
    /// is at or below it gets an image on its next change.
    pub fn redo_point(&self) -> Lsn {
        self.redo_point.load(Ordering::Relaxed)
    }

    pub fn durable(&self) -> Lsn {
        self.durable.load(Ordering::Acquire)
    }

    pub fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// Appends one frame; its LSN is returned. It is durable once [`wait_durable`] returns.
    pub fn append(&self, records: &[Record]) -> io::Result<Lsn> {
        let mut tail = self.tail.lock();
        if let Some(kind) = tail.failed {
            return Err(kind.into());
        }
        let frame = encode_frame(tail.end, records);
        tail.end += frame.len() as u64;
        tail.pending.extend_from_slice(&frame);
        Ok(tail.end)
    }

    /// Returns once the log is durable up to `lsn`, writing and syncing it if no one else is.
    pub fn wait_durable(&self, lsn: Lsn) -> io::Result<()> {
        let mut tail = self.tail.lock();
        loop {
            if self.durable() >= lsn {
                return Ok(());
            }
            if let Some(kind) = tail.failed {
                return Err(kind.into());
            }
            if tail.flushing {
                self.flushed.wait(&mut tail);
                continue;
            }
            tail.flushing = true;
            let bytes = std::mem::take(&mut tail.pending);
            let at = tail.pending_at;
            tail.pending_at = tail.end;
            drop(tail);
            let done = self
                .file
                .write_all_at(&bytes, at)
                .and_then(|()| self.file.sync_data());
            tail = self.tail.lock();
            tail.flushing = false;
            match done {
                Ok(()) => {
                    self.durable.store(at + bytes.len() as u64, Ordering::Release);
                    self.syncs.fetch_add(1, Ordering::Relaxed);
                }
                // Whether the bytes reached the disk is unknown: no later frame may follow them.
                Err(e) => tail.failed = Some(e.kind()),
            }
            self.flushed.notify_all();
        }
    }
}

#[cfg(test)]
mod tests;
