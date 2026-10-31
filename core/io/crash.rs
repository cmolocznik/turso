//! A memory [`IO`] for crash testing.
//!
//! It keeps two images of every file: what the OS shows now, and what has reached the disk.
//! Writes, syncs and truncations are submitted and complete one at a time, in a random order, at
//! [`IO::step`], as an asynchronous backend completes them. [`CrashIO::crash`] returns what a
//! crash at this moment could leave, as a fresh `CrashIO` holding only that:
//!
//! - writes made durable (a sync that covered them, or [`File::pwrite_durable`]) survive;
//! - every other write, completed or still in flight, reaches the disk sector by sector: each
//!   [`SECTOR`] keeps either its durable content or its content after some prefix of the writes
//!   that touched it, the way a page cache writes back the newest contents at any time;
//! - a file's size, and a file's existence in its directory, survive only if a sync made them
//!   durable, or by chance.
//!
//! Faults can be injected: IO errors at a chosen rate, and a capacity past which writes fail with
//! no space. A failed sync loses the writes it covered, as Linux does: they stay visible to reads
//! and never reach the disk.
//!
//! A handle opened with [`OpenFlags::Direct`] refuses IO whose buffer, offset or length is not a
//! multiple of [`DIRECT_IO_ALIGNMENT`], as Linux does.
//!
//! Every choice comes from a seeded generator, so a run and its crashes repeat exactly.

use super::{Buffer, Completion, File, FileId, FileSyncType, OpenFlags, DIRECT_IO_ALIGNMENT, IO};
use crate::io::clock::{Clock, DefaultClock, MonotonicInstant, WallClockInstant};
use crate::sync::Mutex;
use crate::{CompletionError, LimboError, Result};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

/// The unit a crash tears a write at.
pub const SECTOR: usize = 512;

/// Chances, from 0 to 1, that an operation fails with an IO error.
#[derive(Debug, Clone, Copy, Default)]
pub struct Faults {
    pub read: f64,
    pub write: f64,
    pub sync: f64,
}

pub struct CrashIO {
    state: Arc<Mutex<State>>,
}

struct State {
    /// Every path that exists now or exists on disk.
    paths: BTreeMap<String, Entry>,
    in_flight: Vec<Op>,
    rng: Rng,
    faults: Faults,
    capacity: Option<u64>,
    next_seq: u64,
    durable_writes: bool,
    /// Every write completed, per path: where and how long, in completion order.
    writes: BTreeMap<String, Vec<(u64, usize)>>,
}

struct Entry {
    data: Arc<Mutex<FileData>>,
    /// The file exists now.
    exists: bool,
    /// The file's existence is durable: its directory was synced since it was created.
    durable: bool,
}

#[derive(Default)]
struct FileData {
    /// What the OS shows.
    volatile: Vec<u8>,
    /// What has reached the disk.
    durable: Vec<u8>,
    /// Changes completed since they last became durable, oldest first.
    unsynced: Vec<Change>,
}

struct Change {
    seq: u64,
    kind: ChangeKind,
}

enum ChangeKind {
    Write { pos: u64, bytes: Vec<u8> },
    Resize { len: u64 },
}

enum OpKind {
    Read {
        pos: u64,
    },
    Write {
        pos: u64,
        buffer: Arc<Buffer>,
        durable: bool,
    },
    Sync {
        covers: u64,
    },
    Truncate {
        len: u64,
    },
}

struct Op {
    path: String,
    file: Arc<Mutex<FileData>>,
    kind: OpKind,
    completion: Completion,
}

/// splitmix64: small, and the same everywhere.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && (self.next() as f64 / u64::MAX as f64) < p
    }

    fn coin(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

impl CrashIO {
    pub fn new(seed: u64) -> Self {
        Self::with_paths(BTreeMap::new(), seed)
    }

    fn with_paths(paths: BTreeMap<String, Entry>, seed: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                paths,
                in_flight: Vec::new(),
                rng: Rng(seed),
                faults: Faults::default(),
                capacity: None,
                next_seq: 0,
                durable_writes: true,
                writes: BTreeMap::new(),
            })),
        }
    }

    pub fn set_faults(&self, faults: Faults) {
        self.state.lock().faults = faults;
    }

    /// Total bytes the files may hold; writes past it fail with no space.
    pub fn set_capacity(&self, bytes: Option<u64>) {
        self.state.lock().capacity = bytes;
    }

    /// Whether files offer [`File::pwrite_durable`]; without it callers must sync.
    pub fn set_durable_writes(&self, supported: bool) {
        self.state.lock().durable_writes = supported;
    }

    /// Every write completed to `path` so far: where and how long, in completion order.
    pub fn writes_to(&self, path: &str) -> Vec<(u64, usize)> {
        self.state
            .lock()
            .writes
            .get(path)
            .cloned()
            .unwrap_or_default()
    }

    /// Operations submitted and not completed yet.
    pub fn in_flight(&self) -> usize {
        self.state.lock().in_flight.len()
    }

    /// What a crash now could leave on disk, chosen by `seed`, as a fresh IO holding only that.
    /// This IO carries on unchanged.
    pub fn crash(&self, seed: u64) -> CrashIO {
        let state = self.state.lock();
        let mut rng = Rng(seed);
        let mut paths = BTreeMap::new();
        for (path, entry) in &state.paths {
            let on_disk = if entry.durable {
                // Removed since the directory was last synced: the removal may not have stuck.
                entry.exists || rng.coin()
            } else {
                entry.exists && rng.coin()
            };
            if !on_disk {
                continue;
            }
            let data = entry.data.lock();
            let in_flight = state
                .in_flight
                .iter()
                .filter(|op| Arc::ptr_eq(&op.file, &entry.data));
            let image = survivor(&data, in_flight, &mut rng);
            paths.insert(
                path.clone(),
                Entry {
                    data: Arc::new(Mutex::new(FileData {
                        volatile: image.clone(),
                        durable: image,
                        unsynced: Vec::new(),
                    })),
                    exists: true,
                    durable: true,
                },
            );
        }
        let crashed = CrashIO::with_paths(paths, rng.next());
        crashed.state.lock().durable_writes = state.durable_writes;
        crashed
    }

    /// Completes one operation in flight, chosen at random. Callbacks run with no lock held.
    fn complete_one(&self) {
        let (op, outcome) = {
            let mut state = self.state.lock();
            if state.in_flight.is_empty() {
                return;
            }
            let count = state.in_flight.len();
            let at = state.rng.below(count);
            let op = state.in_flight.swap_remove(at);
            let outcome = state.apply(&op);
            (op, outcome)
        };
        match outcome {
            Ok(n) => op.completion.complete(n),
            Err(e) => op.completion.error(e),
        }
    }

    fn submit(
        &self,
        path: &str,
        file: &Arc<Mutex<FileData>>,
        kind: OpKind,
        completion: Completion,
    ) -> Completion {
        let mut state = self.state.lock();
        let kind = match kind {
            OpKind::Sync { .. } => OpKind::Sync {
                covers: state.next_seq,
            },
            kind => kind,
        };
        state.in_flight.push(Op {
            path: path.to_string(),
            file: file.clone(),
            kind,
            completion: completion.clone(),
        });
        completion
    }
}

impl State {
    fn seq(&mut self) -> u64 {
        self.next_seq += 1;
        self.next_seq - 1
    }

    fn used(&self) -> u64 {
        self.paths
            .values()
            .filter(|e| e.exists)
            .map(|e| e.data.lock().volatile.len() as u64)
            .sum()
    }

    /// Carries out `op`; its result is what its completion reports.
    fn apply(&mut self, op: &Op) -> std::result::Result<i32, CompletionError> {
        let faults = self.faults;
        match &op.kind {
            OpKind::Read { pos } => {
                if self.rng.chance(faults.read) {
                    return Err(injected("read"));
                }
                let data = op.file.lock();
                let buf = op.completion.as_read().buf();
                let out = buf.as_mut_slice();
                let start = (*pos as usize).min(data.volatile.len());
                let n = out.len().min(data.volatile.len() - start);
                out[..n].copy_from_slice(&data.volatile[start..start + n]);
                Ok(n as i32)
            }
            OpKind::Write {
                pos,
                buffer,
                durable,
            } => {
                if self.rng.chance(faults.write) {
                    return Err(injected("write"));
                }
                let bytes = buffer.as_slice();
                let end = *pos + bytes.len() as u64;
                let grows = end.saturating_sub(op.file.lock().volatile.len() as u64);
                if let Some(capacity) = self.capacity {
                    if self.used() + grows > capacity {
                        return Err(CompletionError::IOError(ErrorKind::StorageFull, "write"));
                    }
                }
                let seq = self.seq();
                self.writes
                    .entry(op.path.clone())
                    .or_default()
                    .push((*pos, bytes.len()));
                let mut data = op.file.lock();
                write_at(&mut data.volatile, *pos, bytes);
                if *durable {
                    write_at(&mut data.durable, *pos, bytes);
                    // A direct write supersedes what the page cache held for the same bytes.
                    forget_unsynced(&mut data.unsynced, *pos, end);
                } else {
                    data.unsynced.push(Change {
                        seq,
                        kind: ChangeKind::Write {
                            pos: *pos,
                            bytes: bytes.to_vec(),
                        },
                    });
                }
                Ok(bytes.len() as i32)
            }
            OpKind::Sync { covers } => {
                let failed = self.rng.chance(faults.sync);
                let mut data = op.file.lock();
                let (covered, later): (Vec<_>, Vec<_>) = std::mem::take(&mut data.unsynced)
                    .into_iter()
                    .partition(|c| c.seq < *covers);
                data.unsynced = later;
                if failed {
                    // Linux marks the pages clean after a failed writeback: those writes are
                    // gone from the disk's future but stay visible.
                    return Err(injected("sync"));
                }
                for change in covered {
                    change.apply(&mut data.durable);
                }
                Ok(0)
            }
            OpKind::Truncate { len } => {
                let seq = self.seq();
                let mut data = op.file.lock();
                data.volatile.resize(*len as usize, 0);
                data.unsynced.push(Change {
                    seq,
                    kind: ChangeKind::Resize { len: *len },
                });
                Ok(0)
            }
        }
    }
}

impl Change {
    fn apply(&self, image: &mut Vec<u8>) {
        match &self.kind {
            ChangeKind::Write { pos, bytes } => write_at(image, *pos, bytes),
            ChangeKind::Resize { len } => image.resize(*len as usize, 0),
        }
    }
}

fn injected(what: &'static str) -> CompletionError {
    CompletionError::IOError(ErrorKind::Other, what)
}

fn write_at(image: &mut Vec<u8>, pos: u64, bytes: &[u8]) {
    let end = pos as usize + bytes.len();
    if image.len() < end {
        image.resize(end, 0);
    }
    image[pos as usize..end].copy_from_slice(bytes);
}

/// Cuts `[start, end)` out of every unsynced write.
fn forget_unsynced(unsynced: &mut Vec<Change>, start: u64, end: u64) {
    let mut kept = Vec::with_capacity(unsynced.len());
    for change in unsynced.drain(..) {
        let ChangeKind::Write { pos, bytes } = &change.kind else {
            kept.push(change);
            continue;
        };
        let (pos, write_end) = (*pos, *pos + bytes.len() as u64);
        if write_end <= start || pos >= end {
            kept.push(change);
            continue;
        }
        if pos < start {
            kept.push(Change {
                seq: change.seq,
                kind: ChangeKind::Write {
                    pos,
                    bytes: bytes[..(start - pos) as usize].to_vec(),
                },
            });
        }
        if write_end > end {
            kept.push(Change {
                seq: change.seq,
                kind: ChangeKind::Write {
                    pos: end,
                    bytes: bytes[(end - pos) as usize..].to_vec(),
                },
            });
        }
    }
    *unsynced = kept;
}

/// The image of one file a crash could leave.
fn survivor<'a>(
    data: &FileData,
    in_flight: impl Iterator<Item = &'a Op>,
    rng: &mut Rng,
) -> Vec<u8> {
    // The size: durable, or as some unsynced change left it.
    let mut sizes = vec![data.durable.len()];
    let mut image = data.durable.clone();
    // Each sector's contents after each unsynced change that touched it, oldest first.
    let mut versions: BTreeMap<usize, Vec<Vec<u8>>> = BTreeMap::new();
    for change in &data.unsynced {
        change.apply(&mut image);
        sizes.push(image.len());
        let (from, to) = match &change.kind {
            ChangeKind::Write { pos, bytes } => (*pos as usize, *pos as usize + bytes.len()),
            ChangeKind::Resize { .. } => continue,
        };
        for sector in from / SECTOR..to.div_ceil(SECTOR) {
            let at = sector * SECTOR;
            let end = (at + SECTOR).min(image.len());
            versions
                .entry(sector)
                .or_default()
                .push(image[at..end].to_vec());
        }
    }
    let size = sizes[rng.below(sizes.len())];
    let mut out = data.durable.clone();
    out.resize(size.max(out.len()), 0);
    for (sector, versions) in versions {
        let taken = rng.below(versions.len() + 1);
        if taken > 0 {
            let at = sector * SECTOR;
            let version = &versions[taken - 1];
            if out.len() < at + version.len() {
                out.resize(at + version.len(), 0);
            }
            out[at..at + version.len()].copy_from_slice(version);
        }
    }
    for op in in_flight {
        if let OpKind::Write { pos, buffer, .. } = &op.kind {
            let bytes = buffer.as_slice();
            for (i, chunk) in bytes.chunks(SECTOR).enumerate() {
                if rng.coin() {
                    write_at(&mut out, *pos + (i * SECTOR) as u64, chunk);
                }
            }
        }
    }
    out.truncate(size);
    out
}

impl Clock for CrashIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for CrashIO {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        let mut state = self.state.lock();
        let data = match state.paths.get_mut(path) {
            Some(entry) if entry.exists => entry.data.clone(),
            _ if !flags.contains(OpenFlags::Create) => {
                return Err(CompletionError::IOError(ErrorKind::NotFound, "open").into())
            }
            Some(entry) => {
                // Removed, and now created again.
                entry.exists = true;
                entry.data = Arc::new(Mutex::new(FileData::default()));
                entry.data.clone()
            }
            None => {
                let data = Arc::new(Mutex::new(FileData::default()));
                state.paths.insert(
                    path.to_string(),
                    Entry {
                        data: data.clone(),
                        exists: true,
                        durable: false,
                    },
                );
                data
            }
        };
        Ok(Arc::new(CrashFile {
            io: CrashIO {
                state: self.state.clone(),
            },
            path: path.to_string(),
            data,
            direct: flags.contains(OpenFlags::Direct),
        }))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        let mut state = self.state.lock();
        match state.paths.get_mut(path) {
            Some(entry) if entry.exists => {
                entry.exists = false;
                if !entry.durable {
                    state.paths.remove(path);
                }
                Ok(())
            }
            _ => Err(CompletionError::IOError(ErrorKind::NotFound, "remove").into()),
        }
    }

    fn sync_directory(&self, dir: &str) -> Result<()> {
        let mut state = self.state.lock();
        state.paths.retain(|path, entry| {
            if Path::new(path).parent() != Some(Path::new(dir)) {
                return true;
            }
            entry.durable = entry.exists;
            entry.exists
        });
        Ok(())
    }

    fn step(&self) -> Result<()> {
        self.complete_one();
        Ok(())
    }

    fn file_id(&self, path: &str) -> Result<FileId> {
        Ok(FileId::from_path_hash(path))
    }
}

pub struct CrashFile {
    io: CrashIO,
    path: String,
    data: Arc<Mutex<FileData>>,
    direct: bool,
}

impl CrashFile {
    fn check_aligned(&self, pos: u64, buffer: &Buffer) -> Result<()> {
        let aligned = |n: usize| n.is_multiple_of(DIRECT_IO_ALIGNMENT);
        if self.direct
            && !(aligned(pos as usize)
                && aligned(buffer.len())
                && aligned(buffer.as_ptr() as usize))
        {
            return Err(LimboError::CompletionError(CompletionError::IOError(
                ErrorKind::InvalidInput,
                "unaligned direct IO",
            )));
        }
        Ok(())
    }
}

impl File for CrashFile {
    fn lock_file(&self, _exclusive: bool) -> Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.check_aligned(pos, c.as_read().buf())?;
        Ok(self
            .io
            .submit(&self.path, &self.data, OpKind::Read { pos }, c))
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.check_aligned(pos, &buffer)?;
        let kind = OpKind::Write {
            pos,
            buffer,
            durable: false,
        };
        Ok(self.io.submit(&self.path, &self.data, kind, c))
    }

    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> Result<Completion> {
        Ok(self
            .io
            .submit(&self.path, &self.data, OpKind::Sync { covers: 0 }, c))
    }

    fn supports_durable_writes(&self) -> bool {
        self.io.state.lock().durable_writes
    }

    fn pwrite_durable(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.check_aligned(pos, &buffer)?;
        let kind = OpKind::Write {
            pos,
            buffer,
            durable: true,
        };
        Ok(self.io.submit(&self.path, &self.data, kind, c))
    }

    fn allocate(&self, pos: u64, len: u64) -> Result<()> {
        let mut state = self.io.state.lock();
        let seq = state.seq();
        let mut data = self.data.lock();
        let end = (pos + len) as usize;
        if data.volatile.len() < end {
            data.volatile.resize(end, 0);
            data.unsynced.push(Change {
                seq,
                kind: ChangeKind::Resize { len: end as u64 },
            });
        }
        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.data.lock().volatile.len() as u64)
    }

    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        Ok(self
            .io
            .submit(&self.path, &self.data, OpKind::Truncate { len }, c))
    }

    /// A punched range reads back as zeros; so does one written with zeros.
    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        let data = self.data.lock();
        let end = (pos + len).min(data.volatile.len());
        Ok(data.volatile[pos.min(end)..end].iter().all(|&b| b == 0))
    }

    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        let mut state = self.io.state.lock();
        let seq = state.seq();
        let mut data = self.data.lock();
        let end = (pos + len).min(data.volatile.len());
        if pos < end {
            let zeros = vec![0u8; end - pos];
            write_at(&mut data.volatile, pos as u64, &zeros);
            data.unsynced.push(Change {
                seq,
                kind: ChangeKind::Write {
                    pos: pos as u64,
                    bytes: zeros,
                },
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
