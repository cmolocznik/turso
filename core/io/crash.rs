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
//!
//! It also records every synchronous file call (an open, a removal, a rename, a directory sync, an
//! allocation) made from inside a callback its [`IO::step`] delivers
//! ([`CrashIO::file_work_in_callbacks`]): a callback that waits on the file system would hold up
//! every delivery behind it on a backend with one deliverer.

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

/// Chances, from 0 to 1, that an operation fails with an IO error, or, for `short_write`, that a
/// write lands only a prefix of its bytes and says so in its count, as a full device or a signal
/// can leave one.
#[derive(Debug, Clone, Copy, Default)]
pub struct Faults {
    pub read: f64,
    pub write: f64,
    pub sync: f64,
    pub short_write: f64,
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
    /// The path faults apply to; every path if `None`.
    faults_on: Option<String>,
    capacity: Option<u64>,
    next_seq: u64,
    durable_writes: bool,
    /// Every write completed, per path: where and how long, in completion order.
    writes: BTreeMap<String, Vec<(u64, usize)>>,
    /// Operations completed.
    completed: u64,
    captures: Option<Captures>,
    /// The synchronous file calls made from inside a delivered callback, in order.
    file_work_in_callbacks: Vec<&'static str>,
    /// Run as each synchronous file call begins, outside the state's lock: a test's stand-in for
    /// another request failing while the call runs.
    file_work_hook: Option<Arc<dyn Fn(&'static str) + Send + Sync>>,
    /// Run as each step begins: an error it returns is the step's, which then delivers nothing.
    step_hook: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
}

crate::thread::thread_local! {
    /// Set on this thread while a step delivers a callback.
    static DELIVERING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Crash images taken while operations complete, armed by [`CrashIO::capture`].
struct Captures {
    /// One just before each write to a path this accepts completes.
    before_writes_to: Box<dyn Fn(&str) -> bool + Send + Sync>,
    /// And one as every `every`th operation completes.
    every: u64,
    seed: u64,
    images: Vec<CrashIO>,
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
                faults_on: None,
                capacity: None,
                next_seq: 0,
                durable_writes: true,
                writes: BTreeMap::new(),
                completed: 0,
                captures: None,
                file_work_in_callbacks: Vec::new(),
                file_work_hook: None,
                step_hook: None,
            })),
        }
    }

    /// The synchronous file calls made so far from inside a callback a step delivered, each by
    /// what it was.
    pub fn file_work_in_callbacks(&self) -> Vec<&'static str> {
        self.state.lock().file_work_in_callbacks.clone()
    }

    /// Runs `hook` with what each synchronous file call is (an open, a removal, a rename, a
    /// directory sync, an allocation) as it begins; `None` stops it.
    pub fn on_file_work(&self, hook: Option<Arc<dyn Fn(&'static str) + Send + Sync>>) {
        self.state.lock().file_work_hook = hook;
    }

    /// The hook of [`on_file_work`](Self::on_file_work), run with no lock held.
    fn file_work(&self, what: &'static str) {
        let hook = self.state.lock().file_work_hook.clone();
        if let Some(hook) = hook {
            hook(what);
        }
    }

    /// Runs `hook` as each step begins, with no lock held; an error it returns is the step's own
    /// (the stepping failed, not a request), and that step delivers nothing. `None` stops it.
    pub fn on_step(&self, hook: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>) {
        self.state.lock().step_hook = hook;
    }

    pub fn set_faults(&self, faults: Faults) {
        self.state.lock().faults = faults;
    }

    /// Makes faults apply to operations on `path` alone, or on every path with `None`.
    pub fn set_faults_on(&self, path: Option<&str>) {
        self.state.lock().faults_on = path.map(str::to_string);
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

    /// Handles on this IO's state besides this one: its files still open, and every other
    /// handle. Whoever drops all it built on the IO finds none left unless something holds one.
    pub fn other_handles(&self) -> usize {
        Arc::strong_count(&self.state) - 1
    }

    /// Operations completed so far.
    pub fn completed(&self) -> u64 {
        self.state.lock().completed
    }

    /// From now on, takes what a crash could leave just before each write to a path
    /// `before_writes_to` accepts completes, and as every `every`th operation completes, each
    /// chosen by a seed drawn from `seed`: crashes in the middle of whatever runs on this IO, such
    /// as a recovery.
    pub fn capture(
        &self,
        before_writes_to: impl Fn(&str) -> bool + Send + Sync + 'static,
        every: u64,
        seed: u64,
    ) {
        self.state.lock().captures = Some(Captures {
            before_writes_to: Box::new(before_writes_to),
            every: every.max(1),
            seed,
            images: Vec::new(),
        });
    }

    /// The images taken since [`capture`](Self::capture), which stops taking them.
    pub fn captured(&self) -> Vec<CrashIO> {
        self.state
            .lock()
            .captures
            .take()
            .map_or_else(Vec::new, |c| c.images)
    }

    /// What a crash now could leave on disk, chosen by `seed`, as a fresh IO holding only that.
    /// This IO carries on unchanged.
    pub fn crash(&self, seed: u64) -> CrashIO {
        image(&self.state.lock(), seed)
    }
}

/// What a crash in `state` could leave on disk, chosen by `seed`.
fn image(state: &State, seed: u64) -> CrashIO {
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

impl CrashIO {
    /// Completes one operation in flight, chosen at random. Callbacks run with no lock held.
    fn complete_one(&self) {
        let (op, outcome) = {
            let mut state = self.state.lock();
            if state.in_flight.is_empty() {
                return;
            }
            let count = state.in_flight.len();
            let at = state.rng.below(count);
            let before = state.captures.as_ref().is_some_and(|c| {
                let op = &state.in_flight[at];
                matches!(op.kind, OpKind::Write { .. }) && (c.before_writes_to)(&op.path)
            });
            if before {
                state.capture();
            }
            let op = state.in_flight.swap_remove(at);
            let outcome = state.apply(&op);
            state.completed += 1;
            let completed = state.completed;
            if state
                .captures
                .as_ref()
                .is_some_and(|c| completed % c.every == 0)
            {
                state.capture();
            }
            (op, outcome)
        };
        let outer = DELIVERING.with(|d| d.replace(true));
        op.completion.deliver(outcome);
        DELIVERING.with(|d| d.set(outer));
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
        completion.accept();
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
    /// Records a synchronous file call made from inside a delivered callback.
    fn note_file_work(&mut self, what: &'static str) {
        if DELIVERING.with(|d| d.get()) {
            self.file_work_in_callbacks.push(what);
        }
    }

    /// Takes a crash image into the armed captures.
    fn capture(&mut self) {
        let Some(c) = &self.captures else { return };
        let seed = c.seed ^ self.completed.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let crashed = image(self, seed);
        if let Some(c) = &mut self.captures {
            c.images.push(crashed);
        }
    }

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
        let faults = match &self.faults_on {
            Some(path) if *path != op.path => Faults::default(),
            _ => self.faults,
        };
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
                let mut bytes = buffer.as_slice();
                if !bytes.is_empty() && self.rng.chance(faults.short_write) {
                    bytes = &bytes[..self.rng.below(bytes.len())];
                }
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
        self.file_work("open");
        let mut state = self.state.lock();
        state.note_file_work("open");
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
        self.file_work("remove");
        let mut state = self.state.lock();
        state.note_file_work("remove");
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

    /// The rename's two names are each durable only once their directory is synced, so a crash
    /// before then may leave either, both or neither: stricter than a real file system, where
    /// exactly one survives.
    fn rename_file(&self, from: &str, to: &str) -> Result<bool> {
        self.file_work("rename");
        let mut state = self.state.lock();
        state.note_file_work("rename");
        let data = match state.paths.get_mut(from) {
            Some(entry) if entry.exists => {
                entry.exists = false;
                let data = entry.data.clone();
                if !entry.durable {
                    state.paths.remove(from);
                }
                data
            }
            _ => return Err(CompletionError::IOError(ErrorKind::NotFound, "rename").into()),
        };
        match state.paths.get_mut(to) {
            Some(entry) => {
                entry.exists = true;
                entry.data = data;
            }
            None => {
                state.paths.insert(
                    to.to_string(),
                    Entry {
                        data,
                        exists: true,
                        durable: false,
                    },
                );
            }
        }
        Ok(true)
    }

    fn sync_directory(&self, dir: &str) -> Result<()> {
        self.file_work("directory sync");
        let mut state = self.state.lock();
        state.note_file_work("directory sync");
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
        let hook = self.state.lock().step_hook.clone();
        if let Some(hook) = hook {
            hook()?;
        }
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
        self.io.file_work("allocate");
        let mut state = self.io.state.lock();
        state.note_file_work("allocate");
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
