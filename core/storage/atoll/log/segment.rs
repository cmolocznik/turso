//! Log segment files: `{base}.log.{n}` holds the log's bytes from `n·size` to `(n+1)·size`.
//!
//! The files open are cells by segment number, in an array that grows to the highest number
//! reached: a cell each, of one word, per segment the log has ever had. A file is put in its cell
//! by compare-and-swap, read in a step, and let go through the epochs once removed.
//!
//! **Spares** (DESIGN.md §7.4). A segment the log no longer needs is renamed `{base}.log.spare{k}`
//! instead of removed, while a spare's name is free, and the next segment prepared is a spare
//! renamed again: whole, allocated and synced, it needs no zeros. Its old records cannot read as
//! the log where they now lie, since a record's CRC covers its position and padding names its own.

use super::super::arc_cell::{ArcCell, Releasing};
use super::super::epoch::Epochs;
use super::super::requests::{Counted, CountedFile};
use super::super::segmented::Segmented;
use crate::io::{File, OpenFlags, IO};
use crate::sync::atomic::{AtomicU8, Ordering};
use crate::sync::Arc;
use crate::{CompletionError, LimboError, Result};
use std::path::Path;

/// Spares kept. The log prepares one segment ahead of the one it fills (§7.4), so it takes one
/// spare per segment filled, and the checkpoint gives back one per segment released: a second lets
/// a release land while the spare before it is still to be taken.
pub const SPARES: usize = 2;

/// A spare's name holds no file.
const EMPTY: u8 = 0;
/// A segment is being renamed to it.
const FILLING: u8 = 1;
/// It holds a spare.
const FULL: u8 = 2;
/// Its spare is being renamed to a segment.
const TAKING: u8 = 3;

/// How a set of segments opens its files: plainly, for a reader, or counted and typed, for the log
/// that writes them, whose writes and syncs run their retirement observers before they are
/// uncounted (D1b §2).
pub(super) trait Opens: Send + Sync + 'static {
    type File: File + ?Sized + Send + Sync + 'static;
    fn io(&self) -> &Arc<dyn IO>;
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Arc<Self::File>>;
}

/// A reader's segments, opened through any IO.
pub(super) struct Plain(Arc<dyn IO>);

impl Opens for Plain {
    type File = dyn File;
    fn io(&self) -> &Arc<dyn IO> {
        &self.0
    }
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Arc<dyn File>> {
        self.0.open_file(path, flags, false)
    }
}

/// The writing log's segments, opened through the database's counted IO.
pub(super) struct Typed {
    counted: Arc<Counted>,
    io: Arc<dyn IO>,
}

impl Opens for Typed {
    type File = CountedFile;
    fn io(&self) -> &Arc<dyn IO> {
        &self.io
    }
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Arc<CountedFile>> {
        self.counted.open_counted(path, flags, false)
    }
}

pub(super) struct Segments<O: Opens = Plain> {
    opens: O,
    base: String,
    pub size: u64,
    direct: bool,
    files: Segmented<ArcCell<Arc<O::File>>>,
    releasing: Releasing<Arc<O::File>>,
    /// Each spare name's state: one renamer of it at a time, by compare-and-swap.
    spares: [AtomicU8; SPARES],
}

impl Segments<Plain> {
    pub fn new(io: Arc<dyn IO>, base: &str, size: u64, direct: bool) -> Self {
        Self::with(Plain(io), base, size, direct)
    }
}

impl Segments<Typed> {
    pub fn counted(counted: Arc<Counted>, base: &str, size: u64, direct: bool) -> Self {
        let io: Arc<dyn IO> = counted.clone();
        Self::with(Typed { counted, io }, base, size, direct)
    }

    /// The counted IO the segments are opened through.
    pub fn counted_io(&self) -> &Arc<Counted> {
        &self.opens.counted
    }
}

impl<O: Opens> Segments<O> {
    fn with(opens: O, base: &str, size: u64, direct: bool) -> Self {
        Self {
            opens,
            base: base.to_string(),
            size,
            direct,
            files: Segmented::default(),
            releasing: Releasing::new(Arc::new(Epochs::new())),
            spares: std::array::from_fn(|_| AtomicU8::new(EMPTY)),
        }
    }

    pub fn number(&self, lsn: u64) -> u64 {
        lsn / self.size
    }

    pub fn start(&self, number: u64) -> u64 {
        number * self.size
    }

    pub(super) fn path(&self, number: u64) -> String {
        format!("{}.log.{number:016x}", self.base)
    }

    fn flags(&self) -> OpenFlags {
        if self.direct {
            OpenFlags::Direct
        } else {
            OpenFlags::None
        }
    }

    /// The directory the segments are in, whose sync makes their creation and removal durable.
    pub fn directory(&self) -> String {
        Path::new(&self.base)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| ".".to_string())
    }

    fn io(&self) -> &Arc<dyn IO> {
        self.opens.io()
    }

    /// The open file of segment `number`, if it is open.
    pub fn get(&self, number: u64) -> Option<Arc<O::File>> {
        let cell = self.files.get(number)?;
        let step = self.releasing.epochs().enter();
        cell.get(&step).map(|file| (*file).clone())
    }

    /// Segment `number`, or `None` if it does not exist.
    pub fn open(&self, number: u64) -> Result<Option<Arc<O::File>>> {
        if let Some(file) = self.get(number) {
            return Ok(Some(file));
        }
        #[cfg(test)]
        super::SEGMENTS_OPENED.with(|n| n.set(n.get() + 1));
        match self.opens.open(&self.path(number), self.flags()) {
            Ok(file) => {
                // Opened by two at once, the first kept.
                let step = self.releasing.epochs().enter();
                let kept = self
                    .files
                    .get_or_make(number)
                    .put_if_empty(Arc::new(file), &step);
                Ok(Some((*kept).clone()))
            }
            Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::NotFound,
                _,
            ))) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Creates segment `number`, empty.
    pub fn create(&self, number: u64) -> Result<Arc<O::File>> {
        let file = self
            .opens
            .open(&self.path(number), self.flags() | OpenFlags::Create)?;
        self.files
            .get_or_make(number)
            .replace(Some(Arc::new(file.clone())), &self.releasing);
        Ok(file)
    }

    /// The open file of segment `number`, which must have been opened or created.
    pub fn file(&self, number: u64) -> Arc<O::File> {
        self.get(number)
            .unwrap_or_else(|| panic!("log segment {number} is not open"))
    }

    pub fn remove(&self, number: u64) -> Result<()> {
        if let Some(cell) = self.files.get(number) {
            cell.replace(None, &self.releasing);
        }
        self.io().remove_file(&self.path(number))
    }

    fn spare_path(&self, k: usize) -> String {
        format!("{}.log.spare{k}", self.base)
    }

    /// Takes in the spares a crash or an earlier process left: each whole, as a segment is.
    pub fn adopt_spares(&self) -> Result<()> {
        for (k, state) in self.spares.iter().enumerate() {
            match self
                .io()
                .open_file(&self.spare_path(k), self.flags(), false)
            {
                Ok(file) if file.size()? == self.size => state.store(FULL, Ordering::Release),
                // Torn on its way, never a segment's.
                Ok(_) => self.io().remove_file(&self.spare_path(k))?,
                Err(LimboError::CompletionError(CompletionError::IOError(
                    std::io::ErrorKind::NotFound,
                    _,
                ))) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Releases segment `number`, which nothing needs any more: kept as a spare if a spare's name
    /// is free and the backend renames, else removed. Durable once the directory is synced.
    pub fn release(&self, number: u64) -> Result<()> {
        let free = self.spares.iter().enumerate().find(|(_, s)| {
            // Acquire: a spare taken before is renamed away before its name reads free.
            s.compare_exchange(EMPTY, FILLING, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        });
        if let Some((k, state)) = free {
            if let Some(cell) = self.files.get(number) {
                cell.replace(None, &self.releasing);
            }
            match self
                .io()
                .rename_file(&self.path(number), &self.spare_path(k))
            {
                Ok(true) => {
                    // Release: the rename happened before anyone takes the spare.
                    state.store(FULL, Ordering::Release);
                    return Ok(());
                }
                Ok(false) => state.store(EMPTY, Ordering::Release),
                Err(e) => {
                    state.store(EMPTY, Ordering::Release);
                    return Err(e);
                }
            }
        }
        self.remove(number)
    }

    /// Segment `number` made of a spare, if there is one: its file, whole, its old bytes never the
    /// log where they now lie. Durable once the directory is synced.
    pub fn reuse(&self, number: u64) -> Result<Option<Arc<O::File>>> {
        let Some((k, state)) = self.spares.iter().enumerate().find(|(_, s)| {
            // Acquire: pairs with the release that made it full, after its rename.
            s.compare_exchange(FULL, TAKING, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        }) else {
            return Ok(None);
        };
        let renamed = self
            .io()
            .rename_file(&self.spare_path(k), &self.path(number));
        state.store(EMPTY, Ordering::Release);
        if !renamed? {
            return Ok(None);
        }
        let file = self.opens.open(&self.path(number), self.flags())?;
        self.files
            .get_or_make(number)
            .replace(Some(Arc::new(file.clone())), &self.releasing);
        Ok(Some(file))
    }
}
