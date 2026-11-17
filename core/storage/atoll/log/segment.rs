//! Log segment files: `{base}.log.{n}` holds the log's bytes from segment `n`'s start to the
//! next's. The first segment is `first` bytes and each next one twice the one before, up to
//! `size`; every segment from there on is `size` bytes (DISK-MIN1). A new database so writes the
//! zeros of a small segment, not of a full one. Segment `n < growth` starts at
//! `first · (2^n - 1)`; the segments of `size` start at `size - first`, one every `size` bytes.
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
    /// Bytes of the largest segment, every segment's from the `growth`-th on.
    pub size: u64,
    /// Bytes of the first segment.
    first: u64,
    /// How many segments are smaller than `size`: `log2(size / first)`.
    growth: u64,
    /// Where the first segment of `size` starts: `size - first`.
    steady: u64,
    direct: bool,
    files: Segmented<ArcCell<Arc<O::File>>>,
    releasing: Releasing<Arc<O::File>>,
    /// Each spare name's state: one renamer of it at a time, by compare-and-swap.
    spares: [AtomicU8; SPARES],
}

impl Segments<Plain> {
    pub fn new(io: Arc<dyn IO>, base: &str, sizes: (u64, u64), direct: bool) -> Self {
        Self::with(Plain(io), base, sizes, direct)
    }
}

impl Segments<Typed> {
    pub fn counted(counted: Arc<Counted>, base: &str, sizes: (u64, u64), direct: bool) -> Self {
        let io: Arc<dyn IO> = counted.clone();
        Self::with(Typed { counted, io }, base, sizes, direct)
    }

    /// The counted IO the segments are opened through.
    pub fn counted_io(&self) -> &Arc<Counted> {
        &self.opens.counted
    }
}

impl<O: Opens> Segments<O> {
    /// `sizes`: the first segment's bytes and the largest's, the largest a power of two times the
    /// first.
    fn with(opens: O, base: &str, (first, size): (u64, u64), direct: bool) -> Self {
        assert!(
            first > 0 && size.is_multiple_of(first) && (size / first).is_power_of_two(),
            "log segments of {first} bytes first and {size} at most"
        );
        Self {
            opens,
            base: base.to_string(),
            size,
            first,
            growth: u64::from((size / first).ilog2()),
            steady: size - first,
            direct,
            files: Segmented::default(),
            releasing: Releasing::new(Arc::new(Epochs::new())),
            spares: std::array::from_fn(|_| AtomicU8::new(EMPTY)),
        }
    }

    /// The segment `lsn` lies in.
    pub fn number(&self, lsn: u64) -> u64 {
        if lsn >= self.steady {
            self.growth + (lsn - self.steady) / self.size
        } else {
            u64::from((lsn / self.first + 1).ilog2())
        }
    }

    /// Where segment `number` starts.
    pub fn start(&self, number: u64) -> u64 {
        if number >= self.growth {
            self.steady + (number - self.growth) * self.size
        } else {
            self.first * ((1 << number) - 1)
        }
    }

    /// Bytes of segment `number`.
    pub fn size_of(&self, number: u64) -> u64 {
        if number >= self.growth {
            self.size
        } else {
            self.first << number
        }
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

    /// Releases segment `number`, which nothing needs any more: kept as a spare if it is of the
    /// largest size, which every later segment has, a spare's name is free and the backend
    /// renames; else removed. Durable once the directory is synced.
    pub fn release(&self, number: u64) -> Result<()> {
        if self.size_of(number) != self.size {
            return self.remove(number);
        }
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

    /// Segment `number` made of a spare, if it is of the largest size, a spare's, and there is one:
    /// its file, whole, its old bytes never the log where they now lie. Durable once the
    /// directory is synced.
    pub fn reuse(&self, number: u64) -> Result<Option<Arc<O::File>>> {
        if self.size_of(number) != self.size {
            return Ok(None);
        }
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

#[cfg(all(test, not(any(shuttle, atoll_loom))))]
mod tests {
    use super::*;
    use crate::io::MemoryIO;
    use crate::storage::atoll::log::BLOCK;

    /// Every position lies in the segment whose start is at or below it and whose next start is
    /// above it, each segment's size is the distance to the next start, and the sizes double from
    /// the first to the largest and stay there (DISK-MIN1).
    #[test]
    fn a_position_lies_in_one_segment_and_the_sizes_double_to_the_largest() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        // Recorded with the table alive, asserted once it is dropped: it holds an Atoll lock.
        let mut wrong = Vec::new();
        for (first, size) in [(BLOCK, BLOCK), (BLOCK, 8 * BLOCK), (2 * BLOCK, 64 * BLOCK)] {
            let segments = Segments::new(io.clone(), "/table", (first, size), false);
            let mut expected = first;
            for n in 0..40 {
                if segments.size_of(n) != expected {
                    wrong.push(format!(
                        "segment {n} of ({first}, {size}) is {}",
                        segments.size_of(n)
                    ));
                }
                if segments.start(n + 1) - segments.start(n) != expected {
                    wrong.push(format!("segment {n}'s span of ({first}, {size})"));
                }
                expected = (expected * 2).min(size);
            }
            if segments.start(0) != 0 {
                wrong.push(format!("segment 0 of ({first}, {size}) starts past 0"));
            }
            for lsn in (0..segments.start(40)).step_by(BLOCK as usize / 4) {
                let n = segments.number(lsn);
                if !(segments.start(n) <= lsn && lsn < segments.start(n + 1)) {
                    wrong.push(format!("{lsn} placed in segment {n} of ({first}, {size})"));
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "{}",
            wrong[..wrong.len().min(3)].join("; ")
        );
    }

    /// A segment smaller than the largest is removed at its release, never kept as a spare: a
    /// spare becomes a later segment, which is of the largest size. One of the largest size is
    /// kept, and the next segment of that size is made of it.
    #[test]
    fn only_a_segment_of_the_largest_size_is_kept_as_a_spare() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let segments = Segments::new(io.clone(), "/spares", (BLOCK, 4 * BLOCK), false);
        for n in 0..4 {
            segments.create(n).unwrap();
        }
        segments.release(0).unwrap();
        segments.release(1).unwrap();
        // Recorded with the table alive, asserted once it is dropped: it holds an Atoll lock.
        let spare_files = |k: usize| io.open_file(&segments.spare_path(k), OpenFlags::None, false);
        let no_small_spare = spare_files(0).is_err() && spare_files(1).is_err();
        let small_removed =
            segments.open(0).unwrap().is_none() && segments.open(1).unwrap().is_none();
        let file = segments.file(2);
        let (write, written) = crate::storage::atoll::whole_write(4 * BLOCK as usize);
        let zeros = Arc::new(crate::storage::atoll::aligned::AlignedBlock::zeroed(
            4 * BLOCK as usize,
        ));
        drop(
            file.pwrite(0, zeros.region(0, 4 * BLOCK as usize), write)
                .unwrap(),
        );
        crate::storage::atoll::park::wait_for_completion(io.as_ref(), written).unwrap();
        segments.release(2).unwrap();
        let largest_kept = spare_files(0).is_ok();
        let reused = segments.reuse(4).unwrap().is_some();
        drop((file, segments));
        assert!(no_small_spare, "a smaller segment was kept as a spare");
        assert!(small_removed, "a smaller segment's file was left");
        assert!(largest_kept, "a segment of the largest size was not kept");
        assert!(
            reused,
            "the next segment of the largest size was not made of the spare"
        );
    }
}
