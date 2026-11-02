//! Log segment files: `{base}.log.{n}` holds the log's bytes from `n·size` to `(n+1)·size`.
//!
//! The files open are cells by segment number, in an array that grows to the highest number
//! reached: a cell each, of one word, per segment the log has ever had. A file is put in its cell
//! by compare-and-swap, read in a step, and let go through the epochs once removed.

use super::super::arc_cell::{ArcCell, Releasing};
use super::super::epoch::Epochs;
use super::super::segmented::Segmented;
use crate::io::{File, OpenFlags, IO};
use crate::sync::Arc;
use crate::{CompletionError, LimboError, Result};
use std::path::Path;

pub(super) struct Segments {
    io: Arc<dyn IO>,
    base: String,
    pub size: u64,
    direct: bool,
    files: Segmented<ArcCell<Arc<dyn File>>>,
    releasing: Releasing<Arc<dyn File>>,
}

impl Segments {
    pub fn new(io: Arc<dyn IO>, base: &str, size: u64, direct: bool) -> Self {
        Self {
            io,
            base: base.to_string(),
            size,
            direct,
            files: Segmented::default(),
            releasing: Releasing::new(Arc::new(Epochs::new())),
        }
    }

    pub fn number(&self, lsn: u64) -> u64 {
        lsn / self.size
    }

    pub fn start(&self, number: u64) -> u64 {
        number * self.size
    }

    fn path(&self, number: u64) -> String {
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

    /// The open file of segment `number`, if it is open.
    fn get(&self, number: u64) -> Option<Arc<dyn File>> {
        let cell = self.files.get(number)?;
        let step = self.releasing.epochs().enter();
        cell.get(&step).map(|file| (*file).clone())
    }

    /// Segment `number`, or `None` if it does not exist.
    pub fn open(&self, number: u64) -> Result<Option<Arc<dyn File>>> {
        if let Some(file) = self.get(number) {
            return Ok(Some(file));
        }
        match self.io.open_file(&self.path(number), self.flags(), false) {
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
    pub fn create(&self, number: u64) -> Result<Arc<dyn File>> {
        let file =
            self.io
                .open_file(&self.path(number), self.flags() | OpenFlags::Create, false)?;
        self.files
            .get_or_make(number)
            .replace(Some(Arc::new(file.clone())), &self.releasing);
        Ok(file)
    }

    /// The open file of segment `number`, which must have been opened or created.
    pub fn file(&self, number: u64) -> Arc<dyn File> {
        self.get(number)
            .unwrap_or_else(|| panic!("log segment {number} is not open"))
    }

    pub fn remove(&self, number: u64) -> Result<()> {
        if let Some(cell) = self.files.get(number) {
            cell.replace(None, &self.releasing);
        }
        self.io.remove_file(&self.path(number))
    }
}
