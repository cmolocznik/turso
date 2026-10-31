//! Log segment files: `{base}.log.{n}` holds the log's bytes from `n·size` to `(n+1)·size`.

use crate::io::{File, OpenFlags, IO};
use crate::sync::{Arc, Mutex};
use crate::{CompletionError, LimboError, Result};
use std::collections::BTreeMap;
use std::path::Path;

pub(super) struct Segments {
    io: Arc<dyn IO>,
    base: String,
    pub size: u64,
    direct: bool,
    files: Mutex<BTreeMap<u64, Arc<dyn File>>>,
}

impl Segments {
    pub fn new(io: Arc<dyn IO>, base: &str, size: u64, direct: bool) -> Self {
        Self {
            io,
            base: base.to_string(),
            size,
            direct,
            files: Mutex::new(BTreeMap::new()),
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

    /// Segment `number`, or `None` if it does not exist.
    pub fn open(&self, number: u64) -> Result<Option<Arc<dyn File>>> {
        if let Some(file) = self.files.lock().get(&number) {
            return Ok(Some(file.clone()));
        }
        match self.io.open_file(&self.path(number), self.flags(), false) {
            Ok(file) => {
                self.files.lock().insert(number, file.clone());
                Ok(Some(file))
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
        self.files.lock().insert(number, file.clone());
        Ok(file)
    }

    /// The open file of segment `number`, which must have been opened or created.
    pub fn file(&self, number: u64) -> Arc<dyn File> {
        self.files
            .lock()
            .get(&number)
            .cloned()
            .unwrap_or_else(|| panic!("log segment {number} is not open"))
    }

    pub fn remove(&self, number: u64) -> Result<()> {
        self.files.lock().remove(&number);
        self.io.remove_file(&self.path(number))
    }
}
