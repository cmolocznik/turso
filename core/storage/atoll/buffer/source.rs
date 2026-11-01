//! Where a missing page comes from.
//!
//! A miss happens in two steps. [`PageSource::locate`] does whatever must be looked up first, and
//! may wait on IO; nothing is claimed yet, so waiting costs nothing. Then the pool claims a frame,
//! and [`Located::read`] reads the page into it. What `locate` found must still hold when `read`
//! runs: for the local store, the page-table leaf is held in between, so every change to the
//! page's entry reaches the copy `read` looks at, and once the pool has claimed the page nothing
//! can change it until the read is done. `read` reads the entry without a lock, and yields if a
//! change to the leaf holds its latch at that moment.

use super::super::meta::MetaFrame;
use super::super::page::PageId;
use super::super::radix::{self, ALLOCATED, FREE, SLOT_BASE};
use super::super::store::{PageRead, PageStore};
use crate::io::{Buffer, Completion};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, LimboError, Result};

/// Called once a read ends: `Ok` once the buffer holds a whole copy of the page.
pub type ReadDone = Box<dyn FnOnce(std::result::Result<(), CompletionError>) + Send>;

pub trait PageSource: Send + Sync {
    /// Gets ready to read `pid`.
    fn locate(&self, pid: PageId) -> Result<IOResult<Box<dyn Located>>>;
}

pub trait Located: Send {
    /// Reads the page into `into`, exactly its size. `done` runs once, inside this call or later,
    /// unless this returns an error or a yield, in which case it never runs: a yield while what
    /// the read needs is latched for a moment.
    fn read(self: Box<Self>, into: Arc<Buffer>, done: ReadDone) -> Result<IOResult<()>>;
}

/// Pages of the local page store.
pub struct LocalSource {
    store: Arc<PageStore>,
}

impl LocalSource {
    pub fn new(store: Arc<PageStore>) -> Self {
        Self { store }
    }
}

impl PageSource for LocalSource {
    fn locate(&self, pid: PageId) -> Result<IOResult<Box<dyn Located>>> {
        Ok(match self.store.leaf(pid)? {
            IOResult::Done(leaf) => IOResult::Done(Box::new(LocalLocated {
                store: self.store.clone(),
                pid,
                leaf,
            })),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }
}

struct LocalLocated {
    store: Arc<PageStore>,
    pid: PageId,
    /// The page-table leaf holding the page's entry, held in the metadata cache.
    leaf: Option<Arc<MetaFrame>>,
}

impl Located for LocalLocated {
    fn read(mut self: Box<Self>, into: Arc<Buffer>, done: ReadDone) -> Result<IOResult<()>> {
        let pid = self.pid;
        let entry = match self.leaf.take() {
            Some(leaf) => match leaf.read(|b| radix::map_entry(b, pid.number())) {
                Some(entry) => entry,
                None => return Ok(IOResult::IO(IOCompletions(Completion::new_yield()))),
            },
            None => FREE,
        };
        match entry {
            FREE => Err(LimboError::Corrupt(format!(
                "page {pid:?} was read but is not allocated"
            ))),
            ALLOCATED => {
                // A page with no copy yet has never been written back: it is all zeros.
                into.as_mut_slice().fill(0);
                done(Ok(()));
                Ok(IOResult::Done(()))
            }
            entry => {
                let c = self
                    .store
                    .read_slot_into(pid, entry - SLOT_BASE, into, move |read| {
                        done(match read {
                            PageRead::Valid(_) => Ok(()),
                            PageRead::Invalid => {
                                Err(CompletionError::CorruptPage { page: pid.raw() })
                            }
                            PageRead::Failed(e) => Err(e),
                        })
                    })?;
                drop(c);
                Ok(IOResult::Done(()))
            }
        }
    }
}
