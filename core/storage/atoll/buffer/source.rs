//! Where a missing page comes from.
//!
//! A miss happens in two steps. [`PageSource::locate`] does whatever must be looked up first, and
//! may wait on IO; nothing is claimed yet, so waiting costs nothing. Then the pool claims a frame,
//! and [`Located::read`] reads the page into it. What `locate` found must still hold when `read`
//! runs: for the local store, the page-table leaf is held in between, so every change to the
//! page's entry reaches the copy `read` looks at, and once the pool has claimed the page nothing
//! can change it until the read is done. A held metadata page supplies a release wait before a
//! frame is claimed; `read` checks again for a change that began after that look.
//!
//! A page evicted while it differed from its checkpoint copy has its newest copy in a swap slot
//! (audit 18): `read` looks there first. Only a resident frame names a swap copy or gives one up,
//! so the claim holds the swap map's entry still too.

use super::super::meta::MetaRef;
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

/// Called by a read once, right before it hands its request to the pool's IO, and never by one
/// that fills the page itself: from then on the read is the pool's IO in flight, which a fix of
/// the page waits on by stepping (DESIGN.md 9.1.3, wait kinds). Before the request, so the read is
/// counted in flight before it can land.
pub type Submitting<'a> = &'a dyn Fn();

pub trait Located: Send {
    /// Reads the page into `into`, exactly its size. Returning `Done`, the read has either run
    /// `done` (a page it filled itself, `None`) or called `submitting` and handed its request to
    /// the pool's IO, whose landing runs `done` once (`Some`, the request, which the frame's
    /// record holds until it retires: DESIGN.md 22.18, a frame); a page whose landing another
    /// party brings is no read of a source, and the pool refuses one by ending the process: the
    /// fix that started the read keeps its result only so (DESIGN.md 9.1.3, wait kinds).
    /// Returning an error or IO, `done` never runs and no request was handed over; dropping it
    /// lets the pool free the frame before waiting. A dependency already held at
    /// [`PageSource::locate`] is waited for there. `read` may return a pending wait when that
    /// dependency was taken after `locate`.
    fn read(
        self: Box<Self>,
        into: Arc<Buffer>,
        done: ReadDone,
        submitting: Submitting<'_>,
    ) -> Result<IOResult<Option<Completion>>>;
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
            IOResult::Done(leaf) => {
                if let Some(frame) = &leaf {
                    // A swap copy is read without this entry. Otherwise wait before allocating
                    // the located read or claiming its frame; read still checks for a later hold.
                    if frame.is_held() && self.store.swap_slot(pid).is_none() {
                        return Ok(IOResult::IO(IOCompletions(frame.wait_while_held())));
                    }
                }
                IOResult::Done(Box::new(LocalLocated {
                    store: self.store.clone(),
                    pid,
                    leaf,
                }))
            }
            IOResult::IO(io) => IOResult::IO(io),
        })
    }
}

struct LocalLocated {
    store: Arc<PageStore>,
    pid: PageId,
    /// The page-table leaf holding the page's entry, held in the metadata cache.
    leaf: Option<MetaRef>,
}

impl Located for LocalLocated {
    fn read(
        mut self: Box<Self>,
        into: Arc<Buffer>,
        done: ReadDone,
        submitting: Submitting<'_>,
    ) -> Result<IOResult<Option<Completion>>> {
        let pid = self.pid;
        if let Some(slot) = self.store.swap_slot(pid) {
            return self.read_copy(slot, into, done, submitting);
        }
        let entry = match self.leaf.take() {
            Some(leaf) => match leaf.read(|b| radix::map_entry(b, pid.number())) {
                Some(entry) => entry,
                None => return Ok(IOResult::IO(IOCompletions(leaf.wait_while_held()))),
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
                Ok(IOResult::Done(None))
            }
            entry => self.read_copy(entry - SLOT_BASE, into, done, submitting),
        }
    }
}

impl LocalLocated {
    /// Reads the copy of the page in `slot`.
    fn read_copy(
        &self,
        slot: u64,
        into: Arc<Buffer>,
        done: ReadDone,
        submitting: Submitting<'_>,
    ) -> Result<IOResult<Option<Completion>>> {
        let pid = self.pid;
        submitting();
        let c = self.store.read_slot_into(pid, slot, into, move |read| {
            done(match read {
                PageRead::Valid => Ok(()),
                PageRead::Invalid => Err(CompletionError::CorruptPage { page: pid.raw() }),
                PageRead::Failed(e) => Err(e),
            })
        })?;
        Ok(IOResult::Done(Some(c)))
    }
}
