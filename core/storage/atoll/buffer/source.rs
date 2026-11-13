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
#[cfg(test)]
use super::super::requests::CountedFile;
use super::super::requests::Place;
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

/// The pool's hook a read hands its request to, once, and never one that fills the page itself
/// (D2b §3): the read is counted in flight and marked submitted before it can land, from then on
/// the pool's IO in flight, which a fix of the page waits on (DESIGN.md 9.1.3, wait kinds); the
/// frame's record is held first; then the request is queued in that record for the database's IO
/// threads, in a cooperative frame of a database opened async, or else submitted to its file, the
/// file opened there when cold. A source builds the request and opens no file itself.
pub trait Submit {
    /// Hands over the read `c` of the copy at `place`: the request as handed over.
    fn read(&self, place: Place, c: Completion) -> Result<Completion>;
    /// A test source's announcement of a request it lands itself, which no backend holds.
    #[cfg(test)]
    fn announce(&self);
}

pub type Submitting<'a> = &'a dyn Submit;

/// A test's hook: counts what it is handed, announcements included. A read goes to the file
/// `files` names for its place, or comes back unsubmitted, owned by no backend, where it names
/// none.
#[cfg(test)]
pub struct Counting<'a> {
    pub files: Option<&'a dyn Fn(Place) -> Result<(Arc<CountedFile>, u64)>>,
    pub handed: std::cell::Cell<usize>,
}

#[cfg(test)]
impl<'a> Counting<'a> {
    pub fn new(files: Option<&'a dyn Fn(Place) -> Result<(Arc<CountedFile>, u64)>>) -> Self {
        Self {
            files,
            handed: std::cell::Cell::new(0),
        }
    }
}

#[cfg(test)]
impl Submit for Counting<'_> {
    fn read(&self, place: Place, c: Completion) -> Result<Completion> {
        use crate::io::File;
        self.handed.set(self.handed.get() + 1);
        match self.files {
            Some(files) => {
                let (file, at) = files(place)?;
                file.pread(at, c)
            }
            None => Ok(c),
        }
    }

    fn announce(&self) {
        self.handed.set(self.handed.get() + 1);
    }
}

pub trait Located: Send {
    /// Reads the page into `into`, exactly its size. Returning `Done`, the read has either run
    /// `done` (a page it filled itself, `None`) or handed its request to the pool through
    /// `submitting`, whose landing runs `done` once (`Some`, the request, which the frame's
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
        let (place, c) = PageStore::read_request(pid, slot, into, move |read| {
            done(match read {
                PageRead::Valid => Ok(()),
                PageRead::Invalid => Err(CompletionError::CorruptPage { page: pid.raw() }),
                PageRead::Failed(e) => Err(e),
            })
        });
        Ok(IOResult::Done(Some(submitting.read(place, c)?)))
    }
}
