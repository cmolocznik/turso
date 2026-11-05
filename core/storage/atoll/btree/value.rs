//! Values too large to go inline in a leaf (DESIGN.md §13): they go out of line, into blob pages,
//! and the leaf holds a reference.
//!
//! ```text
//! reference: len: u64 | pid: u64 | id: u64
//! blob page: header (24) | id: u64 | bytes
//! ```
//!
//! A value that fits one page of the largest class goes into one page of the smallest class that
//! holds it. A longer one is cut into chunks, each a page of the largest class, and `pid` names a
//! directory page listing them. A value of a billion bytes, SQLite's limit, takes 120 chunks and a
//! 4 KiB directory.
//!
//! **Writing.** Pages are allocated provisional, and their bytes logged as blob writes in pieces
//! of at most one log chunk, so a large value never holds a flush back by more than a chunk. The
//! record that puts the reference in a leaf adopts them; until then a crash frees them.
//!
//! **Reading.** Every page of a value carries the value's id, drawn at random when it was written.
//! A reader that followed a reference before the value was replaced may find its pages freed and
//! taken by something else; the id tells it so, and it reads the leaf again.

use super::super::blob;
use super::super::buffer::{BufferPool, Optimistic};
use super::super::log::Log;
use super::super::page::{self, PageId, SizeClass, CLASS_COUNT, HEADER_SIZE};
use super::super::shared::Bytes;
use super::super::store::PageStore;
use crate::io::Completion;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::{LimboError, Result};

/// Bytes a reference takes in a leaf.
pub const REF: usize = 24;
const ID: usize = 8;

fn chunk_class() -> SizeClass {
    SizeClass::new(CLASS_COUNT - 1).unwrap()
}

/// Bytes of value a page of `class` holds.
fn holds(class: SizeClass) -> usize {
    class.size() - HEADER_SIZE - ID
}

/// Bytes of value a chunk holds.
pub fn chunk() -> usize {
    holds(chunk_class())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueRef {
    pub len: u64,
    /// The page holding the value, or the directory of its chunks.
    pub pid: PageId,
    pub id: u64,
}

impl ValueRef {
    pub fn encode(&self) -> [u8; REF] {
        let mut out = [0u8; REF];
        out[..8].copy_from_slice(&self.len.to_le_bytes());
        out[8..16].copy_from_slice(&self.pid.raw().to_le_bytes());
        out[16..].copy_from_slice(&self.id.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != REF {
            return None;
        }
        let word = |i: usize| u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap());
        Some(Self {
            len: word(0),
            pid: PageId::from_raw(word(1)),
            id: word(2),
        })
    }

    fn chunks(&self) -> Option<usize> {
        (self.len as usize > chunk()).then(|| (self.len as usize).div_ceil(chunk()))
    }
}

/// The classes of the pages a value of `len` bytes takes: the page or directory, then the chunks.
fn plan(len: usize) -> Vec<SizeClass> {
    if len <= chunk() {
        return vec![SizeClass::fitting(HEADER_SIZE + ID + len).expect("a chunk's class")];
    }
    let chunks = len.div_ceil(chunk());
    let directory = SizeClass::fitting(HEADER_SIZE + ID + 8 * chunks)
        .unwrap_or_else(|| panic!("a value of {len} bytes"));
    std::iter::once(directory)
        .chain(std::iter::repeat_n(chunk_class(), chunks))
        .collect()
}

/// The longest value: one whose directory fills a page of the largest class.
pub fn max_len() -> usize {
    holds(chunk_class()) / 8 * chunk()
}

/// Writing a value into provisional blob pages. Step it to the end; then put [`reference`] in a
/// leaf in a group that adopts [`pages`]. Dropped before that, it hands its pages to the store,
/// whose checkpointer frees them.
///
/// [`reference`]: Self::reference
/// [`pages`]: Self::pages
pub struct ValueWrite {
    bytes: Vec<u8>,
    id: u64,
    plan: Vec<SizeClass>,
    pages: Vec<(PageId, u32)>,
    /// The directory's bytes, once every chunk is allocated.
    directory: Vec<u8>,
    /// The page being written and how much of its bytes are.
    at: (usize, usize),
    store: Arc<PageStore>,
    adopted: bool,
}

impl ValueWrite {
    pub fn new(bytes: Vec<u8>, id: u64, store: Arc<PageStore>) -> Self {
        assert!(bytes.len() <= max_len(), "a value of {} bytes", bytes.len());
        Self {
            plan: plan(bytes.len()),
            bytes,
            id,
            pages: Vec::new(),
            directory: Vec::new(),
            at: (0, 0),
            store,
            adopted: false,
        }
    }

    pub fn reference(&self) -> ValueRef {
        ValueRef {
            len: self.bytes.len() as u64,
            pid: self.pages[0].0,
            id: self.id,
        }
    }

    pub fn pages(&self) -> Vec<PageId> {
        self.pages.iter().map(|p| p.0).collect()
    }

    /// The record putting the reference in a leaf adopted the pages.
    pub fn adopted(&mut self) {
        self.adopted = true;
    }

    /// What page `j` holds past its header and the id: the value, the directory, or a chunk.
    fn contents(&self, j: usize) -> &[u8] {
        if self.plan.len() == 1 {
            return &self.bytes;
        }
        if j == 0 {
            return &self.directory;
        }
        let from = (j - 1) * chunk();
        &self.bytes[from..(from + chunk()).min(self.bytes.len())]
    }

    /// Bytes `n` of page `j`'s from `done` on: the id first, then its contents.
    fn piece(&self, j: usize, done: usize, n: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(n);
        if done < ID {
            bytes.extend_from_slice(&self.id.to_le_bytes()[done..ID.min(done + n)]);
        }
        let from = done.max(ID) - ID;
        let rest = n - bytes.len();
        bytes.extend_from_slice(&self.contents(j)[from..from + rest]);
        bytes
    }

    /// Writes the value through `log`, or, for a temporary tree, into temporary pages with
    /// nothing logged.
    pub fn step(&mut self, pool: &Arc<BufferPool>, log: Option<&Arc<Log>>) -> Result<IOResult<()>> {
        while self.pages.len() < self.plan.len() {
            let class = self.plan[self.pages.len()];
            let allocated = match log {
                Some(_) => pool.allocate(class)?,
                None => pool.allocate_temporary(class)?,
            };
            let page = match allocated {
                IOResult::Done(p) => p,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            self.pages.push((page.pid(), page.hint()));
            if self.pages.len() == self.plan.len() && self.plan.len() > 1 {
                self.directory = self.pages[1..]
                    .iter()
                    .flat_map(|(p, _)| p.raw().to_le_bytes())
                    .collect();
            }
        }
        // Pieces of at most a log chunk, record framing included; unlogged, a page at once.
        let piece = log.map_or(usize::MAX, |l| {
            (l.chunk_bytes() as usize).saturating_sub(64).max(1)
        });
        loop {
            let (j, done) = self.at;
            if j == self.pages.len() {
                return Ok(IOResult::Done(()));
            }
            let total = ID + self.contents(j).len();
            if done == total {
                self.at = (j + 1, 0);
                continue;
            }
            let n = (total - done).min(piece);
            let mut admitted = match log {
                Some(log) => match log.admit(blob::record_len(n))? {
                    IOResult::Done(a) => Some(a),
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                },
                None => None,
            };
            let (pid, hint) = self.pages[j];
            let hint = crate::sync::atomic::AtomicU32::new(hint);
            let mut page = match pool.fix_exclusive(pid, Some(&hint))? {
                IOResult::Done(p) => p,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            self.pages[j].1 = page.hint();
            let bytes = self.piece(j, done, n);
            match (log, &mut admitted) {
                (Some(log), Some(admitted)) => {
                    blob::write(log, admitted, &mut page, HEADER_SIZE + done, &bytes);
                }
                _ => {
                    blob::apply(&mut page.page_mut(), HEADER_SIZE + done, &bytes);
                    page.changed(pool.temporary_lsn());
                }
            }
            self.at.1 += n;
        }
    }
}

impl Drop for ValueWrite {
    fn drop(&mut self) {
        if !self.adopted {
            for &(pid, _) in &self.pages {
                self.store.orphaned(pid);
            }
        }
    }
}

/// What reading a value came to.
pub enum Read {
    Value(Vec<u8>),
    /// A page of it no longer belongs to it: the value was replaced or removed since its
    /// reference was read.
    Changed,
    /// The leaf that named the value changed, or left memory, before a page of the value was
    /// read in: read the reference again. What was read so far stays, for the same reference.
    Unheld,
}

/// Checks that `page` is a blob page of the value `id`.
fn of_value<P: Bytes + ?Sized>(page: &P, id: u64) -> bool {
    page::kind(page) == blob::KIND
        && page.compare(HEADER_SIZE, &id.to_le_bytes()) == std::cmp::Ordering::Equal
}

/// Reads an out-of-line value, page by page, so a value larger than memory is read with every
/// page it holds still in place: what was copied stays valid while the id matches.
///
/// Its pages are named by the leaf that holds its reference, which can drop the value between
/// steps. A page is read in only after that leaf is seen unchanged in the same step: `holder`
/// (CONCURRENCY.md §6).
pub struct ValueRead {
    reference: ValueRef,
    out: Vec<u8>,
    chunks: Option<Vec<PageId>>,
}

/// A page of a value, fixed.
enum Fixed<'a> {
    Page(Optimistic<'a>),
    /// Its number is free: the value was removed since its reference was read.
    Freed,
    /// Not in memory, and its holder changed.
    Unheld,
}

impl ValueRead {
    pub fn new(reference: ValueRef) -> Self {
        Self {
            reference,
            out: Vec::new(),
            chunks: None,
        }
    }

    /// A read into `out`'s allocation, its bytes dropped.
    pub fn reusing(reference: ValueRef, mut out: Vec<u8>) -> Self {
        out.clear();
        out.reserve_exact(reference.len as usize);
        Self {
            reference,
            out,
            chunks: None,
        }
    }

    pub fn reference(&self) -> ValueRef {
        self.reference
    }

    /// Appends `len` bytes of value from page `pid`, if the page is still the value's.
    fn copy(
        &mut self,
        pool: &Arc<BufferPool>,
        pid: PageId,
        len: usize,
        holder: &dyn Fn() -> bool,
    ) -> Result<IOResult<Option<Read>>> {
        let page = match fix(pool, pid, holder)? {
            IOResult::Done(Fixed::Page(p)) => p,
            IOResult::Done(Fixed::Freed) => return Ok(IOResult::Done(Some(Read::Changed))),
            IOResult::Done(Fixed::Unheld) => return Ok(IOResult::Done(Some(Read::Unheld))),
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        };
        let (id, before) = (self.reference.id, self.out.len());
        let out = &mut self.out;
        let copied = page.read(|bytes| {
            if !of_value(bytes, id) || HEADER_SIZE + ID + len > bytes.len() {
                return false;
            }
            let start = out.len();
            out.resize(start + len, 0);
            bytes.copy_to(HEADER_SIZE + ID, &mut out[start..]);
            true
        });
        Ok(match copied {
            Some(true) => IOResult::Done(None),
            Some(false) => IOResult::Done(Some(Read::Changed)),
            // The page moved or was evicted while copied: copy it again.
            None => {
                self.out.truncate(before);
                IOResult::IO(IOCompletions(Completion::new_yield()))
            }
        })
    }

    pub fn step(
        &mut self,
        pool: &Arc<BufferPool>,
        holder: &dyn Fn() -> bool,
    ) -> Result<IOResult<Read>> {
        let len = self.reference.len as usize;
        let Some(count) = self.reference.chunks() else {
            return Ok(match self.copy(pool, self.reference.pid, len, holder)? {
                IOResult::Done(None) => IOResult::Done(Read::Value(std::mem::take(&mut self.out))),
                IOResult::Done(Some(stop)) => IOResult::Done(stop),
                IOResult::IO(io) => IOResult::IO(io),
            });
        };
        if self.chunks.is_none() {
            match directory(pool, self.reference, count, holder)? {
                IOResult::Done(Ok(chunks)) => self.chunks = Some(chunks),
                IOResult::Done(Err(stop)) => return Ok(IOResult::Done(stop)),
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
        let chunks = self.chunks.clone().unwrap();
        while self.out.len() < len {
            let i = self.out.len() / chunk();
            let n = (len - self.out.len()).min(chunk());
            match self.copy(pool, chunks[i], n, holder)? {
                IOResult::Done(None) => {}
                IOResult::Done(Some(stop)) => return Ok(IOResult::Done(stop)),
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
        Ok(IOResult::Done(Read::Value(std::mem::take(&mut self.out))))
    }
}

/// Fixes a page of a value that `holder` names.
fn fix<'a>(
    pool: &'a Arc<BufferPool>,
    pid: PageId,
    holder: &dyn Fn() -> bool,
) -> Result<IOResult<Fixed<'a>>> {
    match pool.fix_optimistic_held(pid, None, holder) {
        Ok(IOResult::Done(Some(p))) => Ok(IOResult::Done(Fixed::Page(p))),
        Ok(IOResult::Done(None)) => Ok(IOResult::Done(Fixed::Unheld)),
        Ok(IOResult::IO(io)) => Ok(IOResult::IO(io)),
        Err(LimboError::Corrupt(_)) => Ok(IOResult::Done(Fixed::Freed)),
        Err(e) => Err(e),
    }
}

/// The chunks a chunked value's directory lists; else why they could not be read.
fn directory(
    pool: &Arc<BufferPool>,
    reference: ValueRef,
    count: usize,
    holder: &dyn Fn() -> bool,
) -> Result<IOResult<std::result::Result<Vec<PageId>, Read>>> {
    let page = match fix(pool, reference.pid, holder)? {
        IOResult::Done(Fixed::Page(p)) => p,
        IOResult::Done(Fixed::Freed) => return Ok(IOResult::Done(Err(Read::Changed))),
        IOResult::Done(Fixed::Unheld) => return Ok(IOResult::Done(Err(Read::Unheld))),
        IOResult::IO(io) => return Ok(IOResult::IO(io)),
    };
    let at = HEADER_SIZE + ID;
    let read = page.read(|bytes| {
        if !of_value(bytes, reference.id) || at + 8 * count > bytes.len() {
            return Err(Read::Changed);
        }
        Ok(bytes
            .copied(at..at + 8 * count)
            .chunks_exact(8)
            .map(|c| PageId::from_raw(u64::from_le_bytes(c.try_into().unwrap())))
            .collect())
    });
    Ok(match read {
        Some(chunks) => IOResult::Done(chunks),
        // The page moved or was evicted while read: read it again.
        None => IOResult::IO(IOCompletions(Completion::new_yield())),
    })
}

/// Every page a value takes, which `holder` names; else why they could not be read.
pub fn pages(
    pool: &Arc<BufferPool>,
    reference: ValueRef,
    holder: &dyn Fn() -> bool,
) -> Result<IOResult<std::result::Result<Vec<PageId>, Read>>> {
    let Some(count) = reference.chunks() else {
        return Ok(IOResult::Done(Ok(vec![reference.pid])));
    };
    Ok(match directory(pool, reference, count, holder)? {
        IOResult::Done(Ok(chunks)) => {
            IOResult::Done(Ok(std::iter::once(reference.pid).chain(chunks).collect()))
        }
        IOResult::Done(Err(stop)) => IOResult::Done(Err(stop)),
        IOResult::IO(io) => IOResult::IO(io),
    })
}
