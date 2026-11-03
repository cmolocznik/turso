//! Temporary pages (DESIGN.md §6.4): version indexes, graveyards and spills, which live only as
//! long as the process. Their changes are never logged and the page store never sees them. A
//! dirty one is written to its class's temporary file when it is evicted, at the place its number
//! names, and read back from there. The files are deleted at every open, and recovery never reads
//! them.

use super::super::bits::GrowBits;
use super::super::epoch::{Epochs, Retired};
use super::super::once::{SetOnce, TakeOnce};
use super::super::page::{PageId, SizeClass, CLASS_COUNT};
use super::super::stack::FreeList;
use super::super::whole;
use super::source::ReadDone;
use crate::io::{Buffer, Completion, File, OpenFlags, IO};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::{CompletionError, Result};
#[cfg(test)]
use std::collections::HashSet;

/// Lock-free: a class's numbers are a counter and a free list, the copies written a bitmap, and
/// each file is set once, the first opened kept.
pub struct TempPages {
    io: Arc<dyn IO>,
    base: String,
    classes: [Class; CLASS_COUNT],
    files: [SetOnce<Arc<dyn File>>; CLASS_COUNT],
    /// The next change's LSN. Nothing is logged, so these only order a page's changes and tell
    /// its frame dirty. They start at 1: 0 is a page never changed. Relaxed: only each one's
    /// uniqueness matters.
    lsn: AtomicU64,
    epochs: Arc<Epochs>,
    /// Numbers freed and not yet reusable: a step that could hold one may still run.
    retired: Retired<PageId>,
}

#[derive(Default)]
struct Class {
    /// Numbers from here on were never handed out. Relaxed: only each one's uniqueness matters.
    next: AtomicU64,
    /// Numbers given back and reusable.
    free: FreeList<u64>,
    /// Numbers whose page has a copy in the file.
    written: GrowBits,
}

fn path(base: &str, class: SizeClass) -> String {
    format!("{base}.t{}", class.index())
}

impl TempPages {
    /// Temporary pages in files beside `base`. Whatever an earlier process left there is deleted.
    pub fn new(io: Arc<dyn IO>, base: &str, epochs: Arc<Epochs>) -> Result<Self> {
        for class in SizeClass::all() {
            let path = path(base, class);
            // Created first, so the removal finds it whether or not an earlier process left one.
            drop(io.open_file(&path, OpenFlags::Create, false)?);
            io.remove_file(&path)?;
        }
        Ok(Self {
            io,
            base: base.to_string(),
            classes: Default::default(),
            files: Default::default(),
            lsn: AtomicU64::new(1),
            epochs,
            retired: Retired::default(),
        })
    }

    /// A new temporary page of `class`. It has no copy until it is first written: it is zeros.
    pub fn allocate(&self, class: SizeClass) -> PageId {
        for pid in self.retired.reclaim(&self.epochs) {
            self.classes[pid.class().index()]
                .free
                .push(pid.temporary_number());
        }
        let c = &self.classes[class.index()];
        let step = self.epochs.enter();
        let number = c
            .free
            .pop(&self.epochs, &step)
            .unwrap_or_else(|| c.next.fetch_add(1, Ordering::Relaxed));
        PageId::temporary(class, number)
    }

    /// Gives `pid`'s number back, once no step that could hold it is left. Its frame must be gone
    /// already. It has no copy from here on: a stale reader reads zeros.
    pub fn free(&self, pid: PageId) {
        self.classes[pid.class().index()]
            .written
            .clear(pid.temporary_number());
        self.retired.retire(&self.epochs, pid);
    }

    /// Every temporary page allocated and not freed.
    #[cfg(test)]
    pub fn allocated(&self) -> Vec<PageId> {
        let mut out = Vec::new();
        let retired: HashSet<PageId> = self.retired.waiting().into_iter().collect();
        for class in SizeClass::all() {
            let c = &self.classes[class.index()];
            let free: HashSet<u64> = c
                .free
                .items()
                .into_iter()
                .filter(|&n| !retired.contains(&PageId::temporary(class, n)))
                .chain(
                    retired
                        .iter()
                        .filter(|p| p.class() == class)
                        .map(|p| p.temporary_number()),
                )
                .collect();
            out.extend(
                (0..c.next.load(Ordering::Acquire))
                    .filter(|n| !free.contains(n))
                    .map(|n| PageId::temporary(class, n)),
            );
        }
        out
    }

    /// The LSN to stamp the next change to a temporary page with.
    pub fn next_lsn(&self) -> u64 {
        self.lsn.fetch_add(1, Ordering::Relaxed)
    }

    fn file(&self, class: SizeClass) -> Result<Arc<dyn File>> {
        let cell = &self.files[class.index()];
        if let Some(f) = cell.get() {
            return Ok(f);
        }
        // Two first users may both open it; the first to set it is kept.
        let file = self.io.open_file(
            &path(&self.base, class),
            OpenFlags::Create | OpenFlags::Direct,
            false,
        )?;
        Ok(cell.set(file))
    }

    fn at(pid: PageId) -> u64 {
        pid.temporary_number() * pid.class().size() as u64
    }

    /// Reads `pid` into `into`, exactly its size: its copy, or zeros if it has none.
    pub fn read(&self, pid: PageId, into: Arc<Buffer>, done: ReadDone) -> Result<()> {
        let class = pid.class();
        if !self.classes[class.index()]
            .written
            .get(pid.temporary_number())
        {
            into.as_mut_slice().fill(0);
            done(Ok(()));
            return Ok(());
        }
        let size = class.size();
        let done = TakeOnce::new(done);
        let c = Completion::new_read(into, move |res| {
            let done = done.take()?;
            match res {
                Ok((_, n)) if n as usize == size => {
                    done(Ok(()));
                    None
                }
                Ok((_, n)) => {
                    let e = CompletionError::ShortRead {
                        page_idx: pid.temporary_number() as usize,
                        expected: size,
                        actual: n.max(0) as usize,
                    };
                    done(Err(e));
                    Some(e)
                }
                Err(e) => {
                    done(Err(e));
                    Some(e)
                }
            }
        });
        drop(self.file(class)?.pread(Self::at(pid), c)?);
        Ok(())
    }

    /// Writes `copy` as `pid`'s copy; `done` runs once it is in the file, or the write failed.
    pub fn write<F>(self: &Arc<Self>, pid: PageId, copy: Arc<Buffer>, done: F) -> Result<()>
    where
        F: FnOnce(std::result::Result<(), CompletionError>) + Send + 'static,
    {
        let len = copy.len();
        // Weakly: the completion waits in our own IO, so a strong hold would keep pages dropped
        // with the write in flight alive, and for good once nothing steps that IO.
        let me = Arc::downgrade(self);
        let done = TakeOnce::new(done);
        let c = Completion::new_write(move |res| {
            let Some(done) = done.take() else {
                return;
            };
            let res = whole(len, res);
            if let (Ok(()), Some(me)) = (&res, me.upgrade()) {
                // Before `done`, which may evict the page: a read after that finds the copy.
                me.classes[pid.class().index()]
                    .written
                    .set(pid.temporary_number());
            }
            done(res);
        });
        drop(self.file(pid.class())?.pwrite(Self::at(pid), copy, c)?);
        Ok(())
    }
}
