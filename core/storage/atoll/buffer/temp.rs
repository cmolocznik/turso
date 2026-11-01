//! Temporary pages (DESIGN.md §6.4): version indexes, graveyards and spills, which live only as
//! long as the process. Their changes are never logged and the page store never sees them. A
//! dirty one is written to its class's temporary file when it is evicted, at the place its number
//! names, and read back from there. The files are deleted at every open, and recovery never reads
//! them.

use super::super::epoch::{Epochs, Limbo};
use super::super::page::{PageId, SizeClass, CLASS_COUNT};
use super::super::whole;
use super::source::ReadDone;
use crate::io::{Buffer, Completion, File, OpenFlags, IO};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::{CompletionError, Result};
use std::collections::HashSet;

pub struct TempPages {
    io: Arc<dyn IO>,
    base: String,
    classes: [Mutex<Class>; CLASS_COUNT],
    files: Mutex<[Option<Arc<dyn File>>; CLASS_COUNT]>,
    /// The next change's LSN. Nothing is logged, so these only order a page's changes and tell
    /// its frame dirty. They start at 1: 0 is a page never changed.
    lsn: AtomicU64,
    epochs: Arc<Epochs>,
    /// Numbers freed and not yet reusable: a step that could hold one may still run.
    retired: Limbo<PageId>,
}

#[derive(Default)]
struct Class {
    /// Numbers from here on were never handed out.
    next: u64,
    free: Vec<u64>,
    /// Numbers whose page has a copy in the file.
    written: HashSet<u64>,
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
            files: Mutex::new(Default::default()),
            lsn: AtomicU64::new(1),
            epochs,
            retired: Limbo::default(),
        })
    }

    /// A new temporary page of `class`. It has no copy until it is first written: it is zeros.
    pub fn allocate(&self, class: SizeClass) -> PageId {
        for pid in self.retired.reclaim(&self.epochs) {
            self.classes[pid.class().index()]
                .lock()
                .free
                .push(pid.temporary_number());
        }
        let mut c = self.classes[class.index()].lock();
        let number = c.free.pop().unwrap_or_else(|| {
            c.next += 1;
            c.next - 1
        });
        PageId::temporary(class, number)
    }

    /// Gives `pid`'s number back, once no step that could hold it is left. Its frame must be gone
    /// already. It has no copy from here on: a stale reader reads zeros.
    pub fn free(&self, pid: PageId) {
        self.classes[pid.class().index()]
            .lock()
            .written
            .remove(&pid.temporary_number());
        self.retired.retire(&self.epochs, pid);
    }

    /// Every temporary page allocated and not freed.
    #[cfg(test)]
    pub fn allocated(&self) -> Vec<PageId> {
        let mut out = Vec::new();
        let retired: HashSet<PageId> = self.retired.waiting().into_iter().collect();
        for class in SizeClass::all() {
            let c = self.classes[class.index()].lock();
            let free: HashSet<u64> = c
                .free
                .iter()
                .copied()
                .filter(|&n| !retired.contains(&PageId::temporary(class, n)))
                .chain(
                    retired
                        .iter()
                        .filter(|p| p.class() == class)
                        .map(|p| p.temporary_number()),
                )
                .collect();
            out.extend(
                (0..c.next)
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
        if let Some(f) = &self.files.lock()[class.index()] {
            return Ok(f.clone());
        }
        // Opened outside the lock, which is never held across IO: two first users may both open
        // it, and the first to put it in the map is kept.
        let file = self.io.open_file(
            &path(&self.base, class),
            OpenFlags::Create | OpenFlags::Direct,
            false,
        )?;
        Ok(self.files.lock()[class.index()].get_or_insert(file).clone())
    }

    fn at(pid: PageId) -> u64 {
        pid.temporary_number() * pid.class().size() as u64
    }

    /// Reads `pid` into `into`, exactly its size: its copy, or zeros if it has none.
    pub fn read(&self, pid: PageId, into: Arc<Buffer>, done: ReadDone) -> Result<()> {
        let class = pid.class();
        if !self.classes[class.index()]
            .lock()
            .written
            .contains(&pid.temporary_number())
        {
            into.as_mut_slice().fill(0);
            done(Ok(()));
            return Ok(());
        }
        let size = class.size();
        let done = Mutex::new(Some(done));
        let c = Completion::new_read(into, move |res| {
            let done = done.lock().take()?;
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
        let me = self.clone();
        let done = Mutex::new(Some(done));
        let c = Completion::new_write(move |res| {
            let Some(done) = done.lock().take() else {
                return;
            };
            let res = whole(len, res);
            if res.is_ok() {
                // Before `done`, which may evict the page: a read after that finds the copy.
                me.classes[pid.class().index()]
                    .lock()
                    .written
                    .insert(pid.temporary_number());
            }
            done(res);
        });
        drop(self.file(pid.class())?.pwrite(Self::at(pid), copy, c)?);
        Ok(())
    }
}
