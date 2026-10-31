//! The page store: pages written out of place, one file per size class, through Turso's [`File`].
//!
//! A page id names a page; a *slot* is a place in its class's file. Every write of a page goes to
//! a fresh slot, and the page table maps each page to the slot holding its current copy. The copy
//! a write replaces stays where it was: its slot is *retired*, and is reused only once the new
//! copy and the record of where it went are both durable (at a checkpoint, or at the end of
//! recovery). No write ever lands on a copy someone may still need, so a crash at any point leaves
//! a whole copy of every page, and no copy is ever torn. Slots belong to one class, so space is
//! only ever reused by a page of the same size.
//!
//! Every read and write is submitted and returns its [`Completion`]; nothing here waits. A
//! backend may run the callback inside the submission (the syscall and memory backends) or later
//! from `IO::step` (io_uring), so callbacks take no lock a submitter could hold.
//!
//! The page table and the free slots are held in memory. Recovery rebuilds them from the log's
//! remap records; checkpoints will write them out so the log can be cut. A recycled slot still
//! holds its old copy's bytes until it is written again: erasing them belongs with checkpoints.

use super::page::{self, PageId, SizeClass, CLASS_COUNT};
use crate::io::{Buffer, Completion, File, OpenFlags, IO};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::{CompletionError, Result};

/// What a read of a page's copy found.
pub enum PageRead {
    /// A whole copy of the page.
    Valid(Arc<Buffer>),
    /// No whole copy of the page: torn, never written, or the slot holds another page now.
    Invalid,
    /// The read itself failed.
    Failed(CompletionError),
}

#[derive(Default)]
struct Space {
    /// Page number → its slot plus one; zero for a page with no copy on disk.
    table: Vec<u64>,
    next_slot: u64,
    free: Vec<u64>,
    /// Slots of superseded copies, reusable once their successors are durable.
    retired: Vec<u64>,
}

impl Space {
    fn slot(&self, number: u64) -> Option<u64> {
        self.table
            .get(number as usize)
            .copied()
            .filter(|&s| s != 0)
            .map(|s| s - 1)
    }

    fn take_slot(&mut self) -> u64 {
        self.free.pop().unwrap_or_else(|| {
            self.next_slot += 1;
            self.next_slot - 1
        })
    }

    fn set(&mut self, number: u64, slot: u64) {
        let at = number as usize;
        if self.table.len() <= at {
            self.table.resize(at + 1, 0);
        }
        self.table[at] = slot + 1;
    }

    /// Points `number` at `slot`, retiring the slot it held.
    fn remap(&mut self, number: u64, slot: u64) {
        if let Some(old) = self.slot(number) {
            self.retired.push(old);
        }
        self.set(number, slot);
    }
}

struct ClassFile {
    file: Arc<dyn File>,
    pages: AtomicU64,
    space: Mutex<Space>,
}

pub struct PageStore {
    classes: Vec<ClassFile>,
    reads: AtomicU64,
    writes: AtomicU64,
}

/// Runs a callback once from a completion callback, which may be called through `Fn`.
struct Once<F>(Mutex<Option<F>>);

impl<F> Once<F> {
    fn new(f: F) -> Self {
        Self(Mutex::new(Some(f)))
    }

    fn take(&self) -> F {
        self.0
            .lock()
            .take()
            .expect("a completion runs its callback once")
    }
}

/// The page table a write completion updates, carried to whichever thread runs it.
#[derive(Clone, Copy)]
struct SpacePtr(*const Mutex<Space>);
// SAFETY: the store outlives the IO it submits (its owner waits for that IO before dropping it),
// and the table is behind its lock.
unsafe impl Send for SpacePtr {}
unsafe impl Sync for SpacePtr {}

impl SpacePtr {
    fn get(&self) -> &Mutex<Space> {
        // SAFETY: see above.
        unsafe { &*self.0 }
    }
}

impl PageStore {
    /// Opens or creates the class files `{base}.c{class}`. With `direct`, IO bypasses the OS
    /// page cache; frames and buffers are page-aligned, which O_DIRECT needs. The page table
    /// starts empty: recovery fills it.
    pub fn open(io: &Arc<dyn IO>, base: &str, direct: bool) -> Result<Self> {
        let mut classes = Vec::with_capacity(CLASS_COUNT);
        for class in SizeClass::all() {
            let file = io.open_file(
                &format!("{base}.c{}", class.index()),
                OpenFlags::Create,
                direct,
            )?;
            let slots = file.size()? / class.size() as u64;
            classes.push(ClassFile {
                file,
                pages: AtomicU64::new(0),
                space: Mutex::new(Space {
                    next_slot: slots,
                    ..Space::default()
                }),
            });
        }
        Ok(Self {
            classes,
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
        })
    }

    fn class(&self, class: SizeClass) -> &ClassFile {
        &self.classes[class.index()]
    }

    /// A new page of `class`, with no copy on disk until its first write.
    pub fn allocate(&self, class: SizeClass) -> PageId {
        PageId::new(
            class,
            self.class(class).pages.fetch_add(1, Ordering::Relaxed),
        )
    }

    /// The slot holding `pid`'s current copy.
    pub fn location(&self, pid: PageId) -> Option<u64> {
        self.class(pid.class()).space.lock().slot(pid.number())
    }

    /// Reads `pid`'s current copy; `done` gets it, `Invalid`, or the IO error.
    pub fn read<F>(&self, pid: PageId, done: F) -> Result<Completion>
    where
        F: FnOnce(PageRead) + Send + 'static,
    {
        match self.location(pid) {
            Some(slot) => self.read_slot(pid, slot, done),
            None => {
                done(PageRead::Invalid);
                let c = Completion::new_wait();
                c.complete(0);
                Ok(c)
            }
        }
    }

    /// Reads `slot` as a copy of `pid`.
    pub fn read_slot<F>(&self, pid: PageId, slot: u64, done: F) -> Result<Completion>
    where
        F: FnOnce(PageRead) + Send + 'static,
    {
        let class = pid.class();
        let buf = Arc::new(Buffer::new_temporary(class.size()));
        let done = Once::new(done);
        let c = Completion::new_read(buf, move |res| {
            let (buf, n) = match res {
                Ok(read) => read,
                Err(e) => {
                    (done.take())(PageRead::Failed(e));
                    return Some(e);
                }
            };
            let whole = n as usize == class.size() && page::verify(buf.as_slice(), pid);
            (done.take())(if whole {
                PageRead::Valid(buf)
            } else {
                PageRead::Invalid
            });
            None
        });
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.class(class).file.pread(slot * class.size() as u64, c)
    }

    /// Seals `buf` as a copy of `pid` and writes it to a fresh slot. Once it has landed the page
    /// table points at it and the slot of the copy it replaced is retired; `done` gets the slot.
    pub fn write<F>(&self, pid: PageId, buf: Arc<Buffer>, done: F) -> Result<Completion>
    where
        F: FnOnce(std::result::Result<u64, CompletionError>) + Send + 'static,
    {
        let class = pid.class();
        assert_eq!(buf.len(), class.size());
        page::seal(buf.as_mut_slice(), pid);
        let slot = self.class(class).space.lock().take_slot();
        let len = buf.len();
        let done = Once::new(done);
        let space = SpacePtr(&self.class(class).space);
        let c = Completion::new_write(move |res| {
            let res = match res {
                Ok(n) if n as usize == len => Ok(()),
                Ok(_) => Err(CompletionError::ShortWrite),
                Err(e) => Err(e),
            };
            {
                let mut space = space.get().lock();
                match res {
                    Ok(()) => space.remap(pid.number(), slot),
                    // Nothing refers to the slot: it can take the next write at once.
                    Err(_) => space.free.push(slot),
                }
            }
            (done.take())(res.map(|()| slot));
        });
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.class(class)
            .file
            .pwrite(slot * class.size() as u64, buf, c)
    }

    /// Drops `pid`: its copy's slot is retired.
    pub fn forget(&self, pid: PageId) {
        let mut space = self.class(pid.class()).space.lock();
        if let Some(slot) = space.slot(pid.number()) {
            space.retired.push(slot);
            space.table[pid.number() as usize] = 0;
        }
    }

    /// Makes the retired slots reusable. The caller has made every copy written so far, and the
    /// record of where it went, durable.
    pub fn recycle(&self) {
        for class in &self.classes {
            let mut space = class.space.lock();
            let retired = std::mem::take(&mut space.retired);
            space.free.extend(retired);
        }
    }

    /// Installs the page table recovery found for `class`: where each page's current copy is, and
    /// the next page number. Every other slot in the file is retired, to be reused once the
    /// remaps of the copies recovery wrote are durable ([`recycle`](Self::recycle)).
    pub fn restore(&self, class: SizeClass, next_page: u64, copies: &[(u64, u64)]) {
        let c = self.class(class);
        c.pages.store(next_page, Ordering::Relaxed);
        let mut space = c.space.lock();
        space.table.clear();
        space.retired.clear();
        for &(number, slot) in copies {
            space.set(number, slot);
            space.next_slot = space.next_slot.max(slot + 1);
        }
        let mut used = vec![false; space.next_slot as usize];
        for &s in space.table.iter().filter(|&&s| s != 0) {
            used[s as usize - 1] = true;
        }
        space.free.clear();
        space.retired = (0..space.next_slot)
            .rev()
            .filter(|&s| !used[s as usize])
            .collect();
    }

    /// Makes every write that completed before this call durable.
    pub fn sync(&self) -> Result<Completion> {
        let mut group = crate::io::CompletionGroup::new(|_| {});
        let mut syncs = Vec::with_capacity(self.classes.len());
        for _ in &self.classes {
            let c = Completion::new_sync(|_| {});
            group.add(&c);
            syncs.push(c);
        }
        let all = group.build();
        let mut syncs = syncs.into_iter();
        for class in &self.classes {
            let c = syncs.next().expect("one sync per class");
            if let Err(e) = class.file.sync(c, crate::io::FileSyncType::Fsync) {
                // The group finishes, failed, only once every child has.
                syncs.for_each(|c| c.abort());
                all.abort();
                return Err(e);
            }
        }
        Ok(all)
    }

    pub fn reads(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    pub fn writes(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }

    /// Slots held by superseded copies, waiting for their successors to be durable.
    pub fn retired(&self) -> usize {
        self.classes
            .iter()
            .map(|c| c.space.lock().retired.len())
            .sum()
    }
}
