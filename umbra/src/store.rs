//! The page store: one file per size class.
//!
//! A page's slot is its offset divided by its size, so disk space is only ever reused by a page
//! of the same size. That is the rule Umbra's recovery section depends on: a smaller page written
//! where a larger one stood would leave recovery reading the larger page's bytes as a header.
//!
//! The free slots are held in memory only. Making them durable is the write-ahead log's job.

use crate::page::{self, PageId, SizeClass, CLASS_COUNT};
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub enum ReadError {
    Io(io::Error),
    /// The slot's bytes are not a whole page of its class: torn, or never written.
    Invalid(PageId),
}

impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

struct ClassFile {
    file: File,
    next: AtomicU64,
    free: Mutex<Vec<u64>>,
}

pub struct PageStore {
    classes: Vec<ClassFile>,
    reads: AtomicU64,
    writes: AtomicU64,
}

impl PageStore {
    /// Opens or creates the class files under `dir`. With `direct`, IO bypasses the OS page cache
    /// (O_DIRECT); buffers must then be aligned to the device's block size.
    pub fn open(dir: &Path, direct: bool) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut classes = Vec::with_capacity(CLASS_COUNT);
        for class in SizeClass::all() {
            let mut options = OpenOptions::new();
            options.read(true).write(true).create(true).truncate(false);
            if direct {
                options.custom_flags(libc::O_DIRECT);
            }
            let file = options.open(dir.join(format!("class-{}.pages", class.index())))?;
            let slots = file.metadata()?.len() / class.size() as u64;
            classes.push(ClassFile {
                file,
                next: AtomicU64::new(slots),
                free: Mutex::new(Vec::new()),
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

    pub fn allocate(&self, class: SizeClass) -> PageId {
        let c = self.class(class);
        let slot = c
            .free
            .lock()
            .pop()
            .unwrap_or_else(|| c.next.fetch_add(1, Ordering::Relaxed));
        PageId::new(class, slot)
    }

    pub fn free(&self, pid: PageId) {
        self.class(pid.class()).free.lock().push(pid.slot());
    }

    pub fn read(&self, pid: PageId, buf: &mut [u8]) -> Result<(), ReadError> {
        let class = pid.class();
        assert_eq!(buf.len(), class.size());
        self.class(class).file.read_exact_at(buf, pid.offset())?;
        self.reads.fetch_add(1, Ordering::Relaxed);
        if page::verify(buf, class) {
            Ok(())
        } else {
            Err(ReadError::Invalid(pid))
        }
    }

    /// Seals the page (class and checksum) and writes it to its slot.
    pub fn write(&self, pid: PageId, buf: &mut [u8]) -> io::Result<()> {
        let class = pid.class();
        page::seal(buf, class);
        self.class(class).file.write_all_at(buf, pid.offset())?;
        self.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn sync(&self) -> io::Result<()> {
        for c in &self.classes {
            c.file.sync_data()?;
        }
        Ok(())
    }

    pub fn reads(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    pub fn writes(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::HEADER_SIZE;

    fn class(i: usize) -> SizeClass {
        SizeClass::new(i).unwrap()
    }

    #[test]
    fn a_page_reads_back_as_written_in_every_class() {
        let dir = tempfile::tempdir().unwrap();
        let store = PageStore::open(dir.path(), false).unwrap();
        for c in SizeClass::all().take(4) {
            let pid = store.allocate(c);
            let mut page = vec![0u8; c.size()];
            page[HEADER_SIZE..].iter_mut().enumerate().for_each(|(i, b)| *b = (i % 251) as u8);
            store.write(pid, &mut page).unwrap();
            let mut back = vec![0u8; c.size()];
            store.read(pid, &mut back).unwrap();
            assert_eq!(back, page);
        }
    }

    #[test]
    fn a_freed_slot_is_reused_only_by_its_own_class() {
        let dir = tempfile::tempdir().unwrap();
        let store = PageStore::open(dir.path(), false).unwrap();
        let a = store.allocate(class(0));
        let b = store.allocate(class(0));
        let big = store.allocate(class(1));
        assert_eq!((a.slot(), b.slot(), big.slot()), (0, 1, 0));
        store.free(a);
        assert_eq!(store.allocate(class(1)).slot(), 1, "a larger page took a smaller one's slot");
        assert_eq!(store.allocate(class(0)), a);
    }

    #[test]
    fn a_torn_or_unwritten_slot_reads_as_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let store = PageStore::open(dir.path(), false).unwrap();
        let c = class(0);
        let pid = store.allocate(c);
        let mut page = vec![7u8; c.size()];
        store.write(pid, &mut page).unwrap();
        // The second half of the page never reached disk.
        let path = dir.path().join("class-0.pages");
        let file = OpenOptions::new().write(true).open(path).unwrap();
        file.write_all_at(&vec![0u8; c.size() / 2], (c.size() / 2) as u64).unwrap();
        let mut back = vec![0u8; c.size()];
        assert!(matches!(store.read(pid, &mut back), Err(ReadError::Invalid(p)) if p == pid));
        let never = store.allocate(c);
        file.write_all_at(&vec![0u8; c.size()], never.offset()).unwrap();
        assert!(matches!(store.read(never, &mut back), Err(ReadError::Invalid(_))));
    }

    #[test]
    fn reopening_continues_after_the_last_slot() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = PageStore::open(dir.path(), false).unwrap();
            for _ in 0..3 {
                let pid = store.allocate(class(0));
                store.write(pid, &mut vec![0u8; class(0).size()]).unwrap();
            }
        }
        let store = PageStore::open(dir.path(), false).unwrap();
        assert_eq!(store.allocate(class(0)).slot(), 3);
    }
}
