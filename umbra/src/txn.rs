//! Transactions over pages, logged at commit.
//!
//! A transaction holds every page it changes exclusively until it commits or aborts, so a page
//! with uncommitted changes is never evicted (no-steal) and the log holds committed changes
//! only. It keeps each page's bytes from before its first change, to put back on abort.
//!
//! At commit each page gets one record: its whole image when its LSN is at or below the log's
//! redo point (the first change since), else the byte ranges the transaction wrote, merged.

use crate::buffer::ExclusiveGuard;
use crate::page::{self, HEADER_SIZE};
use crate::wal::{Lsn, Record, Wal};
use std::io;

struct Held<'a> {
    page: ExclusiveGuard<'a>,
    before: Vec<u8>,
    /// Written ranges, sorted and merged.
    ranges: Vec<(usize, usize)>,
    image: bool,
}

pub struct Txn<'a> {
    wal: &'a Wal,
    pages: Vec<Held<'a>>,
}

/// A page taken into a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageRef(usize);

impl<'a> Txn<'a> {
    pub fn new(wal: &'a Wal) -> Self {
        Self { wal, pages: Vec::new() }
    }

    /// Takes a page into the transaction. `created` for a page allocated by it, which is logged
    /// whole.
    pub fn adopt(&mut self, page: ExclusiveGuard<'a>, created: bool) -> PageRef {
        let image = created || page::lsn(&page) <= self.wal.redo_point();
        self.pages.push(Held {
            before: page.to_vec(),
            page,
            ranges: Vec::new(),
            image,
        });
        PageRef(self.pages.len() - 1)
    }

    pub fn page(&self, p: PageRef) -> &[u8] {
        &self.pages[p.0].page
    }

    pub fn write(&mut self, p: PageRef, offset: usize, bytes: &[u8]) {
        assert!(offset >= HEADER_SIZE, "writing into the page header");
        self.pages[p.0].page[offset..offset + bytes.len()].copy_from_slice(bytes);
        self.mark(p, offset, offset + bytes.len());
    }

    /// Logs the swip at `offset` in `p`, which the buffer manager just pointed at a page this
    /// transaction allocated. The log gets the child's page id.
    pub fn link(&mut self, p: PageRef, offset: usize) {
        self.pages[p.0].page.mark_dirty();
        self.mark(p, offset, offset + 8);
    }

    /// The transaction's page `p`, to hand a swip in it to the buffer manager.
    pub fn guard(&self, p: PageRef) -> &ExclusiveGuard<'a> {
        &self.pages[p.0].page
    }

    fn mark(&mut self, p: PageRef, offset: usize, end: usize) {
        let held = &mut self.pages[p.0];
        let ranges = &mut held.ranges;
        let at = ranges.partition_point(|&(_, e)| e < offset);
        let mut merged = (offset, end);
        while at < ranges.len() && ranges[at].0 <= merged.1 {
            merged = (merged.0.min(ranges[at].0), merged.1.max(ranges[at].1));
            ranges.remove(at);
        }
        ranges.insert(at, merged);
    }

    /// Logs the changes, stamps the pages, lets them go, and waits for the log to be durable.
    pub fn commit(mut self) -> io::Result<Lsn> {
        let mut records = Vec::new();
        for held in &self.pages {
            let pid = held.page.pid();
            if held.image {
                // Stamped below with the frame's LSN; recovery sets it the same way.
                let bytes = held.page.durable_bytes(0..held.page.len());
                records.push(Record::Image { pid, bytes });
            } else {
                for &(start, end) in &held.ranges {
                    records.push(Record::Range {
                        pid,
                        offset: start as u32,
                        bytes: held.page.durable_bytes(start..end),
                    });
                }
            }
        }
        let lsn = match self.wal.append(&records) {
            Ok(lsn) => lsn,
            Err(e) => {
                self.abort();
                return Err(e);
            }
        };
        for held in &mut self.pages {
            page::set_lsn(&mut held.page, lsn);
        }
        self.pages.clear();
        self.wal.wait_durable(lsn)?;
        Ok(lsn)
    }

    /// Puts every page back as it was.
    pub fn abort(mut self) {
        for held in &mut self.pages {
            held.page.copy_from_slice(&held.before);
        }
    }
}
