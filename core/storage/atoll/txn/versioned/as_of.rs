//! A versioned leaf as of a snapshot (audit 17, stage 2): what a checkpoint writes of it.
//!
//! Each key keeps the version a snapshot taken at the checkpoint's timestamp reads: the leaf's
//! own where that snapshot sees it, else the one below it in the arenas that it does. A key the
//! snapshot reads as absent or deleted is dropped. Every version kept is settled, its header
//! [`Version::SETTLED`], since every snapshot after a recovery from the checkpoint sees it.
//!
//! Where every version the leaf holds is one the oldest snapshot running sees, and so every snapshot
//! running or taken from now on ([`Transactions::oldest_snapshot`]), the image reads to every
//! reader exactly as the leaf does, and stands for it ([`AsOf::adoptable`]): the leaf is owed
//! nothing more until it changes, and once evicted is read back as the image. An older snapshot
//! that reads below a version the leaf holds would read the image's settled version instead.
//!
//! It is built from a copy of the leaf's bytes, taken under its latch, so writers go on while
//! the arenas are read. The arena entries it follows stay while the snapshot is held. It fits the
//! page, whose reserves kept room for every version it puts back ([`super`]: `reserve`).

use super::{below, corrupt_header, Version, HEADER};
use crate::storage::atoll::btree::node::{self, Entry, Layout, Limits, Shape, View};
use crate::storage::atoll::btree::{Stored, StoredValue};
use crate::storage::atoll::buffer::BufferPool;
use crate::storage::atoll::page::{self, SizeClass};
use crate::storage::atoll::shared::Bytes;
use crate::storage::atoll::txn::{Transactions, Ts};
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};

macro_rules! done {
    ($e:expr) => {
        match $e? {
            IOResult::Done(v) => v,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    };
}

/// A leaf's image as of a snapshot: step it with [`AsOf::step`] until done, then take it with
/// [`AsOf::image`].
pub struct AsOf {
    at: Ts,
    header: [u8; page::HEADER_SIZE],
    len: usize,
    shape: Shape,
    /// The leaf's entries as copied, in key order.
    copied: Vec<Entry>,
    /// The next entry to judge.
    next: usize,
    /// The version of the next entry whose predecessor is read next: one the snapshot does not
    /// see.
    chain: Option<Version>,
    /// The entries kept, settled.
    kept: Vec<Entry>,
    /// Per slot, the start of its last transaction the snapshot sees, once read.
    seen: Vec<Option<Ts>>,
    /// Whether every version the leaf holds is seen by every snapshot running or to come.
    adoptable: bool,
    /// At or below every snapshot running or to come, `at` at most, once read.
    oldest: Option<Ts>,
}

impl AsOf {
    /// The image as of the snapshot taken at `at` of the versioned leaf `page`, a copy of its
    /// bytes.
    pub fn new<P: Bytes + ?Sized>(page: &P, at: Ts) -> Result<Self> {
        let corrupt = || LimboError::Corrupt(format!("a leaf as of {at} is no leaf"));
        let view = View::new(page);
        if !view.is_node() || !view.is_leaf() {
            return Err(corrupt());
        }
        let mut header = [0u8; page::HEADER_SIZE];
        page.copy_to(0, &mut header);
        Ok(Self {
            at,
            header,
            len: page.len(),
            shape: node::shape(page),
            copied: view.entries().ok_or_else(corrupt)?,
            next: 0,
            chain: None,
            kept: Vec::new(),
            seen: Vec::new(),
            adoptable: true,
            oldest: None,
        })
    }

    /// Whether the version written by the slot's transaction that started at `start` is the
    /// snapshot's to see.
    fn sees(&mut self, txns: &Transactions, v: &Version) -> Result<bool> {
        let slot = v.slot as usize;
        let log = txns.slots.get(slot).ok_or_else(|| {
            LimboError::Corrupt(format!("a version of slot {slot}, past the slots"))
        })?;
        if self.seen.len() <= slot {
            self.seen.resize(slot + 1, None);
        }
        let last = *self.seen[slot].get_or_insert_with(|| log.last_at(self.at));
        Ok(v.ts <= last)
    }

    /// Judges every entry, following the arenas where the snapshot does not see the leaf's.
    pub fn step(&mut self, txns: &Transactions, pool: &Arc<BufferPool>) -> Result<IOResult<()>> {
        while self.next < self.copied.len() {
            let v = match self.chain {
                Some(v) => v,
                None => {
                    let e = &self.copied[self.next];
                    let v = e
                        .value
                        .get(..HEADER)
                        .and_then(Version::decode)
                        .ok_or_else(corrupt_header)?;
                    let sees = self.sees(txns, &v)?;
                    // Read after the cut, the horizon at or above `at`.
                    let oldest = *self
                        .oldest
                        .get_or_insert_with(|| txns.oldest_snapshot(self.at));
                    self.adoptable &= txns.committed_by(v.slot, v.ts, oldest);
                    if sees {
                        let e = &mut self.copied[self.next];
                        let (key, value) =
                            (std::mem::take(&mut e.key), std::mem::take(&mut e.value));
                        let out_of_line = e.out_of_line;
                        self.keep(&v, key, &value[HEADER..], out_of_line);
                        continue;
                    }
                    v
                }
            };
            let Some(Stored { head, value }) = done!(below(pool, &v)) else {
                self.chain = None;
                self.next += 1;
                continue;
            };
            let below = Version::decode(&head).ok_or_else(corrupt_header)?;
            if !self.sees(txns, &below)? {
                self.chain = Some(below);
                continue;
            }
            self.chain = None;
            let key = std::mem::take(&mut self.copied[self.next].key);
            match value {
                StoredValue::Inline(value) => self.keep(&below, key, &value, false),
                StoredValue::OutOfLine(r) => self.keep(&below, key, &r.encode(), true),
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Keeps `key` at version `v`, its value or reference `rest`, unless `v` deletes it.
    fn keep(&mut self, v: &Version, key: Vec<u8>, rest: &[u8], out_of_line: bool) {
        self.next += 1;
        if v.deleted {
            return;
        }
        let mut value = Vec::with_capacity(HEADER + rest.len());
        value.extend_from_slice(&Version::SETTLED.encode());
        value.extend_from_slice(rest);
        self.kept.push(Entry {
            key,
            value,
            out_of_line,
        });
    }

    /// Once judged whole: whether every version the leaf held is seen by every snapshot running or
    /// taken from now on, the snapshot's included, so that the image, each version settled and each
    /// deleted key gone, reads to every reader as the leaf does.
    pub fn adoptable(&self) -> bool {
        assert_eq!(self.next, self.copied.len(), "a leaf not yet judged whole");
        self.adoptable
    }

    /// The leaf's image, its page header as copied: what [`step`](Self::step) kept, in the
    /// leaf's layout.
    pub fn image(self) -> Result<Vec<u8>> {
        assert_eq!(
            self.next,
            self.copied.len(),
            "an image not yet judged whole"
        );
        let Shape {
            root,
            level,
            lower,
            upper,
            next,
            reserving,
        } = &self.shape;
        let layout = Layout {
            root: *root,
            level: *level,
            lower: lower.as_deref(),
            upper: upper.as_deref(),
            next: *next,
            reserving: *reserving,
        };
        let class = SizeClass::fitting(self.len)
            .filter(|c| c.size() == self.len)
            .ok_or_else(|| LimboError::Corrupt(format!("a leaf of {} bytes", self.len)))?;
        let (needs, capacity) = (layout.size(&self.kept), Limits::of(class).capacity);
        if needs > capacity {
            return Err(LimboError::Corrupt(format!(
                "a leaf as of {} takes {needs} bytes of its page's {capacity}: its reserves kept \
                 too little",
                self.at
            )));
        }
        let mut out = vec![0u8; self.len];
        out[..page::HEADER_SIZE].copy_from_slice(&self.header);
        node::build(&mut out[..], &layout, &self.kept);
        Ok(out)
    }
}
