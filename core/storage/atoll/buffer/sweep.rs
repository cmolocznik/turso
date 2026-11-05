//! Writing back every dirty page at once, for tests: what eviction does to the pages it chooses,
//! done to all of them, so a test can drive the writeback machinery where it wants it.
//!
//! A sweep looks at every frame: a page clean when looked at is done; a dirty one is written, its
//! copy taken under a latch; one already being written, or written out of its frame, is looked at
//! again once its write lands.

use super::super::latch::Held;
use super::frame::{OUTGOING, RESIDENT, WRITING};
use super::BufferPool;
use crate::io::Completion;
use crate::storage::atoll::page::{PageId, SizeClass, CLASS_COUNT};
use crate::sync::atomic::Ordering;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::Result;

/// What a look at a frame found.
enum Look {
    Done,
    /// Marked writing for this sweep: a page of this class.
    Write(SizeClass),
    /// Being written by someone else, or written out: look again once the write lands.
    Writing,
    /// Latched exclusively for a change or a load: look again shortly.
    Busy,
}

/// A sweep over every frame; see the module docs.
pub struct Sweep {
    /// The next frame to look at for the first time.
    next: u32,
    /// Frames to look at again.
    again: Vec<u32>,
    /// The writebacks this sweep started.
    ours: Vec<Completion>,
}

impl Sweep {
    pub(super) fn new() -> Self {
        Self {
            next: 0,
            again: Vec::new(),
            ours: Vec::new(),
        }
    }

    pub fn step(&mut self, pool: &Arc<BufferPool>) -> Result<IOResult<()>> {
        let _step = pool.store.epochs().enter();
        pool.failed()?;
        // Waiting starts before the looks, so a write landing in between is not missed.
        let landed = pool.released_wait();
        let mut start: [Vec<u32>; CLASS_COUNT] = Default::default();
        let mut writing = false;
        let again = std::mem::take(&mut self.again);
        let first = self.next..pool.memory.grains();
        self.next = pool.memory.grains();
        for frame in again.into_iter().chain(first) {
            match look(pool, frame) {
                Look::Done => {}
                Look::Write(class) => start[class.index()].push(frame),
                Look::Writing => {
                    writing = true;
                    self.again.push(frame);
                }
                Look::Busy => self.again.push(frame),
            }
        }
        for (frames, class) in start.into_iter().zip(SizeClass::all()) {
            if let Some(c) = pool.write_back(class, frames, false) {
                self.ours.push(c);
            }
        }
        if let Some(c) = pool.drive_writebacks()? {
            return Ok(IOResult::IO(IOCompletions(c)));
        }
        self.ours.retain(|c| !c.finished());
        pool.failed()?;
        if !self.ours.is_empty() || writing {
            return Ok(IOResult::IO(IOCompletions(landed)));
        }
        if !self.again.is_empty() {
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        }
        Ok(IOResult::Done(()))
    }
}

fn look(pool: &BufferPool, frame: u32) -> Look {
    let h = pool.header(frame);
    let Some(version) = h.latch.optimistic() else {
        // A write-out holds its frame until the write lands; anything else holds it briefly.
        return if h.status() == OUTGOING {
            Look::Writing
        } else {
            Look::Busy
        };
    };
    let (status, dirty, writing) = (h.status(), h.dirty(), h.has(WRITING));
    let temporary = PageId::from_raw(h.pid()).is_temporary();
    if !h.latch.validate(version) {
        return Look::Busy;
    }
    // A temporary page is written only when it is evicted.
    if status != RESIDENT || (!dirty && !writing) || temporary {
        return Look::Done;
    }
    if writing {
        return Look::Writing;
    }
    // Held at the version seen, the frame keeps the page looked at: claim it if it is still dirty
    // and unclaimed. Nothing in the page changes, so readers are not sent back.
    let Some(hold) = Held::try_take_at(&h.latch, version) else {
        return Look::Busy;
    };
    let claimed = h.dirty() && h.flags.fetch_or(WRITING, Ordering::AcqRel) & WRITING == 0;
    let class = PageId::from_raw(h.pid()).class();
    hold.release_unchanged();
    if claimed {
        Look::Write(class)
    } else {
        Look::Busy
    }
}
