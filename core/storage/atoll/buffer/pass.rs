//! Checkpoint passes over shards of frames (DESIGN.md §10; Haubenschild et al., SIGMOD 2020,
//! §3.4).
//!
//! The pool's frames are split into shards by position. Each shard has a position: every change
//! to a page in its frames that is not yet in a written copy has its log record start at or after
//! it. Recovery may start replaying at the lowest position of any shard.
//!
//! A pass over a shard records where the log ends, then makes sure that every change logged
//! before that is in a written copy: a page clean when looked at is done; a dirty one is written,
//! its copy taken under a latch taken after the end was recorded; one already being written, or
//! written out of its frame, is looked at again once its write lands. A change's record is
//! appended under the page's latch, so a change logged before the end is in the page by the time
//! a later latch is granted. Once the pass is done, the shard's position is that end.
//!
//! A page never moves to another frame, and so never to another shard: memory moves between size
//! classes only by evicting.

use super::super::latch::Held;
use super::super::log::Lsn;
use super::frame::{OUTGOING, RESIDENT, WRITING};
use super::BufferPool;
use crate::io::Completion;
use crate::storage::atoll::page::{PageId, SizeClass, CLASS_COUNT};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::Result;
use std::ops::Range;

pub(super) struct Shard {
    position: AtomicU64,
}

impl Shard {
    pub(super) fn new(position: Lsn) -> Self {
        Self {
            position: AtomicU64::new(position),
        }
    }

    pub(super) fn position(&self) -> Lsn {
        self.position.load(Ordering::SeqCst)
    }
}

/// What a look at a frame found.
enum Look {
    /// Every change logged before the pass began is in a written copy.
    Done,
    /// Marked writing for this pass: a page of this class.
    Write(SizeClass),
    /// Being written by someone else, or written out: look again once the write lands.
    Writing,
    /// Latched exclusively for a change or a load: look again shortly.
    Busy,
}

/// A pass over one shard; see the module docs.
pub struct Pass {
    shard: usize,
    /// Where the log ended when the pass began.
    end: Lsn,
    frames: Range<u32>,
    /// The next frame to look at for the first time.
    next: u32,
    /// Frames to look at again.
    again: Vec<u32>,
    /// The writebacks this pass started.
    ours: Vec<Completion>,
}

impl Pass {
    pub(super) fn new(pool: &BufferPool, shard: usize) -> Self {
        let end = pool.store.log().end();
        let frames = pool.shard_frames(shard);
        Self {
            shard,
            end,
            next: frames.start,
            frames,
            again: Vec::new(),
            ours: Vec::new(),
        }
    }

    /// Where the log ended when the pass began: the shard's position once it is done.
    pub fn end(&self) -> Lsn {
        self.end
    }

    pub fn step(&mut self, pool: &Arc<BufferPool>) -> Result<IOResult<()>> {
        let _step = pool.store.epochs().enter();
        pool.failed()?;
        // Waiting starts before the looks, so a write landing in between is not missed.
        let landed = pool.released_wait();
        let mut start: [Vec<u32>; CLASS_COUNT] = Default::default();
        let mut writing = false;
        let again = std::mem::take(&mut self.again);
        let first = self.next..self.frames.end;
        self.next = self.frames.end;
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
        if let Some(c) = self.ours.first() {
            return Ok(IOResult::IO(IOCompletions(c.clone())));
        }
        if writing {
            return Ok(IOResult::IO(IOCompletions(landed)));
        }
        if !self.again.is_empty() {
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        }
        self.finish(pool);
        Ok(IOResult::Done(()))
    }

    fn finish(&self, pool: &BufferPool) {
        pool.shards[self.shard]
            .position
            .store(self.end, Ordering::SeqCst);
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
    // A temporary page is written only when it is evicted, and holds back no redo point.
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
