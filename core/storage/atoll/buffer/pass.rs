//! Checkpoint passes over shards of frames (DESIGN.md §10; Haubenschild et al., SIGMOD 2020,
//! §3.4).
//!
//! The pool's frames are split into shards by position. Each shard has a position: every change
//! to a page in its frames that is not yet in a written copy has its log record start at or after
//! it. Recovery may start replaying at the lowest position of any shard.
//!
//! A pass over a shard records where the log ends, then makes sure that every change logged
//! before that is in a written copy: a page clean when looked at is done; a dirty one is written,
//! its copy taken under a latch taken after the end was recorded; one already being written is
//! looked at again once its write lands. A change's record is appended under the page's latch, so
//! a change logged before the end is in the page by the time a later latch is granted. Once the
//! pass is done, the shard's position is that end.
//!
//! A dirty page can move to a frame of another shard while memory moves between size classes.
//! It takes its old shard's position along: the new shard's position falls to it if lower, and a
//! pass over the new shard already running keeps it too.

use super::super::log::Lsn;
use super::frame::{RESIDENT, WRITING};
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
    /// The lowest position of a shard a dirty page moved here from since the pass in progress
    /// began.
    incoming: AtomicU64,
}

impl Shard {
    pub(super) fn new(position: Lsn) -> Self {
        Self {
            position: AtomicU64::new(position),
            incoming: AtomicU64::new(u64::MAX),
        }
    }

    pub(super) fn position(&self) -> Lsn {
        self.position.load(Ordering::SeqCst)
    }

    /// A dirty page moved here from a shard at `position`.
    pub(super) fn arrived(&self, position: Lsn) {
        // `incoming` first: a pass ending meanwhile reads it after setting the position.
        self.incoming.fetch_min(position, Ordering::SeqCst);
        self.position.fetch_min(position, Ordering::SeqCst);
    }
}

/// What a look at a frame found.
enum Look {
    /// Every change logged before the pass began is in a written copy.
    Done,
    /// Marked writing for this pass: a page of this class.
    Write(SizeClass),
    /// Being written by someone else: look again once the write lands.
    Writing,
    /// Latched exclusively for a change, a load or a move: look again shortly.
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
    /// TEMPORARY DIAGNOSIS: steps taken.
    steps: u64,
}

impl Pass {
    pub(super) fn new(pool: &BufferPool, shard: usize) -> Self {
        let s = &pool.shards[shard];
        // Before the end is read: a page moving here after this is kept by `incoming`, and one
        // that moved before is in the shard's frames when they are looked at.
        s.incoming.store(u64::MAX, Ordering::SeqCst);
        let end = pool.store.log().end();
        let frames = pool.shard_frames(shard);
        Self {
            shard,
            end,
            next: frames.start,
            frames,
            again: Vec::new(),
            ours: Vec::new(),
            steps: 0,
        }
    }

    /// Where the log ended when the pass began: the shard's position once it is done.
    pub fn end(&self) -> Lsn {
        self.end
    }

    pub fn step(&mut self, pool: &Arc<BufferPool>) -> Result<IOResult<()>> {
        pool.failed()?;
        self.steps += 1;
        if self.steps == 2_000_000 {
            let mut out = String::new();
            for &f in &self.again {
                let h = pool.header(f);
                out.push_str(&format!(
                    "frame {f}: owner site {} thread {} pid {:?} status {} flags {:#x} dirty {} latch {:?} page_lsn {} written {}\n",
                    h.owner.load(Ordering::Relaxed) >> 32,
                    h.owner.load(Ordering::Relaxed) & 0xffff_ffff,
                    PageId::from_raw(h.pid()),
                    h.status(),
                    h.flags.load(Ordering::Acquire),
                    h.dirty(),
                    h.latch.optimistic(),
                    h.page_lsn.load(Ordering::Acquire),
                    h.written_lsn.load(Ordering::Acquire),
                ));
            }
            panic!(
                "TEMPORARY DIAGNOSIS: pass over shard {} stuck; again {:?}\n{out}ours {:?}; writebacks {}; in flight {}",
                self.shard,
                self.again,
                self.ours.iter().map(|c| c.finished()).collect::<Vec<_>>(),
                pool.writebacks.lock().len(),
                pool.writes_in_flight.load(Ordering::Acquire)
            );
        }
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
        let s = &pool.shards[self.shard];
        s.position.store(
            self.end.min(s.incoming.load(Ordering::SeqCst)),
            Ordering::SeqCst,
        );
        // A page that arrived after `incoming` was read lowered the position before this store,
        // and is in `incoming` now.
        s.position
            .fetch_min(s.incoming.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

fn look(pool: &BufferPool, frame: u32) -> Look {
    let h = pool.header(frame);
    let Some(version) = h.latch.optimistic() else {
        return Look::Busy;
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
    if !h.latch.try_lock_shared() {
        return Look::Busy;
    }
    // Under the latch the frame keeps its page: claim it if it is still dirty and unclaimed.
    let claimed = h.status() == RESIDENT
        && h.dirty()
        && h.flags.fetch_or(WRITING, Ordering::AcqRel) & WRITING == 0;
    let class = PageId::from_raw(h.pid()).class();
    h.latch.unlock_shared();
    if claimed {
        Look::Write(class)
    } else {
        Look::Busy
    }
}
