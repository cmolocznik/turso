//! Writing dirty pages back.
//!
//! A writeback takes dirty frames of one class, already marked WRITING by whoever chose them, so
//! they are neither evicted nor dropped while it runs. It plans their writes with the page store,
//! which takes fresh slots under a logged intent. Then it copies each page optimistically, checked
//! against its latch, and writes the copy. A page changed while its copy is in flight stays dirty.
//!
//! A write-out ([`Outgoing`]) is a writeback of a page leaving its frame for good: the frame is
//! held until the write lands, so the copy needs no check, and settling the write frees the frame.
//!
//! **Settling.** A write's completion only records that it landed and wakes the pool's waiters:
//! what follows (the remap, which latches metadata pages; the frame's state) is done when the
//! writeback is next stepped, where a latched page means a yield rather than a wait. So a
//! writeback stays queued until every write it submitted is settled, and whoever waits for a
//! write steps the queue when it wakes.
//!
//! No log flush comes first: a copy's remap is logged after the copy lands, so a remap that
//! survives a crash names a copy whose records survived too (DESIGN.md §6.3).

use super::super::aligned::AlignedBlock;
use super::super::log::Lsn;
use super::super::page::{PageId, SizeClass};
use super::super::store::WritePlan;
use super::frame::{EVICT_AFTER, RESIDENT, WRITING};
use super::{BufferPool, Outgoing};
use crate::io::Completion;
use crate::sync::atomic::Ordering;
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::CompletionError;

/// Writes that ended and are not yet settled, by position, with how each ended.
type Landed = Arc<Mutex<Vec<(usize, std::result::Result<(), CompletionError>)>>>;

/// What stepping a writeback came to.
pub enum Progress {
    /// Waiting on this before it can go on: IO before its writes are submitted, or a yield.
    Waiting(Completion),
    /// Its writes are in flight; each wakes the pool's waiters as it lands.
    InFlight,
    /// Every write is settled.
    Done,
}

pub struct Writeback {
    class: SizeClass,
    frames: Vec<u32>,
    pids: Vec<PageId>,
    /// Per frame, its write-out, for a page leaving the frame; `None` for a page that stays.
    outgoing: Vec<Option<Outgoing>>,
    evict_after: bool,
    /// The pages are temporary: written to the temporary files, with no plan.
    temporary: bool,
    plan: Option<WritePlan>,
    /// The next frame to copy and write.
    next: usize,
    /// Per frame, the LSN its copy holds changes up to, once taken.
    lsns: Vec<Lsn>,
    landed: Landed,
    /// Writes submitted and not yet settled.
    unsettled: usize,
    /// Done once every write is settled.
    pub finished: Completion,
}

impl Writeback {
    /// Writes back `frames`, holding `pids`; they must be marked WRITING already.
    pub fn new(
        class: SizeClass,
        frames: Vec<u32>,
        pids: Vec<PageId>,
        evict_after: bool,
        temporary: bool,
    ) -> Self {
        let finished = Completion::new_wait();
        if frames.is_empty() {
            finished.complete(0);
        }
        Self {
            class,
            outgoing: frames.iter().map(|_| None).collect(),
            lsns: vec![0; frames.len()],
            frames,
            pids,
            evict_after,
            temporary,
            plan: None,
            next: 0,
            landed: Arc::new(Mutex::new(Vec::new())),
            unsettled: 0,
            finished,
        }
    }

    /// Writes out `outgoing`, pages leaving their frames.
    pub fn out(class: SizeClass, outgoing: Vec<Outgoing>, temporary: bool) -> Self {
        let frames = outgoing.iter().map(Outgoing::frame).collect();
        let pids = outgoing.iter().map(Outgoing::pid).collect();
        let mut wb = Self::new(class, frames, pids, false, temporary);
        wb.outgoing = outgoing.into_iter().map(Some).collect();
        wb
    }

    /// Submits what it can, then settles what landed. It never fails: a failed write fails the
    /// pool, and the frames it has not written are given up.
    pub fn step(&mut self, pool: &Arc<BufferPool>) -> Progress {
        if let Some(c) = self.submit(pool) {
            return Progress::Waiting(c);
        }
        let landed = std::mem::take(&mut *self.landed.lock());
        let mut landed = landed.into_iter();
        while let Some((i, res)) = landed.next() {
            if let Some(plan) = &mut self.plan {
                if let IOResult::IO(IOCompletions(c)) = plan.settle(i, &res) {
                    let mut back = self.landed.lock();
                    back.push((i, res));
                    back.extend(landed);
                    return Progress::Waiting(c);
                }
            }
            match self.outgoing[i].take() {
                Some(out) => out.end(pool, self.lsns[i], res),
                None => pool.write_ended(
                    self.frames[i],
                    self.pids[i],
                    self.lsns[i],
                    self.evict_after,
                    res,
                ),
            }
            self.unsettled -= 1;
        }
        if self.next < self.frames.len() || self.unsettled > 0 {
            return Progress::InFlight;
        }
        // The plan held the metadata pages its remaps changed until now.
        self.plan = None;
        self.finished.complete(0);
        Progress::Done
    }

    /// Copies and writes every frame not yet written. Something to wait on if it cannot go on
    /// before then.
    fn submit(&mut self, pool: &Arc<BufferPool>) -> Option<Completion> {
        if self.next == self.frames.len() {
            return None;
        }
        if !self.temporary {
            let plan = self
                .plan
                .get_or_insert_with(|| pool.store.plan_writes(self.pids.clone()));
            match plan.step() {
                Ok(IOResult::Done(())) => {}
                Ok(IOResult::IO(IOCompletions(c))) => return Some(c),
                Err(e) => {
                    pool.fail(super::io_failure(&e));
                    self.abandon(pool);
                    return None;
                }
            }
        }
        let size = self.class.size();
        while self.next < self.frames.len() {
            let i = self.next;
            let frame = self.frames[i];
            let h = pool.header(frame);
            debug_assert_eq!(h.pid(), self.pids[i].raw());
            let copy = Arc::new(AlignedBlock::zeroed(size));
            let copy_frame = || {
                // SAFETY: the copy is fresh; the frame is held, or what is copied counts only
                // once validated.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        pool.memory.region.ptr(frame),
                        copy.as_ptr(),
                        size,
                    )
                };
                h.page_lsn.load(Ordering::Acquire)
            };
            let outgoing = self.outgoing[i].is_some();
            self.lsns[i] = if outgoing {
                // Held by its write-out: nothing changes it.
                copy_frame()
            } else {
                // The frame keeps its page while it is being written: the copy is whole once
                // nothing was written under the latch while it was taken.
                debug_assert_eq!(h.status(), RESIDENT);
                let Some(version) = h.latch.optimistic() else {
                    // A writer holds the page for a moment: come back.
                    return Some(Completion::new_yield());
                };
                let lsn = copy_frame();
                if !h.latch.validate(version) {
                    return Some(Completion::new_yield());
                }
                lsn
            };
            self.next += 1;
            self.unsettled += 1;
            pool.writes_in_flight.fetch_add(1, Ordering::AcqRel);
            let (landed, woken) = (self.landed.clone(), pool.clone());
            let ended = move |res: std::result::Result<(), CompletionError>| {
                landed.lock().push((i, res));
                woken.write_landed(frame, outgoing);
            };
            let written = match &self.plan {
                Some(plan) => plan.write(i, copy.region(0, size), ended).map(drop),
                None => pool.temp.write(self.pids[i], copy.region(0, size), ended),
            };
            if let Err(e) = written {
                // It never started: settled as failed, which fails the pool.
                self.landed.lock().push((i, Err(super::io_failure(&e))));
                self.abandon(pool);
                return None;
            }
        }
        None
    }

    /// Gives up every frame not yet written: they stay dirty, and pages being written out stay
    /// in their frames.
    fn abandon(&mut self, pool: &BufferPool) {
        for (&frame, out) in self.frames[self.next..]
            .iter()
            .zip(&mut self.outgoing[self.next..])
        {
            if out.take().is_none() {
                pool.header(frame)
                    .flags
                    .fetch_and(!(WRITING | EVICT_AFTER), Ordering::AcqRel);
            }
        }
        self.next = self.frames.len();
        pool.signal_released();
    }
}
