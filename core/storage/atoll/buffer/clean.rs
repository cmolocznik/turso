//! Writing dirty pages back.
//!
//! A writeback takes dirty frames of one class, already marked WRITING by whoever chose them, so
//! they are neither evicted nor dropped while it runs. It plans their writes with the page store,
//! which takes fresh slots under a logged intent. Then it copies each page optimistically, checked
//! against its latch, and writes the copy. A page changed while its copy is in flight stays dirty.
//!
//! A write-out ([`Outgoing`]) is a writeback of a page leaving its frame for good: the frame is
//! held until the write lands, so the copy needs no check, and the write's end frees the frame.
//!
//! No log flush comes first: a copy's remap is logged after the copy lands, so a remap that
//! survives a crash names a copy whose records survived too (DESIGN.md §6.3).

use super::super::aligned::AlignedBlock;
use super::super::page::{PageId, SizeClass};
use super::super::store::WritePlan;
use super::frame::RESIDENT;
use super::{BufferPool, Outgoing};
use crate::io::Completion;
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, Result};

pub struct Writeback {
    class: SizeClass,
    frames: Vec<u32>,
    pids: Vec<PageId>,
    /// Per frame, its write-out, for a page leaving the frame; `None` for a page that stays.
    outgoing: Vec<Option<Outgoing>>,
    evict_after: bool,
    /// The pages are temporary: written to the temporary files, with no plan.
    temporary: bool,
    plan: Option<Arc<Mutex<WritePlan>>>,
    /// The next frame to copy and write.
    next: usize,
    /// Writes not yet ended.
    pending: Arc<AtomicUsize>,
    /// Done once every write has ended.
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
            pending: Arc::new(AtomicUsize::new(frames.len())),
            outgoing: frames.iter().map(|_| None).collect(),
            frames,
            pids,
            evict_after,
            temporary,
            plan: None,
            next: 0,
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

    /// Done once every write is submitted; [`finished`](Self::finished) says when they end.
    pub fn step(&mut self, pool: &Arc<BufferPool>) -> Result<IOResult<()>> {
        if self.next == self.frames.len() {
            return Ok(IOResult::Done(()));
        }
        let plan = if self.temporary {
            None
        } else {
            let plan = self
                .plan
                .get_or_insert_with(|| {
                    Arc::new(Mutex::new(pool.store.plan_writes(self.pids.clone())))
                })
                .clone();
            if let IOResult::IO(io) = plan.lock().step()? {
                return Ok(IOResult::IO(io));
            }
            Some(plan)
        };
        let size = self.class.size();
        while self.next < self.frames.len() {
            let i = self.next;
            let frame = self.frames[i];
            let h = &pool.frames.headers[frame as usize];
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
            let out = self.outgoing[i].take();
            let lsn = if out.is_some() {
                // Held by its write-out: nothing changes it.
                copy_frame()
            } else {
                // The frame keeps its page while it is being written: the copy is whole once
                // nothing was written under the latch while it was taken.
                debug_assert_eq!(h.status(), RESIDENT);
                let Some(version) = h.latch.optimistic() else {
                    // A writer holds the page for a moment: come back.
                    return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
                };
                let lsn = copy_frame();
                if !h.latch.validate(version) {
                    return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
                }
                lsn
            };
            self.next += 1;
            let ended = pool.clone();
            let pending = self.pending.clone();
            let finished = self.finished.clone();
            let evict_after = self.evict_after;
            let pid = self.pids[i];
            pool.writes_in_flight.fetch_add(1, Ordering::AcqRel);
            let done = move |res: std::result::Result<(), CompletionError>| {
                match out {
                    Some(out) => out.end(&ended, lsn, res),
                    None => ended.write_ended(frame, pid, lsn, evict_after, res),
                }
                if pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                    finished.complete(0);
                }
            };
            let written = match &plan {
                Some(plan) => WritePlan::write(plan, i, copy.region(0, size), move |res| {
                    done(res.map(|_| ()))
                })
                .map(drop),
                None => pool.temp.write(self.pids[i], copy.region(0, size), done),
            };
            match written {
                Ok(()) => {}
                Err(e) => {
                    // A write-out's frame was given back when its `done` was dropped unrun.
                    pool.write_ended(frame, pid, lsn, false, Err(super::io_failure(&e)));
                    if self.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                        self.finished.complete(0);
                    }
                    return Err(e);
                }
            }
        }
        // The writes in flight hold the plan until they end.
        self.plan = None;
        Ok(IOResult::Done(()))
    }

    /// Gives up every frame not yet written: they stay dirty, and pages being written out stay
    /// in their frames.
    pub fn abandon(&mut self, pool: &BufferPool) {
        for (&frame, out) in self.frames[self.next..]
            .iter()
            .zip(&mut self.outgoing[self.next..])
        {
            if out.take().is_none() {
                pool.frames.headers[frame as usize].flags.fetch_and(
                    !(super::frame::WRITING | super::frame::EVICT_AFTER),
                    Ordering::AcqRel,
                );
            }
        }
        let left = self.frames.len() - self.next;
        self.next = self.frames.len();
        self.plan = None;
        if left > 0 && self.pending.fetch_sub(left, Ordering::AcqRel) == left {
            self.finished.complete(0);
        }
        pool.signal_released();
    }
}
