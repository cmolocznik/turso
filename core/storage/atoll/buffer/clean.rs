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

use super::super::log::Lsn;
use super::super::page::{self, PageId, SizeClass};
use super::super::shared::Bytes;
use super::super::stack::Stack;
use super::super::store::WritePlan;
use super::frame::{EVICT_AFTER, RESIDENT, WRITING};
use super::{BufferPool, Outgoing};
use crate::io::Completion;
use crate::sync::atomic::Ordering;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::CompletionError;

/// Writes that ended and are not yet settled, by position, with how each ended.
pub(super) type Landed = Arc<Stack<(usize, std::result::Result<(), CompletionError>)>>;

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
    /// Run just before the writeback finishes, while its stepper holds it.
    #[cfg(test)]
    pub before_finish: Option<Box<dyn FnOnce() + Send>>,
    /// Run once a step has taken the writes landed so far, while its stepper holds it.
    #[cfg(test)]
    pub after_taking_landed: Option<Box<dyn FnOnce() + Send>>,
    /// Run as a step begins, while its stepper holds it.
    #[cfg(test)]
    pub before_step: Option<Box<dyn FnOnce() + Send>>,
    /// Which writeback, for a test that stalled to show.
    #[cfg(test)]
    pub id: u64,
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
            landed: Arc::new(Stack::default()),
            unsettled: 0,
            finished,
            #[cfg(test)]
            before_finish: None,
            #[cfg(test)]
            after_taking_landed: None,
            #[cfg(test)]
            before_step: None,
            #[cfg(test)]
            id: {
                static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            },
        }
    }

    /// The writes landed and not yet settled, shared with their completions.
    pub fn landings(&self) -> Landed {
        self.landed.clone()
    }

    /// Where it stands, for a test that stalled to show.
    #[cfg(test)]
    pub fn describe(&self) -> String {
        format!(
            "{:?} frames {:?} next {} unsettled {} landed {} outgoing {} plan {}",
            self.class,
            self.frames,
            self.next,
            self.unsettled,
            if self.landed.is_empty() {
                "none"
            } else {
                "some"
            },
            self.outgoing.iter().filter(|o| o.is_some()).count(),
            self.plan
                .as_ref()
                .map_or_else(|| "none".to_string(), WritePlan::describe),
        )
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
        // Stepped again by a thread that found it queued just before it was taken off.
        if self.finished.finished() {
            return Progress::Done;
        }
        #[cfg(test)]
        if let Some(hook) = self.before_step.take() {
            hook();
        }
        if let Some(c) = self.submit(pool) {
            return Progress::Waiting(c);
        }
        let landed = self.landed.take_all();
        #[cfg(test)]
        if let Some(hook) = self.after_taking_landed.take() {
            hook();
        }
        let mut landed = landed.into_iter();
        while let Some((i, res)) = landed.next() {
            if let Some(plan) = &mut self.plan {
                if let IOResult::IO(IOCompletions(c)) = plan.settle(i, &res) {
                    self.landed.push((i, res));
                    for rest in landed {
                        self.landed.push(rest);
                    }
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
        #[cfg(test)]
        if let Some(hook) = self.before_finish.take() {
            hook();
        }
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
            let outgoing = self.outgoing[i].is_some();
            let sealed = !self.temporary;
            // The buffer's room, given back when the write lands, or if it never starts.
            let mut held = None;
            let buffer = if outgoing {
                // Held by its write-out until the write ends, so nothing changes it: written
                // straight from its frame, sealed there.
                if sealed {
                    // SAFETY: the write-out holds the frame exclusively.
                    page::seal(&mut unsafe { pool.writer(frame, self.class) }, self.pids[i]);
                }
                self.lsns[i] = h.page_lsn.load(Ordering::Acquire);
                pool.memory.region.buffer(frame, size)
            } else {
                // The frame keeps its page, which may change while it is written: a copy, whole
                // once nothing was written under the latch while it was taken.
                debug_assert_eq!(h.status(), RESIDENT);
                // Room in the writeback buffer, which only writes in flight hold: each gives its
                // room back as it lands, and wakes the pool. Until then the rest wait unsubmitted.
                let Some(mut copy) = pool.staging.lease(size) else {
                    return None;
                };
                let Some(version) = h.latch.optimistic() else {
                    // A writer holds the page for a moment: come back.
                    return Some(Completion::new_yield());
                };
                pool.bytes(frame, self.class).copy_to(0, copy.bytes());
                let lsn = h.page_lsn.load(Ordering::Acquire);
                if !h.latch.validate(version) {
                    return Some(Completion::new_yield());
                }
                if sealed {
                    page::seal(copy.bytes(), self.pids[i]);
                }
                self.lsns[i] = lsn;
                held = Some(copy);
                held.as_ref().map(|c| c.buffer()).expect("just leased")
            };
            self.next += 1;
            self.unsettled += 1;
            pool.writes_in_flight.fetch_add(1, Ordering::AcqRel);
            let (landed, woken) = (self.landed.clone(), pool.clone());
            let ended = move |res: std::result::Result<(), CompletionError>| {
                // Before the wake, so a waiter woken for room finds it.
                drop(held);
                landed.push((i, res));
                woken.write_landed(frame, outgoing);
            };
            let written = match &self.plan {
                Some(plan) => plan.write(i, buffer, ended).map(drop),
                None => pool.temp.write(self.pids[i], buffer, ended),
            };
            if let Err(e) = written {
                // It never started: settled as failed, which fails the pool.
                self.landed.push((i, Err(super::io_failure(&e))));
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
