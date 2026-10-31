//! Writing dirty pages back.
//!
//! A writeback takes dirty frames of one class, already marked WRITING by whoever chose them, so
//! they are neither evicted nor dropped while it runs. It plans their writes with the page store,
//! which takes fresh slots under a logged intent. Then it copies each page under a shared latch,
//! held only for the copy, and writes the copy. A page changed while its copy is in flight stays
//! dirty.
//!
//! No log flush comes first: a copy's remap is logged after the copy lands, so a remap that
//! survives a crash names a copy whose records survived too (DESIGN.md §6.3).

use super::super::aligned::AlignedBlock;
use super::super::page::{PageId, SizeClass};
use super::super::store::WritePlan;
use super::frame::RESIDENT;
use super::BufferPool;
use crate::io::Completion;
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::Result;

pub struct Writeback {
    class: SizeClass,
    frames: Vec<u32>,
    pids: Vec<PageId>,
    evict_after: bool,
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
    pub fn new(class: SizeClass, frames: Vec<u32>, pids: Vec<PageId>, evict_after: bool) -> Self {
        let finished = Completion::new_wait();
        if frames.is_empty() {
            finished.complete(0);
        }
        Self {
            class,
            pending: Arc::new(AtomicUsize::new(frames.len())),
            frames,
            pids,
            evict_after,
            plan: None,
            next: 0,
            finished,
        }
    }

    /// Done once every write is submitted; [`finished`](Self::finished) says when they end.
    pub fn step(&mut self, pool: &Arc<BufferPool>) -> Result<IOResult<()>> {
        if self.next == self.frames.len() {
            return Ok(IOResult::Done(()));
        }
        let plan = self
            .plan
            .get_or_insert_with(|| Arc::new(Mutex::new(pool.store.plan_writes(self.pids.clone()))))
            .clone();
        if let IOResult::IO(io) = plan.lock().step()? {
            return Ok(IOResult::IO(io));
        }
        let size = self.class.size();
        while self.next < self.frames.len() {
            let i = self.next;
            let frame = self.frames[i];
            let h = &pool.headers[frame as usize];
            if !h.latch.try_lock_shared() {
                // A writer holds the page for a moment: come back.
                return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
            }
            debug_assert_eq!(h.status(), RESIDENT);
            debug_assert_eq!(h.pid(), self.pids[i].raw());
            let copy = Arc::new(AlignedBlock::zeroed(size));
            // SAFETY: the shared latch keeps writers out of the frame; the copy is fresh.
            unsafe {
                std::ptr::copy_nonoverlapping(pool.memory.region.ptr(frame), copy.as_ptr(), size)
            };
            let lsn = h.page_lsn.load(Ordering::Acquire);
            h.latch.unlock_shared();
            self.next += 1;
            let ended = pool.clone();
            let pending = self.pending.clone();
            let finished = self.finished.clone();
            let evict_after = self.evict_after;
            pool.writes_in_flight.fetch_add(1, Ordering::AcqRel);
            let written = WritePlan::write(&plan, i, copy.region(0, size), move |res| {
                ended.write_ended(frame, lsn, evict_after, res.map(|_| ()));
                if pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                    finished.complete(0);
                }
            });
            match written {
                Ok(c) => drop(c),
                Err(e) => {
                    pool.write_ended(frame, lsn, false, Err(super::io_failure(&e)));
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

    /// Gives up every frame not yet written: they stay dirty.
    pub fn abandon(&mut self, pool: &BufferPool) {
        for &frame in &self.frames[self.next..] {
            pool.headers[frame as usize].flags.fetch_and(
                !(super::frame::WRITING | super::frame::EVICT_AFTER),
                Ordering::AcqRel,
            );
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
