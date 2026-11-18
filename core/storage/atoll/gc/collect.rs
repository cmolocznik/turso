//! The collector of the lists of retired values (DESIGN.md §11.5; C, the auditor's #6174 and
//! #6186). Per slot it keeps:
//!
//! - **a claim**, a mutex only ever tried: held within a step, never across a returned IOResult;
//! - **the head**, the position of the first item not yet freed, and its progress, the pages of
//!   its value freed so far;
//! - **the due word**, which a begin's look reads to learn in one or two loads whether the slot has
//!   anything to free. Only the claim's holder sets it. It holds one of three things: [`EMPTY`]
//!   and a position e, when every item before e is freed (the slot has work again once its list's
//!   end has moved past e); [`PENDING`] and a start s, when the head's writer started at s and was
//!   running when last looked at; or, below both, the commit timestamp of the head's writer.
//!
//! A step that finds the word "pending, start s" and the slot's running word moved on reads the
//! list's end and its head item again after that load. A rollback cuts its items before its slot
//! is let go, and the next transaction appends at the same positions, so a head item still s's
//! means s committed; then no published pair for s is a fault.
//!
//! A step frees the head's value a page at a time, once every snapshot sees its writer's commit,
//! and stops at the first item whose writer the cutoff does not see. Each page's free and the
//! head's progress are one change inside the store's change gate, so a checkpoint's cut, which
//! closes the gate, finds both or neither.

use super::super::btree::value::RetiredFree;
use super::super::buffer::BufferPool;
use super::super::store::PageStore;
use super::super::txn::Ts;
use super::list::{Lists, Reader};
use super::Writers;
use crate::storage::atoll::locks::Mutex;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::types::IOResult;
use crate::{LimboError, Result};
use crossbeam_utils::CachePadded;

/// The due word's mark for a list freed up to the position beside it.
pub const EMPTY: u64 = 1 << 63;
/// The due word's mark for a head whose writer, started at the timestamp beside it, was running.
pub const PENDING: u64 = 3 << 62;
const MARK: u64 = 3 << 62;

/// What a due word holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Due {
    /// Every item before this position is freed.
    Empty(u64),
    /// The head's writer started at this timestamp, and was running when last looked at.
    Pending(Ts),
    /// The head's writer committed at this timestamp.
    Commit(Ts),
}

impl Due {
    pub fn decode(word: u64) -> Self {
        match word & MARK {
            EMPTY => Due::Empty(word & !MARK),
            PENDING => Due::Pending(word & !MARK),
            _ => Due::Commit(word),
        }
    }

    fn encode(self) -> u64 {
        match self {
            Due::Empty(at) => EMPTY | at,
            Due::Pending(start) => PENDING | start,
            Due::Commit(ts) => ts,
        }
    }
}

/// Every slot's collector state.
pub struct Collector {
    slots: Box<[CachePadded<SlotState>]>,
}

struct SlotState {
    due: AtomicU64,
    head: AtomicU64,
    progress: AtomicU64,
    claim: Mutex<Work>,
}

/// What the claim's holder keeps between its steps.
#[derive(Default)]
struct Work {
    /// Its place in the list, at the head.
    reader: Reader,
    /// The writer's start the due word was last set for.
    due_for: Option<Ts>,
    /// The head's value being freed, once begun.
    free: Option<RetiredFree>,
}

impl Collector {
    pub fn new(slots: usize) -> Self {
        Self {
            slots: (0..slots)
                .map(|_| {
                    CachePadded::new(SlotState {
                        due: AtomicU64::new(EMPTY),
                        head: AtomicU64::new(0),
                        progress: AtomicU64::new(0),
                        claim: Mutex::new(Work::default()),
                    })
                })
                .collect(),
        }
    }

    /// Whether `slot` has a value to free under `cutoff`, or a due word to bring up to date: a
    /// load of the due word, then the list's end or the slot's running word when the word says
    /// to look. It takes no claim and changes nothing.
    pub fn has_work(&self, slot: u16, cutoff: Ts, lists: &Lists, writers: &dyn Writers) -> bool {
        match Due::decode(self.slots[slot as usize].due.load(Ordering::Acquire)) {
            Due::Commit(ts) => ts <= cutoff,
            Due::Empty(at) => lists.end(slot) != at,
            Due::Pending(start) => !writers.running(slot, start),
        }
    }

    /// The head's position and its progress, as a cut reads them under the closed gate.
    pub fn head(&self, slot: u16) -> (u64, u64) {
        let state = &self.slots[slot as usize];
        (
            state.head.load(Ordering::Acquire),
            state.progress.load(Ordering::Acquire),
        )
    }

    /// The due word of `slot`.
    pub fn due(&self, slot: u16) -> Due {
        Due::decode(self.slots[slot as usize].due.load(Ordering::Acquire))
    }

    /// A step of `slot`'s collection under `cutoff`, at or below every snapshot: frees `budget`
    /// pages at most, from the head, and stops at the first item whose writer's commit the cutoff
    /// does not cover. How many it freed; nothing, if another holds the slot's claim or a cut holds
    /// the gate. A step that reads a page in, or frees one that waits, returns the IO having
    /// changed nothing of it, its claim let go. Step it until done.
    #[allow(clippy::too_many_arguments)]
    pub fn step(
        &self,
        slot: u16,
        cutoff: Ts,
        budget: u64,
        lists: &Lists,
        pool: &Arc<BufferPool>,
        store: &Arc<PageStore>,
        writers: &dyn Writers,
    ) -> Result<IOResult<u64>> {
        let state = &self.slots[slot as usize];
        let Some(mut work) = state.claim.try_lock() else {
            return Ok(IOResult::Done(0));
        };
        let mut freed = 0;
        while freed < budget {
            // Relaxed: changed only under the claim this step holds.
            let head = state.head.load(Ordering::Relaxed);
            if head == lists.end(slot) {
                state
                    .due
                    .store(Due::Empty(head).encode(), Ordering::Release);
                work.due_for = None;
                break;
            }
            if work.reader.at() != head {
                work.reader = Reader::from(head);
            }
            let item = match lists.read(slot, &mut work.reader)? {
                IOResult::Done(Some(item)) => item,
                IOResult::Done(None) => {
                    return Err(LimboError::Corrupt(format!(
                        "slot {slot}'s list ends before its head {head}"
                    )))
                }
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            crate::race::point("gc.head_read");
            if work.due_for != Some(item.start) {
                state
                    .due
                    .store(Due::Pending(item.start).encode(), Ordering::Release);
                work.due_for = Some(item.start);
            }
            let commit = match Due::decode(state.due.load(Ordering::Relaxed)) {
                // An owed item's writer ended with the process before the open: every snapshot
                // sees it committed (DESIGN.md §6.5).
                _ if slot == lists.owed() => 0,
                Due::Commit(ts) => ts,
                Due::Pending(start) => {
                    if writers.running(slot, start) {
                        break;
                    }
                    // Read again after that load. A rollback cuts its items before its slot is
                    // let go, and the slot's next transaction appends at the same positions, so
                    // the word may name a writer that is gone: then it is stored anew.
                    if head == lists.end(slot) {
                        continue;
                    }
                    let now = match lists.read(slot, &mut Reader::from(head))? {
                        IOResult::Done(Some(now)) => now,
                        IOResult::Done(None) => continue,
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    };
                    if now.start != start {
                        continue;
                    }
                    // Still its item: it did not roll back, and its slot published its commit
                    // pair before its running word moved on.
                    let Some(ts) = writers.commit_of(slot, start) else {
                        return Err(LimboError::InternalError(format!(
                            "slot {slot}'s transaction started at {start} is no longer running \
                             and has no commit published, its items still in the list"
                        )));
                    };
                    state.due.store(Due::Commit(ts).encode(), Ordering::Release);
                    ts
                }
                Due::Empty(at) => {
                    return Err(LimboError::InternalError(format!(
                        "slot {slot}'s due word says freed up to {at} at its head {head}"
                    )))
                }
            };
            if commit > cutoff {
                break;
            }
            let progress = match state.progress.load(Ordering::Relaxed) {
                0 => lists.owed_start(slot, head),
                progress => progress,
            } as usize;
            let free = work
                .free
                .get_or_insert_with(|| RetiredFree::new(item.reference, progress));
            // One change: a cut finds the page freed and the progress that says so, or neither.
            let Some(change) = store.change() else {
                break;
            };
            let done = match free.step(pool, store)? {
                IOResult::Done(done) => done,
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            };
            crate::race::point("gc.page_freed");
            match done {
                true => {
                    state.progress.store(0, Ordering::Release);
                    state.head.store(head + 1, Ordering::Release);
                    work.free = None;
                    work.reader.advance();
                }
                false => state.progress.store(free.freed() as u64, Ordering::Release),
            }
            drop(change);
            freed += 1;
        }
        // Pages behind the head: given back now, or by a later step if one waits. Inside the gate,
        // so a cut's kept place, set with the gate closed, is seen by any give-back after it.
        let head = state.head.load(Ordering::Relaxed);
        if let Some(change) = store.change() {
            let _given = lists.give_back(slot, head)?;
            drop(change);
        }
        Ok(IOResult::Done(freed))
    }
}
