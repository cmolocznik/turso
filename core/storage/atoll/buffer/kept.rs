//! The room for the copies a checkpoint's cut keeps (DESIGN.md §9.6; Clay's ruling of
//! 2026-11-05): a page owed an image is copied as it stood at the cut before its first change
//! since, and the copies are held until the checkpoint images them.
//!
//! The copies are charged when made and released only when their bytes are dropped, wherever
//! they then are ([`KeptCopy`]). A change asks for room for every page it will change before
//! its first edit ([`BufferPool::keep`](super::BufferPool::keep)); refused, it lets go of what
//! it holds and waits, driving the checkpoint itself. Only node pages are kept, at most 64 KiB
//! each ([`MAX_CLASS`](super::super::btree::node::MAX_CLASS)), and a change asks for three at
//! most, so every request fits the smallest limit, one page of the largest class.
//!
//! A refused change's wait is a ticket its operation owns across its retries ([`RoomWait`],
//! DESIGN.md 22.4), with the bytes it last asked for and a demand on the checkpoint that frees
//! room; the room presses for the largest request among the tickets alive.

use super::super::aligned::AlignedBlock;
use super::super::executor;
use super::super::page::MIN_PAGE_SIZE;
use crate::io::{Completion, IO};
use crate::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::sync::Arc;
use crate::MonotonicInstant;
use std::ops::Deref;

/// The upper bound of the default limit: past a 2 GiB frame budget, the limit stays here (the
/// ruling's initial choice, not a measured optimum).
pub const KEPT_CAP: usize = 256 << 20;

/// The most one change asks for: only node pages are kept, at most
/// [`MAX_CLASS`](super::super::btree::node::MAX_CLASS), and a merge asks for three.
pub const LARGEST_REQUEST: usize =
    3 * (super::super::page::MIN_PAGE_SIZE << super::super::btree::node::MAX_CLASS);

/// The default limit for a budget of `budget` bytes: an eighth of it, at most [`KEPT_CAP`], and
/// never under [`LARGEST_REQUEST`], so every change's request fits. The copies take frames of the
/// budget, so the floor is no larger than a request (the ruling's floor of one page of the
/// largest class predates the copies living in the frames, and was most of a small pool).
pub fn default_limit(budget: usize) -> usize {
    (budget / 8).clamp(LARGEST_REQUEST, KEPT_CAP)
}

const _: () = assert!(
    LARGEST_REQUEST <= KEPT_CAP,
    "a change's request fits under the cap"
);

/// The sizes a room ticket can name, in pages of the smallest size: a request is a sum of node
/// pages, each a whole number of the smallest, and at most [`LARGEST_REQUEST`].
const SIZES: usize = LARGEST_REQUEST / MIN_PAGE_SIZE + 1;

/// What the room has held and how often it refused, since the pool opened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KeptStats {
    pub limit: usize,
    pub used: usize,
    pub peak: usize,
    pub refusals: u64,
    /// Nanoseconds from each operation's first refusal to its change admitted.
    pub waited_ns: u64,
    /// The longest of those waits since the peaks were last reset.
    pub longest_wait_ns: u64,
}

pub struct KeptRoom {
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    refusals: AtomicU64,
    waited_ns: AtomicU64,
    longest_wait_ns: AtomicU64,
    /// The room tickets alive, counted by the bytes each last asked for, in pages of the
    /// smallest size: the largest is the request the room presses for.
    tickets: [AtomicUsize; SIZES],
    /// How many room tickets are alive: the checkpoint images kept copies first while any is.
    waiting: AtomicUsize,
    /// The pool's supply: room given back, a cut ended and a ticket dropped are changes of it
    /// (DESIGN.md 9.1.3).
    supply: Arc<super::supply::Supply>,
    /// Waits for room handed out, each of the remote kind, for tests.
    #[cfg(test)]
    blocking_waits: AtomicU64,
    io: Arc<dyn IO>,
}

/// A refused change's wait for room (DESIGN.md 22.4), owned by its operation across its retries:
/// empty until the room refuses it, then the ticket of its refusals until its change is
/// admitted. Dropped with the operation (cancelled, the connection gone), it lets the ticket go.
#[derive(Default)]
pub struct RoomWait(Option<RoomTicket>);

impl RoomWait {
    /// Whether the room refused the operation and has not admitted its change since.
    #[cfg(test)]
    pub fn waiting(&self) -> bool {
        self.0.is_some()
    }
}

/// A live room ticket: the bytes its operation last asked for, counted among the room's tickets,
/// when its first refusal was, and its demand on the checkpoint, of the relief class, while the
/// checkpoint is a task.
struct RoomTicket {
    room: Arc<KeptRoom>,
    bytes: usize,
    since: MonotonicInstant,
    _demand: Option<executor::Ticket>,
}

impl Drop for RoomTicket {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        // A smaller largest request may let another waiter in, so the drop is a change of the
        // supply (DESIGN.md 9.1.3, rule 5), the count lowered inside it.
        let room = &self.room;
        room.supply.change(|event| {
            room.tickets[KeptRoom::size(self.bytes)].fetch_sub(1, Ordering::AcqRel);
            room.waiting.fetch_sub(1, Ordering::AcqRel);
            event.arm();
        });
    }
}

impl KeptRoom {
    pub fn new(limit: usize, supply: Arc<super::supply::Supply>, io: Arc<dyn IO>) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            refusals: AtomicU64::new(0),
            waited_ns: AtomicU64::new(0),
            longest_wait_ns: AtomicU64::new(0),
            tickets: std::array::from_fn(|_| AtomicUsize::new(0)),
            waiting: AtomicUsize::new(0),
            supply,
            #[cfg(test)]
            blocking_waits: AtomicU64::new(0),
            io,
        })
    }

    /// Charges `bytes`, if they fit: whether they did.
    pub fn charge(&self, bytes: usize) -> bool {
        debug_assert!(
            bytes <= self.limit,
            "a request of {bytes} bytes no room holds"
        );
        // One strong compare-and-swap, tried once (DESIGN.md 9.1.2): lost, the change is turned
        // down as if the room were full, and `wait_for_room`, registered on the room before it
        // looks again, starts it again at once if room was given back meanwhile. A loss to another
        // charge is that change's success; a loss to a release is what that look sees.
        let used = self.used.load(Ordering::Acquire);
        crate::race::point("kept.charge");
        if used + bytes > self.limit
            || self
                .used
                .compare_exchange(used, used + bytes, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return false;
        }
        self.peak.fetch_max(used + bytes, Ordering::AcqRel);
        true
    }

    /// A ticket's size: its bytes in pages of the smallest size.
    fn size(bytes: usize) -> usize {
        debug_assert!(
            bytes > 0 && bytes <= LARGEST_REQUEST && bytes % MIN_PAGE_SIZE == 0,
            "a request of {bytes} bytes no ticket names"
        );
        bytes / MIN_PAGE_SIZE
    }

    /// A request of `bytes` refused, its operation's wait `wait`: a ticket from a refusal before
    /// names these bytes now, counted under them before it leaves its old ones, so the largest
    /// request read meanwhile is never below either; else a ticket is taken, its wait timed from
    /// now and its demand on the checkpoint made (`demand`). A request of no bytes, its cut ended
    /// since the charge, waits for nothing.
    pub fn refused(
        self: &Arc<Self>,
        bytes: usize,
        wait: &mut RoomWait,
        demand: impl FnOnce() -> Option<executor::Ticket>,
    ) {
        self.refusals.fetch_add(1, Ordering::Relaxed);
        if bytes == 0 {
            return;
        }
        let size = Self::size(bytes);
        match &mut wait.0 {
            Some(ticket) if ticket.bytes == bytes => {}
            Some(ticket) => {
                self.tickets[size].fetch_add(1, Ordering::AcqRel);
                let was = Self::size(ticket.bytes);
                self.supply.change(|event| {
                    self.tickets[was].fetch_sub(1, Ordering::AcqRel);
                    event.arm();
                });
                ticket.bytes = bytes;
            }
            None => {
                self.tickets[size].fetch_add(1, Ordering::AcqRel);
                self.waiting.fetch_add(1, Ordering::AcqRel);
                wait.0 = Some(RoomTicket {
                    room: self.clone(),
                    bytes,
                    since: self.io.current_time_monotonic(),
                    _demand: demand(),
                });
            }
        }
    }

    /// The change of the operation waiting on `wait` admitted: its wait, if it waited, ends, and
    /// its ticket goes.
    pub fn admitted(&self, wait: &mut RoomWait) {
        let Some(ticket) = wait.0.take() else {
            return;
        };
        let waited = self
            .io
            .current_time_monotonic()
            .duration_since(ticket.since);
        let waited = u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX);
        self.waited_ns.fetch_add(waited, Ordering::Relaxed);
        self.longest_wait_ns.fetch_max(waited, Ordering::Relaxed);
    }

    /// The largest request among the room tickets alive, 0 with none.
    pub fn pressing(&self) -> usize {
        (1..SIZES)
            .rev()
            .find(|&size| self.tickets[size].load(Ordering::Acquire) != 0)
            .map_or(0, |size| size * MIN_PAGE_SIZE)
    }

    /// Whether the largest request among the room tickets alive fits now.
    pub fn takes_refused(&self) -> bool {
        self.used.load(Ordering::Acquire) + self.pressing() <= self.limit
    }

    /// Whether a room ticket is alive.
    pub fn pressed(&self) -> bool {
        self.waiting.load(Ordering::Acquire) != 0
    }

    /// The cut ended: no copy is kept, and every waiter looks again. Its tickets live until their
    /// operations' changes are admitted or the operations go.
    pub fn cut_ended(&self) {
        self.supply.change(|event| event.arm());
    }

    /// Gives back `bytes` charged for copies that were not made after all.
    pub fn uncharge(&self, bytes: usize) {
        self.release(bytes);
    }

    /// Room given back (an uncharge undoes an attempt, and is a change all the same: DESIGN.md
    /// 9.1.3, rule 5).
    fn release(&self, bytes: usize) {
        self.supply.change(|event| {
            let before = self.used.fetch_sub(bytes, Ordering::AcqRel);
            debug_assert!(before >= bytes, "a kept copy released twice");
            event.arm();
        });
    }

    /// A wait for room handed out, of the remote kind: its producers are other callers' acts.
    pub fn waiting(&self, ticket: super::supply::Ticket) -> Completion {
        #[cfg(test)]
        self.blocking_waits.fetch_add(1, Ordering::Relaxed);
        ticket.remote()
    }

    /// Waits handed out so far that block until another thread gives room back.
    #[cfg(test)]
    pub fn blocking_waits(&self) -> u64 {
        self.blocking_waits.load(Ordering::Relaxed)
    }

    /// Whoever waits for room looks again: a stepper of the checkpoint leaving, its state let go.
    pub fn wake(&self) {
        self.supply.change(|event| event.arm());
    }

    #[cfg(test)]
    pub fn stats(&self) -> KeptStats {
        KeptStats {
            limit: self.limit,
            used: self.used.load(Ordering::Acquire),
            peak: self.peak.load(Ordering::Acquire),
            refusals: self.refusals.load(Ordering::Relaxed),
            waited_ns: self.waited_ns.load(Ordering::Relaxed),
            longest_wait_ns: self.longest_wait_ns.load(Ordering::Relaxed),
        }
    }

    /// Starts the peak again from what is held now, and the longest wait from none, for a
    /// measurement.
    #[cfg(test)]
    pub fn reset_peaks(&self) {
        self.peak
            .store(self.used.load(Ordering::Acquire), Ordering::Release);
        self.longest_wait_ns.store(0, Ordering::Relaxed);
    }
}

/// A page as a cut found it, in a frame lent for it, charged to the room until the copy drops
/// and gives the frame back.
pub struct KeptCopy {
    block: std::mem::ManuallyDrop<AlignedBlock>,
    room: Arc<KeptRoom>,
}

impl KeptCopy {
    /// `block`'s copy, already charged to `room`.
    pub fn charged(block: AlignedBlock, room: &Arc<KeptRoom>) -> Self {
        Self {
            block: std::mem::ManuallyDrop::new(block),
            room: room.clone(),
        }
    }
}

impl Deref for KeptCopy {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.block.bytes()
    }
}

impl Drop for KeptCopy {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        let len = self.block.len();
        // The frame first: its return is published and signals its class before the room's bytes
        // wake a writer (DESIGN.md 9.1.1), so a writer the bytes wake finds the frame.
        // SAFETY: dropped once, here, and never touched after.
        unsafe { std::mem::ManuallyDrop::drop(&mut self.block) };
        self.room.release(len);
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests;
