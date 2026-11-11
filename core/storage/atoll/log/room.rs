//! Room in the log, granted by compare-and-swap: how far records reach, and how far they may.
//!
//! An operation is granted its bytes before it appends any, if everything granted and appended
//! fits within the prepared segments, and the unflushed log within its buffer. What is granted
//! and appended is one word, the reach: a grant raises it, an append moves the end within it, so
//! a grant is one compare-and-swap of the word it checked. Padding a partial block moves every
//! record granted and not yet appended along with it, so it is granted like a record, within the
//! prepared segments, before the end moves: a grant and a padding racing are then two exchanges of
//! the reach, and whichever comes second sees the first.

use self::sync::{fence, lock, AtomicBool, AtomicU64, Mutex, Ordering};
use crate::io::Completion;
use crate::CompletionError;
use crossbeam_utils::CachePadded;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::storage::atoll::locks::{Mutex, MutexGuard};
    pub(super) use crate::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};

    pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock()
    }
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};
    pub(super) use loom::sync::{Mutex, MutexGuard};

    pub(super) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().unwrap()
    }
}

/// Those waiting to be admitted, sharing one registration (the auditor's #4356): every admitter is
/// woken to try again whenever room may have been made, so they wait on one completion, and a
/// wait called again, or made and dropped, adds nothing. An admitter registers and then, across a
/// fence, looks at the room again; whoever makes room makes it and then, across one, reads whether
/// anyone waits, taking the wait only if so: either the look finds the room, or the waker finds the
/// wait. A wake no one waits for is a fence and a load.
pub(super) struct Admitters<W = Completion> {
    wait: Mutex<Option<W>>,
    waited: AtomicBool,
}

impl<W> Default for Admitters<W> {
    fn default() -> Self {
        Self {
            wait: Mutex::new(None),
            waited: AtomicBool::new(false),
        }
    }
}

impl<W: Clone> Admitters<W> {
    /// The wait the next wake takes, `make`'s if none is registered, registered: look at the room
    /// again after it.
    fn register(&self, make: impl FnOnce() -> W) -> W {
        let w = lock(&self.wait).get_or_insert_with(make).clone();
        self.waited.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        w
    }

    /// Whether anyone waits to be admitted: a flush then pads what it holds for them.
    pub(super) fn waiting(&self) -> bool {
        self.waited.load(Ordering::SeqCst)
    }

    /// The shared wait, taken if anyone waits, after the caller made room: a fence, then the flag
    /// read and cleared.
    fn taken(&self) -> Option<W> {
        fence(Ordering::SeqCst);
        if !self.waited.load(Ordering::Relaxed) || !self.waited.swap(false, Ordering::AcqRel) {
            return None;
        }
        lock(&self.wait).take()
    }
}

impl Admitters {
    /// The wait the next wake finishes, registered: look at the room again after it.
    pub(super) fn wait(&self) -> Completion {
        self.register(crate::storage::atoll::notify::wait)
    }

    /// Wakes everyone waiting, to try again, after the caller made room.
    pub(super) fn wake(&self) {
        if let Some(c) = self.taken() {
            c.complete(0);
        }
    }

    /// Fails everyone waiting with `e`, once the failure is visible to admitters looking again.
    pub(super) fn fail(&self, e: CompletionError) {
        if let Some(c) = self.taken() {
            c.error(e);
        }
    }
}

pub(super) struct Room {
    /// Moved by every append, the reach then the end, so the two share a line of their own.
    ends: CachePadded<Ends>,
    /// Everything before it is durable. Raised by every flush, so on a line of its own.
    durable: CachePadded<AtomicU64>,
    /// Records may be written below this: every segment before it is prepared.
    prepared_end: AtomicU64,
    /// Bytes appended and not yet durable, past which nothing more is granted.
    buffer: u64,
}

struct Ends {
    /// Where the next record will start.
    insert: AtomicU64,
    /// Where the records granted reach once appended: the end, plus the bytes granted and not
    /// yet appended.
    reach: AtomicU64,
}

/// What padding a partial block came to.
pub(super) enum Pad {
    /// Padded to here.
    To(u64),
    /// Someone reserved bytes past the block's start meanwhile.
    Raced,
    /// Records already granted would be pushed past the prepared segments.
    Unprepared,
}

impl Room {
    pub(super) fn new(start: u64, prepared_end: u64, buffer: u64) -> Self {
        Self {
            ends: CachePadded::new(Ends {
                insert: AtomicU64::new(start),
                reach: AtomicU64::new(start),
            }),
            durable: CachePadded::new(AtomicU64::new(start)),
            prepared_end: AtomicU64::new(prepared_end),
            buffer,
        }
    }

    pub(super) fn end(&self) -> u64 {
        self.ends.insert.load(Ordering::Acquire)
    }

    /// [`end`](Self::end), in the total order of every reservation and of the log's input mark
    /// ([`Log::begin_input`](super::Log::begin_input)).
    pub(super) fn end_ordered(&self) -> u64 {
        self.ends.insert.load(Ordering::SeqCst)
    }

    pub(super) fn durable(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }

    pub(super) fn raise_durable(&self, to: u64) {
        self.durable.fetch_max(to, Ordering::SeqCst);
    }

    pub(super) fn prepared_end(&self) -> u64 {
        self.prepared_end.load(Ordering::SeqCst)
    }

    pub(super) fn set_prepared_end(&self, end: u64) {
        self.prepared_end.store(end, Ordering::SeqCst);
    }

    /// Where the records granted reach once appended.
    pub(super) fn reach(&self) -> u64 {
        self.ends.reach.load(Ordering::SeqCst)
    }

    /// Grants `bytes` if they fit within the prepared segments, and within the buffer if
    /// `buffer`: whether it did.
    pub(super) fn grant(&self, bytes: u64, buffer: bool) -> bool {
        // Read first: it is at or behind the end, and so the reach, read after it. Read stale,
        // like the prepared segments' end, it only rises, and only makes the check stricter.
        let durable = self.durable.load(Ordering::SeqCst);
        let mut reach = self.ends.reach.load(Ordering::SeqCst);
        loop {
            let fits = (!buffer || reach - durable + bytes <= self.buffer)
                && reach + bytes <= self.prepared_end.load(Ordering::SeqCst);
            if !fits {
                return false;
            }
            match self.ends.reach.compare_exchange_weak(
                reach,
                reach + bytes,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(now) => reach = now,
            }
        }
    }

    /// Where the prepared segments must end for `bytes` to fit after what is granted, if they end
    /// short of it now.
    pub(super) fn unprepared_for(&self, bytes: u64) -> Option<u64> {
        let wanted = self.reach() + bytes;
        (wanted > self.prepared_end()).then_some(wanted)
    }

    /// Gives back bytes granted and not appended.
    pub(super) fn give_back(&self, bytes: u64) {
        self.ends.reach.fetch_sub(bytes, Ordering::SeqCst);
    }

    /// Takes `len` of the bytes granted at the end; returns where they start. The reach stays.
    pub(super) fn reserve(&self, len: u64) -> u64 {
        self.ends.insert.fetch_add(len, Ordering::SeqCst)
    }

    /// Takes `len` of the bytes granted at the next block boundary past the end: returns where
    /// the end stood, and where the bytes start. The bytes between are the caller's to pad. The
    /// reach stays.
    pub(super) fn reserve_aligned(&self, len: u64) -> (u64, u64) {
        let mut at = self.ends.insert.load(Ordering::SeqCst);
        loop {
            let start = at.next_multiple_of(super::BLOCK);
            match self.ends.insert.compare_exchange_weak(
                at,
                start + len,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return (at, start),
                Err(now) => at = now,
            }
        }
    }

    /// Moves the end from `at` to the end of its block, for padding, unless someone reserved past
    /// `at` meanwhile or it would push the records granted past the prepared segments.
    pub(super) fn pad(&self, at: u64) -> Pad {
        let to = at.next_multiple_of(super::BLOCK);
        if !self.grant(to - at, false) {
            return Pad::Unprepared;
        }
        if self
            .ends
            .insert
            .compare_exchange(at, to, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            self.give_back(to - at);
            return Pad::Raced;
        }
        Pad::To(to)
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::super::BLOCK;
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// An operation is granted room as a partial block is padded, the two together too long for
    /// the prepared segments: one of them is turned away, and what was granted fits.
    #[test]
    fn padding_and_a_grant_never_overrun_the_prepared_segments() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let at = BLOCK - 64;
            // Room for the padding or the grant, not both.
            let room = Arc::new(Room::new(at, BLOCK + 32, u64::MAX));
            let granter = {
                let room = room.clone();
                thread::spawn(move || room.grant(96, true))
            };
            let padded = matches!(room.pad(at), Pad::To(_));
            let granted = granter.join().unwrap();
            assert!(
                !(padded && granted),
                "both the padding and the grant went in"
            );
            assert!(
                room.reach() <= room.prepared_end(),
                "the room granted overruns"
            );
        });
    }

    /// An admission that finds no room, as [`Log::admit`](super::super::Log::admit) does, while
    /// another operation gives room back, as dropping its admission does: either the admission's
    /// second look finds the room, or the give-back wakes it. Never neither.
    #[test]
    fn a_give_back_wakes_or_is_seen_by_an_admission_that_found_no_room() {
        use loom::sync::atomic::AtomicBool;
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            // A buffer of 64 bytes, which the giver's admission holds whole.
            let room = Arc::new(Room::new(0, u64::MAX, 64));
            assert!(room.grant(64, true));
            let waiting = Arc::new(Admitters::<Arc<AtomicBool>>::default());
            let giver = {
                let (room, waiting) = (room.clone(), waiting.clone());
                thread::spawn(move || {
                    room.give_back(64);
                    if let Some(w) = waiting.taken() {
                        w.store(true, Ordering::Release);
                    }
                })
            };
            let first = room.grant(64, true);
            let wait = (!first).then(|| waiting.register(|| Arc::new(AtomicBool::new(false))));
            let admitted = first || room.grant(64, true);
            giver.join().unwrap();
            if !admitted {
                let woken = wait.expect("a wait");
                assert!(woken.load(Ordering::Acquire), "a wait no give-back woke");
            }
        });
    }
}
