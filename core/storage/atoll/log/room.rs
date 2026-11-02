//! Room in the log, granted by compare-and-swap: how far records reach, and how far they may.
//!
//! An operation is granted its bytes before it appends any, if everything granted and appended
//! fits within the prepared segments, and the unflushed log within its buffer. What is granted
//! and appended is one word, the reach: a grant raises it, an append moves the end within it, so
//! a grant is one compare-and-swap of the word it checked. Padding a partial block moves every
//! record granted and not yet appended along with it, so it is granted like a record, within the
//! prepared segments, before the end moves: a grant and a padding racing are then two exchanges of
//! the reach, and whichever comes second sees the first.

use self::sync::{AtomicU64, Ordering};

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicU64, Ordering};
}

pub(super) struct Room {
    /// Where the next record will start.
    insert: AtomicU64,
    /// Where the records granted reach once appended: the end, plus the bytes granted and not
    /// yet appended.
    reach: AtomicU64,
    /// Everything before it is durable.
    durable: AtomicU64,
    /// Records may be written below this: every segment before it is prepared.
    prepared_end: AtomicU64,
    /// Bytes appended and not yet durable, past which nothing more is granted.
    buffer: u64,
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
            insert: AtomicU64::new(start),
            reach: AtomicU64::new(start),
            durable: AtomicU64::new(start),
            prepared_end: AtomicU64::new(prepared_end),
            buffer,
        }
    }

    pub(super) fn end(&self) -> u64 {
        self.insert.load(Ordering::Acquire)
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
        self.reach.load(Ordering::SeqCst)
    }

    /// Grants `bytes` if they fit within the prepared segments, and within the buffer if
    /// `buffer`: whether it did.
    pub(super) fn grant(&self, bytes: u64, buffer: bool) -> bool {
        // Read first: it is at or behind the end, and so the reach, read after it. Read stale,
        // like the prepared segments' end, it only rises, and only makes the check stricter.
        let durable = self.durable.load(Ordering::SeqCst);
        let mut reach = self.reach.load(Ordering::SeqCst);
        loop {
            let fits = (!buffer || reach - durable + bytes <= self.buffer)
                && reach + bytes <= self.prepared_end.load(Ordering::SeqCst);
            if !fits {
                return false;
            }
            match self.reach.compare_exchange_weak(
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

    /// Gives back bytes granted and not appended.
    pub(super) fn give_back(&self, bytes: u64) {
        self.reach.fetch_sub(bytes, Ordering::SeqCst);
    }

    /// Takes `len` of the bytes granted at the end; returns where they start. The reach stays.
    pub(super) fn reserve(&self, len: u64) -> u64 {
        self.insert.fetch_add(len, Ordering::SeqCst)
    }

    /// Moves the end from `at` to the end of its block, for padding, unless someone reserved past
    /// `at` meanwhile or it would push the records granted past the prepared segments.
    pub(super) fn pad(&self, at: u64) -> Pad {
        let to = at.next_multiple_of(super::BLOCK);
        if !self.grant(to - at, false) {
            return Pad::Unprepared;
        }
        if self
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
}
