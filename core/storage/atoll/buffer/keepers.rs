//! Who keeps each frame held between calls by something other than the page table: an operation's
//! top keeping the page it last loaded, by a reference in the load's group
//! ([`super::shortage::Kept`]), and the checkpoint's memory demand keeping the frame handed to it
//! ([`super::upkeep::Upkeep`]). Built only for tests and the simulator, for the census of taken
//! frames and the park report ([`super::census`]), and read by nothing else.
//!
//! An entry is a slot of a fixed table, written as a sequence lock: the slot's word is odd while
//! the slot is written, so a reader that reads the word published and the same before and after
//! reading the fields read one entry whole. Every access is a standard library atomic, which
//! neither shuttle nor loom schedules, and none waits or takes a lock: an entry recorded or
//! cleared adds no step to any schedule and takes nothing the engine's own paths take.

use std::sync::atomic::{fence, AtomicBool, AtomicU64, AtomicUsize, Ordering};

/// Keepers the table names at once: an operation's top keeps one frame at most, and a statement
/// has a top for each of its cursors.
const SLOTS: usize = 512;

/// A slot's word: its state in the low two bits, raised by one as an entry is claimed and as it
/// is published, and by two as it is cleared, so the word never repeats.
const STATE: u64 = 3;
const FREE: u64 = 0;
const PUBLISHED: u64 = 2;

/// How often a reader reads a slot that changes under it before it passes over the slot.
const TRIES: usize = 4;

const OWNER: u64 = 1;
const CHECKPOINT: u64 = 2;

/// Who keeps a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Keeper {
    /// An operation's top: its connection (the `Arc<Conn>`'s address, 0 for an operation of
    /// none), its number ([`Serial`]), and its address during the call that kept the frame, which
    /// a top moved between its calls no longer has.
    Owner { conn: usize, serial: u64, at: usize },
    /// The checkpoint's memory demand.
    Checkpoint,
}

/// A frame kept, and by whom.
#[derive(Clone, Copy, Debug)]
pub(super) struct Entry {
    pub(super) frame: u32,
    pub(super) pid: u64,
    pub(super) keeper: Keeper,
    /// An owner's keep: whether it is idle, its owner returned IO and not called again
    /// (SHORT-HOLD1b).
    pub(super) idle: bool,
}

/// The number an operation's top is named by: one more for each top made.
pub(super) struct Serial(pub(super) u64);

impl Default for Serial {
    fn default() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

pub(super) struct Keepers {
    slots: Box<[Slot]>,
    /// The slot the next record looks at first.
    next: AtomicUsize,
    /// Records that found every slot taken: keepers the table does not name.
    missed: AtomicU64,
}

#[derive(Default)]
struct Slot {
    word: AtomicU64,
    frame: AtomicU64,
    pid: AtomicU64,
    kind: AtomicU64,
    conn: AtomicU64,
    serial: AtomicU64,
    at: AtomicU64,
    /// Whether the keep is idle: changed by its keeper alone, read with the other fields.
    idle: AtomicBool,
}

impl Keepers {
    pub(super) fn new() -> Self {
        Self {
            slots: (0..SLOTS).map(|_| Slot::default()).collect(),
            next: AtomicUsize::new(0),
            missed: AtomicU64::new(0),
        }
    }

    /// Records that `keeper` keeps `frame`, holding `pid`: the slot to clear it by, or `None`, and
    /// counted missed, if every slot is taken.
    pub(super) fn add(&self, frame: u32, pid: u64, keeper: Keeper) -> Option<usize> {
        let start = self.next.load(Ordering::Relaxed);
        for i in 0..self.slots.len() {
            let at = (start + i) % self.slots.len();
            let slot = &self.slots[at];
            let word = slot.word.load(Ordering::Relaxed);
            if word & STATE != FREE
                || slot
                    .word
                    .compare_exchange(word, word + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
            {
                continue;
            }
            // The word odd before any field changes, for a reader that reads a changed field.
            fence(Ordering::Release);
            let (kind, conn, serial, owner) = match keeper {
                Keeper::Owner { conn, serial, at } => (OWNER, conn, serial, at),
                Keeper::Checkpoint => (CHECKPOINT, 0, 0, 0),
            };
            slot.frame.store(frame as u64, Ordering::Relaxed);
            slot.pid.store(pid, Ordering::Relaxed);
            slot.kind.store(kind, Ordering::Relaxed);
            slot.conn.store(conn as u64, Ordering::Relaxed);
            slot.serial.store(serial, Ordering::Relaxed);
            slot.at.store(owner as u64, Ordering::Relaxed);
            slot.idle.store(false, Ordering::Relaxed);
            slot.word.store(word + 2, Ordering::Release);
            self.next.store(at + 1, Ordering::Relaxed);
            return Some(at);
        }
        self.missed.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Records that the keep at `slot` is idle, or active again, by the one that recorded it. One
    /// field changes alone, so a reader reads it whole with the entry's others.
    pub(super) fn set_idle(&self, slot: usize, idle: bool) {
        self.slots[slot].idle.store(idle, Ordering::Relaxed);
    }

    /// Clears the entry `slot` holds, by the one that recorded it.
    pub(super) fn remove(&self, slot: usize) {
        let word = &self.slots[slot].word;
        word.store(word.load(Ordering::Relaxed) + 2, Ordering::Release);
    }

    /// Every entry, as read now: a slot that changes on every read is passed over, its keeper
    /// coming or going.
    pub(super) fn entries(&self) -> Vec<Entry> {
        let mut out = Vec::new();
        for slot in self.slots.iter() {
            for _ in 0..TRIES {
                let before = slot.word.load(Ordering::Acquire);
                if before & STATE != PUBLISHED {
                    break;
                }
                let keeper = match slot.kind.load(Ordering::Relaxed) {
                    OWNER => Some(Keeper::Owner {
                        conn: slot.conn.load(Ordering::Relaxed) as usize,
                        serial: slot.serial.load(Ordering::Relaxed),
                        at: slot.at.load(Ordering::Relaxed) as usize,
                    }),
                    CHECKPOINT => Some(Keeper::Checkpoint),
                    _ => None,
                };
                let (frame, pid, idle) = (
                    slot.frame.load(Ordering::Relaxed) as u32,
                    slot.pid.load(Ordering::Relaxed),
                    slot.idle.load(Ordering::Relaxed),
                );
                // The fields read before the word is read again: one changed by a writer after its
                // claim shows the word changed.
                fence(Ordering::Acquire);
                if slot.word.load(Ordering::Relaxed) != before {
                    continue;
                }
                out.extend(keeper.map(|keeper| Entry {
                    frame,
                    pid,
                    keeper,
                    idle,
                }));
                break;
            }
        }
        out
    }

    /// Records that found no free slot.
    pub(super) fn missed(&self) -> u64 {
        self.missed.load(Ordering::Relaxed)
    }
}
