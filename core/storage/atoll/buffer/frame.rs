//! Frame headers: one per 4 KiB of the pool, in an array that is never unmapped, so a frame
//! hint, however stale, always reads a header.

use super::super::latch::{Marked, VersionedLatch};
use super::super::page::SizeClass;
use super::memory::Zeroable;
use super::watt::Tracker;
use crate::sync::atomic::{AtomicU64, AtomicU8, Ordering};

/// The record's word: the engine's atomic, or loom's, which explores the reorderings of the
/// record's freeing, retirement and marks (a `Zeroed` is built from defaults under loom).
#[cfg(not(atoll_loom))]
type RecordWord = AtomicU8;
#[cfg(atoll_loom)]
type RecordWord = loom::sync::atomic::AtomicU8;

/// The frame holds no page.
pub const FREE: u8 = 0;
/// A page is being read into the frame, which the load holds exclusively.
pub const LOADING: u8 = 1;
/// The frame holds a page.
pub const RESIDENT: u8 = 2;
/// The page is being written out of the frame, which the write-out holds exclusively until the
/// write ends.
pub const OUTGOING: u8 = 3;
/// The frame is lent beside the page table ([`super::lent`]): it holds no page, and its holder
/// gives it back.
pub const LENT: u8 = 4;

/// A copy of the page is being written back.
pub const WRITING: u8 = 1;
/// Evict the page once its writeback lands, unless it changed meanwhile.
pub const EVICT_AFTER: u8 = 2;
/// The page was unlinked from what referred to it and is about to be freed: hints to it are not
/// followed.
pub const ABANDONED: u8 = 4;

/// `pid` of a frame holding no page; no page id has every bit set.
pub const NO_PAGE: u64 = u64::MAX;

/// The frame's record holds a request: its load's or its write-out's, from before the request is
/// handed over until the backend retires it, which may come after its callback ran and the frame
/// is resident and unlatched again (DESIGN.md 22.18, ASYNC1 v3.8, a frame).
const RECORD_HELD: u8 = 1;
/// The frame was freed while its record held a request or its load's group kept it: it goes to the
/// free frames once neither does, never before, as the backend may still read or write its memory
/// and a waiter of its load may still come back for its page.
const RECORD_FREED: u8 = 2;
/// A claim passed over the frame for its record or its group: whichever lets the frame go last
/// tells the pool's supply.
const RECORD_WAITED: u8 = 4;
/// The frame's load group keeps it (ASYNC1 D2e §2.2b, [`super::group`]): set by the loader under
/// its exclusive latch before it publishes the load, cleared by whoever takes the group's word to
/// zero.
const RECORD_GROUP: u8 = 8;

#[derive(Debug, Default)]
pub struct FrameHeader {
    /// The page's latch. Its version also rises on eviction, so a reader holding a hint learns
    /// that the frame changed hands.
    pub latch: VersionedLatch,
    /// The page held; meaningful only with `status` RESIDENT, LOADING or OUTGOING.
    pub pid: AtomicU64,
    /// The page's change count, mirrored in its header: one more at each change.
    pub changes: AtomicU64,
    /// The count of the newest copy written, or loaded: the page is dirty while `changes` is past
    /// it, and may be dropped once it is not.
    pub written: AtomicU64,
    /// The count its checkpoint copy holds: the page is owed an image while `changes` differs
    /// from it (audit 18, review 1).
    pub imaged: AtomicU64,
    /// The checkpoint whose cut found the page owed and has not imaged it yet: its first change
    /// meanwhile keeps its bytes for that checkpoint.
    pub pending: AtomicU64,
    /// This frame's link in its hash chain: the next frame, a mark and a tag ([`super::table`]).
    pub next: super::table::Link,
    pub status: AtomicU8,
    pub flags: AtomicU8,
    /// The frame's request record, beside its kind: [`RECORD_HELD`] and the marks with it. One
    /// request at a time, the frame's latch held exclusively when one is made.
    record: RecordWord,
    pub tracker: Tracker,
    /// The global epoch when a step last fixed the page, the highest any did: a page marked at
    /// or above the epochs' horizon may be held by a step under way. Relaxed: it only prices a
    /// drain, and memory is read into again only once the epochs pass `given_up`'s stamps.
    pub held: AtomicU64,
    /// Where the IO of the hold that reads the frame in or writes it out stands at the backend:
    /// one past the hold's version, shifted, once its request is handed over, and that with its
    /// low bit once it landed. Only rises, as `fetch_max`: a hold's states rise, and every later
    /// hold's are above them, so a mark written late, after the frame changed hands, is lost. A
    /// fix that finds the frame loading or outgoing waits on that IO, stepping it, only while it
    /// is at the backend, and otherwise on the pool's supply, which the submission's and the
    /// landing's events finish (DESIGN.md 9.1.3, wait kinds).
    pub io: AtomicU64,
}

// SAFETY: every field is atomics, valid as zeros: an unlocked latch at version 0, a FREE frame.
unsafe impl Zeroable for FrameHeader {}

impl FrameHeader {
    /// The IO of the hold taken at `version` was handed to the backend.
    pub fn submitted_at(&self, version: u64) {
        self.io.fetch_max(io_mark(version), Ordering::AcqRel);
    }

    /// The IO of the hold taken at `version` landed: none of it is at the backend any more.
    pub fn landed_at(&self, version: u64) {
        self.io.fetch_max(io_mark(version) | 1, Ordering::AcqRel);
    }

    /// Whether the IO of the hold taken at `version` was handed to the backend, landed or not.
    pub fn handed_over(&self, version: u64) -> bool {
        self.io.load(Ordering::Acquire) >= io_mark(version)
    }

    /// Whether the IO of the hold taken at `version` is at the backend: handed over and not landed.
    pub fn at_backend(&self, version: u64) -> bool {
        self.io.load(Ordering::Acquire) == io_mark(version)
    }

    pub fn pid(&self) -> u64 {
        self.pid.load(Ordering::Acquire)
    }

    pub fn status(&self) -> u8 {
        self.status.load(Ordering::Acquire)
    }

    pub fn dirty(&self) -> bool {
        self.changes.load(Ordering::Acquire) > self.written.load(Ordering::Acquire)
    }

    /// Whether the page differs from its checkpoint copy.
    pub fn owed(&self) -> bool {
        self.changes.load(Ordering::Acquire) != self.imaged.load(Ordering::Acquire)
    }

    pub fn has(&self, flag: u8) -> bool {
        self.flags.load(Ordering::Acquire) & flag != 0
    }

    /// The frame's record takes the request about to be handed over: by the holder of the frame's
    /// exclusive latch, before the request is made, so the request is never at the backend with
    /// the record free. `frame` is this header's frame, of `class`. The one maker of a
    /// [`HeldRecord`], which its caller hands to exactly one of the three that let it go: the
    /// queueing of a frame request, the pool's keeping of the record until a request handed over
    /// retires, or the pool's give-back for one never handed over (D2b §1).
    pub fn hold_record(&self, frame: u32, class: SizeClass) -> HeldRecord {
        self.take_record();
        HeldRecord { frame, class }
    }

    fn take_record(&self) {
        // The load's group may keep the frame already, and a claim may have marked it for that.
        let was = self.record.fetch_or(RECORD_HELD, Ordering::AcqRel);
        assert!(
            was & (RECORD_HELD | RECORD_FREED) == 0,
            "a frame's record held twice: {was:#x}"
        );
    }

    /// Whether the frame's record holds a request.
    #[cfg(test)]
    pub fn record_held(&self) -> bool {
        self.record.load(Ordering::Acquire) & RECORD_HELD != 0
    }

    /// Whether the frame's record holds a request or its load's group keeps it: either way an
    /// eviction passes it over.
    pub fn record_keeps(&self) -> bool {
        self.record.load(Ordering::Acquire) & (RECORD_HELD | RECORD_GROUP) != 0
    }

    /// The load's group keeps the frame from here ([`super::group`]): by the loader, under its
    /// exclusive latch, before it publishes the load. A frame claimed for a load holds no request
    /// and no group, and no claim marks a frame that holds neither.
    pub fn group_kept(&self) {
        let was = self.record.swap(RECORD_GROUP, Ordering::AcqRel);
        assert!(
            was == 0,
            "a frame claimed for a load was held or kept: {was:#x}"
        );
    }

    /// Marks the record waited, for a claim that passed over the frame for its record or its
    /// group, after the claim's registration: [`Marked::Waits`] while either still keeps the
    /// frame, so whichever lets it go last tells the pool's supply, else [`Marked::Changed`].
    /// Always a Release write when it reads Waits, even with the mark already there, so this
    /// claim's registration comes before the release that reads the mark. A lost
    /// compare-and-swap is another party's change (the retirement, the group's release, the
    /// freeing or another claim's mark), and the word is looked at again.
    pub fn mark_record_waited(&self) -> Marked {
        let mut was = self.record.load(Ordering::Acquire);
        loop {
            if was & (RECORD_HELD | RECORD_GROUP) == 0 {
                return Marked::Changed;
            }
            match self.record.compare_exchange(
                was,
                was | RECORD_WAITED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Marked::Waits,
                Err(now) => was = now,
            }
        }
    }

    /// Defers the freed frame while its record holds a request or its load's group keeps it:
    /// whether either does, whichever then lets the frame go last handing it to the free frames.
    /// Otherwise the caller does. Called once, by the frame's one freeing caller, which owns the
    /// frame until it or that release hands it to the free frames, so no request starts and no
    /// group opens on it meanwhile: the word changes only by the retirement, the group's release
    /// and claims' marks, and a lost compare-and-swap looks at it again. One load alone where
    /// nothing keeps the frame.
    pub fn free_deferred(&self) -> bool {
        let mut was = self.record.load(Ordering::Acquire);
        loop {
            if was & (RECORD_HELD | RECORD_GROUP) == 0 {
                return false;
            }
            crate::turso_assert!(was & RECORD_FREED == 0, "a frame freed twice");
            match self.record.compare_exchange(
                was,
                was | RECORD_FREED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(now) => was = now,
            }
        }
    }

    /// The record's request retired, or was never handed over: the record is free, and what was
    /// marked on it while it held the request, unless the load's group still keeps the frame,
    /// whose release then lets it go.
    pub fn retire_record(&self) -> Retired {
        self.let_go(
            RECORD_HELD,
            RECORD_GROUP,
            "a frame's record retired with no request",
        )
    }

    /// The load's group let its keep go, its last reference released ([`super::group`]): what was
    /// marked on the frame while the group kept it, unless the record still holds a request, whose
    /// retirement then lets it go.
    pub fn ungroup(&self) -> Retired {
        self.let_go(
            RECORD_GROUP,
            RECORD_HELD,
            "a frame let go by a load's group that did not keep it",
        )
    }

    /// Clears `mine`, which the word holds: with `other` still keeping the frame, only that, and
    /// nothing found; otherwise the whole word, and whether the frame was freed meanwhile and a
    /// claim passed over it. A lost compare-and-swap is another party's change, looked at again.
    fn let_go(&self, mine: u8, other: u8, missing: &'static str) -> Retired {
        let mut was = self.record.load(Ordering::Acquire);
        loop {
            assert!(was & mine != 0, "{missing}: {was:#x}");
            let (now, found) = if was & other != 0 {
                (
                    was & !mine,
                    Retired {
                        freed: false,
                        waited: false,
                    },
                )
            } else {
                (
                    0,
                    Retired {
                        freed: was & RECORD_FREED != 0,
                        waited: was & RECORD_WAITED != 0,
                    },
                )
            };
            match self
                .record
                .compare_exchange(was, now, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return found,
                Err(w) => was = w,
            }
        }
    }
}

/// A frame's record, held for the request about to be handed over (D2b §1): made only by
/// [`FrameHeader::hold_record`], so every one names a record taken, and let go by value exactly
/// once: queued (`Counted::queue_frame`, whose release then lets the record go), kept until the
/// request handed over in place retires, or given back for a request never handed over, a refused
/// queueing's included.
#[must_use = "a held record is queued, kept until its request retires, or given back"]
pub(in crate::storage::atoll) struct HeldRecord {
    frame: u32,
    class: SizeClass,
}

impl HeldRecord {
    pub(in crate::storage::atoll) fn frame(&self) -> u32 {
        self.frame
    }

    pub(in crate::storage::atoll) fn class(&self) -> SizeClass {
        self.class
    }

    /// Let go into its frame request's dispatch record, whose release lets the frame's record go.
    pub(in crate::storage::atoll) fn queued(self) {}
}

/// What a frame's record found as the last of its request and its load's group let the frame go.
pub struct Retired {
    /// The frame was freed meanwhile: it goes to the free frames now.
    pub freed: bool,
    /// A claim passed over the frame for the record: it is told.
    pub waited: bool,
}

/// The IO state of the hold taken at `version`, handed over: a latch's version has 62 bits.
fn io_mark(version: u64) -> u64 {
    (version + 1) << 1
}

/// A frame header's bytes in the engine's own build: what a page of the header table is counted by
/// ([`super::CUT_RUN`]). Shuttle's and loom's atomics are larger than the hardware's, and so are
/// their headers; counting by those would cut the table in runs the engine never makes, a schedule
/// test then stepping a cut many times the units it has.
pub const HEADER_BYTES: usize = 128;

#[cfg(not(any(shuttle, atoll_loom)))]
const _: () = assert!(std::mem::size_of::<FrameHeader>() == HEADER_BYTES);

/// A frame's record under loom: its freeing, its retirement and a claim's mark racing, in every
/// interleaving and C11 reordering. Run with `RUSTFLAGS="--cfg atoll_loom" cargo test -p
/// turso_core --lib --release frame::loom`.
#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::atomic::AtomicBool;
    use loom::sync::Arc;
    use loom::thread;

    fn check(f: impl Fn() + Sync + Send + 'static) {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(f);
    }

    /// The frame freed while its request is held, the request retiring and a claim marking the
    /// record, at once: the frame goes to the free frames exactly once, by whichever of the
    /// freeing and the retirement came second, and the record ends free.
    #[test]
    fn a_freed_frame_goes_free_once_whatever_its_retirement_and_a_mark_race() {
        check(|| {
            let h = Arc::new(FrameHeader::default());
            h.take_record();
            let freed = Arc::new(loom::sync::atomic::AtomicUsize::new(0));
            let free = {
                let (h, freed) = (h.clone(), freed.clone());
                thread::spawn(move || {
                    if !h.free_deferred() {
                        assert!(!h.record_held(), "the freeing caller freed a held frame");
                        freed.fetch_add(1, Ordering::Relaxed);
                    }
                })
            };
            let retire = {
                let (h, freed) = (h.clone(), freed.clone());
                thread::spawn(move || {
                    if h.retire_record().freed {
                        freed.fetch_add(1, Ordering::Relaxed);
                    }
                })
            };
            let mark = {
                let h = h.clone();
                thread::spawn(move || {
                    let _ = h.mark_record_waited();
                })
            };
            free.join().unwrap();
            retire.join().unwrap();
            mark.join().unwrap();
            assert_eq!(
                freed.load(Ordering::Relaxed),
                1,
                "the frame went free other than once"
            );
            assert_eq!(h.record.load(Ordering::Relaxed), 0);
        });
    }

    /// A second claim marking a record another already marked still writes, Release, so a
    /// retirement that reads the mark sees what the claim registered before it.
    #[test]
    fn a_claim_marking_a_record_marked_already_is_seen_by_its_retirement() {
        check(|| {
            let h = Arc::new(FrameHeader::default());
            h.take_record();
            assert_eq!(h.mark_record_waited(), Marked::Waits);
            let registered = Arc::new(AtomicBool::new(false));
            let claim = {
                let (h, registered) = (h.clone(), registered.clone());
                thread::spawn(move || {
                    registered.store(true, Ordering::Relaxed);
                    h.mark_record_waited()
                })
            };
            let retire = {
                let (h, registered) = (h.clone(), registered.clone());
                thread::spawn(move || {
                    let retired = h.retire_record();
                    assert!(retired.waited);
                    registered.load(Ordering::Relaxed)
                })
            };
            let marked = claim.join().unwrap();
            let seen = retire.join().unwrap();
            assert!(
                marked != Marked::Waits || seen,
                "a claim waits on a retirement that did not see it register"
            );
        });
    }
}
