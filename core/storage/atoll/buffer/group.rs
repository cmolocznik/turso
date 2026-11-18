//! A load's group (ASYNC1 D2e §2.2b): the parties that keep the page a frame is being read into,
//! from the load's start until each of them has fixed it or let it go, so a waiter that comes
//! back after the load finds the page it waited for still in memory.
//!
//! Each party holds one reference: the load's guard while the load is under way, the loader's
//! own when its fix runs for an operation's owner, and one for each fix that joins the load. The
//! group as a whole holds one keep on the frame, the record word's group bit
//! ([`super::frame::FrameHeader::group_kept`]), so no count ever moves between words.
//!
//! The word, one per 4 KiB grain at a frame's first grain, holds five fields (SHORT-HOLD1b):
//! whether the load is under way (1 bit); the references (12 bits); how many of them are idle
//! keeps, an operation's whose owner returned IO and has not been called again (12); how many
//! are sturdy keeps, which no drain revokes (12); and the generation (27), raised only by a
//! revocation. Idle and sturdy are never above the references, which every change asserts. A
//! group closed with no references left is the only value a join is refused at, apart from the
//! limit and the last generation. The loader opens the word from no references under its
//! exclusive latch, before it publishes the load; it closes it under the same latch, before it
//! publishes the page or frees the frame, in one subtraction that also drops the guard. Each
//! other reference is released in one change, in either phase. The one party whose change takes
//! the word to no references lets the group's keep go: a release, the close, or a drain's
//! revocation.
//!
//! A reference's holder records the generation it was added at, and each of its changes (idle,
//! active again, released) is a compare-and-swap with that generation in it. A drain revokes a
//! closed group whose references are all idle ordinary keeps, in one swap to the next generation
//! with none left; a holder's change that then finds another generation holds nothing and
//! releases nothing. Without a revocation the generation never moves: a reference held keeps its
//! group from reaching none, so no group closes and opens again under it. A frame whose
//! generation reached its last value takes no keep again.
//!
//! A join lands only on the group of the load its probe found: it is made in the step its probe
//! was made in, and a frame over that grain is read into again only once every step that could
//! still see the old one has ended (`BufferPool::quiet_at`), while the frame's previous group had
//! already reached no references before its keep was let go and the frame freed.

use super::super::epoch::Step;
use super::super::latch::Marked;
use super::frame::FrameHeader;
use super::memory::Zeroable;

#[cfg(not(atoll_loom))]
use crate::sync::atomic::{fence, AtomicU64, Ordering};
#[cfg(atoll_loom)]
use loom::sync::atomic::{fence, AtomicU64, Ordering};

/// The group is open: its load is under way, and its guard holds a reference.
const OPEN: u64 = 1;
/// The bits of each of the three counts.
const COUNT_BITS: u32 = 12;
/// The most each count holds: references, idle ones, sturdy ones.
pub(super) const MOST: u64 = (1 << COUNT_BITS) - 1;
/// One reference.
const REFERENCE: u64 = 1 << 1;
/// One idle keep.
const IDLE: u64 = REFERENCE << COUNT_BITS;
/// One sturdy keep.
const STURDY: u64 = IDLE << COUNT_BITS;
/// One generation.
const GENERATION: u64 = STURDY << COUNT_BITS;
/// Every field but the generation.
const COUNTS: u64 = GENERATION - 1;
/// The last generation, at which a frame takes no keep: 27 bits.
pub(super) const LAST_GENERATION: u32 = (u64::MAX / GENERATION) as u32;

/// What a keep is (SHORT-HOLD1b, F1): an operation's first for its page, which a drain may revoke
/// once it is idle; or the one it takes for the page whose keep a drain revoked, which nothing
/// revokes, so the operation runs with its page within two loads of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Keep {
    Ordinary,
    Sturdy,
}

impl Keep {
    /// What the keep adds to the sturdy count.
    fn sturdy(self) -> u64 {
        match self {
            Keep::Ordinary => 0,
            Keep::Sturdy => STURDY,
        }
    }
}

/// What a holder's release of its reference did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Released {
    /// It was the group's last: its releaser lets the group's keep go.
    Last,
    /// Others are left, the word as the release left it.
    Left(u64),
    /// A drain revoked it: it held nothing, and released nothing.
    Revoked,
}

/// A frame's group word.
#[derive(Debug, Default)]
pub(super) struct GroupWord(AtomicU64);

// SAFETY: an atomic, valid as zero: a group closed with no references, at its first generation.
unsafe impl Zeroable for GroupWord {}

impl GroupWord {
    /// Opens the group of a load starting: the guard's reference, and the loader's own, of the
    /// kind `loader` says, if it keeps the page: the generation it keeps it at, or none where the
    /// loader keeps nothing or the frame's generation is at its last. By the loader, under its
    /// exclusive latch, before it publishes the load. The frame's previous group, if any, reached
    /// no references before its keep was let go and the frame freed, and a holder of a revoked
    /// reference changes nothing, so the word holds a generation alone.
    #[inline]
    pub(super) fn open(&self, loader: Option<Keep>) -> Option<u32> {
        let word = self.0.load(Ordering::Acquire);
        if word & COUNTS != 0 {
            opened_over_references(word);
        }
        let keeps = loader.filter(|_| generation(word) < LAST_GENERATION);
        let to = word + OPEN + REFERENCE + keeps.map_or(0, |k| REFERENCE + k.sturdy());
        if let Err(seen) = self
            .0
            .compare_exchange(word, to, Ordering::AcqRel, Ordering::Acquire)
        {
            changed_under_loader(word, seen);
        }
        keeps.map(|_| generation(word))
    }

    /// Adds a reference of kind `keep` for a fix that found the load under way, in the step
    /// `_step` its probe was made in: the generation it was added at. Refused once the group's
    /// last reference is gone, at the references' limit (SHORT-HOLD1b, A2), and at the last
    /// generation. A lost compare-and-swap is another party's join, release, close or revocation.
    pub(super) fn join(&self, _step: &Step<'_>, keep: Keep) -> Option<u32> {
        self.add_reference(keep)
    }

    /// [`join`](Self::join)'s change.
    fn add_reference(&self, keep: Keep) -> Option<u32> {
        let mut word = self.0.load(Ordering::Acquire);
        loop {
            let refs = references(word);
            if refs == 0 || refs == MOST || generation(word) == LAST_GENERATION {
                return None;
            }
            crate::race::point("group.join_seen");
            let now = word + REFERENCE + keep.sturdy();
            checked(now, "a join");
            match self
                .0
                .compare_exchange(word, now, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(generation(word)),
                Err(seen) => word = seen,
            }
        }
    }

    /// Closes the group and drops its guard, in one change: whether the guard was its last
    /// reference, the closer then letting the group's keep go. By the loader, under its exclusive
    /// latch, before it publishes the page or frees the frame.
    #[inline]
    pub(super) fn close(&self) -> bool {
        let was = self.0.fetch_sub(OPEN + REFERENCE, Ordering::AcqRel);
        if was & OPEN == 0 || references(was) == 0 {
            closed_unopened(was);
        }
        let now = was - (OPEN + REFERENCE);
        checked(now, "a close");
        now & COUNTS == 0
    }

    /// The keep added at generation `at` goes idle, at its owner's IO return, in one addition:
    /// the word as it left it. An active keep is never revoked, so the generation is still `at`,
    /// and its own reference is not idle, so the idle count stays at or below the references: the
    /// one count the change raises, which it asserts; the references and the sturdy count are
    /// as they were.
    #[inline]
    pub(super) fn idle(&self, at: u32) -> u64 {
        let was = self.0.fetch_add(IDLE, Ordering::AcqRel);
        if generation(was) != at {
            revoked_while_active(at, was);
        }
        let now = was + IDLE;
        if idle(now) > references(now) {
            counts_broken("a keep going idle", now);
        }
        now
    }

    /// The idle keep of kind `keep` added at generation `at` goes active, at its owner's next
    /// call, its kind kept (a sturdy keep stays sturdy to its release, SHORT-HOLD1b, the
    /// auditor's #6341): whether it is still held, false if a drain revoked it. The first of
    /// this swap and the drain's revocation wins. Its idle count only falls, so the word it
    /// leaves keeps idle and sturdy at or below the references whenever the word it read did,
    /// which the change that wrote that word asserted.
    #[inline]
    pub(super) fn wake(&self, at: u32, keep: Keep) -> bool {
        let less = IDLE;
        let mut word = self.0.load(Ordering::Acquire);
        loop {
            if generation(word) != at {
                return false;
            }
            crate::race::point("group.wake_seen");
            if idle(word) == 0 || (keep == Keep::Sturdy && sturdy(word) == 0) {
                woke_uncounted(word);
            }
            let now = word - less;
            match self
                .0
                .compare_exchange(word, now, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(seen) => word = seen,
            }
        }
    }

    /// Releases a reference other than the guard, of kind `keep`, added at generation `at` and
    /// idle if `idle`, in either phase, holding no latch. An active keep is never revoked, so its
    /// release is one subtraction; an idle one's is a compare-and-swap with its generation in it,
    /// and the generation decides a race with a revocation: one that finds another releases
    /// nothing.
    #[inline]
    pub(super) fn release(&self, at: u32, idle_now: bool, keep: Keep) -> Released {
        if !idle_now {
            let less = REFERENCE + keep.sturdy();
            let was = self.0.fetch_sub(less, Ordering::AcqRel);
            if generation(was) != at
                || references(was) <= was & OPEN
                || (keep == Keep::Sturdy && sturdy(was) == 0)
            {
                released_unheld("an active", was);
            }
            let now = was - less;
            checked(now, "a release");
            return if now & COUNTS == 0 {
                Released::Last
            } else {
                Released::Left(now)
            };
        }
        let mut word = self.0.load(Ordering::Acquire);
        loop {
            if generation(word) != at {
                return Released::Revoked;
            }
            crate::race::point("group.release_seen");
            if references(word) <= word & OPEN
                || idle(word) == 0
                || (keep == Keep::Sturdy && sturdy(word) == 0)
            {
                released_unheld("an idle", word);
            }
            let now = word - REFERENCE - IDLE - keep.sturdy();
            checked(now, "a release");
            match self
                .0
                .compare_exchange(word, now, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) if now & COUNTS == 0 => return Released::Last,
                Ok(_) => return Released::Left(now),
                Err(seen) => word = seen,
            }
        }
    }

    /// Revokes every reference of the group, from `seen`, the word a drain read revocable
    /// ([`revocable`]), to the next generation with none: whether it did, the drain then letting
    /// the group's keep go. A lost swap is an owner's change or a join, which the drain reads
    /// again.
    pub(super) fn revoke(&self, seen: u64) -> bool {
        assert!(
            revocable(seen),
            "a group revoked that was not revocable: {seen:#x}"
        );
        assert!(
            generation(seen) < LAST_GENERATION,
            "a keep at the last generation: {seen:#x}"
        );
        crate::race::point("group.revoke_seen");
        let next = (seen & !COUNTS) + GENERATION;
        self.0
            .compare_exchange(seen, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// The word as read now.
    pub(super) fn word(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    /// Whether no reference is left, for a check made where the word cannot change: under a hold
    /// of the frame with its group's keep gone, or just before the keep is let go.
    #[cfg(test)]
    pub(super) fn released(&self) -> bool {
        released(self.0.load(Ordering::Acquire))
    }

    /// The references the group holds now, the guard's included, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn references(&self) -> u64 {
        references(self.0.load(Ordering::Acquire))
    }

    /// Sets the word to `word`, for a test that starts a frame at a generation or a count.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn preset(&self, word: u64) {
        self.0.store(word, Ordering::Release);
    }
}

/// The keeper's half of the pair across a frame's group word and its record (SHORT-HOLD1b): a keep's
/// change left the group as `word`; whether a claim that passed the frame over must be told. If the
/// group is revocable now it fences, then reads the record's mark, so either the drain's reread
/// ([`pass_over`]) reads this change or this reads the drain's mark.
pub(super) fn to_tell(word: u64, header: &FrameHeader) -> bool {
    if !revocable(word) {
        return false;
    }
    fence(Ordering::SeqCst);
    header.record_waited()
}

/// What a drain's pass over a kept frame it could not revoke came to ([`pass_over`]).
pub(super) enum Passed {
    /// The frame changed under it: its keeps are revocable or gone now, or its record no longer
    /// keeps it. The claim looks again.
    Changed,
    /// Still kept as the group word reads now: the claim waits, its mark on the record telling
    /// whoever lets the frame go or leaves it revocable.
    Waits(u64),
}

/// The drain's half of the pair with [`to_tell`]: it marks the frame's record waited, fences, then
/// reads the group word again.
pub(super) fn pass_over(header: &FrameHeader, group: &GroupWord) -> Passed {
    if let Marked::Changed = header.mark_record_waited() {
        return Passed::Changed;
    }
    fence(Ordering::SeqCst);
    let word = group.word();
    if revocable(word) || released(word) {
        Passed::Changed
    } else {
        Passed::Waits(word)
    }
}

/// Asserts what every change of the word keeps: idle and sturdy keeps are references.
#[inline(always)]
fn checked(word: u64, change: &'static str) {
    if idle(word) > references(word) || sturdy(word) > references(word) {
        counts_broken(change, word);
    }
}

// The failures of the changes' checks, out of line and cold, so a change's own instructions
// carry its test and branch alone.

/// A load's open found references left in the word.
#[cold]
#[inline(never)]
fn opened_over_references(word: u64) -> ! {
    panic!("a load's group opened over a group with references left: {word:#x}")
}

/// The word changed under its loader between its read and its open.
#[cold]
#[inline(never)]
fn changed_under_loader(word: u64, seen: u64) -> ! {
    panic!("a load's group changed under its loader as it opened: {word:#x}, then {seen:#x}")
}

/// A close found the group not open, or with no reference for its guard.
#[cold]
#[inline(never)]
fn closed_unopened(word: u64) -> ! {
    panic!("a load's group closed that was not open: {word:#x}")
}

/// A change left the word with more idle or sturdy keeps than references.
#[cold]
#[inline(never)]
fn counts_broken(change: &'static str, word: u64) -> ! {
    panic!("{change} left more idle or sturdy keeps than references: {word:#x}")
}

/// An active keep's change found another generation: a drain revoked a keep that was not idle.
#[cold]
#[inline(never)]
fn revoked_while_active(at: u32, word: u64) -> ! {
    panic!("an active keep of generation {at} was revoked: {word:#x}")
}

/// A keep woke that the word does not count idle, or sturdy for a sturdy one.
#[cold]
#[inline(never)]
fn woke_uncounted(word: u64) -> ! {
    panic!("a keep woke that its load's group did not count idle or sturdy: {word:#x}")
}

/// A release of `kind` (active or idle) found no reference of its own in the word.
#[cold]
#[inline(never)]
fn released_unheld(kind: &'static str, word: u64) -> ! {
    panic!("{kind} reference released that its load's group did not hold: {word:#x}")
}

/// The word of a group of these fields, for a test.
#[cfg(all(test, not(any(shuttle, atoll_loom))))]
pub(super) fn word_of(open: bool, references: u64, idle: u64, sturdy: u64, generation: u32) -> u64 {
    open as u64
        + references * REFERENCE
        + idle * IDLE
        + sturdy * STURDY
        + generation as u64 * GENERATION
}

/// Whether a group word says its load is under way.
pub(super) fn open(word: u64) -> bool {
    word & OPEN != 0
}

/// The references a group word counts, the guard's included.
pub(super) fn references(word: u64) -> u64 {
    (word / REFERENCE) & MOST
}

/// The idle keeps a group word counts.
pub(super) fn idle(word: u64) -> u64 {
    (word / IDLE) & MOST
}

/// The sturdy keeps a group word counts.
pub(super) fn sturdy(word: u64) -> u64 {
    (word / STURDY) & MOST
}

/// A group word's generation.
pub(super) fn generation(word: u64) -> u32 {
    (word / GENERATION) as u32
}

/// Whether a group word holds no reference.
pub(super) fn released(word: u64) -> bool {
    word & COUNTS == 0
}

/// Whether a drain may revoke the group (SHORT-HOLD1b): closed, every reference an idle keep and
/// none sturdy.
pub(super) fn revocable(word: u64) -> bool {
    !open(word) && references(word) > 0 && references(word) == idle(word) && sturdy(word) == 0
}

/// Whether the group is held by idle keeps alone, one of them sturdy: what no drain revokes and
/// no step under way lets go, only an owner's next call.
pub(super) fn sturdy_idle(word: u64) -> bool {
    !open(word) && references(word) > 0 && references(word) == idle(word) && sturdy(word) > 0
}

/// A group word's fields, by name, for the census of taken frames and the park report.
#[cfg(any(test, feature = "simulator"))]
pub(super) fn describe(word: u64) -> String {
    format!(
        "{}, references {}, idle {}, sturdy {}, generation {}",
        if open(word) { "open" } else { "closed" },
        references(word),
        idle(word),
        sturdy(word),
        generation(word)
    )
}

/// A load's group and its frame's record word under loom, on the primitives themselves: the close,
/// the references' releases, a late join, the record's retirement and the frame's freeing racing,
/// in every interleaving and C11 reordering. Run with `RUSTFLAGS="--cfg atoll_loom" cargo test -p
/// turso_core --lib --release group::loom`.
#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::super::frame::FrameHeader;
    use super::super::page::SizeClass;
    use super::*;
    use loom::sync::atomic::AtomicUsize;
    use loom::sync::Arc;
    use loom::thread;

    fn check(f: impl Fn() + Sync + Send + 'static) {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(f);
    }

    /// A frame being loaded: its group's keep set and its group opened, as the loader does under
    /// its latch, with the loader's own reference if `loader_keeps`.
    fn loading(loader_keeps: bool) -> (Arc<FrameHeader>, Arc<GroupWord>) {
        let h = Arc::new(FrameHeader::default());
        let g = Arc::new(GroupWord::default());
        h.group_kept();
        g.open(loader_keeps.then_some(Keep::Ordinary));
        (h, g)
    }

    /// A reference released, by a party that holds it: the frame still kept until then; the
    /// group's keep let go if it was the last, counted in `ungroups`, and the frame's freeing
    /// counted in `freed` if that let it go.
    fn release(h: &FrameHeader, g: &GroupWord, ungroups: &AtomicUsize, freed: &AtomicUsize) {
        assert!(
            h.record_keeps(),
            "a reference held on a frame no group keeps"
        );
        if g.release(0, false, Keep::Ordinary) == Released::Last {
            ungroups.fetch_add(1, Ordering::Relaxed);
            if h.ungroup().freed {
                freed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// The loader's close, and two references' releases, in every order: the group's keep goes
    /// exactly once, by whichever took the word to zero, never while a reference is held, and
    /// nothing is left of the group or the frame's record (mutants `close-keeps-guard`,
    /// `close-clears-early`, `last-release-silent`).
    #[test]
    fn a_groups_keep_goes_once_whatever_its_close_and_releases_race() {
        check(|| {
            let (h, g) = loading(true);
            assert!(
                g.add_reference(Keep::Ordinary).is_some(),
                "a joiner admitted to an open group"
            );
            let (ungroups, freed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
            let closer = {
                let (h, g, ungroups) = (h.clone(), g.clone(), ungroups.clone());
                thread::spawn(move || {
                    if g.close() {
                        ungroups.fetch_add(1, Ordering::Relaxed);
                        assert!(!h.ungroup().freed, "a loading frame freed at its close");
                    }
                })
            };
            let owners: Vec<_> = (0..2)
                .map(|_| {
                    let (h, g, ungroups, freed) =
                        (h.clone(), g.clone(), ungroups.clone(), freed.clone());
                    thread::spawn(move || release(&h, &g, &ungroups, &freed))
                })
                .collect();
            closer.join().unwrap();
            for owner in owners {
                owner.join().unwrap();
            }
            assert_eq!(
                ungroups.load(Ordering::Relaxed),
                1,
                "the group's keep went other than once"
            );
            assert!(
                !h.record_keeps(),
                "the frame still kept with its group gone"
            );
            assert_eq!(g.0.load(Ordering::Relaxed), 0, "the group word at the end");
        });
    }

    /// A join racing the loader's close and the last other reference's release: admitted while a
    /// reference is left, holding the frame kept until it releases, which is then the group's last;
    /// or refused at zero, holding nothing. Never admitted once the word reached zero (mutant
    /// `join-at-zero`).
    #[test]
    fn a_late_join_is_admitted_only_while_a_reference_is_left() {
        check(|| {
            let (h, g) = loading(true);
            let (ungroups, freed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
            let closer = {
                let (h, g, ungroups) = (h.clone(), g.clone(), ungroups.clone());
                thread::spawn(move || {
                    if g.close() {
                        ungroups.fetch_add(1, Ordering::Relaxed);
                        h.ungroup();
                    }
                })
            };
            let loader = {
                let (h, g, ungroups, freed) =
                    (h.clone(), g.clone(), ungroups.clone(), freed.clone());
                thread::spawn(move || release(&h, &g, &ungroups, &freed))
            };
            let joiner = {
                let (h, g, ungroups, freed) =
                    (h.clone(), g.clone(), ungroups.clone(), freed.clone());
                thread::spawn(move || {
                    if g.add_reference(Keep::Ordinary).is_some() {
                        release(&h, &g, &ungroups, &freed);
                    }
                })
            };
            closer.join().unwrap();
            loader.join().unwrap();
            joiner.join().unwrap();
            assert_eq!(
                ungroups.load(Ordering::Relaxed),
                1,
                "the group's keep went other than once"
            );
            assert_eq!(g.0.load(Ordering::Relaxed), 0, "the group word at the end");
        });
    }

    /// The common frame's final free, apart from the group's hand-off: a request's retirement,
    /// the group's release, the freeing and a claim's mark racing. The frame goes to the free
    /// frames exactly once, by whichever let it go last, never while the record holds a request
    /// or the group keeps it (mutant `free-ignores-group`).
    #[test]
    fn a_frame_kept_by_its_record_and_its_group_goes_free_once() {
        check(|| {
            let h = Arc::new(FrameHeader::default());
            h.group_kept();
            let _held = h.hold_record(0, SizeClass::new(0).unwrap());
            let freed = Arc::new(AtomicUsize::new(0));
            let free = {
                let (h, freed) = (h.clone(), freed.clone());
                thread::spawn(move || {
                    if !h.free_deferred() {
                        assert!(!h.record_keeps(), "the freeing caller freed a kept frame");
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
            let ungroup = {
                let (h, freed) = (h.clone(), freed.clone());
                thread::spawn(move || {
                    if h.ungroup().freed {
                        freed.fetch_add(1, Ordering::Relaxed);
                    }
                })
            };
            let _ = h.mark_record_waited();
            free.join().unwrap();
            retire.join().unwrap();
            ungroup.join().unwrap();
            assert_eq!(
                freed.load(Ordering::Relaxed),
                1,
                "the frame went free other than once"
            );
            assert!(!h.record_keeps());
        });
    }

    /// A failed load with a joiner: the loader's unload closes the group and frees the frame while
    /// the joiner's and the loader's own references and the load's request still hold it. The
    /// frame goes free exactly once, after the last of them, whatever their order.
    #[test]
    fn a_failed_loads_frame_goes_free_once_after_its_references_and_request() {
        check(|| {
            let (h, g) = loading(true);
            assert!(
                g.add_reference(Keep::Ordinary).is_some(),
                "a joiner admitted to an open group"
            );
            let _held = h.hold_record(0, SizeClass::new(0).unwrap());
            let (ungroups, freed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
            let unload = {
                let (h, g, ungroups, freed) =
                    (h.clone(), g.clone(), ungroups.clone(), freed.clone());
                thread::spawn(move || {
                    if g.close() {
                        ungroups.fetch_add(1, Ordering::Relaxed);
                        h.ungroup();
                    }
                    if !h.free_deferred() {
                        assert!(!h.record_keeps(), "the unload freed a kept frame");
                        freed.fetch_add(1, Ordering::Relaxed);
                    }
                    // The loader's own reference, let go at its next miss.
                    release(&h, &g, &ungroups, &freed);
                })
            };
            let joiner = {
                let (h, g, ungroups, freed) =
                    (h.clone(), g.clone(), ungroups.clone(), freed.clone());
                thread::spawn(move || release(&h, &g, &ungroups, &freed))
            };
            if h.retire_record().freed {
                freed.fetch_add(1, Ordering::Relaxed);
            }
            unload.join().unwrap();
            joiner.join().unwrap();
            assert_eq!(
                ungroups.load(Ordering::Relaxed),
                1,
                "the group's keep went other than once"
            );
            assert_eq!(
                freed.load(Ordering::Relaxed),
                1,
                "the frame went free other than once"
            );
        });
    }

    /// The revocable keep (SHORT-HOLD1b) on the word: owner A keeps the page its load opened and
    /// owner B joins the load; each goes idle at its IO return and wakes at its next call. The
    /// drain revokes a group it reads closed with every reference idle and none sturdy, once for
    /// each of the two frames. A that finds its keep revoked releases nothing and reads its page
    /// into the second frame, keeping it sturdy, idle at its return, and runs with it at its next
    /// call. In every interleaving: no keep is revoked while active (`idle`'s own assertion) or
    /// sturdy, so A loses its page at most once before it runs with it (F1's bound); a revoked
    /// keep's release changes nothing; each group's keep goes exactly once, by its last releaser
    /// or its revoker; and both words end with no reference (mutants `revoke-active`,
    /// `sturdy-revocable`, `revoke-keeps-generation`).
    #[test]
    fn a_keep_is_revoked_only_idle_and_ordinary_and_an_operation_loses_its_page_at_most_once() {
        check(|| {
            let (h, g) = loading(true);
            let at_b = g
                .add_reference(Keep::Ordinary)
                .expect("B joins the open group");
            assert!(!g.close(), "the keeps are left");
            let (h2, g2) = (
                Arc::new(FrameHeader::default()),
                Arc::new(GroupWord::default()),
            );
            let ungroups = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
            let revoked = Arc::new(AtomicUsize::new(0));
            let drain = {
                let frames = [(h.clone(), g.clone()), (h2.clone(), g2.clone())];
                let (ungroups, revoked) = (ungroups.clone(), revoked.clone());
                thread::spawn(move || {
                    for (i, (h, g)) in frames.iter().enumerate() {
                        let word = g.word();
                        if revocable(word) && g.revoke(word) {
                            assert!(h.record_keeps(), "a revoked group kept no frame");
                            assert!(!h.ungroup().freed, "a resident frame freed");
                            ungroups[i].fetch_add(1, Ordering::Relaxed);
                            revoked.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            };
            let a = {
                let (h, g, h2, g2, ungroups) = (
                    h.clone(),
                    g.clone(),
                    h2.clone(),
                    g2.clone(),
                    ungroups.clone(),
                );
                thread::spawn(move || {
                    g.idle(0);
                    if g.wake(0, Keep::Ordinary) {
                        release_active(&h, &g, 0, Keep::Ordinary, &ungroups[0]);
                        return 0;
                    }
                    assert_eq!(
                        g.release(0, true, Keep::Ordinary),
                        Released::Revoked,
                        "a revoked keep released a reference"
                    );
                    // The reload, into the second frame, sturdy.
                    h2.group_kept();
                    let at = g2.open(Some(Keep::Sturdy)).expect("a keep");
                    assert!(!g2.close(), "A's keep is left");
                    g2.idle(at);
                    assert!(g2.wake(at, Keep::Sturdy), "a sturdy keep was revoked");
                    release_active(&h2, &g2, at, Keep::Sturdy, &ungroups[1]);
                    1
                })
            };
            let b = {
                let (h, g, ungroups) = (h.clone(), g.clone(), ungroups.clone());
                thread::spawn(move || {
                    g.idle(at_b);
                    if g.wake(at_b, Keep::Ordinary) {
                        release_active(&h, &g, at_b, Keep::Ordinary, &ungroups[0]);
                    } else {
                        assert_eq!(
                            g.release(at_b, true, Keep::Ordinary),
                            Released::Revoked,
                            "a revoked keep released a reference"
                        );
                    }
                })
            };
            drain.join().unwrap();
            let a_revoked = a.join().unwrap();
            b.join().unwrap();
            assert!(a_revoked <= 1, "A lost its page more than once");
            assert_eq!(
                ungroups[0].load(Ordering::Relaxed),
                1,
                "the first group's keep went other than once"
            );
            assert_eq!(
                ungroups[1].load(Ordering::Relaxed),
                a_revoked,
                "the reload's group's keep went other than once"
            );
            assert!(
                released(g.word()) && released(g2.word()),
                "a reference left"
            );
            assert!(
                !h.record_keeps() && !h2.record_keeps(),
                "a frame still kept"
            );
            assert_eq!(
                generation(g.word()) as usize,
                revoked.load(Ordering::Relaxed),
                "the first group's generation against its revocations"
            );
        });
    }

    /// The pair across a frame's group word and its record (SHORT-HOLD1b), on the code itself
    /// ([`to_tell`], [`pass_over`]): a drain looks at a frame whose one keep is active, cannot
    /// revoke it, and passes it over, marking the record, fencing and reading the group word again;
    /// the keeper's last change, its IO return, leaves the group revocable, fences and reads the
    /// record's mark, waking the claim if it is set, and the keeper never acts again. In every
    /// interleaving and C11 reordering the drain's claim is served, its first look or its reread
    /// reading the frame revocable and revoking it, or woken: never parked with no waker (mutants
    /// `signal-keeper-fence-gone`, `signal-drain-fence-gone`, `signal-fences-weakened`).
    #[test]
    fn a_drain_that_passed_a_kept_frame_over_is_served_or_woken_whatever_its_keepers_last_change() {
        check(|| {
            let (h, g) = loading(true);
            assert!(!g.close(), "the keeper's keep is left, active");
            let woken = Arc::new(loom::sync::atomic::AtomicBool::new(false));
            let keeper = {
                let (h, g, woken) = (h.clone(), g.clone(), woken.clone());
                thread::spawn(move || {
                    let word = g.idle(0);
                    if to_tell(word, &h) {
                        woken.store(true, Ordering::Release);
                    }
                })
            };
            let drain = {
                let (h, g) = (h.clone(), g.clone());
                thread::spawn(move || {
                    let word = g.word();
                    if revocable(word) {
                        return g.revoke(word);
                    }
                    match pass_over(&h, &g) {
                        // It looks again: the keep is idle now, and nothing else changes the word.
                        Passed::Changed => {
                            let word = g.word();
                            assert!(revocable(word), "the claim looked again at no change");
                            g.revoke(word)
                        }
                        Passed::Waits(_) => false,
                    }
                })
            };
            keeper.join().unwrap();
            let served = drain.join().unwrap();
            assert!(
                served || woken.load(Ordering::Acquire),
                "the drain's claim parked with no waker"
            );
        });
    }

    /// The release of an active keep, of a party that holds it: the group's keep let go and counted
    /// in `n` if it was the last.
    fn release_active(h: &FrameHeader, g: &GroupWord, at: u32, keep: Keep, n: &AtomicUsize) {
        match g.release(at, false, keep) {
            Released::Last => {
                h.ungroup();
                n.fetch_add(1, Ordering::Relaxed);
            }
            Released::Left(_) => {}
            Released::Revoked => panic!("an active keep was revoked"),
        }
    }

    /// A frame freed by its group's last release, then claimed for another load: the new load's
    /// open from zero always succeeds, nothing of the old group left, in every order of the
    /// release's two changes against the claim.
    #[test]
    fn a_frame_freed_by_its_groups_last_release_opens_a_new_group_from_zero() {
        check(|| {
            let (h, g) = loading(true);
            assert!(!g.close(), "the loader's reference is left");
            let free = Arc::new(loom::sync::atomic::AtomicBool::new(false));
            let releaser = {
                let (h, g, free) = (h.clone(), g.clone(), free.clone());
                thread::spawn(move || {
                    assert_eq!(
                        g.release(0, false, Keep::Ordinary),
                        Released::Last,
                        "the last reference"
                    );
                    let found = h.ungroup();
                    // Not freed: here the free frames are the flag below.
                    assert!(!found.freed);
                    free.store(true, Ordering::Release);
                })
            };
            while !free.load(Ordering::Acquire) {
                thread::yield_now();
            }
            h.group_kept();
            g.open(None);
            assert!(g.close(), "the new load's guard was its only reference");
            releaser.join().unwrap();
        });
    }
}
