//! The versioned latch (CONCURRENCY.md §5): one 64-bit word, an exclusive bit, a waited bit and
//! 62 bits of version.
//!
//! There is one kind of hold, exclusive, for changing a page or what a frame holds. Readers take
//! nothing: they remember the version, read, and validate that it did not change. Releasing an
//! exclusive hold bumps the version, unless nothing was written under it.
//!
//! A hold exists only as a [`Held`] guard, from the instruction that took it, and every release
//! is the guard's: a thread that unwinds with one alive ends the process before letting it go, so
//! no reader ever sees a page half changed under a latch freed by a panic.
//!
//! A taker that finds it held marks it waited and waits on its owner's supply for the release
//! (DESIGN.md 9.1.3): under Turso's cooperative scheduling the holder may be another connection
//! on the very same thread, so the wait is a completion, never a spin.

use self::sync::{fence, AtomicU64, Ordering};
use std::mem::ManuallyDrop;
use std::ops::Deref;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicU64, Ordering};
}

const EXCLUSIVE: u64 = 1;
/// Set while the latch is held by a claim of the pool that found the latch's page in its way and
/// waits for its release (DESIGN.md 9.1.2); every release clears it and tells the latch's owner.
const WAITED: u64 = 2;
const VERSION_SHIFT: u32 = 2;

#[derive(Debug, Default)]
pub struct VersionedLatch(AtomicU64);

fn version(word: u64) -> u64 {
    word >> VERSION_SHIFT
}

/// The word of the latch free at `version`.
fn free_at(version: u64) -> u64 {
    version << VERSION_SHIFT
}

/// What reaches a latch, and how a hold on it that a waiter marked is let go.
pub trait LatchRef: Deref<Target = VersionedLatch> {
    /// Lets go a hold a waiter marked: `unlock` frees the word. A frame's latch runs it inside a
    /// change of its pool's supply that it arms, so the event is open before the unlock and the
    /// waiter's ticket is finished after it (DESIGN.md 9.1.3); any other latch is never marked,
    /// and just unlocks.
    fn release_marked<F: FnOnce()>(&self, unlock: F) {
        unlock();
    }
}

impl LatchRef for &VersionedLatch {}

impl LatchRef for crate::sync::Arc<VersionedLatch> {}

/// What marking a held latch waited found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Marked {
    /// Marked, the hold as it was: its release will tell the latch's owner.
    Waits,
    /// Free, or held again, or changed: another party's act since it was read.
    Changed,
}

fn held(word: u64) -> bool {
    word & EXCLUSIVE != 0
}

/// What a look at a latch found: free at a version, or held.
#[derive(Clone, Copy, Debug)]
pub enum Look {
    Free(u64),
    Held(HeldWord),
}

/// A held latch's word as a look read it: what a waiter marks, and the hold's version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeldWord(u64);

impl HeldWord {
    /// The hold's version: the one it was taken at.
    pub fn version(self) -> u64 {
        version(self.0)
    }
}

/// Between taking the latch and the first write under it. A reader that reads one of those writes
/// and then validates, across its acquire fence, finds the latch taken. The exchange that took it
/// is an acquire, which keeps later accesses after its read but lets a later store be seen before
/// its own: without this, a reader could read a write under way and still find the version it
/// began with.
fn before_writes() {
    fence(Ordering::Release);
}

impl VersionedLatch {
    /// The version to validate against later, or `None` while a writer holds the latch.
    pub fn optimistic(&self) -> Option<u64> {
        let word = self.0.load(Ordering::Acquire);
        (!held(word)).then_some(version(word))
    }

    /// Marks the latch, read held, waited (DESIGN.md 9.1.2): a compare-and-swap with release order
    /// from the held word, with or without `WAITED` in it, so the mark is a write after whatever the
    /// caller did before it in the word's order, and the release after it sees that. A mark lost to
    /// another waiter's mark, the hold the same, is made once more; lost again, or to a release or a
    /// new hold, the latch changed by another party's act.
    pub fn mark_waited(&self) -> Marked {
        let mut word = self.0.load(Ordering::Relaxed);
        for _ in 0..2 {
            if !held(word) {
                return Marked::Changed;
            }
            crate::race::point("latch.mark");
            match self
                .0
                .compare_exchange(word, word | WAITED, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return Marked::Waits,
                Err(now) if now == word | WAITED => word = now,
                Err(_) => return Marked::Changed,
            }
        }
        Marked::Changed
    }

    /// Free at a version, as [`optimistic`](Self::optimistic) reads it, or held as the word read.
    pub fn look(&self) -> Look {
        let word = self.0.load(Ordering::Acquire);
        if held(word) {
            Look::Held(HeldWord(word))
        } else {
            Look::Free(version(word))
        }
    }

    /// Marks the hold `seen` read waited, a compare-and-swap from that word, with or without
    /// `WAITED` in it, to the word with it (DESIGN.md 9.1.3, a fix that finds its page latched).
    /// Release order: the waiter's ticket, taken before, is seen by the release that reads the
    /// mark, which finishes it. One attempt: any change since the look, another waiter's mark
    /// included, is `Changed`, and the waiter looks again at a word that moved.
    pub fn mark_held(&self, seen: HeldWord) -> Marked {
        crate::race::point("latch.mark_held");
        match self.0.compare_exchange(
            seen.0,
            seen.0 | WAITED,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => Marked::Waits,
            Err(_) => Marked::Changed,
        }
    }

    /// Whether nothing was written under the latch since [`optimistic`](Self::optimistic).
    pub fn validate(&self, seen: u64) -> bool {
        // The fence orders the reads before it ahead of this load, which then needs no acquire.
        fence(Ordering::Acquire);
        self.0.load(Ordering::Relaxed) == free_at(seen)
    }

    fn try_lock_exclusive(&self) -> bool {
        let word = self.0.load(Ordering::Relaxed);
        let taken = !held(word)
            && self
                .0
                .compare_exchange(word, word | EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
                .is_ok();
        if taken {
            before_writes();
        }
        taken
    }

    fn try_lock_exclusive_at(&self, seen: u64) -> bool {
        let taken = self
            .0
            .compare_exchange(
                free_at(seen),
                free_at(seen) | EXCLUSIVE,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok();
        if taken {
            before_writes();
        }
        taken
    }

    /// Lets an unmarked hold go, the version moved on by `bump` (0 or 1): one compare-and-swap
    /// from the held word without `WAITED` to the free word (DESIGN.md 9.1.3, a latch's release).
    /// `false` if a waiter had marked it: a mark is itself a compare-and-swap on the held word, so
    /// of the two one fails, and the hold is still the caller's, for
    /// [`unlock_marked`](Self::unlock_marked).
    fn unlock_unmarked(&self, bump: u64) -> bool {
        // Relaxed, here and below: the holder's own word, whose version only the holder moves; a
        // waiter's mark adds `WAITED` alone.
        let word = self.0.load(Ordering::Relaxed);
        debug_assert!(held(word));
        // Release on success: the writes under the hold are published with the free word. No
        // mark was there, so nothing is acquired.
        word & WAITED == 0
            && self
                .0
                .compare_exchange(
                    word,
                    free_at(version(word) + bump),
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
    }

    /// Lets a marked hold go, the version moved on by `bump`. A swap and no retry: only a mark
    /// changes a held word, and this one carries it. Acquire: the release after it finishes the
    /// ticket the marking waiter took before its mark.
    fn unlock_marked(&self, bump: u64) {
        let word = self.0.load(Ordering::Relaxed);
        let was = self.0.swap(free_at(version(word) + bump), Ordering::AcqRel);
        debug_assert!(
            held(was) && was & WAITED != 0,
            "a marked release of {was:#x}"
        );
    }
}

/// An exclusive hold on the latch `L` reaches: `&VersionedLatch`, or an owner that keeps the
/// latch alive, for a hold that outlives the borrow (a read in flight). Dropping it releases the
/// latch and bumps the version.
#[must_use = "a hold is released when dropped"]
pub struct Held<L: LatchRef>(L);

impl<L: LatchRef> Held<L> {
    /// Takes the latch if no one holds it.
    pub fn try_take(latch: L) -> Option<Self> {
        latch.try_lock_exclusive().then(|| Self(latch))
    }

    /// Takes the latch if nothing was written since `seen`: the holder then changes exactly what
    /// it read, or what it checked the frame holds.
    pub fn try_take_at(latch: L, seen: u64) -> Option<Self> {
        latch.try_lock_exclusive_at(seen).then(|| Self(latch))
    }

    /// What reaches the latch.
    pub fn latch(&self) -> &L {
        &self.0
    }

    /// The version: what it stays at after [`release_unchanged`](Self::release_unchanged). Read
    /// relaxed: the holder's own word.
    pub fn version(&self) -> u64 {
        version(self.0 .0.load(Ordering::Relaxed))
    }

    /// Releases a hold under which nothing was written, keeping the version, so readers are not
    /// sent back for nothing.
    pub fn release_unchanged(self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        let this = ManuallyDrop::new(self);
        // SAFETY: `this` is never used or dropped again; the latch reference is dropped here.
        let latch = unsafe { std::ptr::read(&this.0) };
        // The common release when no waiter marked it, else the latch's marked release, its event
        // opened before the unlock (DESIGN.md 9.1.3).
        if !latch.unlock_unmarked(0) {
            latch.release_marked(|| latch.unlock_marked(0));
        }
    }
}

impl<L: LatchRef> Drop for Held<L> {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        // As `release_unchanged`, the version moved on.
        if !self.0.unlock_unmarked(1) {
            self.0.release_marked(|| self.0.unlock_marked(1));
        }
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests {
    use super::*;

    #[test]
    fn an_optimistic_read_fails_validation_after_a_write_and_only_then() {
        let latch = VersionedLatch::default();
        let seen = latch.optimistic().unwrap();
        let hold = Held::try_take(&latch).unwrap();
        assert_eq!(latch.optimistic(), None);
        assert!(Held::try_take(&latch).is_none(), "two holders");
        assert!(
            !latch.validate(seen),
            "validated while a writer held the latch"
        );
        drop(hold);
        assert!(!latch.validate(seen), "validated across a write");
        assert_eq!(latch.optimistic(), Some(seen + 1));
    }

    #[test]
    fn a_hold_at_a_version_is_taken_only_while_nothing_was_written() {
        let latch = VersionedLatch::default();
        let seen = latch.optimistic().unwrap();
        let hold = Held::try_take_at(&latch, seen).unwrap();
        assert!(Held::try_take_at(&latch, seen).is_none(), "taken twice");
        drop(hold);
        assert!(
            Held::try_take_at(&latch, seen).is_none(),
            "taken across a write"
        );
        let now = latch.optimistic().unwrap();
        let hold = Held::try_take(&latch).unwrap();
        assert_eq!(hold.version(), now);
        hold.release_unchanged();
        assert!(
            latch.validate(now),
            "an unchanged release invalidated a reader"
        );
        drop(Held::try_take_at(&latch, now).unwrap());
    }

    /// A latch reached through a reference that counts the releases a waiter marked.
    struct Counting<'a>(&'a VersionedLatch, &'a std::cell::Cell<u32>);

    impl Deref for Counting<'_> {
        type Target = VersionedLatch;
        fn deref(&self) -> &VersionedLatch {
            self.0
        }
    }

    impl LatchRef for Counting<'_> {
        fn release_marked<F: FnOnce()>(&self, unlock: F) {
            unlock();
            self.1.set(self.1.get() + 1);
        }
    }

    /// A mark on a held latch is told to the latch's owner by the release that follows it, once,
    /// whether the release changed the page or not, and is gone after it; a mark on a free latch
    /// or one taken again since it was read finds the latch changed (DESIGN.md 9.1.2).
    #[test]
    fn a_release_after_a_mark_tells_the_owner_once_and_clears_the_mark() {
        let latch = VersionedLatch::default();
        let told = std::cell::Cell::new(0);
        assert_eq!(
            latch.mark_waited(),
            Marked::Changed,
            "a free latch has no hold to wait on"
        );
        let seen = latch.optimistic().unwrap();
        let hold = Held::try_take(Counting(&latch, &told)).unwrap();
        assert_eq!(latch.mark_waited(), Marked::Waits);
        assert_eq!(
            latch.mark_waited(),
            Marked::Waits,
            "an already marked hold is marked again"
        );
        assert_eq!(latch.optimistic(), None, "a marked hold is still held");
        hold.release_unchanged();
        assert_eq!(told.get(), 1, "the release told the owner");
        assert!(
            latch.validate(seen),
            "an unchanged release keeps the version, the mark gone"
        );
        let hold = Held::try_take(Counting(&latch, &told)).unwrap();
        drop(hold);
        assert_eq!(told.get(), 1, "a release no one marked tells no one");
        assert_eq!(latch.optimistic(), Some(seen + 1));
        let hold = Held::try_take(Counting(&latch, &told)).unwrap();
        assert_eq!(latch.mark_waited(), Marked::Waits);
        drop(hold);
        assert_eq!(
            told.get(),
            2,
            "a changing release after a mark tells the owner"
        );
        assert_eq!(
            latch.optimistic(),
            Some(seen + 2),
            "and moves the version on, the mark gone"
        );
    }

    /// A mark lost to another contender's mark is made once more from the word that one left, and
    /// waits; a mark lost to a release, or to a release and a new hold, finds the latch changed and
    /// marks nothing, the new hold's release telling no one (DESIGN.md 9.1.2).
    #[test]
    fn a_mark_lost_to_a_mark_is_made_again_and_one_lost_to_a_release_is_a_change() {
        use crate::race;
        use std::{cell::RefCell, rc::Rc};
        type Hold = Held<Counting<'static>>;
        let fresh = || -> (&'static VersionedLatch, &'static std::cell::Cell<u32>) {
            (Box::leak(Box::default()), Box::leak(Box::default()))
        };

        // Lost to another's mark.
        let (latch, told) = fresh();
        let hold = Held::try_take(Counting(latch, told)).unwrap();
        race::at("latch.mark", move || {
            assert_eq!(
                latch.mark_waited(),
                Marked::Waits,
                "the other contender marks"
            );
        });
        assert_eq!(
            latch.mark_waited(),
            Marked::Waits,
            "marked again from the marked word"
        );
        assert!(!race::pending("latch.mark"));
        drop(hold);
        assert_eq!(told.get(), 1, "the release told the owner once");

        // Lost to a release, then to a release and a new hold.
        for retake in [false, true] {
            let (latch, told) = fresh();
            let hold: Rc<RefCell<Option<Hold>>> =
                Rc::new(RefCell::new(Held::try_take(Counting(latch, told))));
            {
                let hold = hold.clone();
                race::at("latch.mark", move || {
                    drop(hold.borrow_mut().take());
                    if retake {
                        *hold.borrow_mut() = Held::try_take(Counting(latch, told));
                    }
                });
            }
            assert_eq!(latch.mark_waited(), Marked::Changed, "retake {retake}");
            assert!(!race::pending("latch.mark"));
            assert_eq!(hold.borrow().is_some(), retake);
            drop(hold.borrow_mut().take());
            assert_eq!(
                told.get(),
                0,
                "retake {retake}: no hold was marked, no release told"
            );
        }
    }

    /// A panic while the latch is held ends the process before the latch is let go (DESIGN.md
    /// 22.11). In a child process an observer thread watches the latch held by another thread,
    /// which then panics: the child must end by abort, and the observer, recording what it sees to
    /// a synced file, must never have found the latch free. Were the guard to let it go, the
    /// holder's thread would end, the observer would find the latch free, and the child would exit.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn a_panic_while_holding_the_latch_ends_the_process_before_it_is_let_go() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_panic_while_holding_the_latch_ends_the_process_before_it_is_let_go"
            ),
            || holder_panics(false),
        );
        ended_before_the_latch_was_free(ended);
    }

    /// The same, the hold handed back unchanged by a destructor the panic runs: a release other
    /// than the hold's own drop checks too.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    #[test]
    fn a_hold_handed_back_unchanged_while_unwinding_ends_the_process_before_it_is_let_go() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_hold_handed_back_unchanged_while_unwinding_ends_the_process_before_it_is_let_go"
            ),
            || holder_panics(true),
        );
        ended_before_the_latch_was_free(ended);
    }

    /// A test's holder that hands its hold back unchanged from its drop.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    struct HandsBack(Option<Held<crate::sync::Arc<VersionedLatch>>>);

    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    impl Drop for HandsBack {
        fn drop(&mut self) {
            if let Some(hold) = self.0.take() {
                hold.release_unchanged();
            }
        }
    }

    /// In the child: a thread takes the latch, holding it as itself or through [`HandsBack`], and
    /// panics once an observer watches the latch; the observer records whether it ever reads free.
    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    fn holder_panics(hands_back: bool) {
        use crate::storage::atoll::testing::record;
        use crate::sync::Arc;
        let latch = Arc::new(VersionedLatch::default());
        let (held, watched) = (std::sync::mpsc::channel(), std::sync::mpsc::channel());
        let holder = {
            let latch = latch.clone();
            let (held, watched) = (held.0, watched.1);
            std::thread::spawn(move || {
                let hold = Held::try_take(latch).unwrap();
                let _hold = if hands_back {
                    (None, Some(HandsBack(Some(hold))))
                } else {
                    (Some(hold), None)
                };
                held.send(()).unwrap();
                watched.recv().unwrap();
                panic!("a holder failed");
            })
        };
        held.1.recv().unwrap();
        let observer = std::thread::spawn(move || {
            record("seen", "watching");
            watched.0.send(()).unwrap();
            while latch.optimistic().is_none() {
                std::thread::yield_now();
            }
            record("seen", "free");
        });
        let _ = holder.join();
        observer.join().unwrap();
    }

    #[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
    fn ended_before_the_latch_was_free(ended: Option<crate::storage::atoll::testing::Ended>) {
        let Some(ended) = ended else { return };
        let seen = crate::storage::atoll::testing::recorded(&ended, "seen");
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(ended.stderr.contains("a holder failed"), "{}", ended.stderr);
        assert!(
            seen.contains("watching"),
            "the observer never ran: {seen:?}"
        );
        assert!(
            !seen.contains("free"),
            "the observer found the latch free: {seen:?}"
        );
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// A read that validates saw no write under way: against a writer that takes the latch,
    /// writes two words and lets it go, a reader that reads both optimistically and validates
    /// never finds one written and the other not.
    #[test]
    fn a_validated_read_never_mixes_a_write_under_way_with_what_was_before() {
        loom::model(|| {
            let latch = Arc::new(VersionedLatch::default());
            let words = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
            let writer = {
                let (latch, words) = (latch.clone(), words.clone());
                thread::spawn(move || {
                    let hold = Held::try_take(&*latch).expect("the only writer");
                    words[0].store(1, Ordering::Relaxed);
                    words[1].store(1, Ordering::Relaxed);
                    drop(hold);
                })
            };
            if let Some(seen) = latch.optimistic() {
                let first = words[0].load(Ordering::Relaxed);
                let second = words[1].load(Ordering::Relaxed);
                if latch.validate(seen) {
                    assert_eq!(first, second, "validated a read of a write under way");
                }
            }
            writer.join().unwrap();
        });
    }

    /// What a claim registers on, and what a marked release finishes: the pool's released cell as
    /// the audit's model has it, a current id that a release takes and finishes.
    #[derive(Clone)]
    struct Told {
        latch: Arc<VersionedLatch>,
        cell: Arc<AtomicU64>,
        done: Arc<[AtomicU64; 3]>,
    }

    impl Deref for Told {
        type Target = VersionedLatch;
        fn deref(&self) -> &VersionedLatch {
            &self.latch
        }
    }

    impl LatchRef for Told {
        fn release_marked<F: FnOnce()>(&self, unlock: F) {
            unlock();
            let id = self.cell.swap(0, Ordering::AcqRel);
            if id != 0 {
                self.done[id as usize].store(1, Ordering::Release);
            }
        }
    }

    impl Told {
        /// A claim's round on a latch it found held: registered first, then marked. The wait it
        /// parks on, or none for a latch changed under it.
        fn claim(&self, own: u64) -> Option<u64> {
            let registered =
                match self
                    .cell
                    .compare_exchange(0, own, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => own,
                    Err(id) => id,
                };
            match self.latch.mark_waited() {
                Marked::Waits => Some(registered),
                Marked::Changed => None,
            }
        }

        fn let_go(hold: Held<Told>, unchanged: bool) {
            if unchanged {
                hold.release_unchanged();
            } else {
                drop(hold);
            }
        }
    }

    /// The audit's Loom kills of 2026-11-08 on the real latch (DESIGN.md 9.1.2, item 7): a claim
    /// registers, then marks a held latch; whichever release follows the mark finishes the wait
    /// it parks on, changed or unchanged, with a mark already there, two claims, a second holder
    /// and the latch taken again. A release that decided from the word it loaded before its swap,
    /// or stored instead of swapping, loses a mark made in between and leaves a claim parked.
    #[test]
    fn every_release_after_a_mark_finishes_the_wait_registered_before_it() {
        #[derive(Clone, Copy, Debug)]
        struct Case {
            unchanged: bool,
            already: bool,
            two_claims: bool,
            second_holder: bool,
            retake: bool,
        }
        let plain = Case {
            unchanged: false,
            already: false,
            two_claims: false,
            second_holder: false,
            retake: false,
        };
        let cases = [
            plain,
            Case {
                already: true,
                ..plain
            },
            Case {
                unchanged: true,
                retake: true,
                ..plain
            },
            Case {
                two_claims: true,
                already: true,
                retake: true,
                ..plain
            },
            Case {
                second_holder: true,
                ..plain
            },
            Case {
                second_holder: true,
                unchanged: true,
                ..plain
            },
        ];
        for case in cases {
            let mut model = loom::model::Builder::new();
            model.preemption_bound = Some(3);
            model.check(move || {
                let t = Told {
                    latch: Arc::new(VersionedLatch::default()),
                    cell: Arc::new(AtomicU64::new(0)),
                    done: Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]),
                };
                let hold = Held::try_take(t.clone()).expect("free at first");
                if case.already {
                    assert_eq!(t.latch.mark_waited(), Marked::Waits);
                }
                let claims: Vec<_> = (1..=if case.two_claims { 2 } else { 1 })
                    .map(|own| {
                        let t = t.clone();
                        thread::spawn(move || t.claim(own))
                    })
                    .collect();
                let holder = case.second_holder.then(|| {
                    let t = t.clone();
                    thread::spawn(move || {
                        if let Some(hold) = Held::try_take(t) {
                            Told::let_go(hold, case.unchanged);
                        }
                    })
                });
                Told::let_go(hold, case.unchanged);
                if case.retake {
                    let hold = Held::try_take(t.clone()).expect("free once let go");
                    Told::let_go(hold, case.unchanged);
                }
                let parked: Vec<_> = claims.into_iter().map(|c| c.join().unwrap()).collect();
                if let Some(holder) = holder {
                    holder.join().unwrap();
                }
                for id in parked.into_iter().flatten() {
                    assert_eq!(
                        t.done[id as usize].load(Ordering::Acquire),
                        1,
                        "{case:?}: a wait left pending after the last holder let go"
                    );
                }
                assert!(
                    t.latch.optimistic().is_some(),
                    "{case:?}: let go, the mark gone"
                );
            });
        }
    }
}
