//! A load's group (ASYNC1 D2e §2.2b): the parties that keep the page a frame is being read into,
//! from the load's start until each of them has fixed it or let it go, so a waiter that comes
//! back after the load finds the page it waited for still in memory.
//!
//! Each party holds one reference: the load's guard while the load is under way, the loader's
//! own when its fix runs for an operation's owner, and one for each fix that joins the load. The
//! group as a whole holds one keep on the frame, the record word's group bit
//! ([`super::frame::FrameHeader::group_kept`]), so no count ever moves between words.
//!
//! The word is `2 × references + open`, one per 4 KiB grain, at a frame's first grain. Zero is a
//! group closed with no references left: the only value a join is refused at, apart from the
//! limit. The loader opens the word from zero under its exclusive latch, before it publishes the
//! load; it closes it under the same latch, before it publishes the page or frees the frame, in one
//! subtraction that also drops the guard. Each other reference is released in one subtraction, in
//! either phase. The one party whose change takes the word to zero lets the group's keep go.
//!
//! A join lands only on the group of the load its probe found: it is made in the step its probe
//! was made in, and a frame over that grain is read into again only once every step that could
//! still see the old one has ended (`BufferPool::quiet_at`), while the frame's previous group had
//! already reached zero before its keep was let go and the frame freed.

use super::super::epoch::Step;
use super::memory::Zeroable;

#[cfg(not(atoll_loom))]
use crate::sync::atomic::{AtomicU32, Ordering};
#[cfg(atoll_loom)]
use loom::sync::atomic::{AtomicU32, Ordering};

/// The group is open: its load is under way, and its guard holds a reference.
const OPEN: u32 = 1;
/// One reference.
const REFERENCE: u32 = 2;

/// A frame's group word.
#[derive(Debug, Default)]
pub(super) struct GroupWord(AtomicU32);

// SAFETY: an atomic, valid as zero: a group closed with no references.
unsafe impl Zeroable for GroupWord {}

impl GroupWord {
    /// Opens the group of a load starting: the guard's reference, and the loader's own if
    /// `loader_keeps`. By the loader, under its exclusive latch, before it publishes the load. The
    /// frame's previous group, if any, reached zero before its keep was let go and the frame
    /// freed, so the word is zero.
    pub(super) fn open(&self, loader_keeps: bool) {
        let to = OPEN + REFERENCE + if loader_keeps { REFERENCE } else { 0 };
        let opened = self
            .0
            .compare_exchange(0, to, Ordering::AcqRel, Ordering::Acquire);
        assert!(
            opened.is_ok(),
            "a load's group opened over a group with references left: {opened:?}"
        );
    }

    /// Adds a reference for a fix that found the load under way, in the step `_step` its probe was
    /// made in: whether it did. Refused once the group's last reference is gone, or at the limit.
    /// A lost compare-and-swap is another party's join, release or close.
    pub(super) fn join(&self, _step: &Step<'_>) -> bool {
        self.add_reference()
    }

    /// [`join`](Self::join)'s change: a reference added unless none is left or the word is full.
    fn add_reference(&self) -> bool {
        let mut word = self.0.load(Ordering::Acquire);
        loop {
            if word == 0 || word > u32::MAX - REFERENCE {
                return false;
            }
            match self.0.compare_exchange(
                word,
                word + REFERENCE,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(now) => word = now,
            }
        }
    }

    /// Closes the group and drops its guard, in one change: whether the guard was its last
    /// reference, the closer then letting the group's keep go. By the loader, under its exclusive
    /// latch, before it publishes the page or frees the frame.
    pub(super) fn close(&self) -> bool {
        let was = self.0.fetch_sub(OPEN + REFERENCE, Ordering::AcqRel);
        assert!(
            was & OPEN != 0 && was >= OPEN + REFERENCE,
            "a load's group closed that was not open: {was:#x}"
        );
        was == OPEN + REFERENCE
    }

    /// Releases a reference other than the guard, in either phase, holding no latch: whether it
    /// was the group's last, the releaser then letting the group's keep go.
    pub(super) fn release(&self) -> bool {
        let was = self.0.fetch_sub(REFERENCE, Ordering::AcqRel);
        assert!(
            was >= REFERENCE && was != OPEN + REFERENCE,
            "a reference released that its load's group did not hold: {was:#x}"
        );
        was == REFERENCE
    }

    /// Whether no reference is left, for a check made where the word cannot change: under a hold
    /// of the frame with its group's keep gone, or just before the keep is let go.
    #[cfg(test)]
    pub(super) fn released(&self) -> bool {
        self.0.load(Ordering::Acquire) == 0
    }

    /// The references the group holds now, the guard's included, for a test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn references(&self) -> u32 {
        self.0.load(Ordering::Acquire) / REFERENCE
    }
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
        g.open(loader_keeps);
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
        if g.release() {
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
            assert!(g.add_reference(), "a joiner admitted to an open group");
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
                    if g.add_reference() {
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
            assert!(g.add_reference(), "a joiner admitted to an open group");
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
                    assert!(g.release(), "the last reference");
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
            g.open(false);
            assert!(g.close(), "the new load's guard was its only reference");
            releaser.join().unwrap();
        });
    }
}
