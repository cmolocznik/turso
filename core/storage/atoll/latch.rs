//! The versioned latch (CONCURRENCY.md §5): one 64-bit word, an exclusive bit and 63 bits of
//! version.
//!
//! There is one kind of hold, exclusive, for changing a page or what a frame holds. Readers take
//! nothing: they remember the version, read, and validate that it did not change. Releasing an
//! exclusive hold bumps the version, unless nothing was written under it.
//!
//! A hold exists only as a [`Held`] guard, from the instruction that took it, so a panic while it
//! lives releases it: no path can leave a latch held.
//!
//! Nothing waits for a latch. A taker that finds it held yields to its caller (`IOResult::IO` with
//! an explicit yield) and tries again: under Turso's cooperative scheduling the holder may be
//! another connection on the very same thread.

use crate::sync::atomic::{fence, AtomicU64, Ordering};
use std::mem::ManuallyDrop;
use std::ops::Deref;

const EXCLUSIVE: u64 = 1;

#[derive(Debug, Default)]
pub struct VersionedLatch(AtomicU64);

fn version(word: u64) -> u64 {
    word >> 1
}

fn held(word: u64) -> bool {
    word & EXCLUSIVE != 0
}

impl VersionedLatch {
    /// The version to validate against later, or `None` while a writer holds the latch.
    pub fn optimistic(&self) -> Option<u64> {
        let word = self.0.load(Ordering::Acquire);
        (!held(word)).then_some(version(word))
    }

    /// Whether nothing was written under the latch since [`optimistic`](Self::optimistic).
    pub fn validate(&self, seen: u64) -> bool {
        fence(Ordering::Acquire);
        self.0.load(Ordering::Relaxed) == seen << 1
    }

    fn try_lock_exclusive(&self) -> bool {
        let word = self.0.load(Ordering::Relaxed);
        !held(word)
            && self
                .0
                .compare_exchange(word, word | EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
    }

    fn try_lock_exclusive_at(&self, seen: u64) -> bool {
        self.0
            .compare_exchange(
                seen << 1,
                seen << 1 | EXCLUSIVE,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    fn unlock_exclusive_unchanged(&self) {
        let word = self.0.load(Ordering::Relaxed);
        debug_assert!(held(word));
        self.0.store(word & !EXCLUSIVE, Ordering::Release);
    }

    fn unlock_exclusive(&self) {
        let word = self.0.load(Ordering::Relaxed);
        debug_assert!(held(word));
        self.0.store((version(word) + 1) << 1, Ordering::Release);
    }
}

/// An exclusive hold on the latch `L` reaches: `&VersionedLatch`, or an owner that keeps the
/// latch alive, for a hold that outlives the borrow (a read in flight). Dropping it releases the
/// latch and bumps the version.
#[must_use = "a hold is released when dropped"]
pub struct Held<L: Deref<Target = VersionedLatch>>(L);

impl<L: Deref<Target = VersionedLatch>> Held<L> {
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

    /// The version: what it stays at after [`release_unchanged`](Self::release_unchanged).
    pub fn version(&self) -> u64 {
        version(self.0 .0.load(Ordering::Relaxed))
    }

    /// Releases a hold under which nothing was written, keeping the version, so readers are not
    /// sent back for nothing.
    pub fn release_unchanged(self) {
        let this = ManuallyDrop::new(self);
        // SAFETY: `this` is never used or dropped again; the latch reference is dropped here.
        let latch = unsafe { std::ptr::read(&this.0) };
        latch.unlock_exclusive_unchanged();
    }
}

impl<L: Deref<Target = VersionedLatch>> Drop for Held<L> {
    fn drop(&mut self) {
        self.0.unlock_exclusive();
    }
}

#[cfg(all(test, not(shuttle)))]
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

    #[test]
    fn a_panic_while_holding_releases_the_latch() {
        let latch = VersionedLatch::default();
        let panicked = std::panic::catch_unwind(|| {
            let _hold = Held::try_take(&latch).unwrap();
            panic!("a holder failed");
        });
        assert!(panicked.is_err());
        assert!(
            latch.optimistic().is_some(),
            "the panic left the latch held"
        );
    }
}
