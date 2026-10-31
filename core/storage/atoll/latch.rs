//! The versioned latch (Umbra §2.3): one 64-bit word, 5 bits of state and 59 of version.
//!
//! State 0 is unlocked, 1..=30 counts shared holders, 31 is exclusive. Releasing an exclusive
//! latch bumps the version, so an optimistic reader, which takes no lock and only remembers the
//! version, learns afterwards whether the page changed under it.
//!
//! There is no blocking acquire. Under Turso's cooperative scheduling another connection holding
//! the latch may run on this very thread, so a caller that cannot take the latch yields
//! (`IOResult::IO` with an explicit yield) and tries again.

use crate::sync::atomic::{AtomicU64, Ordering};
use std::sync::atomic::fence;

const STATE_BITS: u32 = 5;
const STATE_MASK: u64 = (1 << STATE_BITS) - 1;
const EXCLUSIVE: u64 = STATE_MASK;
const MAX_SHARED: u64 = EXCLUSIVE - 1;

#[derive(Debug, Default)]
pub struct VersionedLatch(AtomicU64);

fn state(word: u64) -> u64 {
    word & STATE_MASK
}

fn version(word: u64) -> u64 {
    word >> STATE_BITS
}

impl VersionedLatch {
    /// The version to validate against later, or `None` while a writer holds the latch.
    pub fn optimistic(&self) -> Option<u64> {
        let word = self.0.load(Ordering::Acquire);
        (state(word) != EXCLUSIVE).then_some(version(word))
    }

    /// Whether nothing was written under the latch since [`optimistic`](Self::optimistic).
    pub fn validate(&self, seen: u64) -> bool {
        fence(Ordering::Acquire);
        let word = self.0.load(Ordering::Relaxed);
        state(word) != EXCLUSIVE && version(word) == seen
    }

    pub fn try_lock_shared(&self) -> bool {
        let mut word = self.0.load(Ordering::Relaxed);
        loop {
            if state(word) >= MAX_SHARED {
                return false;
            }
            match self
                .0
                .compare_exchange(word, word + 1, Ordering::Acquire, Ordering::Relaxed)
            {
                Ok(_) => return true,
                // Another shared holder came or went: try again with what it left.
                Err(now) => word = now,
            }
        }
    }

    pub fn unlock_shared(&self) {
        let before = self.0.fetch_sub(1, Ordering::Release);
        debug_assert!((1..=MAX_SHARED).contains(&state(before)));
    }

    pub fn try_lock_exclusive(&self) -> bool {
        let word = self.0.load(Ordering::Relaxed);
        state(word) == 0
            && self
                .0
                .compare_exchange(word, word | EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
    }

    /// Turns the caller's shared hold into an exclusive one, if no one else holds the latch.
    pub fn try_upgrade(&self) -> bool {
        let word = self.0.load(Ordering::Relaxed);
        state(word) == 1
            && self
                .0
                .compare_exchange(
                    word,
                    word - 1 + EXCLUSIVE,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
    }

    /// Takes the latch exclusively if it is free and nothing was written since `seen`, so an
    /// optimistic reader can go on to change what it read.
    pub fn try_lock_exclusive_at(&self, seen: u64) -> bool {
        let word = seen << STATE_BITS;
        self.0
            .compare_exchange(word, word | EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// The version, read by the exclusive holder: what it stays at after
    /// [`unlock_exclusive_unchanged`](Self::unlock_exclusive_unchanged).
    pub fn version_held(&self) -> u64 {
        let word = self.0.load(Ordering::Relaxed);
        debug_assert_eq!(state(word), EXCLUSIVE);
        version(word)
    }

    /// Releases an exclusive hold under which nothing was written, keeping the version, so
    /// optimistic readers are not sent back for nothing.
    pub fn unlock_exclusive_unchanged(&self) {
        let word = self.0.load(Ordering::Relaxed);
        debug_assert_eq!(state(word), EXCLUSIVE);
        self.0.store(word & !STATE_MASK, Ordering::Release);
    }

    pub fn unlock_exclusive(&self) {
        let word = self.0.load(Ordering::Relaxed);
        debug_assert_eq!(state(word), EXCLUSIVE);
        self.0
            .store((version(word) + 1) << STATE_BITS, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_optimistic_read_fails_validation_after_a_write_and_only_then() {
        let latch = VersionedLatch::default();
        let seen = latch.optimistic().unwrap();
        assert!(latch.try_lock_shared());
        latch.unlock_shared();
        assert!(
            latch.validate(seen),
            "a shared holder invalidated an optimistic read"
        );
        assert!(latch.try_lock_exclusive());
        assert_eq!(latch.optimistic(), None);
        assert!(
            !latch.validate(seen),
            "validated while a writer held the latch"
        );
        latch.unlock_exclusive();
        assert!(!latch.validate(seen), "validated across a write");
        assert_eq!(latch.optimistic(), Some(seen + 1));
    }

    #[test]
    fn shared_holders_exclude_a_writer_and_a_writer_excludes_everyone() {
        let latch = VersionedLatch::default();
        assert!(latch.try_lock_shared());
        assert!(latch.try_lock_shared());
        assert!(!latch.try_lock_exclusive());
        latch.unlock_shared();
        latch.unlock_shared();
        assert!(latch.try_lock_exclusive());
        assert!(!latch.try_lock_shared());
        assert!(!latch.try_lock_exclusive());
        latch.unlock_exclusive();
        assert!(latch.try_lock_shared());
    }

    #[test]
    fn upgrades_succeed_only_for_a_sole_holder_or_an_unchanged_version() {
        let latch = VersionedLatch::default();
        assert!(latch.try_lock_shared() && latch.try_lock_shared());
        assert!(
            !latch.try_upgrade(),
            "upgraded beside another shared holder"
        );
        latch.unlock_shared();
        assert!(latch.try_upgrade());
        latch.unlock_exclusive();
        let seen = latch.optimistic().unwrap();
        assert!(latch.try_lock_exclusive_at(seen));
        latch.unlock_exclusive();
        assert!(
            !latch.try_lock_exclusive_at(seen),
            "an optimistic reader upgraded across a write"
        );
        let now = latch.optimistic().unwrap();
        assert!(latch.try_lock_shared());
        assert!(
            !latch.try_lock_exclusive_at(now),
            "upgraded beside a shared holder"
        );
        latch.unlock_shared();
        assert!(latch.try_lock_exclusive());
        latch.unlock_exclusive_unchanged();
        assert!(
            latch.validate(now),
            "an unchanged release invalidated a reader"
        );
    }

    #[test]
    fn shared_holders_stop_at_thirty() {
        let latch = VersionedLatch::default();
        for _ in 0..MAX_SHARED {
            assert!(latch.try_lock_shared());
        }
        assert!(!latch.try_lock_shared());
    }
}
