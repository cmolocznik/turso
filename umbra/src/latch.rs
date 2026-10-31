//! The versioned latch (Umbra §2.3): one 64-bit word, 5 bits of state and 59 of version.
//!
//! State 0 is unlocked, 1..=30 counts shared holders, 31 is exclusive. Releasing an exclusive
//! latch bumps the version, so an optimistic reader, which takes no lock and only remembers the
//! version, learns afterwards whether the page changed under it.

use std::sync::atomic::{fence, AtomicU64, Ordering};

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
        let word = self.0.load(Ordering::Relaxed);
        state(word) < MAX_SHARED
            && self
                .0
                .compare_exchange_weak(word, word + 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
    }

    pub fn lock_shared(&self) {
        while !self.try_lock_shared() {
            std::hint::spin_loop();
            std::thread::yield_now();
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

    pub fn lock_exclusive(&self) {
        while !self.try_lock_exclusive() {
            std::hint::spin_loop();
            std::thread::yield_now();
        }
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
        latch.lock_shared();
        latch.unlock_shared();
        assert!(latch.validate(seen), "a shared holder invalidated an optimistic read");
        latch.lock_exclusive();
        assert_eq!(latch.optimistic(), None);
        assert!(!latch.validate(seen), "validated while a writer held the latch");
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
    fn shared_holders_stop_at_thirty() {
        let latch = VersionedLatch::default();
        for _ in 0..MAX_SHARED {
            assert!(latch.try_lock_shared());
        }
        assert!(!latch.try_lock_shared());
    }
}
