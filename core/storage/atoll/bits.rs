//! A bitmap that grows as bits are set, without a lock: its words are a [`Segmented`] array, whose
//! first segment of 64 words holds 4096 bits.

use self::sync::{AtomicU64, Ordering};
use super::segmented::Segmented;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicU64, Ordering};
}

/// Bits in words of a [`Segmented`] array.
#[derive(Default)]
pub struct GrowBits {
    words: Segmented<AtomicU64>,
}

impl GrowBits {
    pub fn get(&self, n: u64) -> bool {
        self.words
            .get(n / 64)
            .is_some_and(|w| w.load(Ordering::Acquire) & 1 << (n % 64) != 0)
    }

    /// Sets bit `n`; true if it was clear.
    pub fn set(&self, n: u64) -> bool {
        let bit = 1 << (n % 64);
        self.words
            .get_or_make(n / 64)
            .fetch_or(bit, Ordering::AcqRel)
            & bit
            == 0
    }

    /// The bits set, in order: a bit set or cleared during the scan may or may not be among them.
    pub fn ones(&self) -> Vec<u64> {
        let mut out = Vec::new();
        self.words.each(|i, word| {
            let mut bits = word.load(Ordering::Acquire);
            while bits != 0 {
                out.push(i * 64 + bits.trailing_zeros() as u64);
                bits &= bits - 1;
            }
        });
        out
    }

    /// Clears bit `n`; true if it was set.
    pub fn clear(&self, n: u64) -> bool {
        let bit = 1 << (n % 64);
        self.words
            .get(n / 64)
            .is_some_and(|w| w.fetch_and(!bit, Ordering::AcqRel) & bit != 0)
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;
    use crate::sync::Arc;

    /// Threads set and clear their own bits across segments made as they go: every bit reads as
    /// its thread left it.
    fn bits_from_threads_read_as_set(threads: u64, per: u64) {
        let bits = Arc::new(GrowBits::default());
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let bits = bits.clone();
                crate::thread::spawn(move || {
                    for i in 0..per {
                        let n = i * threads + t;
                        assert!(bits.set(n));
                        if i % 3 == 0 {
                            assert!(bits.clear(n));
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        for n in 0..threads * per {
            assert_eq!(bits.get(n), (n / threads) % 3 != 0, "bit {n}");
        }
    }

    #[cfg(not(shuttle))]
    #[test]
    fn bits_from_threads_read_as_set_on_threads() {
        bits_from_threads_read_as_set(3, if cfg!(miri) { 2_000 } else { 100_000 });
    }

    #[cfg(shuttle)]
    #[test]
    fn bits_from_threads_read_as_set_in_shuttle_schedules() {
        crate::storage::atoll::testing::schedules(|| bits_from_threads_read_as_set(2, 3), 2_000);
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Two threads set bits in a segment neither has made: one makes it, the other's copy goes,
    /// and both bits read set.
    #[test]
    fn bits_set_as_their_segment_is_made_both_read_set() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let bits = Arc::new(GrowBits::default());
            let other = {
                let bits = bits.clone();
                thread::spawn(move || bits.set(1))
            };
            assert!(bits.set(2));
            assert!(other.join().unwrap());
            assert!(bits.get(1) && bits.get(2));
        });
    }
}
