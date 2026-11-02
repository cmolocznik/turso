//! A bitmap that grows as bits are set, without a lock: segments of doubling size, each made on
//! first use by compare-and-swap and kept until the bitmap is dropped, so a reader never meets a
//! segment freed. The first segment holds 2^12 bits, 512 bytes, so a bitmap of a few thousand
//! numbers is one allocation; each later one doubles, so 53 of them reach every 64-bit number.

use self::sync::{AtomicPtr, AtomicU64, Ordering};
use std::ptr;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
}

const FIRST_SHIFT: u32 = 12;
const SEGMENTS: usize = (64 - FIRST_SHIFT + 1) as usize;

struct Segment {
    words: Box<[AtomicU64]>,
}

pub struct GrowBits {
    segments: [AtomicPtr<Segment>; SEGMENTS],
}

impl Default for GrowBits {
    fn default() -> Self {
        Self {
            segments: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
        }
    }
}

// SAFETY: segments are reached only through their atomic pointers and hold only atomics.
unsafe impl Send for GrowBits {}
unsafe impl Sync for GrowBits {}

/// The segment holding bit `n`, and the bit's place in it: segment `k` holds the 2^(12+k) bits
/// from 2^12 * (2^k - 1).
fn locate(n: u64) -> (usize, u64) {
    let i = (n >> FIRST_SHIFT) + 1;
    let k = (63 - i.leading_zeros()) as usize;
    (k, n - ((1u64 << k) - 1) * (1 << FIRST_SHIFT))
}

impl GrowBits {
    fn segment(&self, k: usize) -> Option<&Segment> {
        // SAFETY: published whole by `segment_or_make`, freed only with the bitmap.
        unsafe { self.segments[k].load(Ordering::Acquire).as_ref() }
    }

    fn segment_or_make(&self, k: usize) -> &Segment {
        if let Some(s) = self.segment(k) {
            return s;
        }
        let words = 1usize << (FIRST_SHIFT as usize + k - 6);
        let made = Box::into_raw(Box::new(Segment {
            words: (0..words).map(|_| AtomicU64::new(0)).collect(),
        }));
        // Release: whoever loads the segment sees its words.
        match self.segments[k].compare_exchange(
            ptr::null_mut(),
            made,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            // SAFETY: published now, freed only with the bitmap.
            Ok(_) => unsafe { &*made },
            Err(theirs) => {
                // SAFETY: never published, so still this thread's own.
                drop(unsafe { Box::from_raw(made) });
                // SAFETY: published by the thread that won.
                unsafe { &*theirs }
            }
        }
    }

    pub fn get(&self, n: u64) -> bool {
        let (k, at) = locate(n);
        self.segment(k).is_some_and(|s| {
            s.words[(at / 64) as usize].load(Ordering::Acquire) & 1 << (at % 64) != 0
        })
    }

    /// Sets bit `n`; true if it was clear.
    pub fn set(&self, n: u64) -> bool {
        let (k, at) = locate(n);
        let bit = 1 << (at % 64);
        self.segment_or_make(k).words[(at / 64) as usize].fetch_or(bit, Ordering::AcqRel) & bit == 0
    }

    /// Clears bit `n`; true if it was set.
    pub fn clear(&self, n: u64) -> bool {
        let (k, at) = locate(n);
        let bit = 1 << (at % 64);
        self.segment(k).is_some_and(|s| {
            s.words[(at / 64) as usize].fetch_and(!bit, Ordering::AcqRel) & bit != 0
        })
    }
}

impl Drop for GrowBits {
    fn drop(&mut self) {
        if cfg!(atoll_loom) && std::thread::panicking() {
            return;
        }
        for s in &self.segments {
            let p = s.load(Ordering::Acquire);
            if !p.is_null() {
                // SAFETY: published once from a box, and the bitmap is its last owner.
                drop(unsafe { Box::from_raw(p) });
            }
        }
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;
    use crate::sync::Arc;

    #[test]
    fn segments_cover_every_number_without_overlap() {
        let mut next = 0u64;
        for k in 0..6 {
            let len = 1u64 << (FIRST_SHIFT as usize + k);
            assert_eq!(locate(next), (k, 0));
            assert_eq!(locate(next + len - 1), (k, len - 1));
            next += len;
        }
        assert_eq!(locate(u64::MAX).0, SEGMENTS - 1);
    }

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
