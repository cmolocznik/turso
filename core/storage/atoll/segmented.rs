//! An array that grows as it is used, without a lock: segments of doubling size, each made on
//! first use by compare-and-swap and kept until the array is dropped, so a reader never meets a
//! segment freed. The first segment holds 64 items, so a small array is one allocation; each later
//! one doubles, so 59 of them reach every 64-bit index.

use self::sync::{AtomicPtr, Ordering};
use std::ptr;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicPtr, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicPtr, Ordering};
}

const FIRST_SHIFT: u32 = 6;
const SEGMENTS: usize = (64 - FIRST_SHIFT + 1) as usize;

/// What [`Segmented::next_from`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    /// The item at this index is wanted.
    At(u64),
    /// None wanted among the items looked at: look again from this index.
    Passed(u64),
    /// No item at or after the index is wanted: every segment after it passed.
    End,
}

pub struct Segmented<T> {
    segments: [AtomicPtr<T>; SEGMENTS],
}

impl<T> Default for Segmented<T> {
    fn default() -> Self {
        Self {
            segments: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
        }
    }
}

// SAFETY: a segment is reached only through its atomic pointer and is shared like `&[T]`.
unsafe impl<T: Send + Sync> Send for Segmented<T> {}
unsafe impl<T: Send + Sync> Sync for Segmented<T> {}

/// The segment holding item `i`, and its place in it: segment `k` holds the 2^(6+k) items from
/// 2^6 * (2^k - 1).
fn locate(i: u64) -> (usize, usize) {
    let n = (i >> FIRST_SHIFT) + 1;
    let k = (63 - n.leading_zeros()) as usize;
    (k, (i - ((1u64 << k) - 1) * (1 << FIRST_SHIFT)) as usize)
}

fn len(k: usize) -> usize {
    1 << (FIRST_SHIFT as usize + k)
}

impl<T: Default> Segmented<T> {
    /// Item `i`, if its segment was ever made.
    pub fn get(&self, i: u64) -> Option<&T> {
        let (k, at) = locate(i);
        let p = self.segments[k].load(Ordering::Acquire);
        // SAFETY: published whole by `get_or_make`, freed only with the array.
        (!p.is_null()).then(|| unsafe { &*p.add(at) })
    }

    /// Every item of the segments made so far, with its index, in order.
    pub fn each(&self, mut f: impl FnMut(u64, &T)) {
        for (k, segment) in self.segments.iter().enumerate() {
            let p = segment.load(Ordering::Acquire);
            if p.is_null() {
                continue;
            }
            let first = ((1u64 << k) - 1) << FIRST_SHIFT;
            for at in 0..len(k) {
                // SAFETY: as in `get`.
                f(first + at as u64, unsafe { &*p.add(at) });
            }
        }
    }

    /// The first item at or after `i` that `wanted` takes, looking at `max` items at most and
    /// passing a segment never made at once (DESIGN.md 22.1).
    pub fn next_from(&self, i: u64, max: u64, wanted: impl Fn(&T) -> bool) -> Next {
        let mut i = i;
        let mut looked = 0;
        while looked < max {
            let (k, _) = locate(i);
            if self.segments[k].load(Ordering::Acquire).is_null() {
                if k + 1 >= SEGMENTS {
                    return Next::End;
                }
                // The next segment's first item: a segment never made is passed whole.
                i = ((1u64 << (k + 1)) - 1) << FIRST_SHIFT;
                continue;
            }
            let item = self.get(i).expect("an item of a segment made");
            looked += 1;
            if wanted(item) {
                return Next::At(i);
            }
            i += 1;
        }
        Next::Passed(i)
    }

    /// Item `i`, its segment made if it was not.
    pub fn get_or_make(&self, i: u64) -> &T {
        let (k, at) = locate(i);
        let mut p = self.segments[k].load(Ordering::Acquire);
        if p.is_null() {
            let made: Box<[T]> = (0..len(k)).map(|_| T::default()).collect();
            let made = Box::into_raw(made).cast::<T>();
            // Release: whoever loads the segment sees its items made.
            p = match self.segments[k].compare_exchange(
                ptr::null_mut(),
                made,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => made,
                Err(theirs) => {
                    // SAFETY: never published, so still this thread's own, of `len(k)` items.
                    drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(made, len(k))) });
                    theirs
                }
            };
        }
        // SAFETY: as in `get`.
        unsafe { &*p.add(at) }
    }
}

impl<T> Drop for Segmented<T> {
    fn drop(&mut self) {
        if cfg!(atoll_loom) && std::thread::panicking() {
            return;
        }
        for (k, s) in self.segments.iter().enumerate() {
            let p = s.load(Ordering::Acquire);
            if !p.is_null() {
                // SAFETY: published once from a box of `len(k)` items, and the array is its last
                // owner.
                drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(p, len(k))) });
            }
        }
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;

    #[test]
    fn segments_cover_every_index_without_overlap() {
        let mut next = 0u64;
        for k in 0..8 {
            assert_eq!(locate(next), (k, 0));
            assert_eq!(locate(next + len(k) as u64 - 1), (k, len(k) - 1));
            next += len(k) as u64;
        }
        assert_eq!(locate(u64::MAX).0, SEGMENTS - 1);
    }

    /// Threads reach items across segments made as they go, each segment by whichever thread
    /// gets there first: every item one thread stores another reads.
    fn items_from_threads_land_once(threads: u64, per: u64) {
        use crate::sync::atomic::AtomicU64;
        use crate::sync::Arc;
        let items = Arc::new(Segmented::<AtomicU64>::default());
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let items = items.clone();
                crate::thread::spawn(move || {
                    for i in 0..per {
                        let n = i * threads + t;
                        items.get_or_make(n).store(n + 1, Ordering::Release);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        for n in 0..threads * per {
            assert_eq!(items.get(n).map(|v| v.load(Ordering::Acquire)), Some(n + 1));
        }
    }

    #[cfg(not(shuttle))]
    #[test]
    fn items_from_threads_land_once_on_threads() {
        items_from_threads_land_once(3, if cfg!(miri) { 300 } else { 100_000 });
    }

    #[cfg(shuttle)]
    #[test]
    fn items_from_threads_land_once_in_shuttle_schedules() {
        crate::storage::atoll::testing::schedules(|| items_from_threads_land_once(2, 70), 1_000);
    }
}
