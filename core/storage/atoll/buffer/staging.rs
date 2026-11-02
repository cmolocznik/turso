//! The writeback buffer: one block of fixed size, allocated once, that copies of pages written
//! back while they stay in their frames are made in. A page written out of its frame needs none:
//! its write-out holds the frame, and it is written from there.
//!
//! The block is a ring. A copy leases the next contiguous run of it, wrapping to the start when
//! the end is too short, and gives it back once its write has landed; the ring's tail moves past
//! leases given back in the order they were taken, so a write landing early holds its room until
//! the older ones land. A lease that does not fit waits for room: the size of the block bounds the
//! writes in flight, and so the share of the device the cleaner takes, and its memory.
//!
//! Lock-free. The head and the tail are positions that only rise, a lap of the ring apart at most:
//! a lease is one compare-and-swap of the head, a wrap a lease of the end's remainder given back
//! at once. A lease given back records its end at the unit it starts in, and whoever gives one
//! back then moves the tail past every lease back at it. The record and the tail are each written
//! and then the other read across a fence, so either the giver sees the tail reach its lease or
//! the thread that moved it sees the lease back: room given back is never left behind the tail.

use self::sync::{fence, AtomicU64, Ordering};
use super::super::aligned::AlignedBlock;
use crate::io::Buffer;
use crate::sync::Arc;

/// The engine's atomics, or loom's.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicU64, Ordering};
}

/// The leases of a ring of `cap` bytes, in whole units.
struct Ring {
    cap: u64,
    unit: u64,
    /// Where the next lease starts, as a position that only rises: its offset is this modulo
    /// `cap`.
    head: AtomicU64,
    /// Where the oldest lease not given back starts; everything before it is free.
    tail: AtomicU64,
    /// Per unit: the end of the lease given back that starts there, or an older one's.
    ends: Box<[AtomicU64]>,
}

pub struct Staging {
    block: Arc<AlignedBlock>,
    ring: Ring,
}

/// A run of the ring, its holder's alone until [`Staging::give_back`].
#[derive(Debug, PartialEq, Eq)]
struct Lease {
    /// The run's offset in the block.
    start: usize,
    len: usize,
    /// Its position.
    at: u64,
}

/// A lease that gives its room back when dropped: a write that never starts, or a writeback
/// dropped before it ends, leaves no room taken.
pub struct Leased {
    staging: Arc<Staging>,
    lease: Option<Lease>,
}

impl Leased {
    /// The leased bytes, to copy a page into.
    pub fn bytes(&mut self) -> &mut [u8] {
        let lease = self.lease.as_mut().expect("held until dropped");
        self.staging.bytes(lease)
    }

    /// The leased bytes as an IO buffer, to write from.
    pub fn buffer(&self) -> Arc<Buffer> {
        self.staging
            .buffer(self.lease.as_ref().expect("held until dropped"))
    }
}

impl Drop for Leased {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.staging.give_back(lease);
        }
    }
}

impl Ring {
    fn new(cap: u64, unit: u64) -> Self {
        Self {
            cap,
            unit,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            ends: (0..cap / unit).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn end_at(&self, at: u64) -> &AtomicU64 {
        &self.ends[((at % self.cap) / self.unit) as usize]
    }

    /// `len` contiguous bytes, if the ring has them free; `len` is a whole number of units and at
    /// most the ring's.
    fn take(&self, len: u64) -> Option<Lease> {
        debug_assert!(len <= self.cap && len % self.unit == 0);
        loop {
            // The tail first: the head, read after it, is at or past it.
            let tail = self.tail.load(Ordering::Acquire);
            let head = self.head.load(Ordering::Acquire);
            let offset = head % self.cap;
            // The end is too short: a lease of the rest, given back at once, wraps to the start.
            let pad = if offset + len > self.cap {
                self.cap - offset
            } else {
                0
            };
            // An empty ring's padding is behind the tail as soon as it is given back.
            let used = if tail == head {
                len
            } else {
                head + pad + len - tail
            };
            if used > self.cap {
                return None;
            }
            match self.head.compare_exchange_weak(
                head,
                head + pad + len,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if pad > 0 {
                        self.give_back(head, pad);
                    }
                    return Some(Lease {
                        start: ((head + pad) % self.cap) as usize,
                        len: len as usize,
                        at: head + pad,
                    });
                }
                Err(_) => {}
            }
        }
    }

    /// Gives back the run of `len` at position `at`, and moves the tail past every run given back
    /// at it.
    fn give_back(&self, at: u64, len: u64) {
        // Release: whoever moves the tail past it sees the run's bytes done with.
        self.end_at(at).store(at + len, Ordering::Release);
        loop {
            // Pairs with the fence of whoever moves the tail: one sees the other.
            fence(Ordering::SeqCst);
            let tail = self.tail.load(Ordering::Acquire);
            // An end recorded a lap before is at or behind the tail.
            let end = self.end_at(tail).load(Ordering::Acquire);
            if end <= tail {
                return;
            }
            let _ = self
                .tail
                .compare_exchange(tail, end, Ordering::AcqRel, Ordering::Acquire);
        }
    }
}

impl Staging {
    /// A ring of `len` bytes, a multiple of the direct IO alignment.
    pub fn new(len: usize) -> Self {
        Self {
            block: Arc::new(AlignedBlock::zeroed(len)),
            ring: Ring::new(len as u64, crate::io::DIRECT_IO_ALIGNMENT as u64),
        }
    }

    /// `len` contiguous bytes, held until the guard is dropped, if the ring has them free.
    pub fn lease(self: &Arc<Self>, len: usize) -> Option<Leased> {
        Some(Leased {
            staging: self.clone(),
            lease: Some(self.take(len)?),
        })
    }

    /// `len` contiguous bytes, if the ring has them free; `len` is a multiple of the direct IO
    /// alignment and at most the ring's.
    fn take(&self, len: usize) -> Option<Lease> {
        self.ring.take(len as u64)
    }

    /// The leased bytes, to write from.
    fn buffer(&self, lease: &Lease) -> Arc<Buffer> {
        self.block.region(lease.start, lease.len)
    }

    /// The leased bytes as a slice. Only the lease's holder reaches them, and only until it gives
    /// them back.
    #[allow(clippy::mut_from_ref)]
    fn bytes<'a>(&'a self, lease: &'a mut Lease) -> &'a mut [u8] {
        // SAFETY: inside the block; a lease's run is no other lease's while it is held.
        unsafe { std::slice::from_raw_parts_mut(self.block.as_ptr().add(lease.start), lease.len) }
    }

    /// Gives `lease` back once its write has landed.
    fn give_back(&self, lease: Lease) {
        self.ring.give_back(lease.at, lease.len as u64);
    }
}

/// Threads lease runs of a ring and give them back in whatever order they finish: no unit is
/// leased twice at once, and once all are back the whole ring is free.
#[cfg(test)]
fn leases_from_threads_never_overlap_and_all_room_comes_back(threads: usize, rounds: usize) {
    use crate::sync::atomic::AtomicBool;
    let units = 8u64;
    let ring = Arc::new(Ring::new(units, 1));
    let used: Arc<Vec<AtomicBool>> = Arc::new((0..units).map(|_| AtomicBool::new(false)).collect());
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (ring, used) = (ring.clone(), used.clone());
            crate::thread::spawn(move || {
                for round in 0..rounds {
                    let len = 1 + ((t + round) as u64 % 3);
                    let Some(lease) = ring.take(len) else {
                        crate::thread::yield_now();
                        continue;
                    };
                    for u in lease.start..lease.start + lease.len {
                        assert!(
                            !used[u].swap(true, Ordering::AcqRel),
                            "unit {u} leased twice"
                        );
                    }
                    crate::thread::yield_now();
                    for u in lease.start..lease.start + lease.len {
                        used[u].store(false, Ordering::Release);
                    }
                    ring.give_back(lease.at, lease.len as u64);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert!(
        ring.take(units).is_some(),
        "room given back was left behind the tail"
    );
}

#[cfg(all(test, not(atoll_loom), not(shuttle)))]
#[test]
fn leases_from_threads_never_overlap_and_all_room_comes_back_on_threads() {
    leases_from_threads_never_overlap_and_all_room_comes_back(3, 100_000);
}

#[cfg(all(test, shuttle))]
#[test]
fn leases_from_threads_never_overlap_and_all_room_comes_back_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || leases_from_threads_never_overlap_and_all_room_comes_back(2, 6),
        2_000,
    );
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::cell::UnsafeCell;
    use loom::sync::Arc;
    use loom::thread;

    /// The ring's bytes, a unit each: loom fails a write by a lease's holder not ordered after
    /// the last holder's.
    struct Units(Vec<UnsafeCell<u64>>);

    // SAFETY: shared to be raced on: loom checks every access.
    unsafe impl Sync for Units {}

    impl Units {
        fn fill(&self, lease: &Lease, with: u64) {
            for u in lease.start..lease.start + lease.len {
                // SAFETY: loom fails this write if another holder's is not ordered with it.
                self.0[u].with_mut(|v| unsafe { *v = with });
            }
        }
    }

    /// Two leases given back out of order while the other is taken: the tail passes both, so
    /// the whole ring is free once both are back, and its next holder's writes follow theirs.
    #[test]
    fn room_given_back_in_any_order_all_comes_back() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let ring = Arc::new(Ring::new(4, 1));
            let units = Arc::new(Units((0..4).map(|_| UnsafeCell::new(0)).collect()));
            let first = ring.take(2).unwrap();
            let other = {
                let (ring, units) = (ring.clone(), units.clone());
                thread::spawn(move || {
                    let lease = ring.take(2).expect("room for two");
                    units.fill(&lease, 2);
                    ring.give_back(lease.at, lease.len as u64);
                })
            };
            units.fill(&first, 1);
            ring.give_back(first.at, first.len as u64);
            other.join().unwrap();
            let whole = ring
                .take(4)
                .expect("room given back was left behind the tail");
            units.fill(&whole, 3);
        });
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;
    use crate::io::DIRECT_IO_ALIGNMENT as A;
    use std::collections::VecDeque;

    /// Leases never overlap, wrap when the end is short, wait while the ring is full, and free
    /// room in the order they were taken.
    #[test]
    fn leases_never_overlap_and_room_frees_in_order() {
        let ring = Staging::new(8 * A);
        let a = ring.take(3 * A).unwrap();
        let b = ring.take(3 * A).unwrap();
        assert_eq!((a.start, b.start), (0, 3 * A));
        // Two units free at the end, none before the oldest: too short for three.
        assert_eq!(ring.take(3 * A), None);
        let c = ring.take(2 * A).unwrap();
        assert_eq!(c.start, 6 * A);
        // The second lands first: its room waits for the first.
        ring.give_back(b);
        assert_eq!(ring.take(A), None);
        ring.give_back(a);
        // Wrapped: the first six units are free again.
        let d = ring.take(6 * A).unwrap();
        assert_eq!(d.start, 0);
        assert_eq!(ring.take(A), None);
        ring.give_back(c);
        ring.give_back(d);
        assert_eq!(ring.take(8 * A).map(|l| l.start), Some(0));
    }

    /// Random leases and returns against a model of which units are in use: none is leased twice.
    #[test]
    fn random_leases_match_a_model_of_the_units_in_use() {
        let units = 16;
        let ring = Staging::new(units * A);
        let mut used = vec![false; units];
        let mut held: VecDeque<Lease> = VecDeque::new();
        let mut state = 7u64;
        let mut next = |n: usize| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as usize % n
        };
        for _ in 0..20_000 {
            if !held.is_empty() && next(2) == 0 {
                let lease = held.remove(next(held.len())).unwrap();
                for u in lease.start / A..(lease.start + lease.len) / A {
                    used[u] = false;
                }
                ring.give_back(lease);
                continue;
            }
            let len = (1 + next(units / 2)) * A;
            if let Some(lease) = ring.take(len) {
                for u in lease.start / A..(lease.start + lease.len) / A {
                    assert!(!used[u], "unit {u} leased twice");
                    used[u] = true;
                }
                held.push_back(lease);
            }
        }
    }
}
