//! The writeback buffer: one block of fixed size, allocated once, that copies of pages written
//! back while they stay in their frames are made in. A page written out of its frame needs none:
//! its write-out holds the frame, and it is written from there.
//!
//! The block is a ring. A copy leases the next contiguous run of it, wrapping to the start when
//! the end is too short, and gives it back once its write has landed; the ring's tail moves past
//! leases given back in the order they were taken, so a write landing early holds its room until
//! the older ones land. A lease that does not fit waits for room: the size of the block bounds the
//! writes in flight, and so the share of the device the cleaner takes, and its memory.

use super::super::aligned::AlignedBlock;
use crate::io::Buffer;
use crate::sync::{Arc, Mutex};
use std::collections::VecDeque;

/// Room taken in the ring: where, how long, and whether it was given back.
struct Taken {
    start: usize,
    len: usize,
    returned: bool,
}

struct Ring {
    /// Where the next lease starts.
    head: usize,
    /// Leases in the order taken, the oldest first: the ring is in use from the first's start to
    /// `head`, wrapping.
    taken: VecDeque<Taken>,
}

pub struct Staging {
    block: Arc<AlignedBlock>,
    ring: Mutex<Ring>,
}

/// A run of the ring, its holder's alone until [`Staging::give_back`].
#[derive(Debug, PartialEq, Eq)]
struct Lease {
    start: usize,
    len: usize,
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

impl Staging {
    /// A ring of `len` bytes, a multiple of the direct IO alignment.
    pub fn new(len: usize) -> Self {
        Self {
            block: Arc::new(AlignedBlock::zeroed(len)),
            ring: Mutex::new(Ring {
                head: 0,
                taken: VecDeque::new(),
            }),
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
        let cap = self.block.len();
        debug_assert!(len <= cap);
        let mut ring = self.ring.lock();
        let start = match ring.taken.front().map(|oldest| oldest.start) {
            None => 0,
            // In use from the tail to the head: free past the head, then before the tail.
            Some(tail) if ring.head > tail => {
                if cap - ring.head >= len {
                    ring.head
                } else if tail >= len {
                    0
                } else {
                    return None;
                }
            }
            // Wrapped: free between the head and the tail, none if they meet.
            Some(tail) if tail - ring.head >= len => ring.head,
            Some(_) => return None,
        };
        ring.head = start + len;
        ring.taken.push_back(Taken {
            start,
            len,
            returned: false,
        });
        Some(Lease { start, len })
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
        let mut ring = self.ring.lock();
        let taken = ring
            .taken
            .iter_mut()
            .find(|t| t.start == lease.start && t.len == lease.len && !t.returned)
            .expect("a lease given back once");
        taken.returned = true;
        while ring.taken.front().is_some_and(|t| t.returned) {
            ring.taken.pop_front();
        }
        if ring.taken.is_empty() {
            ring.head = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::DIRECT_IO_ALIGNMENT as A;

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
