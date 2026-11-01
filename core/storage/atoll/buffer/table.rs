//! The one translation from page id to frame, for pages resident or loading.
//!
//! A bucket holds the first frame of its chain, plus one; each frame header links the next frame.
//! Inserts and removals hold the bucket's mutex, for a few instructions and never across anything
//! else, so it is a leaf in the lock order. Lookups take nothing. A frame can leave its chain and
//! join another while a lookup walks it, so what a lookup returns is checked by the caller against
//! the frame's latch and page id. A walk that finds nothing may have missed a page, so whoever acts
//! on an absence asks again under the mutex ([`Table::insert`] does). A page never moves between
//! frames: it leaves one only by eviction.
//!
//! At one bucket per 4 KiB frame the table costs 8 bytes per 4 KiB of pool, 0.2%.

use super::super::page::PageId;
use super::frame::FrameHeader;
use super::memory::{Zeroable, Zeroed};
use crate::sync::atomic::{AtomicU32, Ordering};
use crate::sync::Mutex;
use crate::Result;

struct Bucket {
    /// The chain's first frame, plus one; 0 for none.
    head: AtomicU32,
    changing: Mutex<()>,
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            head: AtomicU32::new(0),
            changing: Mutex::new(()),
        }
    }
}

// SAFETY: all zeros is a head of 0 and an unlocked mutex (parking_lot's state 0).
unsafe impl Zeroable for Bucket {}

pub struct Table {
    buckets: Zeroed<Bucket>,
    mask: u64,
}

impl Table {
    /// A table for `frames` frames: at least one bucket per frame.
    pub fn new(frames: u32) -> Result<Self> {
        let n = (frames as usize).max(1).next_power_of_two();
        Ok(Self {
            buckets: Zeroed::new(n)?,
            mask: n as u64 - 1,
        })
    }

    fn bucket(&self, pid: u64) -> &Bucket {
        &self.buckets[(PageId::from_raw(pid).hash() & self.mask) as usize]
    }

    /// The frame holding `pid`, unchecked. May miss it while chains change under the walk.
    pub fn find(&self, headers: &[FrameHeader], pid: u64) -> Option<u32> {
        let bucket = self.bucket(pid);
        let mut at = bucket.head.load(Ordering::Acquire);
        // A chain is never longer than the frames; a walk that is has crossed into chains that
        // changed under it.
        for _ in 0..headers.len() {
            if at == 0 {
                return None;
            }
            let h = &headers[at as usize - 1];
            if h.pid() == pid {
                return Some(at - 1);
            }
            at = h.next.load(Ordering::Acquire);
        }
        let _changing = bucket.changing.lock();
        Self::walk(headers, bucket.head.load(Ordering::Acquire), pid)
    }

    fn walk(headers: &[FrameHeader], head: u32, pid: u64) -> Option<u32> {
        let mut at = head;
        while at != 0 {
            let h = &headers[at as usize - 1];
            if h.pid() == pid {
                return Some(at - 1);
            }
            at = h.next.load(Ordering::Acquire);
        }
        None
    }

    /// Links `frame`, which already names `pid`, unless a frame holds `pid` already: then that
    /// frame.
    pub fn insert(&self, headers: &[FrameHeader], pid: u64, frame: u32) -> Result<(), u32> {
        let bucket = self.bucket(pid);
        let _changing = bucket.changing.lock();
        let head = bucket.head.load(Ordering::Acquire);
        if let Some(existing) = Self::walk(headers, head, pid) {
            return Err(existing);
        }
        headers[frame as usize].next.store(head, Ordering::Relaxed);
        bucket.head.store(frame + 1, Ordering::Release);
        Ok(())
    }

    /// Unlinks `frame`, which holds `pid`.
    pub fn remove(&self, headers: &[FrameHeader], pid: u64, frame: u32) {
        let bucket = self.bucket(pid);
        let _changing = bucket.changing.lock();
        let mut head = bucket.head.load(Ordering::Acquire);
        let with = headers[frame as usize].next.load(Ordering::Relaxed);
        if head == frame + 1 {
            head = with;
        } else {
            let mut at = head;
            loop {
                assert_ne!(at, 0, "frame {frame} is not in its page's chain");
                let h = &headers[at as usize - 1];
                if h.next.load(Ordering::Relaxed) == frame + 1 {
                    h.next.store(with, Ordering::Release);
                    break;
                }
                at = h.next.load(Ordering::Relaxed);
            }
        }
        bucket.head.store(head, Ordering::Release);
    }
}
