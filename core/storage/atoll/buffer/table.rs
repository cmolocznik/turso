//! The one translation from page id to frame, for pages resident or loading.
//!
//! A bucket's word holds the first frame of its chain, plus one, and a lock bit; each frame header
//! links the next frame. Inserts and removals take the bucket's lock, held for a few instructions
//! and never across anything else. Lookups take no lock. A frame can leave its chain and join
//! another while a lookup walks it, so what a lookup returns is checked by the caller against the
//! frame's latch and page id. A walk that finds nothing may have missed a page, so whoever acts on
//! an absence asks again under the lock ([`Table::insert`] does).
//!
//! At one bucket per 4 KiB frame the table costs 8 bytes per 4 KiB of pool, 0.2%.

use super::super::page::PageId;
use super::frame::FrameHeader;
use super::memory::Zeroed;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::Result;

const LOCK: u64 = 1 << 32;
const HEAD: u64 = LOCK - 1;

pub struct Table {
    buckets: Zeroed<AtomicU64>,
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

    fn bucket(&self, pid: u64) -> &AtomicU64 {
        &self.buckets[(PageId::from_raw(pid).hash() & self.mask) as usize]
    }

    /// The frame holding `pid`, unchecked. May miss a page moving between frames.
    pub fn find(&self, headers: &[FrameHeader], pid: u64) -> Option<u32> {
        let bucket = self.bucket(pid);
        let mut at = (bucket.load(Ordering::Acquire) & HEAD) as u32;
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
        let head = self.lock(bucket);
        let found = Self::walk(headers, head, pid);
        self.unlock(bucket, head);
        found
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

    fn lock(&self, bucket: &AtomicU64) -> u32 {
        let mut spins = 0u32;
        loop {
            let word = bucket.load(Ordering::Relaxed);
            if word & LOCK == 0
                && bucket
                    .compare_exchange_weak(word, word | LOCK, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                return (word & HEAD) as u32;
            }
            // The holder runs a few instructions; only an OS preemption makes this wait long.
            spins += 1;
            if spins.is_multiple_of(64) {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
    }

    fn unlock(&self, bucket: &AtomicU64, head: u32) {
        bucket.store(head as u64, Ordering::Release);
    }

    /// Links `frame`, which already names `pid`, unless a frame holds `pid` already: then that
    /// frame.
    pub fn insert(&self, headers: &[FrameHeader], pid: u64, frame: u32) -> Result<(), u32> {
        let bucket = self.bucket(pid);
        let head = self.lock(bucket);
        if let Some(existing) = Self::walk(headers, head, pid) {
            self.unlock(bucket, head);
            return Err(existing);
        }
        headers[frame as usize].next.store(head, Ordering::Relaxed);
        self.unlock(bucket, frame + 1);
        Ok(())
    }

    /// Unlinks `frame`, which holds `pid`.
    pub fn remove(&self, headers: &[FrameHeader], pid: u64, frame: u32) {
        self.relink(headers, pid, frame, None);
    }

    /// Puts `to`, which already names `pid`, in `from`'s place.
    pub fn replace(&self, headers: &[FrameHeader], pid: u64, from: u32, to: u32) {
        self.relink(headers, pid, from, Some(to));
    }

    fn relink(&self, headers: &[FrameHeader], pid: u64, frame: u32, to: Option<u32>) {
        let bucket = self.bucket(pid);
        let mut head = self.lock(bucket);
        let after = headers[frame as usize].next.load(Ordering::Relaxed);
        let with = match to {
            Some(to) => {
                headers[to as usize].next.store(after, Ordering::Relaxed);
                to + 1
            }
            None => after,
        };
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
        self.unlock(bucket, head);
    }
}
