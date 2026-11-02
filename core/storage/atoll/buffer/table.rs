//! The one translation from page id to frame, for pages resident or loading.
//!
//! Lock-free: a bucket's chain is a list of frames linked through their headers, Michael's list
//! (2002) for nodes that are reused but never freed. A link word holds the next frame plus one, a
//! mark, and a tag.
//!
//! - Removing a frame marks its own link first, so nothing is linked in after it, then unlinks it
//!   from the word before it. A walk that meets a marked frame unlinks it too.
//! - Every write to a link word raises its tag or sets its mark, so a word read twice and found
//!   equal was not written in between, however often the frames around it changed hands. (A word
//!   comes back to a frame it linked only through both an unlink and an insert, so either raise
//!   alone would do; both are kept so the rule has no exception.)
//! - A walk reads a frame's link and page, then reads again the word that led to the frame. If that
//!   word is unchanged, the frame was in this chain and unmarked when its fields were read, and the
//!   walk moves on; else it starts over. So a walk never follows a frame that left for another
//!   chain, and never misses a page that stays in the table while it walks.
//! - A page goes in only at its chain's head, by a compare-exchange against the head its walk
//!   began from. An insert that walked the whole chain without finding the page and finds the head
//!   unchanged knows no frame holds the page: any other insert would have changed the head.
//! - A frame leaves the table before it is reused: [`Table::remove`] returns once no word links it.
//!
//! The tag takes the bits the frame number leaves: at least 31, at the largest pool, and 39 for a
//! pool of 64 GiB. A walk is fooled only if one word is written that many times between its two
//! reads of it, each write a page loaded into or evicted from that place in the chain.
//!
//! At one bucket per 4 KiB frame the table costs 8 bytes per 4 KiB of pool, 0.2%. Each frame's
//! link is in its header, which it leaves at 104 bytes.

use self::sync::Ordering;
use super::super::page::PageId;
use super::frame::FrameHeader;
use super::memory::Zeroed;
use crate::Result;

/// The engine's atomics, or loom's, which explore the reorderings shuttle cannot.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicU64, Ordering};
}

/// A link word: a bucket's head, or the link in a frame's header to the next frame.
pub type Link = sync::AtomicU64;

/// The frames a table links: their links and the pages they hold.
pub trait Nodes {
    fn len(&self) -> usize;
    fn link(&self, frame: u32) -> &Link;
    fn pid(&self, frame: u32) -> u64;
}

/// The pool's frame headers.
impl Nodes for Zeroed<FrameHeader> {
    fn len(&self) -> usize {
        <[FrameHeader]>::len(self)
    }

    fn link(&self, frame: u32) -> &Link {
        &self[frame as usize].next
    }

    fn pid(&self, frame: u32) -> u64 {
        self[frame as usize].pid()
    }
}

/// Where a link word keeps its frame, mark and tag: the frame plus one in the low bits, as many
/// as the frame numbers need, the mark above it, the tag above that.
#[derive(Clone, Copy)]
struct Layout {
    bits: u32,
}

impl Layout {
    fn for_frames(frames: u32) -> Self {
        Self {
            bits: u32::BITS - frames.leading_zeros(),
        }
    }

    fn frame(self, word: u64) -> Option<u32> {
        let f = word & ((1 << self.bits) - 1);
        (f != 0).then(|| f as u32 - 1)
    }

    fn marked(self, word: u64) -> bool {
        word >> self.bits & 1 != 0
    }

    fn tag(self, word: u64) -> u64 {
        word >> (self.bits + 1)
    }

    /// An unmarked word linking `frame`, with `tag`, wrapping past the highest.
    fn link(self, frame: Option<u32>, tag: u64) -> u64 {
        tag << (self.bits + 1) | frame.map_or(0, |f| f as u64 + 1)
    }

    fn marking(self, word: u64) -> u64 {
        word | 1 << self.bits
    }
}

#[cfg(not(atoll_loom))]
type Heads = Zeroed<Link>;
#[cfg(atoll_loom)]
type Heads = Box<[Link]>;

pub struct Table {
    heads: Heads,
    mask: u64,
    layout: Layout,
}

impl Table {
    /// A table for `frames` frames: at least one bucket per frame.
    pub fn new(frames: u32) -> Result<Self> {
        let n = (frames as usize).max(1).next_power_of_two();
        #[cfg(not(atoll_loom))]
        let heads = Heads::new(n)?;
        #[cfg(atoll_loom)]
        let heads = (0..n).map(|_| Link::new(0)).collect();
        Ok(Self {
            heads,
            mask: n as u64 - 1,
            layout: Layout::for_frames(frames),
        })
    }

    fn head(&self, pid: u64) -> &Link {
        &self.heads[(PageId::from_raw(pid).hash() & self.mask) as usize]
    }

    /// The frame holding `pid`, unchecked: the caller confirms it under the frame's latch.
    pub fn find<N: Nodes + ?Sized>(&self, nodes: &N, pid: u64) -> Option<u32> {
        self.search(nodes, self.head(pid), |f| nodes.pid(f) == pid)
            .1
    }

    /// Links `frame`, which names `pid` already and is in no chain, unless a frame holds `pid`
    /// already: then that frame.
    pub fn insert<N: Nodes + ?Sized>(&self, nodes: &N, pid: u64, frame: u32) -> Result<(), u32> {
        let l = self.layout;
        let head = self.head(pid);
        let link = nodes.link(frame);
        loop {
            let (first, found) = self.search(nodes, head, |f| nodes.pid(f) == pid);
            if let Some(existing) = found {
                return Err(existing);
            }
            // Release: a walk that reads the new link sees the frame's earlier unlinking, and so
            // finds changed the word that led it to the frame's last place.
            let own = link.load(Ordering::Relaxed);
            link.store(l.link(l.frame(first), l.tag(own) + 1), Ordering::Release);
            // Release: a walk that reads the frame here sees its page and link.
            if head
                .compare_exchange(
                    first,
                    l.link(Some(frame), l.tag(first) + 1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Unlinks `frame`, which holds `pid`; once this returns, no word links it.
    pub fn remove<N: Nodes + ?Sized>(&self, nodes: &N, pid: u64, frame: u32) {
        let l = self.layout;
        let link = nodes.link(frame);
        let mut word = link.load(Ordering::Relaxed);
        loop {
            assert!(!l.marked(word), "frame {frame} removed twice");
            // Relaxed: nothing is read through the mark. A walk that meets it unlinks the frame by
            // the word itself, and whoever reuses the frame next is ordered after this by the
            // memory's free and take.
            match link.compare_exchange_weak(
                word,
                l.marking(word),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(now) => word = now,
            }
        }
        // A whole walk unlinks every marked frame it meets; marked, this one is linked anew by
        // no one.
        self.search(nodes, self.head(pid), |_| false);
    }

    /// Walks `head`'s chain, unlinking the marked frames it meets, to the first unmarked frame
    /// `wanted` takes. Returns that frame, if any, and the head word the walk began from.
    fn search<N: Nodes + ?Sized>(
        &self,
        nodes: &N,
        head: &Link,
        wanted: impl Fn(u32) -> bool,
    ) -> (u64, Option<u32>) {
        let l = self.layout;
        'walk: loop {
            let first = head.load(Ordering::Acquire);
            let (mut prev, mut seen) = (head, first);
            // Each pass over a frame moves past it or unlinks it, at most twice per frame.
            for _ in 0..=2 * nodes.len() {
                let Some(cur) = l.frame(seen) else {
                    return (first, None);
                };
                let link = nodes.link(cur);
                let word = link.load(Ordering::Acquire);
                let found = !l.marked(word) && wanted(cur);
                // Unchanged since it led here: `cur` was in this chain, unmarked, when its link
                // and page were read.
                if prev.load(Ordering::Acquire) != seen {
                    continue 'walk;
                }
                if l.marked(word) {
                    let past = l.link(l.frame(word), l.tag(seen) + 1);
                    if prev
                        .compare_exchange(seen, past, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue 'walk;
                    }
                    seen = past;
                } else if found {
                    return (first, Some(cur));
                } else {
                    (prev, seen) = (link, word);
                }
            }
            panic!("a chain longer than twice the frames: the page table is corrupt");
        }
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests;

#[cfg(all(test, atoll_loom))]
mod loom_tests;
