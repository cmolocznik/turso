//! Frames lent beside the page table (one memory, Clay's ruling of 2026-11-05): a kept copy, a
//! checkpoint's image of a node page and the log's ring take frames of the pool's region, so the
//! engine's memory is that region, and what it does not lend caches pages.
//!
//! A lent frame is [`LENT`](super::frame::LENT): in no hash chain, holding no page, so nothing
//! evicts or moves it, a hint to it fails, and a batch holding one drains only once it is given
//! back. Only a quiet frame is lent, as its holder writes it plainly. It is given back when its
//! block drops, from whatever thread or IO callback drops it, as a writeback's end frees its
//! page's frame from its callback.

use super::memory::Region;
use super::BufferPool;
use crate::storage::atoll::aligned::Lender;
use crate::storage::atoll::once::SetOnce;
use crate::storage::atoll::page::SizeClass;
use crate::sync::atomic::{AtomicUsize, Ordering};
use crate::sync::{Arc, Weak};

/// What a lent frame holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Use {
    /// A page as a checkpoint's cut found it.
    Kept,
    /// A checkpoint's image of a page, until its write lands.
    Image,
    /// A chunk of the log's ring, for as long as the log is open.
    Log,
}

const USES: usize = 3;

impl Use {
    #[cfg(test)]
    const ALL: [Use; USES] = [Use::Kept, Use::Image, Use::Log];

    fn index(self) -> usize {
        self as usize
    }
}

/// The bytes lent for each use, now and at most since the last reset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LentStats {
    pub kept: (usize, usize),
    pub image: (usize, usize),
    pub log: (usize, usize),
}

pub(super) struct Lends {
    /// Held so the region stays mapped while any lent block lives, the pool gone or not.
    _region: Arc<Region>,
    /// The pool a block given back returns its frame to; once the pool is gone, the frame goes
    /// with the region.
    pool: SetOnce<Weak<BufferPool>>,
    now: [AtomicUsize; USES],
    peak: [AtomicUsize; USES],
}

impl Lends {
    pub(super) fn new(region: Arc<Region>) -> Self {
        Self {
            _region: region,
            pool: SetOnce::default(),
            now: Default::default(),
            peak: Default::default(),
        }
    }

    pub(super) fn set_pool(&self, pool: &Arc<BufferPool>) {
        self.pool.set(Arc::downgrade(pool));
    }

    /// The token `frame`, of `class`, is lent under for `what`.
    pub(super) fn lent(&self, frame: u32, class: SizeClass, what: Use) -> u64 {
        let i = what.index();
        let now = self.now[i].fetch_add(class.size(), Ordering::AcqRel) + class.size();
        self.peak[i].fetch_max(now, Ordering::AcqRel);
        frame as u64 | (class.index() as u64) << 32 | (i as u64) << 40
    }

    #[cfg(test)]
    pub(super) fn stats(&self) -> LentStats {
        let at = |what: Use| {
            (
                self.now[what.index()].load(Ordering::Acquire),
                self.peak[what.index()].load(Ordering::Acquire),
            )
        };
        LentStats {
            kept: at(Use::Kept),
            image: at(Use::Image),
            log: at(Use::Log),
        }
    }

    /// Starts each peak again from what is lent now, for a measurement.
    #[cfg(test)]
    pub(super) fn reset_peaks(&self) {
        for what in Use::ALL {
            let i = what.index();
            self.peak[i].store(self.now[i].load(Ordering::Acquire), Ordering::Release);
        }
    }
}

impl Lender for Lends {
    fn give_back(&self, token: u64) {
        let frame = token as u32;
        let class = SizeClass::new((token >> 32) as u8 as usize).expect("a lent frame's class");
        let i = (token >> 40) as usize;
        self.now[i].fetch_sub(class.size(), Ordering::AcqRel);
        if let Some(pool) = self.pool.get_ref().and_then(Weak::upgrade) {
            pool.take_back(class, frame);
        }
    }
}
