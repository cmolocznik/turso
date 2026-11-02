//! The pages provisional, without a lock: per class, which entry a page's is, and per entry the
//! page, where the group that made it provisional starts, and where the record that settled it
//! (adopted or freed it) ends, once one did.
//!
//! An entry changes hands only by compare-and-swap of its settled word. Making a page provisional
//! again first takes the word back from settled to unsettled, so a checkpoint that read the entry
//! settled and durable and is removing it cannot take it from under the page: whichever exchange
//! comes first wins, and the loser starts over. A removal marks the word gone before it unlinks
//! the entry, so a page that finds its entry going unlinks it too and takes a new one. Every call
//! runs in a step, and an entry given back is handed out again only once no step that could hold
//! its number is left: so an entry a call read is never another page's by the time it exchanges.

use self::sync::{AtomicU64, Ordering};
// The engine's: what the store hands in, under loom too.
use super::super::epoch::{Epochs, Retired};
use super::super::log::Lsn;
use super::super::page::{PageId, CLASS_COUNT};
use super::super::segmented::Segmented;
use super::super::stack::FreeList;
use crate::sync::Arc;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicU64, Ordering};
}

/// An entry's settled word while it is being removed.
const GONE: u64 = u64::MAX;

#[derive(Default)]
struct Entry {
    pid: AtomicU64,
    /// Where its group starts, plus one; 0 for a free entry.
    start: AtomicU64,
    /// Where the record that settled it ends, plus one; 0 while unsettled; [`GONE`].
    settled: AtomicU64,
}

pub struct Provisionals {
    /// Per class, by page number: its entry plus one, 0 for none.
    by_page: [Segmented<AtomicU64>; CLASS_COUNT],
    entries: Segmented<Entry>,
    /// Entries handed out so far: a scan reads no further.
    used: AtomicU64,
    free: FreeList<u64>,
    /// Entries given back, not reusable until no step that could hold their number is left.
    retired: Retired<u64>,
    epochs: Arc<Epochs>,
}

impl Provisionals {
    pub fn new(epochs: Arc<Epochs>) -> Self {
        Self {
            by_page: Default::default(),
            entries: Segmented::default(),
            used: AtomicU64::new(0),
            free: FreeList::default(),
            retired: Retired::default(),
            epochs,
        }
    }

    fn link(&self, pid: PageId) -> &AtomicU64 {
        self.by_page[pid.class().index()].get_or_make(pid.number())
    }

    /// Makes `pid` provisional from `start`; a page provisional already keeps the earlier start,
    /// and is unsettled again.
    pub fn made(&self, pid: PageId, start: Lsn) {
        let step = self.epochs.enter();
        let link = self.link(pid);
        loop {
            let linked = link.load(Ordering::Acquire);
            if linked != 0 {
                let e = self.entries.get_or_make(linked - 1);
                let settled = e.settled.load(Ordering::Acquire);
                if settled == GONE {
                    // Being removed: unlink it, as its remover will, and take a new one.
                    let _ = link.compare_exchange(linked, 0, Ordering::AcqRel, Ordering::Acquire);
                    continue;
                }
                // Taken back first: then no removal can take it from under the page.
                if e.settled
                    .compare_exchange(settled, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }
                e.start.fetch_min(start + 1, Ordering::AcqRel);
                return;
            }
            let i = self.claim(&step);
            let e = self.entries.get_or_make(i);
            e.pid.store(pid.raw(), Ordering::Relaxed);
            e.settled.store(0, Ordering::Relaxed);
            // Release: a scan or a page that reads the entry sees it whole.
            e.start.store(start + 1, Ordering::Release);
            if link
                .compare_exchange(0, i + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
            self.give_back(i);
        }
    }

    /// Settles `pid` with the record ending at `lsn`, if it is provisional.
    pub fn settled(&self, pid: PageId, lsn: Lsn) {
        let _step = self.epochs.enter();
        let linked = self.link(pid).load(Ordering::Acquire);
        if linked == 0 {
            return;
        }
        let e = self.entries.get_or_make(linked - 1);
        let mut settled = e.settled.load(Ordering::Acquire);
        while settled != GONE {
            match e.settled.compare_exchange_weak(
                settled,
                lsn + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(now) => settled = now,
            }
        }
    }

    /// Forgets `pid`: for recovery, which replays alone.
    pub fn remove(&self, pid: PageId) {
        let linked = self.link(pid).swap(0, Ordering::AcqRel);
        if linked != 0 {
            let e = self.entries.get_or_make(linked - 1);
            e.settled.store(GONE, Ordering::Release);
            self.give_back(linked - 1);
        }
    }

    /// The earliest start of a page provisional or settled by a record not yet durable at
    /// `durable`; the others are removed on the way.
    pub fn least(&self, durable: Lsn) -> Option<Lsn> {
        let _step = self.epochs.enter();
        let mut least: Option<Lsn> = None;
        for i in 0..self.used.load(Ordering::Acquire) {
            let Some(e) = self.entries.get(i) else {
                continue;
            };
            let start = e.start.load(Ordering::Acquire);
            if start == 0 {
                continue;
            }
            let settled = e.settled.load(Ordering::Acquire);
            if settled != 0 && settled != GONE && settled - 1 <= durable {
                if e.settled
                    .compare_exchange(settled, GONE, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    let pid = PageId::from_raw(e.pid.load(Ordering::Relaxed));
                    let _ = self.link(pid).compare_exchange(
                        i + 1,
                        0,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    self.give_back(i);
                    continue;
                }
            }
            if settled != GONE {
                least = Some(least.map_or(start - 1, |l| l.min(start - 1)));
            }
        }
        least
    }

    /// Every page provisional and not settled.
    pub fn unsettled(&self) -> Vec<PageId> {
        let _step = self.epochs.enter();
        (0..self.used.load(Ordering::Acquire))
            .filter_map(|i| self.entries.get(i))
            .filter(|e| {
                e.start.load(Ordering::Acquire) != 0 && e.settled.load(Ordering::Acquire) == 0
            })
            .map(|e| PageId::from_raw(e.pid.load(Ordering::Acquire)))
            .collect()
    }

    fn claim(&self, step: &super::super::epoch::Step) -> u64 {
        for i in self.retired.reclaim(&self.epochs) {
            self.free.push(i);
        }
        self.free
            .pop(&self.epochs, step)
            .unwrap_or_else(|| self.used.fetch_add(1, Ordering::AcqRel))
    }

    fn give_back(&self, i: u64) {
        self.entries
            .get_or_make(i)
            .start
            .store(0, Ordering::Release);
        self.retired.retire(&self.epochs, i);
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::super::super::page::SizeClass;
    use super::*;
    use loom::thread;

    fn check(f: impl Fn() + Sync + Send + 'static) {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(f);
    }

    /// A page settled and durable is made provisional again while a checkpoint removes it: it
    /// stays provisional, from its old start or its new one, never lost.
    #[test]
    fn a_page_made_provisional_again_as_its_entry_goes_stays_provisional() {
        check(|| {
            let p = Arc::new(Provisionals::new(Arc::new(Epochs::new())));
            let pid = PageId::new(SizeClass::new(0).unwrap(), 7);
            p.made(pid, 10);
            p.settled(pid, 20);
            let again = {
                let p = p.clone();
                thread::spawn(move || p.made(pid, 30))
            };
            let _ = p.least(25);
            again.join().unwrap();
            assert_eq!(p.unsettled(), vec![pid]);
            let least = p.least(25);
            assert!(least == Some(10) || least == Some(30), "{least:?}");
            // Its page still leads to it: settled and durable, it goes.
            p.settled(pid, 40);
            assert_eq!(p.least(45), None);
        });
    }

    /// Two pages adopted by one record, the first's entry removed and handed to a third page
    /// while the second is made provisional again: each keeps its own entry.
    #[test]
    fn pages_settled_by_one_record_keep_their_own_entries() {
        check(|| {
            let p = Arc::new(Provisionals::new(Arc::new(Epochs::new())));
            let class = SizeClass::new(0).unwrap();
            let (a, b, c) = (
                PageId::new(class, 1),
                PageId::new(class, 2),
                PageId::new(class, 3),
            );
            p.made(a, 10);
            p.made(b, 10);
            p.settled(a, 20);
            p.settled(b, 20);
            let checkpoint = {
                let p = p.clone();
                thread::spawn(move || {
                    let _ = p.least(25);
                    p.made(c, 40);
                })
            };
            p.made(b, 30);
            checkpoint.join().unwrap();
            let mut unsettled = p.unsettled();
            unsettled.sort();
            assert_eq!(unsettled, vec![b, c]);
        });
    }
}
