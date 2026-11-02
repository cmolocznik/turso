//! The page table on threads and in shuttle schedules: pages evicted and loaded again across a
//! few chains while lookups of pages that never move run beside them.

use super::*;
use crate::sync::atomic::AtomicU64;
use crate::sync::Arc;

/// Frames for a test: their links, and the pages they hold.
struct Frames {
    links: Vec<Link>,
    pids: Vec<AtomicU64>,
}

impl Frames {
    fn new(n: usize) -> Self {
        Self {
            links: (0..n).map(|_| Link::new(0)).collect(),
            pids: (0..n).map(|_| AtomicU64::new(u64::MAX)).collect(),
        }
    }

    fn name(&self, frame: u32, pid: u64) {
        self.pids[frame as usize].store(pid, Ordering::Release);
    }
}

impl Nodes for Frames {
    fn len(&self) -> usize {
        self.links.len()
    }

    fn link(&self, frame: u32) -> &Link {
        &self.links[frame as usize]
    }

    fn pid(&self, frame: u32) -> u64 {
        self.pids[frame as usize].load(Ordering::Acquire)
    }
}

const FRAMES: u32 = 8;
const MOVERS: u32 = 3;

/// Page ids whose chain is `bucket` in a table of [`FRAMES`] buckets.
fn pids_in(bucket: u64, n: usize) -> Vec<u64> {
    (1u64..)
        .filter(|&p| PageId::from_raw(p).hash() & (FRAMES as u64 - 1) == bucket)
        .take(n)
        .collect()
}

/// Frames 0 and 1 hold a page each, in chains 0 and 1, for good. Each mover owns two frames and
/// loads pages from one shared set into them, chains 0 and 1, and evicts them again; two movers
/// race to load one page. A lookup of a page that stays always finds it; a mover finds what it
/// loaded until it evicts it; no page is ever linked in two frames.
fn churn(rounds: usize) {
    let table = Arc::new(Table::new(FRAMES).unwrap());
    let frames = Arc::new(Frames::new(FRAMES as usize));
    let (zero, one) = (pids_in(0, 4), pids_in(1, 4));
    let stay = [zero[0], one[0]];
    for (f, &p) in stay.iter().enumerate() {
        frames.name(f as u32, p);
        table.insert(&*frames, p, f as u32).unwrap();
    }
    let moving: Arc<Vec<u64>> = Arc::new(zero[1..].iter().chain(&one[1..]).copied().collect());
    let movers: Vec<_> = (0..MOVERS)
        .map(|m| {
            let (table, frames, moving) = (table.clone(), frames.clone(), moving.clone());
            crate::thread::spawn(move || {
                let own = [2 + 2 * m, 3 + 2 * m];
                for round in 0..rounds {
                    let f = own[round % 2];
                    let pid = moving[(round * (m as usize + 1) + m as usize) % moving.len()];
                    frames.name(f, pid);
                    match table.insert(&*frames, pid, f) {
                        Ok(()) => {
                            assert_eq!(table.find(&*frames, pid), Some(f), "own page lost");
                            crate::thread::yield_now();
                            table.remove(&*frames, pid, f);
                        }
                        Err(other) => assert_ne!(other, f, "linked twice"),
                    }
                    frames.name(f, u64::MAX);
                }
            })
        })
        .collect();
    let walker = {
        let (table, frames) = (table.clone(), frames.clone());
        crate::thread::spawn(move || {
            for _ in 0..rounds {
                for (f, &p) in stay.iter().enumerate() {
                    assert_eq!(
                        table.find(&*frames, p),
                        Some(f as u32),
                        "a page that stays missed"
                    );
                }
                crate::thread::yield_now();
            }
        })
    };
    for t in movers {
        t.join().unwrap();
    }
    walker.join().unwrap();
    for bucket in 0..FRAMES as u64 {
        let mut at = table.heads[bucket as usize].load(Ordering::Acquire);
        let mut linked = Vec::new();
        while let Some(f) = table.layout.frame(at) {
            assert!(!table.layout.marked(at));
            linked.push(f);
            at = frames.link(f).load(Ordering::Acquire);
        }
        let expected: Vec<u32> = (0..2).filter(|&f| bucket == f as u64).collect();
        assert_eq!(linked, expected, "chain {bucket} once every mover evicted");
    }
}

#[cfg(not(shuttle))]
#[test]
fn pages_moving_between_chains_never_hide_one_that_stays() {
    churn(200_000);
}

#[cfg(shuttle)]
#[test]
fn pages_moving_between_chains_never_hide_one_that_stays_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(|| churn(20), 2_000);
}

/// A frame evicted from one chain and loaded into another while a walk passes it, in four
/// frames: the walk still finds the page after it.
fn a_walk_finds_the_page_past_a_frame_that_moved(rounds: usize) {
    for _ in 0..rounds {
        let table = Arc::new(Table::new(4).unwrap());
        let frames = Arc::new(Frames::new(4));
        let in_chain = |bucket: u64| -> Vec<u64> {
            (1u64..)
                .filter(|&p| PageId::from_raw(p).hash() & 3 == bucket)
                .take(2)
                .collect()
        };
        let (zero, one) = (in_chain(0), in_chain(1));
        let (a, b, c, d) = (zero[0], zero[1], one[0], one[1]);
        for (f, p) in [(1, b), (0, a), (2, d)] {
            frames.name(f, p);
            table.insert(&*frames, p, f).unwrap();
        }
        let mover = {
            let (table, frames) = (table.clone(), frames.clone());
            crate::thread::spawn(move || {
                table.remove(&*frames, a, 0);
                frames.name(0, c);
                table.insert(&*frames, c, 0).unwrap();
            })
        };
        assert_eq!(table.find(&*frames, b), Some(1), "page b missed");
        mover.join().unwrap();
    }
}

#[cfg(not(shuttle))]
#[test]
fn a_walk_finds_the_page_past_a_frame_that_moved_on_threads() {
    a_walk_finds_the_page_past_a_frame_that_moved(20_000);
}

#[cfg(shuttle)]
#[test]
fn a_walk_finds_the_page_past_a_frame_that_moved_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || a_walk_finds_the_page_past_a_frame_that_moved(1),
        2_000,
    );
}
