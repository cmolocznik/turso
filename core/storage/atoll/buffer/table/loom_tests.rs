//! The page table under loom: every interleaving and C11 reordering of a few inserts, removals
//! and walks, with frames moving between chains as eviction moves them.
//!
//! Run with `RUSTFLAGS="--cfg atoll_loom" cargo test -p turso_core --lib --release table::loom`.

use super::*;
use loom::sync::atomic::AtomicU64;
use loom::sync::Arc;
use loom::thread;

/// Four frames: their links, and the pages they hold.
struct Frames {
    links: Vec<Link>,
    pids: Vec<AtomicU64>,
}

impl Frames {
    fn new() -> Self {
        Self {
            links: (0..4).map(|_| Link::new(0)).collect(),
            pids: (0..4).map(|_| AtomicU64::new(u64::MAX)).collect(),
        }
    }

    /// Names `pid` in `frame`, as a load or a new page does before linking it.
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

/// The first `n` page ids whose chain is `bucket` of a table of four.
fn pids_in(bucket: u64, n: usize) -> Vec<u64> {
    (1u64..)
        .filter(|&p| PageId::from_raw(p).hash() & 3 == bucket)
        .take(n)
        .collect()
}

/// The frames linked in `pid`'s chain, once nothing changes it.
fn chain(table: &Table, frames: &Frames, pid: u64) -> Vec<u32> {
    let l = table.layout;
    let mut out = Vec::new();
    let mut at = table.head(pid).load(Ordering::Acquire);
    while let Some(f) = l.frame(at) {
        out.push(f);
        at = frames.link(f).load(Ordering::Acquire);
    }
    out
}

fn check(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(f);
}

/// Two loads of one page into two frames: exactly one links its frame, and the other is told
/// which.
#[test]
fn one_of_two_inserts_of_a_page_wins() {
    check(|| {
        let table = Arc::new(Table::new(4).unwrap());
        let frames = Arc::new(Frames::new());
        let a = pids_in(0, 1)[0];
        let loads: Vec<_> = (0..2u32)
            .map(|f| {
                let (table, frames) = (table.clone(), frames.clone());
                thread::spawn(move || {
                    frames.name(f, a);
                    table.insert(&*frames, a, f)
                })
            })
            .collect();
        let got: Vec<_> = loads.into_iter().map(|t| t.join().unwrap()).collect();
        let won: Vec<u32> = (0..2u32).filter(|&f| got[f as usize].is_ok()).collect();
        assert_eq!(won.len(), 1, "inserts {got:?}");
        assert_eq!(got[1 - won[0] as usize], Err(won[0]));
        assert_eq!(table.find(&*frames, a), Some(won[0]));
        assert_eq!(chain(&table, &frames, a), vec![won[0]]);
    });
}

/// A frame evicted from one chain and loaded into another while a walk passes it: the walk still
/// finds the page after it.
#[test]
fn a_walk_never_follows_a_frame_into_the_chain_it_moved_to() {
    check(|| {
        let table = Arc::new(Table::new(4).unwrap());
        let frames = Arc::new(Frames::new());
        let (zero, one) = (pids_in(0, 2), pids_in(1, 2));
        let (a, b, c, d) = (zero[0], zero[1], one[0], one[1]);
        // Chain 0: frame 0 (a), then frame 1 (b). Chain 1: frame 2 (d).
        for (f, p) in [(1, b), (0, a), (2, d)] {
            frames.name(f, p);
            table.insert(&*frames, p, f).unwrap();
        }
        let mover = {
            let (table, frames) = (table.clone(), frames.clone());
            thread::spawn(move || {
                table.remove(&*frames, a, 0);
                frames.name(0, c);
                table.insert(&*frames, c, 0).unwrap();
            })
        };
        assert_eq!(table.find(&*frames, b), Some(1), "page b missed");
        mover.join().unwrap();
        assert_eq!(chain(&table, &frames, b), vec![1]);
        assert_eq!(chain(&table, &frames, c), vec![0, 2]);
    });
}

/// Two neighbours removed at once: both leave, and the frame after them stays linked and found.
#[test]
fn two_frames_removed_at_once_leave_the_rest_linked() {
    check(|| {
        let table = Arc::new(Table::new(4).unwrap());
        let frames = Arc::new(Frames::new());
        let p = pids_in(0, 3);
        // Chain: frame 0 (p0), frame 1 (p1), frame 2 (p2).
        for f in [2u32, 1, 0] {
            frames.name(f, p[f as usize]);
            table.insert(&*frames, p[f as usize], f).unwrap();
        }
        let removers: Vec<_> = (0..2u32)
            .map(|f| {
                let (table, frames, pid) = (table.clone(), frames.clone(), p[f as usize]);
                thread::spawn(move || table.remove(&*frames, pid, f))
            })
            .collect();
        assert_eq!(
            table.find(&*frames, p[2]),
            Some(2),
            "the frame after them missed"
        );
        for r in removers {
            r.join().unwrap();
        }
        assert_eq!(chain(&table, &frames, p[2]), vec![2]);
    });
}

/// A page evicted and loaded again into another frame while a second load of it races: it ends
/// in one frame at most.
#[test]
fn a_page_loaded_again_while_another_load_races_is_in_one_frame() {
    check(|| {
        let table = Arc::new(Table::new(4).unwrap());
        let frames = Arc::new(Frames::new());
        let a = pids_in(0, 1)[0];
        frames.name(0, a);
        table.insert(&*frames, a, 0).unwrap();
        let reload = {
            let (table, frames) = (table.clone(), frames.clone());
            thread::spawn(move || {
                table.remove(&*frames, a, 0);
                frames.name(1, a);
                table.insert(&*frames, a, 1)
            })
        };
        frames.name(2, a);
        let raced = table.insert(&*frames, a, 2);
        let reloaded = reload.join().unwrap();
        let linked = chain(&table, &frames, a);
        let holding: Vec<u32> = linked
            .iter()
            .copied()
            .filter(|&f| frames.pid(f) == a)
            .collect();
        assert_eq!(
            holding.len(),
            1,
            "page in {holding:?}: {reloaded:?} {raced:?}"
        );
        assert!(reloaded.is_ok() || raced.is_ok());
    });
}
