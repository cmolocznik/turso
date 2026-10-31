use super::*;
use crate::io::IO;
use crate::storage::atoll::log::{framed_len, Admitted, Header, Kind, Log, LogConfig};
use crate::storage::atoll::page::HEADER_SIZE;
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::{backends, drive, Backend};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// The kind of the records logging test changes; the page store owns kinds 1 to 6.
const CHANGE: Kind = Kind::new(100).unwrap();

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

fn open(b: &Backend, budget: usize) -> (Arc<Log>, Arc<BufferPool>) {
    let log = Log::open(
        b.io.clone(),
        &b.base,
        LogConfig {
            segment_bytes: 4 << 20,
            buffer_bytes: 256 << 10,
            chunk_bytes: 16 << 10,
            writes_in_flight: 3,
            direct: true,
        },
        0,
    )
    .unwrap();
    let store = PageStore::new(
        b.io.clone(),
        &b.base,
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        log.clone(),
        [[1; CLASS_COUNT]; 2],
    );
    let pool = BufferPool::new(
        BufferConfig { budget },
        Arc::new(LocalSource::new(store.clone())),
        store,
    )
    .unwrap();
    (log, pool)
}

/// Writes `version` into `page` beside its id: after the header, and at its end, so a copy torn
/// or mixed up with another page's reads wrong.
fn stamp(page: &mut [u8], pid: PageId, version: u64) {
    page[HEADER_SIZE..HEADER_SIZE + 8].copy_from_slice(&pid.raw().to_le_bytes());
    page[HEADER_SIZE + 8..HEADER_SIZE + 16].copy_from_slice(&version.to_le_bytes());
    let end = page.len();
    page[end - 8..].copy_from_slice(&version.to_le_bytes());
}

/// The version stamped in `page`, if it is stamped as `pid` and whole.
fn stamped(page: &[u8], pid: PageId) -> Option<u64> {
    let word = |at: usize| {
        page.get(at..at + 8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
    };
    let version = word(HEADER_SIZE + 8)?;
    (word(HEADER_SIZE)? == pid.raw() && word(page.len() - 8)? == version).then_some(version)
}

/// One page of the model: its id, the version it should hold and the hint kept beside it.
struct Page {
    pid: PageId,
    version: u64,
    hint: AtomicU32,
}

struct Model<'a> {
    io: &'a Arc<dyn IO>,
    log: &'a Arc<Log>,
    pool: &'a Arc<BufferPool>,
    pages: Vec<Page>,
}

impl Model<'_> {
    /// Logs a change to `pid` before latching it, as every change is admitted.
    fn change(&self, page: &mut [u8], pid: PageId, version: u64, admitted: &mut Admitted) -> Lsn {
        stamp(page, pid, version);
        let payload = version.to_le_bytes();
        let header = Header {
            kind: CHANGE,
            flags: 0,
            txn: 0,
            prev: 0,
        };
        self.log
            .append(admitted, &header, &[&pid.raw().to_le_bytes(), &payload])
    }

    fn admit(&self) -> Admitted {
        drive(self.io, || self.log.admit(framed_len(16))).unwrap()
    }

    fn allocate(&mut self, class: SizeClass) {
        let mut admitted = self.admit();
        let mut g = drive(self.io, || self.pool.allocate(class)).unwrap();
        let pid = g.pid();
        let lsn = self.change(g.page_mut(), pid, 1, &mut admitted);
        g.changed(lsn);
        let hint = AtomicU32::new(g.hint());
        drop(g);
        self.pages.push(Page {
            pid,
            version: 1,
            hint,
        });
    }

    fn update(&mut self, i: usize) {
        let mut admitted = self.admit();
        let (pid, version) = (self.pages[i].pid, self.pages[i].version + 1);
        let mut g = drive(self.io, || {
            self.pool.fix_exclusive(pid, Some(&self.pages[i].hint))
        })
        .unwrap();
        assert_eq!(
            stamped(g.page(), pid),
            Some(version - 1),
            "{pid:?} before an update"
        );
        let lsn = self.change(g.page_mut(), pid, version, &mut admitted);
        g.changed(lsn);
        drop(g);
        self.pages[i].version = version;
    }

    fn check(&self, i: usize, optimistic: bool) {
        let p = &self.pages[i];
        let seen = if optimistic {
            loop {
                let g = drive(self.io, || self.pool.fix_optimistic(p.pid, Some(&p.hint))).unwrap();
                if let Some(seen) = g.read(|b| stamped(b, p.pid)) {
                    break seen;
                }
            }
        } else {
            let g = drive(self.io, || self.pool.fix_shared(p.pid, Some(&p.hint))).unwrap();
            stamped(g.page(), p.pid)
        };
        assert_eq!(seen, Some(p.version), "{:?} read back wrong", p.pid);
    }

    fn churn(&mut self, rng: &mut ChaCha8Rng, ops: usize) {
        for _ in 0..ops {
            let i = rng.random_range(0..self.pages.len());
            match rng.random_range(0..4) {
                0 | 1 => self.update(i),
                2 => self.check(i, false),
                _ => self.check(i, true),
            }
        }
    }

    fn check_all(&self) {
        for i in 0..self.pages.len() {
            self.check(i, i % 2 == 0);
        }
    }
}

#[test]
fn pages_keep_their_contents_through_eviction_writeback_and_moves_between_classes() {
    for b in backends() {
        let (log, pool) = open(&b, MIN_BUDGET);
        let mut model = Model {
            io: &b.io,
            log: &log,
            pool: &pool,
            pages: Vec::new(),
        };
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        // 600 pages of 32 KiB: nearly twice the pool, all of it taken by class 3.
        for _ in 0..600 {
            model.allocate(class(3));
        }
        model.churn(&mut rng, 2000);
        assert_eq!(
            pool.held()[3],
            MIN_BUDGET,
            "{}: class 3 holds the pool",
            b.name
        );
        let overflow = 600 * class(3).size() as u64 - MIN_BUDGET as u64;
        assert!(
            pool.evicted.load(Ordering::Relaxed) >= overflow,
            "{}: evicted {} bytes, less than the {overflow} that cannot fit",
            b.name,
            pool.evicted.load(Ordering::Relaxed)
        );
        // 3000 pages of 4 KiB, used as often each as the large ones: per byte they are worth
        // more, so class 0 takes memory from class 3.
        for _ in 0..3000 {
            model.allocate(class(0));
        }
        model.churn(&mut rng, 6000);
        let held = pool.held();
        assert!(
            held[0] > held[3],
            "{}: class 0 holds {} bytes, class 3 {}",
            b.name,
            held[0],
            held[3]
        );
        model.check_all();
        // A pass over the whole pool leaves no page dirty.
        let mut pass = pool.clean(0..pool.memory.grains());
        drive(&b.io, || pass.step(&pool)).unwrap();
        // Writebacks begun by eviction may still be in flight.
        drive(&b.io, || pool.settle_writes()).unwrap();
        let dirty = (0..pool.memory.grains())
            .filter(|&f| pool.header(f).status() == RESIDENT && pool.header(f).dirty())
            .count();
        assert_eq!(dirty, 0, "{}: pages left dirty by a clean pass", b.name);
        // Dropped pages free their frames; the rest still read back.
        let kept = model.pages.split_off(model.pages.len() / 2);
        for p in std::mem::replace(&mut model.pages, kept) {
            drive(&b.io, || {
                match pool.fix_exclusive(p.pid, Some(&p.hint))? {
                    IOResult::Done(page) => pool.free(page),
                    IOResult::IO(io) => Ok(IOResult::IO(io)),
                }
            })
            .unwrap();
        }
        model.check_all();
    }
}

#[test]
fn concurrent_fixes_lose_no_change() {
    const THREADS: usize = 4;
    const OPS: usize = 3000;
    for b in backends() {
        let (log, pool) = open(&b, MIN_BUDGET);
        let mut model = Model {
            io: &b.io,
            log: &log,
            pool: &pool,
            pages: Vec::new(),
        };
        // More 4 KiB pages than the pool's 2,560 frames.
        for _ in 0..4000 {
            model.allocate(class(0));
        }
        let pages = &model.pages;
        let counts: Vec<Vec<u64>> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..THREADS)
                .map(|t| {
                    let model = &model;
                    s.spawn(move || {
                        let mut rng = ChaCha8Rng::seed_from_u64(t as u64);
                        let mut counts = vec![0u64; pages.len()];
                        for _ in 0..OPS {
                            let i = rng.random_range(0..pages.len());
                            let p = &pages[i];
                            if rng.random_bool(0.5) {
                                let mut admitted = model.admit();
                                let mut g = drive(model.io, || {
                                    model.pool.fix_exclusive(p.pid, Some(&p.hint))
                                })
                                .unwrap();
                                let n = stamped(g.page(), p.pid).expect("a whole page") + 1;
                                let lsn = model.change(g.page_mut(), p.pid, n, &mut admitted);
                                g.changed(lsn);
                                counts[i] += 1;
                            } else {
                                loop {
                                    let g = drive(model.io, || {
                                        model.pool.fix_optimistic(p.pid, Some(&p.hint))
                                    })
                                    .unwrap();
                                    if let Some(seen) = g.read(|b| stamped(b, p.pid)) {
                                        assert!(seen.is_some(), "{:?} read torn", p.pid);
                                        break;
                                    }
                                }
                            }
                        }
                        counts
                    })
                })
                .collect();
            workers.into_iter().map(|w| w.join().unwrap()).collect()
        });
        for (i, p) in model.pages.iter_mut().enumerate() {
            p.version = 1 + counts.iter().map(|c| c[i]).sum::<u64>();
        }
        model.check_all();
    }
}
