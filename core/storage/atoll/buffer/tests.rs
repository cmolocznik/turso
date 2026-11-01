use super::*;
use crate::io::crash::CrashIO;
use crate::io::{OpenFlags, IO};
use crate::storage::atoll::log::{framed_len, Admitted, Header, Kind, Log, LogConfig};
use crate::storage::atoll::page::HEADER_SIZE;
use crate::storage::atoll::shared::{Bytes, BytesMut};
use crate::storage::atoll::store::StoreConfig;
use crate::storage::atoll::testing::{backends, drive, Backend};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;

/// The kind of the records logging test changes; the page store owns kinds 1 to 6.
const CHANGE: Kind = Kind::new(100).unwrap();

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

fn open(b: &Backend, budget: usize) -> (Arc<Log>, Arc<BufferPool>) {
    open_on(&b.io, &b.base, budget)
}

fn open_on(io: &Arc<dyn IO>, base: &str, budget: usize) -> (Arc<Log>, Arc<BufferPool>) {
    open_with(io, base, BufferConfig::with_budget(budget))
}

fn open_with(io: &Arc<dyn IO>, base: &str, config: BufferConfig) -> (Arc<Log>, Arc<BufferPool>) {
    let log = Log::open(
        io.clone(),
        base,
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
        io.clone(),
        base,
        StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        log.clone(),
        [[1; CLASS_COUNT]; 2],
    );
    let pool =
        BufferPool::new(config, Arc::new(LocalSource::new(store.clone())), store, 0).unwrap();
    (log, pool)
}

/// Writes `version` into `page` beside its id: after the header, and at its end, so a copy torn
/// or mixed up with another page's reads wrong.
fn stamp<P: BytesMut + ?Sized>(page: &mut P, pid: PageId, version: u64) {
    page.put_u64(HEADER_SIZE, pid.raw());
    page.put_u64(HEADER_SIZE + 8, version);
    let end = page.len();
    page.put_u64(end - 8, version);
}

/// The version stamped in `page`, if it is stamped as `pid` and whole.
fn stamped<P: Bytes + ?Sized>(page: &P, pid: PageId) -> Option<u64> {
    let version = page.u64_at(HEADER_SIZE + 8);
    (page.u64_at(HEADER_SIZE) == pid.raw() && page.u64_at(page.len() - 8) == version)
        .then_some(version)
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
    fn change<P: BytesMut + ?Sized>(
        &self,
        page: &mut P,
        pid: PageId,
        version: u64,
        admitted: &mut Admitted,
    ) -> Lsn {
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
        let lsn = self.change(&mut g.page_mut(), pid, 1, &mut admitted);
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
        let lsn = self.change(&mut g.page_mut(), pid, version, &mut admitted);
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
            let g = drive(self.io, || self.pool.fix_exclusive(p.pid, Some(&p.hint))).unwrap();
            let seen = stamped(g.page(), p.pid);
            drop(g.release_unchanged());
            seen
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
        // A pass over every shard leaves no page dirty, and the pool's redo floor at the log's
        // end when the passes began.
        let end = log.end();
        for shard in 0..pool.shard_count() {
            let mut pass = pool.pass(shard);
            drive(&b.io, || pass.step(&pool)).unwrap();
        }
        let dirty = (0..pool.memory.grains())
            .filter(|&f| pool.header(f).status() == RESIDENT && pool.header(f).dirty())
            .count();
        assert_eq!(dirty, 0, "{}: pages left dirty by passes", b.name);
        assert_eq!(pool.redo_floor(), end, "{}", b.name);
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
                                let lsn = model.change(&mut g.page_mut(), p.pid, n, &mut admitted);
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

/// A page whose write was in flight when a pass began may have changed since its copy was taken,
/// by a change logged before the pass: the pass waits for the write, and writes the page again.
#[test]
fn a_pass_writes_again_a_page_changed_while_its_earlier_write_was_in_flight() {
    // Writes complete only at a step, so one stays in flight while the page changes.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(3));
    let (log, pool) = open_on(&io, "/db/pass", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        log: &log,
        pool: &pool,
        pages: Vec::new(),
    };
    model.allocate(class(0));
    let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
    // Written back the way eviction does: marked, then stepped until the write is submitted.
    pool.header(frame).flags.fetch_or(WRITING, Ordering::AcqRel);
    let written = pool.write_back(class(0), vec![frame], false).unwrap();
    while let Some(c) = pool.drive_writebacks().unwrap() {
        io.wait_for_completion(c).unwrap();
    }
    assert!(!written.finished(), "the write is in flight");
    let changed = log.end();
    model.update(0);
    let shard = pool.shard_of(frame);
    let mut pass = pool.pass(shard);
    drive(&io, || pass.step(&pool)).unwrap();
    assert!(
        !pool.header(frame).dirty() || pool.shards[shard].position() <= changed,
        "the shard's position passed a change no written copy holds"
    );
    model.check_all();
}

/// A batch drained for another class empties even while a page in it changes faster than it can
/// be written: the page is written out, unavailable until its write lands, rather than written
/// back and kept because it changed meanwhile.
#[test]
fn a_drain_empties_its_batch_while_a_page_in_it_keeps_changing() {
    // Writes complete only at a step, so a page can change while its write is in flight.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(5));
    let (log, pool) = open_on(&io, "/db/drain", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        log: &log,
        pool: &pool,
        pages: Vec::new(),
    };
    // Five 2 MiB pages fill the pool: a 4 KiB page needs one of their batches drained.
    for _ in 0..5 {
        model.allocate(class(9));
    }
    // A write lands within a few steps (the intent's flush, then the page); a drain that keeps
    // its page because it changed never ends at all.
    let mut allocated = None;
    for _ in 0..1000 {
        if let IOResult::Done(page) = pool.allocate(class(0)).unwrap() {
            allocated = Some(page.pid());
            break;
        }
        // Every 2 MiB page still resident changes, if it can at once, before the step that
        // lands writes: the one in the batch being drained is a page that keeps changing.
        for i in 0..model.pages.len() {
            let p = &model.pages[i];
            if pool.table.find(&pool.frames.headers, p.pid.raw()).is_none() {
                continue;
            }
            let mut admitted = model.admit();
            if let IOResult::Done(mut g) = pool.fix_exclusive(p.pid, Some(&p.hint)).unwrap() {
                let version = p.version + 1;
                let lsn = model.change(&mut g.page_mut(), p.pid, version, &mut admitted);
                g.changed(lsn);
                drop(g);
                model.pages[i].version = version;
            }
        }
        io.step().unwrap();
    }
    assert!(
        allocated.is_some(),
        "the drained batch never emptied while one of its pages kept changing"
    );
    model.check_all();
}

/// A write-out holds its frame through the frames, never the pool, so a pool dropped with one
/// still queued is freed rather than kept alive by its own queue.
#[test]
fn a_write_out_queued_when_the_pool_is_dropped_does_not_keep_it() {
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(5));
    let (log, pool) = open_on(&io, "/db/dropped", MIN_BUDGET);
    let mut model = Model {
        io: &io,
        log: &log,
        pool: &pool,
        pages: Vec::new(),
    };
    model.allocate(class(9));
    let frame = model.pages[0].hint.load(Ordering::Relaxed) - 1;
    drop(model);
    pool.drain(class(9), frame / memory::GRAINS).unwrap();
    assert_eq!(pool.header(frame).status(), OUTGOING, "not written out");
    let weak = Arc::downgrade(&pool);
    drop(pool);
    assert!(
        weak.upgrade().is_none(),
        "a queued write-out kept the pool alive"
    );
}

#[test]
fn a_failed_upgrade_leaves_the_holders_latch_alone() {
    let b = &backends()[0];
    let (_log, pool) = open(b, MIN_BUDGET);
    let page = drive(&b.io, || pool.allocate(class(0))).unwrap();
    let pid = page.pid();
    drop(page);
    let first = drive(&b.io, || pool.fix_optimistic(pid, None)).unwrap();
    let second = drive(&b.io, || pool.fix_optimistic(pid, None)).unwrap();
    let held = first.upgrade().expect("the first upgrade");
    assert!(second.upgrade().is_none(), "two upgrades from one version");
    assert!(
        matches!(pool.fix_exclusive(pid, None).unwrap(), IOResult::IO(_)),
        "a second hold got in while the page was held"
    );
    drop(held);
    assert!(matches!(
        pool.fix_exclusive(pid, None).unwrap(),
        IOResult::Done(_)
    ));
}

#[test]
fn a_drain_chosen_before_its_batch_changed_class_leaves_the_new_class_alone() {
    for b in backends() {
        let (log, pool) = open(&b, MIN_BUDGET);
        let mut model = Model {
            io: &b.io,
            log: &log,
            pool: &pool,
            pages: Vec::new(),
        };
        for _ in 0..8 {
            model.allocate(class(2));
        }
        // Written back, so a wrong eviction drops them at once rather than writing them first.
        for shard in 0..pool.shard_count() {
            let mut pass = pool.pass(shard);
            drive(&b.io, || pass.step(&pool)).unwrap();
        }
        let frame = |p: &Page| p.hint.load(Ordering::Relaxed) - 1;
        let batch = frame(&model.pages[0]) / memory::GRAINS;
        // A drain of the batch as class 0's, chosen before it went to class 2.
        pool.drain(class(0), batch).unwrap();
        for p in &model.pages {
            assert_eq!(
                pool.table.find(&pool.frames.headers, p.pid.raw()),
                Some(frame(p)),
                "{}: {:?} left its frame",
                b.name,
                p.pid
            );
        }
        assert_eq!(pool.memory.take(class(0)), None, "{}", b.name);
        model.check_all();
    }
}

#[test]
fn temporary_pages_go_to_their_files_and_never_to_the_log_or_a_checkpoint() {
    for b in backends() {
        let (log, pool) = open(&b, MIN_BUDGET);
        let end = log.end();
        let read = |pid: PageId| loop {
            let g = drive(&b.io, || pool.fix_optimistic(pid, None)).unwrap();
            if let Some(seen) = g.read(|p| {
                (
                    stamped(p, pid),
                    p.copied(0..p.len()).iter().all(|&x| x == 0),
                )
            }) {
                break seen;
            }
        };
        // 600 pages of 32 KiB, nearly twice the pool: most are written to the temporary file.
        let mut pages = Vec::new();
        for v in 1..=600u64 {
            let mut g = drive(&b.io, || pool.allocate_temporary(class(3))).unwrap();
            let pid = g.pid();
            stamp(&mut g.page_mut(), pid, v);
            g.changed(pool.temporary_lsn());
            pages.push((pid, v));
        }
        for &(pid, v) in &pages {
            assert_eq!(read(pid).0, Some(v), "{}: {pid:?} read back wrong", b.name);
        }
        let overflow = 600 * class(3).size() as u64 - MIN_BUDGET as u64;
        assert!(
            pool.evicted.load(Ordering::Relaxed) >= overflow,
            "{}",
            b.name
        );
        assert_eq!(log.end(), end, "{}: a temporary page was logged", b.name);
        // Passes leave them dirty, and hold the redo floor back for none of them.
        for shard in 0..pool.shard_count() {
            let mut pass = pool.pass(shard);
            drive(&b.io, || pass.step(&pool)).unwrap();
        }
        let dirty = (0..pool.memory.grains())
            .filter(|&f| {
                let h = pool.header(f);
                h.status() == RESIDENT && h.dirty() && PageId::from_raw(h.pid()).is_temporary()
            })
            .count();
        assert!(dirty > 0, "{}: a pass wrote temporary pages", b.name);
        assert_eq!(pool.redo_floor(), end, "{}", b.name);
        // A freed number comes back as a new page, zeros, even once evicted unchanged.
        let freed: Vec<PageId> = pages.drain(..100).map(|(pid, _)| pid).collect();
        for &pid in &freed {
            drive(&b.io, || match pool.fix_exclusive(pid, None)? {
                IOResult::Done(page) => pool.free(page),
                IOResult::IO(io) => Ok(IOResult::IO(io)),
            })
            .unwrap();
        }
        let mut again: Vec<PageId> = (0..100)
            .map(|_| {
                drive(&b.io, || pool.allocate_temporary(class(3)))
                    .unwrap()
                    .pid()
            })
            .collect();
        again.sort_unstable();
        let mut sorted = freed.clone();
        sorted.sort_unstable();
        assert_eq!(
            again, sorted,
            "{}: freed numbers were not handed out again",
            b.name
        );
        for &(pid, v) in &pages {
            assert_eq!(read(pid).0, Some(v), "{}: {pid:?}", b.name);
        }
        for &pid in &again {
            assert!(read(pid).1, "{}: {pid:?} reads its earlier life", b.name);
        }
        assert_eq!(log.end(), end, "{}", b.name);
        // Opening again deletes the files.
        let path = format!("{}.t3", b.base);
        let file = b.io.open_file(&path, OpenFlags::Create, false).unwrap();
        assert!(file.size().unwrap() > 0, "{}: nothing was written", b.name);
        drop((file, pool));
        let (_log, _pool) = open(&b, MIN_BUDGET);
        let file = b.io.open_file(&path, OpenFlags::Create, false).unwrap();
        assert_eq!(
            file.size().unwrap(),
            0,
            "{}: the temporary file survived",
            b.name
        );
    }
}

/// A page and the version last written to it, if one is allocated.
type Slot = Mutex<Option<(PageId, u64)>>;

/// Three threads allocate, change, read and free pages of 1, 2 and 4 MiB, two of each, in a pool
/// of five 2 MiB batches: they need 14 MiB, so the pool evicts and writes back, and memory moves
/// between the classes, a 4 MiB frame taking an aligned run of two batches. Each page reads back
/// the version last written to it.
fn concurrent_pages(ops: usize, seed: u64) {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let (log, pool) = open_on(&io, "/db/schedules", MIN_BUDGET);
    // Pages and the versions written to them, behind one lock per page: a thread changing a page
    // holds its entry, so the version it writes is the one it records.
    let pages: Arc<Vec<Slot>> = Arc::new((0..6).map(|_| Mutex::new(None)).collect());
    let mut handles = Vec::new();
    for t in 0..3u64 {
        let (io, log, pool, pages) = (io.clone(), log.clone(), pool.clone(), pages.clone());
        handles.push(crate::thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(seed * 10 + t);
            for _ in 0..ops {
                let i = rng.random_range(0..pages.len());
                let Some(mut slot) = pages[i].try_lock() else {
                    continue;
                };
                let admit = || named(&io, "admit", || log.admit(framed_len(16)));
                let class = class(8 + i % 3);
                match (*slot, rng.random_range(0..4)) {
                    (None, _) => {
                        let mut admitted = admit();
                        let mut g = named_in(&io, "allocate", Some(&pool), || pool.allocate(class));
                        let pid = g.pid();
                        stamp(&mut g.page_mut(), pid, 1);
                        let lsn = log.append(&mut admitted, &header(), &[&pid.raw().to_le_bytes()]);
                        g.changed(lsn);
                        *slot = Some((pid, 1));
                    }
                    (Some((pid, version)), 0) => {
                        let mut admitted = admit();
                        let mut g = named_in(&io, "fix_exclusive", Some(&pool), || {
                            pool.fix_exclusive(pid, None)
                        });
                        assert_eq!(stamped(g.page(), pid), Some(version));
                        stamp(&mut g.page_mut(), pid, version + 1);
                        let lsn = log.append(&mut admitted, &header(), &[&pid.raw().to_le_bytes()]);
                        g.changed(lsn);
                        *slot = Some((pid, version + 1));
                    }
                    (Some((pid, _)), 1) => {
                        named_in(&io, "free", Some(&pool), || {
                            match pool.fix_exclusive(pid, None)? {
                                IOResult::Done(page) => pool.free(page),
                                IOResult::IO(io) => Ok(IOResult::IO(io)),
                            }
                        });
                        *slot = None;
                    }
                    (Some((pid, version)), _) => loop {
                        let g = named_in(&io, "fix_optimistic", Some(&pool), || {
                            pool.fix_optimistic(pid, None)
                        });
                        if let Some(seen) = g.read(|p| stamped(p, pid)) {
                            assert_eq!(seen, Some(version), "{pid:?} read back wrong");
                            break;
                        }
                    },
                }
            }
        }));
    }
    // The checkpointer's part: metadata pages written back while allocations and frees change
    // them.
    {
        let io = io.clone();
        handles.push(crate::thread::spawn(move || {
            for _ in 0..ops {
                named_in(&io, "write_metadata", Some(&pool), || {
                    pool.store.write_metadata(log.end())
                });
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

/// Drives `op` like [`drive`], and names it in a panic once it has been tried 100,000 times: a
/// schedule that livelocks says where.
fn named<T>(io: &Arc<dyn IO>, name: &str, op: impl FnMut() -> Result<IOResult<T>>) -> T {
    named_in(io, name, None, op)
}

/// Tries after which an operation that has not finished is reported as livelocked.
const TRIES: u64 = 100_000;

/// Reports an operation that has tried [`TRIES`] times: by default it fails the test. With
/// ATOLL_PARK_ON_LIVELOCK set, the thread stops here instead, so a stall catcher can take every
/// thread's stack while the livelock stands. With ATOLL_SLOW_GOES_ON, it says so and goes on: a
/// run that then ends was slow, not stuck.
fn too_many_tries(state: String) {
    if std::env::var_os("ATOLL_PARK_ON_LIVELOCK").is_some() {
        eprintln!("{state}\nparked");
        loop {
            std::thread::park();
        }
    }
    if std::env::var_os("ATOLL_SLOW_GOES_ON").is_some() {
        eprintln!("{state}\ngoing on");
    } else {
        panic!("{state}");
    }
}

/// [`named`], showing `pool`'s state in the panic too.
fn named_in<T>(
    io: &Arc<dyn IO>,
    name: &str,
    pool: Option<&BufferPool>,
    mut op: impl FnMut() -> Result<IOResult<T>>,
) -> T {
    // Tries handed back something already done (a yield), and something to wait on.
    let (mut tries, mut ready) = (0u64, 0u64);
    drive(io, || {
        tries += 1;
        if tries == TRIES {
            too_many_tries(format!(
                "{name} tried {tries} times, {ready} handed back a yield\n{}",
                pool.map_or(String::new(), BufferPool::describe)
            ));
        }
        let r = op();
        // A slow operation that ends says so, with the steps under way then: the one it waited
        // on has left.
        if tries >= TRIES && matches!(r, Ok(IOResult::Done(_))) {
            eprintln!(
                "{name} done after {tries} tries; {}",
                pool.map_or(String::new(), |p| p.store.epochs().describe())
            );
        }
        if let Ok(IOResult::IO(IOCompletions(c))) = &r {
            ready += c.finished() as u64;
        }
        r
    })
    .unwrap()
}

fn header() -> Header {
    Header {
        kind: CHANGE,
        flags: 0,
        txn: 0,
        prev: 0,
    }
}

#[cfg(not(shuttle))]
#[test]
fn pages_survive_concurrent_fixes_evictions_and_frees() {
    for seed in 0..20 {
        concurrent_pages(200, seed);
    }
}

#[cfg(shuttle)]
#[test]
fn pages_survive_concurrent_fixes_evictions_and_frees_in_shuttle_schedules() {
    static SEED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    crate::storage::atoll::testing::schedules(
        || concurrent_pages(12, SEED.fetch_add(1, std::sync::atomic::Ordering::Relaxed)),
        300,
    );
}

/// A page written back to be evicted afterwards is evicted only if it is still the page in the
/// frame: once its write ends, another thread may evict it and put a new page in the frame,
/// which the writer must not evict in its stead.
fn written_page_eviction() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let (log, pool) = open_on(&io, "/db/written", MIN_BUDGET);
    let class = class(9);
    // Five 2 MiB frames fill the pool; four stay latched, so the page written back is the only
    // one another thread can evict.
    let mut held = Vec::new();
    for v in 0..5u64 {
        let mut admitted = named(&io, "admit", || log.admit(framed_len(16)));
        let mut g = named(&io, "allocate", || pool.allocate(class));
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, v);
        let lsn = log.append(&mut admitted, &header(), &[&pid.raw().to_le_bytes()]);
        g.changed(lsn);
        held.push(g);
    }
    let written = held.remove(0);
    let frame = written.hint() - 1;
    drop(written);
    pool.header(frame)
        .flags
        .fetch_or(WRITING | EVICT_AFTER, Ordering::AcqRel);
    drop(pool.write_back(class, vec![frame], true));
    let writer = {
        let (io, pool) = (io.clone(), pool.clone());
        crate::thread::spawn(move || {
            // As `drive` does: a yield gives the processor up, or a thread spinning here can keep
            // one holding what it waits on from ever running, as a priority scheduler did.
            while let Some(c) = pool.drive_writebacks().unwrap() {
                crate::thread::yield_now();
                io.wait_for_completion(c).unwrap();
            }
        })
    };
    let allocator = {
        let (io, pool) = (io.clone(), pool.clone());
        crate::thread::spawn(move || drop(named(&io, "allocate", || pool.allocate(class))))
    };
    writer.join().unwrap();
    allocator.join().unwrap();
    drop(held);
}

#[cfg(not(shuttle))]
#[test]
fn a_written_page_is_evicted_only_while_its_write_pins_it() {
    for _ in 0..200 {
        written_page_eviction();
    }
}

#[cfg(shuttle)]
#[test]
fn a_written_page_is_evicted_only_while_its_write_pins_it_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(written_page_eviction, 300);
}

/// A pass waiting for its own writeback wakes when another thread finishes it. A writeback
/// finishes after its last write settles, so a pass that looks in between, while the other thread
/// holds the writeback, waits on the pool: only the finish can wake it.
#[test]
fn a_pass_waiting_on_its_writeback_wakes_when_another_thread_finishes_it() {
    // Writes stay in flight until the IO steps, so the pass cannot settle its own on submitting.
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(1));
    let (log, pool) = open_on(&io, "/db/woken", MIN_BUDGET);
    let mut admitted = drive(&io, || log.admit(framed_len(16))).unwrap();
    let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
    let pid = g.pid();
    stamp(&mut g.page_mut(), pid, 1);
    let lsn = log.append(&mut admitted, &header(), &[&pid.raw().to_le_bytes()]);
    g.changed(lsn);
    let frame = g.frame;
    drop(g);
    drop(admitted);
    // The pass claims the page and submits its write, which stays in flight until the IO steps.
    let mut pass = pool.pass(pool.shard_of(frame));
    while pool.writes_in_flight.load(Ordering::Acquire) == 0 {
        match pass.step(&pool).unwrap() {
            IOResult::IO(IOCompletions(c)) => io.wait_for_completion(c).unwrap(),
            IOResult::Done(()) => panic!("the pass wrote nothing"),
        }
    }
    // Another thread settles the write; the pass steps once its last write has settled and before
    // the writeback finishes, and finds the writeback held.
    let waited: Arc<Mutex<Option<Completion>>> = Arc::new(Mutex::new(None));
    let wb = pool.writebacks.lock()[0].clone();
    wb.lock().before_finish = Some({
        let (pool, waited) = (pool.clone(), waited.clone());
        Box::new(move || match pass.step(&pool).unwrap() {
            IOResult::IO(IOCompletions(c)) => *waited.lock() = Some(c),
            IOResult::Done(()) => panic!("the pass finished before its writeback"),
        })
    });
    drive(&io, || pool.settle_writes()).unwrap();
    let waited = waited.lock().take().expect("the pass stepped");
    assert!(
        waited.finished(),
        "the pass waits on a wakeup no one sends once its writeback has finished"
    );
}

/// A pass writing back more pages than the writeback buffer holds copies of writes what fits,
/// takes room again as writes land, and writes every page: none is left dirty, and each reads
/// back as written. The writes stay in flight until the IO steps, so the buffer fills.
#[test]
fn a_pass_writes_back_more_pages_than_the_writeback_buffer_holds() {
    let io: Arc<dyn IO> = Arc::new(CrashIO::new(3));
    let (log, pool) = open_with(
        &io,
        "/db/room",
        BufferConfig {
            budget: MIN_BUDGET,
            checkpoint_shards: 1,
            writeback_bytes: page::MAX_PAGE_SIZE,
        },
    );
    let room = page::MAX_PAGE_SIZE / class(0).size();
    let mut pids = Vec::new();
    for v in 0..(room + room / 8) as u64 {
        let mut admitted = drive(&io, || log.admit(framed_len(16))).unwrap();
        let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, v);
        let lsn = log.append(&mut admitted, &header(), &[&pid.raw().to_le_bytes()]);
        g.changed(lsn);
        pids.push((pid, v));
    }
    assert!(pids.len() > room, "the pages fit the buffer");
    let mut pass = pool.pass(0);
    drive(&io, || pass.step(&pool)).unwrap();
    let dirty = (0..pool.memory.grains())
        .filter(|&f| pool.header(f).status() == RESIDENT && pool.header(f).dirty())
        .count();
    assert_eq!(dirty, 0, "pages left dirty");
    for (pid, v) in pids {
        let g = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
        assert_eq!(g.read(|p| stamped(p, pid)), Some(Some(v)));
    }
}

/// A frame read into from disk is written plainly by the IO, so it is taken only once no step
/// that could still read its memory's last pages is left: every 4 KiB grain it spans counts, not
/// just the first. Two 1 MiB pages share a 2 MiB batch; the first is freed before a reader's step
/// begins, the second while the reader is in its step and has the page fixed. The batch then goes
/// to the 2 MiB class: a new page may take its frame at once, but a read waits for the reader.
#[cfg(not(shuttle))]
#[test]
fn a_frame_is_read_into_only_once_every_grain_it_spans_is_quiet() {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let (_log, pool) = open_on(&io, "/db/quiet", MIN_BUDGET);
    let (small, large) = (class(8), class(9));
    let first = drive(&io, || pool.allocate(small)).unwrap();
    let mut second = drive(&io, || pool.allocate(small)).unwrap();
    let batch = first.frame / memory::GRAINS;
    assert_eq!(
        second.frame / memory::GRAINS,
        batch,
        "the pages share a batch"
    );
    assert!(first.frame < second.frame);
    let (pid, held_frame) = (second.pid(), second.frame);
    stamp(&mut second.page_mut(), pid, 1);
    let first_pid = first.pid();
    drop((first, second));
    let free = |pid| {
        drive(&io, || match pool.fix_exclusive(pid, None)? {
            IOResult::Done(page) => pool.free(page),
            IOResult::IO(io) => Ok(IOResult::IO(io)),
        })
        .unwrap()
    };
    // Every other batch to the large class, so the small pages' batch is the only memory left
    // for a large page.
    let mut others = 0;
    while let Got::Frame(_) = pool.take_or_grow(large, Purpose::New) {
        others += 1;
    }
    assert!(others > 0);
    free(first_pid);
    let frame = memory::frame_at(large, batch, 0);
    assert!(grains(frame, large).contains(&(held_frame as usize)));
    std::thread::scope(|s| {
        // Dropped if this thread panics, which lets the reader go.
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (leave, leave_rx) = std::sync::mpsc::channel::<()>();
        let (pool, io) = (&pool, &io);
        let reader = s.spawn(move || {
            let _step = pool.store.epochs().enter();
            let g = drive(&io, || pool.fix_optimistic(pid, None)).unwrap();
            assert_eq!(g.read(|p| stamped(p, pid)), Some(Some(1)));
            entered.send(()).unwrap();
            let _ = leave_rx.recv();
        });
        entered_rx.recv().unwrap();
        free(pid);
        assert!(pool.memory.drain_for(small, batch, large));
        // A new page is written through the frame's writer, whose stores race with nothing.
        match pool.take_or_grow(large, Purpose::New) {
            Got::Frame(f) => assert_eq!(f, frame),
            _ => panic!("no frame for a new page in the drained batch"),
        }
        pool.memory.put(large, frame);
        assert!(
            matches!(pool.take_or_grow(large, Purpose::Read), Got::Quieting),
            "a read took memory a reader in its step may still be reading"
        );
        leave.send(()).unwrap();
        reader.join().unwrap();
    });
    match pool.take_or_grow(large, Purpose::Read) {
        Got::Frame(f) => assert_eq!(f, frame),
        _ => panic!("the frame stayed unquiet after the reader left"),
    }
}

/// A write that lands while another thread steps its writeback, after that step took the writes
/// landed so far, is left for the next step. A waiter that found the writeback held when the
/// write's own wakeup had already passed waits for the stepper: the stepper wakes it.
#[test]
fn a_write_landing_during_another_threads_step_wakes_whoever_waits_on_the_writeback() {
    // Writes stay in flight until the IO steps, which completes them in an order its seed picks:
    // a seed whose write is still in flight once the pass submitted it builds the case.
    let mut built = 0;
    for seed in 0..32 {
        let crash = Arc::new(CrashIO::new(seed));
        let io: Arc<dyn IO> = crash.clone();
        let (log, pool) = open_on(&io, "/db/landed", MIN_BUDGET);
        let mut admitted = drive(&io, || log.admit(framed_len(16))).unwrap();
        let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
        let pid = g.pid();
        stamp(&mut g.page_mut(), pid, 1);
        let lsn = log.append(&mut admitted, &header(), &[&pid.raw().to_le_bytes()]);
        g.changed(lsn);
        let frame = g.frame;
        drop(g);
        drop(admitted);
        // Stepped until it submits the write, and no further: its wait ends only once it lands.
        let mut pass = pool.pass(pool.shard_of(frame));
        loop {
            match pass.step(&pool).unwrap() {
                IOResult::IO(IOCompletions(_))
                    if pool.writes_in_flight.load(Ordering::Acquire) > 0 =>
                {
                    break
                }
                IOResult::IO(IOCompletions(c)) => io.wait_for_completion(c).unwrap(),
                IOResult::Done(()) => panic!("the pass wrote nothing"),
            }
        }
        let wb = pool.writebacks.lock()[0].clone();
        let landings = wb.lock().landings();
        if !landings.lock().is_empty() || crash.in_flight() == 0 {
            continue;
        }
        built += 1;
        // Inside the step, once it took the landed writes (none): the write lands, its wakeup
        // passes, and a waiter finds the writeback held.
        let waited: Arc<Mutex<Option<Completion>>> = Arc::new(Mutex::new(None));
        wb.lock().after_taking_landed = Some({
            let (pool, waited, crash, io) =
                (pool.clone(), waited.clone(), crash.clone(), io.clone());
            Box::new(move || {
                while crash.in_flight() > 0 {
                    io.step().unwrap();
                }
                assert!(!landings.lock().is_empty(), "the write did not land");
                match pool.settle_writes().unwrap() {
                    IOResult::IO(IOCompletions(c)) => *waited.lock() = Some(c),
                    IOResult::Done(()) => panic!("settled with the write unsettled"),
                }
            })
        });
        assert!(pool.drive_writebacks().unwrap().is_none());
        let waited = waited.lock().take().expect("the waiter looked");
        assert!(
            waited.finished(),
            "seed {seed}: the waiter waits on a wakeup no one sends, with its write landed and \
             unsettled"
        );
    }
    assert!(built > 0, "no seed left the write in flight");
}

/// A page leaving its frame, wanted while another thread steps its write-out, which waits on
/// its write plan before it submits the write: the reader waits for that thread, and is woken by
/// it, not by the write, which nothing submits until someone steps the write-out again.
#[test]
fn a_reader_of_a_page_written_out_wakes_when_its_write_out_waits_before_submitting() {
    // Reads stay in flight until the IO steps, so the write plan's metadata page waits.
    let crash = Arc::new(CrashIO::new(3));
    let io: Arc<dyn IO> = crash.clone();
    let (log, pool) = open_on(&io, "/db/outgoing", MIN_BUDGET);
    let mut admitted = drive(&io, || log.admit(framed_len(16))).unwrap();
    let mut g = drive(&io, || pool.allocate(class(0))).unwrap();
    let pid = g.pid();
    stamp(&mut g.page_mut(), pid, 1);
    let lsn = log.append(&mut admitted, &header(), &[&pid.raw().to_le_bytes()]);
    g.changed(lsn);
    let frame = g.frame;
    drop(g);
    drop(admitted);
    // The page leaves its frame, as a drain makes it.
    let hold = pool
        .latch_to_evict(class(0), frame, || FrameLatch {
            frames: pool.frames.clone(),
            frame,
        })
        .expect("the page is held by no one");
    let out = pool.start_write_out(hold);
    pool.write_out(class(0), vec![out]);
    let waited: Arc<Mutex<Option<Completion>>> = Arc::new(Mutex::new(None));
    let wb = pool.writebacks.lock()[0].clone();
    wb.lock().before_step = Some({
        let (pool, waited) = (pool.clone(), waited.clone());
        Box::new(move || match pool.fix_optimistic(pid, None).unwrap() {
            IOResult::IO(IOCompletions(c)) => *waited.lock() = Some(c),
            IOResult::Done(_) => panic!("the page read as resident while it is written out"),
        })
    });
    match pool.drive_writebacks().unwrap() {
        Some(c) if !c.finished() => {}
        other => panic!(
            "the write-out did not wait before submitting: {:?}",
            other.map(|c| c.finished())
        ),
    }
    assert_eq!(pool.writes_in_flight.load(Ordering::Acquire), 0);
    let waited = waited.lock().take().expect("the reader looked");
    assert!(
        waited.finished(),
        "the reader waits for a write no one submits while the write-out waits on its plan"
    );
}

/// Slots of the directory page in [`linked_pages`]: each names a child and the life it is in.
const SLOTS: usize = 6;

fn slot_at(i: usize) -> usize {
    HEADER_SIZE + 16 * i
}

/// Waits for `c` as [`drive`] does between tries.
fn wait_for(io: &Arc<dyn IO>, c: Completion) {
    crate::thread::yield_now();
    while !c.finished() {
        io.step().unwrap();
        if !c.finished() {
            crate::thread::yield_now();
        }
    }
    assert!(c.get_error().is_none(), "{:?}", c.get_error());
}

/// What one try of an operation through the directory came to; a retry says where it came from.
enum Try<T> {
    Done(T),
    /// Wait for this, holding nothing, then try again.
    Wait(Completion, &'static str),
    Again(&'static str),
}

/// Runs `op` until it is done, waiting between tries with nothing held, as an operation that
/// returns IO drops its guards.
fn through<T>(
    io: &Arc<dyn IO>,
    pool: &BufferPool,
    name: &str,
    mut op: impl FnMut() -> Try<T>,
) -> T {
    let mut retries: BTreeMap<&'static str, u64> = BTreeMap::new();
    for tries in 1u64.. {
        if tries == TRIES {
            too_many_tries(format!(
                "{name} tried {tries} times, retried {retries:?}\n{}",
                pool.describe()
            ));
        }
        match op() {
            Try::Done(v) => {
                if tries >= TRIES {
                    eprintln!(
                        "{name} done after {tries} tries; {}",
                        pool.store.epochs().describe()
                    );
                }
                return v;
            }
            Try::Wait(c, why) => {
                *retries.entry(why).or_default() += 1;
                wait_for(io, c)
            }
            Try::Again(why) => {
                *retries.entry(why).or_default() += 1;
                crate::thread::yield_now()
            }
        }
    }
    unreachable!("tries run out")
}

/// One try of a fix, for [`through`]: a fix of a number the directory no longer names may find
/// it freed, which is only an error while the directory still names it.
fn fixed<T>(r: Result<IOResult<T>>, directory: &Optimistic<'_>, class: SizeClass) -> Try<T> {
    match r {
        Ok(IOResult::Done(v)) => Try::Done(v),
        Ok(IOResult::IO(IOCompletions(c))) => {
            let why = match (c.finished(), class.index()) {
                (true, 8) => "child fix yielded, 1 MiB",
                (true, 9) => "child fix yielded, 2 MiB",
                (true, _) => "child fix yielded, 4 MiB",
                (false, _) => "child fix waited",
            };
            Try::Wait(c, why)
        }
        Err(LimboError::Corrupt(_)) if !directory.validate() => Try::Again("child freed"),
        Err(e) => panic!("a fix through a valid directory failed: {e}"),
    }
}

/// Children linked from one directory page, as tree nodes are from their parent. Three threads
/// read children through the directory, change them, unlink and free them, and allocate and
/// link new ones, with no lock of the test's own: whatever excludes them is the pool's latches
/// and the directory's. The children are 1, 2 and 4 MiB in a pool of 10 MiB, so they are evicted,
/// written back and moved between classes meanwhile, and their numbers freed and reused. A
/// thread that finds the directory unchanged after it read a child read that child whole, as the
/// page and in the life the directory names: never a freed number's next page.
fn linked_pages(ops: usize, seed: u64) {
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let (log, pool) = open_on(&io, "/db/linked", MIN_BUDGET);
    let admit = |log: &Arc<Log>| named(&io, "admit", || log.admit(framed_len(16)));
    let dir = {
        let mut admitted = admit(&log);
        let mut g = named_in(&io, "allocate", Some(&pool), || pool.allocate(class(0)));
        let lsn = log.append(&mut admitted, &header(), &[&g.pid().raw().to_le_bytes()]);
        g.changed(lsn);
        g.pid()
    };
    let lives = Arc::new(AtomicU64::new(1));
    let mut handles = Vec::new();
    for t in 0..3u64 {
        let (io, log, pool, lives) = (io.clone(), log.clone(), pool.clone(), lives.clone());
        handles.push(crate::thread::spawn(move || {
            let admit = || named(&io, "admit", || log.admit(framed_len(16)));
            let free = |pid: PageId| {
                named_in(&io, "free", Some(&pool), || {
                    match pool.fix_exclusive(pid, None)? {
                        IOResult::Done(page) => pool.free(page),
                        IOResult::IO(io) => Ok(IOResult::IO(io)),
                    }
                })
            };
            let mut rng = ChaCha8Rng::seed_from_u64(seed * 10 + t);
            for _ in 0..ops {
                let i = rng.random_range(0..SLOTS);
                match rng.random_range(0..8) {
                    // Allocate a child and link it, if the slot is still empty then.
                    0 => {
                        let life = lives.fetch_add(1, Ordering::Relaxed);
                        let mut admitted = admit();
                        let mut g = named_in(&io, "allocate", Some(&pool), || {
                            pool.allocate(class(8 + i % 3))
                        });
                        let pid = g.pid();
                        stamp(&mut g.page_mut(), pid, life << 32);
                        let lsn = log.append(&mut admitted, &header(), &[&pid.raw().to_le_bytes()]);
                        g.changed(lsn);
                        drop(g);
                        let mut admitted = admit();
                        let linked = through(&io, &pool, "link", || {
                            let mut d = match pool.fix_exclusive(dir, None).unwrap() {
                                IOResult::Done(d) => d,
                                IOResult::IO(IOCompletions(c)) => {
                                    return Try::Wait(c, "directory fix for a link")
                                }
                            };
                            if d.page().u64_at(slot_at(i)) != 0 {
                                d.release_unchanged();
                                return Try::Done(false);
                            }
                            d.page_mut().put_u64(slot_at(i), pid.raw());
                            d.page_mut().put_u64(slot_at(i) + 8, life);
                            let lsn =
                                log.append(&mut admitted, &header(), &[&dir.raw().to_le_bytes()]);
                            d.changed(lsn);
                            Try::Done(true)
                        });
                        if !linked {
                            free(pid);
                        }
                    }
                    // Unlink a child, then free it.
                    1 => {
                        let mut admitted = admit();
                        let unlinked = through(&io, &pool, "unlink", || {
                            let mut d = match pool.fix_exclusive(dir, None).unwrap() {
                                IOResult::Done(d) => d,
                                IOResult::IO(IOCompletions(c)) => {
                                    return Try::Wait(c, "directory fix for an unlink")
                                }
                            };
                            let raw = d.page().u64_at(slot_at(i));
                            if raw == 0 {
                                d.release_unchanged();
                                return Try::Done(None);
                            }
                            d.page_mut().put_u64(slot_at(i), 0);
                            d.page_mut().put_u64(slot_at(i) + 8, 0);
                            let lsn =
                                log.append(&mut admitted, &header(), &[&dir.raw().to_le_bytes()]);
                            d.changed(lsn);
                            Try::Done(Some(PageId::from_raw(raw)))
                        });
                        if let Some(pid) = unlinked {
                            free(pid);
                        }
                    }
                    // Change a child, reached through the directory.
                    2..4 => {
                        let mut admitted = admit();
                        through(&io, &pool, "change", || {
                            let d = match pool.fix_optimistic(dir, None).unwrap() {
                                IOResult::Done(d) => d,
                                IOResult::IO(IOCompletions(c)) => {
                                    return Try::Wait(c, "directory fix for a change")
                                }
                            };
                            let Some((raw, life)) =
                                d.read(|p| (p.u64_at(slot_at(i)), p.u64_at(slot_at(i) + 8)))
                            else {
                                return Try::Again("directory changed while read");
                            };
                            if raw == 0 {
                                return Try::Done(());
                            }
                            let pid = PageId::from_raw(raw);
                            let mut g = match fixed(pool.fix_exclusive(pid, None), &d, pid.class())
                            {
                                Try::Done(g) => g,
                                Try::Wait(c, why) => return Try::Wait(c, why),
                                Try::Again(why) => return Try::Again(why),
                            };
                            if !d.validate() {
                                g.release_unchanged();
                                return Try::Again("directory changed under the child");
                            }
                            let v = stamped(g.page(), pid).unwrap_or_else(|| {
                                panic!("{pid:?} changed torn or as another page")
                            });
                            assert_eq!(v >> 32, life, "{pid:?} changed in another life");
                            stamp(&mut g.page_mut(), pid, v + 1);
                            let lsn = log.append(&mut admitted, &header(), &[&raw.to_le_bytes()]);
                            g.changed(lsn);
                            Try::Done(())
                        });
                    }
                    // Read a child through the directory.
                    _ => through(&io, &pool, "read", || {
                        let d = match pool.fix_optimistic(dir, None).unwrap() {
                            IOResult::Done(d) => d,
                            IOResult::IO(IOCompletions(c)) => {
                                return Try::Wait(c, "directory fix for a read")
                            }
                        };
                        let Some((raw, life)) =
                            d.read(|p| (p.u64_at(slot_at(i)), p.u64_at(slot_at(i) + 8)))
                        else {
                            return Try::Again("directory changed while read");
                        };
                        if raw == 0 {
                            return Try::Done(());
                        }
                        let pid = PageId::from_raw(raw);
                        let holder = || d.validate();
                        let child = match fixed(
                            pool.fix_optimistic_held(pid, None, &holder),
                            &d,
                            pid.class(),
                        ) {
                            Try::Done(Some(c)) => c,
                            Try::Done(None) => {
                                return Try::Again("directory changed before the load")
                            }
                            Try::Again(why) => return Try::Again(why),
                            Try::Wait(c, why) => return Try::Wait(c, why),
                        };
                        let Some(seen) = child.read(|p| stamped(p, pid)) else {
                            return Try::Again("child changed while read");
                        };
                        if !d.validate() {
                            return Try::Again("directory changed under the child");
                        }
                        let v =
                            seen.unwrap_or_else(|| panic!("{pid:?} read torn or as another page"));
                        assert_eq!(v >> 32, life, "{pid:?} read in another life");
                        Try::Done(())
                    }),
                }
            }
        }));
    }
    // The checkpointer's part: metadata pages written back while allocations and frees change
    // them.
    {
        let (io, pool) = (io.clone(), pool.clone());
        handles.push(crate::thread::spawn(move || {
            for _ in 0..ops {
                named_in(&io, "write_metadata", Some(&pool), || {
                    pool.store.write_metadata(log.end())
                });
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[cfg(not(shuttle))]
#[test]
fn children_read_through_their_directory_are_whole_and_in_the_life_it_names() {
    for seed in 0..20 {
        linked_pages(200, seed);
    }
}

#[cfg(shuttle)]
#[test]
fn children_read_through_their_directory_are_whole_and_in_the_life_it_names_in_shuttle_schedules() {
    static SEED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    crate::storage::atoll::testing::schedules(
        || linked_pages(12, SEED.fetch_add(1, std::sync::atomic::Ordering::Relaxed)),
        300,
    );
}
