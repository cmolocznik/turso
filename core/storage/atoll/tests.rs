//! A whole database under crashes: blob writes through the buffer pool while it evicts, writes
//! back and moves memory between size classes, pages freed and their numbers reused, continuous
//! checkpoints, and recovery on several threads.

use super::blob;
use super::buffer::{BufferConfig, MIN_BUDGET};
use super::db::{Atoll, AtollConfig, Opening};
use super::log::{LogConfig, Lsn};
use super::page::{PageId, SizeClass, HEADER_SIZE};
use super::radix::FREE;
use super::store::StoreConfig;
use super::testing::drive;
use crate::io::crash::CrashIO;
use crate::io::IO;
use crate::sync::atomic::AtomicU32;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::Result;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

const BASE: &str = "/db/atoll";
/// Places in a page that writes take in turn, so a page holds several writes at once.
const PLACES: u64 = 8;
const STAMP: usize = 16;

fn config() -> AtollConfig {
    AtollConfig {
        log: LogConfig {
            segment_bytes: 512 << 10,
            buffer_bytes: 128 << 10,
            chunk_bytes: 16 << 10,
            writes_in_flight: 3,
            direct: true,
        },
        store: StoreConfig {
            file_bytes: 1 << 30,
            meta_pages: 1 << 10,
        },
        buffer: BufferConfig {
            budget: MIN_BUDGET,
            checkpoint_shards: 8,
        },
        log_budget: Some(96 << 10),
        txn: Default::default(),
    }
}

fn class(i: usize) -> SizeClass {
    SizeClass::new(i).unwrap()
}

/// Where a write of `version` goes, and what it writes there.
fn stamp(pid: PageId, version: u64) -> (usize, [u8; STAMP]) {
    let mut bytes = [0u8; STAMP];
    bytes[..8].copy_from_slice(&pid.raw().to_le_bytes());
    bytes[8..].copy_from_slice(&version.to_le_bytes());
    (HEADER_SIZE + (version % PLACES) as usize * STAMP, bytes)
}

/// A page number's lives: from the allocation, through writes, to the free that ends it.
#[derive(Default)]
struct Lives(Vec<Life>);

struct Life {
    born: Lsn,
    writes: Vec<(Lsn, u64)>,
    freed: Option<Lsn>,
}

impl Lives {
    /// What the page holds once the log is replayed to `end`: per place, the version written
    /// there last; `None` if no life of the page is live there.
    fn at(&self, end: Lsn) -> Option<[Option<u64>; PLACES as usize]> {
        let life = self.0.iter().rev().find(|l| l.born <= end)?;
        if life.freed.is_some_and(|f| f <= end) {
            return None;
        }
        let mut places = [None; PLACES as usize];
        for &(lsn, version) in &life.writes {
            if lsn <= end {
                places[(version % PLACES) as usize] = Some(version);
            }
        }
        Some(places)
    }

    fn live(&self) -> bool {
        self.0.last().is_some_and(|l| l.freed.is_none())
    }
}

/// Opens the database on `threads` threads, the way an embedder lends them to recovery.
fn open(io: Arc<dyn IO>, threads: usize) -> Atoll {
    let opening = Opening::new(io, BASE, config()).unwrap();
    std::thread::scope(|s| {
        for _ in 1..threads {
            s.spawn(|| opening.work().unwrap());
        }
        opening.work().unwrap();
    });
    opening.finish().unwrap()
}

/// Checks that `db` holds what the model says the log up to `end` leaves.
fn check(db: &Atoll, io: &Arc<dyn IO>, model: &BTreeMap<PageId, Lives>, end: Lsn, label: &str) {
    for (&pid, lives) in model {
        let entry = drive(io, || db.store().entry(pid)).unwrap();
        let Some(places) = lives.at(end) else {
            assert_eq!(entry, FREE, "{label}: {pid:?} was freed by {end}");
            continue;
        };
        assert_ne!(entry, FREE, "{label}: {pid:?} lost");
        let page = loop {
            let g = drive(io, || db.pool().fix_optimistic(pid, None)).unwrap();
            if let Some(bytes) = g.read(|p| p.to_vec()) {
                break bytes;
            }
        };
        for (place, version) in places.iter().enumerate() {
            let at = HEADER_SIZE + place * STAMP;
            let expected = match version {
                Some(v) => stamp(pid, *v).1,
                None => [0; STAMP],
            };
            assert_eq!(
                page[at..at + STAMP],
                expected,
                "{label}: {pid:?} place {place} after recovery to {end}"
            );
        }
    }
}

fn crashes_recover_every_durable_write_and_nothing_torn(durable_writes: bool) {
    for seed in 0..2u64 {
        let crash_io = Arc::new(CrashIO::new(seed));
        crash_io.set_durable_writes(durable_writes);
        let io: Arc<dyn IO> = crash_io.clone();
        let db = open(io.clone(), 1);
        let model: RefCell<BTreeMap<PageId, Lives>> = RefCell::new(BTreeMap::new());
        let acknowledged = Cell::new(0);
        let steps = Cell::new(0u64);
        let checks = Cell::new(0u64);
        let crash_and_check = |crash_seed: u64| {
            let threads = 1 + (crash_seed % 3) as usize;
            let crashed: Arc<dyn IO> = Arc::new(crash_io.crash(crash_seed));
            let label = format!("seed {seed}/{crash_seed}, {threads} threads");
            let recovered = open(crashed.clone(), threads);
            let end = recovered.recovered_end().expect("a recovery");
            assert!(
                end >= acknowledged.get(),
                "{label}: recovery ended at {end}, before {}, which was durable",
                acknowledged.get()
            );
            check(&recovered, &crashed, &model.borrow(), end, &label);
            drop(recovered);
            // What recovery left recovers again to the same state: the log as it was up to
            // where the first recovery ended, which appended no page records of its own.
            let again = open(crashed.clone(), 1);
            let label = format!("{label}, reopened");
            check(&again, &crashed, &model.borrow(), end, &label);
            checks.set(checks.get() + 1);
        };
        let run = |op: &mut dyn FnMut() -> Result<IOResult<()>>| loop {
            match op().unwrap() {
                IOResult::Done(()) => return,
                IOResult::IO(IOCompletions(c)) => {
                    while !c.finished() {
                        io.step().unwrap();
                        steps.set(steps.get() + 1);
                        if steps.get() % 29 == 0 {
                            crash_and_check(seed * 1_000_000 + steps.get());
                        }
                    }
                }
            }
        };
        let hints: RefCell<BTreeMap<PageId, AtomicU32>> = RefCell::new(BTreeMap::new());
        let version = Cell::new(0u64);
        let write = |pid: PageId| {
            let v = version.get() + 1;
            version.set(v);
            let (offset, bytes) = stamp(pid, v);
            let mut admitted = None;
            run(&mut || {
                admitted = Some(match db.log().admit(blob::record_len(STAMP))? {
                    IOResult::Done(a) => a,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                });
                Ok(IOResult::Done(()))
            });
            let mut lsn = 0;
            run(&mut || {
                let hints = hints.borrow();
                let mut page = match db.pool().fix_exclusive(pid, hints.get(&pid))? {
                    IOResult::Done(p) => p,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                };
                lsn = blob::write(
                    db.log(),
                    admitted.as_mut().unwrap(),
                    &mut page,
                    offset,
                    &bytes,
                );
                Ok(IOResult::Done(()))
            });
            let mut model = model.borrow_mut();
            let life = model.get_mut(&pid).unwrap().0.last_mut().unwrap();
            life.writes.push((lsn, v));
        };
        let allocate = |c: SizeClass| -> PageId {
            let mut pid = None;
            run(&mut || {
                let page = match db.pool().allocate(c)? {
                    IOResult::Done(p) => p,
                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                };
                pid = Some((page.pid(), page.hint()));
                Ok(IOResult::Done(()))
            });
            let (pid, hint) = pid.unwrap();
            // The page lives from its adoption on: recovery frees a page it finds unadopted.
            run(&mut || db.store().adopt(&[pid]));
            let born = db.log().end();
            hints.borrow_mut().insert(pid, AtomicU32::new(hint));
            model.borrow_mut().entry(pid).or_default().0.push(Life {
                born,
                writes: Vec::new(),
                freed: None,
            });
            pid
        };
        let free = |pid: PageId| {
            run(&mut || {
                let hints = hints.borrow();
                match db.pool().fix_exclusive(pid, hints.get(&pid))? {
                    IOResult::Done(page) => db.pool().free(page),
                    IOResult::IO(io) => Ok(IOResult::IO(io)),
                }
            });
            let freed = db.log().end();
            model
                .borrow_mut()
                .get_mut(&pid)
                .unwrap()
                .0
                .last_mut()
                .unwrap()
                .freed = Some(freed);
        };
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        // More 16 KiB pages than the pool holds, so pages are evicted and written back.
        for _ in 0..700 {
            let pid = allocate(class(2));
            write(pid);
            run(&mut || db.checkpointer().step());
        }
        for op in 0..1500u32 {
            let live: Vec<PageId> = model
                .borrow()
                .iter()
                .filter(|(_, l)| l.live())
                .map(|(&p, _)| p)
                .collect();
            match rng.random_range(0..100) {
                // Class 0 takes memory from class 2, moving its pages, dirty ones included.
                0..10 => {
                    let pid = allocate(class(0));
                    write(pid);
                }
                10..14 => free(live[rng.random_range(0..live.len())]),
                _ => write(live[rng.random_range(0..live.len())]),
            }
            run(&mut || db.checkpointer().step());
            if op % 16 == 15 {
                let end = db.log().end();
                run(&mut || db.log().wait_durable(end));
                acknowledged.set(end);
            }
        }
        let redo = db.checkpointer().superblock().redo;
        let end = db.log().end();
        assert!(
            redo > 0 && end - redo < end / 2,
            "seed {seed}: the redo point is {redo} with the log at {end}"
        );
        assert!(
            checks.get() >= 100,
            "seed {seed}: only {} crashes",
            checks.get()
        );
        crash_and_check(seed);
    }
}

#[test]
fn crashes_recover_every_durable_write_and_nothing_torn_with_durable_writes() {
    crashes_recover_every_durable_write_and_nothing_torn(true);
}

#[test]
fn crashes_recover_every_durable_write_and_nothing_torn_with_writes_then_syncs() {
    crashes_recover_every_durable_write_and_nothing_torn(false);
}

#[test]
fn recovery_frees_pages_left_provisional_and_the_redo_point_waits_for_them() {
    use super::store::{PageStore, StoreRecord};
    let io: Arc<dyn IO> = Arc::new(crate::io::MemoryIO::new());
    let db = open(io.clone(), 1);
    let allocate = |db: &Atoll| {
        let page = drive(&io, || db.pool().allocate(class(0))).unwrap();
        let pid = page.pid();
        drop(page);
        pid
    };
    let write = |db: &Atoll, pid: PageId| {
        let mut admitted = drive(&io, || db.log().admit(blob::record_len(STAMP))).unwrap();
        let mut page = drive(&io, || db.pool().fix_exclusive(pid, None)).unwrap();
        let (offset, bytes) = stamp(pid, 1);
        blob::write(db.log(), &mut admitted, &mut page, offset, &bytes);
    };
    let checkpoint = |db: &Atoll| {
        let ticket = db.checkpointer().request_full();
        drive(&io, || db.checkpointer().step_full(ticket)).unwrap();
        db.checkpointer().superblock().redo
    };
    // Never adopted: as if the operation that allocated it crashed before linking it in.
    let unlinked_at = db.log().end();
    let unlinked = allocate(&db);
    write(&db, unlinked);
    let kept = allocate(&db);
    drive(&io, || db.store().adopt(&[kept])).unwrap();
    write(&db, kept);
    // Adopted, then abandoned: as if it crashed between unlinking it and freeing it.
    let dropped = allocate(&db);
    drive(&io, || db.store().adopt(&[dropped])).unwrap();
    let abandon = StoreRecord::Abandon {
        pids: vec![dropped],
    };
    let (header, payload) = PageStore::group_record(&abandon);
    let len = crate::storage::atoll::log::framed_len(payload.len());
    let mut admitted = drive(&io, || db.log().admit(len)).unwrap();
    let lsn = db.log().append(&mut admitted, &header, &[&payload]);
    db.store().grouped(&abandon, lsn - len, lsn);
    let redo = checkpoint(&db);
    assert!(
        redo <= unlinked_at,
        "the redo point moved to {redo}, past the provisional page made at {unlinked_at}"
    );
    // An operation dropped with its page in hand hands it over; the checkpointer frees it.
    let orphan = allocate(&db);
    db.store().orphaned(orphan);
    drive(&io, || db.checkpointer().step()).unwrap();
    assert_eq!(drive(&io, || db.store().entry(orphan)).unwrap(), FREE);
    drive(&io, || db.log().wait_durable(db.log().end())).unwrap();
    drop(db);
    let db = open(io.clone(), 1);
    for (pid, live) in [
        (unlinked, false),
        (kept, true),
        (dropped, false),
        (orphan, false),
    ] {
        let entry = drive(&io, || db.store().entry(pid)).unwrap();
        assert_eq!(entry != FREE, live, "{pid:?}");
    }
    let redo = checkpoint(&db);
    assert!(
        redo >= db.recovered_end().unwrap(),
        "nothing provisional is left to hold the redo point at {redo}"
    );
}
