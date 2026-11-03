//! Recovery (DESIGN.md §12): replaying the log from the redo point of the superblock in charge.
//!
//! 1. **Analysis.** One reader replays the page store's records, so the page table and free space
//!    are as the log left them, and finds where the log ends: the first record that does not read
//!    back whole at its own position. It notes the pages freed and allocated again within the
//!    replay: their records before the new allocation belong to the page's earlier life.
//! 2. **Redo**, in parallel. The log is read again in windows. Each window's page records are
//!    split into partitions by page id, and a partition's records apply in log order, window after
//!    window, to pages in the buffer pool. A page starts from the copy the page table names, the
//!    newest whose remap is durable, or from zeros if it has none; a record applies only if it is
//!    newer than the page. Whichever thread asks for work takes the next window to read or the
//!    next partition ready to apply, so any number of threads recover together.
//! 3. **Finishing.** Pages left provisional are freed, slots of intents no remap matched are
//!    given back, every loser is undone ([`rollback::undo_loser`]), every page still retired is
//!    freed, since no snapshot survived to read the version it held, and a full checkpoint writes
//!    back every page redone and undone. Its redo point is past where the log restarted, so
//!    nothing replayed here is replayed again.
//!
//! The analysis also finds the losers: the superblock's writer table names those that need undo
//! by the log before the redo point, and each transaction's records after it move it on, a
//! Commit or an End finishing it. The clock restarts above every timestamp logged.
//!
//! The page store's records are all replayed before anything is appended: a new record would
//! stamp the metadata pages it changes with an LSN past every record still to replay. That is why
//! the log is read twice.
//!
//! **Continuations.** The log reopens at the block after its end, and the bytes between stay as
//! the crash left them: a reader from the superblock in charge would stop at the end, and miss
//! whatever recovery appends past the gap. But recovery's checkpoint writes metadata pages, and
//! data copies they name, before its superblock lands; a crash between would leave pages a later
//! recovery cannot explain from the log it reads. So before it appends anything, recovery writes
//! the superblock in charge again with the gap recorded ([`Superblock::continuations`]), and
//! waits until it is durable: every later reader goes past the gap. A gap is recorded only while
//! the superblock has room for it; with none left, recovery fails before it changes anything. The
//! checkpoint whose redo point passes the gaps drops them.

use super::buffer::{BufferConfig, BufferPool, LocalSource};
use super::checkpoint::Checkpointer;
use super::control::{Control, Superblock, MAX_CONTINUATIONS};
use super::db::AtollConfig;
use super::drive;
use super::log::{
    decode_table, GroupStart, Log, LogConfig, LogReader, Lsn, OwnedRecord, Writers, WRITERS,
};
use super::once::{Baton, SetOnce};
use super::page::{self, PageId};
use super::radix::FREE;
use super::redo::PageRecord;
use super::stack::Stack;
use super::store::{kinds, PageStore, StoreRecord};
use super::txn::Ts;
use super::txn::{self, rollback, Clock};
use crate::io::{Completion, IO};
use crate::sync::atomic::{fence, AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use crate::sync::Arc;
use crate::{LimboError, Result};
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};

/// Windows read and not yet wholly applied, at most: one being read while the others apply, and
/// room for partitions to run up to two windows ahead of the slowest.
const WINDOWS: usize = 4;

/// Partitions per hardware thread: enough that a thread done with its partition usually finds
/// another ready while one holding a hot page runs long.
const PARTITIONS_PER_THREAD: usize = 4;

/// The phases, each moved on by compare-and-swap; the analysis and the finish are taken by the one
/// thread whose exchange moves them on to running.
const ANALYSIS: u8 = 0;
const ANALYZING: u8 = 1;
const REDO: u8 = 2;
const FINISH: u8 = 3;
const FINISHING: u8 = 4;
const DONE: u8 = 5;
const FAILED: u8 = 6;

/// A part's state, beside the index of the window it is part of.
const READY: u64 = 1;
const TAKEN: u64 = 2;
const PART_DONE: u64 = 3;

fn part_state(window: u64, state: u64) -> u64 {
    window << 2 | state
}

/// Recovery without a lock. Its phase moves on by compare-and-swap. What the analysis finds is set
/// once. The log reader is a baton the thread reading a window holds, and the windows read are a
/// ring: a partition's part of a window is one word naming the window, taken by compare-and-swap,
/// so a part is never taken from a window its slot held before or holds after; a window's slot is
/// read into again only once every part of the window it held is done. Threads with nothing to do
/// wait on a take-all stack, looking for work again once on it, and every change wakes them.
pub(super) struct Recovery {
    io: Arc<dyn IO>,
    base: String,
    log_config: LogConfig,
    buffer: BufferConfig,
    log_budget: u64,
    /// The superblock in charge as recovery found it.
    superblock: Superblock,
    /// The writer registry its checkpoint, and then the database's transactions, use.
    writers: Arc<Writers>,
    /// The superblock in charge once the analysis recorded the gap it leaves, if it leaves one.
    recorded: SetOnce<Superblock>,
    store: Arc<PageStore>,
    /// The control file: the analysis's to write, then the finish's to hand on.
    control: Baton<Control>,
    /// Log per window: what one checkpoint pass covers.
    window_bytes: u64,
    partitions: usize,
    phase: AtomicU8,
    failure: SetOnce<String>,
    analyzed: SetOnce<Arc<Analyzed>>,
    /// The redo's reader, held by the thread reading a window.
    reader: Baton<LogReader>,
    /// Windows read so far: written by the thread holding the reader.
    read: AtomicU64,
    /// Set once the last window is read, after `read`.
    read_all: AtomicBool,
    /// The oldest window not wholly applied.
    first: AtomicU64,
    /// The windows read and not wholly applied, by index modulo [`WINDOWS`].
    windows: Box<[Window]>,
    /// Per partition, the index of the next window whose part it applies.
    next: Box<[AtomicU64]>,
    /// Threads waiting for work.
    waits: Stack<Completion>,
    recovered: Baton<Recovered>,
}

/// What recovery leaves behind.
pub(super) struct Recovered {
    /// Where the valid log ended: the state recovered is the log's up to here.
    pub end: Lsn,
    pub log: Arc<Log>,
    pub pool: Arc<BufferPool>,
    pub checkpointer: Checkpointer,
    /// Restarted above every timestamp logged (DESIGN.md §12, step 7).
    pub clock: Arc<Clock>,
}

/// What the analysis found.
struct Analyzed {
    /// Where the valid log ends.
    end: Lsn,
    /// Where the log resumed appending: the end, up to the next block boundary.
    restart: Lsn,
    /// Pages freed and allocated again within the replay: where their last allocation ends.
    reborn: HashMap<PageId, Lsn>,
    /// The highest timestamp the superblock and the replayed log hold.
    clock: Ts,
    /// The transactions with no Commit or End, each with its last group: their undo is owed.
    losers: Vec<(u64, GroupStart)>,
    pool: Arc<BufferPool>,
}

/// A window's slot in the ring.
struct Window {
    /// The window it holds, plus one; 0 before its first.
    index: AtomicU64,
    /// Its parts not yet done.
    left: AtomicUsize,
    parts: Box<[Part]>,
}

/// A partition's records of a window.
struct Part {
    /// [`part_state`]: 0 before its first window.
    state: AtomicU64,
    /// Written by the reader before the part is ready, taken by the thread whose exchange takes
    /// the part.
    records: UnsafeCell<Vec<(PageId, OwnedRecord)>>,
}

// SAFETY: the records are written and taken only by the thread their part's state hands them to.
unsafe impl Sync for Part {}

/// What a thread asking for work gets.
enum Next {
    Analysis,
    Read(LogReader),
    Apply(u64, usize, Vec<(PageId, OwnedRecord)>),
    Finish,
    /// Nothing to do until the state changes.
    Wait,
    Done,
}

impl Recovery {
    /// Recovery of the database at `base` from `superblock`, the one in charge. `store` is the
    /// page store as that superblock left it ([`PageStore::recovering`]).
    pub(super) fn new(
        io: Arc<dyn IO>,
        base: &str,
        config: &AtollConfig,
        store: Arc<PageStore>,
        control: Control,
        superblock: Superblock,
        writers: Arc<Writers>,
    ) -> Self {
        let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
        let partitions = threads * PARTITIONS_PER_THREAD;
        let log_budget = config.log_budget();
        Self {
            window_bytes: (log_budget / config.buffer.checkpoint_shards as u64).max(1),
            io,
            base: base.to_string(),
            log_config: config.log.clone(),
            buffer: config.buffer.clone(),
            log_budget,
            superblock,
            writers,
            recorded: SetOnce::default(),
            store,
            control: Baton::new(Some(control)),
            partitions,
            phase: AtomicU8::new(ANALYSIS),
            failure: SetOnce::default(),
            analyzed: SetOnce::default(),
            reader: Baton::new(None),
            read: AtomicU64::new(0),
            read_all: AtomicBool::new(false),
            first: AtomicU64::new(0),
            windows: (0..WINDOWS)
                .map(|_| Window {
                    index: AtomicU64::new(0),
                    left: AtomicUsize::new(0),
                    parts: (0..partitions)
                        .map(|_| Part {
                            state: AtomicU64::new(0),
                            records: UnsafeCell::new(Vec::new()),
                        })
                        .collect(),
                })
                .collect(),
            next: (0..partitions).map(|_| AtomicU64::new(0)).collect(),
            waits: Stack::default(),
            recovered: Baton::new(None),
        }
    }

    /// Recovers alongside every other thread calling this; returns once recovery is done.
    pub(super) fn work(&self) -> Result<()> {
        // A thread that panics with work in hand fails recovery for every thread, which would
        // otherwise wait for that work forever.
        struct Unwinding<'r>(&'r Recovery);
        impl Drop for Unwinding<'_> {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    self.0.fail("a recovering thread panicked".to_string());
                }
            }
        }
        let _unwinding = Unwinding(self);
        loop {
            // Waiting starts before the look: either a change after the look wakes it, or the
            // look sees the change.
            let changed = Completion::new_wait();
            self.waits.push(changed.clone());
            fence(Ordering::SeqCst);
            let done = match self.next()? {
                Next::Analysis => self.analyze(),
                Next::Read(reader) => self.read(reader),
                Next::Apply(window, partition, records) => self.apply(window, partition, records),
                Next::Finish => self.finish(),
                Next::Wait => {
                    // Another thread holds the work: the analysis, the finish, the reader, or
                    // every partition ready. Step the IO, which may be what it waits on, and yield
                    // through `crate::thread`: under shuttle nothing else in this loop hands the
                    // holder the processor, so the std yield spins the waiter forever.
                    while !changed.finished() {
                        self.io.step()?;
                        crate::thread::yield_now();
                    }
                    continue;
                }
                Next::Done => return Ok(()),
            };
            if let Err(e) = done {
                self.fail(e.to_string());
                return Err(e);
            }
        }
    }

    /// What recovery left behind, once it is done.
    pub(super) fn take_recovered(&self) -> Option<Recovered> {
        self.recovered.take()
    }

    /// Wakes every thread waiting for work, to look again.
    fn signal(&self) {
        fence(Ordering::SeqCst);
        for c in self.waits.take_all() {
            c.complete(0);
        }
    }

    fn fail(&self, why: String) {
        self.failure.set(why);
        self.phase.store(FAILED, Ordering::Release);
        self.signal();
    }

    /// Moves the phase on from `from` to `to`, if it is still at `from`.
    fn move_on(&self, from: u8, to: u8) -> bool {
        self.phase
            .compare_exchange(from, to, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn analyzed(&self) -> &Arc<Analyzed> {
        self.analyzed.get_ref().expect("the analysis done")
    }

    /// The next work for a thread asking.
    fn next(&self) -> Result<Next> {
        loop {
            return Ok(match self.phase.load(Ordering::Acquire) {
                FAILED => {
                    return Err(LimboError::InternalError(format!(
                        "recovery failed: {}",
                        self.failure.get().unwrap_or_default()
                    )))
                }
                DONE => Next::Done,
                ANALYZING | FINISHING => Next::Wait,
                ANALYSIS => match self.move_on(ANALYSIS, ANALYZING) {
                    true => Next::Analysis,
                    false => continue,
                },
                FINISH => match self.move_on(FINISH, FINISHING) {
                    true => Next::Finish,
                    false => continue,
                },
                _ => {
                    for p in 0..self.partitions {
                        let k = self.next[p].load(Ordering::Acquire);
                        let part = &self.windows[k as usize % WINDOWS].parts[p];
                        if part
                            .state
                            .compare_exchange(
                                part_state(k, READY),
                                part_state(k, TAKEN),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            // SAFETY: the exchange handed this thread the part's records.
                            let records = std::mem::take(unsafe { &mut *part.records.get() });
                            return Ok(Next::Apply(k, p, records));
                        }
                    }
                    if !self.read_all.load(Ordering::Acquire) {
                        let room = self.read.load(Ordering::Acquire)
                            < self.first.load(Ordering::Acquire) + WINDOWS as u64;
                        if room {
                            if let Some(reader) = self.reader.take() {
                                return Ok(Next::Read(reader));
                            }
                        }
                    } else if self.first.load(Ordering::Acquire)
                        == self.read.load(Ordering::Acquire)
                    {
                        // Every window read is applied.
                        self.move_on(REDO, FINISH);
                        continue;
                    }
                    Next::Wait
                }
            });
        }
    }

    /// Replays the page store's records and finds where the log ends.
    fn analyze(&self) -> Result<()> {
        let superblock = &self.superblock;
        let mut reader = Log::reader(
            self.io.clone(),
            &self.base,
            &self.log_config,
            superblock.redo,
        )
        .following(&superblock.continuations);
        let mut freed = HashSet::new();
        let mut reborn = HashMap::new();
        let mut clock = superblock.clock;
        // Every transaction that needs undo by the log before the redo point, and its last group;
        // the log from there moves them on.
        let mut open: HashMap<u64, GroupStart> = HashMap::new();
        if let Some(at) = superblock.writers {
            let table = Log::read_group(self.io.clone(), &self.base, &self.log_config, at)?;
            match table.as_slice() {
                [r] if r.header.kind == WRITERS => open.extend(
                    decode_table(&r.payload)?
                        .into_iter()
                        .map(|w| (w.txn, w.last)),
                ),
                _ => {
                    return Err(LimboError::Corrupt(format!(
                        "the superblock's writer table at {} is no writer table",
                        at.get()
                    )))
                }
            }
        }
        while let Some(r) = reader.next_record()? {
            // A record's transaction is named by its start timestamp.
            clock = clock.max(r.header.txn);
            if r.header.txn != 0 {
                if r.header.kind == txn::COMMIT || r.header.kind == txn::END {
                    open.remove(&r.header.txn);
                } else {
                    let group = GroupStart::at(r.group);
                    let last = open.entry(r.header.txn).or_insert(group);
                    *last = (*last).max(group);
                }
            }
            if r.header.kind == txn::COMMIT {
                let ts = txn::commit_ts(&r.payload).ok_or_else(|| {
                    LimboError::Corrupt(format!("a Commit record at {} does not decode", r.start))
                })?;
                clock = clock.max(ts);
            }
            if !kinds::owns(r.header.kind) {
                continue;
            }
            drive(&self.io, || self.store.redo(&r))?;
            match StoreRecord::decode_logged(r.header.kind, &r.payload) {
                Some((StoreRecord::Free { pid, .. }, _)) => {
                    freed.insert(pid);
                }
                Some((StoreRecord::New { pid }, _)) if freed.contains(&pid) => {
                    reborn.insert(pid, r.lsn);
                }
                _ => {}
            }
        }
        let end = reader.position();
        let restart = Log::resumes_at(end);
        if restart > end && superblock.continuations.len() >= MAX_CONTINUATIONS {
            return Err(LimboError::InternalError(format!(
                "recovery would reopen the log at {restart}, past its end at {end}, and the \
                 superblock already records the {MAX_CONTINUATIONS} gaps it holds: recovering on \
                 would leave a gap no reader goes past"
            )));
        }
        // Reopening zeroes everything a crash left from `restart` on, durably. The gap is
        // recorded only then, and before anything is appended: a reader that follows it finds
        // nothing past it but what this recovery appends, never records the crash left there.
        let log = Log::open(self.io.clone(), &self.base, self.log_config.clone(), end)?;
        assert_eq!(log.end(), restart, "the log reopened elsewhere");
        if restart > end {
            let mut continuations = superblock.continuations.clone();
            continuations.push((end, restart));
            let recorded = Superblock {
                seq: superblock.seq + 1,
                continuations,
                ..superblock.clone()
            };
            let control = self.control.take().expect("the control file");
            let written = control.write_and_wait(&recorded);
            self.control.put(control);
            written?;
            self.recorded.set(recorded);
        }
        self.store.resume(log);
        let pool = BufferPool::new(
            self.buffer.clone(),
            Arc::new(LocalSource::new(self.store.clone())),
            self.store.clone(),
            superblock.redo,
        )?;
        let reader = Log::reader(
            self.io.clone(),
            &self.base,
            &self.log_config,
            superblock.redo,
        )
        .following(&superblock.continuations);
        let mut losers: Vec<(u64, GroupStart)> = open.into_iter().collect();
        losers.sort_unstable();
        self.analyzed.set(Arc::new(Analyzed {
            end,
            restart,
            reborn,
            clock,
            losers,
            pool,
        }));
        self.reader.put(reader);
        self.phase.store(REDO, Ordering::Release);
        self.signal();
        Ok(())
    }

    /// Reads the next window of page records, if the ring has room for it.
    fn read(&self, mut reader: LogReader) -> Result<()> {
        let k = self.read.load(Ordering::Relaxed);
        // Its slot's last window, WINDOWS before it, wholly applied.
        if k >= self.first.load(Ordering::Acquire) + WINDOWS as u64 {
            self.reader.put(reader);
            // A window applied meanwhile woke no one for the reader it held.
            self.signal();
            return Ok(());
        }
        let end = self.analyzed().end;
        let mut parts: Vec<Vec<(PageId, OwnedRecord)>> = vec![Vec::new(); self.partitions];
        let from = reader.position();
        let mut read_all = true;
        while reader.position() < end {
            if reader.position() - from >= self.window_bytes {
                read_all = false;
                break;
            }
            let Some(r) = reader.next_record()? else {
                break;
            };
            let Some(pid) = PageRecord::decode(r.header.kind, &r.payload)?.map(|p| p.pid()) else {
                continue;
            };
            parts[(pid.hash() % self.partitions as u64) as usize].push((pid, r));
        }
        let slot = &self.windows[k as usize % WINDOWS];
        // The count before the window, the window before its parts: a part done counts down a
        // count already set, and a window found by its index has its count.
        slot.left.store(
            parts.iter().filter(|p| !p.is_empty()).count(),
            Ordering::Relaxed,
        );
        slot.index.store(k + 1, Ordering::Release);
        for (part, records) in slot.parts.iter().zip(parts) {
            let state = if records.is_empty() { PART_DONE } else { READY };
            // SAFETY: the part's last window is done, and its records taken: no one reaches
            // them until the state below hands them on.
            unsafe { *part.records.get() = records };
            part.state.store(part_state(k, state), Ordering::Release);
        }
        self.read.store(k + 1, Ordering::Release);
        if read_all {
            self.read_all.store(true, Ordering::Release);
        } else {
            self.reader.put(reader);
        }
        for p in 0..self.partitions {
            self.advance(p);
        }
        self.pop_done();
        self.signal();
        Ok(())
    }

    /// Applies a partition's records of one window.
    fn apply(
        &self,
        window: u64,
        partition: usize,
        mut records: Vec<(PageId, OwnedRecord)>,
    ) -> Result<()> {
        let analyzed = self.analyzed();
        // A stable sort keeps a page's records in log order; grouping them fixes each page once.
        records.sort_by_key(|(pid, _)| *pid);
        for group in records.chunk_by(|a, b| a.0 == b.0) {
            let pid = group[0].0;
            // A page dropped by the end of the log: none of its records count.
            if drive(&self.io, || self.store.entry(pid))? == FREE {
                continue;
            }
            let born = analyzed.reborn.get(&pid).copied().unwrap_or(0);
            let mut page = drive(&self.io, || analyzed.pool.fix_exclusive(pid, None))?;
            for (_, r) in group {
                if r.lsn <= born || r.lsn <= page::lsn(page.page()) {
                    continue;
                }
                let record = PageRecord::decode(r.header.kind, &r.payload)?.expect("a page record");
                record.redo(&mut page.page_mut())?;
                page.changed(r.lsn);
            }
        }
        let slot = &self.windows[window as usize % WINDOWS];
        slot.parts[partition]
            .state
            .store(part_state(window, PART_DONE), Ordering::Release);
        slot.left.fetch_sub(1, Ordering::AcqRel);
        self.advance(partition);
        self.pop_done();
        self.signal();
        Ok(())
    }

    /// Moves `partition` past the windows whose part it has done, or that are wholly applied.
    fn advance(&self, partition: usize) {
        let next = &self.next[partition];
        loop {
            let k = next.load(Ordering::Acquire);
            let done = k < self.first.load(Ordering::Acquire)
                || self.windows[k as usize % WINDOWS].parts[partition]
                    .state
                    .load(Ordering::Acquire)
                    == part_state(k, PART_DONE);
            if !done {
                return;
            }
            let _ = next.compare_exchange(k, k + 1, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    /// Moves past the oldest windows once every part of them is done.
    fn pop_done(&self) {
        loop {
            let f = self.first.load(Ordering::Acquire);
            let slot = &self.windows[f as usize % WINDOWS];
            if slot.index.load(Ordering::Acquire) != f + 1 || slot.left.load(Ordering::Acquire) != 0
            {
                return;
            }
            let _ = self
                .first
                .compare_exchange(f, f + 1, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    /// Frees pages left provisional, gives back the slots of unmatched intents and checkpoints
    /// everything redone.
    fn finish(&self) -> Result<()> {
        let analyzed = self.analyzed();
        let (pool, restart) = (analyzed.pool.clone(), analyzed.restart);
        let clock = Arc::new(Clock::after(analyzed.clock));
        let log = self.store.log().clone();
        // Pages allocated and never linked in, or unlinked and never freed, before the crash.
        for pid in self.store.provisional_pages() {
            drive(&self.io, || pool.free_unreferenced(pid))?;
        }
        drive(&self.io, || self.store.release_orphans())?;
        self.store.end_recovery();
        for &(txn, last) in &analyzed.losers {
            rollback::undo_loser(
                &self.io,
                &self.base,
                &self.log_config,
                &pool,
                &self.store,
                &self.writers,
                txn,
                last,
            )?;
        }
        // Undo put back what was the losers', marks and all: what is still retired held versions
        // only a snapshot could read. Through the pool: redo may have put such a page in a frame.
        drive(&self.io, || {
            self.store.free_retired_pages(|pid| pool.free_retired(pid))
        })?;
        let control = self.control.take().expect("the control file");
        let superblock = self
            .recorded
            .get()
            .unwrap_or_else(|| self.superblock.clone());
        let checkpointer = Checkpointer::new(
            pool.clone(),
            self.store.clone(),
            control,
            superblock,
            self.log_budget,
            clock.clone(),
            self.writers.clone(),
        );
        let ticket = checkpointer.request_full();
        drive(&self.io, || checkpointer.step_full(ticket))?;
        let redo = checkpointer.superblock().redo;
        if redo < restart {
            return Err(LimboError::InternalError(format!(
                "recovery's checkpoint left the redo point at {redo}, before the log's restart at {restart}"
            )));
        }
        self.store.allow_erasing();
        drive(&self.io, || self.store.erase_freed())?;
        self.recovered.put(Recovered {
            end: analyzed.end,
            log,
            pool,
            checkpointer,
            clock,
        });
        self.phase.store(DONE, Ordering::Release);
        self.signal();
        Ok(())
    }
}
