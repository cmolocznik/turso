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
//!    given back, and a full checkpoint writes back every page redone. Its redo point is past where the log restarted, so nothing
//!    replayed here is replayed again.
//!
//! Undoing open transactions and restarting the clock come with transactions (DESIGN.md §20, step
//! 7).
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
use super::log::{Log, LogConfig, LogReader, Lsn, OwnedRecord};
use super::page::{self, PageId};
use super::radix::FREE;
use super::redo::PageRecord;
use super::store::{kinds, PageStore, StoreRecord};
use super::txn::Ts;
use super::txn::{self, Clock};
use crate::io::{Completion, IO};
use crate::sync::{Arc, Mutex};
use crate::{LimboError, Result};
use std::collections::{HashMap, HashSet, VecDeque};

/// Windows read and not yet wholly applied, at most: one being read while the others apply, and
/// room for partitions to run up to two windows ahead of the slowest.
const WINDOWS: usize = 4;

/// Partitions per hardware thread: enough that a thread done with its partition usually finds
/// another ready while one holding a hot page runs long.
const PARTITIONS_PER_THREAD: usize = 4;

pub(super) struct Recovery {
    io: Arc<dyn IO>,
    base: String,
    log_config: LogConfig,
    buffer: BufferConfig,
    log_budget: u64,
    /// The superblock in charge: as recovery found it, then with the gap it records.
    superblock: Mutex<Superblock>,
    store: Arc<PageStore>,
    control: Mutex<Option<Control>>,
    /// Log per window: what one checkpoint pass covers.
    window_bytes: u64,
    partitions: usize,
    state: Mutex<State>,
    /// Done when the state next changes, for threads waiting for work.
    changed: Mutex<Option<Completion>>,
}

/// What recovery leaves behind.
pub(super) struct Recovered {
    /// Where the valid log ended: the state recovered is the log's up to here.
    pub end: Lsn,
    pub log: Arc<Log>,
    pub pool: Arc<BufferPool>,
    pub checkpointer: Checkpointer,
    /// Restarted above every timestamp logged (DESIGN.md §12, step 6).
    pub clock: Arc<Clock>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Analysis,
    Redo,
    Finish,
    Done,
    Failed,
}

struct State {
    phase: Phase,
    /// A thread is running the analysis or the finish, which take one thread each.
    running: bool,
    failure: Option<String>,
    /// Where the valid log ends.
    end: Lsn,
    /// Where the log resumed appending: the end, up to the next block boundary.
    restart: Lsn,
    /// Pages freed and allocated again within the replay: where their last allocation ends.
    reborn: Arc<HashMap<PageId, Lsn>>,
    /// The highest timestamp the superblock and the replayed log hold.
    clock: Ts,
    pool: Option<Arc<BufferPool>>,
    reader: Option<LogReader>,
    read_all: bool,
    /// Windows not yet wholly applied, oldest first; `first` is the index of the oldest.
    windows: VecDeque<Window>,
    first: u64,
    /// Per partition, the index of the next window whose part it applies.
    next: Vec<u64>,
    recovered: Option<Recovered>,
}

struct Window {
    parts: Vec<Part>,
    /// Parts not yet done.
    left: usize,
}

enum Part {
    Ready(Vec<(PageId, OwnedRecord)>),
    Taken,
    Done,
}

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
            superblock: Mutex::new(superblock),
            store,
            control: Mutex::new(Some(control)),
            partitions,
            state: Mutex::new(State {
                phase: Phase::Analysis,
                running: false,
                failure: None,
                end: 0,
                restart: 0,
                reborn: Arc::new(HashMap::new()),
                clock: 0,
                pool: None,
                reader: None,
                read_all: false,
                windows: VecDeque::new(),
                first: 0,
                next: vec![0; partitions],
                recovered: None,
            }),
            changed: Mutex::new(None),
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
                    let mut state = self.0.state.lock();
                    state.phase = Phase::Failed;
                    state
                        .failure
                        .get_or_insert_with(|| "a recovering thread panicked".to_string());
                    drop(state);
                    self.0.signal();
                }
            }
        }
        let _unwinding = Unwinding(self);
        loop {
            // Waiting starts before the look, so a change in between is not missed.
            let changed = self.changed_wait();
            let next = self.next(&mut self.state.lock())?;
            let done = match next {
                Next::Analysis => self.analyze(),
                Next::Read(reader) => self.read(reader),
                Next::Apply(window, partition, records) => self.apply(window, partition, records),
                Next::Finish => self.finish(),
                Next::Wait => {
                    // Another thread holds the work: the analysis, the finish, or every
                    // partition ready. Step the IO, which may be what it waits on.
                    while !changed.finished() {
                        self.io.step()?;
                        std::thread::yield_now();
                    }
                    continue;
                }
                Next::Done => return Ok(()),
            };
            if let Err(e) = done {
                let mut state = self.state.lock();
                state.phase = Phase::Failed;
                state.failure.get_or_insert_with(|| e.to_string());
                drop(state);
                self.signal();
                return Err(e);
            }
        }
    }

    /// What recovery left behind, once it is done.
    pub(super) fn take_recovered(&self) -> Option<Recovered> {
        self.state.lock().recovered.take()
    }

    fn changed_wait(&self) -> Completion {
        self.changed
            .lock()
            .get_or_insert_with(Completion::new_wait)
            .clone()
    }

    fn signal(&self) {
        let waiting = self.changed.lock().take();
        if let Some(c) = waiting {
            c.complete(0);
        }
    }

    /// The next work for a thread asking.
    fn next(&self, state: &mut State) -> Result<Next> {
        Ok(match state.phase {
            Phase::Failed => {
                return Err(LimboError::InternalError(format!(
                    "recovery failed: {}",
                    state.failure.as_deref().unwrap_or("")
                )))
            }
            Phase::Done => Next::Done,
            Phase::Analysis | Phase::Finish if state.running => Next::Wait,
            Phase::Analysis => {
                state.running = true;
                Next::Analysis
            }
            Phase::Finish => {
                state.running = true;
                Next::Finish
            }
            Phase::Redo => {
                for p in 0..self.partitions {
                    let k = state.next[p];
                    let Some(window) = state.windows.get_mut((k - state.first) as usize) else {
                        continue;
                    };
                    if let Part::Ready(_) = window.parts[p] {
                        let Part::Ready(records) =
                            std::mem::replace(&mut window.parts[p], Part::Taken)
                        else {
                            unreachable!("the part was ready");
                        };
                        return Ok(Next::Apply(k, p, records));
                    }
                }
                if !state.read_all && state.windows.len() < WINDOWS {
                    if let Some(reader) = state.reader.take() {
                        return Ok(Next::Read(reader));
                    }
                }
                if state.read_all && state.windows.is_empty() {
                    state.phase = Phase::Finish;
                    return self.next(state);
                }
                Next::Wait
            }
        })
    }

    /// Replays the page store's records and finds where the log ends.
    fn analyze(&self) -> Result<()> {
        let superblock = self.superblock.lock().clone();
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
        while let Some(r) = reader.next_record()? {
            // A record's transaction is named by its start timestamp.
            clock = clock.max(r.header.txn);
            if txn::owns(r.header.kind) {
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
        // Recorded before the log reopens past the gap, and before anything is appended there.
        let restart = Log::resumes_at(end);
        if restart > end {
            if superblock.continuations.len() >= MAX_CONTINUATIONS {
                return Err(LimboError::InternalError(format!(
                    "recovery would reopen the log at {restart}, past its end at {end}, and the \
                     superblock already records the {MAX_CONTINUATIONS} gaps it holds: recovering \
                     on would leave a gap no reader goes past"
                )));
            }
            let mut continuations = superblock.continuations.clone();
            continuations.push((end, restart));
            let recorded = Superblock {
                seq: superblock.seq + 1,
                continuations,
                ..superblock.clone()
            };
            self.control
                .lock()
                .as_ref()
                .expect("the control file")
                .write_and_wait(&recorded)?;
            *self.superblock.lock() = recorded;
        }
        let log = Log::open(self.io.clone(), &self.base, self.log_config.clone(), end)?;
        assert_eq!(log.end(), restart, "the log reopened elsewhere");
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
        let mut state = self.state.lock();
        state.end = end;
        state.restart = restart;
        state.reborn = Arc::new(reborn);
        state.clock = clock;
        state.pool = Some(pool);
        state.reader = Some(reader);
        state.running = false;
        state.phase = Phase::Redo;
        drop(state);
        self.signal();
        Ok(())
    }

    /// Reads the next window of page records.
    fn read(&self, mut reader: LogReader) -> Result<()> {
        let end = self.state.lock().end;
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
        let mut state = self.state.lock();
        let left = parts.iter().filter(|p| !p.is_empty()).count();
        state.windows.push_back(Window {
            parts: parts
                .into_iter()
                .map(|p| {
                    if p.is_empty() {
                        Part::Done
                    } else {
                        Part::Ready(p)
                    }
                })
                .collect(),
            left,
        });
        state.read_all = read_all;
        if !read_all {
            state.reader = Some(reader);
        }
        for p in 0..self.partitions {
            advance(&mut state, p);
        }
        pop_done(&mut state);
        drop(state);
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
        let (pool, reborn) = {
            let state = self.state.lock();
            (state.pool.clone().expect("a pool"), state.reborn.clone())
        };
        // A stable sort keeps a page's records in log order; grouping them fixes each page once.
        records.sort_by_key(|(pid, _)| *pid);
        for group in records.chunk_by(|a, b| a.0 == b.0) {
            let pid = group[0].0;
            // A page dropped by the end of the log: none of its records count.
            if drive(&self.io, || self.store.entry(pid))? == FREE {
                continue;
            }
            let born = reborn.get(&pid).copied().unwrap_or(0);
            let mut page = drive(&self.io, || pool.fix_exclusive(pid, None))?;
            for (_, r) in group {
                if r.lsn <= born || r.lsn <= page::lsn(page.page()) {
                    continue;
                }
                let record = PageRecord::decode(r.header.kind, &r.payload)?.expect("a page record");
                record.redo(&mut page.page_mut())?;
                page.changed(r.lsn);
            }
        }
        let mut state = self.state.lock();
        let i = (window - state.first) as usize;
        let w = &mut state.windows[i];
        w.parts[partition] = Part::Done;
        w.left -= 1;
        advance(&mut state, partition);
        pop_done(&mut state);
        drop(state);
        self.signal();
        Ok(())
    }

    /// Frees pages left provisional, gives back the slots of unmatched intents and checkpoints
    /// everything redone.
    fn finish(&self) -> Result<()> {
        let (pool, restart, clock) = {
            let state = self.state.lock();
            (
                state.pool.clone().expect("a pool"),
                state.restart,
                Arc::new(Clock::after(state.clock)),
            )
        };
        let log = self.store.log().clone();
        // Pages allocated and never linked in, or unlinked and never freed, before the crash.
        for pid in self.store.provisional_pages() {
            drive(&self.io, || pool.free_unreferenced(pid))?;
        }
        drive(&self.io, || self.store.release_orphans())?;
        self.store.end_recovery();
        let control = self.control.lock().take().expect("the control file");
        let checkpointer = Checkpointer::new(
            pool.clone(),
            self.store.clone(),
            control,
            self.superblock.lock().clone(),
            self.log_budget,
            clock.clone(),
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
        let mut state = self.state.lock();
        let end = state.end;
        state.recovered = Some(Recovered {
            end,
            log,
            pool,
            checkpointer,
            clock,
        });
        state.running = false;
        state.phase = Phase::Done;
        drop(state);
        self.signal();
        Ok(())
    }
}

/// Moves `partition` past the windows whose part it has done.
fn advance(state: &mut State, partition: usize) {
    loop {
        let k = state.next[partition];
        let Some(i) = k.checked_sub(state.first) else {
            return;
        };
        match state.windows.get(i as usize) {
            Some(w) if matches!(w.parts[partition], Part::Done) => state.next[partition] += 1,
            _ => return,
        }
    }
}

/// Drops the oldest windows once every part of them is done.
fn pop_done(state: &mut State) {
    while state.windows.front().is_some_and(|w| w.left == 0) {
        state.windows.pop_front();
        state.first += 1;
    }
}
