//! Continuous checkpoints (DESIGN.md §10; Haubenschild et al., SIGMOD 2020, §3.4).
//!
//! Each time a share of the log budget has been appended, a pass writes back the dirty pages of
//! the next shard of frames, round robin ([`Pass`]). After a pass, the redo point moves to the
//! lowest position of any shard, as far as the page store allows too: its metadata pages are
//! written up to there and the slots it freed before there are erased. Then the writer table is
//! taken from the registry and appended, and once it is durable a superblock naming the new redo
//! point and the table is written ([`Writers`]). Log segments wholly before the redo point and
//! before the first group of every writer the table lists are deleted only once that superblock
//! is durable. In between, it frees the provisional pages operations gave up on
//! ([`PageStore::orphaned`]).
//!
//! With S shards each taking 1/S of the budget, the redo point trails the log's end by about the
//! budget, plus what is appended while a pass runs.

use super::arc_cell::{ArcCell, Releasing};
use super::buffer::{BufferPool, Pass};
use super::control::{Control, Superblock};
use super::epoch::Epochs;
use super::log::{encode_table, framed_len, Admitted, Header, Log, Lsn, Writer, Writers, WRITERS};
use super::store::PageStore;
use super::txn::Clock;
use crate::io::Completion;
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::Result;

pub struct Checkpointer {
    pool: Arc<BufferPool>,
    store: Arc<PageStore>,
    control: Control,
    clock: Arc<Clock>,
    /// Log appended between passes: the budget split among the shards.
    increment: u64,
    /// How much log recovery should replay at most.
    budget: u64,
    /// The writers that may need undo, which each superblock's table lists.
    writers: Arc<Writers>,
    /// The oldest writer the published table lists, and how far behind the redo point it holds
    /// the log, while one does.
    retention: ArcCell<Retention>,
    releasing_retention: Releasing<Retention>,
    /// Full checkpoints asked for so far.
    full_wanted: AtomicU64,
    /// The superblock in charge, as the checkpoint stepping it last wrote it, for anyone to read.
    published: ArcCell<Superblock>,
    releasing: Releasing<Superblock>,
    /// Owned by whoever steps the checkpoints, and only ever tried.
    state: Mutex<State>,
}

struct State {
    /// The superblock in charge.
    superblock: Superblock,
    next_shard: usize,
    /// Where the log ended when the last pass began.
    last_pass: Lsn,
    work: Work,
    /// Full checkpoints done.
    full_done: u64,
}

enum Work {
    Idle,
    /// Passes running; with the full checkpoint they make, if they make one.
    Passes(Vec<Pass>, Option<u64>),
    Advance(Advance),
}

/// A writer holding the log behind the redo point: segments from its first group on are kept for
/// its undo, however far back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub writer: Writer,
    /// Bytes of log kept behind the redo point for it.
    pub bytes: u64,
}

/// Moving the redo point once passes are done.
struct Advance {
    full: Option<u64>,
    /// The redo point the pool allows.
    floor: Lsn,
    /// Where the log ended when the passes were done: the remaps of their writes are before it.
    end: Lsn,
    stage: Stage,
}

enum Stage {
    Durable,
    Metadata,
    Erase,
    /// The writer table the superblock names, durable before it is.
    Table(Superblock, Table),
    /// With the oldest writer its table lists.
    Superblock(Superblock, Option<Completion>, Option<Writer>),
}

/// A writer table on its way to being durable.
struct Table {
    writers: Vec<Writer>,
    admitted: Option<Admitted>,
    /// Where its record ends, once appended.
    end: Option<Lsn>,
}

impl Checkpointer {
    /// A checkpointer for a database whose superblock in charge is `superblock`, keeping the log
    /// recovery replays to about `budget` bytes.
    pub fn new(
        pool: Arc<BufferPool>,
        store: Arc<PageStore>,
        control: Control,
        superblock: Superblock,
        budget: u64,
        clock: Arc<Clock>,
        writers: Arc<Writers>,
    ) -> Self {
        let last_pass = store.log().end();
        let epochs = Arc::new(Epochs::new());
        Self {
            increment: (budget / pool.shard_count() as u64).max(1),
            budget,
            writers,
            retention: ArcCell::default(),
            pool,
            store,
            control,
            clock,
            full_wanted: AtomicU64::new(0),
            published: ArcCell::new(Arc::new(superblock.clone())),
            releasing: Releasing::new(epochs.clone()),
            releasing_retention: Releasing::new(epochs),
            state: Mutex::new(State {
                superblock,
                next_shard: 0,
                last_pass,
                work: Work::Idle,
                full_done: 0,
            }),
        }
    }

    fn log(&self) -> &Arc<Log> {
        self.store.log()
    }

    /// The log, in bytes, the checkpointer keeps a recovery to replaying: the redo point trails
    /// the log's end by about this much.
    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// How far the redo point in charge trails the log's end, in bytes.
    pub fn lag(&self) -> u64 {
        self.log().end().saturating_sub(self.superblock().redo)
    }

    /// The oldest writer the superblock in charge lists, if it holds the log behind the redo
    /// point, and by how much.
    pub fn retention(&self) -> Option<Retention> {
        let step = self.releasing_retention.epochs().enter();
        self.retention.get(&step).map(|r| *r)
    }

    /// Publishes how far `oldest`, the oldest writer the superblock just published lists, holds
    /// the log behind `redo`, and says so as it first holds more than the budget: recovery reads
    /// that much more, for its undo.
    fn retained(&self, oldest: Option<Writer>, redo: Lsn) {
        let now = oldest
            .filter(|w| w.first.get() < redo)
            .map(|writer| Retention {
                writer,
                bytes: redo - writer.first.get(),
            });
        if let Some(r) = now.filter(|r| r.bytes > self.budget) {
            let before = self.retention();
            if before.is_none_or(|b| b.writer.txn != r.writer.txn || b.bytes <= self.budget) {
                tracing::warn!(
                    "transaction {} holds {} bytes of log behind the redo point for its undo, \
                     past the {} byte log budget",
                    r.writer.txn,
                    r.bytes,
                    self.budget
                );
            }
        }
        self.retention
            .replace(now.map(Arc::new), &self.releasing_retention);
    }

    /// The superblock in charge.
    pub fn superblock(&self) -> Superblock {
        let step = self.releasing.epochs().enter();
        (*self.published.get(&step).expect("a superblock in charge")).clone()
    }

    /// Does whatever is due: a pass once its share of the log has been appended, moving the redo
    /// point after one, and erasing what has become erasable. Done once nothing more is due now,
    /// or while someone else is stepping it.
    pub fn step(&self) -> Result<IOResult<()>> {
        match self.state.try_lock() {
            Some(mut state) => state.run(self),
            None => Ok(IOResult::Done(())),
        }
    }

    /// Asks for a full checkpoint: every shard passed, then the redo point moved as far as it
    /// goes. Step it with [`step_full`](Self::step_full).
    pub fn request_full(&self) -> u64 {
        self.full_wanted.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Done once the full checkpoint `ticket` names is done.
    pub fn step_full(&self, ticket: u64) -> Result<IOResult<()>> {
        let Some(mut state) = self.state.try_lock() else {
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        };
        if state.full_done < ticket {
            if let IOResult::IO(io) = state.run(self)? {
                return Ok(IOResult::IO(io));
            }
        }
        Ok(if state.full_done >= ticket {
            IOResult::Done(())
        } else {
            IOResult::IO(IOCompletions(Completion::new_yield()))
        })
    }
}

impl State {
    fn run(&mut self, cp: &Checkpointer) -> Result<IOResult<()>> {
        loop {
            match &mut self.work {
                Work::Idle => {
                    let wanted = cp.full_wanted.load(Ordering::Acquire);
                    if self.full_done < wanted {
                        let passes = (0..cp.pool.shard_count())
                            .map(|s| cp.pool.pass(s))
                            .collect();
                        self.work = Work::Passes(passes, Some(wanted));
                        continue;
                    }
                    let end = cp.log().end();
                    if end - self.last_pass >= cp.increment {
                        self.last_pass = end;
                        let shard = self.next_shard;
                        self.next_shard = (shard + 1) % cp.pool.shard_count();
                        self.work = Work::Passes(vec![cp.pool.pass(shard)], None);
                        continue;
                    }
                    // Pages an operation gave up on hold the redo point back until freed.
                    let mut orphans = cp.store.take_orphans().into_iter();
                    while let Some(pid) = orphans.next() {
                        if let IOResult::IO(io) = cp.pool.free_unreferenced(pid)? {
                            cp.store.orphaned(pid);
                            orphans.for_each(|p| cp.store.orphaned(p));
                            return Ok(IOResult::IO(io));
                        }
                    }
                    // Deleted data leaves the disk once its freeing is durable, not at the next
                    // checkpoint.
                    return cp.store.erase_freed();
                }
                Work::Passes(passes, full) => {
                    let mut wait = None;
                    let mut left = Vec::new();
                    for mut pass in std::mem::take(passes) {
                        if let IOResult::IO(io) = pass.step(&cp.pool)? {
                            wait.get_or_insert(io);
                            left.push(pass);
                        }
                    }
                    if let Some(io) = wait {
                        *passes = left;
                        return Ok(IOResult::IO(io));
                    }
                    self.work = Work::Advance(Advance {
                        full: *full,
                        floor: cp.pool.redo_floor(),
                        end: cp.log().end(),
                        stage: Stage::Durable,
                    });
                }
                Work::Advance(a) => match &mut a.stage {
                    Stage::Durable => match cp.log().wait_durable(a.end)? {
                        IOResult::Done(()) => a.stage = Stage::Metadata,
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    },
                    Stage::Metadata => match cp.store.write_metadata(a.floor)? {
                        IOResult::Done(()) => a.stage = Stage::Erase,
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    },
                    Stage::Erase => match cp.store.erase_freed()? {
                        IOResult::Done(()) => {
                            let redo = cp.store.redo_bound().map_or(a.floor, |b| b.min(a.floor));
                            if redo <= self.superblock.redo {
                                let full = a.full;
                                self.finish(full);
                                continue;
                            }
                            let sb = Superblock {
                                seq: self.superblock.seq + 1,
                                redo,
                                heights: cp.store.heights(),
                                // Read after every change before the redo point was logged.
                                clock: cp.clock.now(),
                                writers: None,
                                // A gap the redo point passed is read past by no one.
                                continuations: self
                                    .superblock
                                    .continuations
                                    .iter()
                                    .copied()
                                    .filter(|&(_, resumed)| resumed > redo)
                                    .collect(),
                                ..self.superblock.clone()
                            };
                            // Taken once the redo point is fixed: every writer with a group
                            // before it is in the registry, and none whose Commit or End is.
                            let table = Table {
                                writers: cp.writers.snapshot(),
                                admitted: None,
                                end: None,
                            };
                            a.stage = Stage::Table(sb, table);
                        }
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    },
                    Stage::Table(sb, table) => {
                        if !table.writers.is_empty() && table.end.is_none() {
                            let payload = encode_table(&table.writers);
                            if table.admitted.is_none() {
                                match cp.log().admit(framed_len(payload.len()))? {
                                    IOResult::Done(admitted) => table.admitted = Some(admitted),
                                    IOResult::IO(io) => return Ok(IOResult::IO(io)),
                                }
                            }
                            let header = Header {
                                kind: WRITERS,
                                flags: 0,
                                txn: 0,
                                prev: None,
                            };
                            let admitted = table.admitted.as_mut().expect("admitted");
                            let (start, end) =
                                cp.log().append_with(admitted, &header, &[&payload], |_| {});
                            sb.writers = Some(start);
                            table.end = Some(end);
                        }
                        if let Some(end) = table.end {
                            if let IOResult::IO(io) = cp.log().wait_durable(end)? {
                                return Ok(IOResult::IO(io));
                            }
                        }
                        let oldest = table.writers.iter().copied().min_by_key(|w| w.first);
                        let sb = sb.clone();
                        a.stage = Stage::Superblock(sb, None, oldest);
                    }
                    Stage::Superblock(sb, written, oldest) => {
                        match cp.control.step_write(sb, written)? {
                            IOResult::Done(()) => {
                                self.superblock = sb.clone();
                                cp.published
                                    .replace(Some(Arc::new(sb.clone())), &cp.releasing);
                                let full = a.full;
                                let redo = self.superblock.redo;
                                // Undo needs every group of a writer the table lists.
                                let keep = oldest.map_or(redo, |w| w.first.get().min(redo));
                                cp.log().release_before(keep)?;
                                cp.retained(*oldest, redo);
                                self.finish(full);
                            }
                            IOResult::IO(io) => return Ok(IOResult::IO(io)),
                        }
                    }
                },
            }
        }
    }

    fn finish(&mut self, full: Option<u64>) {
        if let Some(ticket) = full {
            self.full_done = self.full_done.max(ticket);
        }
        self.work = Work::Idle;
    }
}
