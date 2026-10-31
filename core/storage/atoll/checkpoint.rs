//! Continuous checkpoints (DESIGN.md §10; Haubenschild et al., SIGMOD 2020, §3.4).
//!
//! Each time a share of the log budget has been appended, a pass writes back the dirty pages of
//! the next shard of frames, round robin ([`Pass`]). After a pass, the redo point moves to the
//! lowest position of any shard, as far as the page store allows too: its metadata pages are
//! written up to there and the slots it freed before there are erased. A superblock naming the
//! new redo point is written, and log segments wholly before it are deleted. In between, it frees
//! the provisional pages operations gave up on ([`PageStore::orphaned`]).
//!
//! With S shards each taking 1/S of the budget, the redo point trails the log's end by about the
//! budget, plus what is appended while a pass runs.

use super::buffer::{BufferPool, Pass};
use super::control::{Control, Superblock};
use super::log::{Log, Lsn};
use super::store::PageStore;
use crate::io::Completion;
use crate::sync::{Arc, Mutex};
use crate::types::{IOCompletions, IOResult};
use crate::Result;

pub struct Checkpointer {
    pool: Arc<BufferPool>,
    store: Arc<PageStore>,
    control: Control,
    /// Log appended between passes: the budget split among the shards.
    increment: u64,
    state: Mutex<State>,
}

struct State {
    /// The superblock in charge.
    superblock: Superblock,
    next_shard: usize,
    /// Where the log ended when the last pass began.
    last_pass: Lsn,
    work: Work,
    /// Full checkpoints asked for so far, and how many of them are done.
    full_wanted: u64,
    full_done: u64,
}

enum Work {
    Idle,
    /// Passes running; with the full checkpoint they make, if they make one.
    Passes(Vec<Pass>, Option<u64>),
    Advance(Advance),
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
    Superblock(Superblock, Option<Completion>),
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
    ) -> Self {
        let last_pass = store.log().end();
        Self {
            increment: (budget / pool.shard_count() as u64).max(1),
            pool,
            store,
            control,
            state: Mutex::new(State {
                superblock,
                next_shard: 0,
                last_pass,
                work: Work::Idle,
                full_wanted: 0,
                full_done: 0,
            }),
        }
    }

    fn log(&self) -> &Arc<Log> {
        self.store.log()
    }

    /// The superblock in charge.
    pub fn superblock(&self) -> Superblock {
        self.state.lock().superblock.clone()
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
        let mut state = self.state.lock();
        state.full_wanted += 1;
        state.full_wanted
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
                    if self.full_done < self.full_wanted {
                        let passes = (0..cp.pool.shard_count())
                            .map(|s| cp.pool.pass(s))
                            .collect();
                        self.work = Work::Passes(passes, Some(self.full_wanted));
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
                                ..self.superblock.clone()
                            };
                            a.stage = Stage::Superblock(sb, None);
                        }
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    },
                    Stage::Superblock(sb, written) => match cp.control.step_write(sb, written)? {
                        IOResult::Done(()) => {
                            self.superblock = sb.clone();
                            let full = a.full;
                            cp.log().release_before(self.superblock.redo)?;
                            self.finish(full);
                        }
                        IOResult::IO(io) => return Ok(IOResult::IO(io)),
                    },
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
