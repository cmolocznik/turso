//! `OPTIMIZE INDEX` of an index method that optimizes in steps
//! ([`super::IndexMethodDefinition::optimize_in_steps`]). Outside a transaction the statement holds
//! no transaction of its own: it runs the method's work on a connection of the engine's beside the
//! statement's own, one step per transaction, `BEGIN CONCURRENT` under MVCC and `BEGIN IMMEDIATE`
//! under WAL, and yields between steps. Readers never wait for it and it pins no snapshot while it
//! runs; a writer waits for one step at most. A step that finds the write lock held reports busy,
//! and the statement's busy handler decides whether it waits; a step whose commit loses to a
//! concurrent transaction is done again, and one waiting for other transactions to end sleeps
//! before the next. Under WAL a step that refused another connection the write lock while it held
//! it lets that connection take the lock before the next step does. Each wait for the lock is one
//! for the statement's busy handler, its timeout bounding each wait rather than their sum over the
//! run. Dropping the statement drops the step connection, which rolls back a step in flight. Inside
//! a transaction the statement runs one `OPTIMIZE INDEX` there instead, which does all the work in
//! that transaction.

use super::OptimizeProgress;
use crate::{
    io::Completion,
    sync::{atomic::Ordering, Arc},
    types::{IOCompletions, IOResult, IOResultOr},
    Connection, LimboError, MonotonicInstant, Result, Statement, StepResult,
};
use std::time::Duration;

/// What an `OPTIMIZE INDEX` on a step connection asks of its index method, as the connection holds
/// it in [`Connection::index_method_optimize_step`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OptimizeStep {
    /// All the work there is.
    Whole,
    /// The first step of an `OPTIMIZE INDEX`, in a read transaction: it finds what work there is.
    First,
    /// A later step.
    Next,
}

impl OptimizeStep {
    pub(crate) fn of(connection: &Connection) -> Self {
        match connection
            .index_method_optimize_step
            .load(Ordering::Acquire)
        {
            1 => Self::First,
            2 => Self::Next,
            _ => Self::Whole,
        }
    }

    fn set(self, connection: &Connection) {
        let code = match self {
            Self::Whole => 0,
            Self::First => 1,
            Self::Next => 2,
        };
        connection
            .index_method_optimize_step
            .store(code, Ordering::Release);
    }
}

/// Record on a step connection what its step left.
pub(crate) fn record_progress(connection: &Connection, progress: OptimizeProgress) {
    let code = match progress {
        OptimizeProgress::Done => 0,
        OptimizeProgress::Moved => 1,
        OptimizeProgress::Waiting => 2,
        OptimizeProgress::Draining => 3,
    };
    connection
        .index_method_optimize_progress
        .store(code, Ordering::Release);
}

fn recorded_progress(connection: &Connection) -> OptimizeProgress {
    match connection
        .index_method_optimize_progress
        .load(Ordering::Acquire)
    {
        1 => OptimizeProgress::Moved,
        2 => OptimizeProgress::Waiting,
        3 => OptimizeProgress::Draining,
        _ => OptimizeProgress::Done,
    }
}

/// Steps in a row whose commit may lose to a concurrent transaction, or that may find all the work
/// left held by other transactions or wait for them to end, before the statement ends its run,
/// leaving the work due for the writers' own maintenance and the next `OPTIMIZE INDEX`.
const LOST_IN_A_ROW: usize = 16;
const WAITING_IN_A_ROW: usize = 64;

/// The longest a step that refused a writer the write lock waits for it to take the lock before
/// the next step takes it: the default busy handler's longest sleep, within which a writer
/// refused during the step tries again.
const GIVE_WAY: Duration = Duration::from_millis(100);

/// How long a step that gives way sleeps between looks at whether the writer has taken the lock.
const GIVE_WAY_POLL: Duration = Duration::from_millis(1);

/// An `OPTIMIZE INDEX` of one index in steps, and where it is.
pub(crate) struct OptimizeSteps {
    /// The index's database on the statement's connection.
    database: usize,
    index: String,
    /// The step connection, opened by the first step.
    steps: Option<Arc<Connection>>,
    phase: Phase,
    first: bool,
    /// What the last step left.
    left: OptimizeProgress,
    lost: usize,
    waiting: usize,
    /// The WAL's count of write-lock refusals when the step in flight took the write lock.
    refusals: u64,
    /// Whether a lock the statement waited for is no longer in its way, for its busy handler.
    lock_wait_over: bool,
    /// The sleep the statement asks of its caller before it is stepped again, while it gives way.
    wait: Option<Duration>,
}

enum Phase {
    Start,
    /// Inside the statement's transaction: one `OPTIMIZE INDEX` there.
    Whole(Option<Statement>),
    Begin(Option<Statement>),
    Step(Option<Statement>),
    Commit(Option<Statement>),
    /// Roll a failed step back, then report its error, or begin the next attempt when its commit
    /// lost to a concurrent transaction.
    Rollback(Option<Statement>, Option<Box<LimboError>>),
    /// Leave the write lock to a connection the last step refused it, until one takes it or
    /// `GIVE_WAY` has passed since `since`, sleeping `GIVE_WAY_POLL` between looks: `grants` is
    /// the WAL's count of write locks taken when the step committed.
    GiveWay {
        since: MonotonicInstant,
        grants: u64,
    },
}

/// Whether a failed commit lost to a concurrent transaction, so the step can be done again.
fn lost(error: &LimboError) -> bool {
    matches!(
        error,
        LimboError::WriteWriteConflict
            | LimboError::CommitDependencyAborted
            | LimboError::SchemaConflict
    )
}

/// The WAL's counts of write locks taken and refused, on `connection`'s main database, if it has
/// a WAL.
fn write_lock_turns(connection: &Connection) -> Option<(u64, u64)> {
    connection
        .get_pager()
        .wal
        .as_ref()
        .map(|wal| wal.write_lock_turns())
}

/// A name as an identifier in SQL.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Run `statement` on, handing back each wait: a wait for a lock as busy, for the caller's busy
/// handler to rule on.
fn drive(statement: &mut Statement) -> IOResultOr<()> {
    loop {
        match statement.step()? {
            StepResult::Done => return Ok(IOResult::Done(())),
            StepResult::Row => {}
            StepResult::IO | StepResult::Yield => {
                let io = statement
                    .take_io_completions()
                    .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                return Ok(IOResult::IO(io));
            }
            StepResult::Busy | StepResult::Sleep { .. } => return Err(Box::new(LimboError::Busy)),
            StepResult::Interrupt => return Err(Box::new(LimboError::Interrupt)),
        }
    }
}

impl OptimizeSteps {
    pub(crate) fn new(database: usize, index: &str) -> Self {
        Self {
            database,
            index: index.to_string(),
            steps: None,
            phase: Phase::Start,
            first: true,
            left: OptimizeProgress::Done,
            lost: 0,
            waiting: 0,
            refusals: 0,
            lock_wait_over: false,
            wait: None,
        }
    }

    /// Whether a lock the statement waited for has stopped being in its way since last asked.
    pub(crate) fn take_lock_wait_over(&mut self) -> bool {
        std::mem::take(&mut self.lock_wait_over)
    }

    /// The sleep the statement asks of its caller before it is stepped again, if any, since last
    /// asked.
    pub(crate) fn take_wait(&mut self) -> Option<Duration> {
        self.wait.take()
    }

    fn steps(&self) -> Result<&Arc<Connection>> {
        self.steps.as_ref().ok_or_else(|| {
            LimboError::InternalError("OPTIMIZE INDEX step without its connection".into())
        })
    }

    /// Run the statement on from where it is: `Done` once the work is done, `IO` to wait on I/O,
    /// to yield between steps, or, with [`Self::take_wait`] set, to sleep while it gives way. On
    /// busy it stays where it is, to go on when stepped again.
    pub(crate) fn step(&mut self, connection: &Arc<Connection>) -> IOResultOr<()> {
        loop {
            match std::mem::replace(&mut self.phase, Phase::Start) {
                Phase::Start => {
                    if !connection.get_auto_commit() {
                        let schema = connection
                            .namespace_name(self.database)
                            .unwrap_or_else(|| "main".to_string());
                        let sql =
                            format!("OPTIMIZE INDEX {}.{}", quoted(&schema), quoted(&self.index));
                        self.phase = Phase::Whole(Some(connection.prepare(sql)?));
                        continue;
                    }
                    if self.steps.is_none() {
                        self.steps = Some(connection.connect_beside(self.database)?);
                    }
                    self.phase = Phase::Begin(None);
                }
                Phase::Whole(mut statement) => {
                    let Some(whole) = statement.as_mut() else {
                        continue;
                    };
                    match drive(whole) {
                        Ok(IOResult::IO(io)) => {
                            self.phase = Phase::Whole(statement);
                            return Ok(IOResult::IO(io));
                        }
                        Ok(IOResult::Done(())) => return Ok(IOResult::Done(())),
                        Err(error) => return Err(error),
                    }
                }
                Phase::Begin(statement) => {
                    let steps = self.steps()?.clone();
                    let mut begin = match statement {
                        Some(begin) => begin,
                        // The first step only reads, so under WAL it takes no write lock.
                        None => steps.prepare(match (steps.mv_store().is_some(), self.first) {
                            (true, _) => "BEGIN CONCURRENT",
                            (false, true) => "BEGIN DEFERRED",
                            (false, false) => "BEGIN IMMEDIATE",
                        })?,
                    };
                    match drive(&mut begin) {
                        Ok(IOResult::IO(io)) => {
                            self.phase = Phase::Begin(Some(begin));
                            return Ok(IOResult::IO(io));
                        }
                        Ok(IOResult::Done(())) => {
                            self.lock_wait_over = true;
                            if let Some((_, refusals)) = write_lock_turns(&steps) {
                                self.refusals = refusals;
                            }
                            self.phase = Phase::Step(None);
                        }
                        Err(error) => {
                            // Nothing has begun: the next attempt begins afresh.
                            self.phase = Phase::Begin(None);
                            return Err(error);
                        }
                    }
                }
                Phase::Step(statement) => {
                    let steps = self.steps()?.clone();
                    let mut step = match statement {
                        Some(step) => step,
                        None => {
                            let ask = if self.first {
                                OptimizeStep::First
                            } else {
                                OptimizeStep::Next
                            };
                            ask.set(&steps);
                            record_progress(&steps, OptimizeProgress::Done);
                            steps.prepare(format!("OPTIMIZE INDEX {}", quoted(&self.index)))?
                        }
                    };
                    match drive(&mut step) {
                        Ok(IOResult::IO(io)) => {
                            self.phase = Phase::Step(Some(step));
                            return Ok(IOResult::IO(io));
                        }
                        Ok(IOResult::Done(())) => {
                            self.left = recorded_progress(&steps);
                            self.phase = Phase::Commit(None);
                        }
                        Err(error) => self.phase = Phase::Rollback(None, Some(error)),
                    }
                }
                Phase::Commit(statement) => {
                    let steps = self.steps()?.clone();
                    let mut commit = match statement {
                        Some(commit) => commit,
                        None => steps.prepare("COMMIT")?,
                    };
                    match drive(&mut commit) {
                        Ok(IOResult::IO(io)) => {
                            self.phase = Phase::Commit(Some(commit));
                            return Ok(IOResult::IO(io));
                        }
                        Ok(IOResult::Done(())) => {
                            self.lock_wait_over = true;
                            // Under WAL every step but the first held the write lock.
                            let held = steps.mv_store().is_none() && !self.first;
                            self.first = false;
                            self.lost = 0;
                            match self.left {
                                OptimizeProgress::Done => return Ok(IOResult::Done(())),
                                OptimizeProgress::Moved => self.waiting = 0,
                                OptimizeProgress::Waiting | OptimizeProgress::Draining => {
                                    self.waiting += 1;
                                    if self.waiting >= WAITING_IN_A_ROW {
                                        return Ok(IOResult::Done(()));
                                    }
                                }
                            }
                            // A step waiting for other transactions to end sleeps before the next,
                            // which would otherwise spin beside them.
                            if self.left == OptimizeProgress::Draining {
                                self.wait = Some(GIVE_WAY_POLL);
                            }
                            self.phase = match write_lock_turns(&steps) {
                                Some((grants, refusals)) if held && refusals > self.refusals => {
                                    Phase::GiveWay {
                                        since: steps.get_pager().io.current_time_monotonic(),
                                        grants,
                                    }
                                }
                                _ => Phase::Begin(None),
                            };
                            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
                        }
                        Err(error) if matches!(*error, LimboError::Busy) => {
                            self.phase = Phase::Commit(None);
                            return Err(error);
                        }
                        Err(error) if lost(&error) => {
                            self.lost += 1;
                            if self.lost >= LOST_IN_A_ROW {
                                return Ok(IOResult::Done(()));
                            }
                            self.phase = Phase::Rollback(None, None);
                        }
                        Err(error) => self.phase = Phase::Rollback(None, Some(error)),
                    }
                }
                Phase::Rollback(statement, error) => {
                    let steps = self.steps()?.clone();
                    if statement.is_some() || !steps.get_auto_commit() {
                        let mut rollback = match statement {
                            Some(rollback) => rollback,
                            None => steps.prepare("ROLLBACK")?,
                        };
                        if let IOResult::IO(io) = drive(&mut rollback)? {
                            self.phase = Phase::Rollback(Some(rollback), error);
                            return Ok(IOResult::IO(io));
                        }
                    }
                    if let Some(error) = error {
                        return Err(error);
                    }
                    self.phase = Phase::Begin(None);
                    return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
                }
                Phase::GiveWay { since, grants } => {
                    let steps = self.steps()?.clone();
                    let taken = write_lock_turns(&steps).is_none_or(|(now, _)| now > grants);
                    let waited = steps
                        .get_pager()
                        .io
                        .current_time_monotonic()
                        .duration_since(since);
                    if taken || waited >= GIVE_WAY {
                        self.phase = Phase::Begin(None);
                        continue;
                    }
                    self.phase = Phase::GiveWay { since, grants };
                    // Sleep between looks: stepped again at once, the statement spun a core for as
                    // long as the writer took to come back for the lock.
                    self.wait = Some(GIVE_WAY_POLL.min(GIVE_WAY - waited));
                    return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
                }
            }
        }
    }
}
