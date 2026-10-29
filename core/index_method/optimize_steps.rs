//! `OPTIMIZE INDEX` of an index method that optimizes in steps
//! ([`super::IndexMethodDefinition::optimize_in_steps`]). Outside a transaction the statement holds
//! no transaction of its own: it runs the method's work on a connection of the engine's beside the
//! statement's own, one step per transaction, `BEGIN CONCURRENT` under MVCC and `BEGIN IMMEDIATE`
//! under WAL, and yields between steps. Readers never wait for it and it pins no snapshot while it
//! runs; a writer waits for one step at most. A step that finds the write lock held reports busy,
//! and the statement's busy handler decides whether it waits; a step whose commit loses to a
//! concurrent transaction is done again. Dropping the statement drops the step connection, which
//! rolls back a step in flight. Inside a transaction the statement runs one `OPTIMIZE INDEX` there
//! instead, which does all the work in that transaction.

use crate::{
    io::Completion,
    sync::{atomic::Ordering, Arc},
    types::{IOCompletions, IOResult, IOResultOr},
    Connection, LimboError, Result, Statement, StepResult,
};

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

/// Steps in a row whose commit may lose to a concurrent transaction before the statement ends its
/// run, leaving the work due for the writers' own maintenance and the next `OPTIMIZE INDEX`.
const LOST_IN_A_ROW: usize = 16;

/// An `OPTIMIZE INDEX` of one index in steps, and where it is.
pub(crate) struct OptimizeSteps {
    /// The index's database on the statement's connection.
    database: usize,
    index: String,
    /// The step connection, opened by the first step.
    steps: Option<Arc<Connection>>,
    phase: Phase,
    first: bool,
    /// Whether the last step left work a further step can do.
    left: bool,
    lost: usize,
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
            left: false,
            lost: 0,
        }
    }

    fn steps(&self) -> Result<&Arc<Connection>> {
        self.steps.as_ref().ok_or_else(|| {
            LimboError::InternalError("OPTIMIZE INDEX step without its connection".into())
        })
    }

    /// Run the statement on from where it is: `Done` once the work is done, `IO` to wait on I/O or
    /// to yield between steps. On busy it stays where it is, to go on when stepped again.
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
                        Ok(IOResult::Done(())) => self.phase = Phase::Step(None),
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
                            steps
                                .index_method_optimize_left
                                .store(false, Ordering::Release);
                            steps.prepare(format!("OPTIMIZE INDEX {}", quoted(&self.index)))?
                        }
                    };
                    match drive(&mut step) {
                        Ok(IOResult::IO(io)) => {
                            self.phase = Phase::Step(Some(step));
                            return Ok(IOResult::IO(io));
                        }
                        Ok(IOResult::Done(())) => {
                            self.left = steps.index_method_optimize_left.load(Ordering::Acquire);
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
                            self.first = false;
                            self.lost = 0;
                            if !self.left {
                                return Ok(IOResult::Done(()));
                            }
                            self.phase = Phase::Begin(None);
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
            }
        }
    }
}
