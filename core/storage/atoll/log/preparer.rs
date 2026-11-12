//! The log's segment preparer (DESIGN.md 22.9 item 2, 22.16 step 8): section 7.4's preparation
//! as one executor task per open log, the only producer of every step of it. A segment is a spare
//! renamed into place, or a fresh file created, allocated, written full of zeros one write at a
//! time and synced; then its directory is synced, the prepared end published, admission woken,
//! the next segment looked for, and the flush that padding waited on retried.
//!
//! One unit a step: a spare's rename with its directory sync and publication; a fresh file's
//! create and allocation; one zero-fill write submitted; the data sync submitted; the finish.
//! The requests' callbacks only record a failure; the task reads each request's end at its next
//! step, woken as the request retires, so its one place in flight is held until its backend lets
//! the request go, whatever its outcome, never only until its result is published.
//!
//! It is due when admission comes within a segment of the prepared end, or when a flush's padding
//! needs a segment (the word, [`preparation`](super::preparation)), and relief through the tickets
//! of the writers waiting on it. Its failure fails the log: every durable and admission waiter is
//! told, and the task ends.

use super::waits::Registration;
use super::{io_failure, retirement, whole, Log};
use crate::io::{Completion, File};
use crate::storage::atoll::executor::{self, Cx, Signal, Step, Task, TaskRef, Watched};
use crate::sync::{Arc, Weak};
use crate::Result;

/// A caller's wait on its log, held across its calls of [`Log::admit`] and
/// [`Log::wait_durable`] (DESIGN.md 22.4): while the wait stands on the log's preparer, the
/// caller's demand on it, of the relief class, raising it before the jobs a writer does not wait
/// for. A call that ends the wait lets it go, and so does dropping the wait (the operation done,
/// cancelled, or its connection gone). A wait made inside a step holds no demand: it names the
/// preparer instead ([`on`](Self::on)), and the step hands its executor that wait, its own class
/// riding on it.
#[derive(Default)]
pub struct LogWait {
    pub(super) demand: Option<executor::Ticket>,
    pub(super) on: Option<TaskRef>,
    /// Its registration among those waiting for durability, and the mark it waits for, so a wait
    /// called again registers no second one. Admitters share one registration and need none.
    pub(super) durability: Option<(u64, Registration)>,
}

impl LogWait {
    /// The preparer a step's wait stands on, for the step to hand its executor (`Step::WaitOn`).
    pub(crate) fn on(&mut self) -> Option<TaskRef> {
        self.on.take()
    }

    /// The wait ended: its demand let go and its registration given up, for the waits' next take
    /// to drop.
    pub(super) fn end(&mut self) {
        self.demand = None;
        self.on = None;
        if let Some((_, registration)) = self.durability.take() {
            registration.give_up();
        }
    }
}

impl Drop for LogWait {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.end();
    }
}

/// What the step of the preparer a caller claimed on this thread came to (DESIGN.md 22.7).
pub(super) enum Told {
    /// More now.
    Again,
    /// Nothing until this request's delivery.
    Wait(Completion),
    /// Nothing due, or the log failed.
    Rest,
}

crate::thread::thread_local! {
    /// The claim of a preparer this thread's caller is making: `Some(None)` once made, then what
    /// the step it runs came to.
    static CLAIMING: std::cell::RefCell<Option<Option<Told>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
crate::thread::thread_local! {
    /// Named claims of a preparer made on this thread.
    pub(super) static CLAIMS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Steps an admission granted on this thread ran that left the preparer more to do, which the
    /// admission did not wait on.
    pub(super) static GRANTED_MORE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Set by a test: run once inside the next preparer unit on this thread.
    pub(super) static DURING_UNIT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// A caller's named claim of `preparer` (DESIGN.md 22.7): one step on this thread, unless another
/// caller steps it or its task is over; what the step came to, `None` when it ran none.
pub(super) fn claim(preparer: &TaskRef) -> Option<Told> {
    #[cfg(test)]
    CLAIMS.with(|n| n.set(n.get() + 1));
    CLAIMING.with(|c| *c.borrow_mut() = Some(None));
    let ran = preparer.run_once();
    let told = CLAIMING.with(|c| c.borrow_mut().take()).flatten();
    if !ran {
        return None;
    }
    Some(told.expect("a claimed step of the preparer did not tell its claimant"))
}

/// Tells a caller whose claim runs the step on this thread what it came to; a step on a worker
/// has no claimant to tell.
fn tell_claimant(told: impl FnOnce() -> Told) {
    CLAIMING.with(|c| {
        let mut c = c.borrow_mut();
        if matches!(*c, Some(None)) {
            *c = Some(Some(told()));
        }
    });
}

/// Where the segment being prepared stands, the task's own.
pub(super) enum Preparing {
    /// No preparation under way.
    Rest,
    /// Segment `number`, created and allocated: zeros written up to `at`, and the write in flight
    /// with its length, if one is, by the wait its retirement finishes.
    Filling {
        number: u64,
        file: Arc<dyn File>,
        at: u64,
        write: Option<(Completion, u64)>,
    },
    /// Segment `number`'s data sync in flight, by the wait its retirement finishes.
    Syncing { number: u64, sync: Completion },
}

/// What one unit of the preparation came to.
pub(super) enum Unit {
    /// More now.
    Again,
    /// Nothing until this request's delivery.
    Wait(Completion),
    /// Nothing due: no segment needed, and enough prepared ahead.
    Rest,
    /// The log failed: nothing more is prepared.
    Failed,
}

/// The preparer as its log's executor task. It holds the log weakly: nothing the executor holds
/// keeps the log alive (DESIGN.md 22.3).
pub(super) struct PreparerTask {
    log: Weak<Log>,
    due: Arc<Signal>,
    state: Preparing,
    /// A turn of the log's flush asked for during the turn its finish took, which its next unit
    /// takes first, as the turn's holder (`Solo`).
    turn_owed: bool,
}

impl PreparerTask {
    pub(super) fn new(log: Weak<Log>, due: Arc<Signal>) -> Self {
        Self {
            log,
            due,
            state: Preparing::Rest,
            turn_owed: false,
        }
    }
}

impl Task for PreparerTask {
    fn step(&mut self, cx: &Cx) -> Result<Step> {
        let Some(log) = self.log.upgrade() else {
            return Ok(Step::Done);
        };
        // Read before the look: a trigger after it moves the count, keeping the task's level.
        let read = self.due.read();
        // A unit of a job's work (DESIGN.md 22.1): no park, its visits counted.
        let unit = {
            let _unit = crate::storage::atoll::unit::Unit::enter();
            #[cfg(test)]
            if let Some(during) = DURING_UNIT.with(|d| d.borrow_mut().take()) {
                during();
            }
            if self.turn_owed {
                self.turn_owed = log.flush_one_turn();
                Unit::Again
            } else {
                log.prepare_unit(&mut self.state, &mut self.turn_owed)
            }
        };
        let step = match unit {
            Unit::Again => Step::Again,
            Unit::Wait(c) if c.finished() => Step::Again,
            Unit::Wait(c) => Step::Wait(c),
            Unit::Rest => {
                cx.clear_due(&self.due, read);
                Step::Idle(Watched::of(&[(&self.due, read)]))
            }
            Unit::Failed => Step::Done,
        };
        tell_claimant(|| match &step {
            Step::Again => Told::Again,
            Step::Wait(c) => Told::Wait(c.clone()),
            _ => Told::Rest,
        });
        Ok(step)
    }

    /// The database closing (DESIGN.md 22.12 step 3): a segment not yet published as prepared is
    /// left for the next open to prepare again, and its requests retire with the database's.
    fn abandon(&mut self) {
        self.state = Preparing::Rest;
    }
}

impl Log {
    /// One unit of the preparation, the preparer's: see the module. `turn_owed` is set when the
    /// finish's turn of the flush owes another, for the next unit.
    pub(super) fn prepare_unit(
        self: &Arc<Self>,
        state: &mut Preparing,
        turn_owed: &mut bool,
    ) -> Unit {
        // A failed log begins and finishes nothing: its waiters are told by the failure.
        if self.failure().is_some() {
            return Unit::Failed;
        }
        match state {
            Preparing::Rest => self.start_unit(state, turn_owed),
            Preparing::Filling {
                number,
                file,
                at,
                write,
            } => {
                if let Some((w, len)) = write.take() {
                    if !w.finished() {
                        *write = Some((w.clone(), len));
                        return Unit::Wait(w);
                    }
                    // Its callback recorded a failure, read here.
                    if self.failure().is_some() {
                        return Unit::Failed;
                    }
                    *at += len;
                }
                if *at == self.segments.size {
                    let me = self.held();
                    let sync = Completion::new_sync(move |res| {
                        if let (Some(me), Err(e)) = (me.upgrade(), res) {
                            me.fail(e);
                        }
                    });
                    if let Err(e) = file.sync_data(sync.clone()) {
                        self.fail(io_failure(&e));
                        return Unit::Failed;
                    }
                    let retired = retirement(&sync);
                    let number = *number;
                    *state = Preparing::Syncing {
                        number,
                        sync: retired.clone(),
                    };
                    return Unit::Wait(retired);
                }
                let len = (self.segments.size - *at).min(self.zeros.len() as u64);
                let me = self.held();
                let c = Completion::new_write(move |res| {
                    if let (Some(me), Err(e)) = (me.upgrade(), whole(len as usize, res)) {
                        me.fail(e);
                    }
                });
                if let Err(e) = file.pwrite(*at, self.zeros.region(0, len as usize), c.clone()) {
                    self.fail(io_failure(&e));
                    return Unit::Failed;
                }
                let retired = retirement(&c);
                *write = Some((retired.clone(), len));
                Unit::Wait(retired)
            }
            Preparing::Syncing { number, sync } => {
                if !sync.finished() {
                    return Unit::Wait(sync.clone());
                }
                // Its callback recorded a failure, read here.
                if self.failure().is_some() {
                    return Unit::Failed;
                }
                let number = *number;
                *state = Preparing::Rest;
                self.finish_unit(number, turn_owed)
            }
        }
    }

    /// Takes the word, if a segment is needed or too little is prepared ahead, and starts the
    /// segment past the prepared end: a spare renamed into place, finished at once, since it is
    /// whole and synced already (section 7.4); or a fresh file created and allocated, its zeros
    /// then written a unit at a time.
    fn start_unit(self: &Arc<Self>, state: &mut Preparing, turn_owed: &mut bool) -> Unit {
        if !self.preparation.take(|| self.short_of_segments()) {
            return Unit::Rest;
        }
        let number = self.segments.number(self.room.prepared_end());
        match self.segments.reuse(number) {
            Ok(Some(_)) => return self.finish_unit(number, turn_owed),
            Ok(None) => {}
            Err(e) => {
                self.fail(io_failure(&e));
                return Unit::Failed;
            }
        }
        let file = match self.segments.create(number) {
            Ok(file) => file,
            Err(e) => {
                self.fail(io_failure(&e));
                return Unit::Failed;
            }
        };
        // Read again once each synchronous call returns: a request may have failed meanwhile.
        if self.failure().is_some() {
            return Unit::Failed;
        }
        // Space reserved where the backend can; one that cannot still takes the zeros written next.
        let _ = file.allocate(0, self.segments.size);
        if self.failure().is_some() {
            return Unit::Failed;
        }
        *state = Preparing::Filling {
            number,
            file,
            at: 0,
            write: None,
        };
        Unit::Again
    }

    /// Finishes segment `number`: syncs the segments' directory, publishes the prepared end and
    /// lets the word go, then wakes the admitters and takes one turn of the flush, since padding
    /// may have waited for the segment, a turn owed left to the next unit. The next segment is
    /// looked for at the preparer's next unit.
    fn finish_unit(self: &Arc<Self>, number: u64, turn_owed: &mut bool) -> Unit {
        if let Err(e) = self.io.sync_directory(&self.segments.directory()) {
            self.fail(io_failure(&e));
            return Unit::Failed;
        }
        // Read again once the synchronous call returns: a request may have failed meanwhile.
        if self.failure().is_some() {
            return Unit::Failed;
        }
        self.room.set_prepared_end(self.segments.start(number + 1));
        self.preparation.done();
        self.wake_admitters();
        *turn_owed = self.flush_one_turn();
        Unit::Again
    }
}
