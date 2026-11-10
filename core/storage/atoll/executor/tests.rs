//! The executor's core on test jobs (DESIGN.md 22.15, proofs 1, 2 and 5, and the workers of 22.7).

use super::*;

#[cfg(not(atoll_loom))]
mod native {
    use super::*;
    use crate::io::crash::CrashIO;
    use crate::io::{FileSyncType, OpenFlags};
    use crate::storage::atoll::locks::Mutex as Lock;
    use crate::storage::atoll::requests::{Counted, Requests};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as O};
    use std::sync::mpsc;
    use std::time::Duration;

    const BOUND: Duration = Duration::from_secs(20);

    /// A task that records each step under its name and asks again `again` times, then is done.
    struct Recorded {
        name: &'static str,
        again: usize,
        log: Arc<Lock<Vec<&'static str>>>,
    }

    impl Task for Recorded {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            self.log.lock().push(self.name);
            if self.again == 0 {
                return Ok(Step::Done);
            }
            self.again -= 1;
            Ok(Step::Again)
        }
    }

    fn recorded(
        name: &'static str,
        again: usize,
        log: &Arc<Lock<Vec<&'static str>>>,
    ) -> Box<dyn Task> {
        Box::new(Recorded {
            name,
            again,
            log: log.clone(),
        })
    }

    fn executor(workers: usize) -> Executor {
        Executor::new(Arc::new(CrashIO::new(1)), workers).unwrap()
    }

    /// Runners take tasks strictly by class and first in first out within one: a task asking
    /// again goes to the back of its class, and a task linked into a higher class is the next
    /// claimed (22.4; proof 5; mutants: requeue at the head; a lower class's turn first).
    #[test]
    fn claims_are_strictly_by_class_and_first_in_first_out_within_one() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        ex.spawn(Class::Maintenance, recorded("m1", 0, &log));
        ex.spawn(Class::Due, recorded("d1", 2, &log));
        ex.spawn(Class::Due, recorded("d2", 1, &log));
        ex.spawn(Class::Relief, recorded("r1", 1, &log));
        // r1 twice; then the due tasks taking turns.
        for _ in 0..3 {
            assert!(ex.run_one());
        }
        // A relief task linked now is the next claimed.
        ex.spawn(Class::Relief, recorded("r2", 0, &log));
        while ex.run_one() {}
        ex.close();
        // Read out before asserting: no panic while an Atoll lock is held (22.11).
        let claimed = log.lock().clone();
        assert_eq!(
            claimed,
            ["r1", "r1", "d1", "r2", "d2", "d1", "d2", "d1", "m1"]
        );
    }

    /// A task that wakes itself during its step and then waits: the wake marks it, so its step's
    /// end queues it again rather than resting it.
    struct WakesItself {
        me: Arc<Lock<Option<TaskRef>>>,
        steps: usize,
    }

    impl Task for WakesItself {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            self.steps += 1;
            if self.steps > 1 {
                return Ok(Step::Done);
            }
            self.me.lock().as_ref().unwrap().wake();
            Ok(Step::Wait(Completion::new_sync(|_| {})))
        }
    }

    /// A wake while the task runs is never lost and never doubles it: its step's end queues it
    /// once (22.3; proof 2; mutant: the end of a step resting the task over the wake).
    #[test]
    fn a_wake_during_a_step_queues_the_task_again_once() {
        let ex = executor(0);
        let me = Arc::new(Lock::new(None));
        let task = ex.spawn(
            Class::Due,
            Box::new(WakesItself {
                me: me.clone(),
                steps: 0,
            }),
        );
        *me.lock() = Some(task.clone());
        // Read out before asserting: an executor dropped while a panic unwinds ends the process
        // (22.11), so every assertion comes after close.
        let claims = [ex.run_one(), ex.run_one()];
        let done = task.is_done();
        let again = ex.run_one();
        ex.close();
        assert_eq!(
            claims,
            [true, true],
            "the first step, then the one its wake queued"
        );
        assert!(done);
        assert!(!again, "queued once, not twice");
    }

    /// A done task stays done: a late wake, as a waker left on a completion gives, queues
    /// nothing (22.3; mutant: a done task's end resting it, so the wake brings it back).
    #[test]
    fn a_done_task_is_never_stepped_again() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let task = ex.spawn(Class::Due, recorded("once", 0, &log));
        assert!(ex.run_one());
        assert!(task.is_done());
        task.wake();
        let again = ex.run_one();
        ex.close();
        let stepped = log.lock().clone();
        assert!(!again, "a done task queued by a late wake");
        assert_eq!(stepped, ["once"]);
    }

    /// Waits on `c`, then is done, saying so.
    struct WaitsOn {
        c: Option<Completion>,
        done: mpsc::Sender<()>,
    }

    impl Task for WaitsOn {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            match self.c.take() {
                Some(c) => Ok(Step::Wait(c)),
                None => {
                    let _ = self.done.send(());
                    Ok(Step::Done)
                }
            }
        }
    }

    /// A task waiting on a completion another thread finishes runs again on a worker once it
    /// finishes, with nothing else waking it.
    #[test]
    fn a_worker_runs_a_task_again_once_what_it_waits_on_finishes() {
        let ex = executor(2);
        let c = Completion::new_sync(|_| {});
        let (done_tx, done_rx) = mpsc::channel();
        let task = ex.spawn(
            Class::Due,
            Box::new(WaitsOn {
                c: Some(c.clone()),
                done: done_tx,
            }),
        );
        // Long enough for a worker to step it and rest it.
        std::thread::sleep(Duration::from_millis(50));
        let rested = !task.is_done();
        c.complete(0);
        let ran = done_rx.recv_timeout(BOUND).is_ok();
        ex.close();
        assert!(rested);
        assert!(ran, "the task ran again");
    }

    /// Syncs a file of a pumped backend and waits on it, then is done.
    struct Syncs {
        file: Arc<dyn crate::io::File>,
        sync: Option<Completion>,
        done: mpsc::Sender<()>,
    }

    impl Task for Syncs {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            if self.sync.is_none() {
                let c = self
                    .file
                    .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?;
                self.sync = Some(c.clone());
                return Ok(Step::Wait(c));
            }
            let _ = self.done.send(());
            Ok(Step::Done)
        }
    }

    /// On a pumped backend the workers deliver what tasks submit, with nothing else stepping it,
    /// and once it holds nothing they park: no step taken over a quiet window. Closed, a worker
    /// takes back the registration its last look left (22.5, 22.7; mutants: a worker that never
    /// steps the backend, so the task never ends; one that steps an empty backend without parking;
    /// one that leaves its registration behind, the auditor's P2-X69-2).
    #[test]
    fn workers_deliver_a_pumped_backends_requests_and_park_once_it_is_empty() {
        let crash = Arc::new(CrashIO::new(3));
        let io = crate::io::wait_tests::StepCounting::new(crash.clone());
        let ex = Executor::new(io.clone(), 1).unwrap();
        let file = io.open_file("/db/f", OpenFlags::Create, false).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        ex.spawn(
            Class::Due,
            Box::new(Syncs {
                file,
                sync: None,
                done: done_tx,
            }),
        );
        let delivered = done_rx.recv_timeout(BOUND).is_ok();
        // Long enough for the worker to find nothing and park.
        std::thread::sleep(Duration::from_millis(50));
        let before = io.steps();
        std::thread::sleep(Duration::from_millis(100));
        let quiet = (before, io.steps());
        let parked = (crash.in_flight(), crash.registered());
        ex.close();
        let left = crash.registered();
        assert!(delivered, "the worker delivered the task's sync");
        assert_eq!(
            quiet.1, quiet.0,
            "steps taken by a worker with nothing to do"
        );
        assert_eq!(parked, (0, 1), "(in flight, registrations) while parked");
        assert_eq!(left, 0, "a registration left behind by a closed worker");
    }

    /// A task that asks again until its count of steps reaches `until`, and flags a step that
    /// overlaps another of its own.
    struct Forever {
        steps: Arc<AtomicUsize>,
        stepping: Arc<AtomicBool>,
        overlapped: Arc<AtomicBool>,
        until: usize,
    }

    impl Task for Forever {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            if self.stepping.swap(true, O::AcqRel) {
                self.overlapped.store(true, O::Release);
            }
            let n = self.steps.fetch_add(1, O::AcqRel) + 1;
            self.stepping.store(false, O::Release);
            Ok(if n == self.until {
                Step::Done
            } else {
                Step::Again
            })
        }
    }

    /// Many workers, many tasks, each asking again a fixed number of times while wakes come from
    /// outside: every task is stepped exactly that many times and never by two runners at once
    /// (22.3; proof 2 under real threads; mutant: a wake that links a task already queued, which
    /// two workers then step).
    #[test]
    fn workers_step_every_task_its_steps_and_never_two_at_once() {
        const TASKS: usize = 64;
        const STEPS: usize = 100;
        let ex = executor(8);
        let overlapped = Arc::new(AtomicBool::new(false));
        let mut all = Vec::new();
        for n in 0..TASKS {
            let steps = Arc::new(AtomicUsize::new(0));
            let task = ex.spawn(
                [Class::Relief, Class::Due, Class::Maintenance][n % CLASSES],
                Box::new(Forever {
                    steps: steps.clone(),
                    stepping: Arc::new(AtomicBool::new(false)),
                    overlapped: overlapped.clone(),
                    until: STEPS,
                }),
            );
            task.wake();
            all.push((task, steps));
        }
        let deadline = std::time::Instant::now() + BOUND;
        while !all.iter().all(|(t, _)| t.is_done()) && std::time::Instant::now() < deadline {
            for (t, _) in &all {
                t.wake();
            }
            std::thread::yield_now();
        }
        ex.close();
        assert!(all.iter().all(|(t, _)| t.is_done()), "every task ended");
        assert!(
            all.iter().all(|(_, s)| s.load(O::Acquire) == STEPS),
            "steps per task: {:?}",
            all.iter()
                .map(|(_, s)| s.load(O::Acquire))
                .collect::<Vec<_>>()
        );
        assert!(
            !overlapped.load(O::Acquire),
            "a task stepped by two runners at once"
        );
    }

    /// Close stops every task: its workers are joined, a task asking again for ever is stepped no
    /// more, and a wake after close queues nothing.
    #[test]
    fn close_stops_every_task_and_joins_its_workers() {
        let ex = executor(4);
        let steps = Arc::new(AtomicUsize::new(0));
        let task = ex.spawn(
            Class::Maintenance,
            Box::new(Forever {
                steps: steps.clone(),
                stepping: Arc::new(AtomicBool::new(false)),
                overlapped: Arc::new(AtomicBool::new(false)),
                until: usize::MAX,
            }),
        );
        while steps.load(O::Acquire) < 10 {
            std::thread::yield_now();
        }
        ex.close();
        let after = steps.load(O::Acquire);
        task.wake();
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(steps.load(O::Acquire), after, "a step after close");
        assert!(!task.is_done(), "closed, not done");
    }

    /// An executor runs over no pumped backend whose steps cannot say what they found: its workers
    /// would step it for ever (DESIGN.md 22.5; mutant: the refusal gone).
    #[test]
    fn an_executor_refuses_a_pumped_backend_whose_steps_cannot_say() {
        let made = Executor::new(Arc::new(Undeclared(Arc::new(CrashIO::new(4)))), 2);
        let refused = made.is_err();
        // Taken, its workers park for good on a backend that never wakes them; nothing closes it.
        if let Ok(ex) = made {
            std::mem::forget(ex);
        }
        assert!(
            refused,
            "an executor took a pumped backend whose steps cannot say"
        );
    }

    /// A pumped backend that cannot say what its steps find, read by the refusal before any
    /// worker steps it. Its steps run nothing and answer Empty with nothing registered, breaking
    /// the contract on purpose: an executor that took it parks its workers for good, where one
    /// stepping a backend that answers Unsaid would abort the process.
    struct Undeclared(Arc<CrashIO>);

    impl crate::io::Clock for Undeclared {
        fn current_time_monotonic(&self) -> crate::io::clock::MonotonicInstant {
            self.0.current_time_monotonic()
        }

        fn current_time_wall_clock(&self) -> crate::io::clock::WallClockInstant {
            self.0.current_time_wall_clock()
        }
    }

    impl crate::io::IO for Undeclared {
        fn open_file(
            &self,
            path: &str,
            flags: OpenFlags,
            direct: bool,
        ) -> Result<Arc<dyn crate::io::File>> {
            self.0.open_file(path, flags, direct)
        }

        fn remove_file(&self, path: &str) -> Result<()> {
            self.0.remove_file(path)
        }

        fn step(&self) -> Result<()> {
            Ok(())
        }

        fn try_step(
            &self,
            _key: crate::io::ReadyKey,
            _ready: &std::task::Waker,
        ) -> Result<crate::io::Stepped> {
            Ok(crate::io::Stepped::Empty)
        }
    }

    /// Submits a sync whose callback submits the next, until `stop`.
    fn resubmit(file: Arc<dyn crate::io::File>, stop: Arc<AtomicBool>) {
        let (next, again) = (file.clone(), stop.clone());
        let c = Completion::new_sync(move |_| {
            if !again.load(O::Acquire) {
                resubmit(next.clone(), again.clone());
            }
        });
        // Delivered and resubmitted by the callback above; nothing waits on it here.
        drop(file.sync(c, FileSyncType::Fsync).unwrap());
    }

    /// Close joins workers whose backend never runs dry, so that they never come to park: each
    /// pass of a worker's loop reads close (mutant: the workers read close only where they would
    /// park, and close waits for them for ever).
    #[test]
    fn close_joins_workers_while_their_backend_keeps_delivering() {
        let crash = Arc::new(CrashIO::new(4));
        let ex = Executor::new(crash.clone(), 2).unwrap();
        let file = crash.open_file("/db/f", OpenFlags::Create, false).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        resubmit(file, stop.clone());
        // Long enough for the workers to be delivering.
        std::thread::sleep(Duration::from_millis(20));
        let (closed_tx, closed_rx) = mpsc::channel();
        std::thread::spawn(move || {
            ex.close();
            let _ = closed_tx.send(());
        });
        let closed = closed_rx.recv_timeout(BOUND).is_ok();
        stop.store(true, O::Release);
        while crash.in_flight() > 0 {
            crash.step().unwrap();
        }
        assert!(closed, "close never joined its workers");
    }

    /// Watches three signals: each step reads their counts, then looks at `ready`; unmet, it
    /// says idle on the counts it read; met, it is done.
    struct Watches {
        signals: [Arc<Signal>; 3],
        ready: Arc<AtomicBool>,
    }

    impl Task for Watches {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let read: Vec<u64> = self.signals.iter().map(|s| s.read()).collect();
            if self.ready.load(O::Acquire) {
                return Ok(Step::Done);
            }
            let watched: Vec<(&Arc<Signal>, u64)> = self.signals.iter().zip(read).collect();
            Ok(Step::Idle(Watched::of(&watched)))
        }
    }

    fn watching(ex: &Executor) -> (TaskRef, [Arc<Signal>; 3], Arc<AtomicBool>) {
        let signals = [
            Arc::new(Signal::default()),
            Arc::new(Signal::default()),
            Arc::new(Signal::default()),
        ];
        let ready = Arc::new(AtomicBool::new(false));
        let task = ex.spawn_watching(
            Class::Due,
            Box::new(Watches {
                signals: signals.clone(),
                ready: ready.clone(),
            }),
            &[&signals[0], &signals[1], &signals[2]],
        );
        (task, signals, ready)
    }

    /// A notify between a task's look and its registration moves the count the registration
    /// reads again, so the task is queued at once (22.2; proof 1; mutant: no second read, which
    /// rests the task over the notify, never woken).
    #[test]
    fn a_notify_between_the_look_and_the_registration_queues_the_task_at_once() {
        let ex = executor(0);
        let (task, signals, ready) = watching(&ex);
        let (s, r) = (signals[1].clone(), ready.clone());
        crate::race::at("executor.registering", move || {
            r.store(true, O::Release);
            s.notify();
        });
        let claims = [ex.run_one(), ex.run_one()];
        let done = task.is_done();
        ex.close();
        assert_eq!(
            claims,
            [true, true],
            "the first look, then queued again by the notify it missed"
        );
        assert!(done);
    }

    /// A notify after a task's registration finds it listed and wakes it (22.2; proof 1; mutant:
    /// a notify that moves the count and wakes no one).
    #[test]
    fn a_notify_after_the_registration_wakes_the_listed_task() {
        let ex = executor(0);
        let (task, signals, ready) = watching(&ex);
        let (s, r) = (signals[2].clone(), ready.clone());
        crate::race::at("executor.registered", move || {
            r.store(true, O::Release);
            s.notify();
        });
        let claims = [ex.run_one(), ex.run_one()];
        let done = task.is_done();
        ex.close();
        assert_eq!(
            claims,
            [true, true],
            "the first look, then woken from the list"
        );
        assert!(done);
    }

    /// A task waits on any of its signals: each, notified alone after the task rested, wakes it;
    /// a task resting again is listed once on each, and a task done is on none (22.2, proof 1's
    /// wait-any).
    #[test]
    fn each_signal_watched_wakes_a_resting_task_alone_and_lists_hold_it_once() {
        for which in 0..3 {
            let ex = executor(0);
            let (task, signals, ready) = watching(&ex);
            let listed = |signals: &[Arc<Signal>; 3]| signals.each_ref().map(|s| s.listed());
            let first = [ex.run_one(), ex.run_one()];
            let resting = listed(&signals);
            // Notified with its condition still unmet: it looks, rests, and is listed once.
            signals[(which + 1) % 3].notify();
            let looked = [ex.run_one(), ex.run_one()];
            let again = listed(&signals);
            ready.store(true, O::Release);
            signals[which].notify();
            let woken = ex.run_one();
            let done = task.is_done();
            let after = listed(&signals);
            ex.close();
            assert_eq!(first, [true, false], "{which}: stepped, then resting");
            assert_eq!(resting, [1, 1, 1], "{which}: listed on each");
            assert_eq!(looked, [true, false], "{which}: looked again, then resting");
            assert_eq!(again, [1, 1, 1], "{which}: listed once");
            assert!(woken && done, "{which}: woken by its signal alone");
            assert_eq!(after, [0, 0, 0], "{which}: a done task left listed");
        }
    }

    /// First waits on `on`'s work with a completion nothing finishes here, then is done.
    struct WaitsOnJob {
        on: Option<TaskRef>,
        log: Arc<Lock<Vec<&'static str>>>,
    }

    impl Task for WaitsOnJob {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            self.log.lock().push("a");
            match self.on.take() {
                Some(on) => Ok(Step::WaitOn(on, Completion::new_sync(|_| {}))),
                None => Ok(Step::Done),
            }
        }
    }

    /// A writer's relief reaches the job a waiting task waits on: the task's logical wait holds a
    /// ticket of its own class on that job, moved whenever its class changes, so the job is
    /// claimed before an unrelated task queued earlier; dropped, the writer's ticket lowers both
    /// again (22.4; proof 4; mutants: no ticket for the dependency; the waiting ticket left at the
    /// class it was taken with).
    #[test]
    fn relief_reaches_the_job_a_waiting_task_waits_on_and_goes_with_its_ticket() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let job = ex.spawn_ranked(Class::Maintenance, 4, recorded("job", 0, &log), &[]);
        let waiting = ex.spawn_ranked(
            Class::Due,
            2,
            Box::new(WaitsOnJob {
                on: Some(job.clone()),
                log: log.clone(),
            }),
            &[],
        );
        ex.spawn(Class::Due, recorded("other", 0, &log));
        let stepped = ex.run_one();
        let carried = job.class();
        let writer = waiting.ticket(Class::Relief);
        let relieved = (waiting.class(), job.class());
        let stepped_again = ex.run_one();
        let first = log.lock().clone();
        drop(writer);
        // The writer's ticket gone: the waiting task's own class, which its wait still carries.
        let lowered = (waiting.class(), job.class());
        while ex.run_one() {}
        // The wait over once the waiting task runs again: its ticket goes too.
        let after = job.class();
        ex.close();
        assert!(stepped && stepped_again, "the first two claims");
        assert_eq!(
            carried,
            Class::Due,
            "the job carries the waiting task's class"
        );
        assert_eq!(relieved, (Class::Relief, Class::Relief));
        assert_eq!(
            first,
            ["a", "job"],
            "the job claimed before the task queued earlier"
        );
        assert_eq!(
            lowered,
            (Class::Due, Class::Due),
            "the writer's ticket gone"
        );
        assert_eq!(after, Class::Maintenance, "the wait's ticket gone");
    }

    /// One ticket dropped never clears another's: two writers' tickets, the first dropped, leave
    /// the task at the second's class (22.4; proof 4; mutant: a drop that clears the class's
    /// count).
    #[test]
    fn a_ticket_dropped_leaves_anothers_demand_standing() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let task = ex.spawn(Class::Maintenance, recorded("t", 0, &log));
        let first = task.ticket(Class::Relief);
        let second = task.ticket(Class::Relief);
        drop(first);
        let standing = task.class();
        drop(second);
        let gone = task.class();
        ex.close();
        assert_eq!(standing, Class::Relief, "the second writer's demand");
        assert_eq!(gone, Class::Maintenance);
    }

    /// A trigger places its task in the due list as it wakes it: a due task spawned after the
    /// trigger is claimed after it, and a maintenance task queued before it after both (22.4;
    /// proof 4; mutant: the trigger wakes without setting the task's level).
    #[test]
    fn a_trigger_places_its_task_in_the_due_list() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let due = Signal::default();
        ex.spawn(Class::Maintenance, recorded("m", 0, &log));
        let triggered = ex.spawn(Class::Maintenance, recorded("t", 0, &log));
        triggered.trigger(&due);
        ex.spawn(Class::Due, recorded("d", 0, &log));
        while ex.run_one() {}
        ex.close();
        let claimed = log.lock().clone();
        assert_eq!(claimed, ["t", "d", "m"]);
    }

    /// Reads `due`, finds its trigger gone, and clears its own level.
    struct Clears {
        due: Arc<Signal>,
        cleared: Arc<AtomicBool>,
    }

    impl Task for Clears {
        fn step(&mut self, cx: &Cx) -> Result<Step> {
            let read = self.due.read();
            self.cleared
                .store(cx.clear_due(&self.due, read), O::Release);
            Ok(Step::Done)
        }
    }

    /// A trigger arriving between a step's look and its clear keeps the task's level: the clear
    /// checks the count it read under the queue's mutex (22.4; proof 4; mutant: the clear not
    /// checked against the count, losing the newer level).
    #[test]
    fn a_trigger_between_a_look_and_its_clear_keeps_the_level() {
        let ex = executor(0);
        let due = Arc::new(Signal::default());
        let cleared = Arc::new(AtomicBool::new(true));
        let task = ex.spawn(
            Class::Due,
            Box::new(Clears {
                due: due.clone(),
                cleared: cleared.clone(),
            }),
        );
        let (t, d) = (task.clone(), due.clone());
        crate::race::at("executor.clearing", move || t.trigger(&d));
        let stepped = ex.run_one();
        let class = task.class();
        let refused = !cleared.load(O::Acquire);
        ex.close();
        assert!(stepped);
        assert!(refused, "a clear over a trigger that came after its look");
        assert_eq!(class, Class::Due);
    }

    /// A queued task whose demand rises moves to the back of its new class's list (22.4; mutant:
    /// the class stored and the task left in its old list).
    #[test]
    fn a_queued_task_whose_demand_rises_moves_to_its_new_class() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        ex.spawn(Class::Due, recorded("d", 0, &log));
        let task = ex.spawn(Class::Maintenance, recorded("m", 0, &log));
        let writer = task.ticket(Class::Relief);
        while ex.run_one() {}
        drop(writer);
        ex.close();
        let claimed = log.lock().clone();
        assert_eq!(claimed, ["m", "d"]);
    }

    /// A named claim takes a queued task out of its list and steps it at once, ahead of a task
    /// queued before it, and it is stepped once; a done one it leaves (22.7; mutant: a queued task
    /// left to the queue's order).
    #[test]
    fn a_named_claim_takes_a_queued_task_out_of_its_list() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        ex.spawn(Class::Due, recorded("a", 0, &log));
        let b = ex.spawn(Class::Due, recorded("b", 0, &log));
        let named = ex.run_named(&b);
        while ex.run_one() {}
        let again = ex.run_named(&b);
        ex.close();
        let claimed = log.lock().clone();
        assert!(named, "the queued task claimed by name");
        assert_eq!(claimed, ["b", "a"]);
        assert!(!again, "a done task claimed by name");
    }

    /// A named claim runs an idle task, which nothing else woke, for its claimer's own reason
    /// (22.7; mutant: an idle task left idle).
    #[test]
    fn a_named_claim_runs_an_idle_task() {
        let ex = executor(0);
        let c = Completion::new_sync(|_| {});
        let (done_tx, done_rx) = mpsc::channel();
        let task = ex.spawn(
            Class::Maintenance,
            Box::new(WaitsOn {
                c: Some(c.clone()),
                done: done_tx,
            }),
        );
        let first = ex.run_one();
        let idle = !ex.run_one();
        let named = ex.run_named(&task);
        let done = (done_rx.try_recv().is_ok(), task.is_done());
        ex.close();
        assert!(first && idle, "stepped, then waiting");
        assert!(named, "the idle task claimed by name");
        assert_eq!(done, (true, true), "its step ran");
    }

    /// Steps once: says it entered, then waits for `go`.
    struct Blocks {
        entered: mpsc::Sender<()>,
        go: mpsc::Receiver<()>,
    }

    impl Task for Blocks {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let _ = self.entered.send(());
            let _ = self.go.recv();
            Ok(Step::Done)
        }
    }

    /// A named claim leaves a running task to its runner, returning at once (22.7).
    #[test]
    fn a_named_claim_leaves_a_running_task_to_its_runner() {
        let ex = executor(1);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        let task = ex.spawn(
            Class::Due,
            Box::new(Blocks {
                entered: entered_tx,
                go: go_rx,
            }),
        );
        let running = entered_rx.recv_timeout(BOUND).is_ok();
        let named = ex.run_named(&task);
        go_tx.send(()).unwrap();
        ex.close();
        assert!(running, "a worker stepped it");
        assert!(!named, "a running task claimed by name");
    }

    /// Reads `signal` and rests on it, every step.
    struct WatchesOne(Arc<Signal>);

    impl Task for WatchesOne {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let read = self.0.read();
            Ok(Step::Idle(Watched::of(&[(&self.0, read)])))
        }
    }

    /// A signal's list keeps room for every task declared to watch it, however many are declared
    /// before any registers: the room is there before the first registration and unchanged by the
    /// last (22.2; the auditor's P2-X69-3; mutant: each declaration making room for one more than
    /// the list holds now).
    #[test]
    fn a_signals_list_keeps_room_for_every_task_declared_to_watch_it() {
        const TASKS: usize = 32;
        let ex = executor(0);
        let signal = Arc::new(Signal::default());
        for _ in 0..TASKS {
            ex.spawn_watching(Class::Due, Box::new(WatchesOne(signal.clone())), &[&signal]);
        }
        let room = signal.room();
        let mut stepped = 0;
        while ex.run_one() {
            stepped += 1;
        }
        let registered = (signal.listed(), signal.room());
        ex.close();
        let closed = signal.listed();
        assert_eq!(stepped, TASKS);
        assert!(room >= TASKS, "room for {room} of {TASKS} declared");
        assert_eq!(
            registered,
            (TASKS, room),
            "(listed, room) once every task registered"
        );
        assert_eq!(closed, 0, "close takes every task off");
    }

    /// A step may watch only the signals its job declared, whose lists keep room for it: one
    /// naming another is refused, and the refusal, passing the executor, ends the process (22.2,
    /// 22.11; mutant: the check gone, so a registration grows a list).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_step_watching_a_signal_its_job_did_not_declare_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_step_watching_a_signal_its_job_did_not_declare_ends_the_process"
            ),
            || {
                let ex = executor(0);
                ex.spawn(
                    Class::Due,
                    Box::new(WatchesOne(Arc::new(Signal::default()))),
                );
                ex.run_one();
                ex.close();
            },
        );
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended
                .stderr
                .contains("a step watching a signal its job did not declare"),
            "{}",
            ended.stderr
        );
    }

    /// Dropping an executor closes it: its workers are joined, and nothing keeps it alive (22.7;
    /// the auditor's P2-X69-4; mutant: a drop that closes nothing, so its parked workers keep it
    /// for ever).
    #[test]
    fn dropping_an_executor_closes_it_and_joins_its_workers() {
        let ex = executor(2);
        let shared = Arc::downgrade(&ex.shared);
        drop(ex);
        assert!(
            shared.upgrade().is_none(),
            "an executor dropped and still alive"
        );
    }

    /// Holds the executor that runs it, and drops it during its step.
    struct DropsItsExecutor {
        executor: Arc<Lock<Option<Executor>>>,
        dropped: mpsc::Sender<()>,
    }

    impl Task for DropsItsExecutor {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let ex = self.executor.lock().take();
            drop(ex);
            let _ = self.dropped.send(());
            Ok(Step::Done)
        }
    }

    /// An executor dropped by a step on one of its own workers closes, joins its other workers
    /// and leaves that one to end once the step returns, never joining it from itself; the end of
    /// that step, its task closed, does nothing more (22.7; the auditor's P2-X69-4).
    #[test]
    fn an_executor_dropped_by_its_own_workers_step_closes_without_joining_that_worker() {
        let slot = Arc::new(Lock::new(None));
        let (dropped_tx, dropped_rx) = mpsc::channel();
        *slot.lock() = Some(executor(2));
        let task = slot.lock().as_ref().unwrap().spawn(
            Class::Due,
            Box::new(DropsItsExecutor {
                executor: slot.clone(),
                dropped: dropped_tx,
            }),
        );
        let dropped = dropped_rx.recv_timeout(BOUND).is_ok();
        let taken = slot.lock().is_none();
        assert!(dropped && taken, "the step dropped its executor");
        assert!(!task.is_done(), "closed, not done");
    }

    /// Holds the executor that runs it and a signal it was declared to watch, drops the executor
    /// during its step and is then done; says so when its job is dropped.
    struct DropsItsExecutorWatching {
        executor: Arc<Lock<Option<Executor>>>,
        gone: mpsc::Sender<()>,
    }

    impl Task for DropsItsExecutorWatching {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let ex = self.executor.lock().take();
            drop(ex);
            Ok(Step::Done)
        }
    }

    impl Drop for DropsItsExecutorWatching {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            let _ = self.gone.send(());
        }
    }

    /// An executor dropped by its own worker's step, the task then done: close takes the task off
    /// the signal it declared, and the step's end on the closed task leaves it no second time
    /// (22.12; the auditor's #4207 case; mutant: close leaving the declared tasks).
    #[test]
    fn an_executor_dropped_by_its_own_step_leaves_each_declared_signal_once() {
        let slot = Arc::new(Lock::new(None));
        let signal = Arc::new(Signal::default());
        let (gone_tx, gone_rx) = mpsc::channel();
        *slot.lock() = Some(executor(1));
        slot.lock().as_ref().unwrap().spawn_watching(
            Class::Due,
            Box::new(DropsItsExecutorWatching {
                executor: slot.clone(),
                gone: gone_tx,
            }),
            &[&signal],
        );
        let gone = gone_rx.recv_timeout(BOUND).is_ok();
        let counts = (signal.declared(), signal.listed());
        assert!(gone, "the task's job dropped once its step ended");
        assert_eq!(
            counts,
            (0, 0),
            "(declared, listed) once the executor closed"
        );
    }

    /// A wait names a job of its own executor only: one naming another's ends the process before
    /// any mutex (22.4, 22.11; the auditor's P2-E4-O1; mutant: the check gone, which links the
    /// foreign job into this executor's queue).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_wait_naming_a_job_of_another_executor_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_wait_naming_a_job_of_another_executor_ends_the_process"
            ),
            || {
                let (a, b) = (executor(0), executor(0));
                let log = Arc::new(Lock::new(Vec::new()));
                let target = b.spawn_ranked(Class::Maintenance, 1, recorded("job", 0, &log), &[]);
                a.spawn_ranked(
                    Class::Due,
                    0,
                    Box::new(WaitsOnJob {
                        on: Some(target),
                        log,
                    }),
                    &[],
                );
                a.run_one();
                a.close();
                b.close();
            },
        );
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended
                .stderr
                .contains("a wait naming a job of another executor"),
            "{}",
            ended.stderr
        );
    }

    /// A task is claimed by name only on its own executor: a claim on another ends the process
    /// before any mutex (22.7, 22.11; mutant: the check gone, which claims it out of a queue it
    /// is not in).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_named_claim_on_another_executor_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_named_claim_on_another_executor_ends_the_process"
            ),
            || {
                let (a, b) = (executor(0), executor(0));
                let log = Arc::new(Lock::new(Vec::new()));
                let task = b.spawn(Class::Due, recorded("t", 0, &log));
                a.run_named(&task);
                a.close();
                b.close();
            },
        );
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended
                .stderr
                .contains("a task claimed by name on another executor"),
            "{}",
            ended.stderr
        );
    }

    /// Records, when it is dropped, whether the executor's table of jobs was free to take: 1 when
    /// it was held, 2 when it was free.
    struct ProbesTheTable {
        shared: Weak<Shared>,
        dropped: Arc<AtomicUsize>,
    }

    impl Task for ProbesTheTable {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            Ok(Step::Done)
        }
    }

    impl Drop for ProbesTheTable {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            if let Some(shared) = self.shared.upgrade() {
                let free = shared.jobs.try_lock().is_some();
                self.dropped.store(if free { 2 } else { 1 }, O::Release);
            }
        }
    }

    /// Close drops every job after the table's mutex: a job's drop may run anything its task
    /// holds, a waker's or a callback's code among it (22.2; the auditor's P2-X69-5; mutant: the
    /// jobs dropped as the table is drained under its mutex).
    #[test]
    fn close_drops_every_job_after_the_tables_mutex() {
        let ex = executor(0);
        let dropped = Arc::new(AtomicUsize::new(0));
        ex.spawn(
            Class::Due,
            Box::new(ProbesTheTable {
                shared: Arc::downgrade(&ex.shared),
                dropped: dropped.clone(),
            }),
        );
        ex.close();
        assert_eq!(
            dropped.load(O::Acquire),
            2,
            "a job dropped under the table's mutex (1), or never (0)"
        );
    }

    /// An external runner's waker: counts its wakes, and records whether the queue's mutex and,
    /// when it watches one, a signal's list were free to take at each, and at its drop.
    struct Probe {
        shared: Weak<Shared>,
        signal: Option<Arc<Signal>>,
        /// A flag the test sets, read at each wake.
        flag: Arc<AtomicBool>,
        woken: AtomicUsize,
        /// Wakes with every watched mutex free; wakes with the flag set.
        free: AtomicUsize,
        flagged: AtomicUsize,
        dropped: Arc<AtomicUsize>,
    }

    impl Probe {
        fn new(ex: &Executor, signal: Option<Arc<Signal>>) -> Arc<Self> {
            Self::reading(ex, signal, Arc::new(AtomicBool::new(false)))
        }

        fn reading(ex: &Executor, signal: Option<Arc<Signal>>, flag: Arc<AtomicBool>) -> Arc<Self> {
            Arc::new(Self {
                shared: Arc::downgrade(&ex.shared),
                signal,
                flag,
                woken: AtomicUsize::new(0),
                free: AtomicUsize::new(0),
                flagged: AtomicUsize::new(0),
                dropped: Arc::new(AtomicUsize::new(0)),
            })
        }

        /// Whether every mutex it watches is free now.
        fn all_free(&self) -> bool {
            let queue = self.shared.upgrade().is_none_or(|shared| {
                shared.queue.try_lock().is_some() && shared.jobs.try_lock().is_some()
            });
            let listed = self.signal.as_ref().is_none_or(|s| s.listed_free());
            queue && listed
        }

        fn counts(&self) -> (usize, usize, usize) {
            (
                self.woken.load(O::Acquire),
                self.free.load(O::Acquire),
                self.flagged.load(O::Acquire),
            )
        }
    }

    impl std::task::Wake for Probe {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            if self.all_free() {
                self.free.fetch_add(1, O::AcqRel);
            }
            if self.flag.load(O::Acquire) {
                self.flagged.fetch_add(1, O::AcqRel);
            }
            self.woken.fetch_add(1, O::AcqRel);
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            // 2 with the queue's mutex free, 1 under it.
            self.dropped
                .store(if self.all_free() { 2 } else { 1 }, O::Release);
        }
    }

    /// An external runner with nothing to claim leaves its waker on its node; a link moves the
    /// node and the linking operation invokes the waker once, after the queue's mutex is let go,
    /// and the runner then steps the task (22.3, 22.7; mutant: the waker invoked by the link,
    /// under the mutex).
    #[test]
    fn an_external_runner_is_woken_by_a_link_after_the_queues_mutex() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let runner = ex.runner();
        let probe = Probe::new(&ex, None);
        let waker = std::task::Waker::from(probe.clone());
        let first = runner.run_one(&waker);
        ex.spawn(Class::Due, recorded("t", 0, &log));
        let woken = probe.counts();
        let stepped = runner.run_one(&waker);
        drop(runner);
        ex.close();
        let claimed = log.lock().clone();
        // Every probe gone before asserting: one dropped while a panic unwinds ends the process.
        drop((waker, probe));
        assert!(!first, "nothing to claim");
        assert_eq!(woken.0, 1, "woken once by the link");
        assert_eq!(woken.1, 1, "woken after the queue's mutex");
        assert!(stepped);
        assert_eq!(claimed, ["t"]);
    }

    /// A runner registering again swaps its new waker into its node: the link wakes that one, and
    /// the one it replaced is dropped after the queue's mutex (22.3; mutant: the first waker kept).
    #[test]
    fn a_runner_registering_again_is_woken_through_its_new_waker() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let runner = ex.runner();
        let (old, new) = (Probe::new(&ex, None), Probe::new(&ex, None));
        let dropped = old.dropped.clone();
        let registered = runner.run_one(&std::task::Waker::from(old.clone()));
        // The node's clone is now the old waker's last handle.
        drop(old);
        let again = runner.run_one(&std::task::Waker::from(new.clone()));
        ex.spawn(Class::Due, recorded("t", 0, &log));
        let woken = new.counts();
        drop(runner);
        ex.close();
        drop(new);
        assert!(!registered && !again, "nothing to claim, twice");
        assert_eq!(
            dropped.load(O::Acquire),
            2,
            "replaced, and dropped after the mutex"
        );
        assert_eq!(
            (woken.0, woken.1),
            (1, 1),
            "the new waker woken, after the mutex"
        );
    }

    /// Each operation's drain invokes no more nodes than its own links moved, from the head of
    /// the to-wake list: an operation coming between another's link and its drain wakes the first
    /// node moved, and the first operation then the second (22.3; mutant: a drain that empties
    /// the list).
    #[test]
    fn each_drain_invokes_no_more_than_its_own_operation_moved() {
        let ex = Arc::new(executor(0));
        let log = Arc::new(Lock::new(Vec::new()));
        let (one, two) = (ex.runner(), ex.runner());
        let flag = Arc::new(AtomicBool::new(false));
        let first = Probe::reading(&ex, None, flag.clone());
        let second = Probe::reading(&ex, None, flag.clone());
        let registered = [
            one.run_one(&std::task::Waker::from(first.clone())),
            two.run_one(&std::task::Waker::from(second.clone())),
        ];
        {
            let (ex, log, flag) = (ex.clone(), log.clone(), flag.clone());
            crate::race::at("executor.draining", move || {
                ex.spawn(Class::Due, recorded("b", 0, &log));
                flag.store(true, O::Release);
            });
        }
        ex.spawn(Class::Due, recorded("a", 0, &log));
        let woken = (first.counts(), second.counts());
        drop((one, two));
        let ex = Arc::into_inner(ex);
        let alone = ex.is_some();
        if let Some(ex) = ex {
            ex.close();
        }
        drop((first, second));
        assert!(alone, "the executor held by the race action");
        assert_eq!(registered, [false, false]);
        assert_eq!(woken.0 .0, 1, "the first node moved woken once");
        assert_eq!(
            woken.0 .2, 0,
            "by the operation that came between, before it returned"
        );
        assert_eq!(
            (woken.1 .0, woken.1 .2),
            (1, 1),
            "the second by the first operation, after"
        );
    }

    /// A runner dropped after a link moved its node, before the drain: its node goes off the
    /// to-wake list, and nothing invokes its waker (22.3; mutant: a drop that leaves the node).
    #[test]
    fn a_runner_dropped_after_its_node_moved_is_never_woken() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let runner = Arc::new(Lock::new(Some(ex.runner())));
        let probe = Probe::new(&ex, None);
        let registered = runner
            .lock()
            .as_ref()
            .unwrap()
            .run_one(&std::task::Waker::from(probe.clone()));
        {
            let runner = runner.clone();
            crate::race::at("executor.draining", move || {
                let gone = runner.lock().take();
                drop(gone);
            });
        }
        ex.spawn(Class::Due, recorded("t", 0, &log));
        let woken = probe.counts().0;
        ex.close();
        drop((runner, probe));
        assert!(!registered);
        assert_eq!(woken, 0, "a dropped runner's waker invoked");
    }

    /// Close wakes the external runners idle among the runners, after its mutexes, so each sees
    /// it closed (22.12; mutant: close moving them and invoking none).
    #[test]
    fn close_wakes_the_idle_external_runners() {
        let ex = executor(0);
        let runner = ex.runner();
        let probe = Probe::new(&ex, None);
        let registered = runner.run_one(&std::task::Waker::from(probe.clone()));
        ex.close();
        let woken = probe.counts();
        let after = runner.run_one(&std::task::Waker::from(probe.clone()));
        drop((runner, probe));
        assert!(!registered && !after, "nothing to claim, open or closed");
        assert_eq!(
            (woken.0, woken.1),
            (1, 1),
            "woken by close, after its mutex"
        );
    }

    /// A notify's wakes move external runners while the signal's mutex is held, and the notify
    /// invokes them once it lets that mutex and the queue's go (22.2, 22.3; mutant: each listed
    /// task woken as an operation of its own, under the signal's mutex).
    #[test]
    fn a_notify_wakes_the_runners_it_moved_after_the_signals_mutex() {
        let ex = executor(0);
        let (task, signals, ready) = watching(&ex);
        let stepped = ex.run_one();
        let runner = ex.runner();
        let probe = Probe::new(&ex, Some(signals[0].clone()));
        let registered = runner.run_one(&std::task::Waker::from(probe.clone()));
        ready.store(true, O::Release);
        signals[0].notify();
        let woken = probe.counts();
        let ran = runner.run_one(&std::task::Waker::from(probe.clone()));
        let done = task.is_done();
        drop(runner);
        ex.close();
        drop(probe);
        assert!(stepped && !registered);
        assert_eq!((woken.0, woken.1), (1, 1), "woken once, after both mutexes");
        assert!(ran && done, "the runner stepped the task the notify queued");
    }

    /// Steps once: syncs a file of a backend that delivers inline, whose completion carries the
    /// probe's waker, registered before the submission, so the sync's finish wakes it inside the
    /// step.
    struct SyncsInline {
        file: Arc<dyn crate::io::File>,
        probe: Option<Arc<Probe>>,
    }

    impl Task for SyncsInline {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let c = Completion::new_sync(|_| {});
            if let Some(probe) = self.probe.take() {
                c.set_waker(&std::task::Waker::from(probe));
            }
            // Finished at its submission: the probe counts the wake its finish made.
            drop(self.file.sync(c, FileSyncType::Fsync)?);
            Ok(Step::Done)
        }
    }

    /// A step holds no executor mutex: a request's waker its unit's inline submission invokes
    /// runs with the queue's and the table's mutexes free, and the task is held by no lock (22.2;
    /// the auditor's P2-E4-W1; mutant: the step taken under the table's mutex).
    #[test]
    fn a_waker_a_step_invokes_runs_with_every_executor_mutex_free() {
        let memory = Arc::new(crate::io::MemoryIO::new());
        let file = memory.open_file("/db/f", OpenFlags::Create, false).unwrap();
        let ex = Executor::new(memory, 0).unwrap();
        let probe = Probe::new(&ex, None);
        ex.spawn(
            Class::Due,
            Box::new(SyncsInline {
                file,
                probe: Some(probe.clone()),
            }),
        );
        let stepped = ex.run_one();
        let woken = probe.counts();
        ex.close();
        drop(probe);
        assert!(stepped);
        assert_eq!(
            (woken.0, woken.1),
            (1, 1),
            "woken once, with every executor mutex free"
        );
    }

    /// Close waits for every external call stepping a task: an embedder's call in a step when
    /// close begins ends that step before close returns (22.7; the auditor's P2-E5-C1; mutant:
    /// close joining its workers alone).
    #[test]
    fn close_waits_for_an_external_call_in_progress() {
        let ex = executor(0);
        let runner = ex.runner();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        ex.spawn(
            Class::Due,
            Box::new(Blocks {
                entered: entered_tx,
                go: go_rx,
            }),
        );
        let call = std::thread::spawn(move || runner.run_one(std::task::Waker::noop()));
        let entered = entered_rx.recv_timeout(BOUND).is_ok();
        let (closed_tx, closed_rx) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            ex.close();
            let _ = closed_tx.send(());
        });
        // Long enough for a close that does not wait to return, which takes microseconds.
        let early = closed_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        go_tx.send(()).unwrap();
        let closed = early || closed_rx.recv_timeout(BOUND).is_ok();
        let stepped = call.join().unwrap();
        closer.join().unwrap();
        assert!(entered, "the external call stepped the task");
        assert!(!early, "close returned while an external call was stepping");
        assert!(closed && stepped);
    }

    /// An executor dropped by a step of an external call closes without waiting for that call,
    /// its own (22.7).
    #[test]
    fn an_executor_dropped_by_an_external_calls_own_step_closes() {
        let slot = Arc::new(Lock::new(None));
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let ex = executor(0);
        let runner = ex.runner();
        ex.spawn(
            Class::Due,
            Box::new(DropsItsExecutor {
                executor: slot.clone(),
                dropped: dropped_tx,
            }),
        );
        *slot.lock() = Some(ex);
        let call = std::thread::spawn(move || runner.run_one(std::task::Waker::noop()));
        let dropped = dropped_rx.recv_timeout(BOUND).is_ok();
        let stepped = dropped && call.join().unwrap();
        let taken = slot.lock().is_none();
        assert!(
            dropped && taken,
            "the call's step dropped its executor and closed it"
        );
        assert!(stepped);
    }

    /// Calls back into its executor from its step: by name when it names a task, else for the
    /// next.
    struct CallsBack {
        executor: Arc<Lock<Option<Arc<Executor>>>>,
        named: Option<TaskRef>,
    }

    impl Task for CallsBack {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let ex = self.executor.lock().clone();
            if let Some(ex) = ex {
                match &self.named {
                    Some(task) => ex.run_named(task),
                    None => ex.run_one(),
                };
            }
            Ok(Step::Done)
        }
    }

    /// Runs, in a child, a task whose step calls back into its own executor.
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn calls_back_from_a_step(named: bool) {
        let ex = Arc::new(executor(0));
        let slot = Arc::new(Lock::new(Some(ex.clone())));
        let log = Arc::new(Lock::new(Vec::new()));
        let other = ex.spawn(Class::Maintenance, recorded("other", 0, &log));
        ex.spawn(
            Class::Due,
            Box::new(CallsBack {
                executor: slot.clone(),
                named: named.then_some(other),
            }),
        );
        ex.run_one();
        drop(slot.lock().take());
        if let Ok(ex) = Arc::try_unwrap(ex) {
            ex.close();
        }
    }

    /// The parent's half: the child ended by abort, saying why.
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn ended_refused(ended: Option<crate::storage::atoll::testing::Ended>) {
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended
                .stderr
                .contains("a runner entered the executor from inside a step or a waker"),
            "{}",
            ended.stderr
        );
    }

    /// A caller's claim never enters from inside a step: one that does ends the process (22.7,
    /// 22.11; the auditor's P2-E5-R1; mutant: the plain caller's entry unchecked).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_caller_entering_from_inside_a_step_ends_the_process() {
        ended_refused(crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_caller_entering_from_inside_a_step_ends_the_process"
            ),
            || calls_back_from_a_step(false),
        ));
    }

    /// A named claim never enters from inside a step either (22.7, 22.11; the auditor's
    /// P2-E5-R1; mutant: the named claim's entry unchecked).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_named_claim_from_inside_a_step_ends_the_process() {
        ended_refused(crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_named_claim_from_inside_a_step_ends_the_process"
            ),
            || calls_back_from_a_step(true),
        ));
    }

    /// Asks again `steps` times, then finishes `c` and is done, recording the thread of each
    /// step.
    struct FinishesAfter {
        steps: usize,
        c: Completion,
        ran_on: Arc<Lock<Vec<std::thread::ThreadId>>>,
    }

    impl Task for FinishesAfter {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            self.ran_on.lock().push(std::thread::current().id());
            if self.steps == 0 {
                self.c.complete(0);
                return Ok(Step::Done);
            }
            self.steps -= 1;
            Ok(Step::Again)
        }
    }

    fn finishes_after(
        steps: usize,
        c: &Completion,
    ) -> (Box<dyn Task>, Arc<Lock<Vec<std::thread::ThreadId>>>) {
        let ran_on = Arc::new(Lock::new(Vec::new()));
        let task = Box::new(FinishesAfter {
            steps,
            c: c.clone(),
            ran_on: ran_on.clone(),
        });
        (task, ran_on)
    }

    /// A blocking wait on executor work runs the executor itself when it has no workers, until
    /// what it waits for finishes (22.7; mutant: a waiter that only parks, which nothing wakes).
    #[test]
    fn a_blocking_wait_without_workers_runs_the_executor_until_its_completion_finishes() {
        let ex = Arc::new(executor(0));
        let c = Completion::new_wait();
        let (task, ran_on) = finishes_after(3, &c);
        ex.spawn(Class::Due, task);
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = {
            let (ex, c) = (ex.clone(), c.clone());
            std::thread::spawn(move || {
                ex.wait(&c);
                let _ = done_tx.send(std::thread::current().id());
            })
        };
        let waited = done_rx.recv_timeout(BOUND).ok();
        if waited.is_none() {
            // A waiter nothing woke: finish what it waits for so the thread ends.
            c.complete(0);
        }
        waiter.join().unwrap();
        let steps = ran_on.lock().clone();
        Arc::try_unwrap(ex).ok().unwrap().close();
        let waiter = waited.expect("the wait returned");
        assert_eq!(steps.len(), 4, "every step of the task");
        assert!(
            steps.iter().all(|&id| id == waiter),
            "a step on another thread"
        );
    }

    /// A blocking wait with nothing to claim and an empty pumped backend parks: one look, no
    /// steps meanwhile; a link wakes it to run what was linked (22.5, 22.7; mutant: a waiter that
    /// keeps stepping instead of parking).
    #[test]
    fn a_blocking_wait_parks_with_nothing_to_do_and_a_link_wakes_it() {
        let crash = Arc::new(CrashIO::new(1));
        let io = crate::io::wait_tests::StepCounting::new(crash.clone());
        let ex = Arc::new(Executor::new(io.clone(), 0).unwrap());
        let c = Completion::new_wait();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = {
            let (ex, c) = (ex.clone(), c.clone());
            std::thread::spawn(move || {
                ex.wait(&c);
                let _ = done_tx.send(());
            })
        };
        // Long enough for the waiter to look and park, which takes microseconds.
        std::thread::sleep(Duration::from_millis(100));
        let parked = (io.steps(), done_rx.try_recv().is_ok());
        let (task, ran_on) = finishes_after(0, &c);
        ex.spawn(Class::Due, task);
        let woken = done_rx.recv_timeout(BOUND).is_ok();
        if !woken {
            c.complete(0);
        }
        waiter.join().unwrap();
        let steps = ran_on.lock().len();
        Arc::try_unwrap(ex).ok().unwrap().close();
        assert_eq!(
            parked,
            (1, false),
            "(backend steps, returned) while nothing was queued"
        );
        assert!(woken, "the link woke the parked waiter");
        assert_eq!(steps, 1);
    }

    /// With workers, a blocking wait only waits: the workers run what it waits for (22.7; mutant:
    /// a waiter that runs the executor beside its workers).
    #[test]
    fn a_blocking_wait_with_workers_only_waits() {
        let ex = executor(2);
        let c = Completion::new_wait();
        let (task, ran_on) = finishes_after(64, &c);
        ex.spawn(Class::Due, task);
        ex.wait(&c);
        let finished = c.finished();
        ex.close();
        let me = std::thread::current().id();
        let steps = ran_on.lock().clone();
        assert!(finished);
        assert_eq!(steps.len(), 65);
        assert!(
            steps.iter().all(|&id| id != me),
            "the waiter stepped a task"
        );
    }

    /// Counts its wakes.
    struct Counts(Arc<AtomicUsize>);

    impl std::task::Wake for Counts {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, O::AcqRel);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, O::AcqRel);
        }
    }

    /// A cooperative wait runs one task step per poll and, while work is queued, asks its host to
    /// poll again at once; ready once what it waits for finishes (22.7; mutant: a poll that asks
    /// for no other while work is queued).
    #[test]
    fn a_cooperative_wait_runs_one_step_per_poll_and_polls_again_while_work_is_queued() {
        use std::future::Future;
        let ex = executor(0);
        let c = Completion::new_wait();
        let (task, ran_on) = finishes_after(2, &c);
        ex.spawn(Class::Due, task);
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = std::task::Waker::from(Arc::new(Counts(wakes.clone())));
        let mut cx = std::task::Context::from_waker(&waker);
        let mut polls = Vec::new();
        {
            let mut wait = std::pin::pin!(ex.wait_async(c.clone()));
            for _ in 0..4 {
                let ready = wait.as_mut().poll(&mut cx).is_ready();
                polls.push((ready, ran_on.lock().len(), wakes.swap(0, O::AcqRel) > 0));
                if ready {
                    break;
                }
            }
        }
        ex.close();
        assert_eq!(
            polls,
            [
                (false, 1, true),
                (false, 2, true),
                (false, 3, true),
                (true, 3, false)
            ],
            "(ready, steps so far, woken) per poll"
        );
    }

    /// A cooperative wait with nothing to claim rests its node among the idle runners and asks
    /// for no poll; a link wakes its host, whose next poll runs the task (22.3, 22.7; mutant: a
    /// poll that leaves no node).
    #[test]
    fn a_cooperative_wait_with_nothing_queued_rests_and_a_link_wakes_its_host() {
        use std::future::Future;
        let ex = executor(0);
        let c = Completion::new_wait();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = std::task::Waker::from(Arc::new(Counts(wakes.clone())));
        let mut cx = std::task::Context::from_waker(&waker);
        let (first, linked, second, third) = {
            let mut wait = std::pin::pin!(ex.wait_async(c.clone()));
            let first = (
                wait.as_mut().poll(&mut cx).is_pending(),
                wakes.swap(0, O::AcqRel),
            );
            let (task, ran_on) = finishes_after(0, &c);
            ex.spawn(Class::Due, task);
            let linked = wakes.swap(0, O::AcqRel);
            let second = (
                wait.as_mut().poll(&mut cx).is_pending(),
                ran_on.lock().len(),
            );
            let third = wait.as_mut().poll(&mut cx).is_ready();
            (first, linked, second, third)
        };
        ex.close();
        assert_eq!(
            first,
            (true, 0),
            "(pending, wakes asked) with nothing queued"
        );
        assert_eq!(linked, 1, "the link woke the host");
        assert_eq!(second, (true, 1), "the next poll stepped the task");
        assert!(third, "ready once it finished");
    }

    /// Steps by its signal: clears its own level against the count it read, then rests on it.
    struct Sleeps {
        signal: Arc<Signal>,
        units: Arc<AtomicUsize>,
    }

    impl Task for Sleeps {
        fn step(&mut self, cx: &Cx) -> Result<Step> {
            let read = self.signal.read();
            cx.clear_due(&self.signal, read);
            self.units.fetch_add(1, O::Relaxed);
            Ok(Step::Idle(Watched::of(&[(&self.signal, read)])))
        }
    }

    /// One round of every way a task is woken and claimed: a ticket of `class`, a trigger, a
    /// plain wake, a named claim; whether each did what it should.
    fn cycle(
        ex: &Executor,
        runner: &Runner,
        task: &TaskRef,
        signal: &Signal,
        waker: &std::task::Waker,
        class: Class,
    ) -> bool {
        let ticket = task.ticket(class);
        let a = runner.run_one(waker);
        drop(ticket);
        let b = !runner.run_one(waker);
        task.trigger(signal);
        let c = runner.run_one(waker);
        let d = !runner.run_one(waker);
        task.wake();
        let e = runner.run_one(waker);
        let f = !runner.run_one(waker);
        let g = ex.run_named(task);
        let h = !runner.run_one(waker);
        a && b && c && d && e && f && g && h
    }

    /// Wakes, claims, class changes and external runners allocate nothing once provisioned: a
    /// task stepped 3,072 times by tickets, triggers, plain wakes and named claims, its runner
    /// woken 2,304 times (22.13; the auditor's probe; mutant: the task boxed again at each step).
    #[test]
    fn repeated_wakes_claims_and_class_changes_allocate_nothing_once_provisioned() {
        let ex = Executor::new(Arc::new(crate::io::MemoryIO::new()), 0).unwrap();
        let signal = Arc::new(Signal::default());
        let units = Arc::new(AtomicUsize::new(0));
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = std::task::Waker::from(Arc::new(Counts(wakes.clone())));
        let task = ex.spawn_watching(
            Class::Maintenance,
            Box::new(Sleeps {
                signal: signal.clone(),
                units: units.clone(),
            }),
            &[&signal],
        );
        let runner = ex.runner();
        let mut valid = runner.run_one(&waker) && !runner.run_one(&waker);
        // Every path once before counting, the race points' thread locals included.
        for class in [Class::Relief, Class::Due, Class::Maintenance] {
            valid &= cycle(&ex, &runner, &task, &signal, &waker, class);
        }
        let before = crate::storage::atoll::testing::allocations();
        let (old_units, old_wakes) = (units.load(O::Relaxed), wakes.load(O::Acquire));
        for _ in 0..256 {
            for class in [Class::Relief, Class::Due, Class::Maintenance] {
                valid &= cycle(&ex, &runner, &task, &signal, &waker, class);
            }
        }
        let allocations = crate::storage::atoll::testing::allocations() - before;
        let measured = (
            units.load(O::Relaxed) - old_units,
            wakes.load(O::Acquire) - old_wakes,
        );
        drop(runner);
        ex.close();
        assert!(valid, "a round did not wake or claim as it should");
        assert_eq!(measured, (3072, 2304), "(steps, external wakes) counted");
        assert_eq!(allocations, 0);
    }

    /// Done at its first step; its destructor enters another executor through `runner`.
    struct EntersFromItsDrop {
        runner: Option<Runner>,
    }

    impl Task for EntersFromItsDrop {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            Ok(Step::Done)
        }
    }

    impl Drop for EntersFromItsDrop {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            if let Some(runner) = self.runner.take() {
                runner.run_one(std::task::Waker::noop());
            }
        }
    }

    /// A unit's cleanup is inside it: a task's destructor, run as its job's last handle goes at
    /// the end of its step, entering any executor ends the process (22.7, 22.11; the auditor's
    /// P2-E8-C2; mutant: the guard scoped to the step alone).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_tasks_destructor_entering_an_executor_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_tasks_destructor_entering_an_executor_ends_the_process"
            ),
            || {
                let (a, b) = (executor(0), executor(0));
                a.spawn(
                    Class::Due,
                    Box::new(EntersFromItsDrop {
                        runner: Some(b.runner()),
                    }),
                );
                a.run_one();
                a.close();
                b.close();
            },
        );
        ended_refused(ended);
    }

    /// Waits on `signal` until `value` reaches `threshold`: wants it, then looks, and is done once
    /// it has.
    struct AwaitsThreshold {
        signal: Arc<Signal>,
        value: Arc<std::sync::atomic::AtomicU64>,
        threshold: u64,
    }

    impl Task for AwaitsThreshold {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let read = self.signal.read();
            self.signal.want(self.threshold);
            if self.value.load(O::Acquire) >= self.threshold {
                return Ok(Step::Done);
            }
            Ok(Step::Idle(Watched::of(&[(&self.signal, read)])))
        }
    }

    /// A producer's change.
    fn publish(value: &std::sync::atomic::AtomicU64, signal: &Signal, to: u64) {
        value.store(to, O::Release);
        if signal.wanted().is_some_and(|least| to >= least) {
            signal.notify();
        }
    }

    /// A threshold signal wakes its waiting tasks only once the value reached the least threshold
    /// wanted, and a notify resets that threshold (22.2; mutants: a want that keeps the last
    /// threshold, not the least; a notify that leaves the threshold standing).
    #[test]
    fn a_threshold_signal_wakes_once_the_least_threshold_wanted_is_reached() {
        let ex = executor(0);
        let signal = Arc::new(Signal::default());
        let value = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let tasks = [3, 5].map(|threshold| {
            ex.spawn_watching(
                Class::Due,
                Box::new(AwaitsThreshold {
                    signal: signal.clone(),
                    value: value.clone(),
                    threshold,
                }),
                &[&signal],
            )
        });
        let looked = [ex.run_one(), ex.run_one(), ex.run_one()];
        let least = signal.wanted();
        publish(&value, &signal, 2);
        let below = ex.run_one();
        publish(&value, &signal, 3);
        let reset = signal.wanted();
        let woken = [ex.run_one(), ex.run_one(), ex.run_one()];
        let after = (signal.wanted(), tasks.each_ref().map(TaskRef::is_done));
        publish(&value, &signal, 5);
        while ex.run_one() {}
        let done = tasks.each_ref().map(TaskRef::is_done);
        ex.close();
        assert_eq!(looked, [true, true, false], "both looked, both wait");
        assert_eq!(least, Some(3), "the least threshold wanted");
        assert!(!below, "woken below every threshold");
        assert_eq!(reset, None, "a notify leaves a threshold standing");
        assert_eq!(
            woken,
            [true, true, false],
            "both woken at the least threshold"
        );
        // The first wanted 3 before it found 3 reached: a want stands until the next notify.
        assert_eq!(
            after,
            (Some(3), [true, false]),
            "(threshold wanted, done) once both looked again"
        );
        assert_eq!(done, [true, true]);
    }

    /// A cooperative wait gives its runner's node back once ready, though its owner keeps it:
    /// a link after that wakes a live waiter, never the finished one (22.3, 22.7; the auditor's
    /// P1-E9-R1; mutant: the node kept until the wait is dropped).
    #[test]
    fn a_ready_wait_kept_alive_takes_no_wake_owed_to_another_waiter() {
        use std::future::Future;
        let ex = executor(0);
        let (first, second) = (Completion::new_wait(), Completion::new_wait());
        let (first_wakes, second_wakes) =
            (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let first_waker = std::task::Waker::from(Arc::new(Counts(first_wakes.clone())));
        let second_waker = std::task::Waker::from(Arc::new(Counts(second_wakes.clone())));
        let mut first_cx = std::task::Context::from_waker(&first_waker);
        let mut second_cx = std::task::Context::from_waker(&second_waker);
        let mut kept = Box::pin(ex.wait_async(first.clone()));
        let rested = kept.as_mut().poll(&mut first_cx).is_pending();
        // Finished by another caller while it rests.
        first.complete(0);
        let ready = kept.as_mut().poll(&mut first_cx).is_ready();
        let mut live = Box::pin(ex.wait_async(second.clone()));
        let pending = live.as_mut().poll(&mut second_cx).is_pending();
        let before = (
            first_wakes.load(O::Acquire),
            second_wakes.swap(0, O::AcqRel),
        );
        let (task, _) = finishes_after(0, &second);
        ex.spawn(Class::Due, task);
        let woken = second_wakes.load(O::Acquire);
        // Ready either way, so every handle goes before close.
        let mut polls = 0;
        while live.as_mut().poll(&mut second_cx).is_pending() && polls < 4 {
            polls += 1;
        }
        let finished = second.finished();
        drop((kept, live));
        ex.close();
        assert!(rested && ready && pending);
        assert_eq!(before.1, 0, "the live waiter woken before any link");
        assert_eq!(
            woken, 1,
            "the link woke the finished waiter, not the live one"
        );
        assert!(finished);
    }

    /// The ring's delivery thread never waits: a blocking wait on executor work made there ends
    /// the process before it runs anything (22.5; the auditor's P2-E9-D1; mutant: the refusal
    /// gone, so the wait runs the executor on the delivery thread).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_wait_on_executor_work_from_the_delivery_thread_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_wait_on_executor_work_from_the_delivery_thread_ends_the_process"
            ),
            || {
                let ex = executor(0);
                let c = Completion::new_wait();
                let (task, _) = finishes_after(0, &c);
                ex.spawn(Class::Due, task);
                crate::io::deliver_on_this_thread();
                ex.wait(&c);
                ex.close();
            },
        );
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended.stderr.contains("on the ring's delivery thread"),
            "{}",
            ended.stderr
        );
    }

    /// Panics when woken.
    struct PanicsOnWake;

    impl std::task::Wake for PanicsOnWake {
        fn wake(self: Arc<Self>) {
            panic!("a host's waker panicked");
        }
    }

    /// A cooperative wait's poll is an entry: a host waker panicking when the poll asks it for
    /// another poll ends the process, though nothing of Atoll's would drop while it unwinds
    /// (22.11; the auditor's P2-E9-F1; mutant: the poll without its frame).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_host_waker_panicking_in_a_poll_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_host_waker_panicking_in_a_poll_ends_the_process"
            ),
            || {
                use std::future::Future;
                // Never dropped: a panic out of the poll unwinds through nothing of Atoll's.
                let ex: &'static Executor = Box::leak(Box::new(executor(0)));
                let c = Completion::new_wait();
                let (task, _) = finishes_after(1, &c);
                ex.spawn(Class::Due, task);
                let wait = Box::leak(Box::new(ex.wait_async(c)));
                // SAFETY: leaked, the wait never moves again.
                let wait = unsafe { std::pin::Pin::new_unchecked(wait) };
                let waker = std::task::Waker::from(Arc::new(PanicsOnWake));
                let mut cx = std::task::Context::from_waker(&waker);
                // A step leaves the task queued: the poll asks its host for another.
                let _ = wait.poll(&mut cx);
            },
        );
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended.stderr.contains("a host's waker panicked"),
            "{}",
            ended.stderr
        );
    }

    /// A runner cancelled after a link chose it, before the link's drain woke it, hands the wake
    /// to another idle runner while the work is queued (22.3; the auditor's P1-EX-H1; mutant: a
    /// cancel that frees the node and hands nothing on).
    #[test]
    fn a_runner_cancelled_after_a_link_chose_it_hands_its_wake_to_another() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let chosen = Arc::new(Lock::new(Some(ex.runner())));
        let other = ex.runner();
        let (chosen_wakes, other_wakes) =
            (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let rested = [
            chosen
                .lock()
                .as_ref()
                .unwrap()
                .run_one(&std::task::Waker::from(Arc::new(Counts(
                    chosen_wakes.clone(),
                )))),
            other.run_one(&std::task::Waker::from(Arc::new(Counts(
                other_wakes.clone(),
            )))),
        ];
        {
            let chosen = chosen.clone();
            crate::race::at("executor.draining", move || {
                let gone = chosen.lock().take();
                drop(gone);
            });
        }
        ex.spawn(Class::Due, recorded("t", 0, &log));
        let woken = (chosen_wakes.load(O::Acquire), other_wakes.load(O::Acquire));
        let ran = other.run_one(std::task::Waker::noop());
        drop(other);
        ex.close();
        assert_eq!(rested, [false, false]);
        assert_eq!(
            woken,
            (0, 1),
            "(cancelled, live) wakes once the cancel let go of its own"
        );
        assert!(ran);
    }

    /// A runner woken by a link and dropped before it acted, the work still queued, hands its
    /// wake to another idle runner (22.3; the auditor's P1-EX-H1, the drop after the wake;
    /// mutant: the handoff gone).
    #[test]
    fn a_runner_woken_and_dropped_before_it_acted_hands_its_wake_on() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let woken_one = ex.runner();
        let other = ex.runner();
        let (first_wakes, other_wakes) =
            (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let rested = [
            woken_one.run_one(&std::task::Waker::from(Arc::new(Counts(
                first_wakes.clone(),
            )))),
            other.run_one(&std::task::Waker::from(Arc::new(Counts(
                other_wakes.clone(),
            )))),
        ];
        ex.spawn(Class::Due, recorded("t", 0, &log));
        let first = (first_wakes.load(O::Acquire), other_wakes.load(O::Acquire));
        drop(woken_one);
        let handed = other_wakes.load(O::Acquire);
        let ran = other.run_one(std::task::Waker::noop());
        drop(other);
        ex.close();
        assert_eq!(rested, [false, false]);
        assert_eq!(first, (1, 0), "the link woke the first idle runner");
        assert_eq!(handed, 1, "the wake left with the dropped runner");
        assert!(ran);
    }

    /// Enters the executor when woken.
    struct EntersWhenWoken(&'static Executor);

    impl std::task::Wake for EntersWhenWoken {
        fn wake(self: Arc<Self>) {
            self.0.run_one();
        }
    }

    /// A cooperative wait invokes its host's waker as a drain does, inside: a host entering the
    /// executor from that wake ends the process rather than running a second unit in the poll
    /// (22.7; the auditor's P2-E9-F1, the self-wake's re-entry; mutant: the wake invoked outside).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_host_entering_from_the_polls_own_wake_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_host_entering_from_the_polls_own_wake_ends_the_process"
            ),
            || {
                use std::future::Future;
                // Never dropped: nothing of Atoll's drops while a panic unwinds.
                let ex: &'static Executor = Box::leak(Box::new(executor(0)));
                let c = Completion::new_wait();
                let (task, _) = finishes_after(2, &c);
                ex.spawn(Class::Due, task);
                let wait = Box::leak(Box::new(ex.wait_async(c)));
                // SAFETY: leaked, the wait never moves again.
                let wait = unsafe { std::pin::Pin::new_unchecked(wait) };
                let waker = std::task::Waker::from(Arc::new(EntersWhenWoken(ex)));
                let mut cx = std::task::Context::from_waker(&waker);
                // A step leaves the task queued: the poll asks its host for another.
                let _ = wait.poll(&mut cx);
            },
        );
        ended_refused(ended);
    }

    /// A host's waker whose destructor panics once armed.
    struct PanicsWhenDropped(Arc<AtomicBool>);

    impl std::task::Wake for PanicsWhenDropped {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for PanicsWhenDropped {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            if self.0.load(O::Acquire) {
                panic!("a host's waker panicked in its destructor");
            }
        }
    }

    /// A host's waker, armed by the flag returned, and a weak handle that counts its clones.
    fn panics_when_dropped() -> (
        std::task::Waker,
        Arc<AtomicBool>,
        std::sync::Weak<PanicsWhenDropped>,
    ) {
        let armed = Arc::new(AtomicBool::new(false));
        let host = Arc::new(PanicsWhenDropped(armed.clone()));
        let clones = Arc::downgrade(&host);
        (std::task::Waker::from(host), armed, clones)
    }

    /// The child aborted on `panicked`, passed up from inside a frame.
    fn ended_in_a_frame(ended: Option<crate::storage::atoll::testing::Ended>, panicked: &str) {
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(ended.stderr.contains(panicked), "{}", ended.stderr);
    }

    /// A cancelled wait drops its host's wakers inside a frame, the last of them in its
    /// completion's own drop after the body: one panicking ends the process (22.11; the
    /// auditor's P2-EX-F2; mutant: the frame's end not the wait's last field).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_host_waker_panicking_as_a_cancelled_wait_drops_it_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_host_waker_panicking_as_a_cancelled_wait_drops_it_ends_the_process"
            ),
            || {
                use std::future::Future;
                // Never dropped: nothing of Atoll's but the wait drops while a panic unwinds.
                let ex: &'static Executor = Box::leak(Box::new(executor(0)));
                let (waker, armed, clones) = panics_when_dropped();
                let mut wait = Box::pin(ex.wait_async(Completion::new_wait()));
                let pending = wait
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(&waker))
                    .is_pending();
                drop(waker);
                // The completion's clone and the runner node's, at the least.
                let held = clones.strong_count();
                assert!(
                    pending && held >= 2,
                    "pending {pending}, {held} clones held"
                );
                armed.store(true, O::Release);
                drop(wait);
            },
        );
        ended_in_a_frame(ended, "a host's waker panicked in its destructor");
    }

    /// A runner dropped drops the host's waker its node held inside a frame: one panicking ends
    /// the process (22.11; the auditor's P2-EX-F2; mutant: the runner's drop unframed).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_host_waker_panicking_as_its_runner_drops_it_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_host_waker_panicking_as_its_runner_drops_it_ends_the_process"
            ),
            || {
                let ex: &'static Executor = Box::leak(Box::new(executor(0)));
                let runner = ex.runner();
                let (waker, armed, clones) = panics_when_dropped();
                let rested = !runner.run_one(&waker);
                drop(waker);
                let held = clones.strong_count();
                assert!(rested && held == 1, "rested {rested}, {held} clones held");
                armed.store(true, O::Release);
                drop(runner);
            },
        );
        ended_in_a_frame(ended, "a host's waker panicked in its destructor");
    }

    /// A task whose destructor panics: queued and never stepped, or done after one step.
    struct PanicsAtItsEnd;

    impl Task for PanicsAtItsEnd {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            Ok(Step::Done)
        }
    }

    impl Drop for PanicsAtItsEnd {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            panic!("a task's destructor panicked");
        }
    }

    /// Close drops the jobs of tasks never stepped, with their tasks, inside a frame: a task's
    /// destructor panicking ends the process (22.11; the auditor's P2-EX-F2; mutant: the
    /// executor's drop unframed).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_tasks_destructor_panicking_as_close_drops_its_job_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_tasks_destructor_panicking_as_close_drops_its_job_ends_the_process"
            ),
            || {
                let ex = executor(0);
                let task = ex.spawn(Class::Due, Box::new(PanicsAtItsEnd));
                ex.close();
                drop(task);
            },
        );
        ended_in_a_frame(ended, "a task's destructor panicked");
    }

    /// The step that ends a task drops its job, with the task, inside the step's unit, on every
    /// road that steps it, a caller's claim among them: the task's destructor panicking ends the
    /// process as the unit's guard drops (22.11; the auditor's P2-EX-F2; mutant: the guard's
    /// check gone).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_tasks_destructor_panicking_as_its_last_step_ends_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_tasks_destructor_panicking_as_its_last_step_ends_ends_the_process"
            ),
            || {
                let ex: &'static Executor = Box::leak(Box::new(executor(0)));
                ex.spawn(Class::Due, Box::new(PanicsAtItsEnd));
                ex.run_one();
            },
        );
        ended_in_a_frame(ended, "a task's destructor panicked");
    }

    /// What happened to whom, in order.
    type Events = Arc<Lock<Vec<(&'static str, &'static str)>>>;

    /// Waits on `waits` if it has one, else is done; records its step, its abandonment (with the
    /// thread and whether inside) and its drop.
    struct Abandons {
        name: &'static str,
        waits: Option<Completion>,
        events: Events,
        abandoned_on: Arc<Lock<Vec<(std::thread::ThreadId, bool)>>>,
    }

    impl Task for Abandons {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            self.events.lock().push((self.name, "stepped"));
            Ok(match &self.waits {
                Some(c) => Step::Wait(c.clone()),
                None => Step::Done,
            })
        }

        fn abandon(&mut self) {
            self.events.lock().push((self.name, "abandoned"));
            self.abandoned_on
                .lock()
                .push((std::thread::current().id(), Inside::here()));
        }
    }

    impl Drop for Abandons {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            self.events.lock().push((self.name, "dropped"));
        }
    }

    fn abandons(
        name: &'static str,
        waits: Option<&Completion>,
        events: &Events,
        abandoned_on: &Arc<Lock<Vec<(std::thread::ThreadId, bool)>>>,
    ) -> Box<dyn Task> {
        Box::new(Abandons {
            name,
            waits: waits.cloned(),
            events: events.clone(),
            abandoned_on: abandoned_on.clone(),
        })
    }

    /// The events of `name`, in order.
    fn of(events: &Events, name: &str) -> Vec<&'static str> {
        events
            .lock()
            .iter()
            .filter(|(n, _)| *n == name)
            .map(|(_, e)| *e)
            .collect()
    }

    /// Close gives up the logical waits of every task it closes, once each, on the closing
    /// thread, after every step and inside, before it drops the task: a queued task and an idle
    /// one alike, and none for a task already done, whose job is gone (22.12 step 3; mutants: no
    /// abandonment; the abandonment outside).
    #[test]
    fn close_gives_up_each_closed_tasks_waits_once_on_the_closing_thread() {
        let ex = executor(0);
        let (events, on) = (Events::default(), Arc::new(Lock::new(Vec::new())));
        let never = Completion::new_wait();
        ex.spawn(Class::Due, abandons("idle", Some(&never), &events, &on));
        ex.spawn(Class::Due, abandons("done", None, &events, &on));
        assert!(ex.run_one() && ex.run_one());
        ex.spawn(Class::Due, abandons("queued", None, &events, &on));
        let closer = std::thread::spawn(move || {
            ex.close();
            std::thread::current().id()
        });
        let closer = closer.join().unwrap();
        // Read out before any assertion: none panics holding a lock of Atoll's (22.11).
        let on = on.lock().clone();
        assert_eq!(of(&events, "idle"), ["stepped", "abandoned", "dropped"]);
        assert_eq!(of(&events, "done"), ["stepped", "dropped"]);
        assert_eq!(of(&events, "queued"), ["abandoned", "dropped"]);
        assert_eq!(on, [(closer, true), (closer, true)]);
    }

    /// Close waits for the logs' durable end while every job still runs: without workers it runs
    /// them itself until the end is durable, and only then closes them (22.12 step 1; mutant:
    /// closing published before the wait).
    #[test]
    fn close_waits_for_the_durable_end_while_every_job_still_runs() {
        let ex = executor(0);
        let durable = Completion::new_wait();
        let (task, ran_on) = finishes_after(3, &durable);
        ex.spawn(Class::Due, task);
        let closed = ex.close_after(&durable, &Requests::default());
        assert!(closed.is_ok(), "{closed:?}");
        let steps = ran_on.lock().len();
        assert!(durable.succeeded(), "closed before the end was durable");
        assert_eq!(steps, 4);
    }

    /// A durable end that failed is returned, after close gave up every task's waits, waited for
    /// every counted request to retire and dropped every job, in that order (22.12; mutants: a
    /// failure returned at once; no wait for the requests; the requests waited for after the
    /// jobs are dropped).
    #[test]
    fn a_failed_durable_end_is_returned_after_every_later_step_of_close() {
        let requests = Arc::new(Requests::default());
        let crash = Arc::new(CrashIO::new(1));
        let io: Arc<dyn IO> = Arc::new(Counted::new(crash.clone(), requests.clone()));
        let ex = Executor::new(io.clone(), 0).unwrap();
        let (events, on) = (Events::default(), Arc::new(Lock::new(Vec::new())));
        let never = Completion::new_wait();
        ex.spawn(Class::Due, abandons("idle", Some(&never), &events, &on));
        assert!(ex.run_one());
        let file = io.open_file("/close/r", OpenFlags::Create, false).unwrap();
        let read = {
            let events = events.clone();
            Completion::new_read(Arc::new(crate::io::Buffer::new_temporary(512)), move |_| {
                events.lock().push(("request", "finished"));
                None
            })
        };
        let read = file.pread(0, read).unwrap();
        assert_eq!((requests.outstanding(), crash.in_flight()), (1, 1));
        let durable = Completion::new_wait();
        durable.error(crate::CompletionError::Aborted);
        let closed = ex.close_after(&durable, &requests);
        events.lock().push(("close", "returned"));
        let events = events.lock().clone();
        assert!(
            matches!(
                closed,
                Err(crate::LimboError::CompletionError(
                    crate::CompletionError::Aborted
                ))
            ),
            "{closed:?}"
        );
        assert_eq!(
            events,
            [
                ("idle", "stepped"),
                ("idle", "abandoned"),
                ("request", "finished"),
                ("idle", "dropped"),
                ("close", "returned"),
            ]
        );
        assert!(read.finished());
        assert_eq!((requests.outstanding(), crash.in_flight()), (0, 0));
    }

    /// Waits on a completion nothing finishes; at close, enters the executor through `runner`.
    struct EntersOnAbandon {
        runner: Option<Runner>,
        never: Completion,
    }

    impl Task for EntersOnAbandon {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            Ok(Step::Wait(self.never.clone()))
        }

        fn abandon(&mut self) {
            if let Some(runner) = &self.runner {
                runner.run_one(std::task::Waker::noop());
            }
        }
    }

    /// A task's abandonment runs inside: one entering the executor from it ends the process
    /// (22.7, 22.12 step 3; mutant: the abandonment outside).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_task_entering_the_executor_from_its_abandonment_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_task_entering_the_executor_from_its_abandonment_ends_the_process"
            ),
            || {
                let ex = executor(0);
                ex.spawn(
                    Class::Due,
                    Box::new(EntersOnAbandon {
                        runner: Some(ex.runner()),
                        never: Completion::new_wait(),
                    }),
                );
                ex.run_one();
                ex.close();
            },
        );
        ended_refused(ended);
    }

    /// Rests watching `signal`, its condition never met; records its steps, an abandonment and
    /// its drop.
    struct WatchesUntilRemoved {
        name: &'static str,
        signal: Arc<Signal>,
        events: Events,
    }

    impl Task for WatchesUntilRemoved {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            self.events.lock().push((self.name, "stepped"));
            let read = self.signal.read();
            Ok(Step::Idle(Watched::of(&[(&self.signal, read)])))
        }

        fn abandon(&mut self) {
            self.events.lock().push((self.name, "abandoned"));
        }
    }

    impl Drop for WatchesUntilRemoved {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            self.events.lock().push((self.name, "dropped"));
        }
    }

    fn watches_until_removed(
        ex: &Executor,
        name: &'static str,
        signal: &Arc<Signal>,
        events: &Events,
    ) -> TaskRef {
        ex.spawn_watching(
            Class::Due,
            Box::new(WatchesUntilRemoved {
                name,
                signal: signal.clone(),
                events: events.clone(),
            }),
            &[signal],
        )
    }

    /// A task removed while queued or idle is closed, unlinked, off its signal and out of the
    /// table, its job dropped by the remover at once: no runner claims it, a notify wakes nothing,
    /// a second removal does nothing, and close gives up nothing of it (22.3; mutant: a queued
    /// task left linked).
    #[test]
    fn a_task_removed_queued_or_idle_lets_its_job_go_at_once() {
        let ex = executor(0);
        let events = Events::default();
        let signal = Arc::new(Signal::default());
        let idle = watches_until_removed(&ex, "idle", &signal, &events);
        assert!(ex.run_one());
        let queued = watches_until_removed(&ex, "queued", &signal, &events);
        let before = (signal.listed(), signal.declared());
        idle.remove();
        queued.remove();
        let after = (signal.listed(), signal.declared());
        let (tasks, linked) = {
            let queue = lock(&ex.shared.queue);
            (
                queue.tasks,
                queue
                    .lists
                    .iter()
                    .map(std::collections::VecDeque::len)
                    .sum::<usize>(),
            )
        };
        let jobs = lock(&ex.shared.jobs).len();
        // No claim of a removed task left linked, which would fail the claim's own check; every
        // assertion after close, so a failing one drops no executor while it unwinds.
        let claimed = linked == 0 && {
            signal.notify();
            ex.run_one()
        };
        idle.remove();
        ex.close();
        assert_eq!(linked, 0, "a removed task left linked");
        assert_eq!(before, (1, 2));
        assert_eq!(after, (0, 0), "(listed, declared) once both are removed");
        assert_eq!((tasks, jobs), (0, 0));
        assert!(!claimed, "a removed task claimed");
        assert_eq!(of(&events, "idle"), ["stepped", "dropped"]);
        assert_eq!(of(&events, "queued"), ["dropped"]);
    }

    /// A task removed while its runner steps it, after its step registered it on a signal and
    /// before the end's rest: the end finds it closed and lets its job go, off the signal it had
    /// just been listed on (22.3; mutant: an end that finds it closed and leaves the job).
    #[test]
    fn a_task_removed_while_its_step_ends_lets_its_job_go_from_that_end() {
        let ex = executor(0);
        let events = Events::default();
        let signal = Arc::new(Signal::default());
        let task = watches_until_removed(&ex, "running", &signal, &events);
        {
            let task = task.clone();
            crate::race::at("executor.registered", move || task.remove());
        }
        let claimed = ex.run_one();
        let after = (signal.listed(), signal.declared());
        let tasks = lock(&ex.shared.queue).tasks;
        let jobs = lock(&ex.shared.jobs).len();
        let claimed_again = ex.run_one();
        ex.close();
        assert!(claimed && !claimed_again);
        assert_eq!(after, (0, 0), "(listed, declared) after the end");
        assert_eq!((tasks, jobs), (0, 0));
        assert_eq!(of(&events, "running"), ["stepped", "dropped"]);
    }

    /// A task removed from inside its own step lets its job go as that step ends (22.3).
    #[test]
    fn a_task_removing_itself_in_its_step_goes_as_the_step_ends() {
        struct RemovesItself {
            me: Arc<Lock<Option<TaskRef>>>,
            events: Events,
        }
        impl Task for RemovesItself {
            fn step(&mut self, _cx: &Cx) -> Result<Step> {
                if let Some(me) = self.me.lock().take() {
                    me.remove();
                }
                self.events.lock().push(("self", "stepped"));
                Ok(Step::Again)
            }
        }
        impl Drop for RemovesItself {
            fn drop(&mut self) {
                crate::storage::atoll::unwind::abort_if_unwinding();
                self.events.lock().push(("self", "dropped"));
            }
        }
        let ex = executor(0);
        let events = Events::default();
        let me = Arc::new(Lock::new(None));
        let task = ex.spawn(
            Class::Due,
            Box::new(RemovesItself {
                me: me.clone(),
                events: events.clone(),
            }),
        );
        *me.lock() = Some(task);
        let claims = [ex.run_one(), ex.run_one()];
        let left = (lock(&ex.shared.queue).tasks, lock(&ex.shared.jobs).len());
        ex.close();
        assert_eq!(claims, [true, false], "a removed task queued again");
        assert_eq!(left, (0, 0));
        assert_eq!(of(&events, "self"), ["stepped", "dropped"]);
    }

    /// Steps once, held mid-step until `go`; then waits on a completion nothing finishes.
    struct HeldMidStep {
        go: Option<mpsc::Receiver<()>>,
        started: mpsc::Sender<()>,
        events: Events,
    }

    impl Task for HeldMidStep {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            if let Some(go) = self.go.take() {
                self.started.send(()).unwrap();
                go.recv().unwrap();
            }
            self.events.lock().push(("held", "stepped"));
            Ok(Step::Wait(Completion::new_wait()))
        }

        fn abandon(&mut self) {
            self.events.lock().push(("held", "abandoned"));
        }
    }

    impl Drop for HeldMidStep {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            self.events.lock().push(("held", "dropped"));
        }
    }

    /// A task an external runner is stepping when close begins: its step's end finds it closed
    /// and leaves it to close, which gives up its waits after the step and drops it (22.12 step 3;
    /// mutant: the end letting the job go itself, as for a removal).
    #[test]
    fn a_task_running_when_close_begins_is_given_up_by_close() {
        const BOUND: Duration = Duration::from_secs(20);
        let ex = executor(0);
        let events = Events::default();
        let (go_tx, go_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        ex.spawn(
            Class::Due,
            Box::new(HeldMidStep {
                go: Some(go_rx),
                started: started_tx,
                events: events.clone(),
            }),
        );
        let runner = ex.runner();
        let stepper = std::thread::spawn(move || runner.run_one(std::task::Waker::noop()));
        started_rx.recv_timeout(BOUND).expect("the step began");
        let shared = ex.shared.clone();
        let closer = std::thread::spawn(move || ex.close());
        let start = std::time::Instant::now();
        while !shared.closed.load(O::Acquire) {
            assert!(start.elapsed() < BOUND, "close never published closing");
            std::thread::sleep(Duration::from_millis(1));
        }
        go_tx.send(()).unwrap();
        assert!(stepper.join().unwrap());
        closer.join().unwrap();
        assert_eq!(of(&events, "held"), ["stepped", "abandoned", "dropped"]);
    }

    /// A waiting task removed gives back the ticket its logical wait held on the job it waited
    /// on, which falls back to its own class (22.3, 22.4; mutant: the wait not ended).
    #[test]
    fn a_waiting_task_removed_gives_back_its_ticket_on_the_job_it_waited_on() {
        let ex = executor(0);
        let log = Arc::new(Lock::new(Vec::new()));
        let job = ex.spawn_ranked(Class::Maintenance, 4, recorded("job", 0, &log), &[]);
        let waiting = ex.spawn_ranked(
            Class::Due,
            2,
            Box::new(WaitsOnJob {
                on: Some(job.clone()),
                log: log.clone(),
            }),
            &[],
        );
        assert!(ex.run_one());
        let carried = job.class();
        waiting.remove();
        let after = job.class();
        ex.close();
        assert_eq!((carried, after), (Class::Due, Class::Maintenance));
    }

    /// Steps once: enters the executor through `runner`.
    struct EntersAgain {
        runner: Arc<Lock<Option<Runner>>>,
    }

    impl Task for EntersAgain {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let waker = std::task::Waker::noop().clone();
            if let Some(runner) = self.runner.lock().as_ref() {
                runner.run_one(&waker);
            }
            Ok(Step::Done)
        }
    }

    /// A runner never enters the executor from inside a step: one that does ends the process
    /// (22.7, 22.11; mutant: the check gone).
    #[test]
    #[cfg(all(unix, not(any(shuttle, miri))))]
    fn a_runner_entering_from_inside_a_step_ends_the_process() {
        let ended = crate::storage::atoll::testing::in_child(
            concat!(
                module_path!(),
                "::a_runner_entering_from_inside_a_step_ends_the_process"
            ),
            || {
                let ex = executor(0);
                let runner = Arc::new(Lock::new(Some(ex.runner())));
                ex.spawn(
                    Class::Due,
                    Box::new(EntersAgain {
                        runner: runner.clone(),
                    }),
                );
                ex.run_one();
                drop(runner.lock().take());
                ex.close();
            },
        );
        let Some(ended) = ended else { return };
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(
            ended
                .stderr
                .contains("a runner entered the executor from inside a step or a waker"),
            "{}",
            ended.stderr
        );
    }
}

#[cfg(atoll_loom)]
mod loom_tests {
    use super::*;
    use loom::thread;

    fn shared() -> Arc<Shared> {
        let mut queue = Queue::new(0);
        queue.tasks = 1;
        Arc::new(Shared {
            queue: Mutex::new(queue),
            closed: AtomicBool::new(false),
            jobs: Mutex::new(Vec::new()),
            io: Arc::new(crate::io::MemoryIO::new()),
        })
    }

    fn header(shared: &Arc<Shared>, state: u8) -> Arc<Header> {
        Arc::new(Header {
            state: AtomicU8::new(state),
            class: AtomicU8::new(Class::Due as u8),
            demand: Mutex::new(Demand {
                own: Class::Due,
                tickets: [0; 2],
                waiting_on: None,
            }),
            rank: 0,
            shared: Arc::downgrade(shared),
            job: Weak::new(),
        })
    }

    /// How many times `header` is linked.
    fn links(shared: &Shared, header: &Arc<Header>) -> usize {
        lock(&shared.queue)
            .lists
            .iter()
            .flatten()
            .filter(|h| Arc::ptr_eq(h, header))
            .count()
    }

    /// A wake racing a running task's rest: whichever comes first, the task ends queued and
    /// linked once (22.3; proof 2; mutant: the rest a load and then a store).
    #[test]
    fn a_wake_racing_a_tasks_rest_leaves_it_queued_once() {
        loom::model(|| {
            let shared = shared();
            let h = header(&shared, RUNNING);
            let waker = {
                let h = h.clone();
                thread::spawn(move || wake(&h))
            };
            shared.rest(&h);
            waker.join().unwrap();
            assert_eq!(h.state.load(Ordering::Acquire), QUEUED);
            assert_eq!(links(&shared, &h), 1);
        });
    }

    /// Two wakes of an idle task link it once (22.3; proof 2).
    #[test]
    fn two_wakes_of_an_idle_task_link_it_once() {
        loom::model(|| {
            let shared = shared();
            let h = header(&shared, IDLE);
            let other = {
                let h = h.clone();
                thread::spawn(move || wake(&h))
            };
            wake(&h);
            other.join().unwrap();
            assert_eq!(h.state.load(Ordering::Acquire), QUEUED);
            assert_eq!(links(&shared, &h), 1);
        });
    }

    /// A wake racing a running task's end as done: the task ends done and is never linked
    /// (22.3; mutant: a done task's end resting it).
    #[test]
    fn a_wake_racing_a_tasks_end_leaves_it_done_and_unlinked() {
        loom::model(|| {
            let shared = shared();
            let h = header(&shared, RUNNING);
            let waker = {
                let h = h.clone();
                thread::spawn(move || wake(&h))
            };
            shared.finish(&h);
            waker.join().unwrap();
            assert_eq!(h.state.load(Ordering::Acquire), DONE);
            assert_eq!(links(&shared, &h), 0);
        });
    }

    /// A notify racing a running task's registration: the task ends queued and linked once,
    /// whether the notify's walk finds it listed or its second read finds the count moved (22.2;
    /// proof 1; mutant: no second read).
    #[test]
    fn a_notify_racing_a_registration_leaves_the_task_queued_once() {
        loom::model(|| {
            let shared = shared();
            let signal = Arc::new(Signal::default());
            let h = header(&shared, RUNNING);
            let read = signal.read();
            let notifier = {
                let signal = signal.clone();
                thread::spawn(move || signal.notify())
            };
            if signal.register(&h, read, &shared) {
                shared.rest(&h);
            } else {
                shared.requeue(&h);
            }
            notifier.join().unwrap();
            assert_eq!(h.state.load(Ordering::Acquire), QUEUED);
            assert_eq!(links(&shared, &h), 1);
        });
    }

    /// Two idle external runners, one cancelled racing a link: whichever comes first, the live
    /// runner's host is woken for the queued work (22.3; the auditor's P1-EX-H1; mutant: a cancel
    /// that hands nothing on).
    #[test]
    fn a_cancel_racing_a_link_leaves_the_live_runner_woken() {
        use loom::sync::atomic::AtomicUsize;
        loom::model(|| {
            let ex = Arc::new(Executor::new(Arc::new(crate::io::MemoryIO::new()), 0).unwrap());
            let (cancelled, live) = (ex.runner(), ex.runner());
            let units = Arc::new(AtomicUsize::new(0));
            let (cancelled_wakes, live_wakes) =
                (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
            let rested = !cancelled
                .run_one(&std::task::Waker::from(Arc::new(Wakes(cancelled_wakes))))
                && !live.run_one(&std::task::Waker::from(Arc::new(Wakes(live_wakes.clone()))));
            let producer = {
                let (ex, units) = (ex.clone(), units.clone());
                thread::spawn(move || {
                    ex.spawn(Class::Due, Box::new(Count(units)));
                })
            };
            let cancel = thread::spawn(move || drop(cancelled));
            producer.join().unwrap();
            cancel.join().unwrap();
            let woken = live_wakes.load(Ordering::Relaxed);
            drop(live);
            Arc::try_unwrap(ex).ok().unwrap().close();
            assert!(rested);
            assert!(woken > 0, "the live runner left asleep with work queued");
        });
    }

    /// Waits on `signal` until `value` reaches `threshold`.
    struct AwaitsThreshold {
        signal: Arc<Signal>,
        value: Arc<loom::sync::atomic::AtomicU64>,
        threshold: u64,
    }

    impl Task for AwaitsThreshold {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let read = self.signal.read();
            self.signal.want(self.threshold);
            if self.value.load(Ordering::Relaxed) >= self.threshold {
                return Ok(Step::Done);
            }
            Ok(Step::Idle(Watched::of(&[(&self.signal, read)])))
        }
    }

    /// A producer's change: the value published, then a notify once it reached the least
    /// threshold wanted.
    fn publish(value: &loom::sync::atomic::AtomicU64, signal: &Signal, to: u64) {
        value.store(to, Ordering::Relaxed);
        if signal.wanted().is_some_and(|least| to >= least) {
            signal.notify();
        }
    }

    /// A producer publishing a value racing a task wanting it: once both are through, the task
    /// is done or queued to run, never waiting on a value already reached (22.2, the threshold
    /// handshake between two fences; mutants: the producer's fence gone; the task's fence gone).
    #[test]
    fn a_threshold_reached_while_a_task_wants_it_is_never_missed() {
        use loom::sync::atomic::AtomicU64;
        loom::model(|| {
            let ex = Executor::new(Arc::new(crate::io::MemoryIO::new()), 0).unwrap();
            let signal = Arc::new(Signal::default());
            let value = Arc::new(AtomicU64::new(0));
            let task = ex.spawn_watching(
                Class::Due,
                Box::new(AwaitsThreshold {
                    signal: signal.clone(),
                    value: value.clone(),
                    threshold: 1,
                }),
                &[&signal],
            );
            let producer = {
                let (signal, value) = (signal.clone(), value.clone());
                thread::spawn(move || publish(&value, &signal, 1))
            };
            let looked = ex.run_one();
            producer.join().unwrap();
            while ex.run_one() {}
            let done = task.is_done();
            ex.close();
            assert!(looked);
            assert!(done, "a task left waiting on a value already reached");
        });
    }

    /// A notify racing a task's look: a task whose look read the count the notify moved wants
    /// after the notify's reset, so the notify cannot erase its want, and a later value that
    /// reaches it wakes it. The notify is a producer's at a value below the task's threshold,
    /// made for a want a task already done left standing (22.2; the auditor's P1-E11-T1 model;
    /// mutant: the count moved outside the mutex, before the reset).
    #[test]
    fn a_notify_cannot_erase_the_want_of_a_task_that_read_its_moved_count() {
        use loom::sync::atomic::AtomicU64;
        loom::model(|| {
            let ex = Arc::new(Executor::new(Arc::new(crate::io::MemoryIO::new()), 0).unwrap());
            let signal = Arc::new(Signal::default());
            let value = Arc::new(AtomicU64::new(3));
            let awaits = |threshold| {
                ex.spawn_watching(
                    Class::Due,
                    Box::new(AwaitsThreshold {
                        signal: signal.clone(),
                        value: value.clone(),
                        threshold,
                    }),
                    &[&signal],
                )
            };
            // Done at once, its want of 3 left standing until the next notify.
            let done = awaits(3);
            let first = ex.run_one();
            let standing = signal.wanted();
            let live = awaits(5);
            let producer = {
                let (signal, value) = (signal.clone(), value.clone());
                thread::spawn(move || publish(&value, &signal, 4))
            };
            let looked = ex.run_one();
            producer.join().unwrap();
            publish(&value, &signal, 5);
            while ex.run_one() {}
            let finished = (done.is_done(), live.is_done());
            let (wanted, listed) = (signal.wanted(), signal.listed());
            Arc::try_unwrap(ex).ok().unwrap().close();
            assert!(first && looked);
            assert_eq!(standing, Some(3));
            assert_eq!(
                finished,
                (true, true),
                "the task wanting 5 left waiting: wanted {wanted:?}, {listed} listed"
            );
        });
    }

    /// Rests watching `signal`; counts its drops.
    struct RestsWatching {
        signal: Arc<Signal>,
        drops: Arc<loom::sync::atomic::AtomicUsize>,
    }

    impl Task for RestsWatching {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            let read = self.signal.read();
            Ok(Step::Idle(Watched::of(&[(&self.signal, read)])))
        }
    }

    impl Drop for RestsWatching {
        fn drop(&mut self) {
            crate::storage::atoll::unwind::abort_if_unwinding();
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A removal racing a step's end: whichever sees the other, the job goes once, off the
    /// signal, out of the table, and nothing is left listed (22.3; mutant: the remover letting
    /// the job go while a runner steps it, so the end lists it after it left).
    #[test]
    fn a_removal_racing_a_steps_end_lets_the_job_go_once() {
        use loom::sync::atomic::AtomicUsize;
        loom::model(|| {
            let ex = Arc::new(Executor::new(Arc::new(crate::io::MemoryIO::new()), 0).unwrap());
            let signal = Arc::new(Signal::default());
            let drops = Arc::new(AtomicUsize::new(0));
            let task = ex.spawn_watching(
                Class::Due,
                Box::new(RestsWatching {
                    signal: signal.clone(),
                    drops: drops.clone(),
                }),
                &[&signal],
            );
            let remover = {
                let task = task.clone();
                thread::spawn(move || task.remove())
            };
            let claimed = ex.run_one();
            remover.join().unwrap();
            let left = (
                signal.listed(),
                signal.declared(),
                lock(&ex.shared.queue).tasks,
                lock(&ex.shared.jobs).len(),
                drops.load(Ordering::Relaxed),
            );
            Arc::try_unwrap(ex).ok().unwrap().close();
            drop(task);
            assert_eq!(left, (0, 0, 0, 0, 1), "claimed {claimed}");
        });
    }

    /// Counts its steps, and is done.
    struct Count(Arc<loom::sync::atomic::AtomicUsize>);

    impl Task for Count {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Step::Done)
        }
    }

    /// Counts the wakes of an external runner's host.
    struct Wakes(Arc<loom::sync::atomic::AtomicUsize>);

    impl std::task::Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A link racing an external runner's registration: the work is either claimed by the
    /// runner or its host is woken (22.3; the auditor's model; mutant: a link that takes no idle
    /// runner's node).
    #[test]
    fn a_link_racing_an_external_registration_leaves_work_claimed_or_host_woken() {
        use loom::sync::atomic::AtomicUsize;
        loom::model(|| {
            let ex = Arc::new(Executor::new(Arc::new(crate::io::MemoryIO::new()), 0).unwrap());
            let runner = Arc::new(ex.runner());
            let units = Arc::new(AtomicUsize::new(0));
            let wakes = Arc::new(AtomicUsize::new(0));
            let producer = {
                let (ex, units) = (ex.clone(), units.clone());
                thread::spawn(move || {
                    ex.spawn(Class::Due, Box::new(Count(units)));
                })
            };
            let host = {
                let (wakes, runner) = (wakes.clone(), runner.clone());
                thread::spawn(move || {
                    let waker = std::task::Waker::from(Arc::new(Wakes(wakes)));
                    runner.run_one(&waker)
                })
            };
            producer.join().unwrap();
            let ran = host.join().unwrap();
            let observed = (units.load(Ordering::Relaxed), wakes.load(Ordering::Relaxed));
            drop(runner);
            Arc::try_unwrap(ex).ok().unwrap().close();
            assert_eq!(observed.0, usize::from(ran));
            assert!(
                ran || observed.1 > 0,
                "a registered host was not woken for queued work"
            );
        });
    }

    /// An external runner dropped while a link races to move its node: the node goes off both
    /// lists and its slot is freed exactly once, its host woken at most once (22.3; the auditor's
    /// model).
    #[test]
    fn an_external_cancel_racing_a_link_frees_its_node_exactly_once() {
        use loom::sync::atomic::AtomicUsize;
        loom::model(|| {
            let ex = Arc::new(Executor::new(Arc::new(crate::io::MemoryIO::new()), 0).unwrap());
            let runner = ex.runner();
            let n = runner.node;
            let units = Arc::new(AtomicUsize::new(0));
            let wakes = Arc::new(AtomicUsize::new(0));
            let waker = std::task::Waker::from(Arc::new(Wakes(wakes.clone())));
            let empty = !runner.run_one(&waker);
            let producer = {
                let (ex, units) = (ex.clone(), units.clone());
                thread::spawn(move || {
                    ex.spawn(Class::Due, Box::new(Count(units)));
                })
            };
            let cancel = thread::spawn(move || drop(runner));
            producer.join().unwrap();
            cancel.join().unwrap();
            let observed = {
                let queue = lock(&ex.shared.queue);
                (
                    queue.nodes[n].node.is_none(),
                    queue.nodes[n].on,
                    queue.free.iter().filter(|&&m| m == n).count(),
                    queue.idle.len(),
                    queue.to_wake.len(),
                    wakes.load(Ordering::Relaxed),
                )
            };
            Arc::try_unwrap(ex).ok().unwrap().close();
            drop(waker);
            assert!(empty && observed.0);
            assert_eq!(observed.1, On::Nowhere);
            assert_eq!(observed.2, 1);
            assert_eq!((observed.3, observed.4), (0, 0));
            assert!(observed.5 <= 1);
        });
    }

    /// Waits on a completion nothing finishes.
    struct Idle;

    impl Task for Idle {
        fn step(&mut self, _cx: &Cx) -> Result<Step> {
            Ok(Step::Wait(Completion::new_wait()))
        }
    }

    /// A count read as moved shows the change its notifier published before it (22.2; the
    /// auditor's model; mutant: the count moved with no release).
    #[test]
    fn a_moved_count_shows_the_change_published_before_it() {
        use loom::sync::atomic::AtomicUsize;
        loom::model(|| {
            let signal = Arc::new(Signal::default());
            let value = Arc::new(AtomicUsize::new(0));
            let publisher = {
                let (signal, value) = (signal.clone(), value.clone());
                thread::spawn(move || {
                    value.store(1, Ordering::Relaxed);
                    signal.notify();
                })
            };
            let moved = signal.read() != 0;
            let seen = value.load(Ordering::Relaxed);
            publisher.join().unwrap();
            assert!(
                !moved || seen == 1,
                "a moved count over the change before it"
            );
        });
    }

    /// Close racing a wake of an idle task: whichever comes first, the task ends closed and
    /// linked nowhere (22.3; the auditor's model; mutant: the move to queued made before the
    /// queue's mutex is taken, which close then misses).
    #[test]
    fn close_racing_a_wake_leaves_the_task_closed_and_unlinked() {
        loom::model(|| {
            let ex = Executor::new(Arc::new(crate::io::MemoryIO::new()), 0).unwrap();
            let task = ex.spawn(Class::Due, Box::new(Idle));
            let stepped = ex.run_one();
            let shared = ex.shared.clone();
            let h = task.0.clone();
            let waker = thread::spawn(move || task.wake());
            ex.close();
            waker.join().unwrap();
            assert!(stepped);
            assert_eq!(h.state.load(Ordering::Acquire), CLOSED);
            assert_eq!(links(&shared, &h), 0, "close left a closed task linked");
        });
    }
}
