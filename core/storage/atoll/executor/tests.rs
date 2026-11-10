//! The executor's core on test jobs (DESIGN.md 22.15, proofs 1, 2 and 5, and the workers of 22.7).

use super::*;

#[cfg(not(atoll_loom))]
mod native {
    use super::*;
    use crate::io::crash::CrashIO;
    use crate::io::{FileSyncType, OpenFlags};
    use crate::storage::atoll::locks::Mutex as Lock;
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
            let queue = self
                .shared
                .upgrade()
                .is_none_or(|shared| shared.queue.try_lock().is_some());
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
