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
        fn step(&mut self) -> Result<Step> {
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
        fn step(&mut self) -> Result<Step> {
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
        assert!(ex.run_one(), "the first step");
        assert!(ex.run_one(), "queued again by the wake during its step");
        assert!(task.is_done());
        assert!(!ex.run_one(), "queued once, not twice");
        ex.close();
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
        fn step(&mut self) -> Result<Step> {
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
        assert!(!task.is_done());
        c.complete(0);
        assert!(done_rx.recv_timeout(BOUND).is_ok(), "the task ran again");
        ex.close();
    }

    /// Syncs a file of a pumped backend and waits on it, then is done.
    struct Syncs {
        file: Arc<dyn crate::io::File>,
        sync: Option<Completion>,
        done: mpsc::Sender<()>,
    }

    impl Task for Syncs {
        fn step(&mut self) -> Result<Step> {
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
    /// and once it holds nothing they park: no step taken over a quiet window (22.5, 22.7;
    /// mutants: a worker that never steps the backend, so the task never ends; one that steps an
    /// empty backend without parking).
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
        assert!(
            done_rx.recv_timeout(BOUND).is_ok(),
            "the worker delivered the task's sync"
        );
        // Long enough for the worker to find nothing and park.
        std::thread::sleep(Duration::from_millis(50));
        let before = io.steps();
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            io.steps(),
            before,
            "steps taken by a worker with nothing to do"
        );
        assert_eq!(crash.in_flight(), 0);
        ex.close();
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
        fn step(&mut self) -> Result<Step> {
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

    /// A pumped backend whose steps cannot say what they found: the trait's default `try_step`.
    struct Unsaying(Arc<CrashIO>);

    impl crate::io::Clock for Unsaying {
        fn current_time_monotonic(&self) -> crate::io::clock::MonotonicInstant {
            self.0.current_time_monotonic()
        }
        fn current_time_wall_clock(&self) -> crate::io::clock::WallClockInstant {
            self.0.current_time_wall_clock()
        }
    }

    impl IO for Unsaying {
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
            self.0.step()
        }
    }

    /// Close joins workers on a pumped backend whose steps cannot say they found nothing, and
    /// which so never come to park (mutant: the workers read close only where they would park,
    /// and close waits for them for ever).
    #[test]
    fn close_joins_workers_on_a_backend_whose_steps_say_nothing() {
        let ex = Executor::new(Arc::new(Unsaying(Arc::new(CrashIO::new(4)))), 2).unwrap();
        // Long enough for the workers to be in their loops.
        std::thread::sleep(Duration::from_millis(20));
        let (closed_tx, closed_rx) = mpsc::channel();
        std::thread::spawn(move || {
            ex.close();
            let _ = closed_tx.send(());
        });
        assert!(
            closed_rx.recv_timeout(BOUND).is_ok(),
            "close never joined its workers"
        );
    }

    /// Watches three signals: each step reads their counts, then looks at `ready`; unmet, it
    /// says idle on the counts it read; met, it is done.
    struct Watches {
        signals: [Arc<Signal>; 3],
        ready: Arc<AtomicBool>,
    }

    impl Task for Watches {
        fn step(&mut self) -> Result<Step> {
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
        assert!(ex.run_one(), "the first look");
        assert!(ex.run_one(), "queued again by the notify it missed");
        assert!(task.is_done());
        ex.close();
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
        assert!(ex.run_one(), "the first look");
        assert!(ex.run_one(), "woken from the list");
        assert!(task.is_done());
        ex.close();
    }

    /// A task waits on any of its signals: each, notified alone after the task rested, wakes it;
    /// a task resting again is listed once on each, and a task done is on none (22.2, proof 1's
    /// wait-any).
    #[test]
    fn each_signal_watched_wakes_a_resting_task_alone_and_lists_hold_it_once() {
        for which in 0..3 {
            let ex = executor(0);
            let (task, signals, ready) = watching(&ex);
            assert!(ex.run_one());
            assert!(!ex.run_one(), "{which}: resting");
            assert!(signals.iter().all(|s| s.listed() == 1));
            // Notified with its condition still unmet: it looks, rests, and is listed once.
            signals[(which + 1) % 3].notify();
            assert!(ex.run_one());
            assert!(!ex.run_one());
            assert!(
                signals.iter().all(|s| s.listed() == 1),
                "{which}: listed once"
            );
            ready.store(true, O::Release);
            signals[which].notify();
            assert!(ex.run_one(), "{which}: woken by its signal alone");
            assert!(task.is_done());
            assert!(
                signals.iter().all(|s| s.listed() == 0),
                "{which}: a done task left listed"
            );
            ex.close();
        }
    }
}

#[cfg(atoll_loom)]
mod loom_tests {
    use super::*;
    use loom::thread;

    fn shared() -> Arc<Shared> {
        Arc::new(Shared {
            queue: Mutex::new(Queue {
                lists: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
                idle: VecDeque::new(),
                tasks: 1,
                closing: false,
            }),
            closed: AtomicBool::new(false),
            jobs: Mutex::new(Vec::new()),
            io: Arc::new(crate::io::MemoryIO::new()),
        })
    }

    fn header(shared: &Arc<Shared>, state: u8) -> Arc<Header> {
        Arc::new(Header {
            state: AtomicU8::new(state),
            class: AtomicU8::new(Class::Due as u8),
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

    /// Two wakes of an idle task link it once (22.3; proof 2; mutant: the move to queued made
    /// before the queue's mutex is taken).
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
            if signal.register(&h, read) {
                shared.rest(&h);
            } else {
                shared.requeue(&h);
            }
            notifier.join().unwrap();
            assert_eq!(h.state.load(Ordering::Acquire), QUEUED);
            assert_eq!(links(&shared, &h), 1);
        });
    }
}
