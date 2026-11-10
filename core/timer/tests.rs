use super::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc, Barrier,
};
use std::task::Wake;
use std::time::Duration;

#[test]
fn cancellation_removes_the_action_and_drops_its_waker_outside_the_lock() {
    struct OnDrop {
        timer: &'static Timer,
        drops: Arc<AtomicUsize>,
        lock_free: Arc<AtomicBool>,
    }
    impl Wake for OnDrop {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for OnDrop {
        fn drop(&mut self) {
            self.lock_free
                .store(self.timer.due.try_lock().is_some(), Ordering::SeqCst);
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    println!(
        "TIMER_LAYOUT timed={} option_timed={} entry={}",
        std::mem::size_of::<Timed>(),
        std::mem::size_of::<Option<Timed>>(),
        std::mem::size_of::<Entry>()
    );
    let timer = local_timer();
    let drops = Arc::new(AtomicUsize::new(0));
    let lock_free = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(OnDrop {
        timer,
        drops: drops.clone(),
        lock_free: lock_free.clone(),
    }));
    let timed = arm(
        timer,
        Instant::now() + Duration::from_secs(60),
        Fire::Wake(waker),
    );
    assert_eq!(timer.due.lock().heap.len(), 1);
    drop(timed);
    assert_eq!(timer.due.lock().heap.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        lock_free.load(Ordering::SeqCst),
        "drop ran under the timer lock"
    );
}

#[test]
fn cancelling_reuses_the_slot_without_retaining_the_completion() {
    let timer = local_timer();
    let at = Instant::now() + Duration::from_secs(60);
    let completion = Completion::new_remote_wait();
    for _ in 0..4096 {
        let timed = arm(timer, at, Fire::Complete(completion.clone()));
        assert_eq!(completion.handles(), 2);
        drop(timed);
        assert_eq!(completion.handles(), 1);
        let due = timer.due.lock();
        assert_eq!(due.heap.len(), 0);
        assert_eq!(
            due.entries.len(),
            1,
            "slab bounded by simultaneous registrations"
        );
    }
    assert!(!completion.finished());
}

#[test]
fn cancelling_before_take_never_fires_and_cancelling_after_take_fires_once() {
    for cancel_before_take in [true, false] {
        let timer = local_timer();
        let at = Instant::now() + Duration::from_secs(60);
        let completion = Completion::new_remote_wait();
        let wakes = Arc::new(AtomicUsize::new(0));
        completion.set_waker(&Waker::from(Arc::new(Count(wakes.clone()))));
        let timed = arm(timer, at, Fire::Complete(completion.clone()));
        let taken = Arc::new(Barrier::new(2));
        let fire = Arc::new(Barrier::new(2));
        if cancel_before_take {
            drop(timed);
        } else {
            let t = taken.clone();
            let f = fire.clone();
            let worker = std::thread::spawn(move || {
                let mut ready = Vec::new();
                timer.due.lock().take_ready(at, &mut ready);
                t.wait();
                f.wait();
                for action in ready {
                    action.fire();
                }
            });
            taken.wait();
            assert!(!completion.finished());
            drop(timed);
            assert!(timer.due.lock().heap.is_empty());
            fire.wait();
            worker.join().unwrap();
            assert!(completion.finished());
            assert_eq!(wakes.load(Ordering::SeqCst), 1);
            continue;
        }
        let mut ready = Vec::new();
        timer.due.lock().take_ready(at, &mut ready);
        assert!(ready.is_empty());
        assert!(!completion.finished());
        assert_eq!(wakes.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn a_registration_dropped_after_expiry_cannot_cancel_a_reused_slot() {
    let timer = local_timer();
    let at = Instant::now();
    let first = Completion::new_remote_wait();
    let old = arm(timer, at, Fire::Complete(first.clone()));
    let mut ready = Vec::new();
    timer.due.lock().take_ready(at, &mut ready);
    let second = Completion::new_remote_wait();
    let next = arm(
        timer,
        at + Duration::from_secs(60),
        Fire::Complete(second.clone()),
    );
    assert_eq!(old.key.slot, next.key.slot);
    drop(old);
    assert_eq!(timer.due.lock().heap.len(), 1);
    for action in ready {
        action.fire();
    }
    assert!(first.finished());
    assert!(!second.finished());
    drop(next);
    assert!(timer.due.lock().heap.is_empty());
    assert_eq!(second.handles(), 1);
}

#[test]
fn a_blocker_finishing_after_take_before_fire_is_not_woken_twice() {
    let timer = local_timer();
    let at = Instant::now();
    let completion = Completion::new_remote_wait();
    let wakes = Arc::new(AtomicUsize::new(0));
    completion.set_waker(&Waker::from(Arc::new(Count(wakes.clone()))));
    let timed = arm(timer, at, Fire::Complete(completion.clone()));
    let mut ready = Vec::new();
    timer.due.lock().take_ready(at, &mut ready);
    completion.error(crate::CompletionError::ShortWrite);
    drop(timed);
    for action in ready {
        action.fire();
    }
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(
        completion.get_error(),
        Some(crate::CompletionError::ShortWrite)
    );
}

#[test]
fn heap_and_slot_positions_match_an_independent_ordered_set() {
    let mut due = Deadlines::default();
    let mut live = Vec::new();
    let mut expected = std::collections::BTreeSet::new();
    let at = Instant::now();
    let mut random = 11u64;
    let mut ready = Vec::new();
    for _ in 0..20_000 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        if live.is_empty() || random % 3 != 0 {
            let time = at + Duration::from_nanos(random % 101);
            let key = due.insert(time, Fire::Complete(Completion::new_remote_wait()));
            live.push((key, time));
            expected.insert((time, key.slot));
        } else {
            let (key, time) = live.swap_remove((random as usize) % live.len());
            assert!(due.remove(key).is_some());
            expected.remove(&(time, key.slot));
        }
        assert_eq!(due.first(), expected.first().map(|x| x.0));
        assert_eq!(due.heap.len(), expected.len());
        for (position, &slot) in due.heap.iter().enumerate() {
            assert_eq!(due.entries[slot].position, position);
            assert!(due.entries[slot].action.is_some());
        }
    }
    due.take_ready(at + Duration::from_nanos(101), &mut ready);
    assert_eq!(ready.len(), expected.len());
    assert!(due.heap.is_empty());
}

#[test]
fn generation_exhaustion_panics_before_mutating_the_queue() {
    let mut due = Deadlines::default();
    let key = due.insert(
        Instant::now(),
        Fire::Complete(Completion::new_remote_wait()),
    );
    due.entries[key.slot].generation = u64::MAX;
    let key = Key {
        generation: u64::MAX,
        ..key
    };
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| due.remove(key)));
    assert!(failed.is_err());
    assert_eq!(due.heap.len(), 1);
    assert!(due.entries[key.slot].action.is_some());
}

#[test]
fn a_real_deadline_can_be_cancelled_or_fired() {
    let (send, recv) = mpsc::channel();
    struct SendWake(mpsc::Sender<()>);
    impl Wake for SendWake {
        fn wake(self: Arc<Self>) {
            let _ = self.0.send(());
        }
    }
    let at = Instant::now() + Duration::from_millis(10);
    let timed = wake_at(at, Waker::from(Arc::new(SendWake(send)))).unwrap();
    recv.recv_timeout(Duration::from_secs(2)).unwrap();
    drop(timed);
}

fn local_timer() -> &'static Timer {
    Box::leak(Box::new(Timer::default()))
}

fn arm(timer: &'static Timer, at: Instant, fire: Fire) -> Timed {
    let completion = match &fire {
        Fire::Complete(c) => c.addr(),
        Fire::Wake(_) => None,
    };
    let key = timer.due.lock().insert(at, fire);
    Timed {
        timer,
        key,
        completion,
        at,
    }
}

struct Count(Arc<AtomicUsize>);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn firing_a_batch_runs_all_non_panicking_actions_outside_the_timer_lock() {
    let timer = local_timer();
    let at = Instant::now();
    let wakes = Arc::new(AtomicUsize::new(0));
    let a = arm(
        timer,
        at,
        Fire::Wake(Waker::from(Arc::new(Count(wakes.clone())))),
    );
    let b = arm(
        timer,
        at,
        Fire::Wake(Waker::from(Arc::new(Count(wakes.clone())))),
    );
    let mut ready = Vec::new();
    timer.due.lock().take_ready(at, &mut ready);
    fire_ready(&mut ready);
    assert_eq!(wakes.load(Ordering::SeqCst), 2);
    assert!(ready.is_empty());
    drop((a, b));
}

#[test]
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn a_panicking_timer_action_ends_the_process_instead_of_losing_its_worker() {
    let test = concat!(
        module_path!(),
        "::a_panicking_timer_action_ends_the_process_instead_of_losing_its_worker"
    );
    let ended = crate::child_test::in_child(test, || {
        struct Panic;
        impl Wake for Panic {
            fn wake(self: Arc<Self>) {
                panic!("timer action probe");
            }
        }
        let worker = std::thread::spawn(|| {
            let timer = Timer::default();
            timer
                .due
                .lock()
                .insert(Instant::now(), Fire::Wake(Waker::from(Arc::new(Panic))));
            timer.run();
        });
        let _ = worker.join();
        crate::child_test::record(
            "worker-returned",
            "the process accepted a dead timer worker",
        );
    });
    if let Some(ended) = ended {
        assert!(
            !ended.hung,
            "child hung instead of observing the worker's outcome"
        );
        assert!(
            ended.aborted,
            "timer worker died quietly: code={:?} stderr={}",
            ended.code, ended.stderr
        );
        assert!(
            ended.stderr.contains("timer action probe"),
            "{}",
            ended.stderr
        );
    }
}

#[test]
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn an_internal_timer_worker_panic_cannot_leave_the_timer_accepting_actions() {
    let test = concat!(
        module_path!(),
        "::an_internal_timer_worker_panic_cannot_leave_the_timer_accepting_actions"
    );
    let ended = crate::child_test::in_child(test, || {
        let worker = std::thread::spawn(|| {
            let timer = Timer::default();
            {
                let mut due = timer.due.lock();
                let key = due.insert(
                    Instant::now(),
                    Fire::Complete(Completion::new_remote_wait()),
                );
                due.entries[key.slot].generation = u64::MAX;
            }
            timer.run();
        });
        let _ = worker.join();
        crate::child_test::record(
            "worker-returned",
            "the process accepted a dead timer worker",
        );
    });
    if let Some(ended) = ended {
        assert!(!ended.hung);
        assert!(
            ended.aborted,
            "timer worker died quietly: code={:?} stderr={}",
            ended.code, ended.stderr
        );
        assert!(ended.stderr.contains("timer slot generation exhausted"));
    }
}

#[test]
#[cfg(all(unix, not(any(shuttle, atoll_loom, miri))))]
fn cancelling_a_timer_during_a_callers_unwind_does_not_turn_it_into_an_abort() {
    let test = concat!(
        module_path!(),
        "::cancelling_a_timer_during_a_callers_unwind_does_not_turn_it_into_an_abort"
    );
    let ended = crate::child_test::in_child(test, || {
        let timer = local_timer();
        let c = Completion::new_remote_wait();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _timed = arm(
                timer,
                Instant::now() + Duration::from_secs(60),
                Fire::Complete(c.clone()),
            );
            panic!("caller unwind probe");
        }));
        crate::child_test::record(
            "cancelled",
            &format!("{} {}", unwound.is_err(), c.handles()),
        );
    });
    if let Some(ended) = ended {
        assert!(!ended.hung);
        assert!(
            !ended.aborted,
            "cancelling during the caller's unwind must not abort"
        );
        assert_eq!(ended.code, Some(0));
        assert_eq!(
            crate::child_test::recorded(&ended, "cancelled").trim(),
            "true 1"
        );
    }
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "a yield has no deadline wait")]
fn a_yield_cannot_be_registered_as_a_deadline_wait() {
    let _timed = complete_at(Instant::now(), Completion::new_yield());
}
