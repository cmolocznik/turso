use super::*;
use crate::sync::atomic::AtomicBool;

const OBJECTS: usize = 64;

/// Readers take the current object inside a step and check it stays alive to the step's end,
/// while a writer replaces it, retires the old one and destroys whatever it reclaims.
fn readers_never_lose_what_they_hold(rounds: usize) {
    let epochs = Arc::new(Epochs::new());
    let alive: Arc<Vec<AtomicBool>> =
        Arc::new((0..OBJECTS).map(|i| AtomicBool::new(i == 0)).collect());
    let current = Arc::new(AtomicU64::new(0));
    let free = Arc::new(Mutex::new((1..OBJECTS).collect::<Vec<usize>>()));
    let limbo: Arc<Limbo<usize>> = Arc::new(Limbo::default());
    let mut handles = Vec::new();
    for _ in 0..2 {
        let (epochs, alive, current) = (epochs.clone(), alive.clone(), current.clone());
        handles.push(crate::thread::spawn(move || {
            for _ in 0..rounds {
                let _step = epochs.enter();
                let held = current.load(Ordering::SeqCst) as usize;
                assert!(alive[held].load(Ordering::SeqCst), "{held} dead when read");
                crate::thread::yield_now();
                assert!(
                    alive[held].load(Ordering::SeqCst),
                    "{held} reclaimed while a step held it"
                );
            }
        }));
    }
    {
        // The writer, last: it takes what is left.
        handles.push(crate::thread::spawn(move || {
            for _ in 0..rounds {
                let Some(next) = free.lock().pop() else {
                    for k in limbo.reclaim(&epochs) {
                        alive[k].store(false, Ordering::SeqCst);
                        free.lock().push(k);
                    }
                    crate::thread::yield_now();
                    continue;
                };
                alive[next].store(true, Ordering::SeqCst);
                let old = current.swap(next as u64, Ordering::SeqCst) as usize;
                limbo.retire(&epochs, old);
                for k in limbo.reclaim(&epochs) {
                    alive[k].store(false, Ordering::SeqCst);
                    free.lock().push(k);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[cfg(not(shuttle))]
#[test]
fn a_retired_object_outlives_every_step_that_could_hold_it() {
    readers_never_lose_what_they_hold(20_000);
}

#[cfg(shuttle)]
#[test]
fn a_retired_object_outlives_every_step_that_could_hold_it_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(|| readers_never_lose_what_they_hold(20), 2_000);
}

#[cfg(not(shuttle))]
#[test]
fn nested_steps_announce_once_and_leave_with_the_outermost() {
    let epochs = Epochs::new();
    assert_eq!(epochs.horizon(), IDLE);
    let outer = epochs.enter();
    let announced = epochs.horizon();
    epochs.stamp();
    let inner = epochs.enter();
    assert_eq!(
        epochs.horizon(),
        announced,
        "an inner step announces nothing new"
    );
    drop(inner);
    assert_eq!(epochs.horizon(), announced, "the outer step still holds");
    drop(outer);
    assert_eq!(epochs.horizon(), IDLE);
}
