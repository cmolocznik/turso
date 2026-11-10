use super::*;
use crate::sync::atomic::AtomicBool;

const OBJECTS: usize = 64;

/// Rounds of a threaded test on threads. Miri interprets every access and checks it for data races
/// and undefined behaviour, thousands of times slower, so it runs fewer, over several schedule
/// seeds: with its default chance of preempting a thread at each basic block, one in a hundred,
/// two hundred rounds switch threads mid-operation many times in every run.
#[cfg(not(shuttle))]
const ROUNDS: usize = if cfg!(miri) { 200 } else { 20_000 };

/// Readers take the current object inside a step and check it stays alive to the step's end,
/// while a writer replaces it, retires the old one and destroys whatever it reclaims.
fn readers_never_lose_what_they_hold(rounds: usize) {
    let epochs = Arc::new(Epochs::new());
    let alive: Arc<Vec<AtomicBool>> =
        Arc::new((0..OBJECTS).map(|i| AtomicBool::new(i == 0)).collect());
    let current = Arc::new(AtomicU64::new(0));
    let free = Arc::new(Mutex::new((1..OBJECTS).collect::<Vec<usize>>()));
    let retired: Arc<Retired<usize>> = Arc::new(Retired::default());
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
                    for k in retired.reclaim(&epochs) {
                        alive[k].store(false, Ordering::SeqCst);
                        free.lock().push(k);
                    }
                    crate::thread::yield_now();
                    continue;
                };
                alive[next].store(true, Ordering::SeqCst);
                let old = current.swap(next as u64, Ordering::SeqCst) as usize;
                retired.retire(&epochs, old);
                for k in retired.reclaim(&epochs) {
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
    readers_never_lose_what_they_hold(ROUNDS);
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

/// A wait ends with the step that holds the horizon below its epoch, and with no other: not a
/// step that announced the epoch or above, nor an inner step of the one holding it.
#[cfg(not(shuttle))]
#[test]
fn a_wait_ends_with_the_step_that_holds_it_back() {
    let epochs = Epochs::new();
    std::thread::scope(|s| {
        let epochs = &epochs;
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (go, go_rx) = std::sync::mpsc::channel::<&str>();
        let (done, done_rx) = std::sync::mpsc::channel();
        let holder = s.spawn(move || {
            let outer = epochs.enter();
            entered.send(()).unwrap();
            assert_eq!(go_rx.recv().unwrap(), "inner");
            drop(epochs.enter());
            done.send(()).unwrap();
            assert_eq!(go_rx.recv().unwrap(), "leave");
            drop(outer);
            done.send(()).unwrap();
        });
        entered_rx.recv().unwrap();
        let stamp = epochs.stamp();
        let waited = crate::storage::atoll::notify::wait();
        assert!(
            epochs.wait_for(stamp, waited.clone()),
            "a step under way holds the horizon below the stamp"
        );
        assert!(!waited.finished());
        // A step entered after the stamp announces it or above.
        s.spawn(|| drop(epochs.enter())).join().unwrap();
        assert!(!waited.finished(), "a step that held nothing back ended it");
        go.send("inner").unwrap();
        done_rx.recv().unwrap();
        assert!(!waited.finished(), "an inner step's end ended it");
        go.send("leave").unwrap();
        done_rx.recv().unwrap();
        assert!(
            waited.finished(),
            "the step holding it back left and it still waits"
        );
        holder.join().unwrap();
        assert!(
            !epochs.wait_for(stamp, crate::storage::atoll::notify::wait()),
            "a wait already reached"
        );
    });
}

/// A wait for a new stamp ends once every step under way at the stamp has ended, whatever began
/// after.
#[cfg(not(shuttle))]
#[test]
fn a_wait_for_a_new_stamp_waits_for_the_steps_under_way_and_no_later_one() {
    let epochs = Epochs::new();
    assert!(
        !epochs.wait_for(epochs.stamp(), crate::storage::atoll::notify::wait()),
        "no step is under way"
    );
    std::thread::scope(|s| {
        let epochs = &epochs;
        let mut threads = Vec::new();
        let mut leaves = Vec::new();
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let mut step = || {
            let (leave, leave_rx) = std::sync::mpsc::channel::<()>();
            let entered = entered.clone();
            threads.push(s.spawn(move || {
                let _step = epochs.enter();
                entered.send(()).unwrap();
                let _ = leave_rx.recv();
            }));
            entered_rx.recv().unwrap();
            leaves.push(leave);
        };
        step();
        let ended = crate::storage::atoll::notify::wait();
        assert!(
            epochs.wait_for(epochs.stamp(), ended.clone()),
            "a step is under way"
        );
        step();
        assert!(!ended.finished());
        leaves[0].send(()).unwrap();
        threads.remove(0).join().unwrap();
        assert!(
            ended.finished(),
            "the step under way at its start ended, and it still waits for a later one"
        );
        leaves[1].send(()).unwrap();
    });
}

/// A thread that exits gives its slot back, so the registry holds as many slots as threads have
/// stepped at once, not every thread that ever did.
#[cfg(not(shuttle))]
#[test]
fn a_thread_that_exits_gives_its_slot_back() {
    let epochs = Arc::new(Epochs::new());
    for _ in 0..32 {
        let epochs = epochs.clone();
        // Joined outside a scope: the thread has exited, and its thread locals are gone.
        std::thread::spawn(move || drop(epochs.enter()))
            .join()
            .unwrap();
    }
    assert_eq!(
        epochs.participants(),
        1,
        "32 threads stepped one after another"
    );
    let both = Arc::new(std::sync::Barrier::new(2));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let (epochs, both) = (epochs.clone(), both.clone());
            std::thread::spawn(move || {
                let _step = epochs.enter();
                both.wait();
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(epochs.participants(), 2, "two threads stepped at once");
}

/// Steps read the current object and check it stays alive to their end, nesting an inner step,
/// while two other threads each replace it, wait for the steps that may hold the old one, and
/// destroy it once the wait ends; they wait from inside a step of their own, as a claim does, and
/// from outside any, so wakes and registrations race each other too. A wait ended too soon
/// destroys an object a step holds; a missed wake leaves a waiter waiting after every other
/// thread is done.
fn waits_end_with_the_steps_holding_them_back(rounds: usize) {
    let epochs = Arc::new(Epochs::new());
    // A fresh object every round: a waiter destroys the object it took out, which may be the
    // other's, and an object used again could be the current one again by the time it does.
    let alive: Arc<Vec<AtomicBool>> = Arc::new(
        (0..1 + 2 * rounds)
            .map(|i| AtomicBool::new(i == 0))
            .collect(),
    );
    let current = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let (epochs, alive, current) = (epochs.clone(), alive.clone(), current.clone());
        handles.push(crate::thread::spawn(move || {
            for _ in 0..rounds {
                let _outer = epochs.enter();
                let held = current.load(Ordering::SeqCst) as usize;
                assert!(alive[held].load(Ordering::SeqCst), "{held} dead when read");
                crate::thread::yield_now();
                drop(epochs.enter());
                crate::thread::yield_now();
                assert!(
                    alive[held].load(Ordering::SeqCst),
                    "{held} destroyed once a wait ended while a step held it"
                );
            }
        }));
    }
    for waiter in 0..2 {
        let (epochs, alive, current) = (epochs.clone(), alive.clone(), current.clone());
        handles.push(crate::thread::spawn(move || {
            for round in 0..rounds {
                // Each waiter its own objects, never the first: odd ones and even ones.
                let next = 1 + waiter + 2 * round;
                alive[next].store(true, Ordering::SeqCst);
                let old = current.swap(next as u64, Ordering::SeqCst) as usize;
                let waited = crate::storage::atoll::notify::wait();
                let waits = if round % 2 == 0 {
                    let _step = epochs.enter();
                    epochs.wait_for(epochs.stamp(), waited.clone())
                } else {
                    epochs.wait_for(epochs.stamp(), waited.clone())
                };
                while waits && !waited.finished() {
                    crate::thread::yield_now();
                }
                alive[old].store(false, Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[cfg(not(shuttle))]
#[test]
fn waits_end_with_the_steps_holding_them_back_on_threads() {
    waits_end_with_the_steps_holding_them_back(ROUNDS);
}

#[cfg(shuttle)]
#[test]
fn waits_end_with_the_steps_holding_them_back_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(
        || waits_end_with_the_steps_holding_them_back(20),
        2_000,
    );
}

/// Threads alive at once hold a slot each, across several chunks of the registry. Only the last
/// to join, in the third chunk, is in a step, so the horizon falls below a later stamp only if a
/// scan reads that far. Threads that exit give their slots back, and later ones take them again.
#[cfg(not(shuttle))]
#[test]
fn the_registry_grows_by_chunks_a_scan_reads_whole() {
    let epochs = Arc::new(Epochs::new());
    let threads = 2 * CHUNK_SLOTS + 1;
    let joined = Arc::new(std::sync::Barrier::new(threads));
    let exit = Arc::new(std::sync::Barrier::new(threads + 1));
    let (in_step, in_step_rx) = std::sync::mpsc::channel();
    let mut handles: Vec<_> = (0..threads - 1)
        .map(|_| {
            let (epochs, joined, exit) = (epochs.clone(), joined.clone(), exit.clone());
            std::thread::spawn(move || {
                drop(epochs.enter());
                joined.wait();
                exit.wait();
            })
        })
        .collect();
    handles.push({
        let (epochs, joined, exit) = (epochs.clone(), joined.clone(), exit.clone());
        std::thread::spawn(move || {
            joined.wait();
            let _step = epochs.enter();
            in_step.send(()).unwrap();
            exit.wait();
        })
    });
    in_step_rx.recv().unwrap();
    assert_eq!(epochs.participants(), threads, "a slot per thread alive");
    let stamp = epochs.stamp();
    assert!(
        epochs.horizon() < stamp,
        "a scan stopped short of the third chunk, where the one step under way is"
    );
    exit.wait();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(epochs.horizon(), IDLE);
    for _ in 0..threads {
        let epochs = epochs.clone();
        std::thread::spawn(move || drop(epochs.enter()))
            .join()
            .unwrap();
    }
    assert_eq!(
        epochs.participants(),
        threads,
        "slots given back were not taken again"
    );
}

/// Threads retire objects and reclaim concurrently, each reclaim handing back what it took: every
/// object comes back exactly once, and none is left once every step has ended.
fn retired_objects_come_back_once(rounds: usize) {
    let epochs = Arc::new(Epochs::new());
    let retired: Arc<Retired<usize>> = Arc::new(Retired::default());
    let back = Arc::new(Mutex::new(Vec::new()));
    let threads = 3;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (epochs, retired, back) = (epochs.clone(), retired.clone(), back.clone());
            crate::thread::spawn(move || {
                for round in 0..rounds {
                    {
                        let _step = epochs.enter();
                        retired.retire(&epochs, round * threads + t);
                        crate::thread::yield_now();
                    }
                    let got = retired.reclaim(&epochs);
                    back.lock().extend(got);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    // No step is left: reclaiming again and again gets its scan within a scan's worth of calls,
    // and that scan finds every object reusable.
    for _ in 0..epochs.participants().max(1) {
        back.lock().extend(retired.reclaim(&epochs));
    }
    let mut back = std::mem::take(&mut *back.lock());
    back.sort_unstable();
    assert_eq!(
        back,
        (0..rounds * threads).collect::<Vec<_>>(),
        "objects lost or handed back twice"
    );
}

#[cfg(not(shuttle))]
#[test]
fn retired_objects_come_back_once_on_threads() {
    retired_objects_come_back_once(ROUNDS);
}

#[cfg(shuttle)]
#[test]
fn retired_objects_come_back_once_in_shuttle_schedules() {
    crate::storage::atoll::testing::schedules(|| retired_objects_come_back_once(10), 2_000);
}

/// Two threads only retire and one only reclaims, with nothing else between them: whatever the
/// reclaimer reads of a retired object reaches it through the list alone, so Miri sees a push or a
/// take that orders too little.
#[cfg(not(shuttle))]
fn retired_objects_cross_threads_through_the_list_alone(rounds: usize) {
    let epochs = Arc::new(Epochs::new());
    let retired: Arc<Retired<usize>> = Arc::new(Retired::default());
    let retirers = 2;
    let handles: Vec<_> = (0..retirers)
        .map(|t| {
            let (epochs, retired) = (epochs.clone(), retired.clone());
            crate::thread::spawn(move || {
                for round in 0..rounds {
                    retired.retire(&epochs, round * retirers + t);
                }
            })
        })
        .collect();
    let reclaimer = {
        let (epochs, retired) = (epochs.clone(), retired.clone());
        crate::thread::spawn(move || {
            let mut back = Vec::new();
            while back.len() < rounds * retirers {
                back.extend(retired.reclaim(&epochs));
                crate::thread::yield_now();
            }
            back
        })
    };
    for h in handles {
        h.join().unwrap();
    }
    let mut back = reclaimer.join().unwrap();
    back.sort_unstable();
    assert_eq!(back, (0..rounds * retirers).collect::<Vec<_>>());
}

#[cfg(not(shuttle))]
#[test]
fn retired_objects_cross_threads_through_the_list_alone_on_threads() {
    retired_objects_cross_threads_through_the_list_alone(ROUNDS);
}

/// A scan that reads a thread's announcement of a later step finds its earlier steps done: a reader
/// reads an object in one step, the object is retired, and the reader enters another step,
/// announcing past the object's stamp, while a scan that then reads the announcement is under way
/// and destroys the object. Miri, like loom, orders every sequentially consistent fence after the
/// ones before it, so a scan fenced after the later announcement is ordered after the earlier step
/// whatever the announcement: the scan has to fence first and read the reader's slot after, which
/// a chunk's worth of slots held ahead of the reader's makes likely. Relaxed flags order none of
/// the reader's memory, so only the announcement orders the destruction after the read.
#[cfg(not(shuttle))]
#[test]
fn a_scan_reading_a_later_step_finds_the_earlier_ones_done() {
    use crate::sync::atomic::AtomicU8;
    use std::sync::Barrier;
    // Every other slot of the first chunk: the reader's is the last a scan of it reads.
    const AHEAD: usize = CHUNK_SLOTS - 1;
    let wait = |flag: &AtomicU8, at: u8, order: Ordering| {
        while flag.load(order) < at {
            crate::thread::yield_now();
        }
    };
    for _ in 0..if cfg!(miri) { 10 } else { 100 } {
        let epochs = Arc::new(Epochs::new());
        let (joined, done) = (
            Arc::new(Barrier::new(AHEAD + 1)),
            Arc::new(Barrier::new(AHEAD + 1)),
        );
        let holders: Vec<_> = (0..AHEAD)
            .map(|_| {
                let (epochs, joined, done) = (epochs.clone(), joined.clone(), done.clone());
                crate::thread::spawn(move || {
                    drop(epochs.enter());
                    joined.wait();
                    done.wait();
                })
            })
            .collect();
        joined.wait();
        let object = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(7u64))));
        // From the reader, relaxed; to it, a release, so its later step announces the stamp.
        let (to_reclaimer, to_reader) = (Arc::new(AtomicU8::new(0)), Arc::new(AtomicU8::new(0)));
        let reader = {
            let (epochs, object) = (epochs.clone(), object.clone());
            let (to_reclaimer, to_reader) = (to_reclaimer.clone(), to_reader.clone());
            crate::thread::spawn(move || {
                {
                    let _step = epochs.enter();
                    // SAFETY: the object is destroyed only once no step that could read it is left.
                    assert_eq!(unsafe { *object.load(Ordering::Acquire) }, 7);
                }
                to_reclaimer.store(1, Ordering::Relaxed);
                wait(&to_reader, 1, Ordering::Acquire);
                let _later = epochs.enter();
                wait(&to_reader, 2, Ordering::Relaxed);
            })
        };
        wait(&to_reclaimer, 1, Ordering::Relaxed);
        // Retired as the pointer: a box would claim it whole while the reader may still read it.
        let retired: Retired<*mut u64> = Retired::default();
        retired.retire(&epochs, object.swap(ptr::null_mut(), Ordering::AcqRel));
        to_reader.store(1, Ordering::Release);
        // Scans as the reader enters its later step: one that reads its slot clear or the later
        // step announced lets the object go.
        let reclaimed = loop {
            epochs.horizon();
            let out = retired.reclaim(&epochs);
            if !out.is_empty() {
                break out;
            }
            crate::thread::yield_now();
        };
        // SAFETY: from `Box::into_raw`, unlinked, and past every step that could read it.
        let mut destroyed = unsafe { Box::from_raw(reclaimed[0]) };
        *destroyed = 0;
        drop(destroyed);
        to_reader.store(2, Ordering::Relaxed);
        reader.join().unwrap();
        done.wait();
        for h in holders {
            h.join().unwrap();
        }
    }
}

/// Two retires each paused between the swap that publishes the node and the store of its link
/// (DESIGN.md 9.1.2): a reclaim that reaches the first carries it, a second reclaim walks from it
/// alone and takes no new head, and once both links are stored every object comes back once, none
/// before its link is read.
#[test]
fn a_retire_paused_before_its_link_is_carried_and_nothing_is_lost_or_freed_early() {
    use crate::race;
    let epochs = Arc::new(Epochs::new());
    let retired: Arc<Retired<u32>> = Arc::new(Retired::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    {
        let (epochs, retired, seen) = (epochs.clone(), retired.clone(), seen.clone());
        race::at("epoch.retire", move || {
            // The first retire's node is published and not linked: the reclaim carries it.
            seen.lock().extend(retired.reclaim(&epochs));
            {
                let (epochs, retired, seen) = (epochs.clone(), retired.clone(), seen.clone());
                race::at("epoch.retire", move || {
                    // The second's is behind the head while the first is still carried.
                    seen.lock().extend(retired.reclaim(&epochs));
                });
            }
            retired.retire(&epochs, 2);
            assert!(!race::pending("epoch.retire"));
        });
    }
    retired.retire(&epochs, 1);
    assert!(!race::pending("epoch.retire"));
    assert!(
        seen.lock().is_empty(),
        "nothing came back before its link was read"
    );
    let mut all = retired.reclaim(&epochs);
    all.extend(retired.reclaim(&epochs));
    all.sort_unstable();
    assert_eq!(all, vec![1, 2], "each object came back once");
    assert!(retired.reclaim(&epochs).is_empty());
}

/// A wait's flag on a step that ends between the scan's read of its slot and the flag: the scan
/// judges by the word the flag returns, the slot gone idle, and holds nothing back for it
/// (DESIGN.md 9.1.2).
#[test]
fn a_flag_on_a_step_that_ended_meanwhile_holds_nothing_back() {
    use crate::race;
    use std::sync::mpsc::channel;
    let epochs = Arc::new(Epochs::new());
    let (entered_tx, entered) = channel();
    let (leave_tx, leave) = channel::<()>();
    let (left_tx, left) = channel();
    let holder = {
        let epochs = epochs.clone();
        crate::thread::spawn(move || {
            let step = epochs.enter();
            entered_tx.send(()).unwrap();
            leave.recv().unwrap();
            drop(step);
            left_tx.send(()).unwrap();
        })
    };
    entered.recv().unwrap();
    race::at("epoch.flag", move || {
        leave_tx.send(()).unwrap();
        left.recv().unwrap();
    });
    let wait = crate::storage::atoll::notify::wait();
    let pending = epochs.wait_for(epochs.stamp(), wait.clone());
    assert!(
        !race::pending("epoch.flag"),
        "the step ended between the read and the flag"
    );
    assert!(
        !pending && wait.finished(),
        "no step under way holds the wait back, so it is finished now"
    );
    holder.join().unwrap();
}

/// The waits a list holds, in its order.
fn drained(mut list: WaitList) -> Vec<Wait> {
    std::iter::from_fn(|| list.pop().map(|wait| *wait)).collect()
}

/// A push lost to a wake that took the slot's whole list: the push then swaps its wait in, and the
/// wait stays registered (DESIGN.md 9.1.2).
#[test]
fn a_push_lost_to_a_take_of_its_list_keeps_its_wait() {
    use crate::race;
    let epochs = Arc::new(Epochs::new());
    let earlier = crate::storage::atoll::notify::wait();
    epochs.push(Box::new(Wait {
        epoch: Plain::new(u64::MAX - 1),
        notify: Box::new(earlier.clone()),
        next: ptr::null_mut(),
        fresh: false,
    }));
    {
        let epochs = epochs.clone();
        race::at("epoch.push", move || {
            assert_eq!(
                drained(epochs.take_waits()).len(),
                1,
                "the take emptied the list"
            );
        });
    }
    let later = crate::storage::atoll::notify::wait();
    epochs.push(Box::new(Wait {
        epoch: Plain::new(u64::MAX - 1),
        notify: Box::new(later.clone()),
        next: ptr::null_mut(),
        fresh: false,
    }));
    assert!(!race::pending("epoch.push"));
    let left = drained(epochs.take_waits());
    assert_eq!(left.len(), 1, "the later wait is on the list");
    assert!(!left[0].notify.notified());
    left[0].notify.notify();
    assert!(later.finished(), "and it is the later one");
}

/// Behind one paused old step, a thread registering one pending completion again after each event
/// (as a claim does, its ticket replaced at every event) leaves its slot one node, not one per
/// registration; finished completions registered between leave none (the auditor's 1/256/1024
/// probe, DESIGN.md 9.1.3, other waits). The old step's end finishes what stands.
#[test]
fn registrations_behind_an_old_step_leave_a_slot_one_node_per_unfinished_completion() {
    use std::sync::mpsc::channel;
    let epochs = Arc::new(Epochs::new());
    let (entered_tx, entered) = channel();
    let (leave_tx, leave) = channel::<()>();
    let holder = {
        let epochs = epochs.clone();
        crate::thread::spawn(move || {
            let step = epochs.enter();
            entered_tx.send(()).unwrap();
            leave.recv().unwrap();
            drop(step);
        })
    };
    entered.recv().unwrap();
    let pending = crate::storage::atoll::notify::wait();
    for i in 0..256 {
        // A finished completion registered and dropped: no node stays for it.
        let gone = crate::storage::atoll::notify::wait();
        assert!(epochs.wait_for(epochs.stamp(), gone.clone()));
        crate::storage::atoll::notify::finish(&gone, Ok(0));
        assert!(epochs.wait_for(epochs.stamp(), pending.clone()), "round {i}");
        assert_eq!(epochs.own_waits(), 1, "round {i}: the slot's list grew");
    }
    leave_tx.send(()).unwrap();
    holder.join().unwrap();
    assert!(pending.finished(), "the old step's end finished the wait");
    assert_eq!(epochs.own_waits(), 0);
}

/// Instance ids taken to the end of their count: each taken once and never 0, the id of the
/// per-thread table's empty cache, and the count stays at its end with every take after it
/// refused, from one thread and from several at once (audit epoch-entry-v1, P2-E1; mutant: a
/// wrapping increment, which hands out the end, then 0, then ids already taken).
#[cfg(not(shuttle))]
#[test]
fn instance_ids_taken_to_their_end_are_never_0_or_taken_twice() {
    use std::sync::atomic::AtomicU64;
    let next = AtomicU64::new(u64::MAX - 2);
    assert_eq!(take_id(&next), Some(u64::MAX - 2));
    assert_eq!(take_id(&next), Some(u64::MAX - 1));
    for _ in 0..3 {
        assert_eq!(take_id(&next), None, "a take past the end");
        assert_eq!(
            next.load(Ordering::Relaxed),
            u64::MAX,
            "the count left its end"
        );
    }
    let start = u64::MAX - 64;
    let next = AtomicU64::new(start);
    let taken: Vec<u64> = std::thread::scope(|s| {
        let takers: Vec<_> = (0..8)
            .map(|_| s.spawn(|| (0..16).filter_map(|_| take_id(&next)).collect::<Vec<u64>>()))
            .collect();
        takers.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    let mut ids = taken.clone();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), taken.len(), "an id taken twice");
    assert_eq!(
        ids,
        (start..u64::MAX).collect::<Vec<u64>>(),
        "the ids taken from several threads"
    );
    assert_eq!(
        next.load(Ordering::Relaxed),
        u64::MAX,
        "the count left its end"
    );
}
