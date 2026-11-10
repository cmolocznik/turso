use super::*;
use crate::race;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{mpsc, Arc as StdArc, Barrier};

const CYCLES: usize = if cfg!(miri) { 64 } else { 10_000 };

#[test]
fn a_paused_page_step_keeps_no_removed_completions() {
    let epochs = Arc::new(Epochs::new());
    let domain = CompletionDomain::new(epochs.clone());
    let cell = CompletionCell::default();
    let dropped = StdArc::new(AtomicUsize::new(0));
    let step = epochs.enter();
    for _ in 0..CYCLES {
        cell.put(counted(&dropped));
        let c = cell.take(&domain).unwrap();
        c.complete(0);
        drop(c);
    }
    let before_leaving = dropped.load(Ordering::Relaxed);
    drop(step);
    drop(cell);
    drop(domain);
    println!("page step paused: cycles={CYCLES}, freed_before_step_end={before_leaving}");
    assert_eq!(before_leaving, CYCLES);
}

#[test]
fn a_paused_cloner_keeps_only_its_own_completion() {
    let epochs = Arc::new(Epochs::new());
    let domain = Arc::new(CompletionDomain::new(epochs));
    let cell = Arc::new(CompletionCell::default());
    let original = StdArc::new(AtomicUsize::new(0));
    let later = StdArc::new(AtomicUsize::new(0));
    cell.put(counted(&original));
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel::<bool>();
    let reader = {
        let (cell, domain) = (cell.clone(), domain.clone());
        std::thread::spawn(move || {
            race::at("cell.validated", move || {
                paused_tx.send(()).unwrap();
                // On a defective build, stop before native code could clone a freed pointer.
                assert!(
                    resume_rx.recv().unwrap(),
                    "validated completion already freed"
                );
            });
            crate::storage::atoll::testing::catch_test_panic(|| cell.get(&domain))
        })
    };
    paused_rx.recv().unwrap();
    let c = cell.take(&domain).unwrap();
    c.complete(0);
    drop(c);
    for _ in 0..CYCLES {
        cell.put(counted(&later));
        let c = cell.take(&domain).unwrap();
        c.complete(0);
        drop(c);
    }
    let original_freed = original.load(Ordering::Relaxed);
    let later_freed = later.load(Ordering::Relaxed);
    resume_tx.send(original_freed == 0).unwrap();
    let got = reader.join().unwrap();
    let read_succeeded = matches!(&got, Ok(Ok(Some(c))) if c.finished());
    drop(got);
    drop(cell);
    drop(domain);
    println!(
        "cloner paused: later={CYCLES}, original_freed={original_freed}, later_freed={later_freed}"
    );
    assert_eq!(original_freed, 0);
    assert_eq!(later_freed, CYCLES);
    assert!(read_succeeded);
    assert_eq!(original.load(Ordering::Relaxed), 1);
}

#[test]
fn repeated_removals_of_the_same_pointer_transfer_only_one_reference() {
    let epochs = Arc::new(Epochs::new());
    let domain = Arc::new(CompletionDomain::new(epochs));
    let cell = Arc::new(CompletionCell::default());
    let dropped = StdArc::new(AtomicUsize::new(0));
    let original = counted(&dropped);
    cell.put(original.clone());
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let reader = {
        let (cell, domain) = (cell.clone(), domain.clone());
        std::thread::spawn(move || {
            race::at("cell.validated", move || {
                paused_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            });
            cell.get(&domain).unwrap().unwrap()
        })
    };
    paused_rx.recv().unwrap();
    let c = cell.take(&domain).unwrap();
    c.complete(0);
    drop(c);
    #[cfg(not(miri))]
    let before = crate::storage::atoll::testing::allocations();
    for _ in 0..CYCLES {
        cell.put(original.clone());
        drop(cell.take(&domain).unwrap());
    }
    #[cfg(not(miri))]
    let allocations = crate::storage::atoll::testing::allocations() - before;
    drop(original);
    resume_tx.send(()).unwrap();
    drop(reader.join().unwrap());
    drop(cell);
    drop(domain);
    assert_eq!(
        dropped.load(Ordering::Relaxed),
        1,
        "a transferred reference remains"
    );
    #[cfg(not(miri))]
    {
        println!("same pointer: cycles={CYCLES}, allocations={allocations}");
        assert_eq!(allocations, 0);
    }
}

#[test]
fn the_last_transferred_reference_can_read_another_cell_when_dropped() {
    let epochs = Arc::new(Epochs::new());
    let domain = Arc::new(CompletionDomain::new(epochs));
    let cell = Arc::new(CompletionCell::default());
    let other = Arc::new(CompletionCell::default());
    other.put(Completion::new_wait());
    let entered = StdArc::new(AtomicUsize::new(0));
    let on_drop = {
        let (domain, other, entered) = (domain.clone(), other.clone(), entered.clone());
        OnDrop(Some(Box::new(move || {
            let c = other.get(&domain).unwrap().unwrap();
            assert!(!c.finished());
            entered.fetch_add(1, Ordering::Relaxed);
        })))
    };
    cell.put(Completion::new_write(move |_| {
        let _ = &on_drop;
    }));
    {
        let (cell, domain) = (cell.clone(), domain.clone());
        race::at("cell.announced", move || {
            let c = cell.take(&domain).unwrap();
            c.complete(0);
            drop(c);
        });
    }
    let got = cell.get(&domain);
    assert!(
        matches!(got, Err(Changed)),
        "the cell was emptied during its read"
    );
    assert_eq!(entered.load(Ordering::Relaxed), 1);
    drop(other.take(&domain));
}

#[test]
fn readers_across_registry_chunks_all_get_a_reference() {
    let readers = if cfg!(miri) { 3 } else { 65 };
    let epochs = Arc::new(Epochs::new());
    let domain = Arc::new(CompletionDomain::new(epochs));
    let cell = Arc::new(CompletionCell::default());
    let dropped = StdArc::new(AtomicUsize::new(0));
    cell.put(counted(&dropped));
    let proceed = StdArc::new(AtomicBool::new(false));
    let barrier = StdArc::new(Barrier::new(readers + 1));
    let (paused_tx, paused_rx) = mpsc::channel();
    let threads: Vec<_> = (0..readers)
        .map(|_| {
            let (cell, domain, proceed, barrier, tx) = (
                cell.clone(),
                domain.clone(),
                proceed.clone(),
                barrier.clone(),
                paused_tx.clone(),
            );
            std::thread::spawn(move || {
                race::at("cell.validated", move || {
                    tx.send(()).unwrap();
                    barrier.wait();
                    assert!(
                        proceed.load(Ordering::Acquire),
                        "completion freed before all readers"
                    );
                });
                crate::storage::atoll::testing::catch_test_panic(|| cell.get(&domain))
            })
        })
        .collect();
    for _ in 0..readers {
        paused_rx.recv().unwrap();
    }
    let c = cell.take(&domain).unwrap();
    c.complete(0);
    drop(c);
    let freed_before = dropped.load(Ordering::Relaxed);
    proceed.store(freed_before == 0, Ordering::Release);
    barrier.wait();
    let mut successful = 0;
    for thread in threads {
        let got = thread.join().unwrap();
        successful += usize::from(matches!(&got, Ok(Ok(Some(c))) if c.finished()));
        drop(got);
    }
    drop(cell);
    drop(domain);
    println!("registry readers={readers}, successful={successful}, freed_before={freed_before}");
    assert_eq!(freed_before, 0);
    assert_eq!(successful, readers);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn thread_exit_reuses_empty_slots_in_two_independent_domains() {
    let a = Arc::new(Epochs::new());
    let b = Arc::new(Epochs::new());
    let da = Arc::new(CompletionDomain::new(a.clone()));
    let db = Arc::new(CompletionDomain::new(b.clone()));
    let ca = Arc::new(CompletionCell::default());
    let cb = Arc::new(CompletionCell::default());
    ca.put(Completion::new_wait());
    cb.put(Completion::new_wait());
    let rounds = if cfg!(miri) { 4 } else { 32 };
    for _ in 0..rounds {
        let (da, db, ca, cb) = (da.clone(), db.clone(), ca.clone(), cb.clone());
        std::thread::spawn(move || {
            assert!(!ca.get(&da).unwrap().unwrap().finished());
            assert!(!cb.get(&db).unwrap().unwrap().finished());
        })
        .join()
        .unwrap();
    }
    assert_eq!(a.participants(), 1);
    assert_eq!(b.participants(), 1);
    drop(ca.take(&da));
    drop(da);
    drop(a);
    drop(ca);
    assert!(!cb.get(&db).unwrap().unwrap().finished());
    drop(cb.take(&db));
}

struct DropCount(StdArc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn counted(dropped: &StdArc<AtomicUsize>) -> Completion {
    let token = DropCount(dropped.clone());
    Completion::new_write(move |_| {
        let _ = &token;
    })
}

struct OnDrop(Option<Box<dyn FnOnce() + Send + Sync>>);
impl Drop for OnDrop {
    fn drop(&mut self) {
        if let Some(action) = self.0.take() {
            action();
        }
    }
}

#[cfg(not(any(miri, loom, atoll_loom, shuttle)))]
#[test]
fn a_completion_constructs_its_wait_state_in_one_allocation() {
    drop(Completion::new_wait());
    let before = crate::storage::atoll::testing::allocations();
    let completion = Completion::new_wait();
    let allocations = crate::storage::atoll::testing::allocations() - before;
    drop(completion);
    assert_eq!(
        allocations, 1,
        "the wait state belongs to the completion's allocation"
    );
}
