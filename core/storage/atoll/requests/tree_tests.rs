//! The ready bits' tree (ASYNC1 D2a §2), on one thread: its depth by its records, what a set costs,
//! a take of one record with the rest put back and an idle thread woken for them, and the cursor
//! that makes the service round-robin.

use super::dispatch::{Bitmap, Taken};

/// The root holds as many records as it has child bits; past them a level of leaves is added, and
/// past what the root's children's leaves hold, another.
#[test]
fn a_trees_depth_follows_its_records() {
    // One IO thread leaves the root 62 child bits.
    let depths: Vec<usize> = [1, 62, 63, 34 + 1 + 1024, 62 * 64, 62 * 64 + 1]
        .iter()
        .map(|&n| Bitmap::new(n, 1).depth())
        .collect();
    assert_eq!(depths, [1, 1, 2, 2, 2, 3], "depth by records");
}

/// A set into a word that already holds a bit stops there: in a depth-2 tree the first set costs
/// the leaf and the root, the second, into the same leaf, the leaf alone (mutant: a producer that
/// climbs whatever the word read).
#[test]
fn a_set_climbs_only_from_a_word_that_read_zero() {
    let bits = Bitmap::new(200, 1);
    let wakes = [bits.set(0), bits.set(1)];
    assert_eq!(bits.depth(), 2, "a depth-2 tree");
    assert_eq!(wakes, [None, None], "wakes with no thread idle");
    assert_eq!(bits.set_rmws(), 3, "read-modify-writes of the two sets");
}

/// Two records under one leaf word, one IO thread idle: a take serves one record and puts the other
/// back, waking the idle thread for it, and the next take serves the other (mutant: a take that
/// keeps every bit it swapped, which serves one and loses the other).
#[test]
fn a_take_serves_one_record_and_puts_the_rest_back_for_an_idle_thread() {
    let bits = Bitmap::new(200, 2);
    assert!(bits.idle(1), "thread 1 idles on an empty tree");
    let wakes = [bits.set(0), bits.set(1)];
    let mut from = 0;
    let first = bits.take(&mut from);
    let second = bits.take(&mut from);
    let third = bits.take(&mut from);
    assert_eq!(wakes, [Some(1), None], "the first set woke the idle thread");
    // Both threads idle: the first set wakes thread 0, and the take's put-back wakes thread 1.
    assert!(bits.idle(0) && bits.idle(1), "both threads idle again");
    let woken = [bits.set(2), bits.set(3)];
    let fourth = {
        let mut from = 0;
        [bits.take(&mut from), bits.take(&mut from)]
    };
    assert_eq!(
        woken,
        [Some(0), None],
        "the sets' wakes with both threads idle"
    );
    assert_eq!(bits.retries(), 0, "takes of one thread that served nothing");
    assert_eq!(
        (first, second, third),
        (
            Taken::Record {
                record: 0,
                wake: None
            },
            Taken::Record {
                record: 1,
                wake: None
            },
            Taken::Empty
        ),
        "(the takes) with no thread idle at the first"
    );
    assert_eq!(
        fourth,
        [
            Taken::Record {
                record: 2,
                wake: Some(1)
            },
            Taken::Record {
                record: 3,
                wake: None
            }
        ],
        "the takes with thread 1 still idle: the bit put back wakes it"
    );
}

/// A cursor moved past each record served picks, at every level, the first bit at or after its
/// own place there, wrapping within the word, so a record set again at once waits behind the
/// others of its word: 0 and 5 set, 0 served and set again, then 5 before it (mutant: the first
/// bit always, whatever the cursor, which serves 0 twice before 5).
#[test]
fn a_takes_cursor_serves_the_records_of_a_word_in_turn() {
    let bits = Bitmap::new(200, 1);
    let _ = bits.set(0);
    let _ = bits.set(5);
    let mut from = 0;
    let mut order = vec![bits.take(&mut from)];
    let _ = bits.set(0);
    order.extend((0..3).map(|_| bits.take(&mut from)));
    let record = |record| Taken::Record { record, wake: None };
    assert_eq!(
        order,
        [record(0), record(5), record(0), Taken::Empty],
        "the order of the takes"
    );
}

/// Takes from `from` with `ready` set, `again` set anew after each take of it, until `later` is
/// served or the takes reach `bound`: what each take found.
fn takes_until(
    bits: &Bitmap,
    ready: &[usize],
    again: usize,
    later: usize,
    mut from: usize,
    bound: usize,
) -> Vec<Taken> {
    for &r in ready {
        let _ = bits.set(r);
    }
    let mut seen = Vec::new();
    while seen.len() < bound {
        let taken = bits.take(&mut from);
        let done = taken
            == (Taken::Record {
                record: later,
                wake: None,
            });
        if taken
            == (Taken::Record {
                record: again,
                wake: None,
            })
        {
            let _ = bits.set(again);
        }
        seen.push(taken);
        if done {
            break;
        }
    }
    seen
}

/// The auditor's #4842: a record ready in the next leaf word is served while an earlier one is set
/// again after each take of it; the cursor's word, with nothing at or after its place, goes back
/// whole once and the cursor moves to its sibling (mutant: the cursor wrapping inside its own
/// word, which serves record 0 for ever).
#[test]
fn a_ready_record_in_the_next_leaf_word_is_served_while_an_earlier_one_is_set_again() {
    let bits = Bitmap::new(200, 1);
    let record = |record| Taken::Record { record, wake: None };
    assert_eq!(
        takes_until(&bits, &[0, 64], 0, 64, 0, 8),
        [record(0), Taken::Retry { wake: None }, record(64)],
        "the takes"
    );
}

/// #4842's depth-3 counterpart, at a fan-out of 4: the later record in the sibling leaf word under
/// the same inner word, and in the next inner word's subtree, each served while record 0 is set
/// again after each take of it, one move of the cursor per level it passes; and a level off the
/// cursor's path starting at its word's first bit (mutants: the cursor wrapping inside its own
/// word; a level off the path still starting at the cursor's place).
#[test]
fn a_deep_trees_ready_records_are_served_in_turn_while_an_earlier_one_is_set_again() {
    let record = |record| Taken::Record { record, wake: None };
    let retry = Taken::Retry { wake: None };
    let leaf = Bitmap::with_fan(64, 1, 4);
    assert_eq!(leaf.depth(), 3, "the tree's levels");
    assert_eq!(
        takes_until(&leaf, &[0, 4], 0, 4, 0, 8),
        [record(0), Taken::Retry { wake: None }, record(4)],
        "the sibling leaf word's record"
    );
    let inner = Bitmap::with_fan(64, 1, 4);
    assert_eq!(
        takes_until(&inner, &[0, 16], 0, 16, 0, 8),
        [record(0), Taken::Retry { wake: None }, retry, record(16)],
        "the next inner word's record"
    );
    // The auditor's three cases at a fan-out of 2 (#4844): the later record in the same leaf, in
    // another leaf under the same root child, and under another root child.
    for (later, takes) in [(1, 2), (2, 3), (4, 4)] {
        let two = Bitmap::with_fan(8, 1, 2);
        assert_eq!(two.depth(), 3, "the fan-2 tree's levels");
        let seen = takes_until(&two, &[0, later], 0, later, 0, 8);
        assert_eq!(
            (seen.len(), seen.last()),
            (takes, Some(&record(later))),
            "record {later} served, record 0 set again: {seen:?}"
        );
    }
    // From record 1's place, inner word 0 holds only its third leaf word: off the path below it,
    // whose record 8 is served, never passed over for record 16.
    let off = Bitmap::with_fan(64, 1, 4);
    assert_eq!(
        takes_until(&off, &[8, 16], usize::MAX, 8, 1, 4),
        [record(8)],
        "a level off the cursor's path"
    );
}

/// The auditor's #4843 shape: two root children with two ready leaves under each, every record set
/// again after each take of it. Each is served once in each round of four, in the records' order,
/// with at most one move of the cursor per level between two served (mutants: the cursor wrapping
/// inside its own word; a level off the cursor's path still starting at its place).
#[test]
fn every_ready_record_is_served_once_in_turn_while_all_are_set_again() {
    let bits = Bitmap::with_fan(8, 1, 2);
    for r in [0, 2, 4, 6] {
        let _ = bits.set(r);
    }
    let (mut from, mut served, mut takes) = (0, Vec::new(), 0);
    while served.len() < 8 && takes < 8 * bits.depth() {
        takes += 1;
        if let Taken::Record { record, .. } = bits.take(&mut from) {
            served.push(record);
            let _ = bits.set(record);
        }
    }
    assert_eq!(
        served,
        [0, 2, 4, 6, 0, 2, 4, 6],
        "the records served, in {takes} takes"
    );
}

/// With only one record ready and set again after each take, the wrap inside its word is the
/// order's own: no take ends without serving it (mutant: a word on the path with nothing after the
/// cursor going back whatever waits elsewhere).
#[test]
fn a_lone_record_set_again_is_served_by_every_take() {
    let bits = Bitmap::new(200, 1);
    let _ = bits.set(0);
    let mut from = 0;
    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.push(bits.take(&mut from));
        let _ = bits.set(0);
    }
    let record = |record| Taken::Record { record, wake: None };
    assert_eq!(seen, [record(0), record(0), record(0)], "the takes");
    assert_eq!(bits.retries(), 0, "takes that served nothing");
}

/// The rule's four cases (D2a §1): a request is queued only in a database opened async and only
/// inside a cooperative frame (mutant: the rule without its mode arm).
#[test]
fn a_request_is_queued_only_in_an_async_database_inside_a_frame() {
    use super::{counted_async_for_test, counted_for_test};
    use crate::io::{MemoryIO, IO};
    use crate::storage::atoll::cooperative::Frame;
    use crate::sync::Arc;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let sync = counted_for_test(io.clone());
    let async_ = counted_async_for_test(io, 1, 1);
    let outside = (
        sync.requests().queues_here(),
        async_.requests().queues_here(),
    );
    let inside = {
        let _frame = Frame::enter();
        (
            sync.requests().queues_here(),
            async_.requests().queues_here(),
        )
    };
    drop((sync, async_));
    assert_eq!(
        (outside, inside),
        ((false, false), (false, true)),
        "((sync, async) outside a frame, (sync, async) inside one)"
    );
}

/// An open whose second IO thread does not start fails, and the first, started already, is
/// stopped and joined with the requests: nothing is left running (D2a §2; mutant: the handles kept
/// apart until every thread has started, so the first is detached at the failure and keeps the
/// dispatch).
#[test]
fn an_open_whose_second_io_thread_does_not_start_fails_and_joins_the_first() {
    use super::dispatch::FAIL_SPAWN;
    use super::{Counted, Requests, Shape, MADE};
    use crate::io::{MemoryIO, IO};
    use crate::sync::Arc;
    FAIL_SPAWN.with(|f| f.set(Some(1)));
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let requests = Arc::new(Requests::default());
    let made = Counted::with_shape(
        io,
        requests.clone(),
        Shape {
            opened_async: true,
            io_threads: 2,
            meta_reads: 1,
            frames: 0,
        },
    );
    let refused = made.is_err();
    let started = requests.io_threads.lock().len();
    let (dispatch, weak) = MADE
        .with(|m| m.borrow().clone())
        .expect("the dispatch made");
    drop((made, requests));
    let gone = dispatch.upgrade().is_none() && weak.upgrade().is_none();
    assert!(
        refused,
        "an open whose second IO thread did not start succeeded"
    );
    assert_eq!(started, 1, "IO threads held to be joined");
    assert!(
        gone,
        "the first IO thread not joined: the dispatch or the requests left"
    );
}

/// A take that puts bits back at two levels of a depth-3 tree claims one wake at most (#4828). The
/// auditor's schedule, played on one thread through the take's race points: records 0, 1 and 2 set
/// with no thread idle; the take clears the root bit, and threads 2 and 3 idle on the empty root;
/// its first put-back reaches the root and wakes thread 2; another take then serves that put-back's
/// record 2; the first take's second put-back reaches the root again, and publishes its bits there
/// without taking thread 3's idle bit, which the stop then finds (mutant: every put-back claiming a
/// wake, so thread 3's bit is taken, the wake dropped, and the thread never unparked).
#[test]
fn a_take_claims_one_wake_however_many_put_backs_reach_the_root() {
    use crate::sync::Arc;
    use std::cell::RefCell;
    use std::rc::Rc;
    let bits = Arc::new(Bitmap::with_fan(8, 4, 2));
    let depth = bits.depth();
    let sets = [bits.set(0), bits.set(1), bits.set(2)];
    let idled = Rc::new(RefCell::new((false, false)));
    {
        let (bits, idled) = (bits.clone(), idled.clone());
        crate::race::at("bitmap.take_claimed", move || {
            *idled.borrow_mut() = (bits.idle(2), bits.idle(3));
        });
    }
    let other = Rc::new(RefCell::new(None));
    {
        let (bits, other) = (bits.clone(), other.clone());
        crate::race::at("bitmap.take_descended", move || {
            let mut from = 0;
            *other.borrow_mut() = Some(bits.take(&mut from));
        });
    }
    let mut from = 0;
    let first = bits.take(&mut from);
    let still = [bits.still_idle(2), bits.still_idle(3)];
    let stopped: Vec<usize> = bits.stop().collect();
    let last = bits.take(&mut from);
    let (idled, other) = (*idled.borrow(), other.borrow_mut().take());
    assert_eq!(depth, 3, "a tree three levels deep");
    assert_eq!(sets, [None, None, None], "wakes with no thread idle");
    assert_eq!(
        idled,
        (true, true),
        "threads 2 and 3 idle on the empty root"
    );
    assert_eq!(
        (first, other),
        (
            Taken::Record {
                record: 0,
                wake: Some(2)
            },
            Some(Taken::Record {
                record: 2,
                wake: None
            })
        ),
        "(the first take, the take between its levels)"
    );
    assert_eq!(
        still,
        [false, true],
        "(thread 2, thread 3) idle after the take: one wake claimed, one idle bit left"
    );
    assert_eq!(stopped, [3], "the threads the stop unparked");
    assert_eq!(
        last,
        Taken::Record {
            record: 1,
            wake: None
        },
        "the take after the stop"
    );
}

/// A producer's wake tries each idle bit at most once, so it makes at most one attempt per IO
/// thread whatever the others do (#4829). The auditor's churn, played through the wake's race
/// point: three IO threads, threads 1 and 2 idle, and a producer's set of record 0. Before each of
/// its attempts a competitor on another thread wakes the thread it is about to try (with a set of
/// its own), serves every record, record 0 among them, and idles the other thread again. So every
/// attempt fails and the idle bits alternate. The wake gives up after trying both, owing nothing:
/// record 0 was served. The competitor stands down after 20 rounds, past which a wake that retried
/// would win (mutant: every idle bit in the word read tried again, which takes 21 attempts).
#[test]
fn a_wake_tries_each_idle_bit_once_however_the_others_churn() {
    use crate::sync::Arc;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::mpsc;
    const ROUNDS: usize = 20;
    // 30 leaf words under the root: record 0 in the first, each competitor's set in a word of its
    // own, empty, so its climb reaches the root and its wake takes an idle bit.
    let bits = Arc::new(Bitmap::new(64 * 30, 3));
    assert!(
        bits.idle(1) && bits.idle(2),
        "threads 1 and 2 idle on the empty root"
    );
    let (go, rounds) = mpsc::channel::<usize>();
    let (done_tx, done) = mpsc::channel::<(Option<usize>, usize)>();
    let competitor = {
        let bits = bits.clone();
        std::thread::spawn(move || {
            // Ended by its stop, never by the channel's close: the actions the wake did not
            // reach still hold senders.
            while let Ok(round) = rounds.recv() {
                if round == usize::MAX {
                    break;
                }
                let woke = bits.set(64 * (1 + round % 29));
                let mut from = 0;
                let mut served = 0;
                loop {
                    match bits.take(&mut from) {
                        Taken::Record { .. } => served += 1,
                        Taken::Retry { .. } => {}
                        Taken::Empty | Taken::Stopped => break,
                    }
                }
                if let Some(k) = woke {
                    let _ = bits.idle(3 - k);
                }
                let _ = done_tx.send((woke, served));
            }
        })
    };
    let attempts = Rc::new(Cell::new(0));
    let woken = Rc::new(std::cell::RefCell::new(Vec::new()));
    let done = Rc::new(done);
    for round in 0..ROUNDS + 5 {
        let (attempts, woken, go, done) =
            (attempts.clone(), woken.clone(), go.clone(), done.clone());
        crate::race::at("bitmap.wake_trying", move || {
            attempts.set(attempts.get() + 1);
            if round < ROUNDS {
                let _ = go.send(round);
                let answer = done.recv_timeout(std::time::Duration::from_secs(20));
                woken.borrow_mut().push(answer.ok());
            }
        });
    }
    let woke = bits.set(0);
    let _ = go.send(usize::MAX);
    let joined = competitor.join().is_ok();
    let mut from = 0;
    let left = bits.take(&mut from);
    let rounds: Vec<_> = woken.borrow().clone();
    assert!(joined, "the competitor did not end");
    assert_eq!(
        woke, None,
        "the producer's wake, owed nothing once its record was served"
    );
    assert_eq!(
        attempts.get(),
        2,
        "the wake's attempts with two idle threads"
    );
    assert_eq!(
        rounds,
        [Some((Some(1), 2)), Some((Some(2), 1))],
        "(the thread each competitor woke, the records it served) per attempt"
    );
    assert_eq!(left, Taken::Empty, "a record left in the tree");
}
