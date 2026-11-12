//! The record's hand-offs under loom, over the real `Record` and `Bitmap` (D1a §6.10, D2a §6.10): a
//! follow-on published by its bit to the IO thread, a record let go to its next owner, and the IO
//! threads' idle beside the producers and the stop that wake them, on one word and on a tree of
//! them. Loom's cell checks that each access to the payload happens after the one before it; each
//! actor of the idle models returns its decision, judged once all have joined, so a lost wake or a
//! lost bit fails an assertion and no thread is left parked.

use super::{Bitmap, Door, End, Record, Taken};
use loom::sync::Arc;
use loom::thread;

/// Whether a take found a record: on one word, nothing is ever put back, so no wake rides with it.
fn took(taken: Taken) -> bool {
    match taken {
        Taken::Record { wake: None, .. } => true,
        Taken::Empty | Taken::Stopped => false,
        other => unreachable!("one word puts nothing back: {other:?}"),
    }
}

fn check(f: impl Fn() + Sync + Send + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.check(f);
}

/// The producer writes the follow-on and publishes it; the IO thread takes the bit, claims the
/// record and reads what was written (mutant: the bit's `fetch_or` and the word's `fetch_and`
/// both `Relaxed`).
#[test]
fn a_follow_on_published_by_its_bit_is_read_whole_by_the_io_thread() {
    check(|| {
        let record = Arc::new(Record::<u64>::default());
        let bits = Arc::new(Bitmap::new(1, 1));
        assert!(record.take(), "a fresh record at rest");
        let producer = {
            let (record, bits) = (record.clone(), bits.clone());
            thread::spawn(move || {
                // SAFETY: held, just taken.
                unsafe { record.with(|p| *p = Some(7)) };
                record.publish(&bits, 0);
            })
        };
        let mut from = 0;
        let read = loop {
            if took(bits.take(&mut from)) {
                assert!(record.claim(), "a bit set for a record not READY");
                // SAFETY: claimed.
                break unsafe { record.with(Option::take) };
            }
            thread::yield_now();
        };
        producer.join().unwrap();
        assert_eq!(read, Some(7), "the follow-on read");
    });
}

/// The old chain's last holder finishes the record while its next owner takes it and writes a
/// chain of its own: the old holder gets its own payload, and the new one's is untouched
/// (mutants: REST `Relaxed`; a read of the record after REST).
#[test]
fn a_record_let_go_is_reused_only_after_its_last_holder_took_its_payload() {
    check(|| {
        let record = Arc::new(Record::<u64>::default());
        assert!(record.take(), "a fresh record at rest");
        // SAFETY: held, just taken.
        unsafe { record.with(|p| *p = Some(1)) };
        let next = {
            let record = record.clone();
            thread::spawn(move || {
                while !record.take() {
                    thread::yield_now();
                }
                // SAFETY: held, just taken.
                unsafe { record.with(|p| *p = Some(2)) };
            })
        };
        // SAFETY: held since the take above.
        let old = unsafe { record.finish() };
        next.join().unwrap();
        // SAFETY: the next owner holds it, and has ended: this thread reads after its join.
        let new = unsafe { record.with(|p| *p) };
        assert_eq!(
            (old, new),
            (Some(1), Some(2)),
            "(the old chain's payload, the new one's)"
        );
    });
}

/// The IO thread's loop beside two producers, each actor's decision returned and judged once all
/// have joined, so a lost wake fails an assertion and never leaves a thread parked (#4721, #4724):
/// the thread takes the word, goes round again on a bit, and idles only by its exchange; a bit it
/// has not seen when it idles was set after the exchange, so some producer read the idle bit and
/// took it, owing the unpark. A wake takes the idle bit (D2a §2), so one unpark covers every bit
/// set while the thread idles: the second producer finds the bit taken and owes none (mutants: the
/// idle exchange whatever the word holds; a producer that ignores the idle bit).
#[test]
fn an_io_thread_that_idles_with_a_bit_unseen_is_owed_its_producers_unpark() {
    check(|| {
        let bits = Arc::new(Bitmap::new(2, 1));
        let io = {
            let bits = bits.clone();
            thread::spawn(move || {
                let (mut seen, mut from) = (0u64, 0);
                loop {
                    match bits.take(&mut from) {
                        Taken::Record { record, wake: None } => {
                            seen |= 1 << record;
                            continue;
                        }
                        Taken::Empty => {}
                        other => unreachable!("one word, no stop: {other:?}"),
                    }
                    if bits.idle(0) {
                        return seen;
                    }
                }
            })
        };
        let producer = {
            let bits = bits.clone();
            thread::spawn(move || bits.set(1).is_some())
        };
        let unparks = [bits.set(0).is_some(), producer.join().unwrap()];
        let seen = io.join().unwrap();
        let unseen = 0b11 & !seen;
        assert!(
            unseen == 0 || unparks.contains(&true),
            "the thread idled with bits {unseen:#b} unseen and no unpark owed it"
        );
        assert!(
            !(unparks[0] && unparks[1]),
            "two unparks owed for one idle thread"
        );
    });
}

/// The IO thread's loop beside the stop, set in the same word: a thread that idles having taken no
/// stop is owed the stop's unpark (mutant: a stop that does not report the idle mark it read).
#[test]
fn an_io_thread_that_idles_before_the_stop_is_owed_its_unpark() {
    check(|| {
        let bits = Arc::new(Bitmap::new(1, 1));
        let io = {
            let bits = bits.clone();
            thread::spawn(move || {
                let mut from = 0;
                loop {
                    match bits.take(&mut from) {
                        Taken::Record { .. } => continue,
                        Taken::Stopped => return false,
                        Taken::Empty => {}
                        other => unreachable!("one word: {other:?}"),
                    }
                    if bits.idle(0) {
                        return true;
                    }
                }
            })
        };
        let unparked = bits.stop().count() == 1;
        let idled = io.join().unwrap();
        assert!(
            !idled || unparked,
            "the thread idled before the stop and no unpark was owed it"
        );
    });
}

/// The log's door under loom, over the real `Door` and `Bitmap` (D1b §7.12), the requests' count
/// beside them: two producers each write a piece of log state Relaxed, count their own request,
/// ring, and only then let their own count go, as a retirement observer does; the IO thread takes
/// the bit, starts a unit, reads both pieces and ends it, setting the bit again when the door says
/// so. Judged once all have joined: the last unit read both pieces, so every ring was followed by a
/// unit started after it that saw its write; the door ends `00`; the count ends at zero (mutants:
/// a load-only coalescing return; the door's RMWs `Relaxed`; the end's swap to `RUNG` a store of
/// `00`).
#[test]
fn every_ring_is_followed_by_a_unit_that_reads_what_its_producer_wrote() {
    use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    check(|| {
        let door = Arc::new(Door::new());
        let bits = Arc::new(Bitmap::new(1, 1));
        let count = Arc::new(AtomicUsize::new(0));
        let state = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
        let finished = Arc::new(AtomicUsize::new(0));
        let producers: Vec<_> = (0..2)
            .map(|i| {
                let (door, bits, count, state, finished) = (
                    door.clone(),
                    bits.clone(),
                    count.clone(),
                    state.clone(),
                    finished.clone(),
                );
                thread::spawn(move || {
                    state[i].store(1, Ordering::Relaxed);
                    count.fetch_add(1, Ordering::AcqRel);
                    if door.ring() {
                        count.fetch_add(1, Ordering::AcqRel);
                        let _ = bits.set(0);
                    }
                    count.fetch_sub(1, Ordering::AcqRel);
                    finished.fetch_add(1, Ordering::Release);
                })
            })
            .collect();
        let mut last = None;
        let mut serve = || {
            assert!(door.start(), "the log's bit served with its door not rung");
            last = Some([0, 1].map(|i| state[i].load(Ordering::Relaxed)));
            match door.end(false) {
                End::Quiet => {
                    count.fetch_sub(1, Ordering::AcqRel);
                }
                End::Again => {
                    let _ = bits.set(0);
                }
            }
        };
        let mut from = 0;
        loop {
            if took(bits.take(&mut from)) {
                serve();
                continue;
            }
            // Every ring set its bit before its producer finished: past both, an empty take
            // leaves nothing to serve, and a taken bit is served.
            if finished.load(Ordering::Acquire) == 2 {
                if !took(bits.take(&mut from)) {
                    break;
                }
                serve();
                continue;
            }
            thread::yield_now();
        }
        for p in producers {
            p.join().unwrap();
        }
        assert_eq!(
            last,
            Some([1, 1]),
            "what the last unit read of each producer's write"
        );
        assert_eq!(door.read(), 0, "the door at the end");
        assert_eq!(count.load(Ordering::Acquire), 0, "the count at the end");
    });
}

/// What an IO thread of the tree's models came to once it idled: the records it served, and the
/// wakes its takes owed for the bits they put back.
fn serve_until_idle(bits: &Bitmap, k: usize) -> (u64, usize) {
    let (mut seen, mut wakes, mut from) = (0u64, 0, 0);
    loop {
        match bits.take(&mut from) {
            Taken::Record { record, wake } => {
                assert_eq!(seen & (1 << record), 0, "record {record} served twice");
                seen |= 1 << record;
                wakes += usize::from(wake.is_some());
                continue;
            }
            Taken::Retry { wake } => {
                wakes += usize::from(wake.is_some());
                continue;
            }
            Taken::Stopped => unreachable!("no stop in this model"),
            Taken::Empty => {}
        }
        if bits.idle(k) {
            return (seen, wakes);
        }
    }
}

/// The records still set in the tree once every actor has joined: taken by this thread alone.
fn left(bits: &Bitmap) -> u64 {
    let (mut left, mut from) = (0u64, 0);
    loop {
        match bits.take(&mut from) {
            Taken::Record { record, .. } => left |= 1 << record,
            Taken::Retry { .. } => {}
            Taken::Empty => return left,
            Taken::Stopped => unreachable!("no stop in this model"),
        }
    }
}

/// The tree's hand-offs (D2a §6.10), depth 2 by a fan-out of 2: two producers set records `a` and `b`
/// while an IO thread takes and idles. Judged once all have joined: no bit is lost (each was served
/// or is still in the tree), and a bit still in the tree was set after the thread idled, so some
/// producer, or a take's put-back, read its idle bit and took it, owing the unpark. Each IO thread
/// of these models idles once, at its end, so every wake owed one is owed for that one idle
/// interval, the last, and the thread it unparks takes every bit left (mutants: the idle exchange
/// ignoring the child bits; a put-back that does not climb; a producer that ignores the idle bit).
fn tree_with_one_taker(a: usize, b: usize) {
    check(move || {
        let bits = Arc::new(Bitmap::with_fan(4, 1, 2));
        assert_eq!(bits.depth(), 2, "a tree of leaves under the root");
        let io = {
            let bits = bits.clone();
            thread::spawn(move || serve_until_idle(&bits, 0))
        };
        let producer = {
            let bits = bits.clone();
            thread::spawn(move || bits.set(b))
        };
        let wakes = [bits.set(a), producer.join().unwrap()];
        let (seen, put_back) = io.join().unwrap();
        let left = left(&bits);
        let set = (1u64 << a) | (1 << b);
        assert_eq!(
            seen | left,
            set,
            "a bit lost: served {seen:#b}, left {left:#b}"
        );
        assert_eq!(seen & left, 0, "a bit served twice");
        assert!(
            left == 0 || wakes.iter().any(Option::is_some) || put_back > 0,
            "bits {left:#b} left with the thread idle and no wake owed it"
        );
    });
}

#[test]
fn a_tree_hands_off_two_bits_under_one_leaf_word_with_no_wake_or_bit_lost() {
    tree_with_one_taker(0, 1);
}

#[test]
fn a_tree_hands_off_two_bits_under_two_leaf_words_with_no_wake_or_bit_lost() {
    tree_with_one_taker(0, 2);
}

/// Two IO threads over the same depth-2 tree, two producers on different leaf words: no bit lost
/// or served twice, and a bit left has a wake owed for it, which named an idle thread (mutants: as
/// above, and the wake that takes no idle bit).
#[test]
fn a_tree_with_two_takers_loses_no_bit_and_no_wake() {
    check(|| {
        let bits = Arc::new(Bitmap::with_fan(4, 2, 2));
        let takers: Vec<_> = (0..2)
            .map(|k| {
                let bits = bits.clone();
                thread::spawn(move || serve_until_idle(&bits, k))
            })
            .collect();
        let producer = {
            let bits = bits.clone();
            thread::spawn(move || bits.set(3))
        };
        let wakes = [bits.set(0), producer.join().unwrap()];
        let ended: Vec<(u64, usize)> = takers.into_iter().map(|t| t.join().unwrap()).collect();
        let left = left(&bits);
        let (seen, put_back) = ([ended[0].0, ended[1].0], ended[0].1 + ended[1].1);
        assert_eq!(seen[0] & seen[1], 0, "a bit served by both threads");
        assert_eq!(
            seen[0] | seen[1] | left,
            0b1001,
            "a bit lost: served {seen:?}, left {left:#b}"
        );
        assert!(
            left == 0 || wakes.iter().any(Option::is_some) || put_back > 0,
            "bits {left:#b} left with both threads idle and no wake owed"
        );
    });
}

/// A stop set while two IO threads idle takes both idle bits, so each is owed its unpark (mutant:
/// a stop that unparks without taking the idle bits, leaving a woken thread parked again).
#[test]
fn a_stop_takes_every_idle_bit_it_unparks() {
    check(|| {
        let bits = Arc::new(Bitmap::with_fan(4, 2, 2));
        let takers: Vec<_> = (0..2)
            .map(|k| {
                let bits = bits.clone();
                thread::spawn(move || {
                    let mut from = 0;
                    loop {
                        match bits.take(&mut from) {
                            Taken::Stopped => return false,
                            Taken::Empty => {}
                            other => unreachable!("nothing set in this model: {other:?}"),
                        }
                        if bits.idle(k) {
                            return true;
                        }
                    }
                })
            })
            .collect();
        let unparked: Vec<usize> = bits.stop().collect();
        let idled: Vec<bool> = takers.into_iter().map(|t| t.join().unwrap()).collect();
        for (k, idled) in idled.into_iter().enumerate() {
            assert!(
                !idled || unparked.contains(&k),
                "thread {k} idled before the stop and was not unparked ({unparked:?})"
            );
            assert!(
                !bits.still_idle(k),
                "thread {k}'s idle bit left set past the stop"
            );
        }
    });
}

/// The tree publishes a record's payload through every edge a producer climbs or stops at (#4832):
/// two producers each write a record's payload and set its bit under one leaf word, so one set
/// coalesces into the other's leaf and climbs no further; the taker claims each record it takes and
/// reads its payload, whose access loom's cell checks happens after the producer's write (mutant:
/// the leaf's fetch_or Relaxed).
#[test]
fn a_payload_published_through_a_coalescing_leaf_is_read_whole() {
    check(|| {
        let bits = Arc::new(Bitmap::with_fan(4, 1, 2));
        let records: Arc<[Record<u64>; 2]> = Arc::new([Record::default(), Record::default()]);
        for record in records.iter() {
            assert!(record.take(), "a fresh record at rest");
        }
        let producers: Vec<_> = (0..2)
            .map(|i| {
                let (bits, records) = (bits.clone(), records.clone());
                thread::spawn(move || {
                    // SAFETY: held, taken above, and published only below.
                    unsafe { records[i].with(|p| *p = Some(10 + i as u64)) };
                    let _ = records[i].publish(&bits, i);
                })
            })
            .collect();
        let mut read = [None, None];
        let mut from = 0;
        while read.iter().any(Option::is_none) {
            match bits.take(&mut from) {
                Taken::Record { record, .. } => {
                    assert!(records[record].claim(), "a bit set for a record not READY");
                    // SAFETY: claimed.
                    read[record] = unsafe { records[record].with(Option::take) };
                }
                _ => thread::yield_now(),
            }
        }
        for p in producers {
            p.join().unwrap();
        }
        assert_eq!(read, [Some(10), Some(11)], "the payloads read");
    });
}
