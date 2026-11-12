//! The record's hand-offs under loom, over the real `Record` and `Bitmap` (D1a §6.10): a follow-on
//! published by its bit to the IO thread, a record let go to its next owner, and the IO thread's
//! idle beside the producers and the stop that wake it. Loom's cell checks that each access to the
//! payload happens after the one before it; each actor of the idle models returns its decision,
//! judged once all have joined, so a lost wake fails an assertion and no thread is left parked.

use super::{Bitmap, Door, End, Record};
use loom::sync::Arc;
use loom::thread;

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
        let bits = Arc::new(Bitmap::new(1));
        assert!(record.take(), "a fresh record at rest");
        let producer = {
            let (record, bits) = (record.clone(), bits.clone());
            thread::spawn(move || {
                // SAFETY: held, just taken.
                unsafe { record.with(|p| *p = Some(7)) };
                record.publish(&bits, 0);
            })
        };
        let read = loop {
            if bits.take().0 != 0 {
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
/// has not seen when it idles was set after the exchange, so its producer read the mark and owes
/// the unpark (mutants: the mark set whatever the word holds; a producer that ignores the mark).
#[test]
fn an_io_thread_that_idles_with_a_bit_unseen_is_owed_its_producers_unpark() {
    check(|| {
        let bits = Arc::new(Bitmap::new(2));
        let io = {
            let bits = bits.clone();
            thread::spawn(move || {
                let mut seen = 0;
                loop {
                    let (word, _) = bits.take();
                    seen |= word;
                    if word != 0 {
                        continue;
                    }
                    if bits.idle() {
                        return seen;
                    }
                }
            })
        };
        let producer = {
            let bits = bits.clone();
            thread::spawn(move || bits.set(1))
        };
        let unparks = [bits.set(0), producer.join().unwrap()];
        let seen = io.join().unwrap();
        for (bit, unparked) in unparks.into_iter().enumerate() {
            assert!(
                seen & (1 << bit) != 0 || unparked,
                "the thread idled with bit {bit} unseen and no unpark owed it (seen {seen:#b})"
            );
        }
    });
}

/// The IO thread's loop beside the stop, set in the same word: a thread that idles having taken no
/// stop is owed the stop's unpark (mutant: a stop that does not report the idle mark it read).
#[test]
fn an_io_thread_that_idles_before_the_stop_is_owed_its_unpark() {
    check(|| {
        let bits = Arc::new(Bitmap::new(1));
        let io = {
            let bits = bits.clone();
            thread::spawn(move || loop {
                let (word, stopped) = bits.take();
                if word != 0 {
                    continue;
                }
                if stopped {
                    return false;
                }
                if bits.idle() {
                    return true;
                }
            })
        };
        let unparked = bits.stop();
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
        let bits = Arc::new(Bitmap::new(1));
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
                        bits.set(0);
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
                    bits.set(0);
                }
            }
        };
        loop {
            if bits.take().0 != 0 {
                serve();
                continue;
            }
            // Every ring set its bit before its producer finished: past both, an empty take
            // leaves nothing to serve, and a taken bit is served.
            if finished.load(Ordering::Acquire) == 2 {
                if bits.take().0 == 0 {
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
