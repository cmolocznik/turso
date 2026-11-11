//! The preparation's word (DESIGN.md 22.9 item 2): whether a flush's padding waits for a
//! segment, and whether the log's preparer is preparing one. Two bits:
//!
//! - *needed*: a flush met padding waiting for a segment, set until the preparer publishes one;
//! - *preparing*: the preparer's, from taking the word until the segment's end is published.
//!
//! Only the preparer takes the word, so nothing else ever runs a step of the preparation. A flush
//! that meets padding sets *needed*; from nothing set it moves the word, and its mover triggers
//! the preparer. Set while the preparer prepares, it says a flush waits on that segment; the
//! preparer clears both bits as it publishes the segment and then flushes, and that flush looks at
//! the padding again, setting *needed* anew if the next segment is wanted too.

use self::sync::{AtomicU8, Ordering};

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU8, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{AtomicU8, Ordering};
}

const DONE: u8 = 0;
const NEEDED: u8 = 1;
const PREPARING: u8 = 2;

/// The word.
pub(super) struct Word(AtomicU8);

impl Default for Word {
    fn default() -> Self {
        Self(AtomicU8::new(DONE))
    }
}

impl Word {
    /// Sets *needed*, where a flush meets padding waiting for a segment: whether it moved the word
    /// from nothing set, which makes its mover trigger the preparer.
    pub(super) fn need(&self) -> bool {
        self.0.fetch_or(NEEDED, Ordering::SeqCst) == DONE
    }

    /// The preparer takes the word, needed or, with nothing set, when `short` says less than a
    /// segment is prepared ahead of what is admitted: whether it took it, which makes the
    /// preparation its own until [`done`](Self::done).
    pub(super) fn take(&self, short: impl Fn() -> bool) -> bool {
        loop {
            let from = match self.0.load(Ordering::SeqCst) {
                NEEDED => NEEDED,
                DONE if short() => DONE,
                _ => return false,
            };
            if self
                .0
                .compare_exchange(from, from | PREPARING, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Both bits cleared once the prepared end is published: the preparer's ownership ends, and
    /// the flush it runs next sets *needed* anew if padding still waits.
    pub(super) fn done(&self) {
        let was = self.0.swap(DONE, Ordering::SeqCst);
        assert!(
            was & PREPARING != 0,
            "a preparation ended that was not taken"
        );
    }

    /// Whether a flush's padding waits for a segment: a wait on the log's durability then waits on
    /// its preparer.
    pub(super) fn needed(&self) -> bool {
        self.0.load(Ordering::SeqCst) & NEEDED != 0
    }

    /// Whether the preparer is preparing a segment.
    #[cfg(test)]
    pub(super) fn preparing(&self) -> bool {
        self.0.load(Ordering::SeqCst) & PREPARING != 0
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::atomic::AtomicUsize;
    use loom::sync::Arc;
    use loom::thread;

    fn check(f: impl Fn() + Sync + Send + 'static) {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(f);
    }

    /// Two flushes meet padding at once: exactly one moves the word to needed, so the preparer is
    /// triggered once for it.
    #[test]
    fn one_flush_moves_the_word_to_needed() {
        check(|| {
            let word = Arc::new(Word::default());
            let moved = Arc::new(AtomicUsize::new(0));
            let flushes: Vec<_> = (0..2)
                .map(|_| {
                    let (word, moved) = (word.clone(), moved.clone());
                    thread::spawn(move || {
                        if word.need() {
                            moved.fetch_add(1, Ordering::SeqCst);
                        }
                    })
                })
                .collect();
            for f in flushes {
                f.join().unwrap();
            }
            assert_eq!(moved.load(Ordering::SeqCst), 1, "moved twice or never");
            assert!(word.needed());
        });
    }

    /// A flush meets padding while the preparer prepares and ends: either it set *needed* after
    /// the end, moving the word and so triggering the preparer, or it set it before, in which
    /// case the end cleared it and the preparer's own flush after the end looks again. Never a
    /// trigger lost with the bit left set and nobody to read it.
    #[test]
    fn a_need_during_a_preparation_triggers_after_it_or_is_left_to_its_flush() {
        check(|| {
            let word = Arc::new(Word::default());
            assert!(word.take(|| true), "a short log's word not taken");
            let preparer = {
                let word = word.clone();
                thread::spawn(move || word.done())
            };
            let moved = word.need();
            preparer.join().unwrap();
            // Moved: set after the end, the preparer triggered. Not: set before, cleared by the end.
            assert_eq!(word.needed(), moved, "the bit and the trigger disagree");
            assert!(!word.preparing(), "a preparation left taken");
        });
    }
}
