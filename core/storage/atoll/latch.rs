//! The versioned latch (CONCURRENCY.md §5): one 64-bit word, an exclusive bit and 63 bits of
//! version.
//!
//! There is one kind of hold, exclusive, for changing a page or what a frame holds. Readers take
//! nothing: they remember the version, read, and validate that it did not change. Releasing an
//! exclusive hold bumps the version, unless nothing was written under it.
//!
//! A hold exists only as a [`Held`] guard, from the instruction that took it, so a panic while it
//! lives releases it: no path can leave a latch held.
//!
//! Nothing waits for a latch. A taker that finds it held yields to its caller (`IOResult::IO` with
//! an explicit yield) and tries again: under Turso's cooperative scheduling the holder may be
//! another connection on the very same thread.

use self::sync::{fence, AtomicU64, Ordering};
use std::mem::ManuallyDrop;
use std::ops::Deref;

#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{fence, AtomicU64, Ordering};
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{fence, AtomicU64, Ordering};
}

const EXCLUSIVE: u64 = 1;

#[derive(Debug, Default)]
pub struct VersionedLatch(AtomicU64);

fn version(word: u64) -> u64 {
    word >> 1
}

fn held(word: u64) -> bool {
    word & EXCLUSIVE != 0
}

/// Between taking the latch and the first write under it. A reader that reads one of those writes
/// and then validates, across its acquire fence, finds the latch taken. The exchange that took it
/// is an acquire, which keeps later accesses after its read but lets a later store be seen before
/// its own: without this, a reader could read a write under way and still find the version it
/// began with.
fn before_writes() {
    fence(Ordering::Release);
}

impl VersionedLatch {
    /// The version to validate against later, or `None` while a writer holds the latch.
    pub fn optimistic(&self) -> Option<u64> {
        let word = self.0.load(Ordering::Acquire);
        (!held(word)).then_some(version(word))
    }

    /// Whether nothing was written under the latch since [`optimistic`](Self::optimistic).
    pub fn validate(&self, seen: u64) -> bool {
        // The fence orders the reads before it ahead of this load, which then needs no acquire.
        fence(Ordering::Acquire);
        self.0.load(Ordering::Relaxed) == seen << 1
    }

    fn try_lock_exclusive(&self) -> bool {
        let word = self.0.load(Ordering::Relaxed);
        let taken = !held(word)
            && self
                .0
                .compare_exchange(word, word | EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
                .is_ok();
        if taken {
            before_writes();
        }
        taken
    }

    fn try_lock_exclusive_at(&self, seen: u64) -> bool {
        let taken = self
            .0
            .compare_exchange(
                seen << 1,
                seen << 1 | EXCLUSIVE,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok();
        if taken {
            before_writes();
        }
        taken
    }

    fn unlock_exclusive_unchanged(&self) {
        // Relaxed, here and below: the holder's own word.
        let word = self.0.load(Ordering::Relaxed);
        debug_assert!(held(word));
        self.0.store(word & !EXCLUSIVE, Ordering::Release);
    }

    fn unlock_exclusive(&self) {
        let word = self.0.load(Ordering::Relaxed);
        debug_assert!(held(word));
        self.0.store((version(word) + 1) << 1, Ordering::Release);
    }
}

/// An exclusive hold on the latch `L` reaches: `&VersionedLatch`, or an owner that keeps the
/// latch alive, for a hold that outlives the borrow (a read in flight). Dropping it releases the
/// latch and bumps the version.
#[must_use = "a hold is released when dropped"]
pub struct Held<L: Deref<Target = VersionedLatch>>(L);

impl<L: Deref<Target = VersionedLatch>> Held<L> {
    /// Takes the latch if no one holds it.
    pub fn try_take(latch: L) -> Option<Self> {
        latch.try_lock_exclusive().then(|| Self(latch))
    }

    /// Takes the latch if nothing was written since `seen`: the holder then changes exactly what
    /// it read, or what it checked the frame holds.
    pub fn try_take_at(latch: L, seen: u64) -> Option<Self> {
        latch.try_lock_exclusive_at(seen).then(|| Self(latch))
    }

    /// What reaches the latch.
    pub fn latch(&self) -> &L {
        &self.0
    }

    /// The version: what it stays at after [`release_unchanged`](Self::release_unchanged). Read
    /// relaxed: the holder's own word.
    pub fn version(&self) -> u64 {
        version(self.0 .0.load(Ordering::Relaxed))
    }

    /// Releases a hold under which nothing was written, keeping the version, so readers are not
    /// sent back for nothing.
    pub fn release_unchanged(self) {
        let this = ManuallyDrop::new(self);
        // SAFETY: `this` is never used or dropped again; the latch reference is dropped here.
        let latch = unsafe { std::ptr::read(&this.0) };
        latch.unlock_exclusive_unchanged();
    }
}

impl<L: Deref<Target = VersionedLatch>> Drop for Held<L> {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.0.unlock_exclusive();
    }
}

#[cfg(all(test, not(shuttle), not(atoll_loom)))]
mod tests {
    use super::*;

    #[test]
    fn an_optimistic_read_fails_validation_after_a_write_and_only_then() {
        let latch = VersionedLatch::default();
        let seen = latch.optimistic().unwrap();
        let hold = Held::try_take(&latch).unwrap();
        assert_eq!(latch.optimistic(), None);
        assert!(Held::try_take(&latch).is_none(), "two holders");
        assert!(
            !latch.validate(seen),
            "validated while a writer held the latch"
        );
        drop(hold);
        assert!(!latch.validate(seen), "validated across a write");
        assert_eq!(latch.optimistic(), Some(seen + 1));
    }

    #[test]
    fn a_hold_at_a_version_is_taken_only_while_nothing_was_written() {
        let latch = VersionedLatch::default();
        let seen = latch.optimistic().unwrap();
        let hold = Held::try_take_at(&latch, seen).unwrap();
        assert!(Held::try_take_at(&latch, seen).is_none(), "taken twice");
        drop(hold);
        assert!(
            Held::try_take_at(&latch, seen).is_none(),
            "taken across a write"
        );
        let now = latch.optimistic().unwrap();
        let hold = Held::try_take(&latch).unwrap();
        assert_eq!(hold.version(), now);
        hold.release_unchanged();
        assert!(
            latch.validate(now),
            "an unchanged release invalidated a reader"
        );
        drop(Held::try_take_at(&latch, now).unwrap());
    }

    /// A panic while the latch is held ends the process before the latch is let go (DESIGN.md
    /// 22.11). In a child process an observer thread watches the latch held by another thread,
    /// which then panics: the child must end by abort, and the observer, recording what it sees to
    /// a synced file, must never have found the latch free. Were the guard to let it go, the
    /// holder's thread would end, the observer would find the latch free, and the child would exit.
    #[cfg(all(unix, not(miri)))]
    #[test]
    fn a_panic_while_holding_the_latch_ends_the_process_before_it_is_let_go() {
        use crate::storage::atoll::testing::{in_child, record, recorded};
        use crate::sync::Arc;
        let ended = in_child(
            concat!(
                module_path!(),
                "::a_panic_while_holding_the_latch_ends_the_process_before_it_is_let_go"
            ),
            || {
                let latch = Arc::new(VersionedLatch::default());
                let (held, watched) = (std::sync::mpsc::channel(), std::sync::mpsc::channel());
                let holder = {
                    let latch = latch.clone();
                    let (held, watched) = (held.0, watched.1);
                    std::thread::spawn(move || {
                        let _hold = Held::try_take(latch).unwrap();
                        held.send(()).unwrap();
                        watched.recv().unwrap();
                        panic!("a holder failed");
                    })
                };
                held.1.recv().unwrap();
                let observer = std::thread::spawn(move || {
                    record("seen", "watching");
                    watched.0.send(()).unwrap();
                    while latch.optimistic().is_none() {
                        std::thread::yield_now();
                    }
                    record("seen", "free");
                });
                let _ = holder.join();
                observer.join().unwrap();
            },
        );
        let Some(ended) = ended else { return };
        let seen = recorded(&ended, "seen");
        assert!(
            ended.aborted,
            "the child did not abort: {:?}\n{}",
            ended.code, ended.stderr
        );
        assert!(ended.stderr.contains("a holder failed"), "{}", ended.stderr);
        assert!(
            seen.contains("watching"),
            "the observer never ran: {seen:?}"
        );
        assert!(
            !seen.contains("free"),
            "the observer found the latch free: {seen:?}"
        );
    }
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// A read that validates saw no write under way: against a writer that takes the latch,
    /// writes two words and lets it go, a reader that reads both optimistically and validates
    /// never finds one written and the other not.
    #[test]
    fn a_validated_read_never_mixes_a_write_under_way_with_what_was_before() {
        loom::model(|| {
            let latch = Arc::new(VersionedLatch::default());
            let words = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
            let writer = {
                let (latch, words) = (latch.clone(), words.clone());
                thread::spawn(move || {
                    let hold = Held::try_take(&*latch).expect("the only writer");
                    words[0].store(1, Ordering::Relaxed);
                    words[1].store(1, Ordering::Relaxed);
                    drop(hold);
                })
            };
            if let Some(seen) = latch.optimistic() {
                let first = words[0].load(Ordering::Relaxed);
                let second = words[1].load(Ordering::Relaxed);
                if latch.validate(seen) {
                    assert_eq!(first, second, "validated a read of a write under way");
                }
            }
            writer.join().unwrap();
        });
    }
}
