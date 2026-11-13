//! The records a database's requests are queued in, and the IO threads that submit them (DESIGN.md
//! 22.18, *A follow-on at retirement* and *Acceptance, the queue and the IO threads*; ASYNC1 D1a,
//! D2a). A request whose owner needs another after it (a metadata or superblock write's sync, on a
//! file that does not write durably) is submitted through [`CountedFile::write_then`] into a record
//! held for its chain. At the request's retirement Counted's one observer makes the follow-on into
//! the same record, counted before the first request is uncounted, and publishes it to the IO
//! threads, which submit it. No callback or observer submits, so no syscall runs nested inside
//! another's delivery. In a database opened async, a request a cooperative frame makes is queued
//! the same way, in its owner's record ([`Queued`]), so no syscall waits on the polling thread; the
//! metadata cache's reads are the first such owner, and the others are D2's later slices.

use super::{io_failure, CountedFile, Requests};
use crate::io::{Completion, Stepped, IO};
use crate::storage::atoll::buffer::HeldRecord;
#[cfg(test)]
use crate::storage::atoll::locks::Mutex;
use crate::sync::{Arc, Weak};
use crate::{CompletionError, Result};

/// The engine's primitives, or loom's.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicU64, AtomicU8, Ordering};

    /// A field its record's holder alone reads and writes; under loom, loom's checked cell.
    pub(super) struct Plain<T>(std::cell::UnsafeCell<T>);

    impl<T> Plain<T> {
        pub(super) fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
        }

        pub(super) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::cell::UnsafeCell as Plain;
    pub(super) use loom::sync::atomic::{AtomicU64, AtomicU8, Ordering};
}

use sync::{AtomicU64, AtomicU8, Plain};
// A test's exit flag: the engine's atomics, whatever the build.
#[cfg(test)]
use crate::sync::atomic::{AtomicBool, Ordering as Order};

/// At rest: no chain holds the record, and its owner may take it.
const REST: u8 = 0;
/// Held by one chain, whose current holder alone touches its payload.
const HELD: u8 = 1;
/// Its follow-on is written and its bit set: the IO thread's to claim.
const READY: u8 = 2;

/// One request at a time, from its owner's take to its chain's end. Its state names its holder,
/// and only that holder touches its payload: the owner that took it; the chain's callbacks and
/// observers, each after the one before by its request's ownership word; the IO thread, which
/// claims a READY record. The payload is published to the IO thread by the bitmap's edge
/// ([`Bitmap`]), and to the record's next owner by its REST, `Release`, which the next take
/// acquires; whoever lets it go touches it no more.
pub(super) struct Record<P> {
    state: AtomicU8,
    payload: Plain<Option<P>>,
}

// SAFETY: the payload is reached only by the holder the state names, handed between threads by
// the orders above.
unsafe impl<P: Send> Sync for Record<P> {}
unsafe impl<P: Send> Send for Record<P> {}

impl<P> Default for Record<P> {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(REST),
            payload: Plain::new(None),
        }
    }
}

impl<P> Record<P> {
    /// Takes the record if it is at rest, acquiring what its last holder let go.
    pub(super) fn take(&self) -> bool {
        self.state
            .compare_exchange(REST, HELD, sync::Ordering::Acquire, sync::Ordering::Relaxed)
            .is_ok()
    }

    /// The payload, to its holder.
    ///
    /// # Safety
    ///
    /// The caller holds the record: it took it, claimed it, or is its chain's callback or
    /// observer, each ordered after the holder before it.
    pub(super) unsafe fn with<R>(&self, f: impl FnOnce(&mut Option<P>) -> R) -> R {
        // SAFETY: the caller's holding, above, makes this the only access.
        self.payload.with_mut(|p| f(unsafe { &mut *p }))
    }

    /// Publishes its request, already written, as bit `i` of `bits`: READY, `Relaxed`, then its
    /// bit, whose `fetch_or` publishes the payload, the claim being ordered by the take of the bit.
    /// The caller touches the record no more; the idle IO thread to unpark, if its bit found one.
    pub(super) fn publish(&self, bits: &Bitmap, i: usize) -> Option<usize> {
        let was = self.state.swap(READY, sync::Ordering::Relaxed);
        debug_assert_eq!(was, HELD, "a follow-on made into a record not held");
        bits.set(i)
    }

    /// The IO thread's claim of a READY record, after its bit was taken with `Acquire`.
    pub(super) fn claim(&self) -> bool {
        self.state
            .compare_exchange(
                READY,
                HELD,
                sync::Ordering::Relaxed,
                sync::Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Ends the holding: the payload taken out first, then the record let go, `Release`, so its
    /// next owner may take it at once; the caller has only what this returns, never the record.
    ///
    /// # Safety
    ///
    /// As [`with`](Self::with): the caller holds the record.
    pub(super) unsafe fn finish(&self) -> Option<P> {
        // SAFETY: the caller holds it, and lets it go only below.
        let payload = unsafe { self.with(Option::take) };
        let was = self.state.swap(REST, sync::Ordering::Release);
        debug_assert_eq!(was, HELD, "a record let go that was not held");
        payload
    }

    #[cfg(test)]
    pub(super) fn at_rest(&self) -> bool {
        self.state.load(sync::Ordering::Acquire) == REST
    }
}

/// The records' ready bits, as a tree of words (ASYNC1 D2a §2): a leaf bit per record, an inner
/// word's bit per child word, and a root whose low bits are its children's, with one idle bit per
/// IO thread and the stop above them. Every hand-off and every wake is a read-modify-write of one
/// word, so each edge is ordered by that word's modification order alone, as D1a's single word
/// was (its one level is depth 1 here).
///
/// A producer sets its leaf bit with `fetch_or`, `AcqRel`, after writing the payload, and climbs
/// to the parent only if the word read zero; a word that read non-zero already has its bit in its
/// parent, or owes it to one of two: a producer still climbing, or a taker that cleared the parent
/// bit and has yet to swap the word, whose swap takes this bit with the rest (#4841). At the root
/// it reads the idle bits from its own `fetch_or`'s result, whatever else the root holds, and wakes
/// one idle thread if it found any.
///
/// A take serves one record. The taker clears one root child bit with `fetch_and`, and having
/// cleared it alone swaps that child to zero, `Acquire`; it keeps one bit of what it swapped and
/// puts the rest back as a producer would, climbing only from a word that read zero and waking an
/// idle thread for them, one at most for the whole take; and so on down to a leaf, whose bit is
/// its record. The leaf's swap reading the producer's `fetch_or` is the payload's publication
/// edge. A bit set before a swap is taken by it; the first set after it finds the word zero and
/// sets the parent bit again. So records in one word are served by as many threads as are idle,
/// never one after another behind a syscall, and a cursor per thread, moved past each record it
/// serves, takes the next ready in the records' order (#4842): no ready record waits behind
/// others served again and again, in its word or any other. A parent bit may outlive its child's bits (a
/// producer still climbing when another take emptied the child), and a take that finds an empty
/// child starts again at the root.
///
/// An IO thread idles only by exchanging a root with no child bit and no stop for the same root
/// with its idle bit set, so a child bit or the stop set before it fails the exchange, and one set
/// after it reads the idle bit and owes the thread its unpark: no wake is lost. The stop takes
/// every idle bit as it unparks their threads.
pub(super) struct Bitmap {
    /// The levels, the root's (one word) first and the leaves' last.
    levels: Box<[Box<[AtomicU64]>]>,
    /// The bits a word below the root holds: [`WORD`], fewer in a model.
    fan: usize,
    /// The records one bit of each level stands for: `fan` to the power of the levels below it.
    strides: Box<[usize]>,
    /// The root's children, and their bits.
    roots: usize,
    children: u64,
    /// The IO threads' idle bits.
    idle: u64,
    /// The read-modify-writes the sets have made, for a test's count of what a set costs.
    #[cfg(test)]
    set_rmws: crate::sync::atomic::AtomicUsize,
    /// The takes that served nothing: a root bit lost to another take, or an emptied child.
    #[cfg(test)]
    retries: crate::sync::atomic::AtomicUsize,
}

/// The bits of a word.
const WORD: usize = 64;
/// Close has sealed the requests, or the last of them dropped: every IO thread ends once it has
/// served every bit set.
const STOP: u64 = 1 << 63;
/// The most IO threads a database takes: their idle bits sit below the stop, and with the root's
/// one child bit at least the layout holds 62.
pub(super) const MOST_THREADS: usize = 16;
/// The most records a dispatch makes, held to what a 32-bit `usize` holds: a tree of at most 6
/// levels.
pub(super) const MOST_RECORDS: usize = u32::MAX as usize;

/// Thread `k`'s idle bit.
fn idle_bit(k: usize) -> u64 {
    1 << (62 - k)
}

/// The first bit of `bits` at or after `start`, else its first.
fn first_from(bits: u64, start: usize) -> usize {
    let after = bits & !((1u64 << start) - 1);
    if after != 0 { after } else { bits }.trailing_zeros() as usize
}

/// What one take found.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Taken {
    /// A record to serve, and the idle thread to unpark for the bits put back beside it.
    Record { record: usize, wake: Option<usize> },
    /// No record this take, which ended after its one descent: its root bit taken by another
    /// thread first, or a child emptied by another take (a parent bit a climbing producer set).
    /// Not idle: the caller takes again, unparking the thread woken for bits it put back, if any.
    Retry { wake: Option<usize> },
    /// No child bit set, and no stop.
    Empty,
    /// No child bit set, and the stop set.
    Stopped,
}

impl Bitmap {
    /// A tree over `records` bits for `threads` IO threads.
    pub(super) fn new(records: usize, threads: usize) -> Self {
        Self::with_fan(records, threads, WORD)
    }

    /// The same, each word below the root holding `fan` bits: a model's small fan-out makes a deep
    /// tree of few records.
    pub(super) fn with_fan(records: usize, threads: usize, fan: usize) -> Self {
        assert!(
            (1..=MOST_THREADS).contains(&threads),
            "{threads} IO threads, outside 1 to {MOST_THREADS}"
        );
        assert!((2..=WORD).contains(&fan), "a fan-out of {fan}");
        assert!(
            records <= MOST_RECORDS,
            "{records} records past {MOST_RECORDS}"
        );
        let root_fan = (63 - threads).min(fan);
        // Words per level from the leaves up, until one root holds what is left.
        let mut widths = Vec::new();
        let mut below = records.max(1);
        while below > root_fan {
            let words = below.div_ceil(fan);
            widths.push(words);
            below = words;
        }
        widths.push(1);
        widths.reverse();
        let depth = widths.len();
        Self {
            levels: widths
                .iter()
                .map(|&n| (0..n).map(|_| AtomicU64::new(0)).collect())
                .collect(),
            fan,
            // The records one bit of each level below the root stands for; the root's own power is
            // never read, and past a 32-bit `usize` at six levels.
            strides: (0..=depth)
                .map(|l| match l {
                    0 => 0,
                    _ => fan
                        .checked_pow((depth - l) as u32)
                        .expect("a tree's stride within a usize"),
                })
                .collect(),
            roots: below,
            children: (1u64 << below) - 1,
            idle: (0..threads).map(idle_bit).fold(0, |m, b| m | b),
            #[cfg(test)]
            set_rmws: crate::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            retries: crate::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn root(&self) -> &AtomicU64 {
        &self.levels[0][0]
    }

    /// Sets record `i`'s bit: the IO thread to unpark, if its climb reached the root and found one
    /// idle there.
    pub(super) fn set(&self, i: usize) -> Option<usize> {
        let leaves = self.levels.len() - 1;
        if leaves == 0 {
            #[cfg(test)]
            self.set_rmws
                .fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
            return self.wake_one(self.root().fetch_or(1 << i, sync::Ordering::AcqRel));
        }
        self.raise(leaves, i / self.fan, 1 << (i % self.fan), true)
    }

    /// Sets `bits` in word `index` of `level`, below the root, and climbs while each word it sets
    /// read zero, the root's bit last: the idle thread to unpark, if the root showed one and the
    /// caller `may_wake`. A caller that may not publishes the same bits and takes no idle bit, which
    /// stays set for the next producer's wake or the stop to find.
    fn raise(&self, level: usize, index: usize, bits: u64, may_wake: bool) -> Option<usize> {
        let (mut index, mut bits) = (index, bits);
        for words in self.levels[1..=level].iter().rev() {
            #[cfg(test)]
            self.set_rmws
                .fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
            if words[index].fetch_or(bits, sync::Ordering::AcqRel) != 0 {
                return None;
            }
            bits = 1 << (index % self.fan);
            index /= self.fan;
        }
        #[cfg(test)]
        self.set_rmws
            .fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
        let was = self.root().fetch_or(bits, sync::Ordering::AcqRel);
        if may_wake {
            self.wake_one(was)
        } else {
            None
        }
    }

    /// Takes one idle thread's bit, if `was`, the root as a `fetch_or` just read it, shows any:
    /// that thread, to be unparked. Each idle bit is tried at most once (#4829), so this makes at
    /// most one attempt per IO thread whatever the others do meanwhile. A failed attempt found its
    /// thread woken by another producer after this one's child bit was set, and that thread serves
    /// the bit before it can idle again: an idle exchange refuses a root with a child bit.
    fn wake_one(&self, mut was: u64) -> Option<usize> {
        let mut untried = self.idle;
        loop {
            let idle = was & untried;
            if idle == 0 {
                return None;
            }
            let at = 63 - idle.leading_zeros();
            let bit = 1u64 << at;
            untried &= !bit;
            #[cfg(all(test, not(atoll_loom)))]
            crate::race::point("bitmap.wake_trying");
            was = self.root().fetch_and(!bit, sync::Ordering::AcqRel);
            if was & bit != 0 {
                return Some(62 - at as usize);
            }
        }
    }

    /// Sets the stop, and takes every idle bit with it: the threads to unpark.
    pub(super) fn stop(&self) -> impl Iterator<Item = usize> {
        self.root().fetch_or(STOP, sync::Ordering::AcqRel);
        let mut idle = self.root().fetch_and(!self.idle, sync::Ordering::AcqRel) & self.idle;
        std::iter::from_fn(move || {
            (idle != 0).then(|| {
                let at = 63 - idle.leading_zeros();
                idle &= !(1u64 << at);
                62 - at as usize
            })
        })
    }

    /// Takes one record by one descent: the first ready at or after the cursor `*from` in the
    /// records' order, wrapping past the last, and moves the cursor past it (#4842). A level on the
    /// cursor's own path starts at the cursor's place in its word, one off it at the word's first
    /// bit. A word on the path with nothing at or after the cursor's place holds the next record
    /// only if none waits outside it: then its first bit is the order's wrap; else every bit goes
    /// back, the cursor moves to the word's next sibling and the take ends as [`Taken::Retry`],
    /// which a take that serves nothing also ends as. So one take is at most one root read, one
    /// root `fetch_and` and, per level below, one swap and one put-back; and a record ready while
    /// others are served again and again is served once the cursor comes round to it.
    pub(super) fn take(&self, from: &mut usize) -> Taken {
        let depth = self.levels.len();
        let word = self.root().load(sync::Ordering::Acquire);
        let kids = word & self.children;
        if kids == 0 {
            return if word & STOP != 0 {
                Taken::Stopped
            } else {
                Taken::Empty
            };
        }
        let child = first_from(kids, (*from / self.strides[1]) % self.roots);
        let bit = 1u64 << child;
        // Cleared by another thread since the load: it takes that child.
        if self.root().fetch_and(!bit, sync::Ordering::AcqRel) & bit == 0 {
            #[cfg(test)]
            self.retries
                .fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
            return Taken::Retry { wake: None };
        }
        #[cfg(all(test, not(atoll_loom)))]
        crate::race::point("bitmap.take_claimed");
        let (mut index, mut wake) = (child, None);
        // On the cursor's own path while every level above chose the cursor's own place there.
        let mut on_path = child == (*from / self.strides[1]) % self.roots;
        // A record waits outside the word this descent is in: another root child, or a bit put
        // back at a level above.
        let mut elsewhere = kids & !bit != 0;
        for level in 1..depth {
            let bits = self.levels[level][index].swap(0, sync::Ordering::Acquire);
            if bits == 0 {
                // A parent bit its child's take outlived: nothing beneath it.
                #[cfg(test)]
                self.retries
                    .fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
                return Taken::Retry { wake };
            }
            let place = if on_path {
                (*from / self.strides[level + 1]) % self.fan
            } else {
                0
            };
            let b = first_from(bits, place);
            if on_path && b < place && elsewhere {
                // The next record in turn is past this word: all of it goes back, as a put-back,
                // and the cursor moves to its next sibling, the order's wrap past the last.
                let woke = self.raise(level, index, bits, wake.is_none());
                wake = wake.or(woke);
                *from = (index + 1).checked_mul(self.strides[level]).unwrap_or(0);
                #[cfg(test)]
                self.retries
                    .fetch_add(1, crate::sync::atomic::Ordering::Relaxed);
                return Taken::Retry { wake };
            }
            on_path &= b == place;
            let rest = bits & !(1u64 << b);
            if rest != 0 {
                // Every put-back publishes its bits; a take claims one wake at most, the first, so
                // no idle bit is taken without its thread being unparked (#4828: in a deep tree a
                // later put-back can reach the root again after another take served the first).
                let woke = self.raise(level, index, rest, wake.is_none());
                wake = wake.or(woke);
                elsewhere = true;
            }
            #[cfg(all(test, not(atoll_loom)))]
            crate::race::point("bitmap.take_descended");
            index = index * self.fan + b;
        }
        *from = index + 1;
        Taken::Record {
            record: index,
            wake,
        }
    }

    /// Marks thread `k` idle if no child bit and no stop is set: whether it may park. Each retry
    /// follows another thread's change of the root.
    pub(super) fn idle(&self, k: usize) -> bool {
        let mut word = self.root().load(sync::Ordering::Acquire);
        loop {
            if word & (self.children | STOP) != 0 {
                return false;
            }
            match self.root().compare_exchange(
                word,
                word | idle_bit(k),
                sync::Ordering::AcqRel,
                sync::Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(now) => word = now,
            }
        }
    }

    /// Whether thread `k`'s idle bit is still set: no producer and no stop has taken it, so an
    /// unpark that finds it was spurious.
    pub(super) fn still_idle(&self, k: usize) -> bool {
        self.root().load(sync::Ordering::Acquire) & idle_bit(k) != 0
    }

    /// The tree's depth, for a test.
    #[cfg(test)]
    pub(super) fn depth(&self) -> usize {
        self.levels.len()
    }

    /// The read-modify-writes every set so far has made.
    #[cfg(test)]
    pub(super) fn set_rmws(&self) -> usize {
        self.set_rmws.load(crate::sync::atomic::Ordering::Relaxed)
    }

    /// The takes so far that served nothing.
    #[cfg(test)]
    pub(super) fn retries(&self) -> usize {
        self.retries.load(crate::sync::atomic::Ordering::Relaxed)
    }
}

/// The log's doorbell (D1b §1, §3): two bits on one byte, every operation on it a read-modify-write,
/// so its order is one release sequence after each ring. RUNG: work owed, rung since the last
/// unit began. SERVING: a unit runs. While it is not quiet the door holds one count on the
/// requests (§3.4 names the windows where an old and a new one overlap).
pub(super) struct Door(AtomicU8);

const RUNG: u8 = 1 << 1;
const SERVING: u8 = 1 << 0;

/// What a unit's end leaves: the door quiet, its count to let go, or rung again, its count kept
/// and its bit to set.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum End {
    Quiet,
    Again,
}

impl Door {
    pub(super) fn new() -> Self {
        Self(AtomicU8::new(0))
    }

    /// A ring: whether it found the door quiet, and so owes it a count and its bit. Whatever it
    /// finds, the RMW releases what its caller wrote before it (§3.3).
    pub(super) fn ring(&self) -> bool {
        self.0.fetch_or(RUNG, sync::Ordering::AcqRel) == 0
    }

    /// A unit's start, from RUNG: the acquire that reads every ring before it. Whether the door
    /// read RUNG; anything else breaks the protocol, as only the IO thread clears RUNG or sets
    /// SERVING.
    pub(super) fn start(&self) -> bool {
        self.0
            .compare_exchange(
                RUNG,
                SERVING,
                sync::Ordering::AcqRel,
                sync::Ordering::Relaxed,
            )
            .is_ok()
    }

    /// A unit's end: quiet if nothing is owed and nothing rang during it, else rung again.
    pub(super) fn end(&self, owed: bool) -> End {
        if !owed
            && self
                .0
                .compare_exchange(SERVING, 0, sync::Ordering::AcqRel, sync::Ordering::Relaxed)
                .is_ok()
        {
            return End::Quiet;
        }
        self.0.swap(RUNG, sync::Ordering::AcqRel);
        End::Again
    }

    /// Quiet, with nothing to let go: the requests are gone, and no producer is left to ring.
    pub(super) fn quiet(&self) {
        self.0.swap(0, sync::Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(super) fn read(&self) -> u8 {
        self.0.load(sync::Ordering::Acquire)
    }
}

/// The log's work for one unit (D1b §4), served by the IO thread: whether more is owed.
pub(in crate::storage::atoll) trait LogWork:
    Send + Sync + 'static
{
    fn unit(self: Arc<Self>) -> bool;
}

/// What a chain's owner is told of it (DESIGN.md 22.18, D1a §2): the write's outcome, the sync's,
/// and the chain's end once every request of it has retired.
pub(in crate::storage::atoll) trait Chain: Send + Sync + 'static {
    /// The write's outcome, a short write already an error; whether its sync is to follow.
    fn written(&self, res: std::result::Result<(), CompletionError>) -> bool;
    /// The sync's outcome, when one followed.
    fn synced(&self, res: std::result::Result<(), CompletionError>);
    /// Once, after every request of the chain retired: its first error, or `Ok`.
    fn ended(&self, outcome: std::result::Result<(), CompletionError>);
}

/// A chain's state in its record.
pub(super) struct Link {
    chain: Arc<dyn Chain>,
    /// The sync, while it is still to be made.
    sync_owed: bool,
    outcome: std::result::Result<(), CompletionError>,
    /// The sync made and published, while it waits for the IO thread.
    ready: Option<Ready>,
}

/// A follow-on published to the IO thread: its file weakly, so no record keeps a file, and the
/// completion Counted made for it, whose callback reaches the record weakly too.
struct Ready {
    file: Weak<CountedFile>,
    completion: Completion,
}

/// Where a queued request's file comes from: open already, or opened by the request's first stage.
pub(in crate::storage::atoll) enum Source {
    Open(Weak<CountedFile>),
    /// A cold file, opened by its owner's stage with this key.
    Stage(Weak<dyn OpensFile>, u64),
}

/// What opens a cold file as a queued request's first stage, run on an IO thread, a blocking host
/// (DESIGN.md 22.18, *The foreground file calls*): the open with its lock, its directory's sync,
/// and the install where its owner keeps it, the first to set it kept.
pub(in crate::storage::atoll) trait OpensFile:
    Send + Sync + 'static
{
    fn open_for(&self, key: u64) -> Result<Arc<CountedFile>>;
}

/// A queued request (D2a §3), from its acceptance to its retirement: its file, its position, the
/// completion its owner made, which holds the region it reads into, and the metadata read credit it
/// was admitted under, whose index is the record's and whose drop, after the record is let go,
/// gives it back. Only the cache's admission makes a credit, so no other site can queue into a
/// record. D2a queues reads; the writes and syncs are the later slices'.
pub(in crate::storage::atoll) struct Queued {
    source: Source,
    pos: u64,
    completion: Completion,
    _credit: crate::storage::atoll::meta::Credit,
}

/// Where a queued frame request goes and what it does (D2b §1), in one word that is never zero:
/// whether it writes, whether its page is temporary, its size class plus one, and the slot or the
/// temporary number. The file and the offset in it follow from it ([`FrameOwner::file_for`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::storage::atoll) struct Place(std::num::NonZeroU64);

impl Place {
    const WRITE: u64 = 1 << 63;
    const TEMPORARY: u64 = 1 << 62;
    const CLASS_SHIFT: u32 = 58;
    const CLASS: u64 = 0xf << Self::CLASS_SHIFT;
    /// The slots and temporary numbers a place holds.
    const NUMBER: u64 = (1 << Self::CLASS_SHIFT) - 1;

    /// The place of slot or temporary number `number` of `class`. A number past the place's bits
    /// is a page store past any size a radix tree of the store's height reaches: an invariant.
    pub(in crate::storage::atoll) fn new(
        write: bool,
        temporary: bool,
        class: crate::storage::atoll::page::SizeClass,
        number: u64,
    ) -> Self {
        crate::turso_assert!(
            number <= Self::NUMBER,
            "a slot or temporary number past a queued request's place"
        );
        let word = if write { Self::WRITE } else { 0 }
            | if temporary { Self::TEMPORARY } else { 0 }
            | ((class.index() as u64 + 1) << Self::CLASS_SHIFT)
            | number;
        Self(std::num::NonZeroU64::new(word).expect("a class plus one is never zero"))
    }

    pub(in crate::storage::atoll) fn writes(self) -> bool {
        self.0.get() & Self::WRITE != 0
    }

    pub(in crate::storage::atoll) fn temporary(self) -> bool {
        self.0.get() & Self::TEMPORARY != 0
    }

    pub(in crate::storage::atoll) fn class(self) -> crate::storage::atoll::page::SizeClass {
        let index = ((self.0.get() & Self::CLASS) >> Self::CLASS_SHIFT) as usize - 1;
        crate::storage::atoll::page::SizeClass::new(index).expect("a place's class")
    }

    pub(in crate::storage::atoll) fn number(self) -> u64 {
        self.0.get() & Self::NUMBER
    }
}

// The class plus one fits the place's four class bits.
const _: () = assert!(crate::storage::atoll::page::CLASS_COUNT < 16);

/// A queued frame request (D2b §1), from its acceptance to its retirement: its place and the
/// completion the pool made, which a read's region rides in. A write's region is its frame's own
/// memory, asked of the owner at the take, so the record carries no buffer. Admitted only by the
/// frame's held record ([`HeldRecord`]), whose frame is the record's index.
pub(in crate::storage::atoll) struct FrameQueued {
    place: Place,
    completion: Completion,
}

// A place's niche keeps the record at its state byte and two words (D2b §1, the auditor's #4872).
#[cfg(not(any(shuttle, atoll_loom)))]
const _: () = assert!(
    std::mem::size_of::<Option<FrameQueued>>() == std::mem::size_of::<FrameQueued>()
        && std::mem::size_of::<Record<FrameQueued>>() == 24
);

/// A queued read of a rollback's read-back (D2c §2), from its acceptance to its retirement: the log
/// segment's counted file, held from the take to the release, the position in it, and the
/// completion the read-back made, which holds the region it reads into. Admitted only by its
/// transaction slot's record at rest. A running transaction holds the log back for its parts, so
/// the segment stays open while it runs, and the file's descriptor stays open through the
/// carrier's retirement (RING-FD1).
pub(in crate::storage::atoll) struct SlotQueued {
    file: Arc<CountedFile>,
    pos: u64,
    completion: Completion,
}

/// The owner of the frame records, the pool, registered once at its open (D2b §1).
pub(in crate::storage::atoll) trait FrameOwner:
    Send + Sync + 'static
{
    /// The counted file `place` is in and the offset in it, the file opened as this request's
    /// first stage when cold (a data file's open and its directory's sync; a temporary file's
    /// open), installed where its store keeps it, the first to set it kept.
    fn file_for(&self, place: Place) -> Result<(Arc<CountedFile>, u64)>;
    /// A write-out's region: `frame`'s memory at `place`'s class, which the write-out holds
    /// outgoing and the frame's record holds to the retirement, so it is the write's alone. The
    /// buffer keeps the memory mapped, not the owner.
    fn region(&self, frame: u32, place: Place) -> Arc<crate::io::Buffer>;
    /// `frame`'s record let its request go: a frame freed meanwhile goes to the free frames, and a
    /// claim that passed over it is told.
    fn record_retired(&self, frame: u32, class: crate::storage::atoll::page::SizeClass);
}

/// A frame request [`Dispatch::queue_frame`] refused before accepting it: its frame's held record
/// back, for its owner's one give-back, and why.
pub(in crate::storage::atoll) struct FrameRefused {
    pub(in crate::storage::atoll) held: HeldRecord,
    pub(in crate::storage::atoll) error: crate::LimboError,
}

/// How many records of each kind a database's dispatch makes, and its IO threads (D2a §1, §2).
#[derive(Clone, Copy, Debug)]
pub(in crate::storage::atoll) struct Shape {
    /// Requests a cooperative frame makes are queued: the database was opened async.
    pub(in crate::storage::atoll) opened_async: bool,
    /// IO threads, 1 to [`MOST_THREADS`].
    pub(in crate::storage::atoll) io_threads: usize,
    /// The metadata cache's read credits, one record each.
    pub(in crate::storage::atoll) meta_reads: usize,
    /// The pool's grains, one frame record each, by the grain's number (D2b §1).
    pub(in crate::storage::atoll) frames: usize,
    /// The transaction slots, one read-back record each, by the slot's number (D2c §2).
    pub(in crate::storage::atoll) slots: usize,
}

impl Default for Shape {
    fn default() -> Self {
        Self {
            opened_async: false,
            io_threads: 1,
            meta_reads: 0,
            frames: 0,
            slots: 0,
        }
    }
}

/// The records a database's requests are queued in, the tree of bits that publishes them and stops
/// the IO threads that submit them, and the threads, to be woken.
pub(in crate::storage::atoll) struct Dispatch {
    records: Box<[Record<Link>]>,
    /// The queued requests' records, one per metadata read credit, by the credit's index.
    queued: Box<[Record<Queued>]>,
    /// The pool's frame records, one per grain, by the frame's number (D2b §1).
    frames: Box<[Record<FrameQueued>]>,
    /// The frame records' owner, the pool, set once at its open before any frame can queue.
    frames_owner: crate::sync::OnceLock<Weak<dyn FrameOwner>>,
    /// The transaction slots' read-back records, one per slot, by the slot's number (D2c §2).
    slot_reads: Box<[Record<SlotQueued>]>,
    /// Each slot record's rest wait: what a take that found the record held waits on, finished by
    /// the release once the record is at rest. Its let-go is a request's retirement, which a taker
    /// on a pumped backend brings by stepping (`LetGo::stepped`).
    slot_rest: Box<[crate::storage::atoll::notify::LetGo]>,
    bits: Bitmap,
    /// The log's doorbell, and the log it rings for, set once at the log's open.
    door: Door,
    log: crate::sync::OnceLock<Weak<dyn LogWork>>,
    /// Each IO thread, set by the thread itself before its first look at the bits, so no producer
    /// can find it idle before it can be unparked.
    threads: Box<[crate::sync::OnceLock<crate::thread::Thread>]>,
    shape: Shape,
    requests: Weak<Requests>,
    /// The thread has left its loop, for a test's look after its owners are gone.
    #[cfg(test)]
    pub(super) exited: AtomicBool,
    /// A test's hold on the IO thread before it claims a record.
    #[cfg(test)]
    hold: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    /// A test's holds before a take, one an IO thread, each taken by the next thread to look.
    #[cfg(test)]
    take_holds: Mutex<Vec<std::sync::mpsc::Receiver<()>>>,
    /// A test's notes: the thread's next idle, sent before its park, and its end.
    #[cfg(test)]
    notes: Mutex<Notes>,
    /// The log units started and those wholly ended, their count let go, for a test's count and
    /// its wait for every unit begun to be over.
    #[cfg(test)]
    units: crate::sync::atomic::AtomicUsize,
    #[cfg(test)]
    ended: crate::sync::atomic::AtomicUsize,
}

#[cfg(test)]
#[derive(Default)]
struct Notes {
    idle: Option<std::sync::mpsc::Sender<()>>,
    exit: Option<std::sync::mpsc::Sender<()>>,
    /// Each log unit's end: the door then, and the requests counted.
    unit: Option<std::sync::mpsc::Sender<(u8, usize)>>,
    /// A pause inside a quiet unit's end, after the door went quiet and before its count is let
    /// go: the door and the count there, and the release.
    end_gap: Option<(
        std::sync::mpsc::Sender<(u8, usize)>,
        std::sync::mpsc::Receiver<()>,
    )>,
    /// A pause of the next queued frame request between its backend call's return and the branch
    /// after it: its record there, and the release the IO thread waits for.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    frame_returned: Option<(
        std::sync::mpsc::Sender<usize>,
        std::sync::mpsc::Receiver<()>,
    )>,
    /// The next queued frame request's branch after its backend call, once run: its record.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    frame_served: Option<std::sync::mpsc::Sender<usize>>,
    /// A pause of the IO thread once its take found no bit and it owes no step, before it marks
    /// itself idle: whether it first withdraws its registration with a pumped backend, the note it
    /// is there, and the release it waits for (ASYNC1 D2d §6.1).
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    before_idle: Option<(
        bool,
        std::sync::mpsc::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
    /// The bits the IO thread serves and the steps it makes, in its order, while a test records
    /// them.
    turns: Option<Vec<Turn>>,
    /// A pause at one point of a slot record's take or release (D2c §3.8): where, the record
    /// there, and the release the paused thread waits for.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    slot: Option<(
        SlotPause,
        std::sync::mpsc::Sender<usize>,
        std::sync::mpsc::Receiver<()>,
    )>,
}

/// One turn of an IO thread's loop, as a test records them: a bit served, or a step of its pumped
/// backend.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::storage::atoll) enum Turn {
    Served,
    Stepped,
}

/// Where a test pauses a slot record's take or release (D2c §3.8).
#[cfg(all(test, not(any(shuttle, atoll_loom))))]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::storage::atoll) enum SlotPause {
    /// A take that found the record held, before it registers on the rest wait.
    Registering,
    /// The release, its request wholly retired and the record not yet at rest.
    BeforeRest,
    /// The release, the record at rest and its rest wait not yet let go.
    BeforeLetGo,
}

/// The records of the metadata cache's writes, `checkpoint::WRITES` of them (DESIGN.md 22.18: the
/// checkpoint's metadata unit keeps at most that many writes in flight), then the superblock's.
pub(super) const META_WRITES: std::ops::Range<usize> = 0..crate::storage::atoll::checkpoint::WRITES;
pub(super) const SUPERBLOCK: std::ops::Range<usize> = META_WRITES.end..META_WRITES.end + 1;
const RECORDS: usize = SUPERBLOCK.end;
/// The log's doorbell bit, after the records.
pub(super) const LOG: usize = RECORDS;
/// A pumped backend's wake of its IO thread, which then steps it (ASYNC1 D2d §2.3).
pub(super) const PUMP: usize = LOG + 1;
/// The queued requests' first bit, after the pump's.
pub(super) const QUEUED: usize = PUMP + 1;

/// A record taken for one chain, by its index.
pub(in crate::storage::atoll) struct Held(usize);

impl Held {
    pub(super) fn index(&self) -> usize {
        self.0
    }
}

impl Dispatch {
    pub(super) fn new(requests: &Arc<Requests>, shape: Shape) -> Self {
        Self {
            records: (0..RECORDS).map(|_| Record::default()).collect(),
            queued: (0..shape.meta_reads).map(|_| Record::default()).collect(),
            frames: (0..shape.frames).map(|_| Record::default()).collect(),
            frames_owner: crate::sync::OnceLock::new(),
            slot_reads: (0..shape.slots).map(|_| Record::default()).collect(),
            slot_rest: (0..shape.slots)
                .map(|_| crate::storage::atoll::notify::LetGo::stepped())
                .collect(),
            bits: Bitmap::new(
                QUEUED + shape.meta_reads + shape.frames + shape.slots,
                shape.io_threads,
            ),
            door: Door::new(),
            log: crate::sync::OnceLock::new(),
            threads: (0..shape.io_threads)
                .map(|_| crate::sync::OnceLock::new())
                .collect(),
            shape,
            requests: Arc::downgrade(requests),
            #[cfg(test)]
            exited: AtomicBool::new(false),
            #[cfg(test)]
            hold: Mutex::new(None),
            #[cfg(test)]
            take_holds: Mutex::new(Vec::new()),
            #[cfg(test)]
            notes: Mutex::new(Notes::default()),
            #[cfg(test)]
            units: crate::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            ended: crate::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// A record of `range` at rest, taken: none at rest is a broken admission bound (D1a §3.1.1),
    /// an error, never a wait.
    pub(super) fn take(&self, range: std::ops::Range<usize>) -> Result<Held> {
        range
            .clone()
            .find(|&i| self.records[i].take())
            .map(Held)
            .ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "no record of {range:?} at rest: more chains in flight than their owner's bound"
                ))
            })
    }

    /// Starts a chain in `held`: `chain`, its sync owed unless the write is durable.
    pub(super) fn start(&self, held: &Held, chain: Arc<dyn Chain>, durable: bool) {
        // SAFETY: just taken.
        unsafe {
            self.records[held.0].with(|link| {
                *link = Some(Link {
                    chain,
                    sync_owed: !durable,
                    outcome: Ok(()),
                    ready: None,
                })
            })
        };
    }

    /// The write's callback: tells the chain, records an error, and disarms the sync on an error
    /// or when the chain wants none.
    pub(super) fn written(&self, i: usize, res: std::result::Result<(), CompletionError>) {
        let record = &self.records[i];
        // SAFETY: the chain's callback, after the submission that wrote the payload.
        let chain = unsafe { record.with(|link| link.as_ref().map(|l| l.chain.clone())) };
        let Some(chain) = chain else { return };
        let more = chain.written(res.clone());
        // SAFETY: as above.
        unsafe {
            record.with(|link| {
                let link = link.as_mut().expect("a chain's record");
                if let Err(e) = res {
                    link.outcome = Err(e);
                }
                if !more || link.outcome.is_err() {
                    link.sync_owed = false;
                }
            })
        };
    }

    /// The sync's callback: tells the chain, and records an error.
    fn synced(&self, i: usize, res: std::result::Result<(), CompletionError>) {
        let record = &self.records[i];
        // SAFETY: the follow-on's callback, after the claim that submitted it.
        let chain = unsafe { record.with(|link| link.as_ref().map(|l| l.chain.clone())) };
        let Some(chain) = chain else { return };
        chain.synced(res.clone());
        if let Err(e) = res {
            // SAFETY: as above.
            unsafe {
                record.with(|link| {
                    let link = link.as_mut().expect("a chain's record");
                    if link.outcome.is_ok() {
                        link.outcome = Err(e);
                    }
                })
            };
        }
    }

    /// A chain that could not start: the refused write's error recorded, as no callback runs for
    /// it, then the chain ended (D1a §3.1.2).
    pub(super) fn refused(&self, held: Held, e: CompletionError) {
        // SAFETY: still held by the caller, the request never accepted.
        unsafe {
            self.records[held.0].with(|link| {
                if let Some(link) = link.as_mut() {
                    link.outcome = Err(e);
                }
            })
        };
        self.end(held.0);
    }

    /// The write's retirement, Counted's one observer: makes its sync, counted, into the record
    /// and publishes it, or ends the chain. Its caller uncounts the write after it returns.
    pub(super) fn after_write(self: &Arc<Self>, i: usize, file: &Weak<CountedFile>) {
        let record = &self.records[i];
        // SAFETY: the write's observer, after its callback.
        let owed = unsafe {
            record.with(|link| {
                let link = link.as_mut().expect("a chain's record");
                std::mem::take(&mut link.sync_owed)
            })
        };
        if !owed {
            return self.end(i);
        }
        crate::race::point("requests.followon_counting");
        let counted = self
            .requests
            .upgrade()
            .is_some_and(|requests| requests.count_one());
        if !counted {
            // SAFETY: as above.
            unsafe {
                record.with(|link| {
                    link.as_mut().expect("a chain's record").outcome = Err(closed());
                })
            };
            return self.end(i);
        }
        let me = Arc::downgrade(self);
        let completion = Completion::new_sync(move |res| {
            if let Some(me) = me.upgrade() {
                me.synced(i, res.map(|_| ()));
            }
        });
        // SAFETY: as above.
        unsafe {
            record.with(|link| {
                link.as_mut().expect("a chain's record").ready = Some(Ready {
                    file: file.clone(),
                    completion,
                })
            })
        };
        // Published: from here the record is the IO threads'.
        if let Some(k) = record.publish(&self.bits, i) {
            self.unpark(k);
        }
        crate::race::point("requests.followon_published");
    }

    /// Ends the chain in `i`: its state taken out, the record let go, then its owner told. Counts
    /// nothing (D1a §3.3).
    fn end(&self, i: usize) {
        // SAFETY: the chain's last holder.
        let link = unsafe { self.records[i].finish() }.expect("a chain's record");
        crate::race::point("requests.record_rest");
        link.chain.ended(link.outcome);
    }

    /// Unparks IO thread `k`, whose idle bit its waker took. Its handle is set before it can idle.
    pub(super) fn unpark(&self, k: usize) {
        self.threads[k]
            .get()
            .expect("an IO thread idle before its handle was set")
            .unpark();
    }

    /// Ends every IO thread once it has served every bit set.
    pub(super) fn stop(&self) {
        for k in self.bits.stop() {
            self.unpark(k);
        }
    }

    /// Serves bit `bit` the IO thread took: the log's door, a follow-on's record, a queued
    /// metadata read's, a queued frame request's or a queued read-back read's. The pump's wake
    /// serves nothing itself: the step owed after it does.
    fn serve_bit(self: &Arc<Self>, bit: usize) {
        match bit {
            LOG => self.serve_log(),
            PUMP => {}
            b if b < RECORDS => self.serve(b),
            b if b < self.frames_bit() => self.serve_queued(b - QUEUED),
            b if b < self.slots_bit() => self.serve_frame(b - self.frames_bit()),
            b => self.serve_slot_read(b - self.slots_bit()),
        }
    }

    /// The frame records' first bit, after the metadata reads' (D2b §1).
    fn frames_bit(&self) -> usize {
        QUEUED + self.shape.meta_reads
    }

    /// The slot records' first bit, after the frames' (D2c §2).
    fn slots_bit(&self) -> usize {
        self.frames_bit() + self.shape.frames
    }

    /// The log this dispatch serves: one per database, set once at its open.
    pub(super) fn attach_log(&self, log: Weak<dyn LogWork>) {
        let set = self.log.set(log);
        assert!(set.is_ok(), "a second log on one database's dispatch");
    }

    /// The log's ring, made at a request's retirement while that request is still counted (D1b
    /// §3.1). The ring that finds the door quiet takes the door's count, which cannot be refused
    /// while its own request is counted, then sets the bit.
    pub(super) fn ring_log(&self, requests: &Requests) {
        if !self.door.ring() {
            return;
        }
        let counted = requests.count_one();
        assert!(
            counted,
            "a log ring refused while its own request is counted"
        );
        if let Some(k) = self.bits.set(LOG) {
            self.unpark(k);
        }
    }

    /// A ring of the log's door by a caller inside a cooperative frame (D2c §1), which has no
    /// request of its own counted: its count is taken first, so a door moved to RUNG always holds
    /// one. A count refused, the database closing, is that error, and the door never moved. A
    /// door already rung or serving lets the count go: the ring before this one owns the unit.
    pub(super) fn ring_log_from_frame(&self, requests: &Requests) -> Result<()> {
        if !requests.count_one() {
            return Err(crate::LimboError::InternalError(
                "a closed database's log".to_string(),
            ));
        }
        if !self.door.ring() {
            requests.uncount_one();
            return Ok(());
        }
        if let Some(k) = self.bits.set(LOG) {
            self.unpark(k);
        }
        Ok(())
    }

    /// One unit of the log's work (D1b §3.2): started from RUNG, its requests and its log upgraded
    /// in that order, each optional; ended quiet with the door's count let go, or rung again.
    fn serve_log(self: &Arc<Self>) {
        #[cfg(test)]
        if let Some(hold) = self.hold.lock().take() {
            let _ = hold.recv();
        }
        let started = self.door.start();
        assert!(started, "the log's bit served with its door not rung");
        #[cfg(test)]
        self.units.fetch_add(1, Order::Relaxed);
        let Some(requests) = self.requests.upgrade() else {
            // The requests are being dropped: no counted request is left to ring, and the count
            // goes with them.
            self.door.quiet();
            #[cfg(test)]
            self.note_unit(None);
            return;
        };
        let log = self.log.get().and_then(Weak::upgrade);
        // The unit takes the log's hold and lets it go as it returns, before the door ends: a
        // drain that reads zero meets no unit holding the log. Its last holder may be this thread;
        // a log owns no executor and no thread, so its drop is one this thread may run.
        let owed = log.is_some_and(|log| log.unit());
        match self.door.end(owed) {
            End::Quiet => {
                #[cfg(test)]
                self.pause_at_end_gap(&requests);
                requests.uncount_one();
            }
            End::Again => {
                // Taken by this thread's next look, or by the idle thread it woke.
                if let Some(k) = self.bits.set(LOG) {
                    self.unpark(k);
                }
            }
        }
        #[cfg(test)]
        self.note_unit(Some(&requests));
        drop(requests);
    }

    /// Submits the follow-on in record `i` through its file, counted already, with Counted's
    /// observer attached; or, its file or its requests gone, or its submission refused, ends the
    /// chain with that error and lets its count go.
    fn serve(self: &Arc<Self>, i: usize) {
        #[cfg(test)]
        if let Some(hold) = self.hold.lock().take() {
            let _ = hold.recv();
        }
        let record = &self.records[i];
        let claimed = record.claim();
        assert!(claimed, "a bit set for a record not READY");
        // SAFETY: claimed.
        let ready =
            unsafe { record.with(|link| link.as_mut().expect("a chain's record").ready.take()) }
                .expect("a READY record's follow-on");
        let requests = self.requests.upgrade();
        let failed = match (&requests, ready.file.upgrade()) {
            (Some(requests), Some(file)) => {
                let me = self.clone();
                let c = ready.completion.clone();
                match requests.submit_counted(
                    || file.file.sync_data(c),
                    move || me.end(i),
                    ready.completion,
                ) {
                    Ok(()) => None,
                    Err(e) => Some(super::io_failure(&e)),
                }
            }
            (_, _) => Some(CompletionError::Aborted),
        };
        if let Some(e) = failed {
            // SAFETY: still claimed: the request was never accepted.
            unsafe {
                record.with(|link| {
                    let link = link.as_mut().expect("a chain's record");
                    if link.outcome.is_ok() {
                        link.outcome = Err(e);
                    }
                })
            };
            self.end(i);
            if let Some(requests) = requests {
                requests.uncount_one();
            }
        }
    }

    /// IO thread `k`'s loop: a take of one subtree at a time, then, with none set, a park until a
    /// producer or the stop takes its idle bit and unparks it; once stopped and every bit served,
    /// its end. Given a pumped `backend` (a database opened async) it owes a step after every
    /// record it serves and after every step that delivered, and parks only once a step has found
    /// the backend holding nothing (ASYNC1 D2d §2.3): every request it submits is submitted serving
    /// a record, so none is left in the backend with no one to step it.
    #[turso_macros::atoll_entry]
    fn run(self: &Arc<Self>, k: usize, backend: Option<Arc<dyn IO>>) {
        let place = crate::io::ReadyPlace::default();
        let mut pump = backend.map(|io| Pump {
            io,
            place: &place,
            waker: pump_waker(Arc::downgrade(self)),
            owed: false,
            registered: false,
        });
        let mut from = 0;
        loop {
            #[cfg(test)]
            {
                let hold = self.take_holds.lock().pop();
                if let Some(hold) = hold {
                    let _ = hold.recv();
                }
            }
            let empty = match self.bits.take(&mut from) {
                Taken::Record { record, wake } => {
                    // Woken first: the bits put back are served beside this one, not after it.
                    if let Some(k) = wake {
                        self.unpark(k);
                    }
                    self.serve_bit(record);
                    #[cfg(test)]
                    self.note_turn(Turn::Served);
                    if let Some(pump) = &mut pump {
                        pump.owed = true;
                    }
                    false
                }
                // Not idle: this thread looks again, at once.
                Taken::Retry { wake } => {
                    if let Some(k) = wake {
                        self.unpark(k);
                    }
                    continue;
                }
                Taken::Stopped => break,
                Taken::Empty => true,
            };
            // One step per pass while one is owed, so a step comes between any two records served
            // and a record between any two steps.
            if let Some(pump) = pump.as_mut().filter(|pump| pump.owed) {
                pump.step();
                #[cfg(test)]
                self.note_turn(Turn::Stepped);
                continue;
            }
            if !empty {
                continue;
            }
            #[cfg(all(test, not(any(shuttle, atoll_loom))))]
            self.pause_at_before_idle(pump.as_mut());
            // Idle only on a root with no child bit and no stop: one set since the take fails it,
            // and one set after it takes this thread's idle bit and unparks it.
            if self.bits.idle(k) {
                #[cfg(test)]
                let note = self.notes.lock().idle.take();
                #[cfg(test)]
                if let Some(note) = note {
                    let _ = note.send(());
                }
                // An unpark that leaves the idle bit set was not a waker's: park again.
                while self.bits.still_idle(k) {
                    crate::storage::atoll::park::thread();
                }
            }
        }
        if let Some(pump) = &mut pump {
            pump.withdraw();
        }
        #[cfg(test)]
        {
            self.exited.store(true, Order::Release);
            let note = self.notes.lock().exit.take();
            if let Some(note) = note {
                let _ = note.send(());
            }
        }
    }

    /// Starts IO thread `k` for `dispatch`: marked a database's IO thread for its life, so it waits
    /// on the file system for what is queued to it and on nothing it would deliver (a close, an
    /// executor's wait, a request), and its handle set before its first look at the bits. Given the
    /// database's `backend` when it is pumped and the database was opened async, which the thread
    /// steps.
    pub(super) fn spawn(
        dispatch: &Arc<Self>,
        k: usize,
        backend: Option<Arc<dyn IO>>,
    ) -> Result<crate::thread::JoinHandle<()>> {
        #[cfg(test)]
        if FAIL_SPAWN.with(|f| f.get() == Some(k)) {
            FAIL_SPAWN.with(|f| f.set(None));
            return Err(crate::LimboError::InternalError(
                "a database's IO thread did not start: a failure injected".to_string(),
            ));
        }
        let mine = dispatch.clone();
        crate::thread::Builder::new()
            .name("atoll-io".to_string())
            .spawn(move || {
                crate::io::serve_on_this_thread_for_life();
                let set = mine.threads[k].set(crate::thread::current());
                debug_assert!(set.is_ok(), "an IO thread started twice");
                mine.run(k, backend);
            })
            .map_err(|e| {
                crate::LimboError::InternalError(format!(
                    "a database's IO thread did not start: {e}"
                ))
            })
    }

    /// The IO threads this dispatch runs.
    pub(super) fn io_threads(&self) -> usize {
        self.shape.io_threads
    }

    /// Whether a request made on this thread is queued (D2a §1): the database was opened async and
    /// the thread is inside a cooperative frame.
    pub(super) fn queues(&self) -> bool {
        self.shape.opened_async && crate::storage::atoll::cooperative::inside()
    }

    /// Whether the database was opened async.
    pub(super) fn opened_async(&self) -> bool {
        self.shape.opened_async
    }

    /// The metadata reads' records, by their owner's index: one per read credit.
    pub(super) fn meta_reads(&self) -> usize {
        self.shape.meta_reads
    }

    /// Queues the read `c` under `credit`, in the metadata read record of its index (D2a §3), all before this returns: the record
    /// taken; the request counted; accepted, so Counted owns it from here, a wait for its retirement
    /// waits for its IO thread and an observer registered on it runs at that retirement; its payload
    /// written; its bit set. No backend ever sees `c`: its IO thread hands the backend a request of
    /// its own that carries it ([`serve_queued`](Self::serve_queued)), so the owner's cancel and the
    /// backend's refusal never meet on one completion. A record not at rest is a broken admission
    /// bound, and a sealed count a closed database's: each an error, neither a wait.
    pub(super) fn queue_read(
        self: &Arc<Self>,
        requests: &Arc<Requests>,
        credit: crate::storage::atoll::meta::Credit,
        source: Source,
        pos: u64,
        c: Completion,
    ) -> Result<()> {
        let j = credit.index();
        let record = self.queued.get(j).ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "metadata read record {j} of {}: more credits than records",
                self.queued.len()
            ))
        })?;
        if !record.take() {
            return Err(crate::LimboError::InternalError(format!(
                "metadata read record {j} not at rest: a credit taken twice"
            )));
        }
        if !requests.count_one() {
            // SAFETY: just taken, and never published.
            drop(unsafe { record.finish() });
            return Err(crate::LimboError::InternalError(
                "a request on a file of a closed database".to_string(),
            ));
        }
        c.accept();
        // SAFETY: just taken.
        unsafe {
            record.with(|p| {
                *p = Some(Queued {
                    source,
                    pos,
                    completion: c,
                    _credit: credit,
                })
            })
        };
        crate::race::point("requests.queued_writing");
        if let Some(k) = record.publish(&self.bits, QUEUED + j) {
            self.unpark(k);
        }
        Ok(())
    }

    /// Serves queued record `j`: a request cancelled before its take ended with no syscall; one
    /// whose file cannot be had, or whose backend refuses it, finished with that error and ended;
    /// any other carried to its backend by a read of the IO thread's own, which reads into the
    /// owner's region, finishes the owner's request from its callback and ends it at its own
    /// retirement. The owner may cancel its request at any point: that finishes it at once, and the
    /// carrier still runs to its retirement, which ends the request and lets its record go.
    fn serve_queued(self: &Arc<Self>, j: usize) {
        #[cfg(test)]
        if let Some(hold) = self.hold.lock().take() {
            let _ = hold.recv();
        }
        let record = &self.queued[j];
        let claimed = record.claim();
        assert!(claimed, "a bit set for a queued record not READY");
        // SAFETY: claimed. The payload stays in the record until the request retires.
        let (source, pos, c) = unsafe {
            record.with(|p| {
                let q = p.as_ref().expect("a READY record's request");
                let source = match &q.source {
                    Source::Open(f) => Source::Open(f.clone()),
                    Source::Stage(s, key) => Source::Stage(s.clone(), *key),
                };
                (source, q.pos, q.completion.clone())
            })
        };
        // The request's strong hold on its requests from its take to its release (#4831): its
        // count keeps a close from sealing, and this keeps the requests themselves, and what they
        // hold for the embedder, alive while a backend still owns its carrier or a cancel's
        // callback on it still runs. Gone, the requests are dropping, and their count with them.
        let requests = self.requests.upgrade();
        // Cancelled before its take: only a cancel finishes a request no backend was handed.
        if c.finished() {
            crate::race::point("requests.queued_cancelled");
            return self.end_queued(j, &c, requests);
        }
        let file = match source {
            Source::Open(f) => f.upgrade().ok_or_else(|| {
                crate::LimboError::InternalError("a queued request's file dropped".to_string())
            }),
            Source::Stage(stage, key) => stage
                .upgrade()
                .ok_or_else(|| {
                    crate::LimboError::InternalError("a queued request's owner dropped".to_string())
                })
                .and_then(|stage| stage.open_for(key)),
        };
        let file = match file {
            Ok(file) => file,
            Err(e) => {
                c.error(io_failure(&e));
                return self.end_queued(j, &c, requests);
            }
        };
        let owner = c.clone();
        let carrier = Completion::new_read(c.as_read().buf_arc(), move |res| {
            match res {
                Ok((_, n)) => owner.complete(n),
                Err(e) => owner.error(e),
            }
            None
        });
        crate::race::point("requests.queued_submitting");
        match file.file.pread(pos, carrier.clone()) {
            Ok(_) => {
                let me = self.clone();
                carrier.on_retired(move || me.end_queued(j, &c, requests));
            }
            // Refused: the backend took nothing, and the carrier's callback never ran.
            Err(e) => {
                c.error(io_failure(&e));
                self.end_queued(j, &c, requests);
            }
        }
    }

    /// Ends the queued request `c` in record `j`, finished (its carrier's callback ran, or an error
    /// or a cancel finished it) and taken by no backend any more: `c` retired, then its record, its
    /// owner's credit and its count let go once `c` has wholly retired. A cancel's callback may
    /// still be running on another thread, in which case `c`'s retirement waits for it to return
    /// (`Completion::retire` marks the end, and that callback's return retires it), and the release
    /// waits with it: it is an observer of the retirement, run at once and allocating nothing when
    /// `c` has already retired, which is the case unless such a callback is in flight. Nothing waits
    /// for the cancelling thread.
    fn end_queued(self: &Arc<Self>, j: usize, c: &Completion, requests: Option<Arc<Requests>>) {
        c.retire();
        let me = self.clone();
        c.on_retired(move || me.release_queued(j, requests));
    }

    /// The queued request in record `j` has wholly retired: the record let go, then its payload
    /// dropped, its credit with it and so given back; its count let go, last, and the request's
    /// hold on its requests with it, which may be their last.
    fn release_queued(&self, j: usize, requests: Option<Arc<Requests>>) {
        // SAFETY: the request's retirement, the record's last holder.
        let payload = unsafe { self.queued[j].finish() };
        crate::race::point("requests.queued_rest");
        drop(payload);
        if let Some(requests) = requests {
            requests.uncount_one();
        }
    }

    /// The frame records' owner, the pool, set once at its open before any frame can queue.
    pub(super) fn attach_frames_owner(&self, owner: Weak<dyn FrameOwner>) {
        let set = self.frames_owner.set(owner);
        assert!(set.is_ok(), "a second pool on one database's dispatch");
    }

    /// The frame records: one per grain of the pool the database was opened with.
    pub(super) fn frame_records(&self) -> usize {
        self.shape.frames
    }

    /// Queues the pool's request `c` for `place` in the record of `held`'s frame (D2b §1, §3, §4),
    /// all before this returns, as [`queue_read`](Self::queue_read) does: the record taken; the
    /// request counted; accepted; its payload written; its bit set. The held record is the
    /// admission, so its frame's dispatch record is at rest whenever the frame's own record is
    /// free (§5): one not at rest is a broken end order, and a sealed count a closed database's,
    /// each refused before the request is accepted, with the held record back for its owner's one
    /// give-back.
    pub(super) fn queue_frame(
        self: &Arc<Self>,
        requests: &Arc<Requests>,
        held: HeldRecord,
        place: Place,
        c: Completion,
    ) -> std::result::Result<(), FrameRefused> {
        let j = held.frame() as usize;
        crate::turso_assert!(
            held.class() == place.class(),
            "a frame request queued at a place of another class than its frame's"
        );
        let Some(record) = self.frames.get(j) else {
            let error = crate::LimboError::InternalError(format!(
                "frame record {j} of {}: a frame past the pool the database was opened with",
                self.frames.len()
            ));
            return Err(FrameRefused { held, error });
        };
        if !record.take() {
            let error = crate::LimboError::InternalError(format!(
                "frame record {j} not at rest while its frame's record was free"
            ));
            return Err(FrameRefused { held, error });
        }
        if !requests.count_one() {
            // SAFETY: just taken, and never published.
            drop(unsafe { record.finish() });
            let error = crate::LimboError::InternalError(
                "a request on a file of a closed database".to_string(),
            );
            return Err(FrameRefused { held, error });
        }
        c.accept();
        #[cfg(test)]
        super::LAST_FRAME_REQUEST.with(|r| *r.borrow_mut() = Some(c.clone()));
        // SAFETY: just taken.
        unsafe {
            record.with(|p| {
                *p = Some(FrameQueued {
                    place,
                    completion: c,
                })
            })
        };
        // The dispatch's release lets the frame's record go from here (`release_frame`).
        held.queued();
        crate::race::point("requests.frame_writing");
        if let Some(k) = record.publish(&self.bits, self.frames_bit() + j) {
            self.unpark(k);
        }
        Ok(())
    }

    /// Serves frame record `j` as [`serve_queued`](Self::serve_queued) serves a metadata read's
    /// (D2b §5): a request cancelled before its take ended with no syscall; one whose owner has
    /// gone, whose file cannot be had or whose backend refuses it finished with that error and
    /// ended; any other carried to its backend by a request of the IO thread's own, a read into the
    /// pool's region or a write out of the frame's memory, which finishes the pool's request from
    /// its callback and ends it at its own retirement. The owner is held only for the stage and the
    /// region, never across the submission.
    fn serve_frame(self: &Arc<Self>, j: usize) {
        #[cfg(test)]
        if let Some(hold) = self.hold.lock().take() {
            let _ = hold.recv();
        }
        let record = &self.frames[j];
        let claimed = record.claim();
        assert!(claimed, "a bit set for a frame record not READY");
        // SAFETY: claimed. The payload stays in the record until the request retires.
        let (place, c) = unsafe {
            record.with(|p| {
                let q = p.as_ref().expect("a READY frame record's request");
                (q.place, q.completion.clone())
            })
        };
        // The request's strong hold on its requests from its take to its release, as a metadata
        // read's (#4831).
        let requests = self.requests.upgrade();
        if c.finished() {
            crate::race::point("requests.frame_cancelled");
            return self.end_frame(j, &c, requests);
        }
        let staged = match self.frames_owner.get().and_then(Weak::upgrade) {
            None => Err(crate::LimboError::InternalError(
                "a queued frame request's pool dropped".to_string(),
            )),
            Some(owner) => owner.file_for(place).map(|(file, at)| {
                let region = place.writes().then(|| owner.region(j as u32, place));
                (file, at, region)
            }),
        };
        let (file, at, region) = match staged {
            Ok(staged) => staged,
            Err(e) => {
                c.error(io_failure(&e));
                return self.end_frame(j, &c, requests);
            }
        };
        let owner = c.clone();
        crate::race::point("requests.frame_submitting");
        let (carrier, submitted) = match region {
            None => {
                let carrier = Completion::new_read(c.as_read().buf_arc(), move |res| {
                    match res {
                        Ok((_, n)) => owner.complete(n),
                        Err(e) => owner.error(e),
                    }
                    None
                });
                let submitted = file.file.pread(at, carrier.clone());
                (carrier, submitted)
            }
            Some(region) => {
                let carrier = Completion::new_write(move |res| match res {
                    Ok(n) => owner.complete(n),
                    Err(e) => owner.error(e),
                });
                let submitted = file.file.pwrite(at, region, carrier.clone());
                (carrier, submitted)
            }
        };
        #[cfg(all(test, not(any(shuttle, atoll_loom))))]
        self.pause_at_frame_returned(j);
        match submitted {
            Ok(_) => {
                let me = self.clone();
                carrier.on_retired(move || me.end_frame(j, &c, requests));
            }
            // Refused: the backend took nothing, and the carrier's callback never ran.
            Err(e) => {
                c.error(io_failure(&e));
                self.end_frame(j, &c, requests);
            }
        }
        #[cfg(all(test, not(any(shuttle, atoll_loom))))]
        self.note_frame_served(j);
    }

    /// Ends the queued frame request `c` in record `j` as [`end_queued`](Self::end_queued) ends a
    /// metadata read's: `c` retired, then the release once it has wholly retired.
    fn end_frame(self: &Arc<Self>, j: usize, c: &Completion, requests: Option<Arc<Requests>>) {
        c.retire();
        let me = self.clone();
        c.on_retired(move || me.release_frame(j, requests));
    }

    /// The queued frame request in record `j` has wholly retired (D2b §5): the record let go, its
    /// payload dropped, then the frame's own record let go by its owner, so the frame's record is
    /// never free while its dispatch record is held; the count let go, and the requests' hold with
    /// it, last. With its owner gone there is no frame to let go.
    fn release_frame(&self, j: usize, requests: Option<Arc<Requests>>) {
        // SAFETY: the request's retirement, the record's last holder.
        let payload = unsafe { self.frames[j].finish() }.expect("a queued frame request");
        crate::race::point("requests.frame_rest");
        let class = payload.place.class();
        drop(payload);
        if let Some(owner) = self.frames_owner.get().and_then(Weak::upgrade) {
            owner.record_retired(j as u32, class);
        }
        crate::race::point("requests.frame_retired");
        if let Some(requests) = requests {
            requests.uncount_one();
        }
    }

    /// Queues the read-back read `c` at `pos` of the log segment `file` in transaction slot
    /// `slot`'s record (D2c §2), all before this returns, as [`queue_read`](Self::queue_read)
    /// does: the record taken; the request counted; accepted; its payload written; its bit set.
    /// The record is the admission, never the completion's retirement, which comes before the
    /// record is at rest. A record still held is no error: the take registers on the slot's rest
    /// wait once and tries once more, and taken neither time returns that wait, which the release
    /// finishes once the record is at rest; a wait left registered is finished by the next
    /// let-go, for no one. A slot past the records, or a sealed count, a closed database's, is an
    /// error, neither a wait.
    pub(super) fn queue_slot_read(
        self: &Arc<Self>,
        requests: &Arc<Requests>,
        slot: usize,
        file: Arc<CountedFile>,
        pos: u64,
        c: Completion,
    ) -> Result<crate::types::IOResult<()>> {
        let (Some(record), Some(rest)) = (self.slot_reads.get(slot), self.slot_rest.get(slot))
        else {
            return Err(crate::LimboError::InternalError(format!(
                "transaction slot {slot}'s read-back record of {}: a slot past the database's",
                self.slot_reads.len()
            )));
        };
        if !record.take() {
            #[cfg(all(test, not(any(shuttle, atoll_loom))))]
            self.pause_at_slot(SlotPause::Registering, slot);
            let wait = rest.wait();
            if !record.take() {
                return Ok(crate::types::IOResult::IO(crate::types::IOCompletions(
                    wait,
                )));
            }
        }
        if !requests.count_one() {
            // SAFETY: just taken, and never published.
            drop(unsafe { record.finish() });
            rest.let_go();
            return Err(crate::LimboError::InternalError(
                "a request on a file of a closed database".to_string(),
            ));
        }
        c.accept();
        // SAFETY: just taken.
        unsafe {
            record.with(|p| {
                *p = Some(SlotQueued {
                    file,
                    pos,
                    completion: c,
                })
            })
        };
        crate::race::point("requests.slot_writing");
        if let Some(k) = record.publish(&self.bits, self.slots_bit() + slot) {
            self.unpark(k);
        }
        Ok(crate::types::IOResult::Done(()))
    }

    /// Serves slot record `j` as [`serve_queued`](Self::serve_queued) serves a metadata read's
    /// (D2c §2): a read cancelled before its take ended with no syscall; one whose backend refuses
    /// it finished with that error and ended; any other carried to its backend by a read of the IO
    /// thread's own into the read-back's region, which finishes the read-back's request from its
    /// callback and ends it at its own retirement.
    fn serve_slot_read(self: &Arc<Self>, j: usize) {
        #[cfg(test)]
        if let Some(hold) = self.hold.lock().take() {
            let _ = hold.recv();
        }
        let record = &self.slot_reads[j];
        let claimed = record.claim();
        assert!(claimed, "a bit set for a slot record not READY");
        // SAFETY: claimed. The payload, the file with it, stays in the record until the request
        // retires.
        let (file, pos, c) = unsafe {
            record.with(|p| {
                let q = p.as_ref().expect("a READY slot record's request");
                (q.file.clone(), q.pos, q.completion.clone())
            })
        };
        // The request's strong hold on its requests from its take to its release, as a metadata
        // read's (#4831).
        let requests = self.requests.upgrade();
        if c.finished() {
            crate::race::point("requests.slot_cancelled");
            return self.end_slot_read(j, &c, requests);
        }
        let owner = c.clone();
        let carrier = Completion::new_read(c.as_read().buf_arc(), move |res| {
            match res {
                Ok((_, n)) => owner.complete(n),
                Err(e) => owner.error(e),
            }
            None
        });
        crate::race::point("requests.slot_submitting");
        match file.file.pread(pos, carrier.clone()) {
            Ok(_) => {
                let me = self.clone();
                carrier.on_retired(move || me.end_slot_read(j, &c, requests));
            }
            // Refused: the backend took nothing, and the carrier's callback never ran.
            Err(e) => {
                c.error(io_failure(&e));
                self.end_slot_read(j, &c, requests);
            }
        }
    }

    /// Ends the queued read-back read `c` in slot record `j` as [`end_queued`](Self::end_queued)
    /// ends a metadata read's: `c` retired, then the release once it has wholly retired.
    fn end_slot_read(self: &Arc<Self>, j: usize, c: &Completion, requests: Option<Arc<Requests>>) {
        c.retire();
        let me = self.clone();
        c.on_retired(move || me.release_slot_read(j, requests));
    }

    /// The read-back read in slot record `j` has wholly retired (D2c §2): the record at rest
    /// first, `Release`, and only then its rest wait let go, so a taker the let-go finishes finds
    /// it at rest, and one that registers after the let-go sees the rest at its second attempt;
    /// the payload, the segment's file with it, dropped; the count let go, last, and the request's
    /// hold on its requests with it, which may be their last.
    fn release_slot_read(&self, j: usize, requests: Option<Arc<Requests>>) {
        #[cfg(all(test, not(any(shuttle, atoll_loom))))]
        self.pause_at_slot(SlotPause::BeforeRest, j);
        // SAFETY: the request's retirement, the record's last holder.
        let payload = unsafe { self.slot_reads[j].finish() };
        crate::race::point("requests.slot_rest");
        #[cfg(all(test, not(any(shuttle, atoll_loom))))]
        self.pause_at_slot(SlotPause::BeforeLetGo, j);
        self.slot_rest[j].let_go();
        drop(payload);
        if let Some(requests) = requests {
            requests.uncount_one();
        }
    }

    /// The log units started so far.
    #[cfg(test)]
    pub(super) fn units(&self) -> usize {
        self.units.load(Order::Relaxed)
    }

    /// The log units wholly ended so far, each one's count let go.
    #[cfg(test)]
    pub(super) fn units_ended(&self) -> usize {
        self.ended.load(Order::Acquire)
    }

    /// The log door's bits.
    #[cfg(test)]
    pub(super) fn door(&self) -> u8 {
        self.door.read()
    }

    /// A note of each log unit's end, from the next on: the door then and the requests counted.
    #[cfg(test)]
    pub(super) fn on_unit_end(&self) -> std::sync::mpsc::Receiver<(u8, usize)> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.notes.lock().unit = Some(tx);
        rx
    }

    /// Pauses the next quiet unit's end between its door going quiet and its count's release: the
    /// door and the count there, and the sender that lets it go on.
    #[cfg(test)]
    pub(super) fn pause_end_gap(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<(u8, usize)>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        self.notes.lock().end_gap = Some((seen_tx, go_rx));
        (seen_rx, go_tx)
    }

    #[cfg(test)]
    fn note_unit(&self, requests: Option<&Requests>) {
        // After the end and its count's release: a reader that sees it sees the release.
        self.ended.fetch_add(1, Order::Release);
        let note = self.notes.lock().unit.clone();
        if let Some(note) = note {
            let counted = requests.map_or(0, Requests::outstanding);
            let _ = note.send((self.door.read(), counted));
        }
    }

    /// Pauses the next queued frame request between its backend call's return and the branch
    /// after it: its record there, and the sender that lets it go on.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn pause_frame_returned(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<usize>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        self.notes.lock().frame_returned = Some((seen_tx, go_rx));
        (seen_rx, go_tx)
    }

    /// A note of the next queued frame request's branch after its backend call, once run, whichever
    /// arm it took.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn on_frame_served(&self) -> std::sync::mpsc::Receiver<usize> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.notes.lock().frame_served = Some(tx);
        rx
    }

    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    fn pause_at_frame_returned(&self, j: usize) {
        let gap = self.notes.lock().frame_returned.take();
        if let Some((seen, go)) = gap {
            let _ = seen.send(j);
            let _ = go.recv();
        }
    }

    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    fn note_frame_served(&self, j: usize) {
        let note = self.notes.lock().frame_served.take();
        if let Some(note) = note {
            let _ = note.send(j);
        }
    }

    /// Pauses the next slot record take or release to reach `at`: the record there, and the
    /// sender that lets it go on.
    #[cfg(all(test, not(any(shuttle, atoll_loom, miri))))]
    pub(super) fn pause_slot(
        &self,
        at: SlotPause,
    ) -> (
        std::sync::mpsc::Receiver<usize>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        self.notes.lock().slot = Some((at, seen_tx, go_rx));
        (seen_rx, go_tx)
    }

    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    fn pause_at_slot(&self, at: SlotPause, j: usize) {
        let pause = {
            let mut notes = self.notes.lock();
            match &notes.slot {
                Some((armed, _, _)) if *armed == at => notes.slot.take(),
                _ => None,
            }
        };
        if let Some((_, seen, go)) = pause {
            let _ = seen.send(j);
            let _ = go.recv();
        }
    }

    #[cfg(test)]
    fn pause_at_end_gap(&self, requests: &Requests) {
        let gap = self.notes.lock().end_gap.take();
        if let Some((seen, go)) = gap {
            let _ = seen.send((self.door.read(), requests.outstanding()));
            let _ = go.recv();
        }
    }

    /// Holds the next IO thread to look at the bits before its take, until the returned sender
    /// sends or drops; armed while the threads are idle, each hold stops one thread woken after.
    #[cfg(test)]
    pub(super) fn hold_next_take(&self) -> std::sync::mpsc::Sender<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.take_holds.lock().push(rx);
        tx
    }

    /// Holds the IO thread before its next claim, until the returned sender sends or drops.
    #[cfg(test)]
    pub(super) fn hold_next_claim(&self) -> std::sync::mpsc::Sender<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        *self.hold.lock() = Some(rx);
        tx
    }

    /// A note of the thread's next idle, sent once it has marked itself idle, past its last look at
    /// the word, before it parks.
    #[cfg(test)]
    pub(super) fn on_idle(&self) -> std::sync::mpsc::Receiver<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.notes.lock().idle = Some(tx);
        rx
    }

    /// A note of the thread's end.
    #[cfg(test)]
    pub(super) fn on_exit(&self) -> std::sync::mpsc::Receiver<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.notes.lock().exit = Some(tx);
        rx
    }

    /// Pauses the IO thread the next time its take finds no bit and it owes no step, before it
    /// marks itself idle, having withdrawn its registration with a pumped backend first if
    /// `withdrawn`: a note once it is there, and the sender whose send or drop lets it go.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn pause_before_idle(
        &self,
        withdrawn: bool,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (at, there) = std::sync::mpsc::channel();
        let (release, held) = std::sync::mpsc::channel();
        self.notes.lock().before_idle = Some((withdrawn, at, held));
        (there, release)
    }

    /// The IO thread at its pause before idle, if a test set one: its registration withdrawn if
    /// asked, then it waits there for the test.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    fn pause_at_before_idle(&self, pump: Option<&mut Pump<'_>>) {
        let pause = self.notes.lock().before_idle.take();
        if let Some((withdrawn, at, held)) = pause {
            if let Some(pump) = pump.filter(|_| withdrawn) {
                pump.withdraw();
            }
            let _ = at.send(());
            let _ = held.recv();
        }
    }

    /// Sets the pump's bit, as the backend's wake does: the IO thread then owes a step.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn wake_pump(&self) {
        if let Some(k) = self.bits.set(PUMP) {
            self.unpark(k);
        }
    }

    /// Starts recording the IO thread's turns, from none.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn record_turns(&self) {
        self.notes.lock().turns = Some(Vec::new());
    }

    /// The turns recorded since [`record_turns`](Self::record_turns), which goes on.
    #[cfg(all(test, not(any(shuttle, atoll_loom))))]
    pub(super) fn turns(&self) -> Vec<Turn> {
        self.notes.lock().turns.clone().unwrap_or_default()
    }

    #[cfg(test)]
    fn note_turn(&self, turn: Turn) {
        if let Some(turns) = &mut self.notes.lock().turns {
            turns.push(turn);
        }
    }

    /// The records at rest, by count.
    #[cfg(test)]
    pub(super) fn at_rest(&self) -> usize {
        self.records.iter().filter(|r| r.at_rest()).count()
    }

    /// Whether a request made on this thread is queued, for a test of the rule's four cases.
    #[cfg(test)]
    pub(super) fn queues_here(&self) -> bool {
        self.queues()
    }

    /// The queued records at rest, by count.
    #[cfg(test)]
    pub(super) fn queued_at_rest(&self) -> usize {
        self.queued.iter().filter(|r| r.at_rest()).count()
    }

    /// The frame records at rest, by count.
    #[cfg(test)]
    pub(super) fn frames_at_rest(&self) -> usize {
        self.frames.iter().filter(|r| r.at_rest()).count()
    }

    /// The slot records at rest, by count.
    #[cfg(all(test, not(any(shuttle, atoll_loom, miri))))]
    pub(super) fn slots_at_rest(&self) -> usize {
        self.slot_reads.iter().filter(|r| r.at_rest()).count()
    }

    /// Let-goes of slot `j`'s rest wait that found a taker waiting.
    #[cfg(all(test, not(any(shuttle, atoll_loom, miri))))]
    pub(super) fn slot_rest_woke(&self, j: usize) -> u64 {
        self.slot_rest[j].woke()
    }
}

#[cfg(test)]
crate::thread::thread_local! {
    /// Set by a test: IO thread `k` of this thread's next dispatch does not start.
    pub(super) static FAIL_SPAWN: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

fn closed() -> CompletionError {
    CompletionError::Aborted
}

/// What an IO thread over a pumped backend keeps for its life (ASYNC1 D2d §2.3): the backend, the
/// place its registration there is kept under, borrowed so it cannot move while the thread runs,
/// and the waker it registers.
struct Pump<'a> {
    io: Arc<dyn IO>,
    place: &'a crate::io::ReadyPlace,
    waker: std::task::Waker,
    /// A record was served, the backend woke the thread, or the last step delivered: the thread
    /// steps before it may park.
    owed: bool,
    /// A step found the backend holding nothing and registered the waker, which the backend may
    /// still hold: a submission takes it as it wakes it.
    registered: bool,
}

impl Pump<'_> {
    /// One step, made as a blocking host (the IO thread serves): owed again after a delivery,
    /// which may leave more to deliver; owed nothing, and registered, once the backend holds
    /// nothing. A step that fails has lost the means to retire what the backend holds, and ends
    /// the process (DESIGN.md 22.11).
    fn step(&mut self) {
        match self.io.try_step(self.place.key(), &self.waker) {
            Ok(Stepped::Delivered) => {}
            Ok(Stepped::Empty) => {
                self.owed = false;
                self.registered = true;
            }
            // Refused at open (`refuse_a_backend_that_would_poll`).
            Ok(Stepped::Unsaid) => {
                unreachable!("a backend that says what its steps find answered Unsaid")
            }
            Err(_) => crate::io::stepping_failed("a database's IO thread"),
        }
    }

    /// Its registration taken back, if the backend may still hold it: at the thread's end, before
    /// its place goes.
    fn withdraw(&mut self) {
        if self.registered {
            self.io.withdraw(self.place.key());
            self.registered = false;
        }
    }
}

/// The waker an IO thread registers with its pumped backend: a wake sets [`PUMP`] as any
/// producer's set does, so a wake between the thread's look and its park fails its `idle`, and one
/// after takes its idle bit and unparks it.
fn pump_waker(dispatch: Weak<Dispatch>) -> std::task::Waker {
    struct Wake(Weak<Dispatch>);
    impl std::task::Wake for Wake {
        fn wake(self: std::sync::Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            if let Some(dispatch) = self.0.upgrade() {
                if let Some(k) = dispatch.bits.set(PUMP) {
                    dispatch.unpark(k);
                }
            }
        }
    }
    std::task::Waker::from(std::sync::Arc::new(Wake(dispatch)))
}

#[cfg(all(test, atoll_loom))]
mod loom_tests;
