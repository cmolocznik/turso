//! Epochs: when an identity may be reused (CONCURRENCY.md §4).
//!
//! Read-copy update's grace periods, made lock-free: no thread ever waits for another to enter
//! or leave a step, scan, retire, reclaim or register a wait.
//!
//! A thread entering a step, one call into Atoll that returns `Done` or `IO`, announces the global
//! epoch; it clears the announcement when the step ends. A freed object, whether a page number, a
//! temporary page or a frame's memory, is retired with a stamp: the epoch the retiring advances
//! the global epoch to. It may be reused once every thread is between steps or announced the stamp
//! or above. A thread that announced below the stamp entered its step before the retiring and may
//! still hold the object; one that announced the stamp or above entered after the object was
//! unlinked, and cannot reach it.
//!
//! Whatever needs an object not yet reusable registers a wait ([`Epochs::wait_for`]): it flags the
//! slot of every step holding it back, and the flagged step's end, one swap of its own slot,
//! finishes it.
//!
//! Steps never block, so a thread holds back reuse by at most one step. Between steps a thread holds
//! names with versions, never references, and checks them before use.
//!
//! Steps nest: a step that calls into another part of Atoll counts the inner step, and only the
//! outermost one announces. Threads run one step at a time, so the announcement is per thread.

use self::sync::{
    fence, Arc, AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Mutex, Ordering, Plain,
};
use crate::io::Completion;
use crossbeam_utils::CachePadded;
use std::cell::{Cell, RefCell};
use std::collections::BinaryHeap;
use std::ptr;

/// The engine's primitives, or loom's, which model the weaker memory orderings shuttle cannot.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::storage::atoll::locks::Mutex;
    pub(super) use crate::sync::atomic::{
        fence, AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering,
    };
    pub(super) use crate::sync::Arc;

    /// A plain field one thread writes and publishes to others; under loom, loom's checked cell.
    pub(super) struct Plain<T>(std::cell::UnsafeCell<T>);

    impl<T> Plain<T> {
        pub(super) fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
        }

        pub(super) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }
    }
}

#[cfg(atoll_loom)]
mod sync {
    pub(super) use loom::sync::atomic::{
        fence, AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering,
    };
    pub(super) use loom::sync::Arc;

    /// loom fails any read of it not ordered after its write.
    pub(super) use loom::cell::UnsafeCell as Plain;

    /// loom's mutex behind the engine's interface.
    pub(super) struct Mutex<T>(loom::sync::Mutex<T>);

    impl<T> Mutex<T> {
        pub(super) fn new(value: T) -> Self {
            Self(loom::sync::Mutex::new(value))
        }

        #[cfg(test)]
        pub(super) fn lock(&self) -> loom::sync::MutexGuard<'_, T> {
            self.0.lock().unwrap()
        }

        pub(super) fn try_lock(&self) -> Option<loom::sync::MutexGuard<'_, T>> {
            self.0.try_lock().ok()
        }
    }
}

/// Whether a loom model that failed is being torn down while its threads unwind: any of its
/// atomics touched then panics again, which aborts the test run, so drops leave them alone.
fn torn_down() -> bool {
    cfg!(atoll_loom) && std::thread::panicking()
}

/// A node's `next`, as its pusher wrote it: the head it read, which may name a node freed since
/// whose address a newer node took before the push's exchange succeeded, the exchange comparing
/// addresses alone. Every node's provenance is exposed as it is pushed, so the pointer is taken
/// again from its address, and names the node there now.
fn linked<T>(next: *mut T) -> *mut T {
    ptr::with_exposed_provenance_mut(next.addr())
}

/// A slot's word while its thread is between steps.
const IDLE: u64 = u64::MAX;

/// In a slot's word, set by a wait the step there holds back: its end wakes the waits.
const WAITED: u64 = 1;

/// A slot's word announcing `epoch`, no wait flagged yet.
fn announcing(epoch: u64) -> u64 {
    epoch << 1
}

/// What a slot's word announces; `None` for a thread between steps.
fn announced(word: u64) -> Option<u64> {
    (word != IDLE).then_some(word >> 1)
}

/// Slots per chunk of the registry: a 4 KiB page of them. Under loom, two, so its few threads cross
/// a chunk's end.
#[cfg(not(atoll_loom))]
const CHUNK_SLOTS: usize = 4096 / std::mem::size_of::<Slot>();
#[cfg(atoll_loom)]
const CHUNK_SLOTS: usize = 2;

/// Distinguishes instances in the per-thread table. Never 0, the id of the table's empty cache.
static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Takes an instance id from `next`: each once, never 0. Taken to its end the count stays there,
/// and every later take refuses rather than wrap to 0 or to an id an instance may still hold, which
/// the per-thread table's cache would read as its own. Made once per instance, so a refused
/// increment costs no step.
fn take_id(next: &std::sync::atomic::AtomicU64) -> Option<u64> {
    use std::sync::atomic::Ordering::Relaxed;
    // Relaxed: only its uniqueness matters, which the increment's atomicity gives.
    next.fetch_update(Relaxed, Relaxed, |id| id.checked_add(1))
        .ok()
}

/// A participant's place in an instance's registry: what it announced, and whether a thread
/// holds it. Padded to a cache line of its own, or to the pair of lines x86_64 and aarch64 cores
/// fetch together, so a thread's announcements never contend with another's.
type Slot = CachePadded<SlotWords>;

struct SlotWords {
    /// [`IDLE`], or [`announcing`] an epoch, with [`WAITED`] set once a wait it holds back flags it.
    word: AtomicU64,
    taken: AtomicBool,
    /// The waits the slot's thread registered and not yet finished, newest first (DESIGN.md
    /// 9.1.2): pushed only by the slot's thread, taken whole by a wake, so a push is one
    /// compare-and-swap and, lost to a take, one swap.
    waits: AtomicPtr<Wait>,
}

/// Slots handed out in order, and the chunk after them once they are all taken.
struct Chunk {
    /// Written once, before the chunk is linked in.
    slots: Plain<Box<[Slot]>>,
    /// Dense completion announcements, separate from the padded page announcements.
    completions: Plain<Box<CachePadded<[AtomicPtr<()>; CHUNK_SLOTS]>>>,
    /// Slots handed out so far, or more: a scan reads no further.
    used: AtomicUsize,
    next: AtomicPtr<Chunk>,
}

// SAFETY: `slots` and `completions` are written only as the chunk is made, before any other thread
// can reach it (each slot's and each announcement's contents are atomics); `used` and `next` are
// atomic.
unsafe impl Sync for Chunk {}
unsafe impl Send for Chunk {}

impl Chunk {
    fn new() -> Self {
        Self {
            slots: Plain::new(
                (0..CHUNK_SLOTS)
                    .map(|_| {
                        // Born taken: the thread whose count handed it out owns it, and a
                        // thread looking for a slot given back never takes it first.
                        CachePadded::new(SlotWords {
                            word: AtomicU64::new(IDLE),
                            taken: AtomicBool::new(true),
                            waits: AtomicPtr::new(ptr::null_mut()),
                        })
                    })
                    .collect(),
            ),
            completions: Plain::new(Box::new(CachePadded::new(std::array::from_fn(|_| {
                AtomicPtr::new(ptr::null_mut())
            })))),
            used: AtomicUsize::new(0),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn slot(&self, i: usize) -> &Slot {
        &self.all()[i]
    }

    /// The slots handed out so far.
    fn used(&self) -> &[Slot] {
        &self.all()[..self.used.load(Ordering::Acquire).min(CHUNK_SLOTS)]
    }

    fn all(&self) -> &[Slot] {
        // SAFETY: written when the chunk was made, before the exchange that linked it in, which a
        // thread reaching the chunk read with acquire; never written since.
        self.slots.with(|s| unsafe { &**s })
    }

    fn completions(&self) -> &[AtomicPtr<()>] {
        // SAFETY: initialized before this chunk is published, like `slots`.
        self.completions.with(|words| unsafe { &***words })
    }
}

/// Every slot of an instance: chunks that only grow, kept until the instance and every thread
/// holding a slot in it are gone, so a slot's address stays good as long as either holds it.
struct Registry {
    first: Chunk,
    /// OR of joined one-based ordinals: at least their maximum and less than twice it.
    completion_extent: CachePadded<AtomicUsize>,
    /// Cleared when the instance is dropped, so threads drop their entries for it.
    open: AtomicBool,
}

struct Joined<'a> {
    slot: &'a Slot,
    completion: &'a AtomicPtr<()>,
}

impl Registry {
    fn chunks(&self) -> impl Iterator<Item = &Chunk> {
        std::iter::successors(Some(&self.first), |c| {
            // SAFETY: a chunk linked in is freed only with the registry, which outlives `self`.
            unsafe { c.next.load(Ordering::Acquire).as_ref() }
        })
    }

    fn slots(&self) -> impl Iterator<Item = &Slot> {
        self.chunks().flat_map(Chunk::used)
    }

    /// A slot for the calling thread: one a thread that exited gave back, or a new one. One pass
    /// over each chunk's slots handed out, a strong compare-and-swap for each given back; then one
    /// slot from the chunk's count, which the count makes this thread's; else the next chunk,
    /// linked by one compare-and-swap whose loser takes the winner's. Bounded by the chunks, at
    /// most the threads (DESIGN.md 9.1.2).
    fn join(&self) -> Joined<'_> {
        let mut chunk = &self.first;
        let mut base = 0usize;
        loop {
            let free = chunk.used().iter().enumerate().find(|(_, s)| {
                !s.taken.load(Ordering::Relaxed)
                    && s.taken
                        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
            });
            if let Some((i, _)) = free {
                return self.joined(chunk, base, i);
            }
            if chunk.used.load(Ordering::Relaxed) < CHUNK_SLOTS {
                let i = chunk.used.fetch_add(1, Ordering::AcqRel);
                if i < CHUNK_SLOTS {
                    // Born taken, and handed out by the count to this thread alone.
                    return self.joined(chunk, base, i);
                }
            }
            let mut next = chunk.next.load(Ordering::Acquire);
            if next.is_null() {
                let new = Box::into_raw(Box::new(Chunk::new()));
                next = match chunk.next.compare_exchange(
                    ptr::null_mut(),
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => new,
                    Err(linked) => {
                        // SAFETY: never linked in, so no other thread has it.
                        drop(unsafe { Box::from_raw(new) });
                        linked
                    }
                };
            }
            // SAFETY: linked in, so freed only with the registry, which outlives `self`.
            chunk = unsafe { &*next };
            base = base
                .checked_add(CHUNK_SLOTS)
                .expect("too many thread slots");
        }
    }

    fn joined<'a>(&self, chunk: &'a Chunk, base: usize, i: usize) -> Joined<'a> {
        let ordinal = base.checked_add(i + 1).expect("too many thread slots");
        assert!(ordinal <= isize::MAX as usize, "too many thread slots");
        // Even an unchanged value publishes this joiner's acquire of the chunk's link.
        // The remover captures the bound after its fence, before reading these dense words.
        self.completion_extent.fetch_or(ordinal, Ordering::SeqCst);
        Joined {
            slot: chunk.slot(i),
            completion: &chunk.completions()[i],
        }
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        let mut next = self.first.next.load(Ordering::Acquire);
        while !next.is_null() {
            // SAFETY: each chunk was linked in once, from `Box::into_raw`, and the registry is
            // its last owner.
            let chunk = unsafe { Box::from_raw(next) };
            next = chunk.next.load(Ordering::Acquire);
        }
    }
}

/// What a wait finishes once the horizon reaches its epoch.
pub trait Notify: Send + Sync + 'static {
    fn notify(&self);
    /// Finished already, by this wait or whatever else it stands for.
    fn notified(&self) -> bool;
    /// What the wait finishes, when two registrations can name one thing: waits of one identity
    /// in a list collapse to the one with the lowest epoch (DESIGN.md 9.1.3, other waits). None
    /// for a wait that stands only for itself.
    fn identity(&self) -> Option<usize> {
        None
    }
}

/// Waits taken out of the slots' lists, owned by this list and linked through their own `next`:
/// walked, kept and put back with no allocation. Dropped, it drops what it still holds.
#[derive(Default)]
struct WaitList(Option<Box<Wait>>);

impl WaitList {
    fn push(&mut self, mut wait: Box<Wait>) {
        wait.next = self.0.take().map_or(ptr::null_mut(), Box::into_raw);
        self.0 = Some(wait);
    }

    fn pop(&mut self) -> Option<Box<Wait>> {
        let mut wait = self.0.take()?;
        let next = std::mem::replace(&mut wait.next, ptr::null_mut());
        // SAFETY: `next` came from `push`'s `Box::into_raw` of a wait this list owned, and nothing
        // else names it.
        self.0 = (!next.is_null()).then(|| unsafe { Box::from_raw(next) });
        Some(wait)
    }

    /// The waits not notified already, waits of one identity collapsed to the one with the lowest
    /// epoch (DESIGN.md 9.1.3): a completion registered again behind one old step, by a caller
    /// that looked again or several on one thread, stands once, at the earliest epoch either
    /// asked for, which wakes it no later than either needed. A wait dropped here is one the
    /// survivor stands for. Quadratic in the list, which holds a slot's unfinished waits.
    fn collapsed(mut self) -> WaitList {
        let mut kept = WaitList::default();
        while let Some(wait) = self.pop() {
            if wait.notify.notified() {
                continue;
            }
            let Some(id) = wait.notify.identity() else {
                kept.push(wait);
                continue;
            };
            let mut node = kept.0.as_deref_mut();
            let mut merged = false;
            while let Some(k) = node {
                if k.notify.identity() == Some(id) {
                    if wait.epoch() < k.epoch() {
                        k.epoch = Plain::new(wait.epoch());
                    }
                    k.fresh |= wait.fresh;
                    merged = true;
                    break;
                }
                // SAFETY: the list owns its waits; `next` is one of them or null.
                node = unsafe { k.next.as_mut() };
            }
            if !merged {
                kept.push(wait);
            }
        }
        kept
    }
}

impl Drop for WaitList {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

impl Wait {
    fn epoch(&self) -> u64 {
        // SAFETY: written before the push that published the wait, and only read since.
        self.epoch.with(|e| unsafe { *e })
    }
}

impl Notify for Completion {
    fn notify(&self) {
        // Every wait Atoll registers in an epoch is one Atoll made (`notify::wait`), whose
        // wakers wait for this thread's open change scope (DESIGN.md 9.1.3).
        crate::storage::atoll::notify::finish(self, Ok(0));
    }

    fn notified(&self) -> bool {
        self.finished()
    }

    fn identity(&self) -> Option<usize> {
        self.addr()
    }
}

/// A wait in the list, which owns it.
struct Wait {
    epoch: Plain<u64>,
    notify: Box<dyn Notify>,
    next: *mut Wait,
    /// The wait `wait_for` registers, while it settles its own slot's list: whether that pass
    /// reached it. Only the owner of the list reads or writes it.
    fresh: bool,
}

pub struct Epochs {
    id: u64,
    /// Read by every step entered, so on a line of its own: no other word's writes take it away.
    global: CachePadded<AtomicU64>,
    registry: Arc<Registry>,
    /// A horizon a scan found: the lowest announcement then, or the epoch then if lower. Every
    /// step under way since announced at or above it, or cannot reach what was retired at or
    /// below it, so what is stamped at or below it may be reused without a scan. Raised by every
    /// scan and read by every reclaim, so on a line of its own.
    known: CachePadded<AtomicU64>,
}

/// A thread's slot for one instance, and how deep its steps are nested. Dropped as the thread
/// exits, which gives the slot back.
struct Entry {
    id: u64,
    registry: Arc<Registry>,
    /// In `registry`, which this entry keeps.
    slot: *const Slot,
    completion: *const AtomicPtr<()>,
    depth: Cell<u32>,
}

impl Entry {
    fn slot(&self) -> &Slot {
        // SAFETY: the slot is in `registry`, kept alive by this entry.
        unsafe { &*self.slot }
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        if torn_down() {
            return;
        }
        // A completion read clears its announcement before it can leave this thread.
        assert!(unsafe { &*self.completion }
            .load(Ordering::Relaxed)
            .is_null());
        // Clear already: a step's guard cannot outlive its thread.
        self.slot().taken.store(false, Ordering::Release);
    }
}

/// A thread's entries, each boxed so its address holds while the vector grows: a step keeps its
/// entry and leaves through it.
#[allow(clippy::vec_box)]
type Entries = Vec<Box<Entry>>;

/// A thread's entries, one per instance it has stepped, and the one its last step used.
struct Table {
    entries: RefCell<Entries>,
    /// The id of the instance this thread last stepped and its entry: the next step there, nearly
    /// always the next one, reads it with no borrow and no search. Set only by `with_entry`, after
    /// any change to `entries`, to an entry `entries` holds; an id is never reused, so a stale
    /// pair is never matched.
    last: Cell<(u64, *const Entry)>,
}

impl Table {
    const fn new() -> Self {
        Self {
            entries: RefCell::new(Vec::new()),
            last: Cell::new((0, ptr::null())),
        }
    }
}

#[cfg(not(atoll_loom))]
crate::thread::thread_local! {
    static ENTRIES: Table = const { Table::new() };
}

#[cfg(atoll_loom)]
loom::thread_local! {
    static ENTRIES: Table = Table::new();
}

impl Default for Epochs {
    fn default() -> Self {
        Self::new()
    }
}

impl Epochs {
    pub fn new() -> Self {
        Self {
            id: take_id(&NEXT_ID).expect("epoch instance ids exhausted"),
            global: CachePadded::new(AtomicU64::new(0)),
            registry: Arc::new(Registry {
                first: Chunk::new(),
                completion_extent: CachePadded::new(AtomicUsize::new(0)),
                open: AtomicBool::new(true),
            }),
            known: CachePadded::new(AtomicU64::new(0)),
        }
    }

    /// Runs `f` on the calling thread's entry for this instance, joining the registry on its
    /// first step here. The table changes only then; every other step reads it. Each entry is
    /// boxed, so its address holds while the table grows: a step keeps it and leaves through it.
    /// An entry goes only with its instance closed, so never under a step of that instance.
    #[inline]
    fn with_entry<R>(&self, f: impl FnOnce(&Entry) -> R) -> R {
        ENTRIES.with(|table| {
            let (id, last) = table.last.get();
            if id == self.id {
                // SAFETY: set below to an entry the table holds, boxed; the table drops one only
                // in `register`, after which the pair is set again before it is read.
                return f(unsafe { &*last });
            }
            let found = table
                .entries
                .borrow()
                .iter()
                .find(|e| e.id == self.id)
                .map(|e| ptr::from_ref::<Entry>(e));
            let entry = match found {
                Some(entry) => entry,
                None => {
                    let i = self.register(&table.entries);
                    ptr::from_ref::<Entry>(&table.entries.borrow()[i])
                }
            };
            table.last.set((self.id, entry));
            // SAFETY: boxed in the table, which keeps it while this instance is open.
            f(unsafe { &*entry })
        })
    }

    /// Registers the calling thread on its first step in this instance: takes it a slot, keeps its
    /// entry, and returns where. Out of line, so the lookup every step makes carries none of it.
    #[cold]
    #[inline(never)]
    fn register(&self, entries: &RefCell<Entries>) -> usize {
        let mut entries = entries.borrow_mut();
        // Entries for instances dropped since go.
        // Relaxed: a stale read keeps an entry one more registration, its registry kept alive.
        entries.retain(|e| e.registry.open.load(Ordering::Relaxed));
        let joined = self.registry.join();
        entries.push(Box::new(Entry {
            id: self.id,
            registry: self.registry.clone(),
            slot: joined.slot,
            completion: joined.completion,
            depth: Cell::new(0),
        }));
        entries.len() - 1
    }

    /// The calling thread's completion announcement. The TLS borrow ends before it is used.
    pub fn completion_slot(&self) -> CompletionSlot<'_> {
        let slot = self.with_entry(|entry| entry.completion);
        // SAFETY: this instance and its thread entry keep the registry alive.
        assert!(unsafe { &*slot }.load(Ordering::Relaxed).is_null());
        CompletionSlot {
            slot,
            _epochs: self,
            _thread: std::marker::PhantomData,
        }
    }

    /// Gives each reader of `removed` at most one reference until its announcement clears.
    /// The caller keeps the cell's removed reference alive throughout this scan.
    pub fn protect_removed_completion(&self, removed: &Completion) {
        let raw = removed.as_raw();
        assert!(!raw.is_null(), "only a cell's owned reference is removed");
        fence(Ordering::SeqCst);
        let mut left = self.registry.completion_extent.load(Ordering::Acquire);
        let mut chunk = &self.registry.first;
        while left != 0 {
            let words = &chunk.completions()[..left.min(CHUNK_SLOTS)];
            let mut blocks = words.chunks_exact(4);
            for block in &mut blocks {
                let seen: [*mut (); 4] = std::array::from_fn(|i| block[i].load(Ordering::Acquire));
                let wanted = raw.cast_mut();
                if (seen[0] == wanted)
                    | (seen[1] == wanted)
                    | (seen[2] == wanted)
                    | (seen[3] == wanted)
                {
                    for (slot, announced) in block.iter().zip(seen) {
                        if announced == wanted {
                            Self::give_removed_completion(slot, announced, removed);
                        }
                    }
                }
            }
            for slot in blocks.remainder() {
                let announced = slot.load(Ordering::Acquire);
                if announced == raw.cast_mut() {
                    Self::give_removed_completion(slot, announced, removed);
                }
            }
            left = left.saturating_sub(CHUNK_SLOTS);
            if left == 0 {
                break;
            }
            // SAFETY: a published chunk lives until the registry is dropped.
            let Some(next) = (unsafe { chunk.next.load(Ordering::Acquire).as_ref() }) else {
                break;
            };
            chunk = next;
        }
    }

    #[cold]
    fn give_removed_completion(slot: &AtomicPtr<()>, announced: *mut (), removed: &Completion) {
        // An owned announcement did not match the untagged pointer and already has a reference.
        let extra = removed.clone().into_raw().cast_mut();
        let owned = extra.map_addr(|address| address | COMPLETION_OWNED);
        if slot
            .compare_exchange(announced, owned, Ordering::Release, Ordering::Relaxed)
            .is_err()
        {
            // SAFETY: the failed exchange did not hand this reference to the reader.
            drop(unsafe { Completion::from_raw(extra) });
        }
    }

    /// Enters a step; it ends when the guard is dropped.
    pub fn enter(&self) -> Step<'_> {
        let entry = self.with_entry(|entry| {
            if entry.depth.get() == 0 {
                // An epoch at or above an object's stamp was loaded from the add that retired it
                // or a later one, so this step's reads see the object unlinked: the fence below
                // makes this load an acquire.
                let epoch = self.global.load(Ordering::Relaxed);
                // Release: a scan that reads this announcement in place of the swap that ended
                // the thread's last step sees every read of that step done, as reading the swap
                // would. A relaxed store orders none: only read-modify-writes carry a release on.
                entry
                    .slot()
                    .word
                    .store(announcing(epoch), Ordering::Release);
                // Either a scan sees this announcement, or this step's reads see everything
                // unlinked before the scan's fence.
                fence(Ordering::SeqCst);
            }
            entry.depth.set(entry.depth.get() + 1);
            ptr::NonNull::from(entry)
        });
        crate::storage::atoll::park::holds::raise();
        Step {
            epochs: self,
            entry,
            _thread: std::marker::PhantomData,
        }
    }

    /// Leaves a step of `entry`, the calling thread's entry the step entered through: no lookup.
    fn leave(&self, entry: &Entry) {
        let depth = entry
            .depth
            .get()
            .checked_sub(1)
            .expect("leaving a step never entered");
        entry.depth.set(depth);
        // Release: a scan that reads the slot clear sees every read of the step done.
        // Acquire: a wait that flagged the slot was pushed before it flagged it.
        let word = (depth == 0).then(|| entry.slot().word.swap(IDLE, Ordering::AcqRel));
        if word.is_some_and(|w| w & WAITED != 0) {
            self.wake();
        }
    }

    /// The stamp for an object retired now: the epoch it advances the global epoch to, so a step
    /// entered from here on announces the stamp or above.
    pub fn stamp(&self) -> u64 {
        self.global.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// The global epoch now: at or above the announcement of any step the calling thread is in,
    /// the epoch only rising.
    pub fn current(&self) -> u64 {
        self.global.load(Ordering::SeqCst)
    }

    /// The global epoch and every announcement of a thread in a step, for a test that stalled
    /// to show.
    #[cfg(test)]
    pub fn describe(&self) -> String {
        let announced: Vec<u64> = self
            .registry
            .slots()
            .filter_map(|s| announced(s.word.load(Ordering::SeqCst)))
            .collect();
        format!(
            "epoch {}, threads in a step announced {announced:?}",
            self.global.load(Ordering::SeqCst)
        )
    }

    /// The slots handed out: what a scan reads.
    pub fn participants(&self) -> usize {
        self.registry.chunks().map(|c| c.used().len()).sum()
    }

    /// The lowest epoch any thread in a step announced, read now; objects stamped at or below it
    /// may be reused. Raises [`known_horizon`](Self::known_horizon) to it.
    pub fn horizon(&self) -> u64 {
        // Loaded first: whatever was retired at or below it was unlinked before the fence, which
        // makes this load an acquire.
        let epoch = self.global.load(Ordering::Relaxed);
        fence(Ordering::SeqCst);
        let lowest = self
            .registry
            .slots()
            .filter_map(|s| announced(s.word.load(Ordering::Relaxed)))
            .min()
            .unwrap_or(IDLE);
        // A slot read clear was cleared after its step's last read.
        fence(Ordering::Acquire);
        // Release: a thread acting on the raised value sees the steps read clear done.
        self.known.fetch_max(lowest.min(epoch), Ordering::AcqRel);
        lowest
    }

    /// A horizon some scan found, with no scan now: objects stamped at or below it may be reused.
    /// At or below the horizon a scan would find.
    pub fn known_horizon(&self) -> u64 {
        self.known.load(Ordering::Acquire)
    }

    /// Notifies `notify` once every step under way has announced `epoch` or above, or ended: the
    /// step whose end gets it there does. False if that is so already; `notify` may then be
    /// notified, and whatever it stands for finished, all the same.
    pub fn wait_for(&self, epoch: u64, notify: impl Notify) -> bool {
        // The slot's own list taken whole (its nodes then this thread's), the new wait added, and
        // `wake`'s own procedure run on it (DESIGN.md 9.1.3, other waits): finished waits dropped,
        // waits of one completion collapsed, so a slot's list holds its unfinished waits, at most
        // one per completion, however many events finished registrations behind one old step.
        let wait = Box::new(Wait {
            epoch: Plain::new(epoch),
            notify: Box::new(notify),
            next: ptr::null_mut(),
            fresh: true,
        });
        let done = wait.notify.notified();
        let mut own = self.take_own_waits();
        own.push(wait);
        let reached = self.settle(own);
        !(done || reached)
    }

    /// How many waits the calling thread's slot holds, for a test of the bound.
    #[cfg(test)]
    pub(crate) fn own_waits(&self) -> usize {
        let mut own = self.take_own_waits();
        let mut n = 0;
        while let Some(wait) = own.pop() {
            n += 1;
            self.push(wait);
        }
        n
    }

    /// The calling thread's own slot's waits, taken whole by a swap: no other thread pushes to
    /// the slot, and a wake that takes it meanwhile finds it empty.
    fn take_own_waits(&self) -> WaitList {
        let mut raw = self.with_entry(|entry| {
            entry
                .slot()
                .waits
                .swap(ptr::null_mut(), Ordering::AcqRel)
        });
        let mut waits = WaitList::default();
        while !raw.is_null() {
            // SAFETY: the list owned each wait, and the swap above handed it all over.
            let wait = unsafe { Box::from_raw(raw) };
            raw = linked(wait.next);
            waits.push(wait);
        }
        waits
    }

    /// Flags the slot of every step announced below `epoch`, so that its end wakes the waits,
    /// and returns the lowest of their announcements; [`IDLE`] if there is none. A step that
    /// ends first is read clear instead: the flag and the end go to the same word, so one sees
    /// the other.
    fn flag_holders(&self, epoch: u64) -> u64 {
        fence(Ordering::SeqCst);
        let mut lowest = IDLE;
        for slot in self.registry.slots() {
            let word = slot.word.load(Ordering::Relaxed);
            if announced(word).is_none_or(|a| a >= epoch) {
                continue;
            }
            // One flag, and the hold judged by the word it returns, never the one loaded (DESIGN.md
            // 9.1.2): a step that ended meanwhile reads `IDLE`, which carries `WAITED` already and
            // holds nothing back, and one announced at or above the epoch holds nothing back either.
            // The flag is made even when the word loaded showed it already: that word may be an
            // older step's, ended since, and only this read-modify-write of the latest word orders
            // this wait's push before the end of the step it reads (the audit's two-wait Loom
            // counterexample, 2026-11-08). Release: the step's end that reads the flag sees the
            // wait pushed.
            crate::race::point("epoch.flag");
            let now = slot.word.fetch_or(WAITED, Ordering::Release);
            if let Some(a) = announced(now).filter(|&a| a < epoch) {
                lowest = lowest.min(a);
            }
        }
        fence(Ordering::Acquire);
        lowest
    }

    /// Pushes `wait` on the calling thread's own slot's list: one compare-and-swap of the head it
    /// read, and, lost, one swap, since the only other writer of the head is a wake taking the
    /// whole list, which leaves it empty.
    fn push(&self, wait: Box<Wait>) {
        let raw = Box::into_raw(wait);
        let _ = raw.expose_provenance();
        self.with_entry(|entry| {
            let waits = &entry.slot().waits;
            let head = waits.load(Ordering::Relaxed);
            crate::race::point("epoch.push");
            // SAFETY: the wait is this thread's until the exchange below publishes it.
            unsafe { (*raw).next = head };
            // Acquire as well: pushed after a wake took the list, a wait's push sees the end
            // of the step that woke it.
            if waits
                .compare_exchange(head, raw, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                // SAFETY: as above.
                unsafe { (*raw).next = ptr::null_mut() };
                let taken = waits.swap(raw, Ordering::AcqRel);
                debug_assert!(taken.is_null(), "a slot's waits pushed by another thread");
            }
        });
    }

    /// Takes every slot's waits, each list whole by a swap, into one list this thread owns.
    fn take_waits(&self) -> WaitList {
        let mut waits = WaitList::default();
        for slot in self.registry.slots() {
            let mut raw = slot.waits.swap(ptr::null_mut(), Ordering::AcqRel);
            while !raw.is_null() {
                // SAFETY: the list owned each wait, and the swap above handed it all over.
                let wait = unsafe { Box::from_raw(raw) };
                raw = linked(wait.next);
                waits.push(wait);
            }
        }
        waits
    }

    /// Notifies every wait the horizon has reached, drops those notified otherwise, and puts the
    /// rest back with their holders flagged.
    fn wake(&self) {
        self.settle(self.take_waits());
    }

    /// `wake`'s procedure over `waits`, which this thread owns: waits notified otherwise dropped
    /// and waits of one identity collapsed; the holders of the highest goal flagged; the waits that
    /// flag shows reached notified, the rest pushed back to this thread's slot; and, with them
    /// published, the highest goal's holders flagged again, the procedure repeating while the
    /// lowest goal is at or below what that second flag returns. Only the second flag, made after
    /// the push, sees a holder that ended while the waits were out of the lists (that end found
    /// them empty); a decision from the first alone strands them (DESIGN.md 9.1.3, the auditor's
    /// `pre_republish_only_check_strands_the_wait`). No node is read after it is pushed, and each
    /// repetition follows a step's end. Whether a wait `wait_for` marked fresh was reached in the
    /// first pass.
    fn settle(&self, mut waits: WaitList) -> bool {
        let mut fresh_reached = false;
        let mut first = true;
        loop {
            let mut kept = waits.collapsed();
            let mut highest = None;
            let mut walk = kept.0.as_deref();
            while let Some(w) = walk {
                highest = highest.max(Some(w.epoch()));
                // SAFETY: the list owns its waits; `next` is one of them or null.
                walk = unsafe { w.next.as_ref() };
            }
            let Some(highest) = highest else {
                return fresh_reached;
            };
            let holders = self.flag_holders(highest);
            let (mut reached, mut left_epochs) = (WaitList::default(), None::<(u64, u64)>);
            while let Some(mut wait) = kept.pop() {
                let e = wait.epoch();
                if e <= holders {
                    fresh_reached |= first && wait.fresh;
                    reached.push(wait);
                } else {
                    left_epochs =
                        Some(left_epochs.map_or((e, e), |(lo, hi)| (lo.min(e), hi.max(e))));
                    wait.fresh = false;
                    self.push(wait);
                }
            }
            while let Some(wait) = reached.pop() {
                wait.notify.notify();
            }
            first = false;
            // A flagged step that ended while these were out of the list found none to wake.
            // Put back since, they see its end: look again, flagging any holder seen first now.
            match left_epochs {
                Some((lowest, highest)) if lowest <= self.flag_holders(highest) => {
                    waits = self.take_waits();
                }
                _ => return fresh_reached,
            }
        }
    }
}

impl Drop for Epochs {
    fn drop(&mut self) {
        if torn_down() {
            return;
        }
        self.registry.open.store(false, Ordering::Relaxed);
        drop(self.take_waits());
    }
}

/// A step in progress. It ends on the thread that entered it: its announcement and nesting are
/// that thread's, so it cannot be sent to another, nor can any guard holding one.
pub struct Step<'e> {
    epochs: &'e Epochs,
    /// The calling thread's entry for `epochs`, boxed in its table and kept there while `epochs`
    /// is open, which it is while this step borrows it.
    entry: ptr::NonNull<Entry>,
    _thread: std::marker::PhantomData<*const ()>,
}

const COMPLETION_OWNED: usize = 1;

/// One thread's brief completion read. Clear before any transferred reference is dropped.
pub struct CompletionSlot<'e> {
    slot: *const AtomicPtr<()>,
    _epochs: &'e Epochs,
    _thread: std::marker::PhantomData<*const ()>,
}

impl CompletionSlot<'_> {
    /// Announce a pointer before validating it against the cell again.
    ///
    /// # Safety
    /// `raw` is an untagged pointer read from a completion cell in this instance.
    pub unsafe fn announce(&self, raw: *mut ()) {
        // SAFETY: the instance and its thread entry keep this slot alive; only this thread
        // writes unowned announcements to it.
        unsafe { &*self.slot }.store(raw, Ordering::Release);
        fence(Ordering::SeqCst);
    }
}

impl Drop for CompletionSlot<'_> {
    fn drop(&mut self) {
        if torn_down() {
            return;
        }
        // SAFETY: the instance and thread entry keep this slot alive. The swap clears it before
        // a callback object's destructor can enter Atoll again, with no TLS borrow outstanding.
        let raw = unsafe { &*self.slot }.swap(ptr::null_mut(), Ordering::AcqRel);
        if raw.addr() & COMPLETION_OWNED != 0 {
            let raw = raw.map_addr(|address| address & !COMPLETION_OWNED);
            // SAFETY: the successful remover CAS handed this slot exactly this reference.
            drop(unsafe { Completion::from_raw(raw) });
        }
    }
}

impl Drop for Step<'_> {
    fn drop(&mut self) {
        crate::storage::atoll::park::holds::lower();
        if torn_down() {
            return;
        }
        // SAFETY: the entry is boxed in this thread's table, which drops it only once its
        // instance is closed, and `epochs` is borrowed, so open, while this step lives; the step
        // is not Send, so this is the thread that entered.
        self.epochs.leave(unsafe { self.entry.as_ref() });
    }
}

/// An object retired, in the list of those retired since a reclaim last took them.
struct Node<T> {
    stamp: Plain<u64>,
    item: T,
    /// The node pushed before it, or [`unlinked`] until its push has written that.
    next: AtomicPtr<Node<T>>,
}

/// A retired node's `next` between its push's swap and its link: address 1, which no node can
/// have, a node being aligned to more than one byte. Never dereferenced.
fn unlinked<T>() -> *mut Node<T> {
    const { assert!(std::mem::align_of::<Node<T>>() > 1) };
    ptr::without_provenance_mut(1)
}

/// An object retired and its stamp, ordered so a heap's top is the earliest stamp.
struct Stamped<T>(u64, T);

impl<T> PartialEq for Stamped<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> Eq for Stamped<T> {}

impl<T> PartialOrd for Stamped<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Stamped<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}

/// What the reclaim under way owns.
struct Taken<T> {
    heap: BinaryHeap<Stamped<T>>,
    /// A node a take reached before its push linked what follows it: the next take starts there.
    carry: AtomicPtr<Node<T>>,
    /// Objects taken, and reclaims made, since the last scan.
    since_scan: usize,
}

/// Objects retired and not yet reusable: page numbers and temporary pages.
pub struct Retired<T> {
    /// Retired since a reclaim last took them, newest first: retiring is a push.
    pushed: AtomicPtr<Node<T>>,
    /// Those a reclaim took. Owned by the reclaim under way and only ever tried: a reclaim that
    /// finds another under way leaves the objects to it.
    taken: Mutex<Taken<T>>,
}

impl<T> Default for Retired<T> {
    fn default() -> Self {
        Self {
            pushed: AtomicPtr::new(ptr::null_mut()),
            taken: Mutex::new(Taken {
                heap: BinaryHeap::new(),
                carry: AtomicPtr::new(ptr::null_mut()),
                since_scan: 0,
            }),
        }
    }
}

impl<T> Retired<T> {
    /// Retires `item`: it is reusable once no step that could hold it is left.
    pub fn retire(&self, epochs: &Epochs, item: T) {
        let raw = Box::into_raw(Box::new(Node {
            stamp: Plain::new(epochs.stamp()),
            item,
            next: AtomicPtr::new(unlinked()),
        }));
        let _ = raw.expose_provenance();
        // One swap and one store, whatever other threads do (DESIGN.md 9.1.2): a take that reaches
        // the node before the store leaves it for the next take (`carry`), never waits on it.
        // Release: the node's stamp and item, to the take that swaps it out.
        let prev = self.pushed.swap(raw, Ordering::AcqRel);
        crate::race::point("epoch.retire");
        // SAFETY: the node is alive until a take frees it, which happens only once its link,
        // written here, is read.
        unsafe { (*raw).next.store(prev, Ordering::Release) };
    }

    /// Takes every object no step can hold any more; none while another reclaim is under way.
    /// Decided by the known horizon, and by a scan once as many objects and reclaims have come
    /// since the last as a scan reads slots: a scan's cost is spread over that many, and a
    /// caller that reclaims again and again for want of objects gets its scan.
    pub fn reclaim(&self, epochs: &Epochs) -> Vec<T> {
        self.reclaim_up_to(epochs, usize::MAX)
    }

    /// [`reclaim`](Self::reclaim), taking `max` objects retired since at most and handing back
    /// `max` at most, the rest left for the next call (DESIGN.md 22.1: a unit's work bounded).
    pub fn reclaim_up_to(&self, epochs: &Epochs, max: usize) -> Vec<T> {
        let Some(mut taken) = self.taken.try_lock() else {
            return Vec::new();
        };
        let taken = &mut *taken;
        taken.since_scan += 1 + self.take_pushed(taken, max);
        // Every object stamped at or below a horizon was unlinked before the scan that found
        // it, whenever it was retired.
        let mut horizon = epochs.known_horizon();
        if taken.heap.peek().is_some_and(|s| s.0 > horizon)
            && taken.since_scan >= epochs.participants()
        {
            taken.since_scan = 0;
            horizon = epochs.horizon();
        }
        let mut out = Vec::new();
        while out.len() < max && taken.heap.peek().is_some_and(|s| s.0 <= horizon) {
            out.push(taken.heap.pop().expect("an object").1);
        }
        out
    }

    /// Moves the objects pushed since into the heap, `max` at most: those after the node the last
    /// take carried, then those pushed since; returns how many.
    fn take_pushed(&self, taken: &mut Taken<T>, max: usize) -> usize {
        // Relaxed: `carry` is read and written only under the reclaim's lock.
        let carried = taken.carry.swap(ptr::null_mut(), Ordering::Relaxed);
        let mut count = Self::take_chain(taken, carried, max);
        if taken.carry.load(Ordering::Relaxed).is_null() {
            let head = self.pushed.swap(ptr::null_mut(), Ordering::Acquire);
            count += Self::take_chain(taken, head, max - count);
        } else {
            // The carried chain's end is still being linked; what was pushed since waits behind
            // it for the next take, which then reaches it through the head.
        }
        count
    }

    /// Takes the chain from `raw` into the heap, `max` nodes at most, stopping at a node whose push
    /// has not linked what follows it yet or past the most: that node is carried to the next take.
    fn take_chain(taken: &mut Taken<T>, mut raw: *mut Node<T>, max: usize) -> usize {
        let mut count = 0;
        while !raw.is_null() {
            if count == max {
                taken.carry.store(raw, Ordering::Relaxed);
                break;
            }
            // SAFETY: published by `retire`, and taken by the swap of the head or carried here,
            // so this thread its only holder.
            let next = unsafe { (*raw).next.load(Ordering::Acquire) };
            if next == unlinked() {
                taken.carry.store(raw, Ordering::Relaxed);
                break;
            }
            // SAFETY: as above; its link is read, so its push is done with it.
            let node = unsafe { Box::from_raw(raw) };
            raw = linked(next);
            // SAFETY: written before the push that published the node, and only read since.
            let stamp = node.stamp.with(|s| unsafe { *s });
            taken.heap.push(Stamped(stamp, node.item));
            count += 1;
        }
        count
    }

    /// What waits, for a test to count it.
    #[cfg(test)]
    pub fn waiting(&self) -> Vec<T>
    where
        T: Clone,
    {
        let mut taken = self.taken.lock();
        self.take_pushed(&mut taken, usize::MAX);
        taken.heap.iter().map(|s| s.1.clone()).collect()
    }
}

impl<T> Drop for Retired<T> {
    fn drop(&mut self) {
        if torn_down() {
            return;
        }
        // With `&mut self` every push is done, so every link is written.
        let carried = self
            .taken
            .lock()
            .carry
            .swap(ptr::null_mut(), Ordering::Relaxed);
        for head in [
            carried,
            self.pushed.swap(ptr::null_mut(), Ordering::Acquire),
        ] {
            let mut raw = head;
            while !raw.is_null() {
                // SAFETY: as in `take_chain`, with `&mut self` holding off every other thread.
                let node = unsafe { Box::from_raw(raw) };
                raw = linked(node.next.load(Ordering::Acquire));
            }
        }
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests;

#[cfg(all(test, atoll_loom))]
mod loom_tests;
