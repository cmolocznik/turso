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
    pub(super) use crate::sync::atomic::{
        fence, AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering,
    };
    pub(super) use crate::sync::{Arc, Mutex};

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

/// Distinguishes instances in the per-thread table.
static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A participant's place in an instance's registry: what it announced, and whether a thread
/// holds it. Padded to a cache line of its own, or to the pair of lines x86_64 and aarch64 cores
/// fetch together, so a thread's announcements never contend with another's.
type Slot = CachePadded<SlotWords>;

struct SlotWords {
    /// [`IDLE`], or [`announcing`] an epoch, with [`WAITED`] set once a wait it holds back flags it.
    word: AtomicU64,
    taken: AtomicBool,
}

/// Slots handed out in order, and the chunk after them once they are all taken.
struct Chunk {
    /// Written once, before the chunk is linked in.
    slots: Plain<Box<[Slot]>>,
    /// Slots handed out so far, or more: a scan reads no further.
    used: AtomicUsize,
    next: AtomicPtr<Chunk>,
}

// SAFETY: `slots` is written only as the chunk is made, before any other thread can reach it;
// everything else in it is atomic.
unsafe impl Sync for Chunk {}
unsafe impl Send for Chunk {}

impl Chunk {
    fn new() -> Self {
        Self {
            slots: Plain::new(
                (0..CHUNK_SLOTS)
                    .map(|_| {
                        CachePadded::new(SlotWords {
                            word: AtomicU64::new(IDLE),
                            taken: AtomicBool::new(false),
                        })
                    })
                    .collect(),
            ),
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
}

/// Every slot of an instance: chunks that only grow, kept until the instance and every thread
/// holding a slot in it are gone, so a slot's address stays good as long as either holds it.
struct Registry {
    first: Chunk,
    /// Cleared when the instance is dropped, so threads drop their entries for it.
    open: AtomicBool,
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

    /// A slot for the calling thread: one a thread that exited gave back, or a new one.
    fn join(&self) -> &Slot {
        let mut chunk = &self.first;
        loop {
            let free = chunk.used().iter().find(|s| {
                !s.taken.load(Ordering::Relaxed)
                    && s.taken
                        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
            });
            if let Some(slot) = free {
                return slot;
            }
            if chunk.used.load(Ordering::Relaxed) < CHUNK_SLOTS {
                let i = chunk.used.fetch_add(1, Ordering::AcqRel);
                // A thread that read the count first may take the slot before this does: then
                // look again.
                if i < CHUNK_SLOTS
                    && chunk
                        .slot(i)
                        .taken
                        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                {
                    return chunk.slot(i);
                }
                continue;
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
}

impl Wait {
    fn epoch(&self) -> u64 {
        // SAFETY: written before the push that published the wait, and only read since.
        self.epoch.with(|e| unsafe { *e })
    }
}

impl Notify for Completion {
    fn notify(&self) {
        self.complete(0);
    }

    fn notified(&self) -> bool {
        self.finished()
    }
}

/// A wait in the list, which owns it.
struct Wait {
    epoch: Plain<u64>,
    notify: Box<dyn Notify>,
    next: *mut Wait,
}

pub struct Epochs {
    id: u64,
    /// Read by every step entered, so on a line of its own: no other word's writes take it away.
    global: CachePadded<AtomicU64>,
    registry: Arc<Registry>,
    /// The waits registered and not yet finished, newest first. Taken whole by a wake, so no
    /// wait is ever unlinked under another thread.
    waits: AtomicPtr<Wait>,
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
        // Clear already: a step's guard cannot outlive its thread.
        self.slot().taken.store(false, Ordering::Release);
    }
}

#[cfg(not(atoll_loom))]
crate::thread::thread_local! {
    static ENTRIES: RefCell<Vec<Entry>> = const { RefCell::new(Vec::new()) };
}

#[cfg(atoll_loom)]
loom::thread_local! {
    static ENTRIES: RefCell<Vec<Entry>> = RefCell::new(Vec::new());
}

impl Default for Epochs {
    fn default() -> Self {
        Self::new()
    }
}

impl Epochs {
    pub fn new() -> Self {
        Self {
            // Relaxed: only its uniqueness matters.
            id: NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            global: CachePadded::new(AtomicU64::new(0)),
            registry: Arc::new(Registry {
                first: Chunk::new(),
                open: AtomicBool::new(true),
            }),
            waits: AtomicPtr::new(ptr::null_mut()),
            known: CachePadded::new(AtomicU64::new(0)),
        }
    }

    /// Runs `f` on the calling thread's entry for this instance, joining the registry on its
    /// first step here. The table changes only then; every other step reads it.
    fn with_entry<R>(&self, f: impl FnOnce(&Entry) -> R) -> R {
        ENTRIES.with(|entries| {
            let found = entries.borrow().iter().position(|e| e.id == self.id);
            let i = found.unwrap_or_else(|| self.register(entries));
            f(&entries.borrow()[i])
        })
    }

    /// Registers the calling thread on its first step in this instance: takes it a slot, keeps its
    /// entry, and returns where. Out of line, so the lookup every step makes carries none of it.
    #[cold]
    #[inline(never)]
    fn register(&self, entries: &RefCell<Vec<Entry>>) -> usize {
        let mut entries = entries.borrow_mut();
        // Entries for instances dropped since go.
        // Relaxed: a stale read keeps an entry one more registration, its registry kept alive.
        entries.retain(|e| e.registry.open.load(Ordering::Relaxed));
        let slot: *const Slot = self.registry.join();
        entries.push(Entry {
            id: self.id,
            registry: self.registry.clone(),
            slot,
            depth: Cell::new(0),
        });
        entries.len() - 1
    }

    /// Enters a step; it ends when the guard is dropped.
    pub fn enter(&self) -> Step<'_> {
        self.with_entry(|entry| {
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
        });
        Step {
            epochs: self,
            _thread: std::marker::PhantomData,
        }
    }

    fn leave(&self) {
        let word = self.with_entry(|entry| {
            let depth = entry
                .depth
                .get()
                .checked_sub(1)
                .expect("leaving a step never entered");
            entry.depth.set(depth);
            // Release: a scan that reads the slot clear sees every read of the step done.
            // Acquire: a wait that flagged the slot was pushed before it flagged it.
            (depth == 0).then(|| entry.slot().word.swap(IDLE, Ordering::AcqRel))
        });
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
        self.push(Box::new(Wait {
            epoch: Plain::new(epoch),
            notify: Box::new(notify),
            next: ptr::null_mut(),
        }));
        if self.flag_holders(epoch) < epoch {
            return true;
        }
        // Reached, and no step's end may come to wake it: finish it now.
        self.wake();
        false
    }

    /// Notifies `notify` once every step under way now has ended, the calling thread's own
    /// included. False if none is.
    pub fn grace_period(&self, notify: impl Notify) -> bool {
        self.wait_for(self.stamp(), notify)
    }

    /// Flags the slot of every step announced below `epoch`, so that its end wakes the waits,
    /// and returns the lowest of their announcements; [`IDLE`] if there is none. A step that
    /// ends first is read clear instead: the flag and the end go to the same word, so one sees
    /// the other.
    fn flag_holders(&self, epoch: u64) -> u64 {
        fence(Ordering::SeqCst);
        let mut lowest = IDLE;
        for slot in self.registry.slots() {
            let mut word = slot.word.load(Ordering::Relaxed);
            while let Some(a) = announced(word).filter(|&a| a < epoch) {
                if word & WAITED != 0 {
                    lowest = lowest.min(a);
                    break;
                }
                // Release: the step's end that reads the flag sees the wait pushed.
                match slot.word.compare_exchange_weak(
                    word,
                    word | WAITED,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        lowest = lowest.min(a);
                        break;
                    }
                    Err(now) => word = now,
                }
            }
        }
        fence(Ordering::Acquire);
        lowest
    }

    fn push(&self, wait: Box<Wait>) {
        let raw = Box::into_raw(wait);
        let _ = raw.expose_provenance();
        let mut head = self.waits.load(Ordering::Relaxed);
        loop {
            // SAFETY: the wait is this thread's until the exchange below publishes it.
            unsafe { (*raw).next = head };
            // Acquire as well: pushed after a wake took the list, a wait's push sees the end
            // of the step that woke it.
            match self
                .waits
                .compare_exchange_weak(head, raw, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(now) => head = now,
            }
        }
    }

    /// Notifies every wait the horizon has reached, drops those notified otherwise, and puts the
    /// rest back with their holders flagged.
    fn wake(&self) {
        loop {
            let mut raw = self.waits.swap(ptr::null_mut(), Ordering::AcqRel);
            let mut waits = Vec::new();
            while !raw.is_null() {
                // SAFETY: the list owned each wait, and the swap above handed it all over.
                let wait = unsafe { Box::from_raw(raw) };
                raw = linked(wait.next);
                if !wait.notify.notified() {
                    waits.push(wait);
                }
            }
            let Some(highest) = waits.iter().map(|w| w.epoch()).max() else {
                return;
            };
            let holders = self.flag_holders(highest);
            let (reached, left): (Vec<_>, Vec<_>) =
                waits.into_iter().partition(|w| w.epoch() <= holders);
            let left_epochs =
                left.iter()
                    .map(|w| w.epoch())
                    .fold(None, |r: Option<(u64, u64)>, e| {
                        Some(r.map_or((e, e), |(lo, hi)| (lo.min(e), hi.max(e))))
                    });
            for wait in left {
                self.push(wait);
            }
            for wait in reached {
                wait.notify.notify();
            }
            // A flagged step that ended while these were out of the list found none to wake.
            // Put back since, they see its end: look again, flagging any holder seen first now.
            match left_epochs {
                Some((lowest, highest)) if lowest <= self.flag_holders(highest) => continue,
                _ => return,
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
        let mut raw = self.waits.swap(ptr::null_mut(), Ordering::Acquire);
        while !raw.is_null() {
            // SAFETY: as in `wake`: the swap handed the whole list to this thread.
            let wait = unsafe { Box::from_raw(raw) };
            raw = linked(wait.next);
        }
    }
}

/// A step in progress. It ends on the thread that entered it: its announcement and nesting are
/// that thread's, so it cannot be sent to another, nor can any guard holding one.
pub struct Step<'e> {
    epochs: &'e Epochs,
    _thread: std::marker::PhantomData<*const ()>,
}

impl Drop for Step<'_> {
    fn drop(&mut self) {
        if torn_down() {
            return;
        }
        self.epochs.leave();
    }
}

/// An object retired, in the list of those retired since a reclaim last took them.
struct Node<T> {
    stamp: Plain<u64>,
    item: T,
    next: *mut Node<T>,
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
            next: ptr::null_mut(),
        }));
        let _ = raw.expose_provenance();
        let mut head = self.pushed.load(Ordering::Relaxed);
        loop {
            // SAFETY: the node is this thread's until the exchange below publishes it.
            unsafe { (*raw).next = head };
            match self
                .pushed
                .compare_exchange_weak(head, raw, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(now) => head = now,
            }
        }
    }

    /// Takes every object no step can hold any more; none while another reclaim is under way.
    /// Decided by the known horizon, and by a scan once as many objects and reclaims have come
    /// since the last as a scan reads slots: a scan's cost is spread over that many, and a
    /// caller that reclaims again and again for want of objects gets its scan.
    pub fn reclaim(&self, epochs: &Epochs) -> Vec<T> {
        let Some(mut taken) = self.taken.try_lock() else {
            return Vec::new();
        };
        let taken = &mut *taken;
        taken.since_scan += 1 + self.take_pushed(&mut taken.heap);
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
        while taken.heap.peek().is_some_and(|s| s.0 <= horizon) {
            out.push(taken.heap.pop().expect("an object").1);
        }
        out
    }

    /// Moves every object pushed since into `heap`; returns how many.
    fn take_pushed(&self, heap: &mut BinaryHeap<Stamped<T>>) -> usize {
        let mut raw = self.pushed.swap(ptr::null_mut(), Ordering::Acquire);
        let mut count = 0;
        while !raw.is_null() {
            // SAFETY: published by `retire` with a release exchange, and the swap above made
            // this thread its only holder.
            let node = unsafe { Box::from_raw(raw) };
            raw = linked(node.next);
            // SAFETY: written before the push that published the node, and only read since.
            let stamp = node.stamp.with(|s| unsafe { *s });
            heap.push(Stamped(stamp, node.item));
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
        self.take_pushed(&mut taken.heap);
        taken.heap.iter().map(|s| s.1.clone()).collect()
    }
}

impl<T> Drop for Retired<T> {
    fn drop(&mut self) {
        if torn_down() {
            return;
        }
        let mut raw = self.pushed.swap(ptr::null_mut(), Ordering::Acquire);
        while !raw.is_null() {
            // SAFETY: as in `take_pushed`, with `&mut self` holding off every other thread.
            let node = unsafe { Box::from_raw(raw) };
            raw = linked(node.next);
        }
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests;

#[cfg(all(test, atoll_loom))]
mod loom_tests;
