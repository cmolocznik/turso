//! The records a database's follow-ons are made in, and the thread that submits them (DESIGN.md
//! 22.18, *A follow-on at retirement*; ASYNC1 D1a). A request whose owner needs another after it
//! (a metadata or superblock write's sync, on a file that does not write durably) is submitted
//! through [`CountedFile::write_then`] into a record held for its chain. At the request's
//! retirement Counted's one observer makes the follow-on into the same record, counted before the
//! first request is uncounted, and publishes it to the database's one IO thread, which submits
//! it. No callback or observer submits, so no syscall runs nested inside another's delivery, and
//! no follow-on is submitted from the thread that retired the request before it. A chain's first
//! request is still submitted by its caller: queueing that one is D2's.

use super::{CountedFile, Requests};
use crate::io::Completion;
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

    /// Publishes its follow-on, already written, as record `i` of `bits`: READY, `Relaxed`, then
    /// its bit, whose `fetch_or` publishes the payload, the claim being ordered by the take of the
    /// bit. The caller touches the record no more; whether the IO thread was idle, to be unparked.
    pub(super) fn publish(&self, bits: &Bitmap, i: usize) -> bool {
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

/// One bit per record, set when its follow-on is READY, the stop, and the IO thread's idle mark, all
/// in one word, so every hand-off and every wake is a read-modify-write of one location, ordered by
/// its modification order with no fence. A producer sets its bit with `fetch_or`, `AcqRel`, after
/// writing the payload, and the IO thread takes the word with `fetch_and`, `Acquire`: the pair is
/// the payload's publication edge. The stop is set the same way and stays set. The IO thread marks
/// itself idle only by exchanging an empty word for [`IDLE`], so a bit or the stop set before the
/// exchange makes it fail, and one set after it reads the mark and unparks the thread: no wake is
/// lost. One word holds the 33 records of D1a; a bitmap of more words is D2's, with its own event
/// count.
pub(super) struct Bitmap {
    word: AtomicU64,
}

/// The IO thread is parked, or about to park: the next producer, or the stop, unparks it.
const IDLE: u64 = 1 << 63;
/// Close has sealed the requests, or the last of them dropped: the IO thread ends once it has
/// served every bit set.
const STOP: u64 = 1 << 62;
/// The records' bits.
const BITS: u64 = STOP - 1;

impl Bitmap {
    pub(super) fn new(bits: usize) -> Self {
        assert!(bits <= 62, "{bits} records past one word's bits");
        Self {
            word: AtomicU64::new(0),
        }
    }

    /// Sets bit `i`; whether the IO thread was idle, to be unparked.
    pub(super) fn set(&self, i: usize) -> bool {
        self.word.fetch_or(1 << i, sync::Ordering::AcqRel) & IDLE != 0
    }

    /// Sets the stop; whether the IO thread was idle, to be unparked.
    pub(super) fn stop(&self) -> bool {
        self.word.fetch_or(STOP, sync::Ordering::AcqRel) & IDLE != 0
    }

    /// Takes every record's bit set, and the idle mark with them, the stop left set: the bits, and
    /// whether the stop is set.
    pub(super) fn take(&self) -> (u64, bool) {
        let word = self.word.fetch_and(STOP, sync::Ordering::Acquire);
        (word & BITS, word & STOP != 0)
    }

    /// Marks the IO thread idle if no bit and no stop is set: whether it may park.
    pub(super) fn idle(&self) -> bool {
        self.word
            .compare_exchange(0, IDLE, sync::Ordering::AcqRel, sync::Ordering::Relaxed)
            .is_ok()
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

/// The records a database's follow-ons are made in, the word that publishes them and stops the one
/// IO thread that submits them, and the thread, to be woken.
pub(in crate::storage::atoll) struct Dispatch {
    records: Box<[Record<Link>]>,
    bits: Bitmap,
    /// The log's doorbell, and the log it rings for, set once at the log's open.
    door: Door,
    log: crate::sync::OnceLock<Weak<dyn LogWork>>,
    thread: crate::sync::OnceLock<crate::thread::Thread>,
    requests: Weak<Requests>,
    /// The thread has left its loop, for a test's look after its owners are gone.
    #[cfg(test)]
    pub(super) exited: AtomicBool,
    /// A test's hold on the IO thread before it claims a record.
    #[cfg(test)]
    hold: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
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
}

/// The records of the metadata cache's writes, `checkpoint::WRITES` of them (DESIGN.md 22.18: the
/// checkpoint's metadata unit keeps at most that many writes in flight), then the superblock's.
pub(super) const META_WRITES: std::ops::Range<usize> = 0..crate::storage::atoll::checkpoint::WRITES;
pub(super) const SUPERBLOCK: std::ops::Range<usize> = META_WRITES.end..META_WRITES.end + 1;
const RECORDS: usize = SUPERBLOCK.end;
/// The log's doorbell bit, after the records.
pub(super) const LOG: usize = RECORDS;

/// A record taken for one chain, by its index.
pub(in crate::storage::atoll) struct Held(usize);

impl Held {
    pub(super) fn index(&self) -> usize {
        self.0
    }
}

impl Dispatch {
    pub(super) fn new(requests: &Arc<Requests>) -> Self {
        Self {
            records: (0..RECORDS).map(|_| Record::default()).collect(),
            bits: Bitmap::new(LOG + 1),
            door: Door::new(),
            log: crate::sync::OnceLock::new(),
            thread: crate::sync::OnceLock::new(),
            requests: Arc::downgrade(requests),
            #[cfg(test)]
            exited: AtomicBool::new(false),
            #[cfg(test)]
            hold: Mutex::new(None),
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
        // Published: from here the record is the IO thread's.
        if record.publish(&self.bits, i) {
            self.unpark();
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

    pub(super) fn unpark(&self) {
        if let Some(thread) = self.thread.get() {
            thread.unpark();
        }
    }

    /// Ends the IO thread once it has served every bit set.
    pub(super) fn stop(&self) {
        if self.bits.stop() {
            self.unpark();
        }
    }

    /// Serves every bit set: whether there was one, and whether the stop is set.
    fn pass(self: &Arc<Self>) -> (bool, bool) {
        let (mut word, stopped) = self.bits.take();
        let served = word != 0;
        while word != 0 {
            let bit = word.trailing_zeros() as usize;
            word &= word - 1;
            if bit == LOG {
                self.serve_log();
            } else {
                self.serve(bit);
            }
        }
        (served, stopped)
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
        if self.bits.set(LOG) {
            self.unpark();
        }
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
                self.bits.set(LOG);
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

    /// The IO thread's loop: a pass over the bits, then, with none set, a park until a producer
    /// or the stop unparks it; once stopped and every bit served, its end.
    #[turso_macros::atoll_entry]
    fn run(self: &Arc<Self>) {
        loop {
            match self.pass() {
                (true, _) => continue,
                (false, true) => break,
                (false, false) => {}
            }
            // Idle only on an empty word: a bit or the stop set since the pass fails it, and one
            // set after it unparks this thread.
            if self.bits.idle() {
                #[cfg(test)]
                let note = self.notes.lock().idle.take();
                #[cfg(test)]
                if let Some(note) = note {
                    let _ = note.send(());
                }
                crate::storage::atoll::park::thread();
            }
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

    /// Starts the IO thread for `dispatch`: marked a delivering thread for its life, so nothing
    /// that waits runs on it (a close, an executor's wait).
    pub(super) fn spawn(dispatch: &Arc<Self>) -> Result<crate::thread::JoinHandle<()>> {
        #[cfg(test)]
        if FAIL_SPAWN.with(|f| f.replace(false)) {
            return Err(crate::LimboError::InternalError(
                "a database's IO thread did not start: a failure injected".to_string(),
            ));
        }
        let mine = dispatch.clone();
        let handle = crate::thread::Builder::new()
            .name("atoll-io".to_string())
            .spawn(move || {
                crate::io::deliver_on_this_thread_for_life();
                mine.run();
            })
            .map_err(|e| {
                crate::LimboError::InternalError(format!(
                    "a database's IO thread did not start: {e}"
                ))
            })?;
        let set = dispatch.thread.set(handle.thread().clone());
        debug_assert!(set.is_ok(), "an IO thread started twice");
        Ok(handle)
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

    #[cfg(test)]
    fn pause_at_end_gap(&self, requests: &Requests) {
        let gap = self.notes.lock().end_gap.take();
        if let Some((seen, go)) = gap {
            let _ = seen.send((self.door.read(), requests.outstanding()));
            let _ = go.recv();
        }
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

    /// The records at rest, by count.
    #[cfg(test)]
    pub(super) fn at_rest(&self) -> usize {
        self.records.iter().filter(|r| r.at_rest()).count()
    }
}

#[cfg(test)]
crate::thread::thread_local! {
    /// Set by a test: this thread's next IO thread does not start.
    pub(super) static FAIL_SPAWN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn closed() -> CompletionError {
    CompletionError::Aborted
}

#[cfg(all(test, atoll_loom))]
mod loom_tests;
