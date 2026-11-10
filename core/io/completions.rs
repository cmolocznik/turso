use crate::{turso_assert, turso_assert_eq};
use core::fmt::{self, Debug};
use std::{
    cell::Cell,
    future::{Future, IntoFuture},
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc, OnceLock, Weak,
    },
    task::{Poll, Waker},
};

use crate::sync::Mutex;
use smallvec::SmallVec;

use crate::{Buffer, CompletionError};

/// Callback for read completions. Returns `Some(error)` if the callback detects an error
/// (e.g., short read), which will be stored in the completion and propagated to VDBE.
pub type ReadComplete =
    dyn Fn(Result<(Arc<Buffer>, i32), CompletionError>) -> Option<CompletionError> + Send + Sync;
pub type WriteComplete = dyn Fn(Result<i32, CompletionError>) + Send + Sync;
pub type SyncComplete = dyn Fn(Result<i32, CompletionError>) + Send + Sync;
pub type TruncateComplete = dyn Fn(Result<i32, CompletionError>) + Send + Sync;

#[must_use]
#[derive(Debug, Clone)]
pub struct Completion {
    /// Optional completion state. If None, it means we are Yield in order to not allocate anything
    pub(super) inner: Option<Arc<CompletionInner>>,
}

impl IntoFuture for Completion {
    type Output = Result<(), crate::LimboError>;
    type IntoFuture = CompletionFuture;

    fn into_future(self) -> Self::IntoFuture {
        CompletionFuture {
            completion: self,
            key: Cell::new(None),
            _pinned: std::marker::PhantomPinned,
        }
    }
}

/// One asynchronous waiter: its latest waker is registered until the waiter is dropped.
pub struct CompletionFuture {
    completion: Completion,
    key: Cell<Option<WakerKey>>,
    // The registration's key is this field's address once the future is polled.
    _pinned: std::marker::PhantomPinned,
}

impl Future for CompletionFuture {
    type Output = Result<(), crate::LimboError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        if !this.completion.finished() {
            let key = this.key.get().unwrap_or_else(|| {
                let key = WakerKey::new(std::ptr::addr_of!(this.key) as usize).unwrap();
                this.key.set(Some(key));
                key
            });
            this.completion.set_keyed_waker(key, cx.waker());
        }
        if this.completion.finished() {
            return Poll::Ready(this.completion.get_error().map_or(Ok(()), |error| {
                Err(crate::LimboError::CompletionError(error))
            }));
        }
        Poll::Pending
    }
}

impl CompletionFuture {
    pub(crate) fn completion(&self) -> &Completion {
        &self.completion
    }

    /// This future's key with a pumped backend once it has been polled pending: its completion
    /// key, the address of its pinned key field, which its drop withdraws.
    pub(crate) fn ready_key(&self) -> Option<crate::io::ReadyKey> {
        self.key.get().map(crate::io::ReadyKey::of_place)
    }
}

impl Drop for CompletionFuture {
    fn drop(&mut self) {
        if let Some(key) = self.key.get() {
            self.completion.withdraw_key(key);
        }
    }
}

/// A key a waiter registers its waker under, unique while it is registered (the address of what
/// owns the waker), so it withdraws exactly that registration: `Waker::will_wake` may say two
/// wakers of one task differ, which cannot withdraw exactly. Nonzero, so an unkeyed registration
/// costs no word of its own.
pub type WakerKey = std::num::NonZeroUsize;

/// A waker registered on a completion: keyed by a waiter that withdraws it, or not.
struct Registered {
    key: Option<WakerKey>,
    waker: Waker,
}

/// Where an engine's wait hands its wakers ([`Completion::new_engine_wait`]): a plain function,
/// stored in the completion beside its result, so a dispatch allocates nothing.
pub type Dispatch = fn(Wakers);

/// The wakers of a wait finished by [`Completion::finish_deferred`], taken and not yet woken.
#[must_use = "a wait's wakers taken and never woken leave its waiters parked"]
pub struct Wakers(SmallVec<[Registered; 1]>);

impl Wakers {
    pub fn wake(self) {
        for registered in self.0 {
            registered.waker.wake();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Default)]
struct ContextInner {
    /// A waker for each task waiting on the completion. Finishing the completion wakes them all.
    wakers: SmallVec<[Registered; 1]>,
    /// What runs once the request is retired, registered while a backend owns it
    /// ([`Completion::on_retired`]).
    on_retired: Vec<Box<dyn FnOnce() + Send>>,
    // TODO: add abort signal
}

impl Debug for ContextInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextInner")
            .field("wakers", &self.wakers.len())
            .field("on_retired", &self.on_retired.len())
            .finish()
    }
}

#[derive(Debug)]
struct ContextState {
    inner: Mutex<ContextInner>,
}

#[derive(Debug, Clone)]
pub struct Context {
    inner: Arc<ContextState>,
}

impl ContextInner {
    pub fn new() -> Self {
        Self {
            wakers: SmallVec::new(),
            on_retired: Vec::new(),
        }
    }

    /// Registers `waker` unless a waker of the same task is registered already, as far as
    /// `will_wake` can tell. Another task's waker stays registered beside it, so each task waiting
    /// on the completion is woken when it finishes, and registering wakes no task.
    /// Takes a clone made before the lock was taken and hands it back unused, to be dropped once
    /// the lock is let go: a waker's clone and drop never run under the lock.
    fn set_waker(&mut self, waker: Waker) -> Option<Waker> {
        if self
            .wakers
            .iter()
            .any(|r| r.key.is_none() && r.waker.will_wake(&waker))
        {
            return Some(waker);
        }
        self.wakers.push(Registered { key: None, waker });
        None
    }

    /// Keeps the latest `waker` under `key`: at most one registration per key, with the previous
    /// waker dropped after the lock is released.
    fn set_keyed_waker(&mut self, key: WakerKey, waker: Waker) -> Option<Waker> {
        if let Some(registered) = self.wakers.iter_mut().find(|r| r.key == Some(key)) {
            return Some(std::mem::replace(&mut registered.waker, waker));
        }
        self.wakers.push(Registered {
            key: Some(key),
            waker,
        });
        None
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ContextState::new()),
        }
    }

    /// Wakes registered tasks with no lock held.
    pub fn wake(&self) {
        self.inner.wake();
    }

    /// Removes the registration under `key`, dropping it after releasing the lock.
    pub fn withdraw_key(&self, key: WakerKey) {
        self.inner.withdraw_key(key);
    }

    /// Registers `waker` beside the other tasks waiting.
    pub fn set_waker(&self, waker: &Waker) {
        self.inner.set_waker(waker);
    }
}

impl ContextState {
    fn new() -> Self {
        Self {
            inner: Mutex::new(ContextInner::new()),
        }
    }

    /// Takes the wakers under the lock and wakes them once the lock is free: a waker may register
    /// again on the same completion from its wake.
    fn wake(&self) {
        let wakers = std::mem::take(&mut self.inner.lock().wakers);
        for registered in wakers {
            registered.waker.wake();
        }
    }

    /// Withdraws the waker registered under `key`, exactly. It is dropped once the lock is let
    /// go: dropping it may run its owner's destructor, which must not run under the completion's
    /// lock.
    fn withdraw_key(&self, key: WakerKey) {
        let withdrawn = {
            let mut inner = self.inner.lock();
            let at = inner.wakers.iter().position(|r| r.key == Some(key));
            at.map(|i| inner.wakers.swap_remove(i))
        };
        drop(withdrawn);
    }

    /// Registers `waker` beside the wakers of the other tasks waiting on the completion.
    fn set_waker(&self, waker: &Waker) {
        // Shuttle cannot see the std `OnceLock` holding a completion's result, so yield here to
        // let it run a completion between a test of that result and this store.
        #[cfg(shuttle)]
        crate::thread::yield_now();
        let waker = waker.clone();
        let unused = self.inner.lock().set_waker(waker);
        drop(unused);
    }

    /// Registers `waker` under `key`, cloned before the lock and any unused clone dropped after.
    fn set_keyed_waker(&self, key: WakerKey, waker: &Waker) {
        let waker = waker.clone();
        let unused = self.inner.lock().set_keyed_waker(key, waker);
        drop(unused);
    }

    /// Takes every waker registered, under the lock, for a dispatch that runs them elsewhere.
    fn take(&self) -> Wakers {
        Wakers(std::mem::take(&mut self.inner.lock().wakers))
    }
}

/// A request's ownership (DESIGN.md 22.5): not submitted until a backend takes it; accepted while a
/// backend holds it and every buffer it reads into or writes from, whoever finishes it meanwhile (a
/// cancel finishes a request the kernel still holds); retired once the backend has let both go,
/// after its callback returned. A group's is the aggregate of the requests beneath it.
const NOT_SUBMITTED: u8 = 0;
const ACCEPTED: u8 = 1;
const RETIRED: u8 = 2;
/// Beside [`ACCEPTED`]: something waits for the retirement ([`Completion::on_retired`]), so
/// whoever ends the ownership takes the waits.
const OBSERVED: u8 = 4;
/// The request's callback, run by a finish other than its delivery (a cancellation, a carrying
/// layer's finish), has returned.
const CALLED: u8 = 8;
/// The backend let the request go while another finish was still running its callback. The
/// retirement waits for both [`CALLED`] and this: whichever is set second, by the one atomic step
/// that sets it, sees the other and retires the request, so each happens once and neither waits.
const ENDED: u8 = 16;

pub(super) struct CompletionInner {
    completion_type: CompletionType,
    /// For a request, [`NOT_SUBMITTED`], [`ACCEPTED`] (with [`OBSERVED`]) or [`RETIRED`].
    ownership: AtomicU8,
    /// None means we completed successfully
    // Thread safe with OnceLock
    pub(super) result: crate::sync::OnceLock<Option<CompletionError>>,
    /// Taken by the one finish that runs the callback and publishes `result`. Every other finish
    /// (a cancellation from the callback itself, from a waker it runs, or from another thread)
    /// finds it taken and returns at once: it never waits for the callback it would re-enter.
    finishing: AtomicBool,
    context: ContextState,
    /// The group this completion belongs to, if any. `CompletionGroup::add`
    /// sets it before the completion is submitted, so by the time the
    /// completion finishes and its callback counts it into the group, the
    /// link is already there.
    parent: Mutex<Option<Completion>>,
    /// Keeps the write buffer alive for async I/O backends (io_uring, VFS)
    /// where pwrite returns before the kernel has consumed the buffer.
    write_buffer: OnceLock<Arc<Buffer>>,
    /// For a wait the engine makes ([`Completion::new_engine_wait`]), where its wakers go: every way
    /// of running them (a finish, a late registration, a nudge) hands them to this function
    /// instead of waking them in place (DESIGN.md 9.1.3). Never counted into a group. Set at
    /// making.
    engine: Option<Dispatch>,
}

const _: () = assert!(std::mem::align_of::<CompletionInner>() >= 2);

impl fmt::Debug for CompletionInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionInner")
            .field("completion_type", &self.completion_type)
            .field("parent", &self.parent.lock().is_some())
            .finish()
    }
}

/// Waits for a set of child completions. The group finishes when every
/// child has finished and `build` has been called.
///
/// The group counts how many things are still outstanding. It starts at 1:
/// a token held by the builder, released by `build`. Each `add` counts one
/// more. That token keeps the group from finishing while children are still
/// being added, even if the ones added so far finish right away.
pub struct CompletionGroup {
    completions: Vec<Completion>,
    /// The group's own completion, handed out by `build`.
    completion: Completion,
    inner: Arc<GroupCompletionInner>,
}

impl fmt::Debug for CompletionGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionGroup")
            .field("children", &self.completions.len())
            .field(
                "outstanding",
                &self.inner.outstanding.load(Ordering::SeqCst),
            )
            .finish()
    }
}

impl CompletionGroup {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        let inner = Arc::new(GroupCompletionInner {
            outstanding: AtomicUsize::new(1),
            members: Arc::new(Members::default()),
            complete: Box::new(callback),
            result: OnceLock::new(),
        });
        let completion = Completion::new(CompletionType::Group(GroupCompletion {
            inner: inner.clone(),
        }));
        Self {
            completions: Vec::new(),
            completion,
            inner,
        }
    }

    /// Add a child. Call this before the child is submitted to the IO
    /// backend. The child's own callback is what counts it into the group,
    /// so the link has to be there before the child can finish.
    pub fn add(&mut self, c: &Completion) {
        turso_assert!(
            !c.is_engine_wait(),
            "an engine's wait added to a group: it is finished only by its maker"
        );
        self.completions.push(c.clone());
        self.inner.outstanding.fetch_add(1, Ordering::SeqCst);
        let mut parent = c.get_inner().parent.lock();
        turso_assert!(parent.is_none(), "completion can only be linked once");
        *parent = Some(self.completion.clone());
        drop(parent);
        turso_assert!(
            !c.finished(),
            "completion was added to a group after it finished"
        );
    }

    /// The children added so far. Used by error paths that need to
    /// wait on the kernel side via `IO::drain_completions` after
    /// cancelling the group.
    pub fn completions(&self) -> &[Completion] {
        &self.completions
    }

    pub fn len(&self) -> usize {
        self.completions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.completions.is_empty()
    }

    pub fn cancel(&self) {
        for c in &self.completions {
            c.abort();
        }
    }

    /// Release the builder's token. The group finishes now if every child
    /// has already finished, or later when the last one does.
    pub fn build(self) -> Completion {
        // Published before the token goes: its release may finish the group at once, and
        // whatever the group's end runs may walk it.
        let members = self
            .completions
            .iter()
            .map(|c| match &c.get_inner().completion_type {
                CompletionType::Group(group) => Member::Group(group.inner.members.clone()),
                _ => Member::Request(Arc::downgrade(c.get_inner())),
            })
            .collect();
        turso_assert!(
            self.inner.members.0.set(members).is_ok(),
            "a group's members published twice"
        );
        self.completion.group_one_done(None);
        self.completion
    }
}

pub struct GroupCompletion {
    inner: Arc<GroupCompletionInner>,
}

impl fmt::Debug for GroupCompletion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupCompletion")
            .field(
                "outstanding",
                &self.inner.outstanding.load(Ordering::SeqCst),
            )
            .finish()
    }
}

struct GroupCompletionInner {
    /// Number of completions that need to finish
    outstanding: AtomicUsize,
    /// What the group lists, which its ownership is the aggregate of.
    members: Arc<Members>,
    /// Callback to invoke when all completions finish
    complete: Box<dyn Fn(Result<i32, CompletionError>) + Send + Sync>,
    /// Cached result after all completions finish
    result: OnceLock<Option<CompletionError>>,
}

/// What a group lists, published once as it is built (the auditor's frozen-group-members-v1, P1-GW1)
/// and walked down to the requests beneath it (DESIGN.md 22.5) with no lock: a request upgraded in
/// a walk may be the last handle to it, and its drop may run a callback's captures, which may walk
/// the group again. A request is held
/// weakly: a backend that owns one holds it, so a request gone is owned by no one, and a strong hold
/// would keep its buffers past its retirement. A group is no backend's, so a request beneath it can
/// be owned after every handle to the group is gone (its last request cancelled, its parent's
/// link to it dropped as it finished): a group's list is held strongly, never the group's
/// completion. Lists point only down and hold no completion, so they keep no buffer and make no
/// cycle.
#[derive(Default)]
struct Members(OnceLock<Vec<Member>>);

enum Member {
    Request(Weak<CompletionInner>),
    Group(Arc<Members>),
}

impl GroupCompletion {
    pub fn callback(&self, result: Result<i32, CompletionError>) {
        turso_assert_eq!(
            self.inner.outstanding.load(Ordering::SeqCst),
            0,
            "callback called before all completions finished"
        );
        (self.inner.complete)(result);
    }
}

impl Debug for CompletionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(..) => f.debug_tuple("Read").finish(),
            Self::Write(..) => f.debug_tuple("Write").finish(),
            Self::Sync(..) => f.debug_tuple("Sync").finish(),
            Self::Truncate(..) => f.debug_tuple("Truncate").finish(),
            Self::Group(..) => f.debug_tuple("Group").finish(),
            Self::Yield => f.debug_tuple("Yield").finish(),
            Self::Remote => f.debug_tuple("Remote").finish(),
        }
    }
}

pub enum CompletionType {
    Read(ReadCompletion),
    Write(WriteCompletion),
    Sync(SyncCompletion),
    Truncate(TruncateCompletion),
    Group(GroupCompletion),
    Yield,
    /// A wait that another thread completes, never an IO step: a waiter may block on it.
    Remote,
}

impl CompletionInner {
    /// Runs the wakers registered: in place for a plain completion, through its dispatch for an
    /// engine's wait, taken under the lock either way.
    fn run_wakers(&self) {
        match self.engine {
            Some(dispatch) => dispatch(self.context.take()),
            None => self.context.wake(),
        }
    }

    fn new(completion_type: CompletionType) -> Self {
        Self {
            completion_type,
            ownership: AtomicU8::new(NOT_SUBMITTED),
            result: OnceLock::new(),
            finishing: AtomicBool::new(false),
            context: ContextState::new(),
            parent: Mutex::new(None),
            write_buffer: OnceLock::new(),
            engine: None,
        }
    }
}

impl Completion {
    pub fn new(completion_type: CompletionType) -> Self {
        Self {
            inner: Some(Arc::new(CompletionInner::new(completion_type))),
        }
    }

    pub(super) fn get_inner(&self) -> &Arc<CompletionInner> {
        self.inner
            .as_ref()
            .expect("completion inner should be initialized")
    }

    /// Stores a write buffer reference in the completion to keep it alive
    /// until the I/O completes. Required for async backends (io_uring, VFS)
    /// where pwrite returns before the kernel has consumed the buffer.
    pub fn keep_write_buffer_alive(&self, buf: Arc<Buffer>) {
        self.get_inner()
            .write_buffer
            .set(buf)
            .expect("write buffer should only be set once");
    }

    pub fn new_write<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        Self::new(CompletionType::Write(WriteCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_read<F>(buf: Arc<Buffer>, complete: F) -> Self
    where
        F: Fn(Result<(Arc<Buffer>, i32), CompletionError>) -> Option<CompletionError>
            + Send
            + Sync
            + 'static,
    {
        Self::new(CompletionType::Read(ReadCompletion::new(
            buf,
            Box::new(complete),
        )))
    }
    pub fn new_sync<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        Self::new(CompletionType::Sync(SyncCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_trunc<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        Self::new(CompletionType::Truncate(TruncateCompletion::new(Box::new(
            complete,
        ))))
    }

    /// Create a yield completion. These are completed by default allowing to yield control without
    /// allocating memory.
    pub fn new_yield() -> Self {
        Self { inner: None }
    }

    /// A completion no IO backend finishes: whoever the caller waits for
    /// calls `complete` on it.
    pub fn new_wait() -> Self {
        Self::new(CompletionType::Yield)
    }

    /// A wait that another thread completes, never an IO step on this one: the end of another
    /// transaction, or a timer. A synchronous waiter blocks on it ([`block_on`](Self::block_on)).
    pub fn new_remote_wait() -> Self {
        Self::new(CompletionType::Remote)
    }

    /// A wait the engine makes, of the stepped kind ([`new_wait`]): whoever finishes or nudges it
    /// (its maker through [`finish_deferred`](Self::finish_deferred), `complete`, `error`,
    /// `abort`, `wake` or a late registration), its wakers go to `dispatch`, never run in place.
    /// Never added to a group.
    pub fn new_engine_wait(dispatch: Dispatch) -> Self {
        Self::engine(CompletionType::Yield, dispatch)
    }

    /// A wait the engine makes and finishes itself, of the remote kind ([`new_remote_wait`]).
    pub fn new_engine_remote_wait(dispatch: Dispatch) -> Self {
        Self::engine(CompletionType::Remote, dispatch)
    }

    fn engine(completion_type: CompletionType, dispatch: Dispatch) -> Self {
        let mut inner = CompletionInner::new(completion_type);
        inner.engine = Some(dispatch);
        Self {
            inner: Some(Arc::new(inner)),
        }
    }

    /// Whether another thread completes this wait, so that a waiter may block on it.
    pub fn is_remote_wait(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| matches!(inner.completion_type, CompletionType::Remote))
    }

    /// This completion as a raw pointer holding its reference, for a lock-free cell that hands
    /// it to other threads; null for a yield.
    pub(crate) fn into_raw(self) -> *const () {
        self.inner
            .map_or(std::ptr::null(), |inner| Arc::into_raw(inner).cast())
    }

    /// The pointer kept alive by this handle, without handing over its reference.
    pub(crate) fn as_raw(&self) -> *const () {
        self.inner
            .as_ref()
            .map_or(std::ptr::null(), |inner| Arc::as_ptr(inner).cast())
    }

    /// The completion behind `raw`, with the reference it held.
    ///
    /// # Safety
    ///
    /// `raw` came from [`into_raw`](Self::into_raw), and its reference is handed back here once.
    pub(crate) unsafe fn from_raw(raw: *const ()) -> Self {
        Self {
            // SAFETY: as the caller promises.
            inner: (!raw.is_null())
                .then(|| unsafe { Arc::from_raw(raw.cast::<CompletionInner>()) }),
        }
    }

    /// A new reference to the completion behind `raw`, which keeps its own.
    ///
    /// # Safety
    ///
    /// `raw` came from [`into_raw`](Self::into_raw), and its reference is still held.
    pub(crate) unsafe fn clone_raw(raw: *const ()) -> Self {
        if !raw.is_null() {
            // SAFETY: as the caller promises, the reference `raw` holds keeps it alive.
            unsafe { Arc::increment_strong_count(raw.cast::<CompletionInner>()) };
        }
        // SAFETY: the reference just taken.
        unsafe { Self::from_raw(raw) }
    }

    /// The address of the completion's shared state: the same for every handle to one completion,
    /// none for a yield. An identity, never dereferenced.
    pub(crate) fn addr(&self) -> Option<usize> {
        self.inner.as_ref().map(|inner| Arc::as_ptr(inner).addr())
    }

    /// Whether `other` is this same completion, not merely an equal one.
    pub(crate) fn ptr_eq(&self, other: &Completion) -> bool {
        match (&self.inner, &other.inner) {
            (Some(this), Some(that)) => Arc::ptr_eq(this, that),
            _ => false,
        }
    }

    pub fn is_wait(&self) -> bool {
        self.inner.as_ref().is_some_and(|inner| {
            matches!(
                inner.completion_type,
                CompletionType::Yield | CompletionType::Remote
            )
        })
    }

    pub fn wake(&self) {
        if let Some(inner) = &self.inner {
            inner.run_wakers();
        }
    }

    /// Fire a "progress wake" without consuming the completion's result —
    /// wakes the waker on this completion *and* on its parent group, if any.
    /// Used by IO backends to nudge the future when an operation made progress
    /// but isn't fully done (e.g. an io_uring writev that completed only the
    /// first chunk and was resubmitted internally). Without this, a poll
    /// whose drained CQEs are all intermediate-chunk completions returns
    /// without waking anything, and since `step()` is the only thing that
    /// drains the CQ, the resubmitted chunks pile up and the task deadlocks.
    pub fn wake_progress(&self) {
        if let Some(inner) = &self.inner {
            let group = inner.parent.lock().clone();
            if let Some(group) = group {
                group.wake();
            }
            inner.run_wakers();
        }
    }

    /// Blocks the calling thread until the completion finishes: for a [remote
    /// wait](Self::new_remote_wait), which another thread completes and no IO step does, so the
    /// thread neither spins on an IO step with nothing to do nor sleeps past the finish. A waker
    /// unparks it; an unpark before the park leaves the thread's token, so no wake is lost.
    pub fn block_on(&self) {
        struct Unpark(crate::thread::Thread);
        impl std::task::Wake for Unpark {
            fn wake(self: std::sync::Arc<Self>) {
                self.0.unpark();
            }
            fn wake_by_ref(self: &std::sync::Arc<Self>) {
                self.0.unpark();
            }
        }
        super::refuse_on_delivery_thread("a blocking wait");
        if self.finished() {
            return;
        }
        let waker = Waker::from(std::sync::Arc::new(Unpark(crate::thread::current())));
        loop {
            // A progress wake consumes the waker before the completion finishes.
            self.set_waker(&waker);
            if self.finished() {
                return;
            }
            crate::thread::park();
        }
    }

    /// Registers `waker` under `key` ([`WakerKey`]), at most once per key, beside the wakers of
    /// the other tasks waiting: for a waiter that withdraws it exactly. Before testing for
    /// completion, as [`set_waker`](Self::set_waker).
    pub fn set_keyed_waker(&self, key: WakerKey, waker: &Waker) {
        match &self.inner {
            None => waker.wake_by_ref(),
            Some(inner) => {
                inner.context.set_keyed_waker(key, waker);
                crate::race::point("completion.registered");
                if self.finished() {
                    inner.run_wakers();
                }
            }
        }
    }

    /// Withdraws the waker registered under `key` from those the completion wakes as it finishes:
    /// a subscription its waiter no longer needs.
    pub fn withdraw_key(&self, key: WakerKey) {
        if let Some(inner) = &self.inner {
            inner.context.withdraw_key(key);
        }
    }

    /// Whether the completion's registry lock is held now, for a test that a waker's clone and
    /// drop run outside it.
    #[cfg(test)]
    pub fn registry_held(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.context.inner.try_lock().is_none())
    }

    /// How many wakers wait on the completion, for a test of a bound.
    #[cfg(test)]
    pub fn wakers(&self) -> usize {
        self.inner
            .as_ref()
            .map_or(0, |inner| inner.context.inner.lock().wakers.len())
    }

    /// How many handles share this completion: 1 when the caller's is the only one. For a holder
    /// that keeps a handle to tell when every other has been dropped.
    pub fn handles(&self) -> usize {
        self.inner.as_ref().map_or(0, Arc::strong_count)
    }

    /// Registers the waker before testing for completion: a completion finished on another thread
    /// between a test and the registration would otherwise wake no one.
    pub fn set_waker(&self, waker: &Waker) {
        match &self.inner {
            None => waker.wake_by_ref(),
            Some(inner) => {
                inner.context.set_waker(waker);
                crate::race::point("completion.registered");
                if self.finished() {
                    inner.run_wakers();
                }
            }
        }
    }

    pub fn succeeded(&self) -> bool {
        match &self.inner {
            Some(inner) => inner.result.get().is_some_and(|e| e.is_none()),
            None => true,
        }
    }

    pub fn failed(&self) -> bool {
        match &self.inner {
            Some(inner) => inner.result.get().is_some_and(|val| val.is_some()),
            None => false,
        }
    }

    pub fn get_error(&self) -> Option<CompletionError> {
        match &self.inner {
            Some(inner) => inner.result.get().and_then(|res| *res),
            None => None,
        }
    }

    /// Checks if the Completion completed or errored
    pub fn finished(&self) -> bool {
        match &self.inner {
            Some(inner) => inner.result.get().is_some(),
            None => true,
        }
    }

    /// Returns true if this completion is an explicit yield — a signal to
    /// return control to the cooperative scheduler so other connections can make
    /// progress. Unlike real I/O completions that happen to be finished,
    /// yield completions must not be treated as "ready to continue immediately"
    /// because the yielding operation is waiting on external state (e.g. a lock
    /// held by another fiber) that can only change when other fibers are stepped.
    pub fn is_explicit_yield(&self) -> bool {
        self.inner.is_none()
    }

    pub fn complete(&self, result: i32) {
        let result = Ok(result);
        self.callback(result);
    }

    pub fn error(&self, err: CompletionError) {
        let result = Err(err);
        self.callback(result);
    }

    pub fn abort(&self) {
        self.error(CompletionError::Aborted);
    }

    /// Whether this is a wait the engine makes and finishes itself: one
    /// [`finish_deferred`](Self::finish_deferred) takes. Read from a mark set at making.
    pub fn is_engine_wait(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.engine.is_some())
    }

    /// Finishes a wait the engine made ([`new_engine_wait`](Self::new_engine_wait),
    /// [`new_engine_remote_wait`](Self::new_engine_remote_wait)): publishes `result` and hands back
    /// its wakers without running them, for a caller that publishes several results before any
    /// waker runs (DESIGN.md 9.1.3). Anything else (a request, a group, any other wait) is refused:
    /// a request's callback and a group's count must run where they finish, and an engine's wait is
    /// never in a group, which `CompletionGroup::add` refuses.
    pub fn finish_deferred(&self, result: Result<i32, CompletionError>) -> Wakers {
        let inner = self.get_inner();
        turso_assert!(
            inner.engine.is_some(),
            "a deferred finish of a completion the engine did not make as its wait"
        );
        // The one finish, as `callback` claims it: a finish already under way publishes and wakes
        // itself, so there is nothing to hand back.
        if inner.finishing.swap(true, Ordering::AcqRel) {
            return Wakers(SmallVec::new());
        }
        let published = inner.result.set(result.err()).is_ok();
        turso_assert!(published, "a completion's result published twice");
        crate::race::point("completion.published");
        Wakers(std::mem::take(&mut inner.context.inner.lock().wakers))
    }

    /// Whether this is a request a backend carries out (a read, a write, a sync, a truncation),
    /// rather than a group or a wait.
    fn is_request(&self) -> bool {
        self.inner.as_ref().is_some_and(|inner| {
            matches!(
                inner.completion_type,
                CompletionType::Read(_)
                    | CompletionType::Write(_)
                    | CompletionType::Sync(_)
                    | CompletionType::Truncate(_)
            )
        })
    }

    /// A backend takes the request: from now until [`retire`](Self::retire) it owns the request
    /// and every buffer the request reads into or writes from, whoever finishes it meanwhile. A
    /// backend that holds a request past its submission's return accepts it before returning; one
    /// that hands an accepted request on to another leaves it accepted, for the one that delivers
    /// it to retire.
    pub fn accept(&self) {
        turso_assert!(self.is_request(), "only a request is submitted");
        // Kept beside the acceptance: a callback a cancellation already ran ([`CALLED`]).
        let was = self
            .get_inner()
            .ownership
            .fetch_or(ACCEPTED, Ordering::AcqRel);
        turso_assert!(was & RETIRED == 0, "a retired request submitted again");
    }

    /// A submission that accepted the request gives it back whole, its callback not run: nothing
    /// owns it, as before it was submitted.
    pub fn refuse(&self) {
        turso_assert!(!self.finished(), "a request refused after it was delivered");
        let was = self
            .get_inner()
            .ownership
            .swap(NOT_SUBMITTED, Ordering::AcqRel);
        turso_assert!(
            was & ACCEPTED != 0,
            "a request refused that was not accepted"
        );
        if was & OBSERVED != 0 {
            self.let_go_observers();
        }
    }

    /// The backend that delivered the request lets it go, and every buffer with it: inline at the
    /// submission's return, a queueing backend once it delivered it, a carrying layer once the
    /// request it carried retired. The request retires once its callback has also returned: now,
    /// or, while another finish still runs it, by that finish as it returns. What waited for the
    /// retirement runs then.
    pub fn retire(&self) {
        turso_assert!(self.is_request(), "only a request is retired");
        turso_assert!(
            self.get_inner().finishing.load(Ordering::Acquire),
            "a request retired before any finish ran its callback"
        );
        self.ended();
    }

    /// Delivers the request's result, running its callback, and retires it: a backend's delivery.
    /// A delivery that runs the callback retires as it returns, with nothing to meet. One that
    /// finds another finish running it (a cancellation) leaves the retirement to whichever of the
    /// two ends second; it neither waits for that callback nor retires before it.
    pub fn deliver(&self, result: Result<i32, CompletionError>) {
        turso_assert!(self.is_request(), "only a request is delivered");
        if self.run_callback(result) {
            self.retire_now();
        } else {
            self.ended();
        }
    }

    /// The backend's end of a request whose callback a finish has claimed: retires it if that
    /// callback returned, or marks the end for its finish to retire it as it returns.
    fn ended(&self) {
        let ownership = &self.get_inner().ownership;
        // Already returned: its finish saw no end and will not retire, so this one does.
        if ownership.load(Ordering::Acquire) & CALLED != 0 {
            return self.retire_now();
        }
        let was = ownership.fetch_or(ENDED, Ordering::AcqRel);
        turso_assert!(was & (RETIRED | ENDED) == 0, "a request retired twice");
        if was & CALLED != 0 {
            self.retire_now();
        }
    }

    /// A finish other than the delivery ran the request's callback, which returned: retires the
    /// request if the backend already let it go, or marks the return for the backend's end.
    fn called(&self) {
        let was = self
            .get_inner()
            .ownership
            .fetch_or(CALLED, Ordering::AcqRel);
        if was & ENDED != 0 {
            self.retire_now();
        }
    }

    /// Ends the ownership, the backend's end and the callback's return both reached; what waited
    /// for it runs.
    fn retire_now(&self) {
        let was = self.get_inner().ownership.swap(RETIRED, Ordering::AcqRel);
        turso_assert!(was != RETIRED, "a request retired twice");
        if was & OBSERVED != 0 {
            self.let_go_observers();
        }
    }

    /// Carries this request out through another, the one `submit` submits, whose callback
    /// finishes this one: a layer between a caller and a backend that reads into a buffer of its
    /// own, or checks what it read, before it finishes the caller's request. This request is
    /// accepted before the submission, given back with it if it is refused, and retired once the
    /// other retires, so a wait for its retirement waits for the backend's.
    pub fn carry(
        &self,
        submit: impl FnOnce() -> crate::Result<Completion>,
    ) -> crate::Result<Completion> {
        self.accept();
        match submit() {
            Ok(inner) => {
                let me = self.clone();
                inner.on_retired(move || me.retire());
                Ok(inner)
            }
            Err(e) => {
                self.refuse();
                Err(e)
            }
        }
    }

    /// Whether a backend owns the request: accepted and not yet retired.
    pub fn is_owned(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.ownership.load(Ordering::Acquire) & ACCEPTED != 0)
    }

    /// Runs `f` once no backend owns the request: now, if none does (never submitted, given back,
    /// or retired), or as the backend that owns it lets it go. An unowned request is read on the
    /// ownership word alone, without its context's lock: Acquire, so `f` sees what the retirement
    /// released, and a request its backend delivered inline costs its observer no lock.
    pub fn on_retired(&self, f: impl FnOnce() + Send + 'static) {
        let inner = self.get_inner();
        if inner.ownership.load(Ordering::Acquire) & ACCEPTED == 0 {
            f();
            return;
        }
        let mut context = inner.context.inner.lock();
        let mut state = inner.ownership.load(Ordering::Acquire);
        while state & ACCEPTED != 0 {
            match inner.ownership.compare_exchange(
                state,
                state | OBSERVED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    context.on_retired.push(Box::new(f));
                    return;
                }
                Err(now) => state = now,
            }
        }
        drop(context);
        f();
    }

    /// Takes what waited for the retirement, under the lock its registrations hold, and runs it
    /// once the lock is free.
    fn let_go_observers(&self) {
        let observers = std::mem::take(&mut self.get_inner().context.inner.lock().on_retired);
        for f in observers {
            f();
        }
    }

    /// Every request this completion stands for, each once across calls sharing `seen`: itself,
    /// or for a group every request beneath it, its nested groups walked down whatever their own
    /// completions' state and whether anyone still holds them ([`Members`]). A request gone is
    /// owned by no one, and left out. Everything walked existed before the walk began (a drain
    /// is made after its caller stopped submitting), so no address in `seen` is reused meanwhile.
    pub fn requests(&self, seen: &mut std::collections::HashSet<usize>) -> Vec<Completion> {
        let mut found = Vec::new();
        let mut lists = Vec::new();
        let mut request = |inner: Arc<CompletionInner>, found: &mut Vec<Completion>| {
            if !matches!(
                inner.completion_type,
                CompletionType::Yield | CompletionType::Remote
            ) && seen.insert(Arc::as_ptr(&inner) as usize)
            {
                found.push(Completion { inner: Some(inner) });
            }
        };
        match self.inner.as_ref().map(|inner| &inner.completion_type) {
            Some(CompletionType::Group(group)) => lists.push(group.inner.members.clone()),
            Some(_) => request(self.get_inner().clone(), &mut found),
            None => {}
        }
        let mut walked = std::collections::HashSet::new();
        while let Some(list) = lists.pop() {
            if !walked.insert(Arc::as_ptr(&list) as usize) {
                continue;
            }
            for member in list.0.get().expect("a group walked before it was built") {
                match member {
                    Member::Group(list) => lists.push(list.clone()),
                    Member::Request(weak) => {
                        if let Some(inner) = weak.upgrade() {
                            // The copy may be the last: its drop runs inside the walk.
                            crate::race::point("completions.member_upgraded");
                            request(inner, &mut found);
                        }
                    }
                }
            }
        }
        found
    }

    /// A finish other than a backend's delivery: runs the callback if no finish has, and for a
    /// request meets the backend's end of it ([`called`](Self::called)).
    fn callback(&self, result: Result<i32, CompletionError>) {
        if self.run_callback(result) && self.is_request() {
            self.called();
        }
    }

    /// Runs the callback, publishes the result and runs the wakers, if this is the first finish:
    /// whether it was.
    fn run_callback(&self, result: Result<i32, CompletionError>) -> bool {
        let inner = self.get_inner();
        // The first finish claims the completion and runs its callback outside any lock: a finish
        // the callback or a waker it runs makes (a cancellation of this completion or of a group
        // above it), or one another thread makes meanwhile, returns here without waiting. The
        // result is published once the callback has run, so `finished` still means it ran.
        if inner.finishing.swap(true, Ordering::AcqRel) {
            return false;
        }
        {
            // Run the type-specific callback. For ReadCompletion, this returns
            // an optional error detected by the callback (e.g., short read).
            let callback_error = match &inner.completion_type {
                CompletionType::Read(r) => r.callback(result),
                CompletionType::Write(w) => {
                    w.callback(result);
                    None
                }
                CompletionType::Sync(s) => {
                    s.callback(result);
                    None
                }
                CompletionType::Truncate(t) => {
                    t.callback(result);
                    None
                }
                CompletionType::Group(g) => {
                    g.callback(result);
                    None
                }
                CompletionType::Yield | CompletionType::Remote => None,
            };

            // Use callback error if present, otherwise use the original IO error
            let published = inner
                .result
                .set(callback_error.or_else(|| result.err()))
                .is_ok();
            turso_assert!(published, "a completion's result published twice");
        }
        // Only the call that finished this completion counts it into its group.
        let parent = inner.parent.lock().take();
        if let Some(group) = parent {
            group.group_one_done(inner.result.get().and_then(|e| *e));
        }
        crate::race::point("completion.published");
        inner.run_wakers();
        true
    }

    fn group_one_done(&self, err: Option<CompletionError>) {
        let group = match &self.get_inner().completion_type {
            CompletionType::Group(group) => &group.inner,
            _ => unreachable!("parent must be a group completion"),
        };
        if let Some(err) = err {
            let _ = group.result.set(Some(err));
        }
        let prev = group.outstanding.fetch_sub(1, Ordering::SeqCst);
        turso_assert!(prev > 0, "completion group counted below zero");
        if prev > 1 {
            self.wake();
            return;
        }
        let _ = group.result.set(None);
        let result = group.result.get().and_then(|e| *e);
        self.callback(result.map_or(Ok(0), Err));
    }

    /// only call this method if you are sure that the completion is
    /// a ReadCompletion, panics otherwise
    pub fn as_read(&self) -> &ReadCompletion {
        let inner = self.get_inner();
        match inner.completion_type {
            CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        }
    }
}

pub struct ReadCompletion {
    pub buf: Arc<Buffer>,
    pub complete: Box<ReadComplete>,
}

impl ReadCompletion {
    pub fn new(buf: Arc<Buffer>, complete: Box<ReadComplete>) -> Self {
        Self { buf, complete }
    }

    pub fn buf(&self) -> &Buffer {
        &self.buf
    }

    pub fn callback(&self, bytes_read: Result<i32, CompletionError>) -> Option<CompletionError> {
        (self.complete)(bytes_read.map(|b| (self.buf.clone(), b)))
    }

    pub fn buf_arc(&self) -> Arc<Buffer> {
        self.buf.clone()
    }
}

pub struct WriteCompletion {
    pub complete: Box<WriteComplete>,
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, bytes_written: Result<i32, CompletionError>) {
        (self.complete)(bytes_written);
    }
}

pub struct SyncCompletion {
    pub complete: Box<SyncComplete>,
}

impl SyncCompletion {
    pub fn new(complete: Box<SyncComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, res: Result<i32, CompletionError>) {
        (self.complete)(res);
    }
}

pub struct TruncateCompletion {
    pub complete: Box<TruncateComplete>,
}

impl TruncateCompletion {
    pub fn new(complete: Box<TruncateComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, res: Result<i32, CompletionError>) {
        (self.complete)(res);
    }
}

#[cfg(test)]
mod tests {
    use crate::CompletionError;

    use super::*;

    /// What a request's callback owns: dropped with the request, it walks the group from another
    /// thread and says whether that walk came back while it waited.
    struct WalksItsGroup {
        root: Arc<std::sync::OnceLock<Completion>>,
        returned: Arc<std::sync::atomic::AtomicBool>,
        peer: Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>,
        bound: std::time::Duration,
    }

    impl Drop for WalksItsGroup {
        fn drop(&mut self) {
            let root = self.root.get().expect("the group was built").clone();
            let (done, finished) = std::sync::mpsc::channel();
            let peer = std::thread::spawn(move || {
                let _ = root.requests(&mut std::collections::HashSet::new());
                let _ = done.send(());
            });
            let came_back = finished.recv_timeout(self.bound).is_ok();
            self.returned
                .store(came_back, std::sync::atomic::Ordering::Release);
            // A walk that waited is let go once this returns, and joined by the test.
            *self.peer.lock().unwrap() = Some(peer);
        }
    }

    /// A walk of a group's members holds no lock: a request it upgrades may be the last handle to it,
    /// and its drop may run captures that walk the group again, here from another thread while the
    /// first walk waits for it (the auditor's P1-GW1; mutant: the members behind a mutex, which the
    /// capture's walk then waits on until the first walk lets it go).
    #[test]
    fn a_group_walk_dropping_a_requests_last_handle_lets_its_captures_walk_the_group() {
        use std::sync::{atomic::AtomicBool, mpsc};
        use std::time::Duration;
        let bound = Duration::from_secs(if cfg!(miri) { 60 } else { 2 });
        let root = Arc::new(std::sync::OnceLock::new());
        let returned = Arc::new(AtomicBool::new(false));
        let peer = Arc::new(std::sync::Mutex::new(None));
        let capture = WalksItsGroup {
            root: root.clone(),
            returned: returned.clone(),
            peer: peer.clone(),
            bound,
        };
        let child = Completion::new_sync(move |_| {
            std::hint::black_box(&capture);
        });
        let mut group = CompletionGroup::new(|_| {});
        group.add(&child);
        let group = group.build();
        assert!(root.set(group.clone()).is_ok());
        // A walk that has seen the child already drops the copy it upgrades inside the walk.
        let mut seen = std::collections::HashSet::new();
        drop(child.requests(&mut seen));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let scanner = {
            let group = group.clone();
            std::thread::spawn(move || {
                crate::race::at("completions.member_upgraded", move || {
                    entered_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                });
                group.requests(&mut seen).len()
            })
        };
        let reached = entered_rx.recv_timeout(Duration::from_secs(5)).is_ok();
        // The walk's copy is now the last handle to the child.
        drop(child);
        go_tx.send(()).unwrap();
        let found = scanner.join();
        let walked = peer
            .lock()
            .unwrap()
            .take()
            .expect("the capture walked its group")
            .join();
        let came_back = returned.load(std::sync::atomic::Ordering::Acquire);
        drop((group, root));
        assert!(reached, "the walk reached the child");
        assert!(matches!(found, Ok(0)) && walked.is_ok());
        assert!(
            came_back,
            "a capture's walk of its group waited for the walk that dropped it"
        );
    }

    /// A group's members are published before the builder lets its token go: that release may finish
    /// the group at once, and its callback may walk it (mutant: the token let go first, so the walk
    /// finds no members).
    #[test]
    fn a_group_publishes_its_members_before_its_builder_lets_its_token_go() {
        use std::sync::atomic::{AtomicUsize, Ordering as O};
        let root = Arc::new(std::sync::Mutex::new(None::<Completion>));
        let walked = Arc::new(AtomicUsize::new(usize::MAX));
        let mut group = {
            let (root, walked) = (root.clone(), walked.clone());
            CompletionGroup::new(move |_| {
                let root = root.lock().unwrap().take().expect("the group's handle");
                let found = root.requests(&mut std::collections::HashSet::new()).len();
                walked.store(found, O::Release);
            })
        };
        let child = Completion::new_sync(|_| {});
        group.add(&child);
        *root.lock().unwrap() = Some(group.completion.clone());
        child.accept();
        child.deliver(Ok(0));
        let group = group.build();
        let found = walked.load(O::Acquire);
        let finished = group.finished();
        drop((child, group, root));
        assert!(finished, "the group finished at its build");
        assert_eq!(found, 1, "the group's callback found its members");
    }

    /// A request no backend owns, never submitted or already retired, runs what waits for its
    /// retirement at once, on its ownership word alone: its context's lock is held by the test
    /// throughout (the auditor's retired-observer-fast-v1; mutant: the lock taken first, and the
    /// observer waits for it).
    fn an_unowned_requests_observer_does_not_wait_for_its_context(retired: bool) {
        use std::sync::{atomic::AtomicUsize, atomic::Ordering, mpsc};
        use std::time::Duration;
        // Long past the few instructions the observer takes, Miri's included.
        let held_for = Duration::from_secs(if cfg!(miri) { 60 } else { 2 });
        let completion = Completion::new_sync(|_| {});
        if retired {
            completion.accept();
            completion.deliver(Ok(0));
        }
        let inner = completion.get_inner().clone();
        let held = inner.context.inner.lock();
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, entry) = mpsc::channel();
        let (done, finished) = mpsc::channel();
        let thread = {
            let calls = calls.clone();
            std::thread::spawn(move || {
                entered.send(()).unwrap();
                completion.on_retired(move || {
                    calls.fetch_add(1, Ordering::AcqRel);
                });
                let _ = done.send(());
            })
        };
        let began = entry.recv_timeout(Duration::from_secs(5)).is_ok();
        let returned_while_held = finished.recv_timeout(held_for).is_ok();
        drop(held);
        let joined = thread.join().is_ok();
        drop(inner);
        assert!(began && joined, "the observing thread did not complete");
        assert!(
            returned_while_held,
            "an unowned request waited for its context"
        );
        assert_eq!(calls.load(Ordering::Acquire), 1, "observers run");
    }

    #[test]
    fn an_observer_of_a_retired_request_does_not_wait_for_its_context() {
        an_unowned_requests_observer_does_not_wait_for_its_context(true);
    }

    #[test]
    fn an_observer_of_an_unsubmitted_request_does_not_wait_for_its_context() {
        an_unowned_requests_observer_does_not_wait_for_its_context(false);
    }

    #[test]
    fn group_finishes_when_child_completes_on_another_thread_during_build() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;
        // A thread per round. Miri checks every access and keeps every thread's state, so it runs
        // fewer rounds, over several schedule seeds, each preempting threads mid-operation.
        let rounds = if cfg!(miri) { 200 } else { 20_000 };
        for i in 0..rounds {
            let child = Completion::new_write(|_| {});
            let go = Arc::new(AtomicBool::new(false));
            let (c2, go2) = (child.clone(), go.clone());
            let t = thread::spawn(move || {
                while !go2.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                c2.complete(0);
            });
            let mut group = CompletionGroup::new(|_| {});
            group.add(&child);
            go.store(true, Ordering::Release);
            let g = group.build();
            t.join().unwrap();
            assert!(child.finished());
            assert!(
                g.finished(),
                "iteration {i}: child finished but the group did not"
            );
        }
    }

    #[test]
    fn group_callback_fires_once_when_children_finished_before_build() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        let calls2 = calls.clone();
        let mut group = CompletionGroup::new(move |_| {
            calls2.fetch_add(1, Ordering::SeqCst);
        });
        group.add(&c1);
        group.add(&c2);
        c1.complete(0);
        c2.complete(0);
        let g = group.build();
        assert!(g.finished());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn group_stays_pending_until_its_callback_returns() {
        use std::sync::{mpsc, Mutex};
        for child_error in [None, Some(CompletionError::Aborted)] {
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel::<()>();
            let entered_tx = Mutex::new(entered_tx);
            let release_rx = Mutex::new(release_rx);
            let mut group = CompletionGroup::new(move |_| {
                entered_tx.lock().unwrap().send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            });
            let child = Completion::new_write(|_| {});
            group.add(&child);
            let group = group.build();

            let child_clone = child.clone();
            let completer = std::thread::spawn(move || match child_error {
                Some(err) => child_clone.error(err),
                None => child_clone.complete(0),
            });
            entered_rx.recv().unwrap();
            assert!(!group.finished());
            assert!(!group.succeeded());
            assert!(!group.failed());
            assert!(group.get_error().is_none());

            release_tx.send(()).unwrap();
            completer.join().unwrap();
            assert!(group.finished());
            assert_eq!(group.succeeded(), child_error.is_none());
            assert_eq!(group.get_error(), child_error);
        }
    }

    #[test]
    #[should_panic(expected = "added to a group after it finished")]
    fn adding_a_finished_completion_panics() {
        let c = Completion::new_write(|_| {});
        c.complete(0);
        let mut group = CompletionGroup::new(|_| {});
        group.add(&c);
    }

    #[test]
    fn test_completion_group_empty() {
        use crate::sync::atomic::{AtomicBool, Ordering};

        let callback_called = Arc::new(AtomicBool::new(false));
        let callback_called_clone = callback_called.clone();

        let group = CompletionGroup::new(move |_| {
            callback_called_clone.store(true, Ordering::SeqCst);
        });
        let group = group.build();
        assert!(group.finished());
        assert!(group.succeeded());
        assert!(group.get_error().is_none());

        // Verify the callback was actually called
        assert!(
            callback_called.load(Ordering::SeqCst),
            "callback should be called for empty group"
        );
    }

    #[test]
    fn test_completion_group_single_completion() {
        let mut group = CompletionGroup::new(|_| {});
        let c = Completion::new_write(|_| {});
        group.add(&c);
        let group = group.build();

        assert!(!group.finished());
        assert!(!group.succeeded());

        c.complete(0);

        assert!(group.finished());
        assert!(group.succeeded());
        assert!(group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_multiple_completions() {
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        let c3 = Completion::new_write(|_| {});
        group.add(&c1);
        group.add(&c2);
        group.add(&c3);
        let group = group.build();

        assert!(!group.succeeded());
        assert!(!group.finished());

        c1.complete(0);
        assert!(!group.succeeded());
        assert!(!group.finished());

        c2.complete(0);
        assert!(!group.succeeded());
        assert!(!group.finished());

        c3.complete(0);
        assert!(group.succeeded());
        assert!(group.finished());
    }

    #[test]
    fn test_completion_group_with_error() {
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        group.add(&c1);
        group.add(&c2);
        let group = group.build();

        c1.complete(0);
        c2.error(CompletionError::Aborted);

        assert!(group.finished());
        assert!(!group.succeeded());
        assert_eq!(group.get_error(), Some(CompletionError::Aborted));
    }

    #[test]
    fn test_completion_group_callback() {
        use crate::sync::atomic::{AtomicBool, Ordering};
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        let mut group = CompletionGroup::new(move |_| {
            called_clone.store(true, Ordering::SeqCst);
        });

        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        group.add(&c1);
        group.add(&c2);
        let group = group.build();

        assert!(!called.load(Ordering::SeqCst));

        c1.complete(0);
        assert!(!called.load(Ordering::SeqCst));

        c2.complete(0);
        assert!(called.load(Ordering::SeqCst));
        assert!(group.finished());
        assert!(group.succeeded());
    }

    #[test]
    fn test_completion_group_some_already_completed() {
        // Test some completions added to group, then finish before build()
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        let c3 = Completion::new_write(|_| {});

        // Add all to group while pending
        group.add(&c1);
        group.add(&c2);
        group.add(&c3);

        // Complete c1 and c2 AFTER adding but BEFORE build()
        c1.complete(0);
        c2.complete(0);

        let group = group.build();

        // c1 and c2 finished before build(), so outstanding should account for them
        // Only c3 should be pending
        assert!(!group.finished());
        assert!(!group.succeeded());

        // Complete c3
        c3.complete(0);

        // Now the group should be finished
        assert!(group.finished());
        assert!(group.succeeded());
        assert!(group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_all_already_completed() {
        // Test when all completions are already finished before build()
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});

        group.add(&c1);
        group.add(&c2);

        // Complete both before build()
        c1.complete(0);
        c2.complete(0);

        let group = group.build();

        // All completions were already complete, so group should be finished immediately
        assert!(group.finished());
        assert!(group.succeeded());
        assert!(group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_mixed_finished_and_pending() {
        use crate::sync::atomic::{AtomicBool, Ordering};
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        let mut group = CompletionGroup::new(move |_| {
            called_clone.store(true, Ordering::SeqCst);
        });

        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        let c3 = Completion::new_write(|_| {});
        let c4 = Completion::new_write(|_| {});

        group.add(&c1);
        group.add(&c2);
        group.add(&c3);
        group.add(&c4);

        // Complete c1 and c3 before build()
        c1.complete(0);
        c3.complete(0);

        let group = group.build();

        // Only c2 and c4 should be pending
        assert!(!group.finished());
        assert!(!called.load(Ordering::SeqCst));

        c2.complete(0);
        assert!(!group.finished());
        assert!(!called.load(Ordering::SeqCst));

        c4.complete(0);
        assert!(group.finished());
        assert!(group.succeeded());
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_completion_group_already_completed_with_error() {
        // Test when a completion finishes with error before build()
        let mut group = CompletionGroup::new(|_| {});
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});

        group.add(&c1);
        group.add(&c2);

        // Fail c1 before build()
        c1.error(CompletionError::Aborted);

        let group = group.build();

        // The group waits for c2 even though c1 already failed: a waiter
        // must not carry on while a child's IO is still in flight.
        assert!(!group.finished());

        c2.complete(0);
        assert!(group.finished());
        assert!(!group.succeeded());
        assert_eq!(group.get_error(), Some(CompletionError::Aborted));
    }

    #[test]
    fn test_completion_group_nested() {
        use crate::sync::atomic::{AtomicUsize, Ordering};

        // Track callbacks at different levels
        let parent_called = Arc::new(AtomicUsize::new(0));
        let child1_called = Arc::new(AtomicUsize::new(0));
        let child2_called = Arc::new(AtomicUsize::new(0));

        // Create child group 1 with 2 completions
        let child1_called_clone = child1_called.clone();
        let mut child_group1 = CompletionGroup::new(move |_| {
            child1_called_clone.fetch_add(1, Ordering::SeqCst);
        });
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        child_group1.add(&c1);
        child_group1.add(&c2);
        let child_group1 = child_group1.build();

        // Create child group 2 with 2 completions
        let child2_called_clone = child2_called.clone();
        let mut child_group2 = CompletionGroup::new(move |_| {
            child2_called_clone.fetch_add(1, Ordering::SeqCst);
        });
        let c3 = Completion::new_write(|_| {});
        let c4 = Completion::new_write(|_| {});
        child_group2.add(&c3);
        child_group2.add(&c4);
        let child_group2 = child_group2.build();

        // Create parent group containing both child groups
        let parent_called_clone = parent_called.clone();
        let mut parent_group = CompletionGroup::new(move |_| {
            parent_called_clone.fetch_add(1, Ordering::SeqCst);
        });
        parent_group.add(&child_group1);
        parent_group.add(&child_group2);
        let parent_group = parent_group.build();

        // Initially nothing should be finished
        assert!(!parent_group.finished());
        assert!(!child_group1.finished());
        assert!(!child_group2.finished());
        assert_eq!(parent_called.load(Ordering::SeqCst), 0);
        assert_eq!(child1_called.load(Ordering::SeqCst), 0);
        assert_eq!(child2_called.load(Ordering::SeqCst), 0);

        // Complete first completion in child group 1
        c1.complete(0);
        assert!(!child_group1.finished());
        assert!(!parent_group.finished());
        assert_eq!(child1_called.load(Ordering::SeqCst), 0);
        assert_eq!(parent_called.load(Ordering::SeqCst), 0);

        // Complete second completion in child group 1 - should finish child group 1
        c2.complete(0);
        assert!(child_group1.finished());
        assert!(child_group1.succeeded());
        assert_eq!(child1_called.load(Ordering::SeqCst), 1);

        // Parent should not be finished yet because child group 2 is still pending
        assert!(!parent_group.finished());
        assert_eq!(parent_called.load(Ordering::SeqCst), 0);

        // Complete first completion in child group 2
        c3.complete(0);
        assert!(!child_group2.finished());
        assert!(!parent_group.finished());
        assert_eq!(child2_called.load(Ordering::SeqCst), 0);
        assert_eq!(parent_called.load(Ordering::SeqCst), 0);

        // Complete second completion in child group 2 - should finish everything
        c4.complete(0);
        assert!(child_group2.finished());
        assert!(child_group2.succeeded());
        assert_eq!(child2_called.load(Ordering::SeqCst), 1);

        // Parent should now be finished
        assert!(parent_group.finished());
        assert!(parent_group.succeeded());
        assert_eq!(parent_called.load(Ordering::SeqCst), 1);
        assert!(parent_group.get_error().is_none());
    }

    #[test]
    fn test_completion_group_nested_with_error() {
        use crate::sync::atomic::{AtomicBool, Ordering};

        let parent_called = Arc::new(AtomicBool::new(false));
        let child_called = Arc::new(AtomicBool::new(false));

        // Create child group with 2 completions
        let child_called_clone = child_called.clone();
        let mut child_group = CompletionGroup::new(move |_| {
            child_called_clone.store(true, Ordering::SeqCst);
        });
        let c1 = Completion::new_write(|_| {});
        let c2 = Completion::new_write(|_| {});
        child_group.add(&c1);
        child_group.add(&c2);
        let child_group = child_group.build();

        // Create parent group containing child group and another completion
        let parent_called_clone = parent_called.clone();
        let mut parent_group = CompletionGroup::new(move |_| {
            parent_called_clone.store(true, Ordering::SeqCst);
        });
        let c3 = Completion::new_write(|_| {});
        parent_group.add(&child_group);
        parent_group.add(&c3);
        let parent_group = parent_group.build();

        // Complete child group with success
        c1.complete(0);
        c2.complete(0);
        assert!(child_group.finished());
        assert!(child_group.succeeded());
        assert!(child_called.load(Ordering::SeqCst));

        // Parent still pending
        assert!(!parent_group.finished());
        assert!(!parent_called.load(Ordering::SeqCst));

        // Complete c3 with error
        c3.error(CompletionError::Aborted);

        // Parent should finish with error
        assert!(parent_group.finished());
        assert!(!parent_group.succeeded());
        assert_eq!(parent_group.get_error(), Some(CompletionError::Aborted));
        assert!(parent_called.load(Ordering::SeqCst));
    }

    // Tests for individual completion success/failure status

    #[test]
    fn test_write_completion_pending_status() {
        let c = Completion::new_write(|_| {});

        // Pending completion should not be finished, succeeded, or failed
        assert!(!c.finished());
        assert!(!c.succeeded());
        assert!(!c.failed());
        assert!(c.get_error().is_none());
    }

    #[test]
    fn test_write_completion_success() {
        let c = Completion::new_write(|_| {});

        c.complete(42);

        assert!(c.finished());
        assert!(c.succeeded());
        assert!(!c.failed());
        assert!(c.get_error().is_none());
    }

    #[test]
    fn test_write_completion_failure() {
        let c = Completion::new_write(|_| {});

        c.error(CompletionError::Aborted);

        assert!(c.finished());
        assert!(!c.succeeded());
        assert!(c.failed());
        assert_eq!(c.get_error(), Some(CompletionError::Aborted));
    }

    #[test]
    fn test_read_completion_pending_status() {
        let buf = Arc::new(crate::Buffer::new_temporary(4096));
        let c = Completion::new_read(buf, |_| None);

        assert!(!c.finished());
        assert!(!c.succeeded());
        assert!(!c.failed());
        assert!(c.get_error().is_none());
    }

    #[test]
    fn test_read_completion_success() {
        let buf = Arc::new(crate::Buffer::new_temporary(4096));
        let c = Completion::new_read(buf, |_| None);

        c.complete(1024);

        assert!(c.finished());
        assert!(c.succeeded());
        assert!(!c.failed());
        assert!(c.get_error().is_none());
    }

    #[test]
    fn test_read_completion_failure() {
        let buf = Arc::new(crate::Buffer::new_temporary(4096));
        let c = Completion::new_read(buf, |_| None);

        c.error(CompletionError::Aborted);

        assert!(c.finished());
        assert!(!c.succeeded());
        assert!(c.failed());
        assert_eq!(c.get_error(), Some(CompletionError::Aborted));
    }

    #[test]
    fn test_sync_completion_pending_status() {
        let c = Completion::new_sync(|_| {});

        assert!(!c.finished());
        assert!(!c.succeeded());
        assert!(!c.failed());
        assert!(c.get_error().is_none());
    }

    #[test]
    fn test_sync_completion_success() {
        let c = Completion::new_sync(|_| {});

        c.complete(0);

        assert!(c.finished());
        assert!(c.succeeded());
        assert!(!c.failed());
        assert!(c.get_error().is_none());
    }

    #[test]
    fn test_sync_completion_failure() {
        let c = Completion::new_sync(|_| {});

        c.error(CompletionError::Aborted);

        assert!(c.finished());
        assert!(!c.succeeded());
        assert!(c.failed());
        assert_eq!(c.get_error(), Some(CompletionError::Aborted));
    }

    #[test]
    fn test_truncate_completion_pending_status() {
        let c = Completion::new_trunc(|_| {});

        assert!(!c.finished());
        assert!(!c.succeeded());
        assert!(!c.failed());
        assert!(c.get_error().is_none());
    }

    #[test]
    fn test_truncate_completion_success() {
        let c = Completion::new_trunc(|_| {});

        c.complete(0);

        assert!(c.finished());
        assert!(c.succeeded());
        assert!(!c.failed());
        assert!(c.get_error().is_none());
    }

    #[test]
    fn test_truncate_completion_failure() {
        let c = Completion::new_trunc(|_| {});

        c.error(CompletionError::Aborted);

        assert!(c.finished());
        assert!(!c.succeeded());
        assert!(c.failed());
        assert_eq!(c.get_error(), Some(CompletionError::Aborted));
    }

    #[test]
    fn test_yield_completion_status() {
        let c = Completion::new_yield();

        // Yield completions are always considered finished and succeeded
        assert!(c.finished());
        assert!(c.succeeded());
        assert!(!c.failed());
        assert!(c.get_error().is_none());
    }

    #[test]
    fn test_completion_abort() {
        let c = Completion::new_write(|_| {});

        c.abort();

        assert!(c.finished());
        assert!(!c.succeeded());
        assert!(c.failed());
        assert_eq!(c.get_error(), Some(CompletionError::Aborted));
    }

    #[test]
    fn test_completion_callback_receives_success_result() {
        use crate::sync::atomic::{AtomicI32, Ordering};

        let result_value = Arc::new(AtomicI32::new(-1));
        let result_value_clone = result_value.clone();

        let c = Completion::new_write(move |res| {
            if let Ok(val) = res {
                result_value_clone.store(val, Ordering::SeqCst);
            }
        });

        c.complete(42);

        assert_eq!(result_value.load(Ordering::SeqCst), 42);
        assert!(c.succeeded());
    }

    #[test]
    fn test_completion_callback_receives_error_result() {
        use crate::sync::atomic::{AtomicBool, Ordering};

        let got_error = Arc::new(AtomicBool::new(false));
        let got_error_clone = got_error.clone();

        let c = Completion::new_write(move |res| {
            if res.is_err() {
                got_error_clone.store(true, Ordering::SeqCst);
            }
        });

        c.error(CompletionError::Aborted);

        assert!(got_error.load(Ordering::SeqCst));
        assert!(c.failed());
    }

    #[test]
    fn test_completion_idempotent_complete() {
        // Completing a completion multiple times should only trigger the callback once
        use crate::sync::atomic::{AtomicUsize, Ordering};

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let c = Completion::new_write(move |_| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        c.complete(1);
        c.complete(2);
        c.complete(3);

        // Callback should only be called once
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert!(c.succeeded());
    }

    #[test]
    fn test_completion_idempotent_error() {
        // Erroring a completion multiple times should only trigger the callback once
        use crate::sync::atomic::{AtomicUsize, Ordering};

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let c = Completion::new_write(move |_| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        c.error(CompletionError::Aborted);
        c.error(CompletionError::Aborted);
        c.complete(0); // Try completing after error

        // Callback should only be called once
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert!(c.failed());
    }

    /// A waker registered while another thread completes the completion must be woken, in every
    /// interleaving of the two, including a completion that finishes after `set_waker` tested it
    /// and before the waker was stored.
    #[cfg(shuttle)]
    #[test]
    fn shuttle_set_waker_racing_complete_always_wakes() {
        use std::task::Wake;

        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        shuttle::check_dfs(
            || {
                let c = Completion::new_wait();
                let count = Arc::new(Count(AtomicUsize::new(0)));
                let completer = {
                    let c = c.clone();
                    crate::thread::spawn(move || c.complete(0))
                };
                c.set_waker(&Waker::from(count.clone()));
                completer.join().unwrap();
                assert_ne!(
                    count.0.load(Ordering::SeqCst),
                    0,
                    "the completion finished without waking its waker"
                );
            },
            None,
        );
    }

    /// A group can wake a waiter before its last child finishes. After that progress wake,
    /// blocking again must register a waker for the final completion.
    #[cfg(shuttle)]
    #[test]
    fn shuttle_blocking_group_wait_registers_again_after_a_progress_wake() {
        shuttle::check_random_with_seed(
            || {
                let first = Completion::new_sync(|_| {});
                let last = Completion::new_sync(|_| {});
                let mut group = CompletionGroup::new(|_| {});
                group.add(&first);
                group.add(&last);
                let group = group.build();
                let waiter = {
                    let group = group.clone();
                    crate::thread::spawn(move || group.block_on())
                };
                first.complete(0);
                crate::thread::yield_now();
                last.complete(0);
                waiter.join().unwrap();
                assert!(group.succeeded());
            },
            0,
            1000,
        );
    }

    /// A waker that registers again on its completion from its wake, as polling the completion
    /// inline does, up to three times.
    struct Again {
        completion: Completion,
        wakes: AtomicUsize,
    }

    impl std::task::Wake for Again {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if self.wakes.fetch_add(1, Ordering::SeqCst) < 3 {
                self.completion.set_waker(&Waker::from(self.clone()));
            }
        }
    }

    /// `set_waker` on a finished completion wakes the waker with the completion's lock free: a
    /// waker may register again on that completion from its wake, as polling the completion
    /// inline does.
    #[test]
    fn a_waker_registering_again_from_its_wake_does_not_deadlock() {
        let (done, waited) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let c = Completion::new_wait();
            c.complete(0);
            let again = Arc::new(Again {
                completion: c.clone(),
                wakes: AtomicUsize::new(0),
            });
            c.set_waker(&Waker::from(again.clone()));
            let _ = done.send(again.wakes.load(Ordering::SeqCst));
        });
        let wakes = waited
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("set_waker deadlocked waking a waker that registers again");
        assert_eq!(
            wakes, 4,
            "each set_waker on the finished completion wakes once"
        );
    }

    /// A task that polls its completion from its wake, as an executor polling the task inline
    /// does: woken for the first time while the completion is pending, it registers again.
    struct PollingTask {
        completion: Completion,
        wakes: AtomicUsize,
    }

    impl std::task::Wake for PollingTask {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if self.wakes.fetch_add(1, Ordering::SeqCst) == 0 && !self.completion.finished() {
                self.completion.set_waker(&Waker::from(self.clone()));
            }
        }
    }

    /// Two tasks waiting on one completion each keep a waker on it: the second to register wakes
    /// neither, so they do not wake each other without end, and finishing the completion wakes
    /// each of them once.
    #[test]
    fn two_tasks_waiting_on_one_completion_are_each_woken_once_when_it_finishes() {
        let (done, waited) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let c = Completion::new_wait();
            let task = || {
                Arc::new(PollingTask {
                    completion: c.clone(),
                    wakes: AtomicUsize::new(0),
                })
            };
            let (a, b) = (task(), task());
            let wakes = || [&a, &b].map(|task| task.wakes.load(Ordering::SeqCst));
            c.set_waker(&Waker::from(a.clone()));
            c.set_waker(&Waker::from(b.clone()));
            let registered = wakes();
            c.complete(0);
            let _ = done.send((registered, wakes()));
        });
        let (registered, finished) = waited
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("set_waker deadlocked waking a task that polls the completion from its wake");
        assert_eq!(registered, [0, 0], "the second registration woke a task");
        assert_eq!(finished, [1, 1], "finishing wakes each task once");
    }

    /// A keyed registration is made at most once per key and withdrawn exactly, whatever
    /// `will_wake` says: here a waker each clone of which owns a fresh box, a legal waker no clone
    /// of which `will_wake` another (the auditor's probe of 2026-11-08).
    #[test]
    fn a_keyed_waker_is_registered_once_and_withdrawn_exactly() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{RawWaker, RawWakerVTable};
        static LIVE: AtomicUsize = AtomicUsize::new(0);
        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_raw);
        fn raw() -> RawWaker {
            LIVE.fetch_add(1, Ordering::SeqCst);
            RawWaker::new(Box::into_raw(Box::new(0u8)) as *const (), &VTABLE)
        }
        unsafe fn clone(_: *const ()) -> RawWaker {
            raw()
        }
        unsafe fn wake(data: *const ()) {
            // SAFETY: the vtable's own waker, owning its box.
            unsafe { drop_raw(data) }
        }
        unsafe fn wake_by_ref(_: *const ()) {}
        unsafe fn drop_raw(data: *const ()) {
            LIVE.fetch_sub(1, Ordering::SeqCst);
            // SAFETY: made by `raw` from a box this waker owns.
            drop(unsafe { Box::from_raw(data as *mut u8) });
        }
        // SAFETY: the vtable keeps the RawWaker contract: each clone owns its box, dropped once.
        let waker = unsafe { Waker::from_raw(raw()) };
        let c = Completion::new_wait();
        let key = WakerKey::new(7).unwrap();
        for _ in 0..64 {
            c.set_keyed_waker(key, &waker);
        }
        assert_eq!(c.wakers(), 1, "one registration for the key");
        c.withdraw_key(key);
        assert_eq!(c.wakers(), 0, "the key's registration withdrawn");
        drop(waker);
        assert_eq!(LIVE.load(Ordering::SeqCst), 0, "every clone let go");
    }

    /// A completion keeps the waker of every task that registers until it wakes, then wakes each
    /// once and lets them all go, a task that stopped waiting included.
    #[test]
    fn a_completion_lets_its_wakers_go_when_it_wakes() {
        use std::task::Wake;

        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let c = Completion::new_wait();
        let tasks: Vec<_> = (0..3)
            .map(|_| Arc::new(Count(AtomicUsize::new(0))))
            .collect();
        for task in &tasks {
            c.set_waker(&Waker::from(task.clone()));
        }
        let wakes = |task: &Arc<Count>| task.0.load(Ordering::SeqCst);
        assert!(
            tasks
                .iter()
                .all(|task| wakes(task) == 0 && Arc::strong_count(task) == 2),
            "a registration woke or let go another task's waker"
        );
        c.complete(0);
        assert!(
            tasks
                .iter()
                .all(|task| wakes(task) == 1 && Arc::strong_count(task) == 1),
            "finishing did not wake each waker once and let it go"
        );
    }
    #[cfg(not(any(loom, atoll_loom, shuttle)))]
    #[test]
    fn a_standalone_context_clone_keeps_its_registered_wakers() {
        use std::task::Wake;
        struct Calls(AtomicUsize);
        impl Wake for Calls {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let called = Arc::new(Calls(AtomicUsize::new(0)));
        let context = Context::new();
        let copied = context.clone();
        context.set_waker(&Waker::from(called.clone()));
        drop(context);
        let before = called.0.load(Ordering::Relaxed);
        copied.wake();
        drop(copied);
        assert_eq!(before, 0);
        assert_eq!(called.0.load(Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&called), 1);
    }
}
