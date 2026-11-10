//! The checkpoint's memory demand (DESIGN.md 9.1.3, *Maintenance keeps what it owns*). A claim the
//! checkpoint makes while it images or frees a page, which finds no frame, names its demand on the
//! supply's checkpoint word: a frame of its class freed, or set aside from a run formed, goes to it
//! before the shortage gate's holder, and before the free lists, where a fast claim would take it
//! first. The demand lives in the checkpointer's state, beside its run, so whichever thread steps
//! the checkpoint next finds it: never in an operation's owner, which the checkpoint's claims do not
//! touch (they run masked). One checkpoint images at a time, so one demand.

use super::shortage::{unowned, Proxy};
use super::{BufferPool, Purpose, SizeClass};
use crate::io::{Completion, WakerKey};
use crate::storage::atoll::notify;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::Result;
use std::cell::Cell;

/// The checkpoint's demand, kept across its steps by the checkpointer's state. Its stepper alone
/// touches it; an offer touches only the supply's checkpoint word and grant cell.
#[derive(Default)]
pub(crate) struct Upkeep {
    /// The pool its demand is on, once it has made one.
    pool: Option<Arc<BufferPool>>,
    /// The demand named on the supply's checkpoint word, its frame not yet taken.
    demand: Option<(SizeClass, Purpose)>,
    /// A frame handed to the demand and adopted, kept for the claim of exactly its class and
    /// purpose: a read's waits until it is quiet.
    reserved: Option<(SizeClass, Purpose, u32)>,
    /// Its own wait while the demand stands, for the claim's wait `under`: finished by the offer
    /// that hands it a frame or by `under`'s producer.
    grant: Option<Completion>,
    /// The claim's wait, and the key of the proxy registered on it (the grant's allocation).
    under: Option<(Completion, WakerKey)>,
    /// A handed frame's quiet wait for a read, registered once with the epochs.
    quiet: Option<Completion>,
    /// Whether a claim reached the demand in the current step.
    claimed: bool,
}

crate::thread::thread_local! {
    /// The checkpoint's demand, while its stepper runs the step on this thread.
    static UPKEEP: Cell<*mut Upkeep> = const { Cell::new(std::ptr::null_mut()) };
}

/// Restores the demand installed before, however the step ends.
struct Installed(*mut Upkeep);

impl Drop for Installed {
    fn drop(&mut self) {
        UPKEEP.with(|u| u.set(self.0));
    }
}

/// Runs `f`, a step of the checkpoint, with `upkeep` as the demand of every claim it makes, and as
/// no operation's: an operation that helps the checkpoint along keeps its own owner and frame. A
/// demand that no claim reached in the step is abandoned at its end: the page it was for was found
/// resident, or the step moved on.
pub(crate) fn scope<T>(upkeep: &mut Upkeep, f: impl FnOnce() -> T) -> T {
    crate::storage::atoll::unwind::abort_if_unwinding();
    upkeep.claimed = false;
    let out = {
        let _installed = Installed(UPKEEP.with(|u| u.replace(upkeep as *mut Upkeep)));
        unowned(f)
    };
    if !upkeep.claimed {
        upkeep.abandon();
    }
    out
}

/// A claim inside the checkpoint's step: its demand's. None outside one.
pub(super) fn claim(
    pool: &Arc<BufferPool>,
    class: SizeClass,
    purpose: Purpose,
) -> Option<Result<IOResult<u32>>> {
    let upkeep = UPKEEP.with(Cell::get);
    if upkeep.is_null() {
        return None;
    }
    // The demand's own claim below runs as an ordinary one, with no demand installed.
    let _installed = Installed(UPKEEP.with(|u| u.replace(std::ptr::null_mut())));
    // SAFETY: installed by `scope` for its call, which is still running and touches the demand
    // only after `f` returns; nothing else reaches it meanwhile.
    let upkeep = unsafe { &mut *upkeep };
    Some(upkeep.claim(pool, class, purpose))
}

impl Upkeep {
    fn claim(
        &mut self,
        pool: &Arc<BufferPool>,
        class: SizeClass,
        purpose: Purpose,
    ) -> Result<IOResult<u32>> {
        self.claimed = true;
        // A demand standing for another class or purpose: the step moved on.
        if self.demand.is_some_and(|d| d != (class, purpose))
            || self
                .reserved
                .is_some_and(|(c, p, _)| (c, p) != (class, purpose))
        {
            self.abandon();
        }
        self.pool.get_or_insert_with(|| pool.clone());
        let frame = match self.adopt(pool, class, purpose) {
            Some(frame) => frame,
            None => match pool.claim(class, purpose, &[])? {
                IOResult::Done(found) => {
                    // A frame handed meanwhile: the demand keeps that, the find goes back.
                    if let Some(handed) = self.withdraw(pool) {
                        pool.give_back_reserved(class, found);
                        self.reserved = Some((class, purpose, handed));
                    } else {
                        return Ok(IOResult::Done(found));
                    }
                    handed_frame(self)
                }
                IOResult::IO(IOCompletions(under)) => match self.adopt(pool, class, purpose) {
                    Some(frame) => frame,
                    None => return Ok(IOResult::IO(IOCompletions(self.wait(pool, under)))),
                },
            },
        };
        // A handed frame keeps its quiet stamp: a read goes into it only once every step that
        // could still read what it held has ended.
        let at = pool.quiet_at(frame, class);
        let epochs = pool.store.epochs();
        if purpose == Purpose::Read && at > epochs.known_horizon() {
            if let Some(quiet) = self.quiet.as_ref().filter(|q| !q.finished()) {
                return Ok(IOResult::IO(IOCompletions(quiet.clone())));
            }
            let quiet = notify::remote_wait();
            if epochs.wait_for(at, quiet.clone()) {
                self.quiet = Some(quiet.clone());
                return Ok(IOResult::IO(IOCompletions(quiet)));
            }
        }
        self.reserved = None;
        self.quiet = None;
        Ok(IOResult::Done(frame))
    }

    /// The frame kept for this demand, or handed to it and adopted now; else the demand named on
    /// the checkpoint word, so a frame of its class freed from here goes to it.
    fn adopt(&mut self, pool: &Arc<BufferPool>, class: SizeClass, purpose: Purpose) -> Option<u32> {
        if let Some((_, _, frame)) = self.reserved {
            return Some(frame);
        }
        match pool.frames.supply.upkeep.want(class) {
            Some(frame) => {
                self.demand = None;
                self.reserved = Some((class, purpose, frame));
                self.grant_done();
                Some(frame)
            }
            None => {
                self.demand = Some((class, purpose));
                None
            }
        }
    }

    /// The demand's own wait for `under`, the wait its claim found: as the shortage gate holder's
    /// (`Queue::wait`), finished by the offer that hands it a frame or by `under`'s producer, of
    /// `under`'s kind; the same wait while `under` is the same.
    fn wait(&mut self, pool: &Arc<BufferPool>, under: Completion) -> Completion {
        if let (Some(grant), Some((on, _))) = (&self.grant, &self.under) {
            if !grant.finished() && on.addr().is_some() && on.addr() == under.addr() {
                return grant.clone();
            }
        }
        self.grant_done();
        let grant = if under.is_remote_wait() {
            notify::remote_wait()
        } else {
            notify::wait()
        };
        let key = WakerKey::new(grant.addr().expect("an engine wait")).expect("a pointer");
        let supply = &pool.frames.supply;
        supply.post_upkeep_grant(grant.clone());
        self.grant = Some(grant.clone());
        self.under = Some((under.clone(), key));
        let proxy = std::sync::Arc::new(Proxy {
            grant: grant.clone(),
            under: under.clone(),
        });
        under.set_keyed_waker(key, &std::task::Waker::from(proxy));
        // Pairs with the offer's fence: a frame handed before the grant was posted is seen here.
        crate::sync::atomic::fence(crate::sync::atomic::Ordering::SeqCst);
        if supply.upkeep.handed() {
            notify::finish(&grant, Ok(0));
        }
        grant
    }

    /// The demand's grant taken back, finished, and its proxy withdrawn: the demand is met or gone.
    fn grant_done(&mut self) {
        if let Some((under, key)) = self.under.take() {
            under.withdraw_key(key);
        }
        if let Some(grant) = self.grant.take() {
            if let Some(pool) = &self.pool {
                pool.frames.supply.take_upkeep_grant();
            }
            notify::finish(&grant, Ok(0));
        }
    }

    /// Withdraws the demand from the checkpoint word: the frame handed to it, if one was.
    fn withdraw(&mut self, pool: &Arc<BufferPool>) -> Option<u32> {
        self.grant_done();
        self.demand.take()?;
        pool.frames.supply.upkeep.withdraw()
    }

    /// Lets the demand go: withdrawn, a frame handed or kept for it freed, its waits finished.
    pub(crate) fn abandon(&mut self) {
        let Some(pool) = self.pool.clone() else {
            return;
        };
        let demand = self.demand;
        if let Some(frame) = self.withdraw(&pool) {
            let (class, _) = demand.expect("a handed frame's demand");
            pool.give_back_reserved(class, frame);
        }
        self.grant_done();
        if let Some((class, _, frame)) = self.reserved.take() {
            pool.give_back_reserved(class, frame);
        }
        if let Some(quiet) = self.quiet.take() {
            notify::finish(&quiet, Ok(0));
        }
    }
}

/// The frame kept for the demand, just recorded.
fn handed_frame(upkeep: &Upkeep) -> u32 {
    upkeep.reserved.expect("a handed frame").2
}

impl Drop for Upkeep {
    fn drop(&mut self) {
        crate::storage::atoll::unwind::abort_if_unwinding();
        self.abandon();
    }
}
