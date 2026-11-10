//! A completion posted where readers find it, without a lock.
//!
//! A reader announces a pointer, then validates the cell before cloning the reference. Removing
//! that pointer gives each matching announcement one reference until it clears. An unrelated
//! page step therefore keeps no removed completions alive.

use super::super::epoch::Epochs;
use super::memory::Zeroable;
use crate::io::Completion;
use crate::sync::atomic::{AtomicPtr, Ordering};
use crate::sync::Arc;
use std::ptr;

#[derive(Default)]
pub struct CompletionCell(AtomicPtr<()>);

// SAFETY: all zeros is a null pointer, an empty cell.
unsafe impl Zeroable for CompletionCell {}

impl Drop for CompletionCell {
    fn drop(&mut self) {
        let raw = self.0.load(Ordering::Acquire);
        if !raw.is_null() {
            // SAFETY: the cell's own reference, and nothing reads the cell any more.
            drop(unsafe { Completion::from_raw(raw) });
        }
    }
}

/// The owning database's completion announcements, independent of its page steps.
pub struct CompletionDomain {
    epochs: Arc<Epochs>,
}

impl CompletionDomain {
    pub fn new(epochs: Arc<Epochs>) -> Self {
        Self { epochs }
    }

    fn removed(&self, c: Completion) {
        self.epochs.protect_removed_completion(&c);
        drop(c);
    }
}

/// The cell changed during its read. Its caller must look at the owning state again.
#[derive(Debug)]
pub struct Changed;

impl CompletionCell {
    /// The completion in the cell, if any, as a reference of the caller's own.
    pub fn get(&self, domain: &CompletionDomain) -> Result<Option<Completion>, Changed> {
        self.read(domain).map(|(_, completion)| completion)
    }

    fn read(&self, domain: &CompletionDomain) -> Result<(*mut (), Option<Completion>), Changed> {
        let raw = self.0.load(Ordering::Acquire);
        if raw.is_null() {
            return Ok((raw, None));
        }
        let announcement = domain.epochs.completion_slot();
        // SAFETY: raw is an untagged pointer read from this cell. It is not dereferenced before
        // validation; the remover supplies a reference if its unlink can race that validation.
        unsafe { announcement.announce(raw) };
        crate::race::point("cell.announced");
        let validated = self.0.load(Ordering::Acquire);
        if validated != raw {
            return Err(Changed);
        }
        crate::race::point("cell.validated");
        // SAFETY: either the cell still owns a reference, or its remover sees the announcement
        // and supplies one until it clears. Clone from the validating load's provenance: an
        // intervening allocation may have reused the first load's address.
        let completion = unsafe { Completion::clone_raw(validated) };
        drop(announcement);
        Ok((validated, Some(completion)))
    }

    /// Puts `c` in the cell, which must be empty: for the cell's owner.
    pub fn put(&self, c: Completion) {
        let old = self.0.swap(c.into_raw().cast_mut(), Ordering::AcqRel);
        assert!(old.is_null(), "a completion put in a cell that held one");
    }

    /// Puts `c` in the cell if it is empty: whether it went in. Of two putting at once one wins,
    /// and the cell's owner is then whoever did.
    pub fn try_put(&self, c: Completion) -> bool {
        let raw = c.into_raw().cast_mut();
        match self
            .0
            .compare_exchange(ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(_) => {
                // SAFETY: never published, so still this thread's own.
                drop(unsafe { Completion::from_raw(raw) });
                false
            }
        }
    }

    /// Takes the completion out of the cell, if any: for the cell's owner.
    pub fn take(&self, domain: &CompletionDomain) -> Option<Completion> {
        let raw = self.0.swap(ptr::null_mut(), Ordering::AcqRel);
        if raw.is_null() {
            return None;
        }
        // SAFETY: the cell's own reference, which the swap handed over.
        let held = unsafe { Completion::from_raw(raw) };
        let out = held.clone();
        domain.removed(held);
        Some(out)
    }

    /// The unfinished completion in the cell, or a new one if empty or finished.
    pub fn current(&self, domain: &CompletionDomain) -> Completion {
        self.current_with(domain, crate::storage::atoll::notify::wait)
    }

    /// [`current`](Self::current), with a choice of logical wait constructor.
    pub fn current_with(&self, domain: &CompletionDomain, fresh: fn() -> Completion) -> Completion {
        // One install, then one protected read of a winner. A moved cell sends its caller to
        // look at the owning state again; no part retries in a loop.
        let (raw, read) = match self.read(domain) {
            Ok(read) => read,
            Err(Changed) => return Completion::new_yield(),
        };
        if let Some(c) = read.as_ref() {
            if !c.finished() {
                return read.unwrap();
            }
        }
        crate::race::point("cell.install");
        let fresh = fresh();
        let fresh_raw = fresh.clone().into_raw().cast_mut();
        match self
            .0
            .compare_exchange(raw, fresh_raw, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                if !raw.is_null() {
                    // SAFETY: the cell's reference handed over by the successful exchange.
                    // `read` also kept its expected pointer's allocation alive across the CAS.
                    domain.removed(unsafe { Completion::from_raw(raw) });
                }
                fresh
            }
            Err(_) => {
                // SAFETY: never published, so still this thread's own.
                drop(unsafe { Completion::from_raw(fresh_raw) });
                match self.read(domain) {
                    Ok((_, Some(winner))) => winner,
                    Ok((_, None)) | Err(Changed) => read.unwrap_or_else(Completion::new_yield),
                }
            }
        }
    }
}

#[cfg(all(test, not(atoll_loom)))]
mod tests {
    use super::*;

    const ROUNDS: usize = if cfg!(miri) { 200 } else { 20_000 };

    /// An install lost to a take that emptied the cell between the read and the compare-and-swap:
    /// the finished completion read is returned, which sends its waiter to look again, and nothing
    /// loops (DESIGN.md 9.1.2).
    #[test]
    fn an_install_lost_to_a_take_returns_the_finished_completion_it_read() {
        use crate::race;
        let epochs = Arc::new(Epochs::new());
        let retiring = Arc::new(CompletionDomain::new(epochs.clone()));
        let cell = Arc::new(CompletionCell::default());
        let first = Completion::new_wait();
        cell.put(first.clone());
        first.complete(0);
        {
            let (cell, retiring) = (cell.clone(), retiring.clone());
            race::at("cell.install", move || {
                assert!(cell.take(&retiring).is_some(), "the take emptied the cell");
            });
        }
        let got = cell.current(&retiring);
        assert!(!race::pending("cell.install"));
        assert!(
            got.finished(),
            "the completion read, finished: its waiter looks again"
        );
    }

    /// The owner puts completions in a cell and takes them out, finishing them, while readers
    /// take references to whatever it holds and others replace a finished one: every
    /// reference is taken to a completion still alive, which Miri checks, and none leaks.
    #[test]
    fn readers_never_reach_a_completion_let_go() {
        let epochs = Arc::new(Epochs::new());
        let retiring = Arc::new(CompletionDomain::new(epochs.clone()));
        let cell = Arc::new(CompletionCell::default());
        let shared = Arc::new(CompletionCell::default());
        let owner = {
            let (cell, retiring) = (cell.clone(), retiring.clone());
            crate::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    cell.put(Completion::new_wait());
                    crate::thread::yield_now();
                    if let Some(c) = cell.take(&retiring) {
                        c.complete(0);
                    }
                }
            })
        };
        let readers: Vec<_> = (0..2)
            .map(|r| {
                let (retiring, cell, shared) = (retiring.clone(), cell.clone(), shared.clone());
                crate::thread::spawn(move || {
                    for _ in 0..ROUNDS {
                        if let Ok(Some(c)) = cell.get(&retiring) {
                            let _ = c.finished();
                        }
                        let c = shared.current(&retiring);
                        if r == 0 && !c.is_explicit_yield() {
                            c.complete(0);
                        }
                        crate::thread::yield_now();
                    }
                })
            })
            .collect();
        owner.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
        assert!(cell.take(&retiring).is_none());
    }
}

#[cfg(all(test, not(atoll_loom), not(shuttle)))]
mod transfer_tests;
