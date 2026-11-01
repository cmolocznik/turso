//! Epochs: when an identity may be reused (CONCURRENCY.md §4).
//!
//! A thread entering a step, one call into Atoll that returns `Done` or `IO`, announces the global
//! epoch; it clears the announcement when the step ends. A freed object, whether a frame, a page
//! number, a memory batch or a version, is retired with a stamp: the global epoch, which the
//! retiring advances. It may be reused once every thread is between steps or announced an epoch
//! above its stamp. A thread that announced at or below the stamp entered its step before the
//! retiring and may still hold the object; one that announced above entered after the object was
//! unlinked, and cannot reach it.
//!
//! Steps never block, so a thread holds back reuse by at most one step. Between steps a thread holds
//! names with versions, never references, and checks them before use.
//!
//! Steps nest: a step that calls into another part of Atoll counts the inner step, and only the
//! outermost one announces. Threads run one step at a time, so the announcement is per thread.

use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::{Arc, Mutex};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

/// A thread between steps.
const IDLE: u64 = u64::MAX;

/// Distinguishes instances in the per-thread table.
static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

struct Announcement {
    epoch: AtomicU64,
}

pub struct Epochs {
    id: u64,
    global: AtomicU64,
    /// Every thread that has stepped here, with what it announced.
    threads: Mutex<Vec<Arc<Announcement>>>,
}

/// A thread's announcement for one instance, and how deep its steps are nested.
struct Entry {
    id: u64,
    announcement: Arc<Announcement>,
    depth: Cell<u32>,
}

crate::thread::thread_local! {
    static ENTRIES: RefCell<Vec<Entry>> = const { RefCell::new(Vec::new()) };
}

impl Default for Epochs {
    fn default() -> Self {
        Self::new()
    }
}

impl Epochs {
    pub fn new() -> Self {
        Self {
            id: NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            global: AtomicU64::new(0),
            threads: Mutex::new(Vec::new()),
        }
    }

    /// Enters a step; it ends when the guard is dropped.
    pub fn enter(&self) -> Step<'_> {
        ENTRIES.with(|entries| {
            let mut entries = entries.borrow_mut();
            let i = match entries.iter().position(|e| e.id == self.id) {
                Some(i) => i,
                None => {
                    let announcement = Arc::new(Announcement {
                        epoch: AtomicU64::new(IDLE),
                    });
                    self.threads.lock().push(announcement.clone());
                    entries.push(Entry {
                        id: self.id,
                        announcement,
                        depth: Cell::new(0),
                    });
                    entries.len() - 1
                }
            };
            let entry = &entries[i];
            if entry.depth.get() == 0 {
                // Sequentially consistent, as the retiring's advance and scan are: either the scan
                // sees this announcement, or this step's reads see the object already unlinked.
                entry
                    .announcement
                    .epoch
                    .store(self.global.load(Ordering::SeqCst), Ordering::SeqCst);
            }
            entry.depth.set(entry.depth.get() + 1);
        });
        Step { epochs: self }
    }

    fn leave(&self) {
        ENTRIES.with(|entries| {
            let entries = entries.borrow();
            let entry = entries
                .iter()
                .find(|e| e.id == self.id)
                .expect("leaving a step never entered");
            let depth = entry.depth.get() - 1;
            entry.depth.set(depth);
            if depth == 0 {
                entry.announcement.epoch.store(IDLE, Ordering::Release);
            }
        });
    }

    /// The stamp for an object retired now: the current epoch, which advances, so a step entered
    /// from here on announces above it.
    pub fn stamp(&self) -> u64 {
        self.global.fetch_add(1, Ordering::SeqCst)
    }

    /// The lowest epoch any thread in a step announced; objects stamped below it may be reused.
    pub fn horizon(&self) -> u64 {
        self.threads
            .lock()
            .iter()
            .map(|a| a.epoch.load(Ordering::SeqCst))
            .min()
            .unwrap_or(IDLE)
    }
}

/// A step in progress.
pub struct Step<'e> {
    epochs: &'e Epochs,
}

impl Drop for Step<'_> {
    fn drop(&mut self) {
        self.epochs.leave();
    }
}

/// Objects retired and not yet reusable, oldest first.
pub struct Limbo<T> {
    items: Mutex<VecDeque<(u64, T)>>,
}

impl<T> Default for Limbo<T> {
    fn default() -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
        }
    }
}

impl<T> Limbo<T> {
    /// Retires `item`: it is reusable once no step that could hold it is left.
    pub fn retire(&self, epochs: &Epochs, item: T) {
        // Stamped under the lock, so the items queue in stamp order.
        let mut items = self.items.lock();
        let stamp = epochs.stamp();
        items.push_back((stamp, item));
    }

    /// Takes every item no step can hold any more.
    pub fn reclaim(&self, epochs: &Epochs) -> Vec<T> {
        // The horizon after the items: each was stamped before it is read, so a step that could
        // hold one announced before the read, at or below its stamp. A horizon read first could
        // miss a step that began, retired an item and still runs.
        let mut items = self.items.lock();
        let horizon = epochs.horizon();
        let mut out = Vec::new();
        while items.front().is_some_and(|(stamp, _)| *stamp < horizon) {
            out.push(items.pop_front().expect("an item").1);
        }
        out
    }

    /// What waits, for a test to count it.
    #[cfg(test)]
    pub fn waiting(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.items.lock().iter().map(|(_, t)| t.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.items.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests;
