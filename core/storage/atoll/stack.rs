//! A lock-free stack whose items are taken all at once: a list many threads push onto and empty,
//! with no lock and nothing to reclaim.
//!
//! Pushing is a compare-and-swap of the head; taking is one swap of the head for none, which hands
//! the taker every node pushed before it, its own from then on. No thread reads a node another
//! may free: a node is reached only from the head, and leaves it only by the swap, so no epoch is
//! needed and the head's reuse of an address is harmless (a push links whatever the head is).

use self::sync::{AtomicPtr, Ordering, Plain};
use super::epoch::{Epochs, Retired, Step};
use std::ptr;

/// The engine's primitives, or loom's.
#[cfg(not(atoll_loom))]
mod sync {
    pub(super) use crate::sync::atomic::{AtomicPtr, Ordering};

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
    pub(super) use loom::cell::UnsafeCell as Plain;
    pub(super) use loom::sync::atomic::{AtomicPtr, Ordering};
}

/// A node's `next`, as its pusher wrote it: the head it read, which may name a node freed since
/// whose address a newer node took before the push's exchange succeeded, the exchange comparing
/// addresses alone. Every node's provenance is exposed as it is pushed, so the pointer is taken
/// again from its address, and names the node there now.
fn linked<T>(next: *mut T) -> *mut T {
    ptr::with_exposed_provenance_mut(next.addr())
}

struct Node<T> {
    item: T,
    /// Written with the node and read by whoever takes it: under loom, the check that the push
    /// published the node to the taker.
    pushed: Plain<bool>,
    next: *mut Node<T>,
}

pub struct Stack<T> {
    head: AtomicPtr<Node<T>>,
}

// SAFETY: items move between threads only through the head's exchanges.
unsafe impl<T: Send> Send for Stack<T> {}
unsafe impl<T: Send> Sync for Stack<T> {}

impl<T> Default for Stack<T> {
    fn default() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

impl<T> Stack<T> {
    pub fn push(&self, item: T) {
        let node = Box::into_raw(Box::new(Node {
            item,
            pushed: Plain::new(true),
            next: ptr::null_mut(),
        }));
        let _ = node.expose_provenance();
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            // SAFETY: the node is this thread's until the exchange below publishes it.
            unsafe { (*node).next = head };
            // Release: whoever takes the node sees it whole.
            match self
                .head
                .compare_exchange_weak(head, node, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(now) => head = now,
            }
        }
    }

    /// Every item pushed and not taken, the oldest first: the caller's from now on.
    pub fn take_all(&self) -> Vec<T> {
        let mut out: Vec<T> = self.take_chain().collect();
        out.reverse();
        out
    }

    /// Every item pushed and not taken, handed over unread with one swap of the head: the
    /// caller's from now on, to read as it goes, the newest first ([`Chain`]).
    pub fn take_chain(&self) -> Chain<T> {
        Chain {
            head: self.head.swap(ptr::null_mut(), Ordering::Acquire),
        }
    }

    /// Whether no item is pushed and not taken; one a taker holds does not count.
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire).is_null()
    }
}

impl<T> Drop for Stack<T> {
    fn drop(&mut self) {
        // A failed loom model is torn down while its threads unwind; its atomics then panic again.
        if cfg!(atoll_loom) && std::thread::panicking() {
            return;
        }
        drop(self.take_chain());
    }
}

/// The items a [`Stack::take_chain`] handed over, read one at a time, the newest first: what a
/// caller reads in runs rather than collects whole.
pub struct Chain<T> {
    head: *mut Node<T>,
}

// SAFETY: the chain's nodes are its holder's alone since the swap that took them.
unsafe impl<T: Send> Send for Chain<T> {}

impl<T> Default for Chain<T> {
    fn default() -> Self {
        Self {
            head: ptr::null_mut(),
        }
    }
}

impl<T> Iterator for Chain<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.head.is_null() {
            return None;
        }
        // SAFETY: published by `push` with a release exchange, and the swap that made this chain
        // made its holder the node's only holder.
        let node = unsafe { Box::from_raw(self.head) };
        // SAFETY: written before the push that published the node.
        assert!(
            node.pushed.with(|p| unsafe { *p }),
            "a node taken unwritten"
        );
        self.head = linked(node.next);
        Some(node.item)
    }
}

impl<T> Drop for Chain<T> {
    fn drop(&mut self) {
        if cfg!(atoll_loom) && std::thread::panicking() {
            return;
        }
        for item in self.by_ref() {
            drop(item);
        }
    }
}

/// A lock-free free list: items pushed and popped one at a time by compare-and-swap of the head.
/// A popped node is freed through the epochs, once no step that could have read it is left: a pop
/// that read a node another took meanwhile still reads it whole and finds the head moved on, and
/// no node's address comes back to the head while a step could hold it, which rules out the ABA
/// a reused address would cause.
pub struct FreeList<T: Send + 'static> {
    head: AtomicPtr<FreeNode<T>>,
    popped: Retired<Popped<T>>,
}

/// A node popped, freed once no step that could read it is left: no box is made of it before
/// then, since making one claims the node alone while a pop that lost may still read its link.
struct Popped<T>(*mut FreeNode<T>);

// SAFETY: the node is reached through this alone once it is retired.
unsafe impl<T: Send> Send for Popped<T> {}

impl<T> Drop for Popped<T> {
    fn drop(&mut self) {
        // SAFETY: from `push`'s box; its item was moved out, and no step can read it any more.
        drop(unsafe { Box::from_raw(self.0) });
    }
}

struct FreeNode<T> {
    /// Moved out by the pop that takes the node, so never dropped with it.
    item: std::mem::ManuallyDrop<T>,
    next: *mut FreeNode<T>,
}

impl<T: Send + 'static> Default for FreeList<T> {
    fn default() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
            popped: Retired::default(),
        }
    }
}

impl<T: Send + 'static> FreeList<T> {
    pub fn push(&self, item: T) {
        let node = Box::into_raw(Box::new(FreeNode {
            item: std::mem::ManuallyDrop::new(item),
            next: ptr::null_mut(),
        }));
        let _ = node.expose_provenance();
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            // SAFETY: the node is this thread's until the exchange below publishes it.
            unsafe { (*node).next = head };
            // Release: a pop that takes the node sees it whole.
            match self
                .head
                .compare_exchange_weak(head, node, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(now) => head = now,
            }
        }
    }

    /// Pops an item, if any, in `_step`, which keeps every node it reads from being freed.
    pub fn pop(&self, epochs: &Epochs, _step: &Step) -> Option<T> {
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            if head.is_null() {
                return None;
            }
            // SAFETY: published by `push`; a node popped meanwhile is freed only once this
            // step has ended.
            let next = linked(unsafe { (*head).next });
            match self
                .head
                .compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    // SAFETY: this exchange took the node; its item is moved out once, here, by a
                    // read through the pointer, which other pops may still read through too.
                    let item = std::mem::ManuallyDrop::into_inner(unsafe {
                        ptr::addr_of!((*head).item).read()
                    });
                    self.popped.retire(epochs, Popped(head));
                    drop(self.popped.reclaim(epochs));
                    return Some(item);
                }
                Err(now) => head = now,
            }
        }
    }

    /// The items not popped, for a test to read once no thread pushes or pops.
    #[cfg(test)]
    pub fn items(&self) -> Vec<T>
    where
        T: Clone,
    {
        let mut out = Vec::new();
        let mut at = self.head.load(Ordering::Acquire);
        while !at.is_null() {
            // SAFETY: no thread pops meanwhile, so every node reached is alive.
            unsafe {
                out.push((*(*at).item).clone());
                at = linked((*at).next);
            }
        }
        out
    }
}

impl<T: Send + 'static> Drop for FreeList<T> {
    fn drop(&mut self) {
        if cfg!(atoll_loom) && std::thread::panicking() {
            return;
        }
        let mut at = self.head.load(Ordering::Acquire);
        while !at.is_null() {
            // SAFETY: the list is the last owner of every node still in it.
            let mut node = unsafe { Box::from_raw(at) };
            at = linked(node.next);
            // SAFETY: never popped, so the item is still in the node.
            unsafe { std::mem::ManuallyDrop::drop(&mut node.item) };
        }
    }
}

/// Pushers push while a taker takes: every item comes out exactly once.
#[cfg(all(test, not(atoll_loom)))]
fn every_item_comes_out_once(pushers: usize, items: usize) {
    use crate::sync::Arc;
    let stack = Arc::new(Stack::default());
    let threads: Vec<_> = (0..pushers)
        .map(|t| {
            let stack = stack.clone();
            crate::thread::spawn(move || {
                for i in 0..items {
                    stack.push(t * items + i);
                }
            })
        })
        .collect();
    let mut out = Vec::new();
    while out.len() < pushers * items {
        out.extend(stack.take_all());
        crate::thread::yield_now();
    }
    for t in threads {
        t.join().unwrap();
    }
    out.sort_unstable();
    assert_eq!(out, (0..pushers * items).collect::<Vec<_>>());
    assert!(stack.is_empty());
}

/// Threads push and pop numbers in steps: every number pushed comes out once.
#[cfg(all(test, not(atoll_loom)))]
fn free_list_numbers_come_out_once(threads: u64, per: u64) {
    use crate::sync::Arc;
    let epochs = Arc::new(Epochs::new());
    let list = Arc::new(FreeList::default());
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let (epochs, list) = (epochs.clone(), list.clone());
            crate::thread::spawn(move || {
                let mut got = Vec::new();
                for i in 0..per {
                    list.push(t * per + i);
                    let step = epochs.enter();
                    if let Some(n) = list.pop(&epochs, &step) {
                        got.push(n);
                    }
                }
                got
            })
        })
        .collect();
    let mut out: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let step = epochs.enter();
    while let Some(n) = list.pop(&epochs, &step) {
        out.push(n);
    }
    drop(step);
    out.sort_unstable();
    assert_eq!(out, (0..threads * per).collect::<Vec<_>>());
}

#[cfg(all(test, not(atoll_loom), not(shuttle)))]
#[test]
fn free_list_numbers_come_out_once_on_threads() {
    free_list_numbers_come_out_once(3, if cfg!(miri) { 200 } else { 100_000 });
}

#[cfg(all(test, shuttle))]
#[test]
fn free_list_numbers_come_out_once_in_shuttle_schedules() {
    super::testing::schedules(|| free_list_numbers_come_out_once(2, 4), 2_000);
}

#[cfg(all(test, not(atoll_loom), not(shuttle)))]
#[test]
fn every_item_comes_out_once_on_threads() {
    every_item_comes_out_once(3, if cfg!(miri) { 200 } else { 100_000 });
}

#[cfg(all(test, shuttle))]
#[test]
fn every_item_comes_out_once_in_shuttle_schedules() {
    super::testing::schedules(|| every_item_comes_out_once(2, 5), 2_000);
}

#[cfg(all(test, atoll_loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Two pops race over a list of two, one pushed meanwhile: each item comes out once, and
    /// what a pop reads of a node another took stays whole until its step ends.
    #[test]
    fn free_list_items_pop_once() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let epochs = Arc::new(Epochs::new());
            let list = Arc::new(FreeList::default());
            list.push(0u64);
            list.push(1);
            let popper = {
                let (epochs, list) = (epochs.clone(), list.clone());
                thread::spawn(move || {
                    let step = epochs.enter();
                    list.pop(&epochs, &step)
                })
            };
            let pusher = {
                let list = list.clone();
                thread::spawn(move || list.push(2))
            };
            let mine = {
                let step = epochs.enter();
                list.pop(&epochs, &step)
            };
            let theirs = popper.join().unwrap();
            pusher.join().unwrap();
            let mut out: Vec<u64> = [mine, theirs].into_iter().flatten().collect();
            let step = epochs.enter();
            while let Some(i) = list.pop(&epochs, &step) {
                out.push(i);
            }
            drop(step);
            out.sort_unstable();
            assert_eq!(out, vec![0, 1, 2]);
        });
    }

    /// Two pushes race a take: each item comes out once, whole.
    #[test]
    fn pushed_items_come_out_once_and_whole() {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(|| {
            let stack = Arc::new(Stack::default());
            let pushers: Vec<_> = (0..2)
                .map(|i| {
                    let stack = stack.clone();
                    thread::spawn(move || stack.push(i))
                })
                .collect();
            let mut out = stack.take_all();
            for p in pushers {
                p.join().unwrap();
            }
            out.extend(stack.take_all());
            out.sort_unstable();
            assert_eq!(out, vec![0, 1]);
        });
    }
}
