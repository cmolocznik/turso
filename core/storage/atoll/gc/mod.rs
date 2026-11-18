//! Garbage collection of retired values (DESIGN.md §6.5, §11.5; the auditor's rulings #6174, #6186
//! and #6269): each slot's list of the values its writes retired ([`list`]), the collector that
//! frees them once every snapshot sees the write ([`collect`]), and the retired file a checkpoint
//! writes them to and the open reads them from ([`file`], [`retired`]).
//!
//! The checkpoint and the open use the file now. The writes' appends and the passes come with E1b
//! (S5), so what only they call is allowed unused outside tests until then, and in the test builds
//! that leave out the retired file's tests (shuttle, loom, miri). Both read the transactions
//! through [`Writers`] and change nothing of them.
#![cfg_attr(any(not(test), shuttle, atoll_loom, miri), allow(dead_code))]

mod collect;
mod file;
mod list;
mod retired;

#[cfg(all(test, unix, not(any(shuttle, atoll_loom, miri))))]
pub use list::Reader;
#[cfg(all(test, unix, not(any(shuttle, atoll_loom, miri))))]
pub use retired::{take_bookkeeping, take_list_reads};
pub use retired::{Retired, RetiredRun};

#[cfg(test)]
mod tests;

use super::txn::Ts;

/// What the lists and the collector read of the transactions.
pub trait Writers {
    /// Whether the transaction running in `slot` is the one that started at `start`.
    fn running(&self, slot: u16, start: Ts) -> bool;

    /// The commit timestamp of `slot`'s transaction that started at `start`, once its pair is
    /// published. A transaction no longer running whose items are still in its slot's list
    /// committed (a rollback cuts its items first), and its pair is published before its slot
    /// lets it go.
    fn commit_of(&self, slot: u16, start: Ts) -> Option<Ts>;
}
