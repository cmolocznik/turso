//! Atoll: a storage mode beside the pager, with LeanStore as its baseline. Its design is in
//! `docs/internals/atoll/DESIGN.md`, whose last section says which parts are built.
//!
//! - [`log`]: the one ordered log of the database: parallel appends, group commit, segments.
//! - [`buffer`]: the buffer manager: frames, the hash table and frame hints, WATT, writeback.
//! - [`store`]: the page store: pages written out of place through the page table.
//! - [`meta`] and [`radix`]: the page table and free space as radix trees of metadata pages.
//! - [`control`]: the control file and its two superblocks.
//! - [`page`]: size classes, page ids and the page header.
//! - [`latch`]: the versioned latch buffer frames will carry.
//! - [`aligned`]: memory aligned for direct IO.
//!
//! Every IO goes through Turso's [`crate::io::IO`] and returns its completion, so this runs under
//! the VDBE's `IOResult` stepping on every backend: io_uring, the syscall backend and memory.

pub mod aligned;
pub mod buffer;
pub mod control;
pub mod latch;
pub mod log;
pub mod meta;
pub mod page;
pub mod radix;
pub mod store;

#[cfg(test)]
mod testing;
