//! Atoll: a storage mode beside the pager, after LeanStore and Umbra. Its design is in
//! `docs/internals/atoll/DESIGN.md`; the modules below are the prototype it replaces step by step.
//!
//! - [`page`]: size classes of exponentially growing page sizes, page ids and the page header.
//! - [`store`]: the page store, one file per size class, disk space reused only by pages of the
//!   same size.
//! - [`latch`]: the versioned latch each buffer frame carries.
//! - [`buffer`]: the buffer manager: one virtual memory region per size class sharing one budget,
//!   swizzled pointers, LeanStore's cooling stage, write-back submitted as pages cool.
//! - [`wal`]: the write-ahead log, a write and an fsync in flight, recovery streamed in chunks.
//! - [`checkpoint`]: the page table made durable in a map file, the log cut at the redo point,
//!   and opening a database: newest checkpoint, then recovery, then a checkpoint.
//!
//! Every IO goes through Turso's [`crate::io::IO`] and returns its completion, so this runs under
//! the VDBE's `IOResult` stepping on every backend: io_uring, the syscall backend and memory.

pub mod aligned;
pub mod control;
pub mod latch;
pub mod log;
pub mod meta;
pub mod page;
pub mod radix;
pub mod store;

#[cfg(test)]
mod testing;
