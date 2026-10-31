//! Atoll: a storage mode beside the pager, with LeanStore as its baseline. Its design is in
//! `docs/internals/atoll/DESIGN.md`, whose last section says which parts are built.
//!
//! - [`db`]: a database: created new, or recovered from what a crash left ([`recovery`]).
//! - [`log`]: the one ordered log of the database: parallel appends, group commit, segments.
//! - [`checkpoint`]: continuous checkpoints, which bound the log recovery replays.
//! - [`btree`]: B+-trees: slotted nodes, optimistic lock coupling, values out of line.
//! - [`redo`]: how each log record replays on the page it changes: [`btree`] nodes and [`blob`]
//!   pages.
//! - [`buffer`]: the buffer manager: frames, the hash table and frame hints, WATT, writeback.
//! - [`store`]: the page store: pages written out of place through the page table.
//! - [`meta`] and [`radix`]: the page table and free space as radix trees of metadata pages.
//! - [`control`]: the control file and its two superblocks.
//! - [`page`]: size classes, page ids and the page header.
//! - [`latch`]: the versioned latch every buffer frame carries.
//! - [`aligned`]: memory aligned for direct IO.
//!
//! Every IO goes through Turso's [`crate::io::IO`] and returns its completion, so this runs under
//! the VDBE's `IOResult` stepping on every backend: io_uring, the syscall backend and memory.

pub mod aligned;
pub mod blob;
pub mod btree;
pub mod buffer;
pub mod checkpoint;
pub mod control;
pub mod db;
pub mod latch;
pub mod log;
pub mod meta;
pub mod page;
pub mod radix;
mod recovery;
pub mod redo;
pub mod store;

#[cfg(test)]
mod testing;
#[cfg(test)]
mod tests;

use crate::io::IO;
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::Result;

/// Runs `op` until it is done, waiting on each completion it hands back: for opening a database,
/// whose caller waits for it anyway.
pub(crate) fn drive<T>(io: &Arc<dyn IO>, mut op: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
    loop {
        match op()? {
            IOResult::Done(v) => return Ok(v),
            IOResult::IO(IOCompletions(c)) => io.wait_for_completion(c)?,
        }
    }
}
