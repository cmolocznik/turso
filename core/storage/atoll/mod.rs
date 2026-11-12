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
//! - [`sql`]: Turso's SQL on it (DESIGN.md §17): the system trees and the catalog.
//!
//! Every IO goes through Turso's [`crate::io::IO`] and returns its completion, so this runs under
//! the VDBE's `IOResult` stepping on every backend: io_uring, the syscall backend and memory.

// Every lock in Atoll is one of its own (`locks`); the lint resolves each path to the type it
// names (`core/clippy.toml`).
#![deny(clippy::disallowed_types)]

mod aligned;
mod arc_cell;
mod bits;
mod blob;
mod btree;
mod buffer;
mod checkpoint;
mod control;
mod cooperative;
mod crc;
pub mod db;
mod epoch;
// The executor's core is built on test jobs before any job of the database moves onto it (DESIGN.md
// 22.16 step 6).
#[cfg_attr(not(test), expect(dead_code))]
mod executor;
// A production build carries only what production calls: an item whose only callers are tests
// is compiled for tests alone, and so are `floors` and `stream`, whose consumers (the collector,
// the stream catalog) are not built.
#[cfg(test)]
mod floors;
mod guarded;
mod latch;
mod locks;
mod log;
mod meta;
mod notify;
mod once;
mod page;
mod park;
mod pins;
mod prefix;
mod radix;
mod recovery;
mod replay;
mod requests;
mod segmented;
mod shared;
mod solo;
pub mod sql;
mod stack;
mod store;
#[cfg(test)]
mod stream;
mod txn;
mod unit;
mod unwind;

#[cfg(all(test, not(atoll_loom), not(shuttle), not(miri)))]
mod bench;
#[cfg(test)]
mod testing;
#[cfg(test)]
mod tests;

use crate::io::{Completion, IO};
use crate::sync::Arc;
use crate::types::{IOCompletions, IOResult};
use crate::{CompletionError, Result};

/// How a write of `len` bytes ended: one that landed short failed. A write's completion counts a
/// short write as done, so every write checks its count with this.
pub(in crate::storage::atoll) fn whole(
    len: usize,
    res: std::result::Result<i32, CompletionError>,
) -> std::result::Result<(), CompletionError> {
    match res {
        Ok(n) if usize::try_from(n) == Ok(len) => Ok(()),
        Ok(_) => Err(CompletionError::ShortWrite),
        Err(e) => Err(e),
    }
}

/// A completion to hand a write of `len` bytes, and one to wait on: it ends when the write does,
/// failed if the write failed or landed short.
pub(in crate::storage::atoll) fn whole_write(len: usize) -> (Completion, Completion) {
    let ended = crate::storage::atoll::notify::wait();
    let done = ended.clone();
    let write = Completion::new_write(move |res| match whole(len, res) {
        Ok(()) => done.complete(0),
        Err(e) => done.error(e),
    });
    (write, ended)
}

/// Runs `op` until it is done, waiting on each completion it hands back through [`IO::wait`]: for
/// opening a database, whose caller waits for it anyway.
#[turso_macros::atoll_entry]
pub(crate) fn drive<T>(io: &Arc<dyn IO>, op: impl FnMut() -> Result<IOResult<T>>) -> Result<T> {
    drive_as(buffer::Owner::new(), io, op)
}

/// [`drive`] for an operation of connection `conn`: a facade method of the connection running it
/// with a live transaction under its cell, which carries the connection's identity, so a shortage
/// it meets takes the place of the connection's own suspended operation rather than waiting
/// behind it (DESIGN.md 9.1.3, *Revoking an inactive tenure*).
#[turso_macros::atoll_entry]
pub(in crate::storage::atoll) fn drive_for<T>(
    conn: &Arc<buffer::Conn>,
    io: &Arc<dyn IO>,
    op: impl FnMut() -> Result<IOResult<T>>,
) -> Result<T> {
    drive_as(buffer::Owner::of(conn.clone()), io, op)
}

fn drive_as<T>(
    mut owner: buffer::Owner,
    io: &Arc<dyn IO>,
    mut op: impl FnMut() -> Result<IOResult<T>>,
) -> Result<T> {
    // The operation's top: a shortage it meets is reserved here, holding nothing.
    loop {
        match owner.call(&mut op)? {
            IOResult::Done(v) => return Ok(v),
            IOResult::IO(IOCompletions(c)) => {
                if c.finished() {
                    // A yield: give the processor to whoever holds what the operation needs.
                    crate::thread::yield_now();
                } else {
                    crate::storage::atoll::park::wait(io.as_ref(), &c);
                }
                if let Some(e) = c.get_error() {
                    return Err(crate::LimboError::CompletionError(e));
                }
            }
        }
    }
}
