//! Replicating a database's MVCC logical log.
//!
//! A [`LogReplicator`] installed on a database in MVCC mode receives each
//! committed transaction's logical-log frame once the frame is written, in the
//! order of the log, with the portable metadata that names the tables the
//! transaction wrote. That order is not always the commit timestamps': group
//! commit writes concurrent transactions' frames in the order they reached
//! its queue. Only transactions that depend on none of each other's writes
//! are written out of timestamp order, so applying the frames in log order
//! gives the same rows. The commit does not wait for the frame to reach another
//! database: where the frame is durable is the replicator's to track. Only a
//! replicator that is full makes a writer wait, on the completion it returns,
//! before the writer's commit finishes.
//!
//! The replicator also says whether the database takes commits at all, asked
//! as each commit starts. A commit it refuses fails with
//! [`crate::LimboError::ReadOnly`] before anything of it is written: a
//! follower's database takes no commits of its own. A connection that applies
//! commits replicated from another database
//! (`Connection::set_applies_replicated_commits`) is not asked, and its frames
//! do not reach the replicator.

use std::sync::Arc;

use crate::Completion;

/// A transaction's frame, as written to the logical log.
#[derive(Debug, Clone)]
pub struct ReplicatedFrame {
    /// The transaction's commit timestamp in this database.
    pub commit_ts: u64,
    /// The frame's bytes: its header, its payload with the portable metadata,
    /// and its trailer.
    pub bytes: Arc<[u8]>,
    /// The log's running CRC-32C before the frame, which the CRC in the
    /// frame's trailer continues.
    pub crc_before: u32,
}

/// Receives each transaction's logical-log frame once it is written.
pub trait LogReplicator: Send + Sync {
    /// Whether the database takes a commit now.
    fn takes_commits(&self) -> bool;

    /// Starts replicating `frame`, which is in the log already. The writer
    /// waits on the returned completion before its commit finishes: a
    /// replicator with room returns one complete already. It must succeed; a
    /// failed one fails the commit, whose frame is in the log, as an I/O error
    /// after the write would.
    fn replicate(&self, frame: ReplicatedFrame) -> Completion;
}
