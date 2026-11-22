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
//! The replicator also says how large a frame it takes. A commit whose frame
//! would be larger fails with [`crate::LimboError::TooBig`] before anything of
//! it is written, as etcd refuses a request over `--max-request-bytes`.
//!
//! The replicator also says whether the database takes commits at all, asked
//! as each commit starts. A commit it refuses fails with
//! [`crate::LimboError::ReadOnly`] before anything of it is written: a
//! follower's database takes no commits of its own. A connection that applies
//! commits replicated from another database
//! (`Connection::set_applies_replicated_commits`) is not asked, and its frames
//! do not reach the replicator. The sync engine's `ReplicatedLogApplier`
//! applies another database's frames on such a connection, row by row.
//!
//! A checkpoint writes the database file only through a cut the replicator
//! gives ([`LogReplicator::checkpoint_cut`]), so that the file holds exactly
//! what the replication had reached there. The database reopens from its file
//! alone (`DatabaseOpts::drop_mvcc_log_on_open`), its logical log dropped, and
//! the replication replays what came after the cut.

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

    /// The most bytes a frame given to [`Self::replicate`] may hold. A commit
    /// whose frame would hold more fails with [`crate::LimboError::TooBig`]
    /// and rolls back before it is written. The default takes any frame.
    fn frame_limit(&self) -> usize {
        usize::MAX
    }

    /// Starts replicating `frame`, which is in the log already. The writer
    /// waits on the returned completion before its commit finishes: a
    /// replicator with room returns one complete already. It must succeed; a
    /// failed one fails the commit, whose frame is in the log, as an I/O error
    /// after the write would.
    fn replicate(&self, frame: ReplicatedFrame) -> Completion;

    /// The newest commit timestamp a checkpoint may write to the database
    /// file, given `newest`, the newest it could. Every commit at or below
    /// `newest` is committed and its frame written: given to
    /// [`Self::replicate`], or committed by a connection that applies
    /// replicated commits. Every commit after it has a later timestamp. The
    /// checkpoint writes every commit at or below the cut and none above it,
    /// with page 1 as of the cut, and the database's next open starts from that.
    /// So the cut must be a timestamp no frame written after it, in log order, is
    /// at or below. A cut above `newest` counts as `newest`, and one at or below
    /// the last checkpoint's moves nothing. The default writes them all.
    fn checkpoint_cut(&self, newest: u64) -> u64 {
        newest
    }
}
