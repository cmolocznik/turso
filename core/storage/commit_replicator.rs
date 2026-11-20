//! Replicating a database's commits before they become visible.
//!
//! A [`CommitReplicator`] installed on a database receives each WAL commit's
//! pages, each with its image before the transaction and after it, once the
//! frames are prepared and before any of them is written. The commit waits on
//! the [`Completion`] the replicator returns. When it succeeds, the frames are
//! written, synced and published as without one. When it fails, the commit
//! fails and the transaction rolls back: no reader saw it, and nothing of it is
//! in the WAL for a crash to recover. This is how dqlite replicates SQLite's WAL
//! through Raft: a commit reaches the leader's log and becomes visible only once
//! a quorum holds it.
//!
//! The writer holds the WAL write lock while it waits, so commits reach the
//! replicator one at a time, in the order they commit. Frames written through
//! `Connection::wal_insert_frame`, as a follower applies a replicated commit,
//! publish themselves and do not reach it.

use std::sync::Arc;

use crate::Completion;

/// A page a commit wrote.
#[derive(Debug, Clone)]
pub struct ReplicatedPage {
    /// The page's number, from 1.
    pub number: u32,
    /// The page as the transaction found it: as every replica holds it before
    /// the commit. All zeros for a page the transaction added.
    pub before: Arc<[u8]>,
    /// The page as the commit writes it.
    pub after: Arc<[u8]>,
}

/// The pages of one commit, in the order of its frames.
#[derive(Debug, Clone)]
pub struct ReplicatedCommit {
    /// The database's size in pages after the commit.
    pub db_size: u32,
    pub pages: Vec<ReplicatedPage>,
}

/// Receives each commit before it becomes visible.
pub trait CommitReplicator: Send + Sync {
    /// Starts replicating `commit`. The commit waits until the returned
    /// completion finishes: it is published if the completion succeeds, and
    /// fails with the completion's error otherwise.
    fn replicate(&self, commit: ReplicatedCommit) -> Completion;
}
