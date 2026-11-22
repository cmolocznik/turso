//! A database's file copied as one read sees it: what a node that falls behind is sent of
//! another's (atoll-forge's decision 102), as bbolt's `Tx.WriteTo` copies etcd's database for a
//! follower its Raft log no longer reaches.

use crate::sync::Arc;
use crate::util::IOExt;
use crate::{Connection, LimboError, Result};

/// What a copy of a database's file holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileCopy {
    /// How many pages it is, each of `page_size` bytes.
    pub pages: u64,
    pub page_size: usize,
    /// Under MVCC, the boundary of the checkpoint the copy is at: the newest commit timestamp
    /// whose rows it holds. `None` for a database not in MVCC mode.
    pub through: Option<u64>,
}

impl Connection {
    /// Copies this database's file page by page, as one read sees it, handing `write` each
    /// page's offset in the file and its bytes, in order. Under MVCC the copy is the B-tree as
    /// the checkpoint the read reaches left it, with its boundary in its own metadata and none
    /// of the logical log: a database opened from it with its log dropped on open
    /// (`DatabaseOpts::with_mvcc_log_dropped_on_open`) holds exactly the commits at or below
    /// [`FileCopy::through`]. The read keeps checkpoints from writing past the pages it reaches
    /// until the copy ends, as any read does; writers go on beside it.
    ///
    /// # Errors
    ///
    /// If the connection is in a transaction already, a page cannot be read, or `write` fails;
    /// the read ends whichever way the copy does.
    pub fn copy_file(
        self: &Arc<Self>,
        write: &mut dyn FnMut(u64, &[u8]) -> Result<()>,
    ) -> Result<FileCopy> {
        if !self.get_auto_commit() {
            return Err(LimboError::InternalError(
                "a file is copied by a connection in no transaction".into(),
            ));
        }
        self.execute("BEGIN")?;
        let copied = self.copy_in_read(write);
        let ended = self.execute("ROLLBACK");
        let copied = copied?;
        ended?;
        Ok(copied)
    }

    fn copy_in_read(
        self: &Arc<Self>,
        write: &mut dyn FnMut(u64, &[u8]) -> Result<()>,
    ) -> Result<FileCopy> {
        // A read begins the transaction: it pins the pages the file is at and, under MVCC, the
        // checkpoint boundary they hold.
        self.execute("SELECT 1 FROM sqlite_schema LIMIT 1")?;
        let pager = self.get_pager();
        let pages = u64::from(
            pager
                .io
                .block(|| pager.with_header(|header| header.database_size.get()))?,
        );
        let page_size = pager.usable_space() + pager.get_reserved_space().unwrap_or(0) as usize;
        let pending = pager.pending_byte_page_id().map(u64::from);
        let zeros = vec![0u8; page_size];
        for pgno in 1..=pages {
            let offset = (pgno - 1) * page_size as u64;
            // The page the lock byte falls on is never written, as SQLite leaves it.
            if pending == Some(pgno) {
                write(offset, &zeros)?;
                continue;
            }
            let number = i64::try_from(pgno)
                .map_err(|_| LimboError::InternalError(format!("page {pgno} past i64")))?;
            let (page, completion) = pager.io.block(|| pager.read_page(number))?;
            if let Some(completion) = completion {
                pager.io.wait_for_completion(completion)?;
            }
            write(offset, page.get_contents().as_ptr())?;
        }
        // The boundary the read pinned, not the latest: a checkpoint may have gone past it since.
        let through = match self.mv_store().as_ref() {
            Some(store) => {
                let tx = self.get_mv_tx_id().ok_or_else(|| {
                    LimboError::InternalError("a read began no MVCC transaction".into())
                })?;
                Some(store.durable_at_begin_of(tx).ok_or_else(|| {
                    LimboError::InternalError("the read's MVCC transaction is gone".into())
                })?)
            }
            None => None,
        };
        Ok(FileCopy {
            pages,
            page_size,
            through,
        })
    }
}
