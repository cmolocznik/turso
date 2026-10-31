//! How each log record replays on the page it changes. Recovery replays the page store's records
//! first, through the store ([`super::store::PageStore::redo`]); every record here changes one
//! page in the buffer pool, and the records of one page replay in log order.

use super::blob;
use super::log::Kind;
use super::page::PageId;
use crate::Result;

/// A record that changes one page in the buffer pool.
pub enum PageRecord<'a> {
    Blob(blob::Write<'a>),
}

impl<'a> PageRecord<'a> {
    /// The record as a page change; `None` for records that change no page in the pool.
    pub fn decode(kind: Kind, payload: &'a [u8]) -> Result<Option<Self>> {
        Ok(match kind {
            k if k == blob::WRITE => Some(Self::Blob(blob::Write::decode(payload)?)),
            _ => None,
        })
    }

    pub fn pid(&self) -> PageId {
        match self {
            Self::Blob(w) => w.pid,
        }
    }

    /// Replays the change on `page`, which does not hold it yet.
    pub fn redo(&self, page: &mut [u8]) {
        match self {
            Self::Blob(w) => w.redo(page),
        }
    }
}
