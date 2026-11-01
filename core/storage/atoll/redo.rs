//! How each log record replays on the page it changes. Recovery replays the page store's records
//! first, through the store ([`super::store::PageStore::redo`]); every record here changes one
//! page in the buffer pool, and the records of one page replay in log order.

use super::blob;
use super::btree::record::Record;
use super::log::Kind;
use super::page::PageId;
use super::shared::BytesMut;
use crate::Result;

/// A record that changes one page in the buffer pool.
pub enum PageRecord<'a> {
    Blob(blob::Write<'a>),
    Node(Record<'a>),
}

impl<'a> PageRecord<'a> {
    /// The record as a page change; `None` for records that change no page in the pool.
    pub fn decode(kind: Kind, payload: &'a [u8]) -> Result<Option<Self>> {
        if kind == blob::WRITE {
            return Ok(Some(Self::Blob(blob::Write::decode(payload)?)));
        }
        Ok(Record::decode(kind, payload)?.map(Self::Node))
    }

    pub fn pid(&self) -> PageId {
        match self {
            Self::Blob(w) => w.pid,
            Self::Node(r) => r.pid,
        }
    }

    /// Replays the change on `page`, which does not hold it yet.
    pub fn redo<P: BytesMut + ?Sized>(&self, page: &mut P) -> Result<()> {
        match self {
            Self::Blob(w) => {
                w.redo(page);
                Ok(())
            }
            Self::Node(r) => r.apply(page),
        }
    }
}
