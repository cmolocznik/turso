//! Blob pages hold the bytes of large values. A write to one is logged physically: the bytes and
//! where they go.
//!
//! ```text
//! pid: u64 | offset: u32 | bytes
//! ```

use super::buffer::Exclusive;
use super::log::{framed_len, Admitted, Header, Kind, Log, Lsn};
use super::page::{self, PageId, PageKind, HEADER_SIZE};
use super::shared::BytesMut;
use crate::sync::Arc;
use crate::{LimboError, Result};

/// The log record of a write.
pub const WRITE: Kind = Kind::new(9).unwrap();

/// What a blob page's header says it holds.
pub const KIND: PageKind = PageKind(1);

const FIXED: usize = 12;

/// Bytes of log a write of `len` bytes takes: what to admit before latching the page.
pub fn record_len(len: usize) -> u64 {
    framed_len(FIXED + len)
}

/// Writes `bytes` at `offset` in the blob page `page` holds, logging the write first. Space for
/// it ([`record_len`]) was admitted before the page was latched.
pub fn write(
    log: &Arc<Log>,
    admitted: &mut Admitted,
    page: &mut Exclusive<'_>,
    offset: usize,
    bytes: &[u8],
) -> Lsn {
    let pid = page.pid();
    assert!(
        offset >= HEADER_SIZE && offset + bytes.len() <= pid.class().size(),
        "a blob write of {} bytes at {offset} in {pid:?}",
        bytes.len()
    );
    let mut head = [0u8; FIXED];
    head[..8].copy_from_slice(&pid.raw().to_le_bytes());
    head[8..].copy_from_slice(&(offset as u32).to_le_bytes());
    let header = Header {
        kind: WRITE,
        flags: 0,
        txn: 0,
        prev: 0,
    };
    let lsn = log.append(admitted, &header, &[&head, bytes]);
    apply(&mut page.page_mut(), offset, bytes);
    page.changed(lsn);
    lsn
}

/// Puts `bytes` at `offset` in a blob page: what a write logs, and what replaying it does.
pub fn apply<P: BytesMut + ?Sized>(page: &mut P, offset: usize, bytes: &[u8]) {
    page::set_kind(page, KIND);
    page.write(offset, bytes);
}

/// A write read back from the log.
pub struct Write<'a> {
    pub pid: PageId,
    offset: usize,
    bytes: &'a [u8],
}

impl<'a> Write<'a> {
    pub fn decode(payload: &'a [u8]) -> Result<Self> {
        let corrupt = || LimboError::Corrupt("a blob write record does not decode".into());
        if payload.len() < FIXED {
            return Err(corrupt());
        }
        let pid = PageId::from_raw(u64::from_le_bytes(payload[..8].try_into().unwrap()));
        let offset = u32::from_le_bytes(payload[8..FIXED].try_into().unwrap()) as usize;
        let bytes = &payload[FIXED..];
        if pid.class().index() >= page::CLASS_COUNT
            || offset < HEADER_SIZE
            || offset + bytes.len() > pid.class().size()
        {
            return Err(corrupt());
        }
        Ok(Self { pid, offset, bytes })
    }

    /// Replays the write on the page.
    pub fn redo<P: BytesMut + ?Sized>(&self, page: &mut P) {
        apply(page, self.offset, self.bytes);
    }
}
