//! Blob pages hold the bytes of large values, written whole into their pages once and never
//! changed after (audit 18).

use super::page::{self, PageKind};
use super::shared::BytesMut;

/// What a blob page's header says it holds.
pub const KIND: PageKind = PageKind(1);

/// Puts `bytes` at `offset` in a blob page.
pub fn apply<P: BytesMut + ?Sized>(page: &mut P, offset: usize, bytes: &[u8]) {
    page::set_kind(page, KIND);
    page.write(offset, bytes);
}
