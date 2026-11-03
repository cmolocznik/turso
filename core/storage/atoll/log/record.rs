//! How a record is framed in the log.
//!
//! ```text
//! payload_len: u32 | kind: u8 | flags: u8 | index: u8 | size: u8 | txn: u64 | prev: u64 | payload | crc32c: u32 | zeros
//! ```
//!
//! `index` and `size` place a record in a group of records appended as one ([`Place`]): recovery
//! replays all of a group or none of it. A record outside any group has both 0.
//!
//! `prev` is where the transaction's previous group starts ([`GroupStart`]), or [`NO_GROUP`].
//!
//! A record starts at a multiple of [`ALIGN`] and is padded with zeros to the next one. The CRC
//! covers the record's position, its first 24 bytes and its payload, so a record read at any other
//! position, a torn record, and whatever an earlier use of the disk left there all read as the
//! end of the log. A record's LSN is the position just past it.
//!
//! Kind 0 is padding. It fills the rest of a [`BLOCK`] after a flush, so the next flush starts in
//! a fresh block and never rewrites bytes already durable:
//!
//! ```text
//! len: u32 | kind = 0: u8 | zeros: 3 | position: u64 | zeros
//! ```
//!
//! Padding has no CRC; its own position stands in for one, so stale padding is not mistaken for
//! the real thing. It is at least [`ALIGN`] bytes and ends exactly at a block boundary.

use crate::io::DIRECT_IO_ALIGNMENT;

pub type Lsn = u64;

/// Where a group of records starts, or a record outside any group: the position a reader begins
/// at to read it whole. A type apart from [`Lsn`], where a record ends: LSNs order a page's
/// changes and mark what is durable, group starts locate records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupStart(u64);

impl GroupStart {
    /// The group starting at `at`, a position records start at.
    pub fn at(at: Lsn) -> Self {
        assert!(
            at.is_multiple_of(ALIGN),
            "a group starting at {at}, off the {ALIGN}-byte grid records start on"
        );
        Self(at)
    }

    pub fn get(self) -> Lsn {
        self.0
    }

    /// `group` as a header or a superblock holds it: [`NO_GROUP`] for none.
    pub fn encode(group: Option<GroupStart>) -> u64 {
        group.map_or(NO_GROUP, GroupStart::get)
    }

    /// What `raw`, as [`encode`](Self::encode) wrote it, names; `None` if it names neither a
    /// group nor none.
    pub fn decode(raw: u64) -> Option<Option<GroupStart>> {
        match raw {
            NO_GROUP => Some(None),
            at if at.is_multiple_of(ALIGN) => Some(Some(GroupStart(at))),
            _ => None,
        }
    }
}

/// No group, where a position names one. Groups start on the [`ALIGN`] grid, which this is off;
/// 0 does name a group.
pub const NO_GROUP: u64 = u64::MAX;

/// Where records start.
pub const ALIGN: u64 = 16;
/// The unit the log is written in; a flush ends on a block boundary.
pub const BLOCK: u64 = DIRECT_IO_ALIGNMENT as u64;
/// The fixed fields before the payload.
pub const HEADER: usize = 24;
const TRAILER: usize = 4;
/// The largest payload: twice the largest page class, so a record can carry a whole page with
/// room to spare.
pub const MAX_PAYLOAD: usize = 2 * (8 << 20);
const PADDING: u8 = 0;

/// What a record is about. Its owner (the page store, a B-tree, the transaction manager) defines
/// its kinds; kind 0 is the log's own padding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Kind(u8);

impl Kind {
    pub const fn new(kind: u8) -> Option<Self> {
        if kind == PADDING {
            None
        } else {
            Some(Self(kind))
        }
    }

    pub fn get(self) -> u8 {
        self.0
    }
}

/// The fixed fields of a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub kind: Kind,
    pub flags: u8,
    /// The transaction that wrote it; 0 for none.
    pub txn: u64,
    /// Where the transaction's previous group starts; `None` for none.
    pub prev: Option<GroupStart>,
}

/// Where a record stands in a group of records appended as one: its index, and how many records
/// the group has. A group's records are contiguous, so its first record's start is where a reader
/// must begin to read any of them. Outside a group, both are 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Place {
    pub index: u8,
    pub size: u8,
}

impl Place {
    pub const ALONE: Self = Self { index: 0, size: 0 };

    pub fn grouped(self) -> bool {
        self.size > 0
    }

    pub fn first(self) -> bool {
        self.index == 0
    }

    pub fn last(self) -> bool {
        self.index + 1 == self.size
    }

    fn valid(self) -> bool {
        if self.size == 0 {
            self.index == 0
        } else {
            self.size >= 2 && self.index < self.size
        }
    }
}

/// Bytes a record with `payload_len` bytes of payload takes in the log.
pub fn framed_len(payload_len: usize) -> u64 {
    ((HEADER + payload_len + TRAILER) as u64).next_multiple_of(ALIGN)
}

/// The first [`HEADER`] bytes of a record.
pub fn header_bytes(header: &Header, place: Place, payload_len: usize) -> [u8; HEADER] {
    let mut out = [0u8; HEADER];
    out[0..4].copy_from_slice(&(payload_len as u32).to_le_bytes());
    out[4] = header.kind.0;
    out[5] = header.flags;
    out[6] = place.index;
    out[7] = place.size;
    out[8..16].copy_from_slice(&header.txn.to_le_bytes());
    out[16..24].copy_from_slice(&GroupStart::encode(header.prev).to_le_bytes());
    out
}

/// The CRC of a record starting at `start`.
pub fn crc(start: Lsn, header: &[u8; HEADER], payload: &[&[u8]]) -> u32 {
    let mut crc = crc32c::crc32c(&start.to_le_bytes());
    crc = crc32c::crc32c_append(crc, header);
    for piece in payload {
        crc = crc32c::crc32c_append(crc, piece);
    }
    crc
}

/// Padding from `start` to the end of its block.
pub fn padding(start: Lsn) -> Vec<u8> {
    let len = start.next_multiple_of(BLOCK) - start;
    debug_assert!(len >= ALIGN && start.is_multiple_of(ALIGN));
    let mut out = vec![0u8; len as usize];
    out[0..4].copy_from_slice(&(len as u32).to_le_bytes());
    out[4] = PADDING;
    out[8..16].copy_from_slice(&start.to_le_bytes());
    out
}

/// A record read from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<'a> {
    pub header: Header,
    pub place: Place,
    pub payload: &'a [u8],
    /// Where it starts.
    pub start: Lsn,
    /// Its LSN: where it ends.
    pub lsn: Lsn,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Parsed<'a> {
    Record(Record<'a>),
    /// Padding to the end of the block, this many bytes long.
    Padding(u64),
    /// No valid record starts here: the log ends.
    End,
    /// The bytes given stop before what is at this position does; it needs this many.
    Need(usize),
}

/// What starts at position `at`, given the log's bytes from there on.
pub fn parse(at: Lsn, bytes: &[u8]) -> Parsed<'_> {
    if !at.is_multiple_of(ALIGN) {
        return Parsed::End;
    }
    let Some(first) = bytes.get(..ALIGN as usize) else {
        return Parsed::Need(ALIGN as usize);
    };
    let len = u32::from_le_bytes(first[0..4].try_into().unwrap()) as usize;
    if first[4] == PADDING {
        let position = u64::from_le_bytes(first[8..16].try_into().unwrap());
        let fits = len as u64 >= ALIGN && len as u64 == at.next_multiple_of(BLOCK) - at;
        return if fits && position == at && first[5..8] == [0, 0, 0] {
            Parsed::Padding(len as u64)
        } else {
            Parsed::End
        };
    }
    if len > MAX_PAYLOAD {
        return Parsed::End;
    }
    let framed = framed_len(len) as usize;
    let Some(record) = bytes.get(..framed) else {
        return Parsed::Need(framed);
    };
    let header: [u8; HEADER] = record[..HEADER].try_into().unwrap();
    let payload = &record[HEADER..HEADER + len];
    let stored = u32::from_le_bytes(
        record[HEADER + len..HEADER + len + TRAILER]
            .try_into()
            .unwrap(),
    );
    let place = Place {
        index: header[6],
        size: header[7],
    };
    if stored != crc(at, &header, &[payload]) || !place.valid() {
        return Parsed::End;
    }
    let Some(prev) = GroupStart::decode(u64::from_le_bytes(header[16..24].try_into().unwrap()))
    else {
        return Parsed::End;
    };
    Parsed::Record(Record {
        place,
        header: Header {
            kind: Kind(header[4]),
            flags: header[5],
            txn: u64::from_le_bytes(header[8..16].try_into().unwrap()),
            prev,
        },
        payload,
        start: at,
        lsn: at + framed as u64,
    })
}

/// A whole record as it goes into the log.
#[cfg(test)]
pub fn encode(start: Lsn, header: &Header, place: Place, payload: &[u8]) -> Vec<u8> {
    let head = header_bytes(header, place, payload.len());
    let crc = crc(start, &head, &[payload]);
    let mut out = Vec::with_capacity(framed_len(payload.len()) as usize);
    out.extend_from_slice(&head);
    out.extend_from_slice(payload);
    out.extend_from_slice(&crc.to_le_bytes());
    out.resize(framed_len(payload.len()) as usize, 0);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> Header {
        Header {
            kind: Kind::new(7).unwrap(),
            flags: 3,
            txn: 42,
            prev: Some(GroupStart::at(4096)),
        }
    }

    #[test]
    fn a_previous_group_at_zero_reads_back_apart_from_none() {
        for prev in [None, Some(GroupStart::at(0)), Some(GroupStart::at(4096))] {
            let h = Header { prev, ..header() };
            let bytes = encode(64, &h, Place::ALONE, b"x");
            let Parsed::Record(r) = parse(64, &bytes) else {
                panic!("no record for {prev:?}")
            };
            assert_eq!(r.header.prev, prev);
        }
    }

    #[test]
    fn a_prev_off_the_grid_is_no_record() {
        let mut head = header_bytes(&header(), Place::ALONE, 1);
        head[16..24].copy_from_slice(&8u64.to_le_bytes());
        let crc = crc(64, &head, &[b"x"]);
        let mut bytes = head.to_vec();
        bytes.push(b'x');
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes.resize(framed_len(1) as usize, 0);
        assert_eq!(parse(64, &bytes), Parsed::End);
    }

    #[test]
    fn a_record_reads_back_as_written_and_only_at_its_own_position() {
        let at = 8192 + 48;
        let place = Place { index: 1, size: 3 };
        let bytes = encode(at, &header(), place, b"payload");
        let Parsed::Record(r) = parse(at, &bytes) else {
            panic!("no record")
        };
        assert_eq!(
            (r.header, r.place, r.payload, r.start),
            (header(), place, &b"payload"[..], at)
        );
        assert_eq!(r.lsn, at + bytes.len() as u64);
        assert_eq!(
            parse(at + ALIGN, &bytes),
            Parsed::End,
            "read at another position"
        );
    }

    #[test]
    fn any_flipped_byte_ends_the_log() {
        let bytes = encode(0, &header(), Place::ALONE, b"some payload bytes");
        let crc_end = HEADER + 18 + TRAILER;
        for at in 0..crc_end {
            let mut torn = bytes.clone();
            torn[at] ^= 0x10;
            assert!(
                !matches!(parse(0, &torn), Parsed::Record(_)),
                "a flip at byte {at} went unseen"
            );
        }
    }

    #[test]
    fn short_bytes_ask_for_more_and_zeros_end_the_log() {
        let bytes = encode(0, &header(), Place::ALONE, &[1; 100]);
        assert_eq!(parse(0, &bytes[..10]), Parsed::Need(ALIGN as usize));
        assert_eq!(parse(0, &bytes[..40]), Parsed::Need(bytes.len()));
        assert_eq!(parse(0, &[0u8; 64]), Parsed::End);
    }

    #[test]
    fn padding_fills_to_the_block_end_and_reads_only_where_it_was_written() {
        let at = BLOCK + 3 * ALIGN;
        let pad = padding(at);
        assert_eq!(at + pad.len() as u64, 2 * BLOCK);
        assert_eq!(parse(at, &pad), Parsed::Padding(pad.len() as u64));
        // The same bytes left behind at another position are not padding there.
        assert_eq!(parse(at - BLOCK, &pad), Parsed::End);
        assert_eq!(parse(at + ALIGN, &pad[ALIGN as usize..]), Parsed::End);
    }

    #[test]
    fn kind_zero_is_the_logs_own() {
        assert_eq!(Kind::new(0), None);
        assert_eq!(Kind::new(1).map(Kind::get), Some(1));
    }
}
