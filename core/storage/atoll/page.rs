//! Size classes, page ids and the page header.
//!
//! Page sizes double from [`MIN_PAGE_SIZE`] (4 KiB) to 8 MiB. A page id names a page, not a
//! place: its size class and its number within the class. Where its current copy lives is the
//! page store's to say, since every write goes to a fresh slot.
//!
//! Every page starts with a [`HEADER_SIZE`]-byte header:
//!
//! ```text
//! seq: u64 | checksum: u32 | class: u8 | kind: u8 | flags: u16 | pid: u64
//! ```
//!
//! The checksum is CRC32C over the whole page with the checksum field taken as zero, so a copy
//! torn on its way to disk, or a slot never written, reads as invalid. `seq` is its owner's: a data
//! page's change count when the copy was taken, a metadata page's checkpoint number. The page id
//! and `seq` tell a copy apart from the others the same place has held.

/// log2 of the smallest page size.
use super::shared::{Bytes, BytesMut};

pub const MIN_PAGE_SHIFT: u32 = 12;
/// The smallest page size: 4 KiB, the unit of direct IO and LeanStore's page size.
pub const MIN_PAGE_SIZE: usize = 1 << MIN_PAGE_SHIFT;
/// Number of size classes: 4 KiB to 8 MiB.
pub const CLASS_COUNT: usize = 12;
/// The largest page: the last class's.
pub const MAX_PAGE_SIZE: usize = MIN_PAGE_SIZE << (CLASS_COUNT - 1);
/// Bytes at the start of every page reserved for the header.
pub const HEADER_SIZE: usize = 24;

const SEQ: std::ops::Range<usize> = 0..8;
const CHECKSUM: std::ops::Range<usize> = 8..12;
const CLASS: usize = 12;
const KIND: usize = 13;
const PID: std::ops::Range<usize> = 16..24;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SizeClass(u8);

impl SizeClass {
    pub const fn new(index: usize) -> Option<Self> {
        if index < CLASS_COUNT {
            Some(Self(index as u8))
        } else {
            None
        }
    }

    /// The smallest class whose pages hold `len` bytes, header included.
    pub fn fitting(len: usize) -> Option<Self> {
        let size = len.max(MIN_PAGE_SIZE).next_power_of_two();
        Self::new((size.trailing_zeros() - MIN_PAGE_SHIFT) as usize)
    }

    pub fn index(self) -> usize {
        self.0 as usize
    }

    pub fn size(self) -> usize {
        MIN_PAGE_SIZE << self.0
    }

    pub fn all() -> impl Iterator<Item = SizeClass> {
        (0..CLASS_COUNT).map(|i| SizeClass(i as u8))
    }
}

/// What a page holds. Each owner of pages defines its kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageKind(pub u8);

/// A page's identity: its size class and its number within the class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(u64);

const NUMBER_BITS: u32 = 56;
const NUMBER_MASK: u64 = (1 << NUMBER_BITS) - 1;
/// The number bit naming a temporary page (DESIGN.md §6.4), which the page store never sees. The
/// store's numbers stay below it: 2^55 pages of 4 KiB is 128 EiB.
const TEMPORARY: u64 = 1 << (NUMBER_BITS - 1);

impl PageId {
    pub fn new(class: SizeClass, number: u64) -> Self {
        assert!(number <= NUMBER_MASK, "page number {number} out of range");
        Self(((class.0 as u64) << NUMBER_BITS) | number)
    }

    /// The temporary page `number` of `class`.
    pub fn temporary(class: SizeClass, number: u64) -> Self {
        assert!(
            number < TEMPORARY,
            "temporary page number {number} out of range"
        );
        Self::new(class, TEMPORARY | number)
    }

    pub fn is_temporary(self) -> bool {
        self.0 & TEMPORARY != 0
    }

    /// The number of a temporary page among its class's temporary pages.
    pub fn temporary_number(self) -> u64 {
        debug_assert!(self.is_temporary());
        self.number() & !TEMPORARY
    }

    pub fn class(self) -> SizeClass {
        SizeClass((self.0 >> NUMBER_BITS) as u8)
    }

    /// The page's number within its class.
    pub fn number(self) -> u64 {
        self.0 & NUMBER_MASK
    }

    pub fn raw(self) -> u64 {
        self.0
    }

    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// The id spread over 64 bits by SplitMix64's finalizer: page numbers are dense, and their
    /// low bits alone would crowd hash buckets and partitions.
    pub fn hash(self) -> u64 {
        let mut z = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

pub fn seq<P: Bytes + ?Sized>(page: &P) -> u64 {
    page.u64_at(SEQ.start)
}

pub fn set_seq<P: BytesMut + ?Sized>(page: &mut P, seq: u64) {
    page.put_u64(SEQ.start, seq);
}

pub fn kind<P: Bytes + ?Sized>(page: &P) -> PageKind {
    PageKind(page.u8_at(KIND))
}

pub fn set_kind<P: BytesMut + ?Sized>(page: &mut P, kind: PageKind) {
    page.put_u8(KIND, kind.0);
}

fn checksum<P: Bytes + ?Sized>(page: &P) -> u32 {
    let mut crc = 0;
    page.pieces(0..CHECKSUM.start, |b| crc = super::crc::append(crc, b));
    crc = super::crc::append(crc, &[0; 4]);
    page.pieces(CHECKSUM.end..page.len(), |b| {
        crc = super::crc::append(crc, b)
    });
    crc
}

/// The page id in a page's header.
pub fn pid<P: Bytes + ?Sized>(page: &P) -> PageId {
    PageId(page.u64_at(PID.start))
}

/// Stamps the page id, class and checksum, just before a copy goes to disk.
pub fn seal<P: BytesMut + ?Sized>(page: &mut P, pid: PageId) {
    debug_assert_eq!(page.len(), pid.class().size());
    page.put_u8(CLASS, pid.class().0);
    page.put_u64(PID.start, pid.0);
    let crc = checksum(page);
    page.put_u32(CHECKSUM.start, crc);
}

/// Whether a copy read from disk is whole and a copy of `pid`.
pub fn verify<P: Bytes + ?Sized>(page: &P, pid: PageId) -> bool {
    page.len() == pid.class().size()
        && page.u8_at(CLASS) == pid.class().0
        && self::pid(page) == pid
        && page.u32_at(CHECKSUM.start) == checksum(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::atoll::shared::Shared;

    #[test]
    fn classes_double_from_4_kib_to_8_mib() {
        let sizes: Vec<usize> = SizeClass::all().map(SizeClass::size).collect();
        assert_eq!(sizes.first(), Some(&(4 << 10)));
        assert_eq!(sizes.last(), Some(&(8 << 20)));
        assert!(sizes.windows(2).all(|w| w[1] == 2 * w[0]));
        assert_eq!(SizeClass::fitting(1), SizeClass::new(0));
        assert_eq!(SizeClass::fitting(MIN_PAGE_SIZE + 1), SizeClass::new(1));
        assert_eq!(SizeClass::fitting((8 << 20) + 1), None);
    }

    #[test]
    fn a_sealed_page_verifies_as_itself_only_and_any_flipped_byte_does_not() {
        let class = SizeClass::new(0).unwrap();
        let pid = PageId::new(class, 7);
        let mut page = vec![0u8; class.size()];
        page[HEADER_SIZE..HEADER_SIZE + 5].copy_from_slice(b"hello");
        set_seq(&mut page, 42);
        set_kind(&mut page, PageKind(3));
        seal(&mut page, pid);
        assert!(verify(&page, pid));
        assert_eq!(
            (seq(&page), self::pid(&page), kind(&page)),
            (42, pid, PageKind(3))
        );
        assert!(
            !verify(&page, PageId::new(class, 8)),
            "a copy of page 7 read as page 8"
        );
        for at in [0, 9, CLASS, KIND, PID.start, HEADER_SIZE, class.size() - 1] {
            let mut torn = page.clone();
            torn[at] ^= 1;
            assert!(!verify(&torn, pid), "flip at {at} went unseen");
        }
        assert!(
            !verify(&vec![0u8; class.size()], pid),
            "a never-written slot verified"
        );
        // Sealed in a frame, as a write-out seals it, the page is the same bytes.
        let mut unsealed = page.clone();
        unsealed[CHECKSUM].fill(0);
        let mut shared = Shared::zeroed(class.size());
        let mut w = shared.owned();
        w.write(0, &unsealed);
        seal(&mut w, pid);
        assert_eq!(w.copied(0..class.size()), page);
        assert!(verify(w.shared(), pid));
    }
}
