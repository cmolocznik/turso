//! Size classes, page ids and the page header.
//!
//! Page sizes grow by powers of two from [`MIN_PAGE_SIZE`]. A page id names a page, not a place:
//! its size class and its number within the class. Where its current copy lives is the page
//! store's to say, since every write-back goes to a fresh slot.
//!
//! Every page starts with a [`HEADER_SIZE`]-byte header:
//!
//! ```text
//! lsn: u64 | checksum: u32 | class: u8 | reserved: [u8; 3] | pid: u64
//! ```
//!
//! The checksum is CRC32C over the whole page with the checksum field taken as zero, so a copy
//! torn on its way to disk, or a slot never written, reads as invalid. The page id and LSN in the
//! header tell a copy apart from the others the same slot has held: a slot is reused once the copy
//! it held is superseded, and a stale record of the slot must not read the new occupant.

/// log2 of the smallest page size.
pub const MIN_PAGE_SHIFT: u32 = 16;
/// The smallest page size, 64 KiB, as in Umbra.
pub const MIN_PAGE_SIZE: usize = 1 << MIN_PAGE_SHIFT;
/// Number of size classes: 64 KiB to 8 MiB.
pub const CLASS_COUNT: usize = 8;
/// Bytes at the start of every page reserved for the header.
pub const HEADER_SIZE: usize = 24;

const LSN: std::ops::Range<usize> = 0..8;
const CHECKSUM: std::ops::Range<usize> = 8..12;
const CLASS: usize = 12;
const PID: std::ops::Range<usize> = 16..24;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SizeClass(u8);

impl SizeClass {
    pub fn new(index: usize) -> Option<Self> {
        (index < CLASS_COUNT).then_some(Self(index as u8))
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

/// A page's identity on disk: its size class and its slot in that class's file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(u64);

const SLOT_BITS: u32 = 56;
const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;

impl PageId {
    pub fn new(class: SizeClass, number: u64) -> Self {
        assert!(number <= SLOT_MASK, "page number {number} out of range");
        Self(((class.0 as u64) << SLOT_BITS) | number)
    }

    pub fn class(self) -> SizeClass {
        SizeClass((self.0 >> SLOT_BITS) as u8)
    }

    /// The page's number within its class.
    pub fn number(self) -> u64 {
        self.0 & SLOT_MASK
    }

    pub fn raw(self) -> u64 {
        self.0
    }

    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

pub fn lsn(page: &[u8]) -> u64 {
    u64::from_le_bytes(page[LSN].try_into().unwrap())
}

pub fn set_lsn(page: &mut [u8], lsn: u64) {
    page[LSN].copy_from_slice(&lsn.to_le_bytes());
}

fn checksum(page: &[u8]) -> u32 {
    let head = crc32c::crc32c(&page[..CHECKSUM.start]);
    let head = crc32c::crc32c_append(head, &[0; 4]);
    crc32c::crc32c_append(head, &page[CHECKSUM.end..])
}

/// The page id in a page's header.
pub fn pid(page: &[u8]) -> PageId {
    PageId(u64::from_le_bytes(page[PID].try_into().unwrap()))
}

/// Stamps the page id, class and checksum, just before a copy goes to disk.
pub fn seal(page: &mut [u8], pid: PageId) {
    debug_assert_eq!(page.len(), pid.class().size());
    page[CLASS] = pid.class().0;
    page[PID].copy_from_slice(&pid.0.to_le_bytes());
    let crc = checksum(page);
    page[CHECKSUM].copy_from_slice(&crc.to_le_bytes());
}

/// Whether a copy read from disk is whole and a copy of `pid`.
pub fn verify(page: &[u8], pid: PageId) -> bool {
    page.len() == pid.class().size()
        && page[CLASS] == pid.class().0
        && self::pid(page) == pid
        && u32::from_le_bytes(page[CHECKSUM].try_into().unwrap()) == checksum(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_double_from_64_kib() {
        let sizes: Vec<usize> = SizeClass::all().map(SizeClass::size).collect();
        assert_eq!(sizes.first(), Some(&(64 << 10)));
        assert_eq!(sizes.last(), Some(&(8 << 20)));
        assert!(sizes.windows(2).all(|w| w[1] == 2 * w[0]));
        assert_eq!(SizeClass::fitting(1), SizeClass::new(0));
        assert_eq!(SizeClass::fitting(MIN_PAGE_SIZE), SizeClass::new(0));
        assert_eq!(SizeClass::fitting(MIN_PAGE_SIZE + 1), SizeClass::new(1));
        assert_eq!(SizeClass::fitting((8 << 20) + 1), None);
    }

    #[test]
    fn a_page_id_carries_its_class_and_number() {
        let class = SizeClass::new(3).unwrap();
        let pid = PageId::new(class, 12345);
        assert_eq!(pid.class(), class);
        assert_eq!(pid.number(), 12345);
        assert_eq!(PageId::from_raw(pid.raw()), pid);
    }

    #[test]
    fn a_sealed_page_verifies_as_itself_only_and_any_flipped_byte_does_not() {
        let class = SizeClass::new(0).unwrap();
        let pid = PageId::new(class, 7);
        let mut page = vec![0u8; class.size()];
        page[HEADER_SIZE..HEADER_SIZE + 5].copy_from_slice(b"hello");
        set_lsn(&mut page, 42);
        seal(&mut page, pid);
        assert!(verify(&page, pid));
        assert_eq!((lsn(&page), self::pid(&page)), (42, pid));
        assert!(
            !verify(&page, PageId::new(class, 8)),
            "a copy of page 7 read as page 8"
        );
        assert!(!verify(&page, PageId::new(SizeClass::new(1).unwrap(), 7)));
        for at in [0, 9, CLASS, PID.start, HEADER_SIZE, class.size() - 1] {
            let mut torn = page.clone();
            torn[at] ^= 1;
            assert!(!verify(&torn, pid), "flip at {at} went unseen");
        }
        assert!(
            !verify(&vec![0u8; class.size()], pid),
            "a never-written slot verified"
        );
    }
}
