//! Size classes, page ids and the page header.
//!
//! Page sizes grow by powers of two from [`MIN_PAGE_SIZE`]. A page id carries its size class, so
//! an unswizzled pointer says both where the page lives and how large it is.
//!
//! Every page starts with a [`HEADER_SIZE`]-byte header:
//!
//! ```text
//! lsn: u64 | checksum: u32 | class: u8 | reserved: [u8; 3]
//! ```
//!
//! The checksum is CRC32C over the whole page with the checksum field taken as zero, so a page
//! that was torn on its way to disk, or a slot that was never written, reads as invalid.

/// log2 of the smallest page size.
pub const MIN_PAGE_SHIFT: u32 = 16;
/// The smallest page size, 64 KiB, as in Umbra.
pub const MIN_PAGE_SIZE: usize = 1 << MIN_PAGE_SHIFT;
/// Number of size classes: 64 KiB to 8 MiB.
pub const CLASS_COUNT: usize = 8;
/// Bytes at the start of every page reserved for the header.
pub const HEADER_SIZE: usize = 16;

const LSN: std::ops::Range<usize> = 0..8;
const CHECKSUM: std::ops::Range<usize> = 8..12;
const CLASS: usize = 12;

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
    pub fn new(class: SizeClass, slot: u64) -> Self {
        assert!(slot <= SLOT_MASK, "page slot {slot} out of range");
        Self(((class.0 as u64) << SLOT_BITS) | slot)
    }

    pub fn class(self) -> SizeClass {
        SizeClass((self.0 >> SLOT_BITS) as u8)
    }

    pub fn slot(self) -> u64 {
        self.0 & SLOT_MASK
    }

    /// Byte offset of the page in its class's file.
    pub fn offset(self) -> u64 {
        self.slot() * self.class().size() as u64
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

/// Stamps the class and checksum, just before the page goes to disk.
pub fn seal(page: &mut [u8], class: SizeClass) {
    debug_assert_eq!(page.len(), class.size());
    page[CLASS] = class.0;
    let crc = checksum(page);
    page[CHECKSUM].copy_from_slice(&crc.to_le_bytes());
}

/// Whether a page read from disk is whole and of the class it was read as.
pub fn verify(page: &[u8], class: SizeClass) -> bool {
    page.len() == class.size()
        && page[CLASS] == class.0
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
    fn a_page_id_carries_its_class_and_slot() {
        let class = SizeClass::new(3).unwrap();
        let pid = PageId::new(class, 12345);
        assert_eq!(pid.class(), class);
        assert_eq!(pid.slot(), 12345);
        assert_eq!(pid.offset(), 12345 * (512 << 10));
        assert_eq!(PageId::from_raw(pid.raw()), pid);
    }

    #[test]
    fn a_sealed_page_verifies_and_any_flipped_byte_does_not() {
        let class = SizeClass::new(0).unwrap();
        let mut page = vec![0u8; class.size()];
        page[HEADER_SIZE..HEADER_SIZE + 5].copy_from_slice(b"hello");
        set_lsn(&mut page, 42);
        seal(&mut page, class);
        assert!(verify(&page, class));
        assert_eq!(lsn(&page), 42);
        assert!(!verify(&page, SizeClass::new(1).unwrap()));
        for at in [0, 9, CLASS, HEADER_SIZE, class.size() - 1] {
            let mut torn = page.clone();
            torn[at] ^= 1;
            assert!(!verify(&torn, class), "flip at {at} went unseen");
        }
        assert!(!verify(&vec![0u8; class.size()], class), "a never-written slot verified");
    }
}
