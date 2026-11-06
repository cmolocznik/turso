//! Memory aligned for direct IO, written to disk without a copy: allocated zeroed, or lent by
//! the buffer pool out of its region and given back to it when dropped.

use crate::io::{Buffer, RegionBuffer, DIRECT_IO_ALIGNMENT};
use crate::sync::Arc;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;

pub struct AlignedBlock {
    ptr: NonNull<u8>,
    len: usize,
    owner: Owner,
}

/// Where a block's memory came from, and goes back to.
enum Owner {
    Heap(Layout),
    /// Lent: given back by its token, the lender keeping the memory mapped meanwhile.
    Lent(Arc<dyn Lender>, u64),
}

/// Whoever lent a block's memory, which takes it back by the token it lent it under.
pub trait Lender: Send + Sync {
    fn give_back(&self, token: u64);
}

// SAFETY: the block is plain memory; whoever writes into it coordinates who writes where.
unsafe impl Send for AlignedBlock {}
unsafe impl Sync for AlignedBlock {}

impl AlignedBlock {
    /// `len` zeroed bytes aligned to [`DIRECT_IO_ALIGNMENT`]; `len` must be a non-zero multiple
    /// of it.
    pub fn zeroed(len: usize) -> Self {
        assert!(
            len > 0 && len.is_multiple_of(DIRECT_IO_ALIGNMENT),
            "an aligned block of {len} bytes"
        );
        let layout = Layout::from_size_align(len, DIRECT_IO_ALIGNMENT).expect("a valid layout");
        // SAFETY: the layout has a non-zero size.
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self {
            ptr,
            len,
            owner: Owner::Heap(layout),
        }
    }

    /// `len` bytes at `ptr`, lent by `lender` under `token` and given back when the block drops.
    /// Their contents are whatever was there.
    ///
    /// # Safety
    /// The bytes are aligned to [`DIRECT_IO_ALIGNMENT`], stay mapped while `lender` lives, and
    /// are no one else's until given back.
    pub unsafe fn lent(ptr: NonNull<u8>, len: usize, lender: Arc<dyn Lender>, token: u64) -> Self {
        debug_assert!(
            len > 0
                && len.is_multiple_of(DIRECT_IO_ALIGNMENT)
                && (ptr.as_ptr() as usize).is_multiple_of(DIRECT_IO_ALIGNMENT),
            "a lent block of {len} bytes"
        );
        Self {
            ptr,
            len,
            owner: Owner::Lent(lender, token),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the block's memory is lent, and goes back to its lender.
    pub fn is_lent(&self) -> bool {
        matches!(self.owner, Owner::Lent(..))
    }

    /// The bytes, to write: the block held alone, so no IO is in flight from them.
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: inside the block, held exclusively.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// The bytes.
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: inside the block.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// The bytes `[offset, offset + len)` as an IO buffer that keeps the block alive.
    pub fn region(self: &Arc<Self>, offset: usize, len: usize) -> Arc<Buffer> {
        assert!(offset + len <= self.len(), "a region past the block");
        // SAFETY: inside the block, which the buffer keeps alive.
        let region = unsafe {
            RegionBuffer::new(
                self.clone(),
                NonNull::new_unchecked(self.ptr.as_ptr().add(offset)),
                len,
            )
        };
        Arc::new(Buffer::new_region(region))
    }
}

impl Drop for AlignedBlock {
    fn drop(&mut self) {
        match &self.owner {
            // SAFETY: allocated with this layout.
            Owner::Heap(layout) => unsafe { dealloc(self.ptr.as_ptr(), *layout) },
            Owner::Lent(lender, token) => lender.give_back(*token),
        }
    }
}
