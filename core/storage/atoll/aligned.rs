//! Zeroed memory aligned for direct IO, written to disk without a copy.

use crate::io::{Buffer, RegionBuffer, DIRECT_IO_ALIGNMENT};
use crate::sync::Arc;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;

pub struct AlignedBlock {
    ptr: NonNull<u8>,
    layout: Layout,
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
        Self { ptr, layout }
    }

    pub fn len(&self) -> usize {
        self.layout.size()
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
        // SAFETY: allocated with this layout.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}
