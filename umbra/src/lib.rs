//! Prototype of an Umbra-style storage layer (Neumann and Freitag, CIDR 2020).
//!
//! - [`page`]: size classes of exponentially growing page sizes, page ids, and the page header.
//! - [`store`]: the page store, one file per size class, disk space reused only by pages of the
//!   same size.
//! - [`latch`]: the versioned latch each buffer frame carries.
//! - [`buffer`]: the buffer manager. One virtual memory region per size class, each as large as
//!   the whole memory budget, so pages of every size share one budget without fragmentation.
//!   Pages are found through swizzled pointers, and evicted through LeanStore's cooling stage.

pub mod buffer;
pub mod latch;
pub mod page;
pub mod store;
