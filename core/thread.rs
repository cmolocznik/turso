#[cfg(shuttle)]
pub(crate) use shuttle_adapter::*;

#[cfg(not(shuttle))]
pub(crate) use std_adapter::*;

#[expect(unused_imports)]
#[cfg(shuttle)]
mod shuttle_adapter {
    pub use shuttle::hint::spin_loop;
    pub use shuttle::thread::{
        current, panicking, scope, sleep, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };

    /// Parks the thread, which a unit of a job's work never does (DESIGN.md 22.1).
    pub fn park() {
        crate::io::refuse_in_unit("a park");
        shuttle::thread::park();
    }
    pub use shuttle::thread_local;
}

#[expect(unused_imports)]
#[cfg(not(shuttle))]
mod std_adapter {
    pub use std::hint::spin_loop;
    pub use std::thread::{
        current, panicking, scope, sleep, spawn, yield_now, Builder, JoinHandle, Scope,
        ScopedJoinHandle, Thread, ThreadId,
    };

    /// Parks the thread, which a unit of a job's work never does (DESIGN.md 22.1).
    pub fn park() {
        crate::io::refuse_in_unit("a park");
        std::thread::park();
    }
    pub use std::thread_local;
}
