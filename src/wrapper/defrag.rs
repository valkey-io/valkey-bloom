use std::os::raw::{c_ulong, c_void};

use valkey_module::raw;

pub struct Defrag {
    pub defrag_ctx: *mut raw::RedisModuleDefragCtx,
}

impl Defrag {
    pub const fn new(defrag_ctx: *mut raw::RedisModuleDefragCtx) -> Self {
        Self { defrag_ctx }
    }

    /// # Safety
    ///
    /// This function is temporary and will be removed once implemented in valkeymodule-rs .
    pub unsafe fn alloc(&self, ptr: *mut c_void) -> *mut c_void {
        unsafe { raw::RedisModule_DefragAlloc.unwrap()(self.defrag_ctx, ptr) }
    }

    /// # Safety
    ///
    /// This function sis temporary and will be removed once implemented in valkeymodule-rs .
    pub unsafe fn curserset(&self, cursor: u64) -> i32 {
        unsafe { raw::RedisModule_DefragCursorSet.unwrap()(self.defrag_ctx, cursor) }
    }

    /// # Safety
    ///
    /// This function sis temporary and will be removed once implemented in valkeymodule-rs .
    pub unsafe fn curserget(&self, cursor: *mut u64) -> i32 {
        unsafe { raw::RedisModule_DefragCursorGet.unwrap()(self.defrag_ctx, cursor) }
    }

    /// # Safety
    ///
    /// This function sis temporary and will be removed once implemented in valkeymodule-rs .
    pub unsafe fn should_stop_defrag(&self) -> i32 {
        unsafe { raw::RedisModule_DefragShouldStop.unwrap()(self.defrag_ctx) }
    }
}
