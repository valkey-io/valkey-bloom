use std::os::raw::c_void;

use valkey_module::{raw, Status};
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
    /// This function is temporary and will be removed once implemented in valkeymodule-rs .
    pub unsafe fn set_cursor(&self, cursor: u64) -> Status {
        let status = unsafe { raw::RedisModule_DefragCursorSet.unwrap()(self.defrag_ctx, cursor) };
        if status as isize == raw::REDISMODULE_OK {
            Status::Ok
        } else {
            Status::Err
        }
    }

    /// # Safety
    ///
    /// This function is temporary and will be removed once implemented in valkeymodule-rs .
    pub unsafe fn get_cursor(&self) -> Option<u64> {
        let mut cursor: u64 = 0;
        let status =
            unsafe { raw::RedisModule_DefragCursorGet.unwrap()(self.defrag_ctx, &mut cursor) };
        if status as isize == raw::REDISMODULE_OK {
            Some(cursor)
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// This function is temporary and will be removed once implemented in valkeymodule-rs .
    pub unsafe fn should_stop_defrag(&self) -> bool {
        unsafe { raw::RedisModule_DefragShouldStop.unwrap()(self.defrag_ctx) != 0 }
    }
}
