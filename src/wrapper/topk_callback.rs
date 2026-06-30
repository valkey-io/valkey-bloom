use crate::topk::data_type::ValkeyDataType;
use crate::topk::utils::TopKObject;
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use valkey_module::digest::Digest;
use valkey_module::logging;
use valkey_module::raw;
use valkey_module::RedisModuleString;

/// # Safety
pub unsafe extern "C" fn topk_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let v = &*value.cast::<TopKObject>();
    raw::save_unsigned(rdb, v.seed());
    raw::save_unsigned(rdb, v.num_items());
    raw::save_slice(rdb, &v.sketch().to_bytes());
}

/// # Safety
pub unsafe extern "C" fn topk_rdb_load(rdb: *mut raw::RedisModuleIO, encver: c_int) -> *mut c_void {
    if let Some(item) = <TopKObject as ValkeyDataType>::load_from_rdb(rdb, encver) {
        let bb = Box::new(item);
        Box::into_raw(bb).cast::<libc::c_void>()
    } else {
        logging::log_warning("Failed to restore topk object.");
        null_mut()
    }
}

/// # Safety
/// Drop the TopKObject when its key is deleted or replaced.
pub unsafe extern "C" fn topk_free(value: *mut c_void) {
    drop(Box::from_raw(value.cast::<TopKObject>()));
}

/// # Safety
/// Approximate memory usage for the MEMORY USAGE command. Reports the wrapper
/// struct plus the sketch's heap allocations (lobby/heavy cell arrays, decay
/// table, priority-queue containers, and the buffers of tracked items). See
/// `TopKObject::memory_usage` for what the estimate still omits.
pub unsafe extern "C" fn topk_mem_usage(value: *const c_void) -> usize {
    let v = &*value.cast::<TopKObject>();
    v.memory_usage()
}

/// # Safety
/// Raw handler for the COPY command. Builds a deep copy of the source
/// TopKObject, duplicating the sketch contents and item count so the copy is
/// a faithful clone rather than an empty sketch.
pub unsafe extern "C" fn topk_copy(
    _from_key: *mut RedisModuleString,
    _to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let curr = &*value.cast::<TopKObject>();
    let new_item = TopKObject::create_copy_from(curr);
    let bb = Box::new(new_item);
    Box::into_raw(bb).cast::<libc::c_void>()
}

/// # Safety
/// Raw handler for the TopK digest callback.
pub unsafe extern "C" fn topk_digest(md: *mut raw::RedisModuleDigest, value: *mut c_void) {
    let dig = Digest::new(md);
    let val = &*(value.cast::<TopKObject>());
    val.debug_digest(dig);
}

/// # Safety
/// Free effort hint. TopKObject is a small flat struct plus one CuckooTopK,
/// so freeing is fast and we always do it inline. Returning 1 stays well
/// below Valkey's async-free threshold.
pub unsafe extern "C" fn topk_free_effort(
    _from_key: *mut RedisModuleString,
    _value: *const c_void,
) -> usize {
    1
}
