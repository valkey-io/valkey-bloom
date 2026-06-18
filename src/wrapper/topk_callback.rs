use crate::topk::utils::TopKObject;
use std::os::raw::c_void;
use valkey_module::RedisModuleString;

/// # Safety
/// Drop the TopKObject when its key is deleted or replaced.
pub unsafe extern "C" fn topk_free(value: *mut c_void) {
    drop(Box::from_raw(value.cast::<TopKObject>()));
}

/// # Safety
/// Approximate memory usage for the MEMORY USAGE command. Reports the wrapper
/// struct plus the sketch's heap allocations (lobby/heavy cell arrays, decay
/// table, and priority-queue containers). Excludes the variable byte contents
/// of individual tracked keys.
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
/// Free effort hint. TopKObject is a small flat struct plus one CuckooTopK,
/// so freeing is fast and we always do it inline. Returning 1 stays well
/// below Valkey's async-free threshold.
pub unsafe extern "C" fn topk_free_effort(
    _from_key: *mut RedisModuleString,
    _value: *const c_void,
) -> usize {
    1
}
