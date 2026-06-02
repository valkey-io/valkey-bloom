use crate::topk::utils::TopKObject;
use std::os::raw::c_void;
use valkey_module::RedisModuleString;

/// # Safety
/// Drop the TopKObject when its key is deleted or replaced.
pub unsafe extern "C" fn topk_free(value: *mut c_void) {
    drop(Box::from_raw(value.cast::<TopKObject>()));
}

/// # Safety
/// Approximate memory usage for the MEMORY USAGE command. Currently reports
/// only the size of the wrapper struct; the heap allocations CuckooTopK
/// performs internally (lobby + heavy slots) are not yet accounted for.
/// Refine when TOPK.ADD lands and we have a stable view into the sketch.
pub unsafe extern "C" fn topk_mem_usage(value: *const c_void) -> usize {
    let _v = &*value.cast::<TopKObject>();
    std::mem::size_of::<TopKObject>()
}

/// # Safety
/// Raw handler for the COPY command. Builds a fresh TopKObject with the same
/// parameters as the source. The sketch is empty after copy, which is
/// correct today because we don't persist sketch contents yet — when
/// TOPK.ADD lands this needs a real deep copy of the heavy/lobby arrays.
pub unsafe extern "C" fn topk_copy(
    _from_key: *mut RedisModuleString,
    _to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let curr = &*value.cast::<TopKObject>();
    let new_item = TopKObject::new_reserved(
        curr.k(),
        curr.width(),
        curr.depth(),
        curr.decay(),
        curr.seed(),
    );
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
