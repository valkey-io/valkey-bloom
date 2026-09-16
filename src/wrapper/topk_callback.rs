use crate::configs;
use crate::metrics;
use crate::topk::data_type::ValkeyDataType;
use crate::topk::utils::TopKObject;
use heavykeeper::Reallocator;
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::null_mut;
use std::sync::atomic::Ordering;
use valkey_module::defrag::Defrag;
use valkey_module::digest::Digest;
use valkey_module::logging;
use valkey_module::raw;
use valkey_module::{RedisModuleDefragCtx, RedisModuleString};

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
pub unsafe extern "C" fn topk_aof_rewrite(
    aof: *mut raw::RedisModuleIO,
    key: *mut RedisModuleString,
    value: *mut c_void,
) {
    let v = &*value.cast::<TopKObject>();
    let blob = v.encode_object();
    let cmd = CString::new("TOPK.LOAD").unwrap();
    let fmt = CString::new("sb").unwrap();
    raw::RedisModule_EmitAOF.unwrap()(
        aof,
        cmd.as_ptr(),
        fmt.as_ptr(),
        key,
        blob.as_ptr().cast::<c_char>(),
        blob.len(),
    );
}

/// # Safety
/// Drop the TopKObject when its key is deleted or replaced.
pub unsafe extern "C" fn topk_free(value: *mut c_void) {
    drop(Box::from_raw(value.cast::<TopKObject>()));
}

/// # Safety
/// Approximate memory usage for the MEMORY USAGE command. Reports the wrapper
/// struct plus the sketch's heap allocations (lobby/heavy cell arrays,
/// priority-queue containers, and the buffers of tracked items). See
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

/// Reallocator passed to the heavykeeper sketch during defrag. Routes each
/// block through Valkey's defrag allocator; a null result means "not
/// relocated", so we keep the original allocation. Assumes `align_of::<T>()
/// <= 16`, which the defrag allocator guarantees and every sketch type meets.
struct Defragger;

impl Reallocator for Defragger {
    fn realloc<T>(&mut self, boxed: Box<[T]>) -> Box<[T]> {
        let len = boxed.len();
        if len == 0 || core::mem::size_of::<T>() == 0 {
            return boxed;
        }
        let old = Box::into_raw(boxed) as *mut c_void;
        let new = unsafe { Defrag::new(core::ptr::null_mut()).alloc(old) };
        if new.is_null() {
            metrics::TOPK_DEFRAG_MISSES.fetch_add(1, Ordering::Relaxed);
            unsafe { Box::from_raw(core::ptr::slice_from_raw_parts_mut(old as *mut T, len)) }
        } else {
            metrics::TOPK_DEFRAG_HITS.fetch_add(1, Ordering::Relaxed);
            unsafe { Box::from_raw(core::ptr::slice_from_raw_parts_mut(new as *mut T, len)) }
        }
    }
}

/// # Safety
/// Raw handler for the TopK object's defrag callback. A TopKObject holds one
/// sketch inline, so we defrag its heap allocations and then the object itself
/// in a single pass, always returning 0 (complete).
pub unsafe extern "C" fn topk_defrag(
    defrag_ctx: *mut RedisModuleDefragCtx,
    _from_key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> i32 {
    if !configs::TOPK_DEFRAG.load(Ordering::Relaxed) {
        return 0;
    }
    // Defrag the sketch's internal heap allocations.
    // The internal vecs shrink after defrag but are reallocated to their original
    // capacity (k) on the next add. Since memory usage is calculated from capacity,
    // this will cause a temporary overcount until the next add restores the capacity.
    let topk_object: &mut TopKObject = &mut *(*value).cast::<TopKObject>();
    topk_object
        .sketch_mut()
        .realloc_large_heap_allocated_objects(&mut Defragger);
    // Attempt to defrag the TopKObject allocation itself.
    let defrag = Defrag::new(defrag_ctx);
    let val = defrag.alloc(*value);
    if !val.is_null() {
        metrics::TOPK_DEFRAG_HITS.fetch_add(1, Ordering::Relaxed);
        *value = val;
    } else {
        metrics::TOPK_DEFRAG_MISSES.fetch_add(1, Ordering::Relaxed);
    }
    0
}
