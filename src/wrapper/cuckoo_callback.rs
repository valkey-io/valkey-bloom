use crate::configs;
use crate::cuckoo;
use crate::cuckoo::data_type::ValkeyDataType;
use crate::cuckoo::utils::CuckooObject;
use crate::metrics;
use std::ffi::CString;
use std::mem;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::null_mut;
use std::sync::atomic::Ordering;
use valkey_module::defrag::Defrag;
use valkey_module::digest::Digest;
use valkey_module::logging;
use valkey_module::logging::{log_io_error, ValkeyLogLevel};
use valkey_module::raw;
use valkey_module::{RedisModuleDefragCtx, RedisModuleString};

// Note: methods in this mod are for the cuckoo module data type callbacks.
// The reason they are unsafe is because the callback methods are expected to be
// "unsafe extern C" based on the Rust module API definition

/// # Safety
pub unsafe extern "C" fn cuckoo_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let v = &*value.cast::<CuckooObject>();
    cuckoo::data_type::rdb_save_cuckoo_object(rdb, v);
}

/// # Safety
pub unsafe extern "C" fn cuckoo_rdb_load(
    rdb: *mut raw::RedisModuleIO,
    encver: c_int,
) -> *mut c_void {
    if let Some(item) = <CuckooObject as ValkeyDataType>::load_from_rdb(rdb, encver) {
        let bb = Box::new(item);
        Box::into_raw(bb).cast::<libc::c_void>()
    } else {
        logging::log_warning("Failed to restore cuckoo object.");
        null_mut()
    }
}

/// # Safety
pub unsafe extern "C" fn cuckoo_aof_rewrite(
    aof: *mut raw::RedisModuleIO,
    key: *mut raw::RedisModuleString,
    value: *mut c_void,
) {
    let cuckoo_obj = &*value.cast::<CuckooObject>();
    let hex = match cuckoo_obj.encode_object() {
        Ok(val) => val,
        Err(err) => {
            log_io_error(aof, ValkeyLogLevel::Warning, err.as_str());
            return;
        }
    };
    let cmd = CString::new("CF.LOAD").unwrap();
    let fmt = CString::new("sb").unwrap();
    valkey_module::raw::RedisModule_EmitAOF.unwrap()(
        aof,
        cmd.as_ptr(),
        fmt.as_ptr(),
        key,
        hex.as_ptr().cast::<c_char>(),
        hex.len(),
    );
}

/// # Safety
/// Load auxiliary data from RDB
pub unsafe extern "C" fn cuckoo_aux_load(
    rdb: *mut raw::RedisModuleIO,
    _encver: c_int,
    _when: c_int,
) -> c_int {
    cuckoo::data_type::cuckoo_rdb_aux_load(rdb)
}

/// # Safety
/// Free a cuckoo object
pub unsafe extern "C" fn cuckoo_free(value: *mut c_void) {
    drop(Box::from_raw(value.cast::<CuckooObject>()));
}

/// # Safety
/// Compute the memory usage for a cuckoo object.
pub unsafe extern "C" fn cuckoo_mem_usage(value: *const c_void) -> usize {
    let item = &*value.cast::<CuckooObject>();
    item.memory_usage()
}

/// # Safety
/// Raw handler for the COPY command.
pub unsafe extern "C" fn cuckoo_copy(
    _from_key: *mut RedisModuleString,
    _to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let curr_item = &*value.cast::<CuckooObject>();
    let new_item = CuckooObject::create_copy_from(curr_item);
    let bb = Box::new(new_item);
    Box::into_raw(bb).cast::<libc::c_void>()
}

/// # Safety
/// Raw handler for the Cuckoo digest callback.
pub unsafe extern "C" fn cuckoo_digest(md: *mut raw::RedisModuleDigest, value: *mut c_void) {
    let dig = Digest::new(md);
    let val = &*(value.cast::<CuckooObject>());
    val.debug_digest(dig);
}

/// # Safety
/// Raw handler for the Cuckoo object's free_effort callback.
pub unsafe extern "C" fn cuckoo_free_effort(
    _from_key: *mut RedisModuleString,
    value: *const c_void,
) -> usize {
    let curr_item = &*value.cast::<CuckooObject>();
    curr_item.free_effort()
}

/// # Safety
/// Raw handler for the Cuckoo object's defrag callback.
pub unsafe extern "C" fn cuckoo_defrag(
    defrag_ctx: *mut RedisModuleDefragCtx,
    _from_key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> i32 {
    // If defrag is disabled we will just exit straight away
    if !configs::CUCKOO_DEFRAG.load(Ordering::Relaxed) {
        return 0;
    }

    // Get the cursor for the CuckooObject otherwise start the cursor at 0
    let defrag = Defrag::new(defrag_ctx);
    let mut cursor = defrag.get_cursor().unwrap_or(0);

    // Convert pointer to CuckooObject so we can operate on it.
    let cuckoo_object: &mut CuckooObject = &mut *(*value).cast::<CuckooObject>();

    let num_filters = cuckoo_object.num_filters() as u64;

    // While we are within a timeframe decided from should_stop_defrag and not over the number of filters defrag the next filter
    while !defrag.should_stop_defrag() && cursor < num_filters {
        // Get mutable access to filters and remove the current filter
        let filters = cuckoo_object.filters_mut();
        let cuckoo_filter_box = filters.remove(cursor as usize);
        let cuckoo_filter = Box::into_raw(cuckoo_filter_box);
        let defrag_result = defrag.alloc(cuckoo_filter as *mut c_void);

        let _defragged_filter = {
            if !defrag_result.is_null() {
                metrics::CUCKOO_DEFRAG_HITS.fetch_add(1, Ordering::Relaxed);
                Box::from_raw(defrag_result as *mut crate::cuckoo::utils::CuckooFilter)
            } else {
                metrics::CUCKOO_DEFRAG_MISSES.fetch_add(1, Ordering::Relaxed);
                Box::from_raw(cuckoo_filter)
            }
        };

        // Reinsert the defragmented filter and increment the cursor
        cuckoo_object
            .filters_mut()
            .insert(cursor as usize, _defragged_filter);
        cursor += 1;
    }

    // Save the cursor for where we will start defragmenting from next time
    defrag.set_cursor(cursor);

    // If not all filters were looked at, return 1 to indicate incomplete defragmentation
    if cursor < num_filters {
        return 1;
    }

    // Defragment the Vec of CuckooFilter/s itself.
    // into_boxed_slice() shrinks capacity to len, so we use filters_len for both len and capacity.
    let filters_vec = mem::take(cuckoo_object.filters_mut());
    let filters_len = filters_vec.len();
    let filters_ptr = Box::into_raw(filters_vec.into_boxed_slice()) as *mut c_void;
    let defragged_filters_ptr = defrag.alloc(filters_ptr);

    if !defragged_filters_ptr.is_null() {
        metrics::CUCKOO_DEFRAG_HITS.fetch_add(1, Ordering::Relaxed);
        *cuckoo_object.filters_mut() = unsafe {
            Vec::from_raw_parts(
                defragged_filters_ptr as *mut Box<crate::cuckoo::utils::CuckooFilter>,
                filters_len,
                filters_len,
            )
        };
    } else {
        metrics::CUCKOO_DEFRAG_MISSES.fetch_add(1, Ordering::Relaxed);
        *cuckoo_object.filters_mut() = unsafe {
            Vec::from_raw_parts(
                filters_ptr as *mut Box<crate::cuckoo::utils::CuckooFilter>,
                filters_len,
                filters_len,
            )
        };
    }

    // Finally, attempt to defragment the CuckooObject itself
    let val = defrag.alloc(*value);
    if !val.is_null() {
        metrics::CUCKOO_DEFRAG_HITS.fetch_add(1, Ordering::Relaxed);
        *value = val;
    } else {
        metrics::CUCKOO_DEFRAG_MISSES.fetch_add(1, Ordering::Relaxed);
    }

    // Return 0 to indicate successful complete defragmentation
    0
}
