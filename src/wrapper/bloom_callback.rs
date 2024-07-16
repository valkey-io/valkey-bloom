use crate::commands::bloom_data_type;
use crate::commands::bloom_util::BloomFilterType;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::null_mut;
use valkey_module::raw;
use valkey_module::RedisModuleString;

// Note: methods in this mod are for the bloom module data type callbacks.
// The reason they are unsafe is because the callback methods are expected to be
// "unsafe extern C" based on the Rust module API definition

/// # Safety
pub unsafe extern "C" fn bloom_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let v = &*value.cast::<BloomFilterType>();
    raw::save_unsigned(rdb, v.filters.len() as u64);
    raw::save_unsigned(rdb, v.expansion as u64);
    raw::save_float(rdb, v.fp_rate);
    let filter_list = &v.filters;
    for filter in filter_list {
        let bloom = &filter.bloom;
        let bitmap = bloom.bitmap();
        raw::RedisModule_SaveStringBuffer.unwrap()(
            rdb,
            bitmap.as_ptr().cast::<c_char>(),
            bitmap.len(),
        );
        raw::save_unsigned(rdb, bloom.number_of_bits());
        raw::save_unsigned(rdb, bloom.number_of_hash_functions() as u64);
        let sip_keys = bloom.sip_keys();
        raw::save_unsigned(rdb, sip_keys[0].0);
        raw::save_unsigned(rdb, sip_keys[0].1);
        raw::save_unsigned(rdb, sip_keys[1].0);
        raw::save_unsigned(rdb, sip_keys[1].1);
        raw::save_unsigned(rdb, filter.num_items as u64);
        raw::save_unsigned(rdb, filter.capacity as u64);
    }
}

/// # Safety
pub unsafe extern "C" fn bloom_rdb_load(
    rdb: *mut raw::RedisModuleIO,
    encver: c_int,
) -> *mut c_void {
    if let Some(item) = bloom_data_type::bloom_rdb_load_data_object(rdb, encver) {
        let bb = Box::new(item);
        Box::into_raw(bb).cast::<libc::c_void>()
    } else {
        null_mut()
    }
}

/// # Safety
/// Load auxiliary data from RDB
pub unsafe extern "C" fn bloom_aux_load(
    rdb: *mut raw::RedisModuleIO,
    _encver: c_int,
    _when: c_int,
) -> c_int {
    bloom_data_type::bloom_rdb_aux_load(rdb)
}

/// # Safety
/// Free a bloom item
pub unsafe extern "C" fn bloom_free(value: *mut c_void) {
    // TODO: Validate with ASAN.
    drop(Box::from_raw(value.cast::<BloomFilterType>()));
}

/// # Safety
/// Compute the memory usage for a bloom object.
pub unsafe extern "C" fn bloom_mem_usage(value: *const c_void) -> usize {
    let item = &*value.cast::<BloomFilterType>();
    item.get_memory_usage()
}

/// # Safety
/// Raw handler for the COPY command.
pub unsafe extern "C" fn bloom_copy(
    _from_key: *mut RedisModuleString,
    _to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let curr_item = &*value.cast::<BloomFilterType>();
    let new_item = BloomFilterType::create_copy_from(curr_item);
    let bb = Box::new(new_item);
    Box::into_raw(bb).cast::<libc::c_void>()
}

/// # Safety
/// Raw handler for the Bloom object's free_effort callback.
/// We return 1 if there are no filters (BF.RESERVE) or if there is 1 filter.
/// Else, we return the number of filters as the free_effort.
/// This is similar to how the core handles aggregated objects.
pub unsafe extern "C" fn bloom_free_effort(
    _from_key: *mut RedisModuleString,
    value: *const c_void,
) -> usize {
    let curr_item = &*value.cast::<BloomFilterType>();
    curr_item.free_effort()
}
