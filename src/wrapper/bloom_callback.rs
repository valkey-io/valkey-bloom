use crate::commands::bloom_data_type;
use redis_module::raw;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::null_mut;

// Note: methods in this mod are for the bloom module data type callbacks.
// The reason they are unsafe is because the callback methods are expected to be
// "unsafe extern C" based on the Rust module API definition

/// # Safety
pub unsafe extern "C" fn bloom_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let v = &*value.cast::<bloom_data_type::BloomFilterType>();
    raw::RedisModule_SaveStringBuffer.unwrap()(
        rdb,
        v.bitmap.as_ptr().cast::<c_char>(),
        v.bitmap.len(),
    );
    raw::save_unsigned(rdb, v.number_of_bits);
    raw::save_unsigned(rdb, v.number_of_hash_functions as u64);
    raw::save_unsigned(rdb, v.sip_key_one_a);
    raw::save_unsigned(rdb, v.sip_key_one_b);
    raw::save_unsigned(rdb, v.sip_key_two_a);
    raw::save_unsigned(rdb, v.sip_key_two_b);
    raw::save_unsigned(rdb, v.num_items);
}

/// # Safety
pub unsafe extern "C" fn bloom_rdb_load(
    rdb: *mut raw::RedisModuleIO,
    encver: c_int,
) -> *mut c_void {
    if let Some(item) = bloom_data_type::bloom_rdb_load_data_object(rdb, encver) {
        let bb = Box::new(item);
        // report data usage for metering
        let data = Box::into_raw(bb).cast::<libc::c_void>();
        data
    } else {
        null_mut()
    }
}

/// # Safety
/// Save auxiliary data to RDB
pub unsafe extern "C" fn bloom_aux_save(rdb: *mut raw::RedisModuleIO, _when: c_int) {
    bloom_data_type::bloom_rdb_aux_save(rdb)
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
    // Decrement the data usage for metering
    drop(Box::from_raw(
        value.cast::<bloom_data_type::BloomFilterType>(),
    ));
}

/// # Safety
/// Compute the memory usage for a bloom string item
pub unsafe extern "C" fn bloom_mem_usage(value: *const c_void) -> usize {
    let item = &*value.cast::<bloom_data_type::BloomFilterType>();
    bloom_data_type::bloom_get_filter_memory_usage(item.bitmap.len())
}
