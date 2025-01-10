use crate::bloom;
use crate::bloom::data_type::ValkeyDataType;
use crate::bloom::utils::BloomFilter;
use crate::bloom::utils::BloomObject;
use crate::configs;
use crate::metrics;
use crate::wrapper::digest::Digest;
use bloomfilter::Bloom;
use lazy_static::lazy_static;
use std::ffi::CString;
use std::mem;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::null_mut;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use valkey_module::logging;
use valkey_module::logging::{log_io_error, ValkeyLogLevel};
use valkey_module::raw;
use valkey_module::{RedisModuleDefragCtx, RedisModuleString};

use super::defrag::Defrag;

// Note: methods in this mod are for the bloom module data type callbacks.
// The reason they are unsafe is because the callback methods are expected to be
// "unsafe extern C" based on the Rust module API definition

/// # Safety
pub unsafe extern "C" fn bloom_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let v = &*value.cast::<BloomObject>();
    raw::save_unsigned(rdb, v.num_filters() as u64);
    raw::save_unsigned(rdb, v.expansion() as u64);
    raw::save_double(rdb, v.fp_rate());
    raw::save_double(rdb, v.tightening_ratio());
    let is_seed_random = if v.is_seed_random() { 1 } else { 0 };
    raw::save_unsigned(rdb, is_seed_random);
    let filter_list = v.filters();
    let mut filter_list_iter = filter_list.iter().peekable();
    while let Some(filter) = filter_list_iter.next() {
        let bloom = filter.raw_bloom();
        let bitmap = bloom.as_slice();
        raw::RedisModule_SaveStringBuffer.unwrap()(
            rdb,
            bitmap.as_ptr().cast::<c_char>(),
            bitmap.len(),
        );
        raw::save_unsigned(rdb, filter.capacity() as u64);
        if filter_list_iter.peek().is_none() {
            raw::save_unsigned(rdb, filter.num_items() as u64);
        }
    }
}

/// # Safety
pub unsafe extern "C" fn bloom_rdb_load(
    rdb: *mut raw::RedisModuleIO,
    encver: c_int,
) -> *mut c_void {
    if let Some(item) = <BloomObject as ValkeyDataType>::load_from_rdb(rdb, encver) {
        let bb = Box::new(item);
        Box::into_raw(bb).cast::<libc::c_void>()
    } else {
        logging::log_warning("Failed to restore bloom object.");
        null_mut()
    }
}

/// # Safety
pub unsafe extern "C" fn bloom_aof_rewrite(
    aof: *mut raw::RedisModuleIO,
    key: *mut raw::RedisModuleString,
    value: *mut c_void,
) {
    let filter = &*value.cast::<BloomObject>();
    let hex = match filter.encode_object() {
        Ok(val) => val,
        Err(err) => {
            log_io_error(aof, ValkeyLogLevel::Warning, err.as_str());
            return;
        }
    };
    let cmd = CString::new("BF.LOAD").unwrap();
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
pub unsafe extern "C" fn bloom_aux_load(
    rdb: *mut raw::RedisModuleIO,
    _encver: c_int,
    _when: c_int,
) -> c_int {
    bloom::data_type::bloom_rdb_aux_load(rdb)
}

/// # Safety
/// Free a bloom object
pub unsafe extern "C" fn bloom_free(value: *mut c_void) {
    drop(Box::from_raw(value.cast::<BloomObject>()));
}

/// # Safety
/// Compute the memory usage for a bloom object.
pub unsafe extern "C" fn bloom_mem_usage(value: *const c_void) -> usize {
    let item = &*value.cast::<BloomObject>();
    item.memory_usage()
}

/// # Safety
/// Raw handler for the COPY command.
pub unsafe extern "C" fn bloom_copy(
    _from_key: *mut RedisModuleString,
    _to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let curr_item = &*value.cast::<BloomObject>();
    let new_item = BloomObject::create_copy_from(curr_item);
    let bb = Box::new(new_item);
    Box::into_raw(bb).cast::<libc::c_void>()
}

/// # Safety
/// Raw handler for the Bloom digest callback.
pub unsafe extern "C" fn bloom_digest(md: *mut raw::RedisModuleDigest, value: *mut c_void) {
    let dig = Digest::new(md);
    let val = &*(value.cast::<BloomObject>());
    val.debug_digest(dig);
}

/// # Safety
/// Raw handler for the Bloom object's free_effort callback.
pub unsafe extern "C" fn bloom_free_effort(
    _from_key: *mut RedisModuleString,
    value: *const c_void,
) -> usize {
    let curr_item = &*value.cast::<BloomObject>();
    curr_item.free_effort()
}

// Lazy static for a default temporary external crate Bloom structure that gets swapped during defrag.
lazy_static! {
    static ref DEFRAG_BLOOM_FILTER: Mutex<Option<Box<Bloom<[u8]>>>> =
        Mutex::new(Some(Box::new(Bloom::<[u8]>::new(1, 1).unwrap())));
}

/// Defragments a vector of bytes (bit vector) of the external crate Bloom structure. This function is designed to be
/// used as a callback.
///
/// This function takes ownership of a `Vec<u8>`, attempts to defragment it using an external
/// defragmentation mechanism, and returns a new `Vec<u8>` that may have been defragmented.
///
/// # Arguments
///
/// * `vec` - A `Vec<u8>` to be defragmented.
///
/// # Returns
///
/// Returns a new `Vec<u8>` that may have been defragmented. If defragmentation was successful,
/// the returned vector will use the newly allocated memory. If defragmentation failed or was
/// not necessary, the original vector's memory will be used.
fn external_vec_defrag(vec: Vec<u8>) -> Vec<u8> {
    let defrag = Defrag::new(core::ptr::null_mut());
    let len = vec.len();
    let capacity = vec.capacity();
    let vec_ptr = Box::into_raw(vec.into_boxed_slice()) as *mut c_void;
    let defragged_filters_ptr = unsafe { defrag.alloc(vec_ptr) };
    if !defragged_filters_ptr.is_null() {
        metrics::BLOOM_DEFRAG_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        unsafe { Vec::from_raw_parts(defragged_filters_ptr as *mut u8, len, capacity) }
    } else {
        metrics::BLOOM_DEFRAG_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        unsafe { Vec::from_raw_parts(vec_ptr as *mut u8, len, capacity) }
    }
}

/// # Safety
/// Raw handler for the Bloom object's defrag callback.
///
/// We will be defragging every allocation of the Bloom data type. We will explain them top down, then afterwards state the order in which
/// we will defrag. Starting from the top, which is passed in as the variable named `value`, we have the BloomObject. This BloomObject
/// contains a vec of BloomFilter structs. Each BloomFilter contains a Bloom structure implemented in an external Rust crate.
/// Finally, each of these external Bloom structures contains a Vec (bit vector).
///
/// The order of defragmention is as follows (1 to 3 is in a loop for the number of filters):
/// 1. BloomFilter structures within the top level BloomObject structure
/// 2. External Bloom structures within each BloomFilter
/// 3. Vec (Bit vector) within each external Bloom structure
/// 4. Vec of the BloomFilter/s in the BloomObject
/// 5. The BloomObject itself
///
/// We use a cursor to track the current filter of BloomObject that we are defragging. This cursor will start at 0
/// if we finished all the filters the last time we defragged this object or if we havent defragged it before. We will determine
/// that we have spent too much time on defragging this specific object from the should_stop_defrag() method. If we didn't defrag
/// all the filters, then we set the cursor so we know where to start from the next time we defrag and return 1 to show we didn't
/// finish.
///
/// # Arguments
///
/// * `defrag_ctx` - A raw pointer to the defragmentation context.
/// * `_from_key` - A raw pointer to the Redis module string (unused in this function).
/// * `value` - A mutable raw pointer to a raw pointer representing the BloomObject to be defragmented.
///
/// # Returns
///
/// Returns an `i32` where:
/// * 0 indicates successful complete defragmentation.
/// * 1 indicates incomplete defragmentation (not all filters were defragged).
pub unsafe extern "C" fn bloom_defrag(
    defrag_ctx: *mut RedisModuleDefragCtx,
    _from_key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> i32 {
    // If defrag is disabled we will just exit straight away
    if !configs::BLOOM_DEFRAG.load(Ordering::Relaxed) {
        return 0;
    }

    // Get the cursor for the BloomObject otherwise start the cursor at 0
    let defrag = Defrag::new(defrag_ctx);
    let mut cursor = defrag.get_cursor().unwrap_or(0);

    // Convert pointer to BloomObject so we can operate on it.
    let bloom_object: &mut BloomObject = &mut *(*value).cast::<BloomObject>();

    let num_filters = bloom_object.num_filters();
    let filters_capacity = bloom_object.filters().capacity();

    // While we are within a timeframe decided from should_stop_defrag and not over the number of filters defrag the next filter
    while !defrag.should_stop_defrag() && cursor < num_filters as u64 {
        // Remove the current BloomFilter, unbox it, and attempt to defragment the BloomFilter.
        let bloom_filter_box = bloom_object.filters_mut().remove(cursor as usize);
        let bloom_filter = Box::into_raw(bloom_filter_box);
        let defrag_result = defrag.alloc(bloom_filter as *mut c_void);
        let mut defragged_filter = {
            if !defrag_result.is_null() {
                metrics::BLOOM_DEFRAG_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Box::from_raw(defrag_result as *mut BloomFilter)
            } else {
                metrics::BLOOM_DEFRAG_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Box::from_raw(bloom_filter)
            }
        };
        // Swap the external crate Bloom structure with a temporary one during its defragmentation.
        let mut temporary_bloom = DEFRAG_BLOOM_FILTER
            .lock()
            .expect("We expect default to exist");
        let inner_bloom = mem::replace(
            defragged_filter.raw_bloom_mut(),
            temporary_bloom.take().expect("We expect default to exist"),
        );
        // Convert the inner_bloom into the correct type and then try to defragment it
        let inner_bloom_ptr = Box::into_raw(inner_bloom);
        let defragged_inner_bloom = defrag.alloc(inner_bloom_ptr as *mut c_void);
        // Defragment the Bit Vec within the external crate Bloom structure using the external callback
        if !defragged_inner_bloom.is_null() {
            metrics::BLOOM_DEFRAG_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let inner_bloom =
                unsafe { Box::from_raw(defragged_inner_bloom as *mut bloomfilter::Bloom<[u8]>) };
            let external_bloom =
                inner_bloom.realloc_large_heap_allocated_objects(external_vec_defrag);
            let placeholder_bloom =
                mem::replace(defragged_filter.raw_bloom_mut(), Box::new(external_bloom));
            *temporary_bloom = Some(placeholder_bloom); // Reset the original static
        } else {
            metrics::BLOOM_DEFRAG_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let inner_bloom = unsafe { Box::from_raw(inner_bloom_ptr) };
            let external_bloom =
                inner_bloom.realloc_large_heap_allocated_objects(external_vec_defrag);
            let placeholder_bloom =
                mem::replace(defragged_filter.raw_bloom_mut(), Box::new(external_bloom));
            *temporary_bloom = Some(placeholder_bloom); // Reset the original static
        }

        // Reinsert the defragmented filter and increment the cursor
        bloom_object
            .filters_mut()
            .insert(cursor as usize, defragged_filter);
        cursor += 1;
    }
    // Save the cursor for where we will start defragmenting from next time
    defrag.set_cursor(cursor);
    // If not all filters were looked at, return 1 to indicate incomplete defragmentation
    if cursor < num_filters as u64 {
        return 1;
    }
    // Defragment the Vec of BloomFilter/s itself
    let filters_vec = mem::take(bloom_object.filters_mut());
    let filters_ptr = Box::into_raw(filters_vec.into_boxed_slice()) as *mut c_void;
    let defragged_filters_ptr = defrag.alloc(filters_ptr);
    if !defragged_filters_ptr.is_null() {
        metrics::BLOOM_DEFRAG_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *bloom_object.filters_mut() = unsafe {
            Vec::from_raw_parts(
                defragged_filters_ptr as *mut Box<BloomFilter>,
                num_filters,
                filters_capacity,
            )
        };
    } else {
        metrics::BLOOM_DEFRAG_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *bloom_object.filters_mut() = unsafe {
            Vec::from_raw_parts(
                filters_ptr as *mut Box<BloomFilter>,
                num_filters,
                filters_capacity,
            )
        };
    }
    // Finally, attempt to defragment the BloomObject itself
    let val = defrag.alloc(*value);
    if !val.is_null() {
        metrics::BLOOM_DEFRAG_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *value = val;
    } else {
        metrics::BLOOM_DEFRAG_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    // Return 0 to indicate successful complete defragmentation
    0
}
