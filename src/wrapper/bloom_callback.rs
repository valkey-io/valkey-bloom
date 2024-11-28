use crate::bloom;
use crate::bloom::data_type::ValkeyDataType;
use crate::bloom::utils::BloomFilter;
use crate::bloom::utils::BloomFilterType;
use crate::configs;
use crate::wrapper::digest::Digest;
use crate::configs;
use bit_vec::BitVec;
use bloomfilter::Bloom;
use lazy_static::lazy_static;
use std::ffi::CString;
use std::mem;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::ptr::null_mut;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use valkey_module::logging;
use valkey_module::logging::{log_io_error, ValkeyLogLevel};
use valkey_module::raw;
use valkey_module::{RedisModuleDefragCtx, RedisModuleString};

// Note: methods in this mod are for the bloom module data type callbacks.
// The reason they are unsafe is because the callback methods are expected to be
// "unsafe extern C" based on the Rust module API definition

/// # Safety
pub unsafe extern "C" fn bloom_rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let v = &*value.cast::<BloomFilterType>();
    raw::save_unsigned(rdb, v.filters.len() as u64);
    raw::save_unsigned(rdb, v.expansion as u64);
    raw::save_double(rdb, v.fp_rate);
    let mut is_seed_random = 0;
    if v.is_seed_random {
        is_seed_random = 1;
    }
    raw::save_unsigned(rdb, is_seed_random);
    let filter_list = &v.filters;
    let mut filter_list_iter = filter_list.iter().peekable();
    while let Some(filter) = filter_list_iter.next() {
        let bloom = &filter.bloom;
        let bitmap = bloom.to_bytes();
        raw::RedisModule_SaveStringBuffer.unwrap()(
            rdb,
            bitmap.as_ptr().cast::<c_char>(),
            bitmap.len(),
        );
        raw::save_unsigned(rdb, filter.capacity as u64);
        if filter_list_iter.peek().is_none() {
            raw::save_unsigned(rdb, filter.num_items as u64);
        }
    }
}

/// # Safety
pub unsafe extern "C" fn bloom_rdb_load(
    rdb: *mut raw::RedisModuleIO,
    encver: c_int,
) -> *mut c_void {
    if let Some(item) = <BloomFilterType as ValkeyDataType>::load_from_rdb(rdb, encver) {
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
    let filter = &*value.cast::<BloomFilterType>();
    let hex = match filter.encode_bloom_filter() {
        Ok(val) => val,
        Err(err) => {
            log_io_error(
                aof,
                ValkeyLogLevel::Warning,
                &format!("encode bloom filter failed. {}", err.as_str()),
            );
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
    drop(Box::from_raw(value.cast::<BloomFilterType>()));
}

/// # Safety
/// Compute the memory usage for a bloom object.
pub unsafe extern "C" fn bloom_mem_usage(value: *const c_void) -> usize {
    let item = &*value.cast::<BloomFilterType>();
    item.memory_usage()
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
/// Raw handler for the Bloom digest callback.
pub unsafe extern "C" fn bloom_digest(md: *mut raw::RedisModuleDigest, value: *mut c_void) {
    let dig = Digest::new(md);
    let val = &*(value.cast::<BloomFilterType>());
    val.debug_digest(dig);
}

/// # Safety
/// Raw handler for the Bloom object's free_effort callback.
pub unsafe extern "C" fn bloom_free_effort(
    _from_key: *mut RedisModuleString,
    value: *const c_void,
) -> usize {
    let curr_item = &*value.cast::<BloomFilterType>();
    curr_item.free_effort()
}

// /// # Safety
// /// Raw handler for the Bloom object's defrag callback.
// pub unsafe extern "C" fn bloom_defrag(
//     _defrag_ctx: *mut RedisModuleDefragCtx,
//     _from_key: *mut RedisModuleString,
//     value: *mut *mut c_void,
// ) -> i32 {
//     if !configs::BLOOM_DEFRAG.load(Ordering::Relaxed) {
//         return 0;
//     }
//     let bloom_filter_type: &mut BloomFilterType = &mut *(*value).cast::<BloomFilterType>();

//     let num_filts = bloom_filter_type.filters.len();

//     for _ in 0..num_filts {
//         let bloom_filter_box = bloom_filter_type.filters.remove(0);
//         let bloom_filter = Box::into_raw(bloom_filter_box);
//         logging::log_warning(format!("Before Address: {:p}", bloom_filter));
//         let defrag_result = unsafe {
//             raw::RedisModule_DefragAlloc.unwrap()(
//                 core::ptr::null_mut(),
//                 bloom_filter as *mut c_void,
//             )
//         };
//         let mut defragged_filter = {
//             if !defrag_result.is_null() {
//                 Box::from_raw(defrag_result as *mut BloomFilter)
//             } else {
//                 Box::from_raw(bloom_filter)
//             }
//         };
//         logging::log_warning(format!("After Address: {:p}", defragged_filter));
//         // let test = Box::leak(defragged_filter.bloom);
//         // let tes = Box::into_raw(test);
//         // let inner_bloom = mem::replace(
//         //     &mut defragged_filter.bloom,
//         //     Box::new(bloomfilter::Bloom::new(1, 1)),
//         // );
//         // let inner_bloom = mem::replace(
//         //     &mut defragged_filter.bloom,
//         //     Box::from_raw(ptr::null::<bloomfilter::Bloom<[u8]>>() as *mut bloomfilter::Bloom<[u8]>),
//         // );
//         let inner_bloom = mem::take(&mut defragged_filter.bloom);
//         let inner_bloom_ptr = Box::into_raw(inner_bloom);
//         logging::log_warning(format!("Before bloom Address: {:p}", inner_bloom_ptr));
//         let defragged_inner_bloom = raw::RedisModule_DefragAlloc.unwrap()(
//             core::ptr::null_mut(),
//             inner_bloom_ptr as *mut c_void,
//         );
//         defragged_filter.bloom = {
//             if !defrag_result.is_null() {
//                 Box::from_raw(defragged_inner_bloom as *mut bloomfilter::Bloom<[u8]>)
//             } else {
//                 Box::from_raw(inner_bloom_ptr)
//             }
//         };
//         logging::log_warning(format!("After bloom Address: {:p}", defragged_filter.bloom));
//         bloom_filter_type.filters.push(defragged_filter);
//     }
//     let val = unsafe { raw::RedisModule_DefragAlloc.unwrap()(core::ptr::null_mut(), *value) };
//     if !val.is_null() {
//         *value = val;
//     }
//     0
// }

lazy_static! {
    static ref DEFRAG_BLOOM_FILTER: Mutex<Option<Box<Bloom<[u8]>>>> =
        Mutex::new(Some(Box::new(Bloom::<[u8]>::new(1, 1))));
    static ref DEFRAG_VEC: Mutex<Option<Vec<u32>>> = Mutex::new(Some(Vec::new()));
}

fn external_vec_defrag(mut vec: Vec<u32>) -> Vec<u32> {
    let clonev = vec.clone();
    let len = vec.len();
    let capacity = vec.capacity();
    // let ptr: *mut u32 = vec.as_mut_ptr();
    let vec_ptr = Box::into_raw(vec.into_boxed_slice()) as *mut c_void;
    logging::log_warning(format!("Before vec_ptr start Address: {:p}", vec_ptr));

    let defragged_filters_ptr =
        unsafe { raw::RedisModule_DefragAlloc.unwrap()(core::ptr::null_mut(), vec_ptr) };
    logging::log_warning(format!(
        "After hmmm vec Address: {:p}",
        defragged_filters_ptr
    ));
    if !defragged_filters_ptr.is_null() {
        unsafe { Vec::from_raw_parts(defragged_filters_ptr as *mut u32, len, capacity) }
    } else {
        unsafe { Vec::from_raw_parts(vec_ptr as *mut u32, len, capacity) }
    }
    // unsafe { Vec::from_raw_parts(defragged_filters_ptr as *mut u32, len, capacity) }
}

fn external_bitvec_defrag(bit_vec: BitVec) -> BitVec {
    // let ptr: *mut BitVec = Box::into_raw(Box::new(bit_vec));
    // logging::log_warning(format!("Before bloom bit_vec Address: {:p}", ptr));
    // let defrag_result =
    //     unsafe { raw::RedisModule_DefragAlloc.unwrap()(core::ptr::null_mut(), ptr as *mut c_void) };
    // let mut defragged_filter = unsafe { Box::from_raw(defrag_result as *mut BitVec) };
    // logging::log_warning(format!("After bloom bit_vec Address: {:p}", defragged_filter));
    // *defragged_filter
    bit_vec
}

/// # Safety
/// Raw handler for the Bloom object's defrag callback.
pub unsafe extern "C" fn bloom_defrag(
    _defrag_ctx: *mut RedisModuleDefragCtx,
    _from_key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> i32 {
    // logging::log_warning(format!("After here 0"));

    let bloom_filter_type: &mut BloomFilterType = &mut *(*value).cast::<BloomFilterType>();

    let num_filts = bloom_filter_type.filters.len();

    logging::log_warning(format!(
        "defrag in box Address: {:p}",
        bloom_filter_type.filters.as_ptr()
    ));

    for _ in 0..num_filts {
        let bloom_filter_box = bloom_filter_type.filters.remove(0);
        let bloom_filter = Box::into_raw(bloom_filter_box);

        let defrag_result = unsafe {
            raw::RedisModule_DefragAlloc.unwrap()(
                core::ptr::null_mut(),
                bloom_filter as *mut c_void,
            )
        };

        logging::log_warning(format!("Before Vec start Address: {:p}", defrag_result));

        let mut defragged_filter = {
            if !defrag_result.is_null() {
                Box::from_raw(defrag_result as *mut BloomFilter)
            } else {
                Box::from_raw(bloom_filter)
            }
        };
        let mut defrag_b = DEFRAG_BLOOM_FILTER.lock().unwrap();
        let inner_bloom = mem::replace(
            &mut defragged_filter.bloom,
            defrag_b.take().expect("We expect default to exist"),
        );
        let inner_bloom_ptr = Box::into_raw(inner_bloom);
        let defragged_inner_bloom = raw::RedisModule_DefragAlloc.unwrap()(
            core::ptr::null_mut(),
            inner_bloom_ptr as *mut c_void,
        );
        logging::log_warning(format!("defrag in box Address: {:p}", defragged_filter));
        if !defragged_inner_bloom.is_null() {
            let inner_bloom = mem::replace(
                &mut defragged_filter.bloom,
                Box::from_raw(defragged_inner_bloom as *mut bloomfilter::Bloom<[u8]>),
            );
            *defrag_b = Some(inner_bloom); // Resetting the original static
        } else {
            let inner_bloom =
                mem::replace(&mut defragged_filter.bloom, Box::from_raw(inner_bloom_ptr));
            *defrag_b = Some(inner_bloom); // Resetting the original static
        }
        // let inner_bloom = mem::replace(
        //     &mut defragged_filter.bloom,
        //     Box::from_raw(defragged_inner_bloom as *mut bloomfilter::Bloom<[u8]>),
        // );
        // *defrag_b = Some(inner_bloom); // Resetting the original static

        // logging::log_warning(format!("1bloom filter len: {}", bloom_filter_type.filters.len()));
        // let mut defrag_v = DEFRAG_VEC.lock().unwrap();
        // let placeholder = defrag_v.take().unwrap();
        // defragged_filter
        //     .bloom
        //     .defrag_no(external_bitvec_defrag, external_vec_defrag);
        // // *defrag_v = Some(newplaceholder); // Resetting the original static
        // logging::log_warning(format!("After bloom Address: {:p}", defragged_filter.bloom));

        // logging::log_warning(format!("2bloom filter len: {}", bloom_filter_type.filters.len()));
        defragged_filter
            .bloom
            .defrag_no(external_bitvec_defrag, external_vec_defrag);

        bloom_filter_type.filters.push(defragged_filter);
    }
    let filters_vec = mem::take(&mut bloom_filter_type.filters);
    let filters_ptr = Box::into_raw(filters_vec.into_boxed_slice()) as *mut c_void;
    // logging::log_warning(format!("Before Vec start Address: {:p}", filters_ptr));

    let defragged_filters_ptr =
        unsafe { raw::RedisModule_DefragAlloc.unwrap()(core::ptr::null_mut(), filters_ptr) };
    logging::log_warning(format!(
        "After Vec start Address: {:p} \n\n\n",
        defragged_filters_ptr
    ));
    if !defragged_filters_ptr.is_null() {
        bloom_filter_type.filters = unsafe {
            Vec::from_raw_parts(
                defragged_filters_ptr as *mut Box<BloomFilter>,
                num_filts,
                num_filts,
            )
        };
    } else {
        bloom_filter_type.filters = unsafe {
            Vec::from_raw_parts(filters_ptr as *mut Box<BloomFilter>, num_filts, num_filts)
        };
    }
    // logging::log_warning(format!("After here last"));

    let val = unsafe { raw::RedisModule_DefragAlloc.unwrap()(core::ptr::null_mut(), *value) };
    if !val.is_null() {
        *value = val;
    }
    logging::log_warning("After here super last");

    0
}
