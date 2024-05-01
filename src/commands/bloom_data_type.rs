use crate::commands::bloom_util::BloomFilter;
use crate::commands::bloom_util::BloomFilterType;
use crate::wrapper::bloom_callback;
use crate::MODULE_NAME;
use redis_module::native_types::RedisType;
use redis_module::{logging, raw};
use std::os::raw::c_int;

const BLOOM_FILTER_TYPE_ENCODING_VERSION: i32 = 0;

pub static BLOOM_FILTER_TYPE: RedisType = RedisType::new(
    "bloomtype",
    BLOOM_FILTER_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(bloom_callback::bloom_rdb_load),
        rdb_save: Some(bloom_callback::bloom_rdb_save),
        aof_rewrite: None,

        mem_usage: Some(bloom_callback::bloom_mem_usage),
        digest: None,
        free: Some(bloom_callback::bloom_free),

        aux_load: Some(bloom_callback::bloom_aux_load),
        aux_save: Some(bloom_callback::bloom_aux_save),
        aux_save2: None,
        aux_save_triggers: raw::Aux::Before as i32,

        free_effort: None,
        unlink: None,
        copy: Some(bloom_callback::bloom_copy),
        defrag: None,

        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

pub fn bloom_rdb_load_data_object(
    rdb: *mut raw::RedisModuleIO,
    encver: i32,
) -> Option<BloomFilterType> {
    if encver > BLOOM_FILTER_TYPE_ENCODING_VERSION {
        logging::log_warning(format!("{}: Cannot load bloomfilter type version {} because it is higher than the current module's string type version {}", MODULE_NAME, encver, BLOOM_FILTER_TYPE_ENCODING_VERSION).as_str());
        return None;
    }
    let Ok(num_filters) = raw::load_unsigned(rdb) else {
        return None;
    };
    let Ok(expansion) = raw::load_signed(rdb) else {
        return None;
    };
    let Ok(fp_rate) = raw::load_float(rdb) else {
        return None;
    };
    let mut filters = Vec::new();
    for _ in 0..num_filters {
        let Ok(bitmap) = raw::load_string_buffer(rdb) else {
            return None;
        };
        let Ok(number_of_bits) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(number_of_hash_functions) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(sip_key_one_a) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(sip_key_one_b) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(sip_key_two_a) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(sip_key_two_b) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(num_items) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(capacity) = raw::load_unsigned(rdb) else {
            return None;
        };

        let sip_keys = [
            (sip_key_one_a, sip_key_one_b),
            (sip_key_two_a, sip_key_two_b),
        ];
        let filter = BloomFilter::from_existing(
            bitmap.as_ref(),
            number_of_bits,
            number_of_hash_functions as u32,
            sip_keys,
            num_items as u32,
            capacity as u32,
        );
        filters.push(filter);
    }
    let item = BloomFilterType {
        expansion: expansion as u32,
        fp_rate,
        filters,
    };
    Some(item)
}

// Save the auxiliary data outside of the regular keyspace to the RDB file
pub fn bloom_rdb_aux_save(_rdb: *mut raw::RedisModuleIO) {
    logging::log_notice("NOOP for now");
}

// Load the auxiliary data outside of the regular keyspace from the RDB file
pub fn bloom_rdb_aux_load(_rdb: *mut raw::RedisModuleIO) -> c_int {
    logging::log_notice("NOOP for now");
    raw::Status::Ok as i32
}
