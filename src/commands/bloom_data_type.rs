use crate::wrapper::bloom_callback;
use crate::MODULE_NAME;
use redis_module::native_types::RedisType;
use redis_module::{logging, raw};
use std::os::raw::c_int;
use bloomfilter::Bloom;


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
        copy: None, // Redis COPY command is not supported
        defrag: None,

        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

pub static BLOOM_FILTER_TYPE2: RedisType = RedisType::new(
    "bloomtype2",
    BLOOM_FILTER_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: None,
        rdb_save: None,
        aof_rewrite: None,

        mem_usage: None,
        digest: None,
        free: None,

        aux_load: None,
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: raw::Aux::Before as i32,

        free_effort: None,
        unlink: None,
        copy: None, // Redis COPY command is not supported
        defrag: None,

        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

/// The BloomFilterType structure is currently 40 bytes.
// #[derive(Debug)]
pub struct BloomFilterType {
    pub bitmap: Vec<u8>,
    pub number_of_bits: u64,
    pub number_of_hash_functions: u32,
    pub sip_key_one_a: u64,
    pub sip_key_one_b: u64,
    pub sip_key_two_a: u64,
    pub sip_key_two_b: u64,
    pub num_items: u64,
}

pub struct BloomFilterType2 {
    pub bloom: Bloom<[u8]>,
    pub num_items: u64,
}

pub fn bloom_rdb_load_data_object(
    rdb: *mut raw::RedisModuleIO,
    encver: i32,
) -> Option<BloomFilterType2> {
    if encver > BLOOM_FILTER_TYPE_ENCODING_VERSION {
        logging::log_warning(format!("{}: Cannot load bloomfilter type version {} because it is higher than the current module's string type version {}", MODULE_NAME, encver, BLOOM_FILTER_TYPE_ENCODING_VERSION).as_str());
        return None;
    }
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

    // let item = BloomFilterType {
    //     bitmap: bitmap.as_ref().to_vec(),
    //     number_of_bits,
    //     number_of_hash_functions: number_of_hash_functions as u32,
    //     sip_key_one_a,
    //     sip_key_one_b,
    //     sip_key_two_a,
    //     sip_key_two_b,
    //     num_items,
    // };

    let sip_keys = [
        (sip_key_one_a, sip_key_one_b),
        (sip_key_two_a, sip_key_two_b),
    ];
    let bloom = Bloom::from_existing(
        bitmap.as_ref(),
        number_of_bits,
        number_of_hash_functions as u32,
        sip_keys,
    );
    let item = BloomFilterType2 {
        bloom,
        num_items,
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

pub fn bloom_get_filter_memory_usage(data_len: usize) -> usize {
    std::mem::size_of::<BloomFilterType>() + data_len
}
