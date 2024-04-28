use crate::wrapper::bloom_callback;
use crate::MODULE_NAME;
use redis_module::native_types::RedisType;
use redis_module::{logging, raw};
use std::os::raw::c_int;
use bloomfilter::Bloom;
use redis_module::RedisString;
use redis_module::RedisValue;

const BLOOM_FILTER_TYPE_ENCODING_VERSION: i32 = 0; 

pub static BLOOM_FILTER_TYPE2: RedisType = RedisType::new(
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

/// The BloomFilterType structure.
pub struct BloomFilterType2 {
    pub expansion: i64,
    pub fp_rate: f64,
    pub filters: Vec<BloomFilter>,
}

impl BloomFilterType2 {
    pub fn new_reserved(fp_rate: f64, capacity: usize, expansion: i64) -> BloomFilterType2 {
        let bloom = BloomFilter::new(
            fp_rate,
            capacity,
        );
        let mut filters = Vec::new();
        filters.push(bloom);
        BloomFilterType2 {
            expansion,
            fp_rate,
            filters,
        }
    }

    pub fn new_with_item(fp_rate: f64, capacity: usize, expansion: i64, item: &RedisString) -> BloomFilterType2 {
        let mut filter = BloomFilter::new(
            fp_rate,
            capacity,
        );
        filter.bloom.set(item.as_slice());
        filter.num_items = 1;
        let mut filters = Vec::new();
        filters.push(filter);
        BloomFilterType2 {
            expansion,
            fp_rate,
            filters,
        }
    }

    pub fn get_memory_usage(&self) -> usize {
        let mut mem = std::mem::size_of::<BloomFilterType2>();
        for filter in &self.filters {
            // TODO: `bitmap()` is a slow operation. Find an alternative to identify the memory usage.
            mem += std::mem::size_of::<BloomFilter>() + std::mem::size_of::<Bloom<u8>>() + filter.bloom.bitmap().len();
        }
        mem
    }

    // TODO: Check if we should change RedisString to slice
    pub fn item_exists(&self, item: &RedisString) -> bool {
        // Check if item exists already.
        for filter in &self.filters {
            if filter.bloom.check(&item) {
                return true;
            }
        }
        false
    }

    pub fn cardinality(&self) -> u64  {
        let mut cardinality = 0;
        // Check if item exists already.
        for filter in &self.filters {
            cardinality += filter.num_items;
        }
        cardinality
    }

    pub fn capacity(&self) -> u64  {
        let mut capacity = 0;
        // Check if item exists already.
        for filter in &self.filters {
            capacity += filter.capacity;
        }
        capacity
    }
    
    // TODO: Check if we should change RedisString to slice
    pub fn add_item(&mut self, item: &RedisString) -> RedisValue {
        // Check if item exists already.
        if self.item_exists(item) {
            return RedisValue::Integer(0);
        }
        if let Some(filter) = self.filters.last_mut() {
            if self.expansion == -1 || filter.num_items < filter.capacity  {
                // Add item.
                filter.bloom.set(&item);
                filter.num_items += 1;
                return RedisValue::Integer(1);
            }
            if filter.num_items >= filter.capacity {
                let new_capacity = filter.capacity * (self.expansion as u64);
                let mut new_filter = BloomFilter::new(self.fp_rate, new_capacity as usize);
                // Add item.
                new_filter.bloom.set(&item);
                new_filter.num_items += 1;
                self.filters.push(new_filter);
                return RedisValue::Integer(1);
            }
        }
        RedisValue::Integer(0)
    }
}

pub struct BloomFilter {
    pub bloom: Bloom<[u8]>,
    pub num_items: u64,
    pub capacity: u64,
}

impl BloomFilter {
    pub fn new(fp_rate: f64, capacity: usize) -> BloomFilter {
        // Instantiate empty bloom filter.
        let bloom = Bloom::new_for_fp_rate(
            capacity,
            fp_rate,
        );
        BloomFilter {
            bloom,
            num_items: 0,
            capacity: capacity as u64,
        }
    }
}

pub fn bloom_rdb_load_data_object(
    rdb: *mut raw::RedisModuleIO,
    encver: i32,
) -> Option<BloomFilterType2> {
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
        let bloom = Bloom::from_existing(
            bitmap.as_ref(),
            number_of_bits,
            number_of_hash_functions as u32,
            sip_keys,
        );
        let filter = BloomFilter {
            bloom,
            num_items,
            capacity,
        };
        filters.push(filter);
    }
    let item = BloomFilterType2 {
        expansion,
        fp_rate: fp_rate as f64,
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
