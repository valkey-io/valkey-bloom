use crate::bloom::utils::BloomFilter;
use crate::bloom::utils::BloomFilterType;
use crate::configs;
use crate::metrics::BLOOM_NUM_OBJECTS;
use crate::metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES;
use crate::wrapper::bloom_callback;
use crate::wrapper::digest::Digest;
use crate::MODULE_NAME;
use std::mem;
use std::os::raw::c_int;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw};

/// Used for decoding and encoding `BloomFilterType`. Currently used in AOF Rewrite.
/// This value must increased when `BloomFilterType` struct change.
pub const BLOOM_TYPE_VERSION: u8 = 1;

const BLOOM_FILTER_TYPE_ENCODING_VERSION: i32 = 1;

pub static BLOOM_FILTER_TYPE: ValkeyType = ValkeyType::new(
    "bloomfltr",
    BLOOM_FILTER_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(bloom_callback::bloom_rdb_load),
        rdb_save: Some(bloom_callback::bloom_rdb_save),
        aof_rewrite: Some(bloom_callback::bloom_aof_rewrite),
        digest: Some(bloom_callback::bloom_digest),

        mem_usage: Some(bloom_callback::bloom_mem_usage),
        free: Some(bloom_callback::bloom_free),

        aux_load: Some(bloom_callback::bloom_aux_load),
        // Callback not needed as there is no AUX (out of keyspace) data to be saved.
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: raw::Aux::Before as i32,

        free_effort: Some(bloom_callback::bloom_free_effort),
        // Callback not needed as it just notifies us when a bloom item is about to be freed.
        unlink: None,
        copy: Some(bloom_callback::bloom_copy),
        defrag: Some(bloom_callback::bloom_defrag),

        // The callbacks below are not needed since the version 1 variants are used when implemented.
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

pub trait ValkeyDataType {
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<BloomFilterType>;
    fn debug_digest(&self, dig: Digest);
}

impl ValkeyDataType for BloomFilterType {
    /// Callback to load and parse RDB data of a bloom item and create it.
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<BloomFilterType> {
        if encver > BLOOM_FILTER_TYPE_ENCODING_VERSION {
            logging::log_warning(format!("{}: Cannot load bloomfltr data type of version {} because it is higher than the loaded module's bloomfltr supported version {}", MODULE_NAME, encver, BLOOM_FILTER_TYPE_ENCODING_VERSION).as_str());
            return None;
        }
        let Ok(num_filters) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(expansion) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(fp_rate) = raw::load_double(rdb) else {
            return None;
        };
        let Ok(tightening_ratio) = raw::load_double(rdb) else {
            return None;
        };
        let mut filters: Vec<BloomFilter> = Vec::with_capacity(num_filters as usize);
        let Ok(is_seed_random_u64) = raw::load_unsigned(rdb) else {
            return None;
        };
        let is_seed_random = is_seed_random_u64 == 1;
        for i in 0..num_filters {
            let Ok(bitmap) = raw::load_string_buffer(rdb) else {
                return None;
            };
            let Ok(capacity) = raw::load_unsigned(rdb) else {
                return None;
            };
            let new_fp_rate = match Self::calculate_fp_rate(fp_rate, num_filters as i32) {
                Ok(rate) => rate,
                Err(_) => {
                    logging::log_warning(
                        "Failed to restore bloom object: Reached max number of filters",
                    );
                    return None;
                }
            };
            if !BloomFilter::validate_size(capacity as u32, new_fp_rate) {
                logging::log_warning("Failed to restore bloom object: Contains a filter larger than the max allowed size limit.");
                return None;
            }
            // Only load num_items when it's the last filter
            let num_items = if i == num_filters - 1 {
                match raw::load_unsigned(rdb) {
                    Ok(num_items) => num_items,
                    Err(_) => return None,
                }
            } else {
                capacity
            };
            let filter =
                BloomFilter::from_existing(bitmap.as_ref(), num_items as u32, capacity as u32);
            if !is_seed_random && filter.seed() != configs::FIXED_SEED {
                logging::log_warning("Failed to restore bloom object: Object in fixed seed mode, but seed does not match FIXED_SEED.");
                return None;
            }
            filters.push(filter);
        }
        BLOOM_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
            mem::size_of::<BloomFilterType>(),
            std::sync::atomic::Ordering::Relaxed,
        );
        BLOOM_NUM_OBJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let item = BloomFilterType {
            expansion: expansion as u32,
            fp_rate,
            tightening_ratio,
            is_seed_random,
            filters,
        };
        Some(item)
    }

    /// Function that is used to generate a digest on the Bloom Object.
    fn debug_digest(&self, mut dig: Digest) {
        dig.add_long_long(self.expansion.into());
        dig.add_string_buffer(&self.fp_rate.to_le_bytes());
        dig.add_string_buffer(&self.tightening_ratio.to_le_bytes());
        for filter in &self.filters {
            dig.add_string_buffer(filter.bloom.as_slice());
            dig.add_long_long(filter.num_items.into());
            dig.add_long_long(filter.capacity.into());
        }
        dig.end_sequence();
    }
}

/// Load the auxiliary data outside of the regular keyspace from the RDB file
pub fn bloom_rdb_aux_load(_rdb: *mut raw::RedisModuleIO) -> c_int {
    logging::log_notice("Ignoring AUX fields during RDB load.");
    raw::Status::Ok as i32
}
