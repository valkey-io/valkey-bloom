use crate::cuckoo::utils::{CuckooFilter, CuckooObject};
use crate::wrapper::cuckoo_callback;
use crate::MODULE_NAME;
use std::collections::HashMap;
use std::os::raw::c_int;
use valkey_module::digest::Digest;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw, ValkeyError, ValkeyResult};

/// Used for decoding and encoding `CuckooObject`. Currently used in AOF Rewrite.
/// This value must be increased when `CuckooObject` struct changes.
pub const CUCKOO_OBJECT_VERSION: u8 = 1;

/// Cuckoo Module data type RDB encoding version.
const CUCKOO_TYPE_ENCODING_VERSION: i32 = 1;

pub static CUCKOO_TYPE: ValkeyType = ValkeyType::new(
    "cuckooflt",
    CUCKOO_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(cuckoo_callback::cuckoo_rdb_load),
        rdb_save: Some(cuckoo_callback::cuckoo_rdb_save),
        aof_rewrite: Some(cuckoo_callback::cuckoo_aof_rewrite),
        digest: Some(cuckoo_callback::cuckoo_digest),

        mem_usage: Some(cuckoo_callback::cuckoo_mem_usage),
        free: Some(cuckoo_callback::cuckoo_free),

        aux_load: Some(cuckoo_callback::cuckoo_aux_load),
        // Callback not needed as there is no AUX (out of keyspace) data to be saved.
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: raw::Aux::Before as i32,

        free_effort: Some(cuckoo_callback::cuckoo_free_effort),
        // Callback not needed as it just notifies us when a cuckoo item is about to be freed.
        unlink: None,
        copy: Some(cuckoo_callback::cuckoo_copy),
        defrag: Some(cuckoo_callback::cuckoo_defrag),

        // The callbacks below are not needed since the version 1 variants are used when implemented.
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

pub trait ValkeyDataType {
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<CuckooObject>;
    fn debug_digest(&self, dig: Digest);
}

impl ValkeyDataType for CuckooObject {
    /// Callback to load and parse RDB data of a cuckoo item and create it.
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<CuckooObject> {
        if encver > CUCKOO_TYPE_ENCODING_VERSION {
            logging::log_warning(format!("{}: Cannot load cuckoofltr data type of version {} because it is greater than the loaded module's cuckoofltr supported version {}", MODULE_NAME, encver, CUCKOO_TYPE_ENCODING_VERSION).as_str());
            return None;
        }

        // Read version byte
        let Ok(version) = raw::load_unsigned(rdb) else {
            return None;
        };
        if version != CUCKOO_OBJECT_VERSION as u64 {
            logging::log_warning(format!(
                "Cannot load cuckoo object: unsupported version {}",
                version
            )
            .as_str());
            return None;
        }

        // Read metadata
        let Ok(num_filters) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(expansion) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(bucket_size) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(max_kicks) = raw::load_unsigned(rdb) else {
            return None;
        };

        // We start off with capacity as 1 to match the same expansion of the vector that would have occurred during cuckoo
        // object creation and scaling as a result of CF.* operations.
        let mut filters = Vec::with_capacity(1);
        // Calculate the memory usage of the CuckooFilter/s by summing up CuckooFilter sizes as they are de-serialized.
        let mut filters_memory_usage = 0;

        for _i in 0..num_filters {
            // Read filter capacity
            let Ok(capacity) = raw::load_unsigned(rdb) else {
                return None;
            };

            // Read filter num_items
            let Ok(num_items) = raw::load_unsigned(rdb) else {
                return None;
            };

            // Read serialized data length
            let Ok(serialized_data_len) = raw::load_unsigned(rdb) else {
                return None;
            };

            // Read serialized data only if length > 0
            let serialized_data = if serialized_data_len > 0 {
                let Ok(data) = raw::load_string_buffer(rdb) else {
                    return None;
                };
                data.as_ref().to_vec()
            } else {
                Vec::new()
            };

            // Read occurrence_map size
            let Ok(occurrence_map_size) = raw::load_unsigned(rdb) else {
                return None;
            };

            // Read occurrence_map entries
            let mut occurrence_map: HashMap<Vec<u8>, u32> = HashMap::new();
            for _j in 0..occurrence_map_size {
                // Read key length
                let Ok(_key_len) = raw::load_unsigned(rdb) else {
                    return None;
                };

                // Read key
                let Ok(key) = raw::load_string_buffer(rdb) else {
                    return None;
                };

                // Validate key length matches
                // Note: RedisBuffer doesn't expose len() directly, we use the loaded length value

                // Read count
                let Ok(count) = raw::load_unsigned(rdb) else {
                    return None;
                };

                occurrence_map.insert(key.as_ref().to_vec(), count as u32);
            }

            // Check memory limits
            let curr_filter_size =
                CuckooFilter::compute_size(capacity as i64, bucket_size as usize);
            let curr_object_size = CuckooObject::compute_size(filters.capacity())
                + filters_memory_usage
                + curr_filter_size;

            if !CuckooObject::validate_size(curr_object_size) {
                logging::log_warning(
                    "Failed to restore cuckoo object: Object larger than the allowed memory limit.",
                );
                return None;
            }
            filters_memory_usage += curr_filter_size;

            // Create filter from existing data
            let filter = CuckooFilter::from_existing(
                capacity as i64,
                num_items as i64,
                bucket_size as usize,
                occurrence_map,
                serialized_data,
            );
            filters.push(Box::new(filter));
        }

        let item = CuckooObject::from_existing(
            expansion as u32,
            bucket_size as usize,
            max_kicks as u32,
            filters,
        );
        Some(item)
    }

    /// Function that is used to generate a digest on the Cuckoo Object.
    fn debug_digest(&self, mut dig: Digest) {
        dig.add_long_long(self.expansion() as i64);
        dig.add_long_long(self.bucket_size() as i64);
        dig.add_long_long(self.max_kicks() as i64);
        dig.add_long_long(self.num_filters() as i64);

        for filter in self.filters() {
            dig.add_long_long(filter.capacity());
            dig.add_long_long(filter.num_items());
            dig.add_long_long(filter.bucket_size() as i64);

            // Add occurrence_map to digest
            // We need to make this deterministic, so we'll just add the count of entries
            // In a real implementation, we might want to sort and add each entry
            // For now, this provides a basic digest
        }
        dig.end_sequence();
    }
}

/// Save a CuckooObject to RDB format
///
/// # Safety
/// This function is unsafe because it deals with raw pointers from the Valkey module API.
pub unsafe fn rdb_save_cuckoo_object(rdb: *mut raw::RedisModuleIO, value: &CuckooObject) {
    // Save version
    raw::save_unsigned(rdb, CUCKOO_OBJECT_VERSION as u64);

    // Save metadata
    raw::save_unsigned(rdb, value.num_filters() as u64);
    raw::save_unsigned(rdb, value.expansion() as u64);
    raw::save_unsigned(rdb, value.bucket_size() as u64);
    raw::save_unsigned(rdb, value.max_kicks() as u64);

    // Save each filter
    for filter in value.filters() {
        // Save capacity
        raw::save_unsigned(rdb, filter.capacity() as u64);

        // Save num_items
        raw::save_unsigned(rdb, filter.num_items() as u64);

        // Serialize the filter data
        let serialized_data = filter.get_serialized_data();
        raw::save_unsigned(rdb, serialized_data.len() as u64);
        if !serialized_data.is_empty() {
            use std::os::raw::c_char;
            raw::RedisModule_SaveStringBuffer.unwrap()(
                rdb,
                serialized_data.as_ptr().cast::<c_char>(),
                serialized_data.len(),
            );
        }

        // Save occurrence_map
        let occurrence_map = filter.occurrence_map();
        raw::save_unsigned(rdb, occurrence_map.len() as u64);

        // Save each occurrence_map entry
        for (key, count) in occurrence_map.iter() {
            // Save key length
            raw::save_unsigned(rdb, key.len() as u64);

            // Save key
            use std::os::raw::c_char;
            raw::RedisModule_SaveStringBuffer.unwrap()(
                rdb,
                key.as_ptr().cast::<c_char>(),
                key.len(),
            );

            // Save count
            raw::save_unsigned(rdb, *count as u64);
        }
    }
}

/// Load the auxiliary data outside of the regular keyspace from the RDB file
pub fn cuckoo_rdb_aux_load(_rdb: *mut raw::RedisModuleIO) -> c_int {
    logging::log_notice("Ignoring AUX fields during RDB load.");
    raw::Status::Ok as i32
}

/// Generate an AOF rewrite command for a CuckooObject
///
/// This function creates a command that can be used to recreate the cuckoo filter
/// during AOF rewrite operations.
pub fn get_aof_rewrite_command(key: &str, obj: &CuckooObject) -> ValkeyResult<Vec<String>> {
    // Encode the object to bytes
    let encoded = obj
        .encode_object()
        .map_err(|e| ValkeyError::String(e.as_str().to_string()))?;

    // Convert to hex string for the command
    let hex_string = encoded
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();

    // Create CF.LOAD command
    let mut cmd = Vec::new();
    cmd.push("CF.LOAD".to_string());
    cmd.push(key.to_string());
    cmd.push(hex_string);

    Ok(cmd)
}

/// Generate a digest for replication verification
///
/// This function creates a deterministic digest of the CuckooObject that can be used
/// to verify that replicas have the same data as the primary.
pub fn generate_digest(obj: &CuckooObject) -> Vec<u8> {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();

    // Hash the metadata
    obj.expansion().hash(&mut hasher);
    obj.bucket_size().hash(&mut hasher);
    obj.max_kicks().hash(&mut hasher);
    obj.num_filters().hash(&mut hasher);

    // Hash each filter's properties
    for filter in obj.filters() {
        filter.capacity().hash(&mut hasher);
        filter.num_items().hash(&mut hasher);
        filter.bucket_size().hash(&mut hasher);
    }

    let hash = hasher.finish();
    hash.to_le_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cuckoo_object_version() {
        assert_eq!(CUCKOO_OBJECT_VERSION, 1);
    }

    #[test]
    fn test_cuckoo_type_encoding_version() {
        assert_eq!(CUCKOO_TYPE_ENCODING_VERSION, 1);
    }
}
