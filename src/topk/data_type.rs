use crate::topk::utils::TopKObject;
use crate::wrapper::topk_callback;
use crate::MODULE_NAME;
use heavykeeper::CuckooTopK;
use valkey_module::digest::Digest;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw};

/// Cell storage widths for the TopK sketch: u32 fingerprint and counter
/// halve per-cell memory versus the u64 default.
type Sketch = CuckooTopK<Vec<u8>, u32, u32>;

/// Used for decoding and encoding `TopKObject`. Currently used in AOF Rewrite.
/// Bump this when the serialized object layout changes.
pub const TOPK_OBJECT_VERSION: u8 = 1;

/// Module data type encoding version for TopK. Bump this whenever the
/// on-disk layout changes.
const TOPK_TYPE_ENCODING_VERSION: i32 = 1;

pub static TOPK_TYPE: ValkeyType = ValkeyType::new(
    "topk-type",
    TOPK_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(topk_callback::topk_rdb_load),
        rdb_save: Some(topk_callback::topk_rdb_save),
        aof_rewrite: Some(topk_callback::topk_aof_rewrite),
        digest: Some(topk_callback::topk_digest),

        mem_usage: Some(topk_callback::topk_mem_usage),
        free: Some(topk_callback::topk_free),

        aux_load: None,
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: 0,

        free_effort: Some(topk_callback::topk_free_effort),
        unlink: None,
        copy: Some(topk_callback::topk_copy),
        defrag: Some(topk_callback::topk_defrag),

        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

pub trait ValkeyDataType {
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<TopKObject>;
    fn debug_digest(&self, dig: Digest);
}

impl ValkeyDataType for TopKObject {
    /// Callback to load and parse RDB data of a TopK item and create it.
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<TopKObject> {
        if encver > TOPK_TYPE_ENCODING_VERSION {
            logging::log_warning(format!("{}: Cannot load topk-type data type of version {} because it is greater than the loaded module's topk-type supported version {}", MODULE_NAME, encver, TOPK_TYPE_ENCODING_VERSION).as_str());
            return None;
        }
        let Ok(seed) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(num_items) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(sketch_bytes) = raw::load_string_buffer(rdb) else {
            return None;
        };
        let sketch = match Sketch::from_bytes(sketch_bytes.as_ref(), seed) {
            Ok(sketch) => sketch,
            Err(err) => {
                logging::log_warning(format!("Failed to restore topk object: {}", err).as_str());
                return None;
            }
        };
        match TopKObject::from_serialized_bytes(seed, num_items, sketch, false) {
            Ok(item) => Some(item),
            Err(err) => {
                logging::log_warning(format!("Failed to restore topk object: {}", err).as_str());
                None
            }
        }
    }

    /// Function that is used to generate a digest on the Topk Object.
    fn debug_digest(&self, mut dig: Digest) {
        dig.add_long_long(self.seed() as i64);
        dig.add_long_long(self.num_items() as i64);
        dig.add_string_buffer(&self.sketch().to_bytes());
        dig.end_sequence();
    }
}
