use crate::topk::utils::{
    TopKObject, TOPK_DEPTH_MAX, TOPK_DEPTH_MIN, TOPK_K_MAX, TOPK_K_MIN, TOPK_WIDTH_MAX,
    TOPK_WIDTH_MIN,
};
use crate::wrapper::topk_callback;
use crate::MODULE_NAME;
use heavykeeper::CuckooTopK;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw};

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
        aof_rewrite: None,
        digest: None,

        mem_usage: Some(topk_callback::topk_mem_usage),
        free: Some(topk_callback::topk_free),

        aux_load: None,
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: 0,

        free_effort: Some(topk_callback::topk_free_effort),
        unlink: None,
        copy: Some(topk_callback::topk_copy),
        defrag: None,

        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

pub trait ValkeyDataType {
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<TopKObject>;
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
        let sketch = match CuckooTopK::<Vec<u8>>::from_bytes(sketch_bytes.as_ref(), seed) {
            Ok(sketch) => sketch,
            Err(err) => {
                logging::log_warning(format!("Failed to restore topk object: {}", err).as_str());
                return None;
            }
        };
        let k = sketch.top_items() as u64;
        let width = sketch.width() as u64;
        let depth = sketch.depth() as u64;
        if !(TOPK_K_MIN as u64..=TOPK_K_MAX as u64).contains(&k) {
            logging::log_warning("Failed to restore topk object: k out of range");
            return None;
        }
        if !(TOPK_WIDTH_MIN as u64..=TOPK_WIDTH_MAX as u64).contains(&width) {
            logging::log_warning("Failed to restore topk object: width out of range");
            return None;
        }
        if !(TOPK_DEPTH_MIN as u64..=TOPK_DEPTH_MAX as u64).contains(&depth) {
            logging::log_warning("Failed to restore topk object: depth out of range");
            return None;
        }
        let decay = sketch.decay();
        if !(decay > 0.0 && decay < 1.0) {
            logging::log_warning("Failed to restore topk object: decay out of range");
            return None;
        }
        let item = TopKObject::from_existing(
            k as u32,
            width as u32,
            depth as u32,
            decay,
            seed,
            sketch,
            num_items,
        );
        Some(item)
    }
}
