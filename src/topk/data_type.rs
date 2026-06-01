use crate::topk::utils::TopKObject;
use crate::wrapper::topk_callback;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw};

/// Module data type RDB encoding version for TopK. Bump this whenever the
/// on-disk layout in topk_rdb_save/load changes.
const TOPK_TYPE_ENCODING_VERSION: i32 = 1;

pub static TOPK_TYPE: ValkeyType = ValkeyType::new(
    "topk-type",
    TOPK_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(topk_callback::topk_rdb_load),
        rdb_save: Some(topk_callback::topk_rdb_save),
        // AOF rewrite, digest, and defrag are wired up alongside TOPK.ADD when
        // we have sketch contents to persist and a stable layout to walk.
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

pub trait TopKDataType {
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<TopKObject>;
}

impl TopKDataType for TopKObject {
    /// Restore a TopKObject from RDB. Currently restores parameters only;
    /// sketch contents will be wired in alongside TOPK.ADD.
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<TopKObject> {
        if encver > TOPK_TYPE_ENCODING_VERSION {
            logging::log_warning(
                format!(
                    "topk: cannot load topk-type of version {} (module supports up to {})",
                    encver, TOPK_TYPE_ENCODING_VERSION
                )
                .as_str(),
            );
            return None;
        }
        let Ok(k) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(width) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(depth) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(decay) = raw::load_double(rdb) else {
            return None;
        };
        let Ok(seed) = raw::load_unsigned(rdb) else {
            return None;
        };
        if k == 0 || width == 0 || depth == 0 {
            logging::log_warning("topk: refusing to load object with zero k/width/depth");
            return None;
        }
        if !(decay > 0.0 && decay < 1.0) {
            logging::log_warning("topk: refusing to load object with decay outside (0, 1)");
            return None;
        }
        Some(TopKObject::from_existing(
            k as u32,
            width as u32,
            depth as u32,
            decay,
            seed,
        ))
    }
}
