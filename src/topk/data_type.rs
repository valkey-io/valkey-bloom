use crate::wrapper::topk_callback;
use valkey_module::native_types::ValkeyType;
use valkey_module::raw;

/// Module data type encoding version for TopK. Bump this whenever the
/// on-disk layout changes. RDB save/load callbacks are intentionally left
/// `None` until TOPK.ADD lands and we can round-trip sketch contents.
const TOPK_TYPE_ENCODING_VERSION: i32 = 1;

pub static TOPK_TYPE: ValkeyType = ValkeyType::new(
    "topk-type",
    TOPK_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        // RDB, AOF, digest, and defrag are wired up alongside TOPK.ADD when
        // we have sketch contents to persist and a stable layout to walk.
        rdb_load: None,
        rdb_save: None,
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
