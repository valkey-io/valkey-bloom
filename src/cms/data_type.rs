use valkey_module::native_types::ValkeyType;
use valkey_module::raw;

const CMS_TYPE_ENCODING_VERSION: i32 = 1;

//Note this is mocked out for now.
pub static CMS_TYPE: ValkeyType = ValkeyType::new(
    "cntmnskch",
    CMS_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: None,
        rdb_save: None,
        aof_rewrite: None,
        digest: None,

        mem_usage: None,
        free: None,

        aux_load: None,

        aux_save: None,
        aux_save2: None,
        aux_save_triggers: raw::Aux::Before as i32,

        free_effort: None,
        unlink: None,
        copy: None,
        defrag: None,

        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);
