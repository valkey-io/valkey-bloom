use redis_module::{redis_module, Context, RedisResult, RedisString, Status};
use redis_module::configuration::ConfigurationFlags;
pub mod bloom_config;
pub mod wrapper;
pub mod commands;
use crate::commands::{bloom};

pub const MODULE_NAME: &str = "bloom";

fn initialize(_ctx: &Context, _args: &[RedisString]) -> Status {
    Status::Ok
}

pub fn deinitialize(_ctx: &Context) -> Status {
    Status::Ok
}

fn bloom_exists_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_exists(ctx, &args)
}

fn bloom_exists2_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_exists2(ctx, &args)
}

fn bloom_add_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_add_value(ctx, &args)
}

fn bloom_add2_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_add2_value(ctx, &args)
}

fn bloom_card_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_card(ctx, &args)
}

//////////////////////////////////////////////////////

redis_module! {
    name: MODULE_NAME,
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    init: initialize,
    deinit: deinitialize,
    commands: [
        ["BF.ADD", bloom_add_command, "write fast deny-oom", 1, 1, 1],
        ["BF.ADD2", bloom_add2_command, "write fast deny-oom", 1, 1, 1],
        ["BF.EXISTS", bloom_exists_command, "readonly", 1, 1, 1],
        ["BF.EXISTS2", bloom_exists2_command, "readonly", 1, 1, 1],
        ["BF.CARD", bloom_card_command, "readonly", 1, -1, 1],
    ],
    configurations: [
        i64: [
            ["bloom_max_item_size", &*bloom_config::BLOOM_MAX_ITEM_SIZE, bloom_config::BLOOM_MAX_ITEM_SIZE_DEFAULT, bloom_config::BLOOM_MAX_ITEM_SIZE_MIN, bloom_config::BLOOM_MAX_ITEM_SIZE_MAX, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
        ],
        bool: [
        ],
        enum: [
        ],
        module_args_as_configuration: true,
    ]
}
