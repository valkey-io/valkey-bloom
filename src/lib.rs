use redis_module::{redis_module, Context, RedisResult, RedisString, Status};
use redis_module::configuration::ConfigurationFlags;
pub mod bloom_config;
pub mod wrapper;
pub mod commands;
use crate::commands::{bloom};
use crate::commands::bloom_data_type::BLOOM_FILTER_TYPE2;

pub const MODULE_NAME: &str = "bloom";

fn initialize(_ctx: &Context, _args: &[RedisString]) -> Status {
    Status::Ok
}

pub fn deinitialize(_ctx: &Context) -> Status {
    Status::Ok
}

fn bloom_exists_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_exists(ctx, &args, false)
}

fn bloom_mexists_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_exists(ctx, &args, true)
}

fn bloom_add_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_add_value(ctx, &args)
}

fn bloom_card_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_card(ctx, &args)
}

fn bloom_reserve_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_reserve(ctx, &args)
}

fn bloom_info_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    bloom::bloom_filter_info(ctx, &args)
}

//////////////////////////////////////////////////////

redis_module! {
    name: MODULE_NAME,
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [
        BLOOM_FILTER_TYPE2,
    ],
    init: initialize,
    deinit: deinitialize,
    commands: [
        ["BF.ADD", bloom_add_command, "write fast deny-oom", 1, 1, 1],
        ["BF.EXISTS", bloom_exists_command, "readonly fast", 1, 1, 1],
        ["BF.MEXISTS", bloom_mexists_command, "readonly fast", 1, 1, 1],
        ["BF.CARD", bloom_card_command, "readonly fast", 1, 1, 1],
        ["BF.RESERVE", bloom_reserve_command, "write fast deny-oom", 1, 1, 1],
        ["BF.INFO", bloom_info_command, "readonly fast", 1, 1, 1],
    ],
    configurations: [
        i64: [
            ["bloom_max_item_size", &*bloom_config::BLOOM_MAX_ITEM_COUNT, bloom_config::BLOOM_MAX_ITEM_COUNT_DEFAULT, bloom_config::BLOOM_MAX_ITEM_COUNT_MIN, bloom_config::BLOOM_MAX_ITEM_COUNT_MAX, ConfigurationFlags::DEFAULT, None],
            ["bloom_expansion_rate", &*bloom_config::BLOOM_EXPANSION, bloom_config::BLOOM_EXPANSION_DEFAULT, bloom_config::BLOOM_EXPANSION_MIN, bloom_config::BLOOM_EXPANSION_MAX, ConfigurationFlags::DEFAULT, None],
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
