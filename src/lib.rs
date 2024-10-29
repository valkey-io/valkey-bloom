use valkey_module::configuration::ConfigurationFlags;
use valkey_module::{valkey_module, Context, Status, ValkeyResult, ValkeyString};
pub mod bloom;
pub mod configs;
pub mod wrapper;
use crate::bloom::command_handler;
use crate::bloom::data_type::BLOOM_FILTER_TYPE;

pub const MODULE_NAME: &str = "bf";

fn initialize(_ctx: &Context, _args: &[ValkeyString]) -> Status {
    Status::Ok
}

fn deinitialize(_ctx: &Context) -> Status {
    Status::Ok
}

/// Command handler for BF.EXISTS <key> <item>
fn bloom_exists_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_exists(ctx, &args, false)
}

/// Command handler for BF.MEXISTS <key> <item> [<item> ...]
fn bloom_mexists_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_exists(ctx, &args, true)
}

/// Command handler for BF.ADD <key> <item>
fn bloom_add_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_add_value(ctx, &args, false)
}

/// Command handler for BF.MADD <key> <item> [<item> ...]
fn bloom_madd_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_add_value(ctx, &args, true)
}

/// Command handler for BF.CARD <key>
fn bloom_card_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_card(ctx, &args)
}

/// Command handler for BF.RESERVE <key> <false_positive_rate> <capacity> [EXPANSION <expansion>] | [NONSCALING]
fn bloom_reserve_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_reserve(ctx, &args)
}

/// Command handler for BF.INFO <key> [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]
fn bloom_info_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_info(ctx, &args)
}

/// Command handler for:
/// BF.INSERT <key> [ERROR <fp_error>] [CAPACITY <capacity>] [EXPANSION <expansion>] [NOCREATE] [NONSCALING] ITEMS <item> [<item> ...]
fn bloom_insert_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_insert(ctx, &args)
}

//////////////////////////////////////////////////////

valkey_module! {
    name: MODULE_NAME,
    version: 1,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [
        BLOOM_FILTER_TYPE,
    ],
    init: initialize,
    deinit: deinitialize,
    commands: [
        ["BF.ADD", bloom_add_command, "write fast deny-oom", 1, 1, 1],
        ["BF.MADD", bloom_madd_command, "write fast deny-oom", 1, 1, 1],
        ["BF.EXISTS", bloom_exists_command, "readonly fast", 1, 1, 1],
        ["BF.MEXISTS", bloom_mexists_command, "readonly fast", 1, 1, 1],
        ["BF.CARD", bloom_card_command, "readonly fast", 1, 1, 1],
        ["BF.RESERVE", bloom_reserve_command, "write fast deny-oom", 1, 1, 1],
        ["BF.INFO", bloom_info_command, "readonly fast", 1, 1, 1],
        ["BF.INSERT", bloom_insert_command, "write fast deny-oom", 1, 1, 1],
    ],
    configurations: [
        i64: [
            ["bloom-capacity", &*configs::BLOOM_CAPACITY, configs::BLOOM_CAPACITY_DEFAULT, configs::BLOOM_CAPACITY_MIN as i64, configs::BLOOM_CAPACITY_MAX as i64, ConfigurationFlags::DEFAULT, None],
            ["bloom-expansion", &*configs::BLOOM_EXPANSION, configs::BLOOM_EXPANSION_DEFAULT, configs::BLOOM_EXPANSION_MIN as i64, configs::BLOOM_EXPANSION_MAX as i64, ConfigurationFlags::DEFAULT, None],
            ["bloom-memory-limit-per-filter", &*configs::BLOOM_MEMORY_LIMIT_PER_FILTER, configs::BLOOM_MEMORY_LIMIT_PER_FILTER_DEFAULT, configs::BLOOM_MEMORY_LIMIT_PER_FILTER_MIN, configs::BLOOM_MEMORY_LIMIT_PER_FILTER_MAX, ConfigurationFlags::DEFAULT, None],
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
