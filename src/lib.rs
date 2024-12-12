use metrics::bloom_info_handler;
use valkey_module::{
    configuration::ConfigurationFlags, valkey_module, Context, InfoContext, Status, ValkeyGILGuard,
    ValkeyResult, ValkeyString,
};
pub mod bloom;
pub mod configs;
pub mod metrics;
pub mod wrapper;
use crate::bloom::command_handler;
use crate::bloom::data_type::BLOOM_FILTER_TYPE;
use valkey_module_macros::info_command_handler;

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

/// Command handler for:
/// BF.LOAD <key> data
fn bloom_load_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_load(ctx, &args)
}

///
/// Module Info
///
#[info_command_handler]
fn info_handler(ctx: &InfoContext, _for_crash_report: bool) -> ValkeyResult<()> {
    bloom_info_handler(ctx)
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
    acl_categories: [
        "bloom",
    ]
    commands: [
        ["BF.ADD", bloom_add_command, "write fast deny-oom", 1, 1, 1, "bloom"],
        ["BF.MADD", bloom_madd_command, "write fast deny-oom", 1, 1, 1, "bloom"],
        ["BF.EXISTS", bloom_exists_command, "readonly fast", 1, 1, 1, "bloom"],
        ["BF.MEXISTS", bloom_mexists_command, "readonly fast", 1, 1, 1, "bloom"],
        ["BF.CARD", bloom_card_command, "readonly fast", 1, 1, 1, "bloom"],
        ["BF.RESERVE", bloom_reserve_command, "write fast deny-oom", 1, 1, 1, "bloom"],
        ["BF.INFO", bloom_info_command, "readonly fast", 1, 1, 1, "bloom"],
        ["BF.INSERT", bloom_insert_command, "write fast deny-oom", 1, 1, 1, "bloom"],
        ["BF.LOAD", bloom_load_command, "write fast deny-oom", 1, 1, 1, "bloom"]
    ],
    configurations: [
        i64: [
            ["bloom-capacity", &*configs::BLOOM_CAPACITY, configs::BLOOM_CAPACITY_DEFAULT, configs::BLOOM_CAPACITY_MIN, configs::BLOOM_CAPACITY_MAX, ConfigurationFlags::DEFAULT, None],
            ["bloom-expansion", &*configs::BLOOM_EXPANSION, configs::BLOOM_EXPANSION_DEFAULT, configs::BLOOM_EXPANSION_MIN as i64, configs::BLOOM_EXPANSION_MAX as i64, ConfigurationFlags::DEFAULT, None],
            ["bloom-memory-limit-per-filter", &*configs::BLOOM_MEMORY_LIMIT_PER_FILTER, configs::BLOOM_MEMORY_LIMIT_PER_FILTER_DEFAULT, configs::BLOOM_MEMORY_LIMIT_PER_FILTER_MIN, configs::BLOOM_MEMORY_LIMIT_PER_FILTER_MAX, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
            ["bloom-fp-rate", &*configs::BLOOM_FP_RATE, configs::BLOOM_FP_RATE_DEFAULT, ConfigurationFlags::DEFAULT, None, Some(Box::new(configs::on_string_config_set))],
            ["bloom-tightening-ratio", &*configs::BLOOM_TIGHTENING_RATIO, configs::TIGHTENING_RATIO_DEFAULT, ConfigurationFlags::DEFAULT, None, Some(Box::new(configs::on_string_config_set))],
        ],
        bool: [
            ["bloom-use-random-seed", &*configs::BLOOM_USE_RANDOM_SEED, configs::BLOOM_USE_RANDOM_SEED_DEFAULT, ConfigurationFlags::DEFAULT, None],
            ["bloom-defrag-enabled", &*configs::BLOOM_DEFRAG, configs::BLOOM_DEFRAG_DEAFULT,  ConfigurationFlags::DEFAULT, None],
        ],
        enum: [
        ],
        module_args_as_configuration: true,
    ]
}
