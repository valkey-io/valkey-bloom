use metrics::bloom_info_handler;
use valkey_module::{
    configuration::ConfigurationFlags, valkey_module, Context, InfoContext, Status, ValkeyResult,
    ValkeyString,
};
pub mod bloom;
pub mod configs;
pub mod metrics;
pub mod wrapper;
use crate::bloom::command_handler;
use crate::bloom::data_type::BLOOM_TYPE;
use crate::bloom::utils::valid_server_version;
use valkey_module::ModuleOptions;
use valkey_module_macros::command as valkey_command;
use valkey_module_macros::info_command_handler;

pub const MODULE_NAME: &str = "bf";
pub const MODULE_VERSION: i32 = 999999;
// The release stage is used in order to provide release status information.
// In unstable branch the status is always "dev".
// During release process the status will be set to rc1,rc2...rcN.
// When the version is released the status will be "ga".
pub const MODULE_RELEASE_STAGE: &str = "dev";

fn initialize(ctx: &Context, _args: &[ValkeyString]) -> Status {
    ctx.set_module_options(ModuleOptions::HANDLE_IO_ERRORS);
    let ver = ctx
        .get_server_version()
        .expect("Unable to get server version!");
    if !valid_server_version(ver) {
        ctx.log_warning(
            format!(
                "The minimum supported Valkey server version for the valkey-bloom module is {:?}",
                configs::BLOOM_MIN_SUPPORTED_VERSION
            )
            .as_str(),
        );
        Status::Err
    } else {
        Status::Ok
    }
}

fn deinitialize(_ctx: &Context) -> Status {
    Status::Ok
}

/// Command handler for BF.EXISTS <key> <item>
#[valkey_command({
    name: "BF.EXISTS",
    summary: "Determines if the bloom filter contains the specified item",
    complexity: "O(N), where N is the number of hash functions used by the bloom filter.",
    since: "1.0.0",
    flags: [ReadOnly, Fast],
    arity: 3,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 }),
    }],
})]
fn bloom_exists_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_exists(ctx, &args, false)
}

/// Command handler for BF.MEXISTS <key> <item> [<item> ...]
#[valkey_command({
    name: "BF.MEXISTS",
    summary: "Determines if the bloom filter contains one or more items",
    complexity: "O(K * N), where N is the number of hash functions used by the bloom filter and K is the number of items",
    since: "1.0.0",
    flags: [ReadOnly, Fast],
    arity: -3,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 }),
    }],
})]
fn bloom_mexists_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_exists(ctx, &args, true)
}

/// Command handler for BF.ADD <key> <item>
#[valkey_command({
    name: "BF.ADD",
    summary: "Add a single item to a bloom filter; creates the filter if it does not exist",
    complexity: "O(N), where N is the number of hash functions used by the bloom filter.",
    since: "1.0.0",
    flags: [Write, DenyOOM, Fast],
    arity: 3,
    key_spec: [{
        flags: [ReadWrite, Insert, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 }),
    }],
})]
fn bloom_add_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_add_value(ctx, &args, false)
}

/// Command handler for BF.MADD <key> <item> [<item> ...]
#[valkey_command({
    name: "BF.MADD",
    summary: "Add one or more items to a bloom filter; creates the filter if it does not exist",
    complexity: "O(N * K), where N is the number of hash functions used by the bloom filter and K is the number of items being added",
    since: "1.0.0",
    flags: [Write, DenyOOM, Fast],
    arity: -3,
    key_spec: [{
        flags: [ReadWrite, Insert, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 }),
    }],
})]
fn bloom_madd_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_add_value(ctx, &args, true)
}

/// Command handler for BF.CARD <key>
#[valkey_command({
    name: "BF.CARD",
    summary: "Returns the cardinality of a bloom filter",
    complexity: "O(1)",
    since: "1.0.0",
    flags: [ReadOnly, Fast],
    arity: 2,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 }),
    }],
})]
fn bloom_card_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_card(ctx, &args)
}

/// Command handler for BF.RESERVE <key> <false_positive_rate> <capacity> [EXPANSION <expansion>] | [NONSCALING]
#[valkey_command({
    name: "BF.RESERVE",
    summary: "Creates an empty bloom filter with the specified properties",
    complexity: "O(1)",
    since: "1.0.0",
    flags: [Write, DenyOOM, Fast],
    arity: -4,
    key_spec: [{
        flags: [ReadWrite, Insert],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 }),
    }],
})]
fn bloom_reserve_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_reserve(ctx, &args)
}

/// Command handler for BF.INFO <key> [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION | ERROR | MAXSCALEDCAPACITY]
#[valkey_command({
    name: "BF.INFO",
    summary: "Returns usage information and properties of a specific bloom filter",
    complexity: "O(1)",
    since: "1.0.0",
    flags: [ReadOnly, Fast],
    arity: -2,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 }),
    }],
})]
fn bloom_info_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_info(ctx, &args)
}

/// Command handler for:
/// BF.INSERT <key> [ERROR <fp_error>] [CAPACITY <capacity>] [EXPANSION <expansion>] [NOCREATE] [NONSCALING] [VALIDATESCALETO <validatescaleto>] ITEMS <item> [<item> ...]
#[valkey_command({
    name: "BF.INSERT",
    summary: "Creates a bloom filter with 0 or more items or adds items to an existing bloom filter",
    complexity: "O(N * K), where N is the number of hash functions used by the bloom filter and K is the number of items being added",
    since: "1.0.0",
    flags: [Write, DenyOOM, Fast],
    arity: -2,
    key_spec: [{
        flags: [ReadWrite, Insert, Update],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 }),
    }],
})]
fn bloom_insert_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_insert(ctx, &args)
}

/// Command handler for:
/// BF.LOAD <key> data
#[valkey_command({
    name: "BF.LOAD",
    summary: "Restores a bloom filter from a dump payload in a single operation",
    complexity: "O(N), where N is the capacity",
    since: "1.0.0",
    flags: [Write, DenyOOM],
    arity: 3,
    key_spec: [{
        flags: [ReadWrite, Insert],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 1, steps: 1, limit: 0 }),
    }],
})]
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
    version: MODULE_VERSION,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [
        BLOOM_TYPE,
    ],
    init: initialize,
    deinit: deinitialize,
    acl_categories: [
        "bloom",
    ]
    commands: [
        ["BF.ADD", bloom_add_command, "write fast deny-oom", 1, 1, 1, "fast write bloom"],
        ["BF.MADD", bloom_madd_command, "write fast deny-oom", 1, 1, 1, "fast write bloom"],
        ["BF.EXISTS", bloom_exists_command, "readonly fast", 1, 1, 1, "fast read bloom"],
        ["BF.MEXISTS", bloom_mexists_command, "readonly fast", 1, 1, 1, "fast read bloom"],
        ["BF.CARD", bloom_card_command, "readonly fast", 1, 1, 1, "fast read bloom"],
        ["BF.RESERVE", bloom_reserve_command, "write fast deny-oom", 1, 1, 1, "fast write bloom"],
        ["BF.INFO", bloom_info_command, "readonly fast", 1, 1, 1, "fast read bloom"],
        ["BF.INSERT", bloom_insert_command, "write fast deny-oom", 1, 1, 1, "fast write bloom"],
        ["BF.LOAD", bloom_load_command, "write deny-oom", 1, 1, 1, "write bloom"]
    ],
    configurations: [
        i64: [
            ["bloom-capacity", &*configs::BLOOM_CAPACITY, configs::BLOOM_CAPACITY_DEFAULT, configs::BLOOM_CAPACITY_MIN, configs::BLOOM_CAPACITY_MAX, ConfigurationFlags::DEFAULT, None],
            ["bloom-expansion", &*configs::BLOOM_EXPANSION, configs::BLOOM_EXPANSION_DEFAULT, 0, configs::BLOOM_EXPANSION_MAX as i64, ConfigurationFlags::DEFAULT, None],
            ["bloom-memory-usage-limit", &*configs::BLOOM_MEMORY_LIMIT_PER_OBJECT, configs::BLOOM_MEMORY_LIMIT_PER_OBJECT_DEFAULT, configs::BLOOM_MEMORY_LIMIT_PER_OBJECT_MIN, configs::BLOOM_MEMORY_LIMIT_PER_OBJECT_MAX, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
            ["bloom-fp-rate", &*configs::BLOOM_FP_RATE, configs::BLOOM_FP_RATE_DEFAULT, ConfigurationFlags::DEFAULT, None, Some(Box::new(configs::on_string_config_set))],
            ["bloom-tightening-ratio", &*configs::BLOOM_TIGHTENING_RATIO, configs::TIGHTENING_RATIO_DEFAULT, ConfigurationFlags::DEFAULT, None, Some(Box::new(configs::on_string_config_set))],
        ],
        bool: [
            ["bloom-use-random-seed", &*configs::BLOOM_USE_RANDOM_SEED, configs::BLOOM_USE_RANDOM_SEED_DEFAULT, ConfigurationFlags::DEFAULT, None],
            ["bloom-defrag-enabled", &*configs::BLOOM_DEFRAG, configs::BLOOM_DEFRAG_DEFAULT,  ConfigurationFlags::DEFAULT, None],
        ],
        enum: [
        ],
        module_args_as_configuration: true,
    ]
}
