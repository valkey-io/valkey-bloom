use metrics::{bloom_info_handler, cuckoo_info_handler};
use valkey_module::{
    configuration::ConfigurationFlags, valkey_module, Context, InfoContext, Status, ValkeyResult,
    ValkeyString,
};
pub mod bloom;
pub mod cuckoo;
pub mod configs;
pub mod metrics;
pub mod wrapper;
use crate::bloom::command_handler;
use crate::bloom::data_type::BLOOM_TYPE;
use crate::bloom::utils::valid_server_version;
use crate::cuckoo::command_handler as cuckoo_handler;
use crate::cuckoo::data_type::CUCKOO_TYPE;
use valkey_module::ModuleOptions;
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

/// Command handler for BF.INFO <key> [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION | ERROR | MAXSCALEDCAPACITY]
fn bloom_info_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_info(ctx, &args)
}

/// Command handler for:
/// BF.INSERT <key> [ERROR <fp_error>] [CAPACITY <capacity>] [EXPANSION <expansion>] [NOCREATE] [NONSCALING] [VALIDATESCALETO <validatescaleto>] ITEMS <item> [<item> ...]
fn bloom_insert_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_insert(ctx, &args)
}

/// Command handler for:
/// BF.LOAD <key> data
fn bloom_load_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    command_handler::bloom_filter_load(ctx, &args)
}

// ==================== Cuckoo Filter Command Handlers ====================

/// Command handler for CF.ADD <key> <item>
fn cuckoo_add_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_add_value(ctx, args, false)
}

/// Command handler for CF.ADDNX <key> <item>
fn cuckoo_addnx_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_addnx(ctx, args, false)
}

/// Command handler for CF.COUNT <key> <item>
fn cuckoo_count_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_count(ctx, args)
}

/// Command handler for CF.DEL <key> <item>
fn cuckoo_del_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_delete(ctx, args)
}

/// Command handler for CF.EXISTS <key> <item>
fn cuckoo_exists_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_exists(ctx, args, false)
}

/// Command handler for CF.MEXISTS <key> <item> [<item> ...]
fn cuckoo_mexists_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_exists(ctx, args, true)
}

/// Command handler for CF.INSERT <key> [CAPACITY <cap>] [BUCKETSIZE <size>] [MAXITERATIONS <iterations>] [NOCREATE] ITEMS <item> [<item> ...]
fn cuckoo_insert_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_insert(ctx, args, false)
}

/// Command handler for CF.INSERTNX <key> [CAPACITY <cap>] [BUCKETSIZE <size>] [MAXITERATIONS <iterations>] [NOCREATE] ITEMS <item> [<item> ...]
fn cuckoo_insertnx_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_insert(ctx, args, true)
}

/// Command handler for CF.RESERVE <key> <capacity> [BUCKETSIZE <size>] [MAXITERATIONS <iterations>] [EXPANSION <expansion>]
fn cuckoo_reserve_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_reserve(ctx, args)
}

/// Command handler for CF.INFO <key> [field]
fn cuckoo_info_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_info(ctx, args)
}

/// Command handler for CF.LOAD <key> <data>
fn cuckoo_load_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    cuckoo_handler::cuckoo_filter_load(ctx, args)
}

///
/// Module Info
///
#[info_command_handler]
fn info_handler(ctx: &InfoContext, _for_crash_report: bool) -> ValkeyResult<()> {
    bloom_info_handler(ctx)?;
    cuckoo_info_handler(ctx)?;
    Ok(())
}

//////////////////////////////////////////////////////

valkey_module! {
    name: MODULE_NAME,
    version: MODULE_VERSION,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [
        BLOOM_TYPE,
        CUCKOO_TYPE,
    ],
    init: initialize,
    deinit: deinitialize,
    acl_categories: [
        "bloom",
        "cuckoo",
    ]
    commands: [
        // Bloom filter commands
        ["BF.ADD", bloom_add_command, "write fast deny-oom", 1, 1, 1, "fast write bloom"],
        ["BF.MADD", bloom_madd_command, "write fast deny-oom", 1, 1, 1, "fast write bloom"],
        ["BF.EXISTS", bloom_exists_command, "readonly fast", 1, 1, 1, "fast read bloom"],
        ["BF.MEXISTS", bloom_mexists_command, "readonly fast", 1, 1, 1, "fast read bloom"],
        ["BF.CARD", bloom_card_command, "readonly fast", 1, 1, 1, "fast read bloom"],
        ["BF.RESERVE", bloom_reserve_command, "write fast deny-oom", 1, 1, 1, "fast write bloom"],
        ["BF.INFO", bloom_info_command, "readonly fast", 1, 1, 1, "fast read bloom"],
        ["BF.INSERT", bloom_insert_command, "write fast deny-oom", 1, 1, 1, "fast write bloom"],
        ["BF.LOAD", bloom_load_command, "write deny-oom", 1, 1, 1, "write bloom"],
        // Cuckoo filter commands
        ["CF.ADD", cuckoo_add_command, "write fast deny-oom", 1, 1, 1, "fast write cuckoo"],
        ["CF.ADDNX", cuckoo_addnx_command, "write fast deny-oom", 1, 1, 1, "fast write cuckoo"],
        ["CF.COUNT", cuckoo_count_command, "readonly fast", 1, 1, 1, "fast read cuckoo"],
        ["CF.DEL", cuckoo_del_command, "write fast", 1, 1, 1, "fast write cuckoo"],
        ["CF.EXISTS", cuckoo_exists_command, "readonly fast", 1, 1, 1, "fast read cuckoo"],
        ["CF.MEXISTS", cuckoo_mexists_command, "readonly fast", 1, 1, 1, "fast read cuckoo"],
        ["CF.INFO", cuckoo_info_command, "readonly fast", 1, 1, 1, "fast read cuckoo"],
        ["CF.INSERT", cuckoo_insert_command, "write fast deny-oom", 1, 1, 1, "fast write cuckoo"],
        ["CF.INSERTNX", cuckoo_insertnx_command, "write fast deny-oom", 1, 1, 1, "fast write cuckoo"],
        ["CF.RESERVE", cuckoo_reserve_command, "write fast deny-oom", 1, 1, 1, "fast write cuckoo"],
        ["CF.LOAD", cuckoo_load_command, "write deny-oom", 1, 1, 1, "write cuckoo"]
    ],
    configurations: [
        i64: [
            ["bloom-capacity", &*configs::BLOOM_CAPACITY, configs::BLOOM_CAPACITY_DEFAULT, configs::BLOOM_CAPACITY_MIN, configs::BLOOM_CAPACITY_MAX, ConfigurationFlags::DEFAULT, None],
            ["bloom-expansion", &*configs::BLOOM_EXPANSION, configs::BLOOM_EXPANSION_DEFAULT, 0, configs::BLOOM_EXPANSION_MAX as i64, ConfigurationFlags::DEFAULT, None],
            ["bloom-memory-usage-limit", &*configs::BLOOM_MEMORY_LIMIT_PER_OBJECT, configs::BLOOM_MEMORY_LIMIT_PER_OBJECT_DEFAULT, configs::BLOOM_MEMORY_LIMIT_PER_OBJECT_MIN, configs::BLOOM_MEMORY_LIMIT_PER_OBJECT_MAX, ConfigurationFlags::DEFAULT, None],
            ["cuckoo-capacity", &*configs::CUCKOO_CAPACITY, configs::CUCKOO_CAPACITY_DEFAULT, configs::CUCKOO_CAPACITY_MIN, configs::CUCKOO_CAPACITY_MAX, ConfigurationFlags::DEFAULT, None],
            ["cuckoo-bucket-size", &*configs::CUCKOO_BUCKET_SIZE, configs::CUCKOO_BUCKET_SIZE_DEFAULT, configs::CUCKOO_BUCKET_SIZE_MIN, configs::CUCKOO_BUCKET_SIZE_MAX, ConfigurationFlags::DEFAULT, None],
            ["cuckoo-max-kicks", &*configs::CUCKOO_MAX_KICKS, configs::CUCKOO_MAX_KICKS_DEFAULT, configs::CUCKOO_MAX_KICKS_MIN, configs::CUCKOO_MAX_KICKS_MAX, ConfigurationFlags::DEFAULT, None],
            ["cuckoo-expansion", &*configs::CUCKOO_EXPANSION, configs::CUCKOO_EXPANSION_DEFAULT, configs::CUCKOO_EXPANSION_MIN as i64, configs::CUCKOO_EXPANSION_MAX as i64, ConfigurationFlags::DEFAULT, None],
            ["cuckoo-memory-usage-limit", &*configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT, configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT_DEFAULT, configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT_MIN, configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT_MAX, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
            ["bloom-fp-rate", &*configs::BLOOM_FP_RATE, configs::BLOOM_FP_RATE_DEFAULT, ConfigurationFlags::DEFAULT, None, Some(Box::new(configs::on_string_config_set))],
            ["bloom-tightening-ratio", &*configs::BLOOM_TIGHTENING_RATIO, configs::TIGHTENING_RATIO_DEFAULT, ConfigurationFlags::DEFAULT, None, Some(Box::new(configs::on_string_config_set))],
        ],
        bool: [
            ["bloom-use-random-seed", &*configs::BLOOM_USE_RANDOM_SEED, configs::BLOOM_USE_RANDOM_SEED_DEFAULT, ConfigurationFlags::DEFAULT, None],
            ["bloom-defrag-enabled", &*configs::BLOOM_DEFRAG, configs::BLOOM_DEFRAG_DEFAULT,  ConfigurationFlags::DEFAULT, None],
            ["cuckoo-defrag-enabled", &*configs::CUCKOO_DEFRAG, configs::CUCKOO_DEFRAG_DEFAULT, ConfigurationFlags::DEFAULT, None],
        ],
        enum: [
        ],
        module_args_as_configuration: true,
    ]
}
