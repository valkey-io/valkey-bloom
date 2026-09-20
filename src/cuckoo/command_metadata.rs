use std::ffi::CStr;
use std::ptr;
use valkey_module::{raw, Context, Status};

const COMMAND_INFO_VERSION: raw::RedisModuleCommandInfoVersion =
    raw::RedisModuleCommandInfoVersion {
        version: 1,
        sizeof_historyentry: std::mem::size_of::<raw::RedisModuleCommandHistoryEntry>(),
        sizeof_keyspec: std::mem::size_of::<raw::RedisModuleCommandKeySpec>(),
        sizeof_arg: std::mem::size_of::<raw::RedisModuleCommandArg>(),
    };

const COMMAND_ARITIES: &[(&CStr, i32)] = &[
    (c"CF.ADD", 3),
    (c"CF.ADDNX", 3),
    (c"CF.COUNT", 3),
    (c"CF.DEL", 3),
    (c"CF.EXISTS", 3),
    (c"CF.MEXISTS", -3),
    (c"CF.INFO", -2),
    (c"CF.INSERT", -4),
    (c"CF.INSERTNX", -4),
    (c"CF.RESERVE", -3),
    (c"CF.LOAD", 3),
];

/// Called from module initialization after the command-registration macro.
/// Keep the existing key specs and ACL categories; only supply command arity.
pub fn register_arity(ctx: &Context) -> Status {
    // The server initializes these API pointers before invoking module init.
    let (Some(get_command), Some(set_info)) =
        (unsafe { (raw::RedisModule_GetCommand, raw::RedisModule_SetCommandInfo) })
    else {
        ctx.log_warning("Command metadata API is unavailable.");
        return Status::Err;
    };
    for &(name, arity) in COMMAND_ARITIES {
        // The macro has already registered each name. GetCommand returns a
        // server-owned handle; SetCommandInfo copies the supplied information.
        let command = unsafe { get_command(ctx.ctx, name.as_ptr()) };
        if command.is_null() {
            ctx.log_warning("A registered Cuckoo command was not found.");
            return Status::Err;
        }
        let info = raw::RedisModuleCommandInfo {
            version: &COMMAND_INFO_VERSION,
            summary: ptr::null(),
            complexity: ptr::null(),
            since: ptr::null(),
            history: ptr::null_mut(),
            tips: ptr::null(),
            arity,
            key_specs: ptr::null_mut(),
            args: ptr::null_mut(),
        };
        if unsafe { set_info(command, &info) } != raw::Status::Ok as i32 {
            ctx.log_warning("Failed to register Cuckoo command arity.");
            return Status::Err;
        }
    }
    Status::Ok
}
