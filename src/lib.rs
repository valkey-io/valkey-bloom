use redis_module::{redis_module, Context, RedisError, RedisResult, RedisString, Status, RedisValue};

fn initialize(ctx: &Context, _args: &[RedisString]) -> Status {
    println!("I am loaded");
    Status::Ok
}

pub fn deinitialize(_ctx: &Context) -> Status {
    Status::Ok
}

fn bloom_exists_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    Ok(RedisValue::Integer(0))
}

fn bloom_add_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    Ok(RedisValue::Integer(1))
}

fn bloom_card_command(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    Ok(RedisValue::Integer(0))
}

//////////////////////////////////////////////////////

redis_module! {
    name: "bloom",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    init: initialize,
    deinit: deinitialize,
    commands: [
        ["BF.ADD", bloom_add_command, "write fast deny-oom", 1, 1, 1],
        ["BF.EXISTS", bloom_exists_command, "readonly", 1, 1, 1],
        ["BF.CARD", bloom_card_command, "readonly", 1, -1, 1],
    ],
}
