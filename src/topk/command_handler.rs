use crate::topk::data_type::TOPK_TYPE;
use crate::topk::utils;
use crate::topk::utils::TopKObject;
use valkey_module::NotifyEvent;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, VALKEY_OK};

/// Structure to help provide the command arguments required for replication. This is used by mutative commands.
struct ReplicateArgs {
    k: u32,
    width: u32,
    depth: u32,
    decay: f64,
    seed: u64,
}

/// Helper function to replicate mutative commands to the replica nodes and publish keyspace events.
/// There are two main cases for replication:
/// - RESERVE operation: replays a deterministic TOPK.RESERVE on replicas.
/// - ADD operation: verbatim replication is safe because TOPK.ADD is
///   deterministic given the same sketch state, and replicas were seeded
///   identically via the replicated TOPK.RESERVE.
fn replicate_and_notify_events(
    ctx: &Context,
    key_name: &ValkeyString,
    reserve_operation: bool,
    add_operation: bool,
    args: Option<ReplicateArgs>,
) {
    if reserve_operation {
        let args = args.expect("reserve replication requires ReplicateArgs");
        let k_val =
            ValkeyString::create_from_slice(std::ptr::null_mut(), args.k.to_string().as_bytes());
        let width_val = ValkeyString::create_from_slice(
            std::ptr::null_mut(),
            args.width.to_string().as_bytes(),
        );
        let depth_val = ValkeyString::create_from_slice(
            std::ptr::null_mut(),
            args.depth.to_string().as_bytes(),
        );
        let decay_val = ValkeyString::create_from_slice(
            std::ptr::null_mut(),
            args.decay.to_string().as_bytes(),
        );
        let seed_str = ValkeyString::create_from_slice(std::ptr::null_mut(), "SEED".as_bytes());
        let seed_val =
            ValkeyString::create_from_slice(std::ptr::null_mut(), args.seed.to_string().as_bytes());
        let cmd = vec![
            key_name, &k_val, &width_val, &depth_val, &decay_val, &seed_str, &seed_val,
        ];
        ctx.replicate("TOPK.RESERVE", cmd.as_slice());
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::RESERVE_EVENT, key_name);
    } else if add_operation {
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::ADD_EVENT, key_name);
    }
}

/// Handle TOPK.RESERVE.
///
/// Syntax:
///     TOPK.RESERVE key topk [SEED seed] [width depth decay] [SEED seed]
///
/// Only `key` and `topk` are required.
/// The SEED keyword is always optional and may appear either right after `topk` or at the very end (but not both).
/// When the user does not supply a seed, we generate a random one on the primary.
pub fn topk_reserve(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    // Valid arities:
    //   3 = key topk
    //   5 = key topk SEED <n>
    //   6 = key topk width depth decay
    //   8 = key topk width depth decay SEED <n>
    //       key topk SEED <n> width depth decay
    if argc != 3 && argc != 5 && argc != 6 && argc != 8 {
        return Err(ValkeyError::WrongArity);
    }

    let mut idx = 1;
    let key_name = &input_args[idx];
    idx += 1;

    let k = match input_args[idx].to_string_lossy().parse::<u32>() {
        Ok(0) => return Err(ValkeyError::Str(utils::TOPK_LARGER_THAN_0)),
        Ok(num) if (utils::TOPK_K_MIN..=utils::TOPK_K_MAX).contains(&num) => num,
        _ => return Err(ValkeyError::Str(utils::BAD_TOPK)),
    };
    idx += 1;

    let mut user_seed: Option<u64> = None;
    let mut sketch: Option<(u32, u32, f64)> = None;
    while idx < argc {
        if is_seed_token(&input_args[idx]) {
            if user_seed.is_some() {
                return Err(ValkeyError::WrongArity);
            }
            idx += 1;
            user_seed = Some(parse_seed_value(input_args, idx, argc)?);
            idx += 1;
        } else {
            if sketch.is_some() {
                return Err(ValkeyError::Str(utils::ERROR));
            }
            if argc - idx < 3 {
                return Err(ValkeyError::Str(utils::ERROR));
            }
            let width = match input_args[idx].to_string_lossy().parse::<u32>() {
                Ok(0) => return Err(ValkeyError::Str(utils::WIDTH_LARGER_THAN_0)),
                Ok(num) if (utils::TOPK_WIDTH_MIN..=utils::TOPK_WIDTH_MAX).contains(&num) => num,
                _ => return Err(ValkeyError::Str(utils::BAD_WIDTH)),
            };
            idx += 1;
            let depth = match input_args[idx].to_string_lossy().parse::<u32>() {
                Ok(0) => return Err(ValkeyError::Str(utils::DEPTH_LARGER_THAN_0)),
                Ok(num) if (utils::TOPK_DEPTH_MIN..=utils::TOPK_DEPTH_MAX).contains(&num) => num,
                _ => return Err(ValkeyError::Str(utils::BAD_DEPTH)),
            };
            idx += 1;
            let decay = match input_args[idx].to_string_lossy().parse::<f64>() {
                Ok(num) if num > 0.0 && num < 1.0 => num,
                Ok(_) => return Err(ValkeyError::Str(utils::DECAY_RANGE)),
                Err(_) => return Err(ValkeyError::Str(utils::BAD_DECAY)),
            };
            idx += 1;
            sketch = Some((width, depth, decay));
        }
    }

    let (width, depth, decay) = sketch.unwrap_or((
        utils::DEFAULT_WIDTH,
        utils::DEFAULT_DEPTH,
        utils::DEFAULT_DECAY,
    ));

    // Reject if the key already exists. TOPK params are immutable for the
    // lifetime of the object, so a second RESERVE cannot mutate the sketch.
    let key = ctx.open_key_writable(key_name);
    match key.get_value::<TopKObject>(&TOPK_TYPE) {
        Ok(Some(_)) => return Err(ValkeyError::Str(utils::KEY_EXISTS)),
        Ok(None) => {}
        Err(_) => return Err(ValkeyError::WrongType),
    };

    let seed = user_seed.unwrap_or_else(random_seed);

    let topk = TopKObject::new_reserved(k, width, depth, decay, seed);
    match key.set_value(&TOPK_TYPE, topk) {
        Ok(()) => {
            let replicate_args = ReplicateArgs {
                k,
                width,
                depth,
                decay,
                seed,
            };
            replicate_and_notify_events(ctx, key_name, true, false, Some(replicate_args));
            VALKEY_OK
        }
        Err(_) => Err(ValkeyError::Str(utils::ERROR)),
    }
}

fn add_with_increments<'a>(
    topk: &mut TopKObject,
    items: impl ExactSizeIterator<Item = (&'a [u8], u64)>,
) -> Vec<ValkeyValue> {
    let mut result: Vec<ValkeyValue> = Vec::with_capacity(items.len());
    for (item, increment) in items {
        match topk.add(item, increment) {
            Some(evicted) => result.push(ValkeyValue::StringBuffer(evicted)),
            None => result.push(ValkeyValue::Null),
        }
    }
    result
}

/// Handle TOPK.ADD.
///
/// Syntax:
///     TOPK.ADD key item [item ...]
///
/// Returns an array, one entry per input item, in order:
///   - Null if no heavy-slot resident was displaced by the insertion.
///   - The bulk-string of the displaced item otherwise.
pub fn topk_add(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = &input_args[1];
    let key = ctx.open_key_writable(key_name);
    let topk = match key.get_value::<TopKObject>(&TOPK_TYPE) {
        Ok(Some(v)) => v,
        Ok(None) => return Err(ValkeyError::Str(utils::NOT_FOUND)),
        Err(_) => return Err(ValkeyError::WrongType),
    };

    let items = &input_args[2..];
    let result = add_with_increments(topk, items.iter().map(|item| (item.as_slice(), 1)));

    replicate_and_notify_events(ctx, key_name, false, true, None);
    Ok(ValkeyValue::Array(result))
}

/// Handle TOPK.INCRBY.
///
/// Syntax:
///     TOPK.INCRBY key item increment [item increment ...]
///
/// Like TOPK.ADD, but each item carries an explicit increment.
/// Returns an array, one entry per item/increment pair, in order:
///   - Null if no heavy-slot resident was displaced by the insertion.
///   - The bulk-string of the displaced item otherwise.
pub fn topk_incrby(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    // key + at least one item/increment pair, and the pairs must be complete.
    if argc < 4 || !argc.is_multiple_of(2) {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = &input_args[1];
    let key = ctx.open_key_writable(key_name);
    let topk = match key.get_value::<TopKObject>(&TOPK_TYPE) {
        Ok(Some(v)) => v,
        Ok(None) => return Err(ValkeyError::Str(utils::NOT_FOUND)),
        Err(_) => return Err(ValkeyError::WrongType),
    };

    let pairs = &input_args[2..];
    let mut parsed: Vec<(&ValkeyString, u64)> = Vec::with_capacity(pairs.len() / 2);
    for pair in pairs.chunks_exact(2) {
        let item = &pair[0];
        let increment = match pair[1].to_string_lossy().parse::<u64>() {
            Ok(0) => return Err(ValkeyError::Str(utils::BAD_INCREMENT)),
            Ok(num) => num,
            Err(_) => return Err(ValkeyError::Str(utils::BAD_INCREMENT)),
        };
        parsed.push((item, increment));
    }

    let result = add_with_increments(
        topk,
        parsed
            .iter()
            .map(|(item, increment)| (item.as_slice(), *increment)),
    );

    replicate_and_notify_events(ctx, key_name, false, true, None);
    Ok(ValkeyValue::Array(result))
}

/// Handle TOPK.INFO.
///
/// Syntax:
///     TOPK.INFO key
///
/// Returns the number of required items (k), width, depth, and decay of the
/// sketch stored at `key`.
pub fn topk_info(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    if input_args.len() != 2 {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = &input_args[1];
    let key = ctx.open_key(key_name);
    let topk = match key.get_value::<TopKObject>(&TOPK_TYPE) {
        Ok(Some(topk)) => topk,
        Ok(None) => return Err(ValkeyError::Str(utils::NOT_FOUND)),
        Err(_) => return Err(ValkeyError::WrongType),
    };

    let result = vec![
        ValkeyValue::SimpleStringStatic("k"),
        ValkeyValue::Integer(topk.k() as i64),
        ValkeyValue::SimpleStringStatic("width"),
        ValkeyValue::Integer(topk.width() as i64),
        ValkeyValue::SimpleStringStatic("depth"),
        ValkeyValue::Integer(topk.depth() as i64),
        ValkeyValue::SimpleStringStatic("decay"),
        ValkeyValue::Float(topk.decay()),
    ];
    Ok(ValkeyValue::Array(result))
}

/// Handle TOPK.LIST.
///
/// Syntax:
///     TOPK.LIST key [WITHCOUNT]
///
/// Return the full list of items in Top-K sketch.
/// With WITHCOUNT, each item is followed by its estimated count.
pub fn topk_list(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if !(2..=3).contains(&argc) {
        return Err(ValkeyError::WrongArity);
    }

    let with_count = if argc == 3 {
        if input_args[2]
            .to_string_lossy()
            .eq_ignore_ascii_case("WITHCOUNT")
        {
            true
        } else {
            return Err(ValkeyError::Str(utils::ERROR));
        }
    } else {
        false
    };

    let key_name = &input_args[1];
    let key = ctx.open_key(key_name);
    let topk = match key.get_value::<TopKObject>(&TOPK_TYPE) {
        Ok(Some(topk)) => topk,
        Ok(None) => return Err(ValkeyError::Str(utils::NOT_FOUND)),
        Err(_) => return Err(ValkeyError::WrongType),
    };

    let items = topk.list();
    let mut result: Vec<ValkeyValue> = Vec::with_capacity(if with_count {
        items.len() * 2
    } else {
        items.len()
    });
    for (item, count) in items {
        result.push(ValkeyValue::StringBuffer(item));
        if with_count {
            result.push(ValkeyValue::Integer(count as i64));
        }
    }
    Ok(ValkeyValue::Array(result))
}

/// Case-insensitive match for the literal SEED keyword.
fn is_seed_token(arg: &ValkeyString) -> bool {
    arg.to_string_lossy().eq_ignore_ascii_case("SEED")
}

/// Parse the u64 value that must follow a SEED keyword.
fn parse_seed_value(args: &[ValkeyString], idx: usize, argc: usize) -> Result<u64, ValkeyError> {
    if idx >= argc {
        return Err(ValkeyError::WrongArity);
    }
    args[idx]
        .to_string_lossy()
        .parse::<u64>()
        .map_err(|_| ValkeyError::Str(utils::INVALID_SEED))
}

/// Generate a u64 seed using stdlib only. Mixes a high-resolution timestamp
/// with the address of a stack local through DefaultHasher. Cheap and
/// non-cryptographic; sufficient for sketch hash diversification.
fn random_seed() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut hasher = DefaultHasher::new();
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    now_nanos.hash(&mut hasher);
    let stack_marker = &now_nanos as *const u64 as u64;
    stack_marker.hash(&mut hasher);
    hasher.finish()
}
