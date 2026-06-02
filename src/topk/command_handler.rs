use crate::topk::data_type::TOPK_TYPE;
use crate::topk::utils;
use crate::topk::utils::TopKObject;
use valkey_module::NotifyEvent;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};

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
/// - RESERVE operation
/// - ADD operation: pending
fn replicate_and_notify_events(
    ctx: &Context,
    key_name: &ValkeyString,
    reserve_operation: bool,
    args: ReplicateArgs,
) {
    if reserve_operation {
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

    let k = parse_positive_u32(&input_args[idx], utils::BAD_TOPK, utils::TOPK_LARGER_THAN_0)?;
    idx += 1;

    // Optional leading SEED <n> block, immediately after `topk`.
    let mut user_seed: Option<u64> = None;
    if idx < argc && is_seed_token(&input_args[idx]) {
        idx += 1;
        user_seed = Some(parse_seed_value(input_args, idx, argc)?);
        idx += 1;
    }

    // Sketch params block: width depth decay (all three or none). If the
    // next token is the literal SEED, this block is skipped and the trailing
    // SEED handler picks it up.
    let (width, depth, decay) = if idx < argc && !is_seed_token(&input_args[idx]) {
        if argc - idx < 3 {
            // Arity is valid overall but the remaining tokens cannot form a
            // complete width/depth/decay tuple and the head is not SEED, so
            // the structure is a syntax error rather than a count error.
            return Err(ValkeyError::Str(utils::ERROR));
        }
        let width = parse_positive_u32(
            &input_args[idx],
            utils::BAD_WIDTH,
            utils::WIDTH_LARGER_THAN_0,
        )?;
        idx += 1;
        let depth = parse_positive_u32(
            &input_args[idx],
            utils::BAD_DEPTH,
            utils::DEPTH_LARGER_THAN_0,
        )?;
        idx += 1;
        let decay = match input_args[idx].to_string_lossy().parse::<f64>() {
            Ok(num) if num > 0.0 && num < 1.0 => num,
            Ok(_) => return Err(ValkeyError::Str(utils::DECAY_RANGE)),
            Err(_) => return Err(ValkeyError::Str(utils::BAD_DECAY)),
        };
        idx += 1;
        (width, depth, decay)
    } else {
        (
            utils::DEFAULT_WIDTH,
            utils::DEFAULT_DEPTH,
            utils::DEFAULT_DECAY,
        )
    };

    // Optional trailing SEED <n>. Reject if a leading SEED was already given.
    if idx < argc {
        if !is_seed_token(&input_args[idx]) {
            return Err(ValkeyError::Str(utils::ERROR));
        }
        if user_seed.is_some() {
            return Err(ValkeyError::WrongArity);
        }
        idx += 1;
        user_seed = Some(parse_seed_value(input_args, idx, argc)?);
        idx += 1;
    }

    if idx != argc {
        return Err(ValkeyError::WrongArity);
    }

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
            replicate_and_notify_events(ctx, key_name, true, replicate_args);
            VALKEY_OK
        }
        Err(_) => Err(ValkeyError::Str(utils::ERROR)),
    }
}

/// Parse an arg as a u32 that must be strictly positive. Returns the
/// `bad_format` error for unparseable input (including negatives) and
/// `not_positive` for explicit zero.
fn parse_positive_u32(
    arg: &ValkeyString,
    bad_format: &'static str,
    not_positive: &'static str,
) -> Result<u32, ValkeyError> {
    match arg.to_string_lossy().parse::<u32>() {
        Ok(0) => Err(ValkeyError::Str(not_positive)),
        Ok(n) => Ok(n),
        Err(_) => Err(ValkeyError::Str(bad_format)),
    }
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
