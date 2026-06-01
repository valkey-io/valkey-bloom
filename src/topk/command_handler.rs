use crate::configs::TOPK_FIXED_SEED;
use crate::topk::data_type::TOPK_TYPE;
use crate::topk::utils;
use crate::topk::utils::TopKObject;
use valkey_module::NotifyEvent;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};

/// Replicate TOPK.RESERVE deterministically. We always append the resolved
/// seed (whether the user passed one or we generated one) so that replicas
/// build a CuckooTopK that hashes items the same way the primary does.
fn replicate_reserve(
    ctx: &Context,
    key_name: &ValkeyString,
    k: u32,
    width: u32,
    depth: u32,
    decay: f64,
    seed: u64,
) {
    let k_str = ValkeyString::create_from_slice(std::ptr::null_mut(), k.to_string().as_bytes());
    let width_str =
        ValkeyString::create_from_slice(std::ptr::null_mut(), width.to_string().as_bytes());
    let depth_str =
        ValkeyString::create_from_slice(std::ptr::null_mut(), depth.to_string().as_bytes());
    let decay_str =
        ValkeyString::create_from_slice(std::ptr::null_mut(), decay.to_string().as_bytes());
    let seed_token = ValkeyString::create_from_slice(std::ptr::null_mut(), b"SEED");
    let seed_val =
        ValkeyString::create_from_slice(std::ptr::null_mut(), seed.to_string().as_bytes());
    let cmd = vec![
        key_name,
        &k_str,
        &width_str,
        &depth_str,
        &decay_str,
        &seed_token,
        &seed_val,
    ];
    ctx.replicate("TOPK.RESERVE", cmd.as_slice());
    ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::RESERVE_EVENT, key_name);
}

/// Handle TOPK.RESERVE.
///
/// Syntax:
///     TOPK.RESERVE key topk [width depth decay] [SEED seed]
///
/// Only `key` and `topk` are required. The three sketch parameters
/// (width, depth, decay) are all-or-nothing: provide all three or omit all
/// three to take the defaults from `utils::DEFAULT_*`. The SEED keyword is
/// always optional; the literal token disambiguates the otherwise-ambiguous
/// trailing-seed case. When the user does not supply a seed, we generate a
/// random one on the primary. The resolved seed is always replicated so
/// replicas build a byte-identical sketch.
///
/// Valid argument counts: 3, 5, 6, 8.
pub fn topk_reserve(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    // Valid arities:
    //   3 = key topk
    //   5 = key topk SEED <n>
    //   6 = key topk width depth decay
    //   8 = key topk width depth decay SEED <n>
    if argc != 3 && argc != 5 && argc != 6 && argc != 8 {
        return Err(ValkeyError::WrongArity);
    }

    let mut idx = 1;
    let key_name = &input_args[idx];
    idx += 1;

    let k = parse_positive_u32(&input_args[idx], utils::BAD_TOPK, utils::TOPK_LARGER_THAN_0)?;
    idx += 1;

    // Sketch params are all-or-nothing. Arities 6 and 8 supply them; 3 and 5
    // take the documented defaults.
    let (width, depth, decay) = if argc >= 6 {
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

    // Trailing SEED <n> pair. Present iff arity is 5 or 8. The token must be
    // the literal "SEED" (case-insensitive) followed by a u64.
    let user_seed = if idx < argc {
        if !input_args[idx]
            .to_string_lossy()
            .eq_ignore_ascii_case("SEED")
        {
            return Err(ValkeyError::Str(utils::ERROR));
        }
        idx += 1;
        let seed = match input_args[idx].to_string_lossy().parse::<u64>() {
            Ok(num) => num,
            Err(_) => return Err(ValkeyError::Str(utils::INVALID_SEED)),
        };
        idx += 1;
        Some(seed)
    } else {
        None
    };

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
            replicate_reserve(ctx, key_name, k, width, depth, decay, seed);
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

/// Generate a u64 seed using stdlib only. Mixes a high-resolution timestamp
/// with the address of a stack local through DefaultHasher. Cheap and
/// non-cryptographic; sufficient for sketch hash diversification.
///
/// Guaranteed to never return `TOPK_FIXED_SEED` so that
/// `TopKObject::is_seed_random()` correctly reflects how the seed was
/// chosen. The collision probability is 1-in-2^64; the loop is a defensive
/// guard rather than a hot path.
fn random_seed() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    loop {
        let mut hasher = DefaultHasher::new();
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        now_nanos.hash(&mut hasher);
        let stack_marker = &now_nanos as *const u64 as u64;
        stack_marker.hash(&mut hasher);
        let candidate = hasher.finish();
        if candidate != TOPK_FIXED_SEED {
            return candidate;
        }
    }
}
