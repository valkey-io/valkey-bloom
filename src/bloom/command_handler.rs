use crate::bloom::data_type::BLOOM_TYPE;
use crate::bloom::utils;
use crate::bloom::utils::BloomObject;
use crate::configs;
use crate::configs::{
    BLOOM_CAPACITY_MAX, BLOOM_CAPACITY_MIN, BLOOM_EXPANSION_MAX, BLOOM_EXPANSION_MIN,
    BLOOM_FP_RATE_MAX, BLOOM_FP_RATE_MIN, BLOOM_TIGHTENING_RATIO_MAX, BLOOM_TIGHTENING_RATIO_MIN,
};
use std::sync::atomic::Ordering;
use valkey_module::ContextFlags;
use valkey_module::NotifyEvent;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, VALKEY_OK};

/// Helper function used to add items to a bloom object. It handles both multi item and single item add operations.
/// It is used by any command that allows adding of items: BF.ADD, BF.MADD, and BF.INSERT.
/// Returns the result of the item add operation on success as a ValkeyValue and a ValkeyError on failure.
fn handle_bloom_add(
    args: &[ValkeyString],
    argc: usize,
    item_idx: usize,
    bf: &mut BloomObject,
    multi: bool,
    add_succeeded: &mut bool,
    validate_size_limit: bool,
) -> Result<ValkeyValue, ValkeyError> {
    match multi {
        true => {
            let mut result = Vec::new();
            for item in args.iter().take(argc).skip(item_idx) {
                match bf.add_item(item.as_slice(), validate_size_limit) {
                    Ok(add_result) => {
                        if add_result == 1 {
                            *add_succeeded = true;
                        }
                        result.push(ValkeyValue::Integer(add_result));
                    }
                    Err(err) => {
                        result.push(ValkeyValue::StaticError(err.as_str()));
                        break;
                    }
                };
            }
            Ok(ValkeyValue::Array(result))
        }
        false => {
            let item = args[item_idx].as_slice();
            match bf.add_item(item, validate_size_limit) {
                Ok(add_result) => {
                    *add_succeeded = add_result == 1;
                    Ok(ValkeyValue::Integer(add_result))
                }
                Err(err) => Err(ValkeyError::Str(err.as_str())),
            }
        }
    }
}

/// Structure to help provide the command arguments required for replication. This is used by mutative commands.
struct ReplicateArgs<'a> {
    capacity: i64,
    expansion: u32,
    fp_rate: f64,
    tightening_ratio: f64,
    seed: [u8; 32],
    items: &'a [ValkeyString],
}

/// Helper function to replicate mutative commands to the replica nodes and publish keyspace events.
/// There are two main cases for replication:
/// - RESERVE operation: This is any bloom object creation which will be replicated with the exact properties of the
///   primary node using BF.INSERT.
/// - ADD operation: This is the case where only items were added to a bloom object. Here, the command is replicated verbatim.
///
/// With this, replication becomes deterministic.
/// For keyspace events, we publish an event for both the RESERVE and ADD scenarios depending on if either or both of the
/// cases occurred.
fn replicate_and_notify_events(
    ctx: &Context,
    key_name: &ValkeyString,
    add_operation: bool,
    reserve_operation: bool,
    args: ReplicateArgs,
) {
    if reserve_operation {
        // Any bloom filter creation should have a deterministic replication with the exact same properties as what was
        // created on the primary. This is done using BF.INSERT.
        let capacity_str =
            ValkeyString::create_from_slice(std::ptr::null_mut(), "CAPACITY".as_bytes());
        let capacity_val = ValkeyString::create_from_slice(
            std::ptr::null_mut(),
            args.capacity.to_string().as_bytes(),
        );
        let fp_rate_str = ValkeyString::create_from_slice(std::ptr::null_mut(), "ERROR".as_bytes());
        let fp_rate_val = ValkeyString::create_from_slice(
            std::ptr::null_mut(),
            args.fp_rate.to_string().as_bytes(),
        );
        let tightening_str =
            ValkeyString::create_from_slice(std::ptr::null_mut(), "TIGHTENING".as_bytes());
        let tightening_val = ValkeyString::create_from_slice(
            std::ptr::null_mut(),
            args.tightening_ratio.to_string().as_bytes(),
        );
        let seed_str = ValkeyString::create_from_slice(std::ptr::null_mut(), "SEED".as_bytes());
        let seed_val = ValkeyString::create_from_slice(std::ptr::null_mut(), &args.seed);
        let mut cmd = vec![
            key_name,
            &capacity_str,
            &capacity_val,
            &fp_rate_str,
            &fp_rate_val,
            &tightening_str,
            &tightening_val,
            &seed_str,
            &seed_val,
        ];
        // Add nonscaling / expansion related arguments.
        let expansion_args = match args.expansion == 0 {
            true => {
                let nonscaling_str =
                    ValkeyString::create_from_slice(std::ptr::null_mut(), "NONSCALING".as_bytes());
                vec![nonscaling_str]
            }
            false => {
                let expansion_str =
                    ValkeyString::create_from_slice(std::ptr::null_mut(), "EXPANSION".as_bytes());
                let expansion_val = ValkeyString::create_from_slice(
                    std::ptr::null_mut(),
                    args.expansion.to_string().as_bytes(),
                );
                vec![expansion_str, expansion_val]
            }
        };
        for arg in &expansion_args {
            cmd.push(arg);
        }
        // Add items if any exist.
        let items_str = ValkeyString::create_from_slice(std::ptr::null_mut(), "ITEMS".as_bytes());
        if !args.items.is_empty() {
            cmd.push(&items_str);
        }
        for item in args.items {
            cmd.push(item);
        }
        ctx.replicate("BF.INSERT", cmd.as_slice());
    } else if add_operation {
        ctx.replicate_verbatim();
    }
    if add_operation {
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::ADD_EVENT, key_name);
    }
    if reserve_operation {
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::RESERVE_EVENT, key_name);
    }
}

/// Function that implements logic to handle the BF.ADD and BF.MADD commands.
pub fn bloom_filter_add_value(
    ctx: &Context,
    input_args: &[ValkeyString],
    multi: bool,
) -> ValkeyResult {
    let argc = input_args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomObject>(&BLOOM_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };
    // Skip bloom filter size validation on replicated cmds.
    let validate_size_limit = !ctx.get_flags().contains(ContextFlags::REPLICATED);
    let mut add_succeeded = false;
    match value {
        Some(bloom) => {
            let response = handle_bloom_add(
                input_args,
                argc,
                curr_cmd_idx,
                bloom,
                multi,
                &mut add_succeeded,
                validate_size_limit,
            );
            let replicate_args = ReplicateArgs {
                capacity: bloom.capacity(),
                expansion: bloom.expansion(),
                fp_rate: bloom.fp_rate(),
                tightening_ratio: bloom.tightening_ratio(),
                seed: bloom.seed(),
                items: &input_args[curr_cmd_idx..],
            };
            replicate_and_notify_events(ctx, filter_name, add_succeeded, false, replicate_args);
            response
        }
        None => {
            // Instantiate empty bloom filter.
            let fp_rate = *configs::BLOOM_FP_RATE_F64
                .lock()
                .expect("Unable to get a lock on fp_rate static");
            let tightening_ratio = *configs::BLOOM_TIGHTENING_F64
                .lock()
                .expect("Unable to get a lock on tightening ratio static");
            let capacity = configs::BLOOM_CAPACITY.load(Ordering::Relaxed);
            let expansion = configs::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
            let use_random_seed = configs::BLOOM_USE_RANDOM_SEED.load(Ordering::Relaxed);
            let seed = match use_random_seed {
                true => (None, true),
                false => (Some(configs::FIXED_SEED), false),
            };
            let mut bloom = match BloomObject::new_reserved(
                fp_rate,
                tightening_ratio,
                capacity,
                expansion,
                seed,
                validate_size_limit,
            ) {
                Ok(bf) => bf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };
            let replicate_args = ReplicateArgs {
                capacity: bloom.capacity(),
                expansion: bloom.expansion(),
                fp_rate: bloom.fp_rate(),
                tightening_ratio: bloom.tightening_ratio(),
                seed: bloom.seed(),
                items: &input_args[curr_cmd_idx..],
            };
            let response = handle_bloom_add(
                input_args,
                argc,
                curr_cmd_idx,
                &mut bloom,
                multi,
                &mut add_succeeded,
                validate_size_limit,
            );
            match filter_key.set_value(&BLOOM_TYPE, bloom) {
                Ok(()) => {
                    replicate_and_notify_events(
                        ctx,
                        filter_name,
                        add_succeeded,
                        true,
                        replicate_args,
                    );
                    response
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}

/// Helper function used to check whether an item (or multiple items) exists on a bloom object.
fn handle_item_exists(value: Option<&BloomObject>, item: &[u8]) -> ValkeyValue {
    if let Some(val) = value {
        if val.item_exists(item) {
            return ValkeyValue::Integer(1);
        }
        // Item has not been added to the filter.
        return ValkeyValue::Integer(0);
    };
    // Key does not exist.
    ValkeyValue::Integer(0)
}

/// Function that implements logic to handle the BF.EXISTS and BF.MEXISTS commands.
pub fn bloom_filter_exists(
    ctx: &Context,
    input_args: &[ValkeyString],
    multi: bool,
) -> ValkeyResult {
    let argc = input_args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the value to be checked whether it exists in the filter
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomObject>(&BLOOM_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };
    if !multi {
        let item = input_args[curr_cmd_idx].as_slice();
        return Ok(handle_item_exists(value, item));
    }
    let mut result = Vec::new();
    while curr_cmd_idx < argc {
        let item = input_args[curr_cmd_idx].as_slice();
        result.push(handle_item_exists(value, item));
        curr_cmd_idx += 1;
    }
    Ok(ValkeyValue::Array(result))
}

/// Function that implements logic to handle the BF.CARD command.
pub fn bloom_filter_card(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if argc != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomObject>(&BLOOM_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };
    match value {
        Some(val) => Ok(ValkeyValue::Integer(val.cardinality())),
        None => Ok(ValkeyValue::Integer(0)),
    }
}

/// Function that implements logic to handle the BF.RESERVE command.
pub fn bloom_filter_reserve(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if !(4..=6).contains(&argc) {
        return Err(ValkeyError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the error rate
    let fp_rate = match input_args[curr_cmd_idx].to_string_lossy().parse::<f64>() {
        Ok(num) if num > BLOOM_FP_RATE_MIN && num < BLOOM_FP_RATE_MAX => num,
        Ok(num) if !(num > BLOOM_FP_RATE_MIN && num < BLOOM_FP_RATE_MAX) => {
            return Err(ValkeyError::Str(utils::ERROR_RATE_RANGE));
        }
        _ => {
            return Err(ValkeyError::Str(utils::BAD_ERROR_RATE));
        }
    };
    curr_cmd_idx += 1;
    // Parse the capacity
    let capacity = match input_args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
        Ok(num) if (BLOOM_CAPACITY_MIN..=BLOOM_CAPACITY_MAX).contains(&num) => num,
        Ok(0) => {
            return Err(ValkeyError::Str(utils::CAPACITY_LARGER_THAN_0));
        }
        _ => {
            return Err(ValkeyError::Str(utils::BAD_CAPACITY));
        }
    };
    curr_cmd_idx += 1;
    let mut expansion = configs::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
    if argc > 4 {
        match input_args[curr_cmd_idx]
            .to_string_lossy()
            .to_uppercase()
            .as_str()
        {
            "NONSCALING" if argc == 5 => {
                expansion = 0;
            }
            "EXPANSION" if argc == 6 => {
                curr_cmd_idx += 1;
                expansion = match input_args[curr_cmd_idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if (BLOOM_EXPANSION_MIN..=BLOOM_EXPANSION_MAX).contains(&num) => num,
                    _ => {
                        return Err(ValkeyError::Str(utils::BAD_EXPANSION));
                    }
                };
            }
            _ => {
                return Err(ValkeyError::Str(utils::ERROR));
            }
        }
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomObject>(&BLOOM_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };
    match value {
        Some(_) => Err(ValkeyError::Str(utils::ITEM_EXISTS)),
        None => {
            let use_random_seed = configs::BLOOM_USE_RANDOM_SEED.load(Ordering::Relaxed);
            let seed = match use_random_seed {
                true => (None, true),
                false => (Some(configs::FIXED_SEED), false),
            };
            // Skip bloom filter size validation on replicated cmds.
            let validate_size_limit = !ctx.get_flags().contains(ContextFlags::REPLICATED);
            let tightening_ratio = *configs::BLOOM_TIGHTENING_F64
                .lock()
                .expect("Unable to get a lock on tightening ratio static");
            let bloom = match BloomObject::new_reserved(
                fp_rate,
                tightening_ratio,
                capacity,
                expansion,
                seed,
                validate_size_limit,
            ) {
                Ok(bf) => bf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };
            let replicate_args = ReplicateArgs {
                capacity: bloom.capacity(),
                expansion: bloom.expansion(),
                fp_rate: bloom.fp_rate(),
                tightening_ratio: bloom.tightening_ratio(),
                seed: bloom.seed(),
                items: &[],
            };
            match filter_key.set_value(&BLOOM_TYPE, bloom) {
                Ok(()) => {
                    replicate_and_notify_events(ctx, filter_name, false, true, replicate_args);
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}

/// Function that implements logic to handle the BF.INSERT command.
pub fn bloom_filter_insert(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    // At the very least, we need: BF.INSERT <key>
    if argc < 2 {
        return Err(ValkeyError::WrongArity);
    }
    let mut idx = 1;
    // Parse the filter name
    let filter_name = &input_args[idx];
    idx += 1;
    let mut fp_rate = *configs::BLOOM_FP_RATE_F64
        .lock()
        .expect("Unable to get a lock on fp_rate static");
    let mut tightening_ratio = *configs::BLOOM_TIGHTENING_F64
        .lock()
        .expect("Unable to get a lock on tightening ratio static");
    let mut capacity = configs::BLOOM_CAPACITY.load(Ordering::Relaxed);
    let mut expansion = configs::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
    let use_random_seed = configs::BLOOM_USE_RANDOM_SEED.load(Ordering::Relaxed);
    let mut seed = match use_random_seed {
        true => (None, true),
        false => (Some(configs::FIXED_SEED), false),
    };
    let mut nocreate = false;
    let mut items_provided = false;
    while idx < argc {
        match input_args[idx].to_string_lossy().to_uppercase().as_str() {
            "ERROR" => {
                if idx >= (argc - 1) {
                    return Err(ValkeyError::WrongArity);
                }
                idx += 1;
                fp_rate = match input_args[idx].to_string_lossy().parse::<f64>() {
                    Ok(num) if num > BLOOM_FP_RATE_MIN && num < BLOOM_FP_RATE_MAX => num,
                    Ok(num) if !(num > BLOOM_FP_RATE_MIN && num < BLOOM_FP_RATE_MAX) => {
                        return Err(ValkeyError::Str(utils::ERROR_RATE_RANGE));
                    }
                    _ => {
                        return Err(ValkeyError::Str(utils::BAD_ERROR_RATE));
                    }
                };
            }
            "TIGHTENING" => {
                // Note: This argument is only supported on replicated commands since primary nodes replicate bloom objects
                // deterministically using every global bloom config/property.
                if idx >= (argc - 1) {
                    return Err(ValkeyError::WrongArity);
                }
                idx += 1;
                tightening_ratio = match input_args[idx].to_string_lossy().parse::<f64>() {
                    Ok(num)
                        if num > BLOOM_TIGHTENING_RATIO_MIN && num < BLOOM_TIGHTENING_RATIO_MAX =>
                    {
                        num
                    }
                    Ok(num)
                        if !(num > BLOOM_TIGHTENING_RATIO_MIN
                            && num < BLOOM_TIGHTENING_RATIO_MAX) =>
                    {
                        return Err(ValkeyError::Str(utils::TIGHTENING_RATIO_RANGE));
                    }
                    _ => {
                        return Err(ValkeyError::Str(utils::BAD_TIGHTENING_RATIO));
                    }
                };
            }
            "CAPACITY" => {
                if idx >= (argc - 1) {
                    return Err(ValkeyError::WrongArity);
                }
                idx += 1;
                capacity = match input_args[idx].to_string_lossy().parse::<i64>() {
                    Ok(num) if (BLOOM_CAPACITY_MIN..=BLOOM_CAPACITY_MAX).contains(&num) => num,
                    Ok(0) => {
                        return Err(ValkeyError::Str(utils::CAPACITY_LARGER_THAN_0));
                    }
                    _ => {
                        return Err(ValkeyError::Str(utils::BAD_CAPACITY));
                    }
                };
            }
            "SEED" => {
                // Note: This argument is only supported on replicated commands since primary nodes replicate bloom objects
                // deterministically using every global bloom config/property.
                if idx >= (argc - 1) {
                    return Err(ValkeyError::WrongArity);
                }
                idx += 1;
                // The BloomObject implementation uses a 32-byte (u8) array as the seed.
                let seed_result: Result<[u8; 32], _> = input_args[idx].as_slice().try_into();
                let Ok(seed_raw) = seed_result else {
                    return Err(ValkeyError::Str(utils::INVALID_SEED));
                };
                let is_seed_random = seed_raw != configs::FIXED_SEED;
                seed = (Some(seed_raw), is_seed_random);
            }
            "NOCREATE" => {
                nocreate = true;
            }
            "NONSCALING" => {
                expansion = 0;
            }
            "EXPANSION" => {
                if idx >= (argc - 1) {
                    return Err(ValkeyError::WrongArity);
                }
                idx += 1;
                expansion = match input_args[idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if (BLOOM_EXPANSION_MIN..=BLOOM_EXPANSION_MAX).contains(&num) => num,
                    _ => {
                        return Err(ValkeyError::Str(utils::BAD_EXPANSION));
                    }
                };
            }
            "ITEMS" => {
                idx += 1;
                items_provided = true;
                break;
            }
            _ => {
                return Err(ValkeyError::Str(utils::UNKNOWN_ARGUMENT));
            }
        }
        idx += 1;
    }
    if idx == argc && items_provided {
        // When the `ITEMS` argument is provided, we expect additional item arg/s to be provided.
        return Err(ValkeyError::WrongArity);
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomObject>(&BLOOM_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };
    // Skip bloom filter size validation on replicated cmds.
    let validate_size_limit = !ctx.get_flags().contains(ContextFlags::REPLICATED);
    let mut add_succeeded = false;
    match value {
        Some(bloom) => {
            let response = handle_bloom_add(
                input_args,
                argc,
                idx,
                bloom,
                true,
                &mut add_succeeded,
                validate_size_limit,
            );
            let replicate_args = ReplicateArgs {
                capacity: bloom.capacity(),
                expansion: bloom.expansion(),
                fp_rate: bloom.fp_rate(),
                tightening_ratio: bloom.tightening_ratio(),
                seed: bloom.seed(),
                items: &input_args[idx..],
            };
            replicate_and_notify_events(ctx, filter_name, add_succeeded, false, replicate_args);
            response
        }
        None => {
            if nocreate {
                return Err(ValkeyError::Str(utils::NOT_FOUND));
            }
            let mut bloom = match BloomObject::new_reserved(
                fp_rate,
                tightening_ratio,
                capacity,
                expansion,
                seed,
                validate_size_limit,
            ) {
                Ok(bf) => bf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };
            let replicate_args = ReplicateArgs {
                capacity: bloom.capacity(),
                expansion: bloom.expansion(),
                fp_rate: bloom.fp_rate(),
                tightening_ratio: bloom.tightening_ratio(),
                seed: bloom.seed(),
                items: &input_args[idx..],
            };
            let response = handle_bloom_add(
                input_args,
                argc,
                idx,
                &mut bloom,
                true,
                &mut add_succeeded,
                validate_size_limit,
            );
            match filter_key.set_value(&BLOOM_TYPE, bloom) {
                Ok(()) => {
                    replicate_and_notify_events(
                        ctx,
                        filter_name,
                        add_succeeded,
                        true,
                        replicate_args,
                    );
                    response
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}

/// Function that implements logic to handle the BF.INFO command.
pub fn bloom_filter_info(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if !(2..=3).contains(&argc) {
        return Err(ValkeyError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomObject>(&BLOOM_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };
    match value {
        Some(val) if argc == 3 => {
            match input_args[curr_cmd_idx]
                .to_string_lossy()
                .to_uppercase()
                .as_str()
            {
                "CAPACITY" => Ok(ValkeyValue::Integer(val.capacity())),
                "SIZE" => Ok(ValkeyValue::Integer(val.memory_usage() as i64)),
                "FILTERS" => Ok(ValkeyValue::Integer(val.num_filters() as i64)),
                "ITEMS" => Ok(ValkeyValue::Integer(val.cardinality())),
                "EXPANSION" => {
                    if val.expansion() == 0 {
                        return Ok(ValkeyValue::Null);
                    }
                    Ok(ValkeyValue::Integer(val.expansion() as i64))
                }
                _ => Err(ValkeyError::Str(utils::INVALID_INFO_VALUE)),
            }
        }
        Some(val) if argc == 2 => {
            let mut result = vec![
                ValkeyValue::SimpleStringStatic("Capacity"),
                ValkeyValue::Integer(val.capacity()),
                ValkeyValue::SimpleStringStatic("Size"),
                ValkeyValue::Integer(val.memory_usage() as i64),
                ValkeyValue::SimpleStringStatic("Number of filters"),
                ValkeyValue::Integer(val.num_filters() as i64),
                ValkeyValue::SimpleStringStatic("Number of items inserted"),
                ValkeyValue::Integer(val.cardinality()),
                ValkeyValue::SimpleStringStatic("Expansion rate"),
            ];
            if val.expansion() == 0 {
                result.push(ValkeyValue::Null);
            } else {
                result.push(ValkeyValue::Integer(val.expansion() as i64));
            }
            Ok(ValkeyValue::Array(result))
        }
        _ => Err(ValkeyError::Str(utils::NOT_FOUND)),
    }
}

/// Function that implements logic to handle the BF.LOAD command.
pub fn bloom_filter_load(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if argc != 3 {
        return Err(ValkeyError::WrongArity);
    }
    let mut idx = 1;
    let filter_name = &input_args[idx];
    idx += 1;
    let value = &input_args[idx];
    // find filter
    let filter_key = ctx.open_key_writable(filter_name);

    let filter = match filter_key.get_value::<BloomObject>(&BLOOM_TYPE) {
        Ok(v) => v,
        Err(_) => {
            // error
            return Err(ValkeyError::WrongType);
        }
    };
    match filter {
        Some(_) => {
            // if bloom exists, return exists error.
            Err(ValkeyError::Str(utils::KEY_EXISTS))
        }
        None => {
            // if filter not exists, create it.
            let hex = value.to_vec();
            let validate_size_limit = !ctx.get_flags().contains(ContextFlags::REPLICATED);
            let bloom = match BloomObject::decode_object(&hex, validate_size_limit) {
                Ok(v) => v,
                Err(err) => {
                    return Err(ValkeyError::Str(err.as_str()));
                }
            };
            let replicate_args = ReplicateArgs {
                capacity: bloom.capacity(),
                expansion: bloom.expansion(),
                fp_rate: bloom.fp_rate(),
                tightening_ratio: bloom.tightening_ratio(),
                seed: bloom.seed(),
                items: &input_args[idx..],
            };
            match filter_key.set_value(&BLOOM_TYPE, bloom) {
                Ok(_) => {
                    replicate_and_notify_events(ctx, filter_name, false, true, replicate_args);
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}
