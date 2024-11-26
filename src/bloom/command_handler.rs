use crate::bloom::data_type::BLOOM_FILTER_TYPE;
use crate::bloom::utils;
use crate::bloom::utils::BloomFilterType;
use crate::configs;
use crate::configs::{
    BLOOM_CAPACITY_MAX, BLOOM_CAPACITY_MIN, BLOOM_EXPANSION_MAX, BLOOM_EXPANSION_MIN,
    BLOOM_FP_RATE_MAX, BLOOM_FP_RATE_MIN,
};
use std::sync::atomic::Ordering;
use valkey_module::ContextFlags;
use valkey_module::NotifyEvent;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, VALKEY_OK};

fn handle_bloom_add(
    args: &[ValkeyString],
    argc: usize,
    item_idx: usize,
    bf: &mut BloomFilterType,
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

fn replicate_and_notify_events(
    ctx: &Context,
    key_name: &ValkeyString,
    add_operation: bool,
    reserve_operation: bool,
) {
    if add_operation || reserve_operation {
        ctx.replicate_verbatim();
    }
    if add_operation {
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::ADD_EVENT, key_name);
    }
    if reserve_operation {
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::RESERVE_EVENT, key_name);
    }
}

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
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(utils::ERROR));
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
            replicate_and_notify_events(ctx, filter_name, add_succeeded, false);
            response
        }
        None => {
            // Instantiate empty bloom filter.
            let fp_rate = configs::BLOOM_FP_RATE_DEFAULT;
            let capacity = configs::BLOOM_CAPACITY.load(Ordering::Relaxed) as u32;
            let expansion = configs::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
            let use_random_seed = configs::BLOOM_USE_RANDOM_SEED.load(Ordering::Relaxed);
            let mut bloom = match BloomFilterType::new_reserved(
                fp_rate,
                capacity,
                expansion,
                use_random_seed,
                validate_size_limit,
            ) {
                Ok(bf) => bf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
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
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bloom) {
                Ok(()) => {
                    replicate_and_notify_events(ctx, filter_name, add_succeeded, true);
                    response
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}

fn handle_item_exists(value: Option<&BloomFilterType>, item: &[u8]) -> ValkeyValue {
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
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(utils::ERROR));
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

pub fn bloom_filter_card(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if argc != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(utils::ERROR));
        }
    };
    match value {
        Some(val) => Ok(ValkeyValue::Integer(val.cardinality())),
        None => Ok(ValkeyValue::Integer(0)),
    }
}

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
    let capacity = match input_args[curr_cmd_idx].to_string_lossy().parse::<u32>() {
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
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(utils::ERROR));
        }
    };
    match value {
        Some(_) => Err(ValkeyError::Str(utils::ITEM_EXISTS)),
        None => {
            let use_random_seed = configs::BLOOM_USE_RANDOM_SEED.load(Ordering::Relaxed);
            // Skip bloom filter size validation on replicated cmds.
            let validate_size_limit = !ctx.get_flags().contains(ContextFlags::REPLICATED);
            let bloom = match BloomFilterType::new_reserved(
                fp_rate,
                capacity,
                expansion,
                use_random_seed,
                validate_size_limit,
            ) {
                Ok(bf) => bf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bloom) {
                Ok(()) => {
                    replicate_and_notify_events(ctx, filter_name, false, true);
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}

pub fn bloom_filter_insert(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    // At the very least, we need: BF.INSERT <key> ITEMS <item>
    if argc < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let mut idx = 1;
    // Parse the filter name
    let filter_name = &input_args[idx];
    idx += 1;
    let mut fp_rate = configs::BLOOM_FP_RATE_DEFAULT;
    let mut capacity = configs::BLOOM_CAPACITY.load(Ordering::Relaxed) as u32;
    let mut expansion = configs::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
    let mut nocreate = false;
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
            "CAPACITY" => {
                if idx >= (argc - 1) {
                    return Err(ValkeyError::WrongArity);
                }
                idx += 1;
                capacity = match input_args[idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if (BLOOM_CAPACITY_MIN..=BLOOM_CAPACITY_MAX).contains(&num) => num,
                    Ok(0) => {
                        return Err(ValkeyError::Str(utils::CAPACITY_LARGER_THAN_0));
                    }
                    _ => {
                        return Err(ValkeyError::Str(utils::BAD_CAPACITY));
                    }
                };
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
                break;
            }
            _ => {
                return Err(ValkeyError::Str(utils::UNKNOWN_ARGUMENT));
            }
        }
        idx += 1;
    }
    if idx == argc {
        // No ITEMS argument from the insert command
        return Err(ValkeyError::WrongArity);
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(utils::ERROR));
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
            replicate_and_notify_events(ctx, filter_name, add_succeeded, false);
            response
        }
        None => {
            if nocreate {
                return Err(ValkeyError::Str(utils::NOT_FOUND));
            }
            let use_random_seed = configs::BLOOM_USE_RANDOM_SEED.load(Ordering::Relaxed);
            let mut bloom = match BloomFilterType::new_reserved(
                fp_rate,
                capacity,
                expansion,
                use_random_seed,
                validate_size_limit,
            ) {
                Ok(bf) => bf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
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
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bloom) {
                Ok(()) => {
                    replicate_and_notify_events(ctx, filter_name, add_succeeded, true);
                    response
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}

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
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(utils::ERROR));
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
                "FILTERS" => Ok(ValkeyValue::Integer(val.filters.len() as i64)),
                "ITEMS" => Ok(ValkeyValue::Integer(val.cardinality())),
                "EXPANSION" => {
                    if val.expansion == 0 {
                        return Ok(ValkeyValue::Null);
                    }
                    Ok(ValkeyValue::Integer(val.expansion as i64))
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
                ValkeyValue::Integer(val.filters.len() as i64),
                ValkeyValue::SimpleStringStatic("Number of items inserted"),
                ValkeyValue::Integer(val.cardinality()),
                ValkeyValue::SimpleStringStatic("Expansion rate"),
            ];
            if val.expansion == 0 {
                result.push(ValkeyValue::Null);
            } else {
                result.push(ValkeyValue::Integer(val.expansion as i64));
            }
            Ok(ValkeyValue::Array(result))
        }
        _ => Err(ValkeyError::Str(utils::NOT_FOUND)),
    }
}

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

    let filter = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            // error
            return Err(ValkeyError::Str(utils::ERROR));
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
            let bf = match BloomFilterType::decode_bloom_filter(&hex, validate_size_limit) {
                Ok(v) => v,
                Err(err) => {
                    return Err(ValkeyError::Str(err.as_str()));
                }
            };
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bf) {
                Ok(_) => {
                    replicate_and_notify_events(ctx, filter_name, false, true);
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}
