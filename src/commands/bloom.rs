use crate::bloom_config;
use crate::bloom_config::BLOOM_EXPANSION_MAX;
use crate::bloom_config::BLOOM_MAX_ITEM_COUNT_MAX;
use crate::commands::bloom_data_type::BLOOM_FILTER_TYPE;
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue, REDIS_OK};
use std::sync::atomic::Ordering;
use crate::commands::bloom_util::{BloomFilterType, ERROR};

// TODO: Replace string literals in error messages with static
// TODO: Check all int / usize casting.

pub fn bloom_filter_add_value(ctx: &Context, input_args: &Vec<RedisString>, multi: bool) -> RedisResult {
    let argc = input_args.len();
    if (!multi && argc != 3) || argc < 3  {
        return Err(RedisError::WrongArity);
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
            return Err(RedisError::Str(ERROR));
        }
    };
    match value {
        Some(bf) => {
            if !multi {
                let item = &input_args[curr_cmd_idx];
                return Ok(RedisValue::Integer(bf.add_item(item)));
            }
            let mut result = Vec::new();
            for idx in curr_cmd_idx..argc {
                let item = &input_args[idx];
                result.push(RedisValue::Integer(bf.add_item(item)));
            }
            Ok(RedisValue::Array(result))
        }
        None => {
            // Instantiate empty bloom filter.
            // TODO: Define the default false positive rate as a config.
            let fp_rate = 0.001;
            let capacity = bloom_config::BLOOM_MAX_ITEM_COUNT.load(Ordering::Relaxed) as u32;
            let expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
            let mut bf = BloomFilterType::new_reserved(fp_rate, capacity, expansion);
            let result = match multi {
                true => {
                    let mut result = Vec::new();
                    for idx in curr_cmd_idx..argc {
                        let item = &input_args[idx];
                        result.push(RedisValue::Integer(bf.add_item(item)));
                    }
                    Ok(RedisValue::Array(result))
                }
                false => {
                    let item = &input_args[curr_cmd_idx];
                    Ok(RedisValue::Integer(bf.add_item(item)))
                }
            };
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bf) {
                Ok(_) => {
                    result
                }
                Err(_) => Err(RedisError::Str(ERROR)),
            }
        }
    }
}

pub fn bloom_filter_exists(ctx: &Context, input_args: &Vec<RedisString>, multi: bool) -> RedisResult {
    let argc = input_args.len();
    if (!multi && argc != 3) || argc < 3  {
        return Err(RedisError::WrongArity);
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
            return Err(RedisError::Str(ERROR));
        }
    };
    if !multi {
        let item = &input_args[curr_cmd_idx];
        return Ok(bloom_filter_item_exists(value, item));
    }
    let mut result = Vec::new();
    while curr_cmd_idx < argc {
        let item = &input_args[curr_cmd_idx];
        result.push(bloom_filter_item_exists(value, item));
        curr_cmd_idx += 1;
    }
    return Ok(RedisValue::Array(result));
}

fn bloom_filter_item_exists(value: Option<&BloomFilterType>, item: &[u8]) -> RedisValue {
    if let Some(val) = value {
        if val.item_exists(item) {
            return RedisValue::Integer(1);
        }
        // Item has not been added to the filter.
        return RedisValue::Integer(0);
    };
    // Key does not exist.
    RedisValue::Integer(0)
}

pub fn bloom_filter_card(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc != 2 {
        return Err(RedisError::WrongArity);
    }
    let curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match value {
        Some(val) => Ok(RedisValue::Integer(val.cardinality())),
        None => Ok(RedisValue::Integer(0)),
    }
}

pub fn bloom_filter_reserve(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc < 4 || argc > 6 {
        return Err(RedisError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the error rate
    let fp_rate = match input_args[curr_cmd_idx].to_string_lossy().parse::<f32>() {
        Ok(num) if num >= 0.0 && num < 1.0  => num,
        _ => {
            return Err(RedisError::Str("ERR (0 < error rate range < 1)"));
        }
    };
    curr_cmd_idx += 1;
    // Parse the capacity
    let capacity = match input_args[curr_cmd_idx].to_string_lossy().parse::<u32>() {
        Ok(num) if num > 0 && num < BLOOM_MAX_ITEM_COUNT_MAX => num,
        _ => {
            return Err(RedisError::Str("ERR Bad capacity"));
        }
    };
    curr_cmd_idx += 1;
    let mut expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
    if argc > 4 {
        match input_args[curr_cmd_idx].to_string_lossy().to_uppercase().as_str() {
            "NONSCALING" if argc == 5 => {
                expansion = 0;
            }
            "EXPANSION" if argc == 6 => {
                curr_cmd_idx += 1;
                expansion = match input_args[curr_cmd_idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if num > 0 && num <= BLOOM_EXPANSION_MAX => num,
                    _ => {
                        return Err(RedisError::Str("ERR bad expansion"));
                    }
                };
            }
            _ => {
                return Err(RedisError::Str(ERROR));
            }
        }
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match value {
        Some(_) => {
            Err(RedisError::Str("ERR item exists"))
        }
        None => {
            let bloom = BloomFilterType::new_reserved(fp_rate, capacity, expansion);
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bloom) {
                Ok(_v) => {
                    REDIS_OK
                }
                Err(_) => Err(RedisError::Str(ERROR)),
            }
        }
    }
}

pub fn bloom_filter_insert(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    // At the very least, we need: BF.INSERT <key> ITEM <item>
    if argc < 4 {
        return Err(RedisError::WrongArity);
    }
    let mut idx = 1;
    // Parse the filter name
    let filter_name = &input_args[idx];
    idx += 1;
    // TODO: Create and use a config for the default fp_rate.
    let mut fp_rate = 0.001;
    let mut capacity = bloom_config::BLOOM_MAX_ITEM_COUNT.load(Ordering::Relaxed) as u32;
    let mut expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
    let mut nocreate = false;
    while idx < argc {
        match input_args[idx].to_string_lossy().to_uppercase().as_str() {
            "ERROR" if idx < (argc - 1) => {
                idx += 1;
                fp_rate = match input_args[idx].to_string_lossy().parse::<f32>() {
                    Ok(num) if num >= 0.0 && num < 1.0  => num,
                    Ok(num) if num < 0.0 && num >= 1.0 => {
                        return Err(RedisError::Str("ERR (0 < error rate range < 1)"));
                    }
                    _ => {
                        return Err(RedisError::Str("ERR Bad error rate"));
                    }
                };
            }
            "CAPACITY" if idx < (argc - 1) => {
                idx += 1;
                capacity = match input_args[idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if num > 0 && num < BLOOM_MAX_ITEM_COUNT_MAX => num,
                    _ => {
                        return Err(RedisError::Str("ERR Bad capacity"));
                    }
                };
            }
            "NOCREATE" => {
                nocreate = true;
            }
            "NONSCALING" => {
                expansion = 0;
            }
            "EXPANSION" if idx < (argc - 1) => {
                idx += 1;
                expansion = match input_args[idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if num > 0 && num <= BLOOM_EXPANSION_MAX => num,
                    _ => {
                        return Err(RedisError::Str("ERR bad expansion"));
                    }
                };
            }
            "ITEMS" if idx < (argc - 1) => {
                idx += 1;
                break;
            }
            _ => {
                return Err(RedisError::WrongArity);
            }
        }
        idx += 1;
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    let mut result = Vec::new();
    match value {
        Some(bf) => {
            for i in idx..argc {
                let item = &input_args[i];
                result.push(RedisValue::Integer(bf.add_item(item)));
            }
            Ok(RedisValue::Array(result))
        }
        None => {
            if nocreate {
                return Err(RedisError::Str("ERR not found"));
            }
            let mut bf = BloomFilterType::new_reserved(fp_rate, capacity, expansion);
            for i in idx..argc {
                let item = &input_args[i];
                result.push(RedisValue::Integer(bf.add_item(item)));
            }
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bf) {
                Ok(_) => {
                    Ok(RedisValue::Array(result))
                }
                Err(_) => {
                    Err(RedisError::Str(ERROR))
                }
            }
        }
    }
}

pub fn bloom_filter_info(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc < 2 || argc > 3 {
        return Err(RedisError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match value {
        Some(val) if argc == 3 => {
            match input_args[curr_cmd_idx].to_string_lossy().to_uppercase().as_str() {
                "CAPACITY" => {
                    return Ok(RedisValue::Integer(val.capacity()));
                }
                "SIZE" => {
                    return Ok(RedisValue::Integer(val.get_memory_usage() as i64))
                }
                "FILTERS" => {
                    return Ok(RedisValue::Integer(val.filters.len() as i64));
                }
                "ITEMS" => {
                    return Ok(RedisValue::Integer(val.cardinality()));
                }
                "EXPANSION" => {
                    if val.expansion == 0 {
                        return Ok(RedisValue::Integer(-1));
                    }
                    return Ok(RedisValue::Integer(val.expansion as i64));
                }
                _ => {
                    return Err(RedisError::Str("ERR Invalid information value"));
                }
            }
        },
        Some(val) if argc == 2 => {
            let mut result = Vec::new();
            result.push(RedisValue::SimpleStringStatic("Capacity"));
            result.push(RedisValue::Integer(val.capacity()));
            result.push(RedisValue::SimpleStringStatic("Size"));
            result.push(RedisValue::Integer(val.get_memory_usage() as i64));
            result.push(RedisValue::SimpleStringStatic("Number of filters"));
            result.push(RedisValue::Integer(val.filters.len() as i64));
            result.push(RedisValue::SimpleStringStatic("Number of items inserted"));
            result.push(RedisValue::Integer(val.cardinality()));
            result.push(RedisValue::SimpleStringStatic("Expansion rate"));
            if val.expansion == 0 {
                result.push(RedisValue::Integer(-1));
            } else {
                result.push(RedisValue::Integer(val.expansion as i64));
            }
            return Ok(RedisValue::Array(result));
        }
        _ => Err(RedisError::Str("ERR not found")),
    }
}
