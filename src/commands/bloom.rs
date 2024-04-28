use crate::bloom_config;
use crate::commands::bloom_data_type::BLOOM_FILTER_TYPE2;
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue};
use std::sync::atomic::Ordering;
use redis_module::key::RedisKeyWritable;
use crate::commands::bloom_util::BloomFilterType2;

// TODO: Replace string literals in error messages with static 

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
    let mut value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_) => {
            return Err(RedisError::Str("ERROR"));
        }
    };
    if !multi {
        let item = &input_args[curr_cmd_idx];
        return Ok(bloom_filter_add_item(&filter_key, &mut value, item));
    }
    let mut result = Vec::new();
    while curr_cmd_idx < argc {
        let item = &input_args[curr_cmd_idx];
        result.push(bloom_filter_add_item(&filter_key, &mut value, item));
        curr_cmd_idx += 1;
    }
    return Ok(RedisValue::Array(result));
}

fn bloom_filter_add_item(filter_key: &RedisKeyWritable, value: &mut Option<&mut BloomFilterType2>, item: &[u8]) -> RedisValue {
    match value {
        Some(val) => {
            // Add to an existing filter.
            RedisValue::Integer(val.add_item(item))
        }
        None => {
            // Instantiate empty bloom filter.
            // TODO: Define the default false positive rate as a config.
            let fp_rate = 0.001;
            let capacity = bloom_config::BLOOM_MAX_ITEM_COUNT.load(Ordering::Relaxed) as usize;
            let expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed);
            let value = BloomFilterType2::new_with_item(fp_rate, capacity, expansion, item);
            match filter_key.set_value(&BLOOM_FILTER_TYPE2, value) {
                Ok(_v) => {
                    RedisValue::Integer(1)
                }
                Err(_) => RedisValue::Integer(0),
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
    let value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_) => {
            return Err(RedisError::Str("ERROR"));
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

fn bloom_filter_item_exists(value: Option<&BloomFilterType2>, item: &[u8]) -> RedisValue {
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
    let value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_) => {
            return Err(RedisError::Str("ERROR"));
        }
    };
    match value {
        Some(val) => Ok(RedisValue::Integer(val.cardinality() as i64)),
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
    // Parse the error_rate
    let error_rate = match input_args[curr_cmd_idx].to_string_lossy().parse::<f64>() {
        Ok(num) if num >= 0.0 && num < 1.0  => num,
        _ => {
            return Err(RedisError::Str("(0 < error rate range < 1)"));
        }
    };
    curr_cmd_idx += 1;
    // Parse the capacity
    let capacity = match input_args[curr_cmd_idx].to_string_lossy().parse::<usize>() {
        Ok(num) => num,
        _ => {
            return Err(RedisError::Str("(capacity should be larger than 0)"));
        }
    };
    curr_cmd_idx += 1;
    let mut expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed);
    let mut noscaling = false; // DEFAULT
    let mut parse_expansion = false; // DEFAULT
    if argc > 4 {
        match input_args[curr_cmd_idx].to_string_lossy().to_uppercase().as_str() {
            "NONSCALING" if argc == 5 => {
                noscaling = true;
            }
            "EXPANSION" if argc == 6 => {
                curr_cmd_idx += 1;
                parse_expansion = true;
            }
            _ => {
                return Err(RedisError::Str("ERROR"));
            }
        }
    }
    if parse_expansion {
        expansion = match input_args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
            Ok(num) => num,
            _ => {
                return Err(RedisError::Str("bad expansion"));
            }
        };
    } else if noscaling {
        expansion = -1;
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_) => {
            return Err(RedisError::Str("ERROR"));
        }
    };
    match value {
        Some(_) => {
            Err(RedisError::Str("item exists"))
        }
        None => {
            let bloom = BloomFilterType2::new_reserved(error_rate, capacity, expansion);
            match filter_key.set_value(&BLOOM_FILTER_TYPE2, bloom) {
                Ok(_v) => {
                    Ok(RedisValue::SimpleStringStatic("OK"))
                }
                Err(_) => Err(RedisError::Str("ERROR")),
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
    let value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_) => {
            return Err(RedisError::Str("ERROR"));
        }
    };
    match value {
        Some(val) if argc == 3 => {
            match input_args[curr_cmd_idx].to_string_lossy().to_uppercase().as_str() {
                "CAPACITY" => {
                    return Ok(RedisValue::Integer(val.capacity() as i64));
                }
                "SIZE" => {
                    return Ok(RedisValue::Integer(val.get_memory_usage() as i64))
                }
                "FILTERS" => {
                    return Ok(RedisValue::Integer(val.filters.len() as i64));
                }
                "ITEMS" => {
                    return Ok(RedisValue::Integer(val.cardinality() as i64));
                }
                "EXPANSION" => {
                    return Ok(RedisValue::Integer(val.expansion as i64));
                }
                _ => {
                    return Err(RedisError::Str("Invalid information value"));
                }
            }
        },
        Some(val) if argc == 2 => {
            let mut result = Vec::new();
            result.push(RedisValue::SimpleStringStatic("Capacity"));
            result.push(RedisValue::Integer(val.capacity() as i64));
            result.push(RedisValue::SimpleStringStatic("Size"));
            result.push(RedisValue::Integer(val.get_memory_usage() as i64));
            result.push(RedisValue::SimpleStringStatic("Number of filters"));
            result.push(RedisValue::Integer(val.filters.len() as i64));
            result.push(RedisValue::SimpleStringStatic("Number of items inserted"));
            result.push(RedisValue::Integer(val.cardinality() as i64));
            result.push(RedisValue::SimpleStringStatic("Expansion rate"));
            result.push(RedisValue::Integer(val.expansion as i64));
            return Ok(RedisValue::Array(result));
        }
        _ => Err(RedisError::Str("not found")),
    }
}
