use crate::bloom_config;
use crate::commands::bloom_data_type::BLOOM_FILTER_TYPE2;
use bloomfilter::Bloom;
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue};
use std::sync::atomic::Ordering;
use redis_module::key::RedisKeyWritable;
use crate::commands::bloom_data_type::BloomFilterType2;
use crate::commands::bloom_data_type;

pub fn bloom_filter_add_value(ctx: &Context, input_args: &Vec<RedisString>, multi: bool) -> RedisResult {
    let argc = input_args.len();
    if (!multi && argc != 3) || argc < 3  {
        return Ok(RedisValue::Array(Vec::new()));
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
            return Ok(RedisValue::Array(Vec::new()));
        }
    };
    if !multi {
        let item = &input_args[curr_cmd_idx];
        return Ok(bloom_filter_add_item(&filter_key, &mut value, &item));
    }
    let mut result = Vec::new();
    while curr_cmd_idx < argc {
        let item = &input_args[curr_cmd_idx];
        result.push(bloom_filter_add_item(&filter_key, &mut value, &item));
        curr_cmd_idx += 1;
    }
    return Ok(RedisValue::Array(result));
}

fn bloom_filter_add_item(filter_key: &RedisKeyWritable, value: &mut Option<&mut BloomFilterType2>, item: &RedisString) -> RedisValue {
    match value {
        Some(val) => {
            // Check if item exists already.
            if val.bloom.check(&item) {
                return RedisValue::Integer(0);
            }
            // Add item.
            val.bloom.set(&item);
            val.num_items += 1;
            RedisValue::Integer(1)
        }
        None => {
            // Instantiate empty bloom filter.
            // TODO: Define false positive rate as a config.
            let fp_rate = 0.001;
            let capacity = bloom_config::BLOOM_MAX_ITEM_COUNT.load(Ordering::Relaxed) as usize;
            let mut bloom = Bloom::new_for_fp_rate(
                capacity,
                fp_rate,
            );
            // Add item.
            bloom.set(item.as_slice());

            let value = BloomFilterType2 {
                bloom: bloom,
                num_items: 1,
                capacity,
                expansion: bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as usize,
            };
            match filter_key.set_value(&BLOOM_FILTER_TYPE2, value) {
                Ok(_v) => {
                    RedisValue::Integer(1)
                }
                Err(_) => RedisValue::Array(Vec::new()),
            }
        }
    }
}

pub fn bloom_filter_exists(ctx: &Context, input_args: &Vec<RedisString>, multi: bool) -> RedisResult {
    let argc = input_args.len();
    if (!multi && argc != 3) || argc < 3  {
        return Ok(RedisValue::Array(Vec::new()));
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
            return Ok(RedisValue::Array(Vec::new()));
        }
    };
    if !multi {
        let item = &input_args[curr_cmd_idx];
        return Ok(bloom_filter_item_exists(&value, &item));
    }
    let mut result = Vec::new();
    while curr_cmd_idx < argc {
        let item = &input_args[curr_cmd_idx];
        result.push(bloom_filter_item_exists(&value, &item));
        curr_cmd_idx += 1;
    }
    return Ok(RedisValue::Array(result));
}

fn bloom_filter_item_exists(value: &Option<&BloomFilterType2>, item: &RedisString) -> RedisValue {
    match value {
        Some(val) => {
            if val.bloom.check(&item) {
                return RedisValue::Integer(1);
            }
            // Item has not been added to the filter.
            RedisValue::Integer(0)
        }
        // Key does not exist.
        None => RedisValue::Integer(0),
    }
}

pub fn bloom_filter_card(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc != 2 {
        return Ok(RedisValue::Array(Vec::new()));
    }
    let curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_e) => {
            return Ok(RedisValue::Array(Vec::new()));
        }
    };
    match value {
        Some(val) => Ok(RedisValue::Integer(val.num_items.try_into().unwrap())),
        None => Ok(RedisValue::Integer(0)),
    }
}

pub fn bloom_filter_reserve(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc < 4 || argc > 6 {
        return Ok(RedisValue::Array(Vec::new()));
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the error_rate
    let error_rate = match input_args[curr_cmd_idx].to_string_lossy().parse::<f64>() {
        Ok(num) if num >= 0.0 && num < 1.0  => num,
        _ => {
            return Ok(RedisValue::Array(Vec::new()));
        }
    };
    curr_cmd_idx += 1;
    // Parse the capacity
    let capacity = match input_args[curr_cmd_idx].to_string_lossy().parse::<usize>() {
        Ok(num) => num,
        _ => {
            return Ok(RedisValue::Array(Vec::new()));
        }
    };
    curr_cmd_idx += 1;
    let mut expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as usize;
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
                return Ok(RedisValue::Array(Vec::new()));
            }
        }
    }
    if parse_expansion {
        expansion = match input_args[curr_cmd_idx].to_string_lossy().parse::<usize>() {
            Ok(num) => num,
            _ => {
                return Ok(RedisValue::Array(Vec::new()));
            }
        };
    } else if noscaling {
        expansion = 1;
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_e) => {
            return Ok(RedisValue::Array(Vec::new()));
        }
    };
    match value {
        Some(_) => {
            Err(RedisError::Str("ERR item exists"))
        }
        None => {
            let bloom = Bloom::new_for_fp_rate(
                capacity,
                error_rate,
            );
            let value = BloomFilterType2 {
                bloom,
                num_items: 0,
                capacity,
                expansion,
            };
            match filter_key.set_value(&BLOOM_FILTER_TYPE2, value) {
                Ok(_v) => {
                    Ok(RedisValue::SimpleStringStatic("OK"))
                }
                Err(_) => Ok(RedisValue::Array(Vec::new())),
            }
        }
    }
}

pub fn bloom_filter_info(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc < 2 || argc > 3 {
        return Ok(RedisValue::Array(Vec::new()));
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_e) => {
            return Ok(RedisValue::Array(Vec::new()));
        }
    };
    match value {
        Some(val) if argc == 3 => {
            match input_args[curr_cmd_idx].to_string_lossy().to_uppercase().as_str() {
                "CAPACITY" => {
                    return Ok(RedisValue::Integer(val.capacity as i64));
                }
                "SIZE" => {
                    // TODO: `bitmap()` is a slow operation. Find an alternative to identify the memory usage.
                    return Ok(RedisValue::Integer(bloom_data_type::bloom_get_filter_memory_usage(val.bloom.bitmap().len()) as i64))
                }
                "FILTERS" => {
                    return Ok(RedisValue::Integer(1));
                }
                "ITEMS" => {
                    return Ok(RedisValue::Integer(val.num_items as i64));
                }
                "EXPANSION" => {
                    return Ok(RedisValue::Integer(val.expansion as i64));
                }
                _ => {
                    return Ok(RedisValue::Array(Vec::new()));
                }
            }
        },
        Some(val) if argc == 2 => {
            let mut result = Vec::new();
            result.push(RedisValue::SimpleStringStatic("Capacity"));
            result.push(RedisValue::Integer(val.capacity as i64));
            result.push(RedisValue::SimpleStringStatic("Size"));
            // TODO: `bitmap()` is a slow operation. Find an alternative to identify the memory usage.
            result.push(RedisValue::Integer(bloom_data_type::bloom_get_filter_memory_usage(val.bloom.bitmap().len()) as i64));
            result.push(RedisValue::SimpleStringStatic("Number of filters"));
            result.push(RedisValue::Integer(1));
            result.push(RedisValue::SimpleStringStatic("Number of items inserted"));
            result.push(RedisValue::Integer(val.num_items as i64));
            result.push(RedisValue::SimpleStringStatic("Expansion rate"));
            result.push(RedisValue::Integer(val.expansion as i64));
            return Ok(RedisValue::Array(result));
        }
        _ => Ok(RedisValue::Array(Vec::new())),
    }
}