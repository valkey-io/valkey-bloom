use crate::bloom_config;
use crate::commands::bloom_data_type::{BloomFilterType, BLOOM_FILTER_TYPE};
use crate::commands::bloom_util::{bloom_create_new_item, bloom_update_filter, ERROR};
use bloomfilter::Bloom;
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue};
use std::sync::atomic::Ordering;

pub fn bloom_filter_add_value(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc != 3 {
        return Err(RedisError::Str(ERROR));
    }
    let mut curr_cmd_idx = 0;
    let _cmd = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the value to be added to the filter
    let item = &input_args[curr_cmd_idx];
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let my_value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_e) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match my_value {
        Some(val) => {
            // Instantiate bloom filter.
            let sip_keys = [
                (val.sip_key_one_a, val.sip_key_one_b),
                (val.sip_key_two_a, val.sip_key_two_b),
            ];
            let old_filter_bitmap_len = val.bitmap.len();
            let mut bloom = Bloom::from_existing(
                &val.bitmap,
                val.number_of_bits,
                val.number_of_hash_functions,
                sip_keys,
            );

            // Check if item exists.
            if bloom.check(&item) {
                return Ok(RedisValue::Integer(0));
            }

            // Add item.
            bloom.set(&item);

            // Update filter.
            bloom_update_filter(
                val,
                old_filter_bitmap_len,
                bloom.bitmap(),
                bloom.number_of_bits(),
                bloom.number_of_hash_functions(),
                bloom.sip_keys(),
            );
            Ok(RedisValue::Integer(1))
        }
        None => {
            // Instantiate empty bloom filter.
            // TODO: Define false positive rate as a config.
            let fp_rate = 0.001;
            let mut bloom = Bloom::new_for_fp_rate(
                bloom_config::BLOOM_MAX_ITEM_SIZE.load(Ordering::Relaxed) as usize,
                fp_rate,
            );

            // Add item.
            bloom.set(&item);

            bloom_create_new_item(
                &filter_key,
                bloom.bitmap(),
                bloom.number_of_bits(),
                bloom.number_of_hash_functions(),
                bloom.sip_keys(),
            )?;
            Ok(RedisValue::Integer(1))
        }
    }
}

pub fn bloom_filter_exists(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc != 3 {
        return Err(RedisError::Str(ERROR));
    }
    let mut curr_cmd_idx = 0;
    let _cmd = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the value to be checked whether it exists in the filter
    let item = &input_args[curr_cmd_idx];
    let filter_key = ctx.open_key(filter_name);
    let my_value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_e) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match my_value {
        Some(val) => {
            // Instantiate bloom filter.
            let sip_keys = [
                (val.sip_key_one_a, val.sip_key_one_b),
                (val.sip_key_two_a, val.sip_key_two_b),
            ];
            let bloom = Bloom::from_existing(
                &val.bitmap,
                val.number_of_bits,
                val.number_of_hash_functions,
                sip_keys,
            );
            // Check if item exists.
            if bloom.check(&item) {
                return Ok(RedisValue::Integer(1));
            }
            Ok(RedisValue::Integer(0))
        }
        None => Ok(RedisValue::Integer(0)),
    }
}

pub fn bloom_filter_card(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc != 2 {
        return Err(RedisError::Str(ERROR));
    }
    let mut curr_cmd_idx = 0;
    let _cmd = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    let filter_key = ctx.open_key(filter_name);
    let my_value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_e) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match my_value {
        Some(val) => Ok(RedisValue::Integer(val.num_items.try_into().unwrap())),
        None => Ok(RedisValue::Integer(0)),
    }
}
