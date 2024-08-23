use crate::bloom::data_type::BLOOM_FILTER_TYPE;
use crate::bloom::utils;
use crate::bloom::utils::BloomFilterType;
use crate::configs;
use crate::configs::BLOOM_CAPACITY_MAX;
use crate::configs::BLOOM_EXPANSION_MAX;
use std::sync::atomic::Ordering;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, VALKEY_OK};

fn single_add_helper(
    ctx: &Context,
    item: &[u8],
    bf: &mut BloomFilterType,
    replicate_on_success: bool,
) -> Result<ValkeyValue, ValkeyError> {
    match bf.add_item(item) {
        Ok(result) => {
            if replicate_on_success && result == 1 {
                ctx.replicate_verbatim();
            }
            Ok(ValkeyValue::Integer(result))
        }
        Err(err) => Err(ValkeyError::Str(err.as_str())),
    }
}

fn multi_add_helper(
    ctx: &Context,
    args: &[ValkeyString],
    argc: usize,
    skip_idx: usize,
    bf: &mut BloomFilterType,
    replicate_on_success: bool,
) -> Result<ValkeyValue, ValkeyError> {
    let mut result = Vec::new();
    let mut write_operation = false;
    for item in args.iter().take(argc).skip(skip_idx) {
        match bf.add_item(item.as_slice()) {
            Ok(add_result) => {
                if add_result == 1 {
                    write_operation = true;
                }
                result.push(ValkeyValue::Integer(add_result));
            }
            Err(err) => {
                result.push(ValkeyValue::StaticError(err.as_str()));
                continue;
            }
        };
    }
    if replicate_on_success && write_operation {
        ctx.replicate_verbatim();
    }
    Ok(ValkeyValue::Array(result))
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
    match value {
        Some(bf) => {
            if !multi {
                let item = input_args[curr_cmd_idx].as_slice();
                return single_add_helper(ctx, item, bf, true);
            }
            multi_add_helper(ctx, input_args, argc, curr_cmd_idx, bf, true)
        }
        None => {
            // Instantiate empty bloom filter.
            let fp_rate = configs::BLOOM_FP_RATE_DEFAULT;
            let capacity = configs::BLOOM_CAPACITY.load(Ordering::Relaxed) as u32;
            let expansion = configs::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
            let mut bf = BloomFilterType::new_reserved(fp_rate, capacity, expansion);
            let result = match multi {
                true => multi_add_helper(ctx, input_args, argc, curr_cmd_idx, &mut bf, false),
                false => {
                    let item = input_args[curr_cmd_idx].as_slice();
                    single_add_helper(ctx, item, &mut bf, false)
                }
            };
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bf) {
                Ok(_) => {
                    ctx.replicate_verbatim();
                    result
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}

fn item_exists_helper(value: Option<&BloomFilterType>, item: &[u8]) -> ValkeyValue {
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
        return Ok(item_exists_helper(value, item));
    }
    let mut result = Vec::new();
    while curr_cmd_idx < argc {
        let item = input_args[curr_cmd_idx].as_slice();
        result.push(item_exists_helper(value, item));
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
    let fp_rate = match input_args[curr_cmd_idx].to_string_lossy().parse::<f32>() {
        Ok(num) if (0.0..1.0).contains(&num) => num,
        Ok(num) if !(0.0..1.0).contains(&num) => {
            return Err(ValkeyError::Str(utils::ERROR_RATE_RANGE));
        }
        _ => {
            return Err(ValkeyError::Str(utils::BAD_ERROR_RATE));
        }
    };
    curr_cmd_idx += 1;
    // Parse the capacity
    let capacity = match input_args[curr_cmd_idx].to_string_lossy().parse::<u32>() {
        Ok(num) if num > 0 && num < BLOOM_CAPACITY_MAX => num,
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
                    Ok(num) if num > 0 && num <= BLOOM_EXPANSION_MAX => num,
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
            let bloom = BloomFilterType::new_reserved(fp_rate, capacity, expansion);
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bloom) {
                Ok(_v) => {
                    ctx.replicate_verbatim();
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
            "ERROR" if idx < (argc - 1) => {
                idx += 1;
                fp_rate = match input_args[idx].to_string_lossy().parse::<f32>() {
                    Ok(num) if (0.0..1.0).contains(&num) => num,
                    Ok(num) if !(0.0..1.0).contains(&num) => {
                        return Err(ValkeyError::Str(utils::ERROR_RATE_RANGE));
                    }
                    _ => {
                        return Err(ValkeyError::Str(utils::BAD_ERROR_RATE));
                    }
                };
            }
            "CAPACITY" if idx < (argc - 1) => {
                idx += 1;
                capacity = match input_args[idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if num > 0 && num < BLOOM_CAPACITY_MAX => num,
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
            "EXPANSION" if idx < (argc - 1) => {
                idx += 1;
                expansion = match input_args[idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if num > 0 && num <= BLOOM_EXPANSION_MAX => num,
                    _ => {
                        return Err(ValkeyError::Str(utils::BAD_EXPANSION));
                    }
                };
            }
            "ITEMS" if idx < (argc - 1) => {
                idx += 1;
                break;
            }
            _ => {
                return Err(ValkeyError::WrongArity);
            }
        }
        idx += 1;
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
        Some(bf) => multi_add_helper(ctx, input_args, argc, idx, bf, true),
        None => {
            if nocreate {
                return Err(ValkeyError::Str(utils::NOT_FOUND));
            }
            let mut bf = BloomFilterType::new_reserved(fp_rate, capacity, expansion);
            let result = multi_add_helper(ctx, input_args, argc, idx, &mut bf, false);
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bf) {
                Ok(_) => {
                    ctx.replicate_verbatim();
                    result
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
                "SIZE" => Ok(ValkeyValue::Integer(val.get_memory_usage() as i64)),
                "FILTERS" => Ok(ValkeyValue::Integer(val.filters.len() as i64)),
                "ITEMS" => Ok(ValkeyValue::Integer(val.cardinality())),
                "EXPANSION" => {
                    if val.expansion == 0 {
                        return Ok(ValkeyValue::Integer(-1));
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
                ValkeyValue::Integer(val.get_memory_usage() as i64),
                ValkeyValue::SimpleStringStatic("Number of filters"),
                ValkeyValue::Integer(val.filters.len() as i64),
                ValkeyValue::SimpleStringStatic("Number of items inserted"),
                ValkeyValue::Integer(val.cardinality()),
                ValkeyValue::SimpleStringStatic("Expansion rate"),
            ];
            if val.expansion == 0 {
                result.push(ValkeyValue::Integer(-1));
            } else {
                result.push(ValkeyValue::Integer(val.expansion as i64));
            }
            Ok(ValkeyValue::Array(result))
        }
        _ => Err(ValkeyError::Str(utils::NOT_FOUND)),
    }
}
