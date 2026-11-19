use crate::configs;
use crate::cuckoo::data_type::CUCKOO_TYPE;
use crate::cuckoo::utils::{
    CuckooObject, ADD_EVENT, BAD_BUCKET_SIZE, BAD_CAPACITY, BAD_EXPANSION, BAD_MAX_ITERATIONS,
    BUCKET_SIZE_ARG_REQUIRED, BUCKET_SIZE_OUT_OF_RANGE, CAPACITY_ARG_REQUIRED,
    CAPACITY_MUST_BE_LARGER_THAN_ZERO, CAPACITY_OUT_OF_RANGE, CREATE_EVENT, DEL_EVENT,
    EXPANSION_ARG_REQUIRED, FAILED_TO_SET_FILTER, INSERT_EVENT, ITEMS_KEYWORD_REQUIRED,
    ITEM_EXISTS, LOAD_EVENT, MAX_ITERATIONS_ARG_REQUIRED, MAX_KICKS_OUT_OF_RANGE, NOT_FOUND,
    NO_ITEMS_SPECIFIED, RESERVE_EVENT, UNKNOWN_OPTION, UNKNOWN_OPTION_OR_MISSING_ITEMS,
};
use crate::wrapper::must_obey_client;
use std::sync::atomic::Ordering;
use valkey_module::{
    Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, VALKEY_OK,
};

fn validate_capacity(capacity: i64) -> Result<(), ValkeyError> {
    if capacity == 0 {
        return Err(ValkeyError::Str(CAPACITY_MUST_BE_LARGER_THAN_ZERO));
    }
    // CUCKOO_CAPACITY_MAX == i64::MAX, so only the lower bound matters
    if capacity < configs::CUCKOO_CAPACITY_MIN {
        return Err(ValkeyError::Str(CAPACITY_OUT_OF_RANGE));
    }
    Ok(())
}

fn validate_bucket_size(bucket_size: i64) -> Result<(), ValkeyError> {
    if !(configs::CUCKOO_BUCKET_SIZE_MIN..=configs::CUCKOO_BUCKET_SIZE_MAX).contains(&bucket_size) {
        return Err(ValkeyError::Str(BUCKET_SIZE_OUT_OF_RANGE));
    }
    Ok(())
}

fn validate_max_kicks(max_kicks: i64) -> Result<(), ValkeyError> {
    if !(configs::CUCKOO_MAX_KICKS_MIN..=configs::CUCKOO_MAX_KICKS_MAX).contains(&max_kicks) {
        return Err(ValkeyError::Str(MAX_KICKS_OUT_OF_RANGE));
    }
    Ok(())
}

struct InsertOptions {
    capacity: Option<i64>,
    bucket_size: Option<u8>,
    max_kicks: Option<u32>,
    nocreate: bool,
}

fn parse_insert_options(
    args: &[ValkeyString],
    start_idx: usize,
) -> Result<(InsertOptions, usize), ValkeyError> {
    let mut options = InsertOptions {
        capacity: None,
        bucket_size: None,
        max_kicks: None,
        nocreate: false,
    };

    let mut curr_idx = start_idx;
    let argc = args.len();

    while curr_idx < argc {
        match args[curr_idx].to_string_lossy().to_uppercase().as_str() {
            "CAPACITY" => {
                curr_idx += 1;
                if curr_idx >= argc {
                    return Err(ValkeyError::Str(CAPACITY_ARG_REQUIRED));
                }
                let cap = match args[curr_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_capacity(num)?;
                        num
                    }
                    _ => return Err(ValkeyError::Str(BAD_CAPACITY)),
                };
                options.capacity = Some(cap);
                curr_idx += 1;
            }
            "BUCKETSIZE" => {
                curr_idx += 1;
                if curr_idx >= argc {
                    return Err(ValkeyError::Str(BUCKET_SIZE_ARG_REQUIRED));
                }
                let bs = match args[curr_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_bucket_size(num)?;
                        num as u8
                    }
                    _ => return Err(ValkeyError::Str(BAD_BUCKET_SIZE)),
                };
                options.bucket_size = Some(bs);
                curr_idx += 1;
            }
            "MAXITERATIONS" => {
                curr_idx += 1;
                if curr_idx >= argc {
                    return Err(ValkeyError::Str(MAX_ITERATIONS_ARG_REQUIRED));
                }
                let mk = match args[curr_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_max_kicks(num)?;
                        num as u32
                    }
                    _ => return Err(ValkeyError::Str(BAD_MAX_ITERATIONS)),
                };
                options.max_kicks = Some(mk);
                curr_idx += 1;
            }
            "NOCREATE" => {
                options.nocreate = true;
                curr_idx += 1;
            }
            "ITEMS" => {
                curr_idx += 1;
                return Ok((options, curr_idx));
            }
            _ => {
                return Err(ValkeyError::Str(UNKNOWN_OPTION_OR_MISSING_ITEMS));
            }
        }
    }

    Err(ValkeyError::Str(ITEMS_KEYWORD_REQUIRED))
}

fn handle_cuckoo_add(
    args: &[ValkeyString],
    argc: usize,
    item_idx: usize,
    cuckoo: &mut CuckooObject,
    multi: bool,
    add_succeeded: &mut bool,
    validate_size_limit: bool,
) -> Result<ValkeyValue, ValkeyError> {
    match multi {
        true => {
            let mut result = Vec::with_capacity(argc - item_idx);
            let mut curr_cmd_idx = item_idx;
            while curr_cmd_idx < argc {
                let item = args[curr_cmd_idx].as_slice();
                match cuckoo.add_item(item, validate_size_limit) {
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
                curr_cmd_idx += 1;
            }
            Ok(ValkeyValue::Array(result))
        }
        false => {
            let item = args[item_idx].as_slice();
            match cuckoo.add_item(item, validate_size_limit) {
                Ok(add_result) => {
                    *add_succeeded = add_result == 1;
                    Ok(ValkeyValue::Integer(add_result))
                }
                Err(err) => Err(ValkeyError::Str(err.as_str())),
            }
        }
    }
}

/// Implements CF.ADD and CF.MADD commands.
pub fn cuckoo_filter_add_value(
    ctx: &Context,
    args: Vec<ValkeyString>,
    multi: bool,
) -> ValkeyResult {
    let argc = args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let validate_size_limit = !must_obey_client(ctx);
    let mut add_succeeded = false;
    let curr_cmd_idx = 2;

    let key_name = &args[1];

    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match value {
        Some(cuckoo) => {
            let response = handle_cuckoo_add(
                &args,
                argc,
                curr_cmd_idx,
                cuckoo,
                multi,
                &mut add_succeeded,
                validate_size_limit,
            );
            if add_succeeded {
                ctx.replicate_verbatim();
                ctx.notify_keyspace_event(NotifyEvent::MODULE, ADD_EVENT, key_name);
            }
            response
        }
        None => {
            let capacity = configs::CUCKOO_CAPACITY.load(Ordering::Relaxed);
            let bucket_size = configs::CUCKOO_BUCKET_SIZE.load(Ordering::Relaxed) as usize;
            let max_kicks = configs::CUCKOO_MAX_KICKS.load(Ordering::Relaxed) as u32;
            let expansion = configs::CUCKOO_EXPANSION.load(Ordering::Relaxed) as u32;

            let mut cuckoo = match CuckooObject::new_reserved(
                capacity,
                bucket_size,
                max_kicks,
                expansion,
                validate_size_limit,
            ) {
                Ok(cf) => cf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };

            let response = handle_cuckoo_add(
                &args,
                argc,
                curr_cmd_idx,
                &mut cuckoo,
                multi,
                &mut add_succeeded,
                validate_size_limit,
            );

            match filter_key.set_value(&CUCKOO_TYPE, cuckoo) {
                Ok(()) => {
                    if add_succeeded {
                        ctx.replicate_verbatim();
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, CREATE_EVENT, key_name);
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, ADD_EVENT, key_name);
                    }
                    response
                }
                Err(_) => Err(ValkeyError::Str(FAILED_TO_SET_FILTER)),
            }
        }
    }
}

fn handle_cuckoo_addnx(
    args: &[ValkeyString],
    argc: usize,
    item_idx: usize,
    cuckoo: &mut CuckooObject,
    multi: bool,
    add_succeeded: &mut bool,
    validate_size_limit: bool,
) -> Result<ValkeyValue, ValkeyError> {
    match multi {
        true => {
            let mut result = Vec::with_capacity(argc - item_idx);
            let mut curr_cmd_idx = item_idx;
            while curr_cmd_idx < argc {
                let item = args[curr_cmd_idx].as_slice();
                if cuckoo.item_exists(item) {
                    result.push(ValkeyValue::Integer(0));
                } else {
                    match cuckoo.add_item(item, validate_size_limit) {
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
                    }
                }
                curr_cmd_idx += 1;
            }
            Ok(ValkeyValue::Array(result))
        }
        false => {
            let item = args[item_idx].as_slice();
            if cuckoo.item_exists(item) {
                Ok(ValkeyValue::Integer(0))
            } else {
                match cuckoo.add_item(item, validate_size_limit) {
                    Ok(add_result) => {
                        *add_succeeded = add_result == 1;
                        Ok(ValkeyValue::Integer(add_result))
                    }
                    Err(err) => Err(ValkeyError::Str(err.as_str())),
                }
            }
        }
    }
}

/// Implements CF.ADDNX and CF.MADDNX commands.
pub fn cuckoo_filter_addnx(ctx: &Context, args: Vec<ValkeyString>, multi: bool) -> ValkeyResult {
    let argc = args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let validate_size_limit = !must_obey_client(ctx);
    let mut add_succeeded = false;
    let curr_cmd_idx = 2;

    let key_name = &args[1];

    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match value {
        Some(cuckoo) => {
            let response = handle_cuckoo_addnx(
                &args,
                argc,
                curr_cmd_idx,
                cuckoo,
                multi,
                &mut add_succeeded,
                validate_size_limit,
            );
            if add_succeeded {
                ctx.replicate_verbatim();
                ctx.notify_keyspace_event(NotifyEvent::MODULE, ADD_EVENT, key_name);
            }
            response
        }
        None => {
            let capacity = configs::CUCKOO_CAPACITY.load(Ordering::Relaxed);
            let bucket_size = configs::CUCKOO_BUCKET_SIZE.load(Ordering::Relaxed) as usize;
            let max_kicks = configs::CUCKOO_MAX_KICKS.load(Ordering::Relaxed) as u32;
            let expansion = configs::CUCKOO_EXPANSION.load(Ordering::Relaxed) as u32;

            let mut cuckoo = match CuckooObject::new_reserved(
                capacity,
                bucket_size,
                max_kicks,
                expansion,
                validate_size_limit,
            ) {
                Ok(cf) => cf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };

            let response = handle_cuckoo_addnx(
                &args,
                argc,
                curr_cmd_idx,
                &mut cuckoo,
                multi,
                &mut add_succeeded,
                validate_size_limit,
            );

            match filter_key.set_value(&CUCKOO_TYPE, cuckoo) {
                Ok(()) => {
                    if add_succeeded {
                        ctx.replicate_verbatim();
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, CREATE_EVENT, key_name);
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, ADD_EVENT, key_name);
                    }
                    response
                }
                Err(_) => Err(ValkeyError::Str(FAILED_TO_SET_FILTER)),
            }
        }
    }
}

/// Implements CF.DEL command.
pub fn cuckoo_filter_delete(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc != 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = &args[1];
    let item = args[2].as_slice();

    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match value {
        Some(cuckoo) => match cuckoo.delete_item(item) {
            Ok(deleted) => {
                if deleted == 1 {
                    ctx.replicate_verbatim();
                    ctx.notify_keyspace_event(NotifyEvent::MODULE, DEL_EVENT, key_name);
                }
                Ok(ValkeyValue::Integer(deleted))
            }
            Err(err) => Err(ValkeyError::Str(err.as_str())),
        },
        None => Err(ValkeyError::Str(NOT_FOUND)),
    }
}

/// Implements CF.COUNT command.
pub fn cuckoo_filter_count(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc != 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = &args[1];
    let item = args[2].as_slice();

    let filter_key = ctx.open_key(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match value {
        Some(val) => Ok(ValkeyValue::Integer(val.count_item(item))),
        None => Ok(ValkeyValue::Integer(0)),
    }
}

fn handle_item_exists(value: Option<&CuckooObject>, item: &[u8]) -> ValkeyValue {
    if let Some(val) = value {
        if val.item_exists(item) {
            return ValkeyValue::Integer(1);
        }
        return ValkeyValue::Integer(0);
    };
    ValkeyValue::Integer(0)
}

/// Implements CF.EXISTS and CF.MEXISTS commands.
pub fn cuckoo_filter_exists(ctx: &Context, args: Vec<ValkeyString>, multi: bool) -> ValkeyResult {
    let argc = args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let mut curr_cmd_idx = 1;
    let key_name = &args[curr_cmd_idx];
    curr_cmd_idx += 1;

    let filter_key = ctx.open_key(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    if !multi {
        let item = args[curr_cmd_idx].as_slice();
        return Ok(handle_item_exists(value, item));
    }

    let mut result = Vec::with_capacity(argc - curr_cmd_idx);
    while curr_cmd_idx < argc {
        let item = args[curr_cmd_idx].as_slice();
        result.push(handle_item_exists(value, item));
        curr_cmd_idx += 1;
    }
    Ok(ValkeyValue::Array(result))
}

/// Implements CF.INSERT and CF.INSERTNX commands.
pub fn cuckoo_filter_insert(ctx: &Context, args: Vec<ValkeyString>, nx_mode: bool) -> ValkeyResult {
    let argc = args.len();
    if argc < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let validate_size_limit = !must_obey_client(ctx);

    let key_name = &args[1];

    let (options, items_idx) = parse_insert_options(&args, 2)?;

    if items_idx >= argc {
        return Err(ValkeyError::Str(NO_ITEMS_SPECIFIED));
    }

    let mut add_succeeded = false;

    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match value {
        Some(cuckoo) => {
            let response = if nx_mode {
                handle_cuckoo_addnx(
                    &args,
                    argc,
                    items_idx,
                    cuckoo,
                    true,
                    &mut add_succeeded,
                    validate_size_limit,
                )
            } else {
                handle_cuckoo_add(
                    &args,
                    argc,
                    items_idx,
                    cuckoo,
                    true,
                    &mut add_succeeded,
                    validate_size_limit,
                )
            };

            if add_succeeded {
                ctx.replicate_verbatim();
                ctx.notify_keyspace_event(NotifyEvent::MODULE, INSERT_EVENT, key_name);
            }
            response
        }
        None => {
            if options.nocreate {
                return Err(ValkeyError::Str(NOT_FOUND));
            }

            let capacity = options
                .capacity
                .unwrap_or_else(|| configs::CUCKOO_CAPACITY.load(Ordering::Relaxed));
            let bucket_size = options
                .bucket_size
                .map(|b| b as usize)
                .unwrap_or_else(|| configs::CUCKOO_BUCKET_SIZE.load(Ordering::Relaxed) as usize);
            let max_kicks = options
                .max_kicks
                .unwrap_or_else(|| configs::CUCKOO_MAX_KICKS.load(Ordering::Relaxed) as u32);
            let expansion = configs::CUCKOO_EXPANSION.load(Ordering::Relaxed) as u32;

            let mut cuckoo = match CuckooObject::new_reserved(
                capacity,
                bucket_size,
                max_kicks,
                expansion,
                validate_size_limit,
            ) {
                Ok(cf) => cf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };

            let response = if nx_mode {
                handle_cuckoo_addnx(
                    &args,
                    argc,
                    items_idx,
                    &mut cuckoo,
                    true,
                    &mut add_succeeded,
                    validate_size_limit,
                )
            } else {
                handle_cuckoo_add(
                    &args,
                    argc,
                    items_idx,
                    &mut cuckoo,
                    true,
                    &mut add_succeeded,
                    validate_size_limit,
                )
            };

            match filter_key.set_value(&CUCKOO_TYPE, cuckoo) {
                Ok(()) => {
                    if add_succeeded {
                        ctx.replicate_verbatim();
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, CREATE_EVENT, key_name);
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, INSERT_EVENT, key_name);
                    }
                    response
                }
                Err(_) => Err(ValkeyError::Str(FAILED_TO_SET_FILTER)),
            }
        }
    }
}

/// Implements CF.RESERVE command.
pub fn cuckoo_filter_reserve(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let mut curr_cmd_idx = 1;
    let key_name = &args[curr_cmd_idx];
    curr_cmd_idx += 1;

    let capacity = match args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
        Ok(num) => {
            validate_capacity(num)?;
            num
        }
        _ => return Err(ValkeyError::Str(BAD_CAPACITY)),
    };
    curr_cmd_idx += 1;

    let mut bucket_size = configs::CUCKOO_BUCKET_SIZE.load(Ordering::Relaxed);
    let mut max_kicks = configs::CUCKOO_MAX_KICKS.load(Ordering::Relaxed);
    let mut expansion = configs::CUCKOO_EXPANSION.load(Ordering::Relaxed);

    while curr_cmd_idx < argc {
        match args[curr_cmd_idx].to_string_lossy().to_uppercase().as_str() {
            "BUCKETSIZE" => {
                curr_cmd_idx += 1;
                if curr_cmd_idx >= argc {
                    return Err(ValkeyError::Str(BUCKET_SIZE_ARG_REQUIRED));
                }
                bucket_size = match args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_bucket_size(num)?;
                        num
                    }
                    _ => return Err(ValkeyError::Str(BAD_BUCKET_SIZE)),
                };
            }
            "MAXITERATIONS" => {
                curr_cmd_idx += 1;
                if curr_cmd_idx >= argc {
                    return Err(ValkeyError::Str(MAX_ITERATIONS_ARG_REQUIRED));
                }
                max_kicks = match args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_max_kicks(num)?;
                        num
                    }
                    _ => return Err(ValkeyError::Str(BAD_MAX_ITERATIONS)),
                };
            }
            "EXPANSION" => {
                curr_cmd_idx += 1;
                if curr_cmd_idx >= argc {
                    return Err(ValkeyError::Str(EXPANSION_ARG_REQUIRED));
                }
                expansion = match args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) if num >= configs::CUCKOO_EXPANSION_MIN as i64 => num,
                    _ => return Err(ValkeyError::Str(BAD_EXPANSION)),
                };
            }
            _ => return Err(ValkeyError::Str(UNKNOWN_OPTION)),
        }
        curr_cmd_idx += 1;
    }

    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match value {
        Some(_) => Err(ValkeyError::Str(ITEM_EXISTS)),
        None => {
            let validate_size_limit = !must_obey_client(ctx);

            let cuckoo = match CuckooObject::new_reserved(
                capacity,
                bucket_size as usize,
                max_kicks as u32,
                expansion as u32,
                validate_size_limit,
            ) {
                Ok(cf) => cf,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };

            match filter_key.set_value(&CUCKOO_TYPE, cuckoo) {
                Ok(()) => {
                    ctx.replicate_verbatim();
                    ctx.notify_keyspace_event(NotifyEvent::MODULE, RESERVE_EVENT, key_name);
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str(FAILED_TO_SET_FILTER)),
            }
        }
    }
}

/// Implements CF.INFO command.
pub fn cuckoo_filter_info(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if !(2..=3).contains(&argc) {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = &args[1];

    let filter_key = ctx.open_key(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match value {
        Some(cuckoo) => {
            if argc == 3 {
                let field_name = args[2].to_string_lossy().to_uppercase();
                return match field_name.as_str() {
                    "SIZE" => Ok(ValkeyValue::Integer(cuckoo.memory_usage() as i64)),
                    "NUMBER OF BUCKETS" => Ok(ValkeyValue::Integer(cuckoo.num_filters() as i64)),
                    "NUMBER OF ITEMS INSERTED" => Ok(ValkeyValue::Integer(cuckoo.num_items())),
                    "NUMBER OF FILTERS" => Ok(ValkeyValue::Integer(cuckoo.num_filters() as i64)),
                    "BUCKET SIZE" => Ok(ValkeyValue::Integer(cuckoo.bucket_size() as i64)),
                    "MAX ITERATIONS" => Ok(ValkeyValue::Integer(cuckoo.max_kicks() as i64)),
                    "EXPANSION RATE" => Ok(ValkeyValue::Integer(cuckoo.expansion() as i64)),
                    _ => Err(ValkeyError::Str(UNKNOWN_OPTION)),
                };
            }
            let result = vec![
                ValkeyValue::SimpleStringStatic("Size"),
                ValkeyValue::Integer(cuckoo.memory_usage() as i64),
                ValkeyValue::SimpleStringStatic("Number of buckets"),
                ValkeyValue::Integer(cuckoo.num_filters() as i64),
                ValkeyValue::SimpleStringStatic("Number of items inserted"),
                ValkeyValue::Integer(cuckoo.num_items()),
                ValkeyValue::SimpleStringStatic("Number of filters"),
                ValkeyValue::Integer(cuckoo.num_filters() as i64),
                ValkeyValue::SimpleStringStatic("Bucket size"),
                ValkeyValue::Integer(cuckoo.bucket_size() as i64),
                ValkeyValue::SimpleStringStatic("Max iterations"),
                ValkeyValue::Integer(cuckoo.max_kicks() as i64),
                ValkeyValue::SimpleStringStatic("Expansion rate"),
                ValkeyValue::Integer(cuckoo.expansion() as i64),
            ];
            Ok(ValkeyValue::Array(result))
        }
        None => Err(ValkeyError::Str(NOT_FOUND)),
    }
}

/// Implements CF.LOAD command for AOF operations.
pub fn cuckoo_filter_load(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc != 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = &args[1];
    let data = args[2].as_slice();

    let cuckoo = match CuckooObject::decode_object(data, true) {
        Ok(cf) => cf,
        Err(err) => return Err(ValkeyError::Str(err.as_str())),
    };

    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    if value.is_some() {
        return Err(ValkeyError::Str(ITEM_EXISTS));
    }

    match filter_key.set_value(&CUCKOO_TYPE, cuckoo) {
        Ok(()) => {
            ctx.replicate_verbatim();
            ctx.notify_keyspace_event(NotifyEvent::MODULE, LOAD_EVENT, key_name);
            VALKEY_OK
        }
        Err(_) => Err(ValkeyError::Str(FAILED_TO_SET_FILTER)),
    }
}
