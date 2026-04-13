use crate::cuckoo::data_type::CUCKOO_TYPE;
use crate::cuckoo::utils::CuckooObject;
use crate::configs;
use crate::wrapper::must_obey_client;
use std::sync::atomic::Ordering;
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, VALKEY_OK};

/// Helper function to validate capacity parameter for CF.RESERVE.
/// Capacity must be within allowed range and greater than zero.
fn validate_capacity(capacity: i64) -> Result<(), ValkeyError> {
    if capacity < configs::CUCKOO_CAPACITY_MIN || capacity > configs::CUCKOO_CAPACITY_MAX {
        return Err(ValkeyError::Str("ERR capacity must be between min and max"));
    }
    if capacity == 0 {
        return Err(ValkeyError::Str("ERR capacity must be larger than 0"));
    }
    Ok(())
}

/// Helper function to validate bucket size parameter for CF.RESERVE.
/// Bucket size affects the number of entries per bucket in the cuckoo filter.
fn validate_bucket_size(bucket_size: i64) -> Result<(), ValkeyError> {
    if bucket_size < configs::CUCKOO_BUCKET_SIZE_MIN || bucket_size > configs::CUCKOO_BUCKET_SIZE_MAX {
        return Err(ValkeyError::Str("ERR bucket size must be between min and max"));
    }
    Ok(())
}

/// Helper function to validate max kicks parameter for CF.RESERVE.
/// Max kicks determines how many times we try to relocate items before giving up.
fn validate_max_kicks(max_kicks: i64) -> Result<(), ValkeyError> {
    if max_kicks < configs::CUCKOO_MAX_KICKS_MIN || max_kicks > configs::CUCKOO_MAX_KICKS_MAX {
        return Err(ValkeyError::Str("ERR max kicks must be between min and max"));
    }
    Ok(())
}

/// Helper structure to hold parsed CF.INSERT/CF.INSERTNX options.
struct InsertOptions {
    capacity: Option<i64>,
    bucket_size: Option<u8>,
    max_kicks: Option<u32>,
    nocreate: bool,
}

/// Helper function to parse CF.INSERT and CF.INSERTNX command options.
/// Returns InsertOptions and the index where items begin.
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
                    return Err(ValkeyError::Str("ERR CAPACITY requires an argument"));
                }
                let cap = match args[curr_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_capacity(num)?;
                        num
                    }
                    _ => return Err(ValkeyError::Str("ERR bad capacity")),
                };
                options.capacity = Some(cap);
                curr_idx += 1;
            }
            "BUCKETSIZE" => {
                curr_idx += 1;
                if curr_idx >= argc {
                    return Err(ValkeyError::Str("ERR BUCKETSIZE requires an argument"));
                }
                let bs = match args[curr_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_bucket_size(num)?;
                        num as u8
                    }
                    _ => return Err(ValkeyError::Str("ERR bad bucket size")),
                };
                options.bucket_size = Some(bs);
                curr_idx += 1;
            }
            "MAXITERATIONS" => {
                curr_idx += 1;
                if curr_idx >= argc {
                    return Err(ValkeyError::Str("ERR MAXITERATIONS requires an argument"));
                }
                let mk = match args[curr_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_max_kicks(num)?;
                        num as u32
                    }
                    _ => return Err(ValkeyError::Str("ERR bad max iterations")),
                };
                options.max_kicks = Some(mk);
                curr_idx += 1;
            }
            "NOCREATE" => {
                options.nocreate = true;
                curr_idx += 1;
            }
            "ITEMS" => {
                // Found ITEMS keyword, items start at next index
                curr_idx += 1;
                return Ok((options, curr_idx));
            }
            _ => {
                return Err(ValkeyError::Str("ERR unknown option or missing ITEMS keyword"));
            }
        }
    }

    Err(ValkeyError::Str("ERR ITEMS keyword required"))
}

/// Helper function to handle adding items to a cuckoo filter
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
/// CF.ADD adds a single item to a cuckoo filter.
/// CF.MADD adds multiple items to a cuckoo filter.
/// Creates the filter if it doesn't exist.
pub fn cuckoo_filter_add_value(
    ctx: &Context,
    args: Vec<ValkeyString>,
    multi: bool,
) -> ValkeyResult {
    let argc = args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    // Check if this is a client operation that must follow certain rules
    let _ = must_obey_client(ctx);

    let validate_size_limit = true;
    let mut add_succeeded = false;
    let curr_cmd_idx = 2; // Start of items

    // Parse key name
    let key_name = &args[1];

    // Open key for writing
    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    match value {
        Some(cuckoo) => {
            // Filter exists, add items to it
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
                // Replicate the command
                ctx.replicate_verbatim();
                // Notify keyspace event
                ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.add", key_name);
            }
            response
        }
        None => {
            // Create new filter with default parameters
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
                        // Replicate the command
                        ctx.replicate_verbatim();
                        // Notify keyspace events (both creation and add)
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.create", key_name);
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.add", key_name);
                    }
                    response
                }
                Err(_) => Err(ValkeyError::Str("ERR failed to set cuckoo filter")),
            }
        }
    }
}

/// Helper function to handle adding items with ADDNX logic (only add if not exists)
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
                // Check if item exists first
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
            // Check if item exists first
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
/// Similar to CF.ADD but only adds if the item doesn't already exist.
/// Returns 1 if added, 0 if already exists.
pub fn cuckoo_filter_addnx(
    ctx: &Context,
    args: Vec<ValkeyString>,
    multi: bool,
) -> ValkeyResult {
    let argc = args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    // Check if this is a client operation that must follow certain rules
    let _ = must_obey_client(ctx);

    let validate_size_limit = true;
    let mut add_succeeded = false;
    let curr_cmd_idx = 2; // Start of items

    // Parse key name
    let key_name = &args[1];

    // Open key for writing
    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    match value {
        Some(cuckoo) => {
            // Filter exists, add items to it (only if not present)
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
                // Replicate the command
                ctx.replicate_verbatim();
                // Notify keyspace event
                ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.add", key_name);
            }
            response
        }
        None => {
            // Create new filter with default parameters
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
                        // Replicate the command
                        ctx.replicate_verbatim();
                        // Notify keyspace events (both creation and add)
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.create", key_name);
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.add", key_name);
                    }
                    response
                }
                Err(_) => Err(ValkeyError::Str("ERR failed to set cuckoo filter")),
            }
        }
    }
}

/// Implements CF.DEL command.
/// Deletes an item from the cuckoo filter.
/// Returns 1 if deleted, 0 if item was not found.
pub fn cuckoo_filter_delete(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc != 3 {
        return Err(ValkeyError::WrongArity);
    }

    // Parse key name
    let key_name = &args[1];
    // Parse item to delete
    let item = args[2].as_slice();

    // Open key for writing
    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    match value {
        Some(cuckoo) => {
            match cuckoo.delete_item(item) {
                Ok(deleted) => {
                    if deleted == 1 {
                        // Replicate the command to replicas
                        ctx.replicate_verbatim();
                        // Notify keyspace event
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.del", key_name);
                    }
                    Ok(ValkeyValue::Integer(deleted))
                }
                Err(err) => Err(ValkeyError::Str(err.as_str())),
            }
        }
        None => Ok(ValkeyValue::Integer(0)), // Key doesn't exist
    }
}

/// Implements CF.COUNT command.
/// Returns the number of times an item may be in the filter.
/// Due to the nature of cuckoo filters, this may return > 1.
pub fn cuckoo_filter_count(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc != 3 {
        return Err(ValkeyError::WrongArity);
    }

    // Parse key name
    let key_name = &args[1];
    // Parse item to count
    let item = args[2].as_slice();

    // Open key for reading
    let filter_key = ctx.open_key(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    match value {
        Some(val) => Ok(ValkeyValue::Integer(val.count_item(item))),
        None => Ok(ValkeyValue::Integer(0)),
    }
}

/// Helper function to check if an item exists in the cuckoo filter.
fn handle_item_exists(value: Option<&CuckooObject>, item: &[u8]) -> ValkeyValue {
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

/// Implements CF.EXISTS and CF.MEXISTS commands.
/// CF.EXISTS checks if a single item exists in the filter.
/// CF.MEXISTS checks if multiple items exist in the filter.
/// Returns 1 if exists, 0 otherwise.
pub fn cuckoo_filter_exists(
    ctx: &Context,
    args: Vec<ValkeyString>,
    multi: bool,
) -> ValkeyResult {
    let argc = args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let mut curr_cmd_idx = 1;
    // Parse key name
    let key_name = &args[curr_cmd_idx];
    curr_cmd_idx += 1;

    // Open key for reading
    let filter_key = ctx.open_key(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    if !multi {
        let item = args[curr_cmd_idx].as_slice();
        return Ok(handle_item_exists(value, item));
    }

    // Handle multiple items (MEXISTS)
    let mut result = Vec::with_capacity(argc - curr_cmd_idx);
    while curr_cmd_idx < argc {
        let item = args[curr_cmd_idx].as_slice();
        result.push(handle_item_exists(value, item));
        curr_cmd_idx += 1;
    }
    Ok(ValkeyValue::Array(result))
}

/// Implements CF.INSERT and CF.INSERTNX commands.
/// CF.INSERT adds items with optional filter creation parameters.
/// CF.INSERTNX only adds items if they don't exist.
/// Supports CAPACITY, BUCKETSIZE, MAXITERATIONS, NOCREATE options.
pub fn cuckoo_filter_insert(
    ctx: &Context,
    args: Vec<ValkeyString>,
    nx_mode: bool,
) -> ValkeyResult {
    let argc = args.len();
    if argc < 4 {
        // Minimum: command, key, ITEMS, item
        return Err(ValkeyError::WrongArity);
    }

    // Check if this is a client operation that must follow certain rules
    let _ = must_obey_client(ctx);

    // Parse key name
    let key_name = &args[1];

    // Parse insert options and get items start index
    let (options, items_idx) = parse_insert_options(&args, 2)?;

    if items_idx >= argc {
        return Err(ValkeyError::Str("ERR no items specified"));
    }

    let validate_size_limit = true;
    let mut add_succeeded = false;

    // Open key for writing
    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    match value {
        Some(cuckoo) => {
            // Filter exists, insert items
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
                ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.insert", key_name);
            }
            response
        }
        None => {
            // Filter doesn't exist
            if options.nocreate {
                return Err(ValkeyError::Str("ERR not found"));
            }

            // Create new filter with specified or default parameters
            let capacity = options.capacity.unwrap_or_else(|| configs::CUCKOO_CAPACITY.load(Ordering::Relaxed));
            let bucket_size = options.bucket_size.map(|b| b as usize)
                .unwrap_or_else(|| configs::CUCKOO_BUCKET_SIZE.load(Ordering::Relaxed) as usize);
            let max_kicks = options.max_kicks
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
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.create", key_name);
                        ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.insert", key_name);
                    }
                    response
                }
                Err(_) => Err(ValkeyError::Str("ERR failed to set cuckoo filter")),
            }
        }
    }
}

/// Implements CF.RESERVE command.
/// Creates an empty cuckoo filter with specified capacity.
/// Optional parameters: BUCKETSIZE, MAXITERATIONS, EXPANSION.
/// Returns error if key already exists.
pub fn cuckoo_filter_reserve(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let mut curr_cmd_idx = 1;
    // Parse key name
    let key_name = &args[curr_cmd_idx];
    curr_cmd_idx += 1;

    // Parse capacity
    let capacity = match args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
        Ok(num) => {
            validate_capacity(num)?;
            num
        }
        _ => {
            return Err(ValkeyError::Str("ERR bad capacity"));
        }
    };
    curr_cmd_idx += 1;

    // Parse optional parameters
    let mut bucket_size = configs::CUCKOO_BUCKET_SIZE.load(Ordering::Relaxed);
    let mut max_kicks = configs::CUCKOO_MAX_KICKS.load(Ordering::Relaxed);
    let mut expansion = configs::CUCKOO_EXPANSION.load(Ordering::Relaxed);

    while curr_cmd_idx < argc {
        match args[curr_cmd_idx].to_string_lossy().to_uppercase().as_str() {
            "BUCKETSIZE" => {
                curr_cmd_idx += 1;
                if curr_cmd_idx >= argc {
                    return Err(ValkeyError::Str("ERR BUCKETSIZE requires an argument"));
                }
                bucket_size = match args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_bucket_size(num)?;
                        num
                    }
                    _ => {
                        return Err(ValkeyError::Str("ERR bad bucket size"));
                    }
                };
            }
            "MAXITERATIONS" => {
                curr_cmd_idx += 1;
                if curr_cmd_idx >= argc {
                    return Err(ValkeyError::Str("ERR MAXITERATIONS requires an argument"));
                }
                max_kicks = match args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) => {
                        validate_max_kicks(num)?;
                        num
                    }
                    _ => {
                        return Err(ValkeyError::Str("ERR bad max iterations"));
                    }
                };
            }
            "EXPANSION" => {
                curr_cmd_idx += 1;
                if curr_cmd_idx >= argc {
                    return Err(ValkeyError::Str("ERR EXPANSION requires an argument"));
                }
                expansion = match args[curr_cmd_idx].to_string_lossy().parse::<i64>() {
                    Ok(num) if num >= configs::CUCKOO_EXPANSION_MIN as i64 => num,
                    _ => {
                        return Err(ValkeyError::Str("ERR bad expansion"));
                    }
                };
            }
            _ => {
                return Err(ValkeyError::Str("ERR unknown option"));
            }
        }
        curr_cmd_idx += 1;
    }

    // Open key for writing
    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    // Check if key already exists
    match value {
        Some(_) => Err(ValkeyError::Str("ERR item exists")),
        None => {
            // Skip size validation for replicated commands
            let validate_size_limit = !must_obey_client(ctx);

            // Create new CuckooObject
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
                    // Replicate the command
                    ctx.replicate_verbatim();
                    // Notify keyspace event
                    ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.reserve", key_name);
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str("ERR failed to set cuckoo filter")),
            }
        }
    }
}

/// Implements CF.INFO command.
/// Returns information about the cuckoo filter.
/// Can return all info or a specific field.
pub fn cuckoo_filter_info(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if !(2..=3).contains(&argc) {
        return Err(ValkeyError::WrongArity);
    }

    // Parse key name
    let key_name = &args[1];

    // Open key for reading
    let filter_key = ctx.open_key(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    match value {
        Some(cuckoo) => {
            // Build info response
            let mut result = Vec::new();

            result.push(ValkeyValue::SimpleStringStatic("Size"));
            result.push(ValkeyValue::Integer(cuckoo.memory_usage() as i64));

            result.push(ValkeyValue::SimpleStringStatic("Number of buckets"));
            result.push(ValkeyValue::Integer(cuckoo.num_filters() as i64));

            result.push(ValkeyValue::SimpleStringStatic("Number of items inserted"));
            result.push(ValkeyValue::Integer(cuckoo.num_items()));

            result.push(ValkeyValue::SimpleStringStatic("Number of filters"));
            result.push(ValkeyValue::Integer(cuckoo.num_filters() as i64));

            result.push(ValkeyValue::SimpleStringStatic("Bucket size"));
            result.push(ValkeyValue::Integer(cuckoo.bucket_size() as i64));

            result.push(ValkeyValue::SimpleStringStatic("Max iterations"));
            result.push(ValkeyValue::Integer(cuckoo.max_kicks() as i64));

            result.push(ValkeyValue::SimpleStringStatic("Expansion rate"));
            result.push(ValkeyValue::Integer(cuckoo.expansion() as i64));

            Ok(ValkeyValue::Array(result))
        }
        None => Err(ValkeyError::Str("ERR not found")),
    }
}

/// Implements CF.SCANDUMP command.
/// Begins an incremental save of the cuckoo filter.
/// Returns iterator value and data chunk.
/// Used in conjunction with CF.LOADCHUNK for filter migration.
pub fn cuckoo_filter_scandump(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc != 3 {
        return Err(ValkeyError::WrongArity);
    }

    // Parse key name
    let key_name = &args[1];
    // Parse iterator value
    let iter_val = match args[2].to_string_lossy().parse::<i64>() {
        Ok(num) => num,
        _ => return Err(ValkeyError::Str("ERR bad iterator")),
    };

    // Open key for reading
    let filter_key = ctx.open_key(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    match value {
        Some(cuckoo) => {
            if iter_val == 0 {
                // First call: serialize entire object
                match cuckoo.encode_object() {
                    Ok(data) => {
                        // Return [0, data] to indicate completion (one-shot serialization)
                        Ok(ValkeyValue::Array(vec![
                            ValkeyValue::Integer(0),
                            ValkeyValue::StringBuffer(data),
                        ]))
                    }
                    Err(err) => Err(ValkeyError::Str(err.as_str())),
                }
            } else {
                // Already done, return empty
                Ok(ValkeyValue::Array(vec![
                    ValkeyValue::Integer(0),
                    ValkeyValue::StringBuffer(vec![]),
                ]))
            }
        }
        None => Err(ValkeyError::Str("ERR not found")),
    }
}

/// Implements CF.LOADCHUNK command.
/// Restores a cuckoo filter previously saved with CF.SCANDUMP.
/// Receives iterator and data chunk.
pub fn cuckoo_filter_loadchunk(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc != 4 {
        return Err(ValkeyError::WrongArity);
    }

    // Parse key name
    let key_name = &args[1];
    // Parse iterator
    let iter_val = match args[2].to_string_lossy().parse::<i64>() {
        Ok(num) => num,
        _ => return Err(ValkeyError::Str("ERR bad iterator")),
    };
    // Parse data chunk
    let data = args[3].as_slice();

    if iter_val != 0 {
        return Err(ValkeyError::Str("ERR invalid iterator"));
    }

    // For one-shot deserialization (iter == 0)
    if data.is_empty() {
        return VALKEY_OK; // Empty data, nothing to load
    }

    // Deserialize the CuckooObject
    let cuckoo = match CuckooObject::decode_object(data, true) {
        Ok(cf) => cf,
        Err(err) => return Err(ValkeyError::Str(err.as_str())),
    };

    // Open key for writing
    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    // Check if key already exists
    if value.is_some() {
        return Err(ValkeyError::Str("ERR item exists"));
    }

    // Set the value
    match filter_key.set_value(&CUCKOO_TYPE, cuckoo) {
        Ok(()) => {
            // Replicate the command
            ctx.replicate_verbatim();
            // Notify keyspace event
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.loadchunk", key_name);
            VALKEY_OK
        }
        Err(_) => Err(ValkeyError::Str("ERR failed to set cuckoo filter")),
    }
}

/// Implements CF.LOAD command for AOF operations.
/// Loads a complete cuckoo filter from serialized data.
/// Similar to BF.LOAD, used for persistence.
pub fn cuckoo_filter_load(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc != 3 {
        return Err(ValkeyError::WrongArity);
    }

    // Parse key name
    let key_name = &args[1];
    // Parse serialized filter data
    let data = args[2].as_slice();

    // Deserialize the CuckooObject
    let cuckoo = match CuckooObject::decode_object(data, true) {
        Ok(cf) => cf,
        Err(err) => return Err(ValkeyError::Str(err.as_str())),
    };

    // Open key for writing
    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::WrongType);
        }
    };

    // Check if key already exists
    if value.is_some() {
        return Err(ValkeyError::Str("ERR item exists"));
    }

    // Set the value
    match filter_key.set_value(&CUCKOO_TYPE, cuckoo) {
        Ok(()) => {
            // Replicate the command
            ctx.replicate_verbatim();
            // Notify keyspace event
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "cuckoo.load", key_name);
            VALKEY_OK
        }
        Err(_) => Err(ValkeyError::Str("ERR failed to set cuckoo filter")),
    }
}
