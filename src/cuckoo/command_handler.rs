use crate::configs;
use crate::cuckoo::data_type::CUCKOO_TYPE;
use crate::cuckoo::utils::{
    self, CuckooObject, ADD_EVENT, BAD_BUCKET_SIZE, BAD_CAPACITY, BAD_EXPANSION,
    BAD_MAX_ITERATIONS, BUCKET_SIZE_ARG_REQUIRED, BUCKET_SIZE_OUT_OF_RANGE, CAPACITY_ARG_REQUIRED,
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
    utils::validate_capacity(capacity).map_err(|_| ValkeyError::Str(CAPACITY_OUT_OF_RANGE))
}

fn validate_bucket_size(bucket_size: i64) -> Result<(), ValkeyError> {
    let bucket_size =
        usize::try_from(bucket_size).map_err(|_| ValkeyError::Str(BUCKET_SIZE_OUT_OF_RANGE))?;
    utils::validate_bucket_size(bucket_size).map_err(|_| ValkeyError::Str(BUCKET_SIZE_OUT_OF_RANGE))
}

fn validate_max_kicks(max_kicks: i64) -> Result<(), ValkeyError> {
    let max_kicks =
        u32::try_from(max_kicks).map_err(|_| ValkeyError::Str(MAX_KICKS_OUT_OF_RANGE))?;
    utils::validate_max_kicks(max_kicks).map_err(|_| ValkeyError::Str(MAX_KICKS_OUT_OF_RANGE))
}

// Creation always carries every property, even if the first insertion fails.
fn replicate_creation(
    ctx: &Context,
    key: &ValkeyString,
    capacity: i64,
    bucket_size: usize,
    max_kicks: u32,
    expansion: u32,
) {
    let values: Vec<ValkeyString> = [
        capacity.to_string(),
        "BUCKETSIZE".into(),
        bucket_size.to_string(),
        "MAXITERATIONS".into(),
        max_kicks.to_string(),
        "EXPANSION".into(),
        expansion.to_string(),
    ]
    .iter()
    .map(|s| ValkeyString::create_from_slice(std::ptr::null_mut(), s.as_bytes()))
    .collect();
    let mut command = vec![key];
    command.extend(values.iter());
    ctx.replicate("CF.RESERVE", command.as_slice());
}

// Replay actual batch insertions before the first error, omitting skipped NX items.
// Replaying the whole command could add items the primary did not store.
fn replicate_items(ctx: &Context, args: &[ValkeyString], item_idx: usize, response: &ValkeyResult) {
    let nocreate = ValkeyString::create_from_slice(std::ptr::null_mut(), b"NOCREATE");
    let items = ValkeyString::create_from_slice(std::ptr::null_mut(), b"ITEMS");
    let mut command = Vec::with_capacity(3 + args.len() - item_idx);
    command.extend([&args[1], &nocreate, &items]);
    match response {
        Ok(ValkeyValue::Array(values)) => command.extend(
            values
                .iter()
                .zip(&args[item_idx..])
                .filter_map(|(v, item)| matches!(v, ValkeyValue::Integer(1)).then_some(item)),
        ),
        _ => return,
    }
    if command.len() > 3 {
        ctx.replicate("CF.INSERT", command.as_slice());
    }
}

#[derive(Default)]
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
    let mut options = InsertOptions::default();

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

// All insertion commands share creation, partial success, replication and events.
fn insert_items(
    ctx: &Context,
    args: &[ValkeyString],
    options: InsertOptions,
    item_idx: usize,
    multi: bool,
    nx_mode: bool,
    event: &str,
) -> ValkeyResult {
    let validate_size_limit = !must_obey_client(ctx);
    let key_name = &args[1];
    let filter_key = ctx.open_key_writable(key_name);
    let value = filter_key
        .get_value::<CuckooObject>(&CUCKOO_TYPE)
        .map_err(|_| ValkeyError::WrongType)?;
    let apply = |cuckoo: &mut CuckooObject| -> ValkeyResult {
        let mut result = Vec::with_capacity(if multi { args.len() - item_idx } else { 0 });
        for item in &args[item_idx..] {
            let added = if nx_mode {
                cuckoo.add_item_nx(item.as_slice(), validate_size_limit)
            } else {
                cuckoo.add_item(item.as_slice(), validate_size_limit)
            };
            if !multi {
                return added
                    .map(ValkeyValue::Integer)
                    .map_err(|err| ValkeyError::Str(err.as_str()));
            }
            match added {
                Ok(value) => result.push(ValkeyValue::Integer(value)),
                Err(err) => {
                    result.push(ValkeyValue::StaticError(err.as_str()));
                    break;
                }
            }
        }
        Ok(ValkeyValue::Array(result))
    };
    let response = if let Some(cuckoo) = value {
        apply(cuckoo)
    } else {
        if options.nocreate {
            return Err(ValkeyError::Str(NOT_FOUND));
        }
        let capacity = options
            .capacity
            .unwrap_or_else(|| configs::CUCKOO_CAPACITY.load(Ordering::Relaxed));
        let bucket_size = options
            .bucket_size
            .map(usize::from)
            .unwrap_or_else(|| configs::CUCKOO_BUCKET_SIZE.load(Ordering::Relaxed) as usize);
        let max_kicks = options
            .max_kicks
            .unwrap_or_else(|| configs::CUCKOO_MAX_KICKS.load(Ordering::Relaxed) as u32);
        let expansion = configs::CUCKOO_EXPANSION.load(Ordering::Relaxed) as u32;
        let mut cuckoo = CuckooObject::new_reserved(
            capacity,
            bucket_size,
            max_kicks,
            expansion,
            validate_size_limit,
        )
        .map_err(|err| ValkeyError::Str(err.as_str()))?;
        let response = apply(&mut cuckoo);
        filter_key
            .set_value(&CUCKOO_TYPE, cuckoo)
            .map_err(|_| ValkeyError::Str(FAILED_TO_SET_FILTER))?;
        replicate_creation(ctx, key_name, capacity, bucket_size, max_kicks, expansion);
        ctx.notify_keyspace_event(NotifyEvent::MODULE, CREATE_EVENT, key_name);
        response
    };
    let changed = match &response {
        Ok(ValkeyValue::Integer(1)) => true,
        Ok(ValkeyValue::Array(values)) => {
            values.iter().any(|v| matches!(v, ValkeyValue::Integer(1)))
        }
        _ => false,
    };
    if changed {
        if multi {
            replicate_items(ctx, args, item_idx, &response);
        } else {
            // Preserve CF.ADD/CF.ADDNX keyspace events on replicas.
            ctx.replicate_verbatim();
        }
        ctx.notify_keyspace_event(NotifyEvent::MODULE, event, key_name);
    }
    response
}

/// Implements CF.ADD command.
pub fn cuckoo_filter_add_value(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    insert_items(
        ctx,
        &args,
        InsertOptions::default(),
        2,
        false,
        false,
        ADD_EVENT,
    )
}

/// Implements CF.ADDNX command.
pub fn cuckoo_filter_addnx(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }
    insert_items(
        ctx,
        &args,
        InsertOptions::default(),
        2,
        false,
        true,
        ADD_EVENT,
    )
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

    let (options, items_idx) = parse_insert_options(&args, 2)?;
    if items_idx >= argc {
        return Err(ValkeyError::Str(NO_ITEMS_SPECIFIED));
    }
    insert_items(ctx, &args, options, items_idx, true, nx_mode, INSERT_EVENT)
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
                    Ok(num)
                        if (configs::CUCKOO_EXPANSION_MIN as i64
                            ..=configs::CUCKOO_EXPANSION_MAX as i64)
                            .contains(&num) =>
                    {
                        num
                    }
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
                    replicate_creation(
                        ctx,
                        key_name,
                        capacity,
                        bucket_size as usize,
                        max_kicks as u32,
                        expansion as u32,
                    );
                    ctx.notify_keyspace_event(NotifyEvent::MODULE, RESERVE_EVENT, key_name);
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str(FAILED_TO_SET_FILTER)),
            }
        }
    }
}

// Keep response labels and accessors shared by full and single-field replies.
type InfoField = (&'static str, fn(&CuckooObject) -> i64);
const INFO_FIELDS: [InfoField; 8] = [
    ("Size", |c| c.memory_usage() as i64),
    ("Number of buckets", |c| {
        c.filters().iter().map(|f| f.bucket_count() as i64).sum()
    }),
    ("Number of items inserted", CuckooObject::num_items),
    ("Number of items deleted", CuckooObject::num_deleted),
    ("Number of filters", |c| c.num_filters() as i64),
    ("Bucket size", |c| c.bucket_size() as i64),
    ("Max iterations", |c| c.max_kicks() as i64),
    ("Expansion rate", |c| c.expansion() as i64),
];

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
                let field_name = args[2].to_string_lossy();
                return INFO_FIELDS
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(&field_name))
                    .map(|(_, value)| ValkeyValue::Integer(value(cuckoo)))
                    .ok_or(ValkeyError::Str(UNKNOWN_OPTION));
            }
            let result = INFO_FIELDS
                .iter()
                .flat_map(|(name, value)| {
                    [
                        ValkeyValue::SimpleStringStatic(name),
                        ValkeyValue::Integer(value(cuckoo)),
                    ]
                })
                .collect();
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

    let filter_key = ctx.open_key_writable(key_name);
    let value = match filter_key.get_value::<CuckooObject>(&CUCKOO_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    if value.is_some() {
        return Err(ValkeyError::Str(ITEM_EXISTS));
    }

    let cuckoo = match CuckooObject::decode_object(data, !must_obey_client(ctx)) {
        Ok(cf) => cf,
        Err(err) => return Err(ValkeyError::Str(err.as_str())),
    };

    match filter_key.set_value(&CUCKOO_TYPE, cuckoo) {
        Ok(()) => {
            ctx.replicate_verbatim();
            ctx.notify_keyspace_event(NotifyEvent::MODULE, LOAD_EVENT, key_name);
            VALKEY_OK
        }
        Err(_) => Err(ValkeyError::Str(FAILED_TO_SET_FILTER)),
    }
}
