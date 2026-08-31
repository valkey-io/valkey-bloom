use valkey_module::{
    key::ValkeyKey, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
    VALKEY_OK,
};

use crate::cms::data_type::CMS_TYPE;
use crate::cms::utils::{self, CMSObject};

enum Replications {
    ReplicateArgsDim { width: u64, depth: u64 },
    ReplicateArgsProb { error_rate: f64, fp_rate: f64 },
}

enum Operation {
    Initialization { replications: Replications },
    Increment,
    Merge,
}

fn replicate_and_notify_events(ctx: &Context, key_name: &ValkeyString, operation: Operation) {
    match operation {
        Operation::Initialization { replications } => match replications {
            Replications::ReplicateArgsDim { width, depth } => {
                let width_val = ValkeyString::create_from_slice(
                    std::ptr::null_mut(),
                    width.to_string().as_bytes(),
                );
                let depth_val = ValkeyString::create_from_slice(
                    std::ptr::null_mut(),
                    depth.to_string().as_bytes(),
                );
                let cmd = vec![&key_name, &width_val, &depth_val];
                ctx.replicate("CMS.INITBYDIM", cmd.as_slice());
                ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::INITBYDIM_EVENT, key_name);
            }
            Replications::ReplicateArgsProb {
                error_rate,
                fp_rate,
            } => {
                let error_val = ValkeyString::create_from_slice(
                    std::ptr::null_mut(),
                    error_rate.to_string().as_bytes(),
                );
                let fp_val = ValkeyString::create_from_slice(
                    std::ptr::null_mut(),
                    fp_rate.to_string().as_bytes(),
                );
                let cmd = vec![&key_name, &error_val, &fp_val];
                ctx.replicate("CMS.INITBYPROB", cmd.as_slice());
                ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::INITBYPROB_EVENT, key_name);
            }
        },
        Operation::Increment => {
            ctx.replicate_verbatim();
            ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::INCR_EVENT, key_name);
        }
        Operation::Merge => {
            //TODO::How should this replication be done, and why vs verbatim
            ctx.replicate_verbatim();
            ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::MERGE_EVENT, key_name);
        }
    }
}

/// Function that implements logic to handle the CMS.INITBYDIM command.
pub fn cms_initialize_by_dimensions(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let args_count = args.len();
    if args_count != 4 {
        return Err(valkey_module::ValkeyError::WrongArity);
    }

    let key = &args[1];

    let width = match args[2].to_string_lossy().parse::<u64>() {
        Ok(w) if w > 0 => w,
        _ => return Err(ValkeyError::Str(utils::BAD_WIDTH)),
    };

    let depth = match args[3].to_string_lossy().parse::<u64>() {
        Ok(d) if d > 0 => d,
        _ => return Err(ValkeyError::Str(utils::BAD_DEPTH)),
    };

    let filter_key = ctx.open_key_writable(key);
    let cms = match filter_key.get_value::<CMSObject>(&CMS_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match cms {
        Some(_) => Err(ValkeyError::Str(utils::ITEM_EXISTS)),
        None => {
            let cms = match utils::CMSObject::new_by_dimension(width, depth) {
                Ok(v) => v,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };

            match filter_key.set_value(&CMS_TYPE, cms) {
                Ok(()) => {
                    let replications = Replications::ReplicateArgsDim { width, depth };
                    replicate_and_notify_events(
                        ctx,
                        key,
                        Operation::Initialization { replications },
                    );
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}

/// Function that implements logic to handle the CMS.INITBYPROB command.
pub fn cms_initialize_by_probability(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let args_count = args.len();
    if args_count != 4 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];

    // Maximum allowable error rate.  Epsilon
    // For an epsilon of 1% and a estimated count of 1000 the actual could be between 990 and 1010
    let error_rate = match args[2].to_string_lossy().parse::<f64>() {
        Ok(e) if e > 0.0 && e < 1.0 => e,
        Ok(_) => return Err(ValkeyError::Str(utils::ERROR_RATE_RANGE)),
        Err(_) => return Err(ValkeyError::Str(utils::BAD_ERROR_RATE)),
    };

    //False positive rate. Delta
    // A delta of 1% means the count will be outside of the epsilon range 1% of the time.
    let fp_rate = match args[3].to_string_lossy().parse::<f64>() {
        Ok(p) if p > 0.0 && p < 1.0 => p,
        Ok(_) => return Err(ValkeyError::Str(utils::PROBABILITY_RANGE)),
        Err(_) => return Err(ValkeyError::Str(utils::BAD_PROBABILITY)),
    };

    let filter_key = ctx.open_key_writable(key);
    let cms = match filter_key.get_value::<CMSObject>(&CMS_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match cms {
        Some(_) => Err(ValkeyError::Str(utils::ITEM_EXISTS)),
        None => {
            let cms = match utils::CMSObject::new_by_probability(error_rate, fp_rate) {
                Ok(v) => v,
                Err(err) => return Err(ValkeyError::Str(err.as_str())),
            };

            match filter_key.set_value(&CMS_TYPE, cms) {
                Ok(()) => {
                    let replications = Replications::ReplicateArgsProb {
                        error_rate,
                        fp_rate,
                    };

                    replicate_and_notify_events(
                        ctx,
                        key,
                        Operation::Initialization { replications },
                    );
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}

/// Function that implements logic to handle the CMS.INCRBY command.
pub fn cms_increment_by(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let args_count = args.len();
    if args_count < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let key = &args[1];

    let args_left = args_count - 2;
    let is_even = args_left % 2 == 0;
    if !is_even {
        return Err(ValkeyError::WrongArity);
    }

    let mut i = 2;
    let mut pairs: Vec<(&[u8], u64)> = Vec::new();
    while i < args_count {
        let k = args[i].as_slice();
        let v = args[i + 1]
            .parse_unsigned_integer()
            .map_err(|_| ValkeyError::Str(utils::BAD_INCREMENT))?;
        pairs.push((k, v));
        i += 2
    }

    let filter_key = ctx.open_key_writable(key);
    let value = match filter_key.get_value::<CMSObject>(&CMS_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    let mut results = Vec::new();
    match value {
        None => Err(ValkeyError::nonexistent_key()),
        Some(v) => {
            for (item, increment) in pairs {
                let count = v.increment_by(item, increment);
                results.push(ValkeyValue::Integer(count as i64));
            }
            replicate_and_notify_events(ctx, key, Operation::Increment);
            Ok(ValkeyValue::Array(results))
        }
    }
}

/// Function that implements logic to handle the CMS.QUERY command.
pub fn cms_query(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let argc = args.len();
    if argc < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = &args[1];

    let existing_key = ctx.open_key(key_name);
    let cms = match existing_key.get_value::<CMSObject>(&CMS_TYPE) {
        Ok(v) => v,
        Err(_) => return Err(ValkeyError::WrongType),
    };

    match cms {
        None => Err(ValkeyError::nonexistent_key()),
        Some(v) => {
            let estimates: Vec<ValkeyValue> = args[2..]
                .iter()
                .map(|item| {
                    let estimate = v.estimate(&item);
                    ValkeyValue::Integer(estimate as i64)
                })
                .collect();
            Ok(ValkeyValue::Array(estimates))
        }
    }
}

/// Function that implements logic to handle the CMS.MERGE command.
pub fn cms_merge(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let args_count = args.len();
    if args_count < 5 {
        return Err(ValkeyError::WrongArity);
    }

    //This must already be initialized.
    let destination_key = &args[1];

    let number_of_keys_value = args[2]
        .to_string_lossy()
        .parse::<usize>()
        .map_err(|_| ValkeyError::Str("ERR invalid number of keys value"))?;

    //Indexes 3 -> 3 + N-1 are keys to merge
    let sketch_end_index = 3 + number_of_keys_value - 1;
    //Up to non inclusive grab 3 up to sketch_end_index
    let source_keys: Vec<&ValkeyString> = args[3..=sketch_end_index].iter().collect();

    //Then Parse the optional WEIGHTS section of the command.  WEIGHTS is at sketch_end + 1
    let passed_in_weights = if sketch_end_index + 1 == args_count {
        Vec::new()
    } else {
        //Make sure we have at least WEIGHT weight left
        let weight_args_left = args_count - sketch_end_index - 1;
        if weight_args_left < 2 {
            return Err(ValkeyError::WrongArity);
        }

        //There should be at least 2 args left WEIGHT weight [weight ...]
        let weights_keyword_index = sketch_end_index + 1;
        let weights_keyword = args[weights_keyword_index].to_string_lossy();
        if weights_keyword.to_uppercase() != "WEIGHTS" {
            return Err(ValkeyError::Str("ERR invalid argument"));
        }
        let weights_start = weights_keyword_index + 1;
        let weights_args = args[weights_start..].iter();

        let weights: Vec<f64> = weights_args
            .map(|weight| {
                weight
                    .parse_float()
                    .map_err(|_| ValkeyError::Str("ERR invalid weight value"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        weights
    };

    let source_key_handles: Vec<ValkeyKey> =
        source_keys.iter().map(|key| ctx.open_key(key)).collect();
    let sketches = source_key_handles
        .iter()
        .map(|key_handle| {
            key_handle
                .get_value::<CMSObject>(&CMS_TYPE)
                .and_then(|opt| opt.ok_or_else(|| ValkeyError::Str("ERR key does not exist")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let destination_sketch = ctx
        .open_key_writable(destination_key)
        .get_value::<CMSObject>(&CMS_TYPE)
        .and_then(|opt| opt.ok_or_else(|| ValkeyError::Str("ERR key does not exist")))?;

    let sketches_with_weights: Vec<(&CMSObject, f64)> = sketches
        .into_iter()
        .enumerate()
        .map(|(i, sketch)| {
            let weight = passed_in_weights.get(i).copied().unwrap_or(1.0);
            (sketch, weight)
        })
        .collect();

    //Mutates the destination_sketch's internal CMS to be the merge of the sketches_with_weights
    //Impl note:  We do not handle the weights yet in the called function, as the lib-source needs to change.
    destination_sketch
        .merge(&sketches_with_weights)
        .map_err(|_| {
            ValkeyError::Str("ERR destination key is not of the same width and/or depth")
        })?;

    replicate_and_notify_events(ctx, destination_key, Operation::Merge);
    VALKEY_OK
}
