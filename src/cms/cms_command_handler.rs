use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};

use crate::cms::data_type::CMS_TYPE;
use crate::cms::utils::{self, CMSObject};

struct ReplicateArgs {
    //seed: [u8; 32],
    args: Replications,
}

enum Replications {
    ReplicateArgsDim { width: u64, depth: u64 },
    ReplicateArgsProb { error_rate: f64, fp_rate: f64 },
}

fn replicate_and_notify_events(
    ctx: &Context,
    key_name: &ValkeyString,
    init_operation: bool,
    incr_operation: bool,
    args: ReplicateArgs,
) {
    if init_operation {
        //For later when we need seeding discussions (cms.merge)
        // let seed_str = ValkeyString::create_from_slice(std::ptr::null_mut(), "SEED".as_bytes());
        // let seed_val = ValkeyString::create_from_slice(std::ptr::null_mut(), &args.seed);

        match args.args {
            Replications::ReplicateArgsDim { width, depth } => {
                let width_val = ValkeyString::create_from_slice(
                    std::ptr::null_mut(),
                    width.to_string().as_bytes(),
                );
                let depth_val = ValkeyString::create_from_slice(
                    std::ptr::null_mut(),
                    depth.to_string().as_bytes(),
                );
                let cmd = vec![&width_val, &depth_val];
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
                let cmd = vec![&error_val, &fp_val];
                ctx.replicate("CMS.INITBYPROB", cmd.as_slice());
                ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::INITBYPROB_EVENT, key_name);
            }
        }
    } else if incr_operation {
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::GENERIC, utils::INCR_EVENT, key_name);
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
                    let r = Replications::ReplicateArgsDim { width, depth };
                    let replicate_args = ReplicateArgs { args: r };
                    replicate_and_notify_events(ctx, key, true, false, replicate_args);
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
                    let r = Replications::ReplicateArgsProb {
                        error_rate,
                        fp_rate,
                    };
                    let replicate_args = ReplicateArgs { args: r };
                    replicate_and_notify_events(ctx, key, true, false, replicate_args);
                    VALKEY_OK
                }
                Err(_) => Err(ValkeyError::Str(utils::ERROR)),
            }
        }
    }
}
