use valkey_module::{Context, ContextFlags};

pub mod bloom_callback;
pub mod defrag;

/// Wrapper for the ValkeyModule_MustObeyClient function.
/// Takes in an Context and returns true if the if commands are arriving
/// from the primary client or AOF client and should never be rejected.
/// False otherwise.
pub fn must_obey_client(ctx: &Context) -> bool {
    // If we are using valkey 8.0 then we cannot use ValkeyModule_MustObeyClient so must go back to the default
    // of checking for the replicated flag
    #[cfg(not(feature = "valkey_8_0"))]
    {
        let ctx_raw = ctx.get_raw() as *mut valkey_module::ValkeyModuleCtx;

        match unsafe { valkey_module::raw::ValkeyModule_MustObeyClient } {
            Some(func) => {
                let status = unsafe { func(ctx_raw) as isize };
                match status {
                    1 => true,
                    0 => false,
                    _ => panic!("We do not expect ValkeyModule_MustObeyClient to return anything other than 1 or 0."),
                }
            }
            // Fallback to checking for replicated flag in the GetContextFlags API as a best effort.
            None => ctx.get_flags().contains(ContextFlags::REPLICATED),
        }
    }

    #[cfg(feature = "valkey_8_0")]
    {
        ctx.get_flags().contains(ContextFlags::REPLICATED)
    }
}
