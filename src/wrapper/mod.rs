use valkey_module::Context;

pub mod bloom_callback;

/// Wrapper for the ValkeyModule_MustObeyClient function.
/// Takes in an Context and returns true if the if commands are arriving
/// from the primary client or AOF client and should never be rejected.
/// False otherwise.
pub fn must_obey_client(ctx: &Context) -> bool {
    #[cfg(not(feature = "valkey_8_0"))]
    {
        // ValkeyModule_MustObeyClient exists since Valkey 8.1, so we use this as it is a more performant check.
        let ctx_raw = ctx.get_raw() as *mut valkey_module::ValkeyModuleCtx;

        let status = unsafe { valkey_module::raw::ValkeyModule_MustObeyClient.unwrap()(ctx_raw) };
        match status {
            1 => true,
            0 => false,
            _ => panic!("We do not expect ValkeyModule_MustObeyClient to return anything other than 1 or 0."),
        }
    }

    #[cfg(feature = "valkey_8_0")]
    {
        // On Valkey 8.0, fallback to checking for replicated flag in the GetContextFlags API as a best effort.
        ctx.get_flags()
            .contains(valkey_module::ContextFlags::REPLICATED)
    }
}
