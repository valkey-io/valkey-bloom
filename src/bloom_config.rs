use lazy_static::lazy_static;
use std::sync::atomic::AtomicI64;

// TODO: Review min / default / max values for the configs.
// TODO: Decide if we need a config for the max number of allowed sub filters per object.
// TODO: Decide if we need default false positive rate as a config.
// TODO: Decide if we need a config for max bloom object size, beyond which we disallow defrag and
//       disallow synchronous freeing.

pub const BLOOM_FP_RATE_DEFAULT: f32 = 0.001;

pub const BLOOM_MAX_ITEM_COUNT_DEFAULT: i64 = 100000;
pub const BLOOM_MAX_ITEM_COUNT_MIN: i64 = 1;
pub const BLOOM_MAX_ITEM_COUNT_MAX: u32 = u32::MAX;

pub const BLOOM_EXPANSION_DEFAULT: i64 = 2;
pub const BLOOM_EXPANSION_MIN: i64 = 1;
pub const BLOOM_EXPANSION_MAX: u32 = 10;

lazy_static! {
    pub static ref BLOOM_MAX_ITEM_COUNT: AtomicI64 = AtomicI64::new(BLOOM_MAX_ITEM_COUNT_DEFAULT);
    pub static ref BLOOM_EXPANSION: AtomicI64 = AtomicI64::new(BLOOM_EXPANSION_DEFAULT);
}
