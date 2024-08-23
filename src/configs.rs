use lazy_static::lazy_static;
use std::sync::atomic::AtomicI64;

/// Configurations
pub const BLOOM_CAPACITY_DEFAULT: i64 = 100000;
pub const BLOOM_CAPACITY_MIN: i64 = 1;
pub const BLOOM_CAPACITY_MAX: u32 = u32::MAX;

pub const BLOOM_EXPANSION_DEFAULT: i64 = 2;
pub const BLOOM_EXPANSION_MIN: i64 = 1;
pub const BLOOM_EXPANSION_MAX: u32 = 10;

lazy_static! {
    pub static ref BLOOM_CAPACITY: AtomicI64 = AtomicI64::new(BLOOM_CAPACITY_DEFAULT);
    pub static ref BLOOM_EXPANSION: AtomicI64 = AtomicI64::new(BLOOM_EXPANSION_DEFAULT);
}

/// Constants
pub const BLOOM_FP_RATE_DEFAULT: f32 = 0.001;
pub const TIGHTENING_RATIO: f32 = 0.8515625;
