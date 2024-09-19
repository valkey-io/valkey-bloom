use lazy_static::lazy_static;
use std::sync::atomic::AtomicI64;

/// Configurations
pub const BLOOM_CAPACITY_DEFAULT: i64 = 100000;
pub const BLOOM_CAPACITY_MIN: u32 = 1;
pub const BLOOM_CAPACITY_MAX: u32 = u32::MAX;

pub const BLOOM_EXPANSION_DEFAULT: i64 = 2;
pub const BLOOM_EXPANSION_MIN: u32 = 1;
pub const BLOOM_EXPANSION_MAX: u32 = 10;

pub const BLOOM_FP_RATE_DEFAULT: f32 = 0.001;
pub const BLOOM_FP_RATE_MIN: f32 = 0.0;
pub const BLOOM_FP_RATE_MAX: f32 = 1.0;

lazy_static! {
    pub static ref BLOOM_CAPACITY: AtomicI64 = AtomicI64::new(BLOOM_CAPACITY_DEFAULT);
    pub static ref BLOOM_EXPANSION: AtomicI64 = AtomicI64::new(BLOOM_EXPANSION_DEFAULT);
}

/// Constants
pub const TIGHTENING_RATIO: f32 = 0.5;
pub const MAX_FILTERS_PER_OBJ: i32 = i32::MAX;
/// Below constants are fixed seed and sip keys to help create bloom objects using the same seed and to restore the bloom objects with the same hasher which
/// generated using rust crate bloomfilter https://crates.io/crates/bloomfilter
pub const FIXED_SEED: [u8; 32] = [
    89, 15, 245, 34, 234, 120, 17, 218, 167, 20, 216, 9, 59, 62, 123, 217, 29, 137, 138, 115, 62,
    152, 136, 135, 48, 127, 151, 205, 40, 7, 51, 131,
];
pub const FIXED_SIP_KEY_ONE_A: u64 = 15713473521876537177;
pub const FIXED_SIP_KEY_ONE_B: u64 = 15671187751654921383;
pub const FIXED_SIP_KEY_TWO_A: u64 = 9766223185946773789;
pub const FIXED_SIP_KEY_TWO_B: u64 = 9453907914610147120;
