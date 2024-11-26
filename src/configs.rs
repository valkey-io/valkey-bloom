use lazy_static::lazy_static;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;

/// Configurations
pub const BLOOM_CAPACITY_DEFAULT: i64 = 100000;
pub const BLOOM_CAPACITY_MIN: u32 = 1;
pub const BLOOM_CAPACITY_MAX: u32 = u32::MAX;

pub const BLOOM_EXPANSION_DEFAULT: i64 = 2;
pub const BLOOM_EXPANSION_MIN: u32 = 1;
pub const BLOOM_EXPANSION_MAX: u32 = 10;

pub const BLOOM_FP_RATE_DEFAULT: f64 = 0.001;
pub const BLOOM_FP_RATE_MIN: f64 = 0.0;
pub const BLOOM_FP_RATE_MAX: f64 = 1.0;

pub const BLOOM_USE_RANDOM_SEED_DEFAULT: bool = true;

// Max Memory usage allowed per bloom filter within a bloom object (64MB).
// Beyond this threshold, a bloom object is classified as large and is exempt from defrag operations.
// Also, write operations that result in bloom object allocation larger than this size will be rejected.
pub const BLOOM_MEMORY_LIMIT_PER_FILTER_DEFAULT: i64 = 64 * 1024 * 1024;
pub const BLOOM_MEMORY_LIMIT_PER_FILTER_MIN: i64 = 0;
pub const BLOOM_MEMORY_LIMIT_PER_FILTER_MAX: i64 = i64::MAX;

lazy_static! {
    pub static ref BLOOM_CAPACITY: AtomicI64 = AtomicI64::new(BLOOM_CAPACITY_DEFAULT);
    pub static ref BLOOM_EXPANSION: AtomicI64 = AtomicI64::new(BLOOM_EXPANSION_DEFAULT);
    pub static ref BLOOM_MEMORY_LIMIT_PER_FILTER: AtomicI64 =
        AtomicI64::new(BLOOM_MEMORY_LIMIT_PER_FILTER_DEFAULT);
    pub static ref BLOOM_USE_RANDOM_SEED: AtomicBool = AtomicBool::default();
}

/// Constants
// Tightening ratio used during scale out for the calculation of fp_rate of every new filter within a bloom object to
// maintain the bloom object's overall fp_rate to the configured value.
pub const TIGHTENING_RATIO: f64 = 0.5;
// Max number of filters allowed within a bloom object.
pub const MAX_FILTERS_PER_OBJ: i32 = i32::MAX;
/// Below constants are fixed seed and sip keys to help create bloom objects using the same seed and to restore the bloom objects with the same hasher which
/// generated using rust crate bloomfilter https://crates.io/crates/bloomfilter
pub const FIXED_SEED: [u8; 32] = [
    89, 15, 245, 34, 234, 120, 17, 218, 167, 20, 216, 9, 59, 62, 123, 217, 29, 137, 138, 115, 62,
    152, 136, 135, 48, 127, 151, 205, 40, 7, 51, 131,
];
