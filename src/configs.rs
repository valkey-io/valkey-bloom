use crate::bloom::utils;
use lazy_static::lazy_static;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Mutex;
use valkey_module::{
    configuration::ConfigurationContext, ConfigurationValue, ValkeyError, ValkeyGILGuard,
    ValkeyString,
};

/// Configurations
pub const BLOOM_CAPACITY_DEFAULT: i64 = 100;
pub const BLOOM_CAPACITY_MIN: i64 = 1;
pub const BLOOM_CAPACITY_MAX: i64 = i64::MAX;

pub const BLOOM_EXPANSION_DEFAULT: i64 = 2;
pub const BLOOM_EXPANSION_MIN: u32 = 1;
pub const BLOOM_EXPANSION_MAX: u32 = u32::MAX;

pub const BLOOM_FP_RATE_DEFAULT: &str = "0.01";
pub const BLOOM_FP_RATE_MIN: f64 = 0.0;
pub const BLOOM_FP_RATE_MAX: f64 = 1.0;

// Tightening ratio used during scale out for the calculation of fp_rate of every new filter within a bloom object to
// maintain the bloom object's overall fp_rate to the configured value.
pub const TIGHTENING_RATIO_DEFAULT: &str = "0.5";
pub const BLOOM_TIGHTENING_RATIO_MIN: f64 = 0.0;
pub const BLOOM_TIGHTENING_RATIO_MAX: f64 = 1.0;

pub const BLOOM_USE_RANDOM_SEED_DEFAULT: bool = true;

pub const BLOOM_DEFRAG_DEAFULT: bool = true;

// Max Memory usage allowed overall within a bloom object (128MB).
// Beyond this threshold, a bloom object is classified as large.
// Write operations that result in bloom object allocation larger than this size will be rejected.
pub const BLOOM_MEMORY_LIMIT_PER_OBJECT_DEFAULT: i64 = 128 * 1024 * 1024;
pub const BLOOM_MEMORY_LIMIT_PER_OBJECT_MIN: i64 = 0;
pub const BLOOM_MEMORY_LIMIT_PER_OBJECT_MAX: i64 = i64::MAX;

lazy_static! {
    pub static ref BLOOM_CAPACITY: AtomicI64 = AtomicI64::new(BLOOM_CAPACITY_DEFAULT);
    pub static ref BLOOM_EXPANSION: AtomicI64 = AtomicI64::new(BLOOM_EXPANSION_DEFAULT);
    pub static ref BLOOM_MEMORY_LIMIT_PER_OBJECT: AtomicI64 =
        AtomicI64::new(BLOOM_MEMORY_LIMIT_PER_OBJECT_DEFAULT);
    pub static ref BLOOM_USE_RANDOM_SEED: AtomicBool = AtomicBool::default();
    pub static ref BLOOM_DEFRAG: AtomicBool = AtomicBool::new(BLOOM_DEFRAG_DEAFULT);
    pub static ref BLOOM_FP_RATE_F64: Mutex<f64> = Mutex::new(
        BLOOM_FP_RATE_DEFAULT
            .parse::<f64>()
            .expect("Expected valid f64 for fp rate.")
    );
    pub static ref BLOOM_FP_RATE: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, BLOOM_FP_RATE_DEFAULT));
    pub static ref BLOOM_TIGHTENING_F64: Mutex<f64> = Mutex::new(
        TIGHTENING_RATIO_DEFAULT
            .parse::<f64>()
            .expect("Expected valid f64 for tightening ratio.")
    );
    pub static ref BLOOM_TIGHTENING_RATIO: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, TIGHTENING_RATIO_DEFAULT));
}

/// Constants
// Max number of filters allowed within a bloom object.
pub const BLOOM_NUM_FILTERS_PER_OBJECT_LIMIT_MAX: i32 = i32::MAX;
/// Below constants are fixed seed and sip keys to help create bloom objects using the same seed and to restore the bloom objects with the same hasher which
/// generated using rust crate bloomfilter https://crates.io/crates/bloomfilter
pub const FIXED_SEED: [u8; 32] = [
    89, 15, 245, 34, 234, 120, 17, 218, 167, 20, 216, 9, 59, 62, 123, 217, 29, 137, 138, 115, 62,
    152, 136, 135, 48, 127, 151, 205, 40, 7, 51, 131,
];

/// This is a config set handler for the False Positive Rate and Tightening Ratio configs.
pub fn on_string_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx);
    let value_str = v.to_string_lossy();
    let value = match value_str.parse::<f64>() {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str("Invalid floating-point value"));
        }
    };
    match name {
        "bloom-fp-rate" => {
            if !(value > BLOOM_FP_RATE_MIN && value < BLOOM_FP_RATE_MAX) {
                return Err(ValkeyError::Str(utils::ERROR_RATE_RANGE));
            }
            let mut fp_rate = BLOOM_FP_RATE_F64
                .lock()
                .expect("We expect the fp_rate static to exist.");
            *fp_rate = value;
            Ok(())
        }
        "bloom-tightening-ratio" => {
            if !(value > BLOOM_TIGHTENING_RATIO_MIN && value < BLOOM_TIGHTENING_RATIO_MAX) {
                return Err(ValkeyError::Str(utils::TIGHTENING_RATIO_RANGE));
            }
            let mut tightening = BLOOM_TIGHTENING_F64
                .lock()
                .expect("We expect the tightening_ratio static to exist.");
            *tightening = value;
            Ok(())
        }
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}
