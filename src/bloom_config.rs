use lazy_static::lazy_static;
use std::sync::atomic::AtomicI64;

pub const BLOOM_MAX_ITEM_SIZE_DEFAULT: i64 = 10000; // Default item size is 1MB
pub const BLOOM_MAX_ITEM_SIZE_MIN: i64 = 1; // Minumim Item size is 1KB
pub const BLOOM_MAX_ITEM_SIZE_MAX: i64 = 1000000; // Maximum item size is 128MB

lazy_static! {
    pub static ref BLOOM_MAX_ITEM_SIZE: AtomicI64 = AtomicI64::new(BLOOM_MAX_ITEM_SIZE_DEFAULT);
}
