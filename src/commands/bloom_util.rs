use crate::commands::bloom_data_type::{
    BloomFilterType, BLOOM_FILTER_TYPE,
};
use redis_module::key::RedisKeyWritable;
use redis_module::RedisError;

pub const ERROR: &str = "ERROR";


pub fn bloom_create_new_item(
    key: &RedisKeyWritable,
    bitmap: Vec<u8>,
    number_of_bits: u64,
    number_of_hash_functions: u32,
    sip_keys: [(u64, u64); 2],
) -> Result<(), RedisError> {
    let _bitmap_len = bitmap.len();
    let value = BloomFilterType {
        bitmap,
        number_of_bits,
        number_of_hash_functions,
        sip_key_one_a: sip_keys[0].0,
        sip_key_one_b: sip_keys[0].1,
        sip_key_two_a: sip_keys[1].0,
        sip_key_two_b: sip_keys[1].1,
        num_items: 1,
    };
    match key.set_value(&BLOOM_FILTER_TYPE, value) {
        Ok(_v) => {
            Ok(())
        }
        Err(_e) => Err(RedisError::Str("ERROR")),
    }
}

pub fn bloom_update_filter(
    v: &mut BloomFilterType,
    _old_val_len: usize,
    bitmap: Vec<u8>,
    number_of_bits: u64,
    number_of_hash_functions: u32,
    sip_keys: [(u64, u64); 2],
) {
    let _bitmap_len = bitmap.len();
    v.bitmap = bitmap;
    v.number_of_bits = number_of_bits;
    v.number_of_hash_functions = number_of_hash_functions;
    v.sip_key_one_a = sip_keys[0].0;
    v.sip_key_one_b = sip_keys[0].1;
    v.sip_key_two_a = sip_keys[1].0;
    v.sip_key_two_b = sip_keys[1].1;
    v.num_items += 1;
}
