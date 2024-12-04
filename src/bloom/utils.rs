use crate::{
    configs::{
        self, BLOOM_EXPANSION_MAX, BLOOM_EXPANSION_MIN, BLOOM_FP_RATE_MAX, BLOOM_FP_RATE_MIN,
    },
    metrics,
};
use bloomfilter;
use bloomfilter::{deserialize, serialize};
use serde::{Deserialize, Serialize};

use super::data_type::BLOOM_TYPE_VERSION;
use std::{mem, sync::atomic::Ordering};

/// KeySpace Notification Events
pub const ADD_EVENT: &str = "bloom.add";
pub const RESERVE_EVENT: &str = "bloom.reserve";

/// Errors
pub const ERROR: &str = "ERROR";
pub const NON_SCALING_FILTER_FULL: &str = "ERR non scaling filter is full";
pub const NOT_FOUND: &str = "ERR not found";
pub const ITEM_EXISTS: &str = "ERR item exists";
pub const INVALID_INFO_VALUE: &str = "ERR invalid information value";
pub const BAD_EXPANSION: &str = "ERR bad expansion";
pub const BAD_CAPACITY: &str = "ERR bad capacity";
pub const BAD_ERROR_RATE: &str = "ERR bad error rate";
pub const ERROR_RATE_RANGE: &str = "ERR (0 < error rate range < 1)";
pub const CAPACITY_LARGER_THAN_0: &str = "ERR (capacity should be larger than 0)";
pub const MAX_NUM_SCALING_FILTERS: &str = "ERR bloom object reached max number of filters";
pub const UNKNOWN_ARGUMENT: &str = "ERR unknown argument received";
pub const EXCEEDS_MAX_BLOOM_SIZE: &str =
    "ERR operation results in filter allocation exceeding size limit";
pub const KEY_EXISTS: &str = "BUSYKEY Target key name already exists.";
pub const ENCODE_BLOOM_FILTER_FAILED: &str = "ERR encode bloom filter failed.";
pub const DECODE_BLOOM_FILTER_FAILED: &str = "ERR decode bloom filter failed.";
pub const DECODE_UNSUPPORTED_VERSION: &str =
    "ERR decode bloom filter failed.  Unsupported version.";

#[derive(Debug, PartialEq)]
pub enum BloomError {
    NonScalingFilterFull,
    MaxNumScalingFilters,
    ExceedsMaxBloomSize,
    EncodeBloomFilterFailed,
    DecodeBloomFilterFailed,
    DecodeUnsupportedVersion,
    ErrorRateRange,
    BadExpansion,
}

impl BloomError {
    pub fn as_str(&self) -> &'static str {
        match self {
            BloomError::NonScalingFilterFull => NON_SCALING_FILTER_FULL,
            BloomError::MaxNumScalingFilters => MAX_NUM_SCALING_FILTERS,
            BloomError::ExceedsMaxBloomSize => EXCEEDS_MAX_BLOOM_SIZE,
            BloomError::EncodeBloomFilterFailed => ENCODE_BLOOM_FILTER_FAILED,
            BloomError::DecodeBloomFilterFailed => DECODE_BLOOM_FILTER_FAILED,
            BloomError::DecodeUnsupportedVersion => DECODE_UNSUPPORTED_VERSION,
            BloomError::ErrorRateRange => ERROR_RATE_RANGE,
            BloomError::BadExpansion => BAD_EXPANSION,
        }
    }
}

/// The BloomFilterType structure. 40 bytes.
/// Can contain one or more filters.
/// This is a generic top level structure which is not coupled to any bloom crate.
#[derive(Serialize, Deserialize)]
pub struct BloomFilterType {
    pub expansion: u32,
    pub fp_rate: f64,
    pub filters: Vec<BloomFilter>,
}

impl BloomFilterType {
    /// Create a new BloomFilterType object.
    pub fn new_reserved(
        fp_rate: f64,
        capacity: u32,
        expansion: u32,
        validate_size_limit: bool,
    ) -> Result<BloomFilterType, BloomError> {
        // Reject the request, if the operation will result in creation of a bloom object containing a filter
        // of size greater than what is allowed.
        if validate_size_limit && !BloomFilter::validate_size(capacity, fp_rate) {
            return Err(BloomError::ExceedsMaxBloomSize);
        }
        metrics::BLOOM_NUM_OBJECTS.fetch_add(1, Ordering::Relaxed);
        metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
            mem::size_of::<BloomFilterType>(),
            std::sync::atomic::Ordering::Relaxed,
        );

        // Create the bloom filter and add to the main BloomFilter object.
        let bloom = BloomFilter::new(fp_rate, capacity);
        let filters = vec![bloom];
        let bloom = BloomFilterType {
            expansion,
            fp_rate,
            filters,
        };
        Ok(bloom)
    }

    /// Create a new BloomFilterType object from an existing one.
    pub fn create_copy_from(from_bf: &BloomFilterType) -> BloomFilterType {
        let mut filters: Vec<BloomFilter> = Vec::with_capacity(from_bf.filters.len());
        metrics::BLOOM_NUM_OBJECTS.fetch_add(1, Ordering::Relaxed);
        metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
            mem::size_of::<BloomFilterType>(),
            std::sync::atomic::Ordering::Relaxed,
        );
        for filter in &from_bf.filters {
            let new_filter = BloomFilter::create_copy_from(filter);
            filters.push(new_filter);
        }
        BloomFilterType {
            expansion: from_bf.expansion,
            fp_rate: from_bf.fp_rate,
            filters,
        }
    }

    /// Return the total memory usage of the BloomFilterType object.
    pub fn memory_usage(&self) -> usize {
        let mut mem: usize = std::mem::size_of::<BloomFilterType>();
        for filter in &self.filters {
            mem += filter.number_of_bytes();
        }
        mem
    }

    /// Returns the Bloom object's free_effort.
    /// We return 1 if there are no filters (BF.RESERVE) or if there is 1 filter.
    /// Else, we return the number of filters as the free_effort.
    /// This is similar to how the core handles aggregated objects.
    pub fn free_effort(&self) -> usize {
        self.filters.len()
    }

    /// Check if item exists already.
    pub fn item_exists(&self, item: &[u8]) -> bool {
        self.filters.iter().any(|filter| filter.check(item))
    }

    /// Return a count of number of items added to all sub filters in the BloomFilterType object.
    pub fn cardinality(&self) -> i64 {
        let mut cardinality: i64 = 0;
        for filter in &self.filters {
            cardinality += filter.num_items as i64;
        }
        cardinality
    }

    /// Return a total capacity summed across all sub filters in the BloomFilterType object.
    pub fn capacity(&self) -> i64 {
        let mut capacity: i64 = 0;
        // Check if item exists already.
        for filter in &self.filters {
            capacity += filter.capacity as i64;
        }
        capacity
    }

    /// Add an item to the BloomFilterType object.
    /// If scaling is enabled, this can result in a new sub filter creation.
    pub fn add_item(&mut self, item: &[u8], validate_size_limit: bool) -> Result<i64, BloomError> {
        // Check if item exists already.
        if self.item_exists(item) {
            return Ok(0);
        }
        let num_filters = self.filters.len() as i32;
        if let Some(filter) = self.filters.last_mut() {
            if filter.num_items < filter.capacity {
                // Add item.
                filter.set(item);
                filter.num_items += 1;
                metrics::BLOOM_NUM_ITEMS_ACROSS_OBJECTS
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(1);
            }
            // Non Scaling Filters that are filled to capacity cannot handle more inserts.
            if self.expansion == 0 {
                return Err(BloomError::NonScalingFilterFull);
            }
            if num_filters == configs::MAX_FILTERS_PER_OBJ {
                return Err(BloomError::MaxNumScalingFilters);
            }
            // Scale out by adding a new filter with capacity bounded within the u32 range. false positive rate is also
            // bound within the range f64::MIN_POSITIVE <= x < 1.0.
            let new_fp_rate = match Self::calculate_fp_rate(self.fp_rate, num_filters) {
                Ok(rate) => rate,
                Err(e) => return Err(e),
            };
            let new_capacity = match filter.capacity.checked_mul(self.expansion) {
                Some(new_capacity) => new_capacity,
                None => {
                    // u32:max cannot be reached with 64MB memory usage limit per filter even with a high fp rate (e.g. 0.9).
                    return Err(BloomError::MaxNumScalingFilters);
                }
            };
            // Reject the request, if the operation will result in creation of a filter of size greater than what is allowed.
            if validate_size_limit && !BloomFilter::validate_size(new_capacity, new_fp_rate) {
                return Err(BloomError::ExceedsMaxBloomSize);
            }
            let mut new_filter = BloomFilter::new(new_fp_rate, new_capacity);
            // Add item.
            new_filter.set(item);
            new_filter.num_items += 1;
            self.filters.push(new_filter);

            metrics::BLOOM_NUM_ITEMS_ACROSS_OBJECTS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(1);
        }
        Ok(0)
    }

    /// Serializes bloomFilter to a byte array.
    pub fn encode_bloom_filter(&self) -> Result<Vec<u8>, BloomError> {
        match bincode::serialize(self) {
            Ok(vec) => {
                let mut final_vec = Vec::with_capacity(1 + vec.len());
                final_vec.push(BLOOM_TYPE_VERSION);
                final_vec.extend(vec);
                Ok(final_vec)
            }
            Err(_) => Err(BloomError::EncodeBloomFilterFailed),
        }
    }

    /// Calculate the false positive rate for the Nth filter using tightening ratio.
    pub fn calculate_fp_rate(fp_rate: f64, num_filters: i32) -> Result<f64, BloomError> {
        match fp_rate * configs::TIGHTENING_RATIO.powi(num_filters) {
            x if x > f64::MIN_POSITIVE => Ok(x),
            _ => Err(BloomError::MaxNumScalingFilters),
        }
    }

    /// Deserialize a byte array to bloom filter.
    /// We will need to handle any current or previous version and deserializing the bytes into a bloom object of the running Module's current version `BLOOM_TYPE_VERSION`.
    pub fn decode_bloom_filter(
        decoded_bytes: &[u8],
        validate_size_limit: bool,
    ) -> Result<BloomFilterType, BloomError> {
        if decoded_bytes.is_empty() {
            return Err(BloomError::DecodeBloomFilterFailed);
        }
        let version = decoded_bytes[0];
        match version {
            1 => {
                // always use new version to init bloomFilterType.
                // This is to ensure that the new fields can be recognized when the object is serialized and deserialized in the future.
                let (expansion, fp_rate, filters): (u32, f64, Vec<BloomFilter>) =
                    match bincode::deserialize::<(u32, f64, Vec<BloomFilter>)>(&decoded_bytes[1..])
                    {
                        Ok(values) => {
                            if !(BLOOM_EXPANSION_MIN..=BLOOM_EXPANSION_MAX).contains(&values.0) {
                                return Err(BloomError::BadExpansion);
                            }
                            if !(values.1 > BLOOM_FP_RATE_MIN && values.1 < BLOOM_FP_RATE_MAX) {
                                return Err(BloomError::ErrorRateRange);
                            }
                            if values.2.len() >= configs::MAX_FILTERS_PER_OBJ as usize {
                                return Err(BloomError::MaxNumScalingFilters);
                            }
                            for _filter in values.2.iter() {
                                // Reject the request, if the operation will result in creation of a filter of size greater than what is allowed.
                                if validate_size_limit
                                    && _filter.number_of_bytes()
                                        > configs::BLOOM_MEMORY_LIMIT_PER_FILTER
                                            .load(Ordering::Relaxed)
                                            as usize
                                {
                                    return Err(BloomError::ExceedsMaxBloomSize);
                                }
                            }
                            values
                        }
                        Err(_) => {
                            return Err(BloomError::DecodeBloomFilterFailed);
                        }
                    };
                let item = BloomFilterType {
                    expansion,
                    fp_rate,
                    filters,
                };
                // add bloom filter type metrics.
                metrics::BLOOM_NUM_OBJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
                    mem::size_of::<BloomFilterType>(),
                    std::sync::atomic::Ordering::Relaxed,
                );
                // add bloom filter metrics.
                for filter in &item.filters {
                    metrics::BLOOM_NUM_FILTERS_ACROSS_OBJECTS
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
                        filter.number_of_bytes(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    metrics::BLOOM_NUM_ITEMS_ACROSS_OBJECTS.fetch_add(
                        filter.num_items.into(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    metrics::BLOOM_CAPACITY_ACROSS_OBJECTS
                        .fetch_add(filter.capacity.into(), std::sync::atomic::Ordering::Relaxed);
                }

                Ok(item)
            }

            _ => Err(BloomError::DecodeUnsupportedVersion),
        }
    }
}

/// Structure representing a single bloom filter. 200 Bytes.
/// Using Crate: "bloomfilter"
/// The reason for using u32 for num_items and capacity is because
/// we have a limit on the memory usage of a `BloomFilter` to be 64MB.
/// Based on this, we expect the number of items on the `BloomFilter` to be
/// well within the u32::MAX limit.
#[derive(Serialize, Deserialize)]
pub struct BloomFilter {
    #[serde(serialize_with = "serialize", deserialize_with = "deserialize")]
    pub bloom: bloomfilter::Bloom<[u8]>,
    pub num_items: u32,
    pub capacity: u32,
}

impl BloomFilter {
    /// Instantiate empty BloomFilter object.
    pub fn new(fp_rate: f64, capacity: u32) -> BloomFilter {
        let bloom = bloomfilter::Bloom::new_for_fp_rate_with_seed(
            capacity as usize,
            fp_rate,
            &configs::FIXED_SEED,
        )
        .expect("We expect bloomfilter::Bloom<[u8]> creation to succeed");
        let fltr = BloomFilter {
            bloom,
            num_items: 0,
            capacity,
        };
        metrics::BLOOM_NUM_FILTERS_ACROSS_OBJECTS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_add(fltr.number_of_bytes(), std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_CAPACITY_ACROSS_OBJECTS
            .fetch_add(capacity.into(), std::sync::atomic::Ordering::Relaxed);
        fltr
    }

    /// Create a new BloomFilter from dumped information (RDB load).
    pub fn from_existing(bitmap: &[u8], num_items: u32, capacity: u32) -> BloomFilter {
        let bloom = bloomfilter::Bloom::from_slice(bitmap)
            .expect("We expect bloomfilter::Bloom<[u8]> creation to succeed");

        let fltr = BloomFilter {
            bloom,
            num_items,
            capacity,
        };
        metrics::BLOOM_NUM_FILTERS_ACROSS_OBJECTS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_add(fltr.number_of_bytes(), std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_NUM_ITEMS_ACROSS_OBJECTS
            .fetch_add(num_items.into(), std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_CAPACITY_ACROSS_OBJECTS
            .fetch_add(capacity.into(), std::sync::atomic::Ordering::Relaxed);
        fltr
    }

    pub fn number_of_bytes(&self) -> usize {
        std::mem::size_of::<BloomFilter>() + (self.bloom.len() / 8) as usize
    }

    /// Caculates the number of bytes that the bloom filter will require to be allocated.
    /// This is used before actually creating the bloom filter when checking if the filter is within the allowed size.
    /// Returns whether the bloom filter is of a valid size or not.
    pub fn validate_size(capacity: u32, fp_rate: f64) -> bool {
        let bytes = bloomfilter::Bloom::<[u8]>::compute_bitmap_size(capacity as usize, fp_rate)
            + std::mem::size_of::<BloomFilter>();
        if bytes > configs::BLOOM_MEMORY_LIMIT_PER_FILTER.load(Ordering::Relaxed) as usize {
            return false;
        }
        true
    }

    pub fn check(&self, item: &[u8]) -> bool {
        self.bloom.check(item)
    }

    pub fn set(&mut self, item: &[u8]) {
        self.bloom.set(item)
    }

    /// Create a new BloomFilter from an existing BloomFilter object (COPY command).
    pub fn create_copy_from(bf: &BloomFilter) -> BloomFilter {
        BloomFilter::from_existing(&bf.bloom.to_bytes(), bf.num_items, bf.capacity)
    }
}

impl Drop for BloomFilterType {
    fn drop(&mut self) {
        metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES.fetch_sub(
            std::mem::size_of::<BloomFilterType>(),
            std::sync::atomic::Ordering::Relaxed,
        );
        metrics::BLOOM_NUM_OBJECTS.fetch_sub(1, Ordering::Relaxed);
    }
}
impl Drop for BloomFilter {
    fn drop(&mut self) {
        metrics::BLOOM_NUM_FILTERS_ACROSS_OBJECTS
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_sub(self.number_of_bytes(), std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_NUM_ITEMS_ACROSS_OBJECTS
            .fetch_sub(self.num_items.into(), std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_CAPACITY_ACROSS_OBJECTS
            .fetch_sub(self.capacity.into(), std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use configs::FIXED_SEED;
    use rand::{distributions::Alphanumeric, Rng};

    /// Returns random string with specified number of characters.
    fn random_prefix(len: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    /// Loops until the capacity of the provided bloom filter is reached and adds a new item to it in every iteration.
    /// The item name is rand_prefix + the index (starting from starting_item_idx).
    /// With every add operation, fp_count is tracked as we expect the add operation to return 1, since it is a new item.
    /// Returns the number of errors (false positives) and the final item index.
    fn add_items_till_capacity(
        bf: &mut BloomFilterType,
        capacity_needed: i64,
        starting_item_idx: i64,
        rand_prefix: &String,
    ) -> (i64, i64) {
        let mut new_item_idx = starting_item_idx;
        let mut fp_count = 0;
        let mut cardinality = bf.cardinality();
        while cardinality < capacity_needed {
            let item = format!("{}{}", rand_prefix, new_item_idx);
            let result = bf.add_item(item.as_bytes(), true);
            match result {
                Ok(0) => {
                    fp_count += 1;
                }
                Ok(1) => {
                    cardinality += 1;
                }
                Ok(i64::MIN..=-1_i64) | Ok(2_i64..=i64::MAX) => {
                    panic!("We do not expect add_item to return any Integer other than 0 or 1.")
                }
                Err(e) => {
                    panic!("We do not expect add_item to throw errors on this scalable filter test, {:?}", e);
                }
            };
            new_item_idx += 1;
        }
        (fp_count, new_item_idx - 1)
    }

    /// Loops from the start index till the end index and uses the exists operation on the provided bloom filter.
    /// The item name used in exists operations is rand_prefix + the index (based on the iteration).
    /// The results are matched against the `expected_result` and an error_count tracks the wrong results.
    /// Asserts that the error_count is within the expected false positive (+ margin) rate.
    /// Returns the error count and number of operations performed.
    fn check_items_exist(
        bf: &BloomFilterType,
        start_idx: i64,
        end_idx: i64,
        expected_result: bool,
        rand_prefix: &String,
    ) -> (i64, i64) {
        let mut error_count = 0;
        for i in start_idx..=end_idx {
            let item = format!("{}{}", rand_prefix, i);
            let result = bf.item_exists(item.as_bytes());
            if result != expected_result {
                error_count += 1;
            }
        }
        let num_operations = (end_idx - start_idx) + 1;
        (error_count, num_operations)
    }

    fn fp_assert(error_count: i64, num_operations: i64, expected_fp_rate: f64, fp_margin: f64) {
        let real_fp_rate = error_count as f64 / num_operations as f64;
        let fp_rate_with_margin = expected_fp_rate + fp_margin;
        assert!(
            real_fp_rate < fp_rate_with_margin,
            "The actual fp_rate, {}, is greater than the configured fp_rate with margin. {}.",
            real_fp_rate,
            fp_rate_with_margin
        );
    }

    fn verify_restored_items(
        original_bloom_filter_type: &BloomFilterType,
        restored_bloom_filter_type: &BloomFilterType,
        add_operation_idx: i64,
        expected_fp_rate: f64,
        fp_margin: f64,
        rand_prefix: &String,
    ) {
        assert_eq!(
            restored_bloom_filter_type.capacity(),
            original_bloom_filter_type.capacity()
        );
        assert_eq!(
            restored_bloom_filter_type.cardinality(),
            original_bloom_filter_type.cardinality(),
        );
        assert_eq!(
            restored_bloom_filter_type.free_effort(),
            original_bloom_filter_type.free_effort()
        );
        assert_eq!(
            restored_bloom_filter_type.memory_usage(),
            original_bloom_filter_type.memory_usage()
        );
        assert!(restored_bloom_filter_type
            .filters
            .iter()
            .all(|restore_filter| original_bloom_filter_type
                .filters
                .iter()
                .any(
                    |filter| (filter.bloom.seed() == restore_filter.bloom.seed())
                        && (restore_filter.bloom.seed() == FIXED_SEED)
                )));
        assert!(restored_bloom_filter_type
            .filters
            .iter()
            .all(|restore_filter| original_bloom_filter_type
                .filters
                .iter()
                .any(|filter| filter.bloom.number_of_hash_functions()
                    == restore_filter.bloom.number_of_hash_functions())));
        assert!(restored_bloom_filter_type
            .filters
            .iter()
            .all(|restore_filter| original_bloom_filter_type
                .filters
                .iter()
                .any(|filter| filter.bloom.as_slice() == restore_filter.bloom.as_slice())));
        let (error_count, _) = check_items_exist(
            restored_bloom_filter_type,
            1,
            add_operation_idx,
            true,
            rand_prefix,
        );
        assert!(error_count == 0);
        let (error_count, num_operations) = check_items_exist(
            restored_bloom_filter_type,
            add_operation_idx + 1,
            add_operation_idx * 2,
            false,
            rand_prefix,
        );
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin);
    }

    #[test]
    fn test_non_scaling_filter() {
        let rand_prefix = random_prefix(7);
        // 1 in every 1000 operations is expected to be a false positive.
        let expected_fp_rate: f64 = 0.001;
        let initial_capacity = 10000;
        // Expansion of 0 indicates non scaling.
        let expansion = 0;
        // Validate the non scaling behavior of the bloom filter.
        let mut bf =
            BloomFilterType::new_reserved(expected_fp_rate, initial_capacity, expansion, true)
                .expect("Expect bloom creation to succeed");
        let (error_count, add_operation_idx) =
            add_items_till_capacity(&mut bf, initial_capacity as i64, 1, &rand_prefix);
        assert_eq!(
            bf.add_item(b"new_item", true),
            Err(BloomError::NonScalingFilterFull)
        );
        assert_eq!(bf.capacity(), initial_capacity as i64);
        assert_eq!(bf.cardinality(), initial_capacity as i64);
        let expected_free_effort = 1;
        assert_eq!(bf.free_effort(), expected_free_effort);
        assert!(bf.memory_usage() > 0);
        // Use a margin on the expected_fp_rate when asserting for correctness.
        let fp_margin = 0.002;
        // Validate that item "add" operations on bloom filters are ensuring correctness.
        fp_assert(error_count, add_operation_idx, expected_fp_rate, fp_margin);
        // Validate item "exists" operations on bloom filters are ensuring correctness.
        // This tests for items already added to the filter and expects them to exist.
        let (error_count, _) = check_items_exist(&bf, 1, add_operation_idx, true, &rand_prefix);
        assert!(error_count == 0);
        // This tests for items which are not added to the filter and expects them to not exist.
        let (error_count, num_operations) = check_items_exist(
            &bf,
            add_operation_idx + 1,
            add_operation_idx * 2,
            false,
            &rand_prefix,
        );
        // Validate that the real fp_rate is not much more than the configured fp_rate.
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin);

        // Verify restore
        let mut restore_bf = BloomFilterType::create_copy_from(&bf);
        assert_eq!(
            restore_bf.add_item(b"new_item", true),
            Err(BloomError::NonScalingFilterFull)
        );
        verify_restored_items(
            &bf,
            &restore_bf,
            add_operation_idx,
            expected_fp_rate,
            fp_margin,
            &rand_prefix,
        );
    }

    #[test]
    fn test_scaling_filter() {
        let rand_prefix = random_prefix(7);
        // 1 in every 1000 operations is expected to be a false positive.
        let expected_fp_rate: f64 = 0.001;
        let initial_capacity = 10000;
        let expansion = 2;
        let num_filters_to_scale = 5;
        let mut bf =
            BloomFilterType::new_reserved(expected_fp_rate, initial_capacity, expansion, true)
                .expect("Expect bloom creation to succeed");
        assert_eq!(bf.capacity(), initial_capacity as i64);
        assert_eq!(bf.cardinality(), 0);
        let mut total_error_count = 0;
        let mut add_operation_idx = 0;
        // Validate the scaling behavior of the bloom filter.
        for filter_idx in 1..=num_filters_to_scale {
            let expected_total_capacity = initial_capacity * (expansion.pow(filter_idx) - 1);
            let (error_count, new_add_operation_idx) = add_items_till_capacity(
                &mut bf,
                expected_total_capacity as i64,
                add_operation_idx + 1,
                &rand_prefix,
            );
            add_operation_idx = new_add_operation_idx;
            total_error_count += error_count;
            assert_eq!(bf.capacity(), expected_total_capacity as i64);
            assert_eq!(bf.cardinality(), expected_total_capacity as i64);
            let expected_free_effort = filter_idx as usize;
            assert_eq!(bf.free_effort(), expected_free_effort);
            assert!(bf.memory_usage() > 0);
        }
        // Use a margin on the expected_fp_rate when asserting for correctness.
        let fp_margin = 0.002;
        // Validate that item "add" operations on bloom filters are ensuring correctness.
        fp_assert(
            total_error_count,
            add_operation_idx,
            expected_fp_rate,
            fp_margin,
        );
        // Validate item "exists" operations on bloom filters are ensuring correctness.
        // This tests for items already added to the filter and expects them to exist.
        let (error_count, _) = check_items_exist(&bf, 1, add_operation_idx, true, &rand_prefix);
        assert!(error_count == 0);
        // This tests for items which are not added to the filter and expects them to not exist.
        let (error_count, num_operations) = check_items_exist(
            &bf,
            add_operation_idx + 1,
            add_operation_idx * 2,
            false,
            &rand_prefix,
        );
        // Validate that the real fp_rate is not much more than the configured fp_rate.
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin);

        // Verify restore
        let restore_bloom_filter_type = BloomFilterType::create_copy_from(&bf);
        verify_restored_items(
            &bf,
            &restore_bloom_filter_type,
            add_operation_idx,
            expected_fp_rate,
            fp_margin,
            &rand_prefix,
        );
    }

    #[test]
    fn test_seed() {
        // The value of sip keys generated by the sip_keys with fixed seed should be equal to the constant in configs.rs
        let test_bloom_filter = BloomFilter::new(0.5_f64, 1000_u32);
        let seed = test_bloom_filter.bloom.seed();
        assert_eq!(seed, FIXED_SEED);
    }

    #[test]
    fn test_exceeded_size_limit() {
        // Validate that bloom filter allocations within bloom objects are rejected if their memory usage would be beyond
        // the configured limit.
        let result = BloomFilterType::new_reserved(0.5_f64, u32::MAX, 1, true);
        assert_eq!(result.err(), Some(BloomError::ExceedsMaxBloomSize));
        let capacity = 50000000;
        assert!(!BloomFilter::validate_size(capacity, 0.001_f64));
        let result2 = BloomFilterType::new_reserved(0.001_f64, capacity, 1, true);
        assert_eq!(result2.err(), Some(BloomError::ExceedsMaxBloomSize));
    }

    #[test]
    fn test_bf_encode_and_decode() {
        // arrange: prepare bloom filter
        let mut bf = BloomFilterType::new_reserved(0.5_f64, 1000_u32, 2, true).unwrap();
        let key = "key";
        let _ = bf.add_item(key.as_bytes(), true);

        // action
        let encoder_result = bf.encode_bloom_filter();

        // assert
        // encoder sucess
        assert!(encoder_result.is_ok());
        let vec = encoder_result.unwrap();

        // assert decode:
        let new_bf_result = BloomFilterType::decode_bloom_filter(&vec, true);

        let new_bf = new_bf_result.unwrap();

        // verify new_bf and bf
        assert_eq!(bf.fp_rate, new_bf.fp_rate);
        assert_eq!(bf.expansion, new_bf.expansion);
        assert_eq!(bf.capacity(), new_bf.capacity());

        // contains key
        assert!(new_bf.item_exists(key.as_bytes()));
    }

    #[test]
    fn test_bf_decode_when_unsupported_version_should_failed() {
        // arrange: prepare bloom filter
        let mut bf = BloomFilterType::new_reserved(0.5_f64, 1000_u32, 2, true).unwrap();
        let key = "key";
        let _ = bf.add_item(key.as_bytes(), true).unwrap();

        let encoder_result = bf.encode_bloom_filter();
        assert!(encoder_result.is_ok());

        // 1. unsupport version should return error
        let mut vec = encoder_result.unwrap();
        vec[0] = 10;

        // assert decode:
        // should return error
        assert_eq!(
            BloomFilterType::decode_bloom_filter(&vec, true).err(),
            Some(BloomError::DecodeUnsupportedVersion)
        );
    }

    #[test]
    fn test_bf_decode_when_bytes_is_empty_should_failed() {
        // arrange: prepare bloom filter
        let mut bf = BloomFilterType::new_reserved(0.5_f64, 1000_u32, 2, true).unwrap();
        let key = "key";
        let _ = bf.add_item(key.as_bytes(), true);

        let encoder_result = bf.encode_bloom_filter();
        assert!(encoder_result.is_ok());

        // 1. empty vec should return error
        let vec: Vec<u8> = Vec::new();
        // assert decode:
        // should return error
        assert_eq!(
            BloomFilterType::decode_bloom_filter(&vec, true).err(),
            Some(BloomError::DecodeBloomFilterFailed)
        );
    }

    #[test]
    fn test_bf_decode_when_bytes_is_exceed_limit_should_failed() {
        // arrange: prepare bloom filter
        let mut bf = BloomFilterType::new_reserved(0.5_f64, 1000_u32, 2, true).unwrap();
        let key = "key";
        let _ = bf.add_item(key.as_bytes(), true);
        let origin_expansion = bf.expansion;
        let origin_fp_rate = bf.fp_rate;
        // unsupoort expansion
        bf.expansion = 0;

        let encoder_result = bf.encode_bloom_filter();

        // 1. unsupport expansion
        let vec = encoder_result.unwrap();
        // assert decode:
        // should return error
        assert_eq!(
            BloomFilterType::decode_bloom_filter(&vec, true).err(),
            Some(BloomError::BadExpansion)
        );

        // 1.2 Exceeded the maximum expansion
        bf.expansion = BLOOM_EXPANSION_MAX + 1;

        let vec = bf.encode_bloom_filter().unwrap();
        assert_eq!(
            BloomFilterType::decode_bloom_filter(&vec, true).err(),
            Some(BloomError::BadExpansion)
        );
        // recover
        bf.expansion = origin_expansion;

        // 2. unsupport fp_rate
        bf.fp_rate = -0.5;
        let vec = bf.encode_bloom_filter().unwrap();
        // should return error
        assert_eq!(
            BloomFilterType::decode_bloom_filter(&vec, true).err(),
            Some(BloomError::ErrorRateRange)
        );
        bf.fp_rate = origin_fp_rate;

        // 3. build a larger than 64mb filter
        let extra_large_filter =
            BloomFilterType::new_reserved(0.01_f64, 57000000, 2, false).unwrap();
        let vec = extra_large_filter.encode_bloom_filter().unwrap();
        // should return error
        assert_eq!(
            BloomFilterType::decode_bloom_filter(&vec, true).err(),
            Some(BloomError::ExceedsMaxBloomSize)
        );
    }
}
