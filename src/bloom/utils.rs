use super::data_type::BLOOM_OBJECT_VERSION;
use crate::{
    configs::{
        self, BLOOM_EXPANSION_MAX, BLOOM_FP_RATE_MAX, BLOOM_FP_RATE_MIN,
        BLOOM_TIGHTENING_RATIO_MAX, BLOOM_TIGHTENING_RATIO_MIN,
    },
    metrics,
};
use bloomfilter::Bloom;
use bloomfilter::{deserialize, serialize};
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::atomic::Ordering;

/// KeySpace Notification Events
pub const ADD_EVENT: &str = "bloom.add";
pub const RESERVE_EVENT: &str = "bloom.reserve";

/// Client Errors
pub const ERROR: &str = "ERROR";
pub const NON_SCALING_FILTER_FULL: &str = "ERR non scaling filter is full";
pub const NOT_FOUND: &str = "ERR not found";
pub const ITEM_EXISTS: &str = "ERR item exists";
pub const INVALID_INFO_VALUE: &str = "ERR invalid information value";
pub const INVALID_SEED: &str = "ERR invalid seed";
pub const BAD_EXPANSION: &str = "ERR bad expansion";
pub const BAD_CAPACITY: &str = "ERR bad capacity";
pub const BAD_ERROR_RATE: &str = "ERR bad error rate";
pub const ERROR_RATE_RANGE: &str = "ERR (0 < error rate range < 1)";
pub const BAD_TIGHTENING_RATIO: &str = "ERR bad tightening ratio";
pub const TIGHTENING_RATIO_RANGE: &str = "ERR (0 < tightening ratio range < 1)";
pub const CAPACITY_LARGER_THAN_0: &str = "ERR (capacity should be larger than 0)";
pub const FALSE_POSITIVE_DEGRADES_TO_O: &str = "ERR false positive degrades to 0 on scale out";
pub const UNKNOWN_ARGUMENT: &str = "ERR unknown argument received";
pub const EXCEEDS_MAX_BLOOM_SIZE: &str = "ERR operation exceeds bloom object memory limit";
pub const VALIDATE_SCALE_TO_EXCEEDS_MAX_SIZE: &str =
    "ERR provided VALIDATESCALETO causes bloom object to exceed memory limit";
pub const MAX_NUM_SCALING_FILTERS: &str = "ERR bloom object reached max number of filters";
pub const VALIDATE_SCALE_TO_FALSE_POSITIVE_INVALID: &str =
    "ERR provided VALIDATESCALETO causes false positive to degrade to 0";
pub const KEY_EXISTS: &str = "BUSYKEY Target key name already exists.";
pub const DECODE_BLOOM_OBJECT_FAILED: &str = "ERR bloom object decoding failed";
pub const DECODE_UNSUPPORTED_VERSION: &str =
    "ERR bloom object decoding failed. Unsupported version";
pub const NON_SCALING_AND_VALIDATE_SCALE_TO_IS_INVALID: &str =
    "ERR cannot use NONSCALING and VALIDATESCALETO options together";
/// Logging Error messages
pub const ENCODE_BLOOM_OBJECT_FAILED: &str = "Failed to encode bloom object.";

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
    FalsePositiveReachesZero,
    BadCapacity,
    ValidateScaleToExceedsMaxSize,
    ValidateScaleToFalsePositiveInvalid,
}

impl BloomError {
    pub fn as_str(&self) -> &'static str {
        match self {
            BloomError::NonScalingFilterFull => NON_SCALING_FILTER_FULL,
            BloomError::MaxNumScalingFilters => MAX_NUM_SCALING_FILTERS,
            BloomError::ExceedsMaxBloomSize => EXCEEDS_MAX_BLOOM_SIZE,
            BloomError::EncodeBloomFilterFailed => ENCODE_BLOOM_OBJECT_FAILED,
            BloomError::DecodeBloomFilterFailed => DECODE_BLOOM_OBJECT_FAILED,
            BloomError::DecodeUnsupportedVersion => DECODE_UNSUPPORTED_VERSION,
            BloomError::ErrorRateRange => ERROR_RATE_RANGE,
            BloomError::BadExpansion => BAD_EXPANSION,
            BloomError::FalsePositiveReachesZero => FALSE_POSITIVE_DEGRADES_TO_O,
            BloomError::BadCapacity => BAD_CAPACITY,
            BloomError::ValidateScaleToExceedsMaxSize => VALIDATE_SCALE_TO_EXCEEDS_MAX_SIZE,
            BloomError::ValidateScaleToFalsePositiveInvalid => {
                VALIDATE_SCALE_TO_FALSE_POSITIVE_INVALID
            }
        }
    }
}

/// The BloomObject structure which implements either scaling / non scaling bloom filters.
/// Can contain one (non scaling) or more (scaling) filters.
/// This is a generic top level structure which is not coupled to any bloom Rust crate / library.
#[derive(Serialize, Deserialize)]
#[allow(clippy::vec_box)]
pub struct BloomObject {
    expansion: u32,
    fp_rate: f64,
    tightening_ratio: f64,
    is_seed_random: bool,
    filters: Vec<Box<BloomFilter>>,
}

impl BloomObject {
    /// Create a new BloomObject object.
    pub fn new_reserved(
        fp_rate: f64,
        tightening_ratio: f64,
        capacity: i64,
        expansion: u32,
        seed: (Option<[u8; 32]>, bool),
        validate_size_limit: bool,
    ) -> Result<BloomObject, BloomError> {
        // Reject the request, if the operation will result in creation of a bloom object
        // of size greater than what is allowed.
        if validate_size_limit && !BloomObject::validate_size_before_create(capacity, fp_rate) {
            return Err(BloomError::ExceedsMaxBloomSize);
        }
        // Create the bloom filter and add to the main Bloom object.
        let is_seed_random;
        let bloom = match seed {
            (None, _) => {
                is_seed_random = true;
                Box::new(BloomFilter::with_random_seed(fp_rate, capacity))
            }
            (Some(seed), is_random) => {
                is_seed_random = is_random;
                Box::new(BloomFilter::with_fixed_seed(fp_rate, capacity, &seed))
            }
        };
        let filters = vec![bloom];
        let bloom = BloomObject {
            expansion,
            fp_rate,
            tightening_ratio,
            filters,
            is_seed_random,
        };
        bloom.bloom_object_incr_metrics_on_new_create();
        Ok(bloom)
    }

    /// Create a BloomObject from existing data (RDB Load / Restore).
    pub fn from_existing(
        expansion: u32,
        fp_rate: f64,
        tightening_ratio: f64,
        is_seed_random: bool,
        filters: Vec<Box<BloomFilter>>,
    ) -> BloomObject {
        let bloom = BloomObject {
            expansion,
            fp_rate,
            tightening_ratio,
            is_seed_random,
            filters,
        };
        bloom.bloom_object_incr_metrics_on_new_create();
        bloom
    }

    /// Create a new BloomObject from an existing one (COPY).
    pub fn create_copy_from(from_bf: &BloomObject) -> BloomObject {
        let mut filters: Vec<Box<BloomFilter>> = Vec::with_capacity(from_bf.filters.capacity());
        for filter in &from_bf.filters {
            let new_filter = Box::new(BloomFilter::create_copy_from(filter));
            filters.push(new_filter);
        }
        let new_copy = BloomObject {
            expansion: from_bf.expansion,
            fp_rate: from_bf.fp_rate,
            tightening_ratio: from_bf.tightening_ratio,
            is_seed_random: from_bf.is_seed_random,
            filters,
        };
        new_copy.bloom_object_incr_metrics_on_new_create();
        new_copy
    }

    /// Return the total memory usage of the BloomObject and every allocation it contains.
    pub fn memory_usage(&self) -> usize {
        let mut mem: usize = self.bloom_object_memory_usage();
        for filter in &self.filters {
            mem += filter.number_of_bytes();
        }
        mem
    }

    /// Calculates the memory usage of the BloomObject structure (not its nested allocations).
    fn bloom_object_memory_usage(&self) -> usize {
        BloomObject::compute_size(self.filters.capacity())
    }

    /// Calculates the memory usage of the BloomObject structure (not its nested allocations). Used when `self` is unavailable.
    pub fn compute_size(filters_vec_capacity: usize) -> usize {
        std::mem::size_of::<BloomObject>()
            + (filters_vec_capacity * std::mem::size_of::<Box<BloomFilter>>())
    }

    /// Caculates the number of bytes that the bloom object will require to be allocated.
    /// This is used when scaling out a bloom object to check if the new
    /// size will be within the allowed size limit.
    /// Returns whether the bloom object is of a valid size or not.
    fn validate_size_before_scaling(&self, capacity: i64, fp_rate: f64) -> bool {
        let bytes = self.memory_usage() + BloomFilter::compute_size(capacity, fp_rate);
        BloomObject::validate_size(bytes)
    }

    /// Caculates the number of bytes that the bloom object will require to be allocated.
    /// This is used when creating a new bloom object to check if the size is within the allowed size limit.
    /// Returns whether the bloom object is of a valid size or not.
    fn validate_size_before_create(capacity: i64, fp_rate: f64) -> bool {
        let bytes = std::mem::size_of::<BloomObject>()
            + std::mem::size_of::<Box<BloomFilter>>()
            + BloomFilter::compute_size(capacity, fp_rate);
        BloomObject::validate_size(bytes)
    }

    /// Returns whether the bloom object is of a valid size or not.
    pub fn validate_size(bytes: usize) -> bool {
        if bytes > configs::BLOOM_MEMORY_LIMIT_PER_OBJECT.load(Ordering::Relaxed) as usize {
            return false;
        }
        true
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

    /// Return a count of number of items added to all sub filters in the BloomObject structure.
    pub fn cardinality(&self) -> i64 {
        let mut cardinality: i64 = 0;
        for filter in &self.filters {
            cardinality += filter.num_items;
        }
        cardinality
    }

    /// Return a total capacity summed across all sub filters in the BloomObject structure.
    pub fn capacity(&self) -> i64 {
        let mut capacity: i64 = 0;
        // Check if item exists already.
        for filter in &self.filters {
            capacity += filter.capacity;
        }
        capacity
    }

    /// Return the seed used by the Bloom object. Every filter in the bloom object uses the same seed as the
    /// first filter regardless if the seed is fixed or randomly generated.
    pub fn seed(&self) -> [u8; 32] {
        self.filters
            .first()
            .expect("Every BloomObject is expected to have at least one filter")
            .seed()
    }
    /// Return the starting capacity used by the Bloom object. This capacity is held within the first filter
    pub fn starting_capacity(&self) -> i64 {
        self.filters
            .first()
            .expect("Every BloomObject is expected to have at least one filter")
            .capacity()
    }

    /// Return the expansion of the bloom object.
    pub fn expansion(&self) -> u32 {
        self.expansion
    }

    /// Return the false postive rate of the bloom object.
    pub fn fp_rate(&self) -> f64 {
        self.fp_rate
    }

    /// Return the tightening ratio of the bloom object.
    pub fn tightening_ratio(&self) -> f64 {
        self.tightening_ratio
    }

    /// Return whether the bloom object uses a random seed.
    pub fn is_seed_random(&self) -> bool {
        self.is_seed_random
    }

    /// Return the number of filters in the bloom object.
    pub fn num_filters(&self) -> usize {
        self.filters.len()
    }

    /// Return a borrowed ref to the vector of filters in the bloom object.
    pub fn filters(&self) -> &Vec<Box<BloomFilter>> {
        &self.filters
    }

    /// Return a mutatively borrowed ref to the vector of filters in the bloom object.
    pub fn filters_mut(&mut self) -> &mut Vec<Box<BloomFilter>> {
        &mut self.filters
    }

    /// Add an item to the BloomObject structure.
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
            if num_filters == configs::BLOOM_NUM_FILTERS_PER_OBJECT_LIMIT_MAX {
                return Err(BloomError::MaxNumScalingFilters);
            }
            // Scale out by adding a new filter with capacity bounded within the u32 range. false positive rate is also
            // bound within the range f64::MIN_POSITIVE <= x < 1.0.
            let new_fp_rate =
                Self::calculate_fp_rate(self.fp_rate, num_filters, self.tightening_ratio)?;
            let new_capacity = match filter.capacity.checked_mul(self.expansion.into()) {
                Some(new_capacity) => new_capacity,
                None => {
                    // With a 128MB memory limit for a bloom object overall, it is not possible to reach u32:max capacity.
                    return Err(BloomError::BadCapacity);
                }
            };
            // Reject the request, if the operation will result in creation of a filter of size greater than what is allowed.
            if validate_size_limit && !self.validate_size_before_scaling(new_capacity, new_fp_rate)
            {
                return Err(BloomError::ExceedsMaxBloomSize);
            }
            let seed = self.seed();
            let mut new_filter = Box::new(BloomFilter::with_fixed_seed(
                new_fp_rate,
                new_capacity,
                &seed,
            ));
            let memory_usage_before: usize = self.bloom_object_memory_usage();
            // Add item.
            new_filter.set(item);
            new_filter.num_items += 1;
            self.filters.push(new_filter);
            // If we went over capacity and scaled the vec out we need to update the memory usage by the new capacity
            let memory_usage_after = self.bloom_object_memory_usage();

            metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
                memory_usage_after - memory_usage_before,
                std::sync::atomic::Ordering::Relaxed,
            );
            metrics::BLOOM_NUM_ITEMS_ACROSS_OBJECTS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(1);
        }
        Ok(0)
    }

    /// Serializes bloomFilter to a byte array.
    pub fn encode_object(&self) -> Result<Vec<u8>, BloomError> {
        match bincode::serialize(self) {
            Ok(vec) => {
                let mut final_vec = Vec::with_capacity(1 + vec.len());
                final_vec.push(BLOOM_OBJECT_VERSION);
                final_vec.extend(vec);
                Ok(final_vec)
            }
            Err(_) => Err(BloomError::EncodeBloomFilterFailed),
        }
    }

    /// Calculate the false positive rate for the Nth filter using tightening ratio.
    pub fn calculate_fp_rate(
        fp_rate: f64,
        num_filters: i32,
        tightening_ratio: f64,
    ) -> Result<f64, BloomError> {
        match fp_rate * tightening_ratio.powi(num_filters) {
            x if x > f64::MIN_POSITIVE => Ok(x),
            _ => Err(BloomError::FalsePositiveReachesZero),
        }
    }

    /// Increments metrics related to Bloom filter memory usage upon creation of a new filter.
    fn bloom_object_incr_metrics_on_new_create(&self) {
        metrics::BLOOM_NUM_OBJECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
            self.bloom_object_memory_usage(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Deserialize a byte array to bloom filter.
    /// We will need to handle any current or previous version and deserializing the bytes into a bloom object of the running Module's current version `BLOOM_OBJECT_VERSION`.
    pub fn decode_object(
        decoded_bytes: &[u8],
        validate_size_limit: bool,
    ) -> Result<BloomObject, BloomError> {
        if decoded_bytes.is_empty() {
            return Err(BloomError::DecodeBloomFilterFailed);
        }
        let version = decoded_bytes[0];
        match version {
            1 => {
                // Always use new version to initialize a BloomObject.
                // This is to ensure that the new fields can be recognized when the object is serialized and deserialized in the future.
                let (expansion, fp_rate, tightening_ratio, is_seed_random, filters): (
                    u32,
                    f64,
                    f64,
                    bool,
                    Vec<Box<BloomFilter>>,
                ) = match bincode::deserialize::<(u32, f64, f64, bool, Vec<Box<BloomFilter>>)>(
                    &decoded_bytes[1..],
                ) {
                    Ok(values) => {
                        // Add individual bloom filter metrics.
                        for filter in &values.4 {
                            metrics::BLOOM_NUM_ITEMS_ACROSS_OBJECTS.fetch_add(
                                filter.num_items as u64,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            filter.bloom_filter_incr_metrics_on_new_create();
                        }
                        // Expansion ratio can range from 0 to BLOOM_EXPANSION_MAX as we internally set this to 0
                        // in case of non scaling filters.
                        if !(0..=BLOOM_EXPANSION_MAX).contains(&values.0) {
                            return Err(BloomError::BadExpansion);
                        }
                        if !(values.1 > BLOOM_FP_RATE_MIN && values.1 < BLOOM_FP_RATE_MAX) {
                            return Err(BloomError::ErrorRateRange);
                        }
                        if !(values.2 > BLOOM_TIGHTENING_RATIO_MIN
                            && values.2 < BLOOM_TIGHTENING_RATIO_MAX)
                        {
                            return Err(BloomError::ErrorRateRange);
                        }
                        if values.4.len()
                            >= configs::BLOOM_NUM_FILTERS_PER_OBJECT_LIMIT_MAX as usize
                        {
                            return Err(BloomError::MaxNumScalingFilters);
                        }
                        values
                    }
                    Err(_) => {
                        return Err(BloomError::DecodeBloomFilterFailed);
                    }
                };
                let item = BloomObject {
                    expansion,
                    fp_rate,
                    tightening_ratio,
                    is_seed_random,
                    filters,
                };
                // Add overall bloom object metrics.
                item.bloom_object_incr_metrics_on_new_create();
                let bytes = item.memory_usage();
                // Reject the request, if the operation will result in creation of a bloom object of size greater than what is allowed.
                if validate_size_limit && !BloomObject::validate_size(bytes) {
                    return Err(BloomError::ExceedsMaxBloomSize);
                }
                Ok(item)
            }
            _ => Err(BloomError::DecodeUnsupportedVersion),
        }
    }

    /// This method is called from two different bloom commands: BF.INFO and BF.INSERT. The functionality varies slightly on which command it
    /// is called from. When called from BF.INFO, this method is used to find the maximum possible size that the bloom object could scale to
    /// without throwing an error. When called from BF.INSERT, this method is used to determine if it is possible to reach the provided `validate_scale_to`.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The size of the initial filter in the bloom object.
    /// * `fp_rate` - the false positive rate for the bloom object
    /// * `validate_scale_to` - the capacity we check to see if it can scale to. If this method is called from BF.INFO this is set as -1 as we
    ///                       want to check the maximum size we could scale up till
    /// * `tightening_ratio` - The tightening ratio of the object
    /// * `expansion` - The expanison rate of the object
    ///
    /// # Returns
    /// * i64 - The maximum capacity that can be reached if called from BF.INFO. If called from BF.INSERT the size it reached when it became greater than `validate_scale_to`
    /// * ValkeyError - Can return two different errors:
    ///     VALIDATE_SCALE_TO_EXCEEDS_MAX_SIZE: When scaling to the wanted capacity would go over the bloom object memory limit
    ///     VALIDATE_SCALE_TO_FALSE_POSITIVE_INVALID: When scaling to the wanted capacity would cause the false positive rate to reach 0
    pub fn calculate_max_scaled_capacity(
        capacity: i64,
        fp_rate: f64,
        validate_scale_to: i64,
        tightening_ratio: f64,
        expansion: u32,
    ) -> Result<i64, BloomError> {
        let mut curr_filter_capacity = capacity;
        let mut curr_total_capacity = 0;
        let mut curr_num_filters: u64 = 0;
        let mut filters_memory_usage = 0;
        while curr_total_capacity < validate_scale_to || validate_scale_to == -1 {
            // Check to see if scaling to the next filter will cause a degradation in FP to 0
            let curr_fp_rate = match BloomObject::calculate_fp_rate(
                fp_rate,
                curr_num_filters as i32,
                tightening_ratio,
            ) {
                Ok(rate) => rate,
                Err(_) => {
                    if validate_scale_to == -1 {
                        return Ok(curr_total_capacity);
                    }
                    return Err(BloomError::ValidateScaleToFalsePositiveInvalid);
                }
            };
            // Check that if it scales to this number of filters that the object won't exceed the memory limit
            let curr_filter_size = BloomFilter::compute_size(curr_filter_capacity, curr_fp_rate);
            // The capacity is always a power of two above or equal to the size other than for vectors of size 1 where the capacity is 1 and for size 2 where the
            // capacity of the vec is 4.
            let curr_object_size = BloomObject::compute_size(if curr_num_filters == 0 {
                1
            } else {
                (std::cmp::max(4, curr_num_filters + 1)).next_power_of_two()
            } as usize)
                + filters_memory_usage
                + curr_filter_size;
            if !BloomObject::validate_size(curr_object_size) {
                if validate_scale_to == -1 {
                    return Ok(curr_total_capacity);
                }
                return Err(BloomError::ValidateScaleToExceedsMaxSize);
            }
            // Update overall memory usage
            filters_memory_usage += curr_filter_size;
            curr_total_capacity += curr_filter_capacity;
            curr_filter_capacity = match curr_filter_capacity.checked_mul(expansion.into()) {
                Some(new_capacity) => new_capacity,
                None => {
                    // With a 128MB memory limit for a bloom object overall, it is not possible to reach u32:max capacity.
                    return Err(BloomError::BadCapacity);
                }
            };
            curr_num_filters += 1;
        }
        Ok(curr_total_capacity)
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
    #[serde(
        serialize_with = "serialize",
        deserialize_with = "deserialize_boxed_bloom"
    )]
    bloom: Box<bloomfilter::Bloom<[u8]>>,
    num_items: i64,
    capacity: i64,
}

pub fn deserialize_boxed_bloom<'de, D>(deserializer: D) -> Result<Box<Bloom<[u8]>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize(deserializer).map(Box::new)
}

impl BloomFilter {
    /// Instantiate empty BloomFilter object with a fixed seed used to create sip keys.
    pub fn with_fixed_seed(fp_rate: f64, capacity: i64, fixed_seed: &[u8; 32]) -> BloomFilter {
        let bloom =
            bloomfilter::Bloom::new_for_fp_rate_with_seed(capacity as usize, fp_rate, fixed_seed)
                .expect("We expect bloomfilter::Bloom<[u8]> creation to succeed");
        let fltr = BloomFilter {
            bloom: Box::new(bloom),
            num_items: 0,
            capacity,
        };
        fltr.bloom_filter_incr_metrics_on_new_create();
        fltr
    }

    /// Instantiate empty BloomFilter object with a randomly generated seed used to create sip keys.
    pub fn with_random_seed(fp_rate: f64, capacity: i64) -> BloomFilter {
        let bloom = Box::new(
            bloomfilter::Bloom::new_for_fp_rate(capacity as usize, fp_rate)
                .expect("We expect bloomfilter::Bloom<[u8]> creation to succeed"),
        );
        let fltr = BloomFilter {
            bloom,
            num_items: 0,
            capacity,
        };
        fltr.bloom_filter_incr_metrics_on_new_create();
        fltr
    }

    /// Create a new BloomFilter from dumped information (RDB load).
    pub fn from_existing(bitmap: &[u8], num_items: i64, capacity: i64) -> BloomFilter {
        let bloom = bloomfilter::Bloom::from_slice(bitmap)
            .expect("We expect bloomfilter::Bloom<[u8]> creation to succeed");

        let fltr = BloomFilter {
            bloom: Box::new(bloom),
            num_items,
            capacity,
        };
        fltr.bloom_filter_incr_metrics_on_new_create();
        metrics::BLOOM_NUM_ITEMS_ACROSS_OBJECTS
            .fetch_add(num_items as u64, std::sync::atomic::Ordering::Relaxed);
        fltr
    }

    /// Create a new BloomFilter from an existing BloomFilter object (COPY command).
    pub fn create_copy_from(bf: &BloomFilter) -> BloomFilter {
        BloomFilter::from_existing(&bf.bloom.to_bytes(), bf.num_items, bf.capacity)
    }

    fn bloom_filter_incr_metrics_on_new_create(&self) {
        metrics::BLOOM_NUM_FILTERS_ACROSS_OBJECTS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_add(self.number_of_bytes(), std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_CAPACITY_ACROSS_OBJECTS
            .fetch_add(self.capacity as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Return the seed used by the sip hasher of the raw bloom.
    pub fn seed(&self) -> [u8; 32] {
        self.bloom.seed()
    }

    /// Return the numer of items in the BloomFilter.
    pub fn num_items(&self) -> i64 {
        self.num_items
    }

    /// Return the capcity of the BloomFilter - number of items that can be added to it.
    pub fn capacity(&self) -> i64 {
        self.capacity
    }

    /// Return a borrowed ref to the raw bloom of the BloomFilter.
    pub fn raw_bloom(&self) -> &bloomfilter::Bloom<[u8]> {
        &self.bloom
    }

    /// Return a mutatively borrowed ref to the raw bloom of the BloomFilter.
    pub fn raw_bloom_mut(&mut self) -> &mut Box<bloomfilter::Bloom<[u8]>> {
        &mut self.bloom
    }

    pub fn number_of_bytes(&self) -> usize {
        std::mem::size_of::<BloomFilter>()
            + std::mem::size_of::<bloomfilter::Bloom<[u8]>>()
            + (self.bloom.len() / 8) as usize
    }

    /// Calculates the number of bytes that the bloom filter will require to be allocated.
    pub fn compute_size(capacity: i64, fp_rate: f64) -> usize {
        std::mem::size_of::<BloomFilter>()
            + std::mem::size_of::<bloomfilter::Bloom<[u8]>>()
            + bloomfilter::Bloom::<[u8]>::compute_bitmap_size(capacity as usize, fp_rate)
    }

    pub fn check(&self, item: &[u8]) -> bool {
        self.bloom.check(item)
    }

    pub fn set(&mut self, item: &[u8]) {
        self.bloom.set(item)
    }
}

impl Drop for BloomObject {
    fn drop(&mut self) {
        metrics::BLOOM_OBJECT_TOTAL_MEMORY_BYTES.fetch_sub(
            self.bloom_object_memory_usage(),
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
            .fetch_sub(self.num_items as u64, std::sync::atomic::Ordering::Relaxed);
        metrics::BLOOM_CAPACITY_ACROSS_OBJECTS
            .fetch_sub(self.capacity as u64, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::TIGHTENING_RATIO_DEFAULT;
    use configs;
    use rand::{distributions::Alphanumeric, Rng};
    use rstest::rstest;

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
    /// There is an option to pass in an expected error and assert that we throw that error
    /// Returns the number of errors (false positives) and the final item index.
    fn add_items_till_capacity(
        bf: &mut BloomObject,
        capacity_needed: i64,
        starting_item_idx: i64,
        rand_prefix: &String,
        expected_error: Option<BloomError>,
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
                    if let Some(err) = expected_error {
                        panic!(
                            "Expected error on the bloom object during during item add: {:?}",
                            err
                        );
                    }
                    cardinality += 1;
                }
                Ok(i64::MIN..=-1_i64) | Ok(2_i64..=i64::MAX) => {
                    panic!("We do not expect add_item to return any Integer other than 0 or 1.")
                }
                Err(e) => match &expected_error {
                    Some(expected) => {
                        assert_eq!(&e, expected, "Error doesn't match the expected error");
                        break;
                    }
                    None => {
                        panic!("Unexpected error when adding items: {:?}", e);
                    }
                },
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
        bf: &BloomObject,
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
        original_bloom_object: &BloomObject,
        restored_bloom_object: &BloomObject,
        add_operation_idx: i64,
        expected_fp_rate: f64,
        fp_margin: f64,
        rand_prefix: &String,
    ) {
        let is_seed_random = original_bloom_object.is_seed_random;
        assert_eq!(
            restored_bloom_object.is_seed_random,
            original_bloom_object.is_seed_random
        );
        let original_filter_seed = original_bloom_object.filters.first().unwrap().seed();
        assert_eq!(original_filter_seed, original_bloom_object.seed(),);
        if is_seed_random {
            assert_ne!(original_filter_seed, configs::FIXED_SEED);
            assert!(restored_bloom_object.filters.iter().all(|restore_filter| {
                original_bloom_object.filters.iter().any(|filter| {
                    (filter.seed() == restore_filter.seed())
                        && (restore_filter.seed() == original_filter_seed)
                })
            }));
        } else {
            assert!(restored_bloom_object.filters.iter().all(|restore_filter| {
                original_bloom_object.filters.iter().any(|filter| {
                    (filter.seed() == restore_filter.seed())
                        && (restore_filter.seed() == configs::FIXED_SEED)
                })
            }));
        }
        assert_eq!(restored_bloom_object.fp_rate, original_bloom_object.fp_rate);
        assert_eq!(
            restored_bloom_object.tightening_ratio,
            original_bloom_object.tightening_ratio
        );
        assert_eq!(
            restored_bloom_object.capacity(),
            original_bloom_object.capacity()
        );
        assert_eq!(
            restored_bloom_object.cardinality(),
            original_bloom_object.cardinality(),
        );
        assert_eq!(
            restored_bloom_object.free_effort(),
            original_bloom_object.free_effort()
        );
        assert_eq!(
            restored_bloom_object.memory_usage(),
            original_bloom_object.memory_usage()
        );
        assert!(restored_bloom_object
            .filters
            .iter()
            .all(|restore_filter| original_bloom_object
                .filters
                .iter()
                .any(|filter| filter.bloom.number_of_hash_functions()
                    == restore_filter.bloom.number_of_hash_functions())));
        assert!(restored_bloom_object
            .filters
            .iter()
            .all(|restore_filter| original_bloom_object
                .filters
                .iter()
                .any(|filter| filter.bloom.as_slice() == restore_filter.bloom.as_slice())));
        let (error_count, _) = check_items_exist(
            restored_bloom_object,
            1,
            add_operation_idx,
            true,
            rand_prefix,
        );
        assert!(error_count == 0);
        let (error_count, num_operations) = check_items_exist(
            restored_bloom_object,
            add_operation_idx + 1,
            add_operation_idx * 2,
            false,
            rand_prefix,
        );
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin);
    }

    #[rstest(
        seed,
        case::random_seed((None, true)),
        case::fixed_seed((Some(configs::FIXED_SEED), false))
    )]
    fn test_non_scaling_filter(seed: (Option<[u8; 32]>, bool)) {
        let rand_prefix = random_prefix(7);
        // 1 in every 1000 operations is expected to be a false positive.
        let expected_fp_rate: f64 = 0.001;
        let tightening_ratio: f64 = 0.5;
        let initial_capacity = 10000;
        // Expansion of 0 indicates non scaling.
        let expansion = 0;
        // Validate the non scaling behavior of the bloom filter.
        let mut bf = BloomObject::new_reserved(
            expected_fp_rate,
            tightening_ratio,
            initial_capacity,
            expansion,
            seed,
            true,
        )
        .expect("Expect bloom creation to succeed");
        let (error_count, add_operation_idx) =
            add_items_till_capacity(&mut bf, initial_capacity, 1, &rand_prefix, None);
        // Check adding to a full non scaling filter will throw an error
        add_items_till_capacity(
            &mut bf,
            initial_capacity + 1,
            add_operation_idx + 1,
            &rand_prefix,
            Some(BloomError::NonScalingFilterFull),
        );
        assert_eq!(bf.capacity(), initial_capacity);
        assert_eq!(bf.cardinality(), initial_capacity);
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
        let restore_bf = BloomObject::create_copy_from(&bf);
        add_items_till_capacity(
            &mut bf,
            initial_capacity + 1,
            add_operation_idx + 1,
            &rand_prefix,
            Some(BloomError::NonScalingFilterFull),
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

    #[rstest(
        seed,
        case::random_seed((None, true)),
        case::fixed_seed((Some(configs::FIXED_SEED), false))
    )]
    fn test_scaling_filter(seed: (Option<[u8; 32]>, bool)) {
        let rand_prefix = random_prefix(7);
        // 1 in every 1000 operations is expected to be a false positive.
        let expected_fp_rate: f64 = 0.001;
        let tightening_ratio: f64 = 0.5;
        let initial_capacity = 10000;
        let expansion = 2;
        let num_filters_to_scale = 5;
        let mut bf = BloomObject::new_reserved(
            expected_fp_rate,
            tightening_ratio,
            initial_capacity,
            expansion,
            seed,
            true,
        )
        .expect("Expect bloom creation to succeed");
        assert_eq!(bf.capacity(), initial_capacity);
        assert_eq!(bf.cardinality(), 0);
        let mut total_error_count = 0;
        let mut add_operation_idx = 0;
        // Validate the scaling behavior of the bloom filter.
        for filter_idx in 1..=num_filters_to_scale {
            let filter_expansion: i64 = (expansion.pow(filter_idx) - 1).into();
            let expected_total_capacity = initial_capacity * filter_expansion;
            let (error_count, new_add_operation_idx) = add_items_till_capacity(
                &mut bf,
                expected_total_capacity,
                add_operation_idx + 1,
                &rand_prefix,
                None,
            );
            add_operation_idx = new_add_operation_idx;
            total_error_count += error_count;
            assert_eq!(bf.capacity(), expected_total_capacity);
            assert_eq!(bf.cardinality(), expected_total_capacity);
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
        let restore_bloom_object = BloomObject::create_copy_from(&bf);
        verify_restored_items(
            &bf,
            &restore_bloom_object,
            add_operation_idx,
            expected_fp_rate,
            fp_margin,
            &rand_prefix,
        );
    }

    #[test]
    fn test_seed() {
        // When using the with_fixed_seed API, the sip keys generated should be equal to the constants from configs.rs
        let test_bloom_filter1 =
            BloomFilter::with_fixed_seed(0.5_f64, 1000_i64, &configs::FIXED_SEED);
        let test_seed1 = test_bloom_filter1.seed();
        assert_eq!(test_seed1, configs::FIXED_SEED);
        // When using the with_random_seed API, the sip keys generated should not be equal to the constant sip_keys.
        let test_bloom_filter2 = BloomFilter::with_random_seed(0.5_f64, 1000_i64);
        let test_seed2 = test_bloom_filter2.seed();
        assert_ne!(test_seed2, configs::FIXED_SEED);
        // Check that the random seed changes for each BloomFilter
        let test_bloom_filter3 = BloomFilter::with_random_seed(0.5_f64, 1000_i64);
        let test_seed3 = test_bloom_filter3.seed();
        assert_ne!(test_seed2, test_seed3);
    }

    #[test]
    fn test_exceeded_size_limit() {
        // Validate that bloom filter allocations within bloom objects are rejected if their memory usage would be beyond
        // the configured limit.
        let result = BloomObject::new_reserved(0.5_f64, 0.5_f64, i64::MAX, 1, (None, true), true);
        assert_eq!(result.err(), Some(BloomError::ExceedsMaxBloomSize));
        let capacity = 76000000;
        // With the capacity and fp rate, the memory usage will be roughly 130MB which is greater than the allowed limit.
        assert!(!BloomObject::validate_size_before_create(
            capacity, 0.001_f64
        ));
        let result2 =
            BloomObject::new_reserved(0.001_f64, 0.5_f64, capacity, 1, (None, true), true);
        assert_eq!(result2.err(), Some(BloomError::ExceedsMaxBloomSize));
    }

    #[rstest]
    #[case(1000, 0.01, 10000, 2, 15000)]
    #[case(10000, 0.001, 100000, 4, 210000)]
    #[case(50000, 0.0001, 500000, 3, 650000)]
    #[case(100000, 0.00001, 1000000, 2, 1500000)]
    #[case(100, 0.00001, 1000, 1, 1000)]
    fn test_calculate_max_scaled_capacity(
        #[case] capacity: i64,
        #[case] fp_rate: f64,
        #[case] validate_scale_to: i64,
        #[case] expansion: u32,
        #[case] resulting_size: i64,
    ) {
        // Validate that max scaled capacity returns the correct capacity reached when a valid validate_scale_to to is provided
        let returned_size = BloomObject::calculate_max_scaled_capacity(
            capacity,
            fp_rate,
            validate_scale_to,
            TIGHTENING_RATIO_DEFAULT
                .parse()
                .expect("global config should always be 0.5"),
            expansion,
        );
        assert_eq!(resulting_size, returned_size.unwrap());
        // Test that with a -1 validate_scale_to the returned value will be the max capacity
        let max_returned_size = BloomObject::calculate_max_scaled_capacity(
            capacity,
            fp_rate,
            -1,
            TIGHTENING_RATIO_DEFAULT
                .parse()
                .expect("global config should always be 0.5"),
            expansion,
        );
        // Check that 1 more than the max will trigger the error cases
        let failed_returned_size = BloomObject::calculate_max_scaled_capacity(
            capacity,
            fp_rate,
            max_returned_size.unwrap() + 1,
            TIGHTENING_RATIO_DEFAULT
                .parse()
                .expect("global config should always be 0.5"),
            expansion,
        );
        if expansion == 1 {
            // FP rate reaches 0 case
            assert!(failed_returned_size
                .unwrap_err()
                .as_str()
                .contains("provided VALIDATESCALETO causes false positive to degrade to 0"));
        } else {
            // Exceeds memory limit case
            assert!(failed_returned_size
                .unwrap_err()
                .as_str()
                .contains("provided VALIDATESCALETO causes bloom object to exceed memory limit"));
        }
    }

    #[rstest(expansion, case::nonscaling(0), case::scaling(2))]
    fn test_bf_encode_and_decode(expansion: u32) {
        let mut bf =
            BloomObject::new_reserved(0.5_f64, 0.5_f64, 1000_i64, expansion, (None, true), true)
                .unwrap();
        let item = "item1";
        let _ = bf.add_item(item.as_bytes(), true);
        // action
        let encoder_result = bf.encode_object();
        // assert encode success
        assert!(encoder_result.is_ok());
        let vec = encoder_result.unwrap();
        // assert decode success:
        let new_bf_result = BloomObject::decode_object(&vec, true);
        let new_bf = new_bf_result.unwrap();
        // verify new_bf and bf
        assert_eq!(bf.fp_rate, new_bf.fp_rate);
        assert_eq!(bf.tightening_ratio, new_bf.tightening_ratio);
        assert_eq!(bf.expansion, new_bf.expansion);
        assert_eq!(bf.capacity(), new_bf.capacity());
        // verify item1 exists.
        assert!(new_bf.item_exists(item.as_bytes()));
    }

    #[test]
    fn test_bf_decode_when_unsupported_version_should_failed() {
        // arrange: prepare bloom filter
        let mut bf =
            BloomObject::new_reserved(0.5_f64, 0.5_f64, 1000_i64, 2, (None, true), true).unwrap();
        let key = "key";
        let _ = bf.add_item(key.as_bytes(), true).unwrap();

        let encoder_result = bf.encode_object();
        assert!(encoder_result.is_ok());

        // 1. unsupport version should return error
        let mut vec = encoder_result.unwrap();
        vec[0] = 10;

        // assert decode:
        // should return error
        assert_eq!(
            BloomObject::decode_object(&vec, true).err(),
            Some(BloomError::DecodeUnsupportedVersion)
        );
    }

    #[test]
    fn test_bf_decode_when_bytes_is_empty_should_failed() {
        // arrange: prepare bloom filter
        let mut bf =
            BloomObject::new_reserved(0.5_f64, 0.5_f64, 1000_i64, 2, (None, true), true).unwrap();
        let key = "key";
        let _ = bf.add_item(key.as_bytes(), true);

        let encoder_result = bf.encode_object();
        assert!(encoder_result.is_ok());

        // 1. empty vec should return error
        let vec: Vec<u8> = Vec::new();
        // assert decode:
        // should return error
        assert_eq!(
            BloomObject::decode_object(&vec, true).err(),
            Some(BloomError::DecodeBloomFilterFailed)
        );
    }

    #[test]
    fn test_bf_decode_when_bytes_is_exceed_limit_should_failed() {
        // arrange: prepare bloom filter
        let mut bf =
            BloomObject::new_reserved(0.5_f64, 0.5_f64, 1000_i64, 2, (None, true), true).unwrap();
        let key = "key";
        let _ = bf.add_item(key.as_bytes(), true);
        let origin_fp_rate = bf.fp_rate;

        // unsupport fp_rate
        bf.fp_rate = -0.5;
        let vec = bf.encode_object().unwrap();
        // should return error
        assert_eq!(
            BloomObject::decode_object(&vec, true).err(),
            Some(BloomError::ErrorRateRange)
        );
        bf.fp_rate = origin_fp_rate;

        // build a larger than 64mb filter
        let extra_large_filter =
            BloomObject::new_reserved(0.01_f64, 0.5_f64, 114000000, 2, (None, true), false)
                .unwrap();
        let vec = extra_large_filter.encode_object().unwrap();
        // should return error
        assert_eq!(
            BloomObject::decode_object(&vec, true).err(),
            Some(BloomError::ExceedsMaxBloomSize)
        );
    }

    #[test]
    fn test_vec_capacity_matches_size_calculations() {
        // This unit test is designed to make sure out calculations with capcity will always match the correct vec capacity
        let mut test_v = vec![0];
        for i in 0..5000 {
            let x = if i == 0 {
                1
            } else {
                (std::cmp::max(4, i + 1) as u32).next_power_of_two()
            };
            assert!(test_v.capacity() == x as usize);
            test_v.push(i);
        }
    }
}
