use crate::configs::MAX_FILTERS_PER_OBJ;
use crate::configs::TIGHTENING_RATIO;
use bloomfilter;

/// KeySpace Notification Events
pub const ADD_EVENT: &str = "bloom.add";
pub const RESERVE_EVENT: &str = "bloom.reserve";

/// Errors
pub const ERROR: &str = "ERROR";
pub const NON_SCALING_FILTER_FULL: &str = "ERR non scaling filter is full";
pub const NOT_FOUND: &str = "ERR not found";
pub const ITEM_EXISTS: &str = "ERR item exists";
pub const INVALID_INFO_VALUE: &str = "ERR Invalid information value";
pub const BAD_EXPANSION: &str = "ERR bad expansion";
pub const BAD_CAPACITY: &str = "ERR bad capacity";
pub const BAD_ERROR_RATE: &str = "ERR bad error rate";
pub const ERROR_RATE_RANGE: &str = "ERR (0 < error rate range < 1)";
pub const CAPACITY_LARGER_THAN_0: &str = "ERR (capacity should be larger than 0)";
pub const MAX_NUM_SCALING_FILTERS: &str = "ERR max number of scaling filters reached";

pub enum BloomError {
    NonScalingFilterFull,
    MaxNumScalingFilters,
}

impl BloomError {
    pub fn as_str(&self) -> &'static str {
        match self {
            BloomError::NonScalingFilterFull => NON_SCALING_FILTER_FULL,
            BloomError::MaxNumScalingFilters => MAX_NUM_SCALING_FILTERS,
        }
    }
}

/// The BloomFilterType structure. 32 bytes.
/// Can contain one or more filters.
/// This is a generic top level structure which is not coupled to any bloom crate.
pub struct BloomFilterType {
    pub expansion: u32,
    pub fp_rate: f32,
    pub filters: Vec<BloomFilter>,
}

impl BloomFilterType {
    /// Create a new BloomFilterType object.
    pub fn new_reserved(fp_rate: f32, capacity: u32, expansion: u32) -> BloomFilterType {
        let bloom = BloomFilter::new(fp_rate, capacity);
        let filters = vec![bloom];
        BloomFilterType {
            expansion,
            fp_rate,
            filters,
        }
    }

    /// Create a new BloomFilterType object from an existing one.
    pub fn create_copy_from(from_bf: &BloomFilterType) -> BloomFilterType {
        let mut filters = Vec::new();
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
    pub fn get_memory_usage(&self) -> usize {
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
        if self.filters.is_empty() {
            return 1;
        }
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
    pub fn add_item(&mut self, item: &[u8]) -> Result<i64, BloomError> {
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
                return Ok(1);
            }
            // Non Scaling Filters that are filled to capacity cannot handle more inserts.
            if self.expansion == 0 {
                return Err(BloomError::NonScalingFilterFull);
            }
            if num_filters == MAX_FILTERS_PER_OBJ {
                return Err(BloomError::MaxNumScalingFilters);
            }
            // Scale out by adding a new filter with capacity bounded within the u32 range.
            let new_fp_rate = self.fp_rate * TIGHTENING_RATIO.powi(num_filters);
            let new_capacity = match filter.capacity.checked_mul(self.expansion) {
                Some(new_capacity) => new_capacity,
                None => u32::MAX,
            };
            let mut new_filter = BloomFilter::new(new_fp_rate, new_capacity);
            // Add item.
            new_filter.set(item);
            new_filter.num_items += 1;
            self.filters.push(new_filter);
            return Ok(1);
        }
        Ok(0)
    }
}

// Structure representing a single bloom filter. 200 Bytes.
// Using Crate: "bloomfilter"
// The reason for using u32 for num_items and capacity is because
// we have a limit on the memory usage of a `BloomFilter` to be 64MB.
// Based on this, we expect the number of items on the `BloomFilter` to be
// well within the u32::MAX limit.
pub struct BloomFilter {
    pub bloom: bloomfilter::Bloom<[u8]>,
    pub num_items: u32,
    pub capacity: u32,
}

impl BloomFilter {
    /// Instantiate empty BloomFilter object.
    pub fn new(fp_rate: f32, capacity: u32) -> BloomFilter {
        let bloom = bloomfilter::Bloom::new_for_fp_rate(capacity as usize, fp_rate as f64);
        BloomFilter {
            bloom,
            num_items: 0,
            capacity,
        }
    }

    /// Create a new BloomFilter from dumped information (RDB load).
    pub fn from_existing(
        bitmap: &[u8],
        number_of_bits: u64,
        number_of_hash_functions: u32,
        sip_keys: [(u64, u64); 2],
        num_items: u32,
        capacity: u32,
    ) -> BloomFilter {
        let bloom = bloomfilter::Bloom::from_existing(
            bitmap,
            number_of_bits,
            number_of_hash_functions,
            sip_keys,
        );
        BloomFilter {
            bloom,
            num_items,
            capacity,
        }
    }

    pub fn number_of_bytes(&self) -> usize {
        std::mem::size_of::<BloomFilter>() + (self.bloom.number_of_bits() / 8) as usize
    }

    pub fn check(&self, item: &[u8]) -> bool {
        self.bloom.check(item)
    }

    pub fn set(&mut self, item: &[u8]) {
        self.bloom.set(item)
    }

    /// Create a new BloomFilter from an existing BloomFilter object (COPY command).
    pub fn create_copy_from(bf: &BloomFilter) -> BloomFilter {
        BloomFilter::from_existing(
            &bf.bloom.bitmap(),
            bf.bloom.number_of_bits(),
            bf.bloom.number_of_hash_functions(),
            bf.bloom.sip_keys(),
            bf.num_items,
            bf.capacity,
        )
    }
}
