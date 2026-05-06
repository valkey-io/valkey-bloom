use crate::configs;
use cuckoofilter::CuckooFilter as ExternalCuckooFilter;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

/// Used for decoding and encoding `CuckooObject`. Must match CUCKOO_TYPE_ENCODING_VERSION in data_type.rs.
pub const CUCKOO_OBJECT_VERSION: u8 = 1;

/// KeySpace Notification Events
pub const ADD_EVENT: &str = "cuckoo.add";
pub const CREATE_EVENT: &str = "cuckoo.create";
pub const RESERVE_EVENT: &str = "cuckoo.reserve";
pub const DEL_EVENT: &str = "cuckoo.del";
pub const INSERT_EVENT: &str = "cuckoo.insert";
pub const LOAD_EVENT: &str = "cuckoo.load";

/// Client Errors
pub const ERROR: &str = "ERROR";
pub const FILTER_FULL: &str = "ERR cuckoo filter is full";
pub const NON_SCALING_FILTER_FULL: &str = "ERR non scaling cuckoo filter is full";
pub const NOT_FOUND: &str = "ERR not found";
pub const ITEM_EXISTS: &str = "ERR item exists";
pub const INVALID_INFO_VALUE: &str = "ERR invalid information value";
pub const BAD_EXPANSION: &str = "ERR bad expansion";
pub const BAD_CAPACITY: &str = "ERR bad capacity";
pub const BAD_BUCKET_SIZE: &str = "ERR bad bucket size";
pub const BAD_MAX_KICKS: &str = "ERR bad max kicks";
pub const BAD_MAX_ITERATIONS: &str = "ERR bad max iterations";
pub const BUCKET_SIZE_RANGE: &str = "ERR (bucket size must be between 1 and 255)";
pub const CAPACITY_LARGER_THAN_0: &str = "ERR (capacity should be larger than 0)";
pub const CAPACITY_OUT_OF_RANGE: &str = "ERR capacity must be between min and max";
pub const CAPACITY_MUST_BE_LARGER_THAN_ZERO: &str = "ERR capacity must be larger than 0";
pub const BUCKET_SIZE_OUT_OF_RANGE: &str = "ERR bucket size must be between min and max";
pub const MAX_KICKS_OUT_OF_RANGE: &str = "ERR max kicks must be between min and max";
pub const CAPACITY_ARG_REQUIRED: &str = "ERR CAPACITY requires an argument";
pub const BUCKET_SIZE_ARG_REQUIRED: &str = "ERR BUCKETSIZE requires an argument";
pub const MAX_ITERATIONS_ARG_REQUIRED: &str = "ERR MAXITERATIONS requires an argument";
pub const EXPANSION_ARG_REQUIRED: &str = "ERR EXPANSION requires an argument";
pub const ITEMS_KEYWORD_REQUIRED: &str = "ERR ITEMS keyword required";
pub const UNKNOWN_OPTION_OR_MISSING_ITEMS: &str = "ERR unknown option or missing ITEMS keyword";
pub const UNKNOWN_ARGUMENT: &str = "ERR unknown argument received";
pub const UNKNOWN_OPTION: &str = "ERR unknown option";
pub const EXCEEDS_MAX_CUCKOO_SIZE: &str = "ERR operation exceeds cuckoo object memory limit";
pub const MAX_NUM_SCALING_FILTERS: &str = "ERR cuckoo object reached max number of filters";
pub const KEY_EXISTS: &str = "BUSYKEY Target key name already exists.";
pub const DECODE_CUCKOO_OBJECT_FAILED: &str = "ERR cuckoo object decoding failed";
pub const DECODE_UNSUPPORTED_VERSION: &str =
    "ERR cuckoo object decoding failed. Unsupported version";
pub const NO_ITEMS_SPECIFIED: &str = "ERR no items specified";
pub const FAILED_TO_SET_FILTER: &str = "ERR failed to set cuckoo filter";

/// Logging Error messages
pub const ENCODE_CUCKOO_OBJECT_FAILED: &str = "Failed to encode cuckoo object.";

/// Max number of filters allowed within a cuckoo object.
pub const CUCKOO_NUM_FILTERS_PER_OBJECT_LIMIT_MAX: i32 = i32::MAX;

pub const MIN_BUCKET_SIZE: usize = 1;
pub const MAX_BUCKET_SIZE: usize = 255;

#[derive(Debug, PartialEq)]
pub enum CuckooError {
    FilterFull,
    NotFound,
    ExceedsMaxSize,
    InvalidParameter,
    SerializationError,
    MaxNumScalingFilters,
    BadCapacity,
    BadBucketSize,
    BadMaxKicks,
    BadExpansion,
    NonScalingFilterFull,
    EncodeFilterFailed,
    DecodeFilterFailed,
    DecodeUnsupportedVersion,
}

impl CuckooError {
    pub fn as_str(&self) -> &'static str {
        match self {
            CuckooError::FilterFull => FILTER_FULL,
            CuckooError::NotFound => NOT_FOUND,
            CuckooError::ExceedsMaxSize => EXCEEDS_MAX_CUCKOO_SIZE,
            CuckooError::InvalidParameter => ERROR,
            CuckooError::SerializationError => ENCODE_CUCKOO_OBJECT_FAILED,
            CuckooError::MaxNumScalingFilters => MAX_NUM_SCALING_FILTERS,
            CuckooError::BadCapacity => BAD_CAPACITY,
            CuckooError::BadBucketSize => BAD_BUCKET_SIZE,
            CuckooError::BadMaxKicks => BAD_MAX_KICKS,
            CuckooError::BadExpansion => BAD_EXPANSION,
            CuckooError::NonScalingFilterFull => NON_SCALING_FILTER_FULL,
            CuckooError::EncodeFilterFailed => ENCODE_CUCKOO_OBJECT_FAILED,
            CuckooError::DecodeFilterFailed => DECODE_CUCKOO_OBJECT_FAILED,
            CuckooError::DecodeUnsupportedVersion => DECODE_UNSUPPORTED_VERSION,
        }
    }
}

/// Top-level CuckooObject structure that can contain multiple filters for scaling
#[derive(Serialize, Deserialize)]
#[allow(clippy::vec_box)]
pub struct CuckooObject {
    expansion: u32,
    bucket_size: usize,
    max_kicks: u32,
    filters: Vec<Box<CuckooFilter>>,
}

impl CuckooObject {
    /// Create a new reserved CuckooObject
    pub fn new_reserved(
        capacity: i64,
        bucket_size: usize,
        max_kicks: u32,
        expansion: u32,
        validate_size_limit: bool,
    ) -> Result<CuckooObject, CuckooError> {
        if capacity <= 0 {
            return Err(CuckooError::BadCapacity);
        }
        if bucket_size < MIN_BUCKET_SIZE || bucket_size > MAX_BUCKET_SIZE {
            return Err(CuckooError::BadBucketSize);
        }
        if validate_size_limit && !CuckooObject::validate_size_before_create(capacity, bucket_size)
        {
            return Err(CuckooError::ExceedsMaxSize);
        }

        let filter = Box::new(CuckooFilter::new(capacity, bucket_size, max_kicks));
        let filters = vec![filter];

        let cuckoo = CuckooObject {
            expansion,
            bucket_size,
            max_kicks,
            filters,
        };

        cuckoo.cuckoo_object_incr_metrics_on_new_create();
        Ok(cuckoo)
    }

    /// Create a CuckooObject from existing data (RDB Load / Restore)
    pub fn from_existing(
        expansion: u32,
        bucket_size: usize,
        max_kicks: u32,
        filters: Vec<Box<CuckooFilter>>,
    ) -> CuckooObject {
        let cuckoo = CuckooObject {
            expansion,
            bucket_size,
            max_kicks,
            filters,
        };

        cuckoo.cuckoo_object_incr_metrics_on_new_create();
        cuckoo
    }

    /// Create a copy of an existing CuckooObject
    pub fn create_copy_from(from: &CuckooObject) -> CuckooObject {
        let mut filters: Vec<Box<CuckooFilter>> = Vec::with_capacity(from.filters.len());
        for filter in &from.filters {
            let new_filter = Box::new(CuckooFilter::create_copy_from(filter));
            filters.push(new_filter);
        }

        let new_copy = CuckooObject {
            expansion: from.expansion,
            bucket_size: from.bucket_size,
            max_kicks: from.max_kicks,
            filters,
        };

        new_copy.cuckoo_object_incr_metrics_on_new_create();
        new_copy
    }

    /// Add an item to the CuckooObject, with auto-scaling if enabled
    pub fn add_item(
        &mut self,
        item: &[u8],
        validate_size_limit: bool,
    ) -> Result<i64, CuckooError> {
        let num_filters = self.filters.len() as i32;
        if let Some(filter) = self.filters.last_mut() {
            match filter.add(item) {
                Ok(true) => {
                    use crate::metrics;
                    metrics::CUCKOO_NUM_ITEMS_ACROSS_OBJECTS.fetch_add(1, Ordering::Relaxed);
                    return Ok(1);
                }
                Ok(false) => {
                    return Ok(1);
                }
                Err(CuckooError::FilterFull) => {
                    if self.expansion == 0 {
                        return Err(CuckooError::NonScalingFilterFull);
                    }
                    if num_filters >= CUCKOO_NUM_FILTERS_PER_OBJECT_LIMIT_MAX {
                        return Err(CuckooError::MaxNumScalingFilters);
                    }

                    let new_capacity = match filter.capacity().checked_mul(self.expansion.into()) {
                        Some(cap) => cap,
                        None => return Err(CuckooError::BadCapacity),
                    };

                    if validate_size_limit
                        && !self.validate_size_before_scaling(new_capacity, self.bucket_size)
                    {
                        return Err(CuckooError::ExceedsMaxSize);
                    }

                    let memory_usage_before = self.cuckoo_object_memory_usage();
                    let mut new_filter =
                        Box::new(CuckooFilter::new(new_capacity, self.bucket_size, self.max_kicks));

                    match new_filter.add(item) {
                        Ok(_) => {
                            self.filters.push(new_filter);
                            let memory_usage_after = self.cuckoo_object_memory_usage();

                            use crate::metrics;
                            metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
                                memory_usage_after - memory_usage_before,
                                Ordering::Relaxed,
                            );
                            metrics::CUCKOO_NUM_ITEMS_ACROSS_OBJECTS.fetch_add(1, Ordering::Relaxed);
                            Ok(1)
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(0)
        }
    }

    /// Delete an item from the CuckooObject.
    /// Iterates in reverse so newer (larger) filters are checked first, matching insertion order.
    pub fn delete_item(&mut self, item: &[u8]) -> Result<i64, CuckooError> {
        for filter in self.filters.iter_mut().rev() {
            if filter.delete(item)? {
                use crate::metrics;
                metrics::CUCKOO_NUM_ITEMS_ACROSS_OBJECTS.fetch_sub(1, Ordering::Relaxed);
                return Ok(1);
            }
        }
        Ok(0)
    }

    /// Check if an item exists in any filter
    pub fn item_exists(&self, item: &[u8]) -> bool {
        self.filters.iter().any(|filter| filter.contains(item))
    }

    /// Count occurrences of an item across all filters
    pub fn count_item(&self, item: &[u8]) -> i64 {
        let mut total = 0i64;
        for filter in &self.filters {
            total += filter.count(item) as i64;
        }
        total
    }

    /// Get total memory usage
    pub fn memory_usage(&self) -> usize {
        let mut mem = self.cuckoo_object_memory_usage();
        for filter in &self.filters {
            mem += filter.number_of_bytes();
        }
        mem
    }

    fn cuckoo_object_memory_usage(&self) -> usize {
        CuckooObject::compute_size(self.filters.capacity())
    }

    pub fn compute_size(filters_vec_capacity: usize) -> usize {
        std::mem::size_of::<CuckooObject>()
            + (filters_vec_capacity * std::mem::size_of::<Box<CuckooFilter>>())
    }

    pub fn capacity(&self) -> i64 {
        self.filters.iter().map(|f| f.capacity()).sum()
    }

    pub fn num_items(&self) -> i64 {
        self.filters.iter().map(|f| f.num_items()).sum()
    }

    pub fn num_filters(&self) -> usize {
        self.filters.len()
    }

    pub fn expansion(&self) -> u32 {
        self.expansion
    }

    pub fn bucket_size(&self) -> usize {
        self.bucket_size
    }

    pub fn max_kicks(&self) -> u32 {
        self.max_kicks
    }

    pub fn starting_capacity(&self) -> i64 {
        self.filters
            .first()
            .expect("Every CuckooObject is expected to have at least one filter")
            .capacity()
    }

    pub fn free_effort(&self) -> usize {
        self.filters.len()
    }

    pub fn filters(&self) -> &Vec<Box<CuckooFilter>> {
        &self.filters
    }

    pub fn filters_mut(&mut self) -> &mut Vec<Box<CuckooFilter>> {
        &mut self.filters
    }

    fn validate_size_before_create(capacity: i64, bucket_size: usize) -> bool {
        let bytes = std::mem::size_of::<CuckooObject>()
            + std::mem::size_of::<Box<CuckooFilter>>()
            + CuckooFilter::compute_size(capacity, bucket_size);
        CuckooObject::validate_size(bytes)
    }

    fn validate_size_before_scaling(&self, new_capacity: i64, bucket_size: usize) -> bool {
        let bytes = self.memory_usage() + CuckooFilter::compute_size(new_capacity, bucket_size);
        CuckooObject::validate_size(bytes)
    }

    pub fn validate_size(bytes: usize) -> bool {
        bytes <= configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT.load(Ordering::Relaxed) as usize
    }

    pub fn encode_object(&self) -> Result<Vec<u8>, CuckooError> {
        match bincode::serialize(self) {
            Ok(vec) => {
                let mut final_vec = Vec::with_capacity(1 + vec.len());
                final_vec.push(CUCKOO_OBJECT_VERSION);
                final_vec.extend(vec);
                Ok(final_vec)
            }
            Err(_) => Err(CuckooError::EncodeFilterFailed),
        }
    }

    pub fn decode_object(
        decoded_bytes: &[u8],
        validate_size_limit: bool,
    ) -> Result<CuckooObject, CuckooError> {
        if decoded_bytes.is_empty() {
            return Err(CuckooError::DecodeFilterFailed);
        }

        let version = decoded_bytes[0];
        match version {
            1 => {
                let (expansion, bucket_size, max_kicks, filters): (
                    u32,
                    usize,
                    u32,
                    Vec<Box<CuckooFilter>>,
                ) = match bincode::deserialize::<(u32, usize, u32, Vec<Box<CuckooFilter>>)>(
                    &decoded_bytes[1..],
                ) {
                    Ok(values) => {
                        use crate::metrics;
                        for filter in &values.3 {
                            metrics::CUCKOO_NUM_ITEMS_ACROSS_OBJECTS.fetch_add(
                                filter.num_items as u64,
                                Ordering::Relaxed,
                            );
                            filter.cuckoo_filter_incr_metrics_on_new_create();
                        }

                        if values.1 < MIN_BUCKET_SIZE || values.1 > MAX_BUCKET_SIZE {
                            return Err(CuckooError::BadBucketSize);
                        }
                        if values.3.len() >= CUCKOO_NUM_FILTERS_PER_OBJECT_LIMIT_MAX as usize {
                            return Err(CuckooError::MaxNumScalingFilters);
                        }

                        values
                    }
                    Err(_) => {
                        return Err(CuckooError::DecodeFilterFailed);
                    }
                };

                let item = CuckooObject {
                    expansion,
                    bucket_size,
                    max_kicks,
                    filters,
                };

                item.cuckoo_object_incr_metrics_on_new_create();

                let bytes = item.memory_usage();
                if validate_size_limit && !CuckooObject::validate_size(bytes) {
                    return Err(CuckooError::ExceedsMaxSize);
                }

                Ok(item)
            }
            _ => Err(CuckooError::DecodeUnsupportedVersion),
        }
    }

    fn cuckoo_object_incr_metrics_on_new_create(&self) {
        use crate::metrics;
        metrics::CUCKOO_NUM_OBJECTS.fetch_add(1, Ordering::Relaxed);
        metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
            self.cuckoo_object_memory_usage(),
            Ordering::Relaxed,
        );
    }

    fn cuckoo_object_decr_metrics_on_drop(&self) {
        use crate::metrics;
        metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES.fetch_sub(
            self.cuckoo_object_memory_usage(),
            Ordering::Relaxed,
        );
        metrics::CUCKOO_NUM_OBJECTS.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for CuckooObject {
    fn drop(&mut self) {
        self.cuckoo_object_decr_metrics_on_drop();
    }
}

/// Individual cuckoo filter wrapper that tracks item counts
#[derive(Serialize, Deserialize)]
pub struct CuckooFilter {
    #[serde(skip)]
    filter: ExternalCuckooFilter<DefaultHasher>,
    occurrence_map: HashMap<Vec<u8>, u32>,
    capacity: i64,
    num_items: i64,
    bucket_size: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    serialized_data: Option<Vec<u8>>,
}

impl CuckooFilter {
    pub fn new(capacity: i64, bucket_size: usize, _max_kicks: u32) -> CuckooFilter {
        let filter = ExternalCuckooFilter::with_capacity(capacity as usize);

        let cf = CuckooFilter {
            filter,
            occurrence_map: HashMap::new(),
            capacity,
            num_items: 0,
            bucket_size,
            serialized_data: None,
        };

        cf.cuckoo_filter_incr_metrics_on_new_create();
        cf
    }

    pub fn from_existing(
        capacity: i64,
        num_items: i64,
        bucket_size: usize,
        occurrence_map: HashMap<Vec<u8>, u32>,
        serialized_data: Vec<u8>,
    ) -> CuckooFilter {
        let mut filter = ExternalCuckooFilter::with_capacity(capacity as usize);

        // Rebuild the filter by re-adding each unique item from the occurrence_map
        for key in occurrence_map.keys() {
            let _ = filter.add(key.as_slice());
        }

        let cf = CuckooFilter {
            filter,
            occurrence_map,
            capacity,
            num_items,
            bucket_size,
            serialized_data: Some(serialized_data),
        };

        cf.cuckoo_filter_incr_metrics_on_new_create();
        cf
    }

    /// Returns Ok(true) if added as a new item, Ok(false) if it already existed
    pub fn add(&mut self, item: &[u8]) -> Result<bool, CuckooError> {
        if self.filter.contains(item) {
            *self.occurrence_map.entry(item.to_vec()).or_insert(0) += 1;
            return Ok(false);
        }

        if self.filter.add(item).is_ok() {
            self.num_items += 1;
            *self.occurrence_map.entry(item.to_vec()).or_insert(0) += 1;
            Ok(true)
        } else {
            Err(CuckooError::FilterFull)
        }
    }

    pub fn contains(&self, item: &[u8]) -> bool {
        self.filter.contains(item)
    }

    /// Returns Ok(true) if deleted, Ok(false) if not found
    pub fn delete(&mut self, item: &[u8]) -> Result<bool, CuckooError> {
        if self.filter.delete(item) {
            self.num_items -= 1;

            if let Some(count) = self.occurrence_map.get_mut(item) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    self.occurrence_map.remove(item);
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn count(&self, item: &[u8]) -> u32 {
        self.occurrence_map.get(item).copied().unwrap_or(0)
    }

    pub fn number_of_bytes(&self) -> usize {
        let base_size = std::mem::size_of::<CuckooFilter>();
        let map_size = self.occurrence_map.len()
            * (std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<u32>());
        let map_keys_size: usize = self.occurrence_map.keys().map(|k| k.len()).sum();
        let filter_size = (self.capacity as usize) * self.bucket_size;
        base_size + map_size + map_keys_size + filter_size
    }

    pub fn compute_size(capacity: i64, bucket_size: usize) -> usize {
        std::mem::size_of::<CuckooFilter>() + (capacity as usize) * bucket_size
    }

    pub fn create_copy_from(from: &CuckooFilter) -> CuckooFilter {
        CuckooFilter {
            filter: ExternalCuckooFilter::with_capacity(from.capacity as usize),
            occurrence_map: from.occurrence_map.clone(),
            capacity: from.capacity,
            num_items: from.num_items,
            bucket_size: from.bucket_size,
            serialized_data: from.serialized_data.clone(),
        }
    }

    pub fn capacity(&self) -> i64 {
        self.capacity
    }

    pub fn num_items(&self) -> i64 {
        self.num_items
    }

    pub fn bucket_size(&self) -> usize {
        self.bucket_size
    }

    pub fn occurrence_map(&self) -> &HashMap<Vec<u8>, u32> {
        &self.occurrence_map
    }

    pub fn get_serialized_data(&self) -> Vec<u8> {
        if let Some(ref data) = self.serialized_data {
            data.clone()
        } else {
            Vec::new()
        }
    }

    fn cuckoo_filter_incr_metrics_on_new_create(&self) {
        use crate::metrics;
        metrics::CUCKOO_NUM_FILTERS_ACROSS_OBJECTS.fetch_add(1, Ordering::Relaxed);
        metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_add(self.number_of_bytes(), Ordering::Relaxed);
        metrics::CUCKOO_CAPACITY_ACROSS_OBJECTS
            .fetch_add(self.capacity as u64, Ordering::Relaxed);
    }
}

impl Drop for CuckooFilter {
    fn drop(&mut self) {
        use crate::metrics;
        metrics::CUCKOO_NUM_FILTERS_ACROSS_OBJECTS.fetch_sub(1, Ordering::Relaxed);
        metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_sub(self.number_of_bytes(), Ordering::Relaxed);
        metrics::CUCKOO_NUM_ITEMS_ACROSS_OBJECTS
            .fetch_sub(self.num_items as u64, Ordering::Relaxed);
        metrics::CUCKOO_CAPACITY_ACROSS_OBJECTS
            .fetch_sub(self.capacity as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_BUCKET_SIZE: usize = crate::configs::CUCKOO_BUCKET_SIZE_DEFAULT as usize;
    const DEFAULT_MAX_KICKS: u32 = crate::configs::CUCKOO_MAX_KICKS_DEFAULT as u32;

    #[test]
    fn test_cuckoo_filter_basic_operations() {
        let mut cf = CuckooFilter::new(1000, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS);

        let item = b"test_item";
        assert_eq!(cf.add(item).unwrap(), true);
        assert_eq!(cf.num_items(), 1);

        assert!(cf.contains(item));

        assert_eq!(cf.add(item).unwrap(), false);

        assert_eq!(cf.delete(item).unwrap(), true);
        assert_eq!(cf.num_items(), 0);
        assert!(!cf.contains(item));

        assert_eq!(cf.delete(item).unwrap(), false);
    }

    #[test]
    fn test_cuckoo_object_basic_operations() {
        let mut co =
            CuckooObject::new_reserved(1000, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS, 0, false)
                .unwrap();

        let item = b"test_item";

        assert_eq!(co.add_item(item, false).unwrap(), 1);
        assert_eq!(co.num_items(), 1);
        assert!(co.item_exists(item));

        assert_eq!(co.add_item(item, false).unwrap(), 0);

        assert_eq!(co.delete_item(item).unwrap(), 1);
        assert_eq!(co.num_items(), 0);
        assert!(!co.item_exists(item));
    }

    #[test]
    fn test_cuckoo_object_capacity_and_memory() {
        let co =
            CuckooObject::new_reserved(1000, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS, 2, false)
                .unwrap();

        assert_eq!(co.capacity(), 1000);
        assert_eq!(co.num_filters(), 1);
        assert!(co.memory_usage() > 0);
    }

    #[test]
    fn test_cuckoo_filter_count() {
        let mut cf = CuckooFilter::new(1000, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS);

        let item = b"test_item";

        cf.add(item).unwrap();
        assert_eq!(cf.count(item), 1);

        cf.add(item).unwrap();
        assert_eq!(cf.count(item), 2);
    }

    #[test]
    fn test_cuckoo_object_create_copy() {
        let mut co =
            CuckooObject::new_reserved(1000, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS, 0, false)
                .unwrap();

        let item = b"test_item";
        co.add_item(item, false).unwrap();

        let copy = CuckooObject::create_copy_from(&co);

        assert_eq!(copy.num_items(), co.num_items());
        assert_eq!(copy.capacity(), co.capacity());
        assert!(copy.item_exists(item));
    }

    #[test]
    fn test_bad_bucket_size() {
        let result = CuckooObject::new_reserved(1000, 0, DEFAULT_MAX_KICKS, 0, false);
        assert_eq!(result.err(), Some(CuckooError::BadBucketSize));

        let result = CuckooObject::new_reserved(1000, 256, DEFAULT_MAX_KICKS, 0, false);
        assert_eq!(result.err(), Some(CuckooError::BadBucketSize));
    }

    #[test]
    fn test_bad_capacity() {
        let result = CuckooObject::new_reserved(0, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS, 0, false);
        assert_eq!(result.err(), Some(CuckooError::BadCapacity));

        let result =
            CuckooObject::new_reserved(-1, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS, 0, false);
        assert_eq!(result.err(), Some(CuckooError::BadCapacity));
    }

    #[test]
    fn test_encode_decode() {
        let mut co =
            CuckooObject::new_reserved(1000, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS, 2, false)
                .unwrap();

        let item = b"test_item";
        co.add_item(item, false).unwrap();

        let encoded = co.encode_object().unwrap();
        assert!(!encoded.is_empty());

        let decoded = CuckooObject::decode_object(&encoded, false).unwrap();

        assert_eq!(decoded.expansion(), co.expansion());
        assert_eq!(decoded.bucket_size(), co.bucket_size());
        assert_eq!(decoded.max_kicks(), co.max_kicks());
        assert_eq!(decoded.capacity(), co.capacity());
    }
}
