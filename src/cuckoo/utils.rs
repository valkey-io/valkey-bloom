use crate::configs;
use cuckoofilter::CuckooFilter as ExternalCuckooFilter;
use cuckoofilter::ItemHash;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::hash::Hasher;
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
pub const FILTER_FULL: &str = "ERR cuckoo filter is full";
pub const NON_SCALING_FILTER_FULL: &str = "ERR non scaling cuckoo filter is full";
pub const NOT_FOUND: &str = "ERR not found";
pub const ITEM_EXISTS: &str = "ERR item exists";
pub const BAD_EXPANSION: &str = "ERR bad expansion";
pub const BAD_CAPACITY: &str = "ERR bad capacity";
pub const BAD_BUCKET_SIZE: &str = "ERR bad bucket size";
pub const BAD_MAX_KICKS: &str = "ERR bad max kicks";
pub const BAD_MAX_ITERATIONS: &str = "ERR bad max iterations";
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
pub const UNKNOWN_OPTION: &str = "ERR unknown option";
pub const EXCEEDS_MAX_CUCKOO_SIZE: &str = "ERR operation exceeds cuckoo object memory limit";
pub const MAX_SCALING_CAPACITY: &str = "ERR cuckoo object reached max capacity";
pub const MAX_NUM_SCALING_FILTERS: &str = "ERR cuckoo object reached max number of filters";
pub const DECODE_CUCKOO_OBJECT_FAILED: &str = "ERR cuckoo object decoding failed";
pub const DECODE_UNSUPPORTED_VERSION: &str =
    "ERR cuckoo object decoding failed. Unsupported version";
pub const NO_ITEMS_SPECIFIED: &str = "ERR no items specified";
pub const FAILED_TO_SET_FILTER: &str = "ERR failed to set cuckoo filter";

/// Max number of filters allowed within a cuckoo object.
pub const CUCKOO_NUM_FILTERS_PER_OBJECT_LIMIT_MAX: i32 = 1024;

pub const MIN_BUCKET_SIZE: usize = 1;
pub const MAX_BUCKET_SIZE: usize = 255;

#[derive(Debug, PartialEq)]
pub enum CuckooError {
    FilterFull,
    ExceedsMaxSize,
    MaxNumScalingFilters,
    MaxScalingCapacity,
    BadCapacity,
    BadBucketSize,
    BadMaxKicks,
    BadExpansion,
    NonScalingFilterFull,
    DecodeFilterFailed,
    DecodeUnsupportedVersion,
}

impl CuckooError {
    pub fn as_str(&self) -> &'static str {
        match self {
            CuckooError::FilterFull => FILTER_FULL,
            CuckooError::ExceedsMaxSize => EXCEEDS_MAX_CUCKOO_SIZE,
            CuckooError::MaxNumScalingFilters => MAX_NUM_SCALING_FILTERS,
            CuckooError::MaxScalingCapacity => MAX_SCALING_CAPACITY,
            CuckooError::BadCapacity => BAD_CAPACITY,
            CuckooError::BadBucketSize => BAD_BUCKET_SIZE,
            CuckooError::BadMaxKicks => BAD_MAX_KICKS,
            CuckooError::BadExpansion => BAD_EXPANSION,
            CuckooError::NonScalingFilterFull => NON_SCALING_FILTER_FULL,
            CuckooError::DecodeFilterFailed => DECODE_CUCKOO_OBJECT_FAILED,
            CuckooError::DecodeUnsupportedVersion => DECODE_UNSUPPORTED_VERSION,
        }
    }
}

/// Shared range checks; command handlers map these errors to their public
/// argument-specific messages, while constructors preserve their own contract.
pub fn validate_capacity(capacity: i64) -> Result<(), CuckooError> {
    if (configs::CUCKOO_CAPACITY_MIN..=configs::CUCKOO_CAPACITY_MAX).contains(&capacity) {
        Ok(())
    } else {
        Err(CuckooError::BadCapacity)
    }
}

pub fn validate_bucket_size(bucket_size: usize) -> Result<(), CuckooError> {
    if (MIN_BUCKET_SIZE..=MAX_BUCKET_SIZE).contains(&bucket_size) {
        Ok(())
    } else {
        Err(CuckooError::BadBucketSize)
    }
}

pub fn validate_max_kicks(max_kicks: u32) -> Result<(), CuckooError> {
    if (configs::CUCKOO_MAX_KICKS_MIN as u32..=configs::CUCKOO_MAX_KICKS_MAX as u32)
        .contains(&max_kicks)
    {
        Ok(())
    } else {
        Err(CuckooError::BadMaxKicks)
    }
}

/// Top-level CuckooObject structure that can contain multiple filters for scaling
#[allow(clippy::vec_box)]
pub struct CuckooObject {
    expansion: u32,
    bucket_size: usize,
    max_kicks: u32,
    filters: Vec<Box<CuckooFilter>>,
    num_deleted: u64,
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
        validate_capacity(capacity)?;
        validate_bucket_size(bucket_size)?;
        validate_max_kicks(max_kicks)?;
        if expansion > configs::CUCKOO_EXPANSION_MAX {
            return Err(CuckooError::BadExpansion);
        }
        if !CuckooObject::validate_size_before_create(capacity, bucket_size, validate_size_limit) {
            return Err(CuckooError::ExceedsMaxSize);
        }

        let filter = Box::new(CuckooFilter::new(capacity, bucket_size, max_kicks));
        let filters = vec![filter];

        let cuckoo = CuckooObject {
            expansion,
            bucket_size,
            max_kicks,
            filters,
            num_deleted: 0,
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
        num_deleted: u64,
    ) -> CuckooObject {
        let cuckoo = CuckooObject {
            expansion,
            bucket_size,
            max_kicks,
            filters,
            num_deleted,
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
            num_deleted: from.num_deleted,
        };

        new_copy.cuckoo_object_incr_metrics_on_new_create();
        new_copy
    }

    /// Store a fingerprint for every successful add, including duplicate items.
    /// Scale automatically when enabled.
    pub fn add_item(&mut self, item: &[u8], validate_size_limit: bool) -> Result<i64, CuckooError> {
        self.add_hashed(&ExternalFilter::hash_item(item), validate_size_limit)
    }

    pub fn add_item_nx(
        &mut self,
        item: &[u8],
        validate_size_limit: bool,
    ) -> Result<i64, CuckooError> {
        let hash = ExternalFilter::hash_item(item);
        if self.filters.iter().any(|f| f.filter.contains_hashed(&hash)) {
            return Ok(0);
        }
        self.add_hashed(&hash, validate_size_limit)
    }

    fn add_hashed(
        &mut self,
        hash: &ItemHash,
        validate_size_limit: bool,
    ) -> Result<i64, CuckooError> {
        // Reuse any directly available slot before spending RNG and
        // MAXITERATIONS evictions in the newest filter. Keep newest-first order
        // within this direct pass, independently of the local growth limit.
        for filter in self.filters.iter_mut().rev() {
            if filter.add_hashed(hash, false).is_ok() {
                return Ok(1);
            }
        }
        let newest = self.filters.last_mut().expect("at least one filter");
        if newest.add_hashed(hash, true).is_ok() {
            return Ok(1);
        }
        let capacity = self.scaling_capacity(validate_size_limit)?;
        let mut filter = Box::new(CuckooFilter::new(
            capacity,
            self.bucket_size,
            self.max_kicks,
        ));
        filter.add_hashed(hash, true)?;
        let before = self.cuckoo_object_memory_usage();
        self.filters.push(filter);
        crate::metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(
            self.cuckoo_object_memory_usage() - before,
            Ordering::Relaxed,
        );
        Ok(1)
    }

    fn scaling_capacity(&self, validate_size_limit: bool) -> Result<i64, CuckooError> {
        if self.expansion == 0 {
            return Err(CuckooError::NonScalingFilterFull);
        }
        if self.filters.len() >= CUCKOO_NUM_FILTERS_PER_OBJECT_LIMIT_MAX as usize {
            return Err(CuckooError::MaxNumScalingFilters);
        }
        let capacity = self
            .filters
            .last()
            .expect("at least one filter")
            .capacity()
            .checked_mul(self.expansion.into())
            .filter(|n| *n <= configs::CUCKOO_CAPACITY_MAX)
            .ok_or(CuckooError::MaxScalingCapacity)?;
        if !self.validate_size_before_scaling(capacity, self.bucket_size, validate_size_limit) {
            return Err(CuckooError::ExceedsMaxSize);
        }
        Ok(capacity)
    }

    /// Remove the first matching fingerprint, searching subfilters from newest
    /// to oldest.
    pub fn delete_item(&mut self, item: &[u8]) -> Result<i64, CuckooError> {
        let hash = ExternalFilter::hash_item(item);
        for filter in self.filters.iter_mut().rev() {
            if filter.delete_hashed(&hash) {
                // RESP integers are signed. Saturation is deterministic across
                // persistence and replication and never wraps into a negative value.
                self.num_deleted = (self.num_deleted + 1).min(i64::MAX as u64);
                return Ok(1);
            }
        }
        Ok(0)
    }

    /// Check if an item exists in any filter
    pub fn item_exists(&self, item: &[u8]) -> bool {
        let hash = ExternalFilter::hash_item(item);
        self.filters.iter().any(|f| f.filter.contains_hashed(&hash))
    }

    /// Estimate multiplicity by counting matching fingerprints across all filters
    pub fn count_item(&self, item: &[u8]) -> i64 {
        let hash = ExternalFilter::hash_item(item);
        self.filters
            .iter()
            .map(|f| f.filter.count_hashed(&hash) as i64)
            .sum()
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

    pub fn num_deleted(&self) -> i64 {
        self.num_deleted as i64
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

    fn validate_size_before_create(
        capacity: i64,
        bucket_size: usize,
        validate_size_limit: bool,
    ) -> bool {
        CuckooFilter::compute_size(capacity, bucket_size)
            .checked_add(std::mem::size_of::<CuckooObject>())
            .and_then(|n| n.checked_add(std::mem::size_of::<Box<CuckooFilter>>()))
            .is_some_and(|bytes| !validate_size_limit || Self::validate_size(bytes))
    }

    fn validate_size_before_scaling(
        &self,
        new_capacity: i64,
        bucket_size: usize,
        validate_size_limit: bool,
    ) -> bool {
        let vector_growth = if self.filters.len() == self.filters.capacity() {
            self.filters.capacity().max(4) * std::mem::size_of::<Box<CuckooFilter>>()
        } else {
            0
        };
        CuckooFilter::compute_size(new_capacity, bucket_size)
            .checked_add(self.memory_usage())
            .and_then(|n| n.checked_add(vector_growth))
            .is_some_and(|bytes| !validate_size_limit || Self::validate_size(bytes))
    }

    pub fn validate_size(bytes: usize) -> bool {
        bytes <= configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT.load(Ordering::Relaxed) as usize
    }

    pub fn encode_object(&self) -> Vec<u8> {
        // Fixed-width little-endian header, then metadata and raw buckets per filter.
        let size = 1
            + 5 * 8
            + self
                .filters
                .iter()
                .map(|f| 5 * 8 + f.as_bytes().len())
                .sum::<usize>();
        let mut bytes = Vec::with_capacity(size);
        bytes.push(CUCKOO_OBJECT_VERSION);
        for field in self.snapshot_header() {
            bytes.extend_from_slice(&field.to_le_bytes());
        }
        for filter in &self.filters {
            for field in filter.snapshot_header() {
                bytes.extend_from_slice(&field.to_le_bytes());
            }
            bytes.extend_from_slice(filter.as_bytes());
        }
        bytes
    }

    pub fn snapshot_header(&self) -> [u64; 5] {
        [
            self.expansion as u64,
            self.bucket_size as u64,
            self.max_kicks as u64,
            self.filters.len() as u64,
            self.num_deleted,
        ]
    }

    pub fn validate_snapshot_header(header: [u64; 5]) -> Result<(), CuckooError> {
        let [expansion, bucket_size, max_kicks, count, num_deleted] = header;
        if !(MIN_BUCKET_SIZE as u64..=MAX_BUCKET_SIZE as u64).contains(&bucket_size) {
            return Err(CuckooError::BadBucketSize);
        }
        if !(configs::CUCKOO_MAX_KICKS_MIN as u64..=configs::CUCKOO_MAX_KICKS_MAX as u64)
            .contains(&max_kicks)
        {
            return Err(CuckooError::BadMaxKicks);
        }
        if expansion > configs::CUCKOO_EXPANSION_MAX as u64 {
            return Err(CuckooError::BadExpansion);
        }
        if count == 0
            || count > CUCKOO_NUM_FILTERS_PER_OBJECT_LIMIT_MAX as u64
            || num_deleted > i64::MAX as u64
        {
            return Err(CuckooError::DecodeFilterFailed);
        }
        Ok(())
    }

    pub fn decode_object(mut bytes: &[u8], validate_size_limit: bool) -> Result<Self, CuckooError> {
        let (&version, rest) = bytes.split_first().ok_or(CuckooError::DecodeFilterFailed)?;
        if version != CUCKOO_OBJECT_VERSION {
            return Err(CuckooError::DecodeUnsupportedVersion);
        }
        bytes = rest;
        fn header<const N: usize>(bytes: &mut &[u8]) -> Result<[u64; N], CuckooError> {
            let mut fields = [0; N];
            for field in &mut fields {
                let value = bytes.get(..8).ok_or(CuckooError::DecodeFilterFailed)?;
                *field = u64::from_le_bytes(value.try_into().unwrap());
                *bytes = &bytes[8..];
            }
            Ok(fields)
        }
        let fields @ [expansion, bucket_size, max_kicks, count, num_deleted] = header(&mut bytes)?;
        Self::validate_snapshot_header(fields)?;
        // Inspect every subfilter before allocating bucket storage. Reserve the
        // pointer-vector capacity produced by the previous incremental decoder.
        let vector_capacity = if count == 1 {
            1
        } else {
            (count as usize).next_power_of_two().max(4)
        };
        let mut remaining = bytes;
        let mut size = Self::compute_size(vector_capacity);
        for _ in 0..count {
            let fields = header(&mut remaining)?;
            let checked = CuckooFilter::validate_snapshot_header(fields, bucket_size as usize)?;
            let buckets = checked.bucket_bytes();
            remaining = remaining
                .get(buckets..)
                .ok_or(CuckooError::DecodeFilterFailed)?;
            size = size
                .checked_add(std::mem::size_of::<CuckooFilter>())
                .and_then(|n| n.checked_add(buckets))
                .ok_or(CuckooError::DecodeFilterFailed)?;
        }
        if !remaining.is_empty() {
            return Err(CuckooError::DecodeFilterFailed);
        }
        if validate_size_limit && !Self::validate_size(size) {
            return Err(CuckooError::ExceedsMaxSize);
        }
        let mut filters = Vec::with_capacity(vector_capacity);
        for _ in 0..count {
            let fields = header(&mut bytes)?;
            let checked = CuckooFilter::validate_snapshot_header(fields, bucket_size as usize)?;
            let size = checked.bucket_bytes();
            let values = bytes.get(..size).ok_or(CuckooError::DecodeFilterFailed)?;
            let filter = CuckooFilter::from_snapshot(checked, values.into(), max_kicks as u32)?;
            bytes = &bytes[size..];
            filters.push(Box::new(filter));
        }
        if !bytes.is_empty() {
            return Err(CuckooError::DecodeFilterFailed);
        }
        let object = Self::from_existing(
            expansion as u32,
            bucket_size as usize,
            max_kicks as u32,
            filters,
            num_deleted,
        );
        debug_assert_eq!(size, object.memory_usage());
        Ok(object)
    }

    fn cuckoo_object_incr_metrics_on_new_create(&self) {
        use crate::metrics;
        metrics::CUCKOO_NUM_OBJECTS.fetch_add(1, Ordering::Relaxed);
        metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_add(self.cuckoo_object_memory_usage(), Ordering::Relaxed);
    }

    fn cuckoo_object_decr_metrics_on_drop(&self) {
        use crate::metrics;
        metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_sub(self.cuckoo_object_memory_usage(), Ordering::Relaxed);
        metrics::CUCKOO_NUM_OBJECTS.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for CuckooObject {
    fn drop(&mut self) {
        self.cuckoo_object_decr_metrics_on_drop();
    }
}

// SipHash-1-3 with fixed keys and canonical length encoding. Keep hashing
// unchanged for the lifetime of the snapshot format.
#[derive(Clone, Default)]
pub struct FixedHasher(siphasher::sip::SipHasher13);
impl Hasher for FixedHasher {
    fn finish(&self) -> u64 {
        self.0.finish()
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
    fn write_usize(&mut self, value: usize) {
        self.write(&(value as u64).to_le_bytes());
    }
}
type ExternalFilter = ExternalCuckooFilter<FixedHasher, ChaCha8Rng>;
const RNG_SEED: u64 = 42;

/// A filter stores fingerprints and RNG state, never the original item bytes.
pub struct CuckooFilter {
    filter: ExternalFilter,
    capacity: i64,
    bucket_size: usize,
}

/// Validated metadata; only the header validator can construct this value.
pub struct ValidatedCuckooFilterHeader {
    fields: [u64; 5],
    bucket_size: usize,
    bucket_bytes: usize,
}

impl ValidatedCuckooFilterHeader {
    pub fn bucket_bytes(&self) -> usize {
        self.bucket_bytes
    }
}

impl CuckooFilter {
    pub fn new(capacity: i64, bucket_size: usize, max_kicks: u32) -> Self {
        let filter = ExternalFilter::with_config_and_rng(
            capacity as usize,
            bucket_size,
            max_kicks,
            ChaCha8Rng::seed_from_u64(RNG_SEED),
        )
        .expect("validated filter configuration");
        let result = Self {
            filter,
            capacity,
            bucket_size,
        };
        result.cuckoo_filter_incr_metrics_on_new_create();
        result
    }

    pub fn snapshot_header(&self) -> [u64; 5] {
        let position = self.filter.rng().get_word_pos();
        [
            self.capacity as u64,
            self.filter.len() as u64,
            position as u64,
            (position >> 64) as u64,
            self.as_bytes().len() as u64,
        ]
    }

    pub fn validate_snapshot_header(
        fields: [u64; 5],
        bucket_size: usize,
    ) -> Result<ValidatedCuckooFilterHeader, CuckooError> {
        let [capacity, length, _, high, size] = fields;
        if !(configs::CUCKOO_CAPACITY_MIN as u64..=configs::CUCKOO_CAPACITY_MAX as u64)
            .contains(&capacity)
            || high >= 16
            || length > size
        {
            return Err(CuckooError::DecodeFilterFailed);
        }
        let size = usize::try_from(size).map_err(|_| CuckooError::DecodeFilterFailed)?;
        let native_capacity =
            usize::try_from(capacity).map_err(|_| CuckooError::DecodeFilterFailed)?;
        if size
            != ExternalFilter::allocation_size(native_capacity, bucket_size)
                .map_err(|_| CuckooError::DecodeFilterFailed)?
        {
            return Err(CuckooError::DecodeFilterFailed);
        }
        Ok(ValidatedCuckooFilterHeader {
            fields,
            bucket_size,
            bucket_bytes: size,
        })
    }

    pub fn from_snapshot(
        header: ValidatedCuckooFilterHeader,
        values: Box<[u8]>,
        max_kicks: u32,
    ) -> Result<Self, CuckooError> {
        if header.bucket_bytes != values.len() {
            return Err(CuckooError::DecodeFilterFailed);
        }
        let [capacity, length, low, high, _] = header.fields;
        let bucket_size = header.bucket_size;
        let mut rng = ChaCha8Rng::seed_from_u64(RNG_SEED);
        rng.set_word_pos(u128::from(low) | (u128::from(high) << 64));
        let filter = ExternalFilter::from_bytes_with_rng(
            values,
            length as usize,
            bucket_size,
            max_kicks,
            rng,
        )
        .map_err(|_| CuckooError::DecodeFilterFailed)?;
        let result = Self {
            filter,
            capacity: capacity as i64,
            bucket_size,
        };
        result.cuckoo_filter_incr_metrics_on_new_create();
        Ok(result)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.filter.as_bytes()
    }

    pub fn realloc_buckets(&mut self, f: impl FnOnce(Box<[u8]>) -> Box<[u8]>) {
        self.filter.realloc_buckets(f);
    }

    fn add_hashed(&mut self, hash: &ItemHash, allow_eviction: bool) -> Result<(), CuckooError> {
        // Occupancy is proof of fullness for every hash. A failed eviction is
        // only evidence about that attempt, so it must not suppress later ones.
        let inserted = if self.filter.len() == self.as_bytes().len() {
            false
        } else if !allow_eviction {
            self.filter.try_add_no_evict_hashed(hash)
        } else {
            self.filter.try_add_hashed(hash).is_ok()
        };
        if !inserted {
            return Err(CuckooError::FilterFull);
        }
        crate::metrics::CUCKOO_NUM_ITEMS_ACROSS_OBJECTS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn delete_hashed(&mut self, hash: &ItemHash) -> bool {
        let deleted = self.filter.delete_hashed(hash);
        if deleted {
            crate::metrics::CUCKOO_NUM_ITEMS_ACROSS_OBJECTS.fetch_sub(1, Ordering::Relaxed);
        }
        deleted
    }
    pub fn number_of_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.filter.memory_usage()
            - std::mem::size_of::<ExternalFilter>()
    }
    pub fn compute_size(capacity: i64, bucket_size: usize) -> usize {
        usize::try_from(capacity)
            .ok()
            .and_then(|n| ExternalFilter::allocation_size(n, bucket_size).ok())
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>()))
            .unwrap_or(usize::MAX)
    }
    pub fn create_copy_from(from: &Self) -> Self {
        let result = Self {
            filter: from.filter.clone(),
            capacity: from.capacity,
            bucket_size: from.bucket_size,
        };
        result.cuckoo_filter_incr_metrics_on_new_create();
        result
    }
    pub fn capacity(&self) -> i64 {
        self.capacity
    }
    pub fn num_items(&self) -> i64 {
        self.filter.len() as i64
    }
    pub fn bucket_size(&self) -> usize {
        self.bucket_size
    }
    pub fn bucket_count(&self) -> usize {
        self.filter.bucket_count()
    }
    fn cuckoo_filter_incr_metrics_on_new_create(&self) {
        use crate::metrics;
        metrics::CUCKOO_NUM_FILTERS_ACROSS_OBJECTS.fetch_add(1, Ordering::Relaxed);
        metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_add(self.number_of_bytes(), Ordering::Relaxed);
        metrics::CUCKOO_CAPACITY_ACROSS_OBJECTS.fetch_add(self.capacity as u64, Ordering::Relaxed);
        metrics::CUCKOO_NUM_ITEMS_ACROSS_OBJECTS
            .fetch_add(self.num_items() as u64, Ordering::Relaxed);
    }
}

impl Drop for CuckooFilter {
    fn drop(&mut self) {
        use crate::metrics;
        metrics::CUCKOO_NUM_FILTERS_ACROSS_OBJECTS.fetch_sub(1, Ordering::Relaxed);
        metrics::CUCKOO_OBJECT_TOTAL_MEMORY_BYTES
            .fetch_sub(self.number_of_bytes(), Ordering::Relaxed);
        metrics::CUCKOO_NUM_ITEMS_ACROSS_OBJECTS
            .fetch_sub(self.num_items() as u64, Ordering::Relaxed);
        metrics::CUCKOO_CAPACITY_ACROSS_OBJECTS.fetch_sub(self.capacity as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_BUCKET_SIZE: usize = crate::configs::CUCKOO_BUCKET_SIZE_DEFAULT as usize;
    const DEFAULT_MAX_KICKS: u32 = crate::configs::CUCKOO_MAX_KICKS_DEFAULT as u32;

    #[test]
    fn duplicates_and_nx_have_distinct_semantics() {
        let mut object = CuckooObject::new_reserved(1000, 4, 20, 2, false).unwrap();
        let item = b"test_item";
        for count in 1..=10 {
            assert_eq!(object.add_item(item, false), Ok(1));
            assert_eq!(object.count_item(item), count);
            let before = object.encode_object();
            assert_eq!(object.add_item_nx(item, false), Ok(0));
            assert_eq!(before, object.encode_object());
        }
        for count in (0..10).rev() {
            assert_eq!(object.delete_item(item), Ok(1));
            assert_eq!(object.count_item(item), count);
        }
        assert!(!object.item_exists(item));
        assert_eq!(object.delete_item(item), Ok(0));
    }

    #[test]
    fn test_cuckoo_object_capacity_and_memory() {
        let co = CuckooObject::new_reserved(1000, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS, 2, false)
            .unwrap();

        assert_eq!(co.capacity(), 1000);
        assert_eq!(co.num_filters(), 1);
        assert!(co.memory_usage() > 0);
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
        let result =
            CuckooObject::new_reserved(0, DEFAULT_BUCKET_SIZE, DEFAULT_MAX_KICKS, 0, false);
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

        let encoded = co.encode_object();
        assert!(!encoded.is_empty());

        let decoded = CuckooObject::decode_object(&encoded, false).unwrap();

        assert_eq!(decoded.expansion(), co.expansion());
        assert_eq!(decoded.bucket_size(), co.bucket_size());
        assert_eq!(decoded.max_kicks(), co.max_kicks());
        assert_eq!(decoded.capacity(), co.capacity());
    }
    #[test]
    fn snapshot_and_copy_resume_identical_evictions() {
        let mut original = CuckooObject::new_reserved(32, 2, 20, 2, false).unwrap();
        for item in 0..70_u64 {
            original.add_item(&item.to_le_bytes(), false).unwrap();
        }
        let bytes = original.encode_object();
        let mut restored = CuckooObject::decode_object(&bytes, false).unwrap();
        let mut copied = CuckooObject::create_copy_from(&original);
        assert_eq!(bytes, restored.encode_object());
        assert_eq!(bytes, copied.encode_object());
        for item in 70..300_u64 {
            let key = item.to_le_bytes();
            original.add_item(&key, false).unwrap();
            restored.add_item(&key, false).unwrap();
            copied.add_item(&key, false).unwrap();
            assert_eq!(original.encode_object(), restored.encode_object());
            assert_eq!(original.encode_object(), copied.encode_object());
        }
        assert!(original
            .filters
            .iter()
            .any(|f| f.filter.rng().get_word_pos() > 0));
    }

    #[test]
    fn duplicate_in_old_filter_survives_one_delete() {
        let mut object = CuckooObject::new_reserved(8, 2, 20, 2, false).unwrap();
        let key = b"original";
        object.add_item(key, false).unwrap();
        for item in 0..30_u64 {
            object.add_item(&item.to_le_bytes(), false).unwrap();
        }
        assert!(object.num_filters() > 1);
        let before = object.encode_object();
        object.add_item(key, false).unwrap();
        assert_ne!(before, object.encode_object());
        assert_eq!(object.count_item(key), 2);
        assert_eq!(object.delete_item(key).unwrap(), 1);
        assert!(object.item_exists(key));
        assert_eq!(object.delete_item(key).unwrap(), 1);
        assert!(!object.item_exists(key));
    }

    #[test]
    fn failed_insert_preserves_snapshot_and_existing_items() {
        let mut object = CuckooObject::new_reserved(16, 1, 1, 0, false).unwrap();
        let mut inserted = Vec::new();
        for item in 0..100_u64 {
            let key = item.to_le_bytes();
            let before = object.encode_object();
            if object.add_item(&key, false).is_ok() {
                inserted.push(key);
            } else {
                assert_eq!(before, object.encode_object());
            }
            for key in &inserted {
                assert!(object.item_exists(key));
            }
        }
    }

    #[test]
    fn reject_corrupt_snapshots() {
        let object = CuckooObject::new_reserved(32, 4, 20, 2, false).unwrap();
        let bytes = object.encode_object();
        for end in 0..bytes.len() {
            assert!(CuckooObject::decode_object(&bytes[..end], false).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(CuckooObject::decode_object(&trailing, false).is_err());
        // Corrupt each metadata field independently, including length/occupied-slot mismatch.
        for (offset, value) in [
            (1, u64::MAX),
            (9, 0),
            (17, 0),
            (25, 0),
            (25, 1025),
            (33, u64::MAX),
            (41, 0),
            (49, 1),
            (65, 16),
            (73, 1),
            (73, 31),
            (73, u64::MAX),
        ] {
            let mut invalid = bytes.clone();
            invalid[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            assert!(
                CuckooObject::decode_object(&invalid, false).is_err(),
                "offset {offset}"
            );
        }
        for version in [0, 2, 3, 4, 5, 255] {
            let mut invalid = bytes.clone();
            invalid[0] = version;
            assert!(matches!(
                CuckooObject::decode_object(&invalid, false),
                Err(CuckooError::DecodeUnsupportedVersion)
            ));
        }
    }

    #[test]
    fn direct_insertion_prefers_newest_when_both_filters_have_space() {
        let mut object = CuckooObject::from_existing(
            2,
            4,
            20,
            vec![
                Box::new(CuckooFilter::new(64, 4, 20)),
                Box::new(CuckooFilter::new(128, 4, 20)),
            ],
            0,
        );
        let old_header = object.filters[0].snapshot_header();
        let old_buckets = object.filters[0].as_bytes().to_vec();
        let rng = object.filters[1].snapshot_header()[2..4].to_vec();
        assert_eq!(object.add_item(b"available-in-both", false), Ok(1));
        assert_eq!(object.filters[0].snapshot_header(), old_header);
        assert_eq!(object.filters[0].as_bytes(), old_buckets);
        assert_eq!(object.filters[1].num_items(), 1);
        assert_eq!(object.filters[1].snapshot_header()[2..4], rng);
    }

    #[test]
    fn direct_old_slot_precedes_useful_newest_eviction() {
        let mut newest = CuckooFilter::new(64, 4, 20);
        for item in 0..1000 {
            let hash = ExternalFilter::hash_item(item.to_string().as_bytes());
            if newest.add_hashed(&hash, true).is_err() {
                break;
            }
        }
        let candidate = (1000..10000)
            .map(|i| i.to_string())
            .find(|key| {
                let hash = ExternalFilter::hash_item(key.as_bytes());
                let mut probe = CuckooFilter::create_copy_from(&newest);
                probe.add_hashed(&hash, false).is_err() && probe.add_hashed(&hash, true).is_ok()
            })
            .expect("a deterministic useful eviction");
        let mut old = CuckooFilter::new(64, 4, 20);
        let deleted = ExternalFilter::hash_item(b"deleted".as_slice());
        old.add_hashed(&deleted, false).unwrap();
        assert!(old.delete_hashed(&deleted));
        let mut object =
            CuckooObject::from_existing(2, 4, 20, vec![Box::new(old), Box::new(newest)], 0);
        let mut restored = CuckooObject::decode_object(&object.encode_object(), false).unwrap();
        let before_header = object.filters[1].snapshot_header();
        let before_buckets = object.filters[1].as_bytes().to_vec();
        assert_eq!(object.add_item(candidate.as_bytes(), false), Ok(1));
        assert_eq!(restored.add_item(candidate.as_bytes(), false), Ok(1));
        assert_eq!(object.encode_object(), restored.encode_object());
        assert_eq!(object.filters[0].num_items(), 1);
        assert_eq!(object.filters[1].snapshot_header(), before_header);
        assert_eq!(object.filters[1].as_bytes(), before_buckets);
    }

    #[test]
    fn scaling_keeps_old_direct_slots_available() {
        let mut object = CuckooObject::new_reserved(64, 2, 20, 1, false).unwrap();
        let mut at_first_scale = None;
        for item in 0..200 {
            object
                .add_item(format!("item:{item}").as_bytes(), false)
                .unwrap();
            if object.num_filters() == 2 && at_first_scale.is_none() {
                at_first_scale = Some(object.filters[0].num_items());
            }
        }
        assert!(object.filters[0].num_items() > at_first_scale.unwrap());
        assert_eq!(object.num_deleted(), 0);
        // Pin placement across scaling, including direct reuse of older filters.
        let mut digest = FixedHasher::default();
        digest.write(&object.encode_object());
        assert_eq!(
            digest.finish(),
            12_499_279_960_782_828_056,
            "scaled placement fixture"
        );
    }

    #[test]
    fn capacity_ceiling_preserves_failed_insertion_state() {
        // Model the requested capacity independently of backing storage so the
        // ceiling check does not require a multi-GiB allocation in unit tests.
        let backing = ExternalFilter::from_bytes_with_rng(
            vec![7; 4].into_boxed_slice(),
            4,
            4,
            20,
            ChaCha8Rng::seed_from_u64(RNG_SEED),
        )
        .unwrap();
        let filter = CuckooFilter {
            filter: backing,
            capacity: (1_i64 << 31) + 1,
            bucket_size: 4,
        };
        filter.cuckoo_filter_incr_metrics_on_new_create();
        let mut object = CuckooObject::from_existing(2, 4, 20, vec![Box::new(filter)], 0);
        let before = object.encode_object();
        // Bypass the local memory limit; the arithmetic ceiling is unconditional.
        assert_eq!(
            object.add_item(b"overflow", false),
            Err(CuckooError::MaxScalingCapacity)
        );
        assert_eq!(object.encode_object(), before);
        assert_eq!(
            CuckooError::MaxScalingCapacity.as_str(),
            "ERR cuckoo object reached max capacity"
        );
    }

    #[test]
    fn copy_and_snapshot_match_probes_after_first_failure() {
        for kicks in [1, 20, 512] {
            let mut original = CuckooObject::new_reserved(1024, 4, kicks, 0, false).unwrap();
            let first_failure = (0..2000)
                .find(|i| {
                    original
                        .add_item(format!("item:{i}").as_bytes(), false)
                        .is_err()
                })
                .expect("the non-scaling filter must reject an insertion");
            let mut copy = CuckooObject::create_copy_from(&original);
            let mut restored =
                CuckooObject::decode_object(&original.encode_object(), false).unwrap();
            for i in first_failure + 1..first_failure + 3001 {
                let key = format!("item:{i}");
                let before = original.encode_object();
                let result = original.add_item(key.as_bytes(), false);
                assert_eq!(result, copy.add_item(key.as_bytes(), false));
                assert_eq!(result, restored.add_item(key.as_bytes(), false));
                assert_eq!(original.encode_object(), copy.encode_object());
                assert_eq!(original.encode_object(), restored.encode_object());
                if result.is_err() {
                    assert_eq!(original.encode_object(), before);
                }
            }
        }
    }

    #[test]
    fn freed_capacity_is_reused_before_scaling() {
        let mut object = CuckooObject::new_reserved(16, 4, 20, 1, false).unwrap();
        for item in 0..32_u64 {
            object.add_item(&item.to_le_bytes(), false).unwrap();
        }
        let count = object.num_filters();
        let capacity = object.capacity();
        for item in 0..16_u64 {
            object.delete_item(&item.to_le_bytes()).unwrap();
        }
        for item in 100..108_u64 {
            object.add_item(&item.to_le_bytes(), false).unwrap();
        }
        assert_eq!(object.num_filters(), count);
        assert_eq!(object.capacity(), capacity);
    }
    #[test]
    fn use_rounded_bucket_capacity_before_scaling() {
        let mut object = CuckooObject::new_reserved(100, 4, 500, 0, false).unwrap();
        for item in 0..110_u64 {
            object.add_item(&item.to_le_bytes(), false).unwrap();
        }
        assert!(object.num_items() > object.capacity());
        assert_eq!(object.num_filters(), 1);
        let snapshot = object.encode_object();
        let restored = CuckooObject::decode_object(&snapshot, false).unwrap();
        assert_eq!(snapshot, restored.encode_object());
    }
    #[test]
    fn fingerprint_collision_survives_deleting_other_item() {
        let mut object = CuckooObject::new_reserved(32, 4, 20, 2, false).unwrap();
        let first = 0_u64.to_le_bytes();
        object.add_item(&first, false).unwrap();
        let collision = (1..100_000_u64)
            .map(u64::to_le_bytes)
            .find(|key| object.item_exists(key))
            .expect("deterministic collision");
        object.add_item(&collision, false).unwrap();
        assert_eq!(object.num_items(), 2);
        object.delete_item(&first).unwrap();
        assert!(object.item_exists(&collision));
    }

    #[test]
    fn million_successful_adds_store_million_fingerprints() {
        let mut object = CuckooObject::new_reserved(250_000, 4, 500, 2, false).unwrap();
        for item in 0..1_000_000_u64 {
            object.add_item(&item.to_le_bytes(), false).unwrap();
        }
        assert_eq!(object.num_items(), 1_000_000);
        for item in 0..1_000_000_u64 {
            assert!(object.item_exists(&item.to_le_bytes()));
        }
    }

    #[test]
    fn deletion_count_survives_copy_snapshot_and_saturates() {
        let mut object = CuckooObject::new_reserved(32, 4, 20, 2, false).unwrap();
        assert_eq!(object.delete_item(b"missing"), Ok(0));
        assert_eq!(object.num_deleted(), 0);
        object.add_item(b"item", false).unwrap();
        assert_eq!(object.delete_item(b"item"), Ok(1));
        assert_eq!(object.num_deleted(), 1);
        assert_eq!(CuckooObject::create_copy_from(&object).num_deleted(), 1);
        let restored = CuckooObject::decode_object(&object.encode_object(), false).unwrap();
        assert_eq!(restored.num_deleted(), 1);
        object.num_deleted = i64::MAX as u64;
        object.add_item(b"item", false).unwrap();
        assert_eq!(object.delete_item(b"item"), Ok(1));
        assert_eq!(object.num_deleted(), i64::MAX);
        let restored = CuckooObject::decode_object(&object.encode_object(), false).unwrap();
        assert_eq!(restored.num_deleted(), i64::MAX);
    }

    #[test]
    fn size_overflow_is_rejected_even_when_local_limits_are_bypassed() {
        let object = CuckooObject::new_reserved(32, 4, 20, 2, false).unwrap();
        // Force the allocation-size sentinel without attempting a large allocation.
        for (capacity, bucket_size) in [(32, 0), (i64::MAX, 1), (-1, 4)] {
            assert_eq!(
                CuckooFilter::compute_size(capacity, bucket_size),
                usize::MAX
            );
            for validate_limit in [false, true] {
                assert!(!CuckooObject::validate_size_before_create(
                    capacity,
                    bucket_size,
                    validate_limit
                ));
                assert!(!object.validate_size_before_scaling(
                    capacity,
                    bucket_size,
                    validate_limit
                ));
            }
        }
    }

    #[test]
    fn validated_header_still_rejects_mismatched_bucket_storage() {
        for values in [vec![100; 31], vec![100; 33], vec![7; 32]] {
            let header = CuckooFilter::validate_snapshot_header([32, 0, 0, 0, 32], 4).unwrap();
            assert!(matches!(
                CuckooFilter::from_snapshot(header, values.into_boxed_slice(), 20),
                Err(CuckooError::DecodeFilterFailed)
            ));
        }
    }

    // All tests that change the global Cuckoo memory limit belong here. Other
    // unit tests bypass this limit; the guard restores it even on assertion failure.
    #[test]
    fn memory_limit_preflights_allocations_and_can_be_raised_after_failure() {
        use crate::test_allocator::largest_allocation;
        struct ResetLimit(i64);
        impl Drop for ResetLimit {
            fn drop(&mut self) {
                configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT.store(self.0, Ordering::Relaxed);
            }
        }
        let _reset = ResetLimit(configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT.load(Ordering::Relaxed));
        for capacities in [vec![16 * 1024 * 1024], vec![1024 * 1024; 3]] {
            let mut filters = Vec::with_capacity(1);
            for &capacity in &capacities {
                filters.push(Box::new(CuckooFilter::new(capacity, 4, 20)));
            }
            let object = CuckooObject::from_existing(2, 4, 20, filters, 0);
            let snapshot = object.encode_object();
            let exact_size = object.memory_usage();
            for limit in [1024, exact_size - 1, exact_size] {
                configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT.store(limit as i64, Ordering::Relaxed);
                let (result, largest) =
                    largest_allocation(|| CuckooObject::decode_object(&snapshot, true));
                if limit < exact_size {
                    assert!(matches!(result, Err(CuckooError::ExceedsMaxSize)));
                    assert_eq!(largest, 0, "oversized snapshot allocated before rejection");
                } else {
                    let restored = result.unwrap();
                    assert_eq!(restored.memory_usage(), exact_size);
                    assert_eq!(restored.encode_object(), snapshot);
                    assert_eq!(largest, capacities[0] as usize);
                }
            }
            configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT.store(0, Ordering::Relaxed);
            assert!(CuckooObject::decode_object(&snapshot, false).is_ok());
        }
        let mut original = CuckooObject::new_reserved(64, 4, 1, 2, false).unwrap();
        configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT
            .store(original.memory_usage() as i64, Ordering::Relaxed);
        let mut failures = 0;
        for item in 0..300 {
            let key = item.to_string();
            let before = original.encode_object();
            let mut copy = CuckooObject::create_copy_from(&original);
            let result = original.add_item(key.as_bytes(), true);
            assert_eq!(result, copy.add_item(key.as_bytes(), true));
            assert_eq!(original.encode_object(), copy.encode_object());
            if result.is_err() {
                failures += 1;
                assert_eq!(before, original.encode_object());
            }
        }
        assert!(failures > 0);
        configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT.store(1_000_000, Ordering::Relaxed);
        original.add_item(b"growth is allowed again", true).unwrap();
        assert_eq!(original.num_filters(), 2);
    }

    #[test]
    fn failed_insert_does_not_suppress_other_hashes() {
        let mut original = CuckooObject::new_reserved(64, 4, 1, 0, false).unwrap();
        let mut failures = 0;
        let mut successes_after_failure = 0;
        for item in 0..300 {
            let key = item.to_string();
            let before = original.encode_object();
            let mut copied = CuckooObject::create_copy_from(&original);
            let mut restored = CuckooObject::decode_object(&before, false).unwrap();
            let result = original.add_item(key.as_bytes(), false);
            assert_eq!(
                result,
                copied.add_item(key.as_bytes(), false),
                "item {item}"
            );
            assert_eq!(
                result,
                restored.add_item(key.as_bytes(), false),
                "item {item}"
            );
            let after = original.encode_object();
            assert_eq!(after, copied.encode_object());
            assert_eq!(after, restored.encode_object());
            if result.is_err() {
                failures += 1;
                assert_eq!(before, after);
            } else if failures > 0 {
                successes_after_failure += 1;
            }
        }
        assert!(failures > 1 && successes_after_failure > 1);
    }

    #[test]
    fn filter_count_limit_keeps_evictions_and_old_slots_available() {
        let mut filters = Vec::new();
        // All older filters are actually full; only the newest has spare slots.
        for _ in 0..CUCKOO_NUM_FILTERS_PER_OBJECT_LIMIT_MAX - 1 {
            filters.push(Box::new(
                CuckooFilter::from_snapshot(
                    CuckooFilter::validate_snapshot_header([64, 64, 0, 0, 64], 4).unwrap(),
                    vec![7; 64].into_boxed_slice(),
                    1,
                )
                .unwrap(),
            ));
        }
        filters.push(Box::new(CuckooFilter::new(64, 4, 1)));
        let old_hash = (0..10000)
            .map(|i| ExternalFilter::hash_item(i.to_string().as_bytes()))
            .find(|hash| filters[0].filter.contains_hashed(hash))
            .unwrap();
        let mut original = CuckooObject::from_existing(2, 4, 1, filters, 0);
        let mut failures = 0;
        let mut accepted_after_failure = 0;
        for item in 0..300 {
            // Simulate a deletion from an older subfilter once growth is blocked.
            if item == 50 {
                assert!(original.filters[0].delete_hashed(&old_hash));
            }
            let key = item.to_string();
            let before = original.encode_object();
            let mut copy = CuckooObject::create_copy_from(&original);
            let result = original.add_item(key.as_bytes(), false);
            assert_eq!(result, copy.add_item(key.as_bytes(), false));
            assert_eq!(original.encode_object(), copy.encode_object());
            if result.is_err() {
                assert_eq!(result, Err(CuckooError::MaxNumScalingFilters));
                assert_eq!(before, original.encode_object());
                failures += 1;
            } else if failures > 0 {
                accepted_after_failure += 1;
            }
        }
        assert!(failures > 0 && accepted_after_failure > 0);
        assert!(original.filters[0].num_items() > 0);
    }

    #[test]
    fn full_filter_preserves_successful_replication_and_reuses_deletion() {
        let mut primary = CuckooObject::new_reserved(64, 4, 32, 0, false).unwrap();
        let mut replica = CuckooObject::create_copy_from(&primary);
        let mut accepted = Vec::new();
        for item in 0..1000_u64 {
            let key = item.to_le_bytes();
            if primary.add_item(&key, false).is_ok() {
                accepted.push(key);
                replica.add_item(&key, false).unwrap();
            }
            assert_eq!(primary.encode_object(), replica.encode_object());
        }
        assert_eq!(
            primary.num_items() as usize,
            primary.filters[0].as_bytes().len()
        );
        primary.delete_item(&accepted[0]).unwrap();
        primary.add_item(&accepted[0], false).unwrap();
    }

    #[test]
    fn bucket_four_false_positive_rate() {
        let mut object = CuckooObject::new_reserved(16384, 4, 500, 0, false).unwrap();
        for item in 0..14000_u64 {
            object.add_item(&item.to_le_bytes(), false).unwrap();
        }
        let positives = (100_000..200_000_u64)
            .filter(|item| object.item_exists(&item.to_le_bytes()))
            .count();
        let rate = positives as f64 / 100_000.0;
        assert!((0.018..0.04).contains(&rate), "FPR: {rate}");
    }
    // Fixed single-filter bucket and RNG fixture for the published dependency.
    // Do not regenerate these values merely to accommodate a failing test.
    const EXPECTED_BUCKETS: [u8; 64] = [
        142, 84, 122, 190, 253, 101, 202, 223, 168, 103, 141, 47, 221, 105, 221, 15, 50, 229, 147,
        164, 244, 125, 133, 31, 188, 194, 107, 200, 149, 248, 254, 170, 122, 182, 136, 202, 204,
        190, 140, 108, 32, 180, 182, 57, 134, 24, 35, 136, 66, 207, 82, 161, 242, 82, 254, 62, 58,
        232, 213, 48, 214, 96, 91, 8,
    ];
    const EXPECTED_RNG_WORD_POS: u128 = 291;

    #[test]
    fn slice_hash_representation_stays_compatible() {
        use std::hash::Hash;
        for length in 0..=1024 {
            let data: Vec<u8> = (0..length).map(|i| (i * 37) as u8).collect();
            let mut through_std = FixedHasher::default();
            data.as_slice().hash(&mut through_std);
            let mut canonical = FixedHasher::default();
            canonical.write(&(length as u64).to_le_bytes());
            canonical.write(&data);
            assert_eq!(through_std.finish(), canonical.finish(), "length {length}");
        }
    }

    #[test]
    fn deterministic_snapshot_fixture() {
        let mut filter =
            ExternalFilter::with_config_and_rng(64, 4, 32, ChaCha8Rng::seed_from_u64(42)).unwrap();
        let mut accepted = Vec::new();
        for item in 0..100_u64 {
            if filter.try_add(item.to_le_bytes().as_slice()).is_ok() {
                accepted.push(item);
            }
        }
        assert_eq!(accepted, (0..63).chain([84]).collect::<Vec<_>>());
        assert_eq!(filter.len(), EXPECTED_BUCKETS.len());
        assert_eq!(filter.as_bytes(), EXPECTED_BUCKETS);
        assert_eq!(filter.rng().get_word_pos(), EXPECTED_RNG_WORD_POS);
        for item in &accepted {
            assert!(filter.contains(item.to_le_bytes().as_slice()));
        }

        let mut rng = ChaCha8Rng::seed_from_u64(42);
        rng.set_word_pos(EXPECTED_RNG_WORD_POS);
        let mut restored = ExternalFilter::from_bytes_with_rng(
            Box::new(EXPECTED_BUCKETS),
            EXPECTED_BUCKETS.len(),
            4,
            32,
            rng,
        )
        .unwrap();
        for item in accepted.iter().step_by(3) {
            assert!(filter.delete(item.to_le_bytes().as_slice()));
            assert!(restored.delete(item.to_le_bytes().as_slice()));
        }
        let position = filter.rng().get_word_pos();
        for item in 100..150_u64 {
            let h = ExternalFilter::hash_item(item.to_le_bytes().as_slice());
            assert_eq!(
                filter.try_add_hashed(&h).is_ok(),
                restored.try_add_hashed(&h).is_ok()
            );
            assert_eq!(filter.as_bytes(), restored.as_bytes());
            assert_eq!(filter.len(), restored.len());
            assert_eq!(filter.rng().get_word_pos(), restored.rng().get_word_pos());
        }
        assert!(filter.rng().get_word_pos() > position);
    }
    #[test]
    fn failed_insertion_preserves_replication_after_reusing_old_filter() {
        let mut primary = CuckooObject::new_reserved(64, 4, 1, 2, false).unwrap();
        for item in 0..80_u64 {
            primary.add_item(&item.to_le_bytes(), false).unwrap();
        }
        assert!(primary.num_filters() > 1);
        // Model growth becoming unavailable after earlier scaling (e.g. a memory limit).
        primary.expansion = 0;
        for item in 80..1000_u64 {
            if primary.add_item(&item.to_le_bytes(), false).is_err() {
                break;
            }
        }
        let mut replica = CuckooObject::create_copy_from(&primary);
        // Deletion makes an older filter reusable after a failure in the newest.
        for item in 0..32_u64 {
            let hash = ExternalFilter::hash_item(&item.to_le_bytes());
            assert_eq!(
                primary.filters[0].delete_hashed(&hash),
                replica.filters[0].delete_hashed(&hash)
            );
        }
        for item in 1000..2000_u64 {
            let key = item.to_le_bytes();
            if primary.add_item(&key, false).is_ok() {
                replica.add_item(&key, false).unwrap();
            }
            assert_eq!(
                primary.encode_object(),
                replica.encode_object(),
                "item {item}"
            );
        }
    }
}
