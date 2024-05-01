use bloomfilter;

pub const ERROR: &str = "ERROR";

/// The BloomFilterType structure. 32 bytes.
/// Can contain one or more filters.
pub struct BloomFilterType {
    pub expansion: u32,
    pub fp_rate: f32,
    pub filters: Vec<BloomFilter>,
}

impl BloomFilterType {
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
        let mut mem = std::mem::size_of::<BloomFilterType>();
        for filter in &self.filters {
            mem += std::mem::size_of::<BloomFilter>()
                + (filter.bloom.number_of_bits() / 8u64) as usize;
        }
        mem
    }

    /// Check if item exists already.
    pub fn item_exists(&self, item: &[u8]) -> bool {
        for filter in &self.filters {
            if filter.bloom.check(item) {
                return true;
            }
        }
        false
    }

    /// Return a count of number of items added to all sub filters in the BloomFilterType object.
    pub fn cardinality(&self) -> i64 {
        let mut cardinality = 0;
        for filter in &self.filters {
            cardinality += filter.num_items;
        }
        cardinality as i64
    }

    /// Return a total capacity summed across all sub filters in the BloomFilterType object.
    pub fn capacity(&self) -> i64 {
        let mut capacity = 0;
        // Check if item exists already.
        for filter in &self.filters {
            capacity += filter.capacity;
        }
        capacity as i64
    }

    /// Add an item to the BloomFilterType object.
    /// If scaling is enabled, this can result in a new sub filter creation.
    pub fn add_item(&mut self, item: &[u8]) -> i64 {
        // Check if item exists already.
        if self.item_exists(item) {
            return 0;
        }
        if let Some(filter) = self.filters.last_mut() {
            if self.expansion == 0 || filter.num_items < filter.capacity {
                // Add item.
                filter.bloom.set(item);
                filter.num_items += 1;
                return 1;
            }
            // Scale out by adding a new filter.
            if filter.num_items >= filter.capacity {
                let new_capacity = filter.capacity * self.expansion;
                let mut new_filter = BloomFilter::new(self.fp_rate, new_capacity);
                // Add item.
                new_filter.bloom.set(item);
                new_filter.num_items += 1;
                self.filters.push(new_filter);
                return 1;
            }
        }
        0
    }
}

// Structure representing a single bloom filter. 200 Bytes.
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
