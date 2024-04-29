use bloomfilter::Bloom;

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
        let bloom = BloomFilter::new(
            fp_rate,
            capacity,
        );
        let mut filters = Vec::new();
        filters.push(bloom);
        BloomFilterType {
            expansion,
            fp_rate,
            filters,
        }
    }

    pub fn get_memory_usage(&self) -> usize {
        let mut mem = std::mem::size_of::<BloomFilterType>();
        for filter in &self.filters {
            mem += std::mem::size_of::<BloomFilter>() + (filter.bloom.number_of_bits() / 8u64) as usize;
        }
        mem
    }

    pub fn item_exists(&self, item: &[u8]) -> bool {
        // Check if item exists already.
        for filter in &self.filters {
            if filter.bloom.check(item) {
                return true;
            }
        }
        false
    }

    pub fn cardinality(&self) -> i64  {
        let mut cardinality = 0;
        // Check if item exists already.
        for filter in &self.filters {
            cardinality += filter.num_items;
        }
        cardinality as i64
    }

    pub fn capacity(&self) -> i64  {
        let mut capacity = 0;
        // Check if item exists already.
        for filter in &self.filters {
            capacity += filter.capacity;
        }
        capacity as i64
    }

    pub fn add_item(&mut self, item: &[u8]) -> i64 {
        // Check if item exists already.
        if self.item_exists(item) {
            return 0;
        }
        if let Some(filter) = self.filters.last_mut() {
            if self.expansion == 0 || filter.num_items < filter.capacity  {
                // Add item.
                filter.bloom.set(item);
                filter.num_items += 1;
                return 1;
            }
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
    pub bloom: Bloom<[u8]>,
    pub num_items: u32,
    pub capacity: u32,
}

impl BloomFilter {
    pub fn new(fp_rate: f32, capacity: u32) -> BloomFilter {
        // Instantiate empty bloom filter.
        let bloom = Bloom::new_for_fp_rate(
            capacity as usize,
            fp_rate as f64,
        );
        BloomFilter {
            bloom,
            num_items: 0,
            capacity,
        }
    }

    pub fn from_existing(bitmap: &[u8], number_of_bits: u64, number_of_hash_functions: u32, sip_keys: [(u64, u64); 2], num_items: u32, capacity: u32) -> BloomFilter {
        let bloom = Bloom::from_existing(
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
}
