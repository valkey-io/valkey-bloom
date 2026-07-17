use crate::configs;
use crate::metrics;
use crate::topk::data_type::TOPK_OBJECT_VERSION;
use heavykeeper::CuckooTopK;
use std::sync::atomic::Ordering;

/// KeySpace Notification Events
pub const RESERVE_EVENT: &str = "topk.reserve";
pub const ADD_EVENT: &str = "topk.add";
pub const DEFAULT_WIDTH: u32 = 8;
pub const DEFAULT_DEPTH: u32 = 7;
pub const DEFAULT_DECAY: f64 = 0.9;

pub const TOPK_K_MIN: u32 = 1;
pub const TOPK_K_MAX: u32 = u32::MAX;
pub const TOPK_WIDTH_MIN: u32 = 1;
pub const TOPK_WIDTH_MAX: u32 = u32::MAX;
pub const TOPK_DEPTH_MIN: u32 = 1;
pub const TOPK_DEPTH_MAX: u32 = u32::MAX;

/// Client Errors
pub const ERROR: &str = "ERROR";
pub const NOT_FOUND: &str = "ERR TopK: key does not exist";
pub const INVALID_INFO_VALUE: &str = "ERR invalid information value";
pub const KEY_EXISTS: &str = "BUSYKEY Target key name already exists.";
pub const BAD_TOPK: &str = "ERR bad topk";
pub const BAD_WIDTH: &str = "ERR bad width";
pub const BAD_DEPTH: &str = "ERR bad depth";
pub const BAD_DECAY: &str = "ERR bad decay";
pub const INVALID_SEED: &str = "ERR invalid seed";
pub const BAD_INCREMENT: &str = "ERR bad increment";
pub const TOPK_LARGER_THAN_0: &str = "ERR (topk should be larger than 0)";
pub const WIDTH_LARGER_THAN_0: &str = "ERR (width should be larger than 0)";
pub const DEPTH_LARGER_THAN_0: &str = "ERR (depth should be larger than 0)";
pub const DECAY_RANGE: &str = "ERR (0 < decay < 1)";
pub const EXCEEDS_MAX_TOPK_SIZE: &str = "ERR operation exceeds topk object memory limit";
pub const DECODE_TOPK_OBJECT_FAILED: &str = "ERR topk object decoding failed";
pub const DECODE_UNSUPPORTED_VERSION: &str = "ERR topk object decoding failed. Unsupported version";

/// TopKObject wraps the underlying CuckooTopK sketch together with the
/// parameters used to construct it.
///  (k, width, depth, decay, seed)
pub struct TopKObject {
    k: u32,
    width: u32,
    depth: u32,
    decay: f64,
    seed: u64,
    sketch: CuckooTopK<Vec<u8>>,
    num_items: u64,
}

impl TopKObject {
    /// Build a fresh TopKObject. Called from the TOPK.RESERVE command path
    /// after the handler has parsed and validated all parameters.
    pub fn new_reserved(k: u32, width: u32, depth: u32, decay: f64, seed: u64) -> TopKObject {
        let sketch = CuckooTopK::with_seed(k as usize, width as usize, depth as usize, decay, seed);
        let topk = TopKObject {
            k,
            width,
            depth,
            decay,
            seed,
            sketch,
            num_items: 0,
        };
        topk.topk_object_incr_metrics_on_new_create();
        topk
    }

    /// Create a new TopK object from existing data.
    pub fn from_existing(
        k: u32,
        width: u32,
        depth: u32,
        decay: f64,
        seed: u64,
        sketch: CuckooTopK<Vec<u8>>,
        num_items: u64,
    ) -> TopKObject {
        let topk = TopKObject {
            k,
            width,
            depth,
            decay,
            seed,
            sketch,
            num_items,
        };
        topk.topk_object_incr_metrics_on_new_create();
        topk
    }

    /// Build a deep copy of `src` for the COPY command. Clones the sketch
    /// contents (heavy/lobby cells and priority queue) and carries over the
    /// running item count.
    pub fn create_copy_from(src: &TopKObject) -> TopKObject {
        let topk = TopKObject {
            k: src.k,
            width: src.width,
            depth: src.depth,
            decay: src.decay,
            seed: src.seed,
            sketch: src.sketch.clone(),
            num_items: src.num_items,
        };
        topk.topk_object_incr_metrics_on_new_create();
        topk
    }

    /// Estimated heap size of this object: wrapper struct + sketch internals
    /// (cell arrays, decay table, priority queue) + per-item buffer capacity.
    /// The remaining undercount is allocator overhead and HashMap metadata.
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<TopKObject>() + self.sketch.mem_bytes(|item| item.capacity())
    }

    /// Bytes the sketch allocates up front.
    pub fn estimated_size(k: u32, width: u32, depth: u32) -> u64 {
        let (k, width, depth) = (k as u64, width as u64, depth as u64);
        // Saturate: products can overflow u64 at u32::MAX, and wrapping would
        // let an oversized sketch slip under the limit.
        let heavy = width.saturating_mul(depth).saturating_mul(16); // heavy cells: width × depth × 16 bytes
        (std::mem::size_of::<TopKObject>() as u64) // wrapper struct
            .saturating_add(width.saturating_mul(16)) // lobby cells: width × 16 bytes
            .saturating_add(heavy)
            .saturating_add(1024 * 8) // decay table: 1024 entries × 8 bytes
            .saturating_add(k.saturating_mul(128)) // priority queue: ~128 bytes per k entry
    }

    /// Whether these params fit within the configured topk-memory-usage-limit.
    pub fn validate_size(k: u32, width: u32, depth: u32) -> bool {
        let size_limit = configs::TOPK_MEMORY_LIMIT_PER_OBJECT.load(Ordering::Relaxed) as u64;
        Self::estimated_size(k, width, depth) <= size_limit
    }

    /// Increments metrics related to object count, memory, and summed k upon creation of a new object.
    fn topk_object_incr_metrics_on_new_create(&self) {
        metrics::TOPK_NUM_OBJECTS.fetch_add(1, Ordering::Relaxed);
        metrics::TOPK_OBJECT_TOTAL_MEMORY_BYTES.fetch_add(self.memory_usage(), Ordering::Relaxed);
        metrics::TOPK_SUM_K_ACROSS_OBJECTS.fetch_add(self.k as u64, Ordering::Relaxed);
        metrics::TOPK_TOTAL_ITEMS_ADDED_ACROSS_OBJECTS.fetch_add(self.num_items, Ordering::Relaxed);
    }

    pub fn k(&self) -> u32 {
        self.k
    }
    pub fn width(&self) -> u32 {
        self.width
    }
    pub fn depth(&self) -> u32 {
        self.depth
    }
    pub fn decay(&self) -> f64 {
        self.decay
    }
    pub fn seed(&self) -> u64 {
        self.seed
    }
    pub fn num_items(&self) -> u64 {
        self.num_items
    }
    pub fn sketch(&self) -> &CuckooTopK<Vec<u8>> {
        &self.sketch
    }
    pub fn sketch_mut(&mut self) -> &mut CuckooTopK<Vec<u8>> {
        &mut self.sketch
    }

    /// Serialize the object into a byte array.
    pub fn encode_object(&self) -> Vec<u8> {
        let sketch_bytes = self.sketch.to_bytes();
        let mut out = Vec::with_capacity(1 + 8 + 8 + sketch_bytes.len());
        out.push(TOPK_OBJECT_VERSION);
        out.extend_from_slice(&self.seed.to_le_bytes());
        out.extend_from_slice(&self.num_items.to_le_bytes());
        out.extend_from_slice(&sketch_bytes);
        out
    }

    /// Validate the params recovered from a decoded sketch and build the object.
    /// Shared by the RDB load path and [`decode_object`]. `validate_size_limit`
    /// gates the memory check; the RDB path passes `false` so a tightened limit
    /// can't reject already-persisted objects on load.
    pub fn from_serialized_bytes(
        seed: u64,
        num_items: u64,
        sketch: CuckooTopK<Vec<u8>>,
        validate_size_limit: bool,
    ) -> Result<TopKObject, &'static str> {
        let k = sketch.top_items() as u64;
        let width = sketch.width() as u64;
        let depth = sketch.depth() as u64;
        if !(TOPK_K_MIN as u64..=TOPK_K_MAX as u64).contains(&k) {
            return Err(BAD_TOPK);
        }
        if !(TOPK_WIDTH_MIN as u64..=TOPK_WIDTH_MAX as u64).contains(&width) {
            return Err(BAD_WIDTH);
        }
        if !(TOPK_DEPTH_MIN as u64..=TOPK_DEPTH_MAX as u64).contains(&depth) {
            return Err(BAD_DEPTH);
        }
        let decay = sketch.decay();
        if !(decay > 0.0 && decay < 1.0) {
            return Err(DECAY_RANGE);
        }
        if validate_size_limit && !Self::validate_size(k as u32, width as u32, depth as u32) {
            return Err(EXCEEDS_MAX_TOPK_SIZE);
        }
        Ok(TopKObject::from_existing(
            k as u32,
            width as u32,
            depth as u32,
            decay,
            seed,
            sketch,
            num_items,
        ))
    }

    /// Deserialize a byte array to TopK object
    pub fn decode_object(
        blob: &[u8],
        validate_size_limit: bool,
    ) -> Result<TopKObject, &'static str> {
        // Header: 1 version byte + 8 seed + 8 num_items.
        if blob.len() < 17 {
            return Err(DECODE_TOPK_OBJECT_FAILED);
        }
        if blob[0] != TOPK_OBJECT_VERSION {
            return Err(DECODE_UNSUPPORTED_VERSION);
        }
        let seed = u64::from_le_bytes(blob[1..9].try_into().expect("8 bytes"));
        let num_items = u64::from_le_bytes(blob[9..17].try_into().expect("8 bytes"));
        let sketch = CuckooTopK::<Vec<u8>>::from_bytes(&blob[17..], seed)
            .map_err(|_| DECODE_TOPK_OBJECT_FAILED)?;
        Self::from_serialized_bytes(seed, num_items, sketch, validate_size_limit)
    }

    /// Add `increment` occurrences of `item` to the sketch and return the
    /// heavy-slot resident displaced by this insertion (if any). At most one
    /// item can be evicted per call.
    pub fn add(&mut self, item: &[u8], increment: u64) -> Option<Vec<u8>> {
        // Saturate like the underlying sketch (heavykeeper uses saturating_add),
        // and feed the gauge the actual delta so Drop's subtraction stays balanced.
        let new_num_items = self.num_items.saturating_add(increment);
        let delta = new_num_items - self.num_items;
        self.num_items = new_num_items;
        metrics::TOPK_TOTAL_ITEMS_ADDED_ACROSS_OBJECTS.fetch_add(delta, Ordering::Relaxed);
        let (evicted, inserted) = self.sketch.add_with_evicted(item, increment);
        let added = if inserted { item.len() } else { 0 };
        let removed = evicted.as_ref().map_or(0, Vec::len);
        // The priority queue stores each item's bytes twice (HashMap key +
        // item_store), so scale the byte delta by 2.
        if added >= removed {
            metrics::TOPK_OBJECT_TOTAL_MEMORY_BYTES
                .fetch_add(2 * (added - removed), Ordering::Relaxed);
        } else {
            metrics::TOPK_OBJECT_TOTAL_MEMORY_BYTES
                .fetch_sub(2 * (removed - added), Ordering::Relaxed);
        }
        evicted
    }

    /// Return the estimated count for `item`, or 0 if it has no residual
    /// presence in the priority queue or sketch cells.
    pub fn count(&self, item: &[u8]) -> u64 {
        self.sketch.count(item)
    }

    /// Return whether `item` is currently in the Top-K list.
    pub fn query(&self, item: &[u8]) -> bool {
        self.sketch.contains_top_k(item)
    }

    /// Return the Top-K items
    pub fn list(&self) -> Vec<(Vec<u8>, u64)> {
        self.sketch
            .list()
            .into_iter()
            .map(|node| (node.item, node.count))
            .collect()
    }
}

impl Drop for TopKObject {
    fn drop(&mut self) {
        metrics::TOPK_NUM_OBJECTS.fetch_sub(1, Ordering::Relaxed);
        metrics::TOPK_OBJECT_TOTAL_MEMORY_BYTES.fetch_sub(self.memory_usage(), Ordering::Relaxed);
        metrics::TOPK_SUM_K_ACROSS_OBJECTS.fetch_sub(self.k as u64, Ordering::Relaxed);
        metrics::TOPK_TOTAL_ITEMS_ADDED_ACROSS_OBJECTS.fetch_sub(self.num_items, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn test_new_reserved_stores_params() {
        // The constructor should round-trip every parameter through the getters.
        let topk = TopKObject::new_reserved(5, 50, 4, 0.9, 42);
        assert_eq!(topk.k(), 5);
        assert_eq!(topk.width(), 50);
        assert_eq!(topk.depth(), 4);
        assert_eq!(topk.decay(), 0.9);
        assert_eq!(topk.seed(), 42);
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_add_zero_increment_is_noop(seed: u64) {
        let mut topk = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, seed);
        // A zero increment does no work and never evicts.
        assert_eq!(topk.add(b"apple", 0), None);
        assert!(topk.list().is_empty());
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_add_no_eviction_until_full(seed: u64) {
        let mut topk = TopKObject::new_reserved(2, 256, 4, DEFAULT_DECAY, seed);
        // Priority queue has room (k=2), so neither insert evicts.
        assert_eq!(topk.add(b"apple", 5), None);
        assert_eq!(topk.add(b"banana", 10), None);
        assert_eq!(topk.list().len(), 2);
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_add_returns_evicted_item(seed: u64) {
        let mut topk = TopKObject::new_reserved(2, 256, 4, DEFAULT_DECAY, seed);
        assert_eq!(topk.add(b"apple", 5), None);
        assert_eq!(topk.add(b"banana", 10), None);
        assert_eq!(topk.add(b"cherry", 20), Some(b"apple".to_vec()));

        let items: Vec<Vec<u8>> = topk.list().into_iter().map(|(item, _)| item).collect();
        assert!(items.contains(&b"banana".to_vec()));
        assert!(items.contains(&b"cherry".to_vec()));
        assert!(!items.contains(&b"apple".to_vec()));
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_add_low_count_does_not_displace_min(seed: u64) {
        let mut topk = TopKObject::new_reserved(2, 256, 4, DEFAULT_DECAY, seed);
        topk.add(b"hot", 50);
        topk.add(b"warm", 30);
        // "cold" never beats the current minimum (30), so nothing is evicted.
        assert_eq!(topk.add(b"cold", 10), None);
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_add_existing_item_accumulates_count(seed: u64) {
        let mut topk = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, seed);
        topk.add(b"apple", 4);
        topk.add(b"apple", 6);
        let counts: std::collections::HashMap<Vec<u8>, u64> = topk.list().into_iter().collect();
        assert_eq!(counts.get(b"apple".as_slice()), Some(&10));
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_list_sorted_by_descending_count(seed: u64) {
        let mut topk = TopKObject::new_reserved(5, 256, 4, DEFAULT_DECAY, seed);
        topk.add(b"apple", 10);
        topk.add(b"banana", 5);
        topk.add(b"cherry", 2);

        let listed = topk.list();
        assert_eq!(
            listed,
            vec![
                (b"apple".to_vec(), 10),
                (b"banana".to_vec(), 5),
                (b"cherry".to_vec(), 2),
            ]
        );
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_list_never_exceeds_k(seed: u64) {
        let mut topk = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, seed);
        for (item, incr) in [
            (b"a".as_slice(), 10),
            (b"b".as_slice(), 9),
            (b"c".as_slice(), 8),
            (b"d".as_slice(), 7),
            (b"e".as_slice(), 6),
        ] {
            topk.add(item, incr);
        }
        assert!(topk.list().len() <= 3);
    }

    #[rstest(
        item_a,
        item_b,
        case::numeric(b"12345", b"67890"),
        case::mixed(b"item-1", b"item-2")
    )]
    fn test_add_and_list_with_non_alpha_items(item_a: &[u8], item_b: &[u8]) {
        let mut topk = TopKObject::new_reserved(2, 256, 4, DEFAULT_DECAY, 42);
        topk.add(item_a, 10);
        topk.add(item_b, 5);

        let counts: std::collections::HashMap<Vec<u8>, u64> = topk.list().into_iter().collect();
        assert_eq!(counts.get(item_a), Some(&10));
        assert_eq!(counts.get(item_b), Some(&5));
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_count_reflects_added_increments(seed: u64) {
        let mut topk = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, seed);
        // Untracked items report a count of zero before anything is added.
        assert_eq!(topk.count(b"apple"), 0);
        topk.add(b"apple", 10);
        topk.add(b"banana", 5);
        assert_eq!(topk.count(b"apple"), 10);
        assert_eq!(topk.count(b"banana"), 5);
        // An item that was never added also reports zero.
        assert_eq!(topk.count(b"cherry"), 0);
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_count_accumulates_repeated_adds(seed: u64) {
        let mut topk = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, seed);
        topk.add(b"apple", 4);
        topk.add(b"apple", 6);
        assert_eq!(topk.count(b"apple"), 10);
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_count_never_exceeds_true_count(seed: u64) {
        let mut topk = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, seed);
        for item in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            for _ in 0..20 {
                topk.add(item, 1);
            }
            assert!(topk.count(item) <= 20);
        }
    }

    #[test]
    fn test_count_matches_list_withcount() {
        // The count reported by count() agrees with the count surfaced by list().
        let mut topk = TopKObject::new_reserved(5, 256, 4, DEFAULT_DECAY, 42);
        topk.add(b"apple", 10);
        topk.add(b"banana", 5);
        let counts: std::collections::HashMap<Vec<u8>, u64> = topk.list().into_iter().collect();
        assert_eq!(topk.count(b"apple"), counts[b"apple".as_slice()]);
        assert_eq!(topk.count(b"banana"), counts[b"banana".as_slice()]);
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_query_tracked_item_is_true(seed: u64) {
        let mut topk = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, seed);
        topk.add(b"apple", 10);
        topk.add(b"banana", 5);
        assert!(topk.query(b"apple"));
        assert!(topk.query(b"banana"));
        assert!(!topk.query(b"cherry"));
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_query_evicted_item_is_false(seed: u64) {
        // An item displaced from the top-k by hotter items is no longer a
        // member, even though it may still have a residual sketch count.
        let mut topk = TopKObject::new_reserved(2, 256, 4, DEFAULT_DECAY, seed);
        topk.add(b"apple", 5);
        topk.add(b"banana", 10);
        topk.add(b"cherry", 20);
        assert!(!topk.query(b"apple"));
        assert!(topk.query(b"banana"));
        assert!(topk.query(b"cherry"));
    }

    #[test]
    fn test_query_agrees_with_list() {
        // query() membership matches what list() reports.
        let mut topk = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, 42);
        topk.add(b"apple", 10);
        topk.add(b"banana", 5);
        let listed: std::collections::HashSet<Vec<u8>> =
            topk.list().into_iter().map(|(item, _)| item).collect();
        assert_eq!(topk.query(b"apple"), listed.contains(b"apple".as_slice()));
        assert_eq!(topk.query(b"banana"), listed.contains(b"banana".as_slice()));
        assert_eq!(
            topk.query(b"missing"),
            listed.contains(b"missing".as_slice())
        );
    }

    #[test]
    fn test_seed() {
        // Two sketches built with the same seed are deterministic: identical
        // input produces identical Top-K output.
        let mut a = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, 42);
        let mut b = TopKObject::new_reserved(3, 256, 4, DEFAULT_DECAY, 42);
        for (item, incr) in [
            (b"apple".as_slice(), 7),
            (b"banana".as_slice(), 11),
            (b"cherry".as_slice(), 3),
        ] {
            a.add(item, incr);
            b.add(item, incr);
        }
        assert_eq!(a.seed(), b.seed());
        assert_eq!(a.list(), b.list());
    }

    #[rstest(seed, case::seed_a(1), case::seed_b(42), case::seed_c(123456789))]
    fn test_topk_encode_and_decode(seed: u64) {
        let mut topk = TopKObject::new_reserved(5, 50, 4, 0.9, seed);
        for (item, incr) in [
            (b"apple".as_slice(), 10),
            (b"banana".as_slice(), 7),
            (b"cherry".as_slice(), 3),
            (b"date".as_slice(), 12),
            (b"elderberry".as_slice(), 5),
            (b"fig".as_slice(), 8),
        ] {
            topk.add(item, incr);
        }

        let blob = topk.encode_object();
        let decoded = TopKObject::decode_object(&blob, false).expect("round trip should succeed");

        assert_eq!(decoded.k(), topk.k());
        assert_eq!(decoded.width(), topk.width());
        assert_eq!(decoded.depth(), topk.depth());
        assert_eq!(decoded.decay(), topk.decay());
        assert_eq!(decoded.seed(), topk.seed());
        assert_eq!(decoded.num_items(), topk.num_items());
        assert_eq!(decoded.list(), topk.list());
        // The sketch bytes must match exactly so DEBUG DIGEST-VALUE agrees.
        assert_eq!(decoded.sketch().to_bytes(), topk.sketch().to_bytes());
    }

    #[test]
    fn test_topk_encode_and_decode_empty_object() {
        // A freshly reserved object with no items round-trips too.
        let topk = TopKObject::new_reserved(3, 16, 4, 0.9, 42);
        let blob = topk.encode_object();
        let decoded = TopKObject::decode_object(&blob, false).expect("round trip should succeed");
        assert_eq!(decoded.num_items(), 0);
        assert_eq!(decoded.sketch().to_bytes(), topk.sketch().to_bytes());
    }

    #[test]
    fn test_topk_decode_when_bytes_is_truncated_should_fail() {
        // A blob shorter than the 17-byte header is rejected.
        assert_eq!(
            TopKObject::decode_object(&[], false).err(),
            Some(DECODE_TOPK_OBJECT_FAILED)
        );
        assert_eq!(
            TopKObject::decode_object(&[TOPK_OBJECT_VERSION; 10], false).err(),
            Some(DECODE_TOPK_OBJECT_FAILED)
        );
    }

    #[test]
    fn test_topk_decode_when_unsupported_version_should_fail() {
        // A valid blob with a bumped version byte is rejected.
        let topk = TopKObject::new_reserved(3, 16, 4, 0.9, 42);
        let mut blob = topk.encode_object();
        blob[0] = TOPK_OBJECT_VERSION.wrapping_add(1);
        assert_eq!(
            TopKObject::decode_object(&blob, false).err(),
            Some(DECODE_UNSUPPORTED_VERSION)
        );
    }

    #[test]
    fn test_topk_decode_when_wrong_seed_should_fail() {
        // The sketch header carries a hasher probe keyed by the seed, so decoding
        // with a seed that does not match the one baked into the blob fails.
        let topk = TopKObject::new_reserved(3, 16, 4, 0.9, 42);
        let mut blob = topk.encode_object();
        blob[1..9].copy_from_slice(&99u64.to_le_bytes());
        assert_eq!(
            TopKObject::decode_object(&blob, false).err(),
            Some(DECODE_TOPK_OBJECT_FAILED)
        );
    }
}
