//! `BucketedTopK`: HeavyKeeper variant that hashes once into a single bucket
//! of `depth` cells. Preserves the decay rule but not the paper's
//! row-independence accuracy bounds.

use std::borrow::Borrow;
use std::fmt::Debug;
use std::hash::Hash;

use ahash::RandomState;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use thiserror::Error;

use crate::priority_queue::TopKQueue;

const DECAY_LOOKUP_SIZE: usize = 1024;

/// Probe hashed at merge time to detect mismatched hashers.
const MERGE_HASHER_PROBE: &[u8] = b"heavykeeper-merge-compat-probe";

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BucketedNode<T> {
    pub item: T,
    pub count: u64,
}

impl<T: Ord> Ord for BucketedNode<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.count.cmp(&self.count)
    }
}

impl<T: Ord> PartialOrd for BucketedNode<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum BucketedMergeError {
    #[error("Incompatible width: self ({self_width}) != other ({other_width})")]
    IncompatibleWidth {
        self_width: usize,
        other_width: usize,
    },

    #[error("Incompatible depth: self ({self_depth}) != other ({other_depth})")]
    IncompatibleDepth {
        self_depth: usize,
        other_depth: usize,
    },

    #[error("Incompatible decay: self ({self_decay}) != other ({other_decay})")]
    IncompatibleDecay { self_decay: f64, other_decay: f64 },

    #[error("Incompatible top_items: self ({self_items}) != other ({other_items})")]
    IncompatibleTopItems {
        self_items: usize,
        other_items: usize,
    },

    #[error("Incompatible hashers: sketches were built with different seeds or hasher state")]
    IncompatibleHasher,
}

#[derive(Error, Debug)]
pub enum BucketedBuilderError {
    #[error("Missing required field: {field}")]
    MissingField { field: String },
    #[error("Invalid depth {depth}: must be >= 1")]
    InvalidDepth { depth: usize },
    #[error("Invalid width {width}: must be >= 1")]
    InvalidWidth { width: usize },
    #[error("Invalid decay {decay}: must be a finite value in 0.0..=1.0")]
    InvalidDecay { decay: f64 },
}

#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
struct Cell {
    fingerprint: u64,
    count: u64,
}

fn precompute_decay_thresholds(decay: f64, num_entries: usize) -> Box<[u64]> {
    (0..num_entries)
        .map(|count| (decay.powf(count as f64) * u64::MAX as f64) as u64)
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

#[derive(Clone)]
pub struct BucketedTopK<T: Ord + Clone + Hash> {
    width: usize,
    /// Non-zero when `width` is a power of two and `> 1`; in that case
    /// bucket indexing uses `hash & width_mask` instead of `% width`.
    width_mask: usize,
    depth: usize,
    decay: f64,
    cells: Box<[Cell]>,
    decay_thresholds: Box<[u64]>,
    priority_queue: TopKQueue<T>,
    hasher: RandomState,
    rng: SmallRng,
    min_pq_count: u64,
    top_items: usize,
}

impl<T: Ord + Clone + Hash> BucketedTopK<T> {
    pub fn new(k: usize, width: usize, depth: usize, decay: f64) -> Self {
        Self::with_seed(k, width, depth, decay, 12345)
    }

    pub fn with_seed(k: usize, width: usize, depth: usize, decay: f64, seed: u64) -> Self {
        let hasher = RandomState::with_seeds(seed, seed, seed, seed);
        Self::with_hasher(k, width, depth, decay, hasher)
    }

    /// Caller-supplied hasher. Merge compatibility is probe-checked; see `merge`.
    pub fn with_hasher(
        k: usize,
        width: usize,
        depth: usize,
        decay: f64,
        hasher: RandomState,
    ) -> Self {
        Self::with_components(k, width, depth, decay, hasher, SmallRng::seed_from_u64(0))
    }

    pub fn builder() -> BucketedBuilder<T> {
        BucketedBuilder::new()
    }

    fn with_components(
        k: usize,
        width: usize,
        depth: usize,
        decay: f64,
        hasher: RandomState,
        rng: SmallRng,
    ) -> Self {
        let priority_queue = TopKQueue::with_capacity_and_hasher(k, hasher.clone());
        let width_mask = if width > 1 && width.is_power_of_two() {
            width - 1
        } else {
            0
        };
        Self {
            width,
            width_mask,
            depth,
            decay,
            cells: vec![Cell::default(); width * depth].into_boxed_slice(),
            decay_thresholds: precompute_decay_thresholds(decay, DECAY_LOOKUP_SIZE),
            priority_queue,
            hasher,
            rng,
            min_pq_count: 0,
            top_items: k,
        }
    }

    #[inline]
    fn bucket_index(&self, hash: u64) -> usize {
        if self.width_mask != 0 {
            (hash as usize) & self.width_mask
        } else {
            (hash as usize) % self.width
        }
    }

    #[inline]
    fn bucket_range(&self, bucket_idx: usize) -> std::ops::Range<usize> {
        let start = bucket_idx * self.depth;
        start..start + self.depth
    }

    pub fn add<Q>(&mut self, item: &Q, increment: u64)
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        let _ = self.add_with_evicted(item, increment);
    }

    pub fn add_with_evicted<Q>(&mut self, item: &Q, increment: u64) -> (Option<T>, bool)
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        if increment == 0 {
            return (None, false);
        }
        let h = self.hasher.hash_one(item);
        let fp = h;
        let bucket_idx = self.bucket_index(h);
        let range = self.bucket_range(bucket_idx);
        let bucket_start = range.start;
        let cells = &mut self.cells[range];

        let mut matched: Option<usize> = None;
        let mut first_empty: Option<usize> = None;
        let mut min_idx: usize = 0;
        let mut min_count: u64 = u64::MAX;

        for (i, cell) in cells.iter().enumerate() {
            if cell.count == 0 {
                if first_empty.is_none() {
                    first_empty = Some(i);
                }
                continue;
            }
            if matched.is_none() && cell.fingerprint == fp {
                matched = Some(i);
            }
            if cell.count < min_count {
                min_count = cell.count;
                min_idx = i;
            }
        }

        let cell_count: Option<u64> = if let Some(i) = matched {
            cells[i].count = cells[i].count.saturating_add(increment);
            Some(cells[i].count)
        } else if let Some(i) = first_empty {
            cells[i].fingerprint = fp;
            cells[i].count = increment;
            Some(increment)
        } else {
            self.decay_and_maybe_evict(bucket_start, min_idx, fp, increment)
        };

        let max_count = match cell_count {
            Some(c) => c,
            None => return (None, false),
        };

        // Paper Algorithm 1: heap value is max(maxv, existing_heap_value).
        // Item already in PQ → only raise; cell-decay must not drag PQ down.
        if let Some(current) = self.priority_queue.get(item) {
            if max_count > current {
                self.priority_queue.update_if_present(item, max_count);
                self.min_pq_count = self.priority_queue.min_count();
            }
            return (None, false);
        }

        if self.priority_queue.is_full() && max_count <= self.min_pq_count {
            return (None, false);
        }

        let had_room = !self.priority_queue.is_full();
        let evicted = self.priority_queue.upsert(item.to_owned(), max_count);
        self.min_pq_count = self.priority_queue.min_count();
        let inserted = evicted.is_some() || had_room;
        (evicted, inserted)
    }

    pub fn count<Q>(&self, item: &Q) -> u64
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        if let Some(c) = self.priority_queue.get(item) {
            return c;
        }
        self.bucket_count(item)
    }

    pub fn bucket_count<Q>(&self, item: &Q) -> u64
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        let h = self.hasher.hash_one(item);
        let bucket_idx = self.bucket_index(h);
        let cells = &self.cells[self.bucket_range(bucket_idx)];
        for cell in cells {
            if cell.count > 0 && cell.fingerprint == h {
                return cell.count;
            }
        }
        0
    }

    /// Returns true if `item` is present in the sketch (its estimated count
    /// is non-zero). This is a probabilistic membership test and may report
    /// false positives due to fingerprint collisions; it is *not* the same as
    /// [`contains_top_k`](Self::contains_top_k), which tests top-k membership.
    pub fn contains<Q>(&self, item: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        self.count(item) > 0
    }

    /// Deprecated alias for [`contains`](Self::contains).
    #[deprecated(since = "0.6.9", note = "renamed to `contains`")]
    pub fn query<Q>(&self, item: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        self.contains(item)
    }

    /// Returns true if `item` is currently one of the top-k tracked flows.
    pub fn contains_top_k<Q>(&self, item: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.priority_queue.contains(item)
    }

    pub fn list(&self) -> Vec<BucketedNode<T>> {
        let mut nodes: Vec<BucketedNode<T>> = self
            .priority_queue
            .iter()
            .map(|(item, count)| BucketedNode {
                item: item.clone(),
                count,
            })
            .collect();
        nodes.sort();
        nodes
    }

    /// Estimated heap memory (in bytes) used by this sketch, including the heap
    /// each tracked item owns beyond `size_of::<T>()`. `item_heap(t)` returns
    /// the bytes `t` points to (e.g. `String::capacity`); pass `|_| 0`
    /// for a `T` that owns no heap.
    pub fn mem_bytes<F>(&self, item_heap: F) -> usize
    where
        F: Fn(&T) -> usize,
    {
        use std::mem::size_of;
        self.cells.len() * size_of::<Cell>()
            + self.decay_thresholds.len() * size_of::<u64>()
            + self.priority_queue.mem_bytes(item_heap)
    }

    /// Merge `other` into `self`. PQ merged first using pre-merge bucket
    /// counts as fallback; cells then unioned per bucket by fingerprint
    /// (min-count eviction on full buckets).
    pub fn merge(&mut self, other: &Self) -> Result<(), BucketedMergeError> {
        if self.width != other.width {
            return Err(BucketedMergeError::IncompatibleWidth {
                self_width: self.width,
                other_width: other.width,
            });
        }
        if self.depth != other.depth {
            return Err(BucketedMergeError::IncompatibleDepth {
                self_depth: self.depth,
                other_depth: other.depth,
            });
        }
        if self.decay != other.decay {
            return Err(BucketedMergeError::IncompatibleDecay {
                self_decay: self.decay,
                other_decay: other.decay,
            });
        }
        if self.top_items != other.top_items {
            return Err(BucketedMergeError::IncompatibleTopItems {
                self_items: self.top_items,
                other_items: other.top_items,
            });
        }
        if self.hasher.hash_one(MERGE_HASHER_PROBE) != other.hasher.hash_one(MERGE_HASHER_PROBE) {
            return Err(BucketedMergeError::IncompatibleHasher);
        }

        // PQ merge before cell merge so we read pre-merge bucket counts.
        let other_pq_pairs: Vec<(T, u64)> = other
            .priority_queue
            .iter()
            .map(|(item, count)| (item.clone(), count))
            .collect();
        let self_only_updates: Vec<(T, u64)> = self
            .priority_queue
            .iter()
            .filter(|(item, _)| other.priority_queue.get(item).is_none())
            .map(|(item, self_pq)| {
                let other_bucket = other.bucket_count(item);
                (item.clone(), self_pq.saturating_add(other_bucket))
            })
            .collect();
        for (item, other_pq) in other_pq_pairs {
            let merged = match self.priority_queue.get(&item) {
                Some(self_pq) => self_pq.saturating_add(other_pq),
                None => self.bucket_count(&item).saturating_add(other_pq),
            };
            self.priority_queue.upsert(item, merged);
        }
        for (item, count) in self_only_updates {
            self.priority_queue.upsert(item, count);
        }

        for b in 0..self.width {
            let range = self.bucket_range(b);
            let (self_start, self_end) = (range.start, range.end);
            let other_start = b * self.depth;

            for o in other_start..other_start + self.depth {
                let oc = other.cells[o];
                if oc.count == 0 {
                    continue;
                }

                let mut matched: Option<usize> = None;
                let mut first_empty: Option<usize> = None;
                let mut min_idx = self_start;
                let mut min_count = u64::MAX;

                for i in self_start..self_end {
                    let sc = self.cells[i];
                    if sc.count == 0 {
                        if first_empty.is_none() {
                            first_empty = Some(i);
                        }
                        continue;
                    }
                    if sc.fingerprint == oc.fingerprint {
                        matched = Some(i);
                        break;
                    }
                    if sc.count < min_count {
                        min_count = sc.count;
                        min_idx = i;
                    }
                }

                if let Some(i) = matched {
                    self.cells[i].count = self.cells[i].count.saturating_add(oc.count);
                } else if let Some(i) = first_empty {
                    self.cells[i] = oc;
                } else if oc.count > min_count {
                    self.cells[min_idx] = oc;
                }
            }
        }

        self.min_pq_count = self.priority_queue.min_count();
        Ok(())
    }

    /// `Some(count)` if the new item took the cell, `None` if the victim survived.
    fn decay_and_maybe_evict(
        &mut self,
        bucket_start: usize,
        min_idx: usize,
        fp: u64,
        increment: u64,
    ) -> Option<u64> {
        let cell_idx = bucket_start + min_idx;
        let mut remaining = increment;
        while remaining > 0 {
            let current_count = self.cells[cell_idx].count;
            let threshold = self.decay_threshold(current_count);
            if self.rng.next_u64() < threshold {
                let cell = &mut self.cells[cell_idx];
                cell.count = cell.count.saturating_sub(1);
                if cell.count == 0 {
                    cell.fingerprint = fp;
                    cell.count = remaining;
                    return Some(remaining);
                }
            }
            remaining -= 1;
        }
        None
    }

    fn decay_threshold(&self, count: u64) -> u64 {
        if count < self.decay_thresholds.len() as u64 {
            return self.decay_thresholds[count as usize];
        }
        let tbl = &self.decay_thresholds;
        let last = tbl[tbl.len() - 1] as f64 / u64::MAX as f64;
        let divisor = (tbl.len() - 1) as u64;
        // q is u64 — use powf(q as f64) instead of powi(q as i32) which
        // would truncate (not saturate) for q > i32::MAX.
        let q = (count / divisor) as f64;
        let r = (count % divisor) as usize;
        let rem_thr = tbl[r] as f64 / u64::MAX as f64;
        ((last.powf(q) * rem_thr) * u64::MAX as f64) as u64
    }
}

#[cfg(test)]
impl<T: Ord + Clone + Hash> BucketedTopK<T> {
    pub(crate) fn hasher(&self) -> &RandomState {
        &self.hasher
    }

    pub(crate) fn decay_threshold_for_test(&self, count: u64) -> u64 {
        self.decay_threshold(count)
    }
}

pub struct BucketedBuilder<T> {
    k: Option<usize>,
    width: Option<usize>,
    depth: Option<usize>,
    decay: Option<f64>,
    seed: Option<u64>,
    hasher: Option<RandomState>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Ord + Clone + Hash> Default for BucketedBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Ord + Clone + Hash> BucketedBuilder<T> {
    pub fn new() -> Self {
        Self {
            k: None,
            width: None,
            depth: None,
            decay: None,
            seed: None,
            hasher: None,
            _phantom: std::marker::PhantomData,
        }
    }
    pub fn k(mut self, k: usize) -> Self {
        self.k = Some(k);
        self
    }
    pub fn width(mut self, w: usize) -> Self {
        self.width = Some(w);
        self
    }
    pub fn depth(mut self, d: usize) -> Self {
        self.depth = Some(d);
        self
    }
    pub fn decay(mut self, d: f64) -> Self {
        self.decay = Some(d);
        self
    }
    pub fn seed(mut self, s: u64) -> Self {
        self.seed = Some(s);
        self
    }
    pub fn hasher(mut self, h: RandomState) -> Self {
        self.hasher = Some(h);
        self
    }

    pub fn build(self) -> Result<BucketedTopK<T>, BucketedBuilderError> {
        let k = self
            .k
            .ok_or_else(|| BucketedBuilderError::MissingField { field: "k".into() })?;
        let width = self
            .width
            .ok_or_else(|| BucketedBuilderError::MissingField {
                field: "width".into(),
            })?;
        let depth = self
            .depth
            .ok_or_else(|| BucketedBuilderError::MissingField {
                field: "depth".into(),
            })?;
        let decay = self
            .decay
            .ok_or_else(|| BucketedBuilderError::MissingField {
                field: "decay".into(),
            })?;
        if width < 1 {
            return Err(BucketedBuilderError::InvalidWidth { width });
        }
        if depth < 1 {
            return Err(BucketedBuilderError::InvalidDepth { depth });
        }
        if !decay.is_finite() || !(0.0..=1.0).contains(&decay) {
            return Err(BucketedBuilderError::InvalidDecay { decay });
        }
        let hasher = self.hasher.unwrap_or_else(|| {
            if let Some(s) = self.seed {
                RandomState::with_seeds(s, s, s, s)
            } else {
                RandomState::new()
            }
        });
        let rng = SmallRng::seed_from_u64(self.seed.unwrap_or(0));
        Ok(BucketedTopK::with_components(
            k, width, depth, decay, hasher, rng,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_bytes_covers_cells_and_decay_table() {
        let topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 100, 4, 0.9);
        let cells = 100 * 4 * std::mem::size_of::<Cell>();
        let decay = DECAY_LOOKUP_SIZE * std::mem::size_of::<u64>();
        // The fixed cell array and decay table are exact; the priority
        // queue adds more on top.
        assert!(topk.mem_bytes(|_| 0) >= cells + decay);
    }

    #[test]
    fn test_mem_bytes_grows_with_width() {
        let small: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 100, 4, 0.9);
        let large: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 400, 4, 0.9);
        assert!(large.mem_bytes(|_| 0) > small.mem_bytes(|_| 0));
    }

    #[test]
    fn test_mem_bytes_grows_with_depth() {
        let shallow: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 100, 2, 0.9);
        let deep: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 100, 8, 0.9);
        assert!(deep.mem_bytes(|_| 0) > shallow.mem_bytes(|_| 0));
    }

    #[test]
    fn test_new_default_params() {
        let topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 100, 4, 0.9);
        assert_eq!(topk.width, 100);
        assert_eq!(topk.depth, 4);
        assert_eq!(topk.decay, 0.9);
        assert_eq!(topk.top_items, 10);
        assert_eq!(topk.cells.len(), 400);
        assert_eq!(topk.priority_queue.len(), 0);
    }

    #[test]
    fn test_new_depth_two() {
        let topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 100, 2, 0.9);
        assert_eq!(topk.depth, 2);
        assert_eq!(topk.cells.len(), 200);
    }

    #[test]
    fn test_new_depth_eight() {
        let topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 64, 8, 0.9);
        assert_eq!(topk.depth, 8);
        assert_eq!(topk.cells.len(), 512);
    }

    #[test]
    fn test_add_increments_existing() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 64, 4, 0.9);
        let item = b"hello".to_vec();
        topk.add(&item, 3);
        topk.add(&item, 2);
        assert_eq!(topk.count(&item), 5);
    }

    #[test]
    fn test_add_increments_existing_depth_two() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 64, 2, 0.9);
        let item = b"hello".to_vec();
        topk.add(&item, 3);
        topk.add(&item, 2);
        assert_eq!(topk.count(&item), 5);
    }

    #[test]
    fn test_contains_and_list() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 64, 4, 0.9);
        topk.add(&b"a".to_vec(), 5);
        topk.add(&b"b".to_vec(), 1);
        assert!(topk.contains(&b"a".to_vec()));
        assert!(!topk.contains(&b"missing".to_vec()));
        let list = topk.list();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].item, b"a".to_vec());
        assert_eq!(list[0].count, 5);
    }

    /// The deprecated `query` alias must still compile and delegate to `contains`.
    #[test]
    #[allow(deprecated)]
    fn test_query_alias_delegates_to_contains() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 64, 4, 0.9);
        topk.add(&b"a".to_vec(), 5);
        assert_eq!(topk.query(&b"a".to_vec()), topk.contains(&b"a".to_vec()));
        assert_eq!(
            topk.query(&b"missing".to_vec()),
            topk.contains(&b"missing".to_vec())
        );
    }

    #[test]
    fn test_contains_top_k_distinguishes_tracked_from_sketch_only() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(1, 1, 1, 0.9);

        topk.add(b"hot".as_slice(), 100);
        assert!(topk.contains_top_k(b"hot".as_slice()));

        topk.add(b"cold".as_slice(), 1);
        assert!(!topk.contains_top_k(b"cold".as_slice()));
        assert!(!topk.contains_top_k(b"absent".as_slice()));
    }

    #[test]
    fn test_contains_top_k_borrowed_lookup() {
        let mut topk: BucketedTopK<String> = BucketedTopK::new(10, 100, 4, 0.9);
        topk.add("foo", 5);
        assert!(topk.contains_top_k("foo"));
        assert!(!topk.contains_top_k("bar"));
    }

    #[test]
    fn test_add_evicts_on_full_decay() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(1, 1, 4, 1.0);
        topk.add(&b"a".to_vec(), 1);
        topk.add(&b"b".to_vec(), 1);
        topk.add(&b"c".to_vec(), 1);
        topk.add(&b"d".to_vec(), 1);
        topk.decay_thresholds.iter_mut().for_each(|t| *t = u64::MAX);
        topk.add(&b"e".to_vec(), 1);
        assert_eq!(topk.count(&b"e".to_vec()), 1);
    }

    #[test]
    fn test_merge_basic() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 64, 4, 0.9);
        let mut b: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 64, 4, 0.9);
        a.add(&b"x".to_vec(), 5);
        a.add(&b"y".to_vec(), 3);
        b.add(&b"x".to_vec(), 4);
        b.add(&b"z".to_vec(), 6);
        a.merge(&b).expect("compatible");
        assert_eq!(a.count(&b"x".to_vec()), 9);
        assert_eq!(a.count(&b"z".to_vec()), 6);
    }

    #[test]
    fn test_merge_incompatible_width() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 64, 4, 0.9);
        let b: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 32, 4, 0.9);
        match a.merge(&b) {
            Err(BucketedMergeError::IncompatibleWidth {
                self_width,
                other_width,
            }) => {
                assert_eq!(self_width, 64);
                assert_eq!(other_width, 32);
            }
            _ => panic!("expected IncompatibleWidth"),
        }
    }

    #[test]
    fn test_merge_incompatible_depth() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 64, 4, 0.9);
        let b: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 64, 2, 0.9);
        match a.merge(&b) {
            Err(BucketedMergeError::IncompatibleDepth {
                self_depth,
                other_depth,
            }) => {
                assert_eq!(self_depth, 4);
                assert_eq!(other_depth, 2);
            }
            _ => panic!("expected IncompatibleDepth"),
        }
    }

    #[test]
    fn test_builder_missing_fields() {
        let r = BucketedBuilder::<Vec<u8>>::new()
            .width(64)
            .depth(4)
            .decay(0.9)
            .build();
        assert!(matches!(r, Err(BucketedBuilderError::MissingField { field }) if field == "k"));

        let r = BucketedBuilder::<Vec<u8>>::new()
            .k(10)
            .depth(4)
            .decay(0.9)
            .build();
        assert!(matches!(r, Err(BucketedBuilderError::MissingField { field }) if field == "width"));

        let r = BucketedBuilder::<Vec<u8>>::new()
            .k(10)
            .width(64)
            .decay(0.9)
            .build();
        assert!(matches!(r, Err(BucketedBuilderError::MissingField { field }) if field == "depth"));

        let r = BucketedBuilder::<Vec<u8>>::new()
            .k(10)
            .width(64)
            .depth(4)
            .build();
        assert!(matches!(r, Err(BucketedBuilderError::MissingField { field }) if field == "decay"));
    }

    #[test]
    fn test_builder_invalid_depth_zero() {
        let r = BucketedBuilder::<Vec<u8>>::new()
            .k(10)
            .width(64)
            .depth(0)
            .decay(0.9)
            .build();
        assert!(matches!(
            r,
            Err(BucketedBuilderError::InvalidDepth { depth: 0 })
        ));
    }

    #[test]
    fn test_builder_accepts_depths_other_than_four() {
        for d in [1usize, 2, 3, 5, 8] {
            let r = BucketedBuilder::<Vec<u8>>::new()
                .k(10)
                .width(64)
                .depth(d)
                .decay(0.9)
                .build();
            assert!(r.is_ok(), "depth={d} should build");
        }
    }

    #[test]
    fn test_non_ascii_and_emoji() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(5, 100, 4, 0.9);
        let p = "पुष्पं अस्ति।".as_bytes().to_vec();
        let emoji = "🚀🌟".as_bytes().to_vec();
        topk.add(&p, 1);
        topk.add(&emoji, 1);
        assert!(topk.contains(&p));
        assert!(topk.contains(&emoji));
        assert_eq!(topk.count(&p), 1);
        assert_eq!(topk.count(&emoji), 1);
    }

    #[test]
    fn test_add_more_items_than_capacity() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(2, 100, 4, 0.9);
        for name in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()] {
            topk.add(&name, 1);
        }
        assert_eq!(topk.list().len(), 2);
    }

    #[test]
    fn test_large_number_of_duplicates() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 100, 4, 0.9);
        let item = b"rep".to_vec();
        topk.add(&item, 1000);
        assert_eq!(topk.count(&item), 1000);
    }

    #[test]
    fn test_add_varied_frequencies_top_k_membership() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 2000, 4, 0.98);
        for i in 0..100u32 {
            let k = format!("item{i}").into_bytes();
            for _ in 0..=(i as u64) {
                topk.add(&k, 1);
            }
        }
        assert_eq!(topk.list().len(), 10);
        let expected: Vec<Vec<u8>> = (90..100).map(|i| format!("item{i}").into_bytes()).collect();
        let found = expected
            .iter()
            .filter(|e| topk.list().iter().any(|n| &n.item == *e))
            .count();
        assert!(
            found >= 8,
            "at least 8 of top 10 should be in list (got {found})"
        );
    }

    #[test]
    fn test_borrow_str_and_slice() {
        let mut topk: BucketedTopK<String> = BucketedTopK::new(10, 100, 4, 0.9);
        topk.add("foo", 1);
        assert!(topk.contains("foo"));
        assert_eq!(topk.count("foo"), 1);

        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 100, 4, 0.9);
        let item: &[u8] = b"foo";
        topk.add(item, 1);
        assert!(topk.contains(item));
        assert_eq!(topk.count(item), 1);
    }

    #[test]
    fn test_merge_slot_order_independent() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::new(2, 2, 4, 0.9);
        let mut b: BucketedTopK<Vec<u8>> = BucketedTopK::new(2, 2, 4, 0.9);

        // Find four keys hashing to bucket 0 so we can control slot order.
        let mut bucket0_keys: Vec<Vec<u8>> = Vec::new();
        let probe = BucketedTopK::<Vec<u8>>::new(2, 2, 4, 0.9);
        let mut n: u32 = 0;
        while bucket0_keys.len() < 4 {
            let k = format!("k{n}").into_bytes();
            if (probe.hasher().hash_one(&k) as usize) % 2 == 0 {
                bucket0_keys.push(k);
            }
            n += 1;
        }
        let (k0, k1, k2, k3) = (
            bucket0_keys[0].clone(),
            bucket0_keys[1].clone(),
            bucket0_keys[2].clone(),
            bucket0_keys[3].clone(),
        );

        a.add(&k0, 10);
        a.add(&k1, 20);
        a.add(&k2, 30);
        a.add(&k3, 40);

        // Reverse insert order: same fingerprints in different slots.
        b.add(&k3, 5);
        b.add(&k2, 7);
        b.add(&k1, 9);
        b.add(&k0, 11);

        a.merge(&b).expect("compatible");

        assert_eq!(a.bucket_count(&k0), 21, "k0 = 10 + 11");
        assert_eq!(a.bucket_count(&k1), 29, "k1 = 20 + 9");
        assert_eq!(a.bucket_count(&k2), 37, "k2 = 30 + 7");
        assert_eq!(a.bucket_count(&k3), 45, "k3 = 40 + 5");
    }

    #[test]
    fn test_merge_bucket_overflow_evicts_min() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 1, 2, 0.9);
        let mut b: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 1, 2, 0.9);

        a.add(&b"big".to_vec(), 100);
        a.add(&b"small".to_vec(), 5);

        b.add(&b"medium".to_vec(), 50);

        a.merge(&b).unwrap();

        assert_eq!(a.bucket_count(&b"big".to_vec()), 100, "big survives");
        assert_eq!(a.bucket_count(&b"medium".to_vec()), 50, "medium installed");
        assert_eq!(a.bucket_count(&b"small".to_vec()), 0, "small evicted");
    }

    #[test]
    fn test_merge_overflow_preserves_when_incoming_is_smaller() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 1, 2, 0.9);
        let mut b: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 1, 2, 0.9);

        a.add(&b"big".to_vec(), 100);
        a.add(&b"medium".to_vec(), 50);

        b.add(&b"tiny".to_vec(), 3);

        a.merge(&b).unwrap();

        assert_eq!(a.bucket_count(&b"big".to_vec()), 100);
        assert_eq!(a.bucket_count(&b"medium".to_vec()), 50);
        assert_eq!(a.bucket_count(&b"tiny".to_vec()), 0);
    }

    #[test]
    fn test_merge_priority_queue_reflects_summed_counts() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 64, 4, 0.9);
        let mut b: BucketedTopK<Vec<u8>> = BucketedTopK::new(3, 64, 4, 0.9);

        for _ in 0..100 {
            a.add(&b"hot".to_vec(), 1);
        }
        for _ in 0..50 {
            a.add(&b"warm".to_vec(), 1);
        }
        for _ in 0..200 {
            b.add(&b"hot".to_vec(), 1);
        }
        for _ in 0..30 {
            b.add(&b"cool".to_vec(), 1);
        }

        a.merge(&b).unwrap();

        assert_eq!(a.count(&b"hot".to_vec()), 300);
        assert_eq!(a.count(&b"warm".to_vec()), 50);
        assert_eq!(a.count(&b"cool".to_vec()), 30);

        let list = a.list();
        assert_eq!(list[0].item, b"hot".to_vec());
        assert_eq!(list[0].count, 300);
    }

    #[test]
    fn test_merge_incompatible_hasher_different_seed() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::with_seed(10, 64, 4, 0.9, 1);
        let b: BucketedTopK<Vec<u8>> = BucketedTopK::with_seed(10, 64, 4, 0.9, 2);
        match a.merge(&b) {
            Err(BucketedMergeError::IncompatibleHasher) => {}
            other => panic!("expected IncompatibleHasher, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_compatible_with_same_explicit_seed() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::with_seed(10, 64, 4, 0.9, 7);
        let mut b: BucketedTopK<Vec<u8>> = BucketedTopK::with_seed(10, 64, 4, 0.9, 7);
        a.add(&b"x".to_vec(), 3);
        b.add(&b"x".to_vec(), 4);
        a.merge(&b).expect("same seed should be compatible");
        assert_eq!(a.count(&b"x".to_vec()), 7);
    }

    #[test]
    fn test_merge_unseeded_builder_hashers_incompatible() {
        let mut a: BucketedTopK<Vec<u8>> = BucketedTopK::builder()
            .k(10)
            .width(64)
            .depth(4)
            .decay(0.9)
            .build()
            .unwrap();
        let b: BucketedTopK<Vec<u8>> = BucketedTopK::builder()
            .k(10)
            .width(64)
            .depth(4)
            .decay(0.9)
            .build()
            .unwrap();
        match a.merge(&b) {
            Err(BucketedMergeError::IncompatibleHasher) => {}
            other => panic!("expected IncompatibleHasher, got {:?}", other),
        }
    }

    #[test]
    fn test_add_increment_zero_is_noop() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(5, 64, 4, 0.9);
        topk.add(&b"a".to_vec(), 0);
        assert_eq!(topk.count(&b"a".to_vec()), 0);
        assert!(topk.list().is_empty());
    }

    #[test]
    fn test_builder_rejects_decay_out_of_range() {
        let cases = [-0.1f64, 1.1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
        for d in cases {
            let res: Result<BucketedTopK<Vec<u8>>, _> = BucketedTopK::builder()
                .k(10)
                .width(64)
                .depth(4)
                .decay(d)
                .build();
            match res {
                Ok(_) => panic!("expected InvalidDecay for {d}, got Ok"),
                Err(BucketedBuilderError::InvalidDecay { decay }) => {
                    assert!(
                        decay.is_nan() || decay == d,
                        "got back {decay} for input {d}"
                    );
                }
                Err(other) => panic!("expected InvalidDecay for {d}, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_add_count_saturates_on_overflow() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(2, 1, 1, 0.9);
        topk.add(&b"x".to_vec(), u64::MAX);
        topk.add(&b"x".to_vec(), 1);
        assert_eq!(topk.count(&b"x".to_vec()), u64::MAX);
        topk.add(&b"x".to_vec(), 1_000_000);
        assert_eq!(topk.count(&b"x".to_vec()), u64::MAX);
    }

    #[test]
    fn test_decay_threshold_no_usize_truncation_for_large_count() {
        // On 32-bit targets, `count as usize` would truncate u64 counts
        // greater than u32::MAX, returning a large threshold from the
        // start of the lookup table instead of a tiny one. Verify that
        // for counts > u32::MAX the returned threshold is effectively zero
        // (decay^4_billion underflows to ~0).
        let topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 64, 4, 0.9);
        let huge: u64 = (u32::MAX as u64) + 5000;
        let thr = topk.decay_threshold_for_test(huge);
        // A buggy 32-bit truncation would return ~u64::MAX (a small lookup index).
        assert!(
            thr < u64::MAX / 2,
            "expected ~0 threshold for huge count, got {thr}"
        );
    }

    #[test]
    fn test_decay_threshold_no_powi_i32_overflow_for_huge_count() {
        // `q = count / 1023` past i32::MAX would truncate (not saturate)
        // to a negative i32; `powi(neg)` of a fractional base diverges
        // and the threshold saturates to u64::MAX. Use powf instead.
        let topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(10, 64, 4, 0.9);
        let huge: u64 = (i32::MAX as u64) * 2048;
        let thr = topk.decay_threshold_for_test(huge);
        assert!(
            thr < u64::MAX / 2,
            "expected ~0 threshold for huge count, got {thr}"
        );
    }

    #[test]
    fn test_failed_eviction_does_not_pollute_pq() {
        // decay=0.0 makes eviction impossible: a losing add must not inherit
        // the resident victim's count.
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(2, 1, 1, 0.0);
        topk.add(&b"heavy".to_vec(), 100);
        topk.add(&b"new".to_vec(), 1);

        assert_eq!(topk.count(&b"heavy".to_vec()), 100);
        assert_eq!(
            topk.count(&b"new".to_vec()),
            0,
            "new lost the eviction race; it must not inherit heavy's count"
        );
        let list = topk.list();
        assert!(
            list.iter()
                .all(|n| n.item != b"new".to_vec() || n.count < 100),
            "new must not appear at heavy's count in top-k list"
        );
    }

    #[test]
    fn test_add_with_evicted_returns_displaced_item() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(2, 64, 4, 0.9);

        // New keys into free space: no eviction, but inserted.
        assert_eq!(topk.add_with_evicted(&b"a".to_vec(), 5), (None, true));
        assert_eq!(topk.add_with_evicted(&b"b".to_vec(), 10), (None, true));
        assert_eq!(topk.list().len(), 2);

        let (evicted, inserted) = topk.add_with_evicted(&b"c".to_vec(), 20);
        assert_eq!(evicted.expect("expected an eviction"), b"a".to_vec());
        assert!(inserted);

        let items: Vec<_> = topk.list().iter().map(|n| n.item.clone()).collect();
        assert!(items.contains(&b"b".to_vec()));
        assert!(items.contains(&b"c".to_vec()));
        assert!(!items.contains(&b"a".to_vec()));
    }

    #[test]
    fn test_add_with_evicted_no_eviction_cases() {
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(2, 64, 4, 0.9);

        // increment == 0 → no work, nothing tracked.
        assert_eq!(topk.add_with_evicted(&b"a".to_vec(), 0), (None, false));

        // PQ not yet full → no eviction, but inserted.
        assert_eq!(topk.add_with_evicted(&b"a".to_vec(), 5), (None, true));
        assert_eq!(topk.add_with_evicted(&b"b".to_vec(), 10), (None, true));

        // Updating an already-tracked item → neither evicted nor inserted.
        assert_eq!(topk.add_with_evicted(&b"a".to_vec(), 1), (None, false));

        // New item whose count cannot beat the PQ minimum → nothing tracked.
        let mut topk: BucketedTopK<Vec<u8>> = BucketedTopK::new(2, 64, 4, 0.9);
        topk.add_with_evicted(&b"hot".to_vec(), 50);
        topk.add_with_evicted(&b"warm".to_vec(), 30);
        assert_eq!(topk.add_with_evicted(&b"cold".to_vec(), 10), (None, false));
    }
}
