// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 flowstats Contributors
// SPDX-FileContributor: https://github.com/vnvo/flowstats/blob/v0.1.2/src/frequency/count_min.rs

//! Count-Min Sketch frequency estimator
//!
//! The Count-Min Sketch is a probabilistic data structure for estimating
//! the frequency of elements in a data stream.

use crate::math;
use crate::traits::{FrequencySketch, MergeError, Sketch};
use xxhash_rust::xxh3::xxh3_64_with_seed;

#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Count-Min Sketch for frequency estimation
///
/// The Count-Min Sketch provides frequency estimates with the following guarantees:
/// - Point query: `actual_count <= estimate <= actual_count + ε * N`
/// - Where ε = e/width and N is the total count
/// - Probability of exceeding the error bound: δ = 1/2^depth
///
/// # Example
///
/// ```
/// use flowstats::frequency::CountMinSketch;
/// use flowstats::traits::FrequencySketch;
///
/// // Create with 1% error rate and 0.1% failure probability
/// let mut cms = CountMinSketch::new(0.01, 0.001);
///
/// // Add items
/// cms.add(b"apple", 5);
/// cms.add(b"banana", 3);
/// cms.add(b"apple", 2);
///
/// // Query frequency
/// let apple_count = cms.estimate(b"apple"); // ~7
/// let banana_count = cms.estimate(b"banana"); // ~3
/// ```
#[derive(Clone, Debug)]
pub struct CountMinSketch {
    /// Width of each row
    width: usize,
    /// Number of rows (hash functions)
    depth: usize,
    /// Counter table
    table: Vec<Vec<u64>>,
    /// Total count of all items
    total_count: u64,
    /// Number of updates
    num_updates: u64,
    /// Seeds for hash functions
    seeds: Vec<u64>,
}

impl CountMinSketch {
    /// Create a new Count-Min Sketch with the given error parameters
    ///
    /// # Arguments
    ///
    /// * `epsilon` - Maximum overcount as a fraction of total (e.g., 0.01 for 1%)
    /// * `delta` - Probability of exceeding the error bound (e.g., 0.001 for 0.1%)
    ///
    /// # Panics
    ///
    /// Panics if epsilon or delta are not in (0, 1)
    pub fn new(epsilon: f64, delta: f64) -> Self {
        assert!(epsilon > 0.0 && epsilon < 1.0, "epsilon must be in (0, 1)");
        assert!(delta > 0.0 && delta < 1.0, "delta must be in (0, 1)");

        // width = ceil(e / epsilon)
        // depth = ceil(ln(1/delta))
        let width = math::ceil(core::f64::consts::E / epsilon) as usize;
        let depth = math::ceil(math::ln(1.0 / delta)) as usize;

        Self::with_dimensions(width, depth)
    }

    /// Create a Count-Min Sketch with specific dimensions
    ///
    /// # Arguments
    ///
    /// * `width` - Width of each row (larger = lower error)
    /// * `depth` - Number of rows (larger = lower failure probability)
    pub fn with_dimensions(width: usize, depth: usize) -> Self {
        assert!(width > 0, "width must be positive");
        assert!(depth > 0, "depth must be positive");

        // Generate random seeds for hash functions
        let seeds: Vec<u64> = (0..depth)
            .map(|i| (i as u64).wrapping_mul(0x9e3779b97f4a7c15))
            .collect();

        Self {
            width,
            depth,
            table: vec![vec![0u64; width]; depth],
            total_count: 0,
            num_updates: 0,
            seeds,
        }
    }

    /// Get the width of the sketch
    pub fn width(&self) -> usize {
        self.width
    }

    /// Get the depth of the sketch
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Get the total count of all items
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Add count to an item
    pub fn add(&mut self, item: &[u8], count: u64) {
        self.num_updates += 1;
        self.total_count += count;

        for (row, &seed) in self.seeds.iter().enumerate() {
            let hash = xxh3_64_with_seed(item, seed);
            let col = (hash as usize) % self.width;
            self.table[row][col] = self.table[row][col].saturating_add(count);
        }
    }

    /// Add count using conservative update
    ///
    /// Conservative update improves accuracy by only incrementing counters
    /// up to the new estimated value. This reduces over-counting.
    pub fn add_conservative(&mut self, item: &[u8], count: u64) {
        self.num_updates += 1;
        self.total_count += count;

        // First pass: find current estimate (minimum)
        let min_val = self.estimate(item);
        let new_val = min_val.saturating_add(count);

        // Second pass: set all counters to at least new_val
        for (row, &seed) in self.seeds.iter().enumerate() {
            let hash = xxh3_64_with_seed(item, seed);
            let col = (hash as usize) % self.width;

            if self.table[row][col] < new_val {
                self.table[row][col] = new_val;
            }
        }
    }

    /// Estimate the frequency of an item
    pub fn estimate(&self, item: &[u8]) -> u64 {
        let mut min_count = u64::MAX;

        for (row, &seed) in self.seeds.iter().enumerate() {
            let hash = xxh3_64_with_seed(item, seed);
            let col = (hash as usize) % self.width;
            min_count = min_count.min(self.table[row][col]);
        }

        min_count
    }

    /// Inner product of two sketches
    ///
    /// This can be used to estimate the dot product of two frequency distributions.
    pub fn inner_product(&self, other: &Self) -> Option<u64> {
        if self.width != other.width || self.depth != other.depth {
            return None;
        }

        let mut min_product = u64::MAX;

        for row in 0..self.depth {
            let product: u64 = self.table[row]
                .iter()
                .zip(other.table[row].iter())
                .fold(0u64, |acc, (&a, &b)| {
                    acc.saturating_add(a.saturating_mul(b))
                });
            min_product = min_product.min(product);
        }

        Some(min_product)
    }

    /// Theoretical error bound (epsilon * total_count)
    pub fn error_bound(&self) -> u64 {
        let epsilon = core::f64::consts::E / self.width as f64;
        (epsilon * self.total_count as f64) as u64
    }
}

impl Sketch for CountMinSketch {
    type Item = [u8];

    fn update(&mut self, item: &[u8]) {
        self.add(item, 1);
    }

    fn merge(&mut self, other: &Self) -> Result<(), MergeError> {
        if self.width != other.width || self.depth != other.depth {
            return Err(MergeError::IncompatibleConfig {
                expected: format!("{}x{}", self.width, self.depth),
                found: format!("{}x{}", other.width, other.depth),
            });
        }

        for row in 0..self.depth {
            for col in 0..self.width {
                self.table[row][col] = self.table[row][col].saturating_add(other.table[row][col]);
            }
        }

        self.total_count += other.total_count;
        self.num_updates += other.num_updates;

        Ok(())
    }

    fn clear(&mut self) {
        for row in &mut self.table {
            row.fill(0);
        }
        self.total_count = 0;
        self.num_updates = 0;
    }

    fn size_bytes(&self) -> usize {
        core::mem::size_of::<Self>()
            + self.depth * self.width * core::mem::size_of::<u64>()
            + self.seeds.len() * core::mem::size_of::<u64>()
    }

    fn count(&self) -> u64 {
        self.num_updates
    }
}


impl FrequencySketch for CountMinSketch {
    fn estimate_frequency(&self, item: &[u8]) -> u64 {
        self.estimate(item)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for CountMinSketch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("CountMinSketch", 6)?;
        state.serialize_field("width", &self.width)?;
        state.serialize_field("depth", &self.depth)?;
        state.serialize_field("table", &self.table)?;
        state.serialize_field("total_count", &self.total_count)?;
        state.serialize_field("num_updates", &self.num_updates)?;
        state.serialize_field("seeds", &self.seeds)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let mut cms = CountMinSketch::new(0.01, 0.01);

        cms.add(b"apple", 5);
        cms.add(b"banana", 3);
        cms.add(b"cherry", 1);
        cms.add(b"apple", 2);

        // Estimates should be at least the true count
        assert!(cms.estimate(b"apple") >= 7);
        assert!(cms.estimate(b"banana") >= 3);
        assert!(cms.estimate(b"cherry") >= 1);
    }

    #[test]
    fn test_empty() {
        let cms = CountMinSketch::new(0.01, 0.01);
        assert_eq!(cms.estimate(b"anything"), 0);
        assert_eq!(cms.total_count(), 0);
    }

    #[test]
    fn test_conservative_update() {
        let mut cms1 = CountMinSketch::new(0.001, 0.001);
        let mut cms2 = CountMinSketch::new(0.001, 0.001);

        // Add many items
        for i in 0..10000 {
            let item = format!("item_{}", i);
            cms1.add(item.as_bytes(), 1);
            cms2.add_conservative(item.as_bytes(), 1);
        }

        // Conservative update should generally have lower estimates for items we query
        // (fewer collisions impacting the count)
        let test_item = b"test_item";
        cms1.add(test_item, 100);
        cms2.add_conservative(test_item, 100);

        // Both should report at least 100
        assert!(cms1.estimate(test_item) >= 100);
        assert!(cms2.estimate(test_item) >= 100);
    }

    #[test]
    fn test_merge() {
        let mut cms1 = CountMinSketch::with_dimensions(1000, 5);
        let mut cms2 = CountMinSketch::with_dimensions(1000, 5);

        cms1.add(b"apple", 5);
        cms2.add(b"banana", 3);

        cms1.merge(&cms2).unwrap();

        assert!(cms1.estimate(b"apple") >= 5);
        assert!(cms1.estimate(b"banana") >= 3);
        assert_eq!(cms1.total_count(), 8);
    }

    #[test]
    fn test_merge_incompatible() {
        let mut cms1 = CountMinSketch::with_dimensions(1000, 5);
        let cms2 = CountMinSketch::with_dimensions(2000, 5);

        assert!(cms1.merge(&cms2).is_err());
    }

    #[test]
    fn test_dimensions() {
        let cms = CountMinSketch::with_dimensions(1000, 5);
        assert_eq!(cms.width(), 1000);
        assert_eq!(cms.depth(), 5);
    }

    #[test]
    fn test_clear() {
        let mut cms = CountMinSketch::new(0.01, 0.01);

        cms.add(b"item", 100);
        assert!(cms.estimate(b"item") >= 100);

        cms.clear();
        assert_eq!(cms.estimate(b"item"), 0);
        assert_eq!(cms.total_count(), 0);
    }

    #[test]
    fn test_heavy_usage() {
        let mut cms = CountMinSketch::new(0.01, 0.001);

        // Add 100,000 items
        for i in 0..100_000 {
            let item = format!("user_{}", i % 1000); // 1000 unique items
            cms.add(item.as_bytes(), 1);
        }

        // Each item should have been added ~100 times
        // With 1% error, we allow some slack
        for i in 0..10 {
            let item = format!("user_{}", i);
            let estimate = cms.estimate(item.as_bytes());
            // Should be at least 100, and not more than 100 + error_bound
            assert!(estimate >= 100, "item {} estimate {} < 100", i, estimate);
        }
    }

    #[test]
    fn test_error_bound() {
        let cms = CountMinSketch::new(0.01, 0.001);
        // Error bound formula: epsilon * total_count
        // Initially 0
        assert_eq!(cms.error_bound(), 0);
    }

    #[test]
    fn test_inner_product() {
        let mut cms1 = CountMinSketch::with_dimensions(1000, 5);
        let mut cms2 = CountMinSketch::with_dimensions(1000, 5);

        cms1.add(b"a", 10);
        cms1.add(b"b", 5);

        cms2.add(b"a", 3);
        cms2.add(b"b", 2);

        // Inner product should give approximation of sum(f1[i] * f2[i])
        // = 10*3 + 5*2 = 40
        let ip = cms1.inner_product(&cms2).unwrap();
        assert!(ip >= 40, "inner product {} < 40", ip);
    }
}
