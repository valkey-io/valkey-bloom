// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 flowstats Contributors
// SPDX-FileContributor: https://github.com/vnvo/flowstats/blob/v0.1.2/src/frequency/space_saving.rs

//! Space-Saving algorithm for heavy hitters
//!
//! The Space-Saving algorithm efficiently tracks the k most frequent items
//! in a data stream using only O(k) space.
//!
//! **Note**: This module requires the `std` feature. The Space-Saving algorithm
//! does not support correct distributed merging due to its replacement strategy.
//! Attempting to merge will return an error.

use crate::traits::{FrequencySketch, MergeError, Sketch};
use core::hash::Hash;
use std::collections::HashMap;
use std::vec::Vec;

/// Entry in the Space-Saving structure
#[derive(Clone, Debug)]
struct Counter<T> {
    /// The item
    item: T,
    /// Estimated count
    count: u64,
    /// Error bound (maximum overcount)
    error: u64,
}

impl<T> Counter<T> {
    fn new(item: T, count: u64, error: u64) -> Self {
        Self { item, count, error }
    }
}

/// Space-Saving algorithm for finding frequent items
///
/// The Space-Saving algorithm maintains a summary of the k most frequent items
/// with the following guarantees:
///
/// - Any item with true frequency > n/k is guaranteed to be in the summary
/// - The maximum overcount error for any item is at most n/k
///
/// # Example
///
/// ```
/// use flowstats::frequency::SpaceSaving;
/// use flowstats::traits::HeavyHitters;
///
/// let mut ss = SpaceSaving::new(10); // Track top 10
///
/// // Add some items
/// for _ in 0..100 { ss.add("apple"); }
/// for _ in 0..50 { ss.add("banana"); }
/// for _ in 0..25 { ss.add("cherry"); }
/// for _ in 0..10 { ss.add("date"); }
///
/// // Get top 3 items
/// let top = ss.top_k(3);
/// println!("Top items: {:?}", top);
/// ```
#[derive(Clone, Debug)]
pub struct SpaceSaving<T: Hash + Eq + Clone + core::fmt::Debug> {
    /// Maximum number of counters to maintain
    capacity: usize,
    /// Map from item to counter index
    item_to_index: HashMap<T, usize>,
    /// Counter array
    counters: Vec<Counter<T>>,
    /// Total count of all items
    total_count: u64,
    /// Number of updates
    num_updates: u64,
}

impl<T: Hash + Eq + Clone + core::fmt::Debug> SpaceSaving<T> {
    /// Create a new Space-Saving structure with the given capacity
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of items to track (k)
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");

        Self {
            capacity,
            item_to_index: HashMap::with_capacity(capacity),
            counters: Vec::with_capacity(capacity),
            total_count: 0,
            num_updates: 0,
        }
    }

    /// Get the capacity (k)
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the number of distinct items currently tracked
    pub fn num_tracked(&self) -> usize {
        self.counters.len()
    }

    /// Get the total count
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Add an item to the structure
    pub fn add(&mut self, item: T) {
        self.add_count(item, 1);
    }

    /// Add an item with a specific count
    pub fn add_count(&mut self, item: T, count: u64) {
        self.num_updates += 1;
        self.total_count += count;

        // Check if item is already tracked
        if let Some(&idx) = self.item_to_index.get(&item) {
            self.counters[idx].count += count;
            return;
        }

        // Not tracked - either add new or replace minimum
        if self.counters.len() < self.capacity {
            // Still have room, add new counter
            let idx = self.counters.len();
            self.counters.push(Counter::new(item.clone(), count, 0));
            self.item_to_index.insert(item, idx);
        } else {
            // Find and replace minimum counter
            let min_idx = self.find_min_index();
            let min_count = self.counters[min_idx].count;

            // Remove old item from map
            let old_item = self.counters[min_idx].item.clone();
            self.item_to_index.remove(&old_item);

            // Replace with new item
            self.counters[min_idx] = Counter::new(item.clone(), min_count + count, min_count);
            self.item_to_index.insert(item, min_idx);
        }
    }

    /// Find the index of the counter with minimum count
    fn find_min_index(&self) -> usize {
        self.counters
            .iter()
            .enumerate()
            .min_by_key(|(_, c)| c.count)
            .map(|(i, _)| i)
            .unwrap_or(0)
    }

    /// Estimate the frequency of an item
    pub fn estimate(&self, item: &T) -> u64 {
        self.item_to_index
            .get(item)
            .map(|&idx| self.counters[idx].count)
            .unwrap_or(0)
    }

    /// Get the error bound for an item's estimate
    pub fn error(&self, item: &T) -> u64 {
        self.item_to_index
            .get(item)
            .map(|&idx| self.counters[idx].error)
            .unwrap_or(0)
    }

    /// Get guaranteed minimum count for an item
    ///
    /// Returns (count - error), which is guaranteed to be at most the true count.
    pub fn guaranteed_count(&self, item: &T) -> u64 {
        self.item_to_index
            .get(item)
            .map(|&idx| {
                let c = &self.counters[idx];
                c.count.saturating_sub(c.error)
            })
            .unwrap_or(0)
    }

    /// Check if an item is currently tracked
    pub fn contains(&self, item: &T) -> bool {
        self.item_to_index.contains_key(item)
    }
}

impl<T: Hash + Eq + Clone + core::fmt::Debug> Sketch for SpaceSaving<T> {
    type Item = T;

    fn update(&mut self, item: &T) {
        self.add(item.clone());
    }

    fn merge(&mut self, _other: &Self) -> Result<(), MergeError> {
        // Space-Saving does not support correct distributed merge.
        // The algorithm's replacement strategy makes merging two summaries
        // produce incorrect results (inflated counts, dropped heavy hitters).
        // Use a single SpaceSaving instance or aggregate raw data instead.
        Err(MergeError::IncompatibleConfig {
            expected: "Space-Saving does not support merge".into(),
            found: "merge attempted".into(),
        })
    }

    fn clear(&mut self) {
        self.item_to_index.clear();
        self.counters.clear();
        self.total_count = 0;
        self.num_updates = 0;
    }

    fn size_bytes(&self) -> usize {
        core::mem::size_of::<Self>() + self.counters.capacity() * core::mem::size_of::<Counter<T>>()
    }

    fn count(&self) -> u64 {
        self.num_updates
    }
}

impl<T: Hash + Eq + Clone + core::fmt::Debug> FrequencySketch for SpaceSaving<T> {
    fn estimate_frequency(&self, item: &T) -> u64 {
        self.estimate(item)
    }
}

#[cfg(feature = "serde")]
impl<T: Hash + Eq + Clone + core::fmt::Debug + serde::Serialize> serde::Serialize
    for SpaceSaving<T>
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let items: Vec<_> = self
            .counters
            .iter()
            .map(|c| (&c.item, c.count, c.error))
            .collect();

        let mut state = serializer.serialize_struct("SpaceSaving", 3)?;
        state.serialize_field("capacity", &self.capacity)?;
        state.serialize_field("total_count", &self.total_count)?;
        state.serialize_field("items", &items)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let mut ss = SpaceSaving::<String>::new(10);

        ss.add("apple".to_string());
        ss.add("apple".to_string());
        ss.add("banana".to_string());

        assert!(ss.estimate(&"apple".to_string()) >= 2);
        assert!(ss.estimate(&"banana".to_string()) >= 1);
    }

    #[test]
    fn test_empty() {
        let ss = SpaceSaving::<String>::new(10);
        assert_eq!(ss.estimate(&"anything".to_string()), 0);
        assert_eq!(ss.total_count(), 0);
    }

    #[test]
    fn test_top_k() {
        let mut ss = SpaceSaving::<&str>::new(10);

        for _ in 0..100 {
            ss.add("apple");
        }
        for _ in 0..50 {
            ss.add("banana");
        }
        for _ in 0..25 {
            ss.add("cherry");
        }

        let top = ss.top_k(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "apple");
        assert_eq!(top[1].0, "banana");
    }

    #[test]
    fn test_heavy_hitters() {
        let mut ss = SpaceSaving::<&str>::new(10);

        for _ in 0..100 {
            ss.add("apple");
        }
        for _ in 0..10 {
            ss.add("banana");
        }
        for _ in 0..1 {
            ss.add("cherry");
        }

        // Items with > 5% of total
        let heavy = ss.heavy_hitters(0.05);
        assert!(heavy.iter().any(|(item, _)| *item == "apple"));
        assert!(heavy.iter().any(|(item, _)| *item == "banana"));
        // Cherry should not be included (1/111 < 5%)
    }

    #[test]
    fn test_replacement() {
        let mut ss = SpaceSaving::<i32>::new(3);

        // Fill up capacity
        ss.add(1);
        ss.add(2);
        ss.add(3);

        assert_eq!(ss.num_tracked(), 3);

        // Add a new item - should replace the minimum
        ss.add(4);

        // Still only 3 items tracked
        assert_eq!(ss.num_tracked(), 3);
    }

    #[test]
    fn test_contains() {
        let mut ss = SpaceSaving::<&str>::new(10);

        ss.add("apple");

        assert!(ss.contains(&"apple"));
        assert!(!ss.contains(&"banana"));
    }

    #[test]
    fn test_merge_not_supported() {
        let mut ss1 = SpaceSaving::<&str>::new(10);
        let ss2 = SpaceSaving::<&str>::new(10);

        // Space-Saving does not support merge
        assert!(ss1.merge(&ss2).is_err());
    }

    #[test]
    fn test_guaranteed_count() {
        let mut ss = SpaceSaving::<&str>::new(3);

        // Add items that will cause replacements
        ss.add("a");
        ss.add("b");
        ss.add("c");

        // Add more to force replacement
        ss.add("d");

        // Check that guaranteed count is <= estimated count
        for item in ["a", "b", "c", "d"] {
            let est = ss.estimate(&item);
            let guar = ss.guaranteed_count(&item);
            assert!(
                guar <= est,
                "guaranteed {} > estimate {} for {}",
                guar,
                est,
                item
            );
        }
    }

    #[test]
    fn test_zipf_distribution() {
        // Simulate Zipf distribution (common in real data)
        let mut ss = SpaceSaving::<i32>::new(10);

        // Item 1 appears 1000 times, item 2 appears 500 times, etc.
        for rank in 1..=100 {
            let count = 1000 / rank;
            for _ in 0..count {
                ss.add(rank);
            }
        }

        // Top items should be 1, 2, 3, ...
        let top = ss.top_k(5);
        assert!(!top.is_empty());
        // Item 1 should definitely be tracked
        assert!(ss.contains(&1));
    }

    #[test]
    fn test_clear() {
        let mut ss = SpaceSaving::<&str>::new(10);

        ss.add("apple");
        ss.add("banana");

        ss.clear();

        assert_eq!(ss.num_tracked(), 0);
        assert_eq!(ss.total_count(), 0);
        assert!(!ss.contains(&"apple"));
    }
}
