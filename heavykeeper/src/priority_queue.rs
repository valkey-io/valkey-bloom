use ahash::RandomState;
use hashbrown::HashTable;
use std::borrow::Borrow;
use std::hash::Hash;

use crate::cuckoo::{realloc_large_heap_allocated_object, Reallocator};

/// Relocate `vec`'s backing allocation through `reallocator`, in place. Trimmed
/// to a boxed slice first to drop spare capacity; the rebuilt `Vec` has
/// capacity equal to its length.
fn realloc_vec<E, R: Reallocator>(vec: &mut Vec<E>, reallocator: &mut R) {
    let mut boxed = std::mem::take(vec).into_boxed_slice();
    realloc_large_heap_allocated_object(&mut boxed, reallocator);
    *vec = boxed.into_vec();
}

#[derive(Clone)]
struct Slot<T> {
    item: T,
    count: u64,
    sequence: u32,
    heap_pos: u32,
}

/// A specialized priority queue for HeavyKeeper that maintains top-k items by count
///
/// - `linear = true` (default): linear scan over `item_store`. Better cache
///   locality for small `k` and avoids hashing on the lookup path; the hash
///   table is left unallocated.
/// - `linear = false`: a `hashbrown` hash table maps items to slot indices for
///   O(1) lookup, at the cost of the table's memory and per-op hashing.
#[derive(Clone)]
pub(crate) struct TopKQueue<T> {
    item_store: Vec<Slot<T>>,
    heap: Vec<u32>,        // slot indices, min-heap ordered by count
    table: HashTable<u32>, // hash -> slot index into `item_store` (unused when `linear`)
    linear: bool,
    capacity: usize,
    sequence: u32,
    hasher: RandomState,
}

impl<T: Ord + Clone + Hash + PartialEq> TopKQueue<T> {
    /// Build a queue using the hash-table lookup strategy.
    pub(crate) fn with_capacity_and_hasher(capacity: usize, hasher: RandomState) -> Self {
        Self::with_capacity_hasher_linear(capacity, hasher, false)
    }

    /// Build a queue, choosing the lookup strategy: `linear` scans `item_store`
    /// and leaves the hash table unallocated; otherwise the hash table is used.
    pub(crate) fn with_capacity_hasher_linear(
        capacity: usize,
        hasher: RandomState,
        linear: bool,
    ) -> Self {
        Self {
            item_store: Vec::with_capacity(capacity),
            heap: Vec::with_capacity(capacity + 1),
            // Linear lookup never touches the table; leave it unallocated.
            table: if linear {
                HashTable::new()
            } else {
                HashTable::with_capacity(capacity)
            },
            linear,
            capacity,
            sequence: 0,
            hasher,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_hasher_linear(capacity, RandomState::new(), true)
    }

    pub(crate) fn len(&self) -> usize {
        self.item_store.len()
    }

    /// Returns the heap memory (in bytes) used by this queue's containers.
    ///
    /// Computed from the allocated *capacity* of the slots, heap vector,
    /// and hash table, plus the heap each live item owns beyond
    /// its inline `size_of::<T>()`. `item_heap(t)` should return the bytes `t`
    /// points to (e.g. `String::capacity`).
    pub(crate) fn mem_bytes<F>(&self, item_heap: F) -> usize
    where
        F: Fn(&T) -> usize,
    {
        use std::mem::size_of;
        let store_bytes = self.item_store.capacity() * size_of::<Slot<T>>();
        let heap_bytes = self.heap.capacity() * size_of::<u32>();
        // hashbrown internals: `buckets` is the next power of two >= ceil(capacity*8/7).
        let buckets = {
            let cap = self.table.capacity();
            if cap == 0 {
                0
            } else {
                ((cap * 8 + 6) / 7).next_power_of_two()
            }
        };
        #[cfg(all(
            target_feature = "sse2",
            any(target_arch = "x86", target_arch = "x86_64")
        ))]
        const GROUP_WIDTH: usize = 16;
        #[cfg(not(all(
            target_feature = "sse2",
            any(target_arch = "x86", target_arch = "x86_64")
        )))]
        const GROUP_WIDTH: usize = 8;
        let table_bytes = if buckets == 0 {
            0
        } else {
            buckets * (size_of::<u32>() + 1) + GROUP_WIDTH
        };
        let item_bytes: usize = self.item_store.iter().map(|s| item_heap(&s.item)).sum();
        store_bytes + heap_bytes + table_bytes + item_bytes
    }

    /// Relocate the `heap` and `item_store` vectors through `reallocator`. For
    /// `item_store` only the outer buffer moves; any heap a `T` owns (e.g. a
    /// `Vec<u8>` key's bytes) stays put, as elements are copied byte-for-byte.
    /// The hash table owns its own allocation and is not relocated.
    pub(crate) fn realloc_large_heap_allocated_objects<R: Reallocator>(
        &mut self,
        reallocator: &mut R,
    ) {
        realloc_vec(&mut self.heap, reallocator);
        realloc_vec(&mut self.item_store, reallocator);
    }

    pub(crate) fn get<Q>(&self, item: &Q) -> Option<u64>
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        self.find_slot(item).map(|idx| self.item_store[idx].count)
    }

    pub(crate) fn contains<Q>(&self, item: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find_slot(item).is_some()
    }

    /// Increase an existing entry's count. Caller must guarantee the new count
    /// is >= the current count (paper Algorithm 1: heap is max(maxv, existing)).
    pub(crate) fn update_if_present<Q>(&mut self, item: &Q, count: u64) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(slot_idx) = self.find_slot(item) {
            let slot = &mut self.item_store[slot_idx];
            debug_assert!(count >= slot.count, "update_if_present must not decrease");
            if count == slot.count {
                return true;
            }
            slot.count = count;
            let pos = slot.heap_pos as usize;
            self.sift_down(pos);
            true
        } else {
            false
        }
    }

    pub(crate) fn min_count(&self) -> u64 {
        // If heap is empty, return 0
        // Otherwise return count from root node (index 0)
        if self.item_store.is_empty() {
            0
        } else {
            self.item_store[self.heap[0] as usize].count
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        self.item_store.len() >= self.capacity
    }

    /// Insert or update `item` to `count`.
    ///
    /// Returns `Some(evicted)` when a previously tracked item is displaced
    /// by this call, otherwise `None`.
    pub(crate) fn upsert(&mut self, item: T, count: u64) -> Option<T> {
        let hash = if self.linear {
            0
        } else {
            self.hasher.hash_one(&item)
        };
        // Fast path: update existing item
        let existing = if self.linear {
            self.find_slot_linear(&item)
        } else {
            self.find_slot_with_hash(&item, hash)
        };
        if let Some(slot_idx) = existing {
            let slot = &mut self.item_store[slot_idx];
            if count == slot.count {
                return None;
            }
            slot.count = count;
            let pos = slot.heap_pos as usize;
            self.sift_down(pos);
            self.sift_up(pos);
            return None;
        }

        // For new items, if we have space just add it
        if self.item_store.len() < self.capacity {
            // Restore capacity to k after a defrag trimmed it, so it stays a
            // known constant for memory tracking.
            if self.heap.capacity() < self.capacity + 1 {
                self.heap.reserve_exact(self.capacity + 1 - self.heap.len());
            }
            if self.item_store.capacity() < self.capacity {
                self.item_store
                    .reserve_exact(self.capacity - self.item_store.len());
            }

            let slot_idx = self.item_store.len() as u32;
            let heap_pos = slot_idx;
            self.sequence = self.sequence.wrapping_add(1);

            self.item_store.push(Slot {
                item,
                count,
                sequence: self.sequence,
                heap_pos,
            });
            self.heap.push(slot_idx);

            if !self.linear {
                self.table.insert_unique(hash, slot_idx, |&idx| {
                    self.hasher.hash_one(&self.item_store[idx as usize].item)
                });
            }
            self.sift_up(heap_pos as usize);
            return None;
        }

        // Queue is full - check if new count beats minimum
        if !self.item_store.is_empty() {
            let min_slot_idx = self.heap[0] as usize;
            let min_count = self.item_store[min_slot_idx].count;
            if count > min_count {
                if !self.linear {
                    let old_hash = self.hasher.hash_one(&self.item_store[min_slot_idx].item);
                    if let Ok(entry) = self
                        .table
                        .find_entry(old_hash, |&idx| idx == min_slot_idx as u32)
                    {
                        entry.remove();
                    }
                }

                let old_item =
                    std::mem::replace(&mut self.item_store[min_slot_idx].item, item);
                self.item_store[min_slot_idx].count = count;
                self.sequence = self.sequence.wrapping_add(1);
                self.item_store[min_slot_idx].sequence = self.sequence;

                if !self.linear {
                    self.table.insert_unique(hash, min_slot_idx as u32, |&idx| {
                        self.hasher.hash_one(&self.item_store[idx as usize].item)
                    });
                }
                self.sift_down(0);
                return Some(old_item);
            }
        }
        None
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&T, u64)> {
        let mut items: Vec<_> = self
            .item_store
            .iter()
            .map(|s| (&s.item, s.count, s.sequence))
            .collect();

        // Sort by count descending, then by sequence ascending.
        items.sort_unstable_by(|(_, c1, s1), (_, c2, s2)| match c2.cmp(c1) {
            std::cmp::Ordering::Equal => s1.cmp(s2),
            other => other,
        });

        // Return an iterator over (&T, count), preserving sorted order.
        items.into_iter().map(|(k, count, _)| (k, count))
    }

    /// Iterate items in ascending insertion-`sequence` order.
    ///
    /// Serialization uses this so restore (re-`upsert` in this order) reassigns
    /// sequences that preserve the count-tie ordering.
    pub(crate) fn iter_by_sequence(&self) -> impl Iterator<Item = (&T, u64)> {
        let mut items: Vec<_> = self
            .item_store
            .iter()
            .map(|s| (&s.item, s.count, s.sequence))
            .collect();
        items.sort_unstable_by_key(|(_, _, seq)| *seq);
        items.into_iter().map(|(k, count, _)| (k, count))
    }

    fn find_slot<Q>(&self, item: &Q) -> Option<usize>
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if self.linear {
            return self.find_slot_linear(item);
        }
        let hash = self.hasher.hash_one(item);
        self.find_slot_with_hash(item, hash)
    }

    /// Linear scan over `item_store`. Used when `linear` is set.
    #[inline]
    fn find_slot_linear<Q>(&self, item: &Q) -> Option<usize>
    where
        T: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.item_store
            .iter()
            .position(|s| s.item.borrow() == item)
    }

    #[inline]
    fn find_slot_with_hash<Q>(&self, item: &Q, hash: u64) -> Option<usize>
    where
        T: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.table
            .find(hash, |&idx| {
                self.item_store[idx as usize].item.borrow() == item
            })
            .map(|&idx| idx as usize)
    }

    // Binary heap helper methods using Eytzinger layout (0-based indexing)
    fn parent(i: usize) -> usize {
        (i - 1) >> 1
    }
    fn left(i: usize) -> usize {
        2 * i + 1
    }
    fn right(i: usize) -> usize {
        2 * i + 2
    }

    fn sift_up(&mut self, mut pos: usize) {
        while pos > 0 {
            let parent = Self::parent(pos);
            if self.item_store[self.heap[parent] as usize].count
                > self.item_store[self.heap[pos] as usize].count
            {
                self.swap_nodes(parent, pos);
                pos = parent;
            } else {
                break;
            }
        }
    }

    fn sift_down(&mut self, mut pos: usize) {
        loop {
            let mut smallest = pos;
            let left = Self::left(pos);
            let right = Self::right(pos);

            if left < self.heap.len()
                && self.item_store[self.heap[left] as usize].count
                    < self.item_store[self.heap[smallest] as usize].count
            {
                smallest = left;
            }
            if right < self.heap.len()
                && self.item_store[self.heap[right] as usize].count
                    < self.item_store[self.heap[smallest] as usize].count
            {
                smallest = right;
            }

            if smallest == pos {
                break;
            }

            self.swap_nodes(pos, smallest);
            pos = smallest;
        }
    }

    fn swap_nodes(&mut self, i: usize, j: usize) {
        self.heap.swap(i, j);
        // Update heap positions in item_store
        self.item_store[self.heap[i] as usize].heap_pos = i as u32;
        self.item_store[self.heap[j] as usize].heap_pos = j as u32;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insertion() {
        let mut queue = TopKQueue::with_capacity(2);
        queue.upsert("a", 1);
        queue.upsert("b", 2);

        let items: Vec<_> = queue.iter().collect();
        assert_eq!(items, vec![(&"b", 2), (&"a", 1)]);
    }

    #[test]
    fn test_update_existing() {
        let mut queue = TopKQueue::with_capacity_and_hasher(2, RandomState::new());
        queue.upsert("a", 1);
        queue.upsert("b", 2);
        queue.upsert("a", 3); // Update a's count

        let items: Vec<_> = queue.iter().collect();
        assert_eq!(items, vec![(&"a", 3), (&"b", 2)]);
    }

    #[test]
    fn test_heap_cleanup() {
        let mut queue = TopKQueue::with_capacity_and_hasher(2, RandomState::new());

        // Insert initial items
        queue.upsert("a", 1);
        queue.upsert("b", 2);

        // Update 'a' multiple times
        queue.upsert("a", 3);
        queue.upsert("a", 4);
        queue.upsert("a", 5);

        // Insert new item with higher count
        queue.upsert("c", 6);

        // Check heap size vs items size
        assert_eq!(queue.heap.len(), 2, "Expected 2 items");

        let items: Vec<_> = queue.iter().collect();
        assert_eq!(items, vec![(&"c", 6), (&"a", 5)]);
    }

    #[test]
    fn test_insertion_order() {
        let mut queue = TopKQueue::with_capacity_and_hasher(3, RandomState::new());

        // Insert items with same count in specific order
        queue.upsert("a", 1);
        queue.upsert("b", 1);
        queue.upsert("c", 1);

        let items: Vec<_> = queue.iter().collect();
        assert_eq!(items, vec![(&"a", 1), (&"b", 1), (&"c", 1)]);
    }

    #[test]
    fn test_heap_consistency() {
        let mut queue = TopKQueue::with_capacity_and_hasher(2, RandomState::new());

        // Fill queue
        queue.upsert("a", 1);
        queue.upsert("b", 2);

        // Update existing item multiple times
        for i in 3..10 {
            queue.upsert("a", i);
        }

        // Try to insert new item
        queue.upsert("c", 5);

        // Verify min_count is accurate
        assert_eq!(queue.min_count(), 5);
    }

    #[test]
    fn test_capacity_overflow() {
        let mut queue = TopKQueue::with_capacity_and_hasher(2, RandomState::new());

        // Insert more items than capacity
        queue.upsert("a", 1);
        queue.upsert("b", 2);
        queue.upsert("c", 3);
        queue.upsert("d", 4);
        queue.upsert("e", 5);

        assert_eq!(queue.len(), 2, "Queue should maintain capacity");

        let items: Vec<_> = queue.iter().collect();
        assert_eq!(items, vec![(&"e", 5), (&"d", 4)]);
    }

    #[test]
    fn test_repeated_updates() {
        let mut queue = TopKQueue::with_capacity_and_hasher(2, RandomState::new());

        // Insert and update same item repeatedly
        for i in 1..100 {
            queue.upsert("a", i);
        }

        queue.upsert("b", 50);

        assert_eq!(queue.len(), 2);

        let items: Vec<_> = queue.iter().collect();
        assert_eq!(items, vec![(&"a", 99), (&"b", 50)]);
    }

    #[test]
    fn test_heap_property() {
        let mut queue = TopKQueue::with_capacity_and_hasher(10, RandomState::new());

        // Insert in reverse order to test heap maintenance
        for i in (0..=10).rev() {
            queue.upsert(format!("item{}", i), i as u64);
        }

        // Verify heap property: parent should be <= children for min-heap
        for i in 1..queue.heap.len() {
            let parent_idx = TopKQueue::<String>::parent(i);
            if parent_idx > 0 {
                // Skip root's parent
                let parent_count = queue.item_store[queue.heap[parent_idx] as usize].count;
                let child_count = queue.item_store[queue.heap[i] as usize].count;
                assert!(
                    parent_count <= child_count,
                    "Heap property violated: parent count {} at index {} is greater than child count {} at index {}",
                    parent_count,
                    parent_idx,
                    child_count,
                    i
                );
            }
        }

        // Verify items are stored in descending order (highest counts first)
        let items: Vec<_> = queue.iter().collect();
        for i in 0..items.len() - 1 {
            assert!(
                items[i].1 >= items[i + 1].1,
                "Items not properly ordered by count: {} before {}",
                items[i].1,
                items[i + 1].1
            );
        }
    }
}
