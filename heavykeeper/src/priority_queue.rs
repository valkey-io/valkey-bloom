use crate::sip::SipState;
use crate::traits::Counter;
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
struct Slot<T, C> {
    item: T,
    count: C,
    sequence: u64,
    heap_pos: u32,
}

/// A specialized priority queue for HeavyKeeper that maintains top-k items by count
///
/// Items live in `item_store`; a `hashbrown` hash table maps each item to its
/// slot index for O(1) lookup on every add.
///
/// `C` is the stored counter width. The public API speaks `u64`; values are
/// saturated into `C` on the way in. A sketch whose cells are `u32` never
/// produces a count above `u32::MAX`, so storing `u64` here would waste four
/// bytes per slot.
#[derive(Clone)]
pub(crate) struct TopKQueue<T, C: Counter = u64> {
    item_store: Vec<Slot<T, C>>,
    heap: Vec<u32>,        // slot indices, min-heap ordered by count
    table: HashTable<u32>, // hash -> slot index into `item_store`
    capacity: usize,
    /// Monotonic insertion counter used to break count ties (older wins).
    sequence: u64,
    hasher: SipState,
}

impl<T: Ord + Clone + Hash + PartialEq, C: Counter> TopKQueue<T, C> {
    pub(crate) fn with_capacity_and_hasher(capacity: usize, hasher: SipState) -> Self {
        Self {
            item_store: Vec::with_capacity(capacity),
            heap: Vec::with_capacity(capacity),
            table: HashTable::with_capacity(capacity),
            capacity,
            sequence: 0,
            hasher,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, SipState::random())
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
        let store_bytes = self.item_store.capacity() * size_of::<Slot<T, C>>();
        let heap_bytes = self.heap.capacity() * size_of::<u32>();
        // hashbrown reports its own allocation, so the estimate cannot drift
        // from the crate's internal layout across upgrades.
        let table_bytes = self.table.allocation_size();
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
        Q: Hash + Eq + ?Sized,
    {
        self.find_slot(item)
            .map(|idx| self.item_store[idx].count.as_u64())
    }

    pub(crate) fn contains<Q>(&self, item: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find_slot(item).is_some()
    }

    /// Update an existing entry's count to `count`, if `item` is tracked.
    ///
    /// Returns `true` if `item` is present (whether or not this call
    /// actually raised its count), `false` if it isn't tracked at all —
    #[inline]
    pub(crate) fn update_if_present<Q>(&mut self, item: &Q, count: u64) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let count = C::from_u64(count);
        if let Some(slot_idx) = self.find_slot(item) {
            let slot = &mut self.item_store[slot_idx];
            // The count-min-sketch estimate can fall below the value already
            // tracked (a later add where every row decayed yields a smaller
            // max_count). The PQ keeps the high-water estimate, so a
            // non-increasing update is a no-op, not an error.
            if count <= slot.count {
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
            self.item_store[self.heap[0] as usize].count.as_u64()
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        self.item_store.len() >= self.capacity
    }

    /// Insert or update `item` to `count`.
    ///
    /// `count` is saturated into the stored width `C`.
    ///
    /// Returns `Some(evicted)` when a previously tracked item is displaced
    /// by this call, otherwise `None`.
    pub(crate) fn upsert(&mut self, item: T, count: u64) -> Option<T> {
        let count = C::from_u64(count);
        let hash = self.hasher.hash_one(&item);
        // Fast path: update existing item
        if let Some(slot_idx) = self.find_slot_with_hash(&item, hash) {
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
            if self.heap.capacity() < self.capacity {
                self.heap.reserve_exact(self.capacity - self.heap.len());
            }
            if self.item_store.capacity() < self.capacity {
                self.item_store
                    .reserve_exact(self.capacity - self.item_store.len());
            }

            let slot_idx = self.item_store.len() as u32;
            let heap_pos = slot_idx;
            self.sequence += 1;

            self.item_store.push(Slot {
                item,
                count,
                sequence: self.sequence,
                heap_pos,
            });
            self.heap.push(slot_idx);

            self.table.insert_unique(hash, slot_idx, |&idx| {
                self.hasher.hash_one(&self.item_store[idx as usize].item)
            });
            self.sift_up(heap_pos as usize);
            return None;
        }

        // Queue is full - check if new count beats minimum
        if !self.item_store.is_empty() {
            let min_slot_idx = self.heap[0] as usize;
            let min_count = self.item_store[min_slot_idx].count;
            if count > min_count {
                let old_hash = self.hasher.hash_one(&self.item_store[min_slot_idx].item);
                if let Ok(entry) = self
                    .table
                    .find_entry(old_hash, |&idx| idx == min_slot_idx as u32)
                {
                    entry.remove();
                }

                let old_item = std::mem::replace(&mut self.item_store[min_slot_idx].item, item);
                self.item_store[min_slot_idx].count = count;
                self.sequence += 1;
                self.item_store[min_slot_idx].sequence = self.sequence;

                self.table.insert_unique(hash, min_slot_idx as u32, |&idx| {
                    self.hasher.hash_one(&self.item_store[idx as usize].item)
                });
                self.compact_table_if_grown();
                self.sift_down(0);
                return Some(old_item);
            }
        }
        None
    }

    /// Rebuild the lookup table at its construction size if evict/insert
    /// churn made it grow. hashbrown leaves tombstones on `remove`, and once
    /// they exhaust the growth budget it doubles the allocation instead of
    /// rehashing in place, even though live entries never exceed `capacity`.
    /// Rebuilding clears the tombstones and keeps the table's footprint fixed,
    /// which the memory gauge and the up-front size estimate rely on.
    fn compact_table_if_grown(&mut self) {
        let store = &self.item_store;
        let hasher = &self.hasher;
        self.table.shrink_to(self.capacity, |&idx| {
            hasher.hash_one(&store[idx as usize].item)
        });
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&T, u64)> {
        let mut items: Vec<_> = self
            .item_store
            .iter()
            .map(|s| (&s.item, s.count.as_u64(), s.sequence))
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
            .map(|s| (&s.item, s.count.as_u64(), s.sequence))
            .collect();
        items.sort_unstable_by_key(|(_, _, seq)| *seq);
        items.into_iter().map(|(k, count, _)| (k, count))
    }

    fn find_slot<Q>(&self, item: &Q) -> Option<usize>
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(item);
        self.find_slot_with_hash(item, hash)
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

    /// Heap order: count, then sequence. Ties broken by sequence keep the
    /// minimum unique, so eviction picks the same item on every replica.
    #[inline]
    fn heap_less(&self, a: u32, b: u32) -> bool {
        let (slot_a, slot_b) = (&self.item_store[a as usize], &self.item_store[b as usize]);
        (slot_a.count, slot_a.sequence) < (slot_b.count, slot_b.sequence)
    }

    fn sift_up(&mut self, mut pos: usize) {
        while pos > 0 {
            let parent = Self::parent(pos);
            if self.heap_less(self.heap[pos], self.heap[parent]) {
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

            if left < self.heap.len() && self.heap_less(self.heap[left], self.heap[smallest]) {
                smallest = left;
            }
            if right < self.heap.len() && self.heap_less(self.heap[right], self.heap[smallest]) {
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
        let mut queue = TopKQueue::<_, u64>::with_capacity(2);
        queue.upsert("a", 1);
        queue.upsert("b", 2);

        let items: Vec<_> = queue.iter().collect();
        assert_eq!(items, vec![(&"b", 2), (&"a", 1)]);
    }

    #[test]
    fn test_update_existing() {
        let mut queue = TopKQueue::<_, u64>::with_capacity_and_hasher(2, SipState::random());
        queue.upsert("a", 1);
        queue.upsert("b", 2);
        queue.upsert("a", 3); // Update a's count

        let items: Vec<_> = queue.iter().collect();
        assert_eq!(items, vec![(&"a", 3), (&"b", 2)]);
    }

    #[test]
    fn test_heap_cleanup() {
        let mut queue = TopKQueue::<_, u64>::with_capacity_and_hasher(2, SipState::random());

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
        let mut queue = TopKQueue::<_, u64>::with_capacity_and_hasher(3, SipState::random());

        // Insert items with same count in specific order
        queue.upsert("a", 1);
        queue.upsert("b", 1);
        queue.upsert("c", 1);

        let items: Vec<_> = queue.iter().collect();
        assert_eq!(items, vec![(&"a", 1), (&"b", 1), (&"c", 1)]);
    }

    #[test]
    fn test_heap_consistency() {
        let mut queue = TopKQueue::<_, u64>::with_capacity_and_hasher(2, SipState::random());

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
        let mut queue = TopKQueue::<_, u64>::with_capacity_and_hasher(2, SipState::random());

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
    fn test_upsert_returns_evicted_item() {
        // The displaced item is reported and lookups stay consistent afterwards.
        let mut queue = TopKQueue::<_, u64>::with_capacity_and_hasher(2, SipState::random());
        assert_eq!(queue.upsert("a", 1), None);
        assert_eq!(queue.upsert("b", 2), None);
        // Full queue, does not beat the min: rejected, nothing evicted.
        assert_eq!(queue.upsert("c", 1), None);
        // Full queue, beats the min: "a" is displaced and returned.
        assert_eq!(queue.upsert("c", 3), Some("a"));
        assert!(!queue.contains(&"a"));
        assert_eq!(queue.get(&"c"), Some(3));
        assert_eq!(queue.get(&"b"), Some(2));
    }

    // A `u32` counter width saturates on the way in and keeps ordering
    // consistent with the saturated value; the slot is 32 bytes for a
    // `Box<[u8]>` key (16-byte fat pointer + u32 count + u64 sequence +
    // u32 heap_pos).
    #[test]
    fn test_u32_counter_saturates_and_slot_layout() {
        let mut queue: TopKQueue<Box<[u8]>, u32> =
            TopKQueue::with_capacity_and_hasher(2, SipState::with_seed(1));
        queue.upsert(Box::from(&b"a"[..]), 10);
        queue.upsert(Box::from(&b"b"[..]), u64::MAX);
        assert_eq!(queue.get(&b"b"[..]), Some(u32::MAX as u64));
        // Raising past the cap is a no-op, and a same-as-cap update is too.
        assert!(queue.update_if_present(&b"b"[..], u64::MAX - 1));
        assert_eq!(queue.get(&b"b"[..]), Some(u32::MAX as u64));
        assert_eq!(queue.min_count(), 10);
        // A count that saturates to the current min does not displace it.
        assert_eq!(queue.upsert(Box::from(&b"c"[..]), 10), None);
        assert_eq!(
            queue.upsert(Box::from(&b"c"[..]), 11),
            Some(Box::from(&b"a"[..]))
        );

        assert_eq!(std::mem::size_of::<Slot<Box<[u8]>, u32>>(), 32);
        assert_eq!(std::mem::size_of::<Slot<Vec<u8>, u64>>(), 48);
    }

    #[test]
    fn test_repeated_updates() {
        let mut queue = TopKQueue::<_, u64>::with_capacity_and_hasher(2, SipState::random());

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
        let mut queue = TopKQueue::<_, u64>::with_capacity_and_hasher(10, SipState::random());

        // Insert in reverse order to test heap maintenance
        for i in (0..=10).rev() {
            queue.upsert(format!("item{}", i), i as u64);
        }

        // Verify heap property: parent should be <= children for min-heap
        for i in 1..queue.heap.len() {
            let parent_idx = TopKQueue::<String, u64>::parent(i);
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

    // Evict/insert churn at a constant live count must not grow the lookup
    // table: hashbrown leaves tombstones on remove and would otherwise double
    // the allocation once they exhaust the growth budget.
    #[test]
    fn test_table_footprint_is_fixed_under_churn() {
        for k in [10usize, 100, 1000] {
            let mut queue: TopKQueue<Vec<u8>> =
                TopKQueue::<_, u64>::with_capacity_and_hasher(k, SipState::with_seed(7));
            for i in 0..k {
                queue.upsert(format!("seed-{i}").into_bytes(), 1_000);
            }
            let table_bytes = queue.table.allocation_size();

            // Every insert beats the current min, so each one evicts exactly one.
            for r in 0..5000u64 {
                assert!(queue
                    .upsert(format!("hot-{r}").into_bytes(), 2_000 + r)
                    .is_some());
            }
            assert_eq!(
                queue.table.allocation_size(),
                table_bytes,
                "k={k}: table grew under churn"
            );
            // Lookups still work after the rebuilds.
            assert!(queue.contains(b"hot-4999".as_slice()));
            assert!(!queue.contains(b"seed-0".as_slice()));
        }
    }
}
