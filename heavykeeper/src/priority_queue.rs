use ahash::RandomState;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

/// A specialized priority queue for HeavyKeeper that maintains top-k items by count
#[derive(Clone)]
pub(crate) struct TopKQueue<T> {
    items: HashMap<T, (u64, usize), RandomState>, // item -> (count, heap_index)
    heap: Vec<(u64, usize, usize)>,               // (count, sequence, item_index)
    item_store: Vec<T>,                           // Store actual items here
    free_slots: Vec<usize>,                       // Track free slots in item_store
    capacity: usize,
    sequence: usize,
}

impl<T: Ord + Clone + Hash + PartialEq> TopKQueue<T> {
    pub(crate) fn with_capacity_and_hasher(capacity: usize, hasher: RandomState) -> Self {
        Self {
            items: HashMap::with_capacity_and_hasher(capacity, hasher),
            heap: Vec::with_capacity(capacity + 1),
            item_store: Vec::with_capacity(capacity),
            free_slots: Vec::with_capacity(capacity),
            capacity,
            sequence: 0,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, RandomState::new())
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns the heap memory (in bytes) used by this queue's containers.
    ///
    /// Computed from the allocated *capacity* of the `HashMap`, heap vector,
    /// item store, and free-slot list, plus the heap each live item owns beyond
    /// its inline `size_of::<T>()`. `item_heap(t)` should return the bytes `t`
    /// points to (e.g. `String::capacity`).
    ///
    /// The `HashMap` term mirrors hashbrown's internal SwissTable layout (not
    /// public API, but stable in practice across std releases).
    ///
    /// Each tracked item is stored twice: once as a `HashMap` key and once in
    /// `item_store`.
    pub(crate) fn mem_bytes<F>(&self, item_heap: F) -> usize
    where
        F: Fn(&T) -> usize,
    {
        use std::mem::size_of;
        // hashbrown internals: `buckets` is the next power of two >= ceil(capacity*8/7).
        let cap = self.items.capacity();
        let buckets = if cap == 0 {
            0
        } else {
            ((cap * 8 + 6) / 7).next_power_of_two()
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
        let map_bytes = if buckets == 0 {
            0
        } else {
            buckets * size_of::<(T, (u64, usize))>() + buckets + GROUP_WIDTH
        };
        // `items` is the source of truth for which items are live.
        let item_bytes: usize = self.items.keys().map(item_heap).sum();
        map_bytes
            + self.heap.capacity() * size_of::<(u64, usize, usize)>()
            + self.item_store.capacity() * size_of::<T>()
            + self.free_slots.capacity() * size_of::<usize>()
            + 2 * item_bytes
    }

    pub(crate) fn get<Q>(&self, item: &Q) -> Option<u64>
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        self.items.get(item).map(|(count, _)| *count)
    }

    pub(crate) fn contains<Q>(&self, item: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.items.contains_key(item)
    }

    /// Increase an existing entry's count. Caller must guarantee the new count
    /// is >= the current count (paper Algorithm 1: heap is max(maxv, existing)).
    pub(crate) fn update_if_present<Q>(&mut self, item: &Q, count: u64) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some((old_count, pos)) = self.items.get_mut(item) {
            debug_assert!(count >= *old_count, "update_if_present must not decrease");
            if count == *old_count {
                return true;
            }
            *old_count = count;
            let pos = *pos;
            self.heap[pos].0 = count;
            self.sift_down(pos);
            true
        } else {
            false
        }
    }

    pub(crate) fn min_count(&self) -> u64 {
        // If heap is empty, return 0
        // Otherwise return count from root node (index 0)
        self.heap.first().map(|(count, _, _)| *count).unwrap_or(0)
    }

    pub(crate) fn is_full(&self) -> bool {
        self.items.len() >= self.capacity
    }

    /// Insert or update `item` to `count`.
    ///
    /// Returns `Some(evicted)` when a previously tracked item is displaced
    /// by this call, otherwise `None`.
    pub(crate) fn upsert(&mut self, item: T, count: u64) -> Option<T> {
        // Fast path: update existing item
        if let Some((old_count, pos)) = self.items.get_mut(&item) {
            if count == *old_count {
                return None;
            }
            *old_count = count;

            // Update heap - no need to clone item
            let pos = *pos;
            let item_idx = self.heap[pos].2;
            self.heap[pos] = (count, self.heap[pos].1, item_idx);
            self.sift_down(pos);
            self.sift_up(pos);
            return None;
        }

        // For new items, if we have space just add it
        if self.len() < self.capacity {
            let pos = self.heap.len();
            self.sequence += 1;

            // Store item once
            let item_idx = if let Some(idx) = self.free_slots.pop() {
                self.item_store[idx] = item.clone();
                idx
            } else {
                self.item_store.push(item.clone());
                self.item_store.len() - 1
            };

            self.heap.push((count, self.sequence, item_idx));
            self.items.insert(item, (count, pos));
            self.sift_up(pos);
            return None;
        }

        // Queue is full - check if new count beats minimum
        if let Some(&(min_count, _, item_idx)) = self.heap.first() {
            if count > min_count {
                let old_item = std::mem::replace(&mut self.item_store[item_idx], item.clone());
                self.items.remove(&old_item);

                self.items.insert(item, (count, 0));
                self.sequence += 1;
                self.heap[0] = (count, self.sequence, item_idx);
                self.sift_down(0);
                return Some(old_item);
            }
        }
        None
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&T, u64)> {
        // Materialize (key, count, sequence) using stored heap index so
        // per-comparison work is O(1) instead of scanning the heap.
        let mut items: Vec<_> = self
            .items
            .iter()
            .map(|(k, (count, heap_idx))| {
                let seq = self.heap[*heap_idx].1;
                (k, *count, seq)
            })
            .collect();

        // Sort by count descending, then by sequence ascending.
        items.sort_unstable_by(|(_, c1, s1), (_, c2, s2)| match c2.cmp(c1) {
            std::cmp::Ordering::Equal => s1.cmp(s2),
            other => other,
        });

        // Return an iterator over (&T, count), preserving sorted order.
        items.into_iter().map(|(k, count, _)| (k, count))
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
            if self.heap[parent].0 > self.heap[pos].0 {
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

            if left < self.heap.len() && self.heap[left].0 < self.heap[smallest].0 {
                smallest = left;
            }
            if right < self.heap.len() && self.heap[right].0 < self.heap[smallest].0 {
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
        // Update indices in items map
        let (_, _, item_idx_i) = self.heap[i];
        let (_, _, item_idx_j) = self.heap[j];

        // Get references to the actual items
        let item_i = &self.item_store[item_idx_i];
        let item_j = &self.item_store[item_idx_j];

        // Update the positions in the items map
        if let Some((_, pos_i)) = self.items.get_mut(item_i) {
            *pos_i = i;
        }
        if let Some((_, pos_j)) = self.items.get_mut(item_j) {
            *pos_j = j;
        }
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
                assert!(
                    queue.heap[parent_idx].0 <= queue.heap[i].0,
                    "Heap property violated: parent count {} at index {} is greater than child count {} at index {}",
                    queue.heap[parent_idx].0,
                    parent_idx,
                    queue.heap[i].0,
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
