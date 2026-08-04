//! Cuckoo-hash HeavyKeeper variant.
//!
//! Each bucket is split into a probabilistic-decay "lobby" cell plus `depth`
//! non-decaying "heavy" slots. New items land in the lobby of their primary
//! bucket and are promoted to a heavy slot once they outweigh the resident
//! lobby fingerprint. Heavy items live in one of two candidate buckets
//! (cuckoo-style); on collision the lower-count occupant is evicted and
//! re-homed in its other candidate bucket via a bounded kick chain.

use std::borrow::Borrow;
use std::fmt::Debug;
use std::hash::Hash;

use ahash::RandomState;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use thiserror::Error;

use crate::priority_queue::TopKQueue;

const DECAY_LOOKUP_SIZE: usize = 1024;

/// Default upper bound on the cuckoo kick chain. Higher values raise the
/// effective load factor of the heavy slots (fewer silent drops on
/// collision) at the cost of worst-case work per `add`. Override with
/// [`CuckooBuilder::max_kicks`].
pub const DEFAULT_MAX_CUCKOO_KICKS: usize = 8;

/// Probe hashed at merge time to detect mismatched hashers.
const MERGE_HASHER_PROBE: &[u8] = b"heavykeeper-merge-compat-probe";

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum CuckooMergeError {
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
pub enum CuckooBuilderError {
    #[error("Missing required field: {field}")]
    MissingField { field: String },
    #[error("Invalid depth {depth}: must be >= 1")]
    InvalidDepth { depth: usize },
    #[error("Invalid width {width}: must be >= 1")]
    InvalidWidth { width: usize },
    #[error("Invalid decay {decay}: must be a finite value in 0.0..=1.0")]
    InvalidDecay { decay: f64 },
    #[error("Invalid max_kicks {max_kicks}: must be >= 1")]
    InvalidMaxKicks { max_kicks: usize },
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CuckooNode<T> {
    pub item: T,
    pub count: u64,
}

impl<T: Ord> Ord for CuckooNode<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.count.cmp(&self.count)
    }
}

impl<T: Ord> PartialOrd for CuckooNode<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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

#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

/// HeavyKeeper variant where each bucket has a probabilistic-decay "lobby"
/// cell plus `depth` non-decaying "heavy" slots. New items land in the lobby
/// of their primary bucket and are promoted to a heavy slot once they
/// outweigh the resident lobby fingerprint. Heavy items live in one of two
/// candidate buckets (cuckoo-style); on collision the lower-count occupant
/// is evicted and re-homed in its other candidate bucket via a bounded kick
/// chain.
///
/// Implements [`Clone`] as a true deep copy
#[derive(Clone)]
pub struct CuckooTopK<T: Ord + Clone + Hash> {
    width: usize,
    width_mask: usize,
    depth: usize,
    decay: f64,
    lobbies: Box<[Cell]>,
    heavy: Box<[Cell]>,
    decay_thresholds: Box<[u64]>,
    priority_queue: TopKQueue<T>,
    hasher: RandomState,
    rng: SmallRng,
    min_pq_count: u64,
    top_items: usize,
    max_kicks: usize,
}

impl<T: Ord + Clone + Hash> CuckooTopK<T> {
    /// Build a `CuckooTopK` with a fixed default seed and the default kick
    /// limit ([`DEFAULT_MAX_CUCKOO_KICKS`]). Parameters are not validated;
    /// use [`CuckooTopK::builder`] for a fallible, validated construction
    /// path.
    pub fn new(k: usize, width: usize, depth: usize, decay: f64) -> Self {
        Self::with_seed(k, width, depth, decay, 12345)
    }

    /// Build a `CuckooTopK` with the given seed for both the hasher and
    /// internal RNG. Two instances built with the same seed are
    /// `merge`-compatible. Parameters are not validated; use
    /// [`CuckooTopK::builder`] for a fallible, validated construction path.
    pub fn with_seed(k: usize, width: usize, depth: usize, decay: f64, seed: u64) -> Self {
        let hasher = RandomState::with_seeds(seed, seed, seed, seed);
        Self::with_components(
            k,
            width,
            depth,
            decay,
            hasher,
            SmallRng::seed_from_u64(seed),
            DEFAULT_MAX_CUCKOO_KICKS,
        )
    }

    /// Build a `CuckooTopK` with a caller-supplied hasher. Merge
    /// compatibility is probe-checked against the partner's hasher; see
    /// [`CuckooTopK::merge`]. Parameters are not validated; use
    /// [`CuckooTopK::builder`] for a fallible, validated construction path.
    pub fn with_hasher(
        k: usize,
        width: usize,
        depth: usize,
        decay: f64,
        hasher: RandomState,
    ) -> Self {
        Self::with_components(
            k,
            width,
            depth,
            decay,
            hasher,
            SmallRng::seed_from_u64(0),
            DEFAULT_MAX_CUCKOO_KICKS,
        )
    }

    /// Fluent builder; see [`CuckooBuilder`].
    pub fn builder() -> CuckooBuilder<T> {
        CuckooBuilder::new()
    }

    fn with_components(
        k: usize,
        width: usize,
        depth: usize,
        decay: f64,
        hasher: RandomState,
        rng: SmallRng,
        max_kicks: usize,
    ) -> Self {
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
            lobbies: vec![Cell::default(); width].into_boxed_slice(),
            heavy: vec![Cell::default(); width * depth].into_boxed_slice(),
            decay_thresholds: precompute_decay_thresholds(decay, DECAY_LOOKUP_SIZE),
            priority_queue: TopKQueue::with_capacity_and_hasher(k, hasher.clone()),
            hasher,
            rng,
            min_pq_count: 0,
            top_items: k,
            max_kicks,
        }
    }

    /// Insert `increment` occurrences of `item`. Items first land in the
    /// lobby of their primary bucket and only appear in [`list`] after they
    /// have been promoted into a heavy slot.
    ///
    /// [`list`]: CuckooTopK::list
    pub fn add<Q>(&mut self, item: &Q, increment: u64)
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        let _ = self.add_with_evicted(item, increment);
    }

    /// Same as [`add`], but returns `(evicted, inserted)`: `evicted` is the
    /// top-k item displaced from a full priority queue by this call (if any),
    /// and `inserted` is `true` when `item` became newly tracked (into free
    /// space, or by displacing `evicted`). Updating an already-tracked item,
    /// or a count too low to be tracked, yields `(None, false)`.
    ///
    /// [`add`]: CuckooTopK::add
    pub fn add_with_evicted<Q>(&mut self, item: &Q, increment: u64) -> (Option<T>, bool)
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        if increment == 0 {
            return (None, false);
        }

        let fp = self.hasher.hash_one(item);
        let (primary, alternate) = self.bucket_pair(fp);

        if let Some(idx) = self.find_heavy(fp, primary, alternate) {
            self.heavy[idx].count = self.heavy[idx].count.saturating_add(increment);
            return self.update_priority_queue(item, self.heavy[idx].count);
        }

        let lobby_count = match self.update_lobby(primary, fp, increment) {
            Some(c) => c,
            None => return (None, false),
        };

        if self.promote(fp, lobby_count, primary, alternate) {
            self.clear_lobby(primary, fp);
            return self.update_priority_queue(item, lobby_count);
        }
        (None, false)
    }

    /// Estimated count for `item`. Consults the priority queue first
    /// (max-ever-seen, paper Algorithm 1) before falling back to the raw
    /// sketch reading via [`bucket_count`].
    ///
    /// [`bucket_count`]: CuckooTopK::bucket_count
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

    /// Raw current sketch count for `item`: heavy slot if present, else the
    /// lobby cell at the primary bucket, else 0. Skips the priority queue
    /// (so this can be lower than [`count`] for an item that has decayed).
    ///
    /// [`count`]: CuckooTopK::count
    pub fn bucket_count<Q>(&self, item: &Q) -> u64
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        let fp = self.hasher.hash_one(item);
        let (primary, alternate) = self.bucket_pair(fp);
        if let Some(idx) = self.find_heavy(fp, primary, alternate) {
            return self.heavy[idx].count;
        }
        let lobby = self.lobbies[primary];
        if lobby.fingerprint == fp {
            lobby.count
        } else {
            0
        }
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

    /// Returns true if `item` is currently one of the top-k tracked flows
    pub fn contains_top_k<Q>(&self, item: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.priority_queue.contains(item)
    }

    /// Top-k items currently tracked by the priority queue, sorted by
    /// descending count. Items still in the lobby (not yet promoted to a
    /// heavy slot) do not appear here.
    pub fn list(&self) -> Vec<CuckooNode<T>> {
        let mut nodes: Vec<CuckooNode<T>> = self
            .priority_queue
            .iter()
            .map(|(item, count)| CuckooNode {
                item: item.clone(),
                count,
            })
            .collect();
        nodes.sort();
        nodes
    }

    pub fn width(&self) -> usize {
        self.width
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn decay(&self) -> f64 {
        self.decay
    }

    pub fn top_items(&self) -> usize {
        self.top_items
    }

    /// Maximum number of cuckoo kicks attempted when relocating an evicted
    /// heavy slot. See [`DEFAULT_MAX_CUCKOO_KICKS`] for the default and
    /// trade-off.
    pub fn max_kicks(&self) -> usize {
        self.max_kicks
    }

    /// Estimated heap memory (in bytes) used by this sketch.
    ///
    /// Sums the lobby and heavy cell arrays, the precomputed decay-threshold
    /// table, and the priority queue's allocations, plus the heap each tracked
    /// item owns beyond its inline `size_of::<T>()`. `item_heap(t)` should
    /// return the bytes `t` points to (e.g. `String::capacity`); for a `T`
    /// that owns no heap, pass `|_| 0`.
    pub fn mem_bytes<F>(&self, item_heap: F) -> usize
    where
        F: Fn(&T) -> usize,
    {
        use std::mem::size_of;
        self.lobbies.len() * size_of::<Cell>()
            + self.heavy.len() * size_of::<Cell>()
            + self.decay_thresholds.len() * size_of::<u64>()
            + self.priority_queue.mem_bytes(item_heap)
    }

    /// Merge `other` into `self`. Both sketches must share width, depth,
    /// decay, top_items and a compatible hasher (probe-checked at merge
    /// time). Heavy fingerprints from `other` are re-inserted into the
    /// pair of candidate buckets they hash to in `self`; lobby
    /// fingerprints are unioned without probabilistic decay (merges are
    /// deterministic). The priority queue is rebuilt from snapshots
    /// captured before mutation so pre-merge bucket counts can be used as
    /// fallback for the side that does not already track an item.
    pub fn merge(&mut self, other: &Self) -> Result<(), CuckooMergeError> {
        if self.width != other.width {
            return Err(CuckooMergeError::IncompatibleWidth {
                self_width: self.width,
                other_width: other.width,
            });
        }
        if self.depth != other.depth {
            return Err(CuckooMergeError::IncompatibleDepth {
                self_depth: self.depth,
                other_depth: other.depth,
            });
        }
        if self.decay != other.decay {
            return Err(CuckooMergeError::IncompatibleDecay {
                self_decay: self.decay,
                other_decay: other.decay,
            });
        }
        if self.top_items != other.top_items {
            return Err(CuckooMergeError::IncompatibleTopItems {
                self_items: self.top_items,
                other_items: other.top_items,
            });
        }
        if self.hasher.hash_one(MERGE_HASHER_PROBE) != other.hasher.hash_one(MERGE_HASHER_PROBE) {
            return Err(CuckooMergeError::IncompatibleHasher);
        }

        // PQ merge runs BEFORE heavy/lobby mutation so `self.bucket_count`
        // reflects pre-merge state when used as a fallback.
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

        // Walk other's heavy cells; re-insert each fingerprint into self's
        // candidate buckets via cuckoo semantics. If the same fingerprint
        // is currently in self's lobby for that primary bucket, fold its
        // count in and clear the lobby — an item should live in heavy XOR
        // lobby, never both.
        for o_idx in 0..other.heavy.len() {
            let oc = other.heavy[o_idx];
            if oc.count == 0 {
                continue;
            }
            let fp = oc.fingerprint;
            let mut count = oc.count;
            let (primary, alternate) = self.bucket_pair(fp);

            if self.lobbies[primary].count > 0 && self.lobbies[primary].fingerprint == fp {
                count = count.saturating_add(self.lobbies[primary].count);
                self.lobbies[primary] = Cell::default();
            }

            if let Some(idx) = self.find_heavy(fp, primary, alternate) {
                self.heavy[idx].count = self.heavy[idx].count.saturating_add(count);
                continue;
            }

            if let Some(idx) = self.find_empty_heavy_in_bucket(primary) {
                self.heavy[idx] = Cell {
                    fingerprint: fp,
                    count,
                };
                continue;
            }
            if alternate != primary {
                if let Some(idx) = self.find_empty_heavy_in_bucket(alternate) {
                    self.heavy[idx] = Cell {
                        fingerprint: fp,
                        count,
                    };
                    continue;
                }
            }

            let (victim_idx, victim_count) = self.min_heavy_in_candidates(primary, alternate);
            if count > victim_count {
                let victim_bucket = victim_idx / self.depth;
                let victim = self.heavy[victim_idx];
                self.heavy[victim_idx] = Cell {
                    fingerprint: fp,
                    count,
                };
                self.relocate_victim(victim, victim_bucket);
            }
            // else: count too low to evict, drop.
        }

        // Walk other's lobbies. If the fingerprint is already heavy in
        // self (via either candidate bucket), fold the lobby count into
        // the heavy entry. Otherwise resolve lobby-vs-lobby conflicts
        // deterministically.
        for o_idx in 0..other.lobbies.len() {
            let oc = other.lobbies[o_idx];
            if oc.count == 0 {
                continue;
            }
            let fp = oc.fingerprint;
            let count = oc.count;
            let (primary, alternate) = self.bucket_pair(fp);

            if let Some(idx) = self.find_heavy(fp, primary, alternate) {
                self.heavy[idx].count = self.heavy[idx].count.saturating_add(count);
                continue;
            }

            let lobby = self.lobbies[primary];
            if lobby.count > 0 && lobby.fingerprint == fp {
                self.lobbies[primary].count = lobby.count.saturating_add(count);
            } else if lobby.count == 0 || count > lobby.count {
                self.lobbies[primary] = Cell {
                    fingerprint: fp,
                    count,
                };
            }
            // else: keep self's lobby occupant (higher count wins; ties keep
            // self deterministically).
        }

        self.min_pq_count = self.priority_queue.min_count();
        Ok(())
    }

    #[inline]
    fn heavy_range(&self, bucket: usize) -> std::ops::Range<usize> {
        let start = bucket * self.depth;
        start..start + self.depth
    }

    #[inline]
    fn bucket_index(&self, fingerprint: u64) -> usize {
        if self.width_mask != 0 {
            fingerprint as usize & self.width_mask
        } else {
            (fingerprint as usize) % self.width
        }
    }

    #[inline]
    fn bucket_pair(&self, fingerprint: u64) -> (usize, usize) {
        let primary = self.bucket_index(fingerprint);
        if self.width == 1 {
            return (primary, primary);
        }

        let mut alternate = self.bucket_index(mix64(fingerprint ^ 0x9e3779b97f4a7c15));
        if alternate == primary {
            alternate = (alternate + 1) % self.width;
        }
        (primary, alternate)
    }

    #[inline]
    fn find_heavy(&self, fingerprint: u64, primary: usize, alternate: usize) -> Option<usize> {
        if let Some(idx) = self.find_heavy_in_bucket(fingerprint, primary) {
            return Some(idx);
        }
        if alternate != primary {
            self.find_heavy_in_bucket(fingerprint, alternate)
        } else {
            None
        }
    }

    #[inline]
    fn find_heavy_in_bucket(&self, fingerprint: u64, bucket: usize) -> Option<usize> {
        self.heavy_range(bucket)
            .find(|&idx| self.heavy[idx].count > 0 && self.heavy[idx].fingerprint == fingerprint)
    }

    #[inline]
    fn find_empty_heavy_in_bucket(&self, bucket: usize) -> Option<usize> {
        self.heavy_range(bucket)
            .find(|&idx| self.heavy[idx].count == 0)
    }

    #[inline]
    fn min_heavy_in_bucket(&self, bucket: usize) -> (usize, u64) {
        let mut min_idx = bucket * self.depth;
        let mut min_count = u64::MAX;
        for idx in self.heavy_range(bucket) {
            let count = self.heavy[idx].count;
            if count < min_count {
                min_idx = idx;
                min_count = count;
            }
        }
        (min_idx, min_count)
    }

    #[inline]
    fn min_heavy_in_candidates(&self, primary: usize, alternate: usize) -> (usize, u64) {
        let (mut min_idx, mut min_count) = self.min_heavy_in_bucket(primary);
        if alternate != primary {
            let (alternate_min_idx, alternate_min_count) = self.min_heavy_in_bucket(alternate);
            if alternate_min_count < min_count {
                min_idx = alternate_min_idx;
                min_count = alternate_min_count;
            }
        }
        (min_idx, min_count)
    }

    fn update_lobby(&mut self, bucket: usize, fingerprint: u64, increment: u64) -> Option<u64> {
        let lobby = &mut self.lobbies[bucket];
        if lobby.count == 0 || lobby.fingerprint == fingerprint {
            lobby.fingerprint = fingerprint;
            lobby.count = lobby.count.saturating_add(increment);
            return Some(lobby.count);
        }

        self.decay_lobby_and_maybe_replace(bucket, fingerprint, increment)
    }

    fn clear_lobby(&mut self, bucket: usize, fingerprint: u64) {
        let lobby = &mut self.lobbies[bucket];
        if lobby.fingerprint == fingerprint {
            *lobby = Cell::default();
        }
    }

    fn promote(&mut self, fingerprint: u64, count: u64, primary: usize, alternate: usize) -> bool {
        if let Some(idx) = self.find_empty_heavy_in_bucket(primary) {
            self.heavy[idx] = Cell { fingerprint, count };
            return true;
        }

        if alternate != primary {
            if let Some(idx) = self.find_empty_heavy_in_bucket(alternate) {
                self.heavy[idx] = Cell { fingerprint, count };
                return true;
            }
        }

        let (victim_idx, victim_count) = self.min_heavy_in_candidates(primary, alternate);
        if count <= victim_count {
            return false;
        }

        let victim_bucket = victim_idx / self.depth;
        let victim = self.heavy[victim_idx];
        self.heavy[victim_idx] = Cell { fingerprint, count };
        self.relocate_victim(victim, victim_bucket);
        true
    }

    fn relocate_victim(&mut self, mut victim: Cell, mut from_bucket: usize) {
        for _ in 0..self.max_kicks {
            if victim.count == 0 {
                return;
            }

            let (primary, alternate) = self.bucket_pair(victim.fingerprint);
            let target = if from_bucket == primary {
                alternate
            } else {
                primary
            };
            if target == from_bucket {
                return;
            }

            if let Some(empty_idx) = self.find_empty_heavy_in_bucket(target) {
                self.heavy[empty_idx] = victim;
                return;
            }

            let (target_min_idx, target_min_count) = self.min_heavy_in_bucket(target);
            if victim.count <= target_min_count {
                return;
            }

            std::mem::swap(&mut self.heavy[target_min_idx], &mut victim);
            from_bucket = target;
        }
    }

    fn decay_lobby_and_maybe_replace(
        &mut self,
        bucket: usize,
        fingerprint: u64,
        increment: u64,
    ) -> Option<u64> {
        let mut remaining = increment;
        while remaining > 0 {
            let current_count = self.lobbies[bucket].count;
            let threshold = self.decay_threshold(current_count);
            if self.rng.next_u64() < threshold {
                let lobby = &mut self.lobbies[bucket];
                lobby.count = lobby.count.saturating_sub(1);
                if lobby.count == 0 {
                    lobby.fingerprint = fingerprint;
                    lobby.count = remaining;
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
        // would truncate (not saturate) for q > i32::MAX and produce a
        // negative exponent, sending threshold to ∞ for very-hot keys.
        let q = (count / divisor) as f64;
        let r = (count % divisor) as usize;
        let rem_thr = tbl[r] as f64 / u64::MAX as f64;
        ((last.powf(q) * rem_thr) * u64::MAX as f64) as u64
    }

    fn update_priority_queue<Q>(&mut self, item: &Q, count: u64) -> (Option<T>, bool)
    where
        T: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = T> + ?Sized,
    {
        if let Some(current) = self.priority_queue.get(item) {
            if count > current {
                self.priority_queue.update_if_present(item, count);
                self.min_pq_count = self.priority_queue.min_count();
            }
            return (None, false);
        }

        if self.priority_queue.is_full() && count <= self.min_pq_count {
            return (None, false);
        }

        let had_room = !self.priority_queue.is_full();
        let evicted = self.priority_queue.upsert(item.to_owned(), count);
        self.min_pq_count = self.priority_queue.min_count();
        let inserted = evicted.is_some() || had_room;
        (evicted, inserted)
    }
}

pub struct CuckooBuilder<T> {
    k: Option<usize>,
    width: Option<usize>,
    depth: Option<usize>,
    decay: Option<f64>,
    seed: Option<u64>,
    hasher: Option<RandomState>,
    max_kicks: Option<usize>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Ord + Clone + Hash> Default for CuckooBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Ord + Clone + Hash> CuckooBuilder<T> {
    pub fn new() -> Self {
        Self {
            k: None,
            width: None,
            depth: None,
            decay: None,
            seed: None,
            hasher: None,
            max_kicks: None,
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
    /// Override the cuckoo kick chain limit; see [`DEFAULT_MAX_CUCKOO_KICKS`].
    /// Must be `>= 1`.
    pub fn max_kicks(mut self, n: usize) -> Self {
        self.max_kicks = Some(n);
        self
    }

    pub fn build(self) -> Result<CuckooTopK<T>, CuckooBuilderError> {
        let k = self
            .k
            .ok_or_else(|| CuckooBuilderError::MissingField { field: "k".into() })?;
        let width = self.width.ok_or_else(|| CuckooBuilderError::MissingField {
            field: "width".into(),
        })?;
        let depth = self.depth.ok_or_else(|| CuckooBuilderError::MissingField {
            field: "depth".into(),
        })?;
        let decay = self.decay.ok_or_else(|| CuckooBuilderError::MissingField {
            field: "decay".into(),
        })?;
        if width < 1 {
            return Err(CuckooBuilderError::InvalidWidth { width });
        }
        if depth < 1 {
            return Err(CuckooBuilderError::InvalidDepth { depth });
        }
        if !decay.is_finite() || !(0.0..=1.0).contains(&decay) {
            return Err(CuckooBuilderError::InvalidDecay { decay });
        }
        let max_kicks = self.max_kicks.unwrap_or(DEFAULT_MAX_CUCKOO_KICKS);
        if max_kicks < 1 {
            return Err(CuckooBuilderError::InvalidMaxKicks { max_kicks });
        }
        let hasher = self.hasher.unwrap_or_else(|| {
            if let Some(s) = self.seed {
                RandomState::with_seeds(s, s, s, s)
            } else {
                RandomState::new()
            }
        });
        let rng = SmallRng::seed_from_u64(self.seed.unwrap_or(0));
        Ok(CuckooTopK::with_components(
            k, width, depth, decay, hasher, rng, max_kicks,
        ))
    }
}

#[cfg(test)]
impl<T: Ord + Clone + Hash> CuckooTopK<T> {
    pub(crate) fn decay_threshold_for_test(&self, count: u64) -> u64 {
        self.decay_threshold(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The deprecated `query` alias must still compile and delegate to `contains`.
    #[test]
    #[allow(deprecated)]
    fn test_query_alias_delegates_to_contains() {
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 64, 3, 0.9);
        topk.add(b"alpha".as_slice(), 5);
        assert_eq!(topk.query(b"alpha".as_slice()), topk.contains(b"alpha".as_slice()));
        assert_eq!(topk.query(b"missing".as_slice()), topk.contains(b"missing".as_slice()));
    }

    #[test]
    fn test_new_default_params() {
        let topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 64, 3, 0.9);
        assert_eq!(topk.width, 64);
        assert_eq!(topk.depth, 3);
        assert_eq!(topk.decay, 0.9);
        assert_eq!(topk.top_items, 10);
        assert_eq!(topk.lobbies.len(), 64);
        assert_eq!(topk.heavy.len(), 192);
    }

    #[test]
    fn test_mem_bytes_covers_cells_and_decay_table() {
        let topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 64, 3, 0.9);
        let cell = std::mem::size_of::<Cell>();
        let lobbies = 64 * cell;
        let heavy = 64 * 3 * cell;
        let decay = DECAY_LOOKUP_SIZE * std::mem::size_of::<u64>();
        // The fixed sketch arrays are exact; the priority queue adds more.
        assert!(topk.mem_bytes(|_| 0) >= lobbies + heavy + decay);
    }

    #[test]
    fn test_mem_bytes_grows_with_width() {
        let small: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 64, 3, 0.9);
        let large: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 256, 3, 0.9);
        assert!(large.mem_bytes(|_| 0) > small.mem_bytes(|_| 0));
    }

    #[test]
    fn test_mem_bytes_grows_with_depth() {
        let shallow: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 64, 2, 0.9);
        let deep: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 64, 8, 0.9);
        assert!(deep.mem_bytes(|_| 0) > shallow.mem_bytes(|_| 0));
    }

    #[test]
    fn test_add_promotes_to_heavy_and_counts() {
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 32, 2, 0.9);
        topk.add(b"alpha".as_slice(), 1);
        topk.add(b"alpha".as_slice(), 4);

        assert_eq!(topk.count(b"alpha".as_slice()), 5);
        assert!(topk.contains(b"alpha".as_slice()));
        assert_eq!(topk.list()[0].item, b"alpha".to_vec());
        assert_eq!(topk.list()[0].count, 5);
    }

    #[test]
    fn test_two_candidate_buckets_can_hold_primary_collisions() {
        let mut topk: CuckooTopK<u64> = CuckooTopK::with_seed(8, 8, 1, 0.9, 7);

        let mut keys = Vec::new();
        for key in 0..10_000u64 {
            let fp = topk.hasher.hash_one(key);
            let (primary, alternate) = topk.bucket_pair(fp);
            if primary == 0 && alternate != 0 {
                keys.push(key);
                if keys.len() == 2 {
                    break;
                }
            }
        }
        assert_eq!(keys.len(), 2);

        for _ in 0..10 {
            topk.add(&keys[0], 1);
            topk.add(&keys[1], 1);
        }

        assert_eq!(topk.count(&keys[0]), 10);
        assert_eq!(topk.count(&keys[1]), 10);
    }

    #[test]
    fn test_stronger_lobby_candidate_replaces_heavy_victim() {
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(2, 1, 1, 0.9);

        for _ in 0..10 {
            topk.add(b"small".as_slice(), 1);
        }
        for _ in 0..20 {
            topk.add(b"large".as_slice(), 1);
        }

        assert!(topk.count(b"large".as_slice()) > topk.bucket_count(b"small".as_slice()));
    }

    #[test]
    fn test_add_increment_zero_is_noop() {
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(5, 64, 4, 0.9);
        topk.add(&b"a".to_vec(), 0);
        assert_eq!(topk.count(&b"a".to_vec()), 0);
        assert!(topk.list().is_empty());
    }

    #[test]
    fn test_add_count_saturates_on_overflow() {
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(2, 1, 1, 0.9);
        topk.add(&b"x".to_vec(), u64::MAX);
        topk.add(&b"x".to_vec(), 1);
        assert_eq!(topk.count(&b"x".to_vec()), u64::MAX);
        topk.add(&b"x".to_vec(), 1_000_000);
        assert_eq!(topk.count(&b"x".to_vec()), u64::MAX);
    }

    #[test]
    fn test_add_more_items_than_capacity() {
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(2, 100, 4, 0.9);
        for name in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()] {
            topk.add(&name, 1);
        }
        assert!(topk.list().len() <= 2);
    }

    #[test]
    fn test_non_ascii_and_emoji() {
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(5, 100, 4, 0.9);
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
    fn test_borrow_str_and_slice() {
        let mut topk: CuckooTopK<String> = CuckooTopK::new(10, 100, 4, 0.9);
        topk.add("foo", 1);
        assert!(topk.contains("foo"));
        assert_eq!(topk.count("foo"), 1);

        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 100, 4, 0.9);
        let item: &[u8] = b"foo";
        topk.add(item, 1);
        assert!(topk.contains(item));
        assert_eq!(topk.count(item), 1);
    }

    #[test]
    fn test_seed_determinism() {
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(5, 64, 4, 0.9, 42);
        let mut b: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(5, 64, 4, 0.9, 42);
        for i in 0..200u32 {
            let key = format!("k{i}").into_bytes();
            for _ in 0..(i as u64 % 7 + 1) {
                a.add(&key, 1);
                b.add(&key, 1);
            }
        }
        let la = a.list();
        let lb = b.list();
        assert_eq!(la.len(), lb.len());
        for (na, nb) in la.iter().zip(lb.iter()) {
            assert_eq!(na.item, nb.item);
            assert_eq!(na.count, nb.count);
        }
    }

    #[test]
    fn test_builder_missing_fields() {
        let r = CuckooBuilder::<Vec<u8>>::new()
            .width(64)
            .depth(4)
            .decay(0.9)
            .build();
        assert!(matches!(r, Err(CuckooBuilderError::MissingField { field }) if field == "k"));

        let r = CuckooBuilder::<Vec<u8>>::new()
            .k(10)
            .depth(4)
            .decay(0.9)
            .build();
        assert!(matches!(r, Err(CuckooBuilderError::MissingField { field }) if field == "width"));

        let r = CuckooBuilder::<Vec<u8>>::new()
            .k(10)
            .width(64)
            .decay(0.9)
            .build();
        assert!(matches!(r, Err(CuckooBuilderError::MissingField { field }) if field == "depth"));

        let r = CuckooBuilder::<Vec<u8>>::new()
            .k(10)
            .width(64)
            .depth(4)
            .build();
        assert!(matches!(r, Err(CuckooBuilderError::MissingField { field }) if field == "decay"));
    }

    #[test]
    fn test_builder_invalid_depth_zero() {
        let r = CuckooBuilder::<Vec<u8>>::new()
            .k(10)
            .width(64)
            .depth(0)
            .decay(0.9)
            .build();
        assert!(matches!(
            r,
            Err(CuckooBuilderError::InvalidDepth { depth: 0 })
        ));
    }

    #[test]
    fn test_builder_max_kicks_default_and_override() {
        let default_topk: CuckooTopK<Vec<u8>> = CuckooTopK::builder()
            .k(10)
            .width(64)
            .depth(4)
            .decay(0.9)
            .build()
            .unwrap();
        assert_eq!(default_topk.max_kicks(), DEFAULT_MAX_CUCKOO_KICKS);

        let custom: CuckooTopK<Vec<u8>> = CuckooTopK::builder()
            .k(10)
            .width(64)
            .depth(4)
            .decay(0.9)
            .max_kicks(32)
            .build()
            .unwrap();
        assert_eq!(custom.max_kicks(), 32);

        let infallible: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 64, 4, 0.9);
        assert_eq!(infallible.max_kicks(), DEFAULT_MAX_CUCKOO_KICKS);
    }

    #[test]
    fn test_builder_invalid_max_kicks_zero() {
        let r: Result<CuckooTopK<Vec<u8>>, _> = CuckooTopK::builder()
            .k(10)
            .width(64)
            .depth(4)
            .decay(0.9)
            .max_kicks(0)
            .build();
        assert!(matches!(
            r,
            Err(CuckooBuilderError::InvalidMaxKicks { max_kicks: 0 })
        ));
    }

    #[test]
    fn test_builder_rejects_decay_out_of_range() {
        let cases = [-0.1f64, 1.1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
        for d in cases {
            let res: Result<CuckooTopK<Vec<u8>>, _> = CuckooTopK::builder()
                .k(10)
                .width(64)
                .depth(4)
                .decay(d)
                .build();
            match res {
                Ok(_) => panic!("expected InvalidDecay for {d}, got Ok"),
                Err(CuckooBuilderError::InvalidDecay { decay }) => {
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
    fn test_merge_basic() {
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 4, 0.9);
        let mut b: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 4, 0.9);
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
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 4, 0.9);
        let b: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 32, 4, 0.9);
        match a.merge(&b) {
            Err(CuckooMergeError::IncompatibleWidth {
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
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 4, 0.9);
        let b: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 2, 0.9);
        match a.merge(&b) {
            Err(CuckooMergeError::IncompatibleDepth {
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
    fn test_merge_incompatible_decay() {
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 4, 0.9);
        let b: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 4, 0.8);
        match a.merge(&b) {
            Err(CuckooMergeError::IncompatibleDecay {
                self_decay,
                other_decay,
            }) => {
                assert_eq!(self_decay, 0.9);
                assert_eq!(other_decay, 0.8);
            }
            _ => panic!("expected IncompatibleDecay"),
        }
    }

    #[test]
    fn test_merge_incompatible_top_items() {
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 4, 0.9);
        let b: CuckooTopK<Vec<u8>> = CuckooTopK::new(5, 64, 4, 0.9);
        match a.merge(&b) {
            Err(CuckooMergeError::IncompatibleTopItems {
                self_items,
                other_items,
            }) => {
                assert_eq!(self_items, 3);
                assert_eq!(other_items, 5);
            }
            _ => panic!("expected IncompatibleTopItems"),
        }
    }

    #[test]
    fn test_merge_incompatible_hasher_different_seed() {
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(10, 64, 4, 0.9, 1);
        let b: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(10, 64, 4, 0.9, 2);
        match a.merge(&b) {
            Err(CuckooMergeError::IncompatibleHasher) => {}
            other => panic!("expected IncompatibleHasher, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_compatible_with_same_explicit_seed() {
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(10, 64, 4, 0.9, 7);
        let mut b: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(10, 64, 4, 0.9, 7);
        a.add(&b"x".to_vec(), 3);
        b.add(&b"x".to_vec(), 4);
        a.merge(&b).expect("same seed should be compatible");
        assert_eq!(a.count(&b"x".to_vec()), 7);
    }

    #[test]
    fn test_merge_folds_other_lobby_into_self_heavy() {
        // self has x heavy with a high count; other has x in lobby.
        // The lobby contribution from other must fold into self's heavy
        // entry — not be written into self.lobbies alongside it (where
        // bucket_count() would miss it because it short-circuits on
        // heavy hits).
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(10, 1, 1, 0.9, 1);
        let mut b: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(10, 1, 1, 0.9, 1);

        a.add(&b"x".to_vec(), 1000);
        b.add(&b"y".to_vec(), 200); // fills b's heavy slot
        b.add(&b"x".to_vec(), 5); // forced into b's lobby

        a.merge(&b).expect("compatible");

        assert_eq!(a.bucket_count(&b"x".to_vec()), 1005);
    }

    #[test]
    fn test_merge_folds_self_lobby_into_incoming_heavy() {
        // self has x in lobby; other has x heavy. Heavy walk must fold
        // self's lobby contribution into the incoming count and clear
        // the lobby so x lives in heavy XOR lobby, never both.
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(10, 1, 1, 0.9, 1);
        let mut b: CuckooTopK<Vec<u8>> = CuckooTopK::with_seed(10, 1, 1, 0.9, 1);

        a.add(&b"y".to_vec(), 200); // fills a's heavy slot
        a.add(&b"x".to_vec(), 5); // forced into a's lobby
        b.add(&b"x".to_vec(), 1000);

        a.merge(&b).expect("compatible");

        assert_eq!(a.bucket_count(&b"x".to_vec()), 1005);
    }

    #[test]
    fn test_merge_priority_queue_reflects_summed_counts() {
        let mut a: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 4, 0.9);
        let mut b: CuckooTopK<Vec<u8>> = CuckooTopK::new(3, 64, 4, 0.9);

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
    fn test_decay_threshold_no_usize_truncation_for_large_count() {
        // On 32-bit targets, `count as usize` would truncate u64 counts
        // greater than u32::MAX, returning a large threshold from the
        // start of the lookup table instead of a tiny one. Verify that
        // for counts > u32::MAX the returned threshold is effectively zero
        // (decay^4_billion underflows to ~0).
        let topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 64, 4, 0.9);
        let huge: u64 = (u32::MAX as u64) + 5000;
        let thr = topk.decay_threshold_for_test(huge);
        assert!(
            thr < u64::MAX / 2,
            "expected ~0 threshold for huge count, got {thr}"
        );
    }

    #[test]
    fn test_decay_threshold_no_powi_i32_overflow_for_huge_count() {
        // `q = count / 1023` once exceeded i32::MAX, casting to i32
        // truncated (not saturated) and produced a negative exponent —
        // `powi(neg_huge)` of a fractional base diverges to ∞ and the
        // threshold saturated to u64::MAX (always decay) instead of 0
        // (never decay). Ensure huge counts still produce a tiny threshold.
        let topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(10, 64, 4, 0.9);
        // q = huge / 1023 well past i32::MAX (~2.15e9).
        let huge: u64 = (i32::MAX as u64) * 2048;
        let thr = topk.decay_threshold_for_test(huge);
        assert!(
            thr < u64::MAX / 2,
            "expected ~0 threshold for huge count, got {thr}"
        );
    }

    #[test]
    fn test_add_with_evicted_returns_displaced_item() {
        // k=2 priority queue. Fill it with two items, then add a third
        // hotter item. The minimum tracked item should be returned with
        // its pre-eviction count.
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(2, 64, 2, 0.9);

        // Pump enough to force promotion into heavy slots so PQ gets
        // populated.
        // New keys into free space: no eviction, but inserted.
        assert_eq!(
            topk.add_with_evicted(&b"a".to_vec(), 5),
            (None, true),
            "first add should insert without evicting"
        );
        assert_eq!(
            topk.add_with_evicted(&b"b".to_vec(), 10),
            (None, true),
            "second add (still under k) should insert without evicting"
        );

        // Sanity-check both items reached the priority queue.
        let list = topk.list();
        assert_eq!(list.len(), 2);
        let (evicted, inserted) = topk.add_with_evicted(&b"c".to_vec(), 20);
        assert_eq!(evicted.expect("expected an eviction"), b"a".to_vec());
        assert!(inserted);

        // PQ now holds b and c.
        let list = topk.list();
        let items: Vec<_> = list.iter().map(|n| n.item.clone()).collect();
        assert!(items.contains(&b"b".to_vec()));
        assert!(items.contains(&b"c".to_vec()));
        assert!(!items.contains(&b"a".to_vec()));
    }

    #[test]
    fn test_add_with_evicted_no_eviction_cases() {
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(2, 64, 2, 0.9);

        // increment == 0 → no work, nothing tracked.
        assert_eq!(topk.add_with_evicted(&b"a".to_vec(), 0), (None, false));

        // PQ not yet full → no eviction, but inserted.
        assert_eq!(topk.add_with_evicted(&b"a".to_vec(), 5), (None, true));
        assert_eq!(topk.add_with_evicted(&b"b".to_vec(), 10), (None, true));

        // Updating an already-tracked item → neither evicted nor inserted.
        assert_eq!(topk.add_with_evicted(&b"a".to_vec(), 1), (None, false));

        // New item with count not high enough to beat the PQ minimum (5)
        // → nothing tracked. Use a fresh sketch so heavy slots are free for
        // promotion.
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(2, 64, 2, 0.9);
        topk.add_with_evicted(&b"hot".to_vec(), 50);
        topk.add_with_evicted(&b"warm".to_vec(), 30);
        assert_eq!(topk.add_with_evicted(&b"cold".to_vec(), 10), (None, false));
    }

    #[test]
    fn test_contains_top_k_distinguishes_tracked_from_sketch_only() {
        let mut topk: CuckooTopK<Vec<u8>> = CuckooTopK::new(1, 1, 1, 0.9);

        topk.add(b"hot".as_slice(), 100);
        assert!(
            topk.contains_top_k(b"hot".as_slice()),
            "hot is the tracked top-1"
        );

        // A weaker item: present in the sketch but not in the top-k.
        topk.add(b"cold".as_slice(), 1);
        assert!(
            !topk.contains_top_k(b"cold".as_slice()),
            "cold is not tracked in the top-k"
        );

        // An item never added at all is neither in the sketch nor top-k.
        assert!(!topk.contains_top_k(b"absent".as_slice()));
    }

    #[test]
    fn test_contains_top_k_borrowed_lookup() {
        let mut topk: CuckooTopK<String> = CuckooTopK::new(10, 100, 4, 0.9);
        topk.add("foo", 5);
        assert!(topk.contains_top_k("foo"));
        assert!(!topk.contains_top_k("bar"));
    }
}
