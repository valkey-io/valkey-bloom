# Local Modifications to heavykeeper

Changes applied to the vendored `heavykeeper` crate, in the order they were
committed on top of the upstream `main` import. Each row explains what the
change does and why we want it. Commit links point at
`detemmienation/valkey-bloom`.

Baseline: [Integrate heavykeeper-rs as a local path dependency](https://github.com/detemmienation/valkey-bloom/commit/12580474d60d9729dc96859010b99684d723f8fe)
vendors upstream `main` (default `u64` cells) so we can modify it in-tree.

| # | Change | Why | Notes / follow-up | Commit |
|---|--------|-----|-------------------|--------|
| 1 | Serialization support | Byte (de)serialization for `CuckooTopK`, needed for RDB/AOF persistence and merge-compatible restore. | — | [804778d](https://github.com/detemmienation/valkey-bloom/commit/804778dbce949452b5b76fe141b6f0dfab716b08) |
| 2 | Defrag support | Relocate the sketch's heap allocations through Valkey's defrag allocator via a safe `Reallocator` trait. | — | [4756214](https://github.com/detemmienation/valkey-bloom/commit/4756214193aab515a87c37c8075f7bf3ec30241c) |
| 3 | Generic `Fingerprint`/`Counter` types | Make cell fingerprint and counter widths generic (`F`/`C`), defaulting to `u64`. Enables narrower cells without forking the type. | Optional precision: `CuckooTopK<Vec<u8>>` stays `u64`; switching widths is just the type alias. | [27b90f7](https://github.com/detemmienation/valkey-bloom/commit/27b90f72de910ecd86147a2f48cc5d63493cdc12) |
| 4 | `u32` fingerprint + counter | Instantiate the TopK sketch as `CuckooTopK<Vec<u8>, u32, u32>`, halving per-cell memory from 16 to 8 bytes. | Smaller chance someone needs `u64`. Consider exposing width as a config/command argument. Evaluate whether Valkey's counter (or other TopK counters) need `u32` vs `u64`. | [c96a1cc](https://github.com/detemmienation/valkey-bloom/commit/c96a1cc67492cd49c1b5b1144a385c3b73477f01) |
| 5 | Single-copy priority queue + drop free-slot list | Store each tracked item once (`hashbrown::HashTable<u32>` maps to a slot index instead of duplicating the key), halving per-item memory in the PQ. A full queue now replaces the min slot in place instead of deleting and reusing slots, so the `free_slots` list is no longer needed and is removed. | Item stored once instead of twice; no free-slot. | [5a85b9c](https://github.com/detemmienation/valkey-bloom/commit/5a85b9c63d9069b0694139f61e410f222d48ca8b) |
| 6 | Narrow `sequence` to `u32` | The PQ tie-break `sequence` fits in `u32`; halves that field. Uses `wrapping_add` to avoid overflow panics. | Only matters past ~4.3B inserts, at which point tie ordering can wrap (acceptable, matches upstream). | [41fa47b](https://github.com/detemmienation/valkey-bloom/commit/41fa47b9f638da3e76220a17b4d602514f963731) |
| 7 | Remove decay-threshold lookup table | Drop the 1024-entry `u64` precomputed table; compute `decay^count` directly with `powf`. Saves the table's memory (~8 KB/sketch). | Kept because it most benefits small `k`. Trade-off: a `powf` per lobby-decay check instead of an O(1) table lookup. | [97a0735](https://github.com/detemmienation/valkey-bloom/commit/97a07356cd99eb3b34d937270fa4afd2ab389968) |
| 8 | Linear-scan PQ lookup (default) | Add a runtime lookup strategy: `CuckooTopK` defaults to a linear scan over `item_store` instead of the hash table, leaving the table unallocated. Better cache locality for small `k` and no hashing on the lookup path. | Fine as default, but should be exposed as a config or command argument so large-`k` users can opt into the hash table. Not yet persisted to RDB (safe while all keys default to linear). | [d508c78](https://github.com/detemmienation/valkey-bloom/commit/d508c780a4687be86fccf96c0bc46f4ddbdc8ead) |
