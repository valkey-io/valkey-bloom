# heavykeeper-rs

[![Crates.io][crates-badge]][crates-url]
[![MIT / Apache 2.0 licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/heavykeeper.svg
[crates-url]: https://crates.io/crates/heavykeeper
[license-badge]: https://img.shields.io/crates/l/heavykeeper.svg
[license-url]: https://github.com/pmcgleenon/heavykeeper-rs/blob/master/LICENSE
[actions-badge]: https://github.com/pmcgleenon/heavykeeper-rs/actions/workflows/rust.yml/badge.svg
[actions-url]: https://github.com/pmcgleenon/heavykeeper-rs/actions?query=workflow%3Arust+branch%3Amain


[📖 Docs](https://docs.rs/heavykeeper)

Top-K Heavykeeper algorithm for Top-K elephant flows

This is based on the [paper](https://www.usenix.org/system/files/conference/atc18/atc18-gong.pdf)
HeavyKeeper: An Accurate Algorithm for Finding Top-k Elephant Flows
by Junzhi Gong, Tong Yang, Haowei Zhang, and Hao Li, Peking University;
Steve Uhlig, Queen Mary, University of London; Shigang Chen, University of Florida;
Lorna Uden, Staffordshire University; Xiaoming Li, Peking University

# Example

See [examples/basic.rs](examples/basic.rs) for a complete example, or [examples/ip_files.rs](examples/ip_files.rs) for an example of counting top-k IP flows in a file.

Basic usage:
```rust
use heavykeeper::TopK;

// create a new TopK with k=10, width=1000, depth=4, decay=0.9
let mut topk: TopK<String> = TopK::new(10, 1000, 4, 0.9);

// add some items
topk.add("example item", 5);
topk.add("another item", 1);

// check the counts
for node in topk.list() {
    println!("{} {}", node.item, node.count);
}
```

# Variants

The crate ships three top-K sketches that share the same public API
(`new` / `with_seed` / `with_hasher` / `builder` / `add` / `count` /
`contains` / `list` / `merge`):

| Sketch          | Layout                                           | Insert throughput on Zipf(s=1.2), 1M | Recall @ φ=0.0005 |
| --------------- | ------------------------------------------------ | -----------------------------------: | ----------------: |
| `TopK`          | `depth` independent rows × `width` buckets        |                       21.0 Melem / s |             0.942 |
| `BucketedTopK`  | one bucket of `depth` cells per key               |                       29.0 Melem / s |             0.985 |
| `CuckooTopK`    | per-bucket lobby + `depth` heavy slots, 2-bucket cuckoo |                       29.8 Melem / s |             1.000 |

Numbers are from `cargo bench --bench topk_vs_bucketed` at `K=100,
width=4096, depth=4, decay=0.9` on `u64` keys. Recall is from
`tests/accuracy_compare.rs` (paper-style heavy-hitter test, φ = 0.0005,
1 M Zipf samples).

`TopK` is the canonical implementation from the paper, with its
accuracy bounds. `BucketedTopK` and `CuckooTopK` are derived variants —
they don't carry the paper's row-independence accuracy bounds, but the
empirical accuracy on Zipf streams is competitive and often better.

Pick by workload:

- **`TopK`** — when you want the published algorithm and its bounds.
- **`BucketedTopK`** — best general-purpose insert throughput; closest to `TopK`'s cost model with a single bucket per key.
- **`CuckooTopK`** — best accuracy *and* throughput on heavy-hitter-skewed traffic (the elephant-flow use case). Each bucket has a single lobby cell with probabilistic decay plus `depth` non-decaying heavy slots; promoted items live in one of two cuckoo candidate buckets and are re-homed on collision via a kick chain (bound configurable via `CuckooBuilder::max_kicks`, default 8).

All three support seedable construction, custom hashers, and `merge`
between compatible instances. Errors are returned via
`BuilderError`/`MergeError` enums; the infallible constructors
(`new`, `with_seed`, `with_hasher`) trust the caller. Bucket indexing
uses an AND-mask fast-path when `width` is a power of two; pick
power-of-two widths in production for the best per-add cost.

# Real-world packet trace

`examples/ip_files.rs` runs all three sketches over a CAIDA-style trace
(27.5 M packets, 1.03 M distinct 13-byte flow keys = src IP : src port →
dst IP : dst port + protocol). Same `K=1000, decay=0.95`, equal cell
budgets across variants:

| Sketch          | Width × depth          | Throughput | hit\_ratio | ARE on reported | ARE on true top-K |
| --------------- | ---------------------- | ---------: | ---------: | --------------: | ----------------: |
| `TopK`          | 16384 × 2              | 14.1 Mpps  |     0.9270 |          0.0050 |            0.0745 |
| `BucketedTopK`  | 8192 × 4               | 18.1 Mpps  |     0.9860 |          0.0035 |            0.0129 |
| `CuckooTopK`    | 8192 × (1 + 4 heavy)   | 17.0 Mpps  | **0.9990** |      **0.0012** |        **0.0012** |

# Other HeavyKeeper Implementations

| Name                       | Language | Github Repo                                                                  |
|----------------------------|----------|------------------------------------------------------------------------------|
| SegmentIO                  | Go       | https://github.com/segmentio/topk                                            |
| Aegis                      | Go       | https://github.com/go-kratos/aegis/blob/main/topk/heavykeeper.go             |
| Tomasz Kolaj               | Go       | https://github.com/migotom/heavykeeper                                       |
| HeavyKeeper Paper          | C++      | https://github.com/papergitkeeper/heavy-keeper-project                       |
| Jigsaw-Sketch              | C++      | https://github.com/duyang92/jigsaw-sketch-paper/tree/main/CPU/HeavyKeeper    |
| Redis Bloom Heavykeeper    | C        | https://github.com/RedisBloom/RedisBloom/blob/master/src/topk.c              |
| Count-Min-Sketch           | Rust     | https://github.com/alecmocatta/streaming_algorithms                          |
| Sliding Window HeavyKeeper | Go       | https://github.com/keilerkonzept/topk                                        |

# Running

## Word Count Example

A word count program that demonstrates the HeavyKeeper algorithm can be found at [`examples/word_count.rs`](examples/word_count.rs).

### Usage
```bash
cargo build --example word_count --release
target/release/examples/word_count -k 10 -w 8192 -d 2 -y 0.95 -f data/war_and_peace.txt
```

## Running the basic example 
```bash
cargo run --example basic --release
```

## Running the IPv4 example 
```bash
cargo run --example ip_files --release
```

## Run the benchmarks
```bash
cargo bench
```

## Benchmark the sample word count app
```bash
hyperfine 'target/release/examples/word_count -k 10 -w 8192 -d 2 -y 0.95 -f data/war_and_peace.txt'
```

## Test Data

For information about test data format and how to obtain or generate test data, please see [data/README.md](data/README.md).

# License
This project is dual licensed under the Apache/MIT license.   
