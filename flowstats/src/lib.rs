// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 flowstats Contributors
// SPDX-FileContributor: https://github.com/vnvo/flowstats/blob/v0.1.2/src/lib.rs

//! # Flowstats
//!
//! Production-grade streaming algorithms for Rust.
//!
//! Flowstats provides high-performance implementations of probabilistic data structures
//! and streaming algorithms, designed for real-time analytics and large-scale data processing.
//!
//! ## Features
//!
//! - **Cardinality Estimation**: Count distinct elements with HyperLogLog
//! - **Frequency Estimation**: Track item frequencies with Count-Min Sketch
//! - **Heavy Hitters**: Find top-K elements with Space-Saving
//! - **Quantile Estimation**: Compute percentiles with t-digest
//! - **Full Mergeability**: All sketches support distributed merge operations
//! - **Error Bounds**: Formal guarantees on approximation accuracy
//!
//! ## Quick Start
//!
//! ```rust
//! use flowstats::prelude::*;
//!
//! // Count distinct users
//! let mut hll = HyperLogLog::new(14);
//! for user_id in ["alice", "bob", "charlie", "alice"] {
//!     hll.insert(user_id);
//! }
//! println!("Distinct users: ~{}", hll.estimate());
//!
//! // Track request latencies
//! let mut digest = TDigest::new(100.0);
//! for latency in [12.5, 45.2, 23.1, 67.8, 15.3] {
//!     digest.add(latency);
//! }
//! println!("p99 latency: {:?}", digest.quantile(0.99));
//!  
//! ```
//!
//! ## Distributed Computing
//!
//! All sketches implement the [`Sketch`](traits::Sketch) trait which includes
//! a `merge` operation, allowing sketches to be combined across distributed workers:
//!
//! ```rust
//! use flowstats::cardinality::HyperLogLog;
//! use flowstats::traits::Sketch;
//!
//! let mut worker1 = HyperLogLog::new(14);
//! let mut worker2 = HyperLogLog::new(14);
//!
//! // Each worker processes its partition
//! worker1.insert("user_a");
//! worker2.insert("user_b");
//!
//! // Merge results
//! worker1.merge(&worker2).unwrap();
//! ```
//!
//! ## Feature Flags
//!
//! Algorithm families (pick what you need):
//! - `cardinality` (default): HyperLogLog for distinct counting
//! - `frequency` (default): Count-Min Sketch, Space-Saving (tbd)
//! - `quantiles` (default): t-digest for percentiles
//! - `membership`: (default) Bloom filter
//! - `sampling`: Reservoir and weighted sampling (tbd)
//! - `sets`: Theta sketch for set operations (tbd)
//! - `statistics`: Running moments, entropy (tbd)
//! - `full`: Enable all algorithm families
//!
//! Platform features:
//! - `std` (default): Standard library support
//! - `serde`: Enable serialization

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(not(feature = "std"))]
extern crate alloc;

mod math;
pub mod traits;

#[cfg(feature = "frequency")]
#[cfg_attr(docsrs, doc(cfg(feature = "frequency")))]
pub mod frequency;

pub mod prelude {
    pub use crate::traits::*;

    #[cfg(feature = "frequency")]
    pub use crate::frequency::CountMinSketch;

}

#[cfg(feature = "frequency")]
pub use frequency::CountMinSketch;

