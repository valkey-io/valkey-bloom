// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2025 flowstats Contributors
// SPDX-FileContributor: https://github.com/vnvo/flowstats/blob/v0.1.2/src/mod.rs

//! Frequency estimation algorithms
//!
//! This module provides implementations of sketches for estimating item
//! frequencies in a data stream.
//!
//! # Algorithms
//!
//! - [`CountMinSketch`]: Classic count-min sketch with optional conservative update
//! - [`SpaceSaving`]: Top-K / heavy hitters tracking
//!
//! # Example
//!
//! ```
//! use flowstats::frequency::CountMinSketch;
//! use flowstats::traits::FrequencySketch;
//!
//! let mut cms = CountMinSketch::new(0.01, 0.001); // 1% error, 0.1% probability
//!
//! cms.add(b"item1", 5);
//! cms.add(b"item2", 3);
//!
//! let count = cms.estimate(b"item1");
//! println!("Estimated count: {}", count);
//! ```

mod count_min;
#[cfg(feature = "std")]
mod space_saving;

pub use count_min::CountMinSketch;

#[cfg(feature = "std")]
pub use space_saving::SpaceSaving;
