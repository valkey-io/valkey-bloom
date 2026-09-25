// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: Copyright (c) 2025 flowstats Contributors
// SPDX-FileContributor: https://github.com/vnvo/flowstats/blob/v0.1.2/src/traits.rs

//! Core traits for streaming algorithms
//!
//! All sketches implement the base [`Sketch`] trait, with specialized traits
//! for different algorithm families (cardinality, frequency, quantiles, etc.)

use core::fmt::Debug;

#[cfg(feature = "std")]
use std::{string::String};

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

/// Error during sketch merge operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeError {
    /// Sketches have incompatible configurations
    IncompatibleConfig {
        expected: String,
        found: String,
    },
    /// Sketches have incompatible versions
    VersionMismatch {
        expected: u32,
        found: u32,
    },
}

impl core::fmt::Display for MergeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MergeError::IncompatibleConfig { expected, found } => {
                write!(f, "incompatible config: expected {}, found {}", expected, found)
            }
            MergeError::VersionMismatch { expected, found } => {
                write!(f, "version mismatch: expected {}, found {}", expected, found)
            }
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for MergeError {}

/// Error during sketch decoding
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Input buffer too short
    BufferTooShort { expected: usize, found: usize },
    /// Invalid magic number or header
    InvalidHeader,
    /// Unsupported version
    UnsupportedVersion(u32),
    /// Corrupted data
    Corrupted(String),
}

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            DecodeError::BufferTooShort { expected, found } => {
                write!(f, "buffer too short: expected {}, found {}", expected, found)
            }
            DecodeError::InvalidHeader => write!(f, "invalid header"),
            DecodeError::UnsupportedVersion(v) => write!(f, "unsupported version: {}", v),
            DecodeError::Corrupted(msg) => write!(f, "corrupted data: {}", msg),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for DecodeError {}

/// Error bounds for a sketch estimate
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ErrorBounds {
    /// Lower bound of the estimate
    pub lower: f64,
    /// Point estimate
    pub estimate: f64,
    /// Upper bound of the estimate
    pub upper: f64,
    /// Confidence level (e.g., 0.95 for 95%)
    pub confidence: f64,
}

impl ErrorBounds {
    /// Create new error bounds
    pub fn new(lower: f64, estimate: f64, upper: f64, confidence: f64) -> Self {
        Self {
            lower,
            estimate,
            upper,
            confidence,
        }
    }

    /// Check if a value falls within bounds
    pub fn contains(&self, value: f64) -> bool {
        value >= self.lower && value <= self.upper
    }

    /// Width of the confidence interval
    pub fn width(&self) -> f64 {
        self.upper - self.lower
    }

    /// Relative width (width / estimate)
    pub fn relative_width(&self) -> f64 {
        if self.estimate == 0.0 {
            0.0
        } else {
            self.width() / self.estimate
        }
    }
}

/// Core trait for all streaming sketches
pub trait Sketch: Clone + Debug {
    /// The type of item this sketch processes
    type Item: ?Sized;

    /// Add an item to the sketch
    fn update(&mut self, item: &Self::Item);

    /// Merge another sketch into this one
    ///
    /// Returns an error if sketches are incompatible
    fn merge(&mut self, other: &Self) -> Result<(), MergeError>;

    /// Reset sketch to empty state
    fn clear(&mut self);

    /// Memory usage in bytes
    fn size_bytes(&self) -> usize;

    /// Number of items processed
    fn count(&self) -> u64;

    /// Check if sketch is empty
    fn is_empty(&self) -> bool {
        self.count() == 0
    }
}

/// Cardinality (distinct count) estimation sketches
pub trait CardinalitySketch: Sketch {
    /// Estimate number of distinct items seen
    fn estimate(&self) -> f64;

    /// Get error bounds at given confidence level (0.0 to 1.0)
    fn error_bounds(&self, confidence: f64) -> ErrorBounds;

    /// Relative standard error (RSE) of the estimate
    ///
    /// RSE = standard_error / true_value ≈ 1.04 / sqrt(m) for HLL
    fn relative_error(&self) -> f64;

    /// Estimate with default 95% confidence bounds
    fn estimate_with_bounds(&self) -> ErrorBounds {
        self.error_bounds(0.95)
    }
}

/// Frequency estimation sketches
pub trait FrequencySketch: Sketch {
    /// Estimate frequency of an item
    fn estimate_frequency(&self, item: &Self::Item) -> u64;

    /// Check if frequency exceeds threshold
    fn exceeds_threshold(&self, item: &Self::Item, threshold: u64) -> bool {
        self.estimate_frequency(item) >= threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_bounds() {
        let bounds = ErrorBounds::new(90.0, 100.0, 110.0, 0.95);
        
        assert!(bounds.contains(100.0));
        assert!(bounds.contains(90.0));
        assert!(bounds.contains(110.0));
        assert!(!bounds.contains(89.0));
        assert!(!bounds.contains(111.0));
        
        assert_eq!(bounds.width(), 20.0);
        assert!((bounds.relative_width() - 0.2).abs() < 0.001);
    }
}
