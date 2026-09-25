// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: Copyright (c) 2025 flowstats Contributors
// SPDX-FileContributor: https://github.com/vnvo/flowstats/blob/v0.1.2/src/math.rs

//! Math function wrappers for std/no_std compatibility
//!
//! Uses standard library math when available, falls back to libm for no_std.

#[cfg(feature = "std")]
#[inline]
pub fn ln(x: f64) -> f64 {
    x.ln()
}

#[cfg(not(feature = "std"))]
#[inline]
pub fn ln(x: f64) -> f64 {
    libm::log(x)
}


#[cfg(not(feature = "std"))]
#[inline]
pub fn log2(x: f64) -> f64 {
    libm::log2(x)
}

#[cfg(not(feature = "std"))]
#[inline]
pub fn exp2(x: f64) -> f64 {
    libm::exp2(x)
}

#[cfg(not(feature = "std"))]
#[inline]
pub fn sqrt(x: f64) -> f64 {
    libm::sqrt(x)
}

#[cfg(not(feature = "std"))]
#[inline]
pub fn asin(x: f64) -> f64 {
    libm::asin(x)
}

#[cfg(feature = "std")]
#[inline]
pub fn ceil(x: f64) -> f64 {
    x.ceil()
}

#[cfg(not(feature = "std"))]
#[inline]
pub fn ceil(x: f64) -> f64 {
    libm::ceil(x)
}

#[cfg(feature = "std")]
#[inline]
#[allow(dead_code)]
pub fn floor(x: f64) -> f64 {
    x.floor()
}

#[cfg(not(feature = "std"))]
#[inline]
pub fn floor(x: f64) -> f64 {
    libm::floor(x)
}

#[cfg(not(feature = "std"))]
#[inline]
pub fn powi(x: f64, n: i32) -> f64 {
    libm::pow(x, n as f64)
}
