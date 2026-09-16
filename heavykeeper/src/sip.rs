//! Portable seeded hasher for sketch placement and lookups.

use siphasher::sip::SipHasher13;
use std::hash::{BuildHasher, Hash};

/// Seeded `BuildHasher` producing SipHash-1-3.
///
/// SipHash is a frozen specification: a given key produces identical output on
/// every architecture, endianness, and crate version.
#[derive(Clone, Debug)]
pub struct SipState {
    k0: u64,
    k1: u64,
}

impl SipState {
    pub fn with_seed(seed: u64) -> Self {
        Self { k0: seed, k1: !seed }
    }

    /// Build from a random seed. For sketches that are never serialized (or
    /// whose seed is recorded elsewhere); seeded construction is the norm.
    pub fn random() -> Self {
        Self::with_seed(fastrand::u64(..))
    }

    /// Hash a single value. Inherent so call sites do not need the
    /// `BuildHasher` trait in scope.
    #[inline]
    pub fn hash_one<T: Hash>(&self, value: T) -> u64 {
        BuildHasher::hash_one(self, value)
    }
}

impl BuildHasher for SipState {
    type Hasher = SipHasher13;

    #[inline]
    fn build_hasher(&self) -> SipHasher13 {
        SipHasher13::new_with_keys(self.k0, self.k1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pins portability: SipHash-1-3 with fixed keys is a frozen spec, so these
    // values must hold on every architecture, endianness, and crate version.
    // If this test ever fails, sketch hashing has silently changed and the
    // serialization VERSION must be bumped.
    #[test]
    fn test_hash_values_are_stable_and_portable() {
        assert_eq!(SipState::with_seed(0).hash_one(0u64), 2139874725565397917);
        assert_eq!(SipState::with_seed(42).hash_one(0u64), 13226300229752186361);
        assert_eq!(
            SipState::with_seed(42).hash_one(b"heavykeeper".as_slice()),
            3987788251188275386
        );
    }

    #[test]
    fn test_hash_values_are_equivalent() {
        let a = SipState::with_seed(7);
        let b = SipState::with_seed(7);
        let c = SipState::with_seed(8);
        assert_eq!(a.hash_one("x"), b.hash_one("x"));
        assert_ne!(a.hash_one("x"), c.hash_one("x"));
    }
}
