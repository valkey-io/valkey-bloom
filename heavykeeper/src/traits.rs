//! Generic storage-width traits for fingerprint and counter cells.

use std::fmt::Debug;

/// A fingerprint stored in each cell. Truncates a full `u64` hash into the
/// chosen storage width.
///
/// Cuckoo note: implementations truncate (not shift) so that
/// `bucket_pair(fp.as_u64())` remains consistent with the original hash for
/// widths that cover the bucket index space.
pub trait Fingerprint: Copy + Default + PartialEq + Eq + Debug + Send + Sync + 'static {
    fn from_hash(h: u64) -> Self;
    fn as_u64(self) -> u64;
    fn to_le_bytes_into(self, buf: &mut Vec<u8>);
    fn from_le_bytes(buf: &[u8]) -> Self;
    const SIZE: usize;
}

/// A saturating counter stored in each cell.
pub trait Counter:
    Copy + Default + PartialOrd + Ord + PartialEq + Eq + Debug + Send + Sync + 'static
{
    const ZERO: Self;
    const MAX: Self;
    fn from_u64(v: u64) -> Self;
    fn as_u64(self) -> u64;
    fn saturating_add(self, rhs: Self) -> Self;
    fn saturating_sub(self, rhs: Self) -> Self;
    fn to_le_bytes_into(self, buf: &mut Vec<u8>);
    fn from_le_bytes(buf: &[u8]) -> Self;
    const SIZE: usize;
}

macro_rules! impl_fingerprint {
    ($ty:ty) => {
        impl Fingerprint for $ty {
            #[inline]
            fn from_hash(h: u64) -> Self {
                h as Self
            }
            #[inline]
            fn as_u64(self) -> u64 {
                self as u64
            }
            #[inline]
            fn to_le_bytes_into(self, buf: &mut Vec<u8>) {
                buf.extend_from_slice(&self.to_le_bytes());
            }
            #[inline]
            fn from_le_bytes(buf: &[u8]) -> Self {
                <$ty>::from_le_bytes(buf.try_into().expect("fp bytes"))
            }
            const SIZE: usize = std::mem::size_of::<$ty>();
        }
    };
}

macro_rules! impl_counter {
    ($ty:ty) => {
        impl Counter for $ty {
            const ZERO: Self = 0;
            const MAX: Self = <$ty>::MAX;
            #[inline]
            fn from_u64(v: u64) -> Self {
                if v > Self::MAX as u64 {
                    Self::MAX
                } else {
                    v as Self
                }
            }
            #[inline]
            fn as_u64(self) -> u64 {
                self as u64
            }
            #[inline]
            fn saturating_add(self, rhs: Self) -> Self {
                <$ty>::saturating_add(self, rhs)
            }
            #[inline]
            fn saturating_sub(self, rhs: Self) -> Self {
                <$ty>::saturating_sub(self, rhs)
            }
            #[inline]
            fn to_le_bytes_into(self, buf: &mut Vec<u8>) {
                buf.extend_from_slice(&self.to_le_bytes());
            }
            #[inline]
            fn from_le_bytes(buf: &[u8]) -> Self {
                <$ty>::from_le_bytes(buf.try_into().expect("cnt bytes"))
            }
            const SIZE: usize = std::mem::size_of::<$ty>();
        }
    };
}

impl_fingerprint!(u64);
impl_fingerprint!(u32);
impl_fingerprint!(u16);

impl_counter!(u64);
impl_counter!(u32);
impl_counter!(u16);
