//! Shared byte-serialization error, constants, and readers for all variants.

use ahash::RandomState;
use thiserror::Error;

/// Error returned by every variant's `from_bytes` (aliased per variant).
#[derive(Error, Debug)]
pub enum DeserializeError {
    #[error(
        "Byte stream too short while reading {field}: need {needed} byte(s), have {actual}"
    )]
    UnexpectedEof {
        field: &'static str,
        needed: usize,
        actual: usize,
    },

    #[error("Not a heavykeeper sketch: bad magic bytes {actual:02x?} (expected {expected:02x?})")]
    BadMagic { expected: [u8; 4], actual: [u8; 4] },

    #[error("Payload is a different sketch variant: got tag {actual} (expected {expected})")]
    WrongVariant { expected: u8, actual: u8 },

    #[error("Hasher mismatch: seed produces probe {actual} but payload holds {expected} (wrong seed, or the payload was written with a different ahash version)")]
    HasherMismatch { expected: u64, actual: u64 },

    #[error("Unsupported serialization version {version} (this build expects {expected})")]
    UnsupportedVersion { version: u8, expected: u8 },

    #[error("Invalid {field} value: {detail}")]
    InvalidField { field: &'static str, detail: String },

    #[error("Length mismatch for {field}: payload holds {actual} but expected {expected}")]
    LengthMismatch {
        field: &'static str,
        actual: usize,
        expected: usize,
    },

    #[error("{count} unexpected trailing byte(s) after the sketch payload")]
    TrailingBytes { count: usize },
}

/// Magic tag at the start of every serialized sketch (`b"HVYK"`).
pub(crate) const MAGIC: [u8; 4] = *b"HVYK";
/// On-disk format version. Bump whenever the byte layout changes.
pub(crate) const VERSION: u8 = 1;
/// Probe hashed at serialize time to detect a wrong seed on load.
///
/// `ahash` output is not stable across CPU architectures or `ahash` versions,
/// so a payload only loads on the same architecture and `ahash` version that
/// wrote it; otherwise the probe mismatches and load fails with `HasherMismatch`.
pub(crate) const SERIALIZE_HASHER_PROBE: &[u8] = b"heavykeeper-serialize-hasher-probe";
/// Bytes per serialized cell: `(fingerprint: u64, count: u64)`.
pub(crate) const CELL_SIZE: usize = 16;
/// Bytes in a serialized `fastrand::Rng` state (its 64-bit seed, little-endian).
pub(crate) const RNG_STATE_SIZE: usize = 8;

#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub(crate) struct Cell {
    pub(crate) fingerprint: u64,
    pub(crate) count: u64,
}

/// Parse a `CELL_SIZE`-aligned slice into a boxed cell array.
pub(crate) fn parse_cells(slice: &[u8]) -> Box<[Cell]> {
    slice
        .chunks_exact(CELL_SIZE)
        .map(|chunk| Cell {
            fingerprint: u64::from_le_bytes(chunk[0..8].try_into().expect("8 bytes")),
            count: u64::from_le_bytes(chunk[8..16].try_into().expect("8 bytes")),
        })
        .collect()
}

/// A forward-only cursor over a serialized payload. Every read is
/// bounds-checked and advances the cursor, so `from_bytes` never touches raw
/// offsets and a truncated stream fails with a precise `UnexpectedEof`.
pub(crate) struct ByteReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ByteReader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    /// Read `n` bytes, advancing the cursor.
    pub(crate) fn take(
        &mut self,
        n: usize,
        field: &'static str,
    ) -> Result<&'a [u8], DeserializeError> {
        let available = self.bytes.len().saturating_sub(self.pos);
        if available < n {
            return Err(DeserializeError::UnexpectedEof {
                field,
                needed: n,
                actual: available,
            });
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    /// Read a fixed-size byte array.
    pub(crate) fn take_array<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], DeserializeError> {
        Ok(self.take(N, field)?.try_into().expect("slice is N bytes"))
    }

    /// Read a single byte.
    pub(crate) fn take_u8(&mut self, field: &'static str) -> Result<u8, DeserializeError> {
        Ok(self.take(1, field)?[0])
    }

    /// Read a little-endian `u64`.
    pub(crate) fn take_u64(&mut self, field: &'static str) -> Result<u64, DeserializeError> {
        Ok(u64::from_le_bytes(self.take_array::<8>(field)?))
    }

    /// Read a little-endian `u64` and narrow it to `usize`, erroring on overflow.
    pub(crate) fn take_usize(&mut self, field: &'static str) -> Result<usize, DeserializeError> {
        let value = self.take_u64(field)?;
        usize::try_from(value).map_err(|_| DeserializeError::InvalidField {
            field,
            detail: format!("value {value} exceeds usize range on this platform"),
        })
    }

    /// Verify the fixed header shared by every variant: magic, `variant` tag,
    /// version, and the hasher probe. Rebuilds the hasher from `seed` and
    /// rejects a wrong seed before any params are parsed.
    pub(crate) fn read_header(&mut self, variant: u8, seed: u64) -> Result<(), DeserializeError> {
        let magic = self.take_array::<4>("magic")?;
        if magic != MAGIC {
            return Err(DeserializeError::BadMagic {
                expected: MAGIC,
                actual: magic,
            });
        }
        let got_variant = self.take_u8("variant")?;
        if got_variant != variant {
            return Err(DeserializeError::WrongVariant {
                expected: variant,
                actual: got_variant,
            });
        }
        let version = self.take_u8("version")?;
        if version != VERSION {
            return Err(DeserializeError::UnsupportedVersion {
                version,
                expected: VERSION,
            });
        }
        let expected_probe = self.take_u64("hasher_probe")?;
        let actual_probe =
            RandomState::with_seeds(seed, seed, seed, seed).hash_one(SERIALIZE_HASHER_PROBE);
        if actual_probe != expected_probe {
            return Err(DeserializeError::HasherMismatch {
                expected: expected_probe,
                actual: actual_probe,
            });
        }
        Ok(())
    }

    /// Read and validate the params shared by every variant
    pub(crate) fn read_params(&mut self) -> Result<(usize, usize, f64, usize), DeserializeError> {
        let width = self.take_usize("width")?;
        let depth = self.take_usize("depth")?;
        let decay = f64::from_bits(self.take_u64("decay")?);
        let top_items = self.take_usize("top_items")?;

        if width < 1 {
            return Err(DeserializeError::InvalidField {
                field: "width",
                detail: format!("must be >= 1, got {width}"),
            });
        }
        if depth < 1 {
            return Err(DeserializeError::InvalidField {
                field: "depth",
                detail: format!("must be >= 1, got {depth}"),
            });
        }
        if !decay.is_finite() || !(0.0..=1.0).contains(&decay) {
            return Err(DeserializeError::InvalidField {
                field: "decay",
                detail: format!("must be a finite value in 0.0..=1.0, got {decay}"),
            });
        }

        Ok((width, depth, decay, top_items))
    }

    /// Reject any bytes left after the payload.
    pub(crate) fn finish(&self) -> Result<(), DeserializeError> {
        if self.pos != self.bytes.len() {
            return Err(DeserializeError::TrailingBytes {
                count: self.bytes.len() - self.pos,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEED: u64 = 42;
    const VARIANT: u8 = 0;

    /// Build a valid header (magic, variant, version, probe) for `SEED`.
    fn header(variant: u8) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC);
        out.push(variant);
        out.push(VERSION);
        let probe =
            RandomState::with_seeds(SEED, SEED, SEED, SEED).hash_one(SERIALIZE_HASHER_PROBE);
        out.extend_from_slice(&probe.to_le_bytes());
        out
    }

    #[test]
    fn read_header_rejects_bad_magic() {
        let mut bytes = header(VARIANT);
        bytes[0] ^= 0xff;
        let mut r = ByteReader::new(&bytes);
        assert!(matches!(
            r.read_header(VARIANT, SEED),
            Err(DeserializeError::BadMagic { .. })
        ));
    }

    #[test]
    fn read_header_rejects_wrong_variant() {
        let bytes = header(VARIANT + 1);
        let mut r = ByteReader::new(&bytes);
        assert!(matches!(
            r.read_header(VARIANT, SEED),
            Err(DeserializeError::WrongVariant { .. })
        ));
    }

    #[test]
    fn read_header_rejects_unsupported_version() {
        let mut bytes = header(VARIANT);
        bytes[5] = VERSION + 1;
        let mut r = ByteReader::new(&bytes);
        assert!(matches!(
            r.read_header(VARIANT, SEED),
            Err(DeserializeError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn read_header_rejects_wrong_seed() {
        let bytes = header(VARIANT);
        let mut r = ByteReader::new(&bytes);
        assert!(matches!(
            r.read_header(VARIANT, SEED + 1),
            Err(DeserializeError::HasherMismatch { .. })
        ));
    }

    #[test]
    fn read_header_rejects_truncated() {
        let bytes = header(VARIANT);
        let mut r = ByteReader::new(&bytes[..bytes.len() - 1]);
        assert!(matches!(
            r.read_header(VARIANT, SEED),
            Err(DeserializeError::UnexpectedEof { .. })
        ));
    }

    #[test]
    fn read_params_validates_scalars() {
        // width, depth, decay, top_items
        let mut ok = Vec::new();
        ok.extend_from_slice(&8u64.to_le_bytes());
        ok.extend_from_slice(&4u64.to_le_bytes());
        ok.extend_from_slice(&0.9f64.to_bits().to_le_bytes());
        ok.extend_from_slice(&10u64.to_le_bytes());
        let mut r = ByteReader::new(&ok);
        assert_eq!(r.read_params().unwrap(), (8, 4, 0.9, 10));

        // width = 0 is rejected.
        let mut bad = ok.clone();
        bad[0..8].copy_from_slice(&0u64.to_le_bytes());
        let mut r = ByteReader::new(&bad);
        assert!(matches!(
            r.read_params(),
            Err(DeserializeError::InvalidField { field: "width", .. })
        ));

        // depth = 0 is rejected.
        let mut bad = ok.clone();
        bad[8..16].copy_from_slice(&0u64.to_le_bytes());
        let mut r = ByteReader::new(&bad);
        assert!(matches!(
            r.read_params(),
            Err(DeserializeError::InvalidField { field: "depth", .. })
        ));

        // out-of-range decay is rejected.
        let mut bad = ok.clone();
        bad[16..24].copy_from_slice(&2.0f64.to_bits().to_le_bytes());
        let mut r = ByteReader::new(&bad);
        assert!(matches!(
            r.read_params(),
            Err(DeserializeError::InvalidField { field: "decay", .. })
        ));
    }

    #[test]
    fn finish_rejects_trailing_bytes() {
        let bytes = [0u8; 2];
        let mut r = ByteReader::new(&bytes);
        r.take(1, "x").unwrap();
        assert!(matches!(
            r.finish(),
            Err(DeserializeError::TrailingBytes { count: 1 })
        ));
    }
}
