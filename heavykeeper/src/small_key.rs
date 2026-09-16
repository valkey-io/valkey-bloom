//! `SmallKey`: a 16-byte owned byte string that stores up to 15 bytes inline
//! and spills longer keys to the heap.
//!
//! The priority queue holds one key per tracked item. With `Box<[u8]>` every
//! key is a separate heap allocation (plus allocator rounding) reached
//! through a pointer. Most real keys (IDs, IPv4 addresses, short tokens) fit
//! in 15 bytes, so `SmallKey` keeps those in the slot itself: no allocation,
//! no rounding, no pointer chase on compare. Longer keys cost exactly what a
//! `Box<[u8]>` costs today.
//!
//! Layout (16 bytes, align 8). Byte 15 is the discriminant in both variants:
//!
//! ```text
//! inline:  [ data[0..15]                                  | len  (0..=15) ]
//! heap:    [ ptr (8)          | len u32 (4) | pad (3)     | 0xFF          ]
//! ```

use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ptr::NonNull;

use crate::traits::FromBorrowed;

/// Largest key stored without a heap allocation.
pub const INLINE_CAP: usize = 15;
const HEAP_TAG: u8 = 0xFF;

#[repr(C)]
#[derive(Clone, Copy)]
struct Inline {
    data: [u8; INLINE_CAP],
    len: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Heap {
    ptr: NonNull<u8>,
    len: u32,
    _pad: [u8; 3],
    tag: u8,
}

#[repr(C)]
union Repr {
    inline: Inline,
    heap: Heap,
}

/// Owned byte key, 16 bytes, inline up to [`INLINE_CAP`] bytes.
#[repr(transparent)]
pub struct SmallKey {
    repr: Repr,
}

const _: () = assert!(std::mem::size_of::<SmallKey>() == 16);
const _: () = assert!(std::mem::align_of::<SmallKey>() == 8);
const _: () = assert!(std::mem::size_of::<Inline>() == 16);
const _: () = assert!(std::mem::size_of::<Heap>() == 16);

// SAFETY: a heap `SmallKey` exclusively owns its allocation, exactly like a
// `Box<[u8]>`; nothing is shared or interior-mutable.
unsafe impl Send for SmallKey {}
unsafe impl Sync for SmallKey {}

impl SmallKey {
    /// Copy `bytes` into a new key, inline if it fits.
    pub fn new(bytes: &[u8]) -> Self {
        if bytes.len() <= INLINE_CAP {
            let mut data = [0u8; INLINE_CAP];
            data[..bytes.len()].copy_from_slice(bytes);
            SmallKey {
                repr: Repr {
                    inline: Inline {
                        data,
                        len: bytes.len() as u8,
                    },
                },
            }
        } else {
            let len = u32::try_from(bytes.len()).expect("SmallKey longer than u32::MAX bytes");
            let boxed: Box<[u8]> = Box::from(bytes);
            let ptr = Box::into_raw(boxed) as *mut u8;
            SmallKey {
                repr: Repr {
                    heap: Heap {
                        // SAFETY: Box::into_raw never yields null.
                        ptr: unsafe { NonNull::new_unchecked(ptr) },
                        len,
                        _pad: [0; 3],
                        tag: HEAP_TAG,
                    },
                },
            }
        }
    }

    #[inline]
    fn tag(&self) -> u8 {
        // SAFETY: byte 15 is written by both constructors (inline `len` or
        // heap `tag`), so it is always initialized whichever variant is live.
        unsafe { self.repr.inline.len }
    }

    /// Whether the bytes live on the heap rather than inline.
    #[inline]
    pub fn is_heap(&self) -> bool {
        self.tag() == HEAP_TAG
    }

    /// Heap bytes this key owns beyond its 16 inline bytes: 0 when inline,
    /// the key length when spilled. This is what a memory accountant should
    /// charge per key on top of `size_of::<SmallKey>()`.
    #[inline]
    pub fn heap_bytes(&self) -> usize {
        if self.is_heap() {
            // SAFETY: tag says heap, so the heap variant is live.
            unsafe { self.repr.heap.len as usize }
        } else {
            0
        }
    }

    /// Heap bytes a key of `len` bytes would own, without constructing it.
    #[inline]
    pub const fn heap_bytes_for_len(len: usize) -> usize {
        if len <= INLINE_CAP {
            0
        } else {
            len
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        if self.is_heap() {
            // SAFETY: tag says heap; ptr/len came from a live Box<[u8]> of
            // exactly `len` bytes that this key owns.
            unsafe {
                let h = self.repr.heap;
                std::slice::from_raw_parts(h.ptr.as_ptr(), h.len as usize)
            }
        } else {
            // SAFETY: tag <= INLINE_CAP means the inline variant is live and
            // `len` bytes of `data` were written by the constructor.
            unsafe {
                let i = &self.repr.inline;
                &i.data[..i.len as usize]
            }
        }
    }

    /// Consume the key, returning its bytes. A heap key hands over its
    /// existing allocation; an inline key copies.
    pub fn into_vec(self) -> Vec<u8> {
        if self.is_heap() {
            let this = std::mem::ManuallyDrop::new(self);
            // SAFETY: heap variant is live; we reconstitute the Box we
            // leaked in `new` exactly once (ManuallyDrop prevents a second
            // free via Drop).
            unsafe {
                let h = this.repr.heap;
                let raw = std::ptr::slice_from_raw_parts_mut(h.ptr.as_ptr(), h.len as usize);
                Box::from_raw(raw).into_vec()
            }
        } else {
            self.as_slice().to_vec()
        }
    }
}

impl Drop for SmallKey {
    fn drop(&mut self) {
        if self.is_heap() {
            // SAFETY: heap variant is live and owns the allocation; this is
            // the only place it is freed (into_vec uses ManuallyDrop).
            unsafe {
                let h = self.repr.heap;
                let raw = std::ptr::slice_from_raw_parts_mut(h.ptr.as_ptr(), h.len as usize);
                drop(Box::from_raw(raw));
            }
        }
    }
}

impl Clone for SmallKey {
    fn clone(&self) -> Self {
        SmallKey::new(self.as_slice())
    }
}

impl PartialEq for SmallKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl Eq for SmallKey {}

impl PartialOrd for SmallKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SmallKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

/// Hashes exactly like `[u8]` (and therefore like `Vec<u8>` / `Box<[u8]>`),
/// so a `&[u8]` probe finds a stored `SmallKey` in the lookup table.
impl Hash for SmallKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl Borrow<[u8]> for SmallKey {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for SmallKey {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl FromBorrowed<[u8]> for SmallKey {
    fn from_borrowed(borrowed: &[u8]) -> Self {
        SmallKey::new(borrowed)
    }
}

impl fmt::Debug for SmallKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SmallKey")
            .field("bytes", &self.as_slice())
            .field("heap", &self.is_heap())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn h<T: Hash + ?Sized>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    #[test]
    fn round_trips_every_length_across_the_inline_boundary() {
        for len in 0..=64usize {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            let key = SmallKey::new(&bytes);
            assert_eq!(key.as_slice(), &bytes[..], "len={len}");
            assert_eq!(key.is_heap(), len > INLINE_CAP, "len={len}");
            assert_eq!(key.heap_bytes(), if len > INLINE_CAP { len } else { 0 });
            assert_eq!(SmallKey::heap_bytes_for_len(len), key.heap_bytes());
            let cloned = key.clone();
            assert_eq!(cloned, key);
            assert_eq!(cloned.into_vec(), bytes);
            assert_eq!(key.into_vec(), bytes);
        }
    }

    #[test]
    fn inline_bytes_are_never_misread_as_heap_tag() {
        // An inline key whose data bytes are all 0xFF must not be taken for
        // a heap key: the tag is byte 15, which inline stores `len` in.
        let key = SmallKey::new(&[0xFF; INLINE_CAP]);
        assert!(!key.is_heap());
        assert_eq!(key.as_slice(), &[0xFF; INLINE_CAP]);
        let empty = SmallKey::new(b"");
        assert!(!empty.is_heap());
        assert!(empty.as_slice().is_empty());
    }

    #[test]
    fn hash_eq_ord_match_slice_semantics() {
        for a in [
            &b""[..],
            b"a",
            b"abc",
            b"255.255.255.255",
            b"a much longer key value",
        ] {
            let ka = SmallKey::new(a);
            assert_eq!(h(&ka), h(a), "hash must equal [u8] hash for {a:?}");
            assert_eq!(h(&ka), h(&Box::<[u8]>::from(a)));
            assert_eq!(h(&ka), h(&a.to_vec()));
            let borrowed: &[u8] = ka.borrow();
            assert_eq!(borrowed, a);
            for b in [
                &b""[..],
                b"a",
                b"abd",
                b"255.255.255.254",
                b"a much longer key valuf",
            ] {
                let kb = SmallKey::new(b);
                assert_eq!(ka.cmp(&kb), a.cmp(b), "{a:?} vs {b:?}");
                assert_eq!(ka == kb, a == b);
            }
        }
    }

    #[test]
    fn works_as_a_hashbrown_key_probed_by_slice() {
        use crate::sip::SipState;
        use hashbrown::HashTable;
        let hasher = SipState::with_seed(3);
        let keys: Vec<SmallKey> = [
            &b"short"[..],
            b"exactly15bytes!",
            b"a spilled key over 15 bytes",
        ]
        .into_iter()
        .map(SmallKey::new)
        .collect();
        let mut table: HashTable<usize> = HashTable::with_capacity(keys.len());
        for (i, k) in keys.iter().enumerate() {
            table.insert_unique(hasher.hash_one(k), i, |&j| hasher.hash_one(&keys[j]));
        }
        for (i, probe) in [
            &b"short"[..],
            b"exactly15bytes!",
            b"a spilled key over 15 bytes",
        ]
        .into_iter()
        .enumerate()
        {
            let found = table.find(hasher.hash_one(probe), |&j| keys[j].as_slice() == probe);
            assert_eq!(found, Some(&i), "{probe:?}");
        }
        assert!(table
            .find(hasher.hash_one(&b"missing"[..]), |&j| keys[j].as_slice()
                == b"missing")
            .is_none());
    }
}
