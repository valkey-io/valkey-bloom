use crate::cuckoo::utils::{CuckooFilter, CuckooObject, CUCKOO_OBJECT_VERSION};
use crate::wrapper::cuckoo_callback;
use std::os::raw::c_int;
use valkey_module::digest::Digest;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw};

// Maximum bucket bytes per RDB string buffer.
const RDB_BUCKET_CHUNK_SIZE: usize = 1024 * 1024;

/// Grow only after reading and checking a chunk, never from an untrusted header alone.
/// Grow geometrically up to half the final size, then reserve the final buffer.
/// This bounds simultaneous old/new allocations even for non-power-of-two sizes
/// and avoids reallocating on conversion to Box. LoadStringBuffer itself
/// allocates on the server before this helper can check the returned length.
fn load_bucket_chunks<B: AsRef<[u8]>>(
    size: usize,
    mut load_chunk: impl FnMut() -> Option<B>,
) -> Option<Box<[u8]>> {
    if size == 0 || size > isize::MAX as usize {
        return None;
    }
    let mut values = Vec::new();
    while values.len() < size {
        let expected = RDB_BUCKET_CHUNK_SIZE.min(size - values.len());
        let data = load_chunk()?;
        let bytes = data.as_ref();
        if bytes.len() != expected {
            return None;
        }
        let required = values.len().checked_add(expected)?;
        if required > values.capacity() {
            let capacity = if required > size / 2 {
                size
            } else {
                (size / 2).min(values.capacity().checked_mul(2)?.max(required))
            };
            values.try_reserve_exact(capacity - values.len()).ok()?;
        }
        values.extend_from_slice(bytes);
    }
    Some(values.into_boxed_slice())
}

const CUCKOO_TYPE_ENCODING_VERSION: i32 = CUCKOO_OBJECT_VERSION as i32;

pub static CUCKOO_TYPE: ValkeyType = ValkeyType::new(
    "cuckooflt",
    CUCKOO_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(cuckoo_callback::cuckoo_rdb_load),
        rdb_save: Some(cuckoo_callback::cuckoo_rdb_save),
        aof_rewrite: Some(cuckoo_callback::cuckoo_aof_rewrite),
        digest: Some(cuckoo_callback::cuckoo_digest),

        mem_usage: Some(cuckoo_callback::cuckoo_mem_usage),
        free: Some(cuckoo_callback::cuckoo_free),

        aux_load: Some(cuckoo_callback::cuckoo_aux_load),
        // Callback not needed as there is no AUX (out of keyspace) data to be saved.
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: raw::Aux::Before as i32,

        free_effort: Some(cuckoo_callback::cuckoo_free_effort),
        // Callback not needed as it just notifies us when a cuckoo item is about to be freed.
        unlink: None,
        copy: Some(cuckoo_callback::cuckoo_copy),
        defrag: Some(cuckoo_callback::cuckoo_defrag),

        // The callbacks below are not needed since the version 1 variants are used when implemented.
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

pub trait ValkeyDataType {
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<CuckooObject>;
    fn debug_digest(&self, dig: Digest);
}

impl ValkeyDataType for CuckooObject {
    fn load_from_rdb(rdb: *mut raw::RedisModuleIO, encver: i32) -> Option<CuckooObject> {
        if encver != CUCKOO_TYPE_ENCODING_VERSION {
            logging::log_warning("Unsupported cuckoo persistence version.");
            return None;
        }
        fn load_header<const N: usize>(rdb: *mut raw::RedisModuleIO) -> Option<[u64; N]> {
            let mut fields = [0; N];
            for field in &mut fields {
                *field = raw::load_unsigned(rdb).ok()?;
            }
            Some(fields)
        }
        let header @ [expansion, bucket_size, max_kicks, count, num_deleted] = load_header(rdb)?;
        CuckooObject::validate_snapshot_header(header).ok()?;
        let mut filters = Vec::with_capacity(1);
        for _ in 0..count {
            let header =
                CuckooFilter::validate_snapshot_header(load_header(rdb)?, bucket_size as usize)
                    .ok()?;
            let values =
                load_bucket_chunks(header.bucket_bytes(), || raw::load_string_buffer(rdb).ok())?;
            let filter = CuckooFilter::from_snapshot(header, values, max_kicks as u32).ok()?;
            filters.push(Box::new(filter));
        }
        let object = Self::from_existing(
            expansion as u32,
            bucket_size as usize,
            max_kicks as u32,
            filters,
            num_deleted,
        );
        if !Self::validate_size(object.memory_usage()) {
            logging::log_warning(format!(
                "Loaded cuckoo object using {} bytes, exceeding local memory limit {}.",
                object.memory_usage(),
                crate::configs::CUCKOO_MEMORY_LIMIT_PER_OBJECT
                    .load(std::sync::atomic::Ordering::Relaxed)
            ));
        }
        Some(object)
    }

    fn debug_digest(&self, mut dig: Digest) {
        for field in self.snapshot_header() {
            dig.add_long_long(field as i64);
        }
        for filter in self.filters() {
            for field in filter.snapshot_header() {
                dig.add_long_long(field as i64);
            }
            dig.add_string_buffer(filter.as_bytes());
        }
        dig.end_sequence();
    }
}

/// Save directly from the bucket allocations without creating a snapshot buffer.
///
/// # Safety
/// `rdb` must be a valid Valkey persistence context.
pub unsafe fn rdb_save_cuckoo_object(rdb: *mut raw::RedisModuleIO, value: &CuckooObject) {
    for field in value.snapshot_header() {
        raw::RedisModule_SaveUnsigned.unwrap()(rdb, field);
    }
    for filter in value.filters() {
        for field in filter.snapshot_header() {
            raw::RedisModule_SaveUnsigned.unwrap()(rdb, field);
        }
        for chunk in filter.as_bytes().chunks(RDB_BUCKET_CHUNK_SIZE) {
            raw::RedisModule_SaveStringBuffer.unwrap()(rdb, chunk.as_ptr().cast(), chunk.len());
        }
    }
}

pub fn cuckoo_rdb_aux_load(_rdb: *mut raw::RedisModuleIO) -> c_int {
    raw::Status::Ok as i32
}

#[cfg(test)]
mod tests {
    use super::{load_bucket_chunks, RDB_BUCKET_CHUNK_SIZE as CHUNK};
    use crate::test_allocator::{largest_allocation, measure_allocations};

    #[test]
    fn missing_first_chunk_does_not_allocate_declared_size() {
        let (result, largest) =
            largest_allocation(|| load_bucket_chunks::<&[u8]>(16 * CHUNK, || None));
        assert!(result.is_none());
        assert_eq!(largest, 0);
    }

    #[test]
    fn truncated_stream_only_allocates_for_received_chunks() {
        let chunk = vec![100; CHUNK];
        for received in [1_usize, 2, 3, 5] {
            let mut calls = 0;
            let (result, largest) = largest_allocation(|| {
                load_bucket_chunks(16 * CHUNK, || {
                    calls += 1;
                    (calls <= received).then_some(chunk.as_slice())
                })
            });
            assert!(result.is_none());
            assert_eq!(calls, received + 1);
            assert_eq!(largest, received.next_power_of_two() * CHUNK);
        }
    }

    #[test]
    fn intermediate_capacity_is_bounded_for_non_power_of_two_sizes() {
        let chunk = vec![100; CHUNK];
        for (size, received) in [(7 * CHUNK, 3), (13 * CHUNK, 5)] {
            let mut calls = 0;
            let (result, stats) = measure_allocations(|| {
                load_bucket_chunks(size, || {
                    calls += 1;
                    (calls <= received).then_some(chunk.as_slice())
                })
            });
            assert!(result.is_none());
            // A plain doubling strategy would reserve 4 / 8 MiB here, making
            // the subsequent final allocation coexist with more than size/2.
            assert_eq!(stats.largest_allocation, size / 2);
            assert!(stats.peak_live_bytes <= size + size / 2);
            assert_eq!(stats.live_bytes, 0);
        }
    }

    #[test]
    fn wrong_chunk_lengths_are_rejected_before_growing() {
        let full = vec![100; CHUNK];
        let oversized = vec![100; CHUNK + 1];
        for (size, bad, expected_allocation) in [
            (16 * CHUNK, &[][..], CHUNK),
            (16 * CHUNK, &full[..CHUNK - 1], CHUNK),
            (16 * CHUNK, oversized.as_slice(), CHUNK),
            (CHUNK + 3, full.as_slice(), CHUNK + 3),
        ] {
            let mut calls = 0;
            let (result, largest) = largest_allocation(|| {
                load_bucket_chunks(size, || {
                    calls += 1;
                    Some(if calls == 1 { full.as_slice() } else { bad })
                })
            });
            assert!(result.is_none());
            assert_eq!(largest, expected_allocation);
        }
        let (result, largest) =
            largest_allocation(|| load_bucket_chunks(16 * CHUNK, || Some(&[0])));
        assert!(result.is_none());
        assert_eq!(largest, 0);
    }

    #[test]
    fn chunks_preserve_contents_and_partial_tail() {
        for size in [1, CHUNK, CHUNK + 3, 3 * CHUNK, 5 * CHUNK + 7] {
            let input: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
            let mut chunks = input.chunks(CHUNK);
            let (result, stats) =
                measure_allocations(|| load_bucket_chunks(size, || chunks.next()));
            assert_eq!(result.unwrap().as_ref(), input);
            assert_eq!(stats.largest_allocation, size);
            assert!(stats.peak_live_bytes <= size + size / 2);
            assert_eq!(stats.live_bytes, size);
            assert!(chunks.next().is_none());
        }
    }

    #[test]
    fn invalid_allocation_sizes_do_not_read_or_allocate() {
        for size in [0, isize::MAX as usize + 1, usize::MAX] {
            let (result, largest) = largest_allocation(|| {
                load_bucket_chunks::<&[u8]>(size, || panic!("invalid size must not read input"))
            });
            assert!(result.is_none());
            assert_eq!(largest, 0);
        }
    }
}
