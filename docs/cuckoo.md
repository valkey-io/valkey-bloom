# Cuckoo filters

Cuckoo filters store one-byte fingerprints. Membership and counts are approximate:
collisions can match an item that was never inserted.

- `CF.ADD` and `CF.INSERT` store duplicates; `CF.ADDNX` and `CF.INSERTNX` skip existing matches.
- `CF.COUNT` returns the number of matching fingerprints.
- `CF.DEL` removes one match and returns 1, or 0 if none exists. Only delete items
  known to have been inserted: deleting a false positive can remove another item's fingerprint.
- Batch inserts stop at the first error; earlier successful insertions remain stored.

See [command definitions](../src/commands) for syntax and options.

## Scaling

Insertion checks free candidate slots in every subfilter, newest first, then
attempts up to `MAXITERATIONS` evictions in the newest subfilter before scaling.
Slots freed by deletion can be reused. A failed item insertion preserves existing
fingerprints and RNG state.

`EXPANSION 0` disables scaling. Growth is also bounded by 1,024 subfilters,
a requested capacity of 2^32 per subfilter and `bf.cuckoo-memory-usage-limit`.
An unsuccessful insertion costs O(L + (F + I) × B), where L is item length,
F is the subfilter count, I is `MAXITERATIONS` and B is bucket size.

## CF.INFO

`CF.INFO key [field]` returns name/value pairs or one integer for the named field.
Field names are case insensitive; quote names containing spaces.

| Field | Meaning |
| --- | --- |
| Size | Allocated object memory in bytes, including metadata |
| Number of buckets | Total buckets across subfilters |
| Number of items inserted | Currently stored fingerprints, including duplicates |
| Number of items deleted | Lifetime successful deletions, capped at 2^63 − 1 |
| Number of filters | Subfilter count |
| Bucket size | Fingerprint slots per bucket |
| Max iterations | Maximum eviction attempts per insertion |
| Expansion rate | Growth factor; 0 disables scaling |

## Persistence and replication

RDB, AOF, replication and `COPY` preserve filter contents, deletion counts and
RNG state. Use matching module builds on primary and replica. Rebuild incompatible
filters from the original items.

RDB and `CF.LOAD` accept format identifier 1. A `CF.LOAD` snapshot contains a
version byte followed by little-endian u64 fields:

- Object: expansion, bucket size, max iterations, subfilter count, deletion count.
- Each subfilter: requested capacity, fingerprint count, low and high RNG word
  position, bucket-byte length, then bucket bytes.

RDB stores the same metadata and splits bucket data into chunks of at most 1 MiB.
`CF.LOAD` requires an absent key and checks the complete allocation size before
allocating buckets. Local creation, growth and client `CF.LOAD` enforce the object
memory limit; RDB loading, `RESTORE` and mandatory replication/AOF replay bypass it.
This limit is not a cap on total server memory during restoration.

## Defragmentation and CI

`INFO MODULES` exposes `bf_cuckoo_defrag_hits`, `bf_cuckoo_defrag_misses` and
`bf_cuckoo_defrag_bucket_attempts`; the last counts bucket-buffer visits.

[CI](../.github/workflows/ci.yml) runs Rust tests with ASAN and leak detection for
default and `valkey_8_0`, including the defrag relocation test. A separate matrix
tests ASAN-instrumented Valkey 8.0, 8.1 and unstable servers with a normal module
build. Logs and server JUnit reports are retained for 14 days. The workflow contains
the commands for local reproduction.
