//! Exercise the production callback with an allocator that really relocates data.
//! Raw API replacement runs in a child process with exactly one selected test.
use super::*;
use crate::cuckoo::utils::CuckooFilter;
use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::process::Command;

const CHILD_MODE: &str = "VALKEY_CUCKOO_DEFRAG_TEST_CHILD";
const TEST_NAME: &str = "wrapper::cuckoo_callback::tests::defrag_relocates_owned_allocations";

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Object,
    Vector,
    Filter,
    Buckets,
}

struct Allocation {
    original: *mut u8,
    current: *mut u8,
    layout: Layout,
    kind: Kind,
    attempts: usize,
}

struct Mover {
    allocations: Vec<Allocation>,
    move_allocations: bool,
    cursor: u64,
    remaining_filters: usize,
}

impl Mover {
    fn register(&mut self, ptr: *mut u8, layout: Layout, kind: Kind) {
        self.allocations.push(Allocation {
            original: ptr,
            current: ptr,
            layout,
            kind,
            attempts: 0,
        });
    }
}

// Allocate through the same global allocator as Box/Vec, copy without dropping
// the payload, then free only the old storage. Ownership stays with the callback.
unsafe extern "C" fn move_allocation(
    ctx: *mut RedisModuleDefragCtx,
    ptr: *mut c_void,
) -> *mut c_void {
    let mover = &mut *ctx.cast::<Mover>();
    let allocation = mover
        .allocations
        .iter_mut()
        .find(|allocation| allocation.current == ptr.cast())
        .expect("callback passed an unregistered allocation");
    allocation.attempts += 1;
    if !mover.move_allocations {
        return null_mut();
    }
    let new = alloc(allocation.layout);
    if new.is_null() {
        handle_alloc_error(allocation.layout);
    }
    // Allocating before freeing guarantees that these live regions differ.
    assert_ne!(new, allocation.current);
    // Registered pointers are only address records. Their shared-reference
    // provenance may have been invalidated by Box::into_raw in the callback.
    // Access and free storage through the owning pointer supplied by that call.
    let source = ptr.cast::<u8>();
    std::ptr::copy_nonoverlapping(source, new, allocation.layout.size());
    dealloc(source, allocation.layout);
    allocation.current = new;
    new.cast()
}

unsafe extern "C" fn get_cursor(ctx: *mut RedisModuleDefragCtx, cursor: *mut u64) -> c_int {
    *cursor = (*ctx.cast::<Mover>()).cursor;
    0
}

unsafe extern "C" fn set_cursor(ctx: *mut RedisModuleDefragCtx, cursor: u64) -> c_int {
    (*ctx.cast::<Mover>()).cursor = cursor;
    0
}

unsafe extern "C" fn should_stop(ctx: *mut RedisModuleDefragCtx) -> c_int {
    let mover = &mut *ctx.cast::<Mover>();
    if mover.remaining_filters == 0 {
        1
    } else {
        mover.remaining_filters -= 1;
        0
    }
}

#[derive(Debug, PartialEq)]
enum DigestEntry {
    Integer(i64),
    Bytes(Vec<u8>),
    End,
}

unsafe extern "C" fn digest_integer(ctx: *mut raw::RedisModuleDigest, value: i64) {
    (*ctx.cast::<Vec<DigestEntry>>()).push(DigestEntry::Integer(value));
}

unsafe extern "C" fn digest_bytes(
    ctx: *mut raw::RedisModuleDigest,
    ptr: *const c_char,
    len: usize,
) {
    let bytes = std::slice::from_raw_parts(ptr.cast::<u8>(), len);
    (*ctx.cast::<Vec<DigestEntry>>()).push(DigestEntry::Bytes(bytes.to_vec()));
}

unsafe extern "C" fn digest_end(ctx: *mut raw::RedisModuleDigest) {
    (*ctx.cast::<Vec<DigestEntry>>()).push(DigestEntry::End);
}

struct OwnedObject(*mut c_void);
impl Drop for OwnedObject {
    fn drop(&mut self) {
        unsafe { cuckoo_free(self.0) }
    }
}

unsafe fn digest(value: *mut c_void) -> Vec<DigestEntry> {
    let mut trace = Vec::<DigestEntry>::new();
    // Compare the exact typed inputs emitted by the production digest callback,
    // avoiding a second implementation of its metadata/bucket traversal.
    cuckoo_digest((&mut trace as *mut Vec<DigestEntry>).cast(), value);
    trace
}

fn logical_info(object: &CuckooObject) -> [i64; 7] {
    [
        object
            .filters()
            .iter()
            .map(|f| f.bucket_count() as i64)
            .sum(),
        object.num_items(),
        object.num_deleted(),
        object.num_filters() as i64,
        object.bucket_size() as i64,
        object.max_kicks() as i64,
        object.expansion() as i64,
    ]
}

unsafe fn relocation_case(capacity: i64, delete_items: bool, move_allocations: bool) {
    let items: Vec<_> = (0..20).map(|i| format!("item:{i}")).collect();
    let mut original = CuckooObject::new_reserved(capacity, 4, 20, 2, false).unwrap();
    for item in &items {
        original.add_item(item.as_bytes(), false).unwrap();
    }
    original.add_item(b"duplicate", false).unwrap();
    original.add_item(b"duplicate", false).unwrap();
    if delete_items {
        for item in items.iter().step_by(2) {
            assert_eq!(original.delete_item(item.as_bytes()).unwrap(), 1);
        }
    }
    // COPY gives an exact-length pointer vector; its own realloc is not the
    // allocator operation under test. Server tests also cover vector shrinking.
    let object = Box::new(CuckooObject::create_copy_from(&original));
    drop(original);
    assert_eq!(object.filters().capacity(), object.num_filters());
    assert_eq!(object.num_filters() > 1, capacity == 8);
    let mut mover = Mover {
        allocations: Vec::new(),
        move_allocations,
        cursor: 0,
        remaining_filters: 1,
    };
    for filter in object.filters() {
        mover.register(
            (&**filter as *const CuckooFilter).cast_mut().cast(),
            Layout::new::<CuckooFilter>(),
            Kind::Filter,
        );
        mover.register(
            filter.as_bytes().as_ptr().cast_mut(),
            Layout::array::<u8>(filter.as_bytes().len()).unwrap(),
            Kind::Buckets,
        );
    }
    mover.register(
        object.filters().as_ptr().cast_mut().cast(),
        Layout::array::<Box<CuckooFilter>>(object.num_filters()).unwrap(),
        Kind::Vector,
    );
    let mut owned = OwnedObject(Box::into_raw(object).cast());
    mover.register(owned.0.cast(), Layout::new::<CuckooObject>(), Kind::Object);
    let before = &*owned.0.cast::<CuckooObject>();
    let snapshot = before.encode_object();
    let info = logical_info(before);
    let membership: Vec<_> = items
        .iter()
        .map(|item| before.item_exists(item.as_bytes()))
        .collect();
    let counts: Vec<_> = items
        .iter()
        .map(|item| before.count_item(item.as_bytes()))
        .collect();
    let duplicate_count = before.count_item(b"duplicate");
    let digest_before = digest(owned.0);
    let filters = before.num_filters();
    for visited in 1..=filters {
        mover.remaining_filters = 1;
        // Derive a fresh context after owner access on every iteration.
        let ctx = (&mut mover as *mut Mover).cast();
        let status = cuckoo_defrag(ctx, null_mut(), &mut owned.0);
        assert_eq!(status, i32::from(visited < filters));
        assert_eq!(mover.cursor, visited as u64);
        assert_eq!(digest(owned.0), digest_before);
        let after = &*owned.0.cast::<CuckooObject>();
        assert_eq!(after.encode_object(), snapshot);
        assert_eq!(logical_info(after), info);
        assert_eq!(
            items
                .iter()
                .map(|item| after.item_exists(item.as_bytes()))
                .collect::<Vec<_>>(),
            membership
        );
        assert_eq!(
            items
                .iter()
                .map(|item| after.count_item(item.as_bytes()))
                .collect::<Vec<_>>(),
            counts
        );
        assert_eq!(after.count_item(b"duplicate"), duplicate_count);
    }
    let after = &mut *owned.0.cast::<CuckooObject>();
    assert_eq!(after.add_item(b"after-defrag", false).unwrap(), 1);
    assert!(after.item_exists(b"after-defrag"));
    assert_eq!(after.delete_item(b"after-defrag").unwrap(), 1);
    assert_eq!(after.delete_item(b"duplicate").unwrap(), 1);
    assert_eq!(after.count_item(b"duplicate"), duplicate_count - 1);
    // Exercise the production free callback before checking the move criterion,
    // including in the negative control. The Rust ASAN CI job checks this
    // ownership transfer (see docs/cuckoo.md for local reproduction).
    drop(owned);
    for kind in [Kind::Filter, Kind::Buckets, Kind::Vector, Kind::Object] {
        let allocations: Vec<_> = mover
            .allocations
            .iter()
            .filter(|a| a.kind == kind)
            .collect();
        assert!(!allocations.is_empty());
        for allocation in allocations {
            assert_eq!(allocation.attempts, 1);
            assert_ne!(
                allocation.original, allocation.current,
                "allocation was not moved: {kind:?}"
            );
        }
    }
    println!("relocation verified: capacity={capacity}, deleted={delete_items}, filters={filters}");
}

struct Restore<F: FnOnce()>(Option<F>);
impl<F: FnOnce()> Drop for Restore<F> {
    fn drop(&mut self) {
        self.0.take().unwrap()();
    }
}

unsafe fn run_child(move_allocations: bool) {
    let saved = (
        raw::RedisModule_DefragAlloc,
        raw::RedisModule_DefragCursorGet,
        raw::RedisModule_DefragCursorSet,
        raw::RedisModule_DefragShouldStop,
        raw::RedisModule_DigestAddLongLong,
        raw::RedisModule_DigestAddStringBuffer,
        raw::RedisModule_DigestEndSequence,
    );
    let _restore = Restore(Some(|| {
        raw::RedisModule_DefragAlloc = saved.0;
        raw::RedisModule_DefragCursorGet = saved.1;
        raw::RedisModule_DefragCursorSet = saved.2;
        raw::RedisModule_DefragShouldStop = saved.3;
        raw::RedisModule_DigestAddLongLong = saved.4;
        raw::RedisModule_DigestAddStringBuffer = saved.5;
        raw::RedisModule_DigestEndSequence = saved.6;
    }));
    raw::RedisModule_DefragAlloc = Some(move_allocation);
    raw::RedisModule_DefragCursorGet = Some(get_cursor);
    raw::RedisModule_DefragCursorSet = Some(set_cursor);
    raw::RedisModule_DefragShouldStop = Some(should_stop);
    raw::RedisModule_DigestAddLongLong = Some(digest_integer);
    raw::RedisModule_DigestAddStringBuffer = Some(digest_bytes);
    raw::RedisModule_DigestEndSequence = Some(digest_end);
    assert!(configs::CUCKOO_DEFRAG.load(Ordering::Relaxed));
    for capacity in [200, 8] {
        for deleted in [false, true] {
            relocation_case(capacity, deleted, move_allocations);
        }
    }
}

#[test]
fn defrag_relocates_owned_allocations() {
    if let Ok(mode) = std::env::var(CHILD_MODE) {
        unsafe { run_child(mode == "move") };
        return;
    }
    for mode in ["move", "no-move"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
            .env(CHILD_MODE, mode)
            .output()
            .unwrap();
        let report = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        // Check child diagnostics in the Rust ASAN CI job and local sanitizer runs.
        assert!(!report.contains("ERROR: AddressSanitizer"), "{report}");
        assert!(!report.contains("LeakSanitizer"), "{report}");
        if mode == "move" {
            assert!(output.status.success(), "{report}");
        } else {
            assert!(
                !output.status.success(),
                "negative control unexpectedly passed"
            );
            assert!(
                report.contains("allocation was not moved: Filter"),
                "{report}"
            );
        }
        println!("{mode}: {report}");
    }
}
