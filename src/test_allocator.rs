//! Thread-local allocation measurements; production always uses ValkeyAlloc.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default)]
pub struct AllocationStats {
    pub largest_allocation: usize,
    pub peak_live_bytes: usize,
    pub live_bytes: usize,
    invalid_deallocation: bool,
}

thread_local! {
    static STATS: Cell<Option<AllocationStats>> = const { Cell::new(None) };
}

pub struct TrackingAllocator;

fn allocated(size: usize) {
    let _ = STATS.try_with(|stats| {
        if let Some(mut current) = stats.get() {
            current.largest_allocation = current.largest_allocation.max(size);
            current.live_bytes += size;
            current.peak_live_bytes = current.peak_live_bytes.max(current.live_bytes);
            stats.set(Some(current));
        }
    });
}

fn freed(size: usize) {
    let _ = STATS.try_with(|stats| {
        if let Some(mut current) = stats.get() {
            if let Some(remaining) = current.live_bytes.checked_sub(size) {
                current.live_bytes = remaining;
            } else {
                current.invalid_deallocation = true;
            }
            stats.set(Some(current));
        }
    });
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    // Use GlobalAlloc's allocate-copy-free realloc, just like ValkeyAlloc. This
    // exposes both live allocations to the counter instead of hiding the old
    // buffer behind System.realloc's net capacity change.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        freed(layout.size());
    }
}

/// Measure allocations on this thread. The closure must not free/reallocate
/// storage allocated outside it or transfer measured storage to another thread.
/// Borrowing preallocated input and returning an owned result are supported.
pub fn measure_allocations<T>(f: impl FnOnce() -> T) -> (T, AllocationStats) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            STATS.set(None);
        }
    }
    assert!(STATS.get().is_none(), "allocation measurements cannot nest");
    let (value, stats) = {
        STATS.set(Some(AllocationStats::default()));
        let _reset = Reset;
        let value = f();
        (value, STATS.get().unwrap())
    };
    assert!(!stats.invalid_deallocation, "freed unmeasured storage");
    (value, stats)
}

pub fn largest_allocation<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let (value, stats) = measure_allocations(f);
    (value, stats.largest_allocation)
}

#[test]
fn tracks_live_buffers_across_growth_shrink_and_free() {
    let (_, stats) = measure_allocations(|| unsafe {
        let initial = Layout::from_size_align(16, 1).unwrap();
        let ptr = std::alloc::alloc_zeroed(initial);
        assert!(!ptr.is_null());
        ptr.write(42);
        let ptr = std::alloc::realloc(ptr, initial, 32);
        assert!(!ptr.is_null());
        assert_eq!(ptr.read(), 42);
        let grown = Layout::from_size_align(32, 1).unwrap();
        let ptr = std::alloc::realloc(ptr, grown, 8);
        assert!(!ptr.is_null());
        assert_eq!(ptr.read(), 42);
        std::alloc::dealloc(ptr, Layout::from_size_align(8, 1).unwrap());
    });
    assert_eq!(stats.largest_allocation, 32);
    assert_eq!(stats.peak_live_bytes, 48);
    assert_eq!(stats.live_bytes, 0);
}

#[test]
fn distinguishes_two_live_buffers_from_the_largest_allocation() {
    let (_, stats) = measure_allocations(|| {
        let first = vec![1_u8; 4096].into_boxed_slice();
        let second = first.clone();
        std::hint::black_box((&first, &second));
    });
    assert_eq!(stats.largest_allocation, 4096);
    assert_eq!(stats.peak_live_bytes, 8192);
    assert_eq!(stats.live_bytes, 0);
}
