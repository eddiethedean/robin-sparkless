//! Optional process-wide Rust global allocator counters for release resource witnesses.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct AllocationSnapshot {
    pub live_bytes: usize,
    pub peak_live_bytes: usize,
    pub cumulative_allocated_bytes: usize,
}

struct TrackingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
static CUMULATIVE_ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, old_layout: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, old_layout, new_size) };
        if !resized.is_null() {
            if new_size >= old_layout.size() {
                record_allocation(new_size - old_layout.size());
            } else {
                LIVE_BYTES.fetch_sub(old_layout.size() - new_size, Ordering::Relaxed);
            }
        }
        resized
    }
}

fn record_allocation(bytes: usize) {
    CUMULATIVE_ALLOCATED_BYTES.fetch_add(bytes, Ordering::Relaxed);
    let current = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
    let mut peak = PEAK_BYTES.load(Ordering::Relaxed);
    while current > peak {
        match PEAK_BYTES.compare_exchange_weak(peak, current, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

pub fn snapshot() -> AllocationSnapshot {
    AllocationSnapshot {
        live_bytes: LIVE_BYTES.load(Ordering::Relaxed),
        peak_live_bytes: PEAK_BYTES.load(Ordering::Relaxed),
        cumulative_allocated_bytes: CUMULATIVE_ALLOCATED_BYTES.load(Ordering::Relaxed),
    }
}
