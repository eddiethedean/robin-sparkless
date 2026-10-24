//! Reproducible single-process Phase 1 range/count/limit measurement.
//!
//! Build once, then run this binary separately for each cardinality while collecting
//! process peak RSS externally (for example with `/usr/bin/time -l`).

use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use robin_sparkless_polars::{Column, DataType, SparkSession, column_to_expr, lit_i64};

struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
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
    ALLOCATED_BYTES.fetch_add(bytes, Ordering::Relaxed);
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

fn begin_measurement() -> usize {
    let baseline = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(baseline, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    baseline
}

fn measurement(baseline: usize) -> (usize, usize, usize) {
    let live = LIVE_BYTES.load(Ordering::Relaxed);
    let peak = PEAK_BYTES.load(Ordering::Relaxed);
    (
        ALLOCATED_BYTES.load(Ordering::Relaxed),
        live.saturating_sub(baseline),
        peak.saturating_sub(baseline),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows: i64 = env::args()
        .nth(1)
        .ok_or("usage: phase1_range_benchmark <rows>")?
        .parse()?;
    if rows <= 0 {
        return Err("rows must be positive".into());
    }

    let session = SparkSession::builder()
        .app_name("phase1-range-benchmark")
        .get_or_create();
    let warmup = session.range(0, 1, 1)?;
    assert_eq!(warmup.count()?, 1);
    drop(warmup);

    let baseline = begin_measurement();
    let construction_started = Instant::now();
    let range = session.range(0, rows, 1)?;
    let schema = range.schema()?;
    let filtered = range.filter(column_to_expr(
        &Column::new("id".to_string()).gt(lit_i64(5)),
    ))?;
    let construction_micros = construction_started.elapsed().as_micros();
    let construction_memory = measurement(baseline);
    if schema.fields().len() != 1 {
        return Err("range benchmark produced an unexpected schema width".into());
    }

    let baseline = begin_measurement();
    let count_started = Instant::now();
    let count = filtered.count()?;
    let count_micros = count_started.elapsed().as_micros();
    let count_memory = measurement(baseline);

    let baseline = begin_measurement();
    let limit_started = Instant::now();
    let limited = range.limit(10)?.collect()?;
    let limit_micros = limit_started.elapsed().as_micros();
    let limit_memory = measurement(baseline);
    let output_bytes = limited.estimated_size();
    let output_values = limited
        .column("id")?
        .i64()?
        .into_no_null_iter()
        .collect::<Vec<_>>();

    println!(
        "rows={rows} schema_columns={} schema_type={:?} schema_nullable={} predicate=id>5 \
         count={count} expected_count={} \
         limit_rows={} limit_values={output_values:?} output_bytes={output_bytes} \
         strategy=lazy_range_bounded_batches construction_us={construction_micros} \
         construction_allocated_bytes={} construction_live_bytes={} construction_peak_live_bytes={} \
         count_us={count_micros} count_allocated_bytes={} count_live_bytes={} count_peak_live_bytes={} \
         limit_us={limit_micros} limit_allocated_bytes={} limit_live_bytes={} limit_peak_live_bytes={}",
        schema.fields().len(),
        schema.fields()[0].data_type,
        schema.fields()[0].nullable,
        rows.saturating_sub(6),
        limited.height(),
        construction_memory.0,
        construction_memory.1,
        construction_memory.2,
        count_memory.0,
        count_memory.1,
        count_memory.2,
        limit_memory.0,
        limit_memory.1,
        limit_memory.2,
    );

    if schema.fields().len() != 1
        || !matches!(schema.fields()[0].data_type, DataType::Long)
        || schema.fields()[0].nullable
        || count != rows.saturating_sub(6) as usize
        || output_values != (0..10).collect::<Vec<_>>()
    {
        return Err("range benchmark result did not match the expected count/limit".into());
    }
    Ok(())
}
