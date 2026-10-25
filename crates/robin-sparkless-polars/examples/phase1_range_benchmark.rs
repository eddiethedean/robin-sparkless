//! Reproducible single-process Phase 1 range/count/limit measurement.
//!
//! Build once, then run this binary separately for each cardinality while collecting
//! process peak RSS externally (for example with `/usr/bin/time -l`).

use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use robin_sparkless_core::CancellationToken;
use robin_sparkless_polars::{Column, DataType, SparkSession, column_to_expr, lit_i64};

const PROJECTED_BATCH_ROWS: usize = 16_384;

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

    let projected_plan_baseline = begin_measurement();
    let projected_plan_started = Instant::now();
    let projected = range.select_exprs(vec![
        (column_to_expr(&Column::new("id".to_string())) * lit_i64(2)).alias("doubled"),
    ])?;
    let projected_schema = projected.schema()?;
    let projected_plan_micros = projected_plan_started.elapsed().as_micros();
    let projected_plan_memory = measurement(projected_plan_baseline);

    let projected_scan_baseline = begin_measurement();
    let projected_scan_started = Instant::now();
    let mut batches = projected
        .collect_batches_with_cancellation(PROJECTED_BATCH_ROWS, &CancellationToken::new())?;
    let mut projected_rows = 0usize;
    let mut projected_batch_count = 0usize;
    let mut projected_largest_batch = 0usize;
    let mut projected_first_value = None;
    let mut projected_last_value = None;
    for batch in &mut batches {
        let batch = batch?;
        let values = batch.column("doubled")?.i64()?;
        let batch_first = values
            .get(0)
            .ok_or("projected range emitted an empty batch")?;
        let batch_last = values
            .get(batch.height() - 1)
            .ok_or("projected range batch has no final value")?;
        let expected_first = i64::try_from(projected_rows)?
            .checked_mul(2)
            .ok_or("projected first value overflowed i64")?;
        let last_row = projected_rows
            .checked_add(batch.height())
            .and_then(|end| end.checked_sub(1))
            .ok_or("projected batch row index overflowed usize")?;
        let expected_last = i64::try_from(last_row)?
            .checked_mul(2)
            .ok_or("projected last value overflowed i64")?;
        if batch_first != expected_first || batch_last != expected_last {
            return Err("projected range batch returned incorrect endpoint values".into());
        }
        projected_first_value.get_or_insert(batch_first);
        projected_last_value = Some(batch_last);
        projected_rows += batch.height();
        projected_batch_count += 1;
        projected_largest_batch = projected_largest_batch.max(batch.height());
    }
    let projected_scan_micros = projected_scan_started.elapsed().as_micros();
    let projected_scan_memory = measurement(projected_scan_baseline);
    let expected_rows = usize::try_from(rows)?;
    let expected_batch_count = expected_rows.div_ceil(PROJECTED_BATCH_ROWS);
    let expected_last_value = rows
        .checked_sub(1)
        .and_then(|value| value.checked_mul(2))
        .ok_or("projected range endpoint overflowed i64")?;
    let projected_field = projected_schema.fields().first();
    let projected_field_matches = projected_field.is_some_and(|field| {
        field.name == "doubled" && matches!(field.data_type, DataType::Long) && field.nullable
    });
    if projected_schema.fields().len() != 1
        || !projected_field_matches
        || projected_rows != expected_rows
        || projected_batch_count != expected_batch_count
        || projected_largest_batch > PROJECTED_BATCH_ROWS
        || projected_first_value != Some(0)
        || projected_last_value != Some(expected_last_value)
    {
        let (schema_name, schema_type, schema_nullable) = projected_field
            .map(|field| {
                (
                    field.name.as_str(),
                    format!("{:?}", field.data_type),
                    field.nullable.to_string(),
                )
            })
            .unwrap_or_else(|| ("<none>", "<none>".to_string(), "<none>".to_string()));
        return Err(format!(
            "projected range witness mismatch: schema_columns={} schema_name={} schema_type={} nullable={} rows={}/{} batches={}/{} largest_batch={} first={:?} last={:?} expected_last={expected_last_value}",
            projected_schema.fields().len(),
            schema_name,
            schema_type,
            schema_nullable,
            projected_rows,
            expected_rows,
            projected_batch_count,
            expected_batch_count,
            projected_largest_batch,
            projected_first_value,
            projected_last_value,
        )
        .into());
    }

    println!(
        "rows={rows} schema_columns={} schema_type={:?} schema_nullable={} predicate=id>5 \
         count={count} expected_count={} \
         limit_rows={} limit_values={output_values:?} output_bytes={output_bytes} \
         strategy=lazy_range_bounded_batches construction_us={construction_micros} \
         construction_allocated_bytes={} construction_live_bytes={} construction_peak_live_bytes={} \
         count_us={count_micros} count_allocated_bytes={} count_live_bytes={} count_peak_live_bytes={} \
         limit_us={limit_micros} limit_allocated_bytes={} limit_live_bytes={} limit_peak_live_bytes={} \
         projected_strategy=bounded_batches projected_columns={} projected_schema_type={:?} \
         projected_nullable={} projected_rows={} projected_batch_count={} \
         projected_largest_batch_rows={} projected_first_value={} projected_last_value={} \
         projected_plan_us={projected_plan_micros} projected_plan_allocated_bytes={} \
         projected_plan_live_bytes={} projected_plan_peak_live_bytes={} \
         projected_scan_us={projected_scan_micros} projected_scan_allocated_bytes={} \
         projected_scan_live_bytes={} projected_scan_peak_live_bytes={}",
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
        projected_schema.fields().len(),
        projected_schema.fields()[0].data_type,
        projected_schema.fields()[0].nullable,
        projected_rows,
        projected_batch_count,
        projected_largest_batch,
        projected_first_value.unwrap_or_default(),
        projected_last_value.unwrap_or_default(),
        projected_plan_memory.0,
        projected_plan_memory.1,
        projected_plan_memory.2,
        projected_scan_memory.0,
        projected_scan_memory.1,
        projected_scan_memory.2,
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
