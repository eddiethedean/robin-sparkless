//! Shared catalog ownership, leases, and buffer accounting.

use super::*;

pub(super) fn next_temp_view_scope_id() -> u64 {
    NEXT_TEMP_VIEW_SCOPE_ID.fetch_add(1, Ordering::Relaxed)
}

pub(super) fn global_temp_catalog() -> Arc<Mutex<HashMap<String, DataFrame>>> {
    GLOBAL_TEMP_CATALOG
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

/// Catalog of temporary view names to DataFrames (session-scoped). Uses Arc<Mutex<>> for Send+Sync (Python bindings).
pub type TempViewCatalog = Arc<Mutex<HashMap<String, DataFrame>>>;

/// Catalog of saved table names to DataFrames (session-scoped). Used by saveAsTable.
pub(super) type TableCatalog = Arc<Mutex<HashMap<String, DataFrame>>>;

/// Names of databases/schemas created via CREATE DATABASE / CREATE SCHEMA (session-scoped). Persisted when SQL DDL runs.
pub(super) type DatabaseCatalog = Arc<Mutex<HashSet<String>>>;

/// Estimate retained catalog data by unioning the visible Arrow buffer address ranges across
/// columns, table versions, temporary-view snapshots, and statement-pinned versions. Materialized
/// frames stay alive for the full calculation so allocator address reuse cannot make two unrelated
/// allocations appear identical.
pub(super) fn deduplicated_catalog_bytes(frames: &[DataFrame]) -> Result<usize, PolarsError> {
    let materialized = frames
        .iter()
        .map(DataFrame::collect_inner)
        .collect::<Result<Vec<_>, _>>()?;
    let mut ranges = Vec::new();
    for frame in &materialized {
        for column in frame.columns() {
            for chunk in column.as_materialized_series().chunks() {
                add_array_buffer_ranges(chunk.as_ref(), &mut ranges);
            }
        }
    }
    ranges.sort_unstable_by_key(|(start, _)| *start);
    let mut total = 0usize;
    let mut current: Option<(usize, usize)> = None;
    for (start, end) in ranges {
        match current {
            Some((current_start, current_end)) if start <= current_end => {
                current = Some((current_start, current_end.max(end)));
            }
            Some((current_start, current_end)) => {
                total = total.saturating_add(current_end.saturating_sub(current_start));
                current = Some((start, end));
            }
            None => current = Some((start, end)),
        }
    }
    if let Some((start, end)) = current {
        total = total.saturating_add(end.saturating_sub(start));
    }
    Ok(total)
}

fn add_buffer_range<T>(values: &[T], ranges: &mut Vec<(usize, usize)>) {
    let start = values.as_ptr() as usize;
    let end = start.saturating_add(std::mem::size_of_val(values));
    if start < end {
        ranges.push((start, end));
    }
}

fn add_bitmap_range(bitmap: Option<&Bitmap>, ranges: &mut Vec<(usize, usize)>) {
    if let Some(bitmap) = bitmap {
        let (bytes, _, _) = bitmap.as_slice();
        add_buffer_range(bytes, ranges);
    }
}

fn add_array_buffer_ranges(array: &dyn Array, ranges: &mut Vec<(usize, usize)>) {
    use PhysicalType::*;
    add_bitmap_range(array.validity(), ranges);
    match array.dtype().to_physical_type() {
        Null => {}
        Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let (bytes, _, _) = array.values().as_slice();
            add_buffer_range(bytes, ranges);
        }
        Primitive(PrimitiveType::DaysMs) => {
            let array = array.as_any().downcast_ref::<DaysMsArray>().unwrap();
            add_buffer_range(array.values().as_slice(), ranges);
        }
        Primitive(primitive) => {
            with_match_primitive_type_full!(primitive, |$T| {
                let array = array.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap();
                add_buffer_range(array.values().as_slice(), ranges);
            });
        }
        Binary => add_binary_ranges::<i32>(array, ranges),
        LargeBinary => add_binary_ranges::<i64>(array, ranges),
        Utf8 => add_utf8_ranges::<i32>(array, ranges),
        LargeUtf8 => add_utf8_ranges::<i64>(array, ranges),
        FixedSizeBinary => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            add_buffer_range(array.values().as_slice(), ranges);
        }
        List => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            add_buffer_range(array.offsets().buffer().as_slice(), ranges);
            add_array_buffer_ranges(array.values().as_ref(), ranges);
        }
        LargeList => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            add_buffer_range(array.offsets().buffer().as_slice(), ranges);
            add_array_buffer_ranges(array.values().as_ref(), ranges);
        }
        FixedSizeList => {
            let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            add_array_buffer_ranges(array.values().as_ref(), ranges);
        }
        Struct => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            for field in array.values() {
                add_array_buffer_ranges(field.as_ref(), ranges);
            }
        }
        Map => {
            let array = array.as_any().downcast_ref::<MapArray>().unwrap();
            add_buffer_range(array.offsets().buffer().as_slice(), ranges);
            add_array_buffer_ranges(array.field().as_ref(), ranges);
        }
        Union => {
            let array = array.as_any().downcast_ref::<UnionArray>().unwrap();
            add_buffer_range(array.types().as_slice(), ranges);
            if let Some(offsets) = array.offsets() {
                add_buffer_range(offsets.as_slice(), ranges);
            }
            for field in array.fields() {
                add_array_buffer_ranges(field.as_ref(), ranges);
            }
        }
        Dictionary(key_type) => {
            match_integer_type!(key_type, |$T| {
                let array = array.as_any().downcast_ref::<DictionaryArray<$T>>().unwrap();
                add_array_buffer_ranges(array.keys(), ranges);
                add_array_buffer_ranges(array.values().as_ref(), ranges);
            });
        }
        Utf8View => {
            add_binview_ranges::<str>(array, ranges);
        }
        BinaryView => {
            add_binview_ranges::<[u8]>(array, ranges);
        }
    }
}

fn add_binview_ranges<T: ViewType + ?Sized>(array: &dyn Array, ranges: &mut Vec<(usize, usize)>) {
    let array = array
        .as_any()
        .downcast_ref::<BinaryViewArrayGeneric<T>>()
        .unwrap();
    for (index, view) in array.views().iter().enumerate() {
        if !array.is_valid(index) {
            continue;
        }
        if let Some(bytes) = view.get_inlined_slice() {
            add_buffer_range(bytes, ranges);
        } else if let Some(buffer) = array.data_buffers().get(view.buffer_idx as usize) {
            let buffer = buffer.as_slice();
            let start = view.offset as usize;
            let end = start.saturating_add(view.length as usize);
            if end <= buffer.len() {
                add_buffer_range(&buffer[start..end], ranges);
            }
        }
    }
}

fn add_binary_ranges<O: polars_arrow::types::Offset>(
    array: &dyn Array,
    ranges: &mut Vec<(usize, usize)>,
) {
    let array = array.as_any().downcast_ref::<BinaryArray<O>>().unwrap();
    add_buffer_range(array.offsets().buffer().as_slice(), ranges);
    add_buffer_range(array.values().as_slice(), ranges);
}

fn add_utf8_ranges<O: polars_arrow::types::Offset>(
    array: &dyn Array,
    ranges: &mut Vec<(usize, usize)>,
) {
    let array = array.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
    add_buffer_range(array.offsets().buffer().as_slice(), ranges);
    add_buffer_range(array.values().as_slice(), ranges);
}

pub(super) type SharedCatalogMutationWriterGuards<'a> = (
    Option<std::sync::MutexGuard<'a, ()>>,
    Option<std::sync::MutexGuard<'a, ()>>,
);

/// Catalog state deliberately shared by the logical sessions of one application or service.
/// A fresh value creates an independent catalog; cloning a value shares saved tables,
/// database names, and catalog-scoped global views. Session temporary views and configuration
/// remain on `SparkSession` and are never included here.
#[derive(Clone)]
pub struct SharedCatalog {
    pub(super) tables: TableCatalog,
    pub(super) databases: DatabaseCatalog,
    pub(super) global_temp_views: TempViewCatalog,
    pub(super) writer: Arc<Mutex<()>>,
    pub(super) mutation_lane: Arc<Mutex<()>>,
    /// Shared references to session-local view snapshots for catalog-wide buffer accounting.
    /// The owning session remains responsible for resolving and removing each view.
    pub(super) view_accounting: Arc<Mutex<HashMap<u64, HashMap<String, DataFrame>>>>,
    /// Old table/view versions retained while a statement may still reference them.
    pub(super) pinned_versions: Arc<Mutex<Vec<DataFrame>>>,
    pub(super) active_readers: Arc<AtomicUsize>,
}

impl Default for SharedCatalog {
    fn default() -> Self {
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
            databases: Arc::new(Mutex::new(HashSet::new())),
            global_temp_views: Arc::new(Mutex::new(HashMap::new())),
            writer: Arc::new(Mutex::new(())),
            mutation_lane: Arc::new(Mutex::new(())),
            view_accounting: Arc::new(Mutex::new(HashMap::new())),
            pinned_versions: Arc::new(Mutex::new(Vec::new())),
            active_readers: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// A catalog read lease keeps versions that were visible to a statement charged until its
/// execution and result encoding have finished.
pub struct SharedCatalogReadLease {
    pub(super) catalog: SharedCatalog,
}

impl Drop for SharedCatalogReadLease {
    fn drop(&mut self) {
        let Ok(_writer) = self.catalog.writer.lock() else {
            // Keeping the pins is the safe outcome if the publication lock is poisoned.
            return;
        };
        let previous = self.catalog.active_readers.fetch_sub(1, Ordering::AcqRel);
        if previous == 1
            && let Ok(mut pinned) = self.catalog.pinned_versions.lock()
        {
            pinned.clear();
        }
    }
}

impl SharedCatalog {
    /// Create a new, empty catalog scope. Sessions must explicitly attach to this value.
    pub fn new() -> Self {
        Self::default()
    }
}
