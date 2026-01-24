// Conversion utilities between Arrow 52 types and DataFusion 42's arrow types
// DataFusion uses its own arrow crate which may be a different version

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::arrow::datatypes::{DataType as DFDataType, Field as DFField, Schema as DFSchema};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use datafusion::arrow::record_batch::RecordBatch as DFRecordBatch;
use std::sync::Arc;

/// Convert Arrow 52 DataType to DataFusion's DataType
pub fn arrow_to_df_datatype(dt: &ArrowDataType) -> DFDataType {
    match dt {
        ArrowDataType::Null => DFDataType::Null,
        ArrowDataType::Boolean => DFDataType::Boolean,
        ArrowDataType::Int8 => DFDataType::Int8,
        ArrowDataType::Int16 => DFDataType::Int16,
        ArrowDataType::Int32 => DFDataType::Int32,
        ArrowDataType::Int64 => DFDataType::Int64,
        ArrowDataType::UInt8 => DFDataType::UInt8,
        ArrowDataType::UInt16 => DFDataType::UInt16,
        ArrowDataType::UInt32 => DFDataType::UInt32,
        ArrowDataType::UInt64 => DFDataType::UInt64,
        ArrowDataType::Float16 => DFDataType::Float16,
        ArrowDataType::Float32 => DFDataType::Float32,
        ArrowDataType::Float64 => DFDataType::Float64,
        ArrowDataType::Timestamp(unit, tz) => {
            use datafusion::arrow::datatypes::TimeUnit as DFTimeUnit;
            let df_unit = match unit {
                arrow::datatypes::TimeUnit::Second => DFTimeUnit::Second,
                arrow::datatypes::TimeUnit::Millisecond => DFTimeUnit::Millisecond,
                arrow::datatypes::TimeUnit::Microsecond => DFTimeUnit::Microsecond,
                arrow::datatypes::TimeUnit::Nanosecond => DFTimeUnit::Nanosecond,
            };
            DFDataType::Timestamp(df_unit, tz.clone())
        }
        ArrowDataType::Date32 => DFDataType::Date32,
        ArrowDataType::Date64 => DFDataType::Date64,
        ArrowDataType::Time32(unit) => {
            use datafusion::arrow::datatypes::TimeUnit as DFTimeUnit;
            let df_unit = match unit {
                arrow::datatypes::TimeUnit::Second => DFTimeUnit::Second,
                arrow::datatypes::TimeUnit::Millisecond => DFTimeUnit::Millisecond,
                arrow::datatypes::TimeUnit::Microsecond => DFTimeUnit::Microsecond,
                arrow::datatypes::TimeUnit::Nanosecond => DFTimeUnit::Nanosecond,
            };
            DFDataType::Time32(df_unit)
        }
        ArrowDataType::Time64(unit) => {
            use datafusion::arrow::datatypes::TimeUnit as DFTimeUnit;
            let df_unit = match unit {
                arrow::datatypes::TimeUnit::Second => DFTimeUnit::Second,
                arrow::datatypes::TimeUnit::Millisecond => DFTimeUnit::Millisecond,
                arrow::datatypes::TimeUnit::Microsecond => DFTimeUnit::Microsecond,
                arrow::datatypes::TimeUnit::Nanosecond => DFTimeUnit::Nanosecond,
            };
            DFDataType::Time64(df_unit)
        }
        ArrowDataType::Duration(unit) => {
            use datafusion::arrow::datatypes::TimeUnit as DFTimeUnit;
            let df_unit = match unit {
                arrow::datatypes::TimeUnit::Second => DFTimeUnit::Second,
                arrow::datatypes::TimeUnit::Millisecond => DFTimeUnit::Millisecond,
                arrow::datatypes::TimeUnit::Microsecond => DFTimeUnit::Microsecond,
                arrow::datatypes::TimeUnit::Nanosecond => DFTimeUnit::Nanosecond,
            };
            DFDataType::Duration(df_unit)
        }
        ArrowDataType::Interval(unit) => {
            use datafusion::arrow::datatypes::IntervalUnit as DFIntervalUnit;
            let df_unit = match unit {
                arrow::datatypes::IntervalUnit::YearMonth => DFIntervalUnit::YearMonth,
                arrow::datatypes::IntervalUnit::DayTime => DFIntervalUnit::DayTime,
                arrow::datatypes::IntervalUnit::MonthDayNano => DFIntervalUnit::MonthDayNano,
            };
            DFDataType::Interval(df_unit)
        }
        ArrowDataType::Binary => DFDataType::Binary,
        ArrowDataType::FixedSizeBinary(size) => DFDataType::FixedSizeBinary(*size),
        ArrowDataType::LargeBinary => DFDataType::LargeBinary,
        ArrowDataType::Utf8 => DFDataType::Utf8,
        ArrowDataType::LargeUtf8 => DFDataType::LargeUtf8,
        ArrowDataType::List(field) => {
            DFDataType::List(Arc::new(arrow_to_df_field(field.as_ref())))
        }
        ArrowDataType::FixedSizeList(field, size) => {
            DFDataType::FixedSizeList(Arc::new(arrow_to_df_field(field.as_ref())), *size)
        }
        ArrowDataType::LargeList(field) => {
            DFDataType::LargeList(Arc::new(arrow_to_df_field(field.as_ref())))
        }
        ArrowDataType::Struct(fields) => {
            DFDataType::Struct(fields.iter().map(|f| arrow_to_df_field(f)).collect())
        }
        ArrowDataType::Union(fields, mode) => {
            // Arrow 52 Union has UnionFields which is Vec<(i8, Arc<Field>)>
            // DataFusion Union also uses UnionFields
            use datafusion::arrow::datatypes::{UnionMode as DFUnionMode, UnionFields};
            let df_mode = match mode {
                arrow::datatypes::UnionMode::Sparse => DFUnionMode::Sparse,
                arrow::datatypes::UnionMode::Dense => DFUnionMode::Dense,
            };
            let df_fields: UnionFields = fields.iter()
                .map(|(id, field)| (id, Arc::new(arrow_to_df_field(field.as_ref()))))
                .collect();
            DFDataType::Union(df_fields, df_mode)
        }
        ArrowDataType::Dictionary(key_type, value_type) => {
            DFDataType::Dictionary(Box::new(arrow_to_df_datatype(key_type)), Box::new(arrow_to_df_datatype(value_type)))
        }
        ArrowDataType::Map(field, sorted) => {
            DFDataType::Map(Arc::new(arrow_to_df_field(field.as_ref())), *sorted)
        }
        ArrowDataType::RunEndEncoded(run_ends, values) => {
            // RunEndEncoded takes Field types, not DataType
            DFDataType::RunEndEncoded(
                Arc::new(arrow_to_df_field(run_ends.as_ref())),
                Arc::new(arrow_to_df_field(values.as_ref()))
            )
        }
        _ => DFDataType::Utf8, // Fallback
    }
}

/// Convert DataFusion's DataType to Arrow 52 DataType
pub fn df_to_arrow_datatype(dt: &DFDataType) -> ArrowDataType {
    match dt {
        DFDataType::Null => ArrowDataType::Null,
        DFDataType::Boolean => ArrowDataType::Boolean,
        DFDataType::Int8 => ArrowDataType::Int8,
        DFDataType::Int16 => ArrowDataType::Int16,
        DFDataType::Int32 => ArrowDataType::Int32,
        DFDataType::Int64 => ArrowDataType::Int64,
        DFDataType::UInt8 => ArrowDataType::UInt8,
        DFDataType::UInt16 => ArrowDataType::UInt16,
        DFDataType::UInt32 => ArrowDataType::UInt32,
        DFDataType::UInt64 => ArrowDataType::UInt64,
        DFDataType::Float16 => ArrowDataType::Float16,
        DFDataType::Float32 => ArrowDataType::Float32,
        DFDataType::Float64 => ArrowDataType::Float64,
        DFDataType::Timestamp(unit, tz) => {
            let arrow_unit = match unit {
                datafusion::arrow::datatypes::TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
                datafusion::arrow::datatypes::TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
                datafusion::arrow::datatypes::TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
            };
            ArrowDataType::Timestamp(arrow_unit, tz.clone())
        }
        DFDataType::Date32 => ArrowDataType::Date32,
        DFDataType::Date64 => ArrowDataType::Date64,
        DFDataType::Time32(unit) => {
            let arrow_unit = match unit {
                datafusion::arrow::datatypes::TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
                datafusion::arrow::datatypes::TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
                datafusion::arrow::datatypes::TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
            };
            ArrowDataType::Time32(arrow_unit)
        }
        DFDataType::Time64(unit) => {
            let arrow_unit = match unit {
                datafusion::arrow::datatypes::TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
                datafusion::arrow::datatypes::TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
                datafusion::arrow::datatypes::TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
            };
            ArrowDataType::Time64(arrow_unit)
        }
        DFDataType::Duration(unit) => {
            let arrow_unit = match unit {
                datafusion::arrow::datatypes::TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
                datafusion::arrow::datatypes::TimeUnit::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
                datafusion::arrow::datatypes::TimeUnit::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
            };
            ArrowDataType::Duration(arrow_unit)
        }
        DFDataType::Interval(unit) => {
            let arrow_unit = match unit {
                datafusion::arrow::datatypes::IntervalUnit::YearMonth => arrow::datatypes::IntervalUnit::YearMonth,
                datafusion::arrow::datatypes::IntervalUnit::DayTime => arrow::datatypes::IntervalUnit::DayTime,
                datafusion::arrow::datatypes::IntervalUnit::MonthDayNano => arrow::datatypes::IntervalUnit::MonthDayNano,
            };
            ArrowDataType::Interval(arrow_unit)
        }
        DFDataType::Binary => ArrowDataType::Binary,
        DFDataType::FixedSizeBinary(size) => ArrowDataType::FixedSizeBinary(*size),
        DFDataType::LargeBinary => ArrowDataType::LargeBinary,
        DFDataType::Utf8 => ArrowDataType::Utf8,
        DFDataType::LargeUtf8 => ArrowDataType::LargeUtf8,
        DFDataType::List(field) => {
            ArrowDataType::List(Arc::new(df_to_arrow_field(field.as_ref())))
        }
        DFDataType::FixedSizeList(field, size) => {
            ArrowDataType::FixedSizeList(Arc::new(df_to_arrow_field(field.as_ref())), *size)
        }
        DFDataType::LargeList(field) => {
            ArrowDataType::LargeList(Arc::new(df_to_arrow_field(field.as_ref())))
        }
        DFDataType::Struct(fields) => {
            ArrowDataType::Struct(fields.iter().map(|f| df_to_arrow_field(f)).collect())
        }
        DFDataType::Union(fields, mode) => {
            // DataFusion Union has UnionFields which is Vec<(i8, Arc<Field>)>
            // Arrow 52 Union also uses UnionFields
            let arrow_mode = match mode {
                datafusion::arrow::datatypes::UnionMode::Sparse => arrow::datatypes::UnionMode::Sparse,
                datafusion::arrow::datatypes::UnionMode::Dense => arrow::datatypes::UnionMode::Dense,
            };
            let arrow_fields: arrow::datatypes::UnionFields = fields.iter()
                .map(|(id, field)| (id, Arc::new(df_to_arrow_field(field.as_ref()))))
                .collect();
            ArrowDataType::Union(arrow_fields, arrow_mode)
        }
        DFDataType::Dictionary(key_type, value_type) => {
            ArrowDataType::Dictionary(Box::new(df_to_arrow_datatype(key_type)), Box::new(df_to_arrow_datatype(value_type)))
        }
        DFDataType::Map(field, sorted) => {
            ArrowDataType::Map(Arc::new(df_to_arrow_field(field.as_ref())), *sorted)
        }
        DFDataType::RunEndEncoded(run_ends, values) => {
            // RunEndEncoded takes Field types, not DataType
            ArrowDataType::RunEndEncoded(
                Arc::new(df_to_arrow_field(run_ends.as_ref())),
                Arc::new(df_to_arrow_field(values.as_ref()))
            )
        }
        _ => ArrowDataType::Utf8, // Fallback
    }
}

/// Convert Arrow 52 Field to DataFusion's Field
pub fn arrow_to_df_field(field: &ArrowField) -> DFField {
    DFField::new(
        field.name(),
        arrow_to_df_datatype(field.data_type()),
        field.is_nullable(),
    )
    .with_metadata(field.metadata().clone())
}

/// Convert DataFusion's Field to Arrow 52 Field
pub fn df_to_arrow_field(field: &DFField) -> ArrowField {
    ArrowField::new(
        field.name(),
        df_to_arrow_datatype(field.data_type()),
        field.is_nullable(),
    )
    .with_metadata(field.metadata().clone())
}

/// Convert Arrow 52 Schema to DataFusion's Schema
pub fn arrow_to_df_schema(schema: &ArrowSchema) -> Arc<DFSchema> {
    let fields: Vec<DFField> = schema.fields().iter()
        .map(|f| arrow_to_df_field(f))
        .collect();
    Arc::new(DFSchema::new(fields))
}

/// Convert DataFusion's Schema to Arrow 52 Schema
pub fn df_to_arrow_schema(schema: &DFSchema) -> Arc<ArrowSchema> {
    let fields: Vec<ArrowField> = schema.fields().iter()
        .map(|f| df_to_arrow_field(f))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// Convert Arrow 52 RecordBatch to DataFusion's RecordBatch
/// Note: This uses unsafe conversion since Arrow 52 and DataFusion's arrow use compatible
/// but different types. Arrays are compatible at runtime.
pub fn arrow_to_df_record_batch(batch: &ArrowRecordBatch) -> Result<DFRecordBatch, datafusion::arrow::error::ArrowError> {
    let df_schema = arrow_to_df_schema(batch.schema().as_ref());
    // Arrays are compatible at runtime - use unsafe conversion
    // This is safe because DataFusion's arrow and Arrow 52 use the same underlying representation
    use datafusion::arrow::array::Array as DFArray;
    use std::mem;
    let df_arrays: Vec<Arc<dyn DFArray>> = batch.columns().iter()
        .map(|arr| {
            // Unsafe conversion - arrays are compatible but have different trait types
            unsafe {
                mem::transmute::<Arc<dyn arrow::array::Array>, Arc<dyn DFArray>>(arr.clone())
            }
        })
        .collect();
    DFRecordBatch::try_new(df_schema, df_arrays)
}

/// Convert DataFusion's RecordBatch to Arrow 52 RecordBatch
/// Note: This uses unsafe conversion since Arrow 52 and DataFusion's arrow use compatible
/// but different types. Arrays are compatible at runtime.
pub fn df_to_arrow_record_batch(batch: &DFRecordBatch) -> Result<ArrowRecordBatch, arrow::error::ArrowError> {
    let arrow_schema = df_to_arrow_schema(batch.schema().as_ref());
    // Arrays are compatible at runtime - use unsafe conversion
    // This is safe because DataFusion's arrow and Arrow 52 use the same underlying representation
    use arrow::array::Array;
    use std::mem;
    let arrow_arrays: Vec<Arc<dyn Array>> = batch.columns().iter()
        .map(|arr| {
            // Unsafe conversion - arrays are compatible but have different trait types
            unsafe {
                mem::transmute::<Arc<dyn datafusion::arrow::array::Array>, Arc<dyn Array>>(arr.clone())
            }
        })
        .collect();
    ArrowRecordBatch::try_new(arrow_schema, arrow_arrays)
}
