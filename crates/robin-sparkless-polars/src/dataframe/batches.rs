//! Bounded range and Arrow IPC batch production for Polars DataFrames.

use super::*;

#[derive(Clone)]
pub(crate) struct RangeCardinality {
    pub start: i64,
    pub step: i64,
    pub row_count: u64,
    pub(super) operations: Vec<RangeOperation>,
}

#[derive(Clone)]
pub(super) enum RangeOperation {
    Filter(Expr),
    Limit(u64),
    Project(Vec<Expr>),
    Rename(Vec<(String, String)>),
}

pub(super) enum RangeBatchError {
    Polars(PolarsError),
    Engine(EngineError),
}

pub(super) struct RangeBatchProducer {
    range: RangeCardinality,
    case_sensitive: bool,
    token: Option<CancellationToken>,
    batch_rows: usize,
    first_index: u64,
    limit_consumed: Vec<u64>,
}

impl RangeBatchProducer {
    pub(super) fn new(
        range: RangeCardinality,
        case_sensitive: bool,
        token: Option<CancellationToken>,
        batch_rows: usize,
    ) -> Self {
        let limit_consumed = vec![0; range.operations.len()];
        Self {
            range,
            case_sensitive,
            token,
            batch_rows: batch_rows.max(1),
            first_index: 0,
            limit_consumed,
        }
    }

    pub(super) fn next_batch(&mut self) -> Result<Option<PlDataFrame>, RangeBatchError> {
        loop {
            if let Some(token) = &self.token {
                token.check_cancelled().map_err(RangeBatchError::Engine)?;
            }
            let limit_satisfied = self
                .range
                .operations
                .iter()
                .enumerate()
                .any(|(index, operation)| {
                    matches!(operation, RangeOperation::Limit(limit) if self.limit_consumed[index] >= *limit)
                });
            if self.first_index >= self.range.row_count || limit_satisfied {
                return Ok(None);
            }

            let rows = usize::try_from(
                (self.range.row_count - self.first_index).min(self.batch_rows as u64),
            )
            .expect("range batch size is bounded by usize");
            let mut batch = self
                .range
                .batch(self.first_index, rows)
                .map_err(RangeBatchError::Polars)?;
            self.first_index += rows as u64;

            for (operation_index, operation) in self.range.operations.iter().enumerate() {
                if let Some(token) = &self.token {
                    token.check_cancelled().map_err(RangeBatchError::Engine)?;
                }
                match operation {
                    RangeOperation::Filter(condition) => {
                        let frame = DataFrame::from_polars_with_options(batch, self.case_sensitive);
                        let filtered = frame
                            .filter(condition.clone())
                            .map_err(RangeBatchError::Polars)?;
                        batch = filtered
                            .collect_inner()
                            .map_err(RangeBatchError::Polars)?
                            .as_ref()
                            .clone();
                    }
                    RangeOperation::Limit(limit) => {
                        let remaining = limit.saturating_sub(self.limit_consumed[operation_index]);
                        if remaining < batch.height() as u64 {
                            let frame =
                                DataFrame::from_polars_with_options(batch, self.case_sensitive);
                            let limited = frame
                                .limit(remaining as usize)
                                .map_err(RangeBatchError::Polars)?;
                            batch = limited
                                .collect_inner()
                                .map_err(RangeBatchError::Polars)?
                                .as_ref()
                                .clone();
                        }
                        self.limit_consumed[operation_index] = self.limit_consumed[operation_index]
                            .saturating_add(batch.height() as u64);
                    }
                    RangeOperation::Rename(renames) => {
                        for (old_name, new_name) in renames {
                            batch
                                .rename(old_name, new_name.as_str().into())
                                .map_err(RangeBatchError::Polars)?;
                        }
                    }
                    RangeOperation::Project(expressions) => {
                        let frame = DataFrame::from_polars_with_options(batch, self.case_sensitive);
                        let projected = frame
                            .select_exprs(expressions.clone())
                            .map_err(RangeBatchError::Polars)?;
                        batch = projected
                            .collect_inner()
                            .map_err(RangeBatchError::Polars)?
                            .as_ref()
                            .clone();
                    }
                }
            }
            if let Some(token) = &self.token {
                token.check_cancelled().map_err(RangeBatchError::Engine)?;
            }
            if batch.height() > 0 {
                return Ok(Some(batch));
            }
        }
    }
}

impl RangeCardinality {
    pub(crate) fn count_matching(&self, expression: &Expr) -> Option<u64> {
        // This arithmetic shortcut is exact only before any other filter or limit has
        // changed the sequence being counted. All compound cases use bounded execution.
        if !self.operations.is_empty() {
            return None;
        }
        let Expr::BinaryExpr { left, op, right } = expression else {
            return None;
        };
        if !matches!(
            op,
            Operator::Eq
                | Operator::EqValidity
                | Operator::NotEq
                | Operator::NotEqValidity
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ) {
            return None;
        }
        let (operator, threshold) = match (left.as_ref(), right.as_ref()) {
            (Expr::Column(name), Expr::Literal(value))
                if name.as_str().eq_ignore_ascii_case("id") =>
            {
                (*op, integer_literal(value)?)
            }
            (Expr::Literal(value), Expr::Column(name))
                if name.as_str().eq_ignore_ascii_case("id") =>
            {
                (reverse_comparison(*op)?, integer_literal(value)?)
            }
            _ => return None,
        };

        if self.row_count == 0 {
            return Some(0);
        }
        if matches!(
            operator,
            Operator::Eq | Operator::EqValidity | Operator::NotEq | Operator::NotEqValidity
        ) {
            let delta = threshold as i128 - self.start as i128;
            let step = self.step as i128;
            let equal = if delta % step == 0 {
                let index = delta / step;
                (index >= 0 && (index as u128) < self.row_count as u128) as u64
            } else {
                0
            };
            return Some(if matches!(operator, Operator::Eq | Operator::EqValidity) {
                equal
            } else {
                self.row_count - equal
            });
        }

        let matches = |index: u64| {
            let value = self.start as i128 + index as i128 * self.step as i128;
            match operator {
                Operator::Lt => value < threshold as i128,
                Operator::LtEq => value <= threshold as i128,
                Operator::Gt => value > threshold as i128,
                Operator::GtEq => value >= threshold as i128,
                _ => false,
            }
        };
        let first = matches(0);
        let last = matches(self.row_count - 1);
        if first == last {
            return Some(if first { self.row_count } else { 0 });
        }

        let mut low = 1;
        let mut high = self.row_count;
        while low < high {
            let middle = low + (high - low) / 2;
            if matches(middle) == first {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Some(if first { low } else { self.row_count - low })
    }

    fn batch(&self, first_index: u64, rows: usize) -> Result<PlDataFrame, PolarsError> {
        let mut values = Vec::with_capacity(rows);
        for offset in 0..rows {
            let index = first_index.checked_add(offset as u64).ok_or_else(|| {
                PolarsError::ComputeError("range scan row index overflowed UInt64".into())
            })?;
            let value = self.start as i128 + index as i128 * self.step as i128;
            values.push(i64::try_from(value).map_err(|_| {
                PolarsError::ComputeError("range value exceeds Int64 bounds".into())
            })?);
        }
        let id = polars::prelude::Series::new("id".into(), values);
        PlDataFrame::new_infer_height(vec![id.into()])
    }
}

pub(super) enum IpcBatchSource {
    Polars(CollectBatches),
    Range(RangeBatchProducer),
    Eager {
        frame: Arc<PlDataFrame>,
        next_row: usize,
        max_rows: usize,
    },
}

impl IpcBatchSource {
    pub(super) fn next_batch(&mut self) -> Option<Result<PlDataFrame, EngineError>> {
        match self {
            Self::Polars(batches) => batches
                .next()
                .map(|result| result.map_err(polars_to_core_error)),
            Self::Range(producer) => match producer.next_batch() {
                Ok(Some(batch)) => Some(Ok(batch)),
                Ok(None) => None,
                Err(RangeBatchError::Polars(error)) => Some(Err(polars_to_core_error(error))),
                Err(RangeBatchError::Engine(error)) => Some(Err(error)),
            },
            Self::Eager {
                frame,
                next_row,
                max_rows,
            } => {
                if *next_row >= frame.height() {
                    return None;
                }
                let offset = match i64::try_from(*next_row) {
                    Ok(offset) => offset,
                    Err(_) => {
                        return Some(Err(EngineError::invalid_input(
                            "eager Arrow batch offset exceeds Int64".into(),
                        )));
                    }
                };
                let rows = (*max_rows).min(frame.height() - *next_row);
                *next_row += rows;
                Some(Ok(frame.slice(offset, rows)))
            }
        }
    }
}

/// A bounded, cancellable stream of collected DataFrame batches.
///
/// This export iterator uses the native range producer for plans that can be
/// evaluated per range batch, avoiding Polars' unsupported `AnonymousScan`
/// lowering for those plans.
pub struct DataFrameBatches {
    pub(super) source: Option<IpcBatchSource>,
    pub(super) token: CancellationToken,
}

impl Iterator for DataFrameBatches {
    type Item = Result<PlDataFrame, EngineError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(error) = self.token.check_cancelled() {
            self.source.take();
            return Some(Err(error));
        }
        let result = self.source.as_mut()?.next_batch()?;
        if result.is_err() {
            self.source.take();
        }
        Some(result)
    }
}

pub(super) fn merge_union_logical_type(
    left: &LogicalDataType,
    right: &LogicalDataType,
    output_physical: &DataType,
) -> Option<LogicalDataType> {
    use LogicalDataType as D;

    match (left, right, output_physical) {
        (
            D::Array(_) | D::ArrayWithNullability { .. },
            D::Array(_) | D::ArrayWithNullability { .. },
            DataType::List(output_element),
        ) => {
            let (left_element, left_nullable) = match left {
                D::Array(element) => (element.as_ref(), true),
                D::ArrayWithNullability {
                    element_type,
                    contains_null,
                } => (element_type.as_ref(), *contains_null),
                _ => return None,
            };
            let (right_element, right_nullable) = match right {
                D::Array(element) => (element.as_ref(), true),
                D::ArrayWithNullability {
                    element_type,
                    contains_null,
                } => (element_type.as_ref(), *contains_null),
                _ => return None,
            };
            Some(D::ArrayWithNullability {
                element_type: Box::new(merge_union_logical_type(
                    left_element,
                    right_element,
                    output_element,
                )?),
                contains_null: left_nullable || right_nullable,
            })
        }
        (
            D::LogicalStruct(left_fields),
            D::LogicalStruct(right_fields),
            DataType::Struct(output_fields),
        ) if left_fields.len() == right_fields.len()
            && left_fields.len() == output_fields.len() =>
        {
            let fields = left_fields
                .iter()
                .zip(right_fields)
                .zip(output_fields)
                .map(|((left, right), output)| {
                    if left.name != right.name || left.name != output.name().as_str() {
                        return None;
                    }
                    let mut field = left.clone();
                    field.data_type = merge_union_logical_type(
                        &left.data_type,
                        &right.data_type,
                        output.dtype(),
                    )?;
                    field.nullable = left.nullable || right.nullable;
                    Some(field)
                })
                .collect::<Option<Vec<_>>>()?;
            Some(D::LogicalStruct(fields))
        }
        _ => crate::schema_conv::checked_polars_type_to_data_type(output_physical, "union").ok(),
    }
}

pub(super) fn conservative_union_logical_type(
    output_physical: &DataType,
) -> Option<LogicalDataType> {
    match output_physical {
        DataType::List(element) => Some(LogicalDataType::ArrayWithNullability {
            element_type: Box::new(conservative_union_logical_type(element)?),
            contains_null: true,
        }),
        DataType::Struct(fields) => Some(LogicalDataType::LogicalStruct(
            fields
                .iter()
                .map(|field| {
                    Some(StructField::new(
                        field.name().to_string(),
                        conservative_union_logical_type(field.dtype())?,
                        true,
                    ))
                })
                .collect::<Option<Vec<_>>>()?,
        )),
        _ => crate::schema_conv::checked_polars_type_to_data_type(output_physical, "union").ok(),
    }
}

pub(super) fn integer_literal(value: &LiteralValue) -> Option<i64> {
    if let LiteralValue::Dyn(DynLiteralValue::Int(value)) = value {
        return i64::try_from(*value).ok();
    }
    let LiteralValue::Scalar(value) = value else {
        return None;
    };
    match value.as_any_value() {
        AnyValue::Int8(value) => Some(value as i64),
        AnyValue::Int16(value) => Some(value as i64),
        AnyValue::Int32(value) => Some(value as i64),
        AnyValue::Int64(value) => Some(value),
        AnyValue::UInt8(value) => Some(value as i64),
        AnyValue::UInt16(value) => Some(value as i64),
        AnyValue::UInt32(value) => Some(value as i64),
        AnyValue::UInt64(value) => i64::try_from(value).ok(),
        _ => None,
    }
}

fn reverse_comparison(operator: Operator) -> Option<Operator> {
    Some(match operator {
        Operator::Eq | Operator::EqValidity => operator,
        Operator::NotEq | Operator::NotEqValidity => operator,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

pub(super) fn external_arrow_batch_limit(
    options: Option<&[(String, String)]>,
    option: &str,
    default: usize,
) -> Result<usize, PolarsError> {
    let Some((_, value)) = options
        .unwrap_or_default()
        .iter()
        .rev()
        .find(|(key, _)| key.eq_ignore_ascii_case(option))
    else {
        return Ok(default);
    };
    let limit = value.parse::<usize>().map_err(|_| {
        PolarsError::InvalidOperation(
            format!("{option} must be a positive integer, got '{value}'").into(),
        )
    })?;
    if limit == 0 {
        return Err(PolarsError::InvalidOperation(
            format!("{option} must be greater than zero").into(),
        ));
    }
    Ok(limit)
}

/// Resolve a column or dotted struct path against a schema.
/// Resolution order:
/// 1. If a column with the full name exists (respecting case sensitivity), return it.
/// 2. If the name contains dots, treat the first component as the base column and the rest
///    as struct field names, building a struct.field_by_name(...) chain.
/// 3. Otherwise, return a ColumnNotFound error listing available columns.
pub fn resolve_column_with_schema(
    name: &str,
    schema: &Schema,
    case_sensitive: bool,
    extra_valid_names: Option<&[String]>,
) -> Result<String, PolarsError> {
    let mut candidates: Vec<String> = schema
        .iter_names_and_dtypes()
        .map(|(n, _)| n.to_string())
        .collect();
    if let Some(extra) = extra_valid_names {
        candidates.extend_from_slice(extra);
    }
    if case_sensitive {
        if candidates.iter().any(|n| n == name) {
            return Ok(name.to_string());
        }
    } else {
        let name_lower = name.to_lowercase();
        for n in &candidates {
            if n.to_lowercase() == name_lower {
                return Ok(n.clone());
            }
        }
    }
    let available = candidates.join(", ");
    Err(PolarsError::ColumnNotFound(
        format!(
            "cannot resolve: column '{name}' not found in DataFrame. Available: [{available}]."
        )
        .into(),
    ))
}

/// Map Polars DataType to PySpark type name for schema alignment (#790, #734).
pub(super) fn pyspark_type_name(dtype: &DataType) -> String {
    use polars::datatypes::DataType as PlDataType;
    match dtype {
        PlDataType::Int32 => "IntegerType".to_string(),
        PlDataType::Int64 => "LongType".to_string(),
        PlDataType::String => "StringType".to_string(),
        PlDataType::Float32 | PlDataType::Float64 => "DoubleType".to_string(),
        PlDataType::Boolean => "BooleanType".to_string(),
        PlDataType::Date => "DateType".to_string(),
        PlDataType::Datetime(_, _) => "TimestampType".to_string(),
        PlDataType::List(inner) => format!("ArrayType({})", pyspark_type_name(inner)),
        PlDataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), pyspark_type_name(f.dtype())))
                .collect();
            format!("StructType([{}])", parts.join(", "))
        }
        _ => format!("{dtype:?}"),
    }
}

/// Inner representation: eager (legacy) or lazy (preferred).
/// Transformations extend LazyFrame; only actions trigger collect.
#[allow(clippy::large_enum_variant)]
pub(crate) enum DataFrameInner {
    #[allow(dead_code)]
    Eager(Arc<PlDataFrame>),
    Lazy(LazyFrame),
}

pub(super) struct PolarsIpcBatchIterator {
    pub(super) source: Option<IpcBatchSource>,
    pub(super) empty_schema: Arc<Schema>,
    pub(super) current: Option<PlDataFrame>,
    pub(super) current_offset: usize,
    pub(super) deferred_batch: Option<PlDataFrame>,
    pub(super) max_rows: usize,
    pub(super) max_bytes: usize,
    pub(super) logical_schema_json: Option<String>,
    pub(super) connect_output_schema: Option<StructType>,
    pub(super) token: CancellationToken,
    pub(super) deferred_error: Option<EngineError>,
    pub(super) emitted_empty: bool,
    pub(super) closed: bool,
    pub(super) trace_enabled: bool,
    pub(super) trace_emitted: bool,
    pub(super) source_batches: u64,
    pub(super) source_rows: u64,
    pub(super) output_batches: u64,
    pub(super) serialization_time: std::time::Duration,
    pub(super) rechunk_time: std::time::Duration,
    pub(super) ipc_write_time: std::time::Duration,
    pub(super) schema_rewrite_time: std::time::Duration,
    pub(super) combine_time: std::time::Duration,
    pub(super) max_source_batch_bytes_estimated: usize,
    pub(super) peak_buffered_frame_bytes_estimated: usize,
    pub(super) peak_combined_frame_bytes_estimated: usize,
    pub(super) peak_ipc_buffer_bytes: usize,
    pub(super) peak_ipc_rewrite_bytes_estimated: usize,
    pub(super) serialized_ipc_bytes: u64,
    pub(super) emitted_ipc_bytes: u64,
}

impl PolarsIpcBatchIterator {
    fn emit_trace(&mut self) {
        if !self.trace_enabled || self.trace_emitted {
            return;
        }
        self.trace_emitted = true;
        eprintln!(
            "sparkless_arrow_trace event=complete source_batches={} source_rows={} output_batches={} serialization_ms={:.3} rechunk_ms={:.3} ipc_write_ms={:.3} schema_rewrite_ms={:.3} combine_ms={:.3} max_source_batch_bytes_estimated={} peak_buffered_frame_bytes_estimated={} peak_combined_frame_bytes_estimated={} peak_ipc_buffer_bytes={} peak_ipc_rewrite_bytes_estimated={} serialized_ipc_bytes={} emitted_ipc_bytes={}",
            self.source_batches,
            self.source_rows,
            self.output_batches,
            self.serialization_time.as_secs_f64() * 1_000.0,
            self.rechunk_time.as_secs_f64() * 1_000.0,
            self.ipc_write_time.as_secs_f64() * 1_000.0,
            self.schema_rewrite_time.as_secs_f64() * 1_000.0,
            self.combine_time.as_secs_f64() * 1_000.0,
            self.max_source_batch_bytes_estimated,
            self.peak_buffered_frame_bytes_estimated,
            self.peak_combined_frame_bytes_estimated,
            self.peak_ipc_buffer_bytes,
            self.peak_ipc_rewrite_bytes_estimated,
            self.serialized_ipc_bytes,
            self.emitted_ipc_bytes,
        );
    }

    fn close(&mut self) {
        self.emit_trace();
        self.source.take();
        self.current.take();
        self.current_offset = 0;
        self.deferred_batch.take();
        self.closed = true;
    }

    fn next_source_batch(&mut self, max_rows: usize) -> Option<Result<PlDataFrame, EngineError>> {
        if let Some(batch) = self.deferred_batch.take() {
            let rows = max_rows.min(batch.height());
            if rows < batch.height() {
                self.deferred_batch = Some(batch.slice(rows as i64, batch.height() - rows));
            }
            return Some(Ok(batch.slice(0, rows)));
        }
        if self.current.is_none() {
            match self.source.as_mut().and_then(IpcBatchSource::next_batch) {
                Some(Ok(batch)) => {
                    self.source_batches = self.source_batches.saturating_add(1);
                    self.source_rows = self
                        .source_rows
                        .saturating_add(batch.height().try_into().unwrap_or(u64::MAX));
                    if self.trace_enabled {
                        self.max_source_batch_bytes_estimated = self
                            .max_source_batch_bytes_estimated
                            .max(batch.estimated_size());
                    }
                    self.current = Some(batch);
                }
                Some(Err(error)) => return Some(Err(error)),
                None => return None,
            }
        }
        let current = self.current.as_ref().expect("source batch was set");
        let rows = max_rows.min(current.height() - self.current_offset);
        let batch = current.slice(self.current_offset as i64, rows);
        self.current_offset += rows;
        if self.current_offset >= current.height() {
            self.current.take();
            self.current_offset = 0;
        }
        Some(Ok(batch))
    }

    fn combine_rows(rows: &[PlDataFrame]) -> Result<PlDataFrame, polars::prelude::PolarsError> {
        let mut combined = rows.first().expect("non-empty rows").clone();
        for row in &rows[1..] {
            combined.vstack_mut(row)?;
        }
        Ok(combined)
    }

    fn serialize_batch(&mut self, batch: &mut PlDataFrame) -> Result<Vec<u8>, EngineError> {
        let started = self.trace_enabled.then(std::time::Instant::now);
        let rechunk_started = self.trace_enabled.then(std::time::Instant::now);
        batch.rechunk_mut();
        if let Some(started) = rechunk_started {
            self.rechunk_time += started.elapsed();
        }
        let mut cursor = std::io::Cursor::new(Vec::new());
        let ipc_write_started = self.trace_enabled.then(std::time::Instant::now);
        let raw_bytes = (|| {
            {
                use polars::io::ipc::IpcStreamWriter;
                use polars::prelude::SerWriter;
                let mut writer = IpcStreamWriter::new(&mut cursor);
                if self.connect_output_schema.is_none()
                    && let Some(schema) = &self.logical_schema_json
                {
                    let mut metadata = polars_arrow::datatypes::Metadata::new();
                    metadata.insert("sparkless.logical_schema.v1".into(), schema.clone().into());
                    writer.set_custom_schema_metadata(Arc::new(metadata));
                }
                writer.finish(batch).map_err(polars_to_core_error)?;
            }
            Ok::<_, EngineError>(cursor.into_inner())
        })();
        if let Some(started) = ipc_write_started {
            self.ipc_write_time += started.elapsed();
        }
        let result = raw_bytes.and_then(|bytes| {
            if let Some(schema) = &self.connect_output_schema {
                let rewrite_started = self.trace_enabled.then(std::time::Instant::now);
                let rewritten = rewrite_connect_arrow_stream_schema(&bytes, schema);
                if let Some(started) = rewrite_started {
                    self.schema_rewrite_time += started.elapsed();
                }
                rewritten.inspect(|rewritten| {
                    if self.trace_enabled {
                        self.peak_ipc_rewrite_bytes_estimated = self
                            .peak_ipc_rewrite_bytes_estimated
                            .max(bytes.len().saturating_add(rewritten.len()));
                    }
                })
            } else {
                Ok(bytes)
            }
        });
        if let Some(started) = started {
            self.serialization_time += started.elapsed();
        }
        if let Ok(bytes) = &result
            && self.trace_enabled
        {
            self.peak_ipc_buffer_bytes = self.peak_ipc_buffer_bytes.max(bytes.len());
            self.serialized_ipc_bytes = self
                .serialized_ipc_bytes
                .saturating_add(bytes.len().try_into().unwrap_or(u64::MAX));
        }
        result
    }

    fn record_emitted_ipc_bytes(&mut self, bytes: usize) {
        self.emitted_ipc_bytes = self
            .emitted_ipc_bytes
            .saturating_add(bytes.try_into().unwrap_or(u64::MAX));
    }
}

impl Drop for PolarsIpcBatchIterator {
    fn drop(&mut self) {
        self.emit_trace();
    }
}

/// Replace the writer's unique physical field names with the ordered public Connect schema.
/// Arrow schemas permit duplicate field names, while Polars' dataframe schema does not. The
/// record-batch buffers remain unchanged; only the first IPC schema message is rebuilt.
fn rewrite_connect_arrow_stream_schema(
    bytes: &[u8],
    public_schema: &StructType,
) -> Result<Vec<u8>, EngineError> {
    use polars_arrow::io::ipc::format::ipc::planus::{Builder, ReadAsRoot};
    use polars_arrow::io::ipc::format::ipc::{Message, MessageHeader, MessageRef};

    if bytes.len() < 8 || bytes[..4] != [0xff; 4] {
        return Err(EngineError::internal(
            "native Arrow writer returned an invalid stream prefix".into(),
        ));
    }
    let original_metadata_len = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let original_metadata_len = usize::try_from(original_metadata_len).map_err(|_| {
        EngineError::internal("native Arrow writer returned an invalid schema length".into())
    })?;
    let original_message_end = 8usize
        .checked_add(original_metadata_len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| {
            EngineError::internal("native Arrow writer returned a truncated schema".into())
        })?;
    let message_ref =
        MessageRef::read_as_root(&bytes[8..original_message_end]).map_err(|error| {
            EngineError::internal(format!("native Arrow schema cannot be read: {error}"))
        })?;
    let mut message = Message::try_from(message_ref).map_err(|error| {
        EngineError::internal(format!("native Arrow schema cannot be converted: {error}"))
    })?;
    let Some(MessageHeader::Schema(schema_message)) = message.header.as_mut() else {
        return Err(EngineError::internal(
            "native Arrow stream does not begin with a schema".into(),
        ));
    };
    let fields = schema_message
        .fields
        .as_mut()
        .ok_or_else(|| EngineError::internal("native Arrow schema has no fields".into()))?;
    if fields.len() != public_schema.logical_fields().len() {
        return Err(EngineError::internal(format!(
            "Connect Arrow schema has {} fields but the public schema has {}",
            fields.len(),
            public_schema.logical_fields().len()
        )));
    }
    for (arrow_field, logical_field) in fields.iter_mut().zip(public_schema.logical_fields()) {
        arrow_field.name = Some(logical_field.name.clone());
        arrow_field.nullable = logical_field.nullable;
    }
    if let Some(metadata) = schema_message.custom_metadata.as_mut() {
        metadata.retain(|entry| entry.key.as_deref() != Some("sparkless.logical_schema.v1"));
        if metadata.is_empty() {
            schema_message.custom_metadata = None;
        }
    }

    let serialized_message = {
        let mut builder = Builder::new();
        builder.finish(&message, None).to_vec()
    };
    let padded_message_len = serialized_message
        .len()
        .checked_add(7)
        .map(|length| length & !7)
        .ok_or_else(|| EngineError::internal("Connect Arrow schema size overflow".into()))?;
    let continuation_len = i32::try_from(padded_message_len).map_err(|_| {
        EngineError::internal("Connect Arrow schema exceeds IPC framing range".into())
    })?;
    let mut output = Vec::with_capacity(
        8usize
            .saturating_add(padded_message_len)
            .saturating_add(bytes.len() - original_message_end),
    );
    output.extend_from_slice(&[0xff; 4]);
    output.extend_from_slice(&continuation_len.to_le_bytes());
    output.extend_from_slice(&serialized_message);
    output.resize(8 + padded_message_len, 0);
    output.extend_from_slice(&bytes[original_message_end..]);
    Ok(output)
}

impl Iterator for PolarsIpcBatchIterator {
    type Item = Result<ArrowIpcStream, EngineError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.closed {
            return None;
        }
        let mut rows = Vec::new();
        let mut row_count = 0usize;
        let mut estimated_bytes = 0usize;
        let mut source_ended = false;
        loop {
            if self.current.is_none() {
                if let Some(error) = self.deferred_error.take() {
                    if rows.is_empty() {
                        self.close();
                        return Some(Err(error));
                    }
                    self.deferred_error = Some(error);
                    break;
                }
            }
            if let Err(error) = self.token.check_cancelled() {
                self.close();
                return Some(Err(error));
            }
            let remaining_rows = self.max_rows.saturating_sub(row_count).max(1);
            let batch = match self.next_source_batch(remaining_rows) {
                Some(Ok(batch)) if batch.height() > 0 => batch,
                Some(Ok(_)) => continue,
                Some(Err(error)) => {
                    if !rows.is_empty() {
                        self.deferred_error = Some(error);
                        break;
                    }
                    self.close();
                    return Some(Err(error));
                }
                None => {
                    source_ended = true;
                    break;
                }
            };
            if let Err(error) = self.token.check_cancelled() {
                self.close();
                return Some(Err(error));
            }

            let batch_bytes = batch.estimated_size();
            if !rows.is_empty() && estimated_bytes.saturating_add(batch_bytes) > self.max_bytes {
                self.deferred_batch = Some(batch);
                break;
            }
            estimated_bytes = estimated_bytes.saturating_add(batch_bytes);
            row_count = row_count.saturating_add(batch.height());
            rows.push(batch);
            if self.trace_enabled {
                self.peak_buffered_frame_bytes_estimated = self
                    .peak_buffered_frame_bytes_estimated
                    .max(estimated_bytes);
            }
            if row_count >= self.max_rows || estimated_bytes >= self.max_bytes {
                break;
            }
        }

        if rows.is_empty() {
            if source_ended {
                if !self.emitted_empty {
                    self.emitted_empty = true;
                    let mut empty = PlDataFrame::empty_with_schema(&self.empty_schema);
                    let bytes = match self.serialize_batch(&mut empty) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            self.close();
                            return Some(Err(error));
                        }
                    };
                    if bytes.len() > self.max_bytes {
                        self.close();
                        return Some(Err(EngineError::invalid_input(format!(
                            "an empty Arrow IPC schema requires {} bytes, exceeding max_bytes={}",
                            bytes.len(),
                            self.max_bytes
                        ))));
                    }
                    self.output_batches = self.output_batches.saturating_add(1);
                    self.record_emitted_ipc_bytes(bytes.len());
                    self.close();
                    return Some(Ok(ArrowIpcStream {
                        bytes,
                        row_count: 0,
                    }));
                }
                self.close();
            }
            return None;
        }
        let combine_started = self.trace_enabled.then(std::time::Instant::now);
        let mut combined = match Self::combine_rows(&rows) {
            Ok(combined) => combined,
            Err(error) => {
                self.close();
                return Some(Err(polars_to_core_error(error)));
            }
        };
        if let Some(started) = combine_started {
            self.combine_time += started.elapsed();
        }
        if self.trace_enabled {
            self.peak_combined_frame_bytes_estimated = self
                .peak_combined_frame_bytes_estimated
                .max(combined.estimated_size());
        }
        let bytes = match self.serialize_batch(&mut combined) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.close();
                return Some(Err(error));
            }
        };
        if bytes.len() <= self.max_bytes {
            self.output_batches = self.output_batches.saturating_add(1);
            self.record_emitted_ipc_bytes(bytes.len());
            if source_ended {
                self.close();
            }
            return Some(Ok(ArrowIpcStream {
                bytes,
                row_count: combined.height() as u64,
            }));
        }

        if combined.height() == 1 {
            let actual_bytes = bytes.len();
            self.close();
            return Some(Err(EngineError::invalid_input(format!(
                "a single Arrow IPC row requires {actual_bytes} bytes, exceeding max_bytes={}",
                self.max_bytes
            ))));
        }
        let mut prefix_rows = combined.height() / 2;
        while prefix_rows > 0 {
            let mut prefix = combined.slice(0, prefix_rows);
            let prefix_bytes = match self.serialize_batch(&mut prefix) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.close();
                    return Some(Err(error));
                }
            };
            if prefix_bytes.len() <= self.max_bytes {
                let mut pending =
                    combined.slice(prefix_rows as i64, combined.height() - prefix_rows);
                if let Some(deferred_batch) = self.deferred_batch.take()
                    && let Err(error) = pending.vstack_mut(&deferred_batch)
                {
                    self.close();
                    return Some(Err(polars_to_core_error(error)));
                }
                if let Some(remaining_pending) = self.current.take() {
                    let remaining_pending = remaining_pending.slice(
                        self.current_offset as i64,
                        remaining_pending.height() - self.current_offset,
                    );
                    if let Err(error) = pending.vstack_mut(&remaining_pending) {
                        self.close();
                        return Some(Err(polars_to_core_error(error)));
                    }
                }
                self.current = Some(pending);
                self.current_offset = 0;
                self.output_batches = self.output_batches.saturating_add(1);
                self.record_emitted_ipc_bytes(prefix_bytes.len());
                return Some(Ok(ArrowIpcStream {
                    bytes: prefix_bytes,
                    row_count: prefix_rows as u64,
                }));
            }
            prefix_rows /= 2;
        }
        self.close();
        Some(Err(EngineError::invalid_input(format!(
            "a single Arrow IPC row exceeds max_bytes={}",
            self.max_bytes
        ))))
    }
}
